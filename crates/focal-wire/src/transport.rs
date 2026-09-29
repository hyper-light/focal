//! Mutually authenticated TLS 1.3 over Quinn. One bounded request occupies one
//! bidirectional stream; slow work on one stream does not serialize other streams.
use crate::*;
use quinn::{
    Connection, Endpoint,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::{net::SocketAddr, sync::Arc};
use tokio::{sync::Semaphore, task::JoinSet};

/// DER credentials supplied by the deployment's existing PKI. Private keys are
/// never Debug/Serialize and are never accepted in a request envelope.
pub struct TlsIdentity {
    pub certificate_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
}
impl TlsIdentity {
    pub fn from_pkcs8(certificate_chain: Vec<Vec<u8>>, private_key: Vec<u8>) -> Self {
        Self {
            certificate_chain: certificate_chain
                .into_iter()
                .map(CertificateDer::from)
                .collect(),
            private_key: PrivatePkcs8KeyDer::from(private_key).into(),
        }
    }
}
fn roots(certificates: Vec<Vec<u8>>) -> Result<rustls::RootCertStore, WireError> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(CertificateDer::from(certificate))
            .map_err(|_| WireError::Authentication)?;
    }
    if roots.is_empty() {
        return Err(WireError::Authentication);
    }
    Ok(roots)
}
// Quinn/rustls retain shared configurations across their internal connection
// tasks; these Arc types are required by those libraries' public APIs.
/// Longest silence before a QUIC connection is considered dead.
const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// The most a stream is given to send ahead of what its reader has taken.
///
/// quinn keeps what arrives out of order as the spans it arrived in, and
/// closes a connection whose stream holds more than 1,024 of them once it
/// has merged what it merges (`too many gaps in stream buffer`): spans next
/// to each other of less than a 1,024th of what is held. A megabyte holds
/// no more than 1,024 spans that are not merged, however it arrived. With
/// the 10 MiB of a frame as its window, a sender that overshot a fast and
/// long path in its first round trips, as every law does that probes for
/// the rate, lost every second datagram to the bottleneck's queue and its
/// connection with them (`tests/congestion.rs`). A frame longer than the
/// window is read as it arrives, so the window bounds what is in flight
/// and not what is sent.
pub const STREAM_WINDOW_CEILING: u32 = 1024 * 1024;
/// The path the content lane is sized for: what one gigabit per second
/// holds in flight over a hundred milliseconds (27 §7), a wide-area link
/// between data centres; a nearer path fills fewer of its streams and a
/// further one is not carried faster by more of them than the window
/// the law opens.
pub const REFERENCE_PATH_BITS_PER_SECOND: u64 = 1_000_000_000;
pub const REFERENCE_PATH_ROUND_TRIP: std::time::Duration = std::time::Duration::from_millis(100);
/// The streams a transfer to one peer may hold at once: what the
/// reference path holds in flight, a stream's window at a time, and one
/// ([`bulk_width`]): thirteen.
pub fn content_streams() -> u32 {
    let in_flight = REFERENCE_PATH_BITS_PER_SECOND
        .saturating_mul(u64::try_from(REFERENCE_PATH_ROUND_TRIP.as_millis()).unwrap_or(u64::MAX))
        .checked_div(8_000)
        .unwrap_or(0);
    let streams = in_flight
        .div_ceil(u64::from(STREAM_WINDOW_CEILING))
        .saturating_add(1);
    u32::try_from(streams).unwrap_or(u32::MAX)
}

/// By how many streams a transfer goes on a connection whose law holds
/// `window` bytes in flight, `most` at most: one for every megabyte the
/// window holds, and one. A stream carries no more than a megabyte in a
/// round trip ([`STREAM_WINDOW_CEILING`]), so a window of less is filled by
/// one; and the law opens its window no further than what is sent fills
/// it, so the stream that is one more is what lets it find that the path
/// holds more (`tests/congestion.rs`).
pub fn bulk_width(window: u64, most: usize) -> usize {
    usize::try_from(
        window
            .checked_div(u64::from(STREAM_WINDOW_CEILING))
            .unwrap_or(0),
    )
    .unwrap_or(usize::MAX)
    .saturating_add(1)
    .min(most.max(1))
}
/// What the exchanges under way on a connection have to send, in bytes.
#[derive(Default)]
struct Held(std::sync::atomic::AtomicU64);
/// What one exchange has to send, held until it ends.
struct Sending<'a> {
    held: &'a Held,
    bytes: u64,
}
impl Held {
    fn send(&self, bytes: usize) -> (Sending<'_>, u64) {
        use std::sync::atomic::Ordering;
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        // A count that would not fit is held at what fits.
        let mut before = self.0.load(Ordering::Acquire);
        loop {
            let after = before.saturating_add(bytes);
            match self
                .0
                .compare_exchange_weak(before, after, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    let sending = Sending {
                        held: self,
                        bytes: after.saturating_sub(before),
                    };
                    return (sending, after);
                }
                Err(found) => before = found,
            }
        }
    }
    fn now(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}
impl Drop for Sending<'_> {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        let mut before = self.held.0.load(Ordering::Acquire);
        while let Err(found) = self.held.0.compare_exchange_weak(
            before,
            before.saturating_sub(self.bytes),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            before = found;
        }
    }
}
/// Wait for `work`, which sends `bytes` over `connection`, as long as the
/// path takes to carry them. Every `period` that ends without it is
/// charged what the connection sent in it and has not found lost. The
/// work is given up at the end of a period in which less than a datagram
/// was sent ([`LEAST_PROGRESS`]), and at the end of one that began when
/// all the exchanges on the connection had to send beside it (`held`) and
/// `bytes` more had been sent: the peer had the request and a period to
/// answer it. So a megabyte is given eight seconds and more on a path
/// that carries a megabit in a second, and an exchange whose peer
/// stopped taking it one period or two on any path.
///
/// What is held is no more than the frames of the streams of a
/// connection, and every period but the last sends a datagram of it: the
/// wait ends.
async fn carried<T>(
    connection: &Connection,
    held: &Held,
    bytes: usize,
    period: std::time::Duration,
    work: impl Future<Output = Result<T, WireError>>,
) -> Result<T, WireError> {
    let sent = || {
        let stats = connection.stats();
        stats.udp_tx.bytes.saturating_sub(stats.path.lost_bytes)
    };
    let (_sending, mut owed) = held.send(bytes);
    let mut charged = 0_u64;
    let mut before = sent();
    let mut work = std::pin::pin!(work);
    loop {
        let had = charged >= owed;
        if let Ok(done) = tokio::time::timeout(period, work.as_mut()).await {
            return done;
        }
        let now = sent();
        let moved = now.saturating_sub(before);
        before = now;
        if had || moved < u64::try_from(LEAST_PROGRESS).unwrap_or(u64::MAX) {
            return Err(WireError::Timeout);
        }
        charged = charged.saturating_add(moved);
        // What came to the connection since is sent in turn with this.
        owed = owed.max(held.now());
    }
}
fn transport(limits: &WireLimits) -> Result<Arc<quinn::TransportConfig>, WireError> {
    quic_transport(limits).map(Arc::new)
}
/// What focal's connections are set to, for a caller that measures them.
pub fn quic_transport(limits: &WireLimits) -> Result<quinn::TransportConfig, WireError> {
    limits.validate()?;
    let streams = limits
        .streams_per_connection
        .checked_add(2)
        .ok_or(WireError::Limit)?;
    let frame = limits
        .max_frame_bytes
        .checked_add(HEADER_BYTES as u32)
        .ok_or(WireError::Limit)?
        .min(STREAM_WINDOW_CEILING);
    let window = u64::from(frame)
        .checked_mul(u64::from(streams))
        .ok_or(WireError::Limit)?;
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(streams.into());
    transport.max_concurrent_uni_streams(0u8.into());
    transport.stream_receive_window(frame.into());
    transport.receive_window(quinn::VarInt::from_u64(window).map_err(|_| WireError::Limit)?);
    transport.send_window(window);
    // A peer that died or restarted is noticed within the idle bound rather
    // than the full request timeout: keep-alive pings hold a healthy
    // connection open across long requests, and a silent one is closed so
    // the next attempt reconnects instead of waiting on a dead connection.
    let idle = limits.request_timeout.min(IDLE_TIMEOUT);
    transport.max_idle_timeout(Some(idle.try_into().map_err(|_| WireError::Limit)?));
    transport.keep_alive_interval(Some(idle.checked_div(4).ok_or(WireError::Limit)?));
    // Chosen by measurement against the laws quinn brings
    // (`tests/congestion.rs`, 27 §3.1 P10): the one that stalled on no path
    // and kept what is asked beside a transfer from waiting for a full
    // queue.
    transport.congestion_controller_factory(Arc::new(crate::congestion::CopaConfig::default()));
    Ok(transport)
}

pub fn server_tls(
    identity: TlsIdentity,
    client_roots: Vec<Vec<u8>>,
    limits: &WireLimits,
) -> Result<quinn::ServerConfig, WireError> {
    limits.validate()?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots(client_roots)?),
        provider.clone(),
    )
    .build()
    .map_err(|_| WireError::Authentication)?;
    let mut tls = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| WireError::Authentication)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(identity.certificate_chain, identity.private_key)
        .map_err(|_| WireError::Authentication)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    server_transport(tls, limits)
}

/// Apply the shared stream/window policy to an already configured TLS server.
/// Multiplexed listeners supply their certificate resolver and client verifier
/// once instead of constructing and discarding a second TLS configuration.
pub fn server_transport(
    mut tls: rustls::ServerConfig,
    limits: &WireLimits,
) -> Result<quinn::ServerConfig, WireError> {
    let transport = transport(limits)?;
    tls.max_early_data_size = 0;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(tls).map_err(|_| WireError::Authentication)?,
    ));
    config.transport_config(transport);
    Ok(config)
}
pub fn client_tls(
    identity: TlsIdentity,
    server_roots: Vec<Vec<u8>>,
    limits: &WireLimits,
) -> Result<quinn::ClientConfig, WireError> {
    limits.validate()?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| WireError::Authentication)?
        .with_root_certificates(roots(server_roots)?)
        .with_client_auth_cert(identity.certificate_chain, identity.private_key)
        .map_err(|_| WireError::Authentication)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.enable_early_data = false;
    let mut config = quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls).map_err(|_| WireError::Authentication)?,
    ));
    config.transport_config(transport(limits)?);
    Ok(config)
}

pub struct QuicServer {
    endpoint: Endpoint,
    registry: PeerRegistry,
    limits: WireLimits,
    admission: crate::Admission,
}
impl QuicServer {
    pub fn bind(
        address: SocketAddr,
        tls: quinn::ServerConfig,
        registry: PeerRegistry,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        let admission = crate::AdmissionLimits::for_connections(limits.max_connections);
        Self::bind_admitting(address, tls, registry, limits, admission)
    }
    /// [`Self::bind`] with the admission bounds stated.
    pub fn bind_admitting(
        address: SocketAddr,
        tls: quinn::ServerConfig,
        registry: PeerRegistry,
        limits: WireLimits,
        admission: crate::AdmissionLimits,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        let admission = crate::Admission::new(admission).map_err(|_| WireError::Limit)?;
        let endpoint = transport_setup(|| Ok(Endpoint::server(tls, address)?))?;
        Ok(Self {
            endpoint,
            registry,
            limits,
            admission,
        })
    }
    /// Who holds this server's connections, and what it refused.
    pub fn admission(&self) -> crate::AdmissionStats {
        self.admission.stats()
    }
    pub fn local_addr(&self) -> Result<SocketAddr, WireError> {
        Ok(self.endpoint.local_addr()?)
    }
    pub fn close(&self) {
        self.endpoint.close(0u8.into(), b"shutdown");
    }
    pub async fn serve<H: RequestHandler + Clone>(&self, handler: H) -> Result<(), WireError> {
        require_runtime()?;
        // The connections borrow this server and its admission; each
        // serves its streams on tasks of its own.
        let mut connections = futures_util::stream::FuturesUnordered::new();
        loop {
            tokio::select! {
                Some(())=futures_util::StreamExt::next(&mut connections),if !connections.is_empty()=>{},
                incoming=self.endpoint.accept()=>{
                    let Some(incoming)=incoming else{break};
                    if connections.len() >= self.limits.max_connections {incoming.refuse();continue;}
                    let Ok(pending)=self.admission.begin() else {incoming.refuse();continue;};
                    let registry=self.registry.clone();let limits=self.limits.clone();let handler=handler.clone();
                    connections.push(async move {
                        let _ = transport_exchange(async {
                            let connection = tokio::time::timeout(limits.request_timeout, incoming)
                                .await.map_err(|_| WireError::Timeout)?
                                .map_err(|_| WireError::Connection)?;
                            serve_admitted_connection(connection, registry, limits, handler, pending).await
                        }).await;
                    });
                }
            }
        }
        drop(connections);
        Ok(())
    }
}
/// Serve an already established connection selected by an ALPN multiplexer.
/// The data protocol still requires a verified client certificate registered in
/// PeerRegistry; optional client authentication on another ALPN grants no access.
pub async fn serve_authenticated_connection<H: RequestHandler + Clone>(
    connection: Connection,
    registry: PeerRegistry,
    limits: WireLimits,
    handler: H,
) -> Result<(), WireError> {
    transport_exchange(serve_authenticated_connection_inner(
        connection, registry, limits, handler, None,
    ))
    .await
}
/// [`serve_authenticated_connection`] for a listener that admits by identity
/// (27 §3.1 P5): `pending` is the place the handshake held, exchanged for a
/// charge to the identity the certificate authenticates as.
pub async fn serve_admitted_connection<H: RequestHandler + Clone>(
    connection: Connection,
    registry: PeerRegistry,
    limits: WireLimits,
    handler: H,
    pending: crate::Pending<'_>,
) -> Result<(), WireError> {
    transport_exchange(serve_authenticated_connection_inner(
        connection,
        registry,
        limits,
        handler,
        Some(pending),
    ))
    .await
}
async fn serve_authenticated_connection_inner<H: RequestHandler + Clone>(
    connection: Connection,
    registry: PeerRegistry,
    limits: WireLimits,
    handler: H,
    pending: Option<crate::Pending<'_>>,
) -> Result<(), WireError> {
    limits.validate()?;
    let authenticated = (|| {
        let handshake = connection
            .handshake_data()
            .ok_or(WireError::Authentication)?
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .map_err(|_| WireError::Authentication)?;
        if handshake.protocol.as_deref() != Some(ALPN) {
            return Err(WireError::Authentication);
        }
        let identity = connection
            .peer_identity()
            .ok_or(WireError::Authentication)?
            .downcast::<Vec<CertificateDer<'static>>>()
            .map_err(|_| WireError::Authentication)?;
        let fingerprint =
            certificate_fingerprint(identity.first().ok_or(WireError::Authentication)?.as_ref());
        let peer = registry
            .authenticate(fingerprint)
            .map_err(|_| WireError::Authentication)?;
        Ok((fingerprint, peer))
    })();
    let (fingerprint, peer) = match authenticated {
        Ok(authenticated) => authenticated,
        Err(error) => {
            connection.close(1u8.into(), b"unauthorized");
            return Err(error);
        }
    };
    // Charged to the identity from here until this connection is served.
    let admitted = match pending
        .map(|pending| pending.authenticated(peer.principal(), peer.role(), &connection))
        .transpose()
    {
        Ok(admitted) => admitted,
        Err(_) => {
            connection.close(4u8.into(), b"capacity");
            return Err(WireError::Access(AccessError::Capacity));
        }
    };
    let handshake = async {
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .map_err(|_| WireError::Connection)?;
        let hello: Hello = read_frame(&mut recv, FrameKind::Hello, 4096).await?;
        require_end(&mut recv).await?;
        let negotiated = match limits.negotiate_native(
            &hello,
            handler.supports_managed_requests(),
            handler.supports_participant_requests(),
            handler.supports_native_requests(),
        ) {
            Ok(value) => value,
            Err(error) => {
                write_frame(
                    &mut send,
                    FrameKind::HelloReply,
                    &HelloReply::Rejected(error.clone()),
                    4096,
                )
                .await?;
                send.finish().map_err(|_| WireError::Connection)?;
                // Preserve the typed rejection until the peer acknowledges the
                // stream; dropping the last connection handle immediately can
                // otherwise race the response and expose only a connection loss.
                send.stopped().await.map_err(|_| WireError::Connection)?;
                return Err(WireError::Access(error));
            }
        };
        write_frame(
            &mut send,
            FrameKind::HelloReply,
            &HelloReply::Accepted(negotiated),
            4096,
        )
        .await?;
        send.finish().map_err(|_| WireError::Connection)?;
        Ok(negotiated)
    };
    let negotiated = tokio::time::timeout(limits.request_timeout, handshake)
        .await
        .map_err(|_| WireError::Timeout)??;
    let mut limits = limits;
    limits.max_frame_bytes = negotiated.max_frame_bytes;
    limits.max_items = negotiated.max_items;
    let task_limit = (limits.streams_per_connection as usize).saturating_add(2);
    let mut tasks = JoinSet::new();
    // What the answers under way on this connection have to send.
    let held = Arc::new(Held::default());
    loop {
        tokio::select! {
            Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
            streams=connection.accept_bi()=>{
                let Ok((mut send,mut recv))=streams else{break};
                if let Some(admitted) = &admitted { admitted.used(); }
                if tasks.len() >= task_limit {let _=send.reset(2u8.into());let _=recv.stop(2u8.into());continue;}
                let registry=registry.clone();let limits=limits.clone();let handler=handler.clone();let carrying=connection.clone();let held=held.clone();
                tasks.spawn(async move {
                    // Each part of an exchange has its own wait: what is
                    // asked as it arrives, its handler the time of a
                    // request (`dispatch`), and its answer as long as the
                    // path takes to carry it.
                    let work=async {
                        // Recheck registry on every stream, so revocation applies
                        // to already established authenticated connections.
                        let peer=registry.authenticate(fingerprint)?;
                        let header=tokio::time::timeout(limits.request_timeout,read_frame_header(&mut recv,FrameKind::Request,limits.max_frame_bytes))
                            .await.map_err(|_|WireError::Timeout)??;
                        let request:RequestEnvelope=read_payload_arriving(&mut recv,header,limits.request_timeout).await?;
                        tokio::time::timeout(limits.request_timeout,require_end(&mut recv))
                            .await.map_err(|_|WireError::Timeout)??;
                        send.set_priority(request.operation.class().priority()).map_err(|_|WireError::Connection)?;
                        let response = if negotiated.accepts_protocol(request.protocol) {
                            dispatch_accounted(&handler,peer,request,&limits).await
                        } else {
                            OwnedResponse::new(request.reply(Response::Error(AccessError::UnsupportedProtocol)))
                        };
                        let bytes=postcard::experimental::serialized_size(response.envelope()).map_err(|_|WireError::InvalidFrame)?;
                        carried(&carrying,&held,bytes,limits.request_timeout,send_owned_response(send,response,limits.max_frame_bytes)).await
                    };
                    let _=transport_exchange(work).await;
                });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
}

/// Reset buffered transport data before releasing its allowance on timeout,
/// cancellation or peer loss. Successful delivery disarms the reset after ACK.
struct ResponseDelivery {
    stream: quinn::SendStream,
    response: OwnedResponse,
    acknowledged: bool,
}
impl Drop for ResponseDelivery {
    fn drop(&mut self) {
        if !self.acknowledged {
            let _ = self.stream.reset(3u8.into());
        }
    }
}
async fn send_owned_response(
    stream: quinn::SendStream,
    response: OwnedResponse,
    limit: u32,
) -> Result<(), WireError> {
    let mut delivery = ResponseDelivery {
        stream,
        response,
        acknowledged: false,
    };
    write_frame(
        &mut delivery.stream,
        FrameKind::Response,
        delivery.response.envelope(),
        limit,
    )
    .await?;
    delivery
        .stream
        .finish()
        .map_err(|_| WireError::Connection)?;
    if delivery
        .stream
        .stopped()
        .await
        .map_err(|_| WireError::Connection)?
        .is_some()
    {
        return Err(WireError::Connection);
    }
    delivery.acknowledged = true;
    Ok(())
}

pub struct QuicConnector {
    endpoint: Endpoint,
    limits: WireLimits,
    /// The identity presented on every connection opened from now on; a
    /// renewal replaces it without rebinding the endpoint.
    tls: std::sync::RwLock<quinn::ClientConfig>,
}
impl QuicConnector {
    pub fn limits(&self) -> &WireLimits {
        &self.limits
    }
    pub fn bind(
        address: SocketAddr,
        tls: quinn::ClientConfig,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        let endpoint = transport_setup(|| Ok(Endpoint::client(address)?))?;
        Ok(Self {
            endpoint,
            limits,
            tls: std::sync::RwLock::new(tls),
        })
    }
    /// Present another client identity on every connection opened from now
    /// on; connections already open keep the identity they were opened with.
    pub fn replace_tls(&self, tls: quinn::ClientConfig) -> Result<(), WireError> {
        *self.tls.write().map_err(|_| WireError::Connection)? = tls;
        Ok(())
    }
    /// A clonable handle that opens connections with the identity presented
    /// now; a peer pool's detached dial holds one so a caller giving up under
    /// its own deadline neither abandons the dial nor keeps the connector.
    pub fn dialer(&self) -> Result<QuicDialer, WireError> {
        Ok(QuicDialer {
            endpoint: self.endpoint.clone(),
            limits: self.limits.clone(),
            tls: self.tls.read().map_err(|_| WireError::Connection)?.clone(),
        })
    }
    pub async fn connect(
        &self,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<QuicRemote, WireError> {
        let tls = self.tls.read().map_err(|_| WireError::Connection)?.clone();
        transport_exchange(open_remote(
            &self.endpoint,
            tls,
            &self.limits,
            address,
            server_name,
        ))
        .await
    }
}
/// The identity and limits a [`QuicConnector`] presented when the handle was
/// taken; connections it opens are the connector's own (same endpoint, same
/// certificate check by `server_name`).
#[derive(Clone)]
pub struct QuicDialer {
    endpoint: Endpoint,
    limits: WireLimits,
    tls: quinn::ClientConfig,
}
impl QuicDialer {
    pub async fn connect(
        &self,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<QuicRemote, WireError> {
        transport_exchange(open_remote(
            &self.endpoint,
            self.tls.clone(),
            &self.limits,
            address,
            server_name,
        ))
        .await
    }
}
async fn open_remote(
    endpoint: &Endpoint,
    tls: quinn::ClientConfig,
    limits: &WireLimits,
    address: SocketAddr,
    server_name: &str,
) -> Result<QuicRemote, WireError> {
    let connecting = endpoint
        .connect_with(tls, address, server_name)
        .map_err(|_| WireError::Connection)?;
    let connection = tokio::time::timeout(limits.request_timeout, connecting)
        .await
        .map_err(|_| WireError::Timeout)?
        .map_err(|_| WireError::Authentication)?;
    let handshake = async {
        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(|_| WireError::Connection)?;
        let hello = Hello {
            versions: vec![
                crate::NATIVE_PROTOCOL_VERSION,
                PEER_PROTOCOL_VERSION,
                MANAGED_PROTOCOL_VERSION,
                PROTOCOL_VERSION,
            ],
            max_frame_bytes: limits.max_frame_bytes,
            max_items: limits.max_items,
        };
        write_frame(&mut send, FrameKind::Hello, &hello, 4096).await?;
        send.finish().map_err(|_| WireError::Connection)?;
        let reply: HelloReply = read_frame(&mut recv, FrameKind::HelloReply, 4096).await?;
        require_end(&mut recv).await?;
        match reply {
            HelloReply::Accepted(value) => Ok(value),
            HelloReply::Rejected(error) => Err(WireError::Access(error)),
        }
    };
    let negotiated = tokio::time::timeout(limits.request_timeout, handshake)
        .await
        .map_err(|_| WireError::Timeout)??;
    if !matches!(
        negotiated.protocol,
        PROTOCOL_VERSION
            | MANAGED_PROTOCOL_VERSION
            | PEER_PROTOCOL_VERSION
            | crate::NATIVE_PROTOCOL_VERSION
    ) || negotiated.max_frame_bytes > limits.max_frame_bytes
        || negotiated.max_items > limits.max_items
    {
        return Err(WireError::InvalidFrame);
    }
    Ok(QuicRemote {
        connection,
        negotiated,
        limits: limits.clone(),
        _endpoint: endpoint.clone(),
        capacity: Arc::new(RemoteCapacity {
            data: Semaphore::new(limits.streams_per_connection as usize),
            control: Semaphore::new(limits.control_streams as usize),
            held: Held::default(),
        }),
    })
}
#[derive(Clone)]
pub struct QuicRemote {
    connection: Connection,
    negotiated: Negotiated,
    limits: WireLimits,
    _endpoint: Endpoint,
    // All clones of one physical connection must share its stream admission.
    capacity: Arc<RemoteCapacity>,
}
struct RemoteCapacity {
    data: Semaphore,
    control: Semaphore,
    /// What the requests under way on the connection have to send.
    held: Held,
}
impl QuicRemote {
    pub fn negotiated(&self) -> Negotiated {
        self.negotiated
    }
    pub fn close(&self) {
        self.connection.close(0u8.into(), b"client closed");
    }
    /// What the law of the connection holds in flight, in bytes.
    pub fn window(&self) -> u64 {
        self.connection.stats().path.cwnd
    }
    pub async fn request(&self, request: &RequestEnvelope) -> Result<ResponseEnvelope, WireError> {
        self.request_within(request, self.limits.request_timeout)
            .await
    }
    /// An exchange whose peer is given `period` to answer what it has
    /// been asked, and whose request and answer are given as long as the
    /// path takes to carry them ([`carried`], [`read_payload_arriving`]).
    pub async fn request_within(
        &self,
        request: &RequestEnvelope,
        period: std::time::Duration,
    ) -> Result<ResponseEnvelope, WireError> {
        transport_exchange(self.request_inner(request, period)).await
    }
    async fn request_inner(
        &self,
        request: &RequestEnvelope,
        period: std::time::Duration,
    ) -> Result<ResponseEnvelope, WireError> {
        if !self.negotiated.accepts_protocol(request.protocol) {
            return Err(WireError::Access(AccessError::UnsupportedProtocol));
        }
        let lane = if matches!(request.operation, Operation::Raft { .. }) {
            &self.capacity.control
        } else {
            &self.capacity.data
        };
        // The request waits its turn on the lane, no longer than the time
        // its peer is given: a burst of a group's messages is carried in
        // order, never refused for the lane being full at that instant.
        let _permit = tokio::time::timeout(period, lane.acquire())
            .await
            .map_err(|_| WireError::Limit)?
            .map_err(|_| WireError::Connection)?;
        let bytes = postcard::experimental::serialized_size(request)
            .map_err(|_| WireError::InvalidFrame)?;
        // The answer begins once the request has been carried and the
        // peer has answered it.
        let asked = async {
            let (mut send, mut recv) = self
                .connection
                .open_bi()
                .await
                .map_err(|_| WireError::Connection)?;
            send.set_priority(request.operation.class().priority())
                .map_err(|_| WireError::Connection)?;
            write_frame(
                &mut send,
                FrameKind::Request,
                request,
                self.negotiated.max_frame_bytes,
            )
            .await?;
            send.finish().map_err(|_| WireError::Connection)?;
            let header = read_frame_header(
                &mut recv,
                FrameKind::Response,
                self.negotiated.max_frame_bytes,
            )
            .await?;
            Ok((recv, header))
        };
        let (mut recv, header) =
            carried(&self.connection, &self.capacity.held, bytes, period, asked).await?;
        let response: ResponseEnvelope = read_payload_arriving(&mut recv, header, period).await?;
        tokio::time::timeout(period, require_end(&mut recv))
            .await
            .map_err(|_| WireError::Timeout)??;
        validate_response(request, &response, None, &self.limits)?;
        Ok(response)
    }
}
