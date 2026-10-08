//! Mutually authenticated TLS 1.3 over Quinn. One bounded request occupies one
//! bidirectional stream; slow work on one stream does not serialize other streams.
use crate::*;
use focal_memory::MemoryBudget;
use quinn::{Connection, Endpoint};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};
use tokio::{
    sync::{Semaphore, watch},
    task::JoinSet,
};

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
// Quinn/rustls retain shared configurations across their internal connection
// tasks; these Arc types are required by those libraries' public APIs.
/// Longest silence before a QUIC connection is considered dead: what the
/// transport reclaims a vanished peer's connection by, with a keep-alive every
/// quarter of it, so a live peer is heard from several times in every window
/// however late its scheduler runs it. It is never a request's deadline.
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
struct Held {
    bytes: std::sync::atomic::AtomicU64,
    /// The longest round trip a carriage on the connection has seen while
    /// it waited: what every carriage's residency was judged by, for a
    /// measurer of the connection (`QuicRemote::longest_round_trip`).
    longest: std::sync::atomic::AtomicU64,
}
/// One connection's delivery for its readers (`frame::Delivery`): what the
/// connection received, and what each class declared and read.
struct ConnectionDelivery<'a> {
    connection: &'a Connection,
    counts: &'a crate::frame::Counts,
}
impl crate::frame::Delivery for ConnectionDelivery<'_> {
    fn received(&self) -> u64 {
        self.connection.stats().udp_rx.bytes
    }
    fn delivered(&self, rank: u8) -> u64 {
        self.counts.delivered(rank)
    }
    fn backlog(&self, rank: u8) -> u64 {
        self.counts.backlog(rank)
    }
    fn declared(&self, rank: u8, bytes: u64) {
        self.counts.declared(rank, bytes);
    }
    fn read(&self, rank: u8, bytes: u64) {
        self.counts.read(rank, bytes);
    }
    fn released(&self, rank: u8, bytes: u64) {
        self.counts.released(rank, bytes);
    }
    fn gave_up(&self, reason: crate::frame::GiveUp) {
        self.counts.gave_up(reason);
    }
}
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
        let mut before = self.bytes.load(Ordering::Acquire);
        loop {
            let after = before.saturating_add(bytes);
            match self.bytes.compare_exchange_weak(
                before,
                after,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
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
    /// A carriage saw this round trip while it waited.
    fn saw(&self, round_trip: std::time::Duration) {
        let nanos = u64::try_from(round_trip.as_nanos()).unwrap_or(u64::MAX);
        self.longest
            .fetch_max(nanos, std::sync::atomic::Ordering::AcqRel);
    }
    fn longest(&self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.longest.load(std::sync::atomic::Ordering::Acquire))
    }
    fn now(&self) -> u64 {
        self.bytes.load(std::sync::atomic::Ordering::Acquire)
    }
}
impl Drop for Sending<'_> {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        let mut before = self.held.bytes.load(Ordering::Acquire);
        while let Err(found) = self.held.bytes.compare_exchange_weak(
            before,
            before.saturating_sub(self.bytes),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            before = found;
        }
    }
}
/// The time an exchange is given to send what it has to send, told by its
/// own stream and never by the connection's counters (the audit's F38): what
/// a connection has sent is in flight, sent again or another stream's, and
/// says nothing of what one stream delivered. An exchange credited with it
/// was given its peer's time to answer while its last window was still on
/// the path, and given up on a path that carried it all the while; and one
/// its peer had stopped reading was kept for as long as the others of its
/// connection sent a datagram a period.
///
/// A sender knows one thing of its own stream: that the peer has
/// acknowledged all of it, or has stopped taking it
/// ([`Carriage::acknowledged`]). Until then there is nothing to see. What
/// the stream takes of what is written says little: it takes a window at
/// once, and more only as the peer's reading lets it, an eighth of a window
/// at a time, which on a narrow path is longer than any period. So the
/// wait ends by a bound and not by a guess at delivery: the [`residency`]
/// of what the exchanges under way on the connection had to send beside
/// this one (`held`), at the longest round trip the path has shown — the
/// least a live path delivers, and what the receiver holds the same bytes
/// to. A connection whose path carries nothing ends by its own idle
/// timeout, and its streams with it; a stream its peer does not read, or
/// one starved by the others of its connection, ends here.
struct Carriage<'a> {
    connection: &'a Connection,
    held: &'a Held,
    owed: u64,
    period: std::time::Duration,
    began: tokio::time::Instant,
    longest: std::time::Duration,
    _sending: Sending<'a>,
}
/// How a stream that was written whole ended its wait.
enum Carried<T> {
    /// The peer has all of it (`None`), or stopped taking it and said why.
    Stream(Option<quinn::VarInt>),
    /// What was waited for beside it came first.
    Answered(T),
}
impl<'a> Carriage<'a> {
    fn begin(
        connection: &'a Connection,
        held: &'a Held,
        bytes: usize,
        period: std::time::Duration,
    ) -> Self {
        let (sending, owed) = held.send(bytes);
        Self {
            connection,
            held,
            owed,
            period,
            began: tokio::time::Instant::now(),
            longest: std::time::Duration::ZERO,
            _sending: sending,
        }
    }
    /// How long the next wait may be: a period, or what is left of the
    /// time the path is given; `Timeout` once none is.
    fn wait(&mut self) -> Result<std::time::Duration, WireError> {
        self.longest = self.longest.max(self.connection.rtt());
        self.held.saw(self.longest);
        // What came to the connection since is sent in turn with this.
        self.owed = self.owed.max(self.held.now());
        let given = residency(
            usize::try_from(self.owed).unwrap_or(usize::MAX),
            self.longest,
        )
        .max(self.period);
        let left = given.saturating_sub(self.began.elapsed());
        if left.is_zero() {
            return Err(WireError::Timeout);
        }
        Ok(left.min(self.period))
    }
    /// The longest round trip the path has shown while this was carried.
    fn longest(&self) -> std::time::Duration {
        self.longest.max(self.connection.rtt())
    }
    /// Wait for `write`, which writes to the stream: given up when the
    /// path's time is spent.
    async fn written<T>(
        &mut self,
        write: impl Future<Output = Result<T, WireError>>,
    ) -> Result<T, WireError> {
        let mut write = std::pin::pin!(write);
        loop {
            let wait = self.wait()?;
            if let Ok(done) = tokio::time::timeout(wait, write.as_mut()).await {
                return done;
            }
        }
    }
    /// Wait until the peer has acknowledged all of a stream that was
    /// written and ended, or stopped it, or `beside` is done, whichever is
    /// first; given up when the path's time is spent.
    async fn acknowledged<T>(
        &mut self,
        stream: &quinn::SendStream,
        beside: std::pin::Pin<&mut impl Future<Output = Result<T, WireError>>>,
    ) -> Result<Carried<T>, WireError> {
        let mut stopped = std::pin::pin!(stream.stopped());
        let mut beside = beside;
        loop {
            let wait = self.wait()?;
            tokio::select! {
                biased;
                done = beside.as_mut() => return done.map(Carried::Answered),
                ended = &mut stopped => {
                    return ended.map(Carried::Stream).map_err(|_| WireError::Connection);
                }
                () = tokio::time::sleep(wait) => {}
            }
        }
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
    // The connection's liveness is its own, never a request's deadline. A
    // request ends at its deadline (`request_timeout`) and the connection
    // carrying it stays; keep-alive pings hold a healthy connection open, and
    // only a peer silent for the whole idle bound is gone. A restarted peer is
    // told sooner by its stateless reset (RFC 9000 §10.3), and node death is
    // the membership's to decide (SWIM with Lifeguard), not the transport's.
    // Bounding the idle period by the request timeout made a short deadline a
    // short liveness window: a process stalled for half a second under load
    // lost every connection it held, healthy or not (a busy machine's
    // focal-wire run, 2026-10-07). Quinn floors the bound at three PTOs
    // (RFC 9000 §10.1).
    let idle = IDLE_TIMEOUT;
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
    let provider = Arc::new(crate::crypto_provider());
    // A client's chain is verified against the roots this server knows;
    // a successor issuer's endorsement by one of them is an ordinary
    // intermediate to it (`trust`).
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(crate::TrustRoots::new(client_roots)?.store()?),
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
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crate::quic_server(tls)?));
    config.transport_config(transport);
    Ok(config)
}
pub fn client_tls(
    identity: TlsIdentity,
    server_roots: Vec<Vec<u8>>,
    limits: &WireLimits,
) -> Result<quinn::ClientConfig, WireError> {
    limits.validate()?;
    let provider = Arc::new(crate::crypto_provider());
    // The server's chain is verified against the roots this client knows;
    // a successor issuer's endorsement by one of them is an ordinary
    // intermediate to it (`trust`).
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(crate::TrustRoots::new(server_roots)?.store()?),
        provider.clone(),
    )
    .build()
    .map_err(|_| WireError::Authentication)?;
    client_tls_with(identity, verifier, provider, limits)
}
/// [`client_tls`] for a client that adopts an issuer a verified chain
/// carried endorsed and it did not hold (24 §11): the configuration, and
/// where the adoption is read.
pub fn client_tls_adopting(
    identity: TlsIdentity,
    server_roots: Vec<Vec<u8>>,
    limits: &WireLimits,
) -> Result<(quinn::ClientConfig, crate::AdoptedRoots), WireError> {
    limits.validate()?;
    let provider = Arc::new(crate::crypto_provider());
    let (verifier, adopted) = crate::AdoptingServerVerifier::new(
        crate::TrustRoots::new(server_roots)?,
        provider.clone(),
    )?;
    Ok((
        client_tls_with(identity, verifier, provider, limits)?,
        adopted,
    ))
}
fn client_tls_with(
    identity: TlsIdentity,
    verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
    provider: Arc<rustls::crypto::CryptoProvider>,
    limits: &WireLimits,
) -> Result<quinn::ClientConfig, WireError> {
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| WireError::Authentication)?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(identity.certificate_chain, identity.private_key)
        .map_err(|_| WireError::Authentication)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    tls.enable_early_data = false;
    let mut config = quinn::ClientConfig::new(Arc::new(crate::quic_client(tls)?));
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
    /// `budget` funds the request bodies the server admits (the audit's
    /// F03): a body is permitted from it before it is allocated.
    pub fn bind(
        address: SocketAddr,
        tls: quinn::ServerConfig,
        registry: PeerRegistry,
        limits: WireLimits,
        budget: MemoryBudget,
    ) -> Result<Self, WireError> {
        let admission = crate::AdmissionLimits::for_connections(limits.max_connections);
        Self::bind_admitting(address, tls, registry, limits, admission, budget)
    }
    /// [`Self::bind`] with the admission bounds stated.
    pub fn bind_admitting(
        address: SocketAddr,
        tls: quinn::ServerConfig,
        registry: PeerRegistry,
        limits: WireLimits,
        admission: crate::AdmissionLimits,
        budget: MemoryBudget,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        let admission = crate::Admission::new(admission, budget).map_err(|_| WireError::Limit)?;
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
                    // Handshakes are bounded by their pending places and
                    // connections by the admission's total, met after the
                    // replacement rule (the audit's F20); a source that has
                    // not proven its address takes no place while half of
                    // them are taken (RFC 9000 §8.1.2, Retry under load).
                    if validate_address(&incoming, &self.admission) { let _ = incoming.retry(); continue; }
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
    pending: crate::Pending,
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
/// Whether `incoming` should prove its address before it takes a pending
/// place: it has not yet, and half the places are taken.
pub fn validate_address(incoming: &quinn::Incoming, admission: &crate::Admission) -> bool {
    if incoming.remote_address_validated() || !incoming.may_retry() {
        return false;
    }
    admission.stats().pending.saturating_mul(2) >= admission.limits().pending
}
async fn serve_authenticated_connection_inner<H: RequestHandler + Clone>(
    connection: Connection,
    registry: PeerRegistry,
    limits: WireLimits,
    handler: H,
    pending: Option<crate::Pending>,
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
        let certificate = identity.first().ok_or(WireError::Authentication)?.as_ref();
        let fingerprint = certificate_fingerprint(certificate);
        // The certificate the projection names, or a renewal of an enrolled
        // key it has not applied yet (24 §11).
        let peer = registry
            .authenticate_certificate(certificate)
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
    // Held under its certificate until served: a revocation closes it.
    let _live = match registry.attach(fingerprint, &connection, limits.max_connections) {
        Ok(live) => live,
        Err(error) => {
            connection.close(4u8.into(), b"capacity");
            return Err(WireError::Access(error));
        }
    };
    let lane = admitted.as_ref().map(crate::Admitted::lane);
    let handshake = async {
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .map_err(|_| WireError::Connection)?;
        let hello: Hello = read_frame(&mut recv, FrameKind::Hello, 4096).await?;
        require_end(&mut recv).await?;
        let negotiated = match limits.negotiate_ordered(
            &hello,
            handler.supports_managed_requests(),
            handler.supports_participant_requests(),
            handler.supports_native_requests(),
            handler.supports_ordered_replication(),
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
    let counts = Arc::new(crate::frame::Counts::default());
    loop {
        tokio::select! {
            Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
            streams=connection.accept_bi()=>{
                let Ok((mut send,mut recv))=streams else{break};
                if let Some(admitted) = &admitted { admitted.used(); }
                if tasks.len() >= task_limit {let _=send.reset(2u8.into());let _=recv.stop(2u8.into());continue;}
                let registry=registry.clone();let limits=limits.clone();let handler=handler.clone();let carrying=connection.clone();let held=held.clone();let lane=lane.clone();let counts=counts.clone();
                tasks.spawn(async move {
                    // Each part of an exchange has its own wait: what is
                    // asked as it arrives, its handler the time of a
                    // request (`dispatch`), and its answer as long as the
                    // path takes to carry it.
                    let work=async {
                        // The grant is looked up twice: here, so that a
                        // revoked certificate is refused before anything
                        // is read for it, and once the whole request has
                        // arrived, so that what is dispatched is
                        // authorized by the grant current then, never by
                        // one captured before its body (the audit's F35).
                        registry.granted(fingerprint)?;
                        let header=tokio::time::timeout(limits.request_timeout,read_frame_header(&mut recv,FrameKind::Request,limits.max_frame_bytes))
                            .await.map_err(|_|WireError::Timeout)??;
                        // The body is permitted before any of it is
                        // allocated (F03): the identity's share of the
                        // listener's ingress, from its budget. A refusal
                        // is a reset the peer sees as capacity.
                        let ingress = match lane.as_ref().map(|lane| lane.take(header.payload_bytes(), usize::try_from(limits.max_frame_bytes).unwrap_or(usize::MAX))).transpose() {
                            Ok(ingress) => ingress,
                            Err(_) => {
                                let _=send.reset(5u8.into());let _=recv.stop(5u8.into());
                                return Err(WireError::Access(AccessError::Capacity));
                            }
                        };
                        // A request's class is in its body: until it is read it is the most urgent.
                        let delivery=ConnectionDelivery{connection:&carrying,counts:&counts};
                        let request:RequestEnvelope=read_payload_arriving(&mut recv,header,limits.request_timeout,||carrying.rtt(),&delivery,0).await?;
                        // The end of the stream is the last thing the path carries of it.
                        tokio::time::timeout(limits.request_timeout.max(residency(1,carrying.rtt())),require_end(&mut recv))
                            .await.map_err(|_|WireError::Timeout)??;
                        let peer=registry.authenticate(fingerprint)?;
                        send.set_priority(request.operation.class().priority()).map_err(|_|WireError::Connection)?;
                        let response = if negotiated.accepts_protocol(request.protocol) {
                            dispatch_accounted_by(
                                &handler,
                                peer,
                                request,
                                &limits,
                                carrying.rtt(),
                            )
                            .await
                        } else {
                            OwnedResponse::new(request.reply(Response::Error(AccessError::UnsupportedProtocol)))
                        };
                        // The body was consumed by its dispatch.
                        drop(ingress);
                        let bytes=postcard::experimental::serialized_size(response.envelope()).map_err(|_|WireError::InvalidFrame)?;
                        send_owned_response(Carriage::begin(&carrying,&held,bytes,limits.request_timeout),send,response,limits.max_frame_bytes).await
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
/// An answer is given as long as the path takes to carry it, told by its
/// own stream ([`Carriage`]).
async fn send_owned_response(
    mut carriage: Carriage<'_>,
    stream: quinn::SendStream,
    response: OwnedResponse,
    limit: u32,
) -> Result<(), WireError> {
    let mut delivery = ResponseDelivery {
        stream,
        response,
        acknowledged: false,
    };
    carriage
        .written(write_frame(
            &mut delivery.stream,
            FrameKind::Response,
            delivery.response.envelope(),
            limit,
        ))
        .await?;
    delivery
        .stream
        .finish()
        .map_err(|_| WireError::Connection)?;
    let nothing = std::pin::pin!(std::future::pending::<Result<(), WireError>>());
    match carriage.acknowledged(&delivery.stream, nothing).await? {
        Carried::Stream(None) => {}
        Carried::Stream(Some(_)) | Carried::Answered(()) => return Err(WireError::Connection),
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
    /// The profiles offered in every Hello, the newest first: all this
    /// binary speaks, unless [`Self::offering`] made it an older one.
    offers: Vec<u16>,
}
/// Every profile this binary speaks, the newest first: what a connector
/// offers in its Hello.
pub const OFFERED_PROTOCOLS: [u16; 5] = [
    crate::ORDERED_PROTOCOL_VERSION,
    crate::NATIVE_PROTOCOL_VERSION,
    PEER_PROTOCOL_VERSION,
    MANAGED_PROTOCOL_VERSION,
    PROTOCOL_VERSION,
];
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
            offers: OFFERED_PROTOCOLS.to_vec(),
        })
    }
    /// A connector that offers only `versions` in its Hello: what a
    /// connector of an older binary offers, for a fleet's tests of a mixed
    /// window (27 §12). The base profile is always among them.
    pub fn offering(mut self, versions: &[u16]) -> Result<Self, WireError> {
        if versions.is_empty()
            || versions.len() > OFFERED_PROTOCOLS.len()
            || !versions.contains(&PROTOCOL_VERSION)
            || versions
                .iter()
                .any(|version| !OFFERED_PROTOCOLS.contains(version))
        {
            return Err(WireError::Limit);
        }
        self.offers = versions.to_vec();
        Ok(self)
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
            offers: self.offers.clone(),
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
            &self.offers,
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
    offers: Vec<u16>,
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
            &self.offers,
            address,
            server_name,
        ))
        .await
    }
}
/// The connections a participant keeps to the routes it reaches — its
/// initial endpoint and the hints its requests are redirected to — one per
/// route, dialed once (the audit's F60). Concurrent cold calls to one route
/// wait on the one dial in flight instead of each dialing, which opened as
/// many connections as callers and let the server's per-identity limit
/// replace the earlier ones under calls already dispatched. A dial runs on
/// its own task, so a caller giving up under its own deadline neither
/// abandons it nor keeps the others waiting, and a connection leaves the
/// cache only when the generation that failed is still the one cached.
pub struct RouteConnections {
    connector: QuicConnector,
    max_routes: usize,
    routes: std::sync::Mutex<Routes>,
    dials: std::sync::atomic::AtomicU64,
}
/// A connection the cache handed out, with the generation it holds it
/// under: a request that fails on it names the generation when it asks for
/// the route to be forgotten.
pub struct Connected {
    pub remote: QuicRemote,
    pub generation: u64,
}
#[derive(Clone)]
enum DialOutcome {
    Pending,
    Connected(QuicRemote),
    Failed,
}
/// A dial in flight for a route. Whoever calls the route while it dials
/// waits on it, holding nothing of the cache but a receiver, and then takes
/// its turn on the connection's lanes as any request does. The wait is
/// bounded by the dial's own deadline: the dial resolves, connects and
/// greets under one `request_timeout` each, and its task always speaks —
/// or, dropped with its runtime, closes the channel, which a waiter reads
/// as the dial failing.
struct RouteDial {
    state: watch::Receiver<DialOutcome>,
}
struct CachedRoute {
    remote: QuicRemote,
    used: u64,
    generation: u64,
}
type RouteKey = (String, String);
#[derive(Default)]
struct Routes {
    entries: BTreeMap<RouteKey, CachedRoute>,
    dialing: BTreeMap<RouteKey, RouteDial>,
    clock: u64,
    generations: u64,
}
impl Routes {
    fn store(&mut self, key: &RouteKey, remote: QuicRemote) -> Connected {
        self.clock = self.clock.saturating_add(1);
        if let Some(entry) = self.entries.get_mut(key) {
            entry.used = self.clock;
            return Connected {
                remote: entry.remote.clone(),
                generation: entry.generation,
            };
        }
        self.generations = self.generations.saturating_add(1);
        let generation = self.generations;
        self.entries.insert(
            key.clone(),
            CachedRoute {
                remote: remote.clone(),
                used: self.clock,
                generation,
            },
        );
        Connected { remote, generation }
    }
}
impl RouteConnections {
    pub fn new(connector: QuicConnector, max_routes: usize) -> Result<Self, WireError> {
        if max_routes == 0 || max_routes > 1024 {
            return Err(WireError::Limit);
        }
        Ok(Self {
            connector,
            max_routes,
            routes: std::sync::Mutex::new(Routes::default()),
            dials: std::sync::atomic::AtomicU64::new(0),
        })
    }
    pub fn limits(&self) -> &WireLimits {
        self.connector.limits()
    }
    /// Physical dials this cache has started since it opened.
    pub fn dials(&self) -> u64 {
        self.dials.load(std::sync::atomic::Ordering::Acquire)
    }
    /// The connection to `endpoint` under `server_name`: the cached one, the
    /// one being dialed once it is there, or a fresh dial. A caller waits on
    /// at most two dials — the one in flight when it arrived and, should
    /// that one fail, its own — and the number of callers waiting on a dial
    /// is the caller's own concurrency: the cache holds nothing per waiter.
    pub async fn connect(&self, endpoint: &str, server_name: &str) -> Result<Connected, WireError> {
        tokio::runtime::Handle::try_current().map_err(|_| WireError::Connection)?;
        if endpoint.len() > 512 || server_name.len() > 253 {
            return Err(WireError::Limit);
        }
        let key: RouteKey = (endpoint.to_owned(), server_name.to_owned());
        for _ in 0..2 {
            let mut receiver = {
                let mut routes = self.routes.lock().map_err(|_| WireError::Connection)?;
                routes.clock = routes.clock.saturating_add(1);
                let clock = routes.clock;
                if let Some(entry) = routes.entries.get_mut(&key) {
                    entry.used = clock;
                    return Ok(Connected {
                        remote: entry.remote.clone(),
                        generation: entry.generation,
                    });
                }
                // A dial that ended while nobody watched settles now.
                let settled = routes
                    .dialing
                    .get(&key)
                    .map(|dial| dial.state.borrow().clone());
                match settled {
                    Some(DialOutcome::Connected(remote)) => {
                        routes.dialing.remove(&key);
                        return Ok(routes.store(&key, remote));
                    }
                    Some(DialOutcome::Failed) => {
                        routes.dialing.remove(&key);
                    }
                    Some(DialOutcome::Pending) | None => {}
                }
                if let Some(dial) = routes.dialing.get(&key) {
                    dial.state.clone()
                } else {
                    // A new route: room for it, or the least recently used
                    // cached one leaves; dials in flight hold their room.
                    if routes.entries.len().saturating_add(routes.dialing.len()) >= self.max_routes
                    {
                        let evict = routes
                            .entries
                            .iter()
                            .min_by_key(|(_, cached)| cached.used)
                            .map(|(evict_key, _)| evict_key.clone());
                        match evict {
                            Some(evict) => {
                                routes.entries.remove(&evict);
                            }
                            None => return Err(WireError::Limit),
                        }
                    }
                    let dialer = self.connector.dialer()?;
                    let period = self.connector.limits().request_timeout;
                    let (sender, receiver) = watch::channel(DialOutcome::Pending);
                    self.dials.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    let (endpoint, server_name) = key.clone();
                    tokio::spawn(async move {
                        // The resolver answers under the same deadline the
                        // connection and the greeting each have, so the dial
                        // ends — and its waiters with it — within three.
                        let outcome = async {
                            let mut addresses = tokio::time::timeout(
                                period,
                                tokio::net::lookup_host(endpoint.as_str()),
                            )
                            .await
                            .map_err(|_| WireError::Timeout)?
                            .map_err(|_| WireError::Connection)?;
                            let address = addresses.next().ok_or(WireError::Connection)?;
                            dialer.connect(address, &server_name).await
                        }
                        .await;
                        sender.send_replace(match outcome {
                            Ok(remote) => DialOutcome::Connected(remote),
                            Err(_) => DialOutcome::Failed,
                        });
                    });
                    routes.dialing.insert(
                        key.clone(),
                        RouteDial {
                            state: receiver.clone(),
                        },
                    );
                    receiver
                }
            };
            // Wait for the dial to end, under its deadline; the lock is
            // never held across the wait.
            loop {
                let outcome = receiver.borrow_and_update().clone();
                match outcome {
                    DialOutcome::Pending => {
                        if receiver.changed().await.is_err() {
                            // The dial's task ended without a word: treat
                            // it as failed and dial again.
                            let mut routes =
                                self.routes.lock().map_err(|_| WireError::Connection)?;
                            routes.dialing.remove(&key);
                            break;
                        }
                    }
                    DialOutcome::Connected(remote) => {
                        let mut routes = self.routes.lock().map_err(|_| WireError::Connection)?;
                        if routes.dialing.remove(&key).is_some() {
                            // This caller settles the dial: its connection
                            // is the route's.
                            return Ok(routes.store(&key, remote));
                        }
                        // Another caller settled it first: what it cached
                        // serves — unless the connection already failed and
                        // was forgotten, in which case it is not cached
                        // again here; this caller dials afresh.
                        routes.clock = routes.clock.saturating_add(1);
                        let clock = routes.clock;
                        if let Some(entry) = routes.entries.get_mut(&key) {
                            entry.used = clock;
                            return Ok(Connected {
                                remote: entry.remote.clone(),
                                generation: entry.generation,
                            });
                        }
                        break;
                    }
                    DialOutcome::Failed => {
                        let mut routes = self.routes.lock().map_err(|_| WireError::Connection)?;
                        routes.dialing.remove(&key);
                        break;
                    }
                }
            }
        }
        Err(WireError::Connection)
    }
    /// Forget the route's connection a request found failed — only while it
    /// is still the one cached; a newer connection another caller opened
    /// since stays.
    pub fn forget(&self, endpoint: &str, server_name: &str, generation: u64) {
        let key: RouteKey = (endpoint.to_owned(), server_name.to_owned());
        if let Ok(mut routes) = self.routes.lock()
            && routes
                .entries
                .get(&key)
                .is_some_and(|entry| entry.generation == generation)
        {
            routes.entries.remove(&key);
        }
    }
}
async fn open_remote(
    endpoint: &Endpoint,
    tls: quinn::ClientConfig,
    limits: &WireLimits,
    offers: &[u16],
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
            versions: offers.to_vec(),
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
            | crate::ORDERED_PROTOCOL_VERSION
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
            counts: crate::frame::Counts::default(),
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
    /// What the answers under way declared and delivered, by class.
    counts: crate::frame::Counts,
}
impl QuicRemote {
    pub fn negotiated(&self) -> Negotiated {
        self.negotiated
    }
    pub fn close(&self) {
        self.connection.close(0u8.into(), b"client closed");
    }
    /// Whether the connection has ended, by either side or by its path.
    pub fn closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }
    /// What the law of the connection holds in flight, in bytes.
    pub fn window(&self) -> u64 {
        self.connection.stats().path.cwnd
    }
    /// The round trip the connection measures of its path.
    pub fn round_trip(&self) -> std::time::Duration {
        self.connection.rtt()
    }
    /// The longest round trip any exchange carried on this connection was
    /// judged by, and the path's now: what bounds, in the path's terms,
    /// how long an exchange of so many bytes was given.
    pub fn longest_round_trip(&self) -> std::time::Duration {
        self.capacity.held.longest().max(self.connection.rtt())
    }
    /// Bytes the connection has received so far, of anything: what a body's
    /// arrival is judged by (`Arriving::judge`), for a measurer of the
    /// connection.
    pub fn received(&self) -> u64 {
        self.connection.stats().udp_rx.bytes
    }
    /// The bodies this connection's readers gave up, by reason
    /// (`frame::GiveUp`): a quiet connection, or a peer withholding a body
    /// while it delivers others.
    pub fn given_up(&self) -> crate::frame::GiveUps {
        self.capacity.counts.given_up()
    }
    pub async fn request(&self, request: &RequestEnvelope) -> Result<ResponseEnvelope, WireError> {
        self.request_within(request, self.limits.request_timeout)
            .await
    }
    /// An exchange whose peer is given `period` to answer what it has
    /// been asked, and whose request and answer are given as long as the
    /// path takes to carry them ([`Carriage`], [`read_payload_arriving`]).
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
        let lane = if matches!(
            request.operation,
            Operation::Raft { .. } | Operation::RaftOrdered { .. }
        ) {
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
        let mut carriage = Carriage::begin(&self.connection, &self.capacity.held, bytes, period);
        // The lanes keep the streams under way within what the peer lets a
        // connection open, so a stream is there to be had at once.
        let (mut send, mut recv) = tokio::time::timeout(period, self.connection.open_bi())
            .await
            .map_err(|_| WireError::Timeout)?
            .map_err(|_| WireError::Connection)?;
        send.set_priority(request.operation.class().priority())
            .map_err(|_| WireError::Connection)?;
        carriage
            .written(write_frame(
                &mut send,
                FrameKind::Request,
                request,
                self.negotiated.max_frame_bytes,
            ))
            .await?;
        send.finish().map_err(|_| WireError::Connection)?;
        // The peer's time to answer begins when it has the request: when
        // the stream is acknowledged whole. An answer that comes before
        // that says so itself; a peer that stopped the stream says why in
        // what it answers, or by the end of the stream it answers on.
        let header = {
            let mut answer = std::pin::pin!(read_frame_header(
                &mut recv,
                FrameKind::Response,
                self.negotiated.max_frame_bytes,
            ));
            match carriage.acknowledged(&send, answer.as_mut()).await? {
                Carried::Answered(header) => header,
                // The peer's period to answer, and what the path is given
                // to carry the first of the answer.
                Carried::Stream(_) => tokio::time::timeout(
                    period.saturating_add(residency(HEADER_BYTES, carriage.longest())),
                    answer,
                )
                .await
                .map_err(|_| WireError::Timeout)??,
            }
        };
        drop(carriage);
        let delivery = ConnectionDelivery {
            connection: &self.connection,
            counts: &self.capacity.counts,
        };
        let response: ResponseEnvelope = read_payload_arriving(
            &mut recv,
            header,
            period,
            || self.connection.rtt(),
            &delivery,
            request.operation.class().rank(),
        )
        .await?;
        // The end of the stream is the last thing the path carries of it.
        tokio::time::timeout(
            period.max(residency(1, self.connection.rtt())),
            require_end(&mut recv),
        )
        .await
        .map_err(|_| WireError::Timeout)??;
        validate_response(request, &response, None, &self.limits)?;
        Ok(response)
    }
}
