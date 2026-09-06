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
fn transport(limits: &WireLimits) -> Result<Arc<quinn::TransportConfig>, WireError> {
    limits.validate()?;
    let streams = limits
        .streams_per_connection
        .checked_add(2)
        .ok_or(WireError::Limit)?;
    let frame = limits
        .max_frame_bytes
        .checked_add(HEADER_BYTES as u32)
        .ok_or(WireError::Limit)?;
    let window = u64::from(frame)
        .checked_mul(u64::from(streams))
        .ok_or(WireError::Limit)?;
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(streams.into());
    transport.max_concurrent_uni_streams(0u8.into());
    transport.stream_receive_window(frame.into());
    transport.receive_window(quinn::VarInt::from_u64(window).map_err(|_| WireError::Limit)?);
    transport.send_window(window);
    transport.max_idle_timeout(Some(
        limits
            .request_timeout
            .try_into()
            .map_err(|_| WireError::Limit)?,
    ));
    Ok(Arc::new(transport))
}

pub fn server_tls(
    identity: TlsIdentity,
    client_roots: Vec<Vec<u8>>,
    limits: &WireLimits,
) -> Result<quinn::ServerConfig, WireError> {
    limits.validate()?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
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
    let provider = Arc::new(rustls::crypto::ring::default_provider());
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
}
impl QuicServer {
    pub fn bind(
        address: SocketAddr,
        tls: quinn::ServerConfig,
        registry: PeerRegistry,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        let endpoint = transport_setup(|| Ok(Endpoint::server(tls, address)?))?;
        Ok(Self {
            endpoint,
            registry,
            limits,
        })
    }
    pub fn local_addr(&self) -> Result<SocketAddr, WireError> {
        Ok(self.endpoint.local_addr()?)
    }
    pub fn close(&self) {
        self.endpoint.close(0u8.into(), b"shutdown");
    }
    pub async fn serve<H: RequestHandler + Clone>(&self, handler: H) -> Result<(), WireError> {
        require_runtime()?;
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                incoming=self.endpoint.accept()=>{
                    let Some(incoming)=incoming else{break}; if tasks.len() >= self.limits.max_connections {incoming.refuse();continue;}
                    let registry=self.registry.clone();let limits=self.limits.clone();let handler=handler.clone();
                    tasks.spawn(async move {
                        let _ = transport_exchange(async {
                            let connection = tokio::time::timeout(limits.request_timeout, incoming)
                                .await.map_err(|_| WireError::Timeout)?
                                .map_err(|_| WireError::Connection)?;
                            serve_authenticated_connection(connection, registry, limits, handler).await
                        }).await;
                    });
                }
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
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
        connection, registry, limits, handler,
    ))
    .await
}
async fn serve_authenticated_connection_inner<H: RequestHandler + Clone>(
    connection: Connection,
    registry: PeerRegistry,
    limits: WireLimits,
    handler: H,
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
        registry
            .authenticate(fingerprint)
            .map_err(|_| WireError::Authentication)?;
        Ok(fingerprint)
    })();
    let fingerprint = match authenticated {
        Ok(fingerprint) => fingerprint,
        Err(error) => {
            connection.close(1u8.into(), b"unauthorized");
            return Err(error);
        }
    };
    let handshake = async {
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .map_err(|_| WireError::Connection)?;
        let hello: Hello = read_frame(&mut recv, FrameKind::Hello, 4096).await?;
        require_end(&mut recv).await?;
        let negotiated = match limits.negotiate_profiles(
            &hello,
            handler.supports_managed_requests(),
            handler.supports_participant_requests(),
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
    loop {
        tokio::select! {
            Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
            streams=connection.accept_bi()=>{
                let Ok((mut send,mut recv))=streams else{break}; if tasks.len() >= task_limit {let _=send.reset(2u8.into());let _=recv.stop(2u8.into());continue;}
                let registry=registry.clone();let limits=limits.clone();let handler=handler.clone();
                tasks.spawn(async move {
                    let work=async {
                        // Recheck registry on every stream, so revocation applies
                        // to already established authenticated connections.
                        let peer=registry.authenticate(fingerprint)?;
                        let request:RequestEnvelope=read_frame(&mut recv,FrameKind::Request,limits.max_frame_bytes).await?;require_end(&mut recv).await?;
                        if matches!(request.operation,Operation::Raft{..}){send.set_priority(10).map_err(|_|WireError::Connection)?;}
                        let response = if negotiated.accepts_protocol(request.protocol) {
                            dispatch_accounted(&handler,peer,request,&limits).await
                        } else {
                            OwnedResponse::new(request.reply(Response::Error(AccessError::UnsupportedProtocol)))
                        };
                        send_owned_response(send,response,limits.max_frame_bytes).await
                    };
                    let _=transport_exchange(async {
                        tokio::time::timeout(limits.request_timeout,work)
                            .await.map_err(|_|WireError::Timeout)?
                    }).await;
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

#[derive(Clone)]
pub struct QuicConnector {
    endpoint: Endpoint,
    limits: WireLimits,
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
        let mut endpoint = transport_setup(|| Ok(Endpoint::client(address)?))?;
        endpoint.set_default_client_config(tls);
        Ok(Self { endpoint, limits })
    }
    pub async fn connect(
        &self,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<QuicRemote, WireError> {
        transport_exchange(self.connect_inner(address, server_name)).await
    }
    async fn connect_inner(
        &self,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<QuicRemote, WireError> {
        let connecting = self
            .endpoint
            .connect(address, server_name)
            .map_err(|_| WireError::Connection)?;
        let connection = tokio::time::timeout(self.limits.request_timeout, connecting)
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
                    PEER_PROTOCOL_VERSION,
                    MANAGED_PROTOCOL_VERSION,
                    PROTOCOL_VERSION,
                ],
                max_frame_bytes: self.limits.max_frame_bytes,
                max_items: self.limits.max_items,
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
        let negotiated = tokio::time::timeout(self.limits.request_timeout, handshake)
            .await
            .map_err(|_| WireError::Timeout)??;
        if !matches!(
            negotiated.protocol,
            PROTOCOL_VERSION | MANAGED_PROTOCOL_VERSION | PEER_PROTOCOL_VERSION
        ) || negotiated.max_frame_bytes > self.limits.max_frame_bytes
            || negotiated.max_items > self.limits.max_items
        {
            return Err(WireError::InvalidFrame);
        }
        Ok(QuicRemote {
            connection,
            negotiated,
            limits: self.limits.clone(),
            _endpoint: self.endpoint.clone(),
            capacity: Arc::new(RemoteCapacity {
                data: Semaphore::new(self.limits.streams_per_connection as usize),
                control: Semaphore::new(2),
            }),
        })
    }
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
}
impl QuicRemote {
    pub fn negotiated(&self) -> Negotiated {
        self.negotiated
    }
    pub fn close(&self) {
        self.connection.close(0u8.into(), b"client closed");
    }
    pub async fn request(&self, request: &RequestEnvelope) -> Result<ResponseEnvelope, WireError> {
        transport_exchange(async {
            tokio::time::timeout(self.limits.request_timeout, self.request_inner(request))
                .await
                .map_err(|_| WireError::Timeout)?
        })
        .await
    }
    async fn request_inner(
        &self,
        request: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, WireError> {
        if !self.negotiated.accepts_protocol(request.protocol) {
            return Err(WireError::Access(AccessError::UnsupportedProtocol));
        }
        let lane = if matches!(request.operation, Operation::Raft { .. }) {
            &self.capacity.control
        } else {
            &self.capacity.data
        };
        let _permit = lane.try_acquire().map_err(|_| WireError::Limit)?;
        let (mut send, mut recv) = self
            .connection
            .open_bi()
            .await
            .map_err(|_| WireError::Connection)?;
        if matches!(request.operation, Operation::Raft { .. }) {
            send.set_priority(10).map_err(|_| WireError::Connection)?;
        }
        write_frame(
            &mut send,
            FrameKind::Request,
            request,
            self.negotiated.max_frame_bytes,
        )
        .await?;
        send.finish().map_err(|_| WireError::Connection)?;
        let response = read_frame(
            &mut recv,
            FrameKind::Response,
            self.negotiated.max_frame_bytes,
        )
        .await?;
        require_end(&mut recv).await?;
        validate_response(request, &response, None, &self.limits)?;
        Ok(response)
    }
}
