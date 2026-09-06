//! One advertised QUIC endpoint, two strictly separated authentication paths.
//! Enrollment pins the bootstrap certificate before sending its secret; normal
//! requests require a validated node/client certificate and a live server grant.
use focal_enrollment::{CredentialMaterial, ENROLLMENT_ALPN, JoinHandler, TransportLimits};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_wire::{ALPN, PeerRegistry, RequestHandler, TlsIdentity, WireError, WireLimits};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use rustls::{
    pki_types::CertificateDer,
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};
use std::{
    future::Future,
    net::{SocketAddr, UdpSocket},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

/// Quinn retains its runtime in both the endpoint and connection drivers. The
/// pinned Quinn version drops each driver's socket before its runtime handle.
/// Observe that final release so shutdown can promise that the listener address
/// is available again, including after the last public Endpoint has been dropped.
/// Audit: Quinn 0.11.11 endpoint::State drops socket/prev_socket before runtime;
/// connection::State drops conn_events/socket/io_poller before runtime. No raw
/// endpoint or socket escapes this listener. Recheck on dependency upgrades.
#[derive(Debug)]
struct ListenerRuntime {
    delegate: quinn::TokioRuntime,
    _released: tokio::sync::oneshot::Sender<()>,
    // Covers the runtime holder and one completion channel, including canceled
    // service shutdown while Quinn still owns the actual socket/driver lifetime.
    _allocation: Allocation,
}
impl quinn::Runtime for ListenerRuntime {
    fn new_timer(&self, at: Instant) -> Pin<Box<dyn quinn::AsyncTimer>> {
        self.delegate.new_timer(at)
    }
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.delegate.spawn(future);
    }
    fn wrap_udp_socket(
        &self,
        socket: UdpSocket,
    ) -> std::io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        self.delegate.wrap_udp_socket(socket)
    }
    fn now(&self) -> Instant {
        self.delegate.now()
    }
}

#[derive(Debug)]
struct ProtocolCertificate {
    // rustls requires shared immutable certified keys across concurrent handshakes.
    data: Arc<dyn ResolvesServerCert>,
    enrollment: Option<Arc<dyn ResolvesServerCert>>,
}
impl ResolvesServerCert for ProtocolCertificate {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        // Match the server's ALPN preference if a client offers both protocols.
        let data = hello
            .alpn()
            .is_some_and(|mut protocols| protocols.any(|p| p == ALPN));
        if data {
            self.data.resolve(hello)
        } else {
            self.enrollment
                .as_ref()
                .and_then(|resolver| resolver.resolve(hello))
        }
    }
}
pub struct NetworkListener {
    endpoint: Option<quinn::Endpoint>,
    released: Option<tokio::sync::oneshot::Receiver<()>>,
    registry: PeerRegistry,
    limits: WireLimits,
    join_limits: TransportLimits,
    enrollment_slots: tokio::sync::Semaphore,
    budget: MemoryBudget,
}
impl NetworkListener {
    pub fn bind(
        address: SocketAddr,
        node: &CredentialMaterial,
        enrollment: Option<&CredentialMaterial>,
        ca: &[u8],
        registry: PeerRegistry,
        limits: WireLimits,
        budget: MemoryBudget,
    ) -> Result<Self, WireError> {
        tokio::runtime::Handle::try_current().map_err(|_| WireError::Connection)?;
        Self::from_socket(
            UdpSocket::bind(address)?,
            node,
            enrollment,
            ca,
            registry,
            limits,
            budget,
        )
    }
    pub(crate) fn from_socket(
        socket: UdpSocket,
        node: &CredentialMaterial,
        enrollment: Option<&CredentialMaterial>,
        ca: &[u8],
        registry: PeerRegistry,
        limits: WireLimits,
        budget: MemoryBudget,
    ) -> Result<Self, WireError> {
        tokio::runtime::Handle::try_current().map_err(|_| WireError::Connection)?;
        limits.validate()?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(ca))
            .map_err(|_| WireError::Authentication)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .allow_unauthenticated()
        .build()
        .map_err(|_| WireError::Authentication)?;
        let identity = TlsIdentity::from_pkcs8(
            node.certificate_chain().to_vec(),
            node.private_key_der().to_vec(),
        );
        let mut tls = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| WireError::Authentication)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(identity.certificate_chain, identity.private_key)
            .map_err(|_| WireError::Authentication)?;
        let enrollment_resolver = enrollment
            .map(CredentialMaterial::server_config)
            .transpose()
            .map_err(|_| WireError::Authentication)?
            .map(|config| config.cert_resolver);
        tls.cert_resolver = Arc::new(ProtocolCertificate {
            data: tls.cert_resolver,
            enrollment: enrollment_resolver,
        });
        tls.alpn_protocols = vec![ALPN.to_vec()];
        if enrollment.is_some() {
            tls.alpn_protocols.push(ENROLLMENT_ALPN.to_vec());
        }
        let config = focal_wire::server_transport(tls, &limits)?;
        let join_limits = TransportLimits::default();
        let allocation = budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 4096)
            .map_err(|_| WireError::Limit)?
            .commit();
        let (released, completion) = tokio::sync::oneshot::channel();
        // Tokio exposes no fallible driver-capability query. Check time before
        // Quinn spawns its endpoint driver, and contain its IO registration panic.
        let endpoint = catch_unwind(AssertUnwindSafe(|| {
            drop(tokio::time::sleep(Duration::ZERO));
            // Quinn requires shared runtime ownership across its endpoint and
            // connection drivers. This replaces its default runtime allocation.
            quinn::Endpoint::new(
                quinn::EndpointConfig::default(),
                Some(config),
                socket,
                Arc::new(ListenerRuntime {
                    delegate: quinn::TokioRuntime,
                    _released: released,
                    _allocation: allocation,
                }),
            )
        }))
        .map_err(|_| WireError::Connection)??;
        Ok(Self {
            endpoint: Some(endpoint),
            released: Some(completion),
            registry,
            limits,
            enrollment_slots: tokio::sync::Semaphore::new(join_limits.max_connections),
            join_limits,
            budget,
        })
    }
    pub fn local_addr(&self) -> Result<SocketAddr, WireError> {
        self.endpoint
            .as_ref()
            .ok_or(WireError::Connection)?
            .local_addr()
            .map_err(Into::into)
    }
    pub fn close(&self) {
        if let Some(endpoint) = &self.endpoint {
            endpoint.close(0u8.into(), b"shutdown");
        }
    }
    pub(crate) async fn shutdown(&mut self) {
        self.close();
        drop(self.endpoint.take());
        // Sender cancellation is the expected signal: every Quinn owner of the
        // runtime (and thus its sockets) has been dropped. The service supplies
        // the bounded shutdown deadline around this wait.
        if let Some(released) = self.released.as_mut() {
            let _ = released.await;
            self.released = None;
        }
    }
    /// Futures borrow this listener and its enrollment semaphore. No per-handler
    /// Arc wrapper or detached task is needed to share the physical endpoint.
    pub async fn serve<H: RequestHandler + Clone, J: JoinHandler + Clone>(
        &self,
        data: H,
        enrollment: Option<J>,
    ) -> Result<(), WireError> {
        tokio::runtime::Handle::try_current().map_err(|_| WireError::Connection)?;
        // A handle can belong to a runtime built without its time driver.
        catch_unwind(|| drop(tokio::time::sleep(Duration::ZERO)))
            .map_err(|_| WireError::Connection)?;
        // Quinn and Tokio assume configured drivers. If a dependency unwinds,
        // discard these connection futures and their charges; never poll a
        // partially unwound exchange again. Normal operation adds no tasks.
        AssertUnwindSafe(self.serve_inner(data, enrollment))
            .catch_unwind()
            .await
            .map_err(|_| WireError::Connection)?
    }

    async fn serve_inner<H: RequestHandler + Clone, J: JoinHandler + Clone>(
        &self,
        data: H,
        enrollment: Option<J>,
    ) -> Result<(), WireError> {
        let endpoint = self.endpoint.as_ref().ok_or(WireError::Connection)?;
        let mut connections = FuturesUnordered::new();
        loop {
            tokio::select! {
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break; };
                    if connections.len() >= self.limits.max_connections { incoming.refuse(); continue; }
                    let Ok(charge) = self.budget.reserve(BudgetKind::Control, BudgetLane::Ordinary, 64 * 1024) else { incoming.refuse(); continue; };
                    let data = data.clone();
                    let enrollment = enrollment.clone();
                    connections.push(async move {
                        let _charge = charge.commit();
                        let Ok(Ok(connection)) = tokio::time::timeout(self.limits.request_timeout, incoming).await else { return; };
                        let protocol = connection.handshake_data().and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
                            .and_then(|data| data.protocol);
                        if protocol.as_deref() == Some(ALPN) {
                            let _ = focal_wire::serve_authenticated_connection(connection, self.registry.clone(), self.limits.clone(), data).await;
                        } else if protocol.as_deref() == Some(ENROLLMENT_ALPN) {
                            if let (Some(handler), Ok(_slot)) = (enrollment, self.enrollment_slots.try_acquire()) {
                                let _ = focal_enrollment::serve_enrollment_connection(connection, handler, self.join_limits.timeout).await;
                            } else { connection.close(1u8.into(), b"enrollment unavailable"); }
                        } else { connection.close(1u8.into(), b"unsupported protocol"); }
                    });
                }
                _ = connections.next(), if !connections.is_empty() => {}
            }
        }
        drop(connections);
        Ok(())
    }
}
