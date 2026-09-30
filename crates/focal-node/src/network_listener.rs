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
/// The TLS server configuration one advertised endpoint presents: the node's
/// data identity, the founder's enrollment identity when it signs, and the
/// cluster CA for client certificates.
fn server_config(
    node: &CredentialMaterial,
    enrollment: Option<&CredentialMaterial>,
    ca: &[u8],
    limits: &WireLimits,
) -> Result<quinn::ServerConfig, WireError> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
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
    focal_wire::server_transport(tls, limits)
}
/// The endpoint's identity, swappable while it serves: a renewed node
/// credential, or a succeeded bootstrap server certificate (24 §11), is
/// presented to the next handshake without rebinding.
#[derive(Clone)]
pub struct ListenerIdentity {
    endpoint: quinn::Endpoint,
    ca: Vec<u8>,
    limits: WireLimits,
}
impl ListenerIdentity {
    /// Present `node` as the data identity and `enrollment` as the
    /// enrollment identity (the founder's; none elsewhere) from the next
    /// handshake on.
    pub fn replace(
        &self,
        node: &CredentialMaterial,
        enrollment: Option<&CredentialMaterial>,
    ) -> Result<(), WireError> {
        let config = server_config(node, enrollment, &self.ca, &self.limits)?;
        self.endpoint.set_server_config(Some(config));
        Ok(())
    }
}
pub struct NetworkListener {
    endpoint: Option<quinn::Endpoint>,
    released: Option<tokio::sync::oneshot::Receiver<()>>,
    registry: PeerRegistry,
    limits: WireLimits,
    join_limits: TransportLimits,
    enrollment_slots: tokio::sync::Semaphore,
    /// Who holds this listener's connections (27 §3.1 P5): handshakes in
    /// progress apart from authenticated connections, which are charged to
    /// the identity they authenticated as.
    admission: focal_wire::Admission,
    budget: MemoryBudget,
    ca: Vec<u8>,
}
impl NetworkListener {
    /// A handle that swaps the identity this endpoint presents.
    pub fn identity(&self) -> Result<ListenerIdentity, WireError> {
        Ok(ListenerIdentity {
            endpoint: self.endpoint.clone().ok_or(WireError::Connection)?,
            ca: self.ca.clone(),
            limits: self.limits.clone(),
        })
    }
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
        let config = server_config(node, enrollment, ca, &limits)?;
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
        // The listener's budget funds the request bodies it admits (the
        // audit's F03), beside the charge of each connection.
        let admission = focal_wire::Admission::new(
            focal_wire::AdmissionLimits::for_connections(limits.max_connections),
            budget.clone(),
        )
        .map_err(|_| WireError::Limit)?;
        Ok(Self {
            endpoint: Some(endpoint),
            released: Some(completion),
            registry,
            limits,
            enrollment_slots: tokio::sync::Semaphore::new(join_limits.max_connections),
            admission,
            join_limits,
            budget,
            ca: ca.to_vec(),
        })
    }
    pub fn local_addr(&self) -> Result<SocketAddr, WireError> {
        self.endpoint
            .as_ref()
            .ok_or(WireError::Connection)?
            .local_addr()
            .map_err(Into::into)
    }
    /// Who holds this listener's connections, and what it refused.
    pub fn admission(&self) -> focal_wire::AdmissionStats {
        self.admission.stats()
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
                    // A handshake takes a pending place, which no
                    // authenticated connection uses; connections are bounded
                    // by the admission's total, met after the replacement
                    // rule, and enrollment by its slots — never by an outer
                    // count that a full listener would refuse a replacement
                    // with (the audit's F20). A source that has not proven
                    // its address takes no place while half of them are
                    // taken (RFC 9000 §8.1.2, Retry under load).
                    if focal_wire::validate_address(&incoming, &self.admission) { let _ = incoming.retry(); continue; }
                    let Ok(pending) = self.admission.begin() else { incoming.refuse(); continue; };
                    let Ok(charge) = self.budget.reserve(BudgetKind::Control, BudgetLane::Ordinary, 64 * 1024) else { incoming.refuse(); continue; };
                    let data = data.clone();
                    let enrollment = enrollment.clone();
                    connections.push(async move {
                        let _charge = charge.commit();
                        let Ok(Ok(connection)) = tokio::time::timeout(self.limits.request_timeout, incoming).await else { return; };
                        let protocol = connection.handshake_data().and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
                            .and_then(|data| data.protocol);
                        if protocol.as_deref() == Some(ALPN) {
                            let _ = focal_wire::serve_admitted_connection(connection, self.registry.clone(), self.limits.clone(), data, pending).await;
                        } else if protocol.as_deref() == Some(ENROLLMENT_ALPN) {
                            // A joiner has no identity yet: its own bound is
                            // the enrollment slots.
                            drop(pending);
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
