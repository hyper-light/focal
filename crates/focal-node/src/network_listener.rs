//! One advertised QUIC endpoint, two strictly separated authentication paths, on focal-wire's
//! endpoint (hyper-transport on hyper-rt). The endpoint's directory names each connection by the
//! protocol its handshake negotiated: on the data protocol by a validated node or client
//! certificate and a live grant, checked again as each request is dispatched; on the enrollment
//! protocol as a joiner of its own, which may send the enrollment kinds alone. Enrollment pins the
//! bootstrap certificate before sending its secret.
use focal_enrollment::{
    CredentialMaterial, ENROLLMENT_ALPN, ENROLLMENT_PROTOCOL, EnrollmentService, JoinHandler,
    TransportLimits,
};
use focal_memory::MemoryBudget;
use focal_wire::{
    ALPN, AdmissionLimits, AdmissionStats, Bind, DataService, PeerRegistry, QuicServer,
    RequestHandler, ServerIdentity, ServerTls, TlsIdentity, WireError, WireLimits,
};
use rustls::{
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};
use std::net::SocketAddr;

/// The certificate a handshake is presented: the data identity to a client that offers the data
/// protocol, the enrollment identity to a joiner.
#[derive(Debug)]
struct ProtocolCertificate {
    data: Box<dyn ResolvesServerCert>,
    enrollment: Option<Box<dyn ResolvesServerCert>>,
}
impl ResolvesServerCert for ProtocolCertificate {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<&CertifiedKey> {
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
/// What stands in the configuration's place while its resolver is wrapped.
#[derive(Debug)]
struct NoCertificate;
impl ResolvesServerCert for NoCertificate {
    fn resolve(&self, _: ClientHello<'_>) -> Option<&CertifiedKey> {
        None
    }
}

/// The TLS server configuration one advertised endpoint presents: the node's data identity, the
/// founder's enrollment identity when it signs, and the issuers clients' chains are verified
/// against.
fn server_config(
    node: &CredentialMaterial,
    enrollment: Option<&CredentialMaterial>,
    roots: &[Vec<u8>],
) -> Result<ServerTls, WireError> {
    let provider = focal_wire::crypto_provider();
    // A client's chain is verified against the issuers this node trusts; a successor issuer's
    // endorsement by one of them is an ordinary intermediate to it (24 §11). The enrollment
    // protocol authenticates a joiner without a certificate.
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        focal_wire::TrustRoots::new(roots.to_vec())?.store()?,
        provider,
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
        .map(|mut config| std::mem::replace(&mut config.cert_resolver, Box::new(NoCertificate)));
    let data = std::mem::replace(&mut tls.cert_resolver, Box::new(NoCertificate));
    tls.cert_resolver = Box::new(ProtocolCertificate {
        data,
        enrollment: enrollment_resolver,
    });
    tls.alpn_protocols = vec![ALPN.to_vec()];
    if enrollment.is_some() {
        tls.alpn_protocols.push(ENROLLMENT_ALPN.to_vec());
    }
    Ok(ServerTls::from_config(tls))
}
/// The endpoint's identity, swappable while it serves: a renewed node credential, or a succeeded
/// bootstrap server certificate (24 §11), is presented to the next handshake without rebinding.
#[derive(Clone)]
pub struct ListenerIdentity {
    server: ServerIdentity,
}
impl ListenerIdentity {
    /// Present `node` as the data identity and `enrollment` as the enrollment identity (the
    /// founder's; none elsewhere) from the next handshake on, verifying clients against `roots` —
    /// the issuers the committed registry names (24 §11), as the controller holds them.
    pub async fn replace(
        &self,
        node: &CredentialMaterial,
        enrollment: Option<&CredentialMaterial>,
        roots: &[Vec<u8>],
    ) -> Result<(), WireError> {
        self.server
            .replace(server_config(node, enrollment, roots)?)
            .await
    }
}
pub struct NetworkListener {
    server: QuicServer,
    registry: PeerRegistry,
    limits: WireLimits,
    /// Whether the endpoint serves the enrollment protocol: its TLS names it, and its directory
    /// admits joiners.
    enrolls: bool,
    join_limits: TransportLimits,
}
impl NetworkListener {
    /// A handle that swaps the identity this endpoint presents.
    pub fn identity(&self) -> Result<ListenerIdentity, WireError> {
        Ok(ListenerIdentity {
            server: self.server.identity(),
        })
    }
    /// `roots`: the issuers' certificates clients' chains are verified against (24 §11). Bound on
    /// the current shard, whose task serves it: on `address`, or on the socket its caller held.
    pub fn bind(
        address: impl Into<Bind>,
        node: &CredentialMaterial,
        enrollment: Option<&CredentialMaterial>,
        roots: &[Vec<u8>],
        registry: PeerRegistry,
        limits: WireLimits,
        budget: MemoryBudget,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        let tls = server_config(node, enrollment, roots)?;
        // Handshakes take pending places apart from authenticated connections, which are charged
        // to the identity they authenticated as (27 §3.1 P5); the listener's budget funds the
        // request bodies it admits (the audit's F03).
        let admission = AdmissionLimits::for_connections(limits.max_connections)
            .replacing_after(limits.request_timeout);
        let server = QuicServer::bind_serving(
            address,
            tls,
            registry.clone(),
            enrollment.is_some().then_some(ENROLLMENT_PROTOCOL),
            limits.clone(),
            admission,
            budget,
        )?;
        Ok(Self {
            server,
            registry,
            limits,
            enrolls: enrollment.is_some(),
            join_limits: TransportLimits::default(),
        })
    }
    pub fn local_addr(&self) -> Result<SocketAddr, WireError> {
        self.server.local_addr()
    }
    /// Who holds this listener's connections, and what it refused.
    pub async fn admission(&self) -> AdmissionStats {
        self.server.admission().await
    }
    pub fn close(&self) {
        // A close the shard could not queue leaves the endpoint to the service's shutdown, which
        // awaits its end.
        let _ = self.server.close();
    }
    /// Closes the endpoint and waits for its end: its socket gone, the address free again. The
    /// service supplies the bounded shutdown deadline around this wait.
    pub(crate) async fn shutdown(&mut self) {
        self.close();
        self.server.ended().await;
    }
    /// Serves the data protocol with `data`, and the enrollment protocol with `enrollment` where
    /// this endpoint offers it, until the endpoint is closed.
    pub async fn serve<H: RequestHandler + Clone, J: JoinHandler + Clone>(
        &self,
        data: H,
        enrollment: Option<J>,
    ) -> Result<(), WireError> {
        let data = DataService::new(data, self.registry.clone(), self.limits.clone());
        match enrollment.filter(|_| self.enrolls) {
            Some(handler) => {
                let service = EnrollmentService::new(data, handler, self.join_limits.clone())
                    .map_err(|_| WireError::Limit)?;
                self.server.serve_with(service).await
            }
            None => self.server.serve_with(data).await,
        }
    }
}
