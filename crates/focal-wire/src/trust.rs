//! Trust roots that bridge an issuer's succession (24 §11, the audit's F13
//! stage 3). A cluster's credentials are issued by one root at a time; a
//! successor root is committed in the enrollment registry before anything
//! is issued under it, and every node adopts it from there. A verifier that
//! has not — a node behind the commit, a client holding the trust its
//! invitation carried — would refuse every credential issued under the
//! successor and could never catch up through the peers it refuses. So a
//! successor is *endorsed* by its predecessor: a CA certificate for the
//! successor's key and name, signed by the predecessor's key, is committed
//! beside the self-signed one and presented in every chain. A verifier
//! that knows only the predecessor accepts the chain through the
//! endorsement — the predecessor's signature over the successor's key —
//! and verifies the leaf under it as under any anchor; one that knows the
//! successor needs no bridge. The genesis root is issued with a path length
//! of zero, so X.509 path building cannot cross it; the bridge is this
//! module's own, explicit rule: an endorsement is a presented CA
//! certificate, valid now, whose signature verifies under a known root.
use crate::WireError;
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, Error as TlsError, SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    crypto::CryptoProvider,
    pki_types::{CertificateDer, ServerName, UnixTime},
    server::{
        WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
};
use std::sync::Arc;
use x509_parser::prelude::{FromDer, X509Certificate};

/// The most roots a verifier holds: a current issuer, its staged
/// successor, the one it succeeded while credentials issued under it live,
/// and the genesis identity an older trust still carries.
pub const MAX_TRUST_ROOTS: usize = 4;
/// The most certificates a peer may present beside its leaf: the issuer's
/// self-signed certificate and its endorsement, with room for one
/// succession a lagging presenter has yet to drop.
pub const MAX_PRESENTED_INTERMEDIATES: usize = 4;
/// The most a certificate may be, matching the enrollment registry's bound.
const MAX_CERTIFICATE_BYTES: usize = 4096;

/// The DER roots a verifier knows, bounded and each parseable. A root is
/// trusted as configured, whatever its own constraints say — as any trust
/// anchor is; what must be a CA certificate is an endorsement.
#[derive(Clone)]
pub struct TrustRoots {
    roots: Vec<Vec<u8>>,
}
impl std::fmt::Debug for TrustRoots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrustRoots")
            .field("roots", &self.roots.len())
            .finish()
    }
}
impl TrustRoots {
    pub fn new(roots: Vec<Vec<u8>>) -> Result<Self, WireError> {
        if roots.is_empty() || roots.len() > MAX_TRUST_ROOTS {
            return Err(WireError::Authentication);
        }
        for root in &roots {
            if root.len() > MAX_CERTIFICATE_BYTES {
                return Err(WireError::Authentication);
            }
            let (rest, _) =
                X509Certificate::from_der(root).map_err(|_| WireError::Authentication)?;
            if !rest.is_empty() {
                return Err(WireError::Authentication);
            }
        }
        Ok(Self { roots })
    }
    fn store(&self) -> Result<rustls::RootCertStore, WireError> {
        store_of(
            self.roots
                .iter()
                .map(|root| CertificateDer::from(root.as_slice())),
        )
    }
    /// The presented certificate, if any, that a known root endorses: a CA
    /// certificate valid at `now` whose signature verifies under a known
    /// root's key. It anchors the leaf as the successor root would.
    fn endorsed_anchor<'a>(
        &self,
        presented: &[CertificateDer<'a>],
        now: UnixTime,
    ) -> Result<Option<CertificateDer<'a>>, WireError> {
        if presented.len() > MAX_PRESENTED_INTERMEDIATES {
            return Err(WireError::Authentication);
        }
        let now = i64::try_from(now.as_secs()).map_err(|_| WireError::Authentication)?;
        for candidate in presented {
            if candidate.len() > MAX_CERTIFICATE_BYTES {
                return Err(WireError::Authentication);
            }
            let Ok((rest, endorsement)) = X509Certificate::from_der(candidate) else {
                continue;
            };
            if !rest.is_empty()
                || !endorsement.is_ca()
                || endorsement.validity().not_before.timestamp() > now
                || endorsement.validity().not_after.timestamp() <= now
            {
                continue;
            }
            for root in &self.roots {
                let Ok((_, root)) = X509Certificate::from_der(root) else {
                    continue;
                };
                // A root endorses no certificate of its own key: that is
                // the root itself, known or not.
                if root.public_key().raw == endorsement.public_key().raw {
                    continue;
                }
                if endorsement
                    .verify_signature(Some(root.public_key()))
                    .is_ok()
                {
                    return Ok(Some(candidate.clone()));
                }
            }
        }
        Ok(None)
    }
}
fn store_of<'a>(
    certificates: impl Iterator<Item = CertificateDer<'a>>,
) -> Result<rustls::RootCertStore, WireError> {
    let mut store = rustls::RootCertStore::empty();
    for certificate in certificates {
        store
            .add(certificate.into_owned())
            .map_err(|_| WireError::Authentication)?;
    }
    if store.is_empty() {
        return Err(WireError::Authentication);
    }
    Ok(store)
}
/// Whether a refusal may be one the bridge answers: any fault in the
/// chain's path — an issuer unknown, or a path the genesis root's length
/// constraint forbids. The bridged verifier verifies the leaf in full
/// again, so a fault that was the leaf's own is refused the same way.
fn bridgeable(error: &TlsError) -> bool {
    matches!(error, TlsError::InvalidCertificate(_))
}
fn tls_error() -> TlsError {
    TlsError::InvalidCertificate(CertificateError::UnknownIssuer)
}

/// Verifies a server's chain against the known roots, and through an
/// endorsement a known root signed when the chain's issuer is unknown.
#[derive(Debug)]
pub struct EndorsingServerVerifier {
    roots: TrustRoots,
    provider: Arc<CryptoProvider>,
    known: Arc<WebPkiServerVerifier>,
}
impl EndorsingServerVerifier {
    pub fn new(roots: TrustRoots, provider: Arc<CryptoProvider>) -> Result<Arc<Self>, WireError> {
        let known =
            WebPkiServerVerifier::builder_with_provider(Arc::new(roots.store()?), provider.clone())
                .build()
                .map_err(|_| WireError::Authentication)?;
        Ok(Arc::new(Self {
            roots,
            provider,
            known,
        }))
    }
    fn bridged(
        &self,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<Arc<WebPkiServerVerifier>, TlsError> {
        let anchor = self
            .roots
            .endorsed_anchor(intermediates, now)
            .map_err(|_| tls_error())?
            .ok_or_else(tls_error)?;
        let store = store_of(std::iter::once(anchor)).map_err(|_| tls_error())?;
        WebPkiServerVerifier::builder_with_provider(Arc::new(store), self.provider.clone())
            .build()
            .map_err(|_| tls_error())
    }
}
impl ServerCertVerifier for EndorsingServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        match self.known.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(error) if bridgeable(&error) => self
                .bridged(intermediates, now)?
                .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now),
            verified => verified,
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.known.verify_tls12_signature(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.known.verify_tls13_signature(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.known.supported_verify_schemes()
    }
}

/// Verifies a client's chain against the known roots, and through an
/// endorsement a known root signed when the chain's issuer is unknown.
#[derive(Debug)]
pub struct EndorsingClientVerifier {
    roots: TrustRoots,
    provider: Arc<CryptoProvider>,
    allow_unauthenticated: bool,
    known: Arc<dyn ClientCertVerifier>,
}
impl EndorsingClientVerifier {
    /// `allow_unauthenticated` admits a handshake without a client
    /// certificate, for an endpoint whose enrollment protocol authenticates
    /// by other means; a certificate presented is always verified.
    pub fn new(
        roots: TrustRoots,
        provider: Arc<CryptoProvider>,
        allow_unauthenticated: bool,
    ) -> Result<Arc<Self>, WireError> {
        let known = Self::verifier(&roots.store()?, &provider, allow_unauthenticated)
            .map_err(|_| WireError::Authentication)?;
        Ok(Arc::new(Self {
            roots,
            provider,
            allow_unauthenticated,
            known,
        }))
    }
    fn verifier(
        store: &rustls::RootCertStore,
        provider: &Arc<CryptoProvider>,
        allow_unauthenticated: bool,
    ) -> Result<Arc<dyn ClientCertVerifier>, TlsError> {
        let builder =
            WebPkiClientVerifier::builder_with_provider(Arc::new(store.clone()), provider.clone());
        let builder = if allow_unauthenticated {
            builder.allow_unauthenticated()
        } else {
            builder
        };
        builder.build().map_err(|_| tls_error())
    }
    fn bridged(
        &self,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<Arc<dyn ClientCertVerifier>, TlsError> {
        let anchor = self
            .roots
            .endorsed_anchor(intermediates, now)
            .map_err(|_| tls_error())?
            .ok_or_else(tls_error)?;
        let store = store_of(std::iter::once(anchor)).map_err(|_| tls_error())?;
        Self::verifier(&store, &self.provider, self.allow_unauthenticated)
    }
}
impl ClientCertVerifier for EndorsingClientVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.known.root_hint_subjects()
    }
    fn offer_client_auth(&self) -> bool {
        self.known.offer_client_auth()
    }
    fn client_auth_mandatory(&self) -> bool {
        self.known.client_auth_mandatory()
    }
    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, TlsError> {
        match self
            .known
            .verify_client_cert(end_entity, intermediates, now)
        {
            Err(error) if bridgeable(&error) => self
                .bridged(intermediates, now)?
                .verify_client_cert(end_entity, intermediates, now),
            verified => verified,
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.known.verify_tls12_signature(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.known.verify_tls13_signature(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.known.supported_verify_schemes()
    }
}

/// Whether `certificate` was issued by `issuer`: its issuer name is the
/// issuer's subject and its signature verifies under the issuer's key.
pub fn issued_by(certificate: &[u8], issuer: &[u8]) -> Result<bool, WireError> {
    if certificate.len() > MAX_CERTIFICATE_BYTES || issuer.len() > MAX_CERTIFICATE_BYTES {
        return Err(WireError::Authentication);
    }
    let (rest, certificate) =
        X509Certificate::from_der(certificate).map_err(|_| WireError::Authentication)?;
    if !rest.is_empty() {
        return Err(WireError::Authentication);
    }
    let (rest, issuer) =
        X509Certificate::from_der(issuer).map_err(|_| WireError::Authentication)?;
    if !rest.is_empty() {
        return Err(WireError::Authentication);
    }
    Ok(certificate.issuer().as_raw() == issuer.subject().as_raw()
        && certificate
            .verify_signature(Some(issuer.public_key()))
            .is_ok())
}
