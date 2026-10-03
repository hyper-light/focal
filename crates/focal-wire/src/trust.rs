//! Trust roots across an issuer's succession (24 §11, the audit's F13
//! stage 3). A cluster's credentials are issued by one root at a time; a
//! successor root is committed in the enrollment registry before anything
//! is issued under it, and every node adopts it from there. A verifier that
//! has not — a node behind the commit, a client holding the trust its
//! invitation carried — would refuse every credential issued under the
//! successor and could never catch up through the peers it refuses. So a
//! successor is *endorsed* by its predecessor: a CA certificate for the
//! successor's key and name, signed by the predecessor's key, is committed
//! beside the self-signed one and presented in every chain. To a verifier
//! that holds the predecessor the endorsement is an ordinary intermediate:
//! the leaf chains to it and it to the anchor. The genesis root is issued
//! with a path length of zero, and that does not stand in the way — a trust
//! anchor's own constraints are not applied in path building (RFC 5280
//! §6.1.1 leaves them to policy; webpki applies none), which
//! `tests::webpki_crosses_a_zero_length_anchor_through_an_endorsement`
//! holds the dependency to. What this module adds is the bounded root set
//! every verifier is built from, and, for a client that holds an older
//! trust, the word that a chain carried an issuer it does not hold —
//! endorsed by one it does — so the client adopts it before the succession
//! after that one, endorsed by the issuer it never held, would cut it off.
use crate::WireError;
use rustls::{
    DigitallySignedStruct, Error as TlsError, SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    crypto::CryptoProvider,
    pki_types::{CertificateDer, ServerName, UnixTime},
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
/// anchor is.
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
    /// The roots as a store a verifier is built from.
    pub fn store(&self) -> Result<rustls::RootCertStore, WireError> {
        let mut store = rustls::RootCertStore::empty();
        for root in &self.roots {
            store
                .add(CertificateDer::from(root.as_slice()).into_owned())
                .map_err(|_| WireError::Authentication)?;
        }
        if store.is_empty() {
            return Err(WireError::Authentication);
        }
        Ok(store)
    }
    /// The presented certificate, if any, that endorses an issuer this
    /// verifier does not hold: a CA certificate valid at `now`, for a key
    /// that is no root's, whose signature verifies under a known root's
    /// key. A verifier that holds the predecessor alone verified the chain
    /// through it; the successor it certifies is what such a verifier
    /// adopts.
    fn endorsed_issuer<'a>(
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
            // A root endorses no certificate of its own key: that is the
            // root itself, which is held.
            let held = self.roots.iter().any(|root| {
                X509Certificate::from_der(root)
                    .is_ok_and(|(_, root)| root.public_key().raw == endorsement.public_key().raw)
            });
            if held {
                continue;
            }
            for root in &self.roots {
                let Ok((_, root)) = X509Certificate::from_der(root) else {
                    continue;
                };
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
    /// The presented self-signed certificate of the key `endorsement`
    /// certifies, if the chain carried it: the issuer itself.
    fn self_signed_of<'a>(
        presented: &[CertificateDer<'a>],
        endorsement: &CertificateDer<'_>,
    ) -> Option<CertificateDer<'a>> {
        let (_, endorsement) = X509Certificate::from_der(endorsement).ok()?;
        presented.iter().find_map(|candidate| {
            let (rest, certificate) = X509Certificate::from_der(candidate).ok()?;
            (rest.is_empty()
                && certificate.public_key().raw == endorsement.public_key().raw
                && certificate.issuer().as_raw() == certificate.subject().as_raw())
            .then(|| candidate.clone())
        })
    }
}

/// An issuer a verifier accepted through its predecessor's endorsement: the
/// endorsement, verified under a root the verifier holds (`anchor`), and
/// the issuer's own self-signed certificate when the chain presented it.
/// What a verifier holding an older trust adopts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Adopted {
    pub anchor: Vec<u8>,
    pub issuer: Option<Vec<u8>>,
}
/// The latest issuer a client's verifier found endorsed and not held (24
/// §11): read once a request succeeded, recorded by the client that holds
/// the trust.
#[derive(Debug)]
pub struct AdoptedRoots(tokio::sync::watch::Receiver<Option<Adopted>>);
impl AdoptedRoots {
    /// The adoption not yet taken, if any.
    pub fn take(&mut self) -> Option<Adopted> {
        if !self.0.has_changed().unwrap_or(false) {
            return None;
        }
        self.0.borrow_and_update().clone()
    }
}

/// Verifies a server's chain against the known roots as webpki does, and
/// tells its holder when a verified chain carried an issuer the roots do
/// not hold, endorsed by one they do — so the holder adopts it.
#[derive(Debug)]
pub struct AdoptingServerVerifier {
    roots: TrustRoots,
    known: Arc<WebPkiServerVerifier>,
    adoption: tokio::sync::watch::Sender<Option<Adopted>>,
}
impl AdoptingServerVerifier {
    pub fn new(
        roots: TrustRoots,
        provider: Arc<CryptoProvider>,
    ) -> Result<(Arc<Self>, AdoptedRoots), WireError> {
        let known = WebPkiServerVerifier::builder_with_provider(Arc::new(roots.store()?), provider)
            .build()
            .map_err(|_| WireError::Authentication)?;
        let (adoption, receive) = tokio::sync::watch::channel(None);
        Ok((
            Arc::new(Self {
                roots,
                known,
                adoption,
            }),
            AdoptedRoots(receive),
        ))
    }
}
impl ServerCertVerifier for AdoptingServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let verified = self.known.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        // Verified; a chain that carried an issuer the roots do not hold,
        // endorsed by one they do, is told to the holder. A chain the
        // bound refuses to read is still the verified chain it was.
        if let Ok(Some(endorsement)) = self.roots.endorsed_issuer(intermediates, now) {
            let issuer = TrustRoots::self_signed_of(intermediates, &endorsement)
                .map(|certificate| certificate.to_vec());
            self.adoption.send_replace(Some(Adopted {
                anchor: endorsement.to_vec(),
                issuer,
            }));
        }
        Ok(verified)
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
