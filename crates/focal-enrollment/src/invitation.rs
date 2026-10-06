use crate::{pki::SecretBytes, *};
use rustls::{
    client::danger::ServerCertVerifier,
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use serde::{Deserialize, Serialize};
// rustls requires shared verifier/provider ownership across connection tasks.
use std::{sync::Arc, time::Duration};
use zeroize::Zeroizing;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnrollmentRole {
    Node,
    Client,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerTrust {
    pub endpoint: String,
    pub server_name: String,
    /// The genesis issuer's certificate: the cluster's identity (24 §11),
    /// never the trust — the issuers are.
    pub ca_certificate: Vec<u8>,
    pub server_fingerprint: Fingerprint,
    /// The bootstrap server certificate staged to succeed the pinned one
    /// (24 §11), accepted as it is: an invitation issued while one is staged
    /// carries both, and a joined node learns both from the registry.
    pub successor_fingerprint: Option<Fingerprint>,
    /// The issuers trusted when this trust was written (24 §11): what
    /// chains are verified against, through an endorsement when a chain's
    /// issuer succeeded one of them since. A joined node adopts the
    /// committed set on every refresh.
    pub issuers: Vec<IssuerRecord>,
}
/// The trust as schema 1 invitations and network states wrote it: one pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerTrustV1 {
    pub endpoint: String,
    pub server_name: String,
    pub ca_certificate: Vec<u8>,
    pub server_fingerprint: Fingerprint,
}
/// The trust as schema 2 invitations and schema 3 network states wrote it:
/// two pins, the genesis issuer as the one root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerTrustV2 {
    pub endpoint: String,
    pub server_name: String,
    pub ca_certificate: Vec<u8>,
    pub server_fingerprint: Fingerprint,
    pub successor_fingerprint: Option<Fingerprint>,
}
impl TryFrom<ServerTrustV1> for ServerTrust {
    type Error = EnrollmentError;
    fn try_from(legacy: ServerTrustV1) -> Result<Self, EnrollmentError> {
        Ok(Self {
            issuers: vec![IssuerRecord::of(&legacy.ca_certificate, None)?],
            endpoint: legacy.endpoint,
            server_name: legacy.server_name,
            ca_certificate: legacy.ca_certificate,
            server_fingerprint: legacy.server_fingerprint,
            successor_fingerprint: None,
        })
    }
}
impl TryFrom<ServerTrustV2> for ServerTrust {
    type Error = EnrollmentError;
    fn try_from(legacy: ServerTrustV2) -> Result<Self, EnrollmentError> {
        Ok(Self {
            issuers: vec![IssuerRecord::of(&legacy.ca_certificate, None)?],
            endpoint: legacy.endpoint,
            server_name: legacy.server_name,
            ca_certificate: legacy.ca_certificate,
            server_fingerprint: legacy.server_fingerprint,
            successor_fingerprint: legacy.successor_fingerprint,
        })
    }
}
/// The invitation schema whose trust carries one pin.
pub(crate) const INVITATION_SCHEMA_V1: u16 = 1;
/// The invitation schema whose trust may carry a staged successor's pin.
pub(crate) const INVITATION_SCHEMA_V2: u16 = 2;
/// The invitation schema whose trust carries the issuers.
pub(crate) const INVITATION_SCHEMA: u16 = 3;
/// The most issuers a trust carries: current, staged, retiring, and the
/// genesis identity an older trust names as its root.
pub const MAX_TRUSTED_ISSUERS: usize = focal_wire::MAX_TRUST_ROOTS;
impl ServerTrust {
    /// Validate persisted trust before using its endpoint or building TLS state.
    pub fn validate(&self) -> Result<(), EnrollmentError> {
        if self.endpoint.is_empty()
            || self.endpoint.len() > 512
            || self.server_name.len() > 253
            || self.ca_certificate.len() > 4096
            || self.issuers.len() > MAX_TRUSTED_ISSUERS
        {
            return Err(EnrollmentError::Capacity);
        }
        if self.server_fingerprint == [0; 32]
            || self.ca_certificate.is_empty()
            || self.issuers.is_empty()
        {
            return Err(EnrollmentError::Invalid);
        }
        for (index, issuer) in self.issuers.iter().enumerate() {
            if issuer.fingerprint != crate::registry::issuer_fingerprint(&issuer.certificate)
                || self
                    .issuers
                    .iter()
                    .take(index)
                    .any(|other| other.fingerprint == issuer.fingerprint)
            {
                return Err(EnrollmentError::Invalid);
            }
        }
        if self
            .successor_fingerprint
            .is_some_and(|successor| successor == [0; 32] || successor == self.server_fingerprint)
        {
            return Err(EnrollmentError::Invalid);
        }
        ServerName::try_from(self.server_name.clone()).map_err(|_| EnrollmentError::Invalid)?;
        self.roots()?;
        Ok(())
    }
    /// The issuers' certificates: the roots a verifier of this cluster's
    /// credentials holds.
    pub fn root_certificates(&self) -> Vec<Vec<u8>> {
        self.issuers
            .iter()
            .map(|issuer| issuer.certificate.clone())
            .collect()
    }
    /// Whether `fingerprint` is a pin this trust accepts: the pinned
    /// certificate's, or its staged successor's.
    pub fn accepts(&self, fingerprint: Fingerprint) -> bool {
        fingerprint == self.server_fingerprint || Some(fingerprint) == self.successor_fingerprint
    }
    /// Whether this trust names the same sponsor as `other`: the same
    /// endpoint, name and genesis issuer. The pins and the issuers are
    /// facts the registry moves as the bootstrap server certificate and
    /// the issuer succeed themselves (24 §11).
    pub fn same_sponsor(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
            && self.server_name == other.server_name
            && self.ca_certificate == other.ca_certificate
    }
    /// The trust's fingerprint as an invitation of `schema` bound it: over
    /// the encoding that schema wrote, so an invitation issued before the
    /// successor pin existed still matches the record it made.
    pub(crate) fn fingerprint_as(&self, schema: u16) -> Result<Fingerprint, EnrollmentError> {
        // An older schema named the genesis issuer as its one root.
        let genesis_alone = || {
            self.issuers.len() == 1
                && self.issuers.first().is_some_and(|issuer| {
                    issuer.certificate == self.ca_certificate && issuer.endorsement.is_none()
                })
        };
        if schema == INVITATION_SCHEMA_V1 {
            if self.successor_fingerprint.is_some() || !genesis_alone() {
                return Err(EnrollmentError::Invalid);
            }
            let legacy = ServerTrustV1 {
                endpoint: self.endpoint.clone(),
                server_name: self.server_name.clone(),
                ca_certificate: self.ca_certificate.clone(),
                server_fingerprint: self.server_fingerprint,
            };
            return Ok(hash("focal.enrollment.server-trust.v1", &encode(&legacy)?));
        }
        if schema == INVITATION_SCHEMA_V2 {
            if !genesis_alone() {
                return Err(EnrollmentError::Invalid);
            }
            let legacy = ServerTrustV2 {
                endpoint: self.endpoint.clone(),
                server_name: self.server_name.clone(),
                ca_certificate: self.ca_certificate.clone(),
                server_fingerprint: self.server_fingerprint,
                successor_fingerprint: self.successor_fingerprint,
            };
            return Ok(hash("focal.enrollment.server-trust.v1", &encode(&legacy)?));
        }
        Ok(hash("focal.enrollment.server-trust.v1", &encode(self)?))
    }
    /// The verifier of a bootstrap connection's chain: the issuers this
    /// trust holds; a successor issuer's endorsement by one of them is an
    /// ordinary intermediate to it (24 §11).
    fn verifier(
        &self,
        provider: Arc<rustls::crypto::CryptoProvider>,
    ) -> Result<Arc<rustls::client::WebPkiServerVerifier>, EnrollmentError> {
        rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(self.roots()?),
            provider,
        )
        .build()
        .map_err(|_| EnrollmentError::Crypto)
    }
    /// Ordinary TLS 1.3 chain/name verification against the pinned CA, for
    /// an enrollment connection made by a node that already holds a
    /// credential; the exact leaf pin is checked on the connection.
    pub fn client_config(&self) -> Result<rustls::ClientConfig, EnrollmentError> {
        self.validate()?;
        let provider = Arc::new(focal_wire::crypto_provider());
        let verifier = self.verifier(provider.clone())?;
        let mut config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| EnrollmentError::Crypto)?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();
        config.alpn_protocols = vec![ENROLLMENT_ALPN.to_vec()];
        config.enable_early_data = false;
        Ok(config)
    }
    /// Verify an established enrollment connection carries the pinned leaf:
    /// one that chains to a held issuer and is not pinned is
    /// [`EnrollmentError::Unpinned`], the rest `Unauthorized`.
    pub fn verify_quic(
        &self,
        connection: &quinn::Connection,
        now: i64,
    ) -> Result<(), EnrollmentError> {
        if connection.close_reason().is_some() {
            return Err(EnrollmentError::Unauthorized);
        }
        let handshake = connection
            .handshake_data()
            .ok_or(EnrollmentError::Unauthorized)?
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .map_err(|_| EnrollmentError::Unauthorized)?;
        if handshake.protocol.as_deref() != Some(ENROLLMENT_ALPN) {
            return Err(EnrollmentError::Unauthorized);
        }
        let peer = connection
            .peer_identity()
            .ok_or(EnrollmentError::Unauthorized)?
            .downcast::<Vec<CertificateDer<'static>>>()
            .map_err(|_| EnrollmentError::Unauthorized)?;
        self.verify_chain(&peer, now)
    }
    fn roots(&self) -> Result<rustls::RootCertStore, EnrollmentError> {
        let mut roots = rustls::RootCertStore::empty();
        for issuer in &self.issuers {
            roots
                .add(CertificateDer::from(issuer.certificate.clone()))
                .map_err(|_| EnrollmentError::Invalid)?;
        }
        Ok(roots)
    }
    pub(crate) fn verify_chain(
        &self,
        certificates: &[CertificateDer<'_>],
        now: i64,
    ) -> Result<(), EnrollmentError> {
        let (first, intermediates) = certificates
            .split_first()
            .ok_or(EnrollmentError::Unauthorized)?;
        let provider = Arc::new(focal_wire::crypto_provider());
        let verifier = self.verifier(provider)?;
        let now = UnixTime::since_unix_epoch(Duration::from_secs(
            u64::try_from(now).map_err(|_| EnrollmentError::Invalid)?,
        ));
        verifier
            .verify_server_cert(
                first,
                intermediates,
                &ServerName::try_from(self.server_name.clone())
                    .map_err(|_| EnrollmentError::Invalid)?,
                &[],
                now,
            )
            .map_err(|_| EnrollmentError::Unauthorized)?;
        if !self.accepts(server_fingerprint(first.as_ref())) {
            return Err(EnrollmentError::Unpinned);
        }
        Ok(())
    }
}
pub fn server_fingerprint(certificate: &[u8]) -> Fingerprint {
    hash("focal.enrollment.bootstrap-server.v1", certificate)
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct InvitationData {
    pub schema: u16,
    pub id: InvitationId,
    pub cluster: ClusterId,
    pub role: EnrollmentRole,
    pub expires_at: i64,
    pub secret: SecretBytes,
    pub trust: ServerTrust,
}
/// The invitation as schema 1 wrote it: a trust with one pin.
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct InvitationDataV1 {
    schema: u16,
    id: InvitationId,
    cluster: ClusterId,
    role: EnrollmentRole,
    expires_at: i64,
    secret: SecretBytes,
    trust: ServerTrustV1,
}
/// The invitation as schema 2 wrote it: two pins, the genesis issuer as
/// the one root.
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct InvitationDataV2 {
    schema: u16,
    id: InvitationId,
    cluster: ClusterId,
    role: EnrollmentRole,
    expires_at: i64,
    secret: SecretBytes,
    trust: ServerTrustV2,
}
impl InvitationData {
    /// Decode an invitation of any schema; schema 1 carries one pin, schema
    /// 2 two pins and the genesis issuer alone.
    pub(crate) fn decode_any(bytes: &[u8]) -> Result<Self, EnrollmentError> {
        let (schema, _) = postcard::take_from_bytes::<u16>(bytes)?;
        if schema == INVITATION_SCHEMA_V1 {
            let legacy: InvitationDataV1 = decode(bytes)?;
            return Ok(Self {
                schema: legacy.schema,
                id: legacy.id,
                cluster: legacy.cluster,
                role: legacy.role,
                expires_at: legacy.expires_at,
                secret: legacy.secret,
                trust: legacy.trust.try_into()?,
            });
        }
        if schema == INVITATION_SCHEMA_V2 {
            let legacy: InvitationDataV2 = decode(bytes)?;
            return Ok(Self {
                schema: legacy.schema,
                id: legacy.id,
                cluster: legacy.cluster,
                role: legacy.role,
                expires_at: legacy.expires_at,
                secret: legacy.secret,
                trust: legacy.trust.try_into()?,
            });
        }
        decode(bytes)
    }
    /// The trust's fingerprint as this invitation bound it.
    pub(crate) fn trust_fingerprint(&self) -> Result<Fingerprint, EnrollmentError> {
        self.trust.fingerprint_as(self.schema)
    }
}
/// Operator-delivered bearer material. The explicit encoding operation exposes
/// the invitation; Debug and errors always redact its secret.
#[derive(Clone)]
pub struct Invitation {
    pub(crate) data: InvitationData,
}
impl std::fmt::Debug for Invitation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Invitation")
            .field("cluster", &hex(&self.data.cluster))
            .field("role", &self.data.role)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}
impl Invitation {
    pub fn id(&self) -> InvitationId {
        self.data.id
    }
    pub fn cluster(&self) -> ClusterId {
        self.data.cluster
    }
    pub fn role(&self) -> EnrollmentRole {
        self.data.role
    }
    pub fn expires_at(&self) -> i64 {
        self.data.expires_at
    }
    pub fn trust(&self) -> &ServerTrust {
        &self.data.trust
    }
    /// The token as a schema 1 binary wrote it, for the upgrade test.
    #[cfg(test)]
    pub(crate) fn expose_token_as_schema_one_for_tests(
        &self,
    ) -> Result<Zeroizing<String>, EnrollmentError> {
        if self.data.trust.successor_fingerprint.is_some() || self.data.trust.issuers.len() != 1 {
            return Err(EnrollmentError::Invalid);
        }
        let legacy = InvitationDataV1 {
            schema: INVITATION_SCHEMA_V1,
            id: self.data.id,
            cluster: self.data.cluster,
            role: self.data.role,
            expires_at: self.data.expires_at,
            secret: self.data.secret.clone(),
            trust: ServerTrustV1 {
                endpoint: self.data.trust.endpoint.clone(),
                server_name: self.data.trust.server_name.clone(),
                ca_certificate: self.data.trust.ca_certificate.clone(),
                server_fingerprint: self.data.trust.server_fingerprint,
            },
        };
        let bytes = Zeroizing::new(encode(&legacy)?);
        Ok(Zeroizing::new(format!("focal-invite-v1:{}", hex(&bytes))))
    }
    pub fn expose_token(&self) -> Result<Zeroizing<String>, EnrollmentError> {
        let bytes = Zeroizing::new(encode(&self.data)?);
        Ok(Zeroizing::new(format!("focal-invite-v1:{}", hex(&bytes))))
    }
    pub fn parse(token: &str) -> Result<Self, EnrollmentError> {
        let text = token
            .strip_prefix("focal-invite-v1:")
            .ok_or(EnrollmentError::Invalid)?;
        if text.len() > MAX_MESSAGE_BYTES * 2 || text.len() % 2 != 0 {
            return Err(EnrollmentError::Capacity);
        }
        let mut bytes = Zeroizing::new(Vec::with_capacity(text.len() / 2));
        for pair in text.as_bytes().chunks_exact(2) {
            let digit = |v: u8| match v {
                b'0'..=b'9' => Ok(v.saturating_sub(b'0')),
                b'a'..=b'f' => Ok(v.saturating_sub(b'a').saturating_add(10)),
                _ => Err(EnrollmentError::Invalid),
            };
            let [high, low] = pair else {
                return Err(EnrollmentError::Invalid);
            };
            bytes.push((digit(*high)? << 4) | digit(*low)?);
        }
        let data = InvitationData::decode_any(&bytes)?;
        if !(data.schema == INVITATION_SCHEMA_V1
            || data.schema == INVITATION_SCHEMA_V2
            || data.schema == INVITATION_SCHEMA)
            || data.id == [0; 16]
            || data.cluster == [0; 16]
            || data.secret.0.len() != 32
        {
            return Err(EnrollmentError::Invalid);
        }
        data.trust.validate()?;
        Ok(Self { data })
    }
    /// Ordinary TLS 1.3 chain/name verification; no custom verifier or 0-RTT.
    pub fn client_config(&self) -> Result<rustls::ClientConfig, EnrollmentError> {
        self.data.trust.client_config()
    }
    /// Call only on the established connection that will carry this request.
    /// There is no token-bearing request until normal PKI and the exact leaf pin
    /// have both passed; application bytes and early data are unnecessary.
    pub fn request_after_tls(
        &self,
        connection: &rustls::ClientConnection,
        key: &JoinKey,
        now: i64,
    ) -> Result<JoinRequest, EnrollmentError> {
        if connection.is_handshaking() || connection.alpn_protocol() != Some(ENROLLMENT_ALPN) {
            return Err(EnrollmentError::Unauthorized);
        }
        self.data.trust.verify_chain(
            connection
                .peer_certificates()
                .ok_or(EnrollmentError::Unauthorized)?,
            now,
        )?;
        self.request(key, now)
    }
    pub fn request_after_quic(
        &self,
        connection: &quinn::Connection,
        key: &JoinKey,
        now: i64,
    ) -> Result<JoinRequest, EnrollmentError> {
        if connection.close_reason().is_some() {
            return Err(EnrollmentError::Unauthorized);
        }
        let handshake = connection
            .handshake_data()
            .ok_or(EnrollmentError::Unauthorized)?
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .map_err(|_| EnrollmentError::Unauthorized)?;
        if handshake.protocol.as_deref() != Some(ENROLLMENT_ALPN) {
            return Err(EnrollmentError::Unauthorized);
        }
        let peer = connection
            .peer_identity()
            .ok_or(EnrollmentError::Unauthorized)?
            .downcast::<Vec<CertificateDer<'static>>>()
            .map_err(|_| EnrollmentError::Unauthorized)?;
        self.data.trust.verify_chain(&peer, now)?;
        self.request(key, now)
    }
    pub(crate) fn request(&self, key: &JoinKey, now: i64) -> Result<JoinRequest, EnrollmentError> {
        if self.data.cluster != key.cluster() {
            return Err(EnrollmentError::WrongCluster);
        }
        // Only the authority knows whether admission committed before expiry.
        // A saved join identity may recover that exact receipt afterwards.
        if now < 0 {
            return Err(EnrollmentError::Invalid);
        }
        Ok(JoinRequest {
            schema: 1,
            invitation: self.data.id,
            cluster: self.data.cluster,
            role: self.data.role,
            trust: self.data.trust_fingerprint()?,
            secret: self.data.secret.clone(),
            request: key.request_id(),
            csr: key.csr().to_vec(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRequest {
    pub(crate) schema: u16,
    pub(crate) invitation: InvitationId,
    pub(crate) cluster: ClusterId,
    pub(crate) role: EnrollmentRole,
    pub(crate) trust: Fingerprint,
    pub(crate) secret: SecretBytes,
    pub(crate) request: JoinId,
    pub(crate) csr: Vec<u8>,
}
impl std::fmt::Debug for JoinRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinRequest")
            .field("request", &hex(&self.request))
            .field("secret", &"[REDACTED]")
            .finish()
    }
}
impl JoinRequest {
    pub fn invitation_id(&self) -> InvitationId {
        self.invitation
    }
    pub fn request_id(&self) -> JoinId {
        self.request
    }
    /// Contains the secret; transmit solely on the connection just authenticated.
    pub fn encode(&self) -> Result<Zeroizing<Vec<u8>>, EnrollmentError> {
        Ok(Zeroizing::new(encode(self)?))
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, EnrollmentError> {
        let request: Self = decode(bytes)?;
        if request.schema != 1
            || request.secret.0.len() != 32
            || request.csr.len() > 4096
            || request.request == [0; 16]
        {
            return Err(EnrollmentError::Invalid);
        }
        Ok(request)
    }
}
