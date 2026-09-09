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
    pub ca_certificate: Vec<u8>,
    pub server_fingerprint: Fingerprint,
}
impl ServerTrust {
    /// Validate persisted trust before using its endpoint or building TLS state.
    pub fn validate(&self) -> Result<(), EnrollmentError> {
        if self.endpoint.is_empty()
            || self.endpoint.len() > 512
            || self.server_name.len() > 253
            || self.ca_certificate.len() > 4096
        {
            return Err(EnrollmentError::Capacity);
        }
        if self.server_fingerprint == [0; 32] || self.ca_certificate.is_empty() {
            return Err(EnrollmentError::Invalid);
        }
        ServerName::try_from(self.server_name.clone()).map_err(|_| EnrollmentError::Invalid)?;
        self.roots()?;
        Ok(())
    }
    pub(crate) fn fingerprint(&self) -> Result<Fingerprint, EnrollmentError> {
        Ok(hash("focal.enrollment.server-trust.v1", &encode(self)?))
    }
    /// Ordinary TLS 1.3 chain/name verification against the pinned CA, for
    /// an enrollment connection made by a node that already holds a
    /// credential; the exact leaf pin is checked on the connection.
    pub fn client_config(&self) -> Result<rustls::ClientConfig, EnrollmentError> {
        self.validate()?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| EnrollmentError::Crypto)?
            .with_root_certificates(self.roots()?)
            .with_no_client_auth();
        config.alpn_protocols = vec![ENROLLMENT_ALPN.to_vec()];
        config.enable_early_data = false;
        Ok(config)
    }
    /// Verify an established enrollment connection carries the pinned leaf.
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
        roots
            .add(CertificateDer::from(self.ca_certificate.clone()))
            .map_err(|_| EnrollmentError::Invalid)?;
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
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(self.roots()?),
            provider,
        )
        .build()
        .map_err(|_| EnrollmentError::Crypto)?;
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
        if server_fingerprint(first.as_ref()) != self.server_fingerprint {
            return Err(EnrollmentError::Unauthorized);
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
        let data: InvitationData = decode(&bytes)?;
        if data.schema != 1
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
        self.data.trust.validate()?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| EnrollmentError::Crypto)?
            .with_root_certificates(self.data.trust.roots()?)
            .with_no_client_auth();
        config.alpn_protocols = vec![ENROLLMENT_ALPN.to_vec()];
        config.enable_early_data = false;
        Ok(config)
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
            trust: self.data.trust.fingerprint()?,
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
