use crate::{
    invitation::InvitationData,
    pki::{SecretBytes, csr_key_hash, verify_issued, verify_issued_shape},
    *,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[path = "registry_admin.rs"]
mod admin;
pub use admin::{EnrolledCredentialStatus, InvitationStatus};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollmentLimits {
    pub max_invitations: usize,
    pub max_enrollments: usize,
    pub max_invitation_lifetime: u64,
    pub credential_lifetime: u64,
    pub max_checkpoint_bytes: usize,
    /// Tenants the cluster may admit; every grant names the admitted ones.
    pub max_tenants: usize,
    /// How long an issuer the cluster creates lasts (24 §11): the genesis
    /// issuer and every successor, each staged in the last third of it.
    /// Committed policy, as the credential lifetime is; at least
    /// `MIN_ISSUER_LIFETIMES` credential lifetimes.
    pub issuer_lifetime: u64,
}
/// The issuer lifetime a founding takes when its operator names none:
/// `DEFAULT_ISSUER_LIFETIMES` credential lifetimes — the ratio Let's
/// Encrypt keeps between its intermediates (three years) and the leaves
/// they issue (ninety days).
pub const DEFAULT_ISSUER_LIFETIMES: u64 = 12;
/// The least an issuer lasts, in credential lifetimes: its succession is
/// staged in the last third of its lifetime, and that third must hold the
/// activation — within a credential lifetime, every holder renews — and the
/// retirement of the issuer it succeeded, once every credential issued
/// under it has expired: another lifetime. A third of six is two.
pub const MIN_ISSUER_LIFETIMES: u64 = 6;
/// The longest an issuer lasts: ten years, the lifetime the genesis issuer
/// was created with before its succession existed — no successor outlives
/// what the cluster has lived with.
pub const MAX_ISSUER_LIFETIME: u64 = 10 * 365 * 86400;
impl Default for EnrollmentLimits {
    fn default() -> Self {
        Self {
            max_invitations: 4096,
            max_enrollments: 4096,
            max_invitation_lifetime: 86400,
            credential_lifetime: 30 * 86400,
            issuer_lifetime: DEFAULT_ISSUER_LIFETIMES * 30 * 86400,
            max_checkpoint_bytes: 8 * 1024 * 1024,
            max_tenants: 1024,
        }
    }
}
/// The shortest credential lifetime a registry admits, in seconds. The
/// registry decides in whole seconds and a renewal must be decided in a later
/// second than the issue it extends and before the expiry of the certificate
/// that proves it; the holder renews in the last third of the lifetime
/// (`focal_node::credential_renewal::renewal_window`), so the lifetime has
/// three seconds at least: one to be issued in, one to renew in, one to
/// expire in.
pub const MIN_CREDENTIAL_LIFETIME: u64 = 3;
/// The longest credential lifetime a registry admits: a year.
pub const MAX_CREDENTIAL_LIFETIME: u64 = 365 * 86400;
impl EnrollmentLimits {
    /// The issuer lifetime a cluster of `credential_lifetime` takes when
    /// none was committed: the default ratio, within the bound.
    pub fn issuer_lifetime_for(credential_lifetime: u64) -> u64 {
        credential_lifetime
            .saturating_mul(DEFAULT_ISSUER_LIFETIMES)
            .min(MAX_ISSUER_LIFETIME)
    }
    /// The capacity limits: what the process restoring a registry bounds
    /// (its memory), as opposed to the lifetimes, which are the committed
    /// policy of the cluster the registry belongs to.
    fn capacities(&self) -> (usize, usize, usize, usize) {
        (
            self.max_invitations,
            self.max_enrollments,
            self.max_checkpoint_bytes,
            self.max_tenants,
        )
    }
    pub fn validate(&self) -> Result<(), EnrollmentError> {
        if self.max_invitations == 0
            || self.max_invitations > 65536
            || self.max_enrollments == 0
            || self.max_enrollments > self.max_invitations
            || self.max_invitation_lifetime == 0
            || self.max_invitation_lifetime > 7 * 86400
            || self.credential_lifetime < MIN_CREDENTIAL_LIFETIME
            || self.credential_lifetime > MAX_CREDENTIAL_LIFETIME
            || self
                .credential_lifetime
                .checked_mul(MIN_ISSUER_LIFETIMES)
                .is_none_or(|least| self.issuer_lifetime < least)
            || self.issuer_lifetime > MAX_ISSUER_LIFETIME
            || !(16 * 1024..=64 * 1024 * 1024).contains(&self.max_checkpoint_bytes)
            || self.max_tenants == 0
            || self.max_tenants > 65536
        {
            return Err(EnrollmentError::Capacity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct InviteOptions {
    pub endpoint: String,
    pub server_name: String,
    pub role: EnrollmentRole,
    pub expires_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignedIdentity {
    pub cluster: ClusterId,
    pub role: EnrollmentRole,
    /// Issuance authorizes a node identity only, never Raft voter membership.
    pub node_id: Option<u64>,
    pub principal: [u8; 16],
    pub server_name: String,
}
/// The committed upgrade fence (24 §21): the capability level every node
/// of the cluster is held to. A binary announcing less refuses to serve;
/// features gated on a level open only once the fence reaches it. Level
/// zero is no fence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeFence {
    pub level: u32,
    pub activated_at: i64,
    /// The registry revision the activation committed at.
    pub revision: u64,
}
/// A bootstrap server certificate as the registry names it (24 §11): its
/// fingerprint and its validity, never the certificate itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ServerRecord {
    pub fingerprint: Fingerprint,
    pub issued_at: i64,
    pub expires_at: i64,
}
impl ServerRecord {
    pub fn of(certificate: &[u8]) -> Result<Self, EnrollmentError> {
        let (issued_at, expires_at) = crate::pki::certificate_validity(certificate)?;
        Ok(Self {
            fingerprint: server_fingerprint(certificate),
            issued_at,
            expires_at,
        })
    }
    /// A registry that predates the record names no certificate.
    pub fn is_unknown(&self) -> bool {
        self.fingerprint == [0; 32]
    }
    fn validate(&self) -> Result<(), EnrollmentError> {
        if self.is_unknown() || self.issued_at <= 0 || self.expires_at <= self.issued_at {
            return Err(EnrollmentError::Invalid);
        }
        Ok(())
    }
}
/// A bootstrap server certificate staged to succeed the current one: it is
/// presented once every invitation open when it was staged — an invitation
/// that pins the current certificate alone — has closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedRecord {
    pub record: ServerRecord,
    pub staged_at: i64,
    pub awaiting: std::collections::BTreeSet<InvitationId>,
}
/// The bootstrap server certificate the founder's enrollment endpoint
/// presents, as committed (24 §11): what joined nodes and invitations pin.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BootstrapServer {
    pub current: ServerRecord,
    pub successor: Option<StagedRecord>,
}
/// What an awaited invitation's id costs the checkpoint.
const AWAITING_CHARGE: usize = 32;
impl BootstrapServer {
    fn charge(&self) -> Result<usize, EnrollmentError> {
        self.successor
            .as_ref()
            .map_or(Some(0), |staged| {
                staged.awaiting.len().checked_mul(AWAITING_CHARGE)
            })
            .ok_or(EnrollmentError::Capacity)
    }
    fn validate(&self, max_invitations: usize) -> Result<(), EnrollmentError> {
        if self.current.is_unknown() {
            if self.successor.is_some() {
                return Err(EnrollmentError::Invalid);
            }
            return Ok(());
        }
        self.current.validate()?;
        if let Some(staged) = &self.successor {
            staged.record.validate()?;
            if staged.record.fingerprint == self.current.fingerprint
                || staged.staged_at <= 0
                || staged.awaiting.len() > max_invitations
            {
                return Err(EnrollmentError::Invalid);
            }
        }
        Ok(())
    }
}
/// The domain an issuer's fingerprint is taken in.
pub fn issuer_fingerprint(certificate: &[u8]) -> Fingerprint {
    hash("focal.enrollment.issuer.v1", certificate)
}
/// An issuer as the registry names it (24 §11): the self-signed CA
/// certificate credentials chain to, its endorsement by the issuer it
/// succeeded — a CA certificate for the same key and name under the
/// predecessor's signature, none for the genesis issuer — and its
/// validity. Every verifier adopts the record from the registry; one that
/// has not yet verifies a chain through the endorsement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuerRecord {
    pub fingerprint: Fingerprint,
    pub certificate: Vec<u8>,
    pub endorsement: Option<Vec<u8>>,
    pub issued_at: i64,
    pub expires_at: i64,
}
/// The most an issuer's certificate or endorsement may be.
const MAX_ISSUER_BYTES: usize = 4096;
impl IssuerRecord {
    pub fn of(certificate: &[u8], endorsement: Option<&[u8]>) -> Result<Self, EnrollmentError> {
        let (issued_at, expires_at) = crate::pki::certificate_validity(certificate)?;
        let record = Self {
            fingerprint: issuer_fingerprint(certificate),
            certificate: certificate.to_vec(),
            endorsement: endorsement.map(<[u8]>::to_vec),
            issued_at,
            expires_at,
        };
        record.validate()?;
        Ok(record)
    }
    /// The certificates a credential issued under this issuer presents
    /// beside its leaf: the issuer's own, then its endorsement.
    pub fn chain(&self) -> impl Iterator<Item = &[u8]> {
        std::iter::once(self.certificate.as_slice()).chain(self.endorsement.as_deref())
    }
    fn charge(&self) -> Result<usize, EnrollmentError> {
        self.certificate
            .len()
            .checked_add(self.endorsement.as_ref().map_or(0, Vec::len))
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or(EnrollmentError::Capacity)
    }
    fn validate(&self) -> Result<(), EnrollmentError> {
        if self.certificate.len() > MAX_ISSUER_BYTES
            || self
                .endorsement
                .as_ref()
                .is_some_and(|endorsement| endorsement.len() > MAX_ISSUER_BYTES)
        {
            return Err(EnrollmentError::Capacity);
        }
        if self.fingerprint != issuer_fingerprint(&self.certificate)
            || crate::pki::certificate_validity(&self.certificate)?
                != (self.issued_at, self.expires_at)
            || self.issued_at <= 0
            || self.expires_at <= self.issued_at
            || !crate::pki::is_issuer(&self.certificate)?
        {
            return Err(EnrollmentError::Invalid);
        }
        if let Some(endorsement) = &self.endorsement
            && !crate::pki::endorses(endorsement, &self.certificate)?
        {
            return Err(EnrollmentError::Invalid);
        }
        Ok(())
    }
}
/// An issuer staged to succeed the current one: committed, so every node
/// adopts it before anything is issued under it; activated at the next step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedIssuer {
    pub record: IssuerRecord,
    pub staged_at: i64,
}
/// The cluster's issuers as committed (24 §11): the one issuing now, one
/// staged to succeed it, and the one it succeeded while a credential
/// issued under it still lives. At most two generations overlap: a
/// successor is staged only once the previous issuer has retired.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuerSuccession {
    pub current: IssuerRecord,
    pub successor: Option<StagedIssuer>,
    pub retiring: Option<IssuerRecord>,
}
impl IssuerSuccession {
    /// The genesis issuer alone: the cluster's identity, issuing.
    pub fn genesis(ca_certificate: &[u8]) -> Result<Self, EnrollmentError> {
        Ok(Self {
            current: IssuerRecord::of(ca_certificate, None)?,
            successor: None,
            retiring: None,
        })
    }
    /// Every issuer a verifier trusts: current, staged, retiring.
    pub fn trusted(&self) -> impl Iterator<Item = &IssuerRecord> {
        std::iter::once(&self.current)
            .chain(self.successor.as_ref().map(|staged| &staged.record))
            .chain(self.retiring.as_ref())
    }
    /// The trusted issuers' certificates: the roots a verifier holds.
    pub fn roots(&self) -> impl Iterator<Item = &[u8]> {
        self.trusted().map(|record| record.certificate.as_slice())
    }
    /// The issuer that issued `certificate`, if a trusted one did.
    pub fn issuer_of(&self, certificate: &[u8]) -> Option<&IssuerRecord> {
        self.trusted()
            .find(|record| focal_wire::issued_by(certificate, &record.certificate).unwrap_or(false))
    }
    fn charge(&self) -> Result<usize, EnrollmentError> {
        self.trusted().try_fold(0usize, |total, record| {
            total
                .checked_add(record.charge()?)
                .ok_or(EnrollmentError::Capacity)
        })
    }
    fn validate(&self) -> Result<(), EnrollmentError> {
        for record in self.trusted() {
            record.validate()?;
        }
        let mut fingerprints: Vec<Fingerprint> =
            self.trusted().map(|record| record.fingerprint).collect();
        fingerprints.sort_unstable();
        fingerprints.dedup();
        if fingerprints.len() != self.trusted().count() {
            return Err(EnrollmentError::Invalid);
        }
        if let Some(staged) = &self.successor
            && (staged.staged_at <= 0
                || staged.record.endorsement.is_none()
                || self.retiring.is_some())
        {
            return Err(EnrollmentError::Invalid);
        }
        Ok(())
    }
}
/// A move of the issuer succession under the founder authority (24 §11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssuerChange {
    /// A successor the authority issued, endorsed by the current issuer.
    Stage(StagedIssuer),
    /// The staged successor issues from now; the current issuer retires
    /// once nothing live was issued under it.
    Activate,
    /// The retiring issuer is trusted no more: nothing live was issued
    /// under it.
    Retire,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollmentReceipt {
    pub invitation: InvitationId,
    pub request: JoinId,
    pub identity: AssignedIdentity,
    pub public_key: Fingerprint,
    pub csr_hash: Fingerprint,
    pub issued_at: i64,
    pub expires_at: i64,
    pub revision: u64,
    pub certificate: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InviteMetadata {
    id: InvitationId,
    cluster: ClusterId,
    role: EnrollmentRole,
    expires_at: i64,
    token_hash: Fingerprint,
    trust: Fingerprint,
    revoked: bool,
    receipt: Option<EnrollmentReceipt>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Change {
    Invite(InviteMetadata),
    Consume {
        invitation: InvitationId,
        csr: Vec<u8>,
        receipt: EnrollmentReceipt,
    },
    Revoke {
        invitation: InvitationId,
    },
    /// The same key under a fresh certificate and lifetime; the previous
    /// certificate keeps authorizing until `retire_previous_at`.
    Renew {
        invitation: InvitationId,
        receipt: EnrollmentReceipt,
        retire_previous_at: i64,
    },
    /// A tenant the cluster serves from now on: every grant issued from a
    /// certificate names it, and a node may create sessions under it.
    AdmitTenant {
        tenant: [u8; 16],
    },
    /// The same identity under a new key: a certificate issued for the new
    /// key's request, proven by the previous key; the previous certificate
    /// keeps authorizing until `retire_previous_at` (24 §11).
    Rotate {
        invitation: InvitationId,
        receipt: EnrollmentReceipt,
        retire_previous_at: i64,
    },
    /// The upgrade fence raised to `level` under the founder authority
    /// (24 §21); a fence only rises.
    ActivateFence {
        level: u32,
    },
    /// The bootstrap server certificate as the founder's authority holds it
    /// (24 §11): recorded once, a successor staged, the successor presented.
    BootstrapServer {
        current: ServerRecord,
        successor: Option<StagedRecord>,
    },
    /// The issuer succession moved under the founder authority (24 §11):
    /// a successor staged, the staged one activated, the retiring one
    /// retired.
    Issuer(IssuerChange),
}
/// A certificate a renewal replaced: still authorized for the grace the
/// authority decided, so connections and statements in flight complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RetiredCredential {
    invitation: InvitationId,
    receipt: EnrollmentReceipt,
    retire_at: i64,
}
/// A holder's request to renew its own credential: the same key and CSR, the
/// expiry it currently holds (so a retried request finds the committed
/// renewal instead of issuing again), signed by the credential it holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewRequest {
    schema: u16,
    cluster: ClusterId,
    invitation: InvitationId,
    request: JoinId,
    csr: Vec<u8>,
    holds_until: i64,
    proof: SignedNodeStatement,
}
/// A `RenewRequest` whose CSR carries a new key: a rotation.
pub const ROTATION_SCHEMA: u16 = 2;
impl RenewRequest {
    /// Whether this request rotates the key rather than renewing it.
    pub fn is_rotation(&self) -> bool {
        self.schema == ROTATION_SCHEMA
    }
    pub fn invitation_id(&self) -> InvitationId {
        self.invitation
    }
    pub fn request_id(&self) -> JoinId {
        self.request
    }
    pub fn holds_until(&self) -> i64 {
        self.holds_until
    }
    pub fn cluster(&self) -> ClusterId {
        self.cluster
    }
    pub fn encode(&self) -> Result<Vec<u8>, EnrollmentError> {
        encode(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, EnrollmentError> {
        decode(bytes)
    }
    fn statement(&self) -> Result<Vec<u8>, EnrollmentError> {
        encode(&(
            b"focal.enrollment.renew.v1",
            self.schema,
            self.cluster,
            self.invitation,
            self.request,
            hash("focal.enrollment.csr.v1", &self.csr),
            self.holds_until,
        ))
    }
}
impl CredentialMaterial {
    /// Ask for a rotation of the credential this material holds to the key
    /// `next` (a fresh join key with its own request identity and CSR):
    /// the request is proven by the current credential.
    pub fn rotation_request(
        &self,
        current: &JoinKey,
        next: &JoinKey,
        receipt: &EnrollmentReceipt,
    ) -> Result<RenewRequest, EnrollmentError> {
        if receipt.request != current.request_id()
            || receipt.csr_hash != hash("focal.enrollment.csr.v1", current.csr())
            || receipt.identity.cluster != current.cluster()
            || next.cluster() != current.cluster()
            || next.request_id() == current.request_id()
            || csr_key_hash(next.csr())? == receipt.public_key
        {
            return Err(EnrollmentError::Unauthorized);
        }
        let mut request = RenewRequest {
            schema: ROTATION_SCHEMA,
            cluster: current.cluster(),
            invitation: receipt.invitation,
            request: next.request_id(),
            csr: next.csr().to_vec(),
            holds_until: receipt.expires_at,
            proof: SignedNodeStatement {
                cluster: current.cluster(),
                certificate: Vec::new(),
                statement_hash: [0; 32],
                signature: Vec::new(),
            },
        };
        request.proof = self.sign_node_statement(current.cluster(), &request.statement()?)?;
        Ok(request)
    }
    /// Ask for a renewal of the credential this material holds.
    pub fn renewal_request(
        &self,
        key: &JoinKey,
        receipt: &EnrollmentReceipt,
    ) -> Result<RenewRequest, EnrollmentError> {
        if receipt.request != key.request_id()
            || receipt.csr_hash != hash("focal.enrollment.csr.v1", key.csr())
            || receipt.identity.cluster != key.cluster()
        {
            return Err(EnrollmentError::Unauthorized);
        }
        let mut request = RenewRequest {
            schema: 1,
            cluster: key.cluster(),
            invitation: receipt.invitation,
            request: receipt.request,
            csr: key.csr().to_vec(),
            holds_until: receipt.expires_at,
            proof: SignedNodeStatement {
                cluster: key.cluster(),
                certificate: Vec::new(),
                statement_hash: [0; 32],
                signature: Vec::new(),
            },
        };
        request.proof = self.sign_node_statement(key.cluster(), &request.statement()?)?;
        Ok(request)
    }
}
pub enum RenewPreparation {
    /// The registry already holds a receipt newer than the one the holder
    /// presented: the renewal committed before, or twice is refused.
    Existing(EnrollmentReceipt),
    Commit(EnrollmentCommand),
}
/// Safe for the metadata log: no invitation secret or private key is present.
/// Serialize this command, commit through the metadata authority, then apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollmentCommand {
    revision: u64,
    decided_at: i64,
    change: Change,
}
impl EnrollmentCommand {
    /// A command as a peer might commit it, for tests of what apply refuses.
    #[cfg(test)]
    pub(crate) fn for_tests(revision: u64, decided_at: i64, change: Change) -> Self {
        Self {
            revision,
            decided_at,
            change,
        }
    }
    pub fn expected_revision(&self) -> u64 {
        self.revision
    }
    pub fn encode(&self) -> Result<Vec<u8>, EnrollmentError> {
        encode(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, EnrollmentError> {
        decode(bytes)
    }
}
pub struct InvitationDraft {
    command: EnrollmentCommand,
    invitation: Invitation,
}
impl InvitationDraft {
    pub fn command(&self) -> &EnrollmentCommand {
        &self.command
    }
    /// Save the secret-bearing draft in a locked owner-only directory before
    /// proposing its public metadata command. This is local recovery custody;
    /// it does not release the invitation to a caller or joining process.
    pub fn persist(
        self,
        path: impl AsRef<std::path::Path>,
        intent_hash: Fingerprint,
    ) -> Result<PendingInvitation, EnrollmentError> {
        let directory = crate::files::PrivateDirectory::open(path.as_ref())?;
        if directory.read("invitation.bin")?.is_some() {
            return Err(EnrollmentError::Conflict);
        }
        let saved = SavedInvitation {
            schema: 1,
            intent_hash,
            command: self.command,
            invitation: self.invitation.data,
        };
        let bytes = zeroize::Zeroizing::new(encode(&saved)?);
        directory.install_new("invitation.bin", &bytes)?;
        Ok(PendingInvitation {
            saved,
            _directory: directory,
        })
    }
    /// An uncommitted invitation cannot be printed or delivered by this API.
    pub fn release(self, registry: &EnrollmentRegistry) -> Result<Invitation, EnrollmentError> {
        let Change::Invite(expected) = &self.command.change else {
            return Err(EnrollmentError::Invalid);
        };
        let actual = registry
            .records
            .get(&expected.id)
            .ok_or(EnrollmentError::NotCommitted)?;
        if actual != expected {
            return Err(EnrollmentError::Conflict);
        }
        Ok(self.invitation)
    }
}
#[derive(Serialize, Deserialize)]
struct SavedInvitation {
    schema: u16,
    intent_hash: Fingerprint,
    command: EnrollmentCommand,
    invitation: InvitationData,
}
impl SavedInvitation {
    /// Decode a saved draft; one saved with a schema 1 invitation (one pin)
    /// decodes by that invitation's layout.
    fn decode_any(bytes: &[u8]) -> Result<Self, EnrollmentError> {
        if bytes.len() > MAX_MESSAGE_BYTES {
            return Err(EnrollmentError::Capacity);
        }
        let (schema, rest) = postcard::take_from_bytes::<u16>(bytes)?;
        let (intent_hash, rest) = postcard::take_from_bytes::<Fingerprint>(rest)?;
        let (command, rest) = postcard::take_from_bytes::<EnrollmentCommand>(rest)?;
        let invitation = InvitationData::decode_any(rest)?;
        Ok(Self {
            schema,
            intent_hash,
            command,
            invitation,
        })
    }
}
/// Durable private retry state. Secret material has no Debug/Serialize surface;
/// the only public release method checks the committed enrollment registry.
pub struct PendingInvitation {
    saved: SavedInvitation,
    _directory: crate::files::PrivateDirectory,
}
impl PendingInvitation {
    pub fn open(
        path: impl AsRef<std::path::Path>,
        cluster: ClusterId,
    ) -> Result<Self, EnrollmentError> {
        let directory = crate::files::PrivateDirectory::open(path.as_ref())?;
        let bytes = directory
            .read("invitation.bin")?
            .ok_or(EnrollmentError::Corrupt)?;
        let saved = SavedInvitation::decode_any(&bytes)?;
        let Change::Invite(record) = &saved.command.change else {
            return Err(EnrollmentError::Corrupt);
        };
        let data = &saved.invitation;
        if saved.schema != 1
            || !(data.schema == crate::invitation::INVITATION_SCHEMA_V1
                || data.schema == crate::invitation::INVITATION_SCHEMA)
            || data.cluster != cluster
            || record.cluster != cluster
            || record.id != data.id
            || data.id == [0; 16]
            || record.role != data.role
            || record.expires_at != data.expires_at
            || data.secret.0.len() != 32
            || record.token_hash != token_hash(&data.secret.0)
            || record.trust != data.trust_fingerprint()?
            || record.revoked
            || record.receipt.is_some()
        {
            return Err(EnrollmentError::Corrupt);
        }
        data.trust.validate()?;
        Ok(Self {
            saved,
            _directory: directory,
        })
    }
    pub fn intent_hash(&self) -> Fingerprint {
        self.saved.intent_hash
    }
    pub fn id(&self) -> InvitationId {
        self.saved.invitation.id
    }
    pub fn command(&self) -> &EnrollmentCommand {
        &self.saved.command
    }
    pub fn release(&self, registry: &EnrollmentRegistry) -> Result<Invitation, EnrollmentError> {
        let Change::Invite(expected) = &self.saved.command.change else {
            return Err(EnrollmentError::Corrupt);
        };
        let actual = registry
            .records
            .get(&expected.id)
            .ok_or(EnrollmentError::NotCommitted)?;
        if actual.revoked {
            return Err(EnrollmentError::Revoked);
        }
        // Consuming an invitation does not destroy exact retry custody. The
        // same committed token still permits only its original CSR/request.
        let mut immutable = actual.clone();
        immutable.receipt = None;
        if immutable != *expected {
            return Err(EnrollmentError::Conflict);
        }
        Ok(Invitation {
            data: self.saved.invitation.clone(),
        })
    }
}
#[derive(Debug)]
pub enum JoinPreparation {
    Commit(EnrollmentCommand),
    Existing(EnrollmentReceipt),
}

/// A fully validated replacement prepared under the host's memory reservation.
/// Publication only swaps owned state after the metadata commit is durable.
pub struct PreparedEnrollmentUpdate {
    owner: Option<Fingerprint>,
    base_revision: u64,
    base_index: u64,
    next: EnrollmentRegistry,
}
impl PreparedEnrollmentUpdate {
    pub fn charged_bytes(&self) -> usize {
        self.next.charged_bytes
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrollmentRegistry {
    // Private, owned publication lineage. Raw Deserialize leaves it absent;
    // only validated new/restore assigns fresh OS randomness. No Arc is needed.
    #[serde(skip)]
    owner: Option<Fingerprint>,
    schema: u16,
    cluster: ClusterId,
    ca_certificate: Vec<u8>,
    limits: EnrollmentLimits,
    revision: u64,
    applied_index: u64,
    time_floor: i64,
    next_node: u64,
    charged_bytes: usize,
    records: BTreeMap<InvitationId, InviteMetadata>,
    certificates: BTreeMap<Fingerprint, InvitationId>,
    enrolled_keys: BTreeMap<Fingerprint, InvitationId>,
    /// Certificates a renewal replaced, by fingerprint, until they retire.
    retired: BTreeMap<Fingerprint, RetiredCredential>,
    /// Tenants admitted by the founder authority, in admission order of
    /// identity; bounded by `EnrollmentLimits::max_tenants`.
    tenants: std::collections::BTreeSet<[u8; 16]>,
    /// The committed upgrade fence (24 §21); zero until one is activated.
    fence: UpgradeFence,
    /// The bootstrap server certificate the enrollment endpoint presents
    /// (24 §11); unknown in a registry founded before it was recorded.
    bootstrap: BootstrapServer,
    /// The issuers credentials chain to (24 §11): the genesis issuer in a
    /// registry founded before the succession was recorded.
    issuer: IssuerSuccession,
}
/// The limits as schemas 3 to 5 wrote them, before the issuer lifetime.
#[derive(Deserialize)]
struct LimitsV3 {
    max_invitations: usize,
    max_enrollments: usize,
    max_invitation_lifetime: u64,
    credential_lifetime: u64,
    max_checkpoint_bytes: usize,
    max_tenants: usize,
}
impl From<LimitsV3> for EnrollmentLimits {
    fn from(legacy: LimitsV3) -> Self {
        Self {
            max_invitations: legacy.max_invitations,
            max_enrollments: legacy.max_enrollments,
            max_invitation_lifetime: legacy.max_invitation_lifetime,
            credential_lifetime: legacy.credential_lifetime,
            max_checkpoint_bytes: legacy.max_checkpoint_bytes,
            max_tenants: legacy.max_tenants,
            issuer_lifetime: Self::issuer_lifetime_for(legacy.credential_lifetime),
        }
    }
}
/// The limits as schemas 3 to 5 wrote them, for the upgrade tests.
#[cfg(test)]
#[derive(Serialize)]
struct LegacyLimits {
    max_invitations: usize,
    max_enrollments: usize,
    max_invitation_lifetime: u64,
    credential_lifetime: u64,
    max_checkpoint_bytes: usize,
    max_tenants: usize,
}
#[cfg(test)]
impl LegacyLimits {
    fn of(limits: &EnrollmentLimits) -> Self {
        Self {
            max_invitations: limits.max_invitations,
            max_enrollments: limits.max_enrollments,
            max_invitation_lifetime: limits.max_invitation_lifetime,
            credential_lifetime: limits.credential_lifetime,
            max_checkpoint_bytes: limits.max_checkpoint_bytes,
            max_tenants: limits.max_tenants,
        }
    }
}
/// The registry as schema 5 wrote it, before the issuer succession.
#[derive(Deserialize)]
struct RegistryV5 {
    schema: u16,
    cluster: ClusterId,
    ca_certificate: Vec<u8>,
    limits: LimitsV3,
    revision: u64,
    applied_index: u64,
    time_floor: i64,
    next_node: u64,
    charged_bytes: usize,
    records: BTreeMap<InvitationId, InviteMetadata>,
    certificates: BTreeMap<Fingerprint, InvitationId>,
    enrolled_keys: BTreeMap<Fingerprint, InvitationId>,
    retired: BTreeMap<Fingerprint, RetiredCredential>,
    tenants: std::collections::BTreeSet<[u8; 16]>,
    fence: UpgradeFence,
    bootstrap: BootstrapServer,
}
/// The registry as schema 4 wrote it, before the bootstrap server record.
#[derive(Deserialize)]
struct RegistryV4 {
    schema: u16,
    cluster: ClusterId,
    ca_certificate: Vec<u8>,
    limits: LimitsV3,
    revision: u64,
    applied_index: u64,
    time_floor: i64,
    next_node: u64,
    charged_bytes: usize,
    records: BTreeMap<InvitationId, InviteMetadata>,
    certificates: BTreeMap<Fingerprint, InvitationId>,
    enrolled_keys: BTreeMap<Fingerprint, InvitationId>,
    retired: BTreeMap<Fingerprint, RetiredCredential>,
    tenants: std::collections::BTreeSet<[u8; 16]>,
    fence: UpgradeFence,
}
/// The registry as schema 3 wrote it, before the upgrade fence.
#[derive(Deserialize)]
struct RegistryV3 {
    schema: u16,
    cluster: ClusterId,
    ca_certificate: Vec<u8>,
    limits: LimitsV3,
    revision: u64,
    applied_index: u64,
    time_floor: i64,
    next_node: u64,
    charged_bytes: usize,
    records: BTreeMap<InvitationId, InviteMetadata>,
    certificates: BTreeMap<Fingerprint, InvitationId>,
    enrolled_keys: BTreeMap<Fingerprint, InvitationId>,
    retired: BTreeMap<Fingerprint, RetiredCredential>,
    tenants: std::collections::BTreeSet<[u8; 16]>,
}
/// The registry's persisted layout; schema 2 (before admitted tenants)
/// restores with none, schema 3 without a fence, schema 4 without the
/// bootstrap server record, schema 5 with the genesis issuer alone; any
/// other schema is not this registry.
const REGISTRY_SCHEMA: u16 = 6;
/// The schema 2 layout, converted on restore.
#[derive(Deserialize)]
struct RegistryV2 {
    schema: u16,
    cluster: ClusterId,
    ca_certificate: Vec<u8>,
    limits: LimitsV2,
    revision: u64,
    applied_index: u64,
    time_floor: i64,
    next_node: u64,
    charged_bytes: usize,
    records: BTreeMap<InvitationId, InviteMetadata>,
    certificates: BTreeMap<Fingerprint, InvitationId>,
    enrolled_keys: BTreeMap<Fingerprint, InvitationId>,
    retired: BTreeMap<Fingerprint, RetiredCredential>,
}
#[derive(Deserialize)]
struct LimitsV2 {
    max_invitations: usize,
    max_enrollments: usize,
    max_invitation_lifetime: u64,
    credential_lifetime: u64,
    max_checkpoint_bytes: usize,
}
impl EnrollmentRegistry {
    pub fn new(
        cluster: ClusterId,
        ca_certificate: Vec<u8>,
        next_node: u64,
        limits: EnrollmentLimits,
    ) -> Result<Self, EnrollmentError> {
        limits.validate()?;
        if cluster == [0; 16] || next_node == 0 || ca_certificate.len() > 4096 {
            return Err(EnrollmentError::Invalid);
        }
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(ca_certificate.clone().into())
            .map_err(|_| EnrollmentError::Invalid)?;
        let issuer = IssuerSuccession::genesis(&ca_certificate)?;
        let charged_bytes = 8192usize
            .checked_add(issuer.charge()?)
            .ok_or(EnrollmentError::Capacity)?;
        Ok(Self {
            owner: Some(random()?),
            schema: REGISTRY_SCHEMA,
            cluster,
            ca_certificate,
            limits,
            revision: 0,
            applied_index: 0,
            time_floor: 0,
            next_node,
            charged_bytes,
            records: BTreeMap::new(),
            certificates: BTreeMap::new(),
            enrolled_keys: BTreeMap::new(),
            retired: BTreeMap::new(),
            tenants: std::collections::BTreeSet::new(),
            fence: UpgradeFence::default(),
            bootstrap: BootstrapServer::default(),
            issuer,
        })
    }
    // Only the private new-genesis draft constructor can select the existing
    // founding identity. Ordinary enrollment always uses assigned()/next_node.
    pub(crate) fn founding(
        authority: &BootstrapAuthority,
        key: &JoinKey,
        node: u64,
        principal: [u8; 16],
        limits: EnrollmentLimits,
        level: u32,
        now: i64,
    ) -> Result<(Self, EnrollmentReceipt), EnrollmentError> {
        let next_node = node.checked_add(1).ok_or(EnrollmentError::Capacity)?;
        let mut registry = Self::new(
            authority.cluster(),
            authority.ca_certificate().to_vec(),
            next_node,
            limits,
        )?;
        registry.bootstrap.current = ServerRecord::of(authority.server_certificate())?;
        registry.check_time(now)?;
        let public_key = csr_key_hash(key.csr())?;
        let mut identity = assigned(registry.cluster, EnrollmentRole::Node, node, public_key);
        identity.principal = principal;
        let lifetime = registry.limits.credential_lifetime;
        let receipt = EnrollmentReceipt {
            invitation: random()?,
            request: key.request_id(),
            identity,
            public_key,
            csr_hash: hash("focal.enrollment.csr.v1", key.csr()),
            issued_at: now,
            expires_at: now
                .checked_add(i64::try_from(lifetime).map_err(|_| EnrollmentError::Capacity)?)
                .ok_or(EnrollmentError::Capacity)?,
            revision: 1,
            certificate: Vec::new(),
        };
        let receipt = EnrollmentReceipt {
            certificate: authority.issue_founder(key.csr(), &receipt.identity, now, lifetime)?,
            ..receipt
        };
        verify_issued(&receipt, registry.trust_roots())?;
        // The cluster is founded at the founding binary's capability level
        // (24 §21): its one node runs it, so the fence is that level from
        // genesis, and behaviour gated on it is open from the start.
        registry.fence = if level == 0 {
            UpgradeFence::default()
        } else {
            UpgradeFence {
                level,
                activated_at: now,
                revision: 1,
            }
        };
        let record = InviteMetadata {
            id: receipt.invitation,
            cluster: registry.cluster,
            role: EnrollmentRole::Node,
            expires_at: receipt.expires_at,
            // No secret exists for this initial local identity. There is no
            // bootstrap invitation that can be reused for another CSR.
            token_hash: random()?,
            trust: server_fingerprint(authority.server_certificate()),
            revoked: false,
            receipt: Some(receipt.clone()),
        };
        let bytes = record_charge(&record)?;
        registry.reserve(bytes)?;
        registry.charged_bytes = registry
            .charged_bytes
            .checked_add(bytes)
            .ok_or(EnrollmentError::Capacity)?;
        registry
            .certificates
            .insert(server_fingerprint(&receipt.certificate), receipt.invitation);
        registry
            .enrolled_keys
            .insert(public_key, receipt.invitation);
        registry.records.insert(receipt.invitation, record);
        registry.revision = 1;
        registry.time_floor = now;
        Ok((registry, receipt))
    }
    pub fn cluster(&self) -> ClusterId {
        self.cluster
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }
    pub fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }
    pub fn limits(&self) -> &EnrollmentLimits {
        &self.limits
    }
    pub fn prepare_command(
        &self,
        command: &EnrollmentCommand,
    ) -> Result<PreparedEnrollmentUpdate, EnrollmentError> {
        if self.owner.is_none() {
            return Err(EnrollmentError::Conflict);
        }
        let mut next = self.clone();
        next.apply_committed(
            command,
            self.applied_index
                .checked_add(1)
                .ok_or(EnrollmentError::Capacity)?,
        )?;
        Ok(PreparedEnrollmentUpdate {
            owner: self.owner,
            base_revision: self.revision,
            base_index: self.applied_index,
            next,
        })
    }
    pub fn publish(
        &mut self,
        mut prepared: PreparedEnrollmentUpdate,
        committed_index: u64,
    ) -> Result<(), EnrollmentError> {
        if self.owner.is_none()
            || self.owner != prepared.owner
            || self.revision != prepared.base_revision
            || self.applied_index != prepared.base_index
        {
            return Err(EnrollmentError::Conflict);
        }
        if committed_index <= self.applied_index {
            return Err(EnrollmentError::NotCommitted);
        }
        prepared.next.applied_index = committed_index;
        *self = prepared.next;
        Ok(())
    }
    pub fn ca_certificate(&self) -> &[u8] {
        &self.ca_certificate
    }
    /// Revocations remain represented; call authorize_certificate before granting
    /// access when rebuilding the transport registry from these public records.
    pub fn enrollments(&self) -> impl Iterator<Item = &EnrollmentReceipt> {
        self.records
            .values()
            .filter_map(|record| record.receipt.as_ref())
    }
    /// Certificates a renewal replaced that still authorize at `now`, with the
    /// receipt they were issued under and the moment they retire.
    pub fn retired(&self, now: i64) -> impl Iterator<Item = (&EnrollmentReceipt, i64)> {
        self.retired.values().filter_map(move |retired| {
            let record = self.records.get(&retired.invitation)?;
            (!record.revoked && now < retired.retire_at)
                .then_some((&retired.receipt, retired.retire_at))
        })
    }
    pub fn invitation_revoked(&self, id: InvitationId) -> Result<bool, EnrollmentError> {
        self.records
            .get(&id)
            .map(|record| record.revoked)
            .ok_or(EnrollmentError::Unauthorized)
    }
    /// The tenants the cluster serves besides the founder's own.
    pub fn tenants(&self) -> impl Iterator<Item = [u8; 16]> + '_ {
        self.tenants.iter().copied()
    }
    pub fn admits_tenant(&self, tenant: [u8; 16]) -> bool {
        self.tenants.contains(&tenant)
    }
    /// The issuers credentials chain to, as committed (24 §11).
    pub fn issuers(&self) -> &IssuerSuccession {
        &self.issuer
    }
    /// The roots a verifier of this cluster's credentials holds.
    pub fn trust_roots(&self) -> impl Iterator<Item = &[u8]> {
        self.issuer.roots()
    }
    /// Whether a credential issued under the retiring issuer still lives
    /// at `now` — a reason the issuer is trusted on — or the bootstrap
    /// server certificate the authority presents is under it.
    pub fn retiring_issuer_in_use(&self, authority: &BootstrapAuthority, now: i64) -> bool {
        let Some(retiring) = &self.issuer.retiring else {
            return false;
        };
        // A certificate that cannot be read is held to be under it: the
        // retiring issuer stays trusted rather than be dropped on a doubt.
        let under = |certificate: &[u8]| {
            focal_wire::issued_by(certificate, &retiring.certificate).unwrap_or(true)
        };
        self.live_receipts(now)
            .any(|receipt| under(&receipt.certificate))
            || under(authority.server_certificate())
    }
    /// Every receipt — current or retired — whose certificate is valid at
    /// `now`.
    fn live_receipts(&self, now: i64) -> impl Iterator<Item = &EnrollmentReceipt> {
        self.records
            .values()
            .filter_map(|record| record.receipt.as_ref())
            .chain(self.retired.values().map(|retired| &retired.receipt))
            .filter(move |receipt| receipt.expires_at > now)
    }
    /// The committed upgrade fence (24 §21).
    pub fn fence(&self) -> UpgradeFence {
        self.fence
    }
    /// The bootstrap server certificate as committed (24 §11).
    pub fn bootstrap(&self) -> &BootstrapServer {
        &self.bootstrap
    }
    /// The invitations still open at `now`: unredeemed, unrevoked, unexpired.
    fn open_invitations(&self, now: i64) -> std::collections::BTreeSet<InvitationId> {
        self.records
            .values()
            .filter(|record| !record.revoked && record.receipt.is_none() && record.expires_at > now)
            .map(|record| record.id)
            .collect()
    }
    /// Whether every invitation open when the successor was staged has
    /// closed, so no joiner pins the current certificate alone any more.
    pub fn bootstrap_ready_to_activate(&self, now: i64) -> bool {
        self.bootstrap.successor.as_ref().is_some_and(|staged| {
            staged.awaiting.iter().all(|id| {
                self.records.get(id).is_none_or(|record| {
                    record.revoked || record.receipt.is_some() || record.expires_at <= now
                })
            })
        })
    }
    /// The next committed step of the bootstrap server certificate's
    /// succession under the founder authority (24 §11), from what the
    /// authority holds: the record of a certificate the registry does not
    /// name yet, the staging of a successor the authority issued, or the
    /// activation of a staged successor once every invitation open at its
    /// staging has closed. None when the registry says what the authority
    /// holds, or the activation committed and the authority has yet to
    /// present it (`activate_successor`).
    pub fn prepare_bootstrap_server(
        &self,
        authority: &BootstrapAuthority,
        now: i64,
    ) -> Result<Option<EnrollmentCommand>, EnrollmentError> {
        self.check_time(now)?;
        self.check_authority(authority)?;
        let current = ServerRecord::of(authority.server_certificate())?;
        let staged = authority
            .successor()
            .map(|(certificate, staged_at)| {
                Ok::<_, EnrollmentError>((ServerRecord::of(certificate)?, staged_at))
            })
            .transpose()?;
        let committed = &self.bootstrap;
        let stage = |record: ServerRecord, staged_at: i64| StagedRecord {
            record,
            staged_at,
            awaiting: self.open_invitations(now),
        };
        let change = if committed.current.is_unknown() {
            Change::BootstrapServer {
                current,
                successor: staged.map(|(record, staged_at)| stage(record, staged_at)),
            }
        } else if committed.current.fingerprint == current.fingerprint {
            match (&committed.successor, staged) {
                (None, None) => return Ok(None),
                (None, Some((record, staged_at))) => Change::BootstrapServer {
                    current,
                    successor: Some(stage(record, staged_at)),
                },
                (Some(known), Some((record, _)))
                    if known.record.fingerprint == record.fingerprint =>
                {
                    if !self.bootstrap_ready_to_activate(now) {
                        return Ok(None);
                    }
                    Change::BootstrapServer {
                        current: record,
                        successor: None,
                    }
                }
                _ => return Err(EnrollmentError::Conflict),
            }
        } else if committed.successor.is_none()
            && staged.is_some_and(|(record, _)| record.fingerprint == committed.current.fingerprint)
        {
            return Ok(None);
        } else {
            return Err(EnrollmentError::Conflict);
        };
        Ok(Some(EnrollmentCommand {
            revision: self.revision,
            decided_at: now,
            change,
        }))
    }
    /// Raise the upgrade fence to `level` under the founder authority: a
    /// conflict when the fence is there already (an operator's retry reads
    /// that as done), invalid when it would lower the fence.
    pub fn prepare_activate_fence(
        &self,
        authority: &BootstrapAuthority,
        level: u32,
        now: i64,
    ) -> Result<EnrollmentCommand, EnrollmentError> {
        self.check_time(now)?;
        self.check_authority(authority)?;
        if level == 0 || level < self.fence.level {
            return Err(EnrollmentError::Invalid);
        }
        if level == self.fence.level {
            return Err(EnrollmentError::Conflict);
        }
        Ok(EnrollmentCommand {
            revision: self.revision,
            decided_at: now,
            change: Change::ActivateFence { level },
        })
    }
    /// The next committed step of the issuer succession under the founder
    /// authority (24 §11), from what the authority holds: the staging of a
    /// successor the authority issued and endorsed, the activation of a
    /// staged successor, or the retirement of the issuer it succeeded once
    /// nothing live was issued under it. None when the registry says what
    /// the authority holds, or an activation committed that the authority
    /// has yet to adopt (`BootstrapAuthority::activate_issuer`).
    pub fn prepare_issuer(
        &self,
        authority: &BootstrapAuthority,
        now: i64,
    ) -> Result<Option<EnrollmentCommand>, EnrollmentError> {
        self.check_time(now)?;
        self.check_authority(authority)?;
        let issuing = authority.issuer_record()?;
        let change = match (&self.issuer.successor, authority.issuer_successor()?) {
            // The registry names the successor the authority staged; the
            // authority still issues under the predecessor: activate.
            (Some(staged), Some(held)) if staged.record == held => IssuerChange::Activate,
            // The registry's current issuer is what the authority staged:
            // the activation committed and the authority adopts it.
            (_, Some(held)) if self.issuer.current == held => return Ok(None),
            (None, Some(held)) => {
                if self.issuer.retiring.is_some() || self.issuer.current != issuing {
                    return Ok(None);
                }
                IssuerChange::Stage(StagedIssuer {
                    record: held,
                    staged_at: now,
                })
            }
            (Some(_), Some(_)) => return Err(EnrollmentError::Conflict),
            (Some(_), None) => {
                // A staged successor the authority no longer holds: the
                // authority adopted it, the registry has yet to.
                if self
                    .issuer
                    .successor
                    .as_ref()
                    .is_some_and(|staged| staged.record == issuing)
                {
                    IssuerChange::Activate
                } else {
                    return Err(EnrollmentError::Conflict);
                }
            }
            (None, None) => {
                if self.issuer.current != issuing {
                    return Err(EnrollmentError::Conflict);
                }
                if self.issuer.retiring.is_none() || self.retiring_issuer_in_use(authority, now) {
                    return Ok(None);
                }
                IssuerChange::Retire
            }
        };
        Ok(Some(EnrollmentCommand {
            revision: self.revision,
            decided_at: now,
            change: Change::Issuer(change),
        }))
    }
    /// Admit a tenant under the founder authority: a conflict when it is
    /// admitted already (an operator's retry reads that as done).
    pub fn prepare_admit_tenant(
        &self,
        authority: &BootstrapAuthority,
        tenant: [u8; 16],
        now: i64,
    ) -> Result<EnrollmentCommand, EnrollmentError> {
        self.check_time(now)?;
        self.check_authority(authority)?;
        if tenant == [0; 16] {
            return Err(EnrollmentError::Invalid);
        }
        if self.tenants.contains(&tenant) {
            return Err(EnrollmentError::Conflict);
        }
        if self.tenants.len() >= self.limits.max_tenants {
            return Err(EnrollmentError::Capacity);
        }
        Ok(EnrollmentCommand {
            revision: self.revision,
            decided_at: now,
            change: Change::AdmitTenant { tenant },
        })
    }
    pub fn prepare_invitation(
        &self,
        authority: &BootstrapAuthority,
        options: InviteOptions,
        now: i64,
    ) -> Result<InvitationDraft, EnrollmentError> {
        self.check_time(now)?;
        self.check_authority(authority)?;
        if self.records.len() >= self.limits.max_invitations {
            return Err(EnrollmentError::Capacity);
        }
        self.check_invitation_expiry(options.expires_at, now)?;
        // A successor the authority staged and the registry committed is
        // pinned beside the current certificate, so this invitation still
        // redeems once the successor is presented (24 §11).
        let successor_fingerprint = authority
            .successor()
            .map(|(certificate, _)| server_fingerprint(certificate))
            .filter(|staged| {
                self.bootstrap
                    .successor
                    .as_ref()
                    .is_some_and(|known| known.record.fingerprint == *staged)
            });
        let trust = ServerTrust {
            endpoint: options.endpoint,
            server_name: options.server_name,
            ca_certificate: self.ca_certificate.clone(),
            server_fingerprint: server_fingerprint(authority.server_certificate()),
            successor_fingerprint,
            issuers: self.issuer.trusted().cloned().collect(),
        };
        trust.validate()?;
        let chain: Vec<rustls::pki_types::CertificateDer<'_>> = authority
            .server_identity()
            .certificate_chain()
            .iter()
            .map(|certificate| certificate.clone().into())
            .collect();
        trust.verify_chain(&chain, now)?;
        let data = InvitationData {
            schema: crate::invitation::INVITATION_SCHEMA,
            id: random()?,
            cluster: self.cluster,
            role: options.role,
            expires_at: options.expires_at,
            secret: SecretBytes(random::<32>()?.to_vec()),
            trust,
        };
        let record = InviteMetadata {
            id: data.id,
            cluster: data.cluster,
            role: data.role,
            expires_at: data.expires_at,
            token_hash: token_hash(&data.secret.0),
            trust: data.trust_fingerprint()?,
            revoked: false,
            receipt: None,
        };
        self.reserve(record_charge(&record)?)?;
        let command = EnrollmentCommand {
            revision: self.revision,
            decided_at: now,
            change: Change::Invite(record),
        };
        Ok(InvitationDraft {
            command,
            invitation: Invitation { data },
        })
    }
    /// Rebase the same privately persisted token after a definitive metadata
    /// comparison rejection. Never call while an earlier proposal is unresolved.
    pub fn prepare_pending_invitation(
        &self,
        pending: &PendingInvitation,
        authority: &BootstrapAuthority,
        now: i64,
    ) -> Result<EnrollmentCommand, EnrollmentError> {
        self.check_time(now)?;
        self.check_authority(authority)?;
        let Change::Invite(record) = &pending.saved.command.change else {
            return Err(EnrollmentError::Corrupt);
        };
        if record.cluster != self.cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        if self.records.contains_key(&record.id) {
            return Err(EnrollmentError::Conflict);
        }
        if self.records.len() >= self.limits.max_invitations {
            return Err(EnrollmentError::Capacity);
        }
        self.check_invitation_expiry(record.expires_at, now)?;
        self.reserve(record_charge(record)?)?;
        Ok(EnrollmentCommand {
            revision: self.revision,
            decided_at: now,
            change: Change::Invite(record.clone()),
        })
    }
    pub fn prepare_join(
        &self,
        authority: &BootstrapAuthority,
        request: &JoinRequest,
        now: i64,
    ) -> Result<JoinPreparation, EnrollmentError> {
        self.check_authority(authority)?;
        let record = self.authenticate_request(request, now)?;
        if let Some(receipt) = &record.receipt {
            return Ok(JoinPreparation::Existing(receipt.clone()));
        }
        if self.certificates.len() >= self.limits.max_enrollments {
            return Err(EnrollmentError::Capacity);
        }
        let public_key = csr_key_hash(&request.csr)?;
        if self.enrolled_keys.contains_key(&public_key) {
            return Err(EnrollmentError::Used);
        }
        let identity = assigned(self.cluster, request.role, self.next_node, public_key);
        let expires_at = now
            .checked_add(
                i64::try_from(self.limits.credential_lifetime)
                    .map_err(|_| EnrollmentError::Invalid)?,
            )
            .ok_or(EnrollmentError::Invalid)?;
        let receipt = EnrollmentReceipt {
            invitation: request.invitation,
            request: request.request,
            identity,
            public_key,
            csr_hash: hash("focal.enrollment.csr.v1", &request.csr),
            issued_at: now,
            expires_at,
            revision: self
                .revision
                .checked_add(1)
                .ok_or(EnrollmentError::Capacity)?,
            certificate: vec![],
        };
        let certificate = authority.issue(
            &request.csr,
            &receipt.identity,
            now,
            self.limits.credential_lifetime,
        )?;
        let receipt = EnrollmentReceipt {
            certificate,
            ..receipt
        };
        let mut updated = record.clone();
        updated.receipt = Some(receipt.clone());
        self.reserve(
            record_charge(&updated)?
                .checked_sub(record_charge(record)?)
                .ok_or(EnrollmentError::Corrupt)?,
        )?;
        Ok(JoinPreparation::Commit(EnrollmentCommand {
            revision: self.revision,
            decided_at: now,
            change: Change::Consume {
                invitation: request.invitation,
                csr: request.csr.clone(),
                receipt,
            },
        }))
    }
    /// Renew the credential a holder presents: the same key and CSR receive a
    /// fresh certificate and lifetime, and the certificate it holds retires
    /// after `grace_seconds`. A holder presenting an expiry the registry has
    /// already moved past is answered with the committed renewal.
    pub fn prepare_renew(
        &self,
        authority: &BootstrapAuthority,
        request: &RenewRequest,
        now: i64,
        grace_seconds: u64,
    ) -> Result<RenewPreparation, EnrollmentError> {
        self.check_time(now)?;
        self.check_authority(authority)?;
        let record = self.authenticate_renewal(request, now)?;
        let current = record
            .receipt
            .as_ref()
            .ok_or(EnrollmentError::NotCommitted)?;
        let rotation = request.is_rotation();
        let next_key = csr_key_hash(&request.csr)?;
        // A rotation already committed to this key answers with it; a
        // renewal that already extended past what the holder holds does too.
        if (rotation && current.public_key == next_key && current.request == request.request)
            || (!rotation && current.expires_at > request.holds_until)
        {
            return Ok(RenewPreparation::Existing(current.clone()));
        }
        let lifetime =
            i64::try_from(self.limits.credential_lifetime).map_err(|_| EnrollmentError::Invalid)?;
        let expires_at = now.checked_add(lifetime).ok_or(EnrollmentError::Invalid)?;
        // A renewal must extend the credential; one decided within the
        // second the current certificate was issued would not.
        if !rotation && expires_at <= current.expires_at {
            return Err(EnrollmentError::Conflict);
        }
        let retire_previous_at = now
            .checked_add(i64::try_from(grace_seconds).map_err(|_| EnrollmentError::Invalid)?)
            .ok_or(EnrollmentError::Invalid)?
            .min(current.expires_at)
            .max(now);
        // The principal was derived by the key this enrollment began with;
        // a rotated key, and every renewal after a rotation, carries it in
        // a CA-signed subject so the binding stays verifiable.
        let derived = current.identity
            == assigned(
                self.cluster,
                current.identity.role,
                current.identity.node_id.unwrap_or(1),
                if rotation {
                    next_key
                } else {
                    current.public_key
                },
            );
        let founding = crate::pki::founding_principal(current)?;
        // The founder's principal was assigned at genesis: a renewal of the
        // founding key is issued under the founding subject, as the genesis
        // certificate was, so the binding stays verifiable.
        let certificate = if founding && !rotation {
            authority.issue_founder(
                &request.csr,
                &current.identity,
                now,
                self.limits.credential_lifetime,
            )?
        } else if derived {
            authority.issue(
                &request.csr,
                &current.identity,
                now,
                self.limits.credential_lifetime,
            )?
        } else {
            authority.issue_carried(
                &request.csr,
                &current.identity,
                now,
                self.limits.credential_lifetime,
            )?
        };
        let receipt = EnrollmentReceipt {
            invitation: current.invitation,
            request: if rotation {
                request.request
            } else {
                current.request
            },
            identity: current.identity.clone(),
            public_key: if rotation {
                next_key
            } else {
                current.public_key
            },
            csr_hash: if rotation {
                hash("focal.enrollment.csr.v1", &request.csr)
            } else {
                current.csr_hash
            },
            issued_at: now,
            expires_at,
            revision: self
                .revision
                .checked_add(1)
                .ok_or(EnrollmentError::Capacity)?,
            certificate,
        };
        let retired = RetiredCredential {
            invitation: current.invitation,
            receipt: current.clone(),
            retire_at: retire_previous_at,
        };
        self.reserve(retired_charge(&retired)?)?;
        if rotation {
            return Ok(RenewPreparation::Commit(EnrollmentCommand {
                revision: self.revision,
                decided_at: now,
                change: Change::Rotate {
                    invitation: current.invitation,
                    receipt,
                    retire_previous_at,
                },
            }));
        }
        Ok(RenewPreparation::Commit(EnrollmentCommand {
            revision: self.revision,
            decided_at: now,
            change: Change::Renew {
                invitation: current.invitation,
                receipt,
                retire_previous_at,
            },
        }))
    }
    /// The committed renewal a holder's request produced, once applied.
    pub fn release_renewal(
        &self,
        request: &RenewRequest,
        now: i64,
    ) -> Result<EnrollmentReceipt, EnrollmentError> {
        let record = self.authenticate_renewal(request, now)?;
        let current = record
            .receipt
            .as_ref()
            .ok_or(EnrollmentError::NotCommitted)?;
        let committed = if request.is_rotation() {
            current.public_key == csr_key_hash(&request.csr)? && current.request == request.request
        } else {
            current.expires_at > request.holds_until
        };
        if committed {
            Ok(current.clone())
        } else {
            Err(EnrollmentError::NotCommitted)
        }
    }
    fn authenticate_renewal(
        &self,
        request: &RenewRequest,
        now: i64,
    ) -> Result<&InviteMetadata, EnrollmentError> {
        self.check_time(now)?;
        if request.cluster != self.cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        if !(request.schema == 1 || request.schema == ROTATION_SCHEMA) || request.request == [0; 16]
        {
            return Err(EnrollmentError::Unauthorized);
        }
        let record = self
            .records
            .get(&request.invitation)
            .ok_or(EnrollmentError::Unauthorized)?;
        if record.revoked {
            return Err(EnrollmentError::Revoked);
        }
        let receipt = record
            .receipt
            .as_ref()
            .ok_or(EnrollmentError::Unauthorized)?;
        if receipt.identity.role != EnrollmentRole::Node {
            return Err(EnrollmentError::Unauthorized);
        }
        let csr_key = csr_key_hash(&request.csr)?;
        if request.is_rotation() {
            // A rotation names a new key under a new request; a rotation the
            // registry already committed names the key it holds now.
            let committed = receipt.request == request.request && receipt.public_key == csr_key;
            if !committed && (receipt.request == request.request || receipt.public_key == csr_key) {
                return Err(EnrollmentError::Unauthorized);
            }
        } else if receipt.request != request.request
            || receipt.csr_hash != hash("focal.enrollment.csr.v1", &request.csr)
            || receipt.public_key != csr_key
        {
            return Err(EnrollmentError::Unauthorized);
        }
        // The proof is signed by a certificate of this very enrollment: the
        // current one, or the one a renewal or rotation just retired while it
        // still authorizes, never a certificate of another enrollment. After
        // a rotation committed, the proof carries the previous key, which the
        // retired certificate still names.
        let signer = self.verify_node_statement(&request.proof, &request.statement()?, now)?;
        let signing_key = certificate_key_hash(&request.proof.certificate)?;
        let held_key = signing_key == receipt.public_key
            || self.retired.values().any(|retired| {
                retired.invitation == request.invitation
                    && retired.receipt.public_key == signing_key
            });
        if signer != receipt.identity || !held_key {
            return Err(EnrollmentError::Unauthorized);
        }
        Ok(record)
    }
    pub fn prepare_revoke(
        &self,
        invitation: InvitationId,
        now: i64,
    ) -> Result<EnrollmentCommand, EnrollmentError> {
        self.check_time(now)?;
        if !self.records.contains_key(&invitation) {
            return Err(EnrollmentError::Unauthorized);
        }
        Ok(EnrollmentCommand {
            revision: self.revision,
            decided_at: now,
            change: Change::Revoke { invitation },
        })
    }
    /// The caller MUST invoke this only for an authority-committed log entry.
    /// A log commit racing another prepared command may return Conflict; retry
    /// preparation against the newly published registry before issuing a reply.
    /// No private key or invitation secret ever enters this deterministic path.
    pub fn apply_committed(
        &mut self,
        command: &EnrollmentCommand,
        committed_index: u64,
    ) -> Result<(), EnrollmentError> {
        if command.revision != self.revision {
            return Err(EnrollmentError::Conflict);
        }
        if committed_index <= self.applied_index {
            return Err(EnrollmentError::NotCommitted);
        }
        self.check_time(command.decided_at)?;
        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or(EnrollmentError::Capacity)?;
        match &command.change {
            Change::Invite(record) => {
                if self.records.len() >= self.limits.max_invitations {
                    return Err(EnrollmentError::Capacity);
                }
                self.check_invitation_expiry(record.expires_at, command.decided_at)?;
                if record.cluster != self.cluster
                    || record.id == [0; 16]
                    || record.revoked
                    || record.receipt.is_some()
                    || self.records.contains_key(&record.id)
                {
                    return Err(EnrollmentError::Invalid);
                }
                let charge = record_charge(record)?;
                self.reserve(charge)?;
                self.records.insert(record.id, record.clone());
                self.charged_bytes = self
                    .charged_bytes
                    .checked_add(charge)
                    .ok_or(EnrollmentError::Capacity)?;
            }
            Change::Consume {
                invitation,
                csr,
                receipt,
            } => {
                let record = self
                    .records
                    .get(invitation)
                    .ok_or(EnrollmentError::Unauthorized)?;
                if record.revoked {
                    return Err(EnrollmentError::Revoked);
                }
                if record.expires_at <= command.decided_at {
                    return Err(EnrollmentError::Expired);
                }
                if record.receipt.is_some() {
                    return Err(EnrollmentError::Used);
                }
                if self.certificates.len() >= self.limits.max_enrollments {
                    return Err(EnrollmentError::Capacity);
                }
                let public_key = csr_key_hash(csr)?;
                if self.enrolled_keys.contains_key(&public_key) {
                    return Err(EnrollmentError::Used);
                }
                let expected = assigned(self.cluster, record.role, self.next_node, public_key);
                if receipt.identity != expected
                    || receipt.request == [0; 16]
                    || receipt.invitation != *invitation
                    || receipt.revision != next_revision
                    || receipt.public_key != public_key
                    || receipt.csr_hash != hash("focal.enrollment.csr.v1", csr)
                    || receipt.issued_at != command.decided_at
                    || receipt.expires_at.checked_sub(receipt.issued_at)
                        != Some(self.limits.credential_lifetime as i64)
                {
                    return Err(EnrollmentError::Invalid);
                }
                verify_issued(receipt, self.trust_roots())?;
                let next_node = if expected.node_id.is_some() {
                    self.next_node
                        .checked_add(1)
                        .ok_or(EnrollmentError::Capacity)?
                } else {
                    self.next_node
                };
                let fingerprint = server_fingerprint(&receipt.certificate);
                if self.certificates.contains_key(&fingerprint) {
                    return Err(EnrollmentError::Conflict);
                }
                let mut updated = record.clone();
                updated.receipt = Some(receipt.clone());
                let extra = record_charge(&updated)?
                    .checked_sub(record_charge(record)?)
                    .ok_or(EnrollmentError::Corrupt)?;
                self.reserve(extra)?;
                self.records
                    .get_mut(invitation)
                    .ok_or(EnrollmentError::Corrupt)?
                    .receipt = Some(receipt.clone());
                self.certificates.insert(fingerprint, *invitation);
                self.enrolled_keys.insert(public_key, *invitation);
                self.next_node = next_node;
                self.charged_bytes = self
                    .charged_bytes
                    .checked_add(extra)
                    .ok_or(EnrollmentError::Capacity)?;
            }
            Change::Revoke { invitation } => {
                self.records
                    .get_mut(invitation)
                    .ok_or(EnrollmentError::Unauthorized)?
                    .revoked = true
            }
            Change::ActivateFence { level } => {
                if *level == 0 || *level <= self.fence.level {
                    return Err(EnrollmentError::Invalid);
                }
                self.fence = UpgradeFence {
                    level: *level,
                    activated_at: command.decided_at,
                    revision: next_revision,
                };
            }
            Change::BootstrapServer { current, successor } => {
                let next = BootstrapServer {
                    current: *current,
                    successor: successor.clone(),
                };
                next.validate(self.limits.max_invitations)?;
                if next
                    .successor
                    .as_ref()
                    .is_some_and(|staged| staged.staged_at > command.decided_at)
                {
                    return Err(EnrollmentError::Invalid);
                }
                let previous = &self.bootstrap;
                // Recorded once, staged over the same current, or the staged
                // successor made current: no other move.
                let admitted = if previous.current.is_unknown() {
                    true
                } else if previous.current == next.current {
                    previous.successor.is_none() && next.successor.is_some()
                } else {
                    previous
                        .successor
                        .as_ref()
                        .is_some_and(|staged| staged.record == next.current)
                        && next.successor.is_none()
                };
                if !admitted {
                    return Err(EnrollmentError::Invalid);
                }
                let before = previous.charge()?;
                let after = next.charge()?;
                if let Some(more) = after.checked_sub(before).filter(|more| *more > 0) {
                    self.reserve(more)?;
                }
                self.charged_bytes = self
                    .charged_bytes
                    .checked_sub(before)
                    .and_then(|bytes| bytes.checked_add(after))
                    .ok_or(EnrollmentError::Corrupt)?;
                self.bootstrap = next;
            }
            Change::Issuer(change) => {
                let previous = &self.issuer;
                let next =
                    match change {
                        IssuerChange::Stage(staged) => {
                            // One generation at a time: nothing staged, nothing
                            // retiring; the successor is endorsed by the issuer it
                            // succeeds and is not that issuer.
                            if previous.successor.is_some()
                                || previous.retiring.is_some()
                                || staged.staged_at != command.decided_at
                                || staged.record.expires_at <= command.decided_at
                                || staged.record.fingerprint == previous.current.fingerprint
                                || !staged.record.endorsement.as_deref().is_some_and(
                                    |endorsement| {
                                        focal_wire::issued_by(
                                            endorsement,
                                            &previous.current.certificate,
                                        )
                                        .unwrap_or(false)
                                    },
                                )
                            {
                                return Err(EnrollmentError::Invalid);
                            }
                            IssuerSuccession {
                                current: previous.current.clone(),
                                successor: Some(staged.clone()),
                                retiring: None,
                            }
                        }
                        IssuerChange::Activate => {
                            let staged = previous
                                .successor
                                .as_ref()
                                .ok_or(EnrollmentError::Invalid)?;
                            IssuerSuccession {
                                current: staged.record.clone(),
                                successor: None,
                                retiring: Some(previous.current.clone()),
                            }
                        }
                        IssuerChange::Retire => {
                            let retiring =
                                previous.retiring.as_ref().ok_or(EnrollmentError::Invalid)?;
                            // Nothing live was issued under it, at the decision.
                            if self.live_receipts(command.decided_at).any(|receipt| {
                                focal_wire::issued_by(&receipt.certificate, &retiring.certificate)
                                    .unwrap_or(true)
                            }) {
                                return Err(EnrollmentError::Invalid);
                            }
                            IssuerSuccession {
                                current: previous.current.clone(),
                                successor: previous.successor.clone(),
                                retiring: None,
                            }
                        }
                    };
                next.validate()?;
                let before = previous.charge()?;
                let after = next.charge()?;
                if let Some(more) = after.checked_sub(before).filter(|more| *more > 0) {
                    self.reserve(more)?;
                }
                self.charged_bytes = self
                    .charged_bytes
                    .checked_sub(before)
                    .and_then(|bytes| bytes.checked_add(after))
                    .ok_or(EnrollmentError::Corrupt)?;
                self.issuer = next;
            }
            Change::AdmitTenant { tenant } => {
                if *tenant == [0; 16] || self.tenants.contains(tenant) {
                    return Err(EnrollmentError::Invalid);
                }
                if self.tenants.len() >= self.limits.max_tenants {
                    return Err(EnrollmentError::Capacity);
                }
                self.reserve(64)?;
                self.tenants.insert(*tenant);
                self.charged_bytes = self
                    .charged_bytes
                    .checked_add(64)
                    .ok_or(EnrollmentError::Capacity)?;
            }
            Change::Renew {
                invitation,
                receipt,
                retire_previous_at,
            } => {
                let record = self
                    .records
                    .get(invitation)
                    .ok_or(EnrollmentError::Unauthorized)?;
                if record.revoked {
                    return Err(EnrollmentError::Revoked);
                }
                let current = record
                    .receipt
                    .as_ref()
                    .ok_or(EnrollmentError::Unauthorized)?;
                let fingerprint = server_fingerprint(&receipt.certificate);
                if receipt.invitation != *invitation
                    || receipt.request != current.request
                    || receipt.identity != current.identity
                    || receipt.public_key != current.public_key
                    || receipt.csr_hash != current.csr_hash
                    || receipt.revision != next_revision
                    || receipt.issued_at != command.decided_at
                    || receipt.expires_at.checked_sub(receipt.issued_at)
                        != Some(self.limits.credential_lifetime as i64)
                    || *retire_previous_at < command.decided_at
                    || *retire_previous_at > current.expires_at
                    || self.certificates.contains_key(&fingerprint)
                    || self.retired.contains_key(&fingerprint)
                {
                    return Err(EnrollmentError::Invalid);
                }
                verify_issued(receipt, self.trust_roots())?;
                if !crate::pki::identity_bound(receipt)? {
                    return Err(EnrollmentError::Invalid);
                }
                let previous = server_fingerprint(&current.certificate);
                let retired = RetiredCredential {
                    invitation: *invitation,
                    receipt: current.clone(),
                    retire_at: *retire_previous_at,
                };
                let mut updated = record.clone();
                updated.receipt = Some(receipt.clone());
                let charge = retired_charge(&retired)?
                    .checked_add(record_charge(&updated)?)
                    .and_then(|bytes| bytes.checked_sub(record_charge(record).ok()?))
                    .ok_or(EnrollmentError::Corrupt)?;
                self.reserve(charge)?;
                self.records
                    .get_mut(invitation)
                    .ok_or(EnrollmentError::Corrupt)?
                    .receipt = Some(receipt.clone());
                self.certificates.remove(&previous);
                self.certificates.insert(fingerprint, *invitation);
                self.retired.insert(previous, retired);
                self.charged_bytes = self
                    .charged_bytes
                    .checked_add(charge)
                    .ok_or(EnrollmentError::Capacity)?;
            }
            Change::Rotate {
                invitation,
                receipt,
                retire_previous_at,
            } => {
                let record = self
                    .records
                    .get(invitation)
                    .ok_or(EnrollmentError::Unauthorized)?;
                if record.revoked {
                    return Err(EnrollmentError::Revoked);
                }
                let current = record
                    .receipt
                    .as_ref()
                    .ok_or(EnrollmentError::Unauthorized)?;
                let fingerprint = server_fingerprint(&receipt.certificate);
                if receipt.invitation != *invitation
                    || receipt.request == current.request
                    || receipt.identity != current.identity
                    || receipt.public_key == current.public_key
                    || receipt.csr_hash == current.csr_hash
                    || receipt.public_key != certificate_key_hash(&receipt.certificate)?
                    || receipt.revision != next_revision
                    || receipt.issued_at != command.decided_at
                    || receipt.expires_at.checked_sub(receipt.issued_at)
                        != Some(self.limits.credential_lifetime as i64)
                    || *retire_previous_at < command.decided_at
                    || *retire_previous_at > current.expires_at
                    || self.certificates.contains_key(&fingerprint)
                    || self.retired.contains_key(&fingerprint)
                    || self.enrolled_keys.contains_key(&receipt.public_key)
                {
                    return Err(EnrollmentError::Invalid);
                }
                verify_issued(receipt, self.trust_roots())?;
                if !crate::pki::identity_bound(receipt)? {
                    return Err(EnrollmentError::Invalid);
                }
                let previous = server_fingerprint(&current.certificate);
                let previous_key = current.public_key;
                let retired = RetiredCredential {
                    invitation: *invitation,
                    receipt: current.clone(),
                    retire_at: *retire_previous_at,
                };
                let mut updated = record.clone();
                updated.receipt = Some(receipt.clone());
                let charge = retired_charge(&retired)?
                    .checked_add(record_charge(&updated)?)
                    .and_then(|bytes| bytes.checked_sub(record_charge(record).ok()?))
                    .ok_or(EnrollmentError::Corrupt)?;
                self.reserve(charge)?;
                self.records
                    .get_mut(invitation)
                    .ok_or(EnrollmentError::Corrupt)?
                    .receipt = Some(receipt.clone());
                self.certificates.remove(&previous);
                self.certificates.insert(fingerprint, *invitation);
                self.enrolled_keys.remove(&previous_key);
                self.enrolled_keys.insert(receipt.public_key, *invitation);
                self.retired.insert(previous, retired);
                self.charged_bytes = self
                    .charged_bytes
                    .checked_add(charge)
                    .ok_or(EnrollmentError::Capacity)?;
            }
        }
        // Retired certificates past their grace leave the table, so the
        // registry never grows with the renewals of long-lived nodes.
        let mut reclaimed = 0usize;
        let decided_at = command.decided_at;
        self.retired.retain(|_, retired| {
            if retired.retire_at <= decided_at {
                reclaimed = reclaimed.saturating_add(retired_charge(retired).unwrap_or(0));
                false
            } else {
                true
            }
        });
        self.charged_bytes = self.charged_bytes.saturating_sub(reclaimed);
        self.revision = next_revision;
        self.applied_index = committed_index;
        self.time_floor = command.decided_at;
        Ok(())
    }
    /// Only committed enrollment records can produce a usable response.
    pub fn release(
        &self,
        request: &JoinRequest,
        now: i64,
    ) -> Result<EnrollmentReceipt, EnrollmentError> {
        self.authenticate_request(request, now)?
            .receipt
            .clone()
            .ok_or(EnrollmentError::NotCommitted)
    }
    /// Whether a member that opens on this registry may present the
    /// credential it holds. The registry authorizes it; or the registry has
    /// yet to reach the revision it was issued at — a renewal or rotation the
    /// holder was handed once it committed, which the holder's own replica
    /// has not applied: a replica follows the registry its credential came
    /// from, and after a restart knows only what its log says committed.
    /// The credential is then the CA's, for the identity the registry lists
    /// under the same enrollment, unrevoked and in its validity; the member
    /// converges on the committed renewal as it applies it.
    pub fn authorize_held(
        &self,
        held: &EnrollmentReceipt,
        now: i64,
    ) -> Result<(), EnrollmentError> {
        match self.authorize_certificate(&held.certificate, now) {
            Ok(identity) if identity == held.identity => Ok(()),
            Ok(_) => Err(EnrollmentError::Unauthorized),
            Err(EnrollmentError::Unauthorized) if held.revision > self.revision => {
                let record = self
                    .records
                    .get(&held.invitation)
                    .ok_or(EnrollmentError::Unauthorized)?;
                if record.revoked {
                    return Err(EnrollmentError::Revoked);
                }
                let listed = record
                    .receipt
                    .as_ref()
                    .ok_or(EnrollmentError::Unauthorized)?;
                if listed.identity != held.identity {
                    return Err(EnrollmentError::Unauthorized);
                }
                if now < held.issued_at || now >= held.expires_at {
                    return Err(EnrollmentError::Expired);
                }
                verify_issued(held, self.trust_roots())?;
                if !crate::pki::identity_bound(held)? {
                    return Err(EnrollmentError::Unauthorized);
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
    /// Recheck this on each privileged connection/request; a certificate's valid
    /// signature alone does not override a committed enrollment revocation.
    pub fn authorize_certificate(
        &self,
        certificate: &[u8],
        now: i64,
    ) -> Result<AssignedIdentity, EnrollmentError> {
        self.check_time(now)?;
        if certificate.len() > 4096 {
            return Err(EnrollmentError::Capacity);
        }
        let fingerprint = server_fingerprint(certificate);
        if let Some(retired) = self.retired.get(&fingerprint) {
            let record = self
                .records
                .get(&retired.invitation)
                .ok_or(EnrollmentError::Corrupt)?;
            if record.revoked {
                return Err(EnrollmentError::Revoked);
            }
            if now < retired.receipt.issued_at || now >= retired.retire_at {
                return Err(EnrollmentError::Expired);
            }
            return Ok(retired.receipt.identity.clone());
        }
        let id = self
            .certificates
            .get(&fingerprint)
            .ok_or(EnrollmentError::Unauthorized)?;
        let record = self.records.get(id).ok_or(EnrollmentError::Corrupt)?;
        if record.revoked {
            return Err(EnrollmentError::Revoked);
        }
        let receipt = record
            .receipt
            .as_ref()
            .ok_or(EnrollmentError::NotCommitted)?;
        if now < receipt.issued_at || now >= receipt.expires_at {
            return Err(EnrollmentError::Expired);
        }
        Ok(receipt.identity.clone())
    }
    pub fn checkpoint(&self) -> Result<Vec<u8>, EnrollmentError> {
        let size = postcard::experimental::serialized_size(self)?;
        if size > self.limits.max_checkpoint_bytes {
            return Err(EnrollmentError::Capacity);
        }
        let mut bytes = vec![0; size];
        let length = postcard::to_slice(self, &mut bytes)
            .map_err(|_| EnrollmentError::Capacity)?
            .len();
        bytes.truncate(length);
        Ok(bytes)
    }
    /// The schema 5 encoding of this registry, for the upgrade test.
    #[cfg(test)]
    pub(crate) fn encode_as_schema_five_for_tests(&self) -> Result<Vec<u8>, EnrollmentError> {
        #[derive(Serialize)]
        struct Legacy<'a> {
            schema: u16,
            cluster: ClusterId,
            ca_certificate: &'a [u8],
            limits: LegacyLimits,
            revision: u64,
            applied_index: u64,
            time_floor: i64,
            next_node: u64,
            charged_bytes: usize,
            records: &'a BTreeMap<InvitationId, InviteMetadata>,
            certificates: &'a BTreeMap<Fingerprint, InvitationId>,
            enrolled_keys: &'a BTreeMap<Fingerprint, InvitationId>,
            retired: &'a BTreeMap<Fingerprint, RetiredCredential>,
            tenants: &'a std::collections::BTreeSet<[u8; 16]>,
            fence: UpgradeFence,
            bootstrap: &'a BootstrapServer,
        }
        encode(&Legacy {
            schema: 5,
            cluster: self.cluster,
            ca_certificate: &self.ca_certificate,
            limits: LegacyLimits::of(&self.limits),
            revision: self.revision,
            applied_index: self.applied_index,
            time_floor: self.time_floor,
            next_node: self.next_node,
            charged_bytes: self
                .charged_bytes
                .checked_sub(self.issuer.charge()?)
                .ok_or(EnrollmentError::Corrupt)?,
            records: &self.records,
            certificates: &self.certificates,
            enrolled_keys: &self.enrolled_keys,
            retired: &self.retired,
            tenants: &self.tenants,
            fence: self.fence,
            bootstrap: &self.bootstrap,
        })
    }
    /// The schema 4 encoding of this registry, for the upgrade test.
    #[cfg(test)]
    pub(crate) fn encode_as_schema_four_for_tests(&self) -> Result<Vec<u8>, EnrollmentError> {
        #[derive(Serialize)]
        struct Legacy<'a> {
            schema: u16,
            cluster: ClusterId,
            ca_certificate: &'a [u8],
            limits: LegacyLimits,
            revision: u64,
            applied_index: u64,
            time_floor: i64,
            next_node: u64,
            charged_bytes: usize,
            records: &'a BTreeMap<InvitationId, InviteMetadata>,
            certificates: &'a BTreeMap<Fingerprint, InvitationId>,
            enrolled_keys: &'a BTreeMap<Fingerprint, InvitationId>,
            retired: &'a BTreeMap<Fingerprint, RetiredCredential>,
            tenants: &'a std::collections::BTreeSet<[u8; 16]>,
            fence: UpgradeFence,
        }
        encode(&Legacy {
            schema: 4,
            cluster: self.cluster,
            ca_certificate: &self.ca_certificate,
            limits: LegacyLimits::of(&self.limits),
            revision: self.revision,
            applied_index: self.applied_index,
            time_floor: self.time_floor,
            next_node: self.next_node,
            charged_bytes: self
                .charged_bytes
                .checked_sub(self.bootstrap.charge()?)
                .and_then(|bytes| bytes.checked_sub(self.issuer.charge().ok()?))
                .ok_or(EnrollmentError::Corrupt)?,
            records: &self.records,
            certificates: &self.certificates,
            enrolled_keys: &self.enrolled_keys,
            retired: &self.retired,
            tenants: &self.tenants,
            fence: self.fence,
        })
    }
    /// The schema 3 encoding of this registry, for the upgrade test.
    #[cfg(test)]
    pub(crate) fn encode_as_schema_three_for_tests(&self) -> Result<Vec<u8>, EnrollmentError> {
        #[derive(Serialize)]
        struct Legacy<'a> {
            schema: u16,
            cluster: ClusterId,
            ca_certificate: &'a [u8],
            limits: LegacyLimits,
            revision: u64,
            applied_index: u64,
            time_floor: i64,
            next_node: u64,
            charged_bytes: usize,
            records: &'a BTreeMap<InvitationId, InviteMetadata>,
            certificates: &'a BTreeMap<Fingerprint, InvitationId>,
            enrolled_keys: &'a BTreeMap<Fingerprint, InvitationId>,
            retired: &'a BTreeMap<Fingerprint, RetiredCredential>,
            tenants: &'a std::collections::BTreeSet<[u8; 16]>,
        }
        encode(&Legacy {
            schema: 3,
            cluster: self.cluster,
            ca_certificate: &self.ca_certificate,
            limits: LegacyLimits::of(&self.limits),
            revision: self.revision,
            applied_index: self.applied_index,
            time_floor: self.time_floor,
            next_node: self.next_node,
            // A schema-3 checkpoint charged no issuer.
            charged_bytes: self
                .charged_bytes
                .checked_sub(self.issuer.charge()?)
                .ok_or(EnrollmentError::Corrupt)?,
            records: &self.records,
            certificates: &self.certificates,
            enrolled_keys: &self.enrolled_keys,
            retired: &self.retired,
            tenants: &self.tenants,
        })
    }
    /// The schema 2 encoding of this registry, for the upgrade test.
    #[cfg(test)]
    pub(crate) fn encode_as_schema_two_for_tests(&self) -> Result<Vec<u8>, EnrollmentError> {
        #[derive(Serialize)]
        struct Legacy<'a> {
            schema: u16,
            cluster: ClusterId,
            ca_certificate: &'a [u8],
            limits: (usize, usize, u64, u64, usize),
            revision: u64,
            applied_index: u64,
            time_floor: i64,
            next_node: u64,
            charged_bytes: usize,
            records: &'a BTreeMap<InvitationId, InviteMetadata>,
            certificates: &'a BTreeMap<Fingerprint, InvitationId>,
            enrolled_keys: &'a BTreeMap<Fingerprint, InvitationId>,
            retired: &'a BTreeMap<Fingerprint, RetiredCredential>,
        }
        encode(&Legacy {
            schema: 2,
            cluster: self.cluster,
            ca_certificate: &self.ca_certificate,
            limits: (
                self.limits.max_invitations,
                self.limits.max_enrollments,
                self.limits.max_invitation_lifetime,
                self.limits.credential_lifetime,
                self.limits.max_checkpoint_bytes,
            ),
            revision: self.revision,
            applied_index: self.applied_index,
            time_floor: self.time_floor,
            next_node: self.next_node,
            // A schema-2 checkpoint never charged tenants, nor an issuer.
            charged_bytes: self
                .charged_bytes
                .saturating_sub(self.tenants.len().saturating_mul(64))
                .checked_sub(self.issuer.charge()?)
                .ok_or(EnrollmentError::Corrupt)?,
            records: &self.records,
            certificates: &self.certificates,
            enrolled_keys: &self.enrolled_keys,
            retired: &self.retired,
        })
    }
    pub fn restore(
        bytes: &[u8],
        expected_cluster: ClusterId,
        limits: EnrollmentLimits,
    ) -> Result<Self, EnrollmentError> {
        limits.validate()?;
        if bytes.len() > limits.max_checkpoint_bytes {
            return Err(EnrollmentError::Capacity);
        }
        let (schema, _) = postcard::take_from_bytes::<u16>(bytes)?;
        let (mut registry, rest): (Self, &[u8]) = if schema == 2 {
            let (legacy, rest): (RegistryV2, &[u8]) = postcard::take_from_bytes(bytes)?;
            if legacy.schema != 2 {
                return Err(EnrollmentError::Corrupt);
            }
            let LimitsV2 {
                max_invitations,
                max_enrollments,
                max_invitation_lifetime,
                credential_lifetime,
                max_checkpoint_bytes,
            } = legacy.limits;
            (
                Self {
                    owner: None,
                    schema: REGISTRY_SCHEMA,
                    cluster: legacy.cluster,
                    ca_certificate: legacy.ca_certificate.clone(),
                    limits: EnrollmentLimits {
                        max_invitations,
                        max_enrollments,
                        max_invitation_lifetime,
                        credential_lifetime,
                        max_checkpoint_bytes,
                        max_tenants: limits.max_tenants,
                        issuer_lifetime: EnrollmentLimits::issuer_lifetime_for(credential_lifetime),
                    },
                    revision: legacy.revision,
                    applied_index: legacy.applied_index,
                    time_floor: legacy.time_floor,
                    next_node: legacy.next_node,
                    charged_bytes: legacy.charged_bytes,
                    records: legacy.records,
                    certificates: legacy.certificates,
                    enrolled_keys: legacy.enrolled_keys,
                    retired: legacy.retired,
                    tenants: std::collections::BTreeSet::new(),
                    fence: UpgradeFence::default(),
                    bootstrap: BootstrapServer::default(),
                    issuer: IssuerSuccession::genesis(&legacy.ca_certificate)?,
                },
                rest,
            )
        } else if schema == 3 {
            // A schema-3 checkpoint never activated a fence.
            let (legacy, rest): (RegistryV3, &[u8]) = postcard::take_from_bytes(bytes)?;
            if legacy.schema != 3 {
                return Err(EnrollmentError::Corrupt);
            }
            (
                Self {
                    owner: None,
                    schema: REGISTRY_SCHEMA,
                    cluster: legacy.cluster,
                    ca_certificate: legacy.ca_certificate.clone(),
                    limits: legacy.limits.into(),
                    revision: legacy.revision,
                    applied_index: legacy.applied_index,
                    time_floor: legacy.time_floor,
                    next_node: legacy.next_node,
                    charged_bytes: legacy.charged_bytes,
                    records: legacy.records,
                    certificates: legacy.certificates,
                    enrolled_keys: legacy.enrolled_keys,
                    retired: legacy.retired,
                    tenants: legacy.tenants,
                    fence: UpgradeFence::default(),
                    bootstrap: BootstrapServer::default(),
                    issuer: IssuerSuccession::genesis(&legacy.ca_certificate)?,
                },
                rest,
            )
        } else if schema == 4 {
            // A schema-4 checkpoint names no bootstrap server certificate;
            // the founder records the one it holds (24 §11).
            let (legacy, rest): (RegistryV4, &[u8]) = postcard::take_from_bytes(bytes)?;
            if legacy.schema != 4 {
                return Err(EnrollmentError::Corrupt);
            }
            (
                Self {
                    owner: None,
                    schema: REGISTRY_SCHEMA,
                    cluster: legacy.cluster,
                    ca_certificate: legacy.ca_certificate.clone(),
                    limits: legacy.limits.into(),
                    revision: legacy.revision,
                    applied_index: legacy.applied_index,
                    time_floor: legacy.time_floor,
                    next_node: legacy.next_node,
                    charged_bytes: legacy.charged_bytes,
                    records: legacy.records,
                    certificates: legacy.certificates,
                    enrolled_keys: legacy.enrolled_keys,
                    retired: legacy.retired,
                    tenants: legacy.tenants,
                    fence: legacy.fence,
                    bootstrap: BootstrapServer::default(),
                    issuer: IssuerSuccession::genesis(&legacy.ca_certificate)?,
                },
                rest,
            )
        } else if schema == 5 {
            // A schema-5 checkpoint names its genesis issuer alone (24 §11).
            let (legacy, rest): (RegistryV5, &[u8]) = postcard::take_from_bytes(bytes)?;
            if legacy.schema != 5 {
                return Err(EnrollmentError::Corrupt);
            }
            (
                Self {
                    owner: None,
                    schema: REGISTRY_SCHEMA,
                    cluster: legacy.cluster,
                    issuer: IssuerSuccession::genesis(&legacy.ca_certificate)?,
                    ca_certificate: legacy.ca_certificate.clone(),
                    limits: legacy.limits.into(),
                    revision: legacy.revision,
                    applied_index: legacy.applied_index,
                    time_floor: legacy.time_floor,
                    next_node: legacy.next_node,
                    charged_bytes: legacy.charged_bytes,
                    records: legacy.records,
                    certificates: legacy.certificates,
                    enrolled_keys: legacy.enrolled_keys,
                    retired: legacy.retired,
                    tenants: legacy.tenants,
                    fence: legacy.fence,
                    bootstrap: legacy.bootstrap,
                },
                rest,
            )
        } else {
            postcard::take_from_bytes(bytes)?
        };
        // A checkpoint written before the succession was recorded charged
        // nothing for its issuer; the genesis record is charged now.
        if schema < REGISTRY_SCHEMA {
            registry.charged_bytes = registry
                .charged_bytes
                .checked_add(registry.issuer.charge()?)
                .ok_or(EnrollmentError::Capacity)?;
        }
        registry
            .issuer
            .validate()
            .map_err(|_| EnrollmentError::Corrupt)?;
        registry
            .bootstrap
            .validate(limits.max_invitations)
            .map_err(|_| EnrollmentError::Corrupt)?;
        if registry.tenants.len() > limits.max_tenants || registry.tenants.contains(&[0; 16]) {
            return Err(EnrollmentError::Corrupt);
        }
        if (registry.fence.level == 0)
            != (registry.fence.activated_at == 0 && registry.fence.revision == 0)
            || registry.fence.activated_at < 0
            || registry.fence.revision > registry.revision
        {
            return Err(EnrollmentError::Corrupt);
        }
        // The lifetimes are the committed policy of the cluster (the founder
        // chose them at genesis); the restoring process bounds the capacities.
        registry
            .limits
            .validate()
            .map_err(|_| EnrollmentError::Corrupt)?;
        if !rest.is_empty()
            || registry.schema != REGISTRY_SCHEMA
            || registry.limits.capacities() != limits.capacities()
            || registry.records.len() > limits.max_invitations
            || registry.certificates.len() > limits.max_enrollments
        {
            return Err(EnrollmentError::Corrupt);
        }
        if registry.cluster != expected_cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        if registry.next_node == 0 || registry.ca_certificate.len() > 4096 {
            return Err(EnrollmentError::Corrupt);
        }
        let mut certificates = BTreeMap::new();
        let mut enrolled_keys = BTreeMap::new();
        let mut node_ids = std::collections::BTreeSet::new();
        let mut charged_bytes = 8192usize;
        for (id, record) in &registry.records {
            charged_bytes = charged_bytes
                .checked_add(record_charge(record)?)
                .ok_or(EnrollmentError::Capacity)?;
            if *id != record.id || record.cluster != registry.cluster {
                return Err(EnrollmentError::Corrupt);
            }
            if let Some(receipt) = &record.receipt {
                if receipt.invitation != *id
                    || receipt.identity.cluster != registry.cluster
                    || receipt.identity.role != record.role
                    || receipt.revision > registry.revision
                    || receipt
                        .identity
                        .node_id
                        .is_some_and(|n| n == 0 || n >= registry.next_node)
                {
                    return Err(EnrollmentError::Corrupt);
                }
                // A live receipt chains to a trusted issuer; one that expired
                // may have been issued under an issuer retired since, and is
                // held to its shape alone — it authorizes nothing.
                if receipt.expires_at > registry.time_floor {
                    verify_issued(receipt, registry.issuer.roots())?;
                } else {
                    verify_issued_shape(receipt)?;
                }
                if certificates
                    .insert(server_fingerprint(&receipt.certificate), *id)
                    .is_some()
                {
                    return Err(EnrollmentError::Corrupt);
                }
                if enrolled_keys.insert(receipt.public_key, *id).is_some() {
                    return Err(EnrollmentError::Corrupt);
                }
                if let Some(node) = receipt.identity.node_id
                    && !node_ids.insert(node)
                {
                    return Err(EnrollmentError::Corrupt);
                }
                let mut expected = assigned(
                    registry.cluster,
                    record.role,
                    receipt.identity.node_id.unwrap_or(1),
                    receipt.public_key,
                );
                if crate::pki::founding_principal(receipt)?
                    || crate::pki::carried_principal(receipt)?
                {
                    expected.principal = receipt.identity.principal;
                }
                if receipt.identity != expected {
                    return Err(EnrollmentError::Corrupt);
                }
            }
        }
        if certificates != registry.certificates || enrolled_keys != registry.enrolled_keys {
            return Err(EnrollmentError::Corrupt);
        }
        if registry.retired.len() > limits.max_enrollments {
            return Err(EnrollmentError::Corrupt);
        }
        for (fingerprint, retired) in &registry.retired {
            charged_bytes = charged_bytes
                .checked_add(retired_charge(retired)?)
                .ok_or(EnrollmentError::Capacity)?;
            let current = registry
                .records
                .get(&retired.invitation)
                .and_then(|record| record.receipt.as_ref())
                .ok_or(EnrollmentError::Corrupt)?;
            // A renewal retires a certificate of the current key and
            // request; a rotation retires the previous key and request
            // together, and that key is enrolled no more.
            let renewed = retired.receipt.public_key == current.public_key
                && retired.receipt.request == current.request;
            let rotated = retired.receipt.public_key != current.public_key
                && retired.receipt.request != current.request
                && !enrolled_keys.contains_key(&retired.receipt.public_key);
            if *fingerprint != server_fingerprint(&retired.receipt.certificate)
                || certificates.contains_key(fingerprint)
                || retired.receipt.invitation != retired.invitation
                || retired.receipt.identity != current.identity
                || !(renewed || rotated)
                || retired.retire_at > retired.receipt.expires_at
                || retired.receipt.revision >= current.revision
            {
                return Err(EnrollmentError::Corrupt);
            }
            if retired.receipt.expires_at > registry.time_floor {
                verify_issued(&retired.receipt, registry.issuer.roots())?;
            } else {
                verify_issued_shape(&retired.receipt)?;
            }
        }
        charged_bytes = charged_bytes
            .checked_add(
                registry
                    .tenants
                    .len()
                    .checked_mul(64)
                    .ok_or(EnrollmentError::Capacity)?,
            )
            .and_then(|bytes| bytes.checked_add(registry.bootstrap.charge().ok()?))
            .and_then(|bytes| bytes.checked_add(registry.issuer.charge().ok()?))
            .ok_or(EnrollmentError::Capacity)?;
        if charged_bytes != registry.charged_bytes || charged_bytes > limits.max_checkpoint_bytes {
            return Err(EnrollmentError::Corrupt);
        }
        registry.owner = Some(random()?);
        Ok(registry)
    }
    fn reserve(&self, additional: usize) -> Result<(), EnrollmentError> {
        if self
            .charged_bytes
            .checked_add(additional)
            .is_none_or(|bytes| bytes > self.limits.max_checkpoint_bytes)
        {
            return Err(EnrollmentError::Capacity);
        }
        Ok(())
    }
    fn check_time(&self, now: i64) -> Result<(), EnrollmentError> {
        if now < self.time_floor {
            return Err(EnrollmentError::Invalid);
        }
        Ok(())
    }
    fn check_authority(&self, authority: &BootstrapAuthority) -> Result<(), EnrollmentError> {
        if authority.cluster() != self.cluster || authority.ca_certificate() != self.ca_certificate
        {
            return Err(EnrollmentError::WrongCluster);
        }
        Ok(())
    }
    fn check_invitation_expiry(&self, expires: i64, now: i64) -> Result<(), EnrollmentError> {
        let lifetime = expires.checked_sub(now).ok_or(EnrollmentError::Invalid)?;
        if lifetime <= 0 {
            return Err(EnrollmentError::Expired);
        }
        if lifetime as u64 > self.limits.max_invitation_lifetime {
            return Err(EnrollmentError::Capacity);
        }
        Ok(())
    }
    fn authenticate_request(
        &self,
        request: &JoinRequest,
        now: i64,
    ) -> Result<&InviteMetadata, EnrollmentError> {
        self.check_time(now)?;
        if request.cluster != self.cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        let record = self
            .records
            .get(&request.invitation)
            .ok_or(EnrollmentError::Unauthorized)?;
        // Hash comparison uses BLAKE3's constant-time Hash equality implementation.
        if request.schema != 1
            || request.request == [0; 16]
            || request.secret.0.len() != 32
            || request.role != record.role
            || request.trust != record.trust
            || blake3::Hash::from(token_hash(&request.secret.0))
                != blake3::Hash::from(record.token_hash)
        {
            return Err(EnrollmentError::Unauthorized);
        }
        if record.revoked {
            return Err(EnrollmentError::Revoked);
        }
        let public_key = csr_key_hash(&request.csr)?;
        if let Some(receipt) = &record.receipt {
            if receipt.request != request.request
                || receipt.public_key != public_key
                || receipt.csr_hash != hash("focal.enrollment.csr.v1", &request.csr)
            {
                return Err(EnrollmentError::Used);
            }
            if now >= receipt.expires_at {
                return Err(EnrollmentError::Expired);
            }
        } else if record.expires_at <= now {
            return Err(EnrollmentError::Expired);
        }
        Ok(record)
    }
}
fn record_charge(record: &InviteMetadata) -> Result<usize, EnrollmentError> {
    // Canonical payload plus map keys/node overhead. The fixed 8 KiB registry
    // reservation covers its CA certificate, counters and collection headers.
    encode(record)?
        .len()
        .checked_add(128)
        .and_then(|value| value.checked_add(if record.receipt.is_some() { 256 } else { 0 }))
        .ok_or(EnrollmentError::Capacity)
}
fn retired_charge(retired: &RetiredCredential) -> Result<usize, EnrollmentError> {
    encode(retired)?
        .len()
        .checked_add(128)
        .ok_or(EnrollmentError::Capacity)
}
fn token_hash(secret: &[u8]) -> Fingerprint {
    hash("focal.enrollment.invitation-secret.v1", secret)
}
pub(crate) fn assigned(
    cluster: ClusterId,
    role: EnrollmentRole,
    next_node: u64,
    public_key: Fingerprint,
) -> AssignedIdentity {
    let mut input = Vec::with_capacity(49);
    input.extend_from_slice(&cluster);
    input.extend_from_slice(&public_key);
    input.push(match role {
        EnrollmentRole::Node => 1,
        EnrollmentRole::Client => 2,
    });
    let digest = hash("focal.enrollment.assigned-principal.v1", &input);
    let mut principal = [0; 16];
    for (output, input) in principal.iter_mut().zip(digest.iter()) {
        *output = *input;
    }
    let node_id = (role == EnrollmentRole::Node).then_some(next_node);
    let name = match node_id {
        Some(id) => format!("node-{id}"),
        None => format!("client-{}", hex(&principal)),
    };
    AssignedIdentity {
        cluster,
        role,
        node_id,
        principal,
        server_name: format!("{name}.{}.focal.internal", hex(&cluster)),
    }
}
