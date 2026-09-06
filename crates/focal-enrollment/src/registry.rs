use crate::{
    invitation::InvitationData,
    pki::{SecretBytes, csr_key_hash, verify_issued},
    *,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollmentLimits {
    pub max_invitations: usize,
    pub max_enrollments: usize,
    pub max_invitation_lifetime: u64,
    pub credential_lifetime: u64,
    pub max_checkpoint_bytes: usize,
}
impl Default for EnrollmentLimits {
    fn default() -> Self {
        Self {
            max_invitations: 4096,
            max_enrollments: 4096,
            max_invitation_lifetime: 86400,
            credential_lifetime: 30 * 86400,
            max_checkpoint_bytes: 8 * 1024 * 1024,
        }
    }
}
impl EnrollmentLimits {
    fn validate(&self) -> Result<(), EnrollmentError> {
        if self.max_invitations == 0
            || self.max_invitations > 65536
            || self.max_enrollments == 0
            || self.max_enrollments > self.max_invitations
            || self.max_invitation_lifetime == 0
            || self.max_invitation_lifetime > 7 * 86400
            || self.credential_lifetime == 0
            || self.credential_lifetime > 365 * 86400
            || !(16 * 1024..=64 * 1024 * 1024).contains(&self.max_checkpoint_bytes)
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
struct InviteMetadata {
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
enum Change {
    Invite(InviteMetadata),
    Consume {
        invitation: InvitationId,
        csr: Vec<u8>,
        receipt: EnrollmentReceipt,
    },
    Revoke {
        invitation: InvitationId,
    },
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
        let saved: SavedInvitation = decode(&bytes)?;
        let Change::Invite(record) = &saved.command.change else {
            return Err(EnrollmentError::Corrupt);
        };
        let data = &saved.invitation;
        if saved.schema != 1
            || data.schema != 1
            || data.cluster != cluster
            || record.cluster != cluster
            || record.id != data.id
            || data.id == [0; 16]
            || record.role != data.role
            || record.expires_at != data.expires_at
            || data.secret.0.len() != 32
            || record.token_hash != token_hash(&data.secret.0)
            || record.trust != data.trust.fingerprint()?
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
        Ok(Self {
            owner: Some(random()?),
            schema: 1,
            cluster,
            ca_certificate,
            limits,
            revision: 0,
            applied_index: 0,
            time_floor: 0,
            next_node,
            charged_bytes: 8192,
            records: BTreeMap::new(),
            certificates: BTreeMap::new(),
            enrolled_keys: BTreeMap::new(),
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
        now: i64,
    ) -> Result<(Self, EnrollmentReceipt), EnrollmentError> {
        let next_node = node.checked_add(1).ok_or(EnrollmentError::Capacity)?;
        let mut registry = Self::new(
            authority.cluster(),
            authority.ca_certificate().to_vec(),
            next_node,
            limits,
        )?;
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
        verify_issued(&receipt, authority.ca_certificate())?;
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
    pub fn invitation_revoked(&self, id: InvitationId) -> Result<bool, EnrollmentError> {
        self.records
            .get(&id)
            .map(|record| record.revoked)
            .ok_or(EnrollmentError::Unauthorized)
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
        let trust = ServerTrust {
            endpoint: options.endpoint,
            server_name: options.server_name,
            ca_certificate: self.ca_certificate.clone(),
            server_fingerprint: server_fingerprint(authority.server_certificate()),
        };
        trust.validate()?;
        trust.verify_chain(&[authority.server_certificate().to_vec().into()], now)?;
        let data = InvitationData {
            schema: 1,
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
            trust: data.trust.fingerprint()?,
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
                verify_issued(receipt, &self.ca_certificate)?;
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
        }
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
        let id = self
            .certificates
            .get(&server_fingerprint(certificate))
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
    pub fn restore(
        bytes: &[u8],
        expected_cluster: ClusterId,
        limits: EnrollmentLimits,
    ) -> Result<Self, EnrollmentError> {
        limits.validate()?;
        if bytes.len() > limits.max_checkpoint_bytes {
            return Err(EnrollmentError::Capacity);
        }
        let (mut registry, rest): (Self, &[u8]) = postcard::take_from_bytes(bytes)?;
        if !rest.is_empty()
            || registry.schema != 1
            || registry.limits != limits
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
                verify_issued(receipt, &registry.ca_certificate)?;
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
                if crate::pki::founding_principal(receipt)? {
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
fn token_hash(secret: &[u8]) -> Fingerprint {
    hash("focal.enrollment.invitation-secret.v1", secret)
}
fn assigned(
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
