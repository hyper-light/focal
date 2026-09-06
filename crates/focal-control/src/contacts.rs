//! Root-owned reachability records. An enrolled node may announce only its own
//! address. These rows convey no voter, topology, storage, or Runtime authority.
use crate::{ControlError, ControlIdentity, ControlRequest};
use focal_enrollment::{EnrollmentRegistry, EnrollmentRole};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone, Copy)]
pub struct ContactLimits {
    pub max_nodes: usize,
    pub max_checkpoint_bytes: usize,
}
impl Default for ContactLimits {
    fn default() -> Self {
        Self {
            max_nodes: 1024,
            max_checkpoint_bytes: 1024 * 1024,
        }
    }
}
impl ContactLimits {
    fn validate(self) -> Result<(), ControlError> {
        if !(1..=65536).contains(&self.max_nodes)
            || !(4096..=8 * 1024 * 1024).contains(&self.max_checkpoint_bytes)
        {
            return Err(ControlError::Invalid);
        }
        Ok(())
    }
}

/// The host derives node/principal/fingerprint from authenticated TLS and time
/// from its clock. Only address and expected generation come from the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeContactCommand {
    pub node: u64,
    pub principal: [u8; 16],
    pub certificate_fingerprint: [u8; 32],
    pub advertise: SocketAddr,
    pub expected_generation: u64,
    pub decided_at: i64,
}
impl NodeContactCommand {
    /// The owner's decision time is committed in the full envelope, but does
    /// not change client intent on retry. Existing command hashes stay intact.
    pub(crate) fn intent_hash(&self, request: &ControlRequest) -> Result<[u8; 32], ControlError> {
        crate::hash(
            "focal.control.node-contact-request.v1",
            &(
                request.id,
                request.acknowledged_through,
                self.node,
                self.principal,
                self.certificate_fingerprint,
                self.advertise,
                self.expected_generation,
            ),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactRecord {
    pub node: u64,
    pub principal: [u8; 16],
    /// Same domain as focal_wire::certificate_fingerprint; distinct from the
    /// enrollment transport's server-certificate pin domain.
    pub certificate_fingerprint: [u8; 32],
    pub advertise: SocketAddr,
    /// Taken from the committed enrollment, never from the announcement.
    pub server_name: String,
    pub generation: u64,
    pub decided_at: i64,
    pub committed_index: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactCheckpoint {
    pub schema: u16,
    pub cluster: [u8; 16],
    pub revision: u64,
    pub applied_index: u64,
    /// Strictly sorted by physical node. Each node has at most one current row.
    pub records: Vec<ContactRecord>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactSnapshot {
    pub identity: ControlIdentity,
    /// Root read barrier prefix; may be later than the last contact mutation.
    pub applied_index: u64,
    pub contacts: ContactCheckpoint,
}

pub fn validate_contact_address(address: SocketAddr) -> Result<(), ControlError> {
    if address.port() == 0
        || address.ip().is_unspecified()
        || address.ip().is_multicast()
        || matches!(address, SocketAddr::V4(address) if address.ip().is_broadcast())
        || matches!(address, SocketAddr::V6(address) if address.scope_id()!=0 || address.flowinfo()!=0)
    {
        return Err(ControlError::Invalid);
    }
    Ok(())
}
fn fingerprint(certificate: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("focal.transport.peer-certificate.v1");
    hasher.update(certificate);
    *hasher.finalize().as_bytes()
}
/// Recheck committed enrollment on every new proposal and before using a
/// historical contact as a route. Retaining a contact does not override revoke.
pub fn authorize_node_contact(
    enrollment: &EnrollmentRegistry,
    node: u64,
    principal: [u8; 16],
    certificate_fingerprint: [u8; 32],
    now: i64,
) -> Result<focal_enrollment::AssignedIdentity, ControlError> {
    let receipt = enrollment
        .enrollments()
        .find(|receipt| {
            receipt.identity.node_id == Some(node)
                && receipt.identity.principal == principal
                && fingerprint(&receipt.certificate) == certificate_fingerprint
        })
        .ok_or(focal_enrollment::EnrollmentError::Unauthorized)?;
    let identity = enrollment.authorize_certificate(&receipt.certificate, now)?;
    if identity.role != EnrollmentRole::Node
        || identity.node_id != Some(node)
        || identity.principal != principal
    {
        return Err(focal_enrollment::EnrollmentError::Unauthorized.into());
    }
    Ok(identity)
}

static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);
/// One bounded root table. Preparation copies its sorted rows under an owned
/// allowance; publication only swaps the complete candidate and its permit.
/// This metadata path makes no claim of incremental or parallel preparation.
pub(crate) struct NodeContacts {
    state: ContactCheckpoint,
    limits: ContactLimits,
    budget: MemoryBudget,
    owner: u64,
    _allocation: Allocation,
}
pub(crate) struct PreparedNodeContact {
    state: ContactCheckpoint,
    owner: u64,
    base_revision: u64,
    base_index: u64,
    node: u64,
    allocation: Allocation,
}
impl NodeContacts {
    pub(crate) fn new(
        cluster: [u8; 16],
        limits: ContactLimits,
        budget: MemoryBudget,
    ) -> Result<Self, ControlError> {
        Self::restore(
            ContactCheckpoint {
                schema: 1,
                cluster,
                revision: 0,
                applied_index: 0,
                records: Vec::new(),
            },
            limits,
            budget,
            0,
        )
    }
    pub(crate) fn restore(
        state: ContactCheckpoint,
        limits: ContactLimits,
        budget: MemoryBudget,
        max_index: u64,
    ) -> Result<Self, ControlError> {
        limits.validate()?;
        validate_checkpoint(&state, limits, max_index)?;
        let allocation = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                state_charge(&state, 0)?,
            )?
            .commit();
        let owner = NEXT_OWNER
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .map_err(|_| ControlError::Capacity)?;
        Ok(Self {
            state,
            limits,
            budget,
            owner,
            _allocation: allocation,
        })
    }
    pub(crate) fn checkpoint(&self) -> &ContactCheckpoint {
        &self.state
    }
    pub(crate) fn charged_bytes(&self) -> usize {
        self._allocation.bytes()
    }
    pub(crate) fn prepare(
        &self,
        command: &NodeContactCommand,
        enrollment: &EnrollmentRegistry,
    ) -> Result<PreparedNodeContact, ControlError> {
        validate_contact_address(command.advertise)?;
        let existing = self
            .state
            .records
            .binary_search_by_key(&command.node, |record| record.node);
        let generation = existing
            .ok()
            .and_then(|index| self.state.records.get(index))
            .map_or(0, |record| record.generation);
        if command.expected_generation != generation {
            return Err(focal_directory::DirectoryError::CompareFailed.into());
        }
        if existing.is_err() && self.state.records.len() >= self.limits.max_nodes {
            return Err(ControlError::Capacity);
        }
        if command.decided_at <= 0
            || self
                .state
                .records
                .iter()
                .any(|record| record.decided_at > command.decided_at)
        {
            return Err(ControlError::Invalid);
        }
        // Reserve before certificate authorization clones its bounded identity,
        // and before constructing the owned candidate table.
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                state_charge(&self.state, 1)?,
            )?
            .commit();
        let identity = authorize_node_contact(
            enrollment,
            command.node,
            command.principal,
            command.certificate_fingerprint,
            command.decided_at,
        )?;
        if identity.cluster != self.state.cluster
            || identity.server_name.is_empty()
            || identity.server_name.len() > 253
        {
            return Err(ControlError::WrongOwner);
        }
        let next = ContactRecord {
            node: command.node,
            principal: command.principal,
            certificate_fingerprint: command.certificate_fingerprint,
            advertise: command.advertise,
            server_name: identity.server_name,
            generation: generation.checked_add(1).ok_or(ControlError::Capacity)?,
            decided_at: command.decided_at,
            committed_index: 0,
        };
        let mut records = Vec::new();
        records
            .try_reserve_exact(
                self.state
                    .records
                    .len()
                    .checked_add(1)
                    .ok_or(ControlError::Capacity)?,
            )
            .map_err(|_| ControlError::Capacity)?;
        for record in &self.state.records {
            records.push(clone_record(record)?);
        }
        match existing {
            Ok(index) => *records.get_mut(index).ok_or(ControlError::Failed)? = next,
            Err(index) if index <= records.len() && records.len() < records.capacity() => {
                records.insert(index, next)
            }
            Err(_) => return Err(ControlError::Failed),
        }
        let state = ContactCheckpoint {
            schema: 1,
            cluster: self.state.cluster,
            revision: self
                .state
                .revision
                .checked_add(1)
                .ok_or(ControlError::Capacity)?,
            applied_index: self.state.applied_index,
            records,
        };
        if postcard::experimental::serialized_size(&state)?
            .checked_add(20)
            .is_none_or(|size| size > self.limits.max_checkpoint_bytes)
        {
            return Err(ControlError::Capacity);
        }
        Ok(PreparedNodeContact {
            state,
            owner: self.owner,
            base_revision: self.state.revision,
            base_index: self.state.applied_index,
            node: command.node,
            allocation,
        })
    }
    pub(crate) fn publish(
        &mut self,
        mut prepared: PreparedNodeContact,
        index: u64,
    ) -> Result<(), ControlError> {
        if prepared.owner != self.owner
            || prepared.base_revision != self.state.revision
            || prepared.base_index != self.state.applied_index
            || index <= self.state.applied_index
        {
            return Err(ControlError::Corrupt("contact publication prefix"));
        }
        let position = prepared
            .state
            .records
            .binary_search_by_key(&prepared.node, |record| record.node)
            .map_err(|_| ControlError::Failed)?;
        prepared
            .state
            .records
            .get_mut(position)
            .ok_or(ControlError::Failed)?
            .committed_index = index;
        prepared.state.applied_index = index;
        self.state = prepared.state;
        self._allocation = prepared.allocation;
        Ok(())
    }
}
fn clone_record(record: &ContactRecord) -> Result<ContactRecord, ControlError> {
    let mut name = String::new();
    name.try_reserve_exact(record.server_name.len())
        .map_err(|_| ControlError::Capacity)?;
    name.push_str(&record.server_name);
    Ok(ContactRecord {
        node: record.node,
        principal: record.principal,
        certificate_fingerprint: record.certificate_fingerprint,
        advertise: record.advertise,
        server_name: name,
        generation: record.generation,
        decided_at: record.decided_at,
        committed_index: record.committed_index,
    })
}
fn state_charge(state: &ContactCheckpoint, extra: usize) -> Result<usize, ControlError> {
    let rows = state
        .records
        .capacity()
        .max(state.records.len())
        .checked_add(extra)
        .ok_or(ControlError::Capacity)?;
    rows.checked_mul(
        size_of::<ContactRecord>()
            .checked_add(512)
            .ok_or(ControlError::Capacity)?,
    )
    .and_then(|bytes| bytes.checked_add(4096))
    .ok_or(ControlError::Capacity)
}
fn validate_checkpoint(
    state: &ContactCheckpoint,
    limits: ContactLimits,
    max_index: u64,
) -> Result<(), ControlError> {
    if state.schema != 1
        || state.cluster == [0; 16]
        || state.applied_index > max_index
        || state.records.len() > limits.max_nodes
        || state.revision < state.records.len() as u64
        || state.records.is_empty() != (state.revision == 0)
        || state.records.is_empty() != (state.applied_index == 0)
        || postcard::experimental::serialized_size(state)? > limits.max_checkpoint_bytes
    {
        return Err(ControlError::Corrupt("contact checkpoint"));
    }
    let mut prior = 0;
    let mut revisions = 0u64;
    let mut latest_index = 0;
    for record in &state.records {
        validate_contact_address(record.advertise)?;
        if record.node <= prior
            || record.principal == [0; 16]
            || record.certificate_fingerprint == [0; 32]
            || record.server_name.is_empty()
            || record.server_name.len() > 253
            || record.generation == 0
            || record.generation > state.revision
            || record.committed_index == 0
            || record.committed_index > state.applied_index
            || record.decided_at <= 0
        {
            return Err(ControlError::Corrupt("contact row"));
        }
        prior = record.node;
        revisions = revisions
            .checked_add(record.generation)
            .ok_or(ControlError::Capacity)?;
        latest_index = latest_index.max(record.committed_index);
    }
    if revisions != state.revision || latest_index != state.applied_index {
        return Err(ControlError::Corrupt("contact checkpoint counters"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use focal_enrollment::{
        BootstrapAuthority, EnrollmentLimits, FoundingEnrollmentDraft, JoinKey,
    };
    const NOW: i64 = 1_783_000_000;
    const CLUSTER: [u8; 16] = [31; 16];
    struct NoAuthority;
    impl focal_directory::AuthorityVerifier for NoAuthority {
        fn verify_enrollment(
            &self,
            _: &focal_directory::NodeEnrollment,
        ) -> Result<(), focal_directory::DirectoryError> {
            Err(focal_directory::DirectoryError::UnverifiedAuthority)
        }
        fn verify_session_fence(
            &self,
            _: &focal_directory::SessionFence,
        ) -> Result<(), focal_directory::DirectoryError> {
            Err(focal_directory::DirectoryError::UnverifiedAuthority)
        }
        fn verify_replica_ready(
            &self,
            _: &focal_directory::ReplicaReady,
        ) -> Result<(), focal_directory::DirectoryError> {
            Err(focal_directory::DirectoryError::UnverifiedAuthority)
        }
        fn verify_delegation(
            &self,
            _: &focal_directory::DelegationFence,
        ) -> Result<(), focal_directory::DirectoryError> {
            Err(focal_directory::DirectoryError::UnverifiedAuthority)
        }
    }
    fn fixture() -> (tempfile::TempDir, EnrollmentRegistry, NodeContactCommand) {
        let disk = tempfile::tempdir().unwrap();
        let authority = BootstrapAuthority::open_or_create(
            disk.path().join("ca"),
            CLUSTER,
            vec!["root.focal.test".into()],
            NOW,
        )
        .unwrap();
        let key = JoinKey::open_or_create(disk.path().join("key"), CLUSTER).unwrap();
        let draft = FoundingEnrollmentDraft::open_or_create(
            disk.path().join("founder"),
            &authority,
            &key,
            1,
            [32; 16],
            EnrollmentLimits::default(),
            NOW,
        )
        .unwrap();
        let registry = EnrollmentRegistry::restore(
            &draft.registry().checkpoint().unwrap(),
            CLUSTER,
            EnrollmentLimits::default(),
        )
        .unwrap();
        let command = NodeContactCommand {
            node: 1,
            principal: [32; 16],
            certificate_fingerprint: fingerprint(&draft.receipt().certificate),
            advertise: "127.0.0.1:7443".parse().unwrap(),
            expected_generation: 0,
            decided_at: NOW,
        };
        (disk, registry, command)
    }
    fn budget() -> MemoryBudget {
        MemoryBudget::new(1024 * 1024, 512 * 1024).unwrap()
    }
    #[test]
    fn contact_publication_is_commit_only_fenced_and_checkpoint_exact() {
        let (_disk, registry, mut command) = fixture();
        let allowance = budget();
        let mut contacts =
            NodeContacts::new(CLUSTER, ContactLimits::default(), allowance.clone()).unwrap();
        let before = allowance.stats().used;
        let staged = contacts.prepare(&command, &registry).unwrap();
        assert!(allowance.stats().used > before);
        assert!(contacts.checkpoint().records.is_empty());
        drop(staged);
        assert_eq!(allowance.stats().used, before);
        let staged = contacts.prepare(&command, &registry).unwrap();
        let stale = contacts.prepare(&command, &registry).unwrap();
        contacts.publish(staged, 5).unwrap();
        assert!(contacts.publish(stale, 6).is_err());
        assert_eq!(contacts.checkpoint().records[0].generation, 1);
        assert_eq!(contacts.checkpoint().records[0].committed_index, 5);
        assert!(matches!(
            contacts.prepare(&command, &registry),
            Err(ControlError::Directory(
                focal_directory::DirectoryError::CompareFailed
            ))
        ));
        command.expected_generation = 1;
        command.advertise = "127.0.0.1:7444".parse().unwrap();
        let staged = contacts.prepare(&command, &registry).unwrap();
        contacts.publish(staged, 9).unwrap();
        let bytes = postcard::to_stdvec(contacts.checkpoint()).unwrap();
        let restored = NodeContacts::restore(
            postcard::from_bytes(&bytes).unwrap(),
            ContactLimits::default(),
            budget(),
            9,
        )
        .unwrap();
        assert_eq!(restored.checkpoint(), contacts.checkpoint());
        assert!(
            NodeContacts::restore(
                contacts.checkpoint().clone(),
                ContactLimits::default(),
                budget(),
                8
            )
            .is_err()
        );
        let mut corrupted = contacts.checkpoint().clone();
        corrupted.revision += 1;
        assert!(NodeContacts::restore(corrupted, ContactLimits::default(), budget(), 9).is_err());
    }
    #[test]
    fn contact_identity_is_active_enrollment_and_cannot_name_another_node() {
        let (_disk, mut registry, command) = fixture();
        let contacts = NodeContacts::new(CLUSTER, ContactLimits::default(), budget()).unwrap();
        for field in 0..3 {
            let mut bad = command.clone();
            match field {
                0 => bad.node = 2,
                1 => bad.principal = [99; 16],
                _ => bad.certificate_fingerprint = [99; 32],
            }
            assert!(contacts.prepare(&bad, &registry).is_err());
        }
        let receipt = registry.enrollments().next().unwrap().clone();
        let revoke = registry
            .prepare_revoke(receipt.invitation, NOW + 1)
            .unwrap();
        registry.apply_committed(&revoke, 1).unwrap();
        let mut revoked = command;
        revoked.decided_at = NOW + 1;
        assert!(matches!(
            contacts.prepare(&revoked, &registry),
            Err(ControlError::Enrollment(
                focal_enrollment::EnrollmentError::Revoked
            ))
        ));
    }
    #[test]
    fn staging_is_admitted_and_foreign_prepared_values_are_rejected() {
        let (_disk, registry, command) = fixture();
        let allowance = budget();
        let contacts =
            NodeContacts::new(CLUSTER, ContactLimits::default(), allowance.clone()).unwrap();
        let mut other = NodeContacts::new(CLUSTER, ContactLimits::default(), budget()).unwrap();
        let staged = contacts.prepare(&command, &registry).unwrap();
        assert!(other.publish(staged, 1).is_err());
        assert!(other.checkpoint().records.is_empty());
        let free = allowance.stats().limit - allowance.stats().used;
        let occupied = allowance
            .reserve(BudgetKind::Pending, BudgetLane::Completion, free)
            .unwrap()
            .commit();
        assert!(matches!(
            contacts.prepare(&command, &registry),
            Err(ControlError::Memory(_))
        ));
        assert!(contacts.checkpoint().records.is_empty());
        drop(occupied);
        assert!(contacts.prepare(&command, &registry).is_ok());
    }
    #[test]
    fn retry_digest_excludes_only_owner_decision_time() {
        let (_disk, _registry, command) = fixture();
        let request = ControlRequest {
            id: crate::ControlRequestId {
                client: command.principal,
                sequence: 1,
            },
            acknowledged_through: 0,
            command: crate::ControlCommand::NodeContact(command.clone()),
        };
        let digest = command.intent_hash(&request).unwrap();
        let mut retry = command;
        retry.decided_at += 100;
        assert_eq!(retry.intent_hash(&request).unwrap(), digest);
        retry.expected_generation = 1;
        assert_ne!(retry.intent_hash(&request).unwrap(), digest);
        retry.expected_generation = 0;
        retry.advertise = "127.0.0.1:7444".parse().unwrap();
        assert_ne!(retry.intent_hash(&request).unwrap(), digest);
    }
    #[test]
    fn uncheckpointed_contact_replay_preserves_receipt_with_later_retry_clock() {
        let (disk, registry, command) = fixture();
        let allowance = || MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
        let directory = focal_directory::RootDirectory::new(
            focal_directory::ClusterId(CLUSTER),
            focal_directory::RootConfig::default(),
            allowance(),
        )
        .unwrap();
        let bootstrap = crate::ControlBootstrap::root(&directory, &registry).unwrap();
        let options =
            crate::ControlOptions::new(focal_consensus::NodeConfig::single(1, CLUSTER, [44; 16]));
        let path = disk.path().join("root-wal");
        let mut replica =
            crate::ControlReplica::open(options.clone(), bootstrap.clone(), allowance(), &path)
                .unwrap();
        replica.drain(&NoAuthority).unwrap();
        replica.campaign().unwrap();
        replica.drain(&NoAuthority).unwrap();
        let request = ControlRequest {
            id: crate::ControlRequestId {
                client: command.principal,
                sequence: 1,
            },
            acknowledged_through: 0,
            command: crate::ControlCommand::NodeContact(command.clone()),
        };
        assert_eq!(
            replica.submit(request.clone(), &NoAuthority).unwrap(),
            crate::ControlSubmission::Pending(request.id)
        );
        let receipt = replica.drain(&NoAuthority).unwrap().completed.unwrap();
        let contacts = replica.contacts().unwrap().clone();
        assert!(replica.root().unwrap().checkpoint().regions.is_empty());
        assert!(replica.authority().is_none());
        drop(replica); // WAL only: deliberately no checkpoint.
        let mut replica =
            crate::ControlReplica::open(options, bootstrap, allowance(), &path).unwrap();
        replica.drain(&NoAuthority).unwrap();
        assert_eq!(replica.contacts().unwrap(), &contacts);
        assert_eq!(replica.receipt(request.id).unwrap(), Some(receipt));
        let mut retry = request;
        if let crate::ControlCommand::NodeContact(command) = &mut retry.command {
            command.decided_at += 100;
        }
        // A retained receipt is exact even before campaigning after restart.
        assert_eq!(
            replica.submit(retry.clone(), &NoAuthority).unwrap(),
            crate::ControlSubmission::Existing(receipt)
        );
        if let crate::ControlCommand::NodeContact(command) = &mut retry.command {
            command.advertise = "127.0.0.1:7449".parse().unwrap();
        }
        assert!(matches!(
            replica.submit(retry, &NoAuthority),
            Err(ControlError::RetryConflict)
        ));
        assert_eq!(replica.contacts().unwrap(), &contacts);
    }
}
