use crate::DirectoryError;
use focal_model::{ContentHash, LedgerId, RaftIndex, RaftTerm, RouteEpoch, SessionSeq};
use serde::{Deserialize, Serialize};

macro_rules! identifier {
    ($($name:ident),+) => {$(
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub [u8; 16]);
        impl $name { pub const fn from_u128(value: u128) -> Self { Self(value.to_be_bytes()) } }
    )+};
}
identifier!(
    ClusterId,
    RegionId,
    ZoneId,
    PartitionId,
    LogGroupId,
    OperationId,
    WorkId
);
impl RegionId {
    /// Geography has not been established. This value is never a registered
    /// region, residency choice, or independently promised failure domain.
    pub const UNKNOWN: Self = Self([0; 16]);
}

/// Canonical tenant/session bytes, without ambient hashing or per-claim keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NamespaceKey(pub [u8; 32]);
impl NamespaceKey {
    pub const MIN: Self = Self([0; 32]);
    pub fn of(ledger: LedgerId) -> Self {
        let mut key = [0; 32];
        for (target, source) in key
            .iter_mut()
            .zip(ledger.tenant.0.iter().chain(&ledger.session.0))
        {
            *target = *source;
        }
        Self(key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceRange {
    pub start: NamespaceKey,
    /// Exclusive end; None is the end of the entire namespace.
    pub end: Option<NamespaceKey>,
}
impl NamespaceRange {
    pub fn all() -> Self {
        Self {
            start: NamespaceKey::MIN,
            end: None,
        }
    }
    pub fn validate(self) -> Result<(), DirectoryError> {
        if self.end.is_some_and(|end| end <= self.start) {
            Err(DirectoryError::Invalid("empty or reversed namespace range"))
        } else {
            Ok(())
        }
    }
    pub fn contains(self, ledger: LedgerId) -> bool {
        self.contains_key(NamespaceKey::of(ledger))
    }
    pub fn contains_key(self, key: NamespaceKey) -> bool {
        key >= self.start && self.end.is_none_or(|end| key < end)
    }
    pub fn overlaps(self, other: Self) -> bool {
        self.end.is_none_or(|end| other.start < end) && other.end.is_none_or(|end| self.start < end)
    }
}

/// Verified enrollment inputs come from the infrastructure authority, never
/// caller-provided topology labels. Re-enrollment increments generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEnrollment {
    pub node: u64,
    pub generation: u64,
    /// Zero is unknown, never a usable failure domain or residency claim.
    pub region: RegionId,
    /// Zero is unknown. A known zone requires a known enclosing region.
    pub zone: ZoneId,
    pub endpoint: String,
    /// The enrolled key's identity (`EnrollmentReceipt::public_key`), stable
    /// across certificate renewal; statements are verified by the certificate
    /// the registry authorizes and bound to this key.
    pub identity: ContentHash,
    pub authority_epoch: u64,
    pub attestation: ContentHash,
    pub eligible: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLoad {
    pub node: u64,
    pub generation: u64,
    pub report: u64,
    pub available_memory: u64,
    pub active_weight: u64,
    /// Bytes the node's data directory can still take before its disk
    /// watermark refuses admission. A report of zero never receives new
    /// assignments; the V1 row codec restores it as zero.
    pub disk_available: u64,
    /// The capability level the node's binary announces (24 §21); zero is
    /// unknown (a report from before the level was announced). The V1 row
    /// codec restores it as zero.
    pub capability: u32,
}

/// What the fleet's failure detector last committed about a node through the
/// partition owner ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §12).
/// Liveness never touches enrollment: a dead node keeps its identity,
/// generation and grants, and a revived one needs no re-enrollment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLiveness {
    pub alive: bool,
    /// The node's own incarnation the verdict was reached at; a later
    /// incarnation always wins, an older one is stale.
    pub incarnation: u64,
    /// The node whose detector reached the verdict.
    pub witness: u64,
    pub decided_at: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub enrollment: NodeEnrollment,
    pub load: Option<NodeLoad>,
    /// Absent until the detector committed a verdict; a node with no
    /// verdict counts as alive.
    pub liveness: Option<NodeLiveness>,
}
impl NodeRecord {
    pub fn is_alive(&self) -> bool {
        self.liveness.is_none_or(|liveness| liveness.alive)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionFenceKind {
    Created,
    Cutover,
    Activated,
}

/// Coordinates of an already committed session-log authority record. A hash
/// is not an authorization credential: AuthorityVerifier must validate this
/// against the session's committed log/certificate before directory preparation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionFence {
    pub kind: SessionFenceKind,
    pub ledger: LedgerId,
    pub log_group: LogGroupId,
    pub operation: OperationId,
    pub sequence: SessionSeq,
    pub index: RaftIndex,
    pub term: RaftTerm,
    pub from_route: RouteEpoch,
    pub to_route: RouteEpoch,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    /// Digest of the exact placement and policy authorized by the session.
    pub placement_digest: ContentHash,
    pub record_hash: ContentHash,
}

/// A bounded verifier over previously authenticated/committed evidence. It
/// must not make remote calls inside a control partition's deterministic apply.
pub trait AuthorityVerifier {
    fn verify_enrollment(&self, enrollment: &NodeEnrollment) -> Result<(), DirectoryError>;
    fn verify_session_fence(&self, fence: &SessionFence) -> Result<(), DirectoryError>;
    fn verify_replica_ready(&self, ready: &ReplicaReady) -> Result<(), DirectoryError>;
    fn verify_delegation(&self, fence: &DelegationFence) -> Result<(), DirectoryError>;
    fn verify_custody(&self, proof: &CustodyProof) -> Result<(), DirectoryError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaReady {
    pub ledger: LedgerId,
    pub operation: OperationId,
    pub route_epoch: RouteEpoch,
    pub node: u64,
    pub node_generation: u64,
    pub through: SessionSeq,
    /// Verified durable artifact/checkpoint custody through this prefix.
    pub custody: ContentHash,
    pub attestation: ContentHash,
}

/// A copy holder's statement that it durably holds the content set of one
/// session through `verified_through` at one placement epoch. Like
/// `ReplicaReady`, the attestation field is zero in the signed body and only
/// the holder itself signs; a transfer request or an install intent is never
/// custody.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustodyProof {
    pub ledger: LedgerId,
    pub node: u64,
    pub node_generation: u64,
    pub custody_epoch: u64,
    pub verified_through: SessionSeq,
    /// Digest of the exact content set the holder verified.
    pub content: ContentHash,
    pub attestation: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationFence {
    pub cluster: ClusterId,
    pub operation: OperationId,
    pub source: PartitionId,
    pub destination: PartitionId,
    pub namespace: NamespaceRange,
    pub from_epoch: u64,
    pub to_epoch: u64,
    pub sealed_revision: u64,
    pub checkpoint: ContentHash,
    pub destination_ready: ContentHash,
}

pub(crate) fn nonzero_hash(hash: ContentHash) -> bool {
    hash.0 != [0; 32]
}
