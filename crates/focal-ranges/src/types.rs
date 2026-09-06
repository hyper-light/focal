use crate::*;
use focal_model::{ContentHash, LedgerId, RaftIndex, RaftTerm, RouteEpoch, SessionSeq};
use serde::{Deserialize, Serialize};

macro_rules! id {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub [u8; 16]);
        impl $name { pub const fn from_u128(value: u128) -> Self { Self(value.to_be_bytes()) } }
    )+};
}
id!(
    RangeId,
    TransferId,
    ControllerIncarnation,
    QueryId,
    TransactionId
);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReplicaId {
    pub node: u64,
    pub generation: u64,
}
impl ReplicaId {
    pub(crate) fn validate(self) -> Result<(), RangeError> {
        if self.node == 0 || self.generation == 0 {
            Err(RangeError::Generation)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RangeLimits {
    pub max_ranges: usize,
    pub max_readers: usize,
    pub max_history: usize,
    pub max_pins: usize,
    pub max_pin_ttl: u64,
    pub max_blocks: usize,
    pub max_block_rows: usize,
    pub max_block_bytes: usize,
    pub max_replica_rows: usize,
    pub max_batch_writes: usize,
    pub max_receipts: usize,
    pub max_checkpoint_bytes: usize,
}
impl Default for RangeLimits {
    fn default() -> Self {
        Self {
            max_ranges: 64,
            max_readers: 4,
            max_history: 8,
            max_pins: 128,
            max_pin_ttl: 30_000,
            max_blocks: 1024,
            max_block_rows: 256,
            max_block_bytes: 1024 * 1024,
            max_replica_rows: 1_000_000,
            max_batch_writes: 4096,
            max_receipts: 128,
            max_checkpoint_bytes: 64 * 1024 * 1024,
        }
    }
}
impl RangeLimits {
    pub(crate) fn validate(self) -> Result<(), RangeError> {
        if [
            self.max_ranges,
            self.max_readers,
            self.max_history,
            self.max_pins,
            self.max_blocks,
            self.max_block_rows,
            self.max_block_bytes,
            self.max_replica_rows,
            self.max_batch_writes,
            self.max_receipts,
            self.max_checkpoint_bytes,
        ]
        .contains(&0)
            || self.max_pin_ttl == 0
        {
            return Err(RangeError::Invalid("zero range limit"));
        }
        Ok(())
    }
}
/// Verified session-log authority, not a new range transaction decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitProof {
    pub ledger: LedgerId,
    pub sequence: SessionSeq,
    pub index: RaftIndex,
    pub term: RaftTerm,
    pub command: ContentHash,
    pub attestation: ContentHash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSealProof {
    pub ledger: LedgerId,
    pub operation: TransferId,
    pub old_epoch: RouteEpoch,
    pub range: RangeId,
    pub range_generation: u64,
    pub replica: ReplicaId,
    pub cut: SessionSeq,
    pub checkpoint: ContentHash,
    pub attestation: ContentHash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationReady {
    pub ledger: LedgerId,
    pub operation: TransferId,
    pub new_epoch: RouteEpoch,
    pub range: RangeId,
    pub range_generation: u64,
    pub replica: ReplicaId,
    pub seed: SessionSeq,
    pub through: SessionSeq,
    pub snapshot: ContentHash,
    pub state: ContentHash,
    pub checkpoint: ContentHash,
    pub attestation: ContentHash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryProof {
    pub ledger: LedgerId,
    pub epoch: RouteEpoch,
    pub through: SessionSeq,
    pub manifest: ContentHash,
    pub attestation: ContentHash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeProgress {
    pub ledger: LedgerId,
    pub epoch: RouteEpoch,
    pub range: RangeId,
    pub range_generation: u64,
    pub replica: ReplicaId,
    pub through: SessionSeq,
    pub term: RaftTerm,
    pub root: ContentHash,
    pub attestation: ContentHash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadAvailability {
    pub ledger: LedgerId,
    pub epoch: RouteEpoch,
    pub range: RangeId,
    pub range_generation: u64,
    pub replica: ReplicaId,
    pub prefix: SessionSeq,
    pub lease: u64,
    pub expires_at: u64,
    pub root: ContentHash,
    pub attestation: ContentHash,
}
/// Pinned authenticated evidence supplied by the session/replica runtime.
/// A hash alone is never treated as proof of disk custody or authority.
pub trait RangeVerifier {
    fn commit(&self, proof: &CommitProof) -> Result<(), RangeError>;
    /// Verify this range's deterministic fragment belongs to the one committed
    /// session decision, including its predecessor reads and transaction ID.
    fn batch(&self, batch: &RangeBatch, proof: &CommitProof) -> Result<(), RangeError>;
    fn source_seal(&self, proof: &SourceSealProof) -> Result<(), RangeError>;
    fn destination_ready(&self, proof: &DestinationReady) -> Result<(), RangeError>;
    fn recovery(&self, proof: &RecoveryProof) -> Result<(), RangeError>;
    fn progress(&self, proof: &RangeProgress) -> Result<(), RangeError>;
    fn read(&self, proof: &ReadAvailability) -> Result<(), RangeError>;
}
pub(crate) fn verify_commit(
    proof: &CommitProof,
    ledger: LedgerId,
    sequence: SessionSeq,
    command: ContentHash,
    verifier: &impl RangeVerifier,
) -> Result<(), RangeError> {
    if proof.ledger != ledger {
        return Err(RangeError::WrongLedger);
    }
    if proof.sequence != sequence || proof.command != command {
        return Err(RangeError::Conflict);
    }
    if proof.index.0 == 0 || proof.term.0 == 0 || !nonzero(proof.attestation) {
        return Err(RangeError::Unverified);
    }
    verifier.commit(proof)
}
