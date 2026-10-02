#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! Durable metadata hosting: one serial owner per independent Raft group.
//! Preparation owns capacity before proposal; only durable committed entries
//! publish directory/enrollment state and complete a client request.

mod authority;
mod contacts;
mod membership;
mod replica;
mod retry;
mod rpc;
mod state;

pub use authority::*;
pub use contacts::*;
pub use membership::*;
pub use replica::*;
pub use rpc::*;

/// The voter a stopping leader hands its log to (27 §5): among the voters
/// it has heard from recently and is not sending a snapshot, the one whose log
/// matches furthest — the transfer catches it up to the last entry before
/// it is asked to campaign, so the nearest one is the quickest — and the
/// lowest id among equals, so that every reading of the same progress
/// names one heir. None while no other voter qualifies: a leader alone
/// has no one to hand off to, and stops as it did.
pub fn heir(
    status: &focal_consensus::NodeStatus,
    peers: &[focal_consensus::PeerProgress],
) -> Option<u64> {
    peers
        .iter()
        .filter(|peer| {
            peer.node != status.node_id
                && status.voters.contains(&peer.node)
                && peer.state != focal_consensus::PEER_SNAPSHOT
                && peer.recent_active
        })
        .max_by_key(|peer| (peer.matched, core::cmp::Reverse(peer.node)))
        .map(|peer| peer.node)
}
pub use state::*;

use focal_directory::{DirectoryError, PartitionCommand, PartitionConfig, RootCommand, RootConfig};
use focal_enrollment::{EnrollmentCommand, EnrollmentError, EnrollmentLimits};
use focal_memory::MemoryError;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("metadata consensus: {0}")]
    Consensus(#[from] focal_consensus::ConsensusError),
    #[error("directory: {0:?}")]
    Directory(#[from] DirectoryError),
    #[error("enrollment: {0}")]
    Enrollment(#[from] EnrollmentError),
    #[error("memory admission: {0:?}")]
    Memory(#[from] MemoryError),
    #[error("metadata codec: {0}")]
    Codec(#[from] postcard::Error),
    #[error("metadata capacity exceeded")]
    Capacity,
    #[error("invalid metadata configuration or request")]
    Invalid,
    #[error("metadata scope, cluster, group, or genesis mismatch")]
    WrongOwner,
    #[error("another metadata command is already pending")]
    Busy,
    #[error("leader has not published a current-term committed prefix")]
    NotReady,
    #[error("request ID was reused with different content")]
    RetryConflict,
    #[error("request is below the durably acknowledged retry floor")]
    RetryExpired,
    #[error("client requests must be consecutive and acknowledgments cannot exceed completed work")]
    RetryOrder,
    #[error("metadata replica failed during durable delivery; reopen for recovery")]
    Failed,
    #[error("corrupt committed metadata: {0}")]
    Corrupt(&'static str),
}

#[derive(Debug, Clone)]
pub struct ControlLimits {
    pub max_command_bytes: usize,
    pub max_checkpoint_bytes: usize,
    pub max_clients: usize,
    pub max_receipts_per_client: usize,
}
impl Default for ControlLimits {
    fn default() -> Self {
        Self {
            max_command_bytes: 128 * 1024,
            max_checkpoint_bytes: 8 * 1024 * 1024,
            max_clients: 1024,
            max_receipts_per_client: 32,
        }
    }
}
#[derive(Debug, Clone)]
pub struct ControlOptions {
    pub consensus: focal_consensus::NodeConfig,
    /// The genesis of a group founded on a sealed image this replica does
    /// not hold (a seated member of a split destination, 24 §13): the
    /// replica opens with the group's identity and no state, its state
    /// comes by snapshot, and it applies no entry before one — the founder
    /// compacted at founding, so none is ever sent.
    pub founded_elsewhere: Option<[u8; 32]>,
    pub limits: ControlLimits,
    pub root: RootConfig,
    pub partition: PartitionConfig,
    pub enrollment: EnrollmentLimits,
    pub authority: focal_directory::AuthorityConfig,
    pub contacts: ContactLimits,
}
impl ControlOptions {
    pub fn new(consensus: focal_consensus::NodeConfig) -> Self {
        Self {
            consensus,
            founded_elsewhere: None,
            limits: ControlLimits::default(),
            root: RootConfig::default(),
            partition: PartitionConfig::default(),
            enrollment: EnrollmentLimits::default(),
            authority: focal_directory::AuthorityConfig::default(),
            contacts: ContactLimits::default(),
        }
    }
    fn validate(&self) -> Result<(), ControlError> {
        if self.limits.max_command_bytes == 0
            || self.limits.max_command_bytes > self.consensus.max_entry_bytes
            || self.limits.max_checkpoint_bytes == 0
            || self.limits.max_checkpoint_bytes > 8 * 1024 * 1024
            || self.limits.max_clients == 0
            || self.limits.max_receipts_per_client == 0
        {
            return Err(ControlError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ControlRequestId {
    pub client: [u8; 16],
    pub sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "bounded command owned during admission; avoids unaccounted boxing"
)]
pub enum ControlCommand {
    Root(RootCommand),
    Enrollment(EnrollmentCommand),
    Partition(PartitionCommand),
    // Append only: existing command discriminants are persisted log bytes.
    ActivateAuthority(AuthorityActivation),
    Authority(focal_directory::AuthorityCommand),
    InstallAuthority(AuthorityInstallation),
    VerifiedRoot(VerifiedRootCommand),
    VerifiedPartition(VerifiedPartitionCommand),
    Membership(ControlMembershipCommand),
    NodeContact(NodeContactCommand),
    RetireContact(RetireContactCommand),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRequest {
    /// The authenticated host must bind this client to its caller principal.
    pub id: ControlRequestId,
    /// Durably forget this client's outcomes through this sequence. Forgotten
    /// requests are rejected forever while the client slot remains registered.
    pub acknowledged_through: u64,
    pub command: ControlCommand,
}
impl ControlRequest {
    pub(crate) fn digest(&self) -> Result<[u8; 32], ControlError> {
        match &self.command {
            ControlCommand::NodeContact(command) => command.intent_hash(self),
            _ => hash("focal.control.request.v1", self),
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRevisions {
    pub root: u64,
    pub enrollment: u64,
    pub partition: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlReceipt {
    pub request: ControlRequestId,
    pub request_hash: [u8; 32],
    pub committed_index: u64,
    pub committed_term: u64,
    pub revisions: ControlRevisions,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSubmission {
    Pending(ControlRequestId),
    Existing(ControlReceipt),
}

fn charge(bytes: usize, factor: usize) -> Result<usize, ControlError> {
    bytes.checked_mul(factor).ok_or(ControlError::Capacity)
}
fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, ControlError> {
    let len = postcard::experimental::serialized_size(value)?;
    if len > limit {
        return Err(ControlError::Capacity);
    }
    let mut bytes = vec![0; len];
    postcard::to_slice(value, &mut bytes)?;
    Ok(bytes)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T, ControlError> {
    if bytes.len() > limit {
        return Err(ControlError::Capacity);
    }
    let (value, rest) = postcard::take_from_bytes(bytes)?;
    if !rest.is_empty() {
        return Err(ControlError::Invalid);
    }
    Ok(value)
}
/// Keyed hash of already-serialized bytes. Equivalent to `hash(domain, value)`
/// when `bytes` is `encode(value)`, so a caller that already encoded a value can
/// hash it without serializing it a second time.
fn hash_bytes(domain: &'static str, bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(domain);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}
fn hash<T: Serialize>(domain: &'static str, value: &T) -> Result<[u8; 32], ControlError> {
    struct Sink(blake3::Hasher);
    impl postcard::ser_flavors::Flavor for Sink {
        type Output = [u8; 32];
        fn try_push(&mut self, byte: u8) -> postcard::Result<()> {
            self.0.update(&[byte]);
            Ok(())
        }
        fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
            self.0.update(bytes);
            Ok(())
        }
        fn finalize(self) -> postcard::Result<Self::Output> {
            Ok(*self.0.finalize().as_bytes())
        }
    }
    Ok(postcard::serialize_with_flavor(
        value,
        Sink(blake3::Hasher::new_derive_key(domain)),
    )?)
}

#[cfg(test)]
mod heir_tests {
    use super::heir;
    use focal_consensus::{
        NodeStatus, PEER_PROBE, PEER_REPLICATE, PEER_SNAPSHOT, PeerProgress, StateRole,
    };

    fn peer(node: u64, matched: u64, state: u8, recent_active: bool) -> PeerProgress {
        PeerProgress {
            node,
            matched,
            next_index: matched.saturating_add(1),
            state,
            recent_active,
            paused: false,
            pending_snapshot: 0,
        }
    }
    fn leader(voters: &[u64]) -> NodeStatus {
        NodeStatus {
            node_id: 1,
            leader_id: 1,
            term: 3,
            committed_index: 40,
            applied_index: 40,
            role: StateRole::Leader,
            voters: voters.to_vec(),
            learners: Vec::new(),
        }
    }

    /// A voter the leader is probing after a lost message (27 §3.3) is as
    /// fit an heir as one it streams to — its log is what counts; one being
    /// sent a snapshot, one not heard from and a learner are not asked.
    #[test]
    fn the_heir_is_the_furthest_voter_heard_from_whatever_its_pipeline_state() {
        let status = leader(&[1, 2, 3, 4, 5]);
        let peers = [
            peer(2, 40, PEER_REPLICATE, true),
            peer(3, 40, PEER_PROBE, true),
            peer(4, 12, PEER_SNAPSHOT, true),
            peer(5, 40, PEER_REPLICATE, false),
            peer(6, 40, PEER_REPLICATE, true),
        ];
        // Equal logs: the lowest id, and the probed voter counts.
        assert_eq!(heir(&status, &peers), Some(2));
        let peers = [
            peer(2, 39, PEER_REPLICATE, true),
            peer(3, 40, PEER_PROBE, true),
            peer(4, 41, PEER_SNAPSHOT, true),
        ];
        assert_eq!(heir(&status, &peers), Some(3));
        assert_eq!(heir(&status, &[peer(4, 41, PEER_SNAPSHOT, true)]), None);
    }
}
