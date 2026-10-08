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
//! Disk-durable Raft with an injected clock and transport-independent events.
//!
//! `propose` accepts work for replication. Only `drain` emits committed entries,
//! after Ready and LightReady commit metadata are durable. The application must
//! publish that complete prefix before replying to clients. The core is
//! hyper-raft (27 §4.2), focal's core moved to the shared repository; its log
//! and messages are held in the encoding of `raft-rs` 0.7, which groups ran on
//! before ([`envelope`]), so a group whose nodes are replaced one by one is
//! one group throughout.

mod membership;
mod memory;
pub use membership::*;
mod checkpoint;
mod core_state;
mod decoder;
pub mod envelope;
mod facade;
pub mod group_files;
mod persistence;
mod shell;
mod shell_node;
mod storage;
/// A group's timing, derived from the round trips it measures (27 §3.1 P2).
pub use focal_timing as timing;

use focal_log::{LogError, LogicalLogId, Record, RecordKind, WalIdentity, WalLease, WalOptions};
use focal_memory::{Allocation, BudgetKind, BudgetLane, DiskBudget, MemoryBudget};
use hyper_raft::{Config, Limits, RawNode, Storage, proto, wire::Record as _};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
};
use storage::RamLog;
use thiserror::Error;

pub use envelope::{EnvelopeError, Wire, encode_message, encode_message_in};
pub use focal_log::{FaultPoint, SharedWal};
use hyper_raft::progress::ProgressState;
pub use hyper_raft::proto::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
    Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
};
pub use hyper_raft::proto::{snapshot_index, snapshot_is_empty};
pub use hyper_raft::{SnapshotStatus, StateRole};

/// The image a restored logical log begins with (26 §6): the application
/// snapshot at one index and term, and the decoder floor (with its
/// transition, when the log had one) the application requires.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoredLog {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    pub floor: [u8; 32],
    pub transition: Option<([u8; 32], [u8; 32])>,
}

/// The bytes of committed entries one Ready gives to apply: the page a
/// transition reads from storage at most.
pub(crate) const COMMITTED_PAGE_BYTES: u64 = 16 * 1024 * 1024;
/// The most bytes of the application's state a snapshot, a checkpoint or a
/// restored image holds.
pub(crate) const IMAGE_BYTES: usize = 8 * 1024 * 1024;
/// The most members a group's configuration names, voters and learners together. It is the bound
/// focal has held since before its core moved to hyper-raft, and it sizes the group files' records
/// (`group_files::META_BOUND`), so it is carried unchanged and no file's bounds move. focal states
/// it to the core as the members its groups name (`hyper_raft::Stated::members`), the same at every
/// member, as the core requires.
pub const MAX_MEMBERS: usize = 1024;
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: u64,
    pub cluster_id: [u8; 16],
    pub group_id: [u8; 16],
    /// Bootstrap membership. Subsequent changes must be committed through Raft.
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub election_tick: usize,
    pub heartbeat_tick: usize,
    pub max_entry_bytes: usize,
    pub max_uncommitted_bytes: u64,
    pub max_inflight_messages: usize,
    /// Whether the group has the fast track (27 §4): a proposal from a
    /// member that does not lead goes to every voter at once, and is
    /// committed when a fast quorum holds it. It is part of what the group
    /// is, the same at every member and for as long as the group lives. It
    /// is no part of the identity record, whose bytes are as they were: a
    /// group that has it says so in a record of its own, which a binary
    /// that knows no fast track refuses.
    ///
    /// No owner sets it: an owner must derive nothing from an entry's
    /// term, and a leader that outlives two changes of its configuration
    /// commits by the classic quorum until its term ends (27 §4.6).
    #[serde(skip)]
    pub fast: bool,
    /// A test's election seed, in place of the system's randomness, so that two backends given
    /// one input stream draw alike (the differential, 27 §15.10). Never serialized; it exists only
    /// in this crate's tests.
    #[cfg(test)]
    #[serde(skip)]
    pub(crate) election_seed: Option<u64>,
}

impl NodeConfig {
    pub fn single(node_id: u64, cluster_id: [u8; 16], group_id: [u8; 16]) -> Self {
        Self::joining(node_id, cluster_id, group_id, vec![node_id], Vec::new())
    }
    /// A new physical replica replays the original group's exact bootstrap
    /// membership. Its own ID is deliberately absent until a committed learner
    /// change arrives; it cannot campaign and has no voting weight beforehand.
    pub fn joining(
        node_id: u64,
        cluster_id: [u8; 16],
        group_id: [u8; 16],
        voters: Vec<u64>,
        learners: Vec<u64>,
    ) -> Self {
        Self {
            node_id,
            cluster_id,
            group_id,
            voters,
            learners,
            election_tick: 10,
            heartbeat_tick: 2,
            max_entry_bytes: 4 * 1024 * 1024,
            max_uncommitted_bytes: 32 * 1024 * 1024,
            max_inflight_messages: DEFAULT_INFLIGHT_WINDOW,
            fast: false,
            #[cfg(test)]
            election_seed: None,
        }
    }

    /// Whether `other` names the same member of the same group with the same bootstrap
    /// membership: what a member's persisted identity must match when it opens again, on either
    /// backend. The tunables (ticks, entry and window sizes) may change between starts.
    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        self.node_id == other.node_id
            && self.cluster_id == other.cluster_id
            && self.group_id == other.group_id
            && self.voters == other.voters
            && self.learners == other.learners
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.voters.len().saturating_add(self.learners.len()) > 1024 {
            return Err(ConsensusError::Capacity);
        }
        let ids: BTreeSet<_> = self.voters.iter().chain(&self.learners).copied().collect();
        if self.node_id == 0
            || ids.contains(&0)
            || self.voters.is_empty()
            || ids.len() != self.voters.len().saturating_add(self.learners.len())
            || ids.len() > 1024
            || self.heartbeat_tick == 0
            || self.election_tick <= self.heartbeat_tick
            || self.election_tick > 1_000_000
            || self.max_entry_bytes == 0
            || self.max_entry_bytes > 8 * 1024 * 1024
            || self.max_uncommitted_bytes < (self.max_entry_bytes as u64).saturating_add(1024)
            || self.max_inflight_messages == 0
            || self.max_inflight_messages > 65536
        {
            return Err(ConsensusError::Configuration(
                "invalid membership, tick, or capacity limits",
            ));
        }
        // The shared crates' fast track releases a held fast vote before the entry it covers is
        // safe from a later leader's truncation (09, 2026-10-05). Until the snapshot that fixes
        // it, only this crate's own tests run it; the rule goes with that snapshot.
        #[cfg(not(test))]
        if self.fast {
            return Err(ConsensusError::Configuration(
                "the fast track is withheld until the shared crates' release fix is taken",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("durable log: {0}")]
    Log(#[from] LogError),
    #[error("Raft: {0}")]
    Raft(#[from] hyper_raft::Error),
    #[error("Raft protocol codec: {0}")]
    Envelope(#[from] EnvelopeError),
    #[error("encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("configuration: {0}")]
    Configuration(&'static str),
    #[error("inconsistent durable Raft state: {0}")]
    Corruption(&'static str),
    #[error("replica is not leader (known leader: {leader})")]
    NotLeader { leader: u64 },
    #[error("entry, read context, or pending proposal capacity exceeded")]
    Capacity,
    #[error("node stopped after persistence/apply failure; reopen for recovery")]
    Failed,
    #[error("durability is outstanding; poll try_drain before mutating this group")]
    PersistencePending,
    #[error("the application has not confirmed this group's required decoder")]
    DecoderUnconfirmed,
    #[error("the application decoder differs from the immutable group decoder floor")]
    DecoderMismatch,
    #[error("Raft dependency failed internally; node stopped until recovery")]
    DependencyFailure,
    #[error("checkpoint is not at the delivered committed prefix")]
    CheckpointIndex,
    #[error("learner is not durably caught up through the commit index")]
    LearnerBehind,
    #[error("a membership change is committed and not yet applied here; ask again once it is")]
    MembershipPending,
    #[error("invalid peer message: {0}")]
    MalformedMessage(&'static str),
    #[error("a leader does not remove itself; transfer leadership first")]
    LeaderLeaving,
}

impl From<hyper_raft::StorageError> for ConsensusError {
    fn from(error: hyper_raft::StorageError) -> Self {
        Self::Raft(hyper_raft::Error::Storage(error))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}
/// The messages a leader keeps in flight to one follower before an
/// acknowledgement (`NodeConfig::max_inflight_messages`, thesis §10.2.1's
/// pipeline): the wire's control lane to a peer is derived from it, so the
/// pipeline is never narrower on the wire than in the core (27 §3.1 P1).
pub const DEFAULT_INFLIGHT_WINDOW: usize = 128;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBarrier {
    pub index: u64,
    pub context: Vec<u8>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedSnapshot {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    /// Exact configuration at this snapshot prefix, before any later entries
    /// delivered in the same drain. Snapshot bytes remain application-owned.
    pub configuration: MembershipConfiguration,
}

#[derive(Debug, Default)]
pub struct NodeEvents {
    pub messages: Vec<Message>,
    pub committed: Vec<CommittedEntry>,
    pub membership: Vec<AppliedMembership>,
    pub read_states: Vec<ReadBarrier>,
    /// What was proposed here by the fast track and another entry took the
    /// index of: it is not in the log, and its proposer proposes it again.
    pub displaced: Vec<CommittedEntry>,
    pub snapshot: Option<AppliedSnapshot>,
    /// Includes Raft-internal entries; this is never a SessionSeq.
    pub applied_index: u64,
    allocation: Option<Allocation>,
}
/// Makes, for each write of a group, what the log's writer calls once the
/// write is answered ([`focal_log::Persisted`]).
pub type PersistedSignal = Box<dyn Fn() -> focal_log::Persisted + Send>;
/// A member that is let go writes the commit its log does not hold: its
/// log then says what it applied, and a restart needs no one to tell it.
/// The write is queued, not waited for; the log finishes what it was given
/// before it closes. Nothing is written for a member that failed or still
/// persists something: its log answers for itself.
impl Drop for LogNode {
    fn drop(&mut self) {
        if !self.failed
            && self.persistence.is_none()
            && self.checkpoint.is_none()
            && self.decoder_write.is_none()
            && self.commit_unwritten
        {
            let _ = self.write_commit_behind();
        }
    }
}
impl NodeEvents {
    /// Transfer this permit alongside buffers moved into another owner/queue.
    pub fn take_allocation(&mut self) -> Option<Allocation> {
        self.allocation.take()
    }
}

/// One peer's replication progress as this leader tracks it (§ diagnostics).
/// Meaningful only while this node leads; empty otherwise. `state` is 0=probe,
/// 1=replicate, 2=snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerProgress {
    pub node: u64,
    pub matched: u64,
    pub next_index: u64,
    /// The leader's pipeline to the member: [`PEER_PROBE`] (one message
    /// at a time, after a lost one or a rejection), [`PEER_REPLICATE`]
    /// (streaming) or [`PEER_SNAPSHOT`] (being sent a snapshot).
    pub state: u8,
    pub recent_active: bool,
    pub paused: bool,
    pub pending_snapshot: u64,
}
/// The bounds a member's core holds its queues to, derived from what focal states of the member
/// (`core_state::limits`, hyper-raft `Limits::derive`): what an operator reads to see what a group
/// may hold, and what a memory budget is weighed against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoreLimits {
    /// Messages that wait to be taken.
    pub pending_messages: usize,
    /// Entries not yet durable.
    pub unstable_entries: usize,
    /// Entries one message carries.
    pub entries_per_message: usize,
    /// Entries a member holds approved by itself (the fast track).
    pub proposals: usize,
    /// Reads that wait for their quorum or to be taken.
    pub pending_reads: usize,
}

impl CoreLimits {
    fn of(limits: &Limits) -> Self {
        Self {
            pending_messages: limits.pending_messages,
            unstable_entries: limits.unstable_entries,
            entries_per_message: limits.entries_per_message,
            proposals: limits.proposals,
            pending_reads: limits.pending_reads,
        }
    }
}
pub const PEER_PROBE: u8 = 0;
pub const PEER_REPLICATE: u8 = 1;
pub const PEER_SNAPSHOT: u8 = 2;
#[derive(Clone, Debug)]
pub struct NodeStatus {
    pub node_id: u64,
    pub leader_id: u64,
    pub term: u64,
    pub committed_index: u64,
    pub applied_index: u64,
    pub role: StateRole,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
}
/// The scalars of a [`NodeStatus`] without its membership (the audit's
/// F53): what a check of the term, the role, the leader or an index reads,
/// copied, so a scalar check allocates nothing. The owned status stays for
/// what is published.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeScalars {
    pub node_id: u64,
    pub leader_id: u64,
    pub term: u64,
    pub committed_index: u64,
    pub applied_index: u64,
    pub role: StateRole,
}
/// The node's membership as it holds it, borrowed: a check of who is a
/// voter or a learner reads it in place.
#[derive(Clone, Copy, Debug)]
pub struct MembershipView<'a> {
    pub voters: &'a [u64],
    pub learners: &'a [u64],
}
impl MembershipView<'_> {
    /// Whether `node` is a voter or a learner.
    pub fn holds(&self, node: u64) -> bool {
        self.voters.contains(&node) || self.learners.contains(&node)
    }
    /// The voters and the learners together.
    pub fn len(&self) -> usize {
        self.voters.len().saturating_add(self.learners.len())
    }
    pub fn is_empty(&self) -> bool {
        self.voters.is_empty() && self.learners.is_empty()
    }
}

/// One member of one Raft group, as focal's owners drive it: the replica, its log and what it
/// keeps durable, over the backend it opened on ([27] §15.7, option (B)). Every method is the
/// backend's own; the owners' API is one whichever answers.
///
/// [27]: ../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
pub struct DurableNode {
    backend: Backend,
}

/// The point a checkpoint names: an applied entry's index and term, and the
/// configuration applied through it. Captured by the owner with the state it
/// encodes (`DurableNode::checkpoint_point`) and handed back once that state
/// is durable (`DurableNode::begin_checkpoint_from`), however far the replica
/// went on meanwhile.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointPoint {
    pub index: u64,
    pub term: u64,
    configuration: ConfState,
}

/// What a [`DurableNode`] runs over.
#[expect(
    clippy::large_enum_variant,
    reason = "focal-log's member stays where its owner keeps it, as before the shell: a box would \
              be an allocation every group's allowance counts; the shell's member is boxed and holds \
              its box's bytes under its group's budget"
)]
enum Backend {
    /// focal-log and the core driven by focal's own persistence (`LogNode`).
    Log(LogNode),
    /// hyper-durable's shell over the node's hyper-log log (`ShellNode`), boxed.
    Shell(Box<shell_node::ShellNode>),
}

/// Calls the backend's method of the same name.
macro_rules! dispatch {
    ($node:ident = $backend:expr => $call:expr) => {
        match $backend {
            Backend::Log($node) => $call,
            Backend::Shell($node) => $call,
        }
    };
}
use dispatch;

#[cfg(test)]
impl DurableNode {
    /// The focal-log backend, for the crate's tests that read or drive its own state.
    pub(crate) fn log(&self) -> &LogNode {
        match &self.backend {
            Backend::Log(node) => node,
            Backend::Shell(_) => panic!("a member on the shell has no focal-log backend"),
        }
    }
    /// As [`DurableNode::log`], to change.
    pub(crate) fn log_mut(&mut self) -> &mut LogNode {
        match &mut self.backend {
            Backend::Log(node) => node,
            Backend::Shell(_) => panic!("a member on the shell has no focal-log backend"),
        }
    }
}

/// The focal-log backend of a [`DurableNode`]: the core over focal-log's write-ahead log, its
/// persistence and checkpoints, decoder records and wakes its own.
pub(crate) struct LogNode {
    config: NodeConfig,
    raw: RawNode<RamLog>,
    wal: WalLease,
    failed: bool,
    recovered_snapshot: Option<AppliedSnapshot>,
    recovered_events: Option<NodeEvents>,
    delivered_index: u64,
    budget: MemoryBudget,
    raw_allocation: Option<Allocation>,
    recovered_allocation: Option<Allocation>,
    persistence: Option<persistence::PendingDrain>,
    checkpoint: Option<Box<checkpoint::PendingCheckpoint>>,
    decoders: decoder::DecoderGate,
    decoder_write: Option<decoder::PendingDecoderFloor>,
    // A decoder-gated recovery cannot drain at open (the unconfirmed decoder
    // makes `check` refuse), so the committed conf-change replay that rebuilds
    // membership is deferred until the decoder is confirmed. While it is pending,
    // no election or network step may observe the stale snapshot-only voter set.
    membership_rebuild_pending: bool,
    /// The stored hard state names a commit the log does not hold yet: the
    /// commit moved and no write was made for it (`persistence`). The
    /// group's next record carries it.
    commit_unwritten: bool,
    /// The write of such a commit, behind what was released for it: one in
    /// flight at a time, waited for by no one.
    commit_write: Option<(focal_log::WalAppend, u64)>,
    /// The commit the log holds: what a restart replays to. A change of
    /// membership past it is not applied before a write has stated its
    /// commit (`persistence`).
    commit_durable: u64,
    /// The group applies nothing on a commit its log does not hold
    /// (`apply_on_written_commit`).
    written_commit: bool,
    /// The unwritten commit as the owner's last period found it: one that
    /// is the same a period later is written (`settle_commit`).
    commit_waiting: Option<u64>,
    /// What tells this group's owner that a write of the group was answered
    /// (`notify_persisted`): made anew for every write.
    persisted: Option<PersistedSignal>,
    /// The election priority the owner configured; see `set_priority`.
    priority: i64,
    /// Which fields beyond raft-rs's this member's peers carry (`Wire`): what it reads of their
    /// messages and whether it keeps what arrives ahead of a hole.
    wire: Wire,
    // Drop after any pending Ready/output payloads, including owner cancellation.
    active_allocation: Option<Allocation>,
}

impl LogNode {
    pub fn group_id(&self) -> [u8; 16] {
        self.config.group_id
    }
    pub fn cluster_id(&self) -> [u8; 16] {
        self.config.cluster_id
    }
    pub fn open(config: NodeConfig, data_dir: impl AsRef<Path>) -> Result<Self, ConsensusError> {
        let options = WalOptions::new(WalIdentity {
            cluster: config.cluster_id,
            node: config.node_id,
            stream: 0,
        });
        let wal = SharedWal::open(data_dir, options)?;
        Self::open_on_wal(config, wal)
    }

    pub fn open_in(
        config: NodeConfig,
        data_dir: impl AsRef<Path>,
        parent: &MemoryBudget,
    ) -> Result<Self, ConsensusError> {
        let options = WalOptions::new(WalIdentity {
            cluster: config.cluster_id,
            node: config.node_id,
            stream: 0,
        });
        let wal = SharedWal::open_with_budget(
            data_dir,
            options,
            focal_log::WalWriterLimits::default(),
            parent.clone(),
        )?;
        Self::open_on_wal_in(config, wal, parent)
    }

    /// `restore_on_wal_in` over a fresh physical WAL at `data_dir`.
    pub fn restore_in(
        config: NodeConfig,
        data_dir: impl AsRef<Path>,
        parent: &MemoryBudget,
        image: RestoredLog,
    ) -> Result<Self, ConsensusError> {
        let options = WalOptions::new(WalIdentity {
            cluster: config.cluster_id,
            node: config.node_id,
            stream: 0,
        });
        let wal = SharedWal::open_with_budget(
            data_dir,
            options,
            focal_log::WalWriterLimits::default(),
            parent.clone(),
        )?;
        Self::restore_on_wal_in(config, wal, parent, image)
    }
    /// Begin a logical group's log from a restored image (26 §6): on an
    /// empty logical log, write the group identity, the decoder floor and
    /// transition the image's log promised, the snapshot at the image's
    /// index and term under the bootstrap membership, and a hard state that
    /// commits it; then open the group exactly as a restart would, so the
    /// snapshot is delivered to the application as a recovered one. A log
    /// that already holds any other record is refused: a restore never
    /// overwrites history. One that holds this image and nothing else is
    /// opened: a restore that is issued again goes on where it was cut.
    pub fn restore_on_wal_in(
        config: NodeConfig,
        shared: SharedWal,
        parent_budget: &MemoryBudget,
        image: RestoredLog,
    ) -> Result<Self, ConsensusError> {
        config.validate()?;
        if image.index == 0
            || image.term == 0
            || image.data.is_empty()
            || image.data.len() > IMAGE_BYTES
            || image
                .transition
                .is_some_and(|(predecessor, successor)| predecessor == successor)
            || image
                .transition
                .is_some_and(|(predecessor, _)| predecessor != image.floor)
        {
            return Err(ConsensusError::Configuration("invalid restore image"));
        }
        let identity = shared.identity()?;
        if identity.cluster != config.cluster_id || identity.node != config.node_id {
            return Err(ConsensusError::Configuration(
                "physical WAL node/cluster mismatch",
            ));
        }
        {
            let mut wal = shared.lease(LogicalLogId(config.group_id))?;
            let conf = ConfState {
                voters: config.voters.clone(),
                learners: config.learners.clone(),
                ..ConfState::default()
            };
            validate_conf_state(&conf)?;
            let snapshot = Snapshot {
                data: image.data,
                metadata: Some(SnapshotMetadata {
                    conf_state: Some(conf),
                    index: image.index,
                    term: image.term,
                }),
            };
            let hard = HardState {
                term: image.term,
                commit: image.index,
                ..HardState::default()
            };
            let mut records = Vec::new();
            records
                .try_reserve_exact(6)
                .map_err(|_| ConsensusError::Capacity)?;
            records.push(identity_record(&config)?);
            if config.fast {
                records.push(fast_track_record(&config));
            }
            records.push(decoder::floor_record(config.group_id, image.floor)?);
            if let Some((predecessor, successor)) = image.transition {
                records.push(decoder::transition_record(
                    config.group_id,
                    decoder::DecoderPair {
                        predecessor,
                        successor,
                    },
                )?);
            }
            records.push(proto_record(
                config.group_id,
                RecordKind::Snapshot,
                image.index,
                image.term,
                &snapshot,
            )?);
            records.push(proto_record(
                config.group_id,
                RecordKind::HardState,
                image.index,
                image.term,
                &hard,
            )?);
            // A log that holds this image and nothing else is this
            // restore, begun before and cut before its copy was recorded:
            // the image is written by one append, which a log holds whole
            // or not at all, so it is opened as it is. Any other record is
            // history.
            let mut held = 0_usize;
            let mut same = true;
            wal.replay(|record| {
                same = same && records.get(held) == Some(&record);
                held = held.saturating_add(1);
                Ok(())
            })?;
            if held != 0 && !(same && held == records.len()) {
                return Err(ConsensusError::Configuration(
                    "restore into a populated log",
                ));
            }
            if held == 0 {
                wal.validate_append(&records)?;
                wal.append_in(&records, BudgetLane::Completion)?;
            }
        }
        Self::open_on_wal_in(config, shared, parent_budget)
    }

    /// Host many logical groups on the same node-owned physical WAL. Each group
    /// keeps independent Raft authority while sharing disk flush/segment custody.
    pub fn open_on_wal(config: NodeConfig, shared: SharedWal) -> Result<Self, ConsensusError> {
        let budget = MemoryBudget::new(512 * 1024 * 1024, 128 * 1024 * 1024)
            .map_err(|_| ConsensusError::Capacity)?;
        Self::open_on_wal_in(config, shared, &budget)
    }

    /// What opening a group under `config` charges before its first drain
    /// prices what it holds (`memory::initial_bytes`).
    pub fn initial_estimate(config: &NodeConfig) -> Result<usize, ConsensusError> {
        memory::initial_bytes(config)
    }
    /// Charge this group's retained Raft data and operation staging to a tenant
    /// or node hierarchy. The physical shared WAL has its own node-wide budget.
    pub fn open_on_wal_in(
        config: NodeConfig,
        shared: SharedWal,
        parent_budget: &MemoryBudget,
    ) -> Result<Self, ConsensusError> {
        config.validate()?;
        let budget = parent_budget
            .child(512 * 1024 * 1024, 128 * 1024 * 1024)
            .map_err(|_| ConsensusError::Capacity)?;
        let initial = memory::reserve(
            &budget,
            BudgetKind::Control,
            BudgetLane::Completion,
            memory::initial_bytes(&config)?,
        )?;
        let identity = shared.identity()?;
        if identity.cluster != config.cluster_id || identity.node != config.node_id {
            return Err(ConsensusError::Configuration(
                "physical WAL node/cluster mismatch",
            ));
        }
        let mut wal = shared.lease(LogicalLogId(config.group_id))?;
        let mut storage = RamLog::new(&config, budget.clone())?;
        let mut persisted_config = None;
        let mut persisted_fast = false;
        let mut required_decoder = None;
        let mut decoder_transition = None;
        let mut replay_error = None;
        wal.replay(|record| {
            if replay_error.is_none() {
                if record.log != LogicalLogId(config.group_id) {
                    replay_error = Some(ConsensusError::Configuration(
                        "stream belongs to another group",
                    ));
                } else {
                    let replay = (|| {
                        let bytes = memory::replay_scratch(&record)?;
                        let _decode = memory::reserve(
                            &budget,
                            BudgetKind::Recovery,
                            BudgetLane::Completion,
                            bytes,
                        )?;
                        replay_record(
                            &mut storage,
                            &mut persisted_config,
                            &mut persisted_fast,
                            &mut required_decoder,
                            &mut decoder_transition,
                            record,
                        )
                    })();
                    if let Err(error) = replay {
                        replay_error = Some(error);
                    }
                }
            }
            Ok(())
        })?;
        if let Some(error) = replay_error {
            return Err(error);
        }
        if let Some(persisted) = persisted_config {
            if !persisted.same_identity(&config) || persisted_fast != config.fast {
                return Err(ConsensusError::Configuration(
                    "persisted identity/bootstrap configuration mismatch",
                ));
            }
        } else {
            if storage.last_index()? > 0
                || storage.hard_state != HardState::default()
                || required_decoder.is_some()
            {
                return Err(ConsensusError::Corruption("missing group identity"));
            }
            if persisted_fast {
                return Err(ConsensusError::Corruption("missing group identity"));
            }
            let mut records = Vec::new();
            records
                .try_reserve_exact(2)
                .map_err(|_| ConsensusError::Capacity)?;
            records.push(identity_record(&config)?);
            if config.fast {
                records.push(fast_track_record(&config));
            }
            wal.append_in(&records, BudgetLane::Completion)?;
        }
        storage.validate()?;
        let applied = proto::snapshot_index(&storage.snapshot);
        // The recovered snapshot is an event awaiting the caller's first
        // drain: charged as every delivered event is, so that the drain
        // hands it over under the one charge it carries.
        let recovered_allocation = if proto::snapshot_is_empty(&storage.snapshot) {
            None
        } else {
            Some(memory::reserve(
                &budget,
                BudgetKind::Pending,
                BudgetLane::Completion,
                memory::snapshot_bytes(&storage.snapshot)?,
            )?)
        };
        let recovered_snapshot = (!proto::snapshot_is_empty(&storage.snapshot))
            .then(|| snapshot_event(&storage.snapshot));
        let raft_config = core_state::raft_config(&config, applied)?;
        // Snapshot membership can differ from bootstrap membership on recovery.
        let recovered_members = storage
            .conf_state
            .voters
            .len()
            .saturating_add(storage.conf_state.voters_outgoing.len())
            .saturating_add(storage.conf_state.learners.len())
            .saturating_add(storage.conf_state.learners_next.len());
        let _membership = memory::reserve(
            &budget,
            BudgetKind::Recovery,
            BudgetLane::Completion,
            recovered_members
                .checked_mul(4096)
                .ok_or(ConsensusError::Capacity)?,
        )?;
        let commit_durable = storage.hard_state.commit;
        let raw = catch_unwind(AssertUnwindSafe(|| RawNode::new(&raft_config, storage)))
            .map_err(|_| ConsensusError::DependencyFailure)??;
        let mut node = Self {
            config,
            raw,
            wal,
            failed: false,
            recovered_snapshot,
            recovered_events: None,
            delivered_index: applied,
            budget,
            raw_allocation: Some(initial),
            active_allocation: None,
            recovered_allocation,
            persistence: None,
            checkpoint: None,
            decoders: decoder::DecoderGate::new(required_decoder, decoder_transition),
            decoder_write: None,
            // The complement of the constructor rebuild below: a gated recovery
            // (required_decoder set) cannot drain yet, so its rebuild is deferred
            // to decoder confirmation and fenced until then.
            membership_rebuild_pending: required_decoder.is_some(),
            commit_unwritten: false,
            commit_write: None,
            commit_durable,
            written_commit: false,
            commit_waiting: None,
            persisted: None,
            priority: 0,
            wire: Wire::Frozen,
        };
        // Rebuild committed membership before elections or network messages can
        // run. Application replay is retained for the caller's first drain.
        if node.decoders.required.is_none() {
            let events = node.drain()?;
            node.recovered_events = Some(events);
        }
        Ok(node)
    }

    pub fn campaign(&mut self) -> Result<(), ConsensusError> {
        self.guarded(|replica| replica.campaign_inner())
    }
    /// What one transition of this node may stage at most, as its guard
    /// reserves it (`memory::staging_bytes` for an operation that brings
    /// nothing): the bound a heartbeat, a read barrier or a report is held
    /// to, whatever the history.
    pub fn staging_estimate(&self) -> Result<usize, ConsensusError> {
        memory::staging_bytes(&self.raw, &self.config, 0, 0)
    }
    /// The bound a proposal of `incoming` bytes is held to
    /// (`memory::staging_bytes` as its guard reserves it).
    pub fn staging_estimate_for(&self, incoming: usize) -> Result<usize, ConsensusError> {
        memory::staging_bytes(&self.raw, &self.config, incoming, 0)
    }
    pub fn is_budgeted_within(&self, parent: &MemoryBudget) -> bool {
        self.budget.is_within(parent)
    }
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.propose_in(data, BudgetLane::Ordinary)
    }
    /// Proposes by the fast track (27 §4), from a member that does not
    /// lead: the entry goes to every voter, for the index after what this
    /// member holds, and is committed when a fast quorum holds it or the
    /// leader's classic quorum does, whichever is first. The index it was
    /// proposed for. Success is admission, as of `propose`: the entry is
    /// committed when `drain` gives it, and is given in
    /// `NodeEvents::displaced` when another took its index.
    ///
    /// A leader proposes as it always did.
    pub fn propose_fast(&mut self, data: Vec<u8>) -> Result<u64, ConsensusError> {
        self.propose_fast_in(data, BudgetLane::Ordinary)
    }
    pub fn propose_fast_in(
        &mut self,
        data: Vec<u8>,
        lane: BudgetLane,
    ) -> Result<u64, ConsensusError> {
        self.check()?;
        if !self.config.fast {
            return Err(ConsensusError::Configuration("the group has no fast track"));
        }
        core_state::check_entry(&self.config, data.len())?;
        self.guarded_in(data.capacity(), 0, lane, |replica| {
            replica
                .raw
                .propose_fast(Vec::new(), data)
                .map_err(|error| core_state::proposal_refused(&replica.raw, error))
        })
    }
    /// Whether the group has the fast track.
    pub fn fast(&self) -> bool {
        self.config.fast
    }
    /// What the fast track did at this member since it opened.
    pub fn fast_stats(&self) -> hyper_raft::FastStats {
        self.raw.raft.fast_stats()
    }
    pub fn propose_in(&mut self, data: Vec<u8>, lane: BudgetLane) -> Result<(), ConsensusError> {
        self.guarded_in(data.capacity(), 0, lane, |replica| {
            replica.propose_inner(data)
        })
    }
    /// Submit a caller-retained record without consuming its retry buffer.
    /// The caller keeps that buffer funded until its own commit/rollback fence.
    /// Raft's independent copy and fanout are admitted before allocation; a
    /// capacity refusal leaves the original bytes available for an exact retry.
    /// Success is proposal admission, never an index assignment or commit proof.
    pub fn propose_borrowed_in(
        &mut self,
        data: &[u8],
        lane: BudgetLane,
    ) -> Result<(), ConsensusError> {
        self.check_leader()?;
        core_state::check_entry(&self.config, data.len())?;
        self.guarded_in(data.len(), 0, lane, |replica| {
            let mut owned = Vec::new();
            owned
                .try_reserve_exact(data.len())
                .map_err(|_| ConsensusError::Capacity)?;
            if owned.capacity() > data.len() {
                return Err(ConsensusError::Capacity);
            }
            owned.extend_from_slice(data);
            replica.propose_inner(owned)
        })
    }
    /// The authenticated envelope must bind cluster/group identity. Peer input is
    /// validated before Raft; unexpected dependency failures stop this replica.
    pub fn step(&mut self, message: Message) -> Result<(), ConsensusError> {
        let bytes = memory::message_bytes(&message)?;
        // A change carried in an entry makes no member's progress here: the
        // core appends it, and the members it adds are priced by the drain
        // that applies it. A snapshot restores its configuration as it is
        // stepped: the members it names that the core does not track yet.
        let added = {
            let conf = message
                .snapshot
                .as_deref()
                .map_or(&NO_CONF, |snapshot| conf_of(metadata_of(snapshot)));
            let tracker = self.raw.raft.tracker();
            conf.voters
                .iter()
                .chain(&conf.voters_outgoing)
                .chain(&conf.learners)
                .chain(&conf.learners_next)
                .filter(|member| tracker.get(**member).is_none())
                .count()
                .min(MAX_MEMBERS)
        };
        self.guarded_in(bytes, added, BudgetLane::Completion, |replica| {
            replica.step_inner(message)
        })
    }
    pub fn tick(&mut self) -> Result<(), ConsensusError> {
        self.guarded(|replica| replica.tick_inner())?;
        // The period that passed is the measure of a quiet group: a commit
        // no record has carried through it is written now.
        self.settle_commit().inspect_err(|_| self.failed = true)
    }
    /// Completion arrives in drain after a quorum read barrier. Publication must
    /// reach that index before serving the read; there is no clock lease.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ConsensusError> {
        self.guarded_in(context.capacity(), 0, BudgetLane::Completion, |replica| {
            replica.read_index_inner(context)
        })
    }
    pub fn propose_conf_change(&mut self, change: ConfChangeV2) -> Result<(), ConsensusError> {
        // Proposed, the change is an entry; the members it adds are priced
        // by the drain that applies it.
        self.guarded_in(change.encoded_len(), 0, BudgetLane::Completion, |replica| {
            replica.propose_conf_change_inner(change)
        })
    }
    /// This node's election priority (27 §5). A voter refuses its vote, and
    /// its pre-vote, to a candidate of lower priority unless that candidate's
    /// log is strictly longer than the voter's, so among equally current
    /// members the one of highest priority wins and a lower one that times
    /// out first does not take the group. Priority never outranks the log:
    /// the election restriction is unchanged, and a group whose highest
    /// priority member is gone still elects among the rest. It is policy the
    /// owner sets from committed placement, not part of the node's durable
    /// identity.
    ///
    /// It takes effect once this node has a term. A node still at term zero
    /// has no log to defend, and its refusal would bear no term a candidate
    /// could hear: a group's first election is decided by timeouts alone.
    ///
    /// A lower priority is voted for all the same when its log is more
    /// current than the voter's, by its last term and then its length
    /// (`hyper_raft::Precedence::Log`): a voter that refuses for priority
    /// could then have been elected itself, so priority never leaves a group
    /// that can elect without a leader.
    /// The fields beyond raft-rs's this member's peers carry, from now on (`Wire`): under
    /// `Wire::Kept` it reads a refusal's `kept` and `lost` and keeps what arrives ahead of a hole
    /// (R17); under `Wire::Frozen` it does neither. focal-node raises it once the upgrade fence
    /// opens `RAFT_KEPT_LEVEL`, before a member opened under that fence sends.
    pub fn set_raft_wire(&mut self, wire: Wire) -> Result<(), ConsensusError> {
        self.check()?;
        self.wire = wire;
        let ahead = match wire {
            Wire::Frozen => hyper_raft::Ahead::Refused,
            Wire::Kept => hyper_raft::Ahead::Kept,
        };
        self.raw.set_ahead(ahead);
        Ok(())
    }
    /// The fields beyond raft-rs's this member's peers carry (`set_raft_wire`): what its
    /// messages are encoded under (`encode_message_in`).
    pub fn wire(&self) -> Wire {
        self.wire
    }
    /// The bounds this member's core holds its queues to ([`CoreLimits`]).
    pub fn limits(&self) -> CoreLimits {
        CoreLimits::of(&self.raw.raft.config().limits)
    }
    pub fn set_priority(&mut self, priority: i64) -> Result<(), ConsensusError> {
        self.check()?;
        if priority < 0 {
            return Err(ConsensusError::Configuration(
                "election priority must not be negative",
            ));
        }
        self.priority = priority;
        self.raw.set_priority(priority);
        Ok(())
    }
    /// While this member leads, `peer` is sent no more than `bytes` of
    /// entries ahead of its answers: what its owner learned the path to it
    /// carries ([27 §11]). One entry that is larger is still sent, alone,
    /// and a bound of nothing is one byte. Never more than this group's
    /// budget can stage in one transition beside a page for every member:
    /// a window the budget cannot stage would refuse every answer of the
    /// member it was made for. False for a peer the configuration does not
    /// name. The bound is kept until it is said again; a member the
    /// configuration makes anew begins at one page (`page_bytes`).
    ///
    /// [27 §11]: ../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
    pub fn set_inflight_bytes(&mut self, peer: u64, bytes: u64) -> Result<bool, ConsensusError> {
        self.check()?;
        let page = self.page_bytes();
        // What one transition stages for its sends: a page for every
        // member, and the window of the one whose answer it may be
        // (`memory::staging_bytes`). The most one reservation may be under
        // this budget and every budget above it, less what the group holds
        // at rest and those pages, is the most a window can be and still
        // be staged.
        let members = u64::try_from(self.raw.raft.tracker().len()).unwrap_or(u64::MAX);
        let held = u64::try_from(memory::raw_bytes(&self.raw)?).unwrap_or(u64::MAX);
        let stageable = u64::try_from(self.budget.reservation_limit(BudgetLane::Completion))
            .unwrap_or(u64::MAX)
            .saturating_sub(held)
            .saturating_sub(page.saturating_mul(members.saturating_add(1)));
        Ok(self
            .raw
            .set_inflight_bytes(peer, bytes.min(stageable).max(1)))
    }
    /// The bytes of entries one message carries at most, and one entry at
    /// least: what a member is sent ahead of its answers until its owner
    /// says what the path to it carries.
    pub fn page_bytes(&self) -> u64 {
        self.raw.raft.config().max_size_per_msg
    }
    /// The bytes of entries in flight to `peer` and the bound on them, as
    /// this member leads; `None` for a peer the configuration does not name.
    pub fn inflight_bytes(&self, peer: u64) -> Option<(u64, u64)> {
        self.raw
            .raft
            .tracker()
            .get(peer)
            .map(|progress| (progress.inflights.bytes(), progress.inflights.byte_cap()))
    }
    /// Ticks without leader contact before this node campaigns.
    /// Ticks between a leader's heartbeats.
    pub fn heartbeat_tick(&self) -> usize {
        self.config.heartbeat_tick
    }
    /// A leader sends its heartbeats now; any other role does nothing. For
    /// an owner whose tick period is stretched (27 §3.1 P2): the election
    /// timeout follows the period, and the heartbeats keep the cadence the
    /// followers were configured to expect.
    pub fn beat(&mut self) -> Result<(), ConsensusError> {
        self.guarded(|replica| {
            replica.check()?;
            replica.raw.ping()?;
            Ok(())
        })
    }
    pub fn election_tick(&self) -> usize {
        self.config.election_tick
    }
    /// Whether a read asked here waits for a round of heartbeats that has
    /// not left: it leaves with the next drain, carrying every read asked
    /// by then (`hyper_raft::Raft::ask_reads`). An owner with more work
    /// already queued takes it first, so that reads queued together are
    /// confirmed by one round and not by one each.
    pub fn reads_unasked(&self) -> bool {
        self.raw.raft.reads_unasked()
    }
    /// The reads asked here that wait for a quorum to confirm them.
    pub fn reads_waiting(&self) -> usize {
        self.raw.raft.pending_read_count()
    }
    /// The reads this member may hold in flight: the core's own bound, which
    /// a follower's parked read barriers share (27 §5, follower reads).
    pub fn pending_reads(&self) -> usize {
        self.config.max_inflight_messages.saturating_add(1)
    }
    /// The messages this member lets one peer have in flight at once
    /// (`NodeConfig::max_inflight_messages`): what an owner admits of a
    /// peer's traffic beside its participants (F56).
    pub fn inflight_window(&self) -> usize {
        self.config.max_inflight_messages
    }
    /// The ticks this member waits beyond its election timeout before it
    /// campaigns (`hyper_raft::Raft::set_patience`): what its owner gives
    /// it for the stalls it has seen in itself.
    pub fn set_patience(&mut self, ticks: usize) -> Result<(), ConsensusError> {
        self.check()?;
        self.raw.raft.set_patience(ticks);
        Ok(())
    }
    /// The priority this node was given; in force once it has a term.
    pub fn priority(&self) -> i64 {
        self.priority
    }
    /// The priority votes are judged by now.
    pub fn effective_priority(&self) -> i64 {
        self.raw.raft.priority_in_force()
    }
    pub fn transfer_leader(&mut self, node: u64) -> Result<(), ConsensusError> {
        self.guarded(|replica| replica.transfer_leader_inner(node))
    }
    /// Deterministic election pacing for harnesses: the follower with the
    /// shortest timeout campaigns first once every lease has expired.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_randomized_election_timeout(&mut self, ticks: usize) -> Result<(), ConsensusError> {
        self.check()?;
        let floor = self.config.election_tick;
        let ceiling = floor.checked_mul(2).ok_or(ConsensusError::Capacity)?;
        if ticks < floor || ticks >= ceiling {
            return Err(ConsensusError::Configuration(
                "randomized election timeout must lie in [election_tick, 2 * election_tick)",
            ));
        }
        self.raw.raft.set_randomized_election_timeout(ticks)?;
        Ok(())
    }
    pub fn report_unreachable(&mut self, node: u64) -> Result<(), ConsensusError> {
        self.guarded(|replica| replica.report_unreachable_inner(node))
    }
    pub fn report_snapshot(
        &mut self,
        node: u64,
        status: SnapshotStatus,
    ) -> Result<(), ConsensusError> {
        self.guarded(|replica| replica.report_snapshot_inner(node, status))
    }
    /// Apply delayed transport feedback only to the snapshot still pending for
    /// this peer in this leadership term. Acceptance never proves installation.
    pub fn report_snapshot_at(
        &mut self,
        node: u64,
        term: u64,
        index: u64,
        status: SnapshotStatus,
    ) -> Result<(), ConsensusError> {
        self.check()?;
        if !core_state::snapshot_still_pending(&self.raw, node, term, index) {
            return Ok(());
        }
        self.report_snapshot(node, status)
    }
    /// Install the application's complete state at its delivered prefix, retaining
    /// the Raft suffix until the new WAL generation and fence are durable.
    pub fn checkpoint(&mut self, index: u64, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.begin_checkpoint(index, data)?;
        self.finish_checkpoint()
    }
    /// Term of a fully published durable prefix, independent of a newer
    /// election term. Snapshot-prefix consumers must not substitute status.term.
    pub fn published_term(&self, index: u64) -> Result<u64, ConsensusError> {
        self.check()?;
        if self.persistence_pending() {
            return Err(ConsensusError::PersistencePending);
        }
        if index == 0 || index > self.delivered_index {
            return Err(ConsensusError::CheckpointIndex);
        }
        self.raw.store().term(index).map_err(Into::into)
    }
    /// Whether the entry the member's current term began with is at or below `index`, an entry
    /// it has published: what an entry of a newer term proves of what a former leader proposed
    /// (focal-ledger's `settle`) holds only past it. In a fast group the new leader takes what its
    /// voters approved into its log under its own term, before that entry, so an entry of the term
    /// published is not enough (hyper-raft S-4); where the log cannot show the term-start entry
    /// the answer is no, and what waits on it waits. Without the fast track every entry of the term
    /// follows the one it began with.
    pub fn term_began_by(&self, index: u64) -> Result<bool, ConsensusError> {
        if self.published_term(index)? != self.raw.raft.term() {
            return Ok(false);
        }
        Ok(core_state::term_began_by(&self.raw, index))
    }
    fn campaign_inner(&mut self) -> Result<(), ConsensusError> {
        self.check()?;
        core_state::check_campaign(&self.raw)?;
        self.raw.campaign()?;
        Ok(())
    }
    fn propose_inner(&mut self, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.check_leader()?;
        core_state::check_entry(&self.config, data.len())?;
        self.raw
            .propose(Vec::new(), data)
            .map_err(|error| core_state::proposal_refused(&self.raw, error))
    }
    /// The authenticated transport envelope must bind cluster/group identity.
    fn step_inner(&mut self, message: Message) -> Result<(), ConsensusError> {
        self.check()?;
        core_state::check_message(&self.config, &message)?;
        self.raw.step(message)?;
        Ok(())
    }

    /// Decode through the bounded prost codec and bind the Raft sender to the
    /// authenticated transport principal before any state-machine transition.
    pub fn step_authenticated(
        &mut self,
        peer_node_id: u64,
        encoded: &[u8],
    ) -> Result<(), ConsensusError> {
        self.check()?;
        if self.persistence_pending() {
            return Err(ConsensusError::PersistencePending);
        }
        let scratch = decode_message_charge(encoded)?;
        let _decode = memory::reserve(
            &self.budget,
            BudgetKind::Pending,
            BudgetLane::Completion,
            scratch,
        )?;
        let message = decode_message_in(encoded, self.wire)?;
        if peer_node_id == 0 || message.from != peer_node_id {
            return Err(ConsensusError::MalformedMessage(
                "Raft sender does not match authenticated peer",
            ));
        }
        self.step(message)
    }
    fn tick_inner(&mut self) -> Result<(), ConsensusError> {
        self.check()?;
        self.raw.tick()?;
        Ok(())
    }
    /// Quorum ReadIndex completion arrives in drain. Publication must also reach
    /// its index before a linearizable read is served. No clock lease is involved.
    fn read_index_inner(&mut self, context: Vec<u8>) -> Result<(), ConsensusError> {
        self.check()?;
        let held = self.raw.raft.ready_read_count();
        core_state::check_read(&self.config, &self.raw, &context, held)?;
        self.raw.read_index(context)?;
        Ok(())
    }
    fn propose_conf_change_inner(&mut self, change: ConfChangeV2) -> Result<(), ConsensusError> {
        self.check()?;
        let conf = &self.raw.store().conf_state;
        core_state::check_conf_change(&self.config, &self.raw, conf, &change)?;
        self.raw
            .propose_conf_change(Vec::new(), &change)
            .map_err(|error| core_state::proposal_refused(&self.raw, error))
    }
    /// On the leader, hand leadership to another current voter. On a
    /// follower, only leadership for this node itself may be asked for: raft
    /// forwards the request to the leader it knows, which times the
    /// transferee out into a campaign; any other target is not a follower's
    /// to request.
    fn transfer_leader_inner(&mut self, node: u64) -> Result<(), ConsensusError> {
        self.check()?;
        let conf = &self.raw.store().conf_state;
        core_state::check_transfer(&self.config, &self.raw, conf, node)?;
        self.raw.transfer_leader(node)?;
        Ok(())
    }
    fn report_unreachable_inner(&mut self, node: u64) -> Result<(), ConsensusError> {
        self.check()?;
        self.raw.report_unreachable(node)?;
        Ok(())
    }
    fn report_snapshot_inner(
        &mut self,
        node: u64,
        status: SnapshotStatus,
    ) -> Result<(), ConsensusError> {
        self.check()?;
        self.raw.report_snapshot(node, status)?;
        Ok(())
    }
    /// Immutable bootstrap membership validated against the durable identity
    /// record on every open. This is independent of the current configuration.
    pub fn bootstrap_membership(&self) -> (&[u64], &[u64]) {
        (&self.config.voters, &self.config.learners)
    }
    /// Free bytes on the filesystem holding this replica's WAL, sampled now.
    pub fn disk_available_bytes(&self) -> Result<u64, ConsensusError> {
        Ok(self.wal.available_bytes()?)
    }
    /// The volume envelope this group's WAL promises its writes from; a
    /// session's checkpoint seeds share it (25 §5).
    pub fn disk_budget(&self) -> DiskBudget {
        self.wal.disk_budget()
    }
    pub fn status(&self) -> NodeStatus {
        let conf = &self.raw.store().conf_state;
        core_state::status(&self.raw, self.config.node_id, self.delivered_index, conf)
    }
    /// The status's scalars, copied: a check that needs no membership
    /// allocates nothing (the audit's F53).
    pub fn scalars(&self) -> NodeScalars {
        core_state::scalars(&self.raw, self.config.node_id, self.delivered_index)
    }
    /// The membership as this node holds it, borrowed.
    pub fn membership(&self) -> MembershipView<'_> {
        let conf = &self.raw.store().conf_state;
        MembershipView {
            voters: &conf.voters,
            learners: &conf.learners,
        }
    }
    /// Per-peer replication progress this node tracks as leader (empty when not
    /// leading). Diagnostic only; it reflects in-memory Raft progress and is
    /// never persisted or replicated.
    pub fn peer_progress(&self) -> Vec<PeerProgress> {
        core_state::peer_progress(&self.raw, self.config.node_id)
    }

    /// What this leader tracks of one member's replication; nothing when
    /// it does not lead or tracks no such member. Allocates nothing, so an
    /// owner may ask on every tick.
    pub fn peer(&self, node: u64) -> Option<PeerProgress> {
        core_state::peer(&self.raw, self.config.node_id, node)
    }
    /// The member this leader is handing leadership to, while it is.
    pub fn transferring(&self) -> Option<u64> {
        self.raw.raft.lead_transferee()
    }
    /// A configuration change is in the log and not applied yet.
    pub fn configuration_pending(&self) -> bool {
        self.raw.raft.pending_conf_index() > self.delivered_index
    }

    /// The index of the stored snapshot the log is compacted behind (zero
    /// while the log is complete): a member added after it can only be
    /// seeded by a later snapshot, since Raft discards one whose
    /// configuration does not name the recipient.
    pub fn snapshot_index(&self) -> u64 {
        self.raw.store().snapshot_index()
    }
    /// The index of the last entry this node's log holds, durable or not:
    /// an append that names an entry past it is refused.
    pub fn last_index(&self) -> Result<u64, ConsensusError> {
        Ok(self.raw.raft.log().last_index()?)
    }
    /// Whether the stored snapshot names every member of the configuration
    /// this node has applied. Raft discards a snapshot that does not name
    /// its recipient, so a member added after the log was compacted is
    /// seeded only by a later snapshot; a log complete from its first entry
    /// seeds anyone. A change that only promotes or removes leaves every
    /// member named.
    pub fn snapshot_names_every_member(&self) -> bool {
        let store = self.raw.store();
        if store.snapshot_index() == 0 {
            return true;
        }
        store
            .snapshot
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.conf_state.as_ref())
            .is_some_and(|stated| core_state::names_every_member(stated, &store.conf_state))
    }
    /// A leader cannot complete a quorum ReadIndex until it has committed an
    /// entry in its current term. Ingress uses this to defer readiness probes.
    pub fn has_committed_current_term(&self) -> bool {
        !self.failed
            && self.decoder_confirmed()
            && !self.membership_rebuild_pending
            && !self.persistence_pending()
            && core_state::committed_in_term(&self.raw)
    }

    fn apply_entries(
        &mut self,
        entries: Vec<Entry>,
        events: &mut NodeEvents,
        delivered_index: &mut u64,
    ) -> Result<(), ConsensusError> {
        for entry in entries {
            let index = entry.index;
            if index
                != delivered_index
                    .checked_add(1)
                    .ok_or(ConsensusError::Capacity)?
            {
                return Err(ConsensusError::Corruption(
                    "non-contiguous committed delivery",
                ));
            }
            match entry.entry_type {
                EntryType::EntryNormal => {
                    if !entry.data.is_empty() {
                        events.committed.push(CommittedEntry {
                            index: entry.index,
                            term: entry.term,
                            data: entry.data,
                        });
                    }
                }
                EntryType::EntryConfChange => {
                    let change = if entry.data.is_empty() {
                        ConfChange::default()
                    } else {
                        ConfChange::decode(&entry.data).map_err(EnvelopeError::from)?
                    };
                    let before = self.membership_configuration();
                    let conf = self.raw.apply_conf_change_v1(&change)?;
                    let after = MembershipConfiguration::from_conf(&conf);
                    self.raw.store_mut().conf_state = conf;
                    events.membership.push(AppliedMembership {
                        index: entry.index,
                        term: entry.term,
                        context: change.context,
                        before,
                        after,
                    });
                }
                EntryType::EntryConfChangeV2 => {
                    let change = if entry.data.is_empty() {
                        ConfChangeV2::default()
                    } else {
                        ConfChangeV2::decode(&entry.data).map_err(EnvelopeError::from)?
                    };
                    let before = self.membership_configuration();
                    let conf = self.raw.apply_conf_change(&change)?;
                    let after = MembershipConfiguration::from_conf(&conf);
                    self.raw.store_mut().conf_state = conf;
                    events.membership.push(AppliedMembership {
                        index: entry.index,
                        term: entry.term,
                        context: change.context,
                        before,
                        after,
                    });
                }
            }
            *delivered_index = index;
        }
        Ok(())
    }
    pub fn inject_fault_once(&mut self, point: FaultPoint) -> Result<(), ConsensusError> {
        self.wal.inject_fault_once(point);
        Ok(())
    }
    /// The core returns what it refuses and never unwinds; what it depends on
    /// (the codec, the allocator's collections) is not the core. No unwind
    /// may escape the replica boundary or release partly assembled events:
    /// this is the last fence, and nothing is known to reach it. The node
    /// becomes unusable on a dependency failure, and on an error of the core
    /// that says its state no longer adds up; reopening revalidates disk
    /// state. This boundary requires Rust's unwind profile and cannot contain
    /// abort/OOM.
    fn guarded<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, ConsensusError>,
    ) -> Result<T, ConsensusError> {
        self.guarded_in(0, 0, BudgetLane::Completion, operation)
    }
    fn guarded_in<T>(
        &mut self,
        incoming: usize,
        new_members: usize,
        lane: BudgetLane,
        operation: impl FnOnce(&mut Self) -> Result<T, ConsensusError>,
    ) -> Result<T, ConsensusError> {
        self.check()?;
        // Fence every Raft-participating operation (propose, step, campaign,
        // tick, conf change, transfer, reports) until the deferred committed
        // membership rebuild has run. `poll_drain` bypasses `guarded_in`, so the
        // rebuild that clears this flag is never blocked by it. `confirm_decoder`
        // clears it eagerly, so this is a retryable safety net, not the norm.
        if self.membership_rebuild_pending {
            return Err(ConsensusError::PersistencePending);
        }
        if self.persistence_pending() {
            return Err(ConsensusError::PersistencePending);
        }
        let bytes = memory::staging_bytes(&self.raw, &self.config, incoming, new_members)?;
        self.active_allocation = Some(memory::reserve(
            &self.budget,
            BudgetKind::Pending,
            lane,
            bytes,
        )?);
        let result = catch_unwind(AssertUnwindSafe(|| operation(self)));
        match result {
            Ok(result) => {
                if let Err(ConsensusError::Raft(error)) = &result
                    && error.is_fatal()
                {
                    // An operation stopped half way: what the core holds is
                    // not what it would hold had it not begun.
                    self.failed = true;
                    return result;
                }
                let retained = match memory::raw_bytes(&self.raw) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        self.failed = true;
                        return Err(error);
                    }
                };
                let mut allocation = self
                    .active_allocation
                    .take()
                    .ok_or(ConsensusError::Failed)?;
                if allocation.shrink_to(retained).is_err() {
                    self.active_allocation = Some(allocation);
                    self.failed = true;
                    return Err(ConsensusError::Capacity);
                }
                self.raw_allocation = Some(allocation);
                result
            }
            Err(_) => {
                // Keep the active reservation until node teardown; Raft may
                // retain partially mutated buffers after an upstream unwind.
                self.failed = true;
                Err(ConsensusError::DependencyFailure)
            }
        }
    }

    fn check(&self) -> Result<(), ConsensusError> {
        self.check_state()?;
        if !self.decoder_confirmed() {
            Err(ConsensusError::DecoderUnconfirmed)
        } else {
            Ok(())
        }
    }
    /// Whether this node stopped on a dependency or persistence failure and
    /// must be reopened. A refusal that changed nothing leaves it false, so a
    /// caller can tell "try again" from "this replica is gone".
    pub fn failed(&self) -> bool {
        self.failed
    }
    fn check_state(&self) -> Result<(), ConsensusError> {
        if self.failed {
            Err(ConsensusError::Failed)
        } else {
            core_state::check_core(&self.raw)
        }
    }
    fn check_leader(&self) -> Result<(), ConsensusError> {
        self.check()?;
        core_state::check_leader(&self.raw)
    }
}

fn identity_record(config: &NodeConfig) -> Result<Record, ConsensusError> {
    Ok(Record {
        log: LogicalLogId(config.group_id),
        kind: RecordKind::Identity,
        index: 0,
        term: 0,
        payload: postcard::to_stdvec(config)?,
    })
}
fn proto_record(
    group: [u8; 16],
    kind: RecordKind,
    index: u64,
    term: u64,
    value: &impl envelope::Enveloped,
) -> Result<Record, ConsensusError> {
    Ok(Record {
        log: LogicalLogId(group),
        kind,
        index,
        term,
        payload: value.envelope()?,
    })
}
/// What this replica's election timeouts are drawn from: the system's
/// randomness, and where there is none, the replica's own identity, which
/// no other member of its group shares.
fn election_seed(config: &NodeConfig) -> u64 {
    #[cfg(test)]
    if let Some(seed) = config.election_seed {
        return seed;
    }
    let mut bytes = [0u8; 8];
    if getrandom::fill(&mut bytes).is_ok() {
        return u64::from_le_bytes(bytes);
    }
    config
        .group_id
        .iter()
        .chain(&config.cluster_id)
        .fold(config.node_id, |seed, byte| {
            (seed ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}
fn snapshot_event(snapshot: &Snapshot) -> AppliedSnapshot {
    let metadata = metadata_of(snapshot);
    AppliedSnapshot {
        index: metadata.index,
        term: metadata.term,
        data: snapshot.data.to_vec(),
        configuration: MembershipConfiguration::from_conf(conf_of(metadata)),
    }
}
/// What a snapshot without metadata covers: nothing, raft-rs's reading of an absent field.
static NO_METADATA: SnapshotMetadata = SnapshotMetadata {
    conf_state: None,
    index: 0,
    term: 0,
};
/// The configuration metadata without one states: none.
static NO_CONF: ConfState = ConfState {
    voters: Vec::new(),
    learners: Vec::new(),
    voters_outgoing: Vec::new(),
    learners_next: Vec::new(),
    auto_leave: false,
};
/// What `snapshot` covers.
pub(crate) fn metadata_of(snapshot: &Snapshot) -> &SnapshotMetadata {
    snapshot.metadata.as_ref().unwrap_or(&NO_METADATA)
}
/// The configuration `metadata` states.
pub(crate) fn conf_of(metadata: &SnapshotMetadata) -> &ConfState {
    metadata.conf_state.as_ref().unwrap_or(&NO_CONF)
}

/// Says that the group has the fast track.
fn fast_track_record(config: &NodeConfig) -> Record {
    Record {
        log: LogicalLogId(config.group_id),
        kind: RecordKind::FastTrack,
        index: 0,
        term: 0,
        payload: b"FOCALFT1".to_vec(),
    }
}
fn replay_record(
    storage: &mut RamLog,
    config: &mut Option<NodeConfig>,
    fast: &mut bool,
    required_decoder: &mut Option<[u8; 32]>,
    decoder_transition: &mut Option<decoder::DecoderPair>,
    record: Record,
) -> Result<(), ConsensusError> {
    match record.kind {
        RecordKind::Identity => {
            if config.is_some() {
                return Err(ConsensusError::Corruption("duplicate group identity"));
            }
            *config = Some(postcard::from_bytes(&record.payload)?);
        }
        RecordKind::FastTrack => {
            if config.is_none() || *fast || record.payload != b"FOCALFT1" {
                return Err(ConsensusError::Corruption(
                    "duplicate, unbound or unknown fast track record",
                ));
            }
            *fast = true;
        }
        RecordKind::Proposal => {
            if !*fast {
                return Err(ConsensusError::Corruption(
                    "a proposal in a group that has no fast track",
                ));
            }
            let entry = envelope::decode_entry(&record.payload)?;
            if entry.index != record.index
                || entry.term != record.term
                || entry.entry_type != EntryType::EntryNormal
                || entry.data.is_empty()
            {
                return Err(ConsensusError::Corruption("proposal envelope mismatch"));
            }
            // What the log has reached since is set aside.
            if entry.index > storage.last_index()? {
                storage.hold_proposal(entry)?;
            }
        }
        RecordKind::DecoderFloor => {
            if config.is_none() || required_decoder.is_some() {
                return Err(ConsensusError::Corruption(
                    "duplicate or unbound decoder floor",
                ));
            }
            *required_decoder = Some(decoder::decode_floor(&record)?);
        }
        RecordKind::DecoderTransition => {
            if config.is_none() || decoder_transition.is_some() {
                return Err(ConsensusError::Corruption(
                    "duplicate or unbound decoder transition",
                ));
            }
            let pair = decoder::decode_transition(&record)?;
            if *required_decoder != Some(pair.predecessor) {
                return Err(ConsensusError::Corruption(
                    "decoder transition lacks its original floor",
                ));
            }
            *decoder_transition = Some(pair);
        }
        RecordKind::Entry => {
            let entry = envelope::decode_entry(&record.payload)?;
            if entry.index != record.index || entry.term != record.term {
                return Err(ConsensusError::Corruption("entry envelope mismatch"));
            }
            storage.append(&[entry])?;
        }
        RecordKind::HardState => {
            let hs = envelope::decode_hard_state(&record.payload)?;
            if hs.commit != record.index
                || hs.term != record.term
                || hs.commit < storage.hard_state.commit
                || hs.term < storage.hard_state.term
                || (hs.term == storage.hard_state.term
                    && storage.hard_state.vote != 0
                    && hs.vote != storage.hard_state.vote)
            {
                return Err(ConsensusError::Corruption(
                    "hard state regression or envelope mismatch",
                ));
            }
            storage.hard_state = hs;
            storage.validate()?;
        }
        RecordKind::Snapshot => {
            let snapshot = envelope::decode_snapshot(&record.payload)?;
            if metadata_of(&snapshot).index != record.index
                || metadata_of(&snapshot).term != record.term
            {
                return Err(ConsensusError::Corruption("snapshot envelope mismatch"));
            }
            storage.install_snapshot(snapshot)?;
        }
        _ => return Err(ConsensusError::Corruption("unexpected session Raft record")),
    }
    Ok(())
}

fn validate_conf_state(conf: &ConfState) -> Result<(), ConsensusError> {
    if conf
        .voters
        .len()
        .saturating_add(conf.voters_outgoing.len())
        .saturating_add(conf.learners.len())
        .saturating_add(conf.learners_next.len())
        > 2048
    {
        return Err(ConsensusError::Capacity);
    }
    let incoming: BTreeSet<_> = conf.voters.iter().copied().collect();
    let outgoing: BTreeSet<_> = conf.voters_outgoing.iter().copied().collect();
    let learners: BTreeSet<_> = conf.learners.iter().copied().collect();
    let next: BTreeSet<_> = conf.learners_next.iter().copied().collect();
    if incoming.is_empty()
        || incoming.len() != conf.voters.len()
        || outgoing.len() != conf.voters_outgoing.len()
        || learners.len() != conf.learners.len()
        || next.len() != conf.learners_next.len()
        || incoming
            .iter()
            .chain(&outgoing)
            .chain(&learners)
            .chain(&next)
            .any(|id| *id == 0)
        || incoming
            .len()
            .saturating_add(outgoing.len())
            .saturating_add(learners.len())
            .saturating_add(next.len())
            > 2048
        || !learners.is_disjoint(&incoming)
        || !learners.is_disjoint(&outgoing)
        || !next.is_subset(&outgoing)
        || !next.is_disjoint(&incoming)
        || !next.is_disjoint(&learners)
        || (outgoing.is_empty() && (conf.auto_leave || !next.is_empty()))
    {
        return Err(ConsensusError::Corruption(
            "invalid snapshot voting configuration",
        ));
    }
    Ok(())
}

/// Compute bounded decoding workspace without allocating a protobuf message.
/// The caller must reserve and retain this allowance before `decode_message`,
/// then authenticate its decoded sender before any application or Raft mutation.
/// This shares the exact structural preflight used by `step_authenticated`;
/// repeated entries and snapshot membership count independently of bulk bytes.
pub fn decode_message_charge(bytes: &[u8]) -> Result<usize, ConsensusError> {
    if bytes.len() > 9 * 1024 * 1024 {
        return Err(ConsensusError::Capacity);
    }
    memory::message_scratch(bytes)
}

/// Maximum admitted serialized peer message. QUIC ingress separately enforces
/// its negotiated stream/frame limits before allocating these bytes.
pub fn decode_message(bytes: &[u8]) -> Result<Message, ConsensusError> {
    decode_message_in(bytes, Wire::Frozen)
}

/// A peer's message read under `wire` ([`Wire`]): what the receiving group's member takes of it.
pub fn decode_message_in(bytes: &[u8], wire: Wire) -> Result<Message, ConsensusError> {
    if bytes.len() > 9 * 1024 * 1024 {
        return Err(ConsensusError::Capacity);
    }
    envelope::decode_message_in(bytes, wire).map_err(|error| match error {
        EnvelopeError::Memory => ConsensusError::Capacity,
        error => ConsensusError::MalformedMessage(error.reason()),
    })
}

#[cfg(test)]
mod borrowed_proposal_tests;
#[cfg(test)]
mod differential_tests;
#[cfg(test)]
mod envelope_tests;
#[cfg(test)]
mod fast_track_tests;
#[cfg(test)]
mod raft_safety_tests;
#[cfg(test)]
mod sim_election_tests;
#[cfg(test)]
mod sim_fast_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod wire_tests;

#[cfg(test)]
mod persistence_tests;
