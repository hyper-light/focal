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
//! publish that complete prefix before replying to clients. The implementation
//! follows <https://docs.rs/raft/0.7.0/raft/raw_node/struct.RawNode.html>.

mod membership;
mod memory;
pub use membership::*;
mod checkpoint;
mod decoder;
mod persistence;
mod storage;

use focal_log::{LogError, LogicalLogId, Record, RecordKind, WalIdentity, WalLease, WalOptions};
use focal_memory::{Allocation, BudgetKind, BudgetLane, DiskBudget, MemoryBudget};
use raft::{Config, RawNode, Storage};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
};
use storage::RamLog;
use thiserror::Error;

pub use focal_log::{FaultPoint, SharedWal};
pub use raft::eraftpb::{
    ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
    Entry, EntryType, HardState, Message, MessageType, Snapshot,
};
pub use raft::protocompat::{PbMessage, PbMessageExt};
pub use raft::{SnapshotStatus, StateRole};

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
            max_inflight_messages: 128,
        }
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
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("durable log: {0}")]
    Log(#[from] LogError),
    #[error("Raft: {0}")]
    Raft(#[from] raft::Error),
    #[error("Raft protocol codec: {0}")]
    Protobuf(#[from] raft::protocompat::PbError),
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
    #[error("invalid peer message: {0}")]
    MalformedMessage(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}
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
    pub snapshot: Option<AppliedSnapshot>,
    /// Includes Raft-internal entries; this is never a SessionSeq.
    pub applied_index: u64,
    allocation: Option<Allocation>,
}
impl NodeEvents {
    /// Transfer this permit alongside buffers moved into another owner/queue.
    pub fn take_allocation(&mut self) -> Option<Allocation> {
        self.allocation.take()
    }
}

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

pub struct DurableNode {
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
    required_decoder: Option<[u8; 32]>,
    confirmed_decoder: Option<[u8; 32]>,
    compiled_decoders: Option<decoder::DecoderPair>,
    decoder_transition: Option<decoder::DecoderPair>,
    decoder_write: Option<decoder::PendingDecoderFloor>,
    // A decoder-gated recovery cannot drain at open (the unconfirmed decoder
    // makes `check` refuse), so the committed conf-change replay that rebuilds
    // membership is deferred until the decoder is confirmed. While it is pending,
    // no election or network step may observe the stale snapshot-only voter set.
    membership_rebuild_pending: bool,
    // Drop after any pending Ready/output payloads, including owner cancellation.
    active_allocation: Option<Allocation>,
}

impl DurableNode {
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
    /// that already holds any record is refused: a restore never overwrites
    /// history.
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
            || image.data.len() > 8 * 1024 * 1024
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
            let mut populated = false;
            wal.replay(|_| {
                populated = true;
                Ok(())
            })?;
            if populated {
                return Err(ConsensusError::Configuration(
                    "restore into a populated log",
                ));
            }
            let conf = ConfState {
                voters: config.voters.clone(),
                learners: config.learners.clone(),
                ..ConfState::default()
            };
            validate_conf_state(&conf)?;
            let mut snapshot = Snapshot::default();
            snapshot.mut_metadata().index = image.index;
            snapshot.mut_metadata().term = image.term;
            snapshot.mut_metadata().set_conf_state(conf);
            snapshot.data = image.data;
            let hard = HardState {
                term: image.term,
                commit: image.index,
                ..HardState::default()
            };
            let mut records = Vec::new();
            records
                .try_reserve_exact(5)
                .map_err(|_| ConsensusError::Capacity)?;
            records.push(identity_record(&config)?);
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
            wal.validate_append(&records)?;
            wal.append_in(&records, BudgetLane::Completion)?;
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
            if persisted.node_id != config.node_id
                || persisted.cluster_id != config.cluster_id
                || persisted.group_id != config.group_id
                || persisted.voters != config.voters
                || persisted.learners != config.learners
            {
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
            wal.append_in(&[identity_record(&config)?], BudgetLane::Completion)?;
        }
        storage.validate()?;
        let applied = storage.snapshot.get_metadata().index;
        let recovered_allocation = if storage.snapshot.is_empty() {
            None
        } else {
            Some(memory::reserve(
                &budget,
                BudgetKind::Recovery,
                BudgetLane::Completion,
                memory::snapshot_bytes(&storage.snapshot)?,
            )?)
        };
        let recovered_snapshot =
            (!storage.snapshot.is_empty()).then(|| snapshot_event(&storage.snapshot));
        let raft_config = Config {
            id: config.node_id,
            election_tick: config.election_tick,
            heartbeat_tick: config.heartbeat_tick,
            applied,
            max_size_per_msg: (config.max_entry_bytes as u64).saturating_add(1024),
            max_inflight_msgs: config.max_inflight_messages,
            max_uncommitted_size: config.max_uncommitted_bytes,
            max_committed_size_per_ready: 16 * 1024 * 1024,
            check_quorum: true,
            pre_vote: true,
            ..Default::default()
        };
        raft_config.validate()?;
        let logger = slog::Logger::root(slog::Discard, slog::o!());
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
        let raw = catch_unwind(AssertUnwindSafe(|| {
            RawNode::new(&raft_config, storage, &logger)
        }))
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
            required_decoder,
            confirmed_decoder: None,
            compiled_decoders: None,
            decoder_transition,
            decoder_write: None,
            // The complement of the constructor rebuild below: a gated recovery
            // (required_decoder set) cannot drain yet, so its rebuild is deferred
            // to decoder confirmation and fenced until then.
            membership_rebuild_pending: required_decoder.is_some(),
        };
        // Rebuild committed membership before elections or network messages can
        // run. Application replay is retained for the caller's first drain.
        if node.required_decoder.is_none() {
            let events = node.drain()?;
            node.recovered_events = Some(events);
        }
        Ok(node)
    }

    pub fn campaign(&mut self) -> Result<(), ConsensusError> {
        self.guarded(|replica| replica.campaign_inner())
    }
    pub fn is_budgeted_within(&self, parent: &MemoryBudget) -> bool {
        self.budget.is_within(parent)
    }
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.propose_in(data, BudgetLane::Ordinary)
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
        if data.is_empty() || data.len() > self.config.max_entry_bytes {
            return Err(ConsensusError::Capacity);
        }
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
        let added = if message
            .entries
            .iter()
            .any(|e| e.get_entry_type() != EntryType::EntryNormal)
        {
            1024
        } else {
            message
                .get_snapshot()
                .get_metadata()
                .get_conf_state()
                .voters
                .len()
                .saturating_add(
                    message
                        .get_snapshot()
                        .get_metadata()
                        .get_conf_state()
                        .learners
                        .len(),
                )
        };
        self.guarded_in(bytes, added, BudgetLane::Completion, |replica| {
            replica.step_inner(message)
        })
    }
    pub fn tick(&mut self) -> Result<(), ConsensusError> {
        self.guarded(|replica| replica.tick_inner())
    }
    /// Completion arrives in drain after a quorum read barrier. Publication must
    /// reach that index before serving the read; there is no clock lease.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ConsensusError> {
        self.guarded_in(context.capacity(), 0, BudgetLane::Completion, |replica| {
            replica.read_index_inner(context)
        })
    }
    pub fn propose_conf_change(&mut self, change: ConfChangeV2) -> Result<(), ConsensusError> {
        self.guarded_in(
            change.compute_size() as usize,
            change.changes.len(),
            BudgetLane::Completion,
            |replica| replica.propose_conf_change_inner(change),
        )
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
        self.raw.raft.set_randomized_election_timeout(ticks);
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
        if index == 0
            || self.raw.raft.term != term
            || self
                .raw
                .raft
                .prs()
                .get(node)
                .is_none_or(|progress| progress.pending_snapshot != index)
        {
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
    fn campaign_inner(&mut self) -> Result<(), ConsensusError> {
        self.check()?;
        if !self.raw.raft.promotable() {
            return Err(ConsensusError::Configuration(
                "only an applied voter can campaign",
            ));
        }
        self.raw.campaign()?;
        Ok(())
    }
    fn propose_inner(&mut self, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.check_leader()?;
        if data.is_empty() || data.len() > self.config.max_entry_bytes {
            return Err(ConsensusError::Capacity);
        }
        self.raw.propose(Vec::new(), data)?;
        Ok(())
    }
    /// The authenticated transport envelope must bind cluster/group identity.
    fn step_inner(&mut self, message: Message) -> Result<(), ConsensusError> {
        self.check()?;
        if message.to != self.config.node_id || message.from == 0 {
            return Err(ConsensusError::Configuration(
                "wrong destination or missing sender",
            ));
        }
        if message.compute_size() as usize > 9 * 1024 * 1024
            || message
                .entries
                .iter()
                .any(|entry| entry.data.len() > self.config.max_entry_bytes)
            || message.get_snapshot().data.len() > 8 * 1024 * 1024
        {
            return Err(ConsensusError::Capacity);
        }
        if MessageType::from_i32(message.msg_type).is_none() {
            return Err(ConsensusError::MalformedMessage("unknown message type"));
        }
        if message.get_msg_type() == MessageType::MsgPropose {
            return Err(ConsensusError::MalformedMessage(
                "proposals must enter through the leader's application admission",
            ));
        }
        if [
            message.term,
            message.index,
            message.commit,
            message.log_term,
            message.commit_term,
            message.request_snapshot,
            message.reject_hint,
        ]
        .contains(&u64::MAX)
        {
            return Err(ConsensusError::Capacity);
        }
        if !message.get_snapshot().is_empty() {
            let metadata = message.get_snapshot().get_metadata();
            if metadata.index == u64::MAX || metadata.term == u64::MAX {
                return Err(ConsensusError::Capacity);
            }
            validate_conf_state(metadata.get_conf_state())?;
        }
        let mut expected_index = message
            .index
            .checked_add(1)
            .ok_or(ConsensusError::Capacity)?;
        for entry in &message.entries {
            if EntryType::from_i32(entry.entry_type).is_none() {
                return Err(ConsensusError::MalformedMessage("unknown entry type"));
            }
            if message.get_msg_type() == MessageType::MsgAppend {
                if entry.index != expected_index || entry.term > message.term {
                    return Err(ConsensusError::MalformedMessage(
                        "invalid appended log sequence",
                    ));
                }
                expected_index = expected_index
                    .checked_add(1)
                    .ok_or(ConsensusError::Capacity)?;
            }
            if entry.index == u64::MAX || entry.term == u64::MAX {
                return Err(ConsensusError::Capacity);
            }
            match entry.get_entry_type() {
                EntryType::EntryConfChange => {
                    decode_proto::<ConfChange>(&entry.data)?;
                }
                EntryType::EntryConfChangeV2 => {
                    decode_proto::<ConfChangeV2>(&entry.data)?;
                }
                EntryType::EntryNormal => {}
            }
        }
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
        let message = decode_message(encoded)?;
        if peer_node_id == 0 || message.from != peer_node_id {
            return Err(ConsensusError::MalformedMessage(
                "Raft sender does not match authenticated peer",
            ));
        }
        self.step(message)
    }
    fn tick_inner(&mut self) -> Result<(), ConsensusError> {
        self.check()?;
        self.raw.tick();
        Ok(())
    }
    /// Quorum ReadIndex completion arrives in drain. Publication must also reach
    /// its index before a linearizable read is served. No clock lease is involved.
    fn read_index_inner(&mut self, context: Vec<u8>) -> Result<(), ConsensusError> {
        self.check_leader()?;
        if context.is_empty() || context.len() > 1024 {
            return Err(ConsensusError::Capacity);
        }
        if self
            .raw
            .raft
            .pending_read_count()
            .saturating_add(self.raw.raft.ready_read_count())
            >= self.config.max_inflight_messages
        {
            return Err(ConsensusError::Capacity);
        }
        self.raw.read_index(context);
        Ok(())
    }
    fn propose_conf_change_inner(&mut self, change: ConfChangeV2) -> Result<(), ConsensusError> {
        self.check_leader()?;
        if change.compute_size() as usize > self.config.max_entry_bytes {
            return Err(ConsensusError::Capacity);
        }
        if self.raw.raft.has_pending_conf() {
            return Err(ConsensusError::Configuration(
                "a membership change is already in flight",
            ));
        }
        let joint = !self.raw.store().conf_state.voters_outgoing.is_empty();
        if joint != change.changes.is_empty() {
            return Err(ConsensusError::Configuration(
                "joint membership must be entered and left in separate committed changes",
            ));
        }
        if change.changes.len() > 1024 {
            return Err(ConsensusError::Capacity);
        }
        for update in &change.changes {
            if update.node_id == 0 {
                return Err(ConsensusError::Configuration("zero member ID"));
            }
            if update.get_change_type() == ConfChangeType::AddNode {
                let progress = self
                    .raw
                    .raft
                    .prs()
                    .get(update.node_id)
                    .ok_or(ConsensusError::LearnerBehind)?;
                if progress.matched < self.raw.raft.raft_log.committed {
                    return Err(ConsensusError::LearnerBehind);
                }
            }
        }
        self.raw.propose_conf_change(Vec::new(), change)?;
        Ok(())
    }
    /// On the leader, hand leadership to another current voter. On a
    /// follower, only leadership for this node itself may be asked for: raft
    /// forwards the request to the leader it knows, which times the
    /// transferee out into a campaign; any other target is not a follower's
    /// to request.
    fn transfer_leader_inner(&mut self, node: u64) -> Result<(), ConsensusError> {
        self.check()?;
        let voter = self.raw.store().conf_state.voters.contains(&node);
        if self.raw.raft.state == StateRole::Leader {
            if !voter || node == self.config.node_id {
                return Err(ConsensusError::Configuration(
                    "transfer target must be another current voter",
                ));
            }
            self.raw.transfer_leader(node);
            return Ok(());
        }
        if node != self.config.node_id || !voter || self.raw.raft.leader_id == 0 {
            return Err(ConsensusError::NotLeader {
                leader: self.raw.raft.leader_id,
            });
        }
        self.raw.transfer_leader(node);
        Ok(())
    }
    fn report_unreachable_inner(&mut self, node: u64) -> Result<(), ConsensusError> {
        self.check()?;
        self.raw.report_unreachable(node);
        Ok(())
    }
    fn report_snapshot_inner(
        &mut self,
        node: u64,
        status: SnapshotStatus,
    ) -> Result<(), ConsensusError> {
        self.check()?;
        self.raw.report_snapshot(node, status);
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
        NodeStatus {
            node_id: self.config.node_id,
            leader_id: self.raw.raft.leader_id,
            term: self.raw.raft.term,
            committed_index: self.raw.raft.raft_log.committed,
            applied_index: self.delivered_index,
            role: self.raw.raft.state,
            voters: conf.voters.clone(),
            learners: conf.learners.clone(),
        }
    }

    /// The index of the stored snapshot the log is compacted behind (zero
    /// while the log is complete): a member added after it can only be
    /// seeded by a later snapshot, since Raft discards one whose
    /// configuration does not name the recipient.
    pub fn snapshot_index(&self) -> u64 {
        self.raw.store().snapshot_index()
    }
    /// A leader cannot complete a quorum ReadIndex until it has committed an
    /// entry in its current term. Ingress uses this to defer readiness probes.
    pub fn has_committed_current_term(&self) -> bool {
        !self.failed
            && self.decoder_confirmed()
            && !self.persistence_pending()
            && self
                .raw
                .store()
                .term(self.raw.raft.raft_log.committed)
                .is_ok_and(|term| term == self.raw.raft.term)
    }

    fn apply_entries(
        &mut self,
        entries: Vec<Entry>,
        events: &mut NodeEvents,
        delivered_index: &mut u64,
    ) -> Result<(), ConsensusError> {
        for entry in entries {
            if entry.index
                != delivered_index
                    .checked_add(1)
                    .ok_or(ConsensusError::Capacity)?
            {
                return Err(ConsensusError::Corruption(
                    "non-contiguous committed delivery",
                ));
            }
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    if !entry.data.is_empty() {
                        events.committed.push(CommittedEntry {
                            index: entry.index,
                            term: entry.term,
                            data: entry.data.to_vec(),
                        });
                    }
                }
                EntryType::EntryConfChange => {
                    let change = decode_proto::<ConfChange>(&entry.data)?;
                    let before = self.membership_configuration();
                    let conf = self.raw.apply_conf_change(&change)?;
                    let after = MembershipConfiguration::from_conf(&conf);
                    self.raw.mut_store().conf_state = conf;
                    events.membership.push(AppliedMembership {
                        index: entry.index,
                        term: entry.term,
                        context: change.context,
                        before,
                        after,
                    });
                }
                EntryType::EntryConfChangeV2 => {
                    let change = decode_proto::<ConfChangeV2>(&entry.data)?;
                    let before = self.membership_configuration();
                    let conf = self.raw.apply_conf_change(&change)?;
                    let after = MembershipConfiguration::from_conf(&conf);
                    self.raw.mut_store().conf_state = conf;
                    events.membership.push(AppliedMembership {
                        index: entry.index,
                        term: entry.term,
                        context: change.context,
                        before,
                        after,
                    });
                }
            }
            *delivered_index = entry.index;
        }
        Ok(())
    }
    pub fn inject_fault_once(&mut self, point: FaultPoint) {
        self.wal.inject_fault_once(point);
    }
    /// Upstream Raft uses assertions for invariant violations. No unwind may
    /// escape the replica boundary or release partly assembled events. The node
    /// becomes unusable on a dependency failure; reopening revalidates disk state.
    /// This boundary requires Rust's unwind profile and cannot contain abort/OOM.
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
    fn check_state(&self) -> Result<(), ConsensusError> {
        if self.failed {
            Err(ConsensusError::Failed)
        } else if self.raw.raft.term == u64::MAX || self.raw.store().last_index()? == u64::MAX {
            Err(ConsensusError::Capacity)
        } else {
            Ok(())
        }
    }
    fn check_leader(&self) -> Result<(), ConsensusError> {
        self.check()?;
        if self.raw.raft.state == StateRole::Leader {
            Ok(())
        } else {
            Err(ConsensusError::NotLeader {
                leader: self.raw.raft.leader_id,
            })
        }
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
    message: &impl PbMessage,
) -> Result<Record, ConsensusError> {
    Ok(Record {
        log: LogicalLogId(group),
        kind,
        index,
        term,
        payload: message.write_to_bytes()?,
    })
}
fn snapshot_event(snapshot: &Snapshot) -> AppliedSnapshot {
    AppliedSnapshot {
        index: snapshot.get_metadata().index,
        term: snapshot.get_metadata().term,
        data: snapshot.data.to_vec(),
        configuration: MembershipConfiguration::from_conf(snapshot.get_metadata().get_conf_state()),
    }
}

fn replay_record(
    storage: &mut RamLog,
    config: &mut Option<NodeConfig>,
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
            let entry = decode_proto::<Entry>(&record.payload)?;
            if entry.index != record.index || entry.term != record.term {
                return Err(ConsensusError::Corruption("entry envelope mismatch"));
            }
            storage.append(&[entry])?;
        }
        RecordKind::HardState => {
            let hs = decode_proto::<HardState>(&record.payload)?;
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
            let snapshot = decode_proto::<Snapshot>(&record.payload)?;
            if snapshot.get_metadata().index != record.index
                || snapshot.get_metadata().term != record.term
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
    if bytes.len() > 9 * 1024 * 1024 {
        return Err(ConsensusError::Capacity);
    }
    decode_proto(bytes)
}

fn decode_proto<T: PbMessage + Default>(bytes: &[u8]) -> Result<T, ConsensusError> {
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(ConsensusError::Capacity);
    }
    let mut message = T::default();
    // Upstream's native Prost compatibility layer calls prost::Message::merge;
    // its recursion limit covers unknown groups. No rust-protobuf parser or
    // compatibility runtime is linked into this dependency configuration.
    message.merge_from_bytes(bytes)?;
    Ok(message)
}

#[cfg(test)]
mod borrowed_proposal_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod persistence_tests;
