//! A member over hyper-durable's shell ([27] §15.7, option (B)): the replica owns the group's
//! log, one group of the node's hyper-log log, and its state machine, the hand-over machine,
//! which keeps what was committed for the owner's next drain. `DurableNode`'s methods answer here
//! as over focal-log, except where the shell's design differs, as §15.7 states:
//! - readies are taken ahead of their writes (R-4): no input is refused because a write is out,
//!   so `PersistencePending` does not arise here and `persistence_pending` is false;
//! - a leader's messages of its own term leave with the drive that made them, so nothing waits
//!   for `sendable`, and every other message leaves with the drain after its write is durable;
//! - a checkpoint and a decoder record are written to the group's own files, whole and durable
//!   before the call returns, so nothing waits for a later finish;
//! - the replica elects on ticks, and the owner's periods, counted by its ticks, are the shell's
//!   clock: the core takes no time from it, and the commit no write has stated is written one
//!   whole period after the applied index ran past it, as focal-log's `settle_commit` writes it.
//!
//! [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Wake, Waker};
use std::time::Duration;

use focal_platform::fs::FileMedium;
use hyper_block::file::DeviceFile;
use hyper_durable::{
    Budget, Cause, ClaimError, GroupStore, LogStore, OpenError, Output, Replica, ReplicaError,
    Settings, StateMachine,
};
use hyper_log::Log;
use hyper_raft::Elections;

use super::*;
use crate::decoder::{DecoderGate, DecoderPair, FloorWrite};
use crate::group_files::{GroupFileError, GroupRecords};
use crate::shell::{FloorStore, HandOver, Needs};

type Store = FloorStore<GroupStore<DeviceFile>>;
type ShellReplica = Replica<Store, HandOver<FileMedium>, FocalBudget>;

/// hyper-durable's budget over the group's `MemoryBudget`: what the replica reserves before an
/// input is reserved from the lane the owner's call names, and what it came to hold past that,
/// which cannot be refused, from the completion lane. What that lane cannot hold is owed:
/// counted, never reserved, at most one call's growth (the replica settles after every call),
/// and every reservation is refused until it is released.
pub(crate) struct FocalBudget {
    budget: MemoryBudget,
    ordinary: Option<Allocation>,
    completion: Option<Allocation>,
    owed: u64,
    /// The lane the next reservations are charged to.
    lane: BudgetLane,
}

impl FocalBudget {
    fn new(budget: MemoryBudget) -> Self {
        Self {
            budget,
            ordinary: None,
            completion: None,
            owed: 0,
            lane: BudgetLane::Completion,
        }
    }

    /// Reserves `bytes` from `lane` into what is held there.
    fn hold(&mut self, lane: BudgetLane, bytes: u64) -> bool {
        let Ok(bytes) = usize::try_from(bytes) else {
            return false;
        };
        let Ok(mut taken) = memory::reserve(&self.budget, BudgetKind::Pending, lane, bytes) else {
            return false;
        };
        let slot = match lane {
            BudgetLane::Ordinary => &mut self.ordinary,
            BudgetLane::Completion => &mut self.completion,
        };
        match slot {
            Some(held) => held.absorb(&mut taken).is_ok(),
            None => {
                *slot = Some(taken);
                true
            }
        }
    }
}

/// Gives back up to `bytes` of `held`: what it did not hold, still to give back.
fn give_back(held: &mut Option<Allocation>, bytes: usize) -> usize {
    let Some(allocation) = held.as_mut() else {
        return bytes;
    };
    let given = bytes.min(allocation.bytes());
    match allocation.shrink_to(allocation.bytes().saturating_sub(given)) {
        Ok(()) => bytes.saturating_sub(given),
        // A shrink to fewer bytes than are held never grows; kept, as not given back.
        Err(_) => bytes,
    }
}

impl Budget for FocalBudget {
    const BOUNDED: bool = true;

    fn reserve(&mut self, bytes: u64) -> bool {
        self.owed == 0 && self.hold(self.lane, bytes)
    }

    fn charge(&mut self, bytes: u64) {
        if !self.hold(BudgetLane::Completion, bytes) {
            self.owed = self.owed.saturating_add(bytes);
        }
    }

    fn release(&mut self, bytes: u64) {
        let repaid = bytes.min(self.owed);
        self.owed = self.owed.saturating_sub(repaid);
        let rest = usize::try_from(bytes.saturating_sub(repaid)).unwrap_or(usize::MAX);
        // The completion lane first: it is the allowance completing work draws on.
        let rest = give_back(&mut self.completion, rest);
        give_back(&mut self.ordinary, rest);
    }
}

/// What wakes the group's owner when the log answers a write that went out with it: the call the
/// owner's signal made, called once however often the write's waker is woken or cloned.
///
/// The log keeps a clone of the waker until it answers, so the waker is shared by the `Waker`
/// contract (`Clone + Send + Sync`). That count of clones, the `Arc` a std `Waker` is made from,
/// is this backend's one shared ownership (flagged for the owner's review: the alternative is the
/// same count kept by hand under `unsafe`, which focal allows only in its OS-interface file).
struct WakeOnce {
    /// The owner's call, made at most once.
    persisted: LazyLock<(), focal_log::Persisted>,
    /// A wake came: what went out with this waker woke the owner.
    fired: AtomicBool,
}

impl Wake for WakeOnce {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Release: the owner that reads it fired sees the wake as done.
        self.fired.store(true, Ordering::Release);
        LazyLock::force(&self.persisted);
    }
}

/// A member of one group over the shell (module docs).
pub(crate) struct ShellNode {
    config: NodeConfig,
    replica: ShellReplica,
    /// The buffers a drive fills, kept between drives.
    out: Output<()>,
    budget: MemoryBudget,
    /// The owner's periods since the member opened: the shell's clock (module docs).
    periods: u64,
    /// The commit no write has stated is due a write: a tick found the applied index past the
    /// durable commit, with nothing out, a whole period after it ran past.
    quiet_due: bool,
    /// The last drive left more to do without an answer from the log (`Driven::more`).
    more: bool,
    /// What tells the owner a write of the group was answered (`notify_persisted`).
    persisted: Option<PersistedSignal>,
    /// The waker the latest writes went out with, made from the owner's signal; none until a
    /// drive needs one.
    waker: Option<Arc<WakeOnce>>,
    decoders: DecoderGate,
    /// A decoder-gated member defers the replay that rebuilds its membership past open until its
    /// application confirms the decoder (`confirm_decoder`), as over focal-log.
    rebuild_pending: bool,
    /// What opening replayed and installed, kept for the owner's first drain.
    recovered: Option<NodeEvents>,
    priority: i64,
    failed: bool,
    /// The group's own directory: its records and its image.
    dir: PathBuf,
    disk: DiskBudget,
    /// What the node's own box takes, held under its group's budget for as long as it lives.
    _boxed: Allocation,
}

/// The configuration a group was founded with.
fn founding(config: &NodeConfig) -> ConfState {
    ConfState {
        voters: config.voters.clone(),
        learners: config.learners.clone(),
        ..ConfState::default()
    }
}

/// A group file's failure as the owners' error: a file that does not read whole is corruption,
/// and anything else is the medium's.
fn file_error(error: GroupFileError) -> ConsensusError {
    match error {
        GroupFileError::Corrupt { reason, .. } => ConsensusError::Corruption(reason),
        GroupFileError::Bound { .. } => ConsensusError::Corruption("a group file past its bound"),
        GroupFileError::Encoding(error) => ConsensusError::Encoding(error),
        GroupFileError::Io(error) => ConsensusError::Log(focal_log::LogError::Io(error)),
    }
}

/// Whether `events` give the owner nothing beyond the applied index `delivered` it holds.
fn nothing_in(events: &NodeEvents, delivered: u64) -> bool {
    events.messages.is_empty()
        && events.committed.is_empty()
        && events.membership.is_empty()
        && events.read_states.is_empty()
        && events.snapshot.is_none()
        && events.applied_index == delivered
}

impl ShellNode {
    /// Opens the member `config` names over the node's log `log`, its group's files under the
    /// data directory `root` (`group_files::group_dir`), its memory charged within
    /// `parent_budget` and its disk within `disk`. `needs` is the owner's word on which of its
    /// entries need a decoder, which the group's records must state before a write holding one
    /// goes out ([27] §15.5, O2).
    ///
    /// A group with the fast track is refused: its displaced proposals are not reported on the
    /// shell yet ([27] §15.7).
    pub(crate) fn open(
        config: NodeConfig,
        root: &Path,
        log: &Log<DeviceFile>,
        parent_budget: &MemoryBudget,
        disk: DiskBudget,
        needs: Needs,
    ) -> Result<Box<Self>, ConsensusError> {
        config.validate()?;
        if config.fast {
            return Err(ConsensusError::Configuration(
                "the fast track is not on the durable shell yet",
            ));
        }
        let budget = parent_budget
            .child(512 * 1024 * 1024, 128 * 1024 * 1024)
            .map_err(|_| ConsensusError::Capacity)?;
        // Held while the member opens: what the replica holds once open, it charges itself.
        let _opening = memory::reserve(
            &budget,
            BudgetKind::Control,
            BudgetLane::Completion,
            memory::initial_bytes(&config)?,
        )?;
        // The log cuts a write at its entries: an entry larger than a frame would fail its write
        // and fence the member (`LogError::TooLarge`), so a log that cannot hold the group's
        // largest entry, as the shell encodes it, is refused here.
        let largest = config
            .max_entry_bytes
            .saturating_add(hyper_durable::ENTRY_OVERHEAD);
        let room = log
            .entry_room()
            .map_err(|_| ConsensusError::Configuration("the node's log has no entry room"))?;
        if room < largest {
            return Err(ConsensusError::Configuration(
                "the node's log cannot hold the group's largest entry in one frame",
            ));
        }
        let group = u128::from_be_bytes(config.group_id);
        let store = match GroupStore::claim(log, group) {
            Ok(store) => store,
            Err(ClaimError::Damaged) => {
                return Err(ConsensusError::Corruption(
                    "the group's acknowledged records are damaged",
                ));
            }
            Err(ClaimError::Log(_)) => {
                return Err(ConsensusError::Configuration(
                    "the node's log refused the group",
                ));
            }
        };
        let mut medium = FileMedium;
        let dir = group_files::group_dir(root, config.group_id);
        let records = match group_files::read_records(&medium, &dir).map_err(file_error)? {
            Some(records) => {
                // The identity as focal-log checks it: the tunables may change between starts.
                if !records.identity.same_identity(&config) || records.fast {
                    return Err(ConsensusError::Configuration(
                        "persisted identity/bootstrap configuration mismatch",
                    ));
                }
                records
            }
            None => {
                // A group the log holds anything of was made with records: without them it is
                // not this group's start.
                let view = store
                    .view()
                    .map_err(|_| ConsensusError::Corruption("the group's log did not open"))?;
                if view.start.index > 0 || view.last > 0 || view.hard_state != HardState::default()
                {
                    return Err(ConsensusError::Corruption("missing group identity"));
                }
                let records = GroupRecords {
                    identity: config.clone(),
                    fast: false,
                    decoder_floor: None,
                    decoder_transition: None,
                };
                // Durable before anything of the group is in the log (27 §15.5, O1).
                let dir =
                    group_files::create(&mut medium, root, config.group_id).map_err(file_error)?;
                group_files::write_records(&mut medium, &dir, &records).map_err(file_error)?;
                records
            }
        };
        let transition = records
            .decoder_transition
            .map(|(predecessor, successor)| DecoderPair {
                predecessor,
                successor,
            });
        let decoders = DecoderGate::new(records.decoder_floor, transition);
        let machine = HandOver::open(medium, dir.clone(), false, IMAGE_BYTES, founding(&config))
            .map_err(file_error)?;
        let store = FloorStore::new(
            store,
            needs,
            records.decoder_floor,
            transition.map(|pair| pair.successor),
        );
        let settings = Settings {
            core: core_state::raft_config(&config, machine.durable().index)?,
            elections: Elections::Ticks,
            // One owner period on the shell's clock (module docs).
            quiet: Duration::from_nanos(1),
        };
        let replica = Replica::open(&settings, store, machine, FocalBudget::new(budget.clone()))
            .map_err(|error| match error {
                OpenError::Log(_) | OpenError::Storage(_) => {
                    ConsensusError::Corruption("the group's log did not open")
                }
                OpenError::Core(error) => ConsensusError::Raft(error),
                OpenError::Machine(_) => ConsensusError::Corruption("the group's image"),
                OpenError::StartPastMachine { .. } => {
                    ConsensusError::Corruption("the group's log starts past its image")
                }
            })?;
        let rebuild_pending = decoders.required.is_some();
        let boxed = memory::reserve(
            &budget,
            BudgetKind::Control,
            BudgetLane::Completion,
            size_of::<Self>().saturating_add(focal_memory::ALLOCATOR_OVERHEAD),
        )?;
        let mut node = Box::new(Self {
            config,
            replica,
            out: Output::default(),
            budget,
            periods: 0,
            quiet_due: false,
            more: false,
            persisted: None,
            waker: None,
            decoders,
            rebuild_pending,
            recovered: None,
            priority: 0,
            failed: false,
            dir,
            disk,
            _boxed: boxed,
        });
        // Rebuild committed membership before elections or network messages can run. The
        // replay is retained for the owner's first drain.
        if !node.rebuild_pending {
            node.replay()?;
        }
        Ok(node)
    }

    /// Applies what the log holds committed above the image, a page a drive, into the events the
    /// owner's first drain takes: a drive for every entry at most, since each applies one.
    fn replay(&mut self) -> Result<(), ConsensusError> {
        let mut events = NodeEvents::default();
        let committed = self.replica.core().raft.log().committed();
        let budget = committed.saturating_sub(self.replica.applied().index);
        self.drive_into(&mut events)?;
        for _ in 0..budget {
            if !self.more {
                break;
            }
            self.drive_into(&mut events)?;
        }
        self.recovered = Some(events);
        Ok(())
    }

    pub fn group_id(&self) -> [u8; 16] {
        self.config.group_id
    }
    pub fn cluster_id(&self) -> [u8; 16] {
        self.config.cluster_id
    }
    pub fn campaign(&mut self) -> Result<(), ConsensusError> {
        self.check_participating()?;
        core_state::check_campaign(self.replica.core())?;
        let campaigned = self.replica.campaign();
        self.heard(campaigned)
    }
    /// What the shell reserves before an input that brings nothing: none. What a transition
    /// grows past what it reserved is charged after it (`FocalBudget`).
    pub fn staging_estimate(&self) -> Result<usize, ConsensusError> {
        Ok(0)
    }
    /// What the shell reserves before a proposal of `incoming` bytes: those bytes.
    pub fn staging_estimate_for(&self, incoming: usize) -> Result<usize, ConsensusError> {
        Ok(incoming)
    }
    pub fn is_budgeted_within(&self, parent: &MemoryBudget) -> bool {
        self.budget.is_within(parent)
    }
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.propose_in(data, BudgetLane::Ordinary)
    }
    pub fn propose_fast(&mut self, data: Vec<u8>) -> Result<u64, ConsensusError> {
        self.propose_fast_in(data, BudgetLane::Ordinary)
    }
    pub fn propose_fast_in(
        &mut self,
        _data: Vec<u8>,
        _lane: BudgetLane,
    ) -> Result<u64, ConsensusError> {
        self.check()?;
        Err(ConsensusError::Configuration("the group has no fast track"))
    }
    pub fn fast(&self) -> bool {
        self.config.fast
    }
    pub fn fast_stats(&self) -> hyper_raft::FastStats {
        self.replica.core().raft.fast_stats()
    }
    pub fn propose_in(&mut self, data: Vec<u8>, lane: BudgetLane) -> Result<(), ConsensusError> {
        self.check_participating()?;
        core_state::check_leader(self.replica.core())?;
        core_state::check_entry(&self.config, data.len())?;
        self.replica.budget_mut().lane = lane;
        let proposed = self.replica.propose(Vec::new(), data);
        self.heard(proposed)
    }
    pub fn propose_borrowed_in(
        &mut self,
        data: &[u8],
        lane: BudgetLane,
    ) -> Result<(), ConsensusError> {
        self.check_participating()?;
        core_state::check_leader(self.replica.core())?;
        core_state::check_entry(&self.config, data.len())?;
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(data.len())
            .map_err(|_| ConsensusError::Capacity)?;
        owned.extend_from_slice(data);
        self.replica.budget_mut().lane = lane;
        let proposed = self.replica.propose(Vec::new(), owned);
        self.heard(proposed)
    }
    pub fn step(&mut self, message: Message) -> Result<(), ConsensusError> {
        self.check_participating()?;
        core_state::check_message(&self.config, &message)?;
        self.replica.budget_mut().lane = BudgetLane::Completion;
        let stepped = self.replica.step(message);
        self.heard(stepped)
    }
    pub fn tick(&mut self) -> Result<(), ConsensusError> {
        self.check_participating()?;
        let ticked = self.replica.tick();
        self.heard(ticked)?;
        self.periods = self.periods.saturating_add(1);
        // A period on, room may have been freed: the next drain makes the refused writes again.
        if self.replica.is_stalled() && self.replica.held().is_none() {
            self.more = true;
        }
        // The drive that made the applied index run past the durable commit marked when; a
        // whole period after, the next drive writes the commit (`Settings::quiet`).
        self.quiet_due = self.replica.in_flight() == 0
            && self.replica.behind_fence().is_none()
            && self.replica.applied().index > self.replica.durable_commit();
        Ok(())
    }
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), ConsensusError> {
        self.check_participating()?;
        let held = self
            .replica
            .reads_held()
            .saturating_add(self.replica.core().raft.ready_read_count());
        core_state::check_read(&self.config, self.replica.core(), &context, held)?;
        self.replica.budget_mut().lane = BudgetLane::Completion;
        let asked = self.replica.read(context);
        self.heard(asked)
    }
    pub fn propose_conf_change(&mut self, change: ConfChangeV2) -> Result<(), ConsensusError> {
        self.check_participating()?;
        let conf = self.replica.configuration();
        core_state::check_conf_change(&self.config, self.replica.core(), conf, &change)?;
        self.replica.budget_mut().lane = BudgetLane::Completion;
        let proposed = self.replica.change(Vec::new(), &change);
        self.heard(proposed)
    }
    pub fn set_priority(&mut self, priority: i64) -> Result<(), ConsensusError> {
        self.check()?;
        if priority < 0 {
            return Err(ConsensusError::Configuration(
                "election priority must not be negative",
            ));
        }
        self.priority = priority;
        self.replica.set_priority(priority);
        Ok(())
    }
    /// As over focal-log: never more than this group's budget can stage in one transition
    /// beside a page for every member.
    pub fn set_inflight_bytes(&mut self, peer: u64, bytes: u64) -> Result<bool, ConsensusError> {
        self.check()?;
        let page = self.page_bytes();
        let members = u64::try_from(self.replica.core().raft.tracker().len()).unwrap_or(u64::MAX);
        let stageable = u64::try_from(self.budget.reservation_limit(BudgetLane::Completion))
            .unwrap_or(u64::MAX)
            .saturating_sub(self.replica.charged())
            .saturating_sub(page.saturating_mul(members.saturating_add(1)));
        Ok(self
            .replica
            .set_inflight_bytes(peer, bytes.min(stageable).max(1)))
    }
    pub fn page_bytes(&self) -> u64 {
        self.replica.core().raft.config().max_size_per_msg
    }
    pub fn inflight_bytes(&self, peer: u64) -> Option<(u64, u64)> {
        self.replica
            .core()
            .raft
            .tracker()
            .get(peer)
            .map(|progress| (progress.inflights.bytes(), progress.inflights.byte_cap()))
    }
    pub fn heartbeat_tick(&self) -> usize {
        self.config.heartbeat_tick
    }
    pub fn beat(&mut self) -> Result<(), ConsensusError> {
        self.check()?;
        let beat = self.replica.beat();
        self.heard(beat)
    }
    pub fn election_tick(&self) -> usize {
        self.config.election_tick
    }
    pub fn reads_unasked(&self) -> bool {
        self.replica.core().raft.reads_unasked()
    }
    pub fn reads_waiting(&self) -> usize {
        self.replica.core().raft.pending_read_count()
    }
    pub fn pending_reads(&self) -> usize {
        self.config.max_inflight_messages.saturating_add(1)
    }
    pub fn inflight_window(&self) -> usize {
        self.config.max_inflight_messages
    }
    pub fn set_patience(&mut self, ticks: usize) -> Result<(), ConsensusError> {
        self.check()?;
        self.replica.set_patience(ticks);
        Ok(())
    }
    pub fn priority(&self) -> i64 {
        self.priority
    }
    pub fn effective_priority(&self) -> i64 {
        self.replica.core().raft.priority_in_force()
    }
    pub fn transfer_leader(&mut self, node: u64) -> Result<(), ConsensusError> {
        self.check_participating()?;
        let conf = self.replica.configuration();
        core_state::check_transfer(&self.config, self.replica.core(), conf, node)?;
        let told = self.replica.transfer(node);
        self.heard(told)
    }
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
        let set = self.replica.set_randomized_election_timeout(ticks);
        self.heard(set)
    }
    pub fn report_unreachable(&mut self, node: u64) -> Result<(), ConsensusError> {
        self.check_participating()?;
        let told = self.replica.report_unreachable(node);
        self.heard(told)
    }
    pub fn report_snapshot(
        &mut self,
        node: u64,
        status: SnapshotStatus,
    ) -> Result<(), ConsensusError> {
        self.check_participating()?;
        let told = self
            .replica
            .report_snapshot(node, status == SnapshotStatus::Finish);
        self.heard(told)
    }
    pub fn report_snapshot_at(
        &mut self,
        node: u64,
        term: u64,
        index: u64,
        status: SnapshotStatus,
    ) -> Result<(), ConsensusError> {
        self.check()?;
        if !core_state::snapshot_still_pending(self.replica.core(), node, term, index) {
            return Ok(());
        }
        self.report_snapshot(node, status)
    }
    pub fn checkpoint(&mut self, index: u64, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.begin_checkpoint(index, data)?;
        self.finish_checkpoint()
    }
    pub fn published_term(&self, index: u64) -> Result<u64, ConsensusError> {
        self.check()?;
        if index == 0 || index > self.delivered() {
            return Err(ConsensusError::CheckpointIndex);
        }
        Ok(self.replica.core().store().term(index)?)
    }
    /// As focal-log's backend's: whether the entry the current term began with is at or below
    /// `index`, a published entry.
    pub fn term_began_by(&self, index: u64) -> Result<bool, ConsensusError> {
        let core = self.replica.core();
        if self.published_term(index)? != core.raft.term() {
            return Ok(false);
        }
        Ok(core_state::term_began_by(core, index))
    }
    pub fn step_authenticated(
        &mut self,
        peer_node_id: u64,
        encoded: &[u8],
    ) -> Result<(), ConsensusError> {
        self.check()?;
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
    pub fn bootstrap_membership(&self) -> (&[u64], &[u64]) {
        (&self.config.voters, &self.config.learners)
    }
    /// Free bytes on the filesystem holding the group's files, sampled now, less what the
    /// volume's other durable owners have promised.
    pub fn disk_available_bytes(&self) -> Result<u64, ConsensusError> {
        let dir = &self.dir;
        self.disk
            .refresh_with(|| focal_platform::available_space(dir));
        Ok(self.disk.uncommitted_free())
    }
    pub fn disk_budget(&self) -> DiskBudget {
        self.disk.clone()
    }
    pub fn status(&self) -> NodeStatus {
        let conf = self.replica.configuration();
        core_state::status(
            self.replica.core(),
            self.config.node_id,
            self.delivered(),
            conf,
        )
    }
    pub fn scalars(&self) -> NodeScalars {
        core_state::scalars(self.replica.core(), self.config.node_id, self.delivered())
    }
    pub fn membership(&self) -> MembershipView<'_> {
        let conf = self.replica.configuration();
        MembershipView {
            voters: &conf.voters,
            learners: &conf.learners,
        }
    }
    pub fn peer_progress(&self) -> Vec<PeerProgress> {
        core_state::peer_progress(self.replica.core(), self.config.node_id)
    }
    pub fn peer(&self, node: u64) -> Option<PeerProgress> {
        core_state::peer(self.replica.core(), self.config.node_id, node)
    }
    pub fn transferring(&self) -> Option<u64> {
        self.replica.core().raft.lead_transferee()
    }
    pub fn configuration_pending(&self) -> bool {
        self.replica.core().raft.pending_conf_index() > self.delivered()
    }
    /// The point of the group's image: what a member behind the log is sent, so a member added
    /// after it can only be seeded by a later checkpoint, since Raft discards a snapshot whose
    /// configuration does not name the recipient.
    pub fn snapshot_index(&self) -> u64 {
        self.replica.machine().durable().index
    }
    pub fn last_index(&self) -> Result<u64, ConsensusError> {
        Ok(self.replica.core().raft.log().last_index()?)
    }
    pub fn snapshot_names_every_member(&self) -> bool {
        self.replica
            .machine()
            .image_names_every_member(self.replica.configuration())
    }
    pub fn has_committed_current_term(&self) -> bool {
        !self.failed()
            && self.decoders.confirmed()
            && !self.rebuild_pending
            && core_state::committed_in_term(self.replica.core())
    }
    /// The shell's log takes its faults from its device; focal-log's points are not its own.
    pub fn inject_fault_once(&mut self, _point: FaultPoint) -> Result<(), ConsensusError> {
        Err(ConsensusError::Configuration(
            "the durable shell has no focal-log fault points",
        ))
    }
    pub fn failed(&self) -> bool {
        self.failed || self.replica.fenced().is_some()
    }
    /// A checkpoint is durable before `begin_checkpoint` returns: none is ever pending.
    pub fn checkpoint_pending(&self) -> bool {
        false
    }
    pub fn begin_checkpoint_funded(
        &mut self,
        index: u64,
        data: Vec<u8>,
        allocation: Allocation,
    ) -> Result<(), ConsensusError> {
        let overhead = if data.capacity() == 0 {
            0
        } else {
            const { 4 * size_of::<usize>() }
        };
        let required = data
            .capacity()
            .checked_add(overhead)
            .ok_or(ConsensusError::Capacity)?;
        if allocation.bytes() < required {
            return Err(ConsensusError::Capacity);
        }
        self.begin_checkpoint(index, data)
    }
    /// The owner's state at `index`, the prefix it was handed, made the group's image: written
    /// whole and durable before it returns, then the log let go of what is before it (O3).
    pub fn begin_checkpoint(&mut self, index: u64, data: Vec<u8>) -> Result<(), ConsensusError> {
        self.check()?;
        let at = self.replica.machine().applied();
        if index == 0
            || index != at.index
            || self.replica.machine().holds_events()
            || self.recovered.is_some()
        {
            return Err(ConsensusError::CheckpointIndex);
        }
        if data.len() > IMAGE_BYTES {
            return Err(ConsensusError::Capacity);
        }
        let configuration = self.replica.configuration().clone();
        self.replica
            .machine_mut()
            .checkpoint(at, &configuration, &data)
            .map_err(file_error)?;
        let waker = self.waker();
        let compacted = self.replica.compact(0, self.periods, &waker);
        self.heard(compacted).map(drop)
    }
    pub fn cancel_unadmitted_checkpoint(&mut self) -> bool {
        false
    }
    pub fn try_finish_checkpoint(&mut self) -> Result<bool, ConsensusError> {
        self.check()?;
        Ok(true)
    }
    pub fn finish_checkpoint(&mut self) -> Result<(), ConsensusError> {
        self.check()
    }
    pub fn confirm_decoder(&mut self, hash: [u8; 32]) -> Result<(), ConsensusError> {
        self.check_state()?;
        self.decoders.confirm(hash)?;
        self.rebuild()
    }
    pub fn confirm_decoder_pair(
        &mut self,
        predecessor: [u8; 32],
        successor: [u8; 32],
    ) -> Result<(), ConsensusError> {
        self.check_state()?;
        self.decoders.confirm_pair(predecessor, successor)?;
        self.rebuild()
    }
    pub fn required_decoder(&self) -> Option<[u8; 32]> {
        self.decoders.effective()
    }
    pub fn decoder_floor_ready(&self, hash: [u8; 32]) -> bool {
        !self.failed()
            && self.decoders.confirmed()
            && !self.rebuild_pending
            && self.decoders.states(hash)
    }
    pub fn begin_decoder_floor(&mut self, hash: [u8; 32]) -> Result<(), ConsensusError> {
        self.check()?;
        match self.decoders.floor_write(hash, None)? {
            Some(intent) => self.write_records(intent),
            None => Ok(()),
        }
    }
    pub fn begin_decoder_transition(&mut self) -> Result<(), ConsensusError> {
        self.check()?;
        match self.decoders.transition_write(None)? {
            Some(intent) => self.write_records(intent),
            None => Ok(()),
        }
    }
    /// A decoder record is durable before `begin_decoder_floor` returns: there is nothing to
    /// finish.
    pub fn try_finish_decoder_floor(&mut self) -> Result<bool, ConsensusError> {
        self.check()?;
        Ok(true)
    }
    pub fn finish_decoder_floor(&mut self) -> Result<(), ConsensusError> {
        self.check()
    }
    pub fn membership_configuration(&self) -> MembershipConfiguration {
        MembershipConfiguration::from_conf(self.replica.configuration())
    }
    pub fn propose_membership(
        &mut self,
        expected: &MembershipConfiguration,
        change: MembershipChange,
        context: Vec<u8>,
    ) -> Result<(), ConsensusError> {
        self.check()?;
        core_state::check_leader(self.replica.core())?;
        if !self.has_committed_current_term()
            || self.delivered() < self.replica.core().raft.log().committed()
        {
            return Err(ConsensusError::Configuration(
                "current-term committed configuration is not published",
            ));
        }
        if &self.membership_configuration() != expected {
            return Err(ConsensusError::Configuration(
                "membership precondition changed",
            ));
        }
        change.apply_to(expected)?;
        self.propose_conf_change(change.encode(context))
    }
    /// A member on the shell writes the node's hyper-log log, not focal-log's shared WAL.
    pub fn shared_wal(&self) -> Result<SharedWal, ConsensusError> {
        Err(ConsensusError::Configuration(
            "a member on the durable shell has no shared WAL",
        ))
    }
    /// Readies are taken ahead of their writes: nothing is ever refused for a write out.
    pub fn persistence_pending(&self) -> bool {
        false
    }
    /// Whether a drain has something to do. A replica that waits, whole, for room or for a
    /// record takes no `Ready`: its next drain is due once a tick or a record says it may go
    /// again (`more`).
    pub fn has_ready(&self) -> bool {
        let stalled = self.replica.is_stalled();
        !self.failed()
            && (self.recovered.is_some()
                || self.more
                || (!stalled
                    && (self.quiet_due
                        || self.replica.in_flight() > 0
                        || self.replica.core().has_ready())))
    }
    pub fn try_drain(&mut self) -> Result<Option<NodeEvents>, ConsensusError> {
        self.poll_drain(false)
    }
    pub fn drain(&mut self) -> Result<NodeEvents, ConsensusError> {
        self.poll_drain(true)?
            .ok_or(ConsensusError::PersistencePending)
    }
    /// A leader's messages leave with the drive that made them: none waits here.
    pub fn sendable(&mut self) -> Result<Option<NodeEvents>, ConsensusError> {
        self.check()?;
        Ok(None)
    }
    pub fn apply_on_written_commit(&mut self) {
        self.replica.machine_mut().act_at_start();
    }
    pub fn notify_persisted(&mut self, signal: Option<PersistedSignal>) {
        self.persisted = signal;
        self.waker = None;
    }
    /// Whether the log will tell the owner when what the group waits for is answered: a signal
    /// is set, writes are out, none waits for room, and the waker they went out with has not
    /// woken already (a write's later parts sent with a waker that woke tell no one).
    pub fn wakes_owner(&self) -> bool {
        self.persisted.is_some()
            && self.replica.in_flight() > 0
            && !self.replica.is_stalled()
            && self
                .waker
                .as_ref()
                .is_some_and(|waker| !waker.fired.load(Ordering::Acquire))
    }
    /// Waits for the log to answer the oldest write out, when one is with it.
    pub fn wait_persisted(&mut self) -> Result<bool, ConsensusError> {
        self.check()?;
        if self.replica.in_flight() == 0 {
            return Ok(false);
        }
        Ok(self.replica.log_mut().inner_mut().wait())
    }

    /// The applied index the owner was handed, or is handed by the drain that takes what
    /// opening replayed.
    fn delivered(&self) -> u64 {
        self.replica.machine().applied().index
    }

    /// Refused for a member that failed or whose decoder is unconfirmed.
    fn check(&self) -> Result<(), ConsensusError> {
        self.check_state()?;
        if self.decoders.confirmed() {
            Ok(())
        } else {
            Err(ConsensusError::DecoderUnconfirmed)
        }
    }

    fn check_state(&self) -> Result<(), ConsensusError> {
        if self.failed() {
            Err(ConsensusError::Failed)
        } else {
            core_state::check_core(self.replica.core())
        }
    }

    /// A Raft input: refused, as over focal-log, until a decoder-gated member's membership is
    /// rebuilt.
    fn check_participating(&self) -> Result<(), ConsensusError> {
        self.check()?;
        if self.rebuild_pending {
            Err(ConsensusError::PersistencePending)
        } else {
            Ok(())
        }
    }

    /// The replay a decoder-gated open deferred, once the decoder is confirmed.
    fn rebuild(&mut self) -> Result<(), ConsensusError> {
        if !self.rebuild_pending || !self.decoders.confirmed() {
            return Ok(());
        }
        self.replay()?;
        self.rebuild_pending = false;
        Ok(())
    }

    /// The replica's answer as the owners' error. A refusal changed nothing; a fenced replica
    /// is failed until it is reopened. A replica waiting, whole, for a decoder record its store
    /// holds a write for refuses as focal-log refuses while its floor write is pending
    /// (contract (f)); one waiting for room in the log, as any capacity refusal.
    fn heard<T>(&mut self, outcome: Result<T, ReplicaError>) -> Result<T, ConsensusError> {
        let held = self.replica.held().is_some();
        outcome.map_err(|error| match error {
            ReplicaError::Refused(error) => ConsensusError::Raft(error),
            ReplicaError::Stalled if held => ConsensusError::PersistencePending,
            ReplicaError::Stalled | ReplicaError::Budget(_) => ConsensusError::Capacity,
            ReplicaError::Marked => ConsensusError::Raft(hyper_raft::Error::Lost),
            ReplicaError::Fenced(cause) => {
                self.failed = true;
                match cause {
                    Cause::Unwound => ConsensusError::DependencyFailure,
                    Cause::Core(error) => ConsensusError::Raft(error),
                    Cause::Write(_) | Cause::Machine(_) | Cause::Invariant(_) => {
                        ConsensusError::Failed
                    }
                }
            }
        })
    }

    /// The waker a drive's writes go out with: the one made last while no write has taken it
    /// and its wake has not come, else one made anew from the owner's signal. Without a signal,
    /// none wakes anyone: the owner asks.
    fn waker(&mut self) -> Waker {
        let Some(signal) = self.persisted.as_ref() else {
            return Waker::noop().clone();
        };
        if let Some(waker) = self
            .waker
            .as_ref()
            .filter(|waker| !waker.fired.load(Ordering::Acquire))
        {
            return Waker::from(Arc::clone(waker));
        }
        let made = Arc::new(WakeOnce {
            persisted: LazyLock::new(signal()),
            fired: AtomicBool::new(false),
        });
        self.waker = Some(Arc::clone(&made));
        Waker::from(made)
    }

    /// One drive, what it gave moved into `events`: the bytes it may hand over reserved before
    /// it (contract (g)), and kept with the events once it gave them.
    fn drive_into(&mut self, events: &mut NodeEvents) -> Result<(), ConsensusError> {
        let held = usize::try_from(self.replica.charged()).unwrap_or(usize::MAX);
        let last = self.replica.core().raft.log().last_index()?;
        let unapplied = last.saturating_sub(self.replica.applied().index);
        let bound = memory::drive_bytes(&self.config, held, unapplied)?;
        let mut reserved = memory::reserve(
            &self.budget,
            BudgetKind::Pending,
            BudgetLane::Completion,
            bound,
        )?;
        if self.replica.is_stalled() && self.replica.held().is_none() {
            // Another group's compaction, or this one's, may have freed room since. A write the
            // store holds goes again once the record it waits for releases it (`write_records`).
            self.replica.resume();
        }
        let waker = self.waker();
        self.out.clear();
        let driven = self.replica.drive(self.periods, &waker, &mut self.out);
        let driven = self.heard(driven)?;
        self.more = driven.more;
        self.quiet_due = false;
        events
            .messages
            .try_reserve(self.out.messages.len())
            .map_err(|_| ConsensusError::Capacity)?;
        events.messages.append(&mut self.out.messages);
        events
            .read_states
            .try_reserve(self.out.reads.len())
            .map_err(|_| ConsensusError::Capacity)?;
        events.read_states.extend(
            self.out
                .reads
                .drain(..)
                .map(|(context, index)| ReadBarrier { index, context }),
        );
        self.replica.machine_mut().take(events)?;
        let given = memory::events_bytes(events)?;
        let charged = events.allocation.as_ref().map_or(0, Allocation::bytes);
        let wanted = given.saturating_sub(charged);
        if wanted <= reserved.bytes() {
            if reserved.shrink_to(wanted).is_err() {
                return Err(ConsensusError::Capacity);
            }
        } else {
            // A change's configurations beyond the bound: reserved now, or the events are kept
            // for the next drain, nothing lost.
            let mut more = memory::reserve(
                &self.budget,
                BudgetKind::Pending,
                BudgetLane::Completion,
                wanted.saturating_sub(reserved.bytes()),
            )?;
            if reserved.absorb(&mut more).is_err() {
                return Err(ConsensusError::Capacity);
            }
        }
        match events.allocation.as_mut() {
            Some(allocation) => allocation
                .absorb(&mut reserved)
                .map_err(|_| ConsensusError::Capacity)?,
            None => events.allocation = Some(reserved),
        }
        Ok(())
    }

    /// One drain: a drive, and for a blocking drain, a wait for each write out after it and a
    /// drive to take its answer. A drain that gives nothing while a write is out is `None`: the
    /// owner waits for the write (`wait_persisted`) or is woken by it.
    fn poll_drain(&mut self, blocking: bool) -> Result<Option<NodeEvents>, ConsensusError> {
        self.check()?;
        let delivered = self.delivered();
        let mut events = self.recovered.take().unwrap_or_default();
        if let Err(error) = self.drive_into(&mut events) {
            self.keep(events);
            return Err(error);
        }
        if blocking {
            for _ in 0..self.replica.in_flight() {
                if !self.replica.log_mut().inner_mut().wait() {
                    break;
                }
                if let Err(error) = self.drive_into(&mut events) {
                    self.keep(events);
                    return Err(error);
                }
            }
        } else if self.replica.in_flight() > 0 && nothing_in(&events, delivered) {
            return Ok(None);
        }
        Ok(Some(events))
    }

    /// Events a drain could not give whole, kept for the next one.
    fn keep(&mut self, events: NodeEvents) {
        if !self.failed() {
            self.recovered = Some(events);
        }
    }

    /// Writes the group's records with `intent` and tells the store and the gate it is durable:
    /// a write the store held for it goes out at the next drive.
    fn write_records(&mut self, intent: FloorWrite) -> Result<(), ConsensusError> {
        let mut medium = FileMedium;
        let mut records = group_files::read_records(&medium, &self.dir)
            .map_err(file_error)?
            .ok_or(ConsensusError::Corruption(
                "the group's records are missing",
            ))?;
        let met = match intent {
            FloorWrite::Baseline(hash) => {
                records.decoder_floor = Some(hash);
                hash
            }
            FloorWrite::Transition(pair) => {
                records.decoder_transition = Some((pair.predecessor, pair.successor));
                pair.successor
            }
        };
        group_files::write_records(&mut medium, &self.dir, &records).map_err(file_error)?;
        self.decoders.written(intent);
        self.replica.release(&met);
        // The write the store held goes out at the next drive.
        self.more = true;
        Ok(())
    }
}

/// A member that is let go writes the commit no write has stated, as over focal-log: one drive a
/// period on, its output given to no one. Nothing is written for a member that failed or has a
/// write out: its log answers for itself.
impl Drop for ShellNode {
    fn drop(&mut self) {
        if !self.failed()
            && self.replica.in_flight() == 0
            && !self.replica.is_stalled()
            && self.replica.applied().index > self.replica.durable_commit()
        {
            let waker = Waker::noop().clone();
            self.out.clear();
            let _ = self
                .replica
                .drive(self.periods.saturating_add(1), &waker, &mut self.out);
        }
    }
}

#[cfg(test)]
#[path = "shell_node_tests.rs"]
mod tests;
