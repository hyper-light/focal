//! One member of one group: the core, its group's log, its state machine and its budget, driven
//! by its owner (`docs/durable.md` §2–§7).
//!
//! The owner steps what arrives into the replica and calls [`Replica::drive`] when it stepped
//! something or when the log's answer woke it. Each drive takes the log's answers, applies what
//! the commit fence allows, and takes at most one `Ready` (the owner's quantum, §7), whose write
//! it submits with the owner's waker. Nothing is held between drives but the writes out, each
//! with what waits for it.
//!
//! The invariants of `docs/durable.md` §3, as this shell keeps them (the core keeps the rest):
//!
//! - **I1, I2.** What a `Ready` gives to send at once leaves at once; the core gives a leader's
//!   only while its term and vote are durable. Every other message leaves with the write it
//!   waits for ([`Out::messages`]), once that write and every earlier one is durable: answers are
//!   taken in submission order, and a write refused for room releases nothing until it is made
//!   again.
//! - **I3.** The core is told a write is durable only from its answer
//!   ([`RawNode::on_persist`]).
//! - **I4.** Only what the core gives to apply is applied; R-4 gives what is committed and
//!   durable here.
//! - **I5, the commit fence.** A change of configuration, and an entry the state machine acts on
//!   at its next start, is applied only once the durable commit `C_d` covers it: the greatest of
//!   the commit the durable writes stated and the state machine's durable index. Until then it
//!   waits with everything after it ([`Replica::behind_fence`]), the replica takes no `Ready`,
//!   and a write states the commit: one out already, or one of the hard state alone.
//! - **I6.** Answers are what the state machine returns as it applies.
//! - **I7.** Writes are submitted in order and their answers taken in order; every write states
//!   the commit the core holds when it is laid out, which names only entries it or an earlier
//!   write holds, and a sole voter states the last entry of its own term, which its own write
//!   commits. A stated commit past what the log holds once the write is durable fences the
//!   replica.
//! - **I8.** A snapshot is made durable by the state machine before the write that moves the
//!   log's start to it; a compaction never passes the state machine's durable index; opening
//!   finishes an install the log never recorded.
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Waker;
use std::time::Duration;

use hyper_liveness::{Liveness, PeerId};
use hyper_raft::proto::{
    self, ConfChange, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message, Snapshot,
    SnapshotMetadata,
};
use hyper_raft::wire::Record;
use hyper_raft::{
    Config, Elections, Lost, RawNode, SnapshotStatus, StateRole, StorageError, Timing,
};
use hyper_timing::{Ballot, Flushes, Span, Trust, inflight_window};

use crate::budget::{Budget, Unbounded};
use crate::compaction::Compaction;
use crate::held::Held;
use crate::machine::{Fatal, StateMachine};
use crate::store::{Entries, EntryRef, Fault, Health, LogStore, Point, Write};

/// How a replica runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    /// The core's settings. Its `applied` is the state machine's durable index, its
    /// `limits.readies_in_flight` the store's depth and its `elections` this setting's, whatever
    /// is given here.
    pub core: Config,
    /// How the replica elects, fixed for as long as it runs: by suspicion (timing step L-2,
    /// `docs/timing.md` §2.3), with pre-vote and check-quorum on or the replica does not open; or
    /// on ticks, which the owner gives ([`Replica::tick`]) until it elects by suspicion
    /// (`docs/durable.md` §8). Stated by every owner: there is no default.
    pub elections: Elections,
    /// The owner's period: a commit no write has stated while the applied index ran past the
    /// durable commit for this long is written then, so a member that stops reopens with what it
    /// applied (`docs/durable.md` §4.1; focal F17's period).
    pub quiet: Duration,
}

/// Why a replica is fenced: it takes no call until its owner reopens it from what is durable.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Cause {
    /// The log failed a write: what the device holds is unknown.
    #[error("{0}")]
    Write(Fault),
    /// The core's own state no longer adds up.
    #[error("the core: {0}")]
    Core(hyper_raft::Error),
    /// The state machine failed.
    #[error("{0}")]
    Machine(Fatal),
    /// The core or the state machine unwound inside a call.
    #[error("a call unwound")]
    Unwound,
    /// The shell's own bookkeeping no longer adds up.
    #[error("the shell: {0}")]
    Invariant(&'static str),
}

/// What a call on a replica refuses.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReplicaError {
    /// The core refused; nothing changed.
    #[error("refused: {0}")]
    Refused(hyper_raft::Error),
    /// A write waits for room in the log: the replica takes no part until its owner frees room
    /// and drives again (`docs/durable.md` §2.4). Nothing changed.
    #[error("stalled for room in the log")]
    Stalled,
    /// The member's log may lack entries it acknowledged: it does not campaign.
    #[error("the member's log may lack entries it acknowledged")]
    Marked,
    /// The owner's budget could not hold the input's bytes; nothing changed.
    #[error("the budget could not hold {0} bytes")]
    Budget(u64),
    /// The replica is fenced and must be reopened.
    #[error("fenced: {0}")]
    Fenced(Cause),
}

/// Why a replica did not open.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OpenError {
    /// The log refused or failed.
    #[error("the log: {0}")]
    Log(Fault),
    /// The log could not be read.
    #[error("the log: {0}")]
    Storage(StorageError),
    /// The core would not open.
    #[error("the core: {0}")]
    Core(hyper_raft::Error),
    /// The state machine failed.
    #[error("{0}")]
    Machine(Fatal),
    /// The log starts past what the state machine holds durably (I8): entries the machine needs
    /// are gone.
    #[error("the log starts at {start} past the state machine's {machine}")]
    StartPastMachine {
        /// The log's start.
        start: u64,
        /// The state machine's durable index.
        machine: u64,
    },
}

/// What a drive gave out, in buffers the owner keeps and reuses.
#[derive(Debug)]
pub struct Output<A> {
    /// Messages to send, in order.
    pub messages: Vec<Message>,
    /// What the state machine answered as it applied, in order.
    pub answers: Vec<A>,
    /// Reads a quorum confirmed and the replica has applied far enough to serve: each read's
    /// context and the index it was confirmed at.
    pub reads: Vec<(Vec<u8>, u64)>,
    /// What this member proposed by the fast track and another entry took the index of, in
    /// order (`hyper_raft::Ready::displaced`): no member applies it, so its proposer proposes it
    /// again or answers that it was not taken. The core gives at most its bound on proposals in
    /// one `Ready` (`Limits::proposals`).
    pub displaced: Vec<Entry>,
}

impl<A> Default for Output<A> {
    fn default() -> Self {
        Self {
            messages: Vec::new(),
            answers: Vec::new(),
            reads: Vec::new(),
            displaced: Vec::new(),
        }
    }
}

impl<A> Output<A> {
    /// Empties the buffers, keeping their room.
    pub fn clear(&mut self) {
        self.messages.clear();
        self.answers.clear();
        self.reads.clear();
        self.displaced.clear();
    }
}

/// What a drive left.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Driven {
    /// Driving again now would do more without waiting for an answer from the log: the owner
    /// queues the replica for its next quantum.
    pub more: bool,
    /// Writes out.
    pub out: usize,
    /// A write waits for room: what the log said.
    pub stalled: Option<Fault>,
    /// When to drive the replica again though nothing arrives (`Replica::deadline`): a
    /// campaign's delay, a leader's beat while its group has work in flight, a transfer's end.
    /// None for a group with nothing in flight: it needs no wake at all.
    pub wake: Option<u64>,
    /// The latest write of the replica's that became durable in this drive: when it was
    /// submitted and when its answer was taken. The node's liveness stream takes it as the flush
    /// a heartbeat proves (`hyper_liveness::Liveness::on_durable`, `Write::Log`).
    pub flushed: Option<(u64, u64)>,
}

/// The writes a replica made, by what they were for: what an owner reads to see what its group
/// costs its log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Writes {
    /// `Ready`s whose write held something.
    pub readies: u64,
    /// `Ready`s that held nothing to make durable: no write.
    pub empty: u64,
    /// Writes of the commit alone the fence asked for.
    pub fenced: u64,
    /// Writes of the commit alone after a quiet period.
    pub quiet: u64,
    /// Compactions' starts.
    pub starts: u64,
    /// Writes the store held for its owner ([`Fault::Held`]), each counted once as it was held.
    pub held: u64,
}

/// A write the replica made, oldest first, with what waits for it.
#[derive(Debug)]
struct Out {
    kind: Kind,
    state: State,
    /// Messages to send once this write and every earlier one is durable.
    messages: Vec<Message>,
    /// The commit its hard state stated.
    commit: Option<u64>,
    /// Whether it changed the term or the vote: its time to durable is a vote's flush.
    vote: bool,
    /// Whether it moves the log's start: a snapshot's install or a compaction.
    start: bool,
    submitted: u64,
}

/// What a write is of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// The `Ready` of this number.
    Ready(u64),
    /// The commit alone.
    Commit,
    /// A compaction's start, and the bytes of the image it was made with: what the compaction
    /// rule weighs the log against once the start is durable ([`Compaction`]).
    Start(u64),
}

/// Where a write is.
#[derive(Clone, Debug, PartialEq, Eq)]
enum State {
    /// With the log.
    Submitted,
    /// It held nothing to make durable: it is durable once every write before it is.
    Empty,
    /// The log refused it as it was submitted.
    Refused(Fault),
}

/// Writes refused for room, waiting to be made again.
#[derive(Debug)]
struct Stall {
    fault: Fault,
    /// Writes that were out behind the refused one when its refusal was taken: each is refused
    /// too. A compaction's start submitted after is not, and may be durable.
    behind: usize,
    /// Room may have been freed since the refusal: the replica's own compaction is durable, or
    /// its owner said so ([`Replica::resume`]). Made again before, the writes would only be
    /// refused again, a refusal a drive.
    freed: bool,
    /// The last `Ready` refused: its notice says every refused one is durable.
    ready: Option<u64>,
    messages: Vec<Message>,
}

/// Where a walk of committed entries stopped.
enum Stop {
    /// Everything given was applied.
    Done,
    /// This entry waits for the commit fence.
    Fence(u64),
    /// This entry is past the drive's page: it waits for the next drive.
    Page(u64),
    /// A change of configuration, applied by the core before the walk goes on.
    Change(Point, ConfChangeV2),
    /// The state machine or the entry failed.
    Failed(Cause),
}

/// One member of one group (module docs).
pub struct Replica<L: LogStore, M: StateMachine, B: Budget = Unbounded> {
    node: RawNode<Held<L>>,
    machine: M,
    budget: B,
    /// What the budget holds for this replica.
    charged: u64,
    depth: usize,
    quiet: Duration,
    writes: VecDeque<Out>,
    stall: Option<Stall>,
    /// The hard state of the last write submitted, or what the log held at open.
    issued: HardState,
    /// The commit the durable writes stated.
    logged_commit: u64,
    /// The last entry applied.
    applied: Point,
    /// The bytes of the applied entries the log holds past its start, as the core counts an
    /// entry's ([`EntryRef::encoded_bytes`]): what a compaction frees, and what the compaction
    /// rule weighs ([`Compaction`]).
    compactable: u64,
    /// The bytes of the image the log was last compacted to: the one this member made or
    /// installed, or at open, the image of what it opened with; none from a machine that keeps
    /// no image.
    imaged: Option<u64>,
    /// What the core was last told was applied.
    told_applied: u64,
    /// The index of the entry that made the configuration.
    conf_index: u64,
    /// Committed entries given to apply that wait: the first and last. They wait for the commit
    /// fence, or, when `paged`, for the next drive.
    fence: Option<(u64, u64)>,
    /// The entries that wait are past a drive's page, not behind the commit fence.
    paged: bool,
    /// The bytes of entries a drive applies at most, as the core counts them
    /// (`EntryRef::encoded_bytes`): its committed page (`max_committed_size_per_ready`), or one
    /// entry larger than it.
    page: u64,
    /// The bytes of entries applied in this drive.
    drive_applied: u64,
    /// Reads confirmed, waiting to be applied far enough: index and context.
    reads: VecDeque<(u64, Vec<u8>)>,
    /// Snapshot reports that came while stalled: one a member.
    reports: Vec<(u64, bool)>,
    fenced: Option<Cause>,
    /// When the applied index first ran past the durable commit with nothing out.
    quiet_since: Option<u64>,
    flushes: Flushes,
    last_durable: Option<u64>,
    writes_made: Writes,
    /// The latest write made durable since the last drive ended ([`Driven::flushed`]).
    flushed: Option<(u64, u64)>,
}

/// The term of an entry the state machine applied, as a point.
fn point_of(entry: &EntryRef<'_>) -> Point {
    Point {
        index: entry.index,
        term: entry.term,
    }
}

/// The change a committed entry states, read where it lies; none for one that states none.
fn change_of(entry: &EntryRef<'_>) -> Result<ConfChangeV2, Cause> {
    let decoded = match entry.kind {
        EntryType::EntryNormal => return Err(Cause::Invariant("a normal entry read as a change")),
        EntryType::EntryConfChange if entry.data.is_empty() => {
            Ok(proto::joint(&ConfChange::default()))
        }
        EntryType::EntryConfChange => ConfChange::decode(entry.data).map(|c| proto::joint(&c)),
        EntryType::EntryConfChangeV2 if entry.data.is_empty() => Ok(ConfChangeV2::default()),
        EntryType::EntryConfChangeV2 => ConfChangeV2::decode(entry.data),
    };
    decoded.map_err(|_| Cause::Invariant("a committed change does not decode"))
}

/// Bytes as the budget counts them.
fn bytes(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

/// Every member a configuration names.
fn names(configuration: &ConfState, member: u64) -> bool {
    [
        &configuration.voters,
        &configuration.learners,
        &configuration.voters_outgoing,
        &configuration.learners_next,
    ]
    .iter()
    .any(|ids| ids.contains(&member))
}

impl<L: LogStore, M: StateMachine, B: Budget> Replica<L, M, B> {
    /// Opens a member on what `log` and `machine` hold durably (`docs/durable.md` §4.3).
    pub fn open(settings: &Settings, mut log: L, machine: M, budget: B) -> Result<Self, OpenError> {
        let durable = machine.durable();
        let view = repair_at_open(&mut log, durable)?;
        let depth = log.depth();
        let mut config = settings.core.clone();
        config.applied = durable.index;
        config.limits.readies_in_flight = depth;
        config.elections = settings.elections;
        // The core keeps what the log may lack and acts on it (`docs/durable.md` §5): it judges
        // votes by it, and tells a leader that counts the entries that they are lost (R-5).
        config.lost = match view.health {
            Health::Marked(mark) => Some(Lost {
                index: mark.index,
                term: mark.term,
            }),
            Health::Whole => None,
        };
        // A walk of the log reads a page the size of what one `Ready` gives to apply.
        let page = config.max_committed_size_per_ready;
        let held = Held::new(log, machine.configuration().clone(), view.hard_state, page);
        let node = RawNode::new(&config, held).map_err(OpenError::Core)?;
        let mut writes = VecDeque::new();
        // Room for every write out: the core's depth and one write of the shell's own.
        writes
            .try_reserve_exact(depth.saturating_add(1))
            .map_err(|_| OpenError::Core(hyper_raft::Error::Memory))?;
        let mut replica = Self {
            node,
            machine,
            budget,
            charged: 0,
            depth,
            quiet: settings.quiet,
            writes,
            stall: None,
            issued: view.hard_state,
            logged_commit: view.hard_state.commit,
            applied: durable,
            compactable: 0,
            imaged: None,
            told_applied: durable.index,
            conf_index: durable.index,
            fence: None,
            paged: false,
            page,
            drive_applied: 0,
            reads: VecDeque::new(),
            reports: Vec::new(),
            fenced: None,
            quiet_since: None,
            flushes: Flushes::new(),
            last_durable: None,
            writes_made: Writes::default(),
            flushed: None,
        };
        // A log compacted before the restart serves a lagging member only by snapshot, and its
        // image is what the compaction rule weighs the log against; a log never compacted is
        // weighed against the image of what the member opened with, as its machine states it.
        replica.imaged = if view.start.index > 0 {
            let prepared = replica.prepare().map_err(|e| match e {
                ReplicaError::Fenced(Cause::Machine(fatal)) => OpenError::Machine(fatal),
                _ => OpenError::Core(hyper_raft::Error::Invariant("a snapshot at open")),
            })?;
            Some(prepared)
        } else {
            replica.machine.image_bytes()
        };
        replica.compactable = replica
            .compactable_after(view.start.index)
            .map_err(|e| OpenError::Core(hyper_raft::Error::Storage(e)))?;
        replica.settle();
        Ok(replica)
    }

    /// This member.
    pub fn id(&self) -> u64 {
        self.node.raft.id()
    }

    /// The member's term.
    pub fn term(&self) -> u64 {
        self.node.raft.term()
    }

    /// The member it believes leads, zero for none.
    pub fn leader(&self) -> u64 {
        self.node.raft.leader_id()
    }

    /// Whether it leads.
    pub fn is_leader(&self) -> bool {
        self.node.raft.state() == StateRole::Leader
    }

    /// The last entry applied.
    pub fn applied(&self) -> Point {
        self.applied
    }

    /// The durable commit `C_d` (`docs/durable.md` §4.1): the greatest of the commit the durable
    /// writes stated and the state machine's durable index.
    pub fn durable_commit(&self) -> u64 {
        self.logged_commit.max(self.machine.durable().index)
    }

    /// The commit the member's durable writes stated.
    pub fn logged_commit(&self) -> u64 {
        self.logged_commit
    }

    /// The configuration as of the last entry applied.
    pub fn configuration(&self) -> &ConfState {
        &self.node.store().configuration
    }

    /// Committed entries that wait for the commit fence: the first and the last.
    pub fn behind_fence(&self) -> Option<(u64, u64)> {
        self.fence.filter(|_| !self.paged)
    }

    /// Writes out.
    pub fn in_flight(&self) -> usize {
        self.writes.len()
    }

    /// Whether a write waits for room.
    pub fn is_stalled(&self) -> bool {
        self.stall.is_some()
    }

    /// Why the replica is fenced, if it is.
    pub fn fenced(&self) -> Option<&Cause> {
        self.fenced.as_ref()
    }

    /// The point through which the member's log may lack entries it acknowledged: the core's,
    /// which ends it once its durable log holds what it marks.
    pub fn mark(&self) -> Option<Point> {
        self.node.raft.lost().map(|lost| Point {
            index: lost.index,
            term: lost.term,
        })
    }

    /// The flushes of the writes that made a term or vote durable, submit to durable, on the
    /// owner's clock: what hyper-timing's ballot charges a vote (`docs/timing.md` §2.3).
    pub fn flushes(&self) -> &Flushes {
        &self.flushes
    }

    /// When the log last answered a write durable: the evidence a node heartbeats on
    /// (`docs/timing.md` §2.1, L-3).
    pub fn last_durable(&self) -> Option<u64> {
        self.last_durable
    }

    /// The core, to read.
    pub fn core(&self) -> &RawNode<Held<L>> {
        &self.node
    }

    /// The state machine, to read.
    pub fn machine(&self) -> &M {
        &self.machine
    }

    /// The group's log, for its owner's own use of it: its counts, or a device's fault
    /// injection in a test. A write submitted through it, past the shell, is not one the shell
    /// orders or answers.
    pub fn log_mut(&mut self) -> &mut L {
        &mut self.node.store_mut().log
    }

    /// The state machine, for its owner's own use of it, between drives.
    pub fn machine_mut(&mut self) -> &mut M {
        &mut self.machine
    }

    /// The writes this replica made since it opened, by what they were for.
    pub fn writes(&self) -> Writes {
        self.writes_made
    }

    /// The bytes the budget holds for this replica.
    pub fn charged(&self) -> u64 {
        self.charged
    }

    /// Takes back the state machine, as a crash does with the process that held it; the budget
    /// is given back what it held. The log's handle goes with the core: the core gives no store
    /// back (a `RawNode::into_store` is asked of the core, `docs/durable.md` §11), and a
    /// hyper-log group is claimed again from its log.
    pub fn into_machine(mut self) -> M {
        self.budget.release(self.charged);
        self.machine
    }

    /// Whether this member leads and every voter of its configuration has said its durable
    /// commit reaches the entry that made it: each follower's answers carry its durable commit
    /// (core step R-6), and the leader counts its own. Once true, any one member may be lost and
    /// the others still elect under this configuration (mantle's `configuration_known`).
    pub fn configuration_known(&self) -> bool {
        let tracker = self.node.raft.tracker();
        let id = self.id();
        self.is_leader()
            && self.configuration().voters.iter().all(|&voter| {
                if voter == id {
                    self.durable_commit() >= self.conf_index
                } else {
                    tracker
                        .get(voter)
                        .is_some_and(|p| p.committed_index >= self.conf_index)
                }
            })
    }

    /// Whether this member leads and `member` has confirmed holding everything it knows
    /// committed.
    pub fn caught_up(&self, member: u64) -> bool {
        let committed = self.node.raft.log().committed();
        self.is_leader()
            && self
                .node
                .raft
                .tracker()
                .get(member)
                .is_some_and(|p| p.matched >= committed)
    }

    /// Runs `call` inside the unwind boundary (focal's `guarded_in`): an unwind of the core or
    /// the state machine fences the replica, and is reported, never propagated.
    fn guarded<T>(
        &mut self,
        call: impl FnOnce(&mut Self) -> Result<T, ReplicaError>,
    ) -> Result<T, ReplicaError> {
        if let Some(cause) = &self.fenced {
            return Err(ReplicaError::Fenced(cause.clone()));
        }
        match catch_unwind(AssertUnwindSafe(|| call(&mut *self))) {
            Ok(outcome) => outcome,
            Err(_) => Err(self.fence(Cause::Unwound)),
        }
    }

    /// Fences the replica: every write out is dropped with what waited for it, and every call
    /// after answers `Fenced` until the owner reopens it from what is durable.
    fn fence(&mut self, cause: Cause) -> ReplicaError {
        self.writes.clear();
        self.stall = None;
        self.fence = None;
        self.paged = false;
        self.reads.clear();
        self.reports.clear();
        self.fenced = Some(cause.clone());
        ReplicaError::Fenced(cause)
    }

    /// A refusal of the core is the caller's to hear; a fatal error fences.
    fn heard<T>(&mut self, outcome: hyper_raft::Result<T>) -> Result<T, ReplicaError> {
        match outcome {
            Ok(value) => Ok(value),
            Err(error) if error.is_fatal() => Err(self.fence(Cause::Core(error))),
            Err(error) => Err(ReplicaError::Refused(error)),
        }
    }

    /// What the replica asks of the core itself must not be refused: any error fences.
    fn must<T>(&mut self, outcome: hyper_raft::Result<T>) -> Result<T, ReplicaError> {
        outcome.map_err(|error| self.fence(Cause::Core(error)))
    }

    /// A failure of the state machine fences.
    fn machine_did<T>(&mut self, outcome: Result<T, Fatal>) -> Result<T, ReplicaError> {
        outcome.map_err(|fatal| self.fence(Cause::Machine(fatal)))
    }

    /// The replica takes an input: it is not fenced (checked by the boundary) and not stalled.
    fn takes(&self) -> Result<(), ReplicaError> {
        match self.stall {
            Some(_) => Err(ReplicaError::Stalled),
            None => Ok(()),
        }
    }

    /// Reserves an input's bytes before it changes anything.
    fn reserve(&mut self, len: u64) -> Result<(), ReplicaError> {
        if B::BOUNDED {
            if !self.budget.reserve(len) {
                return Err(ReplicaError::Budget(len));
            }
            self.charged = self.charged.saturating_add(len);
        }
        Ok(())
    }

    /// Brings what the budget holds for this replica to what it holds: the core's resident
    /// bytes and the shell's.
    fn settle(&mut self) {
        if !B::BOUNDED {
            return;
        }
        let held = self.held_bytes();
        if held > self.charged {
            self.budget.charge(held.saturating_sub(self.charged));
        } else {
            self.budget.release(self.charged.saturating_sub(held));
        }
        self.charged = held;
    }

    /// The bytes the replica holds: the core's, and the messages, reads and reports the shell
    /// keeps.
    fn held_bytes(&self) -> u64 {
        let messages = |messages: &[Message]| {
            messages
                .iter()
                .fold(0usize, |sum, m| sum.saturating_add(proto::message_bytes(m)))
        };
        let writes = self.writes.iter().fold(0usize, |sum, out| {
            sum.saturating_add(messages(&out.messages))
                .saturating_add(std::mem::size_of::<Out>())
        });
        let stalled = self
            .stall
            .as_ref()
            .map_or(0, |stall| messages(&stall.messages));
        let reads = self.reads.iter().fold(0usize, |sum, (_, context)| {
            sum.saturating_add(context.capacity())
                .saturating_add(std::mem::size_of::<(u64, Vec<u8>)>())
        });
        bytes(
            self.node
                .raft
                .resident_bytes()
                .saturating_add(writes)
                .saturating_add(stalled)
                .saturating_add(reads),
        )
    }

    /// The owner's detectors suspect `member`'s node (timing step L-2, `docs/timing.md` §2.1):
    /// a follower that knows it for its leader campaigns after its delay, a leader they leave no
    /// quorum steps down. Taken while stalled or marked, as it changes nothing durable: the
    /// replica's campaigns are held meanwhile (`docs/durable.md` §8), and what it believes of its
    /// peers is current when they are let go. A fenced replica takes nothing; its owner tells
    /// the reopened one what its detectors believe.
    pub fn suspect(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let told = r.node.suspect(member);
            r.heard(told)
        })
    }

    /// The owner's detectors trust `member`'s node again.
    pub fn trust(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let told = r.node.trust(member);
            r.heard(told)
        })
    }

    /// The owner's detectors saw `member`'s node start again, a new incarnation: trusted, and
    /// leading nothing it led before it stopped.
    pub fn restarted(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let told = r.node.restarted(member);
            r.heard(told)
        })
    }

    /// What the path to `member` carries in a round trip, as the node's transport measures it: its
    /// congestion window to the member's node (mantle note 32 R16). The core keeps in flight to the
    /// member twice that (`hyper_timing::inflight_window`), what the path carries over the two
    /// round trips a lost append takes to repair. Said whenever the transport's measure
    /// moves; false, and nothing changed, for a member the group does not name, or for a carriage
    /// so large that the window would have no bound where the core counts no messages
    /// (`RawNode::set_inflight_bytes`). Until it is said, a member is sent what the core's settings
    /// give (`Config::max_inflight_bytes`).
    pub fn set_carriage(&mut self, member: u64, carried: u64) -> Result<bool, ReplicaError> {
        self.guarded(|r| Ok(r.node.set_inflight_bytes(member, inflight_window(carried))))
    }

    /// Where catching up the learner `member` stands, for the owner to promote it once ready
    /// (`hyper_raft::CatchUp`, thesis §4.2.1, mantle note 32 R13): its rounds of replication are
    /// judged against an election, the time `Timing::election` says the group's takes. Promoting it
    /// is the owner's change to propose.
    pub fn catch_up(&mut self, member: u64) -> Result<hyper_raft::CatchUp, ReplicaError> {
        self.guarded(|r| {
            let said = r.node.catch_up(member);
            r.heard(said)
        })
    }

    /// What the owner's measurements give the group's elections: the span hyper-timing's law
    /// chose over the measured paths to its voters, and their round tail (`hyper_raft::Timing`,
    /// `Timing::of`). Given again whenever the law's ballot moves.
    pub fn set_timing(&mut self, timing: Timing) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let set = r.node.set_timing(timing);
            r.heard(set)
        })
    }

    /// The other members of the group, each once: the node pairs the node's liveness stream keeps
    /// for this group (`hyper_liveness::Liveness::attach`).
    pub fn peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        let id = self.id();
        self.node
            .raft
            .configuration()
            .members()
            .filter(move |member| *member != id)
    }

    /// Tells the core what the node's liveness stream believes of every other member now: on
    /// opening, and on reopening a fenced replica, before any change reaches it.
    pub fn believe_all(&mut self, liveness: &Liveness) -> Result<(), ReplicaError> {
        let peers: Vec<PeerId> = self.peers().collect();
        for peer in peers {
            if liveness.trust(peer) == Some(Trust::Suspected) {
                self.suspect(peer)?;
            } else {
                self.trust(peer)?;
            }
        }
        Ok(())
    }

    /// The group's timing by hyper-timing's law over what the node's liveness stream measured
    /// (`docs/timing.md` §2.3, §2.9): the ballot over the echoed round trips to the group's
    /// voters (`Liveness::round_trip`), the mean flush of this replica's vote writes or, before
    /// one, of the node's log writes (`Liveness::flush_mean`), and the node's timer granularity;
    /// given to the core when it moved. The span, for the leader's pair's `T_E`
    /// (`Liveness::set_election`); none before a quorum's paths and the granularity are measured,
    /// when the core draws no delay and so does not campaign (§3, item 10).
    pub fn measure(&mut self, liveness: &Liveness) -> Result<Option<Span>, ReplicaError> {
        let Some(granularity) = liveness.granularity() else {
            return Ok(None);
        };
        let id = self.id();
        let configuration = self.node.raft.configuration();
        let voters = configuration.voters();
        let paths = voters
            .iter()
            .filter(|voter| **voter != id)
            .filter_map(|voter| liveness.round_trip(*voter));
        let durable = self
            .flushes
            .mean()
            .or_else(|| liveness.flush_mean())
            .unwrap_or(Duration::ZERO);
        let Some(ballot) = Ballot::measure(paths, voters.len(), durable, granularity) else {
            return Ok(None);
        };
        let Some(span) = ballot.span(granularity) else {
            return Ok(None);
        };
        let timing = Timing::of(&ballot, &span);
        if self.node.raft.timing() != Some(timing) {
            self.set_timing(timing)?;
        }
        Ok(Some(span))
    }

    /// When the owner is to drive the replica though nothing arrives (`Driven::wake`); none when
    /// nothing is timed.
    pub fn deadline(&self) -> Option<u64> {
        self.node.deadline()
    }

    /// Wakes the core at the owner's clock (timing step L-2): what it armed since the last
    /// drive is timed, and a campaign, a beat or a transfer's end that is due is done. Its
    /// campaigns are held while a write waits for room: a stalled member takes part in nothing.
    /// A marked one's are the core's to judge (§5).
    fn wake(&mut self, now: u64) -> Result<(), ReplicaError> {
        if self.node.raft.config().elections == Elections::Ticks {
            return Ok(());
        }
        let held = self.stall.is_some();
        let told = self.node.hold_campaigns(held);
        self.must(told)?;
        let woken = self.node.wake(now);
        self.must(woken).map(drop)
    }

    /// A message from the network, bound to its authenticated sender by the transport.
    pub fn step(&mut self, message: Message) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            r.reserve(bytes(proto::message_bytes(&message)))?;
            let stepped = r.node.step(message);
            let outcome = r.heard(stepped);
            r.settle();
            outcome
        })
    }

    fn last_index(&mut self) -> Result<u64, ReplicaError> {
        let bounds = self.node.store().log.bounds();
        bounds
            .map(|(_, last)| last)
            .map_err(|error| self.fence(Cause::Core(hyper_raft::Error::Storage(error))))
    }

    /// Campaigns now; refused [`ReplicaError::Marked`] while the core finds that what the
    /// member's log may lack keeps it from leading (`docs/durable.md` §5).
    pub fn campaign(&mut self) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            match r.node.campaign() {
                Err(hyper_raft::Error::Lost) => Err(ReplicaError::Marked),
                campaigned => r.heard(campaigned),
            }
        })
    }

    /// Proposes an entry of `data`, with `context` attached; only a leader takes it. The bytes
    /// are moved into the log, not copied.
    pub fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            r.reserve(bytes(data.len().saturating_add(context.len())))?;
            let proposed = r.node.propose(context, data);
            let outcome = r.heard(proposed);
            r.settle();
            outcome
        })
    }

    /// Proposes by the fast track (`hyper_raft::fast`); the index proposed for.
    pub fn propose_fast(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<u64, ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            r.reserve(bytes(data.len().saturating_add(context.len())))?;
            let proposed = r.node.propose_fast(context, data);
            let outcome = r.heard(proposed);
            r.settle();
            outcome
        })
    }

    /// Proposes a change of the configuration.
    pub fn change(&mut self, context: Vec<u8>, change: &ConfChangeV2) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            r.reserve(bytes(change.encoded_len().saturating_add(context.len())))?;
            let proposed = r.node.propose_conf_change(context, change);
            let outcome = r.heard(proposed);
            r.settle();
            outcome
        })
    }

    /// Asks for a read confirmed by a quorum; once it is, and the replica has applied through its
    /// index, a drive gives it back in [`Output::reads`]. Refused past the core's bound on reads,
    /// which counts those the replica holds for its apply too.
    pub fn read(&mut self, context: Vec<u8>) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            let held = r
                .reads
                .len()
                .saturating_add(r.node.raft.pending_read_count());
            if held >= r.node.raft.config().limits.pending_reads {
                return Err(ReplicaError::Refused(hyper_raft::Error::Capacity(
                    "reads waiting",
                )));
            }
            r.reserve(bytes(context.len()))?;
            let asked = r.node.read_index(context);
            let outcome = r.heard(asked);
            r.settle();
            outcome
        })
    }

    /// The owner freed room in the log (another group compacted or left, a queue drained): a
    /// replica stalled for room makes its refused writes again at its next drive.
    pub fn resume(&mut self) {
        if let Some(stall) = self.stall.as_mut() {
            stall.freed = true;
        }
    }

    /// What the store holds the replica's refused write for, while it does ([`Fault::Held`]):
    /// a precondition of the store's own, which its owner meets outside the log.
    pub fn held(&self) -> Option<&L::Hold> {
        self.node.store().log.held()
    }

    /// The owner met `met`, what the store held a write for: the store takes it, and the
    /// replica makes its refused writes again at its next drive.
    pub fn release(&mut self, met: &L::Hold) {
        self.node.store_mut().log.release(met);
        self.resume();
    }

    /// One tick of the owner's period, for a replica that elects on ticks (`docs/durable.md`
    /// §8): the core campaigns or beats as its counts say. True when it acted. A stalled
    /// replica is not ticked, for a member that cannot persist takes no part, and the ticks it
    /// missed are not given again. Refused by suspicion.
    pub fn tick(&mut self) -> Result<bool, ReplicaError> {
        self.guarded(|r| {
            if r.node.raft.config().elections != Elections::Ticks {
                return Err(ReplicaError::Refused(hyper_raft::Error::Settings(
                    "elections by suspicion take no ticks",
                )));
            }
            if r.stall.is_some() {
                return Ok(false);
            }
            let ticked = r.node.tick();
            r.heard(ticked)
        })
    }

    /// A leader sends its heartbeats now, between ticks: for an owner whose period is
    /// stretched, the heartbeats keep the cadence its followers expect. Any other role does
    /// nothing.
    pub fn beat(&mut self) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            let beat = r.node.ping();
            r.heard(beat)
        })
    }

    /// The election timeout, in ticks, from the owner's own pace: within one to two of the
    /// core's `election_tick`, or refused.
    pub fn set_randomized_election_timeout(&mut self, ticks: usize) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            let set = r.node.raft.set_randomized_election_timeout(ticks);
            r.heard(set)
        })
    }

    /// Ticks of patience beyond the election timeout, for the stalls the owner has seen in
    /// itself.
    pub fn set_patience(&mut self, ticks: usize) {
        self.node.raft.set_patience(ticks);
    }

    /// What the member does with an append that arrives ahead of a hole
    /// (`hyper_raft::RawNode::set_ahead`): policy its owner sets once every peer can read a kept
    /// refusal, never part of what is durable.
    pub fn set_ahead(&mut self, ahead: hyper_raft::Ahead) {
        self.node.set_ahead(ahead);
    }

    /// The member's election priority (`hyper_raft::RawNode::set_priority`): policy its owner
    /// sets, never part of what is durable.
    pub fn set_priority(&mut self, priority: i64) {
        self.node.set_priority(priority);
    }

    /// While the member leads, `member` is sent no more than `bytes` of entries ahead of its
    /// answers (`hyper_raft::RawNode::set_inflight_bytes`): what its owner learned the path to
    /// it carries. False for a member the configuration does not name.
    pub fn set_inflight_bytes(&mut self, member: u64, bytes: u64) -> bool {
        self.node.set_inflight_bytes(member, bytes)
    }

    /// The owner's budget, for the owner to say what its next reservations are charged to.
    pub fn budget_mut(&mut self) -> &mut B {
        &mut self.budget
    }

    /// Reads a quorum confirmed that wait for the replica to apply through their index.
    pub fn reads_held(&self) -> usize {
        self.reads.len()
    }

    /// Hands the lead to `to`.
    pub fn transfer(&mut self, to: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            let told = r.node.transfer_leader(to);
            r.heard(told)
        })
    }

    /// The last message to `member` did not arrive.
    pub fn report_unreachable(&mut self, member: u64) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            r.takes()?;
            let told = r.node.report_unreachable(member);
            r.heard(told)
        })
    }

    /// Whether a snapshot sent to `to` arrived. Replication to a member pauses until its
    /// snapshot's fate is known, so a report is never refused: one that comes while the replica
    /// is stalled is kept, the latest for each member of the configuration, and taken once it
    /// runs again (`docs/durable.md` §2.4).
    pub fn report_snapshot(&mut self, to: u64, arrived: bool) -> Result<(), ReplicaError> {
        self.guarded(|r| {
            if r.stall.is_none() {
                return r.take_report(to, arrived);
            }
            if names(r.configuration(), to) {
                match r.reports.iter_mut().find(|(member, _)| *member == to) {
                    Some(report) => report.1 = arrived,
                    None => r.reports.push((to, arrived)),
                }
            }
            Ok(())
        })
    }

    fn take_report(&mut self, to: u64, arrived: bool) -> Result<(), ReplicaError> {
        let status = if arrived {
            SnapshotStatus::Finish
        } else {
            SnapshotStatus::Failure
        };
        let told = self.node.report_snapshot(to, status);
        self.heard(told)
    }

    /// Does what there is to do (`docs/durable.md` §2.2): takes the log's answers, applies what
    /// the fence allows, and takes at most one `Ready`, whose write goes out with `waker`. `now`
    /// is the owner's monotonic clock in nanoseconds, as the core's and hyper-liveness's are.
    /// Fills `out`, which the owner reuses.
    pub fn drive(
        &mut self,
        now: u64,
        waker: &Waker,
        out: &mut Output<M::Answer>,
    ) -> Result<Driven, ReplicaError> {
        self.guarded(|r| {
            r.drive_applied = 0;
            r.take_answers(now, out)?;
            if r.stall.is_some() {
                r.make_again(now, waker)?;
            }
            r.wake(now)?;
            if r.stall.is_none() {
                r.take_reports()?;
                r.release_fence(out)?;
                r.take_ready(now, waker, out)?;
                r.quiet_commit(now, waker)?;
            }
            r.release_reads(out);
            r.settle();
            Ok(r.driven())
        })
    }

    fn driven(&mut self) -> Driven {
        let slot = self.has_slot();
        let paged = self.paged && self.fence.is_some();
        let more = self.stall.is_none()
            && (paged
                || (slot
                    && (self.ready_due()
                        || self
                            .behind_fence()
                            .is_some_and(|(_, last)| !self.fence_covered(last)))));
        Driven {
            more,
            out: self.writes.len(),
            stalled: self.stall.as_ref().map(|stall| stall.fault.clone()),
            wake: self.deadline(),
            flushed: self.flushed.take(),
        }
    }

    /// Whether the replica may submit a write: the store has room, and the writes out are fewer
    /// than the core's depth and one of the shell's own.
    fn has_slot(&self) -> bool {
        self.writes.len() < self.depth.saturating_add(1) && self.node.store().log.room()
    }

    /// Takes the log's answers, in the order submitted. Only here: a write submitted in this
    /// drive is never looked at in it, so what a drive gives out never hangs on how soon a flush
    /// happened to end (mantle's determinism finding, replica.md §5).
    fn take_answers(&mut self, now: u64, out: &mut Output<M::Answer>) -> Result<(), ReplicaError> {
        loop {
            let Some(front) = self.writes.front() else {
                return Ok(());
            };
            let behind = self.stall.as_ref().is_some_and(|stall| stall.behind > 0);
            let answer = match &front.state {
                State::Empty if !behind => Ok(()),
                State::Empty => Err(Fault::Behind),
                State::Refused(fault) => Err(fault.clone()),
                State::Submitted => match self.node.store_mut().log.poll() {
                    Some(answer) => answer,
                    None => return Ok(()),
                },
            };
            let write = self
                .writes
                .pop_front()
                .ok_or(Cause::Invariant("an answer for no write"))
                .map_err(|cause| self.fence(cause))?;
            self.answered(write, answer, now, out)?;
        }
    }

    fn answered(
        &mut self,
        write: Out,
        answer: Result<(), Fault>,
        now: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        let behind = match self.stall.as_mut() {
            Some(stall) if stall.behind > 0 => {
                stall.behind = stall.behind.saturating_sub(1);
                true
            }
            _ => false,
        };
        match answer {
            Ok(()) if behind => Err(self.fence(Cause::Invariant(
                "a write made durable behind a refused one",
            ))),
            Ok(()) => self.durable(write, now, out),
            Err(fault) if fault.changed_nothing() => {
                self.refused(write, fault);
                Ok(())
            }
            Err(fault) => Err(self.fence(Cause::Write(fault))),
        }
    }

    /// A write is durable, and every one before it: what waited for it leaves, the core hears,
    /// and what it gives to apply is applied.
    fn durable(
        &mut self,
        write: Out,
        now: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        if write.state == State::Submitted {
            self.last_durable = Some(now);
            self.flushed = Some((write.submitted, now));
            if write.vote {
                // A fold that is full keeps its mean: the sample is one of more than it counts.
                let _ = self.flushes.on_flush(write.submitted, now);
            }
        }
        if let Some(commit) = write.commit {
            // I7: a commit a write states names only entries it or an earlier write holds.
            if commit > self.last_index()? {
                return Err(self.fence(Cause::Invariant(
                    "a write stated a commit past the entries the log holds",
                )));
            }
            self.logged_commit = self.logged_commit.max(commit);
        }
        let Kind::Ready(number) = write.kind else {
            if let Kind::Start(imaged) = write.kind {
                // The log starts past what it compacted only now: a start refused changed
                // nothing, and is the owner's to ask again (`Replica::make_again`).
                self.imaged = Some(imaged);
                let start = self.node.store().log.bounds().map(|(start, _)| start.index);
                let held = start.and_then(|start| self.compactable_after(start));
                self.compactable =
                    held.map_err(|e| self.fence(Cause::Core(hyper_raft::Error::Storage(e))))?;
                if let Some(stall) = self.stall.as_mut() {
                    stall.freed = true;
                }
            }
            return self.tell_durable_commit();
        };
        self.emit(write.messages, out);
        let persisted = self.node.on_persist(number);
        let mut light = self.must(persisted)?;
        self.emit(light.take_messages(), out);
        self.tell_durable_commit()?;
        if let Some((first, last)) = light.committed_range() {
            self.apply_range(first, last, out)?;
        }
        self.tell_applied()
    }

    /// Tells the core the durable commit `C_d` where a write of the shell's own made it pass what
    /// the core read from `Ready`s' hard states (core step R-6, `RawNode::commit_durable`): a
    /// commit stated beyond its `Ready`'s (`commit = last`), a write of the commit alone, a
    /// compaction's, or the state machine's durable index. Answers then carry it.
    fn tell_durable_commit(&mut self) -> Result<(), ReplicaError> {
        let durable = self.durable_commit();
        if durable > self.node.durable_commit() {
            let told = self.node.commit_durable(durable);
            self.must(told)?;
        }
        Ok(())
    }

    /// A write was refused, changing nothing: it, and every write after it, waits to be made
    /// again once there is room.
    fn refused(&mut self, write: Out, fault: Fault) {
        // A store's hold its owner already met, before this refusal was taken (the owner learns
        // of a hold from the store as the write is submitted): the writes are made again at the
        // next drive, or the replica would wait for a release that has come.
        let released = fault == Fault::Held && self.node.store().log.held().is_none();
        if fault == Fault::Held {
            self.writes_made.held = self.writes_made.held.saturating_add(1);
        }
        let behind = self.writes.len();
        let stall = self.stall.get_or_insert_with(|| Stall {
            fault,
            behind,
            freed: released,
            ready: None,
            messages: Vec::new(),
        });
        if let Kind::Ready(number) = write.kind {
            stall.ready = Some(number);
            stall.messages.extend(write.messages);
        }
    }

    /// Makes the refused writes again, once every write out has been answered and the log has
    /// room: one write of everything the core holds not yet durable, laid out from the core
    /// (R-4 keeps it until its notice), with the latest hard state and the messages every
    /// refused write held. Its notice is the last refused `Ready`'s, which covers all of them.
    fn make_again(&mut self, now: u64, waker: &Waker) -> Result<(), ReplicaError> {
        let freed = self.stall.as_ref().is_some_and(|stall| stall.freed);
        if !freed || !self.writes.is_empty() || !self.node.store().log.room() {
            return Ok(());
        }
        let Some(stall) = self.stall.take() else {
            return Ok(());
        };
        let Some(number) = stall.ready else {
            // Only the shell's own writes were refused: a commit is stated again when it is
            // needed, and a compaction is the owner's to ask again.
            return Ok(());
        };
        // A snapshot no `Ready` gave yet is not installed in the state machine: written here,
        // the log would start past it (I8), and a member stopped before that `Ready` would not
        // open. Its `Ready` installs it and writes it, and the entries after it, which follow
        // it in the core; the commit stated meanwhile names only what earlier writes hold.
        let unissued = self
            .node
            .raft
            .log()
            .unstable()
            .unissued_snapshot()
            .is_some();
        let stated = self.stated_commit()?;
        let hard = HardState {
            commit: if unissued {
                self.issued.commit
            } else {
                stated.max(self.issued.commit)
            },
            ..self.issued
        };
        // Copied: the core lends what it holds and the store's write takes the store mutably,
        // both inside the core. A refusal for room is rare, and this its one copy.
        let unstable = self.node.raft.log().unstable();
        let start = unstable.snapshot().filter(|_| !unissued).map(|s| Point {
            index: proto::snapshot_index(s),
            term: proto::snapshot_term(s),
        });
        let held: Vec<Entry> = if unissued {
            Vec::new()
        } else {
            unstable.entries().to_vec()
        };
        // The fast track's proposals the refused writes held: the core keeps those of every
        // write issued until its notice, and every write out has been answered, so those it
        // keeps are the refused ones'. Fenced here before, a full log cost a fast group its
        // replica.
        let proposals: Vec<Entry> = self.node.issued_proposals().cloned().collect();
        // And what the core released through: written again with them, the release ends no
        // proposal they give.
        let released = Some(self.node.released()).filter(|through| *through > 0);
        let write = Write {
            start,
            entries: entries_of(&held, start),
            hard_state: Some(hard),
            proposals: &proposals,
            released,
        };
        let submitted = self.node.store_mut().log.submit(&write, waker);
        let state = self.submitted_state(submitted)?;
        self.issued = hard;
        self.writes.push_back(Out {
            kind: Kind::Ready(number),
            state,
            messages: stall.messages,
            commit: Some(hard.commit),
            vote: true,
            start: start.is_some(),
            submitted: now,
        });
        Ok(())
    }

    /// What a submission's outcome makes of the write: with the log, refused as nothing, or a
    /// failure that fences.
    fn submitted_state(&mut self, submitted: Result<(), Fault>) -> Result<State, ReplicaError> {
        match submitted {
            Ok(()) => Ok(State::Submitted),
            Err(fault) if fault.changed_nothing() => Ok(State::Refused(fault)),
            Err(fault) => Err(self.fence(Cause::Write(fault))),
        }
    }

    /// The reports kept while stalled.
    fn take_reports(&mut self) -> Result<(), ReplicaError> {
        while let Some((to, arrived)) = self.reports.pop() {
            match self.take_report(to, arrived) {
                Ok(()) | Err(ReplicaError::Refused(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// The commit a write laid out now states (I7): the core's, and for a member that decides
    /// alone the last entry, which its own write commits once durable (focal F17's
    /// `sole_commit`; `docs/durable.md` §4.1).
    fn stated_commit(&self) -> Result<u64, ReplicaError> {
        let log = self.node.raft.log();
        if self.decides_alone() {
            return log
                .last_index()
                .map_err(|_| ReplicaError::Fenced(Cause::Invariant("the log's last entry")));
        }
        Ok(log.committed())
    }

    /// The last entry this write or an earlier one holds, for a write that holds no entries of
    /// its own: the entries before the first no write was issued for (I7). While a snapshot
    /// waits to be written, nothing past what earlier writes stated.
    fn written_through(&self) -> u64 {
        let unstable = self.node.raft.log().unstable();
        if unstable.unissued_snapshot().is_some() {
            return self.issued.commit;
        }
        unstable.issued().saturating_sub(1)
    }

    /// Whether this member alone decides what commits: it leads, it is the one voter of a
    /// configuration that is not joint, no change waits to be applied, and its last entry is of
    /// its term, so its own write commits everything it holds.
    fn decides_alone(&self) -> bool {
        let raft = &self.node.raft;
        let configuration = raft.configuration();
        raft.state() == StateRole::Leader
            && !configuration.is_joint()
            && configuration.voters() == [raft.id()]
            && !raft.has_pending_conf()
            && raft.log().last_term().is_ok_and(|term| term == raft.term())
    }

    /// Whether an entry waits behind the fence that no write out states the commit of: the next
    /// write states it (§4.1).
    fn fence_needs_commit(&self) -> bool {
        self.behind_fence()
            .is_some_and(|(_, last)| !self.fence_covered(last))
    }

    /// Whether a write out states a commit through `index`.
    fn fence_covered(&self, index: u64) -> bool {
        self.durable_commit() >= index
            || self
                .writes
                .iter()
                .any(|write| write.state == State::Submitted && write.commit >= Some(index))
    }

    /// Whether the core has a `Ready` the replica may take now.
    fn ready_due(&self) -> bool {
        self.node.has_ready() && self.node.in_flight() < self.depth && self.has_slot()
    }

    /// Takes the core's next `Ready`, if the replica may (one a quantum): gives out what leaves
    /// at once, installs its snapshot, submits its write and issues it, then applies what it
    /// gives to apply. While entries wait behind the fence the core gives nothing more to apply
    /// (`RawNode::pause_apply`, core step R-6), `Ready`s go on, and the next one's write states
    /// the commit; when none is due, a write of the commit alone does.
    fn take_ready(
        &mut self,
        now: u64,
        waker: &Waker,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        let due = self.ready_due();
        if let Some((_, last)) = self.behind_fence()
            && !due
            && !self.fence_covered(last)
            && self.has_slot()
        {
            self.writes_made.fenced = self.writes_made.fenced.saturating_add(1);
            return self.commit_write(now, waker);
        }
        if !due {
            return Ok(());
        }
        let readied = self.node.ready_in_place();
        let mut ready = self.must(readied)?;
        // A commit that moved alone rides a later write (§4.1), unless the fence needs `C_d`
        // for an entry no write out states: the core then vouches for no commit from this one,
        // and holds the answers among its messages, not yet taken, to the durable commit.
        if !self.fence_needs_commit() {
            let deferred = self.node.defer_commit(&mut ready);
            self.must(deferred)?;
        }
        self.emit(ready.take_messages(), out);
        for read in ready.take_read_states() {
            self.reads.push_back((read.index, read.request_ctx));
        }
        let number = ready.number();
        let messages = ready.take_persisted_messages();
        let range = ready.committed_range();
        let carries = {
            let persist = self.node.to_persist();
            !persist.entries.is_empty()
                || persist
                    .snapshot
                    .is_some_and(|s| !proto::snapshot_is_empty(s))
        } || !ready.proposals().is_empty();
        let (hard, vote) = self.hard_of(ready.hard_state(), carries)?;
        let installs = self
            .node
            .raft
            .log()
            .unstable()
            .unissued_snapshot()
            .is_some_and(|s| !proto::snapshot_is_empty(s));
        let state = self.write_ready(&ready, hard, waker)?;
        if state == State::Empty {
            self.writes_made.empty = self.writes_made.empty.saturating_add(1);
        } else {
            self.writes_made.readies = self.writes_made.readies.saturating_add(1);
        }
        out.displaced.append(&mut ready.take_displaced());
        let issued = self.node.advance_issued(ready);
        self.must(issued)?;
        if let Some(hard) = hard {
            self.issued = hard;
        }
        self.writes.push_back(Out {
            kind: Kind::Ready(number),
            state,
            messages,
            commit: hard.map(|h| h.commit),
            vote,
            start: installs,
            submitted: now,
        });
        // What the `Ready` gave to apply comes before what its own notice gives.
        if let Some((first, last)) = range {
            self.apply_range(first, last, out)?;
        }
        self.tell_applied()?;
        self.take_answers_of_empty(now, out)
    }

    /// A write that held nothing goes no further than the queue: once every write before it is
    /// answered it is durable, which at the front it already is.
    fn take_answers_of_empty(
        &mut self,
        now: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        while self
            .writes
            .front()
            .is_some_and(|write| write.state == State::Empty)
            && self.stall.is_none()
        {
            let write = self
                .writes
                .pop_front()
                .ok_or(Cause::Invariant("a write vanished"))
                .map_err(|cause| self.fence(cause))?;
            self.durable(write, now, out)?;
        }
        Ok(())
    }

    /// The hard state a `Ready`'s write states: its term and vote, and the commit of
    /// [`Replica::stated_commit`]; none when nothing it must state moved since the last write.
    /// With it, whether the term or vote moved: the write's flush is then a vote's.
    ///
    /// A commit costs no write of its own (§4.1, focal F17): it is stated by a write that
    /// `carries` something else (entries, a start or proposals), or when the commit fence needs
    /// `C_d` for an entry no write out states. A commit that moved alone is volatile (Ongaro's
    /// thesis, figure 3.1); etcd's `MustSync` likewise syncs only for entries, a term or a vote.
    /// It rides the next write that carries anything, and the quiet write states it for a member
    /// that goes quiet.
    fn hard_of(
        &self,
        given: Option<&HardState>,
        carries: bool,
    ) -> Result<(Option<HardState>, bool), ReplicaError> {
        let (term, vote) = given.map_or((self.issued.term, self.issued.vote), |h| (h.term, h.vote));
        let commit = self.stated_commit()?.max(self.issued.commit);
        let moved = (term, vote) != (self.issued.term, self.issued.vote);
        let states_commit = commit > self.issued.commit && (carries || self.fence_needs_commit());
        let hard = (moved || states_commit).then_some(HardState { term, vote, commit });
        Ok((hard, moved))
    }

    /// Lays out and submits the write of the `Ready` taken: its snapshot installed in the state
    /// machine first (I8), then its start, entries, hard state and proposals.
    fn write_ready(
        &mut self,
        ready: &hyper_raft::Ready,
        hard: Option<HardState>,
        waker: &Waker,
    ) -> Result<State, ReplicaError> {
        let Self { node, machine, .. } = self;
        let persist = node.to_persist();
        let start = match persist.snapshot {
            Some(snapshot) if !proto::snapshot_is_empty(snapshot) => {
                Some(install(persist.store, machine, snapshot))
            }
            _ => None,
        };
        let start = match start {
            Some(Ok(point)) => Some(point),
            Some(Err(cause)) => return Err(self.fence(cause)),
            None => None,
        };
        let write = Write {
            start,
            entries: entries_of(persist.entries, start),
            hard_state: hard,
            proposals: ready.proposals(),
            released: ready.released(),
        };
        if write.is_empty() {
            return Ok(State::Empty);
        }
        let submitted = persist.store.log.submit(&write, waker);
        if let Some(point) = start {
            self.installed(point);
        }
        self.submitted_state(submitted)
    }

    /// A snapshot was installed at `point`: it replaces whatever page waited behind the fence,
    /// which it covers.
    fn installed(&mut self, point: Point) {
        self.applied = point;
        self.conf_index = point.index;
        // The log starts at the image's point, holding nothing applied past it.
        self.compactable = 0;
        self.imaged = self
            .node
            .store()
            .snapshot
            .as_ref()
            .map(|s| bytes(s.data.len()));
        if self.fence.take().is_some() {
            self.paged = false;
            self.node.resume_apply();
        }
    }

    /// A write of the commit alone, with the term and vote of the last write: the fence waits
    /// for it (`docs/durable.md` §4.1). Every entry the core gave to apply is committed and
    /// durable here, so the commit names only what the log holds once the write is durable.
    fn commit_write(&mut self, now: u64, waker: &Waker) -> Result<(), ReplicaError> {
        let hard = HardState {
            commit: self
                .stated_commit()?
                .min(self.written_through())
                .max(self.issued.commit),
            ..self.issued
        };
        let write = Write {
            hard_state: Some(hard),
            ..Write::default()
        };
        let submitted = self.node.store_mut().log.submit(&write, waker);
        let state = self.submitted_state(submitted)?;
        self.issued = hard;
        self.writes.push_back(Out {
            kind: Kind::Commit,
            state,
            messages: Vec::new(),
            commit: Some(hard.commit),
            vote: false,
            start: false,
            submitted: now,
        });
        Ok(())
    }

    /// A commit no write has stated while the applied index ran past the durable commit for a
    /// whole owner period is written then, one such write at a time, waited for by no one, so a
    /// member that stops reopens with what it applied (focal F17's `settle_commit`).
    fn quiet_commit(&mut self, now: u64, waker: &Waker) -> Result<(), ReplicaError> {
        let behind = self.writes.is_empty()
            && self.behind_fence().is_none()
            && self.applied.index > self.durable_commit();
        if !behind {
            self.quiet_since = None;
            return Ok(());
        }
        let since = *self.quiet_since.get_or_insert(now);
        let quiet = u64::try_from(self.quiet.as_nanos()).unwrap_or(u64::MAX);
        if now.saturating_sub(since) >= quiet && self.has_slot() {
            self.quiet_since = None;
            self.writes_made.quiet = self.writes_made.quiet.saturating_add(1);
            self.commit_write(now, waker)?;
        }
        Ok(())
    }

    /// Committed entries given to apply: applied now, or held behind the fence after those that
    /// wait already.
    fn apply_range(
        &mut self,
        first: u64,
        last: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        match self.fence {
            Some((held, through)) => {
                if through.checked_add(1) != Some(first) {
                    return Err(
                        self.fence(Cause::Invariant("committed entries given out of order"))
                    );
                }
                self.fence = Some((held, last));
                Ok(())
            }
            None => self.apply_from(first, last, out),
        }
    }

    /// What waits behind the fence is applied once the durable commit covers its first entry;
    /// what waits past a drive's page, at the next drive, a page of it.
    fn release_fence(&mut self, out: &mut Output<M::Answer>) -> Result<(), ReplicaError> {
        let Some((first, last)) = self.fence else {
            return Ok(());
        };
        if !self.paged && first > self.durable_commit() {
            return Ok(());
        }
        self.fence = None;
        self.paged = false;
        self.node.resume_apply();
        self.apply_from(first, last, out)?;
        self.tell_applied()
    }

    /// Applies `[first, last]` where the log holds it, in order, up to the first entry the fence
    /// holds; a change of configuration is applied by the core between walks.
    fn apply_from(
        &mut self,
        first: u64,
        last: u64,
        out: &mut Output<M::Answer>,
    ) -> Result<(), ReplicaError> {
        let mut next = first;
        while next <= last {
            match self.walk(next, last, out) {
                Stop::Done => break,
                Stop::Fence(index) => {
                    self.fence = Some((index, last));
                    self.paged = false;
                    self.node.pause_apply();
                    return Ok(());
                }
                Stop::Page(index) => {
                    self.fence = Some((index, last));
                    self.paged = true;
                    self.node.pause_apply();
                    return Ok(());
                }
                Stop::Change(point, change) => {
                    self.apply_change(point, &change)?;
                    next = point.index.saturating_add(1);
                }
                Stop::Failed(cause) => return Err(self.fence(cause)),
            }
        }
        if self.applied.index < last {
            return Err(self.fence(Cause::Invariant("the log held less than was committed")));
        }
        Ok(())
    }

    /// One walk of committed entries from `next` through `last`, applying each that needs
    /// nothing of the core and is not held by the fence: those the store holds read where it
    /// holds them, and a leader's own past it, given before its write is durable
    /// (`Config::apply_unpersisted`, core step R-6), where the core holds them.
    fn walk(&mut self, next: u64, last: u64, out: &mut Output<M::Answer>) -> Stop {
        let durable = self.durable_commit();
        let Self {
            node,
            machine,
            applied,
            compactable,
            page,
            drive_applied,
            ..
        } = self;
        let page = *page;
        let held = node.store();
        let unstable = node.raft.log().unstable().entries();
        let tail = unstable.first().map_or(u64::MAX, |e| e.index);
        let mut stop = Stop::Done;
        let mut step = |entry: EntryRef<'_>| -> bool {
            let fenced = entry.changes_configuration() || machine.acts_at_start(&entry);
            if fenced && entry.index > durable {
                stop = Stop::Fence(entry.index);
                return true;
            }
            // One page a drive, as one `Ready` a drive (§7's quantum), and at least one entry.
            let bytes = entry.encoded_bytes();
            if *drive_applied > 0 && drive_applied.saturating_add(bytes) > page {
                stop = Stop::Page(entry.index);
                return true;
            }
            *drive_applied = drive_applied.saturating_add(bytes);
            // Applied from here, or failed, which fences the replica: it counts afresh at open.
            *compactable = compactable.saturating_add(bytes);
            if entry.changes_configuration() {
                stop = match change_of(&entry) {
                    Ok(change) => Stop::Change(point_of(&entry), change),
                    Err(cause) => Stop::Failed(cause),
                };
                return true;
            }
            match machine.apply(&entry, &mut out.answers) {
                Ok(()) => {
                    *applied = point_of(&entry);
                    false
                }
                Err(fatal) => {
                    stop = Stop::Failed(Cause::Machine(fatal));
                    true
                }
            }
        };
        let end = last.saturating_add(1);
        let mut stopped = false;
        if next < tail {
            let walked = held
                .log
                .visit(next, end.min(tail), held.page, &mut |entry| {
                    stopped = step(entry);
                    stopped
                });
            if let Err(error) = walked {
                return Stop::Failed(Cause::Core(hyper_raft::Error::Storage(error)));
            }
        }
        if !stopped && end > tail {
            for entry in unstable.iter().filter(|e| e.index >= next && e.index < end) {
                if step(EntryRef::of(entry)) {
                    break;
                }
            }
        }
        stop
    }

    /// A committed change of configuration: the core applies it, and the state machine keeps
    /// the configuration it made. A change the core refuses is refused alike by every member,
    /// and leaves the configuration as it was.
    fn apply_change(&mut self, at: Point, change: &ConfChangeV2) -> Result<(), ReplicaError> {
        let applied = self.node.apply_conf_change(change);
        let configuration = match applied {
            Ok(configuration) => configuration,
            Err(error) if !error.is_fatal() => self.machine.configuration().clone(),
            Err(error) => return Err(self.fence(Cause::Core(error))),
        };
        let kept = self.machine.apply_change(at, change, &configuration);
        self.machine_did(kept)?;
        self.applied = at;
        self.conf_index = at.index;
        self.node.store_mut().configuration = configuration;
        Ok(())
    }

    /// Tells the core how far the state machine applied.
    fn tell_applied(&mut self) -> Result<(), ReplicaError> {
        if self.applied.index > self.told_applied {
            let told = self.node.advance_apply_to(self.applied.index);
            self.must(told)?;
            self.told_applied = self.applied.index;
        }
        Ok(())
    }

    /// Gives out `messages`. The emptied vector goes back to the core as its next queue
    /// (`RawNode::recycle_messages`), so the queue's room is not grown again for every ready taken
    /// ahead.
    fn emit(&mut self, mut messages: Vec<Message>, out: &mut Output<M::Answer>) {
        out.messages.append(&mut messages);
        self.node.recycle_messages(messages);
    }

    /// Reads confirmed and now applied far enough leave.
    fn release_reads(&mut self, out: &mut Output<M::Answer>) {
        let applied = self.applied.index;
        let mut at = 0;
        while let Some(&(index, _)) = self.reads.get(at) {
            if index > applied {
                at = at.saturating_add(1);
                continue;
            }
            if let Some((index, context)) = self.reads.swap_remove_back(at) {
                out.reads.push((context, index));
            }
        }
    }

    /// Prepares the snapshot the core serves to members behind the log's start: the state
    /// machine's image, with the configuration the group held at its point. Not the
    /// configuration applied since: a member installing it applies the changes after the
    /// image's point from the log, each once. Its bytes.
    fn prepare(&mut self) -> Result<u64, ReplicaError> {
        let mut data = Vec::new();
        let imaged = self.machine.image(&mut data);
        let (point, configuration) = self.machine_did(imaged)?;
        let snapshot = Snapshot {
            data,
            metadata: Some(SnapshotMetadata {
                conf_state: Some(configuration),
                index: point.index,
                term: point.term,
            }),
        };
        let imaged = bytes(snapshot.data.len());
        self.node.store_mut().snapshot = Some(snapshot);
        Ok(imaged)
    }

    /// Makes the state machine's applied state durable and lets the log free what is before it,
    /// keeping `keep` entries for members that lag; true when a compaction's start went out.
    /// The start never passes the state machine's durable index (I8) nor what the log holds
    /// durably (`docs/durable.md` §4.2). Allowed while stalled: a compaction is what frees room.
    pub fn compact(&mut self, keep: u64, now: u64, waker: &Waker) -> Result<bool, ReplicaError> {
        self.guarded(|r| {
            let persisted = r.machine.persist();
            r.machine_did(persisted)?;
            let durable = r.machine.durable().index;
            let bounds = r.node.store().log.bounds();
            let (start, last) =
                bounds.map_err(|e| r.fence(Cause::Core(hyper_raft::Error::Storage(e))))?;
            let index = durable.saturating_sub(keep).min(last);
            // A start a write out moves is not yet in the store's bounds: a compaction behind it
            // would move the start back. A snapshot's start refused for room is made again past
            // any compaction now: the state machine stands at it, so none passes it.
            let moving = r.writes.iter().any(|w| w.start);
            if index <= start.index || moving || !r.has_slot() {
                return Ok(false);
            }
            let term = r.node.store().log.term(index);
            let term = term.map_err(|e| r.fence(Cause::Core(hyper_raft::Error::Storage(e))))?;
            // The image a member behind the new start is sent, ready before the start moves.
            let imaged = r.prepare()?;
            // The log is compacted only through what it states committed, stated with it.
            let hard = (index > r.issued.commit).then_some(HardState {
                commit: index,
                ..r.issued
            });
            let write = Write {
                start: Some(Point { index, term }),
                hard_state: hard,
                ..Write::default()
            };
            let submitted = r.node.store_mut().log.submit(&write, waker);
            let state = r.submitted_state(submitted)?;
            if let Some(hard) = hard {
                r.issued = hard;
            }
            r.writes_made.starts = r.writes_made.starts.saturating_add(1);
            r.writes.push_back(Out {
                kind: Kind::Start(imaged),
                state,
                messages: Vec::new(),
                commit: hard.map(|h| h.commit),
                vote: false,
                start: true,
                submitted: now,
            });
            Ok(true)
        })
    }

    /// Whether the log is due to be compacted by `rule` ([`Compaction`], Ongaro's thesis §5.1.2),
    /// for the owner to compact with [`Replica::compact`], keeping nothing: the applied entries it
    /// holds weighed against the image it was last compacted to, and while this member leads, a
    /// member that lacks what it applied waited for as long as the log holds no more than twice
    /// the threshold. A machine that keeps no image is never due.
    pub fn compaction_due(&self, rule: Compaction) -> bool {
        let Some(image) = self.imaged else {
            return false;
        };
        // A start out is a compaction or an install under way, and [`Replica::compact`] waits
        // for it.
        if self.writes.iter().any(|w| w.start) {
            return false;
        }
        let raft = &self.node.raft;
        let id = raft.id();
        let applied = self.applied.index;
        let lagging = raft.state() == StateRole::Leader
            && raft
                .tracker()
                .iter()
                .any(|(member, progress)| member != id && progress.matched < applied);
        rule.due(self.compactable, image, lagging)
    }

    /// The bytes of the applied entries the log holds past its start, as the core counts an
    /// entry's: what [`Replica::compaction_due`] weighs. A compaction counts once its start is
    /// durable; an install, as it is written.
    pub fn compactable_bytes(&self) -> u64 {
        self.compactable
    }

    /// The bytes of the applied entries the log holds past `after`, as the core counts them:
    /// those the store holds, and a leader's own past them, applied before its write is durable
    /// (`Config::apply_unpersisted`), where the core holds them. An install not yet durable
    /// starts the log at its point, and what the store holds before it is not counted.
    fn compactable_after(&self, after: u64) -> Result<u64, StorageError> {
        let end = self.applied.index.saturating_add(1);
        let unstable = self.node.raft.log().unstable();
        let after = unstable
            .snapshot()
            .map_or(after, |s| after.max(proto::snapshot_index(s)));
        let first = after.saturating_add(1);
        let held = self.node.store();
        let unstable = unstable.entries();
        let tail = unstable.first().map_or(u64::MAX, |e| e.index);
        let mut bytes = 0u64;
        if first < end.min(tail) {
            held.log
                .visit(first, end.min(tail), held.page, &mut |entry| {
                    bytes = bytes.saturating_add(entry.encoded_bytes());
                    false
                })?;
        }
        for entry in unstable
            .iter()
            .filter(|e| e.index >= first && e.index < end)
        {
            bytes = bytes.saturating_add(EntryRef::of(entry).encoded_bytes());
        }
        Ok(bytes)
    }
}

/// The entries of a write: those given, from the first of them, or none past a snapshot's
/// start, which still says where the log ends.
fn entries_of(entries: &[Entry], start: Option<Point>) -> Option<Entries<'_>> {
    match (entries.first(), start) {
        (Some(first), _) => Some(Entries {
            first: first.index,
            entries,
        }),
        (None, Some(start)) => Some(Entries {
            first: start.index.saturating_add(1),
            entries,
        }),
        (None, None) => None,
    }
}

/// Installs `snapshot` in the state machine, durably, before the write that moves the log's
/// start to it (I8): its point.
fn install<L: LogStore, M: StateMachine>(
    held: &mut Held<L>,
    machine: &mut M,
    snapshot: &Snapshot,
) -> Result<Point, Cause> {
    let metadata = snapshot
        .metadata
        .as_ref()
        .ok_or(Cause::Invariant("a snapshot without metadata"))?;
    let point = Point {
        index: metadata.index,
        term: metadata.term,
    };
    let configuration = metadata.conf_state.clone().unwrap_or_default();
    machine
        .install(&snapshot.data, point, &configuration)
        .map_err(Cause::Machine)?;
    held.configuration = configuration;
    held.snapshot = Some(snapshot.clone());
    Ok(point)
}

/// What the log and the state machine hold when the member opens, made to agree before the core
/// reads them (`docs/durable.md` §4.3):
/// - the log may not start past the state machine (I8);
/// - a state machine at a point the log does not hold (a snapshot installed that the log never
///   recorded, or entries the log lost in its last frame) moves the log's start to that point,
///   with nothing past it, and the commit to it: the state machine reports its point's term, so
///   nothing is inferred from terms along the log;
/// - a state machine past the log's commit within its entries raises the commit to it.
fn repair_at_open<L: LogStore>(log: &mut L, durable: Point) -> Result<crate::StoreView, OpenError> {
    let view = log.view().map_err(OpenError::Log)?;
    if view.start.index > durable.index {
        return Err(OpenError::StartPastMachine {
            start: view.start.index,
            machine: durable.index,
        });
    }
    if durable.index == 0 {
        return Ok(view);
    }
    let holds = if durable.index == view.start.index {
        view.start.term == durable.term
    } else {
        durable.index <= view.last && log.term(durable.index).ok() == Some(durable.term)
    };
    let hard = view.hard_state;
    let write = if !holds {
        Write {
            start: Some(durable),
            entries: Some(Entries {
                first: durable.index.saturating_add(1),
                entries: &[],
            }),
            hard_state: Some(HardState {
                commit: hard.commit.max(durable.index),
                ..hard
            }),
            proposals: &[],
            released: None,
        }
    } else if durable.index > hard.commit {
        Write {
            hard_state: Some(HardState {
                commit: durable.index,
                ..hard
            }),
            ..Write::default()
        }
    } else {
        return Ok(view);
    };
    log.write_now(&write).map_err(OpenError::Log)?;
    log.view().map_err(OpenError::Log)
}
