//! The member as its owner drives it: a [`Ready`] says what to persist,
//! send and apply; [`RawNode::advance_issued`] says its write is on its way,
//! [`RawNode::on_persist`] that the writes through it are durable, and
//! [`RawNode::advance_apply_to`] how far the application applied.
//! [`RawNode::advance_append`] is the two at once, for an owner that finishes
//! each write before it takes the next.
//!
//! The member takes operations while writes are out (core step R-4,
//! `docs/durable.md` §2.1), as etcd's core does with asynchronous storage
//! writes and raft-rs's with `advance_append_async` and `on_persist_ready`.
//! What is not durable stays in the log's unstable part until it is
//! (etcd's rule), each `Ready` gives only what no earlier one gave, and a
//! notice of durability moves only what it can vouch for. The invariants of
//! `docs/durable.md` §3 the core keeps are enforced here and in the log:
//!
//! - **I1, promises; I2, acknowledgements.** A message of a member that does
//!   not lead leaves only after a write durable: those a `Ready` takes are
//!   [`Ready::persisted_messages`], sent by the owner once that write is
//!   durable, and that write holds, with every earlier one, all the member
//!   held when the messages were made. Those made by a notice of durability
//!   leave with it only when nothing is out or unwritten
//!   ([`RawNode::on_persist`]); otherwise they wait for the next `Ready`. A
//!   leader's leave at once only while the term and vote it leads in are
//!   durable: what it sends its members they persist for themselves
//!   (Ongaro's thesis §10.2.1), but its term is a promise (§3.8).
//! - **I3, self-count.** A leader counts itself toward a commit only as its
//!   own writes are durable (`Raft::on_persist_entries`; thesis §10.2.1).
//! - **I4, apply.** Entries are given to apply only once committed and
//!   durable here (`Log::next_entries_since`), but for a leader's own, which
//!   it may apply once committed (`Config::apply_unpersisted`,
//!   `docs/durable.md` §4.2).
//! - **I7, order.** Writes are durable in the order they were issued: a
//!   notice for one is a notice for every one before it, and the owner
//!   releases each write's messages after every earlier write's.
//!
//! The others (I5, a fenced apply; I6, answers; I8, a start the state
//! machine holds) are the durable shell's, hyper-durable's (`docs/durable.md`
//! §4). The core gives it what the fence needs (core step R-6): the durable
//! commit, `C_d`, which a durable `Ready`'s hard state states and the owner
//! states for every other write ([`RawNode::commit_durable`]), and which a
//! member's answers carry in place of its commit; and an apply pause
//! ([`RawNode::pause_apply`]), so that a shell holds at most one page of
//! committed entries behind the fence.
use std::collections::VecDeque;

use crate::wire::Record;

use crate::{
    NodeId,
    error::{Error, Result},
    proto::{
        self, ConfChange, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message,
        MessageType, Plan, Snapshot,
    },
    raft::{Config, Raft, SoftState, StateRole},
    read::ReadState,
    storage::Storage,
};

/// What became of a snapshot that was sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotStatus {
    /// It was sent: the leader waits for the member's answer and probes
    /// past it.
    Finish,
    /// It did not arrive: the leader waits a heartbeat and probes past what
    /// the member is known to hold.
    Failure,
}

/// What a member sends itself and the network never carries.
pub fn is_local(kind: MessageType) -> bool {
    matches!(
        kind,
        MessageType::MsgHup
            | MessageType::MsgBeat
            | MessageType::MsgUnreachable
            | MessageType::MsgSnapStatus
            | MessageType::MsgCheckQuorum
    )
}
/// What an answer says of its commit is held to what is durable when it
/// leaves (core step R-6, `docs/durable.md` §4.1): `MsgAppendResponse` and
/// `MsgHeartbeatResponse` state the commit the member knew when it made them
/// only as far as `durable`, the commit its storage states once they may
/// leave. A leader reads it as what the member reopens with
/// (`Progress::committed_index`), so a commit the member holds only in
/// memory is never said: one a leader made at a notice that its owner never
/// wrote (`LightReady::commit_index`; mantle `1c179e8`), or one a follower's
/// heartbeat moved while a write was out, whose answer a notice releases.
/// raft-rs states the commit it knows; the two say the same wherever the
/// owner writes every commit it is given. An answer was made with a commit
/// no greater than `committed`, the member's now, so where that is durable
/// there is nothing to hold and the messages are not walked.
///
/// What a leader sends at once or with a notice is not walked either: an
/// answer that states a commit is made only by a member that follows
/// (`Raft::handle_append_entries`, `handle_heartbeat`; a leader's answer to
/// an older term states none), and a member leads only in a later term than
/// it followed in. A leader sends at once only once its term and vote are
/// durable (I1), so the write that stated them was taken after every answer
/// it made as a follower, and took them all: none is left for it to send.
#[inline]
fn state_durable_commit(messages: &mut [Message], durable: u64, committed: u64) {
    if durable < committed {
        hold_answers(messages, durable);
    }
}
/// Holds every answer among `messages` to `durable`: the walk an owner that
/// writes every commit it is given never takes, kept off its path.
#[cold]
#[inline(never)]
fn hold_answers(messages: &mut [Message], durable: u64) {
    for message in messages {
        if matches!(
            message.msg_type,
            MessageType::MsgAppendResponse | MessageType::MsgHeartbeatResponse
        ) {
            message.commit = message.commit.min(durable);
        }
    }
}
fn is_answer(kind: MessageType) -> bool {
    matches!(
        kind,
        MessageType::MsgAppendResponse
            | MessageType::MsgRequestVoteResponse
            | MessageType::MsgHeartbeatResponse
            | MessageType::MsgUnreachable
            | MessageType::MsgRequestPreVoteResponse
    )
}

/// What follows once writes are durable ([`RawNode::on_persist`]).
#[derive(Debug, Default, PartialEq)]
pub struct LightReady {
    commit_index: Option<u64>,
    committed_entries: Vec<Entry>,
    /// What to apply, where storage holds it, for a `Ready` given in place.
    committed_range: Option<(u64, u64)>,
    messages: Vec<Message>,
}
impl LightReady {
    /// The commit, when it moved. It need not be durable to be acted on, and
    /// no `Ready`'s hard state states it: an owner that writes it says so
    /// once the write is durable ([`RawNode::commit_durable`]); until a write
    /// states it, the member's answers do not.
    pub fn commit_index(&self) -> Option<u64> {
        self.commit_index
    }
    /// Committed and durable here (or a leader's own, committed:
    /// `Config::apply_unpersisted`): to apply.
    pub fn committed_entries(&self) -> &[Entry] {
        &self.committed_entries
    }
    /// Takes the entries to apply, leaving none.
    pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.committed_entries)
    }
    /// Committed and durable here, to apply where storage holds them: the
    /// first and last index, for a `Ready` given in place
    /// ([`RawNode::ready_in_place`]), whose
    /// [`LightReady::committed_entries`] are none. A leader's own entries
    /// given before they are durable here (`Config::apply_unpersisted`) are
    /// past what storage holds, and read where the log holds them
    /// (`Log::next_range_since`).
    pub fn committed_range(&self) -> Option<(u64, u64)> {
        self.committed_range
    }
    /// To send now: made by what became durable, and none of them waits
    /// for a write still out (module docs, I1 and I2).
    pub fn messages(&self) -> &[Message] {
        &self.messages
    }
    /// Takes the messages to send, leaving none.
    pub fn take_messages(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.messages)
    }
}

/// What the member asks of its owner at once: what to persist, send and
/// apply. One is taken at a time, until [`RawNode::advance_issued`] says its
/// write is on its way; up to [`crate::Limits::readies_in_flight`] are out
/// until [`RawNode::on_persist`] says they are durable.
#[derive(Debug, Default, PartialEq)]
pub struct Ready {
    number: u64,
    soft_state: Option<SoftState>,
    hard_state: Option<HardState>,
    read_states: Vec<ReadState>,
    entries: Vec<Entry>,
    proposals: Vec<Entry>,
    released: Option<u64>,
    displaced: Vec<Entry>,
    snapshot: Option<Snapshot>,
    after_persisting: bool,
    light: LightReady,
    must_sync: bool,
    /// Its messages were taken: a commit can no longer be deferred, since
    /// the answers among them already left stating it
    /// ([`RawNode::defer_commit`]).
    messages_taken: bool,
}
impl Ready {
    /// Which `Ready` this is, counted from one since the member opened.
    pub fn number(&self) -> u64 {
        self.number
    }
    /// Who leads and what this member is, when either changed.
    pub fn soft_state(&self) -> Option<&SoftState> {
        self.soft_state.as_ref()
    }
    /// To persist, when it changed. Once this write is durable, the commit
    /// it states is the member's durable commit
    /// ([`RawNode::durable_commit`]), which its answers carry.
    pub fn hard_state(&self) -> Option<&HardState> {
        self.hard_state.as_ref()
    }
    /// Reads that may be served once their index is applied.
    pub fn read_states(&self) -> &[ReadState] {
        &self.read_states
    }
    /// Takes the reads, leaving none.
    pub fn take_read_states(&mut self) -> Vec<ReadState> {
        std::mem::take(&mut self.read_states)
    }
    /// To persist, replacing what storage holds from the first of them on:
    /// those no earlier `Ready` gave. A `Ready` given in place gives none
    /// here; its entries are read with [`RawNode::to_persist`].
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
    /// Takes the entries to persist, leaving none.
    pub fn take_entries(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.entries)
    }
    /// To persist beside the log: what this member approved by itself
    /// ([`crate::fast`]), a proposal given again at an index replacing the
    /// one storage holds there. What it holds it says only once this is
    /// durable, and storage gives it back when the member opens
    /// ([`crate::InitialState::proposals`]) until a `Ready` releases it
    /// ([`Ready::released`]), whatever its log holds.
    pub fn proposals(&self) -> &[Entry] {
        &self.proposals
    }
    /// The index through which what this member approved by itself is held
    /// no more: it knows the log committed through it by a classic quorum
    /// (`docs/raft.md` §3.5). Storage may drop the proposals it holds at or
    /// below it, before it takes this `Ready`'s; one that keeps them gives
    /// them back at the next open, and the member holds them until it learns
    /// the index again, which costs room and never safety. None when it did
    /// not move, or when the `Ready` persists nothing else: a release waits
    /// for a write the member makes anyway. Storage must not drop a proposal
    /// for any other reason: not when its log reaches the index, not at a
    /// snapshot or a compaction.
    pub fn released(&self) -> Option<u64> {
        self.released
    }
    /// What was proposed here by the fast track and another entry took the
    /// index of: its proposer proposes it again.
    pub fn displaced(&self) -> &[Entry] {
        &self.displaced
    }
    /// Takes the displaced entries, leaving none.
    pub fn take_displaced(&mut self) -> Vec<Entry> {
        std::mem::take(&mut self.displaced)
    }
    /// To persist before the entries: the log begins again after it. A
    /// `Ready` given in place gives none here; its snapshot is read with
    /// [`RawNode::to_persist`].
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }
    /// Committed and durable here (or a leader's own, committed:
    /// `Config::apply_unpersisted`): to apply.
    pub fn committed_entries(&self) -> &[Entry] {
        self.light.committed_entries()
    }
    /// Takes the entries to apply, leaving none.
    pub fn take_committed_entries(&mut self) -> Vec<Entry> {
        self.light.take_committed_entries()
    }
    /// Committed and durable here, to apply where storage holds them, for a
    /// `Ready` given in place ([`LightReady::committed_range`]).
    pub fn committed_range(&self) -> Option<(u64, u64)> {
        self.light.committed_range()
    }
    /// To send at once: a leader's, while the term and vote it leads in
    /// are durable. Lent, not taken, so a commit may still be deferred
    /// ([`RawNode::defer_commit`]) after reading them, and the answers among
    /// them are then restated with the durable commit: a caller that sends
    /// what it reads here, rather than what it takes, defers first.
    pub fn messages(&self) -> &[Message] {
        if self.after_persisting {
            &[]
        } else {
            self.light.messages()
        }
    }
    /// Takes the messages to send at once, leaving none.
    pub fn take_messages(&mut self) -> Vec<Message> {
        self.messages_taken = true;
        if self.after_persisting {
            Vec::new()
        } else {
            self.light.take_messages()
        }
    }
    /// To send once what this `Ready` persists is durable, and every write
    /// issued before it: every message of a member that does not lead, and
    /// a leader's while its term or vote is not durable yet. Lent, not taken,
    /// as [`Ready::messages`] are: a caller that sends what it reads here
    /// defers a commit ([`RawNode::defer_commit`]) before it reads them.
    pub fn persisted_messages(&self) -> &[Message] {
        if self.after_persisting {
            self.light.messages()
        } else {
            &[]
        }
    }
    /// Takes the messages to send once this `Ready` is durable, leaving
    /// none.
    pub fn take_persisted_messages(&mut self) -> Vec<Message> {
        self.messages_taken = true;
        if self.after_persisting {
            self.light.take_messages()
        } else {
            Vec::new()
        }
    }
    /// False when nothing but the commit is to persist: that write need
    /// not be waited for.
    pub fn must_sync(&self) -> bool {
        self.must_sync
    }
}

/// What a `Ready` given in place asks to persist, where the member holds
/// it, beside the storage it goes to ([`RawNode::to_persist`]).
#[derive(Debug)]
pub struct ToPersist<'a, S> {
    /// The storage, to write to.
    pub store: &'a mut S,
    /// The snapshot to persist first, if there is one.
    pub snapshot: Option<&'a Snapshot>,
    /// The entries to persist, replacing what storage holds from the first
    /// of them on.
    pub entries: &'a [Entry],
}

/// What a `Ready` gave to persist, given up by the member once it is durable
/// ([`RawNode::advance_append_keeping`]).
#[derive(Debug, Default, PartialEq)]
pub struct Kept {
    /// The snapshot, if the `Ready` gave one.
    pub snapshot: Option<Snapshot>,
    /// The entries.
    pub entries: Vec<Entry>,
}

/// The `Ready` taken and not yet issued.
#[derive(Clone, Copy, Debug)]
struct Taken {
    number: u64,
    /// Given in place: its entries are read where the member holds them,
    /// and what it gives to apply where storage holds it.
    in_place: bool,
    /// The release given before this `Ready`'s: what [`RawNode::defer_commit`]
    /// takes it back to.
    released_before: u64,
}

/// What an issued [`Ready`]'s write vouches for once it is durable. Nothing
/// changes the member between a `Ready`'s taking and its issue, so it is
/// read at the issue.
#[derive(Clone, Copy, Debug)]
struct Mark {
    number: u64,
    /// The member's term when it was issued. A notice that its write is
    /// durable, heard in a later term, makes nothing durable: another
    /// leader's entries may have replaced what it wrote, and then been
    /// replaced by the same entries again, by a write still out (etcd's
    /// guard against this ABA, `newStorageAppendRespMsg`). Every change of
    /// term makes a write of its own, whose notice does.
    term: u64,
    /// The last entry not yet durable when it was issued. Each entry before
    /// it was given by this `Ready` or an earlier one, and writes are
    /// durable in the order they were issued (I7), so all of them are
    /// durable once this write is.
    last_entry: Option<(u64, u64)>,
    /// The snapshot not yet durable when it was issued, by its index.
    snapshot: Option<u64>,
    /// The term and vote its hard state stated, when it changed them.
    vote: Option<(u64, u64)>,
    /// The commit its hard state stated, or 0 when it gave none: durable
    /// with it, whatever the term the notice is heard in, for it names only
    /// entries this write or an earlier one holds, and a committed entry is
    /// never replaced. The durable commit only rises
    /// (`Raft::commit_durable`), so 0 states nothing.
    commit: u64,
    in_place: bool,
}

/// A `Ready` whose write is out.
#[derive(Clone, Debug)]
struct Given {
    mark: Mark,
    /// What this member approved by itself and gave, moved from the `Ready`
    /// when its write was issued.
    proposals: Vec<Entry>,
    /// What its notice made durable, between finding it and acting on it.
    stable: Stable,
}

/// What a notice made durable of one write: its snapshot, by index, and its
/// last entry.
#[derive(Clone, Copy, Debug, Default)]
struct Stable {
    snapshot: Option<u64>,
    entries: Option<(u64, u64)>,
}

/// A member as its owner drives it: operations in, [`Ready`]s out, and
/// notices of what became durable in.
#[derive(Clone)]
pub struct RawNode<S> {
    /// The state machine itself.
    pub raft: Raft<S>,
    previous_soft: SoftState,
    /// The hard state the last `Ready` gave, or the one the member opened
    /// with.
    previous_hard: HardState,
    number: u64,
    /// The `Ready` taken and not yet issued: one at most, and the member
    /// takes no operation while it is out.
    taken: Option<Taken>,
    /// The `Ready`s whose writes are out, oldest first; at most
    /// [`crate::Limits::readies_in_flight`].
    issued: VecDeque<Given>,
    /// The term and vote storage holds durably: as the member opened, then
    /// as the last write known durable stated them.
    durable_vote: (u64, u64),
    /// What was given to apply reaches this index.
    commit_since: u64,
    /// The owner holds what it was given to apply and is given no more
    /// ([`RawNode::pause_apply`]).
    apply_paused: bool,
    /// The index through which a `Ready` released what the member approved
    /// by itself.
    released: u64,
}

impl<S> RawNode<S> {
    /// Plants `mutant` in this member, or takes the one planted out: a defect the tests show the
    /// oracles catch (`crate::mutant`; the `mutants` feature, which no consumer enables).
    #[cfg(feature = "mutants")]
    pub fn plant(&mut self, mutant: Option<crate::Mutant>) {
        self.raft.mutant = mutant;
    }

    /// Whether a `Ready` with `messages` leaves at once for the planted defect that sends a vote
    /// before it is durable ([`crate::Mutant::VoteBeforeDurable`]).
    fn vote_leaves_early(&self, messages: &[Message]) -> bool {
        self.raft.planted(crate::Mutant::VoteBeforeDurable)
            && messages
                .iter()
                .any(|message| message.msg_type == MessageType::MsgRequestVoteResponse)
    }
}

impl<S: Storage> RawNode<S> {
    /// The member `config` names, opened on what `store` holds.
    pub fn new(config: &Config, store: S) -> Result<Self> {
        let raft = Raft::new(config, store)?;
        let mut issued = VecDeque::new();
        // Reserved once: a write issued never grows the queue.
        issued
            .try_reserve_exact(config.limits.readies_in_flight)
            .map_err(|_| Error::Capacity("readies in flight"))?;
        Ok(Self {
            previous_soft: raft.soft_state(),
            previous_hard: raft.hard_state(),
            durable_vote: (raft.term(), raft.vote()),
            released: raft.classic,
            raft,
            number: 0,
            taken: None,
            issued,
            commit_since: config.applied,
            apply_paused: false,
        })
    }
    /// The index through which the `Ready`s taken released what this member approved by itself
    /// ([`Ready::released`]): an owner that writes again what a refused write held writes this
    /// release with it.
    pub fn released(&self) -> u64 {
        self.released
    }
    /// The release a `Ready` that `persists` something carries: the classic commit this member
    /// knows, if it moved since the last one given.
    fn release_with(&mut self, persists: bool) -> Option<u64> {
        let classic = self.raft.classic;
        if classic > self.released && persists {
            self.released = classic;
            return Some(classic);
        }
        None
    }
    /// The storage the member reads.
    pub fn store(&self) -> &S {
        self.raft.store()
    }
    /// The storage the member reads, for its owner to write.
    pub fn store_mut(&mut self) -> &mut S {
        self.raft.store_mut()
    }
    /// Whether a `Ready` is taken, or a write of one is out and not yet
    /// known durable.
    pub fn outstanding(&self) -> bool {
        self.taken.is_some() || !self.issued.is_empty()
    }
    /// How many `Ready`s' writes are out and not yet known durable.
    pub fn in_flight(&self) -> usize {
        self.issued.len()
    }
    /// The commit this member's storage states durably (`C_d`,
    /// `docs/durable.md` §4.1): the commit the hard state of the last durable
    /// `Ready` stated, or a later one its owner made durable
    /// ([`RawNode::commit_durable`]). A member's `MsgAppendResponse` and
    /// `MsgHeartbeatResponse` state no commit beyond what is durable when
    /// they leave: those a `Ready`'s write holds, beyond its own hard
    /// state's; those sent at once or with a notice, beyond this. That
    /// rests on the owner's part of the contract: it writes a `Ready`'s hard
    /// state as given, its commit with it, or tells the core it does not
    /// ([`RawNode::defer_commit`]) before it takes the `Ready`'s messages.
    pub fn durable_commit(&self) -> u64 {
        self.raft.durable_commit()
    }
    /// A write the owner made outside a `Ready`'s hard state is durable and
    /// states `commit` (core step R-6): the commit a [`LightReady`] gave,
    /// written with the next record or alone; a write of the hard state
    /// alone that the commit fence asked for; the commit a write stated
    /// ahead of its `Ready`'s (`commit = last` where the member alone
    /// decides); or, folded in, the index the state machine holds durably.
    /// A durable `Ready`'s own hard state needs no word: its notice says it.
    /// The durable commit never goes back; a commit beyond the log is
    /// refused, fatally, for no write can state it.
    pub fn commit_durable(&mut self, commit: u64) -> Result<()> {
        if commit > self.raft.log().last_index()? {
            return Err(Error::Invariant("a durable commit beyond the log"));
        }
        self.raft.commit_durable(commit);
        Ok(())
    }
    /// The owner is done with the messages a [`Ready`] or [`LightReady`]
    /// gave: their vector, emptied, becomes the member's queue of messages
    /// or a spare for a later one, so its room is not grown again. The
    /// member keeps a spare for each `Ready` whose write may be out
    /// ([`crate::Limits::readies_in_flight`]). Optional; a vector with no
    /// more room than the spares the member keeps already, or more than
    /// [`crate::Limits::pending_messages`] slots, is dropped.
    pub fn recycle_messages(&mut self, emptied: Vec<Message>) {
        let limits = &self.raft.config.limits;
        let (most, keep) = (limits.pending_messages, limits.readies_in_flight);
        self.raft.msgs.recycle(emptied, most, keep);
    }
    /// The owner holds what it was given to apply and takes no more until
    /// [`RawNode::resume_apply`] (core step R-6, an apply pause as etcd's
    /// `applyingEntsPaused`): `Ready`s and notices give nothing to apply
    /// meanwhile, and everything else as before. A shell whose commit fence
    /// holds a change of configuration behind the durable commit
    /// (`docs/durable.md` §4.1) holds that `Ready`'s committed page and no
    /// more. It applies what it holds as [`RawNode::advance_apply_to`] says,
    /// and a snapshot given meanwhile replaces it.
    pub fn pause_apply(&mut self) {
        self.apply_paused = true;
    }
    /// The owner takes what is committed again ([`RawNode::pause_apply`]).
    pub fn resume_apply(&mut self) {
        self.apply_paused = false;
    }
    /// Whether the owner holds what it was given to apply and takes no more.
    pub fn apply_paused(&self) -> bool {
        self.apply_paused
    }
    /// An operation on the member. The priority in force is settled before
    /// it and after, never within. While a `Ready` is taken and not yet
    /// issued the member does not change.
    // Inlined into each operation, as it was before R-6 grew the member:
    //  found it out of line on the proposal path.
    #[inline]
    fn operate<T>(&mut self, operation: impl FnOnce(&mut Raft<S>) -> Result<T>) -> Result<T> {
        if self.taken.is_some() {
            return Err(Error::Invariant(
                "an operation while a ready is taken and not issued",
            ));
        }
        self.raft.settle_priority();
        let outcome = operation(&mut self.raft);
        self.raft.settle_priority();
        outcome
    }
    /// What this member does with an append ahead of a hole, from the next
    /// append on ([`Raft::set_ahead`]).
    pub fn set_ahead(&mut self, ahead: crate::Ahead) {
        self.raft.set_ahead(ahead);
    }
    /// The priority this member's elections are judged by from the next
    /// operation on ([`Config::priority`]).
    pub fn set_priority(&mut self, priority: i64) {
        self.raft.set_priority(priority);
    }
    /// What the path to `member` carries before it answers
    /// ([`Raft::set_inflight_bytes`]).
    pub fn set_inflight_bytes(&mut self, member: u64, bytes: u64) -> bool {
        self.raft.set_inflight_bytes(member, bytes)
    }
    /// Where catching up the learner `member` stands, for its owner to
    /// promote it once ready ([`Raft::catch_up`], `crate::CatchUp`).
    pub fn catch_up(&mut self, member: NodeId) -> Result<crate::CatchUp> {
        self.operate(|raft| raft.catch_up(member))
    }
    /// One tick of time has passed. True when the member acted on it: it
    /// campaigned, checked its quorum or sent heartbeats.
    pub fn tick(&mut self) -> Result<bool> {
        self.operate(Raft::tick)
    }
    /// The owner's detectors suspect `member`'s node (elections by
    /// suspicion, `crate::raft::Elections::Suspicion`; refused on ticks).
    pub fn suspect(&mut self, member: NodeId) -> Result<()> {
        self.operate(|raft| raft.suspect(member))
    }
    /// The owner's detectors trust `member`'s node again.
    pub fn trust(&mut self, member: NodeId) -> Result<()> {
        self.operate(|raft| raft.trust(member))
    }
    /// The owner's detectors saw `member`'s node start again
    /// ([`Raft::restarted`]): trusted, and leading nothing it led before.
    pub fn restarted(&mut self, member: NodeId) -> Result<()> {
        self.operate(|raft| raft.restarted(member))
    }
    /// What the owner's measurements give this group's elections
    /// ([`crate::Timing`]).
    pub fn set_timing(&mut self, timing: crate::Timing) -> Result<()> {
        self.raft.set_timing(timing)
    }
    /// The last index this member, leading, may take from the fast track
    /// at ([`Raft::cap_takes`]).
    pub fn cap_takes(&mut self, through: Option<u64>) -> Result<()> {
        self.operate(|raft| raft.cap_takes(through))
    }
    /// The owner holds this member's campaigns, or lets them go
    /// ([`Raft::hold_campaigns`]).
    pub fn hold_campaigns(&mut self, held: bool) -> Result<()> {
        self.raft.hold_campaigns(held)
    }
    /// The owner's clock reads `now`, nanoseconds ([`Raft::wake`]): called
    /// after each call the owner makes, once the `Ready` it took is issued,
    /// and at [`RawNode::deadline`].
    pub fn wake(&mut self, now: u64) -> Result<bool> {
        self.operate(|raft| raft.wake(now))
    }
    /// When the member is next to be woken, on the owner's clock
    /// ([`Raft::deadline`]).
    pub fn deadline(&self) -> Option<u64> {
        self.raft.deadline()
    }
    /// Campaigns now, by pre-vote when the group runs it.
    pub fn campaign(&mut self) -> Result<()> {
        self.operate(|raft| raft.step(proto::message(0, MessageType::MsgHup)))
    }
    /// Proposes an entry stating `data`, which the leader appends.
    pub fn propose(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<()> {
        self.operate(|raft| {
            let mut message = proto::message(0, MessageType::MsgPropose);
            message.from = raft.id();
            message
                .entries
                .try_reserve_exact(1)
                .map_err(|_| Error::Capacity("a proposal"))?;
            message.entries.push(Entry {
                data,
                context,
                ..Entry::default()
            });
            raft.step(message)
        })
    }
    /// Proposes by the fast track ([`crate::fast`]); the index proposed
    /// for.
    pub fn propose_fast(&mut self, context: Vec<u8>, data: Vec<u8>) -> Result<u64> {
        self.operate(|raft| raft.propose_fast(context, data))
    }
    /// Proposes a change of the configuration; one that could not be read
    /// when applied is refused here.
    pub fn propose_conf_change(&mut self, context: Vec<u8>, change: &ConfChangeV2) -> Result<()> {
        // What could not be read when it is applied is not proposed.
        Plan::of(change)?;
        // The empty change, leaving the joint configuration, is written as no data, as the
        // leader writes its own leave: one change, one encoding.
        let data = if *change == ConfChangeV2::default() {
            Vec::new()
        } else {
            change.encode_to_vec()
        };
        self.operate(|raft| {
            let mut message = proto::message(0, MessageType::MsgPropose);
            message
                .entries
                .try_reserve_exact(1)
                .map_err(|_| Error::Capacity("a proposal"))?;
            message.entries.push(Entry {
                entry_type: EntryType::EntryConfChangeV2,
                data,
                context,
                ..Entry::default()
            });
            raft.step(message)
        })
    }
    /// A committed change is applied. One the application decides not to
    /// apply is not given here.
    pub fn apply_conf_change(&mut self, change: &ConfChangeV2) -> Result<ConfState> {
        let plan = Plan::of(change)?;
        self.raft.settle_priority();
        let outcome = self.raft.apply_conf_change(&plan);
        self.raft.settle_priority();
        outcome
    }
    /// A committed change in the older encoding is applied.
    pub fn apply_conf_change_v1(&mut self, change: &ConfChange) -> Result<ConfState> {
        let plan = Plan::of(&proto::joint(change))?;
        self.raft.settle_priority();
        let outcome = self.raft.apply_conf_change(&plan);
        self.raft.settle_priority();
        outcome
    }
    /// A message from the network.
    pub fn step(&mut self, message: Message) -> Result<()> {
        if message.msg_type == crate::fast::FAST_PROPOSE
            || message.msg_type == crate::fast::FAST_VOTE
        {
            return self.operate(|raft| raft.step(message));
        }
        let kind = message.msg_type;
        if is_local(kind) {
            return Err(Error::StepLocalMessage);
        }
        if is_answer(kind) && self.raft.tracker().get(message.from).is_none() {
            return Err(Error::StepPeerNotFound);
        }
        self.operate(|raft| raft.step(message))
    }
    /// A leader sends its heartbeats now.
    pub fn ping(&mut self) -> Result<()> {
        self.operate(Raft::ping)
    }
    /// What a member asks of itself on its owner's word. A refusal is no
    /// one's to hear.
    fn tell(&mut self, message: Message) -> Result<()> {
        match self.operate(|raft| raft.step(message)) {
            Err(error) if error.is_fatal() => Err(error),
            _ => Ok(()),
        }
    }
    /// The last message to `member` did not arrive.
    pub fn report_unreachable(&mut self, member: NodeId) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgUnreachable);
        message.from = member;
        self.tell(message)
    }
    /// What became of the snapshot sent to `member`.
    pub fn report_snapshot(&mut self, member: NodeId, status: SnapshotStatus) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgSnapStatus);
        message.from = member;
        message.reject = status == SnapshotStatus::Failure;
        self.tell(message)
    }
    /// Asks the leader for a snapshot that reaches this member's log.
    pub fn request_snapshot(&mut self) -> Result<()> {
        self.operate(Raft::request_snapshot)
    }
    /// `transferee` shall lead.
    pub fn transfer_leader(&mut self, transferee: NodeId) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgTransferLeader);
        message.from = transferee;
        self.tell(message)
    }
    /// Asks at which index a read may be served; the answer comes in a
    /// `Ready` with the same `context`. A leader that has not committed in
    /// its term holds the read until it has; a member with no leader to ask
    /// refuses it ([`Error::ReadDropped`]), so the owner answers it at once.
    pub fn read_index(&mut self, context: Vec<u8>) -> Result<()> {
        let mut message = proto::message(0, MessageType::MsgReadIndex);
        message
            .entries
            .try_reserve_exact(1)
            .map_err(|_| Error::Capacity("a read"))?;
        message.entries.push(Entry {
            data: context,
            ..Entry::default()
        });
        // Every refusal reaches the owner: an `Ok` for a read that went
        // nowhere left its owner to wait out a deadline.
        self.operate(|raft| raft.step(message))
    }

    /// What there is to apply, and, when `release`, the messages to send:
    /// those of a `Ready`, or those a notice may send at once
    /// ([`RawNode::releases_now`]).
    fn light(&mut self, in_place: bool, release: bool) -> Result<LightReady> {
        if self.apply_paused {
            return Ok(self.light_paused(release));
        }
        if in_place {
            return self.light_in_place(release);
        }
        let committed_entries = self
            .raft
            .log()
            .next_entries_since(self.commit_since, self.raft.committed_bytes_per_ready())?;
        self.raft.reduce_uncommitted(&committed_entries);
        if let Some(last) = committed_entries.last() {
            self.given_through(last.index)?;
        }
        Ok(LightReady {
            commit_index: None,
            committed_entries,
            committed_range: None,
            messages: self.messages(release),
        })
    }
    /// As [`RawNode::light`] while the owner holds what it was given to
    /// apply: the messages alone ([`RawNode::pause_apply`]).
    #[cold]
    #[inline(never)]
    fn light_paused(&mut self, release: bool) -> LightReady {
        LightReady {
            messages: self.messages(release),
            ..LightReady::default()
        }
    }
    /// As [`RawNode::light`], giving what to apply as the range storage
    /// holds it, copied nowhere.
    fn light_in_place(&mut self, release: bool) -> Result<LightReady> {
        let range = self.raft.log().next_range_since(
            self.commit_since,
            self.raft.committed_bytes_per_ready(),
            self.raft.leader_tail(),
        )?;
        let committed_range = match range {
            Some(range) => {
                self.raft.reduce_uncommitted_bytes(range.data_above);
                self.given_through(range.last)?;
                Some((range.first, range.last))
            }
            None => None,
        };
        Ok(LightReady {
            commit_index: None,
            committed_entries: Vec::new(),
            committed_range,
            messages: self.messages(release),
        })
    }
    fn messages(&mut self, release: bool) -> Vec<Message> {
        if release {
            self.raft.msgs.take()
        } else {
            Vec::new()
        }
    }
    /// Entries through `last` were given to apply.
    fn given_through(&mut self, last: u64) -> Result<()> {
        if self.commit_since >= last {
            return Err(Error::Invariant("entries given to apply twice"));
        }
        self.commit_since = last;
        Ok(())
    }
    /// Whether every counter the member trusts for what it holds says what
    /// a walk says ([`Raft::check_accounting`]).
    pub fn check_accounting(&self) -> Result<()> {
        self.raft.check_accounting()
    }
    /// Whether the term and vote the member holds are durable.
    #[inline]
    fn vote_durable(&self) -> bool {
        self.durable_vote == (self.raft.term(), self.raft.vote())
    }
    /// Whether what a notice of durability made may be sent at once (I1,
    /// I2): a leader's while its term and vote are durable; any member's
    /// once everything it holds is, with no write out and nothing left to
    /// write. Otherwise it waits in the queue for the next `Ready`, and
    /// leaves as that `Ready`'s messages do.
    #[inline]
    fn releases_now(&self) -> bool {
        let raft = &self.raft;
        if !self.vote_durable() {
            return false;
        }
        if raft.state() == StateRole::Leader {
            return true;
        }
        let unstable = raft.log().unstable();
        self.taken.is_none()
            && self.issued.is_empty()
            && !unstable.has_unissued()
            && unstable.unissued_snapshot().is_none()
            && !raft.held.has_unissued()
    }
    /// Whether [`RawNode::ready`] has anything to give.
    pub fn has_ready(&self) -> bool {
        let raft = &self.raft;
        let unstable = raft.log().unstable();
        !raft.msgs.is_empty()
            || raft.reads_unasked()
            || raft.soft_state() != self.previous_soft
            || raft.hard_state() != self.previous_hard
            || !raft.read_states.is_empty()
            || !raft.displaced.is_empty()
            || raft.held.has_unissued()
            || unstable.has_unissued()
            || unstable
                .unissued_snapshot()
                .is_some_and(|snapshot| !proto::snapshot_is_empty(snapshot))
            || (!self.apply_paused
                && raft
                    .log()
                    .has_next_entries_since(self.commit_since)
                    .unwrap_or(false))
    }
    /// What there is to do. Nothing else is asked of the member until it is
    /// issued ([`RawNode::advance_issued`]); it may be taken while earlier
    /// `Ready`s' writes are out, up to [`crate::Limits::readies_in_flight`],
    /// and is refused for capacity beyond.
    pub fn ready(&mut self) -> Result<Ready> {
        self.ready_given(false)
    }
    /// What there is to do, as [`RawNode::ready`] says it, with nothing
    /// copied that the owner can read where it is:
    /// - the entries to persist are read with [`RawNode::to_persist`] while
    ///   the `Ready` is taken, and issued before anything else is asked of
    ///   the member; [`Ready::entries`] are none;
    /// - what is committed is given as the range storage holds it
    ///   ([`Ready::committed_range`], [`LightReady::committed_range`]),
    ///   chosen by the rule [`Log::next_entries_since`] pages by, and
    ///   [`Ready::committed_entries`] are none.
    ///
    /// An owner that writes entries out (to a file, to a device) and applies
    /// what it holds copies nothing; the member's decisions are those of
    /// [`RawNode::ready`] exactly.
    ///
    /// [`Log::next_entries_since`]: crate::log::Log::next_entries_since
    pub fn ready_in_place(&mut self) -> Result<Ready> {
        self.ready_given(true)
    }
    /// The storage, and what the `Ready` that is taken gives to persist,
    /// where the member holds it ([`RawNode::ready_in_place`]): the snapshot
    /// first, if there is one, then the entries, those no earlier `Ready`
    /// gave.
    pub fn to_persist(&mut self) -> ToPersist<'_, S> {
        let log = &mut self.raft.log;
        ToPersist {
            store: &mut log.store,
            snapshot: log.unstable.unissued_snapshot(),
            entries: log.unstable.unissued(),
        }
    }
    fn ready_given(&mut self, in_place: bool) -> Result<Ready> {
        if self.taken.is_some() {
            return Err(Error::Invariant(
                "a ready asked for while one is taken and not issued",
            ));
        }
        if self.issued.len() >= self.raft.config().limits.readies_in_flight {
            return Err(Error::Capacity("readies in flight"));
        }
        let number = self
            .number
            .checked_add(1)
            .ok_or(Error::Capacity("readies"))?;
        // One round for the reads asked since the last (`Raft::ask_reads`): it
        // leaves with this `Ready`. A round refused is asked for again by
        // the next `Ready`, and by the heartbeat the leader's clock sends.
        self.raft.ask_reads()?;
        let mut ready = Ready {
            number,
            ..Ready::default()
        };
        let unstable = self.raft.log().unstable();
        let new_entries = unstable.has_unissued();
        // Everything that can refuse does before the member is changed.
        let mut new_snapshot = None;
        if let Some(snapshot) = unstable.unissued_snapshot() {
            let index = proto::snapshot_index(snapshot);
            if self.commit_since > index {
                return Err(Error::Invariant(
                    "a snapshot behind what was given to apply",
                ));
            }
            // Entries after a snapshot are not durable before it is, so
            // none is given to apply with it.
            if self.raft.log().has_next_entries_since(index)? {
                return Err(Error::Invariant(
                    "a snapshot with entries to apply after it",
                ));
            }
            if !in_place {
                ready.snapshot = Some(crate::log::copy_snapshot(snapshot)?);
            }
            new_snapshot = Some(index);
            ready.must_sync = true;
        }
        if !in_place {
            crate::log::copy_entries(unstable.unissued(), &mut ready.entries)?;
        }
        self.raft.unissued_proposals(&mut ready.proposals)?;

        if let Some(index) = new_snapshot {
            self.commit_since = index;
        }
        ready.light = self.light(in_place, true)?;
        self.number = number;
        let soft = self.raft.soft_state();
        if soft != self.previous_soft {
            ready.soft_state = Some(soft);
        }
        let hard = self.raft.hard_state();
        if hard != self.previous_hard {
            if hard.vote != self.previous_hard.vote || hard.term != self.previous_hard.term {
                ready.must_sync = true;
            }
            ready.hard_state = Some(hard);
        }
        // What the member knows committed by a classic quorum rides with a
        // write it makes anyway: a release is never worth a write of its own
        // (it frees room, and a store that keeps a proposal longer is still
        // right), and a `Ready` of nothing else to persist is not waited on.
        let persists = new_entries
            || new_snapshot.is_some()
            || ready.hard_state.is_some()
            || !ready.proposals.is_empty();
        let released_before = self.released;
        ready.released = self.release_with(persists);
        // Taken, and not emptied: what the member holds when it rests is
        // what it held before.
        ready.read_states = std::mem::take(&mut self.raft.read_states);
        ready.displaced = self.raft.take_displaced();
        if !ready.proposals.is_empty() || new_entries {
            ready.must_sync = true;
        }
        // I1: a leader's messages leave at once only while the term and
        // vote it leads in are durable, which no write out or this one
        // changes.
        ready.after_persisting = self.raft.state() != StateRole::Leader || !self.vote_durable();
        ready.after_persisting &= !self.vote_leaves_early(ready.light.messages());
        // What leaves with this write is sent once it is durable, when the
        // commit its hard state states is. What leaves at once is a leader's,
        // and holds no answer that states a commit (`state_durable_commit`).
        if ready.after_persisting {
            let durable = ready.hard_state.map_or(self.raft.durable_commit(), |hard| {
                hard.commit.max(self.raft.durable_commit())
            });
            state_durable_commit(
                &mut ready.light.messages,
                durable,
                self.raft.log().committed(),
            );
        }
        self.taken = Some(Taken {
            number,
            in_place,
            released_before,
        });
        Ok(ready)
    }
    /// What this member approved by itself and gave in the `Ready`s issued
    /// and not yet known durable, in issue order: kept until their notice
    /// ([`RawNode::on_persist`]), so an owner whose write of them was refused
    /// makes it again with them (hyper-durable's `make_again`).
    pub fn issued_proposals(&self) -> impl Iterator<Item = &Entry> + '_ {
        self.issued.iter().flat_map(|given| given.proposals.iter())
    }
    /// The write of what `ready` gave to persist is issued: the member takes
    /// operations again, and the next `Ready` gives only what this one did
    /// not. The owner keeps what it needs of `ready` (the persisted messages
    /// it sends once the write is durable) before it hands `ready` back.
    pub fn advance_issued(&mut self, mut ready: Ready) -> Result<()> {
        let mark = self.issue(&ready)?;
        // Room was reserved when the member opened, and `ready_given`
        // refused a `Ready` beyond it.
        self.issued.push_back(Given {
            mark,
            proposals: std::mem::take(&mut ready.proposals),
            stable: Stable::default(),
        });
        Ok(())
    }
    /// The owner writes no hard state for `ready`, the one taken: its commit
    /// moved alone, and rides a later write (`docs/durable.md` §4.1, focal
    /// F17; etcd's `MustSync`, which syncs only for entries, a snapshot, a
    /// term or a vote). Only a `Ready` that need not sync ([`Ready::must_sync`])
    /// and gives a hard state is deferred; any other is left as it is, and
    /// `false` returned. A deferred `Ready`'s write vouches for no commit: its
    /// hard state is given again by the next `Ready`, or as a notice's
    /// [`LightReady::commit_index`]; the release given with it is taken back
    /// (it rode the hard state, and a release is never worth a write of its
    /// own); and the answers that leave once it is durable state no commit
    /// past the durable one.
    ///
    /// Called before any of `ready`'s messages are taken: the answers among
    /// them state the commit, and are held to the durable one here. Refused,
    /// `Error::Invariant`, once they were taken: never deferred with answers
    /// already out stating a commit no write holds.
    pub fn defer_commit(&mut self, ready: &mut Ready) -> Result<bool> {
        let taken = self
            .taken
            .filter(|taken| taken.number == ready.number)
            .ok_or(Error::Invariant(
                "a ready deferred that is not the one taken",
            ))?;
        if ready.messages_taken {
            return Err(Error::Invariant(
                "a commit deferred after its ready's messages were taken",
            ));
        }
        if ready.must_sync || ready.hard_state.is_none() {
            return Ok(false);
        }
        ready.hard_state = None;
        if ready.released.take().is_some() {
            self.released = taken.released_before;
        }
        if ready.after_persisting {
            state_durable_commit(
                &mut ready.light.messages,
                self.raft.durable_commit(),
                self.raft.log().committed(),
            );
        }
        Ok(true)
    }
    /// `ready`, which must be the one taken, is issued: what its write
    /// vouches for.
    #[inline]
    fn issue(&mut self, ready: &Ready) -> Result<Mark> {
        let taken = self
            .taken
            .filter(|taken| taken.number == ready.number)
            .ok_or(Error::Invariant("a ready issued that is not the one taken"))?;
        self.taken = None;
        let mut vote = None;
        if let Some(soft) = ready.soft_state {
            self.previous_soft = soft;
        }
        if let Some(hard) = ready.hard_state {
            if hard.term != self.previous_hard.term || hard.vote != self.previous_hard.vote {
                vote = Some((hard.term, hard.vote));
            }
            self.previous_hard = hard;
        }
        let commit = ready.hard_state.map_or(0, |hard| hard.commit);
        let unstable = &mut self.raft.log.unstable;
        let mark = Mark {
            number: taken.number,
            term: self.raft.term,
            last_entry: unstable.entries.last().map(|last| (last.index, last.term)),
            snapshot: unstable.snapshot.as_ref().map(proto::snapshot_index),
            vote,
            commit,
            in_place: taken.in_place,
        };
        unstable.issue();
        self.raft.held.issue();
        Ok(mark)
    }
    /// The writes of every `Ready` through `number` are durable, in the
    /// order they were issued, and storage answers for them.
    pub fn on_persist(&mut self, number: u64) -> Result<LightReady> {
        self.persist_through(number, None::<fn(&mut S, Kept)>)
    }
    /// As [`RawNode::on_persist`], handing what became durable to `keep`,
    /// with the storage, before the member reads storage for it: an owner
    /// that wrote it out where it was ([`RawNode::ready_in_place`]) and keeps
    /// it in memory too keeps these very entries and this very snapshot, and
    /// copies none. `keep` must leave storage holding them; what else it
    /// does with them is the owner's.
    pub fn on_persist_keeping(
        &mut self,
        number: u64,
        keep: impl FnOnce(&mut S, Kept),
    ) -> Result<LightReady> {
        self.persist_through(number, Some(keep))
    }
    /// [`RawNode::on_persist`] with no `keep`: what became durable is dropped
    /// where it was held, so the log keeps its room for the next entries
    /// rather than giving it away with them. Else
    /// [`RawNode::on_persist_keeping`].
    fn persist_through(
        &mut self,
        number: u64,
        keep: Option<impl FnOnce(&mut S, Kept)>,
    ) -> Result<LightReady> {
        if self.taken.is_some() {
            return Err(Error::Invariant(
                "a write made durable while a ready is taken and not issued",
            ));
        }
        if self
            .issued
            .front()
            .is_none_or(|oldest| oldest.mark.number > number)
            || self
                .issued
                .back()
                .is_none_or(|newest| newest.mark.number < number)
        {
            return Err(Error::Invariant("a write made durable that is not out"));
        }
        let keeps = keep.is_some();
        let mut kept = Kept::default();
        let mut in_place = false;
        let Self {
            raft,
            issued,
            durable_vote,
            ..
        } = self;
        let term = raft.term();
        for given in issued
            .iter_mut()
            .take_while(|given| given.mark.number <= number)
        {
            given.stable = stabilize(
                &mut raft.log,
                term,
                durable_vote,
                &given.mark,
                &mut kept,
                keeps,
            )?;
            raft.commit_durable(given.mark.commit);
            in_place = given.mark.in_place;
        }
        if let Some(keep) = keep
            && (kept.snapshot.is_some() || !kept.entries.is_empty())
        {
            keep(&mut self.raft.log.store, kept);
        }
        self.raft.settle_priority();
        while let Some(given) = self
            .issued
            .pop_front_if(|given| given.mark.number <= number)
        {
            self.persisted(&given.proposals, given.stable)?;
        }
        // What storage holds now may hold what the member lost.
        self.raft.settle_lost()?;
        self.raft.settle_priority();
        self.after_persist(in_place)
    }
    /// What a write made durable is acted on: its snapshot, its entries (I3:
    /// a leader's own progress moves here, and only here), and what this
    /// member approved by itself.
    #[inline]
    fn persisted(&mut self, proposals: &[Entry], stable: Stable) -> Result<()> {
        if let Some(index) = stable.snapshot {
            self.raft.on_persist_snapshot(index)?;
        }
        if let Some((index, term)) = stable.entries {
            self.raft.on_persist_entries(index, term)?;
        }
        self.raft.on_persist_proposals(proposals)
    }
    /// What a notice releases leaves now: a member that does not lead
    /// states no commit beyond what is durable (`state_durable_commit`).
    #[inline]
    fn hold_released(&self, light: &mut LightReady) {
        if self.raft.state() != StateRole::Leader {
            state_durable_commit(
                &mut light.messages,
                self.raft.durable_commit(),
                self.raft.log().committed(),
            );
        }
    }
    /// What follows writes made durable: what there is to apply, and what
    /// may be sent now.
    #[inline]
    fn after_persist(&mut self, in_place: bool) -> Result<LightReady> {
        // What is to send now is sent after what the writes persisted,
        // whoever sends it: a leader that a change it applied made a
        // follower has its last messages here.
        let release = self.releases_now();
        let mut light = self.light(in_place, release)?;
        self.hold_released(&mut light);
        let hard = self.raft.hard_state();
        if hard.commit > self.previous_hard.commit {
            light.commit_index = Some(hard.commit);
            self.previous_hard.commit = hard.commit;
        }
        Ok(light)
    }
    /// What `ready` gave to persist is durable, and storage answers for it:
    /// [`RawNode::advance_issued`] and [`RawNode::on_persist`] at once.
    pub fn advance_append(&mut self, ready: Ready) -> Result<LightReady> {
        self.advance_through(ready, None::<fn(&mut S, Kept)>)
    }
    /// As [`RawNode::advance_append`], handing what `ready` gave to persist
    /// to `keep` ([`RawNode::on_persist_keeping`]).
    pub fn advance_append_keeping(
        &mut self,
        ready: Ready,
        keep: impl FnOnce(&mut S, Kept),
    ) -> Result<LightReady> {
        self.advance_through(ready, Some(keep))
    }
    /// [`RawNode::advance_append`] with no `keep`, else
    /// [`RawNode::advance_append_keeping`] (as [`RawNode::persist_through`]).
    fn advance_through(
        &mut self,
        ready: Ready,
        keep: Option<impl FnOnce(&mut S, Kept)>,
    ) -> Result<LightReady> {
        if !self.issued.is_empty() {
            let number = ready.number;
            self.advance_issued(ready)?;
            return self.persist_through(number, keep);
        }
        // The only write out, issued and durable at once: nothing happened
        // between its taking and now, so it vouches for everything not yet
        // durable, and it need not pass through the queue.
        let taken = self
            .taken
            .filter(|taken| taken.number == ready.number)
            .ok_or(Error::Invariant("a ready issued that is not the one taken"))?;
        self.taken = None;
        if let Some(soft) = ready.soft_state {
            self.previous_soft = soft;
        }
        if let Some(hard) = ready.hard_state {
            self.previous_hard = hard;
            self.durable_vote = (hard.term, hard.vote);
            self.raft.commit_durable(hard.commit);
        }
        let mut kept = Kept::default();
        let log = &mut self.raft.log;
        let snapshot = log.unstable.snapshot.as_ref().map(proto::snapshot_index);
        let last = log
            .unstable
            .entries
            .last()
            .map(|last| (last.index, last.term));
        if let Some(index) = snapshot {
            kept.snapshot = log.take_stable_snapshot(index);
        }
        if let Some((index, term)) = last {
            let into = keep.is_some().then_some(&mut kept.entries);
            log.take_stable_to(index, term, into)?;
        }
        log.unstable.issue();
        self.raft.held.issue();
        if let Some(keep) = keep
            && (kept.snapshot.is_some() || !kept.entries.is_empty())
        {
            keep(&mut self.raft.log.store, kept);
        }
        self.raft.settle_priority();
        self.persisted(
            &ready.proposals,
            Stable {
                snapshot,
                entries: last,
            },
        )?;
        self.raft.settle_lost()?;
        self.raft.settle_priority();
        // Nothing is out and nothing is left to write: what follows leaves
        // now (`RawNode::releases_now`).
        let mut light = self.light(taken.in_place, true)?;
        self.hold_released(&mut light);
        let hard = self.raft.hard_state();
        if hard.commit > self.previous_hard.commit {
            light.commit_index = Some(hard.commit);
            self.previous_hard.commit = hard.commit;
        }
        // Nothing was asked of the member between the two.
        if self.raft.hard_state() != self.previous_hard {
            return Err(Error::Invariant(
                "a term or a vote moved while a ready was out",
            ));
        }
        Ok(light)
    }
    /// The application applied through `applied`.
    pub fn advance_apply_to(&mut self, applied: u64) -> Result<()> {
        self.raft.settle_priority();
        let outcome = self.raft.commit_apply(applied);
        self.raft.settle_priority();
        outcome
    }
    /// Persisted and applied, for an owner that does both at once.
    pub fn advance(&mut self, ready: Ready) -> Result<LightReady> {
        let applied = self.commit_since;
        let light = self.advance_append(ready)?;
        self.advance_apply_to(applied)?;
        Ok(light)
    }
    /// Through which index entries were given to apply.
    pub fn given_to_apply(&self) -> u64 {
        self.commit_since
    }
}

/// What one durable write vouches for leaves what is not yet durable, into
/// `kept`: its term and vote, its snapshot and its entries, each where it is
/// still what the member holds. The ABA guards: the term the write was
/// taken in (etcd's), then `Log::take_stable_to`'s check and, in
/// `Raft::on_persist_entries`, raft-rs's `maybe_persist`.
#[inline]
fn stabilize<S: Storage>(
    log: &mut crate::log::Log<S>,
    term: u64,
    durable_vote: &mut (u64, u64),
    given: &Mark,
    kept: &mut Kept,
    keeps: bool,
) -> Result<Stable> {
    if let Some(vote) = given.vote {
        *durable_vote = vote;
    }
    let mut stable = Stable::default();
    if given.term != term {
        return Ok(stable);
    }
    if let Some(index) = given.snapshot
        && let Some(snapshot) = log.take_stable_snapshot(index)
    {
        kept.snapshot = Some(snapshot);
        stable.snapshot = Some(index);
    }
    if let Some((index, entry_term)) = given.last_entry
        && log.take_stable_to(index, entry_term, keeps.then_some(&mut kept.entries))?
    {
        stable.entries = Some((index, entry_term));
    }
    Ok(stable)
}
