//! The member: what it does with time and with messages.
//!
//! This is Raft as Ongaro's thesis states it and as `raft-rs` runs it, which
//! is what focal's groups ran on before: on one schedule the two say the
//! same, message for message. Where they differ it is by decision, and each
//! place says so:
//!
//! - nothing unwinds; what is asserted there is an [`Error`] here;
//! - a leader that is no voter any more steps down, and hands the lead to a
//!   voter that holds the whole log, where `raft-rs` leaves it leading a
//!   group it is no member of;
//! - priority orders elections and never judges the vote a transfer asks
//!   for, and it is not in force while the member has no term;
//! - a member told to campaign while a change it has committed is not yet
//!   applied campaigns once it is, where `raft-rs` forgets that it was told;
//! - what is queued has a bound ([`Limits`]).
use crate::{
    Configuration, NodeId, Tally,
    error::{Error, Result, StorageError},
    fast::{self, Decided, Proposals, Votes},
    log::Log,
    progress::{Progress, ProgressState, Tracker},
    proto::{
        self, CAMPAIGN_TRANSFER, ConfState, Entry, EntryType, HardState, Message, MessageType,
        Plan, Snapshot,
    },
    read::{ReadOnly, ReadState},
    storage::Storage,
    watch::{Arm, TRANSFER_ROUNDS, Timing, Watch, nanos},
};

/// What grows only to a bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Messages that wait to be taken. One operation adds at most two a
    /// member and one a member for each place in its window; an operation
    /// that finds this many waiting is refused.
    pub pending_messages: usize,
    /// Reads that wait for their quorum or to be taken.
    pub pending_reads: usize,
    /// Entries that are not durable yet.
    pub unstable_entries: usize,
    /// Entries one message carries, whatever their bytes. At most what
    /// may be held not yet durable, so that a member can always take a
    /// leader's message whole.
    pub entries_per_message: usize,
    /// Entries a member holds approved by itself ([`crate::fast`]). A vote
    /// for one that asks to lead carries them all, so their bytes are
    /// what one message may be.
    pub proposals: usize,
    /// The bytes of the entries a member holds approved by itself.
    pub proposal_bytes: usize,
    /// How far above what is committed an entry may be proposed.
    pub fast_window: u64,
    /// The bytes of what a leader was told the voters hold.
    pub vote_bytes: usize,
    /// `Ready`s whose writes are out and not yet known durable
    /// ([`crate::RawNode::advance_issued`]). One is an owner that finishes
    /// each write before it takes the next. An owner over a pipelined store
    /// sets the store's depth: hyper-durable sets `LogStore::depth`, which
    /// for hyper-log is its pipeline frames (`docs/durable.md` §6).
    pub readies_in_flight: usize,
}
/// focal's bounds, carried unchanged. They are literals, not derivations
/// (mantle note 32 §2.10); `Limits::derive` replaces them in R-3
/// (`docs/raft.md`).
impl Default for Limits {
    fn default() -> Self {
        Self {
            pending_messages: 65_536,
            pending_reads: 4_096,
            unstable_entries: 65_536,
            entries_per_message: 16_384,
            proposals: 256,
            proposal_bytes: 8 * 1024 * 1024 - 64 * 1024,
            fast_window: 256,
            vote_bytes: 64 * 1024 * 1024,
            readies_in_flight: 1,
        }
    }
}

/// What a candidate of lower priority must hold to be voted for all the
/// same.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Precedence {
    /// A log more current than the voter's: a later last term, or the same
    /// and more entries. A voter that refuses for priority could then have
    /// been elected itself, so the refusal never leaves a group that can
    /// elect without a leader.
    #[default]
    Log,
    /// More entries than the voter, whatever their terms: the rule of
    /// `raft-rs`. Two voters whose logs are equally long and end in
    /// different terms refuse each other, the one for priority and the
    /// other for the log, and with the third away the group elects no one.
    /// With a marked member (core step R-7) no member need be away: the
    /// voter of the longest log, of an older term, refuses the candidate of
    /// the later term for priority while the marked voter refuses it for its
    /// mark, and neither of them can be elected. A marked member's election
    /// is argued by [`Precedence::Log`] (`docs/durable.md` §5.2). Kept to
    /// compare the two cores under one rule.
    Length,
}

/// When a leader asks its quorum for the reads that wait (Ongaro's thesis
/// §6.4: a round of heartbeats sent after a read was asked, and answered by
/// a quorum, confirms it and every read asked before it).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadRounds {
    /// When the member is next asked what there is to do
    /// ([`crate::RawNode::ready`]): one round, carrying the last read asked,
    /// for every read asked since the round before. A read asked alone
    /// leaves with the `Ready` its owner takes next; reads its owner took
    /// together share a round; and a read asked after a round was sent is
    /// asked for by the next, never confirmed by one that left before it.
    /// A read that is asked again while it waits asks for no round of its
    /// own: a round that was lost is asked again by the leader's clock.
    #[default]
    Shared,
    /// As each read is asked, and each time it is asked again, a round of
    /// its own: the rule of `raft-rs`. Twenty reads taken together are
    /// forty heartbeats to two members, of which the answers to the last
    /// two confirm all twenty. Kept to compare the two cores under one
    /// rule.
    Each,
}

/// What a member says when it answers a heartbeat, and what its leader
/// makes of a full window when it hears it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HeartbeatAnswers {
    /// How far the member's log goes: its last index and that entry's
    /// term. A leader whose term the entry is of made it, and the member
    /// took it and all before it from that leader's appends: it holds the
    /// leader's log through that index, which is what an append's answer
    /// says, and the leader takes it as one. Answers that were lost are
    /// made good, exactly, by the next heartbeat's, and the window gives
    /// back what the member holds and nothing else: a member that answers
    /// heartbeats and holds nothing new is sent nothing more, so the bytes
    /// out stay within their bound. One whose window is full and that has
    /// answered for none of it through a beat of the leader's ticks is
    /// probed; a probe is sent again when it was told lost, or once a beat
    /// has passed since it was sent.
    #[default]
    Position,
    /// Nothing of its log; and a leader that hears from a member whose
    /// window is full frees the window's first message and sends the next,
    /// whatever became of the first: the rule of `raft-rs`. The bytes out
    /// then pass their bound by a message for every beat the member answers
    /// heartbeats and no append. Kept to compare the two cores under one
    /// rule.
    Bare,
}

/// What starts a member's elections and ends a leader's term for want of
/// a quorum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Elections {
    /// The owner's ticks, as `raft-rs` counts them: a follower campaigns
    /// after a timeout drawn from `[election_tick, 2·election_tick)` ticks
    /// without its leader, a leader heartbeats every `heartbeat_tick` and
    /// checks its quorum every `election_tick`. Every group ticks, idle or
    /// not, and each count is a number its owner picks. Kept so that the two
    /// cores compare under one rule (`tests/differential.rs`).
    #[default]
    Ticks,
    /// The owner's failure detectors (timing step L-2, `docs/timing.md`
    /// §2.1–§2.3, [`crate::watch`]): a follower campaigns when it trusts no
    /// leader, after a delay drawn over the span the owner measured; a leader
    /// steps down when its detectors suspect a majority; a leader beats only
    /// while its group has work in flight. The member takes no ticks. Needs
    /// pre-vote and check-quorum.
    Suspicion,
}

/// How a member runs. [`Config::new`] gives the settings `raft-rs` 0.7
/// defaults to, so that the two cores compare under one setting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// This member's identity; never zero.
    pub id: NodeId,
    /// Ticks without the leader before an election.
    pub election_tick: usize,
    /// Ticks between a leader's heartbeats.
    pub heartbeat_tick: usize,
    /// What the application has applied; it is given nothing at or below.
    pub applied: u64,
    /// The bytes of entries one message carries, and one entry at least.
    pub max_size_per_msg: u64,
    /// Messages sent to a member ahead of its answers.
    pub max_inflight_msgs: usize,
    /// The bytes of entries sent to a member ahead of its answers, until
    /// its owner says what the path to it carries
    /// ([`crate::RawNode::set_inflight_bytes`]); `u64::MAX` for no bound of
    /// its own. One entry larger than the bound is still sent, alone.
    pub max_inflight_bytes: u64,
    /// The bytes of proposals a leader holds uncommitted; `u64::MAX` for no
    /// bound of its own.
    pub max_uncommitted_size: u64,
    /// The bytes of committed entries one [`crate::Ready`] gives.
    pub max_committed_size_per_ready: u64,
    /// A leader that hears no quorum for an election timeout steps down,
    /// and a member that hears its leader grants no vote.
    pub check_quorum: bool,
    /// An election is asked about before a term is spent on it.
    pub pre_vote: bool,
    /// This member's rank in elections: a voter of higher priority votes
    /// for a candidate of lower only when the candidate's log is ahead of
    /// its own by [`Config::precedence`]. It never judges a transfer.
    pub priority: i64,
    /// What a candidate of lower priority must hold to be voted for.
    pub precedence: Precedence,
    /// When a leader sends the round of heartbeats that confirms the reads
    /// that wait.
    pub read_rounds: ReadRounds,
    /// What a member says when it answers a heartbeat, and what its leader
    /// makes of a full window.
    pub heartbeat_answers: HeartbeatAnswers,
    /// Whether the group has the fast track ([`crate::fast`]). It is part
    /// of what the group is: every member is opened with the same.
    pub fast: bool,
    /// A commit alone is not sent; it goes with the next message.
    pub skip_bcast_commit: bool,
    /// A leader is given the entries of its own term to apply once they
    /// are committed, whether or not its own write of them is durable yet
    /// (core step R-6, `docs/durable.md` §4.2): a committed entry is durable
    /// on a quorum, and waiting for this member's copy adds nothing to its
    /// durability (TiKV RFC 0112; raft-rs's `max_apply_unpersisted_log_limit`,
    /// PR #537, enabled on a leader only, PR #561). Entries of earlier terms
    /// wait for their durability here, since a write still out may replace
    /// them (the ABA case of `docs/durable.md` §2.1). The entries it covers
    /// are the leader's unstable entries, bounded by
    /// [`Limits::unstable_entries`]. Off, as raft-rs's default (a limit of
    /// zero), so the two cores compare under one setting.
    pub apply_unpersisted: bool,
    /// What starts elections ([`Elections`]).
    pub elections: Elections,
    /// What storage found the log to lack when the member opened ([`Lost`]); none for a whole
    /// log. The member keeps it until its durable log holds what it marks.
    pub lost: Option<Lost>,
    /// What the election timeouts are drawn from.
    pub seed: u64,
    /// The bounds of what grows.
    pub limits: Limits,
}
impl Config {
    /// The member `id` with `raft-rs` 0.7's defaults (its `Config::default`:
    /// an election after twenty ticks, a heartbeat every two, a window of
    /// 256 messages, no byte bounds) and the bounds of [`Limits::default`].
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            election_tick: 20,
            heartbeat_tick: 2,
            applied: 0,
            max_size_per_msg: 0,
            max_inflight_msgs: 256,
            max_inflight_bytes: u64::MAX,
            max_uncommitted_size: u64::MAX,
            max_committed_size_per_ready: u64::MAX,
            check_quorum: false,
            pre_vote: false,
            priority: 0,
            precedence: Precedence::Log,
            read_rounds: ReadRounds::Shared,
            heartbeat_answers: HeartbeatAnswers::Position,
            fast: false,
            skip_bcast_commit: false,
            apply_unpersisted: false,
            elections: Elections::Ticks,
            lost: None,
            seed: id,
            limits: Limits::default(),
        }
    }
    /// Whether the settings describe a member that can run: an identity, an
    /// election that waits longer than a heartbeat, a window, and bounds
    /// that admit something and agree with each other.
    pub fn validate(&self) -> Result<()> {
        if self.id == 0 {
            return Err(Error::Settings("a member's identity is zero"));
        }
        if self.heartbeat_tick == 0 {
            return Err(Error::Settings("no tick between heartbeats"));
        }
        if self.election_tick <= self.heartbeat_tick {
            return Err(Error::Settings(
                "an election must wait longer than a heartbeat",
            ));
        }
        if self.election_tick > usize::MAX / 2 {
            return Err(Error::Settings(
                "an election timeout beyond what is counted",
            ));
        }
        if self.max_inflight_msgs == 0 || self.max_inflight_bytes == 0 {
            return Err(Error::Settings("a window that admits nothing"));
        }
        if self.max_uncommitted_size < self.max_size_per_msg {
            return Err(Error::Settings(
                "a message larger than all that may be uncommitted",
            ));
        }
        if self.limits.pending_messages == 0
            || self.limits.pending_reads == 0
            || self.limits.unstable_entries == 0
            || self.limits.entries_per_message == 0
            || self.limits.proposals == 0
            || self.limits.proposal_bytes == 0
            || self.limits.fast_window == 0
            || self.limits.vote_bytes == 0
            || self.limits.readies_in_flight == 0
        {
            return Err(Error::Settings("a bound that admits nothing"));
        }
        if self.elections == Elections::Suspicion && !(self.pre_vote && self.check_quorum) {
            // A member that opens knowing no leader campaigns: without
            // pre-vote, one that restarted in an idle group would spend a
            // term on it and depose a leader that was never gone; and the
            // lease that refuses its vote request is check-quorum's.
            return Err(Error::Settings(
                "elections by suspicion need pre-vote and check-quorum",
            ));
        }
        if self.limits.entries_per_message > self.limits.unstable_entries {
            return Err(Error::Settings(
                "a message of more entries than a member may hold not yet durable",
            ));
        }
        Ok(())
    }
}

/// What the fast track did at a member since it opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FastStats {
    /// Entries proposed here by the fast track.
    pub proposed: u64,
    /// Of those, the ones another entry took the index of.
    pub displaced: u64,
    /// Entries held here approved by this member.
    pub held: u64,
    /// Entries this member took as leader from the fast track.
    pub taken: u64,
    /// Indexes this member committed as leader by the fast quorum.
    pub committed: u64,
    /// Entries this member took at its election from what the voters held.
    pub recovered: u64,
}

/// What a member's log may lack: entries it acknowledged and lost at rest, through `index`, of
/// terms up to `term` (`docs/durable.md` §5). It is what its store found when it opened:
/// hyper-log's uncertainty mark, a lost last frame whose persist record survived (Protocol-Aware
/// Recovery's faulty entry, Alagappan et al., FAST 2018, §3.3.3). While it lasts the member is
/// held by what it acknowledged, not by its shorter log: it judges votes by the mark, and tells a
/// leader that counts the entries that it lost them (core step R-5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Lost {
    /// The highest index the log may lack.
    pub index: u64,
    /// The highest term an entry it lacks may be of.
    pub term: u64,
}
impl Lost {
    /// Whether a durable log whose last entry is `index`, of `term`, holds again what the mark
    /// says it may lack: past the mark's index it holds those entries, received again; with an
    /// entry of a later term, from a leader, it matches that leader's log through that entry,
    /// and that leader's log holds every entry committed in the mark's terms (Leader
    /// Completeness), all before it since terms never fall along a log. hyper-log's rule
    /// (`resolves`, mantle `docs/design/raft-log.md` §6).
    pub fn resolved_by(self, index: u64, term: u64) -> bool {
        index >= self.index || term > self.term
    }
}

/// What a member is in its group.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StateRole {
    /// It follows a leader, or waits to hear of one.
    #[default]
    Follower,
    /// It asks for votes in a term of its own.
    Candidate,
    /// It leads its term.
    Leader,
    /// It asks whether it could be elected, spending no term.
    PreCandidate,
}

/// What a member says of itself that is not durable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SoftState {
    /// The leader it knows of; zero for none.
    pub leader_id: NodeId,
    /// What it is.
    pub raft_state: StateRole,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Campaign {
    /// Asked about first; no term is spent.
    PreElection,
    Election,
    /// Asked for by the leader: no asking first, and no lease refuses it.
    Transfer,
}

/// One member: its term and vote, its role, its log and what it knows of
/// the others.
pub struct Raft<S> {
    pub(crate) id: NodeId,
    pub(crate) term: u64,
    pub(crate) vote: NodeId,
    pub(crate) state: StateRole,
    pub(crate) leader_id: NodeId,
    pub(crate) lead_transferee: Option<NodeId>,
    /// No change is proposed until what is applied passes this: an index at
    /// or after the last change in the log, if there is one.
    pub(crate) pending_conf_index: u64,
    /// The index this member asked a snapshot to reach; zero for none.
    pub(crate) pending_request_snapshot: u64,
    pub(crate) election_elapsed: usize,
    heartbeat_elapsed: usize,
    randomized_election_timeout: usize,
    /// Ticks this member waits beyond its election timeout before it
    /// campaigns: its patience, given by its owner for what it has seen
    /// of itself. A member whose own periods stalled cannot tell a leader
    /// that stalls as it does from one that died, so it waits as long as
    /// its own longest stall took, and a tail after. A stall is covered in
    /// ticks and not by a longer tick, so nothing else the member counts
    /// in ticks grows with it.
    patience: usize,
    /// What this member approved by itself.
    pub(crate) held: Proposals,
    /// What the voters hold above this member's log, as it was told.
    pub(crate) votes: Votes,
    /// Who holds what this leader took from the fast track.
    pub(crate) decided: Decided,
    /// What was proposed here and another entry took the index of.
    pub(crate) displaced: Vec<Entry>,
    /// The leader, and its term, that was told what this member holds.
    pub(crate) voted_to: (u64, NodeId),
    /// What the fast track did here since the member opened.
    pub(crate) fast_stats: FastStats,
    pub(crate) holders: Vec<NodeId>,
    /// In a fast group that this member leads: the voters of the
    /// configuration it had applied when it was elected, by which a member
    /// that holds entries of this term may still count ([`crate::track`]).
    pub(crate) term_voters: Vec<NodeId>,
    /// The one other set of voters a configuration it applied since named;
    /// empty for none.
    pub(crate) term_next: Vec<NodeId>,
    /// Whether every configuration in force since it was elected names no
    /// voters but those two sets: then what a member that holds an entry of
    /// this term counts by is known here.
    pub(crate) term_known: bool,
    /// The priority the owner gave.
    priority: i64,
    /// The priority votes are judged by: the one given, once the member
    /// has a term.
    priority_in_force: i64,
    pub(crate) promotable: bool,
    /// The commit this member's storage states durably, as its owner said
    /// (`docs/durable.md` §4.1's `C_d`): what a member that restarts reopens
    /// with. Its answers state no more when they leave
    /// (`RawNode::durable_commit`).
    pub(crate) durable_commit: u64,
    /// What this member's durable log may lack of what it acknowledged ([`Lost`]).
    lost: Option<Lost>,
    /// The leader told this member to campaign, and it could not yet: a
    /// change it has committed is not applied. It campaigns once it is,
    /// unless it has heard of a leader or a term since.
    told_to_campaign: bool,
    pub(crate) log: Log<S>,
    pub(crate) tracker: Tracker,
    pub(crate) read_only: ReadOnly,
    pub(crate) read_states: Vec<ReadState>,
    pub(crate) msgs: Outgoing,
    uncommitted_bytes: usize,
    /// The last index when this member last became leader: entries at or
    /// below are no proposals of its own.
    leader_tail: u64,
    random: u64,
    /// What a member keeps that elects by suspicion ([`Elections::Suspicion`]);
    /// none on ticks, where it would be eight bytes and a branch.
    pub(crate) watch: Option<Box<Watch>>,
    pub(crate) config: Config,
}

/// The messages that wait to be taken, and what they hold, by capacity,
/// kept as they are queued so that asking costs nothing.
///
/// The queue grows by a policy of its own — doubled when full, and never
/// given fewer than [`Outgoing::SMALLEST`] slots — so that what a burst of
/// messages may grow it by is known before they are sent
/// ([`Outgoing::growth_of`]), and does not depend on what a library keeps
/// as its own.
#[derive(Debug, Default)]
pub struct Outgoing {
    msgs: Vec<Message>,
    payload: usize,
    /// An emptied queue an owner gave back ([`Outgoing::recycle`]), which
    /// the next [`Outgoing::take`] leaves in place of the one it gives.
    spare: Vec<Message>,
}
impl Outgoing {
    /// The fewest slots a queue is given: what a tick of a three-member
    /// group queues at most, a heartbeat a peer or a vote request a peer,
    /// and one more.
    pub const SMALLEST: usize = 4;
    /// How many messages wait.
    pub fn len(&self) -> usize {
        self.msgs.len()
    }
    /// Whether no message waits.
    pub fn is_empty(&self) -> bool {
        self.msgs.is_empty()
    }
    /// The messages that wait, in the order queued.
    pub fn as_slice(&self) -> &[Message] {
        &self.msgs
    }
    /// The slots the queue holds.
    pub fn capacity(&self) -> usize {
        self.msgs.capacity()
    }
    /// The bytes the messages hold, by capacity ([`proto::message_bytes`]).
    pub fn payload(&self) -> usize {
        self.payload
    }
    /// The bytes held, by capacity: the slots, the spare queue's, and what
    /// the messages hold.
    pub fn resident_bytes(&self) -> usize {
        self.msgs
            .capacity()
            .saturating_add(self.spare.capacity())
            .saturating_mul(std::mem::size_of::<Message>())
            .saturating_add(self.payload)
    }
    /// The most bytes the slots may grow by when `count` messages are
    /// queued. Doubling from a capacity `c` that holds `len <= c` until
    /// `len + count` fit ends below `2 (len + count)` slots, or at
    /// [`Outgoing::SMALLEST`] from nothing: the growth is at most
    /// `SMALLEST + c + 2 count` slots.
    pub fn growth_of(&self, count: usize) -> usize {
        Self::SMALLEST
            .saturating_add(self.msgs.capacity())
            .saturating_add(count.saturating_mul(2))
            .saturating_mul(std::mem::size_of::<Message>())
    }
    /// Queues `message`, whose [`proto::message_bytes`] are `bytes`.
    fn push_counted(&mut self, message: Message, bytes: usize) -> Result<()> {
        if self.msgs.len() == self.msgs.capacity() {
            let more = self.msgs.capacity().max(Self::SMALLEST);
            self.msgs
                .try_reserve_exact(more)
                .map_err(|_| Error::Memory)?;
        }
        self.payload = self.payload.saturating_add(bytes);
        self.msgs.push(message);
        Ok(())
    }
    /// Everything queued, given up: the queue that remains holds nothing,
    /// with the room of the spare an owner gave back, if any.
    pub fn take(&mut self) -> Vec<Message> {
        self.payload = 0;
        let spare = std::mem::take(&mut self.spare);
        std::mem::replace(&mut self.msgs, spare)
    }
    /// An owner gives back a queue it has emptied, so the room a burst grew
    /// stays with the member rather than being grown again from
    /// [`Outgoing::SMALLEST`]: it is the queue now if nothing waits, else the
    /// next [`Outgoing::take`]'s. It is kept only where it has more room than
    /// what it would replace, and no more than `most` slots, the most that
    /// may wait ([`Limits::pending_messages`]); otherwise it is dropped.
    pub(crate) fn recycle(&mut self, mut emptied: Vec<Message>, most: usize) {
        emptied.clear();
        let room = emptied.capacity();
        if room > most {
            return;
        }
        if self.msgs.is_empty() {
            if room > self.msgs.capacity() {
                self.msgs = emptied;
            }
        } else if room > self.spare.capacity() {
            self.spare = emptied;
        }
    }
    /// Whether the counter says what a walk of the messages says.
    pub(crate) fn check(&self) -> Result<()> {
        let payload = self.msgs.iter().fold(0usize, |bytes, message| {
            bytes.saturating_add(proto::message_bytes(message))
        });
        if payload != self.payload {
            return Err(Error::Invariant(
                "what waits to be taken is not what its counter says",
            ));
        }
        Ok(())
    }
}

/// What sends for a leader while it walks its members.
struct Outbox<'a, S> {
    log: &'a Log<S>,
    msgs: &'a mut Outgoing,
    id: NodeId,
    term: u64,
    priority: i64,
    max_bytes: u64,
    max_entries: usize,
}

fn push(msgs: &mut Outgoing, id: NodeId, term: u64, priority: i64, message: Message) -> Result<()> {
    push_with(msgs, id, term, priority, message, None)
}

/// As [`push`], given what the entries' buffers hold by capacity when it is
/// already counted ([`proto::message_bytes_with`]).
fn push_with(
    msgs: &mut Outgoing,
    id: NodeId,
    term: u64,
    priority: i64,
    mut message: Message,
    entries_payload: Option<usize>,
) -> Result<()> {
    if message.from == 0 {
        message.from = id;
    }
    if message.msg_type == fast::FAST_PROPOSE || message.msg_type == fast::FAST_VOTE {
        if message.term != 0 {
            return Err(Error::Invariant(
                "a term given to a message that takes the member's",
            ));
        }
        // A proposal is its proposer's and bears no term; what a member
        // holds it says as of its term.
        if message.msg_type == fast::FAST_VOTE {
            message.term = term;
        }
        return queue(msgs, message, entries_payload);
    }
    let kind = message.msg_type;
    match kind {
        MessageType::MsgRequestVote
        | MessageType::MsgRequestPreVote
        | MessageType::MsgRequestVoteResponse
        | MessageType::MsgRequestPreVoteResponse => {
            // The term of a vote is the one campaigned for, which is not
            // always the member's own.
            if message.term == 0 {
                return Err(Error::Invariant("a vote without its term"));
            }
        }
        _ => {
            if message.term != 0 {
                return Err(Error::Invariant(
                    "a term given to a message that takes the member's",
                ));
            }
            // What is forwarded to the leader is the asker's and bears no
            // term.
            if kind != MessageType::MsgPropose && kind != MessageType::MsgReadIndex {
                message.term = term;
            }
        }
    }
    if matches!(
        kind,
        MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
    ) {
        message.priority = priority;
    }
    queue(msgs, message, entries_payload)
}

/// Queues `message`, its bytes counted from `entries_payload` when given.
fn queue(msgs: &mut Outgoing, message: Message, entries_payload: Option<usize>) -> Result<()> {
    let bytes = match entries_payload {
        Some(payload) => proto::message_bytes_with(&message, payload),
        None => proto::message_bytes(&message),
    };
    msgs.push_counted(message, bytes)
}

fn priority_of(message: &Message) -> i64 {
    message.priority
}

impl<S: Storage> Outbox<'_, S> {
    fn send(&mut self, message: Message) -> Result<()> {
        push(self.msgs, self.id, self.term, self.priority, message)
    }
    /// As [`Outbox::send`], for a message whose entries' payload was
    /// counted as its page was chosen.
    fn send_page(&mut self, message: Message, payload: usize) -> Result<()> {
        push_with(
            self.msgs,
            self.id,
            self.term,
            self.priority,
            message,
            Some(payload),
        )
    }
    fn snapshot(
        &mut self,
        message: &mut Message,
        progress: &mut Progress,
        to: NodeId,
    ) -> Result<bool> {
        if !progress.recent_active {
            return Ok(false);
        }
        let snapshot = match self.log.snapshot(progress.pending_request_snapshot, to) {
            Ok(snapshot) => snapshot,
            Err(Error::Storage(StorageError::SnapshotTemporarilyUnavailable)) => return Ok(false),
            Err(error) => return Err(error),
        };
        let index = proto::snapshot_index(&snapshot);
        if index == 0 {
            return Err(Error::Invariant("a snapshot that states nothing"));
        }
        message.msg_type = MessageType::MsgSnapshot;
        message.snapshot = Some(Box::new(snapshot));
        progress.become_snapshot(index);
        Ok(true)
    }
    /// Sends `to` what follows its progress, if it may be sent. An empty
    /// message carries the commit alone; `allow_empty` says whether one is
    /// worth sending.
    fn append(&mut self, to: NodeId, progress: &mut Progress, allow_empty: bool) -> Result<bool> {
        if progress.is_paused() {
            return Ok(false);
        }
        let mut message = Message {
            to,
            ..Message::default()
        };
        // What the entries' buffers hold, counted as their page is chosen.
        let mut payload = None;
        if progress.pending_request_snapshot != 0 {
            if !self.snapshot(&mut message, progress, to)? {
                return Ok(false);
            }
        } else {
            // The page is bounded by its bytes, by what the member's
            // window has room for and by its entries, before any of it is
            // copied.
            let entries = self.log.page(
                progress.next_index,
                progress.page_bytes(self.max_bytes),
                self.max_entries,
            );
            if !allow_empty
                && entries
                    .as_ref()
                    .map_or(true, |page| page.entries.is_empty())
            {
                return Ok(false);
            }
            let term = self.log.term(progress.next_index.saturating_sub(1));
            // What stops the replica is never taken for a log that is
            // compacted.
            for failed in [term.as_ref().err(), entries.as_ref().err()] {
                if let Some(error) = failed
                    && !matches!(error, Error::Storage(_))
                {
                    return Err(*error);
                }
            }
            match (term, entries) {
                (Ok(term), Ok(page)) => {
                    message.msg_type = MessageType::MsgAppend;
                    message.index = progress.next_index.saturating_sub(1);
                    message.log_term = term;
                    message.commit = self.log.committed();
                    if let Some(last) = page.entries.last() {
                        // Charged by the rule the page was cut by, counted
                        // as it was chosen.
                        progress.sent(last.index, page.bytes)?;
                    }
                    message.entries = page.entries;
                    payload = Some(page.payload);
                }
                (_, Err(Error::Storage(StorageError::LogTemporarilyUnavailable))) => {
                    return Ok(false);
                }
                // What the log no longer holds goes as a snapshot.
                _ => {
                    if !self.snapshot(&mut message, progress, to)? {
                        return Ok(false);
                    }
                }
            }
        }
        match payload {
            Some(payload) => self.send_page(message, payload)?,
            None => self.send(message)?,
        }
        Ok(true)
    }
    /// As many messages as the window admits, and none that is empty.
    fn append_all(&mut self, to: NodeId, progress: &mut Progress, window: usize) -> Result<()> {
        // Each message sent takes a place in the window, so the window
        // bounds them; a member that is probed takes one.
        for _ in 0..window.saturating_add(1) {
            if !self.append(to, progress, false)? {
                break;
            }
        }
        Ok(())
    }
    fn heartbeat(&mut self, to: NodeId, progress: &Progress, context: Option<&[u8]>) -> Result<()> {
        let mut message = proto::message(to, MessageType::MsgHeartbeat);
        // Never a commit the member may not hold.
        message.commit = progress.matched.min(self.log.committed());
        if let Some(context) = context {
            message
                .context
                .try_reserve_exact(context.len())
                .map_err(|_| Error::Memory)?;
            message.context.extend_from_slice(context);
        }
        self.send(message)
    }
}

/// What the term check of [`Raft::step`] leaves of a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TermChecked {
    /// The message goes on to the handler of its kind.
    Handle,
    /// The term check answered it or dropped it.
    Done,
}

/// Whether a message carries a term or an index at the end of what is
/// counted, past which the next could not be named.
fn counts_beyond_bound(message: &Message) -> bool {
    [
        message.term,
        message.index,
        message.commit,
        message.log_term,
        message.commit_term,
        message.request_snapshot,
        message.reject_hint,
    ]
    .contains(&u64::MAX)
        || message
            .entries
            .iter()
            .any(|entry| entry.index == u64::MAX || entry.term == u64::MAX)
}

impl<S: Storage> Raft<S> {
    /// The member `config` names, opened on what `store` holds.
    pub fn new(config: &Config, store: S) -> Result<Self> {
        config.validate()?;
        let initial = store.initial_state()?;
        let configuration = Configuration::from_conf_state(&initial.configuration)?;
        let log = Log::new(store, config.limits.unstable_entries)?;
        let tracker = Tracker::new(
            configuration,
            log.last_index()?,
            config.max_inflight_msgs,
            config.max_inflight_bytes,
        )?;
        let mut raft = Self {
            id: config.id,
            term: 0,
            vote: 0,
            state: StateRole::Follower,
            leader_id: 0,
            lead_transferee: None,
            pending_conf_index: 0,
            pending_request_snapshot: 0,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_election_timeout: config.election_tick,
            patience: 0,
            held: Proposals::new(config.limits.proposals, config.limits.proposal_bytes),
            votes: Votes::new(
                usize::try_from(config.limits.fast_window).unwrap_or(usize::MAX),
                config.limits.vote_bytes,
            ),
            decided: Decided::default(),
            displaced: Vec::new(),
            voted_to: (0, 0),
            fast_stats: FastStats::default(),
            holders: Vec::new(),
            term_voters: Vec::new(),
            term_next: Vec::new(),
            term_known: false,
            priority: config.priority,
            priority_in_force: 0,
            promotable: false,
            durable_commit: 0,
            lost: config.lost,
            told_to_campaign: false,
            log,
            tracker,
            read_only: ReadOnly::new(config.limits.pending_reads),
            read_states: Vec::new(),
            msgs: Outgoing::default(),
            uncommitted_bytes: 0,
            leader_tail: 0,
            random: config.seed,
            watch: match config.elections {
                Elections::Ticks => None,
                // It knows no leader yet (`become_follower` below arms it).
                Elections::Suspicion => Some(Box::default()),
            },
            config: config.clone(),
        };
        raft.promotable = raft.tracker.configuration().votes(raft.id);
        if initial.hard_state != HardState::default() {
            raft.load_state(&initial.hard_state)?;
        }
        // What storage states is durable: the commit of its hard state, or
        // the snapshot it begins after.
        raft.durable_commit = raft.log.committed();
        if config.applied > 0 {
            // What was applied may be ahead of the commit that was durable.
            raft.log.applied_to_unchecked(config.applied);
        }
        let last = raft.log.last_index()?;
        for held in initial.proposals {
            if held.index > last {
                raft.held.hold(held, true, false)?;
            }
        }
        raft.settle_lost()?;
        let term = raft.term;
        raft.become_follower(term, 0)?;
        if let Some(watch) = raft.watch.as_mut() {
            // Opened, it has seen no election: like followers whose detectors
            // suspected their leader together, those opened together draw
            // over the span from now.
            watch.campaign = Arm::Unset { round: false };
            // A member that voted for itself in its term may have led it, and
            // its followers may trust it still: it campaigns, or hands over,
            // until its term moves or it follows another leader of it. Its
            // followers' detectors see it start again, but what it sent
            // before may still arrive.
            if raft.vote == raft.id && raft.term != 0 {
                watch.led = raft.term;
            }
        }
        raft.settle_priority();
        Ok(raft)
    }

    /// The configuration the core runs under.
    pub fn config(&self) -> &Config {
        &self.config
    }
    /// This member's identity.
    pub fn id(&self) -> NodeId {
        self.id
    }
    /// This member's term.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whom this member voted for in its term; zero for none.
    pub fn vote(&self) -> NodeId {
        self.vote
    }
    /// What this member is.
    pub fn state(&self) -> StateRole {
        self.state
    }
    /// The leader this member knows of; zero for none.
    pub fn leader_id(&self) -> NodeId {
        self.leader_id
    }
    /// The index what is applied must pass before a change is proposed.
    pub fn pending_conf_index(&self) -> u64 {
        self.pending_conf_index
    }
    /// The member this leader hands the lead to, while it does.
    pub fn lead_transferee(&self) -> Option<NodeId> {
        self.lead_transferee
    }
    /// This member's log.
    pub fn log(&self) -> &Log<S> {
        &self.log
    }
    /// The storage this member reads.
    pub fn store(&self) -> &S {
        self.log.store()
    }
    /// The storage this member reads, for its owner to write.
    pub fn store_mut(&mut self) -> &mut S {
        self.log.store_mut()
    }
    /// What this member knows of each member, and the votes it holds.
    pub fn tracker(&self) -> &Tracker {
        &self.tracker
    }
    /// The configuration in force.
    pub fn configuration(&self) -> &Configuration {
        self.tracker.configuration()
    }
    /// The messages that wait to be taken, in the order queued.
    pub fn messages(&self) -> &[Message] {
        self.msgs.as_slice()
    }
    /// The messages that wait to be taken, with what they hold.
    pub fn outgoing(&self) -> &Outgoing {
        &self.msgs
    }
    /// The bytes of the entries this member holds approved by itself
    /// ([`crate::fast`]), as counted.
    pub fn held_bytes(&self) -> usize {
        self.held.bytes()
    }
    /// Reads that may be served once their index is applied, not yet
    /// taken.
    pub fn read_states(&self) -> &[ReadState] {
        &self.read_states
    }
    /// How many reads wait for their quorum.
    pub fn pending_read_count(&self) -> usize {
        self.read_only.len()
    }
    /// How many reads wait to be taken.
    pub fn ready_read_count(&self) -> usize {
        self.read_states.len()
    }
    /// Ticks since this member last heard its leader, or last campaigned.
    pub fn election_elapsed(&self) -> usize {
        self.election_elapsed
    }
    /// The ticks this member waits, in this term, before it campaigns:
    /// drawn from `[election_tick, 2 election_tick)` by [`Config::seed`].
    pub fn randomized_election_timeout(&self) -> usize {
        self.randomized_election_timeout
    }
    /// The commit this member's storage states durably, as its owner said:
    /// the commit of the last durable write's hard state, or a later one
    /// the owner made durable ([`crate::RawNode::commit_durable`]).
    pub fn durable_commit(&self) -> u64 {
        self.durable_commit
    }
    /// What this member's durable log may lack of what it acknowledged; none once it holds it.
    pub fn lost(&self) -> Option<Lost> {
        self.lost
    }
    /// The mark ends once the durable log holds what it marks ([`Lost::resolved_by`]): read
    /// at open and at every notice, from storage, which holds what is durable.
    #[inline]
    pub(crate) fn settle_lost(&mut self) -> Result<()> {
        match self.lost {
            None => Ok(()),
            Some(lost) => self.end_lost(lost),
        }
    }
    fn end_lost(&mut self, lost: Lost) -> Result<()> {
        let store = self.log.store();
        let last = store.last_index()?;
        if lost.resolved_by(last, store.term(last)?) {
            self.lost = None;
        }
        Ok(())
    }
    /// Whether this member may campaign as far as what it lost goes (core
    /// step R-7, `docs/durable.md` §5.2). One whose log may lack what it
    /// acknowledged campaigns on its log, and its own vote is not counted:
    /// elected, a quorum of the others answered for no more than its log
    /// holds, so every entry ever committed is in its log (every commit's
    /// quorum meets that one in a member that held the entry, or holds it
    /// under its mark, and answered for it). So only where the others can
    /// be a quorum of each half of its configuration, and not in a fast
    /// group, whose held proposals the argument does not cover. A sole
    /// voter, or one of two, that lost what it acknowledged waits.
    fn may_campaign(&self) -> bool {
        if self.lost.is_none() {
            return true;
        }
        let id = self.id;
        !self.config.fast && self.tracker.quorum_of(|member| member != id)
    }
    /// The last entry this member answers for in an election, `(index, term)`: its log's last,
    /// or what it marks lost, which is later (`docs/durable.md` §5). It may have acknowledged
    /// through the mark, and been counted toward a commit there, so it votes for no log behind
    /// it.
    fn claim(&self) -> Result<(u64, u64)> {
        match self.lost {
            Some(lost) => Ok((lost.index, lost.term)),
            None => Ok((self.log.last_index()?, self.log.last_term()?)),
        }
    }
    /// A write stating `commit` is durable. The durable commit never goes
    /// back.
    pub(crate) fn commit_durable(&mut self, commit: u64) {
        self.durable_commit = self.durable_commit.max(commit);
    }
    pub(crate) fn committed_bytes_per_ready(&self) -> u64 {
        self.config.max_committed_size_per_ready
    }
    /// The bytes of proposals this leader holds uncommitted, as
    /// [`Config::max_uncommitted_size`] counts them.
    pub fn uncommitted_bytes(&self) -> usize {
        self.uncommitted_bytes
    }
    /// The snapshot storage does not hold yet.
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.log.unstable().snapshot()
    }
    /// Who leads and what this member is.
    pub fn soft_state(&self) -> SoftState {
        SoftState {
            leader_id: self.leader_id,
            raft_state: self.state,
        }
    }
    /// The term, the vote and the commit, to persist.
    pub fn hard_state(&self) -> HardState {
        HardState {
            term: self.term,
            vote: self.vote,
            commit: self.log.committed(),
        }
    }
    /// Whether this member may campaign: it votes.
    pub fn promotable(&self) -> bool {
        self.promotable
    }
    /// Whether a change may be in the log and not applied.
    pub fn has_pending_conf(&self) -> bool {
        self.pending_conf_index > self.log.applied()
    }
    /// Whether the committed entry is of this member's term: a leader
    /// serves reads only once it is.
    pub fn commit_to_current_term(&self) -> bool {
        self.log
            .term(self.log.committed())
            .is_ok_and(|term| term == self.term)
    }
    /// The priority the owner gave.
    pub fn priority(&self) -> i64 {
        self.priority
    }
    /// The priority votes are judged by now.
    pub fn priority_in_force(&self) -> i64 {
        self.priority_in_force
    }
    /// The priority the owner gives, in force once the member has a term.
    pub fn set_priority(&mut self, priority: i64) {
        self.priority = priority;
        self.settle_priority();
    }
    /// The path to `member` carries `bytes` before it answers: no more of
    /// entries is sent to it ahead of its answers, but for one entry that
    /// is larger. False for a member the configuration does not name. What
    /// is out stays counted; a bound that rose admits more with the
    /// member's next answer, or the leader's next append.
    pub fn set_inflight_bytes(&mut self, member: NodeId, bytes: u64) -> bool {
        match self.tracker.get_mut(member) {
            Some(progress) => {
                progress.inflights.set_byte_cap(bytes);
                true
            }
            None => false,
        }
    }
    /// A member that has no term has no log to defend, and every candidate
    /// is current against it: its priority judges nothing. Nor does that of
    /// a member that may not campaign — one its configuration names no
    /// voter: a refusal for priority is safe because the member that
    /// refuses could be elected itself, and one that cannot would leave a
    /// group that can elect without a leader. (A leader that applied its
    /// own removal, of higher priority than the voter that remained and
    /// had not yet heard the removal committed, refused that voter for
    /// good.) What is in force changes between operations and never within
    /// one.
    pub(crate) fn settle_priority(&mut self) {
        self.priority_in_force = if self.term == 0 || !self.promotable {
            0
        } else {
            self.priority
        };
    }
    /// The ticks this member waits beyond its election timeout before it
    /// campaigns.
    pub fn patience(&self) -> usize {
        self.patience
    }
    /// Give this member `ticks` of patience beyond its election timeout,
    /// for the stalls its owner has seen in itself.
    pub fn set_patience(&mut self, ticks: usize) {
        self.patience = ticks;
    }
    /// For a harness that orders elections.
    pub fn set_randomized_election_timeout(&mut self, ticks: usize) -> Result<()> {
        if ticks < self.config.election_tick || ticks >= self.config.election_tick.saturating_mul(2)
        {
            return Err(Error::Settings(
                "an election timeout outside one to two timeouts",
            ));
        }
        self.randomized_election_timeout = ticks;
        Ok(())
    }
    /// The bytes held, by capacity: what is queued, what is not durable,
    /// and what is known of the members. The queue and what is not yet
    /// durable, which grow without a bound of their own but the limits,
    /// are counted as they change; the rest is bounded by the
    /// configuration and the limits and is walked.
    pub fn resident_bytes(&self) -> usize {
        let messages = self.msgs.resident_bytes();
        let reads = self
            .read_states
            .capacity()
            .saturating_mul(std::mem::size_of::<ReadState>());
        let reads = self.read_states.iter().fold(reads, |bytes, read| {
            bytes.saturating_add(read.request_ctx.capacity())
        });
        std::mem::size_of::<Self>()
            .saturating_add(messages)
            .saturating_add(reads)
            .saturating_add(self.read_only.resident_bytes())
            .saturating_add(self.held.resident_bytes())
            .saturating_add(self.votes.resident_bytes())
            .saturating_add(self.decided.resident_bytes())
            .saturating_add(
                self.displaced.iter().fold(
                    self.displaced
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Entry>()),
                    |bytes, entry| {
                        bytes
                            .saturating_add(entry.data.capacity())
                            .saturating_add(entry.context.capacity())
                    },
                ),
            )
            .saturating_add(self.tracker.resident_bytes())
            .saturating_add(self.log.unstable().resident_bytes())
            .saturating_add(
                self.watch
                    .as_ref()
                    .map_or(0, |watch| watch.resident_bytes()),
            )
    }
    /// Whether every counter that [`Raft::resident_bytes`] trusts says what
    /// a walk of what it counts says: the queue, what is not yet durable,
    /// and what the fast track holds. An invariant error names the first
    /// that does not.
    pub fn check_accounting(&self) -> Result<()> {
        for (_, progress) in self.tracker.iter() {
            progress.inflights.check()?;
        }
        self.msgs.check()?;
        self.log.unstable().check()?;
        self.held.check()?;
        self.votes.check()
    }
    /// Whether the next tick makes this member campaign: it is no leader,
    /// it may campaign, and its election timeout and its patience end
    /// with that tick. Asked before the tick, so that its owner knows what
    /// the tick may send.
    pub fn campaigns_on_next_tick(&self) -> bool {
        self.watch.is_none()
            && self.state != StateRole::Leader
            && self.promotable
            && self.election_elapsed.saturating_add(1)
                >= self
                    .randomized_election_timeout
                    .saturating_add(self.patience)
    }
    /// Whether the next tick makes this leader send its heartbeats, or ask
    /// itself whether it still has a quorum.
    pub fn beats_on_next_tick(&self) -> bool {
        self.watch.is_none()
            && self.state == StateRole::Leader
            && (self.heartbeat_elapsed.saturating_add(1) >= self.config.heartbeat_tick
                || self.election_elapsed.saturating_add(1) >= self.config.election_tick)
    }

    pub(crate) fn send(&mut self, message: Message) -> Result<()> {
        push(
            &mut self.msgs,
            self.id,
            self.term,
            self.priority_in_force,
            message,
        )
    }
    fn outbox(&mut self) -> (Outbox<'_, S>, &mut Tracker) {
        (
            Outbox {
                log: &self.log,
                msgs: &mut self.msgs,
                id: self.id,
                term: self.term,
                priority: self.priority_in_force,
                max_bytes: self.config.max_size_per_msg,
                max_entries: self.config.limits.entries_per_message,
            },
            &mut self.tracker,
        )
    }
    pub(crate) fn send_append(&mut self, to: NodeId) -> Result<()> {
        let (mut outbox, tracker) = self.outbox();
        if let Some(progress) = tracker.get_mut(to) {
            outbox.append(to, progress, true)?;
        }
        Ok(())
    }
    fn send_append_all(&mut self, to: NodeId) -> Result<()> {
        let window = self.config.max_inflight_msgs;
        let (mut outbox, tracker) = self.outbox();
        if let Some(progress) = tracker.get_mut(to) {
            outbox.append_all(to, progress, window)?;
        }
        Ok(())
    }
    /// To every member but this one, what follows its progress.
    pub fn bcast_append(&mut self) -> Result<()> {
        let (mut outbox, tracker) = self.outbox();
        for position in 0..tracker.len() {
            if let Some((member, progress)) = tracker.at(position)
                && member != outbox.id
            {
                outbox.append(member, progress, true)?;
            }
        }
        Ok(())
    }
    /// A leader sends its heartbeats; any other role does nothing.
    pub fn ping(&mut self) -> Result<()> {
        if self.state == StateRole::Leader {
            self.bcast_heartbeat()?;
        }
        Ok(())
    }
    fn bcast_heartbeat(&mut self) -> Result<()> {
        let mut context = Vec::new();
        let asked = match self.read_only.last_context() {
            Some(last) => {
                context
                    .try_reserve_exact(last.len())
                    .map_err(|_| Error::Memory)?;
                context.extend_from_slice(last);
                true
            }
            None => false,
        };
        self.bcast_heartbeat_with(asked.then_some(context.as_slice()))?;
        // It carried the last read asked: it is a round for every read
        // that waits, and the one that asks again for a round that was
        // lost.
        self.read_only.asked();
        Ok(())
    }
    /// Whether a read waits that no round sent asks for: the member has a
    /// round to send.
    pub fn reads_unasked(&self) -> bool {
        self.state == StateRole::Leader && self.read_only.unasked()
    }
    /// One round of heartbeats for the reads asked since the last one, if
    /// there are any ([`ReadRounds::Shared`]).
    pub fn ask_reads(&mut self) -> Result<()> {
        if self.reads_unasked() {
            self.bcast_heartbeat()?;
        }
        Ok(())
    }
    fn bcast_heartbeat_with(&mut self, context: Option<&[u8]>) -> Result<()> {
        let (mut outbox, tracker) = self.outbox();
        for position in 0..tracker.len() {
            if let Some((member, progress)) = tracker.at(position)
                && member != outbox.id
            {
                outbox.heartbeat(member, progress, context)?;
            }
        }
        Ok(())
    }
    /// Commits what the quorum holds, if it is of this term.
    pub fn maybe_commit(&mut self) -> Result<bool> {
        let index = self.tracker.quorum_index();
        let classic = self.log.maybe_commit(index, self.term)?;
        // What the classic quorum committed may open the next index to the
        // fast one.
        let fast = self.fast_commit()?;
        if !classic && !fast {
            return Ok(false);
        }
        let committed = self.log.committed();
        self.decided.release(committed);
        if let Some(progress) = self.tracker.get_mut(self.id) {
            progress.update_committed(committed);
        }
        Ok(true)
    }
    /// The application applied through `applied`.
    pub fn commit_apply(&mut self, applied: u64) -> Result<()> {
        let before = self.log.applied();
        self.log.applied_to(applied)?;
        if self.told_to_campaign {
            // It is told again by itself if another change waits.
            self.told_to_campaign = false;
            if self.state == StateRole::Follower
                && self.promotable
                && let Err(error) = self.hup(true)
                && error.is_fatal()
            {
                // A campaign it may no longer make (its log may lack what it
                // acknowledged, and the change left the others no quorum
                // without it) is refused, and changes nothing.
                return Err(error);
            }
        }
        // A joint configuration that leaves by itself does so once the
        // leader applied the entry that entered it.
        if self.tracker.configuration().auto_leave()
            && before <= self.pending_conf_index
            && applied >= self.pending_conf_index
            && self.state == StateRole::Leader
        {
            let leave = Entry {
                entry_type: EntryType::EntryConfChangeV2,
                ..Entry::default()
            };
            // An entry that states nothing takes no room and is never
            // refused for it.
            if !self.append_entry(vec![leave])? {
                return Err(Error::Invariant(
                    "the entry that leaves a joint configuration was refused",
                ));
            }
            self.pending_conf_index = self.log.last_index()?;
        }
        // A change that is applied may open the fast track.
        if self.config.fast && self.state == StateRole::Leader && self.maybe_commit()? {
            self.bcast_append()?;
        }
        Ok(())
    }
    fn reset(&mut self, term: u64) -> Result<()> {
        if self.term != term {
            self.term = term;
            self.vote = 0;
        }
        self.leader_id = 0;
        self.reset_randomized_election_timeout();
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        if let Some(watch) = self.watch.as_mut() {
            // A new role or term: the member trusts no leader until it hears
            // one, and a leader's timers start again. Whatever moved it, an
            // election is under way or just lost: it gives that a round
            // before it competes, as a reset gives a whole election timeout
            // on ticks (Raft Figure 2).
            watch.campaign = Arm::Unset { round: true };
            watch.beat = Arm::Off;
            watch.transfer = Arm::Off;
        }
        self.lead_transferee = None;
        self.told_to_campaign = false;
        self.votes.clear();
        self.decided.clear();
        self.tracker.reset_votes();
        self.pending_conf_index = 0;
        self.read_only.clear();
        self.pending_request_snapshot = 0;
        // Only a leader applies before its own write is durable.
        self.log.unpersisted_after = u64::MAX;
        let next = self.log.last_index()?.saturating_add(1);
        let (committed, persisted, id) = (self.log.committed(), self.log.persisted(), self.id);
        for (member, progress) in self.tracker.iter_mut() {
            progress.reset(next);
            if member == id {
                progress.matched = persisted;
                progress.committed_index = committed;
            }
        }
        Ok(())
    }
    /// Whether `bytes` more of proposals may be held uncommitted; counted when
    /// they may.
    fn admit_uncommitted(&mut self, bytes: usize) -> bool {
        if self.config.max_uncommitted_size == u64::MAX {
            return true;
        }
        let limit = usize::try_from(self.config.max_uncommitted_size).unwrap_or(usize::MAX);
        // An entry that states nothing is never refused, and one proposal
        // is always admitted.
        if bytes == 0
            || self.uncommitted_bytes == 0
            || bytes.saturating_add(self.uncommitted_bytes) <= limit
        {
            self.uncommitted_bytes = self.uncommitted_bytes.saturating_add(bytes);
            true
        } else {
            false
        }
    }
    /// The last index when this member last became leader: entries above it
    /// are its own proposals, counted uncommitted until given to apply.
    pub(crate) fn leader_tail(&self) -> u64 {
        self.leader_tail
    }
    /// `bytes` of proposals were committed and given to apply: uncommitted no
    /// more.
    pub(crate) fn reduce_uncommitted_bytes(&mut self, bytes: usize) {
        if self.state != StateRole::Leader || self.config.max_uncommitted_size == u64::MAX {
            return;
        }
        self.uncommitted_bytes = self.uncommitted_bytes.saturating_sub(bytes);
    }
    /// What was committed and given to apply is uncommitted no more.
    pub(crate) fn reduce_uncommitted(&mut self, entries: &[Entry]) {
        if self.state != StateRole::Leader || self.config.max_uncommitted_size == u64::MAX {
            return;
        }
        let bytes = entries
            .iter()
            .filter(|entry| entry.index > self.leader_tail)
            .fold(0usize, |bytes, entry| {
                bytes.saturating_add(entry.data.len())
            });
        self.uncommitted_bytes = self.uncommitted_bytes.saturating_sub(bytes);
    }
    /// A leader's own entries, at the end of its log. False when they
    /// would hold more uncommitted than the leader may.
    pub(crate) fn append_entry(&mut self, entries: Vec<Entry>) -> Result<bool> {
        self.append_entries(entries, false)
    }
    /// `recovered` entries are what a leader finds the voters hold when it
    /// is elected: it takes them whatever it may hold uncommitted, for it
    /// may not lead without them.
    pub(crate) fn append_entries(
        &mut self,
        mut entries: Vec<Entry>,
        recovered: bool,
    ) -> Result<bool> {
        let last = self.log.last_index()?;
        let count = u64::try_from(entries.len()).unwrap_or(u64::MAX);
        if last.checked_add(count).is_none_or(|end| end == u64::MAX) {
            return Err(Error::Capacity("the log's indexes"));
        }
        let bytes = entries.iter().fold(0usize, |bytes, entry| {
            bytes.saturating_add(entry.data.len())
        });
        if recovered {
            if self.config.max_uncommitted_size != u64::MAX {
                self.uncommitted_bytes = self.uncommitted_bytes.saturating_add(bytes);
            }
        } else if !self.admit_uncommitted(bytes) {
            return Ok(false);
        }
        let mut index = last;
        for entry in &mut entries {
            index = index.saturating_add(1);
            entry.term = self.term;
            entry.index = index;
        }
        // The proposals move into the log: a leader copies nothing of its own.
        if let Err(error) = self.log.append_owned(entries) {
            if self.config.max_uncommitted_size != u64::MAX {
                self.uncommitted_bytes = self.uncommitted_bytes.saturating_sub(bytes);
            }
            return Err(error);
        }
        // The leader's own progress moves when the entries are durable.
        Ok(true)
    }
    /// Storage holds the entries through `(index, term)`.
    pub fn on_persist_entries(&mut self, index: u64, term: u64) -> Result<()> {
        if !self.log.maybe_persist(index, term) || self.state != StateRole::Leader {
            return Ok(());
        }
        let moved = self
            .tracker
            .get_mut(self.id)
            .is_some_and(|progress| progress.maybe_update(index));
        if moved && self.maybe_commit()? && self.should_bcast_commit() {
            self.bcast_append()?;
        }
        Ok(())
    }
    /// Storage holds the snapshot at `index`.
    pub fn on_persist_snapshot(&mut self, index: u64) -> Result<()> {
        self.log.maybe_persist_snapshot(index).map(|_| ())
    }
    fn should_bcast_commit(&self) -> bool {
        !self.config.skip_bcast_commit || self.has_pending_conf()
    }

    /// A refusal of what a member asks of itself changes nothing and is no
    /// one's to hear; what stops the replica is.
    fn local(&mut self, kind: MessageType) -> Result<()> {
        let mut message = proto::message(0, kind);
        message.from = self.id;
        match self.step(message) {
            Err(error) if error.is_fatal() => Err(error),
            _ => Ok(()),
        }
    }
    /// One tick of the owner's clock. True when there may be something to
    /// take.
    pub fn tick(&mut self) -> Result<bool> {
        if self.watch.is_some() {
            return Err(Error::Settings("elections by suspicion take no ticks"));
        }
        match self.state {
            StateRole::Leader => self.tick_heartbeat(),
            _ => self.tick_election(),
        }
    }
    fn tick_election(&mut self) -> Result<bool> {
        self.election_elapsed = self.election_elapsed.saturating_add(1);
        if self.election_elapsed
            < self
                .randomized_election_timeout
                .saturating_add(self.patience)
            || !self.promotable
        {
            return Ok(false);
        }
        self.election_elapsed = 0;
        self.local(MessageType::MsgHup)?;
        Ok(true)
    }
    fn tick_heartbeat(&mut self) -> Result<bool> {
        self.heartbeat_elapsed = self.heartbeat_elapsed.saturating_add(1);
        self.election_elapsed = self.election_elapsed.saturating_add(1);
        let mut ready = false;
        if self.election_elapsed >= self.config.election_tick {
            self.election_elapsed = 0;
            if self.config.check_quorum {
                ready = true;
                self.local(MessageType::MsgCheckQuorum)?;
            }
            // A transfer that did not finish within an election timeout
            // is given up.
            if self.state == StateRole::Leader {
                self.lead_transferee = None;
            }
        }
        if self.state != StateRole::Leader {
            return Ok(ready);
        }
        let id = self.id;
        for (member, progress) in self.tracker.iter_mut() {
            if member != id {
                progress.tick();
            }
        }
        if self.heartbeat_elapsed >= self.config.heartbeat_tick {
            self.heartbeat_elapsed = 0;
            ready = true;
            self.local(MessageType::MsgBeat)?;
        }
        Ok(ready)
    }

    // Elections by suspicion (`crate::watch`, `docs/timing.md` §2.3).

    /// Whether the owner's detectors suspect `member`; never on ticks.
    pub fn suspects(&self, member: NodeId) -> bool {
        self.watch
            .as_ref()
            .is_some_and(|watch| watch.suspects(member))
    }
    /// The members the owner's detectors suspect, in order; none on ticks.
    pub fn suspected(&self) -> &[NodeId] {
        self.watch.as_ref().map_or(&[], |watch| watch.suspected())
    }
    fn watch_mut(&mut self) -> Result<&mut Watch> {
        self.watch
            .as_deref_mut()
            .ok_or(Error::Settings("elections run on ticks"))
    }
    /// The owner's detectors suspect `member`'s node. A follower that knows
    /// it for its leader campaigns after its delay; a leader whose
    /// detectors leave it no quorum steps down, and a transfer to it is
    /// given up. A member never suspects itself.
    pub fn suspect(&mut self, member: NodeId) -> Result<()> {
        let (id, leader, state) = (self.id, self.leader_id, self.state);
        let watch = self.watch_mut()?;
        if member == id || !watch.suspect(member)? {
            return Ok(());
        }
        if state != StateRole::Leader {
            if member == leader && watch.campaign == Arm::Off {
                watch.campaign = Arm::Unset { round: false };
            }
            return Ok(());
        }
        if self.lead_transferee == Some(member) {
            self.lead_transferee = None;
            self.watch_mut()?.transfer = Arm::Off;
        }
        if !self.trusted_quorum() {
            self.step_down()?;
        }
        Ok(())
    }
    /// Check-quorum, from the detectors (Raft §6.2): a leader they leave no
    /// quorum steps down, and hands over.
    fn step_down(&mut self) -> Result<()> {
        self.hand_over()?;
        let term = self.term;
        self.become_follower(term, 0)
    }
    /// A leader that stops leading in its term tells the voter it trusts
    /// that holds the most of its log to campaign at once (a transfer's
    /// order, Raft dissertation §3.10). Its followers trust its node, which
    /// lives, and by suspicion nothing else would make them campaign: the
    /// heir's campaign moves them to its term, where they know no leader.
    /// An heir that cannot win leaves them so all the same, and they elect
    /// among themselves.
    ///
    /// Among heirs that hold as much, it takes them in turn, one a hand-over
    /// (`Watch::handovers`): one that restarted knows nothing of what its
    /// followers hold, and the heir it names first may be one that cannot
    /// campaign (a learner by its own configuration) or cannot win, while
    /// its followers keep their lease on it. A schedule found that with a
    /// leader that restarted marked (R-7, seed 75 of the faults at rest by
    /// suspicion): it named the same heir for good.
    fn hand_over(&mut self) -> Result<()> {
        let configuration = self.tracker.configuration();
        let heirs = || {
            self.tracker.iter().filter(|(member, _)| {
                *member != self.id && configuration.votes(*member) && !self.suspects(*member)
            })
        };
        let Some(most) = heirs().map(|(_, progress)| progress.matched).max() else {
            return Ok(());
        };
        let tied = || heirs().filter(|(_, progress)| progress.matched == most);
        let turn = self
            .watch
            .as_ref()
            .map_or(0, |watch| usize::try_from(watch.handovers).unwrap_or(0))
            .checked_rem(tied().count())
            .unwrap_or(0);
        let Some(heir) = tied().map(|(member, _)| member).nth(turn) else {
            return Ok(());
        };
        if let Some(watch) = self.watch.as_deref_mut() {
            watch.handovers = watch.handovers.wrapping_add(1);
        }
        self.send(proto::message(heir, MessageType::MsgTimeoutNow))
    }
    /// The owner's detectors saw `member`'s node start again: it is
    /// trusted, and leads nothing it led before it stopped. A follower that
    /// knew it for its leader knows no leader now, and campaigns after its
    /// delay unless one is elected or the restarted member's own campaign
    /// reaches it first; a leader tells it who leads.
    pub fn restarted(&mut self, member: NodeId) -> Result<()> {
        self.watch_mut()?.trust(member);
        if member == self.id {
            return Ok(());
        }
        if self.state == StateRole::Leader {
            // What was in flight to it went with the incarnation that
            // stopped: its window is empty, and it is probed from what it
            // is known to hold, not waited on.
            if let Some(progress) = self.tracker.get_mut(member) {
                progress.become_probe();
            }
            return self.probe(member);
        }
        if self.leader_id == member {
            self.leader_id = 0;
        }
        Ok(())
    }
    /// The owner's detectors trust `member`'s node again. A follower that
    /// knows it for its leader no longer campaigns for it; a leader tells
    /// it who leads, so a member cut off while the group was idle catches
    /// up.
    pub fn trust(&mut self, member: NodeId) -> Result<()> {
        if !self.watch_mut()?.trust(member) {
            return Ok(());
        }
        if self.state == StateRole::Leader && member != self.id {
            self.probe(member)?;
        }
        Ok(())
    }
    /// What the owner's measurements give this group's elections
    /// ([`Timing`]). A member that has none draws no delay and beats on no
    /// round: a sole voter campaigns without it, and the others wait for
    /// it, as hyper-timing's law chooses no span before a quorum's paths
    /// are measured (`docs/timing.md` §2.3).
    pub fn set_timing(&mut self, timing: Timing) -> Result<()> {
        self.watch_mut()?.timing = Some((nanos(timing.span), nanos(timing.round)));
        Ok(())
    }
    /// The timing the owner last gave, if any.
    pub fn timing(&self) -> Option<Timing> {
        let (span, round) = self.watch.as_ref()?.timing?;
        Some(Timing {
            span: std::time::Duration::from_nanos(span),
            round: std::time::Duration::from_nanos(round),
        })
    }
    /// The owner holds this member's campaigns, or lets them go: it is
    /// stalled for room (`docs/durable.md` §8). A log that may lack what it
    /// acknowledged is the core's to judge since R-7 (`may_campaign`, §5.2):
    /// held for it, a marked member could not be elected where the rule
    /// admits it. Everything else goes on: it trusts and suspects, votes,
    /// and steps down as a leader.
    pub fn hold_campaigns(&mut self, held: bool) -> Result<()> {
        self.watch_mut()?.held = held;
        Ok(())
    }
    /// A heartbeat to `member` alone: it learns who leads, and answers
    /// where its log ends.
    fn probe(&mut self, member: NodeId) -> Result<()> {
        let (mut outbox, tracker) = self.outbox();
        if let Some(progress) = tracker.get_mut(member) {
            outbox.heartbeat(member, progress, None)?;
        }
        Ok(())
    }
    /// Whether this member and those it trusts are a quorum of each half of
    /// its configuration; always on ticks.
    fn trusted_quorum(&self) -> bool {
        let Some(watch) = self.watch.as_ref() else {
            return true;
        };
        let (id, whole) = (self.id, self.lost.is_none());
        // A member whose log may lack what it acknowledged is no part of
        // its own quorum (R-7).
        self.tracker.quorum_of(|member| {
            if member == id {
                whole
            } else {
                !watch.suspects(member)
            }
        })
    }
    /// Whether a leader's group has work in flight, which its beats
    /// recover if a message of it is lost: a transfer, a read, or a member
    /// it trusts that is behind it, has not said it holds the commit, is
    /// probed, sent a snapshot or has messages out. A member it suspects is
    /// left out, as CockroachDB quiesces a range whose behind replicas are
    /// on dead nodes; it is told again once trusted.
    fn active(&self) -> Result<bool> {
        if self.lead_transferee.is_some() || !self.read_only.is_empty() {
            return Ok(true);
        }
        let Some(watch) = self.watch.as_ref() else {
            return Ok(true);
        };
        let (last, committed, id) = (self.log.last_index()?, self.log.committed(), self.id);
        Ok(self.tracker.iter().any(|(member, progress)| {
            member != id
                && !watch.suspects(member)
                && (progress.matched < last
                    || progress.committed_index < committed
                    || progress.state != ProgressState::Replicate
                    || progress.inflights.count() > 0
                    || progress.pending_request_snapshot != 0)
        }))
    }
    /// A leader's beat by suspicion: a round of heartbeats, each member's
    /// wait counted as a tick's was.
    fn beat(&mut self) -> Result<()> {
        let id = self.id;
        for (member, progress) in self.tracker.iter_mut() {
            if member != id {
                progress.tick();
            }
        }
        self.local(MessageType::MsgBeat)
    }
    /// When the member is next to be woken ([`Raft::wake`]), on the owner's
    /// clock: a campaign, a leader's beat or a transfer's end. None when
    /// nothing is timed: a follower that trusts its leader, a leader with
    /// nothing in flight, a member whose campaigns are held or whose
    /// detectors leave it no quorum, or on ticks.
    pub fn deadline(&self) -> Option<u64> {
        let watch = self.watch.as_ref()?;
        let campaigns =
            self.promotable && !watch.held && self.may_campaign() && self.trusted_quorum();
        let campaign = if campaigns || (watch.led == self.term && self.term != 0) {
            Watch::due(watch.campaign)
        } else {
            None
        };
        [campaign, Watch::due(watch.beat), Watch::due(watch.transfer)]
            .into_iter()
            .flatten()
            .min()
    }
    /// The owner's clock reads `now`, nanoseconds: what was armed since
    /// the last wake is timed from it, and what is due is done. True when
    /// the member acted: it campaigned, stepped down, beat or gave up a
    /// transfer. Nothing on ticks.
    pub fn wake(&mut self, now: u64) -> Result<bool> {
        if self.watch.is_none() {
            return Ok(false);
        }
        let mut acted = false;
        if self.state == StateRole::Leader {
            if self.trusted_quorum() {
                return self.wake_leader(now);
            }
            self.step_down()?;
            acted = true;
        }
        Ok(self.wake_follower(now)? || acted)
    }
    fn wake_leader(&mut self, now: u64) -> Result<bool> {
        let active = self.active()?;
        let watch = self.watch_mut()?;
        let (round, beat, transfer) = (watch.round(), watch.beat, watch.transfer);
        watch.campaign = Arm::Off;
        let mut acted = false;
        let transfer = match (self.lead_transferee, transfer, round) {
            (None, ..) => Arm::Off,
            (Some(_), Arm::At(at), _) if now >= at => {
                // Not finished within its rounds: given up (§3.10).
                self.lead_transferee = None;
                acted = true;
                Arm::Off
            }
            (Some(_), Arm::At(at), _) => Arm::At(at),
            (Some(_), _, Some(round)) => {
                Arm::At(now.saturating_add(round.saturating_mul(TRANSFER_ROUNDS)))
            }
            (Some(_), _, None) => Arm::Unset { round: false },
        };
        let beat = match (active, beat, round) {
            (false, ..) => Arm::Off,
            (true, _, None) => Arm::Unset { round: false },
            (true, Arm::At(at), Some(round)) if now >= at => {
                self.beat()?;
                acted = true;
                Arm::At(now.saturating_add(round))
            }
            (true, Arm::At(at), Some(_)) => Arm::At(at),
            (true, _, Some(round)) => Arm::At(now.saturating_add(round)),
        };
        // A beat may have stepped the member down (a heartbeat's answer of
        // a later term arrives later, never within it); its timers stand
        // as a leader's only while it leads.
        if self.state == StateRole::Leader {
            let watch = self.watch_mut()?;
            watch.beat = beat;
            watch.transfer = transfer;
        }
        Ok(acted)
    }
    /// The low and high bounds of the committed entries not yet applied,
    /// and whether a change of the configuration is among them: a member
    /// does not campaign on a configuration it has not applied. A snapshot
    /// not yet durable states its own.
    fn change_unapplied(&self) -> Result<bool> {
        let low = match self.log.unstable().snapshot() {
            Some(snapshot) => proto::snapshot_index(snapshot).saturating_add(1),
            None => self.log.applied().saturating_add(1),
        };
        let high = self.log.committed().saturating_add(1);
        self.has_unapplied_conf_changes(low, high)
    }
    /// It follows `leader` in its term: if it thought it might have led
    /// this term, it did not, for a term has one leader.
    fn followed(&mut self, leader: NodeId) {
        let (id, term) = (self.id, self.term);
        if let Some(watch) = self.watch.as_mut()
            && watch.led == term
            && leader != id
        {
            watch.led = 0;
        }
    }
    /// Whether this member trusts a leader: it knows one, its detectors do
    /// not suspect it, and the configuration it applied names it a voter (a
    /// leader that is none steps down once it applies that, and a member
    /// that applied it knows the change was committed).
    fn trusts_leader(&self, watch: &Watch) -> bool {
        self.leader_id != 0
            && !watch.suspects(self.leader_id)
            && self.tracker.configuration().votes(self.leader_id)
    }
    fn wake_follower(&mut self, now: u64) -> Result<bool> {
        let (promotable, local, term) = (self.promotable, self.config.seed, self.term);
        let may = self.may_campaign();
        let alone = self.tracker.is_singleton() && self.tracker.configuration().votes(self.id);
        let quorum = self.trusted_quorum();
        let trusts = self
            .watch
            .as_deref()
            .is_some_and(|watch| self.trusts_leader(watch));
        let watch = self.watch_mut()?;
        watch.beat = Arm::Off;
        watch.transfer = Arm::Off;
        // It led this term and leads no more: until another leader or a
        // later term reaches it, its followers may trust it still.
        let led = watch.led == term && term != 0;
        if trusts || !(promotable || led) {
            watch.campaign = Arm::Off;
            return Ok(false);
        }
        let armed = match watch.campaign {
            // Its leader stopped leading (it asked for votes, or started
            // again): a round for its campaign.
            Arm::Off => Arm::Unset { round: true },
            armed => armed,
        };
        watch.campaign = match armed {
            Arm::Unset { round } => match (watch.round(), watch.draw(local)) {
                (Some(tail), Some(delay)) => Arm::At(
                    now.saturating_add(if round { tail } else { 0 })
                        .saturating_add(delay),
                ),
                _ => armed,
            },
            armed => armed,
        };
        // A sole voter has no one to split a vote with, nor a leader to
        // suspect: it campaigns at once, timed or not.
        let due = (alone && promotable)
            || match watch.campaign {
                Arm::At(at) => now >= at,
                Arm::Apply => true,
                Arm::Off | Arm::Unset { .. } => false,
            };
        if !due {
            return Ok(false);
        }
        let campaigns = promotable && quorum && may && !watch.held;
        if !campaigns {
            if !led {
                return Ok(false);
            }
            // It cannot campaign, and its followers trust it: it hands over
            // again, a round and a draw after the last, as a candidate asks
            // again, for the order may have been lost.
            watch.campaign = match (watch.round(), watch.draw(local)) {
                (Some(tail), Some(delay)) => {
                    Arm::At(now.saturating_add(tail).saturating_add(delay))
                }
                _ => Arm::Unset { round: true },
            };
            self.hand_over()?;
            return Ok(true);
        }
        if self.change_unapplied()? {
            self.watch_mut()?.campaign = Arm::Apply;
            return Ok(false);
        }
        self.watch_mut()?.campaign = Arm::Off;
        self.hup(false)?;
        if self.state != StateRole::Leader {
            // Unresolved within a round, it draws again.
            let watch = self.watch_mut()?;
            watch.campaign = match (watch.round(), watch.draw(local)) {
                (Some(tail), Some(delay)) => {
                    Arm::At(now.saturating_add(tail).saturating_add(delay))
                }
                _ => Arm::Unset { round: true },
            };
        }
        Ok(true)
    }

    pub(crate) fn become_follower(&mut self, term: u64, leader: NodeId) -> Result<()> {
        let asked = self.pending_request_snapshot;
        self.reset(term)?;
        self.leader_id = leader;
        self.state = StateRole::Follower;
        self.pending_request_snapshot = asked;
        Ok(())
    }
    fn become_candidate(&mut self) -> Result<()> {
        if self.state == StateRole::Leader {
            return Err(Error::Invariant("a leader that campaigns"));
        }
        let term = self
            .term
            .checked_add(1)
            .filter(|term| *term != u64::MAX)
            .ok_or(Error::Capacity("terms"))?;
        self.reset(term)?;
        self.vote = self.id;
        self.state = StateRole::Candidate;
        Ok(())
    }
    fn become_pre_candidate(&mut self) -> Result<()> {
        if self.state == StateRole::Leader {
            return Err(Error::Invariant("a leader that campaigns"));
        }
        if self.term.checked_add(1).is_none_or(|term| term == u64::MAX) {
            return Err(Error::Capacity("terms"));
        }
        // Asking changes neither the term nor the vote.
        self.state = StateRole::PreCandidate;
        self.tracker.reset_votes();
        self.leader_id = 0;
        Ok(())
    }
    fn become_leader(&mut self) -> Result<()> {
        if self.state == StateRole::Follower {
            return Err(Error::Invariant("a follower that leads"));
        }
        let term = self.term;
        // What the voters said they hold outlives the asking.
        let reports = std::mem::replace(
            &mut self.votes,
            Votes::new(
                usize::try_from(self.config.limits.fast_window).unwrap_or(usize::MAX),
                self.config.limits.vote_bytes,
            ),
        );
        self.reset(term)?;
        self.leader_id = self.id;
        self.state = StateRole::Leader;
        // Elected without its own vote by a quorum that answered for no more
        // than its log holds (R-7): what it lost was committed by no one, and
        // its first entry, of a later term, will hold what the mark marks.
        self.lost = None;
        if let Some(watch) = self.watch.as_mut() {
            watch.led = term;
        }
        // Its log need not be durable yet: a member that is the one voter
        // is elected before its writes are (`docs/durable.md` §2.1). It
        // counts itself by what is durable (`reset`), and that moves only as
        // its writes become durable (I3, `Raft::on_persist_entries`).
        let last = self.log.last_index()?;
        self.uncommitted_bytes = 0;
        self.leader_tail = last;
        if self.config.apply_unpersisted {
            // Every entry after `last` is of this term (`append_entries`).
            self.log.unpersisted_after = last;
        }
        if let Some(progress) = self.tracker.get_mut(self.id) {
            progress.become_replicate();
        }
        // There may be a change in the log that is not applied: none is
        // proposed until all of the log is.
        self.pending_conf_index = last;
        self.note_term_configuration()?;
        let mut first = self.recover(reports, last)?;
        first.try_reserve(1).map_err(|_| Error::Memory)?;
        first.push(Entry::default());
        if !self.append_entries(first, true)? {
            return Err(Error::Invariant("a leader's first entry was refused"));
        }
        let last = self.log.last_index()?;
        self.release_proposals(last)
    }

    fn campaign(&mut self, campaign: Campaign) -> Result<()> {
        let (kind, term) = if campaign == Campaign::PreElection {
            self.become_pre_candidate()?;
            (MessageType::MsgRequestPreVote, self.term.saturating_add(1))
        } else {
            self.become_candidate()?;
            (MessageType::MsgRequestVote, self.term)
        };
        // A member whose log may lack what it acknowledged does not count
        // its own vote (R-7): it is no witness for its own log.
        if self.poll(self.id, self.lost.is_none())? == Tally::Won {
            // The one voter there is.
            return Ok(());
        }
        let (commit, commit_term) = self.log.commit_info()?;
        let (index, log_term) = (self.log.last_index()?, self.log.last_term()?);
        let (id, own_term, priority) = (self.id, self.term, self.priority_in_force);
        let Self { tracker, msgs, .. } = self;
        let configuration = tracker.configuration();
        // Every voter of either half, once.
        for voter in configuration
            .members()
            .filter(|member| *member != id && configuration.votes(*member))
        {
            let mut message = proto::message(voter, kind);
            message.term = term;
            message.index = index;
            message.log_term = log_term;
            message.commit = commit;
            message.commit_term = commit_term;
            if campaign == Campaign::Transfer {
                message.context = CAMPAIGN_TRANSFER.to_vec();
            }
            push(msgs, id, own_term, priority, message)?;
        }
        Ok(())
    }
    fn poll(&mut self, from: NodeId, vote: bool) -> Result<Tally> {
        self.tracker.record_vote(from, vote)?;
        let tally = self.tracker.tally_votes();
        match tally {
            Tally::Won => {
                if self.state == StateRole::PreCandidate {
                    self.campaign(Campaign::Election)?;
                } else {
                    self.become_leader()?;
                    self.bcast_append()?;
                }
            }
            Tally::Lost => {
                let term = self.term;
                self.become_follower(term, 0)?;
            }
            Tally::Pending => {}
        }
        Ok(tally)
    }
    /// Whether `[low, high)` holds a change of the configuration: a yes or
    /// a no, read where the entries are and copying none of them.
    fn has_unapplied_conf_changes(&self, low: u64, high: u64) -> Result<bool> {
        if self.log.applied() >= self.log.committed() {
            return Ok(false);
        }
        self.log.any_entry(low, high, proto::changes_configuration)
    }
    fn hup(&mut self, transfer: bool) -> Result<()> {
        if self.state == StateRole::Leader {
            return Ok(());
        }
        // One that is no voter would count votes it cannot hold, and lead
        // a group it is no member of.
        if !self.promotable {
            return Err(Error::NotPromotable);
        }
        if !self.may_campaign() {
            // Its leader is gone, or it would not be asked: it holds no
            // lease on one.
            self.leader_id = 0;
            return Err(Error::Lost);
        }
        // A member does not campaign on a configuration it has not
        // applied.
        if self.change_unapplied()? {
            // One that was told to campaign does once the change is
            // applied: the leader that told it may have left for it.
            self.told_to_campaign = transfer;
            return Ok(());
        }
        if transfer {
            self.campaign(Campaign::Transfer)
        } else if self.config.pre_vote {
            self.campaign(Campaign::PreElection)
        } else {
            self.campaign(Campaign::Election)
        }
    }

    /// What arrived, or what the member asks of itself.
    pub fn step(&mut self, message: Message) -> Result<()> {
        if self.msgs.len() >= self.config.limits.pending_messages {
            return Err(Error::Capacity("messages that wait to be taken"));
        }
        if message.msg_type == fast::FAST_PROPOSE {
            return self.hear_proposal(message);
        }
        if message.msg_type == fast::FAST_VOTE {
            return self.step_fast_vote(message);
        }
        let kind = message.msg_type;
        if counts_beyond_bound(&message) {
            return Err(Error::Violation(
                "a term or an index beyond what is counted",
            ));
        }
        if self.step_term(kind, &message)? == TermChecked::Done {
            return Ok(());
        }

        match kind {
            MessageType::MsgHup => self.hup(false),
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
                self.step_vote(kind, &message)
            }
            _ => match self.state {
                StateRole::PreCandidate | StateRole::Candidate => {
                    self.step_candidate(kind, message)
                }
                StateRole::Follower => self.step_follower(kind, message),
                StateRole::Leader => self.step_leader(kind, message),
            },
        }
    }

    /// Holds a message's term against this member's: a newer one moves the
    /// member's term, an older one is answered and goes no further.
    fn step_term(&mut self, kind: MessageType, message: &Message) -> Result<TermChecked> {
        if message.term == 0 {
            // The member's own, or forwarded to it.
            Ok(TermChecked::Handle)
        } else if message.term > self.term {
            self.step_newer_term(kind, message)
        } else if message.term < self.term {
            self.step_older_term(kind, message)?;
            Ok(TermChecked::Done)
        } else {
            Ok(TermChecked::Handle)
        }
    }

    /// A message of a term newer than this member's.
    fn step_newer_term(&mut self, kind: MessageType, message: &Message) -> Result<TermChecked> {
        if matches!(
            kind,
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
        ) {
            let force = message.context.as_slice() == CAMPAIGN_TRANSFER;
            let in_lease = match self.watch.as_ref() {
                None => {
                    self.config.check_quorum
                        && self.leader_id != 0
                        && self.election_elapsed < self.config.election_tick
                }
                // While it leads, or trusts the leader it knows (Ongaro
                // §9.6) and the request is not that leader's own. A member
                // asks for votes only while it does not lead, and terms
                // only rise at it: a request for a later term from the
                // leader of this one says it leads no more (it stepped down
                // for want of a quorum, or restarted), which its node's
                // detector, trusting a node that lives, cannot say.
                // Nor when the asker was already in a later term than this
                // member when it asked (a request names the term it asks
                // for, one past its own): with pre-vote a term moves only
                // by a campaign a majority let through, and the leader of
                // this member's term is deposed once it and the asker
                // speak. On ticks the lease would have run out; by
                // suspicion only its leader's detector could end it, and it
                // trusts a node that lives.
                Some(watch) => {
                    self.state == StateRole::Leader
                        || (self.leader_id != message.from
                            && message.term <= self.term.saturating_add(1)
                            && self.trusts_leader(watch))
                }
            };
            if self.watch.is_some()
                && self.state != StateRole::Leader
                && self.leader_id == message.from
            {
                // Its leader leads no more: the member trusts no leader, and
                // campaigns itself after its delay if no one else is
                // elected first. Its log or its priority may be the one the
                // group needs.
                self.leader_id = 0;
            }
            if !force && in_lease {
                // One that heard its leader within an election timeout
                // neither moves its term nor votes: a member removed
                // from the group cannot disturb it.
                if self.watch.is_some()
                    && self.state == StateRole::Leader
                    && self.tracker.get(message.from).is_some()
                {
                    // It asks because it knows no leader: tell it.
                    self.probe(message.from)?;
                }
                return Ok(TermChecked::Done);
            }
        }
        let granted = kind == MessageType::MsgRequestPreVoteResponse && !message.reject;
        if kind == MessageType::MsgRequestPreVote || granted {
            // Asking moves no term, and neither does being granted:
            // the term moves when the election is held.
        } else if matches!(
            kind,
            MessageType::MsgAppend | MessageType::MsgHeartbeat | MessageType::MsgSnapshot
        ) {
            self.become_follower(message.term, message.from)?;
        } else {
            self.become_follower(message.term, 0)?;
        }
        Ok(TermChecked::Handle)
    }

    /// A message of a term older than this member's.
    fn step_older_term(&mut self, kind: MessageType, message: &Message) -> Result<()> {
        // By suspicion, an order to campaign too: a member that stopped
        // leading hands over until its term moves (`Raft::wake`), and this
        // answer is how it learns that it has.
        let handed = kind == MessageType::MsgTimeoutNow && self.watch.is_some();
        if (self.config.check_quorum || self.config.pre_vote)
            && (matches!(kind, MessageType::MsgHeartbeat | MessageType::MsgAppend) || handed)
        {
            // A leader of an older term: this member moved its term
            // while it was cut off. Its answer tells that leader, which
            // no vote request of this member would, refused as they
            // are while the leader is heard.
            self.send(proto::message(message.from, MessageType::MsgAppendResponse))?;
        } else if kind == MessageType::MsgRequestPreVote {
            // Answered and not dropped: a candidate of an older term
            // that hears nothing would ask for ever.
            let mut answer = proto::message(message.from, MessageType::MsgRequestPreVoteResponse);
            answer.term = self.term;
            answer.reject = true;
            self.send(answer)?;
        }
        Ok(())
    }

    fn step_vote(&mut self, kind: MessageType, message: &Message) -> Result<()> {
        let answer_kind = if kind == MessageType::MsgRequestVote {
            MessageType::MsgRequestVoteResponse
        } else {
            MessageType::MsgRequestPreVoteResponse
        };
        // A vote already cast for the asker is cast again; else the member
        // has cast none and knows no leader of this term; or it is asked
        // about a term to come.
        let can_vote = self.vote == message.from
            || (self.vote == 0 && self.leader_id == 0)
            || (kind == MessageType::MsgRequestPreVote && message.term > self.term);
        // Priority orders candidates whose logs are equally long. It never
        // judges a transfer, which is a decision that a member shall lead.
        let transfer =
            kind == MessageType::MsgRequestVote && message.context.as_slice() == CAMPAIGN_TRANSFER;
        // Judged against what this member answers for: its log, or what it marks lost.
        let (last_index, last_term) = self.claim()?;
        let ahead = match self.config.precedence {
            Precedence::Log => {
                message.log_term > last_term
                    || (message.log_term == last_term && message.index > last_index)
            }
            Precedence::Length => message.index > last_index,
        };
        // A member whose log may lack what it acknowledged refuses no one
        // for priority: a voter that refuses for priority must be one the
        // group could elect instead (`Precedence::Log`), and a marked member
        // may not be (a schedule, seed 560, found one of the highest
        // priority refusing every candidate it was not behind). Judged
        // here, where votes are, and not settled on every operation.
        let priority = if self.lost.is_some() {
            0
        } else {
            self.priority_in_force
        };
        let ranked = transfer || ahead || priority <= priority_of(message);
        let current = message.log_term > last_term
            || (message.log_term == last_term && message.index >= last_index);
        if can_vote && ranked && current {
            // With the asker's term: the member's own is behind it when
            // it is asked about a term to come, and the asker drops what
            // is behind.
            let mut answer = proto::message(message.from, answer_kind);
            answer.term = message.term;
            if kind == MessageType::MsgRequestVote && self.config.fast {
                // With the vote, what this member approved by itself: one
                // that is elected decides by it what the group holds
                // above its log.
                crate::log::copy_entries_of(self.held.iter(), &mut answer.entries)?;
            }
            self.send(answer)?;
            if kind == MessageType::MsgRequestVote {
                self.election_elapsed = 0;
                self.vote = message.from;
                if let Some(watch) = self.watch.as_mut() {
                    // Granting a vote starts the wait again (Raft Figure 2),
                    // a round for the candidate to win and be heard.
                    watch.campaign = Arm::Unset { round: true };
                }
            }
            return Ok(());
        }
        let mut answer = proto::message(message.from, answer_kind);
        answer.reject = true;
        answer.term = self.term;
        let (commit, commit_term) = self.log.commit_info()?;
        answer.commit = commit;
        answer.commit_term = commit_term;
        if answer.term == 0 {
            // Only one that has no term and refuses all the same: it has
            // nothing to say that the asker could hear.
            return Ok(());
        }
        self.send(answer)?;
        self.maybe_commit_by_vote(message)
    }
    /// A vote says what its sender knows committed.
    fn maybe_commit_by_vote(&mut self, message: &Message) -> Result<()> {
        if message.commit == 0 || message.commit_term == 0 {
            return Ok(());
        }
        let before = self.log.committed();
        if message.commit <= before || self.state == StateRole::Leader {
            return Ok(());
        }
        if !self.log.maybe_commit(message.commit, message.commit_term)? {
            return Ok(());
        }
        if !matches!(self.state, StateRole::Candidate | StateRole::PreCandidate) {
            return Ok(());
        }
        // A candidate counts votes by a configuration; one that learns of
        // a change it has not applied gives the election up.
        let high = self.log.committed().saturating_add(1);
        if self.has_unapplied_conf_changes(before.saturating_add(1), high)? {
            let term = self.term;
            self.become_follower(term, 0)?;
        }
        Ok(())
    }

    fn step_leader(&mut self, kind: MessageType, mut message: Message) -> Result<()> {
        match kind {
            MessageType::MsgBeat => self.bcast_heartbeat(),
            MessageType::MsgCheckQuorum => {
                if !self.tracker.quorum_recently_active(self.id) {
                    let term = self.term;
                    self.become_follower(term, 0)?;
                }
                Ok(())
            }
            MessageType::MsgPropose => self.propose(&mut message),
            MessageType::MsgReadIndex => self.read_index(message),
            MessageType::MsgAppendResponse => self.handle_append_response(&message),
            MessageType::MsgHeartbeatResponse => self.handle_heartbeat_response(&message),
            MessageType::MsgSnapStatus => {
                self.handle_snapshot_status(&message);
                Ok(())
            }
            MessageType::MsgUnreachable => {
                // What was sent ahead is likely lost; a probe that was, is
                // sent again.
                if let Some(progress) = self.tracker.get_mut(message.from) {
                    match progress.state {
                        ProgressState::Replicate => progress.become_probe(),
                        ProgressState::Probe
                            if self.config.heartbeat_answers == HeartbeatAnswers::Position =>
                        {
                            progress.paused = false;
                            progress.stalled = 0;
                        }
                        ProgressState::Probe | ProgressState::Snapshot => {}
                    }
                }
                Ok(())
            }
            MessageType::MsgTransferLeader => self.handle_transfer_leader(&message),
            _ => Ok(()),
        }
    }
    fn propose(&mut self, message: &mut Message) -> Result<()> {
        if message.entries.is_empty() {
            return Err(Error::ProposalDropped);
        }
        if self.tracker.get(self.id).is_none() || self.lead_transferee.is_some() {
            return Err(Error::ProposalDropped);
        }
        let last = self.log.last_index()?;
        let mut pending = self.pending_conf_index;
        let mut index = last;
        for entry in &mut message.entries {
            index = index.saturating_add(1);
            let plan = match Plan::of_entry(entry) {
                Ok(None) => continue,
                Ok(Some(plan)) => plan,
                Err(_) => return Err(Error::ProposalDropped),
            };
            let joint = self.tracker.configuration().is_joint();
            let leaves = plan.stated == 0;
            if pending > self.log.applied() || joint != leaves {
                // One change at a time, and out of a joint configuration
                // before into another: the entry keeps its place and
                // states nothing.
                *entry = Entry::default();
            } else {
                pending = index;
            }
        }
        let entries = std::mem::take(&mut message.entries);
        if !self.append_entry(entries)? {
            return Err(Error::ProposalDropped);
        }
        self.pending_conf_index = pending;
        self.bcast_append()
    }
    fn read_index(&mut self, mut message: Message) -> Result<()> {
        // A leader knows what is committed once it committed in its term.
        if !self.commit_to_current_term() {
            return Ok(());
        }
        let Some(entry) = message.entries.first_mut() else {
            return Ok(());
        };
        let context = std::mem::take(&mut entry.data);
        let committed = self.log.committed();
        if self.tracker.is_singleton() {
            return self.answer_read(message.from, committed, context);
        }
        if self.read_only.len().saturating_add(self.read_states.len())
            >= self.config.limits.pending_reads
        {
            return Err(Error::Capacity("reads that wait for their quorum"));
        }
        match self.config.read_rounds {
            // No round is sent for it here. One leaves when the member is
            // next asked what there is to do (`ask_reads`), carrying the
            // last read asked by then.
            ReadRounds::Shared => self
                .read_only
                .add(committed, context, message.from, self.id),
            ReadRounds::Each => {
                let mut heartbeat = Vec::new();
                heartbeat
                    .try_reserve_exact(context.len())
                    .map_err(|_| Error::Capacity("reads that wait for their quorum"))?;
                heartbeat.extend_from_slice(&context);
                self.read_only
                    .add(committed, context, message.from, self.id)?;
                self.bcast_heartbeat_with(Some(&heartbeat))?;
                self.read_only.asked();
                Ok(())
            }
        }
    }
    /// The read may be served at `index`: said here, or to who asked.
    fn answer_read(&mut self, from: NodeId, index: u64, context: Vec<u8>) -> Result<()> {
        if from == 0 || from == self.id {
            if self.read_states.len() >= self.config.limits.pending_reads {
                return Err(Error::Capacity("reads that wait to be taken"));
            }
            self.read_states.try_reserve(1).map_err(|_| Error::Memory)?;
            self.read_states.push(ReadState {
                index,
                request_ctx: context,
            });
            return Ok(());
        }
        let mut answer = proto::message(from, MessageType::MsgReadIndexResp);
        answer.index = index;
        answer
            .entries
            .try_reserve_exact(1)
            .map_err(|_| Error::Memory)?;
        answer.entries.push(Entry {
            data: context,
            ..Entry::default()
        });
        self.send(answer)
    }
    /// Every read through `context` is confirmed.
    fn confirm_reads(&mut self, context: &[u8]) -> Result<()> {
        let mut confirmed = Vec::new();
        confirmed
            .try_reserve_exact(self.read_only.len())
            .map_err(|_| Error::Memory)?;
        confirmed.extend(self.read_only.advance(context));
        for read in confirmed {
            // Every asker of the read is answered; the context is copied
            // for all but the last.
            let (origins, index, context) = read.into_parts();
            let last = origins.len().saturating_sub(1);
            for (position, from) in origins.into_iter().enumerate() {
                if position == last {
                    self.answer_read(from, index, context)?;
                    break;
                }
                let mut copy = Vec::new();
                copy.try_reserve_exact(context.len())
                    .map_err(|_| Error::Memory)?;
                copy.extend_from_slice(&context);
                self.answer_read(from, index, copy)?;
            }
        }
        Ok(())
    }
    /// A member says it lost entries it acknowledged (core step R-5): if
    /// this leader counts them, its progress goes back to the last entry
    /// the member holds and the entries after it are sent again, not a
    /// snapshot. True when it did. Lowering a member's match revokes no
    /// commit (the commit never goes back, and the entries are this
    /// leader's), so a word that is false costs resends and nothing else.
    fn take_lost(&mut self, message: &Message) -> Result<bool> {
        let held = message.reject_hint;

        if self
            .tracker
            .get(message.from)
            .is_none_or(|progress| progress.matched <= held)
        {
            return Ok(false);
        }
        // What it holds is a prefix of what it acknowledged, which was this
        // leader's log, and a leader never cuts its own: the same entry,
        // where this log still holds it (or its snapshot's point).
        let kept = held.saturating_add(1) >= self.log.first_index()?;
        if kept && self.log.term(held)? != message.log_term {
            return Err(Error::Violation(
                "a member that lost entries holds another log than it acknowledged",
            ));
        }
        if let Some(progress) = self.tracker.get_mut(message.from) {
            progress.lost(held, message.commit);
        }
        self.send_append(message.from)?;
        Ok(true)
    }
    fn handle_append_response(&mut self, message: &Message) -> Result<()> {
        let mut next_probe = message.reject_hint;
        if message.reject {
            // Asked for on a refusal only: an acknowledgement checks nothing
            // more than before.
            if message.lost && self.take_lost(message)? {
                return Ok(());
            }
            if message.log_term > 0 {
                // The member holds `log_term` at its hint. No index of this
                // log at or below the hint with a higher term can match it,
                // for terms only rise along a log: probe at the last one
                // that may.
                next_probe = self
                    .log
                    .find_conflict_by_term(message.reject_hint, message.log_term)?
                    .0;
            }
        }
        let last = self.log.last_index()?;
        let Some(progress) = self.tracker.get_mut(message.from) else {
            return Ok(());
        };
        progress.recent_active = true;
        progress.update_committed(message.commit);
        if message.reject {
            if progress.maybe_decrease_to(message.index, next_probe, message.request_snapshot) {
                if progress.state == ProgressState::Replicate {
                    progress.become_probe();
                }
                self.send_append(message.from)?;
            }
            return Ok(());
        }
        let paused = progress.is_paused();
        if !progress.maybe_update(message.index) {
            return Ok(());
        }
        match progress.state {
            ProgressState::Probe => progress.become_replicate(),
            ProgressState::Snapshot => {
                if progress.is_snapshot_caught_up() {
                    progress.become_probe();
                }
            }
            ProgressState::Replicate => progress.inflights.free_to(message.index),
        }
        if self.maybe_commit()? {
            if self.should_bcast_commit() {
                self.bcast_append()?;
            }
        } else if paused {
            self.send_append(message.from)?;
        }
        // The window may admit several messages now.
        self.send_append_all(message.from)?;
        if self.lead_transferee == Some(message.from)
            && self
                .tracker
                .get(message.from)
                .is_some_and(|progress| progress.matched == last)
        {
            self.send(proto::message(message.from, MessageType::MsgTimeoutNow))?;
        }
        Ok(())
    }
    fn handle_heartbeat_response(&mut self, message: &Message) -> Result<()> {
        let last = self.log.last_index()?;
        let answers = self.config.heartbeat_answers;
        // The member says how far its log goes, and its last entry is of
        // this leader's term: this leader made that entry, and the member
        // took it and everything before it from this leader's appends. It
        // holds this leader's log through that index — what an append's
        // answer says, and it is taken as one: an answer that was lost is
        // made good by the next heartbeat's. Not where entries go by the
        // fast track, whose terms are not the same at every member.
        let held = answers == HeartbeatAnswers::Position
            && message.log_term != 0
            && message.log_term == self.term
            && message.index <= last;
        let news = held
            && !self.config.fast
            && self
                .tracker
                .get(message.from)
                .is_some_and(|progress| progress.matched < message.index);
        if news {
            let mut ack = proto::message(self.id, MessageType::MsgAppendResponse);
            ack.from = message.from;
            ack.index = message.index;
            self.handle_append_response(&ack)?;
        }
        // A beat of the leader's ticks: what was sent a beat ago has had
        // the time, on a path the ticks are stretched by, to be answered.
        // By suspicion a beat is a round tail, time for an answer.
        let beat = if self.watch.is_some() {
            1
        } else {
            self.config.heartbeat_tick.max(1)
        };
        let (mut outbox, tracker) = self.outbox();
        let Some(progress) = tracker.get_mut(message.from) else {
            return Ok(());
        };
        progress.update_committed(message.commit);
        progress.recent_active = true;
        progress.heard_heartbeat(answers, held.then_some(message.index), news, beat);
        if progress.matched < last || progress.pending_request_snapshot != 0 {
            outbox.append(message.from, progress, true)?;
        }
        if message.context.is_empty() {
            return Ok(());
        }
        let confirmed = match self.read_only.ack(message.from, &message.context)? {
            Some(acks) => self.tracker.has_quorum(acks),
            None => false,
        };
        if confirmed {
            self.confirm_reads(&message.context)?;
        }
        Ok(())
    }
    fn handle_snapshot_status(&mut self, message: &Message) {
        let Some(progress) = self.tracker.get_mut(message.from) else {
            return;
        };
        if progress.state != ProgressState::Snapshot {
            return;
        }
        if message.reject {
            progress.snapshot_failure();
        }
        progress.become_probe();
        // Sent: wait for the member's answer. Failed: wait a heartbeat.
        progress.paused = true;
        progress.pending_request_snapshot = 0;
    }
    fn handle_transfer_leader(&mut self, message: &Message) -> Result<()> {
        let target = message.from;
        if self.tracker.get(target).is_none() || !self.tracker.configuration().votes(target) {
            return Ok(());
        }
        if self.lead_transferee == Some(target) {
            return Ok(());
        }
        self.lead_transferee = None;
        if target == self.id {
            return Ok(());
        }
        // A transfer finishes within an election timeout or is given up.
        self.election_elapsed = 0;
        if let Some(watch) = self.watch.as_mut() {
            watch.transfer = Arm::Unset { round: false };
        }
        self.lead_transferee = Some(target);
        let last = self.log.last_index()?;
        if self
            .tracker
            .get(target)
            .is_some_and(|progress| progress.matched == last)
        {
            self.send(proto::message(target, MessageType::MsgTimeoutNow))
        } else {
            self.send_append(target)
        }
    }

    fn step_candidate(&mut self, kind: MessageType, message: Message) -> Result<()> {
        match kind {
            MessageType::MsgPropose => Err(Error::ProposalDropped),
            MessageType::MsgAppend => {
                self.become_follower(message.term, message.from)?;
                self.handle_append_entries(message)
            }
            MessageType::MsgHeartbeat => {
                self.become_follower(message.term, message.from)?;
                self.handle_heartbeat(message)
            }
            MessageType::MsgSnapshot => {
                self.become_follower(message.term, message.from)?;
                self.handle_snapshot(message)
            }
            MessageType::MsgRequestPreVoteResponse | MessageType::MsgRequestVoteResponse => {
                // An answer to what this member asks now: one that asks
                // for votes may still hear about its asking before.
                let asked = if self.state == StateRole::PreCandidate {
                    MessageType::MsgRequestPreVoteResponse
                } else {
                    MessageType::MsgRequestVoteResponse
                };
                if kind != asked {
                    return Ok(());
                }
                if !message.reject && kind == MessageType::MsgRequestVoteResponse {
                    self.hear_report(&message)?;
                }
                self.poll(message.from, !message.reject)?;
                self.maybe_commit_by_vote(&message)
            }
            MessageType::MsgTimeoutNow => {
                // The leader of this term hands over to one that was
                // asking whether it could be elected: it need not ask, and
                // the lease that refuses what it asked refuses no
                // hand-over. Ignored, the leader would wait an election
                // timeout for it and take no proposal meanwhile. One that
                // asks for votes has a later term than whoever told it.
                if self.state == StateRole::PreCandidate && self.promotable {
                    self.hup(true)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    fn forward(&mut self, mut message: Message) -> Result<()> {
        message.to = self.leader_id;
        self.send(message)
    }
    fn step_follower(&mut self, kind: MessageType, mut message: Message) -> Result<()> {
        match kind {
            MessageType::MsgPropose => {
                if self.leader_id == 0 {
                    return Err(Error::ProposalDropped);
                }
                self.forward(message)
            }
            MessageType::MsgAppend => {
                self.election_elapsed = 0;
                self.leader_id = message.from;
                self.told_to_campaign = false;
                self.followed(message.from);
                self.handle_append_entries(message)?;
                self.heard_leader()
            }
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.leader_id = message.from;
                self.told_to_campaign = false;
                self.followed(message.from);
                self.handle_heartbeat(message)?;
                self.heard_leader()
            }
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.leader_id = message.from;
                self.told_to_campaign = false;
                self.followed(message.from);
                self.handle_snapshot(message)?;
                self.heard_leader()
            }
            MessageType::MsgTransferLeader | MessageType::MsgReadIndex => {
                if self.leader_id == 0 {
                    return Ok(());
                }
                self.forward(message)
            }
            MessageType::MsgTimeoutNow => {
                if self.promotable {
                    // The leader asks: the group is not cut off, and there
                    // is nothing to ask about first.
                    self.hup(true)?;
                }
                Ok(())
            }
            MessageType::MsgReadIndexResp => {
                if message.entries.len() != 1 {
                    return Ok(());
                }
                if self.read_states.len() >= self.config.limits.pending_reads {
                    return Err(Error::Capacity("reads that wait to be taken"));
                }
                let Some(entry) = message.entries.first_mut() else {
                    return Ok(());
                };
                let context = std::mem::take(&mut entry.data);
                self.read_states.try_reserve(1).map_err(|_| Error::Memory)?;
                self.read_states.push(ReadState {
                    index: message.index,
                    request_ctx: context,
                });
                // A leader answers reads once it committed in its term, so
                // its commit is of its term.
                self.log.maybe_commit(message.index, message.term)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Asks the leader for a snapshot that reaches this member's log.
    pub fn request_snapshot(&mut self) -> Result<()> {
        if self.state == StateRole::Leader
            || self.leader_id == 0
            || self.snapshot().is_some()
            || self.pending_request_snapshot != 0
        {
            return Err(Error::RequestSnapshotDropped);
        }
        let index = self.log.last_index()?;
        if self.log.term(index)? != self.term {
            return Err(Error::RequestSnapshotDropped);
        }
        self.pending_request_snapshot = index;
        self.send_request_snapshot()
    }
    fn send_request_snapshot(&mut self) -> Result<()> {
        let mut message = proto::message(self.leader_id, MessageType::MsgAppendResponse);
        message.index = self.log.committed();
        message.reject = true;
        message.reject_hint = self.log.last_index()?;
        message.request_snapshot = self.pending_request_snapshot;
        message.log_term = self.log.term(message.reject_hint)?;
        self.send(message)
    }
    fn handle_append_entries(&mut self, mut message: Message) -> Result<()> {
        if self.pending_request_snapshot != 0 {
            return self.send_request_snapshot();
        }
        let mut answer = proto::message(message.from, MessageType::MsgAppendResponse);
        if message.index < self.log.committed() {
            answer.index = self.log.committed();
            answer.commit = self.log.committed();
            return self.send(answer);
        }
        // A leader on another core bounds its messages by their bytes
        // alone. What is more than this member may hold is left for the
        // leader to send again: the answer names the last entry taken.
        let mut taken = std::mem::take(&mut message.entries);
        taken.truncate(self.config.limits.unstable_entries);
        // The leader's entries move into the log uncopied.
        match self.log.append_after_owned(
            message.index,
            message.log_term,
            message.commit,
            taken,
            self.config.fast,
        )? {
            Some((_, last)) => {
                answer.index = last;
                let held = self.log.last_index()?;
                self.release_proposals(held)?;
            }
            None if self.lost.is_some() && message.index > self.log.last_index()? => {
                // What it refuses follows entries it may have acknowledged
                // and lost: it says so.
                return self.send_lost(message.from, message.index);
            }
            None => {
                // The leader's log before its index has terms at most the
                // one it names: the last index here of such a term is
                // where the two may still agree.
                let hint = message.index.min(self.log.last_index()?);
                let (hint, term) = self.log.find_conflict_by_term(hint, message.log_term)?;
                answer.index = message.index;
                answer.reject = true;
                answer.reject_hint = hint;
                answer.log_term =
                    term.ok_or(Error::Invariant("the hinted entry's term is not held"))?;
            }
        }
        answer.commit = self.log.committed();
        self.send(answer)
    }
    /// A member whose log lost entries it acknowledged refuses `index`
    /// and says it lost them, naming the last entry it holds: a leader
    /// that counts them takes its progress back there and resends them
    /// (core step R-5, CTRL's follower repair, Alagappan et al. §3.4). It
    /// is an ordinary refusal besides, for a leader that counts none of
    /// them.
    fn send_lost(&mut self, to: NodeId, index: u64) -> Result<()> {
        let mut answer = proto::message(to, MessageType::MsgAppendResponse);
        answer.index = index;
        answer.reject = true;
        answer.lost = true;
        answer.reject_hint = self.log.last_index()?;
        answer.log_term = self.log.last_term()?;
        answer.commit = self.log.committed();
        self.send(answer)
    }
    fn handle_heartbeat(&mut self, mut message: Message) -> Result<()> {
        if let Some(lost) = self.lost
            && message.commit > self.log.last_index()?
        {
            // The leader counts entries this member lost: it says so.
            return self.send_lost(message.from, lost.index);
        }
        self.log.commit_to(message.commit)?;
        if self.pending_request_snapshot != 0 {
            return self.send_request_snapshot();
        }
        let mut answer = proto::message(message.from, MessageType::MsgHeartbeatResponse);
        answer.context = std::mem::take(&mut message.context);
        answer.commit = self.log.committed();
        if self.config.heartbeat_answers == HeartbeatAnswers::Position {
            // How far this log goes. It is sent once what the log holds is
            // durable, as every answer of a member that does not lead is.
            answer.index = self.log.last_index()?;
            answer.log_term = self.log.last_term()?;
        }
        self.send(answer)
    }
    fn handle_snapshot(&mut self, mut message: Message) -> Result<()> {
        let snapshot = message
            .snapshot
            .take()
            .map(|snapshot| *snapshot)
            .unwrap_or_default();
        let mut answer = proto::message(message.from, MessageType::MsgAppendResponse);
        answer.index = if self.restore(snapshot)? {
            self.log.last_index()?
        } else {
            self.log.committed()
        };
        self.send(answer)
    }
    /// Begins again from `snapshot`. False when the member keeps its log.
    pub fn restore(&mut self, snapshot: Snapshot) -> Result<bool> {
        let (index, term) = (
            proto::snapshot_index(&snapshot),
            proto::snapshot_term(&snapshot),
        );
        if index < self.log.committed() {
            return Ok(false);
        }
        if self.state != StateRole::Follower {
            return Err(Error::Invariant(
                "one that does not follow was sent a snapshot",
            ));
        }
        let stated = snapshot
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.conf_state.as_ref())
            .ok_or(Error::Violation("a snapshot without its configuration"))?;
        let configuration = Configuration::from_conf_state(stated)
            .map_err(|_| Error::Violation("a snapshot whose configuration is none"))?;
        // A snapshot that does not name this member is not for it.
        if !configuration.contains(self.id) {
            return Ok(false);
        }
        if self.pending_request_snapshot == 0 && self.log.match_term(index, term) {
            // The log holds what the snapshot holds.
            self.log.commit_to(index)?;
            return Ok(false);
        }
        self.log.restore(snapshot)?;
        let last = self.log.last_index()?;
        self.release_proposals(last)?;
        self.tracker = Tracker::new(
            configuration,
            last,
            self.config.max_inflight_msgs,
            self.config.max_inflight_bytes,
        )?;
        self.post_conf_change()?;
        if let Some(progress) = self.tracker.get_mut(self.id) {
            let held = progress.next_index.saturating_sub(1);
            progress.maybe_update(held);
        }
        self.pending_request_snapshot = 0;
        Ok(true)
    }

    /// What the detectors said of a member goes when the configuration stops naming it. Kept, a
    /// suspicion outlived the member's removal, and once it was added again nothing told this
    /// member otherwise: the owner tells a replica only of the peers it shares a group with, and
    /// the stream's pair had nothing new to say. A leader then counted a live member out of its
    /// quorum (`a_member_removed_while_suspected_is_believed_anew_when_added_again`). The owner
    /// tells it what its detectors believe of a member it is given again (hyper-durable's
    /// `Owner::pairs`).
    fn forget_unnamed(&mut self) {
        if let Some(watch) = self.watch.as_deref_mut() {
            let configuration = self.tracker.configuration();
            watch.forget_unnamed(|member| configuration.contains(member));
        }
    }
    /// The configuration changed: what follows from it.
    fn post_conf_change(&mut self) -> Result<ConfState> {
        let stated = self.tracker.configuration().to_conf_state()?;
        self.forget_unnamed();
        let votes = self.tracker.configuration().votes(self.id);
        self.promotable = votes;
        if self.state != StateRole::Leader {
            return Ok(stated);
        }
        if !votes {
            // Removed, or a learner now. The group is another's to lead:
            // the voter that holds the whole log is asked to campaign at
            // once, so the group does not wait out an election timeout,
            // and this member follows.
            let last = self.log.last_index()?;
            let configuration = self.tracker.configuration();
            let heir = self
                .tracker
                .iter()
                .filter(|(member, progress)| {
                    configuration.votes(*member) && progress.matched == last
                })
                .map(|(member, _)| member)
                .next();
            match heir {
                Some(heir) => self.send(proto::message(heir, MessageType::MsgTimeoutNow))?,
                // By suspicion its followers trust its node, which lives: none
                // would campaign. The voter that holds the most is told to.
                None if self.watch.is_some() => self.hand_over()?,
                None => {}
            }
            let term = self.term;
            self.become_follower(term, 0)?;
            return Ok(stated);
        }
        if self.maybe_commit()? {
            // A smaller quorum may hold more.
            self.bcast_append()?;
        } else {
            // A new member does not wait for a heartbeat to be probed.
            let (mut outbox, tracker) = self.outbox();
            for position in 0..tracker.len() {
                if let Some((member, progress)) = tracker.at(position)
                    && member != outbox.id
                {
                    outbox.append(member, progress, false)?;
                }
            }
        }
        // A smaller quorum may have confirmed reads.
        let mut context = Vec::new();
        if let Some(last) = self.read_only.last_context() {
            context
                .try_reserve_exact(last.len())
                .map_err(|_| Error::Memory)?;
            context.extend_from_slice(last);
            let confirmed = match self.read_only.ack(self.id, &context)? {
                Some(acks) => self.tracker.has_quorum(acks),
                None => false,
            };
            if confirmed {
                self.confirm_reads(&context)?;
            }
        }
        if self
            .lead_transferee
            .is_some_and(|target| !self.tracker.configuration().votes(target))
        {
            self.lead_transferee = None;
        }
        Ok(stated)
    }
    /// A committed change is applied: the configuration it makes.
    pub fn apply_conf_change(&mut self, plan: &Plan) -> Result<ConfState> {
        let changed = plan.apply(self.tracker.configuration())?;
        let from = self.log.last_index()?;
        self.tracker
            .apply(changed.configuration, &changed.renewed, from)?;
        if self.state == StateRole::Leader {
            self.note_term_change()?;
        }
        self.post_conf_change()
    }
    fn load_state(&mut self, state: &HardState) -> Result<()> {
        if state.commit < self.log.committed() || state.commit > self.log.last_index()? {
            return Err(Error::Invariant("a durable commit outside the log"));
        }
        self.log.committed = state.commit;
        self.term = state.term;
        self.vote = state.vote;
        Ok(())
    }
    /// Between one and two election timeouts, drawn from the seed.
    fn reset_randomized_election_timeout(&mut self) {
        // SplitMix64 (Steele, Lea and Flood, OOPSLA 2014).
        self.random = self.random.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut drawn = self.random;
        drawn = (drawn ^ (drawn >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        drawn = (drawn ^ (drawn >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        drawn ^= drawn >> 31;
        let span = u64::try_from(self.config.election_tick).unwrap_or(u64::MAX);
        // The high half of the product is below `span`.
        let within = (u128::from(drawn).wrapping_mul(u128::from(span)) >> 64) as u64;
        self.randomized_election_timeout = self
            .config
            .election_tick
            .saturating_add(usize::try_from(within).unwrap_or(0));
    }
}

/// What the log of one member holds, for tests of this crate.
#[cfg(test)]
pub(crate) fn held<S: Storage>(raft: &Raft<S>) -> Vec<(u64, u64)> {
    let first = raft.log.first_index().unwrap();
    raft.log
        .entries(first, u64::MAX, usize::MAX)
        .unwrap()
        .iter()
        .map(|entry| (entry.index, entry.term))
        .collect()
}
