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
    pub proposal_bytes: usize,
    /// How far above what is committed an entry may be proposed.
    pub fast_window: u64,
    /// The bytes of what a leader was told the voters hold.
    pub vote_bytes: usize,
}
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
    /// Kept to compare the two cores under one rule.
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
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
    pub priority: i64,
    pub precedence: Precedence,
    pub read_rounds: ReadRounds,
    pub heartbeat_answers: HeartbeatAnswers,
    /// Whether the group has the fast track ([`crate::fast`]). It is part
    /// of what the group is: every member is opened with the same.
    pub fast: bool,
    /// A commit alone is not sent; it goes with the next message.
    pub skip_bcast_commit: bool,
    /// What the election timeouts are drawn from.
    pub seed: u64,
    pub limits: Limits,
}
impl Config {
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
            seed: id,
            limits: Limits::default(),
        }
    }
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
        {
            return Err(Error::Settings("a bound that admits nothing"));
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StateRole {
    #[default]
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

/// What a member says of itself that is not durable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SoftState {
    pub leader_id: NodeId,
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
    /// The priority the owner gave.
    priority: i64,
    /// The priority votes are judged by: the one given, once the member
    /// has a term.
    priority_in_force: i64,
    pub(crate) promotable: bool,
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
}
impl Outgoing {
    /// The fewest slots a queue is given: what a tick of a three-member
    /// group queues at most, a heartbeat a peer or a vote request a peer,
    /// and one more.
    pub const SMALLEST: usize = 4;
    pub fn len(&self) -> usize {
        self.msgs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.msgs.is_empty()
    }
    pub fn as_slice(&self) -> &[Message] {
        &self.msgs
    }
    pub fn capacity(&self) -> usize {
        self.msgs.capacity()
    }
    /// The bytes the messages hold, by capacity ([`proto::message_bytes`]).
    pub fn payload(&self) -> usize {
        self.payload
    }
    /// The bytes held, by capacity: the slots and what the messages hold.
    pub fn resident_bytes(&self) -> usize {
        self.msgs
            .capacity()
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
    fn push(&mut self, message: Message) -> Result<()> {
        if self.msgs.len() == self.msgs.capacity() {
            let more = self.msgs.capacity().max(Self::SMALLEST);
            self.msgs
                .try_reserve_exact(more)
                .map_err(|_| Error::Memory)?;
        }
        self.payload = self.payload.saturating_add(proto::message_bytes(&message));
        self.msgs.push(message);
        Ok(())
    }
    /// Everything queued, given up: the queue that remains holds nothing
    /// and keeps no room.
    pub fn take(&mut self) -> Vec<Message> {
        self.payload = 0;
        std::mem::take(&mut self.msgs)
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

fn push(
    msgs: &mut Outgoing,
    id: NodeId,
    term: u64,
    priority: i64,
    mut message: Message,
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
        return msgs.push(message);
    }
    let kind = proto::message_type(&message).ok_or(Error::Invariant("a message of no kind"))?;
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
        if let Ok(priority) = u64::try_from(priority)
            && priority > 0
        {
            message.deprecated_priority = priority;
        }
        message.priority = priority;
    }
    msgs.push(message)
}

fn priority_of(message: &Message) -> i64 {
    if message.priority != 0 {
        message.priority
    } else {
        i64::try_from(message.deprecated_priority).unwrap_or(i64::MAX)
    }
}

impl<S: Storage> Outbox<'_, S> {
    fn send(&mut self, message: Message) -> Result<()> {
        push(self.msgs, self.id, self.term, self.priority, message)
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
        message.msg_type = MessageType::MsgSnapshot as i32;
        message.snapshot = Some(snapshot);
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
        if progress.pending_request_snapshot != 0 {
            if !self.snapshot(&mut message, progress, to)? {
                return Ok(false);
            }
        } else {
            // The page is bounded by its bytes, by what the member's
            // window has room for and by its entries, before any of it is
            // copied.
            let entries = self.log.entries(
                progress.next_index,
                progress.page_bytes(self.max_bytes),
                self.max_entries,
            );
            if !allow_empty && entries.as_ref().map_or(true, Vec::is_empty) {
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
                (Ok(term), Ok(entries)) => {
                    message.msg_type = MessageType::MsgAppend as i32;
                    message.index = progress.next_index.saturating_sub(1);
                    message.log_term = term;
                    message.commit = self.log.committed();
                    if let Some(last) = entries.last() {
                        // By the rule the page was cut by.
                        let bytes = entries.iter().fold(0u64, |bytes, entry| {
                            bytes.saturating_add(proto::encoded_bytes(entry))
                        });
                        progress.sent(last.index, bytes)?;
                    }
                    message.entries = entries;
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
        self.send(message)?;
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

impl<S: Storage> Raft<S> {
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
            priority: config.priority,
            priority_in_force: 0,
            promotable: false,
            told_to_campaign: false,
            log,
            tracker,
            read_only: ReadOnly::new(config.limits.pending_reads),
            read_states: Vec::new(),
            msgs: Outgoing::default(),
            uncommitted_bytes: 0,
            leader_tail: 0,
            random: config.seed,
            config: config.clone(),
        };
        raft.promotable = raft.tracker.configuration().votes(raft.id);
        if initial.hard_state != HardState::default() {
            raft.load_state(&initial.hard_state)?;
        }
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
        let term = raft.term;
        raft.become_follower(term, 0)?;
        raft.settle_priority();
        Ok(raft)
    }

    /// The configuration the core runs under.
    pub fn config(&self) -> &Config {
        &self.config
    }
    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn vote(&self) -> NodeId {
        self.vote
    }
    pub fn state(&self) -> StateRole {
        self.state
    }
    pub fn leader_id(&self) -> NodeId {
        self.leader_id
    }
    /// The index what is applied must pass before a change is proposed.
    pub fn pending_conf_index(&self) -> u64 {
        self.pending_conf_index
    }
    pub fn lead_transferee(&self) -> Option<NodeId> {
        self.lead_transferee
    }
    pub fn log(&self) -> &Log<S> {
        &self.log
    }
    pub fn store(&self) -> &S {
        self.log.store()
    }
    pub fn store_mut(&mut self) -> &mut S {
        self.log.store_mut()
    }
    pub fn tracker(&self) -> &Tracker {
        &self.tracker
    }
    pub fn configuration(&self) -> &Configuration {
        self.tracker.configuration()
    }
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
    pub fn read_states(&self) -> &[ReadState] {
        &self.read_states
    }
    pub fn pending_read_count(&self) -> usize {
        self.read_only.len()
    }
    pub fn ready_read_count(&self) -> usize {
        self.read_states.len()
    }
    pub fn election_elapsed(&self) -> usize {
        self.election_elapsed
    }
    pub fn randomized_election_timeout(&self) -> usize {
        self.randomized_election_timeout
    }
    pub(crate) fn committed_bytes_per_ready(&self) -> u64 {
        self.config.max_committed_size_per_ready
    }
    pub fn uncommitted_bytes(&self) -> usize {
        self.uncommitted_bytes
    }
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.log.unstable().snapshot()
    }
    pub fn soft_state(&self) -> SoftState {
        SoftState {
            leader_id: self.leader_id,
            raft_state: self.state,
        }
    }
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
        self.state != StateRole::Leader
            && self.promotable
            && self.election_elapsed.saturating_add(1)
                >= self
                    .randomized_election_timeout
                    .saturating_add(self.patience)
    }
    /// Whether the next tick makes this leader send its heartbeats, or ask
    /// itself whether it still has a quorum.
    pub fn beats_on_next_tick(&self) -> bool {
        self.state == StateRole::Leader
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
            if self.state == StateRole::Follower && self.promotable {
                self.hup(true)?;
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
                entry_type: EntryType::EntryConfChangeV2 as i32,
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
        self.lead_transferee = None;
        self.told_to_campaign = false;
        self.votes.clear();
        self.decided.clear();
        self.tracker.reset_votes();
        self.pending_conf_index = 0;
        self.read_only.clear();
        self.pending_request_snapshot = 0;
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
    fn admit_uncommitted(&mut self, entries: &[Entry]) -> bool {
        if self.config.max_uncommitted_size == u64::MAX {
            return true;
        }
        let bytes = entries.iter().fold(0usize, |bytes, entry| {
            bytes.saturating_add(entry.data.len())
        });
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
        if recovered {
            let bytes = entries.iter().fold(0usize, |bytes, entry| {
                bytes.saturating_add(entry.data.len())
            });
            if self.config.max_uncommitted_size != u64::MAX {
                self.uncommitted_bytes = self.uncommitted_bytes.saturating_add(bytes);
            }
        } else if !self.admit_uncommitted(&entries) {
            return Ok(false);
        }
        let mut index = last;
        for entry in &mut entries {
            index = index.saturating_add(1);
            entry.term = self.term;
            entry.index = index;
        }
        if let Err(error) = self.log.append(&entries) {
            let bytes = entries.iter().fold(0usize, |bytes, entry| {
                bytes.saturating_add(entry.data.len())
            });
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
        let last = self.log.last_index()?;
        // A candidate's log is durable before it asks for votes and does
        // not change while it asks.
        if last != self.log.persisted() {
            return Err(Error::Invariant("a leader whose log is not durable"));
        }
        self.uncommitted_bytes = 0;
        self.leader_tail = last;
        if let Some(progress) = self.tracker.get_mut(self.id) {
            progress.become_replicate();
        }
        // There may be a change in the log that is not applied: none is
        // proposed until all of the log is.
        self.pending_conf_index = last;
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
        if self.poll(self.id, true)? == Tally::Won {
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
        // A member does not campaign on a configuration it has not
        // applied. A snapshot not yet durable states its own.
        let low = match self.log.unstable().snapshot() {
            Some(snapshot) => proto::snapshot_index(snapshot).saturating_add(1),
            None => self.log.applied().saturating_add(1),
        };
        let high = self.log.committed().saturating_add(1);
        if self.has_unapplied_conf_changes(low, high)? {
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
        let kind = proto::message_type(&message).ok_or(Error::Violation("a message of no kind"))?;
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
            || message
                .entries
                .iter()
                .any(|entry| entry.index == u64::MAX || entry.term == u64::MAX)
        {
            return Err(Error::Violation(
                "a term or an index beyond what is counted",
            ));
        }
        if message.term == 0 {
            // The member's own, or forwarded to it.
        } else if message.term > self.term {
            if matches!(
                kind,
                MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
            ) {
                let force = message.context.as_slice() == CAMPAIGN_TRANSFER;
                let in_lease = self.config.check_quorum
                    && self.leader_id != 0
                    && self.election_elapsed < self.config.election_tick;
                if !force && in_lease {
                    // One that heard its leader within an election timeout
                    // neither moves its term nor votes: a member removed
                    // from the group cannot disturb it.
                    return Ok(());
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
        } else if message.term < self.term {
            if (self.config.check_quorum || self.config.pre_vote)
                && matches!(kind, MessageType::MsgHeartbeat | MessageType::MsgAppend)
            {
                // A leader of an older term: this member moved its term
                // while it was cut off. Its answer tells that leader, which
                // no vote request of this member would, refused as they
                // are while the leader is heard.
                self.send(proto::message(message.from, MessageType::MsgAppendResponse))?;
            } else if kind == MessageType::MsgRequestPreVote {
                // Answered and not dropped: a candidate of an older term
                // that hears nothing would ask for ever.
                let mut answer =
                    proto::message(message.from, MessageType::MsgRequestPreVoteResponse);
                answer.term = self.term;
                answer.reject = true;
                self.send(answer)?;
            }
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
        let (last_index, last_term) = (self.log.last_index()?, self.log.last_term()?);
        let ahead = match self.config.precedence {
            Precedence::Log => {
                message.log_term > last_term
                    || (message.log_term == last_term && message.index > last_index)
            }
            Precedence::Length => message.index > last_index,
        };
        let ranked = transfer || ahead || self.priority_in_force <= priority_of(message);
        if can_vote && ranked && self.log.is_up_to_date(message.index, message.log_term)? {
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
    fn handle_append_response(&mut self, message: &Message) -> Result<()> {
        let mut next_probe = message.reject_hint;
        if message.reject && message.log_term > 0 {
            // The member holds `log_term` at its hint. No index of this
            // log at or below the hint with a higher term can match it,
            // for terms only rise along a log: probe at the last one that
            // may.
            next_probe = self
                .log
                .find_conflict_by_term(message.reject_hint, message.log_term)?
                .0;
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
        let beat = self.config.heartbeat_tick.max(1);
        let (mut outbox, tracker) = self.outbox();
        let Some(progress) = tracker.get_mut(message.from) else {
            return Ok(());
        };
        progress.update_committed(message.commit);
        progress.recent_active = true;
        match answers {
            HeartbeatAnswers::Bare => {
                progress.paused = false;
                // A full window would wait on answers that may be lost.
                if progress.state == ProgressState::Replicate && progress.inflights.full() {
                    progress.inflights.free_first_one();
                }
            }
            HeartbeatAnswers::Position => match progress.state {
                ProgressState::Replicate => {
                    if held {
                        progress.inflights.free_to(message.index);
                    }
                    // The window is full, and the member answers
                    // heartbeats and nothing of what is out. Nothing more
                    // is sent for that: what is out stays within its
                    // bound. Once a beat has passed with no answer for any
                    // of it, the member is not being reached by it — what
                    // its owner was not told was lost — and it is asked
                    // where it is.
                    if !news && progress.inflights.full() && progress.stalled >= beat {
                        progress.become_probe();
                    }
                }
                // The one message a probe is, is sent again when it was
                // told lost (`MsgUnreachable`), and when a beat has passed
                // since it was sent and the member answers heartbeats and
                // not it — not at every heartbeat's answer, which on a path
                // slower than the heartbeats sent the page again and again
                // behind itself.
                ProgressState::Probe if progress.paused => {
                    if progress.stalled >= beat {
                        progress.paused = false;
                    }
                }
                ProgressState::Probe | ProgressState::Snapshot => {}
            },
        }
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
                self.handle_append_entries(&message)
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
                self.handle_append_entries(&message)?;
                self.heard_leader()
            }
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.leader_id = message.from;
                self.told_to_campaign = false;
                self.handle_heartbeat(message)?;
                self.heard_leader()
            }
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.leader_id = message.from;
                self.told_to_campaign = false;
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
    fn handle_append_entries(&mut self, message: &Message) -> Result<()> {
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
        let taken = message
            .entries
            .get(..self.config.limits.unstable_entries)
            .unwrap_or(&message.entries);
        match self.log.append_after(
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
    fn handle_heartbeat(&mut self, mut message: Message) -> Result<()> {
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
        let snapshot = message.snapshot.take().unwrap_or_default();
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

    /// The configuration changed: what follows from it.
    fn post_conf_change(&mut self) -> Result<ConfState> {
        let stated = self.tracker.configuration().to_conf_state()?;
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
            if let Some(heir) = heir {
                self.send(proto::message(heir, MessageType::MsgTimeoutNow))?;
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
