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
    ahead::Early,
    catchup::{CatchUp, Staging, Stagings},
    error::{Dropped, Error, Result, StorageError},
    fast::{self, Decided, Proposals, Votes},
    log::Log,
    progress::{Progress, ProgressState, Tracker},
    proto::{
        self, CAMPAIGN_TRANSFER, ConfState, Entry, EntryType, HardState, Message, MessageType,
        Plan, Snapshot,
    },
    read::{PendingRead, ReadOnly, ReadState},
    storage::Storage,
    watch::{Arm, TRANSFER_ROUNDS, Timing, Watch, nanos},
    wire::{ENTRY_FIXED_BYTES, MESSAGE_RECORD_FIXED_BYTES},
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
    /// The most members a configuration names, voters and learners
    /// together: what the member counts for each member (its progress, the
    /// confirmations of a read, the holders of a fast entry, the members its
    /// detectors suspect). A leader proposes no change past it, and a member
    /// given a configuration past it stops.
    pub members: usize,
}

/// What an owner states of the member it opens, from which every bound of
/// [`Limits`] is derived ([`Limits::derive`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stated {
    /// The bytes of the largest message the member's transport carries, its
    /// record as the wire writes it ([`MESSAGE_RECORD_FIXED_BYTES`] and its
    /// entries): a leader's append is no larger, nor a vote that carries a
    /// member's proposals ([`crate::fast`]).
    pub message: usize,
    /// The most members a configuration of the group names, voters and
    /// learners together. Every member of a group states the same, as it
    /// opens with the same fast track: a change one refuses and another
    /// applies would leave them in different configurations.
    pub members: usize,
    /// The bytes one of the member's queues may hold, each element counted
    /// at the least it holds: the entries not yet durable, the messages not
    /// yet taken, the reads that wait. What their buffers hold is bounded
    /// besides: the window to each member, the bytes a leader holds
    /// uncommitted, the owner's budget.
    pub memory: usize,
    /// The `Ready`s whose writes may be out at once: the store's depth.
    pub depth: usize,
}

impl Limits {
    /// The bounds `stated` gives, each derived from it:
    /// - a message carries no more entries than its bytes past its fixed
    ///   record hold at an entry's fixed bytes each ([`ENTRY_FIXED_BYTES`]);
    /// - a member holds proposals of no more bytes than a message carries
    ///   past its fixed record, so that a vote carries every one; they are
    ///   counted resident ([`crate::fast`]), each at least an entry, so that
    ///   no more of them are held than those bytes hold entries, and none is
    ///   proposed further above the commit, where no vote could carry it;
    /// - a leader is told what each member holds, a message's bytes from
    ///   each at the most;
    /// - each queue holds what `memory` holds of its least element: an
    ///   entry, a message's allowance ([`proto::MESSAGE_ALLOWANCE`]), a read;
    /// - the members and the readies in flight are as stated.
    ///
    /// Refused where a message carries no entry; [`Config::validate`] refuses
    /// bounds that admit nothing or disagree.
    pub fn derive(stated: Stated) -> Result<Self> {
        let payload = stated
            .message
            .checked_sub(MESSAGE_RECORD_FIXED_BYTES)
            .filter(|payload| *payload >= ENTRY_FIXED_BYTES)
            .ok_or(Error::Settings("a message that carries no entry"))?;
        let entries_per_message = payload.checked_div(ENTRY_FIXED_BYTES).unwrap_or(0);
        let proposals = payload
            .checked_div(std::mem::size_of::<Entry>())
            .unwrap_or(0);
        let vote_bytes = payload.checked_mul(stated.members).ok_or(Error::Settings(
            "the bytes of a quorum's votes beyond what is counted",
        ))?;
        let within = |least: usize| stated.memory.checked_div(least).unwrap_or(0);
        Ok(Self {
            pending_messages: within(proto::MESSAGE_ALLOWANCE),
            pending_reads: within(std::mem::size_of::<PendingRead>()),
            unstable_entries: within(std::mem::size_of::<Entry>()),
            entries_per_message,
            proposals,
            proposal_bytes: payload,
            fast_window: u64::try_from(proposals).unwrap_or(u64::MAX),
            vote_bytes,
            readies_in_flight: stated.depth,
            members: stated.members,
        })
    }
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

/// What a member does with a leader's append that begins past the end of
/// its log: entries that arrived ahead of a hole, one of the leader's
/// appends before them lost or late (mantle note 32 R17, `crate::ahead`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Ahead {
    /// It refuses the append as Raft does, and keeps its entries beside its
    /// log, as many as its log may hold not yet durable
    /// ([`Limits::unstable_entries`]); once an append of the same term fills
    /// the hole it takes them into its log with it, and acknowledges them
    /// with it. One lost append costs its own resend, not the window's (slates
    /// `docs/wip/research/consensus-enhancements.md` §3.5).
    #[default]
    Kept,
    /// It refuses the append and keeps nothing: the rule of `raft-rs`, whose
    /// leader sends every entry after the hole again. Kept to compare the two
    /// cores under one rule.
    Refused,
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
    /// Messages sent to a member ahead of its answers; `usize::MAX` for no
    /// count of their own, the window's bytes bounding it (mantle note 32
    /// R16: the window is what the path carries, and a fixed count, focal's
    /// 128 or raft-rs's 256, is no measure of a path).
    pub max_inflight_msgs: usize,
    /// The bytes sent to a member ahead of its answers, each append's
    /// record ([`crate::wire::MESSAGE_RECORD_FIXED_BYTES`] and its entries),
    /// until its owner says what the path to it carries
    /// ([`crate::RawNode::set_inflight_bytes`]); `u64::MAX` for no bound of
    /// its own, where the messages are counted. One entry larger than the
    /// bound is still sent, alone.
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
    /// for a candidate of lower only when the candidate's log is more
    /// current than its own, a later last term or the same and more
    /// entries. A voter that refuses for priority could then have been
    /// elected itself, so the refusal never leaves a group that can elect
    /// without a leader. It never judges a transfer.
    pub priority: i64,
    /// raft-rs's rule in place of the above, for the differential tests
    /// alone (`docs/raft.md` §3.3): a candidate of lower priority must hold
    /// more entries than the voter, whatever their terms. It can leave a
    /// group that could elect without a leader, so no build but the tests'
    /// has it.
    #[cfg(feature = "raft-rs-precedence")]
    pub raft_rs_precedence: bool,
    /// When a leader sends the round of heartbeats that confirms the reads
    /// that wait.
    pub read_rounds: ReadRounds,
    /// What a member says when it answers a heartbeat, and what its leader
    /// makes of a full window.
    pub heartbeat_answers: HeartbeatAnswers,
    /// What a member does with a leader's entries that arrive ahead of a
    /// hole in its log.
    pub ahead: Ahead,
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
    /// 256 messages, no byte bounds) and the bounds its owner's statement
    /// gives ([`Limits::derive`]).
    pub fn new(id: NodeId, limits: Limits) -> Self {
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
            #[cfg(feature = "raft-rs-precedence")]
            raft_rs_precedence: false,
            read_rounds: ReadRounds::Shared,
            heartbeat_answers: HeartbeatAnswers::Position,
            ahead: Ahead::Kept,
            fast: false,
            skip_bcast_commit: false,
            apply_unpersisted: false,
            elections: Elections::Ticks,
            lost: None,
            seed: id,
            limits,
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
        if self.max_inflight_msgs == usize::MAX && self.max_inflight_bytes == u64::MAX {
            return Err(Error::Settings(
                "a window bounded by neither its messages nor its bytes",
            ));
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
            || self.limits.members == 0
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

/// What decides a member's next campaign when it elects by suspicion ([`Raft::campaign_state`]),
/// for its owner to report: a campaign that does not come is one of these.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CampaignState {
    /// When the campaign is due on the owner's clock, nanoseconds, once timed.
    pub due: Option<u64>,
    /// Whether a campaign is armed at all, timed or not.
    pub armed: bool,
    /// Whether it led its current term and leads no more: it campaigns or hands over.
    pub led: bool,
    /// Whether the owner holds its campaigns.
    pub held: bool,
    /// Whether it and the members its core trusts are a quorum of each half of its configuration.
    pub trusted_quorum: bool,
    /// Whether its log lets it campaign (`may_campaign`) and lead (`may_lead`).
    pub may_campaign: bool,
    /// Whether it could lead a term it campaigned for.
    pub may_lead: bool,
    /// Whether it may campaign by its configuration (`Raft::promotable`).
    pub promotable: bool,
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
#[derive(Clone)]
pub struct Raft<S> {
    /// The defect planted in this member, if any (`crate::mutant`).
    #[cfg(feature = "mutants")]
    pub(crate) mutant: Option<crate::Mutant>,
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
    /// Ticks since this member last heard its leader: what its lease counts (Ongaro's thesis
    /// §4.2.3, etcd's `inLease`: a vote is refused only within the minimum election timeout of
    /// hearing a current leader). Apart from [`Raft::election_elapsed`], its own election timer,
    /// which its own campaigns restart: one counter for both let a member whose campaign waited
    /// for a change to apply keep its lease, and refuse the voter that campaigned (mantle note 32
    /// R4; slates `docs/bugs/2026-09-29-a-yielding-voter-refused-the-voter-it-yielded-to.md`).
    silence: usize,
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
    /// Ticks this leader waits beyond its election timeout before it asks whether a quorum heard
    /// it (`MsgCheckQuorum`): its quorum patience, given by its owner for what it has measured of
    /// its voters' answers (`docs/raft.md` §3.6). A leader cannot tell followers whose owners
    /// stall from followers that are gone, and the check is a liveness device only (no read is
    /// answered by a lease): checking later deposes no sound leader whose followers answer late,
    /// and lets one that is cut off take requests a little longer.
    quorum_patience: usize,
    /// Ticks since this leader last asked whether a quorum heard it: the check's own counter,
    /// apart from the election timer a transfer's give-up counts (`tick_heartbeat`). Started again
    /// where that timer was: at a new role or term, and as a transfer starts.
    quorum_elapsed: usize,
    /// What this member keeps of its leader's appends that arrived ahead of
    /// a hole in its log ([`Ahead::Kept`]).
    early: Early,
    /// The learners this leader catches up, in rounds ([`CatchUp`]).
    stagings: Stagings,
    /// The ticks this member has led through: the clock its learners' rounds
    /// count on ticks.
    ticks: u64,
    /// Entries this member took into its log from what it kept ahead of a
    /// hole.
    taken_ahead: u64,
    /// What this member approved by itself, held until it knows the index committed by a
    /// classic quorum (`docs/raft.md` §3.5).
    pub(crate) held: Proposals,
    /// The index through which this member knows its log committed by a classic quorum: a
    /// leader by its own count (`Raft::maybe_commit`), another member from what a leader of its
    /// term says with what its log then matches (`Message::classic`). Every later leader's log
    /// holds the entries through it; a fast quorum's commit puts an entry in no majority's logs,
    /// so it never moves this. Not kept across a restart: a member that opens knows nothing
    /// committed so, and releases nothing until a leader says.
    pub(crate) classic: u64,
    /// What the voters hold above this member's log, as it was told.
    pub(crate) votes: Votes,
    /// Who holds what this leader took from the fast track.
    pub(crate) decided: Decided,
    /// What was proposed here and another entry took the index of.
    pub(crate) displaced: Vec<Entry>,
    /// The last index the owner lets this leader take from the fast track at, if it caps it
    /// ([`Raft::cap_takes`]): votes past it are kept, and taken once the cap rises.
    pub(crate) takes_through: Option<u64>,
    /// The leader, and its term, that was told what this member holds.
    pub(crate) voted_to: (u64, NodeId),
    /// What the fast track did here since the member opened.
    pub(crate) fast_stats: FastStats,
    pub(crate) holders: Vec<NodeId>,
    /// In a fast group that this member leads: the voters of the
    /// configuration it counted by when it was elected, by which a member
    /// that holds entries of this term may still count ([`crate::track`]).
    pub(crate) term_voters: Vec<NodeId>,
    /// The one other set of voters a configuration it wrote since named;
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
    /// The commit this member's storage states durably, as its owner said
    /// (`docs/durable.md` §4.1's `C_d`): what a member that restarts reopens
    /// with. Its answers state no more when they leave
    /// (`RawNode::durable_commit`).
    pub(crate) durable_commit: u64,
    /// What this member's durable log may lack of what it acknowledged ([`Lost`]).
    lost: Option<Lost>,
    /// The configuration the member counts by is the newest its log states, applied or not
    /// (`docs/raft.md` §3.4): the tracker's. `conf_newest_at` is the index of the entry that made
    /// it, and `conf_before` the configuration before that entry, made at `conf_before_at`. Of the
    /// changes a log holds only the newest may be uncommitted: a leader proposes a change only once
    /// it applied every one its log holds (`Raft::propose`), and one that is elected first applies
    /// its whole log. An append that replaces the newest takes the configuration back to the one
    /// before; one that would replace that is a log no protocol made.
    conf_before: Configuration,
    conf_before_at: u64,
    conf_newest_at: u64,
    /// The configuration the owner applied, entry by entry (`RawNode::apply_conf_change`): what it
    /// is told, which elections and commitment do not count by.
    applied_conf: Configuration,
    pub(crate) log: Log<S>,
    pub(crate) tracker: Tracker,
    pub(crate) read_only: ReadOnly,
    pub(crate) read_states: Vec<ReadState>,
    /// Reads asked of this leader before it committed an entry of its term, in the order asked:
    /// who asked, and the read's context. They wait for that commit, as the thesis's §6.4 step 1
    /// has a leader wait and etcd's `pendingReadIndexMessages` hold them; they count against the
    /// bound of reads with those that wait for their quorum or to be taken, and die with the term.
    deferred_reads: Vec<(NodeId, Vec<u8>)>,
    pub(crate) msgs: Outgoing,
    uncommitted_bytes: usize,
    /// The last index when this member last became leader: entries at or
    /// below are no proposals of its own.
    leader_tail: u64,
    /// The index of the blank entry this member appended on taking its term, after what it
    /// recovered of the fast track; 0 when it does not lead.
    term_start: u64,
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
#[derive(Clone, Debug, Default)]
pub struct Outgoing {
    msgs: Vec<Message>,
    payload: usize,
    /// Emptied queues owners gave back ([`Outgoing::recycle`]), the least
    /// room first: each [`Outgoing::take`] leaves the one with the most in
    /// place of the queue it gives. At most one for each `Ready` whose
    /// write may be out ([`Limits::readies_in_flight`]), since each such
    /// write holds a queue the member gave until the owner gives it back.
    spares: Vec<Vec<Message>>,
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
    /// The bytes held, by capacity: the slots, the spare queues', and what
    /// the messages hold.
    pub fn resident_bytes(&self) -> usize {
        let spare_slots = self.spares.iter().fold(0usize, |slots, spare| {
            slots.saturating_add(spare.capacity())
        });
        self.msgs
            .capacity()
            .saturating_add(spare_slots)
            .saturating_mul(std::mem::size_of::<Message>())
            .saturating_add(
                self.spares
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<Message>>()),
            )
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
    /// Drops the vote requests of `id`'s own that wait, which a campaign it
    /// begins supersedes: each asked every voter, and the campaign asks each
    /// again, so an answer to one it gave up can win it nothing. A member
    /// whose writes stay out through many election timeouts, its owner
    /// ticking it, would otherwise send one campaign's requests for every
    /// timeout once a `Ready` takes them again. Their bytes leave the
    /// counter; the queue keeps its room.
    fn supersede_requests(&mut self, id: NodeId) {
        let mut dropped = 0usize;
        self.msgs.retain(|message| {
            let superseded = message.from == id
                && matches!(
                    message.msg_type,
                    MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
                );
            if superseded {
                dropped = dropped.saturating_add(proto::message_bytes(message));
            }
            !superseded
        });
        self.payload = self.payload.saturating_sub(dropped);
    }
    /// Everything queued, given up: the queue that remains holds nothing,
    /// with the room of the largest spare an owner gave back, if any.
    pub fn take(&mut self) -> Vec<Message> {
        self.payload = 0;
        let spare = self.spares.pop().unwrap_or_default();
        std::mem::replace(&mut self.msgs, spare)
    }
    /// An owner gives back a queue it has emptied, so the room a burst grew
    /// stays with the member rather than being grown again from
    /// [`Outgoing::SMALLEST`]: it is the queue now if nothing waits and it
    /// has more room than the queue, else a spare for a later
    /// [`Outgoing::take`]. Spares are kept up to `keep`, one for each
    /// `Ready` whose write may be out ([`Limits::readies_in_flight`]): an
    /// owner that takes `Ready`s ahead of their writes gives back each
    /// write's queue only once it is durable, and with fewer spares than
    /// writes out a `take` would start a queue from nothing and grow it
    /// again. Past `keep`, a queue with more room replaces the spare with
    /// the least. A queue of more than `most` slots, the most that may
    /// wait ([`Limits::pending_messages`]), or of none, is dropped.
    pub(crate) fn recycle(&mut self, mut emptied: Vec<Message>, most: usize, keep: usize) {
        emptied.clear();
        if emptied.capacity() > most {
            return;
        }
        if self.msgs.is_empty() && emptied.capacity() > self.msgs.capacity() {
            emptied = std::mem::replace(&mut self.msgs, emptied);
        }
        let room = emptied.capacity();
        if room == 0 {
            return;
        }
        if self.spares.len() < keep {
            if self.spares.capacity() == 0 && self.spares.try_reserve_exact(keep).is_err() {
                return;
            }
            let at = self.spares.partition_point(|spare| spare.capacity() < room);
            self.spares.insert(at, emptied);
        } else if self
            .spares
            .first()
            .is_some_and(|least| least.capacity() < room)
        {
            let at = self.spares.partition_point(|spare| spare.capacity() < room);
            // The spare with the least room leaves; `emptied` takes its place in order.
            self.spares.remove(0);
            self.spares.insert(at.saturating_sub(1), emptied);
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
    /// What the leader knows committed by a classic quorum, which its appends say.
    classic: u64,
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
        message.classic = Some(self.classic);
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
                    message.classic = Some(self.classic);
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
    /// What `message`, a refusal of an append that began past the end of the
    /// refuser's log, asks of this leader ([`Ahead::Kept`]): the append
    /// arrived and is held there, so it leaves the window, and each message
    /// sent before it and neither answered nor kept is a hole, lost or late,
    /// and goes again, once (RFC 6675's `IsLost`: data sent after it
    /// arrived). Where the window holds no message that began there (it was
    /// emptied since, or the append carried nothing), what follows the
    /// member's end goes again as far as that append's start, once: from
    /// its hint where its log `agrees` with this one there, else from what it
    /// answered. False when the log no longer holds what goes again.
    fn repair(&mut self, message: &Message, progress: &mut Progress, agrees: bool) -> Result<bool> {
        let matched = progress.matched;
        if let Some(held) = progress.inflights.delivered(message.index, matched) {
            return self.repair_before(message.from, progress, held);
        }
        let end = if agrees && message.reject_hint >= matched {
            message.reject_hint
        } else {
            matched
        };
        let after = end.max(progress.repaired);
        if after >= message.index {
            return Ok(true);
        }
        if !self.resend(message.from, after, message.index)? {
            return Ok(false);
        }
        progress.repaired = message.index;
        Ok(true)
    }
    /// Sends `to` again each message out before the one that ended at
    /// `kept`, which `to` kept ahead of a hole ([`Ahead::Kept`]), that it has
    /// neither answered nor been sent again: what it lacks before what it
    /// kept, as RFC 6675 sends again every segment `IsLost` names once later
    /// data arrived (RFC 2018). Each goes from past what `to` answered or was
    /// sent again, anchored there, so that `to` keeps one that arrives ahead
    /// of an earlier hole too. False when the log no longer holds one.
    fn repair_before(&mut self, to: NodeId, progress: &mut Progress, kept: u64) -> Result<bool> {
        let mut before = progress.matched;
        let mut position = 0usize;
        while let Some(last) = progress.inflights.last_at(position) {
            if last >= kept {
                break;
            }
            let delivered = progress.inflights.delivered_at(position);
            position = position.saturating_add(1);
            let after = before.max(progress.matched).max(progress.repaired);
            before = last;
            if last <= after || delivered {
                continue;
            }
            if !self.resend(to, after, last)? {
                return Ok(false);
            }
            progress.repaired = last;
        }
        Ok(true)
    }
    /// Sends `to` again the entries after `after` through `through`, a page
    /// of them at most, its progress and window as they were: the window
    /// still counts their first sending, as RFC 6675's pipe counts a
    /// retransmission in place of the lost segment. False when the log no
    /// longer holds them.
    fn resend(&mut self, to: NodeId, after: u64, through: u64) -> Result<bool> {
        let held = usize::try_from(through.saturating_sub(after)).unwrap_or(usize::MAX);
        let page = self.log.page(
            after.saturating_add(1),
            self.max_bytes,
            self.max_entries.min(held),
        );
        let term = self.log.term(after);
        let (Ok(term), Ok(page)) = (term, page) else {
            return Ok(false);
        };
        if page.entries.is_empty() {
            return Ok(false);
        }
        let message = Message {
            to,
            msg_type: MessageType::MsgAppend,
            index: after,
            log_term: term,
            commit: self.log.committed(),
            classic: Some(self.classic),
            entries: page.entries,
            ..Message::default()
        };
        self.send_page(message, page.payload)?;
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
        // Never a commit the member may not hold, nor a classic one.
        message.commit = progress.matched.min(self.log.committed());
        message.classic = Some(progress.matched.min(self.classic));
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

/// The last term, and the last index, a member reaches: a message that names `u64::MAX` is beyond
/// what is counted ([`counts_beyond_bound`]), so `u64::MAX` is never a term or an index, and the
/// step past this one has no successor and is refused before anything changes (slates
/// `docs/bugs/2026-09-30-a-saturated-term-let-two-leaders-share-it.md`, mantle note 32 R6: a term
/// saturated at its last value let two leaders share it, and an index saturated there gave two
/// entries one index).
pub(crate) const LAST: u64 = u64::MAX - 1;

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

impl<S> Raft<S> {
    /// Whether `mutant` is planted in this member.
    #[cfg(feature = "mutants")]
    pub(crate) fn planted(&self, mutant: crate::Mutant) -> bool {
        self.mutant == Some(mutant)
    }
    /// Whether `mutant` is planted in this member: never, without the `mutants` feature.
    #[cfg(not(feature = "mutants"))]
    pub(crate) fn planted(&self, _mutant: crate::Mutant) -> bool {
        false
    }
}

impl<S: Storage> Raft<S> {
    /// The member `config` names, opened on what `store` holds.
    pub fn new(config: &Config, store: S) -> Result<Self> {
        config.validate()?;
        let initial = store.initial_state()?;
        let configuration = Configuration::from_conf_state(&initial.configuration)?;
        let conf_before = configuration.try_clone()?;
        let applied_conf = configuration.try_clone()?;
        let log = Log::new(store, config.limits.unstable_entries)?;
        let mut tracker = Tracker::new(
            configuration,
            log.last_index()?,
            config.max_inflight_msgs,
            config.max_inflight_bytes,
            config.limits.members,
        )?;
        tracker.set_page(config.max_size_per_msg);
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
            quorum_patience: 0,
            quorum_elapsed: 0,
            silence: 0,
            heartbeat_elapsed: 0,
            randomized_election_timeout: config.election_tick,
            patience: 0,
            early: Early::default(),
            stagings: Stagings::default(),
            ticks: 0,
            taken_ahead: 0,
            held: Proposals::new(config.limits.proposals, config.limits.proposal_bytes),
            classic: 0,
            votes: Votes::new(
                usize::try_from(config.limits.fast_window).unwrap_or(usize::MAX),
                config.limits.vote_bytes,
                config.limits.members,
            ),
            decided: Decided::default(),
            displaced: Vec::new(),
            takes_through: None,
            voted_to: (0, 0),
            fast_stats: FastStats::default(),
            holders: Vec::new(),
            term_voters: Vec::new(),
            term_next: Vec::new(),
            term_known: false,
            priority: config.priority,
            priority_in_force: 0,
            durable_commit: 0,
            lost: config.lost,
            conf_before: Configuration::default(),
            conf_before_at: 0,
            conf_newest_at: 0,
            applied_conf: Configuration::default(),
            #[cfg(feature = "mutants")]
            mutant: None,
            log,
            tracker,
            read_only: ReadOnly::new(config.limits.pending_reads, config.limits.members),
            read_states: Vec::new(),
            deferred_reads: Vec::new(),
            msgs: Outgoing::default(),
            uncommitted_bytes: 0,
            leader_tail: 0,
            term_start: 0,
            random: config.seed,
            watch: match config.elections {
                Elections::Ticks => None,
                // It knows no leader yet (`become_follower` below arms it).
                Elections::Suspicion => Some(Box::default()),
            },
            config: config.clone(),
        };
        raft.conf_before = conf_before;
        raft.applied_conf = applied_conf;
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
        // The configuration storage states is the one applied; the entries above it make the one
        // elections and commitment count by.
        let applied = raft
            .log
            .applied()
            .max(raft.log.first_index()?.saturating_sub(1));
        raft.conf_before_at = applied;
        raft.conf_newest_at = applied;
        raft.refresh_configuration(applied.saturating_add(1))?;
        // What storage released the member knew committed by a classic quorum; the rest it
        // holds until it knows that of their indexes.
        raft.classic = initial.released.min(raft.log.committed());
        for held in initial.proposals {
            if held.index > raft.classic {
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
    /// The configuration this member counts votes and commitment by: the newest its log states,
    /// committed or not (`docs/raft.md` §3.4).
    pub fn configuration(&self) -> &Configuration {
        self.tracker.configuration()
    }
    /// The configuration the owner applied, change by change ([`Raft::apply_conf_change`]): what
    /// the state machine holds, which lags the one this member counts by while a change is in the
    /// log and not applied.
    pub fn applied_configuration(&self) -> &Configuration {
        &self.applied_conf
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
    /// Whether an answer to round `round` that names the read `context` confirms it, the round
    /// having been sent while it waited ([`ReadOnly::ack_round`]); none where no such read waits.
    pub fn round_confirms(&self, context: &[u8], round: u64) -> Option<bool> {
        self.read_only
            .round_of(context)
            .map(|first| first != 0 && first <= round)
    }
    /// How many reads wait to be taken.
    pub fn ready_read_count(&self) -> usize {
        self.read_states.len()
    }
    /// How many reads wait for this leader's first commit of its term.
    pub fn deferred_read_count(&self) -> usize {
        self.deferred_reads.len()
    }
    /// Ticks since this member last heard its leader, or last campaigned.
    pub fn election_elapsed(&self) -> usize {
        self.election_elapsed
    }
    /// Ticks since this member last heard its leader, whatever its own timer did: what its lease
    /// counts.
    pub fn silence(&self) -> usize {
        self.silence
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
    /// Why this member could not lead a term it campaigned for, if it could not (mantle note 32
    /// R6): its term has no successor a message may name, or its log has no index for the entry
    /// a leader's term begins with. Asked before a campaign changes anything, so that the refusal
    /// is one; and by suspicion before a campaign is armed, so that none is due that cannot be
    /// made.
    fn lead_refusal(&self) -> Result<Option<Error>> {
        if self.term >= LAST {
            return Ok(Some(Error::Capacity("terms")));
        }
        if self.log.last_index()? >= LAST {
            return Ok(Some(Error::Capacity("the log's indexes")));
        }
        Ok(None)
    }
    /// Whether this member could lead a term it campaigned for ([`Raft::lead_refusal`]).
    fn may_lead(&self) -> bool {
        matches!(self.lead_refusal(), Ok(None))
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
    /// Whether this member may campaign: the newest configuration its log states names it a voter;
    /// or that configuration's entry is not known committed and the one before names it a voter,
    /// for the group may still need it until it is (Ongaro's thesis §4.2.2: a server "not part of
    /// its own latest configuration should still start new elections, as it might still be needed
    /// until the C_new entry is committed"). Its own vote counts only where it is a voter.
    pub fn promotable(&self) -> bool {
        let id = self.id;
        self.tracker.configuration().votes(id)
            || (self.log.committed() < self.conf_newest_at && self.conf_before.votes(id))
    }
    /// Whether a change may be in the log and not applied.
    pub fn has_pending_conf(&self) -> bool {
        self.pending_conf_index > self.log.applied()
    }
    /// Whether this leader committed the blank entry it began its term with: it serves reads only
    /// once it has, as then it knows every entry committed before its term (the thesis's §6.4: a
    /// leader "commit[s] a blank no-op entry into the log at the start of its term"). An entry of
    /// its term below that one is not enough: what it recovered of the fast track bears its term
    /// and may lie below an index a fast quorum committed in an earlier term.
    pub fn commit_to_current_term(&self) -> bool {
        // Every entry from that one on is of its term: no term need be read from the log.
        self.term_start != 0 && self.log.committed() >= self.term_start
    }
    /// The priority the owner gave.
    pub fn priority(&self) -> i64 {
        self.priority
    }
    /// The priority votes are judged by now.
    pub fn priority_in_force(&self) -> i64 {
        self.priority_in_force
    }
    /// What the member does with an append that arrives ahead of a hole ([`Ahead`]), from the next
    /// append on. An owner whose peers could not read a kept refusal runs `Ahead::Refused` and
    /// turns `Ahead::Kept` on once every peer can (focal's upgrade fence). Either change is safe at
    /// any point: what was kept is taken in when the hole fills whatever the rule, and the rule
    /// decides only whether a new append is kept and said so.
    pub fn set_ahead(&mut self, ahead: Ahead) {
        self.config.ahead = ahead;
    }
    /// The priority the owner gives, in force once the member has a term.
    pub fn set_priority(&mut self, priority: i64) {
        self.priority = priority;
        self.settle_priority();
    }
    /// The path to `member` carries `bytes` before it answers: no more is
    /// sent to it ahead of its answers, each append counted at its record,
    /// but for one entry that is larger. An owner says twice what the path
    /// carries in a round trip (`hyper_timing::inflight_window`, mantle note
    /// 32 R16). False, and nothing changed, for a member the configuration
    /// does not name, and for no bound at all where the window counts no
    /// messages of its own. What is out stays counted; a bound that rose
    /// admits more with the member's next answer, or the leader's next
    /// append.
    pub fn set_inflight_bytes(&mut self, member: NodeId, bytes: u64) -> bool {
        match self.tracker.get_mut(member) {
            Some(progress) if bytes != u64::MAX || progress.inflights.counts_messages() => {
                progress.inflights.set_byte_cap(bytes);
                true
            }
            _ => false,
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
        self.priority_in_force = if self.term == 0 || !self.promotable() {
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
    /// Ticks this leader waits beyond its election timeout before it asks whether a quorum heard
    /// it.
    pub fn quorum_patience(&self) -> usize {
        self.quorum_patience
    }
    /// Give this leader `ticks` beyond its election timeout before it asks whether a quorum heard
    /// it, for what its owner measured of its voters' answers (`docs/raft.md` §3.6). Refused,
    /// typed, where the election timeout and it pass what a tick counts. Under elections by
    /// suspicion it is kept and changes nothing: those take no ticks, and the owner's detectors
    /// depose.
    pub fn set_quorum_patience(&mut self, ticks: usize) -> Result<()> {
        if self.config.election_tick.checked_add(ticks).is_none() {
            return Err(Error::Settings("a quorum patience past what a tick counts"));
        }
        self.quorum_patience = ticks;
        Ok(())
    }
    /// The ticks after which this leader asks whether a quorum heard it.
    fn quorum_due(&self) -> usize {
        self.config
            .election_tick
            .saturating_add(self.quorum_patience)
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
            .saturating_add(self.early.resident_bytes())
            .saturating_add(self.stagings.resident_bytes())
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
        self.early.check()?;
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
            && self.promotable()
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
                || self.quorum_elapsed.saturating_add(1) >= self.quorum_due())
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
                classic: self.classic,
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
        // It carries the last read asked: it is a round for every read that waits, and the one
        // that asks again for a round that was lost.
        let mut last = Vec::new();
        let asks = match self.read_only.last_context() {
            Some(context) => {
                last.try_reserve_exact(context.len())
                    .map_err(|_| Error::Memory)?;
                last.extend_from_slice(context);
                true
            }
            None => false,
        };
        let round = self.read_only.asked();
        let carried = if asks {
            Some(ReadOnly::round_context(&last, round)?)
        } else {
            None
        };
        self.bcast_heartbeat_with(carried.as_deref())
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
        let term = if self.planted(crate::Mutant::OlderTermCommit) {
            self.log.term(index).unwrap_or(self.term)
        } else {
            self.term
        };
        let classic = self.log.maybe_commit(index, term)?;
        // The quorum holds this leader's entry at `index`, so every later leader's log holds it
        // and every entry before it: what this member holds by itself through it is held no more.
        // A fast quorum may have committed it first. The members learn it with the next append or
        // heartbeat (`Message::classic`), never by a round of its own: a release frees room, and
        // one that comes later is still right (`docs/raft.md` §3.5, "The cost").
        let known =
            index > self.classic && self.log.term(index).is_ok_and(|held| held == self.term);
        if known {
            self.learn_classic(index)?;
        }
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
        self.release_deferred_reads()?;
        self.leave_joint_when_committed()?;
        Ok(true)
    }
    /// A joint configuration that leaves by itself does so once the entry that entered it is
    /// committed: the leader writes the entry that leaves. Written, it is the newest change and
    /// above the commit, so it is written once.
    fn leave_joint_when_committed(&mut self) -> Result<()> {
        let configuration = self.tracker.configuration();
        if self.state != StateRole::Leader
            || !configuration.is_joint()
            || !configuration.auto_leave()
            || self.log.committed() < self.conf_newest_at
        {
            return Ok(());
        }
        let leave = Entry {
            entry_type: EntryType::EntryConfChangeV2,
            ..Entry::default()
        };
        // An entry that states nothing takes no room and is never refused for it.
        if !self.append_entry(vec![leave])? {
            return Err(Error::Invariant(
                "the entry that leaves a joint configuration was refused",
            ));
        }
        self.pending_conf_index = self.log.last_index()?;
        Ok(())
    }
    /// The application applied through `applied`.
    pub fn commit_apply(&mut self, applied: u64) -> Result<()> {
        self.log.applied_to(applied)?;
        if self.state != StateRole::Leader {
            return Ok(());
        }
        // A leader the newest configuration names no voter leads until that configuration is
        // committed and applied, without counting itself; then the group is another's to lead.
        if applied >= self.conf_newest_at && !self.tracker.configuration().votes(self.id) {
            return self.hand_leadership_on();
        }
        // A change that is applied may open the fast track.
        if self.config.fast && self.maybe_commit()? {
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
        self.term_start = 0;
        self.reset_randomized_election_timeout();
        self.election_elapsed = 0;
        self.quorum_elapsed = 0;
        self.silence = 0;
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
        // What was kept ahead of a hole was one leader's, in one term; a
        // learner's rounds are one leader's too.
        self.early.clear();
        self.stagings.clear();
        self.votes.clear();
        self.decided.clear();
        self.tracker.reset_votes();
        self.pending_conf_index = 0;
        self.read_only.clear();
        self.deferred_reads.clear();
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
        if last.checked_add(count).is_none_or(|end| end > LAST) {
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
        // A change takes effect as it is written.
        self.refresh_configuration(last.saturating_add(1))?;
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
        self.silence = self.silence.saturating_add(1);
        if self.election_elapsed
            < self
                .randomized_election_timeout
                .saturating_add(self.patience)
            || !self.promotable()
        {
            return Ok(false);
        }
        self.election_elapsed = 0;
        self.local(MessageType::MsgHup)?;
        Ok(true)
    }
    fn tick_heartbeat(&mut self) -> Result<bool> {
        self.ticks = self.ticks.saturating_add(1);
        self.heartbeat_elapsed = self.heartbeat_elapsed.saturating_add(1);
        self.election_elapsed = self.election_elapsed.saturating_add(1);
        self.quorum_elapsed = self.quorum_elapsed.saturating_add(1);
        let mut ready = false;
        // The quorum check on its own counter: one election timeout and the leader's quorum
        // patience. The members it judges are those heard from at any point since the last
        // check, the marks being cleared only by the check (`ProgressTracker::quorum_recently_active`),
        // so a longer window counts every answer within it.
        if self.quorum_elapsed >= self.quorum_due() {
            self.quorum_elapsed = 0;
            if self.config.check_quorum {
                ready = true;
                self.local(MessageType::MsgCheckQuorum)?;
            }
        }
        // A transfer that did not finish within an election timeout is given up, whatever the
        // quorum patience.
        if self.election_elapsed >= self.config.election_tick {
            self.election_elapsed = 0;
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
    /// What decides this member's next campaign by suspicion; none on ticks.
    pub fn campaign_state(&self) -> Option<CampaignState> {
        let watch = self.watch.as_ref()?;
        Some(CampaignState {
            due: Watch::due(watch.campaign),
            armed: watch.campaign != Arm::Off,
            led: watch.led == self.term && self.term != 0,
            held: watch.held,
            trusted_quorum: self.trusted_quorum(),
            may_campaign: self.may_campaign(),
            may_lead: self.may_lead(),
            promotable: self.promotable(),
        })
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
        let members = self.config.limits.members;
        let watch = self.watch_mut()?;
        if member == id || !watch.suspect(member, members)? {
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
        self.watch_mut()?.timing = Some((
            nanos(timing.span),
            nanos(timing.round),
            nanos(timing.election),
        ));
        Ok(())
    }
    /// The timing the owner last gave, if any.
    pub fn timing(&self) -> Option<Timing> {
        let (span, round, election) = self.watch.as_ref()?.timing?;
        Some(Timing {
            span: std::time::Duration::from_nanos(span),
            round: std::time::Duration::from_nanos(round),
            election: std::time::Duration::from_nanos(election),
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
        // The rules that hold a campaign are asked only of one that is armed: an owner that scans
        // its idle groups asks each member, and a follower that trusts its leader has none.
        let campaign = Watch::due(watch.campaign).filter(|_| {
            (watch.led == self.term && self.term != 0)
                || (self.promotable()
                    && !watch.held
                    && self.may_campaign()
                    && self.may_lead()
                    && self.trusted_quorum())
        });
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
        let Some(watch) = self.watch.as_mut() else {
            return Ok(false);
        };
        watch.now = watch.now.max(now);
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
    /// not suspect it, and the configuration it counts by names it a voter (a
    /// leader that is none leads only until that configuration is committed,
    /// and hands over once it applies it).
    fn trusts_leader(&self, watch: &Watch) -> bool {
        self.leader_id != 0
            && !watch.suspects(self.leader_id)
            && self.tracker.configuration().votes(self.leader_id)
    }
    fn wake_follower(&mut self, now: u64) -> Result<bool> {
        let (promotable, local, term) = (self.promotable(), self.config.seed, self.term);
        let alone = self.tracker.is_singleton() && self.tracker.configuration().votes(self.id);
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
                Arm::Off | Arm::Unset { .. } => false,
            };
        if !due {
            return Ok(false);
        }
        // The rules that hold a campaign are asked only of one that is due: the owner wakes a
        // member after every call it makes (`Raft::deadline` likewise).
        let held = watch.held;
        let campaigns =
            promotable && !held && self.trusted_quorum() && self.may_campaign() && self.may_lead();
        if !campaigns {
            if !led {
                return Ok(false);
            }
            // It cannot campaign, and its followers trust it: it hands over
            // again, a round and a draw after the last, as a candidate asks
            // again, for the order may have been lost.
            let watch = self.watch_mut()?;
            watch.campaign = match (watch.round(), watch.draw(local)) {
                (Some(tail), Some(delay)) => {
                    Arm::At(now.saturating_add(tail).saturating_add(delay))
                }
                _ => Arm::Unset { round: true },
            };
            self.hand_over()?;
            return Ok(true);
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
            .filter(|term| *term <= LAST)
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
        if self.term.checked_add(1).is_none_or(|term| term > LAST) {
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
                self.config.limits.members,
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
        // A member campaigns only with an index for this entry (`Raft::lead_refusal`), and what it
        // recovers lies below the last index (`track::proposable`): no index here is a state that
        // no longer adds up, never a refusal of something that changed nothing.
        let appended = self
            .append_entries(first, true)
            .map_err(|error| match error {
                Error::Capacity(_) => Error::Invariant("a leader's first entry has no index"),
                other => other,
            });
        if !appended? {
            return Err(Error::Invariant("a leader's first entry was refused"));
        }
        let last = self.log.last_index()?;
        self.term_start = last;
        Ok(())
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
        // This campaign asks every voter: what an earlier one asked and has
        // not yet left is superseded.
        msgs.supersede_requests(id);
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
    fn hup(&mut self, transfer: bool) -> Result<()> {
        if self.state == StateRole::Leader {
            return Ok(());
        }
        // One that is no voter would count votes it cannot hold, and lead
        // a group it is no member of.
        if !self.promotable() {
            return Err(Error::NotPromotable);
        }
        if let Some(refusal) = self.lead_refusal()? {
            return Err(refusal);
        }
        if !self.may_campaign() {
            // Its leader is gone, or it would not be asked: it holds no
            // lease on one.
            self.leader_id = 0;
            return Err(Error::Lost);
        }
        // It counts by the newest configuration its log holds, applied or not (`promotable`, and
        // the tracker's): nothing waits for an owner to apply a change.
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
                        && self.silence < self.config.election_tick
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
        if matches!(kind, MessageType::MsgHeartbeat | MessageType::MsgAppend) || handed {
            // A leader of an older term: this member moved its term
            // while it was cut off, or holds a configuration that names
            // that leader no voter. Its answer tells that leader (thesis
            // Figure 3.1: "reply false if term < currentTerm"), which no
            // vote request of this member would: refused while the leader
            // is heard, and never sent to a member its configuration does
            // not name a voter. raft-rs answers only under check-quorum or
            // pre-vote and leaves the rest to vote requests; without either,
            // a leader whose group's later configuration made it a learner
            // led its old term for ever (`docs/sim.md` §15.9).
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
        let ahead = message.log_term > last_term
            || (message.log_term == last_term && message.index > last_index);
        #[cfg(feature = "raft-rs-precedence")]
        let ahead = if self.config.raft_rs_precedence {
            message.index > last_index
        } else {
            ahead
        };
        // A member whose log may lack what it acknowledged refuses no one
        // for priority: a voter that refuses for priority must be one the
        // group could elect instead (the log's precedence), and a marked member
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
        // A candidate counts votes by the newest configuration its log holds, which a commit does
        // not change.
        self.log.maybe_commit(message.commit, message.commit_term)?;
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
            return Err(Error::ProposalDropped(Dropped::Empty));
        }
        if self.tracker.get(self.id).is_none() {
            return Err(Error::ProposalDropped(Dropped::NotMember));
        }
        if self.lead_transferee.is_some() {
            return Err(Error::ProposalDropped(Dropped::Transferring));
        }
        let last = self.log.last_index()?;
        let mut pending = self.pending_conf_index;
        let mut index = last;
        for entry in &mut message.entries {
            index = index.saturating_add(1);
            let plan = match Plan::of_entry(entry) {
                Ok(None) => continue,
                Ok(Some(plan)) => plan,
                Err(_) => return Err(Error::ProposalDropped(Dropped::Malformed)),
            };
            let joint = self.tracker.configuration().is_joint();
            let leaves = plan.stated == 0;
            // A change past the members a configuration of the group names
            // would stop every member it reached.
            let past = plan
                .apply(self.tracker.configuration())
                .is_ok_and(|changed| {
                    changed.configuration.members().count() > self.config.limits.members
                });
            if pending > self.log.applied() || joint != leaves || past {
                // One change at a time, out of a joint configuration before
                // into another, and within the group's members: the entry
                // keeps its place and states nothing.
                *entry = Entry::default();
            } else {
                pending = index;
            }
        }
        let entries = std::mem::take(&mut message.entries);
        if !self.append_entry(entries)? {
            return Err(Error::ProposalDropped(Dropped::Uncommitted));
        }
        self.pending_conf_index = pending;
        self.bcast_append()
    }
    fn read_index(&mut self, mut message: Message) -> Result<()> {
        let Some(entry) = message.entries.first_mut() else {
            return Err(Error::Violation("a read without its context"));
        };
        let context = std::mem::take(&mut entry.data);
        // A leader knows what is committed once it committed in its term: until then the read
        // waits for that commit (the thesis's §6.4 step 1), where it was dropped and its asker
        // left to its deadline.
        if !self.commit_to_current_term() && !self.planted(crate::Mutant::ReadBeforeFirstCommit) {
            if self.reads_held() >= self.config.limits.pending_reads {
                return Err(Error::Capacity(
                    "reads that wait for the term's first commit",
                ));
            }
            self.deferred_reads
                .try_reserve(1)
                .map_err(|_| Error::Memory)?;
            self.deferred_reads.push((message.from, context));
            return Ok(());
        }
        self.serve_read(message.from, context)
    }
    /// Every read this leader holds: waiting for its term's first commit, for its quorum, or to
    /// be taken.
    fn reads_held(&self) -> usize {
        self.read_only
            .len()
            .saturating_add(self.read_states.len())
            .saturating_add(self.deferred_reads.len())
    }
    /// The reads that waited for this leader's first commit of its term, in the order asked, once
    /// that commit is made.
    fn release_deferred_reads(&mut self) -> Result<()> {
        if self.deferred_reads.is_empty() || !self.commit_to_current_term() {
            return Ok(());
        }
        // Taken and put back, so the queue keeps its room for the next term.
        let mut deferred = std::mem::take(&mut self.deferred_reads);
        let served = deferred
            .drain(..)
            .try_for_each(|(from, context)| self.serve_read(from, context));
        self.deferred_reads = deferred;
        served
    }
    /// A read asked of a leader that committed in its term: answered at once by a member alone,
    /// or confirmed by a round.
    fn serve_read(&mut self, from: NodeId, context: Vec<u8>) -> Result<()> {
        let committed = self.log.committed();
        // Alone only where it is the one voter: a leader the newest configuration in its log
        // leaves out leads until that configuration is committed, and its one voter may have
        // been elected and committed since (seed 47 of the hostile schedules).
        if self.tracker.is_singleton() && self.tracker.configuration().votes(self.id) {
            return self.answer_read(from, committed, context);
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
            ReadRounds::Shared => self.read_only.add(committed, context, from, self.id),
            ReadRounds::Each => {
                let mut heartbeat = Vec::new();
                heartbeat
                    .try_reserve_exact(context.len())
                    .map_err(|_| Error::Capacity("reads that wait for their quorum"))?;
                heartbeat.extend_from_slice(&context);
                self.read_only.add(committed, context, from, self.id)?;
                let round = self.read_only.asked();
                let carried = ReadOnly::round_context(&heartbeat, round)?;
                self.bcast_heartbeat_with(Some(&carried))
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
        if message.reject {
            return self.handle_append_refusal(message);
        }
        let last = self.log.last_index()?;
        let Some(progress) = self.tracker.get_mut(message.from) else {
            return Ok(());
        };
        progress.recent_active = true;
        progress.update_committed(message.commit);
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
        self.note_progress(message.from)?;
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
    /// A member refused an append.
    fn handle_append_refusal(&mut self, message: &Message) -> Result<()> {
        // Asked for on a refusal only: an acknowledgement checks nothing
        // more than before.
        if message.lost && self.take_lost(message)? {
            return Ok(());
        }
        let mut next_probe = message.reject_hint;
        if message.log_term > 0 {
            // The member holds `log_term` at its hint. No index of this log
            // at or below the hint with a higher term can match it, for terms
            // only rise along a log: probe at the last one that may.
            next_probe = self
                .log
                .find_conflict_by_term(message.reject_hint, message.log_term)?
                .0;
        }
        // The member refused an append that began past the end of its log
        // (`message.index`, after its hint), and says it kept it (R17): it
        // holds entries past it. Not a conflict inside its log, nor a member
        // that keeps nothing ahead (raft-rs's, or one of `Ahead::Refused`),
        // which the probe below serves.
        let ahead =
            message.kept && message.request_snapshot == 0 && message.index > message.reject_hint;
        // Its log ends at the hint and holds this log's entry there.
        let agrees = ahead
            && self
                .log
                .term(message.reject_hint)
                .is_ok_and(|term| term == message.log_term);
        let (mut outbox, tracker) = self.outbox();
        let Some(progress) = tracker.get_mut(message.from) else {
            return Ok(());
        };
        progress.recent_active = true;
        progress.update_committed(message.commit);
        if ahead
            && progress.state == ProgressState::Replicate
            && outbox.repair(message, progress, agrees)?
        {
            return Ok(());
        }
        // A conflict, a member that keeps nothing ahead, or a hole the log no
        // longer holds: probed (raft-rs's rule), for a snapshot where needed.
        if progress.maybe_decrease_to(message.index, next_probe, message.request_snapshot) {
            if progress.state == ProgressState::Replicate {
                progress.become_probe();
            }
            self.send_append(message.from)?;
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
        // A round's answer names the round and the last read it asked for.
        let Some((context, round)) = ReadOnly::of_round(&message.context) else {
            return Ok(());
        };
        let confirmed = match self.read_only.ack_round(message.from, context, round)? {
            Some(acks) => self.tracker.has_quorum(acks),
            None => false,
        };
        if confirmed {
            self.confirm_reads(context)?;
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
        // A transfer finishes within an election timeout or is given up. The quorum check's
        // counter starts again with it, as when the two shared one counter, so a leader of no
        // quorum patience checks exactly when it did.
        self.election_elapsed = 0;
        self.quorum_elapsed = 0;
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

    /// A read with no leader to ask: refused when asked here, so its owner answers it now; one a
    /// peer forwarded is dropped, and that peer's owner answers its own.
    fn drop_read(message: &Message) -> Result<()> {
        if message.from == 0 {
            return Err(Error::ReadDropped);
        }
        Ok(())
    }

    fn step_candidate(&mut self, kind: MessageType, message: Message) -> Result<()> {
        match kind {
            MessageType::MsgPropose => Err(Error::ProposalDropped(Dropped::NoLeader)),
            MessageType::MsgReadIndex => Self::drop_read(&message),
            MessageType::MsgAppend | MessageType::MsgHeartbeat | MessageType::MsgSnapshot
                if self.state == StateRole::PreCandidate && self.suspects(message.from) =>
            {
                // A pre-candidate keeps its leader's term, so what that leader sends would make it
                // a follower again and end the asking its detectors began: a leader that is up
                // answers a pre-vote with a heartbeat, which can reach the asker before any grant.
                // While it asks it does not hear a leader it suspects; the message is dropped, as
                // a lost one.
                Ok(())
            }
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
                if self.state == StateRole::PreCandidate && self.promotable() {
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
                    return Err(Error::ProposalDropped(Dropped::NoLeader));
                }
                self.forward(message)
            }
            MessageType::MsgAppend => {
                self.election_elapsed = 0;
                self.silence = 0;
                self.leader_id = message.from;
                self.followed(message.from);
                self.handle_append_entries(message)?;
                self.heard_leader()
            }
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.silence = 0;
                self.leader_id = message.from;
                self.followed(message.from);
                self.handle_heartbeat(message)?;
                self.heard_leader()
            }
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.silence = 0;
                self.leader_id = message.from;
                self.followed(message.from);
                self.handle_snapshot(message)?;
                self.heard_leader()
            }
            MessageType::MsgTransferLeader => {
                if self.leader_id == 0 {
                    return Ok(());
                }
                self.forward(message)
            }
            MessageType::MsgReadIndex => {
                if self.leader_id == 0 {
                    return Self::drop_read(&message);
                }
                self.forward(message)
            }
            MessageType::MsgTimeoutNow => {
                // The leader asks: the group is not cut off, and there is nothing to ask about
                // first. One the newest configuration in its log names no voter says so.
                self.hup(true)
            }
            MessageType::MsgReadIndexResp => {
                if message.entries.len() != 1 {
                    return Err(Error::Violation("a read's answer without its one context"));
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
        let end = self.log.last_index()?;
        let kept = message.index > end && self.config.ahead == Ahead::Kept && self.lost.is_none();
        answer.kept = kept;
        if kept {
            // Past the end of the log: the append is refused below, and its
            // entries, this term's leader's, are kept to take in once the
            // hole is filled (`crate::ahead`), as many as the log may hold
            // not yet durable.
            let ahead = std::mem::take(&mut taken);
            let bound = self.config.limits.unstable_entries;
            self.early
                .keep(self.term, message.index, end, ahead, bound)?;
        }
        // The leader's entries move into the log uncopied.
        match self.log.append_after_owned(
            message.index,
            message.log_term,
            message.commit,
            taken,
            self.config.fast,
        )? {
            Some((written, last)) => {
                if written != 0 {
                    // A change takes effect as it is taken, and one the append replaced ends.
                    self.refresh_configuration(written)?;
                }
                let last = self.take_ahead(last, message.commit)?;
                answer.index = last;
                // The log matches the leader's through `last`.
                if let Some(classic) = message.classic {
                    self.learn_classic(classic.min(last))?;
                }
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
    /// Takes into the log what this member kept ahead of a hole that now
    /// continues the entries an append of this term placed through `last`,
    /// as the leader's next append would have, to what the log may hold not
    /// yet durable; the last index it then holds of the leader's log. The
    /// answer that says so leaves with the write that holds them (I2).
    fn take_ahead(&mut self, last: u64, commit: u64) -> Result<u64> {
        if self.early.is_empty() {
            return Ok(last);
        }
        let room = self
            .config
            .limits
            .unstable_entries
            .saturating_sub(self.log.unstable().count_through(last));
        let run = self.early.take_after(last, room);
        if run.is_empty() {
            return Ok(last);
        }
        let term = self.log.term(last)?;
        match self
            .log
            .append_after_owned(last, term, commit, run, self.config.fast)?
        {
            Some((written, taken)) => {
                self.taken_ahead = self.taken_ahead.saturating_add(taken.saturating_sub(last));
                if written != 0 {
                    self.refresh_configuration(written)?;
                }
                Ok(taken)
            }
            None => Err(Error::Invariant(
                "the log does not hold what an append just placed in it",
            )),
        }
    }
    /// The indexes of what this member keeps ahead of a hole in its log, in
    /// order ([`Ahead::Kept`]).
    pub fn kept_ahead(&self) -> impl Iterator<Item = u64> + '_ {
        self.early.indexes()
    }
    /// The entries this member took into its log from what it kept ahead of
    /// a hole, since it opened.
    pub fn taken_ahead(&self) -> u64 {
        self.taken_ahead
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
    /// The member's clock for its learners' rounds: its own ticks; by
    /// suspicion, the owner's clock at its last wake.
    fn clock(&self) -> u64 {
        match self.watch.as_ref() {
            None => self.ticks,
            Some(watch) => watch.now,
        }
    }
    /// An election on that clock ([`CatchUp`]): the minimum election timeout
    /// on ticks (thesis §4.2.1's "an election timeout"); by suspicion, the
    /// election the law expects, once the owner gave it.
    fn election_on_clock(&self) -> Option<u64> {
        match self.watch.as_ref() {
            None => u64::try_from(self.config.election_tick).ok(),
            Some(watch) => watch.election(),
        }
    }
    /// Where catching up `member` stands (Ongaro's thesis §4.2.1,
    /// `crate::catchup`). Asked first while this member leads, it stages the
    /// learner, its first round replicating what the leader holds now; asked
    /// again, it judges the learner as an election passes, and gives it up
    /// once its lag did not shrink over one (said once; asked again, it is
    /// staged afresh). Its rounds end as its answers arrive. A voter is ready.
    pub fn catch_up(&mut self, member: NodeId) -> Result<CatchUp> {
        if self.state != StateRole::Leader {
            return Ok(CatchUp::NotLeader);
        }
        let configuration = self.tracker.configuration();
        if !configuration.contains(member) {
            return Ok(CatchUp::NotMember);
        }
        if configuration.votes(member) {
            return Ok(CatchUp::Ready);
        }
        let (last, clock, election) = (
            self.log.last_index()?,
            self.clock(),
            self.election_on_clock(),
        );
        let matched = self
            .tracker
            .get(member)
            .map_or(0, |progress| progress.matched);
        let lag = last.saturating_sub(matched);
        let Some(staging) = self.stagings.get_mut(member) else {
            let ready = matched >= last;
            self.stagings.put(
                member,
                Staging {
                    round_end: last,
                    began: clock,
                    lag,
                    judged: clock,
                    ready,
                },
            )?;
            return Ok(if ready {
                CatchUp::Ready
            } else {
                CatchUp::Pending
            });
        };
        if staging.ready {
            return Ok(CatchUp::Ready);
        }
        if matched >= staging.round_end {
            // The round ended with no answer since to note it (`note_progress`): a
            // round that began holding what it replicates.
            if election.is_some_and(|election| clock.saturating_sub(staging.began) < election) {
                staging.ready = true;
                return Ok(CatchUp::Ready);
            }
            staging.round_end = last;
            staging.began = clock;
        }
        let due = election.is_some_and(|election| clock.saturating_sub(staging.judged) >= election);
        if !due {
            return Ok(CatchUp::Pending);
        }
        if lag >= staging.lag {
            // Unavailable, or slower than the leader appends: "the leader should
            // also abort the change" (§4.2.1).
            self.stagings.remove(member);
            return Ok(CatchUp::Aborted);
        }
        staging.lag = lag;
        staging.judged = clock;
        Ok(CatchUp::Pending)
    }
    /// An answer moved `member`'s progress: a round of its catch-up ends once
    /// it holds the round's end. One that lasted less than an election is the
    /// last; a longer one begins the next with what this leader holds now.
    fn note_progress(&mut self, member: NodeId) -> Result<()> {
        if self.stagings.is_empty() {
            return Ok(());
        }
        let (last, clock, election) = (
            self.log.last_index()?,
            self.clock(),
            self.election_on_clock(),
        );
        let matched = self
            .tracker
            .get(member)
            .map_or(0, |progress| progress.matched);
        let Some(staging) = self.stagings.get_mut(member) else {
            return Ok(());
        };
        if staging.ready || matched < staging.round_end {
            return Ok(());
        }
        let lasted = clock.saturating_sub(staging.began);
        if election.is_some_and(|election| lasted < election) {
            staging.ready = true;
        } else {
            staging.round_end = last;
            staging.began = clock;
        }
        Ok(())
    }
    fn handle_heartbeat(&mut self, mut message: Message) -> Result<()> {
        if let Some(lost) = self.lost
            && message.commit > self.log.last_index()?
        {
            // The leader counts entries this member lost: it says so.
            return self.send_lost(message.from, lost.index);
        }
        self.log.commit_to(message.commit)?;
        // The leader says no more than the log it knows this member holds.
        if let Some(classic) = message.classic {
            let last = self.log.last_index()?;
            self.learn_classic(classic.min(last))?;
        }
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
        let index = proto::snapshot_index(&snapshot);
        let mut answer = proto::message(message.from, MessageType::MsgAppendResponse);
        answer.index = if self.restore(snapshot)? {
            self.log.last_index()?
        } else {
            self.log.committed()
        };
        // What the snapshot holds the log holds now, if it was not behind it.
        if let Some(classic) = message.classic {
            let held = index.min(self.log.committed());
            self.learn_classic(classic.min(held))?;
        }
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
        // A snapshot that does not name this member is taken all the same: a server processes
        // what a leader of its term sends without consulting its configuration (Ongaro's thesis
        // §4.1). A member a change in the log adds is counted from that entry on, and the snapshot
        // that seeds it may be older than the entry (`docs/raft.md` §3.4).
        if self.pending_request_snapshot == 0 && self.log.match_term(index, term) {
            // The log holds what the snapshot holds.
            self.log.commit_to(index)?;
            return Ok(false);
        }
        self.log.restore(snapshot)?;
        // What was kept ahead of the old log's end follows a log no longer
        // held.
        self.early.clear();
        let last = self.log.last_index()?;
        self.tracker = Tracker::new(
            configuration,
            last,
            self.config.max_inflight_msgs,
            self.config.max_inflight_bytes,
            self.config.limits.members,
        )?;
        self.tracker.set_page(self.config.max_size_per_msg);
        // The snapshot's configuration is its log's through it, applied there.
        self.conf_before = self.tracker.configuration().try_clone()?;
        self.applied_conf = self.tracker.configuration().try_clone()?;
        self.conf_before_at = index;
        self.conf_newest_at = index;
        self.configuration_changed()?;
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
    /// The changes in `[from, last]` take effect: an entry that replaced the newest change took the
    /// configuration back to the one before it, and each change the entries state is folded on in
    /// order. The entries are read a page at a time; a leader reads what it just wrote, a follower
    /// what it just took.
    fn refresh_configuration(&mut self, from: u64) -> Result<()> {
        let mut changed = false;
        if from <= self.conf_newest_at && self.conf_newest_at > self.conf_before_at {
            if from <= self.conf_before_at {
                return Err(Error::Invariant(
                    "an append that replaces a committed change",
                ));
            }
            // Only the newest change can be replaced: the one before is committed (the fields'
            // note). What replaces it is folded on below.
            let before = self.conf_before.try_clone()?;
            let last = self.log.last_index()?;
            self.tracker.apply(before, &[], last)?;
            self.conf_newest_at = self.conf_before_at;
            changed = true;
        }
        let last = self.log.last_index()?;
        let low = from.max(self.log.first_index()?);
        let mut fold = Fold {
            configuration: None,
            before: None,
            before_at: self.conf_before_at,
            newest_at: self.conf_newest_at,
            renewed: Vec::new(),
            error: None,
        };
        if low <= last {
            // Read where the entries are, none copied: the walk stops only at an error.
            let start = self.tracker.configuration();
            self.log.any_entry(low, last.saturating_add(1), |entry| {
                fold.entry(start, entry).is_err_and(|error| {
                    fold.error = Some(error);
                    true
                })
            })?;
        }
        if let Some(error) = fold.error {
            return Err(error);
        }
        if let Some(before) = fold.before {
            self.conf_before = before;
        }
        self.conf_before_at = fold.before_at;
        self.conf_newest_at = fold.newest_at;
        let (configuration, renewed) = (fold.configuration, fold.renewed);
        if let Some(configuration) = configuration {
            self.tracker.apply(configuration, &renewed, last)?;
            changed = true;
        }
        if changed {
            if self.state == StateRole::Leader {
                self.note_term_change()?;
            }
            self.configuration_changed()?;
        }
        Ok(())
    }
    /// The configuration the member counts by changed: what follows from it.
    fn configuration_changed(&mut self) -> Result<()> {
        self.forget_unnamed();
        // Only a learner is caught up: one promoted, or no member any more, is
        // staged no more.
        let Self {
            stagings, tracker, ..
        } = self;
        let configuration = tracker.configuration();
        stagings.retain(|member| configuration.contains(member) && !configuration.votes(member));
        if self.state != StateRole::Leader {
            return Ok(());
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
        Ok(())
    }
    /// A leader the configuration names no voter any more, now that it is committed and applied:
    /// removed, or a learner now. The group is another's to lead: the voter that holds the whole
    /// log is asked to campaign at once, so the group does not wait out an election timeout, and
    /// this member follows.
    fn hand_leadership_on(&mut self) -> Result<()> {
        {
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
        }
        Ok(())
    }
    /// A committed change is applied: the configuration it makes, which is what the owner is
    /// told. The member counts by the newest change its log holds from the moment it holds it
    /// (`Raft::refresh_configuration`); applying changes nothing it counts by.
    pub fn apply_conf_change(&mut self, plan: &Plan) -> Result<ConfState> {
        let changed = plan.apply(&self.applied_conf)?;
        let stated = changed.configuration.to_conf_state()?;
        self.applied_conf = changed.configuration;
        Ok(stated)
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

/// The changes an append's entries state, folded in order onto the configuration before them
/// (`Raft::refresh_configuration`).
struct Fold {
    /// The configuration the entries folded so far make; none before the first change.
    configuration: Option<Configuration>,
    /// The configuration before the newest change folded, made at `before_at`.
    before: Option<Configuration>,
    before_at: u64,
    newest_at: u64,
    /// The members a change removed and added again, once each.
    renewed: Vec<NodeId>,
    error: Option<Error>,
}

impl Fold {
    fn entry(&mut self, start: &Configuration, entry: &Entry) -> Result<()> {
        let Some(plan) = Plan::of_entry(entry)? else {
            return Ok(());
        };
        let current = match self.configuration.take() {
            Some(current) => current,
            None => start.try_clone()?,
        };
        let next = match plan.apply(&current) {
            Ok(next) => next,
            // A change the configuration before it cannot take changes nothing, at every member
            // alike, as its owner's apply is refused (`Raft::apply_conf_change`).
            Err(Error::Configuration(_)) => {
                self.configuration = Some(current);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        for member in next.renewed {
            if !self.renewed.contains(&member) {
                self.renewed.try_reserve(1).map_err(|_| Error::Memory)?;
                self.renewed.push(member);
            }
        }
        self.before = Some(current);
        self.before_at = self.newest_at;
        self.newest_at = entry.index;
        self.configuration = Some(next.configuration);
        Ok(())
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

#[cfg(test)]
mod outgoing {
    use super::{Outgoing, queue};
    use crate::proto::Message;

    fn filled(out: &mut Outgoing, count: usize) {
        for _ in 0..count {
            queue(out, Message::default(), None).unwrap();
        }
    }

    /// An owner that takes `Ready`s ahead of their writes gives back each write's queue only once
    /// the write is durable. The member keeps a spare for each `Ready` that may be out, so each
    /// queue taken while writes are out starts with the room a burst grew rather than from
    /// nothing: with one spare, as before, the second and third take after the queues came back
    /// started from nothing (mantle's range group on hyper-durable, three writes out, grew its
    /// followers' queues from four slots 155 times in 6,000 entries).
    #[test]
    fn a_member_keeps_a_spare_queue_for_each_ready_in_flight() {
        for (keep, roomy) in [(1, 1), (3, 3)] {
            let mut out = Outgoing::default();
            let mut away = Vec::new();
            for _ in 0..3 {
                filled(&mut out, 8);
                away.push(out.take());
            }
            filled(&mut out, 1);
            for emptied in away {
                out.recycle(emptied, 1 << 16, keep);
            }
            assert_eq!(out.spares.len(), keep);
            let mut started_roomy = 0;
            for _ in 0..3 {
                drop(out.take());
                if out.msgs.capacity() >= 8 {
                    started_roomy += 1;
                }
                filled(&mut out, 1);
            }
            assert_eq!(started_roomy, roomy, "keeping {keep}");
            out.check().unwrap();
        }
    }

    /// Past what it keeps, a queue with more room replaces the spare with the least, the spares
    /// stay ordered by room, and a queue of more than the most that may wait is dropped.
    #[test]
    fn spares_past_the_bound_keep_the_most_room() {
        let mut out = Outgoing::default();
        filled(&mut out, 1);
        for room in [8usize, 4, 16, 32] {
            out.recycle(Vec::with_capacity(room), 1 << 16, 2);
        }
        let rooms: Vec<usize> = out.spares.iter().map(Vec::capacity).collect();
        assert_eq!(rooms, [16, 32]);
        out.recycle(Vec::with_capacity(1 << 17), 1 << 16, 2);
        let rooms: Vec<usize> = out.spares.iter().map(Vec::capacity).collect();
        assert_eq!(rooms, [16, 32]);
        // An empty queue takes the most room given back; what it had becomes a spare.
        drop(out.take());
        assert_eq!(out.msgs.capacity(), 32);
        out.recycle(Vec::with_capacity(64), 1 << 16, 2);
        assert_eq!(out.msgs.capacity(), 64);
        let rooms: Vec<usize> = out.spares.iter().map(Vec::capacity).collect();
        assert_eq!(rooms, [16, 32]);
    }
}
