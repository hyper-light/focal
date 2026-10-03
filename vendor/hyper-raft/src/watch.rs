//! Elections started by suspicion (timing step L-2, `docs/timing.md` §2.1–§2.3): what a member
//! keeps when its owner's failure detectors, not a per-group timer, say when a leader is gone.
//!
//! The owner runs one detector per ordered pair of nodes (`docs/timing.md` §2.1) and tells each
//! group's member what its detectors believe of the group's other members: suspected
//! ([`crate::RawNode::suspect`]) or trusted again ([`crate::RawNode::trust`]). A member is trusted
//! until its owner says otherwise. From that alone the member decides:
//!
//! - **A follower campaigns** when it trusts no leader: the leader it knows is suspected, or it
//!   knows none (it just opened, its campaign was lost, a new term began without one). It waits a
//!   delay drawn uniformly from `[0, W)` first, `W` the span hyper-timing's election law chose for
//!   this group ([`Timing::span`]; `hyper_timing::election_delay`), so that the followers that
//!   suspected together do not campaign at once (Ongaro, dissertation §9.2). A suspicion withdrawn
//!   before the delay ends cancels the campaign.
//! - **A candidate whose campaign is unresolved** campaigns again a vote round later
//!   ([`Timing::round`]) and a new draw; so does a voter that granted its vote and heard no leader
//!   (Raft Figure 2: granting a vote resets the election timer).
//! - **A member campaigns only while it and the members it trusts are a quorum** of each half of
//!   its configuration: a campaign that cannot win is not run.
//! - **A leader steps down** once its detectors suspect so many voters that it and those it trusts
//!   are no quorum of either half (check-quorum from the detectors, Raft §6.2).
//! - **A leader beats only while its group has work in flight** (`Raft::active`): a member behind
//!   it, a commit a member has not said it holds, a read waiting, a transfer under way. Its beat is
//!   a heartbeat round, sent once a round tail ([`Timing::round`], the retransmission timeout RFC
//!   6298 §2 and RFC 9002 §6.2.1 compute: mean plus four deviations over the granularity) after the
//!   last, which recovers what was lost. A group with nothing in flight is sent nothing and wakes
//!   for nothing: no ticks (CockroachDB's quiescence; TiKV's hibernated regions).
//! - **Pre-vote keeps its role**: a member that trusts its leader neither grants a pre-vote nor
//!   moves its term for a vote request (Ongaro §9.6: no grant while a leader is heard; here, while
//!   it is trusted). A leader asked for a vote by a member of its group tells it who leads, by a
//!   heartbeat: such a member knows no leader, as one that restarted while its group was idle
//!   does, and nothing else would tell it.
//! - **A leader that trusts a member again** sends it a heartbeat, so a member that was cut off
//!   while the group was idle catches up (CockroachDB wakes a quiesced range when a node becomes
//!   live again).
//!
//! Time is the owner's monotonic clock in nanoseconds, given at [`crate::RawNode::wake`], which the
//! owner calls after each call it makes and at [`crate::RawNode::deadline`]. What a call arms is
//! timed at the next wake: the core never reads a clock.
use std::time::Duration;

use crate::{MAX_MEMBERS, NodeId};

/// A protocol fact, not a tunable: two, the vote rounds a leadership transfer takes at most from
/// the leader's word to the new leader's first append reaching it: the order to campaign reaching
/// the transferee and the new leader's append coming back (one round between them), and the
/// transferee's vote round (Raft dissertation §3.10: a transfer that does not finish within an
/// election is given up). A round is [`Timing::round`].
pub const TRANSFER_ROUNDS: u64 = 2;

/// What the owner's measurements give a group's elections (`docs/timing.md` §2.3), from
/// hyper-timing's election law over the measured paths to the group's voters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timing {
    /// The span `W` the delay from a suspicion to a campaign is drawn over: the one that minimizes
    /// the expected time to a leader (`hyper_timing::Ballot::span`).
    pub span: Duration,
    /// One round to the group's slowest voter and back with a voter's flush, at its tail
    /// (`hyper_timing::Ballot::broadcast_tail`): how long a vote round is given before a candidate
    /// draws again, and how often a leader with work in flight beats.
    pub round: Duration,
}

impl Timing {
    /// The timing hyper-timing's law gives: the span it chose on `ballot`, and the ballot's round
    /// tail.
    pub fn of(ballot: &hyper_timing::Ballot, span: &hyper_timing::Span) -> Self {
        Self {
            span: span.span,
            round: ballot.broadcast_tail,
        }
    }
}

/// A timer the member runs on the owner's clock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Arm {
    /// Not running.
    #[default]
    Off,
    /// To be timed at the next wake: from then, after a round when `round`.
    Unset { round: bool },
    /// Due at this time, nanoseconds on the owner's clock.
    At(u64),
    /// Due, and held until every committed change of the configuration is applied: a member does
    /// not campaign on a configuration it has not applied.
    Apply,
}

/// What a member that elects by suspicion keeps (the module's documentation).
#[derive(Clone, Debug, Default)]
pub(crate) struct Watch {
    /// The members the owner's detectors suspect, in order; at most [`MAX_MEMBERS`].
    suspected: Vec<NodeId>,
    /// The span and the round tail, nanoseconds, once the owner gave them.
    pub(crate) timing: Option<(u64, u64)>,
    /// When this member campaigns.
    pub(crate) campaign: Arm,
    /// When this leader beats next.
    pub(crate) beat: Arm,
    /// When this leader gives up the transfer under way.
    pub(crate) transfer: Arm,
    /// The delays this member drew since it opened: the next draw's index. Every arming of the
    /// campaign draws anew, so a member's delays are independent across elections, as Raft's
    /// randomized timeout is drawn anew at every reset (§5.2, §9.3) and as the law's split
    /// probability takes them (`hyper_timing::election_span`). A draw kept until it fires is not:
    /// a member whose delay never fired keeps a long one while those whose delays fired draw
    /// again, so the delays the next election runs on lean long, and its first round splits more
    /// often than the law says. Past `u32::MAX` it wraps, repeating this member's own sequence,
    /// which nothing else is drawn from.
    pub(crate) draws: u32,
    /// The hand-overs this member made since it opened: whose turn, among the heirs that hold as
    /// much, the next one names.
    pub(crate) handovers: u32,
    /// The last term this member led: while it is the member's term and the
    /// member leads no more, its followers may trust it still, and it
    /// hands over.
    pub(crate) led: u64,
    /// The owner holds this member's campaigns: its log may lack what it acknowledged, or it is
    /// stalled for room (`docs/durable.md` §5, §8).
    pub(crate) held: bool,
}

/// `duration` in nanoseconds, saturating at `u64::MAX` (584 years).
pub(crate) fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl Watch {
    /// Whether the owner's detectors suspect `member`.
    pub(crate) fn suspects(&self, member: NodeId) -> bool {
        self.suspected.binary_search(&member).is_ok()
    }
    /// The detectors suspect `member`. False when they already did. Refused past
    /// [`MAX_MEMBERS`] members, the most a configuration names.
    pub(crate) fn suspect(&mut self, member: NodeId) -> crate::Result<bool> {
        match self.suspected.binary_search(&member) {
            Ok(_) => Ok(false),
            Err(position) => {
                if self.suspected.len() >= MAX_MEMBERS {
                    return Err(crate::Error::Capacity("members suspected"));
                }
                self.suspected
                    .try_reserve(1)
                    .map_err(|_| crate::Error::Capacity("members suspected"))?;
                self.suspected.insert(position, member);
                Ok(true)
            }
        }
    }
    /// The detectors trust `member` again. False when they did already.
    pub(crate) fn trust(&mut self, member: NodeId) -> bool {
        match self.suspected.binary_search(&member) {
            Ok(position) => {
                self.suspected.remove(position);
                true
            }
            Err(_) => false,
        }
    }
    /// Forgets what the detectors said of every member `named` is false for: one the
    /// configuration no longer names.
    pub(crate) fn forget_unnamed(&mut self, named: impl Fn(NodeId) -> bool) {
        self.suspected.retain(|member| named(*member));
    }
    /// The members suspected, in order.
    pub(crate) fn suspected(&self) -> &[NodeId] {
        &self.suspected
    }
    /// The round tail in nanoseconds, once given.
    pub(crate) fn round(&self) -> Option<u64> {
        self.timing.map(|(_, round)| round)
    }
    /// The delay before `local`'s next campaign, drawn anew over the span, once given.
    pub(crate) fn draw(&mut self, local: u64) -> Option<u64> {
        let (span, _) = self.timing?;
        let delay = nanos(hyper_timing::election_delay(
            Duration::from_nanos(span),
            local,
            self.draws,
        ));
        self.draws = self.draws.wrapping_add(1);
        Some(delay)
    }
    /// When an armed timer is due, if it is timed.
    pub(crate) fn due(arm: Arm) -> Option<u64> {
        match arm {
            Arm::At(at) => Some(at),
            Arm::Off | Arm::Unset { .. } | Arm::Apply => None,
        }
    }
    /// The bytes held beyond the member's own.
    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(
            self.suspected
                .capacity()
                .saturating_mul(std::mem::size_of::<NodeId>()),
        )
    }
}
