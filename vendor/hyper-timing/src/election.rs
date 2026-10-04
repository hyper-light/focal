//! Election timing from the failure detector and the split-vote span (`docs/timing.md` §2.1–§2.3),
//! counted in the owner's periods, with slates' timer, priority and window budget (slates
//! `crates/cluster/src/timing.rs` at `c4e2c52`, §4.8 "Derived constants").
//!
//! One design, three measurements:
//! - **The base** is when the follower's detector of the leader's node suspects it: NFD-E trusts the
//!   leader until the next heartbeat's freshness point, `η + α` past the latest heartbeat for the
//!   detector the link runs ([`crate::Configuration::current`]). A follower silent for the base has
//!   lost its leader, as its detector says, and a leader judges its quorum on the same cadence
//!   (check-quorum). The crash is detected within the detector's bound `E(D) + α + η`.
//! - **The span** `W` is the one that minimizes the expected time to a leader once the voters that
//!   suspected together start campaigning, Ongaro's split-vote probability (dissertation §9.2–§9.3)
//!   on the one-way latency and vote round the voters' paths measure ([`Ballot`]). Its expected
//!   election `T_E` is what [`crate::Costs::election`] charges each detection, so the detector's
//!   `η` and `α` are chosen knowing what an election costs here, and the election waits only as
//!   long as that choice assumed.
//! - **The granularity** under the tails is the owner's measured timer lateness, passed in.
//!
//! The broadcast includes the time a voter takes to make its vote or entry durable before it
//! answers (mantle audit §11.7). A path measured by a probe that touches no disk does not hold it,
//! so the caller passes the measured mean flush ([`crate::Flushes`]).
//!
//! The timer ([`ElectionTimer`]) counts periods, not wall time. A node whose own periods are
//! starved waits longer instead of campaigning on its own slowness, which is the Lifeguard direction
//! (slates `docs/bugs/2026-09-13-swim-fixed-probe-deadline-kills-a-starved-live-peer.md`). A core
//! that campaigns on its detector's suspicion instead (L-2) draws the same span in time
//! ([`ElectionTiming::delay`]), and a detector fed kernel receive stamps does not blame a peer for
//! its own late wake (`docs/timing.md` §2.4).
//!
//! The derivations take any [`PathEstimate`], so the estimator that feeds them is chosen by
//! measurement (mantle note 32 §3.7), not by this module.

use std::time::Duration;

use crate::qos::{Detector, Span, election_span};
use crate::{ExchangeRtt, PathRtt};

/// A protocol fact, not a tunable: two, the round trips a lost batch takes to repair when the
/// leader sends batches ahead: the follower's refusal of the batch after the lost one reaches the
/// leader, and the resend reaches the follower (slates `docs/wip/research/consensus-enhancements.md`
/// §3.5). Raft's append consistency check (Ongaro and Ousterhout 2014, §5.3) makes the follower
/// refuse rather than buffer, so no shorter repair exists, and one refusal names the gap, so none
/// longer is needed.
pub const REPAIR_ROUND_TRIPS: u64 = 2;

/// What a timing law reads from a measured path. A path with no sample contributes nothing to a
/// derivation: never an initial guess such as RFC 9002's 333 ms.
pub trait PathEstimate {
    /// How many round trips have fed the estimate.
    fn samples(&self) -> u64;
    /// The central round trip, in nanoseconds; zero before any sample.
    fn smoothed_ns(&self) -> u64;
    /// The mean round trip, in nanoseconds; zero before any sample. An expected time, the
    /// split-vote span's, is a sum of means.
    fn mean_ns(&self) -> u64;
    /// The bound on the path's round-trip tail, in nanoseconds, with the owner's measured timer
    /// granularity under its variation term, or `None` before any sample.
    fn tail_ns(&self, granularity_ns: u64) -> Option<u64>;
    /// The tail's width above the central round trip, or `None` before any sample.
    fn spread_ns(&self, granularity_ns: u64) -> Option<u64> {
        self.tail_ns(granularity_ns)
            .map(|tail| tail.saturating_sub(self.smoothed_ns()))
    }
}

impl PathEstimate for PathRtt {
    fn samples(&self) -> u64 {
        PathRtt::samples(self)
    }
    fn smoothed_ns(&self) -> u64 {
        PathRtt::smoothed_ns(self)
    }
    fn mean_ns(&self) -> u64 {
        PathRtt::mean_ns(self)
    }
    fn tail_ns(&self, granularity_ns: u64) -> Option<u64> {
        PathRtt::tail_ns(self, granularity_ns)
    }
}

impl PathEstimate for ExchangeRtt {
    fn samples(&self) -> u64 {
        ExchangeRtt::samples(self)
    }
    fn smoothed_ns(&self) -> u64 {
        ExchangeRtt::smoothed_ns(self)
    }
    /// RFC 9002's `smoothed_rtt`, an exponentially weighted mean.
    fn mean_ns(&self) -> u64 {
        ExchangeRtt::smoothed_ns(self)
    }
    fn tail_ns(&self, granularity_ns: u64) -> Option<u64> {
        ExchangeRtt::tail_ns(self, granularity_ns)
    }
}

/// A voter's election priority (slates `consensus-enhancements.md` §3.4).
///
/// It is the round trip the voter would commit in as leader, its quorum round trip (the
/// `⌊n/2⌋`-th smallest measured round trip to the other voters, since a leader commits once a
/// majority including itself holds an entry), and the spread of the path that sets it, both in
/// nanoseconds. A zero round trip is unknown: it never outranks and is never outranked, so an
/// unmeasured group behaves as one without priorities.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ElectionPriority {
    /// The quorum round trip, nanoseconds; zero when unknown.
    pub quorum_ns: u64,
    /// The spread of the path that sets it, nanoseconds.
    pub spread_ns: u64,
}

impl ElectionPriority {
    /// Whether this priority is known and commits distinguishably faster than `other`: its
    /// interval, the round trip plus its spread, lies wholly below `other`'s round trip less its
    /// spread. Overlapping intervals tie, so measurement noise never ranks one voter above another.
    pub fn outranks(&self, other: &Self) -> bool {
        self.quorum_ns > 0
            && other.quorum_ns > 0
            && self.quorum_ns.saturating_add(self.spread_ns)
                < other.quorum_ns.saturating_sub(other.spread_ns)
    }
}

/// The `position`-th smallest of `keys`, from zero, ties broken by their order. The rank is
/// counted rather than sorted, so the call allocates nothing: a key's position is the number of
/// keys ordered before it, and a count stops once it passes `position`. That is at most quadratic
/// in a group's voters, a handful.
#[inline]
fn nth_smallest<K: Ord + Copy>(
    keys: impl Iterator<Item = K> + Clone,
    position: usize,
) -> Option<K> {
    for (index, key) in keys.clone().enumerate() {
        let mut rank = 0usize;
        let mut past = false;
        for (other, other_key) in keys.clone().enumerate() {
            if other_key < key || (other_key == key && other < index) {
                rank = rank.saturating_add(1);
                if rank > position {
                    past = true;
                    break;
                }
            }
        }
        if !past && rank == position {
            return Some(key);
        }
    }
    None
}

/// A voter's election priority from its measured paths to the other voters of a group of
/// `voters`: the `⌊voters/2⌋`-th smallest central round trip among `paths`, with that path's
/// spread over the owner's measured granularity. Unknown while a sole voter, or while fewer
/// measured paths than that exist. It allocates nothing.
///
/// The paths are ranked by their spread without the floor and the floor is applied to the one
/// chosen: the floored spread, `max(4·deviation, G)`, never falls as the unfloored one rises, so
/// the two rankings choose paths of the same round trip and floored spread.
pub fn quorum_priority<'a, P: PathEstimate + 'a>(
    paths: impl IntoIterator<Item = Option<&'a P>, IntoIter: Clone>,
    voters: usize,
    granularity_ns: u64,
) -> ElectionPriority {
    let measured = paths
        .into_iter()
        .flatten()
        .filter_map(|path| path.spread_ns(0).map(|spread| (path.smoothed_ns(), spread)));
    (voters / 2)
        .checked_sub(1)
        .and_then(|position| nth_smallest(measured, position))
        .map_or_else(ElectionPriority::default, |(quorum_ns, spread_ns)| {
            ElectionPriority {
                quorum_ns,
                spread_ns: spread_ns.max(granularity_ns),
            }
        })
}

/// A group's election as its voters' paths measure it: the inputs of the split-vote span
/// (`docs/timing.md` §2.3) and the broadcast the window budget repairs over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ballot {
    /// The group's voters, `n`.
    pub voters: u32,
    /// The voters the span is chosen for, `s`: all but the leader whose crash the detector exists
    /// to find, where they are still a majority; all of them otherwise (a group of two elects only
    /// with both).
    pub available: u32,
    /// The latency `l` of Ongaro's split: from a candidate's turn to campaign to its request
    /// reaching the voter it reaches last. Half the slowest measured path's mean round trip (a round
    /// trip is what one clock can measure, and half of it is a symmetric path's one-way delay, the
    /// half NTP's offset takes, RFC 5905 §8, `θ = ½[(T2 − T1) + (T3 − T4)]`), plus the mean flush:
    /// a candidate's request leaves only once its own term and vote are durable (Raft Figure 2's
    /// persistent state; `docs/durable.md` I1), and another member that turns to campaign within
    /// that time has voted for itself before the request reaches it. Found by hyper-durable's
    /// processes (`docs/timing.md` §2.9): with the flush left out, two voters whose flush was some
    /// two orders of magnitude past the path's one-way delay split round after round, the span a sliver of the window
    /// their campaigns collided in.
    pub latency: Duration,
    /// The vote round: the candidate's quorum, the `⌊n/2⌋`-th smallest mean round trip, since a
    /// candidate wins with `⌊n/2⌋` votes beside its own; plus the mean flush a voter makes its vote
    /// durable in before it answers (Raft's `votedFor` is persistent state, Figure 2).
    pub round: Duration,
    /// The slowest measured path's tail over the granularity, plus the mean flush: what the window
    /// budget repairs over.
    pub broadcast_tail: Duration,
    /// The round trips measured across the paths: the witness that the ballot is measured.
    pub samples: u64,
}

impl Ballot {
    /// The ballot of a group of `voters` from this voter's measured `paths` to the others, with
    /// the mean `durable` flush of a vote or entry and the owner's measured `granularity`. `None`
    /// for a sole voter, which never campaigns against anyone, and while fewer paths are measured
    /// than a candidate's quorum needs: no span is chosen for a latency nobody measured.
    pub fn measure<'a, P: PathEstimate + 'a>(
        paths: impl IntoIterator<Item = &'a P, IntoIter: Clone>,
        voters: usize,
        durable: Duration,
        granularity: Duration,
    ) -> Option<Self> {
        let voters = u32::try_from(voters).ok().filter(|count| *count >= 2)?;
        let granularity_ns = nanos(granularity);
        let measured = paths.into_iter().filter(|path| path.samples() > 0);
        let (mut slowest, mut tail, mut samples) = (0u64, 0u64, 0u64);
        for path in measured.clone() {
            slowest = slowest.max(path.mean_ns());
            tail = tail.max(path.tail_ns(granularity_ns).unwrap_or(0));
            samples = samples.saturating_add(path.samples());
        }
        let position = usize::try_from(voters / 2).ok()?.checked_sub(1)?;
        let quorum = nth_smallest(measured.map(|path| path.mean_ns()), position)?;
        let durable_ns = nanos(durable);
        let others = voters.saturating_sub(1);
        Some(Self {
            voters,
            available: if others > voters / 2 { others } else { voters },
            latency: Duration::from_nanos((slowest / 2).saturating_add(durable_ns)),
            round: Duration::from_nanos(quorum.saturating_add(durable_ns)),
            broadcast_tail: Duration::from_nanos(tail.saturating_add(durable_ns)),
            samples,
        })
    }

    /// The span that minimizes the expected time to a leader on this ballot, searched to within the
    /// owner's measured `granularity`, finer than which no wait can be told apart
    /// ([`election_span`]). `None` where no election can succeed.
    ///
    /// Its expected election is the cost the leader's link's detector is configured for, so it is
    /// computed when that detector is ([`crate::LinkEstimator::reconfigure_due`]): between
    /// configurations a new span would change nothing the detector does. A search costs a few
    /// hundred nanoseconds; the timing derived from a held span is a few nanoseconds a period
    /// (`docs/benchmarks.md`, "hyper-timing").
    pub fn span(&self, granularity: Duration) -> Option<Span> {
        election_span(
            self.voters,
            self.available,
            self.latency,
            self.round,
            granularity,
        )
    }
}

/// A group's election timing, from the detector of the leader's link and the group's span, in the
/// owner's periods.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ElectionTiming {
    /// The base election timeout in periods: a follower campaigns after this many periods without
    /// leader contact, plus its jitter, and a leader judges its quorum every this many.
    pub base_periods: u32,
    /// The randomization span in periods: a node's timeout is `base + jitter`, `jitter ∈ [0,
    /// span)`. One where a period is at least the span: such an owner cannot resolve the span in
    /// its periods, and only the phases of the owners' periods spread their campaigns.
    pub span_periods: u32,
    /// The base: the detector's freshness horizon past the leader's latest heartbeat, `η + α`.
    pub base: Duration,
    /// The span `W` that minimizes the expected time to a leader.
    pub span: Duration,
    /// The detector's bound on detecting a crash, `E(D) + α + η`.
    pub detection: Duration,
    /// The expected time from a suspicion to a leader with the span, `T_E(W)`.
    pub election: Duration,
    /// The broadcast tail the window budget repairs over ([`Ballot::broadcast_tail`]).
    pub broadcast_tail: Duration,
    /// The round trips measured across the voter paths that fed the ballot.
    pub samples: u64,
}

impl ElectionTiming {
    /// The timing for an owner whose period is `period`, from the `detector` the follower runs on
    /// its link to the leader's node (the margin in force at the link's interval,
    /// [`crate::Configuration::current`]), the group's `span` and its `ballot`. The base and the
    /// span are rounded up to whole periods, so the base never ends before the detector would
    /// suspect. The detector's [`crate::Costs::election`] is to be `span.election`: the election it
    /// planned for is the one this timing runs.
    pub fn derive(period: Duration, detector: &Detector, span: &Span, ballot: &Ballot) -> Self {
        let period_ns = nanos(period);
        let base = detector.interval.saturating_add(detector.margin);
        Self {
            base_periods: periods_of(nanos(base), period_ns),
            span_periods: periods_of(nanos(span.span), period_ns),
            base,
            span: span.span,
            detection: detector.detection,
            election: span.election,
            broadcast_tail: ballot.broadcast_tail,
            samples: ballot.samples,
        }
    }

    /// The window a leader keeps ahead, in bytes: one `batch_bytes` for each period of
    /// `period_ns` a lost batch takes to repair on the slowest measured voter path
    /// ([`REPAIR_ROUND_TRIPS`] round trips), and at least one.
    ///
    /// It is one batch on a LAN, where an acknowledgement is back within the period. slates
    /// measured four across five Azure regions, where four cut the commit tail under 1 % loss from
    /// 458 to 321 ms at 2,000 proposals a second (slates `crates/cluster/tests/pipelining.rs`,
    /// 2026-09-29).
    pub fn window_budget(&self, period_ns: u64, batch_bytes: usize) -> usize {
        let repair = nanos(self.broadcast_tail).saturating_mul(REPAIR_ROUND_TRIPS);
        let batches = usize::try_from(periods_of(repair, period_ns)).unwrap_or(usize::MAX);
        batch_bytes.saturating_mul(batches)
    }

    /// This node's own timeout in periods for its `attempt`-th campaign: `base + (draw mod span)`.
    ///
    /// The draw is a splitmix64 mix of the id and the attempt. It is deterministic, so a
    /// simulation reproduces from its seed, yet independent across nodes and attempts, as Raft's
    /// randomized timeout is (§5.2, §9.3). A draw of `(local + attempt) mod span` kept two
    /// congruent nodes congruent forever, splitting the vote round after round (slates
    /// `docs/bugs/2026-09-28-correlated-election-jitter-livelocked-a-split-vote.md`).
    pub fn timeout_periods(&self, local: u64, attempt: u32) -> u32 {
        let span = u64::from(self.span_periods.max(1));
        let jitter =
            u32::try_from(draw(local, attempt).checked_rem(span).unwrap_or(0)).unwrap_or(0);
        self.base_periods.saturating_add(jitter)
    }

    /// This node's `attempt`-th delay from a suspicion to a campaign, uniform on `[0, W)` from the
    /// same draw as [`timeout_periods`](Self::timeout_periods) (`docs/timing.md` §2.3): the form
    /// for a core that campaigns on its detector's suspicion and waits in time, to the owner's
    /// granularity, rather than in periods.
    pub fn delay(&self, local: u64, attempt: u32) -> Duration {
        election_delay(self.span, local, attempt)
    }
}

/// `local`'s `attempt`-th delay from a suspicion to a campaign, uniform on `[0, span)`
/// (`docs/timing.md` §2.3): the draw [`ElectionTiming::delay`] makes, for a core that is given the
/// span alone (hyper-raft's elections by suspicion, L-2). The draw is a splitmix64 mix of the id and
/// the attempt, deterministic so a simulation reproduces from its seed, and independent across
/// nodes and attempts as Raft's randomized timeout is (§5.2, §9.3); the scaling takes the high half
/// of the product, so every nanosecond of the span is equally likely to within one part in 2^64.
/// The independence holds across elections only if the caller takes a new attempt at each one:
/// hyper-raft takes one at every arming (`Watch::draws`), since a delay kept until it fires leans
/// the next election's delays long.
pub fn election_delay(span: Duration, local: u64, attempt: u32) -> Duration {
    let scaled = u128::from(nanos(span)).saturating_mul(u128::from(draw(local, attempt)));
    Duration::from_nanos(u64::try_from(scaled >> u64::BITS).unwrap_or(u64::MAX))
}

/// The draw for `local`'s `attempt`-th delay.
fn draw(local: u64, attempt: u32) -> u64 {
    splitmix64(local ^ u64::from(attempt).wrapping_mul(GOLDEN_GAMMA))
}

/// Format: splitmix64's increment, the odd integer nearest 2^64/φ (Steele, Lea and Flood, "Fast
/// splittable pseudorandom number generators", OOPSLA 2014).
const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
/// Format: splitmix64's first finalizer multiplier (Steele, Lea and Flood 2014).
const MIX_ONE: u64 = 0xbf58_476d_1ce4_e5b9;
/// Format: splitmix64's second finalizer multiplier (Steele, Lea and Flood 2014).
const MIX_TWO: u64 = 0x94d0_49bb_1331_11eb;

/// splitmix64's finalizer: a bijective mix whose outputs are statistically independent across
/// nearby inputs.
fn splitmix64(word: u64) -> u64 {
    let mut z = word.wrapping_add(GOLDEN_GAMMA);
    z = (z ^ (z >> 30)).wrapping_mul(MIX_ONE);
    z = (z ^ (z >> 27)).wrapping_mul(MIX_TWO);
    z ^ (z >> 31)
}

/// `span_ns` in whole periods of `period_ns`, rounded up, at least one; saturating.
fn periods_of(span_ns: u64, period_ns: u64) -> u32 {
    let period = period_ns.max(1);
    let periods = span_ns.div_ceil(period).max(1);
    u32::try_from(periods).unwrap_or(u32::MAX)
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What a follower's period asks of its node ([`ElectionTimer::follower_period`]).
#[must_use = "a lapsed leader must be forgotten and a campaign run, or the group elects late"]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FollowerStep {
    /// A leader made contact within the minimum election timeout: keep following.
    Follow,
    /// No leader contact for the minimum election timeout, the base: the node forgets its leader
    /// (thesis §4.2.3), so it grants a candidate's pre-vote, but does not campaign yet.
    LeaderLapsed,
    /// Campaign now.
    Campaign,
}

/// A group's election timer as its owner counts it, one tick per period.
///
/// It holds the follower's age since leader contact (reset at each of its own timeouts) and its
/// silence (not reset there), the last contact value it saw, its jitter rotation, and the timeouts
/// it has yielded to more central voters (slates `consensus-enhancements.md` §3.4).
#[derive(Debug, Default)]
pub struct ElectionTimer {
    idle_periods: u32,
    silent_periods: u32,
    seen_contact: u64,
    attempt: u32,
    yielded: u32,
}

impl ElectionTimer {
    /// A timer at zero age, no contact seen, first attempt.
    pub fn new() -> Self {
        Self::default()
    }

    /// A follower's period.
    ///
    /// `contact` is the group's leader-contact count (Raft Figure 2's two follower timer resets: a
    /// leader's append answered, a vote granted). While it advances, the age and the silence reset,
    /// the yielded timeouts clear, and the node follows.
    ///
    /// Otherwise the timer ages one period. At this node's jittered timeout it rotates the attempt,
    /// resets the age, and campaigns when the node's election `rank` (how many live voters outrank
    /// it) is within the timeouts it has already yielded. Otherwise it yields this timeout and
    /// admits the next rank, so an election waits at most one timeout per live voter that outranks
    /// this one. Short of a campaign, a node silent for the base has lost its leader's lease,
    /// whatever its jitter and rank, so the voter it yields to can win its pre-vote (slates
    /// `docs/bugs/2026-09-29-a-yielding-voter-refused-the-voter-it-yielded-to.md`).
    pub fn follower_period(
        &mut self,
        contact: u64,
        timing: &ElectionTiming,
        local: u64,
        rank: usize,
    ) -> FollowerStep {
        if contact != self.seen_contact {
            self.seen_contact = contact;
            self.idle_periods = 0;
            self.silent_periods = 0;
            self.yielded = 0;
            return FollowerStep::Follow;
        }
        self.idle_periods = self.idle_periods.saturating_add(1);
        self.silent_periods = self.silent_periods.saturating_add(1);
        if self.idle_periods >= timing.timeout_periods(local, self.attempt) {
            self.attempt = self.attempt.saturating_add(1);
            self.idle_periods = 0;
            if u32::try_from(rank).unwrap_or(u32::MAX) <= self.yielded {
                return FollowerStep::Campaign;
            }
            self.yielded = self.yielded.saturating_add(1);
        }
        if self.silent_periods >= timing.base_periods {
            FollowerStep::LeaderLapsed
        } else {
            FollowerStep::Follow
        }
    }

    /// The timeouts this follower has yielded to more central voters since it last heard a leader.
    pub fn yielded(&self) -> u32 {
        self.yielded
    }

    /// A leader's period: ages one period and returns `true` every `base_periods`, when the leader
    /// judges its quorum (Raft §6.2 CheckQuorum on the election-timeout cadence). A leader is its
    /// own contact, so its silence as a follower starts over.
    pub fn leader_period(&mut self, timing: &ElectionTiming) -> bool {
        self.silent_periods = 0;
        self.idle_periods = self.idle_periods.saturating_add(1);
        if self.idle_periods < timing.base_periods.max(1) {
            return false;
        }
        self.idle_periods = 0;
        true
    }

    /// Resets the age and the silence: a sole voter's period, or a role change.
    pub fn reset(&mut self) {
        self.idle_periods = 0;
        self.silent_periods = 0;
    }

    /// Re-baselines the contact after a campaign, so the campaign's own vote and append echoes do
    /// not retrigger a fresh one next period.
    pub fn rebaseline(&mut self, contact: u64) {
        self.seen_contact = contact;
    }

    /// How many campaigns this timer has fired.
    pub fn attempts(&self) -> u32 {
        self.attempt
    }

    /// Periods since the last leader contact, or the last campaign or quorum check.
    pub fn idle_periods(&self) -> u32 {
        self.idle_periods
    }
}

#[cfg(test)]
mod tests {
    //! slates' timer and priority tests (`crates/cluster/src/timing.rs` at `c4e2c52`), on
    //! [`ExchangeRtt`], the RFC 9002 estimator they were written against; the ballot, the span and
    //! the base from a configured detector; and the unified round budget.
    use super::*;
    use crate::{Costs, LinkEstimator, RoundAnchors, RoundBudget, Schedule, configure_arrivals};
    use proptest::prelude::*;

    /// A millisecond in nanoseconds, so the samples read as round times.
    const MS: u64 = 1_000_000;
    /// slates' daemon heartbeat, its owner period: 100 ms.
    const HEARTBEAT: u64 = 100 * MS;
    /// The timer granularity: macOS's 45 µs at a 40 µs wait (`docs/timing.md` §2.4).
    const G: Duration = Duration::from_micros(45);
    /// The loopback round trips slates measured on 2026-09-13: SWIM p99 17 ms, consensus broadcast
    /// p50 11 ms and p99 33 ms.
    const LOOPBACK_SAMPLES_MS: [u64; 4] = [11, 17, 11, 33];
    /// An inter-region path of 80 ms ± 20 ms one way.
    const WAN_SAMPLES_MS: [u64; 6] = [160, 200, 120, 160, 190, 130];

    fn path_of(samples_ms: &[u64]) -> ExchangeRtt {
        let mut path = ExchangeRtt::new();
        for sample in samples_ms {
            path.on_sample(sample * MS);
        }
        path
    }

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    /// The detector the traces configured on macOS at 100 µs (`docs/timing.md` §2.6): a 50 ms
    /// interval and a 65.8 ms margin.
    fn macos_detector() -> Detector {
        Detector {
            interval: ms(50),
            margin: Duration::from_micros(65_800),
            detection: Duration::from_micros(115_900),
            mistake_recurrence: Duration::from_secs(8 * 86_400),
            unavailability: 4.5e-8,
        }
    }

    fn timing_of(period: u64, paths: &[&ExchangeRtt], voters: usize) -> ElectionTiming {
        let ballot = Ballot::measure(paths.iter().copied(), voters, Duration::ZERO, G).unwrap();
        let span = ballot.span(G).unwrap();
        ElectionTiming::derive(
            Duration::from_nanos(period),
            &macos_detector(),
            &span,
            &ballot,
        )
    }

    /// A five-voter group across regions, its timer counted in 1 ms periods.
    fn wan_timing() -> ElectionTiming {
        let paths: Vec<ExchangeRtt> = (0..4).map(|_| path_of(&WAN_SAMPLES_MS)).collect();
        timing_of(MS, &paths.iter().collect::<Vec<_>>(), 5)
    }

    #[test]
    fn a_ballot_is_the_slowest_latency_and_the_quorums_round() {
        let near = path_of(&[10, 10]);
        let middle = path_of(&[40, 40]);
        let far = path_of(&[160, 160]);
        let durable = ms(3);
        let ballot = Ballot::measure([&far, &near, &middle, &near], 5, durable, G).unwrap();
        assert_eq!(ballot.voters, 5);
        assert_eq!(ballot.available, 4, "all but the leader that crashed");
        assert_eq!(
            ballot.latency,
            ms(80) + durable,
            "half the slowest mean round trip, and the candidate's own flush"
        );
        // Five voters: a candidate wins with two votes beside its own, the second-nearest.
        assert_eq!(ballot.round, ms(10) + durable);
        let tail = far.tail_ns(nanos(G)).unwrap();
        assert_eq!(ballot.broadcast_tail, Duration::from_nanos(tail) + durable);
        assert_eq!(ballot.samples, 8);
        // Three voters: one vote beside its own, the nearest; two still up of three.
        let three = Ballot::measure([&middle, &near], 3, durable, G).unwrap();
        assert_eq!((three.available, three.round), (2, ms(10) + durable));
        // Two voters: the survivor alone is no majority, so the span is chosen for both.
        let two = Ballot::measure([&near], 2, durable, G).unwrap();
        assert_eq!(two.available, 2);
        assert!(two.span(G).is_some());
    }

    #[test]
    fn no_ballot_without_the_paths_a_quorum_needs() {
        let near = path_of(&[10]);
        let fresh = ExchangeRtt::new();
        assert_eq!(
            Ballot::measure([&near], 1, Duration::ZERO, G),
            None,
            "a sole voter"
        );
        assert_eq!(
            Ballot::measure::<ExchangeRtt>([], 3, Duration::ZERO, G),
            None
        );
        assert_eq!(
            Ballot::measure([&fresh, &fresh], 3, Duration::ZERO, G),
            None
        );
        // Five voters need two measured paths; one is not a quorum's round.
        assert_eq!(
            Ballot::measure([&near, &fresh, &fresh, &fresh], 5, Duration::ZERO, G),
            None
        );
        assert!(Ballot::measure([&near, &fresh, &near, &fresh], 5, Duration::ZERO, G).is_some());
    }

    /// The span is the split-vote minimum on the ballot's own inputs: what `election_span` gives.
    #[test]
    fn the_span_is_the_minimum_on_the_ballots_inputs() {
        let paths = [path_of(&WAN_SAMPLES_MS), path_of(&LOOPBACK_SAMPLES_MS)];
        let ballot = Ballot::measure(&paths, 3, ms(2), G).unwrap();
        let span = ballot.span(G).unwrap();
        assert_eq!(
            Some(span),
            election_span(3, 2, ballot.latency, ballot.round, G)
        );
        assert!(
            span.span >= ballot.latency,
            "wider than the latency, or every vote splits"
        );
        assert!(span.split < 1.0);
    }

    #[test]
    fn the_base_is_the_detectors_freshness_horizon_in_whole_periods() {
        let detector = macos_detector();
        let paths = [path_of(&LOOPBACK_SAMPLES_MS), path_of(&LOOPBACK_SAMPLES_MS)];
        let ballot = Ballot::measure(&paths, 3, Duration::ZERO, G).unwrap();
        let span = ballot.span(G).unwrap();
        // 115.8 ms in 10 ms periods: twelve; in 100 ms periods: two; in 1 ms periods: 116.
        for (period_ms, base) in [(10, 12), (100, 2), (1, 116)] {
            let timing = ElectionTiming::derive(ms(period_ms), &detector, &span, &ballot);
            assert_eq!(timing.base, Duration::from_micros(115_800));
            assert_eq!(timing.base_periods, base, "{period_ms} ms periods");
            assert_eq!(timing.detection, detector.detection);
            assert_eq!(timing.election, span.election);
        }
        // A period longer than the span cannot resolve it: one period, the owners' phases only.
        let coarse = ElectionTiming::derive(ms(100), &detector, &span, &ballot);
        assert!(span.span < ms(100));
        assert_eq!(coarse.span_periods, 1);
        assert_eq!(coarse.timeout_periods(3, 0), coarse.base_periods);
    }

    proptest! {
        /// The base is never shorter than the detector's horizon and short of it by under a period;
        /// the timeout lies in `[base, base + span)` and the delay in `[0, W)`.
        #[test]
        fn the_timing_rounds_up_and_the_draws_stay_in_range(
            interval_us in 1u64..1_000_000,
            margin_us in 0u64..1_000_000,
            span_us in 1u64..100_000,
            period_us in 1u64..200_000,
            local in any::<u64>(),
            attempt in any::<u32>(),
        ) {
            let detector = Detector {
                interval: Duration::from_micros(interval_us),
                margin: Duration::from_micros(margin_us),
                ..macos_detector()
            };
            let span = Span {
                span: Duration::from_micros(span_us),
                election: Duration::from_micros(span_us),
                split: 0.1,
            };
            let near = path_of(&[1]);
            let ballot = Ballot::measure([&near], 2, Duration::ZERO, G).unwrap();
            let period = Duration::from_micros(period_us);
            let timing = ElectionTiming::derive(period, &detector, &span, &ballot);
            let base = detector.interval + detector.margin;
            let covered = period * timing.base_periods;
            prop_assert!(covered >= base);
            prop_assert!(timing.base_periods == 1 || covered < base + period);
            let timeout = timing.timeout_periods(local, attempt);
            prop_assert!(timeout >= timing.base_periods);
            prop_assert!(timeout < timing.base_periods + timing.span_periods);
            prop_assert!(timing.delay(local, attempt) < span.span);
        }
    }

    /// `x` from `x ^ (x >> shift)`: each pass recovers `shift` more of the high bits.
    fn unshift(word: u64, shift: u32) -> u64 {
        (0..64u32.div_ceil(shift)).fold(word, |x, _| word ^ (x >> shift))
    }

    /// The inverse of an odd multiplier modulo 2^64, by Newton's iteration `x' = x(2 − ax)`, which
    /// doubles the correct low bits each step: an odd number is its own inverse modulo 8, so from
    /// three bits five steps reach 96, past 64.
    fn odd_inverse(odd: u64) -> u64 {
        (0..5).fold(odd, |inverse, _| {
            inverse.wrapping_mul(2u64.wrapping_sub(odd.wrapping_mul(inverse)))
        })
    }

    /// splitmix64 undone, step by step from its last.
    fn unmix(word: u64) -> u64 {
        let z = unshift(word, 31).wrapping_mul(odd_inverse(MIX_TWO));
        let z = unshift(z, 27).wrapping_mul(odd_inverse(MIX_ONE));
        unshift(z, 30).wrapping_sub(GOLDEN_GAMMA)
    }

    /// The draw is uniform over the span exactly as far as its parts are: the finalizer is a
    /// bijection of the 64-bit words (each step is invertible, and inverting it returns every
    /// word), so a uniform input word gives a uniform draw; and the scaling `⌊span·w / 2^64⌋` gives
    /// every nanosecond of the span `⌈(d + 1)2^64/span⌉ − ⌈d·2^64/span⌉` words, which is
    /// `⌊2^64/span⌋` or one more. The delay is always inside the span.
    #[test]
    fn the_draw_is_a_bijection_scaled_without_bias() {
        let mut word = 0x2545_f491_4f6c_dd1du64;
        for _ in 0..100_000 {
            word = word.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            assert_eq!(unmix(splitmix64(word)), word);
        }
        for edge in [0, 1, u64::MAX, 1 << 63, GOLDEN_GAMMA.wrapping_neg()] {
            assert_eq!(unmix(splitmix64(edge)), edge);
        }
        let words = 1u128 << 64;
        for span in [1u128, 2, 3, 7, 1_000, 65_537, 99_991] {
            let least = words / span;
            let ceiling = |d: u128| (d * words).div_ceil(span);
            for d in 0..span {
                let count = ceiling(d + 1) - ceiling(d);
                assert!(
                    count == least || count == least + 1,
                    "span {span}, {d}: {count}"
                );
            }
        }
        let timing = wan_timing();
        for local in 0..20_000u64 {
            let delay = timing.delay(local.wrapping_mul(0x2545_f491_4f6c_dd1d), 0);
            assert!(delay < timing.span);
        }
    }

    /// One design: the timing derived from a link estimator's configured detector, with the
    /// election its configuration was charged, lapses its leader at the period the estimator
    /// suspects it, on a link whose heartbeats arrive with a constant delay.
    #[test]
    fn the_base_lapses_where_the_link_suspects() {
        let interval = ms(50);
        let granularity = ms(1);
        let mut link =
            LinkEstimator::new(interval, granularity, Some(Schedule { seq: 0, at_ns: 0 })).unwrap();
        let delay = 2 * MS;
        let mut seq = 0;
        while seq < 200 {
            link.on_heartbeat(seq, seq * 50 * MS + delay).unwrap();
            seq += 1;
        }
        let paths = [path_of(&[4, 4, 4]), path_of(&[4, 4, 4])];
        let ballot = Ballot::measure(&paths, 3, ms(1), granularity).unwrap();
        let span = ballot.span(granularity).unwrap();
        let costs = Costs {
            election: span.election,
            mtbf: Duration::from_secs(30 * 86_400),
        };
        let configured = link.configure(&costs, granularity, granularity).unwrap();
        let timing = ElectionTiming::derive(granularity, &configured.current, &span, &ballot);
        // The latest heartbeat arrived at `last`; the link trusts its sender until the next one's
        // freshness point, `η + α` later on a link whose delays do not vary.
        let last = (seq - 1) * 50 * MS + delay;
        let until = link.deadline().unwrap();
        assert_eq!(Duration::from_nanos(until - last), timing.base);
        assert_eq!(link.poll(until - 1), None);
        // The period-counting timer, one period a millisecond from the latest contact, lapses at
        // the first period at or past the suspicion.
        let mut timer = ElectionTimer::new();
        let _ = timer.follower_period(1, &timing, 1, 0);
        let lapsed = (1..=10_000u32)
            .find(|_| timer.follower_period(1, &timing, 1, 0) != FollowerStep::Follow)
            .unwrap();
        assert_eq!(lapsed, timing.base_periods);
        let lapsed_at = last + u64::from(lapsed) * MS;
        assert!(lapsed_at >= until && lapsed_at < until + MS);
        assert_eq!(link.poll(until), Some(crate::Event::Suspected));
        // And the configurator was charged the election this timing runs.
        assert_eq!(timing.election, costs.election);
        let best = configure_arrivals(&configured.link, &costs, granularity, granularity).unwrap();
        assert_eq!(best, configured.best);
    }

    #[test]
    fn the_window_holds_a_repairs_worth_of_batches_on_the_slowest_path() {
        const BATCH: usize = 4_367;
        let lan = timing_of(HEARTBEAT, &[&path_of(&LOOPBACK_SAMPLES_MS)], 3);
        assert_eq!(lan.window_budget(HEARTBEAT, BATCH), BATCH);
        let wan = path_of(&WAN_SAMPLES_MS);
        let timing = timing_of(HEARTBEAT, &[&wan], 3);
        let tail = wan.tail_ns(nanos(G)).unwrap();
        let batches = usize::try_from((2 * tail).div_ceil(HEARTBEAT)).unwrap();
        assert_eq!(timing.window_budget(HEARTBEAT, BATCH), BATCH * batches);
        assert!(batches >= 3);
    }

    /// One stall of three answers moves the median path's priority nowhere and the smoothed one's
    /// by seconds: the reason the priority orders voters by the median (`docs/timing.md` §2.6,
    /// item 7).
    #[test]
    fn a_stall_reorders_no_voter_by_the_median_and_does_by_the_smoothed_estimate() {
        let correlation = ms(150);
        let probe = ms(50);
        let mut median = PathRtt::new(correlation, probe).unwrap();
        assert_eq!(
            median.window(),
            7,
            "three late in a stall, seven to outvote them"
        );
        let mut smoothed = ExchangeRtt::new();
        for _ in 0..median.window() {
            median.on_sample(5 * MS);
            smoothed.on_sample(5 * MS);
        }
        let g = nanos(G);
        let before = quorum_priority([Some(&median), Some(&median)], 3, g);
        for late in [2_900, 1_400, 800] {
            median.on_sample(late * MS);
            smoothed.on_sample(late * MS);
        }
        assert_eq!(
            quorum_priority([Some(&median), Some(&median)], 3, g),
            before
        );
        let moved = quorum_priority([Some(&smoothed), Some(&smoothed)], 3, g);
        assert!(moved.quorum_ns > 500 * MS, "{moved:?}");
    }

    fn ages_without_firing(
        timer: &mut ElectionTimer,
        contact: u64,
        timing: &ElectionTiming,
        local: u64,
        periods: u32,
    ) -> bool {
        (0..periods)
            .all(|_| timer.follower_period(contact, timing, local, 0) != FollowerStep::Campaign)
    }

    #[test]
    fn a_follower_campaigns_after_its_jittered_timeout() {
        let timing = wan_timing();
        let local = 3;
        let timeout = timing.timeout_periods(local, 0);
        assert!(
            timeout >= timing.base_periods && timeout < timing.base_periods + timing.span_periods
        );
        let mut timer = ElectionTimer::new();
        assert!(ages_without_firing(
            &mut timer,
            0,
            &timing,
            local,
            timeout - 1
        ));
        assert_eq!(
            timer.follower_period(0, &timing, local, 0),
            FollowerStep::Campaign
        );
        assert_eq!(timer.attempts(), 1);
    }

    #[test]
    fn contact_resets_the_follower_and_the_next_attempt_draws_afresh() {
        let timing = wan_timing();
        let local = 3;
        let mut timer = ElectionTimer::new();
        let first = timing.timeout_periods(local, 0);
        assert!(ages_without_firing(
            &mut timer,
            0,
            &timing,
            local,
            first - 1
        ));
        assert_eq!(
            timer.follower_period(0, &timing, local, 0),
            FollowerStep::Campaign
        );
        assert!(ages_without_firing(&mut timer, 0, &timing, local, 5));
        assert_eq!(
            timer.follower_period(1, &timing, local, 0),
            FollowerStep::Follow
        );
        assert_eq!(timer.idle_periods(), 0);
        let second = timing.timeout_periods(local, 1);
        assert!(ages_without_firing(
            &mut timer,
            1,
            &timing,
            local,
            second - 1
        ));
        assert_eq!(
            timer.follower_period(1, &timing, local, 0),
            FollowerStep::Campaign
        );
    }

    /// No two nodes draw the same word for any attempts: over 32 ids, small and spread, and every
    /// attempt a node makes in 72 rounds, each pair of a node and an attempt has a word of its own,
    /// so no node's campaigns follow another's, at the same attempt or shifted (the finalizer is a
    /// bijection, `the_draw_is_a_bijection_scaled_without_bias`, and the input words here are
    /// distinct). Two nodes can still land in one period by chance, as Ongaro's independent
    /// timeouts do: that is the split `election_span` prices.
    #[test]
    fn distinct_nodes_and_attempts_draw_distinct_words() {
        let ids: Vec<u64> = (1..=16)
            .chain((1..=16).map(|seed: u64| seed.wrapping_mul(0x2545_f491_4f6c_dd1d)))
            .collect();
        let mut words: Vec<u64> = ids
            .iter()
            .flat_map(|id| (0..72u32).map(move |attempt| draw(*id, attempt)))
            .collect();
        let all = words.len();
        words.sort_unstable();
        words.dedup();
        assert_eq!(words.len(), all);
        let timing = wan_timing();
        assert!(
            timing.span_periods > 10,
            "the WAN span resolves in 1 ms periods: {}",
            timing.span_periods
        );
    }

    #[test]
    fn a_leader_checks_its_quorum_every_base_period() {
        let timing = timing_of(10 * MS, &[&path_of(&LOOPBACK_SAMPLES_MS)], 3);
        let mut timer = ElectionTimer::new();
        let fired = (1..=10 * timing.base_periods)
            .filter(|_| timer.leader_period(&timing))
            .count();
        assert_eq!(fired, 10);
        let mut timer = ElectionTimer::new();
        let first = (1..=1_000).find(|_| timer.leader_period(&timing)).unwrap();
        assert_eq!(first, timing.base_periods);
    }

    #[test]
    fn a_follower_yields_one_timeout_per_rank() {
        let timing = wan_timing();
        let local = 9;
        let mut timer = ElectionTimer::new();
        let mut fired = Vec::new();
        for _ in 0..3 {
            let mut periods = 0;
            loop {
                periods += 1;
                let campaign =
                    timer.follower_period(0, &timing, local, 2) == FollowerStep::Campaign;
                if campaign || timer.idle_periods() == 0 {
                    fired.push(campaign);
                    break;
                }
                assert!(periods < 10_000);
            }
        }
        assert_eq!(fired, vec![false, false, true]);
        assert_eq!(
            timer.follower_period(1, &timing, local, 2),
            FollowerStep::Follow
        );
        assert_eq!(timer.yielded(), 0);
        let mut rank_zero = ElectionTimer::new();
        let mut periods = 0;
        while rank_zero.follower_period(0, &timing, local, 0) != FollowerStep::Campaign {
            periods += 1;
            assert!(periods < 10_000);
        }
    }

    #[test]
    fn a_followers_lease_lapses_at_the_minimum_election_timeout() {
        let timing = wan_timing();
        let local = 9;
        let mut timer = ElectionTimer::new();
        let steps: Vec<FollowerStep> = (1..=timing.base_periods)
            .map(|_| timer.follower_period(0, &timing, local, 2))
            .collect();
        let (last, before) = steps.split_last().unwrap();
        assert!(before.iter().all(|step| *step == FollowerStep::Follow));
        assert_eq!(*last, FollowerStep::LeaderLapsed);
        let mut periods = timing.base_periods;
        loop {
            periods += 1;
            match timer.follower_period(0, &timing, local, 2) {
                FollowerStep::Campaign => break,
                step => assert_eq!(step, FollowerStep::LeaderLapsed, "period {periods}"),
            }
            assert!(periods < 10_000);
        }
        assert_eq!(timer.yielded(), 2);
        assert_eq!(
            timer.follower_period(1, &timing, local, 2),
            FollowerStep::Follow
        );
    }

    #[test]
    fn the_quorum_round_trip_is_the_majoritys_farthest_path() {
        let path = |rtt_ms: u64| {
            let mut path = ExchangeRtt::new();
            for _ in 0..8 {
                path.on_sample(rtt_ms * MS);
            }
            path
        };
        let g = nanos(G);
        let near = path(72);
        let middle = path(162);
        let far = path(262);
        let three = quorum_priority([Some(&middle), Some(&near)], 3, g);
        assert_eq!(three.quorum_ns, near.smoothed_ns());
        assert_eq!(Some(three.spread_ns), near.spread_ns(g));
        let five = quorum_priority([Some(&far), None, Some(&near), Some(&middle)], 5, g);
        assert_eq!(five.quorum_ns, middle.smoothed_ns());
        assert_eq!(
            quorum_priority([None, None, Some(&near), None], 5, g),
            ElectionPriority::default()
        );
        assert_eq!(
            quorum_priority::<ExchangeRtt>(std::iter::empty(), 1, g),
            ElectionPriority::default()
        );
    }

    /// The counted rank picks what sorting the measured paths picked, ties and gaps included.
    #[test]
    fn the_counted_rank_is_the_sorted_position() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let g = nanos(G);
        for _ in 0..2_000 {
            let voters = usize::try_from(next() % 9).unwrap() + 1;
            let paths: Vec<Option<ExchangeRtt>> = (1..voters)
                .map(|_| {
                    // Few distinct round trips, so ties are common; a fifth unmeasured.
                    let rtt = (next() % 4 + 1) * MS;
                    (next() % 5 != 0).then(|| {
                        let mut path = ExchangeRtt::new();
                        for _ in 0..usize::try_from(next() % 3).unwrap() + 1 {
                            path.on_sample(rtt + next() % 3);
                        }
                        path
                    })
                })
                .collect();
            let mut sorted: Vec<(u64, u64)> = paths
                .iter()
                .flatten()
                .filter_map(|p| p.spread_ns(g).map(|spread| (p.smoothed_ns(), spread)))
                .collect();
            sorted.sort_unstable();
            let expected = (voters / 2)
                .checked_sub(1)
                .and_then(|position| sorted.get(position))
                .map_or_else(ElectionPriority::default, |&(quorum_ns, spread_ns)| {
                    ElectionPriority {
                        quorum_ns,
                        spread_ns,
                    }
                });
            assert_eq!(
                quorum_priority(paths.iter().map(Option::as_ref), voters, g),
                expected
            );
            // And the ballot's quorum round is the same counted rank over the means.
            let mut means: Vec<u64> = paths
                .iter()
                .flatten()
                .map(ExchangeRtt::smoothed_ns)
                .collect();
            means.sort_unstable();
            let ballot = Ballot::measure(paths.iter().flatten(), voters, Duration::ZERO, G);
            let quorum = (voters / 2)
                .checked_sub(1)
                .and_then(|p| means.get(p))
                .copied();
            assert_eq!(
                ballot.map(|b| nanos(b.round)),
                quorum.filter(|_| voters >= 2)
            );
        }
    }

    #[test]
    fn a_priority_outranks_only_beyond_both_spreads() {
        let near = ElectionPriority {
            quorum_ns: 70 * MS,
            spread_ns: 10 * MS,
        };
        let far = ElectionPriority {
            quorum_ns: 160 * MS,
            spread_ns: 40 * MS,
        };
        let close = ElectionPriority {
            quorum_ns: 90 * MS,
            spread_ns: 20 * MS,
        };
        assert!(near.outranks(&far));
        assert!(!far.outranks(&near));
        assert!(!near.outranks(&close), "overlapping intervals tie");
        assert!(
            !ElectionPriority::default().outranks(&far),
            "unknown never outranks"
        );
    }

    /// slates' round-budget cases under the unified law, with the ceiling the election timing
    /// gives: the base election timeout, so a round never outlasts the timeout it would displace a
    /// leader over. An unmeasured round is given the whole ceiling, as focal's law gives it.
    #[test]
    fn a_round_budget_opens_to_the_measured_tail_within_its_ceiling() {
        let anchors = RoundAnchors {
            heartbeat_ns: HEARTBEAT,
            stall_periods: 2,
            polls_per_period: 10,
            lookahead: (3, 4),
        };
        let g = nanos(G);
        let lan = path_of(&LOOPBACK_SAMPLES_MS);
        let timing = timing_of(HEARTBEAT, &[&lan], 3);
        let ceiling = HEARTBEAT * u64::from(timing.base_periods);
        assert_eq!(anchors.poll_interval_ns(), HEARTBEAT / 10);
        let budget = RoundBudget::derive(&anchors, lan.tail_ns(g), ceiling);
        assert_eq!(
            budget.deadline_ns, HEARTBEAT,
            "a tail inside a period changes nothing"
        );
        assert_eq!(
            budget.max_deadline_ns(),
            ceiling,
            "extended to the base, no further"
        );
        assert_eq!(budget.stall_window_ns, 2 * HEARTBEAT);
        let wan = path_of(&WAN_SAMPLES_MS).tail_ns(g);
        let far = RoundBudget::derive(&anchors, wan, 10 * ceiling);
        assert_eq!(far.deadline_ns, wan.unwrap(), "the base opens to the tail");
        assert!(far.max_deadline_ns() <= 10 * ceiling);
        let unmeasured = RoundBudget::derive(&anchors, None, ceiling);
        assert_eq!(unmeasured.deadline_ns, ceiling);
        assert_eq!(unmeasured.max_extensions, 0);
    }
}
