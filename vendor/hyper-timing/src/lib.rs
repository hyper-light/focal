//! A group's timing, derived from what its links and paths measure (`docs/timing.md`).
//!
//! Raft needs `broadcast time ≪ election timeout ≪ MTBF` (Ongaro and Ousterhout, ATC 2014, §5.6).
//! Each side of that is measured here and none is a picked multiple of another:
//! - **When a leader is gone** is the failure detector's answer ([`LinkEstimator`],
//!   [`configure_arrivals`]): NFD-E on the leader's node-pair link, its interval `η` and margin `α`
//!   chosen to minimize the time a group cannot commit from the link's measured arrivals (each
//!   heartbeat's lateness past its expected arrival), the timer's measured granularity
//!   ([`Lateness`]) and the fleet's measured MTBF ([`Exposure`]). A follower's
//!   base election timeout is the detector's freshness horizon, `η + α` past the leader's latest
//!   heartbeat.
//! - **How long the voters that suspected together spread their campaigns** is the span `W` that
//!   minimizes the expected time to a leader, Ongaro's split-vote probability on the measured
//!   one-way latency and vote round ([`Ballot`], [`election_span`]). Its expected election `T_E` is
//!   the detector's election cost, so the detector and the election law are one minimization.
//! - **The granularity** under every tail is the measured lateness of the owner's own timed waits,
//!   passed in, never RFC 9002's assumed 1 ms.
//!
//! The round trips to the other voters are measured per path by two estimators:
//! [`PathRtt`], the median and median absolute deviation of a window derived from the link's
//! correlation time, which orders voters ([`quorum_priority`]) and which one stall does not reorder;
//! and [`ExchangeRtt`], RFC 9002 §5.3's smoothed estimator, which follows a peer that became slow at
//! once, as an exchange's deadline wants. Both give the mean round trip the split-vote span needs.
//!
//! The Raft core counts ticks. [`ElectionTiming`] gives a period-counting core its base and span in
//! the owner's periods, so a starved owner waits instead of campaigning on its own slowness;
//! [`TickPace`] gives a core whose tick counts are fixed when it opens the period that covers them.

#![cfg_attr(
    test,
    allow(
        clippy::cognitive_complexity,
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cast_possible_wrap,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )
)]

mod progress;
pub use progress::{ProgressDeadline, Spent};
mod round;
pub use round::{DeadlineExtender, ProgressWitness, RoundAnchors, RoundBudget, RoundWait, Verdict};
mod qos;
pub use qos::{
    Arrivals, Costs, Detector, Floors, LinkBehaviour, Span, arrival_detector_at,
    configure_arrivals, detector_at, election_span, lateness_bound, mistake_bound,
};
mod folds;
pub use folds::{Exposure, Flushes, FoldFull, Lateness, Wakes};
mod link;
pub use link::{
    Configuration, EstimateError, Estimates, Event, LinkEstimator, MILLION, PHI_PER_MILLION,
    Refusal, Schedule, Trust, WINDOW_LIMIT, Window,
};
mod election;
pub use election::{
    Ballot, ElectionPriority, ElectionTimer, ElectionTiming, FollowerStep, PathEstimate,
    REPAIR_ROUND_TRIPS, election_delay, quorum_priority,
};

use std::time::Duration;

/// RFC 9002 §5.3: `smoothed_rtt = 7/8 · smoothed_rtt + 1/8 · sample`, the 1/8 as a shift of 3.
const SMOOTHED_SHIFT: u32 = 3;
/// RFC 9002 §5.3: `rttvar = 3/4 · rttvar + 1/4 · |smoothed_rtt − sample|`, the 1/4 as a shift of 2.
const VARIATION_SHIFT: u32 = 2;
/// RFC 9002 §6.2.1: the probe timeout's variation term, `4 · rttvar`.
const TAIL_VARIATION_MULTIPLIER: u64 = 4;

/// Why a path's window could not be derived or built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathWindowError {
    /// A zero probe interval: no spacing to count a stall's samples in.
    ZeroInterval,
    /// A window of no samples.
    Empty,
    /// A window past [`WINDOW_LIMIT`], the most heartbeats any link's estimator holds: a path probed
    /// more than about 33,000 times within one correlation time. No measured link comes near it
    /// (the longest correlation time measured, 250 ms, over the finest granularity measured, 45 µs,
    /// is 5,556 probes; `docs/timing.md` §2.6).
    TooLong,
}

impl std::fmt::Display for PathWindowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ZeroInterval => "a probe interval of zero",
            Self::Empty => "a path window of no samples",
            Self::TooLong => "a path window past the longest a link's estimator holds",
        })
    }
}

impl std::error::Error for PathWindowError {}

/// The measured path to one peer: the median of its latest round trips and their median absolute
/// deviation, over a window derived from the link's correlation time.
///
/// It orders voters ([`quorum_priority`]), whose purpose is that one stall does not reorder them:
/// a peer that is starting, or stalled on its disk, answers late, and an estimator that smooths
/// (RFC 9002's, [`ExchangeRtt`]) is built to follow exactly that. The median does not move until
/// half of its window says so (its breakdown point is one half: Hampel 1971; Rousseeuw and Croux
/// 1993), so the window is the shortest that holds more than twice the samples one stall can make
/// late ([`PathRtt::new`]). A path that has become slow is followed once half of the window has
/// seen it, about one correlation time: as fast as any estimate one stall cannot move.
///
/// Karn's rule is the caller's: only an answered probe is a sample. A path with no sample
/// contributes nothing to a derivation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathRtt {
    /// The window's samples in arrival order, a ring.
    window: Vec<u64>,
    /// Where the next sample goes.
    next: usize,
    samples: u64,
    /// The samples the window holds, in order: the first `held` slots. A sample replaces the one
    /// it evicts by two shifts, so the window is never sorted whole.
    ordered: Vec<u64>,
    /// The sum of the samples the window holds: the mean the split-vote span needs.
    sum: u128,
    /// The window's median and median absolute deviation, computed when a sample changes the
    /// window: a path is read several times a period (the priority, the ballot, the round budget)
    /// and sampled about once, so each read is a field (`docs/benchmarks.md`, "hyper-timing").
    median: u64,
    deviation: u64,
}

impl PathRtt {
    /// A path probed every `interval` on a link whose correlation time is `correlation` (`T_c`,
    /// `docs/timing.md` §2.6, item 6). The window is `2k + 1`, `k = max(1, ⌈T_c / interval⌉)`.
    ///
    /// `T_c` is measured as the spacing past the longest stall, so a stall shorter than it makes at
    /// most `⌈T_c / interval⌉` consecutive probes late, and at least one where `T_c` is under the
    /// interval or none was seen. The median of `2k + 1` outvotes `k`, and no shorter window does.
    /// The two buffers are allocated here, once.
    pub fn new(correlation: Duration, interval: Duration) -> Result<Self, PathWindowError> {
        let interval_ns = nanos(interval);
        if interval_ns == 0 {
            return Err(PathWindowError::ZeroInterval);
        }
        let late = nanos(correlation).div_ceil(interval_ns).max(1);
        let window = late
            .checked_mul(2)
            .and_then(|twice| twice.checked_add(1))
            .ok_or(PathWindowError::TooLong)?;
        Self::with_window(usize::try_from(window).map_err(|_| PathWindowError::TooLong)?)
    }

    /// A path whose window holds `window` samples, at most [`WINDOW_LIMIT`].
    fn with_window(window: usize) -> Result<Self, PathWindowError> {
        if window == 0 {
            return Err(PathWindowError::Empty);
        }
        if u64::try_from(window).map_or(true, |w| w > WINDOW_LIMIT) {
            return Err(PathWindowError::TooLong);
        }
        Ok(Self {
            window: vec![0; window],
            next: 0,
            samples: 0,
            ordered: vec![0; window],
            sum: 0,
            median: 0,
            deviation: 0,
        })
    }

    /// The samples the window holds once full.
    pub fn window(&self) -> usize {
        self.window.len()
    }

    /// Fold one answered round trip in, in place of the oldest of the window.
    pub fn on_sample(&mut self, round_trip_ns: u64) {
        let capacity = self.window.len();
        let mut held = self.held();
        if held == capacity
            && let Some(&evicted) = self.window.get(self.next)
        {
            self.remove_ordered(evicted, held);
            self.sum = self.sum.saturating_sub(u128::from(evicted));
            held = held.saturating_sub(1);
        }
        self.insert_ordered(round_trip_ns, held);
        self.sum = self.sum.saturating_add(u128::from(round_trip_ns));
        if let Some(slot) = self.window.get_mut(self.next) {
            *slot = round_trip_ns;
        }
        self.next = self
            .next
            .saturating_add(1)
            .checked_rem(capacity)
            .unwrap_or(0);
        self.samples = self.samples.saturating_add(1);
        let held = self.held();
        self.median = self
            .ordered
            .get(held.checked_div(2).unwrap_or(0))
            .copied()
            .unwrap_or(0);
        self.deviation = self.median_deviation(held);
    }
    /// How many round trips have been folded in, including those the window no longer holds.
    pub const fn samples(&self) -> u64 {
        self.samples
    }
    /// How many samples the window holds.
    fn held(&self) -> usize {
        let capacity = self.window.len();
        usize::try_from(self.samples)
            .unwrap_or(capacity)
            .min(capacity)
    }
    /// Takes `value`, which the first `held` ordered slots hold, out of them.
    fn remove_ordered(&mut self, value: u64, held: usize) {
        if let Some(ordered) = self.ordered.get_mut(..held) {
            let position = ordered.partition_point(|&sample| sample < value);
            if let Some(after) = ordered.get_mut(position..) {
                after.rotate_left(1);
            }
        }
    }
    /// Puts `value` in order among the first `held` ordered slots, `held` below the window.
    fn insert_ordered(&mut self, value: u64, held: usize) {
        if let Some(ordered) = self.ordered.get_mut(..=held) {
            let position = ordered
                .get(..held)
                .map_or(0, |taken| taken.partition_point(|&sample| sample < value));
            if let Some(after) = ordered.get_mut(position..) {
                after.rotate_right(1);
            }
            if let Some(slot) = ordered.get_mut(position) {
                *slot = value;
            }
        }
    }
    /// The middle of the `held` samples' deviations from the median, the upper of two. Below the
    /// median the deviations rise as the samples fall, and above it they rise with the samples, so
    /// the two sides are merged from the median outwards until the middle one is reached.
    fn median_deviation(&self, held: usize) -> u64 {
        let centre = held.checked_div(2).unwrap_or(0);
        let median = self.median;
        let below = |step: usize| {
            centre
                .checked_sub(step.saturating_add(1))
                .and_then(|index| self.ordered.get(index))
                .map(|sample| median.saturating_sub(*sample))
        };
        let above = |step: usize| {
            centre
                .checked_add(step)
                .filter(|index| *index < held)
                .and_then(|index| self.ordered.get(index))
                .map(|sample| sample.saturating_sub(median))
        };
        let (mut low, mut high, mut deviation) = (0usize, 0usize, 0u64);
        for _ in 0..=centre {
            match (below(low), above(high)) {
                (Some(down), Some(up)) if down < up => {
                    deviation = down;
                    low = low.saturating_add(1);
                }
                (Some(down), None) => {
                    deviation = down;
                    low = low.saturating_add(1);
                }
                (_, Some(up)) => {
                    deviation = up;
                    high = high.saturating_add(1);
                }
                (None, None) => break,
            }
        }
        deviation
    }
    /// The median round trip; zero before a sample.
    pub const fn smoothed_ns(&self) -> u64 {
        self.median
    }
    /// The median absolute deviation from the median; zero before a sample.
    pub const fn variation_ns(&self) -> u64 {
        self.deviation
    }
    /// The mean of the window's round trips; zero before a sample.
    pub fn mean_ns(&self) -> u64 {
        let held = u128::try_from(self.held()).unwrap_or(u128::MAX);
        self.sum
            .checked_div(held)
            .and_then(|mean| u64::try_from(mean).ok())
            .unwrap_or(0)
    }
    /// The bound on this path's round-trip tail, `median + max(4 · deviation, G)` with `G` the
    /// owner's measured timer granularity ([`Lateness`]), or `None` before a sample.
    pub fn tail_ns(&self, granularity_ns: u64) -> Option<u64> {
        (self.samples > 0).then(|| {
            self.smoothed_ns().saturating_add(
                TAIL_VARIATION_MULTIPLIER
                    .saturating_mul(self.variation_ns())
                    .max(granularity_ns),
            )
        })
    }
}

/// What an exchange with one peer takes, the peer's work included: smoothed
/// round trip and mean deviation over every exchange it answered (RFC 9002
/// §5.3). It follows a peer that has become slow at once, which is what a
/// deadline for the next exchange with that peer wants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExchangeRtt {
    smoothed_ns: u64,
    variation_ns: u64,
    samples: u64,
}

impl ExchangeRtt {
    /// An estimator with no sample.
    pub const fn new() -> Self {
        Self {
            smoothed_ns: 0,
            variation_ns: 0,
            samples: 0,
        }
    }
    /// Fold one completed round trip in (RFC 9002 §5.3).
    pub fn on_sample(&mut self, round_trip_ns: u64) {
        if self.samples == 0 {
            self.smoothed_ns = round_trip_ns;
            self.variation_ns = round_trip_ns >> 1;
        } else {
            let deviation = self.smoothed_ns.abs_diff(round_trip_ns);
            self.variation_ns = self
                .variation_ns
                .saturating_sub(self.variation_ns >> VARIATION_SHIFT)
                .saturating_add(deviation >> VARIATION_SHIFT);
            self.smoothed_ns = self
                .smoothed_ns
                .saturating_sub(self.smoothed_ns >> SMOOTHED_SHIFT)
                .saturating_add(round_trip_ns >> SMOOTHED_SHIFT);
        }
        self.samples = self.samples.saturating_add(1);
    }
    /// How many round trips have been folded in.
    pub const fn samples(&self) -> u64 {
        self.samples
    }
    /// The smoothed round trip, `smoothed_rtt` (RFC 9002 §5.3), in nanoseconds.
    pub const fn smoothed_ns(&self) -> u64 {
        self.smoothed_ns
    }
    /// The mean deviation, `rttvar` (RFC 9002 §5.3), in nanoseconds.
    pub const fn variation_ns(&self) -> u64 {
        self.variation_ns
    }
    /// The bound on this path's round-trip tail, `smoothed + max(4 · rttvar, G)` (RFC 9002
    /// §6.2.1), or `None` before a sample. `G` is RFC 9002's `kGranularity`, "Timer granularity.
    /// This is a system-dependent value" (Appendix A.2; §6.1.2 recommends 1 ms for a timer it
    /// cannot see): here the owner's measured lateness of its own timed waits ([`Lateness`]).
    pub fn tail_ns(&self, granularity_ns: u64) -> Option<u64> {
        (self.samples > 0).then(|| {
            self.smoothed_ns.saturating_add(
                TAIL_VARIATION_MULTIPLIER
                    .saturating_mul(self.variation_ns)
                    .max(granularity_ns),
            )
        })
    }
}

/// A group's tick period for now, for a core whose tick counts are fixed when it opens, with what
/// it was derived from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickPace {
    /// How long the owner waits between ticks.
    pub period: Duration,
    /// The election time the period covers in `election_tick` ticks: the longer of the base
    /// election timeout and the span; zero when nothing is measured.
    pub covered: Duration,
    /// Round trips measured across the voter paths that fed this pace: the witness that the pace
    /// is measured and not the floor it defaults to.
    pub samples: u64,
}

impl TickPace {
    /// The pace with nothing measured: the configured period.
    pub fn floor(configured: Duration) -> Self {
        Self {
            period: configured,
            covered: Duration::ZERO,
            samples: 0,
        }
    }

    /// The period, no shorter than `configured` and no longer than `ceiling`, at which
    /// `election_tick` ticks cover both the base election timeout and the span of `timing`.
    ///
    /// A core shaped as raft-rs draws its timeout uniformly from `[election_tick,
    /// 2·election_tick)` ticks, so one tick count is both its base and its span: the base must not
    /// fire before the detector would suspect the leader, and the span must be at least the one
    /// that minimizes the expected election (`docs/timing.md` §2.3), so the period covers the
    /// longer of the two. The ceiling bounds how long a dead leader can go unnoticed; a timing
    /// past what the ceiling allows keeps the ceiling, and `covered` shows the shortfall.
    pub fn derive(
        configured: Duration,
        ceiling: Duration,
        election_tick: usize,
        timing: &ElectionTiming,
    ) -> Self {
        let covered = timing.base.max(timing.span);
        let ticks = u64::try_from(election_tick).unwrap_or(u64::MAX).max(1);
        let needed = nanos(covered).div_ceil(ticks);
        let floor = nanos(configured).max(1);
        let ceiling = nanos(ceiling).max(floor);
        Self {
            period: Duration::from_nanos(needed.clamp(floor, ceiling)),
            covered,
            samples: timing.samples,
        }
    }
    /// The election timeout this pace gives a node of `election_tick` ticks.
    pub fn election_timeout(&self, election_tick: usize) -> Duration {
        let ticks = u32::try_from(election_tick).unwrap_or(u32::MAX);
        self.period.saturating_mul(ticks)
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const MS: u64 = 1_000_000;
    /// The timer granularity the tests' owner measured: macOS's 45 µs at a 40 µs wait
    /// (`docs/timing.md` §2.4).
    const G: u64 = 45_000;
    const TICK: Duration = Duration::from_millis(100);
    const CEILING: Duration = Duration::from_secs(5);
    const ELECTION_TICK: usize = 10;

    fn path_of(window: usize, samples_ms: &[u64]) -> PathRtt {
        let mut path = PathRtt::with_window(window).unwrap();
        for sample in samples_ms {
            path.on_sample(sample * MS);
        }
        path
    }

    fn timing(base_ms: u64, span_ms: u64) -> ElectionTiming {
        ElectionTiming {
            base_periods: 1,
            span_periods: 1,
            base: Duration::from_millis(base_ms),
            span: Duration::from_millis(span_ms),
            detection: Duration::from_millis(base_ms),
            election: Duration::from_millis(span_ms),
            broadcast_tail: Duration::ZERO,
            samples: 6,
        }
    }

    /// The window is `2·max(1, ⌈T_c/p⌉) + 1`: the measured links' correlation times at their
    /// probe intervals, and the refusals.
    #[test]
    fn the_window_outvotes_the_samples_one_stall_makes_late() {
        let window = |tc_ms: f64, p_ms: f64| {
            PathRtt::new(
                Duration::from_secs_f64(tc_ms / 1e3),
                Duration::from_secs_f64(p_ms / 1e3),
            )
            .map(|path| path.window())
        };
        // At or past the correlation time a stall makes one probe late: three.
        assert_eq!(window(50.0, 50.0), Ok(3));
        assert_eq!(window(50.0, 200.0), Ok(3));
        assert_eq!(
            window(0.0, 10.0),
            Ok(3),
            "no stall seen: one late probe still"
        );
        // macOS at a 10 ms probe, T_c 50 ms: five late, eleven.
        assert_eq!(window(50.0, 10.0), Ok(11));
        // Linux with fdatasync, T_c 200 ms, at a 2 ms probe: a hundred late, 201.
        assert_eq!(window(200.0, 2.0), Ok(201));
        assert_eq!(window(50.0, 0.0), Err(PathWindowError::ZeroInterval));
        assert_eq!(
            PathRtt::new(Duration::from_secs(1), Duration::from_nanos(1)).err(),
            Some(PathWindowError::TooLong)
        );
        assert_eq!(
            PathRtt::new(Duration::MAX, Duration::from_nanos(1)).err(),
            Some(PathWindowError::TooLong)
        );
        assert_eq!(PathRtt::with_window(0).err(), Some(PathWindowError::Empty));
    }

    proptest! {
        /// The window kept in order and the merged deviations give what sorting the window gave,
        /// and the mean is the window's, at every derived window.
        #[test]
        fn the_ordered_window_is_the_sorted_window(
            k in 1usize..12,
            samples in prop::collection::vec(1u64..8, 1..120),
        ) {
            let window = 2 * k + 1;
            let middle = |values: &mut Vec<u64>| {
                values.sort_unstable();
                values.get(values.len() / 2).copied().unwrap_or(0)
            };
            let mut path = PathRtt::with_window(window).unwrap();
            let mut latest: std::collections::VecDeque<u64> = std::collections::VecDeque::new();
            for sample in samples {
                // Few distinct values, so ties are common.
                let sample = sample * MS;
                path.on_sample(sample);
                latest.push_back(sample);
                if latest.len() > window {
                    latest.pop_front();
                }
                let median = middle(&mut latest.iter().copied().collect());
                let deviation = middle(&mut latest.iter().map(|s| s.abs_diff(median)).collect());
                prop_assert_eq!((path.smoothed_ns(), path.variation_ns()), (median, deviation));
                let mean = latest.iter().map(|&s| u128::from(s)).sum::<u128>() / latest.len() as u128;
                prop_assert_eq!(u128::from(path.mean_ns()), mean);
            }
        }

        /// A stall of up to `k` consecutive late answers, however late, leaves a path of `2k + 1`
        /// where it was; `k + 1` move it.
        #[test]
        fn a_stall_the_window_outvotes_moves_nothing(k in 1usize..20, late in 1u64..10_000) {
            let mut path = PathRtt::with_window(2 * k + 1).unwrap();
            for _ in 0..2 * k + 1 {
                path.on_sample(5 * MS);
            }
            for _ in 0..k {
                path.on_sample((5 + late) * MS);
            }
            prop_assert_eq!(path.smoothed_ns(), 5 * MS);
            path.on_sample((5 + late) * MS);
            prop_assert_eq!(path.smoothed_ns(), (5 + late) * MS);
        }
    }

    #[test]
    fn a_path_is_the_median_of_its_latest_answers_and_their_deviation() {
        let mut path = PathRtt::with_window(5).unwrap();
        assert_eq!(path.tail_ns(G), None, "no sample, no tail");
        assert_eq!((path.smoothed_ns(), path.variation_ns()), (0, 0));
        path.on_sample(80 * MS);
        assert_eq!(path.smoothed_ns(), 80 * MS);
        assert_eq!(path.variation_ns(), 0);
        assert_eq!(
            path.tail_ns(G),
            Some(80 * MS + G),
            "the granularity at least"
        );
        for sample in [100, 60, 90, 70] {
            path.on_sample(sample * MS);
        }
        // 60 70 80 90 100: the median 80, the deviations 0 10 10 20 20.
        assert_eq!(path.smoothed_ns(), 80 * MS);
        assert_eq!(path.variation_ns(), 10 * MS);
        assert_eq!(path.mean_ns(), 80 * MS);
        assert_eq!(path.tail_ns(G), Some(120 * MS));
        assert_eq!(path.samples(), 5);
    }

    #[test]
    fn a_path_that_became_slow_is_followed_within_half_its_window() {
        let window = 11;
        let mut path = PathRtt::with_window(window).unwrap();
        for _ in 0..window {
            path.on_sample(5 * MS);
        }
        let mut followed = None;
        for sample in 1..=window {
            path.on_sample(160 * MS);
            if followed.is_none() && path.smoothed_ns() == 160 * MS {
                followed = Some(sample);
            }
        }
        assert_eq!(followed, Some(window / 2 + 1), "past half of the window");
        assert_eq!(path.tail_ns(G), Some(160 * MS + G));
        assert_eq!(path.samples(), 2 * window as u64);
    }

    #[test]
    fn the_exchange_estimator_follows_rfc_9002() {
        let mut path = ExchangeRtt::new();
        assert_eq!(path.tail_ns(G), None, "no sample, no tail");
        path.on_sample(80 * MS);
        assert_eq!(path.smoothed_ns(), 80 * MS, "the first sample seeds it");
        assert_eq!(path.variation_ns(), 40 * MS, "half the first sample");
        assert_eq!(path.tail_ns(G), Some(240 * MS));
        path.on_sample(160 * MS);
        assert_eq!(path.variation_ns(), 50 * MS);
        assert_eq!(path.smoothed_ns(), 90 * MS);
        assert_eq!(path.tail_ns(G), Some(290 * MS));
        assert_eq!(path.samples(), 2);
    }

    /// The floor under the variation term is the granularity passed in: none invented where none
    /// was measured, and the measured one where it was.
    #[test]
    fn a_steady_path_keeps_the_measured_granularity_above_its_centre() {
        let mut path = path_of(3, &[]);
        let mut exchange = ExchangeRtt::new();
        for _ in 0..200 {
            path.on_sample(20 * MS);
            exchange.on_sample(20 * MS);
        }
        assert_eq!(path.smoothed_ns(), 20 * MS);
        assert_eq!(path.tail_ns(G), Some(20 * MS + G));
        assert_eq!(path.tail_ns(0), Some(20 * MS));
        assert_eq!(path.tail_ns(MS), Some(21 * MS), "Linux's 1 ms tick");
        assert_eq!(exchange.tail_ns(G), Some(20 * MS + G));
    }

    #[test]
    fn with_nothing_measured_the_pace_is_the_configured_period() {
        let pace = TickPace::floor(TICK);
        assert_eq!(pace.period, TICK);
        assert_eq!(pace.samples, 0);
        assert_eq!(pace.election_timeout(ELECTION_TICK), Duration::from_secs(1));
    }

    #[test]
    fn a_timing_inside_the_ticks_leaves_the_pace_unchanged() {
        // A 50 ms detector with a 65.8 ms margin, as on macOS (§2.6): 116 ms in ten 100 ms ticks.
        let pace = TickPace::derive(TICK, CEILING, ELECTION_TICK, &timing(116, 1));
        assert_eq!(pace.period, TICK, "a LAN group runs as configured");
        assert_eq!(pace.samples, 6, "and records that it measured");
        assert_eq!(pace.covered, Duration::from_millis(116));
    }

    #[test]
    fn the_ticks_cover_the_longer_of_the_base_and_the_span() {
        for (base, span) in [(2_400, 3), (400, 2_400), (1_500, 1_500)] {
            let timing = timing(base, span);
            let pace = TickPace::derive(TICK, CEILING, ELECTION_TICK, &timing);
            let timeout = pace.election_timeout(ELECTION_TICK);
            let covered = timing.base.max(timing.span);
            assert!(pace.period > TICK);
            assert!(timeout >= covered, "{timeout:?} under {covered:?}");
            assert!(
                timeout < covered + Duration::from_nanos(ELECTION_TICK as u64),
                "and no longer than rounding requires"
            );
        }
    }

    #[test]
    fn the_ceiling_bounds_the_period_and_the_measurement_shows_the_shortfall() {
        let timing = timing(10_000, 3);
        let tight = Duration::from_millis(500);
        let pace = TickPace::derive(TICK, tight, ELECTION_TICK, &timing);
        assert_eq!(pace.period, tight);
        assert!(pace.election_timeout(ELECTION_TICK) < pace.covered);
        // A ceiling below the configured period is the configured period.
        let pace = TickPace::derive(TICK, Duration::from_millis(1), ELECTION_TICK, &timing);
        assert_eq!(pace.period, TICK);
    }

    #[test]
    fn extreme_inputs_saturate() {
        let mut path = path_of(3, &[]);
        path.on_sample(u64::MAX);
        path.on_sample(u64::MAX);
        assert_eq!(path.mean_ns(), u64::MAX);
        assert_eq!(path.tail_ns(u64::MAX), Some(u64::MAX));
        let mut timing = timing(0, 0);
        timing.base = Duration::MAX;
        let pace = TickPace::derive(TICK, CEILING, 0, &timing);
        assert_eq!(pace.period, CEILING);
        assert_eq!(
            pace.election_timeout(usize::MAX),
            CEILING.saturating_mul(u32::MAX)
        );
    }
}
