//! A group's timing, derived from the round trips it measures (27 §3.1 P2).
//!
//! Raft needs `broadcast time ≪ election timeout ≪ MTBF` (Ongaro and
//! Ousterhout, ATC 2014, §5.6): a leader must reach its followers several
//! times inside one election timeout, or followers presume it dead and
//! campaign against a live leader. The ratio is an order of magnitude, fixed
//! here at ten ([`ELECTION_MARGIN`]).
//!
//! The broadcast time is measured per voter path by an RFC 9002 §5.3
//! estimator ([`PathRtt`]); its probe-timeout form `smoothed + 4 · rttvar` is
//! Jacobson's mean-deviation bound on the round-trip tail. A round completes
//! when its last voter answers, so the slowest path bounds the broadcast.
//!
//! The Raft core counts ticks, and its tick counts are fixed when a node
//! opens. What is derived is therefore the **tick period**
//! ([`TickPace::derive`]): the period is stretched until
//! `election_tick × period ≥ ELECTION_MARGIN × tail`. On a loopback or a LAN,
//! where every round trip sits inside the configured period, the period is
//! the configured one and nothing changes. A far group's period grows with
//! its measured tail. Because the owner ticks once per period, a node whose
//! own thread is starved ticks late and waits longer instead of campaigning
//! on its own slowness.

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

mod progress;
pub use progress::{ProgressDeadline, Spent};
mod round;
pub use round::{DeadlineExtender, ProgressWitness, RoundBudget, RoundWait, Verdict};

use std::time::Duration;

/// Raft's order-of-magnitude ratio of election timeout to broadcast time.
pub const ELECTION_MARGIN: u64 = 10;
/// The estimator's timer granularity (RFC 9002 §6.1.2, `kGranularity`).
pub const GRANULARITY_NS: u64 = 1_000_000;
const SMOOTHED_SHIFT: u32 = 3;
const VARIATION_SHIFT: u32 = 2;
const TAIL_VARIATION_MULTIPLIER: u64 = 4;

/// How many of a path's latest round trips its estimate is taken over.
pub const PATH_WINDOW: usize = 16;

/// The measured path to one peer: the median of its latest round trips and
/// their median absolute deviation.
///
/// A path's estimate sets a group's election timeout, so it has to say what
/// the path is and not what one answer took. A peer that is starting, or
/// stalled on its disk, answers a probe seconds late; an estimator that
/// smooths (RFC 9002's, [`ExchangeRtt`]) is built to react to exactly that,
/// and one such answer puts its tail at seconds for as long as it takes the
/// smoothing to forget. The median and the median absolute deviation do not
/// move until half of the window says so: fewer than half of the latest
/// [`PATH_WINDOW`] answers, however late, leave the estimate where the path
/// is, and a path that has become slow is followed once most of the window
/// has seen it.
///
/// Karn's rule is the caller's: only an answered probe is a sample. A path
/// with no sample contributes nothing to a derivation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PathRtt {
    window: [u64; PATH_WINDOW],
    /// Where the next sample goes.
    next: usize,
    samples: u64,
}
impl PathRtt {
    pub const fn new() -> Self {
        Self {
            window: [0; PATH_WINDOW],
            next: 0,
            samples: 0,
        }
    }
    /// Fold one answered round trip in, in place of the oldest of the
    /// window.
    pub fn on_sample(&mut self, round_trip_ns: u64) {
        if let Some(slot) = self.window.get_mut(self.next) {
            *slot = round_trip_ns;
        }
        self.next = self
            .next
            .saturating_add(1)
            .checked_rem(PATH_WINDOW)
            .unwrap_or(0);
        self.samples = self.samples.saturating_add(1);
    }
    pub const fn samples(&self) -> u64 {
        self.samples
    }
    /// The samples in the window, sorted, and how many there are.
    fn sorted(&self) -> ([u64; PATH_WINDOW], usize) {
        let held = usize::try_from(self.samples)
            .unwrap_or(PATH_WINDOW)
            .min(PATH_WINDOW);
        let mut sorted = [u64::MAX; PATH_WINDOW];
        for (slot, sample) in sorted.iter_mut().zip(self.window.iter().take(held)) {
            *slot = *sample;
        }
        // The samples held are the first `held` slots of the ring until it
        // is full, and all of it afterwards; the rest sort last.
        sorted.sort_unstable();
        (sorted, held)
    }
    /// The middle of `held` sorted values, the upper one of two.
    fn middle(sorted: &[u64; PATH_WINDOW], held: usize) -> u64 {
        sorted
            .get(held.checked_div(2).unwrap_or(0))
            .copied()
            .unwrap_or(0)
    }
    /// The median round trip; zero before a sample.
    pub fn smoothed_ns(&self) -> u64 {
        let (sorted, held) = self.sorted();
        if held == 0 {
            return 0;
        }
        Self::middle(&sorted, held)
    }
    /// The median absolute deviation from the median; zero before a sample.
    pub fn variation_ns(&self) -> u64 {
        let (sorted, held) = self.sorted();
        if held == 0 {
            return 0;
        }
        let median = Self::middle(&sorted, held);
        let mut deviations = [u64::MAX; PATH_WINDOW];
        for (slot, sample) in deviations.iter_mut().zip(sorted.iter().take(held)) {
            *slot = sample.abs_diff(median);
        }
        deviations.sort_unstable();
        Self::middle(&deviations, held)
    }
    /// The bound on this path's round-trip tail,
    /// `median + max(4 · deviation, granularity)`, or `None` before a
    /// sample.
    pub fn tail_ns(&self) -> Option<u64> {
        (self.samples > 0).then(|| {
            self.smoothed_ns().saturating_add(
                TAIL_VARIATION_MULTIPLIER
                    .saturating_mul(self.variation_ns())
                    .max(GRANULARITY_NS),
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
    pub const fn samples(&self) -> u64 {
        self.samples
    }
    pub const fn smoothed_ns(&self) -> u64 {
        self.smoothed_ns
    }
    pub const fn variation_ns(&self) -> u64 {
        self.variation_ns
    }
    /// The bound on this path's round-trip tail,
    /// `smoothed + max(4 · rttvar, granularity)`, or `None` before a sample.
    pub fn tail_ns(&self) -> Option<u64> {
        (self.samples > 0).then(|| {
            self.smoothed_ns.saturating_add(
                TAIL_VARIATION_MULTIPLIER
                    .saturating_mul(self.variation_ns)
                    .max(GRANULARITY_NS),
            )
        })
    }
}

/// A group's tick period for now, with what it was derived from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickPace {
    /// How long the owner waits between ticks.
    pub period: Duration,
    /// The slowest measured voter path's tail; zero when none is measured.
    pub broadcast_tail_ns: u64,
    /// Round trips measured across the voter paths that fed this pace: the
    /// witness that the pace is measured and not the floor it defaults to.
    pub samples: u64,
}

impl TickPace {
    /// The pace with no voter path measured: the configured period.
    pub fn floor(configured: Duration) -> Self {
        Self::derive(configured, configured, 1, core::iter::empty())
    }

    /// Derive the period from the measured `paths` to the group's other
    /// voters: the smallest period, no shorter than `configured` and no
    /// longer than `ceiling`, for which
    /// `election_tick × period ≥ ELECTION_MARGIN × tail`.
    ///
    /// The ceiling bounds how long a dead leader can go unnoticed; a path
    /// slower than the ceiling allows keeps the ceiling and the measurement
    /// beside it shows the shortfall.
    pub fn derive<'a>(
        configured: Duration,
        ceiling: Duration,
        election_tick: usize,
        paths: impl IntoIterator<Item = &'a PathRtt>,
    ) -> Self {
        let mut tail = 0u64;
        let mut samples = 0u64;
        for path in paths {
            if let Some(path_tail) = path.tail_ns() {
                tail = tail.max(path_tail);
                samples = samples.saturating_add(path.samples());
            }
        }
        let ticks = u64::try_from(election_tick).unwrap_or(u64::MAX).max(1);
        let needed = ELECTION_MARGIN.saturating_mul(tail).div_ceil(ticks);
        let floor = nanos(configured).max(1);
        let ceiling = nanos(ceiling).max(floor);
        Self {
            period: Duration::from_nanos(needed.clamp(floor, ceiling)),
            broadcast_tail_ns: tail,
            samples,
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

    const MS: u64 = 1_000_000;
    const TICK: Duration = Duration::from_millis(100);
    const CEILING: Duration = Duration::from_secs(5);
    const ELECTION_TICK: usize = 10;
    /// Loopback round trips: probe tail 17 ms, broadcast 11 to 33 ms.
    const LOOPBACK_MS: [u64; 4] = [11, 17, 11, 33];
    /// An inter-region path of 80 ms ± 20 ms one way.
    const REGIONAL_MS: [u64; 6] = [160, 200, 120, 160, 190, 130];
    /// A geographic path of 500 ms ± 100 ms one way.
    const GEOGRAPHIC_MS: [u64; 6] = [1000, 1200, 800, 1000, 1150, 850];

    fn path_of(samples_ms: &[u64]) -> PathRtt {
        let mut path = PathRtt::new();
        for sample in samples_ms {
            path.on_sample(sample * MS);
        }
        path
    }

    #[test]
    fn a_path_is_the_median_of_its_latest_answers_and_their_deviation() {
        let mut path = PathRtt::new();
        assert_eq!(path.tail_ns(), None, "no sample, no tail");
        assert_eq!((path.smoothed_ns(), path.variation_ns()), (0, 0));
        path.on_sample(80 * MS);
        assert_eq!(path.smoothed_ns(), 80 * MS);
        assert_eq!(path.variation_ns(), 0);
        assert_eq!(path.tail_ns(), Some(81 * MS), "the granularity at least");
        for sample in [100, 60, 90, 70] {
            path.on_sample(sample * MS);
        }
        // 60 70 80 90 100: the median 80, the deviations 0 10 10 20 20.
        assert_eq!(path.smoothed_ns(), 80 * MS);
        assert_eq!(path.variation_ns(), 10 * MS);
        assert_eq!(path.tail_ns(), Some(120 * MS));
        assert_eq!(path.samples(), 5);
    }

    #[test]
    fn answers_that_came_late_do_not_move_a_path() {
        // A peer that was starting answered three probes seconds late.
        let mut path = PathRtt::new();
        for _ in 0..PATH_WINDOW {
            path.on_sample(5 * MS);
        }
        let before = path.tail_ns().unwrap();
        assert_eq!(before, 6 * MS);
        for late in [2_900, 1_400, 800] {
            path.on_sample(late * MS);
            assert_eq!(path.tail_ns(), Some(before));
        }
        // The smoothing estimator, given the same, is at seconds.
        let mut exchange = ExchangeRtt::new();
        for _ in 0..PATH_WINDOW {
            exchange.on_sample(5 * MS);
        }
        exchange.on_sample(2_900 * MS);
        assert!(exchange.tail_ns().unwrap() > 3_000 * MS);
        // Fewer than half of the window, however late, are outvoted.
        let mut path = PathRtt::new();
        for _ in 0..PATH_WINDOW {
            path.on_sample(5 * MS);
        }
        for _ in 0..PATH_WINDOW / 2 - 1 {
            path.on_sample(10_000 * MS);
        }
        assert_eq!(path.smoothed_ns(), 5 * MS);
    }

    #[test]
    fn a_path_that_became_slow_is_followed_within_its_window() {
        let mut path = PathRtt::new();
        for _ in 0..PATH_WINDOW {
            path.on_sample(5 * MS);
        }
        let mut followed = None;
        for sample in 1..=PATH_WINDOW {
            path.on_sample(160 * MS);
            if followed.is_none() && path.smoothed_ns() == 160 * MS {
                followed = Some(sample);
            }
        }
        assert_eq!(followed, Some(PATH_WINDOW / 2), "at half of the window");
        assert_eq!(path.tail_ns(), Some(161 * MS));
        assert_eq!(path.samples(), 2 * PATH_WINDOW as u64);
    }

    #[test]
    fn the_exchange_estimator_follows_rfc_9002() {
        let mut path = ExchangeRtt::new();
        assert_eq!(path.tail_ns(), None, "no sample, no tail");
        path.on_sample(80 * MS);
        assert_eq!(path.smoothed_ns(), 80 * MS, "the first sample seeds it");
        assert_eq!(path.variation_ns(), 40 * MS, "half the first sample");
        assert_eq!(path.tail_ns(), Some(240 * MS));
        path.on_sample(160 * MS);
        assert_eq!(path.variation_ns(), 50 * MS);
        assert_eq!(path.smoothed_ns(), 90 * MS);
        assert_eq!(path.tail_ns(), Some(290 * MS));
        assert_eq!(path.samples(), 2);
    }

    #[test]
    fn a_steady_path_keeps_at_least_the_granularity_above_its_mean() {
        let mut path = PathRtt::new();
        for _ in 0..200 {
            path.on_sample(20 * MS);
        }
        assert_eq!(path.smoothed_ns(), 20 * MS);
        assert_eq!(path.tail_ns(), Some(20 * MS + GRANULARITY_NS));
    }

    #[test]
    fn with_no_measured_path_the_pace_is_the_configured_period() {
        let pace = TickPace::derive(TICK, CEILING, ELECTION_TICK, core::iter::empty());
        assert_eq!(pace, TickPace::floor(TICK));
        assert_eq!(pace.period, TICK);
        assert_eq!(pace.samples, 0);
        assert_eq!(pace.election_timeout(ELECTION_TICK), Duration::from_secs(1));
        // An unmeasured path beside nothing else contributes nothing.
        let unmeasured = PathRtt::new();
        assert_eq!(
            TickPace::derive(TICK, CEILING, ELECTION_TICK, [&unmeasured]),
            pace
        );
    }

    #[test]
    fn a_path_inside_the_period_leaves_the_pace_unchanged() {
        let path = path_of(&LOOPBACK_MS);
        let pace = TickPace::derive(TICK, CEILING, ELECTION_TICK, [&path]);
        assert_eq!(pace.period, TICK, "a LAN group runs as configured");
        assert_eq!(pace.samples, 4, "and records that it measured");
        assert!(pace.broadcast_tail_ns > 0);
    }

    #[test]
    fn a_far_path_stretches_the_period_to_ten_tails_per_election_timeout() {
        for samples in [&REGIONAL_MS, &GEOGRAPHIC_MS] {
            let path = path_of(samples);
            let tail = path.tail_ns().unwrap();
            let pace = TickPace::derive(TICK, CEILING, ELECTION_TICK, [&path]);
            assert!(pace.period > TICK);
            let timeout = nanos(pace.election_timeout(ELECTION_TICK));
            assert!(
                timeout >= ELECTION_MARGIN * tail,
                "election timeout {timeout} under ten tails of {tail}"
            );
            assert!(
                timeout < ELECTION_MARGIN * tail + ELECTION_TICK as u64,
                "and no longer than rounding requires"
            );
        }
    }

    #[test]
    fn the_slowest_voter_path_bounds_the_broadcast() {
        let near = path_of(&LOOPBACK_MS);
        let far = path_of(&REGIONAL_MS);
        let both = TickPace::derive(TICK, CEILING, ELECTION_TICK, [&near, &far]);
        let alone = TickPace::derive(TICK, CEILING, ELECTION_TICK, [&far]);
        assert_eq!(both.period, alone.period);
        assert_eq!(both.broadcast_tail_ns, far.tail_ns().unwrap());
        assert_eq!(both.samples, 10);
    }

    #[test]
    fn the_ceiling_bounds_the_period_and_the_measurement_shows_the_shortfall() {
        let path = path_of(&GEOGRAPHIC_MS);
        let tight = Duration::from_millis(500);
        let pace = TickPace::derive(TICK, tight, ELECTION_TICK, [&path]);
        assert_eq!(pace.period, tight);
        assert!(
            nanos(pace.election_timeout(ELECTION_TICK)) < ELECTION_MARGIN * pace.broadcast_tail_ns
        );
        // A ceiling below the configured period is the configured period.
        let pace = TickPace::derive(TICK, Duration::from_millis(1), ELECTION_TICK, [&path]);
        assert_eq!(pace.period, TICK);
    }

    #[test]
    fn extreme_inputs_saturate() {
        let mut path = PathRtt::new();
        path.on_sample(u64::MAX);
        path.on_sample(u64::MAX);
        let pace = TickPace::derive(TICK, CEILING, 0, [&path]);
        assert_eq!(pace.period, CEILING);
        assert_eq!(
            pace.election_timeout(usize::MAX),
            CEILING.saturating_mul(u32::MAX)
        );
    }
}
