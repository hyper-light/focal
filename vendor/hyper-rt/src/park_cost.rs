//! What blocking costs a shard in CPU, learned from the parks it pays (docs/runtime.md §3.4, §3.5;
//! docs/research `42-event-loop-under-load.md` in mantle): one park/wake cycle's CPU on the shard's own
//! thread (its re-check, the driver's wait call, the switch out and back in) and on the sender's (the kick's
//! system call), each read on that thread's CPU clock ([`crate::attribution::thread_cpu_now`]). A shard's
//! idle spin lasts as long as blocking costs: spin-then-block is then 2-competitive in the CPU the wait
//! spends against an adversary who knows when the next message comes [A: Karlin, Li, Manasse, Owicki,
//! "Empirical studies of competitive spinning for a shared-memory multiprocessor", SOSP 1991, where the
//! threshold is the cost of a context switch].
//!
//! **Why CPU and not the wake's latency.** The spin used to last the measured wake (kick to running), and a
//! wake's latency is mostly the scheduler's queue, which a busy host stretches: on this machine at load 60–85
//! the wake's mean ran 360–448 µs (p50 63–109 µs) in five runs while the cycle's CPU held at 4.69–4.86 µs
//! (`parkcost`, `benchmark-results/hyper-rt-vs-tokio-20261010/os-parkcost`), and boot calibrations a minute
//! apart sized the spin from 1.9 µs to 577 µs (`runs/base-f5d66a8-r1`). A spin that misses after a
//! latency-sized window burned about eighty cycles' worth of CPU that blocking would have cost, on a host
//! whose other threads needed it: 31.3 µs of CPU per request against tokio's 5.8 µs for the same closed loop.
//!
//! **Learned, not probed once.** The cost depends on the core the shard runs on and its frequency, which an
//! Apple-silicon or hybrid-x86 host changes under the shard; the shard measures the cycles it actually pays.
//! The first [`MIN_SAMPLES`] samples are averaged exactly, and their spread sizes the window of the
//! exponentially weighted mean that follows by the probes' own stopping rule
//! ([`crate::machine::wake::sample_window`]): per-cycle CPU measured with a coefficient of variation of
//! 0.34–0.38 here (`parkcv`) asks for about 222 samples, a window of 256.

use crate::machine::bench::MIN_SAMPLES;
use crate::machine::wake::{WakeEstimate, sample_window, window_shift};

/// An online mean of one cost, nanoseconds: exact over the first [`MIN_SAMPLES`] samples, then exponentially
/// weighted over the window their spread asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParkCost {
    samples: u64,
    /// The warm-up's sum and sum of squares, from which its mean and spread are read.
    sum: u128,
    squares: u128,
    /// The weighted mean, once the warm-up has sized its window.
    tracked: Option<WakeEstimate>,
}

impl ParkCost {
    /// Nothing measured yet.
    pub(crate) const fn new() -> ParkCost {
        ParkCost {
            samples: 0,
            sum: 0,
            squares: 0,
            tracked: None,
        }
    }

    /// Folds one measured cost in.
    pub(crate) fn record(&mut self, cost_ns: u64) {
        self.samples = self.samples.saturating_add(1);
        if let Some(estimate) = self.tracked.as_mut() {
            estimate.record(cost_ns);
            return;
        }
        let cost = u128::from(cost_ns);
        self.sum = self.sum.saturating_add(cost);
        self.squares = self.squares.saturating_add(cost.saturating_mul(cost));
        let warm = u64::try_from(MIN_SAMPLES).unwrap_or(u64::MAX);
        if self.samples >= warm
            && let Some((mean, sd)) = self.warm_mean_and_spread()
        {
            self.tracked = Some(WakeEstimate::new(
                mean,
                window_shift(sample_window(mean, sd)),
            ));
        }
    }

    /// The warm-up's mean and population standard deviation.
    fn warm_mean_and_spread(&self) -> Option<(u64, u64)> {
        let count = u128::from(self.samples);
        let mean = self.sum.checked_div(count)?;
        let variance = self
            .squares
            .checked_div(count)?
            .saturating_sub(mean.saturating_mul(mean));
        Some((
            u64::try_from(mean).ok()?,
            u64::try_from(variance.isqrt()).ok()?,
        ))
    }

    /// The mean cost, nanoseconds; `None` before the first sample.
    pub(crate) fn mean_ns(&self) -> Option<u64> {
        match self.tracked {
            Some(estimate) => Some(estimate.mean_ns()),
            None => self
                .sum
                .checked_div(u128::from(self.samples))
                .and_then(|mean| u64::try_from(mean).ok()),
        }
    }

    /// Samples folded in.
    pub(crate) fn samples(&self) -> u64 {
        self.samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Format: the warm-up's length as a sample count.
    fn warm() -> u64 {
        u64::try_from(MIN_SAMPLES).unwrap()
    }

    #[test]
    fn nothing_measured_is_no_cost_and_the_warm_up_is_an_exact_mean() {
        let mut cost = ParkCost::new();
        assert_eq!(cost.mean_ns(), None);
        cost.record(3_000);
        cost.record(5_000);
        assert_eq!(cost.mean_ns(), Some(4_000));
        assert_eq!(cost.samples(), 2);
    }

    /// A warm-up with no spread asks for the stopping rule's floor of samples, a window of `2^6 = 64` for 59:
    /// the weighted mean then holds a constant exactly and follows a new level, crossing its midpoint about a
    /// window's worth of samples later (an exponential average's half-life, `ln 2 · 64 ≈ 44`).
    #[test]
    fn after_the_warm_up_the_mean_follows_a_new_level_over_its_window() {
        let mut cost = ParkCost::new();
        for _ in 0..warm() {
            cost.record(4_000);
        }
        assert_eq!(cost.mean_ns(), Some(4_000));
        for _ in 0..10 {
            cost.record(4_000);
        }
        assert_eq!(cost.mean_ns(), Some(4_000), "a constant is held exactly");
        let mut crossed = None;
        for step in 1..=1_000u64 {
            cost.record(8_000);
            if crossed.is_none() && cost.mean_ns().unwrap() >= 6_000 {
                crossed = Some(step);
            }
        }
        let crossed = crossed.expect("the mean followed the new level");
        assert!(
            (40..=50).contains(&crossed),
            "crossed the midpoint after {crossed} samples"
        );
        assert!(cost.mean_ns().unwrap() >= 7_990);
    }

    /// A spread warm-up sizes a longer window: the same step crosses its midpoint later than a constant's did.
    #[test]
    fn a_spread_warm_up_learns_over_a_longer_window() {
        let mut cost = ParkCost::new();
        for index in 0..warm() {
            cost.record(if index % 2 == 0 { 2_000 } else { 6_000 });
        }
        let mut crossed = None;
        for step in 1..=10_000u64 {
            cost.record(8_000);
            if crossed.is_none() && cost.mean_ns().unwrap() >= 6_000 {
                crossed = Some(step);
            }
        }
        let crossed = crossed.expect("the mean followed the new level");
        // sd/mean = 0.5 asks for (1.96 · 0.5 / 0.05)² ≈ 385 samples, a window of 512.
        assert!(crossed > 300, "crossed after {crossed} samples");
    }
}
