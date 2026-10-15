//! The measurement harness: run an operation until its bootstrapped interval converges or the caller's
//! wall-time budget ends, batching sub-resolution operations so the clock's own cost cannot move a
//! converged result (slates' `machine::bench`, ORIGIN.md). Every bound is derived below or the caller's;
//! slates' wall budget per probe (250 ms) is now the caller's to state, as calibration takes it.

use std::time::Duration;

use crate::machine::clock::{monotonic_ns, resolution_ns};
use crate::machine::stats::{
    CONVERGED_WIDTH_PERMILLE, Interval, PERMILLE, Percentile, Sample, Xorshift, bootstrap_interval,
    converged,
};

/// Derived: how many times the clock's own cost one reading must span, so that cost is at most half the
/// converged interval's half-width (the most it may move a median that has converged without moving it
/// out of its interval): `2 × 1000 / CONVERGED_WIDTH_PERMILLE`. slates fixed 100 (lmbench's one percent).
pub const TIMER_OVERHEAD_FACTOR: u64 = 2 * PERMILLE / CONVERGED_WIDTH_PERMILLE;

/// Wilks: the smallest sample whose largest value bounds the 95th percentile with 95 % confidence
/// (S. S. Wilks, "Determination of Sample Sizes for Setting Tolerance Limits", Ann. Math. Statist. 12(1),
/// 1941): `n` with `1 − 0.95^n ≥ 0.95`, so 59. The stopping rule accepts no fewer readings. slates took
/// 16 (Kalibera and Jones's minimum repetitions).
pub const MIN_SAMPLES: usize = 59;

/// The result of measuring one operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Measurement {
    /// The 95% bootstrap interval around the median, in nanoseconds per operation.
    pub interval: Interval,
    /// The p99 reading, in nanoseconds per operation.
    pub p99_ns: u64,
    /// The smallest reading, in nanoseconds per operation.
    pub min_ns: u64,
    /// How many samples were taken.
    pub samples: u32,
    /// How many operations each sample batched.
    pub batch: u32,
    /// True when the wall-time bound stopped the probe before its interval converged.
    pub quick: bool,
}

impl Measurement {
    /// The median in nanoseconds per operation.
    pub const fn median_ns(&self) -> u64 {
        self.interval.median
    }
}

/// Measures the cost of reading the monotonic clock itself, in nanoseconds per read: back-to-back reads
/// until they span [`TIMER_OVERHEAD_FACTOR`] times the clock's resolution, so the resolution is a stated
/// fraction of the span. Bounded: every read takes at least a nanosecond, so the reads never outnumber
/// the span's nanoseconds.
pub fn timer_overhead_ns() -> u64 {
    let span = resolution_ns().saturating_mul(TIMER_OVERHEAD_FACTOR).max(1);
    let start = monotonic_ns();
    let mut last = start;
    let mut reads: u64 = 0;
    while last.saturating_sub(start) < span && reads < span {
        last = monotonic_ns();
        reads = reads.saturating_add(1);
    }
    last.saturating_sub(start).checked_div(reads).unwrap_or(0)
}

/// Nanoseconds in a duration, saturating at `u64::MAX` (no narrowing cast).
pub fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// Runs `op` until the interval around its per-operation median converges or `budget` ends. `op` is
/// batched so that one sample lasts at least [`TIMER_OVERHEAD_FACTOR`] times the measured clock cost.
pub fn measure<F: FnMut()>(mut op: F, budget: Duration) -> Measurement {
    let overhead = timer_overhead_ns();
    let floor = overhead.saturating_mul(TIMER_OVERHEAD_FACTOR).max(1);
    let started = monotonic_ns();
    let budget_ns = nanos(budget);
    let mut batch: u32 = 1;
    let mut sample = Sample::new(Vec::new());
    let mut rng = Xorshift::new(Xorshift::SEED);
    loop {
        let t = monotonic_ns();
        for _ in 0..batch {
            op();
        }
        let elapsed = monotonic_ns().saturating_sub(t);
        let per_op = elapsed.checked_div(u64::from(batch)).unwrap_or(0);
        if elapsed < floor && batch < u32::MAX / 2 {
            batch = batch.saturating_mul(2);
            continue;
        }
        sample.push(per_op);
        if elapsed < floor {
            // The operation is unmeasurable at any batch (it was optimized away or costs nothing); report what
            // was seen, marked quick, rather than a converged zero.
            break;
        }
        if sample.len() >= MIN_SAMPLES
            && let Some(interval) = bootstrap_interval(&sample, &mut rng)
            && converged(&interval)
        {
            return finish(&sample, interval, batch, false);
        }
        if monotonic_ns().saturating_sub(started) >= budget_ns {
            break;
        }
    }
    let interval = bootstrap_interval(&sample, &mut rng).unwrap_or(Interval {
        median: 0,
        lower: 0,
        upper: 0,
    });
    finish(&sample, interval, batch, true)
}

fn finish(sample: &Sample, interval: Interval, batch: u32, quick: bool) -> Measurement {
    Measurement {
        interval,
        p99_ns: sample.percentile(Percentile::P99).unwrap_or(0),
        min_ns: sample.min().unwrap_or(0),
        samples: u32::try_from(sample.len()).unwrap_or(u32::MAX),
        batch,
        quick,
    }
}

/// Throughput in bytes per second from bytes processed and nanoseconds elapsed; `u128` arithmetic
/// so the product never overflows, saturating on a zero elapsed time.
pub fn bytes_per_second(bytes: u64, elapsed_ns: u64) -> u64 {
    if elapsed_ns == 0 {
        return u64::MAX;
    }
    /// Format: nanoseconds per second.
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let bps = u128::from(bytes)
        .saturating_mul(NANOS_PER_SECOND)
        .checked_div(u128::from(elapsed_ns))
        .unwrap_or(u128::MAX);
    u64::try_from(bps).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measuring_a_trivial_operation_batches_and_converges_or_stops_at_the_budget() {
        let mut counter = 0u64;
        let m = measure(
            || counter = counter.wrapping_add(1),
            Duration::from_millis(200),
        );
        assert!(m.batch >= 1);
        assert!(m.samples >= 1);
        assert!(m.interval.lower <= m.interval.median && m.interval.median <= m.interval.upper);
        assert!(counter > 0);
    }

    #[test]
    // A test may sleep: the wall bound around a slow operation is what is under test.
    #[allow(clippy::disallowed_methods)]
    fn the_budget_bounds_a_slow_operation_and_marks_it_quick() {
        let m = measure(
            || std::thread::sleep(Duration::from_millis(30)),
            Duration::from_millis(60),
        );
        assert!(m.quick, "{m:?}");
        assert!(m.median_ns() >= 30_000_000, "{m:?}");
    }

    #[test]
    fn throughput_arithmetic_never_overflows() {
        assert_eq!(
            bytes_per_second(1_000_000_000, 1_000_000_000),
            1_000_000_000
        );
        assert_eq!(bytes_per_second(u64::MAX, 1), u64::MAX);
        assert_eq!(bytes_per_second(1, 0), u64::MAX);
    }
}
