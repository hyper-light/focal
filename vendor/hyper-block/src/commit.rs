//! Group commit's wait for the submitters a batch just answered (docs/research/03 rule G6;
//! docs/research/11 §3.3).
//!
//! A writer that forms each batch at once from whatever is queued lets a few closed-loop
//! submitters alternate: those answered last time miss the next batch, and every request
//! waits for two flushes (docs/measurements/2026-09-28-chunk-store-benchmark.md, finding 5).
//! The wait that fixes this is derived, not chosen. With `n` requests in the batch, waiting a
//! further `dw` for one more that arrives with probability `p` costs the batch `n·dw` of
//! latency and saves the newcomer about `S − dw`, where `S` is the batch's service time (its
//! writes and flush), because a request that misses a batch waits for that batch to finish
//! before its own starts. The expected total latency falls while `dw < p·S / (n + p)`, so that
//! is how long the writer waits for each next request, and only while submitters it just
//! answered have yet to return. `S` is measured on every batch, and `p` is learned as the
//! share of answered submitters that return while the writer waits, a ratio of running counts
//! so that each submitter weighs the same. Both decay with the gain of 1/8 that TCP uses for
//! its round-trip estimate (Jacobson, SIGCOMM 1988; RFC 6298 §2), which holds an estimate
//! within 5% for samples varying by 20% and closes 95% of a step in 23 batches.
//!
//! The rule is exact while submitters return well within `S`, the case measured, one newcomer
//! counts at a time, and a batch's service time does not grow with one more request, which
//! holds while the flush dominates. Submitters that take longer than `S/2` are never waited
//! for, even where waiting would lower total latency; the rule that covers them weighs, over
//! the measured distribution of return times, every returner a wait would gather against the
//! delay to the batch and to arrivals behind it, as anticipatory scheduling does (Iyer and
//! Druschel, SOSP 2001, §3.3; docs/research/11 §2.3, §2.6).

use std::time::Duration;

/// The unit of the return counts and of `p`: one.
pub const RATE_ONE: u64 = 1 << 16;

/// What a group-commit writer has learned of its batches and submitters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Anticipation {
    /// Moving average of a batch's service time, in nanoseconds; zero until one is measured.
    service_ns: u64,
    /// Running counts, in 1/2^16ths and decaying by 1/8 a batch, of answered submitters that
    /// returned while the writer waited and of submitters answered.
    returns: (u64, u64),
}

impl Default for Anticipation {
    fn default() -> Self {
        Self::new()
    }
}

impl Anticipation {
    /// Nothing measured: no wait until a batch's service time is, and a share of one in two,
    /// Laplace's rule of succession for a share never observed.
    pub const fn new() -> Self {
        Self {
            service_ns: 0,
            returns: (RATE_ONE, RATE_ONE << 1),
        }
    }

    /// The moving average of a batch's service time, in nanoseconds.
    pub fn service_ns(&self) -> u64 {
        self.service_ns
    }

    /// Records a batch's service time: its writes and its flush.
    pub fn served(&mut self, took_ns: u64) {
        self.service_ns = if self.service_ns == 0 {
            took_ns
        } else {
            smooth(self.service_ns, took_ns)
        };
    }

    /// How long to wait for one more of the submitters just answered while the batch holds
    /// `n` requests: `p·S / (n + p)`. `None` when nothing is measured yet or the wait rounds
    /// to nothing.
    pub fn wait(&self, n: usize) -> Option<Duration> {
        if self.service_ns == 0 {
            return None;
        }
        let n = u128::try_from(n).unwrap_or(u128::MAX);
        let p = u128::from(self.return_share());
        let step = p
            .saturating_mul(u128::from(self.service_ns))
            .checked_div(n.saturating_mul(u128::from(RATE_ONE)).saturating_add(p))
            .and_then(|ns| u64::try_from(ns).ok())
            .unwrap_or(0);
        (step > 0).then(|| Duration::from_nanos(step))
    }

    /// Learns from one gathering: of `answered` submitters the last batch answered,
    /// `returned` came back while the writer waited.
    pub fn learn(&mut self, answered: u64, returned: u64) {
        let came = returned.min(answered).saturating_mul(RATE_ONE);
        self.returns = (
            smooth(self.returns.0, came.saturating_mul(8)),
            smooth(
                self.returns.1,
                answered.saturating_mul(RATE_ONE).saturating_mul(8),
            ),
        );
    }

    /// `p`: the share of answered submitters that return while the writer waits.
    fn return_share(&self) -> u64 {
        self.returns
            .0
            .saturating_mul(RATE_ONE)
            .checked_div(self.returns.1)
            .unwrap_or(RATE_ONE / 2)
            .min(RATE_ONE)
    }
}

/// One step of a moving average with gain 1/8 (RFC 6298 §2).
pub fn smooth(average: u64, sample: u64) -> u64 {
    average
        .saturating_sub(average / 8)
        .saturating_add(sample / 8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wait_grows_with_the_learned_share_and_shrinks_with_the_batch() {
        let mut a = Anticipation::new();
        assert_eq!(a.wait(1), None, "nothing measured yet");
        a.served(4_000_000);
        // p = 1/2 at first: 0.5 · 4 ms / (1 + 0.5).
        assert_eq!(a.wait(1), Some(Duration::from_nanos(1_333_333)));
        assert!(a.wait(4) < a.wait(1));
        for _ in 0..50 {
            a.learn(4, 4);
        }
        let eager = a.wait(1);
        for _ in 0..50 {
            a.learn(4, 0);
        }
        assert!(a.wait(1) < eager);
        // Submitters that stop returning are soon waited for hardly at all.
        assert!(
            a.wait(1) < Some(Duration::from_micros(40)),
            "{:?}",
            a.wait(1)
        );
    }

    #[test]
    fn the_service_time_is_a_moving_average() {
        let mut a = Anticipation::new();
        a.served(8_000);
        assert_eq!(a.service_ns(), 8_000);
        a.served(16_000);
        assert_eq!(a.service_ns(), 9_000);
    }
}
