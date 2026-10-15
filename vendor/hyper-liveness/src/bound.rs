//! The detection bound a receiver can state on its own clock (`docs/timing.md` §2.8).
//!
//! NFD-E suspects the sender at `τ_{h+1} = EA_{h+1} + α` on the receiver's clock, and the sender's
//! last heartbeat was scheduled at `σ_h` on the sender's clock. The time between them is
//! `τ_{h+1} − σ_h − θ`, `θ` the receiver's clock less the sender's, which neither can see; Chen,
//! Toueg and Aguilera bound it by `E(D) + α + η` from a crash, `E(D)` the mean delay of the expected
//! arrival's window, which carries `θ` inside it with unsynchronized clocks.
//!
//! The echo bounds `θ` from below. A heartbeat `j` of the peer's, scheduled at `σ_j` and arriving
//! at `A_j`, carries the echo of one of this node's, from which the receiver has the sum `S_j` of the
//! two directions' delays from their schedules on its own clock (RFC 5905 §8's round trip
//! `δ = (T4 − T1) − (T3 − T2)`, each side's lateness past its schedule added back:
//! `(A_j − s_echo − hold) + late_echo + late_j`). Both delays are positive, so `j`'s own delay,
//! `A_j − σ_j − θ`, is at most `S_j`: `θ ≥ A_j − σ_j − S_j` when `j` came. Between then and the
//! suspicion the clocks move apart by at most their drift, each within RFC 5905's `PHI` of true time
//! (15 ppm, §7.2) over the span its own reading covers, a true span being at most `1/(1 − PHI)` of
//! a clock's. So, measured from the sender's last schedule,
//!
//! ```text
//! τ_{h+1} − σ_h − θ ≤ (τ_{h+1} − σ_h) − (A_j − σ_j − S_j) + PHI/(1 − PHI)·((τ_{h+1} − A_j) + (σ_h − σ_j))
//! ```
//!
//! for every echoed `j`. The best of them is kept: the drift term grows alike for all with the
//! suspicion's times, so the order of `j`s by `A_j − σ_j − S_j + PHI/(1 − PHI)·(A_j + σ_j)` is the
//! order of their bounds at any suspicion, and the greatest is the tightest. No synchronization or
//! path symmetry enters it, and a heartbeat without an echo (the peer's before it heard from this
//! node) leaves it stated. It replaced the mean of the echoed sums over the expected arrival's
//! window, which a single heartbeat without an echo left unstated: a survivor's suspicion of a
//! stalled member, once in twenty runs at one CPU (`docs/benchmarks.md`, "Quiet only while every
//! member is heard"), and which kept a ring of the window's sums a pair, the drift bound's size.

use std::time::Duration;

use hyper_timing::{MILLION, PHI_PER_MILLION};

/// The best lower bound the echoes have put on the clocks' offset, aged so that it compares across
/// heartbeats: `(A_j − σ_j − S_j)·(MILLION − PHI) + PHI·(A_j + σ_j)` for the echoed `j` that makes it
/// greatest, in nanoseconds scaled by `MILLION − PHI` so the drift's fraction stays exact.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Offset {
    best: Option<i128>,
}

/// `MILLION − PHI`: the scale of [`Offset`]'s key.
fn scale() -> i128 {
    i128::from(MILLION.saturating_sub(PHI_PER_MILLION))
}

impl Offset {
    /// A heartbeat of the peer's scheduled at `due_ns` on its clock and stamped `arrival_ns` on this
    /// node's, whose echo gave the delay sum `sum_ns`.
    pub(crate) fn echoed(&mut self, arrival_ns: u64, due_ns: u64, sum_ns: u64) {
        let (arrival, due, sum) = (
            i128::from(arrival_ns),
            i128::from(due_ns),
            i128::from(sum_ns),
        );
        let key = arrival
            .saturating_sub(due)
            .saturating_sub(sum)
            .saturating_mul(scale())
            .saturating_add(
                i128::from(PHI_PER_MILLION).saturating_mul(arrival.saturating_add(due)),
            );
        self.best = Some(self.best.map_or(key, |best| best.max(key)));
    }

    /// The bound on the time from the sender's last schedule, `due_ns` on its clock, to the
    /// freshness point `at_ns` on this node's: `None` before an echo.
    pub(crate) fn detection(&self, at_ns: u64, due_ns: u64) -> Option<Duration> {
        let best = self.best?;
        let (at, due) = (i128::from(at_ns), i128::from(due_ns));
        // (τ − σ_h)·(M − P) − key + P·(τ + σ_h) = ((τ − σ_h) − (A − σ − S))·(M − P)
        // + P·((τ − A) + (σ_h − σ)), over M − P, rounded up: never below the bound.
        let scaled = at
            .saturating_sub(due)
            .saturating_mul(scale())
            .saturating_sub(best)
            .saturating_add(i128::from(PHI_PER_MILLION).saturating_mul(at.saturating_add(due)));
        let bound = scaled
            .max(0)
            .checked_add(scale().saturating_sub(1))?
            .checked_div(scale())?;
        Some(Duration::from_nanos(
            u64::try_from(bound).unwrap_or(u64::MAX),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The bound in exact rationals, from the formula of the module's documentation.
    fn reference(echoes: &[(u64, u64, u64)], at: u64, due: u64) -> Option<u128> {
        let (m, p) = (i128::from(MILLION), i128::from(PHI_PER_MILLION));
        echoes
            .iter()
            .map(|&(arrival, sigma, sum)| {
                // ((τ − σ_h) − (A − σ − S))·(M − P) + P·((τ − A) + (σ_h − σ)).
                let theta = i128::from(arrival) - i128::from(sigma) - i128::from(sum);
                let gap = i128::from(at) - i128::from(due);
                let spans =
                    (i128::from(at) - i128::from(arrival)) + (i128::from(due) - i128::from(sigma));
                (gap - theta) * (m - p) + p * spans
            })
            .min()
            .map(|scaled| {
                u128::try_from(scaled.max(0))
                    .unwrap()
                    .div_ceil(u128::try_from(m - p).unwrap())
            })
    }

    proptest! {
        /// The bound is the least over every echo of the documentation's formula, rounded up, so
        /// the running best is the best at every suspicion.
        #[test]
        fn the_bound_is_the_best_echoes(
            echoes in prop::collection::vec((0u64..1_000_000_000, 0u64..1_000_000_000, 0u64..10_000_000), 1..40),
            at in 1_000_000_000u64..4_000_000_000,
            due in 0u64..1_000_000_000,
        ) {
            let mut offset = Offset::default();
            for &(arrival, sigma, sum) in &echoes {
                offset.echoed(arrival, sigma, sum);
            }
            let expected = reference(&echoes, at, due).map(|ns| Duration::from_nanos(ns as u64));
            prop_assert_eq!(offset.detection(at, due), expected);
        }
    }

    /// On one clock, `θ = 0`, the bound is the time from the schedule to the freshness point plus the
    /// slack of the best echo, `S − D`, and the drift over the spans, never below the time itself.
    #[test]
    fn on_one_clock_the_bound_is_the_gap_and_the_best_slack() {
        let mut offset = Offset::default();
        assert_eq!(offset.detection(10, 0), None, "no echo, no bound");
        // Heartbeat j scheduled at 1 s, arrived 2 ms later; its echo's sum 5 ms: slack 3 ms.
        offset.echoed(1_002_000_000, 1_000_000_000, 5_000_000);
        // The freshness point 1 s after the last schedule at 2 s.
        let bound = offset.detection(3_000_000_000, 2_000_000_000).unwrap();
        // 1 s + 3 ms + ⌈15/(10⁶ − 15)·(1.998 s + 1 s)⌉ = 1.003 s + 44,971 ns.
        assert_eq!(bound, Duration::from_nanos(1_003_000_000 + 44_971));
        assert!(bound >= Duration::from_secs(1));
    }
}
