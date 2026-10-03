//! The detection bound a receiver can state on its own clock (`docs/timing.md` §2.8).
//!
//! NFD-E suspects the sender at `τ_{h+1} = EA_{h+1} + α`, and `EA_{h+1}` is the sender's schedule
//! `σ_{h+1}` plus the mean delay of the window's heartbeats and the two clocks' offset `θ`, which
//! the receiver cannot see. Chen, Toueg and Aguilera bound detection by `E(D) + α + η` from the
//! crash; with unsynchronized clocks NFD-E's analysis needs `E(D)` only for that bound, and
//! `LinkEstimator` feeds the configurator a mean delay of zero (`hyper_timing::link`).
//!
//! The echo gives the receiver the sum of the two directions' delays from each one's schedule, on
//! its own clock: its heartbeat `j` scheduled at `σ^q_j` arrived at the peer at `r^p_j`, and the
//! peer's heartbeat `k` scheduled at `σ^p_k` arrived at `A^q_k`, so
//! `D_qp,j + D_pq,k = (A^q_k − s^q_j − hold) + late^q_j + late^p_k`, in which every term is a
//! difference on one clock (RFC 5905 §8's round-trip delay `δ = (T4 − T1) − (T3 − T2)`, with each
//! side's lateness past its schedule added back). Both delays are positive, so that sum bounds
//! `D_pq,k`, and the mean of the sums over the heartbeats in the expected arrival's window bounds
//! the mean of their delays. Hence, measured from the sender's last schedule,
//! `τ_{h+1} − σ_h = η + α + mean(D_window) ≤ η + α + mean(sums over the window)`: the bound
//! [`Sums::detection`] states, which no assumption about the path's symmetry enters, and which a
//! crash at or after the last send cannot exceed.

use std::time::Duration;

/// The longest sum one heartbeat may add: past it a window of `WINDOW_LIMIT` of them would not fit
/// the ring's wrapping `u64` sums, and the sum is held at it, so the bound only loosens.
const SUM_LIMIT: u64 = u64::MAX / hyper_timing::WINDOW_LIMIT;

/// Prefix sums of the delay sums of the heartbeats an estimator took, in the order it took them,
/// with the count of heartbeats that carried no echo: the same ring the estimator keeps of its
/// offsets, so a window of it is the estimator's window.
#[derive(Clone, Debug)]
pub(crate) struct Sums {
    /// `[sum, unknown]` after each heartbeat taken, wrapping: a window's values are differences
    /// of two, exact while they fit (`SUM_LIMIT`).
    prefix: Vec<[u64; 2]>,
    taken: u64,
}

impl Sums {
    /// A ring for windows of up to `capacity` heartbeats: allocated here, once.
    pub(crate) fn new(capacity: u64) -> Self {
        let slots = usize::try_from(capacity.saturating_add(1)).unwrap_or(usize::MAX);
        Self {
            prefix: vec![[0; 2]; slots],
            taken: 0,
        }
    }

    /// The window started again at `capacity`: in place where the ring holds it already, as the
    /// estimator's own ring is (`LinkEstimator::retime`).
    pub(crate) fn restart(&mut self, capacity: u64) {
        let slots = usize::try_from(capacity.saturating_add(1)).unwrap_or(usize::MAX);
        self.prefix.clear();
        self.prefix.resize(slots, [0; 2]);
        self.taken = 0;
    }

    fn slots(&self) -> u64 {
        u64::try_from(self.prefix.len()).unwrap_or(u64::MAX).max(1)
    }

    fn at(&self, count: u64) -> [u64; 2] {
        usize::try_from(count.checked_rem(self.slots()).unwrap_or(0))
            .ok()
            .and_then(|at| self.prefix.get(at))
            .copied()
            .unwrap_or([0; 2])
    }

    /// The heartbeat the estimator just took: its delay sum, or `None` when it carried no echo.
    pub(crate) fn push(&mut self, sum: Option<u64>) {
        let [total, unknown] = self.at(self.taken);
        let next = self.taken.saturating_add(1);
        let entry = match sum {
            Some(sum) => [total.wrapping_add(sum.min(SUM_LIMIT)), unknown],
            None => [total, unknown.wrapping_add(1)],
        };
        let slots = self.slots();
        if let Some(slot) = usize::try_from(next.checked_rem(slots).unwrap_or(0))
            .ok()
            .and_then(|at| self.prefix.get_mut(at))
        {
            *slot = entry;
        }
        self.taken = next;
    }

    /// `η + α + ⌈mean of the window's sums⌉` for the latest `window` heartbeats: the bound on the
    /// time from the sender's last schedule to the suspicion. `None` while a heartbeat in the
    /// window carried no echo, or before any.
    pub(crate) fn detection(
        &self,
        window: u64,
        interval: Duration,
        margin: Duration,
    ) -> Option<Duration> {
        let length = window.min(self.taken).min(self.slots().saturating_sub(1));
        if length == 0 {
            return None;
        }
        let [newest, newest_unknown] = self.at(self.taken);
        let [oldest, oldest_unknown] = self.at(self.taken.saturating_sub(length));
        if newest_unknown.wrapping_sub(oldest_unknown) != 0 {
            return None;
        }
        // Rounded up: the expected arrival's mean is truncated to the nanosecond, and the bound
        // must not fall below it.
        let mean = newest.wrapping_sub(oldest).div_ceil(length);
        Some(
            interval
                .saturating_add(margin)
                .saturating_add(Duration::from_nanos(mean)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// The window's mean is the mean of the latest pushes, rounded up, and any push without an
        /// echo in the window leaves the bound unstated.
        #[test]
        fn the_bound_is_the_windows_mean(
            pushes in prop::collection::vec(prop::option::weighted(0.9, 0u64..1_000_000_000), 1..300),
            capacity in 1u64..64,
            window in 1u64..80,
        ) {
            let mut sums = Sums::new(capacity);
            for push in &pushes {
                sums.push(*push);
            }
            let length = (window.min(capacity) as usize).min(pushes.len());
            let latest = &pushes[pushes.len() - length..];
            let expected = if latest.iter().all(Option::is_some) {
                let total: u64 = latest.iter().map(|s| s.unwrap()).sum();
                Some(Duration::from_nanos(10 + 3 + total.div_ceil(length as u64)))
            } else {
                None
            };
            prop_assert_eq!(
                sums.detection(window, Duration::from_nanos(10), Duration::from_nanos(3)),
                expected
            );
        }
    }
}
