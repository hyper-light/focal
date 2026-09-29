//! A wait charged to the progress of what it waits on (27 §3.1 P8).
//!
//! A group converges in a number of its own periods. Under load its owners
//! run fewer periods per wall second, so a wall-clock deadline measures the
//! machine and not the group. A [`ProgressDeadline`] is spent in periods:
//! each observed owner's periods since the wait began, charged at the least
//! of them, so one starved owner holds the wait open and an owner that began
//! far ahead never pays for one that stalled. The only wall-clock bound is
//! the frozen window: the slowest owner ran no period at all for that long,
//! which is a wedge and not slowness.
use std::time::{Duration, Instant};

/// Why a wait ended without its condition holding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Spent {
    /// Every observed owner ran the whole budget of its own periods.
    Budget { periods: u64 },
    /// The slowest observed owner ran no period for the frozen window.
    Frozen { periods: u64, stalled: Duration },
}
impl std::fmt::Display for Spent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Budget { periods } => {
                write!(formatter, "every owner ran its budget of {periods} periods")
            }
            Self::Frozen { periods, stalled } => write!(
                formatter,
                "the slowest owner ran no period for {stalled:?} (after {periods})"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProgressDeadline {
    began: Vec<u64>,
    budget: u64,
    frozen: Duration,
    charged: u64,
    advanced: Instant,
}
impl ProgressDeadline {
    /// `counters` are the observed owners' period counts now, `budget` the
    /// periods each may run, and `frozen` how long the slowest may run none.
    pub fn begin(counters: &[u64], budget: u64, frozen: Duration) -> Self {
        Self::begin_at(counters, budget, frozen, Instant::now())
    }
    pub fn begin_at(counters: &[u64], budget: u64, frozen: Duration, now: Instant) -> Self {
        Self {
            began: counters.to_vec(),
            budget,
            frozen,
            charged: 0,
            advanced: now,
        }
    }
    /// The periods a wall-clock allowance holds at a tick period: what a
    /// deadline written in seconds meant on an idle machine.
    pub fn periods(allowance: Duration, period: Duration) -> u64 {
        let period = period.as_nanos().max(1);
        u64::try_from(allowance.as_nanos().div_ceil(period))
            .unwrap_or(u64::MAX)
            .max(1)
    }
    /// The least advance of any observed owner since the wait began. An
    /// owner whose count went backwards was replaced: its advance is its
    /// whole new count.
    fn least(&self, counters: &[u64]) -> u64 {
        self.began
            .iter()
            .zip(counters)
            .map(|(began, now)| now.checked_sub(*began).unwrap_or(*now))
            .min()
            .unwrap_or(0)
    }
    pub fn check(&mut self, counters: &[u64]) -> Result<(), Spent> {
        self.check_at(counters, Instant::now())
    }
    /// `counters` in the order given to `begin`. No observed owner leaves
    /// only the frozen window.
    pub fn check_at(&mut self, counters: &[u64], now: Instant) -> Result<(), Spent> {
        let least = self.least(counters);
        if least != self.charged {
            self.charged = least;
            self.advanced = now;
        }
        if !self.began.is_empty() && least >= self.budget {
            return Err(Spent::Budget { periods: least });
        }
        let stalled = now.saturating_duration_since(self.advanced);
        if stalled >= self.frozen {
            return Err(Spent::Frozen {
                periods: least,
                stalled,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const FROZEN: Duration = Duration::from_secs(30);

    #[test]
    fn a_slow_fleet_is_charged_its_periods_and_not_the_wall_clock() {
        let start = Instant::now();
        let mut wait = ProgressDeadline::begin_at(&[10, 10, 10], 100, FROZEN, start);
        // An hour of wall time in which every owner keeps running periods,
        // slowly: twenty seconds a period, fifty periods. Never spent.
        for step in 1..=50_u64 {
            let now = start + Duration::from_secs(20 * step);
            let counters = [10 + step, 10 + step, 10 + step];
            assert_eq!(wait.check_at(&counters, now), Ok(()));
        }
        let now = start + Duration::from_secs(3600);
        assert_eq!(
            wait.check_at(&[110, 110, 110], now),
            Err(Spent::Budget { periods: 100 })
        );
    }
    #[test]
    fn the_slowest_owner_holds_the_wait_open() {
        let start = Instant::now();
        let mut wait = ProgressDeadline::begin_at(&[0, 0, 0], 100, FROZEN, start);
        // Two owners ran ten budgets; the third is starved but moving.
        let now = start + Duration::from_secs(10);
        assert_eq!(wait.check_at(&[1000, 1000, 3], now), Ok(()));
        let now = start + Duration::from_secs(20);
        assert_eq!(wait.check_at(&[2000, 2000, 99], now), Ok(()));
        let now = start + Duration::from_secs(21);
        assert_eq!(
            wait.check_at(&[2001, 2001, 100], now),
            Err(Spent::Budget { periods: 100 })
        );
    }
    #[test]
    fn an_owner_that_began_ahead_does_not_pay_for_one_that_stalled() {
        let start = Instant::now();
        // The least absolute count would charge 5; the least advance is 0.
        let mut wait = ProgressDeadline::begin_at(&[5000, 5], 100, FROZEN, start);
        let now = start + Duration::from_secs(1);
        assert_eq!(wait.check_at(&[5200, 5], now), Ok(()));
        assert_eq!(wait.least(&[5200, 5]), 0);
    }
    #[test]
    fn an_owner_that_runs_no_period_is_frozen_after_the_window() {
        let start = Instant::now();
        let mut wait = ProgressDeadline::begin_at(&[7, 7], 100, FROZEN, start);
        let now = start + Duration::from_secs(29);
        assert_eq!(wait.check_at(&[90, 7], now), Ok(()));
        let now = start + Duration::from_secs(30);
        assert_eq!(
            wait.check_at(&[95, 7], now),
            Err(Spent::Frozen {
                periods: 0,
                stalled: Duration::from_secs(30)
            })
        );
    }
    #[test]
    fn progress_restarts_the_frozen_window() {
        let start = Instant::now();
        let mut wait = ProgressDeadline::begin_at(&[0], 100, FROZEN, start);
        assert_eq!(wait.check_at(&[0], start + Duration::from_secs(29)), Ok(()));
        assert_eq!(wait.check_at(&[1], start + Duration::from_secs(31)), Ok(()));
        assert_eq!(wait.check_at(&[1], start + Duration::from_secs(60)), Ok(()));
        assert!(matches!(
            wait.check_at(&[1], start + Duration::from_secs(61)),
            Err(Spent::Frozen { periods: 1, .. })
        ));
    }
    #[test]
    fn a_replaced_owner_is_charged_its_new_count() {
        let start = Instant::now();
        let mut wait = ProgressDeadline::begin_at(&[500], 100, FROZEN, start);
        // Reopened: its count restarted below where the wait began.
        assert_eq!(wait.check_at(&[40], start + Duration::from_secs(1)), Ok(()));
        assert_eq!(wait.least(&[40]), 40);
        assert_eq!(
            wait.check_at(&[100], start + Duration::from_secs(2)),
            Err(Spent::Budget { periods: 100 })
        );
    }
    #[test]
    fn no_observed_owner_leaves_only_the_frozen_window() {
        let start = Instant::now();
        let mut wait = ProgressDeadline::begin_at(&[], 1, FROZEN, start);
        assert_eq!(wait.check_at(&[], start + Duration::from_secs(29)), Ok(()));
        assert!(matches!(
            wait.check_at(&[], start + Duration::from_secs(30)),
            Err(Spent::Frozen { periods: 0, .. })
        ));
    }
    #[test]
    fn an_allowance_is_the_periods_it_held_when_idle() {
        let tick = Duration::from_millis(20);
        assert_eq!(
            ProgressDeadline::periods(Duration::from_secs(10), tick),
            500
        );
        assert_eq!(ProgressDeadline::periods(Duration::from_millis(1), tick), 1);
        assert_eq!(ProgressDeadline::periods(Duration::ZERO, tick), 1);
        assert_eq!(
            ProgressDeadline::periods(Duration::from_secs(1), Duration::ZERO),
            1_000_000_000
        );
    }
}
