//! When a session's preferred leader moves (27 §5, stage F).
//!
//! The directory says where a session would be better led
//! (`focal_directory::leader_move`): at another of its voters that leads
//! two sessions fewer. This says when the controller acts on it. A move is
//! a placement plan like any other: every copy verifies its custody under
//! the new route and the session is cut over, so it is made for an
//! imbalance that lasts and never for one that passes:
//!
//! - the same move must be what the directory says on `observations`
//!   passes in a row, over `hold_secs` at least; a pass on which it says
//!   otherwise starts the count again;
//! - one move is under way in a partition at a time, and a move made
//!   starts every count again, so two moves are `hold_secs` apart at least
//!   and each is decided on what the one before it left.
//!
//! What is counted is kept per session, to a bound; a session past it is
//! not observed until another lapses, and is counted as such.
use focal_model::LedgerId;
use std::collections::BTreeMap;

/// `off` keeps every preferred leader where it is: the planner spreads
/// none and the controller moves none.
pub const BALANCE_ENV: &str = "FOCAL_LEADER_BALANCE";
/// The operator's hold, in seconds; a campaign lowers it so a small fleet
/// spreads while it is watched.
pub const HOLD_ENV: &str = "FOCAL_LEADER_BALANCE_HOLD_SECS";
/// The most sessions observed at once.
const MAX_OBSERVED: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderBalancerConfig {
    /// Whether preferred leaders are spread at all.
    pub enabled: bool,
    /// Passes in a row the same move must be due.
    pub observations: u8,
    /// How long it must have been, in seconds: a load report's interval by
    /// default, so a move is decided on a report made since it was due.
    pub hold_secs: i64,
}
impl Default for LeaderBalancerConfig {
    fn default() -> Self {
        Self::standard()
    }
}
impl LeaderBalancerConfig {
    pub const fn standard() -> Self {
        Self {
            enabled: true,
            observations: 3,
            hold_secs: 30,
        }
    }
    /// The standard configuration under the operator's hold, if set.
    pub fn from_env() -> Self {
        let mut config = Self::standard();
        if std::env::var_os(BALANCE_ENV)
            .is_some_and(|value| value.to_str().is_some_and(|text| text.trim() == "off"))
        {
            config.enabled = false;
        }
        if let Some(value) = std::env::var_os(HOLD_ENV)
            && let Some(hold) = value
                .to_str()
                .and_then(|text| text.trim().parse::<i64>().ok())
            && hold >= 0
        {
            config.hold_secs = hold;
        }
        config
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Observed {
    target: u64,
    since: i64,
    count: u8,
}
/// What the balancer did, for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LeaderBalancerStats {
    /// Moves made.
    pub moved: u64,
    /// Observations not kept for the bound on them.
    pub unobserved: u64,
}

#[derive(Debug, Default)]
pub struct LeaderBalancer {
    config: LeaderBalancerConfig,
    seen: BTreeMap<LedgerId, Observed>,
    stats: LeaderBalancerStats,
}
impl LeaderBalancer {
    pub fn new(config: LeaderBalancerConfig) -> Self {
        Self {
            config,
            seen: BTreeMap::new(),
            stats: LeaderBalancerStats::default(),
        }
    }
    pub fn config(&self) -> LeaderBalancerConfig {
        self.config
    }
    pub fn stats(&self) -> LeaderBalancerStats {
        self.stats
    }
    /// Sessions observed now.
    pub fn observed(&self) -> usize {
        self.seen.len()
    }
    /// One pass over one session: where the directory says it would be
    /// better led, if anywhere. Whether the move is due.
    pub fn observe(&mut self, ledger: LedgerId, target: Option<u64>, now: i64) -> bool {
        let Some(target) = target.filter(|_| self.config.enabled) else {
            self.seen.remove(&ledger);
            return false;
        };
        let observed = match self.seen.get_mut(&ledger) {
            // A clock that went back measures nothing: the hold begins
            // again.
            Some(observed) if observed.target == target && observed.since <= now => {
                observed.count = observed.count.saturating_add(1);
                *observed
            }
            Some(observed) => {
                *observed = Observed {
                    target,
                    since: now,
                    count: 1,
                };
                *observed
            }
            None => {
                if self.seen.len() >= MAX_OBSERVED {
                    self.stats.unobserved = self.stats.unobserved.saturating_add(1);
                    return false;
                }
                let observed = Observed {
                    target,
                    since: now,
                    count: 1,
                };
                self.seen.insert(ledger, observed);
                observed
            }
        };
        observed.count >= self.config.observations
            && now.saturating_sub(observed.since) >= self.config.hold_secs
    }
    /// A move was made: every count begins again.
    pub fn moved(&mut self) {
        self.seen.clear();
        self.stats.moved = self.stats.moved.saturating_add(1);
    }
    /// Sessions that left take their observations with them.
    pub fn retain(&mut self, mut stays: impl FnMut(&LedgerId) -> bool) {
        self.seen.retain(|ledger, _| stays(ledger));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger(session: u128) -> LedgerId {
        LedgerId {
            tenant: focal_model::TenantId::from_u128(1),
            session: focal_model::SessionId::from_u128(session),
        }
    }
    fn balancer() -> LeaderBalancer {
        LeaderBalancer::new(LeaderBalancerConfig {
            enabled: true,
            observations: 3,
            hold_secs: 1_000,
        })
    }

    #[test]
    fn a_move_is_due_once_it_was_due_for_long_enough_and_often_enough() {
        let mut balancer = balancer();
        // Often enough and not long enough.
        for now in [0, 10, 20, 30, 999] {
            assert!(!balancer.observe(ledger(1), Some(2), now), "{now}");
        }
        assert!(balancer.observe(ledger(1), Some(2), 1_000));
        // Long enough and not often enough.
        let mut balancer = self::balancer();
        assert!(!balancer.observe(ledger(1), Some(2), 0));
        assert!(!balancer.observe(ledger(1), Some(2), 5_000));
        assert!(balancer.observe(ledger(1), Some(2), 5_001));
    }
    #[test]
    fn a_pass_that_says_otherwise_starts_the_count_again() {
        let mut balancer = balancer();
        assert!(!balancer.observe(ledger(1), Some(2), 0));
        assert!(!balancer.observe(ledger(1), Some(2), 600));
        // Nothing to move for.
        assert!(!balancer.observe(ledger(1), None, 900));
        assert_eq!(balancer.observed(), 0);
        assert!(!balancer.observe(ledger(1), Some(2), 1_200));
        assert!(!balancer.observe(ledger(1), Some(2), 1_800));
        assert!(!balancer.observe(ledger(1), Some(2), 2_100));
        assert!(balancer.observe(ledger(1), Some(2), 2_200));
        // Another node to move to.
        assert!(!balancer.observe(ledger(1), Some(3), 2_300));
        assert!(!balancer.observe(ledger(1), Some(3), 3_299));
        assert!(balancer.observe(ledger(1), Some(3), 3_300));
        // A clock that went back.
        assert!(!balancer.observe(ledger(1), Some(3), 100));
        assert!(!balancer.observe(ledger(1), Some(3), 1_099));
        assert!(balancer.observe(ledger(1), Some(3), 1_100));
    }
    #[test]
    fn a_balancer_that_is_off_moves_nothing() {
        let mut balancer = LeaderBalancer::new(LeaderBalancerConfig {
            enabled: false,
            observations: 1,
            hold_secs: 0,
        });
        for now in 0..100 {
            assert!(!balancer.observe(ledger(1), Some(2), now));
        }
        assert_eq!(balancer.observed(), 0);
        assert!(LeaderBalancerConfig::standard().enabled);
        assert_eq!(LeaderBalancerConfig::standard().hold_secs, 30);
    }
    #[test]
    fn a_move_made_starts_every_count_again() {
        let mut balancer = balancer();
        for now in [0, 500, 1_000] {
            let _ = balancer.observe(ledger(1), Some(2), now);
            let _ = balancer.observe(ledger(2), Some(2), now);
        }
        assert!(balancer.observe(ledger(1), Some(2), 1_001));
        balancer.moved();
        assert_eq!(balancer.stats().moved, 1);
        // The other was as due, and waits for what the move left.
        assert!(!balancer.observe(ledger(2), Some(2), 1_002));
        assert!(!balancer.observe(ledger(2), Some(2), 1_500));
        assert!(!balancer.observe(ledger(2), Some(2), 2_001));
        assert!(balancer.observe(ledger(2), Some(2), 2_002));
    }
    #[test]
    fn what_is_observed_has_a_bound_and_lapses_with_its_session() {
        let mut balancer = balancer();
        for session in 0..MAX_OBSERVED as u128 + 10 {
            assert!(!balancer.observe(ledger(session), Some(2), 0));
        }
        assert_eq!(balancer.observed(), MAX_OBSERVED);
        assert_eq!(balancer.stats().unobserved, 10);
        // One that is observed is counted on at the bound.
        assert!(!balancer.observe(ledger(1), Some(2), 1_000));
        assert!(balancer.observe(ledger(1), Some(2), 1_001));
        balancer.retain(|ledger| ledger.session.0[15] % 2 == 0);
        assert!(balancer.observed() <= MAX_OBSERVED / 2 + 1);
        assert!(!balancer.observe(ledger(MAX_OBSERVED as u128 + 1), Some(2), 0));
        assert_eq!(balancer.stats().unobserved, 10);
        // At their bounds the counters stay.
        let mut balancer = self::balancer();
        for _ in 0..1_000 {
            let _ = balancer.observe(ledger(1), Some(2), i64::MAX);
        }
        assert!(!balancer.observe(ledger(1), Some(2), i64::MAX));
        assert!(!balancer.observe(ledger(2), Some(2), i64::MIN));
        assert!(!balancer.observe(ledger(2), Some(2), i64::MAX));
        assert!(balancer.observe(ledger(2), Some(2), i64::MAX));
    }
}
