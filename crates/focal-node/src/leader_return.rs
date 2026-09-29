//! Leadership returns to where placement put it (27 §5, stage F).
//!
//! The committed placement names a session's preferred leader, and the
//! election priority already keeps a voter that merely timed out first from
//! taking leadership while the preferred one asks for it. Priority decides
//! elections; it does not start one. After the preferred leader was away
//! and came back, another voter leads, and nothing moves leadership until
//! that voter fails. This is the rule that moves it: the voter that leads
//! hands leadership to the preferred leader once that member has stayed
//! current for a while.
//!
//! The decision is a function of what the leader observes on its own ticks.
//! It keeps no clock, and everything it counts is bounded:
//!
//! - **Fit.** The preferred leader must replicate without probing, hold
//!   everything committed, and have answered since the leader last checked
//!   its quorum, for `FIT` election timeouts in a row. A member that has
//!   just returned is not asked to lead while it may leave again.
//! - **Quiet.** The hand-over waits for a moment with nothing proposed and
//!   undecided, since a leader refuses proposals while it hands over. Under
//!   load that never pauses it stops waiting after `PATIENCE` election
//!   timeouts, so load cannot keep leadership away for ever.
//! - **Rest.** After asking, the leader does not ask again for `REST`
//!   election timeouts. A hand-over that was abandoned, or one after which
//!   leadership came back to this member, begins a rest twice as long, up
//!   to `DOUBLINGS` doublings. A preferred leader that cannot keep leadership
//!   costs the group one election per rest, never a storm of them.
//!
//! A change of configuration or placement in progress, a hand-over someone
//! else began, and a member that is stopping all hold the decision.

/// Election timeouts the preferred leader stays current before it is asked.
pub const FIT: u32 = 2;
/// Election timeouts between two asks.
pub const REST: u32 = 4;
/// Election timeouts after which a hand-over no longer waits for a quiet
/// moment.
pub const PATIENCE: u32 = 8;
/// The most times the rest doubles.
pub const DOUBLINGS: u32 = 6;

/// What the leader observes on one tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Seen {
    /// This member leads and has committed in its term.
    pub leads: bool,
    /// The preferred leader, when it is another member that votes in both
    /// the committed placement and the applied configuration.
    pub preferred: Option<u64>,
    /// The preferred leader replicates without probing and holds everything
    /// committed.
    pub current: bool,
    /// The preferred leader answered since the last quorum check.
    pub heard: bool,
    /// A hand-over is under way, whoever began it.
    pub transferring: bool,
    /// No change of configuration or placement is in progress and this
    /// member is not stopping.
    pub settled: bool,
    /// Nothing proposed is undecided.
    pub quiet: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Hold,
    /// Hand leadership to this member now.
    Ask(u64),
}

/// How often leadership was asked to move, for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    pub asked: u64,
    /// Hand-overs abandoned, or undone by leadership coming back.
    pub failed: u64,
}

#[derive(Debug)]
pub struct LeaderReturn {
    /// Ticks in one election timeout, at least one.
    election: u32,
    /// Who the counters below are about.
    target: u64,
    /// Ticks in a row the target was current.
    fit: u32,
    /// Ticks in a row the target was current but not heard.
    silent: u32,
    /// Ticks until the next ask is allowed.
    rest: u32,
    /// Times the rest has doubled.
    doubled: u32,
    /// Ticks since the last ask.
    since: u32,
    /// An ask is out and this member still leads.
    asked: bool,
    /// Whom leadership was handed to when it left this member after it
    /// asked.
    handed: Option<u64>,
    /// This member led on the last tick.
    led: bool,
    stats: Stats,
}

impl LeaderReturn {
    pub fn new(election_ticks: usize) -> Self {
        Self {
            election: u32::try_from(election_ticks).unwrap_or(u32::MAX).max(1),
            target: 0,
            fit: 0,
            silent: 0,
            rest: 0,
            doubled: 0,
            since: u32::MAX,
            asked: false,
            handed: None,
            led: false,
            stats: Stats::default(),
        }
    }
    /// The election timeout changed; counters keep their ticks.
    pub fn set_election(&mut self, election_ticks: usize) {
        self.election = u32::try_from(election_ticks).unwrap_or(u32::MAX).max(1);
    }
    pub fn stats(&self) -> Stats {
        self.stats
    }
    fn timeouts(&self, count: u32) -> u32 {
        self.election.saturating_mul(count)
    }
    /// The rest after `doubled` failures.
    fn rest_now(&self) -> u32 {
        let factor = 1u32.checked_shl(self.doubled.min(DOUBLINGS)).unwrap_or(1);
        self.timeouts(REST).saturating_mul(factor)
    }
    /// How long a hand-over is remembered: leadership that comes back
    /// within the longest rest undid it.
    fn memory(&self) -> u32 {
        let factor = 1u32.checked_shl(DOUBLINGS).unwrap_or(1);
        self.timeouts(REST).saturating_mul(factor)
    }
    fn fail(&mut self) {
        self.doubled = self.doubled.saturating_add(1).min(DOUBLINGS);
        self.rest = self.rest_now();
        self.stats.failed = self.stats.failed.saturating_add(1);
    }
    /// The hand-over was refused where it was asked.
    pub fn refused(&mut self) {
        if self.asked {
            self.asked = false;
            self.fail();
        }
    }
    /// One tick of the member that owns the session.
    pub fn observe(&mut self, seen: Seen) -> Verdict {
        self.rest = self.rest.saturating_sub(1);
        self.since = self.since.saturating_add(1);
        let led = core::mem::replace(&mut self.led, seen.leads);
        if !seen.leads {
            self.fit = 0;
            self.silent = 0;
            if self.asked {
                self.asked = false;
                self.handed = Some(self.target);
            }
            return Verdict::Hold;
        }
        if !led {
            // Leadership is back. It did not hold where it was handed to
            // if that member is still the one to lead; where the placement
            // prefers another since, this member perhaps, nothing failed.
            let undone = self
                .handed
                .take()
                .is_some_and(|to| seen.preferred == Some(to) && self.since <= self.memory());
            if undone {
                self.fail();
            } else {
                self.doubled = 0;
            }
        }
        if self.asked {
            if seen.transferring {
                return Verdict::Hold;
            }
            // Still leading and no longer handing over: it was abandoned.
            self.asked = false;
            self.fail();
        }
        let Some(preferred) = seen.preferred else {
            self.target = 0;
            self.fit = 0;
            self.silent = 0;
            return Verdict::Hold;
        };
        if self.target != preferred {
            self.target = preferred;
            self.fit = 0;
            self.silent = 0;
        }
        if seen.current {
            self.silent = if seen.heard {
                0
            } else {
                self.silent.saturating_add(1)
            };
            // The leader forgets who answered at every quorum check; a
            // member silent for longer than one has stopped answering.
            if self.silent > self.election {
                self.fit = 0;
            } else {
                self.fit = self.fit.saturating_add(1);
            }
        } else {
            self.fit = 0;
            self.silent = 0;
        }
        if seen.transferring
            || !seen.settled
            || !seen.heard
            || self.rest > 0
            || self.fit < self.timeouts(FIT)
        {
            return Verdict::Hold;
        }
        if !seen.quiet && self.fit < self.timeouts(PATIENCE) {
            return Verdict::Hold;
        }
        self.asked = true;
        self.since = 0;
        self.rest = self.rest_now();
        self.stats.asked = self.stats.asked.saturating_add(1);
        Verdict::Ask(preferred)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ELECTION: usize = 10;

    fn fit() -> Seen {
        Seen {
            leads: true,
            preferred: Some(2),
            current: true,
            heard: true,
            transferring: false,
            settled: true,
            quiet: true,
        }
    }
    /// Ticks until the first ask, within `limit`.
    fn until_ask(policy: &mut LeaderReturn, seen: Seen, limit: u32) -> Option<u32> {
        (1..=limit).find(|_| matches!(policy.observe(seen), Verdict::Ask(_)))
    }

    #[test]
    fn a_current_preferred_leader_is_asked_after_it_stayed_fit() {
        let mut policy = LeaderReturn::new(ELECTION);
        assert_eq!(until_ask(&mut policy, fit(), 1_000), Some(20));
        assert_eq!(
            policy.stats(),
            Stats {
                asked: 1,
                failed: 0
            }
        );
    }
    #[test]
    fn nothing_is_asked_of_a_follower_or_without_a_preferred_leader() {
        let mut policy = LeaderReturn::new(ELECTION);
        let follower = Seen {
            leads: false,
            ..fit()
        };
        assert_eq!(until_ask(&mut policy, follower, 10_000), None);
        let none = Seen {
            preferred: None,
            ..fit()
        };
        assert_eq!(until_ask(&mut policy, none, 10_000), None);
        for held in [
            Seen {
                settled: false,
                ..fit()
            },
            Seen {
                transferring: true,
                ..fit()
            },
            Seen {
                current: false,
                ..fit()
            },
            Seen {
                heard: false,
                ..fit()
            },
        ] {
            let mut policy = LeaderReturn::new(ELECTION);
            assert_eq!(until_ask(&mut policy, held, 10_000), None, "{held:?}");
        }
        assert_eq!(policy.stats(), Stats::default());
    }
    #[test]
    fn a_lapse_starts_the_count_again() {
        let mut policy = LeaderReturn::new(ELECTION);
        for _ in 0..19 {
            assert_eq!(policy.observe(fit()), Verdict::Hold);
        }
        let behind = Seen {
            current: false,
            ..fit()
        };
        assert_eq!(policy.observe(behind), Verdict::Hold);
        assert_eq!(until_ask(&mut policy, fit(), 1_000), Some(20));
    }
    #[test]
    fn the_quorum_check_forgetting_who_answered_is_not_a_lapse() {
        let mut policy = LeaderReturn::new(ELECTION);
        let unheard = Seen {
            heard: false,
            ..fit()
        };
        let mut asked = None;
        for tick in 1..=40u32 {
            // Forgotten at every tenth tick, heard again two ticks later.
            let seen = if tick % 10 < 2 { unheard } else { fit() };
            if let Verdict::Ask(target) = policy.observe(seen) {
                asked = Some((tick, target));
                break;
            }
        }
        assert_eq!(asked, Some((22, 2)));
        // A member that stays silent past one election timeout lapses.
        let mut policy = LeaderReturn::new(ELECTION);
        for _ in 0..15 {
            assert_eq!(policy.observe(fit()), Verdict::Hold);
        }
        for _ in 0..11 {
            assert_eq!(policy.observe(unheard), Verdict::Hold);
        }
        assert_eq!(until_ask(&mut policy, fit(), 1_000), Some(20));
    }
    #[test]
    fn a_change_of_preferred_leader_starts_the_count_again() {
        let mut policy = LeaderReturn::new(ELECTION);
        for _ in 0..19 {
            assert_eq!(policy.observe(fit()), Verdict::Hold);
        }
        let other = Seen {
            preferred: Some(3),
            ..fit()
        };
        let mut asked = None;
        for tick in 1..=100u32 {
            if let Verdict::Ask(target) = policy.observe(other) {
                asked = Some((tick, target));
                break;
            }
        }
        assert_eq!(asked, Some((20, 3)));
    }
    #[test]
    fn load_delays_the_hand_over_and_cannot_prevent_it() {
        let mut policy = LeaderReturn::new(ELECTION);
        let busy = Seen {
            quiet: false,
            ..fit()
        };
        for _ in 0..30 {
            assert_eq!(policy.observe(busy), Verdict::Hold);
        }
        // A quiet moment is taken at once.
        assert_eq!(policy.observe(fit()), Verdict::Ask(2));
        let mut policy = LeaderReturn::new(ELECTION);
        assert_eq!(until_ask(&mut policy, busy, 1_000), Some(80));
    }
    #[test]
    fn an_abandoned_hand_over_doubles_the_rest_up_to_its_bound() {
        let mut policy = LeaderReturn::new(ELECTION);
        assert_eq!(until_ask(&mut policy, fit(), 1_000), Some(20));
        let handing = Seen {
            transferring: true,
            ..fit()
        };
        let mut rests = Vec::new();
        for _ in 0..10 {
            for _ in 0..ELECTION {
                assert_eq!(policy.observe(handing), Verdict::Hold);
            }
            // The leader gave up; the rest begins where it did.
            let waited = until_ask(&mut policy, fit(), 100_000);
            rests.push(waited);
        }
        assert_eq!(
            rests,
            vec![
                Some(81),
                Some(161),
                Some(321),
                Some(641),
                Some(1_281),
                Some(2_561),
                Some(2_561),
                Some(2_561),
                Some(2_561),
                Some(2_561)
            ]
        );
        assert_eq!(
            policy.stats(),
            Stats {
                asked: 11,
                failed: 10
            }
        );
    }
    #[test]
    fn leadership_that_comes_back_counts_against_the_next_hand_over() {
        let mut policy = LeaderReturn::new(ELECTION);
        let follower = Seen {
            leads: false,
            ..fit()
        };
        let mut waits = Vec::new();
        for _ in 0..4 {
            waits.push(until_ask(&mut policy, fit(), 100_000));
            // The preferred leader led for three election timeouts and
            // failed; this member was elected again.
            for _ in 0..30 {
                assert_eq!(policy.observe(follower), Verdict::Hold);
            }
        }
        // Each rest begins when leadership comes back.
        assert_eq!(waits, vec![Some(20), Some(81), Some(161), Some(321)]);
        assert_eq!(policy.stats().failed, 3);
        // A hand-over that held for longer than the longest rest is
        // forgotten: the next one waits the plain rest.
        waits.clear();
        waits.push(until_ask(&mut policy, fit(), 100_000));
        for _ in 0..2_561 {
            assert_eq!(policy.observe(follower), Verdict::Hold);
        }
        waits.push(until_ask(&mut policy, fit(), 100_000));
        for _ in 0..ELECTION {
            let _ = policy.observe(Seen {
                transferring: true,
                ..fit()
            });
        }
        waits.push(until_ask(&mut policy, fit(), 100_000));
        assert_eq!(waits, vec![Some(641), Some(20), Some(81)]);
    }
    #[test]
    fn leadership_that_comes_by_another_placement_undoes_nothing() {
        let follower = Seen {
            leads: false,
            ..fit()
        };
        // Handed to the preferred leader; the placement then prefers this
        // member, which is handed leadership in its turn.
        let mut policy = LeaderReturn::new(ELECTION);
        assert_eq!(until_ask(&mut policy, fit(), 1_000), Some(20));
        for _ in 0..30 {
            assert_eq!(policy.observe(follower), Verdict::Hold);
        }
        let preferred = Seen {
            preferred: None,
            ..fit()
        };
        assert_eq!(until_ask(&mut policy, preferred, 1_000), None);
        assert_eq!(
            policy.stats(),
            Stats {
                asked: 1,
                failed: 0
            }
        );
        // And where it prefers a third: asked after the plain rest.
        let mut policy = LeaderReturn::new(ELECTION);
        assert_eq!(until_ask(&mut policy, fit(), 1_000), Some(20));
        for _ in 0..30 {
            assert_eq!(policy.observe(follower), Verdict::Hold);
        }
        let third = Seen {
            preferred: Some(3),
            ..fit()
        };
        assert_eq!(until_ask(&mut policy, third, 1_000), Some(20));
        assert_eq!(
            policy.stats(),
            Stats {
                asked: 2,
                failed: 0
            }
        );
    }
    #[test]
    fn a_refusal_where_it_was_asked_is_a_failure() {
        let mut policy = LeaderReturn::new(ELECTION);
        assert_eq!(until_ask(&mut policy, fit(), 1_000), Some(20));
        policy.refused();
        policy.refused();
        assert_eq!(
            policy.stats(),
            Stats {
                asked: 1,
                failed: 1
            }
        );
        assert_eq!(until_ask(&mut policy, fit(), 1_000), Some(80));
    }
    #[test]
    fn every_counter_saturates() {
        let mut policy = LeaderReturn::new(usize::MAX);
        for _ in 0..1_000 {
            assert_eq!(policy.observe(fit()), Verdict::Hold);
        }
        let mut policy = LeaderReturn::new(0);
        assert_eq!(until_ask(&mut policy, fit(), 10), Some(2));
        policy.since = u32::MAX;
        policy.fit = u32::MAX;
        policy.rest = 0;
        policy.stats.asked = u64::MAX;
        assert_eq!(
            policy.observe(Seen {
                transferring: true,
                ..fit()
            }),
            Verdict::Hold
        );
    }
}
