//! When a round of requests to several peers ends (27 §3.1 P1).
//!
//! A round that waits a fixed time for every peer waits out its dead ones,
//! and one that stops at a fixed time under load stops rounds that were
//! about to complete. A round here ends on what it observes:
//!
//! - it has what it needs (the caller's test), or every peer has reported;
//! - it **stalled**: its deadline's lookahead was reached and nothing new
//!   arrived within the stall window;
//! - it was **extended** as far as it may be: while replies keep arriving a
//!   round past its lookahead is given more time, a bounded number of times.
//!
//! The deadline, the extension and the stall window are derived from what
//! the exchanges with these peers were measured to take
//! ([`RoundBudget::derive`]), so a slow machine or a far group stretches its
//! own rounds. Nothing here reads a clock or sleeps: the caller says what
//! time it is.
use crate::ELECTION_MARGIN;
use std::time::Duration;

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What a round may spend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoundBudget {
    /// When a round that made no progress ends.
    pub deadline_ns: u64,
    /// The part of the deadline in force after which progress is judged, as
    /// a ratio, so that no float enters a decision.
    pub lookahead: (u64, u64),
    /// What one extension adds.
    pub extension_ns: u64,
    pub max_extensions: u32,
    /// Progress older than this is no progress.
    pub stall_window_ns: u64,
}
impl RoundBudget {
    /// A round that ends at `deadline`, whatever arrives.
    pub fn hard(deadline: Duration) -> Self {
        let deadline_ns = nanos(deadline);
        Self {
            deadline_ns,
            lookahead: (1, 1),
            extension_ns: 0,
            max_extensions: 0,
            stall_window_ns: deadline_ns,
        }
    }
    /// From the owner's `period` and the `tail` of the slowest exchange
    /// measured with the round's peers, never past `ceiling` in all: a
    /// round opens for one period or one tail, whichever is longer, is
    /// judged at three quarters of that, is extended a period at a time
    /// while replies arrive, and calls two periods (or one tail) without a
    /// reply a stall. No measurement yet opens the round to its ceiling: a
    /// peer nothing is known about is given the time it was always given.
    pub fn derive(period: Duration, tail: Option<Duration>, ceiling: Duration) -> Self {
        let ceiling_ns = nanos(ceiling).max(1);
        let period_ns = nanos(period).clamp(1, ceiling_ns);
        let Some(tail) = tail else {
            return Self::hard(ceiling);
        };
        let tail_ns = nanos(tail);
        let deadline_ns = period_ns.max(tail_ns).min(ceiling_ns);
        let room = ceiling_ns.saturating_sub(deadline_ns);
        let extensions = room
            .checked_div(period_ns)
            .unwrap_or(0)
            .min(ELECTION_MARGIN);
        Self {
            deadline_ns,
            lookahead: (3, 4),
            extension_ns: period_ns,
            max_extensions: u32::try_from(extensions).unwrap_or(u32::MAX),
            stall_window_ns: period_ns.saturating_mul(2).max(tail_ns).min(ceiling_ns),
        }
    }
    /// The longest a round on this budget lasts.
    pub fn max_deadline_ns(&self) -> u64 {
        self.deadline_ns.saturating_add(
            self.extension_ns
                .saturating_mul(u64::from(self.max_extensions)),
        )
    }
}

/// Whether what a round gathers is still growing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProgressWitness {
    measure: u64,
    advanced_ns: Option<u64>,
    stall_window_ns: u64,
}
impl ProgressWitness {
    pub fn new(stall_window_ns: u64) -> Self {
        Self {
            measure: 0,
            advanced_ns: None,
            stall_window_ns,
        }
    }
    pub fn observe(&mut self, measure: u64, now_ns: u64) {
        if measure > self.measure {
            self.measure = measure;
            self.advanced_ns = Some(now_ns);
        }
    }
    /// A witness that never advanced is not progressing: being begun is not
    /// an advance.
    pub fn is_progressing(&self, now_ns: u64) -> bool {
        self.advanced_ns
            .is_some_and(|advanced| now_ns.saturating_sub(advanced) < self.stall_window_ns)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The lookahead of the deadline in force is not reached.
    Continue,
    /// Past the lookahead and progressing: the deadline moved to this.
    Extend { deadline_ns: u64 },
    /// Past the lookahead and stalled, or out of extensions.
    Expire,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeadlineExtender {
    deadline_ns: u64,
    lookahead: (u64, u64),
    extension_ns: u64,
    max_extensions: u32,
    granted: u32,
}
impl DeadlineExtender {
    pub fn new(budget: &RoundBudget) -> Self {
        Self {
            deadline_ns: budget.deadline_ns,
            lookahead: budget.lookahead,
            extension_ns: budget.extension_ns,
            max_extensions: budget.max_extensions,
            granted: 0,
        }
    }
    pub fn deadline_ns(&self) -> u64 {
        self.deadline_ns
    }
    /// The elapsed time at which the deadline in force is judged.
    pub fn lookahead_ns(&self) -> u64 {
        let (numerator, denominator) = self.lookahead;
        let scaled = u128::from(self.deadline_ns).saturating_mul(u128::from(numerator));
        u64::try_from(scaled.div_ceil(u128::from(denominator.max(1)))).unwrap_or(u64::MAX)
    }
    pub fn evaluate(&mut self, elapsed_ns: u64, witness: &ProgressWitness, now_ns: u64) -> Verdict {
        if elapsed_ns < self.lookahead_ns() {
            return Verdict::Continue;
        }
        if self.granted >= self.max_extensions || !witness.is_progressing(now_ns) {
            return Verdict::Expire;
        }
        self.granted = self.granted.saturating_add(1);
        self.deadline_ns = self.deadline_ns.saturating_add(self.extension_ns);
        Verdict::Extend {
            deadline_ns: self.deadline_ns,
        }
    }
}

/// One round's wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoundWait {
    extender: DeadlineExtender,
    witness: ProgressWitness,
    started_ns: u64,
}
impl RoundWait {
    pub fn begin(budget: &RoundBudget, now_ns: u64) -> Self {
        Self {
            extender: DeadlineExtender::new(budget),
            witness: ProgressWitness::new(budget.stall_window_ns),
            started_ns: now_ns,
        }
    }
    /// Whether the round goes on, having gathered `gathered` by `now_ns`.
    /// The caller reads what has arrived before it asks: a reply in hand is
    /// never discarded by a judgement made without it.
    pub fn judge(&mut self, gathered: u64, now_ns: u64) -> bool {
        self.witness.observe(gathered, now_ns);
        let elapsed = now_ns.saturating_sub(self.started_ns);
        !matches!(
            self.extender.evaluate(elapsed, &self.witness, now_ns),
            Verdict::Expire
        )
    }
    /// The next instant at which a judgement can end the round: a round
    /// that waits for it, and for replies, never polls.
    pub fn next_judgement_ns(&self) -> u64 {
        self.started_ns.saturating_add(self.extender.lookahead_ns())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const MS: u64 = 1_000_000;
    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }
    fn budget() -> RoundBudget {
        RoundBudget::derive(ms(100), Some(ms(10)), ms(5_000))
    }

    #[test]
    fn a_budget_is_derived_from_the_period_the_tail_and_the_ceiling() {
        // A near group: one period, extended a period at a time, ten times.
        assert_eq!(
            budget(),
            RoundBudget {
                deadline_ns: 100 * MS,
                lookahead: (3, 4),
                extension_ns: 100 * MS,
                max_extensions: 10,
                stall_window_ns: 200 * MS,
            }
        );
        assert_eq!(budget().max_deadline_ns(), 1_100 * MS);
        // A far group opens to its tail, and its stall window is the tail.
        let far = RoundBudget::derive(ms(100), Some(ms(1_300)), ms(5_000));
        assert_eq!(far.deadline_ns, 1_300 * MS);
        assert_eq!(far.stall_window_ns, 1_300 * MS);
        assert_eq!(far.max_deadline_ns(), 2_300 * MS);
        // Nothing past the ceiling: the extensions are what room is left.
        let tight = RoundBudget::derive(ms(100), Some(ms(4_750)), ms(5_000));
        assert_eq!((tight.deadline_ns, tight.max_extensions), (4_750 * MS, 2));
        assert!(tight.max_deadline_ns() <= 5_000 * MS);
        let over = RoundBudget::derive(ms(100), Some(ms(9_000)), ms(5_000));
        assert_eq!((over.deadline_ns, over.max_extensions), (5_000 * MS, 0));
        assert_eq!(over.stall_window_ns, 5_000 * MS);
        // No measurement: the ceiling, hard.
        assert_eq!(
            RoundBudget::derive(ms(100), None, ms(5_000)),
            RoundBudget::hard(ms(5_000))
        );
        // A period of nothing divides nothing.
        let zero = RoundBudget::derive(Duration::ZERO, Some(Duration::ZERO), ms(5));
        assert_eq!((zero.deadline_ns, zero.extension_ns), (1, 1));
        assert!(zero.max_deadline_ns() <= 5 * MS);
    }
    #[test]
    fn the_witness_tracks_forward_progress() {
        let mut witness = ProgressWitness::new(200 * MS);
        witness.observe(1, 10 * MS);
        assert!(witness.is_progressing(10 * MS));
        assert!(witness.is_progressing(209 * MS));
        assert!(!witness.is_progressing(210 * MS));
        // The same measure again is no advance; a larger one is.
        witness.observe(1, 300 * MS);
        assert!(!witness.is_progressing(300 * MS));
        witness.observe(2, 300 * MS);
        assert!(witness.is_progressing(499 * MS));
    }
    #[test]
    fn a_witness_that_never_advanced_is_not_progressing() {
        let mut witness = ProgressWitness::new(200 * MS);
        assert!(!witness.is_progressing(0));
        witness.observe(0, 1);
        assert!(!witness.is_progressing(1));
    }
    #[test]
    fn before_the_lookahead_a_round_continues_whatever_it_gathered() {
        let mut wait = RoundWait::begin(&budget(), 1_000 * MS);
        assert_eq!(wait.next_judgement_ns(), 1_075 * MS);
        assert!(wait.judge(0, 1_000 * MS));
        assert!(wait.judge(0, 1_074 * MS));
        // At the lookahead with nothing in hand it ends.
        assert!(!wait.judge(0, 1_075 * MS));
    }
    #[test]
    fn a_progressing_round_is_extended_and_a_stalled_one_ends() {
        let mut wait = RoundWait::begin(&budget(), 0);
        assert!(wait.judge(1, 60 * MS));
        // Past the lookahead, a reply 15 ms ago: extended to 200 ms, judged
        // next at 150 ms.
        assert!(wait.judge(1, 75 * MS));
        assert_eq!(wait.next_judgement_ns(), 150 * MS);
        assert!(wait.judge(2, 140 * MS));
        assert!(wait.judge(2, 150 * MS));
        assert_eq!(wait.next_judgement_ns(), 225 * MS);
        // Nothing since 140 ms: at 340 ms that is a stall.
        assert!(wait.judge(2, 225 * MS));
        assert_eq!(wait.next_judgement_ns(), 300 * MS);
        assert!(wait.judge(2, 300 * MS));
        assert!(!wait.judge(2, 375 * MS));
    }
    #[test]
    fn extensions_are_bounded() {
        let budget = budget();
        let mut wait = RoundWait::begin(&budget, 0);
        let mut now = 0;
        let mut gathered = 0;
        let mut judged = 0u32;
        // A reply every 50 ms for ever: the round still ends.
        while judged < 1_000 {
            now += 50 * MS;
            gathered += 1;
            judged += 1;
            if !wait.judge(gathered, now) {
                break;
            }
        }
        assert!(now <= budget.max_deadline_ns(), "{now}");
        assert!(now >= budget.max_deadline_ns() * 3 / 4, "{now}");
    }
    #[test]
    fn a_hard_budget_ends_at_its_deadline_and_never_extends() {
        let mut wait = RoundWait::begin(&RoundBudget::hard(ms(500)), 0);
        assert_eq!(wait.next_judgement_ns(), 500 * MS);
        assert!(wait.judge(3, 499 * MS));
        assert!(!wait.judge(4, 500 * MS));
    }
}
