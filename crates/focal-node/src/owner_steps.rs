//! A replica owner's slowest recent steps: what it was doing when its thread
//! held the replica's work for more than [`SLOW_STEP`], and for how long. One
//! owner thread serves a replica's every request, so a step this long holds
//! every request queued behind it; the operator reads them in the replica's
//! diagnostics (`inspect replicas --replicas`).
use std::time::{Duration, Instant};

/// A step at least this long is remembered.
pub(crate) const SLOW_STEP: Duration = Duration::from_millis(10);
/// How many the owner remembers: the most recent.
pub(crate) const SLOW_STEPS: usize = 16;
/// And the longest it has seen, whenever they were: a stall that came
/// before a run of merely slow steps is not pushed out by them.
pub(crate) const LONGEST_STEPS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlowStep {
    pub(crate) what: &'static str,
    pub(crate) micros: u64,
    /// When the step ended, in milliseconds since the Unix epoch.
    pub(crate) at_ms: u64,
}

#[derive(Debug, Default)]
pub(crate) struct SlowSteps {
    slots: [Option<SlowStep>; SLOW_STEPS],
    next: usize,
    longest: [Option<SlowStep>; LONGEST_STEPS],
}

impl SlowSteps {
    /// Remember the step `what` begun at `started` if it took at least
    /// [`SLOW_STEP`].
    pub(crate) fn note(&mut self, what: &'static str, started: Instant) {
        let took = started.elapsed();
        if took < SLOW_STEP {
            return;
        }
        let at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|now| u64::try_from(now.as_millis()).ok())
            .unwrap_or(0);
        let step = SlowStep {
            what,
            micros: u64::try_from(took.as_micros()).unwrap_or(u64::MAX),
            at_ms,
        };
        if let Some(slot) = self.slots.get_mut(self.next) {
            *slot = Some(step);
        }
        self.next = self
            .next
            .saturating_add(1)
            .checked_rem(SLOW_STEPS)
            .unwrap_or(0);
        // The shortest of the longest gives its place to a longer one.
        if let Some(shortest) = self
            .longest
            .iter_mut()
            .min_by_key(|slot| slot.map_or(0, |kept| kept.micros))
            && shortest.is_none_or(|kept| kept.micros < step.micros)
        {
            *shortest = Some(step);
        }
    }
    /// The most recent steps, oldest first.
    pub(crate) fn recent(&self) -> impl Iterator<Item = SlowStep> + '_ {
        let (newer, older) = self.slots.split_at(self.next.min(SLOW_STEPS));
        older.iter().chain(newer).flatten().copied()
    }
    /// The most recent steps and the longest, each once, oldest first: at
    /// most [`SLOW_STEPS`] and [`LONGEST_STEPS`].
    pub(crate) fn kept(&self) -> Vec<SlowStep> {
        let mut kept: Vec<SlowStep> = Vec::new();
        if kept
            .try_reserve_exact(SLOW_STEPS.saturating_add(LONGEST_STEPS))
            .is_err()
        {
            return kept;
        }
        for step in self.recent().chain(self.longest.iter().flatten().copied()) {
            if !kept.contains(&step) {
                kept.push(step);
            }
        }
        kept.sort_by_key(|step| step.at_ms);
        kept
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_slow_steps_are_kept_and_the_oldest_go_first() {
        let mut steps = SlowSteps::default();
        steps.note("fast", Instant::now());
        assert_eq!(steps.recent().count(), 0);
        let long_ago = Instant::now()
            .checked_sub(Duration::from_millis(50))
            .unwrap();
        for _ in 0..(SLOW_STEPS + 3) {
            steps.note("slow", long_ago);
        }
        let kept: Vec<SlowStep> = steps.recent().collect();
        assert_eq!(kept.len(), SLOW_STEPS);
        assert!(
            kept.iter()
                .all(|step| step.what == "slow" && step.micros >= 50_000)
        );
        assert!(kept.windows(2).all(|pair| pair[0].at_ms <= pair[1].at_ms));
    }

    /// A long stall is kept past the run of merely slow steps that came
    /// after it, and is reported once, beside them, in time order.
    #[test]
    fn the_longest_steps_outlast_the_recent_ones() {
        let mut steps = SlowSteps::default();
        let ago = |millis: u64| {
            Instant::now()
                .checked_sub(Duration::from_millis(millis))
                .unwrap()
        };
        steps.note("stall", ago(400));
        for _ in 0..(SLOW_STEPS * 2) {
            steps.note("slow", ago(20));
        }
        assert!(steps.recent().all(|step| step.what == "slow"));
        let kept = steps.kept();
        assert_eq!(kept.iter().filter(|step| step.what == "stall").count(), 1);
        assert!(kept.len() <= SLOW_STEPS + LONGEST_STEPS);
        assert!(kept.windows(2).all(|pair| pair[0].at_ms <= pair[1].at_ms));
    }
}
