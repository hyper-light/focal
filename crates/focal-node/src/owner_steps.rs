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
        if let Some(slot) = self.slots.get_mut(self.next) {
            *slot = Some(SlowStep {
                what,
                micros: u64::try_from(took.as_micros()).unwrap_or(u64::MAX),
                at_ms,
            });
        }
        self.next = self
            .next
            .saturating_add(1)
            .checked_rem(SLOW_STEPS)
            .unwrap_or(0);
    }
    /// The remembered steps, oldest first.
    pub(crate) fn recent(&self) -> impl Iterator<Item = SlowStep> + '_ {
        let (newer, older) = self.slots.split_at(self.next.min(SLOW_STEPS));
        older.iter().chain(newer).flatten().copied()
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
}
