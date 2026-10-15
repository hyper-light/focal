//! The typed write queue (`docs/sim.md` §3.5): a device whose writes are values the harness defines
//! (a member's hard state and entries, a `Write` of hyper-durable's `LogStore`), made durable one at
//! a time in the order they were submitted when the world completes them, and answered to their
//! owner later still. hyper-raft's `Lagged` member (take, durable, notify) and hyper-durable's
//! `SimStore` (submit, made durable, poll) are this device.
//!
//! **Completions are events of the world.** While a write is out, the device has one completion
//! pending in the world ([`Writes::arm`]); when the world's strategy takes it, the harness hands it
//! to [`Writes::fire`], which completes the oldest write out, and arms again while one is out. One at
//! a time, because the device makes its writes durable in order: a completion is the device's next
//! step, not a particular write's, so whichever completion the strategy takes, the write it
//! completes is the oldest. How many writes are out at once is the owner's depth, bounded here by
//! the writes held (`bound`), not the harness's choice of steps.
//!
//! **A crash** loses every write out and every answer not taken ([`Writes::crash`]); the harness
//! keeps the device across it, as the member's durable state outlives its process (§3.6), so a
//! completion pending in the world at the crash serves the process that reopens on it.

use std::collections::VecDeque;

use crate::error::SimError;
use crate::world::{NodeId, World};

/// What a queue did, counted, so a test proves what it ran.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Writes submitted.
    pub submitted: u64,
    /// Writes completed: made durable, or refused, as the harness answered.
    pub completed: u64,
    /// Answers the owner took.
    pub answered: u64,
    /// Writes out, and answers not taken, at a crash.
    pub lost: u64,
    /// Completions the world gave with no write out: their writes were completed directly
    /// ([`Writes::complete`]) or lost to a crash.
    pub idle: u64,
}

/// A device's writes: those out, oldest first, then those completed whose answers the owner has not
/// taken. `W` is the write as the harness keeps it, `A` its answer.
#[derive(Clone, Debug)]
pub struct Writes<W, A> {
    out: VecDeque<W>,
    answers: VecDeque<(W, A)>,
    /// The most writes held, out and answered together.
    bound: usize,
    /// Whether the device's next completion is pending in the world.
    armed: bool,
    stats: WriteStats,
}

impl<W, A> Writes<W, A> {
    /// A device holding at most `bound` writes, out and answered together.
    pub fn new(bound: usize) -> Self {
        Self {
            out: VecDeque::new(),
            answers: VecDeque::new(),
            bound,
            armed: false,
            stats: WriteStats::default(),
        }
    }

    /// Takes `write`; refused past the bound ([`SimError::Full`]), with nothing changed.
    pub fn submit(&mut self, write: W) -> Result<(), SimError> {
        if self.held() >= self.bound {
            return Err(SimError::Full {
                what: "writes",
                bound: self.bound,
            });
        }
        self.out.push_back(write);
        self.stats.submitted = self.stats.submitted.saturating_add(1);
        Ok(())
    }

    /// Writes out: submitted and not completed.
    pub fn out(&self) -> usize {
        self.out.len()
    }

    /// Writes completed whose answers the owner has not taken.
    pub fn unanswered(&self) -> usize {
        self.answers.len()
    }

    /// Writes held: out and unanswered.
    pub fn held(&self) -> usize {
        self.out.len().saturating_add(self.answers.len())
    }

    /// Every write held: the unanswered, oldest first, then those out.
    pub fn iter(&self) -> impl Iterator<Item = &W> {
        self.answers
            .iter()
            .map(|(write, _)| write)
            .chain(self.out.iter())
    }

    /// Completes the oldest write out: `answer` makes it durable or refuses it, and says what its
    /// owner is answered. False when none is out.
    pub fn complete(&mut self, answer: impl FnOnce(&W) -> A) -> bool {
        let Some(write) = self.out.pop_front() else {
            return false;
        };
        let answered = answer(&write);
        self.answers.push_back((write, answered));
        self.stats.completed = self.stats.completed.saturating_add(1);
        true
    }

    /// Completes every write out, oldest first, each answered by `answer`: a refusal that every
    /// write behind it shares. How many.
    pub fn complete_all(&mut self, mut answer: impl FnMut(&W) -> A) -> usize {
        let mut completed = 0usize;
        while self.complete(&mut answer) {
            completed = completed.saturating_add(1);
        }
        completed
    }

    /// The oldest answer, taken by the owner.
    pub fn take(&mut self) -> Option<(W, A)> {
        let taken = self.answers.pop_front()?;
        self.stats.answered = self.stats.answered.saturating_add(1);
        Some(taken)
    }

    /// The owner's process is gone: every write out and every answer not taken is lost. What the
    /// completions made durable is the harness's, and stays. A completion pending in the world
    /// stays armed: it serves the process that reopens on the device.
    pub fn crash(&mut self) -> usize {
        let lost = self.held();
        self.out.clear();
        self.answers.clear();
        self.stats.lost = self
            .stats
            .lost
            .saturating_add(u64::try_from(lost).unwrap_or(u64::MAX));
        lost
    }

    /// Gives the world the device's next completion, `event` for `node` now, when a write is out
    /// and none is pending already. Whether it was given.
    pub fn arm<E>(
        &mut self,
        world: &mut World<E>,
        node: NodeId,
        event: impl FnOnce() -> E,
    ) -> Result<bool, SimError> {
        if self.armed || self.out.is_empty() {
            return Ok(false);
        }
        world.schedule(world.now(), node, event())?;
        self.armed = true;
        Ok(true)
    }

    /// The world took the device's completion: the oldest write out is completed as `answer`
    /// says. False when none was out. The harness arms again once its owner has been driven.
    pub fn fire(&mut self, answer: impl FnOnce(&W) -> A) -> bool {
        self.armed = false;
        let completed = self.complete(answer);
        if !completed {
            self.stats.idle = self.stats.idle.saturating_add(1);
        }
        completed
    }

    /// Whether the device's completion is pending in the world.
    pub fn armed(&self) -> bool {
        self.armed
    }

    /// What the queue did.
    pub fn stats(&self) -> WriteStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Discipline;
    use crate::world::{Limits, Random, Source, Step};

    fn world() -> World<u8> {
        World::new(
            Source::Seed(7),
            Discipline::Free,
            Limits {
                events: 16,
                nodes: 2,
                streams: 4,
                steps: 1_000,
                trace_words: 4_096,
            },
        )
        .unwrap()
    }

    /// Three writes out, two completed: what each completion was handed, in order.
    fn two_of_three_completed() -> (Writes<u32, bool>, Vec<u32>) {
        let mut writes: Writes<u32, bool> = Writes::new(4);
        for write in 1..=3 {
            writes.submit(write).unwrap();
        }
        let mut durable = Vec::new();
        for _ in 0..2 {
            assert!(writes.complete(|w| {
                durable.push(*w);
                *w != 2
            }));
        }
        (writes, durable)
    }

    #[test]
    fn writes_complete_in_the_order_submitted() {
        let (writes, durable) = two_of_three_completed();
        assert_eq!(durable, vec![1, 2]);
        assert_eq!((writes.out(), writes.unanswered()), (1, 2));
        assert_eq!(writes.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn answers_are_taken_in_the_order_completed() {
        let (mut writes, _) = two_of_three_completed();
        assert_eq!(writes.take(), Some((1, true)));
        assert_eq!(writes.take(), Some((2, false)));
        assert_eq!(writes.take(), None);
        assert_eq!(writes.complete_all(|_| true), 1);
        assert_eq!(writes.take(), Some((3, true)));
        assert!(!writes.complete(|_| true));
    }

    #[test]
    fn a_write_past_the_bound_is_refused_with_nothing_changed() {
        let mut writes: Writes<u32, ()> = Writes::new(2);
        writes.submit(1).unwrap();
        writes.submit(2).unwrap();
        assert_eq!(
            writes.submit(3),
            Err(SimError::Full {
                what: "writes",
                bound: 2
            })
        );
        assert_eq!(writes.held(), 2);
        // An answer not taken still holds its place.
        assert!(writes.complete(|_| ()));
        assert!(writes.submit(3).is_err());
        assert!(writes.take().is_some());
        assert!(writes.submit(3).is_ok());
    }

    #[test]
    fn one_completion_is_pending_at_a_time() {
        let mut world = world();
        let node = world.node(crate::Clock::default()).unwrap();
        let mut writes: Writes<u32, ()> = Writes::new(4);
        assert!(!writes.arm(&mut world, node, || 9).unwrap(), "nothing out");
        writes.submit(1).unwrap();
        writes.submit(2).unwrap();
        assert!(writes.arm(&mut world, node, || 9).unwrap());
        assert!(!writes.arm(&mut world, node, || 9).unwrap());
        assert_eq!(world.pending(), 1);
    }

    /// Two writes out, the first completed and unanswered, and a crash: the completion stays
    /// pending in the world.
    fn crashed(world: &mut World<u8>, node: NodeId) -> Writes<u32, ()> {
        let mut writes: Writes<u32, ()> = Writes::new(4);
        writes.submit(1).unwrap();
        writes.submit(2).unwrap();
        assert!(writes.arm(world, node, || 9).unwrap());
        assert!(writes.complete(|_| ()));
        assert_eq!(writes.crash(), 2);
        writes
    }

    #[test]
    fn a_crash_loses_what_is_out_and_unanswered_and_keeps_the_completion_armed() {
        let mut world = world();
        let node = world.node(crate::Clock::default()).unwrap();
        let mut writes = crashed(&mut world, node);
        assert_eq!(writes.held(), 0);
        assert!(writes.armed());
        // The process that reopens submits; the completion pending completes its write.
        writes.submit(3).unwrap();
        assert_eq!(
            world.next(&mut Random).unwrap(),
            Step::Event { node, event: 9 }
        );
        assert!(writes.fire(|_| ()));
        assert_eq!(writes.take(), Some((3, ())));
        assert!(!writes.armed());
        let counted = WriteStats {
            submitted: 3,
            completed: 2,
            answered: 1,
            lost: 2,
            idle: 0,
        };
        assert_eq!(writes.stats(), counted);
    }

    #[test]
    fn a_completion_with_nothing_out_is_counted_idle() {
        let mut world = world();
        let node = world.node(crate::Clock::default()).unwrap();
        let mut writes: Writes<u32, ()> = Writes::new(4);
        writes.submit(1).unwrap();
        assert!(writes.arm(&mut world, node, || 0).unwrap());
        // The harness made it durable directly; the world's completion then finds nothing out.
        assert!(writes.complete(|_| ()));
        assert!(!writes.fire(|_| ()));
        assert_eq!(writes.stats().idle, 1);
        assert!(!writes.arm(&mut world, node, || 0).unwrap());
    }
}
