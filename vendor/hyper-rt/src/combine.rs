//! Combining futures within one task: the first of several to finish ([`race2`], [`race3`]), all of them
//! ([`join2`], [`join3`]), and a bounded set of spawned tasks awaited as they end ([`TaskSet`]) — what a
//! consumer moving from tokio reaches for in `select!`, `join!` and `JoinSet`.
//!
//! **Biased, by design.** A race polls its futures in the order given, every poll, so a ready earlier one
//! always wins: deterministic, which the simulation needs to replay a run from its seed (tokio's `select!`
//! picks at random). A caller that must not starve a later branch puts it first, or bounds the earlier one.
//! The losers are dropped when the race returns, which is what cancel-safe futures are for.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

use crate::error::RtError;
use crate::futures::{Join, join, spawn};
use crate::shard::TaskId;
use crate::task::Outcome;

/// Which of two futures finished first, with its output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Either<A, B> {
    /// The first.
    First(A),
    /// The second.
    Second(B),
}

/// Which of three futures finished first, with its output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Either3<A, B, C> {
    /// The first.
    First(A),
    /// The second.
    Second(B),
    /// The third.
    Third(C),
}

/// The first of `a` and `b` to finish; the other is dropped. Polls `a` first.
pub fn race2<A: Future, B: Future>(a: A, b: B) -> Race2<A, B> {
    Race2 {
        a,
        b,
        finished: false,
    }
}

/// The first of `a`, `b` and `c` to finish; the others are dropped. Polls in that order.
pub fn race3<A: Future, B: Future, C: Future>(a: A, b: B, c: C) -> Race3<A, B, C> {
    Race3 {
        a,
        b,
        c,
        finished: false,
    }
}

/// Both outputs, the futures running together in this task.
pub fn join2<A: Future, B: Future>(a: A, b: B) -> Join2<A, B> {
    Join2 {
        a: Slot::Running { future: a },
        b: Slot::Running { future: b },
    }
}

/// All three outputs, the futures running together in this task.
pub fn join3<A: Future, B: Future, C: Future>(a: A, b: B, c: C) -> Join3<A, B, C> {
    Join3 {
        a: Slot::Running { future: a },
        b: Slot::Running { future: b },
        c: Slot::Running { future: c },
    }
}

// Each combinator is a named future holding what it was given in place, once. As `async fn`s they held each
// future twice, the argument beside the pinned local it moved into (rust-lang/rust#62958), so every level of
// nesting doubled what it held and a debug build copied it on the stack besides (docs/runtime.md §3.8).

pin_project! {
    /// The wait of [`race2`].
    #[must_use = "a race does nothing unless awaited"]
    pub struct Race2<A, B> {
        #[pin]
        a: A,
        #[pin]
        b: B,
        // Set once a future has finished: the race is not polled again, and a finished future never is.
        finished: bool,
    }
}

impl<A: Future, B: Future> Future for Race2<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if *this.finished {
            return Poll::Pending;
        }
        let won = if let Poll::Ready(out) = this.a.poll(cx) {
            Either::First(out)
        } else if let Poll::Ready(out) = this.b.poll(cx) {
            Either::Second(out)
        } else {
            return Poll::Pending;
        };
        *this.finished = true;
        Poll::Ready(won)
    }
}

impl<A, B> fmt::Debug for Race2<A, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Race2")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

pin_project! {
    /// The wait of [`race3`].
    #[must_use = "a race does nothing unless awaited"]
    pub struct Race3<A, B, C> {
        #[pin]
        a: A,
        #[pin]
        b: B,
        #[pin]
        c: C,
        finished: bool,
    }
}

impl<A: Future, B: Future, C: Future> Future for Race3<A, B, C> {
    type Output = Either3<A::Output, B::Output, C::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if *this.finished {
            return Poll::Pending;
        }
        let won = if let Poll::Ready(out) = this.a.poll(cx) {
            Either3::First(out)
        } else if let Poll::Ready(out) = this.b.poll(cx) {
            Either3::Second(out)
        } else if let Poll::Ready(out) = this.c.poll(cx) {
            Either3::Third(out)
        } else {
            return Poll::Pending;
        };
        *this.finished = true;
        Poll::Ready(won)
    }
}

impl<A, B, C> fmt::Debug for Race3<A, B, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Race3")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

pin_project! {
    /// One future of a join: running, finished with its output held (the future itself dropped), or its
    /// output handed on.
    #[project = SlotProj]
    #[project_replace = SlotReplace]
    // The tag stays out of the future's bytes: a niche there would be read to match the slot while the
    // running future is lent out pinned (tokio's `MaybeDone` holds the same, rust-lang/miri#3780).
    #[repr(C)]
    enum Slot<F: Future> {
        Running {
            #[pin]
            future: F,
        },
        Done {
            output: F::Output,
        },
        Taken,
    }
}

impl<F: Future> Slot<F> {
    /// Polls a running future, keeping its output once it has one; whether the output is held.
    fn poll_done(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> bool {
        let output = match self.as_mut().project() {
            SlotProj::Running { future } => match future.poll(cx) {
                Poll::Ready(output) => output,
                Poll::Pending => return false,
            },
            SlotProj::Done { .. } => return true,
            SlotProj::Taken => return false,
        };
        // The finished future is dropped here, in place, and what it held let go with it.
        self.set(Slot::Done { output });
        true
    }

    /// The held output, the slot left taken; `None` (and the slot unchanged) when it holds none.
    fn take(self: Pin<&mut Self>) -> Option<F::Output> {
        if !matches!(&*self, Slot::Done { .. }) {
            return None;
        }
        match self.project_replace(Slot::Taken) {
            SlotReplace::Done { output } => Some(output),
            SlotReplace::Running { .. } | SlotReplace::Taken => None,
        }
    }
}

pin_project! {
    /// The wait of [`join2`].
    #[must_use = "a join does nothing unless awaited"]
    pub struct Join2<A: Future, B: Future> {
        #[pin]
        a: Slot<A>,
        #[pin]
        b: Slot<B>,
    }
}

impl<A: Future, B: Future> Future for Join2<A, B> {
    type Output = (A::Output, B::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let a = this.a.as_mut().poll_done(cx);
        let b = this.b.as_mut().poll_done(cx);
        if !(a && b) {
            return Poll::Pending;
        }
        match (this.a.take(), this.b.take()) {
            (Some(a), Some(b)) => Poll::Ready((a, b)),
            // Each held its output a moment ago; a join whose outputs were handed on waits.
            _ => Poll::Pending,
        }
    }
}

impl<A: Future, B: Future> fmt::Debug for Join2<A, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Join2").finish_non_exhaustive()
    }
}

pin_project! {
    /// The wait of [`join3`].
    #[must_use = "a join does nothing unless awaited"]
    pub struct Join3<A: Future, B: Future, C: Future> {
        #[pin]
        a: Slot<A>,
        #[pin]
        b: Slot<B>,
        #[pin]
        c: Slot<C>,
    }
}

impl<A: Future, B: Future, C: Future> Future for Join3<A, B, C> {
    type Output = (A::Output, B::Output, C::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let a = this.a.as_mut().poll_done(cx);
        let b = this.b.as_mut().poll_done(cx);
        let c = this.c.as_mut().poll_done(cx);
        if !(a && b && c) {
            return Poll::Pending;
        }
        match (this.a.take(), this.b.take(), this.c.take()) {
            (Some(a), Some(b), Some(c)) => Poll::Ready((a, b, c)),
            _ => Poll::Pending,
        }
    }
}

impl<A: Future, B: Future, C: Future> fmt::Debug for Join3<A, B, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Join3").finish_non_exhaustive()
    }
}

/// A bounded set of tasks spawned on the current shard, awaited as they end (tokio's `JoinSet`): at most
/// `capacity` at once, a spawn past it refused `Capacity`. Dropping the set leaves its tasks running
/// (cancel them with [`TaskSet::cancel_all`] first when they must not outlive it).
#[derive(Debug)]
pub struct TaskSet {
    tasks: Vec<TaskId>,
    capacity: usize,
}

impl TaskSet {
    /// An empty set of at most `capacity` tasks.
    pub fn new(capacity: usize) -> Result<Self, RtError> {
        let mut tasks = Vec::new();
        tasks
            .try_reserve_exact(capacity)
            .map_err(|_| RtError::Capacity {
                what: "task set",
                bound: capacity,
            })?;
        Ok(Self { tasks, capacity })
    }

    /// Spawns `future` on the current shard into the set.
    pub fn spawn<F: Future<Output = ()> + 'static>(
        &mut self,
        future: F,
    ) -> Result<TaskId, RtError> {
        if self.tasks.len() >= self.capacity {
            return Err(RtError::Capacity {
                what: "task set",
                bound: self.capacity,
            });
        }
        let id = spawn(future)?;
        self.tasks.push(id);
        Ok(id)
    }

    /// Tasks in the set.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// The next task to end, and how; `None` when the set is empty.
    pub fn join_next(&mut self) -> JoinNext<'_> {
        JoinNext { set: self }
    }

    /// Requests every task's cancellation; [`TaskSet::join_next`] then reports each as it ends.
    pub fn cancel_all(&self) {
        for id in &self.tasks {
            let _ = crate::futures::cancel(*id);
        }
    }
}

/// The wait of [`TaskSet::join_next`].
#[derive(Debug)]
pub struct JoinNext<'a> {
    set: &'a mut TaskSet,
}

impl Future for JoinNext<'_> {
    type Output = Option<(TaskId, Result<Outcome, RtError>)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.set.tasks.is_empty() {
            return Poll::Ready(None);
        }
        // Each task's join registers this task as its joiner; the first terminal one is taken.
        let mut done = None;
        for (index, id) in self.set.tasks.iter().enumerate() {
            let mut join: Join = join(*id);
            if let Poll::Ready(outcome) = Pin::new(&mut join).poll(cx) {
                done = Some((index, *id, outcome));
                break;
            }
        }
        match done {
            Some((index, id, outcome)) => {
                self.set.tasks.swap_remove(index);
                Poll::Ready(Some((id, outcome)))
            }
            None => Poll::Pending,
        }
    }
}
