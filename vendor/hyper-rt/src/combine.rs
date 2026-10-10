//! Combining futures within one task: the first of several to finish ([`race2`], [`race3`]), all of them
//! ([`join2`], [`join3`]), and a bounded set of spawned tasks awaited as they end ([`TaskSet`]) — what a
//! consumer moving from tokio reaches for in `select!`, `join!` and `JoinSet`.
//!
//! **Biased, by design.** A race polls its futures in the order given, every poll, so a ready earlier one
//! always wins: deterministic, which the simulation needs to replay a run from its seed (tokio's `select!`
//! picks at random). A caller that must not starve a later branch puts it first, or bounds the earlier one.
//! The losers are dropped when the race returns, which is what cancel-safe futures are for.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

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
pub async fn race2<A: Future, B: Future>(a: A, b: B) -> Either<A::Output, B::Output> {
    let (mut a, mut b) = (std::pin::pin!(a), std::pin::pin!(b));
    std::future::poll_fn(|cx| {
        if let Poll::Ready(out) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::First(out));
        }
        if let Poll::Ready(out) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::Second(out));
        }
        Poll::Pending
    })
    .await
}

/// The first of `a`, `b` and `c` to finish; the others are dropped. Polls in that order.
pub async fn race3<A: Future, B: Future, C: Future>(
    a: A,
    b: B,
    c: C,
) -> Either3<A::Output, B::Output, C::Output> {
    let (mut a, mut b, mut c) = (std::pin::pin!(a), std::pin::pin!(b), std::pin::pin!(c));
    std::future::poll_fn(|cx| {
        if let Poll::Ready(out) = a.as_mut().poll(cx) {
            return Poll::Ready(Either3::First(out));
        }
        if let Poll::Ready(out) = b.as_mut().poll(cx) {
            return Poll::Ready(Either3::Second(out));
        }
        if let Poll::Ready(out) = c.as_mut().poll(cx) {
            return Poll::Ready(Either3::Third(out));
        }
        Poll::Pending
    })
    .await
}

/// Both outputs, the futures running together in this task.
pub async fn join2<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let (mut a, mut b) = (std::pin::pin!(a), std::pin::pin!(b));
    let (mut out_a, mut out_b) = (None, None);
    std::future::poll_fn(|cx| {
        if out_a.is_none()
            && let Poll::Ready(out) = a.as_mut().poll(cx)
        {
            out_a = Some(out);
        }
        if out_b.is_none()
            && let Poll::Ready(out) = b.as_mut().poll(cx)
        {
            out_b = Some(out);
        }
        match (out_a.take(), out_b.take()) {
            (Some(a), Some(b)) => Poll::Ready((a, b)),
            (a, b) => {
                (out_a, out_b) = (a, b);
                Poll::Pending
            }
        }
    })
    .await
}

/// All three outputs, the futures running together in this task.
pub async fn join3<A: Future, B: Future, C: Future>(
    a: A,
    b: B,
    c: C,
) -> (A::Output, B::Output, C::Output) {
    let ((a, b), c) = join2(join2(a, b), c).await;
    (a, b, c)
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
