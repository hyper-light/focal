//! What a task can do: spawn siblings and children, join, cancel, yield, and sleep on the
//! shard's wheel (§4.3). Every future here is cancel-safe by construction: the timer a `Sleep`
//! arms lives in the wheel's slab and is disarmed when the future drops.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

use crate::error::RtError;
use crate::registry;
use crate::shard::{ShardId, TaskId, TimerId};
use crate::task::Outcome;
use crate::waker::polling_task;

/// The current shard's id, if this thread runs one.
pub fn shard_id() -> Option<ShardId> {
    registry::with_current(|ctx| ShardId(ctx.id))
}

/// The current shard's monotonic clock in nanoseconds, or zero if this thread runs no shard (a caller
/// that reads it off a shard thread, such as timing a round trip, gets the driver's clock; the sim's
/// clock in a simulation).
pub fn now_ns() -> u64 {
    registry::with_current(|ctx| ctx.now_ns()).unwrap_or(0)
}

/// The current shard's measured scheduler quantum in nanoseconds — how late its steps have been
/// running after the waits before them, an exponentially-forgetting maximum
/// ([`crate::shard::ShardContext::scheduler_overrun_ns`]: only an idle shard waits, so this is the
/// operating system's descheduling of the shard, not the latency of its own tasks). Zero off a shard
/// thread. The fleet's failure detector floors its windows at it (§4.8 "SWIM period = max(k × RTT
/// p99, scheduler quantum)").
pub fn scheduler_overrun_ns() -> u64 {
    registry::with_current(|ctx| ctx.scheduler_overrun_ns()).unwrap_or(0)
}

/// The current shard's counters as they stand now ([`crate::shard::ShardContext::counters`]): what a task
/// tells of the loop it runs on (its steps, waits, spins, timers fired, the reads its learning made), as a
/// node reports them when it stops. None off a shard thread.
pub fn shard_counters() -> Option<crate::shard_loop::Counters> {
    registry::with_current(|ctx| ctx.counters())
}

/// The current shard's timer tick in nanoseconds ([`crate::shard::ShardContext::timer_tick_ns`]): a sleep
/// ends up to a tick later than due by design, and later still by [`scheduler_overrun_ns`]. A wait that must
/// end before another's (an answer before its asker gives up) sets both aside. Zero off a shard thread.
pub fn timer_tick_ns() -> u64 {
    registry::with_current(|ctx| ctx.timer_tick_ns()).unwrap_or(0)
}

/// The current shard's step quantum in nanoseconds (§4.3, "a step longer than a peer's wake starves the
/// shard"): its online wake estimate while it tracks one, else its configured step budget
/// ([`crate::shard::ShardContext::quantum_ns`]). A cooperative operation sizes its slices by it (a
/// destroy's, an archive's). `None` off a shard thread.
pub fn step_budget_ns() -> Option<u64> {
    registry::with_current(|ctx| ctx.quantum_ns())
}

/// The current shard's online wake estimate in nanoseconds (the configured step budget when it tracks
/// none); what the daemon's own idle window scales. `None` off a shard thread.
pub fn wake_cost_ns() -> Option<u64> {
    registry::with_current(|ctx| ctx.wake_cost_ns())
}

/// The task being polled on this thread, if any.
pub fn current_task() -> Option<TaskId> {
    registry::with_current(|ctx| ctx.current_task()).flatten()
}

/// Spawns a joinable task on the current shard with no parent.
pub fn spawn<F: Future<Output = ()> + 'static>(future: F) -> Result<TaskId, RtError> {
    registry::with_current(|ctx| ctx.spawn_framed(future, None, true, false))
        .ok_or(RtError::NotOnShardThread)?
}

/// Spawns a joinable service on the current shard. Unlike ordinary tasks,
/// cancellation is a level the service observes; its future remains owned until Ready.
/// Cleanup after driver loss must use channel completions, not new native readiness waits.
/// A service must turn typed failures into cleanup. A panicked future cannot be safely
/// resumed; this opt-in does not promise recovery from panic or a blocking destructor.
pub fn spawn_service<F: Future<Output = ()> + 'static>(future: F) -> Result<TaskId, RtError> {
    registry::with_current(|ctx| ctx.spawn_framed(future, None, true, true))
        .ok_or(RtError::NotOnShardThread)?
}

/// Awaits cancellation of this service. Repeated polls are level-triggered. Dropping
/// this borrowed future does not clear cancellation or release the service's slot.
pub fn cancelled() -> Cancelled {
    Cancelled
}

/// The current service's cancellation level, without registering another waiter.
/// A service checks it before admitting another operation between borrowed waits.
pub fn cancellation_requested() -> Result<bool, RtError> {
    registry::with_current(|ctx| {
        let task = ctx.current_task().ok_or(RtError::NotOnShardThread)?;
        let cell = ctx.task(task.0.slot()).ok_or(RtError::NotOnShardThread)?;
        if !cell.service.get() {
            return Err(RtError::BadConfig {
                what: "cancellation cleanup requires service admission",
            });
        }
        Ok(cell.cancelled.get())
    })
    .ok_or(RtError::NotOnShardThread)?
}

/// A service's cancellation level, with no allocation or stored waker.
#[derive(Debug)]
pub struct Cancelled;

impl Future for Cancelled {
    type Output = Result<(), RtError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(RtError::NotOnShardThread));
        };
        registry::with_current(|ctx| {
            let Some(cell) = ctx.task(word.slot()) else {
                return Poll::Ready(Err(RtError::NotOnShardThread));
            };
            if word.shard() != ctx.id || word.generation() != cell.generation.get() {
                return Poll::Ready(Err(RtError::NotOnShardThread));
            }
            if !cell.service.get() {
                return Poll::Ready(Err(RtError::BadConfig {
                    what: "cancellation cleanup requires service admission",
                }));
            }
            if cell.cancelled.get() {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
        .unwrap_or(Poll::Ready(Err(RtError::NotOnShardThread)))
    }
}

/// Spawns a detached task on the current shard: nobody joins it, and its slot is freed when it ends (a
/// server's per-connection task).
pub fn spawn_detached<F: Future<Output = ()> + 'static>(future: F) -> Result<TaskId, RtError> {
    registry::with_current(|ctx| ctx.spawn_framed(future, None, false, false))
        .ok_or(RtError::NotOnShardThread)?
}

/// Spawns a joinable child of the current task; a parent's completion cancels and joins it.
pub fn spawn_child<F: Future<Output = ()> + 'static>(future: F) -> Result<TaskId, RtError> {
    registry::with_current(|ctx| {
        let parent = ctx.current_task().map(|t| t.0.slot());
        ctx.spawn_framed(future, parent, true, false)
    })
    .ok_or(RtError::NotOnShardThread)?
}

/// Requests a task's cancellation (same shard only in this phase).
pub fn cancel(id: TaskId) -> Result<(), RtError> {
    registry::with_current(|ctx| ctx.cancel(id)).ok_or(RtError::NotOnShardThread)?
}

/// Detaches a joinable task.
pub fn detach(id: TaskId) -> Result<(), RtError> {
    registry::with_current(|ctx| ctx.detach(id)).ok_or(RtError::NotOnShardThread)?
}

/// Awaits a task's terminal state (same shard only in this phase).
pub fn join(id: TaskId) -> Join {
    Join { id }
}

/// The join future.
#[derive(Debug)]
pub struct Join {
    id: TaskId,
}

impl Future for Join {
    type Output = Result<Outcome, RtError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = self.id;
        registry::with_current(|ctx| {
            ctx.poll_join(id, crate::waker::polling_task_in(ctx, cx.waker()))
        })
        .unwrap_or(Poll::Ready(Err(RtError::NotOnShardThread)))
    }
}

/// Yields once: the task wakes itself, and so resumes after the loop has next read its inputs (its
/// control, wakes from other threads, timers, pollers and the driver's completions) and polled what they
/// made ready ([`crate::queue`]).
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// The yield future.
#[derive(Debug)]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Yields only where the shard is wanted ([`ShardContext::wanted`], docs/runtime.md §3.10): a task listed
/// or yielded, an intent unapplied, a wake or control message from another thread, a timer or the driver's
/// harvest due. Where nothing is, it is ready at once, and the task goes on without the loop's round trip,
/// which with nothing else to run would only poll it again. The cooperative point of a task that runs
/// operations back to back (Seastar's `maybe_yield`; IX's batching only under congestion). Off a shard's
/// thread it yields, as [`yield_now`] does.
///
/// [`ShardContext::wanted`]: crate::shard::ShardContext::wanted
pub fn maybe_yield() -> MaybeYield {
    MaybeYield { yielded: false }
}

/// The conditional yield's future.
#[derive(Debug)]
pub struct MaybeYield {
    yielded: bool,
}

impl Future for MaybeYield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded || !registry::with_current(|ctx| ctx.wanted()).unwrap_or(true) {
            return Poll::Ready(());
        }
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Yields without re-queueing: the task resumes only when something wakes it (a registered
/// poller's ring, a foreign wake, a cancellation). The idle form of a ring-polling task.
pub fn idle() -> Idle {
    Idle { yielded: false }
}

/// The idle future.
#[derive(Debug)]
pub struct Idle {
    yielded: bool,
}

impl Future for Idle {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }
        self.yielded = true;
        Poll::Pending
    }
}

/// Sleeps for `ns` on the shard's wheel (accuracy: one tick).
pub fn sleep(ns: u64) -> Sleep {
    Sleep {
        ns,
        deadline: None,
        timer: None,
    }
}

/// The sleep future: completes, `Ok`, no earlier than its deadline — the shard's clock at the first poll
/// plus the span. When every timer of the shard is taken it does not end early: the task waits for a timer
/// to free (counted) and arms then, or completes if its deadline passed meanwhile. It is refused only where
/// no wait can succeed: polled off a shard, or with a waker that is not a task's (`NotOnShardThread`).
/// The timer it takes is let go when it drops (slates' AUD-29-39 behaviour, through the desk's intents).
#[derive(Debug)]
pub struct Sleep {
    ns: u64,
    deadline: Option<u64>,
    timer: Option<TimerId>,
}

impl Future for Sleep {
    type Output = Result<(), RtError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(word) = polling_task(cx.waker()) else {
            return Poll::Ready(Err(RtError::NotOnShardThread));
        };
        if registry::with_current(|ctx| ctx.driver_lost()) == Some(true) {
            if let Some(id) = self.timer.take() {
                let _ = registry::with_current(|ctx| ctx.disarm_timer(id));
            }
            return Poll::Ready(Err(RtError::DriverLost));
        }
        let span = self.ns;
        let held = self.timer;
        let polled = registry::with_current(|ctx| {
            let now = ctx.now_ns();
            // A deadline past the clock's range saturates: the sleep then never completes, which is exactly "no
            // earlier than its deadline".
            let deadline = now.saturating_add(span);
            (now, deadline, held.is_some_and(|id| ctx.timer_pending(id)))
        });
        let Some((now, first_deadline, armed)) = polled else {
            return Poll::Ready(Err(RtError::NotOnShardThread));
        };
        let deadline = *self.deadline.get_or_insert(first_deadline);
        if now >= deadline {
            if let Some(id) = self.timer.take() {
                let _ = registry::with_current(|ctx| ctx.disarm_timer(id));
            }
            return Poll::Ready(Ok(()));
        }
        if armed {
            return Poll::Pending;
        }
        let taken = registry::with_current(|ctx| match ctx.arm_timer(deadline, word.word()) {
            Some(id) => Ok(Some(id)),
            None => ctx.wait_for_timer(word).map(|()| None),
        });
        match taken {
            Some(Ok(id)) => {
                self.timer = id;
                Poll::Pending
            }
            Some(Err(refusal)) => Poll::Ready(Err(refusal)),
            None => Poll::Ready(Err(RtError::NotOnShardThread)),
        }
    }
}

/// Races `work` against a deadline `ns` from now on the shard's wheel: `Ok(Some(output))` when the work
/// finishes first (preferred when both are ready), `Ok(None)` once the deadline has passed, and the sleep's
/// refusal when no deadline can be kept (polled off a shard) — never a deadline that did not pass
/// (AUD-29-39). On a wheel with no free timer the deadline, like any sleep, arms once a timer frees (counted,
/// `Counters::timer_waits`), so under timer exhaustion it is enforced late, never early; the shard's timer
/// bound is derived to cover its tasks' concurrent waits, which makes that count an overload tripwire. The one race every timed wait in the workspace uses; before 2026-09-30 each call site raced
/// a pinned sleep by hand and read a refused sleep's completion as the deadline.
pub fn within<F: Future>(ns: u64, work: F) -> Within<F> {
    Within {
        work,
        deadline: sleep(ns),
        finished: false,
    }
}

pin_project! {
    /// The wait of [`within`]: the work and its deadline, held in place. A named future rather than an
    /// `async fn`, which held the work twice (docs/runtime.md §3.8).
    #[must_use = "a timed wait does nothing unless awaited"]
    pub struct Within<F> {
        #[pin]
        work: F,
        deadline: Sleep,
        // Set once the wait has its output: the work, finished, is never polled again.
        finished: bool,
    }
}

impl<F: Future> Future for Within<F> {
    type Output = Result<Option<F::Output>, RtError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if *this.finished {
            return Poll::Pending;
        }
        let output = if let Poll::Ready(output) = this.work.poll(cx) {
            Ok(Some(output))
        } else {
            match Pin::new(&mut *this.deadline).poll(cx) {
                Poll::Ready(Ok(())) => Ok(None),
                Poll::Ready(Err(refusal)) => Err(refusal),
                Poll::Pending => return Poll::Pending,
            }
        };
        *this.finished = true;
        // The deadline's timer is let go now, not when the wait is dropped: a sleep not yet polled holds none.
        *this.deadline = sleep(0);
        Poll::Ready(output)
    }
}

impl<F> std::fmt::Debug for Within<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Within")
            .field("deadline", &self.deadline)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(id) = self.timer.take() {
            let _ = registry::with_current(|ctx| ctx.disarm_timer(id));
        }
    }
}
