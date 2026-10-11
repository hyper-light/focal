#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! focal on hyper-rt (the migration of 2026-10-05, 27 §15): every async task runs on hyper-rt's
//! thread-per-core runtime, its wakers `Copy`, its tasks in arenas, nothing work-stolen and nothing
//! reference counted. This crate is how focal shapes it: the runtime of one shard a test, a short
//! command or a measurement runs on, and a runtime of several shards for work that needs the
//! machine's cores at once.
//!
//! A shard's tasks are polled on its thread only; a future spawned with
//! [`hyper_rt::futures::spawn`] stays on the shard that spawned it. Work that must run elsewhere
//! goes by [`Runtime::spawn_on`] (a `Send` future), and values cross between shards and plain
//! threads through `hyper_rt::sync`, whose every primitive is bounded.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper_rt::runtime::{LocalRuntime, interests_for};
pub use hyper_rt::{RtError, Runtime, RuntimeConfig};

pub mod time;
pub mod watch;

/// Tasks one shard of the local runtime holds at once: what a test or a command keeps in flight,
/// its connections' exchanges among them. A task past it is refused `TooManyTasks`, typed, and the
/// refusal is the caller's to handle (`docs/archictecutre/27` §15: a bound reached is a refusal).
/// The node's own runtime is sized by Little's law on what it serves instead (`hyper_rt::runtime::
/// admission_limit`).
pub const LOCAL_TASKS: usize = 16_384;

/// Timers one shard of the local runtime holds at once: a timer for each task, one wait each, so a
/// shard's tasks never contend for a timer slot.
pub const LOCAL_TIMERS: usize = LOCAL_TASKS;

/// The inbound ring of a local shard: the messages another shard or thread may queue for it between
/// two of its loop's drains (a wake, a spawn, a cancellation). One per task: a shard's tasks are each
/// woken at most once per drain.
pub const LOCAL_RING: usize = LOCAL_TASKS;

/// The step budget of a local shard, nanoseconds: how long one task's poll may hold the shard before
/// the watchdog counts it late. A local runtime has no calibration to derive it from (a test or a
/// command does not pay the machine's probes, `hyper_rt::machine::calibration`); one millisecond is
/// RFC 9002's `kGranularity`, the coarsest timer a transport's deadlines are judged at, so a step
/// that holds the shard longer delays every timer behind it past what the transport assumes.
pub const LOCAL_STEP_NS: u64 = 1_000_000;

/// The timing wheel's tick on a local shard, nanoseconds: a tenth of the step budget, so a timer
/// fires within a tenth of the transport's granularity of its deadline on an idle shard.
pub const LOCAL_TICK_NS: u64 = LOCAL_STEP_NS / 10;

/// The items one loop phase of a local shard takes: a sixty-fourth of its ring, so a full ring is
/// drained in sixty-four phases and no phase holds the shard for a whole ring's work.
pub const LOCAL_BATCH: usize = LOCAL_RING / 64;

/// The base page the task slab's segments are sized by, bytes: the smallest base page of the six
/// targets (4 KiB on x86-64 and on Linux and Windows on aarch64; macOS on aarch64 has 16 KiB, of
/// which a segment then uses a quarter).
pub const LOCAL_PAGE_BYTES: usize = 4096;

/// The configuration of a local shard: fixed quanta, no calibration, no pinning, no spin (a test or
/// a command shares the machine; it parks at once when idle).
pub fn local_config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: LOCAL_TASKS,
        timers_per_shard: LOCAL_TIMERS,
        interests_per_shard: interests_for(LOCAL_TASKS),
        ring_entries: LOCAL_RING,
        step_budget_ns: LOCAL_STEP_NS,
        timer_tick_ns: LOCAL_TICK_NS,
        batch: LOCAL_BATCH,
        pin: false,
        cores: Vec::new(),
        page_bytes: LOCAL_PAGE_BYTES,
        spin_ns: 0,
        wake_tracking: None,
    }
}

/// Runs `future` to completion as the root task of one shard on this thread; whatever it spawned and
/// left running is cancelled and joined before this returns.
///
/// The future is boxed on entry: a root is often large (a command's or a test's whole state), and a
/// debug build copies a value on the stack each time it is passed on, so only the pointer travels
/// past here.
pub fn block_on<T: 'static>(future: impl Future<Output = T> + 'static) -> Result<T, RtError> {
    let future = Box::pin(future);
    let mut runtime = LocalRuntime::new(&local_config())?;
    runtime.block_on(future)
}

/// Runs `future` on the first of `shards` shards, each on a thread of its own, this thread waiting
/// for its output; the runtime is shut down before this returns. For work whose tasks must run at
/// once on several cores (a fleet's nodes, a measurement under load), placed with
/// [`Runtime::spawn_on`] from inside `future` through [`hyper_rt::registry`].
pub fn block_on_shards<T: Send + 'static>(
    shards: u16,
    future: impl Future<Output = T> + Send + 'static,
) -> Result<T, RtError> {
    let future = Box::pin(future);
    let config = RuntimeConfig {
        shards,
        ..local_config()
    };
    let runtime = Runtime::start(&config)?;
    let first = *runtime.shard_ids().first().ok_or(RtError::BadConfig {
        what: "a runtime of no shards",
    })?;
    let (sender, receiver) = hyper_rt::sync::oneshot()?;
    runtime.spawn_on(first, async move {
        let _ = sender.send(future.await);
    })?;
    let output = receiver
        .blocking_recv()
        .map_err(|_| RtError::ShardGone { shard: first.0 });
    runtime.shutdown()?;
    output
}

/// Runs `future` on one simulated shard (`hyper_rt::SimRuntime`, seeded by `seed`): virtual time,
/// which moves only to the earliest deadline once nothing can run, so a test of waits measured in
/// seconds takes none and gives the same order every run. `None` when the future never finished:
/// it waited on something that never came.
pub fn simulate<T: Send + 'static>(
    seed: u64,
    future: impl Future<Output = T> + Send + 'static,
) -> Result<Option<T>, RtError> {
    let future = Box::pin(future);
    let mut sim = hyper_rt::SimRuntime::new(&local_config(), seed)?;
    let shard = *sim.shard_ids().first().ok_or(RtError::BadConfig {
        what: "a runtime of no shards",
    })?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    sim.spawn_on(shard, async move {
        let _ = sender.send(future.await);
    })?;
    sim.run_until_idle();
    Ok(receiver.try_recv().ok())
}

/// Futures run together in the task that polls them, each output taken as it ends: what
/// `futures_util`'s `FuturesUnordered` was before the migration. A hyper-rt future registers the task
/// polling it by that task's own waker, and refuses any other (`NotOnShardThread`: the waker's word
/// names the task, `hyper_rt::waker`), so a set cannot give each future a waker of its own to learn
/// which one a wake was for. A wake polls every future still running instead, from one past the
/// future that ended last, so none is always polled last; it costs as many polls as futures are
/// running, never more than the set's bound. Biased only by that rotation, and deterministic, as the
/// simulation needs to replay a run from its seed.
///
/// At most `capacity` futures at once: a push past it is refused, typed. The slots are reserved
/// when the set is made and reused as futures end, so a set never grows past its first reservation.
///
/// Futures of different kinds share a set as one `dyn Future` type, each pushed boxed once
/// ([`Concurrent::push_pinned`]).
pub struct Concurrent<F: ?Sized> {
    slots: Vec<Option<Pin<Box<F>>>>,
    /// Slots whose future has ended, taken before the slots grow.
    free: Vec<usize>,
    running: usize,
    /// Where the next poll starts.
    start: usize,
    capacity: usize,
}

impl<F: Future + ?Sized> Concurrent<F> {
    /// An empty set of at most `capacity` futures.
    pub fn new(capacity: usize) -> Result<Self, RtError> {
        let refused = RtError::Capacity {
            what: "concurrent futures",
            bound: capacity,
        };
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|_| refused.clone())?;
        let mut free = Vec::new();
        free.try_reserve_exact(capacity).map_err(|_| refused)?;
        Ok(Self {
            slots,
            free,
            running: 0,
            start: 0,
            capacity,
        })
    }

    /// Adds `future`, first polled by the next [`Concurrent::join_next`].
    pub fn push(&mut self, future: F) -> Result<(), RtError>
    where
        F: Sized,
    {
        self.push_pinned(Box::pin(future))
    }

    /// Adds a future already boxed: one of a set of `dyn Future`s.
    pub fn push_pinned(&mut self, future: Pin<Box<F>>) -> Result<(), RtError> {
        if self.running >= self.capacity {
            return Err(RtError::Capacity {
                what: "concurrent futures",
                bound: self.capacity,
            });
        }
        let future = Some(future);
        match self.free.pop() {
            Some(index) => match self.slots.get_mut(index) {
                Some(slot) => *slot = future,
                None => {
                    return Err(RtError::Capacity {
                        what: "concurrent futures",
                        bound: self.capacity,
                    });
                }
            },
            // No slot is free, so every slot holds a running future and there are fewer than
            // `capacity`: within the reservation, the push never reallocates.
            None => self.slots.push(future),
        }
        self.running = self.running.saturating_add(1);
        Ok(())
    }

    /// Futures still running.
    pub fn len(&self) -> usize {
        self.running
    }

    /// Whether no future is running.
    pub fn is_empty(&self) -> bool {
        self.running == 0
    }

    /// Drops every future still running.
    pub fn clear(&mut self) {
        self.slots.clear();
        self.free.clear();
        self.running = 0;
        self.start = 0;
    }

    /// The output of the next future to end; `None` when none is running.
    pub fn join_next(&mut self) -> impl Future<Output = Option<F::Output>> + '_ {
        std::future::poll_fn(move |cx| self.poll_next(cx))
    }

    /// Polls the futures still running, from one past the last to end, until one ends.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
        if self.running == 0 {
            return Poll::Ready(None);
        }
        let count = self.slots.len();
        let start = self.start.checked_rem(count).unwrap_or(0);
        let order = (start..count).chain(0..start);
        for index in order {
            let Some(slot) = self.slots.get_mut(index) else {
                continue;
            };
            let Some(future) = slot.as_mut() else {
                continue;
            };
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                *slot = None;
                // At most one entry for each slot: within the reservation.
                self.free.push(index);
                self.running = self.running.saturating_sub(1);
                self.start = index.saturating_add(1);
                return Poll::Ready(Some(output));
            }
        }
        Poll::Pending
    }
}

/// `make`'s future, boxed where it is made: its value is built in this function's frame, gone before
/// the caller polls it. A debug build keeps each future an `async fn` makes as a temporary of its own
/// poll frame, at once; a caller that awaits many large futures, made through this, holds a pointer
/// for each, and its frame stays small however deep the chain of awaits beneath it.
pub fn boxed<'a, F: Future + 'a>(
    make: impl FnOnce() -> F,
) -> Pin<Box<dyn Future<Output = F::Output> + 'a>> {
    Box::pin(make())
}

/// A task spawned on the current shard whose output is awaited later (what tokio's `JoinHandle`
/// was): awaited, its output, or [`JoinError`] when it ended without one. Dropping the handle
/// leaves the task running; [`JoinHandle::abort`] cancels it.
#[derive(Debug)]
pub struct JoinHandle<T> {
    output: std::cell::RefCell<hyper_rt::sync::OneshotReceiver<T>>,
    /// What [`JoinHandle::is_finished`] found ended, kept for the join.
    ended: std::cell::RefCell<Option<Result<T, JoinError>>>,
    task: hyper_rt::TaskId,
    /// Whether [`JoinHandle::abort`] cancelled it: an output it never gave is then `Cancelled`.
    aborted: std::cell::Cell<bool>,
}

/// Why a joined task gave no output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum JoinError {
    /// Its handle cancelled it before it ended.
    #[error("the task was cancelled")]
    Cancelled,
    /// It ended without its output: it unwound, or its shard cancelled it.
    #[error("the task ended without its output")]
    Lost,
    /// The join was awaited off a shard: no wake could be kept.
    #[error("no wait for the task could be kept")]
    Refused,
}

/// Spawns `future` on the current shard, its output kept for the handle. Refused off a shard, at
/// the shard's task bound, or at the runtime's cell bound.
pub fn spawn<T: 'static>(
    future: impl Future<Output = T> + 'static,
) -> Result<JoinHandle<T>, RtError> {
    // Boxed first: the task's frame holds the pointer, never a copy of a large future.
    let future = Box::pin(future);
    let (sender, output) = hyper_rt::sync::oneshot()?;
    let task = hyper_rt::futures::spawn_detached(async move {
        // A handle dropped before the end leaves nobody to take the output.
        let _ = sender.send(future.await);
    })?;
    Ok(JoinHandle {
        output: std::cell::RefCell::new(output),
        ended: std::cell::RefCell::new(None),
        task,
        aborted: std::cell::Cell::new(false),
    })
}

impl JoinError {
    /// Whether the handle cancelled the task (tokio's `JoinError::is_cancelled`).
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

impl<T> JoinHandle<T> {
    /// Cancels the task; what it held is dropped as it ends, and the handle's join is
    /// `Cancelled` unless the output came first.
    pub fn abort(&self) {
        self.aborted.set(true);
        // A task already ended has nothing to cancel.
        let _ = hyper_rt::futures::cancel(self.task);
    }

    /// Whether the task has ended: its output is in, or it ended without one.
    pub fn is_finished(&self) -> bool {
        if self.ended.borrow().is_some() {
            return true;
        }
        let ended = match self.output.borrow_mut().try_recv() {
            Ok(Some(output)) => Ok(output),
            Ok(None) => return false,
            Err(_) => Err(self.lost()),
        };
        *self.ended.borrow_mut() = Some(ended);
        true
    }

    fn lost(&self) -> JoinError {
        if self.aborted.get() {
            JoinError::Cancelled
        } else {
            JoinError::Lost
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(ended) = self.ended.borrow_mut().take() {
            return Poll::Ready(ended);
        }
        let lost = self.lost();
        let mut output = self.output.borrow_mut();
        Pin::new(&mut *output).poll(cx).map(|output| {
            output.map_err(|refused| match refused {
                hyper_rt::sync::SyncError::NotOnShardThread(()) => JoinError::Refused,
                _ => lost,
            })
        })
    }
}

/// Every one of `futures`' outputs, in their order, the futures running together in this task: as
/// many at once as were given, which bounds them.
pub async fn join_all<F: Future>(
    futures: impl IntoIterator<Item = F>,
) -> Result<Vec<F::Output>, RtError> {
    let futures: Vec<F> = futures.into_iter().collect();
    let count = futures.len();
    let mut set = Concurrent::new(count)?;
    for (index, future) in futures.into_iter().enumerate() {
        set.push(async move { (index, future.await) })?;
    }
    let mut ended = Vec::new();
    ended
        .try_reserve_exact(count)
        .map_err(|_| RtError::Capacity {
            what: "joined outputs",
            bound: count,
        })?;
    while let Some(output) = set.join_next().await {
        ended.push(output);
    }
    // As they ended, then in the order given: each output is kept, none dropped.
    ended.sort_unstable_by_key(|(index, _)| *index);
    Ok(ended.into_iter().map(|(_, output)| output).collect())
}

/// The jobs that may wait for a place of one of the blocking pool's shares: a job waits only while
/// a task awaits its result, and a root holds at most [`LOCAL_TASKS`] tasks, so a line this long is
/// never what refuses an awaited job. The pool holds a waiting job's memory only while it waits.
const WAITING: usize = LOCAL_TASKS;

/// The blocking pool's share name resolution runs under (`hyper_rt::dns::SHARE`): four lookups at
/// once, what a node resolves in one round of its peers' announced names (24 §24) beside a client's
/// endpoint; the lookups past them wait their turn, in order.
pub const DNS: hyper_rt::blocking::Share = hyper_rt::blocking::Share {
    name: hyper_rt::dns::SHARE,
    limit: 4,
    waiting: WAITING,
};

/// The share whole-file work runs under (a backup's copy, a bootstrap's store setup): two at once,
/// a device's sequential stream each, so the work never takes the threads resolution needs; the
/// work past them waits its turn, in order, as nodes starting together do.
pub const FILES: hyper_rt::blocking::Share = hyper_rt::blocking::Share {
    name: "files",
    limit: 2,
    waiting: WAITING,
};

/// The share long computation runs under (a placement's rewrite of its directory): two at once, so
/// a rewrite in flight never holds every worker; the rewrites past them wait their turn, in order.
pub const WORK: hyper_rt::blocking::Share = hyper_rt::blocking::Share {
    name: "work",
    limit: 2,
    waiting: WAITING,
};

/// Starts the process's blocking pool with focal's shares, once; a later call reports how the first
/// went. A worker for each share's every place, so no job of a share waits for another share's.
pub fn blocking() -> Result<(), RtError> {
    static STARTED: std::sync::OnceLock<Result<(), RtError>> = std::sync::OnceLock::new();
    STARTED
        .get_or_init(|| {
            let shares = vec![DNS, FILES, WORK];
            let workers = shares
                .iter()
                .map(|share| share.limit)
                .fold(0usize, usize::saturating_add);
            hyper_rt::blocking::start(hyper_rt::blocking::Config { workers, shares })
        })
        .clone()
}

/// Why work handed to the blocking pool gave no result.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PoolError {
    /// The pool refused the job: its share's places and line full, or no pool running.
    #[error("the blocking pool refused the job: {0}")]
    Refused(RtError),
    /// The job ended without its result: it unwound on its worker.
    #[error("the blocking job ended without its result")]
    Lost,
}

/// Runs `job` on the process's blocking pool under `share` (the pool started if it was not) and
/// waits for its result as a task: whole-file work, a computation, what must not hold a shard.
pub async fn on_pool<T: Send + 'static>(
    share: &hyper_rt::blocking::Share,
    job: impl FnOnce() -> T + Send + 'static,
) -> Result<T, PoolError> {
    blocking().map_err(PoolError::Refused)?;
    let output = hyper_rt::blocking::run(share.name, job).map_err(PoolError::Refused)?;
    output.await.map_err(|_| PoolError::Lost)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_spawned_task_gives_its_output_to_its_handle_and_an_aborted_one_is_cancelled() {
        let (output, aborted) = block_on(async {
            let answered = spawn(async {
                hyper_rt::futures::sleep(1_000_000).await.unwrap();
                5u8
            })
            .unwrap();
            let waiting = spawn(std::future::pending::<u8>()).unwrap();
            hyper_rt::futures::yield_now().await;
            assert!(!waiting.is_finished());
            waiting.abort();
            hyper_rt::futures::yield_now().await;
            assert!(waiting.is_finished());
            (answered.await, waiting.await)
        })
        .unwrap();
        assert_eq!(output, Ok(5));
        assert_eq!(aborted, Err(JoinError::Cancelled));
    }

    #[test]
    fn join_all_gives_every_output_in_the_order_given_however_they_end() {
        let outputs = block_on(async {
            join_all(
                [30_000_000u64, 10_000_000, 20_000_000].map(|after| async move {
                    hyper_rt::futures::sleep(after).await.unwrap();
                    after
                }),
            )
            .await
        })
        .unwrap()
        .unwrap();
        assert_eq!(outputs, vec![30_000_000, 10_000_000, 20_000_000]);
    }

    #[test]
    fn a_local_shard_runs_a_future_and_its_timers() {
        let slept = block_on(async {
            hyper_rt::futures::sleep(2_000_000).await.unwrap();
            7
        })
        .unwrap();
        assert_eq!(slept, 7);
    }

    #[test]
    fn several_shards_run_a_future_whose_tasks_answer_across_them() {
        let answered = block_on_shards(2, async {
            let (sender, mut receiver) = hyper_rt::sync::channel::<u64>(4).unwrap();
            let shard = hyper_rt::futures::shard_id().unwrap();
            for value in 0..4u64 {
                let sender = sender.clone();
                hyper_rt::futures::spawn_detached(async move {
                    sender.send(value).await.unwrap();
                })
                .unwrap();
            }
            let mut sum = 0;
            for _ in 0..4 {
                sum += receiver.recv().await.unwrap();
            }
            (shard, sum)
        })
        .unwrap();
        assert_eq!(answered.1, 6);
    }

    #[test]
    fn a_simulated_shard_runs_a_second_of_waits_in_no_time_and_tells_one_that_never_ends() {
        let began = std::time::Instant::now();
        let now = simulate(7, async {
            hyper_rt::futures::sleep(1_000_000_000).await.unwrap();
            hyper_rt::futures::now_ns()
        })
        .unwrap()
        .unwrap();
        assert!(now >= 1_000_000_000);
        assert!(began.elapsed() < std::time::Duration::from_secs(1));
        let never = simulate(7, std::future::pending::<()>()).unwrap();
        assert_eq!(never, None);
    }

    #[test]
    fn the_blocking_pool_starts_once_and_resolves_names() {
        blocking().unwrap();
        blocking().unwrap();
        let resolved =
            block_on(async { hyper_rt::dns::resolve("localhost", 7443, 4).await }).unwrap();
        assert!(resolved.is_ok_and(|addresses| addresses.iter().all(|a| a.port() == 7443)));
    }

    #[test]
    fn concurrent_futures_end_as_their_waits_do_each_registering_the_task_that_polls_it() {
        // Timers and a oneshot one future of the set sends another: under a combinator that gave
        // each future a waker of its own, every one of these waits would be refused.
        type Waiting = Pin<Box<dyn Future<Output = Result<u64, RtError>> + Send>>;
        let (order, now) = simulate(3, async {
            let mut set: Concurrent<Waiting> = Concurrent::new(4).unwrap();
            let (sender, receiver) = hyper_rt::sync::oneshot::<u64>().unwrap();
            for (value, after) in [(1u64, 30_000_000u64), (2, 10_000_000)] {
                set.push(Box::pin(async move {
                    hyper_rt::futures::sleep(after).await.map(|()| value)
                }))
                .unwrap();
            }
            set.push(Box::pin(async move {
                receiver.await.map_err(|_| RtError::NotOnShardThread)
            }))
            .unwrap();
            set.push(Box::pin(async move {
                hyper_rt::futures::sleep(20_000_000).await?;
                sender.send(3).map_err(|_| RtError::NotOnShardThread)?;
                Ok(4)
            }))
            .unwrap();
            let mut order = Vec::new();
            while let Some(ended) = set.join_next().await {
                order.push(ended.unwrap());
            }
            (order, hyper_rt::futures::now_ns())
        })
        .unwrap()
        .unwrap();
        assert_eq!(order, vec![2, 4, 3, 1]);
        assert!(now >= 30_000_000, "{now}");
    }

    #[test]
    fn a_concurrent_set_refuses_past_its_bound_and_reuses_an_ended_futures_slot_in_turn() {
        block_on(async {
            let mut set = Concurrent::new(3).unwrap();
            set.push(std::future::ready(1)).unwrap();
            set.push(std::future::ready(2)).unwrap();
            set.push(std::future::ready(3)).unwrap();
            assert!(matches!(
                set.push(std::future::ready(4)),
                Err(RtError::Capacity { bound: 3, .. })
            ));
            assert_eq!(set.join_next().await, Some(1));
            // Into the first slot, which the next poll reaches last: the futures after the one
            // that ended go first.
            set.push(std::future::ready(4)).unwrap();
            assert_eq!(set.len(), 3);
            assert_eq!(set.join_next().await, Some(2));
            assert_eq!(set.join_next().await, Some(3));
            assert_eq!(set.join_next().await, Some(4));
            assert_eq!(set.join_next().await, None);
            assert!(set.is_empty());
            // Never past its first reservation.
            assert_eq!(set.slots.capacity(), 3);
            set.push(std::future::ready(5)).unwrap();
            set.clear();
            assert_eq!(set.join_next().await, None);
        })
        .unwrap();
    }

    #[test]
    fn the_local_configuration_is_one_the_runtime_takes() {
        let config = local_config();
        assert_eq!(config.shards, 1);
        assert!(config.batch >= 1 && config.batch <= config.ring_entries);
        block_on(async {}).unwrap();
    }
}
