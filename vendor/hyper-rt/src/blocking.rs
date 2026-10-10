//! Blocking work (docs/runtime.md §9): one bounded pool per process for work that blocks a thread — a
//! synchronous library call, `getaddrinfo`, a code sandbox. `workers` threads are started once and reused;
//! jobs wait in a queue of at most `queue`; past it, or past a share's limit, a job is refused `Capacity`
//! before any thread is asked. The consumer divides the pool into named shares so one kind of work cannot
//! take all of it. A job's result returns through a [`crate::sync::oneshot`], which a task awaits.
//!
//! **No locks** (the workspace's wall: single owners and moves over bounded channels). A dispatcher thread
//! owns the job queue's receiving end and the list of idle workers; each worker owns a one-job slot and
//! tells the dispatcher over a bounded channel when it is idle again, so a job is handed to exactly one
//! idle worker, one to one. Shares and counters are atomics.
//!
//! A job cannot be cancelled (the call is the OS's): dropping its receiver discards the result, and the
//! job's share stays charged until it ends — its share's limit bounds that damage, stated. A job that
//! panics ends the job, not its worker (an unwind boundary, CLAUDE.md §1), and its share is given back.
//! A job's share is given back before its result is delivered or its sender dropped, so a task that sees
//! the job end can submit again under the same share without a refusal.
//!
//! **One per process.** The pool's channels and shares are a process-wide static set once by [`start`];
//! [`stop`] drains the queue and joins every thread, after which a second start is refused.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread::JoinHandle;

use crate::error::RtError;
use crate::sync::{OneshotReceiver, oneshot};

/// One kind of work and the most jobs of it in the pool at once, queued or running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Share {
    /// The kind's name, as a job names it.
    pub name: &'static str,
    /// Its limit.
    pub limit: usize,
}

/// The pool's shape, the consumer's (derived from its own measurement: a device's queue and knee, a
/// sandbox's concurrency budget).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// Threads.
    pub workers: usize,
    /// Jobs that may wait for a thread.
    pub queue: usize,
    /// The kinds of work and their limits.
    pub shares: Vec<Share>,
}

/// What the pool has done.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Jobs accepted.
    pub accepted: u64,
    /// Jobs refused for a full queue or share.
    pub refused: u64,
    /// Jobs finished.
    pub finished: u64,
    /// Of those, jobs that panicked: their receivers see the sender gone; the worker and the share live on.
    pub panicked: u64,
}

type Job = Box<dyn FnOnce() + Send>;

/// What the dispatcher receives.
enum Message {
    /// A job.
    Job(Job),
    /// Drain the queue, stop the workers, join them, and say so.
    Stop(SyncSender<bool>),
}

/// A share's limit and what it holds.
struct ShareState {
    share: Share,
    held: AtomicUsize,
}

/// The pool, once started.
struct Pool {
    submit: SyncSender<Message>,
    /// The queue's bound, as a refusal reports it.
    queue: usize,
    shares: Box<[ShareState]>,
}

static POOL: OnceLock<Pool> = OnceLock::new();
static ACCEPTED: AtomicU64 = AtomicU64::new(0);
static REFUSED: AtomicU64 = AtomicU64::new(0);
static FINISHED: AtomicU64 = AtomicU64::new(0);
static PANICKED: AtomicU64 = AtomicU64::new(0);

/// Starts a named thread of the pool.
fn thread(name: String, body: impl FnOnce() + Send + 'static) -> Result<JoinHandle<()>, RtError> {
    #[allow(
        clippy::disallowed_methods,
        reason = "the blocking pool's threads (docs/runtime.md §9): started once, joined by `stop`"
    )]
    let spawned = std::thread::Builder::new().name(name).spawn(body);
    spawned.map_err(|_| RtError::BadConfig {
        what: "the OS refused a blocking pool thread",
    })
}

/// Starts the pool. Refused `BadConfig` when one was started already in this process, or for no workers
/// or no shares; a thread the OS refuses stops the start (the threads started end with their channels).
pub fn start(config: Config) -> Result<(), RtError> {
    if config.workers == 0 || config.shares.is_empty() {
        return Err(RtError::BadConfig {
            what: "a blocking pool with no workers or no shares",
        });
    }
    if POOL.get().is_some() {
        return Err(RtError::BadConfig {
            what: "a second blocking pool in one process",
        });
    }
    let (submit, jobs) = sync_channel(config.queue);
    let (idle_tx, idle) = sync_channel(config.workers);
    let mut slots = Vec::new();
    let mut workers = Vec::new();
    for index in 0..config.workers {
        let (slot, take) = sync_channel::<Job>(1);
        let idle_tx = idle_tx.clone();
        workers.push(thread(format!("hyper-rt-blocking-{index}"), move || {
            work(index, &take, &idle_tx);
        })?);
        slots.push(slot);
    }
    drop(idle_tx);
    let shares = config
        .shares
        .iter()
        .map(|share| ShareState {
            share: *share,
            held: AtomicUsize::new(0),
        })
        .collect();
    let pool = Pool {
        submit,
        queue: config.queue,
        shares,
    };
    if POOL.set(pool).is_err() {
        return Err(RtError::BadConfig {
            what: "a second blocking pool in one process",
        });
    }
    thread("hyper-rt-blocking-dispatch".to_owned(), move || {
        dispatch(jobs, &idle, slots, workers);
    })?;
    Ok(())
}

/// A worker: tells the dispatcher it is idle, takes the job it is handed, runs it, and again; ends when
/// its slot closes.
fn work(index: usize, take: &Receiver<Job>, idle: &SyncSender<usize>) {
    loop {
        if idle.send(index).is_err() {
            return;
        }
        let Ok(job) = take.recv() else {
            return;
        };
        job();
    }
}

/// The dispatcher: hands each job to an idle worker, waiting for one when all are busy; on `Stop` it closes
/// the queue, closes the slots and joins the workers, and only then answers the stop.
fn dispatch(
    jobs: Receiver<Message>,
    idle: &Receiver<usize>,
    slots: Vec<SyncSender<Job>>,
    workers: Vec<JoinHandle<()>>,
) {
    let hand_out = |job: Job| -> bool {
        let Ok(worker) = idle.recv() else {
            return false;
        };
        slots.get(worker).is_some_and(|slot| slot.send(job).is_ok())
    };
    while let Ok(message) = jobs.recv() {
        match message {
            Message::Job(job) => {
                if !hand_out(job) {
                    break;
                }
            }
            Message::Stop(done) => {
                // Jobs sent before the stop are queued ahead of it and already handed out. The queue closes
                // before the stop is answered, so a `run` once `stop` has returned is refused; it closed when
                // the dispatcher's thread ended, after the answer, and such a `run` was taken for a job that
                // never ran (CI, ubuntu-24.04, run 38003581811). A `run` that raced the stop is dropped unrun,
                // its receiver seeing the sender gone.
                drop(jobs);
                drop(slots);
                let mut clean = true;
                for worker in workers {
                    clean &= worker.join().is_ok();
                }
                let _ = done.send(clean);
                return;
            }
        }
    }
}

/// Runs `job` on the pool under `share`; its result arrives on the returned receiver. Refused `Capacity`
/// when the queue or the share is full, `BadConfig` for an unknown share or a pool not running.
pub fn run<T, F>(share: &str, job: F) -> Result<OneshotReceiver<T>, RtError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let pool = POOL.get().ok_or(RtError::BadConfig {
        what: "a blocking job with no pool running",
    })?;
    let (index, state) = pool
        .shares
        .iter()
        .enumerate()
        .find(|(_, state)| state.share.name == share)
        .ok_or(RtError::BadConfig {
            what: "a blocking job of an unknown share",
        })?;
    if state.held.fetch_add(1, Ordering::AcqRel) >= state.share.limit {
        state.held.fetch_sub(1, Ordering::AcqRel);
        REFUSED.fetch_add(1, Ordering::Relaxed);
        return Err(RtError::Capacity {
            what: "blocking pool share",
            bound: state.share.limit,
        });
    }
    let (sender, receiver) = oneshot().inspect_err(|_| {
        state.held.fetch_sub(1, Ordering::AcqRel);
    })?;
    let wrapped: Job = Box::new(move || {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
        if outcome.is_err() {
            PANICKED.fetch_add(1, Ordering::Relaxed);
        }
        // The share is given back before the receiver can see anything: a task that awaited this job's
        // result, or saw its sender gone, may submit at once and must find the slot free.
        if let Some(state) = POOL.get().and_then(|pool| pool.shares.get(index)) {
            state.held.fetch_sub(1, Ordering::AcqRel);
        }
        FINISHED.fetch_add(1, Ordering::Relaxed);
        match outcome {
            // A dropped receiver discards the result.
            Ok(value) => drop(sender.send(value)),
            Err(_) => drop(sender),
        }
    });
    match pool.submit.try_send(Message::Job(wrapped)) {
        Ok(()) => {
            ACCEPTED.fetch_add(1, Ordering::Relaxed);
            Ok(receiver)
        }
        Err(TrySendError::Full(_)) => {
            state.held.fetch_sub(1, Ordering::AcqRel);
            REFUSED.fetch_add(1, Ordering::Relaxed);
            Err(RtError::Capacity {
                what: "blocking pool queue",
                bound: pool.queue,
            })
        }
        Err(TrySendError::Disconnected(_)) => {
            state.held.fetch_sub(1, Ordering::AcqRel);
            Err(RtError::BadConfig {
                what: "a blocking job after the pool stopped",
            })
        }
    }
}

/// What the pool has done.
pub fn stats() -> Stats {
    Stats {
        accepted: ACCEPTED.load(Ordering::Relaxed),
        refused: REFUSED.load(Ordering::Relaxed),
        finished: FINISHED.load(Ordering::Relaxed),
        panicked: PANICKED.load(Ordering::Relaxed),
    }
}

/// Stops the pool: the jobs queued still run, then every thread ends and is joined. Blocks the calling
/// thread on the jobs running, which cannot be cancelled; call it from the consumer's shutdown path, not
/// from a shard.
pub fn stop() -> Result<(), RtError> {
    let pool = POOL.get().ok_or(RtError::BadConfig {
        what: "stopping a blocking pool that never started",
    })?;
    let (done, joined) = sync_channel(1);
    pool.submit
        .send(Message::Stop(done))
        .map_err(|_| RtError::BadConfig {
            what: "the blocking pool stopped already",
        })?;
    match joined.recv() {
        Ok(true) => Ok(()),
        Ok(false) => Err(RtError::BadConfig {
            what: "a blocking pool worker ended abnormally",
        }),
        Err(_) => Err(RtError::BadConfig {
            what: "the blocking pool's dispatcher ended abnormally",
        }),
    }
}
