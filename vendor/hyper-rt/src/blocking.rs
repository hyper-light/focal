//! Blocking work (docs/runtime.md §9): one bounded pool per process for work that blocks a thread — a
//! synchronous library call, `getaddrinfo`, a code sandbox. `workers` threads are started once and reused.
//! The consumer divides the pool into named shares so one kind of work cannot take all of it: a share runs
//! at most `limit` of its jobs at once, and the jobs past those wait for one of its places, in the order
//! given, up to `waiting` of them; past that a job is refused `Capacity` before any thread is asked. A job
//! is never refused because its share is busy while its line has room: essential work queues for its
//! turn, as a busy machine's work must. A job's result returns through a [`crate::sync::oneshot`], which a
//! task awaits.
//!
//! **No locks** (the workspace's wall: single owners and moves over channels). A dispatcher thread owns
//! the shares' lines, their running counts and the list of idle workers; each worker owns a one-job slot
//! and tells the dispatcher when it is idle again and which share's job it finished, so a job is handed to
//! exactly one idle worker, one to one. A share's admitted jobs (running or waiting) are an atomic count,
//! checked as a job is submitted.
//!
//! **Bounded by admission.** The dispatcher's channel holds at most the jobs admitted (each share's
//! `limit + waiting`), one idle notice per worker and one stop, so it is bounded by the admission count,
//! not preallocated: the pool's memory is that of the jobs it holds.
//!
//! A job cannot be cancelled (the call is the OS's): dropping its receiver discards the result, and the
//! job's share stays charged until it ends — its share's bound bounds that damage, stated. A job that
//! panics ends the job, not its worker (an unwind boundary, CLAUDE.md §1), and its share is given back.
//! A job's share is given back before its result is delivered or its sender dropped, so a task that sees
//! the job end can submit again under the same share without a refusal.
//!
//! **One per process.** The pool's channels and shares are a process-wide static set once by [`start`];
//! [`stop`] runs the jobs admitted and joins every thread, after which a second start is refused.

use std::collections::VecDeque;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};
use std::thread::JoinHandle;

use crate::error::RtError;
use crate::sync::{OneshotReceiver, oneshot};

/// One kind of work: the most jobs of it running at once, and the most waiting for one of those places.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Share {
    /// The kind's name, as a job names it.
    pub name: &'static str,
    /// The most of its jobs running at once: the threads it may hold.
    pub limit: usize,
    /// The most of its jobs waiting for one of its places, in the order given; past them a job is
    /// refused. Zero refuses a job the moment the share is full.
    pub waiting: usize,
}

impl Share {
    /// A share of `limit` places and no line: past its places a job is refused at once, as the pool
    /// refused before shares had lines. The constructor a consumer's `Share { name, limit }` from
    /// before `waiting` becomes.
    #[must_use]
    pub const fn new(name: &'static str, limit: usize) -> Self {
        Self {
            name,
            limit,
            waiting: 0,
        }
    }
}

/// The pool's shape, the consumer's (derived from its own measurement: a device's queue and knee, a
/// sandbox's concurrency budget).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// Threads.
    pub workers: usize,
    /// The kinds of work, their limits and their lines.
    pub shares: Vec<Share>,
}

/// What the pool has done.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Jobs accepted.
    pub accepted: u64,
    /// Jobs refused for a share whose places and line were full.
    pub refused: u64,
    /// Jobs finished.
    pub finished: u64,
    /// Of those, jobs that panicked: their receivers see the sender gone; the worker and the share live on.
    pub panicked: u64,
}

type Job = Box<dyn FnOnce() + Send>;

/// What the dispatcher receives.
enum Message {
    /// A job of the share at `share`.
    Job { share: usize, job: Job },
    /// A worker is idle again, after finishing a job of `finished` when it ran one.
    Idle {
        worker: usize,
        finished: Option<usize>,
    },
    /// Run the jobs admitted, stop the workers, join them, and say so.
    Stop(SyncSender<bool>),
}

/// A share's bounds and the jobs it has admitted, running or waiting.
struct ShareState {
    share: Share,
    held: AtomicUsize,
}

/// The pool, once started.
struct Pool {
    submit: Sender<Message>,
    shares: Box<[ShareState]>,
    /// Set as [`stop`] begins: a job submitted from then on is refused.
    stopping: AtomicBool,
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

/// Starts the pool. Refused `BadConfig` when one was started already in this process, for no workers or
/// no shares, or for a share with no place; a thread the OS refuses stops the start (the threads started
/// end with their channels).
pub fn start(config: Config) -> Result<(), RtError> {
    if config.workers == 0
        || config.shares.is_empty()
        || config.shares.iter().any(|share| share.limit == 0)
    {
        return Err(RtError::BadConfig {
            what: "a blocking pool with no workers, no shares, or a share with no place",
        });
    }
    if POOL.get().is_some() {
        return Err(RtError::BadConfig {
            what: "a second blocking pool in one process",
        });
    }
    let (submit, messages) = channel();
    let mut slots = Vec::new();
    let mut workers = Vec::new();
    for index in 0..config.workers {
        let (slot, take) = sync_channel::<(usize, Job)>(1);
        let notify = submit.clone();
        workers.push(thread(format!("hyper-rt-blocking-{index}"), move || {
            work(index, &take, &notify);
        })?);
        slots.push(slot);
    }
    let shares: Box<[ShareState]> = config
        .shares
        .iter()
        .map(|share| ShareState {
            share: *share,
            held: AtomicUsize::new(0),
        })
        .collect();
    let limits: Vec<usize> = config.shares.iter().map(|share| share.limit).collect();
    let pool = Pool {
        submit,
        shares,
        stopping: AtomicBool::new(false),
    };
    if POOL.set(pool).is_err() {
        return Err(RtError::BadConfig {
            what: "a second blocking pool in one process",
        });
    }
    thread("hyper-rt-blocking-dispatch".to_owned(), move || {
        Dispatcher::new(slots, limits).run(&messages, workers);
    })?;
    Ok(())
}

/// A worker: tells the dispatcher it is idle (and which share's job it finished), takes the job it is
/// handed, runs it, and again; ends when its slot or the dispatcher's channel closes.
fn work(index: usize, take: &Receiver<(usize, Job)>, notify: &Sender<Message>) {
    let mut finished = None;
    loop {
        let idle = Message::Idle {
            worker: index,
            finished,
        };
        if notify.send(idle).is_err() {
            return;
        }
        let Ok((share, job)) = take.recv() else {
            return;
        };
        job();
        finished = Some(share);
    }
}

/// The dispatcher's own state: each share's line and running count, and the workers idle.
struct Dispatcher {
    slots: Vec<SyncSender<(usize, Job)>>,
    limits: Vec<usize>,
    running: Vec<usize>,
    lines: Vec<VecDeque<Job>>,
    idle: Vec<usize>,
    /// The share the next hand-out looks at first, so the shares take the idle workers in turn.
    next: usize,
}

impl Dispatcher {
    fn new(slots: Vec<SyncSender<(usize, Job)>>, limits: Vec<usize>) -> Self {
        let shares = limits.len();
        Self {
            slots,
            limits,
            running: vec![0; shares],
            lines: (0..shares).map(|_| VecDeque::new()).collect(),
            idle: Vec::new(),
            next: 0,
        }
    }

    /// Takes each message, hands out what can run, and on `Stop` runs what was admitted before closing
    /// the slots, joining the workers and only then answering.
    fn run(mut self, messages: &Receiver<Message>, workers: Vec<JoinHandle<()>>) {
        let total = workers.len();
        let mut stopping = None;
        while let Ok(message) = messages.recv() {
            match message {
                Message::Job { share, job } => {
                    if let Some(line) = self.lines.get_mut(share) {
                        line.push_back(job);
                    }
                }
                Message::Idle { worker, finished } => self.idle_again(worker, finished),
                Message::Stop(done) => stopping = Some(done),
            }
            self.hand_out();
            let drained = self.lines.iter().all(VecDeque::is_empty) && self.idle.len() == total;
            if drained && let Some(done) = stopping.take() {
                // A job submitted once the stop began is refused. One that raced the stop's start ran
                // if it reached the lines before they drained, and is dropped unrun after, its
                // receiver seeing the sender gone. The slots close and the channel's receiver goes
                // before the stop is answered, so a `run` once `stop` has returned is refused (CI,
                // ubuntu-24.04, run 38003581811).
                drop(self.slots);
                let mut clean = true;
                for worker in workers {
                    clean &= worker.join().is_ok();
                }
                let _ = done.send(clean);
                return;
            }
        }
    }

    /// A worker idle again: the share whose job it finished gives back a place.
    fn idle_again(&mut self, worker: usize, finished: Option<usize>) {
        if let Some(running) = finished.and_then(|share| self.running.get_mut(share)) {
            *running = running.saturating_sub(1);
        }
        self.idle.push(worker);
    }

    /// Hands each idle worker the next job of a share with a free place, the shares taken in turn and each
    /// share's jobs in the order given.
    fn hand_out(&mut self) {
        let shares = self.lines.len();
        while !self.idle.is_empty() {
            let Some(share) = (0..shares)
                .filter_map(|offset| self.next.checked_add(offset)?.checked_rem(shares))
                .find(|&share| self.can_run(share))
            else {
                return;
            };
            let (Some(job), Some(worker)) = (
                self.lines.get_mut(share).and_then(VecDeque::pop_front),
                self.idle.pop(),
            ) else {
                return;
            };
            if let Some(running) = self.running.get_mut(share) {
                *running = running.saturating_add(1);
            }
            self.next = share
                .checked_add(1)
                .and_then(|next| next.checked_rem(shares))
                .unwrap_or(0);
            // A worker announced idle holds an empty slot of one, so the send never waits; a worker
            // whose thread ended leaves the job unrun, its receiver seeing the sender gone.
            if let Some(slot) = self.slots.get(worker) {
                let _ = slot.send((share, job));
            }
        }
    }

    /// Whether the share at `share` has a job waiting and a place free.
    fn can_run(&self, share: usize) -> bool {
        let waiting = self.lines.get(share).is_some_and(|line| !line.is_empty());
        let free = match (self.running.get(share), self.limits.get(share)) {
            (Some(running), Some(limit)) => running < limit,
            _ => false,
        };
        waiting && free
    }
}

/// Runs `job` on the pool under `share`; its result arrives on the returned receiver. The job runs when
/// one of its share's places frees, after the share's jobs submitted before it. Refused `Capacity` when
/// the share's places and line are full, `BadConfig` for an unknown share, a pool not running or one
/// stopping.
pub fn run<T, F>(share: &str, job: F) -> Result<OneshotReceiver<T>, RtError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let pool = POOL.get().ok_or(RtError::BadConfig {
        what: "a blocking job with no pool running",
    })?;
    if pool.stopping.load(Ordering::Acquire) {
        return Err(RtError::BadConfig {
            what: "a blocking job after the pool stopped",
        });
    }
    let (index, state) = pool
        .shares
        .iter()
        .enumerate()
        .find(|(_, state)| state.share.name == share)
        .ok_or(RtError::BadConfig {
            what: "a blocking job of an unknown share",
        })?;
    let bound = state.share.limit.saturating_add(state.share.waiting);
    if state.held.fetch_add(1, Ordering::AcqRel) >= bound {
        state.held.fetch_sub(1, Ordering::AcqRel);
        REFUSED.fetch_add(1, Ordering::Relaxed);
        return Err(RtError::Capacity {
            what: "blocking pool share",
            bound,
        });
    }
    let (sender, receiver) = oneshot().inspect_err(|_| {
        state.held.fetch_sub(1, Ordering::AcqRel);
    })?;
    let wrapped: Job = Box::new(move || {
        let outcome = crate::unwind::contained(job);
        if outcome.is_none() {
            PANICKED.fetch_add(1, Ordering::Relaxed);
        }
        // The share is given back before the receiver can see anything: a task that awaited this job's
        // result, or saw its sender gone, may submit at once and must find the slot free.
        if let Some(state) = POOL.get().and_then(|pool| pool.shares.get(index)) {
            state.held.fetch_sub(1, Ordering::AcqRel);
        }
        FINISHED.fetch_add(1, Ordering::Relaxed);
        // A dropped receiver discards the result, whose drop is the job's code: inside an unwind
        // boundary too.
        match outcome {
            Some(value) => {
                let _ = crate::unwind::contained(move || drop(sender.send(value)));
            }
            None => drop(sender),
        }
    });
    match pool.submit.send(Message::Job {
        share: index,
        job: wrapped,
    }) {
        Ok(()) => {
            ACCEPTED.fetch_add(1, Ordering::Relaxed);
            Ok(receiver)
        }
        Err(_) => {
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

/// Stops the pool: the jobs admitted still run, then every thread ends and is joined; a job submitted
/// once the stop began is refused. Blocks the calling thread on the jobs running and waiting, which
/// cannot be cancelled; call it from the consumer's shutdown path, not from a shard.
pub fn stop() -> Result<(), RtError> {
    let pool = POOL.get().ok_or(RtError::BadConfig {
        what: "stopping a blocking pool that never started",
    })?;
    if pool.stopping.swap(true, Ordering::AcqRel) {
        return Err(RtError::BadConfig {
            what: "the blocking pool stopped already",
        });
    }
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
