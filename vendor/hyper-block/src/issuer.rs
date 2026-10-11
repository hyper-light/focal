//! One issuer per physical device: every volume on the device hands it its writes and flushes,
//! and it keeps the device's depth in flight on a fixed set of workers (mantle
//! docs/design/node.md §1.2; chunk-store.md §4).
//!
//! The issuer is one thread and a pool of blocking workers, all started when the device opens
//! and none after, whatever the number of volumes, batches or regions: a thread per region of a
//! batch, which mantle's chunk writer started before, is a thread start on the write path and a
//! count of threads that grows with load (mantle research/26 §4.7, recommendation 7). The pool is
//! the portable path of research/26 §2.5, the only one macOS offers: no interface there keeps
//! more than 16 file I/Os in flight without a blocked thread per I/O, and none carries
//! `F_FULLFSYNC` (§2.4). Linux's io_uring and Windows's completion port, which node.md §1.2 takes
//! where they exist, are not yet built; the pool runs on every platform until they are.
//!
//! The pool holds `min(device queue, measured depth, thread budget)` workers ([`depth`]): the
//! device queues no more than its queue, throughput stops growing at the measured depth, so a
//! transfer past it only waits, and every pool draws from the process's budget before any thread
//! starts (research/26 §2.5, recommendation 4). Where the budget has fewer left the device runs
//! at the depth it leaves.
//!
//! Ownership is single and passed by message. A submitter's file is duplicated once for each
//! worker ([`BlockFile::try_clone`]), and each worker owns its duplicates in an arena of its own,
//! so no file is shared between threads and nothing is locked. The issuer thread owns the
//! dispatch state; each worker owns one slot its tasks arrive on, woken alone (research/26 §5.3).
//! A worker is told of a file attached or detached in its own slot, before any transfer for it,
//! since a transfer goes only to a worker with no such news waiting. A volume's writer holds an
//! [`Attached`], through which it submits a batch and waits for its answer ([`Attached::write`]),
//! or keeps up to the batches it attached for out at once and takes each answer when it needs it
//! ([`Attached::submit`], [`Attached::answer`]): a writer on a latency path hands a write over
//! and goes on. A writer that starts after the device opened, on a thread of its own, attaches
//! through an [`Attacher`], which carries only the way to the issuer's inbox
//! ([`Issuer::attacher`]). A batch's writes all complete before its flush is issued, and the flush
//! only if all succeeded:
//! a write that failed fails its batch, and its flush is never issued, because the caller then
//! fences and recovers rather than trust what reached the device (Rebello et al., ATC 2020). The
//! answer comes once, after the flush.
//!
//! A batch's transfers travel in the vector its submitter handed over, and that vector comes back
//! with the answer, each buffer and offset where it was given and its allocation intact. While the
//! batch is out the issuer keeps its slots there, a buffer on a worker leaving an empty one
//! ([`AlignedBuf::empty`], no allocation) in its place, so a batch costs the issuer no allocation
//! of its own, and a submitter that keeps its vectors in a pool submits and is answered with none.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread::{Builder, JoinHandle};

use hyper_rt::sync::{self, ChannelReceiver, Sender, SyncError};

use crate::DiskError;
pub use crate::aio::Transfers;
use crate::block::BlockFile;
use crate::buf::AlignedBuf;
use crate::threads::{self, Reservation};

/// The queue assumed of a device the OS cannot describe: Native Command Queuing's 32 commands,
/// the shallowest queue of a command-queuing interface (Serial ATA Revision 2.6, §13.6.2;
/// AHCI 1.3.1 §1.1), which calibration's ladder then measures (mantle CLAUDE.md §5).
pub const UNDESCRIBED_QUEUE_DEPTH: usize = 32;

/// The transfers a device's issuer keeps in flight: the smaller of the queue the OS reports
/// (`UNDESCRIBED_QUEUE_DEPTH` when it reports none) and the depth calibration measured
/// throughput to stop growing at. A device calibration has not measured gets one at a time,
/// as its reads do (mantle docs/design/chunk-store.md §7).
pub fn depth(queue: Option<u32>, measured: Option<usize>) -> usize {
    let queue = queue
        .and_then(|q| usize::try_from(q).ok())
        .filter(|&q| q > 0)
        .unwrap_or(UNDESCRIBED_QUEUE_DEPTH);
    queue.min(measured.filter(|&d| d > 0).unwrap_or(1))
}

/// A device's issuer: its thread, its workers and the budget they hold. Dropping it lets the
/// transfers in flight end, refuses what is queued, and joins every thread.
pub struct Issuer {
    events: SyncSender<Event>,
    thread: Option<JoinHandle<()>>,
    workers: usize,
    path: PathBuf,
    /// Given back once the threads are joined.
    budget: Option<Reservation>,
}

/// What a batch's submitter is answered: the batch's own vector of transfers given back, each buffer
/// and offset in the order given (a read's buffer filled), once every transfer and the flush asked
/// for have completed; or the batch's first failure, the vector and its buffers then dropped.
pub type Answer = Result<Transfers, DiskError>;

/// An answer with the number of the batch it answers ([`Attached::submit`]).
type Numbered = (u64, Answer);

/// A file as a worker holds it.
type Handle = Box<dyn BlockFile>;

enum Event {
    Attach {
        /// One duplicate of the file for each worker.
        files: Vec<Handle>,
        /// The batches the submitter may have out at once.
        batches: usize,
        answers: Sender<Numbered>,
        reply: SyncSender<Result<(usize, u64), DiskError>>,
    },
    Detach {
        slot: usize,
        generation: u64,
        done: SyncSender<()>,
    },
    Batch {
        slot: usize,
        generation: u64,
        number: u64,
        transfers: Transfers,
        kind: Kind,
    },
    Done {
        worker: usize,
        report: Report,
    },
    Stop,
}

/// What a batch's transfers do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Write each buffer at its offset, then flush if asked and every write succeeded.
    Write { flush: bool },
    /// Fill each buffer, all of its length, from its offset.
    Read,
}

enum Op {
    /// The `index`th write of its batch.
    Write {
        index: usize,
        buf: AlignedBuf,
        at: u64,
    },
    /// The `index`th read of its batch.
    Read {
        index: usize,
        buf: AlignedBuf,
        at: u64,
    },
    Flush,
}

/// What a worker is handed: news of a file, or a transfer.
enum Task {
    Attach {
        slot: usize,
        generation: u64,
        file: Handle,
    },
    Detach {
        slot: usize,
        generation: u64,
    },
    Transfer(Transfer),
}

struct Transfer {
    slot: usize,
    generation: u64,
    /// The batch it belongs to.
    number: u64,
    op: Op,
}

/// A worker's report of one task.
enum Report {
    /// A file attached, or one detached and dropped: which.
    News(Option<(usize, u64)>),
    Finished(Finished),
}

/// A transfer's end: the write's or read's index and buffer, `None` for a flush.
struct Finished {
    slot: usize,
    generation: u64,
    number: u64,
    write: Option<(usize, Option<AlignedBuf>)>,
    result: Result<(), DiskError>,
}

impl Issuer {
    /// Starts the issuer of the device at `path` with `depth` workers, or fewer when the
    /// process's thread budget has fewer left; [`DiskError::Threads`] when it has none for one
    /// worker and the issuer's own thread. Every thread starts here, before any is used. Its
    /// submitters may have `depth` batches out together without waiting to hand one over
    /// ([`Self::start_for`]).
    pub fn start(path: &Path, depth: usize) -> Result<Self, DiskError> {
        Self::start_for(path, depth, depth)
    }

    /// [`Self::start`] for submitters that together keep up to `batches` out
    /// ([`Self::attach_deep`]): the issuer's inbox holds every worker's report and every such
    /// batch at once, so within that budget a submission is handed over without waiting, even
    /// while the issuer's thread is not running. Past it, a submission waits for room.
    #[allow(
        clippy::disallowed_methods,
        reason = "the issuer's bounded device workers: hyper-block runs its device's I/O (clippy.toml's file rule names it)"
    )]
    pub fn start_for(path: &Path, depth: usize, batches: usize) -> Result<Self, DiskError> {
        if depth == 0 {
            return Err(invalid(path, "an issuer needs a depth of one at least"));
        }
        let left = threads::left()?;
        let workers = depth.min(left.saturating_sub(1));
        let threads = workers.saturating_add(1);
        if workers == 0 {
            return Err(DiskError::Threads {
                path: path.to_path_buf(),
                asked: depth.saturating_add(1),
                left,
                ceiling: threads::ceiling()?,
            });
        }
        let budget = threads::reserve(threads, path)?;
        // Room for every worker's report and every batch the submitters may have out, so a worker
        // never waits to report and a submission within the budget is never held behind them.
        let (events, inbox) = sync_channel(workers.saturating_add(batches));
        let (ready, started) = sync_channel(1);
        let completions = events.clone();
        let device = path.to_path_buf();
        let thread = Builder::new()
            .name("hyper-issuer".into())
            .spawn(move || run(&device, &inbox, &completions, workers, &ready))
            .map_err(|source| DiskError::Io {
                op: "start a device issuer",
                path: path.to_path_buf(),
                source,
            })?;
        let outcome = started
            .recv()
            .unwrap_or_else(|_| Err(stopped(path, "the issuer ended before it started")));
        if let Err(e) = outcome {
            // The issuer has stopped what it started; its thread is ending.
            let _ = thread.join();
            return Err(e);
        }
        Ok(Self {
            events,
            thread: Some(thread),
            workers,
            path: path.to_path_buf(),
            budget: Some(budget),
        })
    }

    /// The workers the issuer runs: the transfers it keeps in flight at most.
    pub fn depth(&self) -> usize {
        self.workers
    }

    /// Hands the issuer duplicates of `file`, one for each worker, for one submitter's batches,
    /// one out at a time.
    pub fn attach<F: BlockFile + 'static>(&self, file: &F) -> Result<Attached, DiskError> {
        self.attach_deep(file, 1)
    }

    /// [`Self::attach`] for a submitter that keeps up to `batches` out at once
    /// ([`Attached::submit`]); its bound, which the issuer holds it to, is the submitter's to
    /// state: the buffers it can spare while their writes are out.
    pub fn attach_deep<F: BlockFile + 'static>(
        &self,
        file: &F,
        batches: usize,
    ) -> Result<Attached, DiskError> {
        attach(&self.events, self.workers, &self.path, file, batches)
    }

    /// A way to attach submitters to this issuer that its holder owns and may send to another
    /// thread: a submitter started after the device opened, on a thread of its own, attaches
    /// through it ([`Attacher::attach_deep`]). It holds the way to the issuer's inbox and nothing
    /// else, so it keeps no thread and no device alive: once the issuer has stopped, an attach
    /// through it is refused as one through the issuer would be.
    pub fn attacher(&self) -> Attacher {
        Attacher {
            events: self.events.clone(),
            workers: self.workers,
            path: self.path.clone(),
        }
    }
}

/// An owned way to attach submitters to a device's issuer ([`Issuer::attacher`]): a maintenance
/// worker that starts after its shard attached, on its own thread, attaches through one rather
/// than borrow the issuer.
#[derive(Clone, Debug)]
pub struct Attacher {
    events: SyncSender<Event>,
    workers: usize,
    path: PathBuf,
}

impl Attacher {
    /// [`Issuer::attach_deep`], through the issuer this came from: refused, before any duplicate
    /// is handed over, once that issuer has stopped.
    pub fn attach_deep<F: BlockFile + 'static>(
        &self,
        file: &F,
        batches: usize,
    ) -> Result<Attached, DiskError> {
        attach(&self.events, self.workers, &self.path, file, batches)
    }
}

/// Hands the issuer whose inbox is `events` duplicates of `file`, one for each of its `workers`, for
/// a submitter that keeps up to `batches` out at once.
fn attach<F: BlockFile + 'static>(
    events: &SyncSender<Event>,
    workers: usize,
    path: &Path,
    file: &F,
    batches: usize,
) -> Result<Attached, DiskError> {
    if batches == 0 {
        return Err(invalid(path, "a submitter needs one batch at least"));
    }
    let mut files: Vec<Handle> = Vec::with_capacity(workers);
    for _ in 0..workers {
        files.push(Box::new(file.try_clone()?));
    }
    // Each batch out is answered once: room for every answer the submitter may be owed.
    let (answers, answered) = sync::channel(batches).map_err(|error| DiskError::Io {
        op: "attach completion channel",
        path: path.to_path_buf(),
        source: std::io::Error::other(error),
    })?;
    let (reply, replied) = sync_channel(1);
    let gone = || stopped(path, "the device's issuer has stopped");
    events
        .send(Event::Attach {
            files,
            batches,
            answers,
            reply,
        })
        .map_err(|_| gone())?;
    let (slot, generation) = replied.recv().map_err(|_| gone())??;
    Ok(Attached {
        slot,
        generation,
        events: events.clone(),
        answers: answered,
        path: path.to_path_buf(),
        batches,
        out: 0,
        next: 0,
    })
}

impl Drop for Issuer {
    fn drop(&mut self) {
        // The issuer drains its inbox while it runs, so the stop finds room.
        let _ = self.events.send(Event::Stop);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.budget.take();
    }
}

/// One submitter's way to its device's issuer: a volume's writer holds one. Dropping it
/// detaches the file and returns once no worker holds a duplicate of it.
#[derive(Debug)]
pub struct Attached {
    slot: usize,
    generation: u64,
    events: SyncSender<Event>,
    answers: ChannelReceiver<Numbered>,
    path: PathBuf,
    /// The batches it may have out at once, those out, and the next batch's number.
    batches: usize,
    out: usize,
    next: u64,
}

impl Attached {
    /// Issues every `(buffer, offset)` of `writes` at once, as deep as the device's workers go,
    /// and once all have completed, a flush when `flush` is set and every write succeeded.
    /// Returns `writes` back, every buffer and offset in order, after the last of them; the first
    /// failure otherwise, once none is in flight. Refused while batches submitted are out.
    pub fn write(&mut self, writes: Transfers, flush: bool) -> Answer {
        if self.out > 0 {
            return Err(invalid(
                &self.path,
                "a write while submitted batches are out",
            ));
        }
        if writes.is_empty() && !flush {
            return Ok(writes);
        }
        let number = self.submit(writes, flush)?;
        let (answered, answer) = self.answer()?;
        if answered != number {
            return Err(stopped(&self.path, "an answer to another batch"));
        }
        answer
    }

    /// Makes every completed write durable: the platform's full flush, on a worker.
    pub fn flush(&mut self) -> Result<(), DiskError> {
        self.write(Vec::new(), true).map(|_| ())
    }

    /// Hands a batch, as [`Self::write`] issues one, to the issuer and returns its number
    /// without waiting: its answer is taken by [`Self::answer`] or [`Self::try_answer`]. Refused
    /// with the batches attached for already out, before anything is sent. Batches complete in
    /// any order, and a batch's flush covers only the writes completed when it is issued.
    pub fn submit(&mut self, writes: Transfers, flush: bool) -> Result<u64, DiskError> {
        if self.out >= self.batches {
            return Err(invalid(
                &self.path,
                "a batch past those the submitter attached for; take an answer first",
            ));
        }
        self.send(writes, Kind::Write { flush })
    }

    /// Hands a batch of reads to the issuer and returns its number without waiting, as
    /// [`Self::submit`] does a batch of writes: each `(buffer, offset)` is filled, the whole of the
    /// buffer's length, from its offset, as deep as the device's workers go. The answer
    /// ([`Self::answer`], [`Self::try_answer`]) gives `reads` back, its buffers filled, in the order given,
    /// once every read has completed; or the first failure once none is in flight, a read that
    /// reaches the end of the file first among them (`BlockFile::read_exact_at`). Reads share the
    /// device's depth with writes and count among the batches the submitter attached for: refused
    /// with those already out, before anything is sent. A read's bytes are those of every write
    /// answered before it was submitted; a read of a range a write still out covers sees either.
    pub fn submit_reads(&mut self, reads: Transfers) -> Result<u64, DiskError> {
        if self.out >= self.batches {
            return Err(invalid(
                &self.path,
                "a batch past those the submitter attached for; take an answer first",
            ));
        }
        self.send(reads, Kind::Read)
    }

    /// Sends a batch the submitter has room for, and counts it out.
    fn send(&mut self, transfers: Transfers, kind: Kind) -> Result<u64, DiskError> {
        let number = self.next;
        let gone = || stopped(&self.path, "the device's issuer has stopped");
        self.events
            .send(Event::Batch {
                slot: self.slot,
                generation: self.generation,
                number,
                transfers,
                kind,
            })
            .map_err(|_| gone())?;
        self.next = self.next.wrapping_add(1);
        self.out = self.out.saturating_add(1);
        Ok(number)
    }

    /// The next answer, waiting for it: the batch's number and its answer. Refused with no batch
    /// out.
    pub fn answer(&mut self) -> Result<Numbered, DiskError> {
        if self.out == 0 {
            return Err(invalid(&self.path, "an answer with no batch out"));
        }
        let answered = self
            .answers
            .blocking_recv()
            .map_err(|_| stopped(&self.path, "the device's issuer has stopped"))?;
        self.out = self.out.saturating_sub(1);
        Ok(answered)
    }

    /// The next answer, waiting as a hyper-rt task. Dropping this borrowed wait leaves the
    /// batch out and its buffers owned by the issuer or answer queue; a later wait or sync
    /// receive takes the same answer. Dropping the attachment still waits for its detach.
    pub async fn answer_async(&mut self) -> Result<Numbered, DiskError> {
        if self.out == 0 {
            return Err(invalid(&self.path, "an answer with no batch out"));
        }
        let answered = self
            .answers
            .recv()
            .await
            .map_err(|error| completion_error(&self.path, error))?;
        self.out = self.out.saturating_sub(1);
        Ok(answered)
    }

    /// The next answer if one has come; none otherwise, or with no batch out.
    pub fn try_answer(&mut self) -> Result<Option<Numbered>, DiskError> {
        if self.out == 0 {
            return Ok(None);
        }
        match self.answers.try_recv() {
            Ok(Some(answered)) => {
                self.out = self.out.saturating_sub(1);
                Ok(Some(answered))
            }
            Ok(None) => Ok(None),
            Err(error) => Err(completion_error(&self.path, error)),
        }
    }

    /// Batches submitted and not yet answered.
    pub fn out(&self) -> usize {
        self.out
    }

    /// The batches it may have out at once.
    pub fn batches(&self) -> usize {
        self.batches
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        let (done, detached) = sync_channel(1);
        let detach = Event::Detach {
            slot: self.slot,
            generation: self.generation,
            done,
        };
        if self.events.send(detach).is_ok() {
            // Answered once every worker has dropped its duplicate, or dropped if the issuer
            // stops first.
            let _ = detached.recv();
        }
    }
}

/// The issuer's thread: starts the workers in its own scope, reports whether all started, then
/// dispatches until stopped.
fn run(
    path: &Path,
    inbox: &Receiver<Event>,
    completions: &SyncSender<Event>,
    workers: usize,
    ready: &SyncSender<Result<(), DiskError>>,
) {
    std::thread::scope(|scope| {
        let mut slots = Vec::with_capacity(workers);
        let mut handles = Vec::with_capacity(workers);
        let mut failed = None;
        for id in 0..workers {
            let (tasks, assigned) = sync_channel::<Task>(1);
            let done = completions.clone();
            match Builder::new()
                .name("hyper-io".into())
                .spawn_scoped(scope, move || work(id, &assigned, &done))
            {
                Ok(handle) => {
                    slots.push(tasks);
                    handles.push(handle);
                }
                Err(source) => {
                    failed = Some(DiskError::Io {
                        op: "start a device worker",
                        path: path.to_path_buf(),
                        source,
                    });
                    break;
                }
            }
        }
        let started = match failed {
            Some(e) => Err(e),
            None => Ok(()),
        };
        if ready.send(started).is_ok() && slots.len() == workers {
            Dispatch::new(slots, path).run(inbox);
        } else {
            drop(slots);
        }
        // Each worker ends once its slot's sender is gone; a worker that unwound is already
        // ended. Joined here, so the scope has nothing left to report.
        for handle in handles {
            let _ = handle.join();
        }
    });
}

/// A worker: takes each task from its own slot, carries it out on the duplicates it owns, and
/// reports. An unwind from a file, which production code never raises, is caught here and
/// reported as an error, so a batch's submitter never waits on a report that cannot come.
fn work(id: usize, tasks: &Receiver<Task>, done: &SyncSender<Event>) {
    let mut files: Vec<Option<(u64, Handle)>> = Vec::new();
    while let Ok(task) = tasks.recv() {
        let report = match task {
            Task::Attach {
                slot,
                generation,
                file,
            } => {
                place(&mut files, slot, Some((generation, file)));
                Report::News(None)
            }
            Task::Detach { slot, generation } => {
                if files
                    .get(slot)
                    .is_some_and(|f| f.as_ref().is_some_and(|(g, _)| *g == generation))
                {
                    place(&mut files, slot, None);
                }
                Report::News(Some((slot, generation)))
            }
            Task::Transfer(transfer) => Report::Finished(carry_out(&files, transfer)),
        };
        if done.send(Event::Done { worker: id, report }).is_err() {
            return;
        }
    }
}

/// Puts `file` in slot `slot` of a worker's arena, growing it to hold the slot.
fn place(files: &mut Vec<Option<(u64, Handle)>>, slot: usize, file: Option<(u64, Handle)>) {
    if files.len() <= slot {
        files.resize_with(slot.saturating_add(1), || None);
    }
    if let Some(at) = files.get_mut(slot) {
        *at = file;
    }
}

/// Issues one transfer on the worker's duplicate of its file.
fn carry_out(files: &[Option<(u64, Handle)>], transfer: Transfer) -> Finished {
    let Transfer {
        slot,
        generation,
        number,
        op,
    } = transfer;
    let file = match files.get(slot) {
        Some(Some((g, file))) if *g == generation => Some(file),
        _ => None,
    };
    let missing = || {
        stopped(
            Path::new(""),
            "a transfer for a file the worker does not hold",
        )
    };
    let (write, result) = match op {
        Op::Write { index, buf, at } => {
            let issued = std::panic::catch_unwind(AssertUnwindSafe(move || {
                let result =
                    file.map_or_else(|| Err(missing()), |f| f.write_all_at(buf.as_slice(), at));
                (buf, result)
            }));
            match issued {
                Ok((buf, result)) => (Some((index, Some(buf))), result),
                Err(_) => (Some((index, None)), Err(unwound())),
            }
        }
        Op::Read { index, mut buf, at } => {
            let issued = std::panic::catch_unwind(AssertUnwindSafe(move || {
                let result = file.map_or_else(
                    || Err(missing()),
                    |f| f.read_exact_at(buf.as_mut_slice(), at),
                );
                (buf, result)
            }));
            match issued {
                Ok((buf, result)) => (Some((index, Some(buf))), result),
                Err(_) => (Some((index, None)), Err(unwound())),
            }
        }
        Op::Flush => {
            let issued = std::panic::catch_unwind(AssertUnwindSafe(|| {
                file.map_or_else(|| Err(missing()), |f| f.sync_data())
            }));
            (None, issued.unwrap_or_else(|_| Err(unwound())))
        }
    };
    Finished {
        slot,
        generation,
        number,
        write,
        result,
    }
}

/// A submitter as the issuer keeps it: where its answers go, and its batches out, at most the
/// number it attached for.
struct Client {
    generation: u64,
    answers: Sender<Numbered>,
    limit: usize,
    batches: VecDeque<Batch>,
}

struct Batch {
    number: u64,
    /// Transfers issued or queued and not yet reported.
    outstanding: usize,
    /// The submitter's vector: each transfer's offset, and its buffer once reported, an empty one
    /// standing in while it is out.
    transfers: Transfers,
    failed: Option<DiskError>,
    /// A flush is still to be issued once the writes have completed.
    flush: bool,
}

/// A file being detached: the workers yet to drop their duplicates, and who waits for them.
struct Detaching {
    slot: usize,
    generation: u64,
    remaining: usize,
    done: SyncSender<()>,
}

/// The issuer's state, owned by its thread.
struct Dispatch<'a> {
    /// Each worker's slot.
    workers: Vec<SyncSender<Task>>,
    /// Workers with no task.
    idle: Vec<usize>,
    /// News of files waiting for each worker, given before any transfer: at most one attach and
    /// one detach for each file attached, so bounded by the submitters.
    news: Vec<VecDeque<Task>>,
    /// Tasks handed to workers and not yet reported.
    in_flight: usize,
    /// Transfers waiting for a worker, in the order they arrived. Each attached submitter has
    /// at most the batches it attached for out, so these are at most those batches' transfers,
    /// whose buffers their submitters already gave.
    pending: VecDeque<Transfer>,
    /// Indexed by slot. A slot is reused once every worker has dropped the file it held.
    clients: Vec<Option<Client>>,
    detaching: Vec<Detaching>,
    generation: u64,
    stopping: bool,
    path: &'a Path,
}

impl<'a> Dispatch<'a> {
    fn new(workers: Vec<SyncSender<Task>>, path: &'a Path) -> Self {
        let count = workers.len();
        Self {
            idle: (0..count).rev().collect(),
            news: (0..count).map(|_| VecDeque::new()).collect(),
            workers,
            in_flight: 0,
            pending: VecDeque::new(),
            clients: Vec::new(),
            detaching: Vec::new(),
            generation: 0,
            stopping: false,
            path,
        }
    }

    fn run(mut self, inbox: &Receiver<Event>) {
        while let Ok(event) = inbox.recv() {
            self.event(event);
            self.dispatch();
            if self.stopping && self.in_flight == 0 {
                return;
            }
        }
    }

    fn event(&mut self, event: Event) {
        match event {
            Event::Attach {
                files,
                batches,
                answers,
                reply,
            } => {
                let attached = self.attach(files, batches, answers);
                let _ = reply.try_send(attached);
            }
            Event::Detach {
                slot,
                generation,
                done,
            } => self.detach(slot, generation, done),
            Event::Batch {
                slot,
                generation,
                number,
                transfers,
                kind,
            } => self.batch(slot, generation, number, transfers, kind),
            Event::Done { worker, report } => {
                self.idle.push(worker);
                self.in_flight = self.in_flight.saturating_sub(1);
                match report {
                    Report::News(dropped) => self.dropped(dropped),
                    Report::Finished(finished) => self.finished(finished),
                }
            }
            Event::Stop => {
                self.stopping = true;
                for transfer in std::mem::take(&mut self.pending) {
                    self.refused(transfer);
                }
            }
        }
    }

    fn attach(
        &mut self,
        files: Vec<Handle>,
        limit: usize,
        answers: Sender<Numbered>,
    ) -> Result<(usize, u64), DiskError> {
        if self.stopping {
            return Err(stopped(self.path, "the device's issuer is stopping"));
        }
        if files.len() != self.workers.len() {
            return Err(invalid(
                self.path,
                "one duplicate of the file for each worker",
            ));
        }
        self.generation = self.generation.saturating_add(1);
        let generation = self.generation;
        // A slot is free once its client detached and every worker dropped its file.
        let busy = |slot: usize, clients: &[Option<Client>], detaching: &[Detaching]| {
            clients.get(slot).is_some_and(Option::is_some)
                || detaching.iter().any(|d| d.slot == slot)
        };
        let slot = (0..self.clients.len())
            .find(|&s| !busy(s, &self.clients, &self.detaching))
            .unwrap_or(self.clients.len());
        for (news, file) in self.news.iter_mut().zip(files) {
            news.push_back(Task::Attach {
                slot,
                generation,
                file,
            });
        }
        let client = Some(Client {
            generation,
            answers,
            limit,
            batches: VecDeque::with_capacity(limit),
        });
        match self.clients.get_mut(slot) {
            Some(free) => *free = client,
            None => self.clients.push(client),
        }
        Ok((slot, generation))
    }

    fn detach(&mut self, slot: usize, generation: u64, done: SyncSender<()>) {
        if self.client(slot, generation).is_none() {
            let _ = done.try_send(());
            return;
        }
        // The submitter waits for nothing once it detaches; its batches, if any, have ended.
        if let Some(client) = self.clients.get_mut(slot) {
            *client = None;
        }
        for news in &mut self.news {
            news.push_back(Task::Detach { slot, generation });
        }
        self.detaching.push(Detaching {
            slot,
            generation,
            remaining: self.workers.len(),
            done,
        });
    }

    /// A worker dropped its duplicate of a detached file; the last one answers the detach.
    fn dropped(&mut self, dropped: Option<(usize, u64)>) {
        let Some((slot, generation)) = dropped else {
            return;
        };
        let Some(at) = self
            .detaching
            .iter()
            .position(|d| d.slot == slot && d.generation == generation)
        else {
            return;
        };
        let finished = self.detaching.get_mut(at).is_some_and(|d| {
            d.remaining = d.remaining.saturating_sub(1);
            d.remaining == 0
        });
        if finished {
            let d = self.detaching.swap_remove(at);
            let _ = d.done.try_send(());
        }
    }

    fn batch(
        &mut self,
        slot: usize,
        generation: u64,
        number: u64,
        mut transfers: Transfers,
        kind: Kind,
    ) {
        let flush = kind == Kind::Write { flush: true };
        let refused = self.stopping;
        let path = self.path;
        let Some(client) = self.client(slot, generation) else {
            // Not attached: the submitter's answers went with it.
            return;
        };
        if refused || client.batches.len() >= client.limit {
            let why = if refused {
                "the device's issuer is stopping"
            } else {
                "a batch past those its submitter attached for"
            };
            let _ = client.answers.try_send((number, Err(stopped(path, why))));
            return;
        }
        let count = transfers.len();
        if count == 0 && !flush {
            let _ = client.answers.try_send((number, Ok(transfers)));
            return;
        }
        if count == 0 {
            self.pending.push_back(Transfer {
                slot,
                generation,
                number,
                op: Op::Flush,
            });
        }
        // Each buffer goes to its transfer; its slot keeps the offset and an empty buffer until
        // the transfer reports.
        for (index, (kept, at)) in transfers.iter_mut().enumerate() {
            let buf = std::mem::replace(kept, AlignedBuf::empty());
            let at = *at;
            let op = match kind {
                Kind::Write { .. } => Op::Write { index, buf, at },
                Kind::Read => Op::Read { index, buf, at },
            };
            self.pending.push_back(Transfer {
                slot,
                generation,
                number,
                op,
            });
        }
        let outstanding = if count == 0 { 1 } else { count };
        if let Some(client) = self.client(slot, generation) {
            client.batches.push_back(Batch {
                number,
                outstanding,
                transfers,
                failed: None,
                flush: flush && count > 0,
            });
        }
    }

    fn client(&mut self, slot: usize, generation: u64) -> Option<&mut Client> {
        self.clients
            .get_mut(slot)
            .and_then(Option::as_mut)
            .filter(|c| c.generation == generation)
    }

    /// Records a transfer's report, and answers its batch or issues its flush once every write
    /// has completed.
    fn finished(&mut self, finished: Finished) {
        let Finished {
            slot,
            generation,
            number,
            write,
            result,
        } = finished;
        let Some(client) = self.client(slot, generation) else {
            return;
        };
        let Some(at) = client.batches.iter().position(|b| b.number == number) else {
            return;
        };
        let Some(batch) = client.batches.get_mut(at) else {
            return;
        };
        // A buffer lost to a worker's unwind leaves its slot empty; its batch has failed.
        if let Some((index, Some(buf))) = write
            && let Some((kept, _)) = batch.transfers.get_mut(index)
        {
            *kept = buf;
        }
        if let Err(e) = result {
            batch.failed.get_or_insert(e);
        }
        batch.outstanding = batch.outstanding.saturating_sub(1);
        if batch.outstanding > 0 {
            return;
        }
        if batch.flush && batch.failed.is_none() {
            batch.flush = false;
            batch.outstanding = 1;
            self.pending.push_back(Transfer {
                slot,
                generation,
                number,
                op: Op::Flush,
            });
            return;
        }
        if let Some(batch) = client.batches.remove(at) {
            let answer = match batch.failed {
                Some(e) => Err(e),
                None => Ok(batch.transfers),
            };
            let _ = client.answers.try_send((number, answer));
        }
    }

    /// A transfer never issued: its batch fails.
    fn refused(&mut self, transfer: Transfer) {
        self.failed(transfer, "the device's issuer is stopping");
    }

    /// Fails `transfer`'s batch with `why`, giving its buffer back.
    fn failed(&mut self, transfer: Transfer, why: &str) {
        let write = match transfer.op {
            Op::Write { index, buf, .. } | Op::Read { index, buf, .. } => Some((index, Some(buf))),
            Op::Flush => None,
        };
        let path = self.path;
        self.finished(Finished {
            slot: transfer.slot,
            generation: transfer.generation,
            number: transfer.number,
            write,
            result: Err(stopped(path, why)),
        });
    }

    /// Hands each idle worker its news first, then queued transfers to the idle workers with
    /// no news waiting, in the order they arrived.
    fn dispatch(&mut self) {
        let mut idle = std::mem::take(&mut self.idle);
        idle.retain(|&worker| !self.tell(worker));
        while let Some(&worker) = idle.last() {
            let Some(transfer) = self.pending.pop_front() else {
                break;
            };
            idle.pop();
            let sent = match self.workers.get(worker) {
                Some(slot) => slot.try_send(Task::Transfer(transfer)),
                None => Err(TrySendError::Disconnected(Task::Transfer(transfer))),
            };
            // An idle worker's slot is empty; one that is not, or whose worker ended, keeps no
            // place among the idle, and its transfer fails its batch.
            match sent {
                Ok(()) => self.in_flight = self.in_flight.saturating_add(1),
                Err(TrySendError::Full(task) | TrySendError::Disconnected(task)) => {
                    if let Task::Transfer(transfer) = task {
                        self.failed(transfer, "a device worker has ended");
                    }
                }
            }
        }
        self.idle = idle;
    }

    /// Gives an idle worker the next news it waits for: whether it was handed some.
    fn tell(&mut self, worker: usize) -> bool {
        let Some(task) = self.news.get_mut(worker).and_then(VecDeque::pop_front) else {
            return false;
        };
        let sent = self
            .workers
            .get(worker)
            .is_some_and(|slot| slot.try_send(task).is_ok());
        if sent {
            self.in_flight = self.in_flight.saturating_add(1);
        }
        // A worker that ended takes no news and no transfer: it leaves the idle.
        true
    }
}

fn stopped(path: &Path, why: &str) -> DiskError {
    DiskError::Io {
        op: "issue",
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, why.to_owned()),
    }
}

fn completion_error(path: &Path, error: SyncError<()>) -> DiskError {
    match error {
        SyncError::Closed(()) => stopped(path, "the device's issuer has stopped"),
        SyncError::NotOnShardThread(()) => {
            invalid(path, "an async answer must be polled by a hyper-rt task")
        }
        SyncError::Full(()) => invalid(path, "the completion channel refused a receive"),
    }
}

fn unwound() -> DiskError {
    stopped(Path::new(""), "a device worker unwound")
}

fn invalid(path: &Path, why: &str) -> DiskError {
    DiskError::Io {
        op: "issue",
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, why.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use hyper_rt::combine::{Either, race2};
    use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

    use super::*;
    use crate::buf::Alignment;
    use crate::file::{CachingRequest, DeviceFile};

    /// What every duplicate of one probe counts, in one place each test leaks for its run.
    #[derive(Default)]
    struct Counts {
        in_flight: AtomicUsize,
        most: AtomicUsize,
        entered: AtomicUsize,
        open: AtomicBool,
        /// The offset whose write fails, `u64::MAX` for none.
        fail_at: AtomicU64,
        flushes: AtomicUsize,
        /// Writes completed when the last flush was issued.
        completed_at_flush: AtomicUsize,
        completed: AtomicUsize,
        /// Duplicates alive.
        alive: AtomicUsize,
    }

    /// A file on disk that counts the transfers in flight, holds every write until the test
    /// opens it, and fails the write at one offset when asked; each duplicate is a handle of
    /// the same file and counts in the same place.
    struct Probe {
        file: DeviceFile,
        counts: &'static Counts,
    }

    impl Probe {
        fn new(dir: &Path, open: bool, fail_at: Option<u64>) -> Self {
            let counts: &'static Counts = Box::leak(Box::default());
            counts.open.store(open, Ordering::SeqCst);
            counts
                .fail_at
                .store(fail_at.unwrap_or(u64::MAX), Ordering::SeqCst);
            counts.alive.store(1, Ordering::SeqCst);
            let file = DeviceFile::open(
                &dir.join("probe"),
                true,
                CachingRequest::Buffered,
                Alignment::new(4096).unwrap(),
            )
            .unwrap();
            Self { file, counts }
        }

        /// Waits until `n` writes have entered: the fact the test needs.
        fn entered(&self, n: usize) {
            while self.counts.entered.load(Ordering::SeqCst) < n {
                std::thread::yield_now();
            }
        }

        fn open(&self) {
            self.counts.open.store(true, Ordering::SeqCst);
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            self.counts.alive.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl BlockFile for Probe {
        fn alignment(&self) -> Alignment {
            Alignment::new(4096).unwrap()
        }

        fn len(&self) -> Result<u64, DiskError> {
            self.file.len()
        }

        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
            self.file.read_exact_at(buf, offset)
        }

        fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
            let c = self.counts;
            let now = c.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            c.most.fetch_max(now, Ordering::SeqCst);
            c.entered.fetch_add(1, Ordering::SeqCst);
            while !c.open.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            let fail = c.fail_at.load(Ordering::SeqCst) == offset;
            if !fail {
                self.file.write_all_at(buf, offset)?;
            }
            c.in_flight.fetch_sub(1, Ordering::SeqCst);
            c.completed.fetch_add(1, Ordering::SeqCst);
            if fail {
                return Err(stopped(Path::new("probe"), "a write the test fails"));
            }
            Ok(())
        }

        fn sync_data(&self) -> Result<(), DiskError> {
            let c = self.counts;
            c.completed_at_flush
                .store(c.completed.load(Ordering::SeqCst), Ordering::SeqCst);
            c.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn try_clone(&self) -> Result<Self, DiskError> {
            self.counts.alive.fetch_add(1, Ordering::SeqCst);
            Ok(Self {
                file: self.file.try_clone()?,
                counts: self.counts,
            })
        }
    }

    /// The byte the `i`th write of a batch is filled with.
    fn fill(i: usize) -> u8 {
        u8::try_from(i + 1).unwrap()
    }

    fn writes(n: usize) -> Vec<(AlignedBuf, u64)> {
        (0..n)
            .map(|i| {
                let mut buf = AlignedBuf::zeroed(4096, Alignment::new(4096).unwrap()).unwrap();
                buf.extend_from_slice(&[fill(i); 4096]).unwrap();
                (buf, (i * 4096) as u64)
            })
            .collect()
    }

    fn runtime() -> LocalRuntime {
        // Test shape: one shard, room for the waiter and an independent task that opens I/O.
        LocalRuntime::new(&RuntimeConfig {
            shards: 1,
            tasks_per_shard: 64,
            timers_per_shard: 64,
            interests_per_shard: 64,
            ring_entries: 64,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            batch: 64,
            pin: false,
            cores: Vec::new(),
            page_bytes: 4096,
            spin_ns: 0,
            wake_tracking: None,
        })
        .unwrap()
    }

    struct OpenOnDrop(&'static Counts);

    impl Drop for OpenOnDrop {
        fn drop(&mut self) {
            self.0.open.store(true, Ordering::SeqCst);
        }
    }

    /// Cancelling a borrowed wait spends neither a batch credit nor its buffers. A repeated
    /// poll and a spurious wake still wait, and another task on the shard can release the I/O.
    #[test]
    fn a_cancelled_async_answer_can_be_waited_again() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), false, None);
        let mut attached = issuer.attach(&probe).unwrap();
        let number = attached.submit(writes(1), true).unwrap();
        probe.entered(1);
        let counts = probe.counts;
        let mut rt = runtime();
        let attached = rt
            .block_on(async move {
                let mut attached = attached;
                let _open = OpenOnDrop(counts);
                {
                    let mut answer = std::pin::pin!(attached.answer_async());
                    let waker = poll_fn(|cx| {
                        assert!(answer.as_mut().poll(cx).is_pending());
                        Poll::Ready(cx.waker().clone())
                    })
                    .await;
                    waker.wake_by_ref();
                    let cancelled = race2(answer.as_mut(), async {}).await;
                    assert!(matches!(cancelled, Either::Second(())));
                }
                assert_eq!(attached.out(), 1);
                assert!(attached.try_answer().unwrap().is_none());
                assert!(attached.submit(writes(1), false).is_err());
                hyper_rt::futures::spawn_detached(async move {
                    counts.open.store(true, Ordering::SeqCst);
                })
                .unwrap();
                let (answered, buffers) = attached.answer_async().await.unwrap();
                assert_eq!(answered, number);
                assert_eq!(buffers.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
                assert_eq!(attached.out(), 0);
                assert!(attached.try_answer().unwrap().is_none());
                assert!(attached.answer_async().await.is_err());
                attached
            })
            .unwrap();
        assert_eq!(attached.out(), 0);
        assert_eq!(probe.counts.flushes.load(Ordering::SeqCst), 1);
    }

    /// A cancelled wait's task can end while the attachment remains owned. The next task
    /// registers its own wake and receives the completion rather than the old task's wake.
    #[test]
    fn an_async_answer_can_move_to_another_task_after_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), false, None);
        let mut attached = issuer.attach(&probe).unwrap();
        let number = attached.submit(writes(1), false).unwrap();
        probe.entered(1);
        let counts = probe.counts;
        let mut rt = runtime();
        let (attached, open) = rt
            .block_on(async move {
                let mut attached = attached;
                let open = OpenOnDrop(counts);
                assert!(matches!(
                    race2(attached.answer_async(), async {}).await,
                    Either::Second(())
                ));
                assert_eq!(attached.out(), 1);
                (attached, open)
            })
            .unwrap();
        let mut next_rt = runtime();
        let attached = next_rt
            .block_on(async move {
                let mut attached = attached;
                let _open = open;
                hyper_rt::futures::spawn_detached(async move {
                    counts.open.store(true, Ordering::SeqCst);
                })
                .unwrap();
                let (answered, buffers) = attached.answer_async().await.unwrap();
                assert_eq!(answered, number);
                assert_eq!(buffers.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
                attached
            })
            .unwrap();
        assert_eq!(attached.out(), 0);
    }

    /// A foreign executor cannot register a pending wait; refusal retains the batch so the
    /// existing synchronous answer still receives exactly its original buffers.
    #[test]
    fn an_async_answer_off_the_runtime_refuses_without_consuming() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), false, None);
        let mut attached = issuer.attach(&probe).unwrap();
        let _open = OpenOnDrop(probe.counts);
        let number = attached.submit(writes(1), false).unwrap();
        probe.entered(1);
        {
            let mut answer = std::pin::pin!(attached.answer_async());
            let mut cx = Context::from_waker(Waker::noop());
            assert!(matches!(
                answer.as_mut().poll(&mut cx),
                Poll::Ready(Err(DiskError::Io { source, .. }))
                    if source.kind() == std::io::ErrorKind::InvalidInput
            ));
        }
        assert_eq!(attached.out(), 1);
        probe.open();
        let (answered, buffers) = attached.answer().unwrap();
        assert_eq!(answered, number);
        assert_eq!(buffers.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
        assert_eq!(attached.out(), 0);
    }

    /// Reads can finish before earlier writes; the async path retains their numbers and the
    /// read bytes, then the sync path takes the held write and its flush in the same attachment.
    #[test]
    fn async_and_sync_answers_keep_out_of_order_batch_identity() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), false, None);
        probe.file.write_all_at(&[7; 4096], 4096).unwrap();
        let mut attached = issuer.attach_deep(&probe, 2).unwrap();
        let held = attached.submit(writes(1), true).unwrap();
        probe.entered(1);
        let read = attached.submit_reads(reads(1, 1)).unwrap();
        let counts = probe.counts;
        let mut rt = runtime();
        let mut attached = rt
            .block_on(async move {
                let mut attached = attached;
                let _open = OpenOnDrop(counts);
                let (answered, buffers) = attached.answer_async().await.unwrap();
                assert_eq!(answered, read);
                assert_eq!(buffers.unwrap()[0].0.as_slice(), &[7; 4096]);
                assert_eq!(attached.out(), 1);
                attached
            })
            .unwrap();
        let (answered, buffers) = attached.answer().unwrap();
        assert_eq!(answered, held);
        assert_eq!(buffers.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
        assert_eq!(attached.out(), 0);
        assert_eq!(probe.counts.flushes.load(Ordering::SeqCst), 1);
    }

    /// A failed batch is one answer, not a broken wake or spent queue slot; a later flush on
    /// the attachment succeeds. Stopping the issuer then refuses submission without a credit.
    #[test]
    fn async_failure_and_stopped_issuer_preserve_batch_accounting() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, Some(0));
        let mut attached = issuer.attach(&probe).unwrap();
        let number = attached.submit(writes(1), true).unwrap();
        let mut rt = runtime();
        let mut attached = rt
            .block_on(async move {
                let mut attached = attached;
                let (answered, result) = attached.answer_async().await.unwrap();
                assert_eq!(answered, number);
                assert!(matches!(result, Err(DiskError::Io { .. })));
                assert_eq!(attached.out(), 0);
                attached
            })
            .unwrap();
        assert_eq!(probe.counts.flushes.load(Ordering::SeqCst), 0);
        attached.flush().unwrap();
        drop(issuer);
        assert!(
            matches!(attached.submit(Vec::new(), true), Err(DiskError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::BrokenPipe)
        );
        assert_eq!(attached.out(), 0);
        assert!(attached.try_answer().unwrap().is_none());
    }

    /// Issuer shutdown drains its accepted I/O before the completion sender closes. Those
    /// already-queued answers remain available to an async wait, one credit per answer.
    #[test]
    fn async_answers_already_queued_survive_issuer_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let mut attached = issuer.attach_deep(&probe, 2).unwrap();
        let write = attached.submit(writes(1), true).unwrap();
        let read = attached.submit_reads(reads(1, 1)).unwrap();
        drop(issuer);
        assert_eq!(attached.out(), 2);
        let mut rt = runtime();
        let attached = rt
            .block_on(async move {
                let mut attached = attached;
                let mut answers = [
                    attached.answer_async().await.unwrap(),
                    attached.answer_async().await.unwrap(),
                ];
                answers.sort_by_key(|(number, _)| *number);
                let [(written, buffers), (read_back, failed)] = answers;
                assert_eq!(written, write);
                // The write may finish or be refused during shutdown; either is its numbered
                // answer, and only a completed write returns the original buffers.
                if let Ok(buffers) = buffers {
                    assert_eq!(buffers[0].0.as_slice(), &[fill(0); 4096]);
                }
                assert_eq!(read_back, read);
                assert!(failed.is_err());
                assert_eq!(attached.out(), 0);
                attached
            })
            .unwrap();
        assert_eq!(attached.out(), 0);
    }

    /// Channel admission is bounded by the runtime's public cell limit. Isolate that global
    /// limit in a child process; refusal leaves the issuer usable once capacity is restored.
    #[test]
    // The test reruns itself as a child process, the environment marking the child: the global
    // cell limit it sets must not reach the other tests of this binary.
    #[allow(clippy::disallowed_methods)]
    fn completion_channel_capacity_refusal_leaves_the_issuer_usable() {
        const CHILD: &str = "MANTLE_COMPLETION_CELL_REFUSAL";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "issuer::tests::completion_channel_capacity_refusal_leaves_the_issuer_usable",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        hyper_rt::sync::cell::set_limit(0);
        assert!(
            matches!(issuer.attach(&probe), Err(DiskError::Io { source, .. })
            if matches!(source.get_ref().and_then(|cause| cause.downcast_ref::<hyper_rt::RtError>()),
                Some(hyper_rt::RtError::Capacity { bound: 0, .. })))
        );
        hyper_rt::sync::cell::set_limit(hyper_rt::sync::cell::MAX_CELLS);
        let mut attached = issuer.attach(&probe).unwrap();
        let buffers = attached.write(writes(1), true).unwrap();
        assert_eq!(buffers[0].0.as_slice(), &[fill(0); 4096]);
    }

    #[test]
    fn the_depth_is_the_smallest_of_queue_and_measurement() {
        assert_eq!(depth(Some(253), Some(64)), 64);
        assert_eq!(depth(Some(16), Some(64)), 16);
        // Unmeasured: one at a time, as unmeasured reads are.
        assert_eq!(depth(Some(253), None), 1);
        // Undescribed: NCQ's 32 bounds the measurement.
        assert_eq!(depth(None, Some(200)), UNDESCRIBED_QUEUE_DEPTH);
        assert_eq!(depth(Some(0), Some(0)), 1);
        assert!(Issuer::start(Path::new("dev"), 0).is_err());
    }

    /// A batch of three times the depth keeps exactly the depth in flight, its buffers come
    /// back in order, and its one flush is issued after every write has completed.
    #[test]
    fn a_batch_keeps_the_depth_and_flushes_after_its_writes() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 4).unwrap();
        assert_eq!(issuer.depth(), 4);
        let probe = Probe::new(dir.path(), false, None);
        let mut attached = issuer.attach(&probe).unwrap();
        std::thread::scope(|s| {
            let batch = s.spawn(|| attached.write(writes(12), true));
            probe.entered(4);
            assert_eq!(probe.counts.in_flight.load(Ordering::SeqCst), 4);
            probe.open();
            let back = batch.join().unwrap().unwrap();
            assert_eq!(back.len(), 12);
            for (i, (buf, _)) in back.iter().enumerate() {
                assert_eq!(buf.as_slice()[0], fill(i));
            }
        });
        let c = probe.counts;
        assert_eq!(c.most.load(Ordering::SeqCst), 4);
        assert_eq!(c.flushes.load(Ordering::SeqCst), 1);
        assert_eq!(c.completed_at_flush.load(Ordering::SeqCst), 12);
        let mut data = vec![0u8; 12 * 4096];
        probe.read_exact_at(&mut data, 0).unwrap();
        for i in 0..12 {
            assert!(data[i * 4096..(i + 1) * 4096].iter().all(|&b| b == fill(i)));
        }
    }

    /// A write that fails fails its batch once every write has ended, and the flush is never
    /// issued.
    #[test]
    fn a_failed_write_fails_its_batch_and_issues_no_flush() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), true, Some(2 * 4096));
        let mut attached = issuer.attach(&probe).unwrap();
        assert!(attached.write(writes(5), true).is_err());
        let c = probe.counts;
        assert_eq!(c.completed.load(Ordering::SeqCst), 5);
        assert_eq!(c.flushes.load(Ordering::SeqCst), 0);
        // The next batch is the submitter's to decide on; the issuer serves it.
        attached.flush().unwrap();
        assert_eq!(c.flushes.load(Ordering::SeqCst), 1);
        assert_eq!(c.completed_at_flush.load(Ordering::SeqCst), 5);
    }

    /// Several submitters on one device share its workers: the device never has more than the
    /// depth in flight, whatever the number of files attached.
    #[test]
    fn submitters_share_the_depth() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), false, None);
        let mut attached: Vec<Attached> = (0..3).map(|_| issuer.attach(&probe).unwrap()).collect();
        std::thread::scope(|s| {
            let batches: Vec<_> = attached
                .iter_mut()
                .map(|a| s.spawn(move || a.write(writes(4), true)))
                .collect();
            probe.entered(2);
            probe.open();
            for batch in batches {
                assert_eq!(batch.join().unwrap().unwrap().len(), 4);
            }
        });
        let c = probe.counts;
        assert_eq!(c.most.load(Ordering::SeqCst), 2);
        assert_eq!(c.flushes.load(Ordering::SeqCst), 3);
    }

    /// A submitter attached for two batches has both out at once, their writes in flight
    /// together; a third is refused before anything is sent; each answer names its batch and
    /// gives back that batch's buffers; and a blocking write is refused while batches are out.
    #[test]
    fn submitted_batches_are_out_together_and_each_answer_names_its_batch() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 4).unwrap();
        let probe = Probe::new(dir.path(), false, None);
        assert!(issuer.attach_deep(&probe, 0).is_err());
        let mut attached = issuer.attach_deep(&probe, 2).unwrap();
        assert_eq!(attached.batches(), 2);
        let first = attached.submit(writes(2), false).unwrap();
        let mut second = writes(4);
        second.drain(..2);
        let second = attached.submit(second, true).unwrap();
        assert_eq!((first, second, attached.out()), (0, 1, 2));
        // Both batches' writes are on the device's workers at once.
        probe.entered(4);
        assert_eq!(probe.counts.in_flight.load(Ordering::SeqCst), 4);
        assert!(attached.submit(writes(1), false).is_err());
        assert_eq!(attached.out(), 2);
        assert!(attached.write(writes(1), false).is_err());
        assert!(attached.try_answer().unwrap().is_none());
        probe.open();
        let mut answers = [attached.answer().unwrap(), attached.answer().unwrap()];
        answers.sort_by_key(|(n, _)| *n);
        let [(n0, a0), (n1, a1)] = answers;
        assert_eq!((n0, n1), (0, 1));
        let (a0, a1) = (a0.unwrap(), a1.unwrap());
        assert_eq!(
            a0.iter().map(|(b, _)| b.as_slice()[0]).collect::<Vec<_>>(),
            [fill(0), fill(1)]
        );
        assert_eq!(
            a1.iter().map(|(b, _)| b.as_slice()[0]).collect::<Vec<_>>(),
            [fill(2), fill(3)]
        );
        assert_eq!(attached.out(), 0);
        assert!(attached.answer().is_err());
        assert!(attached.try_answer().unwrap().is_none());
        // The second batch's flush followed its own writes.
        assert_eq!(probe.counts.flushes.load(Ordering::SeqCst), 1);
        // Out of batches, the submitter writes as one with a single batch does.
        assert_eq!(attached.write(writes(1), true).unwrap().len(), 1);
    }

    /// Empty page buffers to read `n` pages into, at offsets `first..first + n` pages.
    fn reads(first: usize, n: usize) -> Vec<(AlignedBuf, u64)> {
        (first..first + n)
            .map(|i| {
                let mut buf = AlignedBuf::zeroed(4096, Alignment::new(4096).unwrap()).unwrap();
                buf.set_len(4096).unwrap();
                (buf, (i * 4096) as u64)
            })
            .collect()
    }

    /// Do: write four pages and take the answer, then read them back in one batch. Expect: the
    /// read batch's answer gives every buffer back, in order, holding its page's bytes.
    #[test]
    fn a_read_returns_the_bytes_an_answered_write_left() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 4).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let mut attached = issuer.attach_deep(&probe, 1).unwrap();
        attached.write(writes(4), true).unwrap();
        let number = attached.submit_reads(reads(0, 4)).unwrap();
        let (answered, bufs) = attached.answer().unwrap();
        assert_eq!(answered, number);
        let bufs = bufs.unwrap();
        assert_eq!(bufs.len(), 4);
        for (i, (buf, _)) in bufs.iter().enumerate() {
            assert!(
                buf.as_slice().iter().all(|&b| b == fill(i)),
                "page {i} read back other bytes"
            );
        }
    }

    /// Do: write pages 0 to 3 and take the answer; hold a batch of writes to pages 4 to 7 on the
    /// device's workers; read pages 0 to 3 while they are held. Expect: the reads are answered,
    /// with the answered writes' bytes, while the writes to the other offsets are still in
    /// flight; then the writes are answered once let go.
    #[test]
    fn reads_complete_while_writes_to_other_offsets_are_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 8).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let mut attached = issuer.attach_deep(&probe, 2).unwrap();
        attached.write(writes(4), false).unwrap();
        probe.counts.open.store(false, Ordering::SeqCst);
        let mut later = writes(8);
        later.drain(..4);
        let held = attached.submit(later, true).unwrap();
        probe.entered(8);
        assert_eq!(probe.counts.in_flight.load(Ordering::SeqCst), 4);
        let read = attached.submit_reads(reads(0, 4)).unwrap();
        let (answered, bufs) = attached.answer().unwrap();
        assert_eq!(answered, read, "the held writes were answered first");
        assert_eq!(probe.counts.in_flight.load(Ordering::SeqCst), 4);
        for (i, (buf, _)) in bufs.unwrap().iter().enumerate() {
            assert!(buf.as_slice().iter().all(|&b| b == fill(i)));
        }
        probe.open();
        let (answered, written) = attached.answer().unwrap();
        assert_eq!(answered, held);
        assert_eq!(written.unwrap().len(), 4);
    }

    /// Do: read a page past the end of the file, beside one inside it. Expect: the batch fails as a
    /// whole with the short read's error, and nothing is left out.
    #[test]
    fn a_read_past_the_end_fails_its_batch() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let mut attached = issuer.attach_deep(&probe, 1).unwrap();
        attached.write(writes(1), false).unwrap();
        let mut batch = reads(0, 1);
        batch.extend(reads(8, 1));
        attached.submit_reads(batch).unwrap();
        let (_, answer) = attached.answer().unwrap();
        assert!(answer.is_err(), "a read past the end was answered");
        assert_eq!(attached.out(), 0);
    }

    /// Do: hold a batch of writes, which fills the one batch the submitter attached for, and submit
    /// reads. Expect: the reads are refused before anything is sent; reads count among the
    /// batches out, as writes do.
    #[test]
    fn reads_past_the_batches_attached_for_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), false, None);
        let mut attached = issuer.attach_deep(&probe, 1).unwrap();
        attached.submit(writes(1), false).unwrap();
        assert!(attached.submit_reads(reads(0, 1)).is_err());
        assert_eq!(attached.out(), 1);
        probe.open();
        attached.answer().unwrap().1.unwrap();
    }

    /// Detaching gives every duplicate back before it returns; once the issuer is dropped, a
    /// batch is refused rather than left waiting.
    #[test]
    fn detach_releases_the_file_and_a_stopped_issuer_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let alive = || probe.counts.alive.load(Ordering::SeqCst);
        let attached = issuer.attach(&probe).unwrap();
        assert_eq!(
            alive(),
            3,
            "the probe and a duplicate for each of two workers"
        );
        drop(attached);
        assert_eq!(alive(), 1);
        let mut attached = issuer.attach(&probe).unwrap();
        drop(issuer);
        assert!(attached.write(writes(1), true).is_err());
        drop(attached);
        assert_eq!(alive(), 1);
    }

    /// An attacher is owned and moves to another thread: a submitter started after the issuer
    /// attaches through it, its batch lands and is flushed, and its detach gives every duplicate
    /// back, as an attachment through the issuer does.
    #[test]
    fn an_attacher_moved_to_another_thread_attaches_and_its_writes_land() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let attacher = issuer.attacher();
        let file = probe.try_clone().unwrap();
        let landed = std::thread::scope(|s| {
            s.spawn(move || {
                let mut attached = attacher.attach_deep(&file, 2).unwrap();
                let number = attached.submit(writes(2), true).unwrap();
                let (answered, answer) = attached.answer().unwrap();
                assert_eq!(answered, number);
                answer.map(|buffers| buffers.len())
            })
            .join()
            .unwrap()
        });
        assert_eq!(landed.unwrap(), 2);
        assert_eq!(probe.counts.flushes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
        let mut page = vec![0u8; 4096];
        probe.read_exact_at(&mut page, 4096).unwrap();
        assert_eq!(page, [fill(1); 4096]);
    }

    /// An attacher keeps no issuer alive: once its issuer has stopped, an attach through it is
    /// refused, typed, and no duplicate of the file outlives the refusal.
    #[test]
    fn an_attacher_whose_issuer_stopped_is_refused_and_keeps_no_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let attacher = issuer.attacher();
        drop(issuer);
        let refused = attacher.attach_deep(&probe, 1);
        assert!(
            matches!(&refused, Err(DiskError::Io { source, .. }) if source.kind() == std::io::ErrorKind::BrokenPipe),
            "{refused:?}"
        );
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
    }
}
