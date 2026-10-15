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
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::thread::{Builder, JoinHandle, Thread};

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
    events: Events,
    submission_pending: Sender<()>,
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

/// The same reusable completion channel carries retirement after every batch was answered.
#[derive(Debug)]
enum Completion {
    Batch(Numbered),
    Detached(Result<(), DiskError>),
}

#[derive(Debug)]
struct Retirement {
    slot: usize,
    generation: u64,
}

/// A lifecycle refusal, retained as a scalar until its terminal error is delivered.
#[derive(Clone, Copy, Debug)]
enum LifecycleFailure {
    FileDrop,
    FileArena,
    DispatchArena,
    Worker,
    Dispatcher,
    RetiredDrop,
}

impl LifecycleFailure {
    fn error(self, path: &Path) -> DiskError {
        let why = match self {
            Self::FileDrop => "a device file's Drop unwound",
            Self::FileArena => "the device worker's file arena refused its allocation",
            Self::DispatchArena => {
                "the device dispatcher's retirement arena refused its allocation"
            }
            Self::Worker => "a device worker's lifecycle ended unexpectedly",
            Self::Dispatcher => "the device dispatcher unwound",
            Self::RetiredDrop => "a retired value's Drop unwound",
        };
        stopped(path, why)
    }
}

/// A single owned duplicate. Explicit retirement reports its failure; implicit cleanup
/// during already-refused setup cannot let a foreign destructor unwind past ownership.
struct Handle(Option<Box<dyn BlockFile>>);

impl Handle {
    fn retire(&mut self) -> Option<LifecycleFailure> {
        let file = self.0.take()?;
        // The payload of a drop that unwound is dropped inside a boundary too (`unwind::contained`).
        crate::unwind::contained(|| drop(file))
            .is_none()
            .then_some(LifecycleFailure::FileDrop)
    }

    fn read_exact_at(&self, bytes: &mut [u8], at: u64) -> Result<(), DiskError> {
        self.0.as_ref().map_or_else(
            || Err(stopped(Path::new(""), "a read after duplicate retirement")),
            |file| file.read_exact_at(bytes, at),
        )
    }

    fn read_resident_at(&mut self, bytes: &mut [u8], at: u64) -> Result<bool, DiskError> {
        self.0
            .as_mut()
            .map_or(Ok(false), |file| file.read_resident_at(bytes, at))
    }

    fn residency(&self) -> crate::Residency {
        self.0
            .as_ref()
            .map_or_else(crate::Residency::default, |file| file.residency())
    }

    fn set_residency_shadow(&mut self, on: bool) {
        if let Some(file) = self.0.as_mut() {
            file.set_residency_shadow(on);
        }
    }

    fn write_all_at(&self, bytes: &[u8], at: u64) -> Result<(), DiskError> {
        self.0.as_ref().map_or_else(
            || Err(stopped(Path::new(""), "a write after duplicate retirement")),
            |file| file.write_all_at(bytes, at),
        )
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.0.as_ref().map_or_else(
            || Err(stopped(Path::new(""), "a flush after duplicate retirement")),
            |file| file.sync_data(),
        )
    }
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Handle").field(&self.0.is_some()).finish()
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Successful paths explicitly retire and report first. A rejected cold setup
        // already has its primary error; this protects its secondary cleanup too.
        let _ = self.retire();
    }
}

enum Event {
    Attach {
        /// One duplicate of the file for each worker.
        files: Vec<Handle>,
        /// The batches the submitter may have out at once.
        batches: usize,
        queued: VecDeque<Batch>,
        answers: Sender<Completion>,
        submissions: ChannelReceiver<Submission>,
        reply: SyncSender<Result<(usize, u64), DiskError>>,
    },
    Watch {
        slot: usize,
        generation: u64,
        done: SyncSender<Result<(), DiskError>>,
        reply: SyncSender<Result<(), DiskError>>,
    },
    Detach {
        slot: usize,
        generation: u64,
        done: SyncSender<()>,
    },
    Stop,
}

/// One attachment's bounded lane. A terminal command follows all consumed batch answers,
/// so it needs no room beyond the existing batch capacity.
#[derive(Debug)]
enum Submission {
    Batch {
        number: u64,
        transfers: Transfers,
        kind: Kind,
    },
    Retire,
}

/// Every cold inbox publication wakes the dispatcher. Batch/retirement submissions and
/// native reports have separate bounded lanes, and consume no cold inbox room.
#[derive(Clone, Debug)]
struct Events {
    queue: SyncSender<Event>,
    dispatcher: Thread,
}

impl Events {
    fn send(&self, event: Event) -> Result<(), SyncError<()>> {
        self.queue.send(event).map_err(|_| SyncError::Closed(()))?;
        self.wake();
        Ok(())
    }

    fn wake(&self) {
        self.dispatcher.unpark();
    }
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
    /// A taken cold news slot; never assigned to a native worker.
    Retired,
    /// Retained in that same slot until all native joins, with no new fault allocation.
    RetirementPanicked(Box<dyn std::any::Any + Send>),
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
    /// One lifecycle task ended, even if the foreign destructor failed.
    News {
        slot: usize,
        generation: u64,
        detached: bool,
        failed: Option<LifecycleFailure>,
    },
    Finished(Finished),
}

/// One not-yet-consumed assignment report plus one exit notice per actually started worker.
/// Taking that report is required before another assignment, so both roles fit without waiting.
const NATIVE_EXIT_ROLES: usize = 2;

enum NativeExit {
    Report(Report),
    Ended(Option<LifecycleFailure>),
}

/// Lives outside the worker body so an unexpected unwind still publishes its exit.
/// This notice is before TLS destruction; the broker must join before certification.
struct WorkerEnded<'a> {
    publisher: &'a NativePublisher,
    failed: Option<LifecycleFailure>,
}

impl Drop for WorkerEnded<'_> {
    fn drop(&mut self) {
        self.publisher.publish(NativeExit::Ended(self.failed));
    }
}

struct NativePublisher {
    retired: SyncSender<NativeExit>,
    pending: SyncSender<()>,
    dispatcher: Thread,
}

impl NativePublisher {
    fn publish(&self, exit: NativeExit) -> bool {
        // One counted assignment may publish one report before the broker takes it;
        // its exit is the other role. Thus both try_send calls are bounded and do not wait.
        let accepted = self.retired.try_send(exit).is_ok();
        let _ = self.pending.try_send(());
        self.dispatcher.unpark();
        accepted
    }
}

/// Cold ownership for one actually started native worker and its two exit roles.
struct NativeWorker<'scope> {
    handle: Option<std::thread::ScopedJoinHandle<'scope, Option<LifecycleFailure>>>,
    retirement: Receiver<NativeExit>,
    panic: Option<Box<dyn std::any::Any + Send>>,
    report: Option<Report>,
    ended: bool,
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
    /// ([`Self::attach_deep`]). Its declared cold inbox shape is retained; each attachment's
    /// own batch lane admits its configured credits without waiting on that shared inbox.
    /// Native completion and exit receipts have their own bounded worker lanes.
    #[allow(
        clippy::disallowed_methods,
        reason = "the issuer's bounded device workers: hyper-block runs its device's I/O (clippy.toml's file rule names it)"
    )]
    pub fn start_for(path: &Path, depth: usize, batches: usize) -> Result<Self, DiskError> {
        blocking_call_allowed(path, "issuer startup inside a runtime task")?;
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
        // Preserve the declared cold inbox shape. Batch publication uses each attachment's
        // lane; native workers now own distinct report/exit lanes and never wait on this inbox.
        let bound = workers
            .checked_add(batches)
            .ok_or_else(|| invalid(path, "the issuer's workers and batch budget exceed usize"))?;
        channel_capacity::<Event>(path, bound)?;
        // Attachments own their batch lanes. This coalesced pending-scan signal asks the
        // dispatcher to inspect them, without taking report or cold-setup inbox room.
        let (submission_pending, mut pending) =
            sync::channel(1).map_err(|error| DiskError::Io {
                op: "make an issuer submission doorbell",
                path: path.to_path_buf(),
                source: std::io::Error::other(error),
            })?;
        let (events, inbox) = sync_channel(bound);
        let (ready, started) = sync_channel(1);
        let device = path.to_path_buf();
        let thread = Builder::new()
            .name("hyper-issuer".into())
            .spawn(move || run(&device, inbox, workers, &ready, &mut pending))
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
            events: Events {
                queue: events,
                dispatcher: thread.thread().clone(),
            },
            submission_pending,
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
        attach(
            &self.events,
            &self.submission_pending,
            self.workers,
            &self.path,
            file,
            batches,
        )
    }

    /// A way to attach submitters to this issuer that its holder owns and may send to another
    /// thread: a submitter started after the device opened, on a thread of its own, attaches
    /// through it ([`Attacher::attach_deep`]). It holds the way to the issuer's inbox and nothing
    /// else, so it keeps no thread and no device alive: once the issuer has stopped, an attach
    /// through it is refused as one through the issuer would be.
    pub fn attacher(&self) -> Attacher {
        Attacher {
            events: self.events.clone(),
            submission_pending: self.submission_pending.clone(),
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
    events: Events,
    submission_pending: Sender<()>,
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
        attach(
            &self.events,
            &self.submission_pending,
            self.workers,
            &self.path,
            file,
            batches,
        )
    }
}

/// Hands the issuer whose inbox is `events` duplicates of `file`, one for each of its `workers`, for
/// a submitter that keeps up to `batches` out at once.
fn attach<F: BlockFile + 'static>(
    events: &Events,
    submission_pending: &Sender<()>,
    workers: usize,
    path: &Path,
    file: &F,
    batches: usize,
) -> Result<Attached, DiskError> {
    blocking_call_allowed(path, "issuer attachment inside a runtime task")?;
    if batches == 0 {
        return Err(invalid(path, "a submitter needs one batch at least"));
    }
    // Numbered batch results plus exactly one physical terminal result. Batch/data
    // admission remains batches, including queued, dispatched and untaken answers.
    let answer_bound = batches
        .checked_add(1)
        .ok_or_else(|| invalid(path, "batch answers plus terminal result exceed usize"))?;
    channel_capacity::<Completion>(path, answer_bound)?;
    channel_capacity::<Submission>(path, batches)?;
    // Reserve the same per-client batch storage before cloning or publishing ownership.
    let mut queued = VecDeque::new();
    queued
        .try_reserve_exact(batches)
        .map_err(|error| DiskError::Io {
            op: "reserve an attachment's batch arena",
            path: path.to_path_buf(),
            source: std::io::Error::other(error),
        })?;
    let mut files: Vec<Handle> = Vec::new();
    files
        .try_reserve_exact(workers)
        .map_err(|error| DiskError::Io {
            op: "reserve an attachment's file duplicates",
            path: path.to_path_buf(),
            source: std::io::Error::other(error),
        })?;
    for _ in 0..workers {
        files.push(Handle(Some(Box::new(file.try_clone()?))));
    }
    // The submitter's own duplicate, for what it reads from memory on its own thread
    // (`Attached::read_resident_at`): kept only for a file that can tell what the OS holds.
    let reader = if file.reads_resident() {
        Some(Handle(Some(Box::new(file.try_clone()?))))
    } else {
        None
    };
    // Each admitted batch has one numbered answer, plus one post-quiescence terminal fact.
    let (answers, answered) = sync::channel(answer_bound).map_err(|error| DiskError::Io {
        op: "attach completion channel",
        path: path.to_path_buf(),
        source: std::io::Error::other(error),
    })?;
    // Out counts queued, issued and answered-but-untaken batches. With out < batches,
    // this lane has room; retirement requires out == 0 and therefore an empty lane.
    let (submit, submissions) = sync::channel(batches).map_err(|error| DiskError::Io {
        op: "attach submission channel",
        path: path.to_path_buf(),
        source: std::io::Error::other(error),
    })?;
    let (reply, replied) = sync_channel(1);
    let gone = || stopped(path, "the device's issuer has stopped");
    events
        .send(Event::Attach {
            files,
            batches,
            queued,
            answers,
            submissions,
            reply,
        })
        .map_err(|_| gone())?;
    let (slot, generation) = replied.recv().map_err(|_| gone())??;
    Ok(Attached {
        slot,
        generation,
        events: events.clone(),
        submissions: Some(submit),
        submission_pending: submission_pending.clone(),
        answers: answered,
        path: path.to_path_buf(),
        batches,
        out: 0,
        next: 0,
        retirement: RetirementState::Live,
        watch_prepared: false,
        reader,
        reader_failure: None,
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

/// One submitter's way to its device's issuer: a volume's writer holds one. A cold owner
/// dropping it waits until no worker holds a duplicate. On an entered shard, Drop hands
/// accepted work and duplicate retirement to the issuer without a success acknowledgement;
/// await [`Attached::retire_async`] when that acknowledgement is required.
#[derive(Debug)]
pub struct Attached {
    slot: usize,
    generation: u64,
    events: Events,
    submissions: Option<Sender<Submission>>,
    submission_pending: Sender<()>,
    answers: ChannelReceiver<Completion>,
    path: PathBuf,
    /// The batches it may have out at once, those out, and the next batch's number.
    batches: usize,
    out: usize,
    next: u64,
    retirement: RetirementState,
    watch_prepared: bool,
    /// This submitter's own duplicate of the file, for the reads it makes from memory on its own
    /// thread ([`Self::read_resident_at`]): none for a file that cannot tell what the OS holds,
    /// and none once retirement began. It is closed before retirement is signalled, so the
    /// original, which its owner keeps open until this attachment's retirement is answered
    /// ([`RetirementWatch`]), is never the duplicate's to outlive: its close releases a
    /// descriptor and is never the file's physical close.
    reader: Option<Handle>,
    /// That duplicate's Drop unwound: retirement reports it.
    reader_failure: Option<LifecycleFailure>,
}

/// An independent physical fence for one registered attachment. Its receiver can
/// be cold-owned by a native retirement worker before the submitter enters a shard.
/// Dropping the submitter or its batch-answer receiver cannot complete this fence.
#[derive(Debug)]
pub struct RetirementWatch {
    received: Receiver<Result<(), DiskError>>,
    result: Option<Result<(), DiskError>>,
    path: PathBuf,
}

impl RetirementWatch {
    /// Waits on a cold/native owner until accepted I/O and every native file duplicate
    /// have physically retired. Every returned native result, including an error, is
    /// terminal; an entered-shard refusal leaves the receipt and state untouched.
    pub fn wait_blocking(&mut self) -> Result<(), DiskError> {
        blocking_call_allowed(&self.path, "a retirement watch inside a runtime shard")?;
        if self.result.is_none() {
            self.result =
                Some(self.received.recv().unwrap_or_else(|_| {
                    Err(stopped(&self.path, "the device's issuer has stopped"))
                }));
        }
        match self.result.as_ref() {
            Some(Ok(())) => Ok(()),
            Some(Err(error)) => Err(copy_disk_error(error)),
            None => Err(stopped(
                &self.path,
                "a retirement watch has no terminal result",
            )),
        }
    }

    /// True only after this independent receipt, or its post-native-join closure,
    /// was consumed. Context/admission refusal never establishes physical retirement.
    pub fn is_retired(&self) -> bool {
        self.result.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetirementState {
    Live,
    Waiting,
    Retired,
}

impl Attached {
    /// Registers one independent physical-retirement watch from the cold owner.
    /// The broker acknowledges ownership before the watch may be adopted elsewhere.
    /// Refusal leaves this attachment and every batch credit unchanged.
    pub fn prepare_retirement_watch(&mut self) -> Result<RetirementWatch, DiskError> {
        blocking_call_allowed(
            &self.path,
            "prepare a retirement watch inside a runtime shard",
        )?;
        if self.watch_prepared || self.retirement != RetirementState::Live {
            return Err(invalid(
                &self.path,
                "a retirement watch needs one live unregistered attachment",
            ));
        }
        // Exactly one physical terminal result and one cold registration acknowledgement.
        let (done, received) = sync_channel(1);
        let (reply, replied) = sync_channel(1);
        self.events
            .send(Event::Watch {
                slot: self.slot,
                generation: self.generation,
                done,
                reply,
            })
            .map_err(|_| stopped(&self.path, "the device's issuer has stopped"))?;
        replied
            .recv()
            .map_err(|_| stopped(&self.path, "the device's issuer has stopped"))??;
        self.watch_prepared = true;
        Ok(RetirementWatch {
            received,
            result: None,
            path: self.path.clone(),
        })
    }

    /// Issues every `(buffer, offset)` of `writes` at once, as deep as the device's workers go,
    /// and once all have completed, a flush when `flush` is set and every write succeeded.
    /// Returns `writes` back, every buffer and offset in order, after the last of them; the first
    /// failure otherwise, once none is in flight. Refused while batches submitted are out.
    pub fn write(&mut self, writes: Transfers, flush: bool) -> Answer {
        blocking_call_allowed(
            &self.path,
            "a synchronous device write inside a runtime task",
        )?;
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
        if self.retirement != RetirementState::Live {
            return Err(invalid(
                &self.path,
                "a batch after attachment retirement began",
            ));
        }
        let number = self.next;
        let gone = || stopped(&self.path, "the device's issuer has stopped");
        self.submissions
            .as_ref()
            .ok_or_else(gone)?
            .try_send(Submission::Batch {
                number,
                transfers,
                kind,
            })
            .map_err(|_| gone())?;
        self.next = self.next.wrapping_add(1);
        self.out = self.out.saturating_add(1);
        self.notify_submissions();
        Ok(number)
    }

    /// The next answer, waiting for it: the batch's number and its answer. Refused with no batch
    /// out.
    pub fn answer(&mut self) -> Result<Numbered, DiskError> {
        blocking_call_allowed(
            &self.path,
            "a synchronous device answer inside a runtime task",
        )?;
        if self.out == 0 {
            return Err(invalid(&self.path, "an answer with no batch out"));
        }
        let received = self.answers.blocking_recv();
        let answered = self.completion_result(received)?;
        self.batch_answer(answered)
    }

    /// The next answer, waiting as a hyper-rt task. Dropping this borrowed wait leaves the
    /// batch out and its buffers owned by the issuer or answer queue; a later wait or sync
    /// receive takes the same answer. Dropping the attachment still waits for its detach.
    pub async fn answer_async(&mut self) -> Result<Numbered, DiskError> {
        if self.out == 0 {
            return Err(invalid(&self.path, "an answer with no batch out"));
        }
        let answered = self.receive_completion().await?;
        self.batch_answer(answered)
    }

    /// The next answer if one has come; none otherwise, or with no batch out.
    pub fn try_answer(&mut self) -> Result<Option<Numbered>, DiskError> {
        if self.out == 0 {
            return Ok(None);
        }
        match self.answers.try_recv() {
            Ok(Some(answered)) => self.batch_answer(answered).map(Some),
            Ok(None) => Ok(None),
            Err(error) => self.completion_result(Err(error)).map(|_| None),
        }
    }

    fn batch_answer(&mut self, completion: Completion) -> Result<Numbered, DiskError> {
        match completion {
            Completion::Batch(answered) => {
                self.out = self.out.saturating_sub(1);
                Ok(answered)
            }
            Completion::Detached(result) => {
                self.retirement = RetirementState::Retired;
                self.out = 0;
                match result {
                    Err(error) => Err(error),
                    Ok(()) => Err(stopped(
                        &self.path,
                        "a detach where a batch answer was required",
                    )),
                }
            }
        }
    }

    /// Retires this attachment as a hyper-rt task, after every submitted batch's answer
    /// was taken. Every worker drops its duplicate before this returns. Another attachment's
    /// held I/O may delay that fact; this task waits without blocking its shard.
    ///
    /// The retirement owns its cold setup control slot and uses this attachment's existing
    /// completion channel. Canceling the borrowed wait neither repeats retirement nor drops
    /// a duplicate early; a later wait resumes it. The final Drop then has no work left.
    pub async fn retire_async(&mut self) -> Result<(), DiskError> {
        // Refuse before publishing retirement; every later receive poll checks again.
        std::future::poll_fn(|cx| std::task::Poll::Ready(completion_context(&self.path, cx)))
            .await?;
        self.begin_retirement()?;
        if self.retirement == RetirementState::Retired {
            return Ok(());
        }
        let completion = self.receive_completion().await?;
        self.retirement_result(completion)
    }

    /// Retires this attachment from its cold owner, after every submitted batch's answer
    /// was taken. This waits for the same physical retirement as `retire_async` and reports
    /// a native duplicate's lifecycle failure instead of discarding it during Drop.
    /// Any entered runtime shard is refused before publishing or consuming retirement.
    pub fn retire_blocking(&mut self) -> Result<(), DiskError> {
        blocking_call_allowed(
            &self.path,
            "a synchronous device retirement inside a runtime shard",
        )?;
        self.begin_retirement()?;
        if self.retirement == RetirementState::Retired {
            return Ok(());
        }
        let received = self.answers.blocking_recv();
        let completion = self.completion_result(received)?;
        self.retirement_result(completion)
    }

    fn begin_retirement(&mut self) -> Result<(), DiskError> {
        if self.out != 0 {
            return Err(invalid(
                &self.path,
                "retirement with batch answers still out",
            ));
        }
        if self.retirement == RetirementState::Live {
            self.close_reader();
            let submitted = match self.submissions.as_ref() {
                Some(sender) => sender.try_send(Submission::Retire),
                None => Err(SyncError::Closed(Submission::Retire)),
            };
            match submitted {
                Ok(()) => {
                    self.retirement = RetirementState::Waiting;
                    self.notify_submissions();
                }
                Err(SyncError::Closed(_)) => {
                    // Stop closes admission before joining. Its completion channel stays
                    // alive until every native duplicate has actually dropped.
                    self.retirement = RetirementState::Waiting;
                }
                Err(error) => {
                    return Err(DiskError::Io {
                        op: "retire a device attachment",
                        path: self.path.clone(),
                        source: std::io::Error::other(format!("{error:?}")),
                    });
                }
            }
        }
        Ok(())
    }

    fn retirement_result(&mut self, completion: Completion) -> Result<(), DiskError> {
        match completion {
            Completion::Detached(result) => {
                self.retirement = RetirementState::Retired;
                self.out = 0;
                result.and(
                    self.reader_failure
                        .take()
                        .map_or(Ok(()), |failed| Err(failed.error(&self.path))),
                )
            }
            Completion::Batch(_) => Err(stopped(
                &self.path,
                "a batch after all its answers were taken",
            )),
        }
    }

    async fn receive_completion(&mut self) -> Result<Completion, DiskError> {
        let received = {
            let path = &self.path;
            let mut receive = std::pin::pin!(self.answers.recv());
            std::future::poll_fn(|cx| {
                // Validate before taking a ready value, including every later poll.
                if let Err(error) = completion_context(path, cx) {
                    return std::task::Poll::Ready(Err(error));
                }
                std::future::Future::poll(receive.as_mut(), cx).map(Ok)
            })
            .await?
        };
        self.completion_result(received)
    }

    fn completion_result(
        &mut self,
        received: Result<Completion, SyncError<()>>,
    ) -> Result<Completion, DiskError> {
        if matches!(&received, Err(SyncError::Closed(()))) {
            // Completion senders survive every device duplicate's native join. Closed
            // is physical retirement, unlike context refusal or a numbered I/O error.
            self.retirement = RetirementState::Retired;
            self.out = 0;
        }
        received.map_err(|error| completion_error(&self.path, error))
    }

    fn notify_submissions(&self) {
        // Full already records the same scan. Closed follows native joins, and the
        // accepted batch/retirement must still be consumed or reported by its completion
        // channel; notification failure cannot turn accepted ownership into refusal.
        let _ = self.submission_pending.try_send(());
        self.events.wake();
    }

    /// Whether every device duplicate has physically retired. True after Detached or
    /// actual completion-channel closure following native joins, even when that closure
    /// reported a stopped-issuer error. Context/admission and numbered I/O errors alone
    /// do not establish this fact.
    pub fn is_retired(&self) -> bool {
        self.retirement == RetirementState::Retired
    }

    /// Batches submitted and not yet answered. Physical issuer closure retires all stale
    /// credits with a typed error; it does not acknowledge the missing batches as successful.
    pub fn out(&self) -> usize {
        self.out
    }

    /// The batches it may have out at once.
    pub fn batches(&self) -> usize {
        self.batches
    }

    /// Fills `buf` from `offset` only if the OS holds every byte of it in memory now: read through
    /// this submitter's own duplicate, on its own thread, with no worker and no wake between it
    /// and the bytes (`crate::resident`). False when the read would wait, the file cannot tell, or
    /// retirement began: the read is then the issuer's to make ([`Self::submit_reads`]).
    pub fn read_resident_at(&mut self, buf: &mut [u8], offset: u64) -> Result<bool, DiskError> {
        if self.retirement != RetirementState::Live {
            return Ok(false);
        }
        self.reader
            .as_mut()
            .map_or(Ok(false), |reader| reader.read_resident_at(buf, offset))
    }

    /// What [`Self::read_resident_at`] did through this submitter's duplicate.
    pub fn residency(&self) -> crate::Residency {
        self.reader
            .as_ref()
            .map_or_else(crate::Residency::default, Handle::residency)
    }

    /// Shadow mode for [`Self::read_resident_at`]'s kept answers, for measurement.
    pub fn set_residency_shadow(&mut self, on: bool) {
        if let Some(reader) = self.reader.as_mut() {
            reader.set_residency_shadow(on);
        }
    }

    /// Closes the submitter's own duplicate, before retirement is signalled (see `reader`).
    fn close_reader(&mut self) {
        if let Some(mut reader) = self.reader.take()
            && let Some(failed) = reader.retire()
        {
            self.reader_failure.get_or_insert(failed);
        }
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        if self.retirement == RetirementState::Retired {
            return;
        }
        // Before any retirement is signalled: see `reader`.
        self.close_reader();
        if hyper_rt::registry::current_shard().is_some() {
            // Closing the sole producer is the terminal command. The issuer drains its
            // queued lane and keeps accepted batches until their physical reports retire.
            self.submissions.take();
            self.notify_submissions();
            return;
        }
        if self.retirement == RetirementState::Waiting {
            // A canceled borrowed retirement still owns its receipt. Plain Drop retains its
            // blocking contract; a shard consumer completes retire_async before dropping.
            while let Ok(completion) = self.answers.blocking_recv() {
                if matches!(completion, Completion::Detached(_)) {
                    return;
                }
            }
            return;
        }
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
    inbox: Receiver<Event>,
    workers: usize,
    ready: &SyncSender<Result<(), DiskError>>,
    submission_pending: &mut ChannelReceiver<()>,
) {
    std::thread::scope(|scope| {
        // Coalesced yes/no native-report notification: one pending inspection.
        let (native_pending, completed) = sync_channel(1);
        let started = start_workers(scope, path, workers, &native_pending);
        let mut handles = started.native;
        let mut dispatch = Dispatch::new(started.assignments, path);
        // A foreign panic payload can itself panic on Drop. Retain it until every
        // native join, exactly like native-thread join payloads, before any sender closes.
        let mut broker_panic = None;
        if ready.send(started.result).is_ok()
            && dispatch.workers.len() == workers
            && let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| {
                dispatch.run(&inbox, submission_pending, &completed, &mut handles);
            }))
        {
            broker_panic = Some(payload);
            dispatch.failure.get_or_insert(LifecycleFailure::Dispatcher);
        }
        // Stop admission and assigned-task publication before joins. On an abnormal
        // broker exit, reports cannot block behind cold Attach traffic in this inbox.
        // Handle's cleanup boundary protects duplicates in never-admitted Attach events.
        dispatch.workers.clear();
        // Close cold admission before terminal waits. Native reports have their own
        // bounded lanes; a foreign unadmitted Attach destructor cannot discard them.
        let mut inbox_panic = std::panic::catch_unwind(AssertUnwindSafe(|| drop(inbox))).err();
        if inbox_panic.is_some() {
            dispatch.failure.get_or_insert(LifecycleFailure::FileDrop);
        }
        dispatch.retire_news();
        finish_native(&mut handles, &mut dispatch);
        dispatch.dispose_news(); // Every foreign disposal is guarded after all joins.
        dispatch.record_disposal(broker_panic.take());
        dispatch.record_disposal(inbox_panic.take());
        // No completion sender leaves before all native duplicates and TLS have joined.
        // The reserved terminal role fits even behind batches untaken numbered answers.
        dispatch.terminal();
    });
}

struct StartedWorkers<'scope> {
    assignments: Vec<SyncSender<Task>>,
    native: Vec<NativeWorker<'scope>>,
    result: Result<(), DiskError>,
}

fn start_workers<'scope, 'env>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    path: &Path,
    workers: usize,
    pending: &SyncSender<()>,
) -> StartedWorkers<'scope> {
    let mut started = StartedWorkers {
        assignments: Vec::with_capacity(workers),
        native: Vec::with_capacity(workers),
        result: Ok(()),
    };
    for _ in 0..workers {
        match start_worker(scope, pending) {
            Ok((assignment, worker)) => {
                started.assignments.push(assignment);
                started.native.push(worker);
            }
            Err(source) => {
                started.result = Err(DiskError::Io {
                    op: "start a device worker",
                    path: path.to_path_buf(),
                    source,
                });
                break;
            }
        }
    }
    started
}

fn start_worker<'scope, 'env>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    pending: &SyncSender<()>,
) -> Result<(SyncSender<Task>, NativeWorker<'scope>), std::io::Error> {
    let (tasks, assigned) = sync_channel(1);
    let (retired, retirement) = sync_channel(NATIVE_EXIT_ROLES);
    let publisher = NativePublisher {
        retired,
        pending: pending.clone(),
        dispatcher: std::thread::current(),
    };
    let handle = Builder::new()
        .name("hyper-io".into())
        .spawn_scoped(scope, move || {
            let mut ended = WorkerEnded {
                publisher: &publisher,
                failed: Some(LifecycleFailure::Worker),
            };
            let failed = work(&assigned, &publisher);
            ended.failed = failed;
            failed
        })?;
    Ok((
        tasks,
        NativeWorker {
            handle: Some(handle),
            retirement,
            panic: None,
            report: None,
            ended: false,
        },
    ))
}

impl NativeWorker<'_> {
    fn keep_exit(&mut self, exit: NativeExit) -> Option<LifecycleFailure> {
        match exit {
            NativeExit::Report(report) => {
                self.report = Some(report);
                None
            }
            NativeExit::Ended(failed) => {
                self.ended = true;
                failed
            }
        }
    }

    fn drain_exit(&mut self) -> Option<LifecycleFailure> {
        let mut first = None;
        for _ in 0..NATIVE_EXIT_ROLES {
            let Ok(exit) = self.retirement.try_recv() else {
                break;
            };
            first = first.or(self.keep_exit(exit));
        }
        first
    }

    fn wait_exit(&mut self) -> Option<LifecycleFailure> {
        if self.ended {
            return self.drain_exit();
        }
        for _ in 0..NATIVE_EXIT_ROLES {
            let Ok(exit) = self.retirement.recv() else {
                return Some(LifecycleFailure::Worker);
            };
            let failed = self.keep_exit(exit);
            if self.ended {
                return failed.or(self.drain_exit());
            }
        }
        Some(LifecycleFailure::Worker)
    }

    fn join(&mut self) -> Option<LifecycleFailure> {
        match self.handle.take()?.join() {
            Ok(failed) => failed,
            Err(payload) => {
                self.panic = Some(payload);
                Some(LifecycleFailure::Worker)
            }
        }
    }
}

fn finish_native(native: &mut [NativeWorker<'_>], dispatch: &mut Dispatch<'_>) {
    for worker in native.iter_mut() {
        if let Some(failed) = worker.wait_exit() {
            dispatch.failure.get_or_insert(failed);
        }
    }
    // Exit notices are before TLS. Every handle is still joined, with foreign panic
    // payloads retained until the entire native cohort joined.
    for worker in native.iter_mut() {
        if let Some(failed) = worker.join() {
            dispatch.failure.get_or_insert(failed);
        }
    }
    latch_native_reports(native, dispatch);
    dispose_native(native, dispatch);
}

fn latch_native_reports(native: &[NativeWorker<'_>], dispatch: &mut Dispatch<'_>) {
    // The original foreign errors remain owned; latching only borrows them.
    for worker in native {
        if let Some(report) = &worker.report {
            dispatch.terminal_report(report);
        }
    }
}

fn dispose_native(native: &mut [NativeWorker<'_>], dispatch: &mut Dispatch<'_>) {
    for worker in native {
        // Eager `or` is deliberate: both owned roles must be disposed, even on failure.
        let failed = worker
            .panic
            .take()
            .and_then(dispose_foreign)
            .or(worker.report.take().and_then(dispose_foreign));
        if let Some(failed) = failed {
            dispatch.failure.get_or_insert(failed);
        }
    }
}

/// A worker owns its file slots. Replacement/take precedes foreign Drop, so every
/// lifecycle task reports Done with deterministic remaining ownership on unwind.
fn work(tasks: &Receiver<Task>, publisher: &NativePublisher) -> Option<LifecycleFailure> {
    let mut files: Vec<FileSlot> = Vec::new();
    while let Ok(task) = tasks.recv() {
        let report = match task {
            Task::Attach {
                slot,
                generation,
                file,
            } => {
                let failed = match place(&mut files, slot, Some((generation, file))) {
                    Ok(old) => retire_file(old),
                    Err((unplaced, failed)) => {
                        let _ = retire_file(unplaced);
                        Some(failed)
                    }
                };
                Report::News {
                    slot,
                    generation,
                    detached: false,
                    failed,
                }
            }
            Task::Detach { slot, generation } => {
                let old = files.get_mut(slot).and_then(|held| {
                    if held.as_ref().is_some_and(|(g, _)| *g == generation) {
                        held.take()
                    } else {
                        None
                    }
                });
                let failed = retire_file(old);
                Report::News {
                    slot,
                    generation,
                    detached: true,
                    failed,
                }
            }
            Task::Transfer(transfer) => Report::Finished(carry_out(&files, transfer)),
            Task::Retired | Task::RetirementPanicked(_) => return Some(LifecycleFailure::Worker),
        };
        if !publisher.publish(NativeExit::Report(report)) {
            break;
        }
    }
    // EOF cannot silently drop native duplicates. Retire each before the joined result;
    // the first remaining lifecycle failure is retained without another allocation.
    let mut first = None;
    while let Some(file) = files.pop() {
        if let Some(failed) = retire_file(file) {
            first.get_or_insert(failed);
        }
    }
    first
}

/// One worker file slot: the attachment generation and its single native duplicate.
type FileSlot = Option<(u64, Handle)>;

/// Puts the new owner in its slot before returning the old one for guarded retirement.
fn place(
    files: &mut Vec<FileSlot>,
    slot: usize,
    file: FileSlot,
) -> Result<FileSlot, (FileSlot, LifecycleFailure)> {
    if files.len() <= slot {
        let Some(need) = slot.checked_add(1) else {
            return Err((file, LifecycleFailure::FileArena));
        };
        if files.try_reserve(need.saturating_sub(files.len())).is_err() {
            return Err((file, LifecycleFailure::FileArena));
        }
        files.resize_with(need, || None);
    }
    match files.get_mut(slot) {
        Some(at) => Ok(std::mem::replace(at, file)),
        None => Err((file, LifecycleFailure::FileArena)),
    }
}

fn retire_file(file: FileSlot) -> Option<LifecycleFailure> {
    file.and_then(|(_, mut file)| file.retire())
}

fn retire_task(task: Task) -> Option<LifecycleFailure> {
    match task {
        Task::Attach { mut file, .. } => file.retire(),
        Task::Detach { .. } | Task::Transfer(_) | Task::Retired => None,
        Task::RetirementPanicked(payload) => {
            drop(payload);
            None
        }
    }
}

fn retire_news(task: &mut Task) -> Option<LifecycleFailure> {
    let owned = std::mem::replace(task, Task::Retired);
    match std::panic::catch_unwind(AssertUnwindSafe(|| retire_task(owned))) {
        Ok(failed) => failed,
        Err(payload) => {
            *task = Task::RetirementPanicked(payload);
            Some(LifecycleFailure::FileDrop)
        }
    }
}

/// Issues one transfer on the worker's duplicate of its file.
fn carry_out(files: &[FileSlot], transfer: Transfer) -> Finished {
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
            let issued = crate::unwind::contained(move || {
                let result =
                    file.map_or_else(|| Err(missing()), |f| f.write_all_at(buf.as_slice(), at));
                (buf, result)
            });
            match issued {
                Some((buf, result)) => (Some((index, Some(buf))), result),
                None => (Some((index, None)), Err(unwound())),
            }
        }
        Op::Read { index, mut buf, at } => {
            let issued = crate::unwind::contained(move || {
                let result = file.map_or_else(
                    || Err(missing()),
                    |f| f.read_exact_at(buf.as_mut_slice(), at),
                );
                (buf, result)
            });
            match issued {
                Some((buf, result)) => (Some((index, Some(buf))), result),
                None => (Some((index, None)), Err(unwound())),
            }
        }
        Op::Flush => {
            let issued =
                crate::unwind::contained(|| file.map_or_else(|| Err(missing()), |f| f.sync_data()));
            (None, issued.unwrap_or_else(|| Err(unwound())))
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
    answers: Sender<Completion>,
    submissions: Option<ChannelReceiver<Submission>>,
    /// The sole owner closed its drained lane; batches still own pending/in-flight data.
    closing: bool,
    failure: Option<LifecycleFailure>,
    /// Cold Drop's receipt survives any retirement-reservation failure through native joins.
    detached: Option<SyncSender<()>>,
    /// Cold-registered independent receipt; it survives loss of the ordinary answers.
    watch: Option<SyncSender<Result<(), DiskError>>>,
    /// First actual write/flush or lifecycle error, even after its batch answer was taken.
    physical_error: Option<DiskError>,
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
    /// Read errors can be repaired; actual write/flush failures fence physical retirement.
    non_abandonable: bool,
    /// A flush is still to be issued once the writes have completed.
    flush: bool,
}

impl Client {
    fn submission(&mut self) -> Option<(u64, Option<Submission>)> {
        match self.submissions.as_mut()?.try_recv() {
            Ok(Some(submission)) => Some((self.generation, Some(submission))),
            Err(SyncError::Closed(())) => {
                self.submissions = None;
                self.closing = true;
                Some((self.generation, None))
            }
            Ok(None) | Err(_) => None,
        }
    }
}

/// A file being detached: the workers yet to drop their duplicates, and who waits for them.
struct Detaching {
    slot: usize,
    generation: u64,
    remaining: usize,
    failure: Option<LifecycleFailure>,
    done: DetachAnswer,
    watch: Option<SyncSender<Result<(), DiskError>>>,
    physical_error: Option<DiskError>,
}

enum DetachAnswer {
    Blocking(SyncSender<()>),
    Task(Sender<Completion>),
}

impl DetachAnswer {
    fn send(self, result: Result<(), DiskError>) {
        match self {
            Self::Blocking(done) => {
                let _ = done.try_send(());
            }
            Self::Task(done) => {
                let _ = done.try_send(Completion::Detached(result));
            }
        }
    }
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
    failure: Option<LifecycleFailure>,
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
            failure: None,
            path,
        }
    }

    fn run(
        &mut self,
        inbox: &Receiver<Event>,
        submission_pending: &mut ChannelReceiver<()>,
        exited: &Receiver<()>,
        native: &mut [NativeWorker<'_>],
    ) {
        let mut scan_needed = false;
        loop {
            // An early native unwind can leave a counted assignment without Done. Its
            // distinct exit lane and permit drive typed shutdown, never a forged completion.
            let exit_notified = matches!(exited.try_recv(), Ok(()));
            if exit_notified {
                self.native_exits(native);
                if self.failure.is_some() {
                    return;
                }
            }
            let mut progressed = match inbox.try_recv() {
                Ok(event) => {
                    self.event(event);
                    true
                }
                Err(TryRecvError::Empty) => false,
                Err(TryRecvError::Disconnected) => return,
            };
            progressed |= self.inspect_submissions(submission_pending, &mut scan_needed);
            self.dispatch();
            if self.failure.is_some() || (self.stopping && self.in_flight == 0) {
                return;
            }
            if !progressed {
                // Publication between the empty checks and park leaves a permit; later
                // publication wakes the parked dispatcher. Spurious wakes just recheck.
                std::thread::park();
            }
        }
    }

    fn inspect_submissions(&mut self, pending: &mut ChannelReceiver<()>, scan: &mut bool) -> bool {
        // Take the coalesced signal before scanning; late publication leaves the next permit.
        if !matches!(pending.try_recv(), Ok(Some(()))) && !*scan {
            return false;
        }
        *scan = false;
        for slot in 0..self.clients.len() {
            let submitted = self
                .clients
                .get_mut(slot)
                .and_then(Option::as_mut)
                .and_then(Client::submission);
            if let Some((generation, submission)) = submitted {
                *scan |= self.submission(slot, generation, submission);
            }
        }
        true
    }

    fn submission(&mut self, slot: usize, generation: u64, submission: Option<Submission>) -> bool {
        match submission {
            Some(Submission::Batch {
                number,
                transfers,
                kind,
            }) => {
                self.batch(slot, generation, number, transfers, kind);
                true
            }
            Some(Submission::Retire) => {
                self.retire(Retirement { slot, generation });
                true
            }
            None => {
                self.retire_if_finished(slot, generation);
                false
            }
        }
    }

    fn event(&mut self, event: Event) {
        match event {
            Event::Attach {
                files,
                batches,
                queued,
                answers,
                submissions,
                reply,
            } => {
                let attached = self.attach(files, batches, queued, answers, submissions);
                let _ = reply.try_send(attached);
            }
            Event::Watch {
                slot,
                generation,
                done,
                reply,
            } => {
                let result = self.watch(slot, generation, done);
                let _ = reply.try_send(result);
            }
            Event::Detach {
                slot,
                generation,
                done,
            } => self.detach(slot, generation, done),
            Event::Stop => {
                self.stopping = true;
                let bound = self.clients.len();
                for slot in 0..bound {
                    let lane = self
                        .clients
                        .get_mut(slot)
                        .and_then(Option::as_mut)
                        .and_then(|client| {
                            Some((client.generation, client.limit, client.submissions.take()?))
                        });
                    if let Some((generation, limit, mut submissions)) = lane {
                        // All pre-Stop publications are in this FIFO prefix, at most the
                        // declared out credit. Refuse each with its number before closing
                        // admission; later racing publications cannot make this unbounded.
                        for _ in 0..limit {
                            let Ok(Some(submission)) = submissions.try_recv() else {
                                break;
                            };
                            if let Submission::Batch {
                                number,
                                transfers,
                                kind,
                            } = submission
                            {
                                self.batch(slot, generation, number, transfers, kind);
                            }
                        }
                    }
                }
                // Completion senders remain through native joins, including for a
                // retirement refused after the admission lane closed.
                for transfer in std::mem::take(&mut self.pending) {
                    self.refused(transfer);
                }
            }
        }
    }

    fn done(&mut self, worker: usize, report: Report) {
        self.idle.push(worker);
        self.in_flight = self.in_flight.saturating_sub(1);
        match report {
            Report::News {
                slot,
                generation,
                detached,
                failed,
            } => self.lifecycle(slot, generation, detached, failed),
            Report::Finished(finished) => self.finished(finished, true),
        }
    }

    fn attach(
        &mut self,
        files: Vec<Handle>,
        limit: usize,
        queued: VecDeque<Batch>,
        answers: Sender<Completion>,
        submissions: ChannelReceiver<Submission>,
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
            submissions: Some(submissions),
            closing: false,
            failure: None,
            detached: None,
            watch: None,
            physical_error: None,
            limit,
            batches: queued,
        });
        match self.clients.get_mut(slot) {
            Some(free) => *free = client,
            None => self.clients.push(client),
        }
        Ok((slot, generation))
    }

    fn watch(
        &mut self,
        slot: usize,
        generation: u64,
        done: SyncSender<Result<(), DiskError>>,
    ) -> Result<(), DiskError> {
        if self.stopping {
            return Err(stopped(self.path, "the device's issuer is stopping"));
        }
        let path = self.path;
        let client = self
            .client(slot, generation)
            .ok_or_else(|| stopped(path, "the watched attachment has retired"))?;
        if client.watch.is_some() || client.closing {
            return Err(invalid(
                path,
                "a physical retirement watch is already registered or closing",
            ));
        }
        client.watch = Some(done);
        Ok(())
    }

    fn detach(&mut self, slot: usize, generation: u64, done: SyncSender<()>) {
        if self.client(slot, generation).is_none() {
            let _ = done.try_send(());
            return;
        }
        // Keep the cold receipt with the client before any fallible growth or batch routing.
        if let Some(client) = self.client(slot, generation) {
            client.detached = Some(done);
        }
        if !self.reserve_detach() {
            return;
        }
        // Batch publications precede Drop's Detach. Taking their bounded lane snapshot
        // preserves that FIFO relation before the client is removed; its owner cannot refill.
        let bound = self
            .client(slot, generation)
            .map_or(0, |client| client.limit);
        for _ in 0..bound {
            let submission = self
                .client(slot, generation)
                .and_then(|client| client.submissions.as_mut()?.try_recv().ok().flatten());
            let Some(submission) = submission else {
                break;
            };
            if let Submission::Batch {
                number,
                transfers,
                kind,
            } = submission
            {
                self.batch(slot, generation, number, transfers, kind);
            }
        }
        if let Some(client) = self.client(slot, generation)
            && client.watch.is_some()
        {
            client.submissions = None;
            client.closing = true;
            self.retire_if_finished(slot, generation);
            return;
        }
        let Some((done, failure)) = self
            .client(slot, generation)
            .and_then(|client| Some((client.detached.as_ref()?.clone(), client.failure)))
        else {
            return;
        };
        self.detach_with(
            slot,
            generation,
            DetachAnswer::Blocking(done),
            failure,
            None,
            None,
        );
        // The published Detaching now owns the receipt; no allocation separates this transfer.
        if let Some(client) = self.clients.get_mut(slot) {
            *client = None;
        }
    }

    fn retire(&mut self, retirement: Retirement) {
        if self
            .client(retirement.slot, retirement.generation)
            .is_none()
        {
            return;
        }
        if !self.reserve_detach() {
            return;
        }
        let Some(client) = self.client(retirement.slot, retirement.generation) else {
            return;
        };
        let done = match client.detached.take() {
            Some(done) => DetachAnswer::Blocking(done),
            None => DetachAnswer::Task(client.answers.clone()),
        };
        let failure = client.failure;
        let watch = client.watch.take();
        let physical_error = client.physical_error.take();
        self.detach_with(
            retirement.slot,
            retirement.generation,
            done,
            failure,
            watch,
            physical_error,
        );
        if let Some(client) = self.clients.get_mut(retirement.slot) {
            *client = None;
        }
    }

    fn retire_if_finished(&mut self, slot: usize, generation: u64) {
        if self
            .client(slot, generation)
            .is_some_and(|client| client.closing && client.batches.is_empty())
        {
            self.retire(Retirement { slot, generation });
        }
    }

    fn reserve_detach(&mut self) -> bool {
        if self.detaching.try_reserve(1).is_err()
            || self
                .news
                .iter_mut()
                .any(|news| news.try_reserve(1).is_err())
        {
            // Retain every client/receipt until the broker closes worker lanes and joins.
            self.failure.get_or_insert(LifecycleFailure::DispatchArena);
            return false;
        }
        true
    }

    fn detach_with(
        &mut self,
        slot: usize,
        generation: u64,
        done: DetachAnswer,
        failure: Option<LifecycleFailure>,
        watch: Option<SyncSender<Result<(), DiskError>>>,
        physical_error: Option<DiskError>,
    ) {
        for news in &mut self.news {
            news.push_back(Task::Detach { slot, generation });
        }
        self.detaching.push(Detaching {
            slot,
            generation,
            remaining: self.workers.len(),
            failure,
            done,
            watch,
            physical_error,
        });
    }

    /// A worker dropped its duplicate of a detached file; the last one answers the detach.
    fn dropped(&mut self, dropped: Option<(usize, u64)>, failed: Option<LifecycleFailure>) {
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
            if let Some(failed) = failed {
                d.failure.get_or_insert(failed);
            }
            d.remaining = d.remaining.saturating_sub(1);
            d.remaining == 0
        });
        if finished {
            let d = self.detaching.swap_remove(at);
            let result = d
                .failure
                .map_or(Ok(()), |failed| Err(failed.error(self.path)));
            send_watch(
                d.watch,
                d.physical_error
                    .or_else(|| d.failure.map(|failed| failed.error(self.path))),
            );
            d.done.send(result);
        }
    }

    fn lifecycle(
        &mut self,
        slot: usize,
        generation: u64,
        detached: bool,
        failed: Option<LifecycleFailure>,
    ) {
        let path = self.path;
        if let Some(failed) = failed
            && let Some(client) = self.client(slot, generation)
        {
            client.failure.get_or_insert(failed);
            for batch in &mut client.batches {
                // A known earlier transfer failure is never overwritten by teardown.
                batch.failed.get_or_insert_with(|| failed.error(path));
            }
        }
        if detached {
            self.dropped(Some((slot, generation)), failed);
        }
    }

    fn native_exits(&mut self, native: &mut [NativeWorker<'_>]) {
        for (index, worker) in native.iter_mut().enumerate() {
            self.native_worker(index, worker);
            if self.failure.is_some() {
                return;
            }
        }
    }

    fn native_worker(&mut self, index: usize, worker: &mut NativeWorker<'_>) {
        for _ in 0..NATIVE_EXIT_ROLES {
            match worker.retirement.try_recv() {
                Ok(NativeExit::Report(report)) => self.done(index, report),
                Ok(NativeExit::Ended(failed)) => {
                    worker.ended = true;
                    self.failure
                        .get_or_insert(failed.unwrap_or(LifecycleFailure::Worker));
                }
                Err(TryRecvError::Disconnected) if !worker.ended => {
                    worker.ended = true;
                    self.failure.get_or_insert(LifecycleFailure::Worker);
                }
                Err(_) => break,
            }
            if self.failure.is_some() {
                return;
            }
        }
    }

    fn terminal_report(&mut self, report: &Report) {
        match report {
            Report::Finished(finished) => self.terminal_transfer(finished),
            Report::News {
                slot,
                generation,
                failed,
                ..
            } => self.terminal_news(*slot, *generation, *failed),
        }
    }

    fn terminal_transfer(&mut self, finished: &Finished) {
        if let Err(error) = &finished.result {
            self.physical_failure(finished.slot, finished.generation, finished.number, error);
        }
    }

    fn terminal_news(&mut self, slot: usize, generation: u64, failed: Option<LifecycleFailure>) {
        let Some(failed) = failed else {
            return;
        };
        if let Some(client) = self.client(slot, generation) {
            client.failure.get_or_insert(failed);
        }
        if let Some(detaching) = self
            .detaching
            .iter_mut()
            .find(|d| d.slot == slot && d.generation == generation)
        {
            detaching.failure.get_or_insert(failed);
        }
    }

    fn retire_news(&mut self) {
        for news in &mut self.news {
            for task in news {
                if let Some(failed) = retire_news(task) {
                    self.failure.get_or_insert(failed);
                }
            }
        }
    }

    fn dispose_news(&mut self) {
        for news in &mut self.news {
            for task in news {
                let owned = std::mem::replace(task, Task::Retired);
                if let Some(failed) = dispose_foreign(owned) {
                    self.failure.get_or_insert(failed);
                }
            }
        }
    }

    fn record_disposal<T>(&mut self, owned: Option<T>) {
        if let Some(failed) = owned.and_then(dispose_foreign) {
            self.failure.get_or_insert(failed);
        }
    }

    fn terminal_watches(&mut self) {
        for client in self.clients.iter_mut().flatten() {
            let watch_error = client.physical_error.take().unwrap_or_else(|| {
                client.failure.or(self.failure).map_or_else(
                    || stopped(self.path, "the device's issuer has stopped"),
                    |failed| failed.error(self.path),
                )
            });
            send_watch(client.watch.take(), Some(watch_error));
        }
        for detaching in &mut self.detaching {
            let watch_error = detaching.physical_error.take().unwrap_or_else(|| {
                detaching.failure.or(self.failure).map_or_else(
                    || stopped(self.path, "the device's issuer has stopped"),
                    |failed| failed.error(self.path),
                )
            });
            send_watch(detaching.watch.take(), Some(watch_error));
        }
    }

    fn terminal(&mut self) {
        self.terminal_watches();
        for client in &mut self.clients {
            if let Some(mut client) = client.take() {
                // Preserve an already-known physical failure before later lifecycle/Stop.
                let failed = client
                    .batches
                    .iter_mut()
                    .find_map(|batch| batch.failed.take());
                let result = Err(failed.unwrap_or_else(|| {
                    client.failure.or(self.failure).map_or_else(
                        || stopped(self.path, "the device's issuer has stopped"),
                        |failed| failed.error(self.path),
                    )
                }));
                let _ = client.answers.try_send(Completion::Detached(result));
                if let Some(done) = client.detached.take() {
                    let _ = done.try_send(());
                }
            }
        }
        for detaching in self.detaching.drain(..) {
            let result = Err(detaching.failure.or(self.failure).map_or_else(
                || stopped(self.path, "the device's issuer has stopped"),
                |failed| failed.error(self.path),
            ));
            detaching.done.send(result);
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
        if let Some(failed) = client.failure {
            let _ = client
                .answers
                .try_send(Completion::Batch((number, Err(failed.error(path)))));
            return;
        }
        if refused || client.batches.len() >= client.limit {
            let why = if refused {
                "the device's issuer is stopping"
            } else {
                "a batch past those its submitter attached for"
            };
            let _ = client
                .answers
                .try_send(Completion::Batch((number, Err(stopped(path, why)))));
            return;
        }
        let count = transfers.len();
        if count == 0 && !flush {
            let _ = client
                .answers
                .try_send(Completion::Batch((number, Ok(transfers))));
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
                non_abandonable: matches!(kind, Kind::Write { .. }),
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
    fn physical_failure(&mut self, slot: usize, generation: u64, number: u64, error: &DiskError) {
        if let Some(client) = self.client(slot, generation)
            && client.physical_error.is_none()
            && client
                .batches
                .iter()
                .any(|batch| batch.number == number && batch.non_abandonable)
        {
            client.physical_error = Some(copy_disk_error(error));
        }
    }

    fn finished(&mut self, finished: Finished, physical: bool) {
        let Finished {
            slot,
            generation,
            number,
            write,
            result,
        } = finished;
        if physical && let Err(error) = &result {
            self.physical_failure(slot, generation, number, error);
        }
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
            let _ = client.answers.try_send(Completion::Batch((number, answer)));
        }
        self.retire_if_finished(slot, generation);
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
        self.finished(
            Finished {
                slot: transfer.slot,
                generation: transfer.generation,
                number: transfer.number,
                write,
                result: Err(stopped(path, why)),
            },
            false,
        );
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
        let sent = match self.workers.get(worker) {
            Some(slot) => slot.try_send(task),
            None => Err(TrySendError::Disconnected(task)),
        };
        match sent {
            Ok(()) => self.in_flight = self.in_flight.saturating_add(1),
            Err(TrySendError::Full(task) | TrySendError::Disconnected(task)) => {
                self.failure.get_or_insert(LifecycleFailure::Worker);
                let _ = retire_task(task); // Secondary cleanup cannot replace the first refusal.
            }
        }
        // Any refused assignment drives broker shutdown and real joins, not a fake Done.
        true
    }
}

/// Drop only after all native joins and inside a boundary. A normal destructor unwind
/// and its ordinary panic payload become a typed failure, before terminal publication.
/// Recursive/process-aborting panic payload destructors are outside recoverable claims.
fn dispose_foreign<T>(owned: T) -> Option<LifecycleFailure> {
    // The unwind's payload is dropped inside a boundary of its own, and one whose drop unwinds again is
    // left unfreed (`unwind::contained`).
    crate::unwind::contained(|| drop(owned))
        .is_none()
        .then_some(LifecycleFailure::RetiredDrop)
}

/// One reserved terminal credit, never a wait on the submitter or its receiver.
fn send_watch(done: Option<SyncSender<Result<(), DiskError>>>, error: Option<DiskError>) {
    if let Some(done) = done {
        let _ = done.try_send(error.map_or(Ok(()), Err));
    }
}

/// Only fault paths duplicate an error: the watch must retain it after the ordinary
/// numbered answer is consumed. Success adds no error allocation or transfer copy.
fn copy_disk_error(error: &DiskError) -> DiskError {
    match error {
        DiskError::Io { op, path, source } => DiskError::Io {
            op,
            path: path.clone(),
            source: source.raw_os_error().map_or_else(
                || {
                    let detail = crate::unwind::contained(|| source.to_string())
                        .unwrap_or_else(|| "a device error's formatter unwound".to_owned());
                    std::io::Error::new(source.kind(), detail)
                },
                std::io::Error::from_raw_os_error,
            ),
        },
        DiskError::Misaligned { offset, len, align } => DiskError::Misaligned {
            offset: *offset,
            len: *len,
            align: *align,
        },
        DiskError::ShortRead {
            path,
            offset,
            missing,
        } => DiskError::ShortRead {
            path: path.clone(),
            offset: *offset,
            missing: *missing,
        },
        DiskError::Buf(error) => DiskError::Buf(error.clone()),
        DiskError::Threads {
            path,
            asked,
            left,
            ceiling,
        } => DiskError::Threads {
            path: path.clone(),
            asked: *asked,
            left: *left,
            ceiling: *ceiling,
        },
        DiskError::Unsupported { path, reason } => DiskError::Unsupported {
            path: path.clone(),
            reason,
        },
        DiskError::Corrupt { path, what } => DiskError::Corrupt {
            path: path.clone(),
            what,
        },
    }
}

fn stopped(path: &Path, why: &str) -> DiskError {
    DiskError::Io {
        op: "issue",
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, why.to_owned()),
    }
}

fn completion_context(path: &Path, cx: &std::task::Context<'_>) -> Result<(), DiskError> {
    if hyper_rt::futures::current_task()
        .is_some_and(|task| hyper_rt::waker::waker_for(task.0).will_wake(cx.waker()))
    {
        Ok(())
    } else {
        Err(completion_error(path, SyncError::NotOnShardThread(())))
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

/// Rust 1.98 std's bounded queue packs an index, mark and lap into usize, and
/// allocates one AtomicUsize plus message slot per capacity. Refuse overflowing
/// ticket arithmetic or allocation layout before calling its infallible constructor.
fn channel_capacity<T>(path: &Path, capacity: usize) -> Result<(), DiskError> {
    let ticket = capacity
        .checked_add(1)
        .and_then(usize::checked_next_power_of_two)
        .and_then(|mark| mark.checked_mul(2));
    if capacity == 0 || ticket.is_none() {
        return Err(invalid(
            path,
            "the bounded channel's ticket capacity exceeds usize",
        ));
    }
    let (slot, _) = std::alloc::Layout::new::<std::sync::atomic::AtomicUsize>()
        .extend(std::alloc::Layout::new::<std::mem::MaybeUninit<T>>())
        .map_err(|_| invalid(path, "the bounded channel's slot layout exceeds isize"))?;
    let slot = slot.pad_to_align();
    let bytes = slot
        .size()
        .checked_mul(capacity)
        .ok_or_else(|| invalid(path, "the bounded channel's array size exceeds usize"))?;
    std::alloc::Layout::from_size_align(bytes, slot.align())
        .map_err(|_| invalid(path, "the bounded channel's array layout exceeds isize"))?;
    Ok(())
}

/// Blocking APIs stay on the cold owner or native workers. Refuse before hardware
/// setup, file duplication, accepted submission or consumption of a ready answer.
fn blocking_call_allowed(path: &Path, why: &'static str) -> Result<(), DiskError> {
    if hyper_rt::registry::current_shard().is_some() {
        Err(invalid(path, why))
    } else {
        Ok(())
    }
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
    #![allow(clippy::disallowed_types)]

    use std::future::{Future, poll_fn};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
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
        /// The offset whose write unwinds, `u64::MAX` for none.
        unwind_at: AtomicU64,
        /// Whether that write's unwind carries a payload whose drop unwinds too.
        bomb: AtomicBool,
        flushes: AtomicUsize,
        /// Writes completed when the last flush was issued.
        completed_at_flush: AtomicUsize,
        completed: AtomicUsize,
        /// Duplicates alive.
        alive: AtomicUsize,
        /// Whether the probe says it reads from memory (`BlockFile::reads_resident`), and the
        /// reads it made there.
        resident: AtomicBool,
        resident_reads: AtomicUsize,
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
            counts.unwind_at.store(u64::MAX, Ordering::SeqCst);
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

        fn reads_resident(&self) -> bool {
            self.counts.resident.load(Ordering::SeqCst)
        }

        fn read_resident_at(&mut self, buf: &mut [u8], offset: u64) -> Result<bool, DiskError> {
            self.file.read_exact_at(buf, offset)?;
            self.counts.resident_reads.fetch_add(1, Ordering::SeqCst);
            Ok(true)
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
            if c.unwind_at.load(Ordering::SeqCst) == offset {
                if c.bomb.load(Ordering::SeqCst) {
                    std::panic::panic_any(Bomb);
                }
                panic!("a write that unwinds");
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

    /// A panic payload whose drop unwinds too.
    struct Bomb;

    impl Drop for Bomb {
        fn drop(&mut self) {
            panic!("a payload that unwinds as it drops");
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
            latency_objective_ns: None,
        })
        .unwrap()
    }

    struct OpenOnDrop(&'static Counts);

    impl Drop for OpenOnDrop {
        fn drop(&mut self) {
            self.0.open.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct DropGate {
        entered: AtomicBool,
        open: Mutex<bool>,
        changed: Condvar,
    }

    impl DropGate {
        fn entered(&self) {
            let mut open = self.open.lock().unwrap();
            while !self.entered.load(Ordering::SeqCst) {
                open = self.changed.wait(open).unwrap();
            }
        }

        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    struct DropProbe {
        file: Probe,
        gate: Option<&'static DropGate>,
        duplicates: &'static DropGate,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            if let Some(gate) = self.gate {
                let mut open = gate.open.lock().unwrap();
                gate.entered.store(true, Ordering::SeqCst);
                gate.changed.notify_all();
                while !*open {
                    open = gate.changed.wait(open).unwrap();
                }
            }
        }
    }

    impl BlockFile for DropProbe {
        fn alignment(&self) -> Alignment {
            self.file.alignment()
        }

        fn len(&self) -> Result<u64, DiskError> {
            self.file.len()
        }

        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
            self.file.read_exact_at(buf, offset)
        }

        fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
            self.file.write_all_at(buf, offset)
        }

        fn sync_data(&self) -> Result<(), DiskError> {
            self.file.sync_data()
        }

        fn try_clone(&self) -> Result<Self, DiskError> {
            Ok(Self {
                file: self.file.try_clone()?,
                gate: Some(self.duplicates),
                duplicates: self.duplicates,
            })
        }
    }

    struct ReleaseDropOnDrop {
        attached: Attached,
        gate: &'static DropGate,
    }

    impl Drop for ReleaseDropOnDrop {
        fn drop(&mut self) {
            // Open before the attachment field's Drop can wait for native retirement.
            self.gate.release();
        }
    }

    /// Closed completion means native duplicates are gone, including when Issuer::Drop
    /// runs concurrently and a worker's actual file Drop is held after its last write.
    #[test]
    fn stopped_issuer_retirement_waits_for_worker_file_drop() {
        let dir = tempfile::tempdir().unwrap();
        let gate: &'static DropGate = Box::leak(Box::default());
        let probe = DropProbe {
            file: Probe::new(dir.path(), true, None),
            gate: None,
            duplicates: gate,
        };
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let mut attached = issuer.attach(&probe).unwrap();
        let returned = attached.write(writes(1), false).unwrap();
        assert_eq!(returned[0].0.as_slice(), &[fill(0); 4096]);
        assert_eq!(attached.out(), 0);
        let counts = probe.file.counts;
        let progressed: &'static AtomicBool = Box::leak(Box::default());
        let mut rt = runtime();
        std::thread::scope(|scope| {
            let mut owned = ReleaseDropOnDrop { attached, gate };
            let shutdown = scope.spawn(move || drop(issuer));
            gate.entered();
            eprintln!("issuer shutdown worker duplicate Drop held after completed write");
            rt.block_on(async move {
                assert_eq!(counts.alive.load(Ordering::SeqCst), 2);
                let mut retirement = std::pin::pin!(owned.attached.retire_async());
                poll_fn(|cx| {
                    assert!(retirement.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                hyper_rt::futures::spawn_detached(async move {
                    assert!(gate.entered.load(Ordering::SeqCst));
                    assert_eq!(counts.alive.load(Ordering::SeqCst), 2);
                    progressed.store(true, Ordering::SeqCst);
                    gate.release();
                })
                .unwrap();
                let result = retirement.await;
                assert!(matches!(
                    result,
                    Err(DiskError::Io { source, .. })
                        if source.kind() == std::io::ErrorKind::BrokenPipe
                ));
                assert!(progressed.load(Ordering::SeqCst));
                assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
            })
            .unwrap();
            shutdown.join().unwrap();
        });
        assert_eq!(probe.file.counts.alive.load(Ordering::SeqCst), 1);
    }

    struct ReleaseIoOnDrop {
        attached: Attached,
        counts: &'static Counts,
    }

    impl Drop for ReleaseIoOnDrop {
        fn drop(&mut self) {
            self.counts.open.store(true, Ordering::SeqCst);
        }
    }

    fn coalesced_submissions_with_held_io(fail_second: bool) {
        let dir = tempfile::tempdir().unwrap();
        let probe = Probe::new(dir.path(), false, fail_second.then_some(4096));
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let mut owned = ReleaseIoOnDrop {
            attached: issuer.attach_deep(&probe, 2).unwrap(),
            counts: probe.counts,
        };
        let first = owned.attached.submit(writes(1), false).unwrap();
        probe.entered(1);
        let progressed: &'static AtomicBool = Box::leak(Box::default());
        let counts = probe.counts;
        let mut rt = runtime();
        let (issuer, probe) = rt
            .block_on(async move {
                let mut second_bytes = writes(1);
                second_bytes[0].1 = 4096;
                let second = owned.attached.submit(second_bytes, false).unwrap();
                assert_eq!(owned.attached.out(), 2);
                assert!(owned.attached.submit(writes(1), false).is_err());
                assert_eq!(owned.attached.out(), 2);
                hyper_rt::futures::spawn_detached(async move {
                    assert_eq!(counts.in_flight.load(Ordering::SeqCst), 1);
                    progressed.store(true, Ordering::SeqCst);
                    counts.open.store(true, Ordering::SeqCst);
                })
                .unwrap();
                for expected in [first, second] {
                    let (number, answer) = owned.attached.answer_async().await.unwrap();
                    assert_eq!(number, expected);
                    if expected == second && fail_second {
                        assert!(answer.is_err());
                    } else {
                        assert_eq!(answer.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
                    }
                }
                assert!(progressed.load(Ordering::SeqCst));
                assert_eq!(owned.attached.out(), 0);
                counts.fail_at.store(u64::MAX, Ordering::SeqCst);
                let number = owned.attached.submit(writes(1), true).unwrap();
                let (answered, bytes) = owned.attached.answer_async().await.unwrap();
                assert_eq!(answered, number);
                assert_eq!(bytes.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
                owned.attached.retire_async().await.unwrap();
                drop(owned);
                (issuer, probe)
            })
            .unwrap();
        let mut read = [0; 4096];
        probe.read_exact_at(&mut read, 0).unwrap();
        assert_eq!(read, [fill(0); 4096]);
        drop(issuer);
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
    }

    /// A held device write cannot block another admitted lane publication or a sibling
    /// task; a coalesced doorbell continues every admitted batch to its exact answer.
    #[test]
    fn coalesced_submissions_keep_progress_and_batch_identity() {
        coalesced_submissions_with_held_io(false);
    }

    /// A failed queued write returns its own error, leaves no out credit, and the same
    /// attachment can write, flush and retire correctly afterward.
    #[test]
    fn a_coalesced_failed_submission_keeps_progress_and_reuse() {
        coalesced_submissions_with_held_io(true);
    }

    /// Stop preserves every pre-Stop batch number while an actual write is still held.
    /// A refused queued batch is answered first; terminal closure waits for the held write.
    #[test]
    fn stop_refuses_queued_numbers_and_waits_for_held_native_io() {
        let dir = tempfile::tempdir().unwrap();
        let probe = Probe::new(dir.path(), false, None);
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let mut owned = ReleaseIoOnDrop {
            attached: issuer.attach_deep(&probe, 2).unwrap(),
            counts: probe.counts,
        };
        let first = owned.attached.submit(writes(1), false).unwrap();
        probe.entered(1);
        let second = owned.attached.submit(writes(1), false).unwrap();
        let counts = probe.counts;
        let progressed: &'static AtomicBool = Box::leak(Box::default());
        let mut rt = runtime();
        eprintln!("stop case: actual first write Held; both batch numbers accepted");
        std::thread::scope(|scope| {
            let shutdown = scope.spawn(move || drop(issuer));
            rt.block_on(async move {
                let (refused, result) = owned.attached.answer_async().await.unwrap();
                assert_eq!(refused, second);
                assert!(matches!(result, Err(DiskError::Io { .. })));
                assert_eq!(counts.in_flight.load(Ordering::SeqCst), 1);
                assert_eq!(owned.attached.out(), 1);
                let (answered, bytes) = {
                    let mut answer = std::pin::pin!(owned.attached.answer_async());
                    poll_fn(|cx| {
                        assert!(answer.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    hyper_rt::futures::spawn_detached(async move {
                        assert_eq!(counts.in_flight.load(Ordering::SeqCst), 1);
                        progressed.store(true, Ordering::SeqCst);
                        counts.open.store(true, Ordering::SeqCst);
                    })
                    .unwrap();
                    answer.await.unwrap()
                };
                assert_eq!(answered, first);
                assert_eq!(bytes.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
                assert!(progressed.load(Ordering::SeqCst));
                assert_eq!(owned.attached.out(), 0);
                assert!(owned.attached.retire_async().await.is_err());
                assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
                drop(owned);
            })
            .unwrap();
            shutdown.join().unwrap();
        });
        let mut read = [0; 4096];
        probe.read_exact_at(&mut read, 0).unwrap();
        assert_eq!(read, [fill(0); 4096]);
    }

    /// Retirement of an idle file waits for a worker busy on another file, without holding
    /// the task's shard. A canceled borrowed wait keeps the same retirement and duplicates.
    #[test]
    fn cancelled_retirement_keeps_duplicates_and_allows_same_shard_progress() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let idle = Probe::new(first.path(), true, None);
        let held = Probe::new(second.path(), false, None);
        let mut idle_attachment = issuer.attach(&idle).unwrap();
        let mut held_attachment = issuer.attach(&held).unwrap();
        let number = held_attachment.submit(writes(1), false).unwrap();
        held.entered(1);
        let _open = OpenOnDrop(held.counts);
        let idle_counts = idle.counts;
        let held_counts = held.counts;
        let progressed: &'static AtomicBool = Box::leak(Box::default());
        let mut rt = runtime();
        rt.block_on(async move {
            // A still-out batch is refused without consuming its answer or retiring its file.
            assert!(held_attachment.retire_async().await.is_err());
            assert_eq!(held_attachment.out(), 1);
            {
                let mut retirement = std::pin::pin!(idle_attachment.retire_async());
                poll_fn(|cx| {
                    assert!(retirement.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                let canceled = race2(retirement.as_mut(), async {}).await;
                assert!(matches!(canceled, Either::Second(())));
            }
            assert_eq!(idle_counts.alive.load(Ordering::SeqCst), 2);
            assert!(idle_attachment.submit(writes(1), false).is_err());
            hyper_rt::futures::spawn_detached(async move {
                assert_eq!(held_counts.in_flight.load(Ordering::SeqCst), 1);
                assert_eq!(idle_counts.alive.load(Ordering::SeqCst), 2);
                progressed.store(true, Ordering::SeqCst);
                held_counts.open.store(true, Ordering::SeqCst);
            })
            .unwrap();
            idle_attachment.retire_async().await.unwrap();
            assert!(progressed.load(Ordering::SeqCst));
            assert_eq!(idle_counts.alive.load(Ordering::SeqCst), 1);
            idle_attachment.retire_async().await.unwrap();
            drop(idle_attachment);
            let (answered, buffers) = held_attachment.answer_async().await.unwrap();
            assert_eq!(answered, number);
            assert_eq!(buffers.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
            held_attachment.retire_async().await.unwrap();
            assert_eq!(held_counts.alive.load(Ordering::SeqCst), 1);
        })
        .unwrap();
        drop(issuer);
        assert_eq!(idle.counts.alive.load(Ordering::SeqCst), 1);
        assert_eq!(held.counts.alive.load(Ordering::SeqCst), 1);
    }

    /// Cold attachments remain independent of the issuer's batch inbox budget. Retirement
    /// releases their duplicates; an issuer that has fully stopped refuses retirement.
    #[test]
    fn retirement_preserves_attachment_setup_and_stopped_issuers_refuse_it() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let mut first = issuer.attach(&probe).unwrap();
        let second = issuer.attach(&probe).unwrap();
        let third = issuer.attach(&probe).unwrap();
        let mut rt = runtime();
        let (mut second, mut third) = rt
            .block_on(async move {
                first.retire_async().await.unwrap();
                drop(first);
                (second, third)
            })
            .unwrap();
        let mut replacement = issuer.attach(&probe).unwrap();
        rt.block_on(async move {
            replacement.retire_async().await.unwrap();
            second.retire_async().await.unwrap();
            third.retire_async().await.unwrap();
            drop(second);
            drop(third);
        })
        .unwrap();
        let mut stopped_attachment = issuer.attach(&probe).unwrap();
        drop(issuer);
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
        assert!(
            rt.block_on(async move {
                let result = stopped_attachment.retire_async().await;
                drop(stopped_attachment);
                result
            })
            .unwrap()
            .is_err()
        );
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
    }

    /// A foreign poll is refused before any retirement, leaving the file usable for an
    /// ordinary write and a later correctly driven task retirement.
    #[test]
    fn refused_foreign_retirement_leaves_attachment_usable() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let mut attached = issuer.attach(&probe).unwrap();
        {
            let mut retirement = std::pin::pin!(attached.retire_async());
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(
                retirement.as_mut().poll(&mut context),
                Poll::Ready(Err(_))
            ));
        }
        let buffers = attached.write(writes(1), false).unwrap();
        assert_eq!(buffers[0].0.as_slice(), &[fill(0); 4096]);
        let mut rt = runtime();
        rt.block_on(async move {
            attached.retire_async().await.unwrap();
        })
        .unwrap();
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
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

    /// A write that unwinds, its payload unwinding again as it drops or not, fails its batch,
    /// typed, and costs the device's one worker nothing: the next batch and a flush are served.
    #[test]
    fn a_write_that_unwinds_fails_its_batch_and_the_worker_goes_on() {
        for bomb in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
            let probe = Probe::new(dir.path(), true, None);
            probe.counts.unwind_at.store(4096, Ordering::SeqCst);
            probe.counts.bomb.store(bomb, Ordering::SeqCst);
            let mut attached = issuer.attach(&probe).unwrap();
            let failed = attached.write(writes(3), true).unwrap_err();
            assert!(
                failed.to_string().contains("a device worker unwound"),
                "bomb {bomb}: {failed}"
            );
            probe.counts.unwind_at.store(u64::MAX, Ordering::SeqCst);
            assert_eq!(
                attached.write(writes(3), true).unwrap().len(),
                3,
                "bomb {bomb}"
            );
            attached.flush().unwrap();
        }
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
    /// Do: attach a file that reads from memory, read through the attachment, then retire it
    /// while a cold thread waits on its retirement watch. Expect: the read is made on the
    /// submitter's own duplicate with the file's bytes, none goes to a worker; when the watch is
    /// answered only the original is left (the duplicate closed before retirement was signalled,
    /// so the original's close is the file's last); a retired attachment reads nothing here.
    #[test]
    fn a_submitters_own_duplicate_reads_from_memory_and_closes_before_retirement() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        probe.counts.resident.store(true, Ordering::SeqCst);
        probe.file.write_all_at(&[7; 8192], 0).unwrap();
        let mut attached = issuer.attach(&probe).unwrap();
        // The original, a duplicate for each of the two workers, and the submitter's own.
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 4);
        let mut buf = vec![0u8; 4096];
        assert!(attached.read_resident_at(&mut buf, 4096).unwrap());
        assert_eq!(buf, [7; 4096]);
        assert_eq!(probe.counts.resident_reads.load(Ordering::SeqCst), 1);
        assert_eq!(attached.out(), 0);
        let mut watch = attached.prepare_retirement_watch().unwrap();
        let counts = probe.counts;
        std::thread::scope(|scope| {
            let watched = scope.spawn(move || {
                watch.wait_blocking().unwrap();
                counts.alive.load(Ordering::SeqCst)
            });
            attached.retire_blocking().unwrap();
            assert_eq!(watched.join().unwrap(), 1);
        });
        assert!(!attached.read_resident_at(&mut buf, 0).unwrap());
        assert_eq!(probe.counts.resident_reads.load(Ordering::SeqCst), 1);
    }

    /// Do: attach a file that cannot tell what the OS holds. Expect: no duplicate is kept for the
    /// submitter, and it reads nothing from memory.
    #[test]
    fn a_file_that_cannot_tell_gets_no_submitter_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 2).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let mut attached = issuer.attach(&probe).unwrap();
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 3);
        let mut buf = vec![0u8; 4096];
        assert!(!attached.read_resident_at(&mut buf, 0).unwrap());
        attached.retire_blocking().unwrap();
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
    }

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
    // Insert inside the existing issuer native test module. Reuses its public Probe,
    // actual native write gate, runtime(), and OpenOnDrop; no production test hook.

    fn invalid_completion_poll<T>(result: &Poll<Result<T, DiskError>>) -> bool {
        matches!(result, Poll::Ready(Err(DiskError::Io { source, .. }))
        if source.kind() == std::io::ErrorKind::InvalidInput)
    }

    #[test]
    fn a_ready_async_answer_refuses_foreign_poll_before_consuming() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let mut attached = issuer.attach(&probe).unwrap();
        let number = attached.submit(Vec::new(), false).unwrap();
        // Stop refuses or finishes every pre-Stop accepted number before native joins.
        // Its completed return guarantees a queued numbered answer, not just a clock delay.
        drop(issuer);
        let foreign = {
            let mut answer = std::pin::pin!(attached.answer_async());
            answer
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        };
        let refused = invalid_completion_poll(&foreign);
        let retained = attached.out() == 1;
        let answered = match foreign {
            Poll::Ready(Ok(answered)) => answered,
            Poll::Ready(Err(_)) => attached.answer().unwrap(),
            Poll::Pending => panic!("a closed completed issuer cannot leave its answer pending"),
        };
        assert_eq!(answered.0, number);
        assert_eq!(attached.out(), 0);
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
        drop(attached);
        // Verdict follows full cleanup, including on the pre-fix consuming path.
        assert!(
            refused && retained,
            "a foreign ready poll consumed its batch credit"
        );
    }

    #[test]
    fn a_pending_async_answer_rechecks_foreign_context_when_ready() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), false, None);
        let mut attached = issuer.attach(&probe).unwrap();
        let number = attached.submit(writes(1), false).unwrap();
        probe.entered(1);
        let counts = probe.counts;
        let mut rt = runtime();
        let _open = OpenOnDrop(counts);
        let (armed, first_pending) = sync_channel(1);
        let (finished, mut joined) = hyper_rt::sync::channel(1).unwrap();
        let owned = ReleaseIoOnDrop { attached, counts };
        let (refused, retained, owned) = std::thread::scope(|scope| {
            let controller = scope.spawn(move || {
                first_pending.recv().unwrap();
                counts.open.store(true, Ordering::SeqCst);
                drop(issuer); // Queued answer and actual physical joins precede this fact.
                finished.try_send(()).unwrap();
            });
            let returned = rt
                .block_on(async move {
                    let mut owned = owned;
                    let foreign = {
                        let mut answer = std::pin::pin!(owned.attached.answer_async());
                        poll_fn(|cx| {
                            assert!(answer.as_mut().poll(cx).is_pending());
                            armed.try_send(()).unwrap();
                            Poll::Ready(())
                        })
                        .await;
                        joined.recv().await.unwrap();
                        // SAME borrowed future, after genuine Pending. The actual runtime task
                        // is present, but this noop waker is foreign to it.
                        answer
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                    };
                    let refused = invalid_completion_poll(&foreign);
                    let retained = owned.attached.out() == 1;
                    let answered = match foreign {
                        Poll::Ready(Ok(answered)) => answered,
                        Poll::Ready(Err(_)) => owned.attached.answer_async().await.unwrap(),
                        Poll::Pending => {
                            panic!("a retired held callback must leave a terminal answer")
                        }
                    };
                    assert_eq!(answered.0, number);
                    assert_eq!(answered.1.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
                    assert_eq!(owned.attached.out(), 0);
                    (refused, retained, owned)
                })
                .unwrap();
            controller.join().unwrap();
            returned
        });
        assert_eq!(counts.completed.load(Ordering::SeqCst), 1);
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        drop(owned);
        assert!(
            refused && retained,
            "the later foreign poll consumed a completed batch"
        );
    }

    #[test]
    fn a_pending_retirement_rechecks_foreign_context_at_terminal_delivery() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), false, None);
        let mut writer = issuer.attach(&probe).unwrap();
        let idle = issuer.attach(&probe).unwrap();
        let number = writer.submit(writes(1), false).unwrap();
        probe.entered(1);
        let counts = probe.counts;
        let mut rt = runtime();
        let _open = OpenOnDrop(counts);
        let (armed, first_pending) = sync_channel(1);
        let (finished, mut joined) = hyper_rt::sync::channel(1).unwrap();
        // Guards are already captured before admission; either attachment opens I/O before
        // its own field Drop on any assertion/refused-admission path.
        let writer = ReleaseIoOnDrop {
            attached: writer,
            counts,
        };
        let idle = ReleaseIoOnDrop {
            attached: idle,
            counts,
        };
        let (refused, writer, idle) = std::thread::scope(|scope| {
            let controller = scope.spawn(move || {
                first_pending.recv().unwrap();
                counts.open.store(true, Ordering::SeqCst);
                drop(issuer);
                finished.try_send(()).unwrap();
            });
            let returned = rt
                .block_on(async move {
                    let mut writer = writer;
                    let mut idle = idle;
                    let foreign = {
                        let mut retirement = std::pin::pin!(idle.attached.retire_async());
                        poll_fn(|cx| {
                            assert!(retirement.as_mut().poll(cx).is_pending());
                            armed.try_send(()).unwrap();
                            Poll::Ready(())
                        })
                        .await;
                        joined.recv().await.unwrap();
                        // Stop may deliver Detached or close after joins. Both are terminal-ready,
                        // and neither permits a foreign waker to bypass the later context check.
                        retirement
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                    };
                    let refused = invalid_completion_poll(&foreign);
                    let (answered, bytes) = writer.attached.answer_async().await.unwrap();
                    assert_eq!(answered, number);
                    assert_eq!(bytes.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
                    if !matches!(foreign, Poll::Ready(Ok(()))) {
                        let result = idle.attached.retire_async().await;
                        assert!(
                            result.is_ok()
                                || matches!(result,
                    Err(DiskError::Io { source, .. })
                        if source.kind() == std::io::ErrorKind::BrokenPipe)
                        );
                    }
                    (refused, writer, idle)
                })
                .unwrap();
            controller.join().unwrap();
            returned
        });
        drop(idle);
        drop(writer);
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        assert!(
            refused,
            "terminal-ready retirement skipped its later context check"
        );
    }
    // Insert into existing issuer native test module; uses its real BlockFile Probe.
    // Public calls and exact buffers only. Baseline cleanup precedes the semantic verdict.

    fn cold_call_refused<T>(result: &Result<T, DiskError>) -> bool {
        matches!(result, Err(DiskError::Io { source, .. })
        if source.kind() == std::io::ErrorKind::InvalidInput)
    }

    #[test]
    fn issuer_start_refuses_on_a_shard_before_hardware_setup() {
        let made = runtime()
            .block_on(async { Issuer::start_for(Path::new("dev"), 1, 1) })
            .unwrap();
        let refused = cold_call_refused(&made);
        drop(made); // Baseline's real native issuer is joined by the cold test owner.
        let healthy = Issuer::start_for(Path::new("dev"), 1, 1).unwrap();
        drop(healthy);
        assert!(refused, "a runtime call started the cold device issuer");
    }

    #[test]
    fn issuer_attach_refuses_on_a_shard_before_cloning_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let counts = probe.counts;
        let (issuer, probe, made, live_at_return) = runtime()
            .block_on(async move {
                let made = issuer.attach(&probe);
                let live = counts.alive.load(Ordering::SeqCst);
                (issuer, probe, made, live)
            })
            .unwrap();
        let refused = cold_call_refused(&made);
        let mut attached = match made {
            Ok(attached) => attached,
            Err(_) => issuer.attach(&probe).unwrap(),
        };
        let buffers = attached.write(writes(1), false).unwrap();
        assert_eq!(buffers[0].0.as_slice(), &[fill(0); 4096]);
        drop(attached);
        drop(issuer);
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        assert!(refused, "a runtime call admitted a cold attachment");
        assert_eq!(live_at_return, 1, "the refusal cloned a native file first");
    }

    fn runtime_write_refusal(flush: bool) {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let counts = probe.counts;
        let mut attached = issuer.attach(&probe).unwrap();
        let (mut attached, result, writes_at_return, flushes_at_return, out_at_return) = runtime()
            .block_on(async move {
                let result = if flush {
                    attached.flush().map(|()| Vec::new())
                } else {
                    attached.write(writes(1), false)
                };
                let wrote = counts.entered.load(Ordering::SeqCst);
                let flushed = counts.flushes.load(Ordering::SeqCst);
                let out = attached.out();
                (attached, result, wrote, flushed, out)
            })
            .unwrap();
        let refused = cold_call_refused(&result);
        drop(result);
        let returned = attached.write(writes(1), true).unwrap();
        assert_eq!(returned[0].0.as_slice(), &[fill(0); 4096]);
        let mut bytes = [0; 4096];
        probe.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(bytes, [fill(0); 4096]);
        drop(attached);
        drop(issuer);
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        assert!(refused, "a synchronous runtime call submitted native work");
        assert_eq!(writes_at_return, 0, "the refusal published a write");
        assert_eq!(flushes_at_return, 0, "the refusal published a flush");
        assert_eq!(out_at_return, 0, "the refusal spent a batch credit");
    }

    #[test]
    fn attached_write_refuses_on_a_shard_before_submission() {
        runtime_write_refusal(false);
    }

    #[test]
    fn attached_flush_refuses_on_a_shard_before_submission() {
        runtime_write_refusal(true);
    }

    #[test]
    fn attached_blocking_answer_refuses_on_a_shard_without_consuming_ready_credit() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, None);
        let mut attached = issuer.attach(&probe).unwrap();
        let number = attached.submit(Vec::new(), false).unwrap();
        drop(issuer); // Actual terminal completion, not an assumed scheduling delay.
        let (mut attached, result, out_at_return) = runtime()
            .block_on(async move {
                let result = attached.answer();
                let out = attached.out();
                (attached, result, out)
            })
            .unwrap();
        let refused = cold_call_refused(&result);
        let answer = match result {
            Ok(answer) => answer,
            Err(_) => attached.answer().unwrap(),
        };
        assert_eq!(answer.0, number);
        assert_eq!(attached.out(), 0);
        drop(attached);
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
        assert!(refused, "a runtime blocking call took its queued answer");
        assert_eq!(out_at_return, 1, "the refused call consumed a batch credit");
    }
    // Insert inside existing issuer native test module. First four cases compile on
    // both the baseline and proposed API; Root's external guard classifies failures
    // only after positive Held + entered-Drop witnesses. No internal timer or sleep.

    fn drop_runtime() -> hyper_rt::Runtime {
        let roles = 2; // One owner/destructor and one independently admitted release task.
        hyper_rt::Runtime::start(&RuntimeConfig {
            shards: 1,
            tasks_per_shard: roles,
            timers_per_shard: roles,
            interests_per_shard: hyper_rt::runtime::interests_for(roles),
            ring_entries: roles,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            batch: roles,
            pin: false,
            cores: Vec::new(),
            page_bytes: 4096,
            spin_ns: 0,
            wake_tracking: None,
            latency_objective_ns: None,
        })
        .unwrap()
    }

    struct ShardDropWitness {
        attached: Option<Attached>,
        entered: SyncSender<(bool, bool)>,
    }

    impl Drop for ShardDropWitness {
        fn drop(&mut self) {
            let entered = hyper_rt::registry::current_shard().is_some();
            let no_current_task = hyper_rt::futures::current_task().is_none();
            let _ = self.entered.try_send((entered, no_current_task));
            drop(self.attached.take());
        }
    }

    #[derive(Debug)]
    enum PhysicalDone {
        Write(bool),
        Flush(bool),
    }

    struct ObservedProbe {
        probe: Probe,
        completed: SyncSender<PhysicalDone>,
    }

    impl BlockFile for ObservedProbe {
        fn alignment(&self) -> Alignment {
            self.probe.alignment()
        }
        fn len(&self) -> Result<u64, DiskError> {
            self.probe.len()
        }
        fn read_exact_at(&self, bytes: &mut [u8], at: u64) -> Result<(), DiskError> {
            self.probe.read_exact_at(bytes, at)
        }
        fn write_all_at(&self, bytes: &[u8], at: u64) -> Result<(), DiskError> {
            let result = self.probe.write_all_at(bytes, at);
            let _ = self.completed.try_send(PhysicalDone::Write(result.is_ok()));
            result
        }
        fn sync_data(&self) -> Result<(), DiskError> {
            // Unlike Probe's counting-only sync, this fixture executes the native barrier.
            let result = self
                .probe
                .file
                .sync_data()
                .and_then(|()| self.probe.sync_data());
            let _ = self.completed.try_send(PhysicalDone::Flush(result.is_ok()));
            result
        }
        fn try_clone(&self) -> Result<Self, DiskError> {
            Ok(Self {
                probe: self.probe.try_clone()?,
                completed: self.completed.clone(),
            })
        }
    }

    fn dropped_accepted_batches(cancel_owner: bool, fail_first: bool) {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        assert_eq!(issuer.depth(), 1);
        let (completed, physically_done) = sync_channel(3); // Two writes + their one required flush.
        let probe = ObservedProbe {
            probe: Probe::new(dir.path(), false, fail_first.then_some(0)),
            completed,
        };
        let counts = probe.probe.counts;
        let _setup_open = OpenOnDrop(counts); // Protect even Runtime startup/admission failure.
        let mut attached = issuer.attach_deep(&probe, 2).unwrap();
        attached.submit(writes(1), false).unwrap();
        probe.probe.entered(1);
        let mut second = writes(1);
        second[0].0.as_mut_slice().fill(fill(1));
        second[0].1 = 4096;
        attached.submit(second, true).unwrap();
        assert_eq!(attached.out(), 2);
        eprintln!("positive held native write; two accepted batches including later flush");
        let rt = drop_runtime();
        let _runtime_open = OpenOnDrop(counts); // Open before Runtime joins on assertion failure.
        let shard = rt.shard_ids()[0];
        let (entered, dropping) = sync_channel(1);
        let (admitted, owner) = sync_channel(1);
        let witness = ShardDropWitness {
            attached: Some(attached),
            entered,
        };
        rt.spawn_on(shard, async move {
            if cancel_owner {
                // Actual actor identity, without private layout or allocation forcing.
                admitted
                    .try_send(hyper_rt::futures::current_task().unwrap())
                    .unwrap();
                let _owned = witness;
                std::future::pending::<()>().await;
            } else {
                drop(witness); // Current task is present in this business poll.
            }
        })
        .unwrap();
        if cancel_owner {
            rt.cancel(owner.recv().unwrap()).unwrap();
        }
        let (on_shard, no_current_task) = dropping.recv().unwrap();
        assert!(on_shard);
        assert_eq!(no_current_task, cancel_owner);
        eprintln!("actual entered Attached Drop witness; current_task_absent={no_current_task}");
        let (progressed, progress) = sync_channel(1);
        rt.spawn_on(shard, async move {
            // A setup/controller guard has not opened this native callback.
            assert!(!counts.open.load(Ordering::SeqCst));
            assert_eq!(counts.in_flight.load(Ordering::SeqCst), 1);
            assert_eq!(counts.alive.load(Ordering::SeqCst), 2);
            progressed.try_send(()).unwrap();
            counts.open.store(true, Ordering::SeqCst);
        })
        .unwrap();
        progress.recv().unwrap(); // Success requires the real same-shard release task to run.
        // Do not Stop the issuer before the queued accepted write and flush execute: Stop
        // is allowed to refuse queued work, so a join alone cannot establish this oracle.
        for expected in [
            PhysicalDone::Write(!fail_first),
            PhysicalDone::Write(true),
            PhysicalDone::Flush(true),
        ] {
            match (physically_done.recv().unwrap(), expected) {
                (PhysicalDone::Write(got), PhysicalDone::Write(want))
                | (PhysicalDone::Flush(got), PhysicalDone::Flush(want)) => assert_eq!(got, want),
                _ => panic!("native write/flush callback order changed"),
            }
        }
        drop(issuer); // Native joins, not elapsed time, close physical write ownership.
        assert_eq!(counts.completed.load(Ordering::SeqCst), 2);
        assert_eq!(counts.flushes.load(Ordering::SeqCst), 1);
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        let mut second_bytes = [0; 4096];
        probe.read_exact_at(&mut second_bytes, 4096).unwrap();
        assert_eq!(second_bytes, [fill(1); 4096]);
        if !fail_first {
            let mut first_bytes = [0; 4096];
            probe.read_exact_at(&mut first_bytes, 0).unwrap();
            assert_eq!(first_bytes, [fill(0); 4096]);
        }
        rt.shutdown().unwrap();
    }

    #[test]
    fn shard_drop_yields_while_accepted_batches_and_flush_are_physically_held() {
        dropped_accepted_batches(false, false);
    }

    #[test]
    fn canceled_owner_drop_yields_after_current_task_clears() {
        dropped_accepted_batches(true, false);
    }

    #[test]
    fn shard_drop_keeps_later_accepted_work_after_a_held_write_failure() {
        dropped_accepted_batches(false, true);
    }

    #[test]
    fn a_waiting_borrowed_retirement_can_be_dropped_without_blocking_its_shard() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        assert_eq!(issuer.depth(), 1);
        let probe = Probe::new(dir.path(), false, None);
        let counts = probe.counts;
        let _setup_open = OpenOnDrop(counts);
        let mut writer = issuer.attach(&probe).unwrap();
        let idle = issuer.attach(&probe).unwrap();
        let number = writer.submit(writes(1), false).unwrap();
        probe.entered(1);
        eprintln!("positive held native write on a different attachment");
        let rt = drop_runtime();
        let _runtime_open = OpenOnDrop(counts);
        let shard = rt.shard_ids()[0];
        let (entered, dropping) = sync_channel(1);
        let witness = ShardDropWitness {
            attached: Some(idle),
            entered,
        };
        rt.spawn_on(shard, async move {
            let mut witness = witness;
            {
                let attached = witness.attached.as_mut().unwrap();
                let mut wait = std::pin::pin!(attached.retire_async());
                poll_fn(|cx| {
                    assert!(wait.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            } // Borrowed wait canceled; the attachment still owns Waiting retirement.
            drop(witness);
        })
        .unwrap();
        let (on_shard, no_current_task) = dropping.recv().unwrap();
        assert!(on_shard && !no_current_task);
        eprintln!("actual entered Waiting-retirement Attached Drop witness");
        let (progressed, progress) = sync_channel(1);
        rt.spawn_on(shard, async move {
            assert!(!counts.open.load(Ordering::SeqCst));
            assert_eq!(counts.in_flight.load(Ordering::SeqCst), 1);
            assert_eq!(counts.alive.load(Ordering::SeqCst), 3);
            progressed.try_send(()).unwrap();
            counts.open.store(true, Ordering::SeqCst);
        })
        .unwrap();
        progress.recv().unwrap();
        let (answered, bytes) = writer.answer().unwrap();
        assert_eq!(answered, number);
        assert_eq!(bytes.unwrap()[0].0.as_slice(), &[fill(0); 4096]);
        drop(writer);
        drop(issuer);
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        rt.shutdown().unwrap();
    }
    // New API follow-ups, stage after production. They are not baseline-compatible
    // RED fixtures because is_retired did not previously exist.

    #[test]
    fn numbered_io_failure_is_not_physical_retirement() {
        let dir = tempfile::tempdir().unwrap();
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let probe = Probe::new(dir.path(), true, Some(0));
        let mut attached = issuer.attach(&probe).unwrap();
        let number = attached.submit(writes(1), false).unwrap();
        let (answered, failed) = attached.answer().unwrap();
        assert_eq!(answered, number);
        assert!(failed.is_err());
        assert_eq!(attached.out(), 0);
        assert!(!attached.is_retired());
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 2);
        let attached = runtime()
            .block_on(async move {
                attached.retire_async().await.unwrap();
                assert!(attached.is_retired());
                attached
            })
            .unwrap();
        assert_eq!(probe.counts.alive.load(Ordering::SeqCst), 1);
        drop(attached);
        drop(issuer);
    }

    #[test]
    fn stopped_completion_retirement_is_observed_only_after_native_duplicate_drop() {
        let dir = tempfile::tempdir().unwrap();
        let gate: &'static DropGate = Box::leak(Box::default());
        let probe = DropProbe {
            file: Probe::new(dir.path(), true, None),
            gate: None,
            duplicates: gate,
        };
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let mut attached = issuer.attach(&probe).unwrap();
        attached.write(writes(1), false).unwrap();
        let counts = probe.file.counts;
        let mut rt = runtime();
        std::thread::scope(|scope| {
            let mut owned = ReleaseDropOnDrop { attached, gate };
            let shutdown = scope.spawn(move || drop(issuer));
            gate.entered();
            rt.block_on(async move {
                {
                    let mut wait = std::pin::pin!(owned.attached.retire_async());
                    poll_fn(|cx| {
                        assert!(wait.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                }
                assert!(!owned.attached.is_retired());
                assert_eq!(counts.alive.load(Ordering::SeqCst), 2);
                hyper_rt::futures::spawn_detached(async move {
                    gate.release();
                })
                .unwrap();
                let failed = owned.attached.retire_async().await;
                assert!(matches!(failed, Err(DiskError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::BrokenPipe));
                assert!(owned.attached.is_retired());
                assert_eq!(owned.attached.out(), 0);
                assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
            })
            .unwrap();
            shutdown.join().unwrap();
        });
    }

    // Insert in existing issuer native test module. This fixture deliberately injects
    // a foreign destructor panic; no production panic site or scheduler sleep is added.
    // Root guard failure is meaningful only after actual completed-write/Drop-attempt facts.

    struct FailingDuplicateDrop {
        probe: Probe,
        fail: bool,
        attempted: SyncSender<()>,
    }

    impl Drop for FailingDuplicateDrop {
        fn drop(&mut self) {
            if self.fail {
                let _ = self.attempted.try_send(());
                panic!("native duplicate Drop failure witness");
            }
        }
    }

    impl BlockFile for FailingDuplicateDrop {
        fn alignment(&self) -> Alignment {
            self.probe.alignment()
        }
        fn len(&self) -> Result<u64, DiskError> {
            self.probe.len()
        }
        fn read_exact_at(&self, bytes: &mut [u8], at: u64) -> Result<(), DiskError> {
            self.probe.read_exact_at(bytes, at)
        }
        fn write_all_at(&self, bytes: &[u8], at: u64) -> Result<(), DiskError> {
            self.probe.write_all_at(bytes, at)
        }
        fn sync_data(&self) -> Result<(), DiskError> {
            self.probe.file.sync_data()
        }
        fn try_clone(&self) -> Result<Self, DiskError> {
            Ok(Self {
                probe: self.probe.try_clone()?,
                fail: true,
                attempted: self.attempted.clone(),
            })
        }
    }

    #[test]
    fn a_worker_duplicate_drop_failure_reports_terminal_error_instead_of_losing_done() {
        let dir = tempfile::tempdir().unwrap();
        let (attempted, dropping) = sync_channel(1);
        let file = FailingDuplicateDrop {
            probe: Probe::new(dir.path(), true, None),
            fail: false,
            attempted,
        };
        let counts = file.probe.counts;
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        assert_eq!(issuer.depth(), 1);
        let mut attached = issuer.attach(&file).unwrap();
        let returned = attached.write(writes(1), false).unwrap();
        assert_eq!(returned[0].0.as_slice(), &[fill(0); 4096]);
        assert_eq!(counts.completed.load(Ordering::SeqCst), 1);
        eprintln!("actual native write completed before duplicate retirement");
        let rt = drop_runtime();
        let shard = rt.shard_ids()[0];
        let (back, received) = sync_channel(1);
        rt.spawn_on(shard, async move {
            let result = attached.retire_async().await;
            // Return the owner to cold cleanup even when the typed terminal result is Err.
            back.try_send((attached, result)).unwrap();
        })
        .unwrap();
        dropping.recv().unwrap();
        eprintln!("actual worker duplicate Drop fault entered");
        let (progressed, progress) = sync_channel(1);
        rt.spawn_on(shard, async move {
            progressed.try_send(()).unwrap();
        })
        .unwrap();
        progress.recv().unwrap(); // Runtime is still polling independent tasks.
        let (attached, result) = received.recv().unwrap();
        assert!(
            matches!(result, Err(DiskError::Io { source, .. })
        if source.to_string().contains("Drop")),
            "the actual lifecycle failure must survive instead of successful Detached or generic Stop"
        );
        drop(attached);
        drop(issuer);
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        rt.shutdown().unwrap();
    }
    // Insert in the current issuer native test module. Existing Probe/runtime/
    // cold_call_refused/FailingDuplicateDrop helpers are the actual baseline wrappers.
    // No timer or ordering sleep. Parent owns compilation and native execution.

    #[test]
    fn explicit_cold_retirement_preserves_duplicate_drop_failure_after_physical_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let (attempted, dropping) = sync_channel(1);
        let file = FailingDuplicateDrop {
            probe: Probe::new(dir.path(), true, None),
            fail: false,
            attempted,
        };
        let counts = file.probe.counts;
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        assert_eq!(issuer.depth(), 1);
        let mut attached = issuer.attach(&file).unwrap();
        let returned = attached.write(writes(1), false).unwrap();
        assert_eq!(returned[0].0.as_slice(), &[fill(0); 4096]);
        assert_eq!(counts.completed.load(Ordering::SeqCst), 1);
        let mut bytes = [0; 4096];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(bytes, [fill(0); 4096]);
        eprintln!("actual native write completed before explicit cold retirement");
        let result = attached.retire_blocking();
        assert_eq!(dropping.try_recv(), Ok(()));
        assert!(attached.is_retired());
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        assert!(
            matches!(result, Err(DiskError::Io { source, .. })
            if source.to_string().contains("Drop")),
            "the explicit cold receipt must preserve the actual duplicate Drop failure"
        );
        drop(attached);

        // The dispatcher and its worker remain usable after the typed lifecycle failure.
        let healthy_dir = tempfile::tempdir().unwrap();
        let healthy = Probe::new(healthy_dir.path(), true, None);
        let mut next = issuer.attach(&healthy).unwrap();
        let returned = next.write(writes(1), true).unwrap();
        assert_eq!(returned[0].0.as_slice(), &[fill(0); 4096]);
        let mut bytes = [0; 4096];
        healthy.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(bytes, [fill(0); 4096]);
        next.retire_blocking().unwrap();
        assert!(next.is_retired());
        assert_eq!(healthy.counts.alive.load(Ordering::SeqCst), 1);
        drop(next);
        drop(issuer);
    }

    #[test]
    fn explicit_cold_retirement_refuses_on_a_shard_without_publishing_or_consuming() {
        let dir = tempfile::tempdir().unwrap();
        let file = Probe::new(dir.path(), true, None);
        let counts = file.counts;
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        let mut attached = issuer.attach(&file).unwrap();
        let (mut attached, result, retired_at_return) = runtime()
            .block_on(async move {
                let result = attached.retire_blocking();
                let retired = attached.is_retired();
                (attached, result, retired)
            })
            .unwrap();
        let refused = cold_call_refused(&result);
        drop(result);
        // The identical attachment still accepts and completes a native batch, proving that
        // the refused call did not publish its terminal command or consume its receipt.
        let returned = attached.write(writes(1), true).unwrap();
        assert_eq!(returned[0].0.as_slice(), &[fill(0); 4096]);
        let mut bytes = [0; 4096];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(bytes, [fill(0); 4096]);
        attached.retire_blocking().unwrap();
        assert!(attached.is_retired());
        drop(attached);
        drop(issuer);
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        assert!(refused, "a synchronous runtime call published retirement");
        assert!(!retired_at_return, "refusal consumed a retirement receipt");
    }
    // Insert in the current issuer native test module. The extreme numeric values
    // are rejected before the first native clone or infallible std queue constructor.

    #[test]
    fn attachment_extreme_batch_bounds_are_typed_refusals_before_any_file_clone() {
        let dir = tempfile::tempdir().unwrap();
        let file = Probe::new(dir.path(), true, None);
        let counts = file.counts;
        let issuer = Issuer::start(Path::new("dev"), 1).unwrap();
        for batches in [usize::MAX, usize::MAX - 1] {
            let result = issuer.attach_deep(&file, batches);
            let refused = cold_call_refused(&result);
            drop(result);
            assert!(
                refused,
                "an unrepresentable batch shape must be typed-refused"
            );
            assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
        }
        let mut attached = issuer.attach(&file).unwrap();
        let returned = attached.write(writes(1), true).unwrap();
        assert_eq!(returned[0].0.as_slice(), &[fill(0); 4096]);
        let mut bytes = [0; 4096];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(bytes, [fill(0); 4096]);
        attached.retire_blocking().unwrap();
        drop(attached);
        drop(issuer);
        assert_eq!(counts.alive.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn issuer_extreme_inbox_bound_is_typed_refused_before_thread_start() {
        let made = Issuer::start_for(Path::new("dev"), 1, usize::MAX - 1);
        let refused = cold_call_refused(&made);
        drop(made);
        let healthy = Issuer::start_for(Path::new("dev"), 1, 1).unwrap();
        drop(healthy);
        assert!(
            refused,
            "the std queue ticket overflow must be a typed refusal"
        );
    }
}
