//! The log's file and what is done with it: the I/O the log's owner asks for, done by a caller
//! that waits on the frame's answer or by the log's I/O thread (`serve`), never by the owner's.
//! The device is one owner's: it travels with the job, in a [`Carrier`], to the thread that does
//! it and comes back to the owner with the completion (hyper-raft CLAUDE.md §1, the sans-io rule's
//! exception).
//!
//! The completion comes back through the owner's returns, which wake no one: the owner reads them
//! before every message it takes, so anything a caller sends after hearing an answer the device
//! gave is heard after the completion that freed its room, as when the writer answered under its
//! lock. The thread that did the job wakes the owner only when the completion leaves the owner
//! something to answer (a frame another frame will confirm, a failure, a sweep, a read, a look);
//! the owner, when it has work waiting on the device, asks the I/O thread to wake it once the job
//! is back (`Request::Watch`). So a write whose frame confirms itself, with no one else
//! submitting, crosses from its caller to the owner and back: the owner sleeps once and the
//! caller once, as mantle's writer and its caller did.
//!
//! The operations of a frame are mantle's, in mantle's order: the frame, its persist record, then
//! one flush, nothing after a failed write, and a failed flush never retried (mantle
//! docs/design/raft-log.md §3). The thread that flushed a frame finishes it as mantle's writer
//! did, without handing back first: the frame before it is confirmed by this one's record, so its
//! callers are answered; then, unless the owner has said another frame follows
//! (`Device::follows`), this frame is confirmed on its own, as the owner would have had it be,
//! and its callers are answered too. The owner hears of the flush before the confirmation is
//! written, and publishes the frame then, as the writer published a frame once flushed. Each
//! answer goes out only after the owner's inbox holds the message that frees its room, so a
//! caller that submits again on hearing it finds the room given back, as it did when the writer
//! answered under its lock.

use std::cell::RefCell;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError};
use std::time::Instant;

use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment, Pool};

use crate::format::{self, FRAME_HEADER_BYTES, FRAME_HEADER_LEN, Owned};
use crate::owner::Message;
use crate::recover::{Found, Reader, Segment};
use crate::ticket::{Answer, Ticket};
use crate::{Entry, LogError};

/// Jobs the owner holds for the device at most: one write-path job (a sweep, a frame or a
/// confirmation), one read of entries and one caller's look at the file, each of which the owner
/// keeps to one at a time.
pub(crate) const JOBS: usize = 3;

/// Frames the owner may say another follows for before the device looks: the frame on the device,
/// and a stale word about the frame before, which the device drops.
pub(crate) const MORE: usize = 2;

/// Requests the I/O thread holds at most: the job out, and the owner's ask to be woken once it is
/// back; the owner has one job out at a time and asks once for it.
pub(crate) const REQUESTS: usize = 2;

/// Tokens the owner's token channel holds while the owner holds its receiver: the owner empties
/// it as it takes each job back and as a watch gives it back, one job is out at a time, and a
/// job's token is sent after the job is back, so the last job's token can follow the emptying and
/// the next job's join it before the next. While the I/O thread watches, it takes each token as
/// it comes, so a job's thread that finds the channel full waits only for it.
pub(crate) const TOKENS: usize = 2;

/// A look at the file a caller runs on the thread that holds the device.
pub(crate) type Look<F> = Box<dyn FnOnce(&F) + Send>;

/// I/O handed to a thread to do: a job and the device, which go back to the owner once done.
pub(crate) trait Io: Send {
    /// Does the job, gives the owner back the device and the completion, and gives the answers
    /// the job gave.
    fn run(self: Box<Self>);
}

/// What the I/O thread is asked to do.
pub(crate) enum Request {
    /// A job, no caller doing it.
    Job(Box<dyn Io>),
    /// Wake the owner once the job of this sequence is back, the token of each job back coming
    /// through this receiver; the receiver goes back to the owner with the word.
    Watch(u64, Receiver<u64>),
}

/// A job and the device on their way to the thread that does it, and the completion and the
/// device on their way back: one box, kept by the owner from job to job.
pub(crate) struct Carrier<F> {
    pub(crate) job: Option<Job<F>>,
    pub(crate) device: Option<Device<F>>,
    pub(crate) completion: Option<Completion>,
    /// The job's place among the jobs the owner has given out.
    pub(crate) sequence: u64,
}

impl<F> Carrier<F> {
    pub(crate) fn new(job: Job<F>, device: Device<F>, sequence: u64) -> Self {
        Self {
            job: Some(job),
            device: Some(device),
            completion: None,
            sequence,
        }
    }
}

thread_local! {
    /// The answers a job gives once the owner holds its completion, kept by each thread that does
    /// jobs from one job to the next: at most one frame's updates and the frame's before it.
    static ANSWERS: RefCell<Vec<Answering>> = const { RefCell::new(Vec::new()) };
}

impl<F: BlockFile + Send + 'static> Io for Carrier<F> {
    fn run(mut self: Box<Self>) {
        let (Some(job), Some(mut device)) = (self.job.take(), self.device.take()) else {
            return;
        };
        let mut answers = ANSWERS
            .try_with(|a| a.try_borrow_mut().map(|mut a| std::mem::take(&mut *a)).ok())
            .ok()
            .flatten()
            .unwrap_or_default();
        let completion = device.execute(job, &mut answers);
        // A confirmation that held answered every caller it confirms: the owner has only room to
        // give back and the frame to settle, which it does before the next message it takes.
        let tell = !matches!(completion, Completion::Confirm { result: Ok(()), .. });
        let (returns, tokens, inbox) = (
            device.returns.clone(),
            device.tokens.clone(),
            device.inbox.clone(),
        );
        let sequence = self.sequence;
        self.completion = Some(completion);
        self.device = Some(device);
        // The owner holds the completion before anyone hears an answer the job gave; should the
        // owner have gone, the answers are dropped and each caller hears the log closed.
        if returns.send(self).is_ok() {
            // Every token is sent, so a watch for this job ends (`TOKENS`).
            let _ = tokens.send(sequence);
            if tell {
                let _ = inbox.send(Message::Returned);
            }
            for answering in answers.drain(..) {
                answering.durable();
            }
        }
        answers.clear();
        let _ = ANSWERS.try_with(|a| {
            if let Ok(mut a) = a.try_borrow_mut() {
                *a = answers;
            }
        });
    }
}

/// The log's I/O thread: it does the jobs no caller does, and wakes the owner when it asks to be
/// woken for a job back. It ends once the owner has.
pub(crate) fn serve<F>(requests: &Receiver<Request>, inbox: &SyncSender<Message<F>>) {
    while let Ok(request) = requests.recv() {
        match request {
            Request::Job(io) => io.run(),
            Request::Watch(sequence, tokens) => {
                loop {
                    match tokens.recv() {
                        Ok(back) if back >= sequence => break,
                        Ok(_) => {}
                        Err(_) => return,
                    }
                }
                if inbox.send(Message::Watched(tokens)).is_err() {
                    return;
                }
            }
        }
    }
}

/// One caller's answer for an update of a frame: its ticket, and the entries a handle's write
/// gets back.
pub(crate) struct Answering {
    pub(crate) ticket: Ticket,
    pub(crate) entries: Vec<Entry>,
}

impl Answering {
    /// Answers the update durable.
    pub(crate) fn durable(mut self) {
        self.ticket.answer(Ok(Answer::Durable(self.entries)));
    }
}

/// What the owner asks of the device.
pub(crate) enum Job<F> {
    /// A frame, and its persist record, then one flush; then the frame before is answered and,
    /// unless another frame follows, this one confirmed and answered.
    Frame(Frame),
    /// A confirmation: a persist record written again, then one flush, then its frame answered.
    Confirm {
        record: AlignedBuf,
        record_at: u64,
        these: Vec<Answering>,
    },
    /// The tail's frames, read through a window of a segment and decoded.
    Sweep {
        segment: Segment,
        offset: u64,
        end: u64,
    },
    /// Entries read back from the file.
    Read(Reads),
    Look(Look<F>),
}

/// A frame for the device.
pub(crate) struct Frame {
    pub(crate) frame: AlignedBuf,
    pub(crate) at: u64,
    pub(crate) record: AlignedBuf,
    pub(crate) record_at: u64,
    /// The frame's sequence.
    pub(crate) sequence: u64,
    /// The frame's own confirmation, written over its record should no frame follow.
    pub(crate) confirm: AlignedBuf,
    /// The answers of the frame before, which this frame's record confirms.
    pub(crate) before: Vec<Answering>,
    /// The answers of this frame's updates.
    pub(crate) these: Vec<Answering>,
    /// The owner knew when it laid the frame out that another follows.
    pub(crate) more: bool,
}

/// What the device answers.
pub(crate) enum Completion {
    /// A frame flushed, or failed. With `confirming`, the device goes on to confirm it on its
    /// own, and a `Confirm` completion follows with the device.
    Frame {
        frame: AlignedBuf,
        record: AlignedBuf,
        /// The frame's own confirmation, unless the device is writing it.
        confirm: AlignedBuf,
        result: Result<(), LogError>,
        /// The batch's service time: its writes and its flush.
        took_ns: u64,
        confirming: bool,
        /// Answers not given: the frame before's when the frame failed, and this frame's unless
        /// the device is confirming it. Emptied lists come back for later frames.
        before: Vec<Answering>,
        these: Vec<Answering>,
    },
    Confirm {
        record: AlignedBuf,
        result: Result<(), LogError>,
        these: Vec<Answering>,
    },
    Sweep(Result<Vec<Swept>, LogError>),
    Read(Reads),
    Looked,
}

/// One verified frame of a swept tail: the file offset of its payload and its records.
pub(crate) struct Swept {
    pub(crate) base: u64,
    pub(crate) records: Vec<Owned>,
}

/// Runs of entries to read back, each a block-aligned span of the file, and where each entry
/// found goes in the caller's reservation.
pub(crate) struct Reads {
    pub(crate) group: u128,
    pub(crate) runs: Vec<Run>,
    /// The entries of every run, a run's after the run's before it.
    pub(crate) wanted: Vec<Wanted>,
    pub(crate) into: crate::Fetched,
    /// Entries a run did not hold as asked: moved since their place was taken, or damaged.
    pub(crate) missed: Vec<Wanted>,
    pub(crate) result: Result<(), LogError>,
}

/// A block-aligned span of the file and the entries in it.
pub(crate) struct Run {
    pub(crate) begin: u64,
    pub(crate) end: u64,
    /// The segment slot the span lies in: a run never crosses into another.
    pub(crate) slot: u32,
    /// The place in the caller's reservation that the run's next entry would take: a run holds
    /// entries asked for one after another.
    pub(crate) contiguous: usize,
    /// Its entries in the reads' list: `count` of them from `first`.
    pub(crate) first: usize,
    pub(crate) count: usize,
}

impl Reads {
    /// Reads of nothing.
    pub(crate) fn none() -> Self {
        Self {
            group: 0,
            runs: Vec::new(),
            wanted: Vec::new(),
            into: crate::Fetched::default(),
            missed: Vec::new(),
            result: Ok(()),
        }
    }
}

/// An entry to read: its position among the entries asked for, its index and term, and the
/// file offset where it was written.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Wanted {
    pub(crate) at: usize,
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) offset: u64,
    pub(crate) len: u32,
}

/// The failure a file operation that unwound is taken for.
fn unwound() -> LogError {
    LogError::Disk(hyper_block::DiskError::Io {
        op: "a file operation unwound",
        path: std::path::PathBuf::new(),
        source: std::io::Error::other("the file unwound"),
    })
}

/// Runs `op` on the file inside an unwind boundary: the file is the caller's code, and should it
/// unwind the operation fails as a failed write, flush or read would (CLAUDE.md §1).
fn guarded<R>(op: impl FnOnce() -> Result<R, LogError>) -> Result<R, LogError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(op)).unwrap_or_else(|_| Err(unwound()))
}

/// The log's file, with what reading it back keeps, and where the owner says another frame
/// follows.
pub(crate) struct Device<F> {
    pub(crate) file: F,
    /// Buffers for reading entries back, kept between reads.
    pool: Pool,
    segment_bytes: u64,
    /// The owner's word that another frame follows the one with this sequence.
    more: Receiver<u64>,
    /// Where the owner hears of a frame's flush while the device confirms it, before the frame
    /// before's callers do (`owner::Owner::flushes`).
    flushed: SyncSender<Completion>,
    /// The owner's inbox, for a word that cannot wait in `flushed`, and to wake the owner for a
    /// completion it must answer.
    inbox: SyncSender<Message<F>>,
    /// Where the carrier goes back to the owner, and where each job back is told by its
    /// sequence, for a watch (`serve`).
    returns: SyncSender<Box<Carrier<F>>>,
    tokens: SyncSender<u64>,
}

/// Where a device's jobs go back to the owner (`Device::returns`, `Device::tokens`).
pub(crate) struct Returns<F> {
    pub(crate) returns: SyncSender<Box<Carrier<F>>>,
    pub(crate) tokens: SyncSender<u64>,
}

impl<F: BlockFile> Device<F> {
    pub(crate) fn new(
        file: F,
        pool: Pool,
        segment_bytes: u64,
        more: Receiver<u64>,
        flushed: SyncSender<Completion>,
        inbox: SyncSender<Message<F>>,
        back: Returns<F>,
    ) -> Self {
        Self {
            file,
            pool,
            segment_bytes,
            more,
            flushed,
            inbox,
            returns: back.returns,
            tokens: back.tokens,
        }
    }

    /// The file, the device done with.
    pub(crate) fn into_file(self) -> F {
        self.file
    }

    /// Does `job`: the completion for the owner, with `answers` holding the answers to give once
    /// the owner's inbox holds it.
    pub(crate) fn execute(&mut self, job: Job<F>, answers: &mut Vec<Answering>) -> Completion {
        match job {
            Job::Frame(frame) => self.frame(frame, answers),
            Job::Confirm {
                mut record,
                record_at,
                mut these,
            } => {
                let result = guarded(|| self.flushed(&mut record, record_at));
                if result.is_ok() {
                    answers.append(&mut these);
                }
                Completion::Confirm {
                    record,
                    result,
                    these,
                }
            }
            Job::Sweep {
                segment,
                offset,
                end,
            } => Completion::Sweep(guarded(|| self.sweep(segment, offset, end))),
            Job::Read(reads) => {
                let group = reads.group;
                let read =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.read(reads)))
                        .unwrap_or_else(|_| Reads {
                            group,
                            result: Err(unwound()),
                            ..Reads::none()
                        });
                Completion::Read(read)
            }
            Job::Look(look) => {
                let file = &self.file;
                // A look that unwinds is its caller's: the caller hears the log closed.
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| look(file)));
                Completion::Looked
            }
        }
    }

    /// A frame, its record and one flush; the frame before answered; this one confirmed and
    /// answered unless another frame follows.
    #[allow(
        clippy::disallowed_methods,
        reason = "hyper-log owns its device and times its flushes (CLAUDE.md §1, sans-io's one exception)"
    )]
    fn frame(&mut self, mut f: Frame, answers: &mut Vec<Answering>) -> Completion {
        let started = Instant::now();
        let result = guarded(|| {
            self.write(&mut f.frame, f.at)?;
            self.flushed(&mut f.record, f.record_at)
        });
        let took_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let confirming = result.is_ok() && !(f.more || self.follows(f.sequence));
        if result.is_ok() {
            // This frame's record confirms the frame before.
            answers.append(&mut f.before);
        }
        if !confirming {
            return Completion::Frame {
                frame: f.frame,
                record: f.record,
                confirm: f.confirm,
                result,
                took_ns,
                confirming,
                before: f.before,
                these: f.these,
            };
        }
        // The owner publishes the frame, and gives back the frame before's room, while the
        // confirmation is written; the frame before's callers are answered then.
        let flushed = Completion::Frame {
            frame: f.frame,
            record: f.record,
            confirm: AlignedBuf::empty(),
            result,
            took_ns,
            confirming,
            before: f.before,
            these: Vec::new(),
        };
        // The owner reads the word before any message a caller sends on hearing its answer. It
        // has read the last frame's word before it hands over the next frame, so the slot is free;
        // should it not be, the word goes through the inbox.
        let heard = match self.flushed.try_send(flushed) {
            Ok(()) => true,
            Err(TrySendError::Full(word) | TrySendError::Disconnected(word)) => {
                self.inbox.send(Message::Done(word)).is_ok()
            }
        };
        if heard {
            for answering in answers.drain(..) {
                answering.durable();
            }
        }
        let result = guarded(|| self.flushed(&mut f.confirm, f.record_at));
        if result.is_ok() {
            answers.append(&mut f.these);
        }
        Completion::Confirm {
            record: f.confirm,
            result,
            these: f.these,
        }
    }

    /// Whether the owner has said another frame follows the one of `sequence`; older words are
    /// dropped.
    fn follows(&self, sequence: u64) -> bool {
        let mut follows = false;
        loop {
            match self.more.try_recv() {
                Ok(s) => follows |= s == sequence,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return follows,
            }
        }
    }

    /// Writes `buf` at `at` and flushes the file.
    fn flushed(&self, buf: &mut AlignedBuf, at: u64) -> Result<(), LogError> {
        self.write(buf, at)?;
        self.file.sync_data().map_err(LogError::from)
    }

    /// Writes `buf`, padded with zeros to the file's alignment, at `at`.
    fn write(&self, buf: &mut AlignedBuf, at: u64) -> Result<(), LogError> {
        let bytes = buf.padded().map_err(|e| LogError::Disk(e.into()))?;
        self.file.write_all_at(bytes, at)?;
        Ok(())
    }

    /// The verified frames of `segment` from `offset` to its end `end`, read through a window of
    /// a segment: nothing writes the tail while it is swept.
    fn sweep(&self, segment: Segment, offset: u64, end: u64) -> Result<Vec<Swept>, LogError> {
        let mut reader = Reader::new(&self.file, self.file.alignment(), self.segment_bytes)?;
        let mut out = Vec::new();
        let mut offset = offset;
        loop {
            let (header, bytes, padded) = match reader.frame_at(segment, offset, end)? {
                Found::Frame(header, bytes, padded) => (header, bytes, padded),
                Found::End => break,
                Found::Invalid => {
                    return Err(LogError::Damaged("a live frame does not verify"));
                }
            };
            let body = bytes
                .get(FRAME_HEADER_LEN..header.frame_len().unwrap_or(0))
                .ok_or(LogError::Damaged("a frame shorter than its header says"))?;
            let records = format::records(body, header.records)
                .ok_or(LogError::Damaged("a verified frame does not decode"))?;
            let base = offset
                .checked_add(FRAME_HEADER_BYTES)
                .ok_or(LogError::Damaged("an offset past u64"))?;
            out.push(Swept { base, records });
            offset = offset
                .checked_add(padded)
                .ok_or(LogError::Damaged("an offset past u64"))?;
        }
        Ok(out)
    }

    /// Reads each run in one read, and copies each entry that verifies as the one asked for
    /// into the caller's reservation.
    fn read(&mut self, mut reads: Reads) -> Reads {
        let runs = std::mem::take(&mut reads.runs);
        let wanted = std::mem::take(&mut reads.wanted);
        for run in &runs {
            let entries = run
                .first
                .checked_add(run.count)
                .and_then(|end| wanted.get(run.first..end))
                .unwrap_or_default();
            if let Err(e) = self.read_run(&mut reads, run, entries) {
                reads.result = Err(e);
                break;
            }
        }
        reads.runs = runs;
        reads.wanted = wanted;
        reads
    }

    fn read_run(
        &mut self,
        reads: &mut Reads,
        run: &Run,
        entries: &[Wanted],
    ) -> Result<(), LogError> {
        let size = usize::try_from(run.end.saturating_sub(run.begin))
            .map_err(|_| LogError::Damaged("a read past usize"))?;
        let mut buf = self.pool.take(size).map_err(|e| LogError::Disk(e.into()))?;
        let read = buf
            .set_len(size)
            .map_err(|e| LogError::Disk(e.into()))
            .and_then(|()| {
                self.file
                    .read_exact_at(buf.as_mut_slice(), run.begin)
                    .map_err(LogError::from)
            });
        if read.is_ok() {
            for wanted in entries {
                take_entry(reads, buf.as_slice(), run.begin, *wanted);
            }
        }
        self.pool.give(buf);
        read
    }
}

/// Copies the entry `wanted` from `bytes`, which hold the file from `begin`, into the caller's
/// reservation if it verifies as the group's entry of that index and term; a miss otherwise.
fn take_entry(reads: &mut Reads, bytes: &[u8], begin: u64, wanted: Wanted) {
    let found = usize::try_from(wanted.offset.saturating_sub(begin))
        .ok()
        .and_then(|skip| bytes.get(skip..))
        .and_then(|at| format::entry_slice(at, reads.group, wanted.index))
        .filter(|(term, _)| *term == wanted.term);
    match found {
        Some((_, payload)) => reads.into.fill(wanted.at, payload),
        None => reads.missed.push(wanted),
    }
}

/// The block-aligned span of the file holding an entry of `len` payload bytes at `offset`.
pub(crate) fn span(align: Alignment, offset: u64, len: u32) -> Option<(u64, u64)> {
    let end = offset
        .checked_add(format::ENTRY_HEADER_BYTES)?
        .checked_add(u64::from(len))
        .and_then(|e| align.up_u64(e))?;
    Some((align.down_u64(offset), end))
}
