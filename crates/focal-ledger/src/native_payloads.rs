//! A session replica's payload log on its volume (doc 31 M3): the bytes of the rows its core
//! spills, in segment files under the session's directory, appended without a sync and read by
//! position. It is a cache of what the replica's records and checkpoints hold, so it is emptied
//! whenever the replica opens and refilled as they are applied; every replica keeps its own.
//!
//! A segment is charged to the volume's disk budget, in the ordinary lane alone, before it is
//! made: a log that cannot have its next segment refuses the payload, and the row keeps its
//! bytes in memory, so the log never takes the completion reserve or the headroom. A sealed
//! segment no row references any longer is removed ([`FilePayloads::sweep`]). A reader handed
//! to another thread holds its own handles, so removing a segment never breaks a read it began.
//!
//! The newest payloads are held in memory, the log's file written behind them by a thread of its
//! own, as Kafka serves a log's tail from memory and writes it behind: the thread that appends (a
//! session's owner) never waits for a write. It still waits on the volume twice: a read older than
//! the window reads the file on the reading thread, under the log's lock, and the append that needs
//! a new segment makes its file. Appends are copied into a window of chunks, each allocated,
//! charged to the session's memory budget and touched once, when the window first needs it, and
//! reused after; the window grows so to an eighth of the session's memory. A chunk is copied into
//! one of a few I/O buffers and handed to the log's writer as it fills, and is let go (to hold the
//! next payloads) once its bytes are in the file, the oldest first once the window can grow no
//! more. A read of a payload the window holds is a copy; only an older one reads the file. A window
//! whose every chunk still waits for the writer refuses the next payload (the row keeps its bytes)
//! rather than wait. A reader for another thread reads the files, behind a barrier the writer
//! answers once every byte appended before it is written: the reader waits on its own thread, never
//! the appender.

use focal_core::native::payloads::{PayloadError, PayloadLog, PayloadRead, PayloadRef};
use focal_memory::{
    Allocation, BudgetKind, BudgetLane, DiskBudget, DiskKind, DiskReservation, MemoryBudget,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::time::Duration;

/// One segment's bytes at most: what a payload's offset addresses.
pub(crate) const SEGMENT_BYTES: u64 = 64 << 20;
/// The segments the log holds at once, sealed and current: what bounds its files, its handles and
/// its volume (16 GiB at most).
pub(crate) const MAX_SEGMENTS: usize = 256;
/// A window chunk's bytes at most: a thousand spilled rows and more, written in one call.
pub(crate) const CHUNK_BYTES: usize = 1 << 20;
/// A window chunk's bytes at least, the segment's size permitting: sixty-four spilled rows.
const MIN_CHUNK_BYTES: usize = 64 << 10;
/// The window's chunks at most, whatever the session's memory: the newest 4 GiB of payloads.
pub(crate) const WINDOW_CHUNKS: usize = 4096;
/// The window takes at most this share of the session's memory: an eighth.
const WINDOW_SHARE: usize = 8;
/// The I/O buffers a window's chunks are copied into for the writer: what the writer has in hand
/// at once, so what its queue holds.
const IO_BUFFERS: usize = 2;
/// The barriers readers may have waiting at the writer at once.
const BARRIERS: usize = 4;
/// How long a reader waits for its barrier before it refuses to read.
const BARRIER_WAIT: Duration = Duration::from_secs(30);
/// How long a log let go waits for its writer to finish what it holds before letting it go.
const CLOSE_WAIT: Duration = Duration::from_secs(30);

/// Where a session keeps its payload log, and the volume it is charged to.
#[derive(Clone)]
pub struct PayloadSource {
    pub root: PathBuf,
    pub disk: DiskBudget,
}

impl std::fmt::Debug for PayloadSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadSource")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

struct Segment {
    file: File,
    /// The volume it holds: outstanding while it fills, committed once it is sealed.
    disk: Option<DiskReservation>,
    /// The bytes appended to it, once it is sealed: the current segment's are the log's tail.
    len: u64,
}

/// Some of the newest payloads, in memory: where in the log they are, and what of them the
/// writer has been handed and has written.
struct Chunk {
    segment: u32,
    /// The segment offset of its first byte.
    start: u64,
    /// Its payloads, within the capacity allocated when the log opened.
    bytes: Vec<u8>,
    /// How many of its bytes the writer was handed, and how many it wrote.
    sent: usize,
    written: usize,
}

impl Chunk {
    /// Where `len` bytes at `offset` in `segment` are in it, if it holds them.
    fn holds(&self, segment: u32, offset: u64, len: usize) -> Option<std::ops::Range<usize>> {
        if segment != self.segment {
            return None;
        }
        let from = usize::try_from(offset.checked_sub(self.start)?).ok()?;
        let to = from.checked_add(len)?;
        (to <= self.bytes.len()).then_some(from..to)
    }
    /// The segment offset past its last byte.
    fn end(&self) -> Option<u64> {
        self.start
            .checked_add(u64::try_from(self.bytes.len()).ok()?)
    }
}

/// An I/O buffer and its charge: they go together, to the writer and back, so a buffer is charged
/// for as long as anything holds it, the writer included after the log has let go of it.
struct Funded {
    bytes: Vec<u8>,
    _charge: Allocation,
}

/// What the log's writer is handed, in order.
enum Job {
    /// `buffer`'s bytes written at `offset` in segment `segment`, through a handle of its own.
    Write {
        segment: u32,
        offset: u64,
        file: File,
        buffer: Funded,
        /// A buffer made for this write beyond the pool (a reader's barrier must follow every
        /// byte): let go, with its charge, when it comes back.
        extra: bool,
    },
    /// Answered once every job before it is done, with how many writes have failed since the
    /// log opened: a reader whose writes were not all in the files refuses to read.
    Barrier(SyncSender<u64>),
}

/// What the writer gives back: the I/O buffer it was handed, and whether its bytes are in the
/// file.
struct Written {
    segment: u32,
    offset: u64,
    len: usize,
    buffer: Funded,
    ok: bool,
    extra: bool,
}

/// Where a test holds a log's writer before each write; nothing outside tests.
#[cfg(test)]
type WriteGate = Option<Receiver<()>>;
#[cfg(not(test))]
struct WriteGate;

/// A gate that holds no write.
#[cfg(test)]
fn no_gate() -> WriteGate {
    None
}
#[cfg(not(test))]
fn no_gate() -> WriteGate {
    WriteGate
}

/// The writer waits at `gate` until a test lets it on (or lets go of the gate).
#[cfg(test)]
fn pass(gate: &WriteGate) {
    if let Some(gate) = gate {
        let _ = gate.recv();
    }
}
#[cfg(not(test))]
fn pass(_: &WriteGate) {}

/// The writer's loop: each write done in order, its buffer given back; each barrier answered once
/// every write before it is done. It ends when the log lets go of its queue, or of what the writer
/// gives back.
fn write_behind(jobs: &Receiver<Job>, returned: &SyncSender<Written>, gate: &WriteGate) {
    let mut failures = 0u64;
    while let Ok(job) = jobs.recv() {
        match job {
            Job::Write {
                segment,
                offset,
                file,
                buffer,
                extra,
            } => {
                pass(gate);
                let ok = write_at(&file, &buffer.bytes, offset).is_ok();
                if !ok {
                    failures = failures.saturating_add(1);
                }
                let len = buffer.bytes.len();
                let written = Written {
                    segment,
                    offset,
                    len,
                    buffer,
                    ok,
                    extra,
                };
                if returned.send(written).is_err() {
                    return;
                }
            }
            Job::Barrier(reply) => {
                // A reader that stopped waiting has nothing to hear.
                let _ = reply.try_send(failures);
            }
        }
    }
}

struct Writer {
    segments: BTreeMap<u32, Segment>,
    /// The segment appends go to, and how many bytes were appended to it.
    current: u32,
    tail: u64,
    /// The newest payloads, oldest first; appends go to the last.
    window: VecDeque<Chunk>,
    /// Chunk buffers no chunk holds, and I/O buffers the writer does not, each with its charge.
    spare: Vec<Vec<u8>>,
    buffers: Vec<Funded>,
    /// The writer's queue and what it gives back (none of either once the log closes).
    jobs: Option<SyncSender<Job>>,
    returned: Option<Receiver<Written>>,
    /// The chunks the window may grow to, the size of each, and the charge for every chunk it
    /// allocated (each I/O buffer carries its own, the writer's queue slots are its thread's).
    max_chunks: usize,
    chunk_bytes: usize,
    charge: Allocation,
    /// The writes the writer has reported failed: what a reader's barrier is checked against.
    failures_seen: u64,
    /// The window's first chunk that may hold bytes the writer was not handed, never past the
    /// chunk appends go to: where a hand begins, so an append does not walk the window.
    unsent_from: usize,
}

impl Writer {
    /// One more chunk for the window, its charge admitted first, touched once: none when the
    /// budget or the allocator refuses it.
    fn grow(&mut self) -> Option<Vec<u8>> {
        self.charge.grow(self.chunk_bytes).ok()?;
        match touched(self.chunk_bytes) {
            Ok(bytes) => Some(bytes),
            Err(_) => {
                let held = self.charge.bytes().saturating_sub(self.chunk_bytes);
                // Given back exactly what was admitted for it.
                let _ = self.charge.shrink_to(held);
                None
            }
        }
    }
    /// What the writer has given back, taken in: each chunk's bytes it wrote, as far as they are
    /// written without a hole (a write after a failed one does not count, as the failed range is
    /// handed again whole), or handed again where it failed; its buffers back to the pool, or an
    /// extra one let go with its charge.
    fn settle(&mut self) {
        while let Some(done) = self
            .returned
            .as_ref()
            .and_then(|returned| returned.try_recv().ok())
        {
            if !done.ok {
                self.failures_seen = self.failures_seen.saturating_add(1);
            }
            let found = chunk_at(&self.window, done.segment, done.offset)
                .and_then(|index| self.window.get_mut(index).map(|chunk| (index, chunk)));
            if let Some((index, chunk)) = found
                && let Some(from) = done
                    .offset
                    .checked_sub(chunk.start)
                    .and_then(|from| usize::try_from(from).ok())
            {
                if done.ok {
                    if from == chunk.written {
                        chunk.written = from.saturating_add(done.len);
                    }
                } else {
                    // Not in the file: those bytes, and every one after them, are handed again.
                    chunk.sent = chunk.sent.min(from);
                    self.unsent_from = self.unsent_from.min(index);
                }
            }
            // A buffer made beyond the pool goes, its charge with it; a pooled one comes back.
            let mut buffer = done.buffer;
            if !done.extra && self.buffers.len() < IO_BUFFERS {
                buffer.bytes.clear();
                self.buffers.push(buffer);
            }
        }
    }

    /// Hand the writer what of each chunk it has not been handed, oldest first from the first
    /// chunk that may hold any, while it has a buffer and room in its queue; the chunk appends go
    /// to only when `all`. With `extra`, a buffer is made, its charge admitted first, for what
    /// the pooled ones cannot take (a reader's barrier must follow every byte). Whether every
    /// byte of the chunks it was to hand is handed.
    fn hand(&mut self, all: bool, extra: bool) -> bool {
        let Self {
            segments,
            window,
            buffers,
            jobs,
            charge,
            unsent_from,
            ..
        } = self;
        let Some(jobs) = jobs.as_ref() else {
            return false;
        };
        let count = window.len();
        let mut index = (*unsent_from).min(count);
        let mut handed = true;
        while let Some(chunk) = window.get_mut(index) {
            if !all && index.saturating_add(1) == count {
                break;
            }
            let Some(rest) = chunk.bytes.get(chunk.sent..) else {
                handed = false;
                break;
            };
            if rest.is_empty() {
                index = index.saturating_add(1);
                continue;
            }
            // A pooled buffer holds a chunk; one made beyond the pool, what it is made for, its
            // charge admitted before it is allocated.
            let (mut buffer, made) = match buffers.pop() {
                Some(buffer) => (buffer, false),
                None if extra => match funded(charge, rest.len()) {
                    Some(buffer) => (buffer, true),
                    None => {
                        handed = false;
                        break;
                    }
                },
                None => {
                    handed = false;
                    break;
                }
            };
            // A buffer not handed: a pooled one back to the pool, a made one let go with its
            // charge.
            let give_back = |mut buffer: Funded, buffers: &mut Vec<Funded>| {
                if !made {
                    buffer.bytes.clear();
                    buffers.push(buffer);
                }
            };
            let Some(file) = segments
                .get(&chunk.segment)
                .and_then(|segment| segment.file.try_clone().ok())
            else {
                give_back(buffer, buffers);
                handed = false;
                break;
            };
            let Some(offset) = u64::try_from(chunk.sent)
                .ok()
                .and_then(|sent| chunk.start.checked_add(sent))
            else {
                give_back(buffer, buffers);
                handed = false;
                break;
            };
            // Copied within its capacity, never grown: a pooled buffer holds a chunk, a made one
            // exactly this.
            if rest.len() > buffer.bytes.capacity() {
                give_back(buffer, buffers);
                handed = false;
                break;
            }
            buffer.bytes.clear();
            buffer.bytes.extend_from_slice(rest);
            match jobs.try_send(Job::Write {
                segment: chunk.segment,
                offset,
                file,
                buffer,
                extra: made,
            }) {
                Ok(()) => {
                    chunk.sent = chunk.bytes.len();
                    index = index.saturating_add(1);
                }
                Err(TrySendError::Full(Job::Write { buffer, .. }))
                | Err(TrySendError::Disconnected(Job::Write { buffer, .. })) => {
                    give_back(buffer, buffers);
                    handed = false;
                    break;
                }
                Err(_) => {
                    handed = false;
                    break;
                }
            }
        }
        // The chunk appends go to takes more bytes after any hand: the next hand looks at it
        // again.
        *unsent_from = index.min(count.saturating_sub(1));
        handed
    }

    /// A chunk for the payloads from `start` in `segment` on: a spare buffer, a new one while
    /// the window may grow and its charge is admitted, or the oldest chunk's once all its bytes
    /// are in the file. None while the writer still holds every chunk's bytes: the payload is
    /// refused rather than waited for.
    fn open_chunk(&mut self, segment: u32, start: u64) -> Option<()> {
        let grown = if self.spare.is_empty() && self.window.len() < self.max_chunks {
            self.grow()
        } else {
            None
        };
        let bytes = match self.spare.pop().or(grown) {
            Some(bytes) => bytes,
            None => {
                let oldest = self.window.front()?;
                if oldest.written < oldest.bytes.len() {
                    return None;
                }
                let mut bytes = self.window.pop_front()?.bytes;
                self.unsent_from = self.unsent_from.saturating_sub(1);
                bytes.clear();
                bytes
            }
        };
        self.window.push_back(Chunk {
            segment,
            start,
            bytes,
            sent: 0,
            written: 0,
        });
        Some(())
    }
}

/// A replica's payload log in segment files (see the module).
pub struct FilePayloads {
    root: PathBuf,
    disk: DiskBudget,
    /// A segment's bytes at most ([`SEGMENT_BYTES`] but in tests), and a chunk's.
    segment_bytes: u64,
    chunk_bytes: usize,
    writer: Mutex<Writer>,
    /// The writer's thread, joined when the log closes, and how long the close waits for it.
    thread: Option<std::thread::JoinHandle<()>>,
    close_wait: Duration,
}

impl std::fmt::Debug for FilePayloads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePayloads")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl Drop for FilePayloads {
    fn drop(&mut self) {
        // The queue let go, even past a panic that poisoned the lock, and the writer ends once it
        // has done what it holds. What it gives back meanwhile is taken, so it never waits on a
        // send no one takes; the taking ends when it ends, as its end of the channel closes.
        let writer = match self.writer.get_mut() {
            Ok(writer) => writer,
            Err(poisoned) => poisoned.into_inner(),
        };
        writer.jobs = None;
        let thread = self.thread.take();
        let Some(returned) = writer.returned.as_ref() else {
            return;
        };
        let Some(deadline) = std::time::Instant::now().checked_add(self.close_wait) else {
            return;
        };
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            match returned.recv_timeout(left) {
                Ok(_) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    // Both queues let go before the thread that is charged for them ends.
                    writer.returned = None;
                    if let Some(thread) = thread {
                        let _ = thread.join();
                    }
                    return;
                }
                // A write the volume has not returned from by the deadline: the writer is let
                // go, and ends when it does, its next send finding no one to take it. What it
                // still holds (the buffers handed to it, its queues) stays charged until then:
                // each buffer carries its charge, and the queues' is its thread's.
                Err(RecvTimeoutError::Timeout) => return,
            }
        }
    }
}

/// Where in `window` the chunk holding `offset` in `segment` stands, if one does: its chunks stand
/// in log order (each opened after the last, removals keeping the order), so a search finds it,
/// never a walk of the window.
fn chunk_at(window: &VecDeque<Chunk>, segment: u32, offset: u64) -> Option<usize> {
    let after = window.partition_point(|chunk| (chunk.segment, chunk.start) <= (segment, offset));
    let index = after.checked_sub(1)?;
    let chunk = window.get(index)?;
    (chunk.segment == segment && chunk.end().is_some_and(|end| offset < end)).then_some(index)
}

/// A buffer for `len` bytes made beyond the pool, with a charge of its own from `charge`'s budget,
/// admitted before it is allocated (and for whatever more the allocator gave): none where either
/// is refused, nothing held.
fn funded(charge: &mut Allocation, len: usize) -> Option<Funded> {
    let mut own = charge.split_off(0).ok()?;
    own.grow(len).ok()?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len).ok()?;
    let more = bytes.capacity().saturating_sub(len);
    if more > 0 {
        own.grow(more).ok()?;
    }
    Some(Funded {
        bytes,
        _charge: own,
    })
}

/// A buffer of `bytes` capacity, every page of it touched once: what the window and the writer
/// reuse, never allocated or faulted in again.
fn touched(bytes: usize) -> std::io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(bytes)
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
    buffer.resize(bytes, 0);
    buffer.clear();
    Ok(buffer)
}

fn segment_path(root: &Path, id: u32) -> PathBuf {
    root.join(format!("{id:010}.seg"))
}

/// Read exactly `into.len()` bytes at `offset`.
fn read_at(file: &File, into: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(into, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        // Each read returns at least one byte or ends the loop: at most `into.len()` passes.
        let mut done = 0usize;
        while done < into.len() {
            let rest = into
                .get_mut(done..)
                .ok_or(std::io::ErrorKind::InvalidInput)?;
            let at = offset
                .checked_add(u64::try_from(done).map_err(|_| std::io::ErrorKind::InvalidInput)?)
                .ok_or(std::io::ErrorKind::InvalidInput)?;
            match file.seek_read(rest, at)? {
                0 => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                read => {
                    done = done
                        .checked_add(read)
                        .ok_or(std::io::ErrorKind::InvalidData)?
                }
            }
        }
        Ok(())
    }
}

/// Write all of `bytes` at `offset`.
fn write_at(file: &File, bytes: &[u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(bytes, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        // Each write takes at least one byte or ends the loop: at most `bytes.len()` passes.
        let mut done = 0usize;
        while done < bytes.len() {
            let rest = bytes.get(done..).ok_or(std::io::ErrorKind::InvalidInput)?;
            let at = offset
                .checked_add(u64::try_from(done).map_err(|_| std::io::ErrorKind::InvalidInput)?)
                .ok_or(std::io::ErrorKind::InvalidInput)?;
            match file.seek_write(rest, at)? {
                0 => return Err(std::io::ErrorKind::WriteZero.into()),
                written => {
                    done = done
                        .checked_add(written)
                        .ok_or(std::io::ErrorKind::InvalidData)?
                }
            }
        }
        Ok(())
    }
}

impl FilePayloads {
    /// The log at `source.root`, emptied (see the module), its first segment made and charged to
    /// the volume, its window to `memory`.
    pub fn open(source: &PayloadSource, memory: &MemoryBudget) -> std::io::Result<Self> {
        Self::open_with(source, memory, SEGMENT_BYTES)
    }
    /// As [`Self::open`], segments of `segment_bytes` at most.
    pub(crate) fn open_with(
        source: &PayloadSource,
        memory: &MemoryBudget,
        segment_bytes: u64,
    ) -> std::io::Result<Self> {
        Self::open_gated(source, memory, segment_bytes, no_gate())
    }
    /// As [`Self::open_with`], its writer held at `gate` before each write (in tests).
    fn open_gated(
        source: &PayloadSource,
        memory: &MemoryBudget,
        segment_bytes: u64,
        gate: WriteGate,
    ) -> std::io::Result<Self> {
        if segment_bytes == 0 || segment_bytes > SEGMENT_BYTES {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        // The window takes an eighth of the session's memory at most: chunks as large as that
        // holds four of (two to fill and write, two I/O buffers), from MIN_CHUNK_BYTES to
        // CHUNK_BYTES and no larger than a segment, and as many of them as it holds beside the
        // I/O buffers, two at least, WINDOW_CHUNKS at most. Two are made now; the rest as the
        // window first needs them.
        let share = memory.limit().checked_div(WINDOW_SHARE).unwrap_or(0);
        let chunk_bytes = share
            .checked_div(IO_BUFFERS.saturating_add(2))
            .unwrap_or(0)
            .clamp(MIN_CHUNK_BYTES, CHUNK_BYTES)
            .min(usize::try_from(segment_bytes).unwrap_or(CHUNK_BYTES));
        let chunks = share
            .checked_div(chunk_bytes)
            .unwrap_or(0)
            .saturating_sub(IO_BUFFERS)
            .clamp(2, WINDOW_CHUNKS);
        const FIRST: usize = 2;
        // The queue holds each chunk's bytes at most twice (a part handed early for a reader or
        // a seal, then the rest) and the readers' barriers; what comes back, as much. Each slot
        // of each is charged with the buffers.
        let depth = chunks
            .checked_mul(2)
            .and_then(|jobs| jobs.checked_add(BARRIERS))
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        let slots = depth
            .checked_mul(size_of::<Job>().saturating_add(size_of::<Written>()))
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        let held = FIRST
            .checked_add(IO_BUFFERS)
            .and_then(|buffers| buffers.checked_mul(chunk_bytes))
            .and_then(|buffers| buffers.checked_add(slots))
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        let out_of_memory = |_| std::io::Error::from(std::io::ErrorKind::OutOfMemory);
        let mut charge = memory
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, held)
            .map_err(out_of_memory)?
            .commit();
        // The queues' slots are charged to the writer's thread, each I/O buffer carries its own:
        // what the log charges is the window's chunks.
        let slots = charge.split_off(slots).map_err(out_of_memory)?;
        let mut spare = Vec::new();
        spare
            .try_reserve_exact(chunks)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        for _ in 0..FIRST {
            spare.push(touched(chunk_bytes)?);
        }
        let mut buffers = Vec::new();
        buffers
            .try_reserve_exact(IO_BUFFERS)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        for _ in 0..IO_BUFFERS {
            buffers.push(Funded {
                bytes: touched(chunk_bytes)?,
                _charge: charge.split_off(chunk_bytes).map_err(out_of_memory)?,
            });
        }
        let mut window = VecDeque::new();
        window
            .try_reserve_exact(chunks)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        match std::fs::symlink_metadata(&source.root) {
            Ok(_) => {
                // A cache: whatever an earlier run left is let go, a bounded number of files.
                let mut removed = 0usize;
                for entry in std::fs::read_dir(&source.root)? {
                    removed = removed.saturating_add(1);
                    if removed > MAX_SEGMENTS.saturating_mul(4) {
                        return Err(std::io::ErrorKind::InvalidData.into());
                    }
                    let path = entry?.path();
                    if path.extension().is_some_and(|extension| extension == "seg") {
                        std::fs::remove_file(&path)?;
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                focal_platform::fs::create_dir_all_private(&source.root)?;
            }
            Err(error) => return Err(error),
        }
        let (jobs, queue) = sync_channel(depth);
        let (back, returned) = sync_channel(depth);
        let thread = std::thread::Builder::new()
            .name("focal-payloads".into())
            .spawn(move || {
                // The queues' slots are this thread's charge: let go once it has let go of its
                // ends, after the log let go of its own.
                let slots = slots;
                write_behind(&queue, &back, &gate);
                drop(queue);
                drop(back);
                drop(slots);
            })?;
        let log = Self {
            root: source.root.clone(),
            disk: source.disk.clone(),
            segment_bytes,
            chunk_bytes,
            writer: Mutex::new(Writer {
                segments: BTreeMap::new(),
                current: 0,
                tail: 0,
                window,
                spare,
                buffers,
                jobs: Some(jobs),
                returned: Some(returned),
                max_chunks: chunks,
                chunk_bytes,
                charge,
                failures_seen: 0,
                unsent_from: 0,
            }),
            thread: Some(thread),
            close_wait: CLOSE_WAIT,
        };
        {
            let mut writer = log
                .writer
                .lock()
                .map_err(|_| std::io::Error::other("payload log writer"))?;
            let first = log.segment(0)?;
            writer.segments.insert(0, first);
        }
        Ok(log)
    }

    /// Segment `id`, made new and charged to the volume, sampled first when its envelope's
    /// cadence says so (an unsampled volume admits nothing under a watermark).
    fn segment(&self, id: u32) -> std::io::Result<Segment> {
        self.disk
            .refresh_with(|| focal_platform::available_space(&self.root));
        let disk = self
            .disk
            .reserve(DiskKind::Payload, BudgetLane::Ordinary, self.segment_bytes)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::StorageFull))?;
        let file =
            focal_platform::fs::create_private_new(&segment_path(&self.root, id), true, true)?;
        Ok(Segment {
            file,
            disk: Some(disk),
            len: 0,
        })
    }

    /// Remove every sealed segment no row references, `live` naming those that some row does:
    /// the committed rows, every pending candidate's, a restore's. The current segment stays.
    /// What the window held of a removed segment is let go with it. How many it removed.
    pub fn sweep(&self, live: &BTreeSet<u32>) -> usize {
        let Ok(mut writer) = self.writer.lock() else {
            return 0;
        };
        let current = writer.current;
        let dead: Vec<u32> = writer
            .segments
            .keys()
            .copied()
            .filter(|id| *id != current && !live.contains(id))
            .collect();
        let mut removed = 0usize;
        for id in dead {
            // A removal that fails keeps the segment, and the next sweep tries again.
            if std::fs::remove_file(segment_path(&self.root, id)).is_ok()
                && writer.segments.remove(&id).is_some()
            {
                removed = removed.saturating_add(1);
            }
        }
        // A chunk of a removed segment goes back to the spares; the writer's copy of it, if it
        // still holds one, comes back to the I/O buffers.
        let Writer {
            segments,
            window,
            spare,
            ..
        } = &mut *writer;
        window.retain_mut(|chunk| {
            if segments.contains_key(&chunk.segment) {
                return true;
            }
            let mut bytes = std::mem::take(&mut chunk.bytes);
            bytes.clear();
            spare.push(bytes);
            false
        });
        // Chunks left from anywhere: the next hand looks from the start, once.
        writer.unsent_from = 0;
        removed
    }

    /// The segments the log holds now, sealed and current.
    pub fn segments(&self) -> usize {
        self.writer.lock().map_or(0, |writer| writer.segments.len())
    }
    /// The payload bytes appended to the segments the log holds, written or in its window.
    pub fn bytes(&self) -> u64 {
        self.writer.lock().map_or(0, |writer| {
            writer.segments.iter().fold(0u64, |sum, (id, segment)| {
                sum.saturating_add(if *id == writer.current {
                    writer.tail
                } else {
                    segment.len
                })
            })
        })
    }
}

impl PayloadRead for FilePayloads {
    fn read(&self, at: PayloadRef, into: &mut [u8]) -> Result<(), PayloadError> {
        if into.len() != at.len() {
            return Err(PayloadError::Corrupt);
        }
        let mut writer = self.writer.lock().map_err(|_| PayloadError::Unavailable)?;
        writer.settle();
        let offset = u64::from(at.offset());
        let held = chunk_at(&writer.window, at.segment(), offset)
            .and_then(|index| writer.window.get(index))
            .and_then(|chunk| {
                chunk
                    .holds(at.segment(), offset, into.len())
                    .and_then(|range| chunk.bytes.get(range))
            });
        match held {
            // The window holds it: a copy.
            Some(bytes) => into.copy_from_slice(bytes),
            // Older than the window, so in the file: a chunk leaves the window only once its
            // bytes are written.
            None => {
                let segment = writer
                    .segments
                    .get(&at.segment())
                    .ok_or(PayloadError::Unavailable)?;
                read_at(&segment.file, into, offset).map_err(|_| PayloadError::Unavailable)?;
            }
        }
        if at.holds(into) {
            Ok(())
        } else {
            Err(PayloadError::Corrupt)
        }
    }
}

impl PayloadLog for FilePayloads {
    fn append(&self, bytes: &[u8]) -> Option<PayloadRef> {
        let len = u64::try_from(bytes.len()).ok()?;
        if len > self.segment_bytes || bytes.len() > self.chunk_bytes {
            return None;
        }
        let mut writer = self.writer.lock().ok()?;
        writer.settle();
        // The chunks filled before the one appends go to are the writer's as it takes them.
        writer.hand(false, false);
        if writer.tail.checked_add(len)? > self.segment_bytes {
            if writer.segments.len() >= MAX_SEGMENTS {
                return None;
            }
            let next = writer.current.checked_add(1)?;
            let segment = self.segment(next).ok()?;
            // The sealed segment's charge is spent: its bytes are the writer's to write.
            let (current, tail) = (writer.current, writer.tail);
            if let Some(sealed) = writer.segments.get_mut(&current) {
                sealed.len = tail;
                if let Some(disk) = sealed.disk.take() {
                    disk.commit();
                }
            }
            writer.segments.insert(next, segment);
            writer.current = next;
            writer.tail = 0;
        }
        let (current, tail) = (writer.current, writer.tail);
        let fits = writer.window.back().is_some_and(|chunk| {
            chunk.segment == current
                && chunk.end() == Some(tail)
                && chunk
                    .bytes
                    .len()
                    .checked_add(bytes.len())
                    .is_some_and(|after| after <= self.chunk_bytes)
        });
        if !fits {
            // The chunk appends went to is filled: the writer's as it takes it.
            writer.hand(true, false);
            writer.open_chunk(current, tail)?;
        }
        let at = PayloadRef::at(current, u32::try_from(tail).ok()?, bytes).ok()?;
        let chunk = writer.window.back_mut()?;
        // Within the chunk's capacity: no reallocation.
        chunk.bytes.extend_from_slice(bytes);
        writer.tail = tail.checked_add(len)?;
        Some(at)
    }

    fn sweep(&self, live: &BTreeSet<u32>) -> usize {
        FilePayloads::sweep(self, live)
    }
    fn held_segments(&self) -> usize {
        self.segments()
    }
    fn held_bytes(&self) -> u64 {
        self.bytes()
    }
    fn reader(&self) -> Result<Box<dyn PayloadRead + Send>, PayloadError> {
        let mut writer = self.writer.lock().map_err(|_| PayloadError::Unavailable)?;
        writer.settle();
        // A reader on another thread reads the files alone: every byte appended is handed to
        // the writer, and the reader waits for the writer's barrier behind them, on its thread.
        if !writer.hand(true, true) {
            return Err(PayloadError::Unavailable);
        }
        // Every failure the writer reported is behind a range handed again above; one it reports
        // past this count failed after, and its bytes may not be in the files.
        let failures = writer.failures_seen;
        let (reply, barrier) = sync_channel(1);
        writer
            .jobs
            .as_ref()
            .ok_or(PayloadError::Unavailable)?
            .try_send(Job::Barrier(reply))
            .map_err(|_| PayloadError::Unavailable)?;
        let mut segments = BTreeMap::new();
        for (id, segment) in &writer.segments {
            let file = segment
                .file
                .try_clone()
                .map_err(|_| PayloadError::Unavailable)?;
            segments.insert(*id, file);
        }
        Ok(Box::new(FileReader {
            segments,
            gate: Mutex::new(Gate::Waiting(barrier, failures)),
        }))
    }
}

/// What a payload log held when the reader was made, through handles of its own, read once the
/// log's writer has written it.
#[derive(Debug)]
struct FileReader {
    segments: BTreeMap<u32, File>,
    gate: Mutex<Gate>,
}

/// Whether a reader's bytes are in the files: waiting on the writer's barrier (and the failures
/// it may report), open once it reports none past those the log had handed again, shut else.
#[derive(Debug)]
enum Gate {
    Waiting(Receiver<u64>, u64),
    Open,
    Shut,
}

impl PayloadRead for FileReader {
    fn read(&self, at: PayloadRef, into: &mut [u8]) -> Result<(), PayloadError> {
        if into.len() != at.len() {
            return Err(PayloadError::Corrupt);
        }
        {
            let mut gate = self.gate.lock().map_err(|_| PayloadError::Unavailable)?;
            let next = match &*gate {
                Gate::Open => None,
                Gate::Shut => return Err(PayloadError::Unavailable),
                Gate::Waiting(barrier, failures) => match barrier.recv_timeout(BARRIER_WAIT) {
                    Ok(reported) if reported == *failures => Some(Gate::Open),
                    // A write before the barrier failed after the log handed the reader's
                    // bytes, or the writer is gone: the files may hold a hole.
                    Ok(_) | Err(_) => Some(Gate::Shut),
                },
            };
            if let Some(next) = next {
                *gate = next;
            }
            if matches!(*gate, Gate::Shut) {
                return Err(PayloadError::Unavailable);
            }
        }
        let file = self
            .segments
            .get(&at.segment())
            .ok_or(PayloadError::Unavailable)?;
        read_at(file, into, u64::from(at.offset())).map_err(|_| PayloadError::Unavailable)?;
        if at.holds(into) {
            Ok(())
        } else {
            Err(PayloadError::Corrupt)
        }
    }
}

#[cfg(test)]
#[path = "native_payloads_tests.rs"]
mod tests;
