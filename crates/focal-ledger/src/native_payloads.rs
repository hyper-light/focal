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
//! own, as Kafka serves a log's tail from memory and writes it behind: the thread that appends
//! and reads (a session's owner) never waits on the volume. Appends are copied into a window of
//! chunks, each allocated, charged to the session's memory budget and touched once, when the
//! window first needs it, and reused after; the window grows so to an eighth of the session's
//! memory. A chunk is copied into one of a few I/O buffers and handed to the log's writer as it
//! fills, and is let go (to hold the next payloads) once its bytes are in the file, the oldest
//! first once the window can grow no more. A read of a payload the window holds is a copy; only an older one reads the file. A
//! window whose every chunk still waits for the writer refuses the next payload (the row keeps
//! its bytes) rather than wait. A reader for another thread reads the files, behind a barrier the
//! writer answers once every byte appended before it is written: the reader waits on its own
//! thread, never the appender.

use focal_core::native::payloads::{PayloadError, PayloadLog, PayloadRead, PayloadRef};
use focal_memory::{
    Allocation, BudgetKind, BudgetLane, DiskBudget, DiskKind, DiskReservation, MemoryBudget,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
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

/// What the log's writer is handed, in order.
enum Job {
    /// `bytes` written at `offset` in segment `segment`, through a handle of its own.
    Write {
        segment: u32,
        offset: u64,
        file: File,
        bytes: Vec<u8>,
    },
    /// Answered once every job before it is done.
    Barrier(SyncSender<()>),
}

/// What the writer gives back: the I/O buffer it was handed, and whether its bytes are in the
/// file.
struct Written {
    segment: u32,
    offset: u64,
    len: usize,
    bytes: Vec<u8>,
    ok: bool,
}

/// The writer's loop: each write done in order, its buffer given back; each barrier answered once
/// every write before it is done. It ends when the log lets go of its queue.
fn write_behind(jobs: &Receiver<Job>, returned: &SyncSender<Written>) {
    while let Ok(job) = jobs.recv() {
        match job {
            Job::Write {
                segment,
                offset,
                file,
                bytes,
            } => {
                let ok = write_at(&file, &bytes, offset).is_ok();
                let len = bytes.len();
                let written = Written {
                    segment,
                    offset,
                    len,
                    bytes,
                    ok,
                };
                if returned.send(written).is_err() {
                    return;
                }
            }
            Job::Barrier(reply) => {
                // A reader that stopped waiting has nothing to hear.
                let _ = reply.try_send(());
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
    /// Chunk buffers no chunk holds, and I/O buffers the writer does not.
    spare: Vec<Vec<u8>>,
    buffers: Vec<Vec<u8>>,
    /// The writer's queue (none once the log closes), and what it gives back.
    jobs: Option<SyncSender<Job>>,
    returned: Receiver<Written>,
    /// The chunks the window may grow to, the size of each, and the charge for every chunk and
    /// I/O buffer it allocated.
    max_chunks: usize,
    chunk_bytes: usize,
    charge: Allocation,
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
    /// What the writer has given back, taken in: each chunk's bytes it wrote, or handed again
    /// where it failed to, and its buffers.
    fn settle(&mut self) {
        while let Ok(done) = self.returned.try_recv() {
            let chunk = self.window.iter_mut().find(|chunk| {
                chunk.segment == done.segment
                    && done.offset >= chunk.start
                    && chunk.end().is_some_and(|end| done.offset < end)
            });
            if let Some(chunk) = chunk
                && let Some(from) = done
                    .offset
                    .checked_sub(chunk.start)
                    .and_then(|from| usize::try_from(from).ok())
            {
                if done.ok {
                    chunk.written = chunk.written.max(from.saturating_add(done.len));
                } else {
                    // Not in the file: those bytes are handed again.
                    chunk.sent = chunk.sent.min(from);
                }
            }
            let mut bytes = done.bytes;
            bytes.clear();
            if self.buffers.len() < IO_BUFFERS {
                self.buffers.push(bytes);
            }
        }
    }

    /// Hand the writer what of each chunk it has not been handed, oldest first, while it has a
    /// buffer and room in its queue; the chunk appends go to only when `all`. With `extra`, a
    /// buffer is made for what the pooled ones cannot take (a reader's barrier must follow every
    /// byte). Whether every byte of the chunks it was to hand is handed.
    fn hand(&mut self, all: bool, extra: bool) -> bool {
        let Self {
            segments,
            window,
            buffers,
            jobs,
            ..
        } = self;
        let Some(jobs) = jobs.as_ref() else {
            return false;
        };
        let count = window.len();
        let mut handed = true;
        for (index, chunk) in window.iter_mut().enumerate() {
            if !all && index.saturating_add(1) == count {
                break;
            }
            let Some(rest) = chunk.bytes.get(chunk.sent..) else {
                handed = false;
                continue;
            };
            if rest.is_empty() {
                continue;
            }
            let Some(mut buffer) = buffers.pop().or_else(|| {
                extra
                    .then(|| {
                        let mut buffer = Vec::new();
                        buffer.try_reserve_exact(rest.len()).ok().map(|()| buffer)
                    })
                    .flatten()
            }) else {
                handed = false;
                break;
            };
            let Some(file) = segments
                .get(&chunk.segment)
                .and_then(|segment| segment.file.try_clone().ok())
            else {
                buffer.clear();
                buffers.push(buffer);
                handed = false;
                continue;
            };
            buffer.clear();
            buffer.extend_from_slice(rest);
            let Some(offset) = u64::try_from(chunk.sent)
                .ok()
                .and_then(|sent| chunk.start.checked_add(sent))
            else {
                handed = false;
                continue;
            };
            match jobs.try_send(Job::Write {
                segment: chunk.segment,
                offset,
                file,
                bytes: buffer,
            }) {
                Ok(()) => chunk.sent = chunk.bytes.len(),
                Err(TrySendError::Full(Job::Write { mut bytes, .. }))
                | Err(TrySendError::Disconnected(Job::Write { mut bytes, .. })) => {
                    bytes.clear();
                    if buffers.len() < IO_BUFFERS {
                        buffers.push(bytes);
                    }
                    handed = false;
                    break;
                }
                Err(_) => {
                    handed = false;
                    break;
                }
            }
        }
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
    /// The writer's thread, joined when the log closes.
    thread: Option<std::thread::JoinHandle<()>>,
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
        // The queue let go, the writer ends once it has done what it holds.
        if let Ok(mut writer) = self.writer.lock() {
            writer.jobs = None;
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
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
        let held = FIRST
            .checked_add(IO_BUFFERS)
            .and_then(|buffers| buffers.checked_mul(chunk_bytes))
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        let charge = memory
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, held)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?
            .commit();
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
            buffers.push(touched(chunk_bytes)?);
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
        // The queue holds each chunk's bytes at most twice (a part handed early for a reader or
        // a seal, then the rest) and the readers' barriers; what comes back, as much.
        let depth = chunks
            .checked_mul(2)
            .and_then(|jobs| jobs.checked_add(BARRIERS))
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        let (jobs, queue) = sync_channel(depth);
        let (back, returned) = sync_channel(depth);
        let thread = std::thread::Builder::new()
            .name("focal-payloads".into())
            .spawn(move || write_behind(&queue, &back))?;
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
                returned,
                max_chunks: chunks,
                chunk_bytes,
                charge,
            }),
            thread: Some(thread),
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
        let held = writer.window.iter().rev().find_map(|chunk| {
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
            barrier: Mutex::new(Some(barrier)),
        }))
    }
}

/// What a payload log held when the reader was made, through handles of its own, read once the
/// log's writer has written it.
#[derive(Debug)]
struct FileReader {
    segments: BTreeMap<u32, File>,
    /// The writer's answer that every byte before the reader is written; none once heard.
    barrier: Mutex<Option<Receiver<()>>>,
}

impl PayloadRead for FileReader {
    fn read(&self, at: PayloadRef, into: &mut [u8]) -> Result<(), PayloadError> {
        if into.len() != at.len() {
            return Err(PayloadError::Corrupt);
        }
        {
            let mut barrier = self.barrier.lock().map_err(|_| PayloadError::Unavailable)?;
            if let Some(waiting) = barrier.as_ref() {
                match waiting.recv_timeout(BARRIER_WAIT) {
                    Ok(()) => *barrier = None,
                    Err(_) => return Err(PayloadError::Unavailable),
                }
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
