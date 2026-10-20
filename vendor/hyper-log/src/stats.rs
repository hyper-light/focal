//! What a log measured from its opening, for an owner's metrics (`docs/durable.md` §13.1): its
//! counts, and histograms of its flushes, its frames' writes and its updates' waits.
//!
//! The thread that does a job's I/O times it and carries the times back in the job's completion;
//! the owner, which hears every completion, counts them, and answers [`crate::Log::stats`] from
//! its counts as it answers any question, as it comes. The owner never waits on I/O, so a flush
//! that stalls delays no answer: it shows in [`LogStats::flushing_since`] instead.

use std::time::Instant;

use hyper_timing::Histogram;

/// What a log measured from its opening to [`LogStats::at`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogStats {
    /// When the owner answered.
    pub at: Instant,
    /// When the frame or confirmation the device holds went to it, while one does: a flush that
    /// stalls is seen as `at − flushing_since` before it ends, where the histograms hear of it
    /// only once it has.
    pub flushing_since: Option<Instant>,
    /// Frames written and flushed.
    pub frames: u64,
    /// Updates those frames carried.
    pub updates: u64,
    /// Bytes written: frames, their persist records and confirmations, each padded to the file's
    /// alignment.
    pub bytes: u64,
    /// Flushes made: each frame's, and each confirmation's written on its own; a failed one,
    /// which fences the log, among them.
    pub flushes: u64,
    /// Each flush's `sync_data`: the platform's full flush (`fdatasync`, `F_FULLFSYNC`,
    /// `FlushFileBuffers`).
    pub flush: Histogram,
    /// Each frame's writes, the frame and its persist record, from the first's start to the
    /// second's end.
    pub write: Histogram,
    /// Each update's wait, from its submission to the end of the flush that let it be answered:
    /// the next frame's, whose record confirms its frame, or its frame's confirmation's.
    pub commit_wait: Histogram,
    /// Writes the device made durable on their own, each without a flush of its cache: a frame's
    /// confirmation, written with `RWF_DSYNC` on a direct file on Linux (a FUA write where the
    /// device has FUA). Where the write falls back to a write and a flush, it counts in `flushes`.
    pub durable_writes: u64,
    /// Durable writes the file made with a write and a flush of its cache instead: on a file that
    /// is not direct, off Linux, or on a kernel that takes no `RWF_DSYNC`. Each also counts in
    /// `flushes`. (Where the device has no FUA, the kernel's own fallback is invisible here: such a
    /// write counts in `durable_writes`, and only the device's counters show its flush.)
    pub durable_fallbacks: u64,
    /// Confirmations written and flushed where the file could have written them durable on their
    /// own: the device measured a write and a flush of its cache cheaper than its durable write (a
    /// consumer NVMe drive's FUA write costs more than a flush). Each also counts in `flushes`.
    pub flushed_confirmations: u64,
    /// Bytes of zeros written over the slots the file grew by, ahead of their first frames while
    /// the device had nothing else to do (`docs/durable.md` §6.3), counted in `bytes` too: what
    /// growing the log wrote that no record holds.
    pub zeroed_bytes: u64,
    /// Slots written whole with zeros and flushed ahead of their first frame: every frame in one
    /// is an overwrite.
    pub fills: u64,
    /// The pieces those fills were written in, each a write no larger than the frame before it.
    pub fill_pieces: u64,
    /// Fills ahead whose write or flush failed: their slots are written by their frames as any
    /// unwritten space is, and nothing is fenced.
    pub fill_failures: u64,
    /// Asks for a slot past the file's end the owner refused (`crate::Growth`), each read as the
    /// file's bound reached: the frame that needed it answered `Full`. It counts asks, not
    /// episodes: the log asks again whenever it looks for room, so one bound reached counts as
    /// often as the log looked while it held.
    pub growth_refused: u64,
    /// Sweeps made: frames that copied the tail segment's live entries ahead of their updates,
    /// so the tail could be let go when the log had fewer than two segments free.
    pub sweeps: u64,
    /// Bytes those sweeps copied, the relocated records laid before the frames' updates: what the
    /// log wrote a second time, its write amplification's own part.
    pub swept_bytes: u64,
}

impl LogStats {
    /// Nothing measured, at `at`.
    pub(crate) fn new(at: Instant) -> Self {
        Self {
            at,
            flushing_since: None,
            frames: 0,
            updates: 0,
            bytes: 0,
            flushes: 0,
            durable_writes: 0,
            durable_fallbacks: 0,
            flushed_confirmations: 0,
            zeroed_bytes: 0,
            fills: 0,
            fill_pieces: 0,
            fill_failures: 0,
            flush: Histogram::new(),
            write: Histogram::new(),
            commit_wait: Histogram::new(),
            growth_refused: 0,
            sweeps: 0,
            swept_bytes: 0,
        }
    }
}

/// Now, for the times the log keeps: when an update was submitted, when a frame went to the
/// device, and its writes' and flush's starts and ends.
#[allow(
    clippy::disallowed_methods,
    reason = "hyper-log times its I/O and its updates' waits (docs/durable.md §13.1; sans-io's one exception)"
)]
pub(crate) fn now() -> Instant {
    Instant::now()
}

/// What one job's I/O took, timed on the thread that did it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Timing {
    /// The job's service time, its writes and its flush, nanoseconds.
    pub(crate) took_ns: u64,
    /// The writes', nanoseconds: a frame's and its record's, or a confirmation's.
    pub(crate) write_ns: u64,
    /// The flush's, nanoseconds; none when a write failed before it.
    pub(crate) flush_ns: Option<u64>,
    /// Bytes written, padding included.
    pub(crate) bytes: u64,
    /// When the flush ended, if it held: the moment the updates it lets be answered waited until.
    pub(crate) flushed_at: Option<Instant>,
    /// The job's durability came from one write made durable on its own (a FUA write,
    /// `BlockFile::write_durable_at`), not from a flush of the device's cache.
    pub(crate) durable_write: bool,
    /// The job asked for a durable write and the file made it with a write and a flush instead.
    pub(crate) durable_fallback: bool,
    /// A confirmation written and flushed, the device having measured that cheaper than a write
    /// made durable on its own (`device::Device::confirm`).
    pub(crate) flushed_confirmation: bool,
    /// Bytes of zeros written over a slot the file grew by, before its first frame (`zero_fill`);
    /// counted in `bytes` too.
    pub(crate) zeroed: u64,
}

/// The nanoseconds from `from` to `to`, saturating.
pub(crate) fn nanos(from: Instant, to: Instant) -> u64 {
    u64::try_from(to.saturating_duration_since(from).as_nanos()).unwrap_or(u64::MAX)
}

/// The owner's counts, kept in one allocation made at open so that the jobs and the owner's
/// steps move no histogram.
pub(crate) struct Tally {
    pub(crate) bytes: u64,
    pub(crate) flushes: u64,
    /// Writes made durable on their own, no flush with them (`Timing::durable_write`).
    pub(crate) durable_writes: u64,
    /// Durable writes made with a write and a flush instead (`Timing::durable_fallback`).
    pub(crate) durable_fallbacks: u64,
    /// Confirmations written and flushed by the device's choice (`Timing::flushed_confirmation`).
    pub(crate) flushed_confirmations: u64,
    /// Bytes of zeros written ahead of the frames of the slots the file grew by.
    pub(crate) zeroed_bytes: u64,
    /// Slots filled ahead whole, the pieces written, and fills ahead that failed.
    pub(crate) fills: u64,
    pub(crate) fill_pieces: u64,
    pub(crate) fill_failures: u64,
    pub(crate) flush: Histogram,
    pub(crate) write: Histogram,
    pub(crate) commit_wait: Histogram,
    /// Sweeps laid, and the bytes of the records they relocated.
    pub(crate) sweeps: u64,
    pub(crate) swept_bytes: u64,
}

impl Tally {
    pub(crate) fn new() -> Box<Self> {
        Box::new(Self {
            bytes: 0,
            flushes: 0,
            durable_writes: 0,
            durable_fallbacks: 0,
            flushed_confirmations: 0,
            zeroed_bytes: 0,
            fills: 0,
            fill_pieces: 0,
            fill_failures: 0,
            flush: Histogram::new(),
            write: Histogram::new(),
            commit_wait: Histogram::new(),
            sweeps: 0,
            swept_bytes: 0,
        })
    }

    /// One job's I/O: its bytes and writes, and its flush once it completed. `frame` says whether
    /// the writes were a frame's, which the write histogram counts, or a confirmation's.
    pub(crate) fn job(&mut self, timing: &Timing, frame: bool) {
        self.bytes = self.bytes.saturating_add(timing.bytes);
        if frame {
            self.write.record(timing.write_ns);
        }
        if let Some(ns) = timing.flush_ns {
            self.flushes = self.flushes.saturating_add(1);
            self.flush.record(ns);
        }
        if timing.durable_write {
            self.durable_writes = self.durable_writes.saturating_add(1);
        }
        if timing.durable_fallback {
            self.durable_fallbacks = self.durable_fallbacks.saturating_add(1);
        }
        self.zeroed_bytes = self.zeroed_bytes.saturating_add(timing.zeroed);
        if timing.flushed_confirmation {
            self.flushed_confirmations = self.flushed_confirmations.saturating_add(1);
        }
    }

    /// One update's wait, from its submission to the flush that let it be answered.
    pub(crate) fn waited(&mut self, submitted: Instant, durable: Instant) {
        self.commit_wait.record(nanos(submitted, durable));
    }
}
