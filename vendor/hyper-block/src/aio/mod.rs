//! Transfers a submitter issues and reaps itself, with no thread between it and the device
//! (hyper-raft `docs/research/issuer-completions.md`).
//!
//! The device issuer ([`crate::issuer`]) carries a batch through its own thread and a pool of
//! workers: four blocking-channel hand-offs, each a kernel wake under load. Its reads bench
//! measured a batch of four cached reads at 43 µs against 2.7 µs read in place, while a batch of
//! four device reads took half the time of four reads one after another (docs/benchmarks.md,
//! "hyper-block: a batch of reads through the issuer"). An [`AioFile`] keeps the overlap and drops
//! the hand-offs: on Linux it submits a batch's reads or writes to the kernel's native AIO
//! (io_submit(2)) from the calling thread and takes their completions on the same thread
//! (io_getevents(2)), with a zero timeout for [`AioFile::try_answer`]. The owner's decision of
//! 2026-10-01 takes native AIO on Linux, not io_uring, whose attack surface was behind 60% of the
//! kernel exploits Google's kCTF paid for (research note §5).
//!
//! Native AIO is asynchronous only without the page cache: "libaio only supports unbuffered
//! accesses (i.e., with O_DIRECT)" (Didona et al., SYSTOR '22, §2), and a buffered transfer is
//! done inside io_submit. So an `AioFile` is made only over a file opened for direct I/O, and is
//! refused, typed ([`crate::DiskError::Unsupported`]), elsewhere and on every other OS: there the
//! caller reads cached pages in place and batches device transfers through the issuer.
//!
//! **Reads** each ask `RWF_NOWAIT` (Linux 4.14): a read that would block inside the kernel (a lock
//! the file system holds, a congested device) completes at once with `EAGAIN` rather than block
//! the submitter, and is then read in place on the submitter's thread. A kernel that takes no
//! `RWF_NOWAIT` refuses the submission with `EINVAL`; it is sent again without the flag.
//!
//! **Writes** keep the issuer's rule: a batch's writes all complete before its flush is issued,
//! and the flush only if every write succeeded, since a failed write leaves the file in a state its
//! caller must fence and recover from, not make durable (Rebello et al., ATC 2020). The flush is an
//! `IOCB_CMD_FDSYNC` through the same context where the kernel takes one (Linux 4.18; asked once,
//! when the file is first flushed), so it is reaped as the writes are; where it does not, the
//! submitter flushes with `fdatasync`. Writes take no `RWF_NOWAIT`: a write the kernel would block
//! on (an allocation, an extent to convert) is the device's to finish, and the flush follows it.
//!
//! Nothing is allocated per transfer: a batch's transfers come back in the vector they were given
//! in, and the context's records are reserved once at its size.

use std::collections::VecDeque;
use std::path::PathBuf;

use crate::DiskError;
use crate::buf::AlignedBuf;
use crate::file::{Caching, DeviceFile};

#[cfg(target_os = "linux")]
mod linux;

/// A batch's transfers: each buffer and the file offset it is read from or written at.
pub type Transfers = Vec<(AlignedBuf, u64)>;

/// A batch's answer: its transfers given back, in the order given (a read's buffer filled), once
/// every transfer and the flush asked for completed; or the batch's first failure.
pub type Answer = Result<Transfers, DiskError>;

/// What a batch does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Read,
    /// Write; then flush, once every write succeeded, when `flush` is set.
    Write {
        flush: bool,
    },
}

/// One batch out: its transfers, how many are still out, its first failure, and whether its
/// flush is still to be issued.
struct Batch {
    number: u64,
    /// Read only where transfers are issued: Linux.
    #[cfg(target_os = "linux")]
    kind: Kind,
    transfers: Transfers,
    outstanding: usize,
    failed: Option<DiskError>,
    flush_due: bool,
}

/// A file's transfers issued to the kernel's native AIO and reaped by the caller (module docs).
pub struct AioFile {
    /// Declared first, so dropped first: destroying the context waits for every transfer out
    /// before the batches that own their buffers are dropped.
    #[cfg(target_os = "linux")]
    ctx: linux::Context,
    file: DeviceFile,
    path: PathBuf,
    /// Batches the caller may have out at once.
    batches: usize,
    /// Batches out, in the order submitted.
    out: VecDeque<Batch>,
    /// Transfers and flushes of the batches out not yet handed to the kernel: their tags, in order.
    waiting: VecDeque<u64>,
    /// Transfers and flushes the kernel holds now: at most the context's size.
    in_flight: usize,
    next: u64,
    /// Flushes made by the submitter with `fdatasync` because the kernel took no AIO flush.
    flushes_in_place: u64,
}

impl std::fmt::Debug for AioFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AioFile")
            .field("path", &self.path)
            .field("batches", &self.batches)
            .field("out", &self.out.len())
            .field("in_flight", &self.in_flight)
            .finish_non_exhaustive()
    }
}

impl AioFile {
    /// Transfers of `file`, at most `depth` in the kernel at once and `batches` batches out at
    /// once. Refused, typed, for a file not opened for direct I/O, for a depth or batch count of
    /// zero, and on every OS but Linux; `Io` where the kernel refuses the context (io_setup(2):
    /// `EAGAIN` past `/proc/sys/fs/aio-max-nr`).
    pub fn new(file: DeviceFile, depth: usize, batches: usize) -> Result<Self, DiskError> {
        let path = file.path().to_path_buf();
        let unsupported = |reason: &'static str| DiskError::Unsupported {
            path: path.clone(),
            reason,
        };
        if depth == 0 || batches == 0 {
            return Err(unsupported("native AIO with no depth or no batch"));
        }
        if file.caching() != Caching::Direct {
            return Err(unsupported(
                "native AIO only over a file opened for direct I/O: a buffered transfer is done inside io_submit",
            ));
        }
        #[cfg(target_os = "linux")]
        {
            let ctx = linux::Context::new(depth).map_err(|source| DiskError::Io {
                op: "io_setup",
                path: path.clone(),
                source,
            })?;
            Ok(Self {
                ctx,
                file,
                path,
                batches,
                out: VecDeque::with_capacity(batches),
                waiting: VecDeque::with_capacity(depth),
                in_flight: 0,
                next: 0,
                flushes_in_place: 0,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = file;
            Err(unsupported(
                "native AIO is Linux's: elsewhere the issuer batches device transfers",
            ))
        }
    }

    /// Hands a batch of reads to the kernel and returns its number without waiting: each
    /// `(buffer, offset)` is filled, the whole of the buffer's length, from its offset.
    pub fn submit_reads(&mut self, reads: Transfers) -> Result<u64, (DiskError, Transfers)> {
        self.submit(reads, Kind::Read)
    }

    /// Hands a batch of writes to the kernel and returns its number without waiting: each
    /// `(buffer, offset)` is written whole at its offset, and once all have completed, the file is
    /// flushed when `flush` is set and every write succeeded.
    pub fn submit_writes(
        &mut self,
        writes: Transfers,
        flush: bool,
    ) -> Result<u64, (DiskError, Transfers)> {
        self.submit(writes, Kind::Write { flush })
    }

    /// Every buffer and offset must meet the file's alignment. Refused with the batches allowed
    /// already out, before anything is submitted, and the transfers are given back with the
    /// refusal. Transfers past the depth wait in order and go to the kernel as earlier ones
    /// complete, at the next [`Self::answer`] or [`Self::try_answer`].
    fn submit(&mut self, transfers: Transfers, kind: Kind) -> Result<u64, (DiskError, Transfers)> {
        if self.out.len() >= self.batches {
            let refused = DiskError::Unsupported {
                path: self.path.clone(),
                reason: "a batch past those allowed out; take an answer first",
            };
            return Err((refused, transfers));
        }
        let align = self.file.alignment();
        if let Some((buf, at)) = transfers
            .iter()
            .find(|(buf, at)| !align.is_aligned(buf.len()) || !align.is_aligned_u64(*at))
        {
            let refused = DiskError::Misaligned {
                offset: *at,
                len: buf.len(),
                align: align.get(),
            };
            return Err((refused, transfers));
        }
        // The kernel's offsets are `loff_t`, signed: a transfer must end within `i64::MAX`.
        if transfers.iter().any(|(buf, at)| {
            u64::try_from(buf.len())
                .ok()
                .and_then(|len| at.checked_add(len))
                .is_none_or(|end| i64::try_from(end).is_err())
        }) {
            let refused = DiskError::Unsupported {
                path: self.path.clone(),
                reason: "a transfer ending past the largest file offset (i64::MAX)",
            };
            return Err((refused, transfers));
        }
        #[cfg(target_os = "linux")]
        if transfers.len() >= linux::FLUSH_INDEX {
            let refused = DiskError::Unsupported {
                path: self.path.clone(),
                reason: "a batch of 2^24 - 1 transfers or more, past what a tag holds",
            };
            return Err((refused, transfers));
        }
        let number = self.next;
        self.next = self.next.wrapping_add(1);
        let count = transfers.len();
        #[cfg(target_os = "linux")]
        for index in 0..count {
            self.waiting.push_back(linux::tag(number, index));
        }
        self.out.push_back(Batch {
            number,
            #[cfg(target_os = "linux")]
            kind,
            transfers,
            outstanding: count,
            failed: None,
            flush_due: kind == Kind::Write { flush: true },
        });
        // A batch of no transfers that asks a flush goes straight to it.
        self.settle();
        if let Err(e) = self.pump() {
            self.fail_waiting(&e);
        }
        Ok(number)
    }

    /// The next answer, waiting for it: a batch's number and its answer. Batches are answered as
    /// they end, in any order. Refused with no batch out.
    pub fn answer(&mut self) -> Result<(u64, Answer), DiskError> {
        loop {
            if let Some(done) = self.take_done() {
                return Ok(done);
            }
            if self.out.is_empty() {
                return Err(DiskError::Unsupported {
                    path: self.path.clone(),
                    reason: "an answer with no batch out",
                });
            }
            self.reap(true)?;
            self.settle();
            if let Err(e) = self.pump() {
                self.fail_waiting(&e);
            }
        }
    }

    /// The next answer if one has come; none otherwise, or with no batch out. Never waits on the
    /// kernel; a flush the kernel takes no AIO for is made here, in place.
    pub fn try_answer(&mut self) -> Result<Option<(u64, Answer)>, DiskError> {
        if let Some(done) = self.take_done() {
            return Ok(Some(done));
        }
        if self.in_flight > 0 {
            self.reap(false)?;
            self.settle();
            if let Err(e) = self.pump() {
                self.fail_waiting(&e);
            }
        }
        Ok(self.take_done())
    }

    /// Batches submitted and not yet answered.
    pub fn out(&self) -> usize {
        self.out.len()
    }

    /// Flushes made with `fdatasync` on the submitter's thread because the kernel took no AIO
    /// flush (`IOCB_CMD_FDSYNC`, Linux 4.18).
    pub fn flushes_in_place(&self) -> u64 {
        self.flushes_in_place
    }

    /// The file.
    pub fn file(&self) -> &DeviceFile {
        &self.file
    }

    /// A batch that has ended, taken out of the batches out.
    fn take_done(&mut self) -> Option<(u64, Answer)> {
        let at = self
            .out
            .iter()
            .position(|batch| batch.outstanding == 0 && !batch.flush_due)?;
        let batch = self.out.remove(at)?;
        let answer = match batch.failed {
            Some(e) => Err(e),
            None => Ok(batch.transfers),
        };
        Some((batch.number, answer))
    }

    /// Issues the flush of every batch whose writes have all completed: through the context where
    /// the kernel takes AIO flushes, else in place; never after a failed write.
    fn settle(&mut self) {
        let mut at = 0;
        while let Some(batch) = self.out.get_mut(at) {
            at = at.saturating_add(1);
            if !batch.flush_due || batch.outstanding > 0 {
                continue;
            }
            batch.flush_due = false;
            if batch.failed.is_some() {
                continue;
            }
            #[cfg(target_os = "linux")]
            if self.ctx.flushes() {
                batch.outstanding = 1;
                self.waiting.push_back(linux::flush_tag(batch.number));
                continue;
            }
            if let Err(e) = self.file.sync_data() {
                batch.failed = Some(e);
            }
            self.flushes_in_place = self.flushes_in_place.saturating_add(1);
        }
    }

    /// Fails every transfer not yet handed to the kernel: the submission was refused, and nothing
    /// of it is in flight.
    fn fail_waiting(&mut self, cause: &DiskError) {
        let reason = match cause {
            DiskError::Io { .. } => "io_submit refused the batch",
            _ => "the batch was not submitted",
        };
        while let Some(tag) = self.waiting.pop_front() {
            #[cfg(target_os = "linux")]
            {
                let (number, _) = linux::untag(tag);
                if let Some(batch) = self
                    .out
                    .iter_mut()
                    .find(|batch| linux::same_number(batch.number, number))
                {
                    batch.outstanding = batch.outstanding.saturating_sub(1);
                    batch.flush_due = false;
                    batch.failed.get_or_insert_with(|| DiskError::Unsupported {
                        path: self.path.clone(),
                        reason,
                    });
                }
            }
            #[cfg(not(target_os = "linux"))]
            let _ = (tag, reason);
        }
    }

    /// Hands waiting transfers and flushes to the kernel up to its size.
    #[cfg(target_os = "linux")]
    fn pump(&mut self) -> Result<(), DiskError> {
        loop {
            let room = self.ctx.room().saturating_sub(self.in_flight);
            if room == 0 || self.waiting.is_empty() {
                return Ok(());
            }
            self.ctx.clear();
            let mut pushed = 0usize;
            while pushed < room {
                let Some(tag) = self.waiting.pop_front() else {
                    break;
                };
                let (number, index) = linux::untag(tag);
                let Some(batch) = self
                    .out
                    .iter_mut()
                    .find(|batch| linux::same_number(batch.number, number))
                else {
                    continue;
                };
                let op = if linux::is_flush(index) {
                    if !self.ctx.flushes() {
                        // The kernel took no AIO flush: the submitter flushes in place.
                        batch.outstanding = batch.outstanding.saturating_sub(1);
                        if let Err(e) = self.file.sync_data() {
                            batch.failed.get_or_insert(e);
                        }
                        self.flushes_in_place = self.flushes_in_place.saturating_add(1);
                        continue;
                    }
                    linux::Op::Flush
                } else {
                    let Some((buf, at)) = batch.transfers.get_mut(index) else {
                        continue;
                    };
                    let bytes = buf.as_mut_slice();
                    // The buffer's heap bytes stay where they are until the transfer ends: its
                    // batch leaves `out` only once nothing of it is out, and the context is
                    // dropped (and waits for every transfer) before the batches are.
                    let (addr, len, offset) = (bytes.as_mut_ptr().addr(), bytes.len(), *at);
                    match batch.kind {
                        Kind::Read => linux::Op::Read { addr, len, offset },
                        Kind::Write { .. } => linux::Op::Write { addr, len, offset },
                    }
                };
                match self.ctx.push(tag, op) {
                    linux::Pushed::Added => pushed = pushed.saturating_add(1),
                    linux::Pushed::Full => {
                        self.waiting.push_front(tag);
                        break;
                    }
                    // `submit` refuses offsets past `i64`, so this is not reached; were it, the
                    // transfer fails rather than wait for room it never fits.
                    linux::Pushed::Unrepresentable => {
                        batch.outstanding = batch.outstanding.saturating_sub(1);
                        batch.failed.get_or_insert_with(|| DiskError::Unsupported {
                            path: self.path.clone(),
                            reason: "a transfer an iocb cannot carry",
                        });
                    }
                }
            }
            let flushes = self.ctx.flushes();
            let submitted =
                self.ctx
                    .submit(self.file.std_file())
                    .map_err(|source| DiskError::Io {
                        op: "io_submit",
                        path: self.path.clone(),
                        source,
                    })?;
            self.in_flight = self.in_flight.saturating_add(submitted);
            // What the kernel did not take goes back to the front, in order.
            let mut at = pushed;
            while at > submitted {
                at = at.saturating_sub(1);
                if let Some(tag) = self.ctx.pushed(at) {
                    self.waiting.push_front(tag);
                }
            }
            // None taken because the first was a flush the kernel refused: send the rest again,
            // that flush now made in place.
            if submitted == 0 && self.ctx.flushes() == flushes {
                return Ok(());
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn pump(&mut self) -> Result<(), DiskError> {
        Ok(())
    }

    /// Takes the kernel's completions, waiting for one when `wait` is set, and records each in
    /// its batch.
    #[cfg(target_os = "linux")]
    fn reap(&mut self, wait: bool) -> Result<(), DiskError> {
        let got = self
            .ctx
            .reap(wait && self.in_flight > 0)
            .map_err(|source| DiskError::Io {
                op: "io_getevents",
                path: self.path.clone(),
                source,
            })?;
        for at in 0..got {
            let Some(event) = self.ctx.event(at) else {
                break;
            };
            self.in_flight = self.in_flight.saturating_sub(1);
            let (number, index) = linux::untag(event.data);
            self.complete(number, index, event.res);
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn reap(&mut self, _wait: bool) -> Result<(), DiskError> {
        Ok(())
    }

    /// Records that transfer `index` of batch `number`, or its flush, ended with `res`: bytes
    /// moved (none for a flush), or a negated errno.
    #[cfg(target_os = "linux")]
    fn complete(&mut self, number: u64, index: usize, res: i64) {
        let path = &self.path;
        let file = &self.file;
        let Some(batch) = self
            .out
            .iter_mut()
            .find(|batch| linux::same_number(batch.number, number))
        else {
            return;
        };
        batch.outstanding = batch.outstanding.saturating_sub(1);
        let errno = |op: &'static str| DiskError::Io {
            op,
            path: path.clone(),
            source: std::io::Error::from_raw_os_error(
                i32::try_from(res.saturating_neg()).unwrap_or(i32::MAX),
            ),
        };
        let outcome = if linux::is_flush(index) {
            if res == 0 {
                Ok(())
            } else {
                Err(errno("aio fdatasync"))
            }
        } else {
            let Some((buf, at)) = batch.transfers.get_mut(index) else {
                return;
            };
            let len = buf.len();
            match (batch.kind, usize::try_from(res)) {
                (_, Ok(moved)) if moved == len => Ok(()),
                (Kind::Read, Ok(read)) => Err(DiskError::ShortRead {
                    path: path.clone(),
                    offset: *at,
                    missing: len.saturating_sub(read),
                }),
                (Kind::Write { .. }, Ok(_)) => Err(DiskError::Io {
                    op: "aio write",
                    path: path.clone(),
                    source: std::io::ErrorKind::WriteZero.into(),
                }),
                // A read the kernel would have blocked on (`RWF_NOWAIT`): read in place.
                (Kind::Read, Err(_)) if res == -linux::EAGAIN => {
                    file.read_exact_at(buf.as_mut_slice(), *at)
                }
                (Kind::Read, Err(_)) => Err(errno("aio read")),
                (Kind::Write { .. }, Err(_)) => Err(errno("aio write")),
            }
        };
        if let Err(e) = outcome {
            batch.failed.get_or_insert(e);
        }
    }
}
