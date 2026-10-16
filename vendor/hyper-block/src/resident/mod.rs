//! Reads the OS already holds in memory, made on the calling thread without waiting for the
//! device ([`crate::file::DeviceFile::read_resident_at`]).
//!
//! A caller that must not wait for the device hands its reads to a thread that may
//! ([`crate::issuer`]) and waits for that thread's answer: each handoff a wake of a parked thread,
//! whose cost the OS's scheduler sets. On a busy machine that is tens of microseconds at the
//! median and milliseconds at the tail (mantle `docs/research/41-resident-reads.md`), for a
//! read the OS answers from memory in microseconds. So a read whose every page is in memory is
//! made where it is asked for, and only a read that would wait is handed over.
//!
//! - Linux: `preadv2(2)` with `RWF_NOWAIT` (since 4.14) returns `EAGAIN`, or only the part in
//!   memory, rather than wait for the device or a lock. A page not in memory has its read-ahead
//!   started before the call answers, and is read if that I/O finished first (`linux`, through
//!   which tests ask the page cache itself).
//! - macOS: no such flag. `mincore(2)` over a read-only shared mapping of the file reports each
//!   page's residency in the unified buffer cache (`macos`), and a read every page of which is
//!   resident is made with `pread(2)`.
//! - Elsewhere, no read is made here: every read goes to the thread that may wait.

#[cfg(not(target_vendor = "apple"))]
use std::fs::File;
#[cfg(not(target_vendor = "apple"))]
use std::io;

#[cfg(target_vendor = "apple")]
mod macos;

#[cfg(target_vendor = "apple")]
pub(crate) use macos::{Epoch, Resident};

#[cfg(all(test, target_os = "linux"))]
mod linux;

#[cfg(all(test, target_os = "linux"))]
pub(crate) use linux::cached;

/// What a handle's reads from memory did ([`crate::block::BlockFile::residency`]): probes made,
/// reads made on an earlier probe's answer instead (macOS, mantle `docs/research/46-reclaim-epochs.md`),
/// epochs read and the changes among them that forgot every answer, and, in shadow mode, the
/// answers a probe made anyway found wrong.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Residency {
    /// Reads that asked the kernel which of their pages it holds (`mincore(2)`).
    pub probes: u64,
    /// Reads made on an earlier probe's answer, kept while the kernel reclaimed nothing.
    pub skips: u64,
    /// Times the kernel's reclaim counters were read.
    pub epochs: u64,
    /// Readings of them that had moved, each forgetting every kept answer.
    pub changes: u64,
    /// In shadow mode, kept answers a probe made anyway found wrong.
    pub wrong: u64,
}

impl Residency {
    /// Both handles' counts summed: one file kept as several (`crate::segmented`).
    #[must_use]
    pub fn plus(self, other: Self) -> Self {
        Self {
            probes: self.probes.saturating_add(other.probes),
            skips: self.skips.saturating_add(other.skips),
            epochs: self.epochs.saturating_add(other.epochs),
            changes: self.changes.saturating_add(other.changes),
            wrong: self.wrong.saturating_add(other.wrong),
        }
    }
}

/// No answer is kept off macOS, so there is no epoch for one to hold in.
#[cfg(not(target_vendor = "apple"))]
#[derive(Debug)]
pub(crate) struct Epoch;

#[cfg(not(target_vendor = "apple"))]
impl Epoch {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn residency(&self) -> Residency {
        Residency::default()
    }
}

/// A handle's state for reads made only from memory: whether the kernel refused `RWF_NOWAIT`.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct Resident {
    /// A kernel before 4.14, or a file system without `FMODE_NOWAIT`, refused the flag: the
    /// handle asks no more, and its reads all go to the thread that may wait.
    refused: bool,
}

#[cfg(target_os = "linux")]
impl Resident {
    /// A handle that has not asked: a direct one, which reads nothing from memory.
    pub(crate) fn new() -> Self {
        Self { refused: false }
    }

    /// A handle on `file` that asked the kernel, as it was opened, whether its file system takes
    /// `RWF_NOWAIT`: a read of one byte at the file's end, which reads nothing and starts no
    /// read-ahead, but which the kernel refuses (`EOPNOTSUPP`) for a file without `FMODE_NOWAIT`
    /// before it looks at the length (`kiocb_set_rw_flags`). overlayfs is one (Linux 6.12,
    /// measured 2026-10-16, where the ext4 beneath it takes the flag), so a handle on a
    /// container's own file system knew it could not read from memory only once a read had
    /// tried, and said it could until then.
    pub(crate) fn probed(file: &File) -> io::Result<Self> {
        use rustix::io::{Errno, ReadWriteFlags, preadv2};
        let end = file.metadata()?.len();
        let mut byte = [0u8; 1];
        match preadv2(
            file,
            &mut [io::IoSliceMut::new(&mut byte)],
            end,
            ReadWriteFlags::NOWAIT,
        ) {
            // Nothing past the end, or a byte another writer put there since: taken. A byte not
            // in memory would wait, which also says the flag is taken.
            Ok(_) => Ok(Self { refused: false }),
            Err(e) if e == Errno::AGAIN || e == Errno::INTR => Ok(Self { refused: false }),
            Err(e) if Self::refusal(e) => Ok(Self { refused: true }),
            Err(e) => Err(e.into()),
        }
    }

    /// A handle that shares `self`'s open file (a duplicate): the same file system's answer.
    pub(crate) fn duplicate(&self) -> Self {
        Self {
            refused: self.refused,
        }
    }

    /// preadv2(2): EOPNOTSUPP for a flag the kernel or file system does not take, ENOSYS before
    /// 4.6, EINVAL where an old kernel reads an unknown flag as invalid.
    fn refusal(e: rustix::io::Errno) -> bool {
        use rustix::io::Errno;
        e == Errno::OPNOTSUPP || e == Errno::NOSYS || e == Errno::INVAL
    }

    /// Whether a read can be made from memory: the kernel has not refused the flag.
    pub(crate) fn reads(&self) -> bool {
        !self.refused
    }

    /// Fills `buf` from `offset` if every byte of it is in memory now: true when it did; false
    /// when the read would wait, the file ends first, or the kernel takes no `RWF_NOWAIT`.
    pub(crate) fn read(
        &mut self,
        file: &File,
        buf: &mut [u8],
        offset: u64,
        _epoch: &mut Epoch,
    ) -> io::Result<bool> {
        use rustix::io::{Errno, ReadWriteFlags, preadv2};
        if self.refused {
            return Ok(false);
        }
        let want = buf.len();
        match preadv2(
            file,
            &mut [io::IoSliceMut::new(buf)],
            offset,
            ReadWriteFlags::NOWAIT,
        ) {
            // Short: the rest would wait, or the file ends there; the read that may wait makes
            // it, or reports the end. Linux 5.9 and 5.10 also answer 0 for a read that would
            // wait (preadv2(2), BUGS), which is short too.
            Ok(read) => Ok(read == want),
            Err(e) if e == Errno::AGAIN || e == Errno::INTR => Ok(false),
            Err(e) if Self::refusal(e) => {
                self.refused = true;
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Whether this handle reads nothing from memory: the kernel refused the flag.
    #[cfg(test)]
    pub(crate) fn refused(&self) -> bool {
        self.refused
    }

    /// Nothing is counted here: `RWF_NOWAIT` asks and reads in one call, so no answer is kept.
    pub(crate) fn residency(&self) -> Residency {
        Residency::default()
    }

    /// No answers to check.
    pub(crate) fn set_shadow(&mut self, _on: bool) {}
}

/// No platform interface: no read is made from memory.
#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
#[derive(Debug)]
pub(crate) struct Resident;

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
impl Resident {
    pub(crate) fn new() -> Self {
        Self
    }

    /// A handle on `file`: nothing to ask, nothing read here.
    pub(crate) fn probed(_file: &File) -> io::Result<Self> {
        Ok(Self)
    }

    /// A duplicate's state.
    pub(crate) fn duplicate(&self) -> Self {
        Self
    }

    /// No read is made from memory here.
    pub(crate) fn reads(&self) -> bool {
        false
    }

    /// Never reads: every read goes to the thread that may wait.
    pub(crate) fn read(
        &mut self,
        _file: &File,
        _buf: &mut [u8],
        _offset: u64,
        _epoch: &mut Epoch,
    ) -> io::Result<bool> {
        Ok(false)
    }

    pub(crate) fn residency(&self) -> Residency {
        Residency::default()
    }

    pub(crate) fn set_shadow(&mut self, _on: bool) {}

    #[cfg(test)]
    pub(crate) fn refused(&self) -> bool {
        true
    }
}
