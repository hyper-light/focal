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
//!   memory, rather than wait for the device or a lock.
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
pub(crate) use macos::Resident;

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
    /// A handle that has not asked yet.
    pub(crate) fn new() -> Self {
        Self { refused: false }
    }

    /// Fills `buf` from `offset` if every byte of it is in memory now: true when it did; false
    /// when the read would wait, the file ends first, or the kernel takes no `RWF_NOWAIT`.
    pub(crate) fn read(&mut self, file: &File, buf: &mut [u8], offset: u64) -> io::Result<bool> {
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
            // preadv2(2): EOPNOTSUPP for a flag the kernel or file system does not take, ENOSYS
            // before 4.6, EINVAL where an old kernel reads an unknown flag as invalid.
            Err(e) if e == Errno::OPNOTSUPP || e == Errno::NOSYS || e == Errno::INVAL => {
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

    /// Never reads: every read goes to the thread that may wait.
    pub(crate) fn read(&mut self, _file: &File, _buf: &mut [u8], _offset: u64) -> io::Result<bool> {
        Ok(false)
    }

    #[cfg(test)]
    pub(crate) fn refused(&self) -> bool {
        true
    }
}
