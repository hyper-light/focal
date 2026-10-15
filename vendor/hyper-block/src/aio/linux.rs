//! Linux's native AIO through its raw system calls (io_setup(2), io_submit(2), io_getevents(2),
//! io_destroy(2)); the `iocb` and `io_event` records are `<linux/aio_abi.h>`'s.
#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

/// `EAGAIN`: a `RWF_NOWAIT` read that would have blocked (io_submit(2), `RWF_NOWAIT`).
pub(super) const EAGAIN: i64 = libc::EAGAIN as i64;

/// `IOCB_CMD_PREAD` (`<linux/aio_abi.h>`).
const IOCB_CMD_PREAD: u16 = 0;

/// `IOCB_CMD_PWRITE` (`<linux/aio_abi.h>`).
const IOCB_CMD_PWRITE: u16 = 1;

/// `IOCB_CMD_FDSYNC` (`<linux/aio_abi.h>`; taken since Linux 4.18, refused with `EINVAL` before).
const IOCB_CMD_FDSYNC: u16 = 3;

/// `RWF_NOWAIT` (`<linux/fs.h>`, Linux 4.14): don't wait if the I/O will block.
const RWF_NOWAIT: u32 = 0x0000_0008;

/// Bits of a transfer's tag that hold its index in its batch: a batch holds fewer than 2^24 - 1
/// transfers, past any depth a device takes; the other 40 bits hold the batch's number, which
/// wraps there.
const INDEX_BITS: u32 = 24;

/// The index a batch's flush is tagged with: the last the index bits hold, no transfer's.
pub(super) const FLUSH_INDEX: usize = (1 << INDEX_BITS) - 1;

/// What [`Context::push`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pushed {
    Added,
    /// The submission holds the context's size: submit, then push again.
    Full,
    /// An address, length or offset an `iocb` cannot carry: never added.
    Unrepresentable,
}

/// One record of a submission.
pub(super) enum Op {
    /// Read `len` bytes at `offset` into the buffer at `addr`.
    Read {
        addr: usize,
        len: usize,
        offset: u64,
    },
    /// Write `len` bytes of the buffer at `addr` at `offset`.
    Write {
        addr: usize,
        len: usize,
        offset: u64,
    },
    /// Flush the file's data (`fdatasync`).
    Flush,
}

/// `struct iocb` of `<linux/aio_abi.h>` on a little-endian machine (`aio_key` before
/// `aio_rw_flags`; the header swaps the two on big-endian ones, which no target of ours is).
#[cfg(target_endian = "little")]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Iocb {
    aio_data: u64,
    aio_key: u32,
    aio_rw_flags: u32,
    aio_lio_opcode: u16,
    aio_reqprio: i16,
    aio_fildes: u32,
    aio_buf: u64,
    aio_nbytes: u64,
    aio_offset: i64,
    aio_reserved2: u64,
    aio_flags: u32,
    aio_resfd: u32,
}

/// `struct io_event` of `<linux/aio_abi.h>`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(super) struct IoEvent {
    /// The `aio_data` its `iocb` carried.
    pub(super) data: u64,
    obj: u64,
    /// Bytes transferred, or a negated errno.
    pub(super) res: i64,
    res2: i64,
}

/// A transfer's tag: its batch's number and its index in the batch.
pub(super) fn tag(number: u64, index: usize) -> u64 {
    let mask = (1u64 << INDEX_BITS) - 1;
    let index = u64::try_from(index).unwrap_or(mask) & mask;
    (number << INDEX_BITS) | index
}

/// The batch number (its low 40 bits) and index a tag carries.
pub(super) fn untag(data: u64) -> (u64, usize) {
    let mask = (1u64 << INDEX_BITS) - 1;
    let index = usize::try_from(data & mask).unwrap_or(usize::MAX);
    (data >> INDEX_BITS, index)
}

/// A batch's flush's tag.
pub(super) fn flush_tag(number: u64) -> u64 {
    tag(number, FLUSH_INDEX)
}

/// Whether an index a tag carries is a flush's.
pub(super) fn is_flush(index: usize) -> bool {
    index == FLUSH_INDEX
}

/// Whether two batch numbers agree in the bits a tag keeps of them.
pub(super) fn same_number(full: u64, tagged: u64) -> bool {
    (full << INDEX_BITS) >> INDEX_BITS == tagged
}

/// An AIO context of a fixed size, with the `iocb`s, their pointers and the event records it
/// works through, each reserved once at its size: no submission or reap allocates. Dropping it
/// destroys the context, which "will cancel any outstanding asynchronous I/O and block on
/// completion" (io_destroy(2)), so no read is left writing into a buffer freed after it.
pub(super) struct Context {
    id: libc::c_ulong,
    iocbs: Vec<Iocb>,
    /// Addresses of `iocbs`' records, the `iocb **` io_submit takes; kept as addresses so the
    /// context, owned by one thread at a time, may move between threads.
    pointers: Vec<usize>,
    events: Vec<IoEvent>,
    /// Events the last reap filled.
    reaped: usize,
    /// Whether the kernel takes `RWF_NOWAIT`; cleared at the first `EINVAL` that says it does not.
    nowait: bool,
    /// Whether the kernel takes `IOCB_CMD_FDSYNC`; cleared at the first `EINVAL` that says it does
    /// not.
    fdsync: bool,
}

impl Context {
    /// A context for `depth` records at once (io_setup(2): `EAGAIN` past `aio-max-nr`).
    pub(super) fn new(depth: usize) -> io::Result<Self> {
        let nr = libc::c_long::try_from(depth).map_err(|_| io::ErrorKind::InvalidInput)?;
        let mut id: libc::c_ulong = 0;
        // SAFETY: io_setup writes the new context's id into the `aio_context_t` it is given, here
        // `id`, a live local of that type for the call's duration.
        let ret = unsafe { libc::syscall(libc::SYS_io_setup, nr, &raw mut id) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            id,
            iocbs: Vec::with_capacity(depth),
            pointers: Vec::with_capacity(depth),
            events: vec![IoEvent::default(); depth],
            reaped: 0,
            nowait: true,
            fdsync: true,
        })
    }

    /// Whether flushes may be handed to the kernel: none has yet been refused.
    pub(super) fn flushes(&self) -> bool {
        self.fdsync
    }

    /// Records the context can be handed in one submission: its size.
    pub(super) fn room(&self) -> usize {
        self.iocbs.capacity()
    }

    /// Starts a submission: none of its records yet.
    pub(super) fn clear(&mut self) {
        self.iocbs.clear();
    }

    /// Adds `op` to the submission, tagged `data`.
    pub(super) fn push(&mut self, data: u64, op: Op) -> Pushed {
        if self.iocbs.len() >= self.iocbs.capacity() {
            return Pushed::Full;
        }
        let (opcode, addr, len, offset) = match op {
            Op::Read { addr, len, offset } => (IOCB_CMD_PREAD, addr, len, offset),
            Op::Write { addr, len, offset } => (IOCB_CMD_PWRITE, addr, len, offset),
            // A flush names no buffer, length or offset: the kernel refuses one that does.
            Op::Flush => (IOCB_CMD_FDSYNC, 0, 0, 0),
        };
        let (Ok(buf), Ok(nbytes), Ok(offset)) = (
            u64::try_from(addr),
            u64::try_from(len),
            i64::try_from(offset),
        ) else {
            return Pushed::Unrepresentable;
        };
        self.iocbs.push(Iocb {
            aio_data: data,
            aio_lio_opcode: opcode,
            aio_buf: buf,
            aio_nbytes: nbytes,
            aio_offset: offset,
            ..Iocb::default()
        });
        Pushed::Added
    }

    /// The tag of the submission's record at `at`.
    pub(super) fn pushed(&self, at: usize) -> Option<u64> {
        self.iocbs.get(at).map(|iocb| iocb.aio_data)
    }

    /// Hands the submission's records to the kernel, on `file`; returns how many it took, from the
    /// first (io_submit(2) may take fewer than it is given; none when the context is full for now,
    /// or when the first is a flush the kernel turned out to take no AIO for, after which
    /// [`Self::flushes`] is false). Only reads ask `RWF_NOWAIT`.
    pub(super) fn submit(&mut self, file: &File) -> io::Result<usize> {
        if self.iocbs.is_empty() {
            return Ok(0);
        }
        let fd = u32::try_from(file.as_raw_fd()).map_err(|_| io::ErrorKind::InvalidInput)?;
        loop {
            let flags = if self.nowait { RWF_NOWAIT } else { 0 };
            self.pointers.clear();
            for iocb in &mut self.iocbs {
                iocb.aio_fildes = fd;
                iocb.aio_rw_flags = if iocb.aio_lio_opcode == IOCB_CMD_PREAD {
                    flags
                } else {
                    0
                };
                self.pointers.push(std::ptr::from_mut(iocb).addr());
            }
            let nr = libc::c_long::try_from(self.pointers.len())
                .map_err(|_| io::ErrorKind::InvalidInput)?;
            // SAFETY: `pointers` holds `nr` addresses of initialised `iocb`s in `iocbs`, both live
            // and unmoved across the call (neither is touched until it returns); the kernel copies
            // each `iocb` during the call. Each `aio_buf` names `aio_nbytes` bytes of a buffer the
            // caller keeps in place until the transfer's completion is reaped, or until this context
            // is destroyed, which waits for it (`Drop`).
            let ret = unsafe {
                libc::syscall(
                    libc::SYS_io_submit,
                    self.id,
                    nr,
                    self.pointers.as_mut_ptr().cast::<*mut Iocb>(),
                )
            };
            if ret >= 0 {
                return usize::try_from(ret).map_err(|_| io::ErrorKind::InvalidData.into());
            }
            let error = io::Error::last_os_error();
            // io_submit fails only when it took none: the first record is the one refused.
            let first = self.iocbs.first().map(|iocb| iocb.aio_lio_opcode);
            match error.raw_os_error() {
                // A kernel before 4.18, or a file system with no AIO flush: the caller flushes
                // in place from now on.
                Some(libc::EINVAL) if first == Some(IOCB_CMD_FDSYNC) && self.fdsync => {
                    self.fdsync = false;
                    return Ok(0);
                }
                // A kernel before 4.14 takes no `RWF_NOWAIT`: submit again without it.
                Some(libc::EINVAL) if first == Some(IOCB_CMD_PREAD) && self.nowait => {
                    self.nowait = false;
                }
                // The context is full for now: none taken; the caller reaps and submits again.
                Some(libc::EAGAIN) => return Ok(0),
                _ => return Err(error),
            }
        }
    }

    /// Takes the completions ready into the context's records, waiting for at least one when
    /// `wait` is set; returns how many ([`Self::event`] reads them).
    pub(super) fn reap(&mut self, wait: bool) -> io::Result<usize> {
        let max =
            libc::c_long::try_from(self.events.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let min = libc::c_long::from(wait);
        let zero = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let timeout: *const libc::timespec = if wait {
            std::ptr::null()
        } else {
            &raw const zero
        };
        loop {
            // SAFETY: `events` holds `max` initialised `io_event` records the kernel may write,
            // live across the call; `timeout` is null (wait) or points to `zero`, live here.
            let ret = unsafe {
                libc::syscall(
                    libc::SYS_io_getevents,
                    self.id,
                    min,
                    max,
                    self.events.as_mut_ptr(),
                    timeout,
                )
            };
            if ret >= 0 {
                self.reaped = usize::try_from(ret).map_err(|_| io::ErrorKind::InvalidData)?;
                return Ok(self.reaped);
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINTR) {
                return Err(error);
            }
        }
    }

    /// The `at`th completion the last reap took.
    pub(super) fn event(&self, at: usize) -> Option<IoEvent> {
        if at < self.reaped {
            self.events.get(at).copied()
        } else {
            None
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // SAFETY: `id` is this context's, set up in `new` and destroyed only here. io_destroy
        // cancels what it can and blocks until every transfer out has completed, so no buffer is
        // written after this returns.
        let _ = unsafe { libc::syscall(libc::SYS_io_destroy, self.id) };
    }
}
