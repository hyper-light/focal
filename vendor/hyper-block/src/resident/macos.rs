//! macOS: whether a range of a file is in the unified buffer cache, from `mincore(2)` over a
//! read-only shared mapping of the file, and a `pread(2)` of the range when every page of it is.
//!
//! Measured on an M5 Max under a load average of 76 to 98 (mantle
//! `docs/research/41-resident-reads.md`): the mapping reports the cache's residency exactly, for
//! pages it never touched and for pages written after the file grew past its end at mapping
//! time: every page of a file written through the cache, none of one written with `F_NOCACHE`,
//! and after one `pread(2)` that page alone. A probe on a kept mapping costs 2.0 to 2.7 µs and one
//! with its own `mmap(2)` and `munmap(2)` 10 to 12 µs, so the mapping is kept, and remapped twice
//! as long when a read reaches past it: the remaps a file's growth costs are logarithmic in its
//! length, and the address space held at most twice what was read (Cormen et al., Introduction
//! to Algorithms, 3rd ed., §17.4, the table that doubles).
//!
//! No byte is read through the mapping, which is only passed to `mincore(2)`: a page evicted, or
//! the file truncated, after the probe costs a `pread(2)` that waits or fails, never a fault in
//! this process (Crotty, Leis and Pavlo, CIDR 2022, on what reading through a mapping costs a
//! database).
#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;

/// A handle's mapping for probes, and the vector `mincore(2)` fills.
#[derive(Debug)]
pub(crate) struct Resident {
    window: Option<Window>,
    /// One byte for each page of the range probed: kept, so a probe allocates only to grow it.
    pages: Vec<libc::c_char>,
    /// The window could not be mapped, or a probe refused: the handle probes no more, and its
    /// reads all go to the thread that may wait.
    refused: bool,
}

/// A read-only shared mapping of the file from its start, its address exposed so the handle
/// that holds it moves between threads: the address is only ever passed to `mincore(2)` and
/// `munmap(2)`.
#[derive(Debug)]
struct Window {
    addr: usize,
    len: usize,
}

impl Window {
    fn map(file: &File, len: usize) -> io::Result<Self> {
        // SAFETY: a new read-only shared mapping at an address the kernel chooses, of a
        // descriptor this borrow holds open for the call; no reference in the process names that
        // range before mmap(2) returns it, and none is made after: the mapping is only probed.
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            addr: addr.expose_provenance(),
            len,
        })
    }
}

impl Drop for Window {
    fn drop(&mut self) {
        // SAFETY: `addr` and `len` are a mapping this process made with mmap(2), unmapped only
        // here, and nothing refers into it: it was only passed to mincore(2). A failure leaves
        // the range mapped, which costs address space and nothing else.
        unsafe {
            libc::munmap(std::ptr::with_exposed_provenance_mut(self.addr), self.len);
        }
    }
}

impl Resident {
    /// A handle with no mapping yet: it maps one on its first read.
    pub(crate) fn new() -> Self {
        Self {
            window: None,
            pages: Vec::new(),
            refused: false,
        }
    }

    /// Fills `buf` from `offset` if every page of it is in the unified buffer cache now: true when
    /// it did; false when a page is not, the file ends first, or the handle cannot probe.
    pub(crate) fn read(&mut self, file: &File, buf: &mut [u8], offset: u64) -> io::Result<bool> {
        if self.refused || buf.is_empty() {
            return Ok(false);
        }
        let page = rustix::param::page_size();
        // The whole pages the read touches, [first, end) of the file.
        let Some((first, end)) = usize::try_from(offset).ok().and_then(|start| {
            let first = start.checked_sub(start.checked_rem(page)?)?;
            let end = start
                .checked_add(buf.len())?
                .checked_next_multiple_of(page)?;
            Some((first, end))
        }) else {
            return Ok(false);
        };
        if self.window.as_ref().is_none_or(|w| w.len < end) {
            let len = self
                .window
                .as_ref()
                .and_then(|w| w.len.checked_mul(2))
                .map_or(end, |doubled| doubled.max(end));
            // The old window is unmapped as the new one replaces it.
            self.window = None;
            match Window::map(file, len) {
                Ok(window) => self.window = Some(window),
                Err(_) => {
                    self.refused = true;
                    return Ok(false);
                }
            }
        }
        let Some(window) = self.window.as_ref() else {
            return Ok(false);
        };
        let (Some(span), Some(at)) = (end.checked_sub(first), window.addr.checked_add(first))
        else {
            return Ok(false);
        };
        let count = span.checked_div(page).unwrap_or(0);
        self.pages.clear();
        if self.pages.try_reserve(count).is_err() {
            return Ok(false);
        }
        self.pages.resize(count, 0);
        // SAFETY: [at, at + span) lies inside the live mapping (`first < end <= window.len`), and
        // `pages` holds one byte for each of its `span / page` pages, which mincore(2) writes.
        let probed = unsafe {
            libc::mincore(
                std::ptr::with_exposed_provenance(at),
                span,
                self.pages.as_mut_ptr(),
            )
        };
        if probed != 0 {
            self.refused = true;
            return Ok(false);
        }
        if !self
            .pages
            .iter()
            .all(|&p| i32::from(p) & libc::MINCORE_INCORE != 0)
        {
            return Ok(false);
        }
        // Every page is in memory: the read copies it from there. A page evicted since the probe
        // makes this read wait for the device once, on this thread; its bytes are right either
        // way.
        match file.read_exact_at(buf, offset) {
            Ok(()) => Ok(true),
            // The file ends inside the range: the read that may wait reports that end.
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Whether this handle reads nothing from memory: its window could not be mapped.
    #[cfg(test)]
    pub(crate) fn refused(&self) -> bool {
        self.refused
    }
}
