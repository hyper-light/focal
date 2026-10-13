//! Linux, for tests: whether every page of a range of a file is in the page cache, from `mincore(2)`
//! over a read-only shared mapping of the range, which reads no byte and starts no I/O.
//!
//! The resident reads cannot say this. `preadv2(2)` with `RWF_NOWAIT` on a page not cached starts
//! read-ahead before it answers, of the page and of the window after it, and reads the page if
//! that I/O finished before it took the page's lock: in Linux 6.8's `mm/filemap.c`,
//! `filemap_get_pages` calls `page_cache_sync_readahead` on a miss unless the read is `IOCB_NOIO`,
//! which no system call sets, and `filemap_update_page` refuses with `EAGAIN` only while the folio
//! is locked under its I/O. A reader preempted between the two finds the read done, and a later
//! read of the window finds it in memory. `mincore(2)` reports a file mapping's pages as the page
//! cache holds them for a file the caller owns or may open for writing, and every page as resident
//! for any other (Linux 6.8 `mm/mincore.c`, `can_do_mincore`): a test's own scratch file is its.
#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

/// Whether every page of [`offset`, `offset + len`) of `file` is in the page cache now.
pub(crate) fn cached(file: &File, offset: u64, len: usize) -> io::Result<bool> {
    let beyond = || io::Error::other("a range past the address space");
    let page = rustix::param::page_size();
    let page_u64 = u64::try_from(page).map_err(|_| beyond())?;
    let start = offset
        .checked_sub(offset.checked_rem(page_u64).ok_or_else(beyond)?)
        .ok_or_else(beyond)?;
    let end = offset
        .checked_add(u64::try_from(len).map_err(|_| beyond())?)
        .and_then(|e| e.checked_next_multiple_of(page_u64))
        .ok_or_else(beyond)?;
    let span = usize::try_from(end.checked_sub(start).ok_or_else(beyond)?).map_err(|_| beyond())?;
    if span == 0 {
        return Ok(true);
    }
    let at = libc::off_t::try_from(start).map_err(|_| beyond())?;
    // SAFETY: a fresh mapping the kernel places (null hint) of `span` bytes of an open file, read-only
    // and shared, from a page-aligned offset; no byte is read through it, and it is unmapped below
    // before the function returns, so nothing outlives the file or refers into it.
    let addr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            span,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            at,
        )
    };
    if addr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    let mut pages = vec![0u8; span.div_ceil(page)];
    // SAFETY: `addr` and `span` are the mapping made above, page-aligned, and `pages` holds one byte
    // for each of its pages, which mincore(2) writes and nothing else touches meanwhile.
    let probed = unsafe { libc::mincore(addr, span, pages.as_mut_ptr()) };
    let failed = (probed != 0).then(io::Error::last_os_error);
    // SAFETY: the mapping made above, unmapped once; only mincore(2) was given its address.
    let unmapped = unsafe { libc::munmap(addr, span) };
    if let Some(e) = failed {
        return Err(e);
    }
    if unmapped != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(pages.iter().all(|p| p & 1 == 1))
}
