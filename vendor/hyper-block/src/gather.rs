//! Windows: copies of one buffer written by gather writes (`WriteFileGather`), every element a
//! page of that one buffer, so the device has a call's copies in flight together, as pwritev(2)
//! gives it on Linux and macOS (`crate::file`), without a buffer the size of the write. A 256 MiB
//! file written whole on the CI runners took 1.7–7.1 s one 128 KiB write at a time and 1.0–1.5 s
//! by 1 MiB writes or larger (mantle `docs/research/48-first-writes.md` §6).
//!
//! `WriteFileGather` takes a handle opened past the cache and for overlapped I/O, an element for
//! each system page of the write, each page-aligned, ended by a null element, and a length that is
//! a multiple of the volume's sector (Microsoft, "WriteFileGather function").
#![allow(unsafe_code)]

use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::Path;

use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, GetLastError};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_NO_BUFFERING, FILE_FLAG_OVERLAPPED, FILE_SEGMENT_ELEMENT, WriteFileGather,
};
use windows_sys::Win32::System::IO::{GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};

fn invalid() -> io::Error {
    io::Error::from(io::ErrorKind::InvalidInput)
}

/// The system's page, which each gather element names.
fn page() -> io::Result<usize> {
    let mut info = SYSTEM_INFO::default();
    // SAFETY: GetSystemInfo writes the SYSTEM_INFO it is given and nothing else; `info` is a
    // live, writable SYSTEM_INFO for the call's duration.
    unsafe { GetSystemInfo(&raw mut info) };
    usize::try_from(info.dwPageSize)
        .ok()
        .filter(|&p| p > 0)
        .ok_or_else(invalid)
}

/// Writes `copies` copies of `fill` back to back from `offset` of the file at `path`, through a
/// handle of its own opened past the cache for overlapped I/O, in gather writes of as many copies
/// as an element list no larger than `fill` names (an element is 8 bytes and names a page): a
/// 128 KiB fill writes 64 MiB a call. `fill` is page-aligned and a whole number of pages.
pub(crate) fn write_copies(path: &Path, fill: &[u8], offset: u64, copies: u64) -> io::Result<()> {
    let page = page()?;
    if fill.is_empty()
        || !fill.len().is_multiple_of(page)
        || !fill.as_ptr().addr().is_multiple_of(page)
    {
        return Err(invalid());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED)
        .open(path)?;
    let pages_a_copy = fill.len().checked_div(page).ok_or_else(invalid)?;
    let element = std::mem::size_of::<FILE_SEGMENT_ELEMENT>();
    let copies_a_call = fill
        .len()
        .checked_div(element)
        .and_then(|n| n.checked_div(pages_a_copy))
        .filter(|&n| n > 0)
        .ok_or_else(invalid)?;
    // One call's elements and the null that ends them, made once and cut short for the last call.
    let mut elements: Vec<FILE_SEGMENT_ELEMENT> = Vec::new();
    let most = copies_a_call
        .checked_mul(pages_a_copy)
        .and_then(|n| n.checked_add(1))
        .ok_or_else(invalid)?;
    elements
        .try_reserve_exact(most)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    let piece = u64::try_from(fill.len()).map_err(|_| invalid())?;
    let mut at = offset;
    let mut left = copies;
    while left > 0 {
        let now = usize::try_from(left).map_or(copies_a_call, |l| l.min(copies_a_call));
        elements.clear();
        for _ in 0..now {
            for p in 0..pages_a_copy {
                let page_at = fill
                    .get(p.checked_mul(page).ok_or_else(invalid)?..)
                    .ok_or_else(invalid)?;
                elements.push(FILE_SEGMENT_ELEMENT {
                    Buffer: page_at.as_ptr().cast_mut().cast(),
                });
            }
        }
        elements.push(FILE_SEGMENT_ELEMENT {
            Buffer: std::ptr::null_mut(),
        });
        let bytes = u64::try_from(now)
            .ok()
            .and_then(|n| n.checked_mul(piece))
            .ok_or_else(invalid)?;
        let len = u32::try_from(bytes).map_err(|_| invalid())?;
        let mut overlapped = OVERLAPPED::default();
        overlapped.Anonymous.Anonymous.Offset =
            u32::try_from(at & u64::from(u32::MAX)).map_err(|_| invalid())?;
        overlapped.Anonymous.Anonymous.OffsetHigh =
            u32::try_from(at.checked_shr(32).ok_or_else(invalid)?).map_err(|_| invalid())?;
        let handle = file.as_raw_handle();
        // SAFETY: `handle` is `file`'s, opened for overlapped writes past the cache, and outlives
        // the call and the wait below; `elements` names `len` bytes as page-aligned system pages
        // of `fill`, ended by a null element, as the call requires, and neither `elements`,
        // `fill` nor `overlapped` is touched or freed until GetOverlappedResult has waited for
        // the write to finish. The reserved argument is null, as documented.
        let started = unsafe {
            WriteFileGather(
                handle,
                elements.as_ptr(),
                len,
                std::ptr::null(),
                &raw mut overlapped,
            )
        };
        // SAFETY: reads the calling thread's last error, set by the call just made.
        if started == 0 && unsafe { GetLastError() } != ERROR_IO_PENDING {
            return Err(io::Error::last_os_error());
        }
        let mut wrote = 0u32;
        // SAFETY: waits (bWait true) for the write started on `handle` with `overlapped`, which
        // stay live until it returns; `wrote` is a live u32 it fills.
        let done = unsafe { GetOverlappedResult(handle, &raw const overlapped, &raw mut wrote, 1) };
        if done == 0 {
            return Err(io::Error::last_os_error());
        }
        if wrote != len {
            return Err(io::ErrorKind::WriteZero.into());
        }
        at = at.checked_add(bytes).ok_or_else(invalid)?;
        left = left.saturating_sub(u64::try_from(now).map_err(|_| invalid())?);
    }
    Ok(())
}
