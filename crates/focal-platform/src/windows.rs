//! The single audited `unsafe` file (decision 11, doc 10). Every call here
//! carries a safety argument; nothing else under `crates/` uses `unsafe`
//! (`scripts/check-contracts.py` enforces the boundary).
#![allow(unsafe_code)]
use std::{ffi::OsStr, os::windows::ffi::OsStrExt, path::Path};
use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

/// A path as a NUL-terminated UTF-16 sequence, the encoding the wide Win32
/// entry points take.
fn wide(path: &Path) -> Vec<u16> {
    OsStr::new(path)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

/// Bytes available to the caller on the volume that holds `path`.
pub(crate) fn available_space(path: &Path) -> Option<u64> {
    let wide = wide(path);
    let mut available: u64 = 0;
    // SAFETY: `wide` is a NUL-terminated UTF-16 buffer that outlives the
    // call; `available` is a live, aligned `u64` we pass by pointer for the
    // one out-parameter we read, and null for the two totals we do not need.
    // GetDiskFreeSpaceExW writes only through the non-null pointer and returns
    // zero on failure without touching it, so an error leaves `available`
    // at its initialised zero, which we discard.
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if ok == 0 { None } else { Some(available) }
}
