//! Windows: the process's thread count from a ToolHelp snapshot of the system's threads
//! (`CreateToolhelp32Snapshot` with `TH32CS_SNAPTHREAD`, then `Thread32First`/`Thread32Next`),
//! a thread's CPU time from `GetThreadTimes`, and the pool ceiling Microsoft states for a thread
//! pool. Windows has no per-process thread limit but virtual memory ("The number of threads a
//! process can create is limited by the available virtual memory", `CreateThread`, Remarks).
#![allow(unsafe_code)]

use std::io;
use std::time::Duration;

use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcessId, GetCurrentThread, GetThreadTimes,
};

/// "By default, each thread pool has a maximum of 500 worker threads" (Microsoft, "Thread
/// Pools", Best Practices): the vendor's statement of what one process's pool runs, as
/// `kern.wq_max_threads` is Apple's (research/26 §1.5).
const POOL_THREADS: usize = 500;

/// One `FILETIME` unit, 100 ns (`FILETIME`, Remarks).
const FILETIME_NANOS: u64 = 100;

pub(super) fn ceiling() -> io::Result<usize> {
    Ok(POOL_THREADS)
}

pub(super) fn count() -> io::Result<usize> {
    // SAFETY: no pointers are passed; the result is a handle this code closes, or
    // INVALID_HANDLE_VALUE.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let size = u32::try_from(std::mem::size_of::<THREADENTRY32>())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let mut entry = THREADENTRY32 {
        dwSize: size,
        cntUsage: 0,
        th32ThreadID: 0,
        th32OwnerProcessID: 0,
        tpBasePri: 0,
        tpDeltaPri: 0,
        dwFlags: 0,
    };
    // SAFETY: takes no arguments and cannot fail.
    let us = unsafe { GetCurrentProcessId() };
    let mut threads = 0usize;
    // SAFETY: `snapshot` is a live snapshot handle and `entry` a writable THREADENTRY32 whose
    // `dwSize` is set, as the call requires.
    let mut more = unsafe { Thread32First(snapshot, &mut entry) } != 0;
    // The snapshot is a fixed list taken once, so the walk ends with it.
    while more {
        if entry.th32OwnerProcessID == us {
            threads = threads.saturating_add(1);
        }
        // SAFETY: as for `Thread32First`.
        more = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
    }
    // SAFETY: `snapshot` is a handle this code owns and closes once.
    unsafe { CloseHandle(snapshot) };
    Ok(threads)
}

pub(super) fn thread_cpu() -> io::Result<Duration> {
    let zero = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let (mut created, mut exited, mut kernel, mut user) = (zero, zero, zero, zero);
    // SAFETY: GetCurrentThread returns a pseudo-handle that needs no closing; the four outputs
    // are writable FILETIMEs that outlive the call.
    let ok = unsafe {
        GetThreadTimes(
            GetCurrentThread(),
            &mut created,
            &mut exited,
            &mut kernel,
            &mut user,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    let units = |t: FILETIME| (u64::from(t.dwHighDateTime) << 32) | u64::from(t.dwLowDateTime);
    let nanos = units(kernel)
        .checked_add(units(user))
        .and_then(|u| u.checked_mul(FILETIME_NANOS))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "a CPU time past u64"))?;
    Ok(Duration::from_nanos(nanos))
}
