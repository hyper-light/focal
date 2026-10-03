//! macOS: the process's thread count from `proc_pidinfo(PROC_PIDTASKINFO)` (<libproc.h>,
//! <sys/proc_info.h>), the pool ceiling from the `kern.wq_max_threads` sysctl, the workqueue's
//! own cap on the threads it runs for one process (XNU `pthread_workqueue.c`, research/26
//! §1.5), and a thread's CPU time from `CLOCK_THREAD_CPUTIME_ID` (clock_gettime(3)).
#![allow(unsafe_code)]

use std::ffi::{c_char, c_int, c_void};
use std::io;
use std::time::Duration;

unsafe extern "C" {
    fn sysctlbyname(
        name: *const c_char,
        oldp: *mut c_void,
        oldlenp: *mut usize,
        newp: *mut c_void,
        newlen: usize,
    ) -> c_int;
    fn proc_pidinfo(
        pid: c_int,
        flavor: c_int,
        arg: u64,
        buffer: *mut c_void,
        buffersize: c_int,
    ) -> c_int;
}

/// `PROC_PIDTASKINFO` of <sys/proc_info.h>.
const PROC_PIDTASKINFO: i32 = 4;

/// `struct proc_taskinfo` of <sys/proc_info.h>, field for field.
#[repr(C)]
#[derive(Default)]
struct TaskInfo {
    virtual_size: u64,
    resident_size: u64,
    total_user: u64,
    total_system: u64,
    threads_user: u64,
    threads_system: u64,
    policy: i32,
    faults: i32,
    pageins: i32,
    cow_faults: i32,
    messages_sent: i32,
    messages_received: i32,
    syscalls_mach: i32,
    syscalls_unix: i32,
    csw: i32,
    threadnum: i32,
    numrunning: i32,
    priority: i32,
}

pub(super) fn count() -> io::Result<usize> {
    let mut info = TaskInfo::default();
    let size = c_int::try_from(std::mem::size_of::<TaskInfo>())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let pid = c_int::try_from(std::process::id())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: `info` is a writable proc_taskinfo of `size` bytes that outlives the call, which
    // writes at most `size` bytes and returns how many it wrote.
    let wrote = unsafe { proc_pidinfo(pid, PROC_PIDTASKINFO, 0, (&raw mut info).cast(), size) };
    if wrote != size {
        return Err(io::Error::last_os_error());
    }
    usize::try_from(info.threadnum).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "the kernel reports a negative thread count",
        )
    })
}

pub(super) fn ceiling() -> io::Result<usize> {
    let mut value: c_int = 0;
    let mut len = std::mem::size_of::<c_int>();
    // SAFETY: the name is a NUL-terminated literal; `value` is a writable int of `len` bytes
    // that outlives the call, which writes at most `len` bytes and sets `len` to what it wrote;
    // nothing is set, so the new value is null and its length zero.
    let ok = unsafe {
        sysctlbyname(
            c"kern.wq_max_threads".as_ptr(),
            (&raw mut value).cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if ok != 0 {
        return Err(io::Error::last_os_error());
    }
    if len != std::mem::size_of::<c_int>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "kern.wq_max_threads is not an int",
        ));
    }
    usize::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "kern.wq_max_threads is negative",
        )
    })
}

pub(super) fn thread_cpu() -> io::Result<Duration> {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    let secs = u64::try_from(t.tv_sec).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    let nanos =
        u32::try_from(t.tv_nsec).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    Ok(Duration::new(secs, nanos))
}
