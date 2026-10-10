//! A shard thread's CPU clock, readable from any thread of the process (§4.14: an observation's budget is counted in
//! the observed shard's own time). A shard records a handle to its own clock as it starts
//! ([`crate::registry::record_cpu_clock`]); an observer reads it ([`crate::registry::shard_cpu`]) to tell a starved
//! shard, which keeps consuming CPU, from a wedged one, which consumes none. Measured 2026-10-03 in a Linux container
//! under 108 CPU burners on 18 cores: shards whose observations timed out were runnable (state R, preempted), not
//! blocked, and the 10 s wall budget read their starvation as a fault
//! (`docs/bugs/2026-10-03-an-observation-read-a-starved-shard-as-wedged.md`).
//!
//! A reading carries the CPU time the thread has run ([`CpuReading::spent_ns`], what the budget is charged) and a count
//! that grows whenever it runs at all ([`CpuReading::progress`], what tells it ran during a window):
//! - Linux: `pthread_getcpuclockid` of the calling thread, read with `clock_gettime` (any thread may read it); the
//!   progress is the time itself.
//! - macOS: the calling thread's Mach port (`pthread_mach_thread_np`), read with `thread_info(THREAD_BASIC_INFO)` (user
//!   plus system time); the progress is the time itself.
//! - Windows: the calling thread's id, opened for each read with query rights only. The time is `GetThreadTimes`'
//!   kernel plus user time, which the scheduler charges a tick at a time (15.6 ms by default), so a thread that ran less
//!   than a tick in a window may show none; the progress is `QueryThreadCycleTime`, the cycles the thread has run,
//!   exact [B: Microsoft Learn, "QueryThreadCycleTime", "GetThreadTimes"].
//! - Miri: no clock; the observation keeps its wall budget there.
//!
//! The handle is a `u64`, nonzero when recorded.
#![allow(unsafe_code)]

/// One reading of a thread's CPU clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuReading {
    /// The CPU time the thread has run, nanoseconds.
    pub spent_ns: u64,
    /// A count that grows whenever the thread runs (its CPU time on Unix, its cycles on Windows).
    pub progress: u64,
}

impl CpuReading {
    /// A reading whose progress is its time (Unix).
    #[cfg(all(unix, not(miri)))]
    fn of_time(spent_ns: u64) -> CpuReading {
        CpuReading {
            spent_ns,
            progress: spent_ns,
        }
    }
}

/// The calling thread's CPU-clock handle, nonzero, or `None` where none can be taken.
#[cfg(all(target_os = "linux", not(miri)))]
pub(crate) fn current() -> Option<u64> {
    let mut clock: libc::clockid_t = 0;
    // SAFETY: `pthread_self` names the calling thread, alive for the call; `clock` is this frame's own value, written
    // by the call, and the status is checked before it is read.
    let status = unsafe { libc::pthread_getcpuclockid(libc::pthread_self(), &mut clock) };
    if status != 0 {
        return None;
    }
    // A thread CPU clock id is negative on Linux; carried as its bits, tagged so zero never names one.
    Some(u64::from(clock.cast_unsigned()) | TAG)
}

/// The CPU clock of the thread `handle` names.
#[cfg(all(target_os = "linux", not(miri)))]
pub(crate) fn read(handle: u64) -> Option<CpuReading> {
    if handle & TAG == 0 {
        return None;
    }
    let clock = u32::try_from(handle & !TAG).ok()?.cast_signed();
    let mut reading = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `reading` is this frame's own value, written by the call; a clock id of a thread that has exited is
    // refused by the kernel (`EINVAL`), which the status check turns into `None`.
    let status = unsafe { libc::clock_gettime(clock, &mut reading) };
    if status != 0 {
        return None;
    }
    nanos(
        u64::try_from(reading.tv_sec).ok()?,
        u64::try_from(reading.tv_nsec).ok()?,
    )
    .map(CpuReading::of_time)
}

/// The calling thread's CPU-clock handle: its Mach port, tagged.
#[cfg(all(target_os = "macos", not(miri)))]
pub(crate) fn current() -> Option<u64> {
    // SAFETY: `pthread_self` names the calling thread, alive for the call; the answer is the thread's Mach port name,
    // valid in this task for the thread's life, and no right is taken.
    let port = unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) };
    (port != 0).then(|| u64::from(port) | TAG)
}

/// The CPU clock (user and system time) of the thread `handle` names.
#[cfg(all(target_os = "macos", not(miri)))]
pub(crate) fn read(handle: u64) -> Option<CpuReading> {
    /// Format: nanoseconds per microsecond (`time_value_t` counts seconds and microseconds).
    const NS_PER_MICRO: u64 = 1_000;
    /// Format: microseconds per second.
    const MICROS_PER_SECOND: u64 = 1_000_000;
    if handle & TAG == 0 {
        return None;
    }
    let port = libc::mach_port_t::try_from(handle & !TAG).ok()?;
    // SAFETY: an all-zero `thread_basic_info` is a valid value of the C struct (integers and `time_value_t`s).
    let mut info: libc::thread_basic_info = unsafe { std::mem::zeroed() };
    let mut count = libc::THREAD_BASIC_INFO_COUNT;
    // SAFETY: `info` is this frame's own value, `count` words long as `THREAD_BASIC_INFO` requires; a port of a thread
    // that has exited is refused (`KERN_*`), which the status check turns into `None`.
    let status = unsafe {
        libc::thread_info(
            port,
            libc::THREAD_BASIC_INFO as libc::thread_flavor_t,
            (&raw mut info).cast(),
            &mut count,
        )
    };
    if status != libc::KERN_SUCCESS {
        return None;
    }
    let micros = |time: libc::time_value_t| {
        u64::try_from(time.seconds)
            .ok()?
            .checked_mul(MICROS_PER_SECOND)?
            .checked_add(u64::try_from(time.microseconds).ok()?)
    };
    micros(info.user_time)?
        .checked_add(micros(info.system_time)?)?
        .checked_mul(NS_PER_MICRO)
        .map(CpuReading::of_time)
}

/// The calling thread's CPU-clock handle: its thread id, tagged.
#[cfg(all(windows, not(miri)))]
pub(crate) fn current() -> Option<u64> {
    // SAFETY: takes no argument and answers the calling thread's id; no OS thread has id zero.
    let id = unsafe { windows_sys::Win32::System::Threading::GetCurrentThreadId() };
    (id != 0).then(|| u64::from(id) | TAG)
}

/// The CPU clock of the thread `handle` names: opened by id with query rights for this read only, then closed.
#[cfg(all(windows, not(miri)))]
pub(crate) fn read(handle: u64) -> Option<CpuReading> {
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE};
    use windows_sys::Win32::System::Threading::{
        GetThreadTimes, OpenThread, THREAD_QUERY_LIMITED_INFORMATION,
    };
    use windows_sys::Win32::System::WindowsProgramming::QueryThreadCycleTime;

    /// The opened thread, closed when the read ends however it ends.
    struct Opened(HANDLE);
    impl Drop for Opened {
        fn drop(&mut self) {
            // SAFETY: the handle is this value's own, opened by `OpenThread` and closed only here.
            unsafe { CloseHandle(self.0) };
        }
    }

    /// Format: nanoseconds per `FILETIME` unit (100 ns).
    const NS_PER_FILETIME_UNIT: u64 = 100;
    if handle & TAG == 0 {
        return None;
    }
    let id = u32::try_from(handle & !TAG).ok()?;
    // SAFETY: opens a thread of this process by id with query rights only; a thread that has exited is refused with a
    // null handle, checked before use.
    let thread = unsafe { OpenThread(THREAD_QUERY_LIMITED_INFORMATION, 0, id) };
    if thread.is_null() {
        return None;
    }
    let thread = Opened(thread);
    let unset = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let (mut created, mut exited, mut kernel, mut user) = (unset, unset, unset, unset);
    // SAFETY: a live handle with query rights (`GetThreadTimes` accepts the limited right), and four of this frame's own
    // values for the call to write; the status is checked before they are read.
    let timed =
        unsafe { GetThreadTimes(thread.0, &mut created, &mut exited, &mut kernel, &mut user) };
    let mut cycles = 0u64;
    // SAFETY: the same live handle (`QueryThreadCycleTime` accepts the limited right) and this frame's own count; the
    // status is checked before it is read.
    let cycled = unsafe { QueryThreadCycleTime(thread.0, &mut cycles) };
    if timed == 0 || cycled == 0 {
        return None;
    }
    let units =
        |time: FILETIME| (u64::from(time.dwHighDateTime) << 32) | u64::from(time.dwLowDateTime);
    Some(CpuReading {
        spent_ns: units(kernel)
            .checked_add(units(user))?
            .checked_mul(NS_PER_FILETIME_UNIT)?,
        progress: cycles,
    })
}

/// No clock here.
#[cfg(any(not(any(target_os = "linux", target_os = "macos", windows)), miri))]
pub(crate) fn current() -> Option<u64> {
    None
}

/// No clock here.
#[cfg(any(not(any(target_os = "linux", target_os = "macos", windows)), miri))]
pub(crate) fn read(_handle: u64) -> Option<CpuReading> {
    None
}

/// Format: the bit marking a recorded handle, so a zero word is never mistaken for one.
#[cfg(all(any(target_os = "linux", target_os = "macos", windows), not(miri)))]
const TAG: u64 = 1 << 63;

/// Seconds and nanoseconds as nanoseconds.
#[cfg(all(target_os = "linux", not(miri)))]
fn nanos(seconds: u64, nanos: u64) -> Option<u64> {
    /// Format: nanoseconds per second.
    const NS_PER_SECOND: u64 = 1_000_000_000;
    seconds.checked_mul(NS_PER_SECOND)?.checked_add(nanos)
}

#[cfg(test)]
mod tests {
    /// §4.14. Do: take the calling thread's clock handle, spin a while, and read it from another thread. Expect: a
    /// reading on every platform with a clock (Linux, macOS, Windows), whose progress grew with the spin and whose time
    /// did not go back.
    #[test]
    fn a_threads_cpu_clock_reads_from_another_thread_and_grows_as_it_runs() {
        if cfg!(miri) {
            eprintln!("skipped: no thread CPU clock under Miri");
            return;
        }
        let handle = super::current();
        assert!(handle.is_some(), "a thread CPU clock on this platform");
        let Some(handle) = handle else { return };
        let before = std::thread::scope(|scope| scope.spawn(|| super::read(handle)).join())
            .ok()
            .flatten();
        let mut spin = 0u64;
        let started = std::time::Instant::now();
        while started.elapsed() < std::time::Duration::from_millis(20) {
            spin = std::hint::black_box(spin.wrapping_add(1));
        }
        let after = std::thread::scope(|scope| scope.spawn(|| super::read(handle)).join())
            .ok()
            .flatten();
        assert!(
            matches!((before, after), (Some(b), Some(a)) if a.progress > b.progress && a.spent_ns >= b.spent_ns),
            "{before:?} → {after:?}"
        );
    }
}
