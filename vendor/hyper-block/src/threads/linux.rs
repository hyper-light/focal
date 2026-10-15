//! Linux: the process's thread count from `Threads:` in /proc/self/status (proc_pid_status(5)),
//! the pool ceiling from the smaller of /proc/sys/kernel/threads-max, which the kernel sets so
//! that thread structures fill at most an eighth of RAM, and the soft `RLIMIT_NPROC`
//! (getrlimit(2); research/26 §1.5), and a thread's CPU time from `CLOCK_THREAD_CPUTIME_ID`
//! (clock_gettime(2)).

use std::io;
use std::time::Duration;

fn invalid(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}

pub(super) fn count() -> io::Result<usize> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:"))
        .and_then(|n| n.trim().parse().ok())
        .ok_or_else(|| invalid("/proc/self/status names no thread count"))
}

pub(super) fn ceiling() -> io::Result<usize> {
    let max: usize = std::fs::read_to_string("/proc/sys/kernel/threads-max")?
        .trim()
        .parse()
        .map_err(|_| invalid("kernel.threads-max is not a count"))?;
    let nproc = rustix::process::getrlimit(rustix::process::Resource::Nproc)
        .current
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(usize::MAX);
    Ok(max.min(nproc))
}

pub(super) fn thread_cpu() -> io::Result<Duration> {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    let secs = u64::try_from(t.tv_sec).map_err(|_| invalid("a negative CPU time"))?;
    let nanos = u32::try_from(t.tv_nsec).map_err(|_| invalid("a negative CPU time"))?;
    Ok(Duration::new(secs, nanos))
}
