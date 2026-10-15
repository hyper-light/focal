//! The process's threads as the operating system counts them, and the budget every pool of
//! blocking workers draws from (docs/design/node.md §1.2; measurement.md §8).
//!
//! The thread count the OS will create is not the limit that matters: thousands of threads
//! waiting on one condition variable panicked macOS long before the OS refused one
//! (research/26 §1.4). So a pool's threads are drawn from one process budget, decided before any
//! thread starts, whose ceiling is the OS's own statement of what one process may run: on macOS
//! `kern.wq_max_threads`, the cap Apple's workqueue under GCD applies to itself; on Linux the
//! smaller of `kernel.threads-max` and the soft `RLIMIT_NPROC`; on Windows the 500 worker
//! threads Microsoft states as a thread pool's default maximum (research/26 §1.5, §2.5). A
//! request past what is left is refused, naming the device and the threads asked for.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::DiskError;

#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;
#[cfg(any(target_os = "linux", target_os = "android"))]
use linux as platform;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
use macos as platform;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as platform;

/// Where the OS states nothing this code reads.
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    windows
)))]
mod platform {
    use std::io;
    use std::time::Duration;

    fn unsupported() -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "no thread interface for this operating system",
        )
    }

    pub fn count() -> io::Result<usize> {
        Err(unsupported())
    }

    pub fn ceiling() -> io::Result<usize> {
        Err(unsupported())
    }

    pub fn thread_cpu() -> io::Result<Duration> {
        Err(unsupported())
    }
}

/// Pool threads drawn from the budget and not yet given back. A draw adds its threads and takes
/// them back if they passed the ceiling, one atomic step each way, so no draw waits for another;
/// one that passes while another's excess is being taken back is refused, the safe direction.
static DRAWN: AtomicUsize = AtomicUsize::new(0);

fn os(op: &'static str, source: std::io::Error) -> DiskError {
    DiskError::Io {
        op,
        path: std::path::PathBuf::new(),
        source,
    }
}

/// The threads this process runs, as the operating system counts them.
pub fn count() -> Result<usize, DiskError> {
    platform::count().map_err(|e| os("count the process's threads", e))
}

/// The most pool threads the process may run at once: the OS's own figure for one process.
pub fn ceiling() -> Result<usize, DiskError> {
    platform::ceiling().map_err(|e| os("read the process's thread ceiling", e))
}

/// The CPU time the calling thread has used, so a load generator can say whether it, rather
/// than the system it drives, was the bottleneck (docs/design/measurement.md §10).
pub fn thread_cpu() -> Result<Duration, DiskError> {
    platform::thread_cpu().map_err(|e| os("read the thread's CPU time", e))
}

/// Pool threads the budget has left.
pub fn left() -> Result<usize, DiskError> {
    Ok(ceiling()?.saturating_sub(DRAWN.load(Ordering::Acquire)))
}

/// Threads drawn from the budget, given back when dropped.
#[derive(Debug)]
pub struct Reservation {
    threads: usize,
}

impl Reservation {
    /// The threads the reservation holds.
    pub fn threads(&self) -> usize {
        self.threads
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        give_back(self.threads);
    }
}

/// Takes `threads` back from the budget: a reservation's own, never more than were drawn.
fn give_back(threads: usize) {
    DRAWN.fetch_sub(threads, Ordering::AcqRel);
}

/// Draws `threads` from the budget for a pool serving `path`, before any of them starts;
/// [`DiskError::Threads`] when the budget has fewer left.
pub fn reserve(threads: usize, path: &Path) -> Result<Reservation, DiskError> {
    let ceiling = ceiling()?;
    let refused = |left: usize| DiskError::Threads {
        path: path.to_path_buf(),
        asked: threads,
        left,
        ceiling,
    };
    if threads > ceiling {
        return Err(refused(left()?));
    }
    // Each draw added is at most the ceiling, which the OS states far below usize::MAX, and
    // every draw past it is taken back at once: the sum never wraps.
    let before = DRAWN.fetch_add(threads, Ordering::AcqRel);
    let left = ceiling.saturating_sub(before);
    if threads > left {
        give_back(threads);
        return Err(refused(left));
    }
    Ok(Reservation { threads })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_os_counts_this_process_and_states_a_ceiling() {
        let before = count().unwrap();
        assert!(before >= 1);
        // A thread parked on its own is counted while it lives.
        std::thread::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            s.spawn(move || rx.recv());
            // Other tests start and end threads meanwhile; this one and the caller are here.
            assert!(count().unwrap() >= 2);
            drop(tx);
        });
        assert!(ceiling().unwrap() >= 1);
        thread_cpu().unwrap();
    }

    /// A draw past what is left is refused and draws nothing; a reservation dropped gives its
    /// threads back.
    #[test]
    fn a_draw_past_the_budget_is_refused() {
        let path = Path::new("/dev/null");
        let ceiling = ceiling().unwrap();
        let err = reserve(ceiling + 1, path).unwrap_err();
        assert!(matches!(err, DiskError::Threads { asked, .. } if asked == ceiling + 1));
        let one = reserve(1, path).unwrap();
        assert_eq!(one.threads(), 1);
        drop(one);
        assert!(left().unwrap() <= ceiling);
    }
}
