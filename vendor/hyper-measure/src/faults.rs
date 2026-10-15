//! The page faults the operating system has charged this process, read from
//! the OS itself.
//!
//! - Linux and macOS: `getrusage(RUSAGE_SELF)`, whose `ru_minflt` counts the
//!   faults served without I/O and `ru_majflt` those that needed it
//!   (getrusage(2): POSIX.1-2008; Linux man-pages; macOS `man 2 getrusage`).
//! - macOS also: `task_info(mach_task_self(), TASK_EVENTS_INFO)`, whose
//!   `task_events_info` counts every fault the Mach VM layer took (`faults`),
//!   the faults that read a page in (`pageins`) and the copy-on-write faults
//!   (`cow_faults`). The structure and flavor number are xnu's
//!   `osfmk/mach/task_info.h` (`TASK_EVENTS_INFO` is 2; eight `integer_t`).
//!   libc does not declare them, so they are declared here.
//! - Windows: `GetProcessMemoryInfo`'s `PageFaultCount`, which counts soft and
//!   hard faults together (Win32 `PROCESS_MEMORY_COUNTERS`); the split is not
//!   reported, so `major` is `None`.
//!
//! A count is cumulative since the process began; a measurement takes the
//! difference of two reads ([`Faults::since`]).
#![allow(unsafe_code)]

/// The OS would not say.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultsError {
    /// The call failed; the number is the OS's error or return code.
    Call(i64),
    /// A count the OS gave did not fit the type it is kept in.
    Range,
    /// This operating system offers no count this crate reads.
    Unsupported,
}

impl std::fmt::Display for FaultsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Call(code) => write!(f, "the OS refused to count faults: {code}"),
            Self::Range => f.write_str("a fault count out of range"),
            Self::Unsupported => f.write_str("no fault count on this OS"),
        }
    }
}

impl std::error::Error for FaultsError {}

/// What the Mach VM layer counted for this task (macOS only).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TaskEvents {
    /// Every page fault.
    pub faults: u64,
    /// Faults that read a page in.
    pub pageins: u64,
    /// Copy-on-write faults.
    pub cow_faults: u64,
}

/// The faults charged to this process so far, or between two reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Faults {
    /// Faults served without I/O (Unix), or every fault (Windows).
    pub minor: u64,
    /// Faults that needed I/O; `None` where the OS does not split them.
    pub major: Option<u64>,
    /// The Mach task's own counts, on macOS.
    pub task: Option<TaskEvents>,
}

impl Faults {
    /// What was charged between `earlier` and `self`. The Mach counts are 32
    /// bits wide in the kernel and wrap there, so their difference is taken
    /// at that width.
    pub fn since(&self, earlier: &Self) -> Self {
        let word = |count: u64| u32::try_from(count & u64::from(u32::MAX)).unwrap_or(0);
        let mach = |now: u64, then: u64| u64::from(word(now).wrapping_sub(word(then)));
        Self {
            minor: self.minor.saturating_sub(earlier.minor),
            major: self
                .major
                .zip(earlier.major)
                .map(|(now, then)| now.saturating_sub(then)),
            task: self.task.zip(earlier.task).map(|(now, then)| TaskEvents {
                faults: mach(now.faults, then.faults),
                pageins: mach(now.pageins, then.pageins),
                cow_faults: mach(now.cow_faults, then.cow_faults),
            }),
        }
    }
    /// Minor and major together: every fault the process was charged.
    pub fn total(&self) -> u64 {
        self.minor.saturating_add(self.major.unwrap_or(0))
    }
}

/// The faults charged to this process since it began.
pub fn read() -> Result<Faults, FaultsError> {
    os::read()
}

/// The context switches charged to this process since it began, voluntary and involuntary
/// together: every time one of its threads gave up a core, by waiting or by being preempted
/// (getrusage(2)'s `ru_nvcsw` and `ru_nivcsw`). A thread woken by another and put to sleep again
/// counts once, so the difference of two reads counts the hand-offs between threads. Windows
/// keeps the count per thread only: `Unsupported` there.
pub fn switches() -> Result<u64, FaultsError> {
    os::switches()
}

#[cfg(unix)]
mod os {
    use super::{Faults, FaultsError};

    fn count(value: libc::c_long) -> Result<u64, FaultsError> {
        u64::try_from(value).map_err(|_| FaultsError::Range)
    }

    pub(super) fn switches() -> Result<u64, FaultsError> {
        let usage = usage()?;
        count(usage.ru_nvcsw)?
            .checked_add(count(usage.ru_nivcsw)?)
            .ok_or(FaultsError::Range)
    }

    fn usage() -> Result<libc::rusage, FaultsError> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        // SAFETY: `usage` is a writable `rusage` that lives across the call,
        // and `RUSAGE_SELF` is a valid `who` (getrusage(2)); the call writes
        // at most one `rusage`.
        let code = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if code != 0 {
            return Err(FaultsError::Call(i64::from(
                std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(code),
            )));
        }
        // SAFETY: getrusage returned 0, so it filled `usage`; and a zeroed
        // `rusage` is a valid one in any case, all its fields being integers.
        Ok(unsafe { usage.assume_init() })
    }

    pub(super) fn read() -> Result<Faults, FaultsError> {
        let usage = usage()?;
        Ok(Faults {
            minor: count(usage.ru_minflt)?,
            major: Some(count(usage.ru_majflt)?),
            task: task(),
        })
    }

    #[cfg(not(target_vendor = "apple"))]
    fn task() -> Option<super::TaskEvents> {
        None
    }

    /// `task_events_info` from xnu's `osfmk/mach/task_info.h`: eight
    /// `integer_t`.
    #[cfg(target_vendor = "apple")]
    #[repr(C)]
    #[derive(Default)]
    struct TaskEventsInfo {
        faults: libc::integer_t,
        pageins: libc::integer_t,
        cow_faults: libc::integer_t,
        messages_sent: libc::integer_t,
        messages_received: libc::integer_t,
        syscalls_mach: libc::integer_t,
        syscalls_unix: libc::integer_t,
        csw: libc::integer_t,
    }
    #[cfg(target_vendor = "apple")]
    unsafe extern "C" {
        /// The task's own port, which the runtime sets at start
        /// (`<mach/mach_init.h>`; libc's wrapper is deprecated in favour of
        /// declaring it).
        static mach_task_self_: libc::mach_port_t;
    }
    /// `TASK_EVENTS_INFO` in xnu's `osfmk/mach/task_info.h`.
    #[cfg(target_vendor = "apple")]
    const TASK_EVENTS_INFO: libc::task_flavor_t = 2;

    #[cfg(target_vendor = "apple")]
    fn task() -> Option<super::TaskEvents> {
        let mut info = TaskEventsInfo::default();
        // `TASK_EVENTS_INFO_COUNT`: the structure's size in `natural_t`s.
        let words = std::mem::size_of::<TaskEventsInfo>()
            .checked_div(std::mem::size_of::<libc::natural_t>())?;
        let mut count = libc::mach_msg_type_number_t::try_from(words).ok()?;
        // SAFETY: `mach_task_self_` is the task port the runtime set before
        // `main`, read by value and never written. `info` is a writable
        // `task_events_info` that lives across the call, and `count` says
        // its size in `natural_t`s, which bounds what the kernel writes
        // (task_info(3)).
        let code = unsafe {
            libc::task_info(
                mach_task_self_,
                TASK_EVENTS_INFO,
                std::ptr::from_mut(&mut info).cast::<libc::integer_t>(),
                &mut count,
            )
        };
        if code != libc::KERN_SUCCESS {
            return None;
        }
        // The kernel says how many words it wrote: anything but this structure's size means the
        // declaration above disagrees with the running kernel's, and its fields are not read.
        if usize::try_from(count).ok()? != words {
            return None;
        }
        // The kernel's counts are 32-bit and wrap; they are kept as the
        // unsigned words they are, so that `Faults::since` takes the
        // difference at that width.
        let word = |value: libc::integer_t| u64::from(value.cast_unsigned());
        Some(super::TaskEvents {
            faults: word(info.faults),
            pageins: word(info.pageins),
            cow_faults: word(info.cow_faults),
        })
    }
}

#[cfg(windows)]
mod os {
    use super::{Faults, FaultsError};

    pub(super) fn switches() -> Result<u64, FaultsError> {
        Err(FaultsError::Unsupported)
    }

    use windows_sys::Win32::System::{
        ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
        Threading::GetCurrentProcess,
    };

    pub(super) fn read() -> Result<Faults, FaultsError> {
        let size = u32::try_from(std::mem::size_of::<PROCESS_MEMORY_COUNTERS>())
            .map_err(|_| FaultsError::Range)?;
        // SAFETY: PROCESS_MEMORY_COUNTERS is plain integers, for which all zeroes is a valid
        // value; `cb` is then set to its size as the call requires.
        let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
        counters.cb = size;
        // SAFETY: `GetCurrentProcess` returns this process's pseudo-handle
        // and has no precondition; `counters` is a writable
        // `PROCESS_MEMORY_COUNTERS` that lives across the call and `size` is
        // its size, which bounds what the call writes.
        let done = unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, size) };
        if done == 0 {
            return Err(FaultsError::Call(i64::from(
                std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            )));
        }
        Ok(Faults {
            minor: u64::from(counters.PageFaultCount),
            major: None,
            task: None,
        })
    }
}

#[cfg(not(any(unix, windows)))]
mod os {
    use super::{Faults, FaultsError};

    pub(super) fn switches() -> Result<u64, FaultsError> {
        Err(FaultsError::Unsupported)
    }

    pub(super) fn read() -> Result<Faults, FaultsError> {
        Err(FaultsError::Unsupported)
    }
}
