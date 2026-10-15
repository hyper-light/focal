//! What the operating system charged a process: CPU time, instructions, cycles, energy, wakeups,
//! device writes and memory, read from the OS's own account of the process without privilege
//! (`docs/tails.md` §1a). The counts are cumulative since the process began; a measurement reads
//! the account before and after its window and takes the difference ([`Usage::since`]). They are
//! the whole process's: a measurement of one piece of work runs it with no other thread of the
//! process busy. A process other than this one is read by its pid ([`of`]), so a test can account
//! for the member processes it started.
//!
//! - macOS: `proc_pid_rusage(pid, RUSAGE_INFO_V6)` (`<libproc.h>`; `struct rusage_info_v6` is
//!   declared here from the SDK's `<sys/resource.h>`, since libc stops at V4). The times are Mach
//!   absolute-time units, converted to nanoseconds by `mach_timebase_info`
//!   (`<mach/mach_time.h>`); the energy is in nanojoules (`ri_energy_nj`); the wakeups are the
//!   package-idle exits and interrupt wakeups charged to the process; the footprint is the
//!   physical footprint the system charges it, now and at its highest.
//! - Linux: `/proc/<pid>/stat`'s `utime` and `stime` (proc(5)), in ticks of
//!   `sysconf(_SC_CLK_TCK)`; `/proc/<pid>/status`'s `VmRSS` and `VmHWM`; `/proc/<pid>/io`'s
//!   `write_bytes`, each read into a buffer on the stack, so the probe allocates nothing beside the
//!   allocation counts it is read with. Instructions and cycles (`perf_event_open`) and energy
//!   (powercap, per package, not per process) are not read: `None`.
//! - Other Unix: this process only, by `getrusage(RUSAGE_SELF)`: user and system times and
//!   `ru_maxrss`, the most resident memory, in KiB on the BSDs (getrusage(2)).
//! - Windows: `GetProcessTimes` (100 ns units), `QueryProcessCycleTime`, and
//!   `GetProcessMemoryInfo`'s `WorkingSetSize` and `PeakWorkingSetSize`, on this process's
//!   pseudo-handle or a handle `OpenProcess` gives with `PROCESS_QUERY_LIMITED_INFORMATION` and
//!   `PROCESS_VM_READ`, the rights those calls need. No instruction count, energy, wakeups or
//!   device writes (`GetProcessIoCounters` counts every I/O, the network's among it).
#![allow(unsafe_code)]

/// The OS would not say.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsageError {
    /// The call failed; the number is the OS's error or return code.
    Call(i64),
    /// The account could not be read or parsed, or a value did not fit.
    Unreadable,
    /// This operating system offers no account this crate reads, for this process or that one.
    Unsupported,
}

impl std::fmt::Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Call(code) => write!(f, "the OS refused the process's account: {code}"),
            Self::Unreadable => f.write_str("the process's account did not read"),
            Self::Unsupported => f.write_str("no process account on this OS"),
        }
    }
}

impl std::error::Error for UsageError {}

/// A process's account: cumulative since it began, or between two reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// CPU time in user mode, nanoseconds.
    pub user_ns: u64,
    /// CPU time in the kernel on the process's behalf, nanoseconds.
    pub system_ns: u64,
    /// Instructions retired, where the OS counts them.
    pub instructions: Option<u64>,
    /// Cycles, where the OS counts them.
    pub cycles: Option<u64>,
    /// Energy, nanojoules, where the OS estimates it per process.
    pub energy_nj: Option<u64>,
    /// Wakeups of an idle package and by interrupts charged to the process, where counted.
    pub wakeups: Option<u64>,
    /// Bytes the process had written to devices, where counted.
    pub disk_written: Option<u64>,
    /// The memory the system charges the process now, bytes (macOS's physical footprint, Linux's
    /// resident set, Windows's working set); a difference keeps the later read's.
    pub footprint: Option<u64>,
    /// The most memory the system has charged the process, bytes; a difference keeps the later
    /// read's, a high-water mark having no difference.
    pub peak_footprint: Option<u64>,
}

impl Usage {
    /// What was charged between `earlier` and `self`: counters differenced, the footprint and its
    /// peak as `self` read them. A counter that went backwards (another process under the pid)
    /// is `None`.
    pub fn since(&self, earlier: &Self) -> Self {
        let less = |now: Option<u64>, then: Option<u64>| {
            now.zip(then).and_then(|(now, then)| now.checked_sub(then))
        };
        Self {
            user_ns: self.user_ns.saturating_sub(earlier.user_ns),
            system_ns: self.system_ns.saturating_sub(earlier.system_ns),
            instructions: less(self.instructions, earlier.instructions),
            cycles: less(self.cycles, earlier.cycles),
            energy_nj: less(self.energy_nj, earlier.energy_nj),
            wakeups: less(self.wakeups, earlier.wakeups),
            disk_written: less(self.disk_written, earlier.disk_written),
            footprint: self.footprint,
            peak_footprint: self.peak_footprint,
        }
    }
    /// Counters summed over processes (a group's members); footprints summed, peaks summed (the
    /// most the group could have held at once, an upper bound). A value one side lacks is the
    /// other's, so a sum begun from [`Usage::default`] holds what its processes counted.
    pub fn plus(&self, other: &Self) -> Self {
        let sum = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(a), Some(b)) => a.checked_add(b),
            (one, None) | (None, one) => one,
        };
        Self {
            user_ns: self.user_ns.saturating_add(other.user_ns),
            system_ns: self.system_ns.saturating_add(other.system_ns),
            instructions: sum(self.instructions, other.instructions),
            cycles: sum(self.cycles, other.cycles),
            energy_nj: sum(self.energy_nj, other.energy_nj),
            wakeups: sum(self.wakeups, other.wakeups),
            disk_written: sum(self.disk_written, other.disk_written),
            footprint: sum(self.footprint, other.footprint),
            peak_footprint: sum(self.peak_footprint, other.peak_footprint),
        }
    }
    /// User and system time together.
    pub fn cpu_ns(&self) -> u64 {
        self.user_ns.saturating_add(self.system_ns)
    }
}

/// This process's account.
pub fn this() -> Result<Usage, UsageError> {
    of(std::process::id())
}

/// Process `pid`'s account (one of this user's).
pub fn of(pid: u32) -> Result<Usage, UsageError> {
    // Miri interprets no foreign call: under it the OS says nothing, as on an OS this reads none.
    if cfg!(miri) {
        return Err(UsageError::Unsupported);
    }
    os::of(pid)
}

/// The machine's one-minute load average, recorded beside every cost (`docs/tails.md` §2.1):
/// getloadavg(3) on Unix; `None` on Windows, which keeps no load average, or where the call fails.
pub fn load() -> Option<f64> {
    if cfg!(miri) {
        return None;
    }
    load_average()
}

#[cfg(unix)]
fn load_average() -> Option<f64> {
    let mut averages = [0f64; 1];
    // SAFETY: `averages` is a writable array of one `double` that lives across the call, and the
    // count passed is one, which bounds what getloadavg(3) writes.
    let filled = unsafe { libc::getloadavg(averages.as_mut_ptr(), 1) };
    if filled == 1 {
        averages.first().copied()
    } else {
        None
    }
}

#[cfg(not(unix))]
fn load_average() -> Option<f64> {
    None
}

#[cfg(target_os = "macos")]
mod os {
    use super::{Usage, UsageError};

    /// `RUSAGE_INFO_V6` (`<sys/resource.h>`).
    const RUSAGE_INFO_V6: libc::c_int = 6;

    /// `struct rusage_info_v6`, field for field from `<sys/resource.h>`.
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct RusageInfoV6 {
        uuid: [u8; 16],
        user_time: u64,
        system_time: u64,
        pkg_idle_wkups: u64,
        interrupt_wkups: u64,
        pageins: u64,
        wired_size: u64,
        resident_size: u64,
        phys_footprint: u64,
        proc_start_abstime: u64,
        proc_exit_abstime: u64,
        child_user_time: u64,
        child_system_time: u64,
        child_pkg_idle_wkups: u64,
        child_interrupt_wkups: u64,
        child_pageins: u64,
        child_elapsed_abstime: u64,
        diskio_bytesread: u64,
        diskio_byteswritten: u64,
        cpu_time_qos: [u64; 7],
        billed_system_time: u64,
        serviced_system_time: u64,
        logical_writes: u64,
        lifetime_max_phys_footprint: u64,
        instructions: u64,
        cycles: u64,
        billed_energy: u64,
        serviced_energy: u64,
        interval_max_phys_footprint: u64,
        runnable_time: u64,
        flags: u64,
        user_ptime: u64,
        system_ptime: u64,
        pinstructions: u64,
        pcycles: u64,
        energy_nj: u64,
        penergy_nj: u64,
        secure_time_in_system: u64,
        secure_ptime_in_system: u64,
        neural_footprint: u64,
        lifetime_max_neural_footprint: u64,
        interval_max_neural_footprint: u64,
        reserved: [u64; 9],
    }

    unsafe extern "C" {
        fn proc_pid_rusage(
            pid: libc::c_int,
            flavor: libc::c_int,
            buffer: *mut RusageInfoV6,
        ) -> libc::c_int;
    }

    /// Nanoseconds of `ticks` Mach absolute-time units.
    #[expect(
        deprecated,
        reason = "libc deprecates its Mach declarations in favour of the mach2 crate, which is not \
                  a dependency; the declarations themselves are unchanged in libc"
    )]
    fn nanos(ticks: u64) -> Result<u64, UsageError> {
        let mut base = libc::mach_timebase_info { numer: 0, denom: 0 };
        // SAFETY: `base` is a writable `mach_timebase_info` that lives across the call, which
        // writes one (<mach/mach_time.h>).
        let code = unsafe { libc::mach_timebase_info(&raw mut base) };
        if code != libc::KERN_SUCCESS || base.denom == 0 {
            return Err(UsageError::Call(i64::from(code)));
        }
        let scaled = u128::from(ticks)
            .checked_mul(u128::from(base.numer))
            .and_then(|product| product.checked_div(u128::from(base.denom)))
            .ok_or(UsageError::Unreadable)?;
        u64::try_from(scaled).map_err(|_| UsageError::Unreadable)
    }

    pub(super) fn of(pid: u32) -> Result<Usage, UsageError> {
        let pid = libc::c_int::try_from(pid).map_err(|_| UsageError::Unreadable)?;
        let mut info = std::mem::MaybeUninit::<RusageInfoV6>::zeroed();
        // SAFETY: `info` is a writable `rusage_info_v6`, laid out as `<sys/resource.h>` declares
        // it, that lives across the call; flavor `RUSAGE_INFO_V6` writes exactly one.
        let code = unsafe { proc_pid_rusage(pid, RUSAGE_INFO_V6, info.as_mut_ptr()) };
        if code != 0 {
            return Err(UsageError::Call(i64::from(
                std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(code),
            )));
        }
        // SAFETY: the call returned 0, so it filled `info`; a zeroed one is valid in any case,
        // every field an integer.
        let info = unsafe { info.assume_init() };
        // A process that has run in user mode has retired instructions, spent cycles and used
        // energy, so a zero count beside user time is a counter the kernel does not keep: a
        // virtual machine whose guest is given no performance counters (GitHub's macOS runners)
        // reports zeros, not the work. Such a count is absent, not zero.
        let counted = |count: u64| (count != 0 || info.user_time == 0).then_some(count);
        Ok(Usage {
            user_ns: nanos(info.user_time)?,
            system_ns: nanos(info.system_time)?,
            instructions: counted(info.instructions),
            cycles: counted(info.cycles),
            energy_nj: counted(info.energy_nj),
            wakeups: info.pkg_idle_wkups.checked_add(info.interrupt_wkups),
            disk_written: Some(info.diskio_byteswritten),
            footprint: Some(info.phys_footprint),
            peak_footprint: Some(info.lifetime_max_phys_footprint),
        })
    }
}

#[cfg(target_os = "linux")]
mod os {
    use std::io::Read;
    use std::os::unix::ffi::OsStrExt;

    use super::{Usage, UsageError};

    /// Shape: room for one of the three `/proc` files this reads, each a few hundred bytes to a couple of
    /// KiB (proc(5): `stat` is one line, `status` some fifty short lines, `io` seven); a file past it is
    /// refused rather than read in part.
    const FILE_BYTES: usize = 8192;
    /// Shape: room for `/proc/<pid>/status`, the longest path read: a `u32` in decimal is ten digits.
    const PATH_BYTES: usize = 32;

    /// `/proc/<pid>/<what>`, read into `buffer` without an allocation: a probe read beside the allocator's
    /// counts must not be counted in them (a read through `read_to_string` and `format!` was 7
    /// allocations and 18 reallocations a call, which `tests/service_driverless_receipts.rs` counted as
    /// the runtime's on Linux). The path is formatted on the stack, and std opens a path that short from
    /// a stack copy (`run_path_with_cstr`).
    fn read<'b>(
        pid: u32,
        what: &str,
        buffer: &'b mut [u8; FILE_BYTES],
    ) -> Result<&'b str, UsageError> {
        let mut path = [0u8; PATH_BYTES];
        let mut free: &mut [u8] = &mut path;
        std::io::Write::write_fmt(&mut free, format_args!("/proc/{pid}/{what}"))
            .map_err(|_| UsageError::Unreadable)?;
        let written = PATH_BYTES.saturating_sub(free.len());
        let path = path.get(..written).ok_or(UsageError::Unreadable)?;
        let mut file = std::fs::File::open(std::path::Path::new(std::ffi::OsStr::from_bytes(path)))
            .map_err(|_| UsageError::Unreadable)?;
        let mut filled = 0usize;
        // Each read fills at least a byte or ends the file; an interrupted one is asked again, at most
        // once a byte of the room.
        for _ in 0..FILE_BYTES {
            let room = buffer.get_mut(filled..).ok_or(UsageError::Unreadable)?;
            if room.is_empty() {
                return Err(UsageError::Unreadable);
            }
            match file.read(room) {
                Ok(0) => {
                    let bytes = buffer.get(..filled).ok_or(UsageError::Unreadable)?;
                    return std::str::from_utf8(bytes).map_err(|_| UsageError::Unreadable);
                }
                Ok(read) => filled = filled.saturating_add(read),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => return Err(UsageError::Unreadable),
            }
        }
        Err(UsageError::Unreadable)
    }

    /// The value of `key` in a `/proc` file of `key: value` lines, its first word.
    fn field(text: &str, key: &str) -> Option<u64> {
        text.lines()
            .find_map(|line| line.strip_prefix(key)?.strip_prefix(':'))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|value| value.parse().ok())
    }

    pub(super) fn of(pid: u32) -> Result<Usage, UsageError> {
        // SAFETY: sysconf reads a constant of the system and takes no pointer.
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        let ticks = u64::try_from(ticks)
            .ok()
            .filter(|ticks| *ticks > 0)
            .ok_or(UsageError::Unreadable)?;
        let nanos = |value: &str| -> Result<u64, UsageError> {
            let value: u64 = value.parse().map_err(|_| UsageError::Unreadable)?;
            value
                .checked_mul(1_000_000_000)
                .and_then(|scaled| scaled.checked_div(ticks))
                .ok_or(UsageError::Unreadable)
        };
        let mut buffer = [0u8; FILE_BYTES];
        let stat = read(pid, "stat", &mut buffer)?;
        // The command is in parentheses and may hold spaces: the fields follow its last ')'. utime and
        // stime are the stat's 14th and 15th fields; after the command, the 12th and 13th words
        // (proc(5)).
        let mut words = stat
            .rsplit_once(')')
            .map(|(_, rest)| rest)
            .ok_or(UsageError::Unreadable)?
            .split_whitespace();
        let user_ns = nanos(words.nth(11).ok_or(UsageError::Unreadable)?)?;
        let system_ns = nanos(words.next().ok_or(UsageError::Unreadable)?)?;
        let status = read(pid, "status", &mut buffer)?;
        let kib = |key: &str| field(status, key).and_then(|kib| kib.checked_mul(1024));
        let (footprint, peak_footprint) = (kib("VmRSS"), kib("VmHWM"));
        let written = read(pid, "io", &mut buffer)
            .ok()
            .and_then(|io| field(io, "write_bytes"));
        Ok(Usage {
            user_ns,
            system_ns,
            instructions: None,
            cycles: None,
            energy_nj: None,
            wakeups: None,
            disk_written: written,
            footprint,
            peak_footprint,
        })
    }
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
mod os {
    use super::{Usage, UsageError};

    fn nanos(time: libc::timeval) -> Result<u64, UsageError> {
        let seconds = u64::try_from(time.tv_sec).map_err(|_| UsageError::Unreadable)?;
        let micros = u64::try_from(time.tv_usec).map_err(|_| UsageError::Unreadable)?;
        seconds
            .checked_mul(1_000_000_000)
            .and_then(|ns| ns.checked_add(micros.checked_mul(1_000)?))
            .ok_or(UsageError::Unreadable)
    }

    pub(super) fn of(pid: u32) -> Result<Usage, UsageError> {
        if pid != std::process::id() {
            return Err(UsageError::Unsupported);
        }
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        // SAFETY: `usage` is a writable `rusage` that lives across the call, and `RUSAGE_SELF`
        // is a valid `who` (getrusage(2)); the call writes at most one `rusage`.
        let code = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if code != 0 {
            return Err(UsageError::Call(i64::from(
                std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(code),
            )));
        }
        // SAFETY: getrusage returned 0, so it filled `usage`; and a zeroed `rusage` is a valid
        // one in any case, all its fields being integers.
        let usage = unsafe { usage.assume_init() };
        // `ru_maxrss` is in KiB on the BSDs (getrusage(2)).
        let peak = u64::try_from(usage.ru_maxrss)
            .ok()
            .and_then(|kib| kib.checked_mul(1_024));
        Ok(Usage {
            user_ns: nanos(usage.ru_utime)?,
            system_ns: nanos(usage.ru_stime)?,
            instructions: None,
            cycles: None,
            energy_nj: None,
            wakeups: None,
            disk_written: None,
            footprint: None,
            peak_footprint: peak,
        })
    }
}

#[cfg(windows)]
mod os {
    use super::{Usage, UsageError};
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE};
    use windows_sys::Win32::System::{
        ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
        Threading::{
            GetCurrentProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
            PROCESS_VM_READ,
        },
        WindowsProgramming::QueryProcessCycleTime,
    };

    fn failed() -> UsageError {
        UsageError::Call(i64::from(
            std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
        ))
    }

    /// A `FILETIME` duration, in 100 ns units, in nanoseconds.
    fn nanos(time: &FILETIME) -> Result<u64, UsageError> {
        let units = u64::from(time.dwHighDateTime) << 32 | u64::from(time.dwLowDateTime);
        units.checked_mul(100).ok_or(UsageError::Unreadable)
    }

    /// A process handle: this process's pseudo-handle, which is never closed, or one
    /// `OpenProcess` gave, closed when dropped.
    struct Process {
        handle: HANDLE,
        opened: bool,
    }

    impl Process {
        fn of(pid: u32) -> Result<Self, UsageError> {
            if pid == std::process::id() {
                // SAFETY: `GetCurrentProcess` has no precondition and returns a pseudo-handle
                // that needs no closing.
                let handle = unsafe { GetCurrentProcess() };
                return Ok(Self {
                    handle,
                    opened: false,
                });
            }
            // SAFETY: `OpenProcess` takes plain values; it returns a handle the caller owns, or
            // null on failure.
            let handle =
                unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_VM_READ, 0, pid) };
            if handle.is_null() {
                return Err(failed());
            }
            Ok(Self {
                handle,
                opened: true,
            })
        }
    }

    impl Drop for Process {
        fn drop(&mut self) {
            if self.opened {
                // SAFETY: `handle` came from `OpenProcess`, is owned here alone and is closed
                // once. A failed close leaves nothing to undo.
                unsafe { CloseHandle(self.handle) };
            }
        }
    }

    pub(super) fn of(pid: u32) -> Result<Usage, UsageError> {
        let process = Process::of(pid)?;
        let zero = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let (mut created, mut exited, mut kernel, mut user) = (zero, zero, zero, zero);
        // SAFETY: `process.handle` is a valid process handle with the query right for as long as
        // `process` lives; each `FILETIME` is writable and lives across the call, which writes
        // one to each.
        let done = unsafe {
            GetProcessTimes(
                process.handle,
                &mut created,
                &mut exited,
                &mut kernel,
                &mut user,
            )
        };
        if done == 0 {
            return Err(failed());
        }
        let mut cycles = 0u64;
        // SAFETY: the handle as above; `cycles` is a writable `u64` that lives across the call,
        // which writes one.
        let counted = unsafe { QueryProcessCycleTime(process.handle, &mut cycles) };
        let size = u32::try_from(std::mem::size_of::<PROCESS_MEMORY_COUNTERS>())
            .map_err(|_| UsageError::Unreadable)?;
        // SAFETY: PROCESS_MEMORY_COUNTERS is plain integers, for which all zeroes is a valid
        // value; `cb` is then set to its size as the call requires.
        let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
        counters.cb = size;
        // SAFETY: the handle as above, with the VM-read right the call needs; `counters` is a
        // writable `PROCESS_MEMORY_COUNTERS` that lives across the call and `size` is its size,
        // which bounds what the call writes.
        let measured = unsafe { K32GetProcessMemoryInfo(process.handle, &mut counters, size) };
        let bytes = |size: usize| u64::try_from(size).ok();
        Ok(Usage {
            user_ns: nanos(&user)?,
            system_ns: nanos(&kernel)?,
            instructions: None,
            cycles: (counted != 0).then_some(cycles),
            energy_nj: None,
            wakeups: None,
            disk_written: None,
            footprint: (measured != 0)
                .then(|| bytes(counters.WorkingSetSize))
                .flatten(),
            peak_footprint: (measured != 0)
                .then(|| bytes(counters.PeakWorkingSetSize))
                .flatten(),
        })
    }
}

#[cfg(not(any(unix, windows)))]
mod os {
    use super::{Usage, UsageError};

    pub(super) fn of(_pid: u32) -> Result<Usage, UsageError> {
        Err(UsageError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )]

    /// Work done between two reads is charged to the process: its CPU time grows; on macOS,
    /// where the kernel keeps the counters, its instructions and cycles do, and where it keeps
    /// none (a virtual machine) both are absent, never zero; and the peak is at least the
    /// footprint wherever both are read.
    #[test]
    #[cfg_attr(miri, ignore = "Miri interprets no foreign call")]
    fn work_between_two_reads_is_charged() {
        let before = super::this().unwrap();
        let mut sum = 0u64;
        for step in 0..20_000_000u64 {
            sum = std::hint::black_box(sum.wrapping_add(step ^ (sum >> 3)));
        }
        std::hint::black_box(sum);
        let after = super::this().unwrap();
        let spent = after.since(&before);
        assert!(spent.cpu_ns() > 0, "{spent:?}");
        if cfg!(target_os = "macos") {
            assert_eq!(
                after.instructions.is_some(),
                after.cycles.is_some(),
                "{after:?}"
            );
            if let (Some(instructions), Some(cycles)) = (spent.instructions, spent.cycles) {
                assert!(instructions >= 20_000_000, "{spent:?}");
                assert!(cycles > 0, "{spent:?}");
            }
        }
        if let (Some(peak), Some(footprint)) = (after.peak_footprint, after.footprint) {
            assert!(peak >= footprint, "{after:?}");
        }
    }

    /// The account's user time is in nanoseconds, not Mach ticks, which differ by the timebase's
    /// ratio (125/3 on Apple silicon). `proc_pid_rusage` and getrusage(2) read two accountings of
    /// the same user time, which skew by less than a microsecond (macos-15-intel, 2026-10-05: the
    /// account 387,867,412 ns, a later getrusage 387,866 us), so neither is held strictly inside
    /// the other's reads; the account is held within a factor of two of them, which the ratio
    /// would put it far outside.
    #[cfg(target_os = "macos")]
    #[test]
    fn user_time_is_in_nanoseconds() {
        /// This process's user time, microseconds, from getrusage(2).
        fn user_us() -> u64 {
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
            // SAFETY: `usage` is a writable `rusage` across the call, which writes one.
            let code = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
            assert_eq!(code, 0);
            // SAFETY: getrusage returned 0 and filled it.
            let usage = unsafe { usage.assume_init() };
            u64::try_from(usage.ru_utime.tv_sec).unwrap() * 1_000_000
                + u64::try_from(usage.ru_utime.tv_usec).unwrap()
        }
        let mut spin = 0u64;
        for i in 0..20_000_000u64 {
            spin = spin.wrapping_add(i).rotate_left(3);
        }
        std::hint::black_box(spin);
        let before = user_us();
        let read = super::this().unwrap();
        let after = user_us();
        assert!(
            before * 1_000 / 2 <= read.user_ns,
            "{before} us, {} ns",
            read.user_ns
        );
        assert!(
            read.user_ns < (after + 1) * 1_000 * 2,
            "{after} us, {} ns",
            read.user_ns
        );
    }

    /// A sum begun from nothing holds what its processes counted.
    #[test]
    fn a_sum_from_nothing_keeps_the_counts() {
        let one = super::Usage {
            instructions: Some(7),
            ..super::Usage::default()
        };
        let sum = super::Usage::default().plus(&one).plus(&one);
        assert_eq!(sum.instructions, Some(14));
        assert_eq!(sum.cycles, None);
    }
}
