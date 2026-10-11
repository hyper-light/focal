//! Who held a long poll (§4.3, the long-step count; A-31). A poll that runs past the step quantum by the
//! wall clock was held either by its task — it ran past the quantum on the CPU, or it waited inside a
//! call (blocked in the kernel, such as the base plane's synchronous `pread` on a cold page cache, or
//! yielding the thread to a full peer ring) — or by the host: the thread was runnable and off the CPU,
//! preempted by the operating system or its virtual CPU stolen by the hypervisor. The wall clock counts
//! both the same. The quantum is the expected wake, a few microseconds (§4.1: a mean of 2.0–3.3 µs on
//! Apple silicon and 10.7–21.7 µs in a Linux container, 2026-09-25), while a time slice handed to another
//! runnable thread is far longer (Linux's EEVDF base slice is 0.75 ms, `sysctl_sched_base_slice` in
//! `kernel/sched/fair.c`, from memory). So by the wall clock alone, every preemption of a correct poll
//! counted as the bounded-work rule's bug signal.
//!
//! The shard reads the calling thread's account ([`ThreadAccount`]) at the start of a window and at the
//! end of a long poll: its CPU time (`CLOCK_THREAD_CPUTIME_ID` on Linux and macOS) and, on Linux, its
//! voluntary context switches (`getrusage(RUSAGE_THREAD)`'s `ru_nvcsw`: the thread blocked in a call;
//! Linux counts a preemption and a `sched_yield` as involuntary). hyper-rt's runtime never yields the
//! thread inside a poll (no wake waits for a full ring: docs/runtime.md §3.2), so slates' report of its own
//! yields to the window is gone. Measured in a Linux container on this Mac (Docker,
//! four CPUs, 2026-09-25, best of five rounds of 200,000 calls): the CPU clock 155–161 ns and `getrusage`
//! 130–134 ns; on Apple silicon the CPU clock 107–129 ns (2026-09-22). That is too dear for every poll,
//! so the shard reads only at the end of a poll that already ran long, and — while a long poll went
//! unattributed for want of a window — at each step's start and each wait's end ([`Tracker`]); a busy
//! period that runs no long poll stops the readings. macOS counts no per-thread voluntary switches, so a
//! poll off the CPU there is unattributed. A virtual machine's
//! guest may count time its virtual CPU was stolen while the thread ran as the thread's CPU; such a poll
//! then reads as a long run and is the task's by this rule (the macOS CI runner, run 36289513559,
//! 2026-09-27: a poll that slept 3 ms took 23 ms and read past a 1 ms quantum on the thread clock). Windows keeps
//! per-thread times at the scheduler tick (about 15.6 ms), so no account is read there and every long
//! poll is unattributed.
//!
//! [`attribute`] and [`Tracker`] are cfg-free and unit-tested on every host; the readings are paired
//! `#[cfg]` functions.
#![allow(unsafe_code)]

/// One reading of the calling thread's scheduling account.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ThreadAccount {
    /// CPU time the thread has run, nanoseconds.
    pub(crate) cpu_ns: u64,
    /// Times the thread has blocked in a call (voluntary context switches), where the operating system
    /// counts them per thread (Linux); `None` elsewhere.
    pub(crate) voluntary_switches: Option<u64>,
}

/// The calling thread's CPU time in nanoseconds (Linux and macOS: `CLOCK_THREAD_CPUTIME_ID`).
#[cfg(all(any(target_os = "linux", target_os = "macos"), not(miri)))]
fn thread_cpu_ns() -> Option<u64> {
    /// Format: nanoseconds per second.
    const NS_PER_SECOND: u64 = 1_000_000_000;
    let reading = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    u64::try_from(reading.tv_sec)
        .ok()?
        .checked_mul(NS_PER_SECOND)?
        .checked_add(u64::try_from(reading.tv_nsec).ok()?)
}

/// The calling thread's voluntary context switches (Linux: `getrusage(RUSAGE_THREAD)`'s `ru_nvcsw`).
/// rustix offers no `getrusage`, so this is the one libc call of the module.
#[cfg(all(target_os = "linux", not(miri)))]
fn voluntary_switches() -> Option<u64> {
    // SAFETY: an all-zero `rusage` is a valid value of the C struct (integers and `timeval`s, padding
    // included); the pointer is to this frame's own value, live for the whole call; `RUSAGE_THREAD` asks
    // the kernel to fill it for the calling thread, and the status is checked before the value is read.
    let (status, usage) = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        let status = libc::getrusage(libc::RUSAGE_THREAD, &mut usage);
        (status, usage)
    };
    if status != 0 {
        return None;
    }
    u64::try_from(usage.ru_nvcsw).ok()
}

/// The calling thread's voluntary context switches now, where the OS counts them per thread (Linux): what
/// tells a park that slept in its wait from one whose wait found a kick already pending (§4.3, the
/// shard's online wake estimate). `None` elsewhere.
#[cfg(all(target_os = "linux", not(miri)))]
pub(crate) fn voluntary_switches_now() -> Option<u64> {
    voluntary_switches()
}

/// No per-thread voluntary switch count here (macOS, Windows), and none under Miri.
#[cfg(any(miri, not(target_os = "linux")))]
pub(crate) fn voluntary_switches_now() -> Option<u64> {
    None
}

/// Linux: the thread's CPU time and its voluntary switches.
#[cfg(all(target_os = "linux", not(miri)))]
pub(crate) fn thread_account() -> Option<ThreadAccount> {
    Some(ThreadAccount {
        cpu_ns: thread_cpu_ns()?,
        voluntary_switches: voluntary_switches(),
    })
}

/// macOS: the thread's CPU time; the kernel counts no per-thread voluntary switches.
#[cfg(all(target_os = "macos", not(miri)))]
pub(crate) fn thread_account() -> Option<ThreadAccount> {
    Some(ThreadAccount {
        cpu_ns: thread_cpu_ns()?,
        voluntary_switches: None,
    })
}

/// No fine per-thread clock here (Windows' thread times tick at about 15.6 ms), and none under Miri.
#[cfg(any(miri, not(any(target_os = "linux", target_os = "macos"))))]
pub(crate) fn thread_account() -> Option<ThreadAccount> {
    None
}

/// What held a poll that ran past the quantum by the wall clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Attribution {
    /// The task: the poll ran past the quantum on the CPU.
    Long,
    /// The task: the poll's CPU stayed within the quantum and the thread waited inside a call during the
    /// window — blocked in the kernel.
    Blocked,
    /// The host: the poll's CPU stayed within the quantum and the thread never waited inside a call during
    /// the window, so it was runnable and off the CPU.
    Preempted,
    /// Unattributed: the window's CPU does not decide this poll's share (the window holds earlier work). A
    /// fresher window would, so the shard arms.
    Ambiguous,
    /// Unattributed: off the CPU, and the platform cannot tell a block from a preemption (macOS).
    OffCpu,
    /// Unattributed: no window was open (the first long poll, or the first after a wait while unarmed).
    NoWindow,
    /// Unattributed: no per-thread clock here.
    NoClock,
}

impl Attribution {
    /// The task's own: the bounded-work rule's bug signal.
    pub(crate) fn is_tasks(self) -> bool {
        matches!(self, Attribution::Long | Attribution::Blocked)
    }

    /// Neither the task's nor the host's, as far as the readings tell.
    pub(crate) fn is_unattributed(self) -> bool {
        !self.is_tasks() && self != Attribution::Preempted
    }
}

/// One window of a long poll: from the start reading to the poll's end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Window {
    /// CPU time the thread ran from the window's start to the poll's end.
    pub(crate) cpu_ns: u64,
    /// Wall time from the window's start to the poll's start: the most CPU the thread can have run before
    /// the poll.
    pub(crate) before_poll_ns: u64,
    /// Whether the thread waited inside a call during the window; `None` where no one can tell.
    pub(crate) waited_in_call: Option<bool>,
}

/// Attributes a poll that ran past `quantum_ns` by the wall clock. Its CPU is at most the window's and at
/// least the window's less the wall time before the poll (the thread ran at most that much before it):
/// past the quantum at the least, the task ran long; within it at the most, the thread was off the CPU
/// for the rest, and whether it waited inside a call decides whose that was; between the two, the window
/// does not decide.
pub(crate) fn attribute(window: &Window, quantum_ns: u64) -> Attribution {
    if window.cpu_ns.saturating_sub(window.before_poll_ns) > quantum_ns {
        return Attribution::Long;
    }
    if window.cpu_ns > quantum_ns {
        return Attribution::Ambiguous;
    }
    match window.waited_in_call {
        Some(true) => Attribution::Blocked,
        Some(false) => Attribution::Preempted,
        None => Attribution::OffCpu,
    }
}

/// The shard's windows and when it reads. A window opens at the end reading of every long poll, and —
/// while armed — at each step's start and each wait's end. A wait closes the window, so a park's own
/// block is never charged to a poll. The shard arms when a long poll found no window, or one that did not
/// decide it; a busy period (from one wait to the next, having done work) that runs no long poll disarms,
/// so a healthy shard reads nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Tracker {
    /// The open window's start: the thread's account and the shard clock then.
    window_start: Option<(ThreadAccount, u64)>,
    /// Whether step starts and wait ends open windows.
    armed: bool,
    /// Whether the busy period since the last wait did work.
    period_worked: bool,
    /// Whether the busy period since the last wait ran a long poll.
    period_long: bool,
}

impl Tracker {
    /// Whether the shard is reading at step starts and wait ends: the loop asks before it reads.
    pub(crate) fn armed(&self) -> bool {
        self.armed
    }

    /// A step begins: while armed, a window opens at what `read` returns — the thread's account and the
    /// shard clock — called only then, so an unarmed shard reads neither.
    pub(crate) fn step_began(&mut self, read: impl FnOnce() -> Option<(ThreadAccount, u64)>) {
        if self.armed {
            self.window_start = read();
        }
    }

    /// A step ended; `did_work` marks the busy period as one.
    pub(crate) fn step_ended(&mut self, did_work: bool) {
        self.period_worked |= did_work;
    }

    /// A wait (a spin or a park) begins: the window closes, and a busy period that ran no long poll disarms.
    pub(crate) fn wait_began(&mut self) {
        self.window_start = None;
        if self.period_worked {
            if !self.period_long {
                self.armed = false;
            }
            self.period_worked = false;
            self.period_long = false;
        }
    }

    /// A wait ended: while armed, a window opens at what `read` returns (called only then).
    pub(crate) fn wait_ended(&mut self, read: impl FnOnce() -> Option<(ThreadAccount, u64)>) {
        if self.armed {
            self.window_start = read();
        }
    }

    /// A poll that began at `poll_started_ns` ran past `quantum_ns` by the wall clock and ended at `now_ns`
    /// with the thread's account `end`: attributes it, arms if the window was missing or undecided, and
    /// opens the next window at `end`.
    pub(crate) fn long_poll(
        &mut self,
        poll_started_ns: u64,
        now_ns: u64,
        end: Option<ThreadAccount>,
        quantum_ns: u64,
    ) -> Attribution {
        self.period_long = true;
        let attribution = match (self.window_start, end) {
            (_, None) => Attribution::NoClock,
            (None, Some(_)) => Attribution::NoWindow,
            (Some((start, start_ns)), Some(end)) => attribute(
                &Window {
                    cpu_ns: end.cpu_ns.saturating_sub(start.cpu_ns),
                    before_poll_ns: poll_started_ns.saturating_sub(start_ns),
                    waited_in_call: self.waited_in_call(start, end),
                },
                quantum_ns,
            ),
        };
        if matches!(attribution, Attribution::NoWindow | Attribution::Ambiguous) {
            self.armed = true;
        }
        self.open(now_ns, end);
        attribution
    }

    /// Whether the thread waited inside a call between two readings: the voluntary switches, where counted.
    fn waited_in_call(&self, start: ThreadAccount, end: ThreadAccount) -> Option<bool> {
        match (start.voluntary_switches, end.voluntary_switches) {
            (Some(before), Some(after)) => Some(after > before),
            _ => None,
        }
    }

    fn open(&mut self, now_ns: u64, reading: Option<ThreadAccount>) {
        self.window_start = reading.map(|account| (account, now_ns));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shape: a quantum of ten microseconds, the order of a virtual machine's mean wake.
    const QUANTUM_NS: u64 = 10_000;

    fn account(cpu_ns: u64, voluntary_switches: Option<u64>) -> Option<ThreadAccount> {
        Some(ThreadAccount {
            cpu_ns,
            voluntary_switches,
        })
    }

    /// A reading taken at `now_ns`, as a step start or a wait end hands it over.
    fn at(now_ns: u64, reading: Option<ThreadAccount>) -> Option<(ThreadAccount, u64)> {
        reading.map(|account| (account, now_ns))
    }

    /// §4.3, A-31: the poll's CPU lies between the window's CPU less the wall time before the poll and the
    /// window's CPU. Past the quantum at the least it is the task's; within it at the most, a wait inside a
    /// call makes it the task's and none makes it the host's; unknown waits stay unattributed; and a window
    /// whose earlier work leaves the poll's share open decides nothing.
    #[test]
    fn a_window_attributes_a_long_poll_by_the_bounds_on_its_cpu() {
        let window = |cpu_ns, before_poll_ns, waited_in_call| Window {
            cpu_ns,
            before_poll_ns,
            waited_in_call,
        };
        assert_eq!(
            attribute(&window(3 * QUANTUM_NS, QUANTUM_NS, Some(false)), QUANTUM_NS),
            Attribution::Long
        );
        assert_eq!(
            attribute(&window(QUANTUM_NS, 0, Some(true)), QUANTUM_NS),
            Attribution::Blocked
        );
        assert_eq!(
            attribute(&window(QUANTUM_NS / 2, 0, Some(false)), QUANTUM_NS),
            Attribution::Preempted
        );
        assert_eq!(
            attribute(&window(QUANTUM_NS / 2, 0, None), QUANTUM_NS),
            Attribution::OffCpu
        );
        // Twice the quantum of CPU in the window, but the whole of it might have run before the poll.
        assert_eq!(
            attribute(
                &window(2 * QUANTUM_NS, 2 * QUANTUM_NS, Some(false)),
                QUANTUM_NS
            ),
            Attribution::Ambiguous
        );
    }

    /// §4.3, A-31: the first long poll has no window and arms; the next is judged in the window its
    /// predecessor's end opened; a busy period with no long poll disarms at the next wait, so a healthy
    /// shard stops reading; a spin that finds nothing and then parks is one wait, not a busy period.
    #[test]
    fn the_tracker_arms_on_an_unattributed_poll_and_disarms_after_a_quiet_busy_period() {
        let mut tracker = Tracker::default();
        tracker.step_began(|| panic!("an unarmed shard reads nothing"));
        assert_eq!(
            tracker.long_poll(0, QUANTUM_NS * 2, account(100, Some(0)), QUANTUM_NS),
            Attribution::NoWindow
        );
        assert!(tracker.armed());
        // Held off the CPU for three quanta right after, with no voluntary switch: the host's.
        assert_eq!(
            tracker.long_poll(
                QUANTUM_NS * 2,
                QUANTUM_NS * 5,
                account(200, Some(0)),
                QUANTUM_NS
            ),
            Attribution::Preempted
        );
        tracker.step_ended(true);
        // A busy period that ran a long poll keeps the arming across its wait.
        tracker.wait_began();
        tracker.wait_ended(|| at(QUANTUM_NS * 6, account(300, Some(1))));
        assert!(tracker.armed());
        tracker.step_began(|| at(QUANTUM_NS * 6, account(300, Some(1))));
        tracker.step_ended(true);
        // A spin that finds nothing, then a park: one wait, and the quiet busy period before it disarms.
        tracker.wait_began();
        tracker.wait_began();
        assert!(!tracker.armed());
        tracker.wait_ended(|| panic!("a disarmed shard reads nothing"));
        tracker.step_began(|| panic!("a disarmed shard reads nothing"));
    }

    /// §4.3, A-31: a wait closes the window, so the park's own block is never charged to the poll after it,
    /// and a window opened by an armed step start judges the next long poll.
    #[test]
    fn a_wait_closes_the_window_and_a_fresh_window_judges_the_next_long_poll() {
        let mut tracker = Tracker::default();
        assert_eq!(
            tracker.long_poll(0, QUANTUM_NS * 2, account(0, None), QUANTUM_NS),
            Attribution::NoWindow
        );
        tracker.step_ended(true);
        tracker.wait_began();
        // The park blocked (a voluntary switch) — then the next busy period opens a fresh window after it.
        tracker.wait_ended(|| at(QUANTUM_NS * 100, account(10, Some(5))));
        assert_eq!(
            tracker.long_poll(
                QUANTUM_NS * 100,
                QUANTUM_NS * 103,
                account(20, Some(5)),
                QUANTUM_NS
            ),
            Attribution::Preempted,
            "the park's switch is outside the window"
        );
        tracker.step_began(|| at(QUANTUM_NS * 104, account(30, Some(5))));
        assert_eq!(
            tracker.long_poll(
                QUANTUM_NS * 104,
                QUANTUM_NS * 107,
                account(40, Some(6)),
                QUANTUM_NS
            ),
            Attribution::Blocked
        );
        // With no switch count, off the CPU is all this platform can say.
        assert_eq!(
            tracker.long_poll(
                QUANTUM_NS * 107,
                QUANTUM_NS * 110,
                account(50, None),
                QUANTUM_NS
            ),
            Attribution::OffCpu
        );
        assert_eq!(
            tracker.long_poll(QUANTUM_NS * 110, QUANTUM_NS * 113, None, QUANTUM_NS),
            Attribution::NoClock
        );
    }
}
