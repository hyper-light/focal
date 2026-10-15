//! The probes the runtime takes its configuration from (slates' `machine::probes`, ORIGIN.md): the
//! cost of a system call that does nothing, and pinning a thread to a core. The park-and-wake latency
//! has its own module, [`crate::machine::wake`]. slates' fault, memcpy, hash, codec, lock-capacity and
//! core-matrix probes size slates' storage, not a runtime, and are not carried (docs/runtime.md §10.1).
//!
//! Method: the Kalibera-Jones stopping rule of [`crate::machine::bench`] [A: Kalibera & Jones,
//! ISMM'13]; every probe takes its wall budget from the caller and reports `quick` when it stopped
//! early.
#![allow(unsafe_code)]

use std::time::Duration;

use crate::machine::bench::{Measurement, measure};

/// How a probe's threads were placed (the core matrix's, the wake probe's).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pinning {
    /// The OS pinned each thread to its core.
    Pinned,
    /// The OS took the placement as a hint only (macOS).
    Hint,
    /// Nothing was pinned by design: production does not fix its threads here (the process shares its
    /// cores in time, [`crate::machine::placement::Placement`]), so the OS placed the probe's threads as it places the
    /// shards — the placement production runs under, not a refusal.
    Scheduled,
    /// Pinning was refused; threads ran wherever the scheduler put them.
    Refused,
}

/// Measures the cost of a system call that does nothing.
pub fn syscall(budget: Duration) -> Measurement {
    measure(platform::null_syscall, budget)
}

/// The weaker of two placements: refused beats a hint beats a pin.
pub(crate) fn weaker(x: Pinning, y: Pinning) -> Pinning {
    match (x, y) {
        (Pinning::Refused, _) | (_, Pinning::Refused) => Pinning::Refused,
        (Pinning::Hint, _) | (_, Pinning::Hint) => Pinning::Hint,
        (Pinning::Scheduled, _) | (_, Pinning::Scheduled) => Pinning::Scheduled,
        _ => Pinning::Pinned,
    }
}

/// A placement as the word a probe thread publishes it in (the core matrix's partner, the wake probe's
/// waiter).
pub(crate) fn pinning_code(p: Pinning) -> u32 {
    match p {
        Pinning::Pinned => 0,
        Pinning::Hint => 1,
        Pinning::Refused => 2,
        // Format: the next word after the three a published placement had before `Scheduled`.
        Pinning::Scheduled => 3,
    }
}

/// The placement a published word names; an unknown word is a refusal (nothing vouches for it).
pub(crate) fn pinning_from_code(c: u32) -> Pinning {
    match c {
        0 => Pinning::Pinned,
        1 => Pinning::Hint,
        // Format: `Scheduled`'s word ([`pinning_code`]).
        3 => Pinning::Scheduled,
        _ => Pinning::Refused,
    }
}

/// Pins the calling thread to `core` where the OS pins (Linux, Windows), hints where it only
/// hints (macOS), and reports which.
pub fn pin_current_thread(core: u32) -> Pinning {
    platform::pin_current(core)
}

/// The calling thread's CPU affinity, kept so a probe that pins the thread can put it back: a process
/// spawned afterwards inherits the calling thread's mask (the anchor measures, then spawns the daemon;
/// docs/bugs/2026-09-14-core-matrix-leaves-the-anchor-pinned-to-one-core.md).
pub(crate) struct SavedAffinity(Option<platform::Affinity>);

impl SavedAffinity {
    /// The calling thread's mask now.
    pub(crate) fn of_calling_thread() -> SavedAffinity {
        SavedAffinity(platform::current_affinity())
    }

    /// Puts the mask back; whether the OS took it (nothing to put back counts as done).
    pub(crate) fn restore(&self) -> bool {
        self.0.as_ref().is_none_or(platform::restore_affinity)
    }
}

#[cfg(unix)]
mod platform {
    use super::Pinning;

    pub(super) fn null_syscall() {
        std::hint::black_box(rustix::process::getppid());
    }

    #[cfg(target_os = "linux")]
    pub(super) fn pin_current(core: u32) -> Pinning {
        use rustix::thread::{CpuSet, sched_setaffinity};
        let index = usize::try_from(core).unwrap_or(usize::MAX);
        if index >= CpuSet::MAX_CPU {
            return Pinning::Refused;
        }
        let mut set = CpuSet::new();
        set.set(index);
        match sched_setaffinity(None, &set) {
            Ok(()) => Pinning::Pinned,
            Err(_) => Pinning::Refused,
        }
    }

    /// The calling thread's CPU affinity as the OS holds it (Linux: the scheduler's mask), to be put back
    /// after the matrix pinned the thread.
    #[cfg(target_os = "linux")]
    pub(super) struct Affinity(rustix::thread::CpuSet);

    #[cfg(target_os = "linux")]
    pub(super) fn current_affinity() -> Option<Affinity> {
        rustix::thread::sched_getaffinity(None).ok().map(Affinity)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn restore_affinity(affinity: &Affinity) -> bool {
        rustix::thread::sched_setaffinity(None, &affinity.0).is_ok()
    }

    #[cfg(target_os = "macos")]
    unsafe extern "C" {
        fn thread_policy_set(
            thread: libc::mach_port_t,
            flavor: libc::c_uint,
            policy_info: *mut libc::c_int,
            count: libc::c_uint,
        ) -> libc::c_int;
    }

    /// Sets the calling thread's affinity tag (mach/thread_policy.h): a tag of zero means "no affinity",
    /// any other groups threads that share it; a hint, not a pin. Returns whether the kernel took it.
    #[cfg(target_os = "macos")]
    fn set_affinity_tag(mut tag: libc::c_int) -> bool {
        /// Format: THREAD_AFFINITY_POLICY, the affinity-tag hint (mach/thread_policy.h).
        const THREAD_AFFINITY_POLICY: libc::c_uint = 4;
        // SAFETY: the calling thread's own mach port and a one-integer policy of the stated count.
        let rc = unsafe {
            thread_policy_set(
                libc::pthread_mach_thread_np(libc::pthread_self()),
                THREAD_AFFINITY_POLICY,
                &raw mut tag,
                1,
            )
        };
        rc == libc::KERN_SUCCESS
    }

    #[cfg(target_os = "macos")]
    pub(super) fn pin_current(core: u32) -> Pinning {
        // Cores are numbered from zero and zero is the null tag, so the tag is core + 1.
        let tag = libc::c_int::try_from(core).unwrap_or(0).saturating_add(1);
        if set_affinity_tag(tag) {
            Pinning::Hint
        } else {
            Pinning::Refused
        }
    }

    /// macOS holds no readable mask: the thread came in with the null tag (no affinity), which is what
    /// is put back.
    #[cfg(target_os = "macos")]
    pub(super) struct Affinity;

    #[cfg(target_os = "macos")]
    pub(super) fn current_affinity() -> Option<Affinity> {
        Some(Affinity)
    }

    /// Puts the null tag back. A kernel that refused the hint when pinning (Apple silicon:
    /// `KERN_NOT_SUPPORTED`) refuses this the same way — the thread was never tagged then, and the
    /// matrix already reports `Refused` for it.
    #[cfg(target_os = "macos")]
    pub(super) fn restore_affinity(_affinity: &Affinity) -> bool {
        /// Format: THREAD_AFFINITY_TAG_NULL — no affinity (mach/thread_policy.h).
        const NULL_TAG: libc::c_int = 0;
        set_affinity_tag(NULL_TAG)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(super) fn pin_current(_core: u32) -> Pinning {
        Pinning::Refused
    }

    /// Nothing pins here, so nothing is put back.
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(super) struct Affinity;

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(super) fn current_affinity() -> Option<Affinity> {
        None
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub(super) fn restore_affinity(_affinity: &Affinity) -> bool {
        true
    }
}

#[cfg(windows)]
mod platform {
    use super::Pinning;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::Threading::{
        CreateEventW, GetCurrentThread, SetEvent, SetThreadAffinityMask,
    };

    fn event() -> HANDLE {
        thread_local! {
          static EVENT: HANDLE = {
            // SAFETY: an anonymous auto-reset event; the handle lives for the thread.
            unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) }
          };
        }
        EVENT.with(|h| *h)
    }

    pub(super) fn null_syscall() {
        // SAFETY: a valid event handle; SetEvent enters the kernel every call.
        std::hint::black_box(unsafe { SetEvent(event()) });
    }

    /// Sets the calling thread's affinity mask; the previous mask, or zero when refused.
    fn set_thread_affinity(mask: usize) -> usize {
        // SAFETY: the calling thread's pseudo-handle and a mask within the process's.
        unsafe { SetThreadAffinityMask(GetCurrentThread(), mask) }
    }

    pub(super) fn pin_current(core: u32) -> Pinning {
        if core >= usize::BITS {
            return Pinning::Refused;
        }
        let mask: usize = 1usize << core;
        if set_thread_affinity(mask) == 0 {
            Pinning::Refused
        } else {
            Pinning::Pinned
        }
    }

    /// The mask the calling thread came in with: the process's affinity mask, which a thread never pinned
    /// before the matrix runs on (the anchor's main thread).
    pub(super) struct Affinity(usize);

    pub(super) fn current_affinity() -> Option<Affinity> {
        crate::machine::facts::process_affinity_mask().map(Affinity)
    }

    pub(super) fn restore_affinity(affinity: &Affinity) -> bool {
        set_thread_affinity(affinity.0) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_syscall_costs_something_and_is_measured_with_an_interval() {
        let m = syscall(Duration::from_millis(40));
        assert!(m.median_ns() > 0, "{m:?}");
        assert!(m.interval.lower <= m.interval.upper);
    }

    #[test]
    fn the_weaker_placement_wins_and_codes_round_trip() {
        for p in [
            Pinning::Pinned,
            Pinning::Hint,
            Pinning::Scheduled,
            Pinning::Refused,
        ] {
            assert_eq!(pinning_from_code(pinning_code(p)), p);
            assert_eq!(weaker(p, Pinning::Refused), Pinning::Refused);
        }
        assert_eq!(weaker(Pinning::Pinned, Pinning::Hint), Pinning::Hint);
    }
}
