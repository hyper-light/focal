//! Host-wide monotonic time (§2.6 supervision, §4.4 recovered leases, §4.14 freshness).
//! A reading belongs to one OS boot/time namespace, never to one process or clock instance.
//! Linux CLOCK_BOOTTIME, Darwin CLOCK_MONOTONIC and Windows interrupt time include suspend,
//! so a resumed process cannot extend a recorded deadline by the time it was asleep. They are
//! independent of wall-clock corrections. Only readings from the same host domain are comparable.
//! Evidence and the failing cross-process history: docs/bugs/2026-09-17-heartbeats-use-different-clock-origins.md.
#![allow(unsafe_code)]

/// Monotonic nanoseconds in this host's boot/time namespace, shared across process restarts.
#[cfg(unix)]
pub fn monotonic_ns() -> u64 {
    // Linux distinguishes active uptime from elapsed boot time. Darwin's MONOTONIC already includes
    // suspend. Both are infallible ClockId queries through rustix, with no local origin to reset.
    #[cfg(target_os = "linux")]
    let clock = rustix::time::ClockId::Boottime;
    #[cfg(not(target_os = "linux"))]
    let clock = rustix::time::ClockId::Monotonic;
    let reading = rustix::time::clock_gettime(clock);
    /// Format: the number of nanoseconds in one second (SI units).
    const NS_PER_SECOND: u64 = 1_000_000_000;
    u64::try_from(reading.tv_sec)
        .unwrap_or(u64::MAX)
        .saturating_mul(NS_PER_SECOND)
        .saturating_add(u64::try_from(reading.tv_nsec).unwrap_or(u64::MAX))
}

/// The resolution of [`monotonic_ns`]'s clock in nanoseconds, as the operating system reports it for that very clock
/// (`clock_getres`): the least step its readings take, at least one. A consumer that judges lateness from readings
/// (hyper-swim's `G`, A-67) is told this, never an assumed value (R3).
#[cfg(unix)]
pub fn resolution_ns() -> u64 {
    #[cfg(target_os = "linux")]
    let clock = rustix::time::ClockId::Boottime;
    #[cfg(not(target_os = "linux"))]
    let clock = rustix::time::ClockId::Monotonic;
    let resolution = rustix::time::clock_getres(clock);
    /// Format: the number of nanoseconds in one second (SI units).
    const NS_PER_SECOND: u64 = 1_000_000_000;
    u64::try_from(resolution.tv_sec)
        .unwrap_or(u64::MAX)
        .saturating_mul(NS_PER_SECOND)
        .saturating_add(u64::try_from(resolution.tv_nsec).unwrap_or(u64::MAX))
        .max(1)
}

/// The resolution of [`monotonic_ns`]'s clock in nanoseconds: the interrupt-time unit `QueryInterruptTimePrecise`
/// reports in (realtimeapiset.h), 100 ns.
#[cfg(windows)]
pub fn resolution_ns() -> u64 {
    /// Format: the Win32 interrupt-time unit is 100 nanoseconds (realtimeapiset.h).
    const NS_PER_TICK: u64 = 100;
    NS_PER_TICK
}

/// Monotonic nanoseconds in this host's boot/time namespace, shared across process restarts.
#[cfg(windows)]
pub fn monotonic_ns() -> u64 {
    let mut ticks = 0;
    // SAFETY: the API writes one u64 through this live, exclusive pointer; it has no failure result.
    unsafe {
        windows_sys::Win32::System::WindowsProgramming::QueryInterruptTimePrecise(&mut ticks);
    }
    /// Format: the Win32 interrupt-time unit is 100 nanoseconds (realtimeapiset.h).
    const NS_PER_TICK: u64 = 100;
    ticks.saturating_mul(NS_PER_TICK)
}

/// The shard's clock: monotonic nanoseconds that count through suspend, read at the cheapest call each OS
/// offers, for the loop's own timing (a poll's length, a timer's deadline, a spin's end) relative to its
/// driver's epoch ([`crate::driver::nanos_since`]). On macOS `CLOCK_MONOTONIC` is the wall clock less the
/// boot time, two commpage reads: 16.1 ns a read at 1 µs resolution, where `mach_continuous_time` reads the
/// continuous timebase itself, which also counts through sleep (`<mach/mach_time.h>`), in 4.8 ns at the
/// timebase's 41.67 ns (`benchmark-results/hyper-rt-vs-tokio-20261010/os-parkcost/clockcost2.txt`, Apple
/// M5 Max under load). The shard read the former twice a poll: about 6 % of a shard's samples on mantle's
/// resident get path (`benchmark-results/mantle-resident-reads-smoke-20261010/reads3m-sample.txt`).
/// Elsewhere [`monotonic_ns`]'s clocks are already the cheap ones (Linux's vDSO `CLOCK_BOOTTIME`, Windows'
/// interrupt time) and this is that clock. Its readings are the host's on every OS but, on macOS, not
/// [`monotonic_ns`]'s: the two are never compared.
#[cfg(target_os = "macos")]
pub fn shard_clock_ns() -> u64 {
    match continuous_scale() {
        Some(scale) => ticks_to_ns(mach_continuous_time(), scale),
        None => monotonic_ns(),
    }
}

/// The shard's clock (see the macOS form): [`monotonic_ns`] where that is already the OS's cheap clock.
#[cfg(not(target_os = "macos"))]
pub fn shard_clock_ns() -> u64 {
    monotonic_ns()
}

/// Format: the fractional bits of the timebase's fixed-point scale, so a tick converts with one 64×64-bit
/// multiply and a shift rather than a 128-bit division: `scale = round((numer << 32) / denom)`, off by at
/// most half of `2⁻³²` of a tick's nanoseconds, a relative error under 10⁻¹¹ for Apple silicon's 125/3.
const SCALE_BITS: u32 = 32;

/// The fixed-point scale of a `numer / denom` timebase, rounded to nearest; `None` for a zero ratio.
#[cfg(any(
    target_os = "macos",
    all(target_os = "linux", target_arch = "aarch64"),
    test
))]
fn scale_of(numer: u32, denom: u32) -> Option<u64> {
    let denom = u64::from(denom);
    u64::from(numer)
        .checked_shl(SCALE_BITS)?
        .checked_add(denom / 2)?
        .checked_div(denom)
        .filter(|scale| *scale > 0)
}

/// Ticks of a timebase whose fixed-point scale is `scale`, as nanoseconds.
fn ticks_to_ns(ticks: u64, scale: u64) -> u64 {
    u128::from(ticks)
        .saturating_mul(u128::from(scale))
        .checked_shr(SCALE_BITS)
        .and_then(|ns| u64::try_from(ns).ok())
        .unwrap_or(u64::MAX)
}

/// The counter the shard carries its clock forward by between the loop's own readings of it (docs/runtime.md
/// §3.9, "a poll's end from the counter"): the generic timer's virtual count, read without synchronization.
/// The OS clocks read it ordered behind the instructions before them: macOS's `mach_continuous_time` reads
/// an Apple register on this machine (`S3_4_C15_C10_6`, where the sampled time sat), CNTVCTSS_EL0 or an `isb`
/// and CNTVCT_EL0 on others (libsystem_kernel, disassembled 2026-10-13), and Linux's vDSO puts an `isb`
/// before its read. An ordered read waits for the poll's outstanding work to complete; unordered, the CPU
/// may take the reading ahead of the instructions before it, by no more than its reorder window, a few
/// hundred cycles, where the loop times polls of microseconds. `None` where this crate reads no counter: the
/// shard clock then takes every reading.
#[cfg(all(
    target_arch = "aarch64",
    any(target_os = "macos", target_os = "linux"),
    not(miri)
))]
pub fn counter_ticks() -> Option<u64> {
    let ticks: u64;
    // SAFETY: CNTVCT_EL0, the generic timer's virtual count, is readable at EL0 under macOS, whose commpage
    // clock reads it, and Linux, whose vDSO reads it (where an erratum keeps EL0 out, the kernel emulates the
    // read); `mrs` writes its one output register and touches no memory, stack or flags.
    unsafe {
        core::arch::asm!(
            "mrs {ticks}, cntvct_el0",
            ticks = out(reg) ticks,
            options(nomem, nostack, preserves_flags)
        );
    }
    Some(ticks)
}

/// No counter read here (see the aarch64 form): the shard clock takes every reading.
#[cfg(not(all(
    target_arch = "aarch64",
    any(target_os = "macos", target_os = "linux"),
    not(miri)
)))]
pub fn counter_ticks() -> Option<u64> {
    None
}

/// The counter's nanoseconds a tick as a fixed-point scale, as the architecture states it: its frequency
/// register, CNTFRQ_EL0, which firmware sets and Linux's own clocksource reads (the kernel's binding for the
/// timer, `Documentation/devicetree/bindings/timer/arm,arch_timer.yaml`, keeps a device-tree frequency only
/// for firmware that set it wrong). Not the timebase macOS's clocks count in: on this machine (Apple M5 Max,
/// 2026-10-13) CNTVCT_EL0 counts at CNTFRQ_EL0's 1 GHz, while `mach_continuous_time` and its register
/// `S3_4_C15_C10_6` count the 24 MHz of the 125/3 timebase. Read once.
#[cfg(all(
    target_arch = "aarch64",
    any(target_os = "macos", target_os = "linux"),
    not(miri)
))]
pub fn counter_scale() -> Option<u64> {
    static SCALE: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    *SCALE.get_or_init(|| {
        let hz: u64;
        // SAFETY: CNTFRQ_EL0 is readable at EL0 wherever the counter is (the same access control gates both);
        // `mrs` writes its one output register and touches no memory, stack or flags.
        unsafe {
            core::arch::asm!(
                "mrs {hz}, cntfrq_el0",
                hz = out(reg) hz,
                options(nomem, nostack, preserves_flags)
            );
        }
        /// Format: nanoseconds per second.
        const NS_PER_SECOND: u32 = 1_000_000_000;
        scale_of(NS_PER_SECOND, u32::try_from(hz).ok()?)
    })
}

/// No counter here: no scale.
#[cfg(not(all(
    target_arch = "aarch64",
    any(target_os = "macos", target_os = "linux"),
    not(miri)
)))]
pub fn counter_scale() -> Option<u64> {
    None
}

/// The shard clock carried forward by the counter: a reading of the shard clock, the counter's ticks then,
/// and the counter's scale. Where the loop reads the shard clock once a step, a reading within the step is
/// that one plus the counter's ticks since, so a scale off by a fraction moves a reading by that fraction of
/// the time since the step's own, never more.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Carried {
    base_ns: u64,
    base_ticks: u64,
    scale: u64,
}

impl Carried {
    /// The shard clock read at `base_ns` beside the counter at `base_ticks`, ticks of `scale` nanoseconds.
    pub const fn new(base_ns: u64, base_ticks: u64, scale: u64) -> Carried {
        Carried {
            base_ns,
            base_ticks,
            scale,
        }
    }

    /// The shard clock at the counter's `ticks`: never before the base, as a counter read ahead of the
    /// base's own can make the ticks since look negative.
    pub fn now_ns(&self, ticks: u64) -> u64 {
        self.base_ns.saturating_add(ticks_to_ns(
            ticks.saturating_sub(self.base_ticks),
            self.scale,
        ))
    }
}

#[cfg(target_os = "macos")]
// SAFETY: `mach_continuous_time` (`<mach/mach_time.h>`, macOS 10.12 and later) takes no argument, reads the
// system's continuous timebase and cannot fail; it touches no memory of this process.
unsafe extern "C" {
    safe fn mach_continuous_time() -> u64;
}

/// The timebase's ticks-to-nanoseconds ratio as a fixed-point scale, read once; `None` when the OS refuses
/// the query, and the shard's clock is then [`monotonic_ns`] for the process's life.
#[cfg(target_os = "macos")]
#[expect(
    deprecated,
    reason = "libc deprecates its Mach declarations in favour of the mach2 crate, which is not a \
              dependency; the declarations themselves are unchanged in libc (as in `udp::macos`)"
)]
fn continuous_scale() -> Option<u64> {
    static SCALE: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    *SCALE.get_or_init(|| {
        let mut info = libc::mach_timebase_info { numer: 0, denom: 0 };
        // SAFETY: the call writes the two `u32` fields of this frame's own value, live and exclusive for the
        // call, and returns its status, checked before the value is read.
        let status = unsafe { libc::mach_timebase_info(&raw mut info) };
        if status != libc::KERN_SUCCESS {
            return None;
        }
        scale_of(info.numer, info.denom)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Apple silicon's 24 MHz timebase (125/3) converts exactly at whole nanoseconds and at a second's ticks;
    /// a one-to-one timebase (Intel) returns the ticks; a zero ratio has no scale.
    #[test]
    fn a_timebase_converts_ticks_to_nanoseconds() {
        let apple = scale_of(125, 3).unwrap();
        assert_eq!(ticks_to_ns(3, apple), 125);
        assert_eq!(ticks_to_ns(24_000_000, apple), 1_000_000_000);
        assert_eq!(ticks_to_ns(0, apple), 0);
        let intel = scale_of(1, 1).unwrap();
        assert_eq!(ticks_to_ns(123_456_789, intel), 123_456_789);
        assert_eq!(scale_of(0, 1), None);
        assert_eq!(scale_of(1, 0), None);
    }

    /// The carried clock by use: a reading is the base plus the ticks since at the scale, exactly, and a
    /// counter read ahead of the base's own (ticks before it) reads the base, never earlier.
    #[test]
    fn a_carried_reading_is_the_base_plus_the_ticks_since() {
        let apple = scale_of(125, 3).unwrap();
        let carried = Carried::new(1_000_000, 24_000, apple);
        assert_eq!(carried.now_ns(24_000), 1_000_000);
        assert_eq!(carried.now_ns(24_003), 1_000_125);
        assert_eq!(carried.now_ns(48_000), 2_000_000);
        assert_eq!(carried.now_ns(23_999), 1_000_000);
    }

    /// A counter read ordered behind everything before it (an `isb` first), so a pairing brackets the shard
    /// clock's reading between two of them.
    #[cfg(all(
        target_arch = "aarch64",
        any(target_os = "macos", target_os = "linux"),
        not(miri)
    ))]
    fn ordered_ticks() -> u64 {
        let ticks: u64;
        // SAFETY: as `counter_ticks`, with an `isb` before the read; neither touches memory or the stack.
        unsafe {
            core::arch::asm!(
                "isb",
                "mrs {ticks}, cntvct_el0",
                ticks = out(reg) ticks,
                options(nostack, preserves_flags)
            );
        }
        ticks
    }

    /// The shard clock's least step here, nanoseconds: macOS's continuous time counts the timebase's ticks;
    /// elsewhere the monotonic clock's reported resolution.
    #[cfg(all(
        target_arch = "aarch64",
        any(target_os = "macos", target_os = "linux"),
        not(miri)
    ))]
    fn shard_clock_step_ns() -> u64 {
        #[cfg(target_os = "macos")]
        let step =
            continuous_scale().map_or_else(resolution_ns, |scale| ticks_to_ns(1, scale).max(1));
        #[cfg(not(target_os = "macos"))]
        let step = resolution_ns();
        step
    }

    /// §3.9 by use on this machine: where the shard reads a counter, it runs at the scale the OS or the
    /// architecture states. Two pairings each read the counter, the shard clock and the counter, so the
    /// clock's reading lies within its bracket; between them the counter's ticks at the stated scale and the
    /// clock's own elapsed time differ by no more than the brackets' halves, a counter tick and a clock step
    /// each. The test waits until the span is twenty times that (so a scale off by the crate's precision,
    /// five percent, would show past it), reading the live host however busy, and holds the bound exactly.
    #[test]
    #[cfg(all(
        target_arch = "aarch64",
        any(target_os = "macos", target_os = "linux"),
        not(miri)
    ))]
    fn the_counter_runs_at_its_stated_scale() {
        let scale = counter_scale().expect("a counter's scale where the shard reads a counter");
        let tick_ns = ticks_to_ns(1, scale).max(1);
        let clock_step_ns = shard_clock_step_ns();
        let pairing = || {
            let before = ordered_ticks();
            let clock = shard_clock_ns();
            let after = ordered_ticks();
            (before, clock, after)
        };
        let (a_before, a_clock, a_after) = pairing();
        let open_a = ticks_to_ns(a_after.saturating_sub(a_before), scale) / 2;
        loop {
            let (b_before, b_clock, b_after) = pairing();
            let open_b = ticks_to_ns(b_after.saturating_sub(b_before), scale) / 2;
            let bound = open_a + open_b + 2 * tick_ns + 2 * clock_step_ns;
            let clock_span = b_clock.saturating_sub(a_clock);
            if clock_span < 20 * bound {
                continue;
            }
            let mid_a = a_before + (a_after - a_before) / 2;
            let mid_b = b_before + (b_after - b_before) / 2;
            let counter_span = ticks_to_ns(mid_b - mid_a, scale);
            assert!(
                counter_span.abs_diff(clock_span) <= bound,
                "counter {counter_span} ns against the clock's {clock_span} ns, bound {bound} ns"
            );
            break;
        }
    }

    /// The shard's clock never runs backwards between two readings on one thread.
    #[test]
    fn the_shard_clock_never_runs_backwards() {
        let mut last = shard_clock_ns();
        for _ in 0..10_000 {
            let now = shard_clock_ns();
            assert!(now >= last, "{now} after {last}");
            last = now;
        }
    }
}
