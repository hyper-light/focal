//! Host-wide monotonic time (§2.6 supervision, §4.4 recovered leases, §4.14 freshness).
//! A reading belongs to one OS boot/time namespace, never to one process or clock instance.
//! Linux CLOCK_BOOTTIME, Darwin CLOCK_MONOTONIC and Windows interrupt time include suspend,
//! so a resumed process cannot extend a recorded deadline by the time it was asleep. They are
//! independent of wall-clock corrections. Only readings from the same host domain are comparable.
//! Evidence and the failing cross-process history: docs/bugs/2026-09-17-heartbeats-use-different-clock-origins.md.
#![allow(unsafe_code)]

#[cfg(target_os = "macos")]
use crate::error::RtError;

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
/// [`monotonic_ns`]'s: the two are never compared. A process Rosetta 2 translates reads its continuous time
/// translated, nanoseconds at a timebase of 1/1, and it counts as a native process's does: read beside one
/// on the M5 Max (2026-10-20), both stood 1,587.183436 s past `mach_absolute_time`, the time the host had
/// slept, and agreed with `CLOCK_MONOTONIC_RAW` to the microsecond. The process's own clock and its own
/// timebase, it takes no kernel rate ([`MachTicks`]).
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

/// Format: nanoseconds in a second (SI units), the unit `hw.tbfrequency` counts ticks in.
#[cfg(any(target_os = "macos", test))]
const SECOND_NS: u64 = 1_000_000_000;

/// macOS's two tick counts (docs/runtime.md §5.1, "Kernel receive stamps"). This process's own clocks,
/// `mach_absolute_time` and `mach_continuous_time`, count ticks of its timebase, `numer / denom` nanoseconds
/// (`mach_timebase_info`). What the kernel takes on its own clock and hands over counts the kernel's ticks,
/// `hw.tbfrequency` a second: a datagram's receive stamp (`SCM_TIMESTAMP_MONOTONIC`, the kernel's
/// `mach_absolute_time()` in `ip_savecontrol`, bsd/netinet/ip_input.c lines 3448-3457 at
/// apple-oss-distributions/xnu xnu-12377.121.6; `ip6_savecontrol_v4`, bsd/netinet6/ip6_input.c lines
/// 1874-1883) and a thread's run time (`thread_selfusage`, bsd/kern/kern_resource.c lines 3606-3613,
/// `recount_current_thread_time_mach`). XNU derives the timebase from that frequency: `timebase_callback`
/// sets `numer / denom` to 10⁹ over the frequency in lowest terms (osfmk/arm/rtclock.c lines 118-137),
/// given `timebase_frequency_hz` (pexpert/arm/pe_init.c lines 598-599), the value `hw.tbfrequency` reads
/// (bsd/kern/kern_mib.c lines 683-689, 986); on Intel the frequency is 10⁹
/// (pexpert/i386/pe_identify_machine.c line 47) and the timebase 1/1 (osfmk/i386/rtclock.c lines 410-414).
/// So for a native process the two are one counter. A process Rosetta 2 translates is shown a timebase of
/// 1/1 and its own clocks in nanoseconds, since "the value returned by the mach_absolute_time function is
/// different for native and translated processes" (Apple, "Addressing architectural differences in your
/// macOS code", "Apply Timebase Information to Mach Absolute Time Values"), while the kernel's values stay
/// in the hardware's ticks, 24 MHz on Apple silicon: there a kernel tick is 125/3 of the process's.
/// Measured on the M5 Max (macOS 26.4.1, 2026-10-20; docs/research/timing.md, "Rosetta 2 and the kernel's
/// ticks"): an x86_64 process reads a timebase of 1/1 and `hw.tbfrequency` 24,000,000, and the stamp of a
/// datagram it sent itself is 1/41.667 of the `mach_absolute_time` read around it.
#[cfg(any(target_os = "macos", test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MachTicks {
    /// A tick of this process's: `numer / denom` nanoseconds, neither zero (`mach_timebase_info`'s
    /// two `uint32_t`s).
    numer: u32,
    /// See `numer`.
    denom: u32,
    /// A tick of the kernel's in this process's ticks, `up / down`: 10⁹ · `denom` over `hw.tbfrequency` ·
    /// `numer`. `None` where that is one, the two counters one (a native process).
    kernel: Option<(u64, u64)>,
}

#[cfg(any(target_os = "macos", test))]
impl MachTicks {
    /// The process's timebase, `numer / denom` nanoseconds a tick, and the kernel's `hz` ticks a second;
    /// `None` when any of them is zero, or when `hz` · `numer` passes 2⁶⁴, which no host comes near (Apple
    /// silicon's is 24 MHz · 125 natively, 24 MHz · 1 translated; Intel's 10⁹ · 1).
    pub fn new(numer: u32, denom: u32, hz: u64) -> Option<MachTicks> {
        if numer == 0 || denom == 0 || hz == 0 {
            return None;
        }
        // At most 10⁹ · 2³², under 2⁶².
        let up = SECOND_NS.checked_mul(u64::from(denom))?;
        let down = hz.checked_mul(u64::from(numer))?;
        Some(MachTicks {
            numer,
            denom,
            kernel: (up != down).then_some((up, down)),
        })
    }

    /// `ticks` of this process's clocks as nanoseconds: × `numer` / `denom`, rounded down, in 128 bits so it
    /// cannot overflow before the division; past `u64`, `u64::MAX`.
    pub fn ns(&self, ticks: u64) -> u64 {
        u128::from(ticks)
            .checked_mul(u128::from(self.numer))
            .and_then(|scaled| scaled.checked_div(u128::from(self.denom)))
            .and_then(|ns| u64::try_from(ns).ok())
            .unwrap_or(u64::MAX)
    }

    /// `ticks` of the kernel's as this process's: the same count where the two are one counter, else
    /// × `up` / `down`, rounded down; a count of at most 2⁶⁴ by an `up` under 2⁶² cannot overflow 128 bits;
    /// past `u64`, `u64::MAX`.
    pub fn of_kernel(&self, ticks: u64) -> u64 {
        match self.kernel {
            None => ticks,
            Some((up, down)) => u128::from(ticks)
                .checked_mul(u128::from(up))
                .and_then(|scaled| scaled.checked_div(u128::from(down)))
                .and_then(|ticks| u64::try_from(ticks).ok())
                .unwrap_or(u64::MAX),
        }
    }

    /// How long before this process's `now` ticks the kernel took a value at its `then` ticks, nanoseconds;
    /// zero for one taken after `now`. The kernel's ticks are carried into the process's first and the
    /// difference converted once, so a native process, whose counters are one, gets the age it got from
    /// `ns(now - then)` to the bit: nanoseconds worked out for each and subtracted could differ from it by
    /// one, each rounded down on its own.
    pub fn age_ns(&self, now: u64, then: u64) -> u64 {
        self.ns(now.saturating_sub(self.of_kernel(then)))
    }

    /// Nanoseconds a library worked out from the kernel's ticks at this process's timebase, at the kernel's
    /// rate instead. libc's `clock_gettime(CLOCK_THREAD_CPUTIME_ID)` is `__thread_selfusage()`, the kernel's
    /// ticks, × `numer` / `denom` (gen/clock_gettime.c lines 105-135 at apple-oss-distributions/Libc
    /// Libc-1752.120.2), so a translated process is told its thread ran 3/125 of what it ran (measured: 1.4 ms
    /// where `getrusage` and `thread_info` said 57.4). The correction is the factor [`MachTicks::of_kernel`]
    /// carries a count by, a kernel tick's nanoseconds over a process tick's; one for a native process, which
    /// keeps libc's reading to the bit.
    pub fn at_kernel_rate(&self, ns: u64) -> u64 {
        self.of_kernel(ns)
    }
}

/// This host's two tick counts ([`MachTicks`]): the timebase and `hw.tbfrequency`, read once for the
/// process's life. Refused when either call is, or a rate is zero.
#[cfg(target_os = "macos")]
pub fn mach_ticks() -> Result<MachTicks, RtError> {
    static TICKS: std::sync::OnceLock<Result<MachTicks, RtError>> = std::sync::OnceLock::new();
    TICKS.get_or_init(read_mach_ticks).clone()
}

#[cfg(target_os = "macos")]
#[expect(
    deprecated,
    reason = "libc deprecates its Mach declarations in favour of the mach2 crate, which is not a \
              dependency; the declarations themselves are unchanged in libc (as in `udp::macos`)"
)]
fn read_mach_ticks() -> Result<MachTicks, RtError> {
    /// The call that reads the kernel's tick rate, as a refusal names it.
    const TBFREQUENCY: &str = "sysctlbyname(hw.tbfrequency)";
    let mut info = libc::mach_timebase_info { numer: 0, denom: 0 };
    // SAFETY: the call writes the two `u32` fields of this frame's own value, live and exclusive for the
    // call, and returns its status, checked before the value is read.
    let status = unsafe { libc::mach_timebase_info(&raw mut info) };
    if status != libc::KERN_SUCCESS {
        return Err(RtError::DriverRefused {
            call: "mach_timebase_info",
            code: Some(status),
        });
    }
    let mut hz: u64 = 0;
    let mut length = size_of::<u64>();
    // SAFETY: the name is NUL-terminated; the out pointer and its length name one live local `u64`, the
    // sysctl's type (`CTLTYPE_QUAD`, bsd/kern/kern_mib.c line 986); no new value is written.
    let outcome = unsafe {
        libc::sysctlbyname(
            c"hw.tbfrequency".as_ptr(),
            (&raw mut hz).cast::<libc::c_void>(),
            &raw mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if outcome != 0 {
        return Err(RtError::os(TBFREQUENCY));
    }
    if length != size_of::<u64>() {
        return Err(RtError::DriverRefused {
            call: TBFREQUENCY,
            code: None,
        });
    }
    MachTicks::new(info.numer, info.denom, hz).ok_or(RtError::DriverRefused {
        call: if hz == 0 {
            TBFREQUENCY
        } else {
            "mach_timebase_info"
        },
        code: None,
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

    /// Apple silicon's rates: a native process's timebase is 125/3 and a translated one's 1/1, the kernel's
    /// 24 MHz either way (both read on the M5 Max, 2026-10-20); Intel's 1/1 against 10⁹.
    const KERNEL_HZ: u64 = 24_000_000;

    /// Instants and spans the sweeps below take, in the kernel's ticks: the edges, a tick's neighbours, the
    /// M5 Max's uptime when the stamp was measured (28,155,671,796,901 ticks, 13.6 days), and a splitmix64
    /// sequence over the whole range from a fixed seed.
    fn instants() -> Vec<u64> {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        let mut instants = vec![
            0,
            1,
            2,
            3,
            4,
            23_999_999,
            24_000_000,
            28_155_671_796_901,
            u64::MAX,
        ];
        instants.extend((0..2_000).map(|_| next()));
        instants.extend((0..2_000).map(|_| next() >> 20));
        instants
    }

    /// The age a stamp got before the kernel's rate was read: the process's timebase on the difference.
    fn age_before(numer: u128, denom: u128, now: u64, then: u64) -> u64 {
        u64::try_from(u128::from(now.saturating_sub(then)) * numer / denom).unwrap_or(u64::MAX)
    }

    /// A native process, whose timebase is the kernel's (125/3 against 24 MHz; 1/1 against 10⁹ on Intel),
    /// takes the kernel's ticks as its own: a stamp's age, a library's reading and a count are what they
    /// were before the kernel's rate was read, to the bit.
    #[test]
    fn a_native_process_reads_the_kernels_ticks_as_its_own() {
        for (numer, denom, hz) in [(125, 3, KERNEL_HZ), (1, 1, 1_000_000_000)] {
            let ticks = MachTicks::new(numer, denom, hz).unwrap();
            let instants = instants();
            for (&now, &then) in instants.iter().zip(instants.iter().rev()) {
                assert_eq!(ticks.of_kernel(now), now);
                assert_eq!(ticks.at_kernel_rate(now), now);
                assert_eq!(
                    ticks.age_ns(now, then),
                    age_before(u128::from(numer), u128::from(denom), now, then),
                    "{numer}/{denom} against {hz} Hz, now {now} then {then}"
                );
            }
        }
    }

    /// A translated process is shown a timebase of 1/1 and its own clock in nanoseconds, the kernel's 24 MHz
    /// tick scaled by 125/3 (the measured stamp, 28,155,671,796,901 ticks, lay between the process's readings
    /// 1,173,152,991,501,583 and 1,173,152,991,549,999 ns around it): the kernel's ticks are carried into its
    /// own at 125/3, and a library's nanoseconds worked out at 1/1 are restated at it.
    #[test]
    fn a_translated_process_carries_the_kernels_ticks_into_its_own() {
        let translated = MachTicks::new(1, 1, KERNEL_HZ).unwrap();
        assert_eq!(translated.of_kernel(3), 125);
        assert_eq!(translated.of_kernel(1), 41);
        assert_eq!(translated.of_kernel(KERNEL_HZ), 1_000_000_000);
        let stamp = translated.of_kernel(28_155_671_796_901);
        assert!(
            (1_173_152_991_501_583..=1_173_152_991_549_999).contains(&stamp),
            "{stamp}"
        );
        assert_eq!(
            translated.of_kernel(u64::MAX),
            u64::MAX,
            "saturates past u64"
        );
        // The thread's time libc gave the translated probe, 1,377,054 ticks read as nanoseconds.
        assert_eq!(translated.at_kernel_rate(1_377_054), 57_377_250);
        assert_eq!(
            translated.age_ns(5, 1_000),
            0,
            "a stamp after now has no age"
        );
    }

    /// The same instant, a stamp's age read by a native process and by a translated one: the native age is
    /// the span in the kernel's ticks at 125/3, rounded down; the translated one is its own clock (Rosetta's
    /// 125/3 of the kernel's, rounded down) less the stamp carried in (rounded down), so at most the
    /// nanosecond one more rounding takes past it.
    #[test]
    fn a_translated_process_and_a_native_one_read_one_age() {
        let native = MachTicks::new(125, 3, KERNEL_HZ).unwrap();
        let translated = MachTicks::new(1, 1, KERNEL_HZ).unwrap();
        let rosetta = |kernel_ticks: u64| {
            u64::try_from(u128::from(kernel_ticks) * 125 / 3).unwrap_or(u64::MAX)
        };
        let instants = instants();
        for (&a, &b) in instants.iter().zip(instants.iter().rev()) {
            // Only instants whose translated readings fit a u64, as a host's uptime does for 14,000 years.
            let (now, then) = (a.max(b) >> 6, a.min(b) >> 6);
            let read_natively = native.age_ns(now, then);
            let read_translated = translated.age_ns(rosetta(now), then);
            assert!(
                read_natively <= read_translated && read_translated <= read_natively + 1,
                "now {now} then {then}: native {read_natively}, translated {read_translated}"
            );
        }
    }

    /// A zero rate, the kernel's or either half of the timebase, has no reading.
    #[test]
    fn a_zero_rate_has_no_ticks() {
        assert_eq!(MachTicks::new(0, 3, KERNEL_HZ), None);
        assert_eq!(MachTicks::new(125, 0, KERNEL_HZ), None);
        assert_eq!(MachTicks::new(125, 3, 0), None);
    }

    /// This host's two rates as this process reads them: one counter where the process runs natively, and
    /// where Rosetta 2 translates it (`sysctl.proc_translated` 1; the name is absent on an Intel Mac) a
    /// kernel tick of 125/3 of its own, Apple silicon's 24 MHz against its 1/1.
    #[cfg(target_os = "macos")]
    #[test]
    fn this_hosts_ticks_are_this_process_s() {
        let mut translated: libc::c_int = 0;
        let mut length = size_of::<libc::c_int>();
        // SAFETY: the name is NUL-terminated; the out pointer and its length name one live local `int`, the
        // sysctl's type; no new value is written. A missing name fails the call and leaves the zero.
        let outcome = unsafe {
            libc::sysctlbyname(
                c"sysctl.proc_translated".as_ptr(),
                (&raw mut translated).cast::<libc::c_void>(),
                &raw mut length,
                std::ptr::null_mut(),
                0,
            )
        };
        let translated = outcome == 0 && translated == 1;
        let ticks = mach_ticks().unwrap();
        if translated {
            assert_eq!(ticks.of_kernel(3), 125, "{ticks:?}");
        } else {
            assert_eq!(ticks.of_kernel(1_000_003), 1_000_003, "{ticks:?}");
        }
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
