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
#[cfg(any(target_os = "macos", test))]
const SCALE_BITS: u32 = 32;

/// The fixed-point scale of a `numer / denom` timebase, rounded to nearest; `None` for a zero ratio.
#[cfg(any(target_os = "macos", test))]
fn scale_of(numer: u32, denom: u32) -> Option<u64> {
    let denom = u64::from(denom);
    u64::from(numer)
        .checked_shl(SCALE_BITS)?
        .checked_add(denom / 2)?
        .checked_div(denom)
        .filter(|scale| *scale > 0)
}

/// Ticks of a timebase whose fixed-point scale is `scale`, as nanoseconds.
#[cfg(any(target_os = "macos", test))]
fn ticks_to_ns(ticks: u64, scale: u64) -> u64 {
    u128::from(ticks)
        .saturating_mul(u128::from(scale))
        .checked_shr(SCALE_BITS)
        .and_then(|ns| u64::try_from(ns).ok())
        .unwrap_or(u64::MAX)
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
