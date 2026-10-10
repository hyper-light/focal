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
