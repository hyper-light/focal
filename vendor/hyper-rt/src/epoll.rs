//! The epoll driver (Linux's one driver; io_uring is not carried, docs/runtime.md §3.7): an eventfd for kicks registered on an epoll instance, and a wait
//! that keeps its timeout to the nanosecond [B: epoll(7); B: eventfd(2)], through rustix's safe
//! wrappers where rustix has one. The unsafe idioms, each with a `// SAFETY:` note: borrowing a
//! caller-owned socket fd by number for a single `epoll_ctl` readiness registration (a `UdpSocket`
//! recv, a `TcpStream` read or write), and `epoll_pwait2(2)` through `libc::syscall` ([`Precise`]).
#![allow(unsafe_code)]

use std::os::fd::{AsRawFd, OwnedFd};

use rustix::event::epoll::{self, CreateFlags, EventData, EventFlags};
use rustix::io::Errno;

use crate::driver::{Completion, Driver, DriverKind, Kick, nanos_since, refused};
use crate::error::RtError;
use crate::interests::Readiness;

/// Creates the kick eventfd both Linux drivers wake on; the registry owns it until every shard driver has
/// retired.
pub fn prepare_eventfd() -> Result<OwnedFd, RtError> {
    rustix::event::eventfd(
        0,
        rustix::event::EventfdFlags::CLOEXEC | rustix::event::EventfdFlags::NONBLOCK,
    )
    .map_err(|e| refused("eventfd", e))
}

/// Format: the user word that marks the kick eventfd in epoll events.
const KICK_TAG: u64 = u64::MAX;
/// Format: the user word that marks the precise wait's timerfd in epoll events: descriptor -2's
/// handle tag ([`crate::interests::tag_of`]), which names no descriptor, as the kick's names -1.
const TIMER_TAG: u64 = u64::MAX - 1;

/// Shape: events drained per wait (see the kqueue driver for the reasoning).
const EVENTS_PER_WAIT: usize = 64;

/// How a wait keeps its timeout to the nanosecond. `epoll_wait(2)` and `epoll_pwait(2)` take
/// milliseconds, and rustix, built without its `linux_5_11` feature (with it, every wait fails on an
/// older kernel), rounds a finer timeout up to the next millisecond: a timer the wheel had due in
/// 50 µs fired a millisecond or more late, whatever the kernel's own timers could keep.
enum Precise {
    /// `epoll_pwait2(2)` (Linux 5.11): the timeout as a timespec.
    Pwait2,
    /// A kernel before 5.11, or a seccomp filter that refuses the call: a timerfd(2) registered on
    /// the instance, armed at each timed wait's timeout, the wait itself untimed — libevent's
    /// precise timer (`EVENT_BASE_FLAG_PRECISE_TIMER`, its epoll.c). `armed` while a timeout set on
    /// it has not fired.
    Timerfd { fd: OwnedFd, armed: bool },
}

/// The kernel's `struct __kernel_timespec`, sixty-four-bit fields on every architecture, which
/// `epoll_pwait2(2)` reads (include/uapi/linux/time_types.h).
#[repr(C)]
struct KernelTimespec {
    tv_sec: i64,
    tv_nsec: i64,
}

/// The driver.
pub struct EpollDriver {
    epfd: OwnedFd,
    efd: crate::driver::KickFd,
    /// The shard-clock reading the driver counts its clock from (`machine::clock::shard_clock_ns`).
    epoch: u64,
    events: Vec<epoll::Event>,
    nops: Vec<u64>,
    precise: Precise,
    /// The timer slack the shard's thread had before [`Driver::set_timer_slack_ns`] set its own,
    /// restored when the driver goes (the thread may outlive the shard: `block_on`).
    slack_before: Option<u64>,
}

impl Drop for EpollDriver {
    fn drop(&mut self) {
        if let Some(before) = self.slack_before.take() {
            // The thread's own setting, put back; a refusal leaves the shard's, which only ends
            // its sleeps sooner.
            let _ = rustix::thread::set_current_timer_slack(std::num::NonZeroU64::new(before));
        }
    }
}

impl std::fmt::Debug for EpollDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpollDriver")
            .field("events_capacity", &self.events.capacity())
            .finish()
    }
}

impl EpollDriver {
    /// Creates the instance over its registry-owned eventfd. The owning runtime retires the
    /// descriptor after this driver and all foreign kick borrows have ended.
    pub fn with_eventfd(efd: crate::driver::KickFd) -> Result<EpollDriver, RtError> {
        let epfd = epoll::create(CreateFlags::CLOEXEC).map_err(|e| refused("epoll_create1", e))?;
        efd.with(|fd| epoll::add(&epfd, fd, EventData::new_u64(KICK_TAG), EventFlags::IN))
            .ok_or(RtError::DriverLost)?
            .map_err(|e| refused("epoll_ctl(ADD eventfd)", e))?;
        let mut events = Vec::with_capacity(EVENTS_PER_WAIT);
        let precise = Self::precise(&epfd, &mut events)?;
        // The probe's events (a kick sent before this driver existed) are level-triggered: the
        // first wait reports them again.
        events.clear();
        Ok(EpollDriver {
            epfd,
            efd,
            epoch: crate::machine::clock::shard_clock_ns(),
            events,
            nops: Vec::new(),
            precise,
            slack_before: None,
        })
    }

    /// The precise wait this kernel allows: `epoll_pwait2` where a zero-timeout call is taken (an
    /// interrupted one was taken too), else a timerfd on the instance. The kernel answers `ENOSYS`
    /// before 5.11; a seccomp filter that does not know the call answers `EPERM` (Docker's default
    /// profile before it listed the call) or `ENOSYS`.
    fn precise(epfd: &OwnedFd, events: &mut Vec<epoll::Event>) -> Result<Precise, RtError> {
        match pwait2(epfd, events, Some(&kernel_timespec(0))) {
            Ok(()) | Err(Errno::INTR) => Ok(Precise::Pwait2),
            Err(Errno::NOSYS | Errno::PERM) => {
                let fd = rustix::time::timerfd_create(
                    rustix::time::TimerfdClockId::Monotonic,
                    rustix::time::TimerfdFlags::CLOEXEC | rustix::time::TimerfdFlags::NONBLOCK,
                )
                .map_err(|e| refused("timerfd_create", e))?;
                // Edge-triggered: each expiry is one event, and the timerfd is never read (a new
                // setting clears its count, timerfd(2)).
                epoll::add(
                    epfd,
                    &fd,
                    EventData::new_u64(TIMER_TAG),
                    EventFlags::IN | EventFlags::ET,
                )
                .map_err(|e| refused("epoll_ctl(ADD timerfd)", e))?;
                Ok(Precise::Timerfd { fd, armed: false })
            }
            Err(other) => Err(refused("epoll_pwait2", other)),
        }
    }

    /// Waits for events into the buffer, for at most `timeout_ns` (none: until one) kept to the
    /// nanosecond. The inner error is the wait's own, with the call that answered it.
    fn collect(
        &mut self,
        timeout_ns: Option<u64>,
    ) -> Result<Result<(), (Errno, &'static str)>, RtError> {
        self.events.clear();
        let (fd, armed) = match &mut self.precise {
            Precise::Pwait2 => {
                let timeout = timeout_ns.map(kernel_timespec);
                return Ok(pwait2(&self.epfd, &mut self.events, timeout.as_ref())
                    .map_err(|e| (e, "epoll_pwait2")));
            }
            Precise::Timerfd { fd, armed } => (fd, armed),
        };
        // A zero timeout needs no timer; a timed wait arms it and waits untimed; an untimed wait
        // first disarms what an earlier one left, or that timer would end this one.
        let timeout = match timeout_ns {
            Some(0) => Some(timespec(0)),
            Some(ns) => {
                Self::set_timer(fd, ns)?;
                *armed = true;
                None
            }
            None if *armed => {
                Self::set_timer(fd, 0)?;
                *armed = false;
                None
            }
            None => None,
        };
        Ok(epoll::wait(
            &self.epfd,
            rustix::buffer::spare_capacity(&mut self.events),
            timeout.as_ref(),
        )
        .map(|_| ())
        .map_err(|e| (e, "epoll_wait")))
    }

    /// The timerfd's next expiry `ns` from now; zero disarms it.
    fn set_timer(fd: &OwnedFd, ns: u64) -> Result<(), RtError> {
        rustix::time::timerfd_settime(
            fd,
            rustix::time::TimerfdTimerFlags::empty(),
            &rustix::time::Itimerspec {
                it_interval: timespec(0),
                it_value: timespec(ns),
            },
        )
        .map(|_| ())
        .map_err(|e| refused("timerfd_settime", e))
    }

    fn drain_kick(&self) {
        let mut word = [0u8; size_of::<u64>()];
        let _ = self.efd.with(|efd| rustix::io::read(efd, &mut word));
    }
}

impl Driver for EpollDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Epoll
    }

    fn kick_handle(&self) -> Kick {
        Kick::Eventfd(self.efd)
    }

    fn now_ns(&self) -> u64 {
        nanos_since(self.epoch)
    }

    fn clock(&self) -> crate::driver::Clock {
        crate::driver::Clock::Since(self.epoch)
    }

    fn wait(&mut self, timeout_ns: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        let mut timeout_ns = timeout_ns;
        if !self.nops.is_empty() {
            out.extend(self.nops.drain(..).map(|user_data| Completion {
                user_data,
                result: 0,
            }));
            timeout_ns = Some(0);
        }
        if let Err((e, call)) = self.collect(timeout_ns)? {
            return match e {
                Errno::INTR => Ok(()),
                Errno::BADF => Err(RtError::DriverLost),
                other => Err(refused(call, other)),
            };
        }
        let mut kicked = false;
        for ev in &self.events {
            // The event record is packed: copy the fields out before touching them.
            let (data, flags) = (ev.data, ev.flags);
            if data.u64() == KICK_TAG {
                kicked = true;
            } else if data.u64() == TIMER_TAG {
                // The timeout passed: the shard's clock tells which timers are due.
                if let Precise::Timerfd { armed, .. } = &mut self.precise {
                    *armed = false;
                }
            } else {
                // An error or a hang-up is both directions' news: the waiter's call reports it.
                let broken = flags.intersects(EventFlags::ERR | EventFlags::HUP);
                let mut fired = Readiness::NONE;
                if broken || flags.intersects(EventFlags::IN | EventFlags::PRI | EventFlags::RDHUP)
                {
                    fired = fired.union(Readiness::READ);
                }
                if broken || flags.contains(EventFlags::OUT) {
                    fired = fired.union(Readiness::WRITE);
                }
                out.push(Completion {
                    user_data: data.u64(),
                    result: fired.bits(),
                });
            }
        }
        if kicked {
            self.drain_kick();
        }
        Ok(())
    }

    fn submit_nop(&mut self, user_data: u64) -> Result<(), RtError> {
        self.nops.push(user_data);
        Ok(())
    }

    fn arm(&mut self, raw: i32, want: Readiness, tag: u64) -> Result<(), RtError> {
        let mut flags = EventFlags::ONESHOT;
        if want.contains(Readiness::READ) {
            flags |= EventFlags::IN;
        }
        if want.contains(Readiness::WRITE) {
            flags |= EventFlags::OUT;
        }
        self.arm_flags(raw, tag, flags)
    }

    fn has_pending(&self) -> bool {
        !self.nops.is_empty()
    }

    /// The calling thread's timer slack (prctl(2) `PR_SET_TIMERSLACK`), which the kernel adds to
    /// every timed wait of the thread for coalescing: 50 µs by default, or a thousandth of the
    /// timeout (fs/select.c `select_estimate_accuracy`).
    fn set_timer_slack_ns(&mut self, slack_ns: u64) -> Result<(), RtError> {
        let before = rustix::thread::current_timer_slack()
            .map_err(|e| refused("prctl(PR_GET_TIMERSLACK)", e))?;
        rustix::thread::set_current_timer_slack(std::num::NonZeroU64::new(slack_ns))
            .map_err(|e| refused("prctl(PR_SET_TIMERSLACK)", e))?;
        self.slack_before.get_or_insert(before);
        Ok(())
    }
}

impl EpollDriver {
    /// Arms one-shot `flags` interest on `raw` under the waker word `user_data`: `EPOLL_CTL_ADD` for a
    /// descriptor this epoll instance has not seen, and `EPOLL_CTL_MOD` for one it has — a one-shot
    /// registration is *disabled* after it fires, not removed, so the descriptor stays in the interest
    /// list and a second `ADD` is refused `EEXIST` (epoll(7): "EPOLLONESHOT … the user must call
    /// epoll_ctl with EPOLL_CTL_MOD to rearm"). Every await re-arms, so a receive loop's second await
    /// — the fleet's serve sockets after their first datagram — is the `MOD`
    /// (docs/bugs/2026-09-14-epoll-readiness-re-add-eexist.md).
    /// One registration of `raw` with `flags`, replacing any earlier one: `MOD` when it is registered, `ADD`
    /// when it is not (a fresh descriptor, or one closed and reused since).
    fn arm_flags(&self, raw: i32, user_data: u64, flags: EventFlags) -> Result<(), RtError> {
        // SAFETY: `raw` is a live socket the caller (a UdpSocket or TcpStream) owns for the registration;
        // the borrow is used only for these epoll_ctl calls and not retained.
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(raw) };
        let data = EventData::new_u64(user_data);
        match epoll::modify(&self.epfd, fd, data, flags) {
            Ok(()) => Ok(()),
            Err(rustix::io::Errno::NOENT) => {
                epoll::add(&self.epfd, fd, data, flags).map_err(|e| refused("epoll_ctl(ADD)", e))
            }
            Err(e) => Err(refused("epoll_ctl(MOD)", e)),
        }
    }
}

/// Format: nanoseconds per second.
const NANOS_PER_SECOND: u64 = 1_000_000_000;

fn timespec(ns: u64) -> rustix::event::Timespec {
    rustix::event::Timespec {
        tv_sec: i64::try_from(ns / NANOS_PER_SECOND).unwrap_or(i64::MAX),
        tv_nsec: i64::try_from(ns % NANOS_PER_SECOND).unwrap_or(0),
    }
}

fn kernel_timespec(ns: u64) -> KernelTimespec {
    KernelTimespec {
        tv_sec: i64::try_from(ns / NANOS_PER_SECOND).unwrap_or(i64::MAX),
        tv_nsec: i64::try_from(ns % NANOS_PER_SECOND).unwrap_or(0),
    }
}

/// `epoll_pwait2(2)` on `epfd`: up to the buffer's capacity of events into `events` (cleared
/// first), waiting at most `timeout`, or until an event without one. rustix carries the call only
/// behind its `linux_5_11` feature, which would also make every wait fail on an older kernel; the
/// raw call lets the driver ask once and keep the timerfd where the kernel refuses it.
fn pwait2(
    epfd: &OwnedFd,
    events: &mut Vec<epoll::Event>,
    timeout: Option<&KernelTimespec>,
) -> Result<(), Errno> {
    events.clear();
    let capacity = libc::c_int::try_from(events.capacity()).unwrap_or(libc::c_int::MAX);
    let timeout = timeout.map_or(std::ptr::null(), std::ptr::from_ref);
    // SAFETY: the buffer has room for `capacity` events and the kernel writes at most that many;
    // rustix's `epoll::Event` is the kernel's `struct epoll_event` (packed on x86-64, as the
    // kernel's is). The timeout, when given, is a live `__kernel_timespec` read during the call. No
    // signal mask is passed (NULL), so the kernel reads no mask and not its size (fs/eventpoll.c,
    // `set_user_sigmask`).
    let n = unsafe {
        libc::syscall(
            libc::SYS_epoll_pwait2,
            epfd.as_raw_fd(),
            events.as_mut_ptr(),
            capacity,
            timeout,
            std::ptr::null::<libc::sigset_t>(),
            0_usize,
        )
    };
    if n < 0 {
        let raw = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        return Err(Errno::from_raw_os_error(raw));
    }
    let n = usize::try_from(n).map_err(|_| Errno::INVAL)?;
    if n > events.capacity() {
        return Err(Errno::INVAL);
    }
    // SAFETY: the kernel wrote `n` events, at most the buffer's capacity (checked), into its first
    // places.
    unsafe { events.set_len(n) };
    Ok(())
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::panic,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
mod tests {
    use super::*;

    /// Shape: a timed wait's timeout, well under the millisecond a rounding wait could not end
    /// before.
    const WAIT_NS: u64 = 200_000;
    /// Shape: the millisecond epoll_wait(2)'s timeout counts in: a wait rounded up to it never ends
    /// sooner, whatever the kernel's timers keep.
    const MILLISECOND_NS: u64 = 1_000_000;
    /// Shape: timed waits each mode makes; one ending between its timeout and the millisecond is
    /// the fact sought, so a busy machine's late wakes only ask for more of them.
    const TRIES: usize = 64;

    /// The shard's tick is its thread's timer slack while its driver lives, and the thread's own
    /// slack is back once the driver goes: the slack is the kernel's coalescing of every timed wait
    /// of the thread (prctl(2)), which outlives a shard on a borrowed thread (`block_on`).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_shard_s_slack_is_its_thread_s_until_its_driver_goes() {
        /// Shape: a slack no default is, so its reading is the driver's.
        const SLACK_NS: u64 = 12_345;
        let before = rustix::thread::current_timer_slack().unwrap();
        let efd = prepare_eventfd().unwrap();
        let registration =
            crate::registry::register(2, 1, crate::runtime::register_kick(Some(efd))).unwrap();
        let kick = crate::registry::with_entry(registration.shard(), |entry| entry.kick).unwrap();
        let crate::driver::Kick::Eventfd(efd) = kick else {
            panic!("the epoll driver's kick is its eventfd");
        };
        let mut driver = EpollDriver::with_eventfd(efd).unwrap();
        driver.set_timer_slack_ns(SLACK_NS).unwrap();
        assert_eq!(rustix::thread::current_timer_slack().unwrap(), SLACK_NS);
        drop(driver);
        assert_eq!(rustix::thread::current_timer_slack().unwrap(), before);
        drop(registration);
    }

    /// A timed wait ends at its timeout, not at the millisecond after it, both where the kernel has
    /// `epoll_pwait2` and through the timerfd before it (forced here). Until 2026-10-16 the driver
    /// handed rustix a timespec that rustix rounded up to whole milliseconds for `epoll_pwait`, so a
    /// 200 µs wait took at least 1 ms. The ends are not asked of a kernel whose monotonic clock
    /// resolves no finer than a microsecond: without high-resolution timers (a LinuxKit VM's kernel
    /// leaves CONFIG_HIGH_RES_TIMERS unset; its clock_getres says 1 ms) no wait can end between
    /// ticks.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn a_timed_wait_ends_at_its_timeout_not_the_next_millisecond() {
        let resolution = rustix::time::clock_getres(rustix::time::ClockId::Monotonic);
        let resolution_ns = u64::try_from(resolution.tv_sec).unwrap() * NANOS_PER_SECOND
            + u64::try_from(resolution.tv_nsec).unwrap();
        // Without high-resolution timers the waits still run, and the timerfd's states are still
        // checked; only their ends cannot be.
        let fine = resolution_ns <= 1_000;
        let efd = prepare_eventfd().unwrap();
        let registration =
            crate::registry::register(2, 1, crate::runtime::register_kick(Some(efd))).unwrap();
        let kick = crate::registry::with_entry(registration.shard(), |entry| entry.kick).unwrap();
        let crate::driver::Kick::Eventfd(efd) = kick else {
            panic!("the epoll driver's kick is its eventfd");
        };
        for force_timerfd in [false, true] {
            let mut driver = EpollDriver::with_eventfd(efd).unwrap();
            if force_timerfd {
                let fd = rustix::time::timerfd_create(
                    rustix::time::TimerfdClockId::Monotonic,
                    rustix::time::TimerfdFlags::CLOEXEC | rustix::time::TimerfdFlags::NONBLOCK,
                )
                .unwrap();
                epoll::add(
                    &driver.epfd,
                    &fd,
                    EventData::new_u64(TIMER_TAG),
                    EventFlags::IN | EventFlags::ET,
                )
                .unwrap();
                driver.precise = Precise::Timerfd { fd, armed: false };
            } else if !matches!(driver.precise, Precise::Pwait2) {
                eprintln!("this kernel refuses epoll_pwait2: the timerfd is its wait");
            }
            let mut out = Vec::new();
            let mut ended = Vec::with_capacity(TRIES);
            for _ in 0..TRIES {
                let began = driver.now_ns();
                driver.wait(Some(WAIT_NS), &mut out).unwrap();
                ended.push(driver.now_ns() - began);
            }
            // A stray kick (the registry's stress tests share the process) ends a wait early; a
            // rounded wait ends at a millisecond or later. Only a precise one ends between.
            if fine {
                assert!(
                    ended
                        .iter()
                        .any(|&took| (WAIT_NS..MILLISECOND_NS).contains(&took)),
                    "timerfd forced {force_timerfd}: no {WAIT_NS} ns wait ended before a millisecond: {ended:?}"
                );
            } else {
                eprintln!(
                    "the monotonic clock resolves {resolution_ns} ns (no high-resolution timers): \
                     timerfd forced {force_timerfd}, waits ended after {ended:?} ns"
                );
            }
            if force_timerfd {
                // A timed wait a kick ends leaves its timer armed; the untimed wait after it
                // disarms it first, so that timer cannot end it (each wait here ends at once on a
                // kick sent before it).
                kick.kick();
                driver.wait(Some(NANOS_PER_SECOND), &mut out).unwrap();
                assert!(matches!(
                    driver.precise,
                    Precise::Timerfd { armed: true, .. }
                ));
                kick.kick();
                driver.wait(None, &mut out).unwrap();
                assert!(matches!(
                    driver.precise,
                    Precise::Timerfd { armed: false, .. }
                ));
            }
        }
        drop(registration);
    }
}
