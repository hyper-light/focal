//! The kqueue driver (macOS, BSD): `EVFILT_USER` for kicks, `kevent` with a timeout for the wait
//! [B: kqueue(2)], through rustix's wrappers. macOS has no completion I/O, so later phases drive
//! sockets by readiness through the same `wait`.
//!
//! On macOS a timed wait is a one-shot `EVFILT_TIMER` with `NOTE_CRITICAL` in the wait's own
//! `kevent` call, the wait itself untimed: XNU coalesces a `kevent` timeout as it sees fit, so a
//! 1 ms wait ended at 1.3 ms at the median and a 5 ms one at 6.3 ms, where the critical timer ended
//! within 0.05 ms of its deadline (measured 2026-10-16, docs/runtime.md §3.6). The shard's tick,
//! the most a wait may end late, would be `kevent64`'s `NOTE_LEEWAY`, but XNU serves a kqueue one
//! record format (`EINVAL` for another, `kevent_internal`'s `KQ_KEV32`), the kick's and the
//! readiness registrations are `kevent`'s, and the tick on Apple silicon is a few microseconds, so
//! the timer takes none. rustix's timer filter sets no `NOTE_CRITICAL`, so this call is libc's.
//!
//! The queue is created and its kick event registered by [`prepare`] on whatever thread builds
//! the shard's seed (the registry owns the descriptor and pins each foreign borrow);
//! the driver itself, which holds an event buffer of raw kernel records, is built on the shard's
//! own thread by [`KqueueDriver::from_prepared`]. rustix marks `kevent` unsafe because the
//! output buffer is filled by the kernel; the three calls here pass a change list of valid
//! events and a buffer with the capacity the call may fill.
#![allow(unsafe_code)]

use std::os::fd::OwnedFd;

use crate::driver::KickFd;
#[cfg(not(target_os = "macos"))]
use std::time::Duration;

use rustix::event::kqueue::{
    Event, EventFilter, EventFlags, UserDefinedFlags, UserFlags, kevent, kqueue,
};

use crate::driver::{Completion, Driver, DriverKind, Kick, nanos_since, refused};
use crate::error::RtError;
use crate::interests::Readiness;

/// Format: the identifier of the kick event on the queue.
const KICK_IDENT: isize = 0;

/// Shape: events drained per wait; a wait that fills the buffer returns and the next wait drains
/// the rest (kqueue keeps them), so the size bounds latency, not correctness.
const EVENTS_PER_WAIT: usize = 64;

/// Format: the identifier of a timed wait's timer on the queue (macOS); filters keep their own
/// identifiers, so the kick's 0 is not in its way.
#[cfg(target_os = "macos")]
const TIMER_IDENT: usize = 1;

/// The driver.
pub struct KqueueDriver {
    kq: KickFd,
    /// The shard-clock reading the driver counts its clock from (`machine::clock::shard_clock_ns`).
    epoch: u64,
    #[cfg(not(target_os = "macos"))]
    events: Vec<Event>,
    /// The records the wait's `kevent` fills (macOS).
    #[cfg(target_os = "macos")]
    events: Vec<libc::kevent>,
    nops: Vec<u64>,
    /// A timed wait's timer (macOS).
    #[cfg(target_os = "macos")]
    timer: Timer,
}

/// A timed wait's one-shot timer: whether one a wait armed has not yet fired.
#[cfg(target_os = "macos")]
#[derive(Debug, Default)]
struct Timer {
    armed: bool,
}

/// The change that arms (`EV_ADD | EV_ONESHOT`, replacing one armed) or removes (`EV_DELETE`) the
/// timer, `ns` from now.
#[cfg(target_os = "macos")]
fn timer_change(flags: u16, ns: u64) -> libc::kevent {
    libc::kevent {
        ident: TIMER_IDENT,
        filter: libc::EVFILT_TIMER,
        flags,
        fflags: libc::NOTE_NSECONDS | libc::NOTE_CRITICAL,
        data: isize::try_from(ns).unwrap_or(isize::MAX),
        udata: std::ptr::null_mut(),
    }
}

impl std::fmt::Debug for KqueueDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KqueueDriver")
            .field("events_capacity", &self.events.capacity())
            .finish()
    }
}

fn user_event(flags: UserFlags, event_flags: EventFlags) -> Event {
    Event::new(
        EventFilter::User {
            ident: KICK_IDENT,
            flags,
            user_flags: UserDefinedFlags::new(0),
        },
        event_flags,
        std::ptr::null_mut(),
    )
}

/// Creates the queue and registers the kick event; the descriptor is owned by the registry slot
/// that the shard registers into (closed at unregistration, never leaked).
pub fn prepare() -> Result<OwnedFd, RtError> {
    let kq = kqueue().map_err(|e| refused("kqueue", e))?;
    let register = user_event(UserFlags::empty(), EventFlags::ADD | EventFlags::CLEAR);
    let mut none: Vec<Event> = Vec::new();
    // SAFETY: the change list is one valid event; the empty output vector receives nothing.
    unsafe { kevent(&kq, &[register], &mut none, None) }
        .map_err(|e| refused("kevent(EV_ADD EVFILT_USER)", e))?;
    Ok(kq)
}

/// Triggers the kick event on `kq`; safe from any thread. A closed slot (the shard unregistered)
/// makes it a no-op.
pub fn trigger(kq: &KickFd) {
    let _ = kq.with(|kq| {
        let trigger = user_event(UserFlags::TRIGGER, EventFlags::empty());
        let mut none: Vec<Event> = Vec::new();
        // SAFETY: one valid change record on an open queue; a closed queue returns an error we ignore.
        let _ = unsafe { kevent(kq, &[trigger], &mut none, None) };
    });
}

impl KqueueDriver {
    /// Builds the driver over the slot's prepared queue, on the shard's thread.
    pub fn from_prepared(kq: KickFd) -> KqueueDriver {
        KqueueDriver {
            kq,
            epoch: crate::machine::clock::shard_clock_ns(),
            events: Vec::with_capacity(EVENTS_PER_WAIT),
            nops: Vec::new(),
            #[cfg(target_os = "macos")]
            timer: Timer::default(),
        }
    }

    /// Waits for events into `out`, for at most `timeout_ns` (none: until one).
    #[cfg(not(target_os = "macos"))]
    fn collect(
        &mut self,
        timeout_ns: Option<u64>,
        out: &mut Vec<Completion>,
    ) -> Result<(), RtError> {
        let timeout = timeout_ns.map(Duration::from_nanos);
        self.events.clear();
        let outcome = self
            .kq
            .with(|kq| {
                // SAFETY: no changes; the output buffer is the vector's spare capacity.
                unsafe {
                    kevent(
                        kq,
                        &[],
                        rustix::buffer::spare_capacity(&mut self.events),
                        timeout,
                    )
                }
            })
            .ok_or(RtError::DriverLost)?;
        if let Err(e) = outcome {
            return match e {
                rustix::io::Errno::INTR => Ok(()),
                rustix::io::Errno::BADF => Err(RtError::DriverLost),
                other => Err(refused("kevent", other)),
            };
        }
        // Kick events carry nothing; other filters become completions keyed by udata.
        for ev in &self.events {
            let fired = match ev.filter() {
                EventFilter::User { .. } => continue,
                EventFilter::Write(_) => Readiness::WRITE,
                _ => Readiness::READ,
            };
            out.push(Completion {
                user_data: u64::try_from(ev.udata().addr()).unwrap_or(0),
                result: fired.bits(),
            });
        }
        Ok(())
    }

    /// Waits for events into `out`, for at most `timeout_ns` (none: until one), the timeout a
    /// one-shot timer in the same call: a timed wait arms it (replacing one an earlier wait left)
    /// and waits untimed, a zero timeout needs none, and an untimed wait removes the one an earlier
    /// wait left, which would end it.
    #[cfg(target_os = "macos")]
    fn collect(
        &mut self,
        timeout_ns: Option<u64>,
        out: &mut Vec<Completion>,
    ) -> Result<(), RtError> {
        use std::os::fd::AsRawFd;
        let change = match timeout_ns {
            Some(0) => None,
            Some(ns) => Some(timer_change(libc::EV_ADD | libc::EV_ONESHOT, ns)),
            None if self.timer.armed => Some(timer_change(libc::EV_DELETE, 0)),
            None => None,
        };
        let zero = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let timeout = if timeout_ns == Some(0) {
            std::ptr::from_ref(&zero)
        } else {
            std::ptr::null()
        };
        let changes = change.as_slice();
        let nchanges = libc::c_int::try_from(changes.len()).unwrap_or(0);
        self.events.clear();
        let capacity = libc::c_int::try_from(self.events.capacity()).unwrap_or(libc::c_int::MAX);
        let events = self.events.as_mut_ptr();
        let outcome = self
            .kq
            .with(|kq| {
                // SAFETY: the change list is `nchanges` valid records; the output buffer has room
                // for `capacity` records, which the kernel fills at most; the timeout, when given,
                // is a live timespec read during the call.
                unsafe {
                    libc::kevent(
                        kq.as_raw_fd(),
                        changes.as_ptr(),
                        nchanges,
                        events,
                        capacity,
                        timeout,
                    )
                }
            })
            .ok_or(RtError::DriverLost)?;
        // A change is applied before the call waits, so an interrupted wait applied it too.
        match timeout_ns {
            Some(0) => {}
            Some(_) => self.timer.armed = true,
            None => self.timer.armed = false,
        }
        if outcome < 0 {
            let raw = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            return match rustix::io::Errno::from_raw_os_error(raw) {
                rustix::io::Errno::INTR => Ok(()),
                rustix::io::Errno::BADF => Err(RtError::DriverLost),
                other => Err(refused("kevent", other)),
            };
        }
        let filled = usize::try_from(outcome).unwrap_or(0);
        if filled > self.events.capacity() {
            return Err(refused("kevent", rustix::io::Errno::INVAL));
        }
        // SAFETY: the kernel wrote `filled` records, at most the buffer's capacity (checked), into
        // its first places.
        unsafe { self.events.set_len(filled) };
        for ev in &self.events {
            if ev.filter == libc::EVFILT_TIMER && ev.ident == TIMER_IDENT {
                // The timeout passed (or, `EV_ERROR`, a timer already fired was not there to
                // remove): the shard's clock tells which timers are due.
                self.timer.armed = false;
                continue;
            }
            if ev.flags & libc::EV_ERROR != 0 {
                // Only the timer is changed here, so an error names nothing else.
                return Err(refused(
                    "kevent change",
                    rustix::io::Errno::from_raw_os_error(i32::try_from(ev.data).unwrap_or(0)),
                ));
            }
            // Kick events carry nothing; other filters become completions keyed by udata.
            let fired = match ev.filter {
                libc::EVFILT_USER => continue,
                libc::EVFILT_WRITE => Readiness::WRITE,
                _ => Readiness::READ,
            };
            out.push(Completion {
                user_data: u64::try_from(ev.udata.addr()).unwrap_or(0),
                result: fired.bits(),
            });
        }
        Ok(())
    }
}

impl Driver for KqueueDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Kqueue
    }

    fn kick_handle(&self) -> Kick {
        Kick::Kqueue(self.kq)
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
        self.collect(timeout_ns, out)
    }

    fn submit_nop(&mut self, user_data: u64) -> Result<(), RtError> {
        self.nops.push(user_data);
        Ok(())
    }

    fn arm(&mut self, raw: i32, want: Readiness, tag: u64) -> Result<(), RtError> {
        // One filter per direction, each its own registration (ident and filter), so arming one never
        // touches the other; a filter armed earlier and no longer wanted fires once, to no waiter.
        let udata = core::ptr::without_provenance_mut(usize::try_from(tag).unwrap_or(usize::MAX));
        let read = want.contains(Readiness::READ).then(|| {
            Event::new(
                EventFilter::Read(raw),
                EventFlags::ADD | EventFlags::ONESHOT,
                udata,
            )
        });
        let write = want.contains(Readiness::WRITE).then(|| {
            Event::new(
                EventFilter::Write(raw),
                EventFlags::ADD | EventFlags::ONESHOT,
                udata,
            )
        });
        let mut none: Vec<Event> = Vec::new();
        for change in [read, write].into_iter().flatten() {
            self.kq
                .with(|kq| {
                    // SAFETY: one valid change record on the open queue; the output buffer receives nothing.
                    unsafe { kevent(kq, &[change], &mut none, None) }
                })
                .ok_or(RtError::DriverLost)?
                .map_err(|e| refused("kevent(EV_ADD)", e))?;
        }
        Ok(())
    }

    fn has_pending(&self) -> bool {
        !self.nops.is_empty()
    }
}

#[cfg(all(test, target_os = "macos"))]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::panic,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::indexing_slicing
    )
)]
mod tests {
    use super::*;

    /// Shape: a timed wait's timeout.
    const WAIT_NS: u64 = 5_000_000;

    /// A timed wait is XNU's critical timer, not a `kevent` timeout XNU may coalesce (a 5 ms wait
    /// ended at 6.3 ms at the median that way, the critical timer within 0.05 ms of its deadline:
    /// measured 2026-10-16, docs/runtime.md §3.6): the change a timed wait arms is a one-shot
    /// `EVFILT_TIMER` in nanoseconds with `NOTE_CRITICAL`, for the wait's own timeout, and a wait with
    /// nothing else to end it is ended by that timer, which fires. The check is the armed record's,
    /// exact; how late a
    /// loaded host then runs the woken thread is the host's, which a median over its wakes judged
    /// (it failed at a load average of 29, its wakes 8.5 to 9.4 ms late, 2026-10-17). And an untimed
    /// wait after a timed one is not ended by the timer that one left.
    #[test]
    fn a_timed_wait_is_a_critical_timer_for_its_own_timeout() {
        let change = timer_change(libc::EV_ADD | libc::EV_ONESHOT, WAIT_NS);
        // Copied out: `kevent` is packed on some targets, so its fields are not borrowed.
        let (ident, filter, flags, fflags, data) = (
            change.ident,
            change.filter,
            change.flags,
            change.fflags,
            change.data,
        );
        assert_eq!(filter, libc::EVFILT_TIMER);
        assert_eq!(ident, TIMER_IDENT);
        assert_eq!(flags, libc::EV_ADD | libc::EV_ONESHOT);
        assert_eq!(
            fflags & (libc::NOTE_NSECONDS | libc::NOTE_CRITICAL),
            libc::NOTE_NSECONDS | libc::NOTE_CRITICAL,
            "nanoseconds, critical: no coalescing"
        );
        assert_eq!(data, WAIT_NS as isize);
        let kq = prepare().unwrap();
        let registration =
            crate::registry::register(2, 1, crate::runtime::register_kick(Some(kq))).unwrap();
        let kick = crate::registry::with_entry(registration.shard(), |entry| entry.kick).unwrap();
        let Kick::Kqueue(fd) = kick else {
            panic!("the kqueue driver's kick is its queue");
        };
        let mut driver = KqueueDriver::from_prepared(fd);
        let mut out = Vec::new();
        driver.wait(Some(WAIT_NS), &mut out).unwrap();
        assert!(out.is_empty(), "nothing but the timer ended the wait");
        assert!(!driver.timer.armed, "the timer that ended the wait fired");
        // A timed wait a kick ends leaves its timer armed; the untimed wait after it removes it.
        kick.kick();
        driver.wait(Some(1_000_000_000), &mut out).unwrap();
        assert!(driver.timer.armed);
        kick.kick();
        driver.wait(None, &mut out).unwrap();
        assert!(!driver.timer.armed);
        drop(driver);
        drop(registration);
    }
}
