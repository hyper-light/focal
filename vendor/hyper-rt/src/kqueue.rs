//! The kqueue driver (macOS, BSD): `EVFILT_USER` for kicks, `kevent` with a timeout for the wait
//! [B: kqueue(2)], through rustix's wrappers. macOS has no completion I/O, so later phases drive
//! sockets by readiness through the same `wait`.
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

/// The driver.
pub struct KqueueDriver {
    kq: KickFd,
    /// The host monotonic reading the driver counts its clock from.
    epoch: u64,
    events: Vec<Event>,
    nops: Vec<u64>,
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
            epoch: crate::machine::clock::monotonic_ns(),
            events: Vec::with_capacity(EVENTS_PER_WAIT),
            nops: Vec::new(),
        }
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
        let mut timeout = timeout_ns.map(Duration::from_nanos);
        if !self.nops.is_empty() {
            out.extend(self.nops.drain(..).map(|user_data| Completion {
                user_data,
                result: 0,
            }));
            timeout = Some(Duration::ZERO);
        }
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
        // Kick events carry nothing; other filters (later phases) become completions keyed by udata.
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
