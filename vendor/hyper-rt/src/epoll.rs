//! The epoll driver (Linux's one driver; io_uring is not carried, docs/runtime.md §3.7): an eventfd for kicks registered on an epoll instance, `epoll_wait` with a
//! timeout for the wait [B: epoll(7); B: eventfd(2)], through rustix's safe wrappers. The one unsafe
//! idiom is borrowing a caller-owned socket fd by number for a single `epoll_ctl` readiness
//! registration (a `UdpSocket` recv, a `TcpStream` read or write), each with a `// SAFETY:` note.
#![allow(unsafe_code)]

use std::os::fd::OwnedFd;

use rustix::event::epoll::{self, CreateFlags, EventData, EventFlags};

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

/// Shape: events drained per wait (see the kqueue driver for the reasoning).
const EVENTS_PER_WAIT: usize = 64;

/// The driver.
pub struct EpollDriver {
    epfd: OwnedFd,
    efd: crate::driver::KickFd,
    /// The shard-clock reading the driver counts its clock from (`machine::clock::shard_clock_ns`).
    epoch: u64,
    events: Vec<epoll::Event>,
    nops: Vec<u64>,
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
        Ok(EpollDriver {
            epfd,
            efd,
            epoch: crate::machine::clock::shard_clock_ns(),
            events: Vec::with_capacity(EVENTS_PER_WAIT),
            nops: Vec::new(),
        })
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
        let mut timeout = timeout_ns.map(timespec);
        if !self.nops.is_empty() {
            out.extend(self.nops.drain(..).map(|user_data| Completion {
                user_data,
                result: 0,
            }));
            timeout = Some(timespec(0));
        }
        self.events.clear();
        let outcome = epoll::wait(
            &self.epfd,
            rustix::buffer::spare_capacity(&mut self.events),
            timeout.as_ref(),
        );
        if let Err(e) = outcome {
            return match e {
                rustix::io::Errno::INTR => Ok(()),
                rustix::io::Errno::BADF => Err(RtError::DriverLost),
                other => Err(refused("epoll_wait", other)),
            };
        }
        let mut kicked = false;
        for ev in &self.events {
            // The event record is packed: copy the fields out before touching them.
            let (data, flags) = (ev.data, ev.flags);
            if data.u64() == KICK_TAG {
                kicked = true;
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

fn timespec(ns: u64) -> rustix::event::Timespec {
    /// Format: nanoseconds per second.
    const NANOS_PER_SECOND: u64 = 1_000_000_000;
    rustix::event::Timespec {
        tv_sec: i64::try_from(ns / NANOS_PER_SECOND).unwrap_or(i64::MAX),
        tv_nsec: i64::try_from(ns % NANOS_PER_SECOND).unwrap_or(0),
    }
}
