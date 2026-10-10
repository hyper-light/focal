//! UDP on the runtime (docs/runtime.md §5.1): [`UdpSocket`], one datagram a call, and [`Batched`], the
//! batched socket a transport drives, with a bounded outbox, receives a batch at a time and each datagram's
//! arrival on the shard's clock.
//!
//! `UdpSocket::recv_from` registers one-shot read-readiness with the shard's driver and yields; the driver
//! wakes the task when the socket is readable, then a non-blocking `recvfrom` takes the datagram.
//! `send_to` is a direct non-blocking `sendto`, and a send the OS has no room for is `WouldBlock`, awaited
//! on writability, never a lost path (slates' AUD-29-61).
//!
//! IPv4 and IPv6: an address is `core::net::SocketAddr`; an IPv6 socket is `IPV6_V6ONLY`, so a dual-stack
//! service binds two sockets, stated. On a real runtime the socket is a platform datagram socket through
//! the [`crate::netsys`] seam (`rustix` on Unix, Winsock 2 on Windows — the lint wall reserves
//! `std::net`), so it backs every readiness-native driver: kqueue/epoll on Unix and the IOCP driver's AFD
//! reactor (`crate::afd`) on Windows, one code path. On the simulation runtime it is a port on the
//! deterministic in-memory fabric (`crate::sim`), which is IPv4 loopback ports: the requested address is
//! ignored there, and a datagram's destination is its port.
//!
//! **Astray reports.** A UDP socket reports a datagram sent earlier that went astray (an ICMP refusal or
//! unreachable, Windows' reset for a send to a closed port) on a later call; the report says nothing about
//! the socket and the call that returns it moves no data. Such a call is counted
//! ([`UdpSocket::astray_reports`]) and returns "nothing now", so the caller awaits the edge again: the
//! report is consumed, and the driver reports the socket ready at once when data waits behind it.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

mod batched;

use std::sync::atomic::{AtomicU64, Ordering};

pub use batched::{Arrival, Batched, Io, IoStats, MAX_BATCH, RECEIVE_BYTES, Sent, Slot};

use crate::error::RtError;
use crate::netsys::{self, Family, Socket};
use crate::readiness::{Target, ready};
use crate::registry;

// The address types are `core::net`'s (the same ones the seam and `rustix::net` use), re-exported so
// a caller names an address without depending on the socket backend.
pub use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

/// The owned OS handle [`UdpSocket::adopt`] takes — a file descriptor on Unix, a Winsock socket on
/// Windows; `std::net::UdpSocket` converts into either with `.into()`.
pub type OwnedDatagram = netsys::OwnedDatagram;

/// An async UDP socket: a real OS datagram socket, or a port on the simulation's in-memory fabric.
/// The representation is hidden (the real/sim choice is the driver's, R8); a caller drives it through
/// the methods below, never by matching a backend.
#[derive(Debug)]
pub struct UdpSocket {
    inner: Inner,
    /// Calls that returned an astray report instead of data (module doc). Atomic only so a task that
    /// borrows the socket across an await stays `Send`; one thread counts it.
    astray: AtomicU64,
}

/// The socket's backend: a real OS datagram socket (through the platform seam), or a simulation
/// fabric port. Private — the distinction is internal (`on_sim`), so a consumer sees one type.
#[derive(Debug)]
enum Inner {
    Real {
        socket: Socket,
    },
    Sim {
        index: u16,
        port: u16,
    },
    /// Handed over ([`UdpSocket::into_owned`]): nothing left to close.
    Moved,
}

impl Drop for UdpSocket {
    /// A simulated socket gives its desk slot back (an OS socket closes with its descriptor).
    fn drop(&mut self) {
        if let Inner::Sim { index, .. } = self.inner {
            crate::sim::sim_close(index);
        }
    }
}

/// Whether the current shard runs the simulation driver (so a socket uses the in-memory fabric).
fn on_sim() -> bool {
    registry::with_current(|ctx| ctx.driver_is_sim()).unwrap_or(false)
}

impl UdpSocket {
    /// Binds a non-blocking UDP socket to `addr` (use port 0 for an OS-assigned port, then
    /// [`UdpSocket::local_addr`]). On the simulation runtime the requested address is ignored and a
    /// fabric port is assigned.
    pub fn bind(addr: impl Into<SocketAddr>) -> Result<UdpSocket, RtError> {
        let addr = addr.into();
        if on_sim() {
            let (index, port) = crate::sim::sim_bind()?;
            return Ok(UdpSocket::of(Inner::Sim { index, port }));
        }
        let socket = netsys::dgram_socket(Family::of(addr))?;
        netsys::bind(&socket, addr)?;
        Ok(UdpSocket::of(Inner::Real { socket }))
    }

    fn of(inner: Inner) -> UdpSocket {
        UdpSocket {
            inner,
            astray: AtomicU64::new(0),
        }
    }

    /// Adopts an already-bound OS datagram socket (a `std::net::UdpSocket` converts into
    /// [`OwnedDatagram`]): the socket-activation shape, for a caller that must hold a port from the moment
    /// it learns it until the runtime serves on it. Binding by number instead gives the port up between the
    /// check and the use, and another socket can take it in between (the fleet fixtures' port race,
    /// `docs/bugs/2026-09-28-a-released-test-port-was-taken-before-the-daemon-bound-it.md`). Refused when
    /// the socket is not a datagram socket, is not bound to an internet address and port, or when the current
    /// shard runs the simulation driver (whose sockets are fabric ports, not OS handles).
    pub fn adopt(socket: OwnedDatagram) -> Result<UdpSocket, RtError> {
        if on_sim() {
            return Err(RtError::BadConfig {
                what: "an adopted OS socket on the simulation driver",
            });
        }
        let socket = netsys::adopt(socket)?;
        let bound = netsys::local_addr(&socket)?;
        if bound.port() == 0 {
            return Err(RtError::DriverRefused {
                call: "adopt(unbound)",
                code: None,
            });
        }
        Ok(UdpSocket::of(Inner::Real { socket }))
    }

    /// Gives up the OS socket's descriptor — the counterpart to [`UdpSocket::adopt`], for a supervisor that
    /// binds a socket and hands it to each process it spawns (the anchor's fleet serve sockets, §4.8, held
    /// across daemon restarts as its NFS listener is, §4.6). Refused on the simulation driver, whose sockets
    /// are fabric ports.
    pub fn into_owned(mut self) -> Result<OwnedDatagram, RtError> {
        match std::mem::replace(&mut self.inner, Inner::Moved) {
            Inner::Real { socket } => Ok(netsys::into_owned(socket)),
            inner => {
                self.inner = inner;
                Err(RtError::BadConfig {
                    what: "a simulated socket has no OS descriptor to hand over",
                })
            }
        }
    }

    /// The local address the socket is bound to (the OS-assigned port on a real socket; the fabric
    /// port, on loopback, in simulation).
    pub fn local_addr(&self) -> Result<SocketAddr, RtError> {
        match &self.inner {
            Inner::Real { socket } => netsys::local_addr(socket),
            Inner::Sim { port, .. } => Ok(SocketAddr::from((Ipv4Addr::LOCALHOST, *port))),
            Inner::Moved => Err(moved()),
        }
    }

    /// Whether every datagram the socket sends carries the don't-fragment bit (RFC 8899 §3), read back from
    /// the OS; on the simulation fabric, which has no fragmentation, `true`.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub fn dont_fragment(&self) -> Result<bool, RtError> {
        match &self.inner {
            Inner::Real { socket } => netsys::dont_fragment(socket),
            Inner::Sim { .. } => Ok(true),
            Inner::Moved => Err(moved()),
        }
    }

    /// The socket's receive buffer in bytes — what the kernel queues for it before dropping datagrams
    /// (`SO_RCVBUF`); a consumer that redistributes the socket among several sessions sizes each session's
    /// queue from it. On the simulation fabric, a stated stand-in (the fabric's mailbox is unbounded).
    pub fn recv_buffer_bytes(&self) -> Result<usize, RtError> {
        match &self.inner {
            Inner::Real { socket } => netsys::recv_buffer_bytes(socket),
            Inner::Sim { .. } => Ok(crate::sim::SIM_RECV_BUFFER_BYTES),
            Inner::Moved => Err(moved()),
        }
    }

    /// Sends a datagram to `addr` without blocking: the bytes accepted, or `RtError::WouldBlock` when the
    /// OS has no room for it now — local pressure, typed apart from a failure of the socket, so a caller
    /// retries it once [`UdpSocket::writable`] (or sends with [`UdpSocket::send_to_writable`]) rather than
    /// treating it as a lost path (AUD-29-61). In simulation the datagram is delivered to `addr`'s port on the
    /// fabric and any waiting receiver is woken.
    pub fn send_to(&self, buf: &[u8], addr: impl Into<SocketAddr>) -> Result<usize, RtError> {
        self.try_send_to(buf, addr)?
            .ok_or(RtError::WouldBlock { call: "sendto" })
    }

    /// Sends a datagram to `addr` without blocking: the bytes accepted, or `None` when nothing was sent now
    /// (the OS has no room, the call was interrupted, or it returned an astray report): await
    /// [`UdpSocket::writable`] and send again.
    pub fn try_send_to(
        &self,
        buf: &[u8],
        addr: impl Into<SocketAddr>,
    ) -> Result<Option<usize>, RtError> {
        Ok(match self.send_once(buf, addr.into())? {
            Outcome::Done(sent) => Some(sent),
            Outcome::NotNow => None,
        })
    }

    /// One non-blocking send.
    fn send_once(&self, buf: &[u8], addr: SocketAddr) -> Result<Outcome<usize>, RtError> {
        match &self.inner {
            Inner::Real { socket } => Ok(self.outcome(netsys::send_to(socket, buf, addr)?)),
            Inner::Sim { index, .. } => Ok(crate::sim::sim_send(*index, addr.port(), buf)?
                .map_or(Outcome::NotNow, Outcome::Done)),
            Inner::Moved => Err(moved()),
        }
    }

    /// One non-blocking receive.
    fn recv_once(&self, buf: &mut [u8]) -> Result<Outcome<(usize, SocketAddr)>, RtError> {
        match &self.inner {
            Inner::Real { socket } => Ok(self.outcome(netsys::recv_from(socket, buf)?)),
            Inner::Sim { index, .. } => Ok(crate::sim::sim_recv(*index, buf)
                .map_or(Outcome::NotNow, |(n, from)| {
                    Outcome::Done((n, SocketAddr::from((Ipv4Addr::LOCALHOST, from))))
                })),
            Inner::Moved => Err(moved()),
        }
    }

    /// A seam outcome as the socket's callers see it, counting an astray report.
    fn outcome<T>(&self, io: netsys::Io<T>) -> Outcome<T> {
        match io {
            netsys::Io::Ready(value) => Outcome::Done(value),
            netsys::Io::WouldBlock | netsys::Io::Interrupted => Outcome::NotNow,
            netsys::Io::Astray => {
                self.astray.fetch_add(1, Ordering::Relaxed);
                Outcome::NotNow
            }
        }
    }

    /// Calls that returned an astray report for an earlier datagram instead of moving data (module doc).
    pub fn astray_reports(&self) -> u64 {
        self.astray.load(Ordering::Relaxed)
    }

    /// Sends a datagram to `addr`, awaiting the socket's writability through the driver while the OS has no
    /// room for it (AUD-29-61): local send pressure delays the datagram, never fails it.
    pub async fn send_to_writable(
        &self,
        buf: &[u8],
        addr: impl Into<SocketAddr>,
    ) -> Result<usize, RtError> {
        let addr = addr.into();
        loop {
            if let Some(sent) = self.try_send_to(buf, addr)? {
                return Ok(sent);
            }
            self.writable().await?;
        }
    }

    /// Awaits the socket's write readiness through the driver (send-buffer space), or a spurious wake — so a
    /// caller follows it with [`UdpSocket::try_send_to`] and loops on `None`.
    pub fn writable(&self) -> crate::readiness::Ready {
        ready(self.target(), true)
    }

    /// The handle [`crate::readiness::readable`] and [`crate::readiness::writable`] wait on; `-1` for a
    /// simulated socket, whose waits go through [`UdpSocket::readable`].
    pub fn readiness_handle(&self) -> i32 {
        match self.target() {
            Target::Os(raw) => raw,
            Target::Sim(_) => -1,
        }
    }

    /// What a readiness wait on this socket watches.
    fn target(&self) -> Target {
        match &self.inner {
            Inner::Real { socket } => Target::Os(socket.raw_id()),
            Inner::Sim { index, .. } => Target::Sim(*index),
            Inner::Moved => Target::Os(-1),
        }
    }

    /// Receives one datagram, awaiting readability through the driver when none is ready. Returns the
    /// byte count and the sender's address. The loop of [`UdpSocket::readable`] and
    /// [`UdpSocket::try_recv_from`] — a caller whose buffer must not be held across the await (one buffer
    /// shared by a shard's readers) drives those two itself.
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), RtError> {
        loop {
            if let Some(received) = self.try_recv_from(buf)? {
                return Ok(received);
            }
            self.readable().await?;
        }
    }

    /// Awaits the socket's read readiness through the driver: a datagram is waiting, or the wait ended
    /// spuriously — so a caller follows it with [`UdpSocket::try_recv_from`] and loops on `None`. The future
    /// names the socket by its descriptor (or fabric port), not by a borrow, so a task that reaches the socket
    /// through a handle (a kept demultiplexer, AUD-29-08) can await it outside the handle's borrow.
    pub fn readable(&self) -> crate::readiness::Ready {
        ready(self.target(), false)
    }

    /// Takes one waiting datagram into `buf` without blocking: the byte count and the sender, or `None`
    /// when nothing was taken now (none waits, the call was interrupted, or it returned an astray report):
    /// await [`UdpSocket::readable`] and take again. A datagram longer than `buf` is truncated to it (size
    /// `buf` to the largest datagram the caller reads).
    pub fn try_recv_from(&self, buf: &mut [u8]) -> Result<Option<(usize, SocketAddr)>, RtError> {
        Ok(match self.recv_once(buf)? {
            Outcome::Done(received) => Some(received),
            Outcome::NotNow => None,
        })
    }
}

/// A call's outcome: done, or nothing now (await the edge and call again).
enum Outcome<T> {
    Done(T),
    NotNow,
}

/// The refusal for a socket whose OS handle was handed over.
fn moved() -> RtError {
    RtError::BadConfig {
        what: "a socket whose descriptor was handed over",
    }
}
