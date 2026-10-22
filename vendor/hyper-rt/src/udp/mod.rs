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
//! report is consumed, and the driver reports the socket ready at once when data waits behind it. A send
//! whose own destination has no route is reported the same way (macOS: an IPv6 destination on an IPv4
//! socket is `EHOSTUNREACH`), every time: a send that meets a report twice for one datagram takes it as
//! the datagram's own and refuses it ([`Batched::send`], [`UdpSocket::send_to_writable`]), so one
//! unroutable destination never holds the datagrams queued behind it.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

mod batched;
#[cfg(any(target_os = "linux", test))]
mod carry;

use std::sync::atomic::{AtomicU64, Ordering};

pub use batched::{
    Arrival, Batched, Io, IoStats, MAX_BATCH, Queued, RECEIVE_BYTES, Ring, Sent, Slot, Taken,
};

use crate::error::RtError;
use crate::netsys::{self, Family, Socket};
use crate::readiness::{Target, ready};
use crate::registry;

// The address types are `core::net`'s (the same ones the seam and `rustix::net` use), re-exported so
// a caller names an address without depending on the socket backend.
pub use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

/// The most datagrams of `size` bytes one segmented send carries where the kernel segments (Linux,
/// `UDP_SEGMENT`): `UDP_MAX_SEGMENTS`, and no more than one IP datagram's length holds, so one at a
/// loopback path's 65,507 bytes; one where the kernel does not segment.
pub fn segments_per_send(size: usize) -> usize {
    #[cfg(target_os = "linux")]
    {
        linux::segments_per_message(size)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = size;
        1
    }
}

/// The owned OS handle [`UdpSocket::adopt`] takes — a file descriptor on Unix, a Winsock socket on
/// Windows; `std::net::UdpSocket` converts into either with `.into()`.
pub type OwnedDatagram = netsys::OwnedDatagram;

/// What the kernel charges a socket's receive queue at one moment ([`UdpSocket::queue_charge`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueCharge {
    /// Bytes charged for the datagrams the queue holds.
    pub charged: u64,
    /// The buffer they are charged against: what `SO_RCVBUF` reports (Linux's, doubled).
    pub buffer: u64,
    /// The datagrams dropped for want of room since the socket was made, where the kernel says.
    pub drops: Option<u64>,
    /// The most the kernel may still charge for datagrams already read: Linux releases a reader's
    /// charge in batches, up to a quarter of the buffer while datagrams wait unread
    /// (`udp_rmem_release`, `forward_threshold = sk_rcvbuf >> 2`); the fabric releases at once.
    pub release_lag: u64,
    /// The least the queue may be charged one message beyond what `charged` tells: on macOS the
    /// mbuf limit `charged` does not read, a datagram's mbufs over `sb_efficiency`; nothing where
    /// `charged` is the kernel's whole account.
    pub message_floor: u64,
    /// What the kernel holds for the socket's buffers and charges to the process's memory: on Linux
    /// the queue's charge and the memory reserved ahead of it (`SK_MEMINFO_FWD_ALLOC`), which a
    /// container's memory limit counts (cgroup v2's `memory.stat` `sock`) though no process's
    /// footprint holds it. None where the kernel charges the process for no socket buffer (macOS's
    /// mbufs are the system's), and on the simulation fabric.
    pub held: Option<u64>,
}

/// An async UDP socket: a real OS datagram socket, or a port on the simulation's in-memory fabric.
/// The representation is hidden (the real/sim choice is the driver's, R8); a caller drives it through
/// the methods below, never by matching a backend.
#[derive(Debug)]
pub struct UdpSocket {
    inner: Inner,
    /// Calls that returned an astray report instead of data (module doc). Atomic only so a task that
    /// borrows the socket across an await stays `Send`; one thread counts it.
    astray: AtomicU64,
    /// The peer the socket is connected to ([`UdpSocket::connect`]), which it sends to alone.
    peer: Option<SocketAddr>,
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
            peer: None,
        }
    }

    /// Binds a non-blocking UDP socket to `addr` whose port the sockets it connects to its peers may
    /// then bind too ([`UdpSocket::join_shared`], `SO_REUSEPORT`), each of which takes its peer's
    /// datagrams into a receive queue of its own, as a TCP connection's socket does. The socket is
    /// bound before its port is shared, so the port is one no other socket holds: refused
    /// `AddrInUse` where one does, and port 0 is given a port of its own. Shared at its bind, a
    /// socket asking for port 0 on Linux could be given a port another of the user's shared sockets
    /// held, whatever process held it (the kernel's choice of a free port passes over a port held by
    /// a socket it may share with), and the two then split one port's datagrams: of 256 clients
    /// bound so, one to three never heard their server in four runs of five, each sharing its port
    /// with another client: Linux's search for a free port leaves out of the ports in use those held
    /// by a socket that shares its port with the binder's user (`udp_lib_lport_inuse`,
    /// net/ipv4/udp.c). Shared after its bind, the port still takes the sockets that join it, as each
    /// kernel checks the held socket's flag when another binds (Linux `udp_lib_lport_inuse` and
    /// `udp_reuseport_add_sock`, XNU `in_pcbbind`). Refused `BadConfig` on Windows, where a shared
    /// port hands a connected socket no peer's datagrams of its own, and on the simulation fabric,
    /// whose ports are one socket's each.
    pub fn bind_shared(addr: impl Into<SocketAddr>) -> Result<UdpSocket, RtError> {
        let addr = addr.into();
        if on_sim() {
            return Err(RtError::BadConfig {
                what: "a shared port on the simulation fabric, whose ports are one socket's",
            });
        }
        netsys::port_sharing()?;
        let socket = netsys::dgram_socket(Family::of(addr))?;
        netsys::bind(&socket, addr)?;
        netsys::share_port(&socket)?;
        Ok(UdpSocket::of(Inner::Real { socket }))
    }

    /// Binds a non-blocking UDP socket to `addr`, a port a socket of this process bound with
    /// [`UdpSocket::bind_shared`] holds, to share it: what a listener's socket for one of its peers
    /// binds before it connects to that peer ([`UdpSocket::connect`]). Refused as
    /// [`UdpSocket::bind_shared`] is where ports are not shared, and `AddrInUse` where the port's
    /// holder does not share it.
    pub fn join_shared(addr: impl Into<SocketAddr>) -> Result<UdpSocket, RtError> {
        let addr = addr.into();
        if on_sim() {
            return Err(RtError::BadConfig {
                what: "a shared port on the simulation fabric, whose ports are one socket's",
            });
        }
        let socket = netsys::dgram_socket(Family::of(addr))?;
        netsys::share_port(&socket)?;
        netsys::bind(&socket, addr)?;
        Ok(UdpSocket::of(Inner::Real { socket }))
    }

    /// Whether other sockets may bind this socket's port too, as [`UdpSocket::bind_shared`] binds
    /// one (`SO_REUSEPORT`): what lets a driver given a socket
    /// already bound share its port with a socket for each peer. Never on Windows, where no socket
    /// shares a port so, nor on the simulation fabric, whose ports are one socket's.
    pub fn shares_port(&self) -> Result<bool, RtError> {
        match &self.inner {
            Inner::Real { socket } => netsys::shares_port(socket),
            _ => Ok(false),
        }
    }

    /// Connects the socket to `remote`: the kernel hands it `remote`'s datagrams to its port in
    /// place of an unconnected socket sharing the port (a connected socket's exact match ranks first,
    /// on Linux and the BSDs alike, measured), and it sends to `remote` alone, naming no address
    /// (BSD refuses one on a connected socket).
    pub fn connect(&mut self, remote: SocketAddr) -> Result<(), RtError> {
        match &self.inner {
            Inner::Real { socket } => netsys::connect(socket, remote)?,
            Inner::Sim { .. } => {
                return Err(RtError::BadConfig {
                    what: "a connected socket on the simulation fabric",
                });
            }
            Inner::Moved => return Err(moved()),
        }
        self.peer = Some(remote);
        Ok(())
    }

    /// The peer the socket is connected to, if it is.
    pub fn peer(&self) -> Option<SocketAddr> {
        self.peer
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

    /// The largest UDP payload the kernel would send to the connected peer in one unfragmented
    /// datagram: the path's MTU as the kernel holds it (the route's, or less where an ICMP "packet
    /// too big" taught it less) less the IP and UDP headers. It is RFC 8899's MAX_PLPMTU for the
    /// path (§5.1.2, "the largest size of PLPMTU", bounded by the local interface), so a search
    /// for the path's MTU need not probe past it. `None` where the platform does not say (macOS),
    /// the socket has no peer, or on the simulation fabric.
    pub fn path_payload(&self) -> Result<Option<u16>, RtError> {
        match &self.inner {
            Inner::Real { socket } => netsys::path_payload(socket),
            Inner::Sim { .. } => Ok(None),
            Inner::Moved => Err(moved()),
        }
    }

    /// The largest UDP payload the kernel would carry from this socket's host to `remote` in one
    /// unfragmented datagram, as [`UdpSocket::path_payload`] reads it on a socket connected there:
    /// a socket of `remote`'s family is connected to it (connecting a datagram socket sends
    /// nothing), asked, and closed. For a socket that serves many peers and so cannot be connected
    /// to one, a listener. `None` where the platform does not say, or on the simulation fabric.
    pub fn path_payload_to(&self, remote: SocketAddr) -> Result<Option<u16>, RtError> {
        match &self.inner {
            Inner::Real { .. } => {
                let asked = netsys::dgram_socket(Family::of(remote))?;
                netsys::connect(&asked, remote)?;
                netsys::path_payload(&asked)
            }
            Inner::Sim { .. } => Ok(None),
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

    /// What its receive queue is charged now, as the kernel counts it, where it says: Linux's
    /// `SO_MEMINFO` (each queued datagram its allocation, against the buffer `SO_RCVBUF` reports,
    /// and the drops for want of room); macOS's byte count (`FIONREAD`, each datagram's payload,
    /// address and control) with its mbuf limit as a floor a message; the simulation fabric's own
    /// account (each datagram its payload). `None` where no such account may be read (Windows).
    pub fn queue_charge(&self) -> Result<Option<QueueCharge>, RtError> {
        match &self.inner {
            #[cfg(target_os = "linux")]
            Inner::Real { socket } => {
                let (charged, buffer, drops, reserved) = linux::queue_charge(socket.raw_id())
                    .map_err(|error| RtError::DriverRefused {
                        call: "getsockopt(SO_MEMINFO)",
                        code: error.raw_os_error(),
                    })?;
                Ok(Some(QueueCharge {
                    charged: u64::from(charged),
                    buffer: u64::from(buffer),
                    drops: drops.map(u64::from),
                    release_lag: u64::from(buffer >> 2),
                    message_floor: 0,
                    held: reserved
                        .map(|reserved| u64::from(charged).saturating_add(u64::from(reserved))),
                }))
            }
            // XNU's byte limit counts each datagram's payload, address and control (`sbappendaddr`)
            // against `sb_hiwat` (what `SO_RCVBUF` reports), and `FIONREAD` reads what it has
            // counted (`soioctl`: `so_rcv.sb_cc`); it releases a datagram's count as it is read.
            #[cfg(target_os = "macos")]
            Inner::Real { socket } => Ok(Some(QueueCharge {
                charged: u64::try_from(netsys::queued_bytes(socket)?).unwrap_or(u64::MAX),
                buffer: u64::try_from(netsys::recv_buffer_bytes(socket)?).unwrap_or(u64::MAX),
                drops: None,
                release_lag: 0,
                message_floor: macos::message_floor(),
                held: None,
            })),
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            Inner::Real { .. } => Ok(None),
            Inner::Sim { index, .. } => {
                let (charged, buffer) = crate::sim::sim_queue_charge(*index);
                Ok(Some(QueueCharge {
                    charged: u64::try_from(charged).unwrap_or(u64::MAX),
                    buffer: u64::try_from(buffer).unwrap_or(u64::MAX),
                    drops: None,
                    release_lag: 0,
                    message_floor: 0,
                    held: None,
                }))
            }
            Inner::Moved => Err(moved()),
        }
    }

    /// The bytes waiting in its receive queue as the platform counts them: every queued datagram's
    /// on Windows and the BSDs (the BSDs with their addresses and control), only the next one's on
    /// Linux (`FIONREAD`), every one's on the simulation fabric.
    pub fn queued_bytes(&self) -> Result<usize, RtError> {
        match &self.inner {
            Inner::Real { socket } => netsys::queued_bytes(socket),
            Inner::Sim { index, .. } => Ok(crate::sim::sim_queued(*index)),
            Inner::Moved => Err(moved()),
        }
    }

    /// Asks the kernel for a receive buffer of `bytes` (`SO_RCVBUFFORCE` where the process may, on
    /// Linux past `net.core.rmem_max`, else `SO_RCVBUF`; a size the OS refuses outright is halved
    /// and asked again) and returns the buffer it reports after the ask
    /// ([`UdpSocket::recv_buffer_bytes`]), which the OS may cap below the ask (Linux at `rmem_max`,
    /// macOS at `kern.ipc.maxsockbuf`) and Linux reports doubled. What the buffer holds of datagram
    /// payload is the transport's to count (hyper-transport's `socket_room`). The simulation
    /// fabric's buffer is fixed: nothing is asked.
    pub fn set_recv_buffer_bytes(&self, bytes: usize) -> Result<usize, RtError> {
        match &self.inner {
            Inner::Real { socket } => netsys::set_recv_buffer(socket, bytes)?,
            Inner::Sim { .. } => {}
            Inner::Moved => return Err(moved()),
        }
        self.recv_buffer_bytes()
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
            Inner::Real { socket } => match self.peer {
                None => Ok(self.outcome(netsys::send_to(socket, buf, addr)?)),
                Some(peer) if peer == addr => Ok(self.outcome(netsys::send(socket, buf)?)),
                Some(_) => Err(RtError::BadConfig {
                    what: "a send to another address than the peer the socket is connected to",
                }),
            },
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
        // An astray report is an earlier datagram's once — the call consumed it, and the datagram is
        // sent again at once — and this one's own the second time: its destination has no route,
        // which waiting for room never gives it (macOS reports an IPv6 destination on an IPv4 socket
        // so), and it is refused.
        let mut reported = false;
        loop {
            let astray = self.astray_reports();
            if let Some(sent) = self.try_send_to(buf, addr)? {
                return Ok(sent);
            }
            if self.astray_reports() != astray {
                if reported {
                    return Err(RtError::DriverRefused {
                        call: "sendto",
                        code: None,
                    });
                }
                reported = true;
                continue;
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

    /// Arms a one-shot wait on the socket's read readiness in `group` for the task polling with `cx`, a
    /// member keyed `key` ([`crate::readiness::ReadyGroup`]): the group tells when it fired, among its
    /// other members, in what fired alone. As with [`UdpSocket::readable`], a fire may be spurious.
    pub fn readable_in(
        &self,
        group: &mut crate::readiness::ReadyGroup,
        cx: &std::task::Context<'_>,
        key: u64,
    ) -> Result<crate::readiness::Member, RtError> {
        group.arm(cx, self.target(), false, key)
    }

    /// [`UdpSocket::readable_in`] for the socket's write readiness (send-buffer space).
    pub fn writable_in(
        &self,
        group: &mut crate::readiness::ReadyGroup,
        cx: &std::task::Context<'_>,
        key: u64,
    ) -> Result<crate::readiness::Member, RtError> {
        group.arm(cx, self.target(), true, key)
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
