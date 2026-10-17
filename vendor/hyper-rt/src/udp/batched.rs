//! One UDP socket, batched (docs/runtime.md §5.1): the datagrams to send wait in a bounded outbox and
//! leave in as few system calls as the platform allows; received ones are taken a batch at a time, each
//! handed over with when it arrived on the shard's clock. hyper-tokio's `socket.rs`, on hyper-rt's
//! readiness instead of tokio's reactor.
//!
//! - Linux: `sendmmsg(2)` and `recvmmsg(2)`, each message a segmented send (`UDP_SEGMENT`) or a
//!   coalesced receive (`UDP_GRO`) where the kernel has them ([`super::linux`]).
//! - macOS and Windows: one datagram a system call (macOS through `recvmsg(2)` when the socket is
//!   stamped). macOS has no public batched UDP call; Windows' segmentation and coalescing
//!   (`UDP_SEND_MSG_SIZE`, `UDP_RECV_MAX_COALESCED_SIZE`) are owed (docs/transport.md §4b).
//! - The simulation fabric: one datagram a call, stamped with the simulated clock.
//!
//! **Stamps.** A heartbeat is judged by when the kernel received it, not when its owner read it
//! (hyper-raft docs/timing.md §2.4), so a stamped socket hands each datagram over with the kernel's
//! receive stamp carried onto the shard's clock ([`crate::shard::ShardContext::now_ns`]), the clock its
//! timers run on. The kernel stamps on a clock of its own (`CLOCK_REALTIME` on Linux, `mach_absolute_time`
//! on macOS), so a stamp is carried over by its age: with the stamps' clock read after the receive and the
//! shard's clock after it, so that a preemption between the two makes the stamp late and never early, the
//! arrival is `now − (stamps_now − stamp)`. It is held within the read's time and no earlier than the
//! arrival handed over before it (a socket's queue is first in, first out). On Windows, where only a NIC
//! miniport stamps and no loopback or virtual NIC does, and in simulation, a datagram is stamped when it
//! is read.
//!
//! **Bounds.** The outbox holds at most [`Io::batch`] datagrams; a datagram queued with the outbox full is
//! dropped and counted. A receive takes at most one batch. Receive buffers: [`Io::batch`] of
//! [`RECEIVE_BYTES`] on Linux, one elsewhere, reserved when the socket is made.

use core::cell::Cell;
use core::net::{Ipv4Addr, SocketAddr};

#[cfg(target_os = "linux")]
use super::linux;
#[cfg(target_os = "macos")]
use super::macos;
use super::{Inner, Outcome, UdpSocket};
use crate::error::RtError;
use crate::registry;

/// Derived: the receive buffer, `GRO_LEGACY_MAX_SIZE` (`include/linux/netdevice.h`, 65,536 bytes), the most
/// the kernel coalesces into one UDP receive, which also holds the largest single UDP payload (65,527
/// bytes over IPv6, 65,507 over IPv4; RFC 768, RFC 8200). A datagram never arrives truncated.
pub const RECEIVE_BYTES: usize = 1 << 16;

/// Format: `UIO_MAXIOV` (`sendmmsg(2)`, `recvmmsg(2)`): the most messages one call takes; a larger batch
/// would be cut by the kernel.
pub const MAX_BATCH: usize = 1_024;

/// How a socket batches; the owner's configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Io {
    /// The most datagrams one system call carries, either way, and so the outbox's bound: 1 to
    /// [`MAX_BATCH`]. Receiving holds this many buffers of [`RECEIVE_BYTES`] on Linux and one elsewhere;
    /// sending holds this many datagrams.
    pub batch: usize,
}

impl Io {
    /// The most a real socket batched by this `Io` holds while it lives, bytes, and so what its owner
    /// charges for it: its receive buffers ([`RECEIVE_BYTES`] each, `batch` of them on Linux and one
    /// elsewhere), its outbox (`batch` slots, each a datagram or a segmented send of at most
    /// [`RECEIVE_BYTES`], [`IoStats::oversize`] past it) and, on Linux, the message headers of a send and a
    /// receive. A socket holds as much once it has sent its largest datagrams and received a full batch.
    pub fn held_bound(&self) -> usize {
        let receive = if cfg!(target_os = "linux") {
            self.batch
        } else {
            1
        };
        let slot = RECEIVE_BYTES.saturating_add(size_of::<Slot>());
        #[cfg(target_os = "linux")]
        let headers = linux::message_bytes().saturating_mul(self.batch);
        #[cfg(not(target_os = "linux"))]
        let headers = 0;
        receive
            .saturating_mul(RECEIVE_BYTES)
            .saturating_add(self.batch.saturating_mul(slot))
            .saturating_add(headers)
    }
}

/// What a socket has done.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoStats {
    /// Datagrams handed to the kernel.
    pub sent: u64,
    /// System calls that sent them.
    pub send_calls: u64,
    /// Datagrams received, each coalesced one counted once per datagram it held.
    pub received: u64,
    /// System calls that received them.
    pub receive_calls: u64,
    /// Receive calls that found nothing queued (`EAGAIN`): a drain's last call, unless the call
    /// before it already said the socket was drained.
    pub receives_empty: u64,
    /// Datagrams the kernel refused to send (too large for the interface, the network down), dropped as
    /// lost: QUIC recovers them, and the plane retransmits nothing by design.
    pub send_errors: u64,
    /// Datagrams dropped because the outbox was full.
    pub outbox_full: u64,
    /// Calls that returned an astray report for an earlier datagram (`super` module doc).
    pub astray: u64,
    /// Whether segmented sends are in use.
    pub gso: bool,
    /// Whether coalesced receives are in use.
    pub gro: bool,
    /// Whether the kernel stamps each datagram received (Linux's `SO_TIMESTAMPNS`, macOS's
    /// `SO_TIMESTAMP_MONOTONIC`); otherwise a datagram is stamped when it is read.
    pub kernel_stamps: bool,
    /// The datagrams the kernel dropped for want of room since the socket was made, as the
    /// datagrams received say (Linux's `SO_RXQ_OVFL`): `Some(0)` until one says more, `None` where
    /// the kernel does not tell. A drop after the last datagram received is told by the next.
    pub drops: Option<u64>,
    /// Datagrams dropped because they were larger than a slot holds ([`RECEIVE_BYTES`]): no UDP payload
    /// or segmented send is, so one is a caller's fault, and the slot never grows past its bound for it.
    pub oversize: u64,
}

impl IoStats {
    /// These and `other`'s summed, each count saturating; a flag is set where either's is.
    #[must_use]
    pub fn plus(&self, other: &Self) -> Self {
        Self {
            sent: self.sent.saturating_add(other.sent),
            send_calls: self.send_calls.saturating_add(other.send_calls),
            received: self.received.saturating_add(other.received),
            receive_calls: self.receive_calls.saturating_add(other.receive_calls),
            receives_empty: self.receives_empty.saturating_add(other.receives_empty),
            send_errors: self.send_errors.saturating_add(other.send_errors),
            outbox_full: self.outbox_full.saturating_add(other.outbox_full),
            astray: self.astray.saturating_add(other.astray),
            gso: self.gso || other.gso,
            gro: self.gro || other.gro,
            kernel_stamps: self.kernel_stamps || other.kernel_stamps,
            drops: match (self.drops, other.drops) {
                (Some(a), Some(b)) => Some(a.saturating_add(b)),
                (a, b) => a.or(b),
            },
            oversize: self.oversize.saturating_add(other.oversize),
        }
    }
}

/// Where and when a datagram arrived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Arrival {
    /// The address it came from.
    pub from: SocketAddr,
    /// When it arrived, nanoseconds on the shard's clock: the kernel's receive stamp where there is one,
    /// else when it was read.
    pub at_ns: u64,
    /// Whether `at_ns` is the kernel's stamp.
    pub kernel: bool,
    /// When it was read, nanoseconds on the shard's clock: `at_ns` or later.
    pub read_ns: u64,
}

/// What [`Batched::receive_queued`] took.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Taken {
    /// Datagrams handed over, each of a coalesced message counted.
    pub datagrams: usize,
    /// Messages the kernel handed up: a coalesced one (`UDP_GRO`) is one message of many
    /// datagrams, which the kernel charges as one.
    pub messages: usize,
    /// Their payload bytes.
    pub bytes: usize,
}

/// How [`Batched::receive_queued`] ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Queued {
    /// The socket held nothing more: await [`Batched::readable`].
    Emptied,
    /// It took what was queued when the receive began, or reached its bound on calls: the socket
    /// may hold more, to take at the owner's next turn without awaiting readiness (which fires
    /// once a datagram arrives, not for one already waiting).
    More,
}

/// A datagram waiting in the outbox.
#[derive(Debug)]
pub struct Slot {
    /// The datagram.
    pub bytes: Vec<u8>,
    /// Where it goes.
    pub to: SocketAddr,
}

/// Receive buffers a driver lends whichever of its sockets it reads ([`Batched::receive_with`]):
/// one batch of them for any number of sockets, where a socket that holds its own holds a batch
/// each. [`Io::batch`] buffers of [`RECEIVE_BYTES`] on Linux and one elsewhere, and on Linux the
/// headers of a receive.
pub struct Ring {
    batch: usize,
    buffers: Vec<Vec<u8>>,
    #[cfg(target_os = "linux")]
    headers: linux::Headers,
    #[cfg(target_os = "linux")]
    received: Vec<linux::Received>,
}

impl std::fmt::Debug for Ring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ring")
            .field("batch", &self.batch)
            .field("buffers", &self.buffers.len())
            .finish()
    }
}

impl Ring {
    /// A ring for batches of `io`'s size. Refused `BadConfig` for a batch outside 1 to
    /// [`MAX_BATCH`], and `Capacity` when its buffers cannot be reserved.
    pub fn new(io: Io) -> Result<Self, RtError> {
        if io.batch == 0 || io.batch > MAX_BATCH {
            return Err(RtError::BadConfig {
                what: "a datagram batch outside 1 to MAX_BATCH",
            });
        }
        let count = if cfg!(target_os = "linux") {
            io.batch
        } else {
            1
        };
        let mut buffers = Vec::new();
        buffers
            .try_reserve_exact(count)
            .map_err(|_| RtError::Capacity {
                what: "datagram receive buffers",
                bound: count,
            })?;
        for _ in 0..count {
            buffers.push(zeroed(RECEIVE_BYTES)?);
        }
        Ok(Self {
            batch: io.batch,
            buffers,
            #[cfg(target_os = "linux")]
            headers: linux::Headers::new(io.batch),
            #[cfg(target_os = "linux")]
            received: Vec::with_capacity(io.batch),
        })
    }

    /// The bytes it holds while it lives: what its owner charges for it.
    pub fn held_bound(&self) -> usize {
        #[cfg(target_os = "linux")]
        let headers = linux::message_bytes().saturating_mul(self.batch);
        #[cfg(not(target_os = "linux"))]
        let headers = 0;
        self.buffers
            .len()
            .saturating_mul(RECEIVE_BYTES)
            .saturating_add(headers)
    }
}

/// Whether the outbox drained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sent {
    /// Everything queued has left.
    Drained,
    /// The socket took no more now: await [`Batched::writable`] and send again.
    Blocked,
}

/// One call's worth of a send.
enum SendStep {
    /// The kernel took this many slots (at least one).
    Took(usize),
    /// Nothing now: await writability.
    NotNow,
    /// The kernel refused the message carrying this many slots; they are dropped.
    Refused(usize),
    /// The call returned an astray report and sent the message carrying this many slots nothing: an
    /// earlier datagram's report, consumed by the call, or the message's own when it has no route —
    /// which a second send of it tells apart.
    Astray(usize),
    /// Segmented sends were just switched off (the device cannot segment); send again unsegmented.
    #[cfg(target_os = "linux")]
    Unsegmented,
}

/// One call's worth of a receive.
enum ReceiveStep {
    /// This many datagrams were handed over, in this many messages (a coalesced one, `UDP_GRO`,
    /// many datagrams); `drained` when the call filled fewer places than it offered, so nothing
    /// more waited as it returned (recvmmsg(2) with `MSG_DONTWAIT` returns what is queued).
    Delivered {
        datagrams: usize,
        messages: usize,
        drained: bool,
    },
    /// A datagram was consumed with nothing to hand over (truncated, or from a non-internet address).
    Skipped,
    /// Nothing waits (or the call was interrupted): await readability.
    Empty,
    /// The call returned an astray report.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Astray,
}

/// The socket.
pub struct Batched {
    udp: UdpSocket,
    batch: usize,
    slots: Vec<Slot>,
    /// Slots filled, and of those, the ones sent.
    queued: usize,
    sent: usize,
    /// The slot whose send returned an astray report once: a second report for it is its own.
    astray_at: Option<usize>,
    buffers: Vec<Vec<u8>>,
    stats: IoStats,
    /// The latest arrival handed over: none after it arrived before it.
    latest_ns: u64,
    #[cfg(target_os = "linux")]
    linux: Linux,
    #[cfg(target_os = "macos")]
    mach: Option<macos::Clock>,
}

#[cfg(target_os = "linux")]
struct Linux {
    send: linux::Headers,
    receive: linux::Headers,
    received: Vec<linux::Received>,
    /// The kernel's stamps carried to the shard's clock, from the first stamped receive (the shard's
    /// clock is read on the shard, which a socket may be made off).
    carry: Option<super::carry::Carry>,
}

impl std::fmt::Debug for Batched {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batched")
            .field("batch", &self.batch)
            .field("queued", &self.queued)
            .field("sent", &self.sent)
            .field("stats", &self.stats)
            .finish()
    }
}

/// A zeroed buffer of `len` bytes, refused `Capacity` when the allocator has no room for it.
fn zeroed(len: usize) -> Result<Vec<u8>, RtError> {
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(len)
        .map_err(|_| RtError::Capacity {
            what: "datagram receive buffer bytes",
            bound: len,
        })?;
    buffer.resize(len, 0);
    Ok(buffer)
}

/// The shard's clock now.
fn shard_now() -> Result<u64, RtError> {
    registry::with_current(|context| context.now_ns()).ok_or(RtError::NotOnShardThread)
}

/// The arrival handed over for a datagram from `from` read at `now_ns` with the kernel's `stamp_ns`, if
/// any (both on the shard's clock): within the read's time and no earlier than `latest_ns`, which it
/// advances.
fn arrival(latest_ns: &mut u64, from: SocketAddr, stamp_ns: Option<u64>, now_ns: u64) -> Arrival {
    let at_ns = stamp_ns.unwrap_or(now_ns).min(now_ns).max(*latest_ns);
    *latest_ns = at_ns;
    Arrival {
        from,
        at_ns,
        kernel: stamp_ns.is_some(),
        read_ns: now_ns.max(at_ns),
    }
}

/// The step a failed batched call maps to, or the refusal it is.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn receive_failed(call: &'static str, error: &std::io::Error) -> Result<ReceiveStep, RtError> {
    match error.raw_os_error() {
        Some(libc::EAGAIN | libc::EINTR) => Ok(ReceiveStep::Empty),
        Some(libc::ECONNREFUSED | libc::ECONNRESET | libc::EHOSTUNREACH | libc::ENETUNREACH) => {
            Ok(ReceiveStep::Astray)
        }
        code => Err(RtError::DriverRefused { call, code }),
    }
}

fn as_u64(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

impl Batched {
    /// Binds a socket to `addr` and batches it; see [`Batched::new`].
    pub fn bind(addr: impl Into<SocketAddr>, io: Io, stamps: bool) -> Result<Self, RtError> {
        Self::new(UdpSocket::bind(addr)?, io, stamps)
    }

    /// Binds a socket to `addr` that other sockets of this process may bind too
    /// ([`UdpSocket::bind_shared`]) and batches it.
    pub fn bind_shared(addr: impl Into<SocketAddr>, io: Io, stamps: bool) -> Result<Self, RtError> {
        Self::new(UdpSocket::bind_shared(addr)?, io, stamps)
    }

    /// Connects the socket to `remote` ([`UdpSocket::connect`]).
    pub fn connect(&mut self, remote: SocketAddr) -> Result<(), RtError> {
        self.udp.connect(remote)
    }

    /// Batches `udp`. With `stamps`, the kernel is asked to stamp each datagram it receives, where it can
    /// (the stats say whether it agreed). Refused `BadConfig` for a batch outside 1 to [`MAX_BATCH`], and
    /// `Capacity` when the receive buffers cannot be reserved.
    pub fn new(udp: UdpSocket, io: Io, stamps: bool) -> Result<Self, RtError> {
        Self::new_with(udp, io, stamps, true)
    }

    /// Binds a socket to `addr`, lent its receive buffers ([`Batched::lent`]).
    pub fn bind_lent(addr: impl Into<SocketAddr>, stamps: bool) -> Result<Self, RtError> {
        Self::lent(UdpSocket::bind(addr)?, stamps)
    }

    /// Binds a socket to `addr` whose port its peers' sockets may join ([`UdpSocket::bind_shared`]),
    /// lent its receive buffers ([`Batched::lent`]).
    pub fn bind_shared_lent(addr: impl Into<SocketAddr>, stamps: bool) -> Result<Self, RtError> {
        Self::lent(UdpSocket::bind_shared(addr)?, stamps)
    }

    /// Binds a socket to `addr`, joining a port a socket of this process shares
    /// ([`UdpSocket::join_shared`]), lent its receive buffers ([`Batched::lent`]).
    pub fn join_shared_lent(addr: impl Into<SocketAddr>, stamps: bool) -> Result<Self, RtError> {
        Self::lent(UdpSocket::join_shared(addr)?, stamps)
    }

    /// Batches `udp` without receive buffers of its own: it receives into a [`Ring`] its owner lends
    /// it ([`Batched::receive_with`]) and sends from its owner's buffers
    /// ([`Batched::send_segments`]), so a driver of many sockets holds one batch of buffers for all
    /// of them where each held its own (a socket a peer, thousands of them). Its outbox holds one
    /// datagram.
    pub fn lent(udp: UdpSocket, stamps: bool) -> Result<Self, RtError> {
        Self::new_with(udp, Io { batch: 1 }, stamps, false)
    }

    fn new_with(udp: UdpSocket, io: Io, stamps: bool, own: bool) -> Result<Self, RtError> {
        if io.batch == 0 || io.batch > MAX_BATCH {
            return Err(RtError::BadConfig {
                what: "a datagram batch outside 1 to MAX_BATCH",
            });
        }
        let unspecified = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(io.batch)
            .map_err(|_| RtError::Capacity {
                what: "datagram outbox slots",
                bound: io.batch,
            })?;
        slots.extend((0..io.batch).map(|_| Slot {
            bytes: Vec::new(),
            to: unspecified,
        }));
        let raw = match &udp.inner {
            Inner::Real { socket } => Some(socket.raw_id()),
            Inner::Sim { .. } | Inner::Moved => None,
        };
        #[cfg_attr(
            not(any(target_os = "linux", target_os = "macos")),
            expect(
                unused_mut,
                reason = "only Linux and macOS ask the kernel for anything"
            )
        )]
        let mut stats = IoStats::default();
        #[cfg(target_os = "linux")]
        let (linux, buffer_count) = {
            if let Some(fd) = raw {
                let offload = linux::offload(fd);
                stats.gso = offload.gso;
                stats.gro = offload.gro;
                stats.kernel_stamps = stamps && linux::enable_stamps(fd);
                stats.drops = linux::enable_drop_counts(fd).then_some(0);
            }
            let linux = Linux {
                send: linux::Headers::new(io.batch),
                receive: linux::Headers::new(io.batch),
                received: Vec::with_capacity(io.batch),
                carry: None,
            };
            (linux, if raw.is_some() { io.batch } else { 1 })
        };
        #[cfg(target_os = "macos")]
        let (mach, buffer_count) = {
            let mach = match raw {
                Some(fd) if stamps && macos::enable_stamps(fd) => {
                    let clock = macos::Clock::new().map_err(|error| RtError::DriverRefused {
                        call: "mach_timebase_info",
                        code: error.raw_os_error(),
                    })?;
                    stats.kernel_stamps = true;
                    Some(clock)
                }
                _ => None,
            };
            (mach, 1)
        };
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let buffer_count = {
            let _ = (raw, stamps);
            1
        };
        let buffer_count = if own { buffer_count } else { 0 };
        let mut buffers = Vec::new();
        buffers
            .try_reserve_exact(buffer_count)
            .map_err(|_| RtError::Capacity {
                what: "datagram receive buffers",
                bound: buffer_count,
            })?;
        for _ in 0..buffer_count {
            buffers.push(zeroed(RECEIVE_BYTES)?);
        }
        Ok(Self {
            udp,
            batch: io.batch,
            slots,
            queued: 0,
            sent: 0,
            astray_at: None,
            buffers,
            stats,
            latest_ns: 0,
            #[cfg(target_os = "linux")]
            linux,
            #[cfg(target_os = "macos")]
            mach,
        })
    }

    /// The most datagrams one of its system calls carries, either way ([`Io::batch`]).
    pub fn batch(&self) -> usize {
        self.batch
    }

    /// The local address the socket is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, RtError> {
        self.udp.local_addr()
    }

    /// What the socket has done.
    pub fn stats(&self) -> IoStats {
        IoStats {
            astray: self.udp.astray_reports().saturating_add(self.stats.astray),
            ..self.stats
        }
    }

    /// The socket underneath.
    pub fn socket(&self) -> &UdpSocket {
        &self.udp
    }

    /// Awaits the socket's read readiness through the driver (or a spurious wake).
    pub fn readable(&self) -> crate::readiness::Ready {
        self.udp.readable()
    }

    /// Awaits the socket's write readiness through the driver (or a spurious wake).
    pub fn writable(&self) -> crate::readiness::Ready {
        self.udp.writable()
    }

    /// Whether datagrams wait for the socket.
    pub fn pending(&self) -> bool {
        self.sent < self.queued
    }

    /// The next free slot, if the outbox has one, for the caller to fill in place; [`Batched::commit`]
    /// queues it.
    pub fn slot(&mut self) -> Option<&mut Slot> {
        if self.sent == self.queued {
            self.sent = 0;
            self.queued = 0;
            self.astray_at = None;
        }
        self.slots.get_mut(self.queued)
    }

    /// Queues the slot [`Batched::slot`] gave; one filled past [`RECEIVE_BYTES`] is dropped and counted
    /// ([`IoStats::oversize`]) and gives back what it grew past its bound.
    pub fn commit(&mut self) {
        if let Some(slot) = self.slots.get_mut(self.queued)
            && slot.bytes.len() > RECEIVE_BYTES
        {
            slot.bytes.clear();
            slot.bytes.shrink_to(RECEIVE_BYTES);
            self.stats.oversize = self.stats.oversize.saturating_add(1);
            return;
        }
        self.queued = self.queued.saturating_add(1).min(self.batch);
    }

    /// What the socket holds now, bytes: its receive buffers, its outbox's slots and what each slot holds,
    /// and on Linux its message headers. Never more than [`Io::held_bound`] of its `Io`.
    pub fn held_bytes(&self) -> usize {
        let buffers = self.buffers.iter().fold(0usize, |held, buffer| {
            held.saturating_add(buffer.capacity())
        });
        let slots = self.slots.iter().fold(0usize, |held, slot| {
            held.saturating_add(size_of::<Slot>())
                .saturating_add(slot.bytes.capacity())
        });
        #[cfg(target_os = "linux")]
        let headers = linux::message_bytes().saturating_mul(self.batch);
        #[cfg(not(target_os = "linux"))]
        let headers = 0;
        buffers.saturating_add(slots).saturating_add(headers)
    }

    /// Queues a copy of `bytes` for `to`; with the outbox full it is dropped and counted, and `false`
    /// returned.
    pub fn queue(&mut self, to: SocketAddr, bytes: &[u8]) -> bool {
        if bytes.len() > RECEIVE_BYTES {
            self.stats.oversize = self.stats.oversize.saturating_add(1);
            return false;
        }
        let Some(slot) = self.slot() else {
            self.stats.outbox_full = self.stats.outbox_full.saturating_add(1);
            return false;
        };
        slot.bytes.clear();
        slot.bytes.extend_from_slice(bytes);
        slot.to = to;
        self.commit();
        true
    }

    /// Sends what is queued, as far as the socket takes it now. A datagram the kernel refuses is dropped
    /// and counted: the protocols above recover a lost one. An astray report is an earlier datagram's
    /// once — the call consumed it, and the datagram is sent again at once — and the datagram's own the
    /// second time (a destination the kernel has no route for, which macOS reports as unreachable): it
    /// is refused like any other, and never holds the queue behind it. Every pass of the loop moves at
    /// least one slot, or returns, or marks the slot it tried astray (once a slot), or switches
    /// segmentation off (once), so it ends within twice the queue's length and one.
    pub fn send(&mut self) -> Sent {
        while self.sent < self.queued {
            match self.send_some() {
                SendStep::Took(count) => {
                    let count = count.max(1);
                    self.sent = self.sent.saturating_add(count).min(self.queued);
                    self.stats.sent = self.stats.sent.saturating_add(as_u64(count));
                    self.stats.send_calls = self.stats.send_calls.saturating_add(1);
                    self.astray_at = None;
                }
                SendStep::NotNow => return Sent::Blocked,
                SendStep::Refused(slots) => {
                    let slots = slots.max(1);
                    self.sent = self.sent.saturating_add(slots).min(self.queued);
                    self.stats.send_errors = self.stats.send_errors.saturating_add(as_u64(slots));
                    self.astray_at = None;
                }
                SendStep::Astray(slots) if self.astray_at == Some(self.sent) => {
                    let slots = slots.max(1);
                    self.sent = self.sent.saturating_add(slots).min(self.queued);
                    self.stats.send_errors = self.stats.send_errors.saturating_add(as_u64(slots));
                    self.astray_at = None;
                }
                SendStep::Astray(_) => self.astray_at = Some(self.sent),
                #[cfg(target_os = "linux")]
                SendStep::Unsegmented => {}
            }
        }
        self.sent = 0;
        self.queued = 0;
        self.astray_at = None;
        Sent::Drained
    }

    /// Sends `bytes` to `to` now as datagrams of `segment` bytes each (the last may be shorter),
    /// straight from the caller's buffer with no copy into the outbox: in segmented sends where the
    /// kernel segments (`UDP_SEGMENT`, each within the kernel's segment and byte bounds), else a
    /// datagram a call. What the outbox holds goes first, so every datagram leaves in the order it
    /// was given. Returns how many of the datagrams are done, sent or refused (a datagram the kernel
    /// refuses is dropped and counted, as the outbox drops one: the protocols above recover it):
    /// all of them, or fewer when the socket takes no more now (await [`Batched::writable`] and send
    /// the rest), none while the outbox still holds datagrams. Each pass of its loop sends a
    /// message, or returns, or marks an astray report (once a datagram), or switches segmentation
    /// off (once), so it ends within three passes a datagram and two.
    pub fn send_segments(&mut self, to: SocketAddr, bytes: &[u8], segment: usize) -> usize {
        let segment = segment.clamp(1, RECEIVE_BYTES);
        let total = bytes.len().div_ceil(segment);
        if total == 0 || (self.pending() && self.send() == Sent::Blocked) {
            return 0;
        }
        let mut done = 0usize;
        let mut astray_at = None;
        for _ in 0..total.saturating_mul(3).saturating_add(2) {
            let Some(left) = total.checked_sub(done).filter(|left| *left > 0) else {
                break;
            };
            let count = self.segments_a_message(segment).min(left);
            let from = done.saturating_mul(segment).min(bytes.len());
            let to_byte = from
                .saturating_add(count.saturating_mul(segment))
                .min(bytes.len());
            let Some(message) = bytes.get(from..to_byte) else {
                break;
            };
            match self.send_message(to, message, segment) {
                SendStep::Took(_) => {
                    done = done.saturating_add(count);
                    self.stats.sent = self.stats.sent.saturating_add(as_u64(count));
                    self.stats.send_calls = self.stats.send_calls.saturating_add(1);
                    astray_at = None;
                }
                SendStep::NotNow => break,
                SendStep::Astray(_) if astray_at != Some(done) => astray_at = Some(done),
                SendStep::Refused(_) | SendStep::Astray(_) => {
                    done = done.saturating_add(count);
                    self.stats.send_errors = self.stats.send_errors.saturating_add(as_u64(count));
                    astray_at = None;
                }
                #[cfg(target_os = "linux")]
                SendStep::Unsegmented => {}
            }
        }
        done
    }

    /// The datagrams of `segment` bytes one message carries: as many as a segmented send takes
    /// where the kernel segments, else one.
    fn segments_a_message(&self, segment: usize) -> usize {
        #[cfg(target_os = "linux")]
        if self.stats.gso && matches!(self.udp.inner, Inner::Real { .. }) {
            return linux::segments_per_message(segment);
        }
        let _ = segment;
        1
    }

    /// One message of `bytes` to `to`, segmented into datagrams of `segment` bytes where it holds
    /// more than one.
    #[cfg(target_os = "linux")]
    fn send_message(&mut self, to: SocketAddr, bytes: &[u8], segment: usize) -> SendStep {
        let Inner::Real { socket } = &self.udp.inner else {
            return self.send_message_portable(to, bytes);
        };
        let fd = socket.raw_id();
        match linux::send_one(fd, to, bytes, segment, &mut self.linux.send) {
            Ok(()) => SendStep::Took(1),
            Err(failed) => match failed.error.raw_os_error() {
                Some(libc::EAGAIN | libc::EINTR) => SendStep::NotNow,
                Some(
                    libc::ECONNREFUSED | libc::ECONNRESET | libc::EHOSTUNREACH | libc::ENETUNREACH,
                ) => {
                    self.stats.astray = self.stats.astray.saturating_add(1);
                    SendStep::Astray(1)
                }
                // EIO or EINVAL from a segmented message: the path cannot segment (`send_some`).
                // Later sends go a datagram a message, this one among them.
                Some(libc::EIO | libc::EINVAL) if self.stats.gso && segment < bytes.len() => {
                    self.stats.gso = false;
                    SendStep::Unsegmented
                }
                _ => SendStep::Refused(1),
            },
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn send_message(&mut self, to: SocketAddr, bytes: &[u8], _segment: usize) -> SendStep {
        self.send_message_portable(to, bytes)
    }

    /// One datagram of `bytes` to `to` through the socket's own send.
    fn send_message_portable(&mut self, to: SocketAddr, bytes: &[u8]) -> SendStep {
        let astray_before = self.udp.astray_reports();
        match self.udp.send_once(bytes, to) {
            Ok(Outcome::Done(_)) => SendStep::Took(1),
            Ok(Outcome::NotNow) if self.udp.astray_reports() != astray_before => {
                SendStep::Astray(1)
            }
            Ok(Outcome::NotNow) => SendStep::NotNow,
            Err(_) => SendStep::Refused(1),
        }
    }

    /// One system call's worth of the queue.
    #[cfg(target_os = "linux")]
    fn send_some(&mut self) -> SendStep {
        let Inner::Real { socket } = &self.udp.inner else {
            return self.send_portable();
        };
        let fd = socket.raw_id();
        let slots = self.slots.get(self.sent..self.queued).unwrap_or(&[]);
        match linux::send(fd, slots, self.stats.gso, &mut self.linux.send) {
            Ok(count) => SendStep::Took(count),
            Err(failed) => match failed.error.raw_os_error() {
                Some(libc::EAGAIN | libc::EINTR) => SendStep::NotNow,
                Some(
                    libc::ECONNREFUSED | libc::ECONNRESET | libc::EHOSTUNREACH | libc::ENETUNREACH,
                ) => {
                    // The report is consumed and the datagram was not sent: the next send carries it,
                    // unless the report was its own (`Batched::send`).
                    self.stats.astray = self.stats.astray.saturating_add(1);
                    SendStep::Astray(failed.slots)
                }
                // EIO or EINVAL from a segmented message: the path cannot segment (udp(7) names EIO
                // for a device without checksum offload; the kernel refuses EINVAL to a socket sending
                // without checksums, and quinn-udp has met it from drivers too). Later sends go one
                // datagram a message, this one among them. A message of one datagram was not
                // segmented, and its error is its own.
                Some(libc::EIO | libc::EINVAL) if self.stats.gso && failed.slots > 1 => {
                    self.stats.gso = false;
                    SendStep::Unsegmented
                }
                _ => SendStep::Refused(failed.slots),
            },
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn send_some(&mut self) -> SendStep {
        self.send_portable()
    }

    /// One datagram through the socket's own send.
    fn send_portable(&mut self) -> SendStep {
        let Some(slot) = self.slots.get(self.sent) else {
            return SendStep::Took(1);
        };
        let astray_before = self.udp.astray_reports();
        match self.udp.send_once(&slot.bytes, slot.to) {
            Ok(Outcome::Done(_)) => SendStep::Took(1),
            // The socket counted the report itself.
            Ok(Outcome::NotNow) if self.udp.astray_reports() != astray_before => {
                SendStep::Astray(1)
            }
            Ok(Outcome::NotNow) => SendStep::NotNow,
            Err(_) => SendStep::Refused(1),
        }
    }

    /// Takes one batch of datagrams that have arrived, handing each to `deliver` with its arrival; returns
    /// how many, 0 when none had (await [`Batched::readable`]). At most [`Io::batch`] calls, so a socket
    /// that keeps reporting astray datagrams cannot hold the shard.
    pub fn receive(&mut self, deliver: impl FnMut(Arrival, &mut [u8])) -> Result<usize, RtError> {
        if self.buffers.is_empty() {
            return Err(RtError::BadConfig {
                what: "a lent socket receives into its owner's ring (Batched::receive_with)",
            });
        }
        self.receive_turns(self.batch, deliver)
    }

    /// [`Batched::receive`] into `ring`, lent for the call: a batch of the ring's size, its buffers
    /// and headers moved in for the call and back out after it.
    pub fn receive_with(
        &mut self,
        ring: &mut Ring,
        deliver: impl FnMut(Arrival, &mut [u8]),
    ) -> Result<usize, RtError> {
        self.lend(ring);
        let received = self.receive_turns(ring.batch, deliver);
        self.lend(ring);
        received
    }

    /// Takes, into `ring`, the datagrams that were queued when the receive began, with the rest of
    /// the call that reached the first that was not, handing each to `deliver`; how many, and how
    /// it ended. A socket with the kernel's receive stamps ([`Arrival::kernel`]) tells them by
    /// their stamps: one stamped after `began_ns`, on the shard's clock, arrived since. A socket
    /// without counts bytes: as many as it held at the first call ([`UdpSocket::queued_bytes`],
    /// every queued datagram's on Windows, the BSDs and the simulation fabric, the next one's on
    /// Linux, so one call's worth there). An owner so takes, each turn, the backlog the turn found,
    /// and what arrives meanwhile waits for its next turn: taking until the socket emptied let a
    /// peer that kept sending hold its owner from sending, the peer's own credit among what waited,
    /// for as long as it kept on. At most `calls` calls.
    pub fn receive_queued(
        &mut self,
        ring: &mut Ring,
        began_ns: u64,
        calls: usize,
        deliver: impl FnMut(Arrival, &mut [u8]),
    ) -> Result<(Taken, Queued), RtError> {
        self.lend(ring);
        let received = self.receive_until_past(began_ns, calls, deliver);
        self.lend(ring);
        received
    }

    fn receive_until_past(
        &mut self,
        began_ns: u64,
        calls: usize,
        mut deliver: impl FnMut(Arrival, &mut [u8]),
    ) -> Result<(Taken, Queued), RtError> {
        let stamped = self.stats.kernel_stamps;
        let held = if stamped {
            None
        } else {
            Some(self.udp.queued_bytes()?)
        };
        let past = Cell::new(false);
        let bytes_taken = Cell::new(0usize);
        let mut handed = |arrival: Arrival, bytes: &mut [u8]| {
            if stamped && arrival.at_ns > began_ns {
                past.set(true);
            }
            bytes_taken.set(bytes_taken.get().saturating_add(bytes.len()));
            deliver(arrival, bytes);
        };
        let mut taken = Taken::default();
        let mut ended = Queued::More;
        for _ in 0..calls {
            match self.receive_some(&mut handed)? {
                ReceiveStep::Delivered {
                    datagrams,
                    messages,
                    drained,
                } => {
                    taken.datagrams = taken.datagrams.saturating_add(datagrams);
                    taken.messages = taken.messages.saturating_add(messages);
                    // The queue was empty as the call returned: another would only answer that
                    // (`EAGAIN`), a system call a drain. What lands after it is a readiness the arm
                    // that follows reports (epoll's re-arm and kqueue's re-add both look again).
                    if drained {
                        ended = Queued::Emptied;
                        break;
                    }
                }
                ReceiveStep::Skipped => {}
                ReceiveStep::Empty => {
                    self.stats.receives_empty = self.stats.receives_empty.saturating_add(1);
                    ended = Queued::Emptied;
                    break;
                }
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                ReceiveStep::Astray => self.stats.astray = self.stats.astray.saturating_add(1),
            }
            if past.get() || held.is_some_and(|held| bytes_taken.get() >= held) {
                break;
            }
        }
        taken.bytes = bytes_taken.get();
        self.stats.received = self.stats.received.saturating_add(as_u64(taken.datagrams));
        Ok((taken, ended))
    }

    /// Exchanges the socket's receive buffers and headers with `ring`'s (twice restores them).
    fn lend(&mut self, ring: &mut Ring) {
        core::mem::swap(&mut self.buffers, &mut ring.buffers);
        #[cfg(target_os = "linux")]
        {
            core::mem::swap(&mut self.linux.receive, &mut ring.headers);
            core::mem::swap(&mut self.linux.received, &mut ring.received);
        }
    }

    fn receive_turns(
        &mut self,
        turns: usize,
        mut deliver: impl FnMut(Arrival, &mut [u8]),
    ) -> Result<usize, RtError> {
        let mut delivered = 0usize;
        for _ in 0..turns {
            match self.receive_some(&mut deliver)? {
                ReceiveStep::Delivered { datagrams, .. } => {
                    delivered = delivered.saturating_add(datagrams);
                    if self.one_call_takes_the_batch() {
                        break;
                    }
                }
                ReceiveStep::Skipped => {}
                ReceiveStep::Empty => break,
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                ReceiveStep::Astray => self.stats.astray = self.stats.astray.saturating_add(1),
            }
        }
        self.stats.received = self.stats.received.saturating_add(as_u64(delivered));
        Ok(delivered)
    }

    /// Whether one receive call takes a whole batch (Linux's `recvmmsg` on a real socket).
    fn one_call_takes_the_batch(&self) -> bool {
        cfg!(target_os = "linux") && matches!(self.udp.inner, Inner::Real { .. })
    }

    #[cfg(target_os = "linux")]
    fn receive_some(
        &mut self,
        deliver: &mut impl FnMut(Arrival, &mut [u8]),
    ) -> Result<ReceiveStep, RtError> {
        let Inner::Real { socket } = &self.udp.inner else {
            return self.receive_portable(deliver);
        };
        let fd = socket.raw_id();
        let Self {
            buffers,
            linux,
            stats,
            latest_ns,
            ..
        } = self;
        linux.received.clear();
        let offered = buffers.len();
        let filled = match linux::receive(fd, buffers, &mut linux.receive, &mut linux.received) {
            Ok(filled) => filled,
            Err(error) => return receive_failed("recvmmsg", &error),
        };
        stats.receive_calls = stats.receive_calls.saturating_add(1);
        // The stamps' clock first, then the shard's: a stamp is carried by the offset between them
        // (`super::carry::Carry`), and a thread preempted between the two reads makes it late, never
        // early.
        let realtime_ns = if stats.kernel_stamps {
            linux::realtime_ns().map_err(|error| RtError::DriverRefused {
                call: "clock_gettime(CLOCK_REALTIME)",
                code: error.raw_os_error(),
            })?
        } else {
            0
        };
        let now_ns = shard_now()?;
        let drained = filled < offered.min(linux.receive.places());
        let carry = if stats.kernel_stamps {
            let carry = linux
                .carry
                .get_or_insert_with(|| super::carry::Carry::new(realtime_ns, now_ns));
            carry.read(realtime_ns, now_ns);
            let read = *carry;
            // A batch that took less than it asked for emptied the socket, once its own stamps
            // are carried.
            if drained {
                carry.emptied();
            }
            Some(read)
        } else {
            None
        };
        let mut delivered = 0usize;
        for taken in &linux.received {
            if let (Some(told), Some(held)) = (taken.drops, stats.drops.as_mut()) {
                *held = (*held).max(u64::from(told));
            }
            let Some(bytes) = buffers
                .get_mut(taken.index)
                .and_then(|buffer| buffer.get_mut(..taken.length))
            else {
                continue;
            };
            let stamp = taken
                .stamp
                .zip(carry)
                .and_then(|(stamp, carry)| carry.carry(stamp));
            let arrived = arrival(latest_ns, taken.from, stamp, now_ns);
            match taken.segment {
                Some(size) => {
                    for datagram in bytes.chunks_mut(size) {
                        deliver(arrived, datagram);
                        delivered = delivered.saturating_add(1);
                    }
                }
                None => {
                    deliver(arrived, bytes);
                    delivered = delivered.saturating_add(1);
                }
            }
        }
        Ok(ReceiveStep::Delivered {
            datagrams: delivered,
            messages: linux.received.len(),
            drained,
        })
    }

    #[cfg(target_os = "macos")]
    fn receive_some(
        &mut self,
        deliver: &mut impl FnMut(Arrival, &mut [u8]),
    ) -> Result<ReceiveStep, RtError> {
        let (Inner::Real { socket }, Some(mach)) = (&self.udp.inner, self.mach) else {
            return self.receive_portable(deliver);
        };
        let fd = socket.raw_id();
        let Some(buffer) = self.buffers.first_mut() else {
            return Ok(ReceiveStep::Empty);
        };
        let taken = match macos::receive(fd, buffer) {
            Ok(taken) => taken,
            Err(error) => return receive_failed("recvmsg", &error),
        };
        self.stats.receive_calls = self.stats.receive_calls.saturating_add(1);
        let Some(taken) = taken else {
            return Ok(ReceiveStep::Skipped);
        };
        // The stamps' clock first, then the shard's (as on Linux).
        let ticks_now = mach.now_ticks();
        let now_ns = shard_now()?;
        let stamp = taken
            .stamp
            .map(|ticks| now_ns.saturating_sub(mach.ticks_ns(ticks_now.saturating_sub(ticks))));
        let arrived = arrival(&mut self.latest_ns, taken.from, stamp, now_ns);
        let Some(bytes) = buffer.get_mut(..taken.length) else {
            return Ok(ReceiveStep::Skipped);
        };
        deliver(arrived, bytes);
        Ok(ReceiveStep::Delivered {
            datagrams: 1,
            messages: 1,
            drained: false,
        })
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn receive_some(
        &mut self,
        deliver: &mut impl FnMut(Arrival, &mut [u8]),
    ) -> Result<ReceiveStep, RtError> {
        self.receive_portable(deliver)
    }

    /// One datagram through the socket's own receive, stamped when it is read.
    fn receive_portable(
        &mut self,
        deliver: &mut impl FnMut(Arrival, &mut [u8]),
    ) -> Result<ReceiveStep, RtError> {
        let Some(buffer) = self.buffers.first_mut() else {
            return Ok(ReceiveStep::Empty);
        };
        let astray_before = self.udp.astray_reports();
        match self.udp.recv_once(buffer)? {
            Outcome::Done((length, from)) => {
                self.stats.receive_calls = self.stats.receive_calls.saturating_add(1);
                let now_ns = shard_now()?;
                let arrived = arrival(&mut self.latest_ns, from, None, now_ns);
                let Some(bytes) = buffer.get_mut(..length) else {
                    return Ok(ReceiveStep::Skipped);
                };
                deliver(arrived, bytes);
                Ok(ReceiveStep::Delivered {
                    datagrams: 1,
                    messages: 1,
                    drained: false,
                })
            }
            // The socket counted the report itself; the step only keeps the batch going.
            Outcome::NotNow if self.udp.astray_reports() != astray_before => {
                Ok(ReceiveStep::Skipped)
            }
            Outcome::NotNow => Ok(ReceiveStep::Empty),
        }
    }
}
