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
}

/// A datagram waiting in the outbox.
#[derive(Debug)]
pub struct Slot {
    /// The datagram.
    pub bytes: Vec<u8>,
    /// Where it goes.
    pub to: SocketAddr,
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
    /// Segmented sends were just switched off (the device cannot segment); send again unsegmented.
    #[cfg(target_os = "linux")]
    Unsegmented,
}

/// One call's worth of a receive.
enum ReceiveStep {
    /// This many datagrams were handed over.
    Delivered(usize),
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

    /// Batches `udp`. With `stamps`, the kernel is asked to stamp each datagram it receives, where it can
    /// (the stats say whether it agreed). Refused `BadConfig` for a batch outside 1 to [`MAX_BATCH`], and
    /// `Capacity` when the receive buffers cannot be reserved.
    pub fn new(udp: UdpSocket, io: Io, stamps: bool) -> Result<Self, RtError> {
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
            }
            let linux = Linux {
                send: linux::Headers::new(io.batch),
                receive: linux::Headers::new(io.batch),
                received: Vec::with_capacity(io.batch),
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
            buffers,
            stats,
            latest_ns: 0,
            #[cfg(target_os = "linux")]
            linux,
            #[cfg(target_os = "macos")]
            mach,
        })
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
        }
        self.slots.get_mut(self.queued)
    }

    /// Queues the slot [`Batched::slot`] gave.
    pub fn commit(&mut self) {
        self.queued = self.queued.saturating_add(1).min(self.batch);
    }

    /// Queues a copy of `bytes` for `to`; with the outbox full it is dropped and counted, and `false`
    /// returned.
    pub fn queue(&mut self, to: SocketAddr, bytes: &[u8]) -> bool {
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
    /// and counted: the protocols above recover a lost one. Every pass of the loop moves at least one
    /// slot, or returns, or switches segmentation off (once), so it ends within the queue's length.
    pub fn send(&mut self) -> Sent {
        while self.sent < self.queued {
            match self.send_some() {
                SendStep::Took(count) => {
                    let count = count.max(1);
                    self.sent = self.sent.saturating_add(count).min(self.queued);
                    self.stats.sent = self.stats.sent.saturating_add(as_u64(count));
                    self.stats.send_calls = self.stats.send_calls.saturating_add(1);
                }
                SendStep::NotNow => return Sent::Blocked,
                SendStep::Refused(slots) => {
                    let slots = slots.max(1);
                    self.sent = self.sent.saturating_add(slots).min(self.queued);
                    self.stats.send_errors = self.stats.send_errors.saturating_add(as_u64(slots));
                }
                #[cfg(target_os = "linux")]
                SendStep::Unsegmented => {}
            }
        }
        self.sent = 0;
        self.queued = 0;
        Sent::Drained
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
                    // The report is consumed and the datagram was not sent: the next send carries it.
                    self.stats.astray = self.stats.astray.saturating_add(1);
                    SendStep::NotNow
                }
                // EIO from a segmented send: the device cannot segment (its checksum offload is off,
                // udp(7)); later sends go one datagram a message, this one among them.
                Some(libc::EIO) if self.stats.gso => {
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
        match self.udp.send_once(&slot.bytes, slot.to) {
            Ok(Outcome::Done(_)) => SendStep::Took(1),
            Ok(Outcome::NotNow) => SendStep::NotNow,
            Err(_) => SendStep::Refused(1),
        }
    }

    /// Takes one batch of datagrams that have arrived, handing each to `deliver` with its arrival; returns
    /// how many, 0 when none had (await [`Batched::readable`]). At most [`Io::batch`] calls, so a socket
    /// that keeps reporting astray datagrams cannot hold the shard.
    pub fn receive(
        &mut self,
        mut deliver: impl FnMut(Arrival, &mut [u8]),
    ) -> Result<usize, RtError> {
        let mut delivered = 0usize;
        for _ in 0..self.batch {
            match self.receive_some(&mut deliver)? {
                ReceiveStep::Delivered(count) => {
                    delivered = delivered.saturating_add(count);
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
        if let Err(error) = linux::receive(fd, buffers, &mut linux.receive, &mut linux.received) {
            return receive_failed("recvmmsg", &error);
        }
        stats.receive_calls = stats.receive_calls.saturating_add(1);
        // The stamps' clock first, then the shard's: a thread preempted between the two reads then makes
        // the age's end later, so a stamp comes out late, never early.
        let realtime_ns = if stats.kernel_stamps {
            linux::realtime_ns().map_err(|error| RtError::DriverRefused {
                call: "clock_gettime(CLOCK_REALTIME)",
                code: error.raw_os_error(),
            })?
        } else {
            0
        };
        let now_ns = shard_now()?;
        let mut delivered = 0usize;
        for taken in &linux.received {
            let Some(bytes) = buffers
                .get_mut(taken.index)
                .and_then(|buffer| buffer.get_mut(..taken.length))
            else {
                continue;
            };
            let stamp = taken
                .stamp
                .map(|stamp| now_ns.saturating_sub(realtime_ns.saturating_sub(stamp)));
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
        Ok(ReceiveStep::Delivered(delivered))
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
        Ok(ReceiveStep::Delivered(1))
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
                Ok(ReceiveStep::Delivered(1))
            }
            // The socket counted the report itself; the step only keeps the batch going.
            Outcome::NotNow if self.udp.astray_reports() != astray_before => {
                Ok(ReceiveStep::Skipped)
            }
            Outcome::NotNow => Ok(ReceiveStep::Empty),
        }
    }
}
