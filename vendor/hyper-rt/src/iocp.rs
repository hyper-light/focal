//! The I/O completion port driver (Windows): `PostQueuedCompletionStatus` for kicks and no-ops,
//! `GetQueuedCompletionStatusEx` with a timeout for the wait [B: Microsoft Learn].
//!
//! A timed wait's own timeout is in milliseconds, and the kernel ends it on its timer interrupt,
//! 15.6 ms apart unless some process has raised the rate: a timer the wheel had due in 200 µs
//! fired a tick of the system clock late. So a timed wait arms a high-resolution waitable timer
//! (Windows 10 1803) whose signal ntdll's wait completion packet posts to the port, and the wait's
//! own timeout only backs it: Go's netpoller since 1.23 (`netpoll_windows.go`, `netpollQueueTimer`).
//! The packet functions are found at run time, as Go finds them, since some platforms (Wine) do not
//! carry them; where they or the timer are missing, a wait is one of milliseconds alone.
#![allow(unsafe_code)]

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT};
use windows_sys::Win32::Networking::WinSock::SOCKET;
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY,
    PostQueuedCompletionStatus,
};

use crate::afd::{Afd, Block, Outcome, READABLE_EVENTS, WRITABLE_EVENTS, base_socket};
use crate::driver::{Completion, Driver, DriverKind, Kick, KickPort, nanos_since};
use crate::error::RtError;
use crate::interests::Readiness;
use std::collections::HashMap;

/// Format: the completion key that marks a kick.
const KICK_KEY: usize = usize::MAX;
/// Format: the completion key that marks a no-op; its user word rides in the overlapped pointer.
const NOP_KEY: usize = usize::MAX - 1;
/// Format: the completion key the AFD readiness device is associated under. Its completions carry a
/// poll block in the overlapped pointer (its head is the block), not a user word in the key.
const AFD_KEY: usize = usize::MAX - 2;
/// Format: the completion key a timed wait's timer posts under ([`Precise`]).
const TIMER_KEY: usize = usize::MAX - 3;

/// Format: NTSTATUS values the wait completion packets answer (ntstatus.h).
const STATUS_SUCCESS: i32 = 0;
/// Format: `STATUS_CANCELLED`, 0xC0000120.
const STATUS_CANCELLED: i32 = i32::from_ne_bytes(0xC000_0120_u32.to_ne_bytes());
/// Format: `GENERIC_ALL`, the access a wait completion packet is made with (as Go makes it).
const GENERIC_ALL: u32 = 0x1000_0000;

/// ntdll's `NtCreateWaitCompletionPacket(handle, access, attributes)`.
type CreatePacket = unsafe extern "system" fn(*mut HANDLE, u32, *const std::ffi::c_void) -> i32;
/// ntdll's `NtAssociateWaitCompletionPacket(packet, port, target, key, apc, status, information,
/// already_signaled)`.
type AssociatePacket = unsafe extern "system" fn(
    HANDLE,
    HANDLE,
    HANDLE,
    *const std::ffi::c_void,
    *const std::ffi::c_void,
    i32,
    usize,
    *mut u8,
) -> i32;
/// ntdll's `NtCancelWaitCompletionPacket(packet, remove_signaled)`.
type CancelPacket = unsafe extern "system" fn(HANDLE, u8) -> i32;

/// A timed wait's high-resolution timer and the wait completion packet that posts its signal to the
/// port under [`TIMER_KEY`].
struct Precise {
    timer: HANDLE,
    packet: HANDLE,
    associate: AssociatePacket,
    cancel: CancelPacket,
    /// Whether the packet waits on a timer a wait armed and has not yet seen fire: an untimed wait
    /// cancels it first, or that timer would end the wait.
    armed: bool,
}

/// What arming the timer came to.
enum Armed {
    /// The timer will post its signal to the port.
    Set,
    /// It passed between its setting and its association: the wait need not wait.
    Signaled,
    /// The packet's last wait could not be cancelled yet (it fired as it was asked), or a call was
    /// refused: this wait keeps its own timeout alone, as Go's does.
    Coarse,
}

impl Precise {
    /// The timer and its packet, where this Windows carries both: none before 1803 (no
    /// high-resolution timer) or where ntdll lacks the packet functions.
    fn make() -> Option<Precise> {
        use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
        use windows_sys::Win32::System::Threading::{
            CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, CreateWaitableTimerExW,
            SYNCHRONIZATION_SYNCHRONIZE, TIMER_MODIFY_STATE, TIMER_QUERY_STATE,
        };
        let ntdll: Vec<u16> = "ntdll.dll\0".encode_utf16().collect();
        // SAFETY: a NUL-terminated wide name; ntdll is loaded in every Windows process.
        let module = unsafe { GetModuleHandleW(ntdll.as_ptr()) };
        if module.is_null() {
            return None;
        }
        // SAFETY: NUL-terminated names looked up in the module just found; a missing one is null.
        let (create, associate, cancel) = unsafe {
            (
                GetProcAddress(module, c"NtCreateWaitCompletionPacket".as_ptr().cast())?,
                GetProcAddress(module, c"NtAssociateWaitCompletionPacket".as_ptr().cast())?,
                GetProcAddress(module, c"NtCancelWaitCompletionPacket".as_ptr().cast())?,
            )
        };
        // SAFETY: each is ntdll's export of that name, whose signature the alias states (the
        // functions Microsoft documents, and Go's runtime calls the same way).
        let (create, associate, cancel): (CreatePacket, AssociatePacket, CancelPacket) = unsafe {
            (
                std::mem::transmute::<unsafe extern "system" fn() -> isize, CreatePacket>(create),
                std::mem::transmute::<unsafe extern "system" fn() -> isize, AssociatePacket>(
                    associate,
                ),
                std::mem::transmute::<unsafe extern "system" fn() -> isize, CancelPacket>(cancel),
            )
        };
        // SAFETY: no name and no attributes; the result is checked.
        let timer = unsafe {
            CreateWaitableTimerExW(
                std::ptr::null(),
                std::ptr::null(),
                CREATE_WAITABLE_TIMER_HIGH_RESOLUTION,
                SYNCHRONIZATION_SYNCHRONIZE | TIMER_QUERY_STATE | TIMER_MODIFY_STATE,
            )
        };
        if timer.is_null() {
            return None;
        }
        let mut packet: HANDLE = std::ptr::null_mut();
        // SAFETY: `packet` is written with the new handle; no attributes.
        let status = unsafe { create(&raw mut packet, GENERIC_ALL, std::ptr::null()) };
        if status != STATUS_SUCCESS || packet.is_null() {
            // SAFETY: the timer was made just above and is closed once here.
            unsafe { CloseHandle(timer) };
            return None;
        }
        Some(Precise {
            timer,
            packet,
            associate,
            cancel,
            armed: false,
        })
    }

    /// Arms the timer `ns` from now, its signal to post to `port`: the packet's last wait, which an
    /// arrival may have outrun, is cancelled first (removing its signal if it was posted), since a
    /// packet waits on one object at a time.
    fn arm(&mut self, port: HANDLE, ns: u64) -> Armed {
        use windows_sys::Win32::System::Threading::SetWaitableTimer;
        // SAFETY: the packet is this value's own handle, open while it lives.
        match unsafe { (self.cancel)(self.packet, 1) } {
            // Nothing waited, or what did fired (which cancels the packet's wait itself).
            STATUS_SUCCESS | STATUS_CANCELLED => self.armed = false,
            // `STATUS_PENDING` (0x103): the wait fired as it was asked and cannot be cancelled yet,
            // its signal to end the next wait; anything else, refused. Either way this wait keeps
            // its own timeout alone.
            _ => return Armed::Coarse,
        }
        // Relative, in 100 ns units, negative (SetWaitableTimer).
        let due = i64::try_from(ns.div_ceil(100))
            .unwrap_or(i64::MAX)
            .max(1)
            .saturating_neg();
        // SAFETY: the timer is this value's own; `due` is live for the call; no completion routine.
        if unsafe { SetWaitableTimer(self.timer, &raw const due, 0, None, std::ptr::null(), 0) }
            == 0
        {
            return Armed::Coarse;
        }
        let mut signaled: u8 = 0;
        // SAFETY: the packet, the port and the timer are open handles; the key is a tag the wait
        // reads back, never dereferenced; `signaled` is written once.
        let status = unsafe {
            (self.associate)(
                self.packet,
                port,
                self.timer,
                std::ptr::without_provenance(TIMER_KEY),
                std::ptr::null(),
                STATUS_SUCCESS,
                0,
                &raw mut signaled,
            )
        };
        match (status, signaled) {
            (STATUS_SUCCESS, 0) => {
                self.armed = true;
                Armed::Set
            }
            (STATUS_SUCCESS, _) => Armed::Signaled,
            _ => Armed::Coarse,
        }
    }

    /// Cancels the packet's wait on a timer an earlier wait armed, its signal removed if posted.
    /// One whose cancel cannot be done yet is firing as it is asked: its signal ends the next wait
    /// once, to no waiter, as a kick does.
    fn disarm(&mut self) {
        if self.armed {
            // SAFETY: the packet is this value's own handle, open while it lives.
            unsafe { (self.cancel)(self.packet, 1) };
            self.armed = false;
        }
    }
}

impl Drop for Precise {
    fn drop(&mut self) {
        // SAFETY: both handles are this value's own, closed once here, the packet's wait cancelled
        // first (its signal removed if posted), so nothing posts to the port after the driver.
        unsafe {
            (self.cancel)(self.packet, 1);
            CloseHandle(self.packet);
            CloseHandle(self.timer);
        }
    }
}

/// Shape: entries drained per wait (see the kqueue driver for the reasoning).
const EVENTS_PER_WAIT: usize = 64;

/// A completion port the registry slot owns (§4.3, D-8), closed when the slot retires its
/// registration — after the shard's thread has ended and every foreign kick borrowing it has returned —
/// so a late kick can never post to a handle value the process has since reused. Kept as its exposed
/// address so it is `Send` without an unsafe impl.
#[derive(Debug)]
pub struct Port {
    handle: usize,
}

impl Port {
    /// The port's raw handle value.
    pub(crate) fn raw(&self) -> usize {
        self.handle
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        let handle: HANDLE = std::ptr::with_exposed_provenance_mut(self.handle);
        // SAFETY: the port was created by `prepare` and is owned by this value alone; closed once here.
        unsafe { CloseHandle(handle) };
    }
}

/// The driver. It borrows its port from the registry slot (see [`Port`]) and never closes it.
pub struct IocpDriver {
    port: HANDLE,
    /// The registry's kick over this driver's port.
    kick: KickPort,
    /// The shard-clock reading the driver counts its clock from (`machine::clock::shard_clock_ns`).
    epoch: u64,
    entries: Vec<OVERLAPPED_ENTRY>,
    /// The AFD readiness device, opened and associated with `port` on the first socket registration
    /// (a shard that only kicks and times out never opens it). Socket readiness is polled through it.
    afd: Option<Afd>,
    /// The poll in flight for each socket armed: one at most (mantle's review, finding 8). A socket's entry
    /// leaves when its poll completes (a closed socket's too, with `AFD_POLL_LOCAL_CLOSE`) or when its last
    /// waiter leaves (`disarm`), so it holds at most the handles waited on, `handles_bound`. Reserved for
    /// twice that: hashbrown rehashes in place, without allocating, while the items are at most half its
    /// capacity (`RawTable::reserve_rehash`), so tombstones never make it grow (mantle's final review, 4).
    outstanding: HashMap<i32, *mut Block>,
    /// The handles `outstanding` may hold (`interests_per_shard`).
    handles_bound: usize,
    /// Polls issued and not yet reclaimed, cancelled ones included: what the drop drains.
    in_flight: usize,
    /// A timed wait's timer, where this Windows carries one ([`Precise`]).
    precise: Option<Precise>,
}

/// Cited: how long a dropped driver waits for completions the kernel still owes it — the I/O manager's own
/// bound on a cancelled IRP: "If a canceled IRP is not completed within 5 minutes, the I/O manager considers
/// the IRP timed out" (Microsoft, *Canceling IRPs*, Windows driver documentation). The wait ends as soon as
/// the last completion arrives; it reaches the bound only when a driver breaks its cancel contract, when
/// Windows itself gives up on the IRP.
const DRAIN_NS: u64 = 300_000_000_000;

/// AFD poll blocks a dropped driver could not reclaim by [`DRAIN_NS`]: left to the kernel, never freed while
/// it may write them (a test's tripwire, expected zero).
static BLOCKS_LEFT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// AFD poll blocks dropped drivers left to the kernel since the process started ([`BLOCKS_LEFT`]).
pub fn afd_blocks_left() -> u64 {
    BLOCKS_LEFT.load(std::sync::atomic::Ordering::Relaxed)
}

impl std::fmt::Debug for IocpDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IocpDriver")
            .field("port", &self.port)
            .field("entries", &self.entries.len())
            .finish()
    }
}

/// Creates the port with one concurrent thread (the shard), for the registry slot to own.
pub fn prepare() -> Result<Port, RtError> {
    // SAFETY: creating a fresh port; the result is checked.
    let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
    if port.is_null() {
        return Err(RtError::os("CreateIoCompletionPort"));
    }
    Ok(Port {
        handle: port.expose_provenance(),
    })
}

impl IocpDriver {
    /// Builds the driver over the port the registry slot owns, on the shard's thread. Refused when the
    /// registration no longer holds a port (it was retired).
    pub fn from_prepared(kick: KickPort) -> Result<IocpDriver, RtError> {
        let port = kick.raw().ok_or(RtError::DriverRefused {
            call: "IOCP driver over a retired registration",
            code: None,
        })?;
        let entries = (0..EVENTS_PER_WAIT)
            // SAFETY: an all-zero OVERLAPPED_ENTRY is a valid, empty record.
            .map(|_| unsafe { std::mem::zeroed() })
            .collect();
        Ok(IocpDriver {
            port: std::ptr::with_exposed_provenance_mut(port),
            kick,
            epoch: crate::machine::clock::shard_clock_ns(),
            entries,
            afd: None,
            outstanding: HashMap::new(),
            handles_bound: 0,
            in_flight: 0,
            precise: Precise::make(),
        })
    }

    /// The AFD readiness device, opened and associated with this port on first use (§4.6). Lazily,
    /// because a driver that never awaits a socket needs no AFD handle.
    fn afd(&mut self) -> Result<&Afd, RtError> {
        if let Some(ref afd) = self.afd {
            return Ok(afd);
        }
        let afd = Afd::open()?;
        // Associate the AFD device with this port so its polls complete here under `AFD_KEY`.
        // SAFETY: both handles are live and ours; `CreateIoCompletionPort` with an existing `port`
        // associates `afd.handle()` with it and returns the port (null on failure).
        let associated = unsafe { CreateIoCompletionPort(afd.handle(), self.port, AFD_KEY, 0) };
        if associated.is_null() {
            return Err(RtError::os("CreateIoCompletionPort(AFD)"));
        }
        // `insert` stores the device and hands back a reference to it — no `expect` on a re-read.
        Ok(self.afd.insert(afd))
    }

    /// Arms a one-shot AFD poll for `events` on the socket `raw` names, waking `user_data` when it
    /// fires. A Windows `SOCKET` fits in a positive `i32` in practice (kernel handle-table values), so
    /// the readiness seam carries it as the same `i32` a Unix fd uses; it is reconstructed here as the
    /// low 32 bits, unsigned. The leaked poll block is owned by the kernel until `wait` reclaims it.
    /// One outstanding AFD poll per socket (mantle's review, finding 8): a poll that already watches every
    /// event wanted stays; otherwise one for the union is issued first and only then the old one cancelled,
    /// so a refused issue leaves the old poll, and the waiters it serves, in place (mantle's final review,
    /// second pass, finding 1: cancelling first stranded them when the issue failed).
    fn arm_events(&mut self, raw: i32, events: u32, tag: u64) -> Result<(), RtError> {
        let Some(&old) = self.outstanding.get(&raw) else {
            if self.outstanding.len() >= self.handles_bound {
                return Err(RtError::Capacity {
                    what: "AFD polls",
                    bound: self.handles_bound,
                });
            }
            let block = self.issue(raw, events, tag)?;
            self.outstanding.insert(raw, block);
            return Ok(());
        };
        // SAFETY: an outstanding block is live until its completion is reclaimed, which removes it here.
        let requested = unsafe { Block::requested(old) };
        if requested & events == events {
            return Ok(());
        }
        let block = self.issue(raw, requested | events, tag)?;
        self.outstanding.insert(raw, block);
        if let Some(afd) = self.afd.as_ref() {
            // SAFETY: the old block was in flight until just now (above); its completion still arrives.
            unsafe { afd.cancel(old) };
        }
        Ok(())
    }

    /// Issues a poll for `events` on `raw`'s base socket: its block, in flight.
    fn issue(&mut self, raw: i32, events: u32, tag: u64) -> Result<*mut Block, RtError> {
        #[cfg(test)]
        if tests::FAIL_NEXT_ISSUE.with(|fail| fail.replace(false)) {
            return Err(RtError::DriverRefused {
                call: "IOCTL_AFD_POLL (forced by a test)",
                code: None,
            });
        }
        // The inverse of `netsys::Socket::raw_id`: the `i32`'s 32 bits, widened to the pointer-width `SOCKET`.
        let socket = SOCKET::try_from(u32::from_ne_bytes(raw.to_ne_bytes())).unwrap_or(SOCKET::MAX);
        let base = base_socket(socket)?;
        let afd = self.afd()?;
        // SAFETY: `afd()` associated the device with this port before returning it, so the poll's
        // completion is delivered here and the block is reclaimed exactly once (`wait`, or the drop's drain).
        let block = unsafe { afd.poll(base, events, tag)? };
        self.in_flight = self.in_flight.saturating_add(1);
        Ok(block)
    }

    /// Takes a poll's completion: reclaims its block and, unless it was cancelled, the readiness it carried.
    fn complete(&mut self, block: *mut Block) -> Option<Completion> {
        // SAFETY: `block` is a `Block` leaked by `Afd::poll` and delivered by the port exactly once.
        let reclaimed = unsafe { Block::reclaim(block) };
        self.in_flight = self.in_flight.saturating_sub(1);
        let raw = crate::interests::handle_of(reclaimed.user_data)?;
        let current = self.outstanding.get(&raw) == Some(&block);
        if current {
            self.outstanding.remove(&raw);
        }
        let fired = match reclaimed.outcome {
            // A cancelled poll the driver replaced or disarmed: nothing happened to deliver.
            Outcome::Cancelled if !current => return None,
            // Cancelled while still the handle's poll — not by this driver (a thread's exit, another caller's
            // cancel): its waiters would wait on nothing, so they are woken to retry (the second pass's
            // hardening of finding 1).
            Outcome::Cancelled | Outcome::Failed => Readiness::READ.union(Readiness::WRITE),
            Outcome::Fired(events) => {
                let mut fired = Readiness::NONE;
                if events & READABLE_EVENTS != 0 {
                    fired = fired.union(Readiness::READ);
                }
                if events & WRITABLE_EVENTS != 0 {
                    fired = fired.union(Readiness::WRITE);
                }
                fired
            }
        };
        Some(Completion {
            user_data: reclaimed.user_data,
            result: fired.bits(),
        })
    }
}

impl Drop for IocpDriver {
    /// Cancels every poll in flight and drains the port until each one's block is reclaimed, before the AFD
    /// handle closes with the driver: the kernel holds each block until its completion is delivered, and a
    /// block never delivered is never freed (mantle's review, finding 8). A cancel is not synchronous
    /// (`CancelIoEx`: "does not wait for all canceled operations to complete"; mio's selector likewise
    /// awaits a cancelled poll's completion on the port), so the drain waits — at most [`DRAIN_NS`], the I/O
    /// manager's own timeout for a cancelled IRP, in total (mantle's final review, second pass, finding 3:
    /// it was the step budget, a wake latency, about a millisecond). A block still owed
    /// at the deadline is left to the kernel and counted ([`afd_blocks_left`]), never freed while the kernel
    /// may write it (mantle's final review, finding 4: the drain waited `INFINITE`). The deadline alone bounds
    /// it: a counted number of rounds would let kick packets end it before the polls' completions came.
    fn drop(&mut self) {
        if let Some(afd) = self.afd.as_ref() {
            for (_, block) in self.outstanding.drain() {
                // SAFETY: an outstanding block is live until its completion is reclaimed.
                unsafe { afd.cancel(block) };
            }
        }
        let deadline = crate::machine::clock::monotonic_ns().saturating_add(DRAIN_NS);
        // Bounded by the deadline: each round returns entries or times out at it.
        while self.in_flight > 0 {
            // The time left, in whole milliseconds rounded up, so a round never returns early with time left.
            let left_ns = deadline.saturating_sub(crate::machine::clock::monotonic_ns());
            if left_ns == 0 {
                break;
            }
            let left_ms =
                u32::try_from(left_ns.div_ceil(1_000_000)).unwrap_or(u32::MAX.saturating_sub(1));
            let mut count: u32 = 0;
            let capacity = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
            // SAFETY: the port is this driver's, open while it lives; `entries` is a live buffer of `capacity`
            // entries the call fills, and `count` receives how many. The timeout is finite (below `INFINITE`).
            let ok = unsafe {
                GetQueuedCompletionStatusEx(
                    self.port,
                    self.entries.as_mut_ptr(),
                    capacity,
                    &raw mut count,
                    left_ms,
                    0,
                )
            };
            if ok == 0 || count == 0 {
                break;
            }
            let delivered: Vec<*mut Block> = self
                .entries
                .iter()
                .take(usize::try_from(count).unwrap_or(0))
                .filter(|entry| entry.lpCompletionKey == AFD_KEY && !entry.lpOverlapped.is_null())
                .map(|entry| entry.lpOverlapped.cast::<Block>())
                .collect();
            for block in delivered {
                let _ = self.complete(block);
            }
        }
        if self.in_flight > 0 {
            BLOCKS_LEFT.fetch_add(
                u64::try_from(self.in_flight).unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }
}

/// Posts a kick packet to `port`, a live port the caller has pinned ([`KickPort::with`]); safe from
/// any thread. A failed post means the port is being torn down with its shard, the documented outcome
/// of a kick to an exiting shard (`Kick::kick`).
pub fn post_kick(port: usize) {
    let handle: HANDLE = std::ptr::with_exposed_provenance_mut(port);
    // SAFETY: the caller pinned the registration that owns this port, so the handle is open.
    unsafe { PostQueuedCompletionStatus(handle, 0, KICK_KEY, std::ptr::null_mut()) };
}

impl Driver for IocpDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Iocp
    }

    fn kick_handle(&self) -> Kick {
        Kick::Iocp(self.kick)
    }

    fn now_ns(&self) -> u64 {
        nanos_since(self.epoch)
    }

    fn clock(&self) -> crate::driver::Clock {
        crate::driver::Clock::Since(self.epoch)
    }

    fn wait(&mut self, timeout_ns: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        /// Format: nanoseconds per millisecond.
        const NANOS_PER_MILLI: u64 = 1_000_000;
        /// Format: INFINITE.
        const INFINITE: u32 = u32::MAX;
        let mut timeout_ms = timeout_ns.map_or(INFINITE, |ns| {
            u32::try_from(ns.div_ceil(NANOS_PER_MILLI)).unwrap_or(INFINITE - 1)
        });
        match (timeout_ns, self.precise.as_mut()) {
            // A zero timeout needs no timer.
            (Some(0), _) | (_, None) => {}
            // The timer ends a timed wait when due; the wait's own timeout only backs it.
            (Some(ns), Some(precise)) => {
                if let Armed::Signaled = precise.arm(self.port, ns) {
                    timeout_ms = 0;
                }
            }
            // An untimed wait first disarms what an earlier one left, or that timer would end it.
            (None, Some(precise)) => precise.disarm(),
        }
        let mut count: u32 = 0;
        let capacity = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        // SAFETY: the entry buffer holds `capacity` records and `count` is writable.
        let ok = unsafe {
            GetQueuedCompletionStatusEx(
                self.port,
                self.entries.as_mut_ptr(),
                capacity,
                &raw mut count,
                timeout_ms,
                0,
            )
        };
        if ok == 0 {
            let code = std::io::Error::last_os_error().raw_os_error();
            return if code == Some(i32::try_from(WAIT_TIMEOUT).unwrap_or(0)) {
                Ok(())
            } else {
                Err(RtError::DriverRefused {
                    call: "GetQueuedCompletionStatusEx",
                    code,
                })
            };
        }
        let mut completed: Vec<*mut Block> = Vec::new();
        for entry in self
            .entries
            .iter()
            .take(usize::try_from(count).unwrap_or(0))
        {
            match entry.lpCompletionKey {
                KICK_KEY => {}
                // A timed wait's timer fired: the shard's clock tells which timers are due.
                TIMER_KEY => {
                    if let Some(precise) = self.precise.as_mut() {
                        precise.armed = false;
                    }
                }
                NOP_KEY => out.push(Completion {
                    user_data: u64::try_from(entry.lpOverlapped.addr()).unwrap_or(0),
                    result: 0,
                }),
                AFD_KEY => {
                    // A socket's poll completed: the overlapped pointer is the leaked poll block (its head is
                    // the `OVERLAPPED`). A cancelled poll (replaced by a wider one) delivers nothing.
                    if !entry.lpOverlapped.is_null() {
                        completed.push(entry.lpOverlapped.cast::<Block>());
                    }
                }
                key => out.push(Completion {
                    user_data: u64::try_from(key).unwrap_or(0),
                    result: i32::try_from(entry.dwNumberOfBytesTransferred).unwrap_or(i32::MAX),
                }),
            }
        }
        for block in completed {
            if let Some(completion) = self.complete(block) {
                out.push(completion);
            }
        }
        Ok(())
    }

    fn has_pending(&self) -> bool {
        false
    }

    fn submit_nop(&mut self, user_data: u64) -> Result<(), RtError> {
        let overlapped = std::ptr::without_provenance_mut(usize::try_from(user_data).unwrap_or(0));
        // SAFETY: the overlapped pointer is a tag the wait reads back as an address, never dereferenced.
        if unsafe { PostQueuedCompletionStatus(self.port, 0, NOP_KEY, overlapped) } == 0 {
            return Err(RtError::os("PostQueuedCompletionStatus"));
        }
        Ok(())
    }

    fn disarm(&mut self, raw: i32) {
        if let Some(block) = self.outstanding.remove(&raw)
            && let Some(afd) = self.afd.as_ref()
        {
            // SAFETY: an outstanding block is live until its completion is reclaimed; its completion, now
            // cancelled, still arrives and is reclaimed in `wait` (or the drop's drain).
            unsafe { afd.cancel(block) };
        }
    }

    fn reserve_handles(&mut self, handles: usize) -> Result<(), RtError> {
        let refused = RtError::Capacity {
            what: "AFD polls",
            bound: handles,
        };
        let room = handles.checked_mul(2).ok_or(refused.clone())?;
        self.outstanding.try_reserve(room).map_err(|_| refused)?;
        self.handles_bound = handles;
        Ok(())
    }

    fn arm(&mut self, raw: i32, want: Readiness, tag: u64) -> Result<(), RtError> {
        let mut events = 0;
        if want.contains(Readiness::READ) {
            events |= READABLE_EVENTS;
        }
        if want.contains(Readiness::WRITE) {
            events |= WRITABLE_EVENTS;
        }
        self.arm_events(raw, events, tag)
    }
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::disallowed_methods,
        clippy::disallowed_macros
    )
)]
mod tests {
    use std::cell::Cell;

    use crate::combine::{Either, join2, race2};
    use crate::futures::{sleep, yield_now};
    use crate::runtime::{LocalRuntime, RuntimeConfig};
    use crate::udp::{Ipv4Addr, SocketAddr, UdpSocket};

    thread_local! {
        /// Set by a test to refuse the next poll this thread's driver issues (the thread is its shard's).
        pub(super) static FAIL_NEXT_ISSUE: Cell<bool> = const { Cell::new(false) };
    }

    /// Shape: how long a wait that should have woken is given before the test fails rather than hangs.
    const PATIENCE_NS: u64 = 10_000_000_000;

    fn config() -> RuntimeConfig {
        RuntimeConfig {
            shards: 1,
            tasks_per_shard: 16,
            timers_per_shard: 16,
            interests_per_shard: 16,
            ring_entries: 64,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            batch: 64,
            pin: false,
            cores: Vec::new(),
            page_bytes: 4096,
            spin_ns: 0,
            wake_tracking: None,
            latency_objective_ns: None,
        }
    }

    /// A timed wait ends at its timeout through the high-resolution timer, not at the system clock's
    /// next interrupt (15.625 ms apart unless some process has raised the rate): a 2 ms sleep's
    /// median end is below half that period, which a wait of whole milliseconds on the clock's own
    /// interrupt cannot reach.
    #[test]
    fn a_timed_wait_ends_at_its_timeout_not_the_clock_s_interrupt() {
        /// Shape: each sleep, well under the default interrupt period.
        const SLEEP_NS: u64 = 2_000_000;
        /// Shape: Windows' default timer interrupt period, 64 a second.
        const DEFAULT_PERIOD_NS: u64 = 15_625_000;
        /// Shape: sleeps taken; their median stands against a busy runner's late wakes.
        const TRIES: usize = 21;
        let mut rt = LocalRuntime::new(&config()).unwrap();
        let mut ended = rt
            .block_on(async {
                let mut ended = Vec::with_capacity(TRIES);
                for _ in 0..TRIES {
                    let began = crate::futures::now_ns();
                    sleep(SLEEP_NS).await.unwrap();
                    ended.push(crate::futures::now_ns().saturating_sub(began));
                }
                ended
            })
            .unwrap();
        ended.sort_unstable();
        let median = ended.get(TRIES / 2).copied().unwrap();
        assert!(
            median < DEFAULT_PERIOD_NS / 2,
            "a {SLEEP_NS} ns sleep ended after {median} ns at the median: {ended:?}"
        );
    }

    /// A timed wait a kick ends leaves its timer armed; the untimed wait after it disarms it first,
    /// so that timer cannot end it (each wait here ends at once on a kick sent before it).
    #[test]
    fn an_untimed_wait_disarms_the_timer_a_timed_one_left() {
        use crate::driver::{Driver, Kick};
        /// Shape: a timed wait's timeout, far past the kick that ends it.
        const WAIT_NS: u64 = 1_000_000_000;
        let port = super::prepare().unwrap();
        let registration =
            crate::registry::register(2, 1, crate::runtime::register_kick(Some(port))).unwrap();
        let kick = crate::registry::with_entry(registration.shard(), |entry| entry.kick).unwrap();
        let Kick::Iocp(port) = kick else {
            panic!("the IOCP driver's kick is its port");
        };
        let mut driver = super::IocpDriver::from_prepared(port).unwrap();
        let armed = |driver: &super::IocpDriver| driver.precise.as_ref().map(|timer| timer.armed);
        if armed(&driver).is_none() {
            eprintln!("this Windows carries no high-resolution timer or no wait completion packet");
            return;
        }
        let mut out = Vec::new();
        kick.kick();
        driver.wait(Some(WAIT_NS), &mut out).unwrap();
        assert_eq!(armed(&driver), Some(true));
        kick.kick();
        driver.wait(None, &mut out).unwrap();
        assert_eq!(armed(&driver), Some(false));
        drop(driver);
        drop(registration);
    }

    /// Mantle's final review, second pass, finding 1. Do: a reader waits on a socket (its AFD poll in flight);
    /// a writer's wait on the same socket needs a wider poll, whose issue is refused; then a datagram arrives.
    /// Expect: the writer's wait is refused, and the reader still wakes. The old poll used to be cancelled
    /// before the wider one was issued, so the refusal left the reader with no poll and it slept for good.
    #[test]
    fn a_refused_wider_poll_leaves_the_waiters_already_on_the_socket() {
        let mut rt = LocalRuntime::new(&config()).unwrap();
        rt.block_on(async {
            let ours = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
            let peer = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
            let to = ours.local_addr().unwrap();
            let joined = join2(ours.readable(), async {
                // One yield: the loop arms the reader's poll before the writer asks for the wider one.
                yield_now().await;
                FAIL_NEXT_ISSUE.with(|fail| fail.set(true));
                assert!(ours.writable().await.is_err(), "the wider poll was refused");
                peer.send_to(b"x", to).unwrap();
            });
            match race2(joined, sleep(PATIENCE_NS)).await {
                Either::First((read, ())) => read.unwrap(),
                Either::Second(_) => {
                    panic!("the reader's wait was stranded by the refused replacement")
                }
            }
        })
        .unwrap();
    }
}
