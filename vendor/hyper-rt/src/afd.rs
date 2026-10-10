//! AFD-based socket readiness for the IOCP driver (§4.6, §4.10a).
//!
//! Windows IOCP is *completion*-based, but the runtime's socket model (`readable`/`writable`,
//! `crate::readiness`) needs *edge readiness* — "tell me when this socket can be read/written", the
//! same shape kqueue and epoll give for free. The kernel's Ancillary Function Driver, `\Device\Afd`,
//! provides it: an `IOCTL_AFD_POLL` issued asynchronously on a socket completes on the associated
//! completion port when the socket reaches one of the requested edges. This is exactly the mechanism
//! wepoll and mio/libuv use to build epoll-like readiness on Windows [C: wepoll, Bert Belder; C: mio
//! `src/sys/windows/afd.rs`]. We keep the reference design's shape: one AFD device handle per driver,
//! associated with the port under a dedicated key; one poll per registration, its state in a heap
//! block whose `OVERLAPPED` the completion delivers back so the waker word is recovered.
//!
//! The FFI is declared here explicitly (the `Nt*` calls and the AFD structs are stable and documented
//! in the WDK but scattered across `windows-sys` feature gates); this keeps the readiness reactor
//! self-contained and matches the reference implementations. Every block carries a `// SAFETY:` line.
#![allow(unsafe_code)]

use std::ffi::c_void;

use windows_sys::Win32::Foundation::{HANDLE, NTSTATUS};
use windows_sys::Win32::Networking::WinSock::{SIO_BASE_HANDLE, SOCKET, WSAIoctl};
use windows_sys::Win32::System::IO::OVERLAPPED;

use crate::error::RtError;

/// Format: `IOCTL_AFD_POLL` — the device control code that arms an AFD readiness poll (wepoll/mio;
/// `FSCTL`-style code for `\Device\Afd`, function 9, method buffered).
const IOCTL_AFD_POLL: u32 = 0x0001_2024;

/// Format: `STATUS_PENDING` — the async IOCTL was accepted and will complete on the port later.
const STATUS_PENDING: NTSTATUS = 0x0000_0103;
/// Format: `STATUS_SUCCESS`.
const STATUS_SUCCESS: NTSTATUS = 0x0000_0000;
/// Format: `STATUS_CANCELLED` (ntstatus.h), the status a cancelled IRP completes with.
const STATUS_CANCELLED: u32 = 0xC000_0120;
/// Format: `FILE_OPEN` — open the existing device, do not create (`NtCreateFile` disposition).
const FILE_OPEN: u32 = 0x0000_0001;
/// Format: `SYNCHRONIZE` — the only access the AFD helper handle needs.
const SYNCHRONIZE: u32 = 0x0010_0000;

/// Format: AFD poll event — the socket has normal data to receive (readable).
pub(crate) const AFD_POLL_RECEIVE: u32 = 0x0001;
/// Format: AFD poll event — the socket has expedited (out-of-band) data (readable).
pub(crate) const AFD_POLL_RECEIVE_EXPEDITED: u32 = 0x0002;
/// Format: AFD poll event — the socket has send-buffer space (writable).
pub(crate) const AFD_POLL_SEND: u32 = 0x0004;
/// Format: AFD poll event — the peer has gracefully shut down the receive side (readable: EOF).
pub(crate) const AFD_POLL_DISCONNECT: u32 = 0x0008;
/// Format: AFD poll event — the connection was aborted (readable/writable: an error to observe).
pub(crate) const AFD_POLL_ABORT: u32 = 0x0010;
/// Format: AFD poll event — the socket handle is being closed locally.
pub(crate) const AFD_POLL_LOCAL_CLOSE: u32 = 0x0020;
/// Format: AFD poll event — a listening socket has a connection to accept (readable).
pub(crate) const AFD_POLL_ACCEPT: u32 = 0x0080;
/// Format: AFD poll event — a non-blocking connect failed (readable/writable: an error to observe).
pub(crate) const AFD_POLL_CONNECT_FAIL: u32 = 0x0100;

/// The AFD poll events that mean a read will not block: data, an incoming connection, EOF, or an
/// error the caller's retried syscall then surfaces. Matches wepoll's `EPOLLIN` set.
pub(crate) const READABLE_EVENTS: u32 = AFD_POLL_RECEIVE
    | AFD_POLL_RECEIVE_EXPEDITED
    | AFD_POLL_ACCEPT
    | AFD_POLL_DISCONNECT
    | AFD_POLL_ABORT
    | AFD_POLL_CONNECT_FAIL
    | AFD_POLL_LOCAL_CLOSE;

/// The AFD poll events that mean a write will not block: send-buffer space, or an error/close the
/// caller's retried syscall then surfaces. Matches wepoll's `EPOLLOUT` set.
pub(crate) const WRITABLE_EVENTS: u32 =
    AFD_POLL_SEND | AFD_POLL_ABORT | AFD_POLL_CONNECT_FAIL | AFD_POLL_LOCAL_CLOSE;

/// A counted UTF-16 string as the object manager takes it (`UNICODE_STRING`).
#[repr(C)]
struct UnicodeString {
    length: u16,
    maximum_length: u16,
    buffer: *mut u16,
}

/// The name and attributes of a kernel object to open (`OBJECT_ATTRIBUTES`).
#[repr(C)]
struct ObjectAttributes {
    length: u32,
    root_directory: HANDLE,
    object_name: *const UnicodeString,
    attributes: u32,
    security_descriptor: *mut c_void,
    security_quality_of_service: *mut c_void,
}

/// The result cell an `Nt*` call fills (`IO_STATUS_BLOCK`). Its first two pointer-width fields alias
/// an `OVERLAPPED`'s `Internal`/`InternalHigh`, which is why one poll block serves as both.
#[repr(C)]
struct IoStatusBlock {
    status: NTSTATUS,
    information: usize,
}

/// One socket's requested events in an [`AfdPollInfo`] (`AFD_POLL_HANDLE_INFO`).
#[repr(C)]
struct AfdPollHandleInfo {
    handle: HANDLE,
    events: u32,
    status: NTSTATUS,
}

/// The input/output buffer of an `IOCTL_AFD_POLL` (`AFD_POLL_INFO`): the requested events on the way
/// in, the events that fired on the way out. One handle per poll (the runtime registers per socket).
#[repr(C)]
struct AfdPollInfo {
    /// A `LARGE_INTEGER` timeout; `i64::MAX` means the poll never expires on its own (one-shot until an
    /// event fires — the readiness future disarms it by consuming the completion, then re-arms if it
    /// still needs the edge).
    timeout: i64,
    number_of_handles: u32,
    exclusive: u32,
    handles: [AfdPollHandleInfo; 1],
}

/// One armed readiness poll: the `OVERLAPPED` the completion port delivers back (its head aliases the
/// `IO_STATUS_BLOCK` the IOCTL fills), the poll buffer, and the waker word to wake when it fires. Heap
/// -allocated and leaked into the kernel for the poll's duration; reclaimed when the completion lands
/// (`Block::reclaim`). `#[repr(C)]` with `overlapped` first, so the delivered pointer *is* the block.
#[repr(C)]
pub(crate) struct Block {
    overlapped: OVERLAPPED,
    poll_info: AfdPollInfo,
    /// The handle's tag the driver reports as the completion's `user_data`.
    user_data: u64,
    /// The events asked for, kept apart from `poll_info`, which the kernel overwrites with those that fired.
    requested: u32,
}

/// What a poll's completion carried, by the status the IRP completed with — not by whether the driver asked
/// to cancel it: a poll that completed before its cancel reached it carries its events, which are delivered
/// (mantle's final review, finding 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The poll completed with these events.
    Fired(u32),
    /// The poll was cancelled before it completed: nothing to deliver.
    Cancelled,
    /// The poll failed: its waiters are woken in every direction, so their calls see the error (wepoll
    /// maps a failed poll to `EPOLLERR` the same way, `sock_feed_event`).
    Failed,
}

/// What a poll's completion carried.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Reclaimed {
    /// The handle's tag.
    pub(crate) user_data: u64,
    /// How the poll ended.
    pub(crate) outcome: Outcome,
}

impl Block {
    /// Boxes a poll block for `base` socket watching `events`, on behalf of `user_data`, and leaks it to
    /// a raw pointer the kernel holds until the poll completes. `overlapped` starts zeroed.
    fn leak(base: SOCKET, events: u32, user_data: u64) -> *mut Block {
        let block = Block {
            // SAFETY: an all-zero OVERLAPPED is a valid, un-started overlapped record.
            overlapped: unsafe { std::mem::zeroed() },
            poll_info: AfdPollInfo {
                timeout: i64::MAX,
                number_of_handles: 1,
                exclusive: 0,
                handles: [AfdPollHandleInfo {
                    handle: base as HANDLE,
                    events,
                    status: 0,
                }],
            },
            user_data,
            requested: events,
        };
        Box::into_raw(Box::new(block))
    }

    /// Reclaims a leaked block once its completion has been delivered, returning the waker word it
    /// carried. The caller must pass a pointer the driver got back from the port exactly once.
    ///
    /// # Safety
    /// `ptr` must be a live `Block` leaked by [`Block::leak`] and delivered by the port, taken once.
    pub(crate) unsafe fn reclaim(ptr: *mut Block) -> Reclaimed {
        // SAFETY: the contract above — a leaked block delivered once; `from_raw` takes ownership back.
        let block = unsafe { Box::from_raw(ptr) };
        // The status the IOCTL left in the overlapped's head (`Internal`, the IO_STATUS_BLOCK's status), as
        // the NTSTATUS's 32 bits.
        let status = u32::try_from(block.overlapped.Internal & 0xFFFF_FFFF).unwrap_or(u32::MAX);
        let outcome = match status {
            0 => Outcome::Fired(
                block
                    .poll_info
                    .handles
                    .first()
                    .map_or(0, |handle| handle.events),
            ),
            STATUS_CANCELLED => Outcome::Cancelled,
            _ => Outcome::Failed,
        };
        Reclaimed {
            user_data: block.user_data,
            outcome,
        }
    }

    /// The events a poll still in flight asked for.
    ///
    /// # Safety
    /// `ptr` must be a live `Block` leaked by [`Block::leak`] whose completion has not been reclaimed. The
    /// kernel writes only the block's `overlapped` and `poll_info`; this reads neither.
    pub(crate) unsafe fn requested(ptr: *mut Block) -> u32 {
        // SAFETY: the contract above; `requested` is the driver's own field, which the kernel never touches.
        unsafe { (*ptr).requested }
    }
}

unsafe extern "system" {
    // structural: allow — declared for the one `FILE_OPEN` of the `\Device\Afd` helper device; creates nothing.
    fn NtCreateFile(
        file_handle: *mut HANDLE,
        desired_access: u32,
        object_attributes: *const ObjectAttributes,
        io_status_block: *mut IoStatusBlock,
        allocation_size: *const i64,
        file_attributes: u32,
        share_access: u32,
        create_disposition: u32,
        create_options: u32,
        ea_buffer: *const c_void,
        ea_length: u32,
    ) -> NTSTATUS;

    fn NtDeviceIoControlFile(
        file_handle: HANDLE,
        event: HANDLE,
        apc_routine: *const c_void,
        apc_context: *const c_void,
        io_status_block: *mut IoStatusBlock,
        io_control_code: u32,
        input_buffer: *const c_void,
        input_buffer_length: u32,
        output_buffer: *mut c_void,
        output_buffer_length: u32,
    ) -> NTSTATUS;

    fn RtlNtStatusToDosError(status: NTSTATUS) -> u32;
    fn NtCancelIoFileEx(
        file_handle: HANDLE,
        io_request_to_cancel: *mut IoStatusBlock,
        io_status_block: *mut IoStatusBlock,
    ) -> NTSTATUS;

    fn CloseHandle(handle: HANDLE) -> i32;
}

/// The AFD helper device the driver polls sockets through, associated with the driver's completion
/// port so a poll's completion lands there. One per driver; closed on drop.
pub(crate) struct Afd {
    handle: HANDLE,
}

impl Afd {
    /// Opens `\Device\Afd\Slates` and associates it with `port` under `key` (via `CreateIoCompletionPort`
    /// — done by the caller, which owns the port handle), so every poll issued on it completes on the
    /// port. The trailing name is arbitrary (wepoll uses `\Device\Afd\Wepoll`); it just names this reuse.
    pub(crate) fn open() -> Result<Afd, RtError> {
        // "\Device\Afd\Slates" as UTF-16, no nul (a counted UNICODE_STRING).
        let name: Vec<u16> = "\\Device\\Afd\\Slates".encode_utf16().collect();
        let byte_len = name
            .len()
            .checked_mul(size_of::<u16>())
            .and_then(|bytes| u16::try_from(bytes).ok())
            .unwrap_or(u16::MAX);
        let unicode = UnicodeString {
            length: byte_len,
            maximum_length: byte_len,
            buffer: name.as_ptr().cast_mut(),
        };
        let attributes = ObjectAttributes {
            length: u32::try_from(size_of::<ObjectAttributes>()).unwrap_or(0),
            root_directory: std::ptr::null_mut(),
            object_name: &unicode,
            attributes: 0,
            security_descriptor: std::ptr::null_mut(),
            security_quality_of_service: std::ptr::null_mut(),
        };
        let mut handle: HANDLE = std::ptr::null_mut();
        let mut iosb = IoStatusBlock {
            status: 0,
            information: 0,
        };
        /// Format: `FILE_SHARE_READ | FILE_SHARE_WRITE` (`0x1 | 0x2`) — the device is shareable across
        /// drivers, so several runtimes can open `\Device\Afd` at once.
        const FILE_SHARE_READ_WRITE: u32 = 0x0000_0003;
        // SAFETY: `attributes` names a valid device and is live for the call; `handle`/`iosb` are writable;
        // the `name` buffer outlives the call. All other pointers are the documented nulls.
        let status = unsafe {
            // structural: allow — `FILE_OPEN` of the `\Device\Afd` helper device with `SYNCHRONIZE` only; no file.
            NtCreateFile(
                &mut handle,
                SYNCHRONIZE,
                &attributes,
                &mut iosb,
                std::ptr::null(),
                0,
                FILE_SHARE_READ_WRITE,
                FILE_OPEN,
                0,
                std::ptr::null(),
                0,
            )
        };
        if status != STATUS_SUCCESS {
            return Err(nt_error("NtCreateFile(\\Device\\Afd)", status));
        }
        Ok(Afd { handle })
    }

    /// The device handle, for the caller to associate with its completion port.
    pub(crate) fn handle(&self) -> HANDLE {
        self.handle
    }

    /// Arms a one-shot poll for `events` on `base` (a *base* socket handle, see [`base_socket`]) on
    /// behalf of `user_data`, leaking a [`Block`] the port delivers back on completion. Returns the raw
    /// block pointer so the driver can track and reclaim it. A poll that fails to arm reclaims its block
    /// and returns the error; one that completes synchronously (the socket is already ready) still posts
    /// to the port, so the driver's wait handles both the pending and immediate cases uniformly.
    ///
    /// # Safety
    /// `self.handle` must be associated with the caller's completion port before any poll is armed, so
    /// the delivered `OVERLAPPED` is the leaked block. The caller reclaims the returned pointer exactly
    /// once, when the port delivers it.
    pub(crate) unsafe fn poll(
        &self,
        base: SOCKET,
        events: u32,
        user_data: u64,
    ) -> Result<*mut Block, RtError> {
        let block = Block::leak(base, events, user_data);
        let info_len = u32::try_from(size_of::<AfdPollInfo>()).unwrap_or(0);
        // SAFETY: `block` is a fresh leaked `Block`; its `overlapped` head aliases the IO_STATUS_BLOCK the
        // IOCTL fills, and its `poll_info` is the in/out buffer. The AFD handle is associated with the
        // port (contract above), so a null event/APC posts the completion to the port on the block's
        // overlapped. On synchronous completion the status is SUCCESS and the packet is still posted.
        let status = unsafe {
            let overlapped = std::ptr::addr_of_mut!((*block).overlapped);
            let info = std::ptr::addr_of_mut!((*block).poll_info);
            NtDeviceIoControlFile(
                self.handle,
                std::ptr::null_mut(),
                std::ptr::null(),
                overlapped.cast(),
                overlapped.cast(),
                IOCTL_AFD_POLL,
                info.cast(),
                info_len,
                info.cast(),
                info_len,
            )
        };
        if status == STATUS_PENDING || status == STATUS_SUCCESS {
            Ok(block)
        } else {
            // The poll never armed: reclaim the block here (no completion will), and report the error.
            // SAFETY: `block` was just leaked and no completion will be delivered for a failed arm.
            let _ = unsafe { Box::from_raw(block) };
            Err(nt_error("NtDeviceIoControlFile(IOCTL_AFD_POLL)", status))
        }
    }
}

impl Afd {
    /// Cancels a poll still in flight: its completion still arrives on the port, and its block is reclaimed
    /// there. What it delivers follows the status it completed with ([`Outcome`]): a cancelled poll delivers
    /// nothing, and one that completed before the cancel reached it (`STATUS_NOT_FOUND` here) delivers its
    /// events. The cancel may finish after this returns (`CancelIoEx` does not wait), so the completion is
    /// awaited on the port, as every completion is.
    ///
    /// # Safety
    /// `ptr` must be a live `Block` this device's [`Afd::poll`] returned, whose completion has not been
    /// reclaimed.
    pub(crate) unsafe fn cancel(&self, ptr: *mut Block) {
        let mut status = IoStatusBlock {
            status: 0,
            information: 0,
        };
        // SAFETY: the contract above. The request's IO_STATUS_BLOCK is the block's `overlapped` head (the
        // address the IOCTL was given), only named here, not read; `status` is a live local for the call's own
        // result. Its result is not needed: the completion's own status says what happened.
        unsafe {
            let request = std::ptr::addr_of_mut!((*ptr).overlapped).cast::<IoStatusBlock>();
            NtCancelIoFileEx(self.handle, request, &raw mut status);
        }
    }
}

impl Drop for Afd {
    fn drop(&mut self) {
        // SAFETY: our handle, opened by `NtCreateFile`; closing it once on drop.
        unsafe { CloseHandle(self.handle) };
    }
}

/// The *base* socket under `socket`: a socket may sit above layered service providers (LSPs), and AFD
/// must poll the base handle the kernel actually owns. `SIO_BASE_HANDLE` returns it (an unlayered
/// socket returns itself). Without this a poll on a layered socket would watch the wrong object.
pub(crate) fn base_socket(socket: SOCKET) -> Result<SOCKET, RtError> {
    let mut base: SOCKET = 0;
    let mut returned: u32 = 0;
    // SAFETY: `socket` is a live socket; `SIO_BASE_HANDLE` takes no input and writes one SOCKET into
    // `base` with the count into `returned`. All other WSAIoctl pointers are the documented nulls.
    let rc = unsafe {
        WSAIoctl(
            socket,
            SIO_BASE_HANDLE,
            std::ptr::null_mut(),
            0,
            std::ptr::addr_of_mut!(base).cast(),
            u32::try_from(size_of::<SOCKET>()).unwrap_or(0),
            &mut returned,
            std::ptr::null_mut(),
            None,
        )
    };
    if rc != 0 {
        return Err(RtError::os("WSAIoctl(SIO_BASE_HANDLE)"));
    }
    Ok(base)
}

/// An [`RtError`] from an `NTSTATUS`, mapped to its Win32 error code so the refusal carries a number a
/// reader can look up (the same shape `RtError::os` produces from `GetLastError`).
fn nt_error(call: &'static str, status: NTSTATUS) -> RtError {
    // SAFETY: a pure mapping function over the status value; no memory is touched.
    let code = unsafe { RtlNtStatusToDosError(status) };
    RtError::DriverRefused {
        call,
        code: i32::try_from(code).ok(),
    }
}
