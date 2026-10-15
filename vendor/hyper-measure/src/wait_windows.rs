//! The Windows half of [`super::arrives`]: a wait for a datagram that has no operation in flight
//! past its return (`WSAPoll`, winsock2.h).
//!
//! A blocking receive or peek that times out (`SO_RCVTIMEO`) is an operation Windows cancels as the
//! call returns, and on windows-11-arm the cancelled operation can complete after the call has
//! returned: it writes its status, the datagram's length, into the frames of a call that has
//! already ended, which the caller's next frames reuse (`docs/raft.md`, "The harness's receive").
//! A poll only asks whether the socket has something to take; it writes nothing past its return.
#![allow(unsafe_code)]

use std::io;
use std::net::UdpSocket;
use std::os::windows::io::AsRawSocket;
use std::time::Duration;

use windows_sys::Win32::Networking::WinSock::{POLLRDNORM, SOCKET_ERROR, WSAPOLLFD, WSAPoll};

/// Nanoseconds in the millisecond `WSAPoll` counts its timeout in.
const NANOS_PER_MILLI: u128 = 1_000_000;

/// The wait `WSAPoll` takes: `-1` for as long as it takes, otherwise `wait` in milliseconds,
/// rounded up so that a wait is never cut short, at most `i32::MAX`.
fn timeout(wait: Option<Duration>) -> i32 {
    match wait {
        None => -1,
        Some(wait) => i32::try_from(wait.as_nanos().div_ceil(NANOS_PER_MILLI)).unwrap_or(i32::MAX),
    }
}

/// Waits until `socket` has a datagram or an error to take, or `wait` passes: whether it has.
pub(super) fn readable(socket: &UdpSocket, wait: Option<Duration>) -> io::Result<bool> {
    let fd = usize::try_from(socket.as_raw_socket())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "a socket past usize"))?;
    let mut polled = WSAPOLLFD {
        fd,
        events: POLLRDNORM,
        revents: 0,
    };
    // SAFETY: `polled` is one writable WSAPOLLFD for the call, which reads its socket and events and
    // writes its returned events before it returns; the count passed is one. The socket is
    // `socket`'s, open for the call.
    let ready = unsafe { WSAPoll(&raw mut polled, 1, timeout(wait)) };
    if ready == SOCKET_ERROR {
        return Err(io::Error::last_os_error());
    }
    Ok(ready > 0)
}
