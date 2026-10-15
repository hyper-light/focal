//! How every real-socket test and E2E member in this repository waits for a datagram on a blocking
//! UDP socket: without taking it, and without an operation in flight once the wait has returned
//! (`docs/raft.md`, "The harness's receive"). The caller takes what arrived with a receive that
//! does not wait.
//!
//! The wait never takes the datagram: a receive that times out can lose the one that arrives as it
//! times out. Microsoft's `setsockopt` reference says of `SO_RCVTIMEO`: "If a blocking receive call
//! times out, the socket is left in an indeterminate state, and should not be used; TCP sockets in
//! this state have a potential for data loss, since the operation could be canceled at the same
//! moment the operation was to be completed." Measured on windows-11-arm, a receiver waiting a
//! millisecond at a time lost 65 of 40,000 datagrams that way.
//!
//! On Linux and macOS the wait is a peek with the timeout, a call the kernel finishes before it
//! returns: a peek takes nothing, and it takes no error either, so the receive after it takes
//! whatever it found. On Windows a peek that times out is an operation cancelled as the call
//! returns, and on windows-11-arm one completed after its call had returned, writing its status
//! into frames the caller had since reused: there the wait is a poll for readability
//! (`wait_windows.rs`), which writes nothing past its return.

use std::io;
use std::net::UdpSocket;
use std::time::Duration;

#[cfg(windows)]
#[path = "wait_windows.rs"]
mod windows;

/// Waits on the blocking `socket` for a datagram, for `wait` (never zero) or, with `None`, for as
/// long as it takes: whether there is something to take, a datagram or an error the socket
/// reports (such as the reset Windows reports after a send to a closed port). `buffer` is what a
/// peek reads into.
#[cfg(not(windows))]
#[allow(
    clippy::disallowed_methods,
    reason = "the wait the lint points to: a peek with the timeout, never a timed receive"
)]
pub fn arrives(socket: &UdpSocket, wait: Option<Duration>, buffer: &mut [u8]) -> io::Result<bool> {
    socket.set_read_timeout(wait)?;
    match socket.peek_from(buffer) {
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            Ok(false)
        }
        Ok(_) | Err(_) => Ok(true),
    }
}

/// Waits on `socket` for a datagram, as on every other platform, by a poll that has no operation
/// in flight past its return.
#[cfg(windows)]
pub fn arrives(socket: &UdpSocket, wait: Option<Duration>, _buffer: &mut [u8]) -> io::Result<bool> {
    windows::readable(socket, wait)
}
