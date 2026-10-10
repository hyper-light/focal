//! Local stream sockets (docs/runtime.md §5.3): `AF_UNIX` streams on Linux, macOS and Windows, one
//! mechanism on all three, awaited through the shard's driver as TCP is (on Windows through AFD, whose
//! readiness on `AF_UNIX` sockets is §15's open item 1, measured on the Windows lanes).
//!
//! **The peer's identity comes from the kernel, never from the bytes**: [`LocalStream::peer`] asks the OS who
//! connected (Linux `SO_PEERCRED`; macOS `getpeereid` and `LOCAL_PEERPID`; Windows `SIO_AF_UNIX_GETPEERPID`
//! and that process's token's user SID), and [`current_user`] names this process's user to compare it with.
//! Access is first kept by the directory the socket lives in, one only its owner may enter, so a peer of
//! another user cannot connect at all; the identity is the second wall.

use std::io::{IoSlice, IoSliceMut};
use std::path::Path;

pub use crate::localsys::{PeerIdentity, UserId};

use crate::error::RtError;
use crate::localsys;
use crate::netsys::Io;
use crate::readiness::{readable, writable};
use crate::tcp::{Shutdown, TcpStream};
use crate::tcpsys::{self, Stream};

/// The user this process runs as, to compare a peer's with.
pub fn current_user() -> Result<UserId, RtError> {
    localsys::current_user()
}

/// A listening local socket.
#[derive(Debug)]
pub struct LocalListener {
    stream: Stream,
    /// An accept takes at most this many connections before it yields.
    backlog: u32,
}

impl LocalListener {
    /// Listens at `path` with a queue of `backlog` pending connections. Refused when the path exists.
    pub fn bind(path: &Path, backlog: u32) -> Result<LocalListener, RtError> {
        let owned = localsys::listen(path, i32::try_from(backlog).unwrap_or(i32::MAX))?;
        Ok(LocalListener {
            stream: tcpsys::adopt(owned)?,
            backlog: backlog.max(1),
        })
    }

    /// Accepts the next connection, awaiting readability when none is pending; at most the backlog's worth
    /// of attempts before it yields to the driver.
    pub async fn accept(&self) -> Result<LocalStream, RtError> {
        loop {
            for _ in 0..self.backlog {
                match tcpsys::accept(&self.stream)? {
                    Io::Ready(accepted) => {
                        // A peer gone already is dropped; the listener goes on (finding 5).
                        if let Ok(inner) = TcpStream::local(accepted) {
                            return Ok(LocalStream { inner });
                        }
                    }
                    Io::WouldBlock => break,
                    Io::Interrupted | Io::Astray => {}
                }
            }
            readable(self.stream.raw_id()).await?;
        }
    }
}

/// A connected local stream.
#[derive(Debug)]
pub struct LocalStream {
    inner: TcpStream,
}

impl LocalStream {
    /// Connects to the listener at `path`.
    pub async fn connect(path: &Path) -> Result<LocalStream, RtError> {
        let (owned, started) = localsys::connect(path)?;
        let stream = tcpsys::adopt(owned)?;
        if let Io::WouldBlock | Io::Interrupted | Io::Astray = started {
            writable(stream.raw_id()).await?;
            if let Some(code) = tcpsys::take_error(&stream)? {
                return Err(RtError::DriverRefused {
                    call: "connect(AF_UNIX)",
                    code: Some(code),
                });
            }
        }
        Ok(LocalStream {
            inner: TcpStream::local(stream)?,
        })
    }

    /// Who is at the other end, as the kernel reports it.
    pub fn peer(&self) -> Result<PeerIdentity, RtError> {
        localsys::peer(self.inner.seam().raw_handle())
    }

    /// Reads into `buf`, awaiting readability: the byte count, zero at end of stream.
    pub async fn read(&self, buf: &mut [u8]) -> Result<usize, RtError> {
        self.inner.read(buf).await
    }

    /// Reads into `bufs` in order, in one call.
    pub async fn read_vectored(&self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize, RtError> {
        self.inner.read_vectored(bufs).await
    }

    /// Writes from `buf`, awaiting writability: the bytes taken.
    pub async fn write(&self, buf: &[u8]) -> Result<usize, RtError> {
        self.inner.write(buf).await
    }

    /// Writes from `bufs` in order, in one call.
    pub async fn write_vectored(&self, bufs: &[IoSlice<'_>]) -> Result<usize, RtError> {
        self.inner.write_vectored(bufs).await
    }

    /// Writes all of `buf`.
    pub async fn write_all(&self, buf: &[u8]) -> Result<(), RtError> {
        self.inner.write_all(buf).await
    }

    /// Shuts one half, or both.
    pub fn shutdown(&self, how: Shutdown) -> Result<(), RtError> {
        self.inner.shutdown(how)
    }
}
