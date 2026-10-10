//! The platform seam of local stream sockets (docs/runtime.md §5.3): `AF_UNIX` stream sockets on Linux,
//! macOS and Windows (Windows 10 1803 and later), and the peer's identity as the kernel reports it. This
//! file creates, binds and connects the sockets and reads the identity; once made, a socket is a stream like
//! a TCP one, read and written through [`crate::tcpsys`].
//!
//! The peer's identity: `SO_PEERCRED` on Linux; `getpeereid` and `LOCAL_PEERPID` on macOS; on Windows
//! `SIO_AF_UNIX_GETPEERPID`, then that process's token's user SID. Never from the bytes.
#![allow(unsafe_code)]

/// Who the peer of a local stream is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerIdentity {
    /// The peer's process, where the OS reports it.
    pub pid: Option<u32>,
    /// The user the peer runs as.
    pub user: UserId,
}

/// A user as the OS names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserId {
    /// A Unix user id.
    Uid(u32),
    /// A Windows security identifier, in its binary form.
    Sid(Box<[u8]>),
}

// ============================================================================== Unix (rustix)

#[cfg(unix)]
mod imp {
    use std::os::fd::OwnedFd;
    use std::path::Path;

    use rustix::io::Errno;
    use rustix::net::{AddressFamily, SocketAddrUnix, SocketFlags, SocketType};

    use super::{PeerIdentity, UserId};
    use crate::driver::refused;
    use crate::error::RtError;
    use crate::netsys::Io;

    fn socket() -> Result<OwnedFd, RtError> {
        let fd = rustix::net::socket_with(
            AddressFamily::UNIX,
            SocketType::STREAM,
            SocketFlags::empty(),
            None,
        )
        .map_err(|e| refused("socket(AF_UNIX)", e))?;
        rustix::io::fcntl_setfd(&fd, rustix::io::FdFlags::CLOEXEC)
            .map_err(|e| refused("fcntl(CLOEXEC)", e))?;
        rustix::io::ioctl_fionbio(&fd, true).map_err(|e| refused("ioctl(FIONBIO)", e))?;
        Ok(fd)
    }

    fn address(path: &Path) -> Result<SocketAddrUnix, RtError> {
        SocketAddrUnix::new(path).map_err(|e| refused("sockaddr_un(path)", e))
    }

    /// A listening socket bound at `path`. Refused when the path exists: a stale socket is the caller's
    /// to remove, in a directory only its owner may enter.
    pub(crate) fn listen(path: &Path, backlog: i32) -> Result<OwnedFd, RtError> {
        let fd = socket()?;
        rustix::net::bind(&fd, &address(path)?).map_err(|e| refused("bind(AF_UNIX)", e))?;
        rustix::net::listen(&fd, backlog).map_err(|e| refused("listen", e))?;
        Ok(fd)
    }

    /// Starts a connection to `path`. A local connect completes at once or is refused; Linux reports a
    /// full accept queue as `EAGAIN` (unix(7)), which waiting on writability would not end, so it is a
    /// typed refusal the caller retries.
    pub(crate) fn connect(path: &Path) -> Result<(OwnedFd, Io<()>), RtError> {
        let fd = socket()?;
        match rustix::net::connect(&fd, &address(path)?) {
            Ok(()) => Ok((fd, Io::Ready(()))),
            Err(Errno::INPROGRESS | Errno::INTR) => Ok((fd, Io::WouldBlock)),
            Err(Errno::AGAIN) => Err(RtError::WouldBlock {
                call: "connect(AF_UNIX): the listener's accept queue is full",
            }),
            Err(e) => Err(refused("connect(AF_UNIX)", e)),
        }
    }

    /// The peer's identity (Linux: `SO_PEERCRED`).
    #[cfg(target_os = "linux")]
    pub(crate) fn peer(fd: i32) -> Result<PeerIdentity, RtError> {
        // SAFETY: `fd` is the caller's open socket, borrowed for this call only.
        let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
        let cred = rustix::net::sockopt::socket_peercred(fd)
            .map_err(|e| refused("getsockopt(SO_PEERCRED)", e))?;
        Ok(PeerIdentity {
            pid: u32::try_from(cred.pid.as_raw_nonzero().get()).ok(),
            user: UserId::Uid(cred.uid.as_raw()),
        })
    }

    /// The peer's identity (macOS: `getpeereid`, and `LOCAL_PEERPID` for its process).
    #[cfg(target_os = "macos")]
    pub(crate) fn peer(fd: i32) -> Result<PeerIdentity, RtError> {
        let (mut uid, mut gid): (libc::uid_t, libc::gid_t) = (0, 0);
        // SAFETY: `fd` is the caller's open socket; the out pointers name two live locals.
        if unsafe { libc::getpeereid(fd, &raw mut uid, &raw mut gid) } != 0 {
            return Err(RtError::os("getpeereid"));
        }
        let mut pid: libc::pid_t = 0;
        let mut len =
            libc::socklen_t::try_from(size_of::<libc::pid_t>()).unwrap_or(libc::socklen_t::MAX);
        // SAFETY: `LOCAL_PEERPID` is a `pid_t`; the out pointer and its length name one live local.
        let outcome = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_LOCAL,
                libc::LOCAL_PEERPID,
                (&raw mut pid).cast::<libc::c_void>(),
                &raw mut len,
            )
        };
        Ok(PeerIdentity {
            pid: (outcome == 0).then(|| u32::try_from(pid).ok()).flatten(),
            user: UserId::Uid(uid),
        })
    }

    /// The user this process runs as (its effective uid, the one a peer's `getpeereid` reports).
    pub(crate) fn current_user() -> Result<UserId, RtError> {
        Ok(UserId::Uid(rustix::process::geteuid().as_raw()))
    }
}

// ============================================================================== Windows (Winsock 2)

#[cfg(windows)]
mod imp {
    use std::os::windows::io::{FromRawSocket, OwnedSocket, RawSocket};
    use std::path::Path;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::Networking::WinSock::{
        AF_UNIX, INVALID_SOCKET, SIO_AF_UNIX_GETPEERPID, SOCK_STREAM, SOCKADDR, SOCKADDR_UN,
        SOCKET, SOCKET_ERROR, WSAEINPROGRESS, WSAEWOULDBLOCK, WSAGetLastError, WSAIoctl,
        bind as ws_bind, connect as ws_connect, listen as ws_listen, socket as ws_socket,
    };
    use windows_sys::Win32::Security::{
        GetLengthSid, GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    use super::{PeerIdentity, UserId};
    use crate::error::RtError;
    use crate::netsys::Io;
    use crate::netsys::winsock::{ensure_started, last, set_nonblocking};

    /// Takes ownership of a fresh socket.
    fn own(socket: SOCKET) -> OwnedSocket {
        let raw = RawSocket::try_from(socket).unwrap_or(RawSocket::MAX);
        // SAFETY: `socket` was just created by `socket()` and is owned by nothing else; the `OwnedSocket`
        // becomes its one owner and closes it once.
        unsafe { OwnedSocket::from_raw_socket(raw) }
    }

    fn socket() -> Result<OwnedSocket, RtError> {
        ensure_started()?;
        // SAFETY: a plain socket creation; the result is checked against INVALID_SOCKET.
        let raw = unsafe { ws_socket(i32::from(AF_UNIX), SOCK_STREAM, 0) };
        if raw == INVALID_SOCKET {
            return Err(last("socket(AF_UNIX)"));
        }
        let owned = own(raw);
        set_nonblocking(raw)?;
        Ok(owned)
    }

    /// `path` as a `SOCKADDR_UN` and its length; refused when it does not fit `sun_path` with its nul.
    fn address(path: &Path) -> Result<(SOCKADDR_UN, i32), RtError> {
        let bytes = path.to_str().ok_or(RtError::BadConfig {
            what: "a local socket path that is not UTF-8",
        })?;
        let mut name = SOCKADDR_UN {
            sun_family: AF_UNIX,
            sun_path: [0; 108],
        };
        if bytes.len() >= name.sun_path.len() {
            return Err(RtError::BadConfig {
                what: "a local socket path longer than sun_path",
            });
        }
        for (into, from) in name.sun_path.iter_mut().zip(bytes.bytes()) {
            *into = i8::from_ne_bytes([from]);
        }
        let len = size_of::<u16>()
            .saturating_add(bytes.len())
            .saturating_add(1);
        Ok((name, i32::try_from(len).unwrap_or(0)))
    }

    pub(crate) fn listen(path: &Path, backlog: i32) -> Result<OwnedSocket, RtError> {
        let owned = socket()?;
        let raw = SOCKET::try_from(std::os::windows::io::AsRawSocket::as_raw_socket(&owned))
            .unwrap_or(INVALID_SOCKET);
        let (name, len) = address(path)?;
        // SAFETY: `name` is a live SOCKADDR_UN of `len` bytes.
        if unsafe { ws_bind(raw, (&raw const name).cast::<SOCKADDR>(), len) } == SOCKET_ERROR {
            return Err(last("bind(AF_UNIX)"));
        }
        // SAFETY: a plain call on our socket.
        if unsafe { ws_listen(raw, backlog) } == SOCKET_ERROR {
            return Err(last("listen"));
        }
        Ok(owned)
    }

    pub(crate) fn connect(path: &Path) -> Result<(OwnedSocket, Io<()>), RtError> {
        let owned = socket()?;
        let raw = SOCKET::try_from(std::os::windows::io::AsRawSocket::as_raw_socket(&owned))
            .unwrap_or(INVALID_SOCKET);
        let (name, len) = address(path)?;
        // SAFETY: `name` is a live SOCKADDR_UN of `len` bytes.
        if unsafe { ws_connect(raw, (&raw const name).cast::<SOCKADDR>(), len) } != SOCKET_ERROR {
            return Ok((owned, Io::Ready(())));
        }
        // SAFETY: a pure query of thread-local last-error state.
        match unsafe { WSAGetLastError() } {
            WSAEWOULDBLOCK | WSAEINPROGRESS => Ok((owned, Io::WouldBlock)),
            _ => Err(last("connect(AF_UNIX)")),
        }
    }

    /// Closes a handle when dropped.
    struct Handle(HANDLE);

    impl Drop for Handle {
        fn drop(&mut self) {
            // SAFETY: a handle this module opened, closed once.
            unsafe { CloseHandle(self.0) };
        }
    }

    /// The user SID of `process`'s token, in binary form.
    fn token_user(process: HANDLE) -> Result<Box<[u8]>, RtError> {
        let mut token: HANDLE = std::ptr::null_mut();
        // SAFETY: `process` is a live process handle; `token` receives a new handle on success.
        if unsafe { OpenProcessToken(process, TOKEN_QUERY, &raw mut token) } == 0 {
            return Err(RtError::os("OpenProcessToken"));
        }
        let token = Handle(token);
        let mut needed: u32 = 0;
        // SAFETY: a null buffer of length 0 asks for the length the information needs.
        unsafe {
            GetTokenInformation(token.0, TokenUser, std::ptr::null_mut(), 0, &raw mut needed)
        };
        let words = usize::try_from(needed)
            .unwrap_or(0)
            .div_ceil(size_of::<u64>());
        // `u64` words, so the buffer is aligned for `TOKEN_USER`.
        let mut buffer = vec![0u64; words.max(1)];
        let len = u32::try_from(buffer.len().saturating_mul(size_of::<u64>())).unwrap_or(0);
        // SAFETY: `buffer` is `len` writable bytes, aligned for `TOKEN_USER`, which the call fills.
        if unsafe {
            GetTokenInformation(
                token.0,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                len,
                &raw mut needed,
            )
        } == 0
        {
            return Err(RtError::os("GetTokenInformation(TokenUser)"));
        }
        // SAFETY: the call wrote a `TOKEN_USER` at the start of the buffer; its SID pointer points inside it.
        let sid = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        // SAFETY: `sid` is a valid SID the token information holds.
        let sid_len = usize::try_from(unsafe { GetLengthSid(sid) }).unwrap_or(0);
        // SAFETY: the SID is `sid_len` bytes inside `buffer`, live for this read.
        let bytes = unsafe { std::slice::from_raw_parts(sid.cast::<u8>(), sid_len) };
        Ok(bytes.into())
    }

    /// The peer's identity: its process (`SIO_AF_UNIX_GETPEERPID`), then that process's user.
    pub(crate) fn peer(raw: SOCKET) -> Result<PeerIdentity, RtError> {
        let mut pid: u32 = 0;
        let mut returned: u32 = 0;
        // SAFETY: the ioctl writes one `u32` (the peer's process id) into `pid`; no overlapped structure.
        let outcome = unsafe {
            WSAIoctl(
                raw,
                SIO_AF_UNIX_GETPEERPID,
                std::ptr::null(),
                0,
                (&raw mut pid).cast(),
                u32::try_from(size_of::<u32>()).unwrap_or(0),
                &raw mut returned,
                std::ptr::null_mut(),
                None,
            )
        };
        if outcome == SOCKET_ERROR {
            return Err(last("WSAIoctl(SIO_AF_UNIX_GETPEERPID)"));
        }
        // SAFETY: opening a process by id for a limited query; the result is checked.
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if process.is_null() {
            return Err(RtError::os("OpenProcess"));
        }
        let process = Handle(process);
        Ok(PeerIdentity {
            pid: Some(pid),
            user: UserId::Sid(token_user(process.0)?),
        })
    }

    pub(crate) fn current_user() -> Result<UserId, RtError> {
        // SAFETY: the pseudo-handle of the current process, which needs no closing.
        Ok(UserId::Sid(token_user(unsafe { GetCurrentProcess() })?))
    }
}

pub(crate) use imp::{connect, current_user, listen, peer};
