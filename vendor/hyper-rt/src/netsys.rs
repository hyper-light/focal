//! The platform datagram-socket seam (§4.10a, the vorpal `mem/src/policy.rs` shape: paired `#[cfg]`
//! functions with identical signatures, so `udp` holds the async logic cfg-free and only the syscalls
//! differ). A [`Socket`] owns one non-blocking **UDP** socket of one family, IPv4 or IPv6 (an IPv6 one is
//! `IPV6_V6ONLY`, docs/runtime.md §5.1); the free functions are the blocking-free primitives the datagram
//! types drive, each returning [`Io`] so "not ready yet" and "an earlier datagram went astray" are values
//! the caller acts on, never errors to classify. The batched calls (`sendmmsg`, `recvmmsg`, the kernel's
//! receive stamps) are `crate::udp`'s own OS files; this seam creates, binds and moves single datagrams.
//!
//! The Unix arm is `rustix` (the lint wall reserves `std::net`; the address types are `core::net`'s,
//! which `rustix::net` re-exports); the Windows arm is Winsock 2 (`windows-sys`). Both present one
//! surface, so the readiness-native drivers back either — kqueue/epoll on Unix, the AFD reactor
//! (`crate::afd`) on the IOCP driver.
#![allow(unsafe_code)]

// The public address types are `core::net`'s on every platform (rustix re-exports the same ones), so a
// caller names a bind address without depending on the socket backend.
pub(crate) use core::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

/// The outcome of a non-blocking call: its value, or why there is none yet.
pub(crate) enum Io<T> {
    /// The operation completed with this value.
    Ready(T),
    /// The socket is not ready (`EAGAIN`/`WSAEWOULDBLOCK`); await the edge through the driver.
    WouldBlock,
    /// The call was interrupted before doing anything (`EINTR`); await the edge and retry.
    Interrupted,
    /// The socket reported a datagram sent earlier that went astray: an ICMP refusal or unreachable
    /// report (`ECONNREFUSED`, `EHOSTUNREACH`, `ENETUNREACH`), or Windows' reset for a send to a closed
    /// port (`WSAECONNRESET`, the `recvfrom` reference). It says nothing about this socket, the call
    /// moved no data, and the report is consumed by the call that returns it.
    Astray,
}

/// The address family a socket is made for: IPv6 sockets are `IPV6_V6ONLY`, so a dual-stack
/// service binds one socket per family, stated (docs/runtime.md §5.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Family {
    /// `AF_INET`.
    V4,
    /// `AF_INET6`, IPv6 only.
    V6,
}

impl Family {
    /// The family of `addr`.
    pub(crate) fn of(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(_) => Self::V4,
            SocketAddr::V6(_) => Self::V6,
        }
    }
}

// ============================================================================== Unix (rustix)

#[cfg(unix)]
mod imp {
    use std::os::fd::{AsRawFd, OwnedFd};

    use rustix::io::Errno;
    use rustix::net::{
        AddressFamily, RecvFlags, SendFlags, SocketAddr, SocketFlags, SocketType, bind as rx_bind,
        getsockname, recvfrom, sendto as rx_sendto, socket_with,
    };

    use super::{Family, Io, Ipv4Addr, SocketAddrV4};
    use crate::driver::refused;
    use crate::error::RtError;

    /// A non-blocking OS UDP socket owned here (closed on drop, by `OwnedFd`), and its family.
    #[derive(Debug)]
    pub(crate) struct Socket {
        fd: OwnedFd,
        family: Family,
    }

    impl Socket {
        /// The readiness handle a `crate::readiness` future registers (the raw fd).
        pub(crate) fn raw_id(&self) -> i32 {
            self.fd.as_raw_fd()
        }
    }

    /// The `Io` a failed non-blocking call maps to, or the refusal it is.
    fn classify<T>(call: &'static str, e: Errno) -> Result<Io<T>, RtError> {
        match e {
            Errno::AGAIN => Ok(Io::WouldBlock),
            Errno::INTR => Ok(Io::Interrupted),
            Errno::CONNREFUSED | Errno::CONNRESET | Errno::HOSTUNREACH | Errno::NETUNREACH => {
                Ok(Io::Astray)
            }
            e => Err(refused(call, e)),
        }
    }

    /// The socket's kernel receive buffer (`SO_RCVBUF`) in bytes.
    pub(crate) fn recv_buffer_bytes(socket: &Socket) -> Result<usize, RtError> {
        rustix::net::sockopt::socket_recv_buffer_size(&socket.fd)
            .map_err(|e| refused("getsockopt", e))
    }

    /// A non-blocking, close-on-exec datagram socket of `family`, IPv6 only when IPv6, sending with
    /// the don't-fragment bit.
    pub(crate) fn dgram_socket(family: Family) -> Result<Socket, RtError> {
        let domain = match family {
            Family::V4 => AddressFamily::INET,
            Family::V6 => AddressFamily::INET6,
        };
        // `SocketFlags` on `socket()` is Linux-only, so non-blocking/cloexec are set after creation.
        let fd = socket_with(domain, SocketType::DGRAM, SocketFlags::empty(), None)
            .map_err(|e| refused("socket(DGRAM)", e))?;
        rustix::io::fcntl_setfd(&fd, rustix::io::FdFlags::CLOEXEC)
            .map_err(|e| refused("fcntl(CLOEXEC)", e))?;
        rustix::io::ioctl_fionbio(&fd, true).map_err(|e| refused("ioctl(FIONBIO)", e))?;
        if family == Family::V6 {
            rustix::net::sockopt::set_ipv6_v6only(&fd, true)
                .map_err(|e| refused("setsockopt(IPV6_V6ONLY)", e))?;
        }
        set_dont_fragment(&fd, family)?;
        Ok(Socket { fd, family })
    }

    /// Sets the don't-fragment bit on every datagram the socket sends, and has the kernel leave the
    /// sizing to the transport (RFC 8899 §3: a packetization layer that probes the path needs its probes
    /// dropped, not fragmented, when too large; §4.4: a datagram larger than the local interface then
    /// fails its send with `EMSGSIZE`). Linux: `IP_PMTUDISC_PROBE` (and IPv6's `IPV6_PMTUDISC_PROBE`)
    /// sets DF and ignores the kernel's own path-MTU cache, so the transport's probes decide.
    #[cfg(target_os = "linux")]
    fn set_dont_fragment(fd: &OwnedFd, family: Family) -> Result<(), RtError> {
        use rustix::net::sockopt::{
            Ipv4PathMtuDiscovery, Ipv6PathMtuDiscovery, set_ip_mtu_discover, set_ipv6_mtu_discover,
        };
        match family {
            Family::V4 => set_ip_mtu_discover(fd, Ipv4PathMtuDiscovery::PROBE)
                .map_err(|e| refused("setsockopt(IP_MTU_DISCOVER)", e)),
            Family::V6 => set_ipv6_mtu_discover(fd, Ipv6PathMtuDiscovery::PROBE)
                .map_err(|e| refused("setsockopt(IPV6_MTU_DISCOVER)", e)),
        }
    }

    /// Format: `IPV6_DONTFRAG`, 62 in XNU's `bsd/netinet6/in6.h` (RFC 3542 §11.2), which libc 0.2.189
    /// does not declare for Apple targets; `dont_fragment` reads it back, so a wrong value fails the
    /// `a_socket_sends_with_dont_fragment` test.
    #[cfg(target_os = "macos")]
    const IPV6_DONTFRAG: libc::c_int = 62;

    /// The option that sets DF for `family` on macOS: `IP_DONTFRAG`, or `IPV6_DONTFRAG`.
    #[cfg(target_os = "macos")]
    fn dont_fragment_option(family: Family) -> (libc::c_int, libc::c_int) {
        match family {
            Family::V4 => (libc::IPPROTO_IP, libc::IP_DONTFRAG),
            Family::V6 => (libc::IPPROTO_IPV6, IPV6_DONTFRAG),
        }
    }

    /// macOS: `IP_DONTFRAG` / `IPV6_DONTFRAG` set DF on each datagram (see the Linux arm).
    #[cfg(target_os = "macos")]
    fn set_dont_fragment(fd: &OwnedFd, family: Family) -> Result<(), RtError> {
        let (level, option) = dont_fragment_option(family);
        let on: libc::c_int = 1;
        let len =
            libc::socklen_t::try_from(size_of::<libc::c_int>()).unwrap_or(libc::socklen_t::MAX);
        // SAFETY: the option takes an `int`; the pointer and length name one live local `c_int`, and the
        // descriptor is this socket's, open for the call.
        let outcome = unsafe {
            libc::setsockopt(
                fd.as_raw_fd(),
                level,
                option,
                (&raw const on).cast::<libc::c_void>(),
                len,
            )
        };
        if outcome == 0 {
            Ok(())
        } else {
            Err(RtError::os("setsockopt(IP_DONTFRAG)"))
        }
    }

    /// Whether the socket sends with the don't-fragment bit as [`set_dont_fragment`] set it: Linux reads
    /// the path-MTU discovery mode back as `PROBE`.
    #[cfg(target_os = "linux")]
    pub(crate) fn dont_fragment(socket: &Socket) -> Result<bool, RtError> {
        use rustix::net::sockopt::{
            Ipv4PathMtuDiscovery, Ipv6PathMtuDiscovery, ip_mtu_discover, ipv6_mtu_discover,
        };
        match socket.family {
            Family::V4 => ip_mtu_discover(&socket.fd)
                .map(|mode| mode == Ipv4PathMtuDiscovery::PROBE)
                .map_err(|e| refused("getsockopt(IP_MTU_DISCOVER)", e)),
            Family::V6 => ipv6_mtu_discover(&socket.fd)
                .map(|mode| mode == Ipv6PathMtuDiscovery::PROBE)
                .map_err(|e| refused("getsockopt(IPV6_MTU_DISCOVER)", e)),
        }
    }

    /// macOS: `IP_DONTFRAG` / `IPV6_DONTFRAG` read back.
    #[cfg(target_os = "macos")]
    pub(crate) fn dont_fragment(socket: &Socket) -> Result<bool, RtError> {
        let (level, option) = dont_fragment_option(socket.family);
        let mut set: libc::c_int = 0;
        let mut len =
            libc::socklen_t::try_from(size_of::<libc::c_int>()).unwrap_or(libc::socklen_t::MAX);
        // SAFETY: the option is an `int`; the out pointer and its length name one live local `c_int`, and
        // the descriptor is this socket's, open for the call.
        let outcome = unsafe {
            libc::getsockopt(
                socket.fd.as_raw_fd(),
                level,
                option,
                (&raw mut set).cast::<libc::c_void>(),
                &raw mut len,
            )
        };
        if outcome == 0 {
            Ok(set != 0)
        } else {
            Err(RtError::os("getsockopt(IP_DONTFRAG)"))
        }
    }

    pub(crate) fn bind(socket: &Socket, addr: SocketAddr) -> Result<(), RtError> {
        rx_bind(&socket.fd, &addr).map_err(|e| refused("bind", e))
    }

    /// An already-bound OS socket the caller hands over (the socket-activation shape): the port was never
    /// released between the caller's bind and this adoption, so nothing can take it in between. Refused
    /// unless it is a datagram socket bound to an internet address; made close-on-exec and non-blocking
    /// like one this seam created.
    pub(crate) fn adopt(fd: OwnedDatagram) -> Result<Socket, RtError> {
        let kind = rustix::net::sockopt::socket_type(&fd)
            .map_err(|e| refused("getsockopt(SO_TYPE)", e))?;
        if kind != SocketType::DGRAM {
            return Err(refused("adopt(SO_TYPE)", Errno::PROTOTYPE));
        }
        let family =
            match SocketAddr::try_from(getsockname(&fd).map_err(|e| refused("getsockname", e))?) {
                Ok(SocketAddr::V4(_)) => Family::V4,
                Ok(SocketAddr::V6(_)) => Family::V6,
                Err(_) => return Err(refused("adopt(family)", Errno::AFNOSUPPORT)),
            };
        rustix::io::fcntl_setfd(&fd, rustix::io::FdFlags::CLOEXEC)
            .map_err(|e| refused("fcntl(CLOEXEC)", e))?;
        rustix::io::ioctl_fionbio(&fd, true).map_err(|e| refused("ioctl(FIONBIO)", e))?;
        set_dont_fragment(&fd, family)?;
        Ok(Socket { fd, family })
    }

    /// The owned OS handle [`adopt`] takes: a file descriptor.
    pub(crate) type OwnedDatagram = OwnedFd;

    /// Gives up the socket's descriptor — the counterpart to [`adopt`], for a supervisor that binds a
    /// socket and hands it to the process it spawns.
    pub(crate) fn into_owned(socket: Socket) -> OwnedDatagram {
        socket.fd
    }

    pub(crate) fn local_addr(socket: &Socket) -> Result<SocketAddr, RtError> {
        SocketAddr::try_from(getsockname(&socket.fd).map_err(|e| refused("getsockname", e))?)
            .map_err(|_| refused("getsockname", Errno::AFNOSUPPORT))
    }

    pub(crate) fn recv_from(
        socket: &Socket,
        buf: &mut [u8],
    ) -> Result<Io<(usize, SocketAddr)>, RtError> {
        match recvfrom(&socket.fd, buf, RecvFlags::empty()) {
            Ok((n, _flags, Some(from))) => match SocketAddr::try_from(from) {
                Ok(from) => Ok(Io::Ready((n, from))),
                Err(_) => Err(refused("recvfrom", Errno::AFNOSUPPORT)),
            },
            Ok((n, _flags, None)) => Ok(Io::Ready((
                n,
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            ))),
            Err(e) => classify("recvfrom", e),
        }
    }

    pub(crate) fn send_to(
        socket: &Socket,
        buf: &[u8],
        addr: SocketAddr,
    ) -> Result<Io<usize>, RtError> {
        match rx_sendto(&socket.fd, buf, SendFlags::empty(), &addr) {
            Ok(n) => Ok(Io::Ready(n)),
            Err(e) => classify("sendto", e),
        }
    }
}

// ============================================================================== Windows (Winsock 2)

#[cfg(windows)]
pub(crate) mod imp {
    use std::os::windows::io::IntoRawSocket;
    use std::sync::OnceLock;

    use windows_sys::Win32::Networking::WinSock::{
        AF_INET, AF_INET6, FIONBIO, IN_ADDR, IN_ADDR_0, IN6_ADDR, IN6_ADDR_0, INVALID_SOCKET,
        IP_DONTFRAGMENT, IPPROTO_IP, IPPROTO_IPV6, IPPROTO_UDP, IPV6_DONTFRAG, IPV6_V6ONLY,
        SO_RCVBUF, SO_TYPE, SOCK_DGRAM, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_IN6_0,
        SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, SOL_SOCKET, WSADATA, WSAECONNREFUSED,
        WSAECONNRESET, WSAEHOSTUNREACH, WSAEINTR, WSAEMSGSIZE, WSAENETRESET, WSAENETUNREACH,
        WSAEWOULDBLOCK, WSAGetLastError, WSAStartup, bind as ws_bind, closesocket, getsockname,
        getsockopt, ioctlsocket, recvfrom as ws_recvfrom, sendto as ws_sendto, setsockopt,
        socket as ws_socket,
    };

    use core::net::{Ipv6Addr, SocketAddrV6};

    use super::{Family, Io, Ipv4Addr, SocketAddr, SocketAddrV4};
    use crate::error::RtError;

    /// A non-blocking Winsock UDP socket owned here (closed on drop), and its family. Held as the raw
    /// `SOCKET`; the drop closes it once. Not `Copy`, so ownership is single.
    #[derive(Debug)]
    pub(crate) struct Socket {
        socket: SOCKET,
        family: Family,
    }

    impl Drop for Socket {
        fn drop(&mut self) {
            // SAFETY: our socket, created by `socket()`; closed exactly once here.
            unsafe { closesocket(self.socket) };
        }
    }

    /// Winsock must be initialized once per process before any socket call; `WSAStartup(2.2)` and no
    /// matching cleanup (process-lifetime) means one success suffices. A `OnceLock` makes it
    /// exactly-once with no lock on the data path.
    pub(crate) fn ensure_started() -> Result<(), RtError> {
        /// Format: the Winsock version to request in `WSAStartup` — 2.2, low byte major, high byte minor
        /// (`MAKEWORD(2, 2)` = `0x0202`), the version every current Windows provides.
        const WINSOCK_VERSION_2_2: u16 = 0x0202;
        static STARTED: OnceLock<bool> = OnceLock::new();
        let ok = *STARTED.get_or_init(|| {
            // SAFETY: an all-zero WSADATA is a valid, uninitialized out-param.
            let mut data: WSADATA = unsafe { std::mem::zeroed() };
            // SAFETY: `data` is a live, writable WSADATA; `WSAStartup` fills it and returns 0 on success.
            unsafe { WSAStartup(WINSOCK_VERSION_2_2, &mut data) == 0 }
        });
        if ok {
            Ok(())
        } else {
            Err(RtError::os("WSAStartup"))
        }
    }

    /// The last Winsock error as an `RtError`, carrying the `WSAGetLastError` code (the shape
    /// `RtError::os` gives on Unix).
    pub(crate) fn last(call: &'static str) -> RtError {
        RtError::DriverRefused {
            call,
            // SAFETY: a pure query of thread-local last-error state.
            code: Some(unsafe { WSAGetLastError() }),
        }
    }

    /// The `Io` the last failed non-blocking call maps to, or the refusal it is.
    fn classify<T>(call: &'static str) -> Result<Io<T>, RtError> {
        // SAFETY: a pure query of thread-local last-error state.
        let code = unsafe { WSAGetLastError() };
        match code {
            WSAEWOULDBLOCK => Ok(Io::WouldBlock),
            WSAEINTR => Ok(Io::Interrupted),
            WSAECONNRESET | WSAENETRESET | WSAECONNREFUSED | WSAEHOSTUNREACH | WSAENETUNREACH => {
                Ok(Io::Astray)
            }
            _ => Err(RtError::DriverRefused {
                call,
                code: Some(code),
            }),
        }
    }

    impl Socket {
        /// The readiness handle a `crate::readiness` future registers. A Winsock `SOCKET` is pointer-width
        /// but a kernel handle-table value that fits in a positive `i32` in practice; the readiness seam
        /// (and the AFD reactor that reconstructs it) carry it as that `i32`, the width a Unix fd uses.
        pub(crate) fn raw_id(&self) -> i32 {
            // The low 32 bits of the socket, reinterpreted as `i32` bit-for-bit — a checked narrowing (the
            // socket fits) then a bit-preserving reinterpret, so the AFD reactor's `raw as u32 as SOCKET`
            // reconstructs the same handle. No lossy `as` cast.
            let low = u32::try_from(self.socket).unwrap_or(u32::MAX);
            i32::from_ne_bytes(low.to_ne_bytes())
        }
    }

    /// A non-blocking datagram socket of `family`, IPv6 only when IPv6, sending with the don't-fragment
    /// bit.
    pub(crate) fn dgram_socket(family: Family) -> Result<Socket, RtError> {
        ensure_started()?;
        let domain = match family {
            Family::V4 => AF_INET,
            Family::V6 => AF_INET6,
        };
        // SAFETY: a plain socket creation; the result is checked against INVALID_SOCKET.
        let raw = unsafe { ws_socket(i32::from(domain), SOCK_DGRAM, IPPROTO_UDP) };
        if raw == INVALID_SOCKET {
            return Err(last("socket(DGRAM)"));
        }
        let socket = Socket {
            socket: raw,
            family,
        };
        set_nonblocking(socket.socket)?;
        if family == Family::V6 {
            set_int(
                socket.socket,
                IPPROTO_IPV6,
                IPV6_V6ONLY,
                1,
                "setsockopt(IPV6_V6ONLY)",
            )?;
        }
        set_dont_fragment(&socket)?;
        Ok(socket)
    }

    /// Puts `socket` in non-blocking mode (`FIONBIO`), as every socket hyper-rt drives must be.
    pub(crate) fn set_nonblocking(socket: SOCKET) -> Result<(), RtError> {
        let mut nonblocking: u32 = 1;
        // SAFETY: FIONBIO takes one u32 by pointer; a live local suffices.
        if unsafe { ioctlsocket(socket, FIONBIO, &mut nonblocking) } == SOCKET_ERROR {
            return Err(last("ioctlsocket(FIONBIO)"));
        }
        Ok(())
    }

    /// Sets one `int`-valued option of `socket` to `value`; `call` names it in a refusal.
    pub(crate) fn set_int(
        socket: SOCKET,
        level: i32,
        option: i32,
        value: i32,
        call: &'static str,
    ) -> Result<(), RtError> {
        let len = i32::try_from(std::mem::size_of::<i32>()).unwrap_or(i32::MAX);
        // SAFETY: the option takes a DWORD-sized `int`; the pointer and length name one live local.
        let outcome =
            unsafe { setsockopt(socket, level, option, (&raw const value).cast::<u8>(), len) };
        if outcome == SOCKET_ERROR {
            return Err(last(call));
        }
        Ok(())
    }

    /// Sets the don't-fragment bit on every datagram the socket sends (`IP_DONTFRAGMENT`,
    /// `IPV6_DONTFRAG`; RFC 8899 §3 — see the Unix arms), so a path-MTU probe that is too large is
    /// dropped rather than fragmented.
    fn set_dont_fragment(socket: &Socket) -> Result<(), RtError> {
        match socket.family {
            Family::V4 => set_int(
                socket.socket,
                IPPROTO_IP,
                IP_DONTFRAGMENT,
                1,
                "setsockopt(IP_DONTFRAGMENT)",
            ),
            Family::V6 => set_int(
                socket.socket,
                IPPROTO_IPV6,
                IPV6_DONTFRAG,
                1,
                "setsockopt(IPV6_DONTFRAG)",
            ),
        }
    }

    /// One `int`-valued option of `socket` (`SO_RCVBUF`, `SO_TYPE`, `SO_ERROR`); `call` names the query in a
    /// refusal.
    pub(crate) fn int_option(
        socket: SOCKET,
        level: i32,
        option: i32,
        call: &'static str,
    ) -> Result<i32, RtError> {
        let mut value: i32 = 0;
        // The option is an `int`: its length is that type's size, which always fits an `i32`.
        let mut len: i32 = i32::try_from(std::mem::size_of::<i32>()).unwrap_or(i32::MAX);
        // SAFETY: the option is an `int`; the out pointer and its length name one live local `i32`.
        let outcome = unsafe {
            getsockopt(
                socket,
                level,
                option,
                (&raw mut value).cast::<u8>(),
                &raw mut len,
            )
        };
        if outcome == SOCKET_ERROR {
            return Err(last(call));
        }
        Ok(value)
    }

    /// The socket's kernel receive buffer (`SO_RCVBUF`) in bytes.
    pub(crate) fn recv_buffer_bytes(socket: &Socket) -> Result<usize, RtError> {
        let bytes = int_option(
            socket.socket,
            SOL_SOCKET,
            SO_RCVBUF,
            "getsockopt(SO_RCVBUF)",
        )?;
        usize::try_from(bytes).map_err(|_| last("getsockopt(SO_RCVBUF)"))
    }

    /// The length of a socket address structure, as Winsock takes it.
    pub(crate) fn length<T>() -> i32 {
        i32::try_from(size_of::<T>()).unwrap_or(0)
    }

    /// `addr` written into a `SOCKADDR_STORAGE` (network byte order for the port and address, as the wire
    /// wants), and its length.
    pub(crate) fn sockaddr(addr: SocketAddr) -> (SOCKADDR_STORAGE, i32) {
        // SAFETY: `SOCKADDR_STORAGE` is plain integers; all zeroes is a valid value.
        let mut storage: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
        match addr {
            SocketAddr::V4(v4) => {
                let name = SOCKADDR_IN {
                    sin_family: AF_INET,
                    sin_port: v4.port().to_be(),
                    sin_addr: IN_ADDR {
                        S_un: IN_ADDR_0 {
                            // The octets in memory order [a, b, c, d] are already network order.
                            S_addr: u32::from_ne_bytes(v4.ip().octets()),
                        },
                    },
                    sin_zero: [0; 8],
                };
                // SAFETY: `SOCKADDR_STORAGE` is large and aligned enough for any address family
                // (ws2def.h), `SOCKADDR_IN` among them; one unaligned-tolerant write of the whole value.
                unsafe {
                    std::ptr::write(std::ptr::from_mut(&mut storage).cast::<SOCKADDR_IN>(), name);
                }
                (storage, length::<SOCKADDR_IN>())
            }
            SocketAddr::V6(v6) => {
                let name = SOCKADDR_IN6 {
                    sin6_family: AF_INET6,
                    sin6_port: v6.port().to_be(),
                    sin6_flowinfo: v6.flowinfo(),
                    sin6_addr: IN6_ADDR {
                        u: IN6_ADDR_0 {
                            Byte: v6.ip().octets(),
                        },
                    },
                    Anonymous: SOCKADDR_IN6_0 {
                        sin6_scope_id: v6.scope_id(),
                    },
                };
                // SAFETY: as above, for `SOCKADDR_IN6`.
                unsafe {
                    std::ptr::write(
                        std::ptr::from_mut(&mut storage).cast::<SOCKADDR_IN6>(),
                        name,
                    );
                }
                (storage, length::<SOCKADDR_IN6>())
            }
        }
    }

    /// The address a filled `SOCKADDR_STORAGE` names, if it is an internet address (the inverse of
    /// [`sockaddr`]).
    pub(crate) fn from_sockaddr(storage: &SOCKADDR_STORAGE) -> Option<SocketAddr> {
        match storage.ss_family {
            AF_INET => {
                // SAFETY: the family says Winsock wrote a `SOCKADDR_IN`, which the storage holds.
                let name = unsafe { &*std::ptr::from_ref(storage).cast::<SOCKADDR_IN>() };
                // SAFETY: reading the `S_addr` arm of the address union — a plain `u32`, always initialized.
                let octets = unsafe { name.sin_addr.S_un.S_addr }.to_ne_bytes();
                Some(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::from(octets),
                    u16::from_be(name.sin_port),
                )))
            }
            AF_INET6 => {
                // SAFETY: as above, for `SOCKADDR_IN6`.
                let name = unsafe { &*std::ptr::from_ref(storage).cast::<SOCKADDR_IN6>() };
                // SAFETY: the `Byte` and `sin6_scope_id` arms of their unions are plain integers, always
                // initialized.
                let (octets, scope) =
                    unsafe { (name.sin6_addr.u.Byte, name.Anonymous.sin6_scope_id) };
                Some(SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(octets),
                    u16::from_be(name.sin6_port),
                    name.sin6_flowinfo,
                    scope,
                )))
            }
            _ => None,
        }
    }

    /// An already-bound OS socket the caller hands over (the socket-activation shape): the port was never
    /// released between the caller's bind and this adoption, so nothing can take it in between. Refused
    /// unless it is a datagram socket bound to an internet address; made non-blocking like one this seam
    /// created.
    pub(crate) fn adopt(owned: OwnedDatagram) -> Result<Socket, RtError> {
        ensure_started()?;
        let Ok(raw) = SOCKET::try_from(owned.into_raw_socket()) else {
            return Err(RtError::DriverRefused {
                call: "adopt(SOCKET)",
                code: None,
            });
        };
        // Owned from here: the drop closes it once, on every refusal below too. The family is read from
        // the bound address before anything family-specific is set.
        let mut socket = Socket {
            socket: raw,
            family: Family::V4,
        };
        let kind = int_option(socket.socket, SOL_SOCKET, SO_TYPE, "getsockopt(SO_TYPE)")?;
        if kind != SOCK_DGRAM {
            return Err(RtError::DriverRefused {
                call: "adopt(SO_TYPE)",
                code: Some(kind),
            });
        }
        socket.family = Family::of(local_addr(&socket)?);
        set_nonblocking(socket.socket)?;
        set_dont_fragment(&socket)?;
        Ok(socket)
    }

    /// The owned OS handle [`adopt`] takes: a Winsock socket.
    pub(crate) type OwnedDatagram = std::os::windows::io::OwnedSocket;

    /// Gives up the socket — the counterpart to [`adopt`], for a supervisor that binds a socket and hands
    /// it to the process it spawns.
    pub(crate) fn into_owned(socket: Socket) -> OwnedDatagram {
        use std::os::windows::io::{FromRawSocket, RawSocket};
        // The drop must not close what is handed over: the handle moves to the `OwnedSocket` instead.
        let socket = std::mem::ManuallyDrop::new(socket);
        let raw = RawSocket::try_from(socket.socket).unwrap_or(RawSocket::MAX);
        // SAFETY: `raw` is this seam's open socket, owned by `socket`, whose drop is suppressed above, so
        // the `OwnedSocket` becomes its one owner and closes it once. A `SOCKET` is pointer-width and a
        // `RawSocket` 64 bits, so the conversion is exact on every Windows target.
        unsafe { OwnedDatagram::from_raw_socket(raw) }
    }

    pub(crate) fn bind(socket: &Socket, addr: SocketAddr) -> Result<(), RtError> {
        let (sa, len) = sockaddr(addr);
        // SAFETY: `sa` is a live socket address of `len` bytes, passed as the generic SOCKADDR.
        let rc = unsafe {
            ws_bind(
                socket.socket,
                std::ptr::addr_of!(sa).cast::<SOCKADDR>(),
                len,
            )
        };
        if rc == SOCKET_ERROR {
            Err(last("bind"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn local_addr(socket: &Socket) -> Result<SocketAddr, RtError> {
        // SAFETY: an all-zero SOCKADDR_STORAGE is a valid empty address getsockname fills.
        let mut sa: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
        let mut len = length::<SOCKADDR_STORAGE>();
        // SAFETY: getsockname writes up to `len` bytes into `sa` and the actual length back into `len`.
        let rc = unsafe {
            getsockname(
                socket.socket,
                std::ptr::addr_of_mut!(sa).cast::<SOCKADDR>(),
                &mut len,
            )
        };
        if rc == SOCKET_ERROR {
            return Err(last("getsockname"));
        }
        from_sockaddr(&sa).ok_or(RtError::DriverRefused {
            call: "getsockname(family)",
            code: None,
        })
    }

    pub(crate) fn recv_from(
        socket: &Socket,
        buf: &mut [u8],
    ) -> Result<Io<(usize, SocketAddr)>, RtError> {
        let len = i32::try_from(buf.len()).unwrap_or(i32::MAX);
        // SAFETY: an all-zero SOCKADDR_STORAGE is a valid empty address recvfrom fills.
        let mut from: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
        let mut from_len = length::<SOCKADDR_STORAGE>();
        // SAFETY: recvfrom writes up to `len` bytes into `buf` and the sender into `from`/`from_len`.
        let rc = unsafe {
            ws_recvfrom(
                socket.socket,
                buf.as_mut_ptr(),
                len,
                0,
                std::ptr::addr_of_mut!(from).cast::<SOCKADDR>(),
                &mut from_len,
            )
        };
        if rc == SOCKET_ERROR {
            // A datagram longer than `buf`: Winsock fills the buffer and reports WSAEMSGSIZE, where Unix
            // truncates silently; it is the same truncation, as the socket's documentation states (mantle's
            // review, finding 9).
            // SAFETY: a pure query of thread-local last-error state.
            if unsafe { WSAGetLastError() } == WSAEMSGSIZE {
                let unspecified = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
                return Ok(Io::Ready((
                    buf.len(),
                    from_sockaddr(&from).unwrap_or(unspecified),
                )));
            }
            return classify("recvfrom");
        }
        let unspecified = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
        Ok(Io::Ready((
            usize::try_from(rc).unwrap_or(0),
            from_sockaddr(&from).unwrap_or(unspecified),
        )))
    }

    pub(crate) fn send_to(
        socket: &Socket,
        buf: &[u8],
        addr: SocketAddr,
    ) -> Result<Io<usize>, RtError> {
        let (sa, sa_len) = sockaddr(addr);
        let len = i32::try_from(buf.len()).unwrap_or(i32::MAX);
        // SAFETY: sendto reads `len` bytes from `buf` and the destination from `sa`.
        let rc = unsafe {
            ws_sendto(
                socket.socket,
                buf.as_ptr(),
                len,
                0,
                std::ptr::addr_of!(sa).cast::<SOCKADDR>(),
                sa_len,
            )
        };
        if rc == SOCKET_ERROR {
            return classify("sendto");
        }
        Ok(Io::Ready(usize::try_from(rc).unwrap_or(0)))
    }
}

/// The Winsock helpers the stream seam (`crate::tcpsys`) shares.
#[cfg(windows)]
pub(crate) use imp as winsock;
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) use imp::dont_fragment;
pub(crate) use imp::{OwnedDatagram, Socket};
pub(crate) use imp::{
    adopt, bind, dgram_socket, into_owned, local_addr, recv_buffer_bytes, recv_from, send_to,
};
