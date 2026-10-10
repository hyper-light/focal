//! macOS: the kernel's receive timestamp and the clock it is on, which neither std nor rustix
//! exposes (rustix's `recvmsg` drops the timestamp control message).
//!
//! - `SO_TIMESTAMP_MONOTONIC` (<sys/socket.h>): XNU attaches `mach_absolute_time()` when UDP input
//!   queues the datagram on the socket (`bsd/netinet/ip_input.c`, `ip_savecontrol`, called from
//!   `udp_input`), as an `SCM_TIMESTAMP_MONOTONIC` control message carrying a `uint64_t`. Read with
//!   `recvmsg(2)` and the `CMSG_*` walk of cmsg(3), through libc, whose declarations its `ctest` suite
//!   checks against the headers; only `SCM_TIMESTAMP_MONOTONIC` is declared here, because libc 0.2.189
//!   does not, and the stamp test fails on a wrong value.
//! - `mach_absolute_time` scaled by `mach_timebase_info` (<mach/mach_time.h>): the clock the stamps are
//!   on, `CLOCK_UPTIME_RAW`, which stops while the host sleeps. The shard's clock (`CLOCK_MONOTONIC`)
//!   does not, so a stamp is carried onto it by its age, read on the stamps' clock (`super::batched`).
//!
//! Moved here from hyper-tokio's `sys/macos.rs` so both drivers share one layer (docs/runtime.md §5.1).

#![allow(unsafe_code)]

use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::RawFd;
use std::ptr;

/// `SCM_TIMESTAMP_MONOTONIC`, 0x04 in XNU's `bsd/sys/socket.h`, which libc 0.2.189 does not
/// declare.
const SCM_TIMESTAMP_MONOTONIC: libc::c_int = 0x04;

/// The control buffer, in 8-byte words for `cmsghdr`'s alignment: `CMSG_SPACE(sizeof(uint64_t))`,
/// 12 bytes of header rounded to 4-byte alignment and 8 of stamp, 20, in three words.
const CONTROL_WORDS: usize = 3;

/// The stamps' clock, `mach_absolute_time`, as nanoseconds.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Clock {
    /// `mach_timebase_info`: nanoseconds are ticks × `numer` / `denom`; `denom` is never zero.
    numer: u64,
    denom: u64,
}

impl Clock {
    #[expect(
        deprecated,
        reason = "libc deprecates its Mach declarations in favour of the mach2 crate, which is not \
                  a dependency; the declarations themselves are unchanged in libc"
    )]
    pub(crate) fn new() -> io::Result<Self> {
        let mut info = libc::mach_timebase_info { numer: 0, denom: 0 };
        // SAFETY: `info` is a live, writable `mach_timebase_info` for the call, which writes that
        // one structure (<mach/mach_time.h>).
        let status = unsafe { libc::mach_timebase_info(&raw mut info) };
        if status != libc::KERN_SUCCESS || info.denom == 0 {
            return Err(io::Error::other("mach_timebase_info failed"));
        }
        Ok(Self {
            numer: u64::from(info.numer),
            denom: u64::from(info.denom),
        })
    }

    /// Now, in ticks: the clock a stamp's age is read on.
    #[expect(deprecated, reason = "as in `new`")]
    pub(crate) fn now_ticks(&self) -> u64 {
        // SAFETY: takes no argument and reads the timebase register (<mach/mach_time.h>).
        unsafe { libc::mach_absolute_time() }
    }

    /// `ticks` × `numer` / `denom`, in 128 bits so it cannot overflow before the division.
    pub(crate) fn ticks_ns(&self, ticks: u64) -> u64 {
        let ns = u128::from(ticks)
            .checked_mul(u128::from(self.numer))
            .and_then(|scaled| scaled.checked_div(u128::from(self.denom)))
            .unwrap_or(u128::MAX);
        u64::try_from(ns).unwrap_or(u64::MAX)
    }
}

/// Asks the kernel to stamp each datagram `fd` receives. Whether it agreed.
pub(crate) fn enable_stamps(fd: RawFd) -> bool {
    let on: libc::c_int = 1;
    let length = libc::socklen_t::try_from(size_of::<libc::c_int>()).unwrap_or(0);
    // SAFETY: `on` is a live `c_int` and `length` its size, which bounds what setsockopt reads.
    let status = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMP_MONOTONIC,
            ptr::addr_of!(on).cast(),
            length,
        )
    };
    status == 0
}

/// One datagram received: who sent it, how long it is, and the kernel's stamp in
/// `mach_absolute_time` ticks, when there is one.
pub(crate) struct Received {
    pub(crate) from: SocketAddr,
    pub(crate) length: usize,
    pub(crate) stamp: Option<u64>,
}

/// Receives one datagram into `buffer` without blocking (the socket is non-blocking). A datagram
/// from an address that is not an internet one, or one the buffer truncated, is `None`: the
/// protocols above treat it as lost.
pub(crate) fn receive(fd: RawFd, buffer: &mut [u8]) -> io::Result<Option<Received>> {
    // SAFETY: `sockaddr_storage` is plain integers; all zeroes is a valid value.
    let mut name: libc::sockaddr_storage = unsafe { zeroed() };
    let mut control = [0u64; CONTROL_WORDS];
    let mut iov = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    // SAFETY: an all-zero `msghdr` is valid: null pointers with zero lengths.
    let mut message: libc::msghdr = unsafe { zeroed() };
    message.msg_name = ptr::addr_of_mut!(name).cast();
    message.msg_namelen =
        libc::socklen_t::try_from(size_of::<libc::sockaddr_storage>()).unwrap_or(0);
    message.msg_iov = &raw mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen =
        libc::socklen_t::try_from(size_of::<[u64; CONTROL_WORDS]>()).unwrap_or(0);
    // SAFETY: `message` points at `name`, `iov` (which covers `buffer`) and `control`, all live
    // and writable for the call, each with its length; recvmsg writes within them.
    let read = unsafe { libc::recvmsg(fd, &raw mut message, libc::MSG_DONTWAIT) };
    let Ok(length) = usize::try_from(read) else {
        return Err(io::Error::last_os_error());
    };
    if message.msg_flags & libc::MSG_TRUNC != 0 {
        return Ok(None);
    }
    let Some(from) = read_name(&name) else {
        return Ok(None);
    };
    Ok(Some(Received {
        from,
        length,
        stamp: stamp_of(&message),
    }))
}

/// The address the kernel wrote into `storage`, if it is an internet address.
fn read_name(storage: &libc::sockaddr_storage) -> Option<SocketAddr> {
    match libc::c_int::from(storage.ss_family) {
        libc::AF_INET => {
            // SAFETY: the family says the kernel wrote a `sockaddr_in`, which the storage holds.
            let name = unsafe { &*ptr::from_ref(storage).cast::<libc::sockaddr_in>() };
            Some(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(name.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(name.sin_port),
            )))
        }
        libc::AF_INET6 => {
            // SAFETY: as above, for `sockaddr_in6`.
            let name = unsafe { &*ptr::from_ref(storage).cast::<libc::sockaddr_in6>() };
            Some(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(name.sin6_addr.s6_addr),
                u16::from_be(name.sin6_port),
                name.sin6_flowinfo,
                name.sin6_scope_id,
            )))
        }
        _ => None,
    }
}

/// The `SCM_TIMESTAMP_MONOTONIC` stamp among the control messages recvmsg left in `message`;
/// `None` when the kernel attached none or truncated it.
fn stamp_of(message: &libc::msghdr) -> Option<u64> {
    let whole = usize::try_from(
        // SAFETY: `CMSG_LEN` is arithmetic on its argument and reads no memory.
        unsafe { libc::CMSG_LEN(u32::try_from(size_of::<u64>()).unwrap_or(u32::MAX)) },
    )
    .ok()?;
    // SAFETY: `message` was filled by recvmsg, whose control buffer and length are its own;
    // CMSG_FIRSTHDR and CMSG_NXTHDR return headers that lie whole within `msg_controllen`, or null
    // (cmsg(3)), and a stamp message whose `cmsg_len` covers the stamp has it at CMSG_DATA, read
    // unaligned.
    unsafe {
        let mut at = libc::CMSG_FIRSTHDR(message);
        while let Some(header) = at.as_ref() {
            if header.cmsg_level == libc::SOL_SOCKET
                && header.cmsg_type == SCM_TIMESTAMP_MONOTONIC
                && usize::try_from(header.cmsg_len).is_ok_and(|length| length >= whole)
            {
                return Some(ptr::read_unaligned(libc::CMSG_DATA(at).cast::<u64>()));
            }
            at = libc::CMSG_NXTHDR(message, at);
        }
    }
    None
}
