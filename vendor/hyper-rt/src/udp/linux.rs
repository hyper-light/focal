//! Linux: many datagrams per system call, `sendmmsg(2)` and `recvmmsg(2)`, and the kernel's UDP
//! offloads where it has them (`udp(7)`): generic segmentation (`UDP_SEGMENT`, Linux 4.18), one
//! send of equal-sized datagrams to one destination that the kernel or the NIC cuts, and generic
//! receive (`UDP_GRO`, Linux 5.0), consecutive datagrams of one flow delivered as one buffer with
//! their size in a control message. The kernel's receive timestamp, `SO_TIMESTAMPNS` (socket(7)):
//! an `SCM_TIMESTAMPNS` control message with a `struct timespec` of `CLOCK_REALTIME` taken when the
//! datagram was received, carried onto the shard's clock by its age (`super::batched`). Neither
//! rustix 1.1 nor std exposes these: rustix lists `recvmmsg` as not yet implemented, has no
//! `UDP_SEGMENT` or `UDP_GRO` option, and its ancillary buffers carry neither control message.
//! Moved here from hyper-tokio's `sys/linux.rs` so both drivers share one layer (docs/runtime.md §5.1).

#![allow(unsafe_code)]

use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::RawFd;
use std::ptr;

use super::batched::Slot;

/// `UDP_MAX_SEGMENTS` (`include/linux/udp.h`): the most datagrams one segmented send may carry,
/// 64 since segmentation was introduced in Linux 4.18. A later kernel's larger bound is not
/// assumed.
pub(crate) const MAX_SEGMENTS: usize = 64;
/// The most bytes one segmented send may carry: the largest IPv4 datagram (RFC 791, a 16-bit
/// total length) less its 20-byte header and the 8-byte UDP header (RFC 768); the kernel refuses
/// a larger send with `EMSGSIZE`. IPv6's bound is larger, so this one holds for both.
pub(crate) const SEGMENTED_BYTES: usize = 65_507;
/// The control buffer of one message, in 8-byte words for `cmsghdr`'s alignment: room for one
/// `UDP_SEGMENT` (a `u16`) or one `UDP_GRO` (an `int`) beside one `SCM_TIMESTAMPNS` (a
/// `struct timespec`): `CMSG_SPACE(4) + CMSG_SPACE(16)`, 24 + 32 = 56 bytes on a 64-bit target
/// (a 16-byte header) and less on a 32-bit one, seven words (`the_control_buffer_holds_both`).
const CONTROL_WORDS: usize = 7;

type Control = [u64; CONTROL_WORDS];

/// Nanoseconds in a second, the unit of `struct timespec`'s `tv_nsec` (POSIX <time.h>).
const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Nanoseconds of a `struct timespec`, `None` when negative or past `u64`.
fn timespec_ns(time: &libc::timespec) -> Option<u64> {
    u64::try_from(time.tv_sec)
        .ok()?
        .checked_mul(NANOS_PER_SECOND)?
        .checked_add(u64::try_from(time.tv_nsec).ok()?)
}

/// `clock_gettime(clock)` in nanoseconds.
fn clock_ns(clock: libc::clockid_t) -> io::Result<u64> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` is a live, writable `timespec` for the call, which writes that one structure
    // (clock_gettime(2)).
    let status = unsafe { libc::clock_gettime(clock, &raw mut time) };
    if status != 0 {
        return Err(io::Error::last_os_error());
    }
    timespec_ns(&time).ok_or_else(|| io::Error::other("clock out of range"))
}

/// `CLOCK_REALTIME`, nanoseconds: the clock `SO_TIMESTAMPNS` stamps on.
pub(crate) fn realtime_ns() -> io::Result<u64> {
    clock_ns(libc::CLOCK_REALTIME)
}

/// Asks the kernel to stamp each datagram `fd` receives (`SO_TIMESTAMPNS`). Whether it agreed.
pub(crate) fn enable_stamps(fd: RawFd) -> bool {
    let on: libc::c_int = 1;
    // SAFETY: `on` is a live `c_int` and the length passed is its size; setsockopt only reads it.
    let status = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            ptr::addr_of!(on).cast(),
            socklen(size_of::<libc::c_int>()),
        )
    };
    status == 0
}

/// What the kernel offers this socket.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Offload {
    /// Segmented sends (`UDP_SEGMENT`).
    pub(crate) gso: bool,
    /// Coalesced receives (`UDP_GRO`), switched on.
    pub(crate) gro: bool,
}

/// Asks the kernel for the offloads, switching on coalesced receives. A kernel without them
/// refuses the option, and the socket sends and receives one datagram a message.
pub(crate) fn offload(fd: RawFd) -> Offload {
    let mut value: libc::c_int = 0;
    let mut length = socklen(size_of::<libc::c_int>());
    // SAFETY: `value` and `length` are live locals of the sizes passed; getsockopt writes at most
    // `length` bytes into `value` and the new length into `length`.
    let gso = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_UDP,
            libc::UDP_SEGMENT,
            ptr::addr_of_mut!(value).cast(),
            &raw mut length,
        )
    } == 0;
    let on: libc::c_int = 1;
    // SAFETY: `on` is a live `c_int` and the length passed is its size; setsockopt only reads it.
    let gro = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_UDP,
            libc::UDP_GRO,
            ptr::addr_of!(on).cast(),
            socklen(size_of::<libc::c_int>()),
        )
    } == 0;
    Offload { gso, gro }
}

/// A length in the type the C library gives the field: `size_t` in glibc's `msghdr`, `int` or
/// `socklen_t` in musl's.
fn field<T: TryFrom<usize> + Default>(value: usize) -> T {
    T::try_from(value).unwrap_or_default()
}

fn socklen(bytes: usize) -> libc::socklen_t {
    libc::socklen_t::try_from(bytes).unwrap_or(libc::socklen_t::MAX)
}

/// The headers one `sendmmsg` or `recvmmsg` call is given, kept between calls so that a call
/// allocates nothing.
pub(crate) struct Headers {
    headers: Vec<libc::mmsghdr>,
    iovecs: Vec<libc::iovec>,
    names: Vec<libc::sockaddr_storage>,
    controls: Vec<Control>,
    /// How many slots each message of a send carries.
    groups: Vec<usize>,
}

// SAFETY: the raw pointers in `headers` and `iovecs` are written by `send` and `receive` from
// borrows that live for the call, and are read only by the system call inside it; between calls
// they are stale and never dereferenced. Moving the vectors to another thread moves nothing they
// point to that anyone reads.
unsafe impl Send for Headers {}

impl Headers {
    /// Headers for up to `batch` messages.
    pub(crate) fn new(batch: usize) -> Self {
        Self {
            headers: Vec::with_capacity(batch),
            iovecs: Vec::with_capacity(batch),
            // SAFETY: `sockaddr_storage` is plain integers; all zeroes is a valid value.
            names: vec![unsafe { zeroed::<libc::sockaddr_storage>() }; batch],
            controls: vec![[0; CONTROL_WORDS]; batch],
            groups: Vec::with_capacity(batch),
        }
    }
}

/// Writes `address` into `storage`; returns its length.
fn write_name(address: SocketAddr, storage: &mut libc::sockaddr_storage) -> libc::socklen_t {
    match address {
        SocketAddr::V4(v4) => {
            // SAFETY: `sockaddr_storage` is large and aligned enough for any address family,
            // `sockaddr_in` among them (socket(7)); the cast reference lives within the borrow.
            let name = unsafe { &mut *ptr::from_mut(storage).cast::<libc::sockaddr_in>() };
            name.sin_family = libc::sa_family_t::try_from(libc::AF_INET).unwrap_or(0);
            name.sin_port = v4.port().to_be();
            name.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(v4.ip().octets()),
            };
            socklen(size_of::<libc::sockaddr_in>())
        }
        SocketAddr::V6(v6) => {
            // SAFETY: as above, for `sockaddr_in6`.
            let name = unsafe { &mut *ptr::from_mut(storage).cast::<libc::sockaddr_in6>() };
            name.sin6_family = libc::sa_family_t::try_from(libc::AF_INET6).unwrap_or(0);
            name.sin6_port = v6.port().to_be();
            name.sin6_flowinfo = v6.flowinfo();
            name.sin6_addr = libc::in6_addr {
                s6_addr: v6.ip().octets(),
            };
            name.sin6_scope_id = v6.scope_id();
            socklen(size_of::<libc::sockaddr_in6>())
        }
    }
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

/// How many of `slots`, from the first, one segmented send carries: consecutive datagrams to one
/// destination, all as long as the first but the last, which may be shorter (`udp(7)`,
/// `UDP_SEGMENT`), within the kernel's segment and byte bounds.
fn group(slots: &[Slot], gso: bool) -> usize {
    let Some(first) = slots.first() else {
        return 0;
    };
    if !gso {
        return 1;
    }
    let size = first.bytes.len();
    let mut count = 1;
    let mut total = size;
    for slot in slots.iter().skip(1) {
        let length = slot.bytes.len();
        let fits = total
            .checked_add(length)
            .filter(|sum| *sum <= SEGMENTED_BYTES);
        if slot.to != first.to || length > size || count >= MAX_SEGMENTS {
            break;
        }
        let Some(sum) = fits else {
            break;
        };
        count = count.saturating_add(1);
        total = sum;
        if length < size {
            break;
        }
    }
    count
}

/// A failed send: the error, and how many slots the message it failed on carried.
pub(crate) struct SendFailed {
    pub(crate) error: io::Error,
    pub(crate) slots: usize,
}

/// Sends `slots` in as few messages as the offloads allow, with one `sendmmsg`; returns how many
/// slots the kernel took, at least one, or the error the first message met.
pub(crate) fn send(
    fd: RawFd,
    slots: &[Slot],
    gso: bool,
    headers: &mut Headers,
) -> Result<usize, SendFailed> {
    let Headers {
        headers: messages,
        iovecs,
        names,
        controls,
        groups,
    } = headers;
    messages.clear();
    iovecs.clear();
    groups.clear();
    let mut at = 0;
    while let Some(rest) = slots.get(at..).filter(|rest| !rest.is_empty()) {
        if groups.len() >= names.len() {
            break;
        }
        let count = group(rest, gso);
        for slot in rest.iter().take(count) {
            iovecs.push(libc::iovec {
                iov_base: slot.bytes.as_ptr().cast_mut().cast(),
                iov_len: slot.bytes.len(),
            });
        }
        groups.push(count);
        at = at.saturating_add(count);
    }
    // The vectors are filled; only now are pointers into them taken, so none moves after.
    let mut first_iovec = 0;
    let mut start = 0;
    for (count, (name, control)) in groups.iter().zip(names.iter_mut().zip(controls.iter_mut())) {
        let Some(slot) = slots.get(start) else {
            break;
        };
        let name_length = write_name(slot.to, name);
        // SAFETY: an all-zero `mmsghdr` is valid: null pointers with zero lengths.
        let mut message: libc::mmsghdr = unsafe { zeroed() };
        message.msg_hdr.msg_name = ptr::from_mut(name).cast();
        message.msg_hdr.msg_namelen = name_length;
        message.msg_hdr.msg_iov = iovecs
            .get_mut(first_iovec..)
            .map_or(ptr::null_mut(), <[libc::iovec]>::as_mut_ptr);
        message.msg_hdr.msg_iovlen = field(*count);
        if *count > 1 {
            segment(&mut message.msg_hdr, control, slot.bytes.len());
        }
        messages.push(message);
        first_iovec = first_iovec.saturating_add(*count);
        start = start.saturating_add(*count);
    }
    let length = libc::c_uint::try_from(messages.len()).unwrap_or(libc::c_uint::MAX);
    // SAFETY: `messages` holds `length` initialised headers whose names, iovecs and controls
    // point into `names`, `iovecs`, `controls` and `slots`, all borrowed for this call and not
    // moved since the pointers were taken; sendmmsg reads them and writes only `msg_len`.
    let sent = unsafe {
        libc::syscall(
            libc::SYS_sendmmsg,
            fd,
            messages.as_mut_ptr(),
            length,
            libc::MSG_DONTWAIT,
        )
    };
    match usize::try_from(sent) {
        Ok(messages_sent) => Ok(groups.iter().take(messages_sent).sum()),
        Err(_) => Err(SendFailed {
            error: io::Error::last_os_error(),
            slots: groups.first().copied().unwrap_or(1),
        }),
    }
}

/// Writes one `UDP_SEGMENT` control message of `size` into `control` and points `header` at it.
fn segment(header: &mut libc::msghdr, control: &mut Control, size: usize) {
    let Ok(size) = u16::try_from(size) else {
        return;
    };
    // SAFETY: CMSG_SPACE only computes a length.
    let space = unsafe { libc::CMSG_SPACE(socklen_u32(size_of::<u16>())) };
    let Ok(space) = usize::try_from(space) else {
        return;
    };
    if space > size_of::<Control>() {
        return;
    }
    header.msg_control = control.as_mut_ptr().cast();
    header.msg_controllen = field(space);
    // SAFETY: the control buffer is `space` bytes or more and aligned for `cmsghdr`, so
    // CMSG_FIRSTHDR returns a header inside it, and CMSG_DATA its data, with room for a `u16`.
    unsafe {
        let first = libc::CMSG_FIRSTHDR(header);
        if first.is_null() {
            header.msg_control = ptr::null_mut();
            header.msg_controllen = 0;
            return;
        }
        (*first).cmsg_level = libc::SOL_UDP;
        (*first).cmsg_type = libc::UDP_SEGMENT;
        (*first).cmsg_len = libc::CMSG_LEN(socklen_u32(size_of::<u16>()))
            .try_into()
            .unwrap_or_default();
        ptr::write_unaligned(libc::CMSG_DATA(first).cast::<u16>(), size);
    }
}

fn socklen_u32(bytes: usize) -> u32 {
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

/// One message `recvmmsg` delivered.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Received {
    /// The buffer the message is in: messages skipped (truncated, not internet) leave gaps, so a message is
    /// paired with its buffer by index, not by position (mantle's review, finding 10).
    pub(crate) index: usize,
    pub(crate) from: SocketAddr,
    pub(crate) length: usize,
    /// The size of each coalesced datagram, when the kernel coalesced them.
    pub(crate) segment: Option<usize>,
    /// The kernel's receive stamp, nanoseconds of `CLOCK_REALTIME`, when it attached one.
    pub(crate) stamp: Option<u64>,
}

/// Receives up to one message into each of `buffers` with one `recvmmsg`, appending what arrived
/// to `out`. A message from an address that is not an internet one, or one the buffer truncated,
/// is skipped: QUIC and the plane treat it as lost.
pub(crate) fn receive(
    fd: RawFd,
    buffers: &mut [impl AsMut<[u8]>],
    headers: &mut Headers,
    out: &mut Vec<Received>,
) -> io::Result<usize> {
    let Headers {
        headers: messages,
        iovecs,
        names,
        controls,
        ..
    } = headers;
    messages.clear();
    iovecs.clear();
    let count = buffers.len().min(names.len());
    for buffer in buffers.iter_mut().take(count) {
        let buffer = buffer.as_mut();
        iovecs.push(libc::iovec {
            iov_base: buffer.as_mut_ptr().cast(),
            iov_len: buffer.len(),
        });
    }
    for ((iovec, name), control) in iovecs
        .iter_mut()
        .zip(names.iter_mut())
        .zip(controls.iter_mut())
    {
        // SAFETY: an all-zero `mmsghdr` is valid: null pointers with zero lengths.
        let mut message: libc::mmsghdr = unsafe { zeroed() };
        message.msg_hdr.msg_name = ptr::from_mut(name).cast();
        message.msg_hdr.msg_namelen = socklen(size_of::<libc::sockaddr_storage>());
        message.msg_hdr.msg_iov = ptr::from_mut(iovec);
        message.msg_hdr.msg_iovlen = 1;
        message.msg_hdr.msg_control = control.as_mut_ptr().cast();
        message.msg_hdr.msg_controllen = field(size_of::<Control>());
        messages.push(message);
    }
    let length = libc::c_uint::try_from(messages.len()).unwrap_or(libc::c_uint::MAX);
    // SAFETY: `messages` holds `length` headers whose names, iovecs and controls point into
    // `names`, `iovecs`, `controls` and `buffers`, borrowed mutably for this call and not moved
    // since; recvmmsg writes within the lengths each header gives. The timeout is null: the
    // socket is non-blocking.
    let received = unsafe {
        libc::syscall(
            libc::SYS_recvmmsg,
            fd,
            messages.as_mut_ptr(),
            length,
            libc::MSG_DONTWAIT,
            ptr::null_mut::<libc::timespec>(),
        )
    };
    let Ok(received) = usize::try_from(received) else {
        return Err(io::Error::last_os_error());
    };
    for (index, (message, name)) in messages
        .iter_mut()
        .zip(names.iter())
        .take(received)
        .enumerate()
    {
        if message.msg_hdr.msg_flags & libc::MSG_TRUNC != 0 {
            continue;
        }
        let Some(from) = read_name(name) else {
            continue;
        };
        let Ok(length) = usize::try_from(message.msg_len) else {
            continue;
        };
        let (segment, stamp) = controls_of(&message.msg_hdr);
        out.push(Received {
            index,
            from,
            length,
            segment,
            stamp,
        });
    }
    Ok(received)
}

/// A length the C library gives as `size_t` (glibc's `cmsg_len`) or `socklen_t` (musl's), as a
/// `usize`; zero for one that does not fit, which no control message is.
fn width<T: TryInto<usize>>(length: T) -> usize {
    length.try_into().unwrap_or(0)
}

/// `CMSG_LEN(bytes)`: the `cmsg_len` of a whole control message of `bytes` of data.
fn whole(bytes: usize) -> usize {
    // SAFETY: CMSG_LEN only computes a length.
    let length = unsafe { libc::CMSG_LEN(socklen_u32(bytes)) };
    usize::try_from(length).unwrap_or(usize::MAX)
}

/// The `UDP_GRO` segment size of a received message, if the kernel coalesced it, and its
/// `SCM_TIMESTAMPNS` stamp in nanoseconds, if the kernel stamped it. A message whose `cmsg_len`
/// does not cover its data (a truncated control buffer) is not read.
fn controls_of(header: &libc::msghdr) -> (Option<usize>, Option<u64>) {
    let (mut segment, mut stamp) = (None, None);
    // SAFETY: `header` was filled by recvmmsg, whose control buffer and length are its own;
    // CMSG_FIRSTHDR and CMSG_NXTHDR stay within `msg_controllen`; a `UDP_GRO` message's data is
    // an `int` and an `SCM_TIMESTAMPNS` message's a `timespec`, each read unaligned once its
    // `cmsg_len` is checked to cover it.
    unsafe {
        let mut at = libc::CMSG_FIRSTHDR(header);
        while let Some(control) = at.as_ref() {
            let length = width(control.cmsg_len);
            if control.cmsg_level == libc::SOL_UDP
                && control.cmsg_type == libc::UDP_GRO
                && length >= whole(size_of::<libc::c_int>())
            {
                let size = ptr::read_unaligned(libc::CMSG_DATA(at).cast::<libc::c_int>());
                segment = usize::try_from(size).ok().filter(|size| *size > 0);
            } else if control.cmsg_level == libc::SOL_SOCKET
                && control.cmsg_type == libc::SCM_TIMESTAMPNS
                && length >= whole(size_of::<libc::timespec>())
            {
                let time = ptr::read_unaligned(libc::CMSG_DATA(at).cast::<libc::timespec>());
                stamp = timespec_ns(&time);
            }
            at = libc::CMSG_NXTHDR(header, at);
        }
    }
    (segment, stamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The control buffer holds a `UDP_GRO` message beside an `SCM_TIMESTAMPNS` one.
    #[test]
    fn the_control_buffer_holds_both() {
        // SAFETY: CMSG_SPACE only computes a length.
        let both = unsafe {
            libc::CMSG_SPACE(socklen_u32(size_of::<libc::c_int>()))
                + libc::CMSG_SPACE(socklen_u32(size_of::<libc::timespec>()))
        };
        assert!(usize::try_from(both).unwrap() <= size_of::<Control>());
    }
}
