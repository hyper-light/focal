use std::{fmt, net::SocketAddr};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{Instant, MAX_CID_SIZE, ResetToken, coding::BufExt, packet::PartialDecode};

/// Events sent from an Endpoint to a Connection
#[derive(Debug)]
pub struct ConnectionEvent(pub(crate) ConnectionEventInner);

#[derive(Debug)]
pub(crate) enum ConnectionEventInner {
    /// A datagram has been received for the Connection
    Datagram(DatagramConnectionEvent),
    /// New connection identifiers have been issued for the Connection
    NewIdentifiers(Vec<IssuedCid>, Instant),
}

/// Variant of [`ConnectionEventInner`].
#[derive(Debug)]
pub(crate) struct DatagramConnectionEvent {
    pub(crate) now: Instant,
    pub(crate) remote: SocketAddr,
    pub(crate) ecn: Option<EcnCodepoint>,
    pub(crate) first_decode: PartialDecode,
    pub(crate) remaining: Option<BytesMut>,
}

/// Events sent from a Connection to an Endpoint
#[derive(Debug)]
pub struct EndpointEvent(pub(crate) EndpointEventInner);

impl EndpointEvent {
    /// Construct an event that indicating that a `Connection` will no longer emit events
    ///
    /// Useful for notifying an `Endpoint` that a `Connection` has been destroyed outside of the
    /// usual state machine flow, e.g. when being dropped by the user.
    pub fn drained() -> Self {
        Self(EndpointEventInner::Drained)
    }

    /// Determine whether this is the last event a `Connection` will emit
    ///
    /// Useful for determining when connection-related event loop state can be freed.
    pub fn is_drained(&self) -> bool {
        self.0 == EndpointEventInner::Drained
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EndpointEventInner {
    /// The connection has been drained
    Drained,
    /// The reset token and/or address eligible for generating resets has been updated
    ResetToken(SocketAddr, ResetToken),
    /// The connection needs connection identifiers
    NeedIdentifiers(Instant, u64),
    /// Stop routing connection ID for this sequence number to the connection
    /// When `bool == true`, a new connection ID will be issued to peer
    RetireConnectionId(Instant, u64, bool),
    /// The server sent an address validation token (NEW_TOKEN) for later connections to it
    NewToken { server_name: String, token: Bytes },
    /// What the connection leaves its endpoint's Careful Resume memory for its remote (RFC 9959
    /// §3.1): a measurement, as its warm-up makes one or as it closes, or at its close what becomes
    /// of the remote's mark it held
    Resume(crate::connection::Leaves),
    /// What the connection counted of its path's loss bursts since it last said
    /// (`connection::LossFit`)
    Losses(Instant, crate::connection::LossFit),
}

/// Protocol-level identifier for a connection.
///
/// Mainly useful for identifying this connection's packets on the wire with tools like Wireshark.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ConnectionId {
    /// length of CID
    len: u8,
    /// CID in byte array
    bytes: [u8; MAX_CID_SIZE],
}

impl ConnectionId {
    /// Construct cid from byte array
    ///
    /// A CID is at most `MAX_CID_SIZE` bytes (RFC 9000 §17.2); this keeps that many of a longer
    /// slice (upstream asserted in debug builds and panicked).
    pub fn new(bytes: &[u8]) -> Self {
        let mut res = Self {
            len: 0,
            bytes: [0; MAX_CID_SIZE],
        };
        let (cid, _) = bytes.split_at(bytes.len().min(MAX_CID_SIZE));
        res.set_len(cid.len());
        res.copy_from_slice(cid);
        res
    }

    /// Constructs cid by reading `len` bytes from a `Buf`
    ///
    /// Callers need to assure that `buf.remaining() >= len` and `len <= MAX_CID_SIZE`; a call
    /// that does not reads only what both allow (upstream asserted the second in debug builds
    /// and panicked on the first).
    pub fn from_buf(buf: &mut (impl Buf + ?Sized), len: usize) -> Self {
        let mut res = Self {
            len: 0,
            bytes: [0; MAX_CID_SIZE],
        };
        res.set_len(len.min(MAX_CID_SIZE).min(buf.remaining()));
        buf.copy_to_slice(&mut res);
        res
    }

    /// Sets the length, which is at most `MAX_CID_SIZE`, and so fits the `u8`.
    fn set_len(&mut self, len: usize) {
        self.len = u8::try_from(len.min(MAX_CID_SIZE)).unwrap_or(0);
    }

    /// Decode from long header format
    pub(crate) fn decode_long(buf: &mut impl Buf) -> Option<Self> {
        let len = buf.get::<u8>().ok()? as usize;
        match len > MAX_CID_SIZE || buf.remaining() < len {
            false => Some(Self::from_buf(buf, len)),
            true => None,
        }
    }

    /// Encode in long header format
    pub(crate) fn encode_long(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.len);
        buf.put_slice(self);
    }
}

impl ::std::ops::Deref for ConnectionId {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // `len` is at most MAX_CID_SIZE (`set_len`).
        self.bytes
            .split_at(usize::from(self.len).min(MAX_CID_SIZE))
            .0
    }
}

impl ::std::ops::DerefMut for ConnectionId {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.bytes
            .split_at_mut(usize::from(self.len).min(MAX_CID_SIZE))
            .0
    }
}

impl fmt::Debug for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.iter() {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Explicit congestion notification codepoint
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum EcnCodepoint {
    /// The ECT(0) codepoint, indicating that an endpoint is ECN-capable
    Ect0 = 0b10,
    /// The ECT(1) codepoint, indicating that an endpoint is ECN-capable
    Ect1 = 0b01,
    /// The CE codepoint, signalling that congestion was experienced
    Ce = 0b11,
}

impl EcnCodepoint {
    /// Create new object from the given bits
    pub fn from_bits(x: u8) -> Option<Self> {
        use EcnCodepoint::*;
        Some(match x & 0b11 {
            0b10 => Ect0,
            0b01 => Ect1,
            0b11 => Ce,
            _ => {
                return None;
            }
        })
    }

    /// Returns whether the codepoint is a CE, signalling that congestion was experienced
    pub fn is_ce(self) -> bool {
        matches!(self, Self::Ce)
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct IssuedCid {
    pub(crate) sequence: u64,
    pub(crate) id: ConnectionId,
    pub(crate) reset_token: ResetToken,
}
