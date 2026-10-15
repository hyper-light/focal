use std::{cmp::Ordering, io, ops::Range, str};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::{
    ConnectionId,
    coding::{self, BufExt, BufMutExt},
    crypto,
};

/// Decodes a QUIC packet's invariant header
///
/// Due to packet number encryption, it is impossible to fully decode a header
/// (which includes a variable-length packet number) without crypto context.
/// The crypto context (represented by the `Crypto` type in Quinn) is usually
/// part of the `Connection`, or can be derived from the destination CID for
/// Initial packets.
///
/// To cope with this, we decode the invariant header (which should be stable
/// across QUIC versions), which gives us the destination CID and allows us
/// to inspect the version and packet type (which depends on the version).
/// This information allows us to fully decode and decrypt the packet.
#[cfg_attr(test, derive(Clone))]
#[derive(Debug)]
pub struct PartialDecode {
    plain_header: ProtectedHeader,
    buf: io::Cursor<BytesMut>,
}

#[allow(clippy::len_without_is_empty)]
impl PartialDecode {
    /// Begin decoding a QUIC packet from `bytes`, returning any trailing data not part of that packet
    pub fn new(
        bytes: BytesMut,
        cid_parser: &(impl ConnectionIdParser + ?Sized),
        supported_versions: &[u32],
        grease_quic_bit: bool,
    ) -> Result<(Self, Option<BytesMut>), PacketDecodeError> {
        let mut buf = io::Cursor::new(bytes);
        let plain_header =
            ProtectedHeader::decode(&mut buf, cid_parser, supported_versions, grease_quic_bit)?;
        let dgram_len = buf.get_ref().len();
        // The payload length is the peer's varint; an end past what a `usize` holds is past the
        // datagram too
        let packet_len = match plain_header.payload_len() {
            Some(len) => buf
                .position()
                .checked_add(len)
                .and_then(|end| usize::try_from(end).ok())
                .ok_or(PacketDecodeError::InvalidHeader(
                    "packet too short to contain payload length",
                ))?,
            None => dgram_len,
        };
        match dgram_len.cmp(&packet_len) {
            Ordering::Equal => Ok((Self { plain_header, buf }, None)),
            Ordering::Less => Err(PacketDecodeError::InvalidHeader(
                "packet too short to contain payload length",
            )),
            Ordering::Greater => {
                let rest = Some(buf.get_mut().split_off(packet_len));
                Ok((Self { plain_header, buf }, rest))
            }
        }
    }

    /// The underlying partially-decoded packet data
    pub(crate) fn data(&self) -> &[u8] {
        self.buf.get_ref()
    }

    pub(crate) fn initial_header(&self) -> Option<&ProtectedInitialHeader> {
        self.plain_header.as_initial()
    }

    pub(crate) fn has_long_header(&self) -> bool {
        !matches!(self.plain_header, ProtectedHeader::Short { .. })
    }

    pub(crate) fn is_initial(&self) -> bool {
        self.space() == Some(SpaceId::Initial)
    }

    pub(crate) fn space(&self) -> Option<SpaceId> {
        use ProtectedHeader::*;
        match self.plain_header {
            Initial { .. } => Some(SpaceId::Initial),
            Long {
                ty: LongType::Handshake,
                ..
            } => Some(SpaceId::Handshake),
            Long {
                ty: LongType::ZeroRtt,
                ..
            } => Some(SpaceId::Data),
            Short { .. } => Some(SpaceId::Data),
            _ => None,
        }
    }

    pub(crate) fn is_0rtt(&self) -> bool {
        match self.plain_header {
            ProtectedHeader::Long { ty, .. } => ty == LongType::ZeroRtt,
            _ => false,
        }
    }

    /// The destination connection ID of the packet
    pub fn dst_cid(&self) -> &ConnectionId {
        self.plain_header.dst_cid()
    }

    /// Length of QUIC packet being decoded
    #[allow(unreachable_pub)] // fuzzing only
    pub fn len(&self) -> usize {
        self.buf.get_ref().len()
    }

    pub(crate) fn finish(
        self,
        header_crypto: Option<&dyn crypto::HeaderKey>,
    ) -> Result<Packet, PacketDecodeError> {
        use ProtectedHeader::*;
        let Self {
            plain_header,
            mut buf,
        } = self;
        // The connection unprotects a protected header only with its space's keys, where
        // upstream unwrapped; a header without them is refused
        let keys = || header_crypto.ok_or(PacketDecodeError::InvalidHeader("no header keys"));

        let header = match plain_header {
            Initial(ProtectedInitialHeader {
                dst_cid,
                src_cid,
                token_pos,
                version,
                ..
            }) => {
                let number = Self::decrypt_header(&mut buf, keys()?)?;
                return Self::finish_initial(buf, dst_cid, src_cid, token_pos, number, version);
            }
            Long {
                ty,
                dst_cid,
                src_cid,
                version,
                ..
            } => Header::Long {
                ty,
                dst_cid,
                src_cid,
                number: Self::decrypt_header(&mut buf, keys()?)?,
                version,
            },
            Retry {
                dst_cid,
                src_cid,
                version,
            } => Header::Retry {
                dst_cid,
                src_cid,
                version,
            },
            Short { spin, dst_cid, .. } => {
                let number = Self::decrypt_header(&mut buf, keys()?)?;
                let first = buf
                    .get_ref()
                    .first()
                    .ok_or(PacketDecodeError::InvalidHeader("empty packet"))?;
                let key_phase = first & KEY_PHASE_BIT != 0;
                Header::Short {
                    spin,
                    key_phase,
                    dst_cid,
                    number,
                }
            }
            VersionNegotiate {
                random,
                dst_cid,
                src_cid,
            } => Header::VersionNegotiate {
                random,
                dst_cid,
                src_cid,
            },
        };

        let header_len = header_len(&buf)?;
        let mut bytes = buf.into_inner();
        Ok(Packet {
            header,
            header_data: bytes.split_to(header_len).freeze(),
            payload: bytes,
        })
    }

    /// Splits an Initial packet, its header unprotected, into header and payload
    fn finish_initial(
        buf: io::Cursor<BytesMut>,
        dst_cid: ConnectionId,
        src_cid: ConnectionId,
        token_pos: Range<usize>,
        number: PacketNumber,
        version: u32,
    ) -> Result<Packet, PacketDecodeError> {
        let header_len = header_len(&buf)?;
        let mut bytes = buf.into_inner();

        let header_data = bytes.split_to(header_len).freeze();
        // The token was found within the header as it was decoded
        if token_pos.start > token_pos.end || token_pos.end > header_data.len() {
            return Err(PacketDecodeError::InvalidHeader("token out of bounds"));
        }
        let token = header_data.slice(token_pos);
        Ok(Packet {
            header: Header::Initial(InitialHeader {
                dst_cid,
                src_cid,
                token,
                number,
                version,
            }),
            header_data,
            payload: bytes,
        })
    }

    fn decrypt_header(
        buf: &mut io::Cursor<BytesMut>,
        header_crypto: &dyn crypto::HeaderKey,
    ) -> Result<PacketNumber, PacketDecodeError> {
        let packet_length = buf.get_ref().len();
        let pn_offset = header_len(buf)?;
        // A required size: saturating can only over-state it, which refuses the packet
        if packet_length
            < pn_offset
                .saturating_add(4)
                .saturating_add(header_crypto.sample_size())
        {
            return Err(PacketDecodeError::InvalidHeader(
                "packet too short to extract header protection sample",
            ));
        }

        header_crypto
            .decrypt(pn_offset, buf.get_mut())
            .map_err(|_| {
                PacketDecodeError::InvalidHeader("packet too short to remove header protection")
            })?;

        let first = *buf
            .get_ref()
            .first()
            .ok_or(PacketDecodeError::InvalidHeader("empty packet"))?;
        PacketNumber::decode(first, buf)
    }
}

pub(crate) struct Packet {
    pub(crate) header: Header,
    pub(crate) header_data: Bytes,
    pub(crate) payload: BytesMut,
}

impl Packet {
    pub(crate) fn reserved_bits_valid(&self) -> bool {
        let mask = match self.header {
            Header::Short { .. } => SHORT_RESERVED_BITS,
            _ => LONG_RESERVED_BITS,
        };
        // A header is at least its first byte
        self.header_data
            .first()
            .is_some_and(|first| first & mask == 0)
    }
}

pub(crate) struct InitialPacket {
    pub(crate) header: InitialHeader,
    pub(crate) header_data: Bytes,
    pub(crate) payload: BytesMut,
}

impl From<InitialPacket> for Packet {
    fn from(x: InitialPacket) -> Self {
        Self {
            header: Header::Initial(x.header),
            header_data: x.header_data,
            payload: x.payload,
        }
    }
}

#[cfg_attr(test, derive(Clone))]
#[derive(Debug)]
pub(crate) enum Header {
    Initial(InitialHeader),
    Long {
        ty: LongType,
        dst_cid: ConnectionId,
        src_cid: ConnectionId,
        number: PacketNumber,
        version: u32,
    },
    Retry {
        dst_cid: ConnectionId,
        src_cid: ConnectionId,
        version: u32,
    },
    Short {
        spin: bool,
        key_phase: bool,
        dst_cid: ConnectionId,
        number: PacketNumber,
    },
    VersionNegotiate {
        random: u8,
        src_cid: ConnectionId,
        dst_cid: ConnectionId,
    },
}

impl Header {
    pub(crate) fn encode(&self, w: &mut Vec<u8>) -> PartialEncode {
        use Header::*;
        let start = w.len();
        match *self {
            Initial(InitialHeader {
                ref dst_cid,
                ref src_cid,
                ref token,
                number,
                version,
            }) => {
                w.write(u8::from(LongHeaderType::Initial) | number.tag());
                w.write(version);
                dst_cid.encode_long(w);
                src_cid.encode_long(w);
                w.write_var(token.len() as u64);
                w.put_slice(token);
                w.write::<u16>(0); // Placeholder for payload length; see `set_payload_length`
                number.encode(w);
                PartialEncode {
                    start,
                    header_len: w.len().saturating_sub(start),
                    pn: Some((number.len(), true)),
                }
            }
            Long {
                ty,
                ref dst_cid,
                ref src_cid,
                number,
                version,
            } => {
                w.write(u8::from(LongHeaderType::Standard(ty)) | number.tag());
                w.write(version);
                dst_cid.encode_long(w);
                src_cid.encode_long(w);
                w.write::<u16>(0); // Placeholder for payload length; see `set_payload_length`
                number.encode(w);
                PartialEncode {
                    start,
                    header_len: w.len().saturating_sub(start),
                    pn: Some((number.len(), true)),
                }
            }
            Retry {
                ref dst_cid,
                ref src_cid,
                version,
            } => {
                w.write(u8::from(LongHeaderType::Retry));
                w.write(version);
                dst_cid.encode_long(w);
                src_cid.encode_long(w);
                PartialEncode {
                    start,
                    header_len: w.len().saturating_sub(start),
                    pn: None,
                }
            }
            Short {
                spin,
                key_phase,
                ref dst_cid,
                number,
            } => {
                w.write(
                    FIXED_BIT
                        | if key_phase { KEY_PHASE_BIT } else { 0 }
                        | if spin { SPIN_BIT } else { 0 }
                        | number.tag(),
                );
                w.put_slice(dst_cid);
                number.encode(w);
                PartialEncode {
                    start,
                    header_len: w.len().saturating_sub(start),
                    pn: Some((number.len(), false)),
                }
            }
            VersionNegotiate {
                ref random,
                ref dst_cid,
                ref src_cid,
            } => {
                w.write(0x80u8 | random);
                w.write::<u32>(0);
                dst_cid.encode_long(w);
                src_cid.encode_long(w);
                PartialEncode {
                    start,
                    header_len: w.len().saturating_sub(start),
                    pn: None,
                }
            }
        }
    }

    /// Whether the packet is encrypted on the wire
    pub(crate) fn is_protected(&self) -> bool {
        !matches!(*self, Self::Retry { .. } | Self::VersionNegotiate { .. })
    }

    pub(crate) fn number(&self) -> Option<PacketNumber> {
        use Header::*;
        Some(match *self {
            Initial(InitialHeader { number, .. }) => number,
            Long { number, .. } => number,
            Short { number, .. } => number,
            _ => {
                return None;
            }
        })
    }

    pub(crate) fn space(&self) -> SpaceId {
        use Header::*;
        match *self {
            Short { .. } => SpaceId::Data,
            Long {
                ty: LongType::ZeroRtt,
                ..
            } => SpaceId::Data,
            Long {
                ty: LongType::Handshake,
                ..
            } => SpaceId::Handshake,
            _ => SpaceId::Initial,
        }
    }

    pub(crate) fn key_phase(&self) -> bool {
        match *self {
            Self::Short { key_phase, .. } => key_phase,
            _ => false,
        }
    }

    pub(crate) fn is_short(&self) -> bool {
        matches!(*self, Self::Short { .. })
    }

    pub(crate) fn is_1rtt(&self) -> bool {
        self.is_short()
    }

    pub(crate) fn is_0rtt(&self) -> bool {
        matches!(
            *self,
            Self::Long {
                ty: LongType::ZeroRtt,
                ..
            }
        )
    }

    pub(crate) fn dst_cid(&self) -> ConnectionId {
        use Header::*;
        match *self {
            Initial(InitialHeader { dst_cid, .. }) => dst_cid,
            Long { dst_cid, .. } => dst_cid,
            Retry { dst_cid, .. } => dst_cid,
            Short { dst_cid, .. } => dst_cid,
            VersionNegotiate { dst_cid, .. } => dst_cid,
        }
    }

    /// Whether the payload of this packet contains QUIC frames
    pub(crate) fn has_frames(&self) -> bool {
        use Header::*;
        match *self {
            Initial(_) => true,
            Long { .. } => true,
            Retry { .. } => false,
            Short { .. } => true,
            VersionNegotiate { .. } => false,
        }
    }
}

pub(crate) struct PartialEncode {
    pub(crate) start: usize,
    pub(crate) header_len: usize,
    // Packet number length, payload length needed
    pn: Option<(usize, bool)>,
}

impl PartialEncode {
    /// The largest end, an absolute position in the buffer, a packet with this header may
    /// reach: a long header's length field has two bytes reserved (a varint below 2^14) for the
    /// packet number and payload that follow it. `None` for a header without the field.
    pub(crate) fn length_limit(&self) -> Option<usize> {
        /// The largest length two varint bytes hold
        const MAX_LENGTH: usize = (1 << 14) - 1;
        let (pn_len, true) = self.pn? else {
            return None;
        };
        // In-memory positions, far below `usize::MAX`
        Some(
            self.start
                .saturating_add(self.header_len)
                .saturating_sub(pn_len)
                .saturating_add(MAX_LENGTH),
        )
    }

    /// Writes the length, encrypts the payload and protects the header; fails if the packet is
    /// too short for its tag or its header protection sample (upstream panicked)
    pub(crate) fn finish(
        self,
        buf: &mut [u8],
        header_crypto: &dyn crypto::HeaderKey,
        crypto: Option<(u64, &dyn crypto::PacketKey)>,
    ) -> Result<(), crypto::CryptoError> {
        let Self { header_len, pn, .. } = self;
        let (pn_len, write_len) = match pn {
            Some((pn_len, write_len)) => (pn_len, write_len),
            None => return Ok(()),
        };

        // The header ends with its packet number
        let pn_pos = header_len.saturating_sub(pn_len);
        if write_len {
            // The packet builder keeps a long header packet within the two bytes reserved for
            // its length (`length_limit`), where upstream asserted it fit
            let len = buf.len().saturating_sub(header_len).saturating_add(pn_len);
            if let (Ok(len), Some(mut slice)) = (
                u16::try_from(len),
                pn_pos
                    .checked_sub(2)
                    .and_then(|length_pos| buf.get_mut(length_pos..pn_pos)),
            ) {
                slice.put_u16(len | (0b01 << 14));
            }
        }

        if let Some((number, crypto)) = crypto {
            crypto.encrypt(number, buf, header_len)?;
        }

        header_crypto.encrypt(pn_pos, buf)
    }
}

/// Plain packet header
#[derive(Clone, Debug)]
pub enum ProtectedHeader {
    /// An Initial packet header
    Initial(ProtectedInitialHeader),
    /// A Long packet header, as used during the handshake
    Long {
        /// Type of the Long header packet
        ty: LongType,
        /// Destination Connection ID
        dst_cid: ConnectionId,
        /// Source Connection ID
        src_cid: ConnectionId,
        /// Length of the packet payload
        len: u64,
        /// QUIC version
        version: u32,
    },
    /// A Retry packet header
    Retry {
        /// Destination Connection ID
        dst_cid: ConnectionId,
        /// Source Connection ID
        src_cid: ConnectionId,
        /// QUIC version
        version: u32,
    },
    /// A short packet header, as used during the data phase
    Short {
        /// Spin bit
        spin: bool,
        /// Destination Connection ID
        dst_cid: ConnectionId,
    },
    /// A Version Negotiation packet header
    VersionNegotiate {
        /// Random value
        random: u8,
        /// Destination Connection ID
        dst_cid: ConnectionId,
        /// Source Connection ID
        src_cid: ConnectionId,
    },
}

impl ProtectedHeader {
    fn as_initial(&self) -> Option<&ProtectedInitialHeader> {
        match self {
            Self::Initial(x) => Some(x),
            _ => None,
        }
    }

    /// The destination Connection ID of the packet
    pub fn dst_cid(&self) -> &ConnectionId {
        use ProtectedHeader::*;
        match self {
            Initial(header) => &header.dst_cid,
            Long { dst_cid, .. } => dst_cid,
            Retry { dst_cid, .. } => dst_cid,
            Short { dst_cid, .. } => dst_cid,
            VersionNegotiate { dst_cid, .. } => dst_cid,
        }
    }

    fn payload_len(&self) -> Option<u64> {
        use ProtectedHeader::*;
        match self {
            Initial(ProtectedInitialHeader { len, .. }) | Long { len, .. } => Some(*len),
            _ => None,
        }
    }

    /// Decode a plain header from given buffer, with given [`ConnectionIdParser`].
    pub fn decode(
        buf: &mut io::Cursor<BytesMut>,
        cid_parser: &(impl ConnectionIdParser + ?Sized),
        supported_versions: &[u32],
        grease_quic_bit: bool,
    ) -> Result<Self, PacketDecodeError> {
        let first = buf.get::<u8>()?;
        if !grease_quic_bit && first & FIXED_BIT == 0 {
            return Err(PacketDecodeError::InvalidHeader("fixed bit unset"));
        }
        if first & LONG_HEADER_FORM == 0 {
            let spin = first & SPIN_BIT != 0;

            Ok(Self::Short {
                spin,
                dst_cid: cid_parser.parse(buf)?,
            })
        } else {
            let version = buf.get::<u32>()?;

            let dst_cid = ConnectionId::decode_long(buf)
                .ok_or(PacketDecodeError::InvalidHeader("malformed cid"))?;
            let src_cid = ConnectionId::decode_long(buf)
                .ok_or(PacketDecodeError::InvalidHeader("malformed cid"))?;

            // TODO: Support long CIDs for compatibility with future QUIC versions
            if version == 0 {
                let random = first & !LONG_HEADER_FORM;
                return Ok(Self::VersionNegotiate {
                    random,
                    dst_cid,
                    src_cid,
                });
            }

            if !supported_versions.contains(&version) {
                return Err(PacketDecodeError::UnsupportedVersion {
                    src_cid,
                    dst_cid,
                    version,
                });
            }

            match LongHeaderType::from_byte(first)? {
                LongHeaderType::Initial => {
                    let token_len = usize::try_from(buf.get_var()?)
                        .ok()
                        .filter(|&len| len <= buf.remaining())
                        .ok_or(PacketDecodeError::InvalidHeader("token out of bounds"))?;
                    let token_start = header_len(buf)?;
                    // Within the datagram, as just checked
                    let token_end = token_start.saturating_add(token_len);
                    buf.advance(token_len);

                    let len = buf.get_var()?;
                    Ok(Self::Initial(ProtectedInitialHeader {
                        dst_cid,
                        src_cid,
                        token_pos: token_start..token_end,
                        len,
                        version,
                    }))
                }
                LongHeaderType::Retry => Ok(Self::Retry {
                    dst_cid,
                    src_cid,
                    version,
                }),
                LongHeaderType::Standard(ty) => Ok(Self::Long {
                    ty,
                    dst_cid,
                    src_cid,
                    len: buf.get_var()?,
                    version,
                }),
            }
        }
    }
}

/// Header of an Initial packet, before decryption
#[derive(Clone, Debug)]
pub struct ProtectedInitialHeader {
    /// Destination Connection ID
    pub dst_cid: ConnectionId,
    /// Source Connection ID
    pub src_cid: ConnectionId,
    /// The position of a token in the packet buffer
    pub token_pos: Range<usize>,
    /// Length of the packet payload
    pub len: u64,
    /// QUIC version
    pub version: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct InitialHeader {
    pub(crate) dst_cid: ConnectionId,
    pub(crate) src_cid: ConnectionId,
    pub(crate) token: Bytes,
    pub(crate) number: PacketNumber,
    pub(crate) version: u32,
}

// An encoded packet number
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum PacketNumber {
    U8(u8),
    U16(u16),
    U24(u32),
    U32(u32),
}

impl PacketNumber {
    pub(crate) fn new(n: u64, largest_acked: u64) -> Option<Self> {
        // Each packet number is past every one acknowledged, and below 2^62; a number 2^31 or
        // more past the largest acknowledged has no encoding (RFC 9000 §17.1), where upstream
        // panicked. The truncation to the low bytes is the encoding.
        let range = n.checked_sub(largest_acked)?.checked_mul(2)?;
        let [.., b3, b2, b1, b0] = n.to_be_bytes();
        Some(if range < 1 << 8 {
            Self::U8(b0)
        } else if range < 1 << 16 {
            Self::U16(u16::from_be_bytes([b1, b0]))
        } else if range < 1 << 24 {
            Self::U24(u32::from_be_bytes([0, b2, b1, b0]))
        } else if range < 1 << 32 {
            Self::U32(u32::from_be_bytes([b3, b2, b1, b0]))
        } else {
            return None;
        })
    }

    pub(crate) fn len(self) -> usize {
        use PacketNumber::*;
        match self {
            U8(_) => 1,
            U16(_) => 2,
            U24(_) => 3,
            U32(_) => 4,
        }
    }

    pub(crate) fn encode<W: BufMut>(self, w: &mut W) {
        use PacketNumber::*;
        match self {
            U8(x) => w.write(x),
            U16(x) => w.write(x),
            U24(x) => w.put_uint(u64::from(x), 3),
            U32(x) => w.write(x),
        }
    }

    /// Decodes the packet number whose length the first byte of its (unprotected) header tags
    pub(crate) fn decode<R: Buf>(first: u8, r: &mut R) -> Result<Self, PacketDecodeError> {
        use PacketNumber::*;
        let pn = match first & 0x03 {
            0 => U8(r.get()?),
            1 => U16(r.get()?),
            2 => {
                let high = r.get::<u8>()?;
                let low = r.get::<u16>()?;
                U24((u32::from(high) << 16) | u32::from(low))
            }
            _ => U32(r.get()?),
        };
        Ok(pn)
    }

    fn tag(self) -> u8 {
        use PacketNumber::*;
        match self {
            U8(_) => 0b00,
            U16(_) => 0b01,
            U24(_) => 0b10,
            U32(_) => 0b11,
        }
    }

    pub(crate) fn expand(self, expected: u64) -> u64 {
        // From Appendix A
        use PacketNumber::*;
        let truncated = match self {
            U8(x) => u64::from(x),
            U16(x) => u64::from(x),
            U24(x) => u64::from(x),
            U32(x) => u64::from(x),
        };
        let win: u64 = match self {
            U8(_) => 1 << 8,
            U16(_) => 1 << 16,
            U24(_) => 1 << 24,
            U32(_) => 1 << 32,
        };
        let hwin = win >> 1;
        // The window is at least 2^8
        let mask = win.saturating_sub(1);
        // The incoming packet number should be greater than expected - hwin and less than or equal
        // to expected + hwin
        //
        // This means we can't just strip the trailing bits from expected and add the truncated
        // because that might yield a value outside the window.
        //
        // The following code calculates a candidate value and makes sure it's within the packet
        // number window.
        let candidate = (expected & !mask) | truncated;
        // Packet numbers are below 2^62, so neither sum saturates
        if expected.checked_sub(hwin).is_some_and(|x| candidate <= x) {
            candidate.saturating_add(win)
        } else if candidate > expected.saturating_add(hwin) && candidate > win {
            candidate.saturating_sub(win)
        } else {
            candidate
        }
    }
}

/// The cursor's position: how much of the packet the header decoded so far takes, which is
/// within the packet
fn header_len<T: AsRef<[u8]>>(buf: &io::Cursor<T>) -> Result<usize, PacketDecodeError> {
    usize::try_from(buf.position())
        .ok()
        .filter(|&len| len <= buf.get_ref().as_ref().len())
        .ok_or(PacketDecodeError::InvalidHeader("header past the packet"))
}

/// A [`ConnectionIdParser`] implementation that assumes the connection ID is of fixed length
pub struct FixedLengthConnectionIdParser {
    expected_len: usize,
}

impl FixedLengthConnectionIdParser {
    /// Create a new instance of `FixedLengthConnectionIdParser`
    pub fn new(expected_len: usize) -> Self {
        Self { expected_len }
    }
}

impl ConnectionIdParser for FixedLengthConnectionIdParser {
    fn parse(&self, buffer: &mut dyn Buf) -> Result<ConnectionId, PacketDecodeError> {
        (buffer.remaining() >= self.expected_len)
            .then(|| ConnectionId::from_buf(buffer, self.expected_len))
            .ok_or(PacketDecodeError::InvalidHeader("packet too small"))
    }
}

/// Parse connection id in short header packet
pub trait ConnectionIdParser {
    /// Parse a connection id from given buffer
    fn parse(&self, buf: &mut dyn Buf) -> Result<ConnectionId, PacketDecodeError>;
}

/// Long packet type including non-uniform cases
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LongHeaderType {
    Initial,
    Retry,
    Standard(LongType),
}

impl LongHeaderType {
    fn from_byte(b: u8) -> Result<Self, PacketDecodeError> {
        use {LongHeaderType::*, LongType::*};
        // Two bits, so the last arm is 0x3
        Ok(match (b & 0x30) >> 4 {
            0x0 => Initial,
            0x1 => Standard(ZeroRtt),
            0x2 => Standard(Handshake),
            _ => Retry,
        })
    }
}

impl From<LongHeaderType> for u8 {
    fn from(ty: LongHeaderType) -> Self {
        use {LongHeaderType::*, LongType::*};
        match ty {
            Initial => LONG_HEADER_FORM | FIXED_BIT,
            Standard(ZeroRtt) => LONG_HEADER_FORM | FIXED_BIT | (0x1 << 4),
            Standard(Handshake) => LONG_HEADER_FORM | FIXED_BIT | (0x2 << 4),
            Retry => LONG_HEADER_FORM | FIXED_BIT | (0x3 << 4),
        }
    }
}

/// Long packet types with uniform header structure
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LongType {
    /// Handshake packet
    Handshake,
    /// 0-RTT packet
    ZeroRtt,
}

/// Packet decode error
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PacketDecodeError {
    /// Packet uses a QUIC version that is not supported
    #[error("unsupported version {version:x}")]
    UnsupportedVersion {
        /// Source Connection ID
        src_cid: ConnectionId,
        /// Destination Connection ID
        dst_cid: ConnectionId,
        /// The version that was unsupported
        version: u32,
    },
    /// The packet header is invalid
    #[error("invalid header: {0}")]
    InvalidHeader(&'static str),
}

impl From<coding::UnexpectedEnd> for PacketDecodeError {
    fn from(_: coding::UnexpectedEnd) -> Self {
        Self::InvalidHeader("unexpected end of packet")
    }
}

/// The Header Form bit (RFC 9000 §17.2)
pub(crate) const LONG_HEADER_FORM: u8 = 0x80;
/// The Fixed Bit (RFC 9000 §17.2)
pub(crate) const FIXED_BIT: u8 = 0x40;
/// The Latency Spin Bit (RFC 9000 §17.3.1)
pub(crate) const SPIN_BIT: u8 = 0x20;
/// A short header's Reserved Bits (RFC 9000 §17.3.1)
const SHORT_RESERVED_BITS: u8 = 0x18;
/// A long header's Reserved Bits (RFC 9000 §17.2)
const LONG_RESERVED_BITS: u8 = 0x0c;
/// The Key Phase bit (RFC 9000 §17.3.1)
const KEY_PHASE_BIT: u8 = 0x04;

/// Packet number space identifiers
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum SpaceId {
    /// Unprotected packets, used to bootstrap the handshake
    Initial = 0,
    Handshake = 1,
    /// Application data space, used for 0-RTT and post-handshake/1-RTT packets
    Data = 2,
}

impl SpaceId {
    pub(crate) fn iter() -> impl Iterator<Item = Self> {
        [Self::Initial, Self::Handshake, Self::Data].iter().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use std::io;

    /// A packet number 2^31 or more past the largest acknowledged has no encoding (RFC 9000
    /// §17.1): upstream panicked, and it is refused
    #[test]
    fn packet_number_beyond_any_encoding_is_refused() {
        assert_eq!(
            PacketNumber::new((1 << 31) - 1, 0),
            Some(PacketNumber::U32((1 << 31) - 1))
        );
        assert_eq!(PacketNumber::new(1 << 31, 0), None);
        assert_eq!(PacketNumber::new(5, 6), None);
    }

    fn check_pn(typed: PacketNumber, encoded: &[u8]) {
        let mut buf = Vec::new();
        typed.encode(&mut buf);
        assert_eq!(&buf[..], encoded);
        let decoded = PacketNumber::decode(typed.tag(), &mut io::Cursor::new(&buf)).unwrap();
        assert_eq!(typed, decoded);
    }

    #[test]
    fn roundtrip_packet_numbers() {
        check_pn(PacketNumber::U8(0x7f), &hex!("7f"));
        check_pn(PacketNumber::U16(0x80), &hex!("0080"));
        check_pn(PacketNumber::U16(0x3fff), &hex!("3fff"));
        check_pn(PacketNumber::U32(0x0000_4000), &hex!("0000 4000"));
        check_pn(PacketNumber::U32(0xffff_ffff), &hex!("ffff ffff"));
    }

    #[test]
    fn pn_encode() {
        check_pn(PacketNumber::new(0x10, 0).unwrap(), &hex!("10"));
        check_pn(PacketNumber::new(0x100, 0).unwrap(), &hex!("0100"));
        check_pn(PacketNumber::new(0x10000, 0).unwrap(), &hex!("010000"));
    }

    #[test]
    fn pn_expand_roundtrip() {
        for expected in 0..1024 {
            for actual in expected..1024 {
                assert_eq!(
                    actual,
                    PacketNumber::new(actual, expected)
                        .unwrap()
                        .expand(expected)
                );
            }
        }
    }

    #[test]
    fn header_encoding() {
        use crate::Side;
        use crate::crypto::rustls::{initial_keys, initial_suite};
        use rustls::quic::Version;

        let dcid = ConnectionId::new(&hex!("06b858ec6f80452b"));

        let suite = initial_suite().unwrap();
        let client = initial_keys(Version::V1, dcid, Side::Client, &suite);
        let mut buf = Vec::new();
        let header = Header::Initial(InitialHeader {
            number: PacketNumber::U8(0),
            src_cid: ConnectionId::new(&[]),
            dst_cid: dcid,
            token: Bytes::new(),
            version: crate::DEFAULT_SUPPORTED_VERSIONS[0],
        });
        let encode = header.encode(&mut buf);
        let header_len = buf.len();
        buf.resize(header_len + 16 + client.packet.local.tag_len(), 0);
        encode
            .finish(
                &mut buf,
                &*client.header.local,
                Some((0, &*client.packet.local)),
            )
            .unwrap();

        for byte in &buf {
            print!("{byte:02x}");
        }
        println!();
        assert_eq!(
            buf[..],
            hex!(
                "c8000000010806b858ec6f80452b00004021be
                 3ef50807b84191a196f760a6dad1e9d1c430c48952cba0148250c21c0a6a70e1"
            )[..]
        );

        let server = initial_keys(Version::V1, dcid, Side::Server, &suite);
        let supported_versions = crate::DEFAULT_SUPPORTED_VERSIONS.to_vec();
        let decode = PartialDecode::new(
            buf.as_slice().into(),
            &FixedLengthConnectionIdParser::new(0),
            &supported_versions,
            false,
        )
        .unwrap()
        .0;
        let mut packet = decode.finish(Some(&*server.header.remote)).unwrap();
        assert_eq!(
            packet.header_data[..],
            hex!("c0000000010806b858ec6f80452b0000402100")[..]
        );
        server
            .packet
            .remote
            .decrypt(0, &packet.header_data, &mut packet.payload)
            .unwrap();
        assert_eq!(packet.payload[..], [0; 16]);
        match packet.header {
            Header::Initial(InitialHeader {
                number: PacketNumber::U8(0),
                ..
            }) => {}
            _ => {
                panic!("unexpected header {:?}", packet.header);
            }
        }
    }
}
