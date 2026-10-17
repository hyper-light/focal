//! A message on a stream: a fixed prefix read into a fixed buffer, then its head, then its body
//! and the body's checksum (node.md §3.2; the audit's §11.8).
//!
//! ```text
//! prefix (20 bytes): version u8 | flags u8 | kind u16 | head length u32 | body length u64 |
//!                    CRC-32C of the prefix's first 16 bytes and the head
//! head:              head length bytes
//! body:              body length bytes, then the CRC-32C of the body (4 bytes), if flags say so
//! ```
//!
//! The receiver reads the prefix alone, checks the lengths against its own bounds for the class the
//! kind and the sender's role give, and only then takes a reservation for the head and reads it
//! (focal `frame.rs`, `read_frame_header`: "Read it first when the caller must acquire a buffer
//! before consuming any body bytes"). Every integer is big-endian (RFC 9000 §1.3's network order).
//! QUIC's AEAD already authenticates every packet; the CRC-32C is mantle's rule that every network
//! payload carries a checksum verified on read (mantle CLAUDE.md §6), and it catches a bug in either
//! side's framing that authentication cannot.

use crate::Refusal;

/// The prefix's length in bytes: 1 + 1 + 2 + 4 + 8 + 4, the fields above.
pub(crate) const PREFIX_BYTES: usize = 20;
/// The part of the prefix its checksum covers, with the head: every field but the checksum.
const COVERED_BYTES: usize = 16;
/// The body's checksum, a CRC-32C.
pub(crate) const TRAILER_BYTES: usize = 4;
/// This layout's version. A prefix of another version is corrupt to this side.
const VERSION: u8 = 1;
/// The flag that says a body follows the head.
const FLAG_BODY: u8 = 1;

/// A message's prefix, decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Prefix {
    /// The project's code for the message's kind ([`crate::Classes::kind_code`]); a reply's is
    /// zero.
    pub(crate) kind: u16,
    /// The head's length in bytes.
    pub(crate) head: u32,
    /// The body's length in bytes, if a body follows.
    pub(crate) body: Option<u64>,
    /// The checksum of the covered prefix and the head.
    checksum: u32,
}

impl Prefix {
    /// The prefix of a message of `kind` carrying `head` and a body of `body` bytes, if any.
    pub(crate) fn encode(
        kind: u16,
        head: &[u8],
        body: Option<u64>,
    ) -> Result<[u8; PREFIX_BYTES], Refusal> {
        Self::encode_covering(kind, head.len(), head, body)
    }

    /// The prefix of a message of `kind` whose head is `length` bytes, its checksum covering the
    /// prefix and only `covered`, the head's first bytes: a lane frame that carries its own
    /// checksum of the rest ([`Embedded`]).
    pub(crate) fn encode_covering(
        kind: u16,
        length: usize,
        covered: &[u8],
        body: Option<u64>,
    ) -> Result<[u8; PREFIX_BYTES], Refusal> {
        let head = covered;
        let length = u32::try_from(length).map_err(|_| Refusal::FrameBound)?;
        let mut bytes = [0u8; PREFIX_BYTES];
        let flags = if body.is_some() { FLAG_BODY } else { 0 };
        let fields = [VERSION, flags];
        write(&mut bytes, 0, &fields)?;
        write(&mut bytes, 2, &kind.to_be_bytes())?;
        write(&mut bytes, 4, &length.to_be_bytes())?;
        write(&mut bytes, 8, &body.unwrap_or(0).to_be_bytes())?;
        let covered = bytes.get(..COVERED_BYTES).ok_or(Refusal::Corrupt)?;
        let checksum = crate::crc::crc32c_append(crate::crc::crc32c(covered), head);
        write(&mut bytes, COVERED_BYTES, &checksum.to_be_bytes())?;
        Ok(bytes)
    }

    /// The prefix in `bytes`, its fields checked for form; its checksum is checked with the head
    /// ([`Prefix::verify`]).
    pub(crate) fn decode(bytes: &[u8; PREFIX_BYTES]) -> Result<Self, Refusal> {
        let [version, flags, ..] = *bytes;
        if version != VERSION || flags & !FLAG_BODY != 0 {
            return Err(Refusal::Corrupt);
        }
        let kind = u16::from_be_bytes(read(bytes, 2)?);
        let head = u32::from_be_bytes(read(bytes, 4)?);
        let length = u64::from_be_bytes(read(bytes, 8)?);
        let checksum = u32::from_be_bytes(read(bytes, COVERED_BYTES)?);
        let body = if flags & FLAG_BODY != 0 {
            Some(length)
        } else if length == 0 {
            None
        } else {
            return Err(Refusal::Corrupt);
        };
        Ok(Self {
            kind,
            head,
            body,
            checksum,
        })
    }

    /// Whether `head` is the head this prefix was written with: its checksum over `bytes`, the
    /// prefix as received, and the head.
    pub(crate) fn verify(&self, bytes: &[u8; PREFIX_BYTES], head: &[u8]) -> Result<(), Refusal> {
        let covered = bytes.get(..COVERED_BYTES).ok_or(Refusal::Corrupt)?;
        let checksum = crate::crc::crc32c_append(crate::crc::crc32c(covered), head);
        if checksum == self.checksum {
            Ok(())
        } else {
            Err(Refusal::Corrupt)
        }
    }

    /// The whole message's length, head and body, as its class's frame bound counts it.
    pub(crate) fn length(&self) -> u64 {
        u64::from(self.head).saturating_add(self.body.unwrap_or(0))
    }
}

fn write(bytes: &mut [u8], at: usize, field: &[u8]) -> Result<(), Refusal> {
    let end = at.checked_add(field.len()).ok_or(Refusal::Corrupt)?;
    bytes
        .get_mut(at..end)
        .ok_or(Refusal::Corrupt)?
        .copy_from_slice(field);
    Ok(())
}

fn read<const N: usize>(bytes: &[u8], at: usize) -> Result<[u8; N], Refusal> {
    let end = at.checked_add(N).ok_or(Refusal::Corrupt)?;
    bytes
        .get(at..end)
        .and_then(|field| field.try_into().ok())
        .ok_or(Refusal::Corrupt)
}

/// A lane frame's own CRC-32C (Castagnoli), which the protocol above it checks end to end (a bus
/// batch's producer checksum; Kafka's record batch CRC, KIP-98): of the frame's bytes from
/// `covered_from` to its end, stated little-endian at `at..at + 4`, which lies before
/// `covered_from`. The transport's prefix checksum then covers the frame only before
/// `covered_from`, so each byte is summed once, and the receiver verifies the frame's own checksum
/// where it would have summed the same bytes again: a bus node summed each batch three times, its
/// prefix's on receipt, the batch's, and its prefix's again to deliver it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Embedded {
    /// Where the bytes the frame's own checksum covers begin.
    pub covered_from: usize,
    /// Where the four bytes stating it are.
    pub at: usize,
}

impl Embedded {
    /// The part of `frame` the transport's prefix checksum covers: what the frame's own does not.
    pub(crate) fn head<'a>(&self, frame: &'a [u8]) -> Result<&'a [u8], Refusal> {
        let stated_end = self.at.checked_add(TRAILER_BYTES).ok_or(Refusal::Corrupt)?;
        if stated_end > self.covered_from {
            return Err(Refusal::Corrupt);
        }
        frame.get(..self.covered_from).ok_or(Refusal::Corrupt)
    }

    /// Whether `frame`'s own checksum is that of its bytes from `covered_from`: what a receiver of
    /// the frame's kind checks, which a protocol declaring one may check its own layout against.
    pub fn verify(&self, frame: &[u8]) -> Result<(), Refusal> {
        let end = self.at.checked_add(TRAILER_BYTES).ok_or(Refusal::Corrupt)?;
        let stated = frame
            .get(self.at..end)
            .and_then(|field| <[u8; TRAILER_BYTES]>::try_from(field).ok())
            .map(u32::from_le_bytes)
            .ok_or(Refusal::Corrupt)?;
        let covered = frame.get(self.covered_from..).ok_or(Refusal::Corrupt)?;
        if crate::crc::crc32c(covered) == stated {
            Ok(())
        } else {
            Err(Refusal::Corrupt)
        }
    }
}

/// A body's running checksum, kept as its bytes pass in either direction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BodySum(u32);

impl BodySum {
    /// The checksum with `bytes` folded in.
    pub(crate) fn fold(&mut self, bytes: &[u8]) {
        self.0 = crate::crc::crc32c_append(self.0, bytes);
    }
    /// The trailer that ends a body with this checksum.
    pub(crate) fn trailer(self) -> [u8; TRAILER_BYTES] {
        self.0.to_be_bytes()
    }
    /// Whether `trailer` is this checksum's.
    pub(crate) fn verify(self, trailer: [u8; TRAILER_BYTES]) -> Result<(), Refusal> {
        if u32::from_be_bytes(trailer) == self.0 {
            Ok(())
        } else {
            Err(Refusal::Corrupt)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prefix_reads_back_and_its_head_verifies() {
        let head = b"the head";
        let bytes = Prefix::encode(7, head, Some(1 << 40)).unwrap();
        let prefix = Prefix::decode(&bytes).unwrap();
        assert_eq!(
            (prefix.kind, prefix.head, prefix.body),
            (7, 8, Some(1 << 40))
        );
        assert_eq!(prefix.length(), (1 << 40) + 8);
        prefix.verify(&bytes, head).unwrap();
        assert_eq!(prefix.verify(&bytes, b"another!"), Err(Refusal::Corrupt));
        let bare = Prefix::decode(&Prefix::encode(1, b"", None).unwrap()).unwrap();
        assert_eq!(bare.body, None);
    }

    #[test]
    fn a_malformed_prefix_is_corrupt() {
        let good = Prefix::encode(7, b"head", Some(3)).unwrap();
        let mut version = good;
        version[0] = 2;
        assert_eq!(Prefix::decode(&version), Err(Refusal::Corrupt));
        let mut flags = good;
        flags[1] = 0x80;
        assert_eq!(Prefix::decode(&flags), Err(Refusal::Corrupt));
        // No body, yet a body length: one of the two lies.
        let mut lying = Prefix::encode(7, b"head", None).unwrap();
        lying[15] = 1;
        assert_eq!(Prefix::decode(&lying), Err(Refusal::Corrupt));
        // A flipped bit in a covered field fails the checksum.
        let mut flipped = good;
        flipped[3] ^= 1;
        let prefix = Prefix::decode(&flipped).unwrap();
        assert_eq!(prefix.verify(&flipped, b"head"), Err(Refusal::Corrupt));
    }

    /// A frame of its own checksum: the prefix covers what that does not, the frame's checksum
    /// the rest, and a changed byte anywhere is found by one of them.
    #[test]
    fn a_frame_s_own_checksum_and_its_prefix_cover_every_byte_once() {
        let embedded = Embedded {
            covered_from: 8,
            at: 0,
        };
        let mut frame = vec![0u8; 8];
        frame.extend((0..100u8).map(|n| n.wrapping_mul(7)));
        let sum = crate::crc::crc32c(&frame[8..]);
        frame[..4].copy_from_slice(&sum.to_le_bytes());
        frame[4..8].copy_from_slice(b"head");
        let bytes =
            Prefix::encode_covering(5, frame.len(), embedded.head(&frame).unwrap(), None).unwrap();
        let prefix = Prefix::decode(&bytes).unwrap();
        assert_eq!(prefix.length(), frame.len() as u64);
        prefix
            .verify(&bytes, embedded.head(&frame).unwrap())
            .unwrap();
        embedded.verify(&frame).unwrap();
        // A covered byte the frame's own checksum finds.
        let mut body = frame.clone();
        body[50] ^= 1;
        assert_eq!(embedded.verify(&body), Err(Refusal::Corrupt));
        prefix
            .verify(&bytes, embedded.head(&body).unwrap())
            .unwrap();
        // A byte before the covered span the prefix's finds, the stated checksum among them.
        for at in [2, 6] {
            let mut head = frame.clone();
            head[at] ^= 1;
            assert_eq!(
                prefix.verify(&bytes, embedded.head(&head).unwrap()),
                Err(Refusal::Corrupt)
            );
        }
        // A frame shorter than its covered span's start, or a stated field past it, is corrupt.
        assert_eq!(embedded.head(&frame[..7]), Err(Refusal::Corrupt));
        let past = Embedded {
            covered_from: 4,
            at: 2,
        };
        assert_eq!(past.head(&frame), Err(Refusal::Corrupt));
    }

    #[test]
    fn a_body_checksum_is_the_same_however_the_body_is_cut() {
        let body: Vec<u8> = (0..10_000u32).map(|n| (n % 251) as u8).collect();
        let mut whole = BodySum::default();
        whole.fold(&body);
        let mut cut = BodySum::default();
        for piece in body.chunks(97) {
            cut.fold(piece);
        }
        assert_eq!(whole, cut);
        cut.verify(whole.trailer()).unwrap();
        assert_eq!(cut.verify([0; 4]), Err(Refusal::Corrupt));
    }
}
