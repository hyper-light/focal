use std::{convert::TryInto, fmt};

use bytes::{Buf, BufMut};
use thiserror::Error;

use crate::coding::{self, Codec, UnexpectedEnd};

#[cfg(feature = "arbitrary")]
use arbitrary::Arbitrary;

/// An integer less than 2^62
///
/// Values of this type are suitable for encoding as QUIC variable-length integer.
// It would be neat if we could express to Rust that the top two bits are available for use as enum
// discriminants
#[derive(Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VarInt(pub(crate) u64);

impl VarInt {
    /// The largest representable value
    pub const MAX: Self = Self((1 << 62) - 1);
    /// The largest encoded value length
    pub const MAX_SIZE: usize = 8;

    /// Construct a `VarInt` infallibly
    pub const fn from_u32(x: u32) -> Self {
        Self(x as u64)
    }

    /// Succeeds iff `x` < 2^62
    pub fn from_u64(x: u64) -> Result<Self, VarIntBoundsExceeded> {
        if x < 2u64.pow(62) {
            Ok(Self(x))
        } else {
            Err(VarIntBoundsExceeded)
        }
    }

    /// Extract the integer value
    pub const fn into_inner(self) -> u64 {
        self.0
    }

    /// Compute the number of bytes needed to encode this value
    pub(crate) const fn size(self) -> usize {
        Self::size_of(self.0)
    }

    /// The number of bytes the variable-length encoding of `x` takes (RFC 9000 §16): 8 for
    /// anything at or above 2^30. A `VarInt` is below 2^62; callers sizing a stream offset,
    /// length or count know it is too (upstream built an unchecked `VarInt` for this).
    pub(crate) const fn size_of(x: u64) -> usize {
        if x < 2u64.pow(6) {
            1
        } else if x < 2u64.pow(14) {
            2
        } else if x < 2u64.pow(30) {
            4
        } else {
            8
        }
    }
}

impl From<VarInt> for u64 {
    fn from(x: VarInt) -> Self {
        x.0
    }
}

impl From<u8> for VarInt {
    fn from(x: u8) -> Self {
        Self(x.into())
    }
}

impl From<u16> for VarInt {
    fn from(x: u16) -> Self {
        Self(x.into())
    }
}

impl From<u32> for VarInt {
    fn from(x: u32) -> Self {
        Self(x.into())
    }
}

impl std::convert::TryFrom<u64> for VarInt {
    type Error = VarIntBoundsExceeded;
    /// Succeeds iff `x` < 2^62
    fn try_from(x: u64) -> Result<Self, VarIntBoundsExceeded> {
        Self::from_u64(x)
    }
}

impl std::convert::TryFrom<u128> for VarInt {
    type Error = VarIntBoundsExceeded;
    /// Succeeds iff `x` < 2^62
    fn try_from(x: u128) -> Result<Self, VarIntBoundsExceeded> {
        Self::from_u64(x.try_into().map_err(|_| VarIntBoundsExceeded)?)
    }
}

impl std::convert::TryFrom<usize> for VarInt {
    type Error = VarIntBoundsExceeded;
    /// Succeeds iff `x` < 2^62
    fn try_from(x: usize) -> Result<Self, VarIntBoundsExceeded> {
        Self::try_from(x as u64)
    }
}

impl fmt::Debug for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(feature = "arbitrary")]
impl<'arbitrary> Arbitrary<'arbitrary> for VarInt {
    fn arbitrary(u: &mut arbitrary::Unstructured<'arbitrary>) -> arbitrary::Result<Self> {
        Ok(Self(u.int_in_range(0..=Self::MAX.0)?))
    }
}

/// Error returned when constructing a `VarInt` from a value >= 2^62
#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
#[error("value too large for varint encoding")]
pub struct VarIntBoundsExceeded;

impl Codec for VarInt {
    fn decode<B: Buf>(r: &mut B) -> coding::Result<Self> {
        if !r.has_remaining() {
            return Err(UnexpectedEnd);
        }
        let first = r.get_u8();
        // The two most significant bits give the length (RFC 9000 §16); the rest is the value's
        // most significant bits.
        let tag = first >> 6;
        let top = first & 0b0011_1111;
        let x = match tag {
            0b00 => u64::from(top),
            0b01 => {
                let mut rest = [0; 1];
                read_exact(r, &mut rest)?;
                let [b1] = rest;
                u64::from(u16::from_be_bytes([top, b1]))
            }
            0b10 => {
                let mut rest = [0; 3];
                read_exact(r, &mut rest)?;
                let [b1, b2, b3] = rest;
                u64::from(u32::from_be_bytes([top, b1, b2, b3]))
            }
            // 0b11: a two-bit tag has no other value.
            _ => {
                let mut rest = [0; 7];
                read_exact(r, &mut rest)?;
                let [b1, b2, b3, b4, b5, b6, b7] = rest;
                u64::from_be_bytes([top, b1, b2, b3, b4, b5, b6, b7])
            }
        };
        Ok(Self(x))
    }

    fn encode<B: BufMut>(&self, w: &mut B) {
        // Within each branch the value fits the bytes taken from it: a `VarInt` is below 2^62.
        let [b7, b6, b5, b4, b3, b2, b1, b0] = self.0.to_be_bytes();
        match self.size() {
            1 => w.put_u8(b0),
            2 => w.put_slice(&[0b0100_0000 | b1, b0]),
            4 => w.put_slice(&[0b1000_0000 | b3, b2, b1, b0]),
            _ => w.put_slice(&[0b1100_0000 | b7, b6, b5, b4, b3, b2, b1, b0]),
        }
    }
}

/// Fills `out` from `r`, or fails if `r` holds fewer bytes.
fn read_exact<B: Buf>(r: &mut B, out: &mut [u8]) -> coding::Result<()> {
    if r.remaining() < out.len() {
        return Err(UnexpectedEnd);
    }
    r.copy_to_slice(out);
    Ok(())
}
