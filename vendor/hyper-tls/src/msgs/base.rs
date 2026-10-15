use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;

use pki_types::CertificateDer;
use zeroize::Zeroize;

use crate::error::InvalidMessage;
use crate::msgs::codec;
use crate::msgs::codec::{Codec, Reader};

/// An externally length'd payload
#[derive(Clone, Eq, PartialEq)]
pub enum Payload<'a> {
    /// Bytes borrowed from the message being read
    Borrowed(&'a [u8]),
    /// Bytes the payload owns
    Owned(Vec<u8>),
}

impl<'a> Codec<'a> for Payload<'a> {
    fn encode(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(self.bytes());
    }

    fn read(r: &mut Reader<'a>) -> Result<Self, InvalidMessage> {
        Ok(Self::read(r))
    }
}

impl<'a> Payload<'a> {
    /// The payload's bytes
    pub fn bytes(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }

    /// The payload, owning its bytes
    pub fn into_owned(self) -> Payload<'static> {
        Payload::Owned(self.into_vec())
    }

    /// The payload's bytes, owned
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Borrowed(bytes) => bytes.to_vec(),
            Self::Owned(bytes) => bytes,
        }
    }

    /// Reads the rest of `r` as a payload
    pub fn read(r: &mut Reader<'a>) -> Self {
        Self::Borrowed(r.rest())
    }
}

impl Payload<'static> {
    /// A payload owning `bytes`
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Owned(bytes.into())
    }

    /// A payload of no bytes
    pub fn empty() -> Self {
        Self::Borrowed(&[])
    }
}

impl<'a> Codec<'a> for CertificateDer<'a> {
    fn encode(&self, bytes: &mut Vec<u8>) {
        codec::low_u24(self.as_ref().len()).encode(bytes);
        bytes.extend(self.as_ref());
    }

    fn read(r: &mut Reader<'a>) -> Result<Self, InvalidMessage> {
        let len = codec::u24::read(r)?.0 as usize;
        let mut sub = r.sub(len)?;
        let body = sub.rest();
        Ok(Self::from(body))
    }
}

impl fmt::Debug for Payload<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        hex(f, self.bytes())
    }
}

/// An arbitrary, unknown-content, u24-length-prefixed payload
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct PayloadU24<'a, C: Cardinality = MaybeEmpty>(
    pub(crate) Payload<'a>,
    PhantomData<C>,
);

impl<C: Cardinality> PayloadU24<'_, C> {
    pub(crate) fn into_owned(self) -> PayloadU24<'static, C> {
        PayloadU24(self.0.into_owned(), PhantomData)
    }
}

impl<'a, C: Cardinality> Codec<'a> for PayloadU24<'a, C> {
    fn encode(&self, bytes: &mut Vec<u8>) {
        let inner = self.0.bytes();
        codec::low_u24(inner.len()).encode(bytes);
        bytes.extend_from_slice(inner);
    }

    fn read(r: &mut Reader<'a>) -> Result<Self, InvalidMessage> {
        let len = codec::u24::read(r)?.0 as usize;
        if len < C::MIN {
            return Err(InvalidMessage::IllegalEmptyList("PayloadU24"));
        }
        let mut sub = r.sub(len)?;
        Ok(Self(Payload::read(&mut sub), PhantomData))
    }
}

impl<'a, C: Cardinality> From<Payload<'a>> for PayloadU24<'a, C> {
    fn from(value: Payload<'a>) -> Self {
        Self(value, PhantomData)
    }
}

impl<C: Cardinality> AsRef<[u8]> for PayloadU24<'_, C> {
    fn as_ref(&self) -> &[u8] {
        self.0.bytes()
    }
}

impl<C: Cardinality> fmt::Debug for PayloadU24<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// An arbitrary, unknown-content, u16-length-prefixed payload
///
/// The `C` type parameter controls whether decoded values may
/// be empty.
#[derive(Clone, Eq, PartialEq)]
pub struct PayloadU16<C: Cardinality = MaybeEmpty>(pub(crate) Vec<u8>, PhantomData<C>);

impl<C: Cardinality> PayloadU16<C> {
    /// A payload owning `bytes`
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes, PhantomData)
    }
}

impl PayloadU16<MaybeEmpty> {
    pub(crate) fn empty() -> Self {
        Self::new(Vec::new())
    }
}

impl<C: Cardinality> Codec<'_> for PayloadU16<C> {
    fn encode(&self, bytes: &mut Vec<u8>) {
        codec::low_u16(self.0.len()).encode(bytes);
        bytes.extend_from_slice(&self.0);
    }

    fn read(r: &mut Reader<'_>) -> Result<Self, InvalidMessage> {
        let len = u16::read(r)? as usize;
        if len < C::MIN {
            return Err(InvalidMessage::IllegalEmptyValue);
        }
        let mut sub = r.sub(len)?;
        let body = sub.rest().to_vec();
        Ok(Self(body, PhantomData))
    }
}

impl<C: Cardinality> fmt::Debug for PayloadU16<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        hex(f, &self.0)
    }
}

/// An arbitrary, unknown-content, u8-length-prefixed payload
///
/// `C` controls the minimum length accepted when decoding.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct PayloadU8<C: Cardinality = MaybeEmpty>(pub(crate) Vec<u8>, PhantomData<C>);

impl<C: Cardinality> PayloadU8<C> {
    pub(crate) fn encode_slice(slice: &[u8], bytes: &mut Vec<u8>) {
        codec::low_u8(slice.len()).encode(bytes);
        bytes.extend_from_slice(slice);
    }

    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self(bytes, PhantomData)
    }
}

impl PayloadU8<MaybeEmpty> {
    pub(crate) fn empty() -> Self {
        Self(Vec::new(), PhantomData)
    }
}

impl<C: Cardinality> Codec<'_> for PayloadU8<C> {
    fn encode(&self, bytes: &mut Vec<u8>) {
        codec::low_u8(self.0.len()).encode(bytes);
        bytes.extend_from_slice(&self.0);
    }

    fn read(r: &mut Reader<'_>) -> Result<Self, InvalidMessage> {
        let len = u8::read(r)? as usize;
        if len < C::MIN {
            return Err(InvalidMessage::IllegalEmptyValue);
        }
        let mut sub = r.sub(len)?;
        let body = sub.rest().to_vec();
        Ok(Self(body, PhantomData))
    }
}

impl<C: Cardinality> Zeroize for PayloadU8<C> {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl<C: Cardinality> fmt::Debug for PayloadU8<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        hex(f, &self.0)
    }
}

pub trait Cardinality: Clone + Eq + PartialEq {
    /// The fewest bytes a decoded value may hold.
    const MIN: usize;
}

#[derive(Clone, Eq, PartialEq)]
pub struct MaybeEmpty;

impl Cardinality for MaybeEmpty {
    /// Zero: the grammar's lower bound is 0.
    const MIN: usize = 0;
}

#[derive(Clone, Eq, PartialEq)]
pub struct NonEmpty;

impl Cardinality for NonEmpty {
    /// One: the grammar's lower bound is not 0.
    const MIN: usize = 1;
}

// Format an iterator of u8 into a hex string
pub(super) fn hex<'a>(
    f: &mut fmt::Formatter<'_>,
    payload: impl IntoIterator<Item = &'a u8>,
) -> fmt::Result {
    for b in payload {
        write!(f, "{b:02x}")?;
    }
    Ok(())
}
