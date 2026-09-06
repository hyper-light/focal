#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! Session-owned range layout, fenced transfers, and bounded RAM replicas.
//! The session log makes each decision once; this crate never creates an
//! independent range or cross-session transaction decision.

mod coordinator;
mod map;
mod pins;
mod replica;
mod types;
pub use coordinator::*;
pub use map::*;
pub use pins::*;
pub use replica::*;
pub use types::*;

use focal_memory::MemoryError;
use focal_model::ContentHash;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeError {
    Invalid(&'static str),
    Capacity,
    Overflow,
    WrongLedger,
    WrongOwner,
    StaleEpoch,
    Generation,
    Gap,
    Overlap,
    Missing,
    Conflict,
    Phase,
    NotReady,
    Unverified,
    Checksum,
    StalePreparation,
    RetryTooOld,
    ReadTooOld,
    Expired,
    ClockRegression,
    Pinned,
    Sealed,
    Memory(MemoryError),
    Codec,
}
impl From<MemoryError> for RangeError {
    fn from(value: MemoryError) -> Self {
        Self::Memory(value)
    }
}
impl From<postcard::Error> for RangeError {
    fn from(_: postcard::Error) -> Self {
        Self::Codec
    }
}
fn add(a: usize, b: usize) -> Result<usize, RangeError> {
    a.checked_add(b).ok_or(RangeError::Overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, RangeError> {
    a.checked_mul(b).ok_or(RangeError::Overflow)
}
fn row<T>() -> usize {
    size_of::<T>().saturating_add(32).saturating_mul(16)
}
fn nonzero(hash: ContentHash) -> bool {
    hash.0 != [0; 32]
}
fn digest<T: Serialize>(domain: &'static str, value: &T) -> Result<ContentHash, RangeError> {
    struct Sink(blake3::Hasher);
    impl postcard::ser_flavors::Flavor for Sink {
        type Output = ContentHash;
        fn try_push(&mut self, byte: u8) -> postcard::Result<()> {
            self.0.update(&[byte]);
            Ok(())
        }
        fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
            self.0.update(bytes);
            Ok(())
        }
        fn finalize(self) -> postcard::Result<Self::Output> {
            Ok(ContentHash(*self.0.finalize().as_bytes()))
        }
    }
    Ok(postcard::serialize_with_flavor(
        value,
        Sink(blake3::Hasher::new_derive_key(domain)),
    )?)
}
fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, RangeError> {
    let len = postcard::experimental::serialized_size(value)?;
    if len > limit {
        return Err(RangeError::Capacity);
    }
    let mut bytes = vec![0; len];
    postcard::to_slice(value, &mut bytes)?;
    Ok(bytes)
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T, RangeError> {
    if bytes.len() > limit {
        return Err(RangeError::Capacity);
    }
    let (value, rest) = postcard::take_from_bytes(bytes)?;
    if !rest.is_empty() {
        return Err(RangeError::Codec);
    }
    Ok(value)
}
