//! Internal historical storage codecs, independent of the live model's Serde
//! implementations. These preserve Postcard V1 fields, numeric codes and enum
//! ordinals. They do not validate present-day admission or rewrite stored hashes.
//!
//! Public visibility allows the Core to use the same frozen nested encoding
//! without a dependency cycle. This is not a new wire or authored-input format.
//! Borrowed views serialize existing allocations; decoding constructs each final
//! row directly. Prepared inputs and canonical command identity use these same
//! frozen representations. Output and managed receipt codecs are included here;
//! Ledger and its dependencies own the surrounding Session envelopes. Historical
//! execution selection remains an independent contract of the Core.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Explicit historical representation for a supported value.
pub trait V1: Sized {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error>;
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error>;

    /// Postcard sequences have one count followed by their element bytes. The
    /// u8 implementation can emit an existing byte slice in bulk with that same
    /// representation; every other element keeps its explicit nested codec.
    #[inline]
    fn serialize_sequence_v1<S: Serializer>(
        values: &[Self],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(values.len()))?;
        for value in values {
            seq.serialize_element(&Ref(value))?;
        }
        seq.end()
    }
}

/// A borrowed historical serializer; no clone or second model is retained.
pub struct Ref<'a, T: V1>(pub &'a T);

impl<T: V1> Serialize for Ref<'_, T> {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize_v1(serializer)
    }
}

/// Borrowed historical sequence; encodes the same count/element bytes as Vec.
pub struct Sequence<'a, T: V1>(pub &'a [T]);
impl<T: V1> Serialize for Sequence<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        T::serialize_sequence_v1(self.0, serializer)
    }
}

/// Exact historical Postcard bytes of one value, for retention verbatim.
pub fn encode<T: V1>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_stdvec(&Ref(value))
}

/// Decode exact historical Postcard bytes; trailing bytes are an error.
pub fn decode<T: V1>(bytes: &[u8]) -> Result<T, postcard::Error> {
    let mut deserializer = postcard::Deserializer::from_bytes(bytes);
    let value = T::deserialize_v1(&mut deserializer)?;
    let rest = deserializer.finalize()?;
    if !rest.is_empty() {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    Ok(value)
}

/// Raw historical Postcard BLAKE3 commitment, without a domain/header prefix.
/// Streams into the existing bounded digest buffer without a payload allocation.
pub fn hash<T: V1>(value: &T) -> Result<crate::ContentHash, postcard::Error> {
    postcard::serialize_with_flavor(&Ref(value), crate::digest::Digest::new())
}

/// A decoded historical value, already converted into its final model type.
pub struct Value<T: V1>(pub T);

impl<'de, T: V1> Deserialize<'de> for Value<T> {
    #[inline]
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize_v1(deserializer).map(Self)
    }
}

impl<T: V1> Serialize for Value<T> {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize_v1(serializer)
    }
}

// Each invocation is an explicit frozen field list. Exhaustive destructuring
// makes live model additions demand an intentional historical conversion.
macro_rules! v1_struct {
    ($name:ident { $($field:ident: $ty:ty),+ $(,)? }) => {
        impl $crate::durable_v1::V1 for $name {
            #[inline]
            fn serialize_v1<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                #[derive(serde::Serialize)]
                struct Record<'a> {
                    $($field: $crate::durable_v1::Ref<'a, $ty>),+
                }
                let Self { $($field),+ } = self;
                serde::Serialize::serialize(&Record {
                    $($field: $crate::durable_v1::Ref($field)),+
                }, serializer)
            }
            fn deserialize_v1<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(serde::Deserialize)]
                struct Record {
                    $($field: $crate::durable_v1::Value<$ty>),+
                }
                let Record { $($field),+ } = serde::Deserialize::deserialize(deserializer)?;
                Ok(Self { $($field: $field.0),+ })
            }
        }
    };
}

mod collections;
mod commands;
mod inputs;
mod leaves;
mod lifecycle;
mod managed;
mod objects;
mod outputs;
mod vocabulary;

pub(crate) use commands::command_code;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod outputs_tests;
