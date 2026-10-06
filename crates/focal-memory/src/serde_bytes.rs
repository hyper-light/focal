//! Byte payloads serialized as bytes, decoded in one exact reservation.
//!
//! A `Vec<u8>` field derives as a sequence: written as varint(len) followed
//! by every byte, and read back one byte at a time into a vector that
//! guesses its size from the length and grows. `serialize_bytes` writes the
//! same varint(len) followed by the same bytes, so a field annotated
//! `#[serde(with = "focal_memory::serde_bytes")]` keeps every wire and
//! durable byte, and is read back with one exact reservation of what the
//! frame holds (the length is checked against the input before anything is
//! reserved), which fails as a decode error and never aborts.
use core::fmt;
use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserializer, Serializer};

pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_bytes(bytes)
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    deserializer.deserialize_byte_buf(Bytes)
}

struct Bytes;
impl<'de> Visitor<'de> for Bytes {
    type Value = Vec<u8>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bytes")
    }
    fn visit_bytes<E: Error>(self, bytes: &[u8]) -> Result<Vec<u8>, E> {
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(bytes.len())
            .map_err(|_| E::custom("no memory for the bytes"))?;
        owned.extend_from_slice(bytes);
        Ok(owned)
    }
    fn visit_byte_buf<E: Error>(self, bytes: Vec<u8>) -> Result<Vec<u8>, E> {
        Ok(bytes)
    }
    /// A format that gives the bytes as a sequence (a JSON array): read as
    /// they come, each reserved before it is kept, so a length the format
    /// claims is never trusted.
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<u8>, A::Error> {
        let mut owned = Vec::new();
        while let Some(byte) = sequence.next_element::<u8>()? {
            owned
                .try_reserve(1)
                .map_err(|_| A::Error::custom("no memory for the bytes"))?;
            owned.push(byte);
        }
        Ok(owned)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct AsBytes {
        #[serde(with = "super")]
        payload: Vec<u8>,
        after: u32,
    }
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct AsSequence {
        payload: Vec<u8>,
        after: u32,
    }

    /// Every length across the varint boundaries encodes to the same bytes
    /// either way and decodes from the other's bytes.
    #[test]
    fn bytes_and_a_sequence_of_bytes_are_the_same_bytes() {
        for len in [0usize, 1, 127, 128, 300, 16_383, 16_384, 70_000] {
            let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let bytes = postcard::to_stdvec(&AsBytes {
                payload: payload.clone(),
                after: 7,
            })
            .unwrap();
            let sequence = postcard::to_stdvec(&AsSequence {
                payload: payload.clone(),
                after: 7,
            })
            .unwrap();
            assert_eq!(bytes, sequence, "length {len}");
            let decoded: AsBytes = postcard::from_bytes(&sequence).unwrap();
            assert_eq!(decoded.payload, payload);
            assert_eq!(decoded.after, 7);
            let decoded: AsSequence = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.payload, payload);
        }
    }

    /// A length beyond the input is refused before anything is reserved.
    #[test]
    fn a_length_beyond_the_input_is_a_decode_error() {
        // varint 2 MiB, then three bytes.
        let input = [0x80u8, 0x80, 0x80, 0x01, 1, 2, 3];
        assert!(postcard::from_bytes::<AsBytes>(&input).is_err());
    }

    /// JSON gives the bytes as a sequence; the field reads it all the same.
    #[test]
    fn a_sequence_from_a_text_format_is_read_as_bytes() {
        let decoded: AsBytes = serde_json::from_str(r#"{"payload":[1,2,3],"after":9}"#).unwrap();
        assert_eq!(decoded.payload, vec![1, 2, 3]);
        assert_eq!(
            serde_json::to_string(&decoded).unwrap(),
            r#"{"payload":[1,2,3],"after":9}"#
        );
    }
}
