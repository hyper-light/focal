//! Stream exact serialized bytes into a digest without an intermediate payload.
use crate::ContentHash;

const BUFFER_BYTES: usize = 1024;

pub(crate) struct Digest {
    hasher: blake3::Hasher,
    buffer: [u8; BUFFER_BYTES],
    used: usize,
}

impl Digest {
    pub(crate) fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            buffer: [0; BUFFER_BYTES],
            used: 0,
        }
    }

    fn flush(&mut self) -> Result<(), postcard::Error> {
        let bytes = self
            .buffer
            .get(..self.used)
            .ok_or(postcard::Error::SerializeBufferFull)?;
        self.hasher.update(bytes);
        self.used = 0;
        Ok(())
    }

    /// Preserve byte order across buffered scalar writes and already contiguous
    /// chunks. Large chunks go directly to BLAKE3 without another payload copy.
    pub(crate) fn update(&mut self, bytes: &[u8]) -> Result<(), postcard::Error> {
        if self.used != 0 {
            self.flush()?;
        }
        self.hasher.update(bytes);
        Ok(())
    }
}

impl postcard::ser_flavors::Flavor for Digest {
    type Output = ContentHash;

    fn try_push(&mut self, byte: u8) -> Result<(), postcard::Error> {
        let slot = self
            .buffer
            .get_mut(self.used)
            .ok_or(postcard::Error::SerializeBufferFull)?;
        *slot = byte;
        self.used = self
            .used
            .checked_add(1)
            .ok_or(postcard::Error::SerializeBufferFull)?;
        if self.used == BUFFER_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn try_extend(&mut self, bytes: &[u8]) -> Result<(), postcard::Error> {
        self.update(bytes)
    }

    fn finalize(mut self) -> Result<Self::Output, postcard::Error> {
        self.flush()?;
        Ok(ContentHash(*self.hasher.finalize().as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use postcard::ser_flavors::Flavor;

    #[test]
    fn mixed_scalar_and_bulk_writes_preserve_order_at_every_buffer_boundary() {
        let bytes: Vec<_> = (0..8197).map(|n| (n % 251) as u8).collect();
        let expected = ContentHash(*blake3::hash(&bytes).as_bytes());
        for first in [0, 1, 1023, 1024, 1025, 4097] {
            for bulk in [0, 1, 1023, 1024, 1025, 4096] {
                let end = (first + bulk).min(bytes.len());
                let mut digest = Digest::new();
                digest.update(&[]).unwrap();
                for byte in &bytes[..first] {
                    digest.try_push(*byte).unwrap();
                }
                digest.try_extend(&bytes[first..end]).unwrap();
                for byte in &bytes[end..] {
                    digest.try_push(*byte).unwrap();
                }
                assert_eq!(digest.finalize().unwrap(), expected);
            }
        }
        assert_eq!(
            Digest::new().finalize().unwrap(),
            ContentHash(*blake3::hash(&[]).as_bytes())
        );
    }

    #[test]
    fn streamed_postcard_matches_original_whole_buffer_hash_for_large_nested_values() {
        let bytes: Vec<_> = (0..65537).map(|n| (n % 256) as u8).collect();
        let value = (
            u64::MAX,
            "prefix λ",
            vec![Some(&bytes), None, Some(&bytes)],
            "tail",
        );
        let original = postcard::to_stdvec(&value).unwrap();
        let expected = ContentHash(*blake3::hash(&original).as_bytes());
        assert_eq!(
            postcard::serialize_with_flavor(&value, Digest::new()).unwrap(),
            expected
        );
    }
}
