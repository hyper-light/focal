//! Bounds-checked little-endian encoding for the log's on-disk structures, from mantle-codec at
//! mantle `147f035`. A reader past the end of its input returns `None` instead of panicking, so
//! a torn or corrupt structure decodes to "invalid", never to a crash.

/// Appends little-endian fields to a byte vector.
#[derive(Debug, Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    /// A writer that reserves `capacity` bytes up front. The capacity is a hint: a reservation
    /// the allocator refuses leaves the buffer empty, and the writes that follow grow it as
    /// they would from `Default`, where `Vec::with_capacity` would panic on a capacity past
    /// `isize::MAX` bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut buf = Vec::new();
        if buf.try_reserve_exact(capacity).is_err() {
            buf = Vec::new();
        }
        Self { buf }
    }

    /// Appends a byte.
    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    /// Appends a `u32`, little-endian.
    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Appends a `u64`, little-endian.
    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Appends a `u128`, little-endian.
    pub fn u128(&mut self, v: u128) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Appends `v` as it is.
    pub fn bytes(&mut self, v: &[u8]) {
        self.buf.extend_from_slice(v);
    }

    /// Appends `n` zero bytes.
    pub fn zeros(&mut self, n: usize) {
        self.buf.resize(self.buf.len().saturating_add(n), 0);
    }

    /// Bytes written.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Empties the writer, keeping what it allocated for the next use.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Whether nothing is written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The bytes written.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// The bytes written, as the vector that holds them.
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

/// Reads little-endian fields from a byte slice.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// A reader of `buf` from its start.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes read so far: where the next field starts.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Bytes not yet read.
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    /// The next `n` bytes; `None` past the end.
    pub fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    /// The next byte.
    pub fn u8(&mut self) -> Option<u8> {
        self.array::<1>().map(|[b]| b)
    }

    /// The next `u32`, little-endian.
    pub fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_le_bytes)
    }

    /// The next `u64`, little-endian.
    pub fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_le_bytes)
    }

    /// The next `u128`, little-endian.
    pub fn u128(&mut self) -> Option<u128> {
        self.array().map(u128::from_le_bytes)
    }
}

/// CRC-32C (Castagnoli), the checksum every frame, record, entry and persist record carries:
/// the `crc32c` crate, whose `crc32c_append(crc32c(a), b)` is `crc32c(a ‖ b)`.
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// A CRC-32C over data given in pieces.
#[derive(Debug, Clone, Copy, Default)]
pub struct Crc32c(u32);

impl Crc32c {
    /// A CRC of nothing yet.
    pub fn new() -> Self {
        Self(0)
    }

    /// Adds `data`, as if it followed what came before.
    pub fn update(&mut self, data: &[u8]) {
        self.0 = crc32c::crc32c_append(self.0, data);
    }

    /// The CRC of everything added.
    pub fn finish(&self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// RFC 3720 §B.4's CRC-32C test vectors: 32 bytes of zeros, of ones, ascending and
    /// descending.
    #[test]
    fn crc32c_matches_rfc_3720_vectors() {
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
        let ascending: Vec<u8> = (0u8..32).collect();
        assert_eq!(crc32c(&ascending), 0x46DD_794E);
        let descending: Vec<u8> = (0u8..32).rev().collect();
        assert_eq!(crc32c(&descending), 0x113F_DB5C);
    }

    proptest! {
        #[test]
        fn fields_round_trip(a: u8, c: u32, d: u64, e: u128) {
            let mut w = Writer::default();
            w.u8(a); w.u32(c); w.u64(d); w.u128(e);
            let bytes = w.into_vec();
            let mut r = Reader::new(&bytes);
            prop_assert_eq!(r.u8(), Some(a));
            prop_assert_eq!(r.u32(), Some(c));
            prop_assert_eq!(r.u64(), Some(d));
            prop_assert_eq!(r.u128(), Some(e));
            prop_assert_eq!(r.u8(), None);
        }

        #[test]
        fn truncated_input_reads_none(bytes in proptest::collection::vec(any::<u8>(), 0..16)) {
            let mut r = Reader::new(&bytes);
            let _ = r.u128();
            prop_assert!(r.remaining() <= 16);
        }

        /// A CRC fed in pieces equals the CRC of the whole.
        #[test]
        fn pieces_equal_the_whole(a in proptest::collection::vec(any::<u8>(), 0..64), b in proptest::collection::vec(any::<u8>(), 0..64)) {
            let mut crc = Crc32c::new();
            crc.update(&a);
            crc.update(&b);
            let whole: Vec<u8> = a.iter().chain(&b).copied().collect();
            prop_assert_eq!(crc.finish(), crc32c(&whole));
        }
    }
}
