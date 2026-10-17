//! CRC-32C (Castagnoli, `CRC-32/ISCSI`), the checksum every prefix, opener and frame carries: crc-fast's,
//! which chooses its widest carry-less multiply when it runs, where the crc32c crate reached its
//! hardware path only in a build for the machine's own CPU. On the bare-metal bench (Ryzen 9
//! 9950X3D2, a build for the generic x86-64 target, as the release builds): 18 ns a 1,230-byte
//! message against 136, 12.3 µs a 1 MiB batch against 98.4; in a build for the machine's own CPU
//! the crc32c crate took 41 ns and 25.1 µs. The function is the same: the same value for the same
//! bytes, however they are divided.

use crc_fast::{CrcAlgorithm, Digest};

/// The CRC-32C of `bytes`.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_append(0, bytes)
}

/// The CRC-32C of the bytes `crc` was taken of followed by `bytes`.
pub(crate) fn crc32c_append(crc: u32, bytes: &[u8]) -> u32 {
    // The register before the final inversion is the checksum inverted (init and xorout are both
    // all ones), so a checksum goes on from where it stood.
    let mut digest = Digest::new_with_init_state(CrcAlgorithm::Crc32Iscsi, u64::from(!crc));
    digest.update(bytes);
    u32::try_from(digest.finalize() & u64::from(u32::MAX)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The check value of `CRC-32/ISCSI` (the catalogue's "123456789"), and every division of a
    /// buffer gives the whole's checksum.
    #[test]
    fn the_checksum_is_crc32c_and_goes_on_from_any_division() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
        let bytes: Vec<u8> = (0..4_096u32)
            .map(|n| (n.wrapping_mul(31) ^ (n >> 3)) as u8)
            .collect();
        let whole = crc32c(&bytes);
        for cut in [0, 1, 7, 8, 15, 16, 63, 64, 255, 1_000, 2_048, 4_095, 4_096] {
            let (head, tail) = bytes.split_at(cut);
            assert_eq!(crc32c_append(crc32c(head), tail), whole, "cut at {cut}");
        }
        let mut running = 0;
        for chunk in bytes.chunks(97) {
            running = crc32c_append(running, chunk);
        }
        assert_eq!(running, whole);
    }
}
