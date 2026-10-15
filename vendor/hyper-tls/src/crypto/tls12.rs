use alloc::boxed::Box;

use super::{hmac, ActiveKeyExchange};
use crate::error::Error;
use crate::version::TLS12;

/// Implements [`Prf`] using a [`hmac::Hmac`].
pub struct PrfUsingHmac<'a>(pub &'a dyn hmac::Hmac);

impl Prf for PrfUsingHmac<'_> {
    fn for_key_exchange(
        &self,
        output: &mut [u8; 48],
        kx: Box<dyn ActiveKeyExchange>,
        peer_pub_key: &[u8],
        label: &[u8],
        seed: &[u8],
    ) -> Result<(), Error> {
        prf(
            output,
            self.0
                .with_key(
                    kx.complete_for_tls_version(peer_pub_key, &TLS12)?
                        .secret_bytes(),
                )
                .as_ref(),
            label,
            seed,
        )
    }

    fn for_secret(
        &self,
        output: &mut [u8],
        secret: &[u8],
        label: &[u8],
        seed: &[u8],
    ) -> Result<(), Error> {
        prf(output, self.0.with_key(secret).as_ref(), label, seed)
    }
}

/// An instantiation of the TLS1.2 PRF with a specific, implicit hash function.
///
/// See the definition in [RFC5246 section 5](https://www.rfc-editor.org/rfc/rfc5246#section-5).
///
/// See [`PrfUsingHmac`] as a route to implementing this trait with just
/// an implementation of [`hmac::Hmac`].
pub trait Prf: Send + Sync {
    /// Computes `PRF(secret, label, seed)` using the secret from a completed key exchange.
    ///
    /// Completes the given key exchange, and then uses the resulting shared secret
    /// to compute the PRF, writing the result into `output`.
    ///
    /// The caller guarantees that `label`, `seed` are non-empty. The caller makes no
    /// guarantees about the contents of `peer_pub_key`. It must be validated by
    /// [`ActiveKeyExchange::complete`].
    fn for_key_exchange(
        &self,
        output: &mut [u8; 48],
        kx: Box<dyn ActiveKeyExchange>,
        peer_pub_key: &[u8],
        label: &[u8],
        seed: &[u8],
    ) -> Result<(), Error>;

    /// Computes `PRF(secret, label, seed)`, writing the result into `output`.
    ///
    /// The caller guarantees that `secret`, `label`, and `seed` are non-empty. An input the
    /// implementation cannot use is an error (upstream's implementations panicked).
    fn for_secret(
        &self,
        output: &mut [u8],
        secret: &[u8],
        label: &[u8],
        seed: &[u8],
    ) -> Result<(), Error>;

    /// Return `true` if this is backed by a FIPS-approved implementation.
    fn fips(&self) -> bool {
        false
    }
}

pub(crate) fn prf(
    out: &mut [u8],
    hmac_key: &dyn hmac::Key,
    label: &[u8],
    seed: &[u8],
) -> Result<(), Error> {
    // A(1)
    let mut current_a = hmac_key.sign(&[label, seed]);

    let chunk_size = hmac_key.tag_len();
    if chunk_size == 0 {
        return Err(Error::Internal("TLS 1.2 PRF over an HMAC with no output"));
    }
    for chunk in out.chunks_mut(chunk_size) {
        // P_hash[i] = HMAC_hash(secret, A(i) + seed)
        let p_term = hmac_key.sign(&[current_a.as_ref(), label, seed]);
        let term = p_term
            .as_ref()
            .get(..chunk.len())
            .ok_or(Error::Internal("HMAC tag shorter than its stated length"))?;
        chunk.copy_from_slice(term);

        // A(i+1) = HMAC_hash(secret, A(i))
        current_a = hmac_key.sign(&[current_a.as_ref()]);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::crypto::aws_lc_rs::hmac;
    use crate::crypto::hmac::Hmac;

    // Below known answer tests come from https://mailarchive.ietf.org/arch/msg/tls/fzVCzk-z3FShgGJ6DOXqM1ydxms/

    #[test]
    fn check_sha256() {
        let secret = b"\x9b\xbe\x43\x6b\xa9\x40\xf0\x17\xb1\x76\x52\x84\x9a\x71\xdb\x35";
        let seed = b"\xa0\xba\x9f\x93\x6c\xda\x31\x18\x27\xa6\xf7\x96\xff\xd5\x19\x8c";
        let label = b"test label";
        let expect = include_bytes!("../testdata/prf-result.1.bin");
        let mut output = [0u8; 100];

        super::prf(
            &mut output,
            &*hmac::HMAC_SHA256.with_key(secret),
            label,
            seed,
        )
        .unwrap();
        assert_eq!(expect.len(), output.len());
        assert_eq!(expect.to_vec(), output.to_vec());
    }

    #[test]
    fn check_sha512() {
        let secret = b"\xb0\x32\x35\x23\xc1\x85\x35\x99\x58\x4d\x88\x56\x8b\xbb\x05\xeb";
        let seed = b"\xd4\x64\x0e\x12\xe4\xbc\xdb\xfb\x43\x7f\x03\xe6\xae\x41\x8e\xe5";
        let label = b"test label";
        let expect = include_bytes!("../testdata/prf-result.2.bin");
        let mut output = [0u8; 196];

        super::prf(
            &mut output,
            &*hmac::HMAC_SHA512.with_key(secret),
            label,
            seed,
        )
        .unwrap();
        assert_eq!(expect.len(), output.len());
        assert_eq!(expect.to_vec(), output.to_vec());
    }

    #[test]
    fn check_sha384() {
        let secret = b"\xb8\x0b\x73\x3d\x6c\xee\xfc\xdc\x71\x56\x6e\xa4\x8e\x55\x67\xdf";
        let seed = b"\xcd\x66\x5c\xf6\xa8\x44\x7d\xd6\xff\x8b\x27\x55\x5e\xdb\x74\x65";
        let label = b"test label";
        let expect = include_bytes!("../testdata/prf-result.3.bin");
        let mut output = [0u8; 148];

        super::prf(
            &mut output,
            &*hmac::HMAC_SHA384.with_key(secret),
            label,
            seed,
        )
        .unwrap();
        assert_eq!(expect.len(), output.len());
        assert_eq!(expect.to_vec(), output.to_vec());
    }
}
