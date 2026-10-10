use alloc::boxed::Box;
use alloc::string::ToString;
use core::fmt;

use zeroize::Zeroize;

use crate::enums::{ContentType, ProtocolVersion};
use crate::error::Error;

pub use crate::msgs::message::{
    BorrowedPayload, InboundOpaqueMessage, InboundPlainMessage, OutboundChunks,
    OutboundOpaqueMessage, OutboundPlainMessage, PlainMessage, PrefixedPayload,
};
use crate::suites::ConnectionTrafficSecrets;

/// Factory trait for building `MessageEncrypter` and `MessageDecrypter` for a TLS1.3 cipher suite.
pub trait Tls13AeadAlgorithm: Send + Sync {
    /// Build a `MessageEncrypter` for the given key/iv.
    fn encrypter(&self, key: AeadKey, iv: Iv) -> Box<dyn MessageEncrypter>;

    /// Build a `MessageDecrypter` for the given key/iv.
    fn decrypter(&self, key: AeadKey, iv: Iv) -> Box<dyn MessageDecrypter>;

    /// The length of key in bytes required by `encrypter()` and `decrypter()`.
    fn key_len(&self) -> usize;

    /// Convert the key material from `key`/`iv`, into a `ConnectionTrafficSecrets` item.
    ///
    /// May return [`UnsupportedOperationError`] if the AEAD algorithm is not a supported
    /// variant of `ConnectionTrafficSecrets`.
    fn extract_keys(
        &self,
        key: AeadKey,
        iv: Iv,
    ) -> Result<ConnectionTrafficSecrets, UnsupportedOperationError>;

    /// Return `true` if this is backed by a FIPS-approved implementation.
    fn fips(&self) -> bool {
        false
    }
}

/// Factory trait for building `MessageEncrypter` and `MessageDecrypter` for a TLS1.2 cipher suite.
pub trait Tls12AeadAlgorithm: Send + Sync + 'static {
    /// Build a `MessageEncrypter` for the given key/iv and extra key block (which can be used for
    /// improving explicit nonce size security, if needed).
    ///
    /// The length of `key` is set by [`KeyBlockShape::enc_key_len`].
    ///
    /// The length of `iv` is set by [`KeyBlockShape::fixed_iv_len`].
    ///
    /// The length of `extra` is set by [`KeyBlockShape::explicit_nonce_len`].
    fn encrypter(&self, key: AeadKey, iv: &[u8], extra: &[u8]) -> Box<dyn MessageEncrypter>;

    /// Build a `MessageDecrypter` for the given key/iv.
    ///
    /// The length of `key` is set by [`KeyBlockShape::enc_key_len`].
    ///
    /// The length of `iv` is set by [`KeyBlockShape::fixed_iv_len`].
    fn decrypter(&self, key: AeadKey, iv: &[u8]) -> Box<dyn MessageDecrypter>;

    /// Return a `KeyBlockShape` that defines how large the `key_block` is and how it
    /// is split up prior to calling `encrypter()`, `decrypter()` and/or `extract_keys()`.
    fn key_block_shape(&self) -> KeyBlockShape;

    /// Convert the key material from `key`/`iv`, into a `ConnectionTrafficSecrets` item.
    ///
    /// The length of `key` is set by [`KeyBlockShape::enc_key_len`].
    ///
    /// The length of `iv` is set by [`KeyBlockShape::fixed_iv_len`].
    ///
    /// The length of `extra` is set by [`KeyBlockShape::explicit_nonce_len`].
    ///
    /// May return [`UnsupportedOperationError`] if the AEAD algorithm is not a supported
    /// variant of `ConnectionTrafficSecrets`.
    fn extract_keys(
        &self,
        key: AeadKey,
        iv: &[u8],
        explicit: &[u8],
    ) -> Result<ConnectionTrafficSecrets, UnsupportedOperationError>;

    /// Return `true` if this is backed by a FIPS-approved implementation.
    fn fips(&self) -> bool {
        false
    }
}

/// An error indicating that the AEAD algorithm does not support the requested operation.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct UnsupportedOperationError;

impl From<UnsupportedOperationError> for Error {
    fn from(value: UnsupportedOperationError) -> Self {
        Self::General(value.to_string())
    }
}

impl fmt::Display for UnsupportedOperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "operation not supported")
    }
}

impl core::error::Error for UnsupportedOperationError {}

/// How a TLS1.2 `key_block` is partitioned.
///
/// Note: ciphersuites with non-zero `mac_key_length` are  not currently supported.
pub struct KeyBlockShape {
    /// How long keys are.
    ///
    /// `enc_key_length` terminology is from the standard ([RFC5246 A.6]).
    ///
    /// [RFC5246 A.6]: <https://www.rfc-editor.org/rfc/rfc5246#appendix-A.6>
    pub enc_key_len: usize,

    /// How long the fixed part of the 'IV' is.
    ///
    /// `fixed_iv_length` terminology is from the standard ([RFC5246 A.6]).
    ///
    /// This isn't usually an IV, but we continue the
    /// terminology misuse to match the standard.
    ///
    /// [RFC5246 A.6]: <https://www.rfc-editor.org/rfc/rfc5246#appendix-A.6>
    pub fixed_iv_len: usize,

    /// This is a non-standard extension which extends the
    /// key block to provide an initial explicit nonce offset,
    /// in a deterministic and safe way.  GCM needs this,
    /// chacha20poly1305 works this way by design.
    pub explicit_nonce_len: usize,
}

/// Objects with this trait can decrypt TLS messages.
pub trait MessageDecrypter: Send + Sync {
    /// Decrypt the given TLS message `msg`, using the sequence number
    /// `seq` which can be used to derive a unique [`Nonce`].
    fn decrypt<'a>(
        &mut self,
        msg: InboundOpaqueMessage<'a>,
        seq: u64,
    ) -> Result<InboundPlainMessage<'a>, Error>;
}

/// Objects with this trait can encrypt TLS messages.
pub trait MessageEncrypter: Send + Sync {
    /// Encrypt the given TLS message `msg`, using the sequence number
    /// `seq` which can be used to derive a unique [`Nonce`].
    fn encrypt(
        &mut self,
        msg: OutboundPlainMessage<'_>,
        seq: u64,
    ) -> Result<OutboundOpaqueMessage, Error>;

    /// Return the length of the ciphertext that results from encrypting plaintext of
    /// length `payload_len`
    fn encrypted_payload_len(&self, payload_len: usize) -> usize;
}

impl dyn MessageEncrypter {
    pub(crate) fn invalid() -> Box<dyn MessageEncrypter> {
        Box::new(InvalidMessageEncrypter {})
    }
}

impl dyn MessageDecrypter {
    pub(crate) fn invalid() -> Box<dyn MessageDecrypter> {
        Box::new(InvalidMessageDecrypter {})
    }
}

/// A write or read IV.
#[derive(Default)]
pub struct Iv([u8; NONCE_LEN]);

impl Iv {
    /// Create a new `Iv` from a byte array, of precisely `NONCE_LEN` bytes.
    pub fn new(value: [u8; NONCE_LEN]) -> Self {
        Self(value)
    }

    /// Create a new `Iv` from a byte slice, of precisely `NONCE_LEN` bytes: `None` for any
    /// other length (upstream panicked).
    pub fn copy(value: &[u8]) -> Option<Self> {
        <[u8; NONCE_LEN]>::try_from(value).ok().map(Self::new)
    }
}

impl From<[u8; NONCE_LEN]> for Iv {
    fn from(bytes: [u8; NONCE_LEN]) -> Self {
        Self(bytes)
    }
}

impl AsRef<[u8]> for Iv {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// A nonce.  This is unique for all messages on a connection.
pub struct Nonce(pub [u8; NONCE_LEN]);

impl Nonce {
    /// Combine an `Iv` and sequence number to produce a unique nonce.
    ///
    /// This is `iv ^ seq` where `seq` is encoded as a 96-bit big-endian integer.
    #[inline]
    pub fn new(iv: &Iv, seq: u64) -> Self {
        Self::for_path(0, iv, seq)
    }

    /// Creates a unique nonce based on the `iv`, the packet number `pn` and multipath `path_id`.
    ///
    /// The nonce is computed as the XOR between the `iv` and the 96-bit big-ending integer formed
    /// by concatenating `path_id` and `pn`.
    pub fn for_path(path_id: u32, iv: &Iv, pn: u64) -> Self {
        let [p0, p1, p2, p3] = path_id.to_be_bytes();
        let [s0, s1, s2, s3, s4, s5, s6, s7] = pn.to_be_bytes();
        Self::new_from_seq(iv, [p0, p1, p2, p3, s0, s1, s2, s3, s4, s5, s6, s7])
    }

    /// Creates a unique nonce based on the `iv` and sequence number `seq`.
    #[inline]
    fn new_from_seq(iv: &Iv, mut seq: [u8; NONCE_LEN]) -> Self {
        seq.iter_mut().zip(iv.0.iter()).for_each(|(s, iv)| {
            *s ^= *iv;
        });

        Self(seq)
    }
}

/// Size of TLS nonces (incorrectly termed "IV" in standard) for all supported ciphersuites
/// (AES-GCM, Chacha20Poly1305)
pub const NONCE_LEN: usize = 12;

/// Returns a TLS1.3 `additional_data` encoding.
///
/// See RFC8446 s5.2 for the `additional_data` definition.
#[inline]
///
/// `payload_len` is the record's `uint16 length` field: a record longer than that does not exist.
pub fn make_tls13_aad(payload_len: u16) -> [u8; 5] {
    let [v0, v1] = ProtocolVersion::TLSv1_2.to_array();
    let [l0, l1] = payload_len.to_be_bytes();
    // Note: the version is `legacy_record_version`, i.e. TLS1.2 even for TLS1.3.
    [ContentType::ApplicationData.into(), v0, v1, l0, l1]
}

/// Returns a TLS1.2 `additional_data` encoding.
///
/// See RFC5246 s6.2.3.3 for the `additional_data` definition.
#[inline]
///
/// `len` is the record's `uint16 length` field: a record longer than that does not exist.
pub fn make_tls12_aad(
    seq: u64,
    typ: ContentType,
    vers: ProtocolVersion,
    len: u16,
) -> [u8; TLS12_AAD_SIZE] {
    let [s0, s1, s2, s3, s4, s5, s6, s7] = seq.to_be_bytes();
    let [v0, v1] = u16::from(vers).to_be_bytes();
    let [l0, l1] = len.to_be_bytes();
    [s0, s1, s2, s3, s4, s5, s6, s7, typ.into(), v0, v1, l0, l1]
}

/// TLS 1.2's additional data: seq_num, type, version and length (RFC 5246 §6.2.3.3).
const TLS12_AAD_SIZE: usize = 8 + 1 + 2 + 2;

/// A key for an AEAD algorithm.
///
/// This is a value type for a byte string up to `AeadKey::MAX_LEN` bytes in length.
pub struct AeadKey {
    buf: [u8; Self::MAX_LEN],
    used: usize,
}

impl AeadKey {
    /// The key `buf`, refused when it is longer than [`Self::MAX_LEN`] (upstream asserted).
    pub(crate) fn new(buf: &[u8]) -> Result<Self, Error> {
        let mut key = Self::from([0u8; Self::MAX_LEN]);
        key.buf
            .get_mut(..buf.len())
            .ok_or(Error::Internal("AEAD key longer than AeadKey::MAX_LEN"))?
            .copy_from_slice(buf);
        key.used = buf.len();
        Ok(key)
    }

    /// The first `len` bytes of this key, refused when it holds fewer (upstream asserted).
    pub(crate) fn with_length(self, len: usize) -> Result<Self, Error> {
        if len > self.used {
            return Err(Error::Internal(
                "AEAD key shorter than its algorithm's key length",
            ));
        }
        Ok(Self {
            buf: self.buf,
            used: len,
        })
    }

    /// Largest possible AEAD key in the ciphersuites we support.
    pub(crate) const MAX_LEN: usize = 32;
}

impl Drop for AeadKey {
    fn drop(&mut self) {
        self.buf.zeroize();
    }
}

impl AsRef<[u8]> for AeadKey {
    fn as_ref(&self) -> &[u8] {
        // `used` never exceeds MAX_LEN: `new` and `with_length` refuse it. Were it to, the empty
        // key is one every AEAD rejects, a typed refusal, never a wrong key.
        self.buf.get(..self.used).unwrap_or_default()
    }
}

impl From<[u8; Self::MAX_LEN]> for AeadKey {
    fn from(bytes: [u8; Self::MAX_LEN]) -> Self {
        Self {
            buf: bytes,
            used: Self::MAX_LEN,
        }
    }
}

/// A `MessageEncrypter` which doesn't work.
struct InvalidMessageEncrypter {}

impl MessageEncrypter for InvalidMessageEncrypter {
    fn encrypt(
        &mut self,
        _m: OutboundPlainMessage<'_>,
        _seq: u64,
    ) -> Result<OutboundOpaqueMessage, Error> {
        Err(Error::EncryptError)
    }

    fn encrypted_payload_len(&self, payload_len: usize) -> usize {
        payload_len
    }
}

/// The cipher in place of an AEAD key the provider rejected: every record fails with
/// [`Error::Internal`]. The key schedule derives exactly `key_len()` bytes, so this is not reached;
/// upstream unwrapped the key's construction.
pub(crate) struct KeyRejected;

impl MessageEncrypter for KeyRejected {
    fn encrypt(
        &mut self,
        _m: OutboundPlainMessage<'_>,
        _seq: u64,
    ) -> Result<OutboundOpaqueMessage, Error> {
        Err(Error::Internal("AEAD key rejected by the provider"))
    }

    fn encrypted_payload_len(&self, payload_len: usize) -> usize {
        payload_len
    }
}

impl MessageDecrypter for KeyRejected {
    fn decrypt<'a>(
        &mut self,
        _m: InboundOpaqueMessage<'a>,
        _seq: u64,
    ) -> Result<InboundPlainMessage<'a>, Error> {
        Err(Error::Internal("AEAD key rejected by the provider"))
    }
}

/// A `MessageDecrypter` which doesn't work.
struct InvalidMessageDecrypter {}

impl MessageDecrypter for InvalidMessageDecrypter {
    fn decrypt<'a>(
        &mut self,
        _m: InboundOpaqueMessage<'a>,
        _seq: u64,
    ) -> Result<InboundPlainMessage<'a>, Error> {
        Err(Error::DecryptError)
    }
}
