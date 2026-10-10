use alloc::boxed::Box;

use aws_lc_rs::{aead, tls_prf};

use crate::crypto::cipher::{
    make_tls12_aad, AeadKey, InboundOpaqueMessage, Iv, KeyBlockShape, KeyRejected,
    MessageDecrypter, MessageEncrypter, Nonce, Tls12AeadAlgorithm, UnsupportedOperationError,
    NONCE_LEN,
};
use crate::crypto::tls12::Prf;
use crate::crypto::{ActiveKeyExchange, KeyExchangeAlgorithm};
use crate::enums::{CipherSuite, SignatureScheme};
use crate::error::Error;
use crate::msgs::fragmenter::MAX_FRAGMENT_LEN;
use crate::msgs::message::{
    InboundPlainMessage, OutboundOpaqueMessage, OutboundPlainMessage, PrefixedPayload,
};
use crate::suites::{CipherSuiteCommon, ConnectionTrafficSecrets, SupportedCipherSuite};
use crate::tls12::Tls12CipherSuite;
use crate::version::TLS12;

/// The TLS1.2 ciphersuite TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256.
pub static TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256: SupportedCipherSuite =
    SupportedCipherSuite::Tls12(&Tls12CipherSuite {
        common: CipherSuiteCommon {
            suite: CipherSuite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
            hash_provider: &super::hash::SHA256,
            confidentiality_limit: u64::MAX,
        },
        kx: KeyExchangeAlgorithm::ECDHE,
        sign: TLS12_ECDSA_SCHEMES,
        aead_alg: &ChaCha20Poly1305,
        prf_provider: &Tls12Prf(&tls_prf::P_SHA256),
    });

/// The TLS1.2 ciphersuite TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
pub static TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256: SupportedCipherSuite =
    SupportedCipherSuite::Tls12(&Tls12CipherSuite {
        common: CipherSuiteCommon {
            suite: CipherSuite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
            hash_provider: &super::hash::SHA256,
            confidentiality_limit: u64::MAX,
        },
        kx: KeyExchangeAlgorithm::ECDHE,
        sign: TLS12_RSA_SCHEMES,
        aead_alg: &ChaCha20Poly1305,
        prf_provider: &Tls12Prf(&tls_prf::P_SHA256),
    });

/// The TLS1.2 ciphersuite TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
pub static TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256: SupportedCipherSuite =
    SupportedCipherSuite::Tls12(&Tls12CipherSuite {
        common: CipherSuiteCommon {
            suite: CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            hash_provider: &super::hash::SHA256,
            confidentiality_limit: 1 << 24,
        },
        kx: KeyExchangeAlgorithm::ECDHE,
        sign: TLS12_RSA_SCHEMES,
        aead_alg: &AES128_GCM,
        prf_provider: &Tls12Prf(&tls_prf::P_SHA256),
    });

/// The TLS1.2 ciphersuite TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
pub static TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384: SupportedCipherSuite =
    SupportedCipherSuite::Tls12(&Tls12CipherSuite {
        common: CipherSuiteCommon {
            suite: CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            hash_provider: &super::hash::SHA384,
            confidentiality_limit: 1 << 24,
        },
        kx: KeyExchangeAlgorithm::ECDHE,
        sign: TLS12_RSA_SCHEMES,
        aead_alg: &AES256_GCM,
        prf_provider: &Tls12Prf(&tls_prf::P_SHA384),
    });

/// The TLS1.2 ciphersuite TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
pub static TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256: SupportedCipherSuite =
    SupportedCipherSuite::Tls12(&Tls12CipherSuite {
        common: CipherSuiteCommon {
            suite: CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
            hash_provider: &super::hash::SHA256,
            confidentiality_limit: 1 << 24,
        },
        kx: KeyExchangeAlgorithm::ECDHE,
        sign: TLS12_ECDSA_SCHEMES,
        aead_alg: &AES128_GCM,
        prf_provider: &Tls12Prf(&tls_prf::P_SHA256),
    });

/// The TLS1.2 ciphersuite TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
pub static TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384: SupportedCipherSuite =
    SupportedCipherSuite::Tls12(&Tls12CipherSuite {
        common: CipherSuiteCommon {
            suite: CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            hash_provider: &super::hash::SHA384,
            confidentiality_limit: 1 << 24,
        },
        kx: KeyExchangeAlgorithm::ECDHE,
        sign: TLS12_ECDSA_SCHEMES,
        aead_alg: &AES256_GCM,
        prf_provider: &Tls12Prf(&tls_prf::P_SHA384),
    });

static TLS12_ECDSA_SCHEMES: &[SignatureScheme] = &[
    SignatureScheme::ED25519,
    SignatureScheme::ECDSA_NISTP521_SHA512,
    SignatureScheme::ECDSA_NISTP384_SHA384,
    SignatureScheme::ECDSA_NISTP256_SHA256,
];

static TLS12_RSA_SCHEMES: &[SignatureScheme] = &[
    SignatureScheme::RSA_PSS_SHA512,
    SignatureScheme::RSA_PSS_SHA384,
    SignatureScheme::RSA_PSS_SHA256,
    SignatureScheme::RSA_PKCS1_SHA512,
    SignatureScheme::RSA_PKCS1_SHA384,
    SignatureScheme::RSA_PKCS1_SHA256,
];

pub(crate) static AES128_GCM: GcmAlgorithm = GcmAlgorithm(&aead::AES_128_GCM);
pub(crate) static AES256_GCM: GcmAlgorithm = GcmAlgorithm(&aead::AES_256_GCM);

pub(crate) struct GcmAlgorithm(&'static aead::Algorithm);

impl Tls12AeadAlgorithm for GcmAlgorithm {
    fn decrypter(&self, dec_key: AeadKey, dec_iv: &[u8]) -> Box<dyn MessageDecrypter> {
        // See `encrypter()`: neither the key nor the salt fails for the key block's shape.
        let dec_key =
            aead::TlsRecordOpeningKey::new(self.0, aead::TlsProtocolId::TLS12, dec_key.as_ref());
        match (dec_key, <[u8; 4]>::try_from(dec_iv)) {
            (Ok(dec_key), Ok(dec_salt)) => Box::new(GcmMessageDecrypter { dec_key, dec_salt }),
            _ => Box::new(KeyRejected),
        }
    }

    fn encrypter(
        &self,
        enc_key: AeadKey,
        write_iv: &[u8],
        explicit: &[u8],
    ) -> Box<dyn MessageEncrypter> {
        // `TlsRecordSealingKey::new` fails if
        // - `enc_key`'s length is wrong for `algorithm`.  But the length is defined by
        //   `algorithm.key_len()` in `key_block_shape()`, below.
        // - `algorithm` is not supported: but `AES_128_GCM` and `AES_256_GCM` is.
        // Either way the connection gets the refusing cipher, never a panic.
        //
        // `TlsProtocolId::TLS13` is deliberate: we reuse the nonce construction from
        // RFC7905 and TLS13: a random starting point, XOR'd with the sequence number.  This means
        // `TlsProtocolId::TLS12` (which wants to see a plain sequence number) is unsuitable.
        //
        // The most important property is that nonce is unique per key, which is satisfied by
        // this construction, even if the nonce is not monotonically increasing.
        let enc_key =
            aead::TlsRecordSealingKey::new(self.0, aead::TlsProtocolId::TLS13, enc_key.as_ref());
        match (enc_key, gcm_iv(write_iv, explicit)) {
            (Ok(enc_key), Some(iv)) => Box::new(GcmMessageEncrypter { enc_key, iv }),
            _ => Box::new(KeyRejected),
        }
    }

    fn key_block_shape(&self) -> KeyBlockShape {
        KeyBlockShape {
            enc_key_len: self.0.key_len(),
            fixed_iv_len: 4,
            explicit_nonce_len: 8,
        }
    }

    fn extract_keys(
        &self,
        key: AeadKey,
        write_iv: &[u8],
        explicit: &[u8],
    ) -> Result<ConnectionTrafficSecrets, UnsupportedOperationError> {
        let iv = gcm_iv(write_iv, explicit).ok_or(UnsupportedOperationError)?;
        match self.0.key_len() {
            16 => Ok(ConnectionTrafficSecrets::Aes128Gcm { key, iv }),
            32 => Ok(ConnectionTrafficSecrets::Aes256Gcm { key, iv }),
            _ => Err(UnsupportedOperationError),
        }
    }

    fn fips(&self) -> bool {
        super::fips()
    }
}

pub(crate) struct ChaCha20Poly1305;

impl Tls12AeadAlgorithm for ChaCha20Poly1305 {
    fn decrypter(&self, dec_key: AeadKey, iv: &[u8]) -> Box<dyn MessageDecrypter> {
        // The key block's shape gives a 32-byte key and a 12-byte IV, which never fail.
        match (
            aead::UnboundKey::new(&aead::CHACHA20_POLY1305, dec_key.as_ref()),
            Iv::copy(iv),
        ) {
            (Ok(dec_key), Some(dec_offset)) => Box::new(ChaCha20Poly1305MessageDecrypter {
                dec_key: aead::LessSafeKey::new(dec_key),
                dec_offset,
            }),
            _ => Box::new(KeyRejected),
        }
    }

    fn encrypter(&self, enc_key: AeadKey, enc_iv: &[u8], _: &[u8]) -> Box<dyn MessageEncrypter> {
        match (
            aead::UnboundKey::new(&aead::CHACHA20_POLY1305, enc_key.as_ref()),
            Iv::copy(enc_iv),
        ) {
            (Ok(enc_key), Some(enc_offset)) => Box::new(ChaCha20Poly1305MessageEncrypter {
                enc_key: aead::LessSafeKey::new(enc_key),
                enc_offset,
            }),
            _ => Box::new(KeyRejected),
        }
    }

    fn key_block_shape(&self) -> KeyBlockShape {
        KeyBlockShape {
            enc_key_len: 32,
            fixed_iv_len: 12,
            explicit_nonce_len: 0,
        }
    }

    fn extract_keys(
        &self,
        key: AeadKey,
        iv: &[u8],
        _explicit: &[u8],
    ) -> Result<ConnectionTrafficSecrets, UnsupportedOperationError> {
        // KeyBlockShape and the Iv nonce len are in agreement.
        Ok(ConnectionTrafficSecrets::Chacha20Poly1305 {
            key,
            iv: Iv::copy(iv).ok_or(UnsupportedOperationError)?,
        })
    }

    fn fips(&self) -> bool {
        false // not FIPS approved
    }
}

/// A `MessageEncrypter` for AES-GCM AEAD ciphersuites. TLS 1.2 only.
struct GcmMessageEncrypter {
    enc_key: aead::TlsRecordSealingKey,
    iv: Iv,
}

/// A `MessageDecrypter` for AES-GCM AEAD ciphersuites.  TLS1.2 only.
struct GcmMessageDecrypter {
    dec_key: aead::TlsRecordOpeningKey,
    dec_salt: [u8; 4],
}

/// The explicit part of a TLS 1.2 GCM nonce carried in each record (RFC 5288 §3).
const GCM_EXPLICIT_NONCE_LEN: usize = 8;
/// A GCM record's bytes beyond its plaintext: the explicit nonce and the 16-byte tag (RFC 5288 §3).
const GCM_OVERHEAD: usize = GCM_EXPLICIT_NONCE_LEN + 16;

impl MessageDecrypter for GcmMessageDecrypter {
    fn decrypt<'a>(
        &mut self,
        mut msg: InboundOpaqueMessage<'a>,
        seq: u64,
    ) -> Result<InboundPlainMessage<'a>, Error> {
        let payload = &msg.payload;
        let (Some(plain_len), Some((explicit, _))) = (
            payload.len().checked_sub(GCM_OVERHEAD),
            payload.split_first_chunk::<GCM_EXPLICIT_NONCE_LEN>(),
        ) else {
            return Err(Error::DecryptError);
        };

        let nonce = aead::Nonce::assume_unique_for_key(concat_nonce(&self.dec_salt, explicit));
        let aad = aead::Aad::from(make_tls12_aad(
            seq,
            msg.typ,
            msg.version,
            u16::try_from(plain_len).map_err(|_| Error::PeerSentOversizedRecord)?,
        ));

        let payload = &mut msg.payload;
        let sealed = payload
            .get_mut(GCM_EXPLICIT_NONCE_LEN..)
            .ok_or(Error::DecryptError)?;
        let plain_len = self
            .dec_key
            .open_in_place(nonce, aad, sealed)
            .map_err(|_| Error::DecryptError)?
            .len();

        if plain_len > MAX_FRAGMENT_LEN {
            return Err(Error::PeerSentOversizedRecord);
        }

        // At most MAX_FRAGMENT_LEN, so the sum does not overflow.
        let plain_end = GCM_EXPLICIT_NONCE_LEN.saturating_add(plain_len);
        msg.into_plain_message_range(GCM_EXPLICIT_NONCE_LEN..plain_end)
            .ok_or(Error::DecryptError)
    }
}

impl MessageEncrypter for GcmMessageEncrypter {
    fn encrypt(
        &mut self,
        msg: OutboundPlainMessage<'_>,
        seq: u64,
    ) -> Result<OutboundOpaqueMessage, Error> {
        let total_len = self.encrypted_payload_len(msg.payload.len());
        let mut payload = PrefixedPayload::with_capacity(total_len);

        let nonce_bytes = Nonce::new(&self.iv, seq).0;
        let (_, explicit) = nonce_bytes.split_at(NONCE_LEN - GCM_EXPLICIT_NONCE_LEN);
        let nonce = aead::Nonce::assume_unique_for_key(nonce_bytes);
        let plain_len = u16::try_from(msg.payload.len()).map_err(|_| Error::EncryptError)?;
        let aad = aead::Aad::from(make_tls12_aad(seq, msg.typ, msg.version, plain_len));
        payload.extend_from_slice(explicit);
        payload.extend_from_chunks(&msg.payload);

        let sealed = payload
            .as_mut()
            .get_mut(GCM_EXPLICIT_NONCE_LEN..)
            .ok_or(Error::EncryptError)?;
        let tag = self
            .enc_key
            .seal_in_place_separate_tag(nonce, aad, sealed)
            .map_err(|_| Error::EncryptError)?;
        payload.extend_from_slice(tag.as_ref());

        Ok(OutboundOpaqueMessage::new(msg.typ, msg.version, payload))
    }

    fn encrypted_payload_len(&self, payload_len: usize) -> usize {
        // A required size: saturating can only over-state it, which refuses rather than overruns.
        payload_len
            .saturating_add(GCM_EXPLICIT_NONCE_LEN)
            .saturating_add(self.enc_key.algorithm().tag_len())
    }
}

/// The RFC7905/RFC7539 ChaCha20Poly1305 construction.
/// This implementation does the AAD construction required in TLS1.2.
/// TLS1.3 uses `TLS13MessageEncrypter`.
struct ChaCha20Poly1305MessageEncrypter {
    enc_key: aead::LessSafeKey,
    enc_offset: Iv,
}

/// The RFC7905/RFC7539 ChaCha20Poly1305 construction.
/// This implementation does the AAD construction required in TLS1.2.
/// TLS1.3 uses `TLS13MessageDecrypter`.
struct ChaCha20Poly1305MessageDecrypter {
    dec_key: aead::LessSafeKey,
    dec_offset: Iv,
}

/// The Poly1305 tag a ChaCha20-Poly1305 record carries beyond its plaintext (RFC 7905 §2).
const CHACHAPOLY1305_OVERHEAD: usize = 16;

impl MessageDecrypter for ChaCha20Poly1305MessageDecrypter {
    fn decrypt<'a>(
        &mut self,
        mut msg: InboundOpaqueMessage<'a>,
        seq: u64,
    ) -> Result<InboundPlainMessage<'a>, Error> {
        let payload = &msg.payload;

        let Some(plain_len) = payload.len().checked_sub(CHACHAPOLY1305_OVERHEAD) else {
            return Err(Error::DecryptError);
        };

        let nonce = aead::Nonce::assume_unique_for_key(Nonce::new(&self.dec_offset, seq).0);
        let aad = aead::Aad::from(make_tls12_aad(
            seq,
            msg.typ,
            msg.version,
            u16::try_from(plain_len).map_err(|_| Error::PeerSentOversizedRecord)?,
        ));

        let payload = &mut msg.payload;
        let plain_len = self
            .dec_key
            .open_in_place(nonce, aad, payload)
            .map_err(|_| Error::DecryptError)?
            .len();

        if plain_len > MAX_FRAGMENT_LEN {
            return Err(Error::PeerSentOversizedRecord);
        }

        payload.truncate(plain_len);
        Ok(msg.into_plain_message())
    }
}

impl MessageEncrypter for ChaCha20Poly1305MessageEncrypter {
    fn encrypt(
        &mut self,
        msg: OutboundPlainMessage<'_>,
        seq: u64,
    ) -> Result<OutboundOpaqueMessage, Error> {
        let total_len = self.encrypted_payload_len(msg.payload.len());
        let mut payload = PrefixedPayload::with_capacity(total_len);

        let nonce = aead::Nonce::assume_unique_for_key(Nonce::new(&self.enc_offset, seq).0);
        let plain_len = u16::try_from(msg.payload.len()).map_err(|_| Error::EncryptError)?;
        let aad = aead::Aad::from(make_tls12_aad(seq, msg.typ, msg.version, plain_len));
        payload.extend_from_chunks(&msg.payload);

        self.enc_key
            .seal_in_place_append_tag(nonce, aad, &mut payload)
            .map_err(|_| Error::EncryptError)?;

        Ok(OutboundOpaqueMessage::new(msg.typ, msg.version, payload))
    }

    fn encrypted_payload_len(&self, payload_len: usize) -> usize {
        // A required size: saturating can only over-state it, which refuses rather than overruns.
        payload_len.saturating_add(self.enc_key.algorithm().tag_len())
    }
}

/// The 12-byte GCM nonce: the 4-byte salt from the key block, then the record's explicit part.
fn concat_nonce(salt: &[u8; 4], explicit: &[u8; GCM_EXPLICIT_NONCE_LEN]) -> [u8; NONCE_LEN] {
    let mut nonce = [0u8; NONCE_LEN];
    let (head, tail) = nonce.split_at_mut(salt.len());
    head.copy_from_slice(salt);
    tail.copy_from_slice(explicit);
    nonce
}

/// The GCM nonce is constructed from a 32-bit 'salt' derived from the master-secret, and a 64-bit
/// explicit part, with no specified construction.  Thanks for that.
///
/// We use the same construction as TLS1.3/ChaCha20Poly1305: a starting point extracted from the
/// key block, xored with the sequence number. `None` unless the key block's shape gave a 4-byte
/// salt and an 8-byte explicit part.
fn gcm_iv(write_iv: &[u8], explicit: &[u8]) -> Option<Iv> {
    Some(Iv::new(concat_nonce(
        write_iv.try_into().ok()?,
        explicit.try_into().ok()?,
    )))
}

struct Tls12Prf(&'static tls_prf::Algorithm);

impl Prf for Tls12Prf {
    fn for_secret(
        &self,
        output: &mut [u8],
        secret: &[u8],
        label: &[u8],
        seed: &[u8],
    ) -> Result<(), Error> {
        // The documented failures are an empty `secret` and an empty `output`, both of which the
        // callers rule out.
        let derived = tls_prf::Secret::new(self.0, secret)
            .and_then(|secret| secret.derive(label, seed, output.len()))
            .map_err(|_| Error::Internal("TLS 1.2 PRF refused its input"))?;
        let derived = derived.as_ref();
        if derived.len() != output.len() {
            return Err(Error::Internal("TLS 1.2 PRF output of the wrong length"));
        }
        output.copy_from_slice(derived);
        Ok(())
    }

    fn for_key_exchange(
        &self,
        output: &mut [u8; 48],
        kx: Box<dyn ActiveKeyExchange>,
        peer_pub_key: &[u8],
        label: &[u8],
        seed: &[u8],
    ) -> Result<(), Error> {
        self.for_secret(
            output,
            kx.complete_for_tls_version(peer_pub_key, &TLS12)?
                .secret_bytes(),
            label,
            seed,
        )
    }

    fn fips(&self) -> bool {
        super::fips()
    }
}
