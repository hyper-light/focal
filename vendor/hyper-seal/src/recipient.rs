//! A key wrapped to another machine (`docs/seal.md` §6): for a backup, an archive, a hand-off to a
//! successor or a restore on a fresh machine, which share no root key with the source.
//!
//! ```text
//! (ct, ss_kem) = ML-KEM-1024.Encaps(recipient's encapsulation key)            (FIPS 203)
//! ss_ecdh      = ECDH-P384(ephemeral, recipient's P-384 key)                   (hybrid only)
//! kek          = HKDF-SHA-384(salt = LABEL, ikm = ss_kem [‖ ss_ecdh],
//!                             info = LABEL ‖ mode ‖ recipient ‖ key ‖ ct [‖ eph ‖ static])
//! record       = mode ‖ ct [‖ eph] ‖ AES-256-KW(kek, key)
//! ```
//!
//! Two modes. **CNSA** is ML-KEM-1024 alone (CNSA 2.0), the only mode under `fips`. **Hybrid** adds
//! ECDH over P-384, the shared secrets concatenated as SP 800-56C Rev. 2 allows, so the key stays
//! safe while either problem stays hard. Every public value and both IDs are in `info`, so a record
//! is bound to its recipient and its key and cannot be replayed as another's.

use aws_lc_rs::agreement::{self, ECDH_P384, UnparsedPublicKey};
use aws_lc_rs::encoding::{AsBigEndian, EcPrivateKeyBin};
use aws_lc_rs::hkdf::{HKDF_SHA384, KeyType, Salt};
use aws_lc_rs::kem::{Ciphertext, DecapsulationKey, EncapsulationKey, ML_KEM_1024};
use aws_lc_rs::key_wrap::{AES_256, AesKek, KeyWrap as _};

use crate::keys::{KeyId, WRAPPED_KEY, WrappingKey};
use crate::memory::wipe;
use crate::stream::{FileOpener, FileSealer, HEADER, Header};
use crate::{SealError, Secret32, TAG, guarded};

/// ML-KEM-1024's ciphertext and encapsulation key (FIPS 203, Table 3).
pub const KEM_CIPHERTEXT: usize = 1568;
/// ML-KEM-1024's encapsulation key.
pub const KEM_PUBLIC: usize = 1568;
/// ML-KEM-1024's decapsulation key.
const KEM_PRIVATE: usize = 3168;
/// A P-384 public key, uncompressed (SEC 1 §2.3.3).
pub const ECDH_PUBLIC: usize = 97;
/// A P-384 private scalar.
const ECDH_PRIVATE: usize = 48;

/// The salt and the first bytes of `info`: this construction's and this version's.
const LABEL: &[u8] = b"hyper-seal recipient v1";

/// Which key establishment a record used, its first byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// ML-KEM-1024 alone: CNSA 2.0, and the only mode under `fips`.
    Cnsa,
    /// ML-KEM-1024 and ECDH over P-384.
    Hybrid,
}

impl Mode {
    fn byte(self) -> u8 {
        match self {
            Self::Cnsa => 1,
            Self::Hybrid => 2,
        }
    }

    fn from_byte(byte: u8) -> Result<Self, SealError> {
        match byte {
            1 => Ok(Self::Cnsa),
            2 => Ok(Self::Hybrid),
            _ => Err(SealError::Malformed),
        }
    }

    /// The bytes of a record in this mode.
    pub fn record_len(self) -> usize {
        match self {
            Self::Cnsa => 1 + KEM_CIPHERTEXT + WRAPPED_KEY,
            Self::Hybrid => 1 + KEM_CIPHERTEXT + ECDH_PUBLIC + WRAPPED_KEY,
        }
    }
}

/// What a sender needs to wrap a key to a recipient: its ID and public keys.
#[derive(Clone, PartialEq, Eq)]
pub struct RecipientPublic {
    /// The recipient's ID: random, bound into every record wrapped to it.
    pub id: [u8; 16],
    /// Its ML-KEM-1024 encapsulation key.
    pub kem: Vec<u8>,
    /// Its P-384 public key, where it takes hybrid records.
    pub ecdh: Option<[u8; ECDH_PUBLIC]>,
}

impl std::fmt::Debug for RecipientPublic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecipientPublic")
            .field("id", &self.id)
            .field("hybrid", &self.ecdh.is_some())
            .finish()
    }
}

/// A machine that keys can be wrapped to: its decapsulation keys, held only here.
pub struct Recipient {
    id: [u8; 16],
    kem: DecapsulationKey,
    kem_public: Vec<u8>,
    ecdh: Option<agreement::PrivateKey>,
    ecdh_public: Option<[u8; ECDH_PUBLIC]>,
}

/// The HKDF output length: one AES-256 key.
struct Kek;

impl KeyType for Kek {
    fn len(&self) -> usize {
        32
    }
}

/// The key-encryption key from the shared secrets and everything public about the record.
fn derive_kek(ikm: &[&[u8]], info: &[&[u8]]) -> Result<Secret32, SealError> {
    let mut kek = Secret32::zeroed()?;
    let mut joined = Vec::new();
    for part in ikm {
        joined.extend_from_slice(part);
    }
    let result = guarded(SealError::Seal, || {
        Salt::new(HKDF_SHA384, LABEL)
            .extract(&joined)
            .expand(info, Kek)?
            .fill(kek.bytes_mut())
    });
    wipe(&mut joined);
    result?;
    Ok(kek)
}

fn ecdh_public_bytes(key: &agreement::PrivateKey) -> Result<[u8; ECDH_PUBLIC], SealError> {
    let public = guarded(SealError::Seal, || key.compute_public_key())?;
    public.as_ref().try_into().map_err(|_| SealError::Seal)
}

impl Recipient {
    /// A new recipient with a random ID, taking hybrid records when `hybrid`.
    pub fn generate(hybrid: bool) -> Result<Self, SealError> {
        let id = crate::random_bytes::<16>()?;
        let kem = guarded(SealError::Seal, || DecapsulationKey::generate(&ML_KEM_1024))?;
        let kem_public = guarded(SealError::Seal, || {
            kem.encapsulation_key()
                .and_then(|ek| ek.key_bytes())
                .map(|b| b.as_ref().to_vec())
        })?;
        let (ecdh, ecdh_public) = if hybrid {
            let key = guarded(SealError::Seal, || {
                agreement::PrivateKey::generate(&ECDH_P384)
            })?;
            let public = ecdh_public_bytes(&key)?;
            (Some(key), Some(public))
        } else {
            (None, None)
        };
        Ok(Self {
            id,
            kem,
            kem_public,
            ecdh,
            ecdh_public,
        })
    }

    /// What a sender needs.
    pub fn public(&self) -> RecipientPublic {
        RecipientPublic {
            id: self.id,
            kem: self.kem_public.clone(),
            ecdh: self.ecdh_public,
        }
    }

    /// The key `record` wraps for `key`. Another recipient's record, another key's, or a change to
    /// any byte is [`SealError::Unwrap`].
    pub fn unwrap(&self, key: KeyId, record: &[u8]) -> Result<Secret32, SealError> {
        let (&mode, rest) = record.split_first().ok_or(SealError::Malformed)?;
        let mode = Mode::from_byte(mode)?;
        if record.len() != mode.record_len() {
            return Err(SealError::Malformed);
        }
        let (ct, rest) = rest
            .split_at_checked(KEM_CIPHERTEXT)
            .ok_or(SealError::Malformed)?;
        let ss_kem = guarded(SealError::Unwrap, || {
            self.kem.decapsulate(Ciphertext::from(ct))
        })?;
        let mode_byte = [mode.byte()];
        let kek = match mode {
            Mode::Cnsa => {
                let info: [&[u8]; 5] = [LABEL, &mode_byte, &self.id, &key.0, ct];
                derive_kek(&[ss_kem.as_ref()], &info)?
            }
            Mode::Hybrid => {
                let ecdh = self.ecdh.as_ref().ok_or(SealError::Unwrap)?;
                let static_public = self.ecdh_public.as_ref().ok_or(SealError::Unwrap)?;
                let (eph, _) = rest
                    .split_at_checked(ECDH_PUBLIC)
                    .ok_or(SealError::Malformed)?;
                let mut ss_ecdh = [0u8; ECDH_PRIVATE];
                guarded(SealError::Unwrap, || {
                    agreement::agree(ecdh, UnparsedPublicKey::new(&ECDH_P384, eph), (), |ss| {
                        let ss: &[u8; ECDH_PRIVATE] = ss.try_into().map_err(|_| ())?;
                        ss_ecdh = *ss;
                        Ok(())
                    })
                })?;
                let info: [&[u8]; 7] =
                    [LABEL, &mode_byte, &self.id, &key.0, ct, eph, static_public];
                let kek = derive_kek(&[ss_kem.as_ref(), &ss_ecdh], &info);
                wipe(&mut ss_ecdh);
                kek?
            }
        };
        let wrapped = record
            .get(record.len().saturating_sub(WRAPPED_KEY)..)
            .ok_or(SealError::Malformed)?;
        let mut out = Secret32::zeroed()?;
        let written = guarded(SealError::Unwrap, || {
            let kw = AesKek::new(&AES_256, kek.bytes())?;
            kw.unwrap(wrapped, out.bytes_mut()).map(|w| w.len())
        })?;
        if written != 32 {
            return Err(SealError::Unwrap);
        }
        Ok(out)
    }

    /// The recipient sealed at rest under `parent` (a key source's key), one STREAM file: its ID,
    /// its decapsulation keys and its public keys.
    pub fn seal(&self, parent: &WrappingKey) -> Result<Vec<u8>, SealError> {
        let kem_private = guarded(SealError::Seal, || {
            self.kem.key_bytes().map(|b| b.as_ref().to_vec())
        })?;
        let mut plain = Vec::new();
        plain.extend_from_slice(&self.id);
        plain.extend_from_slice(&kem_private);
        plain.extend_from_slice(&self.kem_public);
        if let (Some(ecdh), Some(public)) = (&self.ecdh, &self.ecdh_public) {
            let scalar = guarded(SealError::Seal, || {
                AsBigEndian::<EcPrivateKeyBin<'static>>::as_be_bytes(ecdh)
                    .map(|b| b.as_ref().to_vec())
            })?;
            plain.extend_from_slice(&scalar);
            plain.extend_from_slice(public);
        }
        let mut kem_private = kem_private;
        wipe(&mut kem_private);
        let segment = u32::try_from(plain.len().max(crate::stream::MIN_SEGMENT as usize))
            .map_err(|_| SealError::Size)?;
        let (mut sealer, header) = FileSealer::new(parent, segment)?;
        let tag = sealer.seal(&mut plain, true);
        let tag = match tag {
            Ok(tag) => tag,
            Err(e) => {
                wipe(&mut plain);
                return Err(e);
            }
        };
        let mut out = Vec::with_capacity(HEADER.saturating_add(plain.len()).saturating_add(TAG));
        out.extend_from_slice(&header.encode());
        out.extend_from_slice(&plain);
        out.extend_from_slice(&tag);
        Ok(out)
    }

    /// A recipient [`Recipient::seal`] sealed, opened under `parent`.
    pub fn open(parent: &WrappingKey, sealed: &[u8]) -> Result<Self, SealError> {
        let (header, rest) = sealed
            .split_at_checked(HEADER)
            .ok_or(SealError::Malformed)?;
        let header = Header::decode(header)?;
        let body_len = rest.len().checked_sub(TAG).ok_or(SealError::Malformed)?;
        let (body, tag) = rest.split_at(body_len);
        let tag: &[u8; TAG] = tag.try_into().map_err(|_| SealError::Malformed)?;
        let mut plain = body.to_vec();
        let opened =
            FileOpener::new(parent, &header).and_then(|o| o.open(0, true, &mut plain, tag));
        let result = opened.and_then(|()| Self::from_plain(&plain));
        wipe(&mut plain);
        result
    }

    fn from_plain(plain: &[u8]) -> Result<Self, SealError> {
        let (id, rest) = plain.split_at_checked(16).ok_or(SealError::Malformed)?;
        let (kem_private, rest) = rest
            .split_at_checked(KEM_PRIVATE)
            .ok_or(SealError::Malformed)?;
        let (kem_public, rest) = rest
            .split_at_checked(KEM_PUBLIC)
            .ok_or(SealError::Malformed)?;
        let kem = guarded(SealError::Malformed, || {
            DecapsulationKey::new(&ML_KEM_1024, kem_private)
        })?;
        let (ecdh, ecdh_public) = match rest.len() {
            0 => (None, None),
            n if n == ECDH_PRIVATE.saturating_add(ECDH_PUBLIC) => {
                let (scalar, public) = rest.split_at(ECDH_PRIVATE);
                let key = guarded(SealError::Malformed, || {
                    agreement::PrivateKey::from_private_key(&ECDH_P384, scalar)
                })?;
                let public: [u8; ECDH_PUBLIC] =
                    public.try_into().map_err(|_| SealError::Malformed)?;
                if ecdh_public_bytes(&key)? != public {
                    return Err(SealError::Malformed);
                }
                (Some(key), Some(public))
            }
            _ => return Err(SealError::Malformed),
        };
        Ok(Self {
            id: id.try_into().map_err(|_| SealError::Malformed)?,
            kem,
            kem_public: kem_public.to_vec(),
            ecdh,
            ecdh_public,
        })
    }
}

/// `secret`, the key `key`, wrapped to `to` in `mode`. Hybrid needs a recipient that takes it.
pub fn wrap_to(
    to: &RecipientPublic,
    mode: Mode,
    key: KeyId,
    secret: &Secret32,
) -> Result<Vec<u8>, SealError> {
    if crate::fips() && mode == Mode::Hybrid {
        return Err(SealError::Source("only the CNSA mode under fips"));
    }
    let ek = guarded(SealError::Malformed, || {
        EncapsulationKey::new(&ML_KEM_1024, &to.kem)
    })?;
    let (ct, ss_kem) = guarded(SealError::Seal, || ek.encapsulate())?;
    let mode_byte = [mode.byte()];
    let mut record = Vec::with_capacity(mode.record_len());
    record.push(mode.byte());
    record.extend_from_slice(ct.as_ref());
    let kek = match mode {
        Mode::Cnsa => {
            let info: [&[u8]; 5] = [LABEL, &mode_byte, &to.id, &key.0, ct.as_ref()];
            derive_kek(&[ss_kem.as_ref()], &info)?
        }
        Mode::Hybrid => {
            let static_public = to
                .ecdh
                .as_ref()
                .ok_or(SealError::Source("the recipient takes no hybrid records"))?;
            let eph = guarded(SealError::Seal, || {
                agreement::PrivateKey::generate(&ECDH_P384)
            })?;
            let eph_public = ecdh_public_bytes(&eph)?;
            let mut ss_ecdh = [0u8; ECDH_PRIVATE];
            guarded(SealError::Seal, || {
                agreement::agree(
                    &eph,
                    UnparsedPublicKey::new(&ECDH_P384, static_public),
                    (),
                    |ss| {
                        let ss: &[u8; ECDH_PRIVATE] = ss.try_into().map_err(|_| ())?;
                        ss_ecdh = *ss;
                        Ok(())
                    },
                )
            })?;
            record.extend_from_slice(&eph_public);
            let info: [&[u8]; 7] = [
                LABEL,
                &mode_byte,
                &to.id,
                &key.0,
                ct.as_ref(),
                &eph_public,
                static_public,
            ];
            let kek = derive_kek(&[ss_kem.as_ref(), &ss_ecdh], &info);
            wipe(&mut ss_ecdh);
            kek?
        }
    };
    let mut wrapped = [0u8; WRAPPED_KEY];
    let written = guarded(SealError::Seal, || {
        let kw = AesKek::new(&AES_256, kek.bytes())?;
        kw.wrap(secret.bytes(), &mut wrapped).map(|w| w.len())
    })?;
    if written != WRAPPED_KEY {
        return Err(SealError::Seal);
    }
    record.extend_from_slice(&wrapped);
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> (KeyId, Secret32) {
        (KeyId([5; 16]), crate::random_secret().unwrap())
    }

    #[test]
    fn both_modes_round_trip() {
        for (hybrid, mode) in [
            (false, Mode::Cnsa),
            (true, Mode::Cnsa),
            (true, Mode::Hybrid),
        ] {
            let recipient = Recipient::generate(hybrid).unwrap();
            let (id, secret) = key();
            let record = wrap_to(&recipient.public(), mode, id, &secret).unwrap();
            assert_eq!(record.len(), mode.record_len());
            assert_eq!(
                recipient.unwrap(id, &record).unwrap().bytes(),
                secret.bytes()
            );
        }
    }

    #[test]
    fn a_record_for_another_recipient_or_key_does_not_unwrap() {
        let alice = Recipient::generate(true).unwrap();
        let bob = Recipient::generate(true).unwrap();
        for mode in [Mode::Cnsa, Mode::Hybrid] {
            let (id, secret) = key();
            let record = wrap_to(&alice.public(), mode, id, &secret).unwrap();
            assert_eq!(bob.unwrap(id, &record).unwrap_err(), SealError::Unwrap);
            assert_eq!(
                alice.unwrap(KeyId([6; 16]), &record).unwrap_err(),
                SealError::Unwrap
            );
        }
    }

    #[test]
    fn a_changed_byte_anywhere_does_not_unwrap() {
        let recipient = Recipient::generate(true).unwrap();
        let (id, secret) = key();
        let record = wrap_to(&recipient.public(), Mode::Hybrid, id, &secret).unwrap();
        // The mode byte, a ciphertext byte, the ephemeral key, the wrapped key.
        for at in [0, 1, 800, 1568, 1569, 1600, record.len() - 1] {
            let mut changed = record.clone();
            changed[at] ^= 1;
            assert!(recipient.unwrap(id, &changed).is_err(), "byte {at}");
        }
    }

    #[test]
    fn a_hybrid_record_needs_a_recipient_that_takes_one() {
        let recipient = Recipient::generate(false).unwrap();
        let (id, secret) = key();
        assert_eq!(
            wrap_to(&recipient.public(), Mode::Hybrid, id, &secret).unwrap_err(),
            SealError::Source("the recipient takes no hybrid records")
        );
    }

    #[test]
    fn a_recipient_sealed_at_rest_opens_and_unwraps() {
        let parent = WrappingKey::generate(0).unwrap();
        for hybrid in [false, true] {
            let recipient = Recipient::generate(hybrid).unwrap();
            let (id, secret) = key();
            let mode = if hybrid { Mode::Hybrid } else { Mode::Cnsa };
            let record = wrap_to(&recipient.public(), mode, id, &secret).unwrap();
            let sealed = recipient.seal(&parent).unwrap();
            let restored = Recipient::open(&parent, &sealed).unwrap();
            assert_eq!(restored.public(), recipient.public());
            assert_eq!(
                restored.unwrap(id, &record).unwrap().bytes(),
                secret.bytes()
            );
            let stranger = WrappingKey::generate(0).unwrap();
            assert!(Recipient::open(&stranger, &sealed).is_err());
        }
    }
}
