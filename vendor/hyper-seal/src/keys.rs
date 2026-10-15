//! Keys and their hierarchy (`docs/seal.md` §3): every key random, each wrapped by its parent with
//! AES-256 key wrap (SP 800-38F KW, RFC 3394) into a 61-byte record naming the parent and its
//! generation; the commitment a key carries beside what it seals (§2); and the source a root key
//! comes from.

use aws_lc_rs::hmac;
use aws_lc_rs::key_wrap::{AES_256, AesKek, KeyWrap as _};

use crate::{SealError, Secret32, fill, guarded, random_bytes, random_secret};

/// A key wrapped with AES-256 KW: the key and one semiblock (SP 800-38F §6.2).
pub const WRAPPED_KEY: usize = 40;

/// Bytes of a [`Wrapped`] record: version 1, parent 16, generation 4, wrapped key 40 (§3.3).
pub const RECORD: usize = 61;

/// Bytes of a key's commitment: a full HMAC-SHA-256 (§2).
pub const COMMITMENT: usize = 32;

/// The record format this build writes.
const RECORD_VERSION: u8 = 1;

/// The label a commitment is computed under, so it is never another use's MAC.
const COMMIT_LABEL: &[u8] = b"hyper-seal commit";

/// A key's ID: 128 random bits given at its making, never derived from the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyId(pub [u8; 16]);

impl KeyId {
    /// A new ID from the operating system's random source.
    pub fn random() -> Result<Self, SealError> {
        Ok(Self(random_bytes()?))
    }
}

/// A key wrapped by its parent, as a header or a tenant's record keeps it (§3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Wrapped {
    /// The wrapping key's ID.
    pub parent: KeyId,
    /// The wrapping key's generation.
    pub generation: u32,
    /// AES-256-KW of the key.
    pub key: [u8; WRAPPED_KEY],
}

impl Wrapped {
    /// The record's bytes.
    pub fn encode(&self) -> [u8; RECORD] {
        let mut out = [0u8; RECORD];
        fill(
            &mut out,
            &[
                &[RECORD_VERSION],
                &self.parent.0,
                &self.generation.to_le_bytes(),
                &self.key,
            ],
        );
        out
    }

    /// A record from its bytes; anything but a whole record of this version is malformed.
    pub fn decode(bytes: &[u8]) -> Result<Self, SealError> {
        let bytes: &[u8; RECORD] = bytes.try_into().map_err(|_| SealError::Malformed)?;
        let (version, rest) = bytes.split_at(1);
        if version != [RECORD_VERSION] {
            return Err(SealError::Malformed);
        }
        let (parent, rest) = rest.split_at(16);
        let (generation, key) = rest.split_at(4);
        Ok(Self {
            parent: KeyId(parent.try_into().map_err(|_| SealError::Malformed)?),
            generation: u32::from_le_bytes(
                generation.try_into().map_err(|_| SealError::Malformed)?,
            ),
            key: key.try_into().map_err(|_| SealError::Malformed)?,
        })
    }
}

/// A key that wraps keys below it: a root key's generation, a tenant's key, a lineage key. Each
/// level of the hierarchy is one of these; who wraps whom is the consumer's record.
pub struct WrappingKey {
    id: KeyId,
    generation: u32,
    secret: Secret32,
    retired: bool,
}

impl WrappingKey {
    /// A new key at `generation`, random, with a random ID.
    pub fn generate(generation: u32) -> Result<Self, SealError> {
        Ok(Self::new(KeyId::random()?, generation, random_secret()?))
    }

    /// A key the caller holds: unwrapped from its parent, or read from a source.
    pub fn new(id: KeyId, generation: u32, secret: Secret32) -> Self {
        Self {
            id,
            generation,
            secret,
            retired: false,
        }
    }

    /// The key's ID.
    pub fn id(&self) -> KeyId {
        self.id
    }

    /// The key's generation.
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Marks the key past its originator-usage period (SP 800-57 Pt 1 Table 1, at most two years
    /// for a key-wrapping key): it unwraps, for a rewrap under its successor, and never wraps again
    /// (§5.3.6). The consumer keeps the clock and says when.
    pub fn retire(&mut self) {
        self.retired = true;
    }

    /// `child` wrapped under this key.
    pub fn wrap(&self, child: &Secret32) -> Result<Wrapped, SealError> {
        if self.retired {
            return Err(SealError::Expired);
        }
        let mut out = [0u8; WRAPPED_KEY];
        let written = guarded(SealError::Seal, || {
            let kek = AesKek::new(&AES_256, self.secret.bytes())?;
            kek.wrap(child.bytes(), &mut out).map(|w| w.len())
        })?;
        if written != WRAPPED_KEY {
            return Err(SealError::Seal);
        }
        Ok(Wrapped {
            parent: self.id,
            generation: self.generation,
            key: out,
        })
    }

    /// The key `wrapped` holds. A record that names another parent or generation, or whose bytes
    /// fail key wrap's integrity check (forgery probability 2^-64, SP 800-38F App. A.3), is
    /// [`SealError::Unwrap`].
    pub fn unwrap(&self, wrapped: &Wrapped) -> Result<Secret32, SealError> {
        if wrapped.parent != self.id || wrapped.generation != self.generation {
            return Err(SealError::Unwrap);
        }
        let mut child = Secret32::zeroed()?;
        let written = guarded(SealError::Unwrap, || {
            let kek = AesKek::new(&AES_256, self.secret.bytes())?;
            kek.unwrap(&wrapped.key, child.bytes_mut()).map(|w| w.len())
        })?;
        if written != 32 {
            return Err(SealError::Unwrap);
        }
        Ok(child)
    }

    /// A child key derived from this key and `label` with HKDF-SHA-384 (SP 800-56C two-step, RFC
    /// 5869), and its record under this key: the same label always gives the same key and record. A
    /// content-keyed file's key (docs/seal.md §4), whose label is its content's 256-bit hash.
    pub fn derive_child(
        &self,
        purpose: &[u8],
        label: &[u8],
    ) -> Result<(Secret32, Wrapped), SealError> {
        struct Len;
        impl aws_lc_rs::hkdf::KeyType for Len {
            fn len(&self) -> usize {
                32
            }
        }
        let mut child = Secret32::zeroed()?;
        guarded(SealError::Seal, || {
            aws_lc_rs::hkdf::Salt::new(aws_lc_rs::hkdf::HKDF_SHA384, purpose)
                .extract(self.secret.bytes())
                .expand(&[purpose, label], Len)?
                .fill(child.bytes_mut())
        })?;
        let wrapped = self.wrap(&child)?;
        Ok((child, wrapped))
    }

    /// A new child key, random, and its record under this key: what a tenant, a volume or a file
    /// is given when it is made.
    pub fn make_child(&self) -> Result<(Secret32, Wrapped), SealError> {
        let child = random_secret()?;
        let wrapped = self.wrap(&child)?;
        Ok((child, wrapped))
    }
}

/// The commitment to `key` for the file or session `id` (§2): HMAC-SHA-256 under the key, so a
/// header naming another key, or a ciphertext built to open under two keys, is caught before any
/// open.
pub fn commitment(key: &Secret32, id: &[u8; 16]) -> Result<[u8; COMMITMENT], SealError> {
    let tag = guarded(SealError::Seal, || {
        let mac = hmac::Key::new(hmac::HMAC_SHA256, key.bytes());
        let mut ctx = hmac::Context::with_key(&mac);
        ctx.update(COMMIT_LABEL);
        ctx.update(id);
        Ok::<_, ()>(ctx.sign())
    })?;
    tag.as_ref().try_into().map_err(|_| SealError::Seal)
}

/// Whether `key` matches `expected` for `id`, compared in constant time.
pub fn check_commitment(
    key: &Secret32,
    id: &[u8; 16],
    expected: &[u8; COMMITMENT],
) -> Result<(), SealError> {
    let actual = commitment(key, id)?;
    if aws_lc_rs::constant_time::verify_slices_are_equal(&actual, expected).is_ok() {
        Ok(())
    } else {
        Err(SealError::Open)
    }
}

/// A trusted counter that only moves forward (§5.2): a TPM NV counter, the Secure Enclave's.
pub trait Monotonic {
    /// The counter's value now.
    fn read(&mut self) -> Result<u64, SealError>;
    /// Advances the counter past its value and returns the new one; durable before it returns.
    fn advance(&mut self) -> Result<u64, SealError>;
}

/// Where a root key lives (§3.2). The root key's bytes never leave a hardware source; it wraps and
/// unwraps the keys below it in place.
pub trait KeySource {
    /// The root key's ID and generation, recorded in every record it wraps.
    fn id(&self) -> (KeyId, u32);
    /// `key` wrapped by the source's root key.
    fn wrap(&mut self, key: &Secret32) -> Result<Wrapped, SealError>;
    /// The key `wrapped` holds, unwrapped by the source's root key.
    fn unwrap(&mut self, wrapped: &Wrapped) -> Result<Secret32, SealError>;
    /// The source's monotonic counter, where it has one.
    fn monotonic(&mut self) -> Option<&mut dyn Monotonic> {
        None
    }
}

/// A root key held in memory: for tests, and for a key a consumer was handed (a successor's
/// volume key unwrapped from a recipient record, §6).
pub struct MemorySource(WrappingKey);

impl MemorySource {
    /// A source over `key`.
    pub fn new(key: WrappingKey) -> Self {
        Self(key)
    }
}

impl KeySource for MemorySource {
    fn id(&self) -> (KeyId, u32) {
        (self.0.id(), self.0.generation())
    }

    fn wrap(&mut self, key: &Secret32) -> Result<Wrapped, SealError> {
        self.0.wrap(key)
    }

    fn unwrap(&mut self, wrapped: &Wrapped) -> Result<Secret32, SealError> {
        self.0.unwrap(wrapped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: [u8; 32]) -> WrappingKey {
        WrappingKey::new(KeyId([1; 16]), 0, Secret32::from_bytes(&bytes).unwrap())
    }

    /// RFC 3394 §4.6: 256 bits of key data with a 256-bit KEK.
    #[test]
    fn the_wrap_is_rfc_3394s() {
        let kek: [u8; 32] = core::array::from_fn(|i| i as u8);
        let data: [u8; 32] = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
            0x0C, 0x0D, 0x0E, 0x0F,
        ];
        let expected: [u8; 40] = [
            0x28, 0xC9, 0xF4, 0x04, 0xC4, 0xB8, 0x10, 0xF4, 0xCB, 0xCC, 0xB3, 0x5C, 0xFB, 0x87,
            0xF8, 0x26, 0x3F, 0x57, 0x86, 0xE2, 0xD8, 0x0E, 0xD3, 0x26, 0xCB, 0xC7, 0xF0, 0xE7,
            0x1A, 0x99, 0xF4, 0x3B, 0xFB, 0x98, 0x8B, 0x9B, 0x7A, 0x02, 0xDD, 0x21,
        ];
        let parent = key(kek);
        let wrapped = parent.wrap(&Secret32::from_bytes(&data).unwrap()).unwrap();
        assert_eq!(wrapped.key, expected);
        assert_eq!(parent.unwrap(&wrapped).unwrap().bytes(), &data);
    }

    #[test]
    fn a_record_round_trips_and_refuses_other_shapes() {
        let parent = WrappingKey::generate(3).unwrap();
        let (child, wrapped) = parent.make_child().unwrap();
        let bytes = wrapped.encode();
        assert_eq!(Wrapped::decode(&bytes).unwrap(), wrapped);
        assert_eq!(
            parent
                .unwrap(&Wrapped::decode(&bytes).unwrap())
                .unwrap()
                .bytes(),
            child.bytes()
        );
        assert_eq!(Wrapped::decode(&bytes[..60]), Err(SealError::Malformed));
        let mut other = bytes;
        other[0] = 2;
        assert_eq!(Wrapped::decode(&other), Err(SealError::Malformed));
    }

    #[test]
    fn another_parent_or_a_changed_byte_does_not_unwrap() {
        let parent = WrappingKey::generate(0).unwrap();
        let stranger = WrappingKey::generate(0).unwrap();
        let (_, wrapped) = parent.make_child().unwrap();
        assert_eq!(stranger.unwrap(&wrapped).unwrap_err(), SealError::Unwrap);
        let forged = WrappingKey::new(parent.id(), 0, random_secret().unwrap());
        assert_eq!(forged.unwrap(&wrapped).unwrap_err(), SealError::Unwrap);
        for at in 0..WRAPPED_KEY {
            let mut changed = wrapped;
            changed.key[at] ^= 1;
            assert_eq!(parent.unwrap(&changed).unwrap_err(), SealError::Unwrap);
        }
        let mut next_generation = wrapped;
        next_generation.generation = 1;
        assert_eq!(
            parent.unwrap(&next_generation).unwrap_err(),
            SealError::Unwrap
        );
    }

    #[test]
    fn a_retired_key_unwraps_and_never_wraps() {
        let mut parent = WrappingKey::generate(0).unwrap();
        let (child, wrapped) = parent.make_child().unwrap();
        parent.retire();
        assert_eq!(parent.unwrap(&wrapped).unwrap().bytes(), child.bytes());
        assert_eq!(parent.wrap(&child).unwrap_err(), SealError::Expired);
    }

    #[test]
    fn a_rotation_rewraps_and_the_child_is_unchanged() {
        let mut old = WrappingKey::generate(0).unwrap();
        let new = WrappingKey::generate(1).unwrap();
        let (child, wrapped) = old.make_child().unwrap();
        old.retire();
        let rewrapped = new.wrap(&old.unwrap(&wrapped).unwrap()).unwrap();
        assert_eq!(new.unwrap(&rewrapped).unwrap().bytes(), child.bytes());
        assert_eq!(rewrapped.generation, 1);
    }

    #[test]
    fn a_commitment_names_its_key_and_its_id() {
        let a = random_secret().unwrap();
        let b = random_secret().unwrap();
        let c = commitment(&a, &[9; 16]).unwrap();
        assert!(check_commitment(&a, &[9; 16], &c).is_ok());
        assert_eq!(check_commitment(&b, &[9; 16], &c), Err(SealError::Open));
        assert_eq!(check_commitment(&a, &[8; 16], &c), Err(SealError::Open));
    }

    /// RFC 4231 test case 2's construction (HMAC-SHA-256 of "what do ya want for nothing?" under
    /// "Jefe"), checked through the same context the commitment uses, so the MAC is the standard's.
    #[test]
    fn the_mac_is_rfc_4231s() {
        let mac = hmac::Key::new(hmac::HMAC_SHA256, b"Jefe");
        let tag = hmac::sign(&mac, b"what do ya want for nothing?");
        let expected: [u8; 32] = [
            0x5b, 0xdc, 0xc1, 0x46, 0xbf, 0x60, 0x75, 0x4e, 0x6a, 0x04, 0x24, 0x26, 0x08, 0x95,
            0x75, 0xc7, 0x5a, 0x00, 0x3f, 0x08, 0x9d, 0x27, 0x39, 0x83, 0x9d, 0xec, 0x58, 0xb9,
            0x64, 0xec, 0x38, 0x43,
        ];
        assert_eq!(tag.as_ref(), &expected);
    }
}
