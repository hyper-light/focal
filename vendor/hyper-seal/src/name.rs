//! The name a store gives sealed content (`docs/seal.md` §7): HMAC-SHA-256 under the tenant's naming
//! key of the content's plaintext hash, truncated to 128 bits. Without the tenant's key a name
//! confirms nothing about a guessed plaintext (Bellare, Keelveedhi and Ristenpart, EUROCRYPT 2013;
//! DupLESS, USENIX Security 2013); within the tenant, names are equal exactly when hashes are, so
//! deduplication and replication's missing-set exchange compare names.

use aws_lc_rs::hmac;

use crate::{SealError, Secret32, guarded};

/// Bytes of a name: 128 bits, collision-resistant to 2^64 names within a tenant.
pub const NAME: usize = 16;

/// The label names are computed under, so a name is never another use's MAC.
const NAME_LABEL: &[u8] = b"hyper-seal name";

/// A tenant's naming key: a child of its tenant key, so it is erased with the tenant.
pub struct Namer {
    key: hmac::Key,
}

impl Namer {
    /// The namer for the tenant whose naming key is `key`.
    pub fn new(key: &Secret32) -> Result<Self, SealError> {
        let key = guarded(SealError::Seal, || {
            Ok::<_, ()>(hmac::Key::new(hmac::HMAC_SHA256, key.bytes()))
        })?;
        Ok(Self { key })
    }

    /// The name of content whose plaintext hash (the consumer's: BLAKE3, SHA-256) is `hash`.
    pub fn name(&self, hash: &[u8]) -> Result<[u8; NAME], SealError> {
        let tag = guarded(SealError::Seal, || {
            let mut ctx = hmac::Context::with_key(&self.key);
            ctx.update(NAME_LABEL);
            ctx.update(hash);
            Ok::<_, ()>(ctx.sign())
        })?;
        let (name, _) = tag.as_ref().split_at_checked(NAME).ok_or(SealError::Seal)?;
        name.try_into().map_err(|_| SealError::Seal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_equal_exactly_when_hashes_are_within_a_tenant() {
        let namer = Namer::new(&crate::random_secret().unwrap()).unwrap();
        let a = namer.name(&[1; 32]).unwrap();
        assert_eq!(a, namer.name(&[1; 32]).unwrap());
        assert_ne!(a, namer.name(&[2; 32]).unwrap());
    }

    #[test]
    fn another_tenant_names_the_same_content_otherwise() {
        let one = Namer::new(&crate::random_secret().unwrap()).unwrap();
        let two = Namer::new(&crate::random_secret().unwrap()).unwrap();
        assert_ne!(one.name(&[1; 32]).unwrap(), two.name(&[1; 32]).unwrap());
    }
}
