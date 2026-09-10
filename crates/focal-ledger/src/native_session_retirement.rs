//! The committed retirement record (26 §4): one session decision naming a
//! family's root, the bundle that holds its rows and the prefix the bundle
//! claims. Every replica derives the same family from the same committed
//! state at the named prefix and retires it alike; a record whose prefix has
//! passed, or whose family the committed state refuses, is inert on every
//! replica alike, never a refusal. Records carry no rows: the bundle is
//! content under custody, and the core recomputes what leaves.
use super::*;
use focal_model::ClaimId;

pub const MAGIC: [u8; 8] = *b"FOCALRT1";
pub const VERSION: u16 = 1;
const HASH_DOMAIN: &str = "focal.native.session.retirement-record.v1";
/// magic 8 + version 2 + ledger 32 + prefix 8 + root 16 + bundle 32 + length 8 + through 8 + digest 32
pub const BYTES: usize = 146;

/// One family's retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetirementRecord {
    pub ledger: LedgerId,
    /// The native prefix the family was derived at; the record applies only
    /// there, and retiring the family advances the prefix by one.
    pub expected_prefix: SessionSeq,
    pub root: ClaimId,
    /// The archive bundle (`FCNARCHV`) holding every row of the family: the
    /// content root of an object in the ledger's tenant domain under the
    /// checkpoint class, and its length.
    pub bundle: ContentHash,
    pub bytes: u64,
    /// The prefix the bundle claims: at least the family's last event, at
    /// most the prefix it was derived at.
    pub through: SessionSeq,
}

impl RetirementRecord {
    pub fn write_into(&self, output: &mut [u8; BYTES]) {
        let mut offset = 0usize;
        put(output, &mut offset, &MAGIC);
        put(output, &mut offset, &VERSION.to_le_bytes());
        put(output, &mut offset, &self.ledger.tenant.0);
        put(output, &mut offset, &self.ledger.session.0);
        put(output, &mut offset, &self.expected_prefix.0.to_le_bytes());
        put(output, &mut offset, &self.root.0);
        put(output, &mut offset, &self.bundle.0);
        put(output, &mut offset, &self.bytes.to_le_bytes());
        put(output, &mut offset, &self.through.0.to_le_bytes());
        let payload = digest(output.get(..offset).unwrap_or(&[]));
        put(output, &mut offset, &payload);
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, NativeSessionError> {
        if bytes.len() != BYTES {
            return Err(NativeSessionError::Corrupt);
        }
        let (payload, trailer) = bytes
            .split_at_checked(BYTES.saturating_sub(32))
            .ok_or(NativeSessionError::Corrupt)?;
        if digest(payload) != trailer {
            return Err(NativeSessionError::Corrupt);
        }
        let mut offset = 0usize;
        let mut take = |length: usize| -> Result<&[u8], NativeSessionError> {
            let end = offset
                .checked_add(length)
                .ok_or(NativeSessionError::Corrupt)?;
            let value = payload
                .get(offset..end)
                .ok_or(NativeSessionError::Corrupt)?;
            offset = end;
            Ok(value)
        };
        let fixed16 = |bytes: &[u8]| -> Result<[u8; 16], NativeSessionError> {
            bytes.try_into().map_err(|_| NativeSessionError::Corrupt)
        };
        let fixed32 = |bytes: &[u8]| -> Result<[u8; 32], NativeSessionError> {
            bytes.try_into().map_err(|_| NativeSessionError::Corrupt)
        };
        let fixed8 = |bytes: &[u8]| -> Result<[u8; 8], NativeSessionError> {
            bytes.try_into().map_err(|_| NativeSessionError::Corrupt)
        };
        if take(8)? != MAGIC || take(2)? != VERSION.to_le_bytes() {
            return Err(NativeSessionError::Corrupt);
        }
        let ledger = LedgerId {
            tenant: focal_model::TenantId(fixed16(take(16)?)?),
            session: focal_model::SessionId(fixed16(take(16)?)?),
        };
        let expected_prefix = SessionSeq(u64::from_le_bytes(fixed8(take(8)?)?));
        let root = ClaimId(fixed16(take(16)?)?);
        let bundle = ContentHash(fixed32(take(32)?)?);
        let bytes = u64::from_le_bytes(fixed8(take(8)?)?);
        let through = SessionSeq(u64::from_le_bytes(fixed8(take(8)?)?));
        if ledger.tenant.is_zero()
            || ledger.session.is_zero()
            || root.is_zero()
            || bundle.0 == [0; 32]
            || bytes == 0
            || through.0 == 0
            || expected_prefix < through
        {
            return Err(NativeSessionError::Corrupt);
        }
        Ok(Self {
            ledger,
            expected_prefix,
            root,
            bundle,
            bytes,
            through,
        })
    }
}
fn put(output: &mut [u8; BYTES], offset: &mut usize, bytes: &[u8]) {
    let end = offset.saturating_add(bytes.len());
    if let Some(slot) = output.get_mut(*offset..end) {
        slot.copy_from_slice(bytes);
    }
    *offset = end;
}
fn digest(payload: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new_derive_key(HASH_DOMAIN);
    hash.update(payload);
    *hash.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_record_round_trips_and_every_corruption_is_refused() {
        let record = RetirementRecord {
            ledger: LedgerId {
                tenant: focal_model::TenantId([3; 16]),
                session: focal_model::SessionId([4; 16]),
            },
            expected_prefix: SessionSeq(12),
            root: ClaimId([5; 16]),
            bundle: ContentHash([6; 32]),
            bytes: 4096,
            through: SessionSeq(11),
        };
        let mut bytes = [0u8; BYTES];
        record.write_into(&mut bytes);
        assert!(bytes.starts_with(&MAGIC));
        assert_eq!(RetirementRecord::decode(&bytes).unwrap(), record);
        for index in 0..BYTES {
            let mut flipped = bytes;
            flipped[index] ^= 0x40;
            assert!(RetirementRecord::decode(&flipped).is_err(), "byte {index}");
        }
        assert!(RetirementRecord::decode(&bytes[..BYTES - 1]).is_err());
        let stale = RetirementRecord {
            through: SessionSeq(13),
            ..record
        };
        stale.write_into(&mut bytes);
        assert!(RetirementRecord::decode(&bytes).is_err());
    }
}
