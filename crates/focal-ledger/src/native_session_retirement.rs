//! The committed retirement record (26 §4): one session decision naming a
//! family's root, the bundle that holds its rows, the prefix the bundle
//! claims and the outcome bound the authority checked the retirement
//! against. Every replica derives the same family from the same committed
//! state at the named prefix and retires it alike; a record whose prefix has
//! passed, or whose family the committed state refuses, is inert on every
//! replica alike, never a refusal. One refusal is not inert: a record whose
//! family the state accepts but whose outcome this replica's own bound
//! cannot hold. The authority proved the retirement fits the bound the
//! record carries, so a replica configured below it fails closed rather
//! than diverge silently. A version-1 record carries no bound (it was
//! proposed without the check): where it does not fit it is inert, and the
//! replica counts it. Records carry no rows: the bundle is content under
//! custody, and the core recomputes what leaves.
use super::*;
use focal_model::ClaimId;

pub const MAGIC: [u8; 8] = *b"FOCALRT1";
pub const VERSION: u16 = 2;
const HASH_DOMAIN: &str = "focal.native.session.retirement-record.v2";
const HASH_DOMAIN_V1: &str = "focal.native.session.retirement-record.v1";
/// magic 8 + version 2 + ledger 32 + prefix 8 + root 16 + bundle 32 + length 8 + through 8 + outcome bound 8 + digest 32
pub const BYTES: usize = 154;
/// A version-1 record: the same without the outcome bound.
pub const BYTES_V1: usize = 146;
const DIGEST: usize = 32;

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
    /// The outcome bound the authority checked the retirement against (its
    /// `limits.outcomes`): the retirement's outcome, one past the prefix it
    /// was derived at, fits it. `None` in a version-1 record, which was
    /// proposed without the check.
    pub outcome_limit: Option<u64>,
}

impl RetirementRecord {
    /// Write the record in its current version. A record without a bound,
    /// or whose prefix its bound does not hold one past, is not a record
    /// the authority's checks produce and is refused.
    pub fn write_into(&self, output: &mut [u8; BYTES]) -> Result<(), NativeSessionError> {
        let limit = self
            .outcome_limit
            .filter(|limit| self.expected_prefix.0 < *limit)
            .ok_or(NativeSessionError::Corrupt)?;
        let mut offset = 0usize;
        put(output, &mut offset, &MAGIC);
        put(output, &mut offset, &VERSION.to_le_bytes());
        self.put_body(output, &mut offset);
        put(output, &mut offset, &limit.to_le_bytes());
        let payload = digest(HASH_DOMAIN, output.get(..offset).unwrap_or(&[]));
        put(output, &mut offset, &payload);
        Ok(())
    }
    /// Write the record as version 1 wrote it: without the bound. Nothing
    /// proposes this version any more; the tests use it for the records
    /// old logs still carry.
    #[cfg(test)]
    pub(crate) fn write_v1_into(&self, output: &mut [u8; BYTES_V1]) {
        let mut offset = 0usize;
        put(output, &mut offset, &MAGIC);
        put(output, &mut offset, &1u16.to_le_bytes());
        self.put_body(output, &mut offset);
        let payload = digest(HASH_DOMAIN_V1, output.get(..offset).unwrap_or(&[]));
        put(output, &mut offset, &payload);
    }
    /// The fields every version carries, after the magic and the version.
    fn put_body(&self, output: &mut [u8], offset: &mut usize) {
        put(output, offset, &self.ledger.tenant.0);
        put(output, offset, &self.ledger.session.0);
        put(output, offset, &self.expected_prefix.0.to_le_bytes());
        put(output, offset, &self.root.0);
        put(output, offset, &self.bundle.0);
        put(output, offset, &self.bytes.to_le_bytes());
        put(output, offset, &self.through.0.to_le_bytes());
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, NativeSessionError> {
        let version = bytes
            .get(8..10)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_le_bytes)
            .ok_or(NativeSessionError::Corrupt)?;
        let (length, domain) = match version {
            1 => (BYTES_V1, HASH_DOMAIN_V1),
            VERSION => (BYTES, HASH_DOMAIN),
            _ => return Err(NativeSessionError::Corrupt),
        };
        if bytes.len() != length {
            return Err(NativeSessionError::Corrupt);
        }
        let (payload, trailer) = bytes
            .split_at_checked(length.saturating_sub(DIGEST))
            .ok_or(NativeSessionError::Corrupt)?;
        if digest(domain, payload) != trailer {
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
        if take(8)? != MAGIC || take(2)? != version.to_le_bytes() {
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
        let outcome_limit = if version == VERSION {
            Some(u64::from_le_bytes(fixed8(take(8)?)?))
        } else {
            None
        };
        if ledger.tenant.is_zero()
            || ledger.session.is_zero()
            || root.is_zero()
            || bundle.0 == [0; 32]
            || bytes == 0
            || through.0 == 0
            || expected_prefix < through
            // The authority's own check: the retirement's outcome, one past
            // the prefix, fits the bound it carries.
            || outcome_limit.is_some_and(|limit| expected_prefix.0 >= limit)
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
            outcome_limit,
        })
    }
}
fn put(output: &mut [u8], offset: &mut usize, bytes: &[u8]) {
    let end = offset.saturating_add(bytes.len());
    if let Some(slot) = output.get_mut(*offset..end) {
        slot.copy_from_slice(bytes);
    }
    *offset = end;
}
fn digest(domain: &str, payload: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new_derive_key(domain);
    hash.update(payload);
    *hash.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> RetirementRecord {
        RetirementRecord {
            ledger: LedgerId {
                tenant: focal_model::TenantId([3; 16]),
                session: focal_model::SessionId([4; 16]),
            },
            expected_prefix: SessionSeq(12),
            root: ClaimId([5; 16]),
            bundle: ContentHash([6; 32]),
            bytes: 4096,
            through: SessionSeq(11),
            outcome_limit: Some(64),
        }
    }
    #[test]
    fn a_record_round_trips_and_every_corruption_is_refused() {
        let record = record();
        let mut bytes = [0u8; BYTES];
        record.write_into(&mut bytes).unwrap();
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
        stale.write_into(&mut bytes).unwrap();
        assert!(RetirementRecord::decode(&bytes).is_err());
    }
    /// The bound the record carries is the authority's check: a record
    /// without one, or whose prefix its bound does not hold one past, is
    /// not written, and one that reads so is refused.
    #[test]
    fn a_record_carries_the_bound_its_retirement_fits() {
        let mut bytes = [0u8; BYTES];
        for limit in [None, Some(0), Some(12)] {
            let unfit = RetirementRecord {
                outcome_limit: limit,
                ..record()
            };
            assert!(
                matches!(
                    unfit.write_into(&mut bytes),
                    Err(NativeSessionError::Corrupt)
                ),
                "{limit:?}"
            );
        }
        let exact = RetirementRecord {
            outcome_limit: Some(13),
            ..record()
        };
        exact.write_into(&mut bytes).unwrap();
        assert_eq!(RetirementRecord::decode(&bytes).unwrap(), exact);
        // The same bytes with a bound the prefix reaches, digested as the
        // writer would have: refused for the bound, not the digest.
        let mut forged = [0u8; BYTES];
        forged[..BYTES - DIGEST].copy_from_slice(&bytes[..BYTES - DIGEST]);
        forged[BYTES - DIGEST - 8..BYTES - DIGEST].copy_from_slice(&12u64.to_le_bytes());
        let trailer = digest(HASH_DOMAIN, &forged[..BYTES - DIGEST]);
        forged[BYTES - DIGEST..].copy_from_slice(&trailer);
        assert!(matches!(
            RetirementRecord::decode(&forged),
            Err(NativeSessionError::Corrupt)
        ));
    }
    /// A version-1 record, as the logs written before the bound still carry
    /// it: the bytes the version-1 writer produced for this record, decoded
    /// to the record without a bound; every corruption of them refused; the
    /// version-1 writer kept for the tests reproduces them exactly.
    #[test]
    fn a_version_one_record_decodes_as_it_was_written() {
        const GOLDEN: &str = "464f43414c525431010003030303030303030303030303030303040404040404040404040404040404040c0000000000000005050505050505050505050505050505060606060606060606060606060606060606060606060606060606060606060600100000000000000b00000000000000b3e2b79be20e3be7cfc393d4eeb5e7c073778e5cc4bdde6fa2406dd680954c7b";
        let golden: Vec<u8> = (0..GOLDEN.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&GOLDEN[index..index + 2], 16).unwrap())
            .collect();
        assert_eq!(golden.len(), BYTES_V1);
        let expected = RetirementRecord {
            outcome_limit: None,
            ..record()
        };
        assert_eq!(RetirementRecord::decode(&golden).unwrap(), expected);
        let mut written = [0u8; BYTES_V1];
        expected.write_v1_into(&mut written);
        assert_eq!(written.as_slice(), golden.as_slice());
        for index in 0..BYTES_V1 {
            let mut flipped = written;
            flipped[index] ^= 0x40;
            assert!(RetirementRecord::decode(&flipped).is_err(), "byte {index}");
        }
        assert!(RetirementRecord::decode(&written[..BYTES_V1 - 1]).is_err());
        // A version-1 body under the version-2 length or domain is refused.
        let mut long = [0u8; BYTES];
        long[..BYTES_V1].copy_from_slice(&written);
        assert!(RetirementRecord::decode(&long).is_err());
    }
}
