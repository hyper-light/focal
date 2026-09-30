//! The seal record (the audit's F12): a session decision that moves the
//! closed outcomes of the committed prefix into a bundle under custody,
//! applied alike on every replica. The record names what every replica
//! derives the same seal from — the prefix, the floors the pressure forces,
//! the bound of the derivation — and what the authority sealed: the bundle's
//! root and length, the count it must derive, and a fold of older seal rows
//! when the index reached its bound. Variable length: the floors.
use super::*;
use focal_core::native::seal::{Fold, SealBound};
use focal_model::{ParticipantId, RequestEpoch};

pub const MAGIC: [u8; 8] = *b"FOCALSO1";
pub const VERSION: u16 = 1;
const HASH_DOMAIN: &str = "focal.native.session.seal-record.v1";
/// The largest encoded seal record: the movement record's bound (25 §6),
/// so a session record is never larger than one message.
pub const MAX_RECORD_BYTES: usize = 64 * 1024;
const DIGEST: usize = 32;
/// Magic, version, ledger, the expected prefix, the prefix derived at, the
/// bundle's root and length, the count, the bound (principals, rows), the
/// outcome bound the floors were derived under, the fold flag and its four
/// fields, and the floors' count.
const HEAD: usize = 8 + 2 + 32 + 8 + 8 + 32 + 8 + 8 + 8 + 8 + 8 + 1 + (8 + 8 + 32 + 8) + 4;
const FLOOR_BYTES: usize = 16 + 8;
/// The most floors one record names: what fits the bound after the head
/// and the digest.
pub const MAX_FLOORS: usize = (MAX_RECORD_BYTES - HEAD - DIGEST) / FLOOR_BYTES;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealRecord {
    pub ledger: LedgerId,
    /// The native prefix the seal was derived at: applied only there.
    pub expected_prefix: SessionSeq,
    pub through: SessionSeq,
    pub bundle: ContentHash,
    pub bytes: u64,
    pub count: u64,
    pub bound: SealBound,
    /// The resident outcome bound the authority derived the floors under:
    /// a replica configured otherwise derives other floors, and names the
    /// difference (`OutcomeBound`) instead of reading a divergence.
    pub outcome_limit: u64,
    /// The floors the pressure forces, in the order the core derives them.
    pub floors: Vec<(ParticipantId, RequestEpoch)>,
    pub fold: Option<Fold>,
}

impl SealRecord {
    pub fn encode(&self) -> Result<Vec<u8>, NativeSessionError> {
        if self.floors.len() > MAX_FLOORS
            || self.ledger.tenant.is_zero()
            || self.ledger.session.is_zero()
            || self.through != self.expected_prefix
            || self.through.0 == 0
            || self.bundle.0 == [0; 32]
            || self.bytes == 0
            || self.count == 0
            || self.bound.principals == 0
            || self.bound.rows == 0
            || self.outcome_limit == 0
        {
            return Err(NativeSessionError::Corrupt);
        }
        let total = add(
            add(HEAD, self.floors.len().saturating_mul(FLOOR_BYTES))?,
            DIGEST,
        )?;
        let mut bytes = reserved::<u8>(total)?;
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.ledger.tenant.0);
        bytes.extend_from_slice(&self.ledger.session.0);
        bytes.extend_from_slice(&self.expected_prefix.0.to_le_bytes());
        bytes.extend_from_slice(&self.through.0.to_le_bytes());
        bytes.extend_from_slice(&self.bundle.0);
        bytes.extend_from_slice(&self.bytes.to_le_bytes());
        bytes.extend_from_slice(&self.count.to_le_bytes());
        let principals =
            u64::try_from(self.bound.principals).map_err(|_| NativeSessionError::Capacity)?;
        let rows = u64::try_from(self.bound.rows).map_err(|_| NativeSessionError::Capacity)?;
        bytes.extend_from_slice(&principals.to_le_bytes());
        bytes.extend_from_slice(&rows.to_le_bytes());
        bytes.extend_from_slice(&self.outcome_limit.to_le_bytes());
        match &self.fold {
            Some(fold) => {
                if fold.first == 0
                    || fold.first >= fold.last
                    || fold.bundle.0 == [0; 32]
                    || fold.bytes == 0
                {
                    return Err(NativeSessionError::Corrupt);
                }
                bytes.push(1);
                bytes.extend_from_slice(&fold.first.to_le_bytes());
                bytes.extend_from_slice(&fold.last.to_le_bytes());
                bytes.extend_from_slice(&fold.bundle.0);
                bytes.extend_from_slice(&fold.bytes.to_le_bytes());
            }
            None => {
                bytes.push(0);
                bytes.extend_from_slice(&[0u8; 8 + 8 + 32 + 8]);
            }
        }
        let floors = u32::try_from(self.floors.len()).map_err(|_| NativeSessionError::Capacity)?;
        bytes.extend_from_slice(&floors.to_le_bytes());
        for (principal, minimum) in &self.floors {
            if principal.is_zero() || minimum.0 == 0 {
                return Err(NativeSessionError::Corrupt);
            }
            bytes.extend_from_slice(&principal.0);
            bytes.extend_from_slice(&minimum.0.to_le_bytes());
        }
        let digest = blake3::Hasher::new_derive_key(HASH_DOMAIN)
            .update(&bytes)
            .finalize();
        bytes.extend_from_slice(digest.as_bytes());
        if bytes.len() != total {
            return Err(NativeSessionError::Corrupt);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, NativeSessionError> {
        if bytes.len() > MAX_RECORD_BYTES || bytes.len() < add(HEAD, DIGEST)? {
            return Err(NativeSessionError::Corrupt);
        }
        let (payload, digest) = bytes
            .split_at_checked(bytes.len().saturating_sub(DIGEST))
            .ok_or(NativeSessionError::Corrupt)?;
        let expected = blake3::Hasher::new_derive_key(HASH_DOMAIN)
            .update(payload)
            .finalize();
        if digest != expected.as_bytes() {
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
        let u64_of = |bytes: &[u8]| -> Result<u64, NativeSessionError> {
            Ok(u64::from_le_bytes(
                bytes.try_into().map_err(|_| NativeSessionError::Corrupt)?,
            ))
        };
        if take(8)? != MAGIC || take(2)? != VERSION.to_le_bytes() {
            return Err(NativeSessionError::Corrupt);
        }
        let ledger = LedgerId {
            tenant: focal_model::TenantId(fixed16(take(16)?)?),
            session: focal_model::SessionId(fixed16(take(16)?)?),
        };
        let expected_prefix = SessionSeq(u64_of(take(8)?)?);
        let through = SessionSeq(u64_of(take(8)?)?);
        let bundle = ContentHash(fixed32(take(32)?)?);
        let length = u64_of(take(8)?)?;
        let count = u64_of(take(8)?)?;
        let principals =
            usize::try_from(u64_of(take(8)?)?).map_err(|_| NativeSessionError::Corrupt)?;
        let rows = usize::try_from(u64_of(take(8)?)?).map_err(|_| NativeSessionError::Corrupt)?;
        let outcome_limit = u64_of(take(8)?)?;
        let flag = take(1)?
            .first()
            .copied()
            .ok_or(NativeSessionError::Corrupt)?;
        let first = u64_of(take(8)?)?;
        let last = u64_of(take(8)?)?;
        let fold_bundle = ContentHash(fixed32(take(32)?)?);
        let fold_bytes = u64_of(take(8)?)?;
        let fold = match flag {
            0 => {
                if first != 0 || last != 0 || fold_bundle.0 != [0; 32] || fold_bytes != 0 {
                    return Err(NativeSessionError::Corrupt);
                }
                None
            }
            1 => Some(Fold {
                first,
                last,
                bundle: fold_bundle,
                bytes: fold_bytes,
            }),
            _ => return Err(NativeSessionError::Corrupt),
        };
        let floor_count = usize::try_from(u32::from_le_bytes(
            take(4)?
                .try_into()
                .map_err(|_| NativeSessionError::Corrupt)?,
        ))
        .map_err(|_| NativeSessionError::Corrupt)?;
        if floor_count > MAX_FLOORS
            || add(HEAD, floor_count.saturating_mul(FLOOR_BYTES))? != payload.len()
        {
            return Err(NativeSessionError::Corrupt);
        }
        let mut floors = reserved::<(ParticipantId, RequestEpoch)>(floor_count)?;
        for _ in 0..floor_count {
            let principal = ParticipantId(fixed16(take(16)?)?);
            let minimum = RequestEpoch(u64_of(take(8)?)?);
            if principal.is_zero()
                || minimum.0 == 0
                || floors
                    .last()
                    .is_some_and(|(previous, _): &(ParticipantId, RequestEpoch)| {
                        *previous == principal
                    })
            {
                return Err(NativeSessionError::Corrupt);
            }
            floors.push((principal, minimum));
        }
        let record = Self {
            ledger,
            expected_prefix,
            through,
            bundle,
            bytes: length,
            count,
            bound: SealBound { principals, rows },
            outcome_limit,
            floors,
            fold,
        };
        // What the encoder refuses, the decoder refuses.
        record.encode()?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(floors: usize, fold: bool) -> SealRecord {
        SealRecord {
            ledger: LedgerId {
                tenant: focal_model::TenantId([1; 16]),
                session: focal_model::SessionId([2; 16]),
            },
            expected_prefix: SessionSeq(40),
            through: SessionSeq(40),
            bundle: ContentHash([3; 32]),
            bytes: 4096,
            count: 17,
            bound: SealBound {
                principals: 64,
                rows: 30_000,
            },
            outcome_limit: 1_000_000,
            floors: (0..floors)
                .map(|i| {
                    (
                        ParticipantId::from_u128(100 + u128::try_from(i).unwrap()),
                        RequestEpoch(7),
                    )
                })
                .collect(),
            fold: fold.then_some(Fold {
                first: 1,
                last: 8,
                bundle: ContentHash([4; 32]),
                bytes: 8192,
            }),
        }
    }

    #[test]
    fn a_seal_record_round_trips_with_and_without_floors_and_a_fold() {
        for (floors, fold) in [(0, false), (3, false), (0, true), (MAX_FLOORS, true)] {
            let record = record(floors, fold);
            let bytes = record.encode().unwrap();
            assert!(bytes.len() <= MAX_RECORD_BYTES);
            assert_eq!(SealRecord::decode(&bytes).unwrap(), record);
        }
        assert!(record(MAX_FLOORS + 1, false).encode().is_err());
    }

    #[test]
    fn a_record_that_lies_about_itself_is_refused() {
        let bytes = record(2, true).encode().unwrap();
        for at in [0, 9, 20, 60, 100, HEAD + 3, bytes.len() - 1] {
            let mut corrupt = bytes.clone();
            corrupt[at] ^= 0x40;
            assert!(SealRecord::decode(&corrupt).is_err(), "byte {at}");
        }
        assert!(SealRecord::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut empty = record(0, false);
        empty.count = 0;
        assert!(empty.encode().is_err());
        let mut elsewhere = record(0, false);
        elsewhere.expected_prefix = SessionSeq(41);
        assert!(elsewhere.encode().is_err());
        let mut twice = record(2, false);
        twice.floors[1].0 = twice.floors[0].0;
        let bytes = twice.encode().unwrap();
        assert!(SealRecord::decode(&bytes).is_err());
    }
}
