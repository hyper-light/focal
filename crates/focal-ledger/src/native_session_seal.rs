//! The seal record (the audit's F12): a session decision that moves the
//! closed outcomes of the committed prefix into a bundle under custody,
//! applied alike on every replica. The record names what every replica
//! derives the same seal from — the prefix, the floors the pressure forces,
//! the bound of the derivation — and what the authority sealed: the bundle's
//! root and length, the count it must derive, and a fold of older seal rows
//! when the index reached its bound. Variable length: the floors.
use super::*;
use focal_core::native::seal::{Fold, SealBound, SealedPrincipal};
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
        let principals = u64::try_from(self.bound.principals)
            .map_err(|_| NativeSessionError::Capacity("seal principals"))?;
        let rows = u64::try_from(self.bound.rows)
            .map_err(|_| NativeSessionError::Capacity("seal rows"))?;
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
        let floors = u32::try_from(self.floors.len())
            .map_err(|_| NativeSessionError::Capacity("seal floors"))?;
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

/// The anchored seal record (F12, level 5): a seal whose plan names what
/// it seals — each principal's closed generations where it was derived, and
/// the decisions' outcomes at or below that prefix — so the commits made
/// while its bundle is under custody leave it applicable, and whose floors
/// are the pressure's where it applies. A closing seal names floors and
/// nothing to seal (no bundle, count zero): the owner proposes it as soon
/// as the window comes under pressure, and what it closes is sealed whole
/// by a later seal. Variable length: the floors and the principals.
pub const ANCHORED_MAGIC: [u8; 8] = *b"FOCALSO2";
const ANCHORED_VERSION: u16 = 1;
const ANCHORED_DOMAIN: &str = "focal.native.session.anchored-seal-record.v1";
/// Magic, version, ledger, the expected prefix, the ordinal, the prefix
/// derived at, the bundle's root and length, the count, the outcome bound,
/// the fold flag and its four fields, and the counts of floors and
/// principals.
const ANCHORED_HEAD: usize = 8 + 2 + 32 + 8 + 8 + 8 + 32 + 8 + 8 + 8 + 1 + (8 + 8 + 32 + 8) + 4 + 4;
const PRINCIPAL_BYTES: usize = 16 + 8 + 8;
/// The most principals one anchored record names: what fits the bound after
/// the head, the most floors a pressure forces in one record and the digest.
pub const MAX_PRINCIPALS: usize =
    (MAX_RECORD_BYTES - ANCHORED_HEAD - DIGEST - MAX_ANCHORED_FLOORS * FLOOR_BYTES)
        / PRINCIPAL_BYTES;
/// The most floors one anchored record names: half the record.
pub const MAX_ANCHORED_FLOORS: usize = (MAX_RECORD_BYTES / 2) / FLOOR_BYTES;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchoredSealRecord {
    pub ledger: LedgerId,
    /// The native prefix the record applies at: inert anywhere else.
    pub expected_prefix: SessionSeq,
    /// The seal's ordinal, the one its bundle was derived under.
    pub ordinal: u64,
    /// The native prefix the plan was derived at; a closing seal's is its
    /// own prefix.
    pub through: SessionSeq,
    pub bundle: ContentHash,
    pub bytes: u64,
    /// The rows sealed: zero for a closing seal.
    pub count: u64,
    /// The resident outcome bound the authority derived the floors under.
    pub outcome_limit: u64,
    pub fold: Option<Fold>,
    /// The floors the pressure forces where the record applies, in the
    /// order the core derives them.
    pub floors: Vec<(ParticipantId, RequestEpoch)>,
    /// The generations sealed, by principal in order.
    pub principals: Vec<SealedPrincipal>,
}

impl AnchoredSealRecord {
    /// A closing seal (count zero, no bundle).
    pub fn is_closing(&self) -> bool {
        self.count == 0
    }
    fn check(&self) -> Result<(), NativeSessionError> {
        let closing = self.is_closing();
        let ordered = self
            .principals
            .windows(2)
            .all(|pair| matches!(pair, [left, right] if left.principal < right.principal));
        let ranges = self.principals.iter().all(|sealed| {
            !sealed.principal.is_zero() && sealed.first.0 != 0 && sealed.first.0 <= sealed.last.0
        });
        let floors = self
            .floors
            .iter()
            .all(|(principal, minimum)| !principal.is_zero() && minimum.0 > 1)
            && self
                .floors
                .windows(2)
                .all(|pair| matches!(pair, [left, right] if left.0 != right.0));
        let shape = if closing {
            !self.floors.is_empty()
                && self.principals.is_empty()
                && self.bundle.0 == [0; 32]
                && self.bytes == 0
                && self.fold.is_none()
                && self.through == self.expected_prefix
        } else {
            self.bundle.0 != [0; 32] && self.bytes != 0
        };
        let fold = self.fold.is_none_or(|fold| {
            fold.first != 0
                && fold.first < fold.last
                && fold.last < self.ordinal
                && fold.bundle.0 != [0; 32]
                && fold.bytes != 0
        });
        if self.ledger.tenant.is_zero()
            || self.ledger.session.is_zero()
            || self.through.0 == 0
            || self.through > self.expected_prefix
            || self.ordinal == 0
            || self.outcome_limit == 0
            || self.floors.len() > MAX_ANCHORED_FLOORS
            || self.principals.len() > MAX_PRINCIPALS
            || !ordered
            || !ranges
            || !floors
            || !shape
            || !fold
        {
            return Err(NativeSessionError::Corrupt);
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, NativeSessionError> {
        self.check()?;
        let total = add(
            add(
                add(ANCHORED_HEAD, self.floors.len().saturating_mul(FLOOR_BYTES))?,
                self.principals.len().saturating_mul(PRINCIPAL_BYTES),
            )?,
            DIGEST,
        )?;
        let mut bytes = reserved::<u8>(total)?;
        bytes.extend_from_slice(&ANCHORED_MAGIC);
        bytes.extend_from_slice(&ANCHORED_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.ledger.tenant.0);
        bytes.extend_from_slice(&self.ledger.session.0);
        bytes.extend_from_slice(&self.expected_prefix.0.to_le_bytes());
        bytes.extend_from_slice(&self.ordinal.to_le_bytes());
        bytes.extend_from_slice(&self.through.0.to_le_bytes());
        bytes.extend_from_slice(&self.bundle.0);
        bytes.extend_from_slice(&self.bytes.to_le_bytes());
        bytes.extend_from_slice(&self.count.to_le_bytes());
        bytes.extend_from_slice(&self.outcome_limit.to_le_bytes());
        match &self.fold {
            Some(fold) => {
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
        let floors = u32::try_from(self.floors.len())
            .map_err(|_| NativeSessionError::Capacity("seal floors"))?;
        let principals = u32::try_from(self.principals.len())
            .map_err(|_| NativeSessionError::Capacity("seal principals"))?;
        bytes.extend_from_slice(&floors.to_le_bytes());
        bytes.extend_from_slice(&principals.to_le_bytes());
        for (principal, minimum) in &self.floors {
            bytes.extend_from_slice(&principal.0);
            bytes.extend_from_slice(&minimum.0.to_le_bytes());
        }
        for sealed in &self.principals {
            bytes.extend_from_slice(&sealed.principal.0);
            bytes.extend_from_slice(&sealed.first.0.to_le_bytes());
            bytes.extend_from_slice(&sealed.last.0.to_le_bytes());
        }
        let digest = blake3::Hasher::new_derive_key(ANCHORED_DOMAIN)
            .update(&bytes)
            .finalize();
        bytes.extend_from_slice(digest.as_bytes());
        if bytes.len() != total {
            return Err(NativeSessionError::Corrupt);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, NativeSessionError> {
        if bytes.len() > MAX_RECORD_BYTES || bytes.len() < add(ANCHORED_HEAD, DIGEST)? {
            return Err(NativeSessionError::Corrupt);
        }
        let (payload, digest) = bytes
            .split_at_checked(bytes.len().saturating_sub(DIGEST))
            .ok_or(NativeSessionError::Corrupt)?;
        let expected = blake3::Hasher::new_derive_key(ANCHORED_DOMAIN)
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
        let u32_of = |bytes: &[u8]| -> Result<usize, NativeSessionError> {
            usize::try_from(u32::from_le_bytes(
                bytes.try_into().map_err(|_| NativeSessionError::Corrupt)?,
            ))
            .map_err(|_| NativeSessionError::Corrupt)
        };
        if take(8)? != ANCHORED_MAGIC || take(2)? != ANCHORED_VERSION.to_le_bytes() {
            return Err(NativeSessionError::Corrupt);
        }
        let ledger = LedgerId {
            tenant: focal_model::TenantId(fixed16(take(16)?)?),
            session: focal_model::SessionId(fixed16(take(16)?)?),
        };
        let expected_prefix = SessionSeq(u64_of(take(8)?)?);
        let ordinal = u64_of(take(8)?)?;
        let through = SessionSeq(u64_of(take(8)?)?);
        let bundle = ContentHash(fixed32(take(32)?)?);
        let length = u64_of(take(8)?)?;
        let count = u64_of(take(8)?)?;
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
        let floor_count = u32_of(take(4)?)?;
        let principal_count = u32_of(take(4)?)?;
        if floor_count > MAX_ANCHORED_FLOORS
            || principal_count > MAX_PRINCIPALS
            || add(
                add(ANCHORED_HEAD, floor_count.saturating_mul(FLOOR_BYTES))?,
                principal_count.saturating_mul(PRINCIPAL_BYTES),
            )? != payload.len()
        {
            return Err(NativeSessionError::Corrupt);
        }
        let mut floors = reserved::<(ParticipantId, RequestEpoch)>(floor_count)?;
        for _ in 0..floor_count {
            let principal = ParticipantId(fixed16(take(16)?)?);
            let minimum = RequestEpoch(u64_of(take(8)?)?);
            floors.push((principal, minimum));
        }
        let mut principals = reserved::<SealedPrincipal>(principal_count)?;
        for _ in 0..principal_count {
            let principal = ParticipantId(fixed16(take(16)?)?);
            let first = RequestEpoch(u64_of(take(8)?)?);
            let last = RequestEpoch(u64_of(take(8)?)?);
            principals.push(SealedPrincipal {
                principal,
                first,
                last,
            });
        }
        let record = Self {
            ledger,
            expected_prefix,
            ordinal,
            through,
            bundle,
            bytes: length,
            count,
            outcome_limit,
            fold,
            floors,
            principals,
        };
        // What the encoder refuses, the decoder refuses.
        record.check()?;
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

    fn anchored(floors: usize, principals: usize, closing: bool) -> AnchoredSealRecord {
        AnchoredSealRecord {
            ledger: LedgerId {
                tenant: focal_model::TenantId([1; 16]),
                session: focal_model::SessionId([2; 16]),
            },
            expected_prefix: SessionSeq(52),
            ordinal: 9,
            through: if closing {
                SessionSeq(52)
            } else {
                SessionSeq(40)
            },
            bundle: ContentHash(if closing { [0; 32] } else { [3; 32] }),
            bytes: if closing { 0 } else { 4096 },
            count: if closing { 0 } else { 17 },
            outcome_limit: 1_000_000,
            fold: (!closing).then_some(Fold {
                first: 1,
                last: 8,
                bundle: ContentHash([4; 32]),
                bytes: 8192,
            }),
            floors: (0..floors)
                .map(|i| {
                    (
                        ParticipantId::from_u128(100 + u128::try_from(i).unwrap()),
                        RequestEpoch(7),
                    )
                })
                .collect(),
            principals: (0..principals)
                .map(|i| SealedPrincipal {
                    principal: ParticipantId::from_u128(500 + u128::try_from(i).unwrap()),
                    first: RequestEpoch(2),
                    last: RequestEpoch(3),
                })
                .collect(),
        }
    }

    /// An anchored record round trips with floors, principals and a fold,
    /// at the bounds of both; a closing seal (floors, nothing sealed)
    /// round trips; one past either bound is refused.
    #[test]
    fn an_anchored_record_round_trips_sealing_and_closing() {
        for (floors, principals, closing) in [
            (0, 1, false),
            (3, 2, false),
            (MAX_ANCHORED_FLOORS, MAX_PRINCIPALS, false),
            (1, 0, true),
            (MAX_ANCHORED_FLOORS, 0, true),
        ] {
            let record = anchored(floors, principals, closing);
            let bytes = record.encode().unwrap();
            assert!(bytes.len() <= MAX_RECORD_BYTES);
            assert!(bytes.starts_with(&ANCHORED_MAGIC));
            assert_eq!(AnchoredSealRecord::decode(&bytes).unwrap(), record);
            assert_eq!(record.is_closing(), closing);
        }
        assert!(
            anchored(MAX_ANCHORED_FLOORS + 1, 0, false)
                .encode()
                .is_err()
        );
        assert!(anchored(0, MAX_PRINCIPALS + 1, false).encode().is_err());
    }

    /// What an anchored record cannot be is refused at both ends: a byte
    /// changed, a truncation, a closing seal with nothing to close or with
    /// something to seal, a seal with no bundle, a plan derived after the
    /// prefix it applies at, principals out of order or a range backwards,
    /// a fold at or above the ordinal, the same principal's floor twice.
    #[test]
    fn an_anchored_record_that_lies_about_itself_is_refused() {
        let bytes = anchored(2, 2, false).encode().unwrap();
        for at in [0, 9, 20, 60, 100, ANCHORED_HEAD + 3, bytes.len() - 1] {
            let mut corrupt = bytes.clone();
            corrupt[at] ^= 0x40;
            assert!(AnchoredSealRecord::decode(&corrupt).is_err(), "byte {at}");
        }
        assert!(AnchoredSealRecord::decode(&bytes[..bytes.len() - 1]).is_err());
        assert!(anchored(0, 0, true).encode().is_err());
        let mut sealing_closer = anchored(1, 1, true);
        sealing_closer.count = 0;
        assert!(sealing_closer.encode().is_err());
        let mut bundleless = anchored(0, 1, false);
        bundleless.bundle = ContentHash([0; 32]);
        assert!(bundleless.encode().is_err());
        let mut ahead = anchored(0, 1, false);
        ahead.through = SessionSeq(53);
        assert!(ahead.encode().is_err());
        let mut unordered = anchored(0, 2, false);
        unordered.principals.swap(0, 1);
        assert!(unordered.encode().is_err());
        let mut backwards = anchored(0, 1, false);
        backwards.principals[0].last = RequestEpoch(1);
        assert!(backwards.encode().is_err());
        let mut fold = anchored(0, 1, false);
        fold.fold = Some(Fold {
            first: 1,
            last: 9,
            bundle: ContentHash([4; 32]),
            bytes: 1,
        });
        assert!(fold.encode().is_err());
        let mut twice = anchored(2, 0, true);
        twice.floors[1].0 = twice.floors[0].0;
        assert!(twice.encode().is_err());
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
