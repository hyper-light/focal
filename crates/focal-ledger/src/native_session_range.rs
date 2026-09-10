//! The committed layout record (25 §4): one session decision that every
//! replica applies to its range group between native records, so replicas
//! hold one layout under one epoch. The record names the layout epoch it
//! applies to; a record whose epoch has passed is inert on every replica
//! alike, never a refusal. Records carry no rows: a split shares pages and a
//! merge shares every page on each replica independently.
use super::*;

pub const MAGIC: [u8; 8] = *b"FOCALRG1";
pub const VERSION: u16 = 1;
const HASH_DOMAIN: &str = "focal.native.session.layout-record.v1";
/// magic 8 + version 2 + ledger 32 + epoch 8 + tag 1 + affinity 16 + range 16 + digest 32
pub const BYTES: usize = 115;

/// One change to the layout of a session's range group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutOperation {
    /// Add a boundary at affinity `at`; the member holding it keeps its
    /// identity for the affinities below, `id` names the member from `at` on.
    Split { at: [u8; 16], id: RangeId },
    /// Remove the boundary after member `left`, joining it with the next
    /// under its identity.
    Merge { left: RangeId },
}

/// The durable identity of a session's origin member, the one member the
/// group has at genesis: derived from the genesis so every replica names it
/// alike before any layout record.
pub fn origin_member(genesis: &ContentHash) -> Result<RangeId, NativeSessionError> {
    let digest = blake3::derive_key("focal.native.range.origin.v1", &genesis.0);
    let bytes: [u8; 16] = digest
        .get(..16)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(NativeSessionError::Corrupt)?;
    let value = u128::from_le_bytes(bytes);
    if value == 0 {
        return Err(NativeSessionError::Corrupt);
    }
    Ok(RangeId(value))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutRecord {
    pub ledger: LedgerId,
    /// The layout epoch this record applies to; the applied layout then
    /// carries the next epoch.
    pub expected_epoch: u64,
    pub operation: LayoutOperation,
}

impl LayoutRecord {
    pub fn write_into(&self, output: &mut [u8; BYTES]) {
        let mut offset = 0usize;
        put(output, &mut offset, &MAGIC);
        put(output, &mut offset, &VERSION.to_le_bytes());
        put(output, &mut offset, &self.ledger.tenant.0);
        put(output, &mut offset, &self.ledger.session.0);
        put(output, &mut offset, &self.expected_epoch.to_le_bytes());
        match self.operation {
            LayoutOperation::Split { at, id } => {
                put(output, &mut offset, &[1]);
                put(output, &mut offset, &at);
                put(output, &mut offset, &id.0.to_le_bytes());
            }
            LayoutOperation::Merge { left } => {
                put(output, &mut offset, &[2]);
                put(output, &mut offset, &[0; 16]);
                put(output, &mut offset, &left.0.to_le_bytes());
            }
        }
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
        let expected_epoch = u64::from_le_bytes(fixed8(take(8)?)?);
        let tag = take(1)?
            .first()
            .copied()
            .ok_or(NativeSessionError::Corrupt)?;
        let at = fixed16(take(16)?)?;
        let range = RangeId(u128::from_le_bytes(fixed16(take(16)?)?));
        if range.0 == 0 || ledger.tenant.is_zero() || ledger.session.is_zero() {
            return Err(NativeSessionError::Corrupt);
        }
        let operation = match tag {
            1 => LayoutOperation::Split { at, id: range },
            2 if at == [0; 16] => LayoutOperation::Merge { left: range },
            _ => return Err(NativeSessionError::Corrupt),
        };
        Ok(Self {
            ledger,
            expected_epoch,
            operation,
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

    fn ledger() -> LedgerId {
        LedgerId {
            tenant: focal_model::TenantId::from_u128(7),
            session: focal_model::SessionId::from_u128(8),
        }
    }

    #[test]
    fn layout_records_round_trip_and_refuse_every_forgery() {
        for operation in [
            LayoutOperation::Split {
                at: [9; 16],
                id: RangeId(31),
            },
            LayoutOperation::Merge { left: RangeId(31) },
        ] {
            let record = LayoutRecord {
                ledger: ledger(),
                expected_epoch: 4,
                operation,
            };
            let mut bytes = [0u8; BYTES];
            record.write_into(&mut bytes);
            assert!(bytes.starts_with(&MAGIC));
            assert_eq!(LayoutRecord::decode(&bytes).unwrap(), record);
            // A changed byte anywhere fails the digest.
            for at in [0usize, 9, 20, 45, 60, 80] {
                let mut forged = bytes;
                forged[at] ^= 0x01;
                assert!(matches!(
                    LayoutRecord::decode(&forged),
                    Err(NativeSessionError::Corrupt)
                ));
            }
            assert!(matches!(
                LayoutRecord::decode(&bytes[..BYTES - 1]),
                Err(NativeSessionError::Corrupt)
            ));
        }
        // A merge carries no affinity; a zero range names nothing; an
        // unknown tag is refused, each with a recomputed digest.
        let mut merge = [0u8; BYTES];
        LayoutRecord {
            ledger: ledger(),
            expected_epoch: 1,
            operation: LayoutOperation::Merge { left: RangeId(31) },
        }
        .write_into(&mut merge);
        let resign = |bytes: &mut [u8; BYTES]| {
            let payload = digest(&bytes[..BYTES - 32]);
            bytes[BYTES - 32..].copy_from_slice(&payload);
        };
        let mut with_affinity = merge;
        with_affinity[51] = 1;
        resign(&mut with_affinity);
        assert!(matches!(
            LayoutRecord::decode(&with_affinity),
            Err(NativeSessionError::Corrupt)
        ));
        let mut zero_range = merge;
        zero_range[67..83].copy_from_slice(&[0; 16]);
        resign(&mut zero_range);
        assert!(matches!(
            LayoutRecord::decode(&zero_range),
            Err(NativeSessionError::Corrupt)
        ));
        let mut bad_tag = merge;
        bad_tag[50] = 3;
        resign(&mut bad_tag);
        assert!(matches!(
            LayoutRecord::decode(&bad_tag),
            Err(NativeSessionError::Corrupt)
        ));
    }
}
