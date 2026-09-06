use super::ManagedStoreError;
use crate::pending::OperationContext;
use focal_model::{ManagedRequestKey, RequestId, RequestStreamIdentity};
use std::{fmt, str::FromStr};

/// Explicit managed namespace. Parsing an ID never allocates or admits work.
/// Cluster, ledger and authenticated principal come from the persisted context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedOperationId {
    slot: u32,
    generation: u64,
    ordinal: u64,
    request: RequestId,
}
impl ManagedOperationId {
    pub fn from_key(key: ManagedRequestKey) -> Result<Self, ManagedStoreError> {
        if !key.is_valid() {
            return Err(ManagedStoreError::InvalidId);
        }
        Ok(Self {
            slot: key.stream.slot,
            generation: key.stream.generation,
            ordinal: key.ordinal,
            request: key.id,
        })
    }
    pub fn key(self, context: OperationContext) -> ManagedRequestKey {
        ManagedRequestKey {
            stream: RequestStreamIdentity {
                cluster: context.cluster,
                ledger: context.ledger,
                principal: context.principal,
                slot: self.slot,
                generation: self.generation,
            },
            ordinal: self.ordinal,
            id: self.request,
        }
    }
}
impl fmt::Display for ManagedOperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "m1:{:08x}:{:016x}:{:016x}:{:032x}",
            self.slot,
            self.generation,
            self.ordinal,
            u128::from_be_bytes(self.request.0),
        )
    }
}
impl FromStr for ManagedOperationId {
    type Err = ManagedStoreError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut fields = value.split(':');
        if fields.next() != Some("m1") {
            return Err(ManagedStoreError::InvalidId);
        }
        fn field(value: Option<&str>, width: usize) -> Result<&str, ManagedStoreError> {
            value
                .filter(|text| {
                    text.len() == width
                        && text
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
                .ok_or(ManagedStoreError::InvalidId)
        }
        let slot = u32::from_str_radix(field(fields.next(), 8)?, 16)
            .map_err(|_| ManagedStoreError::InvalidId)?;
        let generation = u64::from_str_radix(field(fields.next(), 16)?, 16)
            .map_err(|_| ManagedStoreError::InvalidId)?;
        let ordinal = u64::from_str_radix(field(fields.next(), 16)?, 16)
            .map_err(|_| ManagedStoreError::InvalidId)?;
        let request = u128::from_str_radix(field(fields.next(), 32)?, 16)
            .map_err(|_| ManagedStoreError::InvalidId)?;
        if fields.next().is_some() || generation == 0 || ordinal == 0 || request == 0 {
            return Err(ManagedStoreError::InvalidId);
        }
        Ok(Self {
            slot,
            generation,
            ordinal,
            request: RequestId::from_u128(request),
        })
    }
}
