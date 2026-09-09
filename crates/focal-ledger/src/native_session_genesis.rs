//! The committed native genesis record. It is the first native-domain entry of a
//! physical consensus group and binds the ledger, content profile and decoder
//! identity to that cluster/group before any native mutation is admitted or
//! applied. A supported decoder hash alone never binds a ledger to a group.
use super::*;

pub const MAGIC: [u8; 8] = *b"FCNGENES";
pub const VERSION: u16 = 1;
const HASH_DOMAIN: &str = "focal.native.session.genesis-record.v1";
/// magic 8 + version 2 + cluster 16 + group 16 + ledger 32 + profile 1 + decoder 32 + genesis 32 + digest 32
pub const BYTES: usize = 171;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Genesis {
    pub cluster: [u8; 16],
    pub group: [u8; 16],
    pub ledger: LedgerId,
    pub profile: NativeContentProfile,
    pub decoder: ContentHash,
    pub genesis: ContentHash,
}

impl Genesis {
    /// The genesis hash is derived from the physical identity, not supplied.
    pub fn derive(
        cluster: [u8; 16],
        group: [u8; 16],
        ledger: LedgerId,
        profile: NativeContentProfile,
        decoder: ContentHash,
    ) -> Self {
        Self {
            cluster,
            group,
            ledger,
            profile,
            decoder,
            genesis: crate::native_checkpoint::genesis(cluster, group, ledger, profile, decoder),
        }
    }
    pub fn write_into(&self, output: &mut [u8; BYTES]) {
        let mut offset = 0usize;
        put(output, &mut offset, &MAGIC);
        put(output, &mut offset, &VERSION.to_le_bytes());
        put(output, &mut offset, &self.cluster);
        put(output, &mut offset, &self.group);
        put(output, &mut offset, &self.ledger.tenant.0);
        put(output, &mut offset, &self.ledger.session.0);
        put(
            output,
            &mut offset,
            &[match self.profile {
                NativeContentProfile::ProjectionOnly => 0,
                NativeContentProfile::AuthoredV1 => 1,
            }],
        );
        put(output, &mut offset, &self.decoder.0);
        put(output, &mut offset, &self.genesis.0);
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
        if take(8)? != MAGIC || take(2)? != VERSION.to_le_bytes() {
            return Err(NativeSessionError::Corrupt);
        }
        let cluster = fixed16(take(16)?)?;
        let group = fixed16(take(16)?)?;
        let ledger = LedgerId {
            tenant: focal_model::TenantId(fixed16(take(16)?)?),
            session: focal_model::SessionId(fixed16(take(16)?)?),
        };
        let profile = match take(1)? {
            [0] => NativeContentProfile::ProjectionOnly,
            [1] => NativeContentProfile::AuthoredV1,
            _ => return Err(NativeSessionError::Corrupt),
        };
        let decoder = ContentHash(fixed32(take(32)?)?);
        let genesis = ContentHash(fixed32(take(32)?)?);
        let value = Self {
            cluster,
            group,
            ledger,
            profile,
            decoder,
            genesis,
        };
        if value != Self::derive(cluster, group, ledger, profile, decoder) {
            return Err(NativeSessionError::Corrupt);
        }
        Ok(value)
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
