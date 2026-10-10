//! The plane's key schedule.
//!
//! An epoch's keys come from the secret the TLS session of the QUIC connection to the same peer
//! exports (RFC 8446 §7.5, `Connection::export_keying_material`). Both ends compute it, and no
//! other party can. Each direction gets its own key through HKDF-Expand-Label (RFC 8446 §7.1):
//! the initiator of the connection seals with the `initiator` key and opens with the `acceptor`
//! key, and the acceptor the reverse.
//!
//! A new connection gives a new epoch and new keys, so a node that restarts never seals under an
//! old key again. That closes the nonce reuse of a counter that starts at zero under a key that
//! outlives a process (mantle audit §11.8, slates' `Enrollment::sealer`).

use aws_lc_rs::aead::{AES_256_GCM, LessSafeKey, UnboundKey};
use aws_lc_rs::hkdf::{HKDF_SHA256, KeyType, Prk};

use crate::Refusal;

/// The TLS exporter label (RFC 8446 §7.5) both ends pass to `export_keying_material` for an
/// epoch's secret. Format: a wire identifier; changing it changes every key.
pub const EXPORTER_LABEL: &[u8] = b"EXPORTER-hyper-datagram";

/// The length of the exported secret: SHA-256's output, the hash HKDF-Expand-Label runs over
/// here (RFC 5869 §2.2 takes a PRK of at least the hash length).
pub const SECRET_BYTES: usize = 32;

/// HKDF-Expand-Label's label prefix (RFC 8446 §7.1). Format.
const LABEL_PREFIX: &[u8] = b"tls13 ";
/// The label of the key the connection's initiator seals with. Format.
const INITIATOR_LABEL: &[u8] = b"hyperdg init";
/// The label of the key the connection's acceptor seals with. Format.
const ACCEPTOR_LABEL: &[u8] = b"hyperdg accept";

/// The secret an epoch's keys are expanded from: [`SECRET_BYTES`] bytes the caller exported from
/// the QUIC connection's TLS session under [`EXPORTER_LABEL`] with an empty context.
pub struct ExporterSecret([u8; SECRET_BYTES]);

impl ExporterSecret {
    /// Wraps the exported bytes.
    pub fn new(bytes: [u8; SECRET_BYTES]) -> Self {
        Self(bytes)
    }
}

impl std::fmt::Debug for ExporterSecret {
    /// Redacts the secret.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ExporterSecret(..)")
    }
}

/// Which end of the QUIC connection this node was: it decides which key seals and which opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// This node opened the connection.
    Initiator,
    /// This node accepted it.
    Acceptor,
}

/// An epoch's two keys: the one this node seals with and the one it opens the peer's datagrams
/// with.
pub(crate) struct EpochKeys {
    pub(crate) seal: LessSafeKey,
    pub(crate) open: LessSafeKey,
}

/// AES-256-GCM's key length, as HKDF's output length.
struct Aes256Key;

impl KeyType for Aes256Key {
    fn len(&self) -> usize {
        AES_256_GCM.key_len()
    }
}

/// HKDF-Expand-Label's `HkdfLabel` (RFC 8446 §7.1) with an empty context, for `label` and an
/// output of `length` bytes.
fn hkdf_label(label: &[u8], length: usize) -> Result<Vec<u8>, Refusal> {
    let length = u16::try_from(length).map_err(|_| Refusal::KeySchedule)?;
    let full = LABEL_PREFIX
        .len()
        .checked_add(label.len())
        .ok_or(Refusal::KeySchedule)?;
    let full = u8::try_from(full).map_err(|_| Refusal::KeySchedule)?;
    let mut info = Vec::with_capacity(usize::from(full).saturating_add(4));
    info.extend_from_slice(&length.to_be_bytes());
    info.push(full);
    info.extend_from_slice(LABEL_PREFIX);
    info.extend_from_slice(label);
    info.push(0);
    Ok(info)
}

/// The AES-256-GCM key HKDF-Expand-Label gives for `label` from `prk`.
fn expand(prk: &Prk, label: &[u8]) -> Result<LessSafeKey, Refusal> {
    let info = hkdf_label(label, AES_256_GCM.key_len())?;
    let parts = [info.as_slice()];
    let okm = prk
        .expand(&parts, Aes256Key)
        .map_err(|_| Refusal::KeySchedule)?;
    let mut key = [0u8; 32];
    let out = key
        .get_mut(..AES_256_GCM.key_len())
        .ok_or(Refusal::KeySchedule)?;
    okm.fill(out).map_err(|_| Refusal::KeySchedule)?;
    let unbound = UnboundKey::new(&AES_256_GCM, out).map_err(|_| Refusal::KeySchedule)?;
    Ok(LessSafeKey::new(unbound))
}

/// An epoch's keys for a node in `role`, from `secret`.
pub(crate) fn epoch_keys(secret: &ExporterSecret, role: Role) -> Result<EpochKeys, Refusal> {
    let prk = Prk::new_less_safe(HKDF_SHA256, &secret.0);
    let initiator = expand(&prk, INITIATOR_LABEL)?;
    let acceptor = expand(&prk, ACCEPTOR_LABEL)?;
    Ok(match role {
        Role::Initiator => EpochKeys {
            seal: initiator,
            open: acceptor,
        },
        Role::Acceptor => EpochKeys {
            seal: acceptor,
            open: initiator,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_label_is_rfc_8446s_hkdf_label() {
        let info = hkdf_label(b"key", 16).unwrap();
        // RFC 8446 §7.1: uint16 length, opaque label<7..255> = "tls13 " + label, opaque context<0..255>
        assert_eq!(info, b"\x00\x10\x09tls13 key\x00".to_vec());
    }
}
