//! The key an endpoint keeps across its restarts at one address (docs/transport.md §4ah).

use aws_lc_rs::{hkdf, hmac};
use hyper_quic::{EndpointConfig, HashedConnectionIdGenerator};

use crate::Refusal;

/// HKDF's context for the key the endpoint's stateless reset tokens are made under: one secret's
/// outputs kept apart by their contexts (RFC 5869 §3.2).
const RESET_INFO: &[u8] = b"hyper-transport stateless reset key v1";
/// HKDF's context for the key the endpoint's connection IDs carry the hash of.
const IDS_INFO: &[u8] = b"hyper-transport connection id key v1";

/// The key an endpoint keeps across its restarts at one address ([`crate::Endpoint::new_restartable`]):
/// the stateless resets it sends (RFC 9000 §10.3) and the connection IDs it knows for its own are
/// keyed from it. A restarted endpoint then answers a packet a peer sends on a connection its last
/// run held with a reset the peer verifies, and the peer is told at once that its connection closed,
/// as a TCP peer learns from a restarted host's RST (RFC 9293 §3.5.2), where it waited for its idle
/// timeout: an endpoint keyed afresh at each start drops such a packet, its connection ID one the new
/// key does not know, and a reset it did send carries a token the peer cannot verify.
///
/// One endpoint's alone, and as secret as the endpoint's private key: two endpoints sharing it reset
/// each other's connections when a packet reaches the one without its state (RFC 9000 §21.11), and
/// whoever holds it can reset any connection of the endpoint's. An owner derives it from what it
/// keeps of the endpoint's own, its private key, or keeps it beside that key.
pub struct RestartKey {
    reset: hmac::Key,
    ids: u64,
}

impl RestartKey {
    /// The key derived from `secret`, 32 bytes the owner keeps for this endpoint alone
    /// (HKDF-SHA256, RFC 5869): the key its reset tokens are made under (HMAC-SHA256 of the
    /// connection ID, RFC 9000 §10.3.2), and the key its connection IDs are hashed under.
    pub fn from_secret(secret: &[u8; 32]) -> Result<Self, Refusal> {
        let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]).extract(secret);
        let mut reset = [0u8; 32];
        prk.expand(&[RESET_INFO], Len(reset.len()))
            .and_then(|okm| okm.fill(&mut reset))
            .map_err(|_| Refusal::Configuration)?;
        let mut ids = [0u8; 8];
        prk.expand(&[IDS_INFO], Len(ids.len()))
            .and_then(|okm| okm.fill(&mut ids))
            .map_err(|_| Refusal::Configuration)?;
        Ok(Self {
            reset: hmac::Key::new(hmac::HMAC_SHA256, &reset),
            ids: u64::from_le_bytes(ids),
        })
    }

    /// hyper-quic's endpoint configuration keyed by it: its reset key, and connection IDs hashed
    /// under its own key, so the same set is known for the endpoint's own from one run to the next
    /// (hyper-quic's `HashedConnectionIdGenerator::from_key`).
    pub(crate) fn configure(&self, endpoint: &mut EndpointConfig) {
        endpoint
            .reset_key(self.reset.clone())
            .cid_generator(Box::new(HashedConnectionIdGenerator::from_key(self.ids)));
    }
}

impl std::fmt::Debug for RestartKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RestartKey(..)")
    }
}

/// An output length for HKDF's expansion.
struct Len(usize);

impl hkdf::KeyType for Len {
    fn len(&self) -> usize {
        self.0
    }
}
