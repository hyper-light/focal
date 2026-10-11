//! Which peer a connection names (hyper-transport's `Directory`), from all its handshake established.
//!
//! - A connection this side dialed names the peer it was dialed as: TLS has verified the server's
//!   chain and its name ([`NetDirectory::dial`]), and a dial that pinned the server's leaf (an
//!   enrollment, 24 §11) is refused unless the leaf is one its pins accept ([`LeafPins`]).
//! - A connection a peer opened on the data protocol ([`crate::ALPN`]) names the certificate it
//!   presented, through the registry's grants ([`PeerRegistry::authenticate_certificate`]); one
//!   without a certificate, or whose certificate no grant names, names no peer and is refused,
//!   charged to no identity. Each certificate is its own peer: a renewed certificate is a peer
//!   apart from the one it renews, so the revocation of one closes its connections and not the
//!   other's.
//! - A connection a peer opened on the enrollment protocol is a joiner, each its own peer, whose
//!   role lets it enroll and nothing else ([`super::classes::Sender::Joiner`]).
//!
//! Every table is bounded: a peer's entry goes when the owner forgets it, and a table at its bound
//! makes room from a peer with no connection up, or names no new peer.

use std::collections::{BTreeMap, BTreeSet};

use hyper_transport::{Authenticated, Directory, PeerId};

use super::classes::Sender;
use crate::{AuthenticatedPeer, PeerRegistry, certificate_fingerprint};

/// The leaves a dial accepts, beside the chain and name its TLS verified: a leaf is accepted when
/// its BLAKE3 key derivation under `context` — the caller's own fingerprint domain — is one of
/// `accepted`, a pinned certificate's and the successor's staged to replace it (24 §11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeafPins {
    pub context: &'static str,
    pub accepted: [Option<[u8; 32]>; 2],
}
impl LeafPins {
    /// Whether `leaf` (DER) is one these pins accept.
    pub fn accepts(&self, leaf: &[u8]) -> bool {
        let fingerprint = blake3::derive_key(self.context, leaf);
        self.accepted
            .iter()
            .flatten()
            .any(|pin| *pin == fingerprint)
    }
}

/// A peer this side dials.
#[derive(Debug)]
struct Dialed {
    /// The name its certificate is checked against.
    name: String,
    /// The leaves it may present, when pinned.
    pin: Option<LeafPins>,
    /// Its last connection was refused for a leaf other than the pinned one.
    unpinned: bool,
}

/// A peer that opened a connection to this side.
#[derive(Clone, Debug)]
pub enum Inbound {
    /// On the data protocol: its certificate's fingerprint and the grant it named when it
    /// authenticated. A request is checked against the grant current when it is dispatched
    /// ([`PeerRegistry::authenticate`]), never against this one.
    Data {
        fingerprint: [u8; 32],
        peer: AuthenticatedPeer,
    },
    /// On the enrollment protocol.
    Joiner,
}

/// Which peer each connection of one endpoint names.
pub(crate) struct NetDirectory {
    /// The grants inbound certificates are authenticated by; none on an endpoint that only dials.
    registry: Option<PeerRegistry>,
    /// The data protocol.
    data: &'static [u8],
    /// The enrollment protocol, on an endpoint that serves it.
    enrollment: Option<&'static [u8]>,
    dialed: BTreeMap<PeerId, Dialed>,
    inbound: BTreeMap<PeerId, Inbound>,
    /// The peer each inbound certificate is.
    by_fingerprint: BTreeMap<[u8; 32], PeerId>,
    /// The peers with a connection up, as the owner last said.
    live: BTreeSet<PeerId>,
    /// The id the next new peer takes.
    next: PeerId,
    /// The most peers each table holds.
    bound: usize,
}

impl NetDirectory {
    /// The directory of an endpoint that serves the data protocol, and enrollment when it is
    /// given, naming at most `bound` peers of each table.
    pub(crate) fn serving(
        registry: PeerRegistry,
        enrollment: Option<&'static [u8]>,
        bound: usize,
    ) -> Self {
        Self {
            registry: Some(registry),
            enrollment,
            ..Self::dialing(bound)
        }
    }

    /// The directory of an endpoint that only dials.
    pub(crate) fn dialing(bound: usize) -> Self {
        Self {
            registry: None,
            data: crate::ALPN,
            enrollment: None,
            dialed: BTreeMap::new(),
            inbound: BTreeMap::new(),
            by_fingerprint: BTreeMap::new(),
            live: BTreeSet::new(),
            next: 1,
            bound: bound.max(1),
        }
    }

    fn take_id(&mut self) -> Option<PeerId> {
        let id = self.next;
        self.next = self.next.checked_add(1)?;
        Some(id)
    }

    /// A peer to dial, checked against `name`, and pinned to the leaves `pin` accepts when it is
    /// given: its id, or `None` when the table holds `bound` peers with a connection up each.
    pub(crate) fn dial(&mut self, name: &str, pin: Option<LeafPins>) -> Option<PeerId> {
        if self.dialed.len() >= self.bound {
            let idle = self
                .dialed
                .keys()
                .copied()
                .find(|peer| !self.live.contains(peer))?;
            self.dialed.remove(&idle);
        }
        let id = self.take_id()?;
        self.dialed.insert(
            id,
            Dialed {
                name: name.to_owned(),
                pin,
                unpinned: false,
            },
        );
        Some(id)
    }

    /// Whether the last connection to `peer` was refused for a leaf other than the pinned one,
    /// cleared as it is read.
    pub(crate) fn take_unpinned(&mut self, peer: PeerId) -> bool {
        self.dialed
            .get_mut(&peer)
            .is_some_and(|dialed| std::mem::take(&mut dialed.unpinned))
    }

    /// What `peer` authenticated as, when it opened its connection to this side.
    pub(crate) fn inbound(&self, peer: PeerId) -> Option<&Inbound> {
        self.inbound.get(&peer)
    }

    /// The owner heard `peer`'s connection come up (`live`) or close.
    pub(crate) fn set_live(&mut self, peer: PeerId, live: bool) {
        if live {
            self.live.insert(peer);
        } else {
            self.live.remove(&peer);
        }
    }

    /// The registry's revision ([`PeerRegistry::revision`]), on an endpoint that serves under one.
    pub(crate) fn revision(&self) -> Option<u64> {
        self.registry.as_ref().map(PeerRegistry::revision)
    }

    /// Every inbound peer on the data protocol whose certificate the registry no longer grants.
    pub(crate) fn revoked(&self) -> Vec<PeerId> {
        let Some(registry) = self.registry.as_ref() else {
            return Vec::new();
        };
        self.inbound
            .iter()
            .filter_map(|(peer, inbound)| match inbound {
                Inbound::Data { fingerprint, .. } => {
                    registry.granted(*fingerprint).is_err().then_some(*peer)
                }
                Inbound::Joiner => None,
            })
            .collect()
    }

    /// `peer` is gone: its entry goes, and its id names nothing from now on.
    pub(crate) fn forget(&mut self, peer: PeerId) {
        self.dialed.remove(&peer);
        self.live.remove(&peer);
        if let Some(Inbound::Data { fingerprint, .. }) = self.inbound.remove(&peer) {
            self.by_fingerprint.remove(&fingerprint);
        }
    }

    /// An entry for an inbound peer: room is made from one with no connection up.
    fn admit_inbound(&mut self, inbound: Inbound) -> Option<PeerId> {
        if self.inbound.len() >= self.bound {
            let idle = self
                .inbound
                .keys()
                .copied()
                .find(|peer| !self.live.contains(peer))?;
            self.forget(idle);
        }
        let id = self.take_id()?;
        if let Inbound::Data { fingerprint, .. } = &inbound {
            self.by_fingerprint.insert(*fingerprint, id);
        }
        self.inbound.insert(id, inbound);
        Some(id)
    }
}

impl Directory for NetDirectory {
    type Role = Sender;

    fn identify(&mut self, authenticated: &Authenticated<'_>) -> Option<(PeerId, Sender)> {
        if let Some(dialed) = authenticated.dialed {
            // TLS verified the server's chain and name; a pinned dial needs a leaf it pins.
            let entry = self.dialed.get_mut(&dialed)?;
            if let Some(pins) = entry.pin {
                let leaf = authenticated.certificate?;
                if !pins.accepts(leaf) {
                    entry.unpinned = true;
                    return None;
                }
            }
            return Some((dialed, Sender::Node));
        }
        if self.enrollment == Some(authenticated.protocol) {
            let id = self.admit_inbound(Inbound::Joiner)?;
            return Some((id, Sender::Joiner));
        }
        if authenticated.protocol != self.data {
            return None;
        }
        let certificate = authenticated.certificate?;
        let peer = self
            .registry
            .as_ref()?
            .authenticate_certificate(certificate)
            .ok()?;
        let sender = Sender::of(peer.role());
        let fingerprint = certificate_fingerprint(certificate);
        let id = match self.by_fingerprint.get(&fingerprint).copied() {
            Some(id) => {
                self.inbound.insert(id, Inbound::Data { fingerprint, peer });
                id
            }
            None => self.admit_inbound(Inbound::Data { fingerprint, peer })?,
        };
        Some((id, sender))
    }

    fn server_name(&self, peer: PeerId) -> Option<&str> {
        self.dialed.get(&peer).map(|dialed| dialed.name.as_str())
    }
}
