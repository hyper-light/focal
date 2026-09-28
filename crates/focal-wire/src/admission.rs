//! Admission of inbound connections by identity (27 §3.1 P5).
//!
//! A listener's connections were one pool: any peer could fill it with
//! handshakes that never authenticate, and one identity could hold every
//! slot. Here a connection is admitted twice. Before its handshake it takes
//! one of a bounded number of **pending** places, which authenticated
//! connections never use, so unauthenticated work cannot take the capacity
//! of peers that have proven who they are. Once its certificate has
//! authenticated it is charged to its **identity** (the principal its grant
//! names, which a renewed certificate keeps):
//!
//! - a node holds at most [`AdmissionLimits::per_node`] connections. A node
//!   opens one connection per peer and another only when it thinks the first
//!   is gone, so a connection past the bound replaces the oldest, which is
//!   closed: a peer that restarted or moved is served at once instead of
//!   waiting out the idle timeout of what it left behind;
//! - any other identity holds at most [`AdmissionLimits::per_participant`].
//!   A participant may run several clients at once, so nothing of theirs is
//!   closed for a newcomer: the newcomer is refused and may try again.
//!
//! The number of identities is bounded too. Every refusal is typed and
//! counted.
use crate::PeerRole;
use focal_model::ParticipantId;
use quinn::Connection;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Handshakes in progress.
    pub pending: usize,
    /// Identities holding a connection.
    pub identities: usize,
    pub per_node: usize,
    pub per_participant: usize,
}
impl AdmissionLimits {
    /// For a listener of `connections` in total: a quarter of them may be
    /// handshakes in progress.
    pub fn for_connections(connections: usize) -> Self {
        Self {
            pending: connections.checked_div(4).unwrap_or(0).max(1),
            identities: connections.max(1),
            per_node: 4,
            per_participant: 16,
        }
    }
    fn valid(&self) -> bool {
        self.pending > 0 && self.identities > 0 && self.per_node > 0 && self.per_participant > 0
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionRefusal {
    #[error("admission limits must not be zero")]
    InvalidLimits,
    #[error("too many handshakes are in progress")]
    Pending,
    #[error("too many identities hold connections")]
    Identities,
    #[error("this identity holds its bound of connections")]
    IdentityConnections,
    #[error("the listener is closed")]
    Closed,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdmissionStats {
    pub pending: usize,
    pub identities: usize,
    pub connections: usize,
    pub admitted: u64,
    pub replaced: u64,
    pub refused_pending: u64,
    pub refused_identities: u64,
    pub refused_identity_connections: u64,
    /// Every admission, refusal and release so far: what a wait on this
    /// listener is charged in.
    pub changes: u64,
}
struct Held {
    /// Oldest first.
    connections: VecDeque<(u64, Connection)>,
}
#[derive(Default)]
struct State {
    pending: usize,
    identities: BTreeMap<ParticipantId, Held>,
    connections: usize,
    next: u64,
    admitted: u64,
    replaced: u64,
    refused_pending: u64,
    refused_identities: u64,
    refused_identity_connections: u64,
    changes: u64,
}
pub struct Admission {
    limits: AdmissionLimits,
    state: Mutex<State>,
}
fn bump(counter: &mut u64) {
    *counter = counter.saturating_add(1);
}
impl Admission {
    pub fn new(limits: AdmissionLimits) -> Result<Self, AdmissionRefusal> {
        if !limits.valid() {
            return Err(AdmissionRefusal::InvalidLimits);
        }
        Ok(Self {
            limits,
            state: Mutex::new(State::default()),
        })
    }
    pub fn limits(&self) -> AdmissionLimits {
        self.limits
    }
    pub fn stats(&self) -> AdmissionStats {
        let Ok(state) = self.state.lock() else {
            return AdmissionStats::default();
        };
        AdmissionStats {
            pending: state.pending,
            identities: state.identities.len(),
            connections: state.connections,
            admitted: state.admitted,
            replaced: state.replaced,
            refused_pending: state.refused_pending,
            refused_identities: state.refused_identities,
            refused_identity_connections: state.refused_identity_connections,
            changes: state.changes,
        }
    }
    /// A place for one handshake, held until it authenticates or ends.
    pub fn begin(&self) -> Result<Pending<'_>, AdmissionRefusal> {
        let mut state = self.state.lock().map_err(|_| AdmissionRefusal::Closed)?;
        bump(&mut state.changes);
        if state.pending >= self.limits.pending {
            bump(&mut state.refused_pending);
            return Err(AdmissionRefusal::Pending);
        }
        state.pending = state.pending.saturating_add(1);
        Ok(Pending { admission: self })
    }
    fn admit(
        &self,
        identity: ParticipantId,
        role: PeerRole,
        connection: &Connection,
    ) -> Result<(u64, Option<Connection>), AdmissionRefusal> {
        let mut state = self.state.lock().map_err(|_| AdmissionRefusal::Closed)?;
        bump(&mut state.changes);
        let node = matches!(role, PeerRole::Node { .. });
        let bound = if node {
            self.limits.per_node
        } else {
            self.limits.per_participant
        };
        let held = state
            .identities
            .get(&identity)
            .map_or(0, |held| held.connections.len());
        if held == 0 && state.identities.len() >= self.limits.identities {
            bump(&mut state.refused_identities);
            return Err(AdmissionRefusal::Identities);
        }
        if held >= bound && !node {
            bump(&mut state.refused_identity_connections);
            return Err(AdmissionRefusal::IdentityConnections);
        }
        let id = state.next;
        state.next = state.next.checked_add(1).ok_or(AdmissionRefusal::Closed)?;
        let entry = state.identities.entry(identity).or_insert_with(|| Held {
            connections: VecDeque::new(),
        });
        let replaced = if entry.connections.len() >= bound {
            entry.connections.pop_front().map(|(_, old)| old)
        } else {
            None
        };
        entry.connections.push_back((id, connection.clone()));
        if replaced.is_some() {
            bump(&mut state.replaced);
        } else {
            state.connections = state.connections.saturating_add(1);
        }
        bump(&mut state.admitted);
        Ok((id, replaced))
    }
    fn release(&self, identity: ParticipantId, id: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        bump(&mut state.changes);
        let Some(held) = state.identities.get_mut(&identity) else {
            return;
        };
        let before = held.connections.len();
        held.connections.retain(|(held, _)| *held != id);
        let removed = before.saturating_sub(held.connections.len());
        if held.connections.is_empty() {
            state.identities.remove(&identity);
        }
        state.connections = state.connections.saturating_sub(removed);
    }
}
/// One handshake in progress.
pub struct Pending<'a> {
    admission: &'a Admission,
}
impl<'a> Pending<'a> {
    /// The handshake authenticated as `identity`: charge the connection to
    /// it and give the pending place back. A connection this one replaces is
    /// closed before this returns.
    pub fn authenticated(
        self,
        identity: ParticipantId,
        role: PeerRole,
        connection: &Connection,
    ) -> Result<Admitted<'a>, AdmissionRefusal> {
        let admission = self.admission;
        let (id, replaced) = admission.admit(identity, role, connection)?;
        if let Some(replaced) = replaced {
            replaced.close(3u8.into(), b"replaced");
        }
        Ok(Admitted {
            admission,
            identity,
            id,
        })
    }
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.admission.state.lock() {
            state.pending = state.pending.saturating_sub(1);
            bump(&mut state.changes);
        }
    }
}
/// One authenticated connection, charged to its identity until dropped.
pub struct Admitted<'a> {
    admission: &'a Admission,
    identity: ParticipantId,
    id: u64,
}
impl Drop for Admitted<'_> {
    fn drop(&mut self) {
        self.admission.release(self.identity, self.id);
    }
}
