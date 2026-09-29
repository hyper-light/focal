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
//! - a node holds at most [`AdmissionLimits::per_node`] connections, any
//!   other identity at most [`AdmissionLimits::per_participant`];
//! - a connection past the bound replaces the one of that identity that has
//!   been idle longest, which is closed. A client that exited, a node that
//!   restarted or moved, leaves a connection behind that the listener holds
//!   until its idle timeout: refusing the newcomer would make an identity
//!   wait out what it left behind, and the connection it uses least is the
//!   one most likely to be that. A live client whose idle connection is
//!   closed dials again at its next request.
//!
//! One identity never takes another's place: the number of identities is
//! bounded, and an identity past its own bound displaces only itself. Every
//! refusal is typed and counted.
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
    /// Every admission, refusal and release so far: what a wait on this
    /// listener is charged in.
    pub changes: u64,
}
struct Held {
    /// Least recently used first.
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
        let bound = if matches!(role, PeerRole::Node { .. }) {
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
    /// The connection served a request: it is this identity's most
    /// recently used.
    fn used(&self, identity: ParticipantId, id: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(held) = state.identities.get_mut(&identity) else {
            return;
        };
        if held.connections.back().is_some_and(|(last, _)| *last == id) {
            return;
        }
        if let Some(position) = held.connections.iter().position(|(held, _)| *held == id)
            && let Some(entry) = held.connections.remove(position)
        {
            held.connections.push_back(entry);
        }
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
impl Admitted<'_> {
    /// The connection served a request.
    pub fn used(&self) {
        self.admission.used(self.identity, self.id);
    }
}
impl Drop for Admitted<'_> {
    fn drop(&mut self) {
        self.admission.release(self.identity, self.id);
    }
}
