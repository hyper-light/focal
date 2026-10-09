//! Admission of inbound connections by identity (27 §3.1 P5), and of what
//! they send (the audit's F03 and F20).
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
//!   one most likely to be that. Only a connection that is idle may be
//!   replaced: none of its requests under way, and none begun for
//!   [`AdmissionLimits::replace_after`]. A connection carrying a request is
//!   alive by that request, and closing it made the request's outcome
//!   unknown to its caller; a client that came back holds nothing on what it
//!   left behind. With no idle connection to replace the newcomer is refused,
//!   retryably: an identity using every connection it may hold is told so,
//!   where replacing made its own callers close each other's connections
//!   (64 callers of one principal against a bound of 16 replaced 27,765
//!   connections in 30 s, and a third of their writes ended unknown);
//! - the connections held in all are bounded by [`AdmissionLimits::connections`],
//!   and the bound is met after the replacement rule, never before it: an
//!   identity at its own bound reaches its replacement however full the
//!   listener is, and only a connection that would be one more is refused.
//!   The listener's outer count of everything in flight (handshakes,
//!   enrollment, established) used to refuse first, so full occupancy took
//!   the opportunity to authenticate a replacement with it (F20).
//!
//! What a connection sends is admitted too (F03). A request's header names
//! its body's length; before any of the body is allocated, the identity is
//! given a permit for it ([`IngressLane::take`]) funded from the listener's
//! own budget — nodes on the completion lane, so that control traffic is
//! never starved by participants' bodies, participants on the ordinary one —
//! and bounded to the identity's share of that lane: its capacity divided
//! among the identities holding connections, and one frame at least, so
//! that an identity alone may always ask one frame and no identity takes
//! the pool from the others. The permit is held while the body arrives and
//! is dispatched, and given back with it, cancelled or not.
//!
//! One identity never takes another's place: the number of identities is
//! bounded, and an identity past its own bound displaces only itself. Every
//! refusal is typed and counted.
//!
//! The admission is one shared handle: the tasks that serve a connection's
//! streams outlive any borrow of the listener's loop, and each holds its
//! identity's permits, so the state they charge is theirs together
//! (doc 10), as the peer registry's grants are.
use crate::PeerRole;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::ParticipantId;
use quinn::Connection;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Handshakes in progress.
    pub pending: usize,
    /// Identities holding a connection.
    pub identities: usize,
    /// Connections held in all, once authenticated.
    pub connections: usize,
    pub per_node: usize,
    pub per_participant: usize,
    /// How long a connection must have begun no request before it may be
    /// replaced: a live client between requests is not, one that left its
    /// connection behind is. The listener's request timeout.
    pub replace_after: Duration,
}
impl AdmissionLimits {
    /// For a listener of `connections` in total: a quarter of them may be
    /// handshakes in progress.
    pub fn for_connections(connections: usize) -> Self {
        Self {
            pending: connections.checked_div(4).unwrap_or(0).max(1),
            identities: connections.max(1),
            connections: connections.max(1),
            per_node: 4,
            per_participant: 16,
            replace_after: Duration::ZERO,
        }
    }
    /// These limits, a connection replaceable once it has begun no request
    /// for `idle`.
    pub fn replacing_after(self, idle: Duration) -> Self {
        Self {
            replace_after: idle,
            ..self
        }
    }
    fn valid(&self) -> bool {
        self.pending > 0
            && self.identities > 0
            && self.connections > 0
            && self.per_node > 0
            && self.per_participant > 0
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
    #[error("too many connections are held")]
    Connections,
    #[error("the identity holds every connection it may, and is using each")]
    Busy,
    #[error("the identity's share of the ingress is taken")]
    Bytes,
    #[error("the listener's budget cannot fund the body")]
    Memory,
    #[error("the listener is closed")]
    Closed,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdmissionStats {
    pub pending: usize,
    pub identities: usize,
    pub connections: usize,
    /// Bytes of request bodies permitted and not yet given back.
    pub bytes: usize,
    /// Requests under way on the connections held, their answers until
    /// carried: what keeps a connection from being replaced.
    pub serving: usize,
    pub admitted: u64,
    pub replaced: u64,
    pub refused_pending: u64,
    pub refused_identities: u64,
    pub refused_connections: u64,
    /// Connections refused for an identity at its bound with none idle.
    pub refused_busy: u64,
    pub refused_bytes: u64,
    pub refused_memory: u64,
    /// Every admission, refusal and release so far: what a wait on this
    /// listener is charged in.
    pub changes: u64,
}
struct Held {
    /// Least recently used first.
    connections: VecDeque<HeldConnection>,
    /// Bytes of bodies permitted to this identity and not yet given back.
    bytes: usize,
}
/// One connection an identity holds: its requests under way and when it
/// last began one.
struct HeldConnection {
    id: u64,
    connection: Connection,
    serving: usize,
    used: Instant,
}
#[derive(Default)]
struct State {
    pending: usize,
    identities: BTreeMap<ParticipantId, Held>,
    connections: usize,
    bytes: usize,
    next: u64,
    admitted: u64,
    replaced: u64,
    refused_pending: u64,
    refused_identities: u64,
    refused_connections: u64,
    refused_busy: u64,
    refused_bytes: u64,
    refused_memory: u64,
    changes: u64,
}
struct Inner {
    limits: AdmissionLimits,
    budget: MemoryBudget,
    state: Mutex<State>,
}
/// The admission of one listener; clones share it (see the module).
#[derive(Clone)]
pub struct Admission(Arc<Inner>);
fn bump(counter: &mut u64) {
    *counter = counter.saturating_add(1);
}
impl Admission {
    /// `budget` funds the bodies the listener admits.
    pub fn new(limits: AdmissionLimits, budget: MemoryBudget) -> Result<Self, AdmissionRefusal> {
        if !limits.valid() {
            return Err(AdmissionRefusal::InvalidLimits);
        }
        Ok(Self(Arc::new(Inner {
            limits,
            budget,
            state: Mutex::new(State::default()),
        })))
    }
    pub fn limits(&self) -> AdmissionLimits {
        self.0.limits
    }
    pub fn stats(&self) -> AdmissionStats {
        let Ok(state) = self.0.state.lock() else {
            return AdmissionStats::default();
        };
        AdmissionStats {
            serving: state
                .identities
                .values()
                .flat_map(|held| held.connections.iter())
                .map(|held| held.serving)
                .fold(0usize, usize::saturating_add),
            pending: state.pending,
            identities: state.identities.len(),
            connections: state.connections,
            bytes: state.bytes,
            admitted: state.admitted,
            replaced: state.replaced,
            refused_pending: state.refused_pending,
            refused_identities: state.refused_identities,
            refused_connections: state.refused_connections,
            refused_busy: state.refused_busy,
            refused_bytes: state.refused_bytes,
            refused_memory: state.refused_memory,
            changes: state.changes,
        }
    }
    /// A place for one handshake, held until it authenticates or ends.
    pub fn begin(&self) -> Result<Pending, AdmissionRefusal> {
        let mut state = self.0.state.lock().map_err(|_| AdmissionRefusal::Closed)?;
        bump(&mut state.changes);
        if state.pending >= self.0.limits.pending {
            bump(&mut state.refused_pending);
            return Err(AdmissionRefusal::Pending);
        }
        state.pending = state.pending.saturating_add(1);
        Ok(Pending {
            admission: self.clone(),
        })
    }
    fn admit(
        &self,
        identity: ParticipantId,
        role: PeerRole,
        connection: &Connection,
    ) -> Result<(u64, Option<Connection>), AdmissionRefusal> {
        let mut state = self.0.state.lock().map_err(|_| AdmissionRefusal::Closed)?;
        bump(&mut state.changes);
        let bound = if matches!(role, PeerRole::Node { .. }) {
            self.0.limits.per_node
        } else {
            self.0.limits.per_participant
        };
        let held = state
            .identities
            .get(&identity)
            .map_or(0, |held| held.connections.len());
        if held == 0 && state.identities.len() >= self.0.limits.identities {
            bump(&mut state.refused_identities);
            return Err(AdmissionRefusal::Identities);
        }
        // The replacement rule first: an identity at its bound takes its
        // own place back however full the listener is, from the connection
        // it has used least of those that are idle.
        let replacing = held >= bound;
        let now = Instant::now();
        let idle = if replacing {
            let after = self.0.limits.replace_after;
            let found = state.identities.get(&identity).and_then(|held| {
                held.connections.iter().position(|held| {
                    held.serving == 0 && now.saturating_duration_since(held.used) >= after
                })
            });
            match found {
                Some(position) => Some(position),
                None => {
                    bump(&mut state.refused_busy);
                    return Err(AdmissionRefusal::Busy);
                }
            }
        } else {
            None
        };
        if !replacing && state.connections >= self.0.limits.connections {
            bump(&mut state.refused_connections);
            return Err(AdmissionRefusal::Connections);
        }
        let id = state.next;
        state.next = state.next.checked_add(1).ok_or(AdmissionRefusal::Closed)?;
        let entry = state.identities.entry(identity).or_insert_with(|| Held {
            connections: VecDeque::new(),
            bytes: 0,
        });
        let replaced = idle
            .and_then(|position| entry.connections.remove(position))
            .map(|old| old.connection);
        entry.connections.push_back(HeldConnection {
            id,
            connection: connection.clone(),
            serving: 0,
            used: now,
        });
        if replaced.is_some() {
            bump(&mut state.replaced);
        } else {
            state.connections = state.connections.saturating_add(1);
        }
        bump(&mut state.admitted);
        Ok((id, replaced))
    }
    /// The connection began a request (`begin`) or ended one: it is this
    /// identity's most recently used, and the requests it has under way
    /// keep it from being replaced.
    fn serving(&self, identity: ParticipantId, id: u64, begin: bool) {
        let Ok(mut state) = self.0.state.lock() else {
            return;
        };
        bump(&mut state.changes);
        let Some(held) = state.identities.get_mut(&identity) else {
            return;
        };
        let Some(position) = held.connections.iter().position(|held| held.id == id) else {
            return;
        };
        let Some(mut entry) = held.connections.remove(position) else {
            return;
        };
        entry.serving = if begin {
            entry.serving.saturating_add(1)
        } else {
            entry.serving.saturating_sub(1)
        };
        entry.used = Instant::now();
        held.connections.push_back(entry);
    }
    fn release(&self, identity: ParticipantId, id: u64) {
        let Ok(mut state) = self.0.state.lock() else {
            return;
        };
        bump(&mut state.changes);
        let Some(held) = state.identities.get_mut(&identity) else {
            return;
        };
        let before = held.connections.len();
        held.connections.retain(|held| held.id != id);
        let removed = before.saturating_sub(held.connections.len());
        if held.connections.is_empty() && held.bytes == 0 {
            state.identities.remove(&identity);
        }
        state.connections = state.connections.saturating_sub(removed);
    }
    /// A permit for a body of `bytes` from `identity`, on `lane`: within the
    /// identity's share of the lane — its capacity among the identities
    /// holding connections, `frame` at least — and funded from the budget.
    fn take(
        &self,
        identity: ParticipantId,
        lane: BudgetLane,
        bytes: usize,
        frame: usize,
    ) -> Result<Ingress, AdmissionRefusal> {
        let mut state = self.0.state.lock().map_err(|_| AdmissionRefusal::Closed)?;
        bump(&mut state.changes);
        let stats = self.0.budget.stats();
        let capacity = match lane {
            BudgetLane::Ordinary => stats.limit.saturating_sub(stats.completion_reserve),
            BudgetLane::Completion => stats.limit,
        };
        let share = capacity
            .checked_div(state.identities.len().max(1))
            .unwrap_or(0)
            .max(frame);
        let Some(held) = state.identities.get_mut(&identity) else {
            return Err(AdmissionRefusal::Closed);
        };
        if held.bytes.saturating_add(bytes) > share {
            bump(&mut state.refused_bytes);
            return Err(AdmissionRefusal::Bytes);
        }
        let Ok(reservation) = self.0.budget.reserve(BudgetKind::Pending, lane, bytes) else {
            bump(&mut state.refused_memory);
            return Err(AdmissionRefusal::Memory);
        };
        held.bytes = held.bytes.saturating_add(bytes);
        state.bytes = state.bytes.saturating_add(bytes);
        Ok(Ingress {
            admission: self.clone(),
            identity,
            bytes,
            _allocation: reservation.commit(),
        })
    }
    fn give_back(&self, identity: ParticipantId, bytes: usize) {
        let Ok(mut state) = self.0.state.lock() else {
            return;
        };
        bump(&mut state.changes);
        state.bytes = state.bytes.saturating_sub(bytes);
        let Some(held) = state.identities.get_mut(&identity) else {
            return;
        };
        held.bytes = held.bytes.saturating_sub(bytes);
        if held.connections.is_empty() && held.bytes == 0 {
            state.identities.remove(&identity);
        }
    }
}
/// One handshake in progress.
pub struct Pending {
    admission: Admission,
}
impl Pending {
    /// The handshake authenticated as `identity`: charge the connection to
    /// it and give the pending place back. A connection this one replaces is
    /// closed before this returns.
    pub fn authenticated(
        self,
        identity: ParticipantId,
        role: PeerRole,
        connection: &Connection,
    ) -> Result<Admitted, AdmissionRefusal> {
        let admission = self.admission.clone();
        let (id, replaced) = admission.admit(identity, role, connection)?;
        if let Some(replaced) = replaced {
            replaced.close(3u8.into(), b"replaced");
        }
        Ok(Admitted {
            admission,
            identity,
            role,
            id,
        })
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        if let Ok(mut state) = self.admission.0.state.lock() {
            state.pending = state.pending.saturating_sub(1);
            bump(&mut state.changes);
        }
    }
}
/// One authenticated connection, charged to its identity until dropped.
pub struct Admitted {
    admission: Admission,
    identity: ParticipantId,
    role: PeerRole,
    id: u64,
}
impl Admitted {
    /// A request this connection began, under way until the returned guard
    /// is dropped: the connection is not replaced meanwhile.
    pub fn serving(&self) -> Serving {
        self.admission.serving(self.identity, self.id, true);
        Serving {
            admission: self.admission.clone(),
            identity: self.identity,
            id: self.id,
        }
    }
    /// Where this connection's bodies are admitted: the completion lane for
    /// a node, the ordinary one for any other identity.
    pub fn lane(&self) -> IngressLane {
        IngressLane {
            admission: self.admission.clone(),
            identity: self.identity,
            lane: if matches!(self.role, PeerRole::Node { .. }) {
                BudgetLane::Completion
            } else {
                BudgetLane::Ordinary
            },
        }
    }
}
/// A request a connection has under way ([`Admitted::serving`]).
pub struct Serving {
    admission: Admission,
    identity: ParticipantId,
    id: u64,
}
impl Drop for Serving {
    fn drop(&mut self) {
        self.admission.serving(self.identity, self.id, false);
    }
}
impl Drop for Admitted {
    fn drop(&mut self) {
        self.admission.release(self.identity, self.id);
    }
}
/// The lane an identity's request bodies are admitted on; a stream's task
/// holds one and asks it for each body before allocating it.
#[derive(Clone)]
pub struct IngressLane {
    admission: Admission,
    identity: ParticipantId,
    lane: BudgetLane,
}
impl IngressLane {
    /// A permit for a body of `bytes`, `frame` being the most a frame may
    /// carry: the least an identity alone is always allowed.
    pub fn take(&self, bytes: usize, frame: usize) -> Result<Ingress, AdmissionRefusal> {
        self.admission.take(self.identity, self.lane, bytes, frame)
    }
}
/// A body's permit: its bytes, charged to the identity and funded from the
/// listener's budget until dropped.
pub struct Ingress {
    admission: Admission,
    identity: ParticipantId,
    bytes: usize,
    _allocation: Allocation,
}
impl Ingress {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}
impl Drop for Ingress {
    fn drop(&mut self) {
        self.admission.give_back(self.identity, self.bytes);
    }
}
