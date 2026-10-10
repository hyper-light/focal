//! Admission of inbound connections by identity (27 §3.1 P5), and of what they send (the audit's
//! F03 and F20), as a listener is configured and reports it. The rules live where the connections
//! and the bytes do since the move to hyper-transport (27 §15):
//!
//! - **Connections** are admitted by the endpoint (`hyper_transport::AdmissionLimits`, its
//!   `admission` module, ported from this one): a handshake takes one of a bounded number of
//!   pending places, which authenticated connections never use; an authenticated connection is
//!   charged to its identity (its certificate); an identity past its bound replaces its own
//!   connection used longest ago among those that are idle — none of its exchanges under way, and
//!   nothing moved on it for the listener's request timeout — and with none idle is refused,
//!   retryably (64 callers of one principal against a bound of 16 replaced 27,765 connections in
//!   30 s while replacing busy ones, and a third of their writes ended unknown); the connections
//!   held in all are bounded, met after the replacement rule, and a full listener gives an
//!   identity under its fair share an idle connection of the identity holding the most (max-min
//!   fairness: Bertsekas and Gallager, *Data Networks*, §6.5.2). One identity never takes
//!   another's place but by that rule.
//! - **Bytes** are admitted by the network owner (`net::owner`): before any of a request's body
//!   is reserved, its sender is held to its share of the lane the body is funded from — control
//!   bodies from the completion lane, so participants' bodies never starve consensus — the lane's
//!   capacity among the peers with a connection up, one frame at least; the body is reserved from
//!   the listener's budget and given back as the exchange ends.
//!
//! Every refusal is typed and counted ([`AdmissionStats`]).
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Handshakes in progress.
    pub pending: usize,
    /// Identities holding a connection.
    pub identities: usize,
    /// Connections held in all, once authenticated.
    pub connections: usize,
    /// The most connections a node holds: its peer pool's few. Kept for the configurations that
    /// state it; the endpoint's fair share bounds every identity alike.
    pub per_node: usize,
    /// The least share of the listener a participant is ever left. Kept for the configurations
    /// that state it; the endpoint's fair share is the listener among the identities holding
    /// connections.
    pub per_participant: usize,
    /// How long a connection must have begun no request before it may be replaced: a live client
    /// between requests is not, one that left its connection behind is. The endpoint counts it as
    /// the listener's request timeout, its serve period.
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
}
/// Who holds a listener's connections, what its bodies hold, and what it refused.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdmissionStats {
    pub pending: usize,
    pub identities: usize,
    pub connections: usize,
    /// Bytes the listener holds of its budget: request bodies being read and its connections'
    /// receive windows.
    pub bytes: usize,
    /// Exchanges under way on the connections held: what keeps a connection from being replaced.
    pub serving: usize,
    pub admitted: u64,
    pub replaced: u64,
    pub refused_pending: u64,
    pub refused_identities: u64,
    pub refused_connections: u64,
    /// Connections refused for an identity at its bound with none idle.
    pub refused_busy: u64,
    /// Requests refused for their sender's share of the ingress.
    pub refused_bytes: u64,
    /// Reservations the listener's budget could not fund.
    pub refused_memory: u64,
    /// Every admission and refusal so far: what a wait on this listener is charged in.
    pub changes: u64,
}
impl AdmissionStats {
    /// A listener's admission, from what its owner reports.
    pub fn of(stats: &crate::net::NetStats) -> Self {
        let admission = stats.endpoint.admission;
        let bytes = stats
            .budget
            .ordinary
            .saturating_add(stats.budget.completion);
        let refusals = [
            admission.refused_pending,
            admission.refused_identities,
            admission.refused_connections,
            admission.refused_busy,
            admission.refused_identity,
            stats.refused_ingress,
            stats.budget.refused,
        ];
        Self {
            pending: admission.pending,
            identities: admission.identities,
            connections: admission.connections,
            bytes,
            serving: admission.serving,
            admitted: admission.admitted,
            replaced: admission.replaced,
            refused_pending: admission.refused_pending,
            refused_identities: admission.refused_identities,
            refused_connections: admission.refused_connections,
            refused_busy: admission.refused_busy,
            refused_bytes: stats.refused_ingress,
            refused_memory: stats.budget.refused,
            changes: refusals
                .into_iter()
                .fold(admission.admitted, u64::saturating_add),
        }
    }
}
