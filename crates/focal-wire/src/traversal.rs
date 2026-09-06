//! Bounded immutable graph traversal. Continuations are opaque owner capabilities.
use crate::*;
use focal_model::*;
use serde::{Deserialize, Serialize};

pub const MAX_TRAVERSAL_ROOTS: usize = 32;
pub const MAX_TRAVERSAL_CURSOR_BYTES: usize = 256;
pub const MAX_TRAVERSAL_NODES: u32 = 4096;
pub const MAX_TRAVERSAL_EDGES: u32 = 16_384;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraversalDirection {
    Forward,
    Reverse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TraversalEdge {
    Authored(RelationKind),
    Requirement,
    TestamentOf,
    Evidence,
    ArtifactInput,
    ValidationOf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraversalCursor {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraversalRequest {
    pub roots: Vec<ObjectRef>,
    pub direction: TraversalDirection,
    /// Empty includes all canonical and family edges. Otherwise sorted, unique.
    pub edges: Vec<TraversalEdge>,
    pub max_depth: u16,
    pub max_nodes: u32,
    pub max_edges: u32,
    pub max_items: u32,
    pub max_visits: u32,
    pub max_bytes: u32,
    pub cursor: Option<TraversalCursor>,
}
impl TraversalRequest {
    pub fn validate(&self, ledger: LedgerId, limits: &WireLimits) -> Result<(), AccessError> {
        if self.roots.is_empty()
            || self.roots.len() > MAX_TRAVERSAL_ROOTS
            || self
                .roots
                .iter()
                .any(|r| r.ledger != ledger || r.id.is_zero())
            || self
                .roots
                .windows(2)
                .any(|pair| matches!(pair,[a,b] if a>=b))
            || self.edges.len() > 32
            || self
                .edges
                .windows(2)
                .any(|pair| matches!(pair,[a,b] if a>=b))
        {
            return Err(AccessError::InvalidRequest);
        }
        if self.max_depth > 32
            || self.max_nodes == 0
            || self.max_nodes > MAX_TRAVERSAL_NODES
            || self.max_nodes < self.roots.len() as u32
            || self.max_edges == 0
            || self.max_edges > MAX_TRAVERSAL_EDGES
            || self.max_items == 0
            || self.max_items > limits.max_items
            || self.max_visits == 0
            || self.max_visits > limits.max_items
            || self.max_bytes < 1024
            || self.max_bytes > limits.max_frame_bytes
            || self
                .cursor
                .as_ref()
                .is_some_and(|c| c.bytes.is_empty() || c.bytes.len() > MAX_TRAVERSAL_CURSOR_BYTES)
        {
            return Err(AccessError::Capacity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraversalStop {
    Complete,
    PageLimit,
    DepthLimit,
    NodeLimit,
    EdgeLimit,
    StateLimit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraversalPage {
    pub token: ReadToken,
    pub objects: Vec<ReadObject>,
    pub next: Option<TraversalCursor>,
    pub stop: TraversalStop,
    pub visited: u32,
    pub total_visits: u32,
}

/// Binds all traversal bounds, ordering and visibility to the authenticated peer.
/// The cursor bytes themselves are excluded so subsequent pages share one scope.
pub fn traversal_scope(
    peer: &AuthenticatedPeer,
    ledger: LedgerId,
    query: &TraversalRequest,
) -> Result<ContentHash, AccessError> {
    let role = match peer.role() {
        PeerRole::Actor => 1u8,
        PeerRole::Evaluator => 2,
        PeerRole::Runtime => 3,
        PeerRole::Node { .. } => 4,
    };
    let bytes = postcard::to_stdvec(&(
        peer.principal(),
        role,
        ledger,
        &query.roots,
        query.direction,
        &query.edges,
        query.max_depth,
        query.max_nodes,
        query.max_edges,
        query.max_items,
        query.max_visits,
        query.max_bytes,
    ))
    .map_err(|_| AccessError::InvalidRequest)?;
    Ok(ContentHash(blake3::derive_key(
        "focal.traversal.scope.v1",
        &bytes,
    )))
}

#[cfg(test)]
#[path = "traversal_tests.rs"]
mod tests;
