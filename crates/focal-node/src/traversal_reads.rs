//! Fixed-prefix BFS pages retained by the existing read owner, never a new actor.
use super::*;
use focal_graph::{Direction, GraphRelation, GraphTraversalContinuation, GraphTraversalQuery};
use focal_memory::{Allocation, TraversalLimits};
use serde::{Deserialize, Serialize};

const MAX_CURSORS: usize = 32;
const CURSOR_STATE: usize = 1024 * 1024;
const OUTPUT_OVERHEAD: usize = 1024;

#[derive(Clone, Copy, Serialize, Deserialize)]
struct Position {
    schema: u16,
    id: u64,
    token: ReadToken,
    scope: ContentHash,
    expires_at: u64,
}
pub(super) struct SavedTraversal {
    position: Position,
    state: GraphTraversalContinuation,
    child: Option<u64>,
    _charge: Allocation,
}
impl SavedTraversal {
    pub(super) fn expires_at(&self) -> u64 {
        self.position.expires_at
    }
}

impl ReadViews {
    pub fn traverse(
        &mut self,
        session: &mut Session,
        context: ListReadContext,
        request: &TraversalRequest,
        limits: &WireLimits,
    ) -> Result<TraversalPage, AccessError> {
        request.validate(session.ledger(), limits)?;
        let position = request
            .cursor
            .as_ref()
            .map(|cursor| self.decode_traversal(cursor))
            .transpose()?;
        let now = self.advance(session)?;
        if let Some(p) = position {
            if p.scope != context.scope || p.token.ledger != session.ledger() {
                return Err(AccessError::Unauthorized);
            }
            if p.expires_at <= now || p.token.route_epoch != self.route_epoch {
                return Err(AccessError::SnapshotExpired);
            }
        } else if self.list_key.is_none() {
            let mut key = zeroize::Zeroizing::new([0; 32]);
            getrandom::fill(key.as_mut()).map_err(|_| AccessError::Unavailable)?;
            self.list_key = Some(key);
        }
        let consistency = position.map_or_else(
            || {
                context
                    .barrier
                    .map_or(ReadConsistency::Linearizable, |sequence| {
                        ReadConsistency::AtLeast(ReadToken {
                            ledger: session.ledger(),
                            sequence,
                            route_epoch: self.route_epoch,
                        })
                    })
            },
            |p| ReadConsistency::Exact(p.token),
        );
        let pinned = self.read(
            session,
            context.principal,
            &ReadRequest {
                consistency,
                query: ReadQuery::Objects(Vec::new()),
                max_items: 1,
            },
            context.request_id,
            limits,
        )?;
        let now = self.advance(session)?;
        let view = self
            .views
            .get(&(context.principal, pinned.token.sequence))
            .ok_or(AccessError::SnapshotExpired)?;
        let (continuation, old_child) = match position {
            Some(p) => {
                let saved = self
                    .traversals
                    .get(&p.id)
                    .ok_or(AccessError::SnapshotExpired)?;
                if p.expires_at != view.expires_at()
                    || saved.position.scope != p.scope
                    || saved.position.token != p.token
                {
                    return Err(AccessError::SnapshotExpired);
                }
                (
                    Some(view.clone_traversal(&saved.state).map_err(graph_error)?),
                    saved.child,
                )
            }
            None => (None, None),
        };
        let query = GraphTraversalQuery {
            root: *request.roots.first().ok_or(AccessError::InvalidRequest)?,
            direction: match request.direction {
                TraversalDirection::Forward => Direction::Forward,
                TraversalDirection::Reverse => Direction::Reverse,
            },
            relations: request.edges.iter().copied().map(relation).collect(),
            authority_scope: context.scope,
        };
        let mut result = view
            .traverse_roots(
                &request.roots,
                &query,
                TraversalLimits {
                    max_depth: u32::from(request.max_depth),
                    max_nodes: request.max_nodes as usize,
                    max_edges: request.max_edges as usize,
                    max_state_bytes: CURSOR_STATE,
                },
                ReadBudget {
                    max_items: request.max_items as usize,
                    max_bytes: (request.max_bytes as usize)
                        .checked_sub(OUTPUT_OVERHEAD)
                        .ok_or(AccessError::Capacity)?,
                    max_edge_visits: request.max_visits as usize,
                },
                continuation,
                now,
            )
            .map_err(graph_error)?;
        let mut objects = Vec::new();
        for page in &result.objects {
            for row in page.items() {
                let (GraphKey::Object(kind, id), GraphValue::Object(value)) =
                    (&row.key, &row.value)
                else {
                    return Err(AccessError::Unavailable);
                };
                objects
                    .try_reserve_exact(1)
                    .map_err(|_| AccessError::Capacity)?;
                objects.push(convert(*kind, *id, value)?);
            }
        }
        let pending = if let Some(state) = result.continuation.take() {
            if let Some(id) = old_child {
                let saved = self
                    .traversals
                    .get(&id)
                    .ok_or(AccessError::SnapshotExpired)?;
                Some((saved.position, None))
            } else {
                if self.traversals.len() == MAX_CURSORS {
                    return Err(AccessError::Capacity);
                }
                let id = self
                    .traversal_nonce
                    .checked_add(1)
                    .ok_or(AccessError::Capacity)?;
                let position = Position {
                    schema: 1,
                    id,
                    token: pinned.token,
                    scope: context.scope,
                    expires_at: view.expires_at(),
                };
                let charge = view.reserve_query(1024).map_err(graph_error)?;
                Some((
                    position,
                    Some(SavedTraversal {
                        position,
                        state,
                        child: None,
                        _charge: charge,
                    }),
                ))
            }
        } else {
            None
        };
        let reply = TraversalPage {
            token: pinned.token,
            objects,
            next: pending
                .as_ref()
                .map(|(p, _)| self.encode_traversal(p))
                .transpose()?,
            stop: match result.stop {
                focal_memory::TraversalStop::Complete => TraversalStop::Complete,
                focal_memory::TraversalStop::PageLimit => TraversalStop::PageLimit,
                focal_memory::TraversalStop::DepthLimit => TraversalStop::DepthLimit,
                focal_memory::TraversalStop::NodeLimit => TraversalStop::NodeLimit,
                focal_memory::TraversalStop::EdgeLimit => TraversalStop::EdgeLimit,
                focal_memory::TraversalStop::StateLimit => TraversalStop::StateLimit,
            },
            visited: u32::try_from(result.edge_visits).map_err(|_| AccessError::Capacity)?,
            total_visits: u32::try_from(result.total_edge_visits)
                .map_err(|_| AccessError::Capacity)?,
        };
        encode_payload(&reply, request.max_bytes).map_err(|_| AccessError::Capacity)?;
        // Validate/encode the entire result before publishing any continuation.
        if let Some((p, Some(saved))) = pending {
            self.traversal_nonce = p.id;
            self.traversals.insert(p.id, saved);
            if let Some(parent) = position {
                let saved = self
                    .traversals
                    .get_mut(&parent.id)
                    .ok_or(AccessError::SnapshotExpired)?;
                saved.child = Some(p.id);
            }
        }
        Ok(reply)
    }
    fn encode_traversal(&self, p: &Position) -> Result<TraversalCursor, AccessError> {
        let key = self.list_key.as_ref().ok_or(AccessError::SnapshotExpired)?;
        let mut bytes = postcard::to_stdvec(p).map_err(|_| AccessError::Capacity)?;
        let mac = blake3::keyed_hash(key, &bytes);
        bytes
            .try_reserve_exact(32)
            .map_err(|_| AccessError::Capacity)?;
        bytes.extend_from_slice(mac.as_bytes());
        if bytes.len() > MAX_TRAVERSAL_CURSOR_BYTES {
            return Err(AccessError::Capacity);
        }
        Ok(TraversalCursor { bytes })
    }
    fn decode_traversal(&self, c: &TraversalCursor) -> Result<Position, AccessError> {
        let key = self.list_key.as_ref().ok_or(AccessError::SnapshotExpired)?;
        if c.bytes.len() > MAX_TRAVERSAL_CURSOR_BYTES {
            return Err(AccessError::InvalidRequest);
        }
        let split = c
            .bytes
            .len()
            .checked_sub(32)
            .ok_or(AccessError::InvalidRequest)?;
        let body = c.bytes.get(..split).ok_or(AccessError::InvalidRequest)?;
        let mac: [u8; 32] = c
            .bytes
            .get(split..)
            .ok_or(AccessError::InvalidRequest)?
            .try_into()
            .map_err(|_| AccessError::InvalidRequest)?;
        if blake3::Hash::from_bytes(mac) != blake3::keyed_hash(key, body) {
            return Err(AccessError::InvalidRequest);
        }
        let (p, rest): (Position, _) =
            postcard::take_from_bytes(body).map_err(|_| AccessError::InvalidRequest)?;
        if !rest.is_empty() || p.schema != 1 {
            return Err(AccessError::InvalidRequest);
        }
        Ok(p)
    }
}
fn relation(value: TraversalEdge) -> GraphRelation {
    match value {
        TraversalEdge::Authored(kind) => GraphRelation::Authored(kind),
        TraversalEdge::Requirement => GraphRelation::Requirement,
        TraversalEdge::TestamentOf => GraphRelation::TestamentOf,
        TraversalEdge::Evidence => GraphRelation::Evidence,
        TraversalEdge::ArtifactInput => GraphRelation::ArtifactInput,
        TraversalEdge::ValidationOf => GraphRelation::ValidationOf,
    }
}
