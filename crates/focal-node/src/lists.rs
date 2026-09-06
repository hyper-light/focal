//! Index-backed bounded listing. Cursors authenticate one query and retain an
//! existing fixed-prefix graph lease; there is no additional cursor registry.
use super::*;
use focal_graph::GraphScan;
use serde::{Deserialize, Serialize};

const OUTPUT_OVERHEAD: usize = 768;

pub(crate) struct ListReadContext {
    pub principal: ParticipantId,
    pub scope: ContentHash,
    pub request_id: RequestId,
    /// A quorum host supplies its completed barrier. Local single-voter serving
    /// leaves this absent and reuses the existing linearizable read path.
    pub barrier: Option<SessionSeq>,
}

#[derive(Serialize, Deserialize)]
struct Position {
    schema: u16,
    token: ReadToken,
    scope: ContentHash,
    expires_at: u64,
    after: GraphKey,
}

impl ReadViews {
    pub fn list(
        &mut self,
        session: &mut Session,
        context: ListReadContext,
        request: &ListRequest,
        limits: &WireLimits,
    ) -> Result<ListPage, AccessError> {
        self.list_selected(session, context, request, None, None, limits)
    }

    pub fn validators(
        &mut self,
        session: &mut Session,
        context: ListReadContext,
        request: &ValidatorRequest,
        limits: &WireLimits,
    ) -> Result<ListPage, AccessError> {
        request.validate(limits)?;
        self.list_selected(
            session,
            context,
            &request.query,
            Some(request),
            None,
            limits,
        )
    }

    pub fn selection(
        &mut self,
        session: &mut Session,
        context: ListReadContext,
        request: &SelectionRequest,
        limits: &WireLimits,
    ) -> Result<ListPage, AccessError> {
        request.validate(session.ledger(), limits)?;
        self.list_selected(
            session,
            context,
            &request.query,
            None,
            Some(&request.predicates),
            limits,
        )
    }

    fn list_selected(
        &mut self,
        session: &mut Session,
        context: ListReadContext,
        request: &ListRequest,
        validator: Option<&ValidatorRequest>,
        predicates: Option<&SelectionPredicates>,
        limits: &WireLimits,
    ) -> Result<ListPage, AccessError> {
        request.filter.validate()?;
        if request.max_items == 0
            || request.max_visits == 0
            || request.max_items > limits.max_items
            || request.max_visits > limits.max_items
        {
            return Err(AccessError::Capacity);
        }
        let position = request
            .cursor
            .as_ref()
            .map(|cursor| self.decode_cursor(cursor))
            .transpose()?;
        let now = self.advance(session)?;
        if let Some(position) = &position {
            if position.scope != context.scope || position.token.ledger != session.ledger() {
                return Err(AccessError::Unauthorized);
            }
            if position.expires_at <= now || position.token.route_epoch != self.route_epoch {
                return Err(AccessError::SnapshotExpired);
            }
        } else if self.list_key.is_none() {
            let mut key = zeroize::Zeroizing::new([0; 32]);
            getrandom::fill(key.as_mut()).map_err(|_| AccessError::Unavailable)?;
            self.list_key = Some(key);
        }
        let consistency = position.as_ref().map_or_else(
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
            |position| ReadConsistency::Exact(position.token),
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
        if position
            .as_ref()
            .is_some_and(|position| position.expires_at != view.expires_at())
        {
            return Err(AccessError::SnapshotExpired);
        }
        let (objects, after, visited, more) = select(
            view,
            Selection {
                filter: &request.filter,
                validator,
                predicates,
            },
            position.as_ref().map(|position| &position.after),
            request.max_items,
            request.max_visits,
            now,
            limits,
        )?;
        let next = if more {
            let after = after.ok_or(AccessError::Capacity)?;
            Some(self.encode_cursor(&Position {
                schema: 1,
                token: pinned.token,
                scope: context.scope,
                expires_at: view.expires_at(),
                after,
            })?)
        } else {
            None
        };
        Ok(ListPage {
            token: pinned.token,
            objects,
            next,
            visited,
        })
    }

    fn encode_cursor(&self, position: &Position) -> Result<ListCursor, AccessError> {
        let key = self.list_key.as_ref().ok_or(AccessError::SnapshotExpired)?;
        let mut bytes = postcard::to_stdvec(position).map_err(|_| AccessError::InvalidRequest)?;
        let mac = blake3::keyed_hash(key, &bytes);
        if bytes
            .len()
            .checked_add(32)
            .is_none_or(|len| len > MAX_LIST_CURSOR_BYTES)
        {
            return Err(AccessError::Capacity);
        }
        bytes
            .try_reserve_exact(32)
            .map_err(|_| AccessError::Capacity)?;
        bytes.extend_from_slice(mac.as_bytes());
        Ok(ListCursor { bytes })
    }
    fn decode_cursor(&self, cursor: &ListCursor) -> Result<Position, AccessError> {
        let key = self.list_key.as_ref().ok_or(AccessError::SnapshotExpired)?;
        if cursor.bytes.len() > MAX_LIST_CURSOR_BYTES {
            return Err(AccessError::InvalidRequest);
        }
        let split = cursor
            .bytes
            .len()
            .checked_sub(32)
            .ok_or(AccessError::InvalidRequest)?;
        let body = cursor
            .bytes
            .get(..split)
            .ok_or(AccessError::InvalidRequest)?;
        let mac: [u8; 32] = cursor
            .bytes
            .get(split..)
            .ok_or(AccessError::InvalidRequest)?
            .try_into()
            .map_err(|_| AccessError::InvalidRequest)?;
        if blake3::Hash::from_bytes(mac) != blake3::keyed_hash(key, body) {
            return Err(AccessError::InvalidRequest);
        }
        let (position, rest): (Position, _) =
            postcard::take_from_bytes(body).map_err(|_| AccessError::InvalidRequest)?;
        if !rest.is_empty() || position.schema != 1 {
            return Err(AccessError::InvalidRequest);
        }
        Ok(position)
    }
}

fn index(filter: &ListFilter, ledger: LedgerId) -> GraphScan {
    if let Some(testament) = filter.testament {
        GraphScan::Forward(ObjectRef {
            ledger,
            kind: ObjectKind::Testament,
            id: ObjectId(testament.0),
        })
    } else if let Some(claim) = filter.claim {
        GraphScan::ByClaim(claim)
    } else if let Some(status) = filter.status {
        GraphScan::Status(status)
    } else {
        GraphScan::Objects(Some(filter.kind))
    }
}

struct Candidate {
    object: Option<(ReadObject, usize)>,
    full: bool,
}
impl Candidate {
    fn skipped() -> Self {
        Self {
            object: None,
            full: false,
        }
    }
    fn full() -> Self {
        Self {
            object: None,
            full: true,
        }
    }
}

struct Selection<'a> {
    filter: &'a ListFilter,
    validator: Option<&'a ValidatorRequest>,
    predicates: Option<&'a SelectionPredicates>,
}

fn select(
    view: &GraphSnapshot,
    selection: Selection<'_>,
    after: Option<&GraphKey>,
    max_items: u32,
    max_visits: u32,
    now: u64,
    limits: &WireLimits,
) -> Result<(Vec<ReadObject>, Option<GraphKey>, u32, bool), AccessError> {
    let Selection {
        filter,
        validator,
        predicates,
    } = selection;
    let scan = index(filter, view.ledger());
    let max_bytes = usize::try_from(limits.max_cost).map_err(|_| AccessError::Capacity)?;
    let output_limit = (limits.max_frame_bytes as usize)
        .checked_sub(OUTPUT_OVERHEAD)
        .ok_or(AccessError::Capacity)?;
    let mut visited_bytes = 0usize;
    if let (Some(testament), Some(claim)) = (filter.testament, filter.claim) {
        let reference = ObjectRef {
            ledger: view.ledger(),
            kind: ObjectKind::Testament,
            id: ObjectId(testament.0),
        };
        let parent = view.project_object(reference, now, |object, bytes| {
            let matches = matches!(object, GraphObject::Testament(value) if value.content().claim == claim);
            (matches, bytes)
        }).map_err(graph_error)?;
        match parent {
            Some((_, bytes)) if bytes > max_bytes => return Err(AccessError::Capacity),
            Some((true, bytes)) => visited_bytes = bytes,
            _ => return Ok((Vec::new(), None, 0, false)),
        }
    }
    let mut output_bytes = 0usize;
    let mut visited = 0u32;
    // Grow only for actual matches. Reserving max_items here would retain a
    // large empty vector after the owner shrinks a zero-match page allowance.
    let mut objects = Vec::new();
    let mut last = after.cloned();
    loop {
        let Some(entry) = view
            .next_candidate(&scan, last.as_ref(), now)
            .map_err(graph_error)?
        else {
            return Ok((objects, last, visited, false));
        };
        if visited == max_visits || objects.len() == max_items as usize {
            return Ok((objects, last, visited, true));
        }
        let index_bytes = entry.bytes;
        let candidate = if let Some(reference) = entry.object.filter(|r| r.kind == filter.kind) {
            view.project_object(reference, now, |object, bytes| {
                let bytes = if matches!(entry.key, GraphKey::Object(..)) {
                    bytes
                } else {
                    bytes
                        .checked_add(index_bytes)
                        .ok_or(AccessError::Capacity)?
                };
                if visited_bytes
                    .checked_add(bytes)
                    .is_none_or(|bytes| bytes > max_bytes)
                {
                    return Ok((Candidate::full(), 0));
                }
                if !matches_filter(object, filter) || predicates.is_some_and(|predicate| !matches_predicates(object, predicate)) || validator.is_some_and(|query| !matches!(object, GraphObject::Validation(value) if query.matches(value.content()))) {
                    return Ok((Candidate::skipped(), bytes));
                }
                let size = object_wire_size(object)?;
                if output_bytes
                    .checked_add(size)
                    .is_none_or(|bytes| bytes > output_limit)
                {
                    return Ok((Candidate::full(), 0));
                }
                objects
                    .try_reserve_exact(1)
                    .map_err(|_| AccessError::Capacity)?;
                Ok((
                    Candidate {
                        object: Some((convert(reference.kind, reference.id, object)?, size)),
                        full: false,
                    },
                    bytes,
                ))
            })
            .map_err(graph_error)?
            .ok_or(AccessError::Unavailable)??
        } else {
            (Candidate::skipped(), index_bytes)
        };
        if visited_bytes
            .checked_add(candidate.1)
            .is_none_or(|bytes| bytes > max_bytes)
            || candidate.0.full
        {
            return if visited == 0 {
                Err(AccessError::Capacity)
            } else {
                Ok((objects, last, visited, true))
            };
        }
        visited_bytes = visited_bytes
            .checked_add(candidate.1)
            .ok_or(AccessError::Capacity)?;
        visited = visited.checked_add(1).ok_or(AccessError::Capacity)?;
        last = Some(entry.key);
        if let Some((object, size)) = candidate.0.object {
            output_bytes = output_bytes
                .checked_add(size)
                .ok_or(AccessError::Capacity)?;
            objects.push(object);
        }
    }
}

fn object_wire_size(object: &GraphObject) -> Result<usize, AccessError> {
    let bytes = match object {
        GraphObject::Claim(value) => postcard::experimental::serialized_size(value),
        GraphObject::Testament(value) => postcard::experimental::serialized_size(value),
        GraphObject::Validation(value) => postcard::experimental::serialized_size(value),
        GraphObject::Artifact(value) => postcard::experimental::serialized_size(value),
    }
    .map_err(|_| AccessError::Capacity)?;
    bytes.checked_add(17).ok_or(AccessError::Capacity)
}

fn matches_filter(object: &GraphObject, filter: &ListFilter) -> bool {
    match object {
        GraphObject::Claim(value) => {
            filter
                .source
                .is_none_or(|source| value.content().issuer() == Some(source))
                && filter
                    .target
                    .is_none_or(|target| value.content().subject() == Some(target))
                && filter
                    .status
                    .is_none_or(|status| value.lifecycle().status == status)
                && filter
                    .action
                    .is_none_or(|action| value.content().action() == Some(action))
        }
        GraphObject::Testament(value) => filter
            .claim
            .is_none_or(|claim| value.content().claim == claim),
        GraphObject::Validation(value) => {
            filter
                .claim
                .is_none_or(|claim| value.content().claim == claim)
                && filter
                    .evaluator
                    .is_none_or(|evaluator| value.content().evaluator == evaluator)
                && filter
                    .validation_kind
                    .is_none_or(|kind| value.content().kind == kind)
                && filter
                    .phase
                    .is_none_or(|phase| value.content().phase == phase)
                && filter.mode.is_none_or(|mode| value.content().mode == mode)
        }
        GraphObject::Artifact(value) => {
            filter
                .producer
                .is_none_or(|producer| value.content().producer == producer)
                && filter
                    .artifact_kind
                    .as_ref()
                    .is_none_or(|kind| &value.content().kind == kind)
                && filter
                    .schema
                    .is_none_or(|schema| value.content().schema_hash == schema)
        }
    }
}

#[cfg(test)]
#[path = "lists_tests.rs"]
mod tests;

fn matches_predicates(object: &GraphObject, predicates: &SelectionPredicates) -> bool {
    match object {
        GraphObject::Claim(value) => predicates.claim(value),
        GraphObject::Testament(value) => predicates.testament(value),
        GraphObject::Artifact(value) => predicates.artifact(value),
        GraphObject::Validation(value) => predicates.created(value.lifecycle().created),
    }
}
