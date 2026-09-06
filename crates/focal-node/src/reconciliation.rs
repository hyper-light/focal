//! Authenticated committed receipt reads. Graph leases contain no receipt or
//! epoch authority; callers must supply a completed, current-term ReadIndex.
use crate::host::access;
use focal_ledger::Session;
use focal_model::{
    ClaimId, CursorMutationReceipt, MutationReceipt, ParticipantId, ReconcileQuery, RequestId,
    RouteEpoch, SessionSeq,
};
use focal_wire::{AccessError, ReadToken, ReconcileReply, ResponseEnvelope, WireLimits};

/// Admission upper bound shared by both owners and the copy boundary. Fixed
/// structures conservatively cover their encoded fields; the only variable
/// receipt members are at most max_items fixed-size claim IDs. Owners apply
/// their standard 32x resident/encoding allowance to this bounded payload.
pub(crate) fn response_bytes(query: &ReconcileQuery, frame: u32, max_items: u32) -> Option<usize> {
    let fixed = std::mem::size_of::<ReconcileReply>()
        .checked_add(std::mem::size_of::<ResponseEnvelope>())?
        .checked_add(512)?;
    let bytes = match query {
        ReconcileQuery::Epoch { .. } => fixed,
        ReconcileQuery::Receipt { .. } => fixed
            .checked_add(std::mem::size_of::<MutationReceipt>())?
            .checked_add(std::mem::size_of::<CursorMutationReceipt>())?
            .checked_add((max_items as usize).checked_mul(std::mem::size_of::<ClaimId>())?)?,
    };
    Some(bytes.min(frame as usize))
}

pub(crate) fn after_barrier(
    session: &Session,
    principal: ParticipantId,
    query: &ReconcileQuery,
    prefix: SessionSeq,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<ReconcileReply, AccessError> {
    let view = session
        .reconcile_at_least(session.ledger(), principal, query, prefix)
        .map_err(access)?;
    // Inspect the borrowed variable member before any result allocation.
    if view.item_count() > limits.max_items as usize {
        return Err(AccessError::Capacity);
    }
    let payload_limit = response_bytes(query, limits.max_frame_bytes, limits.max_items)
        .ok_or(AccessError::Capacity)?;
    let copy_limit = payload_limit.checked_mul(16).ok_or(AccessError::Capacity)?;
    if view.owned_bytes().map_err(|_| AccessError::Capacity)? > copy_limit {
        return Err(AccessError::Capacity);
    }
    let page = view.to_owned().map_err(|_| AccessError::Capacity)?;
    let reply = ReconcileReply {
        applied_index: session.status().applied_index,
        token: ReadToken {
            ledger: page.ledger,
            sequence: page.sequence,
            route_epoch: route,
        },
        page,
    };
    // Fixed headroom exceeds the enclosing protocol header's encoded bound.
    let encoded = postcard::experimental::serialized_size(&reply)
        .ok()
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or(AccessError::Capacity)?;
    if encoded > payload_limit {
        return Err(AccessError::Capacity);
    }
    Ok(reply)
}

pub(crate) fn local(
    session: &mut Session,
    principal: ParticipantId,
    query: &ReconcileQuery,
    request: RequestId,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<ReconcileReply, AccessError> {
    let status = session.status();
    // The synchronous adapter cannot deliver inter-node messages. Reject that
    // configuration before creating read interest, so none can be discarded.
    if status.voters.as_slice() != [status.node_id]
        || !status.learners.is_empty()
        || !session.is_authoritative()
        || session.active_route().is_some_and(|active| active != route)
        || session
            .placement()
            .is_some_and(|placement| placement.kind == focal_ledger::SessionFenceKind::Cutover)
    {
        return Err(AccessError::Unavailable);
    }
    let mut context = b"focal.local.reconcile.v1\0".to_vec();
    context.extend_from_slice(&principal.0);
    context.extend_from_slice(&request.0);
    session.read_index(context.clone()).map_err(access)?;
    let events = session.poll().map_err(access)?;
    if !session.is_authoritative() || session.status().term != status.term {
        return Err(AccessError::Unavailable);
    }
    let prefix = events
        .read_barriers
        .iter()
        .find(|(observed, _)| observed == &context)
        .map(|(_, prefix)| *prefix)
        .ok_or(AccessError::Unavailable)?;
    after_barrier(session, principal, query, prefix, route, limits)
}
