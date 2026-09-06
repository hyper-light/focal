//! Scalar counts copied directly from committed Core after an actual ReadIndex.
use crate::host::access;
use focal_ledger::Session;
use focal_model::{ParticipantId, RequestId, RouteEpoch, SessionSeq};
use focal_wire::{AccessError, LedgerSummary, ReadToken, ResponseEnvelope, WireLimits};
pub(crate) fn response_bytes(frame: u32) -> Option<usize> {
    Some(
        std::mem::size_of::<LedgerSummary>()
            .checked_add(std::mem::size_of::<ResponseEnvelope>())?
            .checked_add(512)?
            .min(frame as usize),
    )
}
pub(crate) fn after_barrier(
    session: &Session,
    prefix: SessionSeq,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<LedgerSummary, AccessError> {
    let state = session.read_at_least(prefix).map_err(access)?;
    let count = |n: usize| u64::try_from(n).map_err(|_| AccessError::Capacity);
    let reply = LedgerSummary {
        token: ReadToken {
            ledger: state.ledger,
            sequence: state.sequence,
            route_epoch: route,
        },
        applied_index: session.status().applied_index,
        claims: count(state.claims.len())?,
        testaments: count(state.testaments.len())?,
        artifacts: count(state.artifacts.len())?,
        validations: count(state.validations.len())?,
        evidence_sets: count(state.evidence_sets.len())?,
        validation_runs: count(state.runs.len())?,
    };
    let encoded = postcard::experimental::serialized_size(&reply)
        .ok()
        .and_then(|n| n.checked_add(256))
        .ok_or(AccessError::Capacity)?;
    if encoded > response_bytes(limits.max_frame_bytes).ok_or(AccessError::Capacity)? {
        return Err(AccessError::Capacity);
    }
    Ok(reply)
}
pub(crate) fn local(
    session: &mut Session,
    principal: ParticipantId,
    request: RequestId,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<LedgerSummary, AccessError> {
    let status = session.status();
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
    let mut context = b"focal.local.summary.v1\0".to_vec();
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
    after_barrier(session, prefix, route, limits)
}
