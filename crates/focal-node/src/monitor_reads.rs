//! Owned current-prefix monitor reads; no independent mutable wait registry.
use crate::host::access;
use focal_ledger::Session;
use focal_model::*;
use focal_wire::*;

// Covers the bounded BTreeSet clone, response and transient barrier bookkeeping.
// The existing request allowance moves through delivery with OwnedResponse.
pub(crate) const RESPONSE_BYTES: usize = 64 * 1024;

pub(crate) fn after_barrier(
    session: &Session,
    id: MonitorId,
    prefix: SessionSeq,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<MonitorPage, AccessError> {
    let state = session.read_at_least(prefix).map_err(access)?;
    let monitor = state.monitors.get(&id);
    if monitor
        .is_some_and(|value| value.roots.len() > MAX_MONITOR_ROOTS.min(limits.max_items as usize))
    {
        return Err(AccessError::Capacity);
    }
    let encoded = postcard::experimental::serialized_size(&monitor)
        .ok()
        .and_then(|bytes| bytes.checked_add(512))
        .ok_or(AccessError::Capacity)?;
    if encoded > limits.max_frame_bytes as usize || encoded > RESPONSE_BYTES {
        return Err(AccessError::Capacity);
    }
    Ok(MonitorPage {
        token: ReadToken {
            ledger: state.ledger,
            sequence: state.sequence,
            route_epoch: route,
        },
        applied_index: session.status().applied_index,
        id,
        monitor: monitor.cloned(),
    })
}

pub(crate) fn local(
    session: &mut Session,
    principal: ParticipantId,
    request: RequestId,
    id: MonitorId,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<MonitorPage, AccessError> {
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
    let mut context = b"focal.local.monitor.v1\0".to_vec();
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
    after_barrier(session, id, prefix, route, limits)
}
