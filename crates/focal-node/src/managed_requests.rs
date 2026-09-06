//! Owner-side managed replies. A protocol capability never supplies mutation
//! authority, and read/control success requires the owner's fresh read barrier.
use crate::{embedded::EmbeddedNode, host::access, reads::ReadViews, streams::Streams};
use focal_ledger::{ManagedSubmission, RequestStreamSubmission, Session};
use focal_model::*;
use focal_wire::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn response_bytes(operation: &Operation, frame: u32, max_items: u32) -> Option<usize> {
    let receipt = size_of::<ManagedReceipt>()
        .checked_add(size_of::<ResponseEnvelope>())?
        .checked_add(1024)?
        .checked_add((max_items as usize).checked_mul(size_of::<ClaimId>())?)?;
    let bytes = match operation {
        Operation::Managed {
            operation: ManagedOperation::Cursor(stream),
            ..
        } => receipt.checked_add(stream.credits().bytes.min(frame) as usize)?,
        Operation::Managed { .. }
        | Operation::RequestStreamControl { .. }
        | Operation::RequestStreamRead { .. } => receipt,
        _ => return None,
    };
    Some(bytes.min(frame as usize))
}
pub(crate) fn receipt_copy(
    receipt: &ManagedReceipt,
    limits: &WireLimits,
) -> Result<ManagedReceipt, AccessError> {
    let count = match &receipt.outcome {
        ManagedReceiptOutcome::Domain(
            CommandResult::Generated(ids) | CommandResult::Existing(ids),
        ) => ids.len(),
        ManagedReceiptOutcome::Cursor {
            record: Some(record),
            ..
        } => match &record.filter {
            CursorFilterSnapshot::All => 0,
            CursorFilterSnapshot::Claims(ids) => ids.len(),
        },
        _ => 0,
    };
    if count > limits.max_items as usize
        || postcard::experimental::serialized_size(receipt).map_err(|_| AccessError::Capacity)?
            > limits.max_frame_bytes as usize
    {
        return Err(AccessError::Capacity);
    }
    Ok(receipt.clone())
}
pub(crate) fn reply(
    session: &Session,
    key: &ManagedRequestKey,
    intent: ContentHash,
    family: ManagedRequestFamily,
    stream: Option<StreamReply>,
    limits: &WireLimits,
) -> Result<Option<Response>, AccessError> {
    session
        .managed_receipt(key, intent, family)
        .map_err(access)?
        .map(|receipt| {
            receipt_copy(receipt, limits)
                .map(|receipt| Response::Managed(ManagedReply { receipt, stream }))
        })
        .transpose()
}
pub(crate) fn read_after_barrier(
    session: &Session,
    principal: ParticipantId,
    cluster: [u8; 16],
    query: &RequestStreamQuery,
    prefix: SessionSeq,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<RequestStreamReadReply, AccessError> {
    if cluster != session.cluster_id() || session.sequence() < prefix {
        return Err(AccessError::Unauthorized);
    }
    let view = session
        .request_stream_read(principal, query)
        .map_err(access)?;
    let capacity = response_bytes(
        &Operation::RequestStreamRead {
            cluster,
            query: *query,
        },
        limits.max_frame_bytes,
        limits.max_items,
    )
    .and_then(|bytes| bytes.checked_mul(16))
    .ok_or(AccessError::Capacity)?;
    if view.item_count() > limits.max_items as usize
        || view.owned_bytes().map_err(access)? > capacity
    {
        return Err(AccessError::Capacity);
    }
    let page = view.to_owned().map_err(access)?;
    Ok(RequestStreamReadReply {
        token: ReadToken {
            ledger: session.ledger(),
            sequence: session.sequence(),
            route_epoch: route,
        },
        page,
    })
}
pub(crate) fn control_reply(
    session: &Session,
    input: &RequestStreamControlInput,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<Option<Response>, AccessError> {
    let Some(receipt) = session.request_stream_receipt(input).map_err(access)? else {
        return Ok(None);
    };
    if let RequestStreamControlOutcome::Sealed(receipt) = &receipt.outcome {
        // Inspect the bounded nested receipt before cloning the enclosing value.
        validate_managed_receipt(
            receipt,
            &receipt.key,
            match receipt.outcome {
                ManagedReceiptOutcome::Domain(_) => ManagedRequestFamily::Domain,
                ManagedReceiptOutcome::Cursor { .. } => ManagedRequestFamily::Cursor,
                ManagedReceiptOutcome::Sealed { family } => family,
            },
            receipt.intent_hash,
            limits,
        )
        .map_err(|_| AccessError::Capacity)?;
    }
    if postcard::experimental::serialized_size(receipt).map_err(|_| AccessError::Capacity)?
        > limits.max_frame_bytes as usize
    {
        return Err(AccessError::Capacity);
    }
    Ok(Some(Response::RequestStreamControlled(
        RequestStreamControlReply {
            token: ReadToken {
                ledger: session.ledger(),
                sequence: session.sequence(),
                route_epoch: route,
            },
            receipt: receipt.clone(),
        },
    )))
}
pub(crate) fn local(
    node: &mut EmbeddedNode,
    views: &mut ReadViews,
    streams: &mut Streams,
    verified: VerifiedRequest,
    limits: &WireLimits,
) -> Result<Response, AccessError> {
    let status = node.session.status();
    if status.voters != [status.node_id]
        || !status.learners.is_empty()
        || !node.session.is_authoritative()
    {
        return Err(AccessError::Unavailable);
    }
    let cluster = match &verified.request().operation {
        Operation::Managed { key, .. } => key.stream.cluster,
        Operation::RequestStreamControl { cluster, .. }
        | Operation::RequestStreamRead { cluster, .. } => *cluster,
        _ => return Err(AccessError::InvalidRequest),
    };
    if cluster != node.session.cluster_id() || verified.request().ledger != node.session.ledger() {
        return Err(AccessError::Unauthorized);
    }
    if matches!(
        verified.request().operation,
        Operation::Managed { .. } | Operation::RequestStreamControl { .. }
    ) {
        match node.session.begin_managed_support() {
            Ok(())
            | Err(focal_ledger::LedgerError::Consensus(
                focal_consensus::ConsensusError::PersistencePending,
            )) => {}
            Err(error) => return Err(access(error)),
        }
        let events = node.session.poll().map_err(access)?;
        if !events.messages.is_empty() {
            return Err(AccessError::OutcomeUnknown);
        }
    }
    let request = verified.request();
    let principal = verified.peer().principal();
    let route = request.route_epoch;
    let id = request.request_id;
    match &request.operation {
        Operation::RequestStreamRead { cluster, query } => {
            let prefix = barrier(&mut node.session, principal, id)?;
            read_after_barrier(
                &node.session,
                principal,
                *cluster,
                query,
                prefix,
                route,
                limits,
            )
            .map(Response::RequestStreamRead)
        }
        Operation::RequestStreamControl { .. } => {
            let input = verified.into_request_stream_control()?;
            match node
                .session
                .propose_request_stream(&input)
                .map_err(access)?
            {
                RequestStreamSubmission::Committed(_) => {}
                RequestStreamSubmission::Pending(_) => {
                    let events = node.session.poll().map_err(access)?;
                    if !events.messages.is_empty() {
                        return Err(AccessError::OutcomeUnknown);
                    }
                }
            }
            barrier(&mut node.session, principal, id)?;
            control_reply(&node.session, &input, route, limits)?.ok_or(AccessError::OutcomeUnknown)
        }
        Operation::Managed {
            operation: ManagedOperation::Cursor(stream),
            ..
        } => {
            let (key, family, intent) =
                managed_request_identity(request).map_err(|_| AccessError::InvalidRequest)?;
            if let Some(receipt) = node
                .session
                .managed_receipt(&key, intent, family)
                .map_err(access)?
                && matches!(receipt.outcome, ManagedReceiptOutcome::Sealed { .. })
            {
                return Ok(Response::Managed(ManagedReply {
                    receipt: receipt_copy(receipt, limits)?,
                    stream: None,
                }));
            }
            let delivery = streams.handle(
                &mut node.session,
                views,
                verified.peer(),
                request,
                stream,
                limits,
            )?;
            reply(&node.session, &key, intent, family, Some(delivery), limits)?
                .ok_or(AccessError::OutcomeUnknown)
        }
        Operation::Managed {
            operation: ManagedOperation::Submit { .. },
            ..
        } => {
            let (key, family, intent) =
                managed_request_identity(request).map_err(|_| AccessError::InvalidRequest)?;
            if let Some(known) = reply(&node.session, &key, intent, family, None, limits)? {
                return Ok(known);
            }
            let authority = AuthorityContext {
                runtime: false,
                cause: Cause::Root(node.identity.root),
                policy_revision: 1,
                logical_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| AccessError::Unavailable)?
                    .as_secs(),
                evidence: Vec::new(),
            };
            let mut input = verified.into_managed(authority)?;
            input.authority.evidence = crate::host::attest(node, &input.command)?;
            match node.session.propose_managed(&input).map_err(access)? {
                ManagedSubmission::Committed(receipt) => Ok(Response::Managed(ManagedReply {
                    receipt: *receipt,
                    stream: None,
                })),
                ManagedSubmission::Pending(_) => {
                    let events = node.session.poll().map_err(access)?;
                    if !events.messages.is_empty() {
                        return Err(AccessError::OutcomeUnknown);
                    }
                    reply(&node.session, &key, intent, family, None, limits)?
                        .ok_or(AccessError::OutcomeUnknown)
                }
                ManagedSubmission::Domain(_) => Err(AccessError::InvalidRequest),
            }
        }
        _ => Err(AccessError::UnsupportedOperation),
    }
}
fn barrier(
    session: &mut Session,
    principal: ParticipantId,
    id: RequestId,
) -> Result<SessionSeq, AccessError> {
    let term = session.status().term;
    let mut context = b"focal.local.managed.read.v1\0".to_vec();
    context.extend_from_slice(&principal.0);
    context.extend_from_slice(&id.0);
    session.read_index(context.clone()).map_err(access)?;
    let events = session.poll().map_err(access)?;
    if !events.messages.is_empty() || session.status().term != term || !session.is_authoritative() {
        return Err(AccessError::Unavailable);
    }
    events
        .read_barriers
        .iter()
        .find(|(actual, _)| *actual == context)
        .map(|(_, prefix)| *prefix)
        .ok_or(AccessError::Unavailable)
}
