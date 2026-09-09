use crate::*;
use focal_model::*;
use serde::{Deserialize, Serialize};

/// Managed mutations never enter the legacy principal-wide epoch namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ManagedOperation {
    Submit {
        expected_revision: Option<ObjectRevision>,
        command: Command,
    },
    Cursor(StreamRequest),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedReply {
    pub receipt: ManagedReceipt,
    /// Delivery may advance on an exact retry; the committed receipt never does.
    pub stream: Option<StreamReply>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestStreamControlReply {
    pub token: ReadToken,
    pub receipt: RequestStreamControlReceipt,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestStreamReadReply {
    pub token: ReadToken,
    pub page: RequestStreamRead,
}

/// Authentication-free identity check for private journals. Peer authorization
/// is a separate mandatory server check and cannot be inferred from this value.
pub fn managed_request_identity(
    request: &RequestEnvelope,
) -> Result<(ManagedRequestKey, ManagedRequestFamily, ContentHash), WireError> {
    let Operation::Managed { key, operation } = &request.operation else {
        return Err(WireError::InvalidFrame);
    };
    // Managed operations ride the managed profile or, on a native ledger,
    // the native profile that admits them (`native_profile_operation`).
    if (request.protocol != MANAGED_PROTOCOL_VERSION
        && request.protocol != NATIVE_PROTOCOL_VERSION
        && !is_peer_request(request))
        || request.request_epoch != RequestEpoch(1)
        || request.route_epoch.0 == 0
        || request.request_id != key.id
        || request.ledger != key.stream.ledger
    {
        return Err(WireError::InvalidFrame);
    }
    let (hash, family) = managed_intent(key, operation)?;
    Ok((*key, family, hash))
}
pub fn managed_intent(
    key: &ManagedRequestKey,
    operation: &ManagedOperation,
) -> Result<(ContentHash, ManagedRequestFamily), WireError> {
    if !key.is_valid() {
        return Err(WireError::InvalidFrame);
    }
    match operation {
        ManagedOperation::Submit {
            expected_revision,
            command,
        } => {
            if matches!(
                command,
                Command::NegotiateEpoch { .. } | Command::AdvanceEpochFloor { .. }
            ) {
                return Err(WireError::Access(AccessError::UnsupportedOperation));
            }
            Ok((
                managed_command_parts_hash(
                    key.stream.ledger,
                    key.stream.principal,
                    expected_revision,
                    command,
                )
                .map_err(|_| WireError::InvalidFrame)?,
                ManagedRequestFamily::Domain,
            ))
        }
        ManagedOperation::Cursor(stream) => Ok((
            cursor_request_intent(key.stream.ledger, key.stream.principal, stream)?,
            ManagedRequestFamily::Cursor,
        )),
    }
}
/// The existing cursor intent algorithm, streamed without allocating a second
/// serialized copy of the client's request.
pub fn cursor_request_intent(
    ledger: LedgerId,
    principal: ParticipantId,
    stream: &StreamRequest,
) -> Result<ContentHash, WireError> {
    struct Digest(blake3::Hasher);
    impl postcard::ser_flavors::Flavor for Digest {
        type Output = ContentHash;
        fn try_push(&mut self, byte: u8) -> Result<(), postcard::Error> {
            self.0.update(&[byte]);
            Ok(())
        }
        fn try_extend(&mut self, bytes: &[u8]) -> Result<(), postcard::Error> {
            self.0.update(bytes);
            Ok(())
        }
        fn finalize(self) -> Result<Self::Output, postcard::Error> {
            Ok(ContentHash(*self.0.finalize().as_bytes()))
        }
    }
    postcard::serialize_with_flavor(
        &(ledger, principal, stream),
        Digest(blake3::Hasher::new_derive_key("focal.stream.intent.v1")),
    )
    .map_err(|_| WireError::InvalidFrame)
}

pub fn validate_managed_receipt(
    receipt: &ManagedReceipt,
    key: &ManagedRequestKey,
    family: ManagedRequestFamily,
    intent: ContentHash,
    limits: &WireLimits,
) -> Result<(), WireError> {
    if !key.is_valid()
        || receipt.key != *key
        || receipt.intent_hash != intent
        || receipt.raft_index == 0
    {
        return Err(WireError::InvalidFrame);
    }
    match &receipt.outcome {
        ManagedReceiptOutcome::Domain(outcome) if family == ManagedRequestFamily::Domain => {
            if receipt.sequence.0 == 0 {
                return Err(WireError::InvalidFrame);
            }
            if let CommandResult::Generated(ids) | CommandResult::Existing(ids) = outcome
                && ids.len() > limits.max_items as usize
            {
                return Err(WireError::Limit);
            }
        }
        ManagedReceiptOutcome::Cursor {
            revision,
            floor,
            record,
        } if family == ManagedRequestFamily::Cursor => {
            if *revision == 0 || *floor > receipt.sequence {
                return Err(WireError::InvalidFrame);
            }
            if let Some(record) = record {
                crate::reconcile::validate_cursor_record_at(
                    record,
                    key.stream.ledger,
                    receipt.sequence,
                    limits,
                )?;
            }
        }
        ManagedReceiptOutcome::Sealed { family: actual } if *actual == family => {}
        _ => return Err(WireError::InvalidFrame),
    }
    Ok(())
}
pub(crate) fn validate_managed_reply(
    request: &RequestEnvelope,
    route: RouteEpoch,
    reply: &ManagedReply,
    principal: Option<ParticipantId>,
    limits: &WireLimits,
) -> Result<(), WireError> {
    let (key, family, intent) = managed_request_identity(request)?;
    if route != request.route_epoch || principal.is_some_and(|p| p != key.stream.principal) {
        return Err(WireError::InvalidFrame);
    }
    validate_managed_receipt(&reply.receipt, &key, family, intent, limits)?;
    match (&request.operation, &reply.receipt.outcome, &reply.stream) {
        (
            Operation::Managed {
                operation: ManagedOperation::Cursor(stream),
                ..
            },
            ManagedReceiptOutcome::Cursor { .. },
            Some(delivery),
        ) => {
            crate::handler::validate_stream_response(
                stream,
                delivery,
                request.ledger,
                route,
                limits,
            )?;
        }
        (_, ManagedReceiptOutcome::Sealed { .. } | ManagedReceiptOutcome::Domain(_), None) => {}
        _ => return Err(WireError::InvalidFrame),
    }
    Ok(())
}

pub(crate) fn validate_control_request(
    cluster: [u8; 16],
    ledger: LedgerId,
    principal: ParticipantId,
    command: &RequestStreamCommand,
    max_items: u32,
) -> Result<u64, AccessError> {
    if cluster == [0; 16] || principal.is_zero() {
        return Err(AccessError::InvalidRequest);
    }
    let scope = |stream: &RequestStreamIdentity| {
        if !stream.is_valid() {
            Err(AccessError::InvalidRequest)
        } else if stream.cluster != cluster
            || stream.ledger != ledger
            || stream.principal != principal
        {
            Err(AccessError::Unauthorized)
        } else {
            Ok(())
        }
    };
    match command {
        RequestStreamCommand::Register { owner, window, .. } => {
            if owner.is_zero() || *window == 0 {
                return Err(AccessError::InvalidRequest);
            }
            if *window > max_items {
                return Err(AccessError::Capacity);
            }
            Ok(1)
        }
        RequestStreamCommand::Acknowledge {
            stream,
            expected_revision,
            through,
            receipts,
        } => {
            scope(stream)?;
            if *expected_revision == 0 || *through == 0 || receipts.is_empty() {
                return Err(AccessError::InvalidRequest);
            }
            if receipts.len() > max_items as usize {
                return Err(AccessError::Capacity);
            }
            let mut previous = None;
            for receipt in receipts {
                if !receipt.key.is_valid()
                    || receipt.key.stream != *stream
                    || receipt.key.ordinal > *through
                    || previous.is_some_and(|ordinal: u64| {
                        ordinal.checked_add(1) != Some(receipt.key.ordinal)
                    })
                {
                    return Err(AccessError::InvalidRequest);
                }
                previous = Some(receipt.key.ordinal);
            }
            if previous != Some(*through) {
                return Err(AccessError::InvalidRequest);
            }
            Ok(receipts.len() as u64)
        }
        RequestStreamCommand::Seal {
            key,
            expected_revision,
            ..
        } => {
            scope(&key.stream)?;
            if !key.is_valid() || *expected_revision == 0 {
                return Err(AccessError::InvalidRequest);
            }
            Ok(1)
        }
        RequestStreamCommand::Close {
            stream,
            expected_revision,
            ..
        } => {
            scope(stream)?;
            if *expected_revision == 0 {
                return Err(AccessError::InvalidRequest);
            }
            Ok(1)
        }
    }
}

fn validate_token(
    request: &RequestEnvelope,
    route: RouteEpoch,
    token: ReadToken,
) -> Result<(), WireError> {
    if request.protocol != MANAGED_PROTOCOL_VERSION
        || request.request_epoch != RequestEpoch(1)
        || route.0 == 0
        || route != request.route_epoch
        || token.route_epoch != route
        || token.ledger != request.ledger
    {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
}
fn validate_state(
    state: &RequestStreamState,
    cluster: [u8; 16],
    ledger: LedgerId,
    principal: ParticipantId,
    slot: u32,
    limits: &WireLimits,
) -> Result<(), WireError> {
    match state {
        RequestStreamState::Vacant { slot: actual, .. } if *actual == slot => Ok(()),
        RequestStreamState::Active {
            stream,
            owner,
            revision,
            window,
            ..
        } if stream.is_valid()
            && stream.cluster == cluster
            && stream.ledger == ledger
            && stream.principal == principal
            && stream.slot == slot
            && !owner.is_zero()
            && *revision > 0
            && *window > 0
            && *window <= limits.max_items =>
        {
            Ok(())
        }
        _ => Err(WireError::InvalidFrame),
    }
}
fn family_of(receipt: &ManagedReceipt) -> ManagedRequestFamily {
    match &receipt.outcome {
        ManagedReceiptOutcome::Domain(_) => ManagedRequestFamily::Domain,
        ManagedReceiptOutcome::Cursor { .. } => ManagedRequestFamily::Cursor,
        ManagedReceiptOutcome::Sealed { family } => *family,
    }
}
pub(crate) fn validate_control_reply(
    request: &RequestEnvelope,
    route: RouteEpoch,
    reply: &RequestStreamControlReply,
    principal: Option<ParticipantId>,
    limits: &WireLimits,
) -> Result<(), WireError> {
    let Operation::RequestStreamControl { cluster, command } = &request.operation else {
        return Err(WireError::InvalidFrame);
    };
    validate_token(request, route, reply.token)?;
    let receipt = &reply.receipt;
    if receipt.cluster != *cluster
        || receipt.ledger != request.ledger
        || receipt.principal.is_zero()
        || principal.is_some_and(|p| p != receipt.principal)
        || receipt.id != request.request_id
        || receipt.raft_index == 0
    {
        return Err(WireError::InvalidFrame);
    }
    validate_control_request(
        *cluster,
        request.ledger,
        receipt.principal,
        command,
        limits.max_items,
    )?;
    // The exact control hash includes identity and every ACK manifest element.
    if request_stream_control_hash(*cluster, request.ledger, receipt.principal, command)
        .map_err(|_| WireError::InvalidFrame)?
        != receipt.intent_hash
    {
        return Err(WireError::InvalidFrame);
    }
    match (command, &receipt.outcome) {
        (
            RequestStreamCommand::Register {
                slot,
                expected_generation,
                owner,
                window,
            },
            RequestStreamControlOutcome::Registered(
                state @ RequestStreamState::Active {
                    stream,
                    owner: actual,
                    revision,
                    window: bound,
                    acknowledged_through,
                },
            ),
        ) => {
            validate_state(
                state,
                *cluster,
                request.ledger,
                receipt.principal,
                *slot,
                limits,
            )?;
            if expected_generation.checked_add(1) != Some(stream.generation)
                || owner != actual
                || window != bound
                || *revision != 1
                || *acknowledged_through != 0
            {
                return Err(WireError::InvalidFrame);
            }
        }
        (
            RequestStreamCommand::Acknowledge {
                stream,
                expected_revision,
                through,
                ..
            },
            RequestStreamControlOutcome::Acknowledged {
                stream: actual,
                revision,
                through: accepted,
            },
        ) if stream == actual
            && expected_revision.checked_add(1) == Some(*revision)
            && through == accepted => {}
        (
            RequestStreamCommand::Seal {
                key,
                family,
                intent_hash,
                ..
            },
            RequestStreamControlOutcome::Sealed(sealed),
        ) => {
            validate_managed_receipt(sealed, key, *family, *intent_hash, limits)?;
            if sealed.sequence > reply.token.sequence || sealed.raft_index > receipt.raft_index {
                return Err(WireError::InvalidFrame);
            }
        }
        (
            RequestStreamCommand::Close { stream, .. },
            RequestStreamControlOutcome::Closed {
                stream: actual,
                vacant_generation,
            },
        ) if stream == actual && *vacant_generation == stream.generation => {}
        _ => return Err(WireError::InvalidFrame),
    }
    Ok(())
}
pub(crate) fn validate_stream_read(
    request: &RequestEnvelope,
    route: RouteEpoch,
    reply: &RequestStreamReadReply,
    principal: Option<ParticipantId>,
    limits: &WireLimits,
) -> Result<(), WireError> {
    let Operation::RequestStreamRead { cluster, query } = &request.operation else {
        return Err(WireError::InvalidFrame);
    };
    validate_token(request, route, reply.token)?;
    let page = &reply.page;
    if *cluster == [0; 16]
        || page.schema != MANAGED_REQUEST_SCHEMA
        || page.cluster != *cluster
        || page.ledger != request.ledger
        || page.sequence != reply.token.sequence
        || page.raft_index == 0
        || page.principal.is_zero()
        || principal.is_some_and(|p| p != page.principal)
    {
        return Err(WireError::InvalidFrame);
    }
    match (query, &page.result) {
        (RequestStreamQuery::Slot { slot }, RequestStreamReadResult::Slot(state)) => {
            validate_state(state, *cluster, page.ledger, page.principal, *slot, limits)?
        }
        (
            RequestStreamQuery::Receipt { key },
            RequestStreamReadResult::Receipt {
                key: actual,
                state,
                resolution,
            },
        ) => {
            if !key.is_valid()
                || key != actual
                || key.stream.cluster != *cluster
                || key.stream.ledger != page.ledger
                || key.stream.principal != page.principal
            {
                return Err(WireError::InvalidFrame);
            }
            validate_state(
                state,
                *cluster,
                page.ledger,
                page.principal,
                key.stream.slot,
                limits,
            )?;
            match resolution {
                ManagedReceiptResolution::Retained(receipt) => {
                    if !matches!(state,RequestStreamState::Active{stream,acknowledged_through,..} if *stream==key.stream && key.ordinal>*acknowledged_through)
                        || receipt.sequence > page.sequence
                        || receipt.raft_index > page.raft_index
                    {
                        return Err(WireError::InvalidFrame);
                    }
                    validate_managed_receipt(
                        receipt,
                        key,
                        family_of(receipt),
                        receipt.intent_hash,
                        limits,
                    )?;
                }
                ManagedReceiptResolution::Retired { through } if matches!(state,RequestStreamState::Active{stream,acknowledged_through,..} if *stream==key.stream && through==acknowledged_through && key.ordinal<=*through) =>
                    {}
                ManagedReceiptResolution::StreamClosed { generation }
                    if *generation >= key.stream.generation
                        && match state {
                            RequestStreamState::Vacant {
                                generation: current,
                                ..
                            } => generation == current,
                            RequestStreamState::Active { stream, .. } => {
                                generation.checked_add(1) == Some(stream.generation)
                            }
                        } => {}
                ManagedReceiptResolution::Unknown
                    if match state {
                        RequestStreamState::Vacant { generation, .. } => {
                            key.stream.generation > *generation
                        }
                        RequestStreamState::Active {
                            stream,
                            acknowledged_through,
                            ..
                        } => {
                            key.stream.generation > stream.generation
                                || key.stream == *stream && key.ordinal > *acknowledged_through
                        }
                    } => {}
                _ => return Err(WireError::InvalidFrame),
            }
        }
        _ => return Err(WireError::InvalidFrame),
    }
    Ok(())
}
pub(crate) fn validate_support(
    request: &RequestEnvelope,
    route: RouteEpoch,
    fact: &ManagedFormatSupport,
    _limits: &WireLimits,
) -> Result<(), WireError> {
    let Operation::ManagedSupport { group } = request.operation else {
        return Err(WireError::InvalidFrame);
    };
    if fact.cluster == [0; 16]
        || fact.ledger != request.ledger
        || fact.group != group
        || group == [0; 16]
        || fact.node == 0
        || route != request.route_epoch
        || fact.voters.is_empty()
    {
        return Err(WireError::InvalidFrame);
    }
    let mut total = 0usize;
    for nodes in [
        &fact.voters,
        &fact.voters_outgoing,
        &fact.learners,
        &fact.learners_next,
    ] {
        if nodes.len() > 1024 {
            return Err(WireError::Limit);
        }
        total = total.checked_add(nodes.len()).ok_or(WireError::Limit)?;
        let mut previous = 0;
        for node in nodes {
            if *node <= previous {
                return Err(WireError::InvalidFrame);
            }
            previous = *node;
        }
    }
    if total > 4096 {
        return Err(WireError::Limit);
    }
    Ok(())
}

pub(crate) fn validate_managed_error(
    request: &RequestEnvelope,
    route: RouteEpoch,
    error: &AccessError,
) -> Result<(), WireError> {
    if (request.protocol != MANAGED_PROTOCOL_VERSION && !is_peer_request(request))
        || request.request_epoch != RequestEpoch(1)
        || route != request.route_epoch
    {
        return Err(WireError::InvalidFrame);
    }
    let (stream, key) = match &request.operation {
        Operation::Managed { key, .. } => (Some(key.stream), Some(*key)),
        Operation::RequestStreamControl {
            command: RequestStreamCommand::Seal { key, .. },
            ..
        } => (Some(key.stream), Some(*key)),
        Operation::RequestStreamControl {
            command:
                RequestStreamCommand::Acknowledge { stream, .. }
                | RequestStreamCommand::Close { stream, .. },
            ..
        } => (Some(*stream), None),
        Operation::RequestStreamControl {
            command: RequestStreamCommand::Register { .. },
            ..
        } => (None, None),
        _ => return Err(WireError::InvalidFrame),
    };
    match error {
        AccessError::ManagedRetired { through }
            if key.is_some_and(|key| key.is_valid() && key.ordinal <= *through) =>
        {
            Ok(())
        }
        AccessError::ManagedClosed { generation }
            if stream
                .is_some_and(|stream| stream.is_valid() && stream.generation <= *generation) =>
        {
            Ok(())
        }
        AccessError::ManagedConflict | AccessError::ManagedNotRegistered => Ok(()),
        _ => Err(WireError::InvalidFrame),
    }
}
