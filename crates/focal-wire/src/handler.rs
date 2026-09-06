use crate::*;
use focal_memory::Allocation;
use focal_model::*;
use std::{future::Future, pin::Pin};

pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = ResponseEnvelope> + Send + 'a>>;
pub type OwnedHandlerFuture<'a> = Pin<Box<dyn Future<Output = OwnedResponse> + Send + 'a>>;

/// An owned reply and its resident/encoding allowance. Network adapters retain
/// this value until delivery completes or the stream is cancelled.
pub struct OwnedResponse {
    envelope: ResponseEnvelope,
    _allocation: Option<Allocation>,
    _additional: Option<Allocation>,
}
impl OwnedResponse {
    pub fn new(envelope: ResponseEnvelope) -> Self {
        Self {
            envelope,
            _allocation: None,
            _additional: None,
        }
    }
    pub fn accounted(envelope: ResponseEnvelope, allocation: Allocation) -> Self {
        Self {
            envelope,
            _allocation: Some(allocation),
            _additional: None,
        }
    }
    /// Bounded pair for owners with separate input/response reservations.
    pub fn accounted_pair(
        envelope: ResponseEnvelope,
        allocation: Allocation,
        additional: Option<Allocation>,
    ) -> Self {
        Self {
            envelope,
            _allocation: Some(allocation),
            _additional: additional,
        }
    }
    pub fn envelope(&self) -> &ResponseEnvelope {
        &self.envelope
    }
    /// Compatibility for direct callers that own their response budget. This
    /// explicitly releases the allowance; network adapters must keep OwnedResponse.
    pub fn into_envelope(self) -> ResponseEnvelope {
        self.envelope
    }
}
pub trait RequestHandler: Send + Sync + 'static {
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_>;
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        Box::pin(async move { OwnedResponse::new(self.handle(request).await) })
    }
}
impl<F, Fut> RequestHandler for F
where
    F: Fn(VerifiedRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ResponseEnvelope> + Send + 'static,
{
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_> {
        Box::pin(self(request))
    }
}

// Compatibility for callers that intentionally share a dynamically dispatched
// handler. Concrete actor handles pass directly to servers without allocation.
impl RequestHandler for std::sync::Arc<dyn RequestHandler> {
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_> {
        self.as_ref().handle(request)
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        self.as_ref().handle_accounted(request)
    }
}

/// Shared by QUIC, Unix, and embedded ingress. The handler gets only verified
/// capabilities and cannot return a response under an unrelated request identity.
pub async fn dispatch(
    handler: &dyn RequestHandler,
    peer: AuthenticatedPeer,
    request: RequestEnvelope,
    limits: &WireLimits,
) -> ResponseEnvelope {
    dispatch_accounted(handler, peer, request, limits)
        .await
        .into_envelope()
}

/// Authenticated dispatch retaining the handler's allowance through transport.
pub async fn dispatch_accounted(
    handler: &dyn RequestHandler,
    peer: AuthenticatedPeer,
    request: RequestEnvelope,
    limits: &WireLimits,
) -> OwnedResponse {
    if let Err(error) = limits.validate() {
        return OwnedResponse::new(request.reply(Response::Error(error)));
    }
    if require_runtime().is_err() {
        return OwnedResponse::new(request.reply(Response::Error(AccessError::Unavailable)));
    }
    let verified = match verify_request(peer.clone(), request.clone(), limits) {
        Ok(value) => value,
        Err(error) => return OwnedResponse::new(request.reply(Response::Error(error))),
    };
    let response = match tokio::time::timeout(
        limits.request_timeout,
        handler.handle_accounted(verified),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => {
            return OwnedResponse::new(request.reply(Response::Error(
                if request.operation.is_mutation() {
                    AccessError::OutcomeUnknown
                } else {
                    AccessError::Unavailable
                },
            )));
        }
    };
    if let (Operation::Stream(stream), Response::Stream(reply)) =
        (&request.operation, &response.envelope().result)
        && stream_scope(&peer, request.ledger, stream.filter()).ok() != Some(reply.cursor.scope)
    {
        return OwnedResponse::new(request.reply(Response::Error(AccessError::OutcomeUnknown)));
    }
    if validate_response(
        &request,
        response.envelope(),
        Some(peer.principal()),
        limits,
    )
    .is_err()
    {
        return OwnedResponse::new(request.reply(Response::Error(
            if request.operation.is_mutation() {
                AccessError::OutcomeUnknown
            } else {
                AccessError::InvalidRequest
            },
        )));
    }
    if postcard::experimental::serialized_size(response.envelope())
        .ok()
        .is_none_or(|n| n > limits.max_frame_bytes as usize)
    {
        return OwnedResponse::new(request.reply(Response::Error(
            if request.operation.is_mutation() {
                AccessError::OutcomeUnknown
            } else {
                AccessError::Capacity
            },
        )));
    }
    response
}

pub fn validate_response(
    request: &RequestEnvelope,
    response: &ResponseEnvelope,
    principal: Option<ParticipantId>,
    limits: &WireLimits,
) -> Result<(), WireError> {
    if response.protocol != request.protocol
        || response.ledger != request.ledger
        || response.request_epoch != request.request_epoch
        || response.request_id != request.request_id
    {
        return Err(WireError::InvalidFrame);
    }
    let receipt_valid = |receipt: &MutationReceipt| {
        receipt.ledger == request.ledger
            && receipt.key.epoch == request.request_epoch
            && receipt.key.id == request.request_id
            && principal.is_none_or(|p| receipt.key.principal == p)
    };
    match &response.result {
        Response::Control { response } => {
            if !matches!(
                request.operation,
                Operation::Control { .. }
                    | Operation::PeerControl { .. }
                    | Operation::NodeContact { .. }
                    | Operation::EnrollmentControl { .. }
            ) || response.is_empty()
            {
                return Err(WireError::InvalidFrame);
            }
        }
        Response::Submitted(reply) => {
            if !matches!(
                request.operation,
                Operation::Submit { .. } | Operation::OpenEpoch { .. }
            ) {
                return Err(WireError::InvalidFrame);
            }
            match reply {
                MutationReply::Committed(receipt) if !receipt_valid(receipt) => {
                    return Err(WireError::InvalidFrame);
                }
                MutationReply::Domain(DomainOutcome::Duplicate(receipt))
                    if !receipt_valid(receipt) =>
                {
                    return Err(WireError::InvalidFrame);
                }
                MutationReply::Yield { observed, .. } if observed.ledger != request.ledger => {
                    return Err(WireError::InvalidFrame);
                }
                MutationReply::Pending(key)
                    if key.epoch != request.request_epoch
                        || key.id != request.request_id
                        || principal.is_some_and(|p| p != key.principal) =>
                {
                    return Err(WireError::InvalidFrame);
                }
                _ => {}
            }
        }
        Response::Read(page) => {
            let Operation::Read(read) = &request.operation else {
                return Err(WireError::InvalidFrame);
            };
            validate_page(page, request.ledger, read.max_items.min(limits.max_items))?;
            match &read.consistency {
                ReadConsistency::Exact(token) if page.token != *token => {
                    return Err(WireError::InvalidFrame);
                }
                ReadConsistency::AtLeast(token) if page.token.sequence < token.sequence => {
                    return Err(WireError::InvalidFrame);
                }
                _ => {}
            }
        }
        Response::Subscription(batch) => {
            let Operation::Subscribe(sub) = &request.operation else {
                return Err(WireError::InvalidFrame);
            };
            if batch.token.ledger != request.ledger
                || batch.deltas.len() > sub.credits.items as usize
                || batch
                    .deltas
                    .iter()
                    .any(|d| d.id.ledger != request.ledger || d.id.sequence > batch.token.sequence)
            {
                return Err(WireError::InvalidFrame);
            }
            if let Some(seed) = &batch.seed {
                validate_page(seed, request.ledger, sub.credits.items)?;
                if seed.token != batch.token || !sub.seed {
                    return Err(WireError::InvalidFrame);
                }
            }
            let mut last = sub.after;
            for delta in &batch.deltas {
                if last.is_some_and(|cursor| delta.id <= cursor) {
                    return Err(WireError::InvalidFrame);
                }
                last = Some(delta.id);
            }
            if batch.next != last || encode_payload(batch, sub.credits.bytes).is_err() {
                return Err(WireError::Limit);
            }
        }
        Response::PeerAccepted if !matches!(request.operation, Operation::Raft { .. }) => {
            return Err(WireError::InvalidFrame);
        }
        Response::Stream(reply) => {
            let Operation::Stream(stream) = &request.operation else {
                return Err(WireError::InvalidFrame);
            };
            if reply.token.ledger != request.ledger
                || reply.cursor.key.ledger != request.ledger
                || reply.cursor.position.ledger != request.ledger
                || reply.cursor.position.sequence > reply.token.sequence
                || reply.cursor.same_stream(reply.acknowledged).is_err()
                || reply.acknowledged.position > reply.cursor.position
            {
                return Err(WireError::InvalidFrame);
            }
            let expected = match stream {
                StreamRequest::Open { consumer, .. } => {
                    if reply.cursor.key.consumer != *consumer {
                        return Err(WireError::InvalidFrame);
                    }
                    None
                }
                StreamRequest::Poll { cursor, .. } | StreamRequest::CompleteSeed { cursor, .. } => {
                    Some(*cursor)
                }
            };
            if expected.is_some_and(|cursor| {
                cursor.same_stream(reply.cursor).is_err() || cursor.position > reply.cursor.position
            }) {
                return Err(WireError::InvalidFrame);
            }
            let mut count = 0u32;
            let mut bytes = 0usize;
            let mut position = expected.map(|c| c.position);
            for event in &reply.events {
                let cursor = match event {
                    StreamEvent::Delta { cursor, delta } => {
                        count = count.saturating_add(1);
                        bytes = bytes
                            .saturating_add(encode_payload(event, limits.max_frame_bytes)?.len());
                        if delta.id.ledger != request.ledger
                            || cursor.position != Position::after_delta(delta.id)
                        {
                            return Err(WireError::InvalidFrame);
                        }
                        *cursor
                    }
                    StreamEvent::Resolved { cursor } => {
                        if cursor.position.offset != PositionOffset::Resolved {
                            return Err(WireError::InvalidFrame);
                        }
                        *cursor
                    }
                    StreamEvent::Resync { cursor, .. } => *cursor,
                };
                if cursor.same_stream(reply.cursor).is_err()
                    || cursor.position > reply.cursor.position
                    || position.is_some_and(|p| cursor.position < p)
                {
                    return Err(WireError::InvalidFrame);
                }
                position = Some(cursor.position);
            }
            let credits = stream.credits();
            if count > credits.items
                || bytes > credits.bytes as usize
                || reply.events.len() > (limits.max_items as usize).saturating_add(1)
            {
                return Err(WireError::Limit);
            }
            if let Some(seed) = &reply.seed {
                validate_page(seed, request.ledger, credits.items)?;
            }
        }
        Response::Upload(reply) => {
            let valid = match (&request.operation, reply) {
                (
                    Operation::Upload(UploadRequest::Begin { length, .. }),
                    UploadReply::Offset(offset),
                ) => offset <= length,
                (
                    Operation::Upload(UploadRequest::Append { offset, bytes, .. }),
                    UploadReply::Offset(received),
                ) => offset
                    .checked_add(bytes.len() as u64)
                    .is_some_and(|end| *received >= end),
                (Operation::Upload(UploadRequest::Seal { .. }), UploadReply::Sealed(content)) => {
                    !content.domain.is_zero()
                }
                (Operation::Upload(UploadRequest::Cancel { .. }), UploadReply::Cancelled) => true,
                _ => false,
            };
            if !valid {
                return Err(WireError::InvalidFrame);
            }
        }
        Response::Custody(reply) => {
            let Operation::Custody(operation) = &request.operation else {
                return Err(WireError::InvalidFrame);
            };
            let valid = match (operation, reply) {
                (
                    CustodyRequest::Open { .. },
                    CustodyReply::Opened {
                        chunks,
                        next_missing,
                    },
                ) => next_missing <= chunks,
                (
                    CustodyRequest::Chunk { index, .. },
                    CustodyReply::ChunkStored { index: stored },
                ) => index == stored,
                (
                    CustodyRequest::Seal { .. },
                    CustodyReply::Durable {
                        policy_revision,
                        content,
                    },
                ) => {
                    *policy_revision > 0
                        && content.domain == ContentDomainId(request.ledger.tenant.0)
                }
                (
                    CustodyRequest::Verify {
                        policy_revision,
                        content,
                    },
                    CustodyReply::Durable {
                        policy_revision: accepted,
                        content: retained,
                    },
                ) => policy_revision == accepted && content == retained,
                (CustodyRequest::Cancel { .. }, CustodyReply::Cancelled) => true,
                (
                    CustodyRequest::Manifest {
                        content, max_bytes, ..
                    },
                    CustodyReply::Manifest {
                        content: described,
                        manifest,
                    },
                ) => {
                    content == described
                        && !manifest.is_empty()
                        && manifest.len() <= *max_bytes as usize
                }
                (
                    CustodyRequest::ReadChunk {
                        index, max_bytes, ..
                    },
                    CustodyReply::Chunk { index: read, bytes },
                ) => index == read && !bytes.is_empty() && bytes.len() <= *max_bytes as usize,
                _ => false,
            };
            if !valid {
                return Err(WireError::InvalidFrame);
            }
        }
        Response::Content(chunk) => {
            let Operation::Download {
                content,
                offset,
                max_bytes,
            } = &request.operation
            else {
                return Err(WireError::InvalidFrame);
            };
            let end = chunk
                .offset
                .checked_add(chunk.bytes.len() as u64)
                .ok_or(WireError::InvalidFrame)?;
            if chunk.offset != *offset
                || chunk.bytes.len() > *max_bytes as usize
                || end > content.length
                || chunk.eof != (end == content.length)
                || (chunk.bytes.is_empty() && !chunk.eof)
            {
                return Err(WireError::InvalidFrame);
            }
        }
        Response::Error(AccessError::RouteChanged(route))
            if route.endpoint.len() > 512
                || route.server_name.len() > 253
                || route.epoch < request.route_epoch =>
        {
            return Err(WireError::InvalidFrame);
        }
        Response::Error(AccessError::ResyncRequired { floor: Some(floor) })
            if floor.ledger != request.ledger =>
        {
            return Err(WireError::InvalidFrame);
        }
        _ => {}
    }
    Ok(())
}
fn validate_page(page: &ReadPage, ledger: LedgerId, max_items: u32) -> Result<(), WireError> {
    if page.token.ledger != ledger || page.objects.len() > max_items as usize {
        return Err(WireError::InvalidFrame);
    }
    for object in &page.objects {
        let object_ledger = match object {
            ReadObject::Claim { value, .. } => value.content().ledger,
            ReadObject::Testament { value, .. } => value.content().ledger,
            ReadObject::Validation { value, .. } => value.content().ledger,
            ReadObject::Artifact { value, .. } => value.content().ledger,
        };
        if object_ledger != ledger {
            return Err(WireError::InvalidFrame);
        }
    }
    Ok(())
}
