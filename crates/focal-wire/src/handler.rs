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
    /// Local implementation capability only. Group activation remains a
    /// committed, configuration-fenced decision in the authoritative owner.
    fn supports_managed_requests(&self) -> bool {
        false
    }
    /// The handler performs command-specific participant standing checks.
    fn supports_participant_requests(&self) -> bool {
        false
    }
    /// The handler admits borrowed native frames, native reads and lists on the
    /// native profile. Advertising it without an active native engine is a
    /// handler bug; activation itself stays a committed owner decision.
    fn supports_native_requests(&self) -> bool {
        false
    }
    fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a>;
    fn handle_accounted<'a>(&'a self, request: &'a VerifiedRequest) -> OwnedHandlerFuture<'a> {
        Box::pin(async move { OwnedResponse::new(self.handle(request).await) })
    }
}
impl<F, Fut> RequestHandler for F
where
    F: Fn(VerifiedRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ResponseEnvelope> + Send + 'static,
{
    // Closure handlers (test scaffolding and compatibility callers) take an owned
    // `VerifiedRequest`, so this bridge clones it. Production handlers implement
    // the trait directly on `&VerifiedRequest` and never clone here — the hot
    // dispatch path is clone-free.
    fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
        Box::pin(self(request.clone()))
    }
}

// Compatibility for callers that intentionally share a dynamically dispatched
// handler. Concrete actor handles pass directly to servers without allocation.
impl RequestHandler for std::sync::Arc<dyn RequestHandler> {
    fn supports_participant_requests(&self) -> bool {
        self.as_ref().supports_participant_requests()
    }
    fn supports_managed_requests(&self) -> bool {
        self.as_ref().supports_managed_requests()
    }
    fn supports_native_requests(&self) -> bool {
        self.as_ref().supports_native_requests()
    }
    fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
        self.as_ref().handle(request)
    }
    fn handle_accounted<'a>(&'a self, request: &'a VerifiedRequest) -> OwnedHandlerFuture<'a> {
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
    // `verify_request` consumes `peer` and `request` into `VerifiedRequest`; the
    // whole envelope and peer are no longer cloned per request. Capture the Copy
    // reply identity first so a verification failure can still answer under the
    // request's identity without retaining the moved envelope.
    let (protocol, ledger, route_epoch, request_epoch, request_id) = (
        request.protocol,
        request.ledger,
        request.route_epoch,
        request.request_epoch,
        request.request_id,
    );
    let verified = match verify_request(peer, request, limits) {
        Ok(value) => value,
        Err(error) => {
            return OwnedResponse::new(ResponseEnvelope {
                protocol,
                ledger,
                route_epoch,
                request_epoch,
                request_id,
                result: Response::Error(error),
            });
        }
    };
    if (verified.request().protocol == PEER_PROTOCOL_VERSION
        && (!handler.supports_managed_requests() || !handler.supports_participant_requests()))
        || (verified.request().protocol == crate::NATIVE_PROTOCOL_VERSION
            && (!handler.supports_managed_requests()
                || !handler.supports_participant_requests()
                || !handler.supports_native_requests()))
    {
        return OwnedResponse::new(
            verified
                .request()
                .reply(Response::Error(AccessError::UnsupportedProtocol)),
        );
    }
    let response =
        match tokio::time::timeout(limits.request_timeout, handler.handle_accounted(&verified))
            .await
        {
            Ok(response) => response,
            Err(_) => {
                return OwnedResponse::new(verified.request().reply(Response::Error(
                    if verified.request().operation.is_mutation() {
                        AccessError::OutcomeUnknown
                    } else {
                        AccessError::Unavailable
                    },
                )));
            }
        };
    let delivery = match (&verified.request().operation, &response.envelope().result) {
        (Operation::Stream(stream), Response::Stream(reply)) => Some((stream, reply)),
        (
            Operation::Managed {
                operation: ManagedOperation::Cursor(stream),
                ..
            },
            Response::Managed(ManagedReply {
                stream: Some(reply),
                ..
            }),
        ) => Some((stream, reply)),
        _ => None,
    };
    if let Some((stream, reply)) = delivery
        && stream_scope(verified.peer(), verified.request().ledger, stream.filter()).ok()
            != Some(reply.cursor.scope)
    {
        return OwnedResponse::new(
            verified
                .request()
                .reply(Response::Error(AccessError::OutcomeUnknown)),
        );
    }
    if validate_response(
        verified.request(),
        response.envelope(),
        Some(verified.peer().principal()),
        limits,
    )
    .is_err()
    {
        return OwnedResponse::new(verified.request().reply(Response::Error(
            if verified.request().operation.is_mutation() {
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
        return OwnedResponse::new(verified.request().reply(Response::Error(
            if verified.request().operation.is_mutation() {
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
        Response::Managed(reply) => crate::managed::validate_managed_reply(
            request,
            response.route_epoch,
            reply,
            principal,
            limits,
        )?,
        Response::RequestStreamControlled(reply) => crate::managed::validate_control_reply(
            request,
            response.route_epoch,
            reply,
            principal,
            limits,
        )?,
        Response::RequestStreamRead(reply) => crate::managed::validate_stream_read(
            request,
            response.route_epoch,
            reply,
            principal,
            limits,
        )?,
        Response::ManagedSupport(fact) => {
            crate::managed::validate_support(request, response.route_epoch, fact, limits)?
        }
        Response::Reconciled(reply) => crate::reconcile::validate_reconciliation(
            request,
            response.route_epoch,
            reply,
            principal,
            limits,
        )?,
        Response::Control { response } => {
            if !matches!(
                request.operation,
                Operation::Control { .. }
                    | Operation::PeerControl { .. }
                    | Operation::PlacementControl { .. }
                    | Operation::SessionSign { .. }
                    | Operation::RangeControl { .. }
                    | Operation::SessionControl { .. }
                    | Operation::NodeContact { .. }
                    | Operation::EnrollmentControl { .. }
            ) || response.is_empty()
            {
                return Err(WireError::InvalidFrame);
            }
        }
        Response::Submitted(MutationReply::Domain(
            DomainOutcome::Refuse { .. } | DomainOutcome::Inform { .. },
        )) if matches!(
            request.operation,
            Operation::Managed {
                operation: ManagedOperation::Submit { .. },
                ..
            }
        ) =>
        {
            let (key, family, _) = crate::managed::managed_request_identity(request)?;
            if family != ManagedRequestFamily::Domain
                || response.route_epoch != request.route_epoch
                || principal.is_some_and(|principal| principal != key.stream.principal)
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
        Response::Monitor(value) => crate::monitor::validate(request, response, value, limits)?,
        Response::Summary(value) => crate::summary::validate(request, response, value)?,
        Response::Traversed(page) => {
            let Operation::Traverse(query) = &request.operation else {
                return Err(WireError::InvalidFrame);
            };
            if page.token.ledger != request.ledger
                || page.token.route_epoch != response.route_epoch
                || page.objects.len() > query.max_items.min(limits.max_items) as usize
                || page.visited > query.max_visits
                || page.total_visits > query.max_edges
                || page.visited > page.total_visits
                || page.next.is_some() != (page.stop == TraversalStop::PageLimit)
                || page.next.as_ref().is_some_and(|c| {
                    c.bytes.is_empty() || c.bytes.len() > MAX_TRAVERSAL_CURSOR_BYTES
                })
                || page
                    .objects
                    .iter()
                    .any(|o| matches!(o, ReadObject::ValidationResults { .. }))
            {
                return Err(WireError::InvalidFrame);
            }
            if postcard::experimental::serialized_size(page).map_err(|_| WireError::InvalidFrame)?
                > query.max_bytes as usize
            {
                return Err(WireError::InvalidFrame);
            }
            validate_objects(&page.objects, request.ledger)?;
        }
        Response::Listed(page) | Response::Validators(page) => {
            let list = match (&request.operation, &response.result) {
                (Operation::List(list), Response::Listed(_)) => list,
                (Operation::Select(query), Response::Listed(_)) => {
                    if page.token.route_epoch != request.route_epoch
                        || page
                            .objects
                            .iter()
                            .any(|object| !query.matches_at(object, page.token.sequence))
                    {
                        return Err(WireError::InvalidFrame);
                    }
                    &query.query
                }
                (Operation::Validators(query), Response::Validators(_)) => {
                    if page.objects.iter().any(|object| !matches!(object, ReadObject::Validation { value, .. } if query.matches(value.content()))) {
                        return Err(WireError::InvalidFrame);
                    }
                    &query.query
                }
                _ => return Err(WireError::InvalidFrame),
            };
            if page.token.ledger != request.ledger
                || page.token.route_epoch != response.route_epoch
                || page.objects.len() > list.max_items.min(limits.max_items) as usize
                || page.visited > list.max_visits
                || page.objects.len() > page.visited as usize
                || page.next.as_ref().is_some_and(|cursor| {
                    cursor.bytes.is_empty() || cursor.bytes.len() > MAX_LIST_CURSOR_BYTES
                })
                || (page.next.is_some() && page.visited == 0)
            {
                return Err(WireError::InvalidFrame);
            }
            validate_objects(&page.objects, request.ledger)?;
            if page.objects.iter().any(|object| match object {
                ReadObject::Claim { .. } => list.filter.kind != ObjectKind::Claim,
                ReadObject::Testament { .. } => list.filter.kind != ObjectKind::Testament,
                ReadObject::Validation { .. } => list.filter.kind != ObjectKind::Validation,
                ReadObject::Artifact { .. } => list.filter.kind != ObjectKind::Artifact,
                ReadObject::ValidationResults { .. } => true,
            }) {
                return Err(WireError::InvalidFrame);
            }
        }
        Response::Native(reply) => {
            if !matches!(request.operation, Operation::Native { .. }) {
                return Err(WireError::InvalidFrame);
            }
            let key_valid = |key: &RequestKey| {
                key.epoch == request.request_epoch
                    && key.id == request.request_id
                    && principal.is_none_or(|p| key.principal == p)
            };
            match reply {
                NativeMutationReply::Committed(receipt) => {
                    let NativeInvocationRef::Request(key) = &receipt.invocation else {
                        return Err(WireError::InvalidFrame);
                    };
                    if !key_valid(key) || receipt.sequence.0 == 0 {
                        return Err(WireError::InvalidFrame);
                    }
                }
                NativeMutationReply::Pending(ticket) => {
                    if !key_valid(&ticket.key) {
                        return Err(WireError::InvalidFrame);
                    }
                }
                NativeMutationReply::Refused(refusal) => {
                    if refusal.detail.len() > 4096 {
                        return Err(WireError::InvalidFrame);
                    }
                }
            }
        }
        Response::NativeRead(page) => {
            let Operation::NativeRead(read) = &request.operation else {
                return Err(WireError::InvalidFrame);
            };
            let expected = match &read.query {
                NativeReadQuery::Objects(references) => Some(references.len()),
                NativeReadQuery::Outcome(_)
                | NativeReadQuery::Receipt(_)
                | NativeReadQuery::Monitor { .. }
                | NativeReadQuery::Standing => Some(1),
                _ => None,
            };
            if page.token.ledger != request.ledger
                || page.token.route_epoch != response.route_epoch
                || page.token.sequence != page.native_sequence
                || page.objects.len() > read.max_items.min(limits.max_items) as usize
                || expected.is_some_and(|count| page.objects.len() != count)
                || (page.visited as usize) < page.objects.len()
            {
                return Err(WireError::InvalidFrame);
            }
        }
        Response::NativeListed(page) => {
            let Operation::NativeList(list) = &request.operation else {
                return Err(WireError::InvalidFrame);
            };
            if page.token.ledger != request.ledger
                || page.token.route_epoch != response.route_epoch
                || page.token.sequence != page.native_sequence
                || page.objects.len() > list.max_items.min(limits.max_items) as usize
                || page.visited > list.max_visits
                || (page.visited as usize) < page.objects.len()
                || page.next.as_ref().is_some_and(|cursor| {
                    cursor.0.is_empty() || cursor.0.len() > MAX_NATIVE_LIST_CURSOR_BYTES
                })
                || (page.next.is_some() && page.visited == 0)
            {
                return Err(WireError::InvalidFrame);
            }
        }
        Response::Read(page) => {
            let Operation::Read(read) = &request.operation else {
                return Err(WireError::InvalidFrame);
            };
            if page.token.route_epoch != response.route_epoch {
                return Err(WireError::InvalidFrame);
            }
            validate_page(
                page,
                request.ledger,
                read.max_items.min(limits.max_items),
                matches!(read.query, ReadQuery::ValidationResults { .. }),
            )?;
            match &read.query {
                ReadQuery::SeedScan {
                    after,
                    claims,
                    max_bytes,
                } => {
                    if postcard::experimental::serialized_size(page)
                        .map_err(|_| WireError::InvalidFrame)?
                        > *max_bytes as usize
                    {
                        return Err(WireError::InvalidFrame);
                    }
                    validate_seed_selection(page, *after, claims)?;
                }
                ReadQuery::Objects(references) => {
                    if references.len() > read.max_items.min(limits.max_items) as usize {
                        return Err(WireError::InvalidFrame);
                    }
                    validate_object_selection(page, references, request.ledger)?;
                }
                ReadQuery::ValidationResults { id, after } => {
                    if page.next.is_some() || page.objects.len() > 1 {
                        return Err(WireError::InvalidFrame);
                    }
                    if let Some(object) = page.objects.first() {
                        let ReadObject::ValidationResults {
                            id: actual,
                            value,
                            records,
                            next,
                        } = object
                        else {
                            return Err(WireError::InvalidFrame);
                        };
                        if actual != id
                            || records.len() > read.max_items.min(limits.max_items) as usize
                            || next.is_some_and(|position| {
                                records.last().is_none_or(|last| last.position != position)
                            })
                        {
                            return Err(WireError::InvalidFrame);
                        }
                        let mut previous = *after;
                        let mut header: Option<&ValidationRunSummary> = None;
                        let mut prior_attempt: Option<(&VerdictRecord, usize)> = None;
                        for result in records {
                            if result.position.run.validation != *id
                                || result.position.run.phase != value.content().phase
                                || result.position.run.epoch == 0
                                || result.position.run.epoch > value.lifecycle().latest_epoch
                                || previous.is_some_and(|prior| prior >= result.position)
                            {
                                return Err(WireError::InvalidFrame);
                            }
                            if let Some(prior) =
                                previous.filter(|prior| prior.run == result.position.run)
                            {
                                let expected = match prior.attempt {
                                    None => Some(0),
                                    Some(ordinal) => ordinal.checked_add(1),
                                };
                                if result.position.attempt != expected {
                                    return Err(WireError::InvalidFrame);
                                }
                            } else if result.position.attempt.is_some() {
                                return Err(WireError::InvalidFrame);
                            }
                            match (&result.value, result.position.attempt) {
                                (ValidationResultValue::Run(run), None)
                                    if run.id == result.position.run
                                        && run.claim == value.content().claim
                                        && run.evaluator == value.content().evaluator =>
                                {
                                    validate_run_summary(value.content(), run)?;
                                    header = Some(run);
                                    prior_attempt = None;
                                }
                                (ValidationResultValue::Attempt(attempt), Some(ordinal))
                                    if attempt.run == result.position.run
                                        && attempt.evaluator == value.content().evaluator =>
                                {
                                    let index =
                                        validate_attempt(value.content(), attempt, ordinal)?;
                                    if let Some(run) = header.filter(|run| run.id == attempt.run)
                                        && (attempt.manifest != run.manifest
                                            || ordinal >= run.attempt_count
                                            || (run.final_verdict.is_some()
                                                && ordinal.checked_add(1)
                                                    == Some(run.attempt_count)
                                                && (run.final_verdict != Some(attempt.value)
                                                    || index != run.handler_index as usize)))
                                    {
                                        return Err(WireError::InvalidFrame);
                                    }
                                    if let Some((prior, prior_index)) =
                                        prior_attempt.filter(|(prior, _)| prior.run == attempt.run)
                                        && (prior.manifest != attempt.manifest
                                            || next_handler(value.content(), prior, prior_index)
                                                != Some(index))
                                    {
                                        return Err(WireError::InvalidFrame);
                                    }
                                    prior_attempt = Some((attempt, index));
                                }
                                _ => return Err(WireError::InvalidFrame),
                            }
                            previous = Some(result.position);
                        }
                    }
                }
                _ if page
                    .objects
                    .iter()
                    .any(|object| matches!(object, ReadObject::ValidationResults { .. })) =>
                {
                    return Err(WireError::InvalidFrame);
                }
                _ => {}
            }
            match &read.consistency {
                ReadConsistency::Exact(token) if page.token != *token => {
                    return Err(WireError::InvalidFrame);
                }
                ReadConsistency::AtLeast(token)
                    if token.ledger != request.ledger || page.token.sequence < token.sequence =>
                {
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
                validate_page(seed, request.ledger, sub.credits.items, false)?;
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
            if batch.next != last || payload_len(batch, sub.credits.bytes).is_err() {
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
            validate_stream_response(stream, reply, request.ledger, response.route_epoch, limits)?;
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
                (
                    CustodyRequest::SeedChunk { hash, max_bytes },
                    CustodyReply::SeedChunk { hash: read, bytes },
                ) => hash == read && !bytes.is_empty() && bytes.len() <= *max_bytes as usize,
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
        Response::Error(
            error @ (AccessError::ManagedRetired { .. }
            | AccessError::ManagedClosed { .. }
            | AccessError::ManagedConflict
            | AccessError::ManagedNotRegistered),
        ) => crate::managed::validate_managed_error(request, response.route_epoch, error)?,
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
fn validate_object_selection(
    page: &ReadPage,
    references: &[ObjectRef],
    ledger: LedgerId,
) -> Result<(), WireError> {
    if page.next.is_some()
        || references
            .iter()
            .any(|reference| reference.ledger != ledger)
    {
        return Err(WireError::InvalidFrame);
    }
    // The owner visits references in request order and omits missing objects.
    // Consume that same ordered subsequence without a set or extra allocation.
    // An explicitly repeated request reference may return a repeated object;
    // unsolicited duplicates, reordering and foreign keys cannot match.
    let mut remaining = references.iter();
    for object in &page.objects {
        let (kind, id) = match object {
            ReadObject::Claim { id, .. } => (ObjectKind::Claim, ObjectId(id.0)),
            ReadObject::Testament { id, .. } => (ObjectKind::Testament, ObjectId(id.0)),
            ReadObject::Artifact { id, .. } => (ObjectKind::Artifact, ObjectId(id.0)),
            ReadObject::Validation { id, .. } => (ObjectKind::Validation, ObjectId(id.0)),
            ReadObject::ValidationResults { .. } => return Err(WireError::InvalidFrame),
        };
        if !remaining.any(|reference| reference.kind == kind && reference.id == id) {
            return Err(WireError::InvalidFrame);
        }
    }
    Ok(())
}
fn validate_page(
    page: &ReadPage,
    ledger: LedgerId,
    max_items: u32,
    validation_results: bool,
) -> Result<(), WireError> {
    if page.token.ledger != ledger || page.objects.len() > max_items as usize {
        return Err(WireError::InvalidFrame);
    }
    if !validation_results
        && page
            .objects
            .iter()
            .any(|object| matches!(object, ReadObject::ValidationResults { .. }))
    {
        return Err(WireError::InvalidFrame);
    }
    validate_objects(&page.objects, ledger)
}
fn validate_objects(objects: &[ReadObject], ledger: LedgerId) -> Result<(), WireError> {
    for object in objects {
        let object_ledger = match object {
            ReadObject::Claim { value, .. } => value.content().ledger,
            ReadObject::Testament { value, .. } => value.content().ledger,
            ReadObject::Validation { value, .. } => value.content().ledger,
            ReadObject::Artifact { value, .. } => value.content().ledger,
            ReadObject::ValidationResults { value, .. } => value.content().ledger,
        };
        if object_ledger != ledger {
            return Err(WireError::InvalidFrame);
        }
    }
    Ok(())
}

fn validate_seed_selection(
    page: &ReadPage,
    after: Option<ObjectKey>,
    claims: &[ClaimId],
) -> Result<(), WireError> {
    let mut previous = after;
    for object in &page.objects {
        let (kind, id, claim) = match object {
            ReadObject::Claim { id, .. } => (ObjectKind::Claim, id.0, Some(*id)),
            ReadObject::Testament { id, value } => {
                (ObjectKind::Testament, id.0, Some(value.content().claim))
            }
            ReadObject::Validation { id, value } => {
                (ObjectKind::Validation, id.0, Some(value.content().claim))
            }
            ReadObject::Artifact { id, .. } => (ObjectKind::Artifact, id.0, None),
            ReadObject::ValidationResults { .. } => return Err(WireError::InvalidFrame),
        };
        let key = ObjectKey {
            kind,
            id: ObjectId(id),
        };
        if previous.is_some_and(|previous| previous >= key)
            || (!claims.is_empty()
                && claim.is_some_and(|claim| claims.binary_search(&claim).is_err()))
        {
            return Err(WireError::InvalidFrame);
        }
        previous = Some(key);
    }
    if page.next.is_some_and(|next| {
        after.is_some_and(|after| next <= after) || previous.is_some_and(|previous| next < previous)
    }) {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
}

fn validate_run_summary(
    spec: &ValidationContent,
    run: &ValidationRunSummary,
) -> Result<(), WireError> {
    if spec.kind == ValidationKind::Receipt {
        if run.id.phase != ValidationPhase::WholeWork
            || run.handler_index != 0
            || run.quality_phase
            || run.attempt_count != 1
            || run.final_verdict != Some(VerdictValue::Pass)
        {
            return Err(WireError::InvalidFrame);
        }
        return Ok(());
    }
    let handler = spec
        .handlers
        .get(run.handler_index as usize)
        .ok_or(WireError::InvalidFrame)?;
    if run.attempt_count as usize > spec.handlers.len()
        || run.quality_phase != (handler.agentic && spec.quality_bar.is_some())
        || (run.attempt_count == 0 && (run.final_verdict.is_some() || run.handler_index != 0))
    {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
}
fn validate_attempt(
    spec: &ValidationContent,
    attempt: &VerdictRecord,
    ordinal: u32,
) -> Result<usize, WireError> {
    if attempt.attempt != ordinal {
        return Err(WireError::InvalidFrame);
    }
    if spec.kind == ValidationKind::Receipt {
        // Exact implicit receipt handler used by Core::receipt_passes. Receipt
        // rows prove delivery only; they have no user-configured handler slot.
        let receipt = HandlerRef {
            id: ValidatorId::from_u128(1),
            version: ContentHash(*blake3::hash(b"focal.builtin.receipt.v1").as_bytes()),
            agentic: false,
        };
        if ordinal != 0
            || attempt.handler != receipt
            || attempt.value != VerdictValue::Pass
            || !attempt.evidence.is_empty()
        {
            return Err(WireError::InvalidFrame);
        }
        return Ok(0);
    }
    let index = if attempt.handler.agentic && spec.quality_bar.is_some() {
        spec.handlers
            .len()
            .checked_sub(1)
            .ok_or(WireError::InvalidFrame)?
    } else {
        ordinal as usize
    };
    if ordinal as usize >= spec.handlers.len()
        || spec.handlers.get(index) != Some(&attempt.handler)
        || index < ordinal as usize
        || (ordinal == 0 && index != 0)
    {
        return Err(WireError::InvalidFrame);
    }
    Ok(index)
}
fn next_handler(spec: &ValidationContent, prior: &VerdictRecord, index: usize) -> Option<usize> {
    let quality_phase = prior.handler.agentic && spec.quality_bar.is_some();
    let next = index.checked_add(1)?;
    match prior.value {
        VerdictValue::Error if !quality_phase => spec
            .handlers
            .iter()
            .enumerate()
            .skip(next)
            .find(|(_, handler)| spec.quality_bar.is_none() || !handler.agentic)
            .map(|(index, _)| index),
        VerdictValue::Pass if !quality_phase && spec.quality_bar.is_some() => spec
            .handlers
            .iter()
            .enumerate()
            .skip(next)
            .find(|(_, handler)| handler.agentic)
            .map(|(index, _)| index),
        _ => None,
    }
}

pub(crate) fn validate_stream_response(
    stream: &StreamRequest,
    reply: &StreamReply,
    ledger: LedgerId,
    route: RouteEpoch,
    limits: &WireLimits,
) -> Result<(), WireError> {
    if reply.token.route_epoch != route {
        return Err(WireError::InvalidFrame);
    }
    if reply.token.ledger != ledger
        || reply.cursor.key.ledger != ledger
        || reply.cursor.position.ledger != ledger
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
                bytes = bytes.saturating_add(payload_len(event, limits.max_frame_bytes)?);
                if delta.id.ledger != ledger || cursor.position != Position::after_delta(delta.id) {
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
        validate_page(seed, ledger, credits.items, false)?;
    }
    Ok(())
}
