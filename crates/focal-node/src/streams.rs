//! Pull transport over the session's durable consumer registry. Requests first
//! cross a current-term read barrier, then propose cursor metadata and await its
//! exact durable receipt. This owner never polls or drops replication messages
//! on the multi-voter path.
use crate::{host::access, reads::ReadViews};
use focal_ledger::{
    CursorInput, CursorSubmission, LedgerError, ManagedCursorInput, ManagedSubmission,
    ReadCorrelation, Session, SessionEvents,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use focal_stream::*;
use focal_wire::*;
use std::time::{SystemTime, UNIX_EPOCH};

const LEASE_MS: u64 = 60_000;
const REPLY_OVERHEAD: usize = 512;
// Seed objects use the graph model's conservative 64x heap allowance. Split
// seed pages before cloning so a generous client frame credit remains bounded.
const MAX_SEED_BYTES: u32 = 64 * 1024;

#[derive(Clone, Copy)]
enum StreamKey {
    Legacy(RequestKey),
    Managed(ManagedRequestKey),
}
impl StreamKey {
    fn principal(self) -> ParticipantId {
        match self {
            Self::Legacy(key) => key.principal,
            Self::Managed(key) => key.stream.principal,
        }
    }
    fn id(self) -> RequestId {
        match self {
            Self::Legacy(key) => key.id,
            Self::Managed(key) => key.id,
        }
    }
}
enum Stage {
    Barrier(Vec<u8>),
    Receipt,
    Finished,
}
/// A bounded owned request, including any seed page captured before proposing
/// its tail pin. Dropping this handle abandons only the response waiter: a
/// proposed command remains recoverable and retryable under its exact key.
pub(crate) struct PendingStream {
    ledger: LedgerId,
    route_epoch: RouteEpoch,
    key: StreamKey,
    scope: ContentHash,
    intent_hash: ContentHash,
    stream: StreamRequest,
    term: u64,
    max_frame_bytes: u32,
    max_items: u32,
    stage: Stage,
    seed: Option<ReadPage>,
    /// The ledger runs the native engine: the barrier is the native read
    /// boundary, positions live on the stream line (23 §6), the seed is read
    /// by the client and the reply token names the native prefix.
    native: bool,
    _charge: Allocation,
}
impl PendingStream {
    /// Timeout/leadership-loss outcome depends on whether a proposal could have
    /// reached the log. It never claims that an admitted ACK was rolled back.
    pub fn interrupted(&self) -> AccessError {
        match self.stage {
            Stage::Barrier(_) => AccessError::Unavailable,
            Stage::Receipt | Stage::Finished => AccessError::OutcomeUnknown,
        }
    }
}
pub(crate) struct Streams {
    budget: MemoryBudget,
    incarnation: [u8; 16],
    next: u64,
}
impl Streams {
    #[cfg(test)]
    pub fn charged_bytes(&self) -> usize {
        self.budget.stats().used
    }

    pub fn new() -> Result<Self, AccessError> {
        Self::with_budget(
            MemoryBudget::new(8 * 1024 * 1024, 256 * 1024).map_err(|_| AccessError::Capacity)?,
        )
    }
    pub fn in_budget(parent: &MemoryBudget) -> Result<Self, AccessError> {
        Self::with_budget(
            parent
                .child(8 * 1024 * 1024, 256 * 1024)
                .map_err(|_| AccessError::Capacity)?,
        )
    }
    fn with_budget(budget: MemoryBudget) -> Result<Self, AccessError> {
        let mut incarnation = [0; 16];
        getrandom::fill(&mut incarnation).map_err(|_| AccessError::Unavailable)?;
        Ok(Self {
            budget,
            incarnation,
            next: 0,
        })
    }
    pub fn begin(
        &mut self,
        session: &mut Session,
        peer: &AuthenticatedPeer,
        request: &RequestEnvelope,
        stream: &StreamRequest,
        limits: &WireLimits,
    ) -> Result<PendingStream, AccessError> {
        if request.ledger != session.ledger() {
            return Err(AccessError::Unauthorized);
        }
        let key = match &request.operation {
            Operation::Stream(value) if value == stream => StreamKey::Legacy(RequestKey {
                principal: peer.principal(),
                epoch: request.request_epoch,
                id: request.request_id,
            }),
            Operation::Managed {
                key,
                operation: ManagedOperation::Cursor(value),
            } if value == stream && key.stream.principal == peer.principal() => {
                StreamKey::Managed(*key)
            }
            _ => return Err(AccessError::InvalidRequest),
        };
        if !session.is_authoritative() {
            return Err(AccessError::Unavailable);
        }
        // A native ledger streams schema-2 deltas; only the native profile
        // proves the consumer decodes them, so older profiles are refused at
        // the door rather than handed facts they cannot read.
        let native = session.activation().is_native();
        if native && request.protocol != NATIVE_PROTOCOL_VERSION {
            return Err(AccessError::UnsupportedProtocol);
        }
        let scope = stream_scope(peer, request.ledger, stream.filter())?;
        let request_bytes = postcard::experimental::serialized_size(stream)
            .map_err(|_| AccessError::InvalidRequest)?;
        if request_bytes > limits.max_frame_bytes as usize {
            return Err(AccessError::Capacity);
        }
        // Account bounded clone/index overhead plus seed/response staging before
        // issuing ReadIndex or retaining any client payload. The shared budget
        // bounds all concurrent waiters, including abandoned transport requests.
        let seed = matches!(stream, StreamRequest::Open { seed: true, .. });
        let response_bytes = stream
            .credits()
            .bytes
            .min(limits.max_frame_bytes)
            .min(if seed { MAX_SEED_BYTES } else { u32::MAX })
            as usize;
        let charge = request_bytes
            .checked_mul(64)
            .and_then(|n| {
                response_bytes
                    .checked_mul(if seed { 64 } else { 3 })
                    .and_then(|r| n.checked_add(r))
            })
            .and_then(|n| {
                (stream.credits().items.min(limits.max_items) as usize)
                    .checked_mul(size_of::<StreamEvent>())
                    .and_then(|r| n.checked_add(r))
            })
            .and_then(|n| n.checked_add(4096))
            .ok_or(AccessError::Capacity)?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, charge)
            .map_err(|_| AccessError::Capacity)?
            .commit();
        let intent_hash = cursor_request_intent(request.ledger, peer.principal(), stream)
            .map_err(|_| AccessError::InvalidRequest)?;
        original_cursor_token(session, key, intent_hash)?;
        self.next = self.next.checked_add(1).ok_or(AccessError::Unavailable)?;
        let mut context = b"focal.stream.read.v1\0".to_vec();
        context.extend_from_slice(&self.incarnation);
        context.extend_from_slice(&self.next.to_be_bytes());
        if native {
            session
                .native_read_index(correlation(&context))
                .map_err(cursor_error)?;
        } else {
            session.read_index(context.clone()).map_err(cursor_error)?;
        }
        Ok(PendingStream {
            ledger: request.ledger,
            route_epoch: request.route_epoch,
            key,
            scope,
            intent_hash,
            stream: stream.clone(),
            term: session.status().term,
            max_frame_bytes: limits.max_frame_bytes,
            max_items: limits.max_items,
            stage: Stage::Barrier(context),
            seed: None,
            native,
            _charge: allocation,
        })
    }
    /// Call with every Session::poll result. The host sends all Raft messages
    /// from that result and retains this waiter until completion or a bounded
    /// timeout. No success precedes both its ReadIndex and committed cursor row.
    pub fn advance(
        &mut self,
        session: &mut Session,
        views: &mut ReadViews,
        pending: &mut PendingStream,
        events: &SessionEvents,
        limits: &WireLimits,
    ) -> Result<Option<StreamReply>, AccessError> {
        if pending.max_frame_bytes != limits.max_frame_bytes
            || pending.max_items != limits.max_items
        {
            return Err(AccessError::Capacity);
        }
        if matches!(pending.stage, Stage::Finished) {
            return Err(AccessError::InvalidRequest);
        }
        if session.ledger() != pending.ledger
            || session.status().term != pending.term
            || !session.is_authoritative()
        {
            return Err(pending.interrupted());
        }
        if let Stage::Barrier(context) = &pending.stage {
            // The barrier prefix is a stream-line position on both engines.
            let prefix = if pending.native {
                let wanted = correlation(context);
                match events
                    .native_read_boundaries
                    .iter()
                    .find(|boundary| boundary.correlation == wanted)
                {
                    Some(boundary) => session
                        .stream_sequence_of(boundary.native_sequence)
                        .map_err(access)?,
                    None => return Ok(None),
                }
            } else {
                match events
                    .read_barriers
                    .iter()
                    .find(|(value, _)| value == context)
                {
                    Some((_, prefix)) => *prefix,
                    None => return Ok(None),
                }
            };
            let original = original_cursor_token(session, pending.key, pending.intent_hash)?;
            let now = wall_ms()?.max(session.cursor_clock());
            let operation =
                Self::operation(session, views, pending, original, prefix, now, limits)?;
            let command = CursorCommand {
                expected_revision: session.cursor_revision(),
                now,
                operation,
            };
            match pending.key {
                StreamKey::Legacy(key) => {
                    let input = CursorInput {
                        ledger: pending.ledger,
                        key,
                        intent_hash: pending.intent_hash,
                        command,
                    };
                    match session.submit_cursor(&input).map_err(cursor_error)? {
                        CursorSubmission::Committed(_) | CursorSubmission::Pending(_) => {}
                    }
                }
                StreamKey::Managed(key) => {
                    let input = ManagedCursorInput {
                        key,
                        intent_hash: pending.intent_hash,
                        command,
                    };
                    match session
                        .propose_managed_cursor(&input, false)
                        .map_err(cursor_error)?
                    {
                        ManagedSubmission::Committed(_) | ManagedSubmission::Pending(_) => {}
                        ManagedSubmission::Domain(_) => return Err(AccessError::InvalidRequest),
                    }
                }
            }
            pending.stage = Stage::Receipt;
        }
        let Some(token) = original_cursor_token(session, pending.key, pending.intent_hash)? else {
            return Ok(None);
        };
        let now = wall_ms()?.max(session.cursor_clock());
        let reply = self.reply(session, pending, token, now, limits)?;
        pending.stage = Stage::Finished;
        Ok(Some(reply))
    }
    // Seed reads use a barrier already completed by the host, avoiding the
    // one-voter polling helper inside ReadViews::read(Linearizable).
    fn operation(
        session: &mut Session,
        views: &mut ReadViews,
        pending: &mut PendingStream,
        original: Option<CursorToken>,
        barrier: SessionSeq,
        now: u64,
        limits: &WireLimits,
    ) -> Result<CursorOperation, AccessError> {
        let expires_at = now.checked_add(LEASE_MS).ok_or(AccessError::Unavailable)?;

        let operation = match &pending.stream {
            StreamRequest::Open {
                consumer,
                filter,
                start,
                seed: true,
                credits,
            } => {
                if start.is_some() {
                    return Err(AccessError::UnsupportedOperation);
                }
                if credits.items == 0 || credits.bytes as usize <= REPLY_OVERHEAD + 256 {
                    return Err(AccessError::Capacity);
                }
                if pending.native {
                    // The native engine keeps no historical snapshot for a
                    // server-side seed: the client reads its seed at a prefix
                    // no older than this snapshot and the tail from here, so
                    // every fact between the two arrives at least once.
                    return Ok(CursorOperation::BeginSeed {
                        consumer: *consumer,
                        scope: pending.scope,
                        filter: filter.clone(),
                        snapshot: barrier,
                        expires_at,
                    });
                }
                // Capture the immutable prefix before committing its tail pin.
                // A retry uses its original snapshot; it cannot silently seed a
                // newer prefix under the old consumer generation.
                let prefix = original.map(|token| token.position.sequence);
                let consistency = match prefix {
                    Some(sequence) if sequence != session.graph_sequence() => {
                        ReadConsistency::Exact(ReadToken {
                            ledger: pending.ledger,
                            sequence,
                            route_epoch: pending.route_epoch,
                        })
                    }
                    _ => ReadConsistency::AtLeast(ReadToken {
                        ledger: pending.ledger,
                        sequence: barrier,
                        route_epoch: pending.route_epoch,
                    }),
                };
                let mut seed_limits = limits.clone();
                seed_limits.max_frame_bytes = credits
                    .bytes
                    .min(limits.max_frame_bytes)
                    .min(MAX_SEED_BYTES)
                    .saturating_sub(REPLY_OVERHEAD as u32);
                let page = views.read(
                    session,
                    pending.key.principal(),
                    &ReadRequest {
                        consistency,
                        query: match filter {
                            DeltaFilter::All => ReadQuery::SeedScan {
                                after: None,
                                claims: vec![],
                                max_bytes: seed_limits.max_frame_bytes,
                            },
                            DeltaFilter::Claims(claims) => ReadQuery::SeedScan {
                                after: None,
                                claims: claims.iter().copied().collect(),
                                max_bytes: seed_limits.max_frame_bytes,
                            },
                        },
                        max_items: credits.items,
                    },
                    pending.key.id(),
                    &seed_limits,
                )?;
                let snapshot = page.token.sequence;
                pending.seed = Some(page);
                CursorOperation::BeginSeed {
                    consumer: *consumer,
                    scope: pending.scope,
                    filter: filter.clone(),
                    snapshot,
                    expires_at,
                }
            }
            StreamRequest::Open {
                consumer,
                filter,
                start,
                seed: false,
                ..
            } => CursorOperation::Register {
                consumer: *consumer,
                scope: pending.scope,
                filter: filter.clone(),
                // Missing start means the beginning, never an implicit skip to
                // the current retention floor. Old history requires reseeding.
                start: start.unwrap_or_else(|| Position::origin(pending.ledger)),
                expires_at,
            },
            StreamRequest::Poll {
                cursor,
                acknowledged,
                ..
            } => {
                if original.is_none() {
                    check_cursor(
                        session,
                        pending.key.principal(),
                        *cursor,
                        pending.stream.filter(),
                    )?;
                }
                match acknowledged {
                    Some(token) => CursorOperation::AcknowledgeAndRenew {
                        token: *token,
                        expires_at,
                    },
                    None => CursorOperation::Renew {
                        consumer: cursor.key.consumer,
                        generation: cursor.generation,
                        expires_at,
                    },
                }
            }
            StreamRequest::CompleteSeed {
                cursor, snapshot, ..
            } => {
                if original.is_none() {
                    check_cursor(
                        session,
                        pending.key.principal(),
                        *cursor,
                        pending.stream.filter(),
                    )?;
                }
                CursorOperation::CompleteSeed {
                    consumer: cursor.key.consumer,
                    generation: cursor.generation,
                    snapshot: *snapshot,
                }
            }
        };
        Ok(operation)
    }
    fn reply(
        &self,
        session: &Session,
        pending: &mut PendingStream,
        original: CursorToken,
        now: u64,
        limits: &WireLimits,
    ) -> Result<StreamReply, AccessError> {
        let current = session
            .cursor(original.key.consumer)
            .ok_or(AccessError::ResyncRequired { floor: None })?;
        if original.same_stream(current.token).is_err() {
            return Err(AccessError::ResyncRequired { floor: None });
        }
        if current.expires_at <= now || matches!(current.mode, CursorMode::Resync { .. }) {
            return Err(AccessError::ResyncRequired { floor: None });
        }
        // The token names the published end of the stream line, which every
        // cursor position is bounded by on both engines (23 §6).
        let token = ReadToken {
            ledger: pending.ledger,
            sequence: session.stream_published(),
            route_epoch: pending.route_epoch,
        };
        let mut reply = StreamReply {
            token,
            cursor: original,
            acknowledged: original,
            seed: pending.seed.take(),
            events: Vec::new(),
        };
        if reply.seed.is_some()
            || matches!(&pending.stream, StreamRequest::CompleteSeed { .. })
            || matches!(current.mode, CursorMode::Seeding { .. })
        {
            return Ok(reply);
        }
        // Current durable progress can exceed an old request receipt. Never
        // rewind the durable ACK on retries. `cursor` is only a continuation
        // hint and may skip already delivered but not yet acknowledged bytes.
        // Projection ACKs assert the caller's own progress. They are not proofs
        // of delivery or effect execution; protected consumers cannot use this
        // interface and retain their trusted runtime acknowledgment path.
        reply.acknowledged = current.token;
        let mut delivery_record = current.clone();
        if let StreamRequest::Poll { cursor, .. } = &pending.stream {
            delivery_record.token.position = delivery_record.token.position.max(cursor.position);
        }
        reply.cursor = delivery_record.token;
        delivery_record
            .token
            .position
            .validate(pending.ledger, token.sequence)
            .map_err(stream_error)?;
        let credits = pending.stream.credits();
        let max_bytes = (limits.max_frame_bytes as usize)
            .saturating_sub(REPLY_OVERHEAD)
            .min(credits.bytes as usize);
        let max_items = credits.items.min(limits.max_items).max(1) as usize;
        let mut subscription = Subscription::resume(
            &delivery_record,
            now,
            TransportConfig {
                max_queue_items: max_items,
                max_queue_bytes: max_bytes.max(1),
                max_credit_items: max_items,
                max_credit_bytes: limits.max_frame_bytes as usize,
                ..TransportConfig::default()
            },
            self.budget.clone(),
        )
        .map_err(stream_error)?;
        subscription
            .grant(credits.items as usize, max_bytes)
            .map_err(stream_error)?;
        // No data credit still permits a terminal Resync control message.
        if credits.items > 0 && max_bytes > 0 {
            subscription
                .pump(
                    session,
                    ReplayLimit {
                        max_items: credits.items as usize,
                        max_bytes,
                        max_sequences: 256,
                    },
                    now,
                )
                .map_err(stream_error)?;
        } else {
            subscription
                .reconcile(session.stream_bounds(), now)
                .map_err(stream_error)?;
        }
        while let Some(delivery) = subscription.next(now).map_err(stream_error)? {
            reply.cursor = match delivery.event() {
                StreamEvent::Delta { cursor, .. }
                | StreamEvent::Resolved { cursor }
                | StreamEvent::Resync { cursor, .. } => *cursor,
            };
            reply.events.push(delivery.event().clone());
        }
        Ok(reply)
    }
    /// Convenience driver only for a true one-voter group. Replicated hosts use
    /// begin/advance and retain every emitted peer message in their transport.
    pub fn handle(
        &mut self,
        session: &mut Session,
        views: &mut ReadViews,
        peer: &AuthenticatedPeer,
        request: &RequestEnvelope,
        stream: &StreamRequest,
        limits: &WireLimits,
    ) -> Result<StreamReply, AccessError> {
        let status = session.status();
        if status.voters != [status.node_id] || !status.learners.is_empty() {
            return Err(AccessError::Unavailable);
        }
        let mut pending = self.begin(session, peer, request, stream, limits)?;
        for _ in 0..4 {
            let events = session.poll().map_err(cursor_error)?;
            if !events.messages.is_empty() {
                return Err(pending.interrupted());
            }
            if let Some(reply) = self.advance(session, views, &mut pending, &events, limits)? {
                return Ok(reply);
            }
        }
        Err(pending.interrupted())
    }
}
fn wall_ms() -> Result<u64, AccessError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AccessError::Unavailable)?
            .as_millis(),
    )
    .map_err(|_| AccessError::Unavailable)
}

fn check_cursor(
    session: &Session,
    principal: ParticipantId,
    cursor: CursorToken,
    filter: &DeltaFilter,
) -> Result<(), AccessError> {
    if session.cursor_owner(cursor.key.consumer) != Some(principal) {
        return Err(AccessError::Unauthorized);
    }
    let current = session
        .cursor(cursor.key.consumer)
        .ok_or(AccessError::InvalidRequest)?;
    current.token.same_stream(cursor).map_err(stream_error)?;
    if &current.filter != filter || current.mode == CursorMode::Protected {
        return Err(AccessError::Unauthorized);
    }
    cursor
        .position
        .validate(session.ledger(), session.stream_published())
        .map_err(stream_error)
}
/// The native read barrier correlation of one stream request's context.
fn correlation(context: &[u8]) -> ReadCorrelation {
    let mut hash = blake3::Hasher::new_derive_key("focal.stream.native-barrier.v1");
    hash.update(context);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    ReadCorrelation(bytes)
}
fn cursor_error(error: LedgerError) -> AccessError {
    match error {
        LedgerError::Stream(error) => stream_error(error),
        LedgerError::CursorRequest(_) => AccessError::InvalidRequest,
        error => access(error),
    }
}
fn stream_error(error: StreamError) -> AccessError {
    match error {
        StreamError::Capacity | StreamError::Memory(_) => AccessError::Capacity,
        StreamError::ResyncRequired(_)
        | StreamError::MissingConsumer
        | StreamError::WrongGeneration => AccessError::ResyncRequired { floor: None },
        StreamError::WrongLedger | StreamError::WrongScope | StreamError::WrongConsumer => {
            AccessError::Unauthorized
        }
        StreamError::SourceUnavailable | StreamError::SourceViolation(_) => {
            AccessError::Unavailable
        }
        _ => AccessError::InvalidRequest,
    }
}

fn original_cursor_token(
    session: &Session,
    key: StreamKey,
    intent: ContentHash,
) -> Result<Option<CursorToken>, AccessError> {
    match key {
        StreamKey::Legacy(key) => session
            .cursor_receipt(&key)
            .map(|receipt| {
                if receipt.intent_hash != intent {
                    return Err(AccessError::InvalidRequest);
                }
                receipt
                    .record
                    .as_ref()
                    .map(|record| record.token)
                    .ok_or(AccessError::Unavailable)
            })
            .transpose(),
        StreamKey::Managed(key) => session
            .managed_receipt(&key, intent, ManagedRequestFamily::Cursor)
            .map_err(cursor_error)?
            .map(|receipt| {
                let ManagedReceiptOutcome::Cursor {
                    record: Some(record),
                    ..
                } = &receipt.outcome
                else {
                    return Err(AccessError::InvalidRequest);
                };
                let token = record.token;
                Ok(CursorToken {
                    key: ConsumerKey {
                        ledger: token.key.ledger,
                        consumer: ConsumerId(token.key.consumer),
                    },
                    generation: token.generation,
                    scope: token.scope,
                    position: Position {
                        ledger: token.position.ledger,
                        sequence: token.position.sequence,
                        offset: match token.position.offset {
                            CursorPositionOffsetSnapshot::Delta(value) => {
                                PositionOffset::Delta(value)
                            }
                            CursorPositionOffsetSnapshot::Resolved => PositionOffset::Resolved,
                        },
                    },
                })
            })
            .transpose(),
    }
}
