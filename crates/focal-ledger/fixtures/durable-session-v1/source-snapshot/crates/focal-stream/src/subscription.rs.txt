use crate::{
    ALLOCATOR_OVERHEAD, CursorCommand, CursorMode, CursorOperation, CursorRecord, CursorToken,
    DeltaFilter, Position, PositionOffset, ResyncReason, StreamError, add, mul,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{Delta, DeltaFact, LedgerId, SessionSeq};
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayBounds {
    pub ledger: LedgerId,
    pub floor: SessionSeq,
    pub published: SessionSeq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayLimit {
    pub max_items: usize,
    /// Sum of encoded Delta bytes, excluding the transport cursor envelope.
    pub max_bytes: usize,
    pub max_sequences: u64,
}
impl Default for ReplayLimit {
    fn default() -> Self {
        Self {
            max_items: 256,
            max_bytes: 256 * 1024,
            max_sequences: 256,
        }
    }
}

/// Adapter over the retained ledger/log/archive. No durable events are stored
/// by Subscription. Read only published immutable facts, in exact ID order,
/// visiting every delta before the returned position. Never hydrate old deltas
/// from current live objects. The source must bound disk/scan work as requested.
///
/// Returning Resolved(S) certifies that *all* deltas through S were visited;
/// an item-limited partial transaction returns AfterDelta(last). Sequence gaps
/// are valid only for transactions that produced no deltas. The callback avoids
/// allocating an unbounded or duplicated intermediate replay vector.
pub trait DeltaSource {
    fn bounds(&self) -> ReplayBounds;
    fn replay(
        &self,
        after: Position,
        limit: ReplayLimit,
        visit: &mut dyn FnMut(&Delta) -> Result<(), StreamError>,
    ) -> Result<Position, StreamError>;
}

#[derive(Debug, Clone, Copy)]
pub struct TransportConfig {
    pub max_queue_items: usize,
    pub max_queue_bytes: usize,
    pub max_credit_items: usize,
    pub max_credit_bytes: usize,
    pub max_lag_sequences: u64,
}
impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_queue_items: 256,
            max_queue_bytes: 1024 * 1024,
            max_credit_items: 256,
            max_credit_bytes: 1024 * 1024,
            max_lag_sequences: 100_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "Inline deltas avoid one extra allocation per message; fixed queue slot capacity is explicitly accounted"
)]
pub enum StreamEvent {
    Delta {
        cursor: CursorToken,
        delta: Delta,
    },
    Resolved {
        cursor: CursorToken,
    },
    Resync {
        cursor: CursorToken,
        reason: ResyncReason,
        floor: SessionSeq,
    },
}

/// A dequeued transport message retains its charge until the transport drops
/// it. The adapter must serialize deliveries in this order. Dequeue is not a
/// durable consumer acknowledgment; a failed write causes replay on reconnect.
pub struct Delivery {
    event: StreamEvent,
    wire_bytes: usize,
    _allocation: DeliveryAllocation,
}

// Only the single reserved control slot is shared across the owner and a
// detached transport delivery. Data delivery permits move with their payload.
enum DeliveryAllocation {
    Owned { _permit: Allocation },
    Control { _permit: Arc<Allocation> },
}
impl Delivery {
    pub fn event(&self) -> &StreamEvent {
        &self.event
    }
    pub fn wire_bytes(&self) -> usize {
        self.wire_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriveReport {
    pub scanned: usize,
    pub queued: usize,
    pub position: Position,
    pub backpressured: bool,
    pub resync: Option<ResyncReason>,
}

/// Bounded ephemeral delivery state. Reopening from the last durably published
/// CursorRecord deliberately redelivers unacknowledged facts with the same IDs.
pub struct Subscription {
    acknowledged: CursorToken,
    delivered: Position,
    replayed: Position,
    expires_at: u64,
    protected: bool,
    clock: u64,
    filter: DeltaFilter,
    config: TransportConfig,
    budget: MemoryBudget,
    queue: VecDeque<Delivery>,
    queue_bytes: usize,
    item_credit: usize,
    byte_credit: usize,
    resolved: Option<Position>,
    resync: Option<(ResyncReason, SessionSeq)>,
    closed: bool,
    terminal_reason: Option<ResyncReason>,
    _allocation: Allocation,
    control_allocation: Arc<Allocation>,
}

impl Subscription {
    pub fn resume(
        record: &CursorRecord,
        now: u64,
        config: TransportConfig,
        budget: MemoryBudget,
    ) -> Result<Self, StreamError> {
        if record.token.key.ledger != record.token.position.ledger {
            return Err(StreamError::WrongLedger);
        }
        if record.token.generation == 0 {
            return Err(StreamError::WrongGeneration);
        }
        if record.mode != CursorMode::Protected && record.expires_at <= now {
            return Err(StreamError::ResyncRequired(ResyncReason::LeaseExpired));
        }
        match record.mode {
            CursorMode::Live | CursorMode::Protected => {}
            CursorMode::Seeding { .. } => return Err(StreamError::SeedNotComplete),
            CursorMode::Resync { reason } => return Err(StreamError::ResyncRequired(reason)),
        }
        if config.max_queue_items == 0
            || config.max_queue_bytes == 0
            || config.max_credit_items == 0
            || config.max_credit_bytes == 0
        {
            return Err(StreamError::Invalid("transport limits must be nonzero"));
        }
        let charge = add(
            add(size_of::<Self>(), ALLOCATOR_OVERHEAD)?,
            add(
                mul(config.max_queue_items, size_of::<Delivery>())?,
                record.filter.charge()?,
            )?,
        )?;
        let allocation = budget
            .reserve(BudgetKind::Query, BudgetLane::Ordinary, charge)?
            .commit();
        // This separate reserved lane keeps final control available when all
        // data capacity/credits are exhausted. No allocation on the error path.
        let control_allocation = Arc::new(
            budget
                .reserve(
                    BudgetKind::Control,
                    BudgetLane::Completion,
                    add(
                        add(size_of::<Delivery>(), size_of::<Allocation>())?,
                        mul(2, ALLOCATOR_OVERHEAD)?,
                    )?,
                )?
                .commit(),
        );
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(config.max_queue_items)
            .map_err(|_| StreamError::Capacity)?;
        Ok(Self {
            acknowledged: record.token,
            delivered: record.token.position,
            replayed: record.token.position,
            expires_at: record.expires_at,
            protected: record.mode == CursorMode::Protected,
            clock: now,
            filter: record.filter.clone(),
            config,
            budget,
            queue,
            queue_bytes: 0,
            item_credit: 0,
            byte_credit: 0,
            resolved: None,
            resync: None,
            closed: false,
            terminal_reason: None,
            _allocation: allocation,
            control_allocation,
        })
    }

    pub fn queued_items(&self) -> usize {
        self.queue.len()
    }
    pub fn queued_bytes(&self) -> usize {
        self.queue_bytes
    }
    pub fn acknowledged(&self) -> CursorToken {
        self.acknowledged
    }
    pub fn delivered_position(&self) -> Position {
        self.delivered
    }
    pub fn replayed_position(&self) -> Position {
        self.replayed
    }

    pub fn grant(&mut self, items: usize, bytes: usize) -> Result<(), StreamError> {
        let new_items = add(self.item_credit, items)?;
        let new_bytes = add(self.byte_credit, bytes)?;
        if new_items > self.config.max_credit_items || new_bytes > self.config.max_credit_bytes {
            return Err(StreamError::Capacity);
        }
        self.item_credit = new_items;
        self.byte_credit = new_bytes;
        Ok(())
    }

    /// Call on the runtime's bounded reconciliation schedule even without a
    /// notification. A lost final hint cannot strand authoritative replay.
    pub fn reconcile(&mut self, bounds: ReplayBounds, now: u64) -> Result<(), StreamError> {
        self.advance_clock(now)?;
        if bounds.ledger != self.acknowledged.key.ledger {
            return Err(StreamError::WrongLedger);
        }
        if bounds.floor > bounds.published {
            return Err(StreamError::SourceViolation("floor exceeds publication"));
        }
        if self.closed || self.resync.is_some() {
            return Ok(());
        }
        if self.protected {
            if self.replayed.retention_prefix() < bounds.floor {
                return Err(StreamError::SourceViolation(
                    "protected consumer lost required history",
                ));
            }
            if self.replayed.sequence > bounds.published {
                return Err(StreamError::SourceViolation("publication moved backward"));
            }
            return Ok(());
        }
        if now >= self.expires_at {
            self.require_resync(ResyncReason::LeaseExpired, bounds.floor);
        } else if self.replayed.retention_prefix() < bounds.floor {
            self.require_resync(ResyncReason::HistoryExpired, bounds.floor);
        } else if bounds
            .published
            .0
            .saturating_sub(self.acknowledged.position.sequence.0)
            > self.config.max_lag_sequences
        {
            self.require_resync(ResyncReason::SlowConsumer, bounds.floor);
        } else if self.replayed.sequence > bounds.published {
            return Err(StreamError::SourceViolation("publication moved backward"));
        }
        Ok(())
    }

    /// Replay is staged atomically; malformed source output, capacity errors,
    /// or source failures cannot advance the position or leak a partial batch.
    pub fn pump(
        &mut self,
        source: &impl DeltaSource,
        requested: ReplayLimit,
        now: u64,
    ) -> Result<DriveReport, StreamError> {
        let bounds = source.bounds();
        self.reconcile(bounds, now)?;
        if let Some((reason, _)) = self.resync {
            return Ok(self.report(0, 0, false, Some(reason)));
        }
        if self.closed {
            return Ok(self.report(0, 0, false, None));
        }
        if requested.max_items == 0 || requested.max_bytes == 0 || requested.max_sequences == 0 {
            return Err(StreamError::Invalid("replay work limits must be nonzero"));
        }
        let available_items = self.config.max_queue_items.saturating_sub(self.queue.len());
        let available_bytes = self.config.max_queue_bytes.saturating_sub(self.queue_bytes);
        let envelope_bytes = add(
            1,
            postcard::experimental::serialized_size(&self.acknowledged)
                .map_err(|_| StreamError::Codec)?,
        )?;
        // Position varints can grow as replay advances. Reserve the maximum
        // encoded token growth (sequence + ordinal), rather than trusting its
        // smaller currently acknowledged encoding.
        let envelope_bytes = add(envelope_bytes, 16)?;
        let max_items = requested.max_items.min(available_items).min(
            available_bytes
                .checked_div(add(envelope_bytes, 1)?)
                .ok_or(StreamError::Capacity)?,
        );
        if max_items == 0 {
            return Ok(self.report(0, 0, true, None));
        }
        let max_bytes = requested
            .max_bytes
            .min(available_bytes.saturating_sub(mul(max_items, envelope_bytes)?));
        if max_bytes == 0 {
            return Ok(self.report(0, 0, true, None));
        }
        let limit = ReplayLimit {
            max_items,
            max_bytes,
            max_sequences: requested.max_sequences,
        };
        let _staging = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            add(ALLOCATOR_OVERHEAD, mul(max_items, size_of::<Delivery>())?)?,
        )?;
        let mut staged = Vec::new();
        staged
            .try_reserve_exact(max_items)
            .map_err(|_| StreamError::Capacity)?;
        let mut previous = self.replayed;
        let mut scanned = 0;
        let mut scanned_bytes = 0;
        let mut queued_bytes = 0;
        let end = source.replay(self.replayed, limit, &mut |delta| {
            if scanned == max_items {
                return Err(StreamError::SourceViolation("source exceeded item limit"));
            }
            validate_delta(previous, delta, bounds.published)?;
            if delta.id.sequence.0.saturating_sub(self.replayed.sequence.0) > limit.max_sequences {
                return Err(StreamError::SourceViolation(
                    "source exceeded sequence work limit",
                ));
            }
            let raw_bytes =
                postcard::experimental::serialized_size(delta).map_err(|_| StreamError::Codec)?;
            scanned_bytes = add(scanned_bytes, raw_bytes)?;
            if scanned_bytes > max_bytes {
                return Err(StreamError::SourceViolation("source exceeded byte limit"));
            }
            scanned = add(scanned, 1)?;
            previous = Position::after_delta(delta.id);
            if self.filter.matches(delta) {
                let cursor = self.acknowledged.at(previous);
                let wire_bytes = add(
                    raw_bytes,
                    add(
                        1,
                        postcard::experimental::serialized_size(&cursor)
                            .map_err(|_| StreamError::Codec)?,
                    )?,
                )?;
                queued_bytes = add(queued_bytes, wire_bytes)?;
                if queued_bytes > available_bytes {
                    return Err(StreamError::Capacity);
                }
                let charge = add(
                    add(
                        add(size_of::<Delivery>(), size_of::<Allocation>())?,
                        mul(2, ALLOCATOR_OVERHEAD)?,
                    )?,
                    delta_heap_bytes(delta)?,
                )?;
                let allocation = self
                    .budget
                    .reserve(BudgetKind::Query, BudgetLane::Ordinary, charge)?
                    .commit();
                staged.push(Delivery {
                    event: StreamEvent::Delta {
                        cursor,
                        delta: delta.clone(),
                    },
                    wire_bytes,
                    _allocation: DeliveryAllocation::Owned {
                        _permit: allocation,
                    },
                });
            }
            Ok(())
        })?;
        end.validate(bounds.ledger, bounds.published)?;
        if end < previous
            || end.sequence.0.saturating_sub(self.replayed.sequence.0) > limit.max_sequences
        {
            return Err(StreamError::SourceViolation("invalid replay coverage"));
        }
        if matches!(end.offset, PositionOffset::Delta(_)) && end != previous {
            return Err(StreamError::SourceViolation(
                "partial coverage did not name last delta",
            ));
        }
        let queued = staged.len();
        self.queue.extend(staged);
        self.queue_bytes = add(self.queue_bytes, queued_bytes)?;
        self.replayed = end;
        if end.offset == PositionOffset::Resolved && end > self.delivered {
            self.resolved = Some(end);
        }
        Ok(self.report(scanned, queued, false, None))
    }

    /// Returns a message for the ordered transport. Control ignores data
    /// credits. Resolved waits behind all staged data; Resync supersedes and
    /// clears data because its durable acknowledgment has not advanced.
    pub fn next(&mut self, now: u64) -> Result<Option<Delivery>, StreamError> {
        self.advance_clock(now)?;
        if !self.protected && !self.closed && self.resync.is_none() && now >= self.expires_at {
            self.require_resync(
                ResyncReason::LeaseExpired,
                self.acknowledged.position.retention_prefix(),
            );
        }
        // A held control delivery occupies the sole reserved control slot.
        // Preserve pending resolved/resync state until its transport releases it.
        if Arc::strong_count(&self.control_allocation) > 1 {
            return Ok(None);
        }
        if let Some((reason, floor)) = self.resync.take() {
            self.closed = true;
            return self
                .control(StreamEvent::Resync {
                    cursor: self.acknowledged,
                    reason,
                    floor,
                })
                .map(Some);
        }
        if self.closed {
            return Ok(None);
        }
        if let Some(front) = self.queue.front() {
            if self.item_credit == 0 || self.byte_credit < front.wire_bytes {
                return Ok(None);
            }
            let delivery = self
                .queue
                .pop_front()
                .ok_or(StreamError::Invalid("queue changed without owner"))?;
            self.queue_bytes = self.queue_bytes.saturating_sub(delivery.wire_bytes);
            self.item_credit = self.item_credit.saturating_sub(1);
            self.byte_credit = self.byte_credit.saturating_sub(delivery.wire_bytes);
            if let StreamEvent::Delta { cursor, .. } = &delivery.event {
                self.delivered = cursor.position;
            }
            return Ok(Some(delivery));
        }
        if let Some(position) = self.resolved.take()
            && position > self.delivered
        {
            self.delivered = position;
            return self
                .control(StreamEvent::Resolved {
                    cursor: self.acknowledged.at(position),
                })
                .map(Some);
        }
        Ok(None)
    }

    /// Validate client acknowledgments against this connection before asking
    /// the ledger to persist the command. Clients cannot skip unsent events.
    pub fn acknowledge_command(
        &self,
        token: CursorToken,
        revision: u64,
        now: u64,
    ) -> Result<CursorCommand, StreamError> {
        self.acknowledged.same_stream(token)?;
        if let Some(reason) = self.terminal_reason {
            return Err(StreamError::ResyncRequired(reason));
        }
        if !self.protected && now >= self.expires_at {
            return Err(StreamError::ResyncRequired(ResyncReason::LeaseExpired));
        }
        if token.position > self.delivered {
            return Err(StreamError::BeyondDelivered);
        }
        if token.position < self.acknowledged.position {
            return Err(StreamError::CursorRegression);
        }
        Ok(CursorCommand {
            expected_revision: revision,
            now,
            operation: CursorOperation::Acknowledge { token },
        })
    }

    /// Call only after the registry acknowledgment/renewal is durable and
    /// published. Transport success alone must never call this method.
    pub fn acknowledged_committed(&mut self, record: &CursorRecord) -> Result<(), StreamError> {
        self.acknowledged.same_stream(record.token)?;
        if self.filter != record.filter {
            return Err(StreamError::WrongScope);
        }
        if self.protected != (record.mode == CursorMode::Protected) {
            return Err(StreamError::Invalid(
                "consumer protection cannot change implicitly",
            ));
        }
        if record.token.position > self.delivered {
            return Err(StreamError::BeyondDelivered);
        }
        if record.token.position < self.acknowledged.position {
            return Err(StreamError::CursorRegression);
        }
        if matches!(record.mode, CursorMode::Seeding { .. }) {
            return Err(StreamError::SeedNotComplete);
        }
        self.acknowledged = record.token;
        self.expires_at = record.expires_at;
        if let CursorMode::Resync { reason } = record.mode {
            self.require_resync(reason, record.token.position.retention_prefix());
        }
        Ok(())
    }

    pub fn require_resync(&mut self, reason: ResyncReason, floor: SessionSeq) {
        if self.protected {
            return;
        }
        if self.closed || self.resync.is_some() {
            return;
        }
        self.queue.clear();
        self.queue_bytes = 0;
        self.resolved = None;
        self.resync = Some((reason, floor));
        self.terminal_reason = Some(reason);
    }

    fn advance_clock(&mut self, now: u64) -> Result<(), StreamError> {
        if now < self.clock {
            return Err(StreamError::ClockRegression);
        }
        self.clock = now;
        Ok(())
    }
    fn report(
        &self,
        scanned: usize,
        queued: usize,
        backpressured: bool,
        resync: Option<ResyncReason>,
    ) -> DriveReport {
        DriveReport {
            scanned,
            queued,
            position: self.replayed,
            backpressured,
            resync,
        }
    }
    fn control(&self, event: StreamEvent) -> Result<Delivery, StreamError> {
        let wire_bytes =
            postcard::experimental::serialized_size(&event).map_err(|_| StreamError::Codec)?;
        Ok(Delivery {
            event,
            wire_bytes,
            _allocation: DeliveryAllocation::Control {
                _permit: Arc::clone(&self.control_allocation),
            },
        })
    }
}

fn validate_delta(
    previous: Position,
    delta: &Delta,
    published: SessionSeq,
) -> Result<(), StreamError> {
    let position = Position::after_delta(delta.id);
    position.validate(previous.ledger, published)?;
    if delta.schema != 1 {
        return Err(StreamError::SourceViolation("unknown delta schema"));
    }
    if position <= previous {
        return Err(StreamError::SourceViolation("delta order regressed"));
    }
    if position.sequence == previous.sequence {
        let PositionOffset::Delta(ordinal) = previous.offset else {
            return Err(StreamError::SourceViolation(
                "delta after resolved transaction",
            ));
        };
        if ordinal.checked_add(1) != Some(delta.id.ordinal) {
            return Err(StreamError::SourceViolation("delta ordinal gap"));
        }
    } else if delta.id.ordinal != 0 {
        return Err(StreamError::SourceViolation(
            "transaction starts after ordinal zero",
        ));
    }
    Ok(())
}

fn delta_heap_bytes(delta: &Delta) -> Result<usize, StreamError> {
    match &delta.fact {
        DeltaFact::Progress(text) => add(text.capacity(), ALLOCATOR_OVERHEAD),
        DeltaFact::Verdict(record) => add(
            mul(
                record.evidence.capacity(),
                size_of::<focal_model::ArtifactRef>(),
            )?,
            ALLOCATOR_OVERHEAD,
        ),
        _ => Ok(0),
    }
}
