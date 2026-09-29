use crate::{
    ALLOCATOR_OVERHEAD, ConsumerId, ConsumerKey, CursorToken, DeltaFilter, Position, ResyncReason,
    StreamError, add, mul,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ContentHash, LedgerId, SessionSeq};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CursorMode {
    Live,
    /// The projection must be durably installed before CompleteSeed. Its tail
    /// retention pin begins at this captured prefix immediately.
    Seeding {
        snapshot: SessionSeq,
    },
    Resync {
        reason: ResyncReason,
    },
    /// Proof/effect consumers retain their prefix until an explicit acknowledgment.
    /// Lease expiry, reseeding, and generic resync may never discard this obligation.
    Protected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorRecord {
    pub token: CursorToken,
    pub filter: DeltaFilter,
    pub expires_at: u64,
    pub mode: CursorMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorCheckpoint {
    pub schema: u16,
    pub ledger: LedgerId,
    pub revision: u64,
    pub clock: u64,
    /// All deltas at sequences <= this floor may have been retired.
    pub floor: SessionSeq,
    pub consumers: BTreeMap<ConsumerId, CursorRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorCommand {
    pub expected_revision: u64,
    pub now: u64,
    pub operation: CursorOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CursorOperation {
    Register {
        consumer: ConsumerId,
        scope: ContentHash,
        filter: DeltaFilter,
        start: Position,
        expires_at: u64,
    },
    Acknowledge {
        token: CursorToken,
    },
    Renew {
        consumer: ConsumerId,
        generation: u64,
        expires_at: u64,
    },
    BeginSeed {
        consumer: ConsumerId,
        scope: ContentHash,
        filter: DeltaFilter,
        snapshot: SessionSeq,
        expires_at: u64,
    },
    CompleteSeed {
        consumer: ConsumerId,
        generation: u64,
        snapshot: SessionSeq,
    },
    RequireResync {
        consumer: ConsumerId,
        generation: u64,
        reason: ResyncReason,
    },
    AdvanceFloor {
        through: SessionSeq,
    },
    /// Trusted service registration, never a generic projection-client option.
    RegisterProtected {
        consumer: ConsumerId,
        scope: ContentHash,
        filter: DeltaFilter,
        start: Position,
    },
    /// A projection poll advances its durable cursor and lease in one decision.
    /// Invalid acknowledgments leave both cursor and expiry unchanged.
    AcknowledgeAndRenew {
        token: CursorToken,
        expires_at: u64,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct RegistryConfig {
    pub max_consumers: usize,
    pub max_filter_claims: usize,
    pub max_lease_ttl: u64,
}
impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            max_consumers: 4096,
            max_filter_claims: 256,
            max_lease_ttl: 86_400_000,
        }
    }
}

struct Root {
    checkpoint: CursorCheckpoint,
    _allocation: Allocation,
}

/// This state becomes durable only when the embedding log commits its command
/// or checkpoint. Call prepare before that decision and publish afterward.
pub struct CursorRegistry {
    root: Root,
    owner: focal_memory::OwnerId,
    budget: MemoryBudget,
    config: RegistryConfig,
}
pub struct PreparedCursorUpdate {
    owner: focal_memory::OwnerId,
    base_revision: u64,
    next: Root,
    /// The consumers this update retired: released rows (an ordinary lease
    /// expired, or a cursor sent to resync) whose slot a registration needed
    /// or whose position the floor passed. Their names are free; whatever
    /// the embedding keeps per consumer follows them out.
    retired: Vec<ConsumerId>,
}
impl PreparedCursorUpdate {
    pub fn checkpoint(&self) -> &CursorCheckpoint {
        &self.next.checkpoint
    }
    pub fn retired(&self) -> &[ConsumerId] {
        &self.retired
    }
}

impl CursorRegistry {
    pub fn new(
        ledger: LedgerId,
        config: RegistryConfig,
        budget: MemoryBudget,
    ) -> Result<Self, StreamError> {
        Self::restore(
            CursorCheckpoint {
                schema: 1,
                ledger,
                revision: 0,
                clock: 0,
                floor: SessionSeq(0),
                consumers: BTreeMap::new(),
            },
            SessionSeq(0),
            config,
            budget,
        )
    }
    pub fn restore(
        checkpoint: CursorCheckpoint,
        published: SessionSeq,
        config: RegistryConfig,
        budget: MemoryBudget,
    ) -> Result<Self, StreamError> {
        if config.max_consumers == 0 || config.max_lease_ttl == 0 {
            return Err(StreamError::Invalid("invalid cursor registry limits"));
        }
        validate_checkpoint(&checkpoint, published, config)?;
        let allocation = budget
            .reserve(
                BudgetKind::ReadPins,
                BudgetLane::Completion,
                checkpoint_charge(&checkpoint)?,
            )?
            .commit();
        Ok(Self {
            owner: focal_memory::OwnerId::new()?,
            root: Root {
                checkpoint,
                _allocation: allocation,
            },
            budget,
            config,
        })
    }
    pub fn checkpoint(&self) -> &CursorCheckpoint {
        &self.root.checkpoint
    }
    pub fn get(&self, consumer: ConsumerId) -> Option<&CursorRecord> {
        self.root.checkpoint.consumers.get(&consumer)
    }
    pub fn revision(&self) -> u64 {
        self.root.checkpoint.revision
    }
    pub fn retention_limit(&self, published: SessionSeq) -> SessionSeq {
        retention_limit(&self.root.checkpoint, published)
    }

    pub fn prepare(
        &self,
        command: &CursorCommand,
        published: SessionSeq,
    ) -> Result<PreparedCursorUpdate, StreamError> {
        let lane = if matches!(command.operation, CursorOperation::Register { .. }) {
            BudgetLane::Ordinary
        } else {
            BudgetLane::Completion
        };
        self.prepare_in(command, published, lane)
    }
    /// Rebuild an already committed command under the completion allowance.
    /// Validation is identical; this cannot be used as pre-proposal admission.
    pub fn prepare_committed(
        &self,
        command: &CursorCommand,
        published: SessionSeq,
    ) -> Result<PreparedCursorUpdate, StreamError> {
        self.prepare_in(command, published, BudgetLane::Completion)
    }
    fn prepare_in(
        &self,
        command: &CursorCommand,
        published: SessionSeq,
        lane: BudgetLane,
    ) -> Result<PreparedCursorUpdate, StreamError> {
        let old = &self.root.checkpoint;
        if command.expected_revision != old.revision {
            return Err(StreamError::StalePreparation);
        }
        if command.now < old.clock {
            return Err(StreamError::ClockRegression);
        }
        if old.floor > published {
            return Err(StreamError::CursorAhead);
        }
        let next_revision = old
            .revision
            .checked_add(1)
            .ok_or(StreamError::CounterExhausted)?;
        // Reserve the old tree clone plus the largest possible new row/filter
        // before cloning. Replaced rows leave a conservative extra charge.
        let extra = match &command.operation {
            CursorOperation::Register { filter, .. }
            | CursorOperation::RegisterProtected { filter, .. }
            | CursorOperation::BeginSeed { filter, .. } => {
                if filter.len() > self.config.max_filter_claims {
                    return Err(StreamError::Capacity);
                }
                add(row_charge(), filter.charge()?)?
            }
            _ => 0,
        };
        let allocation = self
            .budget
            .reserve(
                BudgetKind::ReadPins,
                lane,
                add(checkpoint_charge(old)?, extra)?,
            )?
            .commit();
        let mut checkpoint = old.clone();
        checkpoint.clock = command.now;
        checkpoint.revision = next_revision;
        let mut retired = Vec::new();
        apply_operation(
            &mut checkpoint,
            &command.operation,
            published,
            self.config,
            &mut retired,
        )?;
        validate_checkpoint(&checkpoint, published, self.config)?;
        Ok(PreparedCursorUpdate {
            owner: self.owner,
            base_revision: self.revision(),
            next: Root {
                checkpoint,
                _allocation: allocation,
            },
            retired,
        })
    }

    /// No allocation or external work occurs after the durable decision.
    pub fn publish(&mut self, prepared: PreparedCursorUpdate) -> Result<(), StreamError> {
        if self.owner != prepared.owner || self.revision() != prepared.base_revision {
            return Err(StreamError::StalePreparation);
        }
        self.root = prepared.next;
        Ok(())
    }

    /// Replay a command already committed in the authoritative cursor log.
    /// This does not itself provide persistence or acknowledge a network peer.
    pub fn replay_committed(
        &mut self,
        command: &CursorCommand,
        published: SessionSeq,
    ) -> Result<(), StreamError> {
        let prepared = self.prepare_committed(command, published)?;
        self.publish(prepared)
    }
}

/// A row that holds no obligation any more: an ordinary consumer whose lease
/// expired, or whose cursor was sent to resync. Its retention is released
/// already (`retention_limit`); the row stays, so the consumer that comes
/// back reads why it must reseed, until a registration needs its slot —
/// never a protected consumer, whose obligation ends by explicit
/// acknowledgment alone.
fn released(row: &CursorRecord, clock: u64) -> bool {
    row.mode != CursorMode::Protected
        && (row.expires_at <= clock || matches!(row.mode, CursorMode::Resync { .. }))
}
/// Retire every released row, naming each, so the embedding's per-consumer
/// state follows. A retired name registers again under a generation no
/// earlier token carries (generations are the registry's revisions), so
/// nothing stale can move or renew the new incarnation.
fn retire_released(
    state: &mut CursorCheckpoint,
    retired: &mut Vec<ConsumerId>,
) -> Result<(), StreamError> {
    let clock = state.clock;
    let leaving = |row: &CursorRecord| released(row, clock);
    let count = state.consumers.values().filter(|row| leaving(row)).count();
    if count == 0 {
        return Ok(());
    }
    retired
        .try_reserve(count)
        .map_err(|_| StreamError::Capacity)?;
    state.consumers.retain(|id, row| {
        if leaving(row) {
            retired.push(*id);
            false
        } else {
            true
        }
    });
    Ok(())
}
/// Room for one more consumer: the bound, after the released rows leave.
fn admit_consumer(
    state: &mut CursorCheckpoint,
    config: RegistryConfig,
    retired: &mut Vec<ConsumerId>,
) -> Result<(), StreamError> {
    if state.consumers.len() >= config.max_consumers {
        retire_released(state, retired)?;
    }
    if state.consumers.len() >= config.max_consumers {
        return Err(StreamError::Capacity);
    }
    Ok(())
}

fn apply_operation(
    state: &mut CursorCheckpoint,
    operation: &CursorOperation,
    published: SessionSeq,
    config: RegistryConfig,
    retired: &mut Vec<ConsumerId>,
) -> Result<(), StreamError> {
    // A generation is the revision that issued it: unique across every
    // incarnation of a name, so a token of a retired consumer never matches
    // the row that took its name.
    let generation = state.revision;
    match operation {
        CursorOperation::Register {
            consumer,
            scope,
            filter,
            start,
            expires_at,
        } => {
            validate_lease(state.clock, *expires_at, config)?;
            start.validate(state.ledger, published)?;
            if start.retention_prefix() < state.floor {
                return Err(StreamError::ResyncRequired(ResyncReason::HistoryExpired));
            }
            if state.consumers.contains_key(consumer) {
                return Err(StreamError::DuplicateConsumer);
            }
            admit_consumer(state, config, retired)?;
            state.consumers.insert(
                *consumer,
                CursorRecord {
                    token: CursorToken {
                        key: ConsumerKey {
                            ledger: state.ledger,
                            consumer: *consumer,
                        },
                        generation,
                        scope: *scope,
                        position: *start,
                    },
                    filter: filter.clone(),
                    expires_at: *expires_at,
                    mode: CursorMode::Live,
                },
            );
        }
        CursorOperation::Acknowledge { token } => {
            token.position.validate(state.ledger, published)?;
            let row = active_row(state, token.key.consumer, token.generation)?;
            row.token.same_stream(*token)?;
            if !matches!(row.mode, CursorMode::Live | CursorMode::Protected) {
                return Err(StreamError::SeedNotComplete);
            }
            if token.position < row.token.position {
                return Err(StreamError::CursorRegression);
            }
            row.token = *token;
        }
        CursorOperation::AcknowledgeAndRenew { token, expires_at } => {
            validate_lease(state.clock, *expires_at, config)?;
            token.position.validate(state.ledger, published)?;
            let row = active_row(state, token.key.consumer, token.generation)?;
            row.token.same_stream(*token)?;
            if row.mode == CursorMode::Protected {
                return Err(StreamError::Invalid("protected consumers do not expire"));
            }
            if row.mode != CursorMode::Live {
                return Err(StreamError::SeedNotComplete);
            }
            if token.position < row.token.position {
                return Err(StreamError::CursorRegression);
            }
            row.token = *token;
            row.expires_at = *expires_at;
        }
        CursorOperation::Renew {
            consumer,
            generation,
            expires_at,
        } => {
            validate_lease(state.clock, *expires_at, config)?;
            let row = active_row(state, *consumer, *generation)?;
            if row.mode == CursorMode::Protected {
                return Err(StreamError::Invalid("protected consumers do not expire"));
            }
            row.expires_at = *expires_at;
        }
        CursorOperation::BeginSeed {
            consumer,
            scope,
            filter,
            snapshot,
            expires_at,
        } => {
            validate_lease(state.clock, *expires_at, config)?;
            if *snapshot < state.floor {
                return Err(StreamError::ResyncRequired(ResyncReason::HistoryExpired));
            }
            if *snapshot > published {
                return Err(StreamError::CursorAhead);
            }
            match state.consumers.get(consumer) {
                Some(old) if old.mode == CursorMode::Protected => {
                    return Err(StreamError::Invalid(
                        "protected consumer cannot skip history by reseeding",
                    ));
                }
                Some(_) => {}
                None => admit_consumer(state, config, retired)?,
            }
            state.consumers.insert(
                *consumer,
                CursorRecord {
                    token: CursorToken {
                        key: ConsumerKey {
                            ledger: state.ledger,
                            consumer: *consumer,
                        },
                        generation,
                        scope: *scope,
                        position: Position::resolved(state.ledger, *snapshot),
                    },
                    filter: filter.clone(),
                    expires_at: *expires_at,
                    mode: CursorMode::Seeding {
                        snapshot: *snapshot,
                    },
                },
            );
        }
        CursorOperation::CompleteSeed {
            consumer,
            generation,
            snapshot,
        } => {
            let row = active_row(state, *consumer, *generation)?;
            if row.mode
                != (CursorMode::Seeding {
                    snapshot: *snapshot,
                })
            {
                return Err(StreamError::Invalid("seed prefix or phase mismatch"));
            }
            row.mode = CursorMode::Live;
        }
        CursorOperation::RequireResync {
            consumer,
            generation,
            reason,
        } => {
            let row = state
                .consumers
                .get_mut(consumer)
                .ok_or(StreamError::MissingConsumer)?;
            if row.token.generation != *generation {
                return Err(StreamError::WrongGeneration);
            }
            if row.mode == CursorMode::Protected {
                return Err(StreamError::Invalid(
                    "protected consumer cannot release retention through resync",
                ));
            }
            row.mode = CursorMode::Resync { reason: *reason };
        }
        CursorOperation::AdvanceFloor { through } => {
            if *through < state.floor {
                return Err(StreamError::CursorRegression);
            }
            if *through > published {
                return Err(StreamError::CursorAhead);
            }
            let allowed = retention_limit(state, published);
            if *through > allowed {
                return Err(StreamError::RetentionPinned {
                    allowed_through: allowed,
                });
            }
            state.floor = *through;
        }
        CursorOperation::RegisterProtected {
            consumer,
            scope,
            filter,
            start,
        } => {
            start.validate(state.ledger, published)?;
            if start.retention_prefix() < state.floor {
                return Err(StreamError::ResyncRequired(ResyncReason::HistoryExpired));
            }
            if state.consumers.contains_key(consumer) {
                return Err(StreamError::DuplicateConsumer);
            }
            admit_consumer(state, config, retired)?;
            state.consumers.insert(
                *consumer,
                CursorRecord {
                    token: CursorToken {
                        key: ConsumerKey {
                            ledger: state.ledger,
                            consumer: *consumer,
                        },
                        generation,
                        scope: *scope,
                        position: *start,
                    },
                    filter: filter.clone(),
                    expires_at: u64::MAX,
                    mode: CursorMode::Protected,
                },
            );
        }
    }
    Ok(())
}

fn active_row(
    state: &mut CursorCheckpoint,
    consumer: ConsumerId,
    generation: u64,
) -> Result<&mut CursorRecord, StreamError> {
    let row = state
        .consumers
        .get_mut(&consumer)
        .ok_or(StreamError::MissingConsumer)?;
    if row.token.generation != generation {
        return Err(StreamError::WrongGeneration);
    }
    if row.mode != CursorMode::Protected && row.expires_at <= state.clock {
        return Err(StreamError::ResyncRequired(ResyncReason::LeaseExpired));
    }
    if let CursorMode::Resync { reason } = row.mode {
        return Err(StreamError::ResyncRequired(reason));
    }
    Ok(row)
}

fn validate_lease(now: u64, expires_at: u64, config: RegistryConfig) -> Result<(), StreamError> {
    if expires_at <= now || expires_at.saturating_sub(now) > config.max_lease_ttl {
        return Err(StreamError::Invalid("cursor lease outside bounds"));
    }
    Ok(())
}

fn retention_limit(state: &CursorCheckpoint, published: SessionSeq) -> SessionSeq {
    state
        .consumers
        .values()
        .filter(|row| {
            row.mode == CursorMode::Protected
                || row.expires_at > state.clock && !matches!(row.mode, CursorMode::Resync { .. })
        })
        .map(|row| row.token.position.retention_prefix())
        .min()
        .unwrap_or(published)
        .min(published)
}

fn validate_checkpoint(
    state: &CursorCheckpoint,
    published: SessionSeq,
    config: RegistryConfig,
) -> Result<(), StreamError> {
    if state.schema != 1 {
        return Err(StreamError::Invalid("unknown cursor checkpoint schema"));
    }
    if state.floor > published {
        return Err(StreamError::CursorAhead);
    }
    if state.consumers.len() > config.max_consumers {
        return Err(StreamError::Capacity);
    }
    for (id, row) in &state.consumers {
        if row.token.key.ledger != state.ledger || row.token.position.ledger != state.ledger {
            return Err(StreamError::WrongLedger);
        }
        if row.token.key.consumer != *id {
            return Err(StreamError::WrongConsumer);
        }
        if row.token.generation == 0 || row.token.generation > state.revision {
            return Err(StreamError::WrongGeneration);
        }
        row.token.position.validate(state.ledger, published)?;
        if row.filter.len() > config.max_filter_claims {
            return Err(StreamError::Capacity);
        }
        if row.mode == CursorMode::Protected && row.expires_at != u64::MAX {
            return Err(StreamError::Invalid("protected consumer expiry sentinel"));
        }
        if row.mode != CursorMode::Protected
            && row.expires_at > state.clock
            && row.expires_at.saturating_sub(state.clock) > config.max_lease_ttl
        {
            return Err(StreamError::Invalid("persisted cursor lease exceeds bound"));
        }
        if !matches!(row.mode, CursorMode::Resync { .. })
            && (row.mode == CursorMode::Protected || row.expires_at > state.clock)
            && row.token.position.retention_prefix() < state.floor
        {
            return Err(StreamError::Invalid(
                "live cursor lies below retained history",
            ));
        }
        if let CursorMode::Seeding { snapshot } = row.mode
            && row.token.position != Position::resolved(state.ledger, snapshot)
        {
            return Err(StreamError::Invalid("seed cursor is not at its snapshot"));
        }
    }
    Ok(())
}

fn row_charge() -> usize {
    size_of::<(ConsumerId, CursorRecord)>()
        .saturating_add(size_of::<usize>())
        .saturating_mul(16)
        .saturating_add(ALLOCATOR_OVERHEAD)
}
fn checkpoint_charge(checkpoint: &CursorCheckpoint) -> Result<usize, StreamError> {
    let mut charge = add(
        add(size_of::<Root>(), ALLOCATOR_OVERHEAD)?,
        mul(checkpoint.consumers.len(), row_charge())?,
    )?;
    for row in checkpoint.consumers.values() {
        charge = add(charge, row.filter.charge()?)?;
    }
    Ok(charge)
}
