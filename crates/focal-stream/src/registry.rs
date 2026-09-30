use crate::{
    ALLOCATOR_OVERHEAD, ConsumerId, ConsumerKey, CursorToken, DeltaFilter, Position, ResyncReason,
    StreamError, add,
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
    /// The bytes the checkpoint holds, by the lane that admitted them:
    /// registrations under the ordinary lane, everything else — the restored
    /// checkpoint, seeds, protected consumers — under the completion
    /// allowance. Together they are exactly `checkpoint_charge`; rows that
    /// leave return their bytes to the ordinary lane first.
    ordinary: Allocation,
    completion: Allocation,
}

/// This state becomes durable only when the embedding log commits its command
/// or checkpoint. Call prepare before that decision and publish afterward.
pub struct CursorRegistry {
    root: Root,
    owner: focal_memory::OwnerId,
    budget: MemoryBudget,
    config: RegistryConfig,
}
/// What one command changes: at most one row, named — never a copy of the
/// registry (the audit's F61). A renewal or an acknowledgment patches the
/// row's scalars in place at publication and copies nothing, not even its
/// filter; a registration or a seed carries its one row.
enum RowChange {
    None,
    Patch {
        consumer: ConsumerId,
        token: Option<CursorToken>,
        expires_at: Option<u64>,
        mode: Option<CursorMode>,
    },
    Insert {
        consumer: ConsumerId,
        row: CursorRecord,
    },
    Replace {
        consumer: ConsumerId,
        row: CursorRecord,
    },
}
pub struct PreparedCursorUpdate {
    owner: focal_memory::OwnerId,
    base_revision: u64,
    revision: u64,
    clock: u64,
    floor: SessionSeq,
    change: RowChange,
    /// The consumers this update retired: released rows (an ordinary lease
    /// expired, or a cursor sent to resync) whose slot a registration needed.
    /// Their names are free; whatever the embedding keeps per consumer
    /// follows them out.
    retired: Vec<ConsumerId>,
    /// The bytes the rows that leave — retired, or replaced — hold.
    leaving: usize,
    /// The arriving row's bytes, admitted under the command's lane before
    /// the row was built; nothing for a patch.
    lane: BudgetLane,
    arriving: Allocation,
}
impl PreparedCursorUpdate {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn clock(&self) -> u64 {
        self.clock
    }
    pub fn floor(&self) -> SessionSeq {
        self.floor
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
        let completion = budget
            .reserve(
                BudgetKind::ReadPins,
                BudgetLane::Completion,
                checkpoint_charge(&checkpoint)?,
            )?
            .commit();
        let ordinary = budget
            .reserve(BudgetKind::ReadPins, BudgetLane::Ordinary, 0)?
            .commit();
        Ok(Self {
            owner: focal_memory::OwnerId::new()?,
            root: Root {
                checkpoint,
                ordinary,
                completion,
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
        let state = &self.root.checkpoint;
        retention_limit(state, state.clock, published)
    }
    /// The bytes the registry holds for its rows.
    pub fn resident_bytes(&self) -> usize {
        self.root
            .ordinary
            .bytes()
            .saturating_add(self.root.completion.bytes())
    }
    /// `consumer`'s row as it stands once `prepared` is published: the row
    /// the update inserts or replaces, the current row under its patch, or
    /// the current row when the update leaves it alone; nothing for a
    /// consumer the update retires.
    pub fn projected(
        &self,
        prepared: &PreparedCursorUpdate,
        consumer: ConsumerId,
    ) -> Option<CursorRecord> {
        if prepared.retired.contains(&consumer) {
            return None;
        }
        match &prepared.change {
            RowChange::Insert {
                consumer: changed,
                row,
            }
            | RowChange::Replace {
                consumer: changed,
                row,
            } if *changed == consumer => Some(row.clone()),
            RowChange::Patch {
                consumer: changed,
                token,
                expires_at,
                mode,
            } if *changed == consumer => {
                let mut row = self.get(consumer)?.clone();
                patch(&mut row, *token, *expires_at, mode.clone());
                Some(row)
            }
            _ => self.get(consumer).cloned(),
        }
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
        let revision = old
            .revision
            .checked_add(1)
            .ok_or(StreamError::CounterExhausted)?;
        // The one row a registration or a seed brings is admitted before it
        // is built; every other command builds nothing.
        let arriving_bytes = match &command.operation {
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
        let arriving = self
            .budget
            .reserve(BudgetKind::ReadPins, lane, arriving_bytes)?
            .commit();
        let mut retired = Vec::new();
        let header = Header {
            revision,
            clock: command.now,
            floor: old.floor,
        };
        let applied = apply_operation(
            old,
            &header,
            &command.operation,
            published,
            self.config,
            &mut retired,
        )?;
        Ok(PreparedCursorUpdate {
            owner: self.owner,
            base_revision: old.revision,
            revision,
            clock: command.now,
            floor: applied.floor,
            change: applied.change,
            retired,
            leaving: applied.leaving,
            lane,
            arriving,
        })
    }

    /// The decision is durable: the update's one row is written, the rows
    /// it retires leave — their names handed back, so whatever the embedding
    /// keeps per consumer follows — and the registry's charge follows; no
    /// copy, no validation and no refusal past this point, except a
    /// preparation the registry moved on from.
    pub fn publish(
        &mut self,
        mut prepared: PreparedCursorUpdate,
    ) -> Result<Vec<ConsumerId>, StreamError> {
        if self.owner != prepared.owner || self.revision() != prepared.base_revision {
            return Err(StreamError::StalePreparation);
        }
        if let RowChange::Patch { consumer, .. } = &prepared.change
            && !self.root.checkpoint.consumers.contains_key(consumer)
        {
            return Err(StreamError::StalePreparation);
        }
        // The charge first, so a state that publishes is a state that is
        // accounted: the arriving row joins its lane, the leaving rows
        // return theirs, ordinary first.
        match prepared.lane {
            BudgetLane::Ordinary => self.root.ordinary.absorb(&mut prepared.arriving)?,
            BudgetLane::Completion => self.root.completion.absorb(&mut prepared.arriving)?,
        }
        let from_ordinary = prepared.leaving.min(self.root.ordinary.bytes());
        let from_completion = prepared.leaving.saturating_sub(from_ordinary);
        let completion_left = self
            .root
            .completion
            .bytes()
            .checked_sub(from_completion)
            .ok_or(StreamError::Invalid(
                "cursor registry charge below its rows",
            ))?;
        self.root
            .ordinary
            .shrink_to(self.root.ordinary.bytes().saturating_sub(from_ordinary))?;
        self.root.completion.shrink_to(completion_left)?;
        let state = &mut self.root.checkpoint;
        state.revision = prepared.revision;
        state.clock = prepared.clock;
        state.floor = prepared.floor;
        for consumer in &prepared.retired {
            state.consumers.remove(consumer);
        }
        match prepared.change {
            RowChange::None => {}
            RowChange::Patch {
                consumer,
                token,
                expires_at,
                mode,
            } => {
                if let Some(row) = state.consumers.get_mut(&consumer) {
                    patch(row, token, expires_at, mode);
                }
            }
            RowChange::Insert { consumer, row } | RowChange::Replace { consumer, row } => {
                state.consumers.insert(consumer, row);
            }
        }
        Ok(prepared.retired)
    }

    /// Replay a command already committed in the authoritative cursor log.
    /// This does not itself provide persistence or acknowledge a network peer.
    pub fn replay_committed(
        &mut self,
        command: &CursorCommand,
        published: SessionSeq,
    ) -> Result<(), StreamError> {
        let prepared = self.prepare_committed(command, published)?;
        self.publish(prepared).map(drop)
    }
}

fn patch(
    row: &mut CursorRecord,
    token: Option<CursorToken>,
    expires_at: Option<u64>,
    mode: Option<CursorMode>,
) {
    if let Some(token) = token {
        row.token = token;
    }
    if let Some(expires_at) = expires_at {
        row.expires_at = expires_at;
    }
    if let Some(mode) = mode {
        row.mode = mode;
    }
}

/// The checkpoint's scalars as the command leaves them, for the rows it
/// validates against.
struct Header {
    revision: u64,
    clock: u64,
    floor: SessionSeq,
}
struct Applied {
    change: RowChange,
    floor: SessionSeq,
    leaving: usize,
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
/// Name every released row, so it leaves at publication and the embedding's
/// per-consumer state follows; the bytes they hold. A retired name registers
/// again under a generation no earlier token carries (generations are the
/// registry's revisions), so nothing stale can move or renew the new
/// incarnation.
fn retire_released(
    state: &CursorCheckpoint,
    clock: u64,
    retired: &mut Vec<ConsumerId>,
) -> Result<usize, StreamError> {
    let count = state
        .consumers
        .values()
        .filter(|row| released(row, clock))
        .count();
    if count == 0 {
        return Ok(0);
    }
    retired
        .try_reserve(count)
        .map_err(|_| StreamError::Capacity)?;
    let mut leaving = 0;
    for (id, row) in &state.consumers {
        if released(row, clock) {
            retired.push(*id);
            leaving = add(leaving, record_charge(row)?)?;
        }
    }
    Ok(leaving)
}
/// Room for one more consumer: the bound, after the released rows leave.
fn admit_consumer(
    state: &CursorCheckpoint,
    clock: u64,
    config: RegistryConfig,
    retired: &mut Vec<ConsumerId>,
) -> Result<usize, StreamError> {
    let mut leaving = 0;
    if state.consumers.len() >= config.max_consumers {
        leaving = retire_released(state, clock, retired)?;
    }
    if state.consumers.len().saturating_sub(retired.len()) >= config.max_consumers {
        return Err(StreamError::Capacity);
    }
    Ok(leaving)
}

fn apply_operation(
    state: &CursorCheckpoint,
    header: &Header,
    operation: &CursorOperation,
    published: SessionSeq,
    config: RegistryConfig,
    retired: &mut Vec<ConsumerId>,
) -> Result<Applied, StreamError> {
    // A generation is the revision that issued it: unique across every
    // incarnation of a name, so a token of a retired consumer never matches
    // the row that took its name.
    let generation = header.revision;
    let clock = header.clock;
    let mut floor = header.floor;
    let mut leaving = 0;
    let change = match operation {
        CursorOperation::Register {
            consumer,
            scope,
            filter,
            start,
            expires_at,
        } => {
            validate_lease(clock, *expires_at, config)?;
            start.validate(state.ledger, published)?;
            if start.retention_prefix() < state.floor {
                return Err(StreamError::ResyncRequired(ResyncReason::HistoryExpired));
            }
            if state.consumers.contains_key(consumer) {
                return Err(StreamError::DuplicateConsumer);
            }
            leaving = admit_consumer(state, clock, config, retired)?;
            RowChange::Insert {
                consumer: *consumer,
                row: CursorRecord {
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
            }
        }
        CursorOperation::Acknowledge { token } => {
            token.position.validate(state.ledger, published)?;
            let row = active_row(state, clock, token.key.consumer, token.generation)?;
            row.token.same_stream(*token)?;
            if !matches!(row.mode, CursorMode::Live | CursorMode::Protected) {
                return Err(StreamError::SeedNotComplete);
            }
            if token.position < row.token.position {
                return Err(StreamError::CursorRegression);
            }
            RowChange::Patch {
                consumer: token.key.consumer,
                token: Some(*token),
                expires_at: None,
                mode: None,
            }
        }
        CursorOperation::AcknowledgeAndRenew { token, expires_at } => {
            validate_lease(clock, *expires_at, config)?;
            token.position.validate(state.ledger, published)?;
            let row = active_row(state, clock, token.key.consumer, token.generation)?;
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
            RowChange::Patch {
                consumer: token.key.consumer,
                token: Some(*token),
                expires_at: Some(*expires_at),
                mode: None,
            }
        }
        CursorOperation::Renew {
            consumer,
            generation,
            expires_at,
        } => {
            validate_lease(clock, *expires_at, config)?;
            let row = active_row(state, clock, *consumer, *generation)?;
            if row.mode == CursorMode::Protected {
                return Err(StreamError::Invalid("protected consumers do not expire"));
            }
            RowChange::Patch {
                consumer: *consumer,
                token: None,
                expires_at: Some(*expires_at),
                mode: None,
            }
        }
        CursorOperation::BeginSeed {
            consumer,
            scope,
            filter,
            snapshot,
            expires_at,
        } => {
            validate_lease(clock, *expires_at, config)?;
            if *snapshot < state.floor {
                return Err(StreamError::ResyncRequired(ResyncReason::HistoryExpired));
            }
            if *snapshot > published {
                return Err(StreamError::CursorAhead);
            }
            let replaced = match state.consumers.get(consumer) {
                Some(old) if old.mode == CursorMode::Protected => {
                    return Err(StreamError::Invalid(
                        "protected consumer cannot skip history by reseeding",
                    ));
                }
                Some(old) => {
                    leaving = record_charge(old)?;
                    true
                }
                None => {
                    leaving = admit_consumer(state, clock, config, retired)?;
                    false
                }
            };
            let row = CursorRecord {
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
            };
            if replaced {
                RowChange::Replace {
                    consumer: *consumer,
                    row,
                }
            } else {
                RowChange::Insert {
                    consumer: *consumer,
                    row,
                }
            }
        }
        CursorOperation::CompleteSeed {
            consumer,
            generation,
            snapshot,
        } => {
            let row = active_row(state, clock, *consumer, *generation)?;
            if row.mode
                != (CursorMode::Seeding {
                    snapshot: *snapshot,
                })
            {
                return Err(StreamError::Invalid("seed prefix or phase mismatch"));
            }
            RowChange::Patch {
                consumer: *consumer,
                token: None,
                expires_at: None,
                mode: Some(CursorMode::Live),
            }
        }
        CursorOperation::RequireResync {
            consumer,
            generation,
            reason,
        } => {
            let row = state
                .consumers
                .get(consumer)
                .ok_or(StreamError::MissingConsumer)?;
            if row.token.generation != *generation {
                return Err(StreamError::WrongGeneration);
            }
            if row.mode == CursorMode::Protected {
                return Err(StreamError::Invalid(
                    "protected consumer cannot release retention through resync",
                ));
            }
            RowChange::Patch {
                consumer: *consumer,
                token: None,
                expires_at: None,
                mode: Some(CursorMode::Resync { reason: *reason }),
            }
        }
        CursorOperation::AdvanceFloor { through } => {
            if *through < state.floor {
                return Err(StreamError::CursorRegression);
            }
            if *through > published {
                return Err(StreamError::CursorAhead);
            }
            let allowed = retention_limit(state, clock, published);
            if *through > allowed {
                return Err(StreamError::RetentionPinned {
                    allowed_through: allowed,
                });
            }
            floor = *through;
            RowChange::None
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
            leaving = admit_consumer(state, clock, config, retired)?;
            RowChange::Insert {
                consumer: *consumer,
                row: CursorRecord {
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
            }
        }
    };
    // The one row the command touches is validated as it will stand; the
    // clock only ever advances, which expires rows and never revives one,
    // and the floor moves only under the retention limit — so no untouched
    // row's invariant can change.
    let bounds = Header {
        revision: header.revision,
        clock,
        floor,
    };
    match &change {
        RowChange::None => {}
        RowChange::Insert { consumer, row } | RowChange::Replace { consumer, row } => {
            validate_row(
                state.ledger,
                &bounds,
                *consumer,
                row.token,
                &row.mode,
                row.expires_at,
                row.filter.len(),
                published,
                config,
            )?;
        }
        RowChange::Patch {
            consumer,
            token,
            expires_at,
            mode,
        } => {
            let row = state
                .consumers
                .get(consumer)
                .ok_or(StreamError::MissingConsumer)?;
            validate_row(
                state.ledger,
                &bounds,
                *consumer,
                token.unwrap_or(row.token),
                mode.as_ref().unwrap_or(&row.mode),
                expires_at.unwrap_or(row.expires_at),
                row.filter.len(),
                published,
                config,
            )?;
        }
    }
    Ok(Applied {
        change,
        floor,
        leaving,
    })
}

fn active_row(
    state: &CursorCheckpoint,
    clock: u64,
    consumer: ConsumerId,
    generation: u64,
) -> Result<&CursorRecord, StreamError> {
    let row = state
        .consumers
        .get(&consumer)
        .ok_or(StreamError::MissingConsumer)?;
    if row.token.generation != generation {
        return Err(StreamError::WrongGeneration);
    }
    if row.mode != CursorMode::Protected && row.expires_at <= clock {
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

fn retention_limit(state: &CursorCheckpoint, clock: u64, published: SessionSeq) -> SessionSeq {
    state
        .consumers
        .values()
        .filter(|row| {
            row.mode == CursorMode::Protected
                || row.expires_at > clock && !matches!(row.mode, CursorMode::Resync { .. })
        })
        .map(|row| row.token.position.retention_prefix())
        .min()
        .unwrap_or(published)
        .min(published)
}

/// One row's invariants against the checkpoint's scalars: at restore for
/// every row, at preparation for the row the command leaves behind.
#[allow(clippy::too_many_arguments)]
fn validate_row(
    ledger: LedgerId,
    header: &Header,
    id: ConsumerId,
    token: CursorToken,
    mode: &CursorMode,
    expires_at: u64,
    filter_len: usize,
    published: SessionSeq,
    config: RegistryConfig,
) -> Result<(), StreamError> {
    if token.key.ledger != ledger || token.position.ledger != ledger {
        return Err(StreamError::WrongLedger);
    }
    if token.key.consumer != id {
        return Err(StreamError::WrongConsumer);
    }
    if token.generation == 0 || token.generation > header.revision {
        return Err(StreamError::WrongGeneration);
    }
    token.position.validate(ledger, published)?;
    if filter_len > config.max_filter_claims {
        return Err(StreamError::Capacity);
    }
    if *mode == CursorMode::Protected && expires_at != u64::MAX {
        return Err(StreamError::Invalid("protected consumer expiry sentinel"));
    }
    if *mode != CursorMode::Protected
        && expires_at > header.clock
        && expires_at.saturating_sub(header.clock) > config.max_lease_ttl
    {
        return Err(StreamError::Invalid("persisted cursor lease exceeds bound"));
    }
    if !matches!(mode, CursorMode::Resync { .. })
        && (*mode == CursorMode::Protected || expires_at > header.clock)
        && token.position.retention_prefix() < header.floor
    {
        return Err(StreamError::Invalid(
            "live cursor lies below retained history",
        ));
    }
    if let CursorMode::Seeding { snapshot } = mode
        && token.position != Position::resolved(ledger, *snapshot)
    {
        return Err(StreamError::Invalid("seed cursor is not at its snapshot"));
    }
    Ok(())
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
    let header = Header {
        revision: state.revision,
        clock: state.clock,
        floor: state.floor,
    };
    for (id, row) in &state.consumers {
        validate_row(
            state.ledger,
            &header,
            *id,
            row.token,
            &row.mode,
            row.expires_at,
            row.filter.len(),
            published,
            config,
        )?;
    }
    Ok(())
}

fn row_charge() -> usize {
    size_of::<(ConsumerId, CursorRecord)>()
        .saturating_add(size_of::<usize>())
        .saturating_mul(16)
        .saturating_add(ALLOCATOR_OVERHEAD)
}
/// What one row holds: its slot and its filter.
fn record_charge(row: &CursorRecord) -> Result<usize, StreamError> {
    add(row_charge(), row.filter.charge()?)
}
fn checkpoint_charge(checkpoint: &CursorCheckpoint) -> Result<usize, StreamError> {
    let mut charge = add(size_of::<Root>(), ALLOCATOR_OVERHEAD)?;
    for row in checkpoint.consumers.values() {
        charge = add(charge, record_charge(row)?)?;
    }
    Ok(charge)
}
