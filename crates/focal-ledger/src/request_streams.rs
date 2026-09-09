//! One bounded receipt namespace shared by managed domain and cursor work.
//! Candidates own one stream's replacement state; publication never allocates.
use crate::session::LedgerError;
use focal_graph::reference_charge;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub struct RequestStreamLimits {
    /// Total remembered (principal, slot) pairs. Closed generations remain fenced.
    pub max_slots: usize,
    pub max_window: u32,
    pub max_slot_bytes: usize,
}
impl Default for RequestStreamLimits {
    fn default() -> Self {
        Self {
            max_slots: 64,
            max_window: 256,
            max_slot_bytes: 8 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ManagedError {
    #[error("managed request identity is invalid or outside this owner")]
    InvalidIdentity,
    #[error("managed stream is not registered")]
    NotRegistered,
    #[error("managed request or control identity conflicts with retained state")]
    Conflict,
    #[error("managed stream generation is closed through {generation}")]
    Closed { generation: u64 },
    #[error("managed receipt was retired through ordinal {through}")]
    Retired { through: u64 },
    #[error("managed request window or registry capacity exceeded")]
    Capacity,
    #[error("not every voter has authenticated support for this managed format")]
    Unsupported,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StreamSlotData {
    pub principal: ParticipantId,
    pub state: RequestStreamState,
    pub latest: Option<RequestStreamControlReceipt>,
    pub rows: Vec<ManagedReceipt>,
}
impl StreamSlotData {
    fn slot(&self) -> u32 {
        match self.state {
            RequestStreamState::Vacant { slot, .. } => slot,
            RequestStreamState::Active { stream, .. } => stream.slot,
        }
    }
    fn generation(&self) -> u64 {
        match self.state {
            RequestStreamState::Vacant { generation, .. } => generation,
            RequestStreamState::Active { stream, .. } => stream.generation,
        }
    }
    fn charge(&self) -> Result<usize, LedgerError> {
        reference_charge(self)?
            .checked_add(
                self.rows
                    .capacity()
                    .checked_mul(size_of::<ManagedReceipt>())
                    .ok_or(LedgerError::Capacity)?,
            )
            .ok_or(LedgerError::Capacity)
    }
    fn stamp(&self) -> (u64, u64) {
        (
            self.generation(),
            self.rows
                .iter()
                .map(|r| r.raft_index)
                .chain(self.latest.iter().map(|r| r.raft_index))
                .max()
                .unwrap_or(0),
        )
    }
    fn advance_revision(&mut self) -> Result<u64, ManagedError> {
        match &mut self.state {
            RequestStreamState::Active { revision, .. } => {
                *revision = revision.checked_add(1).ok_or(ManagedError::Capacity)?;
                Ok(*revision)
            }
            _ => Err(ManagedError::NotRegistered),
        }
    }
}
struct StreamSlot {
    data: StreamSlotData,
    _charge: Allocation,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct RequestStreamsCheckpoint {
    pub activated: bool,
    pub slots: Vec<StreamSlotData>,
}
pub(crate) struct RequestStreams {
    pub activated: bool,
    cluster: [u8; 16],
    ledger: LedgerId,
    limits: RequestStreamLimits,
    slots: Vec<StreamSlot>,
    /// The largest generation ever assigned by this registry (15 §"Session
    /// owner and controls"): every registration assigns above it, so a
    /// remembered pair evicted at capacity can never see one of its old
    /// generations reused by a later registration.
    next_generation: u64,
    _slots_charge: Option<Allocation>,
}
pub(crate) struct PreparedStream {
    position: usize,
    replacing: bool,
    base: Option<(u64, u64)>,
    /// A closed, vacant pair this publication evicts to make room for a new
    /// pair at capacity: its index and stamp, checked again at publication.
    evict: Option<(usize, (u64, u64))>,
    /// The generation a registration assigned; publication raises the
    /// registry watermark to it.
    assigned: Option<u64>,
    data: StreamSlotData,
    charge: Allocation,
}
impl PreparedStream {
    pub fn control_receipt(&self) -> Option<&RequestStreamControlReceipt> {
        self.data.latest.as_ref()
    }
    pub fn receipt(&self, key: &ManagedRequestKey) -> Option<&ManagedReceipt> {
        self.data.rows.iter().find(|row| row.key == *key)
    }
    /// A cursor receipt names the published end of the ledger's stream line
    /// at apply (23 §6); like the index it is fixed only while publishing the
    /// committed entry, identically on every replica.
    pub fn set_cursor_sequence(&mut self, sequence: SessionSeq) {
        for row in &mut self.data.rows {
            if row.raft_index == 0 && matches!(row.outcome, ManagedReceiptOutcome::Cursor { .. }) {
                row.sequence = sequence;
            }
        }
    }
    /// Index is unknown during proposal. It becomes part of the immutable result
    /// only while publishing the corresponding durably committed entry.
    pub fn set_index(&mut self, index: u64) -> Result<(), LedgerError> {
        if index == 0 {
            return Err(LedgerError::Corrupt);
        }
        for row in &mut self.data.rows {
            if row.raft_index == 0 {
                row.raft_index = index;
            }
        }
        if let Some(receipt) = &mut self.data.latest
            && receipt.raft_index == 0
        {
            receipt.raft_index = index;
            if let RequestStreamControlOutcome::Sealed(row) = &mut receipt.outcome
                && row.raft_index == 0
            {
                row.raft_index = index;
            }
        }
        Ok(())
    }
}
impl RequestStreams {
    pub fn new(
        cluster: [u8; 16],
        ledger: LedgerId,
        limits: RequestStreamLimits,
    ) -> Result<Self, LedgerError> {
        if cluster == [0; 16]
            || limits.max_slots == 0
            || limits.max_window == 0
            || limits.max_slot_bytes < 4096
        {
            return Err(ManagedError::Capacity.into());
        }
        Ok(Self {
            activated: false,
            cluster,
            ledger,
            limits,
            slots: Vec::new(),
            next_generation: 0,
            _slots_charge: None,
        })
    }
    /// The registry's generation watermark, persisted with the checkpoint.
    pub(crate) fn next_generation(&self) -> u64 {
        self.next_generation
    }
    fn locate(&self, principal: ParticipantId, slot: u32) -> Result<usize, usize> {
        self.slots.binary_search_by_key(&(principal, slot), |row| {
            (row.data.principal, row.data.slot())
        })
    }
    fn validate_scope(&self, principal: ParticipantId) -> Result<(), ManagedError> {
        if principal.is_zero() {
            Err(ManagedError::InvalidIdentity)
        } else {
            Ok(())
        }
    }
    pub fn validate_stream(&self, stream: &RequestStreamIdentity) -> Result<(), ManagedError> {
        if !stream.is_valid() || stream.cluster != self.cluster || stream.ledger != self.ledger {
            Err(ManagedError::InvalidIdentity)
        } else {
            Ok(())
        }
    }
    pub fn state(
        &self,
        principal: ParticipantId,
        slot: u32,
    ) -> Result<RequestStreamState, ManagedError> {
        self.validate_scope(principal)?;
        Ok(self.presented(
            self.locate(principal, slot)
                .ok()
                .and_then(|i| self.slots.get(i))
                .map_or(
                    RequestStreamState::Vacant {
                        slot,
                        generation: 0,
                    },
                    |row| row.data.state,
                ),
        ))
    }
    /// The state a principal observes. A vacant pair presents the registry
    /// watermark rather than its own last generation, so the next
    /// registration on it is assigned exactly the presented generation plus
    /// one and still lies above every generation this registry ever issued:
    /// an evicted pair's delayed traffic can never match a reused stream.
    fn presented(&self, state: RequestStreamState) -> RequestStreamState {
        match state {
            RequestStreamState::Vacant { slot, generation } => RequestStreamState::Vacant {
                slot,
                generation: generation.max(self.next_generation),
            },
            active => active,
        }
    }
    fn active(&self, stream: &RequestStreamIdentity) -> Result<&StreamSlotData, ManagedError> {
        self.validate_stream(stream)?;
        let row = self
            .locate(stream.principal, stream.slot)
            .ok()
            .and_then(|i| self.slots.get(i))
            .ok_or(ManagedError::NotRegistered)?;
        match row.data.state {
            RequestStreamState::Active { stream: actual, .. } if actual == *stream => Ok(&row.data),
            RequestStreamState::Active { stream: actual, .. }
                if stream.generation < actual.generation =>
            {
                Err(ManagedError::Closed {
                    generation: actual.generation.saturating_sub(1),
                })
            }
            RequestStreamState::Vacant { generation, .. } if stream.generation <= generation => {
                Err(ManagedError::Closed { generation })
            }
            _ => Err(ManagedError::NotRegistered),
        }
    }
    pub fn lookup(&self, key: &ManagedRequestKey) -> Result<Option<&ManagedReceipt>, ManagedError> {
        if !key.is_valid() {
            return Err(ManagedError::InvalidIdentity);
        }
        let row = self.active(&key.stream)?;
        let RequestStreamState::Active {
            acknowledged_through,
            window,
            ..
        } = row.state
        else {
            return Err(ManagedError::NotRegistered);
        };
        if key.ordinal <= acknowledged_through {
            return Err(ManagedError::Retired {
                through: acknowledged_through,
            });
        }
        if key.ordinal > acknowledged_through.saturating_add(u64::from(window)) {
            return Err(ManagedError::Capacity);
        }
        match row
            .rows
            .binary_search_by_key(&key.ordinal, |row| row.key.ordinal)
            .ok()
            .and_then(|i| row.rows.get(i))
        {
            Some(receipt) if receipt.key != *key => Err(ManagedError::Conflict),
            other => Ok(other),
        }
    }
    pub fn exact(
        &self,
        key: &ManagedRequestKey,
        intent: ContentHash,
        family: ManagedRequestFamily,
    ) -> Result<Option<&ManagedReceipt>, ManagedError> {
        let result = self.lookup(key)?;
        if let Some(receipt) = result
            && (receipt.intent_hash != intent || receipt_family(receipt) != family)
        {
            return Err(ManagedError::Conflict);
        }
        Ok(result)
    }
    fn input_slot(&self, input: &RequestStreamControlInput) -> Result<u32, ManagedError> {
        self.input_slot_parts(
            input.cluster,
            input.ledger,
            input.principal,
            input.id,
            &input.command,
        )
    }
    fn input_slot_parts(
        &self,
        cluster: [u8; 16],
        ledger: LedgerId,
        principal: ParticipantId,
        id: RequestId,
        command: &RequestStreamCommand,
    ) -> Result<u32, ManagedError> {
        if cluster != self.cluster || ledger != self.ledger || principal.is_zero() || id.is_zero() {
            return Err(ManagedError::InvalidIdentity);
        }
        match command {
            RequestStreamCommand::Register { slot, .. } => Ok(*slot),
            RequestStreamCommand::Acknowledge { stream, .. }
            | RequestStreamCommand::Close { stream, .. } => {
                self.validate_stream(stream)?;
                if stream.principal != principal {
                    return Err(ManagedError::InvalidIdentity);
                }
                Ok(stream.slot)
            }
            RequestStreamCommand::Seal { key, .. } => {
                self.validate_stream(&key.stream)?;
                if key.stream.principal != principal || !key.is_valid() {
                    return Err(ManagedError::InvalidIdentity);
                }
                Ok(key.stream.slot)
            }
        }
    }
    pub fn control_receipt(
        &self,
        input: &RequestStreamControlInput,
    ) -> Result<Option<&RequestStreamControlReceipt>, LedgerError> {
        self.control_receipt_parts(
            input.cluster,
            input.ledger,
            input.principal,
            input.id,
            &input.command,
        )
    }
    pub fn control_receipt_parts(
        &self,
        cluster: [u8; 16],
        ledger: LedgerId,
        principal: ParticipantId,
        id: RequestId,
        command: &RequestStreamCommand,
    ) -> Result<Option<&RequestStreamControlReceipt>, LedgerError> {
        let slot = self.input_slot_parts(cluster, ledger, principal, id, command)?;
        let latest = self
            .locate(principal, slot)
            .ok()
            .and_then(|i| self.slots.get(i))
            .and_then(|row| row.data.latest.as_ref());
        if let Some(receipt) = latest
            && receipt.id == id
        {
            if receipt.intent_hash
                != request_stream_control_hash(cluster, ledger, principal, command)
                    .map_err(|_| ManagedError::Capacity)?
            {
                return Err(ManagedError::Conflict.into());
            }
            return Ok(Some(receipt));
        }
        Ok(None)
    }
    fn candidate(
        &mut self,
        principal: ParticipantId,
        slot: u32,
        extra: usize,
        lane: BudgetLane,
        budget: &MemoryBudget,
        registering: bool,
    ) -> Result<PreparedStream, LedgerError> {
        let location = self.locate(principal, slot);
        if location.is_err() && !registering {
            // Only a registration creates a pair; every other command names
            // one that must already exist.
            return Err(ManagedError::NotRegistered.into());
        }
        // At capacity a new pair may take the place of the longest-closed
        // vacant pair: its generation fence survives through the registry
        // watermark, and every replica selects the same victim from the
        // same committed rows.
        let evict = if location.is_err() && self.slots.len() >= self.limits.max_slots {
            let victim = self
                .slots
                .iter()
                .enumerate()
                .filter(|(_, row)| matches!(row.data.state, RequestStreamState::Vacant { .. }))
                .min_by_key(|(_, row)| row.data.stamp())
                .map(|(index, row)| (index, row.data.stamp()));
            match victim {
                Some(victim) => Some(victim),
                None => return Err(ManagedError::Capacity.into()),
            }
        } else {
            None
        };
        if self.slots.capacity() < self.limits.max_slots {
            let bytes = self
                .limits
                .max_slots
                .checked_mul(size_of::<StreamSlot>())
                .and_then(|n| n.checked_add(128))
                .ok_or(ManagedError::Capacity)?;
            let charge = budget
                .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)?
                .commit();
            self.slots
                .try_reserve_exact(self.limits.max_slots.saturating_sub(self.slots.len()))
                .map_err(|_| ManagedError::Capacity)?;
            self._slots_charge = Some(charge);
        }
        let old = location.ok().and_then(|i| self.slots.get(i));
        let bytes = old
            .map(|s| s.data.charge())
            .transpose()?
            .unwrap_or(4096)
            .checked_add(extra)
            .ok_or(ManagedError::Capacity)?;
        let charge = budget.reserve(BudgetKind::Control, lane, bytes)?.commit();
        let data = old.map_or_else(
            || StreamSlotData {
                principal,
                state: RequestStreamState::Vacant {
                    slot,
                    generation: 0,
                },
                latest: None,
                rows: Vec::new(),
            },
            |s| s.data.clone(),
        );
        let base = old.map(|s| s.data.stamp());
        Ok(PreparedStream {
            position: location.unwrap_or_else(|i| i),
            replacing: location.is_ok(),
            base,
            evict,
            assigned: None,
            data,
            charge,
        })
    }
    fn finish_candidate(
        &self,
        mut prepared: PreparedStream,
    ) -> Result<PreparedStream, LedgerError> {
        let bytes = prepared.data.charge()?;
        if bytes > self.limits.max_slot_bytes || bytes > prepared.charge.bytes() {
            return Err(ManagedError::Capacity.into());
        }
        prepared.charge.shrink_to(bytes)?;
        Ok(prepared)
    }
    pub fn prepare_receipt(
        &mut self,
        receipt: ManagedReceipt,
        lane: BudgetLane,
        budget: &MemoryBudget,
    ) -> Result<PreparedStream, LedgerError> {
        if self.lookup(&receipt.key)?.is_some() {
            return Err(ManagedError::Conflict.into());
        }
        let extra = reference_charge(&receipt)?
            .checked_add(4096)
            .ok_or(ManagedError::Capacity)?;
        let mut next = self.candidate(
            receipt.key.stream.principal,
            receipt.key.stream.slot,
            extra,
            lane,
            budget,
            false,
        )?;
        let i = next
            .data
            .rows
            .binary_search_by_key(&receipt.key.ordinal, |row| row.key.ordinal)
            .err()
            .ok_or(ManagedError::Conflict)?;
        next.data
            .rows
            .try_reserve_exact(1)
            .map_err(|_| ManagedError::Capacity)?;
        next.data.rows.insert(i, receipt);
        self.finish_candidate(next)
    }
    pub fn prepare_control(
        &mut self,
        input: &RequestStreamControlInput,
        sequence: SessionSeq,
        budget: &MemoryBudget,
    ) -> Result<PreparedStream, LedgerError> {
        let slot = self.input_slot(input)?;
        if self.control_receipt(input)?.is_some() {
            return Err(ManagedError::Conflict.into());
        }
        let retained = self
            .locate(input.principal, slot)
            .ok()
            .and_then(|i| self.slots.get(i))
            .map(|s| s.data.charge())
            .transpose()?
            .unwrap_or(0);
        let extra = reference_charge(input)?
            .checked_mul(2)
            .and_then(|n| n.checked_add(8192))
            .and_then(|n| n.checked_add(retained))
            .ok_or(ManagedError::Capacity)?;
        let mut next = self.candidate(
            input.principal,
            slot,
            extra,
            BudgetLane::Completion,
            budget,
            matches!(input.command, RequestStreamCommand::Register { .. }),
        )?;
        let outcome = match &input.command {
            RequestStreamCommand::Register {
                expected_generation,
                owner,
                window,
                ..
            } => {
                let RequestStreamState::Vacant { generation, .. } = self.presented(next.data.state)
                else {
                    return Err(ManagedError::Conflict.into());
                };
                // The registration cites the presented generation (the pair's
                // last one or the registry watermark, whichever is higher) and
                // is assigned exactly one above it, which is the rule every
                // client validator holds the reply to.
                if generation != *expected_generation
                    || owner.is_zero()
                    || *window == 0
                    || *window > self.limits.max_window
                {
                    return Err(ManagedError::Conflict.into());
                }
                let assigned = generation.checked_add(1).ok_or(ManagedError::Capacity)?;
                let stream = RequestStreamIdentity {
                    cluster: self.cluster,
                    ledger: self.ledger,
                    principal: input.principal,
                    slot,
                    generation: assigned,
                };
                next.assigned = Some(assigned);
                next.data.state = RequestStreamState::Active {
                    stream,
                    owner: *owner,
                    revision: 1,
                    window: *window,
                    acknowledged_through: 0,
                };
                RequestStreamControlOutcome::Registered(next.data.state)
            }
            RequestStreamCommand::Acknowledge {
                stream,
                expected_revision,
                through,
                receipts,
            } => {
                let (floor, window) = check_revision(&next.data, *stream, *expected_revision)?;
                let count = through.checked_sub(floor).ok_or(ManagedError::Conflict)?;
                if count == 0
                    || count > u64::from(window)
                    || usize::try_from(count).ok() != Some(receipts.len())
                {
                    return Err(ManagedError::Conflict.into());
                }
                let mut ordinal = floor;
                for ack in receipts {
                    ordinal = ordinal.checked_add(1).ok_or(ManagedError::Capacity)?;
                    if ack.key.stream != *stream || ack.key.ordinal != ordinal {
                        return Err(ManagedError::Conflict.into());
                    }
                    let row = next
                        .data
                        .rows
                        .iter()
                        .find(|row| row.key.ordinal == ordinal)
                        .ok_or(ManagedError::Conflict)?;
                    if row.key != ack.key
                        || row.content_hash().map_err(|_| ManagedError::Capacity)?
                            != ack.receipt_hash
                    {
                        return Err(ManagedError::Conflict.into());
                    }
                }
                next.data.rows.retain(|row| row.key.ordinal > *through);
                let revision = next.data.advance_revision()?;
                if let RequestStreamState::Active {
                    acknowledged_through,
                    ..
                } = &mut next.data.state
                {
                    *acknowledged_through = *through;
                }
                RequestStreamControlOutcome::Acknowledged {
                    stream: *stream,
                    revision,
                    through: *through,
                }
            }
            RequestStreamCommand::Seal {
                key,
                expected_revision,
                family,
                intent_hash,
            } => {
                let (floor, window) = check_revision(&next.data, key.stream, *expected_revision)?;
                if key.ordinal <= floor {
                    return Err(ManagedError::Retired { through: floor }.into());
                }
                if key.ordinal > floor.saturating_add(u64::from(window)) {
                    return Err(ManagedError::Capacity.into());
                }
                let receipt = if let Some(row) = next
                    .data
                    .rows
                    .iter()
                    .find(|row| row.key.ordinal == key.ordinal)
                {
                    if row.key != *key
                        || row.intent_hash != *intent_hash
                        || receipt_family(row) != *family
                    {
                        return Err(ManagedError::Conflict.into());
                    }
                    row.clone()
                } else {
                    let row = ManagedReceipt {
                        key: *key,
                        sequence,
                        raft_index: 0,
                        intent_hash: *intent_hash,
                        outcome: ManagedReceiptOutcome::Sealed { family: *family },
                    };
                    let i = next
                        .data
                        .rows
                        .binary_search_by_key(&key.ordinal, |row| row.key.ordinal)
                        .err()
                        .ok_or(ManagedError::Conflict)?;
                    next.data
                        .rows
                        .try_reserve_exact(1)
                        .map_err(|_| ManagedError::Capacity)?;
                    next.data.rows.insert(i, row.clone());
                    row
                };
                next.data.advance_revision()?;
                RequestStreamControlOutcome::Sealed(Box::new(receipt))
            }
            RequestStreamCommand::Close {
                stream,
                expected_revision,
                issued_through,
            } => {
                let (floor, window) = check_revision(&next.data, *stream, *expected_revision)?;
                if *issued_through < floor
                    || *issued_through > floor.saturating_add(u64::from(window))
                {
                    return Err(ManagedError::Conflict.into());
                }
                let mut ordinal = floor;
                for row in &next.data.rows {
                    ordinal = ordinal.checked_add(1).ok_or(ManagedError::Capacity)?;
                    if row.key.ordinal != ordinal
                        || !matches!(row.outcome, ManagedReceiptOutcome::Sealed { .. })
                    {
                        return Err(ManagedError::Conflict.into());
                    }
                }
                if ordinal != *issued_through {
                    return Err(ManagedError::Conflict.into());
                }
                next.data.rows = Vec::new();
                next.data.state = RequestStreamState::Vacant {
                    slot,
                    generation: stream.generation,
                };
                RequestStreamControlOutcome::Closed {
                    stream: *stream,
                    vacant_generation: stream.generation,
                }
            }
        };
        next.data.latest = Some(RequestStreamControlReceipt {
            cluster: self.cluster,
            ledger: self.ledger,
            principal: input.principal,
            id: input.id,
            intent_hash: input.intent_hash().map_err(|_| ManagedError::Capacity)?,
            raft_index: 0,
            outcome,
        });
        self.finish_candidate(next)
    }
    pub fn validate_publication(&self, prepared: &PreparedStream) -> Result<(), LedgerError> {
        let current = self.slots.get(prepared.position).filter(|s| {
            s.data.principal == prepared.data.principal && s.data.slot() == prepared.data.slot()
        });
        let base = current.map(|s| s.data.stamp());
        let room = match prepared.evict {
            Some((index, stamp)) => {
                let victim = self.slots.get(index).ok_or(LedgerError::Corrupt)?;
                if prepared.replacing
                    || !matches!(victim.data.state, RequestStreamState::Vacant { .. })
                    || victim.data.stamp() != stamp
                    || (victim.data.principal, victim.data.slot())
                        == (prepared.data.principal, prepared.data.slot())
                {
                    return Err(LedgerError::Corrupt);
                }
                true
            }
            None => self.slots.len() < self.slots.capacity(),
        };
        if base != prepared.base
            || prepared.replacing != base.is_some()
            || (!prepared.replacing && (!room || prepared.position > self.slots.len()))
            || prepared
                .assigned
                .is_some_and(|generation| generation <= self.next_generation)
        {
            return Err(LedgerError::Corrupt);
        }
        Ok(())
    }
    pub fn publish(&mut self, prepared: PreparedStream) -> Result<(), LedgerError> {
        self.validate_publication(&prepared)?;
        let slot = StreamSlot {
            data: prepared.data,
            _charge: prepared.charge,
        };
        if prepared.replacing {
            *self
                .slots
                .get_mut(prepared.position)
                .ok_or(LedgerError::Corrupt)? = slot;
        } else {
            let mut position = prepared.position;
            if let Some((index, _)) = prepared.evict {
                // The victim was selected from the same rows; removing it
                // first keeps the sorted position exact without allocating.
                self.slots.remove(index);
                if index < position {
                    position = position.checked_sub(1).ok_or(LedgerError::Corrupt)?;
                }
            }
            if self.slots.len() == self.slots.capacity() || position > self.slots.len() {
                return Err(LedgerError::Corrupt);
            }
            self.slots.insert(position, slot);
        }
        if let Some(generation) = prepared.assigned {
            self.next_generation = self.next_generation.max(generation);
        }
        self.activated = true;
        Ok(())
    }
    pub fn checkpoint_charge(&self) -> Result<usize, LedgerError> {
        self.slots.iter().try_fold(4096usize, |n, s| {
            n.checked_add(s.data.charge()?).ok_or(LedgerError::Capacity)
        })
    }
    /// Borrow the retained slot rows in their original durable order.
    pub(crate) fn checkpoint_rows(&self) -> impl ExactSizeIterator<Item = &StreamSlotData> {
        self.slots.iter().map(|slot| &slot.data)
    }
    #[cfg(test)]
    pub fn checkpoint(&self) -> RequestStreamsCheckpoint {
        RequestStreamsCheckpoint {
            activated: self.activated,
            slots: self.slots.iter().map(|s| s.data.clone()).collect(),
        }
    }
    /// `next_generation` is the persisted watermark (`FOCALSS7`); an older
    /// checkpoint without one restores the largest retained generation, which
    /// is exact because such checkpoints never evicted a pair.
    pub fn restore(
        &mut self,
        state: RequestStreamsCheckpoint,
        sequence: SessionSeq,
        index: u64,
        budget: &MemoryBudget,
        next_generation: Option<u64>,
    ) -> Result<(), LedgerError> {
        if state.slots.len() > self.limits.max_slots
            || (state.activated == state.slots.is_empty())
            || (self.activated && !state.activated)
        {
            return Err(LedgerError::Corrupt);
        }
        let retained = state
            .slots
            .iter()
            .map(StreamSlotData::generation)
            .max()
            .unwrap_or(0);
        let watermark = match next_generation {
            Some(watermark) if watermark >= retained => watermark,
            Some(_) => return Err(LedgerError::Corrupt),
            None => retained,
        };
        let mut restored = Self::new(self.cluster, self.ledger, self.limits)?;
        for data in state.slots {
            if data.principal.is_zero() || restored.locate(data.principal, data.slot()).is_ok() {
                return Err(LedgerError::Corrupt);
            }
            match data.state {
                RequestStreamState::Vacant { generation, .. } => {
                    if generation == 0 || !data.rows.is_empty() {
                        return Err(LedgerError::Corrupt);
                    }
                }
                RequestStreamState::Active {
                    stream,
                    owner,
                    revision,
                    window,
                    acknowledged_through,
                } => {
                    self.validate_stream(&stream)?;
                    if stream.principal != data.principal
                        || owner.is_zero()
                        || revision == 0
                        || window == 0
                        || window > self.limits.max_window
                        || data.rows.len()
                            > usize::try_from(window).map_err(|_| ManagedError::Capacity)?
                    {
                        return Err(LedgerError::Corrupt);
                    }
                    let mut previous = acknowledged_through;
                    for row in &data.rows {
                        if row.key.stream != stream
                            || !row.key.is_valid()
                            || row.key.ordinal <= previous
                            || row.key.ordinal
                                > acknowledged_through.saturating_add(u64::from(window))
                            || row.raft_index == 0
                            || row.raft_index > index
                            || row.sequence > sequence
                        {
                            return Err(LedgerError::Corrupt);
                        }
                        validate_receipt(
                            row,
                            self.cluster,
                            self.ledger,
                            data.principal,
                            sequence,
                            index,
                        )?;
                        previous = row.key.ordinal;
                    }
                }
            }
            let latest = data.latest.as_ref().ok_or(LedgerError::Corrupt)?;
            if latest.cluster != self.cluster
                || latest.ledger != self.ledger
                || latest.principal != data.principal
                || latest.id.is_zero()
                || latest.raft_index == 0
                || latest.raft_index > index
            {
                return Err(LedgerError::Corrupt);
            }
            match &latest.outcome {
                RequestStreamControlOutcome::Registered(state) if *state == data.state => {}
                RequestStreamControlOutcome::Acknowledged {
                    stream,
                    revision,
                    through,
                } => {
                    if !matches!(data.state,RequestStreamState::Active{stream:actual,revision:r,acknowledged_through:f,..} if actual==*stream && r==*revision && f==*through)
                    {
                        return Err(LedgerError::Corrupt);
                    }
                }
                RequestStreamControlOutcome::Sealed(receipt) => {
                    validate_receipt(
                        receipt,
                        self.cluster,
                        self.ledger,
                        data.principal,
                        sequence,
                        latest.raft_index,
                    )?;
                    if !data.rows.iter().any(|row| row == receipt.as_ref())
                        || !matches!(data.state,RequestStreamState::Active{stream,..} if stream==receipt.key.stream)
                    {
                        return Err(LedgerError::Corrupt);
                    }
                }
                RequestStreamControlOutcome::Closed {
                    stream,
                    vacant_generation,
                } => {
                    self.validate_stream(stream)?;
                    if stream.principal != data.principal
                        || stream.generation != *vacant_generation
                        || !matches!(data.state,RequestStreamState::Vacant{slot,generation} if slot==stream.slot && generation==*vacant_generation)
                    {
                        return Err(LedgerError::Corrupt);
                    }
                }
                _ => return Err(LedgerError::Corrupt),
            }
            // Restoration re-creates each retained pair as a registration
            // would, below the checked capacity, so nothing is evicted.
            let mut next = restored.candidate(
                data.principal,
                data.slot(),
                data.charge()?,
                BudgetLane::Completion,
                budget,
                true,
            )?;
            next.data = data;
            let next = restored.finish_candidate(next)?;
            restored.publish(next)?;
        }
        restored.activated = state.activated;
        restored.next_generation = watermark;
        *self = restored;
        Ok(())
    }
}
fn check_revision(
    data: &StreamSlotData,
    stream: RequestStreamIdentity,
    expected: u64,
) -> Result<(u64, u32), ManagedError> {
    match data.state {
        RequestStreamState::Active {
            stream: actual,
            revision,
            acknowledged_through,
            window,
            ..
        } if actual == stream && revision == expected => Ok((acknowledged_through, window)),
        _ => Err(ManagedError::Conflict),
    }
}
pub(crate) fn receipt_family(receipt: &ManagedReceipt) -> ManagedRequestFamily {
    match receipt.outcome {
        ManagedReceiptOutcome::Domain(_) => ManagedRequestFamily::Domain,
        ManagedReceiptOutcome::Cursor { .. } => ManagedRequestFamily::Cursor,
        ManagedReceiptOutcome::Sealed { family } => family,
    }
}

#[derive(Debug, Clone, Copy)]
enum ReadResolution<'a> {
    Retained(&'a ManagedReceipt),
    Retired(u64),
    Unknown,
    Closed(u64),
}
/// Borrowed output. Hosts reserve owned_bytes before materializing the exact
/// result and retain that allowance through transport delivery.
pub struct RequestStreamReadView<'a> {
    cluster: [u8; 16],
    ledger: LedgerId,
    principal: ParticipantId,
    sequence: SessionSeq,
    raft_index: u64,
    state: RequestStreamState,
    key: Option<ManagedRequestKey>,
    resolution: ReadResolution<'a>,
}
impl RequestStreamReadView<'_> {
    pub fn owned_bytes(&self) -> Result<usize, LedgerError> {
        let extra = match self.resolution {
            ReadResolution::Retained(row) => reference_charge(row)?,
            _ => 0,
        };
        extra.checked_add(1024).ok_or(LedgerError::Capacity)
    }
    pub fn item_count(&self) -> usize {
        match self.resolution {
            ReadResolution::Retained(ManagedReceipt {
                outcome:
                    ManagedReceiptOutcome::Domain(
                        CommandResult::Generated(ids) | CommandResult::Existing(ids),
                    ),
                ..
            }) => ids.len(),
            ReadResolution::Retained(ManagedReceipt {
                outcome:
                    ManagedReceiptOutcome::Cursor {
                        record:
                            Some(CursorRecordSnapshot {
                                filter: CursorFilterSnapshot::Claims(ids),
                                ..
                            }),
                        ..
                    },
                ..
            }) => ids.len(),
            _ => 0,
        }
    }
    pub fn to_owned(&self) -> Result<RequestStreamRead, LedgerError> {
        let result = if let Some(key) = self.key {
            RequestStreamReadResult::Receipt {
                key,
                state: self.state,
                resolution: match self.resolution {
                    ReadResolution::Retained(row) => {
                        ManagedReceiptResolution::Retained(Box::new(row.clone()))
                    }
                    ReadResolution::Retired(through) => {
                        ManagedReceiptResolution::Retired { through }
                    }
                    ReadResolution::Closed(generation) => {
                        ManagedReceiptResolution::StreamClosed { generation }
                    }
                    ReadResolution::Unknown => ManagedReceiptResolution::Unknown,
                },
            }
        } else {
            RequestStreamReadResult::Slot(self.state)
        };
        Ok(RequestStreamRead {
            schema: MANAGED_REQUEST_SCHEMA,
            cluster: self.cluster,
            ledger: self.ledger,
            principal: self.principal,
            sequence: self.sequence,
            raft_index: self.raft_index,
            result,
        })
    }
}
impl RequestStreams {
    pub fn read(
        &self,
        principal: ParticipantId,
        query: &RequestStreamQuery,
        sequence: SessionSeq,
        raft_index: u64,
    ) -> Result<RequestStreamReadView<'_>, LedgerError> {
        let (slot, key) = match query {
            RequestStreamQuery::Slot { slot } => (*slot, None),
            RequestStreamQuery::Receipt { key } => (key.stream.slot, Some(*key)),
        };
        let state = self.state(principal, slot)?;
        let resolution = if let Some(key) = key {
            if key.stream.principal != principal {
                return Err(ManagedError::InvalidIdentity.into());
            }
            match self.lookup(&key) {
                Ok(Some(row)) => ReadResolution::Retained(row),
                Ok(None) => ReadResolution::Unknown,
                Err(ManagedError::Retired { through }) => ReadResolution::Retired(through),
                Err(ManagedError::Closed { generation }) => ReadResolution::Closed(generation),
                Err(ManagedError::NotRegistered | ManagedError::Capacity) => {
                    ReadResolution::Unknown
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            ReadResolution::Unknown
        };
        Ok(RequestStreamReadView {
            cluster: self.cluster,
            ledger: self.ledger,
            principal,
            sequence,
            raft_index,
            state,
            key,
            resolution,
        })
    }
}

fn validate_receipt(
    row: &ManagedReceipt,
    cluster: [u8; 16],
    ledger: LedgerId,
    principal: ParticipantId,
    prefix: SessionSeq,
    index: u64,
) -> Result<(), LedgerError> {
    if !row.key.is_valid()
        || row.key.stream.cluster != cluster
        || row.key.stream.ledger != ledger
        || row.key.stream.principal != principal
        || row.raft_index == 0
        || row.raft_index > index
        || row.sequence > prefix
    {
        return Err(LedgerError::Corrupt);
    }
    match &row.outcome {
        ManagedReceiptOutcome::Domain(_) if row.sequence == SessionSeq(0) => {
            return Err(LedgerError::Corrupt);
        }
        ManagedReceiptOutcome::Cursor {
            revision,
            floor,
            record,
        } => {
            if *revision == 0 || *floor > row.sequence {
                return Err(LedgerError::Corrupt);
            }
            if let Some(record) = record {
                if record.token.key.ledger != ledger
                    || record.token.position.ledger != ledger
                    || record.token.position.sequence > row.sequence
                    || record.token.generation == 0
                {
                    return Err(LedgerError::Corrupt);
                }
                if let CursorFilterSnapshot::Claims(ids) = &record.filter
                    && !ids.windows(2).all(|pair| matches!(pair,[a,b] if a<b))
                {
                    return Err(LedgerError::Corrupt);
                }
                match record.mode {
                    CursorModeSnapshot::Seeding { snapshot }
                        if snapshot > row.sequence
                            || record.token.position.sequence != snapshot
                            || record.token.position.offset
                                != CursorPositionOffsetSnapshot::Resolved =>
                    {
                        return Err(LedgerError::Corrupt);
                    }
                    CursorModeSnapshot::Protected if record.expires_at != u64::MAX => {
                        return Err(LedgerError::Corrupt);
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    Ok(())
}
