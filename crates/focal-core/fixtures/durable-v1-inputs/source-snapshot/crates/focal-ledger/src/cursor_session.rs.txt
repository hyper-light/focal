/// Authenticated ingress input. `intent_hash` must be computed by the trusted
/// adapter from the versioned client operation, excluding server-generated time,
/// expiry and expected registry revision. Never accept a client-asserted hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorInput {
    pub ledger: LedgerId,
    pub key: RequestKey,
    pub intent_hash: ContentHash,
    pub command: CursorCommand,
}

/// Original durable outcome, retained even after this consumer advances again.
/// Cursor revision orders metadata; domain_sequence names the domain prefix at
/// which this Raft entry applied. Cursor commands do not consume SessionSeq.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorReceipt {
    pub ledger: LedgerId,
    pub key: RequestKey,
    pub intent_hash: ContentHash,
    pub revision: u64,
    pub domain_sequence: SessionSeq,
    pub raft_index: u64,
    pub floor: SessionSeq,
    pub record: Option<CursorRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorSubmission {
    Committed(Box<CursorReceipt>),
    Pending(RequestKey),
}

#[derive(Default, Serialize, Deserialize)]
struct CursorMetadata {
    receipts: BTreeMap<RequestKey, CursorReceipt>,
    owners: BTreeMap<ConsumerId, ParticipantId>,
}
#[derive(Serialize, Deserialize)]
struct CursorEnvelope {
    schema: u16,
    ledger: LedgerId,
    domain_sequence: SessionSeq,
    replay_floor: SessionSeq,
    trusted_control: bool,
    input: CursorInput,
}
#[derive(Serialize, Deserialize)]
struct LegacyCursorEnvelope {
    schema: u16,
    ledger: LedgerId,
    domain_sequence: SessionSeq,
    trusted_control: bool,
    input: CursorInput,
}
#[derive(Serialize, Deserialize)]
struct SnapshotEnvelopeV2 {
    schema: u16,
    ledger: LedgerId,
    raft_index: u64,
    core: Vec<u8>,
    cursors: CursorCheckpoint,
    cursor_meta: CursorMetadata,
    delta_floor: SessionSeq,
    deltas: Vec<Delta>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotEnvelopeV3 {
    state: SnapshotEnvelopeV2,
    membership: MembershipState,
}
struct CursorCandidate {
    entry_hash: ContentHash,
    prepared: PreparedCursorUpdate,
    metadata: CursorMetadata,
    receipt: CursorReceipt,
    metadata_charge: Allocation,
    result_charge: Allocation,
}

impl Session {
    pub fn cursor(&self, consumer: ConsumerId) -> Option<&CursorRecord> {
        self.cursors.get(consumer)
    }
    pub fn cursor_receipt(&self, key: &RequestKey) -> Option<&CursorReceipt> {
        self.cursor_meta.receipts.get(key)
    }
    pub fn cursor_revision(&self) -> u64 {
        self.cursors.revision()
    }
    pub fn cursor_clock(&self) -> u64 {
        self.cursors.checkpoint().clock
    }
    pub fn cursor_owner(&self, consumer: ConsumerId) -> Option<ParticipantId> {
        self.cursor_meta.owners.get(&consumer).copied()
    }
    pub fn stream_bounds(&self) -> ReplayBounds {
        ReplayBounds {
            ledger: self.ledger,
            floor: self.delta_floor.max(self.cursors.checkpoint().floor),
            published: self.sequence(),
        }
    }

    /// Submit an authenticated projection operation. Consumer ownership and the
    /// shared domain request-epoch window are checked before reservation.
    pub fn submit_cursor(&mut self, input: &CursorInput) -> Result<CursorSubmission, LedgerError> {
        self.propose_cursor(input, false)
    }
    /// Trusted runtime only: additionally allows protected consumers and floor
    /// maintenance. Network adapters must never derive this authority from payload.
    pub fn submit_cursor_control(
        &mut self,
        input: &CursorInput,
    ) -> Result<CursorSubmission, LedgerError> {
        self.propose_cursor(input, true)
    }
    pub fn submit_cursor_local(
        &mut self,
        input: &CursorInput,
    ) -> Result<CursorSubmission, LedgerError> {
        self.cursor_local(input, false)
    }
    pub fn submit_cursor_control_local(
        &mut self,
        input: &CursorInput,
    ) -> Result<CursorSubmission, LedgerError> {
        self.cursor_local(input, true)
    }
    fn cursor_local(
        &mut self,
        input: &CursorInput,
        control: bool,
    ) -> Result<CursorSubmission, LedgerError> {
        let status = self.status();
        if status.voters != [status.node_id] || !status.learners.is_empty() {
            return Err(LedgerError::NotReady {
                leader: status.leader_id,
            });
        }
        let outcome = self.propose_cursor(input, control)?;
        let CursorSubmission::Pending(key) = outcome else {
            return Ok(outcome);
        };
        let events = self.poll()?;
        if !events.messages.is_empty() {
            return Err(LedgerError::OutcomeUnknown);
        }
        self.cursor_receipt(&key)
            .cloned()
            .map(Box::new)
            .map(CursorSubmission::Committed)
            .ok_or(LedgerError::OutcomeUnknown)
    }
    fn propose_cursor(
        &mut self,
        input: &CursorInput,
        control: bool,
    ) -> Result<CursorSubmission, LedgerError> {
        self.check()?;
        let status = self.status();
        if status.role != StateRole::Leader || self.ready_term != Some(status.term) {
            return Err(LedgerError::NotReady {
                leader: status.leader_id,
            });
        }
        if input.ledger != self.ledger {
            return Err(LedgerError::CursorRequest(ErrorCode::InvalidNamespace));
        }
        if let Some(receipt) = self.cursor_receipt(&input.key) {
            return if receipt.intent_hash == input.intent_hash {
                Ok(CursorSubmission::Committed(Box::new(receipt.clone())))
            } else {
                Err(LedgerError::CursorRequest(ErrorCode::IdempotencyConflict))
            };
        }
        if self.effective()?.receipt(&input.key).is_some() {
            return Err(LedgerError::CursorRequest(ErrorCode::IdempotencyConflict));
        }
        if let Some(pending) = &self.pending_cursor {
            if pending.receipt.key == input.key {
                return if pending.receipt.intent_hash == input.intent_hash {
                    Ok(CursorSubmission::Pending(input.key))
                } else {
                    Err(LedgerError::CursorRequest(ErrorCode::IdempotencyConflict))
                };
            }
            return Err(LedgerError::Capacity);
        }
        // A single cursor candidate serializes controls with domain reservations:
        // every tail forecast observes exactly the committed set of retention pins.
        if self.pending_managed.is_some()
            || !self.pending.is_empty()
            || self.pending_maintenance.is_some()
            || self.pending_placement.is_some()
        {
            return Err(LedgerError::Capacity);
        }
        if postcard::experimental::serialized_size(input)? > self.limits.core.max_command_bytes {
            return Err(LedgerError::Capacity);
        }
        let charge = reference_charge(input)?;
        let _encode = self
            .budget
            .reserve(BudgetKind::Pending, BudgetLane::Completion, charge)?;
        let envelope = CursorEnvelope {
            schema: 2,
            ledger: self.ledger,
            domain_sequence: self.sequence(),
            replay_floor: self.stream_bounds().floor,
            trusted_control: control,
            input: input.clone(),
        };
        let mut data = CURSOR_MAGIC.to_vec();
        data.extend(postcard::to_stdvec(&envelope)?);
        let candidate = self.build_cursor_candidate(
            &envelope,
            ContentHash(*blake3::hash(&data).as_bytes()),
            false,
        )?;
        self.consensus.propose_in(data, BudgetLane::Completion)?;
        self.pending_cursor = Some(candidate);
        Ok(CursorSubmission::Pending(input.key))
    }
    fn build_cursor_candidate(
        &self,
        envelope: &CursorEnvelope,
        entry_hash: ContentHash,
        committed: bool,
    ) -> Result<CursorCandidate, LedgerError> {
        let input = &envelope.input;
        if envelope.schema != 2
            || envelope.ledger != self.ledger
            || input.ledger != self.ledger
            || envelope.domain_sequence != self.sequence()
            || envelope.replay_floor > self.sequence()
            || envelope.replay_floor < self.cursors.checkpoint().floor
        {
            return Err(LedgerError::Corrupt);
        }
        if input.key.principal.is_zero() || input.key.id.is_zero() {
            return Err(LedgerError::CursorRequest(ErrorCode::InvalidSchema));
        }
        if postcard::experimental::serialized_size(input)? > self.limits.core.max_command_bytes {
            return Err(LedgerError::Capacity);
        }
        if self.core.snapshot().receipts.contains_key(&input.key)
            || self.cursor_receipt(&input.key).is_some()
        {
            return Err(LedgerError::CursorRequest(ErrorCode::IdempotencyConflict));
        }
        let window = self
            .core
            .snapshot()
            .epochs
            .get(&input.key.principal)
            .ok_or(LedgerError::CursorRequest(
                ErrorCode::RequestEpochNotAdmitted,
            ))?;
        if input.key.epoch < window.minimum {
            return Err(LedgerError::CursorRequest(ErrorCode::RequestHistoryExpired));
        }
        if !window.admitted.contains(&input.key.epoch) {
            return Err(LedgerError::CursorRequest(
                ErrorCode::RequestEpochNotAdmitted,
            ));
        }
        let operation = &input.command.operation;
        let receipt_limit = if matches!(
            operation,
            CursorOperation::Register { .. }
                | CursorOperation::RegisterProtected { .. }
                | CursorOperation::BeginSeed { .. }
        ) {
            self.limits
                .cursor_receipts
                .saturating_sub((self.limits.cursor_receipts / 8).max(1))
        } else {
            self.limits.cursor_receipts
        };
        if self.cursor_meta.receipts.len() >= receipt_limit {
            return Err(LedgerError::Capacity);
        }
        if matches!(
            operation,
            CursorOperation::RegisterProtected { .. } | CursorOperation::AdvanceFloor { .. }
        ) && !envelope.trusted_control
        {
            return Err(LedgerError::CursorRequest(ErrorCode::WrongActor));
        }
        let consumer = operation_consumer(operation);
        if let Some(id) = consumer {
            if let Some(owner) = self.cursor_owner(id) {
                if owner != input.key.principal {
                    return Err(LedgerError::CursorRequest(ErrorCode::WrongActor));
                }
            } else if !matches!(
                operation,
                CursorOperation::Register { .. }
                    | CursorOperation::RegisterProtected { .. }
                    | CursorOperation::BeginSeed { .. }
            ) {
                return Err(StreamError::MissingConsumer.into());
            }
        }
        match operation {
            CursorOperation::Register { start, .. }
            | CursorOperation::RegisterProtected { start, .. } => {
                self.validate_replay_position(*start)?;
                if start.retention_prefix() < envelope.replay_floor {
                    return Err(LedgerError::Corrupt);
                }
            }
            CursorOperation::BeginSeed { snapshot, .. } => {
                self.validate_replay_position(Position::resolved(self.ledger, *snapshot))?;
                if *snapshot < envelope.replay_floor {
                    return Err(LedgerError::Corrupt);
                }
            }
            CursorOperation::Acknowledge { token }
            | CursorOperation::AcknowledgeAndRenew { token, .. } => {
                self.validate_replay_position(token.position)?;
            }
            _ => {}
        }
        let meta_bytes = reference_charge(&self.cursor_meta)?
            .checked_add(reference_charge(input)?)
            .and_then(|n| {
                n.checked_add(self.placement_charge.as_ref().map_or(0, Allocation::bytes))
            })
            .and_then(|n| n.checked_mul(3))
            .ok_or(LedgerError::Capacity)?;
        let _scratch =
            self.budget
                .reserve(BudgetKind::Pending, BudgetLane::Completion, meta_bytes)?;
        let prepared = if committed {
            self.cursors
                .prepare_committed(&input.command, self.sequence())?
        } else {
            self.cursors.prepare(&input.command, self.sequence())?
        };
        // A lease clock advance can release projection pins, never protected pins.
        let receipt = CursorReceipt {
            ledger: self.ledger,
            key: input.key,
            intent_hash: input.intent_hash,
            revision: prepared.checkpoint().revision,
            domain_sequence: self.sequence(),
            raft_index: 0,
            floor: envelope.replay_floor.max(prepared.checkpoint().floor),
            record: consumer.and_then(|id| prepared.checkpoint().consumers.get(&id).cloned()),
        };
        let mut metadata = CursorMetadata {
            receipts: self.cursor_meta.receipts.clone(),
            owners: self.cursor_meta.owners.clone(),
        };
        if let Some(consumer) = consumer {
            metadata
                .owners
                .entry(consumer)
                .or_insert(input.key.principal);
        }
        metadata.receipts.insert(input.key, receipt.clone());
        let metadata_charge = self
            .budget
            .reserve(
                BudgetKind::ReadPins,
                BudgetLane::Completion,
                reference_charge(&metadata)?,
            )?
            .commit();
        let result_charge = self
            .budget
            .reserve(
                BudgetKind::Pending,
                BudgetLane::Completion,
                reference_charge(&receipt)?,
            )?
            .commit();
        Ok(CursorCandidate {
            entry_hash,
            prepared,
            metadata,
            receipt,
            metadata_charge,
            result_charge,
        })
    }
    fn apply_cursor_entry(
        &mut self,
        data: &[u8],
        raft_index: u64,
    ) -> Result<(CursorReceipt, Allocation), LedgerError> {
        self.pending_maintenance = None;
        let digest = ContentHash(*blake3::hash(data).as_bytes());
        let mut candidate = if self
            .pending_cursor
            .as_ref()
            .is_some_and(|p| p.entry_hash == digest)
        {
            self.pending_cursor.take().ok_or(LedgerError::Corrupt)?
        } else {
            self.pending_cursor = None;
            self.clear_pending();
            let _decode = self.budget.reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                data.len()
                    .checked_mul(64)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(LedgerError::Capacity)?,
            )?;
            let envelope = if data.starts_with(CURSOR_MAGIC) {
                postcard::from_bytes::<CursorEnvelope>(
                    data.strip_prefix(CURSOR_MAGIC)
                        .ok_or(LedgerError::Corrupt)?,
                )?
            } else {
                let old: LegacyCursorEnvelope = postcard::from_bytes(
                    data.strip_prefix(LEGACY_CURSOR_MAGIC)
                        .ok_or(LedgerError::Corrupt)?,
                )?;
                if old.schema != 1 {
                    return Err(LedgerError::Corrupt);
                }
                CursorEnvelope {
                    schema: 2,
                    ledger: old.ledger,
                    domain_sequence: old.domain_sequence,
                    replay_floor: self.stream_bounds().floor,
                    trusted_control: old.trusted_control,
                    input: old.input,
                }
            };
            self.build_cursor_candidate(&envelope, digest, true)?
        };
        candidate.receipt.raft_index = raft_index;
        candidate
            .metadata
            .receipts
            .get_mut(&candidate.receipt.key)
            .ok_or(LedgerError::Corrupt)?
            .raft_index = raft_index;
        self.cursors.publish(candidate.prepared)?;
        self.cursor_meta = candidate.metadata;
        self.cursor_charge = candidate.metadata_charge;
        self.retire_deltas(candidate.receipt.floor)?;
        Ok((candidate.receipt, candidate.result_charge))
    }

    // Whole transactions are retired atomically. A partial transaction cursor
    // retains its entire sequence. The simulation includes every pending proposal.
    fn retention_forecast_from<'a>(
        &self,
        next: &[RetainedDelta],
        published: SessionSeq,
        prior: impl Iterator<Item = &'a Candidate> + Clone,
    ) -> Result<SessionSeq, LedgerError> {
        let mut items = 0usize;
        let mut bytes = 0usize;
        let current_floor = prior
            .clone()
            .last()
            .map_or(self.delta_floor, |c| c.retire_through);
        let all = || {
            self.deltas
                .iter()
                .chain(prior.clone().flat_map(|c| c.retained.iter()))
                .chain(next.iter())
                .filter(|d| d.delta.id.sequence > current_floor)
        };
        for delta in all() {
            items = items.checked_add(1).ok_or(LedgerError::Capacity)?;
            bytes = bytes
                .checked_add(delta.bytes)
                .ok_or(LedgerError::Capacity)?;
        }
        let mut floor = current_floor;
        let allowed = self.cursors.retention_limit(published);
        let mut iter = all().peekable();
        while items > self.limits.delta_items || bytes > self.limits.delta_bytes {
            let sequence = iter.peek().ok_or(LedgerError::Corrupt)?.delta.id.sequence;
            if sequence > allowed {
                return Err(StreamError::RetentionPinned {
                    allowed_through: allowed,
                }
                .into());
            }
            while iter.peek().is_some_and(|d| d.delta.id.sequence == sequence) {
                let delta = iter.next().ok_or(LedgerError::Corrupt)?;
                items = items.checked_sub(1).ok_or(LedgerError::Corrupt)?;
                bytes = bytes.checked_sub(delta.bytes).ok_or(LedgerError::Corrupt)?;
            }
            floor = sequence;
        }
        Ok(floor)
    }
    fn retire_deltas(&mut self, through: SessionSeq) -> Result<(), LedgerError> {
        self.delta_floor = self.delta_floor.max(through);
        while self
            .deltas
            .front()
            .is_some_and(|d| d.delta.id.sequence <= self.delta_floor)
        {
            let delta = self.deltas.pop_front().ok_or(LedgerError::Corrupt)?;
            self.delta_bytes = self
                .delta_bytes
                .checked_sub(delta.bytes)
                .ok_or(LedgerError::Corrupt)?;
        }
        Ok(())
    }
    fn validate_replay_position(&self, position: Position) -> Result<(), StreamError> {
        position.validate(self.ledger, self.sequence())?;
        if position.retention_prefix() < self.stream_bounds().floor {
            return Err(StreamError::ResyncRequired(ResyncReason::HistoryExpired));
        }
        if let PositionOffset::Delta(ordinal) = position.offset {
            let index = self.deltas.partition_point(|d| {
                (d.delta.id.sequence, d.delta.id.ordinal) < (position.sequence, ordinal)
            });
            if !self.deltas.get(index).is_some_and(|d| {
                d.delta.id.sequence == position.sequence && d.delta.id.ordinal == ordinal
            }) {
                return Err(StreamError::Invalid(
                    "delta cursor does not name a retained delta",
                ));
            }
        }
        Ok(())
    }
}

fn operation_consumer(operation: &CursorOperation) -> Option<ConsumerId> {
    match operation {
        CursorOperation::Register { consumer, .. }
        | CursorOperation::RegisterProtected { consumer, .. }
        | CursorOperation::Renew { consumer, .. }
        | CursorOperation::BeginSeed { consumer, .. }
        | CursorOperation::CompleteSeed { consumer, .. }
        | CursorOperation::RequireResync { consumer, .. } => Some(*consumer),
        CursorOperation::Acknowledge { token }
        | CursorOperation::AcknowledgeAndRenew { token, .. } => Some(token.key.consumer),
        CursorOperation::AdvanceFloor { .. } => None,
    }
}

impl DeltaSource for Session {
    fn bounds(&self) -> ReplayBounds {
        self.stream_bounds()
    }
    fn replay(
        &self,
        after: Position,
        limit: ReplayLimit,
        visit: &mut dyn FnMut(&Delta) -> Result<(), StreamError>,
    ) -> Result<Position, StreamError> {
        if self.failed {
            return Err(StreamError::SourceUnavailable);
        }
        self.validate_replay_position(after)?;
        if limit.max_items == 0 || limit.max_bytes == 0 || limit.max_sequences == 0 {
            return Err(StreamError::Invalid("zero replay budget"));
        }
        let through = SessionSeq(
            after
                .sequence
                .0
                .saturating_add(limit.max_sequences)
                .min(self.sequence().0),
        );
        let mut returned = after;
        let mut bytes = 0usize;
        let start = self
            .deltas
            .partition_point(|d| Position::after_delta(d.delta.id) <= after);
        for (items, delta) in self.deltas.range(start..).enumerate() {
            let position = Position::after_delta(delta.delta.id);
            if delta.delta.id.sequence > through {
                break;
            }
            if items >= limit.max_items || bytes.saturating_add(delta.bytes) > limit.max_bytes {
                if items == 0 {
                    return Err(StreamError::Capacity);
                }
                return Ok(returned);
            }
            visit(&delta.delta)?;
            bytes = bytes
                .checked_add(delta.bytes)
                .ok_or(StreamError::Capacity)?;
            returned = position;
        }
        Ok(Position::resolved(self.ledger, through))
    }
}

struct EncodedCheckpoint {
    bytes: Vec<u8>,
    retained: Option<(Vec<u8>, Allocation)>,
    _scratch: Allocation,
}
impl Session {
    /// Snapshot domain, cursor outcomes and the complete retained history tail
    /// together. Failure leaves the previous durable checkpoint/log authoritative.
    pub fn checkpoint(&mut self) -> Result<(), LedgerError> {
        let Some(encoded) = self.encode_checkpoint(false)? else {
            return Ok(());
        };
        self.consensus
            .checkpoint(self.applied_raft, encoded.bytes)?;
        Ok(())
    }
    fn encode_checkpoint(
        &mut self,
        retain_bytes: bool,
    ) -> Result<Option<EncodedCheckpoint>, LedgerError> {
        self.check()?;
        if self.pending_managed.is_some()
            || !self.pending.is_empty()
            || self.pending_cursor.is_some()
            || self.pending_maintenance.is_some()
            || self.pending_membership.is_some()
            || self.pending_placement.is_some()
            || self.pending_evidence.is_some()
        {
            return Err(LedgerError::Capacity);
        }
        // Raft reserves index zero for the empty prefix. Its durable identity
        // and log already recover this state; there is no snapshot to compact.
        // This also permits clean shutdown before the first election commits.
        if self.applied_raft == 0 {
            if self.sequence() != SessionSeq(0) || self.cursor_revision() != 0 {
                return Err(LedgerError::Corrupt);
            }
            return Ok(None);
        }
        self.graph.audit(self.core.snapshot())?;
        let tail_charge = self.deltas.iter().try_fold(0usize, |sum, d| {
            sum.checked_add(reference_charge(&d.delta)?)
                .ok_or(LedgerError::Capacity)
        })?;
        let cursor_state_charge = reference_charge(self.cursors.checkpoint())?;
        let placement_state_charge = reference_charge(&self.placement_state)?;
        let request_stream_charge = self.request_streams.checkpoint_charge()?;
        let amount = reference_charge(self.core.snapshot())?
            .checked_add(reference_charge(&self.cursor_meta)?)
            .and_then(|n| n.checked_add(cursor_state_charge))
            .and_then(|n| n.checked_add(placement_state_charge))
            .and_then(|n| n.checked_add(request_stream_charge))
            .and_then(|n| n.checked_add(tail_charge))
            .and_then(|n| {
                n.checked_add(self.membership_charge.as_ref().map_or(0, Allocation::bytes))
            })
            .and_then(|n| n.checked_mul(3))
            .ok_or(LedgerError::Capacity)?;
        let _scratch = self
            .budget
            .reserve(BudgetKind::Recovery, BudgetLane::Completion, amount)?
            .commit();
        let envelope = SnapshotEnvelopeV2 {
            schema: 2,
            ledger: self.ledger,
            raft_index: self.applied_raft,
            core: self.core.encode_checkpoint()?,
            cursors: self.cursors.checkpoint().clone(),
            cursor_meta: CursorMetadata {
                receipts: self.cursor_meta.receipts.clone(),
                owners: self.cursor_meta.owners.clone(),
            },
            delta_floor: self.stream_bounds().floor,
            deltas: self.deltas.iter().map(|d| d.delta.clone()).collect(),
        };
        let envelope = SnapshotEnvelopeV3 {
            state: envelope,
            membership: MembershipState {
                configuration_index: self.membership_state.configuration_index,
                latest: self.membership_state.latest.clone(),
            },
        };
        let mut bytes = if self.request_streams.activated {
            SNAPSHOT_V5_MAGIC.to_vec()
        } else if self.placement_state.latest().is_some() {
            SNAPSHOT_V4_MAGIC.to_vec()
        } else {
            SNAPSHOT_V3_MAGIC.to_vec()
        };
        if self.request_streams.activated {
            bytes.extend(postcard::to_stdvec(&SnapshotEnvelopeV5 {
                state: SnapshotEnvelopeV4 {
                    state: envelope,
                    placement: PlacementState {
                        active: self.placement_state.active.clone(),
                        cutover: self.placement_state.cutover.clone(),
                    },
                },
                requests: self.request_streams.checkpoint(),
            })?);
        } else if self.placement_state.latest().is_some() {
            bytes.extend(postcard::to_stdvec(&SnapshotEnvelopeV4 {
                state: envelope,
                placement: PlacementState {
                    active: self.placement_state.active.clone(),
                    cutover: self.placement_state.cutover.clone(),
                },
            })?);
        } else {
            bytes.extend(postcard::to_stdvec(&envelope)?);
        }
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(LedgerError::Capacity);
        }
        let retained = if retain_bytes {
            let allocation = self
                .budget
                .reserve(
                    BudgetKind::Recovery,
                    BudgetLane::Completion,
                    bytes.len().checked_add(4096).ok_or(LedgerError::Capacity)?,
                )?
                .commit();
            let mut copy = Vec::new();
            copy.try_reserve_exact(bytes.len())
                .map_err(|_| LedgerError::Capacity)?;
            copy.extend_from_slice(&bytes);
            Some((copy, allocation))
        } else {
            None
        };
        Ok(Some(EncodedCheckpoint {
            bytes,
            retained,
            _scratch,
        }))
    }

    fn restore_snapshot(
        &mut self,
        data: &[u8],
        index: u64,
        term: u64,
        configuration: &MembershipConfiguration,
    ) -> Result<(), LedgerError> {
        let mut membership = MembershipState::default();
        let mut placement = PlacementState::default();
        let mut requests = RequestStreamsCheckpoint::default();
        let envelope = if let Some(data) = data.strip_prefix(SNAPSHOT_V5_MAGIC) {
            if !self.consensus.decoder_floor_ready(managed_format_hash()) {
                return Err(LedgerError::Corrupt);
            }
            let (envelope, remaining): (SnapshotEnvelopeV5, _) = postcard::take_from_bytes(data)?;
            if !remaining.is_empty()
                || envelope.state.state.state.schema != 2
                || !envelope.requests.activated
            {
                return Err(LedgerError::Corrupt);
            }
            membership = envelope.state.state.membership;
            placement = envelope.state.placement;
            requests = envelope.requests;
            envelope.state.state.state
        } else if let Some(data) = data.strip_prefix(SNAPSHOT_V4_MAGIC) {
            let (envelope, remaining): (SnapshotEnvelopeV4, _) = postcard::take_from_bytes(data)?;
            if !remaining.is_empty() || envelope.state.state.schema != 2 {
                return Err(LedgerError::Corrupt);
            }
            membership = envelope.state.membership;
            placement = envelope.placement;
            envelope.state.state
        } else if let Some(data) = data.strip_prefix(SNAPSHOT_V3_MAGIC) {
            let (envelope, remaining): (SnapshotEnvelopeV3, _) = postcard::take_from_bytes(data)?;
            if !remaining.is_empty() {
                return Err(LedgerError::Corrupt);
            }
            membership = envelope.membership;
            if envelope.state.schema != 2 {
                return Err(LedgerError::Corrupt);
            }
            envelope.state
        } else if data.starts_with(SNAPSHOT_V2_MAGIC) {
            let envelope: SnapshotEnvelopeV2 = postcard::from_bytes(
                data.strip_prefix(SNAPSHOT_V2_MAGIC)
                    .ok_or(LedgerError::Corrupt)?,
            )?;
            if envelope.schema != 2 {
                return Err(LedgerError::Corrupt);
            }
            envelope
        } else if data.starts_with(SNAPSHOT_MAGIC) {
            let old: SnapshotEnvelope = postcard::from_bytes(
                data.strip_prefix(SNAPSHOT_MAGIC)
                    .ok_or(LedgerError::Corrupt)?,
            )?;
            if old.schema != 1 {
                return Err(LedgerError::Corrupt);
            }
            let recovered = Core::decode_checkpoint(&old.core)?;
            let prefix = recovered.sequence();
            SnapshotEnvelopeV2 {
                schema: 2,
                ledger: old.ledger,
                raft_index: old.raft_index,
                core: old.core,
                cursors: CursorCheckpoint {
                    schema: 1,
                    ledger: old.ledger,
                    revision: 0,
                    clock: 0,
                    floor: prefix,
                    consumers: BTreeMap::new(),
                },
                cursor_meta: CursorMetadata::default(),
                delta_floor: prefix,
                deltas: Vec::new(),
            }
        } else {
            return Err(LedgerError::Corrupt);
        };
        if self.placement_state.latest().is_some() && placement.latest().is_none() {
            return Err(LedgerError::Corrupt);
        }
        if envelope.ledger != self.ledger
            || envelope.raft_index != index
            || envelope.cursors.ledger != self.ledger
        {
            return Err(LedgerError::Corrupt);
        }
        if membership.configuration_index > index {
            return Err(LedgerError::Corrupt);
        }
        let membership_charge = if let Some(receipt) = &membership.latest {
            if receipt.id == [0; 16]
                || receipt.index == 0
                || receipt.term == 0
                || receipt.term > term
                || receipt.index != membership.configuration_index
                || &receipt.configuration != configuration
            {
                return Err(LedgerError::Corrupt);
            }
            receipt.configuration.validate()?;
            Some(
                self.budget
                    .reserve(
                        BudgetKind::Control,
                        BudgetLane::Completion,
                        receipt
                            .configuration
                            .charged_bytes()?
                            .checked_add(512)
                            .ok_or(LedgerError::Capacity)?,
                    )?
                    .commit(),
            )
        } else {
            None
        };
        let recovered = Core::decode_checkpoint(&envelope.core)?;
        self.validate_placement_snapshot(&placement, index, term, recovered.sequence())?;
        let placement_charge = if placement.latest().is_some() {
            Some(
                self.budget
                    .reserve(
                        BudgetKind::Control,
                        BudgetLane::Completion,
                        placement_charge(&placement)?,
                    )?
                    .commit(),
            )
        } else {
            None
        };
        if recovered.snapshot().ledger != self.ledger
            || envelope.delta_floor > recovered.sequence()
            || envelope.delta_floor < envelope.cursors.floor
            || envelope.cursor_meta.receipts.len() > self.limits.cursor_receipts
            || recovered.snapshot().receipts.len() > self.limits.core.max_requests
            || envelope.cursor_meta.owners.len() != envelope.cursors.consumers.len()
        {
            return Err(LedgerError::Corrupt);
        }
        for (consumer, owner) in &envelope.cursor_meta.owners {
            if owner.is_zero() || !envelope.cursors.consumers.contains_key(consumer) {
                return Err(LedgerError::Corrupt);
            }
        }
        for (key, receipt) in &envelope.cursor_meta.receipts {
            if *key != receipt.key
                || key.principal.is_zero()
                || key.id.is_zero()
                || receipt.ledger != self.ledger
                || receipt.domain_sequence > recovered.sequence()
                || receipt.raft_index > index
                || receipt.raft_index == 0
                || receipt.revision == 0
                || receipt.revision > envelope.cursors.revision
                || receipt.floor > receipt.domain_sequence
                || recovered.snapshot().receipts.contains_key(key)
                || !recovered
                    .snapshot()
                    .epochs
                    .get(&key.principal)
                    .is_some_and(|w| w.admitted.contains(&key.epoch) || key.epoch < w.minimum)
            {
                return Err(LedgerError::Corrupt);
            }
            if let Some(record) = &receipt.record
                && (record.token.key.ledger != self.ledger
                    || record.token.position.ledger != self.ledger
                    || record.token.position.sequence > receipt.domain_sequence
                    || envelope.cursor_meta.owners.get(&record.token.key.consumer)
                        != Some(&key.principal))
            {
                return Err(LedgerError::Corrupt);
            }
        }
        let cursors = CursorRegistry::restore(
            envelope.cursors,
            recovered.sequence(),
            self.limits.cursors,
            self.budget.clone(),
        )?;
        if cursors.retention_limit(recovered.sequence()) < envelope.delta_floor {
            return Err(LedgerError::Corrupt);
        }
        if envelope.deltas.len() > self.limits.delta_items {
            return Err(LedgerError::Capacity);
        }
        let capacity = if envelope.deltas.is_empty() {
            0
        } else {
            self.limits.delta_items
        };
        let slot_bytes = capacity
            .checked_mul(size_of::<RetainedDelta>())
            .and_then(|n| n.checked_add(usize::from(capacity != 0).saturating_mul(64)))
            .ok_or(LedgerError::Capacity)?;
        let delta_slots = self
            .budget
            .reserve(BudgetKind::Payload, BudgetLane::Completion, slot_bytes)?
            .commit();
        let mut retained = VecDeque::new();
        retained
            .try_reserve_exact(capacity)
            .map_err(|_| LedgerError::Capacity)?;
        let mut bytes = 0usize;
        let mut previous: Option<DeltaId> = None;
        for delta in envelope.deltas {
            if delta.schema != 1
                || delta.id.ledger != self.ledger
                || delta.id.sequence <= envelope.delta_floor
                || delta.id.sequence > recovered.sequence()
            {
                return Err(LedgerError::Corrupt);
            }
            match previous {
                Some(old) if old.sequence == delta.id.sequence => {
                    if old.ordinal.checked_add(1) != Some(delta.id.ordinal) {
                        return Err(LedgerError::Corrupt);
                    }
                }
                old => {
                    if delta.id.ordinal != 0 || old.is_some_and(|old| old >= delta.id) {
                        return Err(LedgerError::Corrupt);
                    }
                }
            }
            let size = postcard::experimental::serialized_size(&delta)?;
            bytes = bytes.checked_add(size).ok_or(LedgerError::Capacity)?;
            if bytes > self.limits.delta_bytes {
                return Err(LedgerError::Capacity);
            }
            let allocation = self
                .budget
                .reserve(
                    BudgetKind::Payload,
                    BudgetLane::Completion,
                    reference_charge(&delta)?,
                )?
                .commit();
            previous = Some(delta.id);
            retained.push_back(RetainedDelta {
                bytes: size,
                delta,
                _charge: allocation,
            });
        }
        let core_charge = self
            .budget
            .reserve(
                BudgetKind::Payload,
                BudgetLane::Completion,
                reference_charge(recovered.snapshot())?,
            )?
            .commit();
        let cursor_charge = self
            .budget
            .reserve(
                BudgetKind::ReadPins,
                BudgetLane::Completion,
                reference_charge(&envelope.cursor_meta)?,
            )?
            .commit();
        let graph = GraphStore::from_state(
            recovered.snapshot(),
            RangeId(u128::from_be_bytes(self.ledger.session.0)),
            self.limits.graph,
            self.budget.clone(),
        )?;
        self.clear_pending();
        self.pending_cursor = None;
        self.pending_managed = None;
        self.managed_support = ManagedSupportCache::default();
        self.request_streams
            .restore(requests, recovered.sequence(), index, &self.budget)?;
        self.pending_maintenance = None;
        self.pending_membership = None;
        self.pending_placement = None;
        self.placement_state = placement;
        self.placement_charge = placement_charge;
        self.membership_state = membership;
        self.membership_charge = membership_charge;
        self.graph = graph;
        self.core = recovered;
        self.core_charge = self
            .budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 0)?
            .commit();
        self.core_completion_charge = core_charge;
        self.cursors = cursors;
        self.cursor_meta = envelope.cursor_meta;
        self.cursor_charge = cursor_charge;
        self.deltas = retained;
        self._delta_slots = delta_slots;
        self.delta_bytes = bytes;
        self.delta_floor = envelope.delta_floor;
        self.applied_raft = index;
        Ok(())
    }
}
