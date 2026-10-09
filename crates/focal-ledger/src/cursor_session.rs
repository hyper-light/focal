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
/// `domain_sequence` is the published end of the ledger's stream line when
/// the entry applied (the domain sequence on a legacy ledger, 23 §6), so it
/// bounds every position the receipt's record names.
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
    #[serde(with = "focal_memory::serde_bytes")]
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
    receipt: CursorReceipt,
    /// The consumer this command names first, and who names it: its owner
    /// once the command applies.
    owner: Option<(ConsumerId, ParticipantId)>,
    /// The receipt's entry and the owner's, admitted before proposal and
    /// joined to the metadata's charge at apply — never a copy of the maps
    /// (the audit's F61).
    entry_charge: Allocation,
    result_charge: Allocation,
}

/// What the cursor metadata holds for one receipt, and for one owner:
/// entries are charged as they arrive and released as they leave.
fn receipt_entry_charge(key: &RequestKey, receipt: &CursorReceipt) -> Result<usize, LedgerError> {
    Ok(reference_charge(&(key, receipt))?)
}
fn owner_entry_charge(consumer: &ConsumerId, principal: &ParticipantId) -> Result<usize, LedgerError> {
    Ok(reference_charge(&(consumer, principal))?)
}
/// The metadata's charge as its entries sum: what a restored metadata is
/// charged, and what the per-entry charges keep it at.
fn metadata_charge(metadata: &CursorMetadata) -> Result<usize, LedgerError> {
    let mut charge = reference_charge(&CursorMetadata::default())?;
    for (key, receipt) in &metadata.receipts {
        charge = charge
            .checked_add(receipt_entry_charge(key, receipt)?)
            .ok_or(LedgerError::Capacity)?;
    }
    for (consumer, principal) in &metadata.owners {
        charge = charge
            .checked_add(owner_entry_charge(consumer, principal)?)
            .ok_or(LedgerError::Capacity)?;
    }
    Ok(charge)
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
            published: self.stream_published(),
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
        let status = self.scalars();
        let members = self.members();
        if members.voters != [status.node_id] || !members.learners.is_empty() {
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
        let status = self.scalars();
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
        if postcard::experimental::serialized_size(&focal_model::durable_v1::Ref(input))?
            > self.limits.core.max_command_bytes
        {
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
        let data = durable_session_v1::encode(CURSOR_MAGIC, &envelope, usize::MAX)?;
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
        if postcard::experimental::serialized_size(&focal_model::durable_v1::Ref(input))?
            > self.limits.core.max_command_bytes
        {
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
        let scratch_bytes = reference_charge(input)?
            .checked_add(self.placement_charge.as_ref().map_or(0, Allocation::bytes))
            .and_then(|n| n.checked_mul(3))
            .ok_or(LedgerError::Capacity)?;
        let _scratch =
            self.budget
                .reserve(BudgetKind::Pending, BudgetLane::Completion, scratch_bytes)?;
        // Positions are validated against the stream line, not the legacy
        // domain sequence; the receipt keeps naming the domain sequence.
        let published = self.stream_published();
        let prepared = if committed {
            self.cursors
                .prepare_committed(&input.command, published)?
        } else {
            self.cursors.prepare(&input.command, published)?
        };
        // A lease clock advance can release projection pins, never protected pins.
        let receipt = CursorReceipt {
            ledger: self.ledger,
            key: input.key,
            intent_hash: input.intent_hash,
            revision: prepared.revision(),
            domain_sequence: self.sequence(),
            raft_index: 0,
            floor: envelope.replay_floor.max(prepared.floor()),
            record: consumer.and_then(|id| self.cursors.projected(&prepared, id)),
        };
        // A consumer named for the first time is owned by its principal.
        let owner = consumer
            .filter(|id| self.cursor_owner(*id).is_none())
            .map(|id| (id, input.key.principal));
        let entry_bytes = receipt_entry_charge(&input.key, &receipt)?
            .checked_add(owner.map_or(Ok(0), |(id, principal)| {
                owner_entry_charge(&id, &principal)
            })?)
            .ok_or(LedgerError::Capacity)?;
        let entry_charge = self
            .budget
            .reserve(BudgetKind::ReadPins, BudgetLane::Completion, entry_bytes)?
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
            receipt,
            owner,
            entry_charge,
            result_charge,
        })
    }
    /// A retired consumer's name is free: its owner leaves with its row, so
    /// whoever registers the name next owns it.
    fn retire_owners(&mut self, retired: &[ConsumerId]) -> Result<(), LedgerError> {
        for consumer in retired {
            if let Some(principal) = self.cursor_meta.owners.remove(consumer) {
                let bytes = owner_entry_charge(consumer, &principal)?;
                self.cursor_charge
                    .shrink_to(self.cursor_charge.bytes().saturating_sub(bytes))?;
            }
        }
        Ok(())
    }
    fn apply_cursor_entry(
        &mut self,
        data: &[u8],
        raft_index: u64,
    ) -> Result<(CursorReceipt, Allocation), LedgerError> {
        self.pending_maintenance = None;
        let digest = ContentHash(*blake3::hash(data).as_bytes());
        let candidate = if self
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
                // CU1/CU2 historically ignore a trailing body suffix. Preserve
                // that decode rule for these tags; snapshots remain exact.
                durable_session_v1::take::<CursorEnvelope>(
                    data.strip_prefix(CURSOR_MAGIC)
                        .ok_or(LedgerError::Corrupt)?,
                )?
                .0
            } else {
                let (old, _): (LegacyCursorEnvelope, _) = durable_session_v1::take(
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
        // The receipt names the published end of the stream line at apply,
        // computed identically on every replica (23 §6); the candidate's
        // proposal-time value bounded the positions it validated.
        let published = self.stream_published();
        let CursorCandidate {
            prepared,
            mut receipt,
            owner,
            mut entry_charge,
            result_charge,
            ..
        } = candidate;
        receipt.raft_index = raft_index;
        receipt.domain_sequence = published;
        let retired = self.cursors.publish(prepared)?;
        self.retire_owners(&retired)?;
        if let Some((consumer, principal)) = owner {
            self.cursor_meta.owners.entry(consumer).or_insert(principal);
        }
        self.cursor_meta.receipts.insert(receipt.key, receipt.clone());
        self.cursor_charge.absorb(&mut entry_charge)?;
        self.retire_deltas(receipt.floor)?;
        Ok((receipt, result_charge))
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
        position.validate(self.ledger, self.stream_published())?;
        if position.retention_prefix() < self.stream_bounds().floor {
            return Err(StreamError::ResyncRequired(ResyncReason::HistoryExpired));
        }
        if let PositionOffset::Delta(ordinal) = position.offset {
            if position.sequence > self.legacy_prefix() {
                if !self.native_delta_exists(position, ordinal) {
                    return Err(StreamError::Invalid(
                        "delta cursor does not name a retained delta",
                    ));
                }
                return Ok(());
            }
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
        self.replay_deltas(after, limit, visit)
    }
}

struct EncodedCheckpoint {
    bytes: Vec<u8>,
    retained: Option<(Vec<u8>, Allocation)>,
    /// How long the native engine's section took on the owner: encoded with
    /// its seeds installed, or, deferred, captured.
    native: std::time::Duration,
    /// A deferred checkpoint's native section, captured and still to encode,
    /// and the envelope it joins; `bytes` is empty until then.
    deferred: Option<DeferredNative>,
    _scratch: Allocation,
}

/// What a deferred checkpoint's thread is given and what the owner keeps:
/// the native section captured at the point, the seed batch its root seals
/// into, and the envelope's head and slot generation taken at the same point.
struct DeferredNative {
    captured: crate::native_session::CapturedCheckpoint,
    batch: Result<focal_evidence::SeedBatch, focal_evidence::ContentError>,
    head: Vec<u8>,
    slot_generation: u64,
}

/// What a deferred checkpoint's thread made of the capture: the native
/// section's bytes with their permit, its seeds durable; the checkpoint itself,
/// staged as the group's next image where consensus stages one, else whole for
/// the owner to write; and how long the thread took.
struct Staged {
    native: Vec<u8>,
    _native_permit: Allocation,
    image: StagedCheckpoint,
    elapsed: std::time::Duration,
}

/// A deferred checkpoint's image as its thread left it.
enum StagedCheckpoint {
    /// Written and durable as the group's next image, for the owner to adopt.
    Staged(focal_consensus::StagedImage),
    /// The checkpoint's bytes, for the owner to write: consensus stages none.
    Whole(Vec<u8>),
}

/// What a deferred checkpoint's thread tells its owner, in order: what it
/// staged, then, once the owner adopted the image, whether its directory is
/// durable.
enum Told {
    Staged(Result<Staged, LedgerError>),
    Settled(Result<(), focal_consensus::ConsensusError>),
}

/// A checkpoint whose state the owner has captured at a point (the native
/// rows frozen, the envelope's other sections encoded), its native root being
/// encoded, its seed chunks made durable and the group's next image written
/// and made durable on a thread of its own while the replica goes on (Ongaro's
/// thesis §5.1: the state machine continues while its snapshot is written; as
/// Redis's fork-time image or a read transaction's snapshot in etcd's bbolt).
/// The owner adopts the image, a rename, only once everything it names is
/// durable, and takes it as the group's durable image, compacting the log
/// behind it, only once the thread made the rename durable; the entries after
/// the point stay in the log. The owner never waits on the disk. Dropped, it
/// waits for its thread: nothing writes the group's files after its owner let
/// it go.
pub(crate) struct DeferredCheckpoint {
    point: focal_consensus::CheckpointPoint,
    native: std::time::Duration,
    envelope: std::time::Duration,
    told: std::sync::mpsc::Receiver<Told>,
    /// Whether the thread is to make an adopted image's name durable: sent
    /// once, after the owner adopted the image or refused it.
    adopted: Option<std::sync::mpsc::SyncSender<bool>>,
    /// The native section of an image the owner adopted, kept until the image
    /// is the group's durable one, and the owner's time spent on it so far.
    adoption: Option<Adoption>,
    worker: Option<std::thread::JoinHandle<()>>,
    _scratch: Allocation,
}

/// An adopted image's native section and the owner's time spent on it.
struct Adoption {
    bytes: u64,
    native: Vec<u8>,
    _native_permit: Allocation,
    owner: std::time::Duration,
    deferred: std::time::Duration,
}

/// A deferred checkpoint's thread ended without telling its result.
fn lost_worker() -> NativeSessionError {
    NativeSessionError::from(crate::native_checkpoint::Error::from(
        focal_evidence::ContentError::Failed,
    ))
}

impl Drop for DeferredCheckpoint {
    fn drop(&mut self) {
        // A thread waiting to hear whether its image was adopted hears it was
        // not, and ends.
        drop(self.adopted.take());
        if let Some(worker) = self.worker.take() {
            // A worker that did not return failed its commit; the result it
            // owed is what `poll` reads as a failure.
            let _ = worker.join();
        }
    }
}

/// The deferred checkpoint's thread: the native root encoded and its seeds
/// made durable, the envelope finished, the image staged; then, if the owner
/// adopts it, the directory made durable.
fn run_deferred(
    capture: DeferredNative,
    point: &focal_consensus::CheckpointPoint,
    stager: Option<&focal_consensus::ImageStager>,
    told: &std::sync::mpsc::SyncSender<Told>,
    adopted: &std::sync::mpsc::Receiver<bool>,
) {
    let DeferredNative {
        captured,
        batch,
        head,
        slot_generation,
    } = capture;
    let started = std::time::Instant::now();
    let encoded = captured
        .encode(batch)
        .and_then(|(bytes, allocation, commit)| {
            if let Some(commit) = commit {
                commit.run().map_err(|error| {
                    NativeSessionError::from(crate::native_checkpoint::Error::from(error))
                })?;
            }
            Ok((bytes, allocation))
        })
        .map_err(LedgerError::from);
    // The frozen rows go before anything is told: their pages are released by
    // the time the owner reads it.
    drop(captured);
    let staged = encoded.and_then(|(native, native_permit)| {
        let whole = durable_session_v1::snapshot_native_finish(head, &native, slot_generation)?;
        let image = match stager {
            Some(stager) => StagedCheckpoint::Staged(stager.stage(point, &whole)?),
            None => StagedCheckpoint::Whole(whole),
        };
        Ok(Staged {
            native,
            _native_permit: native_permit,
            image,
            elapsed: started.elapsed(),
        })
    });
    let waits = matches!(
        &staged,
        Ok(Staged {
            image: StagedCheckpoint::Staged(_),
            ..
        })
    );
    // The receiver may be gone (the session dropped): the result then has no
    // one to tell, and the commit is complete or not.
    if told.send(Told::Staged(staged)).is_err() || !waits {
        return;
    }
    // Refused, or the owner gone: the staged file stays until the next is
    // staged over it, and a start never reads it.
    if adopted.recv() != Ok(true) {
        return;
    }
    let synced = stager.map_or(Ok(()), focal_consensus::ImageStager::settle);
    let _ = told.send(Told::Settled(synced));
}

impl Session {
    /// Snapshot domain, cursor outcomes and the complete retained history tail
    /// together. Failure leaves the previous durable checkpoint/log authoritative.
    pub fn checkpoint(&mut self) -> Result<(), LedgerError> {
        // One checkpoint at a time: a deferred one is finished first.
        self.settle_deferred_checkpoint()?;
        let started = std::time::Instant::now();
        let Some(encoded) = self.encode_checkpoint(false)? else {
            return Ok(());
        };
        let encoded_at = std::time::Instant::now();
        let index = self.applied_raft;
        let bytes = u64::try_from(encoded.bytes.len()).unwrap_or(u64::MAX);
        let native = encoded.native;
        self.consensus.checkpoint(index, encoded.bytes)?;
        let encoding = encoded_at.saturating_duration_since(started);
        self.checkpoint_timings.push(CheckpointTiming {
            index,
            bytes,
            native_micros: micros(native),
            envelope_micros: micros(encoding.saturating_sub(native)),
            deferred_micros: 0,
            write_micros: micros(encoded_at.elapsed()),
        });
        Ok(())
    }

    /// Whether a deferred checkpoint is still being made durable.
    pub fn deferred_checkpoint_pending(&self) -> bool {
        self.deferred.is_some()
    }

    /// Checkpoint the applied prefix without the owner waiting on the disk:
    /// the state is captured here; its seed chunks and the group's next image
    /// are made durable on a thread of their own, and
    /// `poll_deferred_checkpoint` adopts the image and, once its name is
    /// durable, takes it as the group's. A root small enough to travel inline
    /// has no chunks and is written at once, as `checkpoint` does.
    pub fn begin_deferred_checkpoint(&mut self) -> Result<(), LedgerError> {
        if self.deferred.is_some() {
            return Err(LedgerError::Capacity);
        }
        let started = std::time::Instant::now();
        let point = self.consensus.checkpoint_point()?;
        if point.index != self.applied_raft {
            return Err(focal_consensus::ConsensusError::CheckpointIndex.into());
        }
        let stager = self.consensus.image_stager()?;
        let Some(mut encoded) = self.encode_checkpoint_with(false, true)? else {
            return Ok(());
        };
        let encoded_at = std::time::Instant::now();
        let native = encoded.native;
        let envelope = encoded_at
            .saturating_duration_since(started)
            .saturating_sub(native);
        let Some(deferred) = encoded.deferred.take() else {
            // No native engine: the envelope is whole already.
            let bytes = std::mem::take(&mut encoded.bytes);
            return self.write_checkpoint(point, bytes, native, envelope, encoded_at, None);
        };
        // The thread tells at most two things, and hears one.
        let (tell, told) = std::sync::mpsc::sync_channel(2);
        let (adopt, adopted) = std::sync::mpsc::sync_channel(1);
        let at = point.clone();
        let worker = std::thread::Builder::new()
            .name("focal-checkpoint".into())
            .spawn(move || {
                run_deferred(
                    deferred,
                    &at,
                    stager.as_ref(),
                    &tell,
                    &adopted,
                );
            })
            // No thread: the capture and its batch were dropped with the
            // closure, the batch's files removed; the next period tries again.
            .map_err(|_| LedgerError::Capacity)?;
        self.deferred = Some(Box::new(DeferredCheckpoint {
            point,
            native,
            envelope,
            told,
            adopted: Some(adopt),
            adoption: None,
            worker: Some(worker),
            _scratch: encoded._scratch,
        }));
        Ok(())
    }

    /// Move a deferred checkpoint on by what its thread told since: adopt its
    /// staged image, or take an adopted one as the group's durable image; true
    /// when it finished (written, or found covered by a later checkpoint and
    /// dropped). Never waits.
    pub fn poll_deferred_checkpoint(&mut self) -> Result<bool, LedgerError> {
        // A thread tells at most two things: both may be waiting.
        for _ in 0..2 {
            let Some(deferred) = self.deferred.as_ref() else {
                return Ok(false);
            };
            let told = match deferred.told.try_recv() {
                Ok(told) => Ok(told),
                Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(false),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => Err(lost_worker()),
            };
            if self.hear_deferred(told)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Wait for a deferred checkpoint, if one is pending, and finish it:
    /// bounded by its seeds' commit and its image's two syncs.
    fn settle_deferred_checkpoint(&mut self) -> Result<(), LedgerError> {
        for _ in 0..2 {
            let Some(deferred) = self.deferred.as_ref() else {
                return Ok(());
            };
            let told = deferred.told.recv().map_err(|_| lost_worker());
            if self.hear_deferred(told)? {
                return Ok(());
            }
        }
        // A thread that told two things finished, or the second was its last.
        match self.deferred.take() {
            Some(_) => Err(lost_worker().into()),
            None => Ok(()),
        }
    }

    /// What the deferred checkpoint's thread told, acted on; true when the
    /// checkpoint finished.
    fn hear_deferred(&mut self, told: Result<Told, NativeSessionError>) -> Result<bool, LedgerError> {
        let writing = std::time::Instant::now();
        match told {
            Err(lost) => {
                self.deferred = None;
                Err(lost.into())
            }
            Ok(Told::Staged(Err(error))) => {
                self.deferred = None;
                // A seed write or commit that failed fails the store, as one
                // on the owner's thread does: it refuses until reopened.
                if let LedgerError::Native(NativeSessionError::Checkpoint(
                    crate::native_checkpoint::Error::Seeds(_),
                )) = &error
                    && let Some(hosting) = self.hosting.as_mut()
                {
                    hosting.seeds.fail();
                }
                Err(error)
            }
            Ok(Told::Staged(Ok(staged))) => self.adopt_deferred(staged, writing),
            Ok(Told::Settled(synced)) => {
                let Some(mut deferred) = self.deferred.take() else {
                    return Ok(true);
                };
                let adoption = deferred.adoption.take().ok_or_else(lost_worker)?;
                self.consensus
                    .settle_staged_checkpoint(&deferred.point, synced)?;
                // The image is the group's durable one: its chunks alone are
                // kept from here on.
                self.native
                    .as_deref_mut()
                    .ok_or(LedgerError::NativeUnsupported)?
                    .note_checkpoint_seeds(&adoption.native)?;
                let owner = adoption.owner.saturating_add(writing.elapsed());
                self.note_checkpoint_timing(&deferred, adoption.bytes, adoption.deferred, owner);
                Ok(true)
            }
        }
    }

    /// A deferred checkpoint's staged result: an image adopted, its chunks
    /// kept with the durable image's until it is the group's durable one; or,
    /// where consensus stages none, the checkpoint written whole.
    fn adopt_deferred(
        &mut self,
        staged: Staged,
        writing: std::time::Instant,
    ) -> Result<bool, LedgerError> {
        let Staged {
            native,
            _native_permit,
            image,
            elapsed,
        } = staged;
        let image = match image {
            StagedCheckpoint::Whole(bytes) => {
                let Some(deferred) = self.deferred.take() else {
                    return Ok(true);
                };
                let point = deferred.point.clone();
                let written = self.write_checkpoint(
                    point,
                    bytes,
                    deferred.native,
                    deferred.envelope,
                    writing,
                    Some(elapsed),
                );
                return match written {
                    // A later checkpoint or snapshot already covers the point,
                    // or the configuration moved past it: this one is not
                    // needed, and the next period takes a new one.
                    Err(LedgerError::Consensus(
                        focal_consensus::ConsensusError::CheckpointIndex,
                    )) => Ok(true),
                    Err(error) => Err(error),
                    // Written and durable: its chunks are the ones kept.
                    Ok(()) => self
                        .native
                        .as_deref_mut()
                        .ok_or(LedgerError::NativeUnsupported)?
                        .note_checkpoint_seeds(&native)
                        .map(|()| true)
                        .map_err(LedgerError::from),
                };
            }
            StagedCheckpoint::Staged(image) => image,
        };
        let Some(deferred) = self.deferred.as_deref_mut() else {
            return Ok(true);
        };
        let go = deferred.adopted.take();
        // The image's chunks are kept before its name can reach the disk.
        let adopted = match self.native.as_deref_mut() {
            Some(engine) => engine
                .stage_checkpoint_seeds(&native)
                .map_err(LedgerError::from),
            None => Err(LedgerError::NativeUnsupported),
        }
        .and_then(|()| {
            self.consensus
                .adopt_staged_checkpoint(&deferred.point, &image)
                .map_err(LedgerError::from)
        });
        match adopted {
            Ok(()) => {
                deferred.adoption = Some(Adoption {
                    bytes: image.bytes(),
                    native,
                    _native_permit,
                    owner: writing.elapsed(),
                    deferred: elapsed,
                });
                // A thread gone already is heard as lost at the next poll.
                if let Some(go) = go {
                    let _ = go.send(true);
                }
                Ok(false)
            }
            Err(error) => {
                // The thread hears the image was not adopted and ends.
                drop(go);
                self.deferred = None;
                if let Some(engine) = self.native.as_deref_mut() {
                    engine.unstage_checkpoint_seeds();
                }
                match error {
                    // Covered by a later image, or the configuration moved
                    // past it: the next period takes a new one.
                    LedgerError::Consensus(focal_consensus::ConsensusError::CheckpointIndex) => {
                        Ok(true)
                    }
                    error => Err(error),
                }
            }
        }
    }

    fn note_checkpoint_timing(
        &mut self,
        deferred: &DeferredCheckpoint,
        bytes: u64,
        elapsed: std::time::Duration,
        owner: std::time::Duration,
    ) {
        self.checkpoint_timings.push(CheckpointTiming {
            index: deferred.point.index,
            bytes,
            native_micros: micros(deferred.native),
            envelope_micros: micros(deferred.envelope),
            deferred_micros: micros(elapsed),
            write_micros: micros(owner),
        });
    }

    fn write_checkpoint(
        &mut self,
        point: focal_consensus::CheckpointPoint,
        bytes: Vec<u8>,
        native: std::time::Duration,
        envelope: std::time::Duration,
        writing: std::time::Instant,
        deferred: Option<std::time::Duration>,
    ) -> Result<(), LedgerError> {
        let index = point.index;
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        self.consensus.begin_checkpoint_from(point, bytes)?;
        self.consensus.finish_checkpoint()?;
        self.checkpoint_timings.push(CheckpointTiming {
            index,
            bytes: size,
            native_micros: micros(native),
            envelope_micros: micros(envelope),
            deferred_micros: deferred.map_or(0, micros),
            write_micros: micros(writing.elapsed()),
        });
        Ok(())
    }
    fn encode_checkpoint(
        &mut self,
        retain_bytes: bool,
    ) -> Result<Option<EncodedCheckpoint>, LedgerError> {
        self.encode_checkpoint_with(retain_bytes, false)
    }
    /// The checkpoint's bytes; with `defer`, a root sealed as seeds leaves its
    /// chunks to the returned commit.
    fn encode_checkpoint_with(
        &mut self,
        retain_bytes: bool,
        defer: bool,
    ) -> Result<Option<EncodedCheckpoint>, LedgerError> {
        self.check()?;
        // A checkpoint is of the applied prefix, and a proposal in flight is
        // above it, in the log the checkpoint leaves: a domain candidate, a
        // managed or cursor command or a cursor maintenance changes the core,
        // the graph, the deltas, the cursors or the request streams only
        // when it applies (each is prepared from a copy), so it neither waits
        // for nor holds back the checkpoint. A replica a steady load kept
        // with some proposal pending checkpointed only at a period that
        // found none, and kept 182 entries past a cadence of 32 (26 §3). A
        // delivery under way is the prefix itself moving; a membership,
        // placement, evidence or activation record still in flight is rare,
        // one at a time and short, and waits as before.
        if self.pending_membership.is_some()
            || self.pending_placement.is_some()
            || self.pending_evidence.is_some()
            || self.pending_activation.is_some()
            || self.retained.is_some()
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
        // The native section is encoded under its own permit; the envelope
        // copies it once more into the final bytes.
        let native_started = std::time::Instant::now();
        let (native, captured) = match (self.native.as_deref_mut(), self.hosting.as_mut()) {
            (Some(engine), Some(hosting)) if defer && !retain_bytes => (
                None,
                Some((
                    engine.capture_checkpoint(&self.consensus)?,
                    hosting.seeds.batch(),
                )),
            ),
            (Some(engine), Some(hosting)) => (
                Some(engine.encode_checkpoint(&self.consensus, &mut hosting.seeds)?),
                None,
            ),
            (Some(_), None) => return Err(LedgerError::NativeUnsupported),
            (None, _) => (None, None),
        };
        let native_elapsed = native_started.elapsed();
        let native_bytes = native.as_ref().map_or(0, |(bytes, _)| bytes.len());
        let amount = reference_charge(self.core.snapshot())?
            .checked_add(native_bytes)
            .ok_or(LedgerError::Capacity)?
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
        let core = self.core.encode_checkpoint()?;
        if let Some((captured, batch)) = captured {
            let (head, slot_generation) = durable_session_v1::snapshot_native_head(self, &core)?;
            return Ok(Some(EncodedCheckpoint {
                bytes: Vec::new(),
                retained: None,
                native: native_elapsed,
                deferred: Some(DeferredNative {
                    captured,
                    batch,
                    head,
                    slot_generation,
                }),
                _scratch,
            }));
        }
        let bytes = durable_session_v1::snapshot(
            self,
            &core,
            native.as_ref().map(|(bytes, _)| bytes.as_slice()),
        )?;
        // Only the final Session bytes remain before optional retained copying.
        drop(core);
        drop(native);
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
            native: native_elapsed,
            deferred: None,
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
        let mut legacy_core = None;
        let mut native_section: Option<(Vec<u8>, Vec<u8>)> = None;
        // The registry watermark travels only in SS7; an SS6 checkpoint
        // never evicted a pair, so its largest retained generation is exact.
        let mut slot_generation = None;
        let envelope = if let Some(data) = data.strip_prefix(SNAPSHOT_V7_MAGIC) {
            if !self.consensus.decoder_floor_ready(native_format_hash()) {
                return Err(LedgerError::Corrupt);
            }
            let (envelope, remaining): (SnapshotEnvelopeV7, _) = durable_session_v1::take(data)?;
            if !remaining.is_empty() || envelope.state.state.state.schema != 2 {
                return Err(LedgerError::Corrupt);
            }
            membership = envelope.state.state.membership;
            placement = envelope.state.placement;
            requests = envelope.requests;
            native_section = Some((envelope.activation, envelope.native));
            slot_generation = Some(envelope.slot_generation);
            envelope.state.state.state
        } else if let Some(data) = data.strip_prefix(SNAPSHOT_V6_MAGIC) {
            if !self.consensus.decoder_floor_ready(native_format_hash()) {
                return Err(LedgerError::Corrupt);
            }
            let (envelope, remaining): (SnapshotEnvelopeV6, _) = durable_session_v1::take(data)?;
            if !remaining.is_empty() || envelope.state.state.state.schema != 2 {
                return Err(LedgerError::Corrupt);
            }
            membership = envelope.state.state.membership;
            placement = envelope.state.placement;
            requests = envelope.requests;
            native_section = Some((envelope.activation, envelope.native));
            envelope.state.state.state
        } else if let Some(data) = data.strip_prefix(SNAPSHOT_V5_MAGIC) {
            if !self.consensus.decoder_floor_ready(managed_format_hash()) {
                return Err(LedgerError::Corrupt);
            }
            let (envelope, remaining): (SnapshotEnvelopeV5, _) = durable_session_v1::take(data)?;
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
            let (envelope, remaining): (SnapshotEnvelopeV4, _) = durable_session_v1::take(data)?;
            if !remaining.is_empty() || envelope.state.state.schema != 2 {
                return Err(LedgerError::Corrupt);
            }
            membership = envelope.state.membership;
            placement = envelope.placement;
            envelope.state.state
        } else if let Some(data) = data.strip_prefix(SNAPSHOT_V3_MAGIC) {
            let (envelope, remaining): (SnapshotEnvelopeV3, _) = durable_session_v1::take(data)?;
            if !remaining.is_empty() {
                return Err(LedgerError::Corrupt);
            }
            membership = envelope.membership;
            if envelope.state.schema != 2 {
                return Err(LedgerError::Corrupt);
            }
            envelope.state
        } else if let Some(data) = data.strip_prefix(SNAPSHOT_V2_MAGIC) {
            let (envelope, remaining): (SnapshotEnvelopeV2, _) = durable_session_v1::take(data)?;
            if !remaining.is_empty() || envelope.schema != 2 {
                return Err(LedgerError::Corrupt);
            }
            envelope
        } else if let Some(data) = data.strip_prefix(SNAPSHOT_MAGIC) {
            let (old, remaining): (SnapshotEnvelope, _) = durable_session_v1::take(data)?;
            if !remaining.is_empty() || old.schema != 1 {
                return Err(LedgerError::Corrupt);
            }
            let recovered = Core::decode_checkpoint(&old.core)?;
            let prefix = recovered.sequence();
            // V1 lacks a separate domain prefix. Retain this decoded Core and
            // move it into publication instead of decoding the same bytes twice.
            legacy_core = Some(recovered);
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
        let recovered = match legacy_core {
            Some(core) => core,
            None => Core::decode_checkpoint(&envelope.core)?,
        };
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
        // The native engine is rebuilt completely before any legacy state is
        // replaced; a native ledger can never regress to a legacy-only
        // snapshot. Cursor positions are validated against the published end
        // of the stream line it defines (23 §6), never against the sealed
        // legacy prefix alone.
        let native = match &native_section {
            Some((activation, native)) => {
                match self.native_from_checkpoint(activation, native, index, term, configuration) {
                    Ok(restored) => Some(restored),
                    // The Core root is seeded and chunks are missing: record
                    // them for the host and keep the delivery retained; the
                    // engine that would have been built was not adopted.
                    Err(LedgerError::Native(NativeSessionError::CustodyPending)) => {
                        self.seed_pending = self.missing_seed(native, index, term)?;
                        return Err(LedgerError::Native(NativeSessionError::CustodyPending));
                    }
                    Err(error) => return Err(error),
                }
            }
            None if self.activation.is_native() => return Err(LedgerError::Corrupt),
            None => None,
        };
        self.seed_pending = None;
        let published = match &native {
            Some((engine, activation, _)) => {
                stream_line(*activation, recovered.sequence(), engine.sequence()?)?
            }
            None => recovered.sequence(),
        };
        for (key, receipt) in &envelope.cursor_meta.receipts {
            if *key != receipt.key
                || key.principal.is_zero()
                || key.id.is_zero()
                || receipt.ledger != self.ledger
                || receipt.domain_sequence > published
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
                    || record.token.position.sequence > published
                    || envelope.cursor_meta.owners.get(&record.token.key.consumer)
                        != Some(&key.principal))
            {
                return Err(LedgerError::Corrupt);
            }
        }
        let cursors = CursorRegistry::restore(
            envelope.cursors,
            published,
            self.limits.cursors,
            self.budget.clone(),
        )?;
        if cursors.retention_limit(published) < envelope.delta_floor {
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
            let size =
                postcard::experimental::serialized_size(&focal_model::durable_v1::Ref(&delta))?;
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
                metadata_charge(&envelope.cursor_meta)?,
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
        // Managed receipts are bounded by the stream line too: cursor
        // receipts name its published end at apply, domain receipts the
        // legacy prefix within it.
        self.request_streams
            .restore(requests, published, index, &self.budget, slot_generation)?;
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
        self.retained = None;
        self.pending_activation = None;
        if let Some((engine, activation, record)) = native {
            self.native = Some(engine);
            self.activation = activation;
            self.activation_record = Some(record);
        }
        Ok(())
    }
}
