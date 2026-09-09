const MANAGED_DOMAIN_MAGIC: &[u8] = b"FOCALMD1";
const MANAGED_CURSOR_MAGIC: &[u8] = b"FOCALMU1";
const REQUEST_STREAM_MAGIC: &[u8] = b"FOCALMS1";
const SNAPSHOT_V5_MAGIC: &[u8] = b"FOCALSS5";
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedSubmission {
    Committed(Box<ManagedReceipt>),
    Pending(ManagedRequestKey),
    Domain(DomainOutcome),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestStreamSubmission {
    Committed(Box<RequestStreamControlReceipt>),
    Pending(RequestId),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedCursorInput {
    pub key: ManagedRequestKey,
    pub intent_hash: ContentHash,
    pub command: CursorCommand,
}
#[derive(Debug)]
pub struct ManagedCommitted {
    pub receipt: ManagedReceipt,
    pub deltas: Vec<Delta>,
    pub effects: Vec<EffectIntent>,
}
#[derive(Serialize, Deserialize)]
struct ManagedCursorEnvelope {
    schema: u16,
    domain_sequence: SessionSeq,
    replay_floor: SessionSeq,
    trusted_control: bool,
    input: ManagedCursorInput,
}
#[derive(Serialize, Deserialize)]
struct RequestStreamEnvelope {
    schema: u16,
    domain_sequence: SessionSeq,
    input: RequestStreamControlInput,
}
#[derive(Serialize, Deserialize)]
struct SnapshotEnvelopeV5 {
    state: SnapshotEnvelopeV4,
    requests: RequestStreamsCheckpoint,
}
struct ManagedDomainCandidate {
    staged: focal_core::StagedManagedMutation,
    registry: PreparedStream,
    graph: PreparedGraph,
    retained: Vec<RetainedDelta>,
    floor: SessionSeq,
    core_bytes: usize,
    core_growth: Allocation,
    lane: BudgetLane,
    result_charge: Allocation,
    _stage_charge: Allocation,
}
struct ManagedCursorCandidate {
    key: ManagedRequestKey,
    prepared: PreparedCursorUpdate,
    metadata: CursorMetadata,
    metadata_charge: Allocation,
    registry: PreparedStream,
    result_charge: Allocation,
}
// This is owned inside one Box<PendingManaged>; another box per variant would
// add allocation and indirection without reducing the single bounded slot.
#[allow(clippy::large_enum_variant)]
enum ManagedCandidate {
    Domain(ManagedDomainCandidate),
    Cursor(ManagedCursorCandidate),
    Control(PreparedStream),
}
struct PendingManaged {
    entry_hash: ContentHash,
    candidate: ManagedCandidate,
    _charge: Allocation,
}
impl PendingManaged {
    fn key(&self) -> Option<ManagedRequestKey> {
        match &self.candidate {
            ManagedCandidate::Domain(c) => Some(c.staged.result().key),
            ManagedCandidate::Cursor(c) => Some(c.key),
            ManagedCandidate::Control(_) => None,
        }
    }
    fn family(&self) -> Option<ManagedRequestFamily> {
        match self.candidate {
            ManagedCandidate::Domain(_) => Some(ManagedRequestFamily::Domain),
            ManagedCandidate::Cursor(_) => Some(ManagedRequestFamily::Cursor),
            ManagedCandidate::Control(_) => None,
        }
    }
    fn intent(&self) -> Option<ContentHash> {
        match &self.candidate {
            ManagedCandidate::Domain(c) => Some(c.staged.result().command_hash),
            ManagedCandidate::Cursor(c) => c.registry.receipt(&c.key).map(|r| r.intent_hash),
            ManagedCandidate::Control(_) => None,
        }
    }
}
impl Session {
    pub fn request_stream_read(
        &self,
        principal: ParticipantId,
        query: &RequestStreamQuery,
    ) -> Result<crate::RequestStreamReadView<'_>, LedgerError> {
        self.check()?;
        self.request_streams
            .read(principal, query, self.sequence(), self.applied_raft)
    }
    /// Trusted local receipt/fence probe. A miss conveys no admission authority.
    pub fn managed_receipt(
        &self,
        key: &ManagedRequestKey,
        intent_hash: ContentHash,
        family: ManagedRequestFamily,
    ) -> Result<Option<&ManagedReceipt>, LedgerError> {
        self.check()?;
        Ok(self.request_streams.exact(key, intent_hash, family)?)
    }
    pub fn request_stream_receipt(
        &self,
        input: &RequestStreamControlInput,
    ) -> Result<Option<&RequestStreamControlReceipt>, LedgerError> {
        if input.ledger != self.ledger {
            return Err(ManagedError::InvalidIdentity.into());
        }
        self.request_stream_receipt_parts(input.cluster, input.principal, input.id, &input.command)
    }
    /// Borrow an authenticated control intent without copying its ACK manifest.
    /// A missing latest receipt conveys no admission authority.
    pub fn request_stream_receipt_parts(
        &self,
        cluster: [u8; 16],
        principal: ParticipantId,
        id: RequestId,
        command: &RequestStreamCommand,
    ) -> Result<Option<&RequestStreamControlReceipt>, LedgerError> {
        self.check()?;
        // Preserve the original control-input field order while borrowing the
        // ACK manifest. This bound participates in exact retained lookup.
        self.managed_input_size_view(&(
            focal_model::durable_v1::Ref(&cluster),
            focal_model::durable_v1::Ref(&self.ledger),
            focal_model::durable_v1::Ref(&principal),
            focal_model::durable_v1::Ref(&id),
            focal_model::durable_v1::Ref(command),
        ))?;
        self.request_streams
            .control_receipt_parts(cluster, self.ledger, principal, id, command)
    }
    fn managed_admission(&mut self) -> Result<(), LedgerError> {
        self.check()?;
        if !self.is_authoritative() {
            return Err(LedgerError::NotReady {
                leader: self.status().leader_id,
            });
        }
        self.require_managed_support()?;
        if self.pending_count() != 0 {
            return Err(LedgerError::Capacity);
        }
        Ok(())
    }
    fn managed_charge(&self, bytes: usize) -> Result<Allocation, LedgerError> {
        Ok(self
            .budget
            .reserve(BudgetKind::Pending, BudgetLane::Completion, bytes)?
            .commit())
    }
    pub fn propose_request_stream(
        &mut self,
        input: &RequestStreamControlInput,
    ) -> Result<RequestStreamSubmission, LedgerError> {
        self.check()?;
        if let Some(receipt) = self.request_stream_receipt(input)? {
            return Ok(RequestStreamSubmission::Committed(Box::new(
                receipt.clone(),
            )));
        }
        let hash = input.intent_hash().map_err(|_| ManagedError::Capacity)?;
        if let Some(PendingManaged {
            candidate: ManagedCandidate::Control(candidate),
            ..
        }) = self.pending_managed.as_deref()
            && let Some(receipt) = candidate.control_receipt()
            && receipt.id == input.id
            && receipt.principal == input.principal
        {
            return if receipt.intent_hash == hash {
                Ok(RequestStreamSubmission::Pending(input.id))
            } else {
                Err(ManagedError::Conflict.into())
            };
        }
        self.managed_admission()?;
        self.managed_input_size(input)?;
        let charge = self.managed_charge(
            reference_charge(input)?
                .checked_mul(3)
                .and_then(|n| n.checked_add(4096))
                .ok_or(LedgerError::Capacity)?,
        )?;
        let candidate =
            self.request_streams
                .prepare_control(input, self.sequence(), &self.budget)?;
        let envelope = RequestStreamEnvelope {
            schema: 1,
            domain_sequence: self.sequence(),
            input: input.clone(),
        };
        let data = durable_session_v1::encode(REQUEST_STREAM_MAGIC, &envelope, usize::MAX)?;
        let digest = ContentHash(*blake3::hash(&data).as_bytes());
        self.consensus.propose_in(data, BudgetLane::Completion)?;
        self.pending_managed = Some(Box::new(PendingManaged {
            entry_hash: digest,
            candidate: ManagedCandidate::Control(candidate),
            _charge: charge,
        }));
        Ok(RequestStreamSubmission::Pending(input.id))
    }
    pub fn propose_managed(
        &mut self,
        input: &ManagedAuthenticatedInput,
    ) -> Result<ManagedSubmission, LedgerError> {
        self.check()?;
        if self.activation.is_native() {
            return Ok(ManagedSubmission::Domain(DomainOutcome::refuse(
                ErrorCode::UnsupportedSchema,
                "legacy managed commands are refused after native activation",
            )));
        }
        self.managed_input_size(input)?;
        // The shared encoder streams the frozen canonical body into BLAKE3.
        let hash = managed_command_hash(input).map_err(|_| ManagedError::Capacity)?;
        if let Some(receipt) =
            self.managed_receipt(&input.key, hash, ManagedRequestFamily::Domain)?
        {
            return Ok(ManagedSubmission::Committed(Box::new(receipt.clone())));
        }
        if let Some(pending) = self.pending_managed.as_deref()
            && pending.key() == Some(input.key)
        {
            return if pending.intent() == Some(hash)
                && pending.family() == Some(ManagedRequestFamily::Domain)
            {
                Ok(ManagedSubmission::Pending(input.key))
            } else {
                Err(ManagedError::Conflict.into())
            };
        }
        self.managed_admission()?;
        if self.placement_state.paused() {
            return Err(LedgerError::Capacity);
        }
        let lane = mutation_lane(&input.command);
        let scratch = self
            .budget
            .reserve(BudgetKind::Pending, lane, self.staging_bytes())?
            .commit();
        let staged = match self.core.stage_managed_pending_bounded(
            &self.pending_rows,
            input,
            self.staging_bytes(),
        ) {
            Ok(staged) => staged,
            Err(focal_core::StagingError::Capacity) => return Err(LedgerError::Capacity),
            Err(focal_core::StagingError::Domain(outcome)) => {
                return Ok(ManagedSubmission::Domain(outcome));
            }
        };
        {
            let _audit = self
                .budget
                .reserve(BudgetKind::Pending, lane, self.staging_bytes())?;
            self.core.audit_managed_pending_stage(
                &self.pending_rows,
                &staged,
                self.staging_bytes(),
            )?;
        }
        let mut data = MANAGED_DOMAIN_MAGIC.to_vec();
        data.extend(staged.prepared().encode_v1()?);
        let digest = ContentHash(*blake3::hash(&data).as_bytes());
        self.ensure_delta_slots()?;
        let candidate = self.reserve_managed_domain(staged, lane, scratch)?;
        let charge = self.managed_charge(
            size_of::<PendingManaged>()
                .checked_add(data.len())
                .ok_or(LedgerError::Capacity)?,
        )?;
        self.consensus.propose_in(data, lane)?;
        self.pending_managed = Some(Box::new(PendingManaged {
            entry_hash: digest,
            candidate: ManagedCandidate::Domain(candidate),
            _charge: charge,
        }));
        Ok(ManagedSubmission::Pending(input.key))
    }
    fn reserve_managed_domain(
        &mut self,
        staged: focal_core::StagedManagedMutation,
        lane: BudgetLane,
        stage_charge: Allocation,
    ) -> Result<ManagedDomainCandidate, LedgerError> {
        let before = self.pending_rows.view(&self.core)?;
        let core_bytes = reference_charge(&staged.view_after(&self.core, &self.pending_rows)?)?;
        let core_growth = self
            .budget
            .reserve(
                BudgetKind::Payload,
                lane,
                core_bytes.saturating_sub(reference_charge(&before)?),
            )?
            .commit();
        let graph = self
            .graph
            .prepare_patch(before, staged.patch(), None, lane)?;
        let result = staged.result();
        let receipt = ManagedReceipt {
            key: result.key,
            sequence: result.sequence,
            raft_index: 0,
            intent_hash: result.command_hash,
            outcome: ManagedReceiptOutcome::Domain(result.outcome.clone()),
        };
        let result_charge = self.managed_charge(
            reference_charge(result)?
                .checked_add(reference_charge(&receipt)?)
                .ok_or(LedgerError::Capacity)?,
        )?;
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(result.deltas.len())
            .map_err(|_| LedgerError::Capacity)?;
        for delta in &result.deltas {
            let charge = self
                .budget
                .reserve(
                    BudgetKind::Payload,
                    lane,
                    reference_charge(delta)?
                        .checked_add(size_of::<RetainedDelta>())
                        .ok_or(LedgerError::Capacity)?,
                )?
                .commit();
            retained.push(RetainedDelta {
                bytes: postcard::experimental::serialized_size(&focal_model::durable_v1::Ref(
                    delta,
                ))?,
                delta: delta.clone(),
                _charge: charge,
            });
        }
        let floor = self.retention_forecast_from(&retained, result.sequence, std::iter::empty())?;
        let registry = self
            .request_streams
            .prepare_receipt(receipt, lane, &self.budget)?;
        Ok(ManagedDomainCandidate {
            staged,
            registry,
            graph,
            retained,
            floor,
            core_bytes,
            core_growth,
            lane,
            result_charge,
            _stage_charge: stage_charge,
        })
    }
    fn apply_managed_entry(
        &mut self,
        data: &[u8],
        index: u64,
        events: &mut SessionEvents,
    ) -> Result<bool, LedgerError> {
        if !data.starts_with(MANAGED_DOMAIN_MAGIC)
            && !data.starts_with(MANAGED_CURSOR_MAGIC)
            && !data.starts_with(REQUEST_STREAM_MAGIC)
        {
            return Ok(false);
        }
        if self.activation.is_native() && data.starts_with(MANAGED_DOMAIN_MAGIC) {
            // A legacy domain mutation after activation cannot have been admitted.
            return Err(LedgerError::Corrupt);
        }
        if !self.consensus.decoder_floor_ready(managed_format_hash()) {
            return Err(LedgerError::Corrupt);
        }
        let hash = ContentHash(*blake3::hash(data).as_bytes());
        let pending = match self.pending_managed.take() {
            Some(pending) if pending.entry_hash == hash => *pending,
            _ => {
                self.clear_pending();
                self.pending_cursor = None;
                self.pending_maintenance = None;
                let charge = self.managed_charge(
                    data.len()
                        .checked_mul(64)
                        .and_then(|n| n.checked_add(4096))
                        .ok_or(LedgerError::Capacity)?,
                )?;
                let candidate = if let Some(encoded) = data.strip_prefix(REQUEST_STREAM_MAGIC) {
                    let (envelope, rest): (RequestStreamEnvelope, _) =
                        durable_session_v1::take(encoded)?;
                    if !rest.is_empty()
                        || envelope.schema != 1
                        || envelope.domain_sequence != self.sequence()
                    {
                        return Err(LedgerError::Corrupt);
                    }
                    ManagedCandidate::Control(self.request_streams.prepare_control(
                        &envelope.input,
                        self.sequence(),
                        &self.budget,
                    )?)
                } else if let Some(encoded) = data.strip_prefix(MANAGED_DOMAIN_MAGIC) {
                    if self.placement_state.paused() {
                        return Err(LedgerError::Corrupt);
                    }
                    let prepared = focal_core::PreparedManagedMutation::decode_v1(encoded)?;
                    if self
                        .request_streams
                        .exact(
                            &prepared.input.key,
                            prepared.command_hash,
                            ManagedRequestFamily::Domain,
                        )?
                        .is_some()
                    {
                        return Err(LedgerError::Corrupt);
                    }
                    self.ensure_delta_slots()?;
                    let scratch = self.managed_charge(self.staging_bytes())?;
                    let staged = self
                        .core
                        .replay_managed_bounded(&prepared, self.staging_bytes())?;
                    ManagedCandidate::Domain(self.reserve_managed_domain(
                        staged,
                        BudgetLane::Completion,
                        scratch,
                    )?)
                } else {
                    let (envelope, rest): (ManagedCursorEnvelope, _) = durable_session_v1::take(
                        data.strip_prefix(MANAGED_CURSOR_MAGIC)
                            .ok_or(LedgerError::Corrupt)?,
                    )?;
                    if !rest.is_empty() {
                        return Err(LedgerError::Corrupt);
                    }
                    ManagedCandidate::Cursor(self.build_managed_cursor(&envelope, true)?)
                };
                PendingManaged {
                    entry_hash: hash,
                    candidate,
                    _charge: charge,
                }
            }
        };
        match pending.candidate {
            ManagedCandidate::Control(mut candidate) => {
                candidate.set_index(index)?;
                let source = candidate.control_receipt().ok_or(LedgerError::Corrupt)?;
                let charge = self.managed_charge(reference_charge(source)?)?;
                let receipt = source.clone();
                self.request_streams.publish(candidate)?;
                events.request_stream_committed.push(receipt);
                events._charges.push(charge);
            }
            ManagedCandidate::Cursor(mut candidate) => {
                candidate
                    .registry
                    .set_cursor_sequence(self.stream_published());
                candidate.registry.set_index(index)?;
                let receipt = candidate
                    .registry
                    .receipt(&candidate.key)
                    .ok_or(LedgerError::Corrupt)?
                    .clone();
                self.request_streams
                    .validate_publication(&candidate.registry)?;
                self.cursors.publish(candidate.prepared)?;
                self.cursor_meta = candidate.metadata;
                self.cursor_charge = candidate.metadata_charge;
                let floor = match receipt.outcome {
                    ManagedReceiptOutcome::Cursor { floor, .. } => floor,
                    _ => return Err(LedgerError::Corrupt),
                };
                self.request_streams.publish(candidate.registry)?;
                self.retire_deltas(floor)?;
                events.managed_committed.push(ManagedCommitted {
                    receipt,
                    deltas: Vec::new(),
                    effects: Vec::new(),
                });
                events._charges.push(candidate.result_charge);
            }
            ManagedCandidate::Domain(mut candidate) => {
                if self.placement_state.paused() {
                    return Err(LedgerError::Corrupt);
                }
                self.core.validate_managed(&candidate.staged)?;
                self.graph
                    .validate_publication(std::iter::once(&candidate.graph))?;
                let floor = candidate.floor.max(self.delta_floor);
                let mut bytes = 0usize;
                let mut items = 0usize;
                for delta in self
                    .deltas
                    .iter()
                    .chain(&candidate.retained)
                    .filter(|d| d.delta.id.sequence > floor)
                {
                    bytes = bytes
                        .checked_add(delta.bytes)
                        .ok_or(LedgerError::Capacity)?;
                    items = items.checked_add(1).ok_or(LedgerError::Capacity)?;
                }
                if bytes > self.limits.delta_bytes
                    || items > self.limits.delta_items
                    || items > self.deltas.capacity()
                {
                    return Err(LedgerError::Capacity);
                }
                match candidate.lane {
                    BudgetLane::Ordinary => self.core_charge.absorb(&mut candidate.core_growth)?,
                    BudgetLane::Completion => self
                        .core_completion_charge
                        .absorb(&mut candidate.core_growth)?,
                };
                candidate.registry.set_index(index)?;
                let receipt = candidate
                    .registry
                    .receipt(&candidate.staged.result().key)
                    .ok_or(LedgerError::Corrupt)?
                    .clone();
                self.request_streams
                    .validate_publication(&candidate.registry)?;
                let result = self.core.publish_managed(candidate.staged)?;
                self.graph.publish(candidate.graph)?;
                self.request_streams.publish(candidate.registry)?;
                self.retire_deltas(floor)?;
                for delta in candidate.retained {
                    if delta.delta.id.sequence > floor {
                        self.deltas.push_back(delta);
                    }
                }
                self.delta_bytes = bytes;
                self.shrink_core_charge(candidate.core_bytes)?;
                events.managed_committed.push(ManagedCommitted {
                    receipt,
                    deltas: result.deltas,
                    effects: result.effects,
                });
                events._charges.push(candidate.result_charge);
            }
        }
        Ok(true)
    }
}
impl Session {
    pub fn propose_managed_cursor(
        &mut self,
        input: &ManagedCursorInput,
        trusted_control: bool,
    ) -> Result<ManagedSubmission, LedgerError> {
        self.check()?;
        self.managed_input_size(input)?;
        if let Some(receipt) =
            self.managed_receipt(&input.key, input.intent_hash, ManagedRequestFamily::Cursor)?
        {
            return Ok(ManagedSubmission::Committed(Box::new(receipt.clone())));
        }
        if let Some(pending) = self.pending_managed.as_deref()
            && pending.key() == Some(input.key)
        {
            return if pending.intent() == Some(input.intent_hash)
                && pending.family() == Some(ManagedRequestFamily::Cursor)
            {
                Ok(ManagedSubmission::Pending(input.key))
            } else {
                Err(ManagedError::Conflict.into())
            };
        }
        self.managed_admission()?;
        self.managed_input_size(input)?;
        let charge = self.managed_charge(
            reference_charge(input)?
                .checked_mul(3)
                .and_then(|n| n.checked_add(4096))
                .ok_or(LedgerError::Capacity)?,
        )?;
        let envelope = ManagedCursorEnvelope {
            schema: 1,
            domain_sequence: self.sequence(),
            replay_floor: self.stream_bounds().floor,
            trusted_control,
            input: input.clone(),
        };
        let data = durable_session_v1::encode(MANAGED_CURSOR_MAGIC, &envelope, usize::MAX)?;
        let hash = ContentHash(*blake3::hash(&data).as_bytes());
        let candidate = self.build_managed_cursor(&envelope, false)?;
        self.consensus.propose_in(data, BudgetLane::Completion)?;
        self.pending_managed = Some(Box::new(PendingManaged {
            entry_hash: hash,
            candidate: ManagedCandidate::Cursor(candidate),
            _charge: charge,
        }));
        Ok(ManagedSubmission::Pending(input.key))
    }
    fn build_managed_cursor(
        &mut self,
        envelope: &ManagedCursorEnvelope,
        committed: bool,
    ) -> Result<ManagedCursorCandidate, LedgerError> {
        let input = &envelope.input;
        if envelope.schema != 1
            || envelope.domain_sequence != self.sequence()
            || envelope.replay_floor > self.sequence()
            || envelope.replay_floor < self.cursors.checkpoint().floor
        {
            return Err(LedgerError::Corrupt);
        }
        if self
            .request_streams
            .exact(&input.key, input.intent_hash, ManagedRequestFamily::Cursor)?
            .is_some()
        {
            return Err(ManagedError::Conflict.into());
        }
        self.managed_input_size(input)?;
        let operation = &input.command.operation;
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
                if owner != input.key.stream.principal {
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
                self.validate_replay_position(token.position)?
            }
            _ => {}
        }
        // Owner metadata is shared with legacy consumers, but managed outcomes
        // never enter its legacy RequestKey receipt map.
        let meta_bytes = reference_charge(&self.cursor_meta)?
            .checked_add(reference_charge(input)?)
            .and_then(|n| n.checked_mul(3))
            .ok_or(LedgerError::Capacity)?;
        let _scratch = self.managed_charge(meta_bytes)?;
        let published = self.stream_published();
        let prepared = if committed {
            self.cursors
                .prepare_committed(&input.command, published)?
        } else {
            self.cursors.prepare(&input.command, published)?
        };
        let record = consumer
            .and_then(|id| prepared.checkpoint().consumers.get(&id))
            .map(crate::reconciliation::copy_record)
            .transpose()?;
        let receipt = ManagedReceipt {
            key: input.key,
            sequence: self.sequence(),
            raft_index: 0,
            intent_hash: input.intent_hash,
            outcome: ManagedReceiptOutcome::Cursor {
                revision: prepared.checkpoint().revision,
                floor: envelope.replay_floor.max(prepared.checkpoint().floor),
                record,
            },
        };
        let mut metadata = CursorMetadata {
            receipts: self.cursor_meta.receipts.clone(),
            owners: self.cursor_meta.owners.clone(),
        };
        if let Some(consumer) = consumer {
            metadata
                .owners
                .entry(consumer)
                .or_insert(input.key.stream.principal);
        }
        let metadata_charge = self
            .budget
            .reserve(
                BudgetKind::ReadPins,
                BudgetLane::Completion,
                reference_charge(&metadata)?,
            )?
            .commit();
        let result_charge = self.managed_charge(reference_charge(&receipt)?)?;
        let registry =
            self.request_streams
                .prepare_receipt(receipt, BudgetLane::Completion, &self.budget)?;
        Ok(ManagedCursorCandidate {
            key: input.key,
            prepared,
            metadata,
            metadata_charge,
            registry,
            result_charge,
        })
    }
}

impl Session {
    // This is a semantic encoded-byte limit, including historical cursor replay,
    // not a current in-memory allocation charge. Keep its V1 representation.
    fn managed_input_size<T: focal_model::durable_v1::V1>(
        &self,
        value: &T,
    ) -> Result<usize, LedgerError> {
        self.managed_input_size_view(&focal_model::durable_v1::Ref(value))
    }
    fn managed_input_size_view<T: Serialize + ?Sized>(
        &self,
        value: &T,
    ) -> Result<usize, LedgerError> {
        struct BoundedSize {
            count: usize,
            limit: usize,
        }
        impl postcard::ser_flavors::Flavor for BoundedSize {
            type Output = usize;
            fn try_push(&mut self, _: u8) -> Result<(), postcard::Error> {
                self.count = self
                    .count
                    .checked_add(1)
                    .filter(|n| *n <= self.limit)
                    .ok_or(postcard::Error::SerializeBufferFull)?;
                Ok(())
            }
            fn try_extend(&mut self, bytes: &[u8]) -> Result<(), postcard::Error> {
                self.count = self
                    .count
                    .checked_add(bytes.len())
                    .filter(|n| *n <= self.limit)
                    .ok_or(postcard::Error::SerializeBufferFull)?;
                Ok(())
            }
            fn finalize(self) -> Result<usize, postcard::Error> {
                Ok(self.count)
            }
        }
        postcard::serialize_with_flavor(
            value,
            BoundedSize {
                count: 0,
                limit: self.limits.core.max_command_bytes,
            },
        )
        .map_err(|_| LedgerError::Capacity)
    }
}
