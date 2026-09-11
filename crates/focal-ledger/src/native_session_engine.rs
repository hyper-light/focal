//! The native domain engine: pending candidate chain, active owner or passive
//! Core, native prefix and producer-range mapping, genesis and readiness state.
//! It never owns the consensus replica or the content writer; every operation
//! that touches Raft receives the replica explicitly, so the standalone native
//! session and the unified Session run the same code.
use super::*;
use focal_model::ContentDomainId;

const DISK_SAMPLE_INTERVAL: u32 = 32;

pub(super) struct Pending {
    pub(super) candidate: NativeCandidate,
    pub(super) outcome: NativeOutcome,
    pub(super) hash: Option<ContentHash>,
    pub(super) submitted: bool,
    pub(super) term: u64,
}
#[allow(clippy::large_enum_variant)] // One instance per session; variants move only at authority transitions.
pub(super) enum Domain {
    Passive(Core<NativeState>),
    Active(Box<NativeOwner>, Allocation),
}

pub(crate) struct NativeEngine<S: NativeSchemaVerifier> {
    pub(super) delivery: Option<super::apply::Delivery>,
    pub(super) pending: VecDeque<Pending>,
    pub(super) domain: Option<Domain>,
    pub(super) reader: ContentReader,
    /// Where checkpoint seeds are read from on install (25 §5); the writer
    /// belongs to whoever owns the session's physical resources.
    pub(super) seeds: SeedReader,
    /// The seeded checkpoint this replica cannot install until the chunks
    /// it names are local; the host pulls them and polls again.
    pub(super) pending_seed: Option<PendingSeed>,
    /// The objects a retained delivery could not read locally (24 §20).
    pub(super) pending_custody: Option<PendingCustody>,
    pub(super) schemas: S,
    pub(super) budget: MemoryBudget,
    pub(super) ledger: LedgerId,
    pub(super) profile: NativeContentProfile,
    pub(super) range: RangeId,
    pub(super) recording_range: Option<RangeId>,
    pub(super) recording_term: u64,
    /// The prefix that holds no native record yet: zero at genesis, one after an
    /// import, a checkpoint's prefix when it was installed without records.
    pub(super) records_floor: SessionSeq,
    /// Raft index of the committed activation record; zero until it applies.
    pub(super) activation_index: u64,
    pub(super) limits: NativeSessionLimits,
    pub(super) applied_raft: u64,
    pub(super) configuration_index: u64,
    pub(super) genesis: Option<ContentHash>,
    pub(super) genesis_proposed: bool,
    /// The layout record this authority proposed and has not seen applied;
    /// native admission waits for it, and a term change lets it go.
    pub(super) layout_change: Option<super::range::LayoutRecord>,
    /// The prefix the archive reports holding every proof through (26 §3).
    pub(super) archived_through: SessionSeq,
    /// The retirement record this authority proposed and has not seen
    /// applied (26 §4); native admission waits for it, and a term change
    /// lets it go.
    pub(super) retirement: Option<super::retirement::RetirementRecord>,
    /// Families retired through the applied prefix, counted from genesis or
    /// the checkpoint that seeded this replica.
    pub(super) retired_families: u64,
    /// The chunks of this replica's latest checkpoint seed (25 §5): what
    /// its seed store must keep for peers that seed from it (26 §5).
    pub(super) seed_chunks: Vec<ContentHash>,
    /// The movement coordinator (25 §6), built once the genesis names the
    /// origin member and restored from a checkpoint's movement section.
    pub(super) movement: Option<super::movement::Movement>,
    pub(super) disk_sample: Option<u64>,
    pub(super) admissions_since_sample: u32,
    pub(super) ready_term: Option<u64>,
    pub(super) readiness_requested: Option<u64>,
    pub(super) observed_term: u64,
    pub(super) observed_leader: bool,
    pub(super) reconstruction_needed: bool,
    pub(super) failed: bool,
    pub(super) materializer: super::MaterializerStats,
    pub(super) _pending_allocation: Allocation,
}

/// What the engine reads from this node's disk beside the log: the content
/// tree custody verifies against and the seed store a seeded checkpoint is
/// assembled from (25 §5).
pub(crate) struct NativeSources {
    pub(crate) reader: ContentReader,
    pub(crate) seeds: SeedReader,
}

impl<S: NativeSchemaVerifier> NativeEngine<S> {
    /// An empty native domain at the group's origin. Startup delivery restores
    /// the committed prefix from consensus; nothing here reads the log.
    pub(crate) fn new(
        ledger: LedgerId,
        range: RangeId,
        profile: NativeContentProfile,
        limits: NativeSessionLimits,
        parent: &MemoryBudget,
        sources: NativeSources,
        schemas: S,
    ) -> Result<Self, NativeSessionError> {
        let NativeSources { reader, seeds } = sources;
        if range.0 == 0
            || limits.content_domain.is_zero()
            || limits.recovery.native.pending == 0
            || limits.frame_bytes == 0
        {
            return Err(NativeSessionError::Capacity);
        }
        // Validate the derived decode limits once; every frame reuses them.
        input_codec::NativeDecodeLimits::for_native(
            limits.recovery.native,
            limits.frame_bytes,
            limits.decode_work,
        )
        .map_err(NativeOwnerError::from)?;
        let budget = parent.child(limits.memory_bytes, limits.completion_reserve_bytes)?;
        let count = limits.recovery.native.pending;
        let permit = budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            array::<Pending>(count)?,
        )?;
        let mut pending = VecDeque::new();
        pending
            .try_reserve_exact(count)
            .map_err(|_| NativeSessionError::Capacity)?;
        if pending.capacity() > count {
            return Err(NativeSessionError::Capacity);
        }
        let core = match profile {
            NativeContentProfile::ProjectionOnly => {
                Core::new_native(ledger, range, limits.recovery.native, budget.clone())?
            }
            NativeContentProfile::AuthoredV1 => {
                Core::new_native_authored(ledger, range, limits.recovery.native, budget.clone())?
            }
        };
        Ok(Self {
            delivery: None,
            pending,
            domain: Some(Domain::Passive(core)),
            reader,
            seeds,
            pending_seed: None,
            pending_custody: None,
            schemas,
            budget,
            ledger,
            profile,
            range,
            recording_range: None,
            recording_term: 0,
            records_floor: SessionSeq(0),
            activation_index: 0,
            limits,
            applied_raft: 0,
            configuration_index: 0,
            genesis: None,
            genesis_proposed: false,
            layout_change: None,
            archived_through: SessionSeq(0),
            retirement: None,
            retired_families: 0,
            seed_chunks: Vec::new(),
            movement: None,
            disk_sample: None,
            admissions_since_sample: 0,
            ready_term: None,
            readiness_requested: None,
            observed_term: 0,
            observed_leader: false,
            reconstruction_needed: true,
            failed: false,
            materializer: super::MaterializerStats::default(),
            _pending_allocation: permit.commit(),
        })
    }

    pub(super) fn check(&self) -> Result<(), NativeSessionError> {
        if self.failed || self.domain.is_none() {
            Err(NativeSessionError::Failed)
        } else {
            Ok(())
        }
    }
    pub(crate) fn activation_index(&self) -> u64 {
        self.activation_index
    }
    pub(crate) fn set_activation_index(&mut self, index: u64) {
        self.activation_index = index;
    }
    pub(crate) fn failed(&self) -> bool {
        self.failed
    }
    #[cfg(test)]
    pub(crate) fn budget(&self) -> &MemoryBudget {
        &self.budget
    }
    pub(crate) fn decode_limits(
        &self,
    ) -> Result<input_codec::NativeDecodeLimits, NativeSessionError> {
        Ok(input_codec::NativeDecodeLimits::for_native(
            self.limits.recovery.native,
            self.limits.frame_bytes,
            self.limits.decode_work,
        )
        .map_err(NativeOwnerError::from)?)
    }
    /// Current-term authority past its committed readiness barrier, with the
    /// genesis applied, the owner reconstructed and no delivery in flight.
    pub(crate) fn is_authoritative(&self, status: &NodeStatus) -> bool {
        !self.failed
            && self.delivery.is_none()
            && status.role == StateRole::Leader
            && self.ready_term == Some(status.term)
            && self.genesis.is_some()
            && matches!(self.domain, Some(Domain::Active(..)))
    }
    pub(crate) fn genesis(&self) -> Option<ContentHash> {
        self.genesis
    }
    pub(crate) fn committed_core(&self) -> Result<&Core<NativeState>, NativeSessionError> {
        self.check()?;
        match self.domain.as_ref() {
            Some(Domain::Passive(core)) => Ok(core),
            Some(Domain::Active(owner, _)) => Ok(owner.committed_core()),
            None => Err(NativeSessionError::Failed),
        }
    }
    pub(crate) fn sequence(&self) -> Result<SessionSeq, NativeSessionError> {
        Ok(self.committed_core()?.native_sequence())
    }
    /// Propose one layout change as a session decision (25 §4). Only an
    /// authority with no pending candidate and no change in flight may; the
    /// change is checked against the committed layout before the record is
    /// proposed, so an applicable record is what the log carries.
    pub(crate) fn propose_layout(
        &mut self,
        consensus: &mut DurableNode,
        operation: super::range::LayoutOperation,
    ) -> Result<(), NativeSessionError> {
        let status = consensus.status();
        self.require_authority(&status)?;
        if self.layout_change.is_some() {
            return Err(NativeSessionError::LayoutChanging);
        }
        if self.retirement.is_some() {
            return Err(NativeSessionError::Retiring);
        }
        if self
            .movement
            .as_ref()
            .is_some_and(|movement| movement.pending().is_some() || movement.in_flight.is_some())
        {
            return Err(NativeSessionError::RangeMoving);
        }
        if !self.pending.is_empty() || self.delivery.is_some() {
            return Err(NativeSessionError::Capacity);
        }
        let core = self.committed_core()?;
        let layout = core.native_layout();
        match operation {
            super::range::LayoutOperation::Split { at, id } => {
                layout.check_split(at, id, self.limits.recovery.native.max_ranges)?;
            }
            super::range::LayoutOperation::Merge { left } => {
                layout.check_merge(left)?;
            }
        }
        let record = super::range::LayoutRecord {
            ledger: self.ledger,
            expected_epoch: layout.epoch(),
            operation,
        };
        let mut bytes = [0u8; super::range::BYTES];
        record.write_into(&mut bytes);
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(super::range::BYTES)?,
        )?;
        consensus.propose_borrowed_in(&bytes, BudgetLane::Completion)?;
        self.layout_change = Some(record);
        Ok(())
    }
    pub(crate) fn archived_through(&self) -> SessionSeq {
        self.archived_through
    }
    /// The archive's report is monotone: it never takes back what it holds.
    pub(crate) fn note_archived(&mut self, through: SessionSeq) {
        self.archived_through = self.archived_through.max(through);
    }
    pub(crate) fn retirement_in_flight(&self) -> Option<super::retirement::RetirementRecord> {
        self.retirement
    }
    /// The owner must be reconstructed before this engine is authoritative
    /// again: a record it did not author through its owner applied through
    /// its committed core (a retirement, 26 §4).
    pub(crate) fn reconstruction_needed(&self) -> bool {
        self.reconstruction_needed
    }
    pub(crate) fn retired_families(&self) -> u64 {
        self.retired_families
    }
    /// The chunks of the latest checkpoint seed, sorted.
    pub(crate) fn seed_chunks(&self) -> &[ContentHash] {
        &self.seed_chunks
    }
    /// Record the seed chunks a freshly encoded or installed checkpoint
    /// names; an inline checkpoint names none.
    pub(super) fn note_seeds(&mut self, bytes: &[u8]) -> Result<(), NativeSessionError> {
        let mut chunks = Vec::new();
        if let Some(manifest) =
            crate::native_checkpoint::Checkpoint::describe(bytes, self.limits.checkpoint)?
        {
            for chunk in manifest.chunks() {
                let chunk = chunk?;
                chunks
                    .try_reserve_exact(1)
                    .map_err(|_| NativeSessionError::Capacity)?;
                chunks.push(chunk.hash);
            }
        }
        chunks.sort();
        chunks.dedup();
        self.seed_chunks = chunks;
        Ok(())
    }
    /// Propose one family's retirement as a session decision (26 §4). Only
    /// an authority with no pending candidate and nothing else in flight
    /// may; the family is derived from the committed state and the bundle's
    /// claim is checked against it before the record is proposed, so an
    /// applicable record is what the log carries.
    pub(crate) fn propose_retirement(
        &mut self,
        consensus: &mut DurableNode,
        root: focal_model::ClaimId,
        bundle: ContentHash,
        bytes: u64,
        through: SessionSeq,
    ) -> Result<(), NativeSessionError> {
        let status = consensus.status();
        self.require_authority(&status)?;
        if self.retirement.is_some() {
            return Err(NativeSessionError::Retiring);
        }
        if self.layout_change.is_some() {
            return Err(NativeSessionError::LayoutChanging);
        }
        if self
            .movement
            .as_ref()
            .is_some_and(|movement| movement.pending().is_some() || movement.in_flight.is_some())
        {
            return Err(NativeSessionError::RangeMoving);
        }
        if !self.pending.is_empty() || self.delivery.is_some() {
            return Err(NativeSessionError::Capacity);
        }
        let core = self.committed_core()?;
        let family = core
            .retirement_family(root)
            .map_err(NativeSessionError::Retirement)?;
        let prefix = core.native_sequence();
        if bundle.0 == [0; 32]
            || bytes == 0
            || through.0 == 0
            || through < family.through
            || through > prefix
        {
            return Err(NativeError::Contract(
                focal_model::lifecycle::ContractError::InvalidManifest,
            )
            .into());
        }
        let record = super::retirement::RetirementRecord {
            ledger: self.ledger,
            expected_prefix: prefix,
            root,
            bundle,
            bytes,
            through,
        };
        let mut encoded = [0u8; super::retirement::BYTES];
        record.write_into(&mut encoded);
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(super::retirement::BYTES)?,
        )?;
        consensus.propose_borrowed_in(&encoded, BudgetLane::Completion)?;
        self.retirement = Some(record);
        Ok(())
    }
    /// Propose one movement step (25 §6). The step is prepared against the
    /// committed coordinator state under the session verifier before the
    /// record is proposed, so an applicable record is what the log carries;
    /// `Cleanup` additionally waits for every read lease on the group.
    pub(crate) fn propose_range(
        &mut self,
        consensus: &mut DurableNode,
        operation: focal_ranges::RangeOperation,
    ) -> Result<(), NativeSessionError> {
        let status = consensus.status();
        self.require_authority(&status)?;
        if self.layout_change.is_some() {
            return Err(NativeSessionError::LayoutChanging);
        }
        if self.retirement.is_some() {
            return Err(NativeSessionError::Retiring);
        }
        if !self.pending.is_empty() || self.delivery.is_some() {
            return Err(NativeSessionError::Capacity);
        }
        let sequence = self.sequence()?;
        let pinned = self.committed_core()?.native_stats().pinned_snapshots;
        let ledger = self.ledger;
        let movement = self.movement.as_mut().ok_or(NativeSessionError::Corrupt)?;
        if movement.in_flight.is_some() {
            return Err(NativeSessionError::RangeMoving);
        }
        if matches!(operation, focal_ranges::RangeOperation::Cleanup { .. }) && pinned > 0 {
            return Err(NativeSessionError::Range(focal_ranges::RangeError::Pinned));
        }
        let ordinal = movement
            .coordinator
            .checkpoint()
            .control_ordinal
            .checked_add(1)
            .ok_or(NativeSessionError::Capacity)?;
        let prepared = movement.coordinator.prepare(
            ordinal,
            sequence,
            operation.clone(),
            &movement.verifier,
        )?;
        let record = super::movement::MovementRecord {
            ledger,
            ordinal,
            operation,
        };
        let _permit = self.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            array::<u8>(super::movement::MAX_RECORD_BYTES)?,
        )?;
        let bytes = record.encode()?;
        consensus.propose_borrowed_in(&bytes, BudgetLane::Completion)?;
        movement.in_flight = Some((ordinal, prepared.hash()));
        Ok(())
    }
    pub(crate) fn range_map(&self) -> Result<&focal_ranges::RangeMap, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .coordinator
            .map())
    }
    pub(crate) fn movement_pending(
        &self,
    ) -> Result<Option<&focal_ranges::TransferState>, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .pending())
    }
    pub(crate) fn movement_checkpoint(
        &self,
    ) -> Result<&focal_ranges::RangeCheckpoint, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .coordinator
            .checkpoint())
    }
    pub(crate) fn movement_refusals(&self) -> u64 {
        self.movement
            .as_ref()
            .map_or(0, |movement| movement.refusals)
    }
    pub(crate) fn movement_in_flight(&self) -> bool {
        self.movement
            .as_ref()
            .is_some_and(|movement| movement.in_flight.is_some())
    }
    pub(crate) fn range_verifier(
        &self,
    ) -> Result<super::movement::LedgerRangeVerifier, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .verifier)
    }
    /// The `Activate` step of the pending transfer over the proofs the
    /// state holds and the progress of the replica-held members that stay.
    pub(crate) fn range_activation_operation(
        &self,
        unchanged: Vec<focal_ranges::RangeProgress>,
    ) -> Result<focal_ranges::RangeOperation, NativeSessionError> {
        self.check()?;
        Ok(self
            .movement
            .as_ref()
            .ok_or(NativeSessionError::Corrupt)?
            .coordinator
            .activation_operation(unchanged)?)
    }
    pub(crate) fn range_activation(
        &self,
        operation: focal_ranges::TransferId,
    ) -> Option<&focal_ranges::ActivationCertificate> {
        self.movement
            .as_ref()
            .and_then(|movement| movement.coordinator.activation(operation))
    }
    pub(crate) fn applied_raft(&self) -> u64 {
        self.applied_raft
    }
    pub(crate) fn configuration_index(&self) -> u64 {
        self.configuration_index
    }
    /// The seeded checkpoint waiting for its chunks, if any.
    pub(crate) fn pending_seed(&self) -> Option<&PendingSeed> {
        self.pending_seed.as_ref()
    }
    pub(crate) fn pending_custody(&self) -> Option<&PendingCustody> {
        self.pending_custody.as_ref()
    }
    pub(crate) fn take_pending_custody(&mut self) -> Option<PendingCustody> {
        self.pending_custody.take()
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.pending.len()
    }
    /// This replica's producer incarnation: the configured range until an
    /// authoritative snapshot is installed, then a derived incarnation.
    pub(crate) fn range(&self) -> RangeId {
        self.range
    }
    pub(crate) fn recording_range(&self) -> Option<RangeId> {
        self.recording_range
    }
    pub(crate) fn outcome(
        &self,
        request: impl Into<NativeInvocation>,
    ) -> Result<Option<NativeOutcome>, NativeSessionError> {
        Ok(self.committed_core()?.native_outcome(request))
    }
    pub(crate) fn read_at_least(
        &self,
        boundary: NativeReadBoundary,
        status: &NodeStatus,
    ) -> Result<&Core<NativeState>, NativeSessionError> {
        let core = self.committed_core()?;
        if self.applied_raft < boundary.raft_index
            || core.native_sequence() < boundary.native_sequence
        {
            return Err(NativeSessionError::NotReady {
                leader: status.leader_id,
            });
        }
        Ok(core)
    }
    pub(super) fn require_authority(&self, status: &NodeStatus) -> Result<(), NativeSessionError> {
        self.check()?;
        if !self.is_authoritative(status) {
            return Err(NativeSessionError::NotReady {
                leader: status.leader_id,
            });
        }
        Ok(())
    }

    /// Stage one input against the owner and submit the resulting candidate.
    /// An exact retry of committed work is answered from the root before any
    /// queue, disk or memory admission applies to fresh work.
    pub(crate) fn admit(
        &mut self,
        consensus: &mut DurableNode,
        prepare: impl FnOnce(
            &mut NativeOwner,
            &S,
            ContentDomainId,
        ) -> Result<NativeStaging, NativeOwnerError>,
    ) -> Result<NativeSubmission, NativeSessionError> {
        let status = consensus.status();
        self.require_authority(&status)?;
        if self.layout_change.is_some() {
            return Err(NativeSessionError::LayoutChanging);
        }
        if self.retirement.is_some() {
            return Err(NativeSessionError::Retiring);
        }
        let limit = self.limits.recovery.native.pending;
        let full = self.pending.len() >= limit || self.pending.len() == self.pending.capacity();
        let headroom = self.disk_headroom_ok(consensus)?;
        let domain = self.limits.content_domain;
        let Some(Domain::Active(owner, _)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        let owner: &mut NativeOwner = owner;
        // An exact retry never needs a fresh slot, so the owner looks it up first
        // and refuses fresh work itself when its own queue is full.
        let staged = prepare(owner, &self.schemas, domain)?;
        // Between a transfer's barrier and its activation nothing lands on a
        // moving member (25 §6): the fresh candidate is discarded and the
        // caller retries after activation.
        if let NativeStaging::Prepared { candidate, .. } = staged
            && let Some(movement) = self.movement.as_ref()
        {
            let fenced = movement.fenced_members();
            if !fenced.is_empty() {
                let layout: Vec<RangeId> = owner.native_layout().ids().collect();
                let touches = owner
                    .prepared_candidate(candidate)?
                    .touched_members()
                    .filter_map(|position| layout.get(position).copied())
                    .any(|member| fenced.contains(&member));
                if touches {
                    owner.discard_from(candidate)?;
                    return Err(NativeSessionError::RangeMoving);
                }
            }
        }
        if (full || !headroom) && matches!(staged, NativeStaging::Prepared { .. }) {
            // A full queue here is an accounting inconsistency (the owner's queue
            // is bounded by the same limit); missing disk headroom is ordinary
            // pressure. Both refuse only the fresh candidate; an exact retry of
            // committed work was already answered by the owner above.
            if let NativeStaging::Prepared { candidate, .. } = staged {
                owner.discard_from(candidate)?;
            }
            return Err(NativeSessionError::Capacity);
        }
        self.submit_staged(consensus, staged, status.term)
    }
    /// Sample the WAL filesystem's free space when due and decide whether a
    /// fresh candidate may be admitted. Far above the watermark one sample
    /// covers a bounded run of admissions; near it every admission samples. A
    /// failed sample refuses fresh work rather than promising unknown space.
    fn disk_headroom_ok(&mut self, consensus: &DurableNode) -> Result<bool, NativeSessionError> {
        let headroom = self.limits.disk_headroom_bytes;
        if headroom == 0 {
            return Ok(true);
        }
        let due = match self.disk_sample {
            None => true,
            Some(available) => {
                self.admissions_since_sample >= DISK_SAMPLE_INTERVAL
                    || available < headroom.saturating_mul(2)
            }
        };
        if due {
            self.disk_sample = Some(
                consensus
                    .disk_available_bytes()
                    .map_err(|_| NativeSessionError::Capacity)?,
            );
            self.admissions_since_sample = 0;
        }
        self.admissions_since_sample = self.admissions_since_sample.saturating_add(1);
        Ok(self
            .disk_sample
            .is_some_and(|available| available >= headroom))
    }
    fn submit_staged(
        &mut self,
        consensus: &mut DurableNode,
        staged: NativeStaging,
        term: u64,
    ) -> Result<NativeSubmission, NativeSessionError> {
        let (candidate, outcome) = match staged {
            NativeStaging::Existing {
                outcome,
                candidate: None,
            } => return Ok(NativeSubmission::Committed(outcome)),
            NativeStaging::Existing {
                outcome,
                candidate: Some(candidate),
            } => (candidate, outcome),
            NativeStaging::Prepared { candidate, outcome } => {
                self.pending.push_back(Pending {
                    candidate,
                    outcome,
                    hash: None,
                    submitted: false,
                    term,
                });
                (candidate, outcome)
            }
        };
        // Temporary encoding or proposal pressure preserves the candidate: the
        // caller polls and resubmits the exact request without a second candidate.
        match self.flush_proposals(consensus) {
            Ok(()) => {}
            Err(error) => match error.class() {
                FailureClass::Retryable | FailureClass::Authority => {}
                FailureClass::Request => {
                    // A deterministic refusal discarded that candidate and its suffix.
                    if !self
                        .pending
                        .iter()
                        .any(|pending| pending.candidate == candidate)
                    {
                        return Err(error);
                    }
                }
                FailureClass::FailClosed => {
                    self.failed = true;
                    return Err(error);
                }
            },
        }
        Ok(NativeSubmission::Pending { candidate, outcome })
    }
    /// Encode and propose every unsubmitted candidate in chain order. Encoding
    /// refusals discard the candidate and its suffix; proposal pressure keeps
    /// them for the next flush.
    pub(super) fn flush_proposals(
        &mut self,
        consensus: &mut DurableNode,
    ) -> Result<(), NativeSessionError> {
        let Some(Domain::Active(owner, _)) = self.domain.as_mut() else {
            return Err(NativeSessionError::Failed);
        };
        let mut position = 0usize;
        while let Some(pending) = self.pending.get_mut(position) {
            if !pending.submitted {
                let encoded = match owner.encode_candidate(pending.candidate, self.limits.encoding)
                {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        let error = NativeSessionError::Owner(error);
                        if error.class() == FailureClass::Request {
                            let candidate = pending.candidate;
                            owner.discard_from(candidate)?;
                            self.pending.truncate(position);
                        }
                        return Err(error);
                    }
                };
                pending.hash = Some(encoded.hash());
                consensus.propose_borrowed_in(encoded.bytes(), BudgetLane::Completion)?;
                pending.submitted = true;
            }
            position = add(position, 1)?;
        }
        Ok(())
    }
}
