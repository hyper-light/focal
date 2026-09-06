use crate::{pins::PinRegistry, *};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ContentHash, LedgerId, RaftTerm, RouteEpoch, SessionSeq};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeIntent {
    pub ledger: LedgerId,
    pub operation: TransferId,
    pub old_epoch: RouteEpoch,
    pub sources: BTreeSet<RangeId>,
    pub replacements: Vec<RangeDescriptor>,
    pub seed: SessionSeq,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferState {
    pub intent: RangeIntent,
    pub desired: RangeMap,
    pub intent_commit: Option<CommitProof>,
    pub snapshots: BTreeMap<RangeId, ContentHash>,
    pub barrier: Option<CommitProof>,
    pub source_seals: BTreeMap<RangeId, SourceSealProof>,
    pub ready: BTreeMap<RangeId, DestinationReady>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalMap {
    pub map: RangeMap,
    pub activation: ActivationCertificate,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeCheckpoint {
    pub schema: u16,
    pub map: RangeMap,
    pub control_sequence: SessionSeq,
    pub last_commit: Option<CommitProof>,
    pub published: SessionSeq,
    pub pending: Option<TransferState>,
    pub history: Vec<HistoricalMap>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RangeOperation {
    Begin(RangeIntent),
    Snapshot {
        operation: TransferId,
        range: RangeId,
        hash: ContentHash,
    },
    Barrier {
        operation: TransferId,
    },
    SourceSealed(SourceSealProof),
    Ready(DestinationReady),
    Activate {
        operation: TransferId,
        map: ContentHash,
        proofs: ContentHash,
        unchanged: Vec<RangeProgress>,
    },
    Abort {
        operation: TransferId,
    },
    Cleanup {
        operation: TransferId,
        recovery: RecoveryProof,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentCertificate {
    pub intent: RangeIntent,
    pub commit: CommitProof,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierCertificate {
    pub intent: RangeIntent,
    pub commit: CommitProof,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationCertificate {
    pub intent: RangeIntent,
    pub map: RangeMap,
    pub barrier: CommitProof,
    pub snapshots: BTreeMap<RangeId, ContentHash>,
    pub sources: BTreeMap<RangeId, SourceSealProof>,
    pub destinations: BTreeMap<RangeId, DestinationReady>,
    pub unchanged: Vec<RangeProgress>,
    pub commit: CommitProof,
}
impl ActivationCertificate {
    pub fn proof_digest(&self) -> Result<ContentHash, RangeError> {
        proof_digest(
            &self.intent,
            &self.snapshots,
            &self.barrier,
            &self.sources,
            &self.destinations,
        )
    }
    pub fn verify(
        &self,
        limits: RangeLimits,
        verifier: &impl RangeVerifier,
    ) -> Result<(), RangeError> {
        self.map.validate(limits)?;
        if self.map.ledger() != self.intent.ledger
            || self.intent.old_epoch.0.checked_add(1) != Some(self.map.epoch().0)
        {
            return Err(RangeError::StaleEpoch);
        }
        let operation = RangeOperation::Activate {
            operation: self.intent.operation,
            map: self.map.hash()?,
            proofs: self.proof_digest()?,
            unchanged: self.unchanged.clone(),
        };
        verify_commit(
            &self.commit,
            self.intent.ledger,
            self.commit.sequence,
            range_command_hash(self.intent.ledger, self.commit.sequence, &operation)?,
            verifier,
        )?;
        if self.commit.sequence <= self.barrier.sequence || self.barrier.sequence < self.intent.seed
        {
            return Err(RangeError::StaleEpoch);
        }
        for source in self.sources.values() {
            verifier.source_seal(source)?;
        }
        for destination in self.destinations.values() {
            verifier.destination_ready(destination)?;
        }
        Ok(())
    }
}
pub fn range_command_hash(
    ledger: LedgerId,
    sequence: SessionSeq,
    operation: &RangeOperation,
) -> Result<ContentHash, RangeError> {
    digest("focal.range-command.v1", &(ledger, sequence, operation))
}
fn proof_digest(
    intent: &RangeIntent,
    snapshots: &BTreeMap<RangeId, ContentHash>,
    barrier: &CommitProof,
    sources: &BTreeMap<RangeId, SourceSealProof>,
    destinations: &BTreeMap<RangeId, DestinationReady>,
) -> Result<ContentHash, RangeError> {
    digest(
        "focal.range-transfer-proofs.v1",
        &(intent, snapshots, barrier, sources, destinations),
    )
}
struct Version {
    state: RangeCheckpoint,
    _allocation: Allocation,
}
pub struct PreparedRangeCommand {
    owner: focal_memory::OwnerId,
    base_sequence: SessionSeq,
    next: Version,
    sequence: SessionSeq,
    hash: ContentHash,
    operation: RangeOperation,
    existing: bool,
}
impl PreparedRangeCommand {
    pub fn checkpoint(&self) -> &RangeCheckpoint {
        &self.next.state
    }
    pub fn hash(&self) -> ContentHash {
        self.hash
    }
}
pub struct RangeCoordinator {
    root: Version,
    owner: focal_memory::OwnerId,
    limits: RangeLimits,
    budget: MemoryBudget,
    pins: PinRegistry,
    progress: BTreeMap<RangeId, RangeProgress>,
    committed: SessionSeq,
    term: RaftTerm,
    _runtime: Allocation,
}
impl RangeCoordinator {
    pub fn new(
        map: RangeMap,
        incarnation: ControllerIncarnation,
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, RangeError> {
        Self::restore(
            RangeCheckpoint {
                schema: 1,
                map,
                control_sequence: SessionSeq(0),
                last_commit: None,
                published: SessionSeq(0),
                pending: None,
                history: vec![],
            },
            incarnation,
            limits,
            budget,
        )
    }
    pub fn restore(
        state: RangeCheckpoint,
        incarnation: ControllerIncarnation,
        limits: RangeLimits,
        budget: MemoryBudget,
    ) -> Result<Self, RangeError> {
        validate_checkpoint(&state, limits)?;
        let allocation = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                state_charge(&state)?,
            )?
            .commit();
        let runtime = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                mul(limits.max_ranges, row::<RangeProgress>())?,
            )?
            .commit();
        let committed = state.control_sequence.max(state.published);
        let term = state
            .last_commit
            .as_ref()
            .map_or(RaftTerm(0), |proof| proof.term);
        Ok(Self {
            root: Version {
                state,
                _allocation: allocation,
            },
            owner: focal_memory::OwnerId::new()?,
            limits,
            budget: budget.clone(),
            pins: PinRegistry::new(incarnation, limits, budget),
            progress: BTreeMap::new(),
            committed,
            term,
            _runtime: runtime,
        })
    }
    pub fn checkpoint(&self) -> &RangeCheckpoint {
        &self.root.state
    }
    pub fn map(&self) -> &RangeMap {
        &self.root.state.map
    }
    pub fn pending(&self) -> Option<&TransferState> {
        self.root.state.pending.as_ref()
    }
    pub fn activation(&self, operation: TransferId) -> Option<&ActivationCertificate> {
        self.root
            .state
            .history
            .iter()
            .find(|old| old.activation.intent.operation == operation)
            .map(|old| &old.activation)
    }
    pub fn intent_certificate(&self) -> Option<IntentCertificate> {
        self.pending().and_then(|pending| {
            pending
                .intent_commit
                .clone()
                .map(|commit| IntentCertificate {
                    intent: pending.intent.clone(),
                    commit,
                })
        })
    }
    pub fn barrier_certificate(&self) -> Option<BarrierCertificate> {
        self.pending().and_then(|pending| {
            pending.barrier.clone().map(|commit| BarrierCertificate {
                intent: pending.intent.clone(),
                commit,
            })
        })
    }
    pub fn activation_operation(
        &self,
        unchanged: Vec<RangeProgress>,
    ) -> Result<RangeOperation, RangeError> {
        let pending = self.pending().ok_or(RangeError::Phase)?;
        let barrier = pending.barrier.as_ref().ok_or(RangeError::NotReady)?;
        Ok(RangeOperation::Activate {
            operation: pending.intent.operation,
            map: pending.desired.hash()?,
            proofs: proof_digest(
                &pending.intent,
                &pending.snapshots,
                barrier,
                &pending.source_seals,
                &pending.ready,
            )?,
            unchanged,
        })
    }
    pub fn prepare(
        &self,
        sequence: SessionSeq,
        operation: RangeOperation,
        verifier: &impl RangeVerifier,
    ) -> Result<PreparedRangeCommand, RangeError> {
        let hash = range_command_hash(self.map().ledger(), sequence, &operation)?;
        if sequence == self.root.state.control_sequence
            && self
                .root
                .state
                .last_commit
                .as_ref()
                .is_some_and(|proof| proof.command == hash)
        {
            let allocation = self
                .budget
                .reserve(
                    BudgetKind::Control,
                    BudgetLane::Completion,
                    state_charge(&self.root.state)?,
                )?
                .commit();
            return Ok(PreparedRangeCommand {
                owner: self.owner,
                base_sequence: self.root.state.control_sequence,
                next: Version {
                    state: self.root.state.clone(),
                    _allocation: allocation,
                },
                sequence,
                hash,
                operation,
                existing: true,
            });
        }
        if sequence <= self.root.state.control_sequence {
            return Err(RangeError::RetryTooOld);
        }
        let command_bytes = postcard::experimental::serialized_size(&operation)?;
        if command_bytes > self.limits.max_checkpoint_bytes {
            return Err(RangeError::Capacity);
        }
        let bytes = add(
            state_charge(&self.root.state)?,
            mul(add(command_bytes, 8192)?, 32)?,
        )?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)?
            .commit();
        let mut state = self.root.state.clone();
        match &operation {
            RangeOperation::Begin(intent) => {
                if let Some(pending) = &state.pending {
                    if pending.intent != *intent {
                        return Err(RangeError::Phase);
                    }
                } else {
                    if state.history.len() == self.limits.max_history {
                        return Err(RangeError::Capacity);
                    }
                    if intent.ledger != state.map.ledger() {
                        return Err(RangeError::WrongLedger);
                    }
                    if intent.old_epoch != state.map.epoch() || intent.seed > sequence {
                        return Err(RangeError::StaleEpoch);
                    }
                    if state
                        .history
                        .iter()
                        .any(|old| old.activation.intent.operation == intent.operation)
                    {
                        return Err(RangeError::Conflict);
                    }
                    let desired = state.map.replace(
                        &intent.sources,
                        intent.replacements.clone(),
                        self.limits,
                    )?;
                    state.pending = Some(TransferState {
                        intent: intent.clone(),
                        desired,
                        intent_commit: None,
                        snapshots: BTreeMap::new(),
                        barrier: None,
                        source_seals: BTreeMap::new(),
                        ready: BTreeMap::new(),
                    });
                }
            }
            RangeOperation::Snapshot {
                operation,
                range,
                hash,
            } => {
                let pending = pending(&mut state, *operation)?;
                if !pending
                    .intent
                    .replacements
                    .iter()
                    .any(|item| item.id == *range)
                    || !nonzero(*hash)
                {
                    return Err(RangeError::Checksum);
                }
                if pending.snapshots.get(range).is_some_and(|old| old != hash) {
                    return Err(RangeError::Conflict);
                }
                pending.snapshots.insert(*range, *hash);
            }
            RangeOperation::Barrier { operation } => {
                let pending = pending(&mut state, *operation)?;
                if pending.snapshots.len() != pending.intent.replacements.len() {
                    return Err(RangeError::NotReady);
                }
                if pending.barrier.is_some() {
                    return Err(RangeError::Conflict);
                }
                if sequence < pending.intent.seed {
                    return Err(RangeError::StaleEpoch);
                }
                pending.barrier = Some(placeholder(pending.intent.ledger, sequence, hash));
            }
            RangeOperation::SourceSealed(proof) => {
                let source = state
                    .map
                    .get(proof.range)
                    .ok_or(RangeError::Missing)?
                    .clone();
                let pending = pending(&mut state, proof.operation)?;
                let barrier = pending.barrier.as_ref().ok_or(RangeError::NotReady)?;
                if proof.ledger != pending.intent.ledger
                    || proof.old_epoch != pending.intent.old_epoch
                    || !pending.intent.sources.contains(&proof.range)
                    || proof.range_generation != source.generation
                    || proof.replica != source.owner
                    || proof.cut != barrier.sequence
                    || !nonzero(proof.checkpoint)
                    || !nonzero(proof.attestation)
                {
                    return Err(RangeError::Generation);
                }
                verifier.source_seal(proof)?;
                if pending
                    .source_seals
                    .get(&proof.range)
                    .is_some_and(|old| old != proof)
                {
                    return Err(RangeError::Conflict);
                }
                pending.source_seals.insert(proof.range, proof.clone());
            }
            RangeOperation::Ready(proof) => {
                let pending = pending(&mut state, proof.operation)?;
                let target = pending
                    .intent
                    .replacements
                    .iter()
                    .find(|range| range.id == proof.range)
                    .ok_or(RangeError::Missing)?;
                let barrier = pending.barrier.as_ref().ok_or(RangeError::NotReady)?;
                if proof.ledger != pending.intent.ledger
                    || proof.new_epoch != pending.desired.epoch()
                    || proof.range_generation != target.generation
                    || proof.replica != target.owner
                    || proof.seed != pending.intent.seed
                    || proof.through < barrier.sequence
                    || pending.snapshots.get(&proof.range) != Some(&proof.snapshot)
                    || !nonzero(proof.state)
                    || !nonzero(proof.checkpoint)
                    || !nonzero(proof.attestation)
                {
                    return Err(RangeError::NotReady);
                }
                verifier.destination_ready(proof)?;
                if pending
                    .ready
                    .get(&proof.range)
                    .is_some_and(|old| old.through > proof.through)
                {
                    return Err(RangeError::StaleEpoch);
                }
                pending.ready.insert(proof.range, proof.clone());
            }
            RangeOperation::Activate {
                operation,
                map,
                proofs,
                unchanged,
            } => {
                let pending = pending(&mut state, *operation)?.clone();
                let barrier = pending.barrier.as_ref().ok_or(RangeError::NotReady)?;
                if *map != pending.desired.hash()?
                    || *proofs
                        != proof_digest(
                            &pending.intent,
                            &pending.snapshots,
                            barrier,
                            &pending.source_seals,
                            &pending.ready,
                        )?
                {
                    return Err(RangeError::Checksum);
                }
                if pending.source_seals.len() != pending.intent.sources.len()
                    || pending.ready.len() != pending.intent.replacements.len()
                    || sequence <= barrier.sequence
                {
                    return Err(RangeError::NotReady);
                }
                let others: Vec<_> = state
                    .map
                    .ranges()
                    .iter()
                    .filter(|range| !pending.intent.sources.contains(&range.id))
                    .collect();
                if unchanged.len() != others.len() {
                    return Err(RangeError::NotReady);
                }
                for range in others {
                    let matches: Vec<_> = unchanged
                        .iter()
                        .filter(|proof| proof.range == range.id)
                        .collect();
                    if matches.len() != 1 {
                        return Err(RangeError::NotReady);
                    }
                    let proof = matches.first().ok_or(RangeError::NotReady)?;
                    validate_progress(&state.map, proof)?;
                    if proof.through < barrier.sequence {
                        return Err(RangeError::NotReady);
                    }
                    verifier.progress(proof)?;
                }
                let activation = ActivationCertificate {
                    intent: pending.intent,
                    map: pending.desired.clone(),
                    barrier: barrier.clone(),
                    snapshots: pending.snapshots,
                    sources: pending.source_seals,
                    destinations: pending.ready,
                    unchanged: unchanged.clone(),
                    commit: placeholder(state.map.ledger(), sequence, hash),
                };
                state.history.push(HistoricalMap {
                    map: state.map.clone(),
                    activation,
                });
                state.map = pending.desired;
                state.pending = None;
            }
            RangeOperation::Abort { operation } => {
                if pending(&mut state, *operation)?.barrier.is_some() {
                    return Err(RangeError::Sealed);
                }
                state.pending = None;
            }
            RangeOperation::Cleanup {
                operation,
                recovery,
            } => {
                let position = state
                    .history
                    .iter()
                    .position(|old| old.activation.intent.operation == *operation)
                    .ok_or(RangeError::Missing)?;
                let history = state.history.get(position).ok_or(RangeError::Missing)?;
                if self.pins.pinned(history.map.epoch()) {
                    return Err(RangeError::Pinned);
                }
                if recovery.ledger != state.map.ledger()
                    || recovery.epoch < history.activation.map.epoch()
                    || recovery.through < history.activation.barrier.sequence
                    || !nonzero(recovery.manifest)
                    || !nonzero(recovery.attestation)
                {
                    return Err(RangeError::NotReady);
                }
                verifier.recovery(recovery)?;
                state.history.remove(position);
            }
        }
        state.control_sequence = sequence;
        state.last_commit = Some(placeholder(state.map.ledger(), sequence, hash));
        if state_charge(&state)? > bytes {
            return Err(RangeError::Capacity);
        }
        // Incomplete commit proofs exist only inside this private preparation.
        Ok(PreparedRangeCommand {
            owner: self.owner,
            base_sequence: self.root.state.control_sequence,
            next: Version {
                state,
                _allocation: allocation,
            },
            sequence,
            hash,
            operation,
            existing: false,
        })
    }
    pub fn publish(
        &mut self,
        mut prepared: PreparedRangeCommand,
        proof: &CommitProof,
        verifier: &impl RangeVerifier,
    ) -> Result<(), RangeError> {
        if self.owner != prepared.owner
            || self.root.state.control_sequence != prepared.base_sequence
        {
            return Err(RangeError::StalePreparation);
        }
        verify_commit(
            proof,
            self.map().ledger(),
            prepared.sequence,
            prepared.hash,
            verifier,
        )?;
        if prepared.existing {
            return if self.root.state.last_commit.as_ref() == Some(proof) {
                Ok(())
            } else {
                Err(RangeError::Conflict)
            };
        }
        if self
            .root
            .state
            .last_commit
            .as_ref()
            .is_some_and(|old| proof.index <= old.index || proof.term < old.term)
        {
            return Err(RangeError::StaleEpoch);
        }
        let state = &mut prepared.next.state;
        state.last_commit = Some(proof.clone());
        state.published = state.published.max(self.root.state.published);
        match &prepared.operation {
            RangeOperation::Begin(_) => {
                let pending = state.pending.as_mut().ok_or(RangeError::Phase)?;
                if pending.intent_commit.is_none() {
                    pending.intent_commit = Some(proof.clone());
                }
            }
            RangeOperation::Barrier { .. } => {
                state.pending.as_mut().ok_or(RangeError::Phase)?.barrier = Some(proof.clone())
            }
            RangeOperation::Activate { .. } => {
                state
                    .history
                    .last_mut()
                    .ok_or(RangeError::Missing)?
                    .activation
                    .commit = proof.clone();
                self.progress.clear();
            }
            _ => {}
        }
        self.committed = self.committed.max(proof.sequence);
        self.term = self.term.max(proof.term);
        self.root = prepared.next;
        Ok(())
    }
    pub fn observe_committed(
        &mut self,
        proof: &CommitProof,
        verifier: &impl RangeVerifier,
    ) -> Result<(), RangeError> {
        if proof.ledger != self.map().ledger() {
            return Err(RangeError::WrongLedger);
        }
        verifier.commit(proof)?;
        if proof.sequence < self.committed || proof.term < self.term {
            return Err(RangeError::StaleEpoch);
        }
        if proof.term > self.term {
            self.progress.clear();
        }
        self.committed = proof.sequence;
        self.term = proof.term;
        Ok(())
    }
    pub fn observe_progress(
        &mut self,
        proof: RangeProgress,
        verifier: &impl RangeVerifier,
    ) -> Result<(), RangeError> {
        validate_progress(self.map(), &proof)?;
        verifier.progress(&proof)?;
        if proof.through > self.committed
            || proof.term != self.term
            || self
                .progress
                .get(&proof.range)
                .is_some_and(|old| old.through > proof.through)
        {
            return Err(RangeError::StaleEpoch);
        }
        self.progress.insert(proof.range, proof);
        if let Some(published) = self.published() {
            self.root.state.published = self.root.state.published.max(published);
        }
        Ok(())
    }
    pub fn published(&self) -> Option<SessionSeq> {
        if self.progress.len() != self.map().ranges().len() {
            return None;
        }
        let candidate = self
            .progress
            .values()
            .map(|proof| proof.through)
            .min()?
            .min(self.committed);
        (candidate >= self.root.state.published).then_some(candidate)
    }
    pub fn pin(
        &mut self,
        request: PinRequest,
        proofs: &[ReadAvailability],
        verifier: &impl RangeVerifier,
    ) -> Result<RangeCursor, RangeError> {
        if self
            .published()
            .is_none_or(|published| request.prefix > published)
        {
            return Err(RangeError::NotReady);
        }
        self.pins
            .pin(&self.root.state.map, request, proofs, verifier)
    }
    pub fn read_plan(
        &mut self,
        cursor: &RangeCursor,
        now: u64,
        verifier: &impl RangeVerifier,
    ) -> Result<RangeReadPlan, RangeError> {
        self.pins.plan(cursor, now, None, verifier)
    }
    pub fn translate(
        &mut self,
        cursor: &RangeCursor,
        now: u64,
        proofs: &[ReadAvailability],
        verifier: &impl RangeVerifier,
    ) -> Result<RangeReadPlan, RangeError> {
        self.pins
            .plan(cursor, now, Some((&self.root.state.map, proofs)), verifier)
    }
    pub fn release_pin(&mut self, cursor: &RangeCursor) -> Result<(), RangeError> {
        self.pins.release(cursor)
    }
    pub fn advance_clock(&mut self, now: u64) -> Result<(), RangeError> {
        self.pins.advance(now)
    }
}
fn placeholder(ledger: LedgerId, sequence: SessionSeq, command: ContentHash) -> CommitProof {
    CommitProof {
        ledger,
        sequence,
        index: focal_model::RaftIndex(0),
        term: RaftTerm(0),
        command,
        attestation: ContentHash([0; 32]),
    }
}
fn pending(
    state: &mut RangeCheckpoint,
    operation: TransferId,
) -> Result<&mut TransferState, RangeError> {
    let pending = state.pending.as_mut().ok_or(RangeError::Phase)?;
    if pending.intent.operation != operation {
        return Err(RangeError::Conflict);
    }
    Ok(pending)
}
fn validate_progress(map: &RangeMap, proof: &RangeProgress) -> Result<(), RangeError> {
    if proof.ledger != map.ledger() {
        return Err(RangeError::WrongLedger);
    }
    let range = map.get(proof.range).ok_or(RangeError::Missing)?;
    if proof.epoch != map.epoch()
        || proof.range_generation != range.generation
        || proof.replica != range.owner
    {
        return Err(RangeError::Generation);
    }
    if proof.term.0 == 0 || !nonzero(proof.root) || !nonzero(proof.attestation) {
        return Err(RangeError::Unverified);
    }
    Ok(())
}
fn state_charge(state: &RangeCheckpoint) -> Result<usize, RangeError> {
    mul(
        add(postcard::experimental::serialized_size(state)?, 8192)?,
        32,
    )
}
fn validate_checkpoint(state: &RangeCheckpoint, limits: RangeLimits) -> Result<(), RangeError> {
    state.map.validate(limits)?;
    if state.schema != 1 || state.history.len() > limits.max_history {
        return Err(RangeError::Capacity);
    }
    if (state.control_sequence.0 == 0) != state.last_commit.is_none()
        || state.last_commit.as_ref().is_some_and(|proof| {
            proof.ledger != state.map.ledger()
                || proof.sequence != state.control_sequence
                || proof.index.0 == 0
                || proof.term.0 == 0
                || !nonzero(proof.attestation)
        })
    {
        return Err(RangeError::Conflict);
    }
    if let Some(pending) = &state.pending {
        let desired = state.map.replace(
            &pending.intent.sources,
            pending.intent.replacements.clone(),
            limits,
        )?;
        if pending.intent.ledger != state.map.ledger()
            || pending.intent.old_epoch != state.map.epoch()
            || desired != pending.desired
            || pending.intent_commit.is_none()
            || pending.snapshots.len() > pending.intent.replacements.len()
            || pending.ready.len() > pending.intent.replacements.len()
            || pending.source_seals.len() > pending.intent.sources.len()
        {
            return Err(RangeError::Conflict);
        }
        if let Some(barrier) = &pending.barrier {
            if barrier.sequence < pending.intent.seed
                || barrier.sequence > state.control_sequence
                || barrier.ledger != state.map.ledger()
                || barrier.command
                    != range_command_hash(
                        state.map.ledger(),
                        barrier.sequence,
                        &RangeOperation::Barrier {
                            operation: pending.intent.operation,
                        },
                    )?
            {
                return Err(RangeError::Conflict);
            }
        } else if !pending.source_seals.is_empty() || !pending.ready.is_empty() {
            return Err(RangeError::Phase);
        }
    }
    let mut epochs = BTreeSet::new();
    for old in &state.history {
        old.map.validate(limits)?;
        if old.map.ledger() != state.map.ledger()
            || old.map.epoch() >= state.map.epoch()
            || !epochs.insert(old.map.epoch())
            || old.activation.map.epoch().0
                != old
                    .map
                    .epoch()
                    .0
                    .checked_add(1)
                    .ok_or(RangeError::Overflow)?
        {
            return Err(RangeError::StaleEpoch);
        }
    }
    Ok(())
}
