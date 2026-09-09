//! Exact-target response qualification and bounded incremental claim coverage.
//!
//! Only checked response-evaluation and accepted-result capabilities enter this
//! module. The owner stages these indexes beside the artifact, response and claim
//! replacements, retains every returned cause/result as evidence, and publishes
//! them at one committed prefix. These indexes are not an ingress or persistence
//! format. They never discover evidence by schema or scan the response collection.
//!
//! Claim creation pins the complete acceptance manifest. Admission and Increment
//! retain independent readiness and outcomes; closing acceptance also requires
//! their declared Required obligations, without substituting WholeWork evidence.

#[path = "acceptance.rs"]
mod acceptance;
#[path = "aggregation_admission.rs"]
mod admission;
#[path = "aggregation_projection.rs"]
mod projection;
#[path = "registration.rs"]
mod registration;
#[path = "aggregation_snapshot.rs"]
mod snapshot;
#[cfg(test)]
pub(crate) use acceptance::acceptance_for;
pub(in crate::lifecycle) use acceptance::preparation::{SourceShape, check_sources};
pub use acceptance::{
    AcceptancePlan, AcceptancePolicy, AcceptanceSource, AcceptanceSourcePasses,
    AcceptanceSourcePlan, DeclaredObligation, EvaluationRegistry, ObligationTarget, PolicyShape,
    RegisteredEvaluation, SealedTargets,
};
pub use admission::{
    AdmissionLimits, AdmissionView, PublishedAdmissionResult, admission_completion_visits,
    project_admission,
};
pub use projection::{
    ProjectionLimits, ProjectionPlan, ProjectionQuote, ProjectionShape, PublicationPosition,
    PublishedResponse, PublishedResult, WholeWorkProjection, WholeWorkView, prepare_projection,
    quote_projection,
};
pub use registration::{
    NativeSealedTargets, RegistrationHydrationPlan, RegistrationMemberSnapshotV1, RegistrationSet,
    RegistrationSnapshotSource, RegistrationSnapshotV1, RegistrationValue,
};
pub use snapshot::{BlockingCauseSnapshotV1, TerminalCutSnapshotV1};

use super::evidence::ResponseEvaluation;
use super::validation::{AcceptedResult, Phase, Target};
use super::{Binding, ContractError};
use crate::{
    ArtifactId, ArtifactRef, ClaimId, ClaimStatus, ContentHash, ReceiptFence, SessionSeq,
    TestamentId, ValidationId, ValidationMode, VerdictValue,
};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_slots: usize,
    pub max_checks: usize,
    pub max_results: usize,
    pub max_updates: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckPolicy {
    pub declaration_index: u32,
    pub validation: ValidationId,
    pub mode: ValidationMode,
}

/// Immutable declaration data, borrowed for the lifetime of its owner index.
#[derive(Debug, Clone, Copy)]
pub struct SlotPolicy<'a> {
    pub slot: u32,
    /// Index of this slot-presence obligation in the immutable declaration, not
    /// its slot ID. It must be distinct from all slot/check declaration indexes.
    pub missing_declaration_index: u32,
    pub mode: ValidationMode,
    pub checks: &'a [CheckPolicy],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CausePhase {
    Programmatic,
    Quality,
    Delivery,
    MissingTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CauseTarget {
    Admission,
    Increment {
        artifact: ArtifactId,
        content: ContentHash,
    },
    Response(TestamentId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CauseKey {
    pub target: CauseTarget,
    pub declaration_index: u32,
    /// Missing targets and pure delivery do not invent an evaluation run.
    pub generation: Option<u64>,
    pub attempt: Option<u32>,
    pub phase: CausePhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockingKind {
    Incomplete,
    Failed,
    Errored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockingCause {
    key: CauseKey,
    slot: Option<u32>,
    artifact: Option<ArtifactRef>,
    kind: BlockingKind,
    mode: ValidationMode,
    slot_mode: ValidationMode,
    evidence: Option<ArtifactRef>,
}

impl BlockingCause {
    pub fn key(&self) -> CauseKey {
        self.key
    }
    pub fn slot(&self) -> Option<u32> {
        self.slot
    }
    pub fn artifact(&self) -> Option<ArtifactRef> {
        self.artifact
    }
    pub fn kind(&self) -> BlockingKind {
        self.kind
    }
    pub fn mode(&self) -> ValidationMode {
        self.mode
    }
    pub fn slot_mode(&self) -> ValidationMode {
        self.slot_mode
    }
    fn blocks_parent(&self) -> bool {
        self.mode == ValidationMode::Required && self.slot_mode == ValidationMode::Required
    }
    pub fn evidence(&self) -> Option<ArtifactRef> {
        self.evidence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalCut {
    sequence: SessionSeq,
    cause: BlockingCause,
}

impl TerminalCut {
    pub fn sequence(&self) -> SessionSeq {
        self.sequence
    }
    pub fn cause(&self) -> BlockingCause {
        self.cause
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOutcome {
    Pending,
    LocalComplete { sequence: SessionSeq },
    Blocked(TerminalCut),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseOutcome {
    Evaluating,
    Validated { sequence: SessionSeq },
    Blocked(TerminalCut),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactOutcome {
    Pending,
    Passed,
    Blocked(BlockingCause),
}

/// Only a checked exact-target reduction can construct this publication token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactDecision {
    claim: Binding,
    response: Binding,
    slot: u32,
    artifact: ArtifactRef,
    outcome: ArtifactOutcome,
    sequence: SessionSeq,
}

impl ArtifactDecision {
    pub fn claim(&self) -> Binding {
        self.claim
    }
    pub fn response(&self) -> Binding {
        self.response
    }
    pub fn slot(&self) -> u32 {
        self.slot
    }
    pub fn artifact(&self) -> ArtifactRef {
        self.artifact
    }
    pub fn outcome(&self) -> ArtifactOutcome {
        self.outcome
    }
    pub fn sequence(&self) -> SessionSeq {
        self.sequence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckWitness {
    pub validation: ValidationId,
    pub declaration_index: u32,
    pub generation: u64,
    pub attempt: Option<u32>,
    pub programmatic_evidence: Option<ArtifactRef>,
    pub evidence: Option<ArtifactRef>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SlotWitness {
    response: TestamentId,
    slot: u32,
    artifact: ArtifactRef,
    checks: Vec<CheckWitness>,
}

impl SlotWitness {
    pub fn response(&self) -> TestamentId {
        self.response
    }
    pub fn slot(&self) -> u32 {
        self.slot
    }
    pub fn artifact(&self) -> ArtifactRef {
        self.artifact
    }
    pub fn checks(&self) -> &[CheckWitness] {
        &self.checks
    }
    fn copy(&self) -> Result<Self, ContractError> {
        let mut checks = reserved(self.checks.len())?;
        checks.extend_from_slice(&self.checks);
        Ok(Self {
            response: self.response,
            slot: self.slot,
            artifact: self.artifact,
            checks,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryWitness {
    response: Binding,
    receipt: ReceiptFence,
    validation: ValidationId,
    result: Binding,
}

impl DeliveryWitness {
    pub fn response(&self) -> Binding {
        self.response
    }
    pub fn receipt(&self) -> ReceiptFence {
        self.receipt
    }
    pub fn validation(&self) -> ValidationId {
        self.validation
    }
    pub fn result(&self) -> Binding {
        self.result
    }
}

/// Private witness fields prevent a caller constructing coverage from booleans.
#[derive(Debug)]
pub struct ResponseUpdate {
    claim: Binding,
    policy: AcceptancePolicy,
    sequence: SessionSeq,
    delivery: DeliveryWitness,
    delivery_results: Vec<AcceptedResult>,
    witnesses: Vec<SlotWitness>,
    causes: Vec<BlockingCause>,
    outcome: ResponseOutcome,
    artifacts: Vec<ArtifactDecision>,
}

impl ResponseUpdate {
    pub fn decision(&self) -> ResponseDecision<'_> {
        ResponseDecision {
            claim: self.claim,
            response: self.delivery.response,
            receipt: self.delivery.receipt,
            sequence: self.sequence,
            outcome: self.outcome,
            artifacts: &self.artifacts,
        }
    }
    pub fn response_outcome(&self) -> ResponseOutcome {
        self.outcome
    }
    pub fn response_binding(&self) -> Binding {
        self.delivery.response
    }
    pub fn claim_binding(&self) -> Binding {
        self.claim
    }
    pub fn outcome_for_slot(&self, slot: u32) -> Option<&ArtifactDecision> {
        self.artifacts.iter().find(|decision| decision.slot == slot)
    }
    pub fn witnesses(&self) -> &[SlotWitness] {
        &self.witnesses
    }
    /// Includes simultaneous and Observe causes, even when an alternative covers
    /// their slot. The owner must retain them independently of the selected cut.
    pub fn causes(&self) -> &[BlockingCause] {
        &self.causes
    }
    pub fn delivery(&self) -> DeliveryWitness {
        self.delivery
    }
    pub fn delivery_results(&self) -> &[AcceptedResult] {
        &self.delivery_results
    }

    pub fn sequence(&self) -> SessionSeq {
        self.sequence
    }
}

/// A checked borrowed publication proof, produced by an owned aggregate or by
/// the actual-owner projection. There is no participant constructor.
#[derive(Debug, Clone, Copy)]
pub struct ResponseDecision<'a> {
    claim: Binding,
    response: Binding,
    receipt: ReceiptFence,
    sequence: SessionSeq,
    outcome: ResponseOutcome,
    artifacts: &'a [ArtifactDecision],
}
impl ResponseDecision<'_> {
    pub fn claim_binding(&self) -> Binding {
        self.claim
    }
    pub fn response_binding(&self) -> Binding {
        self.response
    }
    pub fn receipt(&self) -> ReceiptFence {
        self.receipt
    }
    pub fn sequence(&self) -> SessionSeq {
        self.sequence
    }
    pub fn response_outcome(&self) -> ResponseOutcome {
        self.outcome
    }
    pub fn outcome_for_slot(&self, slot: u32) -> Option<&ArtifactDecision> {
        self.artifacts.iter().find(|decision| decision.slot == slot)
    }
}

struct ResponseSlot {
    artifact: Option<ArtifactRef>,
    passes: Vec<Option<AcceptedResult>>,
    terminal: Option<(SessionSeq, ArtifactOutcome)>,
}

/// One response's bounded check index; each mutation visits this response only.
pub struct ResponseAggregation {
    claim: Binding,
    response: Binding,
    delivery: DeliveryWitness,
    delivery_results: Vec<AcceptedResult>,
    policy: AcceptancePolicy,
    slots: Vec<ResponseSlot>,
    limits: Limits,
    sequence: SessionSeq,
    outcome: ResponseOutcome,
}

fn reserved<T>(length: usize) -> Result<Vec<T>, ContractError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(length)
        .map_err(|_| ContractError::Capacity)?;
    Ok(values)
}

// Evaluation pins immutable response identity. Lifecycle revisions advance at
// receipt/begin/aggregate; the publishing owner checks its current row revision.
fn same_content(left: Binding, right: Binding) -> Result<(), ContractError> {
    left.check(&Binding {
        revision: left.revision,
        ..right
    })
}

// Scalar policy rules shared with repeatable, unmaterialized authored sources.
// The traversal and its debit remain the responsibility of each checked caller.
pub(in crate::lifecycle) fn check_policy_order(
    previous: Option<u32>,
    current: u32,
) -> Result<(), ContractError> {
    if previous.is_some_and(|old| old >= current) {
        return Err(ContractError::InvalidPolicy);
    }
    Ok(())
}
pub(in crate::lifecycle) fn check_policy_index(left: u32, right: u32) -> Result<(), ContractError> {
    if left == right {
        return Err(ContractError::InvalidPolicy);
    }
    Ok(())
}
pub(in crate::lifecycle) fn check_policy_validation(
    check: CheckPolicy,
) -> Result<(), ContractError> {
    if check.validation.is_zero() {
        return Err(ContractError::InvalidPolicy);
    }
    Ok(())
}
pub(in crate::lifecycle) fn check_policy_pair(
    left: CheckPolicy,
    right: CheckPolicy,
) -> Result<(), ContractError> {
    if left.validation == right.validation {
        return Err(ContractError::InvalidPolicy);
    }
    check_policy_index(left.declaration_index, right.declaration_index)
}

#[cfg(test)]
pub(in crate::lifecycle) fn validate_policies(
    policies: &[SlotPolicy<'_>],
    limits: Limits,
) -> Result<(), ContractError> {
    validate_policy_sources(
        policies.iter().copied(),
        limits,
        &mut super::graph::VisitBudget::new(usize::MAX),
    )
}

pub(in crate::lifecycle) fn validate_policy_sources<'a>(
    policies: impl ExactSizeIterator<Item = SlotPolicy<'a>> + Clone,
    limits: Limits,
    visits: &mut super::graph::VisitBudget,
) -> Result<(), ContractError> {
    if policies.len() > limits.max_slots || limits.max_results == 0 || limits.max_updates == 0 {
        return Err(ContractError::Capacity);
    }
    let mut count = 0usize;
    let mut previous = None;
    for (position, slot) in policies.clone().enumerate() {
        visits.charge(1)?;
        check_policy_order(previous, slot.slot)?;
        for old in policies.clone().take(position) {
            visits.charge(1)?;
            check_policy_index(
                old.missing_declaration_index,
                slot.missing_declaration_index,
            )?;
        }
        previous = Some(slot.slot);
        count = count
            .checked_add(slot.checks.len())
            .ok_or(ContractError::Capacity)?;
        if count > limits.max_checks {
            return Err(ContractError::Capacity);
        }
        for pair in slot.checks.windows(2) {
            visits.charge(1)?;
            match pair {
                [left, right] => {
                    check_policy_order(Some(left.declaration_index), right.declaration_index)?
                }
                _ => return Err(ContractError::InvalidPolicy),
            }
        }
    }
    // Empty slots still cost traversal work when flattened. Charge the complete
    // outer walk, including for a prefix scan that may stop earlier below.
    visits.charge(policies.len())?;
    for (position, check) in policies.clone().flat_map(|slot| slot.checks).enumerate() {
        visits.charge(1)?;
        check_policy_validation(*check)?;
        for slot in policies.clone() {
            visits.charge(1)?;
            check_policy_index(slot.missing_declaration_index, check.declaration_index)?;
        }
        visits.charge(policies.len())?;
        for old in policies.clone().flat_map(|slot| slot.checks).take(position) {
            visits.charge(1)?;
            check_policy_pair(*old, *check)?;
        }
    }
    Ok(())
}

fn phase(value: Phase) -> CausePhase {
    match value {
        Phase::Programmatic => CausePhase::Programmatic,
        Phase::Quality => CausePhase::Quality,
        Phase::Delivery => CausePhase::Delivery,
        Phase::MissingTarget => CausePhase::MissingTarget,
    }
}

fn key(result: &AcceptedResult) -> Result<CauseKey, ContractError> {
    let target = match result.target() {
        Target::Artifact { response, .. }
        | Target::MissingSlot { response, .. }
        | Target::Delivery { response } => CauseTarget::Response(TestamentId(response.object.0)),
        Target::Admission { .. } => CauseTarget::Admission,
        Target::Increment { artifact, .. } => CauseTarget::Increment {
            artifact: ArtifactId(artifact.object.0),
            content: artifact.content,
        },
    };
    let special = matches!(
        result.target(),
        Target::MissingSlot { .. } | Target::Delivery { .. }
    );
    Ok(CauseKey {
        target,
        declaration_index: result.declaration_index(),
        generation: if special {
            None
        } else {
            Some(result.generation())
        },
        attempt: result.attempt(),
        phase: phase(result.phase()),
    })
}

fn cause(
    result: &AcceptedResult,
    slot_mode: ValidationMode,
) -> Result<Option<BlockingCause>, ContractError> {
    if !result.is_terminal() {
        return Ok(None);
    }
    let kind = match result.verdict() {
        VerdictValue::Pass => return Ok(None),
        VerdictValue::Incomplete => BlockingKind::Incomplete,
        VerdictValue::Fail => BlockingKind::Failed,
        VerdictValue::Error => BlockingKind::Errored,
    };
    let (slot, artifact) = match result.target() {
        Target::Artifact { slot, artifact, .. } => (
            Some(slot),
            Some(ArtifactRef {
                id: ArtifactId(artifact.object.0),
                hash: artifact.content,
            }),
        ),
        Target::MissingSlot { slot, .. } => (Some(slot), None),
        Target::Delivery { .. } => (None, None),
        Target::Admission { .. } => (None, None),
        Target::Increment { artifact, .. } => (
            None,
            Some(ArtifactRef {
                id: ArtifactId(artifact.object.0),
                hash: artifact.content,
            }),
        ),
    };
    Ok(Some(BlockingCause {
        key: key(result)?,
        slot,
        artifact,
        kind,
        mode: result.mode(),
        slot_mode,
        evidence: result.evidence(),
    }))
}

fn missing_cause(response: TestamentId, slot: &acceptance::OwnedSlot) -> BlockingCause {
    BlockingCause {
        key: CauseKey {
            target: CauseTarget::Response(response),
            declaration_index: slot.missing_declaration_index,
            generation: None,
            attempt: None,
            phase: CausePhase::MissingTarget,
        },
        slot: Some(slot.slot),
        artifact: None,
        kind: BlockingKind::Incomplete,
        mode: ValidationMode::Required,
        slot_mode: slot.mode,
        evidence: None,
    }
}

impl ResponseAggregation {
    pub fn new(
        claim: &super::claim::ClaimState,
        response: ResponseEvaluation<'_>,
        deliveries: &[AcceptedResult],
        limits: Limits,
    ) -> Result<Self, ContractError> {
        Self::from_policy(
            claim.binding(),
            claim.acceptance(),
            response,
            deliveries,
            limits,
        )
    }
    fn from_policy(
        claim: Binding,
        policy: &AcceptancePolicy,
        response: ResponseEvaluation<'_>,
        deliveries: &[AcceptedResult],
        limits: Limits,
    ) -> Result<Self, ContractError> {
        same_content(claim, policy.claim())?;
        policy.within(limits)?;
        let policies = &policy.slots;
        if claim.ledger != response.binding().ledger {
            return Err(ContractError::WrongLedger);
        }
        if ClaimId(claim.object.0) != response.claim() {
            return Err(ContractError::WrongObject);
        }
        if deliveries.len() > limits.max_results {
            return Err(ContractError::Capacity);
        }
        let required = policy
            .declarations()
            .iter()
            .filter(|row| {
                row.target() == ObligationTarget::Delivery && row.mode() == ValidationMode::Required
            })
            .count();
        if deliveries.len() != required {
            return Err(ContractError::MissingEvidence);
        }
        for (position, result) in deliveries.iter().enumerate() {
            let declaration = policy.check_result(*result)?;
            let Target::Delivery {
                response: delivered,
            } = result.target()
            else {
                return Err(ContractError::InvalidTarget);
            };
            same_content(response.binding(), delivered)?;
            if result.receipt() != Some(response.receipt()) {
                return Err(ContractError::StaleReceipt);
            }
            if declaration.target() != ObligationTarget::Delivery
                || result.mode() != ValidationMode::Required
                || result.verdict() != VerdictValue::Pass
                || !result.is_terminal()
                || result.phase() != Phase::Delivery
            {
                return Err(ContractError::InvalidPolicy);
            }
            if deliveries
                .iter()
                .take(position)
                .any(|old| old.validation() == result.validation())
            {
                return Err(ContractError::InvalidManifest);
            }
        }
        let delivery = deliveries
            .iter()
            .min_by_key(|result| result.declaration_index())
            .ok_or(ContractError::MissingEvidence)?;
        let mut delivery_results = reserved(deliveries.len())?;
        delivery_results.extend_from_slice(deliveries);
        delivery_results.sort_unstable_by_key(|result| result.declaration_index());
        if response.manifest().len() > limits.max_slots {
            return Err(ContractError::Capacity);
        }
        for (position, binding) in response.manifest().iter().enumerate() {
            if !policies.iter().any(|policy| policy.slot == binding.slot)
                || response
                    .manifest()
                    .iter()
                    .take(position)
                    .any(|old| old.slot == binding.slot)
            {
                return Err(ContractError::InvalidManifest);
            }
        }
        let mut slots = reserved(policies.len())?;
        for policy in policies {
            let mut passes = reserved(policy.checks.len())?;
            passes.resize(policy.checks.len(), None);
            slots.push(ResponseSlot {
                artifact: response
                    .manifest()
                    .iter()
                    .find(|binding| binding.slot == policy.slot)
                    .map(|binding| binding.artifact),
                passes,
                terminal: None,
            });
        }
        Ok(Self {
            claim,
            response: response.binding(),
            delivery: DeliveryWitness {
                response: response.binding(),
                receipt: response.receipt(),
                validation: delivery.validation(),
                result: delivery.binding(),
            },
            policy: policy.copy()?,
            delivery_results,
            slots,
            limits,
            sequence: SessionSeq(0),
            outcome: ResponseOutcome::Evaluating,
        })
    }

    pub fn outcome(&self) -> ResponseOutcome {
        self.outcome
    }

    fn check_result(&self, result: &AcceptedResult) -> Result<ValidationMode, ContractError> {
        self.policy.check_result(*result)?;
        if result.receipt() != Some(self.delivery.receipt) {
            return Err(ContractError::StaleReceipt);
        }
        if result.ledger() != self.claim.ledger {
            return Err(ContractError::WrongLedger);
        }
        if result.claim() != ClaimId(self.claim.object.0) {
            return Err(ContractError::WrongObject);
        }
        let (response, slot, artifact) = match result.target() {
            Target::Artifact {
                response,
                slot,
                artifact,
            } => (response, Some(slot), Some(artifact)),
            Target::MissingSlot { response, slot } => (response, Some(slot), None),
            Target::Delivery { response } => {
                same_content(self.response, response)?;
                if result.mode() != ValidationMode::Required
                    || result.verdict() != VerdictValue::Pass
                {
                    return Err(ContractError::InvalidPolicy);
                }
                return Ok(ValidationMode::Required);
            }
            Target::Admission { .. } | Target::Increment { .. } => {
                return Err(ContractError::InvalidTarget);
            }
        };
        same_content(self.response, response)?;
        let (policy, state) = self
            .policy
            .slots
            .iter()
            .zip(&self.slots)
            .find(|(policy, _)| Some(policy.slot) == slot)
            .ok_or(ContractError::InvalidTarget)?;
        let check = policy
            .checks
            .iter()
            .find(|check| {
                check.validation == result.validation()
                    && check.declaration_index == result.declaration_index()
            })
            .ok_or(ContractError::InvalidTarget)?;
        if check.mode != result.mode() {
            return Err(ContractError::InvalidPolicy);
        }
        if matches!(state.terminal, Some((_, ArtifactOutcome::Passed)))
            && check.mode == ValidationMode::Required
            && result.is_terminal()
            && result.verdict() != VerdictValue::Pass
        {
            return Err(ContractError::InvalidTransition);
        }
        match artifact {
            Some(artifact)
                if artifact.ledger == self.claim.ledger
                    && state.artifact
                        == Some(ArtifactRef {
                            id: ArtifactId(artifact.object.0),
                            hash: artifact.content,
                        }) => {}
            None if state.artifact.is_none() => {}
            _ => return Err(ContractError::InvalidTarget),
        }
        Ok(policy.mode)
    }

    /// Inputs are facts accepted in this same staged mutation. Errors leave the
    /// index unchanged. Terminal response outcomes survive all late audit facts.
    pub fn apply(
        &mut self,
        sequence: SessionSeq,
        results: &[AcceptedResult],
    ) -> Result<ResponseUpdate, ContractError> {
        if sequence <= self.sequence {
            return Err(ContractError::InvalidCut);
        }
        if results.len() > self.limits.max_results {
            return Err(ContractError::Capacity);
        }
        let mut causes = reserved(
            results
                .len()
                .checked_add(self.policy.slots.len())
                .ok_or(ContractError::Capacity)?,
        )?;
        for (position, result) in results.iter().enumerate() {
            let mode = self.check_result(result)?;
            let current_key = key(result)?;
            for old in results.iter().take(position) {
                if key(old)? == current_key
                    && (old.verdict() != result.verdict()
                        || old.evidence() != result.evidence()
                        || old.programmatic_evidence() != result.programmatic_evidence()
                        || old.target() != result.target()
                        || old.is_terminal() != result.is_terminal())
                {
                    return Err(ContractError::ConflictingCause);
                }
            }
            if let Some(cause) = cause(result, mode)?
                && !causes.contains(&cause)
            {
                causes.push(cause);
            }
        }
        // Evaluation entry assesses absent declared slots, not streaming arrival.
        // Absence is a structural fact of this exact frozen manifest, not a run.
        if self.sequence == SessionSeq(0) {
            for (policy, state) in self.policy.slots.iter().zip(&self.slots) {
                if state.artifact.is_none() && policy.mode == ValidationMode::Required {
                    if causes.iter().any(|cause| {
                        cause.slot == Some(policy.slot)
                            && cause.artifact.is_none()
                            && cause.kind == BlockingKind::Incomplete
                            && cause.blocks_parent()
                    }) {
                        continue;
                    }
                    let missing = missing_cause(TestamentId(self.response.object.0), policy);
                    if let Some(old) = causes.iter().find(|old| old.key == missing.key) {
                        if old != &missing {
                            return Err(ContractError::ConflictingCause);
                        }
                    } else {
                        causes.push(missing);
                    }
                }
            }
        }
        causes.sort_unstable_by_key(|cause| cause.key);
        let mut witnesses = reserved(self.policy.slots.len())?;
        let mut artifacts = reserved(self.policy.slots.len())?;
        for (policy, state) in self.policy.slots.iter().zip(&self.slots) {
            let Some(artifact) = state.artifact else {
                continue;
            };
            let failure = state
                .terminal
                .and_then(|(_, outcome)| match outcome {
                    ArtifactOutcome::Blocked(cause) => Some(cause),
                    _ => None,
                })
                .or_else(|| {
                    causes
                        .iter()
                        .find(|cause| {
                            cause.slot == Some(policy.slot)
                                && cause.mode == ValidationMode::Required
                        })
                        .copied()
                });
            let mut checks = reserved(policy.checks.len())?;
            let mut complete = failure.is_none();
            if complete {
                for (check, accepted) in policy.checks.iter().zip(&state.passes) {
                    if check.mode == ValidationMode::Observe {
                        continue;
                    }
                    let passed = accepted.as_ref().or_else(|| {
                        results.iter().find(|result| {
                            result.validation() == check.validation
                                && result.is_terminal()
                                && result.verdict() == VerdictValue::Pass
                        })
                    });
                    let Some(passed) = passed else {
                        complete = false;
                        break;
                    };
                    checks.push(CheckWitness {
                        validation: passed.validation(),
                        declaration_index: passed.declaration_index(),
                        generation: passed.generation(),
                        attempt: passed.attempt(),
                        programmatic_evidence: passed
                            .programmatic_evidence()
                            .filter(|proof| Some(*proof) != passed.evidence()),
                        evidence: passed.evidence(),
                    });
                }
            }
            let outcome = match failure {
                Some(cause) => ArtifactOutcome::Blocked(cause),
                None if complete => ArtifactOutcome::Passed,
                None => ArtifactOutcome::Pending,
            };
            artifacts.push(ArtifactDecision {
                claim: self.claim,
                response: self.response,
                slot: policy.slot,
                artifact,
                outcome,
                sequence: state.terminal.map_or(sequence, |(sequence, _)| sequence),
            });
            // A failed response retains its cut. An independently successful
            // artifact can still cover a different slot of an open claim.
            if complete && policy.mode == ValidationMode::Required {
                witnesses.push(SlotWitness {
                    response: TestamentId(self.response.object.0),
                    slot: policy.slot,
                    artifact,
                    checks,
                });
            }
        }
        let mut outcome = self.outcome;
        if outcome == ResponseOutcome::Evaluating {
            // A response's own failure is never erased by another response.
            if let Some(cause) = causes.iter().find(|cause| cause.blocks_parent()) {
                outcome = ResponseOutcome::Blocked(TerminalCut {
                    sequence,
                    cause: *cause,
                });
            } else if self
                .policy
                .slots
                .iter()
                .filter(|policy| policy.mode == ValidationMode::Required)
                .all(|policy| witnesses.iter().any(|witness| witness.slot == policy.slot))
            {
                outcome = ResponseOutcome::Validated { sequence };
            }
        }
        let update_policy = self.policy.copy()?;
        let mut delivery_results = reserved(self.delivery_results.len())?;
        delivery_results.extend_from_slice(&self.delivery_results);
        // All fallible work completed. These bounded indexes were allocated when
        // the response entered evaluation; publication below does not allocate.
        for (policy, state) in self.policy.slots.iter().zip(&mut self.slots) {
            if state.terminal.is_none() {
                state.terminal = artifacts
                    .iter()
                    .find(|decision| {
                        decision.slot == policy.slot && decision.outcome != ArtifactOutcome::Pending
                    })
                    .map(|decision| (decision.sequence, decision.outcome));
            }
            for (check, passed) in policy.checks.iter().zip(&mut state.passes) {
                if passed.is_none()
                    && !matches!(state.terminal, Some((_, ArtifactOutcome::Blocked(_))))
                {
                    *passed = results
                        .iter()
                        .find(|result| {
                            result.validation() == check.validation
                                && result.is_terminal()
                                && result.verdict() == VerdictValue::Pass
                        })
                        .copied();
                }
            }
        }
        self.sequence = sequence;
        self.outcome = outcome;
        Ok(ResponseUpdate {
            claim: self.claim,
            policy: update_policy,
            sequence,
            delivery: self.delivery,
            delivery_results,
            witnesses,
            causes,
            outcome,
            artifacts,
        })
    }
}

/// Incremental coverage only: an update never walks other response manifests.
/// Successful evidence persists in this index until the owner archives the exact
/// acceptance record. A terminal cut or local completion is never reconsidered.
pub struct ClaimAggregation {
    claim: Binding,
    policy: AcceptancePolicy,
    coverage: Vec<Option<SlotWitness>>,
    delivery: Option<DeliveryWitness>,
    delivery_results: Vec<AcceptedResult>,
    limits: Limits,
    sequence: SessionSeq,
    outcome: AggregateOutcome,
    registry: EvaluationRegistry,
    accepted: Vec<NonArtifactWitness>,
    status: ClaimStatus,
}

/// A final accepted result bound to the registered exact target and receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonArtifactWitness {
    result: AcceptedResult,
    receipt: Option<ReceiptFence>,
    sequence: SessionSeq,
}
impl NonArtifactWitness {
    pub fn result(self) -> AcceptedResult {
        self.result
    }
    pub fn receipt(self) -> Option<ReceiptFence> {
        self.receipt
    }
    pub fn sequence(self) -> SessionSeq {
        self.sequence
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionOutcome {
    Pending,
    Passed,
    Blocked(TerminalCut),
}
#[derive(Debug)]
pub struct AdmissionDecision<'a> {
    binding: Binding,
    acceptance: &'a AcceptancePolicy,
    outcome: AdmissionOutcome,
    blocking_publication: Option<(SessionSeq, u32)>,
}
impl AdmissionDecision<'_> {
    pub fn binding(&self) -> Binding {
        self.binding
    }
    pub fn acceptance(&self) -> &AcceptancePolicy {
        self.acceptance
    }
    pub fn outcome(&self) -> AdmissionOutcome {
        self.outcome
    }
    /// Exact publication provenance for the selected native blocking result.
    /// Older ClaimAggregation witnesses contain no event ordinal and return None.
    pub fn blocking_publication(&self) -> Option<(SessionSeq, u32)> {
        self.blocking_publication
    }
}
impl ClaimAggregation {
    fn admission_outcome(&self) -> AdmissionOutcome {
        if let Some(witness) = self
            .accepted
            .iter()
            .filter(|w| {
                matches!(w.result.target(), Target::Admission { .. })
                    && w.result.mode() == ValidationMode::Required
                    && w.result.verdict() != VerdictValue::Pass
            })
            .min_by_key(|w| (w.sequence, key(&w.result).ok()))
            && let Ok(Some(cause)) = cause(&witness.result, ValidationMode::Required)
        {
            return AdmissionOutcome::Blocked(TerminalCut {
                sequence: witness.sequence,
                cause,
            });
        }
        if self.obligations_complete(ObligationTarget::Admission, &self.accepted, true) {
            AdmissionOutcome::Passed
        } else {
            AdmissionOutcome::Pending
        }
    }
}

/// A borrowed checked index decision, not a freely authored terminal status.
pub struct ClaimDecision<'a> {
    binding: Binding,
    sequence: SessionSeq,
    outcome: AggregateOutcome,
    delivery: Option<DeliveryWitness>,
    acceptance: &'a AcceptancePolicy,
    delivery_results: &'a [AcceptedResult],
    nonartifact: &'a [NonArtifactWitness],
    increments_ready: bool,
    coverage: CoverageView<'a>,
}

#[derive(Debug, Clone, Copy)]
pub struct SlotWitnessRef<'a> {
    response: TestamentId,
    slot: u32,
    artifact: ArtifactRef,
    checks: &'a [CheckWitness],
}
impl SlotWitnessRef<'_> {
    pub fn response(&self) -> TestamentId {
        self.response
    }
    pub fn slot(&self) -> u32 {
        self.slot
    }
    pub fn artifact(&self) -> ArtifactRef {
        self.artifact
    }
    pub fn checks(&self) -> &[CheckWitness] {
        self.checks
    }
}

enum CoverageView<'a> {
    Owned(&'a [Option<SlotWitness>]),
    Projected(&'a [projection::ProjectedWitness], &'a [CheckWitness]),
}
impl CoverageView<'_> {
    fn iter(&self) -> impl Iterator<Item = SlotWitnessRef<'_>> {
        let (owned, projected, checks) = match self {
            Self::Owned(rows) => (*rows, &[][..], &[][..]),
            Self::Projected(rows, checks) => (&[][..], *rows, *checks),
        };
        owned
            .iter()
            .filter_map(Option::as_ref)
            .map(|row| SlotWitnessRef {
                response: row.response,
                slot: row.slot,
                artifact: row.artifact,
                checks: &row.checks,
            })
            .chain(projected.iter().scan(checks, |remaining, row| {
                // The private projection validates this flat layout before issuing
                // a decision, and exposes no mutation of either buffer afterward.
                let (checks, tail) = remaining.split_at_checked(row.checks)?;
                *remaining = tail;
                Some(SlotWitnessRef {
                    response: row.response,
                    slot: row.slot,
                    artifact: row.artifact,
                    checks,
                })
            }))
    }
}

impl ClaimDecision<'_> {
    pub fn binding(&self) -> Binding {
        self.binding
    }
    pub fn sequence(&self) -> SessionSeq {
        self.sequence
    }
    pub fn outcome(&self) -> AggregateOutcome {
        self.outcome
    }
    pub fn delivery(&self) -> Option<DeliveryWitness> {
        self.delivery
    }
    pub fn witnesses(&self) -> impl Iterator<Item = SlotWitnessRef<'_>> {
        self.coverage.iter()
    }
    pub fn acceptance(&self) -> &AcceptancePolicy {
        self.acceptance
    }
    pub fn delivery_results(&self) -> &[AcceptedResult] {
        self.delivery_results
    }
    pub fn nonartifact_witnesses(&self) -> &[NonArtifactWitness] {
        self.nonartifact
    }
    pub fn increments_ready(&self) -> bool {
        self.increments_ready
    }
}

impl ClaimAggregation {
    pub fn new(claim: &super::claim::ClaimState, limits: Limits) -> Result<Self, ContractError> {
        Self::from_policy(claim.binding(), claim.status(), claim.acceptance(), limits)
    }
    fn from_policy(
        claim: Binding,
        status: ClaimStatus,
        policy: &AcceptancePolicy,
        limits: Limits,
    ) -> Result<Self, ContractError> {
        same_content(claim, policy.claim())?;
        policy.within(limits)?;
        let mut coverage = reserved(policy.slots.len())?;
        coverage.resize_with(policy.slots.len(), || None);
        Ok(Self {
            claim,
            policy: policy.copy()?,
            coverage,
            delivery: None,
            delivery_results: Vec::new(),
            limits,
            sequence: SessionSeq(0),
            outcome: AggregateOutcome::Pending,
            registry: EvaluationRegistry::from_policy(policy, limits)?,
            accepted: reserved(limits.max_results)?,
            status,
        })
    }
    pub fn outcome(&self) -> AggregateOutcome {
        self.outcome
    }
    pub fn decision(&self) -> ClaimDecision<'_> {
        ClaimDecision {
            binding: self.claim,
            sequence: self.sequence,
            outcome: self.outcome,
            delivery: self.delivery,
            acceptance: &self.policy,
            delivery_results: &self.delivery_results,
            nonartifact: &self.accepted,
            increments_ready: self.increments_ready(),
            coverage: CoverageView::Owned(&self.coverage),
        }
    }
    /// Rebind only to the actual private claim state after response lifecycle
    /// facts advanced its revision. Historical cuts and witnesses are unchanged.
    pub fn rebind(&mut self, current: &super::claim::ClaimState) -> Result<(), ContractError> {
        same_content(self.claim, current.binding())?;
        if current.binding().revision < self.claim.revision {
            return Err(ContractError::StaleRevision);
        }
        if self.delivery.is_some_and(|delivery| {
            current.receipt().map(|receipt| receipt.fence) != Some(delivery.receipt)
        }) {
            // Receipt adoption requires a new pending coverage index. Old
            // witnesses remain history and are never silently adopted or erased.
            return Err(ContractError::StaleReceipt);
        }
        if self.registry.rows().iter().any(|row| {
            row.receipt()
                .is_some_and(|fence| current.receipt().map(|receipt| receipt.fence) != Some(fence))
        }) {
            return Err(ContractError::StaleReceipt);
        }
        if current.acceptance() != &self.policy {
            return Err(ContractError::InvalidPolicy);
        }
        self.status = current.status();
        self.claim = current.binding();
        Ok(())
    }
    pub fn delivery(&self) -> Option<DeliveryWitness> {
        self.delivery
    }
    pub fn witnesses(&self) -> impl Iterator<Item = &SlotWitness> {
        self.coverage.iter().filter_map(Option::as_ref)
    }

    pub fn admission(&self) -> AdmissionDecision<'_> {
        AdmissionDecision {
            binding: self.claim,
            acceptance: &self.policy,
            outcome: self.admission_outcome(),
            blocking_publication: None,
        }
    }
    pub fn registry(&self) -> &EvaluationRegistry {
        &self.registry
    }
    pub fn register(
        &mut self,
        evaluation: &super::validation::Evaluation<'_>,
    ) -> Result<(), ContractError> {
        if self.outcome != AggregateOutcome::Pending {
            return Err(ContractError::InvalidTransition);
        }
        self.registry.register(evaluation)
    }
    pub fn materialize<'a>(
        &mut self,
        principal: super::Principal,
        declaration: &'a super::validation::Declaration,
        materialization: super::validation::Materialization<'_>,
    ) -> Result<super::validation::Evaluation<'a>, ContractError> {
        if self.outcome != AggregateOutcome::Pending {
            return Err(ContractError::InvalidTransition);
        }
        self.registry
            .materialize(principal, declaration, materialization)
    }
    pub fn seal_increment_targets(&mut self) -> Result<(), ContractError> {
        if self.outcome != AggregateOutcome::Pending {
            return Err(ContractError::InvalidTransition);
        }
        self.registry.seal_increment_targets();
        Ok(())
    }
    /// Final audit sealing is independent of business acceptance or failure.
    pub fn seal_targets(&mut self) -> Result<SealedTargets<'_>, ContractError> {
        Ok(self.registry.seal_targets())
    }
    pub fn increments_ready(&self) -> bool {
        self.obligations_complete(ObligationTarget::Increment, &self.accepted, false)
    }
    fn obligations_complete(
        &self,
        target: ObligationTarget,
        accepted: &[NonArtifactWitness],
        require_pass: bool,
    ) -> bool {
        let obligations = self
            .policy
            .declarations()
            .iter()
            .filter(|row| row.mode() == ValidationMode::Required && row.target() == target);
        for declaration in obligations {
            if target == ObligationTarget::Increment && !self.registry.increment_targets_sealed() {
                return false;
            }
            let mut found = false;
            for row in self
                .registry
                .rows()
                .iter()
                .filter(|row| row.binding().object == declaration.binding().object)
            {
                found = true;
                if !accepted.iter().any(|witness| {
                    row.matches(witness.result)
                        && (!require_pass || witness.result.verdict() == VerdictValue::Pass)
                }) {
                    return false;
                }
            }
            if !found && target != ObligationTarget::Increment {
                return false;
            }
        }
        true
    }

    pub fn apply(
        &mut self,
        sequence: SessionSeq,
        updates: &[ResponseUpdate],
    ) -> Result<AggregateOutcome, ContractError> {
        self.apply_acceptance(sequence, updates, &[])
    }

    /// Apply the whole mutation's response and non-artifact facts together. All
    /// validation/allocation finishes before replacing either coverage index.
    pub fn apply_acceptance(
        &mut self,
        sequence: SessionSeq,
        updates: &[ResponseUpdate],
        results: &[AcceptedResult],
    ) -> Result<AggregateOutcome, ContractError> {
        if sequence <= self.sequence {
            return Err(ContractError::InvalidCut);
        }
        if updates.len() > self.limits.max_updates {
            return Err(ContractError::Capacity);
        }
        if results.len() > self.limits.max_results {
            return Err(ContractError::Capacity);
        }
        for (position, result) in results.iter().enumerate() {
            if !matches!(
                result.target(),
                Target::Admission { .. } | Target::Increment { .. }
            ) {
                return Err(ContractError::InvalidTarget);
            }
            self.registry.check_result(*result)?;
            for old in results
                .iter()
                .take(position)
                .chain(self.accepted.iter().map(|w| &w.result))
            {
                if key(old)? == key(result)? && old != result {
                    return Err(ContractError::ConflictingCause);
                }
                if old.is_terminal()
                    && result.is_terminal()
                    && old.validation() == result.validation()
                    && old.target() == result.target()
                    && old.generation() == result.generation()
                    && old != result
                {
                    return Err(ContractError::ConflictingCause);
                }
            }
        }
        let mut accepted = reserved(self.limits.max_results)?;
        accepted.extend_from_slice(&self.accepted);
        for result in results.iter().filter(|result| result.is_terminal()) {
            if accepted.iter().any(|old| old.result == *result) {
                continue;
            }
            if accepted.len() >= self.limits.max_results {
                return Err(ContractError::Capacity);
            }
            let row = self
                .registry
                .rows()
                .iter()
                .find(|row| row.matches(*result))
                .ok_or(ContractError::StaleEvaluation)?;
            accepted.push(NonArtifactWitness {
                result: *result,
                receipt: row.receipt(),
                sequence,
            });
        }
        let mut nonartifact_causes = reserved(accepted.len())?;
        for witness in &accepted {
            if let Some(cause) = cause(&witness.result, ValidationMode::Required)? {
                nonartifact_causes.push(cause);
            }
        }
        for update in updates {
            same_content(self.claim, update.claim)?;
            if update.policy != self.policy {
                return Err(ContractError::InvalidPolicy);
            }
            if update.sequence != sequence {
                return Err(ContractError::InvalidCut);
            }
            if update.witnesses.len() > self.limits.max_slots
                || update.artifacts.len() > self.limits.max_slots
                || update.causes.len()
                    > self
                        .limits
                        .max_results
                        .checked_add(self.limits.max_slots)
                        .ok_or(ContractError::Capacity)?
            {
                return Err(ContractError::Capacity);
            }
            for witness in &update.witnesses {
                if witness.checks.len() > self.limits.max_checks {
                    return Err(ContractError::Capacity);
                }
                let policy = self
                    .policy
                    .slots
                    .iter()
                    .find(|policy| policy.slot == witness.slot)
                    .ok_or(ContractError::InvalidTarget)?;
                if policy.mode != ValidationMode::Required
                    || witness.checks.len()
                        != policy
                            .checks
                            .iter()
                            .filter(|check| check.mode == ValidationMode::Required)
                            .count()
                    || !policy
                        .checks
                        .iter()
                        .filter(|check| check.mode == ValidationMode::Required)
                        .all(|check| {
                            witness.checks.iter().any(|actual| {
                                actual.validation == check.validation
                                    && actual.declaration_index == check.declaration_index
                            })
                        })
                {
                    return Err(ContractError::InvalidPolicy);
                }
            }
        }
        let causes = || {
            updates
                .iter()
                .flat_map(|update| &update.causes)
                .chain(nonartifact_causes.iter())
        };
        for (position, cause) in causes().enumerate() {
            if causes()
                .take(position)
                .any(|old| old.key == cause.key && old != cause)
            {
                return Err(ContractError::ConflictingCause);
            }
        }
        if self.outcome != AggregateOutcome::Pending {
            return Ok(self.outcome);
        }
        let mut changed = reserved(self.policy.slots.len())?;
        for (policy, current) in self.policy.slots.iter().zip(&self.coverage) {
            let selected = if current.is_none() && policy.mode == ValidationMode::Required {
                updates
                    .iter()
                    .flat_map(|update| &update.witnesses)
                    .filter(|witness| witness.slot == policy.slot)
                    .min_by_key(|witness| {
                        (witness.response, witness.artifact.id, witness.artifact.hash)
                    })
            } else {
                None
            };
            changed.push(selected.map(SlotWitness::copy).transpose()?);
        }
        let delivery = self.delivery.or_else(|| {
            updates
                .iter()
                .map(|update| update.delivery)
                .min_by_key(|delivery| delivery.response.object)
        });
        let replacement_delivery_results = if self.delivery.is_none() {
            let selected = updates
                .iter()
                .min_by_key(|update| update.delivery.response.object);
            let mut proofs = reserved(selected.map_or(0, |update| update.delivery_results.len()))?;
            if let Some(selected) = selected {
                proofs.extend_from_slice(&selected.delivery_results);
            }
            Some(proofs)
        } else {
            None
        };
        let covered = |slot| {
            self.policy
                .slots
                .iter()
                .zip(self.coverage.iter().zip(&changed))
                .any(|(policy, (old, new))| policy.slot == slot && (old.is_some() || new.is_some()))
        };
        let complete = self.status == ClaimStatus::Validating
            && self.obligations_complete(ObligationTarget::Admission, &accepted, true)
            && self.obligations_complete(ObligationTarget::Increment, &accepted, true)
            && delivery.is_some()
            && self
                .policy
                .slots
                .iter()
                .filter(|policy| policy.mode == ValidationMode::Required)
                .all(|policy| covered(policy.slot));
        let outcome = if complete {
            AggregateOutcome::LocalComplete { sequence }
        } else {
            causes()
                .filter(|cause| {
                    self.status == ClaimStatus::Validating
                        && cause.blocks_parent()
                        && match cause.key.target {
                            CauseTarget::Admission | CauseTarget::Increment { .. } => true,
                            CauseTarget::Response(_) => match cause.slot {
                                Some(slot) => !covered(slot),
                                None => delivery.is_none(),
                            },
                        }
                })
                .min_by_key(|cause| cause.key)
                .map_or(AggregateOutcome::Pending, |cause| {
                    AggregateOutcome::Blocked(TerminalCut {
                        sequence,
                        cause: *cause,
                    })
                })
        };
        for (target, replacement) in self.coverage.iter_mut().zip(changed) {
            if replacement.is_some() {
                *target = replacement;
            }
        }
        self.accepted = accepted;
        if let Some(proofs) = replacement_delivery_results {
            self.delivery_results = proofs;
        }
        self.delivery = delivery;
        self.sequence = sequence;
        self.outcome = outcome;
        Ok(outcome)
    }
}

#[cfg(test)]
#[path = "aggregation_tests.rs"]
mod tests;
