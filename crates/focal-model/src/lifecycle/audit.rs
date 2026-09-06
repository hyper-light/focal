//! Bounded audit closure, separate from business acceptance. Result evidence has
//! explicit roles and cannot be passed to response attachment or slot qualification.
use super::claim::ClaimState;
use super::validation::{
    AcceptedResult, AuthorityFence, Evaluation, Phase, State, Suppression, Target,
};
use super::{Binding, ContractError, Principal};
use crate::{ArtifactRef, ClaimId, ParticipantId, SessionSeq, ValidationId};

/// A real evaluator result artifact is already generated in the accepted result
/// transaction. This role has no receipt, attachment or validation transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultArtifact {
    result: AcceptedResult,
    artifact: ArtifactRef,
    producer: ParticipantId,
}

impl ResultArtifact {
    pub fn from_result(result: AcceptedResult) -> Result<Self, ContractError> {
        let artifact = result.evidence().ok_or(ContractError::MissingEvidence)?;
        let producer = result.reporter().ok_or(ContractError::WrongActor)?;
        if matches!(result.phase(), Phase::Delivery | Phase::MissingTarget) {
            return Err(ContractError::InvalidTransition);
        }
        Ok(Self {
            result,
            artifact,
            producer,
        })
    }
    pub fn reference(&self) -> ArtifactRef {
        self.artifact
    }
    pub fn producer(&self) -> ParticipantId {
        self.producer
    }
    pub fn result(&self) -> AcceptedResult {
        self.result
    }
    pub fn result_ref(&self) -> &AcceptedResult {
        &self.result
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationKey {
    pub validation: ValidationId,
    pub target: Target,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct OrderKey {
    target: super::aggregation::CauseTarget,
    declaration: u32,
    generation: u64,
    validation: ValidationId,
}

fn order(target: Target, declaration: u32, generation: u64, validation: ValidationId) -> OrderKey {
    let target = match target {
        Target::Artifact { response, .. }
        | Target::MissingSlot { response, .. }
        | Target::Delivery { response } => {
            super::aggregation::CauseTarget::Response(crate::TestamentId(response.object.0))
        }
        Target::Admission { .. } => super::aggregation::CauseTarget::Admission,
        Target::Increment { artifact, .. } => super::aggregation::CauseTarget::Increment {
            artifact: crate::ArtifactId(artifact.object.0),
            content: artifact.content,
        },
    };
    OrderKey {
        target,
        declaration,
        generation,
        validation,
    }
}
fn result_order(result: AcceptedResult) -> (OrderKey, Option<u32>, u8) {
    let phase = match result.phase() {
        Phase::Programmatic => 0,
        Phase::Quality => 1,
        Phase::Delivery => 2,
        Phase::MissingTarget => 3,
    };
    (
        order(
            result.target(),
            result.declaration_index(),
            result.generation(),
            result.validation(),
        ),
        result.attempt(),
        phase,
    )
}
fn result_key(result: AcceptedResult) -> EvaluationKey {
    EvaluationKey {
        validation: result.validation(),
        target: result.target(),
        generation: result.generation(),
    }
}

/// A suppressed Ready member is represented as such; it is not a fake verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditMember {
    definition: super::validation::DefinitionStamp,
    key: EvaluationKey,
    order: OrderKey,
    binding: Binding,
    receipt: Option<crate::ReceiptFence>,
    begun: bool,
    state: State,
    suppression: Option<Suppression>,
    fence: Option<AuthorityFence>,
    last_result: Option<AcceptedResult>,
    sealed: bool,
}
impl AuditMember {
    fn from_evaluation(evaluation: &Evaluation<'_>) -> Self {
        Self {
            definition: evaluation.definition_stamp(),
            key: EvaluationKey {
                validation: evaluation.validation(),
                target: evaluation.target(),
                generation: evaluation.generation(),
            },
            order: order(
                evaluation.target(),
                evaluation.declaration_index(),
                evaluation.generation(),
                evaluation.validation(),
            ),
            binding: evaluation.binding(),
            receipt: evaluation.receipt(),
            begun: evaluation.has_begun(),
            state: evaluation.state(),
            suppression: evaluation.suppression(),
            fence: evaluation.fence(),
            last_result: evaluation.last_result(),
            sealed: evaluation.sealed().is_some(),
        }
    }
    pub fn key(&self) -> EvaluationKey {
        self.key
    }
    pub fn state(&self) -> State {
        self.state
    }
    pub fn suppression(&self) -> Option<Suppression> {
        self.suppression
    }
    pub fn fence(&self) -> Option<AuthorityFence> {
        self.fence
    }
    pub fn complete(&self) -> bool {
        self.state.is_terminal()
            || self.fence.is_some()
            || (!self.begun && self.sealed && self.suppression.is_some())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub evaluations: usize,
    pub results: usize,
}

/// The owner supplies the complete evaluation index and its accepted history at
/// the sealing prefix. Constructor bounds are checked before allocation. Capacity
/// for every admitted attempt is reserved before accepting late completion.
#[derive(Debug)]
pub struct AuditCohort {
    claim: Binding,
    issuer: ParticipantId,
    sequence: SessionSeq,
    members: Vec<AuditMember>,
    results: Vec<AcceptedResult>,
    result_capacity: usize,
}

impl AuditCohort {
    pub fn seal(
        claim: &ClaimState,
        targets: &super::aggregation::SealedTargets<'_>,
        sequence: SessionSeq,
        evaluations: &[Evaluation<'_>],
        history: &[AcceptedResult],
        limits: Limits,
    ) -> Result<Self, ContractError> {
        if targets.policy() != claim.acceptance() {
            return Err(ContractError::InvalidPolicy);
        }
        if !claim.local_complete() && !claim.status().is_terminal() {
            return Err(ContractError::InvalidTransition);
        }
        if sequence.0 == 0 || claim.local_sealed_at() != Some(sequence) {
            return Err(ContractError::InvalidCut);
        }
        if evaluations.len() > limits.evaluations || history.len() > limits.results {
            return Err(ContractError::Capacity);
        }
        if targets.rows().len() != evaluations.len() {
            return Err(ContractError::InvalidManifest);
        }
        let mut capacity = 0usize;
        for evaluation in evaluations {
            let registered = targets
                .rows()
                .iter()
                .find(|row| {
                    row.binding().object == evaluation.binding().object
                        && row.target() == evaluation.target()
                        && row.generation() == evaluation.generation()
                })
                .ok_or(ContractError::StaleEvaluation)?;
            if registered.definition_stamp() != evaluation.definition_stamp()
                || registered.binding().content != evaluation.binding().content
                || registered.binding().revision > evaluation.binding().revision
                || registered.receipt() != evaluation.receipt()
                || registered.declaration_index() != evaluation.declaration_index()
                || registered.mode() != evaluation.mode()
            {
                return Err(ContractError::StaleEvaluation);
            }
            if evaluation.ledger() != claim.binding().ledger {
                return Err(ContractError::WrongLedger);
            }
            if evaluation.claim().0 != claim.binding().object.0 {
                return Err(ContractError::WrongObject);
            }
            if !evaluation.state().is_terminal() && evaluation.sealed().is_none() {
                return Err(ContractError::InvalidTransition);
            }
            let bound = usize::try_from(evaluation.attempt_bound().max(1))
                .map_err(|_| ContractError::Capacity)?;
            capacity = capacity.checked_add(bound).ok_or(ContractError::Capacity)?;
            if capacity > limits.results {
                return Err(ContractError::Capacity);
            }
        }
        if history.len() > capacity {
            return Err(ContractError::Capacity);
        }
        let mut members = Vec::new();
        members
            .try_reserve_exact(evaluations.len())
            .map_err(|_| ContractError::Capacity)?;
        for evaluation in evaluations {
            members.push(AuditMember::from_evaluation(evaluation));
        }
        members.sort_unstable_by_key(|member| member.order);
        for pair in members.windows(2) {
            if let [a, b] = pair
                && a.order == b.order
            {
                return Err(ContractError::InvalidManifest);
            }
        }
        let mut results = Vec::new();
        results
            .try_reserve_exact(capacity)
            .map_err(|_| ContractError::Capacity)?;
        for result in history {
            let member = members
                .iter()
                .find(|member| member.key == result_key(*result))
                .ok_or(ContractError::InvalidTarget)?;
            Self::check_result(claim.binding(), member, *result)?;
            results.push(*result);
        }
        results.sort_unstable_by_key(|result| result_order(*result));
        for member in &members {
            let mut last = None;
            let mut count = 0u32;
            for result in results
                .iter()
                .filter(|result| result_key(**result) == member.key)
            {
                if last.is_some_and(|prior: AcceptedResult| prior.is_terminal()) {
                    return Err(ContractError::InvalidManifest);
                }
                match result.attempt() {
                    Some(attempt) if attempt == count => {}
                    None if count == 0
                        && matches!(result.phase(), Phase::Delivery | Phase::MissingTarget) => {}
                    _ => return Err(ContractError::InvalidManifest),
                }
                count = count.checked_add(1).ok_or(ContractError::Capacity)?;
                last = Some(*result);
            }
            if last != member.last_result {
                return Err(ContractError::InvalidManifest);
            }
        }
        Ok(Self {
            claim: claim.binding(),
            issuer: claim.issuer(),
            sequence,
            members,
            results,
            result_capacity: capacity,
        })
    }

    fn check_result(
        claim: Binding,
        member: &AuditMember,
        result: AcceptedResult,
    ) -> Result<(), ContractError> {
        if result.ledger() != claim.ledger {
            return Err(ContractError::WrongLedger);
        }
        if result.claim().0 != claim.object.0 || member.key != result_key(result) {
            return Err(ContractError::InvalidTarget);
        }
        if result.definition_stamp() != member.definition
            || result.binding().content != member.binding.content
        {
            return Err(ContractError::ContentConflict);
        }
        if result.receipt() != member.receipt {
            return Err(ContractError::StaleReceipt);
        }
        if result.binding().revision > member.binding.revision {
            return Err(ContractError::StaleRevision);
        }
        Ok(())
    }

    /// Admit a checked late phase/attempt or a recorded authority/deadline fence.
    /// A sealed unbegun member cannot become begun by using an old context.
    pub fn record(&mut self, evaluation: &Evaluation<'_>) -> Result<(), ContractError> {
        let next = AuditMember::from_evaluation(evaluation);
        if evaluation.ledger() != self.claim.ledger {
            return Err(ContractError::WrongLedger);
        }
        if evaluation.claim().0 != self.claim.object.0 {
            return Err(ContractError::WrongObject);
        }
        let index = self
            .members
            .binary_search_by_key(&next.order, |member| member.order)
            .map_err(|_| ContractError::InvalidTarget)?;
        let previous = self
            .members
            .get(index)
            .ok_or(ContractError::InvalidTarget)?;
        if previous.definition != next.definition
            || previous.key != next.key
            || previous.binding.content != next.binding.content
            || previous.receipt != next.receipt
        {
            return Err(ContractError::InvalidTarget);
        }
        if next.binding.revision <= previous.binding.revision {
            return Err(ContractError::StaleRevision);
        }
        if previous.complete() || !previous.begun || !next.begun || !next.sealed {
            return Err(ContractError::InvalidTransition);
        }
        let new_result = if next.last_result != previous.last_result {
            next.last_result
        } else {
            None
        };
        if let Some(result) = new_result {
            Self::check_result(self.claim, &next, result)?;
            let expected_attempt = match previous.last_result {
                Some(previous) => previous
                    .attempt()
                    .and_then(|attempt| attempt.checked_add(1))
                    .ok_or(ContractError::InvalidTransition)?,
                None => 0,
            };
            if result.attempt() != Some(expected_attempt) {
                return Err(ContractError::InvalidManifest);
            }
            if self.results.len() >= self.result_capacity {
                return Err(ContractError::Capacity);
            }
        } else if next.fence.is_none() {
            return Err(ContractError::InvalidTransition);
        }
        // Completion storage was reserved at sealing. No allocation follows.
        let row = self
            .members
            .get_mut(index)
            .ok_or(ContractError::InvalidTarget)?;
        *row = next;
        if let Some(result) = new_result {
            self.results.push(result);
        }
        Ok(())
    }

    pub fn complete(&self) -> bool {
        self.members.iter().all(AuditMember::complete)
    }
    pub fn members(&self) -> &[AuditMember] {
        &self.members
    }
    pub fn result_count(&self) -> usize {
        self.results.len()
    }
    pub fn sealed_at(&self) -> SessionSeq {
        self.sequence
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultTestamentState {
    Generated,
    Posted,
}

/// A claimant audit bundle ends at Posted. It has no response manifest and cannot
/// satisfy work slots or create recursive acceptance obligations.
#[derive(Debug)]
pub struct ResultTestament {
    binding: Binding,
    cohort: AuditCohort,
    state: ResultTestamentState,
}

impl ResultTestament {
    pub fn generate(
        binding: Binding,
        principal: Principal,
        mut cohort: AuditCohort,
    ) -> Result<Self, ContractError> {
        principal.require_actor(cohort.issuer)?;
        if binding.object.is_zero() {
            return Err(ContractError::InvalidTarget);
        }
        if binding.ledger != cohort.claim.ledger {
            return Err(ContractError::WrongLedger);
        }
        if !cohort.complete() {
            return Err(ContractError::InvalidTransition);
        }
        cohort
            .results
            .sort_unstable_by_key(|result| result_order(*result));
        Ok(Self {
            binding,
            cohort,
            state: ResultTestamentState::Generated,
        })
    }
    pub fn post(&mut self, principal: Principal, expected: &Binding) -> Result<(), ContractError> {
        self.binding.check(expected)?;
        principal.require_actor(self.cohort.issuer)?;
        if self.state != ResultTestamentState::Generated {
            return Err(ContractError::InvalidTransition);
        }
        let next = self.binding.next()?;
        self.binding = next;
        self.state = ResultTestamentState::Posted;
        Ok(())
    }
    pub fn binding(&self) -> Binding {
        self.binding
    }
    pub fn state(&self) -> ResultTestamentState {
        self.state
    }
    pub fn claim(&self) -> ClaimId {
        ClaimId(self.cohort.claim.object.0)
    }
    pub fn members(&self) -> &[AuditMember] {
        &self.cohort.members
    }
    pub fn results(&self) -> &[AcceptedResult] {
        &self.cohort.results
    }
}

#[cfg(test)]
#[path = "audit_tests.rs"]
mod tests;
