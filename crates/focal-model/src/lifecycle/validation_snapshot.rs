//! Versioned scalar restoration views, not the frozen V1 application or wire
//! format. These checks establish intrinsic row coherence against an actual
//! retained declaration; they do not authorize a report or certify a checkpoint.
//!
//! The native importer must additionally verify the declaration's full content,
//! complete registry membership, original target/receipt/generation, artifact
//! metadata and local custody, and the complete ordered result/event history.
//! In particular, a final quality cursor does not identify which earlier
//! programmatic attempt passed. Original publication positions are owned by that
//! history, not inferred from these rows. No current clock, parent status,
//! receipt holder, policy grant or participant authorization is replayed here.
use super::*;

#[cfg(test)]
#[path = "validation_snapshot_tests.rs"]
mod tests;

/// Complete accepted-result values. The process-local definition stamp is
/// intentionally absent: restoration derives it from the retained declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedResultSnapshotV1 {
    pub binding: Binding,
    pub ledger: LedgerId,
    pub claim: ClaimId,
    pub target: Target,
    pub validation: ValidationId,
    pub declaration_index: u32,
    pub mode: ValidationMode,
    pub verdict: VerdictValue,
    pub phase: Phase,
    pub attempt: Option<u32>,
    pub generation: u64,
    pub receipt: Option<ReceiptFence>,
    pub evidence: Option<ArtifactRef>,
    pub programmatic_evidence: Option<ArtifactRef>,
    pub reporter: Option<ParticipantId>,
    pub resulting_state: State,
}

/// Every retained evaluation field, including the handler-local retry cursor.
/// `handler` has an explicit scalar width; conversion to the host index is
/// checked. This view carries no caller-supplied semantic stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationSnapshotV1 {
    pub binding: Binding,
    pub target: Target,
    pub generation: u64,
    pub receipt: Option<ReceiptFence>,
    pub state: State,
    pub phase: Phase,
    pub handler: u64,
    pub handler_attempt: u32,
    pub attempt: u32,
    pub begun: bool,
    pub suppression: Option<Suppression>,
    pub sealed: Option<ContentHash>,
    pub fence: Option<AuthorityFence>,
    pub programmatic_evidence: Option<ArtifactRef>,
    pub last_result: Option<AcceptedResultSnapshotV1>,
}

impl AcceptedResult {
    pub fn snapshot_v1(self) -> AcceptedResultSnapshotV1 {
        AcceptedResultSnapshotV1 {
            binding: self.binding,
            ledger: self.ledger,
            claim: self.claim,
            target: self.target,
            validation: self.validation,
            declaration_index: self.declaration_index,
            mode: self.mode,
            verdict: self.verdict,
            phase: self.phase,
            attempt: self.attempt,
            generation: self.generation,
            receipt: self.receipt,
            evidence: self.evidence,
            programmatic_evidence: self.programmatic_evidence,
            reporter: self.reporter,
            resulting_state: self.resulting_state,
        }
    }

    /// Conservative work units for scalar checks, the bounded handler scan and
    /// the materialization target-name comparison. No allocation is required.
    pub fn hydration_visits(declaration: &Declaration) -> Result<usize, ContractError> {
        quote(declaration, 96)
    }

    /// Restore recorded facts, never report authority. The enclosing importer
    /// must authenticate its immutable record/checkpoint chain, validate the
    /// actual declaration content and registry membership, and prove original
    /// target/receipt, artifact/custody and complete result/event linkage. These
    /// intrinsic checks cannot certify those historical cross-row facts.
    pub fn hydrate_v1(
        declaration: &Declaration,
        snapshot: AcceptedResultSnapshotV1,
        max_visits: usize,
    ) -> Result<Self, ContractError> {
        if Self::hydration_visits(declaration)? > max_visits {
            return Err(ContractError::Capacity);
        }
        let shape = Shape::inspect(declaration, None)?;
        accepted(declaration, snapshot, &shape)
    }
}

impl EvaluationState {
    pub fn snapshot_v1(self) -> Result<EvaluationSnapshotV1, ContractError> {
        Ok(EvaluationSnapshotV1 {
            binding: self.binding,
            target: self.target,
            generation: self.generation,
            receipt: self.receipt,
            state: self.state,
            phase: self.phase,
            handler: u64::try_from(self.handler).map_err(|_| ContractError::Capacity)?,
            handler_attempt: self.handler_attempt,
            attempt: self.attempt,
            begun: self.begun,
            suppression: self.suppression,
            sealed: self.sealed,
            fence: self.fence,
            programmatic_evidence: self.programmatic_evidence,
            last_result: self.last_result.map(AcceptedResult::snapshot_v1),
        })
    }

    /// Includes the nested last-result check without a second handler scan.
    pub fn hydration_visits(declaration: &Declaration) -> Result<usize, ContractError> {
        quote(declaration, 192)
    }

    /// Restore the complete scalar row against its actual retained declaration.
    /// The importer must independently authenticate the record/checkpoint chain
    /// and check membership, immutable target/receipt/evidence and complete
    /// result/event history. This method neither reauthorizes old reports nor
    /// proves that a supplied, intrinsically consistent history really occurred.
    pub fn hydrate_v1(
        declaration: &Declaration,
        snapshot: EvaluationSnapshotV1,
        max_visits: usize,
    ) -> Result<Self, ContractError> {
        // Refuse before inspecting attacker-selected cursors or scanning policy.
        if Self::hydration_visits(declaration)? > max_visits {
            return Err(ContractError::Capacity);
        }
        let handler = usize::try_from(snapshot.handler).map_err(|_| ContractError::Capacity)?;
        let shape = Shape::inspect(declaration, Some((snapshot.phase, handler)))?;
        frame(
            declaration,
            snapshot.binding,
            snapshot.target,
            snapshot.generation,
            snapshot.receipt,
        )?;
        if snapshot.sealed == Some(ContentHash([0; 32])) {
            return Err(ContractError::InvalidCut);
        }
        if let Some(AuthorityFence {
            reason: FenceReason::Deadline(deadline),
            ..
        }) = snapshot.fence
            && deadline != declaration.spec.deadline
        {
            return Err(ContractError::InvalidCut);
        }
        let last = snapshot
            .last_result
            .map(|value| accepted(declaration, value, &shape))
            .transpose()?;
        if let Some(last) = last {
            identity(snapshot.binding, last.binding)?;
            require(
                last.target == snapshot.target
                    && last.generation == snapshot.generation
                    && last.receipt == snapshot.receipt
                    && last.resulting_state == snapshot.state
                    && last.programmatic_evidence == snapshot.programmatic_evidence,
            )?;
        }
        if snapshot.state.is_terminal() {
            require(last.is_some() && snapshot.fence.is_none() && snapshot.suppression.is_none())?;
        }
        if snapshot.begun {
            begun(declaration, &snapshot, &shape, last)?;
        } else {
            unbegun(declaration, &snapshot, last)?;
        }
        Ok(Self {
            definition: declaration.definition_stamp(),
            binding: snapshot.binding,
            target: snapshot.target,
            generation: snapshot.generation,
            receipt: snapshot.receipt,
            state: snapshot.state,
            phase: snapshot.phase,
            handler,
            handler_attempt: snapshot.handler_attempt,
            attempt: snapshot.attempt,
            begun: snapshot.begun,
            suppression: snapshot.suppression,
            sealed: snapshot.sealed,
            fence: snapshot.fence,
            programmatic_evidence: snapshot.programmatic_evidence,
            last_result: last,
        })
    }
}

fn require(condition: bool) -> Result<(), ContractError> {
    if condition {
        Ok(())
    } else {
        Err(ContractError::InvalidTransition)
    }
}

fn policies(
    declaration: &Declaration,
) -> impl Iterator<Item = (Phase, &definition::OwnedPhasePolicy)> {
    let (first, second) = match &declaration.spec.program {
        OwnedProgram::Delivery => (None, None),
        OwnedProgram::Programmatic { check, quality } => (
            Some((Phase::Programmatic, check)),
            quality.as_ref().map(|policy| (Phase::Quality, policy)),
        ),
        OwnedProgram::Agentic { check } => (Some((Phase::Quality, check)), None),
    };
    first.into_iter().chain(second)
}

fn quote(declaration: &Declaration, base: usize) -> Result<usize, ContractError> {
    let mut visits = base;
    // At most two policies; handler bodies are visited only after this quote.
    for (_, policy) in policies(declaration) {
        visits = visits
            .checked_add(policy.handlers.len())
            .ok_or(ContractError::Capacity)?;
    }
    if let TargetDeclaration::WholeWorkSlot { name, .. } = declaration.target() {
        // Evaluation hydration validates its target and its optional result.
        visits = visits
            .checked_add(name.len().checked_mul(2).ok_or(ContractError::Capacity)?)
            .ok_or(ContractError::Capacity)?;
    }
    Ok(visits)
}

#[derive(Default)]
struct Shape {
    programmatic: u32,
    quality: u32,
    cursor: Option<(u32, u32)>,
}
impl Shape {
    fn inspect(
        declaration: &Declaration,
        selected: Option<(Phase, usize)>,
    ) -> Result<Self, ContractError> {
        let mut shape = Self::default();
        for (phase, policy) in policies(declaration) {
            let mut offset = 0_u32;
            for (index, step) in policy.handlers.iter().enumerate() {
                if selected == Some((phase, index)) {
                    shape.cursor = Some((offset, step.attempts));
                }
                offset = offset
                    .checked_add(step.attempts)
                    .ok_or(ContractError::Capacity)?;
            }
            match phase {
                Phase::Programmatic => shape.programmatic = offset,
                Phase::Quality => shape.quality = offset,
                Phase::Delivery | Phase::MissingTarget => return Err(ContractError::InvalidPolicy),
            }
        }
        Ok(shape)
    }
    fn phase_total(&self, phase: Phase) -> Result<u32, ContractError> {
        match phase {
            Phase::Programmatic if self.programmatic != 0 => Ok(self.programmatic),
            Phase::Quality if self.quality != 0 => Ok(self.quality),
            _ => Err(ContractError::InvalidTransition),
        }
    }
    fn attempt(&self, phase: Phase, attempt: u32) -> Result<(), ContractError> {
        let total = self.phase_total(phase)?;
        if phase == Phase::Quality && self.programmatic != 0 {
            require(
                attempt != 0
                    && attempt
                        < self
                            .programmatic
                            .checked_add(total)
                            .ok_or(ContractError::Capacity)?,
            )
        } else {
            require(attempt < total)
        }
    }
}

fn identity(current: Binding, original: Binding) -> Result<(), ContractError> {
    Binding {
        revision: original.revision,
        ..current
    }
    .check(&original)?;
    if current.revision < original.revision {
        return Err(ContractError::StaleRevision);
    }
    Ok(())
}

fn frame(
    declaration: &Declaration,
    binding: Binding,
    target: Target,
    generation: u64,
    receipt: Option<ReceiptFence>,
) -> Result<(), ContractError> {
    identity(binding, declaration.binding())?;
    let slot_name = match declaration.target() {
        TargetDeclaration::WholeWorkSlot { name, .. } => Some(name),
        _ => None,
    };
    check_target(declaration, target, slot_name)?;
    if generation == 0 {
        return Err(ContractError::StaleEvaluation);
    }
    match (declaration.target(), receipt) {
        (TargetDeclaration::Admission, None) => Ok(()),
        (TargetDeclaration::Admission, Some(_)) | (_, None) => Err(ContractError::StaleReceipt),
        (_, Some(receipt)) if receipt.receipt.is_zero() || receipt.epoch == 0 => {
            Err(ContractError::StaleReceipt)
        }
        (_, Some(_)) => Ok(()),
    }
}

fn accepted(
    declaration: &Declaration,
    value: AcceptedResultSnapshotV1,
    shape: &Shape,
) -> Result<AcceptedResult, ContractError> {
    frame(
        declaration,
        value.binding,
        value.target,
        value.generation,
        value.receipt,
    )?;
    require(
        value.ledger == declaration.binding().ledger
            && value.claim == declaration.spec.claim
            && value.validation.0 == declaration.binding().object.0
            && value.declaration_index == declaration.spec.declaration_index
            && value.mode == declaration.spec.mode,
    )?;
    let structural = matches!(value.phase, Phase::Delivery | Phase::MissingTarget);
    if structural {
        require(
            value.attempt.is_none()
                && value.evidence.is_none()
                && value.programmatic_evidence.is_none()
                && value.reporter.is_none(),
        )?;
        match value.phase {
            Phase::Delivery => require(
                matches!(value.target, Target::Delivery { .. })
                    && value.verdict == VerdictValue::Pass
                    && value.resulting_state == State::Validated,
            )?,
            Phase::MissingTarget => require(
                matches!(value.target, Target::MissingSlot { .. })
                    && value.mode == ValidationMode::Required
                    && value.verdict == VerdictValue::Incomplete
                    && value.resulting_state == State::ValidationIncomplete,
            )?,
            _ => return Err(ContractError::InvalidTransition),
        }
    } else {
        require(!matches!(
            value.target,
            Target::Delivery { .. } | Target::MissingSlot { .. }
        ))?;
        let attempt = value.attempt.ok_or(ContractError::InvalidTransition)?;
        shape.attempt(value.phase, attempt)?;
        require(
            value.evidence.is_some()
                && value.reporter == Some(declaration.policy(value.phase)?.evaluator),
        )?;
        let state = match value.verdict {
            VerdictValue::Pass if value.phase == Phase::Programmatic && shape.quality != 0 => {
                State::ValidatingQualityBar
            }
            VerdictValue::Pass => State::Validated,
            VerdictValue::Incomplete => State::ValidationIncomplete,
            VerdictValue::Fail => match (value.phase, value.mode) {
                (Phase::Programmatic, ValidationMode::Required) => State::ValidationFailed,
                (Phase::Programmatic, ValidationMode::Observe) => {
                    State::ValidationFailedNotRequired
                }
                (Phase::Quality, ValidationMode::Required) => State::QualityBarValidationFailed,
                (Phase::Quality, ValidationMode::Observe) => {
                    State::QualityBarValidationFailedNotRequired
                }
                _ => return Err(ContractError::InvalidTransition),
            },
            VerdictValue::Error if value.resulting_state.is_terminal() => match value.mode {
                ValidationMode::Required => State::Errored,
                ValidationMode::Observe => State::ErroredNotRequired,
            },
            VerdictValue::Error => active_state(value.phase)?,
        };
        require(value.resulting_state == state)?;
        if value.verdict == VerdictValue::Error {
            let phase_total = shape.phase_total(value.phase)?;
            let exhausted = phase_total
                .checked_sub(1)
                .ok_or(ContractError::InvalidPolicy)?;
            if value.phase == Phase::Quality && shape.programmatic != 0 {
                if value.resulting_state.is_terminal() {
                    let first = attempt
                        .checked_sub(exhausted)
                        .ok_or(ContractError::InvalidTransition)?;
                    require(first != 0 && first <= shape.programmatic)?;
                } else {
                    let last = shape
                        .programmatic
                        .checked_add(exhausted)
                        .ok_or(ContractError::Capacity)?;
                    require(exhausted != 0 && attempt < last)?;
                }
            } else {
                require(if value.resulting_state.is_terminal() {
                    attempt == exhausted
                } else {
                    attempt < exhausted
                })?;
            }
        }
        if value.phase == Phase::Programmatic {
            require(
                value.programmatic_evidence
                    == if value.verdict == VerdictValue::Pass {
                        value.evidence
                    } else {
                        None
                    },
            )?;
        } else {
            require(value.programmatic_evidence.is_some() == (shape.programmatic != 0))?;
        }
    }
    let reports = value.attempt.map_or(Ok(1_u64), |attempt| {
        u64::from(attempt)
            .checked_add(2)
            .ok_or(ContractError::Capacity)
    })?;
    minimum_revision(declaration, value.binding, reports)?;
    Ok(AcceptedResult {
        definition: declaration.definition_stamp(),
        binding: value.binding,
        ledger: value.ledger,
        claim: value.claim,
        target: value.target,
        validation: value.validation,
        declaration_index: value.declaration_index,
        mode: value.mode,
        verdict: value.verdict,
        phase: value.phase,
        attempt: value.attempt,
        generation: value.generation,
        receipt: value.receipt,
        evidence: value.evidence,
        programmatic_evidence: value.programmatic_evidence,
        reporter: value.reporter,
        resulting_state: value.resulting_state,
    })
}

fn active_state(phase: Phase) -> Result<State, ContractError> {
    match phase {
        Phase::Programmatic => Ok(State::Validating),
        Phase::Quality => Ok(State::ValidatingQualityBar),
        Phase::Delivery | Phase::MissingTarget => Err(ContractError::InvalidTransition),
    }
}
fn minimum_revision(
    declaration: &Declaration,
    binding: Binding,
    advances: u64,
) -> Result<(), ContractError> {
    let minimum = declaration
        .binding()
        .revision
        .0
        .checked_add(advances)
        .ok_or(ContractError::Capacity)?;
    if binding.revision.0 < minimum {
        return Err(ContractError::StaleRevision);
    }
    Ok(())
}

fn unbegun(
    declaration: &Declaration,
    value: &EvaluationSnapshotV1,
    last: Option<AcceptedResult>,
) -> Result<(), ContractError> {
    require(
        value.phase == declaration.first_phase()
            && value.handler == 0
            && value.handler_attempt == 0
            && value.attempt == 0
            && value.programmatic_evidence.is_none(),
    )?;
    if value.state == State::Ready {
        require(last.is_none())?;
    } else {
        require(value.sealed.is_none())?;
        let last = last.ok_or(ContractError::InvalidTransition)?;
        require(matches!(
            (value.state, last.phase),
            (State::Validated, Phase::Delivery)
                | (State::ValidationIncomplete, Phase::MissingTarget)
        ))?;
    }
    match value.suppression {
        Some(Suppression::MissingTarget) => require(
            matches!(value.target, Target::MissingSlot { .. })
                && declaration.spec.mode == ValidationMode::Observe,
        )?,
        Some(Suppression::ArtifactFailure(_)) => {
            require(matches!(value.target, Target::Artifact { .. }))?
        }
        Some(Suppression::ParentFailure(_)) => {
            require(!matches!(value.target, Target::Delivery { .. }))?
        }
        Some(Suppression::CohortSealed(_)) | None => {}
    }
    if value.sealed.is_some() {
        require(value.suppression.is_some())?;
    }
    if value.suppression.is_some() || value.fence.is_some() || value.sealed.is_some() {
        minimum_revision(declaration, value.binding, 1)?;
    }
    Ok(())
}

fn begun(
    declaration: &Declaration,
    value: &EvaluationSnapshotV1,
    shape: &Shape,
    last: Option<AcceptedResult>,
) -> Result<(), ContractError> {
    require(
        value.suppression.is_none()
            && !matches!(
                value.target,
                Target::Delivery { .. } | Target::MissingSlot { .. }
            ),
    )?;
    let (offset, attempts) = shape.cursor.ok_or(ContractError::InvalidTransition)?;
    require(value.handler_attempt < attempts)?;
    let local = offset
        .checked_add(value.handler_attempt)
        .ok_or(ContractError::Capacity)?;
    shape.attempt(value.phase, value.attempt)?;
    if value.phase == Phase::Quality && shape.programmatic != 0 {
        let first = value
            .attempt
            .checked_sub(local)
            .ok_or(ContractError::InvalidTransition)?;
        require(
            first != 0 && first <= shape.programmatic && value.programmatic_evidence.is_some(),
        )?;
    } else {
        require(value.attempt == local)?;
    }
    if !value.state.is_terminal() {
        require(value.state == active_state(value.phase)?)?;
    }
    minimum_revision(
        declaration,
        value.binding,
        u64::from(value.attempt)
            .checked_add(1)
            .ok_or(ContractError::Capacity)?,
    )?;
    match last {
        None => require(
            value.attempt == 0
                && value.handler == 0
                && value.handler_attempt == 0
                && value.phase == declaration.first_phase()
                && value.state == active_state(value.phase)?
                && value.programmatic_evidence.is_none(),
        )?,
        Some(last) if last.is_terminal() => {
            require(last.phase == value.phase && last.attempt == Some(value.attempt))?;
            if last.verdict == VerdictValue::Error {
                require(
                    value
                        .handler_attempt
                        .checked_add(1)
                        .ok_or(ContractError::Capacity)?
                        == attempts
                        && offset
                            .checked_add(attempts)
                            .ok_or(ContractError::Capacity)?
                            == shape.phase_total(value.phase)?,
                )?;
            }
        }
        Some(last) => {
            require(
                last.attempt.and_then(|attempt| attempt.checked_add(1)) == Some(value.attempt),
            )?;
            match last.verdict {
                VerdictValue::Error => require(last.phase == value.phase)?,
                VerdictValue::Pass => require(
                    last.phase == Phase::Programmatic
                        && value.phase == Phase::Quality
                        && value.handler == 0
                        && value.handler_attempt == 0,
                )?,
                VerdictValue::Fail | VerdictValue::Incomplete => {
                    return Err(ContractError::InvalidTransition);
                }
            }
        }
    }
    Ok(())
}
