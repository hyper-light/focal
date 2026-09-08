//! Check that a WholeWork journal and final non-claim rows describe the same
//! transaction. Model capabilities authorize transitions; this check protects
//! their complete revision history and original publication coordinates.
use super::prepare::{Extras, Scratch};
use super::*;
use focal_model::lifecycle::aggregation::{ArtifactOutcome, PublicationPosition, ResponseOutcome};
use focal_model::{ArtifactRef, ValidationMode};

pub(super) trait Source {
    fn claim(&self, id: ClaimId) -> Option<&ClaimState>;
    fn definition(&self, id: ValidationId) -> Option<&validation::Declaration>;
    fn work(&self, id: ArtifactId) -> Option<&NativeWork>;
    fn response(&self, id: TestamentId) -> Option<&NativeResponseRecord>;
    fn evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState>;
    fn missing(&self, key: NativeResultKey) -> Option<&NativeMissingResult>;
    fn artifact(&self, id: ArtifactId) -> Option<&NativeArtifact>;
    fn accepted(&self, key: NativeResultKey) -> Option<&NativeAccepted>;
}
impl Source for View<'_> {
    fn claim(&self, id: ClaimId) -> Option<&ClaimState> {
        EffectiveClaims::claim(self, id)
    }
    fn definition(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.get(Key::Definition(id)))
    }
    fn work(&self, id: ArtifactId) -> Option<&NativeWork> {
        as_work(self.get(Key::Work(id)))
    }
    fn response(&self, id: TestamentId) -> Option<&NativeResponseRecord> {
        super::response_reads::as_response_record(self.get(Key::Response(id)))
    }
    fn evaluation(&self, key: EvaluationKey) -> Option<&validation::EvaluationState> {
        as_evaluation(self.get(Key::Evaluation(key)))
    }
    fn missing(&self, key: NativeResultKey) -> Option<&NativeMissingResult> {
        super::response_reads::as_missing(self.get(Key::MissingResult(key)))
    }
    fn artifact(&self, id: ArtifactId) -> Option<&NativeArtifact> {
        as_artifact(self.get(Key::Artifact(id)))
    }
    fn accepted(&self, key: NativeResultKey) -> Option<&NativeAccepted> {
        as_result(self.get(Key::Accepted(key)))
    }
}

fn advance(
    binding: &mut Binding,
    before: Option<Binding>,
    after: Binding,
) -> Result<(), NativeError> {
    if before != Some(*binding) || after != binding.next()? {
        return Err(ContractError::StaleRevision.into());
    }
    *binding = after;
    Ok(())
}

struct Events<'a> {
    journal: &'a [NativeFact],
    positions: &'a [(Key, usize)],
    sequence: SessionSeq,
}
impl Events<'_> {
    fn iter(&self) -> impl Iterator<Item = Result<(PublicationPosition, NativeFact), NativeError>> {
        self.positions.iter().map(|(_, ordinal)| {
            let fact = *self
                .journal
                .get(*ordinal)
                .ok_or(ContractError::InvalidCut)?;
            Ok((
                PublicationPosition {
                    sequence: self.sequence,
                    ordinal: u32::try_from(*ordinal)
                        .map_err(|_| NativeError::Capacity("object journal ordinal"))?,
                },
                fact,
            ))
        })
    }
    fn only(&self) -> Result<(PublicationPosition, NativeFact), NativeError> {
        if self.positions.len() != 1 {
            return Err(ContractError::InvalidManifest.into());
        }
        self.iter().next().ok_or(ContractError::InvalidManifest)?
    }
}

fn work(source: &NativeWork, next: &NativeWork, events: Events<'_>) -> Result<(), NativeError> {
    let old = &source.state;
    let final_state = &next.state;
    if source.next != next.next
        || old.claim() != final_state.claim()
        || old.receipt() != final_state.receipt()
        || old.cycle() != final_state.cycle()
        || old.slot() != final_state.slot()
        || old.producer() != final_state.producer()
        || old.attachment() != final_state.attachment()
        || old.diagnostic() != final_state.diagnostic()
    {
        return Err(ContractError::InvalidTarget.into());
    }
    let mut binding = old.binding();
    let mut state = old.state();
    let mut terminal_sequence = old.terminal().map(|(sequence, _)| sequence);
    for event in events.iter() {
        let (
            position,
            NativeFact::Work {
                claim,
                before,
                after,
                state: updated,
            },
        ) = event?
        else {
            return Err(ContractError::InvalidTarget.into());
        };
        if claim != old.claim()
            || !matches!(
                (state, updated),
                (WorkArtifactState::Attached, WorkArtifactState::Validating)
                    | (
                        WorkArtifactState::Validating,
                        WorkArtifactState::Validated | WorkArtifactState::ValidationFailed
                    )
            )
        {
            return Err(ContractError::InvalidTransition.into());
        }
        advance(&mut binding, before, after)?;
        state = updated;
        if state != WorkArtifactState::Validating {
            terminal_sequence = Some(position.sequence);
        }
    }
    binding.check(&final_state.binding())?;
    if state != final_state.state()
        || terminal_sequence != final_state.terminal().map(|(sequence, _)| sequence)
        || !matches!(
            (state, final_state.terminal()),
            (WorkArtifactState::Validating, None)
                | (
                    WorkArtifactState::Validated,
                    Some((_, ArtifactOutcome::Passed))
                )
                | (
                    WorkArtifactState::ValidationFailed,
                    Some((_, ArtifactOutcome::Blocked(_)))
                )
        )
    {
        return Err(ContractError::InvalidTransition.into());
    }
    Ok(())
}

fn response(
    source: &NativeResponseRecord,
    next: &NativeResponseRecord,
    claim: &ClaimState,
    events: Events<'_>,
) -> Result<(), NativeError> {
    let old = source.response();
    let final_state = next.response();
    let mut identity = final_state.identity();
    identity.binding = old.identity().binding;
    if identity != old.identity()
        || old.manifest() != final_state.manifest()
        || old.failed_work() != final_state.failed_work()
        || old.summary() != final_state.summary()
        || old.confidence() != final_state.confidence()
        || old.reported_outcome() != final_state.reported_outcome()
        || old.diagnostics() != final_state.diagnostics()
        || old.respondent() != final_state.respondent()
        || source.received() != next.received()
    {
        return Err(ContractError::ContentConflict.into());
    }
    let mut binding = old.identity().binding;
    let mut state = old.state();
    let mut entered = source.entered();
    let mut terminal_sequence = None;
    for event in events.iter() {
        let (
            position,
            NativeFact::Response {
                claim,
                before,
                after,
                state: updated,
            },
        ) = event?
        else {
            return Err(ContractError::InvalidTarget.into());
        };
        if claim != old.identity().claim
            || !matches!(
                (state, updated),
                (ResponseState::Received, ResponseState::Validating)
                    | (
                        ResponseState::Validating,
                        ResponseState::Validated
                            | ResponseState::ValidationIncomplete
                            | ResponseState::ValidationFailed
                            | ResponseState::ValidationErrored
                    )
            )
        {
            return Err(ContractError::InvalidTransition.into());
        }
        advance(&mut binding, before, after)?;
        if state == ResponseState::Received {
            entered = Some(position);
        }
        state = updated;
        if state != ResponseState::Validating {
            terminal_sequence = Some(position.sequence);
        }
    }
    binding.check(&final_state.identity().binding)?;
    let actual_terminal = match final_state.terminal() {
        None => None,
        Some(ResponseOutcome::Validated { sequence }) => Some(sequence),
        Some(ResponseOutcome::Blocked(cut)) => Some(cut.sequence()),
        Some(ResponseOutcome::Evaluating) => return Err(ContractError::InvalidCut.into()),
    };
    if state != final_state.state()
        || entered != next.entered()
        || terminal_sequence != actual_terminal
    {
        return Err(ContractError::InvalidCut.into());
    }
    let entered = entered.ok_or(ContractError::InvalidCut)?;
    if old.state() != ResponseState::Received {
        if source.entered().is_none() || source.entered() != next.entered() {
            return Err(ContractError::InvalidCut.into());
        }
        return Ok(());
    }
    match claim.status() {
        ClaimStatus::Validating => {}
        ClaimStatus::TestamentAcknowledged => {
            let end = usize::try_from(entered.ordinal).map_err(|_| ContractError::Capacity)?;
            let before_entry = events.journal.get(..end).ok_or(ContractError::InvalidCut)?;
            if !before_entry.iter().any(|fact| matches!(fact,
                NativeFact::Claim(event) if event.kind == NativeEventKind::Validating
                    && event.before == Some(claim.binding()) && event.status == ClaimStatus::Validating))
            {
                return Err(ContractError::InvalidCut.into());
            }
        }
        _ => return Err(ContractError::InvalidTransition.into()),
    }
    Ok(())
}

fn staged(extras: &Extras, key: Key) -> Option<&Row> {
    extras
        .rows
        .binary_search_by_key(&key, |row| row.key)
        .ok()
        .and_then(|at| extras.rows.get(at))
        .map(|row| &row.row)
}

fn entered(
    extras: &Extras,
    source: &impl Source,
    id: TestamentId,
) -> Result<PublicationPosition, NativeError> {
    super::response_reads::as_response_record(staged(extras, Key::Response(id)))
        .or_else(|| source.response(id))
        .and_then(NativeResponseRecord::entered)
        .ok_or(ContractError::InvalidCut.into())
}

fn after_entry(
    extras: &Extras,
    source: &impl Source,
    response: TestamentId,
    event: PublicationPosition,
) -> Result<(), NativeError> {
    let entry = entered(extras, source, response)?;
    if entry.sequence.0 == 0 || entry >= event {
        return Err(ContractError::InvalidCut.into());
    }
    Ok(())
}

// Preserve causal publication phases as well as each object's revision chain.
fn entry_phase(fact: NativeFact) -> Result<u8, NativeError> {
    match fact {
        NativeFact::Claim(event) => match event.kind {
            NativeEventKind::Validating => Ok(0),
            NativeEventKind::LocallyComplete
            | NativeEventKind::ValidationIncomplete
            | NativeEventKind::ValidationFailed
            | NativeEventKind::ValidationErrored => Ok(6),
            NativeEventKind::Satisfied
            | NativeEventKind::DependencyFailed
            | NativeEventKind::Monitor(NativeMonitorEvent::Released { .. }) => Ok(7),
            _ => Err(ContractError::InvalidTransition.into()),
        },
        NativeFact::Response {
            state: ResponseState::Validating,
            ..
        } => Ok(1),
        NativeFact::Work {
            state: WorkArtifactState::Validating,
            ..
        } => Ok(2),
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::MissingTarget,
            ..
        }
        | NativeFact::Missing { .. } => Ok(3),
        NativeFact::Work {
            state: WorkArtifactState::Validated | WorkArtifactState::ValidationFailed,
            ..
        } => Ok(4),
        NativeFact::Response {
            state:
                ResponseState::Validated
                | ResponseState::ValidationIncomplete
                | ResponseState::ValidationFailed
                | ResponseState::ValidationErrored,
            ..
        } => Ok(5),
        _ => Err(ContractError::InvalidTransition.into()),
    }
}

fn phase(operation: NativeOperation, ordinal: usize, fact: NativeFact) -> Result<u8, NativeError> {
    match operation {
        NativeOperation::EnterWholeWork => entry_phase(fact),
        NativeOperation::BeginWork => {
            if ordinal == 0 {
                return if matches!(
                    fact,
                    NativeFact::Evaluation {
                        kind: NativeEvaluationEventKind::Begun,
                        ..
                    }
                ) {
                    Ok(0)
                } else {
                    Err(ContractError::InvalidCut.into())
                };
            }
            entry_phase(fact)?
                .checked_add(1)
                .ok_or(ContractError::Capacity.into())
        }
        NativeOperation::ReportWork => match (ordinal, fact) {
            (0, NativeFact::Artifact { .. }) => Ok(0),
            (
                1,
                NativeFact::Evaluation {
                    kind: NativeEvaluationEventKind::Reported,
                    ..
                },
            ) => Ok(1),
            (2, NativeFact::Accepted { .. }) => Ok(2),
            (0..=2, _) => Err(ContractError::InvalidCut.into()),
            (_, fact) => match entry_phase(fact)? {
                terminal @ 4..=7 => terminal
                    .checked_sub(1)
                    .ok_or(ContractError::Capacity.into()),
                _ => Err(ContractError::InvalidTransition.into()),
            },
        },
        _ => Err(ContractError::InvalidTransition.into()),
    }
}

fn missing_evaluation(
    key: EvaluationKey,
    old: &validation::EvaluationState,
    next: &validation::EvaluationState,
    definition: &validation::Declaration,
    events: Events<'_>,
) -> Result<(), NativeError> {
    check_missing_fact(key, old, next, definition, events.only()?.1)
}

/// Structural absence advances the existing Ready row without an external
/// attempt. Both the original object journal and the completion collector use
/// this exact check; a later cohort seal cannot erase the intermediate fact.
pub(super) fn check_missing_fact(
    key: EvaluationKey,
    old: &validation::EvaluationState,
    next: &validation::EvaluationState,
    definition: &validation::Declaration,
    fact: NativeFact,
) -> Result<(), NativeError> {
    let previous = old.bind(definition)?;
    let updated = next.bind(definition)?;
    let NativeFact::Evaluation {
        kind: NativeEvaluationEventKind::MissingTarget,
        key: recorded,
        before,
        after,
        state,
        phase: validation::Phase::MissingTarget,
        attempt: None,
        fence,
    } = fact
    else {
        return Err(ContractError::InvalidTransition.into());
    };
    let mut binding = old.binding();
    advance(&mut binding, before, after)?;
    if recorded != key
        || EvaluationKey::of(key.claim, next) != key
        || binding != next.binding()
        || old.state() != validation::State::Ready
        || old.has_begun()
        || old.last_result().is_some()
        || old.fence().is_some()
        || next.fence() != fence
        || fence.is_some()
        || previous.suppression().is_some()
        || old.sealed().is_some()
        || next.sealed().is_some()
        || old.target() != next.target()
        || old.receipt() != next.receipt()
        || old.generation() != next.generation()
        || old.phase() != next.phase()
        || next.has_begun()
        || state != next.state()
        || !matches!(key.target, EvaluationTarget::MissingSlot { .. })
    {
        return Err(ContractError::InvalidTransition.into());
    }
    match (next.state(), updated.suppression(), next.last_result()) {
        (validation::State::Ready, Some(validation::Suppression::MissingTarget), None)
            if updated.mode() == ValidationMode::Observe =>
        {
            Ok(())
        }
        (validation::State::ValidationIncomplete, None, Some(result))
            if updated.mode() == ValidationMode::Required
                && result.mode() == ValidationMode::Required
                && result.claim() == key.claim
                && result.validation() == key.validation
                && result.target() == next.target()
                && result.generation() == next.generation()
                && result.receipt() == next.receipt()
                && result.resulting_state() == next.state()
                && result.verdict() == focal_model::VerdictValue::Incomplete
                && result.phase() == validation::Phase::MissingTarget
                && result.binding() == next.binding()
                && result.attempt().is_none()
                && result.reporter().is_none()
                && result.evidence().is_none()
                && result.programmatic_evidence().is_none() =>
        {
            Ok(())
        }
        _ => Err(ContractError::InvalidTransition.into()),
    }
}

fn pinned(current: Binding, expected: Binding) -> Result<(), NativeError> {
    Binding {
        revision: expected.revision,
        ..current
    }
    .check(&expected)?;
    if current.revision < expected.revision {
        return Err(ContractError::StaleRevision.into());
    }
    Ok(())
}

fn work_target(
    source: &impl Source,
    key: EvaluationKey,
    state: &validation::EvaluationState,
    definition: &validation::Declaration,
) -> Result<TestamentId, NativeError> {
    let validation::Target::Artifact {
        response,
        slot,
        artifact,
    } = state.target()
    else {
        return Err(ContractError::InvalidTarget.into());
    };
    let id = TestamentId(response.object.0);
    let claim = source
        .claim(key.claim)
        .ok_or(ContractError::MissingEvidence)?;
    claim.acceptance().check_declaration(definition)?;
    let actual = source
        .response(id)
        .ok_or(ContractError::MissingEvidence)?
        .response();
    let work = &source
        .work(ArtifactId(artifact.object.0))
        .ok_or(ContractError::MissingEvidence)?
        .state;
    pinned(actual.identity().binding, response)?;
    pinned(work.binding(), artifact)?;
    if !matches!(definition.target(), validation::TargetDeclaration::WholeWorkSlot { index, .. } if index == slot)
        || EvaluationKey::of(key.claim, state) != key
        || definition.claim() != key.claim
        || actual.identity().claim != key.claim
        || actual.identity().binding.ledger != state.binding().ledger
        || !matches!(
            actual.state(),
            ResponseState::Received
                | ResponseState::Validating
                | ResponseState::Validated
                | ResponseState::ValidationIncomplete
                | ResponseState::ValidationFailed
                | ResponseState::ValidationErrored
        )
        || actual.identity().cycle == 0
        || state.generation() != u64::from(actual.identity().cycle)
        || state.receipt() != Some(actual.identity().receipt)
        || work.claim() != key.claim
        || work.cycle() != actual.identity().cycle
        || work.receipt() != actual.identity().receipt
        || work.slot() != slot
        || work.attachment() != Some(id)
        || actual
            .manifest()
            .binary_search_by_key(&slot, |entry| entry.slot)
            .ok()
            .and_then(|at| actual.manifest().get(at))
            .is_none_or(|entry| entry.artifact != work.reference())
    {
        return Err(ContractError::InvalidTarget.into());
    }
    Ok(id)
}

fn external_evaluation(
    key: EvaluationKey,
    old: &validation::EvaluationState,
    next: &validation::EvaluationState,
    definition: &validation::Declaration,
    events: Events<'_>,
) -> Result<(), NativeError> {
    let previous = old.bind(definition)?;
    let updated = next.bind(definition)?;
    let (
        _,
        NativeFact::Evaluation {
            kind,
            key: recorded,
            before,
            after,
            state,
            phase,
            attempt,
            fence,
        },
    ) = events.only()?
    else {
        return Err(ContractError::InvalidTransition.into());
    };
    let mut binding = old.binding();
    advance(&mut binding, before, after)?;
    if recorded != key
        || EvaluationKey::of(key.claim, old) != key
        || EvaluationKey::of(key.claim, next) != key
        || binding != next.binding()
        || old.target() != next.target()
        || old.receipt() != next.receipt()
        || old.generation() != next.generation()
        || old.sealed() != next.sealed()
        || old.fence().is_some()
        || fence != next.fence()
        || fence.is_some()
        || previous.suppression().is_some()
        || updated.suppression().is_some()
        || state != next.state()
        || phase != next.phase()
        || !next.has_begun()
    {
        return Err(ContractError::InvalidTransition.into());
    }
    match kind {
        NativeEvaluationEventKind::Begun => {
            if old.state() != validation::State::Ready
                || old.has_begun()
                || old.last_result().is_some()
                || next.last_result().is_some()
                || old.sealed().is_some()
                || old.phase() != next.phase()
                || updated.attempt_index() != Some(0)
                || attempt != Some(updated.current_attempt()?)
                || !matches!(
                    (phase, state),
                    (
                        validation::Phase::Programmatic,
                        validation::State::Validating
                    ) | (
                        validation::Phase::Quality,
                        validation::State::ValidatingQualityBar
                    )
                )
            {
                return Err(ContractError::InvalidTransition.into());
            }
        }
        NativeEvaluationEventKind::Reported => {
            let current = previous.current_attempt()?;
            let result = next.last_result().ok_or(ContractError::MissingEvidence)?;
            if attempt != Some(current)
                || old.last_result() == Some(result)
                || result.binding() != next.binding()
                || result.ledger() != next.binding().ledger
                || result.claim() != key.claim
                || result.validation() != key.validation
                || result.declaration_index() != definition.declaration_index()
                || result.mode() != definition.mode()
                || result.target() != next.target()
                || result.receipt() != next.receipt()
                || result.generation() != next.generation()
                || result.phase() != current.phase
                || result.attempt() != Some(current.index)
                || result.reporter() != Some(current.evaluator)
                || result.evidence().is_none()
                || result.resulting_state() != next.state()
            {
                return Err(ContractError::InvalidTransition.into());
            }
            if !next.state().is_terminal()
                && updated.current_attempt()?.index
                    != current
                        .index
                        .checked_add(1)
                        .ok_or(ContractError::Capacity)?
            {
                return Err(ContractError::StaleEvaluation.into());
            }
        }
        _ => return Err(ContractError::InvalidTransition.into()),
    }
    Ok(())
}

fn accepted(
    extras: &Extras,
    source: &impl Source,
    key: NativeResultKey,
    row: &NativeAccepted,
    events: Events<'_>,
) -> Result<(), NativeError> {
    let (position, fact) = events.only()?;
    let result = row.result();
    if fact != (NativeFact::Accepted { key })
        || source.accepted(key).is_some()
        || NativeResultKey::of(result) != key
        || position.sequence != row.sequence()
        || position.ordinal != row.ordinal()
        || position.ordinal != 2
    {
        return Err(ContractError::InvalidCut.into());
    }
    let next = as_evaluation(staged(extras, Key::Evaluation(key.evaluation)))
        .ok_or(ContractError::MissingEvidence)?;
    let old = source
        .evaluation(key.evaluation)
        .ok_or(ContractError::MissingEvidence)?;
    let definition = source
        .definition(key.evaluation.validation)
        .ok_or(ContractError::InvalidPolicy)?;
    let attempt = old.bind(definition)?.current_attempt()?;
    let evidence = result.evidence().ok_or(ContractError::MissingEvidence)?;
    let artifact = as_artifact(staged(extras, Key::Artifact(evidence.id)))
        .ok_or(ContractError::MissingEvidence)?;
    let facts = artifact.facts().ok_or(ContractError::MissingEvidence)?;
    if next.last_result() != Some(result)
        || row.attempt() != attempt
        || row.artifact().result() != result
        || row.artifact().reference() != evidence
        || row.artifact().producer() != attempt.evaluator
        || facts.binding.object.0 != evidence.id.0
        || facts.binding.content != evidence.hash
        || facts.binding.ledger != result.ledger()
        || facts.claim != key.evaluation.claim
        || facts.validation != key.evaluation.validation
        || facts.target != result.target()
        || facts.generation != result.generation()
        || facts.attempt != attempt
        || facts.producer != attempt.evaluator
        || facts.value != result.verdict()
        || Some(facts.kind) != result.evidence_kind()
        || artifact.descriptor().receipt() != result.receipt()
    {
        return Err(ContractError::MissingEvidence.into());
    }
    Ok(())
}

fn target_response(
    operation: NativeOperation,
    journal: &[NativeFact],
) -> Result<TestamentId, NativeError> {
    let fact = match operation {
        NativeOperation::EnterWholeWork => {
            let mut entries = journal.iter().filter_map(|fact| match fact {
                NativeFact::Response {
                    after,
                    state: ResponseState::Validating,
                    ..
                } => Some(TestamentId(after.object.0)),
                _ => None,
            });
            let id = entries.next().ok_or(ContractError::InvalidCut)?;
            if entries.next().is_some() {
                return Err(ContractError::InvalidCut.into());
            }
            return Ok(id);
        }
        NativeOperation::BeginWork => journal.first(),
        NativeOperation::ReportWork => journal.get(1),
        _ => return Err(ContractError::InvalidTransition.into()),
    };
    match fact {
        Some(NativeFact::Evaluation {
            key:
                EvaluationKey {
                    target: EvaluationTarget::Work { response, .. },
                    ..
                },
            ..
        }) => Ok(*response),
        _ => Err(ContractError::InvalidTarget.into()),
    }
}

fn complete_entry(
    extras: &Extras,
    source: &impl Source,
    next: &NativeResponseRecord,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let response = next.response();
    let id = TestamentId(response.identity().binding.object.0);
    let claim = source
        .claim(response.identity().claim)
        .ok_or(ContractError::MissingEvidence)?;
    super::prepare::within(response.manifest().len(), limits.plan_edges)?;
    super::prepare::within(claim.acceptance().declarations().len(), limits.plan_edges)?;
    // Removing both a row and its facts must not silently omit a manifest
    // member or a structural missing-slot consequence from first entry.
    for member in response.manifest() {
        let work = as_work(staged(extras, Key::Work(member.artifact.id)))
            .ok_or(ContractError::MissingEvidence)?;
        let old = source
            .work(member.artifact.id)
            .ok_or(ContractError::MissingEvidence)?;
        if old.state.state() != WorkArtifactState::Attached
            || work.state.attachment() != Some(id)
            || work.state.reference() != member.artifact
        {
            return Err(ContractError::InvalidTarget.into());
        }
    }
    for summary in claim.acceptance().declarations() {
        let definition = source
            .definition(ValidationId(summary.binding().object.0))
            .ok_or(ContractError::InvalidPolicy)?;
        claim.acceptance().check_declaration(definition)?;
        let validation::TargetDeclaration::WholeWorkSlot { index, .. } = definition.target() else {
            continue;
        };
        if response
            .manifest()
            .binary_search_by_key(&index, |member| member.slot)
            .is_ok()
        {
            continue;
        }
        let key = EvaluationKey {
            claim: response.identity().claim,
            validation: ValidationId(definition.binding().object.0),
            target: EvaluationTarget::MissingSlot {
                response: id,
                slot: index,
            },
            generation: u64::from(response.identity().cycle),
        };
        let old = source
            .evaluation(key)
            .ok_or(ContractError::MissingEvidence)?;
        let bound = old.bind(definition)?;
        if old.state() == validation::State::Ready
            && !old.has_begun()
            && old.fence().is_none()
            && old.sealed().is_none()
            && bound.suppression().is_none()
            && as_evaluation(staged(extras, Key::Evaluation(key))).is_none()
        {
            return Err(ContractError::MissingEvidence.into());
        }
    }
    Ok(())
}

/// The sorted event index is temporary, charged before allocation, and bounded
/// by this transaction's event count. Matching rows and history takes one merge
/// walk; it never scans the ledger or allocates a map for each object.
#[cfg(test)]
pub(super) fn check(
    extras: &mut Extras,
    source: &impl Source,
    operation: NativeOperation,
    sequence: SessionSeq,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<(), NativeError> {
    check_with_monitors(extras, source, operation, sequence, limits, scratch, 0)
}

/// Index rows have already been independently reconstructed from the exact
/// scope journal by OriginalPlan. Object facts still account for all other rows.
pub(super) fn check_with_monitors(
    extras: &mut Extras,
    source: &impl Source,
    operation: NativeOperation,
    sequence: SessionSeq,
    limits: NativeLimits,
    scratch: &mut Scratch,
    validated_index_rows: usize,
) -> Result<(), NativeError> {
    let journal = extras
        .journal
        .as_ref()
        .ok_or(ContractError::InvalidTransition)?;
    if sequence.0 == 0
        || (operation == NativeOperation::BeginWork && journal.is_empty())
        || (operation == NativeOperation::ReportWork && journal.len() < 3)
    {
        return Err(ContractError::InvalidCut.into());
    }
    super::prepare::within(journal.len(), limits.plan_edges)?;
    let target_response = target_response(operation, journal)?;
    let mut index = scratch.reserve::<(Key, usize)>(journal.len())?;
    let mut previous_phase = 0;
    for (ordinal, fact) in journal.iter().enumerate() {
        let next_phase = phase(operation, ordinal, *fact)?;
        if next_phase < previous_phase {
            return Err(ContractError::InvalidCut.into());
        }
        previous_phase = next_phase;
        let key = match *fact {
            NativeFact::Claim(_) => continue,
            NativeFact::Work { after, .. } => Key::Work(ArtifactId(after.object.0)),
            NativeFact::Response { after, .. } => Key::Response(TestamentId(after.object.0)),
            NativeFact::Evaluation { key, .. } => Key::Evaluation(key),
            NativeFact::Missing { key } => Key::MissingResult(key),
            NativeFact::Artifact { binding } => Key::Artifact(ArtifactId(binding.object.0)),
            NativeFact::Accepted { key } => Key::Accepted(key),
            _ => return Err(ContractError::InvalidTransition.into()),
        };
        if index.len() == index.capacity() {
            return Err(ContractError::Capacity.into());
        }
        index.push((key, ordinal));
    }
    index.sort_unstable();
    extras.rows.sort_unstable_by_key(|extra| extra.key);
    let mut cursor = 0usize;
    let mut index_rows = 0usize;
    let mut previous = None;
    for extra in &extras.rows {
        if extra.fact.is_some() || previous.is_some_and(|old| old >= extra.key) {
            return Err(ContractError::InvalidManifest.into());
        }
        previous = Some(extra.key);
        if matches!(
            extra.key,
            Key::Monitor(_) | Key::MonitorHead(_) | Key::MonitorLink(_, _)
        ) {
            index_rows = index_rows.checked_add(1).ok_or(ContractError::Capacity)?;
            super::prepare::within(index_rows, validated_index_rows)?;
            continue;
        }
        // Descriptor identity is the only fact-free companion row. It must
        // name the exact new descriptor whose creation is journaled below.
        if let (Key::ArtifactIdentity(hash), Row::ArtifactIdentity(id)) = (&extra.key, &extra.row) {
            let artifact = as_artifact(staged(extras, Key::Artifact(*id)))
                .ok_or(ContractError::MissingEvidence)?;
            if operation != NativeOperation::ReportWork
                || artifact.descriptor().content_hash() != *hash
                || artifact.descriptor().id() != *id
            {
                return Err(ContractError::InvalidTarget.into());
            }
            continue;
        }
        let start = cursor;
        while index.get(cursor).is_some_and(|(key, _)| *key == extra.key) {
            cursor = cursor.checked_add(1).ok_or(ContractError::Capacity)?;
        }
        if start == cursor {
            return Err(ContractError::InvalidManifest.into());
        }
        let events = Events {
            journal,
            positions: index.get(start..cursor).ok_or(ContractError::InvalidCut)?,
            sequence,
        };
        match (&extra.key, &extra.row) {
            (Key::Work(id), Row::Work(row)) => {
                let next = row.get().ok_or(ContractError::MissingEvidence)?;
                if next.state.attachment() != Some(target_response) {
                    return Err(ContractError::InvalidTarget.into());
                }
                let (position, _) = events.iter().next().ok_or(ContractError::InvalidCut)??;
                after_entry(
                    extras,
                    source,
                    next.state
                        .attachment()
                        .ok_or(ContractError::InvalidTarget)?,
                    position,
                )?;
                work(
                    source.work(*id).ok_or(ContractError::MissingEvidence)?,
                    next,
                    events,
                )?;
            }
            (Key::Response(id), Row::Response(row)) => {
                if *id != target_response {
                    return Err(ContractError::InvalidTarget.into());
                }
                let next = row.record().ok_or(ContractError::MissingEvidence)?;
                let old = source.response(*id).ok_or(ContractError::MissingEvidence)?;
                if old.response().state() == ResponseState::Received {
                    complete_entry(extras, source, next, limits)?;
                }
                response(
                    old,
                    next,
                    source
                        .claim(next.response().identity().claim)
                        .ok_or(ContractError::InvalidTarget)?,
                    events,
                )?;
            }
            (Key::Evaluation(key), Row::Evaluation(row)) => {
                let state = row.get().ok_or(ContractError::MissingEvidence)?;
                let old = source
                    .evaluation(*key)
                    .ok_or(ContractError::MissingEvidence)?;
                let definition = source
                    .definition(key.validation)
                    .ok_or(ContractError::InvalidPolicy)?;
                if matches!(key.target, EvaluationTarget::Work { .. }) {
                    let response = work_target(source, *key, old, definition)?;
                    let (position, fact) = events.only()?;
                    if matches!(
                        fact,
                        NativeFact::Evaluation {
                            kind: NativeEvaluationEventKind::Reported,
                            ..
                        }
                    ) {
                        after_entry(extras, source, response, position)?;
                    } else if source
                        .response(response)
                        .and_then(NativeResponseRecord::entered)
                        .is_none()
                    {
                        let entry = entered(extras, source, response)?;
                        if entry.sequence != sequence || entry <= position {
                            return Err(ContractError::InvalidCut.into());
                        }
                    } else {
                        after_entry(extras, source, response, position)?;
                    }
                    external_evaluation(*key, old, state, definition, events)?;
                    if let Some(result) = state.last_result() {
                        let accepted =
                            as_result(staged(extras, Key::Accepted(NativeResultKey::of(result))))
                                .ok_or(ContractError::MissingEvidence)?;
                        if accepted.result() != result {
                            return Err(ContractError::MissingEvidence.into());
                        }
                    }
                    continue;
                }
                let EvaluationTarget::MissingSlot { response, .. } = key.target else {
                    return Err(ContractError::InvalidTarget.into());
                };
                if response != target_response {
                    return Err(ContractError::InvalidTarget.into());
                }
                after_entry(extras, source, response, events.only()?.0)?;
                missing_evaluation(*key, old, state, definition, events)?;
                if let Some(result) = state.last_result() {
                    let missing = extras
                        .rows
                        .binary_search_by_key(
                            &Key::MissingResult(NativeResultKey::of(result)),
                            |row| row.key,
                        )
                        .ok()
                        .and_then(|at| extras.rows.get(at))
                        .and_then(|row| super::response_reads::as_missing(Some(&row.row)))
                        .ok_or(ContractError::MissingEvidence)?;
                    if missing.result() != result {
                        return Err(ContractError::MissingEvidence.into());
                    }
                }
            }
            (Key::MissingResult(key), Row::MissingResult(row)) => {
                let (position, fact) = events.only()?;
                let result = row.get().ok_or(ContractError::MissingEvidence)?;
                if source.missing(*key).is_some()
                    || fact != (NativeFact::Missing { key: *key })
                    || NativeResultKey::of(result.result()) != *key
                    || result.sequence() != position.sequence
                    || result.ordinal() != position.ordinal
                {
                    return Err(ContractError::InvalidCut.into());
                }
                let evaluated = extras
                    .rows
                    .binary_search_by_key(&Key::Evaluation(key.evaluation), |row| row.key)
                    .ok()
                    .and_then(|at| extras.rows.get(at))
                    .and_then(|row| as_evaluation(Some(&row.row)))
                    .ok_or(ContractError::MissingEvidence)?;
                if evaluated.last_result() != Some(result.result()) {
                    return Err(ContractError::MissingEvidence.into());
                }
                let evaluation_at = index
                    .binary_search_by_key(&Key::Evaluation(key.evaluation), |(key, _)| *key)
                    .ok()
                    .and_then(|at| index.get(at))
                    .map(|(_, ordinal)| *ordinal)
                    .ok_or(ContractError::InvalidCut)?;
                let EvaluationTarget::MissingSlot { response, .. } = key.evaluation.target else {
                    return Err(ContractError::InvalidTarget.into());
                };
                after_entry(
                    extras,
                    source,
                    response,
                    PublicationPosition {
                        sequence,
                        ordinal: u32::try_from(evaluation_at)
                            .map_err(|_| ContractError::Capacity)?,
                    },
                )?;
                if evaluation_at
                    .checked_add(1)
                    .ok_or(ContractError::Capacity)?
                    != usize::try_from(position.ordinal).map_err(|_| ContractError::Capacity)?
                {
                    return Err(ContractError::InvalidCut.into());
                }
            }
            (Key::Artifact(id), Row::Artifact(row)) => {
                let (position, fact) = events.only()?;
                let artifact = row.get().ok_or(ContractError::MissingEvidence)?;
                let descriptor = artifact.descriptor();
                if source.artifact(*id).is_some()
                    || descriptor.id() != *id
                    || fact
                        != (NativeFact::Artifact {
                            binding: descriptor.binding(),
                        })
                    || position.ordinal != 0
                    || !matches!(staged(extras, Key::ArtifactIdentity(descriptor.content_hash())),
                        Some(Row::ArtifactIdentity(recorded)) if recorded == id)
                {
                    return Err(ContractError::MissingEvidence.into());
                }
                let NativeFact::Accepted { key } =
                    journal.get(2).copied().ok_or(ContractError::InvalidCut)?
                else {
                    return Err(ContractError::InvalidCut.into());
                };
                let accepted = as_result(staged(extras, Key::Accepted(key)))
                    .ok_or(ContractError::MissingEvidence)?;
                if accepted.result().evidence()
                    != Some(ArtifactRef {
                        id: *id,
                        hash: descriptor.content_hash(),
                    })
                {
                    return Err(ContractError::MissingEvidence.into());
                }
            }
            (Key::Accepted(key), Row::Accepted(row)) => {
                accepted(
                    extras,
                    source,
                    *key,
                    row.get().ok_or(ContractError::MissingEvidence)?,
                    events,
                )?;
            }
            _ => return Err(ContractError::InvalidTarget.into()),
        }
    }
    if cursor != index.len() || index_rows != validated_index_rows {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(())
}
