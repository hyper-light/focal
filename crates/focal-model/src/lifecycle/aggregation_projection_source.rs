//! Allocation-free validation of one complete effective owner prefix. Every
//! repeated traversal consumes visits before inspecting rows or policy fields.
use super::*;
use crate::lifecycle::evidence::{ResponseState, WorkArtifactState};
use crate::lifecycle::validation::State;

pub(super) struct SourceResponse<'a> {
    pub(super) source: PublishedResponse<'a>,
    pub(super) active: bool,
}

fn position(
    claim: &ClaimState,
    view: &impl WholeWorkView,
    position: PublicationPosition,
) -> Result<(), ContractError> {
    if position.sequence.0 == 0
        || position.sequence < claim.created()
        || position.sequence > view.prefix()
    {
        Err(ContractError::InvalidCut)
    } else {
        Ok(())
    }
}

fn current(current: Binding, original: Binding) -> Result<(), ContractError> {
    same_content(current, original)?;
    if original.revision.0 == 0 || original.revision > current.revision {
        Err(ContractError::StaleRevision)
    } else {
        Ok(())
    }
}

pub(super) fn response<'a>(
    claim: &ClaimState,
    view: &'a impl WholeWorkView,
    id: TestamentId,
    visits: &mut Visits,
) -> Result<SourceResponse<'a>, ContractError> {
    visits.take(1)?;
    let source = view.response(id).ok_or(ContractError::MissingEvidence)?;
    let row = source.response;
    let identity = row.identity();
    if identity.binding.object.0 != id.0 || id.is_zero() || identity.binding.revision.0 == 0 {
        return Err(ContractError::InvalidTarget);
    }
    let history = claim.response_history(row)?;
    let delivered = !matches!(
        row.state(),
        ResponseState::Generated | ResponseState::Posted
    );
    let entered = !matches!(
        row.state(),
        ResponseState::Generated | ResponseState::Posted | ResponseState::Received
    );
    if delivered != source.received.is_some()
        || entered != source.entered.is_some()
        || history.posted() != (row.state() != ResponseState::Generated)
        || (history.received() && !delivered)
        || (entered && !history.received())
        || row.state().is_terminal() != row.terminal().is_some()
    {
        return Err(ContractError::InvalidTransition);
    }
    if let Some(received) = source.received {
        position(claim, view, received)?;
        if history.received()
            && claim
                .local_sealed_at()
                .is_some_and(|sealed| received.sequence > sealed)
        {
            return Err(ContractError::InvalidCut);
        }
        if !history.received() {
            let sealed = claim
                .local_sealed_at()
                .ok_or(ContractError::InvalidTransition)?;
            if (!claim.local_complete() && !claim.is_terminal()) || received.sequence < sealed {
                return Err(ContractError::InvalidCut);
            }
        }
    }
    if let Some(entered) = source.entered {
        position(claim, view, entered)?;
        if source.received.is_none_or(|received| received >= entered)
            || claim
                .local_sealed_at()
                .is_some_and(|sealed| entered.sequence > sealed)
        {
            return Err(ContractError::InvalidCut);
        }
    }
    if let Some(terminal) = row.terminal() {
        let sequence = match terminal {
            ResponseOutcome::Evaluating => return Err(ContractError::InvalidCut),
            ResponseOutcome::Validated { sequence } => sequence,
            ResponseOutcome::Blocked(cut) => cut.sequence(),
        };
        if source
            .entered
            .is_none_or(|entered| sequence < entered.sequence)
            || sequence > view.prefix()
        {
            return Err(ContractError::InvalidCut);
        }
    }
    Ok(SourceResponse {
        source,
        active: history.received()
            && claim.receipt().map(|receipt| receipt.fence) == Some(identity.receipt),
    })
}

fn work_identity(claim: &ClaimState, work: &WorkArtifact) -> Result<(), ContractError> {
    if work.binding().ledger != claim.binding().ledger {
        return Err(ContractError::WrongLedger);
    }
    if work.claim().0 != claim.binding().object.0 {
        return Err(ContractError::WrongObject);
    }
    if work.binding().object.is_zero()
        || work.binding().revision.0 == 0
        || !claim.acceptance().has_slot(work.slot())
        || work.producer().is_zero()
        || work.receipt().receipt.is_zero()
        || work.receipt().epoch == 0
    {
        return Err(ContractError::InvalidTarget);
    }
    let closed = u32::try_from(claim.response_count()).map_err(|_| ContractError::Capacity)?;
    if work.cycle() == 0
        || work.cycle() > claim.max_responses()
        || (work.cycle() > closed && work.cycle().checked_sub(closed) != Some(1))
    {
        return Err(ContractError::InvalidTarget);
    }
    if claim
        .receipt()
        .is_some_and(|receipt| receipt.fence == work.receipt() && receipt.holder != work.producer())
    {
        return Err(ContractError::WrongActor);
    }
    Ok(())
}

fn response_work(
    claim: &ClaimState,
    source: PublishedResponse<'_>,
    view: &impl WholeWorkView,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    let row = source.response;
    let identity = row.identity();
    let mut previous = None;
    for entry in row.manifest() {
        visits.take(1)?;
        if previous.is_some_and(|slot| slot >= entry.slot) {
            return Err(ContractError::InvalidManifest);
        }
        previous = Some(entry.slot);
        let work = view
            .work(entry.artifact.id)
            .ok_or(ContractError::MissingEvidence)?;
        indexed_work(claim, view, work, visits)?;
        work_identity(claim, work)?;
        row.contains(work)?;
        if work.reference() != entry.artifact
            || work.cycle() != identity.cycle
            || work.producer() != row.respondent()
            || !matches!(
                work.state(),
                WorkArtifactState::Attached
                    | WorkArtifactState::Validating
                    | WorkArtifactState::Validated
                    | WorkArtifactState::ValidationFailed
            )
            || (source.entered.is_none() && work.state() != WorkArtifactState::Attached)
            || matches!(
                work.state(),
                WorkArtifactState::Validated | WorkArtifactState::ValidationFailed
            ) != work.terminal().is_some()
        {
            return Err(ContractError::InvalidTarget);
        }
        if let Some((sequence, _)) = work.terminal()
            && (source
                .entered
                .is_none_or(|entered| entered.sequence > sequence)
                || sequence > view.prefix())
        {
            return Err(ContractError::InvalidCut);
        }
    }
    let mut previous = None;
    for failed in row.failed_work() {
        visits.take(1)?;
        if previous.is_some_and(|slot| slot >= failed.slot())
            || row
                .manifest()
                .binary_search_by_key(&failed.slot(), |entry| entry.slot)
                .is_ok()
        {
            return Err(ContractError::InvalidManifest);
        }
        previous = Some(failed.slot());
        let work = view
            .work(ArtifactId(failed.binding().object.0))
            .ok_or(ContractError::MissingEvidence)?;
        indexed_work(claim, view, work, visits)?;
        work_identity(claim, work)?;
        if work.binding() != failed.binding()
            || work.state() != failed.state()
            || work.diagnostic() != Some(failed.diagnostic())
            || work.slot() != failed.slot()
            || work.receipt() != identity.receipt
            || work.cycle() != identity.cycle
            || work.producer() != row.respondent()
            || work.attachment().is_some()
            || !matches!(
                work.state(),
                WorkArtifactState::GenerationFailed | WorkArtifactState::ReceiptFailed
            )
        {
            return Err(ContractError::InvalidTarget);
        }
    }
    Ok(())
}

pub(super) fn publication<'a>(
    claim: &ClaimState,
    registered: RegisteredEvaluation,
    state: &EvaluationState,
    view: &'a impl WholeWorkView,
    visits: &mut Visits,
) -> Result<Option<PublishedResult<'a>>, ContractError> {
    let Some(last) = state.last_result() else {
        return if state.state().is_terminal() {
            Err(ContractError::MissingEvidence)
        } else {
            Ok(None)
        };
    };
    visits.take(1)?;
    let published = view.accepted(&last).ok_or(ContractError::MissingEvidence)?;
    if *published.result != last {
        return Err(ContractError::ContentConflict);
    }
    if !registered.matches(last)
        || last.binding().revision <= registered.binding().revision
        || last.binding().revision > state.binding().revision
        || last.is_terminal() != state.state().is_terminal()
        || (last.is_terminal() && last.resulting_state() != state.state())
    {
        return Err(ContractError::StaleEvaluation);
    }
    position(claim, view, published.position)?;
    visits.take(claim.acceptance().declarations().len())?;
    claim.acceptance().check_result(last)?;
    match last.phase() {
        Phase::Delivery | Phase::MissingTarget => {
            if state.has_begun()
                || last.attempt().is_some()
                || last.reporter().is_some()
                || last.evidence().is_some()
                || last.programmatic_evidence().is_some()
            {
                return Err(ContractError::InvalidTransition);
            }
            match (
                last.phase(),
                last.target(),
                last.verdict(),
                last.resulting_state(),
            ) {
                (
                    Phase::Delivery,
                    Target::Delivery { .. },
                    VerdictValue::Pass,
                    State::Validated,
                ) => {}
                (
                    Phase::MissingTarget,
                    Target::MissingSlot { .. },
                    VerdictValue::Incomplete,
                    State::ValidationIncomplete,
                ) if last.mode() == ValidationMode::Required => {}
                _ => return Err(ContractError::InvalidTransition),
            }
        }
        Phase::Programmatic | Phase::Quality => {
            if !state.has_begun()
                || last.attempt().is_none()
                || last.reporter().is_none()
                || last.evidence().is_none()
                || matches!(
                    last.target(),
                    Target::Delivery { .. } | Target::MissingSlot { .. }
                )
            {
                return Err(ContractError::MissingEvidence);
            }
        }
    }
    let response_id = match last.target() {
        Target::Delivery { response }
        | Target::Artifact { response, .. }
        | Target::MissingSlot { response, .. } => Some(TestamentId(response.object.0)),
        Target::Admission { .. } | Target::Increment { .. } => None,
    };
    if let Some(id) = response_id {
        let source = response(claim, view, id, visits)?.source;
        let origin = if last.phase() == Phase::Delivery {
            source.received
        } else {
            source.entered
        };
        if origin.is_none_or(|origin| origin >= published.position) {
            return Err(ContractError::InvalidCut);
        }
    }
    Ok(Some(published))
}

fn definition<'a>(
    claim: &ClaimState,
    view: &'a impl WholeWorkView,
    id: ValidationId,
    visits: &mut Visits,
) -> Result<&'a Declaration, ContractError> {
    visits.take(
        claim
            .acceptance()
            .declarations()
            .len()
            .checked_add(1)
            .ok_or(ContractError::Capacity)?,
    )?;
    let definition = view.declaration(id).ok_or(ContractError::MissingEvidence)?;
    if definition.binding().object.0 != id.0 {
        return Err(ContractError::InvalidPolicy);
    }
    claim.acceptance().check_declaration(definition)?;
    Ok(definition)
}

fn member_target(
    claim: &ClaimState,
    registered: RegisteredEvaluation,
    state: &EvaluationState,
    view: &impl WholeWorkView,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    match registered.target() {
        Target::Admission { claim: bound } => {
            current(claim.binding(), bound)?;
            if registered.receipt().is_some() {
                return Err(ContractError::StaleEvaluation);
            }
        }
        Target::Increment {
            claim: bound,
            artifact,
        } => {
            current(claim.binding(), bound)?;
            visits.take(1)?;
            let work = view
                .work(ArtifactId(artifact.object.0))
                .ok_or(ContractError::MissingEvidence)?;
            indexed_work(claim, view, work, visits)?;
            work_identity(claim, work)?;
            current(work.binding(), artifact)?;
            if registered.receipt() != Some(work.receipt())
                || registered.generation() != u64::from(work.cycle())
                || work.state() == WorkArtifactState::GenerationFailed
            {
                return Err(ContractError::StaleEvaluation);
            }
        }
        target @ (Target::Delivery { response: bound }
        | Target::Artifact {
            response: bound, ..
        }
        | Target::MissingSlot {
            response: bound, ..
        }) => {
            let source = response(claim, view, TestamentId(bound.object.0), visits)?.source;
            let row = source.response;
            let history = claim.response_history(row)?;
            current(row.identity().binding, bound)?;
            if !history.received()
                || registered.receipt() != Some(row.identity().receipt)
                || registered.generation() != u64::from(row.identity().cycle)
            {
                return Err(ContractError::StaleEvaluation);
            }
            if !matches!(target, Target::Delivery { .. })
                && (state.has_begun() || state.last_result().is_some())
                && source.entered.is_none()
            {
                return Err(ContractError::InvalidTransition);
            }
            match target {
                Target::Artifact { slot, artifact, .. } => {
                    let entry = row
                        .manifest()
                        .binary_search_by_key(&slot, |entry| entry.slot)
                        .ok()
                        .and_then(|index| row.manifest().get(index))
                        .ok_or(ContractError::InvalidManifest)?;
                    if entry.artifact
                        != (ArtifactRef {
                            id: ArtifactId(artifact.object.0),
                            hash: artifact.content,
                        })
                    {
                        return Err(ContractError::InvalidTarget);
                    }
                    visits.take(1)?;
                    let work = view
                        .work(entry.artifact.id)
                        .ok_or(ContractError::MissingEvidence)?;
                    current(work.binding(), artifact)?;
                    row.contains(work)?;
                }
                Target::MissingSlot { slot, .. } => {
                    if row
                        .manifest()
                        .binary_search_by_key(&slot, |entry| entry.slot)
                        .is_ok()
                    {
                        return Err(ContractError::InvalidManifest);
                    }
                }
                Target::Delivery { .. } => {}
                _ => return Err(ContractError::InvalidTarget),
            }
        }
    }
    Ok(())
}

fn require_member(
    rows: &[RegisteredEvaluation],
    definition: ValidationId,
    target: Target,
    generation: u64,
    receipt: Option<ReceiptFence>,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    let mut found = false;
    for row in rows {
        visits.take(1)?;
        if row.binding().object.0 == definition.0
            && super::super::acceptance::same_target(row.target(), target)
        {
            if found || row.generation() != generation || row.receipt() != receipt {
                return Err(ContractError::InvalidManifest);
            }
            found = true;
        }
    }
    if found {
        Ok(())
    } else {
        Err(ContractError::MissingEvidence)
    }
}

fn response_cohort(
    claim: &ClaimState,
    registry: &RegistrationSet,
    source: PublishedResponse<'_>,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    if !claim.response_history(source.response)?.received() {
        return Ok(());
    }
    let response = source.response;
    for summary in claim.acceptance().declarations() {
        visits.take(1)?;
        let target = match summary.target() {
            ObligationTarget::Delivery => Target::Delivery {
                response: response.identity().binding,
            },
            ObligationTarget::Slot(slot) => match response
                .manifest()
                .binary_search_by_key(&slot, |entry| entry.slot)
                .ok()
                .and_then(|index| response.manifest().get(index))
            {
                Some(entry) => Target::Artifact {
                    response: response.identity().binding,
                    slot,
                    artifact: Binding {
                        ledger: response.identity().binding.ledger,
                        object: crate::ObjectId(entry.artifact.id.0),
                        content: entry.artifact.hash,
                        revision: crate::ObjectRevision(1),
                    },
                },
                None => Target::MissingSlot {
                    response: response.identity().binding,
                    slot,
                },
            },
            ObligationTarget::Admission | ObligationTarget::Increment => continue,
        };
        require_member(
            registry.rows(),
            ValidationId(summary.binding().object.0),
            target,
            u64::from(response.identity().cycle),
            Some(response.identity().receipt),
            visits,
        )?;
    }
    Ok(())
}

fn distinct_position(
    claim: &ClaimState,
    registry: &RegistrationSet,
    view: &impl WholeWorkView,
    before: usize,
    candidate: PublicationPosition,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    for earlier in registry.rows().iter().copied().take(before) {
        visits.take(1)?;
        let state = view
            .evaluation(earlier)
            .ok_or(ContractError::MissingEvidence)?;
        if let Some(last) = state.last_result() {
            visits.take(1)?;
            let old = view.accepted(&last).ok_or(ContractError::MissingEvidence)?;
            if old.position == candidate {
                return Err(ContractError::ConflictingCause);
            }
        }
    }
    let mut next = claim.latest_response().map(|link| link.testament);
    let mut remaining = claim.response_count();
    while let Some(id) = next {
        remaining = remaining
            .checked_sub(1)
            .ok_or(ContractError::InvalidManifest)?;
        let source = response(claim, view, id, visits)?.source;
        if source.received == Some(candidate) || source.entered == Some(candidate) {
            return Err(ContractError::ConflictingCause);
        }
        next = source.response.identity().prior;
    }
    Ok(())
}

pub(super) fn inspect(
    claim: &ClaimState,
    registry: &RegistrationSet,
    view: &impl WholeWorkView,
    limits: ProjectionLimits,
) -> Result<Counts, ContractError> {
    let policy = claim.acceptance();
    let rows = registry.rows();
    let responses = claim.response_count();
    let slots = responses
        .checked_mul(policy.slot_count())
        .ok_or(ContractError::Capacity)?;
    if responses > limits.responses
        || slots > limits.slots
        || rows.len() > limits.evaluations
        || policy.declarations().len() > limits.declarations
    {
        return Err(ContractError::Capacity);
    }
    if claim.created().0 == 0 || claim.created() > view.prefix() {
        return Err(ContractError::InvalidCut);
    }
    let mut visits = Visits::new(limits.visits);
    visits.take(policy.declarations().len())?;
    visits.take(policy.slot_count())?;
    for slot in &policy.slots {
        visits.take(slot.checks.len())?;
    }
    registry.check(claim)?;
    for summary in policy.declarations() {
        definition(
            claim,
            view,
            ValidationId(summary.binding().object.0),
            &mut visits,
        )?;
    }

    let mut next = claim.latest_response().map(|link| link.testament);
    let mut remaining = responses;
    while let Some(id) = next {
        let source = response(claim, view, id, &mut visits)?.source;
        if usize::try_from(source.response.identity().cycle).map_err(|_| ContractError::Capacity)?
            != remaining
        {
            return Err(ContractError::InvalidManifest);
        }
        remaining = remaining
            .checked_sub(1)
            .ok_or(ContractError::InvalidManifest)?;
        response_work(claim, source, view, &mut visits)?;
        response_cohort(claim, registry, source, &mut visits)?;
        let mut prior = source.response.identity().prior;
        let mut earlier = remaining;
        while let Some(id) = prior {
            earlier = earlier
                .checked_sub(1)
                .ok_or(ContractError::InvalidManifest)?;
            let old = response(claim, view, id, &mut visits)?.source;
            if source.received.is_some_and(|position| {
                old.received == Some(position) || old.entered == Some(position)
            }) || source.entered.is_some_and(|position| {
                old.received == Some(position) || old.entered == Some(position)
            }) {
                return Err(ContractError::ConflictingCause);
            }
            prior = old.response.identity().prior;
        }
        next = source.response.identity().prior;
    }
    if remaining != 0 {
        return Err(ContractError::InvalidManifest);
    }

    for (index, registered) in rows.iter().copied().enumerate() {
        let definition = definition(
            claim,
            view,
            ValidationId(registered.binding().object.0),
            &mut visits,
        )?;
        visits.take(1)?;
        let state = view
            .evaluation(registered)
            .ok_or(ContractError::MissingEvidence)?;
        registered.check_state(*state, definition)?;
        member_target(claim, registered, state, view, &mut visits)?;
        for old in rows.iter().take(index) {
            visits.take(1)?;
            if old.binding().object == registered.binding().object
                && super::super::acceptance::same_target(old.target(), registered.target())
            {
                return Err(ContractError::InvalidManifest);
            }
        }
        if let Some(result) = publication(claim, registered, state, view, &mut visits)? {
            distinct_position(claim, registry, view, index, result.position, &mut visits)?;
        }
    }

    // A pre-post Generated/control claim has no Admission members. Every live
    // posted lineage, or an already materialized Admission cohort, is complete.
    visits.take(rows.len())?;
    let admission = matches!(
        claim.status(),
        ClaimStatus::Posted
            | ClaimStatus::Received
            | ClaimStatus::Progressed
            | ClaimStatus::TestamentGenerated
            | ClaimStatus::TestamentAcknowledged
            | ClaimStatus::Validating
    ) || rows
        .iter()
        .any(|row| matches!(row.target(), Target::Admission { .. }));
    if admission {
        for summary in policy.declarations() {
            visits.take(1)?;
            if summary.target() == ObligationTarget::Admission {
                let mut count = 0usize;
                for row in rows {
                    visits.take(1)?;
                    if row.binding().object == summary.binding().object {
                        if !matches!(row.target(), Target::Admission { .. }) {
                            return Err(ContractError::InvalidTarget);
                        }
                        count = count.checked_add(1).ok_or(ContractError::Capacity)?;
                    }
                }
                if count != 1 {
                    return Err(ContractError::MissingEvidence);
                }
            }
        }
    }

    inspect_works(claim, registry, view, limits, &mut visits)?;
    entry_gates(claim, registry, view, &mut visits)?;
    Ok(Counts {
        responses,
        slots,
        results: rows.len(),
        events: responses
            .checked_add(rows.len())
            .ok_or(ContractError::Capacity)?,
    })
}

fn indexed_work(
    claim: &ClaimState,
    view: &impl WholeWorkView,
    expected: &WorkArtifact,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    let mut found = false;
    for result in view.works(ClaimId(claim.binding().object.0)) {
        visits.take(1)?;
        let work = result?;
        if work.binding().object == expected.binding().object {
            if found || work != expected {
                return Err(ContractError::InvalidManifest);
            }
            found = true;
        }
    }
    if found {
        Ok(())
    } else {
        Err(ContractError::MissingEvidence)
    }
}

fn entry_gates(
    claim: &ClaimState,
    registry: &RegistrationSet,
    view: &impl WholeWorkView,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    visits.take(claim.acceptance().declarations().len())?;
    let requires_increment = claim.acceptance().declarations().iter().any(|summary| {
        summary.target() == ObligationTarget::Increment
            && summary.mode() == ValidationMode::Required
    });
    let mut next = claim.latest_response().map(|link| link.testament);
    let mut remaining = claim.response_count();
    while let Some(id) = next {
        remaining = remaining
            .checked_sub(1)
            .ok_or(ContractError::InvalidManifest)?;
        let source = response(claim, view, id, visits)?.source;
        next = source.response.identity().prior;
        if !claim.response_history(source.response)?.received() {
            continue;
        }
        if source.entered.is_some() && requires_increment && !registry.increment_targets_sealed() {
            return Err(ContractError::InvalidTransition);
        }
        for registered in registry.rows().iter().copied() {
            visits.take(1)?;
            if registered.mode() != ValidationMode::Required {
                continue;
            }
            let (before, require_pass) = match registered.target() {
                Target::Admission { .. } => (source.received, true),
                Target::Increment { .. }
                    if registered.receipt() == Some(source.response.identity().receipt) =>
                {
                    (source.entered, false)
                }
                _ => continue,
            };
            let Some(before) = before else {
                continue;
            };
            visits.take(1)?;
            let state = view
                .evaluation(registered)
                .ok_or(ContractError::MissingEvidence)?;
            let result = publication(claim, registered, state, view, visits)?
                .ok_or(ContractError::MissingEvidence)?;
            if !result.result.is_terminal()
                || (require_pass && result.result.verdict() != VerdictValue::Pass)
            {
                return Err(ContractError::InvalidTransition);
            }
            if result.position >= before {
                return Err(ContractError::InvalidCut);
            }
        }
    }
    Ok(())
}

fn inspect_works(
    claim: &ClaimState,
    registry: &RegistrationSet,
    view: &impl WholeWorkView,
    limits: ProjectionLimits,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    let id = ClaimId(claim.binding().object.0);
    for (index, result) in view.works(id).enumerate() {
        visits.take(1)?;
        if index >= limits.works {
            return Err(ContractError::Capacity);
        }
        let work = result?;
        work_identity(claim, work)?;
        visits.take(1)?;
        if view.work(ArtifactId(work.binding().object.0)) != Some(work) {
            return Err(ContractError::ContentConflict);
        }
        for earlier in view.works(id).take(index) {
            visits.take(1)?;
            let earlier = earlier?;
            if earlier.binding().object == work.binding().object
                || (earlier.cycle() == work.cycle() && earlier.slot() == work.slot())
            {
                return Err(ContractError::InvalidManifest);
            }
        }
        let mut next = claim.latest_response().map(|link| link.testament);
        let mut remaining = claim.response_count();
        while let Some(id) = next {
            remaining = remaining
                .checked_sub(1)
                .ok_or(ContractError::InvalidManifest)?;
            let response = response(claim, view, id, visits)?.source.response;
            if response.identity().cycle == work.cycle() {
                visits.take(response.manifest().len())?;
                visits.take(response.failed_work().len())?;
                let present = response
                    .manifest()
                    .iter()
                    .any(|entry| entry.artifact == work.reference());
                let failed = response
                    .failed_work()
                    .iter()
                    .any(|entry| entry.binding() == work.binding());
                if present == failed {
                    return Err(ContractError::InvalidManifest);
                }
                break;
            }
            next = response.identity().prior;
        }
        if work.state() == WorkArtifactState::GenerationFailed {
            continue;
        }
        for summary in claim.acceptance().declarations() {
            visits.take(1)?;
            if summary.target() == ObligationTarget::Increment {
                require_member(
                    registry.rows(),
                    ValidationId(summary.binding().object.0),
                    Target::Increment {
                        claim: claim.binding(),
                        artifact: work.binding(),
                    },
                    u64::from(work.cycle()),
                    Some(work.receipt()),
                    visits,
                )?;
            }
        }
    }
    Ok(())
}
