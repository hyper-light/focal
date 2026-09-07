//! One bounded chronological reduction. Immutable terminal result publications
//! and response entries are sufficient; retryable intermediate facts cannot be
//! witnesses and are still validated by the source inspection.
use super::*;
use crate::lifecycle::{claim::ClaimTerminalCut, memory};

const ALLOCATION: usize = 4 * size_of::<usize>();

struct SlotState {
    response: usize,
    policy: usize,
    artifact: Option<ArtifactRef>,
    outcome: ArtifactOutcome,
    terminal: Option<SessionSeq>,
}
struct ResultRow<'a> {
    registered: RegisteredEvaluation,
    publication: Option<PublishedResult<'a>>,
    available: bool,
    response: Option<usize>,
    active: bool,
}
#[derive(Clone, Copy)]
enum EventKind {
    Entry(usize),
    Result(usize),
}
struct Event {
    position: PublicationPosition,
    kind: EventKind,
}
#[derive(Clone, Copy)]
struct Coverage {
    slot: usize,
    sequence: SessionSeq,
}

fn add(a: usize, b: usize) -> Result<usize, ContractError> {
    memory::add(a, b)
}
fn buffer<T>(count: usize) -> Result<usize, ContractError> {
    add(
        memory::array::<T>(count)?,
        if count == 0 { 0 } else { ALLOCATION },
    )
}
fn reserve<T>(count: usize) -> Result<Vec<T>, ContractError> {
    let values = memory::reserve(count)?;
    if buffer::<T>(values.capacity())? > buffer::<T>(count)? {
        return Err(ContractError::Capacity);
    }
    Ok(values)
}
fn push<T>(values: &mut Vec<T>, value: T) -> Result<(), ContractError> {
    if values.len() == values.capacity() {
        return Err(ContractError::Capacity);
    }
    values.push(value);
    Ok(())
}

pub(super) fn construction_charge(
    claim: &ClaimState,
    counts: Counts,
) -> Result<usize, ContractError> {
    let slots = claim.acceptance().slot_count();
    let mut bytes = size_of::<WholeWorkProjection<'_>>();
    for charge in [
        buffer::<ProjectedResponse<'_>>(counts.responses)?,
        buffer::<SlotState>(counts.slots)?,
        buffer::<ResultRow<'_>>(counts.results)?,
        buffer::<Event>(counts.events)?,
        buffer::<BlockingCause>(add(counts.results, counts.slots)?)?,
        buffer::<Option<Coverage>>(slots)?,
        buffer::<ArtifactDecision>(counts.slots)?,
        buffer::<ProjectedWitness>(slots)?,
        buffer::<CheckWitness>(counts.results)?,
        buffer::<NonArtifactWitness>(counts.results)?,
        buffer::<AcceptedResult>(counts.results)?,
    ] {
        bytes = add(bytes, charge)?;
    }
    Ok(bytes)
}

fn response_target(target: Target) -> Option<TestamentId> {
    match target {
        Target::Artifact { response, .. }
        | Target::MissingSlot { response, .. }
        | Target::Delivery { response } => Some(TestamentId(response.object.0)),
        _ => None,
    }
}
fn response_index(rows: &[ProjectedResponse<'_>], target: Target) -> Option<usize> {
    let id = response_target(target)?;
    rows.iter()
        .position(|row| row.source.response.identity().binding.object.0 == id.0)
}
fn final_result<'a>(row: &ResultRow<'a>) -> Option<&'a AcceptedResult> {
    row.publication
        .filter(|published| row.available && published.result.is_terminal())
        .map(|published| published.result)
}
fn obligation_complete(
    claim: &ClaimState,
    registry: &RegistrationSet,
    results: &[ResultRow<'_>],
    target: ObligationTarget,
    passed: bool,
    visits: &mut Visits,
) -> Result<bool, ContractError> {
    for declaration in claim.acceptance().declarations() {
        visits.take(1)?;
        if declaration.mode() != ValidationMode::Required || declaration.target() != target {
            continue;
        }
        if target == ObligationTarget::Increment && !registry.increment_targets_sealed() {
            return Ok(false);
        }
        let mut found = false;
        for row in results {
            visits.take(1)?;
            if !row.active || row.registered.binding().object != declaration.binding().object {
                continue;
            }
            found = true;
            if !final_result(row)
                .is_some_and(|result| !passed || result.verdict() == VerdictValue::Pass)
            {
                return Ok(false);
            }
        }
        // Output existence belongs to WholeWork slot presence. A sealed, fully
        // inspected empty product set has no Increment attempts left to finish.
        if !found && target == ObligationTarget::Admission {
            return Ok(false);
        }
    }
    Ok(true)
}

fn delivered(
    claim: &ClaimState,
    response: usize,
    results: &[ResultRow<'_>],
    visits: &mut Visits,
) -> Result<bool, ContractError> {
    for declaration in claim.acceptance().declarations() {
        visits.take(1)?;
        if declaration.target() != ObligationTarget::Delivery
            || declaration.mode() != ValidationMode::Required
        {
            continue;
        }
        let mut passed = false;
        for row in results {
            visits.take(1)?;
            if row.response == Some(response)
                && row.registered.binding().object == declaration.binding().object
            {
                passed =
                    final_result(row).is_some_and(|result| result.verdict() == VerdictValue::Pass);
                break;
            }
        }
        if !passed {
            return Ok(false);
        }
    }
    Ok(true)
}

fn slot_result<'a>(
    check: &CheckPolicy,
    response: usize,
    results: &[ResultRow<'a>],
    visits: &mut Visits,
) -> Result<Option<&'a AcceptedResult>, ContractError> {
    for row in results {
        visits.take(1)?;
        if row.response == Some(response) && row.registered.binding().object.0 == check.validation.0
        {
            return Ok(final_result(row));
        }
    }
    Err(ContractError::MissingEvidence)
}

fn add_cause(
    causes: &mut Vec<BlockingCause>,
    next: BlockingCause,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    for old in causes.iter() {
        visits.take(1)?;
        if old.key == next.key {
            return if old == &next {
                Ok(())
            } else {
                Err(ContractError::ConflictingCause)
            };
        }
    }
    push(causes, next)
}

struct Mutation<'a> {
    sequence: SessionSeq,
    causes: &'a mut Vec<BlockingCause>,
    visits: &'a mut Visits,
}

fn reduce_response(
    claim: &ClaimState,
    at: usize,
    response: &mut ProjectedResponse<'_>,
    slots: &mut [SlotState],
    results: &[ResultRow<'_>],
    mutation: Mutation<'_>,
) -> Result<(), ContractError> {
    let Mutation {
        sequence,
        causes,
        visits,
    } = mutation;
    response.delivery_ready = delivered(claim, at, results, visits)?;
    let mut response_cause: Option<BlockingCause> = None;
    let mut complete = true;
    let entry = response.source.entered.ok_or(ContractError::InvalidCut)?;
    for state in slots {
        visits.take(1)?;
        if state.response != at {
            continue;
        }
        let policy = claim
            .acceptance()
            .slots
            .get(state.policy)
            .ok_or(ContractError::InvalidPolicy)?;
        let mut current_cause = None;
        if state.artifact.is_none() {
            if policy.mode == ValidationMode::Required {
                complete = false;
                if entry.sequence == sequence {
                    // Prefer the actual MissingSlot check's cause when the same
                    // entry publishes one. Presence still works with zero checks.
                    for check in &policy.checks {
                        visits.take(1)?;
                        if let Some(result) = slot_result(check, at, results, visits)?
                            && let Some(candidate) = cause(result, policy.mode)?
                            && candidate.blocks_parent()
                            && current_cause
                                .is_none_or(|old: BlockingCause| candidate.key < old.key)
                        {
                            current_cause = Some(candidate);
                        }
                    }
                    if current_cause.is_none() {
                        current_cause = Some(missing_cause(
                            TestamentId(response.source.response.identity().binding.object.0),
                            policy,
                        ));
                    }
                }
            }
        } else if state.terminal.is_none() {
            let mut passed = true;
            for check in &policy.checks {
                visits.take(1)?;
                let result = slot_result(check, at, results, visits)?;
                if check.mode == ValidationMode::Required {
                    if result.is_none_or(|result| result.verdict() != VerdictValue::Pass) {
                        passed = false;
                    }
                    if let Some(result) = result
                        && let Some(candidate) = cause(result, policy.mode)?
                        && current_cause.is_none_or(|old: BlockingCause| candidate.key < old.key)
                    {
                        current_cause = Some(candidate);
                    }
                }
            }
            state.outcome = match current_cause {
                Some(cause) => ArtifactOutcome::Blocked(cause),
                None if passed => ArtifactOutcome::Passed,
                None => ArtifactOutcome::Pending,
            };
            if state.outcome != ArtifactOutcome::Pending {
                state.terminal = Some(sequence);
            }
        }
        if policy.mode == ValidationMode::Required && state.outcome != ArtifactOutcome::Passed {
            complete = false;
        }
        if let Some(cause) = current_cause {
            if cause.blocks_parent() && response_cause.is_none_or(|old| cause.key < old.key) {
                response_cause = Some(cause);
            }
            if response.active {
                add_cause(causes, cause, visits)?;
            }
        }
    }
    if response.outcome == ResponseOutcome::Evaluating {
        if let Some(cause) = response_cause {
            response.outcome = ResponseOutcome::Blocked(TerminalCut { sequence, cause });
        } else if complete && response.delivery_ready {
            response.outcome = ResponseOutcome::Validated { sequence };
        }
    }
    Ok(())
}

fn coverage_key(
    state: &SlotState,
    responses: &[ProjectedResponse<'_>],
) -> Result<(TestamentId, ArtifactId, ContentHash), ContractError> {
    let response = responses
        .get(state.response)
        .ok_or(ContractError::InvalidTarget)?;
    let artifact = state.artifact.ok_or(ContractError::MissingEvidence)?;
    Ok((
        TestamentId(response.source.response.identity().binding.object.0),
        artifact.id,
        artifact.hash,
    ))
}

pub(super) fn build<'a, V: WholeWorkView>(
    plan: ProjectionPlan<'a, V>,
) -> Result<WholeWorkProjection<'a>, ContractError> {
    let ProjectionPlan {
        claim,
        registrations,
        view,
        limits,
        counts,
        charge,
    } = plan;
    let mut visits = Visits::new(limits.visits);
    let mut projection = WholeWorkProjection {
        claim,
        sequence: view.prefix(),
        responses: reserve(counts.responses)?,
        artifacts: reserve(counts.slots)?,
        witnesses: reserve(claim.acceptance().slot_count())?,
        checks: reserve(counts.results)?,
        nonartifact: reserve(counts.results)?,
        delivery: None,
        delivery_results: reserve(counts.results)?,
        outcome: AggregateOutcome::Pending,
        increments_ready: false,
        charge,
    };
    let mut slots = reserve::<SlotState>(counts.slots)?;
    let mut results = reserve::<ResultRow<'a>>(counts.results)?;
    let mut events = reserve::<Event>(counts.events)?;
    let mut causes = reserve::<BlockingCause>(add(counts.results, counts.slots)?)?;
    let mut coverage = reserve::<Option<Coverage>>(claim.acceptance().slot_count())?;
    coverage.resize(claim.acceptance().slot_count(), None);
    let mut next = claim.latest_response().map(|link| link.testament);
    while let Some(id) = next {
        visits.take(1)?;
        let row = source::response(claim, view, id, &mut visits)?;
        next = row.source.response.identity().prior;
        push(
            &mut projection.responses,
            ProjectedResponse {
                source: row.source,
                active: row.active,
                started: false,
                outcome: ResponseOutcome::Evaluating,
                delivery_ready: false,
                dirty: false,
                artifacts_start: 0,
                artifacts_len: 0,
            },
        )?;
    }
    projection
        .responses
        .sort_unstable_by_key(|row| row.source.response.identity().binding.object);
    for (index, row) in projection.responses.iter().enumerate() {
        if let Some(position) = row.source.entered {
            push(
                &mut events,
                Event {
                    position,
                    kind: EventKind::Entry(index),
                },
            )?;
        }
        for (policy, slot) in claim.acceptance().slots.iter().enumerate() {
            visits.take(1)?;
            let artifact = row
                .source
                .response
                .manifest()
                .binary_search_by_key(&slot.slot, |entry| entry.slot)
                .ok()
                .and_then(|at| row.source.response.manifest().get(at))
                .map(|entry| entry.artifact);
            push(
                &mut slots,
                SlotState {
                    response: index,
                    policy,
                    artifact,
                    outcome: ArtifactOutcome::Pending,
                    terminal: None,
                },
            )?;
        }
    }
    for registered in registrations.rows().iter().copied() {
        visits.take(1)?;
        let state = view
            .evaluation(registered)
            .ok_or(ContractError::MissingEvidence)?;
        let publication = source::publication(claim, registered, state, view, &mut visits)?;
        visits.take(projection.responses.len())?;
        let response = response_index(&projection.responses, registered.target());
        let active = match registered.target() {
            Target::Admission { .. } => true,
            Target::Increment { .. } => {
                registered.receipt() == claim.receipt().map(|receipt| receipt.fence)
            }
            _ => response
                .and_then(|at| projection.responses.get(at))
                .is_some_and(|row| row.active),
        };
        if let Some(published) = publication {
            push(
                &mut events,
                Event {
                    position: published.position,
                    kind: EventKind::Result(results.len()),
                },
            )?;
        }
        push(
            &mut results,
            ResultRow {
                registered,
                publication,
                response,
                active,
                available: false,
            },
        )?;
    }
    events.sort_unstable_by_key(|event| event.position);
    let mut cursor = 0usize;
    let mut chosen_delivery = None;
    while let Some(first) = events.get(cursor) {
        let sequence = first.position.sequence;
        causes.clear();
        while let Some(event) = events.get(cursor) {
            if event.position.sequence != sequence {
                break;
            }
            visits.take(1)?;
            match event.kind {
                EventKind::Entry(at) => {
                    let response = projection
                        .responses
                        .get_mut(at)
                        .ok_or(ContractError::InvalidTarget)?;
                    response.started = true;
                    response.dirty = true;
                }
                EventKind::Result(at) => {
                    let row = results.get_mut(at).ok_or(ContractError::InvalidTarget)?;
                    row.available = true;
                    if let Some(at) = row.response {
                        projection
                            .responses
                            .get_mut(at)
                            .ok_or(ContractError::InvalidTarget)?
                            .dirty = true;
                    }
                }
            }
            cursor = add(cursor, 1)?;
        }
        for (at, response) in projection.responses.iter_mut().enumerate() {
            visits.take(1)?;
            if response.started && response.dirty {
                reduce_response(
                    claim,
                    at,
                    response,
                    &mut slots,
                    &results,
                    Mutation {
                        sequence,
                        causes: &mut causes,
                        visits: &mut visits,
                    },
                )?;
            }
            response.dirty = false;
        }
        if projection.outcome != AggregateOutcome::Pending
            || claim.local_sealed_at().is_some_and(|cut| sequence > cut)
        {
            continue;
        }
        let mut entered = false;
        for (at, response) in projection.responses.iter().enumerate() {
            visits.take(1)?;
            if response.active && response.started {
                entered = true;
                if response.delivery_ready && chosen_delivery.is_none() {
                    chosen_delivery = Some(at);
                }
            }
        }
        if !entered {
            continue;
        }
        for (at, slot) in slots.iter().enumerate() {
            visits.take(1)?;
            let response = projection
                .responses
                .get(slot.response)
                .ok_or(ContractError::InvalidTarget)?;
            let policy = claim
                .acceptance()
                .slots
                .get(slot.policy)
                .ok_or(ContractError::InvalidPolicy)?;
            if !response.active
                || !response.started
                || !response.delivery_ready
                || policy.mode != ValidationMode::Required
                || slot.outcome != ArtifactOutcome::Passed
            {
                continue;
            }
            let current = coverage
                .get_mut(slot.policy)
                .ok_or(ContractError::InvalidPolicy)?;
            let replace = match *current {
                None => true,
                Some(old) if old.sequence == sequence => {
                    coverage_key(slot, &projection.responses)?
                        < coverage_key(
                            slots.get(old.slot).ok_or(ContractError::InvalidTarget)?,
                            &projection.responses,
                        )?
                }
                Some(_) => false,
            };
            if replace {
                *current = Some(Coverage { slot: at, sequence });
            }
        }
        for row in &results {
            visits.take(1)?;
            if row.active
                && matches!(
                    row.registered.target(),
                    Target::Admission { .. } | Target::Increment { .. }
                )
                && let Some(result) = final_result(row)
                && let Some(cause) = cause(result, ValidationMode::Required)?
            {
                add_cause(&mut causes, cause, &mut visits)?;
            }
        }
        let mut complete = chosen_delivery.is_some();
        for (policy, covered) in claim.acceptance().slots.iter().zip(&coverage) {
            visits.take(1)?;
            if policy.mode == ValidationMode::Required && covered.is_none() {
                complete = false;
            }
        }
        complete &= obligation_complete(
            claim,
            registrations,
            &results,
            ObligationTarget::Admission,
            true,
            &mut visits,
        )?;
        complete &= obligation_complete(
            claim,
            registrations,
            &results,
            ObligationTarget::Increment,
            true,
            &mut visits,
        )?;
        if complete {
            projection.outcome = AggregateOutcome::LocalComplete { sequence };
        } else {
            let mut selected: Option<BlockingCause> = None;
            for cause in &causes {
                visits.take(1)?;
                if !cause.blocks_parent() {
                    continue;
                }
                let uncovered = match cause.key.target {
                    CauseTarget::Admission | CauseTarget::Increment { .. } => true,
                    CauseTarget::Response(_) => match cause.slot {
                        Some(slot) => {
                            visits.take(claim.acceptance().slot_count())?;
                            claim
                                .acceptance()
                                .slots
                                .iter()
                                .position(|policy| policy.slot == slot)
                                .and_then(|at| coverage.get(at))
                                .is_none_or(Option::is_none)
                        }
                        None => chosen_delivery.is_none(),
                    },
                };
                if uncovered && selected.is_none_or(|old| cause.key < old.key) {
                    selected = Some(*cause);
                }
            }
            if let Some(cause) = selected {
                projection.outcome = AggregateOutcome::Blocked(TerminalCut { sequence, cause });
            }
        }
    }
    projection.increments_ready = obligation_complete(
        claim,
        registrations,
        &results,
        ObligationTarget::Increment,
        false,
        &mut visits,
    )?;
    for row in &results {
        visits.take(1)?;
        if row.active
            && matches!(
                row.registered.target(),
                Target::Admission { .. } | Target::Increment { .. }
            )
            && let Some(result) = final_result(row)
        {
            let published = row.publication.ok_or(ContractError::MissingEvidence)?;
            push(
                &mut projection.nonartifact,
                NonArtifactWitness {
                    result: *result,
                    receipt: row.registered.receipt(),
                    sequence: published.position.sequence,
                },
            )?;
        }
    }
    for selected in coverage.iter().flatten() {
        visits.take(1)?;
        let slot = slots
            .get(selected.slot)
            .ok_or(ContractError::InvalidTarget)?;
        let response = projection
            .responses
            .get(slot.response)
            .ok_or(ContractError::InvalidTarget)?;
        let policy = claim
            .acceptance()
            .slots
            .get(slot.policy)
            .ok_or(ContractError::InvalidPolicy)?;
        let start = projection.checks.len();
        for check in &policy.checks {
            visits.take(1)?;
            if check.mode == ValidationMode::Observe {
                continue;
            }
            let passed = slot_result(check, slot.response, &results, &mut visits)?
                .ok_or(ContractError::MissingEvidence)?;
            if passed.verdict() != VerdictValue::Pass {
                return Err(ContractError::ConflictingCause);
            }
            push(
                &mut projection.checks,
                CheckWitness {
                    validation: passed.validation(),
                    declaration_index: passed.declaration_index(),
                    generation: passed.generation(),
                    attempt: passed.attempt(),
                    programmatic_evidence: passed
                        .programmatic_evidence()
                        .filter(|proof| Some(*proof) != passed.evidence()),
                    evidence: passed.evidence(),
                },
            )?;
        }
        push(
            &mut projection.witnesses,
            ProjectedWitness {
                response: TestamentId(response.source.response.identity().binding.object.0),
                slot: policy.slot,
                artifact: slot.artifact.ok_or(ContractError::MissingEvidence)?,
                checks: projection
                    .checks
                    .len()
                    .checked_sub(start)
                    .ok_or(ContractError::Capacity)?,
            },
        )?;
    }
    if let Some(at) = chosen_delivery {
        let response = projection
            .responses
            .get(at)
            .ok_or(ContractError::InvalidTarget)?;
        for row in &results {
            visits.take(1)?;
            if row.response == Some(at)
                && matches!(row.registered.target(), Target::Delivery { .. })
                && row.registered.mode() == ValidationMode::Required
            {
                let result = final_result(row).ok_or(ContractError::MissingEvidence)?;
                push(&mut projection.delivery_results, *result)?;
            }
        }
        projection
            .delivery_results
            .sort_unstable_by_key(|result| result.declaration_index());
        let first = projection
            .delivery_results
            .first()
            .ok_or(ContractError::MissingEvidence)?;
        projection.delivery = Some(DeliveryWitness {
            response: response.source.response.identity().binding,
            receipt: response.source.response.identity().receipt,
            validation: first.validation(),
            result: first.binding(),
        });
    }
    for (at, response) in projection.responses.iter_mut().enumerate() {
        visits.take(1)?;
        response.artifacts_start = projection.artifacts.len();
        if !response.started {
            continue;
        }
        if let Some(original) = response.source.response.terminal()
            && original != response.outcome
        {
            return Err(ContractError::ConflictingCause);
        }
        for slot in &slots {
            visits.take(1)?;
            if slot.response != at {
                continue;
            }
            let Some(artifact) = slot.artifact else {
                continue;
            };
            let work = view
                .work(artifact.id)
                .ok_or(ContractError::MissingEvidence)?;
            if let Some(original) = work.terminal()
                && Some(original) != slot.terminal.map(|sequence| (sequence, slot.outcome))
            {
                return Err(ContractError::ConflictingCause);
            }
            let policy = claim
                .acceptance()
                .slots
                .get(slot.policy)
                .ok_or(ContractError::InvalidPolicy)?;
            push(
                &mut projection.artifacts,
                ArtifactDecision {
                    claim: claim.binding(),
                    response: response.source.response.identity().binding,
                    slot: policy.slot,
                    artifact,
                    outcome: slot.outcome,
                    sequence: slot.terminal.unwrap_or(projection.sequence),
                },
            )?;
        }
        response.artifacts_len = projection
            .artifacts
            .len()
            .checked_sub(response.artifacts_start)
            .ok_or(ContractError::Capacity)?;
    }
    if let Some(ClaimTerminalCut::Required(original)) = claim.terminal_cut()
        && original.cause().key().target != CauseTarget::Admission
        && projection.outcome != AggregateOutcome::Blocked(original)
    {
        return Err(ContractError::ConflictingCause);
    }
    if claim.local_complete()
        && !matches!(projection.outcome, AggregateOutcome::LocalComplete { sequence } if Some(sequence) == claim.local_sealed_at())
    {
        return Err(ContractError::ConflictingCause);
    }
    Ok(projection)
}
