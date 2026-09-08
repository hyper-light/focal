//! Explicit claimant entry and its complete deterministic object consequences.
//! Every model step is journaled; only final rows enter the atomic range write.
use super::prepare::{Extra, Extras, Scratch, heap};
use super::*;
use aggregation::PublicationPosition;
use focal_model::lifecycle::{aggregation, claim::ClaimCut};

fn position(extras: &Extras, cut: ClaimCut) -> Result<PublicationPosition, NativeError> {
    Ok(PublicationPosition {
        sequence: cut.position,
        ordinal: u32::try_from(extras.events())
            .map_err(|_| NativeError::Capacity("whole-work event ordinal"))?,
    })
}
fn copy_claim(claim: &ClaimState, scratch: &mut Scratch) -> Result<ClaimState, NativeError> {
    scratch.charge(heap(claim)?)?;
    Ok(claim.try_copy(claim.retained_bytes()?)?)
}
fn claim_event(
    extras: &mut Extras,
    before: Binding,
    claim: &ClaimState,
    kind: NativeEventKind,
) -> Result<(), NativeError> {
    if before == claim.binding() {
        return Ok(());
    }
    extras.record(NativeFact::Claim(NativeClaimEvent {
        graph: None,
        kind,
        owned_child: None,
        before: Some(before),
        after: claim.binding(),
        status: claim.status(),
    }))
}
fn work_extra(
    before: Binding,
    work: NativeWork,
    scratch: &mut Scratch,
) -> Result<Extra, NativeError> {
    let fact = NativeFact::Work {
        claim: work.state.claim(),
        before: Some(before),
        after: work.state.binding(),
        state: work.state.state(),
    };
    let key = Key::Work(work.state.reference().id);
    scratch.charge(OwnedWork::container_charge())?;
    let row = OwnedWork::new(work)?;
    Ok(Extra {
        key,
        heap: row.heap_charge()?,
        row: Row::Work(row),
        fact: Some(fact),
    })
}
fn response_extra(row: OwnedResponse, fact: Option<NativeFact>) -> Result<Extra, NativeError> {
    let response = row.get().ok_or(ContractError::MissingEvidence)?;
    Ok(Extra {
        key: Key::Response(TestamentId(response.identity().binding.object.0)),
        heap: row.heap_charge()?,
        row: Row::Response(row),
        fact,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn enter(
    view: &View<'_>,
    context: NativeContext,
    claim: Binding,
    expected: Binding,
    cut: ClaimCut,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    enter_with(
        view, context, claim, expected, cut, limits, meta, extras, scratch, None,
    )
}

#[allow(clippy::too_many_arguments)]
fn enter_with(
    view: &View<'_>,
    context: NativeContext,
    claim: Binding,
    expected: Binding,
    cut: ClaimCut,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
    evaluator: Option<&validation::Evaluation<'_>>,
) -> Result<transactions::Plan, NativeError> {
    let id = ClaimId(claim.object.0);
    let old = view.claim(id).ok_or(ContractError::InvalidTarget)?;
    old.binding().check(&claim)?;
    if evaluator.is_none() {
        context.principal.require_actor(old.issuer())?;
    }
    if old.local_complete()
        || !matches!(
            old.status(),
            ClaimStatus::TestamentAcknowledged | ClaimStatus::Validating
        )
        || old
            .deadline()
            .is_some_and(|deadline| context.logical_time >= deadline.at)
    {
        return Err(ContractError::InvalidTransition.into());
    }
    let response_id = TestamentId(expected.object.0);
    let source_row = match view.get(Key::Response(response_id)) {
        Some(Row::Response(row)) => row,
        _ => return Err(ContractError::InvalidTarget.into()),
    };
    let source = source_row.get().ok_or(ContractError::MissingEvidence)?;
    source.identity().binding.check(&expected)?;
    if source.identity().claim != id || source.state() != ResponseState::Received {
        return Err(ContractError::InvalidTransition.into());
    }
    let registry = view
        .owned_claim(id)?
        .registrations()
        .ok_or(ContractError::InvalidPolicy)?;
    if evaluator.is_none() {
        extras.begin_journal(limits.range.max_batch_entries, scratch)?;
    }
    let empty = Extras::new(0, 0)?;
    let mut changed = copy_claim(old, scratch)?;
    let transition = super::projection::with_staged(
        view,
        old,
        registry,
        &empty,
        cut.position,
        limits,
        scratch,
        |projection, _| {
            let acceptance = projection.claim_decision();
            let transition = if let Some(evaluation) = evaluator {
                let transition = source.plan_evaluation(&expected, old, evaluation, &acceptance)?;
                changed.observe_evaluation(&claim, evaluation, &acceptance)?;
                transition
            } else {
                let transition =
                    source.plan_begin(&expected, old, context.principal, &acceptance)?;
                if old.status() == ClaimStatus::TestamentAcknowledged {
                    changed.request_evaluation(&claim, context.principal, &acceptance)?;
                }
                transition
            };
            Ok(transition)
        },
    )?;
    let authority = super::projection::with_staged(
        view,
        &changed,
        registry,
        &empty,
        cut.position,
        limits,
        scratch,
        |projection, _| {
            Ok(source.entry_capability(&transition, &changed, &projection.claim_decision())?)
        },
    )?;
    claim_event(extras, claim, &changed, NativeEventKind::Validating)?;
    let entry = position(extras, cut)?;
    scratch.charge(source_row.heap_charge()?)?;
    let entered = source_row.transition(transition, entry)?;
    let response = entered.get().ok_or(ContractError::MissingEvidence)?;
    extras.record(NativeFact::Response {
        claim: id,
        before: Some(expected),
        after: response.identity().binding,
        state: response.state(),
    })?;
    for binding in response.manifest() {
        let old_work = super::increment_authority::work_product(view, id, binding.artifact.id)?;
        let mut next = *old_work;
        next.state = old_work.state.begin_entered(
            &old_work.state.binding(),
            &changed,
            response,
            &authority,
        )?;
        extras.push(work_extra(old_work.state.binding(), next, scratch)?)?;
    }
    // Every derived consequence consumes the exact checked entry capability;
    // no participant identity is manufactured for structural assessment.
    super::missing_results::prepare(
        view, &changed, response, &authority, entry, registry, limits, meta, extras, scratch,
    )?;
    extras.push(response_extra(entered, None)?)?;
    project_effects(
        view,
        &changed,
        response_id,
        None,
        cut,
        limits,
        extras,
        scratch,
    )
}

/// Evaluator entry retains the actual Begin fact before any response/claim
/// observation. It never impersonates the claimant or authors a new testament.
#[allow(clippy::too_many_arguments)]
pub(super) fn begin(
    view: &View<'_>,
    context: NativeContext,
    claim: Binding,
    key: EvaluationKey,
    expected: Binding,
    cut: ClaimCut,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let parent = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
    let registry = view
        .owned_claim(key.claim)?
        .registrations()
        .ok_or(ContractError::InvalidPolicy)?;
    let empty = Extras::new(0, 0)?;
    let begun = super::projection::with_staged(
        view,
        parent,
        registry,
        &empty,
        cut.position,
        limits,
        scratch,
        |projection, _| {
            super::work_authority::begin(
                view,
                context,
                claim,
                key,
                expected,
                limits,
                &projection.claim_decision(),
            )
        },
    )?;
    let (response, _) = super::work_authority::completion_target(view, &begun.registered, limits)?;
    extras.begin_journal(limits.range.max_batch_entries, scratch)?;
    extras.evaluation(
        key.claim,
        begun.registered.definition,
        Some(begun.registered.state.binding()),
        begun.next,
        scratch,
    )?;
    match response.state() {
        ResponseState::Received => {
            let evaluation = begun.next.bind(begun.registered.definition)?;
            enter_with(
                view,
                context,
                claim,
                response.identity().binding,
                cut,
                limits,
                meta,
                extras,
                scratch,
                Some(&evaluation),
            )
        }
        ResponseState::Validating => Ok(transactions::Plan {
            rows: Vec::new(),
            registry: transactions::RegistryOverrides::new(),
            created: 0,
        }),
        _ => Err(ContractError::InvalidTransition.into()),
    }
}

fn staged<'a>(view: &'a View<'_>, extras: &'a Extras, key: Key) -> Option<&'a Row> {
    extras
        .rows
        .iter()
        .find(|row| row.key == key)
        .map(|row| &row.row)
        .or_else(|| view.get(key))
}
fn install(extras: &mut Extras, row: Extra) -> Result<(), NativeError> {
    if extras.rows.iter().any(|old| old.key == row.key) {
        extras.replace(row)
    } else {
        extras.push(row)
    }
}

/// One complete immutable projection drives final object transitions. Entry
/// considers every manifest work row; reports consider their exact target. A
/// terminal or locally complete claim remains untouched while late begun work
/// and response evidence can still reach their own final outcomes.
#[allow(clippy::too_many_arguments)]
pub(super) fn project_effects(
    view: &View<'_>,
    parent: &ClaimState,
    response_id: TestamentId,
    target: Option<ArtifactId>,
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let id = ClaimId(parent.binding().object.0);
    let registry = view
        .owned_claim(id)?
        .registrations()
        .ok_or(ContractError::InvalidPolicy)?;
    let (work_updates, response_transition, accepted_claim) = super::projection::with_staged(
        view,
        parent,
        registry,
        extras,
        cut.position,
        limits,
        scratch,
        |projection, scratch| {
            let response = as_response(staged(view, extras, Key::Response(response_id)))
                .ok_or(ContractError::MissingEvidence)?;
            let decision = projection
                .response_decision(response_id)
                .ok_or(ContractError::InvalidTransition)?;
            let mut updates = scratch.reserve::<NativeWork>(if target.is_some() {
                1
            } else {
                response.manifest().len()
            })?;
            for attached in response.manifest() {
                if target.is_some_and(|id| id != attached.artifact.id) {
                    continue;
                }
                let old = as_work(staged(view, extras, Key::Work(attached.artifact.id)))
                    .ok_or(ContractError::MissingEvidence)?;
                let next = old.state.apply_decision(&old.state.binding(), &decision)?;
                if next != old.state {
                    if updates.len() == updates.capacity() {
                        return Err(ContractError::Capacity.into());
                    }
                    updates.push(NativeWork {
                        state: next,
                        next: old.next,
                    });
                }
            }
            let response_transition =
                response.plan_decision(&response.identity().binding, &decision)?;
            let claim = if parent.is_terminal() || parent.local_complete() {
                None
            } else {
                let mut changed = copy_claim(parent, scratch)?;
                changed.apply_aggregate(&parent.binding(), &projection.claim_decision())?;
                Some(changed)
            };
            Ok((updates, response_transition, claim))
        },
    )?;
    for work in work_updates {
        let before = as_work(staged(view, extras, Key::Work(work.state.reference().id)))
            .ok_or(ContractError::MissingEvidence)?
            .state
            .binding();
        install(extras, work_extra(before, work, scratch)?)?;
    }
    if let Some(transition) = response_transition {
        let row = match staged(view, extras, Key::Response(response_id)) {
            Some(Row::Response(row)) => row,
            _ => return Err(ContractError::MissingEvidence.into()),
        };
        let before = row
            .get()
            .ok_or(ContractError::MissingEvidence)?
            .identity()
            .binding;
        scratch.charge(row.heap_charge()?)?;
        let next = row.transition(transition, position(extras, cut)?)?;
        let response = next.get().ok_or(ContractError::MissingEvidence)?;
        let fact = NativeFact::Response {
            claim: id,
            before: Some(before),
            after: response.identity().binding,
            state: response.state(),
        };
        install(extras, response_extra(next, Some(fact))?)?;
    }
    let rows = if let Some(claim) = accepted_claim {
        if claim.binding() != parent.binding() {
            let kind = match claim.status() {
                ClaimStatus::Validating => NativeEventKind::LocallyComplete,
                ClaimStatus::ValidationIncomplete => NativeEventKind::ValidationIncomplete,
                ClaimStatus::ValidationFailed => NativeEventKind::ValidationFailed,
                ClaimStatus::ValidationErrored => NativeEventKind::ValidationErrored,
                _ => return Err(ContractError::InvalidTransition.into()),
            };
            claim_event(extras, parent.binding(), &claim, kind)?;
        }
        super::graph_effects::prepare(view, claim, cut, limits, extras, scratch)?
    } else {
        Vec::new()
    };
    Ok(transactions::Plan {
        rows,
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    })
}
