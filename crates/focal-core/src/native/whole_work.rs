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
    let id = ClaimId(claim.object.0);
    let old = view.claim(id).ok_or(ContractError::InvalidTarget)?;
    old.binding().check(&claim)?;
    context.principal.require_actor(old.issuer())?;
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
    extras.begin_journal(limits.range.max_batch_entries, scratch)?;
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
            let transition = source.plan_begin(&expected, old, context.principal, &acceptance)?;
            if old.status() == ClaimStatus::TestamentAcknowledged {
                changed.request_evaluation(&claim, context.principal, &acceptance)?;
            }
            Ok(transition)
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
    let parent = evidence::Parent::from_claim(&changed)?;
    for binding in response.manifest() {
        let old_work = super::increment_authority::work_product(view, id, binding.artifact.id)?;
        let mut next = *old_work;
        next.state = old_work.state.begin(
            &old_work.state.binding(),
            &parent,
            context.principal,
            response,
        )?;
        extras.push(work_extra(old_work.state.binding(), next, scratch)?)?;
    }
    // The gate decision still comes from the complete pre-entry source. It only
    // certifies Increment readiness; structural MissingSlot settlement uses the
    // separately checked entered response held by this transaction.
    super::projection::with_staged(
        view,
        &changed,
        registry,
        &empty,
        cut.position,
        limits,
        scratch,
        |projection, scratch| {
            super::missing_results::prepare(
                context,
                view,
                &changed,
                response,
                &projection.claim_decision(),
                entry,
                registry,
                limits,
                meta,
                extras,
                scratch,
            )?;
            Ok(())
        },
    )?;
    extras.push(response_extra(entered, None)?)?;
    // Derive all consequences while the staged input is immutably borrowed.
    // Return checked transitions and owned copies before changing that input.
    let (work_updates, response_transition, accepted_claim) = super::projection::with_staged(
        view,
        &changed,
        registry,
        extras,
        cut.position,
        limits,
        scratch,
        |projection, scratch| {
            let response = super::response_reads::as_response(
                extras
                    .rows
                    .iter()
                    .find(|row| row.key == Key::Response(response_id))
                    .map(|row| &row.row),
            )
            .ok_or(ContractError::MissingEvidence)?;
            let decision = projection
                .response_decision(response_id)
                .ok_or(ContractError::InvalidTransition)?;
            let mut work_updates = scratch.reserve::<NativeWork>(response.manifest().len())?;
            for attached in response.manifest() {
                let old_work = as_work(
                    extras
                        .rows
                        .iter()
                        .find(|row| row.key == Key::Work(attached.artifact.id))
                        .map(|row| &row.row),
                )
                .ok_or(ContractError::MissingEvidence)?;
                let state = old_work
                    .state
                    .apply_decision(&old_work.state.binding(), &decision)?;
                if state != old_work.state {
                    if work_updates.len() == work_updates.capacity() {
                        return Err(ContractError::Capacity.into());
                    }
                    work_updates.push(NativeWork {
                        state,
                        next: old_work.next,
                    });
                }
            }
            let response_transition =
                response.plan_decision(&response.identity().binding, &decision)?;
            let mut accepted_claim = copy_claim(&changed, scratch)?;
            accepted_claim.apply_aggregate(&changed.binding(), &projection.claim_decision())?;
            Ok((work_updates, response_transition, accepted_claim))
        },
    )?;
    for work in work_updates {
        let previous = as_work(
            extras
                .rows
                .iter()
                .find(|row| row.key == Key::Work(work.state.reference().id))
                .map(|row| &row.row),
        )
        .ok_or(ContractError::MissingEvidence)?
        .state
        .binding();
        extras.replace(work_extra(previous, work, scratch)?)?;
    }
    if let Some(transition) = response_transition {
        let at = position(extras, cut)?;
        let row = match extras
            .rows
            .iter()
            .find(|row| row.key == Key::Response(response_id))
            .map(|row| &row.row)
        {
            Some(Row::Response(row)) => row,
            _ => return Err(ContractError::MissingEvidence.into()),
        };
        let before = row
            .get()
            .ok_or(ContractError::MissingEvidence)?
            .identity()
            .binding;
        scratch.charge(row.heap_charge()?)?;
        let next = row.transition(transition, at)?;
        let response = next.get().ok_or(ContractError::MissingEvidence)?;
        let fact = NativeFact::Response {
            claim: id,
            before: Some(before),
            after: response.identity().binding,
            state: response.state(),
        };
        extras.replace(response_extra(next, Some(fact))?)?;
    }
    let kind = match accepted_claim.status() {
        ClaimStatus::Validating => NativeEventKind::LocallyComplete,
        ClaimStatus::ValidationIncomplete => NativeEventKind::ValidationIncomplete,
        ClaimStatus::ValidationFailed => NativeEventKind::ValidationFailed,
        ClaimStatus::ValidationErrored => NativeEventKind::ValidationErrored,
        _ => return Err(ContractError::InvalidTransition.into()),
    };
    claim_event(extras, changed.binding(), &accepted_claim, kind)?;
    let rows = super::graph_effects::prepare(view, accepted_claim, cut, limits, extras, scratch)?;
    Ok(transactions::Plan {
        rows,
        registry: None,
        created: 0,
    })
}
