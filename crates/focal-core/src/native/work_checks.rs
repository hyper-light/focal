//! Claimant receipt registers the complete WholeWork cohort against the frozen
//! response manifest. Ready does not begin a check or infer a verdict, including
//! when a required slot is missing. Those consequences belong to checked entry.
use super::prepare::{Extras, Scratch, add, within};
use super::*;
use focal_model::lifecycle::aggregation::{ObligationTarget, RegisteredEvaluation};

pub(super) fn count(claim: &ClaimState, limits: NativeLimits) -> Result<usize, NativeError> {
    let declarations = claim.acceptance().declarations();
    within(declarations.len(), limits.plan_edges)?;
    Ok(declarations
        .iter()
        .filter(|declaration| matches!(declaration.target(), ObligationTarget::Slot(_)))
        .count())
}

#[allow(clippy::too_many_arguments)] // Actual borrowed owner prefix and private candidate staging.
pub(super) fn prepare(
    context: NativeContext,
    view: &View<'_>,
    claim: &ClaimState,
    response: &Response,
    limits: NativeLimits,
    meta: &mut Meta,
    registry: &mut RegistrationSet,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<(), NativeError> {
    context.principal.require_actor(claim.issuer())?;
    super::response_budget::check_registration_capacity(claim, registry, limits)?;
    let additional = count(claim, limits)?;
    let final_count = add(registry.rows().len(), additional)?;
    within(final_count, registry.max_rows())?;
    within(final_count, limits.evaluations_per_claim)?;
    let required_heap = final_count
        .checked_mul(size_of::<RegisteredEvaluation>())
        .ok_or(NativeError::Capacity("WholeWork registration bytes"))?;
    within(required_heap, registry.retained_heap_bytes()?)?;
    let registry_heap = transactions::registry_heap(registry)?;
    let allowance = add(registry_heap, size_of::<RegistrationSet>())?;
    let mut evaluations = meta.evaluations;
    transactions::increment(
        &mut evaluations,
        additional,
        limits.evaluations,
        "evaluations",
    )?;
    for summary in claim.acceptance().declarations() {
        let definition = view.definition(ValidationId(summary.binding().object.0))?;
        claim.acceptance().check_declaration(definition)?;
    }
    let id = ClaimId(claim.binding().object.0);
    let identity = response.identity();
    let cycle = NativeCycleKey {
        claim: id,
        receipt: identity.receipt.receipt,
        epoch: identity.receipt.epoch,
        cycle: identity.cycle,
    };
    if !matches!(view.get(Key::Cycle(cycle)), Some(Row::Cycle(row))
        if row.response == Some(TestamentId(identity.binding.object.0)))
    {
        return Err(ContractError::InvalidManifest.into());
    }
    for summary in claim.acceptance().declarations() {
        let ObligationTarget::Slot(slot) = summary.target() else {
            continue;
        };
        let definition = view.definition(ValidationId(summary.binding().object.0))?;
        let work = match response
            .manifest()
            .binary_search_by_key(&slot, |entry| entry.slot)
        {
            Ok(index) => {
                let entry = response
                    .manifest()
                    .get(index)
                    .ok_or(ContractError::InvalidManifest)?;
                let product =
                    super::increment_authority::work_product(view, id, entry.artifact.id)?;
                Some(&product.state)
            }
            Err(_) => None,
        };
        let ready = validation::Evaluation::materialize_work(
            context.principal,
            definition,
            claim,
            response,
            work,
        )?;
        let state = ready.into_state();
        let key = EvaluationKey::of(id, &state);
        if view.get(Key::Evaluation(key)).is_some() {
            return Err(ContractError::StaleEvaluation.into());
        }
        if !registry.register(claim, &ready, allowance)? {
            return Err(ContractError::StaleEvaluation.into());
        }
        if transactions::registry_heap(registry)? != registry_heap {
            return Err(NativeError::Capacity("WholeWork registry growth"));
        }
        extras.evaluation(id, definition, None, state, scratch)?;
    }
    if registry.rows().len() != final_count {
        return Err(ContractError::InvalidPolicy.into());
    }
    meta.evaluations = evaluations;
    Ok(())
}
