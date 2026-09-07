//! Each submitted work output materializes its complete declared Increment
//! cohort in the same candidate. Ready evaluations carry no verdict and do not
//! begin a handler, change artifact acceptance, or close the response cycle.

use super::prepare::{Extras, Scratch, add, within};
use super::*;
use focal_model::lifecycle::{
    aggregation::{ObligationTarget, RegisteredEvaluation},
    evidence::{Parent, WorkArtifact},
};

/// The immutable policy supplies the complete cohort, including Observe checks.
/// The caller includes these rows/events in SubmitWork shape and copy preflight.
pub(super) fn count(claim: &ClaimState, limits: NativeLimits) -> Result<usize, NativeError> {
    let declarations = claim.acceptance().declarations();
    within(declarations.len(), limits.plan_edges)?;
    Ok(declarations
        .iter()
        .filter(|declaration| declaration.target() == ObligationTarget::Increment)
        .count())
}

/// Registry, metadata and extra rows are private staging owned by this one
/// candidate. The caller discards them together on any error and publishes them
/// only with the original Generated work output and its cycle/slot membership.
#[allow(clippy::too_many_arguments)] // Exact internal owner frame, no participant permission flags.
pub(super) fn prepare(
    context: NativeContext,
    view: &View<'_>,
    claim: &ClaimState,
    work: &WorkArtifact,
    limits: NativeLimits,
    meta: &mut Meta,
    registry: &mut RegistrationSet,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<(), NativeError> {
    let parent = Parent::from_claim(claim)?;
    parent.require_open_response()?;
    context.principal.require_actor(parent.holder)?;
    registry.check(claim)?;
    super::response_budget::check_registration_capacity(claim, registry, limits)?;
    let additional = count(claim, limits)?;
    let final_count = add(registry.rows().len(), additional)?;
    within(final_count, registry.max_rows())?;
    within(final_count, limits.evaluations_per_claim)?;
    // The owner reserves and copies this full expanded buffer exactly once.
    // Insufficient prepaid capacity must refuse before any register call can
    // attempt growth; a geometric reallocation would invalidate that admission.
    let required_heap = final_count
        .checked_mul(size_of::<RegisteredEvaluation>())
        .ok_or(NativeError::Capacity("increment registration bytes"))?;
    within(required_heap, registry.retained_heap_bytes()?)?;
    let registry_heap = transactions::registry_heap(registry)?;
    let registry_allowance = add(registry_heap, size_of::<RegistrationSet>())?;
    let mut evaluations = meta.evaluations;
    transactions::increment(
        &mut evaluations,
        additional,
        limits.evaluations,
        "evaluations",
    )?;

    // Resolve every retained definition before staging a member. A missing or
    // substituted definition cannot silently shrink the requested cohort.
    for summary in claim.acceptance().declarations() {
        let definition = view.definition(ValidationId(summary.binding().object.0))?;
        claim.acceptance().check_declaration(definition)?;
    }
    let id = ClaimId(claim.binding().object.0);
    for summary in claim.acceptance().declarations() {
        if summary.target() != ObligationTarget::Increment {
            continue;
        }
        let definition = view.definition(ValidationId(summary.binding().object.0))?;
        // Declaration materialization is an owner consequence of the actual
        // authorized submission. Deriving this issuer role creates Ready only;
        // it does not authorize the respondent to begin or report a check.
        let ready = validation::Evaluation::materialize_increment(
            Principal::Actor(claim.issuer()),
            definition,
            claim,
            work,
        )?;
        let state = ready.into_state();
        let key = EvaluationKey::of(id, &state);
        if view.get(Key::Evaluation(key)).is_some() {
            return Err(ContractError::StaleEvaluation.into());
        }
        if !registry.register(claim, &ready, registry_allowance)? {
            return Err(ContractError::StaleEvaluation.into());
        }
        if transactions::registry_heap(registry)? != registry_heap {
            return Err(NativeError::Capacity("increment registry growth"));
        }
        extras.evaluation(id, definition, None, state, scratch)?;
    }
    if registry.rows().len() != final_count {
        return Err(ContractError::InvalidPolicy.into());
    }
    meta.evaluations = evaluations;
    Ok(())
}
