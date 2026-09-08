//! The claimant freezes target membership; this neither completes an evaluation
//! nor prevents authorized already-registered checks from running to completion.
use super::prepare::{Scratch, heap};
use super::*;

pub(super) fn prepare(
    view: &View<'_>,
    context: NativeContext,
    binding: Binding,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let id = ClaimId(binding.object.0);
    let claim = view.claim(id).ok_or(ContractError::InvalidTarget)?;
    claim.binding().check(&binding)?;
    context.principal.require_actor(claim.issuer())?;
    focal_model::lifecycle::evidence::Parent::from_claim(claim)?.require_open_response()?;
    let source = view
        .owned_claim(id)?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    super::response_budget::check_registration_capacity_in(view, claim, source, limits)?;
    if source.is_sealed() {
        return Err(ContractError::InvalidTransition.into());
    }
    if source.increment_targets_sealed() {
        return Ok(super::work_artifacts::empty_plan());
    }
    // Recheck every registered state against its immutable declaration. The
    // owner seals the complete set, never a caller-selected list of targets.
    super::prepare::within(source.rows().len(), limits.plan_edges)?;
    for row in source.rows() {
        let definition = view.definition(ValidationId(row.binding().object.0))?;
        claim.acceptance().check_declaration(definition)?;
        row.check_state(
            *view.evaluation(transactions::key_for_registered(id, *row))?,
            definition,
        )?;
    }
    let mut registry = transactions::copy_registry(view, claim, limits, scratch)?;
    registry.seal_increment_targets(claim)?;
    let mut rows = scratch.reserve::<ClaimState>(1)?;
    scratch.charge(heap(claim)?)?;
    rows.push(claim.try_copy(claim.retained_bytes()?)?);
    let mut replacements = transactions::RegistryOverrides::new();
    replacements.insert(claim, registry, limits.plan_nodes, scratch)?;
    Ok(transactions::Plan {
        rows,
        registry: replacements,
        created: 0,
    })
}
