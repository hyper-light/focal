//! Trusted exact-evaluation deadline delivery. This does not authorize an actor,
//! fabricate a report, settle missing evidence, or advance any parent lifecycle.
use super::prepare::{Extras, Scratch, within};
use super::*;
use focal_model::lifecycle::claim::ClaimCut;

/// Immutable checked source and its derived replacement. `begun` identifies an
/// outstanding responsibility that requires the owner's existing funded loan.
#[derive(Debug, Clone, Copy)]
pub(super) struct Resolved {
    pub key: EvaluationKey,
    pub binding: Binding,
    pub begun: bool,
    pub next: validation::EvaluationState,
}

pub(super) fn resolve(
    view: &View<'_>,
    input: NativeDeadlineInput,
    logical_time: u64,
    cut: ClaimCut,
    limits: NativeLimits,
) -> Result<Resolved, NativeError> {
    let key = input.evaluation;
    let claim = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
    let definition = view.definition(key.validation)?;
    claim.acceptance().check_declaration(definition)?;
    let state = view.evaluation(key)?;
    let registry = view
        .owned_claim(key.claim)?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    registry.check(claim)?;
    within(registry.rows().len(), limits.evaluations_per_claim)?;
    within(registry.rows().len(), limits.plan_edges)?;
    let registered = registry
        .rows()
        .iter()
        .find(|row| transactions::key_for_registered(key.claim, **row) == key)
        .ok_or(ContractError::InvalidTarget)?;
    registered.check_state(*state, definition)?;
    let next = state.bind(definition)?.fence_deadline(
        &state.binding(),
        claim,
        input.deadline,
        logical_time,
        cut,
    )?;
    Ok(Resolved {
        key,
        binding: state.binding(),
        begun: state.has_begun() && !state.state().is_terminal() && state.fence().is_none(),
        next,
    })
}

pub(super) fn prepare(
    view: &View<'_>,
    resolved: Resolved,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    if resolved.next.binding() != resolved.binding {
        resolved.binding.next()?.check(&resolved.next.binding())?;
        let definition = view.definition(resolved.key.validation)?;
        extras.evaluation(
            resolved.key.claim,
            definition,
            Some(resolved.binding),
            resolved.next,
            scratch,
        )?;
    }
    Ok(transactions::Plan {
        rows: Vec::new(),
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    })
}
