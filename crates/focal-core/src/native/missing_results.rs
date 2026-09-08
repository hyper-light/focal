//! Structural MissingSlot settlement at the actual response-entry publication.
//! Existing Ready rows are resolved from the complete retained registry. No
//! evaluator attempt, evidence artifact or replacement cohort is manufactured.
use super::missing_owned::{NativeMissingResult, OwnedMissingResult};
use super::prepare::{Extra, Extras, Scratch, add, within};
use super::*;
use focal_model::ValidationMode;
use focal_model::lifecycle::aggregation::{ObligationTarget, PublicationPosition};
use focal_model::lifecycle::evidence::ResponseEntry;

fn visit(remaining: &mut usize) -> Result<(), NativeError> {
    *remaining = remaining
        .checked_sub(1)
        .ok_or(NativeError::Capacity("missing target visits"))?;
    Ok(())
}

/// `response` is the owner's checked Validating overlay and `entry` is its
/// already-staged leading event. Every earlier event must be counted by Extras;
/// the caller discards all provisional rows, metadata and scratch on failure.
/// Returns the number of changed evaluations, including Observe suppressions.
#[allow(clippy::too_many_arguments)] // One exact borrowed owner transaction, no authored permission flags.
pub(super) fn prepare(
    view: &View<'_>,
    claim: &ClaimState,
    response: &Response,
    authority: &ResponseEntry,
    entry: PublicationPosition,
    registry: &RegistrationSet,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<usize, NativeError> {
    authority.check(claim, response)?;
    registry.check(claim)?;
    within(registry.rows().len(), limits.evaluations_per_claim)?;
    if response.state() != ResponseState::Validating
        || entry.sequence.0 != add_sequence(view.prefix())?
        || usize::try_from(entry.ordinal)
            .map_err(|_| NativeError::Capacity("missing target entry ordinal"))?
            >= extras.events()
    {
        return Err(ContractError::InvalidCut.into());
    }
    let mut visits = limits.plan_edges;
    // A missing/substituted definition must not silently shrink the cohort.
    // Definition checks themselves traverse the immutable admitted summaries.
    let declarations = claim.acceptance().declarations();
    for summary in declarations {
        visit(&mut visits)?;
        visits = visits
            .checked_sub(declarations.len())
            .ok_or(NativeError::Capacity("missing target definition visits"))?;
        let definition = view.definition(ValidationId(summary.binding().object.0))?;
        claim.acceptance().check_declaration(definition)?;
    }
    let id = ClaimId(claim.binding().object.0);
    let identity = response.identity();
    let mut changed = 0usize;
    let mut results = meta.results;
    for summary in declarations {
        visit(&mut visits)?;
        let ObligationTarget::Slot(slot) = summary.target() else {
            continue;
        };
        if response
            .manifest()
            .binary_search_by_key(&slot, |entry| entry.slot)
            .is_ok()
        {
            continue;
        }
        let key = EvaluationKey {
            claim: id,
            validation: ValidationId(summary.binding().object.0),
            target: EvaluationTarget::MissingSlot {
                response: TestamentId(identity.binding.object.0),
                slot,
            },
            generation: u64::from(identity.cycle),
        };
        let mut member = None;
        for registered in registry.rows().iter().copied() {
            visit(&mut visits)?;
            if transactions::key_for_registered(id, registered) == key {
                if member.is_some() {
                    return Err(ContractError::InvalidManifest.into());
                }
                member = Some(registered);
            }
        }
        let registered = member.ok_or(ContractError::MissingEvidence)?;
        let definition = view.definition(key.validation)?;
        let previous = *view.evaluation(key)?;
        registered.check_state(previous, definition)?;
        let evaluation = previous.bind(definition)?;
        let transition =
            evaluation.settle_missing_entered(&previous.binding(), claim, response, authority)?;
        let next = transition.next.into_state();
        if next == previous {
            if transition.result.is_some() {
                return Err(ContractError::InvalidTransition.into());
            }
            continue;
        }
        match (summary.mode(), transition.result) {
            (ValidationMode::Required, Some(_)) => {}
            (ValidationMode::Observe, None)
                if transition.next.suppression()
                    == Some(validation::Suppression::MissingTarget) => {}
            _ => return Err(ContractError::InvalidTransition.into()),
        }
        let result = if let Some(result) = transition.result {
            let key = NativeResultKey::of(result);
            if view.get(Key::MissingResult(key)).is_some() {
                return Err(ContractError::StaleEvaluation.into());
            }
            transactions::increment(&mut results, 1, limits.results, "results")?;
            Some(result)
        } else {
            None
        };
        scratch.charge(OwnedEvaluation::container_charge())?;
        let row = OwnedEvaluation::new(next)?;
        let heap = row.heap_charge()?;
        extras.push(Extra {
            key: Key::Evaluation(key),
            row: Row::Evaluation(row),
            heap,
            fact: Some(NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::MissingTarget,
                key,
                before: Some(previous.binding()),
                after: next.binding(),
                state: next.state(),
                phase: validation::Phase::MissingTarget,
                attempt: None,
                fence: next.fence(),
            }),
        })?;
        if let Some(result) = result {
            let ordinal = u32::try_from(extras.events())
                .map_err(|_| NativeError::Capacity("missing result publication ordinal"))?;
            let result = NativeMissingResult::new(result, entry.sequence, ordinal)?;
            let key = NativeResultKey::of(result.result());
            scratch.charge(OwnedMissingResult::container_charge())?;
            let row = OwnedMissingResult::new(result)?;
            let heap = row.heap_charge()?;
            extras.push(Extra {
                key: Key::MissingResult(key),
                row: Row::MissingResult(row),
                heap,
                fact: Some(NativeFact::Missing { key }),
            })?;
        }
        changed = add(changed, 1)?;
    }
    meta.results = results;
    Ok(changed)
}

fn add_sequence(prefix: SessionSeq) -> Result<u64, NativeError> {
    prefix
        .0
        .checked_add(1)
        .ok_or(NativeError::Capacity("missing result publication sequence"))
}

#[cfg(test)]
#[path = "missing_results_tests.rs"]
mod tests;
