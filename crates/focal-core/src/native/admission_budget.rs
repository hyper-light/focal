//! Qualify the entire Admission cohort's future projection before Begin.
//! This is a bounded-work prerequisite, not a funded completion entitlement.
use super::*;
use focal_model::lifecycle::aggregation;

#[cfg(test)]
#[path = "admission_budget_tests.rs"]
mod tests;

pub(super) fn check(
    claim: &ClaimState,
    registrations: &RegistrationSet,
    view: &impl aggregation::AdmissionView,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    // Reports need nine writes, or eleven when a Required result first makes
    // the Posted parent PostFailed. Begin cannot admit only the success branch.
    // Eleven primary rows plus the smallest report's index rows: a result
    // artifact without inputs, its verdict, and the parent's status move.
    if limits.range.max_batch_entries < super::index_rows::MINIMUM_FAILED_REPORT_ROWS {
        return Err(NativeError::Capacity("admission completion write set"));
    }
    if claim.acceptance().declarations().len() > limits.definitions
        || registrations.rows().len() > limits.evaluations_per_claim
    {
        return Err(ContractError::Capacity.into());
    }
    if aggregation::admission_completion_visits(claim, registrations)? > limits.plan_edges {
        return Err(NativeError::Capacity(
            "admission completion projection visits",
        ));
    }
    // The arithmetic assumes a complete, valid manifest. Resolve all current
    // definitions, registrations, states and retained results through the sole
    // effective owner; a numeric bound cannot validate those capabilities.
    aggregation::project_admission(
        claim,
        registrations,
        view,
        aggregation::AdmissionLimits {
            declarations: limits.definitions,
            evaluations: limits.evaluations_per_claim,
            visits: limits.plan_edges,
        },
    )?;
    Ok(())
}
