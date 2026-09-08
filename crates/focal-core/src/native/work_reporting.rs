//! WholeWork uses the shared checked evidence/result writer, followed by one
//! complete owner projection. Ordinary terminal parent outcomes retain begun
//! report authority without recreating a claim or rewriting its original cut.
use super::prepare::{Extras, Scratch};
use super::*;
use focal_model::lifecycle::claim::ClaimCut;

#[allow(clippy::too_many_arguments)] // Exact internal report frame and owner staging context.
pub(super) fn prepare(
    claim: Binding,
    key: EvaluationKey,
    expected: Binding,
    report: validation::Report,
    artifact: NativeArtifactInput,
    request: RequestKey,
    evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
    context: NativeContext,
    cut: ClaimCut,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let EvaluationTarget::Work {
        response,
        artifact: target,
        ..
    } = key.target
    else {
        return Err(ContractError::InvalidTarget.into());
    };
    extras.begin_journal(limits.range.max_batch_entries, scratch)?;
    let plan = super::reporting::prepare(
        claim, key, expected, report, artifact, request, evidence, context, cut, view, limits,
        meta, extras, scratch,
    )?;
    if !plan.rows.is_empty() || !plan.registry.is_empty() || plan.created != 0 {
        return Err(ContractError::InvalidTransition.into());
    }
    let parent = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
    super::whole_work::project_effects(
        view,
        parent,
        response,
        Some(target),
        cut,
        limits,
        extras,
        scratch,
    )
}
