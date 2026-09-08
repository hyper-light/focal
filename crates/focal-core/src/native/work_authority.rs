//! WholeWork authority over retained owner rows, before completion funding or
//! custody IO. Begin borrows an already checked acceptance decision; reports do
//! not allocate a projection or require a later parent outcome to remain open.
use super::admission_authority::{Begun, Registered};
use super::report_artifact::ArtifactView;
use super::*;
use focal_model::lifecycle::{aggregation::ClaimDecision, artifact_descriptor::ArtifactDescriptor};

fn external_target(key: EvaluationKey) -> Result<(), NativeError> {
    if matches!(key.target, EvaluationTarget::Work { .. }) {
        Ok(())
    } else {
        // MissingSlot has no external attempt or invented result artifact.
        Err(ContractError::InvalidTarget.into())
    }
}

fn registered<'a>(
    view: &'a View<'_>,
    claim: Binding,
    key: EvaluationKey,
    limits: NativeLimits,
) -> Result<Registered<'a>, NativeError> {
    external_target(key)?;
    let registry = view
        .owned_claim(key.claim)?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    // Bound the common registration search before it visits any member.
    super::prepare::within(registry.rows().len(), limits.evaluations_per_claim)?;
    super::prepare::within(registry.rows().len(), limits.plan_edges)?;
    super::admission_authority::registered_any(view, claim, key)
}

pub(super) fn completion_target<'a>(
    view: &'a View<'_>,
    registered: &Registered<'_>,
    limits: NativeLimits,
) -> Result<(&'a Response, &'a NativeWork), NativeError> {
    super::prepare::within(
        registered.registry.rows().len(),
        limits.evaluations_per_claim,
    )?;
    super::prepare::within(registered.registry.rows().len(), limits.plan_edges)?;
    let validation::Target::Artifact {
        response: pinned,
        slot,
        artifact,
    } = registered.state.target()
    else {
        return Err(ContractError::InvalidTarget.into());
    };
    let id = TestamentId(pinned.object.0);
    let record = super::response_reads::as_response_record(view.get(Key::Response(id)))
        .ok_or(ContractError::MissingEvidence)?;
    let response = record.response();
    let identity = response.identity();
    let received = record.received().ok_or(ContractError::InvalidTransition)?;
    if received.sequence.0 == 0
        || received.sequence > view.prefix()
        || identity.claim.0 != registered.parent.binding().object.0
        || identity.binding.ledger != registered.parent.binding().ledger
        || identity.binding.object != pinned.object
        || identity.binding.content != pinned.content
        || identity.binding.revision < pinned.revision
        || registered.state.receipt() != Some(identity.receipt)
        || registered.state.generation() != u64::from(identity.cycle)
    {
        return Err(ContractError::InvalidTarget.into());
    }
    match (response.state(), record.entered()) {
        (ResponseState::Received, None) => {}
        (
            ResponseState::Validating
            | ResponseState::Validated
            | ResponseState::ValidationIncomplete
            | ResponseState::ValidationFailed
            | ResponseState::ValidationErrored,
            Some(entered),
        ) if entered > received && entered.sequence <= view.prefix() => {}
        _ => return Err(ContractError::InvalidTransition.into()),
    }
    let cycle = NativeCycleKey {
        claim: identity.claim,
        receipt: identity.receipt.receipt,
        epoch: identity.receipt.epoch,
        cycle: identity.cycle,
    };
    if !matches!(view.get(Key::Cycle(cycle)), Some(Row::Cycle(row))
        if row.response == Some(id) && row.work_count != 0
            && row.work_count <= limits.work_artifacts_per_cycle)
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let work = super::increment_authority::work_product(
        view,
        identity.claim,
        ArtifactId(artifact.object.0),
    )?;
    if work.state.slot() != slot
        || work.state.cycle() != identity.cycle
        || work.state.receipt() != identity.receipt
        || work.state.attachment() != Some(id)
        || work.state.producer() != response.respondent()
    {
        return Err(ContractError::InvalidTarget.into());
    }
    Ok((response, work))
}

/// The sole owner creates `decision` from its complete, precharged effective
/// projection. This helper neither allocates that projection nor executes work.
#[allow(clippy::too_many_arguments)] // Exact private owner frame plus checked acceptance capability.
pub(super) fn begin<'a>(
    view: &'a View<'_>,
    context: NativeContext,
    claim: Binding,
    key: EvaluationKey,
    expected: Binding,
    limits: NativeLimits,
    decision: &ClaimDecision<'_>,
) -> Result<Begun<'a>, NativeError> {
    let registered = registered(view, claim, key, limits)?;
    if registered.registry.is_sealed() {
        return Err(ContractError::InvalidTransition.into());
    }
    let (response, work) = completion_target(view, &registered, limits)?;
    if !matches!(
        response.state(),
        ResponseState::Received | ResponseState::Validating
    ) {
        return Err(ContractError::InvalidTransition.into());
    }
    let evaluation = registered.state.bind(registered.definition)?;
    let owner = evaluation.work_owner(
        registered.parent,
        response,
        Some(&work.state),
        decision,
        context.logical_time,
    )?;
    let transition = evaluation.begin(context.principal, &expected, &owner)?;
    if transition.result.is_some() || !transition.next.has_begun() {
        return Err(ContractError::InvalidTransition.into());
    }
    Ok(Begun {
        registered,
        next: transition.next.into_state(),
    })
}

/// Already-begun attempts keep their original response/work bindings. Ordinary
/// terminal outcomes preserve their evidence authority; model controls, receipt
/// adoption, explicit fences and deadlines still refuse further reports.
pub(super) fn report_owner(
    view: &View<'_>,
    registered: &Registered<'_>,
    time: u64,
    limits: NativeLimits,
) -> Result<validation::OwnerState, NativeError> {
    let (response, work) = completion_target(view, registered, limits)?;
    Ok(registered
        .state
        .bind(registered.definition)?
        .work_report_owner(registered.parent, response, Some(&work.state), time)?)
}

/// Validate the minimum inherited restrictions before accepting responsibility
/// for every future proof or diagnostic in this evaluation's funded chain.
pub(super) fn check_completion_target(
    view: &View<'_>,
    registered: &Registered<'_>,
    envelope: &super::completion_envelope::CompletionEnvelope,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let (_, work) = completion_target(view, registered, limits)?;
    let source = as_artifact(view.get(Key::Artifact(work.state.reference().id)))
        .ok_or(ContractError::MissingEvidence)?;
    super::increment_authority::check_completion_visibility(
        source.descriptor(),
        envelope.descriptor_limits(),
        limits.plan_edges,
    )
}

#[allow(clippy::too_many_arguments)] // Exact report and borrowed owner authority before custody IO.
pub(super) fn report<'a>(
    view: &'a View<'_>,
    context: NativeContext,
    claim: Binding,
    key: EvaluationKey,
    expected: Binding,
    report: validation::Report,
    descriptor: &ArtifactDescriptor,
    limits: NativeLimits,
) -> Result<(Registered<'a>, validation::ReportAuthorization), NativeError> {
    report_view(
        view, context, claim, key, expected, report, descriptor, limits,
    )
}

#[allow(clippy::too_many_arguments)] // Same exact report authority over a prepared borrowed body.
pub(super) fn report_view<'a>(
    view: &'a View<'_>,
    context: NativeContext,
    claim: Binding,
    key: EvaluationKey,
    expected: Binding,
    report: validation::Report,
    descriptor: &impl ArtifactView,
    limits: NativeLimits,
) -> Result<(Registered<'a>, validation::ReportAuthorization), NativeError> {
    let registered = registered(view, claim, key, limits)?;
    let owner = report_owner(view, &registered, context.logical_time, limits)?;
    let (_, work) = completion_target(view, &registered, limits)?;
    let source = as_artifact(view.get(Key::Artifact(work.state.reference().id)))
        .ok_or(ContractError::MissingEvidence)?;
    // Sorted descriptor restrictions permit a bounded merge walk. Explicit
    // input references are optional; target restrictions are mandatory.
    let mut remaining = limits.plan_edges;
    let mut labels = descriptor.visibility();
    for required in source.descriptor().visibility() {
        loop {
            remaining = remaining
                .checked_sub(1)
                .ok_or(NativeError::Capacity("WholeWork evidence visibility"))?;
            match labels.next().transpose()? {
                Some(label) if label < required => continue,
                Some(label) if label == required => break,
                _ => return Err(ContractError::InvalidPolicy.into()),
            }
        }
    }
    super::admission_authority::authorize_report(
        view, context, registered, expected, report, descriptor, owner, limits,
    )
}

#[cfg(test)]
#[path = "work_authority_tests.rs"]
mod tests;

#[cfg(test)]
pub(super) fn history_fixture(present: bool, received: bool) -> Core<NativeState> {
    tests::history_fixture(present, received)
}
