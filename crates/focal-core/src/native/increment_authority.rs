//! Increment authority comes from the exact registered work product and receipt.
//! Neither the participant's target selection nor an artifact payload grants it.
use super::admission_authority::{Begun, Registered};
use super::report_artifact::ArtifactView;
use super::*;
use focal_model::lifecycle::artifact_descriptor::ArtifactDescriptor;

/// A funded attempt must be able to carry its target's mandatory restrictions
/// on at least a content-backed diagnostic. Check before promising any report.
pub(super) fn check_completion_target(
    view: &View<'_>,
    registered: &Registered<'_>,
    envelope: &super::completion_envelope::CompletionEnvelope,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let key = EvaluationKey::of(
        ClaimId(registered.parent.binding().object.0),
        registered.state,
    );
    let source = work(view, key)?;
    let source = as_artifact(view.get(Key::Artifact(source.state.reference().id)))
        .ok_or(ContractError::MissingEvidence)?;
    check_completion_visibility(
        source.descriptor(),
        envelope.descriptor_limits(),
        limits.plan_edges,
    )
}

pub(super) fn check_completion_visibility(
    source: &ArtifactDescriptor,
    limits: focal_model::lifecycle::artifact_descriptor::Limits,
    visits: usize,
) -> Result<(), NativeError> {
    check_completion_visibility_view(source, limits, visits)
}

pub(super) fn check_completion_visibility_view(
    source: &impl ArtifactView,
    limits: focal_model::lifecycle::artifact_descriptor::Limits,
    visits: usize,
) -> Result<(), NativeError> {
    use super::prepare::{add, within};
    let count = source.visibility_count();
    within(count, visits)?;
    within(count, limits.visibility_labels)?;
    within("error".len(), limits.kind_bytes)?;
    // ArtifactLimits excludes allocator metadata; the envelope separately
    // precharges it for every possible descriptor buffer.
    let slots = count
        .checked_mul(size_of::<String>())
        .ok_or(NativeError::Capacity("increment visibility slots"))?;
    let mut bytes = add("error".len(), slots)?;
    for label in source.visibility() {
        let label = label?;
        within(label.len(), limits.visibility_label_bytes)?;
        bytes = add(bytes, label.len())?;
    }
    within(
        add(size_of::<ArtifactDescriptor>(), bytes)?,
        limits.construction_bytes,
    )
}

pub(super) fn work<'a>(
    view: &'a View<'_>,
    key: EvaluationKey,
) -> Result<&'a NativeWork, NativeError> {
    let EvaluationTarget::Increment { artifact } = key.target else {
        return Err(ContractError::InvalidTarget.into());
    };
    work_product(view, key.claim, artifact)
}

/// Resolve the actual submitted product independently of its check family.
/// Both Increment and WholeWork retain this immutable source and provenance.
pub(super) fn work_product<'a>(
    view: &'a View<'_>,
    claim: ClaimId,
    artifact: ArtifactId,
) -> Result<&'a NativeWork, NativeError> {
    let work = as_work(view.get(Key::Work(artifact))).ok_or(ContractError::InvalidTarget)?;
    let cycle = NativeCycleKey {
        claim: work.state.claim(),
        receipt: work.state.receipt().receipt,
        epoch: work.state.receipt().epoch,
        cycle: work.state.cycle(),
    };
    if !matches!(view.get(Key::WorkSlot(cycle,work.state.slot())),Some(Row::WorkSlot(id)) if *id == artifact)
        || !matches!(view.get(Key::Cycle(cycle)),Some(Row::Cycle(row)) if row.work_count != 0)
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let source =
        as_artifact(view.get(Key::Artifact(artifact))).ok_or(ContractError::MissingEvidence)?;
    let descriptor = source.descriptor();
    if work.state.claim() != claim
        || descriptor.binding().ledger != work.state.binding().ledger
        || descriptor.id() != work.state.reference().id
        || descriptor.content_hash() != work.state.reference().hash
        || descriptor.receipt() != Some(work.state.receipt())
        || descriptor.producer() != work.state.producer()
        || descriptor.work_provenance()
            != Some(
                focal_model::lifecycle::artifact_descriptor::WorkProvenance {
                    claim,
                    cycle: work.state.cycle(),
                    role: focal_model::lifecycle::artifact_descriptor::WorkRole::Output {
                        slot: work.state.slot(),
                    },
                },
            )
    {
        return Err(ContractError::MissingEvidence.into());
    }
    Ok(work)
}

pub(super) fn begin<'a>(
    view: &'a View<'_>,
    context: NativeContext,
    claim: Binding,
    key: EvaluationKey,
    expected: Binding,
    _limits: NativeLimits,
) -> Result<Begun<'a>, NativeError> {
    let registered = super::admission_authority::registered_any(view, claim, key)?;
    let work = work(view, key)?;
    if registered.registry.is_sealed() {
        return Err(ContractError::InvalidTransition.into());
    }
    let evaluation = registered.state.bind(registered.definition)?;
    let owner = evaluation.increment_owner(registered.parent, &work.state, context.logical_time)?;
    let transition = evaluation.begin(context.principal, &expected, &owner)?;
    if transition.result.is_some() {
        return Err(ContractError::InvalidTransition.into());
    }
    Ok(Begun {
        registered,
        next: transition.next.into_state(),
    })
}

pub(super) fn report_owner(
    view: &View<'_>,
    registered: &Registered<'_>,
    time: u64,
) -> Result<validation::OwnerState, NativeError> {
    let key = EvaluationKey::of(
        ClaimId(registered.parent.binding().object.0),
        registered.state,
    );
    let work = work(view, key)?;
    Ok(registered
        .state
        .bind(registered.definition)?
        .increment_report_owner(registered.parent, &work.state, time)?)
}

#[allow(clippy::too_many_arguments)] // Exact borrowed target/attempt frame before custody IO.
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
    let registered = super::admission_authority::registered_any(view, claim, key)?;
    let owner = report_owner(view, &registered, context.logical_time)?;
    let source = work(view, key)?;
    let source = as_artifact(view.get(Key::Artifact(source.state.reference().id)))
        .ok_or(ContractError::MissingEvidence)?;
    // Target restrictions are mandatory even when no explicit input list was
    // supplied. A result must not remove the evidence's access restrictions.
    let mut remaining = limits.plan_edges;
    let mut labels = descriptor.visibility();
    for required in source.descriptor().visibility() {
        loop {
            remaining = remaining
                .checked_sub(1)
                .ok_or(NativeError::Capacity("increment evidence visibility"))?;
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
