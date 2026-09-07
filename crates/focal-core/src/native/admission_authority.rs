//! Shared borrowed authority checks before completion funding or custody IO.
//! These capabilities describe the exact effective prefix; they own no row,
//! reservation, execution task or fabricated evidence fact.

use super::*;
use focal_model::VerdictValue;
use focal_model::lifecycle::artifact_descriptor::{ArtifactDescriptor, ResultProvenance};

pub(super) struct Registered<'a> {
    pub parent: &'a ClaimState,
    pub definition: &'a validation::Declaration,
    pub registry: &'a RegistrationSet,
    pub registration_index: usize,
    pub state: &'a validation::EvaluationState,
}

pub(super) fn registered<'a>(
    view: &'a View<'_>,
    claim: Binding,
    key: EvaluationKey,
) -> Result<Registered<'a>, NativeError> {
    if key.target != EvaluationTarget::Admission {
        return Err(ContractError::InvalidTarget.into());
    }
    registered_any(view, claim, key)
}

pub(super) fn registered_any<'a>(
    view: &'a View<'_>,
    claim: Binding,
    key: EvaluationKey,
) -> Result<Registered<'a>, NativeError> {
    if !matches!(
        key.target,
        EvaluationTarget::Admission
            | EvaluationTarget::Increment { .. }
            | EvaluationTarget::Work { .. }
    ) || key.claim.0 != claim.object.0
    {
        return Err(ContractError::InvalidTarget.into());
    }
    let parent = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
    parent.binding().check(&claim)?;
    let definition = view.definition(key.validation)?;
    parent.acceptance().check_declaration(definition)?;
    let state = view.evaluation(key)?;
    let registry = view
        .owned_claim(key.claim)?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    registry.check(parent)?;
    let (registration_index, registered) = registry
        .rows()
        .iter()
        .enumerate()
        .find(|(_, row)| transactions::key_for_registered(key.claim, **row) == key)
        .ok_or(ContractError::InvalidTarget)?;
    registered.check_state(*state, definition)?;
    Ok(Registered {
        parent,
        definition,
        registry,
        registration_index,
        state,
    })
}

pub(super) struct Begun<'a> {
    pub registered: Registered<'a>,
    pub next: validation::EvaluationState,
}

pub(super) fn begin<'a>(
    view: &'a View<'_>,
    context: NativeContext,
    claim: Binding,
    key: EvaluationKey,
    expected: Binding,
    limits: NativeLimits,
) -> Result<Begun<'a>, NativeError> {
    let registered = registered(view, claim, key)?;
    let evaluation = registered.state.bind(registered.definition)?;
    let owner = evaluation.admission_owner(registered.parent, context.logical_time)?;
    let transition = evaluation.begin(context.principal, &expected, &owner)?;
    if transition.result.is_some() {
        return Err(ContractError::InvalidTransition.into());
    }
    super::admission_budget::check(
        registered.parent,
        registered.registry,
        &super::admission_view::AdmissionRows {
            view,
            sequence: view.prefix(),
            claim: key.claim,
            report: None,
        },
        limits,
    )?;
    let next = transition.next.into_state();
    Ok(Begun { registered, next })
}

#[allow(clippy::too_many_arguments)] // Exact borrowed owner/intent frame; no new public configuration.
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
    let registered = registered(view, claim, key)?;
    let evaluation = registered.state.bind(registered.definition)?;
    let owner = evaluation.admission_report_owner(registered.parent, context.logical_time)?;
    authorize_report(
        view, context, registered, expected, report, descriptor, owner, limits,
    )
}

#[allow(clippy::too_many_arguments)] // Borrowed exact owner facts resolved before custody IO.
pub(super) fn authorize_report<'a>(
    view: &'a View<'_>,
    context: NativeContext,
    registered: Registered<'a>,
    expected: Binding,
    report: validation::Report,
    descriptor: &ArtifactDescriptor,
    owner: validation::OwnerState,
    limits: NativeLimits,
) -> Result<(Registered<'a>, validation::ReportAuthorization), NativeError> {
    let evaluation = registered.state.bind(registered.definition)?;
    let key = EvaluationKey::of(
        ClaimId(registered.parent.binding().object.0),
        registered.state,
    );
    // Preserve the native actor check before accepting any participant evidence.
    context
        .principal
        .require_actor(evaluation.current_attempt()?.evaluator)?;
    let authorization =
        evaluation.authorize_report(context.principal, &expected, &owner, report)?;
    if descriptor.ledger() != view.ledger() {
        return Err(ContractError::WrongLedger.into());
    }
    if descriptor.producer() != authorization.attempt().evaluator {
        return Err(ContractError::WrongActor.into());
    }
    if descriptor.receipt() != registered.state.receipt() {
        return Err(ContractError::StaleReceipt.into());
    }
    if matches!(report.value, VerdictValue::Incomplete | VerdictValue::Error)
        && descriptor.kind() != "error"
    {
        return Err(ContractError::MissingEvidence.into());
    }
    if descriptor.id() != report.evidence.id {
        return Err(ContractError::WrongObject.into());
    }
    if descriptor.content_hash() != report.evidence.hash {
        return Err(ContractError::ContentConflict.into());
    }
    if descriptor.schema_hash() != authorization.schema()
        || descriptor.result_provenance()
            != Some(ResultProvenance {
                claim: key.claim,
                validation: key.validation,
                target: registered.state.target(),
                generation: registered.state.generation(),
                attempt: authorization.attempt(),
                value: report.value,
            })
    {
        return Err(ContractError::MissingEvidence.into());
    }
    super::reporting::check_inputs(descriptor, view, limits)?;
    if view.get(Key::Artifact(descriptor.id())).is_some()
        || view
            .get(Key::ArtifactIdentity(descriptor.content_hash()))
            .is_some()
    {
        return Err(ContractError::ContentConflict.into());
    }
    Ok((registered, authorization))
}
