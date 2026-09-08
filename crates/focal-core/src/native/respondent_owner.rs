//! Respondent report loans use the existing exclusive owner and shared pool.
//! Actual receipt and response state choose the loan before any custody IO.
use super::*;
use crate::native::respondent_envelope::RespondentEnvelope;
use crate::native::respondent_state::{RespondentKey, RespondentRequest, RespondentSpend};
use focal_evidence::NativeVerificationBudget;

pub(super) fn contract(
    view: &View<'_>,
    claim: &ClaimState,
    limits: NativeLimits,
    schemas: &impl NativeSchemaVerifier,
) -> Result<(RespondentEnvelope, NativeVerificationBudget), NativeError> {
    let verification =
        NativeVerificationBudget::for_schema(focal_evidence::error_report_schema(), schemas)?;
    let registry = view
        .owned_claim(ClaimId(claim.binding().object.0))?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    let descriptor = descriptor_limits(limits, claim, registry)?;
    let evidence = EvidenceBounds {
        workspace_bytes: verification
            .peak_bytes()
            .checked_sub(verification.retained_bytes())
            .ok_or(NativeError::Capacity("respondent verification workspace"))?,
        retained_bytes: verification.retained_bytes(),
    };
    Ok((
        RespondentEnvelope::derive(view, claim, limits, descriptor, evidence)?,
        verification,
    ))
}

pub(super) fn select(
    fresh: &prepare::Fresh<'_>,
    book: &CompletionBook,
    schemas: &impl NativeSchemaVerifier,
) -> Result<Option<(RespondentKey, RespondentSpend)>, NativeError> {
    let Some((key, spend)) = fresh.authorize_respondent()? else {
        return Ok(None);
    };
    if let NativeCommand::SubmitDiagnostic { artifact, .. } = &fresh.input()?.command {
        let descriptor = artifact.get().ok_or(ContractError::MissingEvidence)?;
        // Other trusted schemas use Ordinary admission before consulting the
        // pinned mandatory diagnostic verifier. Their actual publication still
        // updates the source-derived reporting credit.
        if descriptor.schema_hash() != focal_evidence::error_report_schema() {
            return Ok(None);
        }
    }
    let loan = book.respondent_contract(key, spend, fresh.view(), schemas)?;
    match &fresh.input()?.command {
        NativeCommand::SubmitDiagnostic { artifact, .. } => {
            loan.envelope().check_descriptor(artifact)?;
        }
        NativeCommand::CloseResponse { report, .. } => {
            loan.envelope().check_response_input(report)?;
        }
        NativeCommand::PostResponse { .. } => {}
        _ => return Err(ContractError::InvalidTransition.into()),
    }
    Ok(Some((key, spend)))
}

fn scalar_selection(
    view: &View<'_>,
    context: NativeContext,
    request: RespondentRequest,
    limits: NativeLimits,
) -> Result<Option<(RespondentKey, RespondentSpend)>, NativeError> {
    let claim = view
        .claim(ClaimId(request.claim().object.0))
        .ok_or(ContractError::InvalidTarget)?;
    super::respondent_state::spend_request(view, claim, context, request, limits)
}

/// Select an existing receipt grant from a checked borrowed artifact. This
/// reads the effective source and its book without allocating or debiting it.
#[allow(clippy::too_many_arguments)] // Exact source, request and held contract before input allocation.
pub(super) fn select_diagnostic(
    view: &View<'_>,
    context: NativeContext,
    claim: Binding,
    reason: EvidenceFailure,
    descriptor: &impl super::report_artifact::ArtifactView,
    book: &CompletionBook,
    schemas: &impl NativeSchemaVerifier,
    limits: NativeLimits,
) -> Result<Option<(RespondentKey, RespondentSpend)>, NativeError> {
    super::work_artifacts::submission_view(
        view,
        context,
        claim,
        focal_model::lifecycle::artifact_descriptor::WorkRole::Diagnostic { reason },
        descriptor,
        limits,
    )?;
    if reason != EvidenceFailure::Work
        || descriptor.fields().schema_hash != focal_evidence::error_report_schema()
    {
        return Ok(None);
    }
    let Some((key, spend)) = scalar_selection(
        view,
        context,
        RespondentRequest::Diagnostic { claim },
        limits,
    )?
    else {
        return Ok(None);
    };
    book.respondent_contract(key, spend, view, schemas)?
        .envelope()
        .check_descriptor_view(descriptor)?;
    Ok(Some((key, spend)))
}

/// Captured response dimensions select capacity; construction verifies the
/// prepared body's identity before the ordinary checked writer may use it.
#[allow(clippy::too_many_arguments)] // Exact source, response identity and held contract before allocation.
pub(super) fn select_response<S: NativeResponseSource>(
    view: &View<'_>,
    context: NativeContext,
    claim: Binding,
    response: Binding,
    report: &NativeResponseSourcePlan<S>,
    book: &CompletionBook,
    schemas: &impl NativeSchemaVerifier,
    limits: NativeLimits,
) -> Result<Option<(RespondentKey, RespondentSpend)>, NativeError> {
    let Some((key, spend)) = scalar_selection(
        view,
        context,
        RespondentRequest::Close { claim, response },
        limits,
    )?
    else {
        return Ok(None);
    };
    book.respondent_contract(key, spend, view, schemas)?
        .envelope()
        .check_response_plan(report)?;
    Ok(Some((key, spend)))
}

#[allow(clippy::too_many_arguments)] // Exact source and stored response identity before input allocation.
pub(super) fn select_post(
    view: &View<'_>,
    context: NativeContext,
    claim: Binding,
    expected: Binding,
    book: &CompletionBook,
    schemas: &impl NativeSchemaVerifier,
    limits: NativeLimits,
) -> Result<Option<(RespondentKey, RespondentSpend)>, NativeError> {
    let Some((key, spend)) = scalar_selection(
        view,
        context,
        RespondentRequest::Post { claim, expected },
        limits,
    )?
    else {
        return Ok(None);
    };
    book.respondent_contract(key, spend, view, schemas)?;
    Ok(Some((key, spend)))
}

#[allow(clippy::too_many_arguments)] // One authenticated private owner loan.
pub(super) fn build(
    fresh: prepare::Fresh<'_>,
    book: &mut CompletionBook,
    evidence: Option<&VerifiedNativeArtifact>,
    custody: Option<(&mut ContentStore, ContentDomainId)>,
    schemas: &impl NativeSchemaVerifier,
    key: RespondentKey,
    spend: RespondentSpend,
) -> Result<(NativePrepared, CandidateJournal), NativeError> {
    let source = fresh.publication_source();
    let loan = book.respondent_contract(key, spend, fresh.view(), schemas)?;
    let verified = match (&fresh.input()?.command, custody) {
        (NativeCommand::SubmitDiagnostic { artifact, .. }, Some((store, domain))) => {
            Some(store.verify_native_artifact_with_budget(
                fresh.input()?.request,
                artifact.get().ok_or(ContractError::MissingEvidence)?,
                domain,
                loan.source(),
                schemas,
                loan.verification(),
            )?)
        }
        _ => None,
    };
    let built = fresh.build_respondent(
        loan.source(),
        verified.as_ref().or(evidence),
        loan.envelope(),
    )?;
    drop(verified);
    let journal = book.apply_prepared(
        &source,
        built.prepared(),
        None,
        None,
        built.seals(),
        JournalFunding::HeldCompletion,
    )?;
    Ok((built.into_prepared(), CandidateJournal::single(journal)))
}

fn install_new(
    view: &View<'_>,
    prepared: &NativePrepared,
    book: &mut CompletionBook,
    limits: NativeLimits,
    schemas: &impl NativeSchemaVerifier,
) -> Result<super::super::completion_book::Journal, NativeError> {
    use super::super::completion_book::Journal;
    if !matches!(
        prepared.outcome.operation,
        NativeOperation::AcquireReceipt | NativeOperation::AdoptReceipt
    ) {
        return Ok(Journal::empty());
    }
    super::super::prepare::within(
        usize::try_from(prepared.outcome.events).map_err(|_| ContractError::Capacity)?,
        limits.plan_edges,
    )?;
    let mut acquired = None;
    for ordinal in 0..prepared.outcome.events {
        let event = CompletionBook::event(prepared, ordinal)?;
        let binding = match event.fact {
            NativeFact::Receipt { claim, .. } | NativeFact::ReceiptAdopted { claim, .. } => claim,
            _ => continue,
        };
        if acquired.replace(binding).is_some() {
            return Err(ContractError::InvalidManifest.into());
        }
    }
    let binding = acquired.ok_or(ContractError::InvalidManifest)?;
    let claim = view
        .claim(ClaimId(binding.object.0))
        .ok_or(ContractError::InvalidTarget)?;
    let (envelope, verification) = contract(view, claim, limits, schemas)?;
    book.install_respondent(view, claim, &envelope, verification)
}

/// Any refusal drops the candidate's pages before restoring its completion
/// credit. No partially attached journal escapes to the pending queue.
pub(super) fn attach(
    source: &View<'_>,
    prepared: NativePrepared,
    journal: CandidateJournal,
    book: &mut CompletionBook,
    limits: NativeLimits,
    schemas: &impl NativeSchemaVerifier,
) -> Result<(NativePrepared, CandidateJournal), NativeError> {
    let operation = prepared.outcome.operation;
    let held = matches!(
        operation,
        NativeOperation::ReportAdmission
            | NativeOperation::ReportIncrement
            | NativeOperation::ReportWork
            | NativeOperation::SubmitDiagnostic
            | NativeOperation::CloseResponse
            | NativeOperation::PostResponse
    );
    let lane = if matches!(
        operation,
        NativeOperation::Cancel
            | NativeOperation::ClaimDeadline
            | NativeOperation::MonitorDeadline
            | NativeOperation::EvaluationDeadline
    ) {
        BudgetLane::Completion
    } else {
        BudgetLane::Ordinary
    };
    let funding = if held {
        JournalFunding::HeldCompletion
    } else {
        JournalFunding::External {
            source: &source.state.budget,
            lane,
        }
    };
    let update = match book.apply_respondents(source, &prepared, funding) {
        Ok(update) => update,
        Err(error) => {
            drop(prepared);
            book.rollback_candidate(journal)?;
            return Err(error);
        }
    };
    let view = View {
        state: source.state,
        tail: Some(&prepared),
    };
    let install = match install_new(&view, &prepared, book, limits, schemas) {
        Ok(install) => install,
        Err(error) => {
            drop(prepared);
            book.rollback(update)?;
            book.rollback_candidate(journal)?;
            return Err(error);
        }
    };
    if let Err(error) = book.check_respondent_composition(&journal, &update, &install) {
        drop(prepared);
        book.rollback(install)?;
        book.rollback(update)?;
        book.rollback_candidate(journal)?;
        return Err(error);
    }
    Ok((prepared, journal.with_respondents(update, install)))
}
