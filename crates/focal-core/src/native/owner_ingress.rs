//! Admission of complete borrowed input plans. The owner authenticates identity
//! and the effective prefix, selects an actual held entitlement when eligible,
//! and retains its input allocation through custody and candidate construction.
use super::*;
use crate::native::artifact_intent::{ArtifactCommand, ReportKind};
use crate::native::input_codec::{
    DecodeError, DecodedPlan, DecodedRequest, InputHeader, NativeDecodeLimits,
};
use focal_memory::Reservation;

#[cfg(test)]
#[path = "owner_ingress_tests.rs"]
mod tests;

fn check_frame_header(
    context: NativeContext,
    ledger: LedgerId,
    profile: NativeContentProfile,
    header: InputHeader,
) -> Result<(), DecodeError> {
    if header.ledger != ledger {
        return Err(ContractError::WrongLedger.into());
    }
    if header.profile != profile {
        return Err(ContractError::InvalidPolicy.into());
    }
    let request = header.request.ok_or(ContractError::WrongActor)?;
    context.principal.require_actor(request.principal)?;
    if request.principal.is_zero() || request.id.is_zero() || request.epoch.0 == 0 {
        return Err(ContractError::InvalidTarget.into());
    }
    Ok(())
}

impl NativeOwner {
    /// Decode a complete borrowed actor frame with cumulative input work limits.
    /// The caller retains its charged receive buffer until this method returns;
    /// this synchronous API neither receives bytes nor acknowledges durability.
    pub fn prepare_frame(
        &mut self,
        context: NativeContext,
        bytes: &[u8],
        limits: NativeDecodeLimits,
        evidence: Option<&VerifiedNativeArtifact>,
    ) -> Result<NativeStaging, NativeOwnerError> {
        let ledger = self.core.state.ledger;
        let profile = self.core.state.profile;
        limits.with_header_check(
            self.core.limits,
            bytes,
            |header| check_frame_header(context, ledger, profile, header),
            |input, quote| {
                self.prepare_decoded(context, input, quote.construction.total()?, evidence)
            },
        )?
    }

    #[allow(clippy::too_many_arguments)] // Same custody boundary as decoded ingress.
    pub fn prepare_frame_with_custody(
        &mut self,
        context: NativeContext,
        bytes: &[u8],
        limits: NativeDecodeLimits,
        store: &mut ContentStore,
        inline_domain: ContentDomainId,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        let ledger = self.core.state.ledger;
        let profile = self.core.state.profile;
        limits.with_header_check(
            self.core.limits,
            bytes,
            |header| check_frame_header(context, ledger, profile, header),
            |input, quote| {
                self.prepare_decoded_with_custody(
                    context,
                    input,
                    quote.construction.total()?,
                    store,
                    inline_domain,
                    schemas,
                )
            },
        )?
    }

    pub fn prepare_frame_evidenced_with_schemas(
        &mut self,
        context: NativeContext,
        bytes: &[u8],
        limits: NativeDecodeLimits,
        evidence: Option<&VerifiedNativeArtifact>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        let ledger = self.core.state.ledger;
        let profile = self.core.state.profile;
        limits.with_header_check(
            self.core.limits,
            bytes,
            |header| check_frame_header(context, ledger, profile, header),
            |input, quote| {
                self.prepare_decoded_evidenced_with_schemas(
                    context,
                    input,
                    quote.construction.total()?,
                    evidence,
                    schemas,
                )
            },
        )?
    }

    /// Stage a complete decoded actor command. Variable fields stay borrowed
    /// until this owner reserves construction capacity. Exact retries require
    /// neither input allocation nor a remaining construction-work allowance.
    /// Parsing and source/model preparation use the codec's separate limits.
    pub fn prepare_decoded(
        &mut self,
        context: NativeContext,
        input: DecodedRequest<'_, '_>,
        max_build_visits: usize,
        evidence: Option<&VerifiedNativeArtifact>,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_decoded_using(
            context,
            input,
            max_build_visits,
            evidence,
            None,
            &BuiltinNativeSchemas,
        )
    }

    /// Verify external evidence with the same owner-selected capacity used for
    /// input and candidate construction. This performs no participant execution.
    #[allow(clippy::too_many_arguments)] // Mirrors typed custody ingress plus construction allowance.
    pub fn prepare_decoded_with_custody(
        &mut self,
        context: NativeContext,
        input: DecodedRequest<'_, '_>,
        max_build_visits: usize,
        store: &mut ContentStore,
        inline_domain: ContentDomainId,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_decoded_using(
            context,
            input,
            max_build_visits,
            None,
            Some((store, inline_domain)),
            schemas,
        )
    }

    pub fn prepare_decoded_evidenced_with_schemas(
        &mut self,
        context: NativeContext,
        input: DecodedRequest<'_, '_>,
        max_build_visits: usize,
        evidence: Option<&VerifiedNativeArtifact>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_decoded_using(context, input, max_build_visits, evidence, None, schemas)
    }

    fn prepare_decoded_using(
        &mut self,
        context: NativeContext,
        input: DecodedRequest<'_, '_>,
        max_build_visits: usize,
        evidence: Option<&VerifiedNativeArtifact>,
        custody: Option<(&mut ContentStore, ContentDomainId)>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        let header = input.header();
        if header.ledger != self.core.state.ledger {
            return Err(NativeError::Contract(ContractError::WrongLedger).into());
        }
        if header.profile != self.core.state.profile {
            return Err(NativeError::Contract(ContractError::InvalidPolicy).into());
        }
        let request = header
            .request
            .ok_or(ContractError::WrongActor)
            .map_err(NativeError::from)?;
        let checked = self.core.check_request_identity_chain(
            context,
            request,
            input.intent_fingerprint(),
            self.pending.iter().map(|row| &row.prepared),
        )?;
        let view = match checked {
            prepare::RequestCheck::Existing { outcome, committed } => {
                let candidate = if committed {
                    None
                } else {
                    Some(
                        self.pending
                            .iter()
                            .find(|row| row.prepared.outcome() == outcome)
                            .ok_or(NativeOwnerError::UnknownCandidate)?
                            .candidate,
                    )
                };
                return Ok(NativeStaging::Existing { outcome, candidate });
            }
            prepare::RequestCheck::Fresh { view, .. } => view,
        };
        if self.faulted {
            return Err(NativeError::Capacity("completion owner requires reconstruction").into());
        }
        if let Err(error) = self.book.check_health() {
            self.faulted = true;
            return Err(error.into());
        }
        self.next_serial
            .checked_add(1)
            .ok_or(MemoryError::CounterExhausted("native candidate serial"))?;
        if self.pending.len() == self.pending.capacity() {
            return Err(NativeError::Capacity("pending candidates").into());
        }
        input.check_build(self.core.limits.preparation_bytes, max_build_visits)?;
        let reservation = self.reserve_decoded(context, &input, &view, schemas)?;
        // Authorization may replay a borrowed source. It cannot spend the
        // remaining construction passes and then allocate an incomplete input.
        input.check_build(self.core.limits.preparation_bytes, max_build_visits)?;
        let owned = input.build()?;
        // This guard outlives the consumed input and every custody/build path.
        // Each retained candidate page carries its own separately priced debit.
        let _input_allocation = reservation.commit();
        self.prepare_using(
            OwnerInput::Request(context, owned),
            evidence,
            custody,
            schemas,
        )
    }

    fn reserve_decoded(
        &self,
        context: NativeContext,
        input: &DecodedRequest<'_, '_>,
        view: &View<'_>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<Reservation, NativeError> {
        let bytes = input.construction_bytes();
        match &input.plan {
            DecodedPlan::Artifact(plan) => match plan.command() {
                ArtifactCommand::Report {
                    kind,
                    claim,
                    key,
                    expected,
                    report,
                } => {
                    let descriptor = plan.descriptor();
                    let (registered, authorization) = match kind {
                        ReportKind::Admission => super::super::admission_authority::report_view(
                            view,
                            context,
                            claim,
                            key,
                            expected,
                            report,
                            descriptor,
                            self.core.limits,
                        )?,
                        ReportKind::Increment => super::super::increment_authority::report_view(
                            view,
                            context,
                            claim,
                            key,
                            expected,
                            report,
                            descriptor,
                            self.core.limits,
                        )?,
                        ReportKind::Work => super::super::work_authority::report_view(
                            view,
                            context,
                            claim,
                            key,
                            expected,
                            report,
                            descriptor,
                            self.core.limits,
                        )?,
                    };
                    let loan = self.book.report_contract_view(
                        key,
                        registered.state.binding(),
                        registered.parent,
                        registered.registry,
                        descriptor,
                        authorization.schema(),
                        schemas,
                    )?;
                    return Ok(loan.source().reserve(
                        BudgetKind::Pending,
                        BudgetLane::Completion,
                        bytes,
                    )?);
                }
                ArtifactCommand::SubmitDiagnostic { claim, reason } => {
                    if let Some((key, spend)) = respondent::select_diagnostic(
                        view,
                        context,
                        claim,
                        reason,
                        plan.descriptor(),
                        &self.book,
                        schemas,
                        self.core.limits,
                    )? {
                        let loan = self.book.respondent_contract(key, spend, view, schemas)?;
                        return Ok(loan.source().reserve(
                            BudgetKind::Pending,
                            BudgetLane::Completion,
                            bytes,
                        )?);
                    }
                }
                ArtifactCommand::SubmitWork { .. } | ArtifactCommand::RejectWork { .. } => {}
            },
            DecodedPlan::Response(plan) => {
                if let Some((key, spend)) = respondent::select_response(
                    view,
                    context,
                    plan.claim(),
                    plan.response(),
                    plan.body(),
                    &self.book,
                    schemas,
                    self.core.limits,
                )? {
                    let loan = self.book.respondent_contract(key, spend, view, schemas)?;
                    return Ok(loan.source().reserve(
                        BudgetKind::Pending,
                        BudgetLane::Completion,
                        bytes,
                    )?);
                }
            }
            DecodedPlan::Fixed { input, .. } => {
                if let NativeCommand::PostResponse { claim, expected } = input.command
                    && let Some((key, spend)) = respondent::select_post(
                        view,
                        context,
                        claim,
                        expected,
                        &self.book,
                        schemas,
                        self.core.limits,
                    )?
                {
                    let loan = self.book.respondent_contract(key, spend, view, schemas)?;
                    return Ok(loan.source().reserve(
                        BudgetKind::Pending,
                        BudgetLane::Completion,
                        bytes,
                    )?);
                }
            }
            DecodedPlan::Monitor(_) | DecodedPlan::Projection(_) | DecodedPlan::Authored(_) => {}
        }
        Ok(self
            .core
            .state
            .budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bytes)?)
    }
}
