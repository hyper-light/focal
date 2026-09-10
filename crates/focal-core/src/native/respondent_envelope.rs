//! Finite respondent completion demand for one actual receipt. This quote owns
//! no authority, reservation or shared allocation. Diagnostic, close and post
//! credits are independent: several Generated responses can await posting.

use super::completion_envelope::{
    CompletionSlots, EvidenceBounds, entry_heap, parent_bound, pinned_descriptor_charge,
    registry_bound,
};
use super::prepare::{add, array, containers, event_containers, heap, within};
use super::prepare_budget::ConstructionBudget;
use super::respondent_state::{RespondentCredit, RespondentKey};
use super::*;
use focal_memory::{RangeWriteEnvelope, RangeWriteLimits};
use focal_model::lifecycle::artifact_descriptor::{
    ArtifactDescriptor, Limits as ArtifactLimits, WorkRole,
};
use focal_model::lifecycle::evidence::{
    ClosePlan, FailedWork, Parent, ResponseDiagnostic, SlotBinding, WorkArtifact,
};
use focal_model::lifecycle::scope::ScopeLimits;
use focal_model::{ArtifactRef, ReceiptFence};

#[cfg(test)]
#[path = "respondent_envelope_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RespondentDemand {
    pub(super) retained_bytes: usize,
    pub(super) slots: CompletionSlots,
    pub(super) actions: u64,
}

#[derive(Debug, Clone, Copy)]
struct Action {
    storage: RangeWriteEnvelope,
    slots: CompletionSlots,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RespondentEnvelope {
    record_buffers: Option<record_codec::EncodingLimits>,
    key: RespondentKey,
    parent: Binding,
    holder: ParticipantId,
    policy: ContentHash,
    scope_limits: ScopeLimits,
    max_responses: u32,
    registry_limit: usize,
    max_registrations: usize,
    parent_heap: usize,
    registry_heap: usize,
    descriptor: ArtifactLimits,
    input_heap: usize,
    response_input_heap: usize,
    response_heap: usize,
    work_limit: usize,
    diagnostic_limit: usize,
    summary_limit: usize,
    input_visits: usize,
    maximum: RespondentCredit,
    diagnostic: Action,
    close: Action,
    post: Action,
    workspace: usize,
}

fn multiply(left: usize, right: usize) -> Result<usize, NativeError> {
    left.checked_mul(right).ok_or(NativeError::Capacity(
        "respondent completion multiplication",
    ))
}

fn transient(storage: RangeWriteEnvelope) -> Result<usize, NativeError> {
    storage
        .additional_peak_bytes()
        .checked_sub(storage.additional_retained_bytes())
        .ok_or(NativeError::Capacity("respondent range workspace"))
}

fn count(value: u32) -> Result<usize, NativeError> {
    usize::try_from(value).map_err(|_| NativeError::Capacity("respondent action count"))
}

fn action(
    view: &View<'_>,
    max_ranges: usize,
    changes: usize,
    deleted: usize,
    incoming_heap: usize,
    slots: CompletionSlots,
) -> Result<Action, NativeError> {
    Ok(Action {
        storage: view.state.rows.future_write_envelope(
            RangeWriteLimits {
                changed_keys: changes,
                deleted_keys: deleted,
                deleted_heap: 0,
                incoming_heap,
                input_capacity: changes,
            },
            max_ranges,
        )?,
        slots,
    })
}

/// Maximum retained dynamic report content. Failed rows and manifested rows
/// share one actual work set; charging both maxima also covers all mixtures.
fn response_heap(summary: usize, work: usize, diagnostics: usize) -> Result<usize, NativeError> {
    add(
        array::<u8>(summary)?,
        add(
            array::<SlotBinding>(work)?,
            add(
                array::<FailedWork>(work)?,
                array::<ResponseDiagnostic>(diagnostics)?,
            )?,
        )?,
    )
}

/// Original histories can contain Generated reports under this receipt even
/// when later cycles have closed. Their immutable retained capacities are a
/// floor for posting, and their original cycle links must remain complete.
fn generated_heap(
    view: &View<'_>,
    claim: &ClaimState,
    receipt: ReceiptFence,
    limits: NativeLimits,
) -> Result<usize, NativeError> {
    within(claim.response_count(), limits.plan_edges)?;
    let mut next = claim.latest_response().map(|link| link.testament);
    let mut expected_cycle = claim.response_count();
    let mut maximum = 0;
    while expected_cycle != 0 {
        let id = next.ok_or(ContractError::InvalidManifest)?;
        let Some(Row::Response(row)) = view.get(Key::Response(id)) else {
            return Err(ContractError::InvalidManifest.into());
        };
        let response = row.get().ok_or(ContractError::InvalidManifest)?;
        let identity = response.identity();
        if identity.binding.object.0 != id.0 || count(identity.cycle)? != expected_cycle {
            return Err(ContractError::InvalidManifest.into());
        }
        claim.recorded_response(response)?;
        if identity.receipt == receipt && response.state() == ResponseState::Generated {
            identity.binding.next()?;
            maximum = maximum.max(super::response_owned::response_heap(response)?);
        }
        next = identity.prior;
        expected_cycle = expected_cycle
            .checked_sub(1)
            .ok_or(ContractError::Capacity)?;
    }
    if next.is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(maximum)
}

impl RespondentEnvelope {
    pub(super) fn derive(
        view: &View<'_>,
        claim: &ClaimState,
        limits: NativeLimits,
        mut descriptor: ArtifactLimits,
        evidence: EvidenceBounds,
    ) -> Result<Self, NativeError> {
        let (key, maximum) = super::respondent_state::read(view, claim, limits)?
            .ok_or(ContractError::InvalidTransition)?;
        let parent = Parent::from_claim(claim)?;
        let registry = view
            .owned_claim(key.claim)?
            .registrations()
            .ok_or(ContractError::InvalidPolicy)?;
        let (parent_heap, registry_heap) = parent_bound(limits, claim, registry)?;
        let max_registrations = registry_bound(limits, claim, registry)?.0;
        let work_limit = claim
            .acceptance()
            .slot_count()
            .min(super::response_budget::work_limit(limits)?);
        let diagnostic_limit = limits.diagnostics_per_cycle.min(limits.plan_edges);
        if diagnostic_limit == 0
            || limits.response_summary_bytes == 0
            || claim.max_responses().checked_add(1).is_none()
        {
            return Err(NativeError::Capacity("respondent closing cycle"));
        }
        // State reconstruction and every subsequent credit update use one
        // counted reader allowance. Quote the complete authored future history
        // and both full current-cycle cohorts before acquiring responsibility.
        let state_visits = add(
            3,
            add(
                multiply(4, count(claim.max_responses())?)?,
                multiply(3, add(work_limit, diagnostic_limit)?)?,
            )?,
        )?;
        within(state_visits, limits.plan_edges)?;
        let diagnostic_construction =
            ConstructionBudget::for_operation(NativeOperation::SubmitDiagnostic, limits)?;
        let close_construction =
            ConstructionBudget::for_operation(NativeOperation::CloseResponse, limits)?;
        let post_construction =
            ConstructionBudget::for_operation(NativeOperation::PostResponse, limits)?;
        // Index rows (doc 22 §7): a diagnostic artifact's identity, producer,
        // kind, schema and inputs; a close's and a post's claim status move. A
        // diagnostic's eight primary rows and four fixed index rows leave the
        // batch's remainder to the inputs its descriptor may cite.
        let inputs = crate::native::index_rows::cap_inputs(
            super::completion_envelope::input_bound(descriptor),
            add(8, crate::native::index_rows::ARTIFACT_FIXED_ROWS)?,
            limits.range.max_batch_entries,
        );
        descriptor.inputs = inputs;
        let diagnostic_index = crate::native::index_rows::artifact_rows(inputs)?;
        let close_index = crate::native::index_rows::STATUS_ROWS;
        let post_index = crate::native::index_rows::STATUS_ROWS;
        diagnostic_construction.check_counts(0, 4, 2, diagnostic_index)?;
        close_construction.check_counts(
            1,
            add(work_limit, 2)?,
            add(work_limit, 2)?,
            close_index,
        )?;
        post_construction.check_counts(1, 1, 2, post_index)?;
        let dynamic = descriptor
            .construction_bytes
            .checked_sub(size_of::<ArtifactDescriptor>())
            .ok_or(NativeError::Capacity("respondent descriptor construction"))?;
        if descriptor.kind_bytes < "error".len() || dynamic < "error".len() {
            return Err(NativeError::Capacity("respondent minimum diagnostic"));
        }
        let descriptor_heap = pinned_descriptor_charge(dynamic, descriptor)?;
        let input_heap = add(NativeArtifactInput::container_charge(), descriptor_heap)?;
        let diagnostic_scratch = add(
            input_heap,
            add(
                OwnedArtifact::container_charge(),
                OwnedDiagnostic::container_charge(),
            )?,
        )?;
        within(diagnostic_scratch, diagnostic_construction.scratch_bytes)?;

        let response_input_heap = add(
            array::<u8>(limits.response_summary_bytes)?,
            add(
                array::<SlotBinding>(work_limit)?,
                array::<ArtifactRef>(diagnostic_limit)?,
            )?,
        )?;
        let response_heap =
            response_heap(limits.response_summary_bytes, work_limit, diagnostic_limit)?
                .max(generated_heap(view, claim, parent.receipt, limits)?);
        let claim_row = add(containers(1)?, add(parent_heap, registry_heap)?)?;
        let response_row = add(OwnedResponse::container_charge(), response_heap)?;
        let work_rows = multiply(work_limit, OwnedWork::container_charge())?;
        // Close keeps the authored input, complete source cohorts, prepared
        // attachments/report, changed claim and copied registry simultaneously.
        let close_plan = add(
            size_of::<ClosePlan>(),
            add(response_heap, array::<WorkArtifact>(work_limit)?)?,
        )?;
        let close_scratch = add(
            add(response_input_heap, array::<ClaimState>(1)?)?,
            add(
                add(
                    array::<WorkArtifact>(work_limit)?,
                    array::<ResponseDiagnostic>(diagnostic_limit)?,
                )?,
                add(
                    close_plan,
                    add(
                        add(parent_heap, registry_heap)?,
                        add(work_rows, OwnedResponse::container_charge())?,
                    )?,
                )?,
            )?,
        )?;
        let post_scratch = add(
            array::<ClaimState>(1)?,
            add(response_row, add(parent_heap, registry_heap)?)?,
        )?;
        within(close_scratch, close_construction.scratch_bytes)?;
        within(post_scratch, post_construction.scratch_bytes)?;
        let entry = entry_heap(limits)?;
        for bytes in [
            claim_row,
            response_row,
            OwnedWork::container_charge(),
            OwnedDiagnostic::container_charge(),
            OwnedEvent::container_charge(),
            add(OwnedArtifact::container_charge(), descriptor_heap)?,
        ] {
            within(bytes, entry)?;
        }
        // These operations publish no evaluation/cohort updates. The completion
        // collector still visits every event, and the claim journal makes two
        // complete event passes for its single changed parent.
        within(multiply(2, add(work_limit, 2)?)?, limits.plan_edges)?;
        for events in [2, add(work_limit, 2)?, 2] {
            within(
                super::completion_book::respondent_journal_visits(events, 1)?,
                limits.plan_edges,
            )?;
        }
        let diagnostic = action(
            view,
            limits.max_ranges,
            add(8, diagnostic_index)?,
            0,
            add(
                add(
                    descriptor_heap,
                    add(
                        OwnedArtifact::container_charge(),
                        OwnedDiagnostic::container_charge(),
                    )?,
                )?,
                event_containers(2)?,
            )?,
            CompletionSlots {
                artifacts: 1,
                identities: 1,
                outcomes: 1,
                events: 2,
                sequences: 1,
                new_rows: 7,
                ..CompletionSlots::default()
            },
        )?;
        let close_events = add(work_limit, 2)?;
        let close = action(
            view,
            limits.max_ranges,
            add(add(multiply(2, work_limit)?, 7)?, close_index)?,
            1,
            add(
                add(claim_row, response_row)?,
                add(work_rows, event_containers(close_events)?)?,
            )?,
            CompletionSlots {
                responses: 1,
                outcomes: 1,
                events: close_events,
                sequences: 1,
                new_rows: add(work_limit, 5)?,
                ..CompletionSlots::default()
            },
        )?;
        let post = action(
            view,
            limits.max_ranges,
            add(6, post_index)?,
            1,
            add(add(claim_row, response_row)?, event_containers(2)?)?,
            CompletionSlots {
                outcomes: 1,
                events: 2,
                sequences: 1,
                new_rows: 3,
                ..CompletionSlots::default()
            },
        )?;
        // Ingress and verification coexist with Core's conservative input
        // precharge. Only one action executes at once; its range transient is
        // additional to the independently retained sum of every future action.
        let diagnostic_workspace = add(
            add(
                input_heap,
                add(evidence.workspace_bytes, evidence.retained_bytes)?,
            )?,
            add(
                diagnostic_construction.temporary_bytes()?,
                transient(diagnostic.storage)?,
            )?,
        )?;
        let close_workspace = add(
            response_input_heap,
            add(
                close_construction.temporary_bytes()?,
                transient(close.storage)?,
            )?,
        )?;
        let post_workspace = add(
            post_construction.temporary_bytes()?,
            transient(post.storage)?,
        )?;
        let envelope = Self {
            record_buffers: None,
            key,
            parent: claim.binding(),
            holder: parent.holder,
            policy: claim.acceptance().intent_fingerprint(),
            scope_limits: claim.scopes().limits(),
            max_responses: claim.max_responses(),
            registry_limit: registry.max_rows(),
            max_registrations,
            parent_heap,
            registry_heap,
            descriptor,
            input_heap,
            response_input_heap,
            response_heap,
            work_limit,
            diagnostic_limit,
            summary_limit: limits.response_summary_bytes,
            input_visits: limits.plan_edges,
            maximum,
            diagnostic,
            close,
            post,
            workspace: diagnostic_workspace
                .max(close_workspace)
                .max(post_workspace),
        };
        envelope.demand(maximum)?;
        envelope.check_parent(view, claim, limits)?;
        Ok(envelope)
    }

    pub(super) fn demand(&self, credit: RespondentCredit) -> Result<RespondentDemand, NativeError> {
        if credit.diagnostics > self.maximum.diagnostics
            || credit.closes > self.maximum.closes
            || credit.posts > self.maximum.posts
        {
            return Err(NativeError::Capacity("respondent credit exceeds contract"));
        }
        let mut retained_bytes = 0;
        let mut slots = CompletionSlots::default();
        for (action, count) in [
            (self.diagnostic, credit.diagnostics),
            (self.close, credit.closes),
            (self.post, credit.posts),
        ] {
            retained_bytes = add(
                retained_bytes,
                multiply(
                    add(
                        crate::native::mutation::retained(action.storage)?,
                        self.record_buffers
                            .map(|limits| record_codec::future_record_bytes(action.storage, limits))
                            .transpose()?
                            .unwrap_or(0),
                    )?,
                    self::count(count)?,
                )?,
            )?;
            slots = slots.checked_add(action.slots.checked_scale(count)?)?;
        }
        let actions = u64::from(credit.diagnostics)
            .checked_add(u64::from(credit.closes))
            .and_then(|n| n.checked_add(u64::from(credit.posts)))
            .ok_or(NativeError::Capacity("respondent action serials"))?;
        Ok(RespondentDemand {
            retained_bytes,
            slots,
            actions,
        })
    }

    pub(super) fn workspace_bytes(&self) -> usize {
        self.workspace
    }

    pub(super) fn with_record_buffers(
        mut self,
        limits: record_codec::EncodingLimits,
    ) -> Result<Self, NativeError> {
        if self.record_buffers.is_some() {
            return Err(ContractError::InvalidTransition.into());
        }
        for action in [self.diagnostic, self.close, self.post] {
            record_codec::future_record_bytes(action.storage, limits)?;
        }
        self.record_buffers = Some(limits);
        self.demand(self.maximum)?;
        Ok(self)
    }
    pub(super) fn construction(
        &self,
        operation: NativeOperation,
        limits: NativeLimits,
    ) -> Result<ConstructionBudget, NativeError> {
        self.storage(operation)?;
        let construction = ConstructionBudget::for_operation(operation, limits)?;
        within(
            add(
                construction.temporary_bytes()?,
                transient(self.storage(operation)?)?,
            )?,
            self.workspace,
        )?;
        Ok(construction)
    }

    pub(super) fn storage(
        &self,
        operation: NativeOperation,
    ) -> Result<RangeWriteEnvelope, NativeError> {
        Ok(match operation {
            NativeOperation::SubmitDiagnostic => self.diagnostic.storage,
            NativeOperation::CloseResponse => self.close.storage,
            NativeOperation::PostResponse => self.post.storage,
            _ => return Err(ContractError::InvalidTransition.into()),
        })
    }

    pub(super) fn check_descriptor(&self, input: &NativeArtifactInput) -> Result<(), NativeError> {
        // The private input constructor reconciles its exact singleton. The
        // shared view includes that charge and actual owned buffer capacities.
        self.check_descriptor_view(input.get().ok_or(ContractError::MissingEvidence)?)
    }

    pub(super) fn check_descriptor_view(
        &self,
        descriptor: &impl super::report_artifact::ArtifactView,
    ) -> Result<(), NativeError> {
        super::report_artifact::check_limits(
            descriptor,
            self.descriptor,
            self.input_heap,
            "respondent diagnostic dimensions",
        )?;
        let fields = descriptor.fields();
        if fields.kind != "error"
            || fields.result.is_some()
            || fields.work.is_none_or(|work| {
                work.claim != self.key.claim
                    || work.role
                        != WorkRole::Diagnostic {
                            reason: EvidenceFailure::Work,
                        }
            })
            || fields.receipt
                != Some(ReceiptFence {
                    receipt: self.key.receipt,
                    epoch: self.key.epoch,
                })
            || fields.producer != self.holder
            || fields.ledger != self.parent.ledger
        {
            return Err(ContractError::MissingEvidence.into());
        }
        // check_inputs restarts its sorted visibility merge per source. Both
        // mandatory and advanced authored descriptors are refused before the
        // loan when their complete successful traversal cannot fit.
        within(
            multiply(
                descriptor.input_count(),
                add(1, multiply(3, descriptor.visibility_count())?)?,
            )?,
            self.input_visits,
        )
    }

    fn check_response_shape(
        &self,
        summary: usize,
        manifest: usize,
        diagnostics: usize,
        heap: usize,
    ) -> Result<(), NativeError> {
        if summary > self.summary_limit
            || manifest > self.work_limit
            || diagnostics > self.diagnostic_limit
        {
            return Err(NativeError::Capacity(
                "respondent authored response dimensions",
            ));
        }
        within(heap, self.response_input_heap)
    }

    pub(super) fn check_response_input(
        &self,
        input: &NativeResponseInput,
    ) -> Result<(), NativeError> {
        self.check_response_shape(
            input.summary.len(),
            input.manifest.len(),
            input.diagnostics.len(),
            input.heap_charge()?,
        )
    }

    pub(super) fn check_response_plan<S: NativeResponseSource>(
        &self,
        plan: &NativeResponseSourcePlan<S>,
    ) -> Result<(), NativeError> {
        // Captured dimensions and bytes belong to the successfully prepared
        // body. Build independently refuses a changed generic source.
        self.check_response_shape(
            plan.summary_bytes(),
            plan.manifest_len(),
            plan.diagnostic_len(),
            plan.quote().bytes,
        )
    }

    /// Run on the effective proposed prefix before accepting optional evidence
    /// or parent growth. Authority and retirement on receipt replacement remain
    /// the owner's job; this checks the unchanged receipt's promised capacities.
    pub(super) fn check_parent(
        &self,
        view: &View<'_>,
        claim: &ClaimState,
        limits: NativeLimits,
    ) -> Result<(), NativeError> {
        self.parent.check(&Binding {
            revision: self.parent.revision,
            ..claim.binding()
        })?;
        if claim.binding().revision < self.parent.revision
            || claim.acceptance().intent_fingerprint() != self.policy
            || claim.scopes().limits() != self.scope_limits
            || claim.max_responses() != self.max_responses
        {
            return Err(ContractError::InvalidPolicy.into());
        }
        let (key, credit) = super::respondent_state::read(view, claim, limits)?
            .ok_or(ContractError::InvalidTransition)?;
        if key != self.key {
            return Err(ContractError::StaleReceipt.into());
        }
        self.demand(credit)?;
        let registry = view
            .owned_claim(key.claim)?
            .registrations()
            .ok_or(ContractError::InvalidPolicy)?;
        registry.check(claim)?;
        if registry.max_rows() != self.registry_limit
            || registry.rows().len() > self.max_registrations
        {
            return Err(ContractError::InvalidPolicy.into());
        }
        within(heap(claim)?, self.parent_heap)?;
        within(transactions::registry_heap(registry)?, self.registry_heap)?;
        let parent = Parent::from_claim(claim)?;
        if parent.holder != self.holder {
            return Err(ContractError::WrongActor.into());
        }
        let revisions = u64::from(credit.closes)
            .checked_add(u64::from(credit.posts))
            .ok_or(ContractError::Capacity)?;
        claim
            .binding()
            .revision
            .0
            .checked_add(revisions)
            .ok_or(ContractError::Capacity)?;
        within(
            generated_heap(view, claim, parent.receipt, limits)?,
            self.response_heap,
        )?;
        if credit.closes != 0 {
            let cycle = super::work_artifacts::cycle(view, &parent, limits)?;
            within(cycle.work_count, self.work_limit)?;
            within(cycle.diagnostic_count, self.diagnostic_limit)?;
            let mut next = cycle.work_head;
            for _ in 0..cycle.work_count {
                let id = next.ok_or(ContractError::InvalidManifest)?;
                let row = super::response_reads::as_work(view.get(Key::Work(id)))
                    .ok_or(ContractError::InvalidManifest)?;
                if row.state.reference().id != id
                    || row.state.claim() != key.claim
                    || row.state.receipt() != parent.receipt
                    || row.state.cycle() != parent.next_cycle
                    || row.state.producer() != self.holder
                    || !matches!(view.get(Key::WorkSlot(NativeCycleKey::of(&parent), row.state.slot())), Some(Row::WorkSlot(found)) if *found == id)
                {
                    return Err(ContractError::InvalidManifest.into());
                }
                match row.state.state() {
                    WorkArtifactState::Generated | WorkArtifactState::Received => {
                        row.state.binding().next()?;
                    }
                    WorkArtifactState::GenerationFailed | WorkArtifactState::ReceiptFailed => (),
                    _ => return Err(ContractError::InvalidTransition.into()),
                }
                next = row.next;
            }
            if next.is_some() {
                return Err(ContractError::InvalidManifest.into());
            }
        }
        Ok(())
    }
}
