//! WholeWork report demand over immutable targets and a fixed indexed graph.
//! The checked owner separately grants authority and holds the quoted backing.
use super::*;
use crate::native::{admission_authority::Registered, completion_book::GraphMembers};
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::ReceiptFence;
use focal_model::lifecycle::aggregation::ProjectionShape;

#[derive(Debug, Clone, Copy)]
pub(super) struct WorkCompletion {
    key: EvaluationKey,
    work: Binding,
    response: Binding,
    receipt: ReceiptFence,
    cycle: u32,
    response_heap: usize,
    projection: NativeProjectionQuote,
}

fn target_revision(pinned: Binding, current: Binding) -> Result<(), NativeError> {
    pinned.check(&Binding {
        revision: pinned.revision,
        ..current
    })?;
    if current.revision < pinned.revision {
        return Err(ContractError::StaleRevision.into());
    }
    Ok(())
}

fn margins(claim: &ClaimState, response: &Response, work: &NativeWork) -> Result<(), NativeError> {
    if !work.state.state().is_terminal() {
        work.state.binding().next()?;
    }
    if !response.state().is_terminal() {
        response.identity().binding.next()?;
    }
    if !claim.is_terminal() {
        let mut next = claim.binding().next()?;
        for _ in claim.scopes().iter().filter(|scope| scope.active()) {
            next = next.next()?;
        }
        if !claim.local_complete() {
            // An aggregate local outcome and subsequent graph release are
            // separate real revisions, even when they share one publication.
            next.next()?;
        }
    }
    Ok(())
}

fn future_shape(
    view: &View<'_>,
    limits: NativeLimits,
    claim: &ClaimState,
    registry: &RegistrationSet,
) -> Result<ProjectionShape, NativeError> {
    crate::native::response_budget::check_registration_capacity_in(view, claim, registry, limits)?;
    let responses = usize::try_from(claim.max_responses()).map_err(|_| ContractError::Capacity)?;
    let retired =
        crate::native::retired_cycles::head(view, ClaimId(claim.binding().object.0), limits)?;
    Ok(ProjectionShape {
        responses,
        // Failed and unclosed output rows also occupy a declared slot. No
        // authored response or zero-check slot is removed from the estimate.
        works: add(
            multiply(responses, claim.acceptance().slot_count())?,
            retired.work_count,
        )?,
        evaluations: crate::native::response_budget::required_with_retired(
            claim,
            limits,
            retired.work_count,
        )?,
    })
}

pub(super) fn capped_descriptor(
    limits: NativeLimits,
    fixed: usize,
    mut descriptor: ArtifactLimits,
) -> Result<ArtifactLimits, NativeError> {
    let space = limits
        .preparation_bytes
        .checked_sub(fixed)
        .ok_or(NativeError::Capacity("compound WholeWork report scratch"))?;
    let space = space.min(
        entry_heap(limits)?
            .checked_sub(OwnedArtifact::container_charge())
            .ok_or(NativeError::Capacity("WholeWork artifact entry"))?,
    );
    let supplied = descriptor
        .construction_bytes
        .checked_sub(size_of::<ArtifactDescriptor>())
        .ok_or(ContractError::Capacity)?;
    let mut low = 0usize;
    let mut high = supplied.min(space);
    while low < high {
        let span = high.checked_sub(low).ok_or(ContractError::Capacity)?;
        let middle = add(
            low,
            add(
                span.checked_div(2).ok_or(ContractError::Capacity)?,
                usize::from(!span.is_multiple_of(2)),
            )?,
        )?;
        if pinned_descriptor_charge(middle, descriptor).is_ok_and(|bytes| bytes <= space) {
            low = middle;
        } else {
            high = middle.checked_sub(1).ok_or(ContractError::Capacity)?;
        }
    }
    if low < "error".len() || descriptor.kind_bytes < "error".len() {
        return Err(NativeError::Capacity(
            "minimum WholeWork diagnostic descriptor",
        ));
    }
    descriptor.construction_bytes = add(size_of::<ArtifactDescriptor>(), low)?;
    descriptor.kind_bytes = descriptor.kind_bytes.min(low);
    descriptor.metadata_bytes = descriptor.metadata_bytes.min(low);
    descriptor.inline_bytes = descriptor.inline_bytes.min(low);
    descriptor.inputs = descriptor.inputs.min(limits.plan_edges).min(
        low.checked_div(size_of::<ObjectRef>())
            .ok_or(ContractError::Capacity)?,
    );
    descriptor.visibility_labels = descriptor.visibility_labels.min(
        low.checked_div(size_of::<String>())
            .ok_or(ContractError::Capacity)?,
    );
    descriptor.visibility_label_bytes = descriptor.visibility_label_bytes.min(low);
    Ok(descriptor)
}

impl CompletionEnvelope {
    /// Price the entire retry/fallback/quality chain before accepting the first
    /// evaluator attempt. Initial graph discovery is itself ordinarily funded;
    /// its exact complete member set becomes the owner's reverse protections.
    pub(in crate::native) fn derive_work(
        view: &View<'_>,
        limits: NativeLimits,
        registered: &Registered<'_>,
        descriptor: ArtifactLimits,
        evidence: EvidenceBounds,
    ) -> Result<(Self, GraphMembers), NativeError> {
        let parent = registered.parent;
        let registry = registered.registry;
        let definition = registered.definition;
        if CompletionTarget::of(definition)? != CompletionTarget::Work
            || definition.attempt_bound() == 0
        {
            return Err(ContractError::InvalidTarget.into());
        }
        parent.acceptance().check_declaration(definition)?;
        registry.check(parent)?;
        registry
            .rows()
            .get(registered.registration_index)
            .ok_or(ContractError::InvalidTarget)?
            .check_state(*registered.state, definition)?;
        let (response, work) =
            crate::native::work_authority::completion_target(view, registered, limits)?;
        margins(parent, response, work)?;
        let key = EvaluationKey::of(ClaimId(parent.binding().object.0), registered.state);
        let shape = future_shape(view, limits, parent, registry)?;
        let retired = crate::native::retired_cycles::head(view, key.claim, limits)?;
        // Four result rows precede the report projection; reserve six so its
        // fixed complete row shape also covers target Work/Response overrides.
        let projection =
            NativeProjectionQuote::derive_with_retired(limits, parent, shape, 6, retired.count)?;
        let reservation = view.state.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            limits.preparation_bytes,
        )?;
        let mut scratch = prepare::Scratch {
            used: 0,
            max: limits.preparation_bytes,
        };
        let plan = graph_effects::preflight(view, parent, limits, None, &mut scratch)?;
        let (graph, cohort) = CompletionGraph::from_plan(view, &plan, limits)?;
        let graph_charges = graph.budget.charges();
        let g = graph_charges.changed_rows;
        let original_events = add(graph_charges.graph_events, 6)?;
        let original_extras = add(6, graph_charges.monitor_index_rows)?;
        let events = add(original_events, cohort.events())?;
        within(
            crate::native::completion_book::respondent_journal_visits(events, g)?,
            limits.plan_edges,
        )?;
        graph.check_journal(original_events, limits)?;
        check_journal_visits(limits, parent, original_events)?;
        check_cohort_visits(limits, cohort, g, original_extras, original_events)?;
        let changes = add(
            add(add(g, original_events)?, add(original_extras, 2)?)?,
            cohort.changed_keys(),
        )?;
        let construction = ConstructionBudget::for_operation(NativeOperation::ReportWork, limits)?
            .with_cohort(cohort, limits)?;
        construction.check_counts(g, add(original_extras, cohort.evaluations())?, events)?;
        within(changes, limits.range.max_batch_entries)?;
        let response_heap = add(
            OwnedResponse::container_charge(),
            crate::native::response_owned::response_heap(response)?,
        )?;
        let fixed = [
            graph_charges.preparation_bytes,
            projection.model().construction_charge(),
            response_heap,
            OwnedWork::container_charge(),
            array::<NativeWork>(1)?,
            result_containers()?,
            NativeArtifactInput::container_charge(),
            array::<NativeFact>(limits.range.max_batch_entries)?,
            array::<(Key, usize)>(events)?,
            cohort.construction_bytes()?,
        ]
        .into_iter()
        .try_fold(0, add)?;
        let descriptor = capped_descriptor(limits, fixed, descriptor)?;
        let dynamic = descriptor
            .construction_bytes
            .checked_sub(size_of::<ArtifactDescriptor>())
            .ok_or(ContractError::Capacity)?;
        let descriptor_heap = pinned_descriptor_charge(dynamic, descriptor)?;
        within(add(fixed, descriptor_heap)?, construction.scratch_bytes)?;
        let input_heap = add(NativeArtifactInput::container_charge(), descriptor_heap)?;
        let incoming_heap = [
            descriptor_heap,
            result_containers()?,
            OwnedWork::container_charge(),
            response_heap,
            containers(g)?,
            graph_charges.claim_heap_bytes,
            graph_charges.registry_heap_bytes,
            event_containers(events)?,
            multiply(cohort.evaluations(), OwnedEvaluation::container_charge())?,
        ]
        .into_iter()
        .try_fold(0, add)?;
        let entry = entry_heap(limits)?;
        for bytes in [
            graph_charges.maximum_entry_heap_bytes,
            add(OwnedArtifact::container_charge(), descriptor_heap)?,
            OwnedWork::container_charge(),
            response_heap,
            OwnedEvaluation::container_charge(),
            OwnedAccepted::container_charge(),
            OwnedEvent::container_charge(),
        ] {
            within(bytes, entry)?;
        }
        let ordinary_report = view.state.rows.future_write_envelope(RangeWriteLimits {
            changed_keys: changes,
            deleted_keys: 0,
            incoming_heap,
            input_capacity: changes,
        })?;
        let reports = definition.attempt_bound();
        // A report may seal any newly terminal graph cohort. Its mixed credit
        // journal remains owned by the pending candidate, so each attempt must
        // retain its own allowance rather than reuse the serial workspace.
        let respondent_journal =
            super::super::completion_book::respondent_journal_bytes(graph_charges.changed_rows)?;
        let ordinary_journal_bytes = add(
            super::super::completion_book::journal_bytes(add(1, cohort.evaluations())?)?,
            respondent_journal,
        )?;
        let retained = multiply(
            usize::try_from(reports).map_err(|_| ContractError::Capacity)?,
            add(
                crate::native::mutation::retained(ordinary_report)?,
                ordinary_journal_bytes,
            )?,
        )?;
        let transient = ordinary_report
            .additional_peak_bytes()
            .checked_sub(ordinary_report.additional_retained_bytes())
            .ok_or(ContractError::Capacity)?;
        // Serial protection checks use a complete graph preflight, but no
        // projection/copy of the entire acceptance state. Keeping the full graph
        // preparation allowance safely covers its capture and bounded cursors.
        let check_bytes = graph.check_bytes;
        let workspace = [
            input_heap,
            evidence.workspace_bytes,
            evidence.retained_bytes,
            construction.temporary_bytes()?,
            transient,
            check_bytes,
            respondent_journal,
        ]
        .into_iter()
        .try_fold(0, add)?;
        let slot_demand = SlotDemand {
            per_report: CompletionSlots {
                artifacts: 1,
                identities: 1,
                results: 1,
                outcomes: 1,
                events,
                sequences: 1,
                new_rows: add(add(graph_charges.graph_events, 10)?, cohort.events())?,
                ..CompletionSlots::default()
            },
            failure: None,
        };
        let (parent_heap, registry_heap) = parent_bound(limits, parent, registry)?;
        let result = Self {
            target: CompletionTarget::Work,
            parent: parent.binding(),
            policy: parent.acceptance().intent_fingerprint(),
            scope_limits: parent.scopes().limits(),
            max_responses: parent.max_responses(),
            parent_heap,
            registry_heap,
            registrations: registry.rows().len(),
            max_registrations: registry_bound(limits, parent, registry)?.0,
            registry_limit: registry.max_rows(),
            descriptor,
            input_heap,
            reports,
            ordinary_report,
            failed_report: None,
            ordinary_journal_bytes,
            failed_journal_bytes: 0,
            retained,
            workspace,
            required: add(retained, workspace)?,
            slot_demand,
            slots: slot_demand.remaining(reports, false)?,
            cohort,
            work: Some(WorkCompletion {
                key,
                work: work.state.binding(),
                response: response.identity().binding,
                receipt: response.identity().receipt,
                cycle: response.identity().cycle,
                response_heap,
                projection,
            }),
            graph: Some(graph),
        };
        result.check_parent(parent, registry)?;
        crate::native::work_authority::check_completion_target(view, registered, &result, limits)?;
        let members = GraphMembers::copy_from(&view.state.budget, plan.members())?;
        drop(plan);
        drop(reservation);
        Ok((result, members))
    }

    pub(in crate::native) fn is_work(self) -> bool {
        self.work.is_some()
    }

    #[cfg(test)]
    pub(in crate::native) fn work_check_bytes(self) -> usize {
        self.graph_check_bytes()
    }

    /// Recheck the canonical proposed prefix before any growth is published.
    /// All output membership (including failed/unclosed rows) is visited once;
    /// no global ledger scan, projection allocation or grant scan is performed.
    #[cfg(test)]
    pub(in crate::native) fn check_work(
        &self,
        view: &View<'_>,
        limits: NativeLimits,
        scratch: &mut prepare::Scratch,
    ) -> Result<(), NativeError> {
        self.check_work_with_members(view, limits, &[], scratch)
    }

    pub(in crate::native) fn check_work_with_members(
        &self,
        view: &View<'_>,
        limits: NativeLimits,
        members: &[ClaimId],
        scratch: &mut prepare::Scratch,
    ) -> Result<(), NativeError> {
        let contract = self.work.ok_or(ContractError::InvalidTarget)?;
        let parent = view
            .claim(contract.key.claim)
            .ok_or(ContractError::InvalidTarget)?;
        let registered = crate::native::admission_authority::registered_any(
            view,
            parent.binding(),
            contract.key,
        )?;
        self.check_parent(parent, registered.registry)?;
        let (response, work) =
            crate::native::work_authority::completion_target(view, &registered, limits)?;
        target_revision(contract.work, work.state.binding())?;
        target_revision(contract.response, response.identity().binding)?;
        if response.identity().receipt != contract.receipt
            || response.identity().cycle != contract.cycle
        {
            return Err(ContractError::StaleReceipt.into());
        }
        margins(parent, response, work)?;
        within(
            add(
                OwnedResponse::container_charge(),
                crate::native::response_owned::response_heap(response)?,
            )?,
            contract.response_heap,
        )?;
        let shape = contract.projection.model().shape();
        let retired = crate::native::retired_cycles::head(view, contract.key.claim, limits)?;
        within(retired.count, contract.projection.retired_cycles())?;
        within(parent.response_count(), shape.responses)?;
        within(registered.registry.rows().len(), shape.evaluations)?;
        if contract.projection.lookup_visits() > limits.plan_edges {
            return Err(NativeError::Capacity("WholeWork projection lookup visits"));
        }
        let visits = crate::native::projection_visits::Visits::new(limits.plan_edges);
        let mut works = 0usize;
        for work in crate::native::projection_work::works_with_budget(
            view,
            contract.key.claim,
            limits,
            Some(&visits),
        ) {
            work?;
            works = add(works, 1)?;
            within(works, shape.works)?;
        }
        visits.check()?;
        self.graph
            .ok_or(ContractError::InvalidTarget)?
            .check(self, view, parent, limits, members, scratch)
    }
}
