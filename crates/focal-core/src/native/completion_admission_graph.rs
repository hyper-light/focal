//! Required Admission has one possible graph-producing outcome. Its retained
//! allowance includes that complete failure and every later audit-only report.
use super::*;
use crate::native::{admission_authority::Registered, completion_book::GraphMembers};
use focal_memory::{BudgetKind, BudgetLane};

impl CompletionEnvelope {
    pub(in crate::native) fn derive_admission(
        view: &View<'_>,
        limits: NativeLimits,
        registered: &Registered<'_>,
        descriptor: ArtifactLimits,
        evidence: EvidenceBounds,
    ) -> Result<(Self, Option<GraphMembers>), NativeError> {
        let parent = registered.parent;
        let registry = registered.registry;
        let definition = registered.definition;
        if CompletionTarget::of(definition)? != CompletionTarget::Admission {
            return Err(ContractError::InvalidTarget.into());
        }
        registry.check(parent)?;
        registry
            .rows()
            .get(registered.registration_index)
            .ok_or(ContractError::InvalidTarget)?
            .check_state(*registered.state, definition)?;
        if definition.mode() != ValidationMode::Required || parent.status() != ClaimStatus::Posted {
            return Ok((
                Self::derive(
                    &view.state.rows,
                    limits,
                    parent,
                    registry,
                    definition,
                    descriptor,
                    evidence,
                )?,
                None,
            ));
        }

        // Even an isolated parent is protected: a later incoming dependency or
        // subscription must not enlarge this already accepted responsibility.
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
        let charges = graph.budget.charges();
        let claims = charges.changed_rows;
        // The source graph's one nonterminal root event becomes PostFailed;
        // monitor releases can revise that terminal root, but cannot fail it again.
        let original_events = add(3, charges.graph_events)?;
        let original_extras = add(4, charges.monitor_index_rows)?;
        let events = add(original_events, cohort.events())?;
        within(
            crate::native::completion_book::respondent_journal_visits(events, claims)?,
            limits.plan_edges,
        )?;
        let changes = add(
            add(add(claims, original_extras)?, add(original_events, 2)?)?,
            cohort.changed_keys(),
        )?;
        within(changes, limits.range.max_batch_entries)?;
        within(events, limits.plan_edges)?;
        graph.check_journal(original_events, limits)?;
        if crate::native::admission_graph::checker_visits_bound(
            plan.members(),
            original_extras,
            original_events,
        )? > limits.plan_edges
        {
            return Err(NativeError::Capacity("admission graph history visits"));
        }
        // The completion collector also resolves the pinned original failure.
        check_journal_visits(limits, parent, add(original_events, 1)?)?;
        check_cohort_visits(limits, cohort, claims, original_extras, original_events)?;

        // Graph preparation already prices the caller's first root copy, all
        // later peer/monitor copies, registry heaps, and index replay. The exact
        // original report journal and its checked inline proof are additional.
        let fixed = [
            charges.preparation_bytes,
            result_containers()?,
            NativeArtifactInput::container_charge(),
            array::<NativeFact>(original_events)?,
            array::<(Key, usize)>(events)?,
            crate::native::admission_graph::Proof::charge(),
            cohort.construction_bytes()?,
        ]
        .into_iter()
        .try_fold(0, add)?;
        let descriptor = work::capped_descriptor(limits, fixed, descriptor)?;
        let mut result = Self::derive(
            &view.state.rows,
            limits,
            parent,
            registry,
            definition,
            descriptor,
            evidence,
        )?;
        let descriptor_heap = result
            .input_heap
            .checked_sub(NativeArtifactInput::container_charge())
            .ok_or(ContractError::Capacity)?;
        within(add(fixed, descriptor_heap)?, limits.preparation_bytes)?;
        let incoming_heap = [
            descriptor_heap,
            result_containers()?,
            containers(claims)?,
            charges.claim_heap_bytes,
            charges.registry_heap_bytes,
            event_containers(events)?,
            multiply(cohort.evaluations(), OwnedEvaluation::container_charge())?,
        ]
        .into_iter()
        .try_fold(0, add)?;
        let entry = entry_heap(limits)?;
        for bytes in [
            charges.maximum_entry_heap_bytes,
            add(OwnedArtifact::container_charge(), descriptor_heap)?,
            OwnedEvaluation::container_charge(),
            OwnedAccepted::container_charge(),
            OwnedEvent::container_charge(),
        ] {
            within(bytes, entry)?;
        }
        let failed = view.state.rows.future_write_envelope(RangeWriteLimits {
            changed_keys: changes,
            deleted_keys: 0,
            incoming_heap,
            input_capacity: changes,
        })?;
        result.graph = Some(graph);
        result.cohort = cohort;
        result.failed_report = Some(failed);
        let respondent_journal = crate::native::completion_book::respondent_journal_bytes(claims)?;
        result.failed_journal_bytes = add(
            crate::native::completion_book::journal_bytes(add(1, cohort.evaluations())?)?,
            respondent_journal,
        )?;
        let construction = result.report_construction(limits)?;
        construction.check_counts(claims, add(original_extras, cohort.evaluations())?, events)?;
        let count = usize::try_from(result.reports).map_err(|_| ContractError::Capacity)?;
        result.retained = add(
            multiply(
                count.checked_sub(1).ok_or(ContractError::Capacity)?,
                crate::native::mutation::retained(result.ordinary_report)?,
            )?,
            add(
                crate::native::mutation::retained(failed)?,
                result.failed_journal_bytes,
            )?,
        )?;
        let transient = |range: RangeWriteEnvelope| {
            range
                .additional_peak_bytes()
                .checked_sub(range.additional_retained_bytes())
                .ok_or(NativeError::Capacity("range temporary charge"))
        };
        result.workspace = [
            result.input_heap,
            evidence.workspace_bytes,
            evidence.retained_bytes,
            construction.temporary_bytes()?,
            transient(result.ordinary_report)?.max(transient(failed)?),
            graph.check_bytes,
            respondent_journal,
        ]
        .into_iter()
        .try_fold(0, add)?;
        result.required = add(result.retained, result.workspace)?;
        let extra_events = add(charges.graph_events, cohort.events())?;
        result.slot_demand.failure = Some(CompletionSlots {
            events: extra_events,
            new_rows: extra_events,
            ..CompletionSlots::default()
        });
        result.slots = result.slot_demand.remaining(result.reports, true)?;
        result.check_parent(parent, registry)?;
        let members = GraphMembers::copy_from(&view.state.budget, plan.members())?;
        drop(plan);
        drop(reservation);
        Ok((result, Some(members)))
    }
}
