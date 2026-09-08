//! Immutable ownership of the original transaction during history emission.
//! Explicit journals are semantically checked before encapsulation. Implicit
//! claim-history chains remain checked by the single emission pass; this type
//! does not assert that deferred validation has already happened.
//! No history capture buffer or clone is needed: original payloads stay owned
//! and cannot be changed while their original event coordinates are emitted.
use super::*;
use crate::native::cohort_seals::CohortSeals;
use focal_model::lifecycle::validation::SealTransition;

pub(in crate::native) struct OriginalPlan<'source> {
    plan: transactions::Plan,
    extras: Extras,
    source: View<'source>,
    meta: Meta,
    outcome: NativeOutcome,
    limits: NativeLimits,
}

pub(in crate::native) struct SealedPlan<'source> {
    original: OriginalPlan<'source>,
    suffix: CohortSeals,
}

pub(in crate::native) struct SealedChanges {
    pub(in crate::native) changes: Vec<Change<Key, Row>>,
    pub(in crate::native) outcome: NativeOutcome,
    pub(in crate::native) meta: Meta,
    pub(in crate::native) seals: Vec<SealTransition>,
}

impl SealedPlan<'_> {
    pub(in crate::native) fn into_changes(
        self,
        allowance: usize,
        scratch: &mut Scratch,
    ) -> Result<SealedChanges, NativeError> {
        let suffix = if self.suffix.is_empty() {
            None
        } else {
            Some(self.suffix)
        };
        self.original.finish(suffix, allowance, scratch)
    }
}

impl<'source> OriginalPlan<'source> {
    pub(in crate::native) fn check(
        mut plan: transactions::Plan,
        mut extras: Extras,
        meta: Meta,
        outcome: NativeOutcome,
        view: &View<'source>,
        limits: NativeLimits,
        scratch: &mut Scratch,
    ) -> Result<Self, NativeError> {
        plan.rows.sort_unstable_by_key(|row| row.binding().object);
        plan.registry.check_rows(&plan.rows)?;
        crate::native::authored::check_plan(&plan, &extras, view, meta, outcome, limits)?;
        let journaled = extras.journal.is_some();
        let controlled = extras.control_graph.is_some();
        let admission = extras.admission_graph.is_some();
        if (controlled
            && !matches!(
                outcome.operation,
                NativeOperation::Create | NativeOperation::Cancel | NativeOperation::Post
            ))
            || (admission && outcome.operation != NativeOperation::ReportAdmission)
            || (admission && controlled)
            || journaled
                != (history_assembly::explicit(outcome.operation) || controlled || admission)
        {
            return Err(ContractError::InvalidTransition.into());
        }
        if journaled {
            if controlled {
                crate::native::control_graph::check(&plan.rows, &extras, view, outcome, limits)?;
            }
            check_journal(&plan.rows, &extras, view, limits)?;
            let index_rows = crate::native::monitor_index::check_journal(
                view, &plan.rows, &extras, limits, scratch,
            )?;
            if controlled {
                // The retained capability checked the exact original control
                // prefix above; common claim and index checks prove its suffix.
            } else if admission {
                crate::native::admission_graph::check(
                    &plan.rows, &extras, view, outcome, limits, index_rows,
                )?;
            } else if outcome.operation == NativeOperation::ClaimDeadline {
                crate::native::claim_deadlines::check_journal(
                    &plan.rows,
                    &mut extras,
                    view,
                    outcome,
                    limits,
                    index_rows,
                )?;
            } else if outcome.operation == NativeOperation::MonitorDeadline {
                crate::native::monitor_deadlines::check_journal(
                    &plan.rows,
                    &mut extras,
                    view,
                    outcome,
                    limits,
                    index_rows,
                )?;
            } else if matches!(
                outcome.operation,
                NativeOperation::RegisterMonitor
                    | NativeOperation::RebindMonitor
                    | NativeOperation::CancelMonitor
            ) {
                crate::native::monitor_commands::check_journal(
                    &plan.rows, &extras, view, outcome, limits, index_rows,
                )?;
            } else if outcome.operation == NativeOperation::ReleaseScope {
                crate::native::scope_release::check_journal_with_monitors(
                    &plan.rows, &extras, view, outcome, limits, index_rows,
                )?;
            } else {
                crate::native::object_journal::check_with_monitors(
                    &mut extras,
                    view,
                    outcome.operation,
                    outcome.sequence,
                    limits,
                    scratch,
                    index_rows,
                )?;
            }
        }
        Ok(Self {
            plan,
            extras,
            source: View {
                state: view.state,
                tail: view.tail,
            },
            meta,
            outcome,
            limits,
        })
    }

    pub(in crate::native) fn plan(&self) -> &transactions::Plan {
        &self.plan
    }
    pub(in crate::native) fn extras(&self) -> &Extras {
        &self.extras
    }
    pub(in crate::native) fn source(&self) -> &View<'source> {
        &self.source
    }
    pub(in crate::native) fn outcome(&self) -> NativeOutcome {
        self.outcome
    }
    pub(in crate::native) fn meta(&self) -> Meta {
        self.meta
    }
    pub(in crate::native) fn limits(&self) -> NativeLimits {
        self.limits
    }

    /// Build only the separate suffix. Original rows, journals and publication
    /// positions remain inaccessible to mutation until their emission finishes.
    pub(in crate::native) fn with_seals(
        self,
        scratch: &mut Scratch,
    ) -> Result<SealedPlan<'source>, NativeError> {
        let suffix = CohortSeals::prepare(&self, scratch)?;
        Ok(SealedPlan {
            original: self,
            suffix,
        })
    }

    #[cfg(test)]
    pub(super) fn into_changes(
        self,
        allowance: usize,
        scratch: &mut Scratch,
    ) -> Result<Vec<Change<Key, Row>>, NativeError> {
        Ok(self.finish(None, allowance, scratch)?.changes)
    }

    fn finish(
        self,
        mut suffix: Option<CohortSeals>,
        allowance: usize,
        scratch: &mut Scratch,
    ) -> Result<SealedChanges, NativeError> {
        let rows = &self.plan.rows;
        let extras = &self.extras;
        let view = &self.source;
        let original_outcome = self.outcome;
        let mut outcome = original_outcome;
        let mut meta = self.meta;
        let limits = self.limits;
        let additional_evaluations = suffix
            .as_ref()
            .map_or(0, CohortSeals::additional_evaluations);
        if let Some(suffix) = &suffix {
            let events = suffix.events()?;
            outcome.events = outcome
                .events
                .checked_add(
                    u32::try_from(events).map_err(|_| NativeError::Capacity("cohort events"))?,
                )
                .ok_or(NativeError::Capacity("cohort events"))?;
            outcome.evaluations = outcome
                .evaluations
                .checked_add(
                    u32::try_from(additional_evaluations)
                        .map_err(|_| NativeError::Capacity("cohort evaluations"))?,
                )
                .ok_or(NativeError::Capacity("cohort evaluations"))?;
            meta.events = add(meta.events, events)?;
            within(meta.events, limits.events)?;
        }
        let event_charge = event_containers(
            usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("events"))?,
        )?;
        let count = add(
            add(
                rows.len(),
                usize::try_from(outcome.events).map_err(|_| NativeError::Capacity("events"))?,
            )?,
            add(add(2, extras.rows.len())?, additional_evaluations)?,
        )?;
        if count > limits.range.max_batch_entries {
            return Err(NativeError::Capacity(
                "changes including history and outcome",
            ));
        }
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(
            add(
                add(
                    array::<Change<Key, Row>>(changes.capacity())?,
                    array::<History>(rows.len())?,
                )?,
                add(containers(rows.len())?, event_charge)?,
            )?,
            allowance,
        )?;
        let mut history = Vec::new();
        history
            .try_reserve_exact(rows.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(
            add(
                add(
                    array::<Change<Key, Row>>(changes.capacity())?,
                    array::<History>(history.capacity())?,
                )?,
                add(containers(rows.len())?, event_charge)?,
            )?,
            allowance,
        )?;
        let mut ordinal = 0;
        let visited = visit_history(
            rows,
            extras,
            view,
            outcome.operation,
            &mut history,
            |fact| record_fact(&mut changes, outcome, &mut ordinal, fact),
        )?;
        if visited
            != usize::try_from(original_outcome.events)
                .map_err(|_| NativeError::Capacity("events"))?
            || ordinal != original_outcome.events
        {
            return Err(ContractError::InvalidManifest.into());
        }
        if let Some(suffix) = &mut suffix {
            suffix.emit(&self, |fact| {
                record_fact(&mut changes, outcome, &mut ordinal, fact)
            })?;
        }
        if ordinal != outcome.events {
            return Err(ContractError::InvalidManifest.into());
        }
        // Emission has finished and its exact original count was checked. Only
        // now may payload ownership move into final range rows.
        let Self {
            plan,
            mut extras,
            source,
            ..
        } = self;
        let view = &source;
        let seals = if let Some(suffix) = suffix {
            suffix.merge(plan, extras, view, limits, scratch, &mut changes)?
        } else {
            let transactions::Plan { rows, registry, .. } = plan;
            let mut registry = registry.into_iter().peekable();
            for row in rows {
                let id = ClaimId(row.binding().object.0);
                let registrations = if registry.peek().is_some_and(|(owner, _)| *owner == id) {
                    registry.next().ok_or(ContractError::InvalidTarget)?.1
                } else {
                    transactions::copy_registry(view, &row, limits, scratch)?
                };
                let row = OwnedClaim::new(row, registrations)?;
                let heap_bytes = row.heap_charge()?;
                changes.push(Change::Put(Entry::new(
                    Key::Claim(id),
                    Row::Claim(row),
                    heap_bytes,
                )));
            }
            if registry.next().is_some() {
                return Err(ContractError::InvalidTarget.into());
            }
            append_rows(&mut changes, &mut extras)?;
            Vec::new()
        };
        if ordinal != outcome.events {
            return Err(ContractError::InvalidManifest.into());
        }
        if changes.len().checked_add(2) != Some(count) || count > changes.capacity() {
            return Err(ContractError::InvalidManifest.into());
        }
        changes.push(Change::Put(Entry::new(Key::Meta, Row::Meta(meta), 0)));
        changes.push(Change::Put(Entry::new(
            Key::Outcome(outcome.invocation),
            Row::Outcome(outcome),
            0,
        )));
        Ok(SealedChanges {
            changes,
            outcome,
            meta,
            seals,
        })
    }
}
