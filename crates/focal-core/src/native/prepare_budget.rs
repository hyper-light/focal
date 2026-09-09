//! Private construction ceilings for one checked native operation. These cover
//! temporary owned rows and change/history buffers, not future completion work
//! or range-page/directory construction. A quote does not reserve its allowance.

use super::prepare::{Extra, add, array, containers, event_containers};
use super::{
    Key, NativeError, NativeLimits, NativeOperation, OwnedEvaluation, OwnedWork, Row, claim_changes,
};
use focal_memory::Change;

#[cfg(test)]
#[path = "prepare_budget_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy)]
pub(super) struct ConstructionBudget {
    pub(super) operation: NativeOperation,
    pub(super) scratch_bytes: usize,
    pub(super) changes_bytes: usize,
    pub(super) extras_count: usize,
    // Original Extras storage and final replacement rows have separate owners.
    // A suffix can replace/add evaluations without growing the original vector.
    pub(super) max_extra_rows: usize,
    pub(super) extras_bytes: usize,
    pub(super) max_claim_rows: usize,
    pub(super) max_changes: usize,
    pub(super) max_events: usize,
    /// Secondary index rows (doc 22 §7) the operation can write, funded
    /// beside its primary rows and bounded by the same batch.
    pub(super) max_index_rows: usize,
    /// The due-timer rows among `max_index_rows` this operation can retire:
    /// the deletions a write envelope must admit beyond status moves.
    pub(super) max_timer_rows: usize,
}

fn changes_bytes(
    max_changes: usize,
    max_claim_rows: usize,
    max_events: usize,
    max_index_rows: usize,
) -> Result<usize, NativeError> {
    add(
        add(
            add(
                array::<Change<Key, Row>>(max_changes)?,
                array::<claim_changes::History>(max_claim_rows)?,
            )?,
            add(containers(max_claim_rows)?, event_containers(max_events)?)?,
        )?,
        array::<super::index_rows::IndexChange>(max_index_rows)?,
    )
}

impl ConstructionBudget {
    pub(super) fn for_operation(
        operation: NativeOperation,
        limits: NativeLimits,
    ) -> Result<Self, NativeError> {
        let batch = limits.range.max_batch_entries;
        let (scratch_bytes, max_claim_rows, max_changes, extras_count, max_events) = match operation
        {
            // Import never prepares a mutation; it is a one-time translation (23 §5).
            NativeOperation::Import => {
                return Err(focal_model::lifecycle::ContractError::InvalidTransition.into());
            }
            NativeOperation::AdoptReceipt => (
                limits.preparation_bytes,
                1.min(limits.plan_nodes),
                batch,
                batch / 2,
                batch,
            ),
            NativeOperation::GenerateResultTestament => (
                limits.preparation_bytes,
                0,
                5.min(batch),
                2.min(batch),
                1.min(batch),
            ),
            NativeOperation::PostResultTestament => (
                limits.preparation_bytes,
                0,
                4.min(batch),
                1.min(batch),
                1.min(batch),
            ),
            NativeOperation::SealIncrementTargets => (
                limits.preparation_bytes,
                1.min(limits.plan_nodes),
                4.min(batch),
                0,
                1.min(batch),
            ),
            NativeOperation::FailWorkProduction => (
                limits.preparation_bytes,
                0,
                6.min(batch),
                3.min(batch),
                1.min(batch),
            ),
            NativeOperation::RejectWork => (
                limits.preparation_bytes,
                0,
                7.min(batch),
                3.min(batch),
                2.min(batch),
            ),
            NativeOperation::SubmitWork => (
                limits.preparation_bytes,
                1.min(limits.plan_nodes),
                batch,
                add(batch / 2, 1)?.min(batch),
                batch,
            ),
            NativeOperation::SubmitDiagnostic => (
                limits.preparation_bytes,
                0,
                8.min(batch),
                4.min(batch),
                2.min(batch),
            ),
            NativeOperation::ReceiveWork => (
                OwnedWork::container_charge().min(limits.preparation_bytes),
                0,
                4.min(batch),
                1.min(batch),
                1.min(batch),
            ),
            NativeOperation::PostResponse => (
                limits.preparation_bytes,
                1.min(limits.plan_nodes),
                6.min(batch),
                1.min(batch),
                2.min(batch),
            ),
            NativeOperation::CloseResponse => (
                limits.preparation_bytes,
                1.min(limits.plan_nodes),
                batch,
                batch / 2,
                batch,
            ),
            NativeOperation::ReceiveResponse => (
                limits.preparation_bytes,
                1.min(limits.plan_nodes),
                batch,
                batch / 2,
                batch,
            ),
            NativeOperation::BeginAdmission
            | NativeOperation::BeginIncrement
            | NativeOperation::EvaluationDeadline => (
                OwnedEvaluation::container_charge().min(limits.preparation_bytes),
                0,
                4.min(batch),
                1.min(batch / 2),
                1.min(batch),
            ),
            NativeOperation::ReportAdmission => (
                limits.preparation_bytes,
                1.min(limits.plan_nodes),
                11.min(batch),
                4.min(batch / 2),
                4.min(batch),
            ),
            NativeOperation::ReportIncrement => (
                limits.preparation_bytes,
                0,
                9.min(batch),
                4.min(batch / 2),
                3.min(batch),
            ),
            NativeOperation::Create
            | NativeOperation::MonitorDeadline
            | NativeOperation::RegisterMonitor
            | NativeOperation::RebindMonitor
            | NativeOperation::CancelMonitor => (
                limits.preparation_bytes,
                limits.plan_nodes,
                batch,
                batch,
                batch,
            ),
            NativeOperation::BeginWork
            | NativeOperation::ReportWork
            | NativeOperation::EnterWholeWork
            | NativeOperation::Cancel
            | NativeOperation::ClaimDeadline
            | NativeOperation::ReleaseScope
            | NativeOperation::Post
            | NativeOperation::AcquireReceipt => (
                limits.preparation_bytes,
                limits.plan_nodes,
                batch,
                batch / 2,
                batch,
            ),
        };
        // Extras grows by constructing a replacement before dropping the old
        // vector. Keep both maximum buffers charged, including their separate
        // allocator bookkeeping; nested row heaps consume Scratch instead.
        let extras_array = array::<Extra>(extras_count)?;
        let extras_bytes = add(extras_array, extras_array)?;
        let max_index_rows = super::index_rows::bound(
            operation,
            max_claim_rows,
            extras_count,
            max_events,
            super::index_rows::input_bound(limits),
            batch,
        )?;
        let max_timer_rows =
            super::index_rows::timer_bound(operation, max_claim_rows, extras_count, max_events)?
                .min(max_index_rows);
        let max_changes = add(max_changes, max_index_rows)?.min(batch);
        let changes_bytes = changes_bytes(max_changes, max_claim_rows, max_events, max_index_rows)?;
        let budget = Self {
            operation,
            scratch_bytes,
            changes_bytes,
            extras_count,
            max_extra_rows: extras_count,
            extras_bytes,
            max_claim_rows,
            max_changes,
            max_events,
            max_index_rows,
            max_timer_rows,
        };
        // Detect overflow of the complete reservation before any allocation.
        budget.pending_bytes()?;
        Ok(budget)
    }

    pub(super) fn pending_bytes(self) -> Result<usize, NativeError> {
        add(self.temporary_bytes()?, self.mutation_bytes()?)
    }

    pub(super) fn mutation_bytes(self) -> Result<usize, NativeError> {
        super::mutation::bytes(self.max_changes)
    }

    pub(super) fn temporary_bytes(self) -> Result<usize, NativeError> {
        add(
            add(self.scratch_bytes, self.changes_bytes)?,
            self.extras_bytes,
        )
    }

    /// Expand original transaction buffers to the complete checked graph shape.
    /// Cohort suffixes are added separately: their replacement rows do not grow
    /// the original Extras vector. No count depends on unrelated ledger rows.
    pub(super) fn with_graph(
        mut self,
        claims: usize,
        extras: usize,
        events: usize,
        limits: NativeLimits,
    ) -> Result<Self, NativeError> {
        self.max_claim_rows = self.max_claim_rows.max(claims);
        self.extras_count = self.extras_count.max(extras);
        self.max_extra_rows = self.max_extra_rows.max(extras);
        self.max_events = self.max_events.max(events);
        // Every graph consequence may fence an evaluation or dispose of a
        // monitor and retire its due timer (doc 22 §7).
        let graph_timers = add(extras, events)?;
        self.max_index_rows = self
            .max_index_rows
            .max(add(
                super::index_rows::bound(
                    self.operation,
                    self.max_claim_rows,
                    self.max_extra_rows,
                    self.max_events,
                    super::index_rows::input_bound(limits),
                    limits.range.max_batch_entries,
                )?,
                graph_timers,
            )?)
            .min(limits.range.max_batch_entries);
        self.max_timer_rows = add(self.max_timer_rows, graph_timers)?.min(self.max_index_rows);
        self.max_changes = self.max_changes.max(add(
            add(self.max_claim_rows, self.max_extra_rows)?,
            add(add(self.max_events, 2)?, self.max_index_rows)?,
        )?);
        super::prepare::within(self.max_claim_rows, limits.plan_nodes)?;
        super::prepare::within(self.max_changes, limits.range.max_batch_entries)?;
        let extras_bytes = array::<Extra>(self.extras_count)?;
        self.extras_bytes = add(extras_bytes, extras_bytes)?;
        self.changes_bytes = changes_bytes(
            self.max_changes,
            self.max_claim_rows,
            self.max_events,
            self.max_index_rows,
        )?;
        self.pending_bytes()?;
        Ok(self)
    }

    pub(super) fn with_cohort(
        mut self,
        cohort: super::cohort_budget::CohortBudget,
        limits: NativeLimits,
    ) -> Result<Self, NativeError> {
        let batch = limits.range.max_batch_entries;
        // Every sealed cohort claim may leave one status and enter another,
        // and every sealed evaluation and terminal claim retires its timer.
        let cohort_timers = super::index_rows::timer_rows(0, cohort.evaluations(), 0)?;
        let cohort_index = add(add(cohort.claims(), cohort.claims())?, cohort_timers)?;
        self.max_events = add(self.max_events, cohort.events())?.min(batch);
        self.max_extra_rows = add(self.max_extra_rows, cohort.evaluations())?.min(batch);
        self.max_index_rows = self.max_index_rows.max(super::index_rows::bound(
            self.operation,
            self.max_claim_rows,
            self.max_extra_rows,
            self.max_events,
            super::index_rows::input_bound(limits),
            batch,
        )?);
        self.max_index_rows = add(self.max_index_rows, cohort_index)?.min(batch);
        self.max_timer_rows = add(self.max_timer_rows, cohort_timers)?.min(self.max_index_rows);
        self.max_changes =
            add(self.max_changes, add(cohort.changed_keys(), cohort_index)?)?.min(batch);
        // The suffix's nested allocations share the already reserved Scratch.
        // Its producer must fit the complete prefix and suffix in that ceiling.
        super::prepare::within(cohort.construction_bytes()?, self.scratch_bytes)?;
        self.changes_bytes = changes_bytes(
            self.max_changes,
            self.max_claim_rows,
            self.max_events,
            self.max_index_rows,
        )?;
        self.pending_bytes()?;
        Ok(self)
    }

    /// Check actual output counts before allocating the final change vector.
    /// Operation ceilings are not minimum requirements: a report without a
    /// derived claim failure needs nine changes even though its ceiling is eleven.
    /// Meta and the exact-request outcome always occupy two additional rows.
    pub(super) fn check_counts(
        self,
        claims: usize,
        extras: usize,
        events: usize,
        index: usize,
    ) -> Result<(), NativeError> {
        if claims > self.max_claim_rows {
            return Err(NativeError::Capacity("construction claim rows"));
        }
        if extras > self.max_extra_rows {
            return Err(NativeError::Capacity("construction extra rows"));
        }
        if events > self.max_events {
            return Err(NativeError::Capacity("construction events"));
        }
        if index > self.max_index_rows {
            return Err(NativeError::Capacity("construction index rows"));
        }
        let changes = add(add(claims, extras)?, add(add(events, 2)?, index)?)?;
        if changes > self.max_changes {
            return Err(NativeError::Capacity("construction changes"));
        }
        Ok(())
    }
}
