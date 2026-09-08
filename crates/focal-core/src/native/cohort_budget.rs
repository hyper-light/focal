//! Checked construction and traversal ceilings for an automatic cohort suffix.
//! Actual writers supply present counts; held reports supply immutable future
//! registration ceilings. Neither form grants authority or allocates memory.
use super::prepare::{add, array, event_containers, within};
use super::*;
use focal_model::lifecycle::validation::SealTransition;

fn multiply(left: usize, right: usize) -> Result<usize, NativeError> {
    left.checked_mul(right)
        .ok_or(NativeError::Capacity("cohort multiplication"))
}

fn levels(count: usize) -> Result<usize, NativeError> {
    usize::try_from(
        usize::BITS
            .checked_sub(count.leading_zeros())
            .ok_or(ContractError::Capacity)?,
    )
    .map_err(|_| ContractError::Capacity.into())
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct CohortBudget {
    claims: usize,
    evaluations: usize,
    events: usize,
    changed_keys: usize,
    registry_heap: usize,
    policy_visits: usize,
    member_visits: usize,
    maximum_declarations: usize,
}

impl CohortBudget {
    pub(super) fn empty() -> Self {
        Self::default()
    }

    /// `maximum_rows` includes all legal future members when used for a held
    /// report. Preserve actual spare registry capacity as a heap floor.
    pub(super) fn add_claim(
        mut self,
        claim: &ClaimState,
        registry: &RegistrationSet,
        maximum_rows: usize,
        maximum_registry_heap: usize,
    ) -> Result<Self, NativeError> {
        registry.check(claim)?;
        if registry.is_sealed() {
            return Ok(self);
        }
        within(registry.rows().len(), maximum_rows)?;
        within(maximum_rows, registry.max_rows())?;
        within(
            transactions::registry_heap(registry)?,
            maximum_registry_heap,
        )?;
        let declarations = claim.acceptance().declarations().len();
        let slots = claim.acceptance().slot_count();
        self.claims = add(self.claims, 1)?;
        self.evaluations = add(self.evaluations, maximum_rows)?;
        self.events = add(self.evaluations, self.claims)?;
        self.changed_keys = add(multiply(2, self.evaluations)?, self.claims)?;
        self.registry_heap = add(self.registry_heap, maximum_registry_heap)?;
        self.policy_visits = add(
            self.policy_visits,
            add(multiply(2, declarations)?, add(slots, 1)?)?,
        )?;
        // A model seal validates the complete policy and exact declaration.
        // Include fixed binding/state/membership work around that validation.
        self.member_visits = add(
            self.member_visits,
            multiply(
                maximum_rows,
                add(multiply(3, declarations)?, add(slots, 8)?)?,
            )?,
        )?;
        self.maximum_declarations = self.maximum_declarations.max(declarations);
        self.construction_bytes()?;
        self.incoming_heap()?;
        Ok(self)
    }

    pub(super) fn claims(self) -> usize {
        self.claims
    }
    pub(super) fn evaluations(self) -> usize {
        self.evaluations
    }
    pub(super) fn events(self) -> usize {
        self.events
    }
    pub(super) fn changed_keys(self) -> usize {
        self.changed_keys
    }

    /// All suffix buffers and owned replacements may coexist with the original
    /// plan. Final Changes/history containers and the collector journal have
    /// separate owner reservations and are deliberately excluded here.
    pub(super) fn construction_bytes(self) -> Result<usize, NativeError> {
        [
            array::<SealTransition>(self.evaluations)?,
            array::<super::cohort_seals::SealUpdate>(self.evaluations)?,
            array::<(ClaimId, RegistrationSet)>(self.claims)?,
            self.registry_heap,
            multiply(self.evaluations, OwnedEvaluation::container_charge())?,
        ]
        .into_iter()
        .try_fold(0, add)
    }

    /// Existing claim rows already price their complete registry heap. Only
    /// additional evaluation replacements and new history enter this suffix.
    pub(super) fn incoming_heap(self) -> Result<usize, NativeError> {
        add(
            multiply(self.evaluations, OwnedEvaluation::container_charge())?,
            event_containers(self.events)?,
        )
    }

    /// Two source passes, full semantic checks, two in-place heap sorts, and
    /// final key lookups. The original claim scan is separate from the selected
    /// cohort size because some original rows require no seal.
    pub(super) fn writer_visits(
        self,
        original_claims: usize,
        original_extras: usize,
    ) -> Result<usize, NativeError> {
        let depth = levels(self.evaluations)?;
        let passes = multiply(
            2,
            [
                original_claims,
                self.policy_visits,
                self.member_visits,
                multiply(multiply(2, original_extras)?, self.evaluations)?,
            ]
            .into_iter()
            .try_fold(0, add)?,
        )?;
        [
            passes,
            multiply(multiply(12, self.evaluations)?, depth)?,
            multiply(8, self.evaluations)?,
            multiply(2, self.claims)?,
            multiply(2, original_claims)?,
            original_extras,
            multiply(original_extras, depth)?,
            multiply(multiply(2, self.claims)?, levels(original_claims)?)?,
        ]
        .into_iter()
        .try_fold(0, add)
    }

    /// Two event passes and one heap sort when there are multiple records;
    /// parent-group lookup, canonical token probes, and complete model guards.
    /// This bound also covers Ready members without completion grants.
    pub(super) fn journal_visits(
        self,
        original_events: usize,
        original_records: usize,
    ) -> Result<usize, NativeError> {
        let records = add(original_records, self.evaluations)?;
        let events = add(original_events, self.events)?;
        let depth = levels(records)?;
        [
            multiply(2, events)?,
            self.evaluations,
            multiply(multiply(6, records)?, depth)?,
            multiply(12, records)?,
            multiply(self.evaluations, add(depth, 1)?)?,
            multiply(multiply(2, records)?, levels(self.evaluations)?)?,
            self.policy_visits,
            multiply(2, self.member_visits)?,
            multiply(
                original_records,
                // Include the original Admission failure lookup when a report
                // and automatic seals share this completion journal.
                add(multiply(3, self.maximum_declarations)?, 9)?,
            )?,
        ]
        .into_iter()
        .try_fold(0, add)
    }

    /// Heap/count/semantic-work ceilings are monotone under permitted growth.
    /// Exact component identity remains the graph protection's responsibility.
    pub(super) fn check_within(self, maximum: Self) -> Result<(), NativeError> {
        for (actual, bound) in [
            (self.claims, maximum.claims),
            (self.evaluations, maximum.evaluations),
            (self.registry_heap, maximum.registry_heap),
            (self.policy_visits, maximum.policy_visits),
            (self.member_visits, maximum.member_visits),
            (self.maximum_declarations, maximum.maximum_declarations),
        ] {
            within(actual, bound)?;
        }
        Ok(())
    }
}

/// Resolve the actual pending/committed members before promising their final
/// seal revision. Never infer an effective state from a committed-only store.
pub(super) fn check_source(
    view: &View<'_>,
    claim: &ClaimState,
    registry: &RegistrationSet,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let declarations = claim.acceptance().declarations().len();
    let policy = add(
        multiply(2, declarations)?,
        add(claim.acceptance().slot_count(), 1)?,
    )?;
    let cost = add(
        policy,
        multiply(registry.rows().len(), add(declarations, 4)?)?,
    )?;
    if cost > limits.plan_edges {
        return Err(NativeError::Capacity("cohort source visits"));
    }
    registry.check(claim)?;
    if registry.is_sealed() {
        return Ok(());
    }
    for member in registry.rows() {
        let key = transactions::key_for_registered(ClaimId(claim.binding().object.0), *member);
        let definition = view.definition(key.validation)?;
        claim.acceptance().check_declaration(definition)?;
        let state = view.evaluation(key)?;
        member.check_state(*state, definition)?;
        if !state.state().is_terminal() && state.sealed().is_none() {
            state.binding().next()?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "cohort_budget_tests.rs"]
mod tests;
