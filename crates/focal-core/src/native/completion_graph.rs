//! One retained graph ceiling shared by Admission failure and WholeWork.
use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct CompletionGraph {
    pub(super) budget: graph_effects::ConsequenceBudget,
    pub(super) check_bytes: usize,
    scope_lookup_bound: usize,
}
impl CompletionGraph {
    pub(super) fn from_plan(
        view: &View<'_>,
        plan: &graph_effects::ConsequencePlan<'_>,
        limits: NativeLimits,
    ) -> Result<(Self, CohortBudget), NativeError> {
        let mut cohort = CohortBudget::empty();
        let mut scope_lookup_bound = 0usize;
        let budget = plan.with_future_heaps(|claim| {
            let registry = view
                .owned_claim(ClaimId(claim.binding().object.0))?
                .registrations()
                .ok_or(ContractError::InvalidTarget)?;
            crate::native::response_budget::check_registration_capacity_in(
                view, claim, registry, limits,
            )?;
            crate::native::cohort_budget::check_source(view, claim, registry, limits)?;
            let depth = claim
                .scopes()
                .limits()
                .scopes
                .checked_ilog2()
                .map(|depth| usize::try_from(depth).map_err(|_| ContractError::Capacity))
                .transpose()?
                .map(|depth| add(depth, 1))
                .transpose()?
                .unwrap_or(0);
            scope_lookup_bound = scope_lookup_bound.max(add(depth, 1)?);
            let bound = parent_bound(limits, claim, registry)?;
            let (members, _) = registry_bound(limits, claim, registry)?;
            cohort = cohort.add_claim(claim, registry, members, bound.1)?;
            Ok(bound)
        })?;
        Ok((
            Self {
                check_bytes: budget.charges().preparation_bytes,
                budget,
                scope_lookup_bound,
            },
            cohort,
        ))
    }
    /// The common claim checker scans each claim/event pair twice and
    /// resolves each actual monitor event through its owner's bounded registry.
    pub(super) fn check_journal(
        self,
        events: usize,
        limits: NativeLimits,
    ) -> Result<(), NativeError> {
        let charges = self.budget.charges();
        let visits = add(
            multiply(2, multiply(charges.changed_rows, events)?)?,
            multiply(charges.monitor_events, self.scope_lookup_bound)?,
        )?;
        if visits > limits.plan_edges {
            return Err(NativeError::Capacity("graph claim journal visits"));
        }
        Ok(())
    }
    pub(super) fn check(
        self,
        envelope: &CompletionEnvelope,
        view: &View<'_>,
        parent: &ClaimState,
        limits: NativeLimits,
        members: &[ClaimId],
        scratch: &mut prepare::Scratch,
    ) -> Result<(), NativeError> {
        let started = scratch.used;
        let plan = graph_effects::preflight_with_members(
            view,
            parent,
            limits,
            Some(self.budget),
            members,
            scratch,
        )?;
        let mut cohort = CohortBudget::empty();
        for claim in plan.members() {
            if claim.is_terminal()
                && claim.binding().object != parent.binding().object
                && !claim.scopes().iter().any(|scope| scope.active())
            {
                continue;
            }
            let registry = view
                .owned_claim(ClaimId(claim.binding().object.0))?
                .registrations()
                .ok_or(ContractError::InvalidTarget)?;
            crate::native::cohort_budget::check_source(view, claim, registry, limits)?;
            let (members, registry_heap) = registry_bound(limits, claim, registry)?;
            cohort = cohort.add_claim(claim, registry, members, registry_heap)?;
        }
        cohort.check_within(envelope.cohort)?;
        within(
            scratch
                .used
                .checked_sub(started)
                .ok_or(ContractError::Capacity)?,
            self.check_bytes,
        )?;
        Ok(())
    }
}
impl CompletionEnvelope {
    pub(in crate::native) fn has_graph(self) -> bool {
        self.graph.is_some()
    }
    pub(in crate::native) fn graph_check_bytes(self) -> usize {
        self.graph.map_or(0, |graph| graph.check_bytes)
    }
    pub(in crate::native) fn check_graph_with_members(
        &self,
        view: &View<'_>,
        limits: NativeLimits,
        members: &[ClaimId],
        scratch: &mut prepare::Scratch,
    ) -> Result<(), NativeError> {
        let graph = self.graph.ok_or(ContractError::InvalidTarget)?;
        if self.work.is_some() {
            return self.check_work_with_members(view, limits, members, scratch);
        }
        if self.target != CompletionTarget::Admission {
            return Err(ContractError::InvalidTarget.into());
        }
        let parent = view
            .claim(ClaimId(self.parent.object.0))
            .ok_or(ContractError::InvalidTarget)?;
        // Once receipt or a terminal outcome closes Admission, an already begun
        // check is independent audit evidence and causes no future graph write.
        if parent.status() != ClaimStatus::Posted {
            return Ok(());
        }
        let registry = view
            .owned_claim(ClaimId(parent.binding().object.0))?
            .registrations()
            .ok_or(ContractError::InvalidTarget)?;
        self.check_parent(parent, registry)?;
        graph.check(self, view, parent, limits, members, scratch)
    }
    /// Runtime and admission use the same count and pending-buffer ceiling.
    /// Graph counts include the Posted parent's one PostFailed transition.
    pub(in crate::native) fn report_construction(
        self,
        limits: NativeLimits,
    ) -> Result<ConstructionBudget, NativeError> {
        let operation = match self.target {
            CompletionTarget::Admission => NativeOperation::ReportAdmission,
            CompletionTarget::Increment => NativeOperation::ReportIncrement,
            CompletionTarget::Work => NativeOperation::ReportWork,
        };
        let mut result = ConstructionBudget::for_operation(operation, limits)?;
        if self.target == CompletionTarget::Admission
            && let Some(graph) = self.graph
        {
            let charges = graph.budget.charges();
            result = result.with_graph(
                charges.changed_rows,
                add(4, charges.monitor_index_rows)?,
                add(3, charges.graph_events)?,
                limits,
            )?;
        }
        result.with_cohort(self.cohort, limits)
    }
}
