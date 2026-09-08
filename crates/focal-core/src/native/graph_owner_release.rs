//! Owner release has a separate checked entry because its one authorized scope
//! change must not weaken the unchanged-scope guard used by ordinary reports.
use super::*;
use focal_model::lifecycle::scope;

/// Complete original graph and ordered peers for the model's bounded release
/// builder. The source View stays borrowed through application and propagation.
pub(in crate::native) struct OwnerReleasePlan<'a, 'b> {
    view: &'a View<'b>,
    source: &'a ClaimState,
    plan: ConsequencePlan<'a>,
    peers: Vec<&'a ClaimState>,
    cut: ClaimCut,
    limits: NativeLimits,
}

/// Only consuming a real checked OwnerReleased transition constructs this
/// capability. No mutable accessor or caller-selected scope replacement exists.
pub(in crate::native) struct ReleasedRoot<'a, 'b> {
    view: &'a View<'b>,
    source: &'a ClaimState,
    claim: ClaimState,
    cut: ClaimCut,
    limits: NativeLimits,
}

pub(in crate::native) fn owner_release<'a, 'b>(
    view: &'a View<'b>,
    source: &'a ClaimState,
    cut: ClaimCut,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<OwnerReleasePlan<'a, 'b>, NativeError> {
    check_cut(view, cut)?;
    let actual = view
        .claim(ClaimId(source.binding().object.0))
        .ok_or(ContractError::InvalidTarget)?;
    if actual != source {
        return Err(ContractError::ContentConflict.into());
    }
    if !source.is_terminal() || source.scopes().released() {
        return Err(ContractError::InvalidTransition.into());
    }
    source.binding().next()?;
    let plan = preflight(view, source, limits, None, scratch)?;
    let count = plan
        .members()
        .len()
        .checked_sub(1)
        .ok_or(ContractError::InvalidManifest)?;
    let mut peer_rows = scratch.reserve(count)?;
    // Borrow the retained source references, not references whose lifetime is
    // shortened to the temporary plan facade; the plan moves with this buffer.
    peers(
        &mut peer_rows,
        &plan.claims,
        ClaimId(source.binding().object.0),
    )?;
    Ok(OwnerReleasePlan {
        view,
        source,
        plan,
        peers: peer_rows,
        cut,
        limits,
    })
}

impl<'a, 'b> OwnerReleasePlan<'a, 'b> {
    pub(in crate::native) fn snapshot(&self) -> &graph::Snapshot {
        &self.plan.snapshot
    }

    pub(in crate::native) fn peers(&self) -> &[&ClaimState] {
        &self.peers
    }

    pub(in crate::native) fn apply(
        self,
        mut root: ClaimState,
        transition: scope::Transition,
    ) -> Result<ReleasedRoot<'a, 'b>, NativeError> {
        if root != *self.source {
            return Err(ContractError::ContentConflict.into());
        }
        if transition.event() != (scope::Event::OwnerReleased { cut: self.cut }) {
            return Err(ContractError::InvalidTransition.into());
        }
        root.apply_scope(&self.source.binding(), transition, &self.peers)?;
        self.source.binding().next()?.check(&root.binding())?;
        if root.status() != self.source.status()
            || root.terminal_cut() != self.source.terminal_cut()
            || root.local_sealed_at() != self.source.local_sealed_at()
            || root.local_complete() != self.source.local_complete()
            || root.receipt() != self.source.receipt()
            || root.scopes().release_cut() != Some(self.cut)
        {
            return Err(ContractError::InvalidTransition.into());
        }
        // Owner release changes only inline cut/released fields. No monitor or
        // child membership can be added by this capability.
        within(heap(&root)?, heap(self.source)?)?;
        Ok(ReleasedRoot {
            view: self.view,
            source: self.source,
            claim: root,
            cut: self.cut,
            limits: self.limits,
        })
    }
}

impl ReleasedRoot<'_, '_> {
    pub(in crate::native) fn claim(&self) -> &ClaimState {
        &self.claim
    }
}

pub(in crate::native) fn prepare_released(
    released: ReleasedRoot<'_, '_>,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    // These are the limits used to discover and quote the complete original
    // graph, not a second independently widened traversal or topology policy.
    if limits.plan_nodes != released.limits.plan_nodes
        || limits.plan_edges != released.limits.plan_edges
        || limits.preparation_bytes != released.limits.preparation_bytes
    {
        return Err(ContractError::InvalidPolicy.into());
    }
    check_cut(released.view, released.cut)?;
    let actual = released
        .view
        .claim(ClaimId(released.source.binding().object.0))
        .ok_or(ContractError::InvalidTarget)?;
    if actual != released.source {
        return Err(ContractError::ContentConflict.into());
    }
    prepare_checked(
        released.view,
        released.claim,
        released.cut,
        limits,
        extras,
        scratch,
    )
}

#[cfg(test)]
#[path = "graph_owner_release_tests.rs"]
mod tests;
