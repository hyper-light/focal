//! Atomic dependency consequences over the complete indexed affected closure.
//! All witnesses read one frozen snapshot: the supplied new root and original
//! effective peers. Replacement rows never become accidental proof inputs.
use super::prepare::{Extras, Scratch, heap, within};
use super::*;
use focal_model::{
    Cause,
    lifecycle::{claim::ClaimCut, graph},
};

struct Closure<'a> {
    claims: Vec<&'a ClaimState>,
    seen: Vec<ClaimId>,
    visits: usize,
    maximum: usize,
}
impl<'a> Closure<'a> {
    fn charge(&mut self, amount: usize) -> Result<(), NativeError> {
        self.visits = self
            .visits
            .checked_sub(amount)
            .ok_or(NativeError::Capacity("graph consequence discovery"))?;
        Ok(())
    }
    fn source(
        view: &'a View<'_>,
        root: &'a ClaimState,
        id: ClaimId,
    ) -> Result<&'a ClaimState, NativeError> {
        if root.binding().object.0 == id.0 {
            Ok(root)
        } else {
            view.claim(id).ok_or(ContractError::InvalidTarget.into())
        }
    }
    fn insert(
        &mut self,
        view: &'a View<'_>,
        root: &'a ClaimState,
        id: ClaimId,
    ) -> Result<(), NativeError> {
        self.charge(1)?;
        let position = match self.seen.binary_search(&id) {
            Ok(_) => return Ok(()),
            Err(position) => position,
        };
        if self.claims.len() >= self.maximum
            || self.claims.len() == self.claims.capacity()
            || self.seen.len() == self.seen.capacity()
            || position > self.seen.len()
        {
            return Err(NativeError::Capacity("graph consequence nodes"));
        }
        let claim = Self::source(view, root, id)?;
        if claim.binding().ledger != view.ledger() {
            return Err(ContractError::WrongLedger.into());
        }
        if id.is_zero() || claim.binding().object.0 != id.0 || claim.binding().revision.0 == 0 {
            return Err(ContractError::InvalidTarget.into());
        }
        if claim.created().0 == 0 || claim.created() > view.prefix() {
            return Err(ContractError::InvalidCut.into());
        }
        self.seen.insert(position, id);
        self.claims.push(claim);
        Ok(())
    }
}

fn child(owner: &ClaimState, child: &ClaimState) -> Result<(), NativeError> {
    let id = ClaimId(child.binding().object.0);
    let index = owner
        .scopes()
        .children()
        .binary_search_by_key(&id, |row| row.id())
        .map_err(|_| ContractError::InvalidManifest)?;
    let registered = owner
        .scopes()
        .children()
        .get(index)
        .ok_or(ContractError::InvalidManifest)?;
    registered.binding().check(&Binding {
        revision: registered.binding().revision,
        ..child.binding()
    })?;
    if child.binding().revision < registered.binding().revision
        || child.lineage().cause() != &Cause::Claim(ClaimId(owner.binding().object.0))
        || child.created() != registered.registered()
        || child.created() < owner.created()
    {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(())
}

fn closure<'a>(
    view: &'a View<'_>,
    root: &'a ClaimState,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<Vec<&'a ClaimState>, NativeError> {
    let mut closure = Closure {
        claims: scratch.reserve(limits.plan_nodes)?,
        seen: scratch.reserve(limits.plan_nodes)?,
        visits: limits.plan_edges,
        maximum: limits.plan_nodes,
    };
    closure.insert(view, root, ClaimId(root.binding().object.0))?;
    let mut cursor = 0usize;
    while let Some(source) = closure.claims.get(cursor).copied() {
        let id = ClaimId(source.binding().object.0);
        for obligation in source.graph().obligations() {
            closure.insert(view, root, obligation.target)?;
        }
        for scope in source.scopes().iter() {
            closure.charge(1)?;
            if scope.released().is_none() {
                // Native scope writers/import are not enabled. Their reverse
                // roots need an index before this closure can propagate them.
                return Err(ContractError::InvalidPolicy.into());
            }
        }
        for registered in source.scopes().children() {
            closure.charge(1)?;
            let actual = Closure::source(view, root, registered.id())?;
            child(source, actual)?;
            closure.insert(view, root, registered.id())?;
        }
        if let Cause::Claim(owner) = *source.lineage().cause() {
            closure.charge(1)?;
            let parent = Closure::source(view, root, owner)?;
            child(parent, source)?;
            closure.insert(view, root, owner)?;
        }
        // incoming() preflights its entire chain. Mirror its conservative cost
        // in the shared closure allowance so repeated reverse walks cannot each
        // consume an independent full-budget allowance.
        let incoming_limits = NativeLimits {
            plan_edges: closure.visits,
            ..limits
        };
        closure.charge(2)?;
        for dependent in super::incoming_graph::incoming(view, id, incoming_limits) {
            let dependent = dependent?;
            let depth = dependent
                .graph()
                .obligations()
                .len()
                .checked_ilog2()
                .map_or(Ok(0usize), |depth| {
                    usize::try_from(depth)
                        .map_err(|_| ContractError::Capacity)
                        .and_then(|depth| depth.checked_add(1).ok_or(ContractError::Capacity))
                })?;
            let charge = depth
                .checked_mul(4)
                .and_then(|charge| charge.checked_add(4))
                .ok_or(ContractError::Capacity)?;
            closure.charge(charge)?;
            closure.insert(view, root, ClaimId(dependent.binding().object.0))?;
        }
        cursor = cursor
            .checked_add(1)
            .ok_or(NativeError::Capacity("graph consequence cursor"))?;
    }
    closure
        .claims
        .sort_unstable_by_key(|claim| claim.binding().object);
    Ok(closure.claims)
}

fn peers<'a>(
    buffer: &mut Vec<&'a ClaimState>,
    claims: &[&'a ClaimState],
    id: ClaimId,
) -> Result<(), NativeError> {
    buffer.clear();
    for source in claims {
        if source.binding().object.0 == id.0 {
            continue;
        }
        if buffer.len() == buffer.capacity() {
            return Err(NativeError::Capacity("graph consequence peers"));
        }
        buffer.push(*source);
    }
    Ok(())
}

fn journal(
    extras: &mut Extras,
    before: Binding,
    after: &ClaimState,
    kind: NativeEventKind,
) -> Result<(), NativeError> {
    if before.next()? != after.binding() {
        return Err(ContractError::StaleRevision.into());
    }
    extras.record(NativeFact::Claim(NativeClaimEvent {
        kind,
        owned_child: None,
        before: Some(before),
        after: after.binding(),
        status: after.status(),
    }))
}

fn copy(source: &ClaimState, scratch: &mut Scratch) -> Result<ClaimState, NativeError> {
    let charge = heap(source)?;
    scratch.charge(charge)?;
    let copied = source.try_copy(source.retained_bytes()?)?;
    within(heap(&copied)?, charge)?;
    Ok(copied)
}

/// Root's earlier local-outcome transitions are already in Extras' journal.
/// This function returns the moved final root and all additionally changed
/// claims; the enclosing transaction publishes their rows and history together.
pub(super) fn prepare(
    view: &View<'_>,
    mut root: ClaimState,
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    if cut.position.0 == 0
        || cut.cause == ContentHash([0; 32])
        || view.prefix().0.checked_add(1) != Some(cut.position.0)
    {
        return Err(ContractError::InvalidCut.into());
    }
    let root_id = ClaimId(root.binding().object.0);
    let original = view.claim(root_id).ok_or(ContractError::InvalidTarget)?;
    original.binding().check(&Binding {
        revision: original.binding().revision,
        ..root.binding()
    })?;
    if root.binding().revision < original.binding().revision
        || root.created() != original.created()
        || root.graph() != original.graph()
        || root.lineage() != original.lineage()
        || root.scopes() != original.scopes()
    {
        return Err(ContractError::InvalidTarget.into());
    }
    let claims = closure(view, &root, limits, scratch)?;
    let count = claims.len();
    let plan = graph::Snapshot::prepare_capture(
        &claims,
        graph::Limits {
            nodes: limits.plan_nodes,
            edges: limits.plan_edges,
            visits: limits.plan_edges,
        },
        scratch.remaining()?,
    )?;
    let charge = plan.construction_charge();
    scratch.charge(charge)?;
    let snapshot = plan.build()?;
    within(snapshot.retained_charge()?, charge)?;
    snapshot.check_cut(cut.position)?;
    // One reusable peak allowance covers each separately dropped failure
    // witness. No witness survives into the next candidate's construction.
    let failure_charge = snapshot.dependency_failure_charge()?;
    scratch.charge(failure_charge)?;
    let mut output = scratch.reserve::<ClaimState>(count)?;
    let mut peer_rows = scratch
        .reserve::<&ClaimState>(count.checked_sub(1).ok_or(ContractError::InvalidManifest)?)?;
    for source in &claims {
        let id = ClaimId(source.binding().object.0);
        if id == root_id || source.is_terminal() {
            continue;
        }
        peers(&mut peer_rows, &claims, id)?;
        let changed = match snapshot.dependency_failure_with_budget(id, failure_charge) {
            Ok(failure) => {
                let mut changed = copy(source, scratch)?;
                changed.dependency_failed(&source.binding(), &failure, &peer_rows, cut.position)?;
                journal(
                    extras,
                    source.binding(),
                    &changed,
                    NativeEventKind::DependencyFailed,
                )?;
                Some(changed)
            }
            Err(ContractError::InvalidTransition) => {
                if source.status() == ClaimStatus::Validating
                    && source.local_complete()
                    && snapshot.satisfied(id)?
                {
                    let release = snapshot.release(id)?;
                    let mut changed = copy(source, scratch)?;
                    changed.graph_release(&source.binding(), &release, &peer_rows, cut.position)?;
                    journal(
                        extras,
                        source.binding(),
                        &changed,
                        NativeEventKind::Satisfied,
                    )?;
                    Some(changed)
                } else {
                    None
                }
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(changed) = changed {
            if output.len() == output.capacity() {
                return Err(NativeError::Capacity("graph changed rows"));
            }
            output.push(changed);
        }
    }
    // Drop every reference that could borrow the owned root. Rebuild its peer
    // list directly from the immutable View, keeping the same captured bindings.
    drop(peer_rows);
    drop(claims);
    let mut root_peers = scratch
        .reserve::<&ClaimState>(count.checked_sub(1).ok_or(ContractError::InvalidManifest)?)?;
    for binding in snapshot.bindings() {
        if binding.object.0 == root_id.0 {
            continue;
        }
        let source = view
            .claim(ClaimId(binding.object.0))
            .ok_or(ContractError::InvalidTarget)?;
        binding.check(&source.binding())?;
        if root_peers.len() == root_peers.capacity() {
            return Err(NativeError::Capacity("graph root peers"));
        }
        root_peers.push(source);
    }
    if !root.is_terminal() {
        let before = root.binding();
        match snapshot.dependency_failure_with_budget(root_id, failure_charge) {
            Ok(failure) => {
                root.dependency_failed(&before, &failure, &root_peers, cut.position)?;
                journal(extras, before, &root, NativeEventKind::DependencyFailed)?;
            }
            Err(ContractError::InvalidTransition) => {
                if root.status() == ClaimStatus::Validating
                    && root.local_complete()
                    && snapshot.satisfied(root_id)?
                {
                    let release = snapshot.release(root_id)?;
                    root.graph_release(&before, &release, &root_peers, cut.position)?;
                    journal(extras, before, &root, NativeEventKind::Satisfied)?;
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    if output.len() == output.capacity() {
        return Err(NativeError::Capacity("graph root row"));
    }
    output.push(root);
    output.sort_unstable_by_key(|claim| claim.binding().object);
    Ok(output)
}

#[cfg(test)]
#[path = "graph_effects_tests.rs"]
mod tests;
