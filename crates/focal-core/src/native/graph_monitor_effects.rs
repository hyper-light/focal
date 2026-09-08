//! Checked monitor edits and atomic fixed-point consequences. Every mutation
//! consumes a model witness over the complete current replacement snapshot.
use super::*;
use focal_model::{WaitPredicate, lifecycle::scope};

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct MonitorBudget {
    pub events: usize,
    pub index_rows: usize,
    pub construction_bytes: usize,
    pub visits: usize,
    pub writer_visits: usize,
}

fn multiply(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_mul(b).ok_or(ContractError::Capacity.into())
}
pub(super) fn target(root: WaitPredicate) -> ClaimId {
    match root {
        WaitPredicate::Satisfied(id)
        | WaitPredicate::Terminal(id)
        | WaitPredicate::Released(id) => id,
    }
}
fn active(claim: &ClaimState) -> usize {
    claim.scopes().iter().filter(|scope| scope.active()).count()
}
pub(super) fn future_copy_growth(claim: &ClaimState, growth: usize) -> Result<usize, NativeError> {
    multiply(
        add(active(claim), usize::from(!claim.is_terminal()))?,
        growth,
    )
}

// Dispositions may progress without changing this original topology. Rebinding
// an edge inside the same connected component must still invalidate its quote.
pub(super) fn hash_topology(hash: &mut blake3::Hasher, claim: &ClaimState) {
    for scope in claim.scopes().iter() {
        hash.update(b"monitor");
        hash.update(&scope.id().0);
        hash.update(&scope.registered().0.to_be_bytes());
        let deadline = scope.deadline();
        hash.update(&deadline.timer.0);
        hash.update(&deadline.generation.to_be_bytes());
        hash.update(&deadline.at.to_be_bytes());
        for root in scope.roots() {
            hash.update(&[match root {
                WaitPredicate::Satisfied(_) => 0,
                WaitPredicate::Terminal(_) => 1,
                WaitPredicate::Released(_) => 2,
            }]);
            hash.update(&target(*root).0);
        }
        hash.update(b"end-roots");
        if let Some(change) = scope.last_rebinding() {
            hash.update(&[1]);
            hash.update(&change.predecessor.0);
            hash.update(&change.successor.0);
            hash.update(&change.cut.position.0.to_be_bytes());
            hash.update(&change.cut.cause.0);
        } else {
            hash.update(&[0]);
        }
    }
    hash.update(b"end-monitors");
}

/// Additional cumulative construction debits of the active-monitor path. The
/// existing graph quote already covers final rows/registries/event containers.
/// Each monitor can release once and each live claim can terminalize once; a
/// final snapshot proves quiescence. No temporary allowance is silently reused.
pub(super) fn quote(
    view: &View<'_>,
    claims: &[&ClaimState],
    snapshot_bytes: usize,
    failure_bytes: usize,
    limits: NativeLimits,
) -> Result<MonitorBudget, NativeError> {
    if !claims
        .iter()
        .any(|claim| claim.scopes().iter().any(|scope| scope.active()))
    {
        return Ok(MonitorBudget::default());
    }
    let mut result = MonitorBudget::default();
    let mut live = 0;
    let mut edges = 0;
    let mut scopes = 0;
    let mut children = 0;
    let mut model_visits = 0;
    let mut index_visits = 0;
    let mut all_roots = 0;
    let n = claims.len();
    for claim in claims {
        live = add(live, usize::from(!claim.is_terminal()))?;
        scopes = add(scopes, claim.scopes().iter().count())?;
        children = add(children, claim.scopes().children().len())?;
        edges = add(edges, claim.graph().obligations().len())?;
        let mut roots = 0;
        for monitor in claim.scopes().iter() {
            roots = add(roots, monitor.roots().len())?;
            if monitor.active() {
                result.events = add(result.events, 1)?;
                edges = add(edges, monitor.roots().len())?;
                let index =
                    crate::native::monitor_index::release_bound(view, claim, monitor, limits)?;
                result.index_rows = add(result.index_rows, index.extra_rows)?;
                result.construction_bytes = add(result.construction_bytes, index.temporary_bytes)?;
                index_visits = add(index_visits, index.visits)?;
                model_visits = add(model_visits, index.visits)?;
            }
        }
        all_roots = add(all_roots, roots)?;
        let m = active(claim);
        // Model release copies the complete scope registry and graph reads.
        let registry = claim.scopes();
        let transition = add(
            size_of::<scope::Transition>(),
            add(
                array::<Binding>(n)?,
                add(
                    registry.copy_heap_bytes()?,
                    multiply(
                        registry.copy_heap_allocations()?,
                        crate::native::prepare::ALLOCATION,
                    )?,
                )?,
            )?,
        )?;
        result.construction_bytes = add(result.construction_bytes, multiply(m, transition)?)?;
        result.construction_bytes = add(
            result.construction_bytes,
            future_copy_growth(claim, heap(claim)?)?,
        )?;
        let release_visits = add(
            add(multiply(8, n)?, 16)?,
            add(
                multiply(6, registry.iter().count())?,
                add(registry.children().len(), multiply(roots, add(n, 2)?)?)?,
            )?,
        )?;
        model_visits = add(model_visits, multiply(m, release_visits)?)?;
        // One terminal transition plus every monitor release can coexist on a
        // terminal owner; each retains its own intermediate revision and event.
        let margin = add(m, usize::from(!claim.is_terminal()))?;
        let mut binding = claim.binding();
        for _ in 0..margin {
            binding = binding.next()?;
        }
    }
    if result.events == 0 {
        return Ok(MonitorBudget::default());
    }
    let rounds = add(add(result.events, live)?, 1)?;
    let peers = n.checked_sub(1).ok_or(ContractError::InvalidManifest)?;
    let round_bytes = add(
        snapshot_bytes,
        add(
            failure_bytes,
            add(array::<&ClaimState>(n)?, array::<&ClaimState>(peers)?)?,
        )?,
    )?;
    result.construction_bytes = add(result.construction_bytes, multiply(rounds, round_bytes)?)?;
    // Central journal validation replays this bounded inline index suffix once.
    result.construction_bytes = add(
        result.construction_bytes,
        crate::native::monitor_index::replay_bytes(result.index_rows)?,
    )?;
    result.construction_bytes = add(
        result.construction_bytes,
        add(array::<&ClaimState>(n)?, array::<ClaimState>(n)?)?,
    )?;
    // Capture includes a least fixpoint of at most n+1 full sweeps; native
    // checks additionally charge source/index lookups and exact witness reads.
    let capture = add(
        add(multiply(2, add(add(n, scopes)?, children)?)?, edges)?,
        multiply(add(n, 1)?, add(n, edges)?)?,
    )?;
    let scan = add(add(n, scopes)?, multiply(edges, add(n, 1)?)?)?;
    let queries = multiply(n, add(multiply(6, n)?, add(multiply(3, edges)?, 16)?)?)?;
    result.visits = add(
        model_visits,
        multiply(rounds, add(capture, add(scan, queries)?)?)?,
    )?;
    // Work's original transaction has at most six non-graph facts/extras.
    // Replay's pairwise passes inspect actual entries, not reserved capacity.
    // Keep max-batch overlay-read costs inside index_visits; using the unused
    // batch ceiling again for these quadratic passes would quote phantom rows.
    // Ordinary control/timer prefixes have independent checked visit limits;
    // this future report allowance does not promise their arbitrary prefixes.
    let x = add(result.index_rows, 6)?
        .max(add(add(result.events, live)?, 6)?)
        .max(n);
    let h = add(
        usize::try_from(usize::BITS).map_err(|_| ContractError::Capacity)?,
        1,
    )?;
    let replay = add(
        multiply(8, index_visits)?,
        add(
            multiply(scopes, add(x, add(multiply(2, h)?, 8)?)?)?,
            add(
                multiply(2, all_roots)?,
                add(
                    multiply(16, multiply(add(x, 1)?, add(x, 1)?)?)?,
                    multiply(4, multiply(n, add(x, 1)?)?)?,
                )?,
            )?,
        )?,
    )?;
    result.writer_visits = result.visits;
    result.visits = add(result.visits, replay)?;
    Ok(result)
}

pub(in crate::native) struct MonitorPlan<'a, 'b> {
    view: &'a View<'b>,
    source: &'a ClaimState,
    plan: ConsequencePlan<'a>,
    peers: Vec<&'a ClaimState>,
    cut: ClaimCut,
    limits: NativeLimits,
}
pub(in crate::native) struct ChangedMonitorRoot<'a, 'b> {
    view: &'a View<'b>,
    source: &'a ClaimState,
    root: ClaimState,
    original: Vec<&'a ClaimState>,
    cut: ClaimCut,
    limits: NativeLimits,
}

pub(in crate::native) fn monitor<'a, 'b>(
    view: &'a View<'b>,
    source: &'a ClaimState,
    additional: &[ClaimId],
    cut: ClaimCut,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<MonitorPlan<'a, 'b>, NativeError> {
    check_cut(view, cut)?;
    let actual = view
        .claim(ClaimId(source.binding().object.0))
        .ok_or(ContractError::InvalidTarget)?;
    if actual != source {
        return Err(ContractError::ContentConflict.into());
    }
    let plan = preflight_with(view, source, additional, limits, None, scratch)?;
    let mut peer_rows = scratch.reserve(
        plan.claims
            .len()
            .checked_sub(1)
            .ok_or(ContractError::InvalidManifest)?,
    )?;
    peers(
        &mut peer_rows,
        &plan.claims,
        ClaimId(source.binding().object.0),
    )?;
    Ok(MonitorPlan {
        view,
        source,
        plan,
        peers: peer_rows,
        cut,
        limits,
    })
}
impl<'a, 'b> MonitorPlan<'a, 'b> {
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
    ) -> Result<ChangedMonitorRoot<'a, 'b>, NativeError> {
        if root != *self.source {
            return Err(ContractError::ContentConflict.into());
        }
        let cut = match transition.event() {
            scope::Event::Registered { cut, .. } | scope::Event::MonitorReleased { cut, .. } => cut,
            scope::Event::Rebound { change, .. } => change.cut,
            scope::Event::MonitorCancelled { cancellation, .. } => cancellation.cut,
            _ => return Err(ContractError::InvalidTransition.into()),
        };
        if cut != self.cut {
            return Err(ContractError::InvalidCut.into());
        }
        root.apply_scope(&self.source.binding(), transition, &self.peers)?;
        self.source.binding().next()?.check(&root.binding())?;
        if root.status() != self.source.status()
            || root.terminal_cut() != self.source.terminal_cut()
            || root.local_sealed_at() != self.source.local_sealed_at()
            || root.local_complete() != self.source.local_complete()
            || root.receipt() != self.source.receipt()
        {
            return Err(ContractError::InvalidTransition.into());
        }
        Ok(ChangedMonitorRoot {
            view: self.view,
            source: self.source,
            root,
            original: self.plan.claims,
            cut: self.cut,
            limits: self.limits,
        })
    }
}
impl ChangedMonitorRoot<'_, '_> {
    pub(in crate::native) fn claim(&self) -> &ClaimState {
        &self.root
    }
}
pub(in crate::native) fn prepare_monitor(
    changed: ChangedMonitorRoot<'_, '_>,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    if limits.plan_nodes != changed.limits.plan_nodes
        || limits.plan_edges != changed.limits.plan_edges
        || limits.preparation_bytes != changed.limits.preparation_bytes
    {
        return Err(ContractError::InvalidPolicy.into());
    }
    if changed
        .view
        .claim(ClaimId(changed.source.binding().object.0))
        != Some(changed.source)
    {
        return Err(ContractError::ContentConflict.into());
    }
    let mut ids = scratch.reserve(changed.original.len())?;
    for source in &changed.original {
        if ids.len() == ids.capacity() {
            return Err(ContractError::Capacity.into());
        }
        ids.push(ClaimId(source.binding().object.0));
    }
    prepare_with(
        changed.view,
        changed.root,
        &ids,
        changed.cut,
        limits,
        extras,
        scratch,
    )
}

pub(super) fn original_sources<'a>(
    view: &'a View<'_>,
    claims: &[&ClaimState],
    scratch: &mut Scratch,
) -> Result<Vec<&'a ClaimState>, NativeError> {
    let mut original = scratch.reserve(claims.len())?;
    for source in claims {
        let actual = view
            .claim(ClaimId(source.binding().object.0))
            .ok_or(ContractError::InvalidTarget)?;
        if original.len() == original.capacity() {
            return Err(ContractError::Capacity.into());
        }
        original.push(actual);
    }
    Ok(original)
}

fn install(
    changed: &mut Vec<ClaimState>,
    next: ClaimState,
    visits: &mut graph::VisitBudget,
) -> Result<(), NativeError> {
    visits.charge(add(changed.len(), 1)?)?;
    match changed.binary_search_by_key(&next.binding().object, |row| row.binding().object) {
        Ok(index) => {
            let previous = changed.get_mut(index).ok_or(ContractError::InvalidTarget)?;
            previous.binding().next()?.check(&next.binding())?;
            *previous = next;
        }
        Err(index) => {
            if changed.len() == changed.capacity() {
                return Err(ContractError::Capacity.into());
            }
            changed.insert(index, next);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // One current graph transaction and shared held budgets.
pub(in crate::native) fn settle_scopes(
    view: &View<'_>,
    original: &[&ClaimState],
    changed: &mut Vec<ClaimState>,
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
    visits: &mut graph::VisitBudget,
) -> Result<bool, NativeError> {
    check_cut(view, cut)?;
    let mut remaining = 1usize;
    for source in original {
        let current = changed
            .binary_search_by_key(&source.binding().object, |row| row.binding().object)
            .ok()
            .and_then(|index| changed.get(index))
            .unwrap_or(source);
        remaining = add(
            remaining,
            add(active(current), usize::from(!current.is_terminal()))?,
        )?;
    }
    let mut progressed = false;
    loop {
        remaining = remaining.checked_sub(1).ok_or(ContractError::Capacity)?;
        let next = {
            let (current, snapshot) =
                crate::native::claim_deadlines::freeze(original, changed, limits, scratch, visits)?;
            snapshot.check_cut(cut.position)?;
            let mut peers = scratch.reserve(
                current
                    .len()
                    .checked_sub(1)
                    .ok_or(ContractError::InvalidManifest)?,
            )?;
            let failure_bytes = snapshot.dependency_failure_charge()?;
            scratch.charge(failure_bytes)?;
            let mut selected = None;
            for source in &current {
                visits.charge(1)?;
                for monitor in source.scopes().iter() {
                    visits.charge(1)?;
                    if !monitor.active() {
                        continue;
                    }
                    let mut settled = true;
                    for root in monitor.roots() {
                        visits.charge(add(current.len(), 1)?)?;
                        if !snapshot.wait_settled(*root)? {
                            settled = false;
                            break;
                        }
                    }
                    if !settled {
                        continue;
                    }
                    crate::native::claim_deadlines::peers(
                        &current,
                        source.binding(),
                        &mut peers,
                        visits,
                    )?;
                    let plan = scope::Registry::prepare_release_monitor_bounded(
                        source,
                        monitor.id(),
                        &snapshot,
                        &peers,
                        cut,
                        scope::BuildLimits {
                            bytes: scratch.remaining()?,
                            visits: visits.remaining(),
                        },
                    )?;
                    visits.charge(plan.visits())?;
                    let charge = plan.construction_charge();
                    scratch.charge(charge)?;
                    let transition = plan.build()?;
                    within(transition.construction_charge()?, charge)?;
                    let mut next = copy(source, scratch)?;
                    next.apply_scope(&source.binding(), transition, &peers)?;
                    let event = scope::Event::MonitorReleased {
                        id: monitor.id(),
                        cut,
                    };
                    let bound =
                        crate::native::monitor_index::release_bound(view, source, monitor, limits)?;
                    visits.charge(bound.visits)?;
                    let growth = crate::native::monitor_index::stage(
                        view, source, &next, event, limits, extras, scratch,
                    )?;
                    if growth.monitors != 0 || growth.links != 0 {
                        return Err(ContractError::InvalidTransition.into());
                    }
                    journal(
                        extras,
                        source.binding(),
                        &next,
                        NativeEventKind::Monitor(NativeMonitorEvent::Released {
                            id: monitor.id(),
                            cut,
                        }),
                    )?;
                    within(heap(&next)?, heap(source)?)?;
                    selected = Some(next);
                    break;
                }
                if selected.is_some() {
                    break;
                }
                if source.is_terminal() {
                    continue;
                }
                crate::native::claim_deadlines::peers(
                    &current,
                    source.binding(),
                    &mut peers,
                    visits,
                )?;
                match snapshot.dependency_failure_with_visits(
                    ClaimId(source.binding().object.0),
                    failure_bytes,
                    visits,
                ) {
                    Ok(failure) => {
                        let mut next = copy(source, scratch)?;
                        next.dependency_failed(&source.binding(), &failure, &peers, cut.position)?;
                        journal(
                            extras,
                            source.binding(),
                            &next,
                            NativeEventKind::DependencyFailed,
                        )?;
                        selected = Some(next);
                    }
                    Err(ContractError::InvalidTransition) => {
                        if source.status() == ClaimStatus::Validating
                            && source.local_complete()
                            && snapshot.satisfied(ClaimId(source.binding().object.0))?
                        {
                            let witness = snapshot.release(ClaimId(source.binding().object.0))?;
                            let mut next = copy(source, scratch)?;
                            next.graph_release(&source.binding(), &witness, &peers, cut.position)?;
                            journal(extras, source.binding(), &next, NativeEventKind::Satisfied)?;
                            selected = Some(next);
                        }
                    }
                    Err(error) => return Err(error.into()),
                }
                if selected.is_some() {
                    break;
                }
            }
            selected
        };
        let Some(next) = next else {
            return Ok(progressed);
        };
        install(changed, next, visits)?;
        progressed = true;
    }
}

#[allow(clippy::too_many_arguments)] // Shared checked graph preparation; source buffers remain held.
pub(super) fn prepare(
    view: &View<'_>,
    root: ClaimState,
    original: &[&ClaimState],
    cut: ClaimCut,
    limits: NativeLimits,
    budget: ConsequenceBudget,
    events_before: usize,
    started: usize,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    let mut changed = scratch.reserve(original.len())?;
    if changed.len() == changed.capacity() {
        return Err(ContractError::Capacity.into());
    }
    changed.push(root);
    let mut visits = graph::VisitBudget::new(limits.plan_edges);
    visits.charge(budget.discovery_visits)?;
    settle_scopes(
        view,
        original,
        &mut changed,
        cut,
        limits,
        extras,
        scratch,
        &mut visits,
    )?;
    budget.check_output(
        view,
        &changed,
        extras
            .events()
            .checked_sub(events_before)
            .ok_or(ContractError::Capacity)?,
    )?;
    within(
        scratch
            .used
            .checked_sub(started)
            .ok_or(ContractError::Capacity)?,
        budget.graph_bytes,
    )?;
    Ok(changed)
}

#[cfg(test)]
#[path = "graph_monitor_effects_tests.rs"]
mod tests;
