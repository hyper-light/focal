//! Atomic dependency consequences over the complete indexed affected closure.
//! Each witness reads one complete frozen snapshot. Active-monitor progression
//! recaptures after a checked transition before deriving its next consequence.
use super::prepare::{Extras, Scratch, add, array, containers, event_containers, heap, within};
use super::*;
use focal_model::{
    Cause,
    lifecycle::{claim::ClaimCut, graph},
};

#[path = "graph_owner_release.rs"]
mod owner_release;
pub(super) use owner_release::{owner_release, prepare_released};
#[path = "graph_monitor_effects.rs"]
mod monitors;
pub(super) use monitors::{monitor, prepare_monitor, settle_scopes};

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
    fn insert(&mut self, sources: &GraphSources<'a, '_>, id: ClaimId) -> Result<(), NativeError> {
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
        let claim = sources.source(id)?;
        if claim.binding().ledger != sources.view.ledger() {
            return Err(ContractError::WrongLedger.into());
        }
        if id.is_zero() || claim.binding().object.0 != id.0 || claim.binding().revision.0 == 0 {
            return Err(ContractError::InvalidTarget.into());
        }
        if claim.created().0 == 0
            || (claim.created() > sources.view.prefix()
                && !(sources.view.claim(id).is_none()
                    && sources
                        .rows
                        .iter()
                        .any(|row| row.binding().object.0 == id.0)
                    && sources.view.prefix().0.checked_add(1) == Some(claim.created().0)))
        {
            return Err(ContractError::InvalidCut.into());
        }
        self.seen.insert(position, id);
        self.claims.push(claim);
        Ok(())
    }
}

struct GraphSources<'a, 'b> {
    view: &'a View<'b>,
    root: Option<&'a ClaimState>,
    rows: &'a [ClaimState],
}
impl<'a> GraphSources<'a, '_> {
    fn source(&self, id: ClaimId) -> Result<&'a ClaimState, NativeError> {
        if let Some(root) = self.root.filter(|root| root.binding().object.0 == id.0) {
            return Ok(root);
        }
        if let Ok(index) = self
            .rows
            .binary_search_by_key(&id.0, |row| row.binding().object.0)
        {
            return self
                .rows
                .get(index)
                .ok_or(ContractError::InvalidTarget.into());
        }
        self.view
            .claim(id)
            .ok_or(ContractError::InvalidTarget.into())
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
) -> Result<(Vec<&'a ClaimState>, usize), NativeError> {
    closure_with(view, root, &[], limits, scratch)
}

fn closure_with<'a>(
    view: &'a View<'_>,
    root: &'a ClaimState,
    additional: &[ClaimId],
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<(Vec<&'a ClaimState>, usize), NativeError> {
    discover(
        &GraphSources {
            view,
            root: Some(root),
            rows: &[],
        },
        additional,
        limits,
        scratch,
    )
}

pub(super) fn control_closure<'a>(
    view: &'a View<'_>,
    rows: &'a [ClaimState],
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<(Vec<&'a ClaimState>, usize), NativeError> {
    discover(
        &GraphSources {
            view,
            root: None,
            rows,
        },
        &[],
        limits,
        scratch,
    )
}

fn discover<'a>(
    sources: &GraphSources<'a, '_>,
    additional: &[ClaimId],
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<(Vec<&'a ClaimState>, usize), NativeError> {
    let view = sources.view;
    let mut closure = Closure {
        claims: scratch.reserve(limits.plan_nodes)?,
        seen: scratch.reserve(limits.plan_nodes)?,
        visits: limits.plan_edges,
        maximum: limits.plan_nodes,
    };
    if let Some(root) = sources.root {
        closure.insert(sources, ClaimId(root.binding().object.0))?;
    }
    let mut previous = None;
    for root in sources.rows {
        if previous.is_some_and(|id| id >= root.binding().object) {
            return Err(ContractError::InvalidManifest.into());
        }
        previous = Some(root.binding().object);
        closure.insert(sources, ClaimId(root.binding().object.0))?;
    }
    for id in additional {
        closure.insert(sources, *id)?;
    }
    let mut cursor = 0usize;
    while let Some(source) = closure.claims.get(cursor).copied() {
        let id = ClaimId(source.binding().object.0);
        for obligation in source.graph().obligations() {
            closure.insert(sources, obligation.target)?;
        }
        for scope in source.scopes().iter() {
            closure.charge(1)?;
            if scope.active() {
                // Every original live subscription must occur in its complete
                // indexed target chain. A missing head must not silently turn
                // a retained monitor into an unobserved outgoing-only edge.
                // A new/rebound root is separately checked by its private edit
                // capability and the final index journal against Extras.
                if let Some(original_owner) = view.claim(id)
                    && let Some(original_scope) = original_owner.scopes().monitor(scope.id())
                    && original_scope.active()
                {
                    for predicate in original_scope.roots() {
                        let indexed_limits = NativeLimits {
                            plan_edges: closure.visits,
                            ..limits
                        };
                        let debit = super::monitor_index::check_member(
                            view,
                            original_owner,
                            original_scope,
                            monitors::target(*predicate),
                            indexed_limits,
                        )?;
                        closure.charge(debit)?;
                    }
                }
                for predicate in scope.roots() {
                    closure.insert(sources, monitors::target(*predicate))?;
                }
            }
        }
        for registered in source.scopes().children() {
            closure.charge(1)?;
            let actual = sources.source(registered.id())?;
            child(source, actual)?;
            closure.insert(sources, registered.id())?;
        }
        if let Cause::Claim(owner) = *source.lineage().cause() {
            closure.charge(1)?;
            let parent = sources.source(owner)?;
            child(parent, source)?;
            closure.insert(sources, owner)?;
        }
        if view.claim(id).is_none() {
            cursor = cursor.checked_add(1).ok_or(ContractError::Capacity)?;
            continue;
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
            closure.insert(sources, ClaimId(dependent.binding().object.0))?;
        }
        if view.meta().monitors != 0 {
            closure.charge(2)?;
            let subscriber_limits = NativeLimits {
                plan_edges: closure.visits,
                ..limits
            };
            for subscriber in super::monitor_index::subscribers(view, id, subscriber_limits) {
                let subscriber = subscriber?;
                // The iterator validates every retained chain link before yielding;
                // charge its documented checked traversal bound below.
                closure.charge(super::monitor_index::subscriber_visits(
                    subscriber.owner,
                    subscriber.monitor,
                )?)?;
                closure.insert(sources, ClaimId(subscriber.owner.binding().object.0))?;
            }
        }
        cursor = cursor
            .checked_add(1)
            .ok_or(NativeError::Capacity("graph consequence cursor"))?;
    }
    closure
        .claims
        .sort_unstable_by_key(|claim| claim.binding().object);
    Ok((
        closure.claims,
        limits
            .plan_edges
            .checked_sub(closure.visits)
            .ok_or(ContractError::Capacity)?,
    ))
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

/// Read at the actual snapshot boundary, before any consequence is appended.
/// Only explicit journals have ordinal coordinates during graph preparation.
pub(super) fn capture(extras: &Extras) -> Result<NativeGraphCapture, NativeError> {
    let journal = extras
        .journal
        .as_ref()
        .ok_or(ContractError::InvalidTransition)?;
    Ok(NativeGraphCapture {
        before_ordinal: u32::try_from(journal.len()).map_err(|_| ContractError::Capacity)?,
    })
}

fn journal(
    extras: &mut Extras,
    before: Binding,
    after: &ClaimState,
    kind: NativeEventKind,
    graph: Option<NativeGraphCapture>,
) -> Result<(), NativeError> {
    if before.next()? != after.binding() {
        return Err(ContractError::StaleRevision.into());
    }
    extras.record(NativeFact::Claim(NativeClaimEvent {
        graph,
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

/// Complete graph-stage quote over an actual owner prefix. These bytes are
/// additional to all already retained/pinned roots. `preparation_bytes` includes
/// one initial root copy even though `prepare` receives that copy by value.
/// It does not include range input/directory/neighbor preparation, the common
/// object-journal sorted index, or non-graph Work/Response/result rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConsequenceCharges {
    pub nodes: usize,
    pub changed_rows: usize,
    pub graph_events: usize,
    pub monitor_events: usize,
    pub monitor_index_rows: usize,
    /// Reducer plus the bounded original Work report/index replay allowance.
    /// Actor controls and timer prefixes keep their separate checked budgets.
    pub monitor_visits: usize,
    pub preparation_bytes: usize,
    pub claim_heap_bytes: usize,
    pub registry_heap_bytes: usize,
    pub incoming_heap_bytes: usize,
    pub maximum_entry_heap_bytes: usize,
}

/// A checked graph ceiling. WholeWork grants expand its row heap bounds and
/// fund it separately; the owner checks every proposed effective prefix,
/// including incoming-link writes, against the retained component contract.
#[derive(Debug, Clone, Copy)]
pub(super) struct ConsequenceBudget {
    root: Binding,
    range: RangeId,
    prefix: SessionSeq,
    nodes_limit: usize,
    visits_limit: usize,
    discovery_visits: usize,
    snapshot_bytes: usize,
    failure_bytes: usize,
    graph_bytes: usize,
    membership: ContentHash,
    monitor: monitors::MonitorBudget,
    charges: ConsequenceCharges,
}

/// Borrowed source identities and owned snapshot come from the same indexed
/// closure. No claim, registry, graph declaration or separator key is cloned.
pub(super) struct ConsequencePlan<'a> {
    claims: Vec<&'a ClaimState>,
    snapshot: graph::Snapshot,
    budget: ConsequenceBudget,
}
impl ConsequencePlan<'_> {
    pub(super) fn budget(&self) -> ConsequenceBudget {
        self.budget
    }
    pub(super) fn members(&self) -> &[&ClaimState] {
        &self.claims
    }

    /// Keep topology fixed while allowing every currently mutable row to grow
    /// to its authored history/registration capacity. The caller supplies
    /// independently checked per-row bounds; no rows or policies are copied.
    pub(super) fn with_future_heaps(
        &self,
        mut future: impl FnMut(&ClaimState) -> Result<(usize, usize), NativeError>,
    ) -> Result<ConsequenceBudget, NativeError> {
        let mut result = self.budget;
        let mut claims = 0;
        let mut registries = 0;
        let mut root_growth = 0;
        let mut maximum = result.charges.maximum_entry_heap_bytes;
        let mut monitor_growth = 0;
        for claim in &self.claims {
            let is_root = claim.binding().object == result.root.object;
            if claim.is_terminal() && !is_root && !claim.scopes().iter().any(|scope| scope.active())
            {
                continue;
            }
            let (claim_heap, registry_heap) = future(claim)?;
            let current = heap(claim)?;
            within(current, claim_heap)?;
            if result.charges.monitor_events != 0 {
                monitor_growth = add(
                    monitor_growth,
                    monitors::future_copy_growth(
                        claim,
                        claim_heap
                            .checked_sub(current)
                            .ok_or(ContractError::Capacity)?,
                    )?,
                )?;
            }
            if is_root {
                root_growth = claim_heap
                    .checked_sub(current)
                    .ok_or(ContractError::Capacity)?;
            }
            claims = add(claims, claim_heap)?;
            registries = add(registries, registry_heap)?;
            maximum = maximum.max(add(
                OwnedClaim::container_charge(),
                add(claim_heap, registry_heap)?,
            )?);
        }
        let claim_growth = claims
            .checked_sub(result.charges.claim_heap_bytes)
            .ok_or(ContractError::Capacity)?;
        let registry_growth = registries
            .checked_sub(result.charges.registry_heap_bytes)
            .ok_or(ContractError::Capacity)?;
        let growth = add(claim_growth, registry_growth)?;
        result.graph_bytes = add(
            result.graph_bytes,
            claim_growth
                .checked_sub(root_growth)
                .ok_or(ContractError::Capacity)?,
        )?;
        result.graph_bytes = add(result.graph_bytes, monitor_growth)?;
        result.monitor.construction_bytes = add(result.monitor.construction_bytes, monitor_growth)?;
        result.charges.preparation_bytes = add(
            result.charges.preparation_bytes,
            add(growth, monitor_growth)?,
        )?;
        result.charges.incoming_heap_bytes = add(result.charges.incoming_heap_bytes, growth)?;
        result.charges.claim_heap_bytes = claims;
        result.charges.registry_heap_bytes = registries;
        result.charges.maximum_entry_heap_bytes = maximum;
        // Only the future-responsibility constructor promises the entire Work
        // report/index replay. Ordinary control prefixes keep actual stage caps.
        if result.monitor.events != 0
            && add(result.monitor.visits, result.discovery_visits)? > result.visits_limit
        {
            return Err(NativeError::Capacity("monitor report visits"));
        }
        Ok(result)
    }
}

fn membership(claims: &[&ClaimState]) -> ContentHash {
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal-native-held-graph-members-v1");
    // Native immutable content fixes each member's dependency declaration.
    // Added ownership/dependency endpoints necessarily change this complete
    // sorted set; mutable revisions, outcomes and history deliberately do not.
    for claim in claims {
        hash.update(&claim.binding().object.0);
        hash.update(&claim.binding().content.0);
        hash.update(&claim.created().0.to_be_bytes());
        monitors::hash_topology(&mut hash, claim);
    }
    ContentHash(*hash.finalize().as_bytes())
}

fn registry<'a>(
    view: &'a View<'_>,
    claim: &ClaimState,
) -> Result<&'a RegistrationSet, NativeError> {
    let owned = view.owned_claim(ClaimId(claim.binding().object.0))?;
    let registry = owned.registrations().ok_or(ContractError::InvalidTarget)?;
    registry.check(claim)?;
    Ok(registry)
}

fn check_root(view: &View<'_>, root: &ClaimState) -> Result<(), NativeError> {
    let original = view
        .claim(ClaimId(root.binding().object.0))
        .ok_or(ContractError::InvalidTarget)?;
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
    Ok(())
}

/// Claim timer progression holds this complete original topology once, then
/// overlays only checked terminal replacements. Return discovery's actual
/// debit so every subsequent capture and query shares one transaction budget.
pub(super) fn deadline_closure<'a>(
    view: &'a View<'_>,
    root: &'a ClaimState,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<(Vec<&'a ClaimState>, usize), NativeError> {
    check_root(view, root)?;
    closure(view, root, limits, scratch)
}

/// Uses the identical complete incoming/outgoing/ownership walk and Snapshot
/// construction consumed by execution below. Each allocation is charged to
/// Scratch before construction; the complete quote is checked, never debited
/// again as a second reservation. Registry copies are performed by the common
/// claim-row builder, whose actual capacities are separately reconciled there.
/// An optional prior ceiling checks this complete candidate without another
/// discovery/capture pass. Supplying None derives a fresh exact-current quote;
/// it does not install a grant or permit later growth without another check.
pub(super) fn preflight<'a>(
    view: &'a View<'_>,
    root: &'a ClaimState,
    limits: NativeLimits,
    ceiling: Option<ConsequenceBudget>,
    scratch: &mut Scratch,
) -> Result<ConsequencePlan<'a>, NativeError> {
    check_root(view, root)?;
    preflight_checked(view, root, limits, ceiling, scratch)
}

/// Recheck a held component using every originally protected member as a seed.
/// Disposed monitor edges may disconnect it, but cannot hide an original row.
pub(super) fn preflight_with_members<'a>(
    view: &'a View<'_>,
    root: &'a ClaimState,
    limits: NativeLimits,
    ceiling: Option<ConsequenceBudget>,
    seeds: &[ClaimId],
    scratch: &mut Scratch,
) -> Result<ConsequencePlan<'a>, NativeError> {
    check_root(view, root)?;
    preflight_with(view, root, seeds, limits, ceiling, scratch)
}

// Only unchanged-scope preflight or a consumed checked scope capability reaches
// this constructor. Explicit edits retain the original union as additional seeds.
fn preflight_checked<'a>(
    view: &'a View<'_>,
    root: &'a ClaimState,
    limits: NativeLimits,
    ceiling: Option<ConsequenceBudget>,
    scratch: &mut Scratch,
) -> Result<ConsequencePlan<'a>, NativeError> {
    preflight_with(view, root, &[], limits, ceiling, scratch)
}

fn preflight_with<'a>(
    view: &'a View<'_>,
    root: &'a ClaimState,
    additional: &[ClaimId],
    limits: NativeLimits,
    ceiling: Option<ConsequenceBudget>,
    scratch: &mut Scratch,
) -> Result<ConsequencePlan<'a>, NativeError> {
    let started = scratch.used;
    let (claims, actual_discovery_visits) = closure_with(view, root, additional, limits, scratch)?;
    // An original quote reserves one insertion per complete member so later
    // disposition can recheck the same protected union even after disconnection.
    let discovery_visits = add(
        actual_discovery_visits,
        claims
            .len()
            .checked_sub(additional.len())
            .ok_or(ContractError::Capacity)?,
    )?;
    let plan = graph::Snapshot::prepare_capture(
        &claims,
        graph::Limits {
            nodes: limits.plan_nodes,
            edges: limits.plan_edges,
            visits: limits.plan_edges,
        },
        scratch.remaining()?,
    )?;
    let snapshot_bytes = plan.construction_charge();
    scratch.charge(snapshot_bytes)?;
    let snapshot = plan.build()?;
    within(snapshot.retained_charge()?, snapshot_bytes)?;
    snapshot.check_cut(SessionSeq(
        view.prefix()
            .0
            .checked_add(1)
            .ok_or(ContractError::Capacity)?,
    ))?;
    // Only one dependency witness survives at a time during execution.
    let failure_bytes = snapshot.dependency_failure_charge()?;
    scratch.charge(failure_bytes)?;
    let capture_bytes = scratch
        .used
        .checked_sub(started)
        .ok_or(ContractError::Capacity)?;
    let root_id = ClaimId(root.binding().object.0);
    let mut changed_rows = 0usize;
    let mut graph_events = 0usize;
    let mut claim_heap_bytes = 0usize;
    let mut registry_heap_bytes = 0usize;
    let mut incoming_heap_bytes = 0usize;
    let mut maximum_entry_heap_bytes = 0usize;
    for claim in &claims {
        let registrations = registry(view, claim)?;
        if !claim.is_terminal() {
            // A graph transition needs one revision. A target's earlier local
            // outcome may need another; that margin belongs to its caller.
            claim.binding().next()?;
            graph_events = add(graph_events, 1)?;
        }
        if claim.is_terminal()
            && claim.binding().object.0 != root_id.0
            && !claim.scopes().iter().any(|scope| scope.active())
        {
            continue;
        }
        changed_rows = add(changed_rows, 1)?;
        let claim_heap = heap(claim)?;
        let registry_heap = transactions::registry_heap(registrations)?;
        let entry_heap = add(
            OwnedClaim::container_charge(),
            add(claim_heap, registry_heap)?,
        )?;
        claim_heap_bytes = add(claim_heap_bytes, claim_heap)?;
        registry_heap_bytes = add(registry_heap_bytes, registry_heap)?;
        incoming_heap_bytes = add(incoming_heap_bytes, entry_heap)?;
        maximum_entry_heap_bytes = maximum_entry_heap_bytes.max(entry_heap);
    }
    let monitor = monitors::quote(view, &claims, snapshot_bytes, failure_bytes, limits)?;
    if monitor.events != 0 && add(monitor.writer_visits, discovery_visits)? > limits.plan_edges {
        return Err(NativeError::Capacity("monitor consequence visits"));
    }
    graph_events = add(graph_events, monitor.events)?;
    let event_heap = event_containers(graph_events)?;
    incoming_heap_bytes = add(incoming_heap_bytes, event_heap)?;
    if graph_events != 0 {
        maximum_entry_heap_bytes = maximum_entry_heap_bytes.max(OwnedEvent::container_charge());
    }
    let peers = claims
        .len()
        .checked_sub(1)
        .ok_or(ContractError::InvalidManifest)?;
    let buffers = add(
        array::<ClaimState>(claims.len())?,
        add(array::<&ClaimState>(peers)?, array::<&ClaimState>(peers)?)?,
    )?;
    let root_heap = heap(root)?;
    let peer_copy_heap = claim_heap_bytes
        .checked_sub(root_heap)
        .ok_or(ContractError::Capacity)?;
    let graph_bytes = add(
        add(capture_bytes, add(buffers, peer_copy_heap)?)?,
        monitor.construction_bytes,
    )?;
    let preparation_bytes = add(
        graph_bytes,
        add(
            root_heap,
            add(
                registry_heap_bytes,
                add(
                    containers(changed_rows)?,
                    add(array::<NativeFact>(graph_events)?, event_heap)?,
                )?,
            )?,
        )?,
    )?;
    // The root has already been copied by prepare's caller. Check only the
    // remaining stage here, while reporting its full one-copy cost above.
    within(
        add(
            started,
            preparation_bytes
                .checked_sub(root_heap)
                .ok_or(ContractError::Capacity)?,
        )?,
        scratch.max,
    )?;
    let budget = ConsequenceBudget {
        root: root.binding(),
        range: view.state.rows.id(),
        prefix: view.prefix(),
        nodes_limit: limits.plan_nodes,
        visits_limit: limits.plan_edges,
        discovery_visits,
        snapshot_bytes,
        failure_bytes,
        graph_bytes,
        membership: membership(&claims),
        monitor,
        charges: ConsequenceCharges {
            nodes: claims.len(),
            changed_rows,
            graph_events,
            monitor_events: monitor.events,
            monitor_index_rows: monitor.index_rows,
            monitor_visits: monitor.visits,
            preparation_bytes,
            claim_heap_bytes,
            registry_heap_bytes,
            incoming_heap_bytes,
            maximum_entry_heap_bytes,
        },
    };
    let plan = ConsequencePlan {
        claims,
        snapshot,
        budget,
    };
    if let Some(ceiling) = ceiling {
        ceiling.check_candidate(&plan)?;
    }
    Ok(plan)
}

impl ConsequenceBudget {
    pub(super) fn charges(self) -> ConsequenceCharges {
        self.charges
    }

    // Called only after the shared constructor has read the complete proposed
    // effective View, including incoming links. WholeWork grants retain this
    // ceiling and protect its complete component before later publication.
    fn check_candidate(self, candidate: &ConsequencePlan<'_>) -> Result<(), NativeError> {
        let next = candidate.budget;
        self.root.check(&Binding {
            revision: self.root.revision,
            ..next.root
        })?;
        if next.root.revision < self.root.revision || next.prefix < self.prefix {
            return Err(ContractError::StaleRevision.into());
        }
        if next.range != self.range
            || next.nodes_limit != self.nodes_limit
            || next.visits_limit != self.visits_limit
            || next.membership != self.membership
        {
            return Err(ContractError::InvalidPolicy.into());
        }
        for (actual, bound) in [
            (next.discovery_visits, self.discovery_visits),
            (next.snapshot_bytes, self.snapshot_bytes),
            (next.failure_bytes, self.failure_bytes),
            (next.graph_bytes, self.graph_bytes),
            (next.charges.nodes, self.charges.nodes),
            (next.charges.changed_rows, self.charges.changed_rows),
            (next.charges.graph_events, self.charges.graph_events),
            (next.charges.monitor_events, self.charges.monitor_events),
            (
                next.charges.monitor_index_rows,
                self.charges.monitor_index_rows,
            ),
            (next.charges.monitor_visits, self.charges.monitor_visits),
            (
                next.charges.preparation_bytes,
                self.charges.preparation_bytes,
            ),
            (next.charges.claim_heap_bytes, self.charges.claim_heap_bytes),
            (
                next.charges.registry_heap_bytes,
                self.charges.registry_heap_bytes,
            ),
            (
                next.charges.incoming_heap_bytes,
                self.charges.incoming_heap_bytes,
            ),
            (
                next.charges.maximum_entry_heap_bytes,
                self.charges.maximum_entry_heap_bytes,
            ),
        ] {
            within(actual, bound)?;
        }
        Ok(())
    }

    fn check_output(
        self,
        view: &View<'_>,
        rows: &[ClaimState],
        events: usize,
    ) -> Result<(), NativeError> {
        let charges = self.charges();
        within(rows.len(), charges.changed_rows)?;
        within(events, charges.graph_events)?;
        let mut claims = 0usize;
        let mut registries = 0usize;
        let mut incoming = event_containers(events)?;
        for row in rows {
            // Graph success/failure changes only inline fields (including its Copy
            // terminal cut). This still checks actual retained capacities so a later
            // model change cannot silently introduce unquoted output allocation.
            let claim_heap = heap(row)?;
            let registry_heap = transactions::registry_heap(registry(view, row)?)?;
            let entry_heap = add(
                OwnedClaim::container_charge(),
                add(claim_heap, registry_heap)?,
            )?;
            within(entry_heap, charges.maximum_entry_heap_bytes)?;
            claims = add(claims, claim_heap)?;
            registries = add(registries, registry_heap)?;
            incoming = add(incoming, entry_heap)?;
        }
        within(claims, charges.claim_heap_bytes)?;
        within(registries, charges.registry_heap_bytes)?;
        within(incoming, charges.incoming_heap_bytes)
    }
}

/// Root's earlier local-outcome transitions are already in Extras' journal.
/// This function returns the moved final root and all additionally changed
/// claims; the enclosing transaction publishes their rows and history together.
pub(super) fn prepare(
    view: &View<'_>,
    root: ClaimState,
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    check_cut(view, cut)?;
    check_root(view, &root)?;
    prepare_checked(view, root, cut, limits, extras, scratch)
}

fn check_cut(view: &View<'_>, cut: ClaimCut) -> Result<(), NativeError> {
    if cut.position.0 == 0
        || cut.cause == ContentHash([0; 32])
        || view.prefix().0.checked_add(1) != Some(cut.position.0)
    {
        return Err(ContractError::InvalidCut.into());
    }
    Ok(())
}

fn prepare_checked(
    view: &View<'_>,
    root: ClaimState,
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    prepare_with(view, root, &[], cut, limits, extras, scratch)
}

#[allow(clippy::too_many_arguments)] // One checked graph transaction and its original union.
fn prepare_with(
    view: &View<'_>,
    root: ClaimState,
    additional: &[ClaimId],
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    prepare_with_prefix(view, root, additional, cut, limits, None, extras, scratch)
}

/// The actual Admission failure owns the first four facts. Use the existing
/// graph preflight once to reserve its exact complete original journal.
#[allow(clippy::too_many_arguments)] // One private report prefix and checked graph source.
pub(super) fn prepare_admission(
    view: &View<'_>,
    root: ClaimState,
    cut: ClaimCut,
    limits: NativeLimits,
    prefix: [NativeFact; 4],
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    check_cut(view, cut)?;
    check_root(view, &root)?;
    if root.status() != ClaimStatus::PostFailed {
        return Err(ContractError::InvalidTransition.into());
    }
    prepare_with_prefix(view, root, &[], cut, limits, Some(&prefix), extras, scratch)
}

#[allow(clippy::too_many_arguments)] // One graph source with optional authoritative original report prefix.
fn prepare_with_prefix(
    view: &View<'_>,
    mut root: ClaimState,
    additional: &[ClaimId],
    cut: ClaimCut,
    limits: NativeLimits,
    prefix: Option<&[NativeFact; 4]>,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    let root_id = ClaimId(root.binding().object.0);
    let started = scratch.used;
    let mut plan = preflight_with(view, &root, additional, limits, None, scratch)?;
    if let Some(prefix) = prefix {
        let capacity = add(prefix.len(), plan.budget.charges.graph_events)?;
        within(capacity, limits.range.max_batch_entries)?;
        let charge = super::admission_graph::promote(prefix, capacity, extras, scratch)?;
        plan.budget.graph_bytes = add(plan.budget.graph_bytes, charge)?;
    }
    let events_before = extras.events();
    let captured = capture(extras)?;
    if plan.budget.charges.monitor_events != 0 {
        let original = monitors::original_sources(view, &plan.claims, scratch)?;
        let budget = plan.budget;
        drop(plan);
        return monitors::prepare(
            view,
            root,
            &original,
            cut,
            limits,
            budget,
            events_before,
            started,
            extras,
            scratch,
        );
    }
    let count = plan.members().len();
    let budget = plan.budget();
    let ConsequencePlan {
        claims, snapshot, ..
    } = plan;
    snapshot.check_cut(cut.position)?;
    let failure_charge = budget.failure_bytes;
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
                    Some(captured),
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
                        Some(captured),
                    )?;
                    Some(changed)
                } else {
                    None
                }
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(changed) = changed {
            // Both graph transitions retain only inline terminal facts. Keep
            // the per-source bound as well as the aggregate publication check.
            within(heap(&changed)?, heap(source)?)?;
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
        let root_heap = heap(&root)?;
        let before = root.binding();
        match snapshot.dependency_failure_with_budget(root_id, failure_charge) {
            Ok(failure) => {
                root.dependency_failed(&before, &failure, &root_peers, cut.position)?;
                journal(
                    extras,
                    before,
                    &root,
                    NativeEventKind::DependencyFailed,
                    Some(captured),
                )?;
            }
            Err(ContractError::InvalidTransition) => {
                if root.status() == ClaimStatus::Validating
                    && root.local_complete()
                    && snapshot.satisfied(root_id)?
                {
                    let release = snapshot.release(root_id)?;
                    root.graph_release(&before, &release, &root_peers, cut.position)?;
                    journal(
                        extras,
                        before,
                        &root,
                        NativeEventKind::Satisfied,
                        Some(captured),
                    )?;
                }
            }
            Err(error) => return Err(error.into()),
        }
        within(heap(&root)?, root_heap)?;
    }
    if output.len() == output.capacity() {
        return Err(NativeError::Capacity("graph root row"));
    }
    output.push(root);
    output.sort_unstable_by_key(|claim| claim.binding().object);
    budget.check_output(
        view,
        &output,
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
    Ok(output)
}

#[cfg(test)]
#[path = "graph_effects_tests.rs"]
mod tests;
