//! Canonical graph proof at the explicitly retained capture boundary. Immutable
//! graph/scopes remain borrowed; only bounded scalar metadata is reconstructed.
use super::*;
use focal_model::lifecycle::scope::RegistrySnapshotSource;
use focal_model::lifecycle::{claim::ClaimTerminalCut, graph, scope};
use focal_model::{Cause, WaitPredicate};
use graph::{RecordedEdge, RecordedGraph, RecordedNode};

fn capacity<T>(count: usize) -> Result<Vec<T>, NativeError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    require(values.capacity() <= count)?;
    Ok(values)
}
fn push<T>(values: &mut Vec<T>, value: T) -> Result<(), NativeError> {
    if values.len() == values.capacity() {
        return Err(ContractError::Capacity.into());
    }
    values.push(value);
    Ok(())
}
fn target(value: WaitPredicate) -> ClaimId {
    match value {
        WaitPredicate::Satisfied(id)
        | WaitPredicate::Terminal(id)
        | WaitPredicate::Released(id) => id,
    }
}
fn admit<O: Overlay>(
    read: &ReplayRead<'_, '_, O>,
    bytes: usize,
) -> Result<focal_memory::Allocation, NativeError> {
    Ok(read
        .budget
        .reserve(
            focal_memory::BudgetKind::Recovery,
            focal_memory::BudgetLane::Completion,
            bytes,
        )?
        .commit())
}

struct Prefix<'a, 'r, 'bytes, O> {
    read: &'a ReplayRead<'r, 'bytes, O>,
    before: u32,
}
struct Scope<'a> {
    roots: &'a [WaitPredicate],
    active: bool,
    deadline: focal_model::Deadline,
}
impl<O: Overlay> Prefix<'_, '_, '_, O> {
    fn claim(&self, id: ClaimId) -> Result<&ClaimState, NativeError> {
        self.read.claim(id)
    }
    fn original(&self, id: ClaimId) -> Result<Option<&ClaimState>, NativeError> {
        Ok(as_claim(self.read.before(Key::Claim(id))?))
    }
    fn exists(&self, id: ClaimId) -> Result<bool, NativeError> {
        if self.original(id)?.is_some() {
            return Ok(true);
        }
        let mut exists = false;
        self.read.events(Key::Claim(id), |event| {
            self.read.charge(128)?;
            if event.ordinal < self.before
                && matches!(
                    event.fact,
                    NativeFact::Claim(NativeClaimEvent {
                        kind: NativeEventKind::Created,
                        ..
                    })
                )
            {
                exists = true;
            }
            Ok(())
        })?;
        Ok(exists)
    }
    fn node(&self, id: ClaimId) -> Result<RecordedNode, NativeError> {
        let next = self.claim(id)?;
        let old = self.original(id)?;
        let mut binding = old.map(ClaimState::binding);
        let mut status = old.map(ClaimState::status);
        let mut local_complete = old.is_some_and(ClaimState::local_complete);
        let mut local_sealed = old.and_then(ClaimState::local_sealed_at);
        let mut released = old.is_some_and(|claim| claim.scopes().released());
        self.read.events(Key::Claim(id), |event| {
            self.read.charge(256)?;
            if event.ordinal >= self.before {
                return Ok(());
            }
            let NativeFact::Claim(value) = event.fact else {
                return Err(invalid());
            };
            require(value.before == binding)?;
            if value.kind == NativeEventKind::LocallyComplete {
                local_complete = true;
                local_sealed = Some(event.sequence);
            }
            if value.status.is_terminal() {
                local_sealed.get_or_insert(event.sequence);
            }
            if value.kind == NativeEventKind::OwnerReleased {
                released = true;
            }
            binding = Some(value.after);
            status = Some(value.status);
            Ok(())
        })?;
        let binding = binding.ok_or_else(invalid)?;
        let status = status.ok_or_else(invalid)?;
        let origin = if status.is_terminal() && status != ClaimStatus::Satisfied {
            match next.terminal_cut() {
                Some(ClaimTerminalCut::Graph(cut))
                    if cut.kind() == graph::FailureKind::DependencyFailed =>
                {
                    Some(cut.origin().snapshot_v1())
                }
                _ => Some(graph::OriginSnapshotV1 {
                    binding,
                    created: next.created(),
                    terminal: local_sealed.ok_or_else(invalid)?,
                }),
            }
        } else {
            None
        };
        let mut deadline = next.deadline();
        self.read
            .charge(add(next.scopes().snapshot_v1().fields().scopes, 1)?)?;
        for scope in next.scopes().iter() {
            if let Some(scope) = self.scope(id, scope)?
                && scope.active
                && deadline.is_none_or(|old| {
                    (
                        scope.deadline.at,
                        scope.deadline.timer,
                        scope.deadline.generation,
                    ) < (old.at, old.timer, old.generation)
                })
            {
                deadline = Some(scope.deadline);
            }
        }
        Ok(RecordedNode {
            binding,
            created: next.created(),
            status,
            local_complete,
            released,
            deadline,
            origin,
        })
    }
    fn scope<'a>(
        &'a self,
        id: ClaimId,
        next: &'a scope::Scope,
    ) -> Result<Option<Scope<'a>>, NativeError> {
        let old = self
            .original(id)?
            .and_then(|claim| claim.scopes().monitor(next.id()));
        let mut registered = old.is_some();
        let mut active = old.is_some_and(scope::Scope::active);
        let mut roots = old.map_or(next.roots(), scope::Scope::roots);
        self.read.index.monitors(
            self.read.encoded,
            next.id(),
            self.read.parsing,
            self.read.meter,
            |event| {
                self.read.charge(256)?;
                if event.ordinal >= self.before {
                    return Ok(());
                }
                let NativeFact::Claim(NativeClaimEvent {
                    kind: NativeEventKind::Monitor(value),
                    after,
                    ..
                }) = event.fact
                else {
                    return Err(invalid());
                };
                require(after.object.0 == id.0)?;
                match value {
                    NativeMonitorEvent::Registered { .. } => {
                        require(!registered)?;
                        registered = true;
                        active = true;
                        roots = next.roots();
                    }
                    NativeMonitorEvent::Rebound { .. } => {
                        require(registered && active)?;
                        roots = next.roots();
                    }
                    NativeMonitorEvent::Released { .. } | NativeMonitorEvent::Cancelled { .. } => {
                        require(registered && active)?;
                        active = false;
                    }
                }
                Ok(())
            },
        )?;
        Ok(registered.then_some(Scope {
            roots,
            active,
            deadline: next.deadline(),
        }))
    }
    fn insert(&self, ids: &mut Vec<ClaimId>, id: ClaimId) -> Result<(), NativeError> {
        self.read.charge(mul(add(ids.len(), 1)?, 16)?)?;
        if !ids.contains(&id) {
            push(ids, id)?;
        }
        Ok(())
    }
    fn reverse(
        &self,
        ids: &mut Vec<ClaimId>,
        id: ClaimId,
        original: bool,
    ) -> Result<(), NativeError> {
        let row = |key| {
            if original {
                self.read.before(key)
            } else {
                self.read.get(key)
            }
        };
        if let Some(Row::IncomingHead(head)) = row(Key::IncomingHead(id))? {
            if head.count > self.read.limits.plan_edges {
                return Err(ContractError::Capacity.into());
            }
            self.read.charge(add(head.count, 1)?)?;
            let mut at = head.head;
            for _ in 0..head.count {
                let dependent = at.ok_or_else(invalid)?;
                let Some(Row::IncomingLink(link)) = row(Key::IncomingLink(id, dependent))? else {
                    return Err(invalid());
                };
                if self.exists(dependent)? {
                    self.insert(ids, dependent)?;
                }
                at = link.next;
            }
            require(at.is_none())?;
        }
        if let Some(Row::MonitorHead(head)) = row(Key::MonitorHead(id))? {
            if head.count > self.read.limits.plan_edges {
                return Err(ContractError::Capacity.into());
            }
            self.read.charge(add(head.count, 1)?)?;
            let mut at = head.head;
            let mut previous = None;
            for _ in 0..head.count {
                let monitor = at.ok_or_else(invalid)?;
                let Some(Row::MonitorLink(Some(link))) = row(Key::MonitorLink(id, monitor))? else {
                    return Err(invalid());
                };
                require(link.previous == previous)?;
                if self.exists(link.owner)? {
                    self.insert(ids, link.owner)?;
                }
                previous = Some(monitor);
                at = link.next;
            }
            require(at.is_none())?;
        }
        Ok(())
    }
    fn discover(&self, ids: &mut Vec<ClaimId>, seed: ClaimId) -> Result<(), NativeError> {
        self.insert(ids, seed)?;
        if matches!(
            self.read.outcome.operation,
            NativeOperation::Create | NativeOperation::Cancel | NativeOperation::Post
        ) {
            self.read.charge(add(
                usize::try_from(self.before).map_err(|_| ContractError::Capacity)?,
                1,
            )?)?;
            for ordinal in 0..self.before {
                if let NativeFact::Claim(value) = self.read.event(ordinal)?.fact
                    && matches!(
                        value.kind,
                        NativeEventKind::Created
                            | NativeEventKind::ChildRegistered
                            | NativeEventKind::Superseded
                            | NativeEventKind::Cancelled
                            | NativeEventKind::Posted
                    )
                {
                    // The original checked control plan deliberately captures
                    // all roots, including disjoint correction successors.
                    self.insert(ids, ClaimId(value.after.object.0))?;
                }
            }
        }
        let mut cursor = 0usize;
        loop {
            self.read.charge(1)?;
            let Some(id) = ids.get(cursor).copied() else {
                break;
            };
            require(self.exists(id)?)?;
            let next = self.claim(id)?;
            self.read
                .charge(add(next.graph().obligations().len(), 1)?)?;
            for edge in next.graph().obligations() {
                self.insert(ids, edge.target)?;
            }
            // Original active monitor edges retain the protected component even
            // after release/rebinding disconnects it inside this transaction.
            if let Some(old) = self.original(id)? {
                self.read
                    .charge(add(old.scopes().snapshot_v1().fields().scopes, 1)?)?;
                for scope in old.scopes().iter() {
                    if scope.active() {
                        self.read.charge(add(scope.roots().len(), 1)?)?;
                        for root in scope.roots() {
                            self.insert(ids, target(*root))?;
                        }
                    }
                }
            }
            self.read
                .charge(add(next.scopes().snapshot_v1().fields().scopes, 1)?)?;
            for scope in next.scopes().iter() {
                if let Some(scope) = self.scope(id, scope)?
                    && scope.active
                {
                    self.read.charge(add(scope.roots.len(), 1)?)?;
                    for root in scope.roots {
                        self.insert(ids, target(*root))?;
                    }
                }
            }
            self.read.charge(add(next.scopes().children().len(), 1)?)?;
            for child in next.scopes().children() {
                if self.exists(child.id())? {
                    self.insert(ids, child.id())?;
                }
            }
            if let Cause::Claim(owner) = next.lineage().cause() {
                self.insert(ids, *owner)?;
            }
            self.reverse(ids, id, true)?;
            self.reverse(ids, id, false)?;
            cursor = add(cursor, 1)?;
        }
        self.read.charge(mul(
            add(ids.len(), 1)?,
            const { (usize::BITS as usize + 1) * 16 },
        )?)?;
        ids.sort_unstable();
        Ok(())
    }
    fn edges(&self, id: ClaimId, values: &mut Vec<RecordedEdge>) -> Result<(), NativeError> {
        let next = self.claim(id)?;
        self.read
            .charge(add(next.graph().obligations().len(), 1)?)?;
        for edge in next.graph().obligations() {
            let target = match edge.kind {
                graph::Kind::DependsOn => WaitPredicate::Satisfied(edge.target),
                graph::Kind::Awaits => WaitPredicate::Terminal(edge.target),
            };
            push(
                values,
                RecordedEdge {
                    source: id,
                    target,
                    propagates_failure: edge.kind == graph::Kind::DependsOn,
                    runtime: false,
                },
            )?;
        }
        self.read
            .charge(add(next.scopes().snapshot_v1().fields().scopes, 1)?)?;
        for scope in next.scopes().iter() {
            if let Some(scope) = self.scope(id, scope)?
                && scope.active
            {
                self.read.charge(add(scope.roots.len(), 1)?)?;
                for root in scope.roots {
                    push(
                        values,
                        RecordedEdge {
                            source: id,
                            target: *root,
                            propagates_failure: false,
                            runtime: true,
                        },
                    )?;
                }
            }
        }
        Ok(())
    }
    fn edge_count(&self, id: ClaimId) -> Result<usize, NativeError> {
        let next = self.claim(id)?;
        let mut count = next.graph().obligations().len();
        self.read
            .charge(add(next.scopes().snapshot_v1().fields().scopes, 1)?)?;
        for scope in next.scopes().iter() {
            if let Some(scope) = self.scope(id, scope)?
                && scope.active
            {
                count = add(count, scope.roots.len())?;
            }
        }
        Ok(count)
    }
}

fn snapshot<O: Overlay, T>(
    read: &ReplayRead<'_, '_, O>,
    seed: ClaimId,
    before: u32,
    check: impl FnOnce(&RecordedGraph, usize) -> Result<T, NativeError>,
) -> Result<T, NativeError> {
    require(before <= read.outcome.events)?;
    let limits = read.limits;
    let membership = prepare::array::<ClaimId>(limits.plan_nodes)?;
    prepare::within(membership, limits.preparation_bytes)?;
    let _membership_allocation = admit(read, membership)?;
    let mut ids = capacity(limits.plan_nodes)?;
    let prefix = Prefix { read, before };
    prefix.discover(&mut ids, seed)?;
    let mut edge_count = 0usize;
    read.charge(add(ids.len(), 1)?)?;
    for id in &ids {
        edge_count = add(edge_count, prefix.edge_count(*id)?)?;
    }
    prepare::within(edge_count, limits.plan_edges)?;
    let metadata = add(
        prepare::array::<RecordedNode>(ids.len())?,
        prepare::array::<RecordedEdge>(edge_count)?,
    )?;
    let sources = add(membership, metadata)?;
    prepare::within(sources, limits.preparation_bytes)?;
    let _source_allocation = admit(read, metadata)?;
    let mut nodes = capacity(ids.len())?;
    let mut edges = capacity(edge_count)?;
    read.charge(add(ids.len(), 1)?)?;
    for id in ids {
        push(&mut nodes, prefix.node(id)?)?;
        prefix.edges(id, &mut edges)?;
    }
    let quote = RecordedGraph::construction_charge(nodes.len(), edges.len())?;
    prepare::within(add(sources, quote)?, limits.preparation_bytes)?;
    let _snapshot_allocation = admit(read, quote)?;
    let visits = read.meter.remaining();
    let graph = read.meter.budget(|budget| {
        RecordedGraph::build(
            &nodes,
            &edges,
            if before == 0 {
                read.base
            } else {
                read.outcome.sequence
            },
            graph::Limits {
                nodes: limits.plan_nodes,
                edges: limits.plan_edges,
                visits,
            },
            quote,
            budget,
        )
    })?;
    let proof = graph.verification_charge()?;
    prepare::within(add(add(sources, quote)?, proof)?, limits.preparation_bytes)?;
    let _proof_allocation = admit(read, proof)?;
    check(&graph, proof)
}

fn capture<O: Overlay>(
    event: NativeEvent,
    value: NativeClaimEvent,
    read: &ReplayRead<'_, '_, O>,
) -> Result<u32, NativeError> {
    let boundary = value.graph.ok_or_else(invalid)?.before_ordinal;
    require(boundary <= event.ordinal)?;
    if matches!(
        value.kind,
        NativeEventKind::Deadlocked | NativeEventKind::Expired
    ) {
        require(boundary == event.ordinal)?;
    }
    // A frozen multi-output batch may span only graph consequences sharing
    // this exact source; any intervening unrelated fact requires recapture.
    read.charge(
        usize::try_from(event.ordinal.checked_sub(boundary).ok_or_else(invalid)?)
            .map_err(|_| ContractError::Capacity)?
            .checked_add(1)
            .ok_or(ContractError::Capacity)?,
    )?;
    for ordinal in boundary..event.ordinal {
        let NativeFact::Claim(prior) = read.event(ordinal)?.fact else {
            return Err(invalid());
        };
        require(
            matches!(
                prior.kind,
                NativeEventKind::DependencyFailed | NativeEventKind::Satisfied
            ) && prior.graph == value.graph,
        )?;
    }
    Ok(boundary)
}

fn timer<O: Overlay>(
    prefix: &Prefix<'_, '_, '_, O>,
    graph: &RecordedGraph,
) -> Result<(ClaimId, focal_model::Deadline), NativeError> {
    let read = prefix.read;
    let (claim, timer, generation, deadline) = match read.outcome.invocation {
        NativeInvocation::ClaimDeadline(key) => (
            key.claim,
            key.timer,
            key.generation,
            prefix.claim(key.claim)?.deadline().ok_or_else(invalid)?,
        ),
        NativeInvocation::MonitorDeadline(key) => {
            let owner = prefix.claim(key.claim)?;
            let scope = prefix
                .scope(
                    key.claim,
                    owner.scopes().monitor(key.monitor).ok_or_else(invalid)?,
                )?
                .ok_or_else(invalid)?;
            require(scope.active)?;
            read.charge(mul(
                add(scope.roots.len(), 1)?,
                const { (usize::BITS as usize + 1) * 16 },
            )?)?;
            let mut unsettled = false;
            for root in scope.roots {
                unsettled |= !graph.wait_settled(*root)?;
            }
            require(unsettled)?;
            (key.claim, key.timer, key.generation, scope.deadline)
        }
        _ => return Err(invalid()),
    };
    require(
        deadline.timer == timer
            && deadline.generation == generation
            && read.outcome.logical_time >= deadline.at,
    )?;
    Ok((claim, deadline))
}

pub(super) fn validate<O: Overlay>(
    id: ClaimId,
    read: &ReplayRead<'_, '_, O>,
) -> Result<(), NativeError> {
    let next = read.claim(id)?;
    read.events(Key::Claim(id), |event| {
        let NativeFact::Claim(value) = event.fact else {
            return Err(invalid());
        };
        if let NativeEventKind::Monitor(NativeMonitorEvent::Released { id: monitor, .. }) =
            value.kind
        {
            require(value.graph.is_none())?;
            let prefix = Prefix {
                read,
                before: event.ordinal,
            };
            require(Some(prefix.node(id)?.binding) == value.before)?;
            return snapshot(read, id, event.ordinal, |graph, _| {
                let scope = prefix
                    .scope(id, next.scopes().monitor(monitor).ok_or_else(invalid)?)?
                    .ok_or_else(invalid)?;
                require(scope.active)?;
                read.charge(mul(
                    add(scope.roots.len(), 1)?,
                    const { (usize::BITS as usize + 1) * 16 },
                )?)?;
                for root in scope.roots {
                    require(graph.wait_settled(*root)?)?;
                }
                Ok(())
            });
        }
        let graph_kind = matches!(
            value.kind,
            NativeEventKind::DependencyFailed
                | NativeEventKind::Deadlocked
                | NativeEventKind::Satisfied
                | NativeEventKind::Expired
        );
        require(graph_kind == value.graph.is_some())?;
        if !graph_kind {
            return Ok(());
        }
        let before = capture(event, value, read)?;
        let prefix = Prefix { read, before };
        require(Some(prefix.node(id)?.binding) == value.before)?;
        snapshot(read, id, before, |graph, proof| {
            match value.kind {
                NativeEventKind::DependencyFailed | NativeEventKind::Deadlocked => {
                    let Some(ClaimTerminalCut::Graph(cut)) = next.terminal_cut() else {
                        return Err(invalid());
                    };
                    if value.kind == NativeEventKind::Deadlocked {
                        let (trigger, deadline) = timer(&prefix, graph)?;
                        require(
                            cut.origin().binding() == prefix.node(trigger)?.binding
                                && cut.deadline() == Some(deadline),
                        )?;
                    }
                    read.meter
                        .budget(|visits| graph.verify_terminal(id, cut, proof, visits))?;
                }
                NativeEventKind::Satisfied => {
                    let Some(ClaimTerminalCut::Explicit(cut)) = next.terminal_cut() else {
                        return Err(invalid());
                    };
                    read.meter
                        .budget(|visits| graph.verify_release(id, cut, visits))?;
                }
                NativeEventKind::Expired => {
                    let (trigger, deadline) = timer(&prefix, graph)?;
                    require(trigger == id)?;
                    read.meter.budget(|visits| {
                        graph.verify_expiry(id, deadline, read.outcome.logical_time, proof, visits)
                    })?;
                }
                _ => return Err(invalid()),
            }
            Ok(())
        })
    })?;
    // A directly indexed affected claim cannot silently omit a consequence.
    // Every recursive dependent will be selected when its predecessor changes.
    if !next.is_terminal() {
        snapshot(read, id, read.outcome.events, |graph, proof| {
            require(
                !read
                    .meter
                    .budget(|visits| graph.has_dependency_failure(id, proof, visits))?,
            )?;
            read.charge(128)?;
            require(!next.local_complete() || !graph.satisfied(id)?)
        })?;
    }
    Ok(())
}
