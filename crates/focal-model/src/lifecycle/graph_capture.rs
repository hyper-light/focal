//! Preflight for graph capture itself. The caller owns the complete sorted
//! borrowed closure and reserves the returned charge before build; this module
//! allocates only the Snapshot's four metadata vectors. Start/check operations
//! borrow those vectors and do not allocate. Dependency/SCC witness construction
//! remains a separate operation with its own bounded workspace.
use super::*;
use crate::lifecycle::memory as bytes;

const ALLOCATION: usize = 4 * size_of::<usize>();

#[derive(Debug)]
pub struct CapturePlan<'a> {
    claims: &'a [&'a ClaimState],
    limits: Limits,
    total_edges: usize,
    minimum_cut: SessionSeq,
    visits: usize,
    heap: usize,
    allocations: usize,
    charge: usize,
}

fn checked_reserve<T>(capacity: usize) -> Result<Vec<T>, ContractError> {
    #[cfg(test)]
    let requested = EXCESS.with(|extra| {
        if extra.replace(false) {
            capacity.checked_add(1).ok_or(ContractError::Capacity)
        } else {
            Ok(capacity)
        }
    })?;
    #[cfg(not(test))]
    let requested = capacity;
    let values = bytes::reserve::<T>(requested)?;
    if size_of::<T>() != 0 && values.capacity() > capacity {
        return Err(ContractError::Capacity);
    }
    Ok(values)
}

#[cfg(test)]
std::thread_local! {
    static EXCESS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
#[cfg(test)]
pub(super) fn with_excess_capacity<T>(action: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            EXCESS.with(|value| value.set(self.0));
        }
    }
    let _restore = Restore(EXCESS.with(|value| value.replace(true)));
    action()
}

fn metadata(allocations: usize) -> Result<usize, ContractError> {
    allocations
        .checked_mul(ALLOCATION)
        .ok_or(ContractError::Capacity)
}

impl Snapshot {
    /// Supply the complete bounded closure in ClaimId order, including terminal
    /// endpoints. Preflight performs no allocation and counts all capture buffers.
    /// `max_bytes` includes Snapshot inline storage and allocator metadata.
    /// Endpoint resolution and the shared least-fixpoint run during build;
    /// preparing a memory allowance alone does not produce a graph witness.
    pub fn prepare_capture<'a>(
        claims: &'a [&'a ClaimState],
        limits: Limits,
        max_bytes: usize,
    ) -> Result<CapturePlan<'a>, ContractError> {
        if claims.is_empty() || claims.len() > limits.nodes {
            return Err(ContractError::Capacity);
        }
        let mut budget = Budget {
            left: limits.visits,
        };
        let mut total_edges = 0usize;
        let mut minimum_cut = SessionSeq(0);
        let mut prior = None;
        let ledger = claims
            .first()
            .ok_or(ContractError::InvalidTarget)?
            .binding()
            .ledger;
        for claim in claims {
            budget.visit()?;
            if claim.binding().ledger != ledger {
                return Err(ContractError::WrongLedger);
            }
            if prior.is_some_and(|old| old >= claim.binding().object) {
                return Err(ContractError::InvalidManifest);
            }
            if claim.created().0 == 0 {
                return Err(ContractError::InvalidCut);
            }
            minimum_cut = minimum_cut
                .max(claim.created())
                .max(claim.local_sealed_at().unwrap_or(SessionSeq(0)));
            if let Some(cut) = claim.terminal_cut() {
                minimum_cut = minimum_cut.max(match cut {
                    ClaimTerminalCut::Explicit(cut) => cut.position,
                    ClaimTerminalCut::Required(cut) => cut.sequence(),
                    ClaimTerminalCut::Graph(cut) => cut.sequence(),
                });
            }
            if let Some(cut) = claim.scopes().release_cut() {
                minimum_cut = minimum_cut.max(cut.position);
            }
            for child in claim.scopes().children() {
                budget.visit()?;
                minimum_cut = minimum_cut.max(child.registered());
            }
            prior = Some(claim.binding().object);
            total_edges = total_edges
                .checked_add(claim.graph().obligations().len())
                .ok_or(ContractError::Capacity)?;
            for scope in claim.scopes().iter() {
                budget.visit()?;
                minimum_cut = minimum_cut.max(scope.registered());
                if let Some(cut) = scope.release_cut() {
                    minimum_cut = minimum_cut.max(cut.position);
                }
                if let Some(change) = scope.last_rebinding() {
                    minimum_cut = minimum_cut.max(change.cut.position);
                }
                if scope.released().is_none() {
                    total_edges = total_edges
                        .checked_add(scope.roots().len())
                        .ok_or(ContractError::Capacity)?;
                }
            }
            if total_edges > limits.edges {
                return Err(ContractError::Capacity);
            }
        }
        let heap = bytes::add(
            bytes::add(
                bytes::array::<Node>(claims.len())?,
                bytes::array::<bool>(claims.len())?,
            )?,
            bytes::array::<Edge>(total_edges)?
                .checked_mul(2)
                .ok_or(ContractError::Capacity)?,
        )?;
        let allocations = bytes::add(
            bytes::add(
                bytes::allocation::<Node>(claims.len()),
                bytes::allocation::<bool>(claims.len()),
            )?,
            bytes::allocation::<Edge>(total_edges)
                .checked_mul(2)
                .ok_or(ContractError::Capacity)?,
        )?;
        let charge = bytes::add(bytes::total::<Snapshot>(heap)?, metadata(allocations)?)?;
        bytes::fits(charge, max_bytes)?;
        Ok(CapturePlan {
            claims,
            limits,
            total_edges,
            minimum_cut,
            visits: budget.left,
            heap,
            allocations,
            charge,
        })
    }

    /// Actual capacities only; excludes Snapshot inline storage and allocator metadata.
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        let nodes = bytes::array::<Node>(self.nodes.capacity())?;
        let edges = bytes::array::<Edge>(self.edges.capacity())?;
        let incoming = bytes::array::<Edge>(self.incoming.capacity())?;
        let satisfied = bytes::array::<bool>(self.satisfied.capacity())?;
        bytes::add(bytes::add(nodes, edges)?, bytes::add(incoming, satisfied)?)
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        bytes::add(
            bytes::add(
                bytes::allocation::<Node>(self.nodes.capacity()),
                bytes::allocation::<Edge>(self.edges.capacity()),
            )?,
            bytes::add(
                bytes::allocation::<Edge>(self.incoming.capacity()),
                bytes::allocation::<bool>(self.satisfied.capacity()),
            )?,
        )
    }
    /// Model byte convention: inline plus dynamic capacity, excluding allocator metadata.
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    /// Complete actual owner charge, including allocator metadata for each buffer.
    pub fn retained_charge(&self) -> Result<usize, ContractError> {
        bytes::add(self.retained_bytes()?, metadata(self.heap_allocations()?)?)
    }
}

impl CapturePlan<'_> {
    /// Peak owned capture charge: Snapshot inline storage, all retained buffers,
    /// and allocator metadata. No additional algorithm heap workspace is used.
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    /// Raw requested Vec capacity bytes, excluding inline/allocator metadata.
    pub fn construction_heap_bytes(&self) -> usize {
        self.heap
    }
    pub fn construction_heap_allocations(&self) -> usize {
        self.allocations
    }

    pub fn build(self) -> Result<Snapshot, ContractError> {
        let Self {
            claims,
            limits,
            total_edges,
            minimum_cut,
            visits,
            charge,
            ..
        } = self;
        let mut budget = Budget { left: visits };
        let mut nodes = checked_reserve(claims.len())?;
        let mut edges = checked_reserve(total_edges)?;
        let mut incoming = checked_reserve(total_edges)?;
        let mut satisfied = checked_reserve(claims.len())?;
        for (source, claim) in claims.iter().enumerate() {
            let edges_start = edges.len();
            for obligation in claim.graph().obligations() {
                budget.visit()?;
                let target = claims
                    .binary_search_by_key(&obligation.target, |claim| id(claim.binding()))
                    .map_err(|_| ContractError::InvalidTarget)?;
                edges.push(Edge {
                    source,
                    target,
                    predicate: match obligation.kind {
                        Kind::DependsOn => Predicate::Satisfied,
                        Kind::Awaits => Predicate::Terminal,
                    },
                    propagates_failure: obligation.kind == Kind::DependsOn,
                    runtime: false,
                });
            }
            for scope in claim
                .scopes()
                .iter()
                .filter(|scope| scope.released().is_none())
            {
                for root in scope.roots() {
                    budget.visit()?;
                    let (target, predicate) = match *root {
                        WaitPredicate::Satisfied(id) => (id, Predicate::Satisfied),
                        WaitPredicate::Terminal(id) => (id, Predicate::Terminal),
                        WaitPredicate::Released(id) => (id, Predicate::Released),
                    };
                    let target = claims
                        .binary_search_by_key(&target, |claim| id(claim.binding()))
                        .map_err(|_| ContractError::InvalidTarget)?;
                    edges.push(Edge {
                        source,
                        target,
                        predicate,
                        propagates_failure: false,
                        runtime: true,
                    });
                }
            }
            for child in claim.scopes().children() {
                budget.visit()?;
                let child_index = claims
                    .binary_search_by_key(&child.id(), |claim| id(claim.binding()))
                    .map_err(|_| ContractError::InvalidTarget)?;
                let current = claims
                    .get(child_index)
                    .ok_or(ContractError::InvalidTarget)?
                    .binding();
                let declared = child.binding();
                declared.check(&Binding {
                    revision: declared.revision,
                    ..current
                })?;
            }
            let deadline = claim
                .deadline()
                .into_iter()
                .chain(
                    claim
                        .scopes()
                        .iter()
                        .filter(|scope| scope.released().is_none())
                        .map(|scope| scope.deadline()),
                )
                .min_by_key(|deadline| (deadline.at, deadline.timer, deadline.generation));
            let origin = if claim.status().is_terminal() && claim.status() != ClaimStatus::Satisfied
            {
                match claim.terminal_cut() {
                    Some(ClaimTerminalCut::Graph(cut))
                        if cut.kind == FailureKind::DependencyFailed =>
                    {
                        Some(cut.origin)
                    }
                    _ => Some(Origin {
                        binding: claim.binding(),
                        created: claim.created(),
                        terminal: claim.local_sealed_at().ok_or(ContractError::InvalidCut)?,
                    }),
                }
            } else {
                None
            };
            nodes.push(Node {
                binding: claim.binding(),
                created: claim.created(),
                status: claim.status(),
                local_complete: claim.local_complete(),
                released: claim.scopes().released(),
                deadline,
                origin,
                edges_start,
                edges_end: edges.len(),
            });
        }
        incoming.extend_from_slice(&edges);
        incoming
            .sort_unstable_by_key(|edge| (edge.target, edge.source, edge.predicate, edge.runtime));
        satisfied.extend(
            nodes
                .iter()
                .map(|node| node.status == ClaimStatus::Satisfied),
        );
        let mut graph = Snapshot {
            nodes,
            edges,
            incoming,
            satisfied,
            limits,
            minimum_cut,
        };
        loop {
            let mut changed = false;
            for index in 0..graph.nodes.len() {
                budget.visit()?;
                let node = graph.node(index)?;
                if graph.is_satisfied(index)? || node.status.is_terminal() || !node.local_complete {
                    continue;
                }
                if graph.predicates(index, &mut budget)? {
                    *graph
                        .satisfied
                        .get_mut(index)
                        .ok_or(ContractError::InvalidTarget)? = true;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        bytes::fits(graph.retained_charge()?, charge)?;
        Ok(graph)
    }
}
