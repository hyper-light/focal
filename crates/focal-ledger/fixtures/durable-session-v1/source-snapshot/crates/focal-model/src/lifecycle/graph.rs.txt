//! Bounded dependency proofs over one effective owner prefix. Declarations are
//! owned by the generated claim; snapshots copy only graph metadata. Witnesses
//! cannot be manufactured from caller-selected statuses, and consumption rechecks
//! every row read while proving them. The owner publishes the complete consequence
//! set atomically; this module does not publish or read a clock.
use super::claim::{ClaimState, ClaimTerminalCut};
use super::{Binding, ContractError};
use crate::{ClaimId, ClaimStatus, ContentHash, Deadline, SessionSeq, WaitPredicate};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    DependsOn,
    Awaits,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Obligation {
    pub kind: Kind,
    pub target: ClaimId,
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Clone))]
pub struct Declaration {
    obligations: Vec<Obligation>,
}
impl Declaration {
    pub fn empty() -> Self {
        Self {
            obligations: Vec::new(),
        }
    }
    pub fn new(obligations: &[Obligation], max_edges: usize) -> Result<Self, ContractError> {
        if obligations.len() > max_edges {
            return Err(ContractError::Capacity);
        }
        let mut previous = None;
        for obligation in obligations {
            if obligation.target.is_zero() || previous.is_some_and(|old| old >= *obligation) {
                return Err(ContractError::InvalidManifest);
            }
            previous = Some(*obligation);
        }
        let mut owned = reserve(obligations.len())?;
        owned.extend_from_slice(obligations);
        Ok(Self { obligations: owned })
    }
    pub fn obligations(&self) -> &[Obligation] {
        &self.obligations
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub nodes: usize,
    pub edges: usize,
    pub visits: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Origin {
    binding: Binding,
    created: SessionSeq,
    terminal: SessionSeq,
}
impl Origin {
    pub fn binding(&self) -> Binding {
        self.binding
    }
    pub fn created(&self) -> SessionSeq {
        self.created
    }
    pub fn terminal(&self) -> SessionSeq {
        self.terminal
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    DependencyFailed,
    Deadlocked,
}
/// Retained typed provenance. The complete path/SCC is available on the checked
/// witness for the same atomic history publication; its fingerprint binds the cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalCut {
    sequence: SessionSeq,
    kind: FailureKind,
    origin: Origin,
    fingerprint: ContentHash,
    deadline: Option<Deadline>,
    fired_at: Option<u64>,
}
impl TerminalCut {
    pub fn sequence(&self) -> SessionSeq {
        self.sequence
    }
    pub fn kind(&self) -> FailureKind {
        self.kind
    }
    pub fn origin(&self) -> Origin {
        self.origin
    }
    pub fn fingerprint(&self) -> ContentHash {
        self.fingerprint
    }
    pub fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }
    pub fn fired_at(&self) -> Option<u64> {
        self.fired_at
    }
}

#[derive(Debug, Clone, Copy)]
struct Node {
    binding: Binding,
    created: SessionSeq,
    status: ClaimStatus,
    local_complete: bool,
    released: bool,
    deadline: Option<Deadline>,
    origin: Option<Origin>,
    edges_start: usize,
    edges_end: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Predicate {
    Satisfied,
    Terminal,
    Released,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Edge {
    source: usize,
    target: usize,
    predicate: Predicate,
    propagates_failure: bool,
    runtime: bool,
}
#[derive(Debug)]
pub struct Snapshot {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
    incoming: Vec<Edge>,
    satisfied: Vec<bool>,
    limits: Limits,
    minimum_cut: SessionSeq,
}

fn reserve<T>(capacity: usize) -> Result<Vec<T>, ContractError> {
    let mut value = Vec::new();
    value
        .try_reserve_exact(capacity)
        .map_err(|_| ContractError::Capacity)?;
    Ok(value)
}
struct Budget {
    left: usize,
}
impl Budget {
    fn visit(&mut self) -> Result<(), ContractError> {
        self.left = self.left.checked_sub(1).ok_or(ContractError::Capacity)?;
        Ok(())
    }
}
fn id(binding: Binding) -> ClaimId {
    ClaimId(binding.object.0)
}
fn hash_binding(hash: &mut blake3::Hasher, binding: Binding) {
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    hash.update(&binding.revision.0.to_le_bytes());
}

impl Snapshot {
    /// Supply the complete bounded closure in ClaimId order, including terminal
    /// endpoints. Missing endpoints are refused rather than treated as settled.
    pub fn capture(claims: &[&ClaimState], limits: Limits) -> Result<Self, ContractError> {
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
        let mut nodes = reserve(claims.len())?;
        let mut edges = reserve(total_edges)?;
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
        let mut incoming = reserve(total_edges)?;
        incoming.extend_from_slice(&edges);
        incoming
            .sort_unstable_by_key(|edge| (edge.target, edge.source, edge.predicate, edge.runtime));
        let mut satisfied = reserve(nodes.len())?;
        satisfied.extend(
            nodes
                .iter()
                .map(|node| node.status == ClaimStatus::Satisfied),
        );
        let mut graph = Self {
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
        Ok(graph)
    }
    fn node(&self, index: usize) -> Result<&Node, ContractError> {
        self.nodes.get(index).ok_or(ContractError::InvalidTarget)
    }
    fn index(&self, target: ClaimId) -> Result<usize, ContractError> {
        self.nodes
            .binary_search_by_key(&target, |node| id(node.binding))
            .map_err(|_| ContractError::InvalidTarget)
    }
    fn is_satisfied(&self, index: usize) -> Result<bool, ContractError> {
        self.satisfied
            .get(index)
            .copied()
            .ok_or(ContractError::InvalidTarget)
    }
    fn outgoing(&self, index: usize) -> Result<&[Edge], ContractError> {
        let node = self.node(index)?;
        self.edges
            .get(node.edges_start..node.edges_end)
            .ok_or(ContractError::InvalidTarget)
    }
    fn predicates(&self, index: usize, budget: &mut Budget) -> Result<bool, ContractError> {
        for edge in self.outgoing(index)? {
            budget.visit()?;
            if !self.settled(*edge)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn settled(&self, edge: Edge) -> Result<bool, ContractError> {
        match edge.predicate {
            Predicate::Satisfied => self.is_satisfied(edge.target),
            Predicate::Terminal => {
                Ok(self.is_satisfied(edge.target)? || self.node(edge.target)?.status.is_terminal())
            }
            Predicate::Released => Ok(self.node(edge.target)?.released),
        }
    }
    /// Floors only positions represented in this semantic state. The publishing
    /// owner additionally supplies the ordered current mutation prefix; ordinary
    /// phase transitions do not all retain their own sequence here.
    pub fn check_cut(&self, sequence: SessionSeq) -> Result<(), ContractError> {
        if sequence.0 == 0 || sequence < self.minimum_cut {
            Err(ContractError::InvalidCut)
        } else {
            Ok(())
        }
    }
    pub fn binding(&self, target: ClaimId) -> Result<Binding, ContractError> {
        Ok(self.node(self.index(target)?)?.binding)
    }
    pub fn bindings(&self) -> impl Iterator<Item = Binding> + '_ {
        self.nodes.iter().map(|node| node.binding)
    }
    pub fn check_owner(
        &self,
        owner: &ClaimState,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.check(self.index(id(owner.binding()))?, owner, peers)
    }
    pub fn wait_settled(&self, predicate: WaitPredicate) -> Result<bool, ContractError> {
        match predicate {
            WaitPredicate::Satisfied(target) => self.is_satisfied(self.index(target)?),
            WaitPredicate::Terminal(target) => {
                let target = self.index(target)?;
                Ok(self.node(target)?.status.is_terminal() || self.is_satisfied(target)?)
            }
            WaitPredicate::Released(target) => Ok(self.node(self.index(target)?)?.released),
        }
    }
    pub fn satisfied(&self, target: ClaimId) -> Result<bool, ContractError> {
        self.is_satisfied(self.index(target)?)
    }
    pub fn start(&self, target: ClaimId) -> Result<Start<'_>, ContractError> {
        let index = self.index(target)?;
        if self.node(index)?.status != ClaimStatus::Posted {
            return Err(ContractError::InvalidTransition);
        }
        let mut budget = Budget {
            left: self.limits.visits,
        };
        if !self.predicates(index, &mut budget)? {
            return Err(ContractError::InvalidTransition);
        }
        Ok(Start {
            graph: self,
            target: index,
        })
    }
    pub fn release(&self, target: ClaimId) -> Result<Release<'_>, ContractError> {
        let index = self.index(target)?;
        let node = self.node(index)?;
        if node.status != ClaimStatus::Validating
            || !node.local_complete
            || !self.is_satisfied(index)?
        {
            return Err(ContractError::InvalidTransition);
        }
        Ok(Release {
            graph: self,
            target: index,
        })
    }
    fn check(
        &self,
        target: usize,
        current: &ClaimState,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.node(target)?.binding.check(&current.binding())?;
        if peers.len().checked_add(1) != Some(self.nodes.len()) {
            return Err(ContractError::InvalidManifest);
        }
        let mut peers = peers.iter();
        for (index, node) in self.nodes.iter().enumerate() {
            if index == target {
                continue;
            }
            let peer = peers.next().ok_or(ContractError::InvalidManifest)?;
            node.binding.check(&peer.binding())?;
        }
        Ok(())
    }
    fn fingerprint(
        &self,
        domain: &[u8],
        indexes: impl Iterator<Item = usize>,
    ) -> Result<ContentHash, ContractError> {
        let mut hash = blake3::Hasher::new();
        hash.update(domain);
        for index in indexes {
            hash_binding(&mut hash, self.node(index)?.binding);
        }
        Ok(ContentHash(*hash.finalize().as_bytes()))
    }
    pub fn dependency_failure(
        &self,
        target: ClaimId,
    ) -> Result<DependencyFailure<'_>, ContractError> {
        let target = self.index(target)?;
        if self.node(target)?.status.is_terminal() {
            return Err(ContractError::InvalidTransition);
        }
        let mut budget = Budget {
            left: self.limits.visits,
        };
        let mut seen = reserve(self.nodes.len())?;
        seen.resize(self.nodes.len(), false);
        let mut stack = reserve(self.nodes.len())?;
        let mut path = reserve(self.nodes.len())?;
        let mut selected_path = reserve(self.nodes.len())?;
        let mut selected = None;
        stack.push((target, 0usize));
        path.push(target);
        *seen.get_mut(target).ok_or(ContractError::InvalidTarget)? = true;
        while let Some((at, next_edge)) = stack.last_mut() {
            budget.visit()?;
            let edges = self.outgoing(*at)?;
            let Some(edge) = edges.get(*next_edge).copied() else {
                stack.pop();
                path.pop();
                continue;
            };
            *next_edge = next_edge.checked_add(1).ok_or(ContractError::Capacity)?;
            if !edge.propagates_failure {
                continue;
            }
            if let Some(origin) = self.node(edge.target)?.origin {
                if selected.is_none_or(|old: Origin| origin.created < old.created) {
                    selected = Some(origin);
                    selected_path.clear();
                    selected_path.extend_from_slice(&path);
                    selected_path.push(edge.target);
                }
                continue;
            }
            let seen = seen
                .get_mut(edge.target)
                .ok_or(ContractError::InvalidTarget)?;
            if *seen {
                continue;
            }
            *seen = true;
            stack.push((edge.target, 0));
            path.push(edge.target);
        }
        let origin = selected.ok_or(ContractError::InvalidTransition)?;
        let fingerprint = self.fingerprint(
            b"focal.lifecycle.dependency-path.v1\0",
            selected_path.iter().copied(),
        )?;
        Ok(DependencyFailure {
            graph: self,
            target,
            origin,
            path: selected_path,
            fingerprint,
        })
    }
    fn reachable(
        &self,
        start: usize,
        reverse: bool,
        budget: &mut Budget,
    ) -> Result<Vec<bool>, ContractError> {
        let mut seen = reserve(self.nodes.len())?;
        seen.resize(self.nodes.len(), false);
        let mut queue = reserve(self.nodes.len())?;
        *seen.get_mut(start).ok_or(ContractError::InvalidTarget)? = true;
        queue.push(start);
        let mut cursor = 0usize;
        while let Some(at) = queue.get(cursor).copied() {
            budget.visit()?;
            cursor = cursor.checked_add(1).ok_or(ContractError::Capacity)?;
            let edges = if reverse {
                let from = self.incoming.partition_point(|edge| edge.target < at);
                let to = self.incoming.partition_point(|edge| edge.target <= at);
                self.incoming
                    .get(from..to)
                    .ok_or(ContractError::InvalidTarget)?
            } else {
                self.outgoing(at)?
            };
            for edge in edges {
                budget.visit()?;
                if self.settled(*edge)?
                    || (self.node(edge.source)?.status.is_terminal() && !edge.runtime)
                {
                    continue;
                }
                let next = if reverse { edge.source } else { edge.target };
                if (self.node(next)?.status.is_terminal()
                    && edge.predicate != Predicate::Released
                    && !reverse)
                    || self.node(next)?.released
                {
                    continue;
                }
                let visited = seen.get_mut(next).ok_or(ContractError::InvalidTarget)?;
                if !*visited {
                    *visited = true;
                    queue.push(next);
                }
            }
        }
        Ok(seen)
    }
    pub fn deadlock(
        &self,
        trigger: ClaimId,
        deadline: Deadline,
        fired_at: u64,
    ) -> Result<Deadlock<'_>, ContractError> {
        let trigger = self.index(trigger)?;
        let node = self.node(trigger)?;
        if node.deadline != Some(deadline) || fired_at < deadline.at {
            return Err(ContractError::InvalidCut);
        }
        if node.status.is_terminal() || self.is_satisfied(trigger)? {
            return Err(ContractError::InvalidTransition);
        }
        let mut budget = Budget {
            left: self.limits.visits,
        };
        let forward = self.reachable(trigger, false, &mut budget)?;
        let reverse = self.reachable(trigger, true, &mut budget)?;
        let mut component = reserve(self.nodes.len())?;
        for (index, (forward, reverse)) in forward.iter().zip(&reverse).enumerate() {
            if *forward && *reverse {
                component.push(index);
            }
        }
        let self_cycle = self
            .outgoing(trigger)?
            .iter()
            .any(|edge| edge.target == trigger);
        if component.len() < 2 && !self_cycle {
            return Err(ContractError::InvalidTransition);
        }
        let victim = component
            .iter()
            .copied()
            .filter(|index| {
                self.nodes
                    .get(*index)
                    .is_some_and(|node| !node.status.is_terminal())
            })
            .min_by_key(|index| {
                self.nodes
                    .get(*index)
                    .map(|node| (node.created, node.binding.object))
            })
            .ok_or(ContractError::InvalidTransition)?;
        let fingerprint = self.fingerprint(
            b"focal.lifecycle.deadlock-scc.v1\0",
            component.iter().copied(),
        )?;
        Ok(Deadlock {
            graph: self,
            victim,
            trigger,
            component,
            deadline,
            fired_at,
            fingerprint,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Start<'a> {
    graph: &'a Snapshot,
    target: usize,
}
impl Start<'_> {
    pub(super) fn check(
        &self,
        target: &ClaimState,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.graph.check(self.target, target, peers)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Release<'a> {
    graph: &'a Snapshot,
    target: usize,
}
impl Release<'_> {
    pub(super) fn check(
        &self,
        target: &ClaimState,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.graph.check(self.target, target, peers)
    }
    pub(super) fn check_cut(&self, sequence: SessionSeq) -> Result<(), ContractError> {
        self.graph.check_cut(sequence)
    }
    pub(super) fn fingerprint(&self) -> Result<ContentHash, ContractError> {
        self.graph.fingerprint(
            b"focal.lifecycle.graph-release.v1\0",
            0..self.graph.nodes.len(),
        )
    }
}
#[derive(Debug)]
pub struct DependencyFailure<'a> {
    graph: &'a Snapshot,
    target: usize,
    origin: Origin,
    path: Vec<usize>,
    fingerprint: ContentHash,
}
impl DependencyFailure<'_> {
    pub fn origin(&self) -> Origin {
        self.origin
    }
    pub fn path(&self) -> impl Iterator<Item = Binding> + '_ {
        self.path
            .iter()
            .filter_map(|index| self.graph.nodes.get(*index).map(|node| node.binding))
    }
    pub(super) fn check(
        &self,
        target: &ClaimState,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.graph.check(self.target, target, peers)
    }
    pub(super) fn cut(&self, sequence: SessionSeq) -> Result<TerminalCut, ContractError> {
        self.graph.check_cut(sequence)?;
        if sequence.0 == 0 || sequence < self.origin.terminal {
            return Err(ContractError::InvalidCut);
        }
        Ok(TerminalCut {
            sequence,
            kind: FailureKind::DependencyFailed,
            origin: self.origin,
            fingerprint: self.fingerprint,
            deadline: None,
            fired_at: None,
        })
    }
}
#[derive(Debug)]
pub struct Deadlock<'a> {
    graph: &'a Snapshot,
    victim: usize,
    trigger: usize,
    component: Vec<usize>,
    deadline: Deadline,
    fired_at: u64,
    fingerprint: ContentHash,
}
impl Deadlock<'_> {
    pub fn victim(&self) -> Result<Binding, ContractError> {
        Ok(self.graph.node(self.victim)?.binding)
    }
    pub fn trigger(&self) -> Result<Binding, ContractError> {
        Ok(self.graph.node(self.trigger)?.binding)
    }
    pub fn component(&self) -> impl Iterator<Item = Binding> + '_ {
        self.component
            .iter()
            .filter_map(|index| self.graph.nodes.get(*index).map(|node| node.binding))
    }
    pub(super) fn check(
        &self,
        victim: &ClaimState,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.graph.check(self.victim, victim, peers)
    }
    pub(super) fn cut(&self, sequence: SessionSeq) -> Result<TerminalCut, ContractError> {
        self.graph.check_cut(sequence)?;
        let trigger = self.graph.node(self.trigger)?;
        if sequence.0 == 0 || sequence < trigger.created {
            return Err(ContractError::InvalidCut);
        }
        Ok(TerminalCut {
            sequence,
            kind: FailureKind::Deadlocked,
            origin: Origin {
                binding: trigger.binding,
                created: trigger.created,
                terminal: sequence,
            },
            fingerprint: self.fingerprint,
            deadline: Some(self.deadline),
            fired_at: Some(self.fired_at),
        })
    }
}

#[cfg(test)]
#[path = "graph_tests.rs"]
mod tests;
