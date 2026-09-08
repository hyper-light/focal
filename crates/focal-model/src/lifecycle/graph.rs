//! Bounded dependency proofs over one effective owner prefix. Declarations are
//! owned by the generated claim; snapshots copy only graph metadata. Witnesses
//! cannot be manufactured from caller-selected statuses, and consumption rechecks
//! every row read while proving them. The owner publishes the complete consequence
//! set atomically; this module does not publish or read a clock.
use super::claim::{ClaimState, ClaimTerminalCut};
#[path = "graph_capture.rs"]
mod capture;
#[path = "graph_deadline.rs"]
mod deadline;
#[path = "graph_memory.rs"]
mod memory;
use super::{Binding, ContractError};
use crate::{ClaimId, ClaimStatus, ContentHash, Deadline, SessionSeq, WaitPredicate};
pub use capture::CapturePlan;

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
        Self::check_sorted_values(
            obligations.iter().copied().map(Ok),
            obligations.len(),
            max_edges,
            &mut VisitBudget::new(usize::MAX),
        )?;
        let mut owned = reserve(obligations.len())?;
        owned.extend_from_slice(obligations);
        Ok(Self { obligations: owned })
    }
    pub fn obligations(&self) -> &[Obligation] {
        &self.obligations
    }
    /// Validate complete canonical values before allocating their final buffer.
    /// Each iterator step must be bounded; encoded adapters meter parsing separately.
    pub fn check_sorted_values(
        mut obligations: impl Iterator<Item = Result<Obligation, ContractError>>,
        count: usize,
        max_edges: usize,
        visits: &mut VisitBudget,
    ) -> Result<(), ContractError> {
        if count > max_edges {
            return Err(ContractError::Capacity);
        }
        let mut previous = None;
        for _ in 0..count {
            visits.charge(1)?;
            let obligation = obligations.next().ok_or(ContractError::InvalidManifest)??;
            if obligation.target.is_zero() || previous.is_some_and(|old| old >= obligation) {
                return Err(ContractError::InvalidManifest);
            }
            previous = Some(obligation);
        }
        visits.charge(1)?;
        match obligations.next() {
            None => Ok(()),
            Some(Err(error)) => Err(error),
            Some(Ok(_)) => Err(ContractError::InvalidManifest),
        }
    }
    /// Consume the assembly's exact-capacity funded buffer without copying it.
    pub fn from_owned_sorted(
        obligations: Vec<Obligation>,
        max_edges: usize,
        visits: &mut VisitBudget,
    ) -> Result<Self, ContractError> {
        if obligations.len() > max_edges || obligations.capacity() != obligations.len() {
            return Err(ContractError::Capacity);
        }
        Self::check_sorted_values(
            obligations.iter().copied().map(Ok),
            obligations.len(),
            max_edges,
            visits,
        )?;
        Ok(Self { obligations })
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
/// A transaction-wide allowance for the model's graph traversal steps. Reusing
/// this value across captures and queries prevents each operation from renewing
/// the allowance. Native source discovery and witness-consumption scans must be
/// charged separately by the owner through `charge`.
#[derive(Debug)]
pub struct VisitBudget {
    left: usize,
}
type Budget = VisitBudget;
impl VisitBudget {
    pub fn new(visits: usize) -> Self {
        Self { left: visits }
    }
    pub fn remaining(&self) -> usize {
        self.left
    }
    fn visit(&mut self) -> Result<(), ContractError> {
        self.charge(1)
    }
    /// Admit a known bounded amount before doing that work. A refused debit
    /// leaves the remaining allowance unchanged; it never saturates or renews it.
    pub fn charge(&mut self, visits: usize) -> Result<(), ContractError> {
        self.left = self
            .left
            .checked_sub(visits)
            .ok_or(ContractError::Capacity)?;
        Ok(())
    }
    fn with_limit<T>(
        &mut self,
        limit: usize,
        action: impl FnOnce(&mut Budget) -> Result<T, ContractError>,
    ) -> Result<T, ContractError> {
        let admitted = self.left.min(limit);
        let mut local = Self::new(admitted);
        let result = action(&mut local);
        // Keep all work already performed charged even when the operation
        // refused a source, allocation, or later traversal step.
        let consumed = admitted
            .checked_sub(local.left)
            .ok_or(ContractError::Capacity)?;
        self.charge(consumed)?;
        result
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
    /// Compatibility convenience for callers that already enforce their own
    /// complete graph allowance. Native owners reserve a CapturePlan instead.
    pub fn capture(claims: &[&ClaimState], limits: Limits) -> Result<Self, ContractError> {
        Self::prepare_capture(claims, limits, usize::MAX)?.build()
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
        self.dependency_failure_with_budget(target, usize::MAX)
    }

    /// Peak temporary allocation, including the retained selected path. Native
    /// owners reserve this before asking for a dependency-failure witness.
    pub fn dependency_failure_charge(&self) -> Result<usize, ContractError> {
        self.failure_charge(
            self.nodes.len(),
            self.nodes.len(),
            self.nodes.len(),
            self.nodes.len(),
        )
    }

    fn failure_charge(
        &self,
        seen: usize,
        stack: usize,
        path: usize,
        selected: usize,
    ) -> Result<usize, ContractError> {
        use super::memory as bytes;
        const ALLOCATION: usize = 4 * size_of::<usize>();
        let mut total = size_of::<DependencyFailure<'_>>();
        for (heap, count) in [
            (bytes::array::<bool>(seen)?, seen),
            (bytes::array::<(usize, usize)>(stack)?, stack),
            (bytes::array::<usize>(path)?, path),
            (bytes::array::<usize>(selected)?, selected),
        ] {
            total = bytes::add(total, heap)?;
            if count != 0 {
                total = bytes::add(total, ALLOCATION)?;
            }
        }
        Ok(total)
    }

    pub fn dependency_failure_with_budget(
        &self,
        target: ClaimId,
        max_bytes: usize,
    ) -> Result<DependencyFailure<'_>, ContractError> {
        let mut visits = VisitBudget::new(self.limits.visits);
        self.dependency_failure_with_visits(target, max_bytes, &mut visits)
    }

    /// Uses the shared transaction allowance in addition to this Snapshot's
    /// per-operation limit. Traversal already performed remains charged on Err.
    pub fn dependency_failure_with_visits(
        &self,
        target: ClaimId,
        max_bytes: usize,
        visits: &mut VisitBudget,
    ) -> Result<DependencyFailure<'_>, ContractError> {
        visits.with_limit(self.limits.visits, |budget| {
            self.dependency_failure_budget(target, max_bytes, budget)
        })
    }

    fn dependency_failure_budget(
        &self,
        target: ClaimId,
        max_bytes: usize,
        budget: &mut Budget,
    ) -> Result<DependencyFailure<'_>, ContractError> {
        let charge = self.dependency_failure_charge()?;
        if charge > max_bytes {
            return Err(ContractError::Capacity);
        }
        let target = self.index(target)?;
        if self.node(target)?.status.is_terminal() {
            return Err(ContractError::InvalidTransition);
        }
        let mut seen = reserve(self.nodes.len())?;
        seen.resize(self.nodes.len(), false);
        let mut stack = reserve(self.nodes.len())?;
        let mut path = reserve(self.nodes.len())?;
        let mut selected_path = reserve(self.nodes.len())?;
        if self.failure_charge(
            seen.capacity(),
            stack.capacity(),
            path.capacity(),
            selected_path.capacity(),
        )? > charge
        {
            return Err(ContractError::Capacity);
        }
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

#[path = "graph_snapshot.rs"]
mod snapshot;
#[cfg(test)]
#[path = "graph_tests.rs"]
mod tests;
pub use snapshot::{OriginSnapshotV1, TerminalCutSnapshotV1};
