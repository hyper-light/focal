//! Read-only verification of graph evidence reconstructed from retained history.
//! These scalars are untrusted. This wrapper never exposes Snapshot or a live
//! transition witness; callers must separately prove each source/history fact.
use super::*;
use crate::lifecycle::memory as bytes;

#[derive(Debug, Clone, Copy)]
pub struct RecordedNode {
    pub binding: Binding,
    pub created: SessionSeq,
    pub status: ClaimStatus,
    pub local_complete: bool,
    pub released: bool,
    pub deadline: Option<Deadline>,
    pub origin: Option<OriginSnapshotV1>,
}

#[cfg(test)]
#[path = "graph_recorded_tests.rs"]
mod tests;

/// Preserve the original per-source order: immutable obligations first, then
/// active monitors in MonitorId order and their roots in predicate order.
#[derive(Debug, Clone, Copy)]
pub struct RecordedEdge {
    pub source: ClaimId,
    pub target: WaitPredicate,
    pub propagates_failure: bool,
    pub runtime: bool,
}

#[derive(Debug)]
pub struct RecordedGraph(Snapshot);

const ALLOCATION: usize = 4 * size_of::<usize>();
fn buffer<T>(count: usize) -> Result<usize, ContractError> {
    bytes::add(
        bytes::array::<T>(count)?,
        if count == 0 { 0 } else { ALLOCATION },
    )
}
fn reserve<T>(count: usize) -> Result<Vec<T>, ContractError> {
    let value = bytes::reserve::<T>(count)?;
    if value.capacity() > count {
        return Err(ContractError::Capacity);
    }
    Ok(value)
}
fn push<T>(values: &mut Vec<T>, value: T) -> Result<(), ContractError> {
    if values.len() == values.capacity() {
        return Err(ContractError::Capacity);
    }
    values.push(value);
    Ok(())
}
fn total(a: usize, b: usize) -> Result<usize, ContractError> {
    a.checked_add(b).ok_or(ContractError::Capacity)
}
fn times(a: usize, b: usize) -> Result<usize, ContractError> {
    a.checked_mul(b).ok_or(ContractError::Capacity)
}

impl RecordedGraph {
    /// Exact bounded metadata construction charge, including inline storage and
    /// each allocator header. Source scalars remain funded by the caller.
    pub fn construction_charge(nodes: usize, edges: usize) -> Result<usize, ContractError> {
        total(
            size_of::<Self>(),
            total(
                total(buffer::<Node>(nodes)?, buffer::<bool>(nodes)?)?,
                times(buffer::<Edge>(edges)?, 2)?,
            )?,
        )
    }

    pub fn build(
        sources: &[RecordedNode],
        relations: &[RecordedEdge],
        sequence: SessionSeq,
        limits: Limits,
        max_bytes: usize,
        visits: &mut VisitBudget,
    ) -> Result<Self, ContractError> {
        if sources.is_empty() || sources.len() > limits.nodes || relations.len() > limits.edges {
            return Err(ContractError::Capacity);
        }
        let charge = Self::construction_charge(sources.len(), relations.len())?;
        bytes::fits(charge, max_bytes)?;
        // Includes terminal probes, scalar validation, endpoint searches, copies,
        // and the incoming-edge sort. Least fixed-point traversal is additional.
        let search = total(usize::BITS as usize, 1)?;
        visits.charge(times(
            total(total(sources.len(), relations.len())?, 2)?,
            times(search, 256)?,
        )?)?;
        let mut nodes = reserve::<Node>(sources.len())?;
        let mut edges = reserve::<Edge>(relations.len())?;
        let mut incoming = reserve::<Edge>(relations.len())?;
        let mut satisfied = reserve::<bool>(sources.len())?;
        let ledger = sources
            .first()
            .ok_or(ContractError::InvalidManifest)?
            .binding
            .ledger;
        let mut previous = None;
        for source in sources {
            if source.binding.ledger != ledger
                || source.binding.object.is_zero()
                || source.created.0 == 0
                || source.created > sequence
                || previous.is_some_and(|value| value >= source.binding.object)
            {
                return Err(ContractError::InvalidManifest);
            }
            previous = Some(source.binding.object);
            let failed = source.status.is_terminal() && source.status != ClaimStatus::Satisfied;
            if failed != source.origin.is_some() {
                return Err(ContractError::InvalidManifest);
            }
            let origin = match source.origin {
                Some(origin) => {
                    if origin.binding.ledger != ledger
                        || origin.binding.object.is_zero()
                        || origin.created.0 == 0
                        || origin.created > origin.terminal
                        || origin.terminal > sequence
                    {
                        return Err(ContractError::InvalidCut);
                    }
                    Some(Origin {
                        binding: origin.binding,
                        created: origin.created,
                        terminal: origin.terminal,
                    })
                }
                None => None,
            };
            push(
                &mut nodes,
                Node {
                    binding: source.binding,
                    created: source.created,
                    status: source.status,
                    local_complete: source.local_complete,
                    released: source.released,
                    deadline: source.deadline,
                    origin,
                    edges_start: 0,
                    edges_end: 0,
                },
            )?;
            push(&mut satisfied, source.status == ClaimStatus::Satisfied)?;
        }
        let mut next = relations.iter().peekable();
        for (source, row) in sources.iter().enumerate() {
            let start = edges.len();
            while let Some(edge) = next.peek() {
                if edge.source.0 < row.binding.object.0 {
                    return Err(ContractError::InvalidManifest);
                }
                if edge.source.0 != row.binding.object.0 {
                    break;
                }
                let edge = *next.next().ok_or(ContractError::InvalidManifest)?;
                let (target, predicate) = match edge.target {
                    WaitPredicate::Satisfied(id) => (id, Predicate::Satisfied),
                    WaitPredicate::Terminal(id) => (id, Predicate::Terminal),
                    WaitPredicate::Released(id) => (id, Predicate::Released),
                };
                if (edge.runtime && edge.propagates_failure)
                    || (!edge.runtime
                        && (predicate == Predicate::Released
                            || edge.propagates_failure != (predicate == Predicate::Satisfied)))
                {
                    return Err(ContractError::InvalidManifest);
                }
                let target = sources
                    .binary_search_by_key(&target.0, |row| row.binding.object.0)
                    .map_err(|_| ContractError::InvalidTarget)?;
                push(
                    &mut edges,
                    Edge {
                        source,
                        target,
                        predicate,
                        propagates_failure: edge.propagates_failure,
                        runtime: edge.runtime,
                    },
                )?;
            }
            let node = nodes.get_mut(source).ok_or(ContractError::InvalidTarget)?;
            node.edges_start = start;
            node.edges_end = edges.len();
        }
        if next.next().is_some() || edges.len() != relations.len() {
            return Err(ContractError::InvalidManifest);
        }
        for edge in &edges {
            push(&mut incoming, *edge)?;
        }
        incoming
            .sort_unstable_by_key(|edge| (edge.target, edge.source, edge.predicate, edge.runtime));
        let mut snapshot = Snapshot {
            nodes,
            edges,
            incoming,
            satisfied,
            limits,
            minimum_cut: sequence,
        };
        loop {
            let mut changed = false;
            for index in 0..snapshot.nodes.len() {
                visits.charge(1)?;
                let node = snapshot.node(index)?;
                if snapshot.is_satisfied(index)?
                    || node.status.is_terminal()
                    || !node.local_complete
                {
                    continue;
                }
                let yes = visits
                    .with_limit(limits.visits, |budget| snapshot.predicates(index, budget))?;
                if yes {
                    *snapshot
                        .satisfied
                        .get_mut(index)
                        .ok_or(ContractError::InvalidTarget)? = true;
                    changed = true;
                }
            }
            visits.charge(1)?;
            if !changed {
                break;
            }
        }
        bytes::fits(snapshot.retained_charge()?, charge)?;
        Ok(Self(snapshot))
    }

    pub fn verification_charge(&self) -> Result<usize, ContractError> {
        Ok(self
            .0
            .dependency_failure_charge()?
            .max(self.0.deadlock_charge()?))
    }
    fn proof_work(&self, visits: &mut VisitBudget) -> Result<(), ContractError> {
        // Scalar endpoint comparisons and exact binding hash preimages in the
        // canonical path/SCC verifier; its traversal debits the same allowance.
        visits.charge(times(total(self.0.nodes.len(), 1)?, 256)?)
    }
    pub fn verify_terminal(
        &self,
        target: ClaimId,
        cut: TerminalCut,
        max_bytes: usize,
        visits: &mut VisitBudget,
    ) -> Result<(), ContractError> {
        self.proof_work(visits)?;
        let actual = match cut.kind {
            FailureKind::DependencyFailed => self
                .0
                .dependency_failure_with_visits(target, max_bytes, visits)?
                .cut(cut.sequence)?,
            FailureKind::Deadlocked => {
                let deadline = cut.deadline.ok_or(ContractError::InvalidCut)?;
                let fired = cut.fired_at.ok_or(ContractError::InvalidCut)?;
                let witness = self
                    .0
                    .deadlock_query_with_visits(
                        id(cut.origin.binding),
                        deadline,
                        fired,
                        self.0.minimum_cut,
                        max_bytes,
                        visits,
                    )?
                    .ok_or(ContractError::InvalidTransition)?;
                if id(witness.victim()?) != target {
                    return Err(ContractError::InvalidTarget);
                }
                witness.cut(cut.sequence)?
            }
        };
        if actual != cut {
            return Err(ContractError::InvalidManifest);
        }
        Ok(())
    }
    pub fn verify_expiry(
        &self,
        target: ClaimId,
        deadline: Deadline,
        fired: u64,
        max_bytes: usize,
        visits: &mut VisitBudget,
    ) -> Result<(), ContractError> {
        self.proof_work(visits)?;
        if self.0.satisfied(target)?
            || self
                .0
                .deadlock_query_with_visits(
                    target,
                    deadline,
                    fired,
                    self.0.minimum_cut,
                    max_bytes,
                    visits,
                )?
                .is_some()
        {
            return Err(ContractError::InvalidTransition);
        }
        Ok(())
    }
    pub fn satisfied(&self, target: ClaimId) -> Result<bool, ContractError> {
        self.0.satisfied(target)
    }
    pub fn verify_release(
        &self,
        target: ClaimId,
        cut: super::super::claim::ClaimCut,
        visits: &mut VisitBudget,
    ) -> Result<(), ContractError> {
        self.proof_work(visits)?;
        let release = self.0.release(target)?;
        release.check_cut(cut.position)?;
        if release.fingerprint()? != cut.cause {
            return Err(ContractError::InvalidManifest);
        }
        Ok(())
    }
    pub fn wait_settled(&self, target: WaitPredicate) -> Result<bool, ContractError> {
        self.0.wait_settled(target)
    }
    pub fn has_dependency_failure(
        &self,
        target: ClaimId,
        max_bytes: usize,
        visits: &mut VisitBudget,
    ) -> Result<bool, ContractError> {
        self.proof_work(visits)?;
        match self
            .0
            .dependency_failure_with_visits(target, max_bytes, visits)
        {
            Ok(_) => Ok(true),
            Err(ContractError::InvalidTransition) => Ok(false),
            Err(error) => Err(error),
        }
    }
}
