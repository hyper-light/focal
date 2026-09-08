//! Deadline SCC assessment has an explicit negative result. Exhaustion or bad
//! source facts never become evidence that ordinary claim expiry is appropriate.
use super::*;
use crate::lifecycle::memory as bytes;

const ALLOCATION: usize = 4 * size_of::<usize>();

fn buffer<T>(count: usize) -> Result<usize, ContractError> {
    bytes::add(
        bytes::array::<T>(count)?,
        if bytes::allocation::<T>(count) != 0 {
            ALLOCATION
        } else {
            0
        },
    )
}

fn checked_reserve<T>(count: usize) -> Result<Vec<T>, ContractError> {
    #[cfg(test)]
    let requested = EXCESS.with(|value| {
        if value.replace(false) {
            count.checked_add(1).ok_or(ContractError::Capacity)
        } else {
            Ok(count)
        }
    })?;
    #[cfg(not(test))]
    let requested = count;
    let values = bytes::reserve::<T>(requested)?;
    if size_of::<T>() != 0 && values.capacity() > count {
        return Err(ContractError::Capacity);
    }
    Ok(values)
}

fn push<T>(values: &mut Vec<T>, value: T) -> Result<(), ContractError> {
    if values.len() == values.capacity() {
        return Err(ContractError::Capacity);
    }
    values.push(value);
    Ok(())
}

impl Snapshot {
    /// Additional construction peak, while this Snapshot and all original owner
    /// rows remain charged separately: witness inline storage, two reachability
    /// bitmaps (Vec<bool> byte storage), one reusable queue, the retained SCC
    /// component and all four allocator overheads. No allocation occurs here.
    pub fn deadlock_charge(&self) -> Result<usize, ContractError> {
        let count = self.nodes.len();
        bytes::add(
            size_of::<Deadlock<'_>>(),
            bytes::add(
                buffer::<bool>(count)?
                    .checked_mul(2)
                    .ok_or(ContractError::Capacity)?,
                buffer::<usize>(count)?
                    .checked_mul(2)
                    .ok_or(ContractError::Capacity)?,
            )?,
        )
    }

    /// Compatibility wrapper retaining the original witness-or-error contract.
    pub fn deadlock(
        &self,
        trigger: ClaimId,
        deadline: Deadline,
        fired_at: u64,
    ) -> Result<Deadlock<'_>, ContractError> {
        self.deadlock_with_budget(trigger, deadline, fired_at, usize::MAX)
    }

    /// Bounded compatibility wrapper. A publishing owner should use the query
    /// below with its actual effective prefix, so a negative SCC proof is checked
    /// against the same source cut as its subsequent expiry decision.
    pub fn deadlock_with_budget(
        &self,
        trigger: ClaimId,
        deadline: Deadline,
        fired_at: u64,
        max_bytes: usize,
    ) -> Result<Deadlock<'_>, ContractError> {
        self.deadlock_query_with_budget(trigger, deadline, fired_at, self.minimum_cut, max_bytes)?
            .ok_or(ContractError::InvalidTransition)
    }

    /// A valid due, open trigger returns None if it is already graph-satisfied
    /// or has no qualifying unsatisfied SCC. All source, deadline, cut, visit and
    /// allocation errors remain Err; none authorize fallback to ordinary expiry.
    /// The owner supplies a complete Snapshot and separately rechecks its live
    /// rows before publishing either the witness or the negative-query outcome.
    pub fn deadlock_query_with_budget(
        &self,
        trigger: ClaimId,
        deadline: Deadline,
        fired_at: u64,
        sequence: SessionSeq,
        max_bytes: usize,
    ) -> Result<Option<Deadlock<'_>>, ContractError> {
        let mut visits = VisitBudget::new(self.limits.visits);
        self.deadlock_query_with_visits(
            trigger,
            deadline,
            fired_at,
            sequence,
            max_bytes,
            &mut visits,
        )
    }

    /// Checks the same SCC query using a shared transaction allowance. Each
    /// query also retains the Snapshot's original per-operation visit limit;
    /// work preceding an error stays charged to the shared allowance.
    pub fn deadlock_query_with_visits(
        &self,
        trigger: ClaimId,
        deadline: Deadline,
        fired_at: u64,
        sequence: SessionSeq,
        max_bytes: usize,
        visits: &mut VisitBudget,
    ) -> Result<Option<Deadlock<'_>>, ContractError> {
        visits.with_limit(self.limits.visits, |budget| {
            self.deadlock_query_budget(trigger, deadline, fired_at, sequence, max_bytes, budget)
        })
    }

    fn deadlock_query_budget(
        &self,
        trigger: ClaimId,
        deadline: Deadline,
        fired_at: u64,
        sequence: SessionSeq,
        max_bytes: usize,
        budget: &mut Budget,
    ) -> Result<Option<Deadlock<'_>>, ContractError> {
        self.check_cut(sequence)?;
        let trigger = self.index(trigger)?;
        let node = self.node(trigger)?;
        if node.deadline != Some(deadline) || fired_at < deadline.at {
            return Err(ContractError::InvalidCut);
        }
        if node.status.is_terminal() {
            return Err(ContractError::InvalidTransition);
        }
        if self.nodes.len() > self.limits.nodes
            || self.edges.len() > self.limits.edges
            || self.incoming.len() != self.edges.len()
            || self.satisfied.len() != self.nodes.len()
        {
            return Err(ContractError::InvalidManifest);
        }
        let charge = self.deadlock_charge()?;
        bytes::fits(charge, max_bytes)?;
        if self.is_satisfied(trigger)? {
            return Ok(None);
        }

        let count = self.nodes.len();
        // Every buffer is admitted before traversal, with no resizing later.
        // Forward and reverse walks reuse one queue under a shared visit budget.
        let mut forward = checked_reserve::<bool>(count)?;
        let mut reverse = checked_reserve::<bool>(count)?;
        let mut queue = checked_reserve::<usize>(count)?;
        let mut component = checked_reserve::<usize>(count)?;
        forward.resize(count, false);
        reverse.resize(count, false);
        self.deadline_reachable(trigger, false, &mut forward, &mut queue, budget)?;
        self.deadline_reachable(trigger, true, &mut reverse, &mut queue, budget)?;

        let mut victim = None;
        for (index, (forward, reverse)) in forward.iter().zip(&reverse).enumerate() {
            budget.visit()?;
            if *forward && *reverse {
                push(&mut component, index)?;
                let node = self.node(index)?;
                if !node.status.is_terminal() {
                    let order = (node.created, node.binding.object);
                    if victim.is_none_or(|(_, previous)| order < previous) {
                        victim = Some((index, order));
                    }
                }
            }
        }
        let mut self_cycle = false;
        if component.len() < 2 {
            for edge in self.outgoing(trigger)? {
                budget.visit()?;
                if edge.target == trigger && self.deadline_edge(*edge, false)?.is_some() {
                    self_cycle = true;
                    break;
                }
            }
            if !self_cycle {
                return Ok(None);
            }
        }
        let victim = victim.ok_or(ContractError::InvalidTransition)?.0;
        // The canonical fingerprint visits exactly these retained component
        // members; debit that work from the same allowance before hashing.
        budget.charge(component.len())?;
        let fingerprint = self.fingerprint(
            b"focal.lifecycle.deadlock-scc.v1\0",
            component.iter().copied(),
        )?;
        Ok(Some(Deadlock {
            graph: self,
            victim,
            trigger,
            component,
            deadline,
            fired_at,
            fingerprint,
        }))
    }

    fn deadline_edge(&self, edge: Edge, reverse: bool) -> Result<Option<usize>, ContractError> {
        if self.settled(edge)? || (self.node(edge.source)?.status.is_terminal() && !edge.runtime) {
            return Ok(None);
        }
        let next = if reverse { edge.source } else { edge.target };
        let node = self.node(next)?;
        if (node.status.is_terminal() && edge.predicate != Predicate::Released && !reverse)
            || node.released
        {
            return Ok(None);
        }
        Ok(Some(next))
    }

    fn deadline_reachable(
        &self,
        start: usize,
        reverse: bool,
        seen: &mut [bool],
        queue: &mut Vec<usize>,
        budget: &mut Budget,
    ) -> Result<(), ContractError> {
        queue.clear();
        *seen.get_mut(start).ok_or(ContractError::InvalidTarget)? = true;
        push(queue, start)?;
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
                let Some(next) = self.deadline_edge(*edge, reverse)? else {
                    continue;
                };
                let visited = seen.get_mut(next).ok_or(ContractError::InvalidTarget)?;
                if !*visited {
                    *visited = true;
                    push(queue, next)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
std::thread_local! {
    static EXCESS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
#[path = "graph_deadline_tests.rs"]
mod tests;
