//! A monitor's deadline is distinct from the claim's authored deadline. Only a
//! complete negative SCC query can authorize expiry at this earlier deadline.
use super::*;
use crate::lifecycle::{graph, memory as bytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonitorDeadlineRequest {
    pub id: MonitorId,
    pub deadline: Deadline,
    pub fired_at: u64,
}

#[derive(Debug)]
pub struct MonitorDeadlinePlan<'a> {
    owner: &'a ClaimState,
    graph: &'a Snapshot,
    peers: &'a [&'a ClaimState],
    request: MonitorDeadlineRequest,
    cut: ClaimCut,
    charge: usize,
    visits: graph::VisitBudget,
    inactive: bool,
    settled: bool,
}

/// Resolution does not mutate the source or consume a durable timer. A native
/// publisher must retain the decision's provenance and all graph consequences.
#[derive(Debug)]
pub enum MonitorDeadlineDecision<'a> {
    Inactive,
    Settled,
    Deadlock(graph::Deadlock<'a>),
    Expire(MonitorExpiry<'a>),
}

/// Private construction proves this exact active monitor was due and its owner
/// had no qualifying unsatisfied SCC at the supplied complete graph prefix.
#[derive(Debug)]
pub struct MonitorExpiry<'a> {
    owner: &'a ClaimState,
    graph: &'a Snapshot,
    _peers: &'a [&'a ClaimState],
    request: MonitorDeadlineRequest,
    cut: ClaimCut,
}

impl Registry {
    /// No wall clock is read here: `fired_at` and `cut` must be supplied by the
    /// publishing owner. Preparation checks identity/cuts without allocating.
    pub fn prepare_monitor_deadline<'a>(
        owner: &'a ClaimState,
        request: MonitorDeadlineRequest,
        graph: &'a Snapshot,
        peers: &'a [&'a ClaimState],
        cut: ClaimCut,
        limits: BuildLimits,
    ) -> Result<MonitorDeadlinePlan<'a>, ContractError> {
        let mut visits = graph::VisitBudget::new(limits.visits);
        Self::prepare_monitor_deadline_with_visits(
            owner,
            request,
            graph,
            peers,
            cut,
            limits,
            &mut visits,
        )
    }

    /// Shares preparation and subsequent resolution work with the transaction.
    /// Every completed source check remains charged even when preparation fails.
    #[allow(clippy::too_many_arguments)] // Exact borrowed timer source and shared admission limits.
    pub fn prepare_monitor_deadline_with_visits<'a>(
        owner: &'a ClaimState,
        request: MonitorDeadlineRequest,
        graph: &'a Snapshot,
        peers: &'a [&'a ClaimState],
        cut: ClaimCut,
        limits: BuildLimits,
        shared: &mut graph::VisitBudget,
    ) -> Result<MonitorDeadlinePlan<'a>, ContractError> {
        let admitted = limits.visits.min(shared.remaining());
        let mut visits = graph::VisitBudget::new(admitted);
        let result = Self::prepare_monitor_deadline_budget(
            owner,
            request,
            graph,
            peers,
            cut,
            limits,
            &mut visits,
        );
        shared.charge(
            admitted
                .checked_sub(visits.remaining())
                .ok_or(ContractError::Capacity)?,
        )?;
        result
    }

    #[allow(clippy::too_many_arguments)] // One preparation, preserving its actual traversal debit.
    fn prepare_monitor_deadline_budget<'a>(
        owner: &'a ClaimState,
        request: MonitorDeadlineRequest,
        graph: &'a Snapshot,
        peers: &'a [&'a ClaimState],
        cut: ClaimCut,
        limits: BuildLimits,
        visits: &mut graph::VisitBudget,
    ) -> Result<MonitorDeadlinePlan<'a>, ContractError> {
        let count = bytes::add(peers.len(), 1)?;
        visits.charge(bytes::add(
            count.checked_mul(3).ok_or(ContractError::Capacity)?,
            8,
        )?)?;
        Self::check_cut(owner, cut)?;
        graph.check_owner(owner, peers)?;
        graph.check_cut(cut.position)?;
        if cut.cause == crate::ContentHash([0; 32])
            || request.id.is_zero()
            || request.deadline.timer.is_zero()
            || request.deadline.generation == 0
            || request.fired_at < request.deadline.at
        {
            return Err(ContractError::InvalidCut);
        }
        let registry = owner.scopes();
        visits.charge(registry.scopes.len())?;
        let index = registry
            .scopes
            .binary_search_by_key(&request.id, |scope| scope.id)
            .map_err(|_| ContractError::InvalidTarget)?;
        let scope = registry
            .scopes
            .get(index)
            .ok_or(ContractError::InvalidTarget)?;
        if scope.deadline != request.deadline || scope.registered > cut.position {
            return Err(ContractError::InvalidCut);
        }
        let inactive = owner.is_terminal() || registry.released() || !scope.active();
        let mut settled = true;
        if !inactive {
            owner.binding().next()?;
            // Assess all predicates at this exact graph cut. A settled timer
            // requests the real monitor-release transition, never expiry.
            for root in &scope.roots {
                visits.charge(bytes::add(count, 1)?)?;
                settled &= graph.wait_settled(*root)?;
            }
        }
        let charge = if inactive || settled {
            0
        } else {
            graph.deadlock_charge()?
        };
        bytes::fits(charge, limits.bytes)?;
        Ok(MonitorDeadlinePlan {
            owner,
            graph,
            peers,
            request,
            cut,
            charge,
            visits: graph::VisitBudget::new(visits.remaining()),
            inactive,
            settled,
        })
    }
}

impl<'a> MonitorDeadlinePlan<'a> {
    /// Additional SCC construction storage; original sources and this small
    /// borrowed plan remain the caller's separately accounted responsibility.
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    pub fn remaining_visits(&self) -> usize {
        self.visits.remaining()
    }
    pub fn resolve(self) -> Result<MonitorDeadlineDecision<'a>, ContractError> {
        let mut visits = graph::VisitBudget::new(self.visits.remaining());
        self.resolve_with_visits(&mut visits)
    }

    /// Continue under both this plan's original remaining limit and the shared
    /// transaction allowance. Failed or negative SCC queries never renew either.
    pub fn resolve_with_visits(
        self,
        shared: &mut graph::VisitBudget,
    ) -> Result<MonitorDeadlineDecision<'a>, ContractError> {
        let admitted = self.visits.remaining().min(shared.remaining());
        let mut visits = graph::VisitBudget::new(admitted);
        let result = self.resolve_budget(&mut visits);
        shared.charge(
            admitted
                .checked_sub(visits.remaining())
                .ok_or(ContractError::Capacity)?,
        )?;
        result
    }

    fn resolve_budget(
        self,
        visits: &mut graph::VisitBudget,
    ) -> Result<MonitorDeadlineDecision<'a>, ContractError> {
        if self.inactive {
            return Ok(MonitorDeadlineDecision::Inactive);
        }
        if self.settled {
            return Ok(MonitorDeadlineDecision::Settled);
        }
        // The SCC query enforces the captured earliest effective deadline.
        // An earlier due claim/monitor must be processed first; its refusal is
        // not a negative-cycle proof and must not consume this timer.
        let witness = self.graph.deadlock_query_with_visits(
            ClaimId(self.owner.binding().object.0),
            self.request.deadline,
            self.request.fired_at,
            self.cut.position,
            self.charge,
            visits,
        )?;
        match witness {
            Some(witness) => Ok(MonitorDeadlineDecision::Deadlock(witness)),
            None => Ok(MonitorDeadlineDecision::Expire(MonitorExpiry {
                owner: self.owner,
                graph: self.graph,
                _peers: self.peers,
                request: self.request,
                cut: self.cut,
            })),
        }
    }
}

impl MonitorExpiry<'_> {
    pub fn owner(&self) -> Binding {
        self.owner.binding()
    }
    pub fn request(&self) -> MonitorDeadlineRequest {
        self.request
    }
    pub fn cut(&self) -> ClaimCut {
        self.cut
    }
    pub(in crate::lifecycle) fn check(
        &self,
        owner: &ClaimState,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        self.owner.binding().check(&owner.binding())?;
        if owner != self.owner {
            return Err(ContractError::ContentConflict);
        }
        self.graph.check_owner(owner, peers)?;
        Ok(())
    }
}
