//! Native monitor construction keeps complete source borrows until the owner
//! reserves its quote. No allocation occurs during authorization or preflight.
use super::*;
use crate::lifecycle::memory as bytes;
use std::mem::size_of;

#[derive(Debug, Clone, Copy)]
pub struct BuildLimits {
    pub bytes: usize,
    pub visits: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct RebindRequest<'a> {
    pub id: MonitorId,
    pub predecessor: &'a ClaimState,
    pub successor: &'a ClaimState,
}

#[derive(Debug)]
enum Action<'a> {
    Register {
        request: Registration<'a>,
        position: usize,
    },
    Rebind {
        position: usize,
        change: Rebinding,
    },
    Release {
        position: usize,
    },
    Cancel {
        position: usize,
        cancellation: MonitorCancellation,
    },
}

/// A checked operation over one frozen complete graph. This is construction
/// authority only: the native owner must also install reverse subscriptions,
/// recapture the changed graph and publish all consequences atomically.
#[derive(Debug)]
pub struct MonitorPlan<'a> {
    owner: &'a ClaimState,
    graph: &'a Snapshot,
    _peers: &'a [&'a ClaimState],
    action: Action<'a>,
    event: Event,
    cut: ClaimCut,
    scopes: usize,
    reads: usize,
    charge: usize,
    visits: usize,
}

fn buffer<T>(count: usize) -> Result<usize, ContractError> {
    bytes::add(
        bytes::array::<T>(count)?,
        if count == 0 { 0 } else { ALLOCATION },
    )
}

fn take(visits: &mut usize, count: usize) -> Result<(), ContractError> {
    *visits = visits.checked_sub(count).ok_or(ContractError::Capacity)?;
    Ok(())
}

fn common(
    owner: &ClaimState,
    graph: &Snapshot,
    peers: &[&ClaimState],
    cut: ClaimCut,
    remaining: &mut usize,
) -> Result<(), ContractError> {
    // Complete owner/peer comparison plus the bounded binding seeks and inline
    // cut checks. The graph and the caller's ClaimState copy remain separate.
    let count = bytes::add(peers.len(), 1)?;
    take(
        remaining,
        bytes::add(count.checked_mul(3).ok_or(ContractError::Capacity)?, 8)?,
    )?;
    Registry::check_cut(owner, cut)?;
    if cut.cause == crate::ContentHash([0; 32]) || owner.scopes().released() {
        return Err(ContractError::InvalidCut);
    }
    graph.check_owner(owner, peers)?;
    graph.check_cut(cut.position)?;
    owner.binding().next()?;
    Ok(())
}

impl Registry {
    /// Authorize and quote registration before copying any requested roots.
    /// The caller must include every root's complete graph source in `peers`.
    pub fn prepare_register_bounded<'a>(
        owner: &'a ClaimState,
        authority: Authority,
        request: Registration<'a>,
        graph: &'a Snapshot,
        peers: &'a [&'a ClaimState],
        limits: BuildLimits,
    ) -> Result<MonitorPlan<'a>, ContractError> {
        Self::authorize(owner, authority)?;
        let mut remaining = limits.visits;
        common(owner, graph, peers, authority.cut, &mut remaining)?;
        if request.id.is_zero()
            || request.deadline.timer.is_zero()
            || request.deadline.generation == 0
            || request.deadline.at <= authority.now
            || request.roots.is_empty()
        {
            return Err(ContractError::InvalidTarget);
        }
        let registry = owner.scopes();
        take(&mut remaining, registry.scopes.len())?;
        let position = registry
            .scopes
            .binary_search_by_key(&request.id, |scope| scope.id)
            .err()
            .ok_or(ContractError::InvalidTarget)?;
        let reads = bytes::add(peers.len(), 1)?;
        for root in request.roots {
            take(&mut remaining, bytes::add(reads, 1)?)?;
            graph.binding(target(*root))?;
        }
        // Sort/dedup the new root buffer and insert one scope at a known index.
        // The conservative quadratic comparison bound is finite and checked
        // before construction, including duplicate authored predicates.
        take(
            &mut remaining,
            request
                .roots
                .len()
                .checked_mul(request.roots.len())
                .and_then(|n| n.checked_add(request.roots.len()))
                .and_then(|n| n.checked_add(registry.scopes.len()))
                .ok_or(ContractError::Capacity)?,
        )?;
        MonitorPlan::quote(
            owner,
            graph,
            peers,
            authority.cut,
            limits,
            remaining,
            Action::Register { request, position },
            Event::Registered {
                id: request.id,
                cut: authority.cut,
            },
        )
    }

    /// Follow only a named, authenticated Supersedes relationship. Immutable
    /// DependsOn/Awaits declarations and the predecessor stay unchanged.
    pub fn prepare_rebind_bounded<'a>(
        owner: &'a ClaimState,
        authority: Authority,
        request: RebindRequest<'a>,
        graph: &'a Snapshot,
        peers: &'a [&'a ClaimState],
        limits: BuildLimits,
    ) -> Result<MonitorPlan<'a>, ContractError> {
        Self::authorize(owner, authority)?;
        let mut remaining = limits.visits;
        common(owner, graph, peers, authority.cut, &mut remaining)?;
        let RebindRequest {
            id,
            predecessor,
            successor,
        } = request;
        let before = ClaimId(predecessor.binding().object.0);
        let after = ClaimId(successor.binding().object.0);
        take(
            &mut remaining,
            bytes::add(peers.len(), 1)?
                .checked_mul(2)
                .ok_or(ContractError::Capacity)?,
        )?;
        graph.binding(before)?.check(&predecessor.binding())?;
        graph.binding(after)?.check(&successor.binding())?;
        if predecessor.binding().ledger != owner.binding().ledger
            || successor.binding().ledger != owner.binding().ledger
        {
            return Err(ContractError::WrongLedger);
        }
        if before == after
            || predecessor.issuer() != successor.issuer()
            || predecessor.subject() != successor.subject()
        {
            return Err(ContractError::InvalidTarget);
        }
        take(&mut remaining, successor.lineage().corrections().len())?;
        if !successor.lineage().corrections().iter().any(|row| {
            row.kind == CorrectionKind::Supersedes
                && row.predecessor == ObjectRef::claim(owner.binding().ledger, before)
        }) {
            return Err(ContractError::InvalidTarget);
        }
        let registry = owner.scopes();
        take(&mut remaining, registry.scopes.len())?;
        let position = registry
            .scopes
            .binary_search_by_key(&id, |scope| scope.id)
            .map_err(|_| ContractError::InvalidTarget)?;
        let scope = registry
            .scopes
            .get(position)
            .ok_or(ContractError::InvalidTarget)?;
        take(&mut remaining, scope.roots.len())?;
        if !scope.active() || !scope.roots.iter().any(|root| target(*root) == before) {
            return Err(ContractError::InvalidTransition);
        }
        if successor.created() > authority.cut.position {
            return Err(ContractError::InvalidCut);
        }
        take(
            &mut remaining,
            scope
                .roots
                .len()
                .checked_mul(scope.roots.len())
                .and_then(|n| {
                    scope
                        .roots
                        .len()
                        .checked_mul(2)
                        .and_then(|r| n.checked_add(r))
                })
                .ok_or(ContractError::Capacity)?,
        )?;
        let change = Rebinding {
            predecessor: before,
            successor: after,
            cut: authority.cut,
        };
        MonitorPlan::quote(
            owner,
            graph,
            peers,
            authority.cut,
            limits,
            remaining,
            Action::Rebind { position, change },
            Event::Rebound { id, change },
        )
    }

    /// Terminality alone does not settle a Satisfied or Released predicate.
    /// Even a terminal owner retains a monitor until its exact roots settle.
    pub fn prepare_release_monitor_bounded<'a>(
        owner: &'a ClaimState,
        id: MonitorId,
        graph: &'a Snapshot,
        peers: &'a [&'a ClaimState],
        cut: ClaimCut,
        limits: BuildLimits,
    ) -> Result<MonitorPlan<'a>, ContractError> {
        let mut remaining = limits.visits;
        common(owner, graph, peers, cut, &mut remaining)?;
        let registry = owner.scopes();
        take(&mut remaining, registry.scopes.len())?;
        let position = registry
            .scopes
            .binary_search_by_key(&id, |scope| scope.id)
            .map_err(|_| ContractError::InvalidTarget)?;
        let scope = registry
            .scopes
            .get(position)
            .ok_or(ContractError::InvalidTarget)?;
        if !scope.active() {
            return Err(ContractError::InvalidTransition);
        }
        for root in &scope.roots {
            take(&mut remaining, bytes::add(peers.len(), 2)?)?;
            if !graph.wait_settled(*root)? {
                return Err(ContractError::InvalidTransition);
            }
        }
        MonitorPlan::quote(
            owner,
            graph,
            peers,
            cut,
            limits,
            remaining,
            Action::Release { position },
            Event::MonitorReleased { id, cut },
        )
    }

    /// The issuer can explicitly dispose of a terminal claim's remaining wait.
    /// This does not satisfy any root, cancel child work or release the owner.
    pub fn prepare_cancel_monitor_bounded<'a>(
        owner: &'a ClaimState,
        authority: Authority,
        id: MonitorId,
        graph: &'a Snapshot,
        peers: &'a [&'a ClaimState],
        limits: BuildLimits,
    ) -> Result<MonitorPlan<'a>, ContractError> {
        owner.binding().check(&authority.expected)?;
        authority.principal.require_actor(owner.issuer())?;
        if authority.receipt != owner.receipt().map(|receipt| receipt.fence) {
            return Err(ContractError::StaleReceipt);
        }
        if !owner.is_terminal() {
            return Err(ContractError::InvalidTransition);
        }
        let mut remaining = limits.visits;
        common(owner, graph, peers, authority.cut, &mut remaining)?;
        let terminal = match owner.terminal_cut().ok_or(ContractError::InvalidCut)? {
            ClaimTerminalCut::Explicit(cut) => cut.position,
            ClaimTerminalCut::Required(cut) => cut.sequence(),
            ClaimTerminalCut::Graph(cut) => cut.sequence(),
        };
        if terminal > authority.cut.position {
            return Err(ContractError::InvalidCut);
        }
        let registry = owner.scopes();
        take(&mut remaining, registry.scopes.len())?;
        let position = registry
            .scopes
            .binary_search_by_key(&id, |scope| scope.id)
            .map_err(|_| ContractError::InvalidTarget)?;
        if !registry
            .scopes
            .get(position)
            .ok_or(ContractError::InvalidTarget)?
            .active()
        {
            return Err(ContractError::InvalidTransition);
        }
        let cancellation = MonitorCancellation {
            terminal,
            cut: authority.cut,
        };
        MonitorPlan::quote(
            owner,
            graph,
            peers,
            authority.cut,
            limits,
            remaining,
            Action::Cancel {
                position,
                cancellation,
            },
            Event::MonitorCancelled { id, cancellation },
        )
    }
}

impl<'a> MonitorPlan<'a> {
    #[allow(clippy::too_many_arguments)] // One checked source and action, with its original traversal debit.
    fn quote(
        owner: &'a ClaimState,
        graph: &'a Snapshot,
        peers: &'a [&'a ClaimState],
        cut: ClaimCut,
        limits: BuildLimits,
        mut remaining: usize,
        action: Action<'a>,
        event: Event,
    ) -> Result<Self, ContractError> {
        let source = owner.scopes();
        let added = match &action {
            Action::Register { request, .. } => request.roots.len(),
            _ => 0,
        };
        let scopes = bytes::add(
            source.scopes.len(),
            usize::from(matches!(&action, Action::Register { .. })),
        )?;
        bytes::fits(scopes, source.limits.scopes)?;
        bytes::fits(source.children.len(), source.limits.children)?;
        let reads = bytes::add(peers.len(), 1)?;
        let mut roots = added;
        let mut charge = bytes::add(size_of::<Transition>(), buffer::<Binding>(reads)?)?;
        charge = bytes::add(charge, buffer::<Scope>(scopes)?)?;
        charge = bytes::add(charge, buffer::<OwnedChild>(source.children.len())?)?;
        if added != 0 {
            charge = bytes::add(charge, buffer::<WaitPredicate>(added)?)?;
        }
        for scope in &source.scopes {
            take(&mut remaining, 1)?;
            roots = bytes::add(roots, scope.roots.len())?;
            charge = bytes::add(charge, buffer::<WaitPredicate>(scope.roots.len())?)?;
        }
        bytes::fits(roots, source.limits.roots)?;
        // Copies plus the complete post-build allocation reconciliation. Source
        // children/roots are canonical and require no new identity discovery.
        let build = bytes::add(
            bytes::add(reads, source.children.len())?,
            bytes::add(roots, scopes.checked_mul(4).ok_or(ContractError::Capacity)?)?,
        )?;
        take(&mut remaining, build)?;
        bytes::fits(charge, limits.bytes)?;
        let visits = limits
            .visits
            .checked_sub(remaining)
            .ok_or(ContractError::Capacity)?;
        Ok(Self {
            owner,
            graph,
            _peers: peers,
            action,
            event,
            cut,
            scopes,
            reads,
            charge,
            visits,
        })
    }

    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    /// Preflight and build allowance, excluding source graph construction and
    /// the caller's ClaimState copy/application and subscription publication.
    pub fn visits(&self) -> usize {
        self.visits
    }
    pub fn event(&self) -> Event {
        self.event
    }

    pub fn build(self) -> Result<Transition, ContractError> {
        let source = self.owner.scopes();
        let mut scopes = release::checked_reserve(self.scopes)?;
        let mut children = release::checked_reserve(source.children.len())?;
        for scope in &source.scopes {
            let mut roots = release::checked_reserve(scope.roots.len())?;
            roots.extend_from_slice(&scope.roots);
            scopes.push(Scope {
                id: scope.id,
                roots,
                deadline: scope.deadline,
                registered: scope.registered,
                disposition: scope.disposition,
                last_rebinding: scope.last_rebinding,
            });
        }
        children.extend_from_slice(&source.children);
        match self.action {
            Action::Register { request, position } => {
                let mut roots = release::checked_reserve(request.roots.len())?;
                roots.extend_from_slice(request.roots);
                roots.sort_unstable();
                roots.dedup();
                // The retained source fixes this insertion point; still check
                // capacity/index before using Vec's infallible insertion path.
                if scopes.len() == scopes.capacity() || position > scopes.len() {
                    return Err(ContractError::Capacity);
                }
                scopes.insert(
                    position,
                    Scope {
                        id: request.id,
                        roots,
                        deadline: request.deadline,
                        registered: self.cut.position,
                        disposition: None,
                        last_rebinding: None,
                    },
                );
            }
            Action::Rebind { position, change } => {
                let scope = scopes
                    .get_mut(position)
                    .ok_or(ContractError::InvalidTarget)?;
                for root in &mut scope.roots {
                    *root = match *root {
                        WaitPredicate::Satisfied(id) if id == change.predecessor => {
                            WaitPredicate::Satisfied(change.successor)
                        }
                        WaitPredicate::Terminal(id) if id == change.predecessor => {
                            WaitPredicate::Terminal(change.successor)
                        }
                        WaitPredicate::Released(id) if id == change.predecessor => {
                            WaitPredicate::Released(change.successor)
                        }
                        value => value,
                    };
                }
                scope.roots.sort_unstable();
                scope.roots.dedup();
                scope.last_rebinding = Some(change);
            }
            Action::Release { position } => {
                scopes
                    .get_mut(position)
                    .ok_or(ContractError::InvalidTarget)?
                    .disposition = Some(MonitorDisposition::Released(self.cut));
            }
            Action::Cancel {
                position,
                cancellation,
            } => {
                scopes
                    .get_mut(position)
                    .ok_or(ContractError::InvalidTarget)?
                    .disposition = Some(MonitorDisposition::Cancelled(cancellation));
            }
        }
        let mut reads = release::checked_reserve(self.reads)?;
        reads.extend(self.graph.bindings());
        let transition = Transition {
            expected: self.owner.binding(),
            replacement: Registry {
                owner: source.owner,
                limits: source.limits,
                scopes,
                children,
                released: source.released,
                last_cut: self.cut.position,
            },
            reads,
            event: self.event,
        };
        bytes::fits(transition.construction_charge()?, self.charge)?;
        Ok(transition)
    }
}
