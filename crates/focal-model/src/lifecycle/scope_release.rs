//! Allocation-free ownership-release preflight. The graph and complete borrowed
//! peer set remain held while the owner reserves the quoted construction charge.
use super::*;
use crate::lifecycle::memory as bytes;
use std::mem::size_of;

#[derive(Debug)]
pub struct ReleaseOwnerPlan<'a> {
    owner: &'a ClaimState,
    graph: &'a Snapshot,
    // Retain the exact validated peer borrows until the replacement is built;
    // no repeat traversal or duplicate owned source snapshot is necessary.
    _peers: &'a [&'a ClaimState],
    cut: ClaimCut,
    count: usize,
    charge: usize,
    visits: usize,
}

fn take(remaining: &mut usize, count: usize) -> Result<(), ContractError> {
    *remaining = remaining
        .checked_sub(count)
        .ok_or(ContractError::Capacity)?;
    Ok(())
}
fn buffer<T>(count: usize) -> Result<usize, ContractError> {
    bytes::add(
        bytes::array::<T>(count)?,
        if count == 0 { 0 } else { ALLOCATION },
    )
}
pub(super) fn checked_reserve<T>(count: usize) -> Result<Vec<T>, ContractError> {
    #[cfg(test)]
    let requested = EXCESS.with(|extra| {
        if extra.replace(false) {
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

#[cfg(test)]
std::thread_local! {
    static EXCESS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
#[cfg(test)]
pub(super) fn with_excess_capacity<T>(action: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            EXCESS.with(|extra| extra.set(self.0));
        }
    }
    let _restore = Restore(EXCESS.with(|extra| extra.replace(true)));
    action()
}

impl Registry {
    /// Native owner release requires each individual monitor's disposition to
    /// have been recorded already. This does not implicitly settle a monitor,
    /// cancel a child, or invent respondent testimony.
    ///
    /// `max_bytes` includes the Transition, compact registry buffers, complete
    /// binding-read buffer and allocator metadata. It excludes already-held
    /// sources, graph/peers and the caller's separate ClaimState copy.
    pub fn prepare_release_owner_bounded<'a>(
        owner: &'a ClaimState,
        graph: &'a Snapshot,
        peers: &'a [&'a ClaimState],
        cut: ClaimCut,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<ReleaseOwnerPlan<'a>, ContractError> {
        prepare(owner, graph, peers, cut, max_bytes, max_visits, true)
    }
}

pub(super) fn prepare<'a>(
    owner: &'a ClaimState,
    graph: &'a Snapshot,
    peers: &'a [&'a ClaimState],
    cut: ClaimCut,
    max_bytes: usize,
    max_visits: usize,
    native: bool,
) -> Result<ReleaseOwnerPlan<'a>, ContractError> {
    let mut remaining = max_visits;
    let count = bytes::add(peers.len(), 1)?;
    // check_owner performs a binary owner seek and one complete ordered node
    // comparison. A seek costs at most count probes, including the one-row case.
    take(
        &mut remaining,
        bytes::add(count.checked_mul(2).ok_or(ContractError::Capacity)?, 8)?,
    )?;
    Registry::check_cut(owner, cut)?;
    graph.check_owner(owner, peers)?;
    let registry = owner.scopes();
    if !owner.is_terminal() || registry.released() {
        return Err(ContractError::InvalidTransition);
    }
    let terminal = match owner.terminal_cut().ok_or(ContractError::InvalidCut)? {
        ClaimTerminalCut::Explicit(value) => value.position,
        ClaimTerminalCut::Required(value) => value.sequence(),
        ClaimTerminalCut::Graph(value) => value.sequence(),
    };
    if cut.position < terminal || (native && cut.cause == crate::ContentHash([0; 32])) {
        return Err(ContractError::InvalidCut);
    }
    graph.check_cut(cut.position)?;
    owner.binding().next()?;
    bytes::fits(registry.scopes.len(), registry.limits.scopes)?;
    bytes::fits(registry.children.len(), registry.limits.children)?;
    let mut roots = 0usize;
    let mut charge = bytes::add(size_of::<Transition>(), buffer::<Binding>(count)?)?;
    charge = bytes::add(charge, buffer::<Scope>(registry.scopes.len())?)?;
    charge = bytes::add(charge, buffer::<OwnedChild>(registry.children.len())?)?;
    for scope in &registry.scopes {
        take(&mut remaining, 1)?;
        if native && scope.active() {
            return Err(ContractError::InvalidTransition);
        }
        roots = bytes::add(roots, scope.roots.len())?;
        charge = bytes::add(charge, buffer::<WaitPredicate>(scope.roots.len())?)?;
    }
    bytes::fits(roots, registry.limits.roots)?;
    // Both lists are canonical: graph.check_owner verifies peer ordering, and
    // checked child generation maintains unique ClaimId order. Merge once.
    let mut peers_at = peers.iter();
    let mut previous_child = None;
    for child in &registry.children {
        take(&mut remaining, 1)?;
        if previous_child.is_some_and(|previous| previous >= child.binding.object) {
            return Err(ContractError::InvalidManifest);
        }
        previous_child = Some(child.binding.object);
        let actual = loop {
            take(&mut remaining, 1)?;
            let actual = peers_at.next().ok_or(ContractError::InvalidManifest)?;
            match actual.binding().object.cmp(&child.binding.object) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => break actual,
                std::cmp::Ordering::Greater => return Err(ContractError::InvalidManifest),
            }
        };
        same_content(actual.binding(), child.binding)?;
        if native && actual.binding().revision < child.binding.revision {
            return Err(ContractError::StaleRevision);
        }
        if !actual.scopes().released() {
            return Err(ContractError::InvalidTransition);
        }
        if actual
            .scopes()
            .release_cut()
            .is_some_and(|value| value.position > cut.position)
        {
            return Err(ContractError::InvalidCut);
        }
    }
    // Build copies every child, root and graph binding once; scope rows are
    // traversed once to copy and twice to reconcile actual buffer accounting.
    let build_visits = bytes::add(
        bytes::add(count, registry.children.len())?,
        bytes::add(
            roots,
            registry
                .scopes
                .len()
                .checked_mul(3)
                .ok_or(ContractError::Capacity)?,
        )?,
    )?;
    take(&mut remaining, build_visits)?;
    bytes::fits(charge, max_bytes)?;
    let visits = max_visits
        .checked_sub(remaining)
        .ok_or(ContractError::Capacity)?;
    Ok(ReleaseOwnerPlan {
        owner,
        graph,
        _peers: peers,
        cut,
        count,
        charge,
        visits,
    })
}

impl ReleaseOwnerPlan<'_> {
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    /// Complete preflight and construction traversal ceiling. Allocations and
    /// caller-side graph discovery/ClaimState copying are separate operations.
    pub fn visits(&self) -> usize {
        self.visits
    }
    pub fn build(self) -> Result<Transition, ContractError> {
        let source = self.owner.scopes();
        let mut scopes = checked_reserve(source.scopes.len())?;
        let mut children = checked_reserve(source.children.len())?;
        for scope in &source.scopes {
            let mut roots = checked_reserve(scope.roots.len())?;
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
        let mut reads = checked_reserve(self.count)?;
        reads.extend(self.graph.bindings());
        let transition = Transition {
            expected: self.owner.binding(),
            replacement: Registry {
                owner: source.owner,
                limits: source.limits,
                scopes,
                children,
                released: Some(self.cut),
                last_cut: self.cut.position,
            },
            reads,
            event: Event::OwnerReleased { cut: self.cut },
        };
        bytes::fits(transition.construction_charge()?, self.charge)?;
        Ok(transition)
    }
}
