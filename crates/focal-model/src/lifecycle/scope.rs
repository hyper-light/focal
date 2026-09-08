//! Bounded, owner-held blocking scopes and ownership release. These plans use a
//! complete graph snapshot and pin every read until publication. Registration
//! does not settle roots: the graph must be recaptured after installing new
//! edges, since a new edge can create a cycle. Durable suffix subscriptions and
//! transition histories belong to the publishing owner.
use super::claim::{ClaimCut, ClaimState, ClaimTerminalCut};
#[path = "scope_deadline.rs"]
mod deadline;
#[path = "scope_memory.rs"]
mod memory;
#[path = "scope_monitor.rs"]
mod monitor;
#[path = "scope_release.rs"]
mod release;
#[path = "scope_snapshot.rs"]
pub(in crate::lifecycle) mod snapshot;
use super::graph::Snapshot;
use super::succession::CorrectionKind;
use super::{Binding, ContractError, Principal};
use crate::{
    Cause, ClaimId, ClaimStatus, Deadline, MonitorId, ObjectRef, ReceiptFence, SessionSeq,
    WaitPredicate,
};
pub use deadline::{
    MonitorDeadlineDecision, MonitorDeadlinePlan, MonitorDeadlineRequest, MonitorExpiry,
};
pub use monitor::{BuildLimits, MonitorPlan, RebindRequest};
pub use release::ReleaseOwnerPlan;
pub use snapshot::{
    OwnedChildSnapshotV1, RegistryHydrationPlan, RegistrySnapshotSource, RegistrySnapshotV1,
    RegistrySnapshotView, ScopeSnapshotSource, ScopeSnapshotV1,
};

/// Bounds apply to retained rows, including released scopes, and the total
/// roots across all rows. Zero disables the corresponding facility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeLimits {
    pub scopes: usize,
    pub roots: usize,
    pub children: usize,
}
pub type Limits = ScopeLimits;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rebinding {
    pub predecessor: ClaimId,
    pub successor: ClaimId,
    pub cut: ClaimCut,
}

/// Cancellation disposes of a terminal owner's wait without claiming its
/// predicates settled. The original terminal fact remains on the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonitorCancellation {
    pub terminal: SessionSeq,
    pub cut: ClaimCut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorDisposition {
    Released(ClaimCut),
    Cancelled(MonitorCancellation),
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Clone))]
pub struct Scope {
    id: MonitorId,
    roots: Vec<WaitPredicate>,
    deadline: Deadline,
    registered: SessionSeq,
    disposition: Option<MonitorDisposition>,
    last_rebinding: Option<Rebinding>,
}
impl Scope {
    pub fn id(&self) -> MonitorId {
        self.id
    }
    pub fn roots(&self) -> &[WaitPredicate] {
        &self.roots
    }
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }
    pub fn registered(&self) -> SessionSeq {
        self.registered
    }
    pub fn released(&self) -> Option<SessionSeq> {
        self.release_cut().map(|cut| cut.position)
    }
    pub fn release_cut(&self) -> Option<ClaimCut> {
        match self.disposition {
            Some(MonitorDisposition::Released(cut)) => Some(cut),
            _ => None,
        }
    }
    pub fn cancellation(&self) -> Option<MonitorCancellation> {
        match self.disposition {
            Some(MonitorDisposition::Cancelled(cancellation)) => Some(cancellation),
            _ => None,
        }
    }
    pub fn disposition(&self) -> Option<MonitorDisposition> {
        self.disposition
    }
    pub fn active(&self) -> bool {
        self.disposition.is_none()
    }
    pub fn last_rebinding(&self) -> Option<Rebinding> {
        self.last_rebinding
    }
}

/// Immutable child identity, pinned by atomic child generation. A later child
/// revision may change, while its ledger, object and content cannot change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnedChild {
    binding: Binding,
    registered: SessionSeq,
}
impl OwnedChild {
    pub fn binding(&self) -> Binding {
        self.binding
    }
    pub fn id(&self) -> ClaimId {
        ClaimId(self.binding.object.0)
    }
    pub fn registered(&self) -> SessionSeq {
        self.registered
    }
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Clone))]
pub struct Registry {
    owner: Binding,
    limits: ScopeLimits,
    scopes: Vec<Scope>,
    children: Vec<OwnedChild>,
    released: Option<ClaimCut>,
    last_cut: SessionSeq,
}

/// Authenticated intent at the current owner revision. `now` is the publishing
/// owner's effective logical time, not a participant-selected wall clock.
#[derive(Debug, Clone, Copy)]
pub struct Authority {
    pub principal: Principal,
    pub expected: Binding,
    pub receipt: Option<ReceiptFence>,
    pub cut: ClaimCut,
    pub now: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Registration<'a> {
    pub id: MonitorId,
    pub roots: &'a [WaitPredicate],
    pub deadline: Deadline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Registered {
        id: MonitorId,
        cut: ClaimCut,
    },
    Rebound {
        id: MonitorId,
        change: Rebinding,
    },
    MonitorReleased {
        id: MonitorId,
        cut: ClaimCut,
    },
    MonitorCancelled {
        id: MonitorId,
        cancellation: MonitorCancellation,
    },
    ChildRegistered {
        child: Binding,
        cut: ClaimCut,
    },
    OwnerReleased {
        cut: ClaimCut,
    },
}

/// Construction is private. All fallible work and allocation happens before
/// ClaimState advances its revision and installs this replacement registry.
#[derive(Debug)]
pub struct Transition {
    expected: Binding,
    replacement: Registry,
    reads: Vec<Binding>,
    event: Event,
}
const ALLOCATION: usize = 4 * std::mem::size_of::<usize>();

impl Transition {
    pub fn event(&self) -> Event {
        self.event
    }
    /// Actual replacement/read-buffer charge, including allocator metadata.
    /// The source registry and graph remain separately retained by the owner.
    pub fn construction_charge(&self) -> Result<usize, ContractError> {
        self.allocation_charge()
    }
    /// Actual provisional replacement plus its complete read buffer, while the
    /// original registry is still retained. Native preparation checks this peak
    /// against its reserved allowance before installing the replacement.
    pub(super) fn allocation_charge(&self) -> Result<usize, ContractError> {
        use crate::lifecycle::memory as bytes;
        let allocations = bytes::add(
            self.replacement.heap_allocations()?,
            usize::from(self.reads.capacity() != 0),
        )?;
        let mut charge = bytes::add(
            std::mem::size_of::<Self>(),
            self.replacement.retained_heap_bytes()?,
        )?;
        charge = bytes::add(charge, bytes::array::<Binding>(self.reads.capacity())?)?;
        bytes::add(
            charge,
            allocations
                .checked_mul(ALLOCATION)
                .ok_or(ContractError::Capacity)?,
        )
    }
    #[cfg(test)]
    pub(super) fn test_extra_read_capacity(&mut self, extra: usize) -> Result<(), ContractError> {
        if extra != 0 {
            let capacity = self
                .reads
                .capacity()
                .checked_add(extra)
                .ok_or(ContractError::Capacity)?;
            let additional = capacity
                .checked_sub(self.reads.len())
                .ok_or(ContractError::Capacity)?;
            self.reads
                .try_reserve_exact(additional)
                .map_err(|_| ContractError::Capacity)?;
        }
        Ok(())
    }
    pub(super) fn check(
        &self,
        owner: &ClaimState,
        peers: &[&ClaimState],
    ) -> Result<(), ContractError> {
        owner.binding().check(&self.expected)?;
        if peers.len().checked_add(1) != Some(self.reads.len()) {
            return Err(ContractError::InvalidManifest);
        }
        let mut peers = peers.iter();
        for binding in &self.reads {
            if binding.object == owner.binding().object {
                binding.check(&owner.binding())?;
            } else {
                binding.check(
                    &peers
                        .next()
                        .ok_or(ContractError::InvalidManifest)?
                        .binding(),
                )?;
            }
        }
        Ok(())
    }
    pub(super) fn install(self, registry: &mut Registry) {
        *registry = self.replacement;
    }
}

fn reserve<T>(size: usize) -> Result<Vec<T>, ContractError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(size)
        .map_err(|_| ContractError::Capacity)?;
    Ok(values)
}
fn target(root: WaitPredicate) -> ClaimId {
    match root {
        WaitPredicate::Satisfied(id)
        | WaitPredicate::Terminal(id)
        | WaitPredicate::Released(id) => id,
    }
}
fn same_content(actual: Binding, expected: Binding) -> Result<(), ContractError> {
    Binding {
        revision: expected.revision,
        ..actual
    }
    .check(&expected)
}

impl Registry {
    pub fn new(owner: Binding, limits: ScopeLimits) -> Result<Self, ContractError> {
        if owner.object.is_zero() || owner.ledger.tenant.is_zero() || owner.ledger.session.is_zero()
        {
            return Err(ContractError::InvalidTarget);
        }
        Ok(Self {
            owner,
            limits,
            scopes: Vec::new(),
            children: Vec::new(),
            released: None,
            last_cut: SessionSeq(0),
        })
    }
    pub fn iter(&self) -> impl Iterator<Item = &Scope> {
        self.scopes.iter()
    }
    pub fn monitor(&self, id: MonitorId) -> Option<&Scope> {
        self.scopes
            .binary_search_by_key(&id, |scope| scope.id)
            .ok()
            .and_then(|position| self.scopes.get(position))
    }
    pub fn children(&self) -> &[OwnedChild] {
        &self.children
    }
    pub fn released(&self) -> bool {
        self.released.is_some()
    }
    pub fn release_cut(&self) -> Option<ClaimCut> {
        self.released
    }
    pub fn limits(&self) -> ScopeLimits {
        self.limits
    }

    fn check_cut(owner: &ClaimState, cut: ClaimCut) -> Result<(), ContractError> {
        let registry = owner.scopes();
        same_content(owner.binding(), registry.owner)?;
        if cut.position.0 == 0 || cut.position < owner.created() || cut.position < registry.last_cut
        {
            return Err(ContractError::InvalidCut);
        }
        Ok(())
    }
    fn authorize(owner: &ClaimState, authority: Authority) -> Result<(), ContractError> {
        owner.binding().check(&authority.expected)?;
        authority.principal.require_actor(owner.issuer())?;
        if authority.receipt != owner.receipt().map(|receipt| receipt.fence) {
            return Err(ContractError::StaleReceipt);
        }
        if owner.status().is_terminal() || owner.scopes().released() {
            return Err(ContractError::InvalidTransition);
        }
        Self::check_cut(owner, authority.cut)
    }
    fn roots_count(&self) -> Result<usize, ContractError> {
        self.scopes.iter().try_fold(0usize, |count, row| {
            count
                .checked_add(row.roots.len())
                .ok_or(ContractError::Capacity)
        })
    }
    fn copy_with_capacity(&self, scopes: usize, children: usize) -> Result<Self, ContractError> {
        if scopes > self.limits.scopes
            || children > self.limits.children
            || self.roots_count()? > self.limits.roots
        {
            return Err(ContractError::Capacity);
        }
        let mut copied = Self {
            owner: self.owner,
            limits: self.limits,
            scopes: reserve(scopes)?,
            children: reserve(children)?,
            released: self.released,
            last_cut: self.last_cut,
        };
        for row in &self.scopes {
            let mut roots = reserve(row.roots.len())?;
            roots.extend_from_slice(&row.roots);
            copied.scopes.push(Scope {
                id: row.id,
                roots,
                deadline: row.deadline,
                registered: row.registered,
                disposition: row.disposition,
                last_rebinding: row.last_rebinding,
            });
        }
        copied.children.extend_from_slice(&self.children);
        Ok(copied)
    }
    fn plan(
        owner: &ClaimState,
        graph: &Snapshot,
        mut replacement: Self,
        cut: ClaimCut,
        event: Event,
    ) -> Result<Transition, ContractError> {
        graph
            .binding(ClaimId(owner.binding().object.0))?
            .check(&owner.binding())?;
        owner.binding().next()?;
        let count = graph.bindings().count();
        graph.check_cut(cut.position)?;
        let mut reads = reserve(count)?;
        reads.extend(graph.bindings());
        replacement.last_cut = cut.position;
        Ok(Transition {
            expected: owner.binding(),
            replacement,
            reads,
            event,
        })
    }

    pub fn prepare_register(
        owner: &ClaimState,
        authority: Authority,
        request: Registration<'_>,
        graph: &Snapshot,
    ) -> Result<Transition, ContractError> {
        Self::authorize(owner, authority)?;
        let registry = owner.scopes();
        if request.id.is_zero()
            || request.deadline.timer.is_zero()
            || request.deadline.generation == 0
            || request.deadline.at <= authority.now
            || request.roots.is_empty()
        {
            return Err(ContractError::InvalidTarget);
        }
        if registry
            .scopes
            .binary_search_by_key(&request.id, |row| row.id)
            .is_ok()
        {
            return Err(ContractError::InvalidTarget);
        }
        let count = registry
            .scopes
            .len()
            .checked_add(1)
            .ok_or(ContractError::Capacity)?;
        if count > registry.limits.scopes
            || registry
                .roots_count()?
                .checked_add(request.roots.len())
                .ok_or(ContractError::Capacity)?
                > registry.limits.roots
        {
            return Err(ContractError::Capacity);
        }
        for root in request.roots {
            graph.binding(target(*root))?;
        }
        let mut roots = reserve(request.roots.len())?;
        roots.extend_from_slice(request.roots);
        roots.sort_unstable();
        roots.dedup();
        let mut replacement = registry.copy_with_capacity(count, registry.children.len())?;
        replacement.scopes.push(Scope {
            id: request.id,
            roots,
            deadline: request.deadline,
            registered: authority.cut.position,
            disposition: None,
            last_rebinding: None,
        });
        replacement.scopes.sort_unstable_by_key(|row| row.id);
        Self::plan(
            owner,
            graph,
            replacement,
            authority.cut,
            Event::Registered {
                id: request.id,
                cut: authority.cut,
            },
        )
    }

    pub fn prepare_rebind(
        owner: &ClaimState,
        authority: Authority,
        id: MonitorId,
        predecessor: &ClaimState,
        successor: &ClaimState,
        graph: &Snapshot,
    ) -> Result<Transition, ContractError> {
        Self::authorize(owner, authority)?;
        let before = ClaimId(predecessor.binding().object.0);
        let after = ClaimId(successor.binding().object.0);
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
        if !successor.lineage().corrections().iter().any(|row| {
            row.kind == CorrectionKind::Supersedes
                && row.predecessor == ObjectRef::claim(owner.binding().ledger, before)
        }) {
            return Err(ContractError::InvalidTarget);
        }
        let registry = owner.scopes();
        let index = registry
            .scopes
            .binary_search_by_key(&id, |row| row.id)
            .map_err(|_| ContractError::InvalidTarget)?;
        let row = registry
            .scopes
            .get(index)
            .ok_or(ContractError::InvalidTarget)?;
        if !row.active() || !row.roots.iter().any(|root| target(*root) == before) {
            return Err(ContractError::InvalidTransition);
        }
        if successor.created() > authority.cut.position {
            return Err(ContractError::InvalidCut);
        }
        let mut replacement =
            registry.copy_with_capacity(registry.scopes.len(), registry.children.len())?;
        let row = replacement
            .scopes
            .get_mut(index)
            .ok_or(ContractError::InvalidTarget)?;
        for root in &mut row.roots {
            *root = match *root {
                WaitPredicate::Satisfied(value) if value == before => {
                    WaitPredicate::Satisfied(after)
                }
                WaitPredicate::Terminal(value) if value == before => WaitPredicate::Terminal(after),
                WaitPredicate::Released(value) if value == before => WaitPredicate::Released(after),
                value => value,
            };
        }
        row.roots.sort_unstable();
        row.roots.dedup();
        let change = Rebinding {
            predecessor: before,
            successor: after,
            cut: authority.cut,
        };
        row.last_rebinding = Some(change);
        Self::plan(
            owner,
            graph,
            replacement,
            authority.cut,
            Event::Rebound { id, change },
        )
    }

    pub fn prepare_release_monitor(
        owner: &ClaimState,
        id: MonitorId,
        graph: &Snapshot,
        cut: ClaimCut,
    ) -> Result<Transition, ContractError> {
        Self::check_cut(owner, cut)?;
        let registry = owner.scopes();
        let index = registry
            .scopes
            .binary_search_by_key(&id, |row| row.id)
            .map_err(|_| ContractError::InvalidTarget)?;
        let row = registry
            .scopes
            .get(index)
            .ok_or(ContractError::InvalidTarget)?;
        if !row.active() {
            return Err(ContractError::InvalidTransition);
        }
        for root in &row.roots {
            if !graph.wait_settled(*root)? {
                return Err(ContractError::InvalidTransition);
            }
        }
        let mut replacement =
            registry.copy_with_capacity(registry.scopes.len(), registry.children.len())?;
        replacement
            .scopes
            .get_mut(index)
            .ok_or(ContractError::InvalidTarget)?
            .disposition = Some(MonitorDisposition::Released(cut));
        Self::plan(
            owner,
            graph,
            replacement,
            cut,
            Event::MonitorReleased { id, cut },
        )
    }

    pub fn prepare_release_owner(
        owner: &ClaimState,
        graph: &Snapshot,
        peers: &[&ClaimState],
        cut: ClaimCut,
    ) -> Result<Transition, ContractError> {
        release::prepare(owner, graph, peers, cut, usize::MAX, usize::MAX, false)?.build()
    }

    /// Only atomic child generation can mint this token. The child is returned
    /// after installation, so an owned claim cannot escape its parent's list.
    pub(super) fn prepare_child(
        owner: &ClaimState,
        child: &ClaimState,
        expected: &Binding,
        receipt: Option<ReceiptFence>,
        cut: ClaimCut,
    ) -> Result<Transition, ContractError> {
        owner.binding().check(expected)?;
        Self::check_cut(owner, cut)?;
        if owner.receipt().map(|value| value.fence) != receipt {
            return Err(ContractError::StaleReceipt);
        }
        if owner.status().is_terminal()
            || owner.scopes().released()
            || child.status() != ClaimStatus::Generated
        {
            return Err(ContractError::InvalidTransition);
        }
        if child.binding().ledger != owner.binding().ledger {
            return Err(ContractError::WrongLedger);
        }
        if child.created() != cut.position
            || child.binding().object == owner.binding().object
            || child.lineage().cause() != &Cause::Claim(ClaimId(owner.binding().object.0))
        {
            return Err(ContractError::InvalidTarget);
        }
        let registry = owner.scopes();
        if registry
            .children
            .binary_search_by_key(&child.binding().object, |row| row.binding.object)
            .is_ok()
        {
            return Err(ContractError::InvalidTarget);
        }
        let count = registry
            .children
            .len()
            .checked_add(1)
            .ok_or(ContractError::Capacity)?;
        let mut replacement = registry.copy_with_capacity(registry.scopes.len(), count)?;
        replacement.children.push(OwnedChild {
            binding: child.binding(),
            registered: cut.position,
        });
        replacement
            .children
            .sort_unstable_by_key(|row| row.binding.object);
        replacement.last_cut = cut.position;
        owner.binding().next()?;
        let mut reads = reserve(2)?;
        reads.push(owner.binding());
        reads.push(child.binding());
        reads.sort_unstable_by_key(|binding| binding.object);
        Ok(Transition {
            expected: owner.binding(),
            replacement,
            reads,
            event: Event::ChildRegistered {
                child: child.binding(),
                cut,
            },
        })
    }
}

#[cfg(test)]
#[path = "scope_tests.rs"]
mod tests;
