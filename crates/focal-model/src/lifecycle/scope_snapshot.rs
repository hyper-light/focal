//! Intrinsic restoration of recorded scope state. This is a model DTO version,
//! not the live V1 wire format. The importer authenticates original events,
//! actual peer identities, supersession and predicate/release witnesses; this
//! module never reruns graph discovery, current authority or deadline checks.
use super::*;
use crate::ContentHash;
use crate::lifecycle::memory as bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeSnapshotV1 {
    pub id: MonitorId,
    pub roots: usize,
    pub deadline: Deadline,
    pub registered: SessionSeq,
    pub disposition: Option<MonitorDisposition>,
    pub last_rebinding: Option<Rebinding>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnedChildSnapshotV1 {
    pub binding: Binding,
    pub registered: SessionSeq,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrySnapshotV1 {
    pub owner: Binding,
    pub limits: ScopeLimits,
    pub scopes: usize,
    pub children: usize,
    pub released: Option<ClaimCut>,
    pub last_cut: SessionSeq,
}

/// Repeatable value sources allow a byte adapter to yield decoded scalars
/// without temporary typed arrays. The adapter separately bounds parsing work.
pub trait ScopeSnapshotSource {
    fn fields(&self) -> ScopeSnapshotV1;
    type Roots<'a>: Iterator<Item = Result<WaitPredicate, ContractError>>
    where
        Self: 'a;
    fn roots(&self) -> Self::Roots<'_>;
}
pub trait RegistrySnapshotSource {
    fn fields(&self) -> RegistrySnapshotV1;
    type Scope<'a>: ScopeSnapshotSource
    where
        Self: 'a;
    type Scopes<'a>: Iterator<Item = Result<Self::Scope<'a>, ContractError>>
    where
        Self: 'a;
    type Children<'a>: Iterator<Item = Result<OwnedChildSnapshotV1, ContractError>>
    where
        Self: 'a;
    fn scopes(&self) -> Self::Scopes<'_>;
    fn children(&self) -> Self::Children<'_>;
}
impl ScopeSnapshotSource for &Scope {
    fn fields(&self) -> ScopeSnapshotV1 {
        ScopeSnapshotV1 {
            id: self.id,
            roots: self.roots.len(),
            deadline: self.deadline,
            registered: self.registered,
            disposition: self.disposition,
            last_rebinding: self.last_rebinding,
        }
    }
    type Roots<'a>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, WaitPredicate>>,
        fn(WaitPredicate) -> Result<WaitPredicate, ContractError>,
    >
    where
        Self: 'a;
    fn roots(&self) -> Self::Roots<'_> {
        self.roots.iter().copied().map(Ok)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct RegistrySnapshotView<'a>(&'a Registry);
impl RegistrySnapshotSource for RegistrySnapshotView<'_> {
    fn fields(&self) -> RegistrySnapshotV1 {
        RegistrySnapshotV1 {
            owner: self.0.owner,
            limits: self.0.limits,
            scopes: self.0.scopes.len(),
            children: self.0.children.len(),
            released: self.0.released,
            last_cut: self.0.last_cut,
        }
    }
    type Scope<'a>
        = &'a Scope
    where
        Self: 'a;
    type Scopes<'a>
        = std::iter::Map<
        std::slice::Iter<'a, Scope>,
        fn(&'a Scope) -> Result<&'a Scope, ContractError>,
    >
    where
        Self: 'a;
    type Children<'a>
        = std::iter::Map<
        std::slice::Iter<'a, OwnedChild>,
        fn(&OwnedChild) -> Result<OwnedChildSnapshotV1, ContractError>,
    >
    where
        Self: 'a;
    fn scopes(&self) -> Self::Scopes<'_> {
        self.0.scopes.iter().map(Ok)
    }
    fn children(&self) -> Self::Children<'_> {
        self.0.children.iter().map(|child| {
            Ok(OwnedChildSnapshotV1 {
                binding: child.binding,
                registered: child.registered,
            })
        })
    }
}

/// Holds only borrowed source and scalar quotes. Prepare allocates nothing;
/// build must run while the caller holds the complete construction allowance.
pub struct RegistryHydrationPlan<'a, S: RegistrySnapshotSource + ?Sized> {
    source: &'a S,
    current: Binding,
    created: SessionSeq,
    shape: Shape,
}
#[derive(Clone, Copy)]
struct Shape {
    fields: RegistrySnapshotV1,
    heap: usize,
    allocations: usize,
    visits: usize,
    fingerprint: ContentHash,
}
impl Registry {
    pub fn snapshot_v1(&self) -> RegistrySnapshotView<'_> {
        RegistrySnapshotView(self)
    }
    pub fn prepare_hydration_v1<'a, S: RegistrySnapshotSource + ?Sized>(
        current: Binding,
        created: SessionSeq,
        limits: ScopeLimits,
        source: &'a S,
        max_visits: usize,
    ) -> Result<RegistryHydrationPlan<'a, S>, ContractError> {
        let shape = inspect(current, created, limits, source, max_visits)?;
        Ok(RegistryHydrationPlan {
            source,
            current,
            created,
            shape,
        })
    }
}
impl<S: RegistrySnapshotSource + ?Sized> RegistryHydrationPlan<'_, S> {
    pub(in crate::lifecycle) fn check_context(
        &self,
        current: Binding,
        created: SessionSeq,
        limits: ScopeLimits,
    ) -> Result<(), ContractError> {
        self.current.check(&current)?;
        if self.created != created || self.shape.fields.limits != limits {
            return Err(ContractError::InvalidTarget);
        }
        Ok(())
    }
    pub fn fields(&self) -> RegistrySnapshotV1 {
        self.shape.fields
    }
    pub fn inspection_visits(&self) -> usize {
        self.shape.visits
    }
    pub fn build_visits(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.shape
                .visits
                .checked_mul(2)
                .ok_or(ContractError::Capacity)?,
            self.shape
                .fields
                .scopes
                .checked_mul(2)
                .and_then(|count| count.checked_add(8))
                .ok_or(ContractError::Capacity)?,
        )
    }
    pub fn construction_heap_bytes(&self) -> usize {
        self.shape.heap
    }
    pub fn construction_heap_allocations(&self) -> usize {
        self.shape.allocations
    }
    pub fn construction_charge(&self) -> Result<usize, ContractError> {
        complete::<Registry>(self.shape.heap, self.shape.allocations)
    }
    pub fn build(self, max_bytes: usize, max_visits: usize) -> Result<Registry, ContractError> {
        bytes::fits(self.construction_charge()?, max_bytes)?;
        bytes::fits(self.build_visits()?, max_visits)?;
        let mut work = Work::new(self.shape.visits);
        work.charge(64)?;
        let fields = self.source.fields();
        if fields != self.shape.fields {
            return Err(ContractError::ContentConflict);
        }
        let mut remaining = bytes::add(self.shape.heap, overhead(self.shape.allocations)?)?;
        let mut scopes = reserve::<Scope>(fields.scopes, &mut remaining)?;
        let mut scope_source = self.source.scopes();
        for _ in 0..fields.scopes {
            work.charge(33)?;
            let source = scope_source
                .next()
                .ok_or(ContractError::InvalidManifest)??;
            let row = source.fields();
            // Bound every nested allocation by the remaining quoted total.
            // Changed per-scope counts may fit that total; final intrinsic
            // validation and the prepared fingerprint still reject any drift.
            let mut roots = reserve(row.roots, &mut remaining)?;
            let mut values = source.roots();
            for _ in 0..row.roots {
                work.charge(2)?;
                roots.push(values.next().ok_or(ContractError::InvalidManifest)??);
            }
            work.charge(1)?;
            if values.next().is_some() {
                return Err(ContractError::InvalidManifest);
            }
            scopes.push(Scope {
                id: row.id,
                roots,
                deadline: row.deadline,
                registered: row.registered,
                disposition: row.disposition,
                last_rebinding: row.last_rebinding,
            });
        }
        work.charge(1)?;
        if scope_source.next().is_some() {
            return Err(ContractError::InvalidManifest);
        }
        let mut children = reserve(fields.children, &mut remaining)?;
        let mut child_source = self.source.children();
        for _ in 0..fields.children {
            work.charge(17)?;
            let child = child_source
                .next()
                .ok_or(ContractError::InvalidManifest)??;
            children.push(OwnedChild {
                binding: child.binding,
                registered: child.registered,
            });
        }
        work.charge(1)?;
        if child_source.next().is_some() {
            return Err(ContractError::InvalidManifest);
        }
        let restored = Registry {
            owner: fields.owner,
            limits: fields.limits,
            scopes,
            children,
            released: fields.released,
            last_cut: fields.last_cut,
        };
        let actual = inspect(
            self.current,
            self.created,
            fields.limits,
            &restored.snapshot_v1(),
            self.shape.visits,
        )?;
        if actual.fingerprint != self.shape.fingerprint {
            return Err(ContractError::ContentConflict);
        }
        bytes::fits(
            complete::<Registry>(
                restored.retained_heap_bytes()?,
                restored.heap_allocations()?,
            )?,
            max_bytes,
        )?;
        Ok(restored)
    }
}

fn inspect<S: RegistrySnapshotSource + ?Sized>(
    current: Binding,
    created: SessionSeq,
    limits: ScopeLimits,
    source: &S,
    max_visits: usize,
) -> Result<Shape, ContractError> {
    let mut work = Work::new(max_visits);
    work.charge(64)?;
    let fields = source.fields();
    identity(current, fields.owner)?;
    if current.object.is_zero()
        || current.ledger.tenant.is_zero()
        || current.ledger.session.is_zero()
    {
        return Err(ContractError::InvalidTarget);
    }
    if created.0 == 0 || fields.last_cut.0 != 0 && fields.last_cut < created {
        return Err(ContractError::InvalidCut);
    }
    if fields.limits != limits || fields.scopes > limits.scopes || fields.children > limits.children
    {
        return Err(ContractError::Capacity);
    }
    if let Some(released) = fields.released {
        cut(released, created, fields.last_cut)?;
    }
    let mut hash = Hash::new("focal model scope snapshot plan 1");
    hash.binding(fields.owner);
    hash.count(limits.scopes)?;
    hash.count(limits.roots)?;
    hash.count(limits.children)?;
    hash.count(fields.scopes)?;
    hash.count(fields.children)?;
    hash.optional_cut(fields.released);
    hash.u64(fields.last_cut.0);
    let mut heap = bytes::add(
        bytes::array::<Scope>(fields.scopes)?,
        bytes::array::<OwnedChild>(fields.children)?,
    )?;
    let mut allocations = bytes::add(
        usize::from(fields.scopes != 0),
        usize::from(fields.children != 0),
    )?;
    let mut roots_count = 0_usize;
    let mut latest = SessionSeq(0);
    let mut previous = None;
    let mut scopes = source.scopes();
    for _ in 0..fields.scopes {
        work.charge(33)?;
        let source = scopes.next().ok_or(ContractError::InvalidManifest)??;
        let row = source.fields();
        if row.id.is_zero()
            || previous.is_some_and(|id| id >= row.id)
            || row.roots == 0
            || row.deadline.timer.is_zero()
            || row.deadline.generation == 0
            || row.deadline.at == 0
        {
            return Err(ContractError::InvalidTarget);
        }
        position(row.registered, created, fields.last_cut)?;
        previous = Some(row.id);
        latest = latest.max(row.registered);
        roots_count = bytes::add(roots_count, row.roots)?;
        if roots_count > limits.roots {
            return Err(ContractError::Capacity);
        }
        heap = bytes::add(heap, bytes::array::<WaitPredicate>(row.roots)?)?;
        allocations = bytes::add(allocations, 1)?;
        hash.raw(&row.id.0);
        hash.count(row.roots)?;
        hash.deadline(row.deadline);
        hash.u64(row.registered.0);
        let changed = row
            .last_rebinding
            .map_or(row.registered, |change| change.cut.position);
        if let Some(change) = row.last_rebinding {
            if change.predecessor.is_zero()
                || change.successor.is_zero()
                || change.predecessor == change.successor
            {
                return Err(ContractError::InvalidTarget);
            }
            cut(change.cut, row.registered, fields.last_cut)?;
            latest = latest.max(change.cut.position);
            hash.u8(1);
            hash.raw(&change.predecessor.0);
            hash.raw(&change.successor.0);
            hash.cut(change.cut);
        } else {
            hash.u8(0);
        }
        match row.disposition {
            None => {
                hash.u8(0);
                // The legacy model owner-release helper can retain active
                // monitors. Preserve that recorded distinction; a native
                // importer applies its stronger profile/history constraints.
            }
            Some(MonitorDisposition::Released(released)) => {
                cut(released, changed, fields.last_cut)?;
                latest = latest.max(released.position);
                hash.u8(1);
                hash.cut(released);
            }
            Some(MonitorDisposition::Cancelled(cancelled)) => {
                cut(cancelled.cut, changed, fields.last_cut)?;
                position(cancelled.terminal, created, cancelled.cut.position)?;
                latest = latest.max(cancelled.cut.position);
                hash.u8(2);
                hash.u64(cancelled.terminal.0);
                hash.cut(cancelled.cut);
            }
        }
        let mut roots = source.roots();
        let mut prior_root = None;
        let mut predecessor_retained = false;
        let mut successor_retained = false;
        for _ in 0..row.roots {
            work.charge(2)?;
            let root = roots.next().ok_or(ContractError::InvalidManifest)??;
            if target(root).is_zero() || prior_root.is_some_and(|previous| previous >= root) {
                return Err(ContractError::InvalidTarget);
            }
            prior_root = Some(root);
            if let Some(change) = row.last_rebinding {
                predecessor_retained |= target(root) == change.predecessor;
                successor_retained |= target(root) == change.successor;
            }
            hash.predicate(root);
        }
        work.charge(1)?;
        if roots.next().is_some() {
            return Err(ContractError::InvalidManifest);
        }
        if row.last_rebinding.is_some() && (predecessor_retained || !successor_retained) {
            return Err(ContractError::InvalidTarget);
        }
    }
    work.charge(1)?;
    if scopes.next().is_some() {
        return Err(ContractError::InvalidManifest);
    }
    let mut children = source.children();
    let mut previous = None;
    for _ in 0..fields.children {
        work.charge(17)?;
        let child = children.next().ok_or(ContractError::InvalidManifest)??;
        if child.binding.ledger != current.ledger {
            return Err(ContractError::WrongLedger);
        }
        if child.binding.object.is_zero()
            || child.binding.object == current.object
            || previous.is_some_and(|old| old >= child.binding.object)
        {
            return Err(ContractError::InvalidTarget);
        }
        position(child.registered, created, fields.last_cut)?;
        previous = Some(child.binding.object);
        latest = latest.max(child.registered);
        hash.binding(child.binding);
        hash.u64(child.registered.0);
    }
    work.charge(1)?;
    if children.next().is_some() {
        return Err(ContractError::InvalidManifest);
    }
    if let Some(released) = fields.released {
        latest = latest.max(released.position);
    }
    if latest != fields.last_cut {
        return Err(ContractError::InvalidCut);
    }
    Ok(Shape {
        fields,
        heap,
        allocations,
        visits: work.used(),
        fingerprint: hash.finish(),
    })
}

pub(in crate::lifecycle) fn position(
    value: SessionSeq,
    earliest: SessionSeq,
    latest: SessionSeq,
) -> Result<(), ContractError> {
    if value.0 == 0 || value < earliest || value > latest {
        Err(ContractError::InvalidCut)
    } else {
        Ok(())
    }
}
pub(in crate::lifecycle) fn cut(
    value: ClaimCut,
    earliest: SessionSeq,
    latest: SessionSeq,
) -> Result<(), ContractError> {
    position(value.position, earliest, latest)
}
pub(in crate::lifecycle) fn identity(
    current: Binding,
    original: Binding,
) -> Result<(), ContractError> {
    same_content(current, original)?;
    if current.revision < original.revision {
        Err(ContractError::StaleRevision)
    } else {
        Ok(())
    }
}
pub(in crate::lifecycle) fn overhead(count: usize) -> Result<usize, ContractError> {
    count.checked_mul(ALLOCATION).ok_or(ContractError::Capacity)
}
pub(in crate::lifecycle) fn complete<T>(
    heap: usize,
    allocations: usize,
) -> Result<usize, ContractError> {
    bytes::add(bytes::total::<T>(heap)?, overhead(allocations)?)
}
pub(in crate::lifecycle) fn reserve<T>(
    count: usize,
    remaining: &mut usize,
) -> Result<Vec<T>, ContractError> {
    let charge = bytes::add(
        bytes::array::<T>(count)?,
        if count == 0 { 0 } else { ALLOCATION },
    )?;
    let next = remaining
        .checked_sub(charge)
        .ok_or(ContractError::Capacity)?;
    let values = bytes::reserve(count)?;
    if values.capacity() != count {
        return Err(ContractError::Capacity);
    }
    *remaining = next;
    Ok(values)
}
pub(in crate::lifecycle) struct Work {
    limit: usize,
    left: usize,
}
impl Work {
    pub(in crate::lifecycle) fn new(limit: usize) -> Self {
        Self { limit, left: limit }
    }
    pub(in crate::lifecycle) fn charge(&mut self, count: usize) -> Result<(), ContractError> {
        self.left = self
            .left
            .checked_sub(count)
            .ok_or(ContractError::Capacity)?;
        Ok(())
    }
    pub(in crate::lifecycle) fn used(&self) -> usize {
        self.limit.saturating_sub(self.left)
    }
}
pub(in crate::lifecycle) struct Hash(blake3::Hasher);
impl Hash {
    pub(in crate::lifecycle) fn new(domain: &str) -> Self {
        Self(blake3::Hasher::new_derive_key(domain))
    }
    pub(in crate::lifecycle) fn raw(&mut self, value: &[u8]) {
        self.0.update(value);
    }
    pub(in crate::lifecycle) fn u8(&mut self, value: u8) {
        self.raw(&[value]);
    }
    pub(in crate::lifecycle) fn u64(&mut self, value: u64) {
        self.raw(&value.to_be_bytes());
    }
    pub(in crate::lifecycle) fn count(&mut self, value: usize) -> Result<(), ContractError> {
        self.u64(u64::try_from(value).map_err(|_| ContractError::Capacity)?);
        Ok(())
    }
    pub(in crate::lifecycle) fn binding(&mut self, value: Binding) {
        self.raw(&value.ledger.tenant.0);
        self.raw(&value.ledger.session.0);
        self.raw(&value.object.0);
        self.raw(&value.content.0);
        self.u64(value.revision.0);
    }
    pub(in crate::lifecycle) fn cut(&mut self, value: ClaimCut) {
        self.u64(value.position.0);
        self.raw(&value.cause.0);
    }
    pub(in crate::lifecycle) fn optional_cut(&mut self, value: Option<ClaimCut>) {
        if let Some(value) = value {
            self.u8(1);
            self.cut(value);
        } else {
            self.u8(0);
        }
    }
    pub(in crate::lifecycle) fn deadline(&mut self, value: Deadline) {
        self.raw(&value.timer.0);
        self.u64(value.generation);
        self.u64(value.at);
    }
    fn predicate(&mut self, value: WaitPredicate) {
        let (tag, id) = match value {
            WaitPredicate::Satisfied(id) => (0, id),
            WaitPredicate::Terminal(id) => (1, id),
            WaitPredicate::Released(id) => (2, id),
        };
        self.u8(tag);
        self.raw(&id.0);
    }
    pub(in crate::lifecycle) fn finish(self) -> ContentHash {
        ContentHash(*self.0.finalize().as_bytes())
    }
}
