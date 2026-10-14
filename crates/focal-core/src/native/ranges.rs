//! Several stores over key spans of the storage order (25 §4).
//!
//! A native state's rows live in a *range group*: stores in key order, each
//! holding one span of the layout order, together holding every key exactly
//! once at one prefix. Every publication advances every member by one
//! fragment (an empty fragment for a member the write does not touch), so
//! the group has one prefix and a read pins every member at it. The members
//! share one owner identity (what a write envelope checks), one lease clock,
//! one configuration, budget and partition classifier; a split shares every
//! page but the one holding the boundary and a merge shares every page.
//! Member identities are durable (they travel in the checkpoint layout and
//! name ranges to the directory); the producer identity, what records carry,
//! is the group's and is set by whoever opens it.

use super::layout::PackedKey;
use super::prepare::{self, array};
use super::{ContractError, Key, NativeError, Row};
use focal_memory::{
    Allocation, BudgetKind, BudgetLane, Change, Entry, FrozenRange, MemoryBudget, MemoryError,
    PreparedRange, RangeConfig, RangeId, RangePreparationPlan, RangeStats, RangeStore,
    RangeWriteEnvelope, RangeWriteLimits, SnapshotLease, SparseBase, SparseRange,
};

#[cfg(test)]
#[path = "ranges_tests.rs"]
mod tests;

/// The most members a layout may name in a checkpoint frame, whatever the
/// owner's configured bound: parsing a layout is bounded by this alone.
pub const MAX_LAYOUT_MEMBERS: usize = 1024;

/// The affinity a key belongs to (25 §3): the first component of the
/// storage order, so a boundary at an affinity never divides an object's
/// rows. A claim's affinity is its identity's bytes.
pub type Affinity = [u8; 16];

/// A row as a read sees it: its key whole, its row where its member holds it,
/// and the heap the row takes. Members hold keys packed (doc 31 M3); a read
/// gives each back as the key it is.
#[derive(Clone, Copy)]
pub(super) struct RowEntry<'a> {
    pub(super) key: Key,
    pub(super) value: &'a Row,
    pub(super) heap_bytes: usize,
}
impl<'a> RowEntry<'a> {
    pub(super) fn of(entry: &'a Entry<PackedKey, Row>) -> Self {
        Self {
            key: entry.key.unpack(),
            value: &entry.value,
            heap_bytes: entry.heap_bytes,
        }
    }
}
/// A pending change as a read sees it: a row put at a key, or a key deleted.
#[derive(Clone, Copy)]
pub(super) enum RowChange<'a> {
    Put(RowEntry<'a>),
    Delete(Key),
}
impl<'a> RowChange<'a> {
    pub(super) fn of(change: &'a Change<PackedKey, Row>) -> Self {
        match change {
            Change::Put(entry) => Self::Put(RowEntry::of(entry)),
            Change::Delete(key) => Self::Delete(key.unpack()),
        }
    }
    pub(super) fn key(&self) -> Key {
        match self {
            Self::Put(entry) => entry.key,
            Self::Delete(key) => *key,
        }
    }
}
/// A change as the checks over a candidate's changes read it, whichever form holds it: a
/// change a caller built, or one a candidate holds.
pub(super) trait ChangeRead {
    /// The key it changes.
    fn changed(&self) -> Key;
    /// The row it puts, `None` for a deletion.
    fn row(&self) -> Option<&Row>;
}
impl ChangeRead for &Change<Key, Row> {
    fn changed(&self) -> Key {
        *self.key()
    }
    fn row(&self) -> Option<&Row> {
        match self {
            Change::Put(entry) => Some(&entry.value),
            Change::Delete(_) => None,
        }
    }
}
impl ChangeRead for RowChange<'_> {
    fn changed(&self) -> Key {
        self.key()
    }
    fn row(&self) -> Option<&Row> {
        match self {
            Self::Put(entry) => Some(entry.value),
            Self::Delete(_) => None,
        }
    }
}
/// A store's rows read by key, its keys packed underneath: what the checks over a store being
/// restored read.
pub(super) trait KeyedRows {
    fn row(&self, key: &Key) -> Option<&Row>;
    fn rows_from<'a>(
        &'a self,
        key: &Key,
        exclusive: bool,
    ) -> impl Iterator<Item = RowEntry<'a>> + use<'a, Self>;
    fn rows(&self) -> impl Iterator<Item = RowEntry<'_>>;
}
impl<'v> KeyedRows for focal_memory::RangeHydrationView<'v, PackedKey, Row> {
    fn row(&self, key: &Key) -> Option<&Row> {
        self.get(&PackedKey::probe(key))
    }
    fn rows_from<'a>(
        &'a self,
        key: &Key,
        exclusive: bool,
    ) -> impl Iterator<Item = RowEntry<'a>> + use<'a, 'v> {
        self.entries_from(&PackedKey::probe(key), exclusive)
            .map(RowEntry::of)
    }
    fn rows(&self) -> impl Iterator<Item = RowEntry<'_>> {
        self.entries().map(RowEntry::of)
    }
}
/// `changes` with every key packed, each wide key's heap charged to the row it
/// keys (a delete's key is its caller's, gone with the change).
fn pack_changes(
    changes: Vec<Change<Key, Row>>,
) -> Result<Vec<Change<PackedKey, Row>>, MemoryError> {
    let mut packed = Vec::new();
    packed
        .try_reserve_exact(changes.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    for change in changes {
        packed.push(match change {
            Change::Put(entry) => {
                let key = PackedKey::pack(&entry.key)?;
                let heap_bytes = entry
                    .heap_bytes
                    .checked_add(key.heap_bytes())
                    .ok_or(MemoryError::AllocationFailed)?;
                Change::Put(Entry {
                    key,
                    value: entry.value,
                    heap_bytes,
                })
            }
            Change::Delete(key) => Change::Delete(PackedKey::pack(&key)?),
        });
    }
    Ok(packed)
}

/// One member's boundary: its identity and the least affinity it holds,
/// `None` for the first member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RangeBoundary {
    pub(super) id: RangeId,
    pub(super) start: Option<Affinity>,
}

/// Members in key order. Immutable; a group changes its layout through
/// [`NativeRanges::split`] and [`NativeRanges::merge`].
#[derive(Debug)]
pub struct RangeLayout {
    /// Advances by one per applied change; zero for the initial layout. A
    /// committed layout record names the epoch it applies to.
    epoch: u64,
    members: Vec<RangeBoundary>,
    _allocation: Allocation,
}

impl PartialEq for RangeLayout {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.members == other.members
    }
}
impl Eq for RangeLayout {}

impl RangeLayout {
    /// One member holding every key.
    pub(super) fn single(id: RangeId, budget: &MemoryBudget) -> Result<Self, MemoryError> {
        let mut members = Vec::new();
        let allocation = reserve::<RangeBoundary>(budget, 1, BudgetLane::Ordinary)?;
        members
            .try_reserve_exact(1)
            .map_err(|_| MemoryError::AllocationFailed)?;
        members.push(RangeBoundary { id, start: None });
        Ok(Self {
            epoch: 0,
            members,
            _allocation: allocation,
        })
    }
    /// A layout from its boundaries at `epoch`, checked: one to `max`
    /// members, the first from the least key, every other start strictly
    /// above the previous, distinct identities.
    pub(super) fn new(
        epoch: u64,
        members: Vec<RangeBoundary>,
        allocation: Allocation,
        max: usize,
    ) -> Result<Self, NativeError> {
        let layout = Self {
            epoch,
            members,
            _allocation: allocation,
        };
        layout.validate(max)?;
        Ok(layout)
    }
    pub(super) fn members(&self) -> &[RangeBoundary] {
        &self.members
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Whether a split at `at` naming `id` applies to this layout: `at` is
    /// not a member's start, `id` is new and the layout is below `max`.
    pub fn check_split(&self, at: Affinity, id: RangeId, max: usize) -> Result<(), NativeError> {
        let index = route(&self.members, |member| member.start, &at);
        let boundary = self.members.get(index).ok_or(MemoryError::WrongRange)?;
        if boundary.start == Some(at) || self.ids().any(|existing| existing == id) {
            return Err(ContractError::InvalidManifest.into());
        }
        if self.members.len().saturating_add(1) > max.min(MAX_LAYOUT_MEMBERS) {
            return Err(NativeError::Capacity("range layout members"));
        }
        Ok(())
    }
    /// Give member `index` the durable identity `id`, which no other member
    /// holds. Identities name members to records, checkpoints and the
    /// directory; the stores keep their own process-local identities.
    pub(super) fn rename(&mut self, index: usize, id: RangeId) -> Result<(), NativeError> {
        if self.ids().any(|existing| existing == id) {
            return Err(ContractError::InvalidManifest.into());
        }
        let member = self
            .members
            .get_mut(index)
            .ok_or(ContractError::InvalidManifest)?;
        member.id = id;
        Ok(())
    }
    /// The index of member `left` when it has a next member to join.
    pub fn check_merge(&self, left: RangeId) -> Result<usize, NativeError> {
        let index = self
            .members
            .iter()
            .position(|member| member.id == left)
            .ok_or(ContractError::InvalidManifest)?;
        if index.saturating_add(1) >= self.members.len() {
            return Err(ContractError::InvalidManifest.into());
        }
        Ok(index)
    }
    pub fn len(&self) -> usize {
        self.members.len()
    }
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
    pub fn ids(&self) -> impl Iterator<Item = RangeId> + '_ {
        self.members.iter().map(|member| member.id)
    }
    /// Each member's identity and the affinity it starts at.
    pub fn boundaries(&self) -> impl Iterator<Item = (RangeId, Option<Affinity>)> + '_ {
        self.members.iter().map(|member| (member.id, member.start))
    }
    pub(super) fn validate(&self, max: usize) -> Result<(), NativeError> {
        if max == 0 || self.members.is_empty() {
            return Err(NativeError::Capacity("range layout members"));
        }
        if self.members.len() > max.min(MAX_LAYOUT_MEMBERS) {
            return Err(NativeError::Capacity("range layout members"));
        }
        let mut previous: Option<Affinity> = None;
        for (index, member) in self.members.iter().enumerate() {
            match (index, member.start) {
                (0, None) => {}
                (0, Some(_)) | (_, None) => return Err(ContractError::InvalidManifest.into()),
                (_, Some(start)) => {
                    if previous.is_some_and(|last| start <= last) {
                        return Err(ContractError::InvalidManifest.into());
                    }
                    previous = Some(start);
                }
            }
            if self
                .members
                .get(..index)
                .is_some_and(|earlier| earlier.iter().any(|other| other.id == member.id))
            {
                return Err(ContractError::InvalidManifest.into());
            }
        }
        Ok(())
    }
    /// The member holding `key`: the last whose start is at or below the
    /// key's affinity.
    fn route_packed(&self, key: &PackedKey) -> usize {
        self.route_affinity(&key.affinity())
    }
    /// The member holding `affinity`.
    pub fn route_affinity(&self, affinity: &Affinity) -> usize {
        route(&self.members, |member| member.start, affinity)
    }
    /// The identity of the member at `index`.
    pub fn member_id(&self, index: usize) -> Option<RangeId> {
        self.members.get(index).map(|member| member.id)
    }
}

fn reserve<T>(
    budget: &MemoryBudget,
    count: usize,
    lane: BudgetLane,
) -> Result<Allocation, MemoryError> {
    let bytes = array::<T>(count).map_err(|_| MemoryError::Capacity {
        requested: count,
        available: MAX_LAYOUT_MEMBERS,
    })?;
    Ok(budget.reserve(BudgetKind::Roots, lane, bytes)?.commit())
}

/// How many rows a member's split point reads from its middle for an
/// affinity that divides it.
const SPLIT_SCAN: usize = 4096;

fn route<T>(members: &[T], start: impl Fn(&T) -> Option<Affinity>, affinity: &Affinity) -> usize {
    members
        .partition_point(|member| start(member).is_none_or(|start| start <= *affinity))
        .saturating_sub(1)
}

/// A range group frozen at its committed prefix ([`NativeRanges::freeze`]):
/// read-only and sendable, every page charged where it was.
pub(super) struct FrozenRanges {
    producer: RangeId,
    epoch: u64,
    members: Vec<RangeBoundary>,
    stores: Vec<FrozenRange<PackedKey, Row>>,
    _charges: [Allocation; 2],
}

impl FrozenRanges {
    pub(super) fn id(&self) -> RangeId {
        self.producer
    }
    pub(super) fn epoch(&self) -> u64 {
        self.epoch
    }
    pub(super) fn members(&self) -> &[RangeBoundary] {
        &self.members
    }
    pub(super) fn prefix(&self) -> u64 {
        self.stores.first().map_or(0, FrozenRange::prefix)
    }
    pub(super) fn len(&self) -> usize {
        self.stores.iter().map(FrozenRange::len).sum()
    }
    pub(super) fn pages(&self) -> usize {
        self.stores
            .iter()
            .fold(0, |pages, store| pages.saturating_add(store.pages()))
    }
    /// Every row in key order.
    pub(super) fn entries(&self) -> impl Iterator<Item = RowEntry<'_>> {
        self.stores
            .iter()
            .flat_map(FrozenRange::entries)
            .map(RowEntry::of)
    }
}

/// The rows of one native state: the range group.
pub(super) struct NativeRanges {
    producer: RangeId,
    layout: RangeLayout,
    stores: Vec<RangeStore<PackedKey, Row>>,
    _allocation: Allocation,
}

impl std::fmt::Debug for NativeRanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRanges")
            .field("producer", &self.producer)
            .field("members", &self.layout.members.len())
            .field("prefix", &self.prefix())
            .field("entries", &self.len())
            .finish_non_exhaustive()
    }
}

impl NativeRanges {
    /// An empty group of one member, the member's identity being the
    /// producer's.
    pub(super) fn new(
        producer: RangeId,
        config: RangeConfig,
        budget: &MemoryBudget,
        partition: fn(&PackedKey) -> u64,
    ) -> Result<Self, MemoryError> {
        let store = RangeStore::new_partitioned(producer, 0, config, budget.clone(), partition)?;
        let layout = RangeLayout::single(producer, budget)?;
        Self::assemble(
            producer,
            layout,
            store,
            budget,
            BudgetLane::Ordinary,
            prepare::copy,
        )
    }

    /// A group over `store`, which holds every row, laid out per `layout`:
    /// the store is divided at every boundary (sharing every page but the
    /// boundary pages, copied through `copy`). Member identities are the
    /// layout's; the stores' own identities are process-local.
    pub(super) fn from_store<F>(
        producer: RangeId,
        layout: RangeLayout,
        store: RangeStore<PackedKey, Row>,
        budget: &MemoryBudget,
        lane: BudgetLane,
        copy: F,
    ) -> Result<Self, MemoryError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
    {
        Self::assemble(producer, layout, store, budget, lane, copy)
    }

    fn assemble<F>(
        producer: RangeId,
        layout: RangeLayout,
        store: RangeStore<PackedKey, Row>,
        budget: &MemoryBudget,
        lane: BudgetLane,
        mut copy: F,
    ) -> Result<Self, MemoryError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
    {
        let members = layout.members.len();
        if members == 0 {
            return Err(MemoryError::InvalidConfiguration("empty range layout"));
        }
        // The group's own directory is charged to the assembly's lane, as
        // its stores are: a restore funded by the completion allowance
        // assembles under it alone.
        let allocation = reserve::<RangeStore<PackedKey, Row>>(budget, members, lane)?;
        let mut stores = Vec::new();
        stores
            .try_reserve_exact(members)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut remaining = store;
        for (index, boundary) in layout.members.iter().enumerate().skip(1) {
            let start = boundary
                .start
                .ok_or(MemoryError::InvalidConfiguration("unbounded range member"))?;
            let left_id = layout
                .members
                .get(index.wrapping_sub(1))
                .ok_or(MemoryError::WrongRange)?
                .id;
            let (left, right) = remaining.split_where_with(
                |key| key.affinity() >= start,
                left_id,
                boundary.id,
                lane,
                &mut copy,
            )?;
            stores.push(left);
            remaining = right;
        }
        stores.push(remaining);
        Ok(Self {
            producer,
            layout,
            stores,
            _allocation: allocation,
        })
    }

    /// The producer identity records carry.
    pub(super) fn id(&self) -> RangeId {
        self.producer
    }
    pub(super) fn layout(&self) -> &RangeLayout {
        &self.layout
    }
    pub(super) fn members(&self) -> usize {
        self.stores.len()
    }
    pub(super) fn prefix(&self) -> u64 {
        self.stores.first().map_or(0, RangeStore::prefix)
    }
    pub(super) fn len(&self) -> usize {
        self.stores.iter().map(RangeStore::len).sum()
    }
    fn member(&self, key: &PackedKey) -> Option<&RangeStore<PackedKey, Row>> {
        self.stores.get(self.layout.route_packed(key))
    }
    pub(super) fn get(&self, key: &Key) -> Option<&Row> {
        let key = PackedKey::probe(key);
        self.member(&key)?.get(&key)
    }
    /// Every row in key order.
    pub(super) fn entries(&self) -> impl Iterator<Item = RowEntry<'_>> {
        self.stores
            .iter()
            .flat_map(RangeStore::entries)
            .map(RowEntry::of)
    }
    /// Every member frozen at the committed prefix, in the layout they lie
    /// in: an image a checkpoint encodes from while the group goes on. Its
    /// charges are committed work: a checkpoint keeps what was committed, so
    /// an owner whose fresh admissions filled its ordinary lane, as a full
    /// session's have, still takes one (doc 31 M3).
    pub(super) fn freeze(&self, budget: &MemoryBudget) -> Result<FrozenRanges, MemoryError> {
        let count = self.stores.len();
        let boundaries_charge = reserve::<RangeBoundary>(budget, count, BudgetLane::Committed)?;
        let stores_charge =
            reserve::<FrozenRange<PackedKey, Row>>(budget, count, BudgetLane::Committed)?;
        let mut members = Vec::new();
        members
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        members.extend(self.layout.members.iter().copied());
        let mut stores = Vec::new();
        stores
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        for store in &self.stores {
            stores.push(store.freeze_in(BudgetLane::Committed)?);
        }
        Ok(FrozenRanges {
            producer: self.producer,
            epoch: self.layout.epoch,
            members,
            stores,
            _charges: [boundaries_charge, stores_charge],
        })
    }
    /// Rows at or after `key` in key order, across members.
    pub(super) fn entries_from<'a>(
        &'a self,
        key: &Key,
        exclusive: bool,
    ) -> impl Iterator<Item = RowEntry<'a>> + use<'a> {
        let key = PackedKey::probe(key);
        let index = self.layout.route_packed(&key);
        let first = self
            .stores
            .get(index)
            .map(|store| store.entries_from(&key, exclusive));
        let rest = self
            .stores
            .get(index.saturating_add(1)..)
            .into_iter()
            .flatten()
            .flat_map(RangeStore::entries);
        first.into_iter().flatten().chain(rest).map(RowEntry::of)
    }
    /// The rows of the member at `index`, in key order.
    pub(super) fn member_entries(
        &self,
        index: usize,
    ) -> Option<impl Iterator<Item = RowEntry<'_>>> {
        self.stores
            .get(index)
            .map(|store| store.entries().map(RowEntry::of))
    }
    /// The statistics of the member at `index`.
    pub(super) fn member_stats(&self, index: usize) -> Option<RangeStats> {
        self.stores.get(index).map(RangeStore::stats)
    }
    /// An affinity that divides the member at `index` near its middle (25
    /// §8): the first affinity boundary at or after the median row, so both
    /// sides keep rows; `None` when the member's rows share one affinity.
    /// An affinity dividing member `index` near its middle: the first after
    /// a row of a smaller one, from the page before its middle page on
    /// (reached by one descent; the walk from its first row to its middle one
    /// read half its rows, and its balancer asks again at every pass a split
    /// is refused, which it is while candidates are pending). None where no
    /// affinity changes within [`SPLIT_SCAN`] rows of there, or the member has
    /// fewer than two rows.
    pub(super) fn member_split_point(&self, index: usize) -> Option<Affinity> {
        let store = self.stores.get(index)?;
        if store.len() < 2 {
            return None;
        }
        let start = (store.stats().pages / 2).saturating_sub(1);
        let mut previous: Option<Affinity> = None;
        for entry in store.entries_from_page(start).take(SPLIT_SCAN) {
            let affinity = entry.key.affinity();
            if previous.is_some_and(|before| affinity > before) {
                return Some(affinity);
            }
            previous = Some(affinity);
        }
        None
    }
    pub(super) fn stats(&self) -> RangeStats {
        let mut stats = RangeStats {
            prefix: self.prefix(),
            entries: 0,
            pages: 0,
            pinned_snapshots: 0,
            clock: 0,
        };
        for store in &self.stores {
            let member = store.stats();
            stats.entries = stats.entries.saturating_add(member.entries);
            stats.pages = stats.pages.saturating_add(member.pages);
            stats.pinned_snapshots = stats.pinned_snapshots.max(member.pinned_snapshots);
            stats.clock = stats.clock.max(member.clock);
        }
        stats
    }
    /// Audit an ordered chain of fragment sets against every member: the
    /// first on the published prefix, each after on the one before it.
    pub(super) fn validate_chain<'a>(
        &self,
        mut pending: impl Iterator<Item = &'a Fragments> + Clone,
    ) -> Result<(), MemoryError> {
        let Some(first) = pending.clone().next() else {
            return Ok(());
        };
        if let FragmentKind::Sparse(parts) = &first.kind {
            if parts.len() != self.stores.len() {
                return Err(MemoryError::WrongRange);
            }
            for (store, part) in self.stores.iter().zip(parts.iter()) {
                store.check_sparse(&part.value)?;
            }
            let mut previous = pending.next();
            for next in pending {
                if let Some(previous) = previous {
                    previous.validate_successor(next)?;
                }
                previous = Some(next);
            }
            return Ok(());
        }
        for (index, store) in self.stores.iter().enumerate() {
            let mut missing = false;
            store.validate_chain(pending.clone().filter_map(|fragments| {
                let part = fragments.built(index);
                missing |= part.is_none();
                part
            }))?;
            if missing {
                return Err(MemoryError::WrongRange);
            }
        }
        Ok(())
    }
    /// Plan one write at the next prefix as its changes alone (doc 30 §9),
    /// over the base state its caller reads: the published members with the
    /// pending candidates' changes over them, `tail` the newest of those,
    /// `present` the heap of the row that state holds at a key. Each member's
    /// part holds its changes and what its publication may spend; nothing of
    /// the members' pages. A wider group charges one vector of further parts
    /// to `source`.
    pub(super) fn plan_sparse(
        &self,
        source: &MemoryBudget,
        tail: Option<&Fragments>,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        present: impl FnMut(&Key) -> Option<usize>,
    ) -> Result<Fragments, MemoryError> {
        self.plan_sparse_from(source, None, tail, prefix, changes, lane, None, present)
    }
    /// [`Self::plan_sparse`] of a change set that may pass the store's bound on a batch, up to
    /// `max_entries`: a retirement's, whose one family may (doc 31 M2).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn plan_sparse_bounded(
        &self,
        source: &MemoryBudget,
        tail: Option<&Fragments>,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        max_entries: usize,
        present: impl FnMut(&Key) -> Option<usize>,
    ) -> Result<Fragments, MemoryError> {
        self.plan_sparse_from(
            source,
            None,
            tail,
            prefix,
            changes,
            lane,
            Some(max_entries),
            present,
        )
    }
    /// [`Self::plan_sparse`] over the published members alone, each member's
    /// part of the changes funded from `funding`, which its caller reserved
    /// with them (a follower's replayed record): never charged twice.
    pub(super) fn plan_sparse_funded(
        &self,
        source: &MemoryBudget,
        funding: &mut Allocation,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        present: impl FnMut(&Key) -> Option<usize>,
    ) -> Result<Fragments, MemoryError> {
        self.plan_sparse_from(
            source,
            Some(funding),
            None,
            prefix,
            changes,
            lane,
            None,
            present,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn plan_sparse_from(
        &self,
        source: &MemoryBudget,
        mut funding: Option<&mut Allocation>,
        tail: Option<&Fragments>,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        max_entries: Option<usize>,
        mut present: impl FnMut(&Key) -> Option<usize>,
    ) -> Result<Fragments, MemoryError> {
        #[cfg(test)]
        prepare::copy_failure()?;
        let mut changes = pack_changes(changes)?;
        changes.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        let mut present = |key: &PackedKey| present(&key.unpack());
        let members = self.stores.len();
        if tail.is_some_and(|tail| tail.members() != members) {
            return Err(MemoryError::WrongRange);
        }
        let (mut rest, allocation) = Parts::reserve_rest(members, source, lane)?;
        let mut stack = self.divide(&mut changes)?;
        let mut first = None;
        for (index, store) in self.stores.iter().enumerate() {
            let part = if index == 0 {
                std::mem::take(&mut changes)
            } else {
                stack.pop().unwrap_or_default()
            };
            let base = match tail {
                Some(tail) => SparseBase::After(tail.sparse(index).ok_or(MemoryError::WrongRange)?),
                None => SparseBase::Committed,
            };
            let value = match funding.as_deref_mut() {
                Some(funding) => store.plan_sparse_funded(
                    source,
                    funding,
                    base,
                    prefix,
                    part,
                    lane,
                    max_entries.unwrap_or(store.max_batch_entries()),
                    &mut present,
                )?,
                None => store.plan_sparse(
                    source,
                    base,
                    prefix,
                    part,
                    lane,
                    max_entries.unwrap_or(store.max_batch_entries()),
                    &mut present,
                )?,
            };
            let member = RangesMember {
                start: self
                    .layout
                    .members
                    .get(index)
                    .ok_or(MemoryError::WrongRange)?
                    .start,
                value,
            };
            if index == 0 {
                first = Some(member);
            } else {
                if rest.len() >= rest.capacity() {
                    return Err(MemoryError::WrongRange);
                }
                rest.push(member);
            }
        }
        let first = first.ok_or(MemoryError::InvalidConfiguration("empty range group"))?;
        Ok(Fragments {
            producer: self.producer,
            kind: FragmentKind::Sparse(Parts {
                first,
                rest,
                _allocation: allocation,
            }),
        })
    }
    /// Divides sorted `changes` at every member boundary from the last down:
    /// `changes` keeps the first member's, and the stack the rest's, the
    /// second member's on top.
    fn divide(
        &self,
        changes: &mut Vec<Change<PackedKey, Row>>,
    ) -> Result<Vec<Vec<Change<PackedKey, Row>>>, MemoryError> {
        let members = self.stores.len();
        let mut stack: Vec<Vec<Change<PackedKey, Row>>> = Vec::new();
        if members > 1 {
            stack
                .try_reserve_exact(members.saturating_sub(1))
                .map_err(|_| MemoryError::AllocationFailed)?;
        }
        for boundary in self.layout.members.iter().skip(1).rev() {
            let start = boundary
                .start
                .ok_or(MemoryError::InvalidConfiguration("unbounded range member"))?;
            let at = changes.partition_point(|change| change.key().affinity() < start);
            let part = if at == changes.len() {
                Vec::new()
            } else {
                changes.split_off(at)
            };
            stack.push(part);
        }
        Ok(stack)
    }
    /// Plan one write at the next prefix over the published members. A
    /// group of one plans without allocating; a wider group charges one
    /// vector of further plans to `source`, the budget the write will build
    /// from, within the group bytes its envelope carries.
    #[cfg(test)]
    pub(super) fn plan_batch<'a>(
        &'a self,
        source: &MemoryBudget,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        max_bytes: usize,
    ) -> Result<RangesPlan<'a>, MemoryError> {
        self.plan(source, None, prefix, changes, lane, max_bytes, None)
    }
    /// `max_entries`: a decision's own bound on its batch, where a write is
    /// held to the members' `max_batch_entries`.
    #[allow(clippy::too_many_arguments)]
    fn plan<'a>(
        &'a self,
        source: &MemoryBudget,
        predecessor: Option<&'a Fragments>,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        max_bytes: usize,
        max_entries: Option<usize>,
    ) -> Result<RangesPlan<'a>, MemoryError> {
        let mut changes = pack_changes(changes)?;
        changes.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        let members = self.stores.len();
        let (mut rest, allocation) = Parts::reserve_rest(members, source, lane)?;
        // Divide the sorted input at every boundary from the last member
        // down, so every member's part is one split of the same vector; the
        // parts are planned in member order as they come off the stack.
        let mut stack = self.divide(&mut changes)?;
        let mut remaining = max_bytes;
        let mut first = None;
        for (index, store) in self.stores.iter().enumerate() {
            let part = if index == 0 {
                std::mem::take(&mut changes)
            } else {
                stack.pop().unwrap_or_default()
            };
            let plan = match (predecessor, max_entries) {
                (Some(predecessor), _) => {
                    let base = predecessor.built(index).ok_or(MemoryError::WrongRange)?;
                    store.plan_after(base, prefix, part, lane, remaining)?
                }
                (None, Some(bound)) => store.plan_bounded(prefix, part, lane, remaining, bound)?,
                (None, None) => store.plan_batch(prefix, part, lane, remaining)?,
            };
            remaining = remaining.saturating_sub(plan.charges().additional_peak_bytes());
            let member = RangesMember {
                start: self
                    .layout
                    .members
                    .get(index)
                    .ok_or(MemoryError::WrongRange)?
                    .start,
                value: plan,
            };
            if index == 0 {
                first = Some(member);
            } else {
                if rest.len() >= rest.capacity() {
                    return Err(MemoryError::WrongRange);
                }
                rest.push(member);
            }
        }
        let first = first.ok_or(MemoryError::InvalidConfiguration("empty range group"))?;
        Ok(RangesPlan {
            producer: self.producer,
            lane,
            plans: Parts {
                first,
                rest,
                _allocation: allocation,
            },
        })
    }
    /// Publish one set of changes at `prefix` (the next prefix) as a session
    /// decision applies it (26 §4): planned over the published members,
    /// built under the group's budget and published whole or not at all.
    /// The batch is held to `max_entries`, the decision's own bound (a seal's
    /// rows, a family's), not to a write's: a decision is one publication
    /// however many rows it takes. Past its own bound it is refused as a
    /// contract the record broke — never as memory, which its caller would
    /// retry for good.
    pub(super) fn publish_changes(
        &mut self,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        max_entries: usize,
    ) -> Result<(), NativeError> {
        if changes.len() > max_entries {
            return Err(NativeError::Contract(ContractError::InvalidManifest));
        }
        let budget = self
            .stores
            .first()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?
            .budget()
            .clone();
        let fragments = self
            .plan(
                &budget,
                None,
                prefix,
                changes,
                lane,
                usize::MAX,
                Some(max_entries),
            )?
            .build_in_with(&budget, super::prepare::copy)?;
        self.publish_recoverable(fragments)
            .map_err(|(error, _)| error)?;
        Ok(())
    }
    /// Prepare one write over the published members, values copied through
    /// `copy`: the test fixtures' way of forging rows.
    #[cfg(test)]
    pub(super) fn prepare_batch_with<F>(
        &self,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        copy: F,
    ) -> Result<Fragments, MemoryError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
    {
        self.prepare_batch_replacing(prefix, changes, lane, copy, |_, _| Ok(()))
    }
    /// A fixture's write of `changes` over `predecessor` (or the published
    /// members), planned as a leader's is: its changes alone, checked against
    /// the base state, `replaced` given every row of that state a change
    /// replaces or deletes.
    #[cfg(test)]
    pub(super) fn forge<R>(
        &self,
        predecessor: Option<&Fragments>,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        mut replaced: R,
    ) -> Result<Fragments, MemoryError>
    where
        R: FnMut(&Key, &Row) -> Result<(), MemoryError>,
    {
        let budget = self
            .stores
            .first()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?
            .budget()
            .clone();
        let base = |key: &Key| match predecessor {
            Some(previous) if previous.is_sparse() => match previous.change(key) {
                Some(change) => change,
                None => self.get_entry(key),
            },
            Some(previous) => previous.get_entry(key),
            None => self.get_entry(key),
        };
        for change in &changes {
            if let Some(old) = base(change.key()) {
                replaced(change.key(), old.value)?;
            }
        }
        self.plan_sparse(&budget, predecessor, prefix, changes, lane, |key| {
            base(key).map(|entry| entry.heap_bytes)
        })
    }
    /// [`Self::prepare_batch_with`], giving `replaced` every committed row a change replaces or
    /// deletes ([`RangesPlan::build_in_replacing`]).
    #[cfg(test)]
    pub(super) fn prepare_batch_replacing<F, R>(
        &self,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        copy: F,
        replaced: R,
    ) -> Result<Fragments, MemoryError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
        R: FnMut(&Key, &Row) -> Result<(), MemoryError>,
    {
        drop(copy);
        self.forge(None, prefix, changes, lane, replaced)
    }
    /// Prepare one write against an unpublished predecessor's fragments, giving `replaced` every
    /// row of the predecessor's a change replaces or deletes ([`RangesPlan::build_in_replacing`]).
    #[cfg(test)]
    pub(super) fn prepare_after_replacing<F, R>(
        &self,
        predecessor: &Fragments,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        copy: F,
        replaced: R,
    ) -> Result<Fragments, MemoryError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
        R: FnMut(&Key, &Row) -> Result<(), MemoryError>,
    {
        drop(copy);
        self.forge(Some(predecessor), prefix, changes, lane, replaced)
    }
    pub(super) fn get_entry(&self, key: &Key) -> Option<RowEntry<'_>> {
        let key = PackedKey::probe(key);
        self.member(&key)?.get_entry(&key).map(RowEntry::of)
    }
    /// A group of one member over `store`, the member and producer both
    /// named by the store's identity: the fixtures' way of installing
    /// forged rows.
    #[cfg(test)]
    pub(super) fn single_from_store(
        store: RangeStore<PackedKey, Row>,
    ) -> Result<Self, MemoryError> {
        let budget = store.budget().clone();
        let layout = RangeLayout::single(store.id(), &budget)?;
        Self::from_store(
            store.id(),
            layout,
            store,
            &budget,
            BudgetLane::Ordinary,
            prepare::copy,
        )
    }
    /// Publish every fragment, dropping a refused set.
    #[cfg(test)]
    pub(super) fn publish(&mut self, fragments: Fragments) -> Result<(), MemoryError> {
        self.publish_recoverable(fragments)
            .map_err(|(error, _)| error)
    }
    /// Publish every fragment or none: each is checked against its member
    /// before any root moves, and a set refused there is returned whole. A
    /// member that refuses past that check broke an invariant: a built set
    /// returns the refused fragment and those after it, the ones before it
    /// staying published; a sparse set is gone, its member's store poisoned,
    /// and no set comes back.
    #[allow(clippy::result_large_err)] // A refused set is returned whole, never dropped.
    pub(super) fn publish_recoverable(
        &mut self,
        fragments: Fragments,
    ) -> Result<(), (MemoryError, Option<Fragments>)> {
        if fragments.members() != self.stores.len() {
            return Err((MemoryError::WrongRange, Some(fragments)));
        }
        let refusal = match &fragments.kind {
            FragmentKind::Built(parts) => {
                self.stores
                    .iter()
                    .zip(parts.iter())
                    .find_map(|(store, fragment)| {
                        store
                            .validate_chain(std::iter::once(&fragment.value.range))
                            .err()
                    })
            }
            FragmentKind::Sparse(parts) => self
                .stores
                .iter()
                .zip(parts.iter())
                .find_map(|(store, fragment)| store.check_sparse(&fragment.value).err()),
        };
        if let Some(error) = refusal {
            return Err((error, Some(fragments)));
        }
        let Fragments { producer, kind } = fragments;
        match kind {
            FragmentKind::Built(parts) => self
                .publish_built(producer, parts)
                .map_err(|(error, fragments)| (error, Some(fragments))),
            FragmentKind::Sparse(parts) => {
                for (store, fragment) in self.stores.iter_mut().zip(parts.into_iter()) {
                    store
                        .publish_sparse(fragment.value, prepare::copy)
                        .map_err(|refusal| (refusal.into_parts().0, None))?;
                }
                Ok(())
            }
        }
    }
    /// Every fragment passed the check publication repeats, so every root
    /// moves. Should a member still refuse (an invariant failure), the
    /// refused fragment and the unpublished ones after it are returned; the
    /// ones before it stay published, as their stores now hold them.
    #[allow(clippy::result_large_err)] // A refused set is returned whole, never dropped.
    fn publish_built(
        &mut self,
        producer: RangeId,
        parts: Parts<Built>,
    ) -> Result<(), (MemoryError, Fragments)> {
        let mut remaining = parts.into_iter();
        for store in self.stores.iter_mut() {
            let Some(fragment) = remaining.next() else {
                break;
            };
            let touched = fragment.value.touched;
            if let Err((error, range)) = store.publish_recoverable(fragment.value.range) {
                let refused = RangesMember {
                    start: fragment.start,
                    value: Built { range, touched },
                };
                let mut rest = Vec::new();
                if rest.try_reserve_exact(remaining.rest.len()).is_ok() {
                    rest.extend(remaining.rest.by_ref());
                }
                return Err((
                    error,
                    Fragments {
                        producer,
                        kind: FragmentKind::Built(Parts {
                            first: refused,
                            rest,
                            _allocation: remaining._allocation.take(),
                        }),
                    },
                ));
            }
        }
        Ok(())
    }
    /// Pin every member at the group's prefix; a refusal releases every
    /// lease taken so far.
    pub(super) fn pin(&mut self, now: u64, ttl: u64) -> Result<RangeLeases, MemoryError> {
        let budget = self
            .stores
            .first()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?
            .budget()
            .clone();
        let (mut rest, allocation) =
            Parts::reserve_rest(self.stores.len(), &budget, BudgetLane::Ordinary)?;
        let prefix = self.prefix();
        let mut first = None;
        let mut failure = None;
        for (index, store) in self.stores.iter_mut().enumerate() {
            match store.pin(now, ttl) {
                Ok(lease) if lease.prefix() == prefix => {
                    let member = RangesMember {
                        start: self
                            .layout
                            .members
                            .get(index)
                            .and_then(|member| member.start),
                        value: lease,
                    };
                    if index == 0 {
                        first = Some(member);
                    } else if rest.len() < rest.capacity() {
                        rest.push(member);
                    } else {
                        store.release(&member.value)?;
                        failure = Some(MemoryError::WrongRange);
                        break;
                    }
                }
                Ok(lease) => {
                    store.release(&lease)?;
                    failure = Some(MemoryError::InvalidConfiguration(
                        "range group members disagree on the prefix",
                    ));
                    break;
                }
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        if let Some(error) = failure {
            if let Some((store, lease)) = self.stores.first_mut().zip(first.as_ref()) {
                store.release(&lease.value)?;
            }
            for (store, lease) in self.stores.iter_mut().skip(1).zip(rest.iter()) {
                store.release(&lease.value)?;
            }
            return Err(error);
        }
        let first = first.ok_or(MemoryError::InvalidConfiguration("empty range group"))?;
        Ok(RangeLeases {
            prefix,
            epoch: self.layout.epoch,
            leases: Parts {
                first,
                rest,
                _allocation: allocation,
            },
        })
    }
    /// Release a read's leases, each from the member it pinned. After a
    /// layout change some of those members no longer exist: their leases
    /// expired with them and hold nothing here, while the members the
    /// change left alone still hold theirs and give them up now.
    pub(super) fn release(&mut self, read: &RangeLeases) -> Result<(), MemoryError> {
        if read.epoch == self.layout.epoch && read.leases.len() != self.stores.len() {
            return Err(MemoryError::WrongLease);
        }
        let stale = read.epoch != self.layout.epoch;
        for lease in read.leases.iter() {
            let id = lease.value.range_id();
            match self.stores.iter_mut().find(|store| store.id() == id) {
                // A member divided by a split keeps its identity for the
                // left half, a new store that never held the lease.
                Some(store) => match store.release(&lease.value) {
                    Ok(()) => {}
                    Err(MemoryError::LeaseExpired | MemoryError::WrongLease) if stale => {}
                    Err(error) => return Err(error),
                },
                None if stale => {}
                None => return Err(MemoryError::WrongLease),
            }
        }
        Ok(())
    }
    pub(super) fn advance_clock(&mut self, now: u64) -> Result<usize, MemoryError> {
        let mut expired = 0usize;
        for store in &mut self.stores {
            expired = expired.saturating_add(store.advance_clock(now)?);
        }
        Ok(expired)
    }
    /// The input bytes one write of `count` changes needs beyond its one
    /// vector when divided among the members: a vector per touched member
    /// and at most one slot per moved change (the shared envelope's term).
    pub(super) fn input_extra_bytes(&self, count: usize) -> Result<usize, NativeError> {
        let touched = self.stores.len().min(count.max(1));
        if touched <= 1 {
            return Ok(0);
        }
        prepare::add(
            touched
                .saturating_sub(1)
                .checked_mul(prepare::ALLOCATION)
                .ok_or(NativeError::Capacity("range input vectors"))?,
            count
                .checked_mul(size_of::<Change<PackedKey, Row>>())
                .ok_or(NativeError::Capacity("range input vectors"))?,
        )
    }
    /// One future write's envelope over a group of up to `max_members`
    /// members, so a later split or merge never invalidates a promise.
    pub(super) fn future_write_envelope(
        &self,
        limits: RangeWriteLimits,
        max_members: usize,
    ) -> Result<RangeWriteEnvelope, MemoryError> {
        // A row wider than a pointer is boxed (`Row`): each key a write puts may
        // bring at most the widest box, and each it deletes may release as much.
        let wide = super::wide_heap_max();
        let limits = RangeWriteLimits {
            incoming_heap: limits
                .incoming_heap
                .checked_add(
                    limits
                        .changed_keys
                        .checked_mul(wide)
                        .ok_or(MemoryError::AllocationFailed)?,
                )
                .ok_or(MemoryError::AllocationFailed)?,
            deleted_heap: limits
                .deleted_heap
                .checked_add(
                    limits
                        .deleted_keys
                        .checked_mul(wide)
                        .ok_or(MemoryError::AllocationFailed)?,
                )
                .ok_or(MemoryError::AllocationFailed)?,
            ..limits
        };
        let members = max_members.max(self.stores.len());
        let group = group_bytes(members).map_err(|_| MemoryError::Capacity {
            requested: members,
            available: MAX_LAYOUT_MEMBERS,
        })?;
        self.stores
            .first()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?
            .future_write_envelope_shared(limits, members)?
            .with_group_bytes(group)
    }
    /// Give member `index` the durable identity `id` (see
    /// [`RangeLayout::rename`]).
    pub(super) fn rename_member(&mut self, index: usize, id: RangeId) -> Result<(), NativeError> {
        self.layout.rename(index, id)
    }
    /// Add a boundary at affinity `at`: the member holding it keeps its
    /// identity for the affinities below and `id` names the member from
    /// `at` on. Refused when `at` is a member's start, for a known identity
    /// and when the layout is full. Leases taken before the change expire
    /// with the members they pinned; the pages live on, shared by the new
    /// members.
    pub(super) fn split<F>(
        &mut self,
        at: Affinity,
        id: RangeId,
        max: usize,
        lane: BudgetLane,
        copy: F,
    ) -> Result<(), NativeError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
    {
        self.layout.check_split(at, id, max)?;
        let index = route(&self.layout.members, |member| member.start, &at);
        let boundary = *self
            .layout
            .members
            .get(index)
            .ok_or(MemoryError::WrongRange)?;
        let budget = self
            .stores
            .first()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?
            .budget()
            .clone();
        let next = self.stores.len().saturating_add(1);
        let epoch = self
            .layout
            .epoch
            .checked_add(1)
            .ok_or(NativeError::Capacity("range layout epoch"))?;
        let store = self.stores.get(index).ok_or(MemoryError::WrongRange)?;
        let (left, right) =
            store.split_where_with(|key| key.affinity() >= at, boundary.id, id, lane, copy)?;
        let stores_allocation = reserve::<RangeStore<PackedKey, Row>>(&budget, next, lane)?;
        let layout_allocation = reserve::<RangeBoundary>(&budget, next, lane)?;
        let mut stores = Vec::new();
        stores
            .try_reserve_exact(next)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut members = Vec::new();
        members
            .try_reserve_exact(next)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let old_stores = std::mem::take(&mut self.stores);
        let mut halves = Some((left, right));
        for (position, old) in old_stores.into_iter().enumerate() {
            if position == index {
                if let Some((left, right)) = halves.take() {
                    stores.push(left);
                    stores.push(right);
                }
                members.push(boundary);
                members.push(RangeBoundary {
                    id,
                    start: Some(at),
                });
                continue;
            }
            stores.push(old);
            if let Some(member) = self.layout.members.get(position) {
                members.push(*member);
            }
        }
        let layout = RangeLayout::new(epoch, members, layout_allocation, max)?;
        self.stores = stores;
        self.layout = layout;
        self._allocation = stores_allocation;
        Ok(())
    }
    /// Remove the boundary after member `index`, joining it with the next
    /// under its identity, under the lease rule of [`Self::split`].
    pub(super) fn merge(&mut self, index: usize, lane: BudgetLane) -> Result<(), NativeError> {
        let right_index = index.saturating_add(1);
        if right_index >= self.stores.len() {
            return Err(ContractError::InvalidManifest.into());
        }
        let epoch = self
            .layout
            .epoch
            .checked_add(1)
            .ok_or(NativeError::Capacity("range layout epoch"))?;
        let budget = self
            .stores
            .first()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?
            .budget()
            .clone();
        let left = self.stores.get(index).ok_or(MemoryError::WrongRange)?;
        let right = self
            .stores
            .get(right_index)
            .ok_or(MemoryError::WrongRange)?;
        let joined = left.merge_with(right, left.id(), lane)?;
        let count = self.stores.len().saturating_sub(1);
        let stores_allocation = reserve::<RangeStore<PackedKey, Row>>(&budget, count, lane)?;
        let layout_allocation = reserve::<RangeBoundary>(&budget, count, lane)?;
        let mut stores = Vec::new();
        stores
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut members = Vec::new();
        members
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let old_stores = std::mem::take(&mut self.stores);
        let mut joined = Some(joined);
        for (position, old) in old_stores.into_iter().enumerate() {
            if position == index {
                if let Some(joined) = joined.take() {
                    stores.push(joined);
                }
                if let Some(member) = self.layout.members.get(position) {
                    members.push(*member);
                }
                continue;
            }
            if position == right_index {
                drop(old);
                continue;
            }
            stores.push(old);
            if let Some(member) = self.layout.members.get(position) {
                members.push(*member);
            }
        }
        let layout = RangeLayout::new(epoch, members, layout_allocation, MAX_LAYOUT_MEMBERS)?;
        self.stores = stores;
        self.layout = layout;
        self._allocation = stores_allocation;
        Ok(())
    }
}

/// One member's part of a plan, fragment set or lease set.
struct RangesMember<T> {
    start: Option<Affinity>,
    value: T,
}

/// A part per member: the first member's inline, so a group of one
/// allocates nothing per write, the rest in one vector charged to the
/// budget the write builds from. Members are in key order.
struct Parts<T> {
    first: RangesMember<T>,
    rest: Vec<RangesMember<T>>,
    _allocation: Option<Allocation>,
}

impl<T> Parts<T> {
    /// The vector for `members - 1` further parts, charged to `source`.
    fn reserve_rest(
        members: usize,
        source: &MemoryBudget,
        lane: BudgetLane,
    ) -> Result<(Vec<RangesMember<T>>, Option<Allocation>), MemoryError> {
        let further = members.saturating_sub(1);
        if further == 0 {
            return Ok((Vec::new(), None));
        }
        let bytes = array::<RangesMember<T>>(further).map_err(|_| MemoryError::Capacity {
            requested: further,
            available: MAX_LAYOUT_MEMBERS,
        })?;
        let allocation = source.reserve(BudgetKind::Pending, lane, bytes)?.commit();
        let mut rest = Vec::new();
        rest.try_reserve_exact(further)
            .map_err(|_| MemoryError::AllocationFailed)?;
        Ok((rest, Some(allocation)))
    }
    fn len(&self) -> usize {
        self.rest.len().saturating_add(1)
    }
    fn get(&self, index: usize) -> Option<&RangesMember<T>> {
        match index.checked_sub(1) {
            None => Some(&self.first),
            Some(rest) => self.rest.get(rest),
        }
    }
    fn iter(&self) -> impl Iterator<Item = &RangesMember<T>> {
        std::iter::once(&self.first).chain(self.rest.iter())
    }
    /// The member holding `key`: the last whose start is at or below the
    /// key's affinity.
    fn route(&self, key: &PackedKey) -> usize {
        let affinity = key.affinity();
        self.rest
            .partition_point(|member| member.start.is_some_and(|start| start <= affinity))
    }
    fn into_iter(self) -> PartsIter<T> {
        PartsIter {
            first: Some(self.first),
            rest: self.rest.into_iter(),
            _allocation: self._allocation,
        }
    }
}

/// Fields drop in order: the remaining parts before their allocation.
struct PartsIter<T> {
    first: Option<RangesMember<T>>,
    rest: std::vec::IntoIter<RangesMember<T>>,
    _allocation: Option<Allocation>,
}

impl<T> Iterator for PartsIter<T> {
    type Item = RangesMember<T>;
    fn next(&mut self) -> Option<Self::Item> {
        self.first.take().or_else(|| self.rest.next())
    }
}

/// The bytes a group of `members` needs per write beyond its stores: the
/// further plans and fragments (25 §4).
pub(super) fn group_bytes(members: usize) -> Result<usize, NativeError> {
    let further = members.saturating_sub(1);
    prepare::add(
        array::<RangesMember<RangePreparationPlan<'static, PackedKey, Row>>>(further)?,
        array::<RangesMember<PreparedRange<PackedKey, Row>>>(further)?,
    )
}

/// One write's plans, one per member in member order. Planning a group of
/// one allocates nothing; a wider group holds its further plans in one
/// vector charged to the budget the write builds from.
pub(super) struct RangesPlan<'a> {
    producer: RangeId,
    lane: BudgetLane,
    plans: Parts<RangePreparationPlan<'a, PackedKey, Row>>,
}

impl<'a> RangesPlan<'a> {
    /// The changed keys in key order across members.
    #[cfg(test)]
    pub(super) fn changes(&self) -> impl Iterator<Item = RowChange<'_>> {
        self.plans
            .iter()
            .flat_map(|member| member.value.changes().iter())
            .map(RowChange::of)
    }
    #[cfg(test)]
    pub(super) fn changes_len(&self) -> usize {
        self.plans
            .iter()
            .map(|member| member.value.changes().len())
            .sum()
    }
    #[cfg(test)]
    pub(super) fn check_envelope(&self, envelope: &RangeWriteEnvelope) -> Result<(), MemoryError> {
        envelope.check_plans(self.plans.iter().map(|member| &member.value))
    }
    /// Build every fragment from `source` under the copier contract of
    /// [`RangeStore::prepare_batch_with`]; a refusal drops the built ones.
    pub(super) fn build_in_with<F>(
        self,
        source: &MemoryBudget,
        mut copy: F,
    ) -> Result<Fragments, MemoryError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
    {
        self.build_each(source, |plan| plan.build_in_with(source, &mut copy))
    }
    /// [`Self::build_in_with`], giving `replaced` every committed row a change replaces or
    /// deletes as the build takes it out of its page
    /// (`RangePreparationPlan::build_in_replacing`).
    #[cfg(test)]
    pub(super) fn build_in_replacing<F, R>(
        self,
        source: &MemoryBudget,
        mut copy: F,
        mut replaced: R,
    ) -> Result<Fragments, MemoryError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
        R: FnMut(&Key, &Row) -> Result<(), MemoryError>,
    {
        self.build_each(source, |plan| {
            plan.build_in_replacing(source, &mut copy, &mut |key: &PackedKey, row: &Row| {
                replaced(&key.unpack(), row)
            })
        })
    }
    fn build_each(
        self,
        source: &MemoryBudget,
        mut build: impl FnMut(
            RangePreparationPlan<'a, PackedKey, Row>,
        ) -> Result<PreparedRange<PackedKey, Row>, MemoryError>,
    ) -> Result<Fragments, MemoryError> {
        let members = self.plans.len();
        let (mut rest, allocation) = Parts::reserve_rest(members, source, self.lane)?;
        let mut plans = self.plans.into_iter();
        let mut built = |plan: RangePreparationPlan<'a, PackedKey, Row>| {
            let touched = !plan.changes().is_empty();
            build(plan).map(|range| Built { range, touched })
        };
        let first = plans
            .next()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?;
        let first = RangesMember {
            start: first.start,
            value: built(first.value)?,
        };
        for member in plans {
            if rest.len() >= rest.capacity() {
                return Err(MemoryError::WrongRange);
            }
            rest.push(RangesMember {
                start: member.start,
                value: built(member.value)?,
            });
        }
        Ok(Fragments {
            producer: self.producer,
            kind: FragmentKind::Built(Parts {
                first,
                rest,
                _allocation: allocation,
            }),
        })
    }
}

/// One unpublished write over every member: a fragment per member at the
/// next prefix, published together or not at all. A leader's write holds its
/// changes alone and is read through its view's pending chain (doc 30 §9); a
/// follower's replay builds its pages beside the committed root.
pub struct Fragments {
    producer: RangeId,
    kind: FragmentKind,
}

#[allow(
    clippy::large_enum_variant,
    reason = "a leader's sparse set holds its first member inline so a group of one allocates \
              nothing; a replay's built set is published at once"
)]
enum FragmentKind {
    Built(Parts<Built>),
    Sparse(Parts<SparseRange<PackedKey, Row>>),
}

/// A member's built fragment, and whether the write changes the member.
struct Built {
    range: PreparedRange<PackedKey, Row>,
    touched: bool,
}

impl std::fmt::Debug for Fragments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fragments")
            .field("producer", &self.producer)
            .field("members", &self.members())
            .field("base", &self.base_prefix())
            .finish_non_exhaustive()
    }
}

impl Fragments {
    pub(super) fn id(&self) -> RangeId {
        self.producer
    }
    pub(super) fn prefix(&self) -> u64 {
        match &self.kind {
            FragmentKind::Built(parts) => parts.first.value.range.prefix(),
            FragmentKind::Sparse(parts) => parts.first.value.prefix(),
        }
    }
    pub(super) fn base_prefix(&self) -> u64 {
        match &self.kind {
            FragmentKind::Built(parts) => parts.first.value.range.base_prefix(),
            FragmentKind::Sparse(parts) => parts.first.value.base_prefix(),
        }
    }
    fn members(&self) -> usize {
        match &self.kind {
            FragmentKind::Built(parts) => parts.len(),
            FragmentKind::Sparse(parts) => parts.len(),
        }
    }
    /// The rows the members hold once this set publishes.
    pub(super) fn len(&self) -> usize {
        match &self.kind {
            FragmentKind::Built(parts) => parts.iter().map(|part| part.value.range.len()).sum(),
            FragmentKind::Sparse(parts) => parts.iter().map(|part| part.value.len()).sum(),
        }
    }
    /// Member `index`'s part of a sparse set.
    fn sparse(&self, index: usize) -> Option<&SparseRange<PackedKey, Row>> {
        match &self.kind {
            FragmentKind::Built(_) => None,
            FragmentKind::Sparse(parts) => parts.get(index).map(|part| &part.value),
        }
    }
    /// The positions (in layout order) of the members this set writes.
    pub(super) fn touched_members(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.members()).filter(|index| match &self.kind {
            FragmentKind::Built(parts) => parts.get(*index).is_some_and(|part| part.value.touched),
            FragmentKind::Sparse(parts) => parts
                .get(*index)
                .is_some_and(|part| !part.value.changes().is_empty()),
        })
    }
    fn built(&self, index: usize) -> Option<&PreparedRange<PackedKey, Row>> {
        match &self.kind {
            FragmentKind::Built(parts) => parts.get(index).map(|part| &part.value.range),
            FragmentKind::Sparse(_) => None,
        }
    }
    /// The row this set leaves at `key`: a built set's whole state, a sparse
    /// set's own puts alone (a view reads the rest through its chain).
    pub(super) fn get(&self, key: &Key) -> Option<&Row> {
        match &self.kind {
            FragmentKind::Built(parts) => {
                let key = PackedKey::probe(key);
                parts.get(parts.route(&key))?.value.range.get(&key)
            }
            FragmentKind::Sparse(_) => self.change(key)?.map(|entry| entry.value),
        }
    }
    /// A built set's row at `key`, whole; a sparse set's own put.
    pub(super) fn get_entry(&self, key: &Key) -> Option<RowEntry<'_>> {
        match &self.kind {
            FragmentKind::Built(parts) => {
                let key = PackedKey::probe(key);
                parts
                    .get(parts.route(&key))?
                    .value
                    .range
                    .get_entry(&key)
                    .map(RowEntry::of)
            }
            FragmentKind::Sparse(_) => self.change(key)?,
        }
    }
    /// Visits each row a sparse set's publication copies onto `ranges` where
    /// something else still holds a page it edits
    /// (`RangeStore::visit_retained`), member by member: what a caller that
    /// meters a publication's work prepays before anything changes.
    pub(super) fn visit_retained(
        &self,
        ranges: &NativeRanges,
        mut visit: impl FnMut(&Row) -> Result<(), MemoryError>,
    ) -> Result<(), MemoryError> {
        let FragmentKind::Sparse(parts) = &self.kind else {
            return Err(MemoryError::WrongRange);
        };
        if parts.len() != ranges.stores.len() {
            return Err(MemoryError::WrongRange);
        }
        for (store, part) in ranges.stores.iter().zip(parts.iter()) {
            store.visit_retained(&part.value, &mut visit)?;
        }
        Ok(())
    }
    /// Where a sparse set changes `key`: its member and the change's index
    /// there.
    pub(super) fn position(&self, key: &Key) -> Option<(usize, usize)> {
        match &self.kind {
            FragmentKind::Built(_) => None,
            FragmentKind::Sparse(parts) => {
                let key = PackedKey::probe(key);
                let member = parts.route(&key);
                parts
                    .get(member)?
                    .value
                    .position(&key)
                    .map(|index| (member, index))
            }
        }
    }
    /// This set's own change of `key`: `Some(Some(entry))` where it puts one,
    /// `Some(None)` where it deletes the key, `None` where it leaves the key
    /// (and for a built set, which holds its state rather than its changes).
    pub(super) fn change(&self, key: &Key) -> Option<Option<RowEntry<'_>>> {
        match &self.kind {
            FragmentKind::Built(_) => None,
            FragmentKind::Sparse(parts) => {
                let key = PackedKey::probe(key);
                parts
                    .get(parts.route(&key))?
                    .value
                    .change(&key)
                    .map(|entry| entry.map(RowEntry::of))
            }
        }
    }
    /// Member `member`'s `index`th change, by key order (a pending index's
    /// version).
    pub(super) fn change_at(&self, member: usize, index: usize) -> Option<RowChange<'_>> {
        match &self.kind {
            FragmentKind::Built(_) => None,
            FragmentKind::Sparse(parts) => parts
                .get(member)?
                .value
                .changes()
                .get(index)
                .map(RowChange::of),
        }
    }
    /// Every change of a sparse set, with its member and its index there, in
    /// key order; none for a built set.
    pub(super) fn changes(&self) -> impl Iterator<Item = (usize, usize, RowChange<'_>)> + '_ {
        let parts = match &self.kind {
            FragmentKind::Built(_) => None,
            FragmentKind::Sparse(parts) => Some(parts),
        };
        parts.into_iter().flat_map(|parts| {
            parts.iter().enumerate().flat_map(|(member, part)| {
                part.value
                    .changes()
                    .iter()
                    .enumerate()
                    .map(move |(index, change)| (member, index, RowChange::of(change)))
            })
        })
    }
    /// The changes of a sparse set at or after `key` in key order (`exclusive`:
    /// after it); none for a built set.
    pub(super) fn changes_from<'a>(
        &'a self,
        key: &Key,
        exclusive: bool,
    ) -> impl Iterator<Item = RowChange<'a>> + use<'a> {
        let parts = match &self.kind {
            FragmentKind::Built(_) => None,
            FragmentKind::Sparse(parts) => Some(parts),
        };
        let key = PackedKey::probe(key);
        let start = parts.map_or(0, |parts| parts.route(&key));
        parts
            .into_iter()
            .flat_map(move |parts| {
                let key = key.clone();
                parts.iter().skip(start).flat_map(move |part| {
                    let changes = part.value.changes();
                    let at = changes.partition_point(|change| {
                        if exclusive {
                            change.key() <= &key
                        } else {
                            change.key() < &key
                        }
                    });
                    changes.get(at..).unwrap_or(&[]).iter()
                })
            })
            .map(RowChange::of)
    }
    /// What a sparse set's parts hold as pending input and staging.
    #[cfg(test)]
    pub(super) fn pending_bytes(&self) -> usize {
        match &self.kind {
            FragmentKind::Built(_) => 0,
            FragmentKind::Sparse(parts) => parts
                .iter()
                .map(|part| part.value.input_bytes() + part.value.staging_bytes())
                .sum(),
        }
    }
    pub(super) fn is_sparse(&self) -> bool {
        matches!(self.kind, FragmentKind::Sparse(_))
    }
    /// Check a sparse set's parts against one write's envelope.
    pub(super) fn check_envelope(&self, envelope: &RangeWriteEnvelope) -> Result<(), MemoryError> {
        match &self.kind {
            FragmentKind::Built(_) => Err(MemoryError::InvalidConfiguration(
                "a built set's envelope is its plan's",
            )),
            FragmentKind::Sparse(parts) => {
                envelope.check_sparse(parts.iter().map(|part| &part.value))
            }
        }
    }
    /// A built set's whole state; a sparse set's own puts.
    #[cfg(test)]
    pub(super) fn entries(&self) -> impl Iterator<Item = RowEntry<'_>> {
        let parts = match &self.kind {
            FragmentKind::Built(parts) => Some(parts),
            FragmentKind::Sparse(_) => None,
        };
        let built = parts.into_iter().flat_map(|parts| {
            parts
                .iter()
                .flat_map(|part| part.value.range.entries().map(RowEntry::of))
        });
        let puts = self.changes().filter_map(|(_, _, change)| match change {
            RowChange::Put(entry) => Some(entry),
            RowChange::Delete(_) => None,
        });
        built.chain(puts)
    }
    /// A built set's rows at or after `key`; none for a sparse set, which a
    /// view reads through its chain.
    pub(super) fn entries_from<'a>(
        &'a self,
        key: &Key,
        exclusive: bool,
    ) -> impl Iterator<Item = RowEntry<'a>> + use<'a> {
        let parts = match &self.kind {
            FragmentKind::Built(parts) => Some(parts),
            FragmentKind::Sparse(_) => None,
        };
        let key = PackedKey::probe(key);
        let index = parts.map_or(0, |parts| parts.route(&key));
        let first = parts
            .and_then(|parts| parts.get(index))
            .map(|part| part.value.range.entries_from(&key, exclusive));
        let rest = parts.into_iter().flat_map(move |parts| {
            parts
                .iter()
                .skip(index.saturating_add(1))
                .flat_map(|part| part.value.range.entries())
        });
        first.into_iter().flatten().chain(rest).map(RowEntry::of)
    }
    /// Check `next` is the exact successor of this set on every member.
    pub(super) fn validate_successor(&self, next: &Self) -> Result<(), MemoryError> {
        match (&self.kind, &next.kind) {
            (FragmentKind::Built(mine), FragmentKind::Built(theirs)) => {
                if mine.len() != theirs.len() {
                    return Err(MemoryError::WrongRange);
                }
                for (mine, theirs) in mine.iter().zip(theirs.iter()) {
                    mine.value.range.validate_successor(&theirs.value.range)?;
                }
                Ok(())
            }
            (FragmentKind::Sparse(mine), FragmentKind::Sparse(theirs)) => {
                if mine.len() != theirs.len() || self.producer != next.producer {
                    return Err(MemoryError::WrongRange);
                }
                for (mine, theirs) in mine.iter().zip(theirs.iter()) {
                    if theirs.value.base_prefix() != mine.value.prefix() {
                        return Err(MemoryError::StalePreparation {
                            prepared_at: theirs.value.base_prefix(),
                            current_prefix: mine.value.prefix(),
                        });
                    }
                    // Planned on another state at this prefix: a fork.
                    if !theirs.value.follows(&mine.value) {
                        return Err(MemoryError::WrongRange);
                    }
                }
                Ok(())
            }
            _ => Err(MemoryError::WrongRange),
        }
    }
}

/// One read's leases: every member pinned at one prefix.
pub struct RangeLeases {
    prefix: u64,
    /// The layout epoch the leases were taken under.
    epoch: u64,
    leases: Parts<SnapshotLease<PackedKey, Row>>,
}

impl std::fmt::Debug for RangeLeases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeLeases")
            .field("prefix", &self.prefix)
            .field("members", &self.leases.len())
            .finish_non_exhaustive()
    }
}

impl RangeLeases {
    pub(super) fn prefix(&self) -> u64 {
        self.prefix
    }
    /// Project the first row at or after `start` (after it when
    /// `exclusive`) below `end`, wherever it lives: the member holding
    /// `start`, else the first later member with any row below `end`.
    pub(super) fn project_next<R>(
        &self,
        start: &Key,
        exclusive: bool,
        end: &Key,
        now: u64,
        project: impl FnOnce(RowEntry<'_>) -> R,
    ) -> Result<Option<R>, MemoryError> {
        let start = PackedKey::probe(start);
        let end = PackedKey::probe(end);
        let index = self.leases.route(&start);
        let end_affinity = end.affinity();
        let mut project = Some(project);
        for (position, lease) in self.leases.iter().enumerate().skip(index) {
            let (from, exclusive) = if position == index {
                (Some(&start), exclusive)
            } else {
                // A later member begins at its start affinity; past the
                // end's affinity nothing below `end` remains.
                if lease.start.is_some_and(|start| start > end_affinity) {
                    break;
                }
                (None, false)
            };
            let found = lease
                .value
                .project_from(from, exclusive, &end, now, |entry| {
                    project.take().map(|project| project(RowEntry::of(entry)))
                })?;
            match found {
                Some(Some(value)) => return Ok(Some(value)),
                Some(None) => return Ok(None),
                None => continue,
            }
        }
        Ok(None)
    }
}
