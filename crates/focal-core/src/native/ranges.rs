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

use super::prepare::{self, array};
use super::{ContractError, Key, NativeError, Row, layout};
use focal_memory::{
    Allocation, BudgetKind, BudgetLane, Change, Entry, MemoryBudget, MemoryError, PreparedRange,
    RangeConfig, RangeId, RangePreparationPlan, RangeStats, RangeStore, RangeWriteEnvelope,
    RangeWriteLimits, SnapshotLease,
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
        let allocation = reserve::<RangeBoundary>(budget, 1)?;
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
    pub(super) fn route(&self, key: &Key) -> usize {
        route(&self.members, |member| member.start, &layout::affinity(key))
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

fn reserve<T>(budget: &MemoryBudget, count: usize) -> Result<Allocation, MemoryError> {
    let bytes = array::<T>(count).map_err(|_| MemoryError::Capacity {
        requested: count,
        available: MAX_LAYOUT_MEMBERS,
    })?;
    Ok(budget
        .reserve(BudgetKind::Roots, BudgetLane::Ordinary, bytes)?
        .commit())
}

fn route<T>(members: &[T], start: impl Fn(&T) -> Option<Affinity>, affinity: &Affinity) -> usize {
    members
        .partition_point(|member| start(member).is_none_or(|start| start <= *affinity))
        .saturating_sub(1)
}

/// The rows of one native state: the range group.
pub(super) struct NativeRanges {
    producer: RangeId,
    layout: RangeLayout,
    stores: Vec<RangeStore<Key, Row>>,
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
        partition: fn(&Key) -> u64,
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
        store: RangeStore<Key, Row>,
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
        store: RangeStore<Key, Row>,
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
        let allocation = reserve::<RangeStore<Key, Row>>(budget, members)?;
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
                |key| layout::affinity(key) >= start,
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
    fn member(&self, key: &Key) -> Option<&RangeStore<Key, Row>> {
        self.stores.get(self.layout.route(key))
    }
    pub(super) fn get(&self, key: &Key) -> Option<&Row> {
        self.member(key)?.get(key)
    }
    /// Every row in key order.
    pub(super) fn entries(&self) -> impl Iterator<Item = &Entry<Key, Row>> {
        self.stores.iter().flat_map(RangeStore::entries)
    }
    /// Rows at or after `key` in key order, across members.
    pub(super) fn entries_from<'a>(
        &'a self,
        key: &Key,
        exclusive: bool,
    ) -> impl Iterator<Item = &'a Entry<Key, Row>> + use<'a> {
        let index = self.layout.route(key);
        let first = self
            .stores
            .get(index)
            .map(|store| store.entries_from(key, exclusive));
        let rest = self
            .stores
            .get(index.saturating_add(1)..)
            .into_iter()
            .flatten()
            .flat_map(RangeStore::entries);
        first.into_iter().flatten().chain(rest)
    }
    /// The rows of the member at `index`, in key order.
    pub(super) fn member_entries(
        &self,
        index: usize,
    ) -> Option<impl Iterator<Item = &Entry<Key, Row>>> {
        self.stores.get(index).map(|store| store.entries())
    }
    /// The statistics of the member at `index`.
    pub(super) fn member_stats(&self, index: usize) -> Option<RangeStats> {
        self.stores.get(index).map(RangeStore::stats)
    }
    /// An affinity that divides the member at `index` near its middle (25
    /// §8): the first affinity boundary at or after the median row, so both
    /// sides keep rows; `None` when the member's rows share one affinity.
    pub(super) fn member_split_point(&self, index: usize) -> Option<Affinity> {
        let store = self.stores.get(index)?;
        let count = store.len();
        if count < 2 {
            return None;
        }
        let middle = count / 2;
        let mut previous: Option<Affinity> = None;
        for (position, entry) in store.entries().enumerate() {
            let affinity = layout::affinity(&entry.key);
            if position >= middle && previous.is_some_and(|before| affinity > before) {
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
    /// Audit an ordered chain of fragment sets against every member.
    pub(super) fn validate_chain<'a>(
        &self,
        pending: impl Iterator<Item = &'a Fragments> + Clone,
    ) -> Result<(), MemoryError> {
        for (index, store) in self.stores.iter().enumerate() {
            let mut missing = false;
            store.validate_chain(pending.clone().filter_map(|fragments| {
                let part = fragments.part(index);
                missing |= part.is_none();
                part
            }))?;
            if missing {
                return Err(MemoryError::WrongRange);
            }
        }
        Ok(())
    }
    /// Plan one write at the next prefix over the published members. A
    /// group of one plans without allocating; a wider group charges one
    /// vector of further plans to `source`, the budget the write will build
    /// from, within the group bytes its envelope carries.
    pub(super) fn plan_batch<'a>(
        &'a self,
        source: &MemoryBudget,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        max_bytes: usize,
    ) -> Result<RangesPlan<'a>, MemoryError> {
        self.plan(source, None, prefix, changes, lane, max_bytes)
    }
    /// Plan one write against an unpublished predecessor's fragments.
    pub(super) fn plan_after<'a>(
        &'a self,
        source: &MemoryBudget,
        predecessor: &'a Fragments,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        max_bytes: usize,
    ) -> Result<RangesPlan<'a>, MemoryError> {
        if predecessor.parts.len() != self.stores.len() {
            return Err(MemoryError::WrongRange);
        }
        self.plan(source, Some(predecessor), prefix, changes, lane, max_bytes)
    }
    fn plan<'a>(
        &'a self,
        source: &MemoryBudget,
        predecessor: Option<&'a Fragments>,
        prefix: u64,
        mut changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        max_bytes: usize,
    ) -> Result<RangesPlan<'a>, MemoryError> {
        changes.sort_unstable_by(|left, right| left.key().cmp(right.key()));
        let members = self.stores.len();
        let (mut rest, allocation) = Parts::reserve_rest(members, source, lane)?;
        // Divide the sorted input at every boundary from the last member
        // down, so every member's part is one split of the same vector; the
        // parts are planned in member order as they come off the stack.
        let mut stack: Vec<Vec<Change<Key, Row>>> = Vec::new();
        if members > 1 {
            stack
                .try_reserve_exact(members.saturating_sub(1))
                .map_err(|_| MemoryError::AllocationFailed)?;
        }
        for boundary in self.layout.members.iter().skip(1).rev() {
            let start = boundary
                .start
                .ok_or(MemoryError::InvalidConfiguration("unbounded range member"))?;
            let at = changes.partition_point(|change| layout::affinity(change.key()) < start);
            let part = if at == changes.len() {
                Vec::new()
            } else {
                changes.split_off(at)
            };
            stack.push(part);
        }
        let mut remaining = max_bytes;
        let mut first = None;
        for (index, store) in self.stores.iter().enumerate() {
            let part = if index == 0 {
                std::mem::take(&mut changes)
            } else {
                stack.pop().unwrap_or_default()
            };
            let plan = match predecessor {
                Some(predecessor) => {
                    let base = predecessor.part(index).ok_or(MemoryError::WrongRange)?;
                    store.plan_after(base, prefix, part, lane, remaining)?
                }
                None => store.plan_batch(prefix, part, lane, remaining)?,
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
    pub(super) fn publish_changes(
        &mut self,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
    ) -> Result<(), NativeError> {
        let budget = self
            .stores
            .first()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?
            .budget()
            .clone();
        let fragments = self
            .plan_batch(&budget, prefix, changes, lane, usize::MAX)?
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
        let budget = self
            .stores
            .first()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?
            .budget()
            .clone();
        self.plan_batch(&budget, prefix, changes, lane, usize::MAX)?
            .build_in_with(&budget, copy)
    }
    /// Prepare one write against an unpublished predecessor's fragments.
    #[cfg(test)]
    pub(super) fn prepare_after_with<F>(
        &self,
        predecessor: &Fragments,
        prefix: u64,
        changes: Vec<Change<Key, Row>>,
        lane: BudgetLane,
        copy: F,
    ) -> Result<Fragments, MemoryError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
    {
        let budget = self
            .stores
            .first()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?
            .budget()
            .clone();
        self.plan_after(&budget, predecessor, prefix, changes, lane, usize::MAX)?
            .build_in_with(&budget, copy)
    }
    #[cfg(test)]
    pub(super) fn get_entry(&self, key: &Key) -> Option<&Entry<Key, Row>> {
        self.member(key)?.get_entry(key)
    }
    /// A group of one member over `store`, the member and producer both
    /// named by the store's identity: the fixtures' way of installing
    /// forged rows.
    #[cfg(test)]
    pub(super) fn single_from_store(store: RangeStore<Key, Row>) -> Result<Self, MemoryError> {
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
    /// before any root moves.
    #[allow(clippy::result_large_err)] // A refused set is returned whole, never dropped.
    pub(super) fn publish_recoverable(
        &mut self,
        fragments: Fragments,
    ) -> Result<(), (MemoryError, Fragments)> {
        if fragments.parts.len() != self.stores.len() {
            return Err((MemoryError::WrongRange, fragments));
        }
        let refusal =
            self.stores
                .iter()
                .zip(fragments.parts.iter())
                .find_map(|(store, fragment)| {
                    store.validate_chain(std::iter::once(&fragment.value)).err()
                });
        if let Some(error) = refusal {
            return Err((error, fragments));
        }
        // Every fragment passed the check publication repeats, so every root
        // moves. Should a member still refuse (an invariant failure), the
        // refused fragment and the unpublished ones after it are returned;
        // the ones before it stay published, as their stores now hold them.
        let Fragments { producer, parts } = fragments;
        let mut remaining = parts.into_iter();
        for store in self.stores.iter_mut() {
            let Some(fragment) = remaining.next() else {
                break;
            };
            if let Err((error, range)) = store.publish_recoverable(fragment.value) {
                let refused = RangesMember {
                    start: fragment.start,
                    value: range,
                };
                let mut rest = Vec::new();
                if rest.try_reserve_exact(remaining.rest.len()).is_ok() {
                    rest.extend(remaining.rest.by_ref());
                }
                return Err((
                    error,
                    Fragments {
                        producer,
                        parts: Parts {
                            first: refused,
                            rest,
                            _allocation: remaining._allocation.take(),
                        },
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
                .checked_mul(size_of::<Change<Key, Row>>())
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
        let (left, right) = store.split_where_with(
            |key| layout::affinity(key) >= at,
            boundary.id,
            id,
            lane,
            copy,
        )?;
        let stores_allocation = reserve::<RangeStore<Key, Row>>(&budget, next)?;
        let layout_allocation = reserve::<RangeBoundary>(&budget, next)?;
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
        let stores_allocation = reserve::<RangeStore<Key, Row>>(&budget, count)?;
        let layout_allocation = reserve::<RangeBoundary>(&budget, count)?;
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
    fn route(&self, key: &Key) -> usize {
        let affinity = layout::affinity(key);
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
        array::<RangesMember<RangePreparationPlan<'static, Key, Row>>>(further)?,
        array::<RangesMember<PreparedRange<Key, Row>>>(further)?,
    )
}

/// One write's plans, one per member in member order. Planning a group of
/// one allocates nothing; a wider group holds its further plans in one
/// vector charged to the budget the write builds from.
pub(super) struct RangesPlan<'a> {
    producer: RangeId,
    lane: BudgetLane,
    plans: Parts<RangePreparationPlan<'a, Key, Row>>,
}

impl<'a> RangesPlan<'a> {
    /// The changed keys in key order across members.
    pub(super) fn changes(&self) -> impl Iterator<Item = &Change<Key, Row>> {
        self.plans
            .iter()
            .flat_map(|member| member.value.changes().iter())
    }
    pub(super) fn changes_len(&self) -> usize {
        self.plans
            .iter()
            .map(|member| member.value.changes().len())
            .sum()
    }
    /// The input bytes every member's plan holds, summed.
    pub(super) fn input_pending_bytes(&self) -> usize {
        self.plans
            .iter()
            .map(|member| member.value.charges().input_pending_bytes())
            .fold(0, usize::saturating_add)
    }
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
    /// Build every fragment with the input already funded by `input`, which
    /// is divided among the members by each plan's input bytes.
    pub(super) fn build_in_funded_with<F>(
        self,
        source: &MemoryBudget,
        mut input: Allocation,
        mut copy: F,
    ) -> Result<Fragments, MemoryError>
    where
        F: FnMut(&Row) -> Result<Row, MemoryError>,
    {
        self.build_each(source, |plan| {
            let part = input.split_off(plan.charges().input_pending_bytes())?;
            plan.build_in_funded_with(source, part, &mut copy)
        })
    }
    fn build_each(
        self,
        source: &MemoryBudget,
        mut build: impl FnMut(
            RangePreparationPlan<'a, Key, Row>,
        ) -> Result<PreparedRange<Key, Row>, MemoryError>,
    ) -> Result<Fragments, MemoryError> {
        let members = self.plans.len();
        let (mut rest, allocation) = Parts::reserve_rest(members, source, self.lane)?;
        let mut plans = self.plans.into_iter();
        let first = plans
            .next()
            .ok_or(MemoryError::InvalidConfiguration("empty range group"))?;
        let first = RangesMember {
            start: first.start,
            value: build(first.value)?,
        };
        for member in plans {
            if rest.len() >= rest.capacity() {
                return Err(MemoryError::WrongRange);
            }
            rest.push(RangesMember {
                start: member.start,
                value: build(member.value)?,
            });
        }
        Ok(Fragments {
            producer: self.producer,
            parts: Parts {
                first,
                rest,
                _allocation: allocation,
            },
        })
    }
}

/// One unpublished write over every member: a fragment per member at the
/// next prefix, published together or not at all.
pub struct Fragments {
    producer: RangeId,
    parts: Parts<PreparedRange<Key, Row>>,
}

impl std::fmt::Debug for Fragments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fragments")
            .field("producer", &self.producer)
            .field("members", &self.parts.len())
            .field("base", &self.base_prefix())
            .finish_non_exhaustive()
    }
}

impl Fragments {
    pub(super) fn id(&self) -> RangeId {
        self.producer
    }
    pub(super) fn prefix(&self) -> u64 {
        self.parts.first.value.prefix()
    }
    pub(super) fn base_prefix(&self) -> u64 {
        self.parts.first.value.base_prefix()
    }
    pub(super) fn len(&self) -> usize {
        self.parts.iter().map(|part| part.value.len()).sum()
    }
    /// The positions (in layout order) of the members this set writes.
    pub(super) fn touched_members(&self) -> impl Iterator<Item = usize> + '_ {
        self.parts
            .iter()
            .enumerate()
            .filter(|(_, part)| !part.value.is_empty())
            .map(|(index, _)| index)
    }
    fn part(&self, index: usize) -> Option<&PreparedRange<Key, Row>> {
        self.parts.get(index).map(|part| &part.value)
    }
    pub(super) fn get(&self, key: &Key) -> Option<&Row> {
        self.part(self.parts.route(key))?.get(key)
    }
    #[cfg(test)]
    pub(super) fn entries(&self) -> impl Iterator<Item = &Entry<Key, Row>> {
        self.parts.iter().flat_map(|part| part.value.entries())
    }
    pub(super) fn entries_from<'a>(
        &'a self,
        key: &Key,
        exclusive: bool,
    ) -> impl Iterator<Item = &'a Entry<Key, Row>> + use<'a> {
        let index = self.parts.route(key);
        let first = self
            .part(index)
            .map(|part| part.entries_from(key, exclusive));
        let rest = self
            .parts
            .iter()
            .skip(index.saturating_add(1))
            .flat_map(|part| part.value.entries());
        first.into_iter().flatten().chain(rest)
    }
    /// Check `next` is the exact successor of this set on every member.
    pub(super) fn validate_successor(&self, next: &Self) -> Result<(), MemoryError> {
        if self.parts.len() != next.parts.len() {
            return Err(MemoryError::WrongRange);
        }
        for (mine, theirs) in self.parts.iter().zip(next.parts.iter()) {
            mine.value.validate_successor(&theirs.value)?;
        }
        Ok(())
    }
}

/// One read's leases: every member pinned at one prefix.
pub struct RangeLeases {
    prefix: u64,
    /// The layout epoch the leases were taken under.
    epoch: u64,
    leases: Parts<SnapshotLease<Key, Row>>,
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
        project: impl FnOnce(&Entry<Key, Row>) -> R,
    ) -> Result<Option<R>, MemoryError> {
        let index = self.leases.route(start);
        let end_affinity = layout::affinity(end);
        let mut project = Some(project);
        for (position, lease) in self.leases.iter().enumerate().skip(index) {
            let (from, exclusive) = if position == index {
                (Some(start), exclusive)
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
                .project_from(from, exclusive, end, now, |entry| {
                    project.take().map(|project| project(entry))
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
