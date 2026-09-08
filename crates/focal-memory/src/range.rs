use crate::snapshot::{LeaseState, SnapshotLease};
use crate::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, checked_add,
    checked_mul,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[path = "range_preflight.rs"]
mod preflight;
pub use preflight::{RangePreparationCharges, RangePreparationPlan};
#[path = "range_directory.rs"]
mod directory;
use directory::{DirectoryBuild, PageDirectory};
#[path = "range_envelope.rs"]
mod envelope;
pub use envelope::{RangeWriteEnvelope, RangeWriteLimits};
#[cfg(test)]
#[path = "range_cursor_tests.rs"]
mod cursor_tests;
#[cfg(test)]
#[path = "range_directory_tests.rs"]
mod directory_tests;
#[cfg(test)]
#[path = "range_envelope_tests.rs"]
mod envelope_tests;
#[cfg(test)]
#[path = "range_funded_input_tests.rs"]
mod funded_input_tests;
#[cfg(test)]
#[path = "range_funding_tests.rs"]
mod funding_tests;
#[path = "range_groups.rs"]
mod groups;
#[path = "range_hydration.rs"]
mod hydration;
pub use hydration::{
    RangeHydration, RangeHydrationLimits, RangeHydrationLookup, RangeHydrationSource,
    RangeHydrationView,
};
#[cfg(test)]
#[path = "range_import_tests.rs"]
mod import_tests;
#[path = "range_layout.rs"]
mod layout;
#[cfg(test)]
#[path = "range_layout_model_tests.rs"]
mod layout_model_tests;
#[cfg(test)]
#[path = "range_layout_tests.rs"]
mod layout_tests;
#[cfg(test)]
#[path = "range_partition_tests.rs"]
mod partition_tests;
#[cfg(test)]
#[path = "range_preflight_tests.rs"]
mod preflight_tests;
#[cfg(test)]
#[path = "range_successor_tests.rs"]
mod successor_tests;

/// Process-local incarnation, distinct after restoration or ownership transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RangeId(pub u128);

/// Heap charge includes dynamic key/value capacities and their allocator
/// overhead. Inline entry bytes are automatically added by the engine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry<K, V> {
    pub key: K,
    pub value: V,
    pub heap_bytes: usize,
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V, heap_bytes: usize) -> Self {
        Self {
            key,
            value,
            heap_bytes,
        }
    }
    pub(crate) fn read_bytes(&self) -> Result<usize, MemoryError> {
        checked_add(size_of::<Self>(), self.heap_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change<K, V> {
    Put(Entry<K, V>),
    Delete(K),
}

impl<K, V> Change<K, V> {
    pub fn key(&self) -> &K {
        match self {
            Self::Put(entry) => &entry.key,
            Self::Delete(key) => key,
        }
    }
}

/// Internal resource settings derived by the node from its resource allowance.
/// They are not a mandatory user-facing tuning surface.
#[derive(Clone, Copy, Debug)]
pub struct RangeConfig {
    pub page_entries: usize,
    /// Full accounting charge of an ordinary leaf, including its entry vector,
    /// Page/Arc bookkeeping, inline entries and caller-declared key/value heap.
    /// A larger admitted entry occupies an isolated singleton leaf.
    pub page_bytes: usize,
    /// Maximum inline Entry plus caller-declared key/value heap, excluding the
    /// page's own bookkeeping. Checked before construction and during import.
    pub max_entry_bytes: usize,
    pub max_batch_entries: usize,
    pub max_snapshot_leases: usize,
    pub max_snapshot_ttl: u64,
    pub max_query_items: usize,
    pub max_query_bytes: usize,
    pub max_traversal_depth: u32,
    pub max_traversal_nodes: usize,
    pub max_traversal_edges: usize,
    pub max_continuation_bytes: usize,
}

impl Default for RangeConfig {
    fn default() -> Self {
        Self {
            page_entries: 128,
            page_bytes: usize::MAX,
            max_entry_bytes: usize::MAX,
            max_batch_entries: 4096,
            max_snapshot_leases: 256,
            max_snapshot_ttl: 30_000,
            max_query_items: 1024,
            max_query_bytes: 1024 * 1024,
            max_traversal_depth: 64,
            max_traversal_nodes: 16_384,
            max_traversal_edges: 65_536,
            max_continuation_bytes: 4 * 1024 * 1024,
        }
    }
}

impl RangeConfig {
    fn validate(self) -> Result<Self, MemoryError> {
        if self.page_entries == 0
            || self.page_bytes == 0
            || self.max_entry_bytes == 0
            || self.max_batch_entries == 0
            || self.max_snapshot_leases == 0
            || self.max_snapshot_ttl == 0
            || self.max_query_items == 0
            || self.max_query_bytes == 0
            || self.max_traversal_nodes == 0
            || self.max_traversal_edges == 0
            || self.max_continuation_bytes == 0
        {
            return Err(MemoryError::InvalidConfiguration(
                "range limits must be nonzero",
            ));
        }
        Ok(self)
    }
}

pub(crate) struct Page<K, V> {
    pub entries: Vec<Entry<K, V>>,
    _allocation: Allocation,
}

// Field order keeps the complete owned input alive under its permit on every
// early return, including failure before the first page is allocated.
struct PendingChanges<K, V> {
    changes: std::vec::IntoIter<Change<K, V>>,
    _allocation: Allocation,
}

struct PartitionFunding<'a, K, P> {
    source: &'a MemoryBudget,
    lane: BudgetLane,
    classify: fn(&K) -> P,
}

// A merge plan retains no reference into the owned input iterator. This keeps
// descriptors small while newly supplied values move directly into final pages.
enum MergeEntry<'a, K, V> {
    Retained(&'a Entry<K, V>),
    Incoming { heap_bytes: usize },
}

impl<K, V> MergeEntry<'_, K, V> {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Retained(entry) => entry.heap_bytes,
            Self::Incoming { heap_bytes } => *heap_bytes,
        }
    }
}

pub(crate) struct Root<K, V> {
    owner: crate::OwnerId,
    pub range: RangeId,
    pub prefix: u64,
    pub pages: PageDirectory<K, V>,
    pub len: usize,
    _allocation: Allocation,
}

impl<K: Ord, V> Root<K, V> {
    pub fn get(&self, key: &K) -> Option<&Entry<K, V>> {
        let page = self.pages.get(self.page_index(key))?;
        page.entries
            .binary_search_by(|entry| entry.key.cmp(key))
            .ok()
            .and_then(|index| page.entries.get(index))
    }

    pub fn page_index(&self, key: &K) -> usize {
        self.pages.page_index(key)
    }

    pub fn seek(&self, key: Option<&K>, exclusive: bool) -> (usize, usize) {
        let Some(key) = key else {
            return (0, 0);
        };
        let page_index = self.page_index(key);
        let Some(page) = self.pages.get(page_index) else {
            return (0, 0);
        };
        let offset = page.entries.partition_point(|entry| {
            if exclusive {
                &entry.key <= key
            } else {
                &entry.key < key
            }
        });
        (page_index, offset)
    }

    pub fn from(&self, page: usize, offset: usize) -> impl Iterator<Item = &Entry<K, V>> {
        self.pages
            .from(page)
            .enumerate()
            .flat_map(move |(index, page)| {
                page.entries
                    .iter()
                    .skip(if index == 0 { offset } else { 0 })
            })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeStats {
    pub prefix: u64,
    pub entries: usize,
    pub pages: usize,
    pub pinned_snapshots: usize,
    pub clock: u64,
}

/// Sorted COW map with byte-bounded entry pages and a persistent page directory.
/// Mutations copy affected entry pages and directory paths; complete batches
/// publish by one root swap. Reads and scans borrow immutable directory nodes.
pub struct RangeStore<K, V> {
    pub(crate) root: Arc<Root<K, V>>,
    pub(crate) budget: MemoryBudget,
    pub(crate) config: RangeConfig,
    partition: Option<fn(&K) -> u64>,
    pins: BTreeMap<u64, Arc<LeaseState<K, V>>>,
    next_lease: u64,
    clock: Arc<AtomicU64>,
}

/// Fully allocated, unpublished candidate. Admission can hold it across a
/// durable-log proposal and drop it on rejection. Only its originating owner
/// at the unchanged base root can publish it. Publication allocates nothing.
pub struct PreparedRange<K, V> {
    base: Arc<Root<K, V>>,
    root: Arc<Root<K, V>>,
}

impl<K: Ord, V> PreparedRange<K, V> {
    pub fn id(&self) -> RangeId {
        self.root.range
    }
    pub fn base_prefix(&self) -> u64 {
        self.base.prefix
    }
    pub fn prefix(&self) -> u64 {
        self.root.prefix
    }
    pub fn len(&self) -> usize {
        self.root.len
    }
    pub fn is_empty(&self) -> bool {
        self.root.len == 0
    }
    pub fn get(&self, key: &K) -> Option<&V> {
        self.root.get(key).map(|entry| &entry.value)
    }
    /// Complete ordered view of this unpublished prefix. Entries remain owned
    /// by the candidate and cannot be modified through the view.
    pub fn entries(&self) -> impl Iterator<Item = &Entry<K, V>> {
        self.root.from(0, 0)
    }

    /// Ordered entries at or after `key`, excluding equality when `exclusive`.
    /// The cursor borrows this exact unpublished prefix, not the search key;
    /// seeking and iteration allocate nothing and retain no extra root handles.
    /// The caller bounds the number of returned entries it consumes.
    pub fn entries_from<'a>(
        &'a self,
        key: &K,
        exclusive: bool,
    ) -> impl Iterator<Item = &'a Entry<K, V>> + use<'a, K, V> {
        let (page, offset) = self.root.seek(Some(key), exclusive);
        self.root.from(page, offset)
    }

    /// Check the exact next unpublished root without allocation or cloning any
    /// handles. Matching range IDs and prefixes alone do not admit a sibling
    /// branch or a reconstructed owner with otherwise identical contents.
    pub fn validate_successor(&self, next: &Self) -> Result<(), MemoryError> {
        next.check_base(&self.root)
    }

    fn check_base(&self, base: &Arc<Root<K, V>>) -> Result<(), MemoryError> {
        if self.root.range != base.range || !Arc::ptr_eq(&self.base, base) {
            return Err(MemoryError::WrongRange);
        }
        if self.base.prefix != base.prefix {
            return Err(MemoryError::StalePreparation {
                prepared_at: self.base.prefix,
                current_prefix: base.prefix,
            });
        }
        if base.prefix.checked_add(1) != Some(self.root.prefix) {
            return Err(MemoryError::CounterExhausted("publication chain prefix"));
        }
        Ok(())
    }
}

impl<K: Ord + Clone, V> RangeStore<K, V> {
    pub fn new(
        id: RangeId,
        initial_prefix: u64,
        config: RangeConfig,
        budget: MemoryBudget,
    ) -> Result<Self, MemoryError> {
        Self::new_with_partition(id, initial_prefix, config, budget, None)
    }

    /// Create an owner whose leaves never mix different key partitions.
    /// The classifier is fixed for this owner's lifetime and must be
    /// deterministic, allocation-free and independent of mutable state.
    /// Contiguous key namespaces can isolate small immutable rows from updates
    /// and insertions in neighboring namespaces without per-value sharing.
    /// Equal partition IDs may share a leaf; byte/count limits still apply.
    /// Restoring this owner requires the same classifier semantics.
    pub fn new_partitioned(
        id: RangeId,
        initial_prefix: u64,
        config: RangeConfig,
        budget: MemoryBudget,
        partition: fn(&K) -> u64,
    ) -> Result<Self, MemoryError> {
        Self::new_with_partition(id, initial_prefix, config, budget, Some(partition))
    }

    fn new_with_partition(
        id: RangeId,
        initial_prefix: u64,
        config: RangeConfig,
        budget: MemoryBudget,
        partition: Option<fn(&K) -> u64>,
    ) -> Result<Self, MemoryError> {
        let config = config.validate()?;
        layout::validate::<K, V>(config)?;
        let charge = root_charge::<K, V>(0)?;
        let allocation = budget
            .reserve(BudgetKind::Roots, BudgetLane::Ordinary, charge)?
            .commit();
        Ok(Self {
            root: Arc::new(Root {
                owner: crate::OwnerId::new()?,
                range: id,
                prefix: initial_prefix,
                pages: PageDirectory::new(),
                len: 0,
                _allocation: allocation,
            }),
            budget,
            config,
            partition,
            pins: BTreeMap::new(),
            next_lease: 1,
            clock: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn id(&self) -> RangeId {
        self.root.range
    }
    pub fn prefix(&self) -> u64 {
        self.root.prefix
    }
    pub fn len(&self) -> usize {
        self.root.len
    }
    pub fn is_empty(&self) -> bool {
        self.root.len == 0
    }
    pub fn get(&self, key: &K) -> Option<&V> {
        self.root.get(key).map(|entry| &entry.value)
    }
    pub fn get_entry(&self, key: &K) -> Option<&Entry<K, V>> {
        self.root.get(key)
    }
    pub fn entries(&self) -> impl Iterator<Item = &Entry<K, V>> {
        self.root.from(0, 0)
    }
    /// Ordered entries at or after `key`, excluding equality when `exclusive`.
    /// The cursor borrows this committed prefix, not the search key; seeking
    /// and iteration allocate nothing and retain no extra root handles.
    /// The caller bounds the number of returned entries it consumes.
    pub fn entries_from<'a>(
        &'a self,
        key: &K,
        exclusive: bool,
    ) -> impl Iterator<Item = &'a Entry<K, V>> + use<'a, K, V> {
        let (page, offset) = self.root.seek(Some(key), exclusive);
        self.root.from(page, offset)
    }
    pub fn stats(&self) -> RangeStats {
        RangeStats {
            prefix: self.prefix(),
            entries: self.len(),
            pages: self.root.pages.len(),
            pinned_snapshots: self.pins.len(),
            clock: self.clock.load(Ordering::Acquire),
        }
    }

    /// Applies the complete write set at precisely the next prefix. Empty
    /// batches advance ranges that were unaffected by a session-log entry.
    /// Duplicate writes and deletion of absent keys are caller/reducer errors.
    /// Every allocation remains provisional until the final root swap.
    pub fn apply_batch(
        &mut self,
        prefix: u64,
        changes: Vec<Change<K, V>>,
        lane: BudgetLane,
    ) -> Result<(), MemoryError>
    where
        V: Clone,
    {
        let prepared = self.prepare_batch(prefix, changes, lane)?;
        self.publish(prepared)
    }

    /// Reserve and construct every changed page before a durability decision.
    /// Neither point reads nor snapshot pins can observe this candidate.
    pub fn prepare_batch(
        &self,
        prefix: u64,
        changes: Vec<Change<K, V>>,
        lane: BudgetLane,
    ) -> Result<PreparedRange<K, V>, MemoryError>
    where
        V: Clone,
    {
        self.prepare_batch_with(prefix, changes, lane, |value| Ok(value.clone()))
    }

    /// Prepare owned changes without requiring values to implement `Clone`.
    /// Supplied `Put` values move into the candidate and never reach `copy`.
    /// Only retained entries in touched pages require a value copy; untouched
    /// pages remain shared with the original prefix.
    ///
    /// Before invoking `copy`, the engine reserves the entry's `heap_bytes`
    /// for its copied key and value. That recorded charge must conservatively
    /// cover their resulting heap capacities and allocator overhead. The copier
    /// must preserve the value's meaning without mutating its source, and must
    /// separately account any extra temporary workspace. An error discards the
    /// entire provisional candidate; published rows, roots and leases remain
    /// unchanged.
    pub fn prepare_batch_with<F>(
        &self,
        prefix: u64,
        changes: Vec<Change<K, V>>,
        lane: BudgetLane,
        copy: F,
    ) -> Result<PreparedRange<K, V>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        self.plan_batch(prefix, changes, lane, usize::MAX)?
            .build_with(copy)
    }

    /// Prepare a pipelined suffix against an unpublished predecessor. Publish
    /// the candidates in order; dropping a rejected suffix reclaims its pages.
    pub fn prepare_after(
        &self,
        predecessor: &PreparedRange<K, V>,
        prefix: u64,
        changes: Vec<Change<K, V>>,
        lane: BudgetLane,
    ) -> Result<PreparedRange<K, V>, MemoryError>
    where
        V: Clone,
    {
        self.prepare_after_with(
            predecessor,
            prefix,
            changes,
            lane,
            |value| Ok(value.clone()),
        )
    }

    /// Prepare against an unpublished predecessor using the same fallible copy
    /// and heap-charge contract as [`Self::prepare_batch_with`]. Every supplied
    /// value remains owned; the predecessor's published rows remain unchanged.
    pub fn prepare_after_with<F>(
        &self,
        predecessor: &PreparedRange<K, V>,
        prefix: u64,
        changes: Vec<Change<K, V>>,
        lane: BudgetLane,
        copy: F,
    ) -> Result<PreparedRange<K, V>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        self.plan_after(predecessor, prefix, changes, lane, usize::MAX)?
            .build_with(copy)
    }

    fn prepare_planned_with<F>(
        &self,
        funded: preflight::FundedPreparation<'_, K, V>,
        source: &MemoryBudget,
        copy: &mut F,
    ) -> Result<PreparedRange<K, V>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        let preflight::FundedPreparation { plan, input } = funded;
        let RangePreparationPlan {
            base,
            prefix,
            changes,
            lane,
            new_len,
            output_pages,
            charges,
            ..
        } = plan;
        let mut pending = PendingChanges {
            changes: changes.into_iter(),
            _allocation: input,
        };
        // The plan bounds cumulative node construction, but only actual node
        // allocations spend that allowance. Shared subtrees keep their permits.
        let root_bytes = root_charge::<K, V>(0)?;
        let root_allocation = source.reserve(BudgetKind::Roots, lane, root_bytes)?;
        let node_bound = charges
            .directory_bytes()
            .checked_sub(root_bytes)
            .ok_or(MemoryError::InvalidConfiguration("invalid directory quote"))?;
        let mut directory_build = DirectoryBuild::new(source, lane, node_bound);
        let mut pages = base.pages.shared();
        let changes = &mut pending.changes;
        let mut base_after = 0;
        let mut output_after = 0;
        while let Some(group) = groups::next(base, changes.as_slice())? {
            // Route using immutable base boundaries. Earlier groups may have
            // split or removed pages, so translate the rank past those edits.
            let untouched = group
                .rank
                .checked_sub(base_after)
                .ok_or(MemoryError::MissingKey)?;
            let rank = checked_add(output_after, untouched)?;
            let old_page = base.pages.get(group.rank);
            let selected = changes
                .as_slice()
                .get(..group.count)
                .ok_or(MemoryError::MissingKey)?;
            let reusable = old_page
                .map(|page| layout::reusable_page(page, selected, self.config, self.partition))
                .transpose()?
                .flatten();
            let mut merge = |old: &[Entry<K, V>], count, rank, replace| {
                let mut produced = 0;
                self.merge_page_with(old, changes, count, (source, lane), copy, &mut |page| {
                    let at = checked_add(rank, produced)?;
                    pages = if replace && produced == 0 {
                        pages.replace(at, page, &mut directory_build)?
                    } else {
                        pages.insert(at, page, &mut directory_build)?
                    };
                    produced = checked_add(produced, 1)?;
                    Ok(())
                })?;
                if replace && produced == 0 {
                    pages = pages.remove(rank, &mut directory_build)?;
                }
                Ok::<usize, MemoryError>(produced)
            };
            let produced = if let Some(split) = reusable {
                let before = merge(&[], split, rank, false)?;
                // The isolated page retains its existing allocation and rows.
                let through_shared = checked_add(before, 1)?;
                let after = merge(
                    &[],
                    group
                        .count
                        .checked_sub(split)
                        .ok_or(MemoryError::MissingKey)?,
                    checked_add(rank, through_shared)?,
                    false,
                )?;
                checked_add(through_shared, after)?
            } else {
                match old_page {
                    Some(page) => merge(&page.entries, group.count, rank, true)?,
                    None if base.pages.is_empty() => merge(&[], group.count, rank, false)?,
                    None => return Err(MemoryError::MissingKey),
                }
            };
            base_after = checked_add(group.rank, 1)?;
            output_after = checked_add(rank, produced)?;
        }
        if !changes.as_slice().is_empty() {
            return Err(MemoryError::MissingKey);
        }
        if pages.len() != output_pages {
            return Err(MemoryError::InvalidConfiguration(
                "range plan page count changed",
            ));
        }
        let root = Root {
            owner: base.owner,
            range: self.id(),
            prefix,
            pages,
            len: new_len,
            _allocation: root_allocation.commit(),
        };
        Ok(PreparedRange {
            base: Arc::clone(base),
            root: Arc::new(root),
        })
    }

    /// Rebuild a range at a verified checkpoint prefix from strictly ordered
    /// canonical entries. Only one bounded input chunk is staged at a time;
    /// construction prefixes remain private until the returned store exists.
    pub fn from_entries(
        id: RangeId,
        prefix: u64,
        config: RangeConfig,
        budget: MemoryBudget,
        entries: impl IntoIterator<Item = Entry<K, V>>,
    ) -> Result<Self, MemoryError>
    where
        V: Clone,
    {
        Self::from_entries_with_partition(id, prefix, config, budget, entries, None)
    }

    /// Restore strictly ordered entries using the same immutable classifier
    /// contract as [`Self::new_partitioned`]. Imported leaves honor both
    /// partition boundaries and the configured byte/count limits.
    pub fn from_entries_partitioned(
        id: RangeId,
        prefix: u64,
        config: RangeConfig,
        budget: MemoryBudget,
        entries: impl IntoIterator<Item = Entry<K, V>>,
        partition: fn(&K) -> u64,
    ) -> Result<Self, MemoryError>
    where
        V: Clone,
    {
        Self::from_entries_with_partition(id, prefix, config, budget, entries, Some(partition))
    }

    fn from_entries_with_partition(
        id: RangeId,
        prefix: u64,
        config: RangeConfig,
        budget: MemoryBudget,
        entries: impl IntoIterator<Item = Entry<K, V>>,
        partition: Option<fn(&K) -> u64>,
    ) -> Result<Self, MemoryError>
    where
        V: Clone,
    {
        let mut store = Self::new_with_partition(id, 0, config, budget, partition)?;
        let chunk_limit = config.page_entries.min(config.max_batch_entries);
        let mut source = entries.into_iter().peekable();
        while source.peek().is_some() {
            let _staging = store.budget.reserve(
                BudgetKind::Pending,
                BudgetLane::Completion,
                checked_add(
                    2 * ALLOCATOR_OVERHEAD,
                    checked_mul(
                        chunk_limit,
                        checked_add(size_of::<Change<K, V>>(), size_of::<crate::Reservation>())?,
                    )?,
                )?,
            )?;
            let mut payloads = bounded_vec(chunk_limit)?;
            // On every early return, staged values must drop before their
            // payload permits. The outer staging permit owns both Vec buffers.
            let mut chunk = bounded_vec(chunk_limit)?;
            let mut heap_bytes = 0;
            for _ in 0..chunk_limit {
                let Some(next_entry) = source.peek() else {
                    break;
                };
                layout::check_entry::<K, V>(next_entry.heap_bytes, config)?;
                let next_heap = checked_add(heap_bytes, next_entry.heap_bytes)?;
                let next_charge = page_charge::<K, V>(checked_add(chunk.len(), 1)?, next_heap)?;
                let boundary = partition.is_some_and(|classify| {
                    chunk.last().is_some_and(|previous: &Change<K, V>| {
                        classify(previous.key()) != classify(&next_entry.key)
                    })
                });
                if !chunk.is_empty() && (boundary || next_charge > config.page_bytes) {
                    break;
                }
                let Some(entry) = source.next() else {
                    break;
                };
                let previous = chunk.last().map(Change::key).or_else(|| {
                    store
                        .root
                        .pages
                        .last()
                        .and_then(|page| page.entries.last())
                        .map(|last| &last.key)
                });
                if previous.is_some_and(|key| &entry.key <= key) {
                    return Err(MemoryError::InvalidConfiguration(
                        "checkpoint entries must be strictly ordered",
                    ));
                }
                payloads.push(store.budget.reserve(
                    BudgetKind::Pending,
                    BudgetLane::Completion,
                    entry.heap_bytes,
                )?);
                chunk.push(Change::Put(entry));
                heap_bytes = next_heap;
                if next_charge > config.page_bytes {
                    // One admitted oversized row is staged alone. Following
                    // chunks can reuse its isolated page without copying it.
                    break;
                }
            }
            let next = store
                .prefix()
                .checked_add(1)
                .ok_or(MemoryError::CounterExhausted(
                    "checkpoint construction prefix",
                ))?;
            store.apply_batch(next, chunk, BudgetLane::Completion)?;
        }
        let root = Arc::get_mut(&mut store.root).ok_or(MemoryError::WrongRange)?;
        root.prefix = prefix;
        Ok(store)
    }

    /// Audit an entire ordered publication chain without mutation or allocation.
    pub fn validate_chain<'a>(
        &'a self,
        prepared: impl IntoIterator<Item = &'a PreparedRange<K, V>>,
    ) -> Result<(), MemoryError> {
        let mut base = &self.root;
        for next in prepared {
            next.check_base(base)?;
            base = &next.root;
        }
        Ok(())
    }

    /// Publish after durable commitment. The owner must serialize preparations
    /// across the log barrier; refusal leaves the published root unchanged.
    /// Use `publish_recoverable` to retain the refused candidate itself.
    pub fn publish(&mut self, prepared: PreparedRange<K, V>) -> Result<(), MemoryError> {
        self.publish_recoverable(prepared)
            .map_err(|(error, _)| error)
    }

    /// As `publish`, but returns ownership of a refused candidate. The caller
    /// can inspect or discard the complete candidate without losing its permits.
    pub fn publish_recoverable(
        &mut self,
        prepared: PreparedRange<K, V>,
    ) -> Result<(), (MemoryError, PreparedRange<K, V>)> {
        if prepared.root.range != self.id() {
            return Err((MemoryError::WrongRange, prepared));
        }
        if prepared.base.prefix != self.prefix() {
            let error = MemoryError::StalePreparation {
                prepared_at: prepared.base.prefix,
                current_prefix: self.prefix(),
            };
            return Err((error, prepared));
        }
        if !Arc::ptr_eq(&prepared.base, &self.root) {
            return Err((MemoryError::WrongRange, prepared));
        }
        self.root = prepared.root;
        Ok(())
    }

    fn merge_page_with<F, E>(
        &self,
        old: &[Entry<K, V>],
        changes: &mut std::vec::IntoIter<Change<K, V>>,
        count: usize,
        funding: (&MemoryBudget, BudgetLane),
        copy: &mut F,
        emit_page: &mut E,
    ) -> Result<(), MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
        E: FnMut(Arc<Page<K, V>>) -> Result<(), MemoryError>,
    {
        let (source, lane) = funding;
        match self.partition {
            Some(classify) => self.merge_partition_with(
                old,
                changes,
                count,
                PartitionFunding {
                    source,
                    lane,
                    classify,
                },
                copy,
                emit_page,
            ),
            None => self.merge_partition_with(
                old,
                changes,
                count,
                PartitionFunding {
                    source,
                    lane,
                    classify: |_: &K| (),
                },
                copy,
                emit_page,
            ),
        }
    }

    fn merge_partition_with<F, E, P: Copy + Eq>(
        &self,
        old: &[Entry<K, V>],
        changes: &mut std::vec::IntoIter<Change<K, V>>,
        count: usize,
        funding: PartitionFunding<'_, K, P>,
        copy: &mut F,
        emit_page: &mut E,
    ) -> Result<(), MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
        E: FnMut(Arc<Page<K, V>>) -> Result<(), MemoryError>,
    {
        let PartitionFunding {
            source,
            lane,
            classify,
        } = funding;
        // Only one changed page's descriptors exist at once; no full range
        // clone or unbounded collected graph is built to plan an update.
        let selected = changes
            .as_slice()
            .get(..count)
            .ok_or(MemoryError::MissingKey)?;
        let max_entries = checked_add(old.len(), count)?;
        let staging_bytes = merge_partition_charge::<K, V, P>(max_entries)?;
        let _staging = source.reserve(BudgetKind::Pending, lane, staging_bytes)?;
        let mut merged = bounded_vec(max_entries)?;
        visit_merged(old, selected, |key, entry| {
            bounded_push(&mut merged, (entry, classify(key)), max_entries)
        })?;
        // Planning only borrows old rows. The incoming descriptors carry a
        // charge, allowing each owned Put to move out of the input exactly once.
        let mut incoming = changes
            .by_ref()
            .take(count)
            .filter_map(|change| match change {
                Change::Put(entry) => Some(entry),
                Change::Delete(_) => None,
            });
        let mut partition = layout::LeafPartition::new::<K, V>(self.config)?;
        let mut emit = |span: layout::LeafSpan| {
            let end = checked_add(span.start, span.len)?;
            let chunk = merged.get(span.start..end).ok_or(MemoryError::MissingKey)?;
            let bytes = page_charge::<K, V>(span.len, span.heap)?;
            let allocation = source.reserve(BudgetKind::Pages, lane, bytes)?.commit();
            let mut entries = bounded_vec(chunk.len())?;
            for (entry, _) in chunk {
                entries.push(match entry {
                    MergeEntry::Retained(entry) => Entry {
                        key: entry.key.clone(),
                        value: copy(&entry.value)?,
                        heap_bytes: entry.heap_bytes,
                    },
                    MergeEntry::Incoming { .. } => {
                        incoming.next().ok_or(MemoryError::MissingKey)?
                    }
                });
            }
            emit_page(Arc::new(Page {
                entries,
                _allocation: allocation,
            }))?;
            Ok(())
        };
        let mut previous = None;
        for (entry, key) in &merged {
            let boundary = previous.is_some_and(|previous| previous != *key);
            partition.push(entry.heap_bytes(), boundary, &mut emit)?;
            previous = Some(*key);
        }
        partition.finish(&mut emit)?;
        // Drain trailing deletions belonging to this page. An unplanned Put
        // would indicate an incomplete merge and must never be published.
        if incoming.next().is_some() {
            return Err(MemoryError::MissingKey);
        }
        Ok(())
    }

    /// Explicit monotonic time drives expiry. No system clock is consulted.
    /// The runtime must call this on its bounded timer schedule even when idle.
    pub fn advance_clock(&mut self, now: u64) -> Result<usize, MemoryError> {
        let previous = self.clock.fetch_max(now, Ordering::AcqRel);
        if now < previous {
            return Err(MemoryError::ClockRegression {
                current: previous,
                supplied: now,
            });
        }
        let before = self.pins.len();
        self.pins.retain(|_, pin| pin.expires_at > now);
        Ok(before.saturating_sub(self.pins.len()))
    }

    /// Only this bounded owner registry holds strong read roots. The returned
    /// weak lease cannot retain old pages after expiry/release and maintenance.
    pub fn pin(&mut self, now: u64, ttl: u64) -> Result<SnapshotLease<K, V>, MemoryError> {
        self.advance_clock(now)?;
        if ttl == 0 || ttl > self.config.max_snapshot_ttl {
            return Err(MemoryError::InvalidConfiguration(
                "snapshot TTL exceeds allowed lifetime",
            ));
        }
        if self.pins.len() >= self.config.max_snapshot_leases {
            return Err(MemoryError::Capacity {
                requested: 1,
                available: 0,
            });
        }
        let expires_at = now
            .checked_add(ttl)
            .ok_or(MemoryError::CounterExhausted("snapshot expiry"))?;
        let next = self
            .next_lease
            .checked_add(1)
            .ok_or(MemoryError::CounterExhausted("snapshot lease ID"))?;
        let charge = checked_add(
            3 * ALLOCATOR_OVERHEAD,
            checked_add(
                checked_add(size_of::<LeaseState<K, V>>(), size_of::<Allocation>())?,
                checked_mul(
                    16,
                    checked_add(
                        size_of::<(u64, Arc<LeaseState<K, V>>)>(),
                        size_of::<usize>(),
                    )?,
                )?,
            )?,
        )?;
        let allocation = self
            .budget
            .reserve(BudgetKind::ReadPins, BudgetLane::Ordinary, charge)?
            .commit();
        let pin = Arc::new(LeaseState {
            root: Arc::clone(&self.root),
            id: self.next_lease,
            expires_at,
            clock: Arc::clone(&self.clock),
            budget: self.budget.clone(),
            config: self.config,
            _allocation: Arc::new(allocation),
        });
        let lease = SnapshotLease::from_state(&pin);
        self.pins.insert(self.next_lease, pin);
        self.next_lease = next;
        Ok(lease)
    }

    pub fn release(&mut self, lease: &SnapshotLease<K, V>) -> Result<(), MemoryError> {
        if lease.range_id() != self.id() {
            return Err(MemoryError::WrongLease);
        }
        let Some(pin) = self.pins.get(&lease.id()) else {
            return Err(MemoryError::LeaseExpired);
        };
        if !lease.matches(pin) {
            return Err(MemoryError::WrongLease);
        }
        self.pins.remove(&lease.id());
        Ok(())
    }
}

fn root_charge<K, V>(_capacity: usize) -> Result<usize, MemoryError> {
    // Root metadata is constant. Each persistent directory node carries its
    // own accounting permit; no flat page-handle buffer belongs to the root.
    checked_add(
        4 * ALLOCATOR_OVERHEAD,
        checked_add(size_of::<Root<K, V>>(), checked_mul(2, size_of::<usize>())?)?,
    )
}

// A checked reservation precedes every call. Allocator-reported excess capacity
// is refused and freed immediately; it is never admitted by reserving afterward.
fn bounded_vec<T>(capacity: usize) -> Result<Vec<T>, MemoryError> {
    if capacity == 0 {
        return Ok(Vec::new());
    }
    #[cfg(test)]
    let requested = preflight_tests::allocation_capacity(capacity)?;
    #[cfg(not(test))]
    let requested = capacity;
    let mut value = Vec::new();
    value
        .try_reserve_exact(requested)
        .map_err(|_| MemoryError::AllocationFailed)?;
    let actual_bytes = checked_mul(value.capacity(), size_of::<T>())?;
    let reserved_bytes = checked_mul(capacity, size_of::<T>())?;
    if actual_bytes > reserved_bytes {
        return Err(MemoryError::Capacity {
            requested: actual_bytes,
            available: reserved_bytes,
        });
    }
    Ok(value)
}

fn bounded_push<T>(values: &mut Vec<T>, value: T, limit: usize) -> Result<(), MemoryError> {
    if values.len() >= limit || values.len() >= values.capacity() {
        return Err(MemoryError::Capacity {
            requested: checked_add(values.len(), 1)?,
            available: limit,
        });
    }
    values.push(value);
    Ok(())
}

// Both preflight and construction visit the same ordered retained/incoming
// sequence. Preflight only adds counts and charges; it never copies a row.
fn visit_merged<'a, K: Ord, V>(
    old: &'a [Entry<K, V>],
    selected: &[Change<K, V>],
    mut visit: impl FnMut(&K, MergeEntry<'a, K, V>) -> Result<(), MemoryError>,
) -> Result<(), MemoryError> {
    let mut existing = old.iter().peekable();
    for change in selected {
        while existing
            .peek()
            .is_some_and(|entry| &entry.key < change.key())
        {
            let entry = existing.next().ok_or(MemoryError::MissingKey)?;
            visit(&entry.key, MergeEntry::Retained(entry))?;
        }
        if existing
            .peek()
            .is_some_and(|entry| &entry.key == change.key())
        {
            existing.next();
        }
        if let Change::Put(entry) = change {
            visit(
                &entry.key,
                MergeEntry::Incoming {
                    heap_bytes: entry.heap_bytes,
                },
            )?;
        }
    }
    for entry in existing {
        visit(&entry.key, MergeEntry::Retained(entry))?;
    }
    Ok(())
}

fn merge_charge<K, V>(capacity: usize) -> Result<usize, MemoryError> {
    merge_partition_charge::<K, V, ()>(capacity)
}

fn merge_layout_charge<K, V>(capacity: usize, partitioned: bool) -> Result<usize, MemoryError> {
    if partitioned {
        merge_partition_charge::<K, V, u64>(capacity)
    } else {
        merge_charge::<K, V>(capacity)
    }
}

fn merge_partition_charge<K, V, P>(capacity: usize) -> Result<usize, MemoryError> {
    if capacity == 0 {
        return Ok(0);
    }
    checked_add(
        ALLOCATOR_OVERHEAD,
        checked_mul(capacity, size_of::<(MergeEntry<'_, K, V>, P)>())?,
    )
}

fn page_charge<K, V>(entries: usize, heap_bytes: usize) -> Result<usize, MemoryError> {
    checked_add(
        checked_add(
            checked_mul(2, ALLOCATOR_OVERHEAD)?,
            checked_add(size_of::<Page<K, V>>(), checked_mul(2, size_of::<usize>())?)?,
        )?,
        checked_add(checked_mul(entries, size_of::<Entry<K, V>>())?, heap_bytes)?,
    )
}
