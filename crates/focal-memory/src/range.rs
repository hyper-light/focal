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
    pub pages: Vec<Arc<Page<K, V>>>,
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
        self.pages
            .partition_point(|page| page.entries.first().is_some_and(|entry| &entry.key <= key))
            .saturating_sub(1)
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
            .iter()
            .skip(page)
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

/// Sorted, chunked COW map. A mutation copies only touched entry pages and
/// creates a new page directory; complete batches publish by one root swap.
/// The directory is currently linear in page count. This deliberately simple
/// implementation does not claim a constant-cost persistent tree directory.
pub struct RangeStore<K, V> {
    pub(crate) root: Arc<Root<K, V>>,
    pub(crate) budget: MemoryBudget,
    pub(crate) config: RangeConfig,
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
}

impl<K: Ord + Clone, V> RangeStore<K, V> {
    pub fn new(
        id: RangeId,
        initial_prefix: u64,
        config: RangeConfig,
        budget: MemoryBudget,
    ) -> Result<Self, MemoryError> {
        let config = config.validate()?;
        let charge = root_charge::<K, V>(0)?;
        let allocation = budget
            .reserve(BudgetKind::Roots, BudgetLane::Ordinary, charge)?
            .commit();
        Ok(Self {
            root: Arc::new(Root {
                owner: crate::OwnerId::new()?,
                range: id,
                prefix: initial_prefix,
                pages: Vec::new(),
                len: 0,
                _allocation: allocation,
            }),
            budget,
            config,
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
        mut copy: F,
    ) -> Result<PreparedRange<K, V>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        self.prepare_root_with(&self.root, prefix, changes, lane, &mut copy)
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
        mut copy: F,
    ) -> Result<PreparedRange<K, V>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        if predecessor.root.owner != self.root.owner {
            return Err(MemoryError::WrongRange);
        }
        self.prepare_root_with(&predecessor.root, prefix, changes, lane, &mut copy)
    }

    fn prepare_root_with<F>(
        &self,
        base: &Arc<Root<K, V>>,
        prefix: u64,
        mut changes: Vec<Change<K, V>>,
        lane: BudgetLane,
        copy: &mut F,
    ) -> Result<PreparedRange<K, V>, MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        let expected = base
            .prefix
            .checked_add(1)
            .ok_or(MemoryError::CounterExhausted("published prefix"))?;
        if prefix != expected {
            return Err(MemoryError::PrefixMismatch {
                expected,
                actual: prefix,
            });
        }
        if changes.len() > self.config.max_batch_entries {
            return Err(MemoryError::Capacity {
                requested: changes.len(),
                available: self.config.max_batch_entries,
            });
        }
        changes.sort_unstable_by(|a, b| a.key().cmp(b.key()));
        if changes
            .iter()
            .zip(changes.iter().skip(1))
            .any(|(left, right)| left.key() == right.key())
        {
            return Err(MemoryError::DuplicateKey);
        }
        let mut new_len = base.len;
        let mut pending_bytes = checked_add(
            ALLOCATOR_OVERHEAD,
            checked_mul(changes.capacity(), size_of::<Change<K, V>>())?,
        )?;
        for change in &changes {
            match change {
                Change::Put(entry) => {
                    pending_bytes = checked_add(pending_bytes, entry.heap_bytes)?;
                    if base.get(&entry.key).is_none() {
                        new_len = checked_add(new_len, 1)?;
                    }
                }
                Change::Delete(key) => {
                    let existing = base.get(key).ok_or(MemoryError::MissingKey)?;
                    // The existing entry's combined heap charge conservatively
                    // bounds a clone of its key supplied in this deletion.
                    pending_bytes = checked_add(pending_bytes, existing.heap_bytes)?;
                    new_len = new_len.checked_sub(1).ok_or(MemoryError::MissingKey)?;
                }
            }
        }
        let _pending = self
            .budget
            .reserve(BudgetKind::Pending, lane, pending_bytes)?;
        // Each insertion can add at most one extra page. Reserve the directory
        // before its allocation; unused directory capacity stays charged.
        let max_pages = checked_add(base.pages.len(), changes.len())?;
        let directory_charge =
            self.budget
                .reserve(BudgetKind::Roots, lane, root_charge::<K, V>(max_pages)?)?;
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(max_pages)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut changes = changes.into_iter();
        if base.pages.is_empty() {
            let count = changes.len();
            self.merge_page_with(&[], &mut changes, count, &mut pages, lane, copy)?;
        } else {
            for (page_index, page) in base.pages.iter().enumerate() {
                let next_first = base
                    .pages
                    .get(checked_add(page_index, 1)?)
                    .and_then(|page| page.entries.first())
                    .map(|entry| &entry.key);
                let count = changes.as_slice().partition_point(|change| {
                    next_first.is_none_or(|boundary| change.key() < boundary)
                });
                if count == 0 {
                    pages.push(Arc::clone(page));
                } else {
                    self.merge_page_with(
                        &page.entries,
                        &mut changes,
                        count,
                        &mut pages,
                        lane,
                        copy,
                    )?;
                }
            }
        }
        if !changes.as_slice().is_empty() {
            return Err(MemoryError::MissingKey);
        }
        let root = Root {
            owner: base.owner,
            range: self.id(),
            prefix,
            pages,
            len: new_len,
            _allocation: directory_charge.commit(),
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
        let mut store = Self::new(id, 0, config, budget)?;
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
            let mut chunk = Vec::new();
            chunk
                .try_reserve_exact(chunk_limit)
                .map_err(|_| MemoryError::AllocationFailed)?;
            let mut payloads = Vec::new();
            payloads
                .try_reserve_exact(chunk_limit)
                .map_err(|_| MemoryError::AllocationFailed)?;
            for _ in 0..chunk_limit {
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
            if next.root.range != self.id() || !Arc::ptr_eq(&next.base, base) {
                return Err(MemoryError::WrongRange);
            }
            if next.base.prefix != base.prefix {
                return Err(MemoryError::StalePreparation {
                    prepared_at: next.base.prefix,
                    current_prefix: base.prefix,
                });
            }
            if base.prefix.checked_add(1) != Some(next.root.prefix) {
                return Err(MemoryError::CounterExhausted("publication chain prefix"));
            }
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

    fn merge_page_with<F>(
        &self,
        old: &[Entry<K, V>],
        changes: &mut std::vec::IntoIter<Change<K, V>>,
        count: usize,
        pages: &mut Vec<Arc<Page<K, V>>>,
        lane: BudgetLane,
        copy: &mut F,
    ) -> Result<(), MemoryError>
    where
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        // Only one changed page's descriptors exist at once; no full range
        // clone or unbounded collected graph is built to plan an update.
        let selected = changes
            .as_slice()
            .get(..count)
            .ok_or(MemoryError::MissingKey)?;
        let max_entries = checked_add(old.len(), count)?;
        let staging_bytes = checked_add(
            ALLOCATOR_OVERHEAD,
            checked_mul(max_entries, size_of::<MergeEntry<'_, K, V>>())?,
        )?;
        let _staging = self
            .budget
            .reserve(BudgetKind::Pending, lane, staging_bytes)?;
        let mut merged = Vec::new();
        merged
            .try_reserve_exact(max_entries)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut existing = old.iter().peekable();
        for change in selected {
            while existing
                .peek()
                .is_some_and(|entry| &entry.key < change.key())
            {
                merged.push(MergeEntry::Retained(
                    existing.next().ok_or(MemoryError::MissingKey)?,
                ));
            }
            if existing
                .peek()
                .is_some_and(|entry| &entry.key == change.key())
            {
                existing.next();
            }
            if let Change::Put(entry) = change {
                merged.push(MergeEntry::Incoming {
                    heap_bytes: entry.heap_bytes,
                });
            }
        }
        merged.extend(existing.map(MergeEntry::Retained));
        // Planning only borrows old rows. The incoming descriptors carry a
        // charge, allowing each owned Put to move out of the input exactly once.
        let mut incoming = changes
            .by_ref()
            .take(count)
            .filter_map(|change| match change {
                Change::Put(entry) => Some(entry),
                Change::Delete(_) => None,
            });
        for chunk in merged.chunks(self.config.page_entries) {
            let mut bytes = checked_add(
                ALLOCATOR_OVERHEAD,
                checked_add(size_of::<Page<K, V>>(), checked_mul(2, size_of::<usize>())?)?,
            )?;
            bytes = checked_add(bytes, checked_mul(chunk.len(), size_of::<Entry<K, V>>())?)?;
            for entry in chunk {
                bytes = checked_add(bytes, entry.heap_bytes())?;
            }
            let allocation = self
                .budget
                .reserve(BudgetKind::Pages, lane, bytes)?
                .commit();
            let mut entries = Vec::new();
            entries
                .try_reserve_exact(chunk.len())
                .map_err(|_| MemoryError::AllocationFailed)?;
            for entry in chunk {
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
            pages.push(Arc::new(Page {
                entries,
                _allocation: allocation,
            }));
        }
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

fn root_charge<K, V>(capacity: usize) -> Result<usize, MemoryError> {
    checked_add(
        4 * ALLOCATOR_OVERHEAD,
        checked_add(
            checked_add(size_of::<Root<K, V>>(), checked_mul(2, size_of::<usize>())?)?,
            checked_mul(capacity, size_of::<Arc<Page<K, V>>>())?,
        )?,
    )
}
