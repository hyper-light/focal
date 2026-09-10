use crate::range::Root;
use crate::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, Entry, MemoryBudget, MemoryError,
    RangeConfig, RangeId, checked_add, checked_mul,
};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU64, Ordering},
};

pub(crate) struct LeaseState<K, V> {
    pub root: Arc<Root<K, V>>,
    pub id: u64,
    pub expires_at: u64,
    pub clock: Arc<AtomicU64>,
    pub budget: MemoryBudget,
    pub config: RangeConfig,
    pub _allocation: Arc<Allocation>,
}

/// Weak capability for a fixed prefix. Its owner registry, rather than this
/// caller-held handle, retains pages. No read can borrow pages past the call.
pub struct SnapshotLease<K, V> {
    state: Weak<LeaseState<K, V>>,
    range: RangeId,
    id: u64,
    prefix: u64,
    expires_at: u64,
    // A Weak keeps the Arc control allocation alive after expiry. Retain its
    // metadata charge too, without retaining the root or any entry pages.
    _allocation: Arc<Allocation>,
}

impl<K, V> Clone for SnapshotLease<K, V> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            range: self.range,
            id: self.id,
            prefix: self.prefix,
            expires_at: self.expires_at,
            _allocation: Arc::clone(&self._allocation),
        }
    }
}

impl<K, V> std::fmt::Debug for SnapshotLease<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotLease")
            .field("range", &self.range)
            .field("id", &self.id)
            .field("prefix", &self.prefix)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl<K, V> SnapshotLease<K, V> {
    pub(crate) fn from_state(state: &Arc<LeaseState<K, V>>) -> Self {
        Self {
            state: Arc::downgrade(state),
            range: state.root.range,
            id: state.id,
            prefix: state.root.prefix,
            expires_at: state.expires_at,
            _allocation: Arc::clone(&state._allocation),
        }
    }

    pub fn range_id(&self) -> RangeId {
        self.range
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn prefix(&self) -> u64 {
        self.prefix
    }
    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }

    pub(crate) fn matches(&self, state: &Arc<LeaseState<K, V>>) -> bool {
        self.state.ptr_eq(&Arc::downgrade(state))
    }

    pub(crate) fn checked_state(&self, now: u64) -> Result<Arc<LeaseState<K, V>>, MemoryError> {
        let state = self.state.upgrade().ok_or(MemoryError::LeaseExpired)?;
        // A stale clock supplied by a delayed reader cannot revive an expired
        // lease. Readers advance the common clock monotonically with fetch_max.
        let effective_now = state.clock.fetch_max(now, Ordering::AcqRel).max(now);
        if effective_now >= state.expires_at {
            return Err(MemoryError::LeaseExpired);
        }
        Ok(state)
    }
}

/// Per-response bounds. The server's range configuration caps every field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadBudget {
    pub max_items: usize,
    pub max_bytes: usize,
    pub max_edge_visits: usize,
}

impl Default for ReadBudget {
    fn default() -> Self {
        Self {
            max_items: 256,
            max_bytes: 256 * 1024,
            max_edge_visits: 1024,
        }
    }
}

impl ReadBudget {
    pub(crate) fn bounded(self, config: RangeConfig) -> Result<Self, MemoryError> {
        if self.max_items == 0 || self.max_bytes == 0 || self.max_edge_visits == 0 {
            return Err(MemoryError::InvalidConfiguration(
                "read budgets must be nonzero",
            ));
        }
        Ok(Self {
            max_items: self.max_items.min(config.max_query_items),
            max_bytes: self.max_bytes.min(config.max_query_bytes),
            max_edge_visits: self.max_edge_visits.min(config.max_traversal_edges),
        })
    }
}

/// Start inclusive, end exclusive. `heap_bytes` bounds dynamic storage of both
/// cloned query keys and is charged when a continuation retains the query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanQuery<K> {
    pub start: Option<K>,
    pub end: Option<K>,
    pub heap_bytes: usize,
}

impl<K> ScanQuery<K> {
    pub fn all() -> Self {
        Self {
            start: None,
            end: None,
            heap_bytes: 0,
        }
    }
}

/// An in-process continuation bound to exact range incarnation, lease, prefix,
/// and query. External protocols must encode/authenticate their own tokens;
/// these local handles are intentionally not serializable or forgeable.
pub struct ScanContinuation<K> {
    range: RangeId,
    lease: u64,
    prefix: u64,
    query: ScanQuery<K>,
    after: K,
    _allocation: Allocation,
}

impl<K> std::fmt::Debug for ScanContinuation<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanContinuation")
            .field("range", &self.range)
            .field("lease", &self.lease)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

/// Accounted cloned response, independent of pinned pages. Keeping a response
/// retains its memory charge even after the underlying snapshot expires.
pub struct ReadPage<K, V> {
    pub prefix: u64,
    items: Vec<Entry<K, V>>,
    pub continuation: Option<ScanContinuation<K>>,
    _allocation: Allocation,
}

impl<K, V> ReadPage<K, V> {
    pub fn items(&self) -> &[Entry<K, V>] {
        &self.items
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl<K, V> std::fmt::Debug for ReadPage<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadPage")
            .field("prefix", &self.prefix)
            .field("items", &self.items.len())
            .field("has_continuation", &self.continuation.is_some())
            .finish()
    }
}

impl<K: Ord, V> SnapshotLease<K, V> {
    /// Project one immutable entry without copying its potentially large value.
    /// The borrow cannot escape the callback. The caller accounts any owned
    /// projection; this method allocates no response buffer or continuation.
    pub fn project_next<R>(
        &self,
        start: &K,
        exclusive: bool,
        end: &K,
        now: u64,
        project: impl FnOnce(&Entry<K, V>) -> R,
    ) -> Result<Option<R>, MemoryError> {
        self.project_from(Some(start), exclusive, end, now, project)
    }

    /// As [`Self::project_next`]; `None` starts at the least key.
    pub fn project_from<R>(
        &self,
        start: Option<&K>,
        exclusive: bool,
        end: &K,
        now: u64,
        project: impl FnOnce(&Entry<K, V>) -> R,
    ) -> Result<Option<R>, MemoryError> {
        let state = self.checked_state(now)?;
        let (page, offset) = state.root.seek(start, exclusive);
        Ok(state
            .root
            .from(page, offset)
            .next()
            .filter(|entry| entry.key < *end)
            .map(project))
    }
}

impl<K: Ord + Clone, V: Clone> SnapshotLease<K, V> {
    pub fn get(&self, key: &K, now: u64) -> Result<ReadPage<K, V>, MemoryError> {
        let state = self.checked_state(now)?;
        let entry = state.root.get(key);
        let bytes = entry.map(Entry::read_bytes).transpose()?.unwrap_or(0);
        if bytes > state.config.max_query_bytes {
            return Err(MemoryError::ItemTooLarge {
                bytes,
                limit: state.config.max_query_bytes,
            });
        }
        let allocation = response_allocation::<K, V>(&state.budget, bytes)?;
        let mut items = Vec::new();
        if let Some(entry) = entry {
            items
                .try_reserve_exact(1)
                .map_err(|_| MemoryError::AllocationFailed)?;
            items.push(entry.clone());
        }
        Ok(ReadPage {
            prefix: state.root.prefix,
            items,
            continuation: None,
            _allocation: allocation,
        })
    }

    pub fn scan(
        &self,
        query: &ScanQuery<K>,
        budget: ReadBudget,
        continuation: Option<ScanContinuation<K>>,
        now: u64,
    ) -> Result<ReadPage<K, V>, MemoryError> {
        let state = self.checked_state(now)?;
        let budget = budget.bounded(state.config)?;
        if query
            .start
            .as_ref()
            .zip(query.end.as_ref())
            .is_some_and(|(start, end)| start > end)
        {
            return Err(MemoryError::InvalidConfiguration(
                "scan start is after its end",
            ));
        }
        if let Some(cursor) = &continuation {
            if cursor.range != self.range || cursor.lease != self.id || cursor.prefix != self.prefix
            {
                return Err(MemoryError::WrongLease);
            }
            if &cursor.query != query {
                return Err(MemoryError::QueryMismatch);
            }
        }
        let (page, offset) = match &continuation {
            Some(cursor) => state.root.seek(Some(&cursor.after), true),
            None => state.root.seek(query.start.as_ref(), false),
        };
        let mut count = 0;
        let mut bytes = 0;
        let mut last = None;
        let mut more = false;
        for entry in state.root.from(page, offset) {
            if query.end.as_ref().is_some_and(|end| &entry.key >= end) {
                break;
            }
            let next_bytes = checked_add(bytes, entry.read_bytes()?)?;
            if count == budget.max_items || next_bytes > budget.max_bytes {
                if count == 0 {
                    return Err(MemoryError::ItemTooLarge {
                        bytes: next_bytes,
                        limit: budget.max_bytes,
                    });
                }
                more = true;
                break;
            }
            count = checked_add(count, 1)?;
            bytes = next_bytes;
            last = Some(entry);
        }
        let allocation = response_allocation::<K, V>(&state.budget, bytes)?;
        let next = if more {
            let last = last.ok_or(MemoryError::MissingKey)?;
            let charge = checked_add(
                ALLOCATOR_OVERHEAD,
                checked_add(
                    size_of::<ScanContinuation<K>>(),
                    checked_add(query.heap_bytes, last.heap_bytes)?,
                )?,
            )?;
            if charge > state.config.max_continuation_bytes {
                return Err(MemoryError::Capacity {
                    requested: charge,
                    available: state.config.max_continuation_bytes,
                });
            }
            let cursor_allocation = state
                .budget
                .reserve(BudgetKind::Query, BudgetLane::Ordinary, charge)?
                .commit();
            Some(ScanContinuation {
                range: self.range,
                lease: self.id,
                prefix: self.prefix,
                query: query.clone(),
                after: last.key.clone(),
                _allocation: cursor_allocation,
            })
        } else {
            None
        };
        let mut items = Vec::new();
        items
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        items.extend(state.root.from(page, offset).take(count).cloned());
        Ok(ReadPage {
            prefix: state.root.prefix,
            items,
            continuation: next,
            _allocation: allocation,
        })
    }
}

pub(crate) fn response_allocation<K, V>(
    budget: &MemoryBudget,
    entry_bytes: usize,
) -> Result<Allocation, MemoryError> {
    budget
        .reserve(
            BudgetKind::Query,
            BudgetLane::Ordinary,
            checked_add(
                ALLOCATOR_OVERHEAD,
                checked_add(size_of::<ReadPage<K, V>>(), entry_bytes)?,
            )?,
        )
        .map(|r| r.commit())
}

pub(crate) fn entry_vector_charge<K, V>(count: usize) -> Result<usize, MemoryError> {
    checked_add(
        ALLOCATOR_OVERHEAD,
        checked_mul(count, size_of::<Entry<K, V>>())?,
    )
}
