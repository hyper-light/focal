//! Dependency-ordered hydration without exposing an intermediate owner.
use super::*;

#[path = "range_hydration_sources.rs"]
mod sources;
pub use sources::RangeHydrationSource;

#[cfg(test)]
#[path = "range_hydration_phased_tests.rs"]
mod tests;

/// Decoder-derived bounds, not additional deployment configuration. The exact
/// retained row count must come from the checked checkpoint/import frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeHydrationLimits {
    pub expected_entries: usize,
    pub max_phases: usize,
}

/// An unpublished owner whose only row access is through scoped callbacks.
/// Consuming phase operations discard the entire import on any refusal.
/// No root handles, snapshots, pins or intermediate prefixes are exposed.
pub struct RangeHydration<K, V> {
    store: RangeStore<K, V>,
    limits: RangeHydrationLimits,
    phases: usize,
}

/// Borrowed dependency lookup during one row's construction. It includes
/// earlier phases and earlier rows of the current phase, including staged rows.
/// Every returned value remains immutable and its borrow cannot survive the
/// construction callback. Lookups allocate nothing. The decoder must meter
/// its lookup count, key comparison costs and work performed on returned data.
pub struct RangeHydrationLookup<'a, K, V> {
    root: &'a Root<K, V>,
    staged: &'a [Change<K, V>],
}

impl<'a, K: Ord, V> RangeHydrationLookup<'a, K, V> {
    pub fn get(&self, key: &K) -> Option<&'a V> {
        self.get_entry(key).map(|entry| &entry.value)
    }

    pub fn get_entry(&self, key: &K) -> Option<&'a Entry<K, V>> {
        let staged = self
            .staged
            .binary_search_by(|entry| entry.key().cmp(key))
            .ok()
            .and_then(|index| self.staged.get(index));
        match staged {
            Some(Change::Put(entry)) => Some(entry),
            // The constructor only stages insertions. Treating a deletion as
            // absence keeps this lookup conservative if that ever changes.
            Some(Change::Delete(_)) => None,
            None => self.root.get(key),
        }
    }
}

/// The complete but unpublished root supplied to the final validator. This
/// read view cannot produce an owner, snapshot, pin or shared root handle.
/// Reads allocate nothing; the validator separately bounds iteration, lookup,
/// parsing, hashing and any temporary workspace it owns.
pub struct RangeHydrationView<'a, K, V> {
    root: &'a Root<K, V>,
}

impl<K: Ord, V> RangeHydrationView<'_, K, V> {
    pub fn len(&self) -> usize {
        self.root.len
    }

    pub fn is_empty(&self) -> bool {
        self.root.len == 0
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.get_entry(key).map(|entry| &entry.value)
    }

    pub fn get_entry(&self, key: &K) -> Option<&Entry<K, V>> {
        self.root.get(key)
    }

    pub fn entries(&self) -> impl Iterator<Item = &Entry<K, V>> {
        self.root.from(0, 0)
    }

    /// Ordered traversal at/after the key, excluding equality when requested.
    /// The iterator borrows the view, not the temporary search key.
    pub fn entries_from<'a>(
        &'a self,
        key: &K,
        exclusive: bool,
    ) -> impl Iterator<Item = &'a Entry<K, V>> + use<'a, K, V> {
        let (page, offset) = self.root.seek(Some(key), exclusive);
        self.root.from(page, offset)
    }
}

impl<K: Ord + Clone, V> RangeStore<K, V> {
    /// Begin dependency-ordered restoration into a detached, empty owner.
    /// Partition semantics and fallible copying match
    /// [`Self::from_entry_plans_partitioned_with`]. Only a successful
    /// [`RangeHydration::finish`] makes the owner available to the caller.
    pub fn begin_hydration_partitioned(
        id: RangeId,
        config: RangeConfig,
        budget: MemoryBudget,
        partition: fn(&K) -> u64,
        limits: RangeHydrationLimits,
    ) -> Result<RangeHydration<K, V>, MemoryError> {
        if limits.max_phases == 0 {
            return Err(MemoryError::InvalidConfiguration(
                "hydration phase limit must be nonzero",
            ));
        }
        Ok(RangeHydration {
            store: Self::new_partitioned(id, 0, config, budget, partition)?,
            limits,
            phases: 0,
        })
    }
}

impl<K: Ord + Clone, V> RangeHydration<K, V> {
    /// Insert one dependency phase with exactly `expected_entries` plans.
    /// Keys must be strictly increasing within the phase and absent from all
    /// earlier phases; phase order need not follow key order. There are no
    /// updates or deletions. Dependencies can refer to earlier phase rows or
    /// earlier rows in this phase, independently of chunk/page boundaries.
    ///
    /// At most `min(page_entries, max_batch_entries)` owned rows are staged.
    /// Every final key/value heap quote is reserved before invoking `build`,
    /// and actual capacity charges are reconciled before another row is built.
    /// Builders and source adapters obey the same capacity/workspace contract
    /// as [`RangeStore::from_entry_plans_partitioned_with`]. In particular,
    /// source keys/plans and any dependency index remain caller-accounted until
    /// the row allowance is acquired. No whole-import array is constructed.
    /// Copy callbacks preserve meaning and obey [`RangeStore::prepare_batch_with`].
    ///
    /// Exact cardinality and total phase/row limits are enforced by this owner.
    /// The decoder must additionally bound source preparation, parser work,
    /// callback visits, nested comparisons and separately owned workspace.
    pub fn insert_phase<P, B, F>(
        mut self,
        expected_entries: usize,
        entries: impl IntoIterator<Item = Result<Entry<K, P>, MemoryError>>,
        mut build: B,
        mut copy: F,
    ) -> Result<Self, MemoryError>
    where
        B: for<'a> FnMut(
            RangeHydrationLookup<'a, K, V>,
            &K,
            P,
            usize,
        ) -> Result<(V, usize), MemoryError>,
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        if self.phases >= self.limits.max_phases {
            return Err(MemoryError::Capacity {
                requested: checked_add(self.phases, 1)?,
                available: self.limits.max_phases,
            });
        }
        let expected_total = checked_add(self.store.len(), expected_entries)?;
        if expected_total > self.limits.expected_entries {
            return Err(MemoryError::Capacity {
                requested: expected_total,
                available: self.limits.expected_entries,
            });
        }
        let config = self.store.config;
        let partition = self.store.partition.ok_or(MemoryError::WrongRange)?;
        let chunk_limit = config.page_entries.min(config.max_batch_entries);
        let mut source = entries.into_iter().peekable();
        let mut constructed = 0;
        while constructed < expected_entries {
            let _staging = self.store.budget.reserve(
                BudgetKind::Pending,
                BudgetLane::Completion,
                checked_add(
                    checked_mul(2, ALLOCATOR_OVERHEAD)?,
                    checked_mul(
                        chunk_limit,
                        checked_add(size_of::<Change<K, V>>(), size_of::<Allocation>())?,
                    )?,
                )?,
            )?;
            let mut payloads = bounded_vec(chunk_limit)?;
            // Values must be destroyed before their row and staging debits.
            let mut chunk: Vec<Change<K, V>> = bounded_vec(chunk_limit)?;
            let mut heap_bytes = 0;
            while chunk.len() < chunk_limit && constructed < expected_entries {
                let plan = match source.peek() {
                    Some(Ok(plan)) => plan,
                    Some(Err(error)) => return Err(error.clone()),
                    None => return Err(cardinality()),
                };
                if self.store.root.get(&plan.key).is_some() {
                    return Err(MemoryError::InvalidConfiguration(
                        "hydration phase key already exists",
                    ));
                }
                if chunk
                    .last()
                    .is_some_and(|previous| previous.key() >= &plan.key)
                {
                    return Err(order());
                }
                layout::check_entry::<K, V>(plan.heap_bytes, config)?;
                let next_heap = checked_add(heap_bytes, plan.heap_bytes)?;
                let next_charge = page_charge::<K, V>(checked_add(chunk.len(), 1)?, next_heap)?;
                let boundary = chunk
                    .last()
                    .is_some_and(|previous| partition(previous.key()) != partition(&plan.key));
                if !chunk.is_empty() && (boundary || next_charge > config.page_bytes) {
                    break;
                }
                let allowance = plan.heap_bytes;
                let mut allocation = self
                    .store
                    .budget
                    .reserve(BudgetKind::Pending, BudgetLane::Completion, allowance)?
                    .commit();
                let Entry {
                    key, value: plan, ..
                } = source.next().ok_or_else(cardinality)??;
                let dependencies = RangeHydrationLookup {
                    root: &self.store.root,
                    staged: &chunk,
                };
                let (value, actual) = build(dependencies, &key, plan, allowance)?;
                let entry = Entry::new(key, value, actual);
                if actual > allowance {
                    return Err(MemoryError::Capacity {
                        requested: actual,
                        available: allowance,
                    });
                }
                allocation.shrink_to(actual)?;
                heap_bytes = checked_add(heap_bytes, actual)?;
                if chunk.len() >= chunk.capacity() || payloads.len() >= payloads.capacity() {
                    return Err(MemoryError::Capacity {
                        requested: checked_add(chunk.len(), 1)?,
                        available: chunk.capacity().min(payloads.capacity()),
                    });
                }
                // No fallible work separates the owned entry and its permit.
                payloads.push(allocation);
                chunk.push(Change::Put(entry));
                constructed = checked_add(constructed, 1)?;
                if page_charge::<K, V>(chunk.len(), heap_bytes)? > config.page_bytes {
                    break;
                }
            }
            // Check ordering across the chunk boundary while its last key is
            // still owned here. No cloned high-water key or key index is needed.
            match source.peek() {
                Some(Ok(_)) if constructed == expected_entries => {
                    return Err(cardinality());
                }
                Some(Ok(next)) => {
                    if chunk.last().is_some_and(|last| last.key() >= &next.key) {
                        return Err(order());
                    }
                }
                Some(Err(error)) => return Err(error.clone()),
                None if constructed != expected_entries => return Err(cardinality()),
                None => {}
            }
            let next = self
                .store
                .prefix()
                .checked_add(1)
                .ok_or(MemoryError::CounterExhausted(
                    "hydration construction prefix",
                ))?;
            let prepared =
                self.store
                    .prepare_batch_with(next, chunk, BudgetLane::Completion, &mut copy)?;
            self.store.publish(prepared)?;
        }
        // This also checks empty phases without allocating a staging chunk.
        match source.next() {
            Some(Err(error)) => return Err(error),
            Some(Ok(_)) => return Err(cardinality()),
            None => {}
        }
        if self.store.len() != expected_total {
            return Err(cardinality());
        }
        self.phases = checked_add(self.phases, 1)?;
        Ok(self)
    }

    /// Validate the entire detached root and only then bind its verified
    /// checkpoint prefix. The caller proves record provenance, history and
    /// cross-row consistency in `validate`; the memory engine cannot infer
    /// those application invariants from row keys or a numeric prefix.
    /// A refused validation consumes and drops every restored row and permit.
    pub fn finish<F>(
        mut self,
        verified_prefix: u64,
        validate: F,
    ) -> Result<RangeStore<K, V>, MemoryError>
    where
        F: for<'a> FnOnce(RangeHydrationView<'a, K, V>) -> Result<(), MemoryError>,
    {
        if self.store.len() != self.limits.expected_entries {
            return Err(cardinality());
        }
        validate(RangeHydrationView {
            root: &self.store.root,
        })?;
        let root = Arc::get_mut(&mut self.store.root).ok_or(MemoryError::WrongRange)?;
        root.prefix = verified_prefix;
        Ok(self.store)
    }
}

fn cardinality() -> MemoryError {
    MemoryError::InvalidConfiguration("hydration phase or final row count differs from its frame")
}

fn order() -> MemoryError {
    MemoryError::InvalidConfiguration("hydration phase entries must be strictly ordered")
}
