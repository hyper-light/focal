//! Model preparation may borrow already restored dependencies before the engine
//! acquires the exact final payload allowance. No borrowed plan crosses a page
//! publication, and only owned values enter the detached range.
use super::*;

#[cfg(test)]
#[path = "range_hydration_sources_tests.rs"]
mod tests;

/// One borrowed/raw row source with dependency-aware preparation and owned
/// construction. The source and its key must remain semantically immutable.
/// Source storage and preparation workspace, including any owned key in the
/// returned Entry, remain caller-funded until the engine acquires the quoted
/// complete key/value heap allowance. Preparation should allocate nothing when
/// possible. Every prepare/build call must debit the caller's enclosing work
/// allowance before parsing, hashing, dependency lookup or model validation.
///
/// The GAT plan may borrow this source and the scoped immutable dependency rows.
/// Build consumes that plan after the exact final allowance is held, and returns
/// V whose ownership is independent of those temporary borrows. Its actual heap
/// includes the final key, every value buffer/capacity and allocator bookkeeping.
/// Each allocation and actual capacity must fit the allowance before proceeding
/// to another allocation. Additional temporary build workspace is caller-funded.
pub trait RangeHydrationSource<K, V> {
    type Plan<'a>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    /// Stable key access performs no allocation and is separately work-bounded
    /// by the source. It allows order and partition checks before preparation.
    fn key(&self) -> &K;

    fn prepare<'a>(
        &'a self,
        dependencies: RangeHydrationLookup<'a, K, V>,
    ) -> Result<Entry<K, Self::Plan<'a>>, MemoryError>
    where
        K: 'a,
        V: 'a;

    fn build<'a>(plan: Self::Plan<'a>, allowance: usize) -> Result<(V, usize), MemoryError>
    where
        Self: 'a,
        K: 'a,
        V: 'a;
}

impl<K: Ord + Clone, V> RangeHydration<K, V> {
    /// Insert an exact-count phase whose final row quotes depend on restored
    /// model state. Order, disjointness, phase limits, precharges, owned capacity
    /// reconciliation and failure disposal match [`Self::insert_phase`].
    ///
    /// Each source prepares a scoped borrowed plan from preceding rows, then
    /// the engine reserves its complete heap quote before consuming the plan in
    /// `build`. Count and partition boundaries are detected before preparation.
    /// If a prepared row would cross an ordinary page's byte limit, its plan is
    /// dropped, the current chunk is installed, and that source is prepared once
    /// more against the equivalent installed dependencies. A source is prepared
    /// at most twice and built once. Charge every actual preparation pass; no
    /// plan may retain a borrow through a store mutation. Source/index/decoder
    /// workspace is independently funded and metered as required by the trait.
    pub fn insert_sources<S, F>(
        mut self,
        expected_entries: usize,
        entries: impl IntoIterator<Item = Result<S, MemoryError>>,
        mut copy: F,
    ) -> Result<Self, MemoryError>
    where
        S: RangeHydrationSource<K, V>,
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
            // Owned rows drop before their payload and staging allowances.
            let mut chunk: Vec<Change<K, V>> = bounded_vec(chunk_limit)?;
            let mut heap_bytes = 0;
            while chunk.len() < chunk_limit && constructed < expected_entries {
                let (entry, allocation, actual) = {
                    let item = match source.peek() {
                        Some(Ok(item)) => item,
                        Some(Err(error)) => return Err(error.clone()),
                        None => return Err(cardinality()),
                    };
                    let key = item.key();
                    if self.store.root.get(key).is_some() {
                        return Err(MemoryError::InvalidConfiguration(
                            "hydration phase key already exists",
                        ));
                    }
                    if chunk.last().is_some_and(|last| last.key() >= key) {
                        return Err(order());
                    }
                    if chunk
                        .last()
                        .is_some_and(|last| partition(last.key()) != partition(key))
                    {
                        break;
                    }
                    let prepared = item.prepare(RangeHydrationLookup {
                        root: &self.store.root,
                        staged: &chunk,
                    })?;
                    if &prepared.key != key {
                        return Err(MemoryError::InvalidConfiguration(
                            "hydration prepared key differs from source",
                        ));
                    }
                    layout::check_entry::<K, V>(prepared.heap_bytes, config)?;
                    let next_heap = checked_add(heap_bytes, prepared.heap_bytes)?;
                    let next_charge = page_charge::<K, V>(checked_add(chunk.len(), 1)?, next_heap)?;
                    if !chunk.is_empty() && next_charge > config.page_bytes {
                        // Drop all dependency borrows before publishing the chunk.
                        // The same source remains peeked for one re-preparation.
                        break;
                    }
                    let allowance = prepared.heap_bytes;
                    let mut allocation = self
                        .store
                        .budget
                        .reserve(BudgetKind::Pending, BudgetLane::Completion, allowance)?
                        .commit();
                    let Entry {
                        key,
                        value: plan,
                        heap_bytes: _,
                    } = prepared;
                    let (value, actual) = S::build(plan, allowance)?;
                    let entry = Entry::new(key, value, actual);
                    if actual > allowance {
                        return Err(MemoryError::Capacity {
                            requested: actual,
                            available: allowance,
                        });
                    }
                    allocation.shrink_to(actual)?;
                    (entry, allocation, actual)
                };
                // The consumed plan no longer borrows either the source or any
                // staged dependencies. The row remains under its local permit.
                source.next().ok_or_else(cardinality)??;
                heap_bytes = checked_add(heap_bytes, actual)?;
                if chunk.len() >= chunk.capacity() || payloads.len() >= payloads.capacity() {
                    return Err(MemoryError::Capacity {
                        requested: checked_add(chunk.len(), 1)?,
                        available: chunk.capacity().min(payloads.capacity()),
                    });
                }
                payloads.push(allocation);
                chunk.push(Change::Put(entry));
                constructed = checked_add(constructed, 1)?;
                if page_charge::<K, V>(chunk.len(), heap_bytes)? > config.page_bytes {
                    break;
                }
            }
            // Check the next source key while the last owned key still resides
            // in the chunk. This avoids cloning a high-water key or an index.
            match source.peek() {
                Some(Ok(_)) if constructed == expected_entries => return Err(cardinality()),
                Some(Ok(next)) => {
                    if chunk.last().is_some_and(|last| last.key() >= next.key()) {
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
}
