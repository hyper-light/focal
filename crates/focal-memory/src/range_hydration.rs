//! Detached checkpoint construction from borrowed, fallible row plans.
use super::*;

#[path = "range_hydration_phased.rs"]
mod phased;
pub use phased::{
    RangeHydration, RangeHydrationLimits, RangeHydrationLookup, RangeHydrationSource,
    RangeHydrationView,
};

#[cfg(test)]
#[path = "range_hydration_tests.rs"]
mod tests;

impl<K: Ord + Clone, V> RangeStore<K, V> {
    /// Restore strictly ordered checkpoint rows without requiring `V: Clone`.
    /// Each source entry contains its final key, a construction plan `P`, and
    /// an upper bound on the final key/value heap in `heap_bytes`. The engine
    /// reserves that bound before calling `build(&key, plan, bound)`. The
    /// callback returns the owned value and its actual complete key/value heap
    /// charge, including capacities and allocator overhead. An excessive charge
    /// is refused immediately; a smaller charge releases the unused allowance.
    /// The callback must itself measure owned capacities and obey the supplied
    /// bound before allocating further buffers. The engine cannot inspect `V`.
    ///
    /// Source iteration and construction-plan preparation must be bounded and
    /// allocation-free or covered by caller-held accounting. In particular, an
    /// already allocated key or plan remains the caller's responsibility until
    /// the engine acquires its row allowance. Callback workspace is separate
    /// from the final payload allowance. A decoder must enforce its own total
    /// byte/work/count limits and exact source cardinality.
    ///
    /// Only one bounded chunk is staged. Imported values move into final pages;
    /// `copy` is used only when extending a previously constructed leaf, under
    /// the same precharge and semantic contract as [`Self::prepare_batch_with`].
    /// Partition classification obeys [`Self::new_partitioned`]. Every failure
    /// drops owned rows before their permits and discards the entire detached
    /// store. The verified checkpoint prefix becomes visible only on success.
    #[allow(clippy::too_many_arguments)] // Exact import frame plus distinct build and copy contracts.
    pub fn from_entry_plans_partitioned_with<P, B, F>(
        id: RangeId,
        prefix: u64,
        config: RangeConfig,
        budget: MemoryBudget,
        entries: impl IntoIterator<Item = Result<Entry<K, P>, MemoryError>>,
        partition: fn(&K) -> u64,
        mut build: B,
        mut copy: F,
    ) -> Result<Self, MemoryError>
    where
        B: FnMut(&K, P, usize) -> Result<(V, usize), MemoryError>,
        F: FnMut(&V) -> Result<V, MemoryError>,
    {
        let mut store = Self::new_partitioned(id, 0, config, budget, partition)?;
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
                        checked_add(size_of::<Change<K, V>>(), size_of::<Allocation>())?,
                    )?,
                )?,
            )?;
            let mut payloads = bounded_vec(chunk_limit)?;
            // Reverse local drop order keeps rows covered through destruction.
            let mut chunk = bounded_vec(chunk_limit)?;
            let mut heap_bytes = 0;
            for _ in 0..chunk_limit {
                let plan = match source.peek() {
                    Some(Ok(plan)) => plan,
                    Some(Err(error)) => return Err(error.clone()),
                    None => break,
                };
                layout::check_entry::<K, V>(plan.heap_bytes, config)?;
                let next_heap = checked_add(heap_bytes, plan.heap_bytes)?;
                let next_charge = page_charge::<K, V>(checked_add(chunk.len(), 1)?, next_heap)?;
                let boundary = chunk.last().is_some_and(|previous: &Change<K, V>| {
                    partition(previous.key()) != partition(&plan.key)
                });
                if !chunk.is_empty() && (boundary || next_charge > config.page_bytes) {
                    break;
                }
                let previous = chunk.last().map(Change::key).or_else(|| {
                    store
                        .root
                        .pages
                        .last()
                        .and_then(|page| page.entries.last())
                        .map(|last| &last.key)
                });
                if previous.is_some_and(|key| &plan.key <= key) {
                    return Err(MemoryError::InvalidConfiguration(
                        "checkpoint entries must be strictly ordered",
                    ));
                }
                let allowance = plan.heap_bytes;
                let mut allocation = store
                    .budget
                    .reserve(BudgetKind::Pending, BudgetLane::Completion, allowance)?
                    .commit();
                // `peek` already holds this plan, so consuming it cannot ask the
                // adapter to prepare another row before the current precharge.
                // Keys and values declared below drop before this allocation.
                let Entry {
                    key, value: plan, ..
                } = source.next().ok_or(MemoryError::MissingKey)??;
                let (value, actual) = build(&key, plan, allowance)?;
                let entry = Entry::new(key, value, actual);
                if actual > allowance {
                    return Err(MemoryError::Capacity {
                        requested: actual,
                        available: allowance,
                    });
                }
                allocation.shrink_to(actual)?;
                heap_bytes = checked_add(heap_bytes, actual)?;
                let charge = page_charge::<K, V>(checked_add(chunk.len(), 1)?, heap_bytes)?;
                // Check both destinations while the row still owns its local
                // permit; no fallible step may separate these two transfers.
                if chunk.len() >= chunk_limit
                    || chunk.len() >= chunk.capacity()
                    || payloads.len() >= payloads.capacity()
                {
                    return Err(MemoryError::Capacity {
                        requested: checked_add(chunk.len(), 1)?,
                        available: chunk_limit.min(chunk.capacity()).min(payloads.capacity()),
                    });
                }
                payloads.push(allocation);
                chunk.push(Change::Put(entry));
                if charge > config.page_bytes {
                    break;
                }
            }
            let next = store
                .prefix()
                .checked_add(1)
                .ok_or(MemoryError::CounterExhausted(
                    "checkpoint construction prefix",
                ))?;
            let prepared =
                store.prepare_batch_with(next, chunk, BudgetLane::Completion, &mut copy)?;
            store.publish(prepared)?;
        }
        let root = Arc::get_mut(&mut store.root).ok_or(MemoryError::WrongRange)?;
        root.prefix = prefix;
        Ok(store)
    }
}
