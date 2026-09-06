use crate::snapshot::{LeaseState, entry_vector_charge};
use crate::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, Entry, MemoryError, RangeConfig,
    RangeId, ReadBudget, SnapshotLease, checked_add, checked_mul,
};
use std::collections::{BTreeSet, VecDeque};

/// The adapter hashes relation filters, direction, and authority scope into
/// `fingerprint`. The same bound query is required across all pages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraversalQuery<K> {
    pub root: K,
    pub fingerprint: [u8; 32],
}

/// Cumulative bounds, separate from the size/work budget of each response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraversalLimits {
    pub max_depth: u32,
    pub max_nodes: usize,
    pub max_edges: usize,
    pub max_state_bytes: usize,
}

impl Default for TraversalLimits {
    fn default() -> Self {
        Self {
            max_depth: 32,
            max_nodes: 4096,
            max_edges: 16_384,
            max_state_bytes: 1024 * 1024,
        }
    }
}

impl TraversalLimits {
    fn bounded(self, config: RangeConfig) -> Result<Self, MemoryError> {
        if self.max_nodes == 0 || self.max_edges == 0 || self.max_state_bytes == 0 {
            return Err(MemoryError::InvalidConfiguration(
                "traversal cumulative limits must be nonzero",
            ));
        }
        Ok(Self {
            max_depth: self.max_depth.min(config.max_traversal_depth),
            max_nodes: self.max_nodes.min(config.max_traversal_nodes),
            max_edges: self.max_edges.min(config.max_traversal_edges),
            max_state_bytes: self.max_state_bytes.min(config.max_continuation_bytes),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraversalStop {
    Complete,
    /// Resume the returned continuation at the same immutable prefix.
    PageLimit,
    /// The following are explicit cumulative truncation, not complete results.
    /// Restart at the same snapshot with larger server-permitted limits.
    DepthLimit,
    EdgeLimit,
    NodeLimit,
    StateLimit,
}

struct PendingNode<K> {
    key: K,
    depth: u32,
    after: Option<K>,
    emitted: bool,
}

/// Bounded traversal working state. It retains keys and accounting, never
/// snapshot pages. A dropped/expired owner lease makes it unusable.
pub struct TraversalContinuation<K> {
    range: RangeId,
    lease: u64,
    prefix: u64,
    query: TraversalQuery<K>,
    limits: TraversalLimits,
    queue: VecDeque<PendingNode<K>>,
    visited: BTreeSet<K>,
    current: Option<PendingNode<K>>,
    edge_visits: usize,
    depth_pruned: bool,
    estimated_bytes: usize,
    _allocation: Allocation,
}

pub struct TraversalPage<K, V> {
    pub prefix: u64,
    items: Vec<Entry<K, V>>,
    pub continuation: Option<TraversalContinuation<K>>,
    pub stop: TraversalStop,
    /// Neighbor probes, including probes that find no further edge.
    pub edge_visits: usize,
    pub total_edge_visits: usize,
    _allocation: Allocation,
}

impl<K, V> TraversalPage<K, V> {
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

impl<K, V> std::fmt::Debug for TraversalPage<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraversalPage")
            .field("prefix", &self.prefix)
            .field("items", &self.items.len())
            .field("stop", &self.stop)
            .field("edge_visits", &self.edge_visits)
            .finish()
    }
}

impl<K: Ord + Clone, V: Clone> SnapshotLease<K, V> {
    /// Breadth-first traversal with keyset adjacency continuation. The adapter
    /// supplies the next strictly increasing neighbor after `after`, using the
    /// supplied immutable value (or another index pinned to this same prefix).
    /// It must perform bounded indexed work per probe, not collect all edges.
    /// The callback is invoked at most `max_edge_visits` times per response.
    /// An absent referenced key is a typed error, never silently skipped.
    pub fn traverse<N>(
        &self,
        query: &TraversalQuery<K>,
        limits: TraversalLimits,
        budget: ReadBudget,
        continuation: Option<TraversalContinuation<K>>,
        now: u64,
        mut next_neighbor: N,
    ) -> Result<TraversalPage<K, V>, MemoryError>
    where
        N: FnMut(&K, &V, Option<&K>) -> Result<Option<K>, MemoryError>,
    {
        let state = self.checked_state(now)?;
        let limits = limits.bounded(state.config)?;
        let budget = budget.bounded(state.config)?;
        let mut cursor = match continuation {
            Some(cursor) => {
                if cursor.range != self.range_id()
                    || cursor.lease != self.id()
                    || cursor.prefix != self.prefix()
                {
                    return Err(MemoryError::WrongLease);
                }
                if &cursor.query != query || cursor.limits != limits {
                    return Err(MemoryError::QueryMismatch);
                }
                cursor
            }
            None => self.start_traversal(&state, query, limits)?,
        };
        let capacity = budget.max_items.min(state.root.len);
        // Reserve response capacity before cloning any value. Conservative
        // overcharge is released with the response, including an empty page.
        let response_bytes = checked_add(
            size_of::<TraversalPage<K, V>>(),
            checked_add(budget.max_bytes, entry_vector_charge::<K, V>(capacity)?)?,
        )?;
        let allocation = state
            .budget
            .reserve(BudgetKind::Query, BudgetLane::Ordinary, response_bytes)?
            .commit();
        let mut items = Vec::new();
        items
            .try_reserve_exact(capacity)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut bytes = 0;
        let mut probes = 0;
        let stop = loop {
            if cursor.current.is_none() {
                cursor.current = cursor.queue.pop_front();
            }
            let Some(current) = cursor.current.as_mut() else {
                break if cursor.depth_pruned {
                    TraversalStop::DepthLimit
                } else {
                    TraversalStop::Complete
                };
            };
            let entry = state
                .root
                .get(&current.key)
                .ok_or(MemoryError::MissingKey)?;
            if !current.emitted {
                let next_bytes = checked_add(bytes, entry.read_bytes()?)?;
                if items.len() == budget.max_items || next_bytes > budget.max_bytes {
                    if items.is_empty() {
                        return Err(MemoryError::ItemTooLarge {
                            bytes: next_bytes,
                            limit: budget.max_bytes,
                        });
                    }
                    break TraversalStop::PageLimit;
                }
                items.push(entry.clone());
                bytes = next_bytes;
                current.emitted = true;
            }
            if probes == budget.max_edge_visits {
                break TraversalStop::PageLimit;
            }
            if cursor.edge_visits == limits.max_edges {
                break TraversalStop::EdgeLimit;
            }
            let next = next_neighbor(&current.key, &entry.value, current.after.as_ref())?;
            probes = checked_add(probes, 1)?;
            cursor.edge_visits = checked_add(cursor.edge_visits, 1)?;
            let Some(neighbor) = next else {
                cursor.current = None;
                continue;
            };
            if current
                .after
                .as_ref()
                .is_some_and(|after| &neighbor <= after)
            {
                return Err(MemoryError::InvalidNeighbors);
            }
            if current.depth == limits.max_depth {
                cursor.depth_pruned = true;
                cursor.current = None;
                continue;
            }
            let neighbor_entry = state.root.get(&neighbor).ok_or(MemoryError::MissingKey)?;
            if cursor.visited.contains(&neighbor) {
                current.after = Some(neighbor.clone());
                continue;
            }
            if cursor.visited.len() == limits.max_nodes {
                break TraversalStop::NodeLimit;
            }
            let new_bytes = checked_add(
                cursor.estimated_bytes,
                discovered_charge::<K>(neighbor_entry.heap_bytes)?,
            )?;
            if new_bytes > limits.max_state_bytes {
                break TraversalStop::StateLimit;
            }
            cursor.estimated_bytes = new_bytes;
            current.after = Some(neighbor.clone());
            cursor.visited.insert(neighbor.clone());
            cursor.queue.push_back(PendingNode {
                key: neighbor,
                depth: current
                    .depth
                    .checked_add(1)
                    .ok_or(MemoryError::CounterExhausted("traversal depth"))?,
                after: None,
                emitted: false,
            });
        };
        let total_edge_visits = cursor.edge_visits;
        let continuation = (stop == TraversalStop::PageLimit).then_some(cursor);
        Ok(TraversalPage {
            prefix: self.prefix(),
            items,
            continuation,
            stop,
            edge_visits: probes,
            total_edge_visits,
            _allocation: allocation,
        })
    }

    fn start_traversal(
        &self,
        state: &LeaseState<K, V>,
        query: &TraversalQuery<K>,
        limits: TraversalLimits,
    ) -> Result<TraversalContinuation<K>, MemoryError> {
        let root = state.root.get(&query.root).ok_or(MemoryError::MissingKey)?;
        let estimated_bytes = checked_add(
            checked_add(size_of::<TraversalContinuation<K>>(), ALLOCATOR_OVERHEAD)?,
            checked_add(
                root.heap_bytes,
                checked_add(
                    checked_mul(limits.max_nodes, size_of::<PendingNode<K>>())?,
                    discovered_charge::<K>(root.heap_bytes)?,
                )?,
            )?,
        )?;
        if estimated_bytes > limits.max_state_bytes {
            return Err(MemoryError::Capacity {
                requested: estimated_bytes,
                available: limits.max_state_bytes,
            });
        }
        let allocation = state
            .budget
            .reserve(
                BudgetKind::Query,
                BudgetLane::Ordinary,
                limits.max_state_bytes,
            )?
            .commit();
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(limits.max_nodes)
            .map_err(|_| MemoryError::AllocationFailed)?;
        queue.push_back(PendingNode {
            key: root.key.clone(),
            depth: 0,
            after: None,
            emitted: false,
        });
        let mut visited = BTreeSet::new();
        visited.insert(root.key.clone());
        Ok(TraversalContinuation {
            range: self.range_id(),
            lease: self.id(),
            prefix: self.prefix(),
            query: query.clone(),
            limits,
            queue,
            visited,
            current: None,
            edge_visits: 0,
            depth_pruned: false,
            estimated_bytes,
            _allocation: allocation,
        })
    }
}

fn discovered_charge<K>(key_heap_bytes: usize) -> Result<usize, MemoryError> {
    // Up to three dynamic key copies (visited, queue/current, last adjacency
    // cursor); one worst-case tree node per visited key. Queue inline capacity
    // is charged once when the traversal begins.
    checked_add(
        checked_mul(3, key_heap_bytes)?,
        checked_add(
            ALLOCATOR_OVERHEAD,
            checked_mul(16, checked_add(size_of::<K>(), size_of::<usize>())?)?,
        )?,
    )
}
