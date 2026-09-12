use crate::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ContentHash, LedgerId, RouteEpoch, SessionSeq};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A routing hint with provenance, never a serving-authority grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRoute {
    pub ledger: LedgerId,
    pub partition: PartitionId,
    pub delegation_epoch: u64,
    pub source_revision: u64,
    pub route_epoch: RouteEpoch,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    pub leader: u64,
    pub leader_generation: u64,
    pub activation: ContentHash,
}

/// Install from committed local session control records. Checking the fence at
/// the resource prevents a stale directory/cache from authorizing old owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServingFence {
    pub node: u64,
    pub ledger: LedgerId,
    pub route_epoch: RouteEpoch,
    pub placement_epoch: u64,
    pub node_generation: u64,
    pub activation: ContentHash,
    /// An old owner after cutover may finish pinned reads only through C.
    pub retired_after: Option<SessionSeq>,
}
impl ServingFence {
    pub fn check(
        &self,
        route: &SessionRoute,
        prefix: SessionSeq,
        mutation: bool,
    ) -> Result<(), DirectoryError> {
        if route.ledger != self.ledger {
            return Err(DirectoryError::OutsideNamespace);
        }
        if route.leader != self.node
            || route.route_epoch != self.route_epoch
            || route.placement_epoch != self.placement_epoch
            || route.leader_generation != self.node_generation
            || route.activation != self.activation
        {
            return Err(DirectoryError::StaleEpoch);
        }
        if self
            .retired_after
            .is_some_and(|cut| mutation || prefix > cut)
        {
            return Err(DirectoryError::StaleEpoch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteInvalidation {
    pub ledger: LedgerId,
    pub route_epoch: RouteEpoch,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidationBatch {
    pub partition: PartitionId,
    pub delegation_epoch: u64,
    pub after_revision: u64,
    pub through_revision: u64,
    pub changes: Vec<RouteInvalidation>,
}
#[derive(Debug, Clone, Copy)]
pub struct RouteCacheConfig {
    pub max_entries: usize,
    pub max_partitions: usize,
    pub max_ttl: u64,
}
impl Default for RouteCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1024,
            max_partitions: 128,
            max_ttl: 30_000,
        }
    }
}
struct CacheEntry {
    route: SessionRoute,
    expires_at: u64,
    used: u64,
    _allocation: Allocation,
}
struct Watch {
    epoch: u64,
    revision: u64,
    _allocation: Allocation,
}
pub struct RouteCache {
    entries: BTreeMap<LedgerId, CacheEntry>,
    watches: BTreeMap<PartitionId, Watch>,
    config: RouteCacheConfig,
    budget: MemoryBudget,
    clock: u64,
    access: u64,
}
impl RouteCache {
    pub fn new(config: RouteCacheConfig, budget: MemoryBudget) -> Result<Self, DirectoryError> {
        if config.max_entries == 0 || config.max_partitions == 0 || config.max_ttl == 0 {
            return Err(DirectoryError::Invalid("route cache limits"));
        }
        Ok(Self {
            entries: BTreeMap::new(),
            watches: BTreeMap::new(),
            config,
            budget,
            clock: 0,
            access: 0,
        })
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn watched_partitions(&self) -> usize {
        self.watches.len()
    }
    /// Every watched partition with the delegation epoch and revision the
    /// cache has applied through: what to ask each partition for next.
    pub fn watches(&self) -> impl Iterator<Item = (PartitionId, u64, u64)> + '_ {
        self.watches
            .iter()
            .map(|(partition, watch)| (*partition, watch.epoch, watch.revision))
    }
    pub fn get(
        &mut self,
        ledger: LedgerId,
        now: u64,
    ) -> Result<Option<&SessionRoute>, DirectoryError> {
        // Route resolution is per-request: it must be O(log n), not a full-cache
        // TTL sweep plus watch prune (O(entries) + O(watches x entries)). Advance
        // the clock, then lazily treat only the looked-up entry as expired; the
        // bulk sweep and watch pruning run on the cheaper insert/invalidate paths
        // (advance), which also bounds how long a stale entry or orphan watch can
        // linger (both are capped by max_entries/max_partitions).
        if now < self.clock {
            return Err(DirectoryError::ClockRegression);
        }
        self.clock = now;
        if self
            .entries
            .get(&ledger)
            .is_some_and(|entry| entry.expires_at <= now)
        {
            self.entries.remove(&ledger);
            return Ok(None);
        }
        let access = self
            .access
            .checked_add(1)
            .ok_or(DirectoryError::CounterExhausted)?;
        self.access = access;
        if let Some(entry) = self.entries.get_mut(&ledger) {
            entry.used = access;
        }
        Ok(self.entries.get(&ledger).map(|entry| &entry.route))
    }
    pub fn insert(
        &mut self,
        route: SessionRoute,
        now: u64,
        ttl: u64,
    ) -> Result<(), DirectoryError> {
        self.advance(now)?;
        if ttl == 0 || ttl > self.config.max_ttl {
            return Err(DirectoryError::Invalid("route TTL"));
        }
        if route.source_revision == 0
            || route.route_epoch.0 == 0
            || route.membership_epoch == 0
            || route.placement_epoch == 0
            || route.delegation_epoch == 0
            || route.leader_generation == 0
            || route.leader == 0
            || !types::nonzero_hash(route.activation)
        {
            return Err(DirectoryError::Invalid("route metadata"));
        }
        let expires_at = now
            .checked_add(ttl)
            .ok_or(DirectoryError::CounterExhausted)?;
        let used = self
            .access
            .checked_add(1)
            .ok_or(DirectoryError::CounterExhausted)?;
        if let Some(old) = self.entries.get(&route.ledger)
            && (route.route_epoch < old.route.route_epoch
                || route.delegation_epoch < old.route.delegation_epoch)
        {
            return Err(DirectoryError::StaleEpoch);
        }
        if let Some(watch) = self.watches.get(&route.partition)
            && (route.delegation_epoch < watch.epoch
                || (route.delegation_epoch == watch.epoch
                    && route.source_revision < watch.revision))
        {
            return Err(DirectoryError::StaleEpoch);
        }
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Index,
                BudgetLane::Ordinary,
                tree_row::<(LedgerId, CacheEntry)>(),
            )?
            .commit();
        let new_watch = if !self.watches.contains_key(&route.partition) {
            if self.watches.len() >= self.config.max_partitions {
                return Err(DirectoryError::Capacity);
            }
            Some(
                self.budget
                    .reserve(
                        BudgetKind::Index,
                        BudgetLane::Ordinary,
                        tree_row::<(PartitionId, Watch)>(),
                    )?
                    .commit(),
            )
        } else {
            None
        };
        if self.entries.len() == self.config.max_entries
            && !self.entries.contains_key(&route.ledger)
        {
            let victim = self
                .entries
                .iter()
                .min_by_key(|(key, entry)| (entry.used, **key))
                .map(|(key, _)| *key)
                .ok_or(DirectoryError::Missing)?;
            self.entries.remove(&victim);
        }
        if let Some(allocation) = new_watch {
            self.watches.insert(
                route.partition,
                Watch {
                    epoch: route.delegation_epoch,
                    revision: route.source_revision,
                    _allocation: allocation,
                },
            );
        }
        // A point lookup must not move an existing watch past unseen updates.
        self.entries.insert(
            route.ledger,
            CacheEntry {
                route,
                expires_at,
                used,
                _allocation: allocation,
            },
        );
        self.access = used;
        self.prune_watches();
        Ok(())
    }
    pub fn invalidate(&mut self, batch: &InvalidationBatch) -> Result<usize, DirectoryError> {
        if batch.changes.len() > self.config.max_entries
            || batch.through_revision < batch.after_revision
        {
            return Err(DirectoryError::Invalid("invalidation bounds"));
        }
        let watch = self
            .watches
            .get_mut(&batch.partition)
            .ok_or(DirectoryError::Missing)?;
        if batch.delegation_epoch < watch.epoch
            || (batch.delegation_epoch == watch.epoch && batch.through_revision <= watch.revision)
        {
            return Ok(0);
        }
        let before = self.entries.len();
        if batch.delegation_epoch != watch.epoch || batch.after_revision != watch.revision {
            let expected = watch.revision;
            self.entries
                .retain(|_, entry| entry.route.partition != batch.partition);
            watch.epoch = batch.delegation_epoch;
            watch.revision = batch.through_revision;
            self.prune_watches();
            return Err(DirectoryError::WatchGap {
                expected,
                actual: batch.after_revision,
            });
        }
        for change in &batch.changes {
            if self.entries.get(&change.ledger).is_some_and(|entry| {
                entry.route.partition == batch.partition
                    && entry.route.route_epoch < change.route_epoch
            }) {
                self.entries.remove(&change.ledger);
            }
        }
        watch.revision = batch.through_revision;
        self.prune_watches();
        Ok(before.saturating_sub(self.entries.len()))
    }
    /// Explicit runtime reconciliation; no all-fleet subscription or poll loop.
    pub fn advance(&mut self, now: u64) -> Result<(), DirectoryError> {
        if now < self.clock {
            return Err(DirectoryError::ClockRegression);
        }
        self.clock = now;
        self.entries.retain(|_, entry| entry.expires_at > now);
        self.prune_watches();
        Ok(())
    }
    fn prune_watches(&mut self) {
        self.watches.retain(|partition, _| {
            self.entries
                .values()
                .any(|entry| entry.route.partition == *partition)
        });
    }
}
