#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_directory::*;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ContentHash, LedgerId, RouteEpoch, SessionId, TenantId};

fn budget() -> MemoryBudget {
    MemoryBudget::new(1024 * 1024, 16 * 1024).unwrap()
}
fn route(session: u128, partition: u128, revision: u64, epoch: u64) -> SessionRoute {
    SessionRoute {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(session),
        },
        partition: PartitionId::from_u128(partition),
        delegation_epoch: 1,
        source_revision: revision,
        route_epoch: RouteEpoch(epoch),
        membership_epoch: epoch,
        placement_epoch: epoch,
        leader: 1,
        leader_generation: 1,
        activation: ContentHash([1; 32]),
    }
}
fn cache(node: MemoryBudget) -> RouteCache {
    RouteCache::new(
        RouteCacheConfig {
            max_entries: 3,
            max_partitions: 3,
            max_ttl: 100,
        },
        node,
    )
    .unwrap()
}

#[test]
fn scoped_invalidation_and_watch_gaps_clear_only_affected_cached_partition() {
    let mut cache = cache(budget());
    for route in [route(1, 1, 5, 1), route(2, 1, 5, 1), route(3, 2, 12, 1)] {
        cache.insert(route, 0, 100).unwrap();
    }
    let batch = InvalidationBatch {
        partition: PartitionId::from_u128(1),
        delegation_epoch: 1,
        after_revision: 5,
        through_revision: 6,
        changes: vec![RouteInvalidation {
            ledger: route(1, 1, 5, 1).ledger,
            route_epoch: RouteEpoch(2),
        }],
    };
    assert_eq!(cache.invalidate(&batch).unwrap(), 1);
    assert!(cache.get(route(1, 1, 5, 1).ledger, 0).unwrap().is_none());
    assert!(cache.get(route(2, 1, 5, 1).ledger, 0).unwrap().is_some());
    assert_eq!(
        cache.invalidate(&batch).unwrap(),
        0,
        "duplicate delivery is inert"
    );
    let gap = InvalidationBatch {
        after_revision: 8,
        through_revision: 9,
        changes: vec![],
        ..batch
    };
    assert_eq!(
        cache.invalidate(&gap),
        Err(DirectoryError::WatchGap {
            expected: 6,
            actual: 8
        })
    );
    assert!(cache.get(route(2, 1, 5, 1).ledger, 0).unwrap().is_none());
    assert!(cache.get(route(3, 2, 12, 1).ledger, 0).unwrap().is_some());
    assert_eq!(cache.watched_partitions(), 1);
}

#[test]
fn newer_point_lookup_does_not_skip_unseen_partition_watch_updates() {
    let mut cache = cache(budget());
    cache.insert(route(1, 1, 5, 1), 0, 100).unwrap();
    cache.insert(route(2, 1, 9, 3), 0, 100).unwrap();
    let batch = InvalidationBatch {
        partition: PartitionId::from_u128(1),
        delegation_epoch: 1,
        after_revision: 5,
        through_revision: 9,
        changes: vec![
            RouteInvalidation {
                ledger: route(1, 1, 5, 1).ledger,
                route_epoch: RouteEpoch(2),
            },
            RouteInvalidation {
                ledger: route(2, 1, 9, 3).ledger,
                route_epoch: RouteEpoch(2),
            },
        ],
    };
    assert_eq!(cache.invalidate(&batch).unwrap(), 1);
    assert_eq!(
        cache
            .get(route(2, 1, 9, 3).ledger, 0)
            .unwrap()
            .unwrap()
            .route_epoch,
        RouteEpoch(3)
    );
    assert_eq!(
        cache.insert(route(1, 1, 8, 2), 0, 100),
        Err(DirectoryError::StaleEpoch)
    );
    let mut changed_owner = route(1, 1, 10, 2);
    changed_owner.delegation_epoch = 2;
    cache.insert(changed_owner, 0, 100).unwrap();
    let changed_epoch = InvalidationBatch {
        delegation_epoch: 2,
        after_revision: 9,
        through_revision: 10,
        changes: vec![],
        ..batch
    };
    assert!(matches!(
        cache.invalidate(&changed_epoch),
        Err(DirectoryError::WatchGap { .. })
    ));
    assert!(cache.is_empty());
}

#[test]
fn bounded_lru_ttl_and_failed_allocation_release_every_cache_row_and_watch() {
    let node = budget();
    let mut cache = cache(node.clone());
    for id in 1..=3 {
        cache.insert(route(id, id, 1, 1), 0, 100).unwrap();
    }
    cache.get(route(1, 1, 1, 1).ledger, 1).unwrap();
    // Reuse an already watched partition so the cache can evict one LRU row.
    cache.insert(route(4, 1, 1, 1), 2, 10).unwrap();
    assert!(cache.get(route(2, 2, 1, 1).ledger, 2).unwrap().is_none());
    assert_eq!(cache.len(), 3);
    assert_eq!(cache.watched_partitions(), 2);
    let stats = node.stats();
    let available = stats.limit - stats.completion_reserve - stats.ordinary_used;
    let pressure = node
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, available)
        .unwrap()
        .commit();
    let baseline = node.stats();
    assert!(cache.insert(route(5, 1, 1, 1), 2, 10).is_err());
    assert_eq!(node.stats(), baseline);
    assert_eq!(cache.len(), 3);
    drop(pressure);
    cache.advance(12).unwrap();
    assert!(cache.get(route(4, 1, 1, 1).ledger, 12).unwrap().is_none());
    assert_eq!(cache.advance(11), Err(DirectoryError::ClockRegression));
    cache.advance(100).unwrap();
    assert!(cache.is_empty());
    assert_eq!(node.stats().used, 0);
}
