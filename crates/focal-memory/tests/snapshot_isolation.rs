#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_memory::{
    BudgetKind, BudgetLane, Change, Entry, MemoryBudget, MemoryError, RangeConfig, RangeId,
    RangeStore, ReadBudget, ScanQuery,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Barrier},
};

const ORDINARY: BudgetLane = BudgetLane::Ordinary;

fn range(budget: MemoryBudget, page_entries: usize) -> RangeStore<u64, u64> {
    RangeStore::new(
        RangeId(1),
        0,
        RangeConfig {
            page_entries,
            ..RangeConfig::default()
        },
        budget,
    )
    .unwrap()
}

fn random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[test]
fn randomized_atomic_batches_and_paged_scans_match_ordered_oracle() {
    for initial_seed in [9, 873, 123_543] {
        let budget = MemoryBudget::new(32 * 1024 * 1024, 4096).unwrap();
        let mut store = range(budget.clone(), 7);
        let mut oracle = BTreeMap::new();
        let mut seed = initial_seed;
        for prefix in 1..=1000 {
            let count = random(&mut seed) % 8;
            let mut keys = BTreeSet::new();
            let mut changes = Vec::new();
            for _ in 0..count {
                let key = random(&mut seed) % 150;
                if !keys.insert(key) {
                    continue;
                }
                if oracle.contains_key(&key) && random(&mut seed).is_multiple_of(3) {
                    changes.push(Change::Delete(key));
                    oracle.remove(&key);
                } else {
                    let value = random(&mut seed);
                    changes.push(Change::Put(Entry::new(key, value, 0)));
                    oracle.insert(key, value);
                }
            }
            store.apply_batch(prefix, changes, ORDINARY).unwrap();
            assert_eq!(store.prefix(), prefix);
            assert_eq!(store.len(), oracle.len());
            assert_eq!(
                store
                    .entries()
                    .map(|entry| (entry.key, entry.value))
                    .collect::<BTreeMap<_, _>>(),
                oracle
            );
            if prefix % 17 == 0 {
                let lease = store.pin(prefix, 100).unwrap();
                let query = ScanQuery {
                    start: Some(20),
                    end: Some(130),
                    heap_bytes: 0,
                };
                let limits = ReadBudget {
                    max_items: 3,
                    max_bytes: 80,
                    max_edge_visits: 1,
                };
                let mut cursor = None;
                let mut scanned = Vec::new();
                loop {
                    let mut page = lease.scan(&query, limits, cursor, prefix).unwrap();
                    assert_eq!(page.prefix, prefix);
                    assert!(page.len() <= 3);
                    scanned.extend(page.items().iter().map(|entry| (entry.key, entry.value)));
                    cursor = page.continuation.take();
                    if cursor.is_none() {
                        break;
                    }
                }
                assert_eq!(
                    scanned,
                    oracle
                        .range(20..130)
                        .map(|(key, value)| (*key, *value))
                        .collect::<Vec<_>>()
                );
                store.release(&lease).unwrap();
            }
        }
        drop(store);
        assert_eq!(budget.stats().used, 0);
    }
}

#[test]
fn retained_snapshot_stays_at_one_prefix_during_concurrent_writes() {
    let budget = MemoryBudget::new(16 * 1024 * 1024, 4096).unwrap();
    let mut store = range(budget.clone(), 4);
    store
        .apply_batch(
            1,
            (0..20)
                .map(|key| Change::Put(Entry::new(key, key, 0)))
                .collect(),
            ORDINARY,
        )
        .unwrap();
    let lease = store.pin(1, 1000).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let reader = {
        let lease = lease.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            for _ in 0..100 {
                barrier.wait();
                let page = lease
                    .scan(&ScanQuery::all(), ReadBudget::default(), None, 2)
                    .unwrap();
                assert_eq!(page.prefix, 1);
                assert_eq!(
                    page.items()
                        .iter()
                        .map(|entry| (entry.key, entry.value))
                        .collect::<Vec<_>>(),
                    (0..20).map(|key| (key, key)).collect::<Vec<_>>()
                );
                barrier.wait();
            }
        })
    };
    for prefix in 2..102 {
        barrier.wait();
        store
            .apply_batch(
                prefix,
                (0..20)
                    .map(|key| Change::Put(Entry::new(key, prefix, 0)))
                    .collect(),
                ORDINARY,
            )
            .unwrap();
        barrier.wait();
    }
    reader.join().unwrap();
    assert_eq!(store.get(&0), Some(&101));
    let pinned_bytes = budget.stats().used;
    store.release(&lease).unwrap();
    assert!(budget.stats().used < pinned_bytes);
    assert!(matches!(lease.get(&0, 2), Err(MemoryError::LeaseExpired)));
    drop(store);
    drop(lease);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn expiration_reclaims_old_pages_even_when_caller_keeps_lease_handles() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut store = range(budget.clone(), 2);
    store
        .apply_batch(1, vec![Change::Put(Entry::new(1, 1, 10_000))], ORDINARY)
        .unwrap();
    let lease = store.pin(10, 5).unwrap();
    let retained = lease.clone();
    store
        .apply_batch(2, vec![Change::Put(Entry::new(1, 2, 10_000))], ORDINARY)
        .unwrap();
    let before = budget.stats().used;
    assert_eq!(lease.get(&1, 14).unwrap().items()[0].value, 1);
    assert_eq!(store.advance_clock(15).unwrap(), 1);
    assert!(budget.stats().used + 10_000 < before);
    assert!(matches!(
        retained.get(&1, 14),
        Err(MemoryError::LeaseExpired)
    ));
    assert_eq!(store.stats().pinned_snapshots, 0);
    assert_eq!(
        store.advance_clock(14),
        Err(MemoryError::ClockRegression {
            current: 15,
            supplied: 14
        })
    );
}

#[test]
fn response_ownership_remains_charged_after_snapshot_release() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut store = range(budget.clone(), 2);
    store
        .apply_batch(1, vec![Change::Put(Entry::new(1, 1, 1000))], ORDINARY)
        .unwrap();
    let lease = store.pin(0, 10).unwrap();
    let page = lease.get(&1, 0).unwrap();
    store.release(&lease).unwrap();
    drop(store);
    assert!(budget.stats().used >= 1000);
    assert_eq!(page.items()[0].value, 1);
    drop(page);
    drop(lease);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn capacity_failures_anywhere_in_batch_preserve_root_and_accounting() {
    // Sweep free capacity through staging, directory, and successive changed
    // page allocations; this catches cleanup after partially built candidates.
    let mut failures = 0;
    let mut successes = 0;
    for headroom in (0..12_000).step_by(137) {
        let budget = MemoryBudget::new(30_000, 1000).unwrap();
        let mut store = range(budget.clone(), 2);
        store
            .apply_batch(
                1,
                (0..8)
                    .map(|key| Change::Put(Entry::new(key, key, 100)))
                    .collect(),
                ORDINARY,
            )
            .unwrap();
        let available = 29_000 - budget.stats().ordinary_used;
        let occupied = budget
            .reserve(BudgetKind::Monitor, ORDINARY, available - headroom)
            .unwrap();
        let before = budget.stats();
        let result = store.apply_batch(
            2,
            (0..8)
                .map(|key| Change::Put(Entry::new(key, 99, 100)))
                .collect(),
            ORDINARY,
        );
        match result {
            Ok(()) => {
                successes += 1;
                assert_eq!(store.get(&0), Some(&99));
            }
            Err(MemoryError::Capacity { .. }) => {
                failures += 1;
                assert_eq!(store.prefix(), 1);
                assert_eq!(
                    store
                        .entries()
                        .map(|entry| (entry.key, entry.value))
                        .collect::<Vec<_>>(),
                    (0..8).map(|key| (key, key)).collect::<Vec<_>>()
                );
                assert_eq!(budget.stats(), before);
            }
            Err(error) => panic!("unexpected error: {error}"),
        }
        drop(occupied);
        drop(store);
        assert_eq!(budget.stats().used, 0);
    }
    assert!(failures > 10);
    assert!(successes > 10);
}

#[test]
fn duplicate_missing_prefix_and_byte_overflow_fail_before_publication() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut store = range(budget.clone(), 2);
    let before = budget.stats();
    let put = || Change::Put(Entry::new(1, 1, 0));
    assert_eq!(
        store.apply_batch(2, vec![put()], ORDINARY),
        Err(MemoryError::PrefixMismatch {
            expected: 1,
            actual: 2
        })
    );
    assert_eq!(
        store.apply_batch(1, vec![put(), put()], ORDINARY),
        Err(MemoryError::DuplicateKey)
    );
    assert_eq!(
        store.apply_batch(1, vec![Change::Delete(1)], ORDINARY),
        Err(MemoryError::MissingKey)
    );
    assert_eq!(
        store.apply_batch(1, vec![Change::Put(Entry::new(1, 1, usize::MAX))], ORDINARY),
        Err(MemoryError::CounterExhausted("byte charge"))
    );
    assert_eq!(budget.stats(), before);
    store.apply_batch(1, vec![], ORDINARY).unwrap();
    assert_eq!(store.prefix(), 1);
    assert!(store.is_empty());
    let mut exhausted: RangeStore<u64, u64> =
        RangeStore::new(RangeId(2), u64::MAX, RangeConfig::default(), budget).unwrap();
    assert_eq!(
        exhausted.apply_batch(0, vec![], ORDINARY),
        Err(MemoryError::CounterExhausted("published prefix"))
    );
}

#[test]
fn scan_cursor_is_bound_to_query_and_exact_lease() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut store = range(budget, 2);
    store
        .apply_batch(
            1,
            (0..8)
                .map(|key| Change::Put(Entry::new(key, key, 0)))
                .collect(),
            ORDINARY,
        )
        .unwrap();
    let first = store.pin(0, 10).unwrap();
    let second = store.pin(0, 10).unwrap();
    let query = ScanQuery::all();
    let limits = ReadBudget {
        max_items: 2,
        ..ReadBudget::default()
    };
    let cursor = first.scan(&query, limits, None, 0).unwrap().continuation;
    assert!(matches!(
        second.scan(&query, limits, cursor, 0),
        Err(MemoryError::WrongLease)
    ));
    let cursor = first.scan(&query, limits, None, 0).unwrap().continuation;
    let changed = ScanQuery {
        start: Some(3),
        end: None,
        heap_bytes: 0,
    };
    assert!(matches!(
        first.scan(&changed, limits, cursor, 0),
        Err(MemoryError::QueryMismatch)
    ));
    let tiny = ReadBudget {
        max_bytes: 1,
        ..limits
    };
    assert!(matches!(
        first.scan(&query, tiny, None, 0),
        Err(MemoryError::ItemTooLarge { .. })
    ));
}

#[test]
fn pin_limit_and_expiry_overflow_are_typed_and_recoverable() {
    let budget = MemoryBudget::new(100_000, 0).unwrap();
    let mut store: RangeStore<u64, u64> = RangeStore::new(
        RangeId(1),
        0,
        RangeConfig {
            max_snapshot_leases: 1,
            max_snapshot_ttl: 10,
            ..RangeConfig::default()
        },
        budget.clone(),
    )
    .unwrap();
    let lease = store.pin(0, 5).unwrap();
    assert!(matches!(store.pin(1, 5), Err(MemoryError::Capacity { .. })));
    assert!(matches!(
        store.pin(2, 11),
        Err(MemoryError::InvalidConfiguration(_))
    ));
    assert!(store.pin(5, 5).is_ok());
    assert!(matches!(lease.get(&1, 1), Err(MemoryError::LeaseExpired)));
    assert!(matches!(
        store.pin(u64::MAX - 1, 5),
        Err(MemoryError::CounterExhausted("snapshot expiry"))
    ));
    drop(store);
    drop(lease);
    assert_eq!(budget.stats().used, 0);
}
