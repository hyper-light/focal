use super::*;
use crate::{ReadBudget, ScanQuery};
use std::cell::Cell;

const ORDINARY: BudgetLane = BudgetLane::Ordinary;

fn budget() -> MemoryBudget {
    MemoryBudget::new(512 * 1024 * 1024, 1024 * 1024).unwrap()
}

fn config(page_entries: usize) -> RangeConfig {
    RangeConfig {
        page_entries,
        max_batch_entries: 8192,
        max_query_items: 8192,
        ..RangeConfig::default()
    }
}

fn key(rank: usize) -> u64 {
    u64::try_from(rank).unwrap() * 4 + 4
}

fn seed(budget: &MemoryBudget, count: usize, page_entries: usize) -> RangeStore<u64, u64> {
    let mut store = RangeStore::new(RangeId(801), 0, config(page_entries), budget.clone()).unwrap();
    store
        .apply_batch(
            1,
            (0..count)
                .map(|rank| Change::Put(Entry::new(key(rank), key(rank) ^ 73, 0)))
                .collect(),
            ORDINARY,
        )
        .unwrap();
    store
}

fn entries(root: &Root<u64, u64>) -> BTreeMap<u64, u64> {
    root.from(0, 0)
        .map(|entry| (entry.key, entry.value))
        .collect()
}

fn check_directory(store: &RangeStore<u64, u64>, expected: &BTreeMap<u64, u64>) {
    assert!(store.root.pages.check_shape());
    assert_eq!(store.len(), expected.len());
    assert_eq!(store.stats().pages, store.root.pages.len());
    assert_eq!(entries(&store.root), *expected);
    let mut count = 0;
    for (rank, page) in store.root.pages.iter().enumerate() {
        assert!(Arc::ptr_eq(page, store.root.pages.get(rank).unwrap()));
        assert!(!page.entries.is_empty());
        for entry in &page.entries {
            assert_eq!(store.root.pages.page_index(&entry.key), rank);
            assert_eq!(store.get(&entry.key), expected.get(&entry.key));
            count += 1;
        }
    }
    assert_eq!(count, expected.len());
    assert!(store.root.pages.get(store.root.pages.len()).is_none());
}

#[test]
fn directory_rank_seek_and_order_cross_leaf_and_branch_fanout_boundaries() {
    for count in [0, 1, 31, 32, 33, 1023, 1024, 1025, 2049] {
        let budget = budget();
        let store = seed(&budget, count, 1);
        let expected = (0..count).map(|rank| (key(rank), key(rank) ^ 73)).collect();
        check_directory(&store, &expected);
        assert_eq!(store.stats().pages, count);
        assert_eq!(store.root.pages.page_index(&0), 0);
        if count > 32 {
            assert!(store.root.pages.height() >= 2);
        }
        if count > 1024 {
            assert!(store.root.pages.height() >= 3);
        }
        for rank in 0..count {
            assert_eq!(store.root.pages.page_index(&(key(rank) + 3)), rank);
            assert!(store.get(&(key(rank) + 1)).is_none());
            let (page, offset) = store.root.seek(Some(&key(rank)), true);
            assert_eq!(
                store.root.from(page, offset).next().map(|entry| entry.key),
                (rank + 1 < count).then(|| key(rank + 1))
            );
        }
        if count != 0 {
            assert_eq!(store.root.pages.page_index(&u64::MAX), count - 1);
        }
        drop(store);
        assert_eq!(budget.stats().used, 0);
    }
}

#[test]
fn deletion_rebalances_interior_siblings_and_collapses_to_one_leaf_then_empty() {
    let budget = budget();
    let mut store = seed(&budget, 2049, 1);
    let mut expected = entries(&store.root);
    let initial_height = store.root.pages.height();
    assert!(initial_height >= 3);
    for (phase, target) in [1025, 1024, 33, 32, 2, 1, 0].into_iter().enumerate() {
        let keys: Vec<_> = expected.keys().copied().collect();
        let remove = expected.len() - target;
        // Alternating interior positions exercises both neighboring sibling
        // directions instead of trimming only a right-hand append path.
        let selected: Vec<_> = keys
            .iter()
            .skip(phase % 2)
            .step_by(2)
            .chain(keys.iter().skip((phase + 1) % 2).step_by(2))
            .take(remove)
            .copied()
            .collect();
        let changes = selected.iter().copied().map(Change::Delete).collect();
        for key in selected {
            expected.remove(&key);
        }
        let next = store
            .prepare_batch_with(store.prefix() + 1, changes, BudgetLane::Completion, |_| {
                panic!("deleting singleton pages must not copy values")
            })
            .unwrap();
        assert_eq!(next.len(), target);
        store.publish(next).unwrap();
        check_directory(&store, &expected);
        assert!(store.root.pages.height() <= initial_height);
        if target == 1 {
            assert_eq!(store.root.pages.height(), 1);
        }
        if target == 0 {
            assert_eq!(store.root.pages.height(), 0);
            assert_eq!(budget.stats().by_kind[BudgetKind::Pages as usize], 0);
            assert_eq!(
                budget.stats().by_kind[BudgetKind::Roots as usize],
                store.root._allocation.bytes()
            );
        }
    }
    store
        .apply_batch(
            store.prefix() + 1,
            vec![Change::Put(Entry::new(7, 77, 0))],
            ORDINARY,
        )
        .unwrap();
    check_directory(&store, &BTreeMap::from([(7, 77)]));
    assert_eq!(store.root.pages.height(), 1);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn sparse_groups_keep_original_leaf_ranks_after_adjacent_removals_and_splits() {
    let budget = budget();
    let mut store = seed(&budget, 2050, 1);
    let mut expected = entries(&store.root);
    let changes = vec![
        Change::Delete(key(2049)),
        Change::Put(Entry::new(key(1000) + 2, 9002, 0)),
        Change::Delete(key(1)),
        Change::Put(Entry::new(key(31) - 1, 9031, 0)),
        Change::Delete(key(31)),
        Change::Put(Entry::new(key(2048) + 1, 9048, 0)),
        Change::Delete(key(0)),
        Change::Delete(key(2048)),
        Change::Put(Entry::new(key(31) + 1, 9032, 0)),
        Change::Delete(key(1001)),
        Change::Put(Entry::new(key(1000) + 1, 9001, 0)),
    ];
    for change in &changes {
        match change {
            Change::Put(row) => {
                expected.insert(row.key, row.value);
            }
            Change::Delete(key) => {
                assert!(expected.remove(key).is_some());
            }
        }
    }
    let before = budget.stats();
    let plan = store.plan_batch(2, changes, ORDINARY, usize::MAX).unwrap();
    assert_eq!(plan.output_pages(), expected.len());
    assert_eq!(budget.stats(), before);
    let charges = plan.charges();
    let candidate = plan.build().unwrap();
    assert_eq!(entries(&candidate.root), expected);
    assert!(candidate.root.pages.check_shape());
    assert!(budget.stats().used - before.used <= charges.additional_retained_bytes());
    assert_eq!(store.len(), 2050);
    store.publish(candidate).unwrap();
    check_directory(&store, &expected);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

fn random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[test]
fn randomized_multi_level_directory_batches_match_an_independent_ordered_map() {
    for initial_seed in [19, 873, 145_541] {
        let budget = budget();
        let mut store = seed(&budget, 2049, 2);
        assert!(store.stats().pages > 1024);
        let mut expected = entries(&store.root);
        let mut rng = initial_seed;
        for round in 0..128 {
            let live: Vec<_> = expected.keys().copied().collect();
            let mut next = expected.clone();
            let mut selected = std::collections::BTreeSet::new();
            let mut changes = Vec::new();
            for slot in 0..(random(&mut rng) % 21) {
                let number = random(&mut rng);
                let key = if slot % 2 == 0 && !live.is_empty() {
                    live[usize::try_from(number % u64::try_from(live.len()).unwrap()).unwrap()]
                } else {
                    number % 12_288
                };
                if !selected.insert(key) {
                    continue;
                }
                if next.contains_key(&key) && !random(&mut rng).is_multiple_of(3) {
                    next.remove(&key);
                    changes.push(Change::Delete(key));
                } else {
                    let value = random(&mut rng);
                    next.insert(key, value);
                    changes.push(Change::Put(Entry::new(key, value, 0)));
                }
            }
            let retry = changes.clone();
            let before = budget.stats();
            let plan = store
                .plan_batch(store.prefix() + 1, changes, ORDINARY, usize::MAX)
                .unwrap();
            assert_eq!(budget.stats(), before);
            let charges = plan.charges();
            let mut candidate = plan.build().unwrap();
            assert_eq!(entries(&candidate.root), next);
            assert!(candidate.root.pages.check_shape());
            assert!(budget.stats().used - before.used <= charges.additional_retained_bytes());
            assert_eq!(entries(&store.root), expected);
            if round % 11 == 0 {
                drop(candidate);
                assert_eq!(budget.stats(), before);
                candidate = store
                    .prepare_batch(store.prefix() + 1, retry, ORDINARY)
                    .unwrap();
                assert_eq!(entries(&candidate.root), next);
            }
            store.publish(candidate).unwrap();
            expected = next;
            check_directory(&store, &expected);
        }
        drop(store);
        assert_eq!(budget.stats().used, 0);
    }
}

#[derive(Debug)]
struct CountedKey<'a> {
    number: u64,
    clones: &'a Cell<usize>,
}

impl Clone for CountedKey<'_> {
    fn clone(&self) -> Self {
        self.clones.set(self.clones.get() + 1);
        Self {
            number: self.number,
            clones: self.clones,
        }
    }
}
impl PartialEq for CountedKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.number == other.number
    }
}
impl Eq for CountedKey<'_> {}
impl PartialOrd for CountedKey<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CountedKey<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.number.cmp(&other.number)
    }
}

#[test]
fn directory_splits_empty_prefixes_and_sparse_replacements_never_clone_separator_keys() {
    let budget = budget();
    let clones = Cell::new(0);
    let key = |number| CountedKey {
        number,
        clones: &clones,
    };
    let mut store = RangeStore::new(RangeId(802), 0, config(1), budget.clone()).unwrap();
    store
        .apply_batch(
            1,
            (0..2049)
                .map(|number| Change::Put(Entry::new(key(number), number + 10, 0)))
                .collect(),
            ORDINARY,
        )
        .unwrap();
    assert_eq!(clones.get(), 0);
    assert!(store.root.pages.check_shape());
    let untouched = Arc::clone(store.root.pages.get(900).unwrap());
    let empty = store.prepare_batch(2, vec![], ORDINARY).unwrap();
    assert_eq!(clones.get(), 0);
    assert!(Arc::ptr_eq(empty.root.pages.get(900).unwrap(), &untouched));
    store.publish(empty).unwrap();
    let candidate = store
        .prepare_batch(
            3,
            vec![
                Change::Put(Entry::new(key(2048), 30, 0)),
                Change::Delete(key(1000)),
                Change::Put(Entry::new(key(3), 20, 0)),
            ],
            ORDINARY,
        )
        .unwrap();
    assert_eq!(clones.get(), 0);
    assert!(candidate.root.pages.check_shape());
    assert!(Arc::ptr_eq(
        candidate.root.pages.get(900).unwrap(),
        &untouched
    ));
    assert_eq!(candidate.get(&key(3)), Some(&20));
    assert_eq!(candidate.get(&key(2048)), Some(&30));
    assert_eq!(candidate.get(&key(1000)), None);
    assert_eq!(candidate.get(&key(900)), Some(&910));
    assert_eq!(clones.get(), 0);
    store.publish(candidate).unwrap();
    drop(untouched);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn single_leaf_quotes_grow_with_directory_height_and_empty_quotes_ignore_leaf_count() {
    let mut directory = Vec::new();
    let mut empty = Vec::new();
    for count in [32, 1024, 4096] {
        let budget = budget();
        let store = seed(&budget, count, 1);
        let before = budget.stats();
        let plan = store
            .plan_batch(
                2,
                vec![Change::Put(Entry::new(key(count / 2), 123, 0))],
                ORDINARY,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(budget.stats(), before);
        let charge = plan.charges();
        directory.push(charge.directory_bytes());
        let candidate = plan.build().unwrap();
        assert!(candidate.root.pages.check_shape());
        assert!(budget.stats().used - before.used <= charge.additional_retained_bytes());
        drop(candidate);
        assert_eq!(budget.stats(), before);
        let plan = store.plan_batch(2, vec![], ORDINARY, usize::MAX).unwrap();
        assert_eq!(plan.charges().new_pages_bytes(), 0);
        empty.push(plan.charges().directory_bytes());
        let candidate = plan.build().unwrap();
        assert!(Arc::ptr_eq(
            candidate.root.pages.get(count / 2).unwrap(),
            store.root.pages.get(count / 2).unwrap()
        ));
        drop(candidate);
        assert_eq!(budget.stats(), before);
        drop(store);
        assert_eq!(budget.stats().used, 0);
    }
    // Four times as many leaves must not require four times the directory
    // allocation for a one-leaf update. Bounds allow conservative path quotes.
    assert!(directory[2] <= directory[1] * 2, "{directory:?}");
    assert!(directory[2] <= directory[0] * 4, "{directory:?}");
    assert_eq!(empty[0], empty[1]);
    assert_eq!(empty[1], empty[2]);
}

#[test]
fn old_scan_continuations_and_pending_forks_retain_their_exact_large_directory() {
    let budget = budget();
    let mut store = seed(&budget, 2049, 1);
    let original: Vec<_> = store.entries().map(|row| (row.key, row.value)).collect();
    let lease = store.pin(1, 1000).unwrap();
    let query = ScanQuery::all();
    let read = ReadBudget {
        max_items: 37,
        max_bytes: 64 * 1024,
        max_edge_visits: 1,
    };
    let mut page = lease.scan(&query, read, None, 2).unwrap();
    let mut scanned: Vec<_> = page
        .items()
        .iter()
        .map(|row| (row.key, row.value))
        .collect();
    let mut continuation = page.continuation.take();
    assert!(continuation.is_some());
    drop(page);
    let first = store
        .prepare_batch(
            2,
            vec![
                Change::Delete(key(0)),
                Change::Put(Entry::new(key(0) + 2, 22, 0)),
                Change::Put(Entry::new(key(1024), 222, 0)),
            ],
            ORDINARY,
        )
        .unwrap();
    let fork = store
        .prepare_batch(2, vec![Change::Put(Entry::new(key(0), 999, 0))], ORDINARY)
        .unwrap();
    let suffix = store
        .prepare_after(
            &first,
            3,
            vec![
                Change::Delete(key(0) + 2),
                Change::Delete(key(2048)),
                Change::Put(Entry::new(key(0), 333, 0)),
            ],
            ORDINARY,
        )
        .unwrap();
    store.validate_chain([&first, &suffix]).unwrap();
    let before = budget.stats();
    let (error, suffix) = store.publish_recoverable(suffix).unwrap_err();
    assert!(matches!(error, MemoryError::StalePreparation { .. }));
    assert_eq!(budget.stats(), before);
    assert_eq!(suffix.get(&key(0)), Some(&333));
    store.publish(first).unwrap();
    let before = budget.stats();
    let (error, fork) = store.publish_recoverable(fork).unwrap_err();
    assert!(matches!(error, MemoryError::StalePreparation { .. }));
    assert_eq!(budget.stats(), before);
    assert_eq!(fork.get(&key(0)), Some(&999));
    drop(fork);
    store.publish(suffix).unwrap();
    assert_eq!(store.get(&key(0)), Some(&333));
    assert_eq!(store.get(&key(2048)), None);
    assert!(store.root.pages.check_shape());
    while continuation.is_some() {
        let mut page = lease.scan(&query, read, continuation.take(), 3).unwrap();
        assert_eq!(page.prefix, 1);
        scanned.extend(page.items().iter().map(|row| (row.key, row.value)));
        continuation = page.continuation.take();
    }
    assert_eq!(scanned, original);
    assert_eq!(lease.get(&key(0), 3).unwrap().items()[0].value, key(0) ^ 73);
    store.release(&lease).unwrap();
    drop(lease);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn every_multi_level_directory_allocation_failure_preserves_base_pin_and_all_charges() {
    use preflight_tests::{AllocationFault, count_allocations, fault, fault_consumed};

    let budget = budget();
    let mut store = seed(&budget, 2049, 1);
    assert!(store.root.pages.height() >= 3);
    let base = Arc::clone(&store.root);
    let original = entries(&base);
    let lease = store.pin(1, 1000).unwrap();
    let baseline = budget.stats();
    let range_baseline = store.stats();
    let changes = || {
        let mut rows = vec![
            // Early deletions underflow the initial minimum-width left-hand
            // directory nodes; later groups cannot rely on their old ranks.
            Change::Delete(key(0)),
            Change::Delete(key(1)),
            Change::Delete(key(513)),
            Change::Put(Entry::new(key(256) + 1, 80_001, 0)),
            Change::Put(Entry::new(key(1024), 80_002, 0)),
        ];
        // The original rightmost directory leaf has 17 handles. Seventeen
        // additional pages force a split, after unrelated deletion/replace
        // paths and actual retained-value copying have already completed.
        rows.extend(
            (1..=17).map(|offset| Change::Put(Entry::new(key(2048) + offset, 90_000 + offset, 0))),
        );
        rows.reverse();
        rows
    };
    let mut expected = original.clone();
    for change in changes() {
        match change {
            Change::Put(row) => {
                expected.insert(row.key, row.value);
            }
            Change::Delete(key) => {
                assert!(expected.remove(&key).is_some());
            }
        }
    }

    let plan = store
        .plan_batch(2, changes(), ORDINARY, usize::MAX)
        .unwrap();
    let quote = plan.charges();
    let copied = Cell::new(0);
    let (reference, sites) = count_allocations(|| {
        plan.build_with(|value| {
            copied.set(copied.get() + 1);
            Ok(*value)
        })
        .unwrap()
    });
    assert!(sites > 32, "fixture must reach branch and leaf allocations");
    assert!(copied.get() > 0);
    assert!(reference.root.pages.check_shape());
    assert_eq!(entries(&reference.root), expected);
    preflight_tests::assert_retained(&baseline, &budget.stats(), quote);
    drop(reference);
    assert_eq!(budget.stats(), baseline);

    for kind in [AllocationFault::Fail, AllocationFault::Excess] {
        let mut refused_after_copying = false;
        for at in 1..=sites {
            let plan = store
                .plan_batch(2, changes(), ORDINARY, usize::MAX)
                .unwrap();
            assert_eq!(plan.charges(), quote);
            assert_eq!(budget.stats(), baseline);
            let _fault = fault(at, kind);
            copied.set(0);
            let (result, reached) = count_allocations(|| {
                plan.build_with(|value| {
                    copied.set(copied.get() + 1);
                    Ok(*value)
                })
            });
            match kind {
                AllocationFault::Fail => {
                    assert!(matches!(result, Err(MemoryError::AllocationFailed)))
                }
                AllocationFault::Excess => assert!(
                    matches!(result, Err(MemoryError::Capacity { requested, available }) if requested > available)
                ),
            }
            assert_eq!(reached, at, "directory allocation site {at}");
            assert!(
                fault_consumed(),
                "directory allocation {at} was not reached"
            );
            refused_after_copying |= copied.get() != 0;
            assert_eq!(budget.stats(), baseline, "directory allocation site {at}");
            assert_eq!(store.stats(), range_baseline);
            assert!(Arc::ptr_eq(&store.root, &base));
            check_directory(&store, &original);
            assert_eq!(
                lease
                    .project_next(&key(0), false, &u64::MAX, 1, |row| { (row.key, row.value) })
                    .unwrap(),
                Some((key(0), key(0) ^ 73))
            );
            assert_eq!(
                lease
                    .project_next(&key(1024), false, &(key(1024) + 1), 1, |row| { row.value })
                    .unwrap(),
                Some(key(1024) ^ 73)
            );
        }
        assert!(refused_after_copying);
    }

    let candidate = store.prepare_batch(2, changes(), ORDINARY).unwrap();
    store.publish(candidate).unwrap();
    check_directory(&store, &expected);
    assert_eq!(entries(&base), original);
    assert_eq!(
        lease.get(&key(1024), 1).unwrap().items()[0].value,
        key(1024) ^ 73
    );
    store.release(&lease).unwrap();
    drop(lease);
    drop(base);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}
