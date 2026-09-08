use super::*;

fn store(count: u64) -> RangeStore<u64, u64> {
    let mut store = RangeStore::new(
        RangeId(9_701),
        0,
        RangeConfig {
            page_entries: 2,
            max_batch_entries: 256,
            ..RangeConfig::default()
        },
        MemoryBudget::new(8 * 1024 * 1024, 1024 * 1024).unwrap(),
    )
    .unwrap();
    store
        .apply_batch(
            1,
            (1..=count)
                .map(|id| Change::Put(Entry::new(id * 4, id, 0)))
                .collect(),
            BudgetLane::Ordinary,
        )
        .unwrap();
    store
}

#[test]
fn lower_bound_handles_empty_gaps_page_boundaries_and_directory_branches() {
    for count in [0, 1, 4, 70] {
        let store = store(count);
        let all: BTreeMap<_, _> = (1..=count).map(|id| (id * 4, id)).collect();
        for lower in [0, 1, 4, 7, 8, 9, 128, 129, 256, 279, 280, u64::MAX] {
            for exclusive in [false, true] {
                let expected: Vec<_> = all
                    .iter()
                    .filter(|(key, _)| **key > lower || (!exclusive && **key == lower))
                    .map(|(&key, &value)| (key, value))
                    .collect();
                let actual: Vec<_> = store
                    .entries_from(&lower, exclusive)
                    .map(|entry| (entry.key, entry.value))
                    .collect();
                assert_eq!(
                    actual, expected,
                    "{count} rows, lower {lower}, exclusive {exclusive}"
                );
            }
        }
    }
}

#[test]
fn committed_and_pending_cursors_keep_their_exact_prefix_and_release_the_search_key() {
    let mut store = store(4);
    let first = store
        .prepare_batch(
            2,
            vec![
                Change::Delete(8),
                Change::Put(Entry::new(9, 90, 0)),
                Change::Put(Entry::new(12, 120, 0)),
            ],
            BudgetLane::Ordinary,
        )
        .unwrap();
    let second = store
        .prepare_after(
            &first,
            3,
            vec![Change::Delete(12), Change::Put(Entry::new(15, 150, 0))],
            BudgetLane::Ordinary,
        )
        .unwrap();
    assert_eq!(store.prefix(), 1);
    assert_eq!(first.prefix(), 2);
    assert_eq!(second.prefix(), 3);
    let actual: Vec<_> = {
        let lower = 8;
        store.entries_from(&lower, false)
    }
    .map(|entry| (entry.key, entry.value))
    .collect();
    assert_eq!(actual, [(8, 2), (12, 3), (16, 4)]);
    let actual: Vec<_> = {
        let lower = 8;
        first.entries_from(&lower, false)
    }
    .map(|entry| (entry.key, entry.value))
    .collect();
    assert_eq!(actual, [(9, 90), (12, 120), (16, 4)]);
    let actual: Vec<_> = second
        .entries_from(&8, false)
        .map(|entry| (entry.key, entry.value))
        .collect();
    assert_eq!(actual, [(9, 90), (15, 150), (16, 4)]);

    store.publish(first).unwrap();
    assert_eq!(store.entries_from(&12, false).next().unwrap().value, 120);
    assert_eq!(second.entries_from(&12, false).next().unwrap().key, 15);
    drop(second);
    assert_eq!(store.prefix(), 2);
    assert_eq!(store.entries_from(&12, true).next().unwrap().key, 16);
}

#[test]
fn bounded_borrowed_scans_need_no_capacity_or_new_root_handles() {
    let store = store(70);
    let next = store
        .prepare_batch(
            2,
            vec![Change::Put(Entry::new(282, 282, 0))],
            BudgetLane::Ordinary,
        )
        .unwrap();
    let budget = &store.budget;
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.limit() - budget.stats().used,
        )
        .unwrap()
        .commit();
    let before = budget.stats();
    let committed_handles = Arc::strong_count(&store.root);
    let pending_handles = Arc::strong_count(&next.root);
    let _fault = preflight_tests::fault(1, preflight_tests::AllocationFault::Fail);
    let ((committed, pending), allocations) = preflight_tests::count_allocations(|| {
        let committed = store.entries_from(&256, true);
        let pending = next.entries_from(&256, true);
        assert_eq!(Arc::strong_count(&store.root), committed_handles);
        assert_eq!(Arc::strong_count(&next.root), pending_handles);
        (
            committed.take(3).fold(0, |sum, entry| sum + entry.key),
            pending.take(3).fold(0, |sum, entry| sum + entry.key),
        )
    });
    assert_eq!(committed, 260 + 264 + 268);
    assert_eq!(pending, committed);
    assert_eq!(allocations, 0);
    assert!(!preflight_tests::fault_consumed());
    assert_eq!(budget.stats(), before);
    drop(pressure);
}
