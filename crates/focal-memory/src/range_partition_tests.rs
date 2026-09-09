use super::*;
use std::cell::Cell;

type Key = (u64, u64);

// Deliberately not Clone: mutation tests must use the fallible value copier.
#[derive(Debug, PartialEq, Eq)]
struct Value {
    key: Key,
    bytes: Vec<u8>,
}

fn partition(key: &Key) -> u64 {
    key.0
}

fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 8,
        page_bytes: page_charge::<Key, Value>(8, 8 * (8 + ALLOCATOR_OVERHEAD)).unwrap(),
        max_entry_bytes: size_of::<Entry<Key, Value>>() + 2048 + ALLOCATOR_OVERHEAD,
        max_batch_entries: 32,
        ..RangeConfig::default()
    }
}

fn entry(key: Key, bytes: usize) -> Entry<Key, Value> {
    let bytes = vec![17; bytes];
    let heap = bytes.capacity() + usize::from(!bytes.is_empty()) * ALLOCATOR_OVERHEAD;
    Entry::new(key, Value { key, bytes }, heap)
}

fn put(key: Key) -> Change<Key, Value> {
    Change::Put(entry(key, 8))
}

fn copy(value: &Value) -> Result<Value, MemoryError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(value.bytes.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    bytes.extend_from_slice(&value.bytes);
    Ok(Value {
        key: value.key,
        bytes,
    })
}

fn no_copy(_: &Value) -> Result<Value, MemoryError> {
    panic!("an unchanged partition or incoming owned row reached the copier")
}

fn seeded(budget: &MemoryBudget, rows: &[Key]) -> RangeStore<Key, Value> {
    let mut store =
        RangeStore::new_partitioned(RangeId(991), 0, config(), budget.clone(), partition).unwrap();
    let candidate = store
        .prepare_batch_with(
            1,
            rows.iter().copied().map(put).collect(),
            BudgetLane::Ordinary,
            no_copy,
        )
        .unwrap();
    store.publish(candidate).unwrap();
    store
}

fn keys(root: &Root<Key, Value>) -> Vec<Vec<Key>> {
    root.pages
        .iter()
        .map(|page| page.entries.iter().map(|row| row.key).collect())
        .collect()
}

fn check_layout(root: &Root<Key, Value>, config: RangeConfig) {
    let mut previous = None;
    let mut count = 0;
    for page in &root.pages {
        let label = page.entries.first().unwrap().key.0;
        assert!(page.entries.len() <= config.page_entries);
        let mut heap = 0;
        for entry in &page.entries {
            assert_eq!(entry.key.0, label);
            assert!(previous.is_none_or(|key| key < entry.key));
            assert!(entry.read_bytes().unwrap() <= config.max_entry_bytes);
            previous = Some(entry.key);
            heap += entry.heap_bytes;
            count += 1;
        }
        let charge = page_charge::<Key, Value>(page.entries.len(), heap).unwrap();
        assert_eq!(page._allocation.bytes(), charge);
        assert!(charge <= config.page_bytes || page.entries.len() == 1);
    }
    assert_eq!(count, root.len);
}

#[test]
fn small_content_rows_are_shared_across_hot_updates_with_a_pinned_original_view() {
    let budget = MemoryBudget::new(4_000_000, 500_000).unwrap();
    let rows = [(1, 10), (1, 20), (2, 10), (2, 20)];
    // The unchanged default would place all four small rows in one leaf.
    {
        let plain = RangeStore::new(RangeId(992), 0, config(), budget.clone()).unwrap();
        let plain_plan = plain
            .plan_batch(
                1,
                rows.into_iter().map(put).collect(),
                BudgetLane::Ordinary,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(plain_plan.output_pages(), 1);
        let candidate = plain_plan.build_with(no_copy).unwrap();
        assert_eq!(keys(&candidate.root), vec![rows.to_vec()]);
    }

    let mut store = seeded(&budget, &rows);
    let lease = store.pin(0, 100).unwrap();
    assert_eq!(
        keys(&store.root),
        vec![vec![(1, 10), (1, 20)], vec![(2, 10), (2, 20)]]
    );
    let content = Arc::clone(store.root.pages.get(0).unwrap());
    let pointer = store.get(&(1, 10)).unwrap().bytes.as_ptr();
    let mut copied = Vec::new();
    let candidate = store
        .prepare_batch_with(
            2,
            vec![Change::Put(entry((2, 10), 16))],
            BudgetLane::Ordinary,
            |old| {
                assert_eq!(old.key.0, 2, "immutable content was copied by a hot update");
                copied.push(old.key);
                copy(old)
            },
        )
        .unwrap();
    assert_eq!(copied, [(2, 20)]);
    assert!(Arc::ptr_eq(candidate.root.pages.get(0).unwrap(), &content));
    assert_eq!(candidate.get(&(1, 10)).unwrap().bytes.as_ptr(), pointer);
    store.publish(candidate).unwrap();
    assert_eq!(store.get(&(2, 10)).unwrap().bytes.len(), 16);
    assert_eq!(
        lease
            .project_next(&(2, 10), false, &(2, 11), 0, |entry| entry
                .value
                .bytes
                .len())
            .unwrap(),
        Some(8)
    );
    check_layout(&store.root, config());
    store.release(&lease).unwrap();
    drop(lease);
    drop(content);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn inserting_namespaces_on_both_edges_reuses_the_entire_small_content_page() {
    let budget = MemoryBudget::new(4_000_000, 500_000).unwrap();
    let mut store = seeded(&budget, &[(1, 10), (1, 20)]);
    let old = Arc::clone(store.root.pages.get(0).unwrap());
    let pointer = store.get(&(1, 20)).unwrap().bytes.as_ptr();
    let before = budget.stats();
    let plan = store
        .plan_batch(
            2,
            vec![put((2, 20)), put((0, 20)), put((2, 10)), put((0, 10))],
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap();
    assert_eq!(plan.output_pages(), 3);
    assert_eq!(
        budget.stats(),
        before,
        "partition planning is allocation-free"
    );
    let charges = plan.charges();
    let candidate = plan.build_with(no_copy).unwrap();
    preflight_tests::assert_retained(&before, &budget.stats(), charges);
    assert_eq!(
        keys(&candidate.root),
        vec![
            vec![(0, 10), (0, 20)],
            vec![(1, 10), (1, 20)],
            vec![(2, 10), (2, 20)]
        ]
    );
    assert!(Arc::ptr_eq(candidate.root.pages.get(1).unwrap(), &old));
    assert_eq!(candidate.get(&(1, 20)).unwrap().bytes.as_ptr(), pointer);
    // An unpublished successor can delete both adjacent namespaces while
    // retaining the original content page and the complete predecessor view.
    let successor = store
        .prepare_after_with(
            &candidate,
            3,
            vec![
                Change::Delete((0, 10)),
                Change::Delete((0, 20)),
                Change::Delete((2, 10)),
                Change::Delete((2, 20)),
            ],
            BudgetLane::Ordinary,
            no_copy,
        )
        .unwrap();
    assert_eq!(keys(&successor.root), vec![vec![(1, 10), (1, 20)]]);
    assert!(Arc::ptr_eq(successor.root.pages.get(0).unwrap(), &old));
    assert_eq!(candidate.len(), 6);
    store.publish(candidate).unwrap();
    store.publish(successor).unwrap();
    check_layout(&store.root, config());
    drop(old);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn partition_byte_count_and_oversized_splits_have_exact_preflight_leaf_charges() {
    let budget = MemoryBudget::new(4_000_000, 500_000).unwrap();
    for config in [
        RangeConfig {
            page_entries: 2,
            ..config()
        },
        RangeConfig {
            page_bytes: page_charge::<Key, Value>(2, 2 * (8 + ALLOCATOR_OVERHEAD)).unwrap(),
            ..config()
        },
    ] {
        let store = RangeStore::new_partitioned(RangeId(993), 0, config, budget.clone(), partition)
            .unwrap();
        let rows = vec![
            put((0, 10)),
            put((0, 20)),
            put((0, 30)),
            put((1, 10)),
            Change::Put(entry((1, 20), 1024)),
            put((1, 30)),
            put((2, 10)),
        ];
        let bound = RangeWriteLimits {
            changed_keys: rows.len(),
            deleted_keys: 0,
            deleted_heap: 0,
            incoming_heap: rows
                .iter()
                .map(|row| match row {
                    Change::Put(entry) => entry.heap_bytes,
                    Change::Delete(_) => 0,
                })
                .sum(),
            input_capacity: rows.capacity(),
        };
        let envelope = store.future_write_envelope(bound).unwrap();
        let before = budget.stats();
        let plan = store
            .plan_batch(1, rows, BudgetLane::Ordinary, usize::MAX)
            .unwrap();
        assert_eq!(plan.output_pages(), 6);
        envelope.check_plan(&plan).unwrap();
        let charges = plan.charges();
        let candidate = plan.build_with(no_copy).unwrap();
        assert_eq!(
            keys(&candidate.root),
            vec![
                vec![(0, 10), (0, 20)],
                vec![(0, 30)],
                vec![(1, 10)],
                vec![(1, 20)],
                vec![(1, 30)],
                vec![(2, 10)]
            ]
        );
        check_layout(&candidate.root, config);
        preflight_tests::assert_retained(&before, &budget.stats(), charges);
        drop(candidate);
        assert_eq!(budget.stats(), before);
    }
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn funded_partitioned_update_rolls_back_under_pressure_and_preserves_the_content_page() {
    let budget = MemoryBudget::new(4_000_000, 500_000).unwrap();
    let mut store = seeded(&budget, &[(1, 10), (1, 20), (2, 10), (2, 20)]);
    let lease = store.pin(0, 100).unwrap();
    let content = Arc::clone(store.root.pages.get(0).unwrap());
    let changes = || vec![put((0, 10)), put((2, 15)), put((3, 10))];
    let envelope = store
        .future_write_envelope(RangeWriteLimits {
            changed_keys: 3,
            deleted_keys: 0,
            deleted_heap: 0,
            incoming_heap: 3 * (8 + ALLOCATOR_OVERHEAD),
            input_capacity: 3,
        })
        .unwrap();
    let plan = store
        .plan_batch(2, changes(), BudgetLane::Completion, usize::MAX)
        .unwrap();
    envelope.check_plan(&plan).unwrap();
    let charges = plan.charges();
    let pool = budget
        .funded_child(BudgetLane::Ordinary, charges.additional_peak_bytes())
        .unwrap();
    let unspent = pool.stats();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    let parent = budget.stats();
    let mut copied = Vec::new();
    let refused = plan.build_in_with(&pool, |old| {
        assert_eq!(old.key.0, 2);
        copied.push(old.key);
        if old.key == (2, 20) {
            return Err(MemoryError::AllocationFailed);
        }
        copy(old)
    });
    assert!(matches!(refused, Err(MemoryError::AllocationFailed)));
    assert_eq!(copied, [(2, 10), (2, 20)]);
    assert_eq!(pool.stats(), unspent);
    assert_eq!(budget.stats(), parent);
    assert_eq!(store.prefix(), 1);
    assert!(Arc::ptr_eq(store.root.pages.get(0).unwrap(), &content));
    assert!(matches!(
        store.plan_batch(
            2,
            changes(),
            BudgetLane::Completion,
            charges.additional_peak_bytes() - 1
        ),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(budget.stats(), parent);
    let plan = store
        .plan_batch(
            2,
            changes(),
            BudgetLane::Completion,
            charges.additional_peak_bytes(),
        )
        .unwrap();
    assert_eq!(plan.charges(), charges);
    envelope.check_plan(&plan).unwrap();
    let candidate = plan
        .build_in_with(&pool, |old| {
            assert_eq!(old.key.0, 2);
            assert_eq!(budget.stats().used, parent.used);
            copy(old)
        })
        .unwrap();
    preflight_tests::assert_retained(&unspent, &pool.stats(), charges);
    assert!(
        candidate
            .root
            .pages
            .iter()
            .any(|page| Arc::ptr_eq(page, &content))
    );
    check_layout(&candidate.root, config());
    // Discard restores the whole pool; the same exact source can retry.
    drop(candidate);
    assert_eq!(pool.stats(), unspent);
    assert_eq!(budget.stats(), parent);
    let candidate = store
        .plan_batch(
            2,
            changes(),
            BudgetLane::Completion,
            charges.additional_peak_bytes(),
        )
        .unwrap()
        .build_in_with(&pool, |old| {
            assert_eq!(old.key.0, 2);
            copy(old)
        })
        .unwrap();
    store.publish(candidate).unwrap();
    assert_eq!(store.get(&(2, 15)).unwrap().bytes.len(), 8);
    assert_eq!(
        lease
            .project_next(&(2, 15), false, &(2, 16), 0, |entry| entry.key)
            .unwrap(),
        None
    );
    assert_eq!(budget.stats().used, parent.used);
    drop(pressure);
    store.release(&lease).unwrap();
    drop(lease);
    drop(content);
    drop(store);
    assert_eq!(pool.stats().used, 0);
    drop(pool);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn partitioned_import_retains_small_namespace_rows_and_refuses_invalid_order_atomically() {
    #[derive(Debug)]
    struct Imported<'a> {
        key: Key,
        copies: &'a Cell<usize>,
    }
    impl Clone for Imported<'_> {
        fn clone(&self) -> Self {
            self.copies.set(self.copies.get() + 1);
            Self {
                key: self.key,
                copies: self.copies,
            }
        }
    }
    let budget = MemoryBudget::new(4_000_000, 500_000).unwrap();
    let copies = Cell::new(0);
    let incoming = |key| {
        Entry::new(
            key,
            Imported {
                key,
                copies: &copies,
            },
            0,
        )
    };
    let config = RangeConfig {
        page_entries: 8,
        page_bytes: 4096,
        max_entry_bytes: 4096,
        ..RangeConfig::default()
    };
    let mut store = RangeStore::from_entries_partitioned(
        RangeId(994),
        50,
        config,
        budget.clone(),
        [(1, 10), (1, 20), (2, 10), (2, 20)].map(incoming),
        partition,
    )
    .unwrap();
    assert_eq!(store.prefix(), 50);
    assert_eq!(store.stats().pages, 2);
    assert_eq!(
        copies.get(),
        0,
        "importing an adjacent namespace copied completed content"
    );
    let content = Arc::clone(store.root.pages.get(0).unwrap());
    let candidate = store
        .prepare_batch_with(
            51,
            vec![
                Change::Put(incoming((0, 10))),
                Change::Put(incoming((3, 10))),
            ],
            BudgetLane::Ordinary,
            |old| {
                assert_eq!(old.key.0, 2);
                Ok(old.clone())
            },
        )
        .unwrap();
    assert_eq!(copies.get(), 0);
    assert!(
        candidate
            .root
            .pages
            .iter()
            .any(|page| Arc::ptr_eq(page, &content))
    );
    store.publish(candidate).unwrap();
    drop(content);
    drop(store);
    assert_eq!(budget.stats().used, 0);
    for keys in [[(1, 10), (1, 10)], [(2, 10), (1, 10)]] {
        assert!(matches!(
            RangeStore::from_entries_partitioned(
                RangeId(995),
                50,
                config,
                budget.clone(),
                keys.map(incoming),
                partition
            ),
            Err(MemoryError::InvalidConfiguration(_))
        ));
        assert_eq!(budget.stats().used, 0);
    }
}
