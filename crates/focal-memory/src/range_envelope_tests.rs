use super::*;

fn budget() -> MemoryBudget {
    MemoryBudget::new(128 * 1024 * 1024, 1024 * 1024).unwrap()
}

fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 16,
        page_bytes: 4096,
        max_entry_bytes: 64 * 1024,
        max_batch_entries: 2048,
        ..RangeConfig::default()
    }
}

fn limits(changed_keys: usize, deleted_keys: usize, incoming_heap: usize) -> RangeWriteLimits {
    RangeWriteLimits {
        changed_keys,
        deleted_keys,
        incoming_heap,
        input_capacity: changed_keys,
    }
}

fn put(key: u64, bytes: usize) -> Change<u64, Vec<u8>> {
    let value = vec![key as u8; bytes];
    let heap = value.capacity() + usize::from(value.capacity() != 0) * ALLOCATOR_OVERHEAD;
    Change::Put(Entry::new(key, value, heap))
}

#[test]
fn future_envelope_is_unchanged_after_directory_growth_and_pinned_prefixes() {
    let budget = budget();
    let mut store = RangeStore::new(RangeId(870), 0, config(), budget.clone()).unwrap();
    let limits = limits(5, 1, 100_000);
    let envelope = store.future_write_envelope(limits).unwrap();
    let before = budget.stats().used;
    assert_eq!(store.future_write_envelope(limits).unwrap(), envelope);
    assert_eq!(budget.stats().used, before, "quoting allocates no budget");
    store
        .apply_batch(
            1,
            (0..1600).map(|i| put(i * 10, 100)).collect(),
            BudgetLane::Ordinary,
        )
        .unwrap();
    let pinned = store.pin(0, 100).unwrap();
    assert_eq!(store.future_write_envelope(limits).unwrap(), envelope);
    let changes = vec![
        put(1, 30_000),
        put(2, 120),
        Change::Delete(4000),
        put(8001, 200),
        put(15999, 100),
    ];
    let plan = store
        .plan_batch(
            2,
            changes,
            BudgetLane::Completion,
            envelope.additional_peak_bytes(),
        )
        .unwrap();
    envelope.check_plan(&plan).unwrap();
    let first = plan.build().unwrap();
    let plan = store
        .plan_after(
            &first,
            3,
            vec![put(3, 40_000), put(8002, 300)],
            BudgetLane::Completion,
            envelope.additional_peak_bytes(),
        )
        .unwrap();
    envelope.check_plan(&plan).unwrap();
    let second = plan.build().unwrap();
    store.publish(first).unwrap();
    store.publish(second).unwrap();
    assert_eq!(store.future_write_envelope(limits).unwrap(), envelope);
    assert_eq!(store.get(&3).unwrap().len(), 40_000);
    assert!(store.get(&4000).is_none());
    let old = pinned.get(&4000, 1).unwrap();
    assert_eq!(old.items().len(), 1);
    assert_eq!(old.items().first().unwrap().key, 4000);
    drop(old);
    store.release(&pinned).unwrap();
    drop(pinned);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn put_only_envelope_does_not_charge_impossible_large_delete_payloads() {
    let budget = budget();
    let store: RangeStore<u64, Vec<u8>> =
        RangeStore::new(RangeId(871), 0, config(), budget).unwrap();
    let put_only = store.future_write_envelope(limits(11, 0, 2048)).unwrap();
    let with_delete = store.future_write_envelope(limits(11, 1, 2048)).unwrap();
    assert_eq!(
        put_only.input_pending_bytes(),
        ALLOCATOR_OVERHEAD + 11 * size_of::<Change<u64, Vec<u8>>>() + 2048
    );
    assert_eq!(
        with_delete.input_pending_bytes() - put_only.input_pending_bytes(),
        config().max_entry_bytes - size_of::<Entry<u64, Vec<u8>>>()
    );
    assert_eq!(put_only.new_pages_bytes(), with_delete.new_pages_bytes());
}

#[test]
fn empty_deletion_groups_and_reused_oversized_neighbors_fit_the_edit_bound() {
    let budget = budget();
    let mut store = RangeStore::new(
        RangeId(876),
        0,
        RangeConfig {
            page_entries: 1,
            ..config()
        },
        budget.clone(),
    )
    .unwrap();
    let envelope = store
        .future_write_envelope(limits(4, 2, 2 * (20 + ALLOCATOR_OVERHEAD)))
        .unwrap();
    store
        .apply_batch(
            1,
            vec![put(10, 100), put(20, 8000), put(30, 100), put(40, 8000)],
            BudgetLane::Ordinary,
        )
        .unwrap();
    let oversized = Arc::clone(store.root.pages.get(3).unwrap());
    let plan = store
        .plan_batch(
            2,
            vec![
                Change::Delete(10),
                Change::Delete(20),
                put(41, 20),
                put(42, 20),
            ],
            BudgetLane::Completion,
            envelope.additional_peak_bytes(),
        )
        .unwrap();
    envelope.check_plan(&plan).unwrap();
    // Two empty groups require two removals. The final untouched oversized
    // singleton is shared while its two new neighbors require two insertions.
    assert_eq!(
        plan.charges().directory_bytes(),
        root_charge::<u64, Vec<u8>>(0).unwrap()
            + PageDirectory::<u64, Vec<u8>>::edit_bound(4, 6).unwrap()
    );
    assert_eq!(
        plan.charges().new_pages_bytes(),
        2 * page_charge::<u64, Vec<u8>>(1, 20 + ALLOCATOR_OVERHEAD).unwrap()
    );
    let next = plan
        .build_with(|_| panic!("these deletes and insertions have no copied neighbor values"))
        .unwrap();
    assert!(Arc::ptr_eq(next.root.pages.get(1).unwrap(), &oversized));
    store.publish(next).unwrap();
    assert_eq!(
        store.entries().map(|entry| entry.key).collect::<Vec<_>>(),
        [30, 40, 41, 42]
    );
    drop(oversized);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn envelope_rejects_foreign_owner_shape_heap_delete_and_spare_capacity() {
    let budget = budget();
    let mut store = RangeStore::new(RangeId(872), 0, config(), budget.clone()).unwrap();
    store
        .apply_batch(1, vec![put(1, 100)], BudgetLane::Ordinary)
        .unwrap();
    let envelope = store.future_write_envelope(limits(1, 0, 200)).unwrap();
    let foreign = RangeStore::new(RangeId(872), 0, config(), budget.clone()).unwrap();
    let foreign = foreign
        .plan_batch(1, vec![put(2, 50)], BudgetLane::Completion, usize::MAX)
        .unwrap();
    assert_eq!(envelope.check_plan(&foreign), Err(MemoryError::WrongRange));
    for changes in [
        vec![put(2, 50), put(3, 50)],
        vec![put(2, 200)],
        vec![Change::Delete(1)],
        {
            let mut changes = Vec::with_capacity(10);
            changes.push(put(2, 50));
            changes
        },
    ] {
        let plan = store
            .plan_batch(2, changes, BudgetLane::Completion, usize::MAX)
            .unwrap();
        assert!(matches!(
            envelope.check_plan(&plan),
            Err(MemoryError::Capacity { .. })
        ));
    }
    assert_eq!(store.prefix(), 1);
    assert_eq!(store.len(), 1);
}

#[test]
fn unbounded_defaults_are_clamped_to_fixed_owner_capacity_and_checked_for_overflow() {
    let budget = MemoryBudget::new(4 * 1024 * 1024, 1024).unwrap();
    let store: RangeStore<u64, Vec<u8>> =
        RangeStore::new(RangeId(873), 0, RangeConfig::default(), budget.clone()).unwrap();
    let envelope = store.future_write_envelope(limits(2, 1, 128)).unwrap();
    assert_eq!(
        envelope.max_base_pages(),
        budget.stats().limit / page_charge::<u64, Vec<u8>>(1, 0).unwrap()
    );
    assert!(envelope.additional_peak_bytes() < 4 * budget.stats().limit);
    for bad in [
        limits(1, 2, 0),
        RangeWriteLimits {
            input_capacity: 0,
            ..limits(1, 0, 0)
        },
        limits(0, 0, 1),
        limits(RangeConfig::default().max_batch_entries + 1, 0, 0),
        limits(1, 0, usize::MAX),
        RangeWriteLimits {
            input_capacity: usize::MAX,
            ..limits(1, 0, 0)
        },
    ] {
        assert!(store.future_write_envelope(bad).is_err());
    }
    let empty = store.future_write_envelope(limits(0, 0, 0)).unwrap();
    assert_eq!(
        empty.additional_peak_bytes(),
        root_charge::<u64, Vec<u8>>(0).unwrap()
    );
    assert_eq!(empty.max_new_pages(), 0);
    let plan = store
        .plan_batch(
            1,
            Vec::new(),
            BudgetLane::Completion,
            empty.additional_peak_bytes(),
        )
        .unwrap();
    empty.check_plan(&plan).unwrap();
}

#[test]
fn envelope_funded_before_growth_completes_after_parent_capacity_is_exhausted() {
    let budget = budget();
    let mut store = RangeStore::new(RangeId(874), 0, config(), budget.clone()).unwrap();
    let envelope = store.future_write_envelope(limits(3, 0, 80_000)).unwrap();
    let pool = budget
        .funded_child(BudgetLane::Ordinary, envelope.additional_peak_bytes())
        .unwrap();
    store
        .apply_batch(
            1,
            (0..1500)
                .map(|i| put(i * 10, if i % 19 == 0 { 8000 } else { 100 }))
                .collect(),
            BudgetLane::Ordinary,
        )
        .unwrap();
    let original = store.pin(0, 100).unwrap();
    assert_eq!(
        store.future_write_envelope(envelope.limits()).unwrap(),
        envelope
    );
    let plan = store
        .plan_batch(
            2,
            vec![put(1, 20_000), put(2, 20_000), put(14999, 20_000)],
            BudgetLane::Completion,
            envelope.additional_peak_bytes(),
        )
        .unwrap();
    envelope.check_plan(&plan).unwrap();
    let pressure = budget
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    assert!(
        budget
            .reserve(BudgetKind::Pages, BudgetLane::Completion, 1)
            .is_err()
    );
    let candidate = plan.build_in(&pool).unwrap();
    assert!(pool.stats().used <= envelope.additional_peak_bytes());
    store.publish(candidate).unwrap();
    assert_eq!(store.get(&1).unwrap().len(), 20_000);
    // Allocation-free old-prefix projection still works under full pressure.
    assert_eq!(
        original
            .project_next(&0, false, &10, 1, |entry| entry.key)
            .unwrap(),
        Some(0)
    );
    drop(pressure);
    store.release(&original).unwrap();
    drop(original);
    drop(pool);
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn future_bound_covers_adversarial_byte_and_count_partitions_across_mixed_churn() {
    for (page_entries, page_bytes) in [(1, 256), (2, 700), (7, 2048), (32, 4096)] {
        let budget = budget();
        let cfg = RangeConfig {
            page_entries,
            page_bytes,
            ..config()
        };
        let mut store = RangeStore::new(RangeId(875), 0, cfg, budget.clone()).unwrap();
        let envelope = store
            .future_write_envelope(limits(8, 8, 8 * (8000 + ALLOCATOR_OVERHEAD)))
            .unwrap();
        let sizes = [0, 10, 100, 600, 1200, 8000];
        let mut keys: std::collections::BTreeSet<u64> = (0..64).map(|i| i * 4).collect();
        store
            .apply_batch(
                1,
                keys.iter()
                    .enumerate()
                    .map(|(i, key)| put(*key, sizes[i % sizes.len()]))
                    .collect(),
                BudgetLane::Ordinary,
            )
            .unwrap();
        let mut random = 0x2df8_3cd7_592b_331du64;
        for turn in 0..120 {
            let mut selected = std::collections::BTreeSet::new();
            let mut changes = Vec::with_capacity(8);
            let mut expected = keys.clone();
            while changes.len() < 8 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key = (random >> 24) % 512;
                if !selected.insert(key) {
                    continue;
                }
                if random.is_multiple_of(5) && keys.contains(&key) {
                    changes.push(Change::Delete(key));
                    expected.remove(&key);
                } else {
                    let size = sizes[((random >> 40) as usize) % sizes.len()];
                    changes.push(put(key, size));
                    expected.insert(key);
                }
            }
            let before = budget.stats().used;
            let plan = store
                .plan_batch(
                    store.prefix() + 1,
                    changes,
                    BudgetLane::Completion,
                    envelope.additional_peak_bytes(),
                )
                .unwrap();
            envelope.check_plan(&plan).unwrap();
            let candidate = plan
                .build_with(|value| {
                    let heap =
                        value.capacity() + usize::from(value.capacity() != 0) * ALLOCATOR_OVERHEAD;
                    assert!(
                        page_charge::<u64, Vec<u8>>(1, heap).unwrap() <= page_bytes,
                        "unchanged oversized neighbor must remain shared"
                    );
                    Ok(value.clone())
                })
                .unwrap();
            assert!(budget.stats().used - before <= envelope.additional_peak_bytes());
            if turn % 7 == 0 {
                drop(candidate);
                assert_eq!(budget.stats().used, before);
            } else {
                store.publish(candidate).unwrap();
                keys = expected;
            }
            assert_eq!(
                store
                    .entries()
                    .map(|entry| entry.key)
                    .collect::<std::collections::BTreeSet<_>>(),
                keys
            );
            assert_eq!(
                store.future_write_envelope(envelope.limits()).unwrap(),
                envelope
            );
        }
        drop(store);
        assert_eq!(budget.stats().used, 0);
    }
}
