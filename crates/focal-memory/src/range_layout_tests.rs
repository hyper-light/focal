use super::*;

/// Mutation coverage intentionally has no Clone implementation.
#[derive(Debug, PartialEq, Eq)]
struct Payload {
    label: u64,
    bytes: Vec<u8>,
}

fn entry(key: u64, bytes: usize) -> Entry<u64, Payload> {
    let value = Payload {
        label: key,
        bytes: vec![17; bytes],
    };
    let heap = value.bytes.capacity()
        + if value.bytes.capacity() == 0 {
            0
        } else {
            ALLOCATOR_OVERHEAD
        };
    Entry::new(key, value, heap)
}

fn copy(value: &Payload) -> Result<Payload, MemoryError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(value.bytes.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    bytes.extend_from_slice(&value.bytes);
    Ok(Payload {
        label: value.label,
        bytes,
    })
}

fn never_copy(_: &Payload) -> Result<Payload, MemoryError> {
    panic!("new or shared oversized value reached the retained-row copier")
}

fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 8,
        page_bytes: page_charge::<u64, Payload>(2, 2 * (8 + ALLOCATOR_OVERHEAD)).unwrap(),
        max_entry_bytes: size_of::<Entry<u64, Payload>>() + 4096 + ALLOCATOR_OVERHEAD,
        max_batch_entries: 32,
        ..RangeConfig::default()
    }
}

fn seeded(
    budget: &MemoryBudget,
    config: RangeConfig,
    rows: &[(u64, usize)],
) -> RangeStore<u64, Payload> {
    let mut range = RangeStore::new(RangeId(701), 0, config, budget.clone()).unwrap();
    let changes = rows
        .iter()
        .map(|&(key, bytes)| Change::Put(entry(key, bytes)))
        .collect();
    let candidate = range
        .prepare_batch_with(1, changes, BudgetLane::Ordinary, never_copy)
        .unwrap();
    range.publish(candidate).unwrap();
    range
}

fn page_keys(root: &Root<u64, Payload>) -> Vec<Vec<u64>> {
    root.pages
        .iter()
        .map(|page| page.entries.iter().map(|row| row.key).collect())
        .collect()
}

fn check_layout(root: &Root<u64, Payload>, config: RangeConfig) {
    let keys: Vec<_> = root.from(0, 0).map(|entry| entry.key).collect();
    assert_eq!(keys.len(), root.len);
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    for page in root.pages.iter() {
        assert!(!page.entries.is_empty());
        assert!(page.entries.len() <= config.page_entries);
        let heap = page.entries.iter().map(|row| row.heap_bytes).sum();
        let charged = page_charge::<u64, Payload>(page.entries.len(), heap).unwrap();
        assert_eq!(page._allocation.bytes(), charged);
        if charged > config.page_bytes {
            assert_eq!(page.entries.len(), 1);
        }
        for row in &page.entries {
            assert!(row.read_bytes().unwrap() <= config.max_entry_bytes);
        }
    }
}

#[test]
fn exact_full_page_byte_limit_includes_headers_and_splits_one_byte_below_boundary() {
    let exact = config().page_bytes;
    for (bound, pages) in [(exact, 1), (exact - 1, 2), (exact + 1, 1)] {
        let budget = MemoryBudget::new(1_000_000, 0).unwrap();
        let config = RangeConfig {
            page_bytes: bound,
            ..config()
        };
        let range = RangeStore::new(RangeId(701), 0, config, budget.clone()).unwrap();
        let before = budget.stats();
        let plan = range
            .plan_batch(
                1,
                vec![Change::Put(entry(10, 8)), Change::Put(entry(20, 8))],
                BudgetLane::Ordinary,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(plan.output_pages(), pages);
        assert_eq!(budget.stats(), before);
        let charge = plan.charges();
        let candidate = plan.build_with(never_copy).unwrap();
        assert_eq!(candidate.root.pages.len(), pages);
        preflight_tests::assert_retained(&before, &budget.stats(), charge);
        assert_eq!(
            candidate
                .root
                .pages
                .iter()
                .map(|page| page._allocation.bytes())
                .sum::<usize>(),
            charge.new_pages_bytes()
        );
        check_layout(&candidate.root, config);
        drop(candidate);
        assert_eq!(budget.stats(), before);
    }
}

#[test]
fn mixed_heap_sizes_partition_by_bytes_and_isolate_each_oversized_entry() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let range = seeded(
        &budget,
        config(),
        &[
            (10, 8),
            (20, 8),
            (30, 16),
            (40, 8),
            (50, 1024),
            (60, 8),
            (70, 8),
        ],
    );
    assert_eq!(
        page_keys(&range.root),
        vec![vec![10, 20], vec![30], vec![40], vec![50], vec![60, 70]]
    );
    assert!(range.root.pages.get(3).unwrap()._allocation.bytes() > config().page_bytes);
    check_layout(&range.root, config());
    let capped = RangeConfig {
        page_entries: 1,
        ..config()
    };
    let singles = seeded(&budget, capped, &[(10, 8), (20, 8), (30, 1024)]);
    assert_eq!(page_keys(&singles.root), vec![vec![10], vec![20], vec![30]]);
    check_layout(&singles.root, capped);
}

#[test]
fn maximum_entry_charge_is_independent_of_full_page_limit_and_accepts_exact_boundary() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let exact = entry(10, 1024).read_bytes().unwrap();
    let config = RangeConfig {
        max_entry_bytes: exact,
        ..config()
    };
    let range = seeded(&budget, config, &[(10, 1024)]);
    assert_eq!(range.root.pages.len(), 1);
    assert!(range.root.pages.get(0).unwrap()._allocation.bytes() > config.page_bytes);
    check_layout(&range.root, config);
    let before = budget.stats();
    for rows in [
        vec![Change::Put(entry(20, 1025))],
        vec![Change::Put(entry(10, 1025))],
    ] {
        assert!(matches!(
            range.plan_batch(2, rows, BudgetLane::Ordinary, usize::MAX),
            Err(MemoryError::ItemTooLarge { .. })
        ));
        assert_eq!(budget.stats(), before);
        assert_eq!(range.get(&10).unwrap().bytes.len(), 1024);
        assert_eq!(range.prefix(), 1);
    }
}

#[test]
fn inserts_on_both_sides_of_oversized_singleton_share_its_page_and_payload_without_copying() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let mut range = seeded(&budget, config(), &[(20, 1024)]);
    let old = Arc::clone(range.root.pages.get(0).unwrap());
    let pointer = range.get(&20).unwrap().bytes.as_ptr();
    let incoming = entry(10, 8);
    let moved = incoming.value.bytes.as_ptr();
    let before = budget.stats();
    let plan = range
        .plan_batch(
            2,
            vec![
                Change::Put(entry(40, 8)),
                Change::Put(incoming),
                Change::Put(entry(30, 8)),
            ],
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap();
    assert_eq!(plan.output_pages(), 3);
    let charges = plan.charges();
    assert_eq!(
        charges.new_pages_bytes(),
        page_charge::<u64, Payload>(1, 8 + ALLOCATOR_OVERHEAD).unwrap()
            + page_charge::<u64, Payload>(2, 2 * (8 + ALLOCATOR_OVERHEAD)).unwrap()
    );
    let candidate = plan
        .build_with(|_| Err(MemoryError::AllocationFailed))
        .unwrap();
    assert_eq!(
        page_keys(&candidate.root),
        vec![vec![10], vec![20], vec![30, 40]]
    );
    assert!(Arc::ptr_eq(candidate.root.pages.get(1).unwrap(), &old));
    assert_eq!(candidate.get(&20).unwrap().bytes.as_ptr(), pointer);
    assert_eq!(candidate.get(&10).unwrap().bytes.as_ptr(), moved);
    preflight_tests::assert_retained(&before, &budget.stats(), charges);
    check_layout(&candidate.root, config());
    range.publish(candidate).unwrap();
    assert!(Arc::ptr_eq(range.root.pages.get(1).unwrap(), &old));
    assert_eq!(range.get(&20).unwrap().bytes.as_ptr(), pointer);
}

#[test]
fn same_key_replacement_and_deletion_remove_oversized_page_without_copying_its_value() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let mut range = seeded(&budget, config(), &[(20, 1024)]);
    let original = Arc::clone(range.root.pages.get(0).unwrap());
    let replacement = entry(20, 2048);
    let replacement_pointer = replacement.value.bytes.as_ptr();
    let updated = range
        .prepare_batch_with(
            2,
            vec![Change::Put(replacement)],
            BudgetLane::Ordinary,
            never_copy,
        )
        .unwrap();
    assert!(!Arc::ptr_eq(updated.root.pages.get(0).unwrap(), &original));
    assert_eq!(
        updated.get(&20).unwrap().bytes.as_ptr(),
        replacement_pointer
    );
    assert_eq!(original.entries[0].value.bytes.len(), 1024);
    check_layout(&updated.root, config());
    range.publish(updated).unwrap();
    let plan = range
        .plan_batch(
            3,
            vec![Change::Delete(20)],
            BudgetLane::Completion,
            usize::MAX,
        )
        .unwrap();
    assert_eq!(plan.output_pages(), 0);
    assert_eq!(plan.charges().new_pages_bytes(), 0);
    let empty = plan.build_with(never_copy).unwrap();
    assert!(empty.root.pages.is_empty());
    assert!(empty.get(&20).is_none());
    range.publish(empty).unwrap();
    assert_eq!(range.len(), 0);
    assert_eq!(range.prefix(), 3);
}

#[test]
fn oversized_to_small_replacement_repartitions_with_adjacent_incoming_rows() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let mut range = seeded(&budget, config(), &[(20, 1024)]);
    let candidate = range
        .prepare_batch_with(
            2,
            vec![Change::Put(entry(10, 8)), Change::Put(entry(20, 8))],
            BudgetLane::Ordinary,
            never_copy,
        )
        .unwrap();
    assert_eq!(page_keys(&candidate.root), vec![vec![10, 20]]);
    check_layout(&candidate.root, config());
    range.publish(candidate).unwrap();
    let mut copied = Vec::new();
    let candidate = range
        .prepare_batch_with(
            3,
            vec![Change::Put(entry(20, 1024)), Change::Put(entry(30, 8))],
            BudgetLane::Ordinary,
            |old| {
                copied.push(old.label);
                copy(old)
            },
        )
        .unwrap();
    assert_eq!(copied, [10]);
    assert_eq!(
        page_keys(&candidate.root),
        vec![vec![10], vec![20], vec![30]]
    );
    check_layout(&candidate.root, config());
}

#[test]
fn adjacent_oversized_singletons_are_shared_across_a_prepared_chain_and_empty_prefix() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let mut range = seeded(&budget, config(), &[(20, 1024), (40, 2048)]);
    let first = Arc::clone(range.root.pages.get(0).unwrap());
    let second = Arc::clone(range.root.pages.get(1).unwrap());
    let before = budget.stats();
    let initial = range
        .prepare_batch_with(
            2,
            vec![Change::Put(entry(30, 8))],
            BudgetLane::Ordinary,
            never_copy,
        )
        .unwrap();
    let suffix = range
        .prepare_after_with(
            &initial,
            3,
            vec![Change::Put(entry(50, 8))],
            BudgetLane::Ordinary,
            never_copy,
        )
        .unwrap();
    for candidate in [&initial, &suffix] {
        assert!(
            candidate
                .root
                .pages
                .iter()
                .any(|page| Arc::ptr_eq(page, &first))
        );
        assert!(
            candidate
                .root
                .pages
                .iter()
                .any(|page| Arc::ptr_eq(page, &second))
        );
        check_layout(&candidate.root, config());
    }
    assert_eq!(
        page_keys(&suffix.root),
        vec![vec![20], vec![30], vec![40], vec![50]]
    );
    let suffix = range.publish_recoverable(suffix).unwrap_err().1;
    range.publish(initial).unwrap();
    range.publish(suffix).unwrap();
    let empty = range
        .plan_batch(4, vec![], BudgetLane::Completion, usize::MAX)
        .unwrap();
    assert_eq!(empty.charges().new_pages_bytes(), 0);
    let empty = empty.build_with(never_copy).unwrap();
    assert!(Arc::ptr_eq(empty.root.pages.get(0).unwrap(), &first));
    assert!(Arc::ptr_eq(empty.root.pages.get(2).unwrap(), &second));
    assert!(budget.stats().used > before.used);
}

#[test]
fn funded_exact_quote_survives_full_parent_pressure_byte_splitting_pins_and_copy_failure() {
    let budget = MemoryBudget::new(4_000_000, 500_000).unwrap();
    let mut range = seeded(&budget, config(), &[(10, 8), (20, 8), (40, 1024)]);
    let lease = range.pin(0, 100).unwrap();
    let large = Arc::clone(range.root.pages.get(1).unwrap());
    let changes = || vec![Change::Put(entry(15, 64)), Change::Put(entry(50, 8))];
    let plan = range
        .plan_batch(2, changes(), BudgetLane::Completion, usize::MAX)
        .unwrap();
    let charges = plan.charges();
    assert_eq!(plan.output_pages(), 5);
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
    let mut visited = Vec::new();
    let refused = plan.build_in_with(&pool, |old| {
        visited.push(old.label);
        if old.label == 20 {
            return Err(MemoryError::AllocationFailed);
        }
        copy(old)
    });
    assert!(matches!(refused, Err(MemoryError::AllocationFailed)));
    assert_eq!(visited, [10, 20]);
    assert_eq!(pool.stats().used, 0);
    assert_eq!(budget.stats(), parent);
    assert_eq!(range.prefix(), 1);
    let plan = range
        .plan_batch(
            2,
            changes(),
            BudgetLane::Completion,
            charges.additional_peak_bytes(),
        )
        .unwrap();
    assert_eq!(plan.charges(), charges);
    let mut visited = Vec::new();
    let candidate = plan
        .build_in_with(&pool, |old| {
            assert_eq!(budget.stats().used, parent.used);
            assert_eq!(budget.stats().ordinary_used, parent.ordinary_used);
            assert!(pool.stats().used <= charges.additional_peak_bytes());
            visited.push(old.label);
            copy(old)
        })
        .unwrap();
    assert_eq!(visited, [10, 20]);
    preflight_tests::assert_retained(&unspent, &pool.stats(), charges);
    assert!(
        candidate
            .root
            .pages
            .iter()
            .any(|page| Arc::ptr_eq(page, &large))
    );
    check_layout(&candidate.root, config());
    range.publish(candidate).unwrap();
    assert_eq!(range.get(&15).unwrap().bytes.len(), 64);
    assert_eq!(
        lease
            .project_next(&15, false, &u64::MAX, 0, |row| row.key)
            .unwrap(),
        Some(20)
    );
    assert_eq!(budget.stats().used, parent.used);
    drop(pressure);
    range.release(&lease).unwrap();
    drop(lease);
    drop(large);
    drop(range);
    assert_eq!(pool.stats().used, 0);
    drop(pool);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn over_entry_limit_and_overflow_refuse_during_preflight_without_copying_or_charging() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let range = seeded(&budget, config(), &[(10, 8)]);
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    let before = budget.stats();
    let _fault = preflight_tests::fault(1, preflight_tests::AllocationFault::Fail);
    let too_large = Entry::new(
        20,
        Payload {
            label: 20,
            bytes: Vec::new(),
        },
        config().max_entry_bytes,
    );
    assert!(matches!(
        range.plan_batch(
            2,
            vec![Change::Put(too_large)],
            BudgetLane::Ordinary,
            usize::MAX
        ),
        Err(MemoryError::ItemTooLarge { .. })
    ));
    let overflow = Entry::new(
        20,
        Payload {
            label: 20,
            bytes: Vec::new(),
        },
        usize::MAX,
    );
    assert!(matches!(
        range.plan_batch(
            2,
            vec![Change::Put(overflow)],
            BudgetLane::Ordinary,
            usize::MAX
        ),
        Err(MemoryError::CounterExhausted(_))
    ));
    assert_eq!(budget.stats(), before);
    assert_eq!(range.prefix(), 1);
    assert_eq!(range.len(), 1);
    drop(pressure);
    // Both refusals occurred without calling the internal vector allocator.
    // Its first armed failure therefore remains pending for the valid build.
    let plan = range
        .plan_batch(
            2,
            vec![Change::Put(entry(20, 8))],
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap();
    assert!(matches!(
        plan.build_with(never_copy),
        Err(MemoryError::AllocationFailed)
    ));
    assert_eq!(range.prefix(), 1);
}

#[test]
fn impossible_page_and_entry_minima_refuse_before_range_allocation_but_exact_minima_work() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let base = budget.stats();
    let minimum_page = page_charge::<u64, Payload>(1, 0).unwrap();
    let minimum_entry = size_of::<Entry<u64, Payload>>();
    for invalid in [
        RangeConfig {
            page_bytes: 0,
            ..config()
        },
        RangeConfig {
            page_bytes: minimum_page - 1,
            ..config()
        },
        RangeConfig {
            max_entry_bytes: 0,
            ..config()
        },
        RangeConfig {
            max_entry_bytes: minimum_entry - 1,
            ..config()
        },
    ] {
        assert!(matches!(
            RangeStore::<u64, Payload>::new(RangeId(705), 0, invalid, budget.clone()),
            Err(MemoryError::InvalidConfiguration(_))
        ));
        assert_eq!(budget.stats(), base);
    }
    let exact = RangeConfig {
        page_bytes: minimum_page,
        max_entry_bytes: minimum_entry,
        ..config()
    };
    let range = seeded(&budget, exact, &[(10, 0), (20, 0)]);
    assert_eq!(page_keys(&range.root), vec![vec![10], vec![20]]);
    assert!(
        range
            .root
            .pages
            .iter()
            .all(|page| page._allocation.bytes() == minimum_page)
    );
    check_layout(&range.root, exact);
    drop(range);
    assert_eq!(budget.stats(), base);
}

#[test]
fn default_unbounded_byte_limits_preserve_original_entry_count_partitioning() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let legacy = RangeConfig {
        page_entries: 4,
        ..RangeConfig::default()
    };
    assert_eq!(legacy.page_bytes, usize::MAX);
    assert_eq!(legacy.max_entry_bytes, usize::MAX);
    let range = seeded(
        &budget,
        legacy,
        &[(10, 8), (20, 1024), (30, 16), (40, 2048), (50, 8)],
    );
    assert_eq!(page_keys(&range.root), vec![vec![10, 20, 30, 40], vec![50]]);
    check_layout(&range.root, legacy);
}

#[test]
fn checkpoint_import_enforces_byte_layout_ordering_and_complete_entry_bounds() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let small_heap = 8 + ALLOCATOR_OVERHEAD;
    let config = RangeConfig {
        page_entries: 3,
        page_bytes: page_charge::<u64, Vec<u8>>(2, 2 * small_heap).unwrap(),
        max_entry_bytes: size_of::<Entry<u64, Vec<u8>>>() + 1024 + ALLOCATOR_OVERHEAD,
        max_batch_entries: 3,
        ..RangeConfig::default()
    };
    let imported =
        |key: u64, bytes: usize| Entry::new(key, vec![19; bytes], bytes + ALLOCATOR_OVERHEAD);
    let range = RangeStore::from_entries(
        RangeId(703),
        90,
        config,
        budget.clone(),
        [
            imported(10, 8),
            imported(20, 8),
            imported(30, 1024),
            imported(40, 8),
            imported(50, 8),
        ],
    )
    .unwrap();
    assert_eq!(range.prefix(), 90);
    assert_eq!(range.len(), 5);
    for page in range.root.pages.iter() {
        let charged = page._allocation.bytes();
        if charged > config.page_bytes {
            assert_eq!(page.entries.len(), 1);
        }
        for row in &page.entries {
            assert!(row.read_bytes().unwrap() <= config.max_entry_bytes);
        }
    }
    assert_eq!(
        range
            .root
            .pages
            .iter()
            .map(|page| page.entries.iter().map(|row| row.key).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec![10, 20], vec![30], vec![40, 50]]
    );
    let before = budget.stats();
    for input in [
        vec![
            imported(10, 8),
            imported(20, 8),
            imported(30, 8),
            imported(20, 8),
        ],
        vec![
            imported(10, 8),
            imported(20, 8),
            imported(30, 8),
            imported(30, 8),
        ],
        vec![
            imported(10, 8),
            imported(20, 8),
            imported(30, 8),
            imported(40, 1025),
        ],
    ] {
        assert!(RangeStore::from_entries(RangeId(704), 91, config, budget.clone(), input).is_err());
        assert_eq!(budget.stats(), before);
        assert_eq!(range.prefix(), 90);
    }
    drop(range);
    assert_eq!(budget.stats().used, 0);
}
