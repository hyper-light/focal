use super::*;
use std::cell::Cell;

#[derive(Clone, Copy)]
struct Plan {
    bytes: usize,
}

#[derive(Default)]
struct Probe {
    builds: Cell<usize>,
    copies: Cell<usize>,
    created: Cell<usize>,
    dropped: Cell<usize>,
    pending_at_drop: Cell<usize>,
}

// Deliberately has no Clone implementation. Its owned allocation must either
// move into the detached store or pass through the supplied fallible copier.
struct Row<'a> {
    bytes: Vec<u8>,
    budget: &'a MemoryBudget,
    probe: &'a Probe,
}

impl<'a> Row<'a> {
    fn build(
        budget: &'a MemoryBudget,
        probe: &'a Probe,
        plan: Plan,
        allowance: usize,
    ) -> Result<(Self, usize), MemoryError> {
        assert!(budget.stats().by_kind[BudgetKind::Pending as usize] >= allowance);
        probe.builds.set(probe.builds.get() + 1);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(plan.bytes)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let actual = bytes.capacity() + ALLOCATOR_OVERHEAD;
        if actual > allowance {
            return Err(MemoryError::Capacity {
                requested: actual,
                available: allowance,
            });
        }
        bytes.resize(plan.bytes, 37);
        probe.created.set(probe.created.get() + 1);
        Ok((
            Self {
                bytes,
                budget,
                probe,
            },
            actual,
        ))
    }

    fn copy(&self, fail_at: Option<usize>) -> Result<Self, MemoryError> {
        let attempt = self.probe.copies.get() + 1;
        self.probe.copies.set(attempt);
        if fail_at == Some(attempt) {
            return Err(MemoryError::MissingKey);
        }
        let heap = self.bytes.capacity() + ALLOCATOR_OVERHEAD;
        assert!(self.budget.stats().by_kind[BudgetKind::Pages as usize] >= heap);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.bytes.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        let actual = bytes.capacity() + ALLOCATOR_OVERHEAD;
        if actual > heap {
            return Err(MemoryError::Capacity {
                requested: actual,
                available: heap,
            });
        }
        bytes.extend_from_slice(&self.bytes);
        self.probe.created.set(self.probe.created.get() + 1);
        Ok(Self {
            bytes,
            budget: self.budget,
            probe: self.probe,
        })
    }
}

impl Drop for Row<'_> {
    fn drop(&mut self) {
        self.probe.dropped.set(self.probe.dropped.get() + 1);
        let stats = self.budget.stats();
        self.probe
            .pending_at_drop
            .set(stats.by_kind[BudgetKind::Pending as usize]);
        assert!(
            stats.by_kind[BudgetKind::Pending as usize] + stats.by_kind[BudgetKind::Pages as usize]
                >= self.bytes.capacity() + ALLOCATOR_OVERHEAD,
            "the payload outlived its import/page allowance"
        );
    }
}

fn partition(key: &u64) -> u64 {
    key / 100
}

fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 4,
        page_bytes: 512,
        max_entry_bytes: size_of::<Entry<u64, Row<'_>>>() + 4096 + ALLOCATOR_OVERHEAD,
        max_batch_entries: 4,
        ..RangeConfig::default()
    }
}

fn plan(key: u64, bytes: usize) -> Entry<u64, Plan> {
    Entry::new(key, Plan { bytes }, bytes + ALLOCATOR_OVERHEAD)
}

fn load<'a>(
    budget: &'a MemoryBudget,
    probe: &'a Probe,
    config: RangeConfig,
    input: impl IntoIterator<Item = Result<Entry<u64, Plan>, MemoryError>>,
    fail_copy: Option<usize>,
) -> Result<RangeStore<u64, Row<'a>>, MemoryError> {
    RangeStore::from_entry_plans_partitioned_with(
        RangeId(1201),
        81,
        config,
        budget.clone(),
        input,
        partition,
        |_, plan, allowance| Row::build(budget, probe, plan, allowance),
        |row: &Row<'_>| row.copy(fail_copy),
    )
}

#[test]
fn non_clone_rows_move_into_partitioned_and_oversized_leaves_at_the_verified_prefix() {
    let budget = MemoryBudget::new(4_000_000, 500_000).unwrap();
    let before = budget.stats();
    let probe = Probe::default();
    let pointers = Cell::new([std::ptr::null(); 5]);
    let input = [(1, 16), (2, 16), (101, 2048), (102, 16), (201, 16)];
    let store = RangeStore::from_entry_plans_partitioned_with(
        RangeId(1202),
        u64::MAX,
        config(),
        budget.clone(),
        input.map(|(key, bytes)| Ok(plan(key, bytes))),
        partition,
        |_, plan, allowance| {
            let (row, actual) = Row::build(&budget, &probe, plan, allowance)?;
            let mut recorded = pointers.get();
            recorded[probe.builds.get() - 1] = row.bytes.as_ptr();
            pointers.set(recorded);
            Ok((row, actual))
        },
        |row: &Row<'_>| row.copy(None),
    )
    .unwrap();
    assert_eq!(store.prefix(), u64::MAX);
    assert_eq!(store.len(), input.len());
    assert_eq!(store.stats().pages, 4);
    assert_eq!(probe.copies.get(), 0);
    for (index, entry) in store.entries().enumerate() {
        assert_eq!((entry.key, entry.value.bytes.len()), input[index]);
        assert_eq!(entry.value.bytes.as_ptr(), pointers.get()[index]);
        assert_eq!(
            entry.heap_bytes,
            entry.value.bytes.capacity() + ALLOCATOR_OVERHEAD
        );
    }
    for page in store.root.pages.iter() {
        assert!(
            page.entries
                .iter()
                .all(|entry| { partition(&entry.key) == partition(&page.entries[0].key) })
        );
        let heap = page.entries.iter().map(|entry| entry.heap_bytes).sum();
        let bytes = page_charge::<u64, Row<'_>>(page.entries.len(), heap).unwrap();
        assert!(bytes <= config().page_bytes || page.entries.len() == 1);
    }
    assert_eq!(budget.stats().by_kind[BudgetKind::Pending as usize], 0);
    drop(store);
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
}

#[test]
fn row_quote_is_reserved_before_build_and_actual_charge_releases_unused_allowance() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let probe = Probe::default();
    let config = RangeConfig {
        page_bytes: 64 * 1024,
        ..config()
    };
    let staging = 2 * ALLOCATOR_OVERHEAD
        + config.page_entries.min(config.max_batch_entries)
            * (size_of::<Change<u64, Row<'_>>>() + size_of::<Allocation>());
    let retained = Cell::new(0);
    let store = RangeStore::from_entry_plans_partitioned_with(
        RangeId(1203),
        82,
        config,
        budget.clone(),
        [1, 2].map(|key| {
            let mut entry = plan(key, 1024);
            entry.heap_bytes += 512;
            Ok(entry)
        }),
        partition,
        |_, plan, allowance| {
            assert_eq!(
                budget.stats().by_kind[BudgetKind::Pending as usize],
                staging + retained.get() + allowance
            );
            let result = Row::build(&budget, &probe, plan, allowance)?;
            retained.set(retained.get() + result.1);
            Ok(result)
        },
        |row: &Row<'_>| row.copy(None),
    )
    .unwrap();
    assert_eq!(
        store.entries().map(|entry| entry.heap_bytes).sum::<usize>(),
        retained.get()
    );
    assert_eq!(budget.stats().by_kind[BudgetKind::Pending as usize], 0);
    drop(store);
    assert_eq!(budget.stats().used, 0);

    // No second builder runs when its exact payload reservation is one byte
    // short, even though the first row has already allocated its entire body.
    let probe = Probe::default();
    let bytes = 2048;
    let heap = bytes + ALLOCATOR_OVERHEAD;
    let available = root_charge::<u64, Row<'_>>(0).unwrap() + staging + 2 * heap - 1;
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - available,
        )
        .unwrap();
    let before = budget.stats();
    let result = load(
        &budget,
        &probe,
        config,
        [Ok(plan(1, bytes)), Ok(plan(2, bytes))],
        None,
    );
    assert!(
        matches!(result, Err(MemoryError::Capacity { requested, available })
        if requested == heap && available == heap - 1)
    );
    assert_eq!(probe.builds.get(), 1);
    assert_eq!(probe.pending_at_drop.get(), staging + heap);
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
    drop(pressure);
}

#[test]
fn source_and_builder_errors_refund_empty_staged_and_previously_installed_chunks() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let before = budget.stats();
    let probe = Probe::default();
    let empty = load(&budget, &probe, config(), [], None).unwrap();
    assert_eq!(empty.prefix(), 81);
    assert!(empty.is_empty());
    assert_eq!(probe.builds.get(), 0);
    drop(empty);
    assert_eq!(budget.stats(), before);
    for input in [
        vec![Err(MemoryError::MissingKey)],
        vec![Ok(plan(1, 32)), Err(MemoryError::MissingKey)],
        vec![
            Ok(plan(1, 32)),
            Ok(plan(101, 32)),
            Err(MemoryError::MissingKey),
        ],
    ] {
        let probe = Probe::default();
        let expected_builds = input.len() - 1;
        let result = load(&budget, &probe, config(), input, None);
        assert!(matches!(result, Err(MemoryError::MissingKey)));
        assert_eq!(probe.builds.get(), expected_builds);
        assert_eq!(probe.created.get(), probe.dropped.get());
        assert_eq!(budget.stats(), before);
    }
    let probe = Probe::default();
    let result = RangeStore::from_entry_plans_partitioned_with(
        RangeId(1204),
        83,
        config(),
        budget.clone(),
        [Ok(plan(1, 32)), Ok(plan(101, 32))],
        partition,
        |key, plan, allowance| {
            if *key == 101 {
                assert!(budget.stats().by_kind[BudgetKind::Pages as usize] > 0);
                return Err(MemoryError::AllocationFailed);
            }
            Row::build(&budget, &probe, plan, allowance)
        },
        |row: &Row<'_>| row.copy(None),
    );
    assert!(matches!(result, Err(MemoryError::AllocationFailed)));
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
}

#[test]
fn invalid_order_entry_quotes_and_excess_actual_heap_refuse_without_a_partial_store() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let before = budget.stats();
    for keys in [[1, 1], [2, 1], [101, 1]] {
        let probe = Probe::default();
        let result = load(
            &budget,
            &probe,
            config(),
            keys.map(|key| Ok(plan(key, 32))),
            None,
        );
        assert!(matches!(
            result,
            Err(MemoryError::InvalidConfiguration(
                "checkpoint entries must be strictly ordered"
            ))
        ));
        assert_eq!(probe.builds.get(), 1);
        assert_eq!(probe.created.get(), probe.dropped.get());
        assert_eq!(budget.stats(), before);
    }
    let probe = Probe::default();
    let result = load(&budget, &probe, config(), [Ok(plan(1, 4097))], None);
    assert!(matches!(result, Err(MemoryError::ItemTooLarge { .. })));
    assert_eq!(probe.builds.get(), 0);
    assert_eq!(budget.stats(), before);
    let result = load(
        &budget,
        &probe,
        config(),
        [Ok(Entry::new(1, Plan { bytes: 1 }, usize::MAX))],
        None,
    );
    assert!(matches!(result, Err(MemoryError::CounterExhausted(_))));
    assert_eq!(probe.builds.get(), 0);
    assert_eq!(budget.stats(), before);

    // A callback cannot retain a value whose reported capacity exceeds the
    // allowance, even if its construction plan originally quoted less.
    let result = RangeStore::from_entry_plans_partitioned_with(
        RangeId(1205),
        84,
        config(),
        budget.clone(),
        [Ok(plan(1, 32))],
        partition,
        |_, plan, allowance| {
            let (row, _) = Row::build(&budget, &probe, plan, allowance)?;
            Ok((row, allowance + 1))
        },
        |row: &Row<'_>| row.copy(None),
    );
    assert!(
        matches!(result, Err(MemoryError::Capacity { requested, available })
        if requested == available + 1)
    );
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
}

#[test]
fn fallible_copy_and_each_engine_allocation_refuse_with_complete_refunds_and_retry() {
    use crate::range::preflight_tests::{
        AllocationFault, count_allocations, fault, fault_consumed,
    };
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    let config = RangeConfig {
        page_entries: 4,
        max_batch_entries: 1,
        page_bytes: 4096,
        ..config()
    };
    let inputs = || [1, 2, 3, 101].map(|key| Ok(plan(key, 64)));
    let probe = Probe::default();
    let (reference, sites) =
        count_allocations(|| load(&budget, &probe, config, inputs(), None).unwrap());
    assert!(probe.copies.get() >= 3);
    let expected = reference
        .entries()
        .map(|entry| (entry.key, entry.heap_bytes))
        .collect::<Vec<_>>();
    drop(reference);
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
    let probe = Probe::default();
    let failed = load(&budget, &probe, config, inputs(), Some(2));
    assert!(matches!(failed, Err(MemoryError::MissingKey)));
    assert_eq!(probe.copies.get(), 2);
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
    for kind in [AllocationFault::Fail, AllocationFault::Excess] {
        for at in 1..=sites {
            let probe = Probe::default();
            {
                let _fault = fault(at, kind);
                let failed = load(&budget, &probe, config, inputs(), None);
                match kind {
                    AllocationFault::Fail => {
                        assert!(matches!(failed, Err(MemoryError::AllocationFailed)))
                    }
                    AllocationFault::Excess => assert!(matches!(failed,
                        Err(MemoryError::Capacity { requested, available }) if requested > available)),
                }
                assert!(fault_consumed());
            }
            assert_eq!(probe.created.get(), probe.dropped.get(), "allocation {at}");
            assert_eq!(budget.stats(), before, "allocation {at}");
        }
    }
    let probe = Probe::default();
    let retry = load(&budget, &probe, config, inputs(), None).unwrap();
    assert_eq!(
        retry
            .entries()
            .map(|entry| (entry.key, entry.heap_bytes))
            .collect::<Vec<_>>(),
        expected
    );
    drop(retry);
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
}
