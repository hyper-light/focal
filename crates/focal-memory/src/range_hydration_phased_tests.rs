use super::*;
use std::cell::Cell;

#[derive(Default)]
struct Probe {
    built: Cell<usize>,
    copied: Cell<usize>,
    created: Cell<usize>,
    dropped: Cell<usize>,
    max_pending: Cell<usize>,
}

// No Clone: hydration must move incoming ownership and fallibly copy only
// retained neighbors in touched pages under the engine's existing precharges.
struct Row<'a> {
    bytes: Vec<u8>,
    budget: &'a MemoryBudget,
    probe: &'a Probe,
}

impl<'a> Row<'a> {
    fn build(
        budget: &'a MemoryBudget,
        probe: &'a Probe,
        byte: u8,
        allowance: usize,
    ) -> Result<(Self, usize), MemoryError> {
        let pending = budget.stats().by_kind[BudgetKind::Pending as usize];
        assert!(pending >= allowance);
        probe.max_pending.set(probe.max_pending.get().max(pending));
        probe.built.set(probe.built.get() + 1);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(32)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let actual = bytes.capacity() + ALLOCATOR_OVERHEAD;
        if actual > allowance {
            return Err(MemoryError::Capacity {
                requested: actual,
                available: allowance,
            });
        }
        bytes.resize(32, byte);
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

    fn copy(&self, fail: bool) -> Result<Self, MemoryError> {
        self.probe.copied.set(self.probe.copied.get() + 1);
        if fail {
            return Err(MemoryError::MissingKey);
        }
        let quoted = self.bytes.capacity() + ALLOCATOR_OVERHEAD;
        assert!(self.budget.stats().by_kind[BudgetKind::Pages as usize] >= quoted);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.bytes.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        let actual = bytes.capacity() + ALLOCATOR_OVERHEAD;
        if actual > quoted {
            return Err(MemoryError::Capacity {
                requested: actual,
                available: quoted,
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
        assert!(
            stats.by_kind[BudgetKind::Pending as usize] + stats.by_kind[BudgetKind::Pages as usize]
                >= self.bytes.capacity() + ALLOCATOR_OVERHEAD
        );
    }
}

fn partition(key: &u64) -> u64 {
    key / 100
}

fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 4,
        page_bytes: 4096,
        max_batch_entries: 2,
        max_entry_bytes: 4096,
        ..RangeConfig::default()
    }
}

fn plan(key: u64, byte: u8) -> Result<Entry<u64, u8>, MemoryError> {
    Ok(Entry::new(key, byte, 32 + ALLOCATOR_OVERHEAD))
}

fn begin<'a>(budget: &'a MemoryBudget, rows: usize, phases: usize) -> RangeHydration<u64, Row<'a>> {
    RangeStore::begin_hydration_partitioned(
        RangeId(1311),
        config(),
        budget.clone(),
        partition,
        RangeHydrationLimits {
            expected_entries: rows,
            max_phases: phases,
        },
    )
    .unwrap()
}

fn plain_phase<'a>(
    owner: RangeHydration<u64, Row<'a>>,
    budget: &'a MemoryBudget,
    probe: &'a Probe,
    count: usize,
    input: impl IntoIterator<Item = Result<Entry<u64, u8>, MemoryError>>,
    fail_copy: bool,
) -> Result<RangeHydration<u64, Row<'a>>, MemoryError> {
    owner.insert_phase(
        count,
        input,
        |_, _, byte, allowance| Row::build(budget, probe, byte, allowance),
        |row| row.copy(fail_copy),
    )
}

#[test]
fn dependencies_cross_canonical_key_order_and_staging_boundaries_without_extra_roots() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    let probe = Probe::default();
    let owner = begin(&budget, 8, 2)
        .insert_phase(
            4,
            (100..104).map(|key| plan(key, 7)),
            |dependencies, key, byte, allowance| {
                assert!(dependencies.get(key).is_none());
                if *key > 100 {
                    assert_eq!(dependencies.get(&(key - 1)).unwrap().bytes[0], byte);
                }
                Row::build(&budget, &probe, byte, allowance)
            },
            |row| row.copy(false),
        )
        .unwrap();
    let owner = owner
        .insert_phase(
            4,
            (0..4).map(|key| plan(key, 9)),
            |dependencies, key, byte, allowance| {
                assert_eq!(dependencies.get(&(100 + key)).unwrap().bytes[0], 7);
                assert!(dependencies.get(key).is_none());
                if *key > 0 {
                    assert_eq!(dependencies.get(&(key - 1)).unwrap().bytes[0], byte);
                }
                Row::build(&budget, &probe, byte, allowance)
            },
            |row| row.copy(false),
        )
        .unwrap();
    let validated = Cell::new(false);
    let store = owner
        .finish(u64::MAX, |view| {
            assert_eq!(view.len(), 8);
            assert!(!view.is_empty());
            assert_eq!(
                view.entries().map(|entry| entry.key).collect::<Vec<_>>(),
                [0, 1, 2, 3, 100, 101, 102, 103]
            );
            assert_eq!(
                view.entries_from(&101, true)
                    .map(|entry| entry.key)
                    .collect::<Vec<_>>(),
                [102, 103]
            );
            assert_eq!(
                view.get_entry(&2).unwrap().heap_bytes,
                32 + ALLOCATOR_OVERHEAD
            );
            assert_eq!(view.get(&100).unwrap().bytes[0], 7);
            assert!(view.get(&999).is_none());
            validated.set(true);
            Ok(())
        })
        .unwrap();
    assert!(validated.get());
    assert_eq!(store.prefix(), u64::MAX);
    assert_eq!(store.stats().pinned_snapshots, 0);
    assert_eq!(budget.stats().by_kind[BudgetKind::Pending as usize], 0);
    // Only two rows plus their permits and payloads may coexist in staging,
    // even though dependencies include all previously constructed phases.
    let staging =
        2 * ALLOCATOR_OVERHEAD + 2 * (size_of::<Change<u64, Row<'_>>>() + size_of::<Allocation>());
    assert_eq!(
        probe.max_pending.get(),
        staging + 2 * (32 + ALLOCATOR_OVERHEAD)
    );
    drop(store);
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
}

#[test]
fn interleaved_phase_keys_preserve_existing_owned_values_with_fallible_copies() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    let probe = Probe::default();
    let owner = plain_phase(
        begin(&budget, 6, 2),
        &budget,
        &probe,
        3,
        [2, 4, 6].map(|key| plan(key, 20)),
        false,
    )
    .unwrap();
    let owner = owner
        .insert_phase(
            3,
            [1, 3, 5].map(|key| plan(key, 10)),
            |dependencies, key, byte, allowance| {
                assert_eq!(dependencies.get(&(key + 1)).unwrap().bytes[0], 20);
                Row::build(&budget, &probe, byte, allowance)
            },
            |row| row.copy(false),
        )
        .unwrap();
    let store = owner
        .finish(700, |view| {
            assert_eq!(
                view.entries()
                    .map(|entry| (entry.key, entry.value.bytes[0]))
                    .collect::<Vec<_>>(),
                [(1, 10), (2, 20), (3, 10), (4, 20), (5, 10), (6, 20)]
            );
            Ok(())
        })
        .unwrap();
    assert!(probe.copied.get() > 0);
    drop(store);
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
}

#[test]
fn duplicate_and_reordered_phase_keys_drop_all_previously_restored_rows() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    for keys in [[1, 1, 3], [1, 3, 2], [2, 3, 1], [100, 101, 102]] {
        let probe = Probe::default();
        let owner = plain_phase(
            begin(&budget, 5, 2),
            &budget,
            &probe,
            2,
            [100, 102].map(|key| plan(key, 20)),
            false,
        )
        .unwrap();
        let failed = plain_phase(
            owner,
            &budget,
            &probe,
            3,
            keys.map(|key| plan(key, 10)),
            false,
        );
        assert!(matches!(failed, Err(MemoryError::InvalidConfiguration(_))));
        assert_eq!(probe.created.get(), probe.dropped.get());
        assert_eq!(budget.stats(), before);
    }
}

#[test]
fn exact_source_and_total_counts_phase_limits_and_empty_imports_are_enforced() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    let probe = Probe::default();
    let invalid = RangeStore::<u64, Row<'_>>::begin_hydration_partitioned(
        RangeId(1312),
        config(),
        budget.clone(),
        partition,
        RangeHydrationLimits {
            expected_entries: 0,
            max_phases: 0,
        },
    );
    assert!(matches!(invalid, Err(MemoryError::InvalidConfiguration(_))));
    assert_eq!(budget.stats(), before);
    let empty = begin(&budget, 0, 1)
        .finish(0, |view| {
            assert!(view.is_empty());
            assert_eq!(view.entries().count(), 0);
            Ok(())
        })
        .unwrap();
    drop(empty);
    assert_eq!(budget.stats(), before);
    for (expected, keys) in [(0, vec![1]), (2, vec![1]), (2, vec![1, 2, 3]), (1, vec![])] {
        let result = plain_phase(
            begin(&budget, 3, 1),
            &budget,
            &probe,
            expected,
            keys.into_iter().map(|key| plan(key, 10)),
            false,
        );
        assert!(matches!(result, Err(MemoryError::InvalidConfiguration(_))));
        assert_eq!(probe.created.get(), probe.dropped.get());
        assert_eq!(budget.stats(), before);
    }
    let result = plain_phase(
        begin(&budget, 1, 1),
        &budget,
        &probe,
        2,
        [plan(1, 10), plan(2, 20)],
        false,
    );
    assert!(matches!(
        result,
        Err(MemoryError::Capacity {
            requested: 2,
            available: 1
        })
    ));
    assert_eq!(budget.stats(), before);
    let owner = plain_phase(
        begin(&budget, 2, 1),
        &budget,
        &probe,
        1,
        [plan(1, 10)],
        false,
    )
    .unwrap();
    let result = plain_phase(owner, &budget, &probe, 1, [plan(2, 10)], false);
    assert!(matches!(
        result,
        Err(MemoryError::Capacity {
            requested: 2,
            available: 1
        })
    ));
    assert_eq!(budget.stats(), before);
    let owner = plain_phase(
        begin(&budget, 2, 1),
        &budget,
        &probe,
        1,
        [plan(1, 10)],
        false,
    )
    .unwrap();
    let result = owner.finish(100, |_| {
        panic!("incomplete imports cannot reach validation")
    });
    assert!(matches!(result, Err(MemoryError::InvalidConfiguration(_))));
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
}

#[test]
fn source_builder_copy_and_final_validation_refusals_refund_all_phases() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    for fault in 0..4 {
        let probe = Probe::default();
        let owner = plain_phase(
            begin(&budget, 5, 2),
            &budget,
            &probe,
            2,
            [plan(2, 20), plan(4, 20)],
            false,
        )
        .unwrap();
        let owner = owner.insert_phase(
            3,
            [
                plan(1, 10),
                plan(3, 10),
                if fault == 0 {
                    Err(MemoryError::MissingKey)
                } else {
                    plan(5, 10)
                },
            ],
            |_, key, byte, allowance| {
                if fault == 1 && *key == 3 {
                    return Err(MemoryError::MissingKey);
                }
                Row::build(&budget, &probe, byte, allowance)
            },
            |row| row.copy(fault == 2),
        );
        let result = owner.and_then(|owner| {
            owner.finish(500, |view| {
                assert_eq!(view.len(), 5);
                assert_eq!(fault, 3);
                Err(MemoryError::MissingKey)
            })
        });
        assert!(matches!(result, Err(MemoryError::MissingKey)));
        assert_eq!(probe.created.get(), probe.dropped.get(), "fault {fault}");
        assert_eq!(budget.stats(), before, "fault {fault}");
    }
}

#[test]
fn later_phase_precharges_refuse_pressure_and_excess_payloads_before_retention() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    let probe = Probe::default();
    let owner = plain_phase(
        begin(&budget, 2, 2),
        &budget,
        &probe,
        1,
        [plan(100, 20)],
        false,
    )
    .unwrap();
    let staging =
        2 * ALLOCATOR_OVERHEAD + 2 * (size_of::<Change<u64, Row<'_>>>() + size_of::<Allocation>());
    let heap = 32 + ALLOCATOR_OVERHEAD;
    let pressure_bytes = budget.stats().limit - budget.stats().used - staging - heap + 1;
    let pressure = budget
        .reserve(BudgetKind::Payload, BudgetLane::Completion, pressure_bytes)
        .unwrap();
    let result = plain_phase(owner, &budget, &probe, 1, [plan(1, 10)], false);
    assert!(matches!(result,
        Err(MemoryError::Capacity { requested, available }) if requested == heap && available == heap - 1));
    assert_eq!(probe.built.get(), 1);
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats().used, pressure_bytes);
    drop(pressure);
    assert_eq!(budget.stats(), before);

    let owner = plain_phase(
        begin(&budget, 2, 2),
        &budget,
        &probe,
        1,
        [plan(100, 20)],
        false,
    )
    .unwrap();
    let result = owner.insert_phase(
        1,
        [plan(1, 10)],
        |_, _, byte, allowance| {
            let (row, actual) = Row::build(&budget, &probe, byte, allowance)?;
            Ok((row, actual + 1))
        },
        |row| row.copy(false),
    );
    assert!(matches!(result,
        Err(MemoryError::Capacity { requested, available }) if requested == available + 1));
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);

    let before_builds = probe.built.get();
    let result = plain_phase(
        begin(&budget, 1, 1),
        &budget,
        &probe,
        1,
        [Ok(Entry::new(1, 10, config().max_entry_bytes))],
        false,
    );
    assert!(matches!(result, Err(MemoryError::ItemTooLarge { .. })));
    assert_eq!(probe.built.get(), before_builds);
    assert_eq!(budget.stats(), before);
}

#[test]
fn every_engine_allocation_failure_or_excess_discards_detached_phases_and_allows_retry() {
    use crate::range::preflight_tests::{
        AllocationFault, count_allocations, fault, fault_consumed,
    };
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    fn load<'a>(
        budget: &'a MemoryBudget,
        probe: &'a Probe,
    ) -> Result<RangeStore<u64, Row<'a>>, MemoryError> {
        let owner = RangeStore::begin_hydration_partitioned(
            RangeId(1313),
            config(),
            budget.clone(),
            partition,
            RangeHydrationLimits {
                expected_entries: 6,
                max_phases: 2,
            },
        )?;
        let owner = plain_phase(
            owner,
            budget,
            probe,
            3,
            [plan(2, 20), plan(4, 20), plan(6, 20)],
            false,
        )?;
        plain_phase(
            owner,
            budget,
            probe,
            3,
            [plan(1, 10), plan(3, 10), plan(5, 10)],
            false,
        )?
        .finish(909, |_| Ok(()))
    }
    let probe = Probe::default();
    let (store, sites) = count_allocations(|| load(&budget, &probe).unwrap());
    let expected = store
        .entries()
        .map(|entry| (entry.key, entry.value.bytes[0]))
        .collect::<Vec<_>>();
    assert!(sites > 0);
    drop(store);
    assert_eq!(budget.stats(), before);
    for kind in [AllocationFault::Fail, AllocationFault::Excess] {
        for at in 1..=sites {
            let probe = Probe::default();
            {
                let _fault = fault(at, kind);
                let result = load(&budget, &probe);
                match kind {
                    AllocationFault::Fail => {
                        assert!(matches!(result, Err(MemoryError::AllocationFailed)))
                    }
                    AllocationFault::Excess => assert!(matches!(result,
                        Err(MemoryError::Capacity { requested, available }) if requested > available)),
                }
                assert!(fault_consumed());
            }
            assert_eq!(probe.created.get(), probe.dropped.get(), "site {at}");
            assert_eq!(budget.stats(), before, "site {at}");
        }
    }
    let probe = Probe::default();
    let retry = load(&budget, &probe).unwrap();
    assert_eq!(retry.prefix(), 909);
    assert_eq!(
        retry
            .entries()
            .map(|entry| (entry.key, entry.value.bytes[0]))
            .collect::<Vec<_>>(),
        expected
    );
    drop(retry);
    assert_eq!(probe.created.get(), probe.dropped.get());
    assert_eq!(budget.stats(), before);
}
