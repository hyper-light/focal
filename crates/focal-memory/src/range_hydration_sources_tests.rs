use super::*;
use std::cell::Cell;

#[derive(Default)]
struct Probe {
    prepares: Cell<usize>,
    builds: Cell<usize>,
    active_plans: Cell<usize>,
    dropped_plans: Cell<usize>,
    created_rows: Cell<usize>,
    dropped_rows: Cell<usize>,
}

struct Row<'m> {
    bytes: Vec<u8>,
    budget: &'m MemoryBudget,
    probe: &'m Probe,
}

fn heap(bytes: usize) -> usize {
    bytes + if bytes == 0 { 0 } else { ALLOCATOR_OVERHEAD }
}

impl<'m> Row<'m> {
    fn copy(&self) -> Result<Self, MemoryError> {
        assert_eq!(
            self.probe.active_plans.get(),
            0,
            "no dependency plan may survive into page construction"
        );
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.bytes.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        if bytes.capacity() > self.bytes.capacity() {
            return Err(MemoryError::Capacity {
                requested: bytes.capacity(),
                available: self.bytes.capacity(),
            });
        }
        bytes.extend_from_slice(&self.bytes);
        self.probe
            .created_rows
            .set(self.probe.created_rows.get() + 1);
        Ok(Self {
            bytes,
            budget: self.budget,
            probe: self.probe,
        })
    }
}

impl Drop for Row<'_> {
    fn drop(&mut self) {
        let stats = self.budget.stats();
        assert!(
            stats.by_kind[BudgetKind::Pending as usize] + stats.by_kind[BudgetKind::Pages as usize]
                >= heap(self.bytes.capacity())
        );
        self.probe
            .dropped_rows
            .set(self.probe.dropped_rows.get() + 1);
    }
}

struct Source<'m> {
    key: u64,
    dependency: Option<u64>,
    extra: usize,
    budget: &'m MemoryBudget,
    probe: &'m Probe,
    fail_prepare: bool,
    fail_build: bool,
    wrong_key: bool,
    overreport: bool,
}

struct Plan<'a, 'm> {
    source: &'a Source<'m>,
    dependency: Option<&'a Row<'m>>,
    bytes: usize,
}

impl Drop for Plan<'_, '_> {
    fn drop(&mut self) {
        let probe = self.source.probe;
        probe.active_plans.set(probe.active_plans.get() - 1);
        probe.dropped_plans.set(probe.dropped_plans.get() + 1);
    }
}

impl<'m> RangeHydrationSource<u64, Row<'m>> for Source<'m> {
    type Plan<'a>
        = Plan<'a, 'm>
    where
        Self: 'a,
        Row<'m>: 'a;

    fn key(&self) -> &u64 {
        &self.key
    }

    fn prepare<'a>(
        &'a self,
        dependencies: RangeHydrationLookup<'a, u64, Row<'m>>,
    ) -> Result<Entry<u64, Self::Plan<'a>>, MemoryError>
    where
        Row<'m>: 'a,
    {
        self.probe.prepares.set(self.probe.prepares.get() + 1);
        if self.fail_prepare {
            return Err(MemoryError::MissingKey);
        }
        let dependency = self
            .dependency
            .map(|key| dependencies.get(&key).ok_or(MemoryError::MissingKey))
            .transpose()?;
        let bytes = dependency
            .map_or(0, |row| row.bytes.len())
            .checked_add(self.extra)
            .ok_or(MemoryError::CounterExhausted("source quote"))?;
        self.probe
            .active_plans
            .set(self.probe.active_plans.get() + 1);
        Ok(Entry::new(
            if self.wrong_key { 999 } else { self.key },
            Plan {
                source: self,
                dependency,
                bytes,
            },
            heap(bytes),
        ))
    }

    fn build<'a>(plan: Self::Plan<'a>, allowance: usize) -> Result<(Row<'m>, usize), MemoryError>
    where
        Self: 'a,
        Row<'m>: 'a,
    {
        let source = plan.source;
        assert_eq!(allowance, heap(plan.bytes));
        assert!(source.budget.stats().by_kind[BudgetKind::Pending as usize] >= allowance);
        source.probe.builds.set(source.probe.builds.get() + 1);
        if source.fail_build {
            return Err(MemoryError::MissingKey);
        }
        // The borrowed dependency is used during build, not copied into an
        // unaccounted intermediate plan or detached from its source lifetime.
        assert_eq!(
            plan.bytes,
            plan.dependency.map_or(0, |row| row.bytes.len()) + source.extra
        );
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(plan.bytes)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let actual = heap(bytes.capacity());
        if actual > allowance {
            return Err(MemoryError::Capacity {
                requested: actual,
                available: allowance,
            });
        }
        bytes.resize(plan.bytes, 17);
        source
            .probe
            .created_rows
            .set(source.probe.created_rows.get() + 1);
        let row = Row {
            bytes,
            budget: source.budget,
            probe: source.probe,
        };
        Ok((row, actual + usize::from(source.overreport)))
    }
}

fn source<'m>(
    budget: &'m MemoryBudget,
    probe: &'m Probe,
    key: u64,
    dependency: Option<u64>,
    extra: usize,
) -> Source<'m> {
    Source {
        key,
        dependency,
        extra,
        budget,
        probe,
        fail_prepare: false,
        fail_build: false,
        wrong_key: false,
        overreport: false,
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

fn begin<'m>(
    budget: &'m MemoryBudget,
    count: usize,
    phases: usize,
    config: RangeConfig,
) -> Result<RangeHydration<u64, Row<'m>>, MemoryError> {
    RangeStore::begin_hydration_partitioned(
        RangeId(1401),
        config,
        budget.clone(),
        partition,
        RangeHydrationLimits {
            expected_entries: count,
            max_phases: phases,
        },
    )
}

#[test]
fn scoped_plans_borrow_previous_phases_and_staged_rows_to_quote_exact_final_payloads() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    let probe = Probe::default();
    let owner = begin(&budget, 5, 2, config())
        .unwrap()
        .insert_sources(
            2,
            [
                Ok(source(&budget, &probe, 100, None, 7)),
                Ok(source(&budget, &probe, 101, Some(100), 5)),
            ],
            Row::copy,
        )
        .unwrap();
    let owner = owner
        .insert_sources(
            3,
            [
                Ok(source(&budget, &probe, 0, Some(101), 2)),
                Ok(source(&budget, &probe, 1, Some(0), 1)),
                Ok(source(&budget, &probe, 2, Some(1), 1)),
            ],
            Row::copy,
        )
        .unwrap();
    assert_eq!(probe.prepares.get(), 5);
    assert_eq!(probe.builds.get(), 5);
    assert_eq!(probe.active_plans.get(), 0);
    assert_eq!(probe.dropped_plans.get(), 5);
    let store = owner
        .finish(900, |view| {
            assert_eq!(
                view.entries()
                    .map(|entry| (entry.key, entry.value.bytes.len()))
                    .collect::<Vec<_>>(),
                [(0, 14), (1, 15), (2, 16), (100, 7), (101, 12)]
            );
            for entry in view.entries() {
                assert_eq!(entry.heap_bytes, heap(entry.value.bytes.capacity()));
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(store.prefix(), 900);
    assert_eq!(budget.stats().by_kind[BudgetKind::Pending as usize], 0);
    drop(store);
    assert_eq!(probe.created_rows.get(), probe.dropped_rows.get());
    assert_eq!(budget.stats(), before);
}

#[test]
fn only_byte_boundaries_repeat_preparation_and_all_borrows_drop_before_installation() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    let probe = Probe::default();
    let config = RangeConfig {
        max_batch_entries: 4,
        page_bytes: page_charge::<u64, Row<'_>>(1, heap(32)).unwrap(),
        ..config()
    };
    let owner = begin(&budget, 3, 1, config)
        .unwrap()
        .insert_sources(
            3,
            [
                Ok(source(&budget, &probe, 1, None, 16)),
                Ok(source(&budget, &probe, 2, Some(1), 1)),
                Ok(source(&budget, &probe, 3, Some(2), 1)),
            ],
            Row::copy,
        )
        .unwrap();
    assert_eq!(probe.prepares.get(), 5);
    assert_eq!(probe.builds.get(), 3);
    assert_eq!(probe.active_plans.get(), 0);
    assert_eq!(probe.dropped_plans.get(), 5);
    let store = owner.finish(901, |_| Ok(())).unwrap();
    assert_eq!(store.stats().pages, 3);
    drop(store);
    assert_eq!(budget.stats(), before);
    let probe = Probe::default();
    let owner = begin(&budget, 2, 1, config)
        .unwrap()
        .insert_sources(
            2,
            [
                Ok(source(&budget, &probe, 1, None, 16)),
                Ok(source(&budget, &probe, 100, Some(1), 1)),
            ],
            Row::copy,
        )
        .unwrap();
    assert_eq!(
        probe.prepares.get(),
        2,
        "partition boundary should precede preparation"
    );
    assert_eq!(probe.builds.get(), 2);
    drop(owner);
    assert_eq!(probe.created_rows.get(), probe.dropped_rows.get());
    assert_eq!(budget.stats(), before);
}

#[test]
fn preparation_build_wrong_key_and_actual_capacity_refusals_discard_all_phases() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    for fault in 0..5 {
        let probe = Probe::default();
        let owner = begin(&budget, 2, 2, config())
            .unwrap()
            .insert_sources(1, [Ok(source(&budget, &probe, 100, None, 16))], Row::copy)
            .unwrap();
        let mut next = source(&budget, &probe, 1, Some(100), 8);
        match fault {
            0 => next.fail_prepare = true,
            1 => next.fail_build = true,
            2 => next.wrong_key = true,
            3 => next.overreport = true,
            _ => next.dependency = Some(999),
        }
        assert!(owner.insert_sources(1, [Ok(next)], Row::copy).is_err());
        assert_eq!(probe.active_plans.get(), 0, "fault {fault}");
        assert_eq!(
            probe.created_rows.get(),
            probe.dropped_rows.get(),
            "fault {fault}"
        );
        assert_eq!(budget.stats(), before, "fault {fault}");
    }
    let probe = Probe::default();
    let owner = begin(&budget, 2, 2, config())
        .unwrap()
        .insert_sources(1, [Ok(source(&budget, &probe, 100, None, 16))], Row::copy)
        .unwrap();
    let staging =
        2 * ALLOCATOR_OVERHEAD + 2 * (size_of::<Change<u64, Row<'_>>>() + size_of::<Allocation>());
    let required = heap(24);
    let pressure_bytes = budget.stats().limit - budget.stats().used - staging - required + 1;
    let pressure = budget
        .reserve(BudgetKind::Payload, BudgetLane::Completion, pressure_bytes)
        .unwrap();
    let refused =
        owner.insert_sources(1, [Ok(source(&budget, &probe, 1, Some(100), 8))], Row::copy);
    assert!(matches!(refused,
        Err(MemoryError::Capacity { requested, available }) if requested == required && available == required - 1));
    assert_eq!(
        probe.prepares.get(),
        2,
        "exact quote must come from its real dependency"
    );
    assert_eq!(
        probe.builds.get(),
        1,
        "unfunded construction must never run"
    );
    assert_eq!(probe.active_plans.get(), 0);
    assert_eq!(probe.created_rows.get(), probe.dropped_rows.get());
    assert_eq!(budget.stats().used, pressure_bytes);
    drop(pressure);
    assert_eq!(budget.stats(), before);
}

#[test]
fn source_order_disjointness_and_exact_count_are_checked_without_preparing_extra_rows() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    for (count, keys, prepared) in [
        (2, vec![1, 1], 1),
        (3, vec![1, 2, 1], 2),
        (1, vec![1, 2], 1),
        (2, vec![1], 1),
        (0, vec![1], 0),
    ] {
        let probe = Probe::default();
        let result = begin(&budget, 3, 1, config()).unwrap().insert_sources(
            count,
            keys.into_iter()
                .map(|key| Ok(source(&budget, &probe, key, None, 16))),
            Row::copy,
        );
        assert!(matches!(result, Err(MemoryError::InvalidConfiguration(_))));
        assert_eq!(probe.prepares.get(), prepared);
        assert_eq!(probe.active_plans.get(), 0);
        assert_eq!(probe.created_rows.get(), probe.dropped_rows.get());
        assert_eq!(budget.stats(), before);
    }
    let probe = Probe::default();
    let owner = begin(&budget, 2, 2, config())
        .unwrap()
        .insert_sources(1, [Ok(source(&budget, &probe, 100, None, 16))], Row::copy)
        .unwrap();
    let result = owner.insert_sources(1, [Ok(source(&budget, &probe, 100, None, 16))], Row::copy);
    assert!(matches!(result, Err(MemoryError::InvalidConfiguration(_))));
    assert_eq!(probe.prepares.get(), 1);
    assert_eq!(probe.created_rows.get(), probe.dropped_rows.get());
    assert_eq!(budget.stats(), before);
}

#[test]
fn every_engine_allocation_refusal_releases_borrowed_plans_owned_rows_and_allows_retry() {
    use crate::range::preflight_tests::{
        AllocationFault, count_allocations, fault, fault_consumed,
    };
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let before = budget.stats();
    fn load<'m>(
        budget: &'m MemoryBudget,
        probe: &'m Probe,
    ) -> Result<RangeStore<u64, Row<'m>>, MemoryError> {
        let owner = begin(budget, 4, 2, config())?.insert_sources(
            2,
            [
                Ok(source(budget, probe, 2, None, 12)),
                Ok(source(budget, probe, 4, Some(2), 4)),
            ],
            Row::copy,
        )?;
        owner
            .insert_sources(
                2,
                [
                    Ok(source(budget, probe, 1, Some(2), 1)),
                    Ok(source(budget, probe, 3, Some(4), 2)),
                ],
                Row::copy,
            )?
            .finish(902, |_| Ok(()))
    }
    let probe = Probe::default();
    let (store, sites) = count_allocations(|| load(&budget, &probe).unwrap());
    let expected = store
        .entries()
        .map(|entry| (entry.key, entry.value.bytes.len()))
        .collect::<Vec<_>>();
    drop(store);
    assert_eq!(probe.active_plans.get(), 0);
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
            assert_eq!(probe.active_plans.get(), 0, "allocation {at}");
            assert_eq!(
                probe.created_rows.get(),
                probe.dropped_rows.get(),
                "allocation {at}"
            );
            assert_eq!(budget.stats(), before, "allocation {at}");
        }
    }
    let probe = Probe::default();
    let retry = load(&budget, &probe).unwrap();
    assert_eq!(
        retry
            .entries()
            .map(|entry| (entry.key, entry.value.bytes.len()))
            .collect::<Vec<_>>(),
        expected
    );
    drop(retry);
    assert_eq!(probe.created_rows.get(), probe.dropped_rows.get());
    assert_eq!(budget.stats(), before);
}
