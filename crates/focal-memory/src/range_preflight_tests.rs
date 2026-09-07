use super::*;
use std::cell::Cell;

#[derive(Clone, Copy)]
pub(super) enum AllocationFault {
    Fail,
    Excess,
}
thread_local! {
    static FAULT: Cell<Option<(usize, AllocationFault)>> = const { Cell::new(None) };
    static ALLOCATION_ATTEMPTS: Cell<usize> = const { Cell::new(0) };
}
pub(super) struct FaultGuard;
impl Drop for FaultGuard {
    fn drop(&mut self) {
        FAULT.set(None);
    }
}
pub(super) fn fault(at: usize, kind: AllocationFault) -> FaultGuard {
    assert!(at > 0);
    assert!(FAULT.get().is_none());
    FAULT.set(Some((at, kind)));
    FaultGuard
}
pub(super) fn allocation_capacity(capacity: usize) -> Result<usize, MemoryError> {
    ALLOCATION_ATTEMPTS.set(ALLOCATION_ATTEMPTS.get() + 1);
    FAULT.with(|slot| match slot.get() {
        None => Ok(capacity),
        Some((1, kind)) => {
            slot.set(None);
            match kind {
                AllocationFault::Fail => Err(MemoryError::AllocationFailed),
                AllocationFault::Excess => checked_add(capacity, 1),
            }
        }
        Some((remaining, kind)) => {
            slot.set(Some((remaining - 1, kind)));
            Ok(capacity)
        }
    })
}

pub(super) fn count_allocations<T>(action: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOCATION_ATTEMPTS.get();
    let result = action();
    (result, ALLOCATION_ATTEMPTS.get() - before)
}

pub(super) fn fault_consumed() -> bool {
    FAULT.get().is_none()
}

/// Directory path copying may retain less than its conservative quote. Leaf
/// charges remain exact, and no Pending or other temporary debit may escape.
pub(super) fn assert_retained(
    before: &crate::BudgetStats,
    after: &crate::BudgetStats,
    charges: RangePreparationCharges,
) {
    let pages =
        after.by_kind[BudgetKind::Pages as usize] - before.by_kind[BudgetKind::Pages as usize];
    let roots =
        after.by_kind[BudgetKind::Roots as usize] - before.by_kind[BudgetKind::Roots as usize];
    assert_eq!(pages, charges.new_pages_bytes());
    assert!(roots <= charges.directory_bytes());
    assert_eq!(after.used - before.used, pages + roots);
    assert!(after.used - before.used <= charges.additional_retained_bytes());
    for (index, (&previous, &current)) in before.by_kind.iter().zip(&after.by_kind).enumerate() {
        if index != BudgetKind::Pages as usize && index != BudgetKind::Roots as usize {
            assert_eq!(current, previous, "unexpected retained category {index}");
        }
    }
}

/// No Clone implementation: the planner cannot estimate by cloning rows.
#[derive(Debug, PartialEq, Eq)]
struct Row {
    label: u64,
    bytes: Vec<u8>,
}
fn entry(key: u64, bytes: usize) -> Entry<u64, Row> {
    let value = Row {
        label: key,
        bytes: vec![17; bytes],
    };
    let heap = value.bytes.capacity() + ALLOCATOR_OVERHEAD;
    Entry::new(key, value, heap)
}
fn copy(row: &Row) -> Result<Row, MemoryError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(row.bytes.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    bytes.extend_from_slice(&row.bytes);
    Ok(Row {
        label: row.label,
        bytes,
    })
}
fn no_copy(_: &Row) -> Result<Row, MemoryError> {
    panic!("unexpected copied value");
}
fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 4,
        max_batch_entries: 64,
        ..RangeConfig::default()
    }
}
fn seed(budget: &MemoryBudget) -> RangeStore<u64, Row> {
    let mut range = RangeStore::new(RangeId(17), 0, config(), budget.clone()).unwrap();
    let candidate = range
        .prepare_batch_with(
            1,
            (1..=12)
                .map(|n| Change::Put(entry(n * 10, n as usize * 10)))
                .collect(),
            BudgetLane::Ordinary,
            no_copy,
        )
        .unwrap();
    range.publish(candidate).unwrap();
    range
}
fn changes() -> Vec<Change<u64, Row>> {
    vec![
        Change::Put(entry(25, 2048)),
        Change::Delete(30),
        Change::Put(entry(20, 1024)),
    ]
}
fn labels(range: &RangeStore<u64, Row>) -> Vec<u64> {
    range.entries().map(|entry| entry.value.label).collect()
}
fn pinned(lease: &SnapshotLease<u64, Row>) -> Vec<u64> {
    let mut values = Vec::new();
    let mut key = 0;
    while let Some(next) = lease
        .project_next(&key, false, &u64::MAX, 0, |entry| entry.key)
        .unwrap()
    {
        values.push(next);
        key = next + 1;
    }
    values
}

#[test]
fn exact_touched_page_quote_counts_moved_heap_without_copying_untouched_rows() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let range = seed(&budget);
    let baseline = budget.stats();
    let incoming = changes();
    let pointer = match &incoming[0] {
        Change::Put(row) => row.value.bytes.as_ptr(),
        _ => unreachable!(),
    };
    let untouched = range.get(&80).unwrap().bytes.as_ptr();
    let plan = range
        .plan_batch(2, incoming, BudgetLane::Ordinary, usize::MAX)
        .unwrap();
    assert_eq!(plan.output_pages(), 3); // Four surviving first-page entries; two shared pages.
    assert_eq!(plan.base_prefix(), 1);
    assert_eq!(plan.prefix(), 2);
    assert_eq!(budget.stats(), baseline);
    let charges = plan.charges();
    let mut copied = Vec::new();
    let candidate = plan
        .build_with(|old| {
            copied.push(old.label);
            assert!(budget.stats().used - baseline.used <= charges.additional_peak_bytes());
            copy(old)
        })
        .unwrap();
    assert_eq!(copied, [10, 40]);
    assert_eq!(candidate.get(&25).unwrap().bytes.as_ptr(), pointer);
    assert_eq!(candidate.get(&80).unwrap().bytes.as_ptr(), untouched);
    assert_eq!(candidate.len(), 12);
    assert!(candidate.get(&30).is_none());
    let after = budget.stats();
    assert_retained(&baseline, &after, charges);
    // Each new page owns both an Arc allocation and an entry vector allocation.
    let expected = 2 * ALLOCATOR_OVERHEAD
        + size_of::<Page<u64, Row>>()
        + 2 * size_of::<usize>()
        + 4 * size_of::<Entry<u64, Row>>()
        + (10 + 40 + 1024 + 2048)
        + 4 * ALLOCATOR_OVERHEAD;
    assert_eq!(charges.new_pages_bytes(), expected);
    drop(candidate);
    assert_eq!(budget.stats(), baseline);
}

#[test]
fn one_byte_below_quote_refuses_without_internal_allocation_or_published_change() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let mut range = seed(&budget);
    let lease = range.pin(0, 10).unwrap();
    let baseline = budget.stats();
    let original = labels(&range);
    let bound = range
        .plan_batch(2, changes(), BudgetLane::Ordinary, usize::MAX)
        .unwrap()
        .charges()
        .additional_peak_bytes();
    let _fault = fault(1, AllocationFault::Fail);
    let refused = range.plan_batch(2, changes(), BudgetLane::Ordinary, bound - 1);
    assert!(
        matches!(refused, Err(MemoryError::Capacity { requested, available }) if requested == bound && available == bound - 1)
    );
    assert_eq!(budget.stats(), baseline);
    assert_eq!(labels(&range), original);
    assert_eq!(pinned(&lease), original);
    // Neither quotation used an internal allocation; the first real build still
    // reaches the injected allocator refusal before invoking the copier.
    let plan = range
        .plan_batch(2, changes(), BudgetLane::Ordinary, bound)
        .unwrap();
    assert!(matches!(
        plan.build_with(no_copy),
        Err(MemoryError::AllocationFailed)
    ));
    assert_eq!(budget.stats(), baseline);
    assert_eq!(range.prefix(), 1);
}

#[test]
fn quoted_peak_allowance_funds_actual_build_and_is_not_a_hidden_reservation() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let range = seed(&budget);
    let baseline = budget.stats();
    let plan = range
        .plan_batch(2, changes(), BudgetLane::Ordinary, usize::MAX)
        .unwrap();
    let charges = plan.charges();
    assert_eq!(budget.stats(), baseline);
    let blocker = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            baseline.limit - baseline.used - charges.additional_peak_bytes(),
        )
        .unwrap();
    let occupied = budget.stats();
    let mut copied = 0;
    let candidate = plan
        .build_with(|old| {
            copied += 1;
            assert!(budget.stats().used <= budget.stats().limit);
            assert!(budget.stats().used - occupied.used <= charges.additional_peak_bytes());
            copy(old)
        })
        .unwrap();
    assert_eq!(copied, 2);
    assert_retained(&occupied, &budget.stats(), charges);
    drop(candidate);
    assert_eq!(budget.stats(), occupied);
    drop(blocker);
    assert_eq!(budget.stats(), baseline);

    // A quote provides no entitlement: later pressure can refuse a build.
    let plan = range
        .plan_batch(2, changes(), BudgetLane::Ordinary, usize::MAX)
        .unwrap();
    let blocker = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            baseline.limit - baseline.used,
        )
        .unwrap();
    assert!(matches!(
        plan.build_with(no_copy),
        Err(MemoryError::Capacity { .. })
    ));
    drop(blocker);
    assert_eq!(budget.stats(), baseline);
}

#[test]
fn every_range_buffer_failure_or_excess_capacity_rolls_back_pages_roots_and_pins() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let mut range = seed(&budget);
    let lease = range.pin(0, 10).unwrap();
    let baseline = budget.stats();
    let original = labels(&range);
    let writes = || vec![Change::Put(entry(20, 1024)), Change::Put(entry(100, 2048))];
    let successful = range
        .plan_batch(2, writes(), BudgetLane::Ordinary, usize::MAX)
        .unwrap();
    let (candidate, sites) = count_allocations(|| successful.build_with(copy).unwrap());
    assert!(sites > 0);
    drop(candidate);
    assert_eq!(budget.stats(), baseline);
    // Cover every observed buffer allocation, including newly introduced
    // directory nodes, without freezing an implementation-specific site count.
    for kind in [AllocationFault::Fail, AllocationFault::Excess] {
        let mut failed_after_copy = false;
        for at in 1..=sites {
            let _fault = fault(at, kind);
            let plan = range
                .plan_batch(2, writes(), BudgetLane::Ordinary, usize::MAX)
                .unwrap();
            let mut copies = 0;
            let result = plan.build_with(|old| {
                copies += 1;
                copy(old)
            });
            match kind {
                AllocationFault::Fail => {
                    assert!(matches!(result, Err(MemoryError::AllocationFailed)))
                }
                AllocationFault::Excess => assert!(
                    matches!(result, Err(MemoryError::Capacity { requested, available }) if requested > available)
                ),
            }
            assert!(FAULT.get().is_none(), "injected site {at} was not reached");
            failed_after_copy |= copies > 0;
            assert_eq!(budget.stats(), baseline, "site {at}");
            assert_eq!(labels(&range), original);
            assert_eq!(pinned(&lease), original);
        }
        assert!(
            failed_after_copy,
            "no injected failure exercised a partially copied candidate"
        );
    }
}

#[test]
fn checkpoint_import_staging_and_later_page_failures_release_complete_partial_store() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let baseline = budget.stats();
    let input = || (1..=8).map(|key| Entry::new(key, vec![42u8; 64], 64 + ALLOCATOR_OVERHEAD));
    let (complete, sites) = count_allocations(|| {
        RangeStore::from_entries(RangeId(19), 77, config(), budget.clone(), input()).unwrap()
    });
    assert!(sites > 0);
    drop(complete);
    assert_eq!(budget.stats(), baseline);
    let (first_chunk, first_chunk_sites) = count_allocations(|| {
        RangeStore::from_entries(RangeId(19), 77, config(), budget.clone(), input().take(4))
            .unwrap()
    });
    drop(first_chunk);
    assert!(
        sites > first_chunk_sites,
        "fixture must include later import allocations"
    );
    assert_eq!(budget.stats(), baseline);
    for kind in [AllocationFault::Fail, AllocationFault::Excess] {
        // Failures after the first chunk also discard its privately installed
        // root. Every directory and import buffer reached by the successful
        // reference construction participates in the same allocator hook.
        let mut failed_after_first_chunk = false;
        for at in 1..=sites {
            let _fault = fault(at, kind);
            let result =
                RangeStore::from_entries(RangeId(19), 77, config(), budget.clone(), input());
            match kind {
                AllocationFault::Fail => {
                    assert!(matches!(result, Err(MemoryError::AllocationFailed)))
                }
                AllocationFault::Excess => assert!(
                    matches!(result, Err(MemoryError::Capacity { requested, available }) if requested > available)
                ),
            }
            assert!(
                FAULT.get().is_none(),
                "import allocation {at} was not reached"
            );
            failed_after_first_chunk |= at > first_chunk_sites;
            assert_eq!(budget.stats(), baseline, "import site {at}");
        }
        assert!(failed_after_first_chunk);
    }
    let store = RangeStore::from_entries(
        RangeId(19),
        77,
        config(),
        budget.clone(),
        (1..=8).map(|key| Entry::new(key, key, 0)),
    )
    .unwrap();
    assert_eq!(store.prefix(), 77);
    assert_eq!(
        store.entries().map(|entry| entry.key).collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    drop(store);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn suffix_plan_uses_actual_predecessor_heap_and_retains_exact_publication_chain() {
    let budget = MemoryBudget::new(4_000_000, 0).unwrap();
    let mut range = seed(&budget);
    let initial = budget.stats();
    let first = range
        .plan_batch(
            2,
            vec![Change::Put(entry(20, 50_000))],
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap()
        .build_with(copy)
        .unwrap();
    let suffix = vec![Change::Put(entry(30, 5))];
    let cheap_quote = range
        .plan_batch(
            2,
            vec![Change::Put(entry(30, 5))],
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap()
        .charges();
    assert!(matches!(
        range.plan_after(
            &first,
            3,
            suffix,
            BudgetLane::Ordinary,
            cheap_quote.additional_peak_bytes()
        ),
        Err(MemoryError::Capacity { .. })
    ));
    let plan = range
        .plan_after(
            &first,
            3,
            vec![Change::Put(entry(30, 5))],
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap();
    assert!(plan.charges().new_pages_bytes() > cheap_quote.new_pages_bytes());
    let second = plan.build_with(copy).unwrap();
    assert_eq!(second.get(&20).unwrap().bytes.len(), 50_000);
    range.validate_chain([&first, &second]).unwrap();
    let (error, second) = range.publish_recoverable(second).err().unwrap();
    assert!(matches!(error, MemoryError::StalePreparation { .. }));
    let foreign = RangeStore::new(RangeId(17), 1, config(), budget.clone()).unwrap();
    assert!(matches!(
        foreign.plan_after(&first, 3, vec![], BudgetLane::Ordinary, usize::MAX),
        Err(MemoryError::WrongRange)
    ));
    drop(foreign);
    assert_eq!(range.prefix(), 1);
    range.publish(first).unwrap();
    range.publish(second).unwrap();
    assert_eq!(range.prefix(), 3);
    assert_eq!(range.get(&20).unwrap().bytes.len(), 50_000);
    assert!(budget.stats().used > initial.used);
}

#[test]
fn deletion_empty_batches_and_input_capacity_preserve_quote_components() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let range = seed(&budget);
    let plan = range
        .plan_batch(
            2,
            (1..=4).map(|key| Change::Delete(key * 10)).collect(),
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap();
    assert_eq!(plan.output_pages(), 2);
    assert_eq!(plan.charges().new_pages_bytes(), 0);
    let candidate = plan.build_with(no_copy).unwrap();
    assert_eq!(
        candidate
            .entries()
            .map(|entry| entry.key)
            .collect::<Vec<_>>(),
        (5..=12).map(|key| key * 10).collect::<Vec<_>>()
    );
    let empty = range
        .plan_after(&candidate, 3, vec![], BudgetLane::Ordinary, usize::MAX)
        .unwrap();
    assert_eq!(empty.output_pages(), 2);
    assert_eq!(empty.charges().input_pending_bytes(), 0);
    assert_eq!(empty.charges().merge_pending_bytes(), 0);
    assert_eq!(empty.charges().new_pages_bytes(), 0);
    drop(empty.build_with(no_copy).unwrap());
    let compact = range
        .plan_batch(
            2,
            vec![Change::Put(entry(20, 1))],
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap()
        .charges();
    let mut roomy = Vec::with_capacity(60);
    roomy.push(Change::Put(entry(20, 1)));
    let capacity = roomy.capacity();
    let larger = range
        .plan_batch(2, roomy, BudgetLane::Ordinary, usize::MAX)
        .unwrap()
        .charges();
    assert_eq!(
        larger.input_pending_bytes() - compact.input_pending_bytes(),
        (capacity - 1) * size_of::<Change<u64, Row>>()
    );
    assert_eq!(larger.new_pages_bytes(), compact.new_pages_bytes());
    assert_eq!(larger.directory_bytes(), compact.directory_bytes());
}
