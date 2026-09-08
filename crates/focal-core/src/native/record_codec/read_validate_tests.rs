use super::*;
use crate::native::report_tests as fixture;
use focal_memory::{Entry, RangeHydrationLimits};

fn with_read<T>(budget: &MemoryBudget, visits: usize, run: impl FnOnce(&ValidationRead<'_, '_>) -> T) -> T {
    let owner_budget = MemoryBudget::new(4 << 20, 0).unwrap();
    let detached = RangeStore::<Key, Row>::begin_hydration_partitioned(
        RangeId(50), RangeConfig::default(), owner_budget, page_partition,
        RangeHydrationLimits { expected_entries: 0, max_phases: 1 },
    ).unwrap();
    let meter = read_source::Meter::new(visits);
    let mut result = None;
    let owner = detached.finish(0, |root| {
        let read = ValidationRead { root: &root, ledger: fixture::binding(1).ledger,
            profile: NativeContentProfile::ProjectionOnly, prefix: SessionSeq(9),
            limits: NativeLimits::default(), meter: &meter, budget };
        result = Some(run(&read));
        Ok(())
    }).unwrap();
    drop(owner);
    result.unwrap()
}

#[test]
fn outcome_bitmap_proves_unsorted_sequence_uniqueness_and_refunds_exact_scratch() {
    let budget = MemoryBudget::new(1 << 20, 0).unwrap();
    let baseline = budget.stats();
    with_read(&budget, usize::MAX, |read| {
        let mut bits = Sequences::new(9, read).unwrap();
        assert_eq!(budget.stats().used - baseline.used, 2 + prepare::ALLOCATION);
        for sequence in [9, 2, 7, 1, 8, 3, 5, 4, 6] { bits.mark(SessionSeq(sequence), 9, read).unwrap(); }
        assert!(bits.mark(SessionSeq(9), 9, read).is_err());
        assert!(bits.mark(SessionSeq(0), 9, read).is_err());
        assert!(bits.mark(SessionSeq(10), 9, read).is_err());
        assert_eq!(bits.bits, [255, 1]);
    });
    assert_eq!(budget.stats(), baseline);
}

#[test]
fn bitmap_funding_and_work_refuse_before_allocation_and_remain_retryable() {
    let missing = MemoryBudget::new(prepare::ALLOCATION + 1, 0).unwrap();
    let baseline = missing.stats();
    with_read(&missing, usize::MAX, |read| assert!(Sequences::new(9, read).is_err()));
    assert_eq!(missing.stats(), baseline);
    let budget = MemoryBudget::new(prepare::ALLOCATION + 2, 0).unwrap();
    let baseline = budget.stats();
    with_read(&budget, 2, |read| assert!(Sequences::new(9, read).is_err()));
    assert_eq!(budget.stats(), baseline);
    with_read(&budget, 3, |read| {
        let mut bits = Sequences::new(9, read).unwrap();
        assert_eq!(read.meter.remaining(), 0);
        assert!(bits.mark(SessionSeq(1), 9, read).is_err());
        assert_eq!(bits.bits, [0, 0]);
    });
    assert_eq!(budget.stats(), baseline);
    with_read(&budget, 11, |read| Sequences::new(9, read).unwrap().mark(SessionSeq(1), 9, read).unwrap());
    assert_eq!(budget.stats(), baseline);
}

#[test]
fn exact_counts_refuse_profile_rows_missing_prefix_and_host_limits() {
    let budget = MemoryBudget::new(1024, 0).unwrap();
    with_read(&budget, usize::MAX, |read| {
        let mut counts = Counts::default();
        counts.meta.outcomes = 9;
        counts.check(counts.meta, read).unwrap();
        let mut expected = counts.meta;
        expected.results = 1;
        assert!(counts.check(expected, read).is_err());
        counts.contents = 1;
        assert!(counts.check(counts.meta, read).is_err());
        counts.contents = 0;
        counts.meta.outcomes = 8;
        assert!(counts.check(counts.meta, read).is_err());
        counts.meta.outcomes = 9;
        counts.meta.claims = read.limits.claims + 1;
        assert!(matches!(counts.check(counts.meta, read), Err(NativeError::Contract(ContractError::Capacity))));
    });
}

#[test]
fn zero_prefix_rejects_a_fabricated_meta_row_and_discards_detached_state() {
    let budget = MemoryBudget::new(4 << 20, 0).unwrap();
    let baseline = budget.stats();
    let meter = read_source::Meter::new(100_000);
    let root = RangeStore::<Key, Row>::begin_hydration_partitioned(
        RangeId(50), RangeConfig::default(), budget.clone(), page_partition,
        RangeHydrationLimits { expected_entries: 1, max_phases: 1 },
    ).unwrap().insert_phase(1,
        std::iter::once(Ok(Entry::new(Key::Meta, Meta::default(), 0))),
        |_, _, meta, _| Ok((Row::Meta(meta), 0)), prepare::copy,
    ).unwrap();
    let result = root.finish(0, |root| {
        let result = validate(root, fixture::binding(1).ledger, NativeContentProfile::ProjectionOnly,
            SessionSeq(0), NativeLimits::default(), &meter, &budget);
        assert!(matches!(result, Err(NativeError::Contract(ContractError::InvalidManifest))));
        Err(MemoryError::InvalidConfiguration("rejected fabricated genesis"))
    });
    assert!(result.is_err());
    assert_eq!(budget.stats(), baseline);
}
