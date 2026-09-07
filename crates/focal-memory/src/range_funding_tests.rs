use super::*;

fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 2,
        ..RangeConfig::default()
    }
}
fn value(key: u64) -> Change<u64, Vec<u8>> {
    Change::Put(Entry::new(key, vec![42; 1024], 1024 + ALLOCATOR_OVERHEAD))
}
fn seeded(budget: &MemoryBudget) -> RangeStore<u64, Vec<u8>> {
    let mut range = RangeStore::new(RangeId(501), 0, config(), budget.clone()).unwrap();
    range
        .apply_batch(
            1,
            (1..=4).map(|key| value(key * 10)).collect(),
            BudgetLane::Ordinary,
        )
        .unwrap();
    range
}

#[test]
fn funded_build_at_full_parent_returns_temporary_and_failed_copy_capacity_to_pool() {
    let budget = MemoryBudget::new(2_000_000, 500_000).unwrap();
    let range = seeded(&budget);
    let plan = range
        .plan_batch(2, vec![value(10)], BudgetLane::Completion, usize::MAX)
        .unwrap();
    let charge = plan.charges();
    let pool = budget
        .funded_child(BudgetLane::Completion, charge.additional_peak_bytes())
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
    let mut copied = 0;
    let candidate = plan
        .build_in_with(&pool, |old| {
            copied += 1;
            assert_eq!(budget.stats().used, parent.used);
            assert!(pool.stats().used <= charge.additional_peak_bytes());
            Ok(old.clone())
        })
        .unwrap();
    assert_eq!(copied, 1);
    preflight_tests::assert_retained(&unspent, &pool.stats(), charge);
    assert_eq!(budget.stats().used, parent.used);
    assert_eq!(candidate.get(&10).unwrap().len(), 1024);
    drop(candidate);
    assert_eq!(pool.stats().used, 0);
    assert_eq!(budget.stats(), parent);

    let plan = range
        .plan_batch(2, vec![value(10)], BudgetLane::Completion, usize::MAX)
        .unwrap();
    assert!(matches!(
        plan.build_in_with(&pool, |_| Err(MemoryError::AllocationFailed)),
        Err(MemoryError::AllocationFailed)
    ));
    assert_eq!(pool.stats().used, 0);
    assert_eq!(budget.stats(), parent);
    assert_eq!(range.prefix(), 1);
    assert_eq!(range.len(), 4);
    drop(pressure);
    drop(pool);
    drop(range);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn insufficient_pool_and_foreign_sources_refuse_without_published_change() {
    let budget = MemoryBudget::new(2_000_000, 500_000).unwrap();
    let range = seeded(&budget);
    let plan = range
        .plan_batch(2, vec![value(10)], BudgetLane::Completion, usize::MAX)
        .unwrap();
    let pool = budget
        .funded_child(
            BudgetLane::Completion,
            plan.charges().input_pending_bytes() + root_charge::<u64, Vec<u8>>(0).unwrap() - 1,
        )
        .unwrap();
    let parent = budget.stats();
    assert!(matches!(
        plan.build_in_with(&pool, |_| panic!("capacity must refuse before copier")),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(pool.stats().used, 0);
    assert_eq!(budget.stats(), parent);
    let foreign = MemoryBudget::new(2_000_000, 0).unwrap();
    let foreign_pool = foreign
        .funded_child(BudgetLane::Completion, 100_000)
        .unwrap();
    let foreign_before = foreign.stats();
    let plan = range
        .plan_batch(2, vec![value(10)], BudgetLane::Completion, usize::MAX)
        .unwrap();
    assert!(matches!(
        plan.build_in_with(&foreign_pool, |_| panic!("foreign source reached copier")),
        Err(MemoryError::InvalidConfiguration(_))
    ));
    assert_eq!(foreign.stats(), foreign_before);
    assert_eq!(budget.stats(), parent);
    assert_eq!(range.prefix(), 1);
}

#[test]
fn source_lane_policy_preserves_ordinary_hold_and_denies_completion_laundering() {
    let budget = MemoryBudget::new(2_000_000, 500_000).unwrap();
    let range = seeded(&budget);
    let quote = range
        .plan_batch(2, vec![value(10)], BudgetLane::Completion, usize::MAX)
        .unwrap()
        .charges();
    let ordinary = budget
        .funded_child(BudgetLane::Ordinary, quote.additional_peak_bytes())
        .unwrap();
    let baseline = budget.stats();
    let completed = range
        .plan_batch(2, vec![value(10)], BudgetLane::Completion, usize::MAX)
        .unwrap()
        .build_in(&ordinary)
        .unwrap();
    assert_eq!(budget.stats().ordinary_used, baseline.ordinary_used);
    assert_eq!(budget.stats().used, baseline.used);
    drop(completed);
    assert_eq!(budget.stats(), baseline);

    let completion = budget
        .funded_child(BudgetLane::Completion, quote.additional_peak_bytes())
        .unwrap();
    let descendant = completion.child(quote.additional_peak_bytes(), 0).unwrap();
    let baseline = budget.stats();
    for source in [&completion, &descendant] {
        let plan = range
            .plan_batch(2, vec![value(10)], BudgetLane::Ordinary, usize::MAX)
            .unwrap();
        assert!(
            plan.build_in_with(source, |_| panic!("invalid lane reached copier"))
                .is_err()
        );
        assert_eq!(source.stats().used, 0);
        assert_eq!(budget.stats(), baseline);
    }
}

#[test]
fn published_funded_pages_keep_pool_until_last_pinned_root_releases_them() {
    let budget = MemoryBudget::new(2_000_000, 500_000).unwrap();
    let mut range = seeded(&budget);
    let plan = range
        .plan_batch(2, vec![value(10)], BudgetLane::Completion, usize::MAX)
        .unwrap();
    let before_pool = budget.stats().used;
    let pool = budget
        .funded_child(
            BudgetLane::Completion,
            plan.charges().additional_peak_bytes(),
        )
        .unwrap();
    let pool_hold = budget.stats().used - before_pool;
    let candidate = plan.build_in(&pool).unwrap();
    range.publish(candidate).unwrap();
    let lease = range.pin(0, 10).unwrap();
    drop(pool);
    // A new normal-budget root replaces all rows, leaving the old funded pages
    // reachable only through the owner-held snapshot pin.
    range
        .apply_batch(
            3,
            (1..=4).map(|key| value(key * 10)).collect(),
            BudgetLane::Ordinary,
        )
        .unwrap();
    assert_eq!(
        lease
            .project_next(&0, false, &u64::MAX, 0, |entry| (
                entry.key,
                entry.value.len()
            ))
            .unwrap(),
        Some((10, 1024))
    );
    let pinned = budget.stats().used;
    range.release(&lease).unwrap();
    assert!(pinned - budget.stats().used >= pool_hold);
    assert!(matches!(
        lease.project_next(&0, false, &u64::MAX, 0, |entry| entry.key),
        Err(MemoryError::LeaseExpired)
    ));
    drop(range);
    // The expired handle keeps only its weak control metadata charged; the
    // funded root/pages and their complete pool backing have already gone.
    let expired = budget.stats();
    let metadata = expired.by_kind[BudgetKind::ReadPins as usize];
    assert!(metadata > 0);
    assert_eq!(expired.used, metadata);
    assert_eq!(expired.by_kind[BudgetKind::Reserved as usize], 0);
    drop(lease);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn failed_directory_reservation_keeps_input_charge_until_payload_destructor_runs() {
    use std::cell::Cell;
    struct Input<'a> {
        budget: &'a MemoryBudget,
        observed: &'a Cell<usize>,
        bytes: Vec<u8>,
    }
    impl Drop for Input<'_> {
        fn drop(&mut self) {
            // The Vec's own destructor runs after this observation; its charge
            // must still be held even though no destination page was allocated.
            self.observed
                .set(self.budget.stats().by_kind[BudgetKind::Pending as usize]);
        }
    }
    let budget = MemoryBudget::new(2_000_000, 500_000).unwrap();
    let observed = Cell::new(0);
    let range = RangeStore::new(RangeId(503), 0, config(), budget.clone()).unwrap();
    let input = Input {
        budget: &budget,
        observed: &observed,
        bytes: vec![17; 256],
    };
    let heap = input.bytes.capacity() + ALLOCATOR_OVERHEAD;
    let plan = range
        .plan_batch(
            1,
            vec![Change::Put(Entry::new(10u64, input, heap))],
            BudgetLane::Completion,
            usize::MAX,
        )
        .unwrap();
    let input_charge = plan.charges().input_pending_bytes();
    let pool = budget
        .funded_child(BudgetLane::Completion, input_charge)
        .unwrap();
    assert!(matches!(
        plan.build_in_with(&pool, |_| panic!("failed directory reached copier")),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(observed.get(), input_charge);
    assert_eq!(pool.stats().used, 0);
    assert_eq!(budget.stats().by_kind[BudgetKind::Pending as usize], 0);
    assert!(range.is_empty());
}
