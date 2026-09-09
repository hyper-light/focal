use crate::*;

fn parent() -> MemoryBudget {
    MemoryBudget::new(16 * 1024 * 1024, 1024 * 1024).unwrap()
}

fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 2,
        page_bytes: 8192,
        max_entry_bytes: 128 * 1024,
        max_batch_entries: 64,
        ..RangeConfig::default()
    }
}

fn put(key: u64, fill: u8) -> Change<u64, Vec<u8>> {
    let value = vec![fill; 1024];
    let heap = value.capacity() + ALLOCATOR_OVERHEAD;
    Change::Put(Entry::new(key, value, heap))
}

fn replacements(fill: u8) -> Vec<Change<u64, Vec<u8>>> {
    (1..=4).map(|key| put(key * 10, fill)).collect()
}

fn seeded(source: &MemoryBudget) -> RangeStore<u64, Vec<u8>> {
    let mut range = RangeStore::new(RangeId(990), 0, config(), source.clone()).unwrap();
    range
        .apply_batch(1, replacements(1), BudgetLane::Ordinary)
        .unwrap();
    range
}

fn pressure(parent: &MemoryBudget) -> Reservation {
    parent
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            parent.limit() - parent.stats().used,
        )
        .unwrap()
}

#[test]
fn elastic_range_envelope_stays_fixed_across_growth_trim_and_full_parent_preparation() {
    let parent = parent();
    let ceiling = 2 * 1024 * 1024;
    let mut pool = parent
        .elastic_funded_child(BudgetLane::Ordinary, ceiling, 32 * 1024)
        .unwrap();
    // The range itself belongs to the elastic source. Its envelope must use the
    // immutable ceiling, rather than today's smaller amount of funded credit.
    let mut range = seeded(pool.budget());
    let limits = RangeWriteLimits {
        changed_keys: 1,
        deleted_keys: 0,
        deleted_heap: 0,
        incoming_heap: 1024 + ALLOCATOR_OVERHEAD,
        input_capacity: 1,
    };
    let envelope = range.future_write_envelope(limits).unwrap();
    pool.grow(envelope.additional_peak_bytes()).unwrap();
    assert_eq!(range.future_write_envelope(limits).unwrap(), envelope);
    let before_trim = parent.stats().used;
    pool.trim_unused(4096).unwrap();
    assert_eq!(parent.stats().used, before_trim - 4096);
    assert_eq!(pool.budget().limit(), ceiling);
    assert_eq!(range.future_write_envelope(limits).unwrap(), envelope);
    pool.grow(4096).unwrap();
    assert_eq!(range.future_write_envelope(limits).unwrap(), envelope);

    let plan = range
        .plan_batch(
            2,
            vec![put(10, 9)],
            BudgetLane::Completion,
            envelope.additional_peak_bytes(),
        )
        .unwrap();
    envelope.check_plan(&plan).unwrap();
    let pressure = pressure(&parent);
    let full = parent.stats();
    assert!(
        parent
            .reserve(BudgetKind::Pages, BudgetLane::Completion, 1)
            .is_err()
    );
    let mut copied = 0;
    let prepared = plan
        .build_in_with(pool.budget(), |old| {
            copied += 1;
            assert_eq!(parent.stats().used, full.used);
            assert_eq!(parent.stats().ordinary_used, full.ordinary_used);
            Ok(old.clone())
        })
        .unwrap();
    assert_eq!(copied, 1);
    assert_eq!(range.get(&10).unwrap().first(), Some(&1));
    range.publish(prepared).unwrap();
    assert_eq!(range.get(&10).unwrap().first(), Some(&9));
    assert_eq!(range.future_write_envelope(limits).unwrap(), envelope);
    assert_eq!(parent.stats().used, full.used);
    assert_eq!(parent.stats().ordinary_used, full.ordinary_used);
    drop(pressure);
    drop(range);
    assert_eq!(pool.budget().stats().used, 0);
    assert_eq!(pool.available(), pool.funded_capacity());
    drop(pool);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn trim_preserves_prepared_and_pinned_pages_until_final_source_release_in_either_drop_order() {
    for controller_first in [false, true] {
        let parent = parent();
        let mut range = seeded(&parent);
        let first = range
            .plan_batch(2, replacements(2), BudgetLane::Completion, usize::MAX)
            .unwrap();
        let initial = first.charges().additional_peak_bytes();
        let mut pool = parent
            .elastic_funded_child(BudgetLane::Ordinary, 4 * 1024 * 1024, initial)
            .unwrap();
        let source = pool.budget().clone();
        let first = first.build_in(&source).unwrap();
        let live_first = source.stats().used;
        assert!(live_first > 0);
        pool.trim_unused(pool.available()).unwrap();
        assert_eq!(pool.funded_capacity(), live_first);
        assert_eq!(pool.available(), 0);
        let before = parent.stats();
        assert!(matches!(
            pool.trim_unused(1),
            Err(MemoryError::Capacity { .. })
        ));
        assert_eq!(parent.stats(), before);
        assert_eq!(first.get(&10).unwrap().first(), Some(&2));

        let second = range
            .plan_after(
                &first,
                3,
                replacements(3),
                BudgetLane::Completion,
                usize::MAX,
            )
            .unwrap();
        pool.grow(second.charges().additional_peak_bytes()).unwrap();
        let second = second.build_in(&source).unwrap();
        pool.trim_unused(pool.available()).unwrap();
        assert_eq!(pool.funded_capacity(), source.stats().used);
        assert!(pool.trim_unused(1).is_err());
        let jointly_held = pool.funded_capacity();
        drop(second);
        assert!(pool.available() > 0);
        pool.trim_unused(pool.available()).unwrap();
        assert_eq!(pool.funded_capacity(), live_first);
        assert!(pool.funded_capacity() < jointly_held);

        range.publish(first).unwrap();
        let pinned = range.pin(0, 10).unwrap();
        // All new live rows now use the ordinary parent source. Only the old
        // committed pin retains the elastic pages and their directory/root.
        range
            .apply_batch(3, replacements(4), BudgetLane::Ordinary)
            .unwrap();
        assert!(source.stats().used > 0);
        assert_eq!(pool.available(), 0);
        assert!(pool.trim_unused(1).is_err());
        let before_drop = parent.stats().used;
        if controller_first {
            drop(pool);
            assert_eq!(parent.stats().used, before_drop);
            drop(source);
        } else {
            drop(source);
            assert_eq!(parent.stats().used, before_drop);
            drop(pool);
        }
        assert_eq!(parent.stats().used, before_drop);
        assert_eq!(
            pinned
                .project_next(&0, false, &100, 1, |entry| (entry.key, entry.value[0]))
                .unwrap(),
            Some((10, 2))
        );
        range.release(&pinned).unwrap();
        assert!(parent.stats().used < before_drop);
        assert_eq!(parent.stats().by_kind[BudgetKind::Reserved as usize], 0);
        drop(range);
        let after = parent.stats();
        // An expired lease still owns its weak control metadata, not its pages.
        assert!(after.used > 0);
        assert_eq!(after.used, after.by_kind[BudgetKind::ReadPins as usize]);
        drop(pinned);
        assert_eq!(parent.stats().used, 0);
    }
}

#[test]
fn failed_elastic_range_copy_restores_credit_that_can_be_trimmed_regrown_and_retried() {
    let parent = parent();
    let range = seeded(&parent);
    let plan = range
        .plan_batch(2, vec![put(10, 9)], BudgetLane::Completion, usize::MAX)
        .unwrap();
    let capacity = plan.charges().additional_peak_bytes();
    let mut pool = parent
        .elastic_funded_child(BudgetLane::Ordinary, 2 * 1024 * 1024, 0)
        .unwrap();
    pool.grow(capacity).unwrap();
    let pressure = pressure(&parent);
    let full = parent.stats();
    let unused = pool.budget().stats();
    let mut copied = 0;
    assert!(matches!(
        plan.build_in_with(pool.budget(), |_| {
            copied += 1;
            Err(MemoryError::AllocationFailed)
        }),
        Err(MemoryError::AllocationFailed)
    ));
    assert_eq!(
        copied, 1,
        "failure must happen after page construction starts"
    );
    assert_eq!(pool.budget().stats(), unused);
    assert_eq!(pool.available(), capacity);
    assert_eq!(parent.stats(), full);
    assert_eq!(range.prefix(), 1);
    assert_eq!(range.get(&10).unwrap().first(), Some(&1));

    pool.trim_unused(capacity).unwrap();
    assert_eq!(pool.funded_capacity(), 0);
    assert_eq!(parent.stats().used, full.used - capacity);
    pool.grow(capacity).unwrap();
    assert_eq!(parent.stats(), full);
    let retry = range
        .plan_batch(2, vec![put(10, 9)], BudgetLane::Completion, usize::MAX)
        .unwrap()
        .build_in(pool.budget())
        .unwrap();
    assert_eq!(retry.get(&10).unwrap().first(), Some(&9));
    drop(retry);
    assert_eq!(pool.available(), capacity);
    assert_eq!(parent.stats(), full);
    drop(pressure);
    drop(range);
    drop(pool);
    assert_eq!(parent.stats().used, 0);
}
