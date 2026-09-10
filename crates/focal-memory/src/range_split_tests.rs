use super::*;

fn budget() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 1024 * 1024).unwrap()
}
fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 8,
        page_bytes: 4096,
        max_entry_bytes: 64 * 1024,
        max_batch_entries: 4096,
        ..RangeConfig::default()
    }
}
fn put(key: u64, bytes: usize) -> Change<u64, Vec<u8>> {
    let value = vec![key as u8; bytes];
    let heap = value.capacity() + usize::from(value.capacity() != 0) * ALLOCATOR_OVERHEAD;
    Change::Put(Entry::new(key, value, heap))
}
// The copier contract names the value type itself, `&V` with `V = Vec<u8>`.
#[allow(clippy::ptr_arg)]
fn copy(value: &Vec<u8>) -> Result<Vec<u8>, MemoryError> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(value.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    copied.extend_from_slice(value);
    Ok(copied)
}
fn filled(budget: &MemoryBudget, count: u64) -> RangeStore<u64, Vec<u8>> {
    let mut store = RangeStore::new(RangeId(700), 0, config(), budget.clone()).unwrap();
    let batch = u64::try_from(config().max_batch_entries).unwrap();
    let mut next = 0;
    while next < count {
        let end = (next + batch).min(count);
        store
            .apply_batch(
                store.prefix() + 1,
                (next..end).map(|i| put(i * 10, 16)).collect(),
                BudgetLane::Ordinary,
            )
            .unwrap();
        next = end;
    }
    store
}
fn keys(store: &RangeStore<u64, Vec<u8>>) -> Vec<u64> {
    store.entries().map(|entry| entry.key).collect()
}
fn page_handles(store: &RangeStore<u64, Vec<u8>>) -> Vec<usize> {
    store.root.pages.iter().map(Arc::strong_count).collect()
}

#[test]
fn a_split_shares_every_whole_page_and_copies_only_the_boundary_page() {
    let budget = budget();
    let store = filled(&budget, 200);
    let pages = store.root.pages.len();
    assert!(pages >= 20);
    let before = budget.stats().used;
    // Pages hold eight keys, so 1_230 (the 124th key) lies inside a page:
    // neither the first nor the last key of one.
    let at = 1_230;
    let (left, right) = store
        .split_with(&at, RangeId(701), RangeId(702), BudgetLane::Ordinary, copy)
        .unwrap();
    assert_eq!(left.id(), RangeId(701));
    assert_eq!(right.id(), RangeId(702));
    assert_eq!(left.prefix(), store.prefix());
    assert_eq!(right.prefix(), store.prefix());
    assert!(left.is_sibling(&right) && store.is_sibling(&left));
    let expected_left: Vec<u64> = (0..200).map(|i| i * 10).filter(|k| *k < at).collect();
    let expected_right: Vec<u64> = (0..200).map(|i| i * 10).filter(|k| *k >= at).collect();
    assert_eq!(keys(&left), expected_left);
    assert_eq!(keys(&right), expected_right);
    assert_eq!(left.len() + right.len(), store.len());
    assert!(left.root.pages.check_shape() && right.root.pages.check_shape());
    // Every page but the boundary page is shared with the source (two
    // handles); the boundary page keeps one handle and its two halves are
    // new pages.
    let handles = page_handles(&store);
    assert_eq!(handles.iter().filter(|count| **count == 1).count(), 1);
    assert_eq!(
        handles.iter().filter(|count| **count == 2).count(),
        pages - 1
    );
    assert_eq!(left.root.pages.len() + right.root.pages.len(), pages + 1);
    // The growth is two roots, two directories and the two half pages: far
    // below the source's retained bytes.
    let grown = budget.stats().used - before;
    assert!(grown < before / 4, "grew {grown} of {before}");
    // The source is untouched and still serves reads.
    assert_eq!(store.len(), 200);
    assert!(store.get(&at).is_some());
    // Both halves accept writes at the next prefix independently.
    let mut left = left;
    let mut right = right;
    let next = store.prefix() + 1;
    left.apply_batch(next, vec![put(5, 8)], BudgetLane::Ordinary)
        .unwrap();
    right
        .apply_batch(next, vec![Change::Delete(1_240)], BudgetLane::Ordinary)
        .unwrap();
    assert_eq!(left.get(&5).map(Vec::len), Some(8));
    assert!(right.get(&1_240).is_none());
    assert_eq!(left.prefix(), next);
    assert_eq!(right.prefix(), next);
}

#[test]
fn boundaries_at_page_edges_and_past_the_ends_copy_nothing() {
    let budget = budget();
    let store = filled(&budget, 64);
    let pages = store.root.pages.len();
    // The first key of the third page is a page boundary: no copies.
    let third_first = store
        .root
        .pages
        .get(2)
        .unwrap()
        .entries
        .first()
        .unwrap()
        .key;
    let before = budget.stats().used;
    let (left, right) = store
        .split_with(
            &third_first,
            RangeId(1),
            RangeId(2),
            BudgetLane::Ordinary,
            copy,
        )
        .unwrap();
    assert_eq!(left.root.pages.len(), 2);
    assert_eq!(right.root.pages.len(), pages - 2);
    assert_eq!(left.len() + right.len(), 64);
    assert!(page_handles(&store).iter().all(|count| *count == 2));
    let roots_and_directories = budget.stats().used - before;
    // Below the first key everything is right; past the last, everything left.
    let (empty, all) = store
        .split_with(&0, RangeId(3), RangeId(4), BudgetLane::Ordinary, copy)
        .unwrap();
    assert!(empty.is_empty() && empty.root.pages.is_empty());
    assert_eq!(all.len(), 64);
    let (all, empty) = store
        .split_with(
            &u64::MAX,
            RangeId(5),
            RangeId(6),
            BudgetLane::Ordinary,
            copy,
        )
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(all.len(), 64);
    // A key inside the last page of a one-page store.
    let small = {
        let mut small = store.new_sibling(RangeId(7)).unwrap();
        small
            .apply_batch(
                2,
                vec![put(1, 4), put(2, 4), put(3, 4)],
                BudgetLane::Ordinary,
            )
            .unwrap();
        small
    };
    let (l, r) = small
        .split_with(&2, RangeId(8), RangeId(9), BudgetLane::Ordinary, copy)
        .unwrap();
    assert_eq!((keys(&l), keys(&r)), (vec![1], vec![2, 3]));
    assert!(roots_and_directories > 0);
    // An empty store splits into two empty stores.
    let none = store.new_sibling(RangeId(10)).unwrap();
    let (l, r) = none
        .split_with(&5, RangeId(11), RangeId(12), BudgetLane::Ordinary, copy)
        .unwrap();
    assert!(l.is_empty() && r.is_empty());
}

#[test]
fn a_merge_shares_every_page_and_refuses_disorder_or_foreign_stores() {
    let budget = budget();
    let store = filled(&budget, 100);
    let (left, right) = store
        .split_with(&505, RangeId(1), RangeId(2), BudgetLane::Ordinary, copy)
        .unwrap();
    let before = budget.stats().used;
    let joined = left
        .merge_with(&right, RangeId(3), BudgetLane::Ordinary)
        .unwrap();
    assert_eq!(keys(&joined), keys(&store));
    assert_eq!(joined.len(), 100);
    assert_eq!(joined.prefix(), 1);
    assert!(joined.root.pages.check_shape());
    // Only a root and a directory: no page was copied.
    let grown = budget.stats().used - before;
    assert!(grown < 64 * 1024, "grew {grown}");
    assert!(page_handles(&left).iter().all(|count| *count >= 2));
    // The reverse order overlaps: refused.
    assert!(matches!(
        right
            .merge_with(&left, RangeId(4), BudgetLane::Ordinary)
            .map(|_| ()),
        Err(MemoryError::InvalidNeighbors)
    ));
    // A store from another group cannot join.
    let foreign = RangeStore::new(RangeId(5), 1, config(), budget.clone()).unwrap();
    assert!(matches!(
        left.merge_with(&foreign, RangeId(6), BudgetLane::Ordinary)
            .map(|_| ()),
        Err(MemoryError::WrongRange)
    ));
    // Siblings at different prefixes cannot join.
    let mut advanced = right.new_sibling(RangeId(7)).unwrap();
    advanced
        .apply_batch(2, vec![put(9_999, 4)], BudgetLane::Ordinary)
        .unwrap();
    assert!(matches!(
        left.merge_with(&advanced, RangeId(8), BudgetLane::Ordinary)
            .map(|_| ()),
        Err(MemoryError::PrefixMismatch { .. })
    ));
    // Merging with an empty sibling is the identity on rows.
    let empty = right.new_sibling(RangeId(9)).unwrap();
    let same = left
        .merge_with(&empty, RangeId(10), BudgetLane::Ordinary)
        .unwrap();
    assert_eq!(keys(&same), keys(&left));
}

#[test]
fn siblings_share_one_clock_and_one_envelope_owner() {
    let budget = budget();
    let mut store = filled(&budget, 40);
    let mut sibling = store.new_sibling(RangeId(2)).unwrap();
    assert!(store.is_sibling(&sibling));
    assert_eq!(sibling.prefix(), 1);
    store.advance_clock(50).unwrap();
    assert_eq!(sibling.stats().clock, 50);
    // A lease on one member expires by the shared clock advanced on the other.
    let lease = sibling.pin(50, 10).unwrap();
    store.advance_clock(61).unwrap();
    assert_eq!(sibling.advance_clock(61).unwrap(), 1);
    assert_eq!(
        sibling.release(&lease).unwrap_err(),
        MemoryError::LeaseExpired
    );
    // One shared envelope covers a write divided between the two members.
    let limits = RangeWriteLimits {
        changed_keys: 4,
        deleted_keys: 1,
        deleted_heap: 64 * 1024,
        incoming_heap: 4096,
        input_capacity: 4,
    };
    let envelope = store.future_write_envelope_shared(limits, 2).unwrap();
    assert_eq!(envelope.members(), 2);
    let single = store.future_write_envelope(limits).unwrap();
    assert!(envelope.directory_bytes() > single.directory_bytes());
    assert!(envelope.input_pending_bytes() > single.input_pending_bytes());
    assert_eq!(envelope.new_pages_bytes(), single.new_pages_bytes());
    assert_eq!(envelope.merge_pending_bytes(), single.merge_pending_bytes());
    let mut changes = vec![
        put(5, 100),
        put(15, 100),
        Change::Delete(20),
        put(10_000, 100),
    ];
    let right = changes.split_off(3);
    let left_plan = store
        .plan_batch(2, changes, BudgetLane::Ordinary, usize::MAX)
        .unwrap();
    let right_plan = sibling
        .plan_batch(2, right, BudgetLane::Ordinary, usize::MAX)
        .unwrap();
    envelope.check_plans([&left_plan, &right_plan]).unwrap();
    // Fewer fragments than members fit the envelope (a group can shrink);
    // more do not, and none is refused.
    envelope.check_plans([&left_plan]).unwrap();
    envelope.check_plan(&left_plan).unwrap();
    assert_eq!(
        envelope
            .check_plans(std::iter::empty::<&RangePreparationPlan<'_, u64, Vec<u8>>>())
            .unwrap_err(),
        MemoryError::InvalidConfiguration(
            "a range group publishes one fragment per member within its envelope"
        )
    );
    // The single envelope refuses a fragment pair.
    assert_eq!(
        single.check_plans([&left_plan, &right_plan]).unwrap_err(),
        MemoryError::InvalidConfiguration(
            "a range group publishes one fragment per member within its envelope"
        )
    );
    // A foreign store's plan is refused by owner.
    let foreign = RangeStore::new(RangeId(9), 1, config(), budget.clone()).unwrap();
    let foreign_plan = foreign
        .plan_batch(2, vec![put(1, 1)], BudgetLane::Ordinary, usize::MAX)
        .unwrap();
    assert_eq!(
        envelope
            .check_plans([&left_plan, &foreign_plan])
            .unwrap_err(),
        MemoryError::WrongRange
    );
    // Too many changed keys across the fragments.
    let wide = store
        .plan_batch(
            2,
            vec![put(1, 1), put(2, 1), put(3, 1), put(4, 1)],
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap();
    assert!(envelope.check_plans([&wide, &right_plan]).is_err());
    let built_left = left_plan.build_with(copy).unwrap();
    let built_right = right_plan.build_with(copy).unwrap();
    store.publish(built_left).unwrap();
    sibling.publish(built_right).unwrap();
    assert_eq!(store.prefix(), 2);
    assert_eq!(sibling.prefix(), 2);
    assert_eq!(sibling.get(&10_000).map(Vec::len), Some(100));
}

#[test]
fn a_bulk_built_directory_matches_the_incremental_shape_at_every_width() {
    let budget = budget();
    for count in [1u64, 8, 9, 40, 200, 700, 2_100, 9_000] {
        let store = filled(&budget, count);
        let (left, right) = store
            .split_with(&0, RangeId(1), RangeId(2), BudgetLane::Ordinary, copy)
            .unwrap();
        assert!(left.root.pages.is_empty());
        assert_eq!(right.root.pages.len(), store.root.pages.len());
        assert!(right.root.pages.check_shape(), "count {count}");
        assert_eq!(keys(&right), keys(&store));
        for probe in [0u64, 7, 400, count * 10 - 10, count * 10] {
            assert_eq!(right.get(&probe), store.get(&probe));
        }
        let (all, none) = store
            .split_with(
                &u64::MAX,
                RangeId(3),
                RangeId(4),
                BudgetLane::Ordinary,
                copy,
            )
            .unwrap();
        assert!(none.root.pages.is_empty());
        assert!(all.root.pages.check_shape());
        let (l, r) = store
            .split_with(
                &(count * 5),
                RangeId(5),
                RangeId(6),
                BudgetLane::Ordinary,
                copy,
            )
            .unwrap();
        assert!(l.root.pages.check_shape() && r.root.pages.check_shape());
        assert_eq!(l.len() + r.len(), store.len());
    }
}
