use super::*;

#[derive(Debug, PartialEq, Eq)]
struct Value(u64);

fn change(key: u64) -> Vec<Change<u64, Value>> {
    vec![Change::Put(Entry::new(key, Value(key * 10), 0))]
}
fn store(budget: &MemoryBudget, id: u128) -> RangeStore<u64, Value> {
    RangeStore::new(RangeId(id), 0, RangeConfig::default(), budget.clone()).unwrap()
}
fn copy(value: &Value) -> Result<Value, MemoryError> {
    Ok(Value(value.0))
}

#[test]
fn exact_pending_successor_checks_need_no_credit_or_new_handles() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut range = store(&budget, 901);
    let first = range
        .prepare_batch_with(1, change(1), BudgetLane::Ordinary, copy)
        .unwrap();
    let second = range
        .prepare_after_with(&first, 2, change(2), BudgetLane::Ordinary, copy)
        .unwrap();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            budget.limit() - budget.stats().used,
        )
        .unwrap();
    let charged = budget.stats();
    let handles = (
        Arc::strong_count(&first.root),
        Arc::strong_count(&second.root),
        Arc::strong_count(&range.root),
    );
    for _ in 0..64 {
        first.validate_successor(&second).unwrap();
        range.validate_chain([&first, &second]).unwrap();
    }
    assert_eq!(budget.stats(), charged);
    assert_eq!(
        (
            Arc::strong_count(&first.root),
            Arc::strong_count(&second.root),
            Arc::strong_count(&range.root),
        ),
        handles
    );
    assert_eq!(range.prefix(), 0);
    assert!(range.get(&1).is_none());
    assert_eq!(second.get(&1), Some(&Value(10)));
    assert_eq!(second.get(&2), Some(&Value(20)));
    range.publish(first).unwrap();
    range.validate_chain(std::iter::once(&second)).unwrap();
    range.publish(second).unwrap();
    drop(pressure);
    drop(range);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn equal_prefix_and_contents_cannot_substitute_a_sibling_branch() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let range = store(&budget, 902);
    let first = range
        .prepare_batch_with(1, change(1), BudgetLane::Ordinary, copy)
        .unwrap();
    let sibling = range
        .prepare_batch_with(1, change(1), BudgetLane::Ordinary, copy)
        .unwrap();
    let next = range
        .prepare_after_with(&first, 2, change(2), BudgetLane::Ordinary, copy)
        .unwrap();
    let sibling_next = range
        .prepare_after_with(&sibling, 2, change(2), BudgetLane::Ordinary, copy)
        .unwrap();
    assert_eq!(first.prefix(), sibling.prefix());
    assert_eq!(first.get(&1), sibling.get(&1));
    assert_eq!(next.prefix(), sibling_next.prefix());
    assert_eq!(next.get(&2), sibling_next.get(&2));
    let charged = budget.stats();
    first.validate_successor(&next).unwrap();
    sibling.validate_successor(&sibling_next).unwrap();
    for wrong in [&first, &sibling, &sibling_next] {
        assert!(matches!(
            first.validate_successor(wrong),
            Err(MemoryError::WrongRange)
        ));
    }
    assert!(matches!(
        range.validate_chain([&sibling, &next]),
        Err(MemoryError::WrongRange)
    ));
    assert_eq!(budget.stats(), charged);
    assert_eq!(range.prefix(), 0);
    drop((next, sibling_next, first, sibling, range));
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn other_owner_is_refused_even_when_range_id_and_rows_match() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let range = store(&budget, 903);
    let first = range
        .prepare_batch_with(1, change(1), BudgetLane::Ordinary, copy)
        .unwrap();
    for id in [903, 904] {
        let other = store(&budget, id);
        let other_first = other
            .prepare_batch_with(1, change(1), BudgetLane::Ordinary, copy)
            .unwrap();
        let other_next = other
            .prepare_after_with(&other_first, 2, change(2), BudgetLane::Ordinary, copy)
            .unwrap();
        assert_eq!(first.prefix(), other_first.prefix());
        assert_eq!(first.get(&1), other_first.get(&1));
        let charged = budget.stats();
        other_first.validate_successor(&other_next).unwrap();
        assert!(matches!(
            first.validate_successor(&other_next),
            Err(MemoryError::WrongRange)
        ));
        assert!(matches!(
            range.validate_chain(std::iter::once(&other_first)),
            Err(MemoryError::WrongRange)
        ));
        assert_eq!(budget.stats(), charged);
    }
    drop((first, range));
    assert_eq!(budget.stats().used, 0);
}
