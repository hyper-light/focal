use super::*;
use crate::native::NativeError;

fn at(offset: u32) -> PayloadRef {
    PayloadRef::at(0, offset, &offset.to_le_bytes()).unwrap()
}

fn budget() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 0).unwrap()
}

fn charge(budget: &MemoryBudget, bytes: usize) -> Allocation {
    budget
        .reserve(BudgetKind::Query, LANE, bytes)
        .unwrap()
        .commit()
}

#[test]
fn a_row_read_back_is_found_again_until_the_state_changes_and_its_charge_goes_with_it() {
    let budget = budget();
    let mut reads = ReadBack::new(NativeLimits::default());
    let before = budget.stats().used;
    assert!(reads.find(at(7)).is_none());
    let held = reads
        .hold(&budget, at(7), Row::Index, charge(&budget, 1000))
        .unwrap();
    assert!(matches!(held, Row::Index));
    assert!(matches!(reads.find(at(7)), Some(Row::Index)));
    assert!(reads.find(at(8)).is_none());
    assert_eq!(reads.rows(), 1);
    // The row's charge and the first table's, held together.
    assert!(reads.bytes() >= 1000);
    assert_eq!(budget.stats().used, before + reads.bytes());
    reads.release();
    assert!(reads.find(at(7)).is_none());
    assert_eq!((reads.rows(), reads.bytes()), (0, 0));
    assert_eq!(budget.stats().used, before);
}

#[test]
fn rows_read_back_past_a_table_go_in_the_next_and_every_one_is_found() {
    let budget = budget();
    let mut reads = ReadBack::new(NativeLimits::default());
    for offset in 0..5000 {
        reads
            .hold(&budget, at(offset), Row::Index, charge(&budget, 8))
            .unwrap();
    }
    assert_eq!(reads.rows(), 5000);
    for offset in 0..5000 {
        assert!(reads.find(at(offset)).is_some(), "offset {offset}");
    }
    assert!(reads.find(at(5000)).is_none());
    reads.release();
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn a_row_past_what_reads_may_hold_is_refused_for_capacity_and_nothing_is_kept_for_it() {
    let budget = MemoryBudget::new(2 * READ_BACK_BYTES, 0).unwrap();
    let reads = ReadBack::new(NativeLimits::default());
    reads
        .hold(
            &budget,
            at(1),
            Row::Index,
            charge(&budget, READ_BACK_BYTES - 64 * 1024),
        )
        .unwrap();
    let used = budget.stats().used;
    let refused = reads.hold(&budget, at(2), Row::Index, charge(&budget, 128 * 1024));
    assert!(matches!(
        refused,
        Err(NativeError::Memory(MemoryError::Capacity { .. }))
    ));
    // The refused row's charge went back with it.
    assert_eq!(budget.stats().used, used);
    assert!(reads.find(at(2)).is_none());
}

#[test]
fn the_first_failure_is_kept_until_it_is_asked_for_and_then_cleared() {
    let reads = ReadBack::new(NativeLimits::default());
    assert!(reads.failure().is_ok());
    reads.fail(NativeError::Capacity("first"));
    reads.fail(NativeError::Capacity("second"));
    assert!(matches!(
        reads.failure(),
        Err(NativeError::Capacity("first"))
    ));
    assert!(reads.failure().is_ok());
}

#[test]
fn rows_read_back_from_several_threads_at_once_are_each_held_and_found() {
    let budget = budget();
    let reads = ReadBack::new(NativeLimits::default());
    std::thread::scope(|scope| {
        for thread in 0..4u32 {
            let (reads, budget) = (&reads, &budget);
            scope.spawn(move || {
                for offset in 0..500 {
                    let offset = thread * 1000 + offset;
                    reads
                        .hold(budget, at(offset), Row::Index, charge(budget, 16))
                        .unwrap();
                }
            });
        }
    });
    assert_eq!(reads.rows(), 2000);
    for thread in 0..4u32 {
        for offset in 0..500 {
            assert!(reads.find(at(thread * 1000 + offset)).is_some());
        }
    }
}

#[test]
fn reads_within_the_floor_leave_the_budget_as_it_was_and_a_release_gives_the_floor_back() {
    let budget = budget();
    let mut reads = ReadBack::new(NativeLimits::default());
    reads.hold_floor(&budget);
    let with_floor = budget.stats().used;
    assert_eq!(with_floor, FLOOR);
    // A row and its table charged to the floor: the budget does not move.
    let charge = reads.fund(&budget, 1000).unwrap();
    reads.hold(&budget, at(1), Row::Index, charge).unwrap();
    assert_eq!(budget.stats().used, with_floor);
    // Past what is left of it, a read is charged to the budget.
    let charge = reads.fund(&budget, FLOOR).unwrap();
    reads.hold(&budget, at(2), Row::Index, charge).unwrap();
    assert_eq!(budget.stats().used, with_floor + FLOOR);
    // A release gives the floor back what reads took, and the budget the rest.
    reads.release();
    assert_eq!(budget.stats().used, with_floor);
    let charge = reads.fund(&budget, FLOOR).unwrap();
    assert_eq!(budget.stats().used, with_floor);
    drop(charge);
    drop(reads);
}
