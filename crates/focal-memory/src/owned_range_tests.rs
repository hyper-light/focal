use crate::{
    ALLOCATOR_OVERHEAD, BudgetKind, BudgetLane, Change, Entry, MemoryBudget, MemoryError,
    PreparedRange, RangeConfig, RangeId, RangeStore, SnapshotLease,
};

const ORDINARY: BudgetLane = BudgetLane::Ordinary;
const PAYLOAD_BYTES: usize = 4096;

/// Deliberately not Clone: these tests must exercise the fallible owner copier.
#[derive(Debug, PartialEq, Eq)]
struct Row {
    label: u64,
    payload: Vec<u8>,
}

fn row(label: u64) -> Row {
    Row {
        label,
        payload: vec![17; PAYLOAD_BYTES],
    }
}
fn entry(key: u64, label: u64) -> Entry<u64, Row> {
    let value = row(label);
    let bytes = value.payload.capacity() + ALLOCATOR_OVERHEAD;
    Entry::new(key, value, bytes)
}
fn copy(row: &Row) -> Result<Row, MemoryError> {
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(row.payload.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    payload.extend_from_slice(&row.payload);
    Ok(Row {
        label: row.label,
        payload,
    })
}
fn no_copy(_: &Row) -> Result<Row, MemoryError> {
    panic!("the owned preparation unexpectedly copied a value")
}
fn store(budget: &MemoryBudget) -> RangeStore<u64, Row> {
    RangeStore::new(
        RangeId(1),
        0,
        RangeConfig {
            page_entries: 4,
            max_batch_entries: 64,
            ..RangeConfig::default()
        },
        budget.clone(),
    )
    .unwrap()
}
fn seeded(budget: &MemoryBudget, count: u64) -> RangeStore<u64, Row> {
    let mut range = store(budget);
    let changes = (1..=count)
        .map(|n| Change::Put(entry(n * 10, n * 10)))
        .collect();
    let prepared = range
        .prepare_batch_with(1, changes, ORDINARY, no_copy)
        .unwrap();
    range.publish(prepared).unwrap();
    range
}
fn values(range: &RangeStore<u64, Row>) -> Vec<(u64, u64)> {
    range
        .entries()
        .map(|entry| (entry.key, entry.value.label))
        .collect()
}
fn candidate_values(range: &PreparedRange<u64, Row>) -> Vec<(u64, u64)> {
    range
        .entries()
        .map(|entry| (entry.key, entry.value.label))
        .collect()
}
fn pinned_values(lease: &SnapshotLease<u64, Row>) -> Vec<(u64, u64)> {
    let mut values = Vec::new();
    let mut start = 0;
    while let Some((key, label)) = lease
        .project_next(&start, false, &u64::MAX, 0, |entry| {
            (entry.key, entry.value.label)
        })
        .unwrap()
    {
        values.push((key, label));
        start = key + 1;
    }
    values
}

#[test]
fn nonclone_incoming_rows_move_without_copier_calls_and_empty_batches_share_pages() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut range = store(&budget);
    let incoming = entry(10, 10);
    let payload = incoming.value.payload.as_ptr();
    let baseline = budget.stats();
    let prepared = range
        .prepare_batch_with(1, vec![Change::Put(incoming)], ORDINARY, no_copy)
        .unwrap();
    assert_eq!(prepared.get(&10).unwrap().payload.as_ptr(), payload);
    assert_eq!(candidate_values(&prepared), vec![(10, 10)]);
    assert!(range.is_empty());
    assert_eq!(range.prefix(), 0);
    range.publish(prepared).unwrap();
    assert_eq!(range.get(&10).unwrap().payload.as_ptr(), payload);
    let published = budget.stats();
    let empty = range
        .prepare_batch_with(2, vec![], ORDINARY, no_copy)
        .unwrap();
    assert_eq!(candidate_values(&empty), vec![(10, 10)]);
    assert_eq!(empty.get(&10).unwrap().payload.as_ptr(), payload);
    assert!(budget.stats().used > published.used);
    drop(empty);
    assert_eq!(budget.stats(), published);
    assert!(published.used > baseline.used);
    drop(range);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn only_retained_values_on_touched_pages_copy_after_the_page_charge() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let mut range = seeded(&budget, 12);
    assert_eq!(range.stats().pages, 3);
    let old = values(&range);
    let lease = range.pin(0, 10).unwrap();
    let replacement = entry(20, 200);
    let replacement_pointer = replacement.value.payload.as_ptr();
    let inserted = entry(25, 250);
    let inserted_pointer = inserted.value.payload.as_ptr();
    let untouched_pointer = range.get(&80).unwrap().payload.as_ptr();
    let page_charge = budget.stats().by_kind[BudgetKind::Pages as usize];
    let minimum_new_page =
        4 * (std::mem::size_of::<Entry<u64, Row>>() + PAYLOAD_BYTES + ALLOCATOR_OVERHEAD);
    let mut copied = Vec::new();
    let prepared = range
        .prepare_batch_with(
            2,
            vec![
                Change::Put(inserted),
                Change::Delete(30),
                Change::Put(replacement),
            ],
            ORDINARY,
            |old| {
                assert!(
                    budget.stats().by_kind[BudgetKind::Pages as usize]
                        >= page_charge + minimum_new_page
                );
                copied.push(old.label);
                copy(old)
            },
        )
        .unwrap();
    assert_eq!(copied, vec![10, 40]);
    assert_eq!(
        prepared.get(&20).unwrap().payload.as_ptr(),
        replacement_pointer
    );
    assert_eq!(
        prepared.get(&25).unwrap().payload.as_ptr(),
        inserted_pointer
    );
    assert_eq!(
        prepared.get(&80).unwrap().payload.as_ptr(),
        untouched_pointer
    );
    assert!(prepared.get(&30).is_none());
    assert_eq!(values(&range), old);
    assert_eq!(pinned_values(&lease), old);
    range.publish(prepared).unwrap();
    assert_eq!(range.prefix(), 2);
    assert_eq!(range.get(&20).unwrap().label, 200);
    assert_eq!(pinned_values(&lease), old);
    assert_eq!(lease.prefix(), 1);
    range.release(&lease).unwrap();
    assert_eq!(
        lease.project_next(&0, false, &u64::MAX, 0, |entry| entry.key),
        Err(MemoryError::LeaseExpired)
    );
    drop(lease);
    drop(range);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn fallible_copy_after_completed_pages_rolls_back_every_provisional_charge() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let mut range = seeded(&budget, 12);
    let lease = range.pin(0, 10).unwrap();
    let original = values(&range);
    let original_stats = range.stats();
    let baseline = budget.stats();
    let mut copied = Vec::new();
    let rejected = range.prepare_batch_with(
        2,
        vec![Change::Put(entry(20, 200)), Change::Put(entry(100, 1000))],
        ORDINARY,
        |old| {
            copied.push(old.label);
            if old.label == 110 {
                return Err(MemoryError::AllocationFailed);
            }
            copy(old)
        },
    );
    assert!(matches!(rejected, Err(MemoryError::AllocationFailed)));
    assert_eq!(copied, vec![10, 30, 40, 90, 110]);
    assert_eq!(range.stats(), original_stats);
    assert_eq!(values(&range), original);
    assert_eq!(pinned_values(&lease), original);
    assert_eq!(budget.stats(), baseline);
    // Failure did not poison this exact base or consume its next prefix.
    let accepted = range
        .prepare_batch_with(
            2,
            vec![Change::Put(entry(20, 200)), Change::Put(entry(100, 1000))],
            ORDINARY,
            copy,
        )
        .unwrap();
    range.publish(accepted).unwrap();
    assert_eq!(range.get(&100).unwrap().label, 1000);
    assert_eq!(pinned_values(&lease), original);
    assert_eq!(range.advance_clock(10).unwrap(), 1);
    assert_eq!(
        lease.project_next(&0, false, &u64::MAX, 10, |entry| entry.key),
        Err(MemoryError::LeaseExpired)
    );
    drop(lease);
    drop(range);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn deleting_complete_pages_consumes_owned_changes_without_copying_any_value() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut range = seeded(&budget, 8);
    let untouched = range.get(&80).unwrap().payload.as_ptr();
    let first = range
        .prepare_batch_with(
            2,
            (1..=4).map(|key| Change::Delete(key * 10)).collect(),
            ORDINARY,
            no_copy,
        )
        .unwrap();
    assert_eq!(
        candidate_values(&first),
        vec![(50, 50), (60, 60), (70, 70), (80, 80)]
    );
    assert_eq!(first.get(&80).unwrap().payload.as_ptr(), untouched);
    range.publish(first).unwrap();
    assert_eq!(range.stats().pages, 1);
    let last = range
        .prepare_batch_with(
            3,
            (5..=8).map(|key| Change::Delete(key * 10)).collect(),
            ORDINARY,
            no_copy,
        )
        .unwrap();
    assert!(last.is_empty());
    assert_eq!(last.entries().count(), 0);
    range.publish(last).unwrap();
    assert_eq!(range.stats().pages, 0);
    assert!(range.is_empty());
    drop(range);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn owned_suffix_iteration_tracks_pending_rows_and_publication_checks_exact_ancestry() {
    let budget = MemoryBudget::new(2_000_000, 0).unwrap();
    let mut range = store(&budget);
    let first = range
        .prepare_batch_with(
            1,
            vec![Change::Put(entry(2, 20)), Change::Put(entry(1, 10))],
            ORDINARY,
            no_copy,
        )
        .unwrap();
    let second = range
        .prepare_after_with(
            &first,
            2,
            vec![
                Change::Delete(1),
                Change::Put(entry(2, 200)),
                Change::Put(entry(3, 300)),
            ],
            ORDINARY,
            no_copy,
        )
        .unwrap();
    let mut copied = Vec::new();
    let third = range
        .prepare_after_with(
            &second,
            3,
            vec![Change::Put(entry(4, 400))],
            ORDINARY,
            |old| {
                copied.push(old.label);
                copy(old)
            },
        )
        .unwrap();
    assert_eq!(copied, vec![200, 300]);
    assert_eq!(candidate_values(&first), vec![(1, 10), (2, 20)]);
    assert_eq!(candidate_values(&second), vec![(2, 200), (3, 300)]);
    assert_eq!(candidate_values(&third), vec![(2, 200), (3, 300), (4, 400)]);
    assert_eq!(
        (
            first.base_prefix(),
            second.base_prefix(),
            third.base_prefix()
        ),
        (0, 1, 2)
    );
    assert!(range.is_empty());
    range.validate_chain([&first, &second, &third]).unwrap();
    assert!(range.validate_chain([&first, &third]).is_err());
    let out_of_order = range
        .prepare_after_with(&first, 2, vec![], ORDINARY, no_copy)
        .unwrap();
    assert_eq!(
        range.publish(out_of_order),
        Err(MemoryError::StalePreparation {
            prepared_at: 1,
            current_prefix: 0
        })
    );
    let foreign = store(&budget); // Same RangeId, different owner incarnation.
    let wrong = foreign
        .prepare_batch_with(1, vec![], ORDINARY, no_copy)
        .unwrap();
    assert!(matches!(
        range.prepare_after_with(&wrong, 2, vec![], ORDINARY, no_copy),
        Err(MemoryError::WrongRange)
    ));
    assert_eq!(range.publish(wrong), Err(MemoryError::WrongRange));
    let stale = range
        .prepare_batch_with(1, vec![Change::Put(entry(9, 900))], ORDINARY, no_copy)
        .unwrap();
    range.publish(first).unwrap();
    assert_eq!(
        range.publish(stale),
        Err(MemoryError::StalePreparation {
            prepared_at: 0,
            current_prefix: 1
        })
    );
    assert_eq!(values(&range), vec![(1, 10), (2, 20)]);
    range.publish(second).unwrap();
    range.publish(third).unwrap();
    assert_eq!(range.prefix(), 3);
    assert_eq!(values(&range), vec![(2, 200), (3, 300), (4, 400)]);
    drop(foreign);
    drop(range);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn real_lane_exhaustion_refuses_before_copy_and_reserved_completion_can_publish() {
    let budget = MemoryBudget::new(256 * 1024, 64 * 1024).unwrap();
    let mut range = seeded(&budget, 4);
    let lease = range.pin(0, 10).unwrap();
    let original = pinned_values(&lease);
    let stats = budget.stats();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            ORDINARY,
            stats.limit - stats.completion_reserve - stats.ordinary_used,
        )
        .unwrap();
    let exhausted_ordinary = budget.stats();
    assert!(matches!(
        range.prepare_batch_with(2, vec![Change::Put(entry(20, 200))], ORDINARY, no_copy),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(budget.stats(), exhausted_ordinary);
    let stats = budget.stats();
    let completion_pressure = budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            stats.limit - stats.used,
        )
        .unwrap();
    assert!(matches!(
        range.prepare_batch_with(
            2,
            vec![Change::Put(entry(20, 200))],
            BudgetLane::Completion,
            no_copy
        ),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(budget.stats().used, budget.stats().limit);
    drop(completion_pressure);
    assert_eq!(budget.stats(), exhausted_ordinary);
    let mut copied = Vec::new();
    let prepared = range
        .prepare_batch_with(
            2,
            vec![Change::Put(entry(20, 200))],
            BudgetLane::Completion,
            |old| {
                copied.push(old.label);
                copy(old)
            },
        )
        .unwrap();
    assert_eq!(copied, vec![10, 30, 40]);
    let stats = budget.stats();
    let final_pressure = budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            stats.limit - stats.used,
        )
        .unwrap();
    assert_eq!(budget.stats().used, budget.stats().limit);
    range.publish(prepared).unwrap(); // No allocation after the durable decision.
    assert_eq!(range.get(&20).unwrap().label, 200);
    assert_eq!(pinned_values(&lease), original);
    drop(final_pressure);
    drop(pressure);
    range.release(&lease).unwrap();
    drop(lease);
    drop(range);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn invalid_owned_input_is_rejected_before_copy_without_advancing_any_state() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let range = seeded(&budget, 4);
    let before = budget.stats();
    let original = values(&range);
    for changes in [
        vec![Change::Put(entry(20, 200)), Change::Delete(20)],
        vec![Change::Delete(999)],
        vec![Change::Put(Entry::new(20, row(200), usize::MAX))],
    ] {
        assert!(
            range
                .prepare_batch_with(2, changes, ORDINARY, no_copy)
                .is_err()
        );
        assert_eq!(budget.stats(), before);
        assert_eq!(range.prefix(), 1);
        assert_eq!(values(&range), original);
    }
    assert!(matches!(
        range.prepare_batch_with(3, vec![], ORDINARY, no_copy),
        Err(MemoryError::PrefixMismatch {
            expected: 2,
            actual: 3
        })
    ));
    assert_eq!(budget.stats(), before);
    drop(range);
    assert_eq!(budget.stats().used, 0);
}
