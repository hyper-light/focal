#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_memory::{
    BudgetKind, BudgetLane, Change, Entry, ImmutableContent, MemoryBudget, MemoryError,
    RangeConfig, RangeId, RangeStore, VersionedRecord,
};

const ORDINARY: BudgetLane = BudgetLane::Ordinary;

#[test]
fn pipelined_candidates_see_pending_state_and_publish_only_in_order() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut range = RangeStore::new(RangeId(1), 0, RangeConfig::default(), budget.clone()).unwrap();
    let first = range
        .prepare_batch(1, vec![Change::Put(Entry::new(1, 10, 0))], ORDINARY)
        .unwrap();
    let second = range
        .prepare_after(
            &first,
            2,
            vec![Change::Delete(1), Change::Put(Entry::new(2, 20, 0))],
            ORDINARY,
        )
        .unwrap();
    assert_eq!(range.get(&1), None);
    assert_eq!(second.get(&1), None);
    assert_eq!(second.get(&2), Some(&20));
    let out_of_order = range.prepare_after(&first, 2, vec![], ORDINARY).unwrap();
    assert!(matches!(
        range.publish(out_of_order),
        Err(MemoryError::StalePreparation { .. })
    ));
    range.publish(first).unwrap();
    assert_eq!(range.get(&1), Some(&10));
    range.publish(second).unwrap();
    assert_eq!(range.prefix(), 2);
    assert_eq!(range.get(&1), None);
    assert_eq!(range.get(&2), Some(&20));
    let foreign = RangeStore::new(RangeId(1), 0, RangeConfig::default(), budget.clone()).unwrap();
    let foreign_candidate = foreign
        .prepare_batch(1, vec![Change::Put(Entry::new(3, 30, 0))], ORDINARY)
        .unwrap();
    assert!(matches!(
        range.prepare_after(&foreign_candidate, 2, vec![], ORDINARY),
        Err(MemoryError::WrongRange)
    ));
}

#[test]
fn checkpoint_seed_is_bounded_and_rejects_noncanonical_order() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let config = RangeConfig {
        page_entries: 4,
        max_batch_entries: 2,
        ..RangeConfig::default()
    };
    let range = RangeStore::from_entries(
        RangeId(1),
        u64::MAX,
        config,
        budget.clone(),
        (0..100).map(|key| Entry::new(key, key * 2, 0)),
    )
    .unwrap();
    assert_eq!(range.prefix(), u64::MAX);
    assert_eq!(range.len(), 100);
    assert_eq!(range.get(&99), Some(&198));
    drop(range);
    assert_eq!(budget.stats().used, 0);
    assert!(
        RangeStore::from_entries(
            RangeId(1),
            22,
            config,
            budget.clone(),
            [Entry::new(2, 2, 0), Entry::new(1, 1, 0)]
        )
        .is_err()
    );
    assert!(
        RangeStore::from_entries(
            RangeId(1),
            22,
            config,
            budget.clone(),
            [Entry::new(1, 1, 0), Entry::new(1, 1, 0)]
        )
        .is_err()
    );
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn candidates_are_unpublished_rollback_on_drop_and_publish_without_allocation() {
    let budget = MemoryBudget::new(100_000, 0).unwrap();
    let mut range = RangeStore::new(RangeId(1), 0, RangeConfig::default(), budget.clone()).unwrap();
    let before = budget.stats();
    let candidate = range
        .prepare_batch(
            1,
            vec![Change::Put(Entry::new(1u64, 99u64, 1000))],
            ORDINARY,
        )
        .unwrap();
    assert_eq!(range.prefix(), 0);
    assert_eq!(range.get(&1), None);
    assert_eq!(candidate.base_prefix(), 0);
    assert_eq!(candidate.prefix(), 1);
    assert_eq!(candidate.get(&1), Some(&99));
    assert!(budget.stats().used > before.used);
    // Simulates a rejected proposal or lost authority before commitment.
    drop(candidate);
    assert_eq!(budget.stats(), before);
    let candidate = range
        .prepare_batch(1, vec![Change::Put(Entry::new(1, 99, 1000))], ORDINARY)
        .unwrap();
    let remaining = budget.stats().limit - budget.stats().used;
    let other_work = budget
        .reserve(BudgetKind::Pending, ORDINARY, remaining)
        .unwrap();
    assert_eq!(budget.stats().used, budget.stats().limit);
    // A durable decision cannot encounter a fresh allocation failure here.
    range.publish(candidate).unwrap();
    assert_eq!(range.prefix(), 1);
    assert_eq!(range.get(&1), Some(&99));
    drop(other_work);
    drop(range);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn candidates_bind_owner_incarnation_and_unchanged_base() {
    let budget = MemoryBudget::new(100_000, 0).unwrap();
    let mut first: RangeStore<u64, u64> =
        RangeStore::new(RangeId(1), 0, RangeConfig::default(), budget.clone()).unwrap();
    let mut other: RangeStore<u64, u64> =
        RangeStore::new(RangeId(2), 0, RangeConfig::default(), budget.clone()).unwrap();
    let candidate = first.prepare_batch(1, vec![], ORDINARY).unwrap();
    assert_eq!(other.publish(candidate), Err(MemoryError::WrongRange));
    let candidate = first.prepare_batch(1, vec![], ORDINARY).unwrap();
    // Even accidental reuse of the supplied incarnation cannot publish a
    // different owner's candidate just because both have the same prefix.
    let mut alias: RangeStore<u64, u64> =
        RangeStore::new(RangeId(1), 0, RangeConfig::default(), budget.clone()).unwrap();
    assert_eq!(alias.publish(candidate), Err(MemoryError::WrongRange));
    let one = first
        .prepare_batch(1, vec![Change::Put(Entry::new(1, 11, 0))], ORDINARY)
        .unwrap();
    let two = first
        .prepare_batch(1, vec![Change::Put(Entry::new(1, 22, 0))], ORDINARY)
        .unwrap();
    first.publish(one).unwrap();
    assert_eq!(
        first.publish(two),
        Err(MemoryError::StalePreparation {
            prepared_at: 0,
            current_prefix: 1
        })
    );
    assert_eq!(first.get(&1), Some(&11));
}

#[test]
fn immutable_authored_payload_is_charged_once_across_lifecycle_versions() {
    #[derive(Debug, PartialEq, Eq)]
    struct Authored(Vec<u8>); // Intentionally not Clone.

    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let content =
        ImmutableContent::new(Authored(vec![42; 10_000]), 10_000, &budget, ORDINARY).unwrap();
    let content_charge = content.allocated_bytes();
    let original = VersionedRecord {
        content,
        lifecycle: 1u64,
    };
    let changed = original.with_lifecycle(2);
    assert_eq!(budget.stats().used, content_charge);
    assert!(std::ptr::eq(original.content.get(), changed.content.get()));
    let mut range = RangeStore::new(RangeId(1), 0, RangeConfig::default(), budget.clone()).unwrap();
    // Shared content is already accounted. Only owned lifecycle heap bytes
    // would belong in the entry charge; this lifecycle is entirely inline.
    range
        .apply_batch(1, vec![Change::Put(Entry::new(1, original, 0))], ORDINARY)
        .unwrap();
    let lease = range.pin(0, 10).unwrap();
    range
        .apply_batch(2, vec![Change::Put(Entry::new(1, changed, 0))], ORDINARY)
        .unwrap();
    let old = lease.get(&1, 0).unwrap();
    assert_eq!(old.items()[0].value.lifecycle, 1);
    assert_eq!(range.get(&1).unwrap().lifecycle, 2);
    assert!(std::ptr::eq(
        old.items()[0].value.content.get(),
        range.get(&1).unwrap().content.get()
    ));
    assert_eq!(
        budget.stats().by_kind[BudgetKind::Payload as usize],
        content_charge
    );
    drop(range);
    drop(lease);
    assert_eq!(old.items()[0].value.content.get().0[0], 42);
    drop(old);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn expired_weak_handles_keep_metadata_charged_but_release_old_pages() {
    let budget = MemoryBudget::new(100_000, 0).unwrap();
    let mut range: RangeStore<u64, u64> =
        RangeStore::new(RangeId(1), 0, RangeConfig::default(), budget.clone()).unwrap();
    let base = budget.stats().used;
    let lease = range.pin(0, 1).unwrap();
    range.advance_clock(1).unwrap();
    assert_eq!(range.stats().pinned_snapshots, 0);
    assert!(budget.stats().used > base);
    assert!(budget.stats().by_kind[BudgetKind::ReadPins as usize] > 0);
    drop(range);
    assert!(budget.stats().used > 0);
    drop(lease);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn publication_chain_checks_every_ancestor_before_any_root_changes() {
    let budget = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut range = RangeStore::new(RangeId(1), 0, RangeConfig::default(), budget.clone()).unwrap();
    let first = range
        .prepare_batch(1, vec![Change::Put(Entry::new(1, 10, 0))], ORDINARY)
        .unwrap();
    let second = range
        .prepare_after(&first, 2, vec![Change::Put(Entry::new(2, 20, 0))], ORDINARY)
        .unwrap();
    let foreign = RangeStore::new(RangeId(1), 0, RangeConfig::default(), budget).unwrap();
    let wrong = foreign
        .prepare_batch(1, vec![Change::Put(Entry::new(3, 30, 0))], ORDINARY)
        .unwrap();
    assert!(range.validate_chain([&first, &wrong]).is_err());
    assert_eq!(range.prefix(), 0);
    assert_eq!(range.get(&1), None);
    range.validate_chain([&first, &second]).unwrap();
    range.publish(first).unwrap();
    range.publish(second).unwrap();
    assert_eq!(range.prefix(), 2);
}
#[test]
fn allocation_absorption_preserves_counters_and_failed_transfer_ownership() {
    let budget = MemoryBudget::new(1000, 200).unwrap();
    let mut first = budget
        .reserve(BudgetKind::Payload, ORDINARY, 100)
        .unwrap()
        .commit();
    let mut second = budget
        .reserve(BudgetKind::Payload, ORDINARY, 200)
        .unwrap()
        .commit();
    let before = budget.stats();
    first.absorb(&mut second).unwrap();
    assert_eq!(first.bytes(), 300);
    assert_eq!(second.bytes(), 0);
    assert_eq!(budget.stats(), before);
    let mut other_lane = budget
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 40)
        .unwrap()
        .commit();
    let mut other_kind = budget
        .reserve(BudgetKind::Pending, ORDINARY, 30)
        .unwrap()
        .commit();
    let foreign = MemoryBudget::new(1000, 0).unwrap();
    let mut other_owner = foreign
        .reserve(BudgetKind::Payload, ORDINARY, 50)
        .unwrap()
        .commit();
    for other in [&mut other_lane, &mut other_kind, &mut other_owner] {
        let amount = other.bytes();
        assert!(first.absorb(other).is_err());
        assert_eq!(first.bytes(), 300);
        assert_eq!(other.bytes(), amount);
    }
    drop((first, second, other_lane, other_kind, other_owner));
    assert_eq!(budget.stats().used, 0);
    assert_eq!(foreign.stats().used, 0);
}
