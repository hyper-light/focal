use super::*;
use std::cell::Cell;

fn config() -> RangeConfig {
    RangeConfig {
        page_entries: 2,
        ..RangeConfig::default()
    }
}

// The absence of Clone also checks that supplied values move and only retained
// neighbours reach the explicitly selected copier.
struct Value<'a> {
    bytes: Vec<u8>,
    probe: Option<Probe<'a>>,
}
struct Probe<'a> {
    budget: &'a MemoryBudget,
    kind: BudgetKind,
    observed: &'a Cell<usize>,
}
impl Drop for Value<'_> {
    fn drop(&mut self) {
        if let Some(probe) = &self.probe {
            probe
                .observed
                .set(probe.budget.stats().by_kind[probe.kind as usize]);
        }
    }
}
fn value(key: u64, probe: Option<Probe<'_>>) -> Change<u64, Value<'_>> {
    let value = Value {
        bytes: vec![42; 1024],
        probe,
    };
    let heap = value.bytes.capacity() + ALLOCATOR_OVERHEAD;
    Change::Put(Entry::new(key, value, heap))
}
fn copy<'a>(old: &Value<'a>) -> Result<Value<'a>, MemoryError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(old.bytes.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    bytes.extend_from_slice(&old.bytes);
    Ok(Value { bytes, probe: None })
}
fn seeded<'a>(budget: &MemoryBudget) -> RangeStore<u64, Value<'a>> {
    let mut range = RangeStore::new(RangeId(510), 0, config(), budget.clone()).unwrap();
    let candidate = range
        .prepare_batch_with(
            1,
            (1..=4).map(|key| value(key * 10, None)).collect(),
            BudgetLane::Ordinary,
            |_| panic!("empty predecessor has no retained values"),
        )
        .unwrap();
    range.publish(candidate).unwrap();
    range
}

#[test]
fn transferred_input_matches_normal_peak_and_moves_without_copy_at_full_parent() {
    let budget = MemoryBudget::new(2_000_000, 500_000).unwrap();
    let range = seeded(&budget);
    let plan = range
        .plan_batch(2, vec![value(10, None)], BudgetLane::Completion, usize::MAX)
        .unwrap();
    let quote = plan.charges();
    let pool = budget
        .funded_child(BudgetLane::Completion, quote.additional_peak_bytes())
        .unwrap();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    let parent = budget.stats();
    let mut normal_pending = 0;
    let mut normal_used = 0;
    let normal = plan
        .build_in_with(&pool, |old| {
            normal_pending = pool.stats().by_kind[BudgetKind::Pending as usize];
            normal_used = pool.stats().used;
            copy(old)
        })
        .unwrap();
    let normal_retained = pool.stats();
    assert!(normal_pending >= quote.input_pending_bytes());
    drop(normal);
    assert_eq!(pool.stats().used, 0);

    let supplied = value(10, None);
    let address = match &supplied {
        Change::Put(entry) => entry.value.bytes.as_ptr(),
        Change::Delete(_) => unreachable!(),
    };
    let plan = range
        .plan_batch(2, vec![supplied], BudgetLane::Completion, usize::MAX)
        .unwrap();
    assert_eq!(plan.charges(), quote);
    let input = pool
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            quote.input_pending_bytes(),
        )
        .unwrap()
        .commit();
    assert_eq!(pool.stats().used, quote.input_pending_bytes());
    let mut copied = 0;
    let candidate = plan
        .build_in_funded_with(&pool, input, |old| {
            copied += 1;
            // Both the pending category and total peak equal the ordinary
            // builder. An extra incoming debit cannot hide in quote slack.
            assert_eq!(
                pool.stats().by_kind[BudgetKind::Pending as usize],
                normal_pending
            );
            assert_eq!(pool.stats().used, normal_used);
            assert_eq!(budget.stats().used, parent.used);
            copy(old)
        })
        .unwrap();
    assert_eq!(copied, 1);
    assert_eq!(candidate.get(&10).unwrap().bytes.as_ptr(), address);
    assert_eq!(candidate.get(&20).unwrap().bytes.len(), 1024);
    assert_eq!(pool.stats(), normal_retained);
    assert_eq!(range.prefix(), 1);
    drop(candidate);
    assert_eq!(pool.stats().used, 0);
    assert_eq!(budget.stats(), parent);
    drop(pressure);
}

#[test]
fn mismatched_transfer_refuses_before_copy_and_keeps_credit_through_input_drop() {
    let budget = MemoryBudget::new(2_000_000, 500_000).unwrap();
    let child = budget.child(500_000, 100_000).unwrap();
    let sibling = budget.child(500_000, 100_000).unwrap();
    let foreign = MemoryBudget::new(2_000_000, 500_000).unwrap();
    // source, permit owner, category, lane, extra bytes. The same parent and
    // equal-sized children are deliberately not interchangeable owners.
    let cases = [
        (
            &budget,
            &budget,
            BudgetKind::Payload,
            BudgetLane::Completion,
            0isize,
        ),
        (
            &budget,
            &budget,
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            0,
        ),
        (
            &budget,
            &budget,
            BudgetKind::Pending,
            BudgetLane::Completion,
            -1,
        ),
        (
            &budget,
            &budget,
            BudgetKind::Pending,
            BudgetLane::Completion,
            1,
        ),
        (
            &child,
            &budget,
            BudgetKind::Pending,
            BudgetLane::Completion,
            0,
        ),
        (
            &budget,
            &child,
            BudgetKind::Pending,
            BudgetLane::Completion,
            0,
        ),
        (
            &child,
            &sibling,
            BudgetKind::Pending,
            BudgetLane::Completion,
            0,
        ),
        (
            &foreign,
            &foreign,
            BudgetKind::Pending,
            BudgetLane::Completion,
            0,
        ),
    ];
    for (source, owner, kind, lane, extra) in cases {
        let observed = Cell::new(0);
        let range = RangeStore::new(RangeId(511), 0, config(), budget.clone()).unwrap();
        let plan = range
            .plan_batch(
                1,
                vec![value(
                    10,
                    Some(Probe {
                        budget: owner,
                        kind,
                        observed: &observed,
                    }),
                )],
                BudgetLane::Completion,
                usize::MAX,
            )
            .unwrap();
        let bytes = plan
            .charges()
            .input_pending_bytes()
            .checked_add_signed(extra)
            .unwrap();
        let before = owner.stats();
        let permit = owner.reserve(kind, lane, bytes).unwrap().commit();
        assert!(matches!(
            plan.build_in_funded_with(source, permit, |_| panic!("mismatched permit reached copy")),
            Err(MemoryError::InvalidConfiguration(_))
        ));
        assert_eq!(observed.get(), before.by_kind[kind as usize] + bytes);
        assert_eq!(owner.stats(), before);
        assert!(range.is_empty());
        assert_eq!(range.prefix(), 0);
    }
}

#[test]
fn transferred_credit_outlives_inputs_on_directory_refusal() {
    let budget = MemoryBudget::new(2_000_000, 500_000).unwrap();
    let observed = Cell::new(0);
    let range = RangeStore::new(RangeId(512), 0, config(), budget.clone()).unwrap();
    let plan = range
        .plan_batch(
            1,
            vec![value(
                10,
                Some(Probe {
                    budget: &budget,
                    kind: BudgetKind::Pending,
                    observed: &observed,
                }),
            )],
            BudgetLane::Completion,
            usize::MAX,
        )
        .unwrap();
    let bytes = plan.charges().input_pending_bytes();
    let input = budget
        .reserve(BudgetKind::Pending, BudgetLane::Completion, bytes)
        .unwrap()
        .commit();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    assert!(matches!(
        plan.build_in_funded_with(&budget, input, |_| panic!("directory refusal reached copy")),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(observed.get(), bytes);
    assert_eq!(budget.stats().by_kind[BudgetKind::Pending as usize], 0);
    assert!(range.is_empty());
    drop(pressure);
}

#[test]
fn transferred_credit_outlives_moved_inputs_when_copier_refuses() {
    let budget = MemoryBudget::new(2_000_000, 500_000).unwrap();
    let observed = Cell::new(0);
    let range = seeded(&budget);
    let before = budget.stats();
    let plan = range
        .plan_batch(
            2,
            vec![value(
                10,
                Some(Probe {
                    budget: &budget,
                    kind: BudgetKind::Pending,
                    observed: &observed,
                }),
            )],
            BudgetLane::Completion,
            usize::MAX,
        )
        .unwrap();
    let bytes = plan.charges().input_pending_bytes();
    let input = budget
        .reserve(BudgetKind::Pending, BudgetLane::Completion, bytes)
        .unwrap()
        .commit();
    let mut copies = 0;
    assert!(matches!(
        plan.build_in_funded_with(&budget, input, |_| {
            copies += 1;
            Err(MemoryError::AllocationFailed)
        }),
        Err(MemoryError::AllocationFailed)
    ));
    assert_eq!(copies, 1);
    // The first incoming row moved into the unfinished destination before the
    // retained neighbour's copier refused. Both input and merge credit remain.
    assert!(observed.get() >= bytes);
    assert_eq!(budget.stats(), before);
    assert_eq!(range.prefix(), 1);
    assert_eq!(range.get(&10).unwrap().bytes.len(), 1024);
}
