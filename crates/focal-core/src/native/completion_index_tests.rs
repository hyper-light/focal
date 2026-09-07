use super::*;
use crate::native::EvaluationTarget;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::{ClaimId, ValidationId};
use std::collections::BTreeMap;

// Deliberately not Clone: moving a grant table must not require copying owned
// policies, schema buffers, or other grant payloads.
#[derive(Debug, PartialEq, Eq)]
struct Row {
    value: u64,
    workspace: usize,
}

fn source() -> MemoryBudget {
    MemoryBudget::new(16 * 1024 * 1024, 0).unwrap()
}

fn key(value: u64) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(u128::from(value / 32 + 1)),
        validation: ValidationId::from_u128(u128::from(value) + 10_000),
        target: EvaluationTarget::Admission,
        generation: value % 3 + 1,
    }
}

fn put(index: &mut CompletionIndex<Row>, source: &MemoryBudget, value: u64, workspace: usize) {
    drop(index.grow(source, 8192).unwrap());
    index
        .insert(key(value), Row { value, workspace }, workspace)
        .unwrap();
}

fn check(index: &CompletionIndex<Row>, oracle: &BTreeMap<EvaluationKey, (u64, usize)>) {
    index.validate().unwrap();
    assert_eq!(index.len(), oracle.len());
    assert_eq!(
        index.maximum(),
        oracle
            .values()
            .map(|(_, weight)| *weight)
            .max()
            .unwrap_or(0)
    );
    let lower = EvaluationKey {
        claim: ClaimId::from_u128(0),
        validation: ValidationId::from_u128(0),
        target: EvaluationTarget::Admission,
        generation: 0,
    };
    let actual: Vec<_> = index
        .iter_from(lower)
        .map(|(key, row)| (key, (row.value, row.workspace)))
        .collect();
    let expected: Vec<_> = oracle.iter().map(|(key, row)| (*key, *row)).collect();
    assert_eq!(actual, expected);
    for (key, &(value, workspace)) in oracle {
        assert_eq!(index.get(*key), Some(&Row { value, workspace }));
    }
}

fn slots(index: &CompletionIndex<Row>, values: &[u64]) -> BTreeMap<u64, usize> {
    values
        .iter()
        .map(|&value| (value, index.slot(key(value)).unwrap()))
        .collect()
}

fn random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn all_avl_rotation_shapes_preserve_physical_grant_slots_and_exact_maximum() {
    for order in [[30, 20, 10], [10, 20, 30], [30, 10, 20], [10, 30, 20]] {
        let source = source();
        let mut index = CompletionIndex::new();
        let mut oracle = BTreeMap::new();
        let mut original_slots = BTreeMap::new();
        for value in order {
            put(&mut index, &source, value, value as usize);
            original_slots.insert(value, index.slot(key(value)).unwrap());
            oracle.insert(key(value), (value, value as usize));
            check(&index, &oracle);
            for (&old, &slot) in &original_slots {
                assert_eq!(index.slot(key(old)), Some(slot));
            }
        }
        index
            .replace_weight(key(10), 99, |row| row.workspace = 99)
            .unwrap();
        oracle.get_mut(&key(10)).unwrap().1 = 99;
        check(&index, &oracle);
        index
            .replace_weight(key(10), 0, |row| row.workspace = 0)
            .unwrap();
        oracle.get_mut(&key(10)).unwrap().1 = 0;
        check(&index, &oracle);
        assert_eq!(index.maximum(), 30);
        for value in order {
            assert_eq!(index.slot(key(value)), Some(original_slots[&value]));
        }
        drop(index);
        assert_eq!(source.stats().used, 0);
    }
}

#[test]
fn deleting_two_child_nodes_transplants_successor_without_relocating_other_grants() {
    for (order, removed) in [
        (vec![20, 10, 30], 20),
        (vec![40, 20, 60, 10, 30, 50, 70, 45, 55], 40),
        (vec![40, 20, 60, 10, 30, 50, 70, 45, 55], 60),
    ] {
        let source = source();
        let mut index = CompletionIndex::new();
        let mut oracle = BTreeMap::new();
        for &value in &order {
            put(&mut index, &source, value, (value * 3) as usize);
            oracle.insert(key(value), (value, (value * 3) as usize));
        }
        let before = slots(&index, &order);
        let removed_row = index.remove(key(removed)).unwrap();
        assert_eq!(removed_row.value, removed);
        oracle.remove(&key(removed));
        check(&index, &oracle);
        for &value in order.iter().filter(|&&value| value != removed) {
            assert_eq!(index.slot(key(value)), Some(before[&value]));
        }
        let budget = source.stats();
        // Existing vacant capacity remains usable after an interior deletion;
        // no surviving grant needs to move or obtain a new allocation.
        assert!(index.grow(&source, index.capacity()).unwrap().is_none());
        index
            .insert(
                key(99),
                Row {
                    value: 99,
                    workspace: 999,
                },
                999,
            )
            .unwrap();
        let new_slot = index.slot(key(99)).unwrap();
        for &value in order.iter().filter(|&&value| value != removed) {
            assert_ne!(new_slot, before[&value]);
            assert_eq!(index.slot(key(value)), Some(before[&value]));
        }
        assert_eq!(source.stats(), budget);
        oracle.insert(key(99), (99, 999));
        check(&index, &oracle);
        drop(index);
        assert_eq!(source.stats().used, 0);
    }
}

#[test]
fn randomized_insert_remove_weight_updates_and_lower_bounds_match_ordered_oracle() {
    let source = source();
    let mut index = CompletionIndex::new();
    let mut oracle = BTreeMap::new();
    let mut seed = 0x1cb2_30dc_761e_4a8d;
    for step in 0..6000_u64 {
        let value = random(&mut seed) % 256;
        let weight = (random(&mut seed) % 16384) as usize;
        match random(&mut seed) % 4 {
            0 if !oracle.contains_key(&key(value)) => {
                put(&mut index, &source, value, weight);
                oracle.insert(key(value), (value, weight));
            }
            1 => {
                let before = oracle.remove(&key(value));
                match before {
                    Some((stored, workspace)) => {
                        assert_eq!(
                            index.remove(key(value)).unwrap(),
                            Row {
                                value: stored,
                                workspace
                            }
                        )
                    }
                    None => assert!(index.remove(key(value)).is_err()),
                }
            }
            2 => {
                let result = index.replace_weight(key(value), weight, |row| {
                    row.value = step;
                    row.workspace = weight;
                });
                match oracle.get_mut(&key(value)) {
                    Some(row) => {
                        result.unwrap();
                        *row = (step, weight);
                    }
                    None => assert!(result.is_err()),
                }
            }
            _ => {
                let actual = index
                    .iter_from(key(value))
                    .next()
                    .map(|(key, row)| (key, (row.value, row.workspace)));
                let expected = oracle
                    .range(key(value)..)
                    .next()
                    .map(|(key, row)| (*key, *row));
                assert_eq!(actual, expected, "lower bound after step {step}");
            }
        }
        check(&index, &oracle);
    }
    for (key, (value, workspace)) in oracle {
        assert_eq!(index.remove(key).unwrap(), Row { value, workspace });
        index.validate().unwrap();
    }
    assert_eq!(index.len(), 0);
    assert_eq!(index.maximum(), 0);
    drop(index);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn lower_bound_and_claim_cohort_traversal_skip_unrelated_grants() {
    let source = source();
    let mut index = CompletionIndex::new();
    let mut oracle = BTreeMap::new();
    for value in (0..2048).rev() {
        put(&mut index, &source, value, (value % 31) as usize);
        oracle.insert(key(value), (value, (value % 31) as usize));
    }
    for claim in [1, 2, 31, 32, 63, 64, 65] {
        let lower = EvaluationKey {
            claim: ClaimId::from_u128(claim),
            validation: ValidationId::from_u128(0),
            target: EvaluationTarget::Admission,
            generation: 0,
        };
        index.reset_visits();
        let actual: Vec<_> = index
            .iter_from(lower)
            .take_while(|(key, _)| key.claim == lower.claim)
            .map(|(key, row)| (key, (row.value, row.workspace)))
            .collect();
        let visits = index.visits();
        let expected: Vec<_> = oracle
            .range(lower..)
            .take_while(|(key, _)| key.claim == lower.claim)
            .map(|(key, row)| (*key, *row))
            .collect();
        assert_eq!(actual, expected);
        assert!(
            visits <= 256 + 32 * actual.len(),
            "cohort touches {visits} nodes for {} grants",
            actual.len()
        );
    }
    assert!(index.iter_from(key(u64::MAX - 1)).next().is_none());
    drop(index);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn max_capacity_and_parent_pressure_refuse_growth_without_mutating_the_live_index() {
    let source = source();
    let mut index = CompletionIndex::new();
    assert!(
        index
            .insert(
                key(1),
                Row {
                    value: 1,
                    workspace: 1
                },
                1
            )
            .is_err()
    );
    assert_eq!(source.stats().used, 0);
    assert!(index.grow(&source, 0).is_err());
    drop(index.grow(&source, 1).unwrap());
    index
        .insert(
            key(1),
            Row {
                value: 1,
                workspace: 1,
            },
            1,
        )
        .unwrap();
    let original_slot = index.slot(key(1));
    let budget = source.stats();
    assert!(index.grow(&source, 1).is_err());
    assert_eq!(source.stats(), budget);
    assert!(
        index
            .insert(
                key(1),
                Row {
                    value: 999,
                    workspace: 999
                },
                999
            )
            .is_err()
    );
    assert_eq!(
        index.get(key(1)),
        Some(&Row {
            value: 1,
            workspace: 1
        })
    );
    let pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit();
    let full = source.stats();
    assert!(index.grow(&source, 2).is_err());
    assert_eq!(source.stats(), full);
    assert_eq!(index.slot(key(1)), original_slot);
    assert_eq!(index.maximum(), 1);
    assert_eq!(index.capacity(), 1);
    index.validate().unwrap();
    index.remove(key(1)).unwrap();
    assert!(index.grow(&source, 1).unwrap().is_none());
    index
        .insert(
            key(2),
            Row {
                value: 2,
                workspace: 2,
            },
            2,
        )
        .unwrap();
    assert_eq!(index.slot(key(2)), original_slot);
    assert_eq!(source.stats(), full);
    drop(pressure);
    drop(index);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn growth_rollback_preserves_committed_older_deletion_and_refuses_live_trailing_slot() {
    let source = source();
    let mut index = CompletionIndex::new();
    for value in 1..=4 {
        put(&mut index, &source, value, value as usize);
    }
    assert_eq!(index.capacity(), 4);
    let original = slots(&index, &[1, 2, 3, 4]);
    let budget = source.stats();
    let growth = index.grow(&source, 8).unwrap().unwrap();
    index
        .insert(
            key(5),
            Row {
                value: 5,
                workspace: 50,
            },
            50,
        )
        .unwrap();
    assert!(index.slot(key(5)).unwrap() >= 4);
    let expanded = source.stats();
    assert!(index.check_restore_growth(&growth).is_err());
    assert_eq!(source.stats(), expanded);
    assert_eq!(index.maximum(), 50);
    // An older terminal head can publish while this Begin remains pending.
    // Rolling back the later growth must not resurrect that removed grant.
    index.remove(key(2)).unwrap();
    index.remove(key(5)).unwrap();
    index.check_restore_growth(&growth).unwrap();
    index.restore_growth(growth).unwrap();
    assert_eq!(source.stats(), budget);
    assert_eq!(index.capacity(), 4);
    assert_eq!(index.len(), 3);
    assert!(index.get(key(2)).is_none());
    for value in [1, 3, 4] {
        assert_eq!(index.slot(key(value)), Some(original[&value]));
    }
    index.validate().unwrap();
    assert_eq!(index.maximum(), 4);
    assert!(index.grow(&source, 4).unwrap().is_none());
    index
        .insert(
            key(6),
            Row {
                value: 6,
                workspace: 60,
            },
            60,
        )
        .unwrap();
    assert_eq!(index.slot(key(6)), Some(original[&2]));
    assert_eq!(source.stats(), budget);
    drop(index);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn nested_growth_rollback_keeps_removed_old_slots_vacant_and_refunds_each_buffer_once() {
    let source = source();
    let mut index = CompletionIndex::new();
    put(&mut index, &source, 1, 10);
    let one = source.stats();
    let first = index.grow(&source, 8).unwrap().unwrap();
    index
        .insert(
            key(2),
            Row {
                value: 2,
                workspace: 20,
            },
            20,
        )
        .unwrap();
    let two = source.stats();
    let second = index.grow(&source, 8).unwrap().unwrap();
    index
        .insert(
            key(3),
            Row {
                value: 3,
                workspace: 30,
            },
            30,
        )
        .unwrap();
    let slot_two = index.slot(key(2));
    index.remove(key(1)).unwrap();
    index.remove(key(3)).unwrap();
    index.restore_growth(second).unwrap();
    assert_eq!(source.stats(), two);
    assert_eq!(index.slot(key(2)), slot_two);
    assert_eq!(index.len(), 1);
    assert!(index.get(key(1)).is_none());
    index.remove(key(2)).unwrap();
    index.restore_growth(first).unwrap();
    assert_eq!(source.stats(), one);
    assert_eq!(index.capacity(), 1);
    assert_eq!(index.len(), 0);
    assert_eq!(index.maximum(), 0);
    index.validate().unwrap();
    put(&mut index, &source, 9, 90);
    assert_eq!(index.slot(key(9)), Some(0));
    assert_eq!(source.stats(), one);
    drop(index);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn sustained_large_index_churn_has_logarithmic_structural_work_and_no_growth_allocations() {
    for population in [64, 512, 4096] {
        let source = source();
        let mut index = CompletionIndex::new();
        let mut oracle = BTreeMap::new();
        for value in 0..population {
            put(&mut index, &source, value, value as usize);
            oracle.insert(key(value), (value, value as usize));
        }
        let budget = source.stats();
        // A per-operation logarithmic bound across three population sizes allows
        // repeated local metadata accesses while rejecting a whole-index scan.
        let log = (population.ilog2() + 2) as usize;
        let max_touches = 64 * log;
        for step in 0..population / 2 {
            let old = step;
            index.reset_visits();
            assert_eq!(index.remove(key(old)).unwrap().value, old);
            assert!(
                index.visits() < max_touches,
                "delete visits {}",
                index.visits()
            );
            oracle.remove(&key(old));
            let next = population + step;
            index.reset_visits();
            index
                .insert(
                    key(next),
                    Row {
                        value: next,
                        workspace: next as usize,
                    },
                    next as usize,
                )
                .unwrap();
            assert!(
                index.visits() < max_touches,
                "insert visits {}",
                index.visits()
            );
            oracle.insert(key(next), (next, next as usize));
            let changed = step + population / 2;
            let weight = if step % 2 == 0 {
                100_000 + step as usize
            } else {
                0
            };
            index.reset_visits();
            index
                .replace_weight(key(changed), weight, |row| row.workspace = weight)
                .unwrap();
            assert!(
                index.visits() < max_touches,
                "update visits {}",
                index.visits()
            );
            oracle.get_mut(&key(changed)).unwrap().1 = weight;
            index.reset_visits();
            assert_eq!(index.get(key(changed)).unwrap().workspace, weight);
            assert!(index.visits() < 8 * log, "lookup visits {}", index.visits());
            assert_eq!(
                index.maximum(),
                oracle.values().map(|row| row.1).max().unwrap()
            );
            assert_eq!(source.stats(), budget);
            if step % 128 == 0 {
                check(&index, &oracle);
            }
        }
        check(&index, &oracle);
        drop(index);
        assert_eq!(source.stats().used, 0);
    }
}

fn refuses_all_mutations_after_poison(index: &mut CompletionIndex<Row>, source: &MemoryBudget) {
    assert!(index.check_health().is_err());
    let budget = source.stats();
    let len = index.len();
    let capacity = index.capacity();
    let invoked = std::cell::Cell::new(false);
    assert!(
        index
            .replace_weight(key(20), 999, |_| invoked.set(true))
            .is_err()
    );
    assert!(!invoked.get());
    assert!(
        index
            .insert(
                key(999),
                Row {
                    value: 999,
                    workspace: 999
                },
                999
            )
            .is_err()
    );
    assert!(index.remove(key(20)).is_err());
    assert!(index.grow(source, 8192).is_err());
    assert_eq!(index.len(), len);
    assert_eq!(index.capacity(), capacity);
    assert_eq!(source.stats(), budget);
}

#[test]
fn missing_root_or_referenced_child_poison_reads_and_refuse_later_mutations() {
    for missing_root in [false, true] {
        let source = source();
        let mut index = CompletionIndex::new();
        for value in [20, 10, 30] {
            put(&mut index, &source, value, value as usize);
        }
        if missing_root {
            index.root = None;
            assert_eq!(index.maximum(), 0);
            assert!(index.iter_from(key(10)).next().is_none());
        } else {
            let child = index.slot(key(10)).unwrap();
            index.slots[child].node = None;
            assert!(index.get(key(10)).is_none());
        }
        refuses_all_mutations_after_poison(&mut index, &source);
        drop(index);
        assert_eq!(source.stats().used, 0);
    }
}

#[test]
fn broken_parent_cursor_repair_and_vacancy_cycles_refuse_with_bounded_work() {
    for case in 0..4 {
        let source = source();
        let mut index = CompletionIndex::new();
        for value in [40, 20, 60, 10, 30, 50, 70] {
            put(&mut index, &source, value, value as usize);
        }
        let low = index.slot(key(10)).unwrap();
        let high = index.slot(key(70)).unwrap();
        let wrong_parent = index.slot(key(60)).unwrap();
        index.reset_visits();
        match case {
            0 => {
                index.slots[low].node.as_mut().unwrap().parent = Some(wrong_parent);
                assert!(index.iter_from(key(10)).next().is_none());
            }
            1 => {
                // The successor points back to its own key. Strict cursor
                // progress must detect this even without consuming len rows.
                index.slots[high].node.as_mut().unwrap().right = Some(high);
                assert!(index.iter_from(key(70)).take(16).count() <= 1);
            }
            2 => {
                index.slots[high].node.as_mut().unwrap().parent = Some(high);
                assert!(
                    index
                        .replace_weight(key(70), 999, |row| row.workspace = 999)
                        .is_err()
                );
            }
            _ => {
                index.remove(key(70)).unwrap();
                let free = index.free.unwrap();
                index.slots[free].free_previous = Some(free);
                assert!(
                    index
                        .insert(
                            key(80),
                            Row {
                                value: 80,
                                workspace: 80
                            },
                            80
                        )
                        .is_err()
                );
                assert!(
                    !index
                        .slots
                        .iter()
                        .any(|slot| slot.node.as_ref().is_some_and(|node| node.key == key(80)))
                );
            }
        }
        assert!(
            index.visits() < WALK_LIMIT as usize * 64,
            "corruption case {case} touched {} nodes",
            index.visits()
        );
        refuses_all_mutations_after_poison(&mut index, &source);
        drop(index);
        assert_eq!(source.stats().used, 0);
    }
}
