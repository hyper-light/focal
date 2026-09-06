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
    Arena, ArenaConfig, ArenaId, BudgetKind, BudgetLane, Handle, MemoryBudget, MemoryError,
    StableIndex,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier},
};

const ORDINARY: BudgetLane = BudgetLane::Ordinary;

fn random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[test]
fn generational_arena_and_stable_index_match_map_oracle() {
    for initial_seed in [1, 42, 912_321, 8_989_871] {
        let budget = MemoryBudget::new(16 * 1024 * 1024, 4096).unwrap();
        let mut arena = Arena::new(
            ArenaId(initial_seed as u128),
            ArenaConfig {
                page_slots: 7,
                max_slots: 128,
            },
            budget.clone(),
        )
        .unwrap();
        let mut index = StableIndex::new(arena.id(), budget.clone());
        let mut oracle = BTreeMap::<u64, u64>::new();
        let mut deleted = Vec::<Handle<u64>>::new();
        let mut seed = initial_seed;
        for step in 0..10_000 {
            let key = random(&mut seed) % 64;
            let value = random(&mut seed);
            match random(&mut seed) % 3 {
                0 if oracle.contains_key(&key) => {
                    let handle = index.get(&key).unwrap();
                    assert_eq!(
                        arena.replace(handle, value, 0, ORDINARY).unwrap(),
                        oracle.insert(key, value).unwrap()
                    );
                }
                0 | 1 if !oracle.contains_key(&key) => {
                    let handle = arena.insert(value, 0, ORDINARY).unwrap();
                    index.insert(key, handle, 0, ORDINARY).unwrap();
                    oracle.insert(key, value);
                }
                _ if oracle.contains_key(&key) => {
                    let handle = index.remove(&key).unwrap();
                    assert_eq!(arena.remove(handle).unwrap(), oracle.remove(&key).unwrap());
                    deleted.push(handle);
                }
                _ => {}
            }
            assert_eq!(arena.len(), oracle.len());
            assert_eq!(index.len(), oracle.len());
            assert_eq!(
                index
                    .iter()
                    .map(|(key, handle)| (*key, *arena.get(handle).unwrap()))
                    .collect::<BTreeMap<_, _>>(),
                oracle
            );
            if step % 100 == 0 {
                for handle in &deleted {
                    assert_eq!(arena.get(*handle), Err(MemoryError::StaleHandle));
                }
            }
        }
        assert!(arena.capacity() <= 128);
        drop(arena);
        drop(index);
        assert_eq!(budget.stats().used, 0);
    }
}

#[test]
fn stale_and_foreign_handles_never_resolve_reused_slots() {
    let budget = MemoryBudget::new(100_000, 0).unwrap();
    let config = ArenaConfig {
        page_slots: 1,
        max_slots: 1,
    };
    let mut first = Arena::new(ArenaId(1), config, budget.clone()).unwrap();
    let mut second = Arena::new(ArenaId(2), config, budget.clone()).unwrap();
    let old = first.insert("old", 0, ORDINARY).unwrap();
    let foreign = second.insert("foreign", 0, ORDINARY).unwrap();
    assert_eq!(old.slot(), foreign.slot());
    assert_eq!(old.generation(), foreign.generation());
    assert_eq!(first.get(foreign), Err(MemoryError::WrongArena));
    assert_eq!(first.remove(foreign), Err(MemoryError::WrongArena));
    first.remove(old).unwrap();
    let new = first.insert("new", 0, ORDINARY).unwrap();
    assert_eq!(new.slot(), old.slot());
    assert_ne!(new.generation(), old.generation());
    assert_eq!(first.get(old), Err(MemoryError::StaleHandle));
    assert_eq!(first.get(new), Ok(&"new"));
    let mut index = StableIndex::new(first.id(), budget);
    assert_eq!(
        index.insert(1, foreign, 0, ORDINARY),
        Err(MemoryError::WrongArena)
    );
    index.insert(1, new, 0, ORDINARY).unwrap();
    assert_eq!(index.resolve(&1, &second), Err(MemoryError::WrongArena));
    first.remove(new).unwrap();
    assert_eq!(index.resolve(&1, &first), Err(MemoryError::StaleHandle));
}

#[test]
fn failed_allocation_and_replacement_release_reservations() {
    let budget = MemoryBudget::new(4096, 512).unwrap();
    let mut arena = Arena::new(
        ArenaId(1),
        ArenaConfig {
            page_slots: 2,
            max_slots: 2,
        },
        budget.clone(),
    )
    .unwrap();
    let handle = arena.insert("present", 500, ORDINARY).unwrap();
    let before = budget.stats();
    assert!(matches!(
        arena.replace(handle, "too big", 4096, ORDINARY),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(arena.get(handle), Ok(&"present"));
    assert!(matches!(
        arena.insert("too big", usize::MAX, ORDINARY),
        Err(MemoryError::Capacity { .. })
    ));
    assert_eq!(budget.stats(), before);
    arena.remove(handle).unwrap();
    assert_eq!(budget.stats().used, before.used - 500);
    drop(arena);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn reservation_rollback_and_completion_lane_are_enforced() {
    let budget = MemoryBudget::new(1000, 200).unwrap();
    let admission = budget.reserve(BudgetKind::Pending, ORDINARY, 800).unwrap();
    assert_eq!(admission.bytes(), 800);
    assert!(matches!(
        budget.reserve(BudgetKind::Payload, ORDINARY, 1),
        Err(MemoryError::Capacity { available: 0, .. })
    ));
    let complete = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, 200)
        .unwrap()
        .commit();
    assert_eq!(budget.stats().used, 1000);
    assert!(matches!(
        budget.reserve(BudgetKind::Control, BudgetLane::Completion, 1),
        Err(MemoryError::Capacity { available: 0, .. })
    ));
    drop(admission);
    assert_eq!(budget.stats().used, 200);
    assert_eq!(budget.stats().ordinary_used, 0);
    drop(complete);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn concurrent_reservations_never_exceed_allowance_and_release_every_category() {
    let budget = MemoryBudget::new(8192, 2048).unwrap();
    let barrier = Arc::new(Barrier::new(9));
    let threads: Vec<_> = (0..8)
        .map(|worker| {
            let budget = budget.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..5000 {
                    let lane = if worker % 3 == 0 {
                        BudgetLane::Completion
                    } else {
                        ORDINARY
                    };
                    let first = budget.reserve(BudgetKind::Query, lane, 500);
                    let second = budget.reserve(BudgetKind::Recovery, lane, 800);
                    assert!(budget.stats().used <= 8192);
                    assert!(budget.stats().ordinary_used <= 6144);
                    drop(first);
                    drop(second);
                }
            })
        })
        .collect();
    barrier.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    let stats = budget.stats();
    assert_eq!(stats.used, 0);
    assert_eq!(stats.ordinary_used, 0);
    assert!(stats.by_kind.iter().all(|value| *value == 0));
}
