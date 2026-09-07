use crate::{
    BudgetKind, BudgetLane, BudgetStats, Change, Entry, MemoryBudget, MemoryError, RangeConfig,
    RangeId, RangeStore,
};

fn bytes(stats: &BudgetStats, kind: BudgetKind) -> usize {
    stats.by_kind[kind as usize]
}
fn quiescent(stats: &BudgetStats) {
    assert_eq!(stats.by_kind.iter().sum::<usize>(), stats.used);
    assert!(stats.used <= stats.limit);
    assert!(stats.ordinary_used <= stats.used);
    assert!(stats.ordinary_used <= stats.limit - stats.completion_reserve);
}
fn bounded_snapshot(stats: &BudgetStats) {
    // A diagnostic snapshot is not atomic across categories. Each individual
    // counter must still be bounded; only quiescent snapshots may be summed.
    assert!(stats.used <= stats.limit);
    assert!(stats.ordinary_used <= stats.limit - stats.completion_reserve);
    assert!(stats.by_kind.iter().all(|value| *value <= stats.limit));
}

#[test]
fn spendable_capacity_excludes_additionally_reserved_pool_metadata() {
    let parent = MemoryBudget::new(1_000_000, 100_000).unwrap();
    let existing = parent
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 100)
        .unwrap();
    let baseline = parent.stats();
    let pool = parent.funded_child(BudgetLane::Ordinary, 4096).unwrap();
    let backed = parent.stats();
    let backing = backed.used - baseline.used;
    assert!(backing > 4096);
    assert_eq!(bytes(&backed, BudgetKind::Reserved), backing);
    assert_eq!(backed.ordinary_used - baseline.ordinary_used, backing);
    assert_eq!(pool.stats().limit, 4096);
    assert_eq!(pool.stats().completion_reserve, 0);
    assert_eq!(pool.stats().used, 0);
    let all = pool
        .reserve(BudgetKind::Pages, BudgetLane::Completion, 4096)
        .unwrap();
    assert_eq!(pool.stats().used, 4096);
    assert_eq!(parent.stats().used, backed.used);
    assert_eq!(parent.stats().ordinary_used, backed.ordinary_used);
    assert_eq!(bytes(&parent.stats(), BudgetKind::Pages), 4096);
    assert_eq!(bytes(&parent.stats(), BudgetKind::Reserved), backing - 4096);
    let before = (pool.stats(), parent.stats());
    assert!(
        pool.reserve(BudgetKind::Pending, BudgetLane::Completion, 1)
            .is_err()
    );
    assert_eq!((pool.stats(), parent.stats()), before);
    drop(all);
    assert_eq!(parent.stats(), backed);
    quiescent(&pool.stats());
    drop(pool);
    assert_eq!(parent.stats(), baseline);
    drop(existing);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn full_parent_pressure_cannot_block_spending_or_reusing_refunded_credit() {
    let parent = MemoryBudget::new(1_000_000, 100_000).unwrap();
    let baseline = parent.stats();
    let pool = parent.funded_child(BudgetLane::Ordinary, 4096).unwrap();
    let backed = parent.stats();
    let pressure = parent
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            backed.limit - backed.used,
        )
        .unwrap();
    let occupied = parent.stats();
    assert_eq!(occupied.used, occupied.limit);
    assert!(
        parent
            .reserve(BudgetKind::Payload, BudgetLane::Completion, 1)
            .is_err()
    );
    let mut pages = pool
        .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 1200)
        .unwrap()
        .commit();
    let control = pool
        .reserve(BudgetKind::Control, BudgetLane::Completion, 4096 - 1200)
        .unwrap();
    assert_eq!(pool.stats().used, 4096);
    assert_eq!(pool.stats().ordinary_used, 1200);
    assert_eq!(parent.stats().used, occupied.used);
    assert_eq!(parent.stats().ordinary_used, occupied.ordinary_used);
    pages.shrink_to(600).unwrap();
    let pending = pool
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 600)
        .unwrap();
    assert_eq!(bytes(&parent.stats(), BudgetKind::Pages), 600);
    assert_eq!(bytes(&parent.stats(), BudgetKind::Pending), 600);
    assert_eq!(
        bytes(&parent.stats(), BudgetKind::Reserved),
        bytes(&backed, BudgetKind::Reserved) - 4096
    );
    drop(pages);
    drop(control);
    drop(pending);
    assert_eq!(parent.stats(), occupied);
    for kind in [BudgetKind::Roots, BudgetKind::ReadPins, BudgetKind::Dedup] {
        let held = pool.reserve(kind, BudgetLane::Completion, 4096).unwrap();
        assert_eq!(bytes(&parent.stats(), kind), 4096);
        assert_eq!(parent.stats().used, occupied.used);
        drop(held);
        assert_eq!(parent.stats(), occupied);
    }
    drop(pressure);
    assert_eq!(parent.stats(), backed);
    drop(pool);
    assert_eq!(parent.stats(), baseline);
}

#[test]
fn completion_funding_cannot_be_laundered_into_ordinary_descendant_work() {
    let parent = MemoryBudget::new(2_000_000, 1_000_000).unwrap();
    let baseline = parent.stats();
    let pool = parent
        .funded_child(BudgetLane::Completion, 256_000)
        .unwrap();
    assert_eq!(parent.stats().ordinary_used, baseline.ordinary_used);
    assert_eq!(pool.reservation_limit(BudgetLane::Ordinary), 0);
    let normal = pool.child(128_000, 0).unwrap();
    let nested = normal.funded_child(BudgetLane::Completion, 32_000).unwrap();
    let below = nested.child(64_000, 0).unwrap();
    assert!(below.is_within(&parent));
    assert!(below.is_within(&pool));
    assert!(below.is_within(&nested));
    for source in [&pool, &normal, &nested, &below] {
        let before = (
            parent.stats(),
            pool.stats(),
            normal.stats(),
            nested.stats(),
            below.stats(),
        );
        assert_eq!(source.reservation_limit(BudgetLane::Ordinary), 0);
        assert!(
            source
                .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 1)
                .is_err()
        );
        assert!(source.funded_child(BudgetLane::Ordinary, 1024).is_err());
        assert_eq!(
            (
                parent.stats(),
                pool.stats(),
                normal.stats(),
                nested.stats(),
                below.stats()
            ),
            before
        );
    }
    let retained = below
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 32_000)
        .unwrap();
    assert_eq!(parent.stats().ordinary_used, baseline.ordinary_used);
    assert_eq!(bytes(&parent.stats(), BudgetKind::Payload), 32_000);
    assert_eq!(bytes(&pool.stats(), BudgetKind::Payload), 32_000);
    assert_eq!(bytes(&normal.stats(), BudgetKind::Payload), 32_000);
    assert_eq!(bytes(&nested.stats(), BudgetKind::Payload), 32_000);
    assert_eq!(bytes(&below.stats(), BudgetKind::Payload), 32_000);
    assert!(
        below
            .reserve(BudgetKind::Payload, BudgetLane::Completion, 1)
            .is_err()
    );
    drop(retained);
    drop(below);
    drop(nested);
    drop(normal);
    drop(pool);
    assert_eq!(parent.stats(), baseline);
}

#[test]
fn mixed_nested_budgets_keep_every_ancestor_funding_lane_and_category_exact() {
    let root = MemoryBudget::new(4_000_000, 500_000).unwrap();
    let tenant = root.child(3_000_000, 100_000).unwrap();
    let baseline = (root.stats(), tenant.stats());
    let outer = tenant.funded_child(BudgetLane::Ordinary, 512_000).unwrap();
    let top = (root.stats(), tenant.stats(), outer.stats());
    let normal = outer.child(256_000, 64_000).unwrap();
    let inner = normal.funded_child(BudgetLane::Completion, 64_000).unwrap();
    let nested = (root.stats(), tenant.stats(), outer.stats(), normal.stats());
    assert_eq!(root.stats().used, top.0.used);
    assert_eq!(tenant.stats().used, top.1.used);
    assert_eq!(root.stats().ordinary_used, top.0.ordinary_used);
    assert_eq!(outer.stats().ordinary_used, 0);
    let held = inner
        .reserve(BudgetKind::Arena, BudgetLane::Completion, 20_000)
        .unwrap();
    for source in [&root, &tenant, &outer, &normal, &inner] {
        let stats = source.stats();
        assert_eq!(bytes(&stats, BudgetKind::Arena), 20_000);
        quiescent(&stats);
    }
    assert_eq!(root.stats().used, nested.0.used);
    assert_eq!(tenant.stats().used, nested.1.used);
    assert_eq!(outer.stats().used, nested.2.used);
    assert_eq!(normal.stats().used, nested.3.used);
    drop(held);
    assert_eq!(
        (root.stats(), tenant.stats(), outer.stats(), normal.stats()),
        nested
    );
    drop(inner);
    assert_eq!((root.stats(), tenant.stats(), outer.stats()), top);
    drop(normal);
    drop(outer);
    assert_eq!((root.stats(), tenant.stats()), baseline);
}

#[test]
fn failed_pool_creation_leaves_all_ancestors_at_their_original_balances() {
    let parent = MemoryBudget::new(4096, 2048).unwrap();
    let child = parent.child(16_384, 0).unwrap();
    for source in [&parent, &child] {
        for (lane, amount) in [
            (BudgetLane::Ordinary, 0),
            (BudgetLane::Ordinary, 2048),
            (BudgetLane::Completion, 4096),
            (BudgetLane::Completion, usize::MAX),
        ] {
            let before = (parent.stats(), child.stats());
            assert!(source.funded_child(lane, amount).is_err());
            assert_eq!((parent.stats(), child.stats()), before);
        }
    }
    let pressure = parent
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 4096)
        .unwrap();
    let before = (parent.stats(), child.stats());
    assert!(child.funded_child(BudgetLane::Completion, 1).is_err());
    assert_eq!((parent.stats(), child.stats()), before);
    drop(pressure);
    let pool = child.funded_child(BudgetLane::Ordinary, 1024).unwrap();
    assert!(pool.is_within(&child) && pool.is_within(&parent));
    drop(pool);
    assert_eq!(parent.stats().used, 0);
    assert_eq!(child.stats().used, 0);
}

#[test]
fn funded_and_normal_hierarchy_depth_is_shared_and_failed_extension_refunds_nothing_twice() {
    let root = MemoryBudget::new(16_000_000, 0).unwrap();
    let baseline = root.stats();
    let independent = MemoryBudget::new(16_000_000, 0).unwrap();
    let mut deepest = root.clone();
    let mut capacity = 8_000_000;
    for depth in 2..=8 {
        deepest = if depth % 2 == 0 {
            capacity /= 2;
            deepest
                .funded_child(BudgetLane::Ordinary, capacity)
                .unwrap()
        } else {
            deepest.child(capacity, 0).unwrap()
        };
        assert!(deepest.is_within(&root));
        assert!(!root.is_within(&deepest));
        assert!(!deepest.is_within(&independent));
    }
    let before = (root.stats(), deepest.stats());
    assert!(matches!(
        deepest.child(1, 0),
        Err(MemoryError::InvalidConfiguration(_))
    ));
    assert!(matches!(
        deepest.funded_child(BudgetLane::Completion, 1),
        Err(MemoryError::InvalidConfiguration(_))
    ));
    assert_eq!((root.stats(), deepest.stats()), before);
    let value = deepest
        .reserve(BudgetKind::Control, BudgetLane::Completion, 64)
        .unwrap();
    drop(deepest);
    assert_eq!(bytes(&root.stats(), BudgetKind::Control), 64);
    assert_eq!(root.stats().used, before.0.used);
    drop(value);
    assert_eq!(root.stats(), baseline);
}

#[test]
fn split_absorb_and_shrink_transfer_one_permit_without_double_credit_or_readmission() {
    let parent = MemoryBudget::new(1_000_000, 0).unwrap();
    let pool = parent.funded_child(BudgetLane::Ordinary, 1024).unwrap();
    let backed = parent.stats();
    let pressure = parent
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            backed.limit - backed.used,
        )
        .unwrap();
    let mut first = pool
        .reserve(BudgetKind::Pages, BudgetLane::Completion, 1024)
        .unwrap()
        .commit();
    let occupied = (pool.stats(), parent.stats());
    assert!(first.split_off(1025).is_err());
    assert!(first.shrink_to(1025).is_err());
    assert_eq!(first.bytes(), 1024);
    assert_eq!((pool.stats(), parent.stats()), occupied);
    let mut second = first.split_off(600).unwrap();
    let mut zero = first.split_off(0).unwrap();
    assert_eq!((first.bytes(), second.bytes(), zero.bytes()), (424, 600, 0));
    assert_eq!((pool.stats(), parent.stats()), occupied);
    first.absorb(&mut second).unwrap();
    assert_eq!((first.bytes(), second.bytes()), (1024, 0));
    zero.absorb(&mut first).unwrap();
    assert_eq!((zero.bytes(), first.bytes()), (1024, 0));
    drop(first);
    drop(second);
    assert_eq!((pool.stats(), parent.stats()), occupied);
    zero.shrink_to(100).unwrap();
    assert_eq!(pool.stats().used, 100);
    let returned = pool
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 924)
        .unwrap();
    assert_eq!(pool.stats().used, 1024);
    assert_eq!(parent.stats().ordinary_used, backed.ordinary_used);
    drop(returned);
    drop(zero);
    assert_eq!(pool.stats().used, 0);
    assert_eq!(
        bytes(&parent.stats(), BudgetKind::Reserved),
        bytes(&backed, BudgetKind::Reserved)
    );
    drop(pressure);
    drop(pool);
    assert_eq!(parent.stats().used, 0);
    quiescent(&parent.stats());
}

#[test]
fn differing_sources_kinds_and_lanes_cannot_absorb_each_others_funded_credit() {
    let parent = MemoryBudget::new(1_000_000, 0).unwrap();
    let a = parent.funded_child(BudgetLane::Ordinary, 2048).unwrap();
    let b = parent.funded_child(BudgetLane::Ordinary, 2048).unwrap();
    assert!(!a.is_within(&b));
    assert!(!b.is_within(&a));
    let ordinary_child = a.child(1024, 0).unwrap();
    let mut first = a
        .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 100)
        .unwrap()
        .commit();
    let mut candidates = vec![
        b.reserve(BudgetKind::Pages, BudgetLane::Ordinary, 200)
            .unwrap()
            .commit(),
        a.reserve(BudgetKind::Payload, BudgetLane::Ordinary, 200)
            .unwrap()
            .commit(),
        a.reserve(BudgetKind::Pages, BudgetLane::Completion, 200)
            .unwrap()
            .commit(),
        ordinary_child
            .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 200)
            .unwrap()
            .commit(),
    ];
    let before = (parent.stats(), a.stats(), b.stats(), ordinary_child.stats());
    for other in &mut candidates {
        assert!(first.absorb(other).is_err());
        assert_eq!((first.bytes(), other.bytes()), (100, 200));
        assert_eq!(
            (parent.stats(), a.stats(), b.stats(), ordinary_child.stats()),
            before
        );
    }
    drop(candidates);
    drop(first);
    drop(ordinary_child);
    drop(a);
    drop(b);
    assert_eq!(parent.stats().used, 0);
    quiescent(&parent.stats());
}

#[test]
fn outstanding_allocation_keeps_idle_backing_until_its_last_source_handle_drops() {
    let parent = MemoryBudget::new(1_000_000, 0).unwrap();
    let baseline = parent.stats();
    let pool = parent.funded_child(BudgetLane::Ordinary, 4096).unwrap();
    let backed = parent.stats();
    let mut result = pool
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 16)
        .unwrap()
        .commit();
    drop(pool);
    assert_eq!(parent.stats().used, backed.used);
    assert_eq!(
        bytes(&parent.stats(), BudgetKind::Reserved),
        bytes(&backed, BudgetKind::Reserved) - 16
    );
    result.shrink_to(0).unwrap();
    assert_eq!(parent.stats(), backed);
    // Even an emptied permit retains its source handle until the permit drops.
    assert_eq!(result.bytes(), 0);
    drop(result);
    assert_eq!(parent.stats(), baseline);
}

#[test]
fn prepared_storage_pages_outlive_the_pool_handle_without_releasing_their_backing() {
    let parent = MemoryBudget::new(4_000_000, 0).unwrap();
    let baseline = parent.stats();
    let pool = parent
        .funded_child(BudgetLane::Ordinary, 1_000_000)
        .unwrap();
    let backed = parent.stats();
    let store = RangeStore::from_entries(
        RangeId(900),
        1,
        RangeConfig {
            page_entries: 4,
            ..RangeConfig::default()
        },
        pool.clone(),
        vec![Entry::new(1_u64, 10_u64, 0)],
    )
    .unwrap();
    let prepared = store
        .prepare_batch(
            2,
            vec![Change::Put(Entry::new(2_u64, 20_u64, 0))],
            BudgetLane::Completion,
        )
        .unwrap();
    assert!(bytes(&parent.stats(), BudgetKind::Pages) > 0);
    assert!(bytes(&parent.stats(), BudgetKind::Roots) > 0);
    drop(pool);
    drop(store);
    assert_eq!(prepared.get(&1), Some(&10));
    assert_eq!(prepared.get(&2), Some(&20));
    assert_eq!(parent.stats().used, backed.used);
    assert_eq!(parent.stats().ordinary_used, backed.ordinary_used);
    quiescent(&parent.stats());
    drop(prepared);
    assert_eq!(parent.stats(), baseline);
}

#[test]
fn concurrent_mixed_kind_spend_shrink_split_and_drop_never_underflow_held_credit() {
    let parent = MemoryBudget::new(2_000_000, 200_000).unwrap();
    let baseline = parent.stats();
    let pool = parent.funded_child(BudgetLane::Ordinary, 4096).unwrap();
    let backed = parent.stats();
    let pressure = parent
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            backed.limit - backed.used,
        )
        .unwrap();
    let occupied = parent.stats();
    let completed = std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for worker in 0..8 {
            let source = &pool;
            let ancestor = &parent;
            workers.push(scope.spawn(move || {
                let mut completed = 0;
                for cycle in 0..500 {
                    let kind = [
                        BudgetKind::Pages,
                        BudgetKind::Roots,
                        BudgetKind::Payload,
                        BudgetKind::Control,
                    ][(worker + cycle) % 4];
                    let lane = if (worker + cycle) % 2 == 0 {
                        BudgetLane::Ordinary
                    } else {
                        BudgetLane::Completion
                    };
                    // Whole-pool turns force Reserved down to metadata only;
                    // smaller turns race refund/reuse across different kinds.
                    let amount = [4096, 1024, 2048][(worker + cycle) % 3];
                    let mut allocation = match source.reserve(kind, lane, amount) {
                        Ok(reservation) => reservation.commit(),
                        Err(MemoryError::Capacity { .. }) => {
                            std::thread::yield_now();
                            continue;
                        }
                        Err(error) => panic!("unexpected funded reservation error: {error:?}"),
                    };
                    completed += 1;
                    let mut split = allocation.split_off(amount / 2).unwrap();
                    std::thread::yield_now();
                    split.shrink_to(amount / 4).unwrap();
                    allocation.absorb(&mut split).unwrap();
                    drop(split);
                    bounded_snapshot(&source.stats());
                    let current = ancestor.stats();
                    bounded_snapshot(&current);
                    assert_eq!(current.used, occupied.used);
                    assert_eq!(current.ordinary_used, occupied.ordinary_used);
                    drop(allocation);
                }
                completed
            }));
        }
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .sum::<usize>()
    });
    assert!(completed > 0);
    assert_eq!(pool.stats().used, 0);
    assert_eq!(pool.stats().ordinary_used, 0);
    assert_eq!(parent.stats(), occupied);
    quiescent(&pool.stats());
    let full = pool
        .reserve(BudgetKind::Pages, BudgetLane::Completion, 4096)
        .unwrap();
    assert_eq!(pool.stats().used, 4096);
    drop(full);
    assert_eq!(parent.stats(), occupied);
    drop(pressure);
    drop(pool);
    assert_eq!(parent.stats(), baseline);
}
