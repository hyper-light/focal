use super::*;

fn kind(stats: &BudgetStats, kind: BudgetKind) -> usize {
    stats.by_kind[kind as usize]
}
fn quiescent(budget: &MemoryBudget) {
    let stats = budget.stats();
    assert_eq!(stats.by_kind.iter().sum::<usize>(), stats.used);
    bounded(&stats);
    assert!(stats.ordinary_used <= stats.used);
}
fn bounded(stats: &BudgetStats) {
    // Individual counters are bounded during mutation; cross-counter equality
    // is asserted only at an acknowledged quiescent boundary.
    assert!(stats.used <= stats.limit);
    assert!(stats.ordinary_used <= stats.limit - stats.completion_reserve);
    assert!(stats.by_kind.iter().all(|bytes| *bytes <= stats.limit));
}

#[test]
fn metadata_only_source_grows_spends_trims_to_zero_and_reuses_its_immutable_ceiling() {
    let root = MemoryBudget::new(1_000_000, 100_000).unwrap();
    let unrelated = root
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, 37)
        .unwrap();
    let baseline = root.stats();
    let mut pool = root
        .elastic_funded_child(BudgetLane::Ordinary, 4096, 0)
        .unwrap();
    let metadata_only = root.stats();
    let metadata = metadata_only.used - baseline.used;
    assert!(metadata > 0);
    assert_eq!(kind(&metadata_only, BudgetKind::Reserved), metadata);
    assert_eq!((pool.funded_capacity(), pool.available()), (0, 0));
    assert_eq!(pool.budget().limit(), 4096);
    assert_eq!(pool.budget().reservation_limit(BudgetLane::Ordinary), 4096);
    assert!(
        pool.budget()
            .reserve(BudgetKind::Pages, BudgetLane::Completion, 1)
            .is_err()
    );
    pool.grow(0).unwrap();
    pool.trim_unused(0).unwrap();
    assert_eq!(root.stats(), metadata_only);

    pool.grow(2048).unwrap();
    let pages = pool
        .budget()
        .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 600)
        .unwrap();
    let control = pool
        .budget()
        .reserve(BudgetKind::Control, BudgetLane::Completion, 400)
        .unwrap();
    assert_eq!((pool.funded_capacity(), pool.available()), (2048, 1048));
    assert_eq!(pool.budget().stats().ordinary_used, 600);
    assert_eq!(
        root.stats().ordinary_used,
        metadata_only.ordinary_used + 2048
    );
    pool.trim_unused(1000).unwrap();
    assert_eq!((pool.funded_capacity(), pool.available()), (1048, 48));
    assert_eq!(root.stats().used, metadata_only.used + 1048);
    assert_eq!(kind(&root.stats(), BudgetKind::Reserved), metadata + 48);
    assert_eq!(pool.budget().limit(), 4096);
    drop(pages);
    drop(control);
    assert_eq!(pool.available(), 1048);
    pool.grow(3048).unwrap();
    let all = pool
        .budget()
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 4096)
        .unwrap();
    assert_eq!(pool.available(), 0);
    assert!(pool.grow(1).is_err());
    assert!(pool.trim_unused(1).is_err());
    drop(all);
    pool.trim_unused(4096).unwrap();
    assert_eq!(root.stats(), metadata_only);
    assert_eq!(pool.budget().stats().used, 0);
    assert_eq!(pool.budget().limit(), 4096);
    quiescent(pool.budget());
    drop(pool);
    assert_eq!(root.stats(), baseline);
    drop(unrelated);
    assert_eq!(root.stats().used, 0);
}

#[test]
fn failed_growth_and_exact_trim_preserve_every_counter_then_retry_succeeds() {
    let root = MemoryBudget::new(8192, 1024).unwrap();
    let tenant = root.child(16_384, 0).unwrap();
    let mut pool = tenant
        .elastic_funded_child(BudgetLane::Ordinary, 4096, 1024)
        .unwrap();
    let baseline = (root.stats(), tenant.stats(), pool.budget().stats());
    let pressure = root
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            root.limit() - root.stats().used,
        )
        .unwrap();
    let mut held = pool
        .budget()
        .reserve(BudgetKind::Pages, BudgetLane::Completion, 700)
        .unwrap()
        .commit();
    let occupied = (
        root.stats(),
        tenant.stats(),
        pool.budget().stats(),
        pool.funded_capacity(),
        pool.available(),
    );
    assert!(pool.grow(1).is_err());
    assert!(pool.grow(4096).is_err());
    assert!(pool.grow(usize::MAX).is_err());
    assert!(pool.trim_unused(325).is_err());
    assert!(pool.trim_unused(1025).is_err());
    assert!(pool.trim_unused(usize::MAX).is_err());
    assert!(held.split_off(701).is_err());
    assert!(held.shrink_to(701).is_err());
    assert_eq!(
        (
            root.stats(),
            tenant.stats(),
            pool.budget().stats(),
            pool.funded_capacity(),
            pool.available()
        ),
        occupied
    );
    // Parent pressure blocks only new backing, not reuse of previously held credit.
    held.shrink_to(400).unwrap();
    let recycled = pool
        .budget()
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 300)
        .unwrap();
    assert_eq!(pool.available(), 324);
    assert_eq!(root.stats().used, root.limit());
    drop(recycled);
    drop(held);
    drop(pressure);
    assert_eq!(
        (root.stats(), tenant.stats(), pool.budget().stats()),
        baseline
    );
    pool.grow(512).unwrap();
    pool.trim_unused(1536).unwrap();
    assert_eq!((pool.funded_capacity(), pool.available()), (0, 0));
    drop(pool);
    assert_eq!((root.stats().used, tenant.stats().used), (0, 0));
}

#[test]
fn mixed_normal_fixed_elastic_hierarchy_preserves_categories_and_funding_lanes() {
    let root = MemoryBudget::new(4_000_000, 500_000).unwrap();
    let tenant = root.child(3_000_000, 100_000).unwrap();
    let baseline = (root.stats(), tenant.stats());
    let mut outer = tenant
        .elastic_funded_child(BudgetLane::Ordinary, 512_000, 128_000)
        .unwrap();
    let top = (root.stats(), tenant.stats(), outer.budget().stats());
    let normal = outer.budget().child(256_000, 64_000).unwrap();
    let fixed = normal.funded_child(BudgetLane::Completion, 32_000).unwrap();
    let mut inner = fixed
        .elastic_funded_child(BudgetLane::Completion, 16_000, 4096)
        .unwrap();
    let below = inner.budget().child(32_000, 0).unwrap();
    for source in [&fixed, inner.budget(), &below] {
        let before = (
            root.stats(),
            outer.budget().stats(),
            fixed.stats(),
            inner.budget().stats(),
            below.stats(),
        );
        assert_eq!(source.reservation_limit(BudgetLane::Ordinary), 0);
        assert!(
            source
                .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 1)
                .is_err()
        );
        assert!(
            source
                .elastic_funded_child(BudgetLane::Ordinary, 1024, 0)
                .is_err()
        );
        assert_eq!(
            (
                root.stats(),
                outer.budget().stats(),
                fixed.stats(),
                inner.budget().stats(),
                below.stats()
            ),
            before
        );
    }
    let payload = below
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 2048)
        .unwrap();
    for source in [
        &root,
        &tenant,
        outer.budget(),
        &normal,
        &fixed,
        inner.budget(),
        &below,
    ] {
        assert_eq!(kind(&source.stats(), BudgetKind::Payload), 2048);
        quiescent(source);
    }
    assert_eq!(root.stats().used, top.0.used);
    assert_eq!(root.stats().ordinary_used, top.0.ordinary_used);
    assert_eq!(outer.budget().stats().ordinary_used, 0);
    let fixed_usage = outer.budget().stats().used;
    outer.trim_unused(outer.available()).unwrap();
    assert_eq!(
        (outer.funded_capacity(), outer.available()),
        (fixed_usage, 0)
    );
    let held_parent = root.stats();
    inner.grow(8192).unwrap();
    inner.trim_unused(10_000).unwrap();
    assert_eq!((inner.funded_capacity(), inner.available()), (2288, 240));
    // Resizing inside the fixed boundary changes its usage, not ancestor backing.
    assert_eq!(root.stats(), held_parent);
    assert_eq!(outer.budget().stats().used, fixed_usage);
    drop(payload);
    drop(below);
    inner.trim_unused(2288).unwrap();
    drop(inner);
    drop(fixed);
    drop(normal);
    assert_eq!(outer.budget().stats().used, 0);
    outer.trim_unused(outer.available()).unwrap();
    drop(outer);
    assert_eq!((root.stats(), tenant.stats()), baseline);
}

#[test]
fn elastic_inside_fixed_ordinary_backing_rolls_back_normal_child_lane_refusals() {
    let root = MemoryBudget::new(1_000_000, 100_000).unwrap();
    let fixed = root.funded_child(BudgetLane::Ordinary, 64_000).unwrap();
    let fixed_backing = root.stats();
    let mut elastic = fixed
        .elastic_funded_child(BudgetLane::Ordinary, 8192, 2048)
        .unwrap();
    let ordinary = elastic.budget().child(1024, 512).unwrap();
    let held = ordinary
        .reserve(BudgetKind::Control, BudgetLane::Completion, 700)
        .unwrap();
    let before = (
        root.stats(),
        fixed.stats(),
        elastic.budget().stats(),
        ordinary.stats(),
        elastic.available(),
    );
    assert!(
        ordinary
            .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 400)
            .is_err()
    );
    assert!(
        ordinary
            .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 513)
            .is_err()
    );
    assert_eq!(
        (
            root.stats(),
            fixed.stats(),
            elastic.budget().stats(),
            ordinary.stats(),
            elastic.available()
        ),
        before
    );
    elastic.trim_unused(1348).unwrap();
    assert_eq!(elastic.funded_capacity(), 700);
    assert_eq!(root.stats().used, fixed_backing.used);
    assert_eq!(root.stats().ordinary_used, fixed_backing.ordinary_used);
    drop(held);
    elastic.grow(500).unwrap();
    let ordinary_bytes = ordinary
        .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 512)
        .unwrap();
    assert_eq!(elastic.budget().stats().ordinary_used, 512);
    drop(ordinary_bytes);
    drop(ordinary);
    elastic.trim_unused(1200).unwrap();
    drop(elastic);
    assert_eq!(root.stats(), fixed_backing);
    drop(fixed);
    assert_eq!(root.stats().used, 0);
}

#[test]
fn dropped_controller_keeps_backing_through_source_descendant_and_zero_byte_split() {
    let root = MemoryBudget::new(1_000_000, 0).unwrap();
    let baseline = root.stats();
    let pool = root
        .elastic_funded_child(BudgetLane::Ordinary, 4096, 2048)
        .unwrap();
    let backed = root.stats();
    let source = pool.budget().clone();
    let descendant = source.child(4096, 0).unwrap();
    let mut payload = descendant
        .reserve(BudgetKind::Payload, BudgetLane::Completion, 512)
        .unwrap()
        .commit();
    let zero = payload.split_off(0).unwrap();
    drop(pool);
    assert_eq!(root.stats().used, backed.used);
    drop(source);
    drop(descendant);
    assert_eq!(kind(&root.stats(), BudgetKind::Payload), 512);
    drop(payload);
    // A zero-byte Allocation is still a source handle. Its final drop must
    // release the exact aggregate once, despite having no issued debit itself.
    assert_eq!(root.stats(), backed);
    drop(zero);
    assert_eq!(root.stats(), baseline);

    let mut pool = root
        .elastic_funded_child(BudgetLane::Completion, 4096, 2048)
        .unwrap();
    let zero = pool
        .budget()
        .reserve(BudgetKind::Control, BudgetLane::Completion, 0)
        .unwrap();
    pool.trim_unused(2048).unwrap();
    let metadata_only = root.stats();
    assert!(metadata_only.used > 0);
    assert_eq!(metadata_only.ordinary_used, 0);
    drop(pool);
    assert_eq!(root.stats(), metadata_only);
    drop(zero);
    assert_eq!(root.stats(), baseline);
}

#[test]
fn invalid_creation_overflow_and_shared_depth_limit_leave_no_backing_debit() {
    let root = MemoryBudget::new(16_000_000, 0).unwrap();
    let baseline = root.stats();
    for (ceiling, initial) in [(0, 0), (1, 2), (usize::MAX, usize::MAX)] {
        assert!(
            root.elastic_funded_child(BudgetLane::Ordinary, ceiling, initial)
                .is_err()
        );
        assert_eq!(root.stats(), baseline);
    }
    let mut huge = root
        .elastic_funded_child(BudgetLane::Ordinary, usize::MAX, 0)
        .unwrap();
    let metadata_only = root.stats();
    assert!(huge.grow(usize::MAX).is_err());
    assert_eq!(root.stats(), metadata_only);
    assert_eq!((huge.funded_capacity(), huge.available()), (0, 0));
    drop(huge);
    assert_eq!(root.stats(), baseline);

    let mut deepest = root.clone();
    let mut controllers = Vec::new();
    let mut capacity = 8_000_000;
    for depth in 2..=8 {
        capacity /= 2;
        deepest = match depth % 3 {
            0 => deepest.child(capacity, 0).unwrap(),
            1 => deepest
                .funded_child(BudgetLane::Ordinary, capacity)
                .unwrap(),
            _ => {
                let pool = deepest
                    .elastic_funded_child(BudgetLane::Ordinary, capacity, capacity)
                    .unwrap();
                let source = pool.budget().clone();
                controllers.push(pool);
                source
            }
        };
    }
    let before = (root.stats(), deepest.stats());
    assert!(
        deepest
            .elastic_funded_child(BudgetLane::Completion, 1, 0)
            .is_err()
    );
    assert!(deepest.funded_child(BudgetLane::Completion, 1).is_err());
    assert!(deepest.child(1, 0).is_err());
    assert_eq!((root.stats(), deepest.stats()), before);
    let held = deepest
        .reserve(BudgetKind::Timer, BudgetLane::Completion, 64)
        .unwrap();
    drop(controllers);
    drop(deepest);
    assert_eq!(root.stats().used, before.0.used);
    assert_eq!(kind(&root.stats(), BudgetKind::Timer), 64);
    drop(held);
    assert_eq!(root.stats(), baseline);
}

#[test]
fn sibling_pools_cannot_absorb_or_trim_each_others_issued_credit() {
    let root = MemoryBudget::new(1_000_000, 0).unwrap();
    let mut first = root
        .elastic_funded_child(BudgetLane::Ordinary, 4096, 1024)
        .unwrap();
    let mut second = root
        .elastic_funded_child(BudgetLane::Ordinary, 4096, 1024)
        .unwrap();
    let mut a = first
        .budget()
        .reserve(BudgetKind::Pages, BudgetLane::Completion, 1024)
        .unwrap()
        .commit();
    let mut b = second
        .budget()
        .reserve(BudgetKind::Pages, BudgetLane::Completion, 1024)
        .unwrap()
        .commit();
    let occupied = (
        root.stats(),
        first.budget().stats(),
        second.budget().stats(),
    );
    assert!(a.absorb(&mut b).is_err());
    assert!(first.trim_unused(1).is_err());
    assert!(second.trim_unused(1).is_err());
    assert_eq!((a.bytes(), b.bytes()), (1024, 1024));
    assert_eq!(
        (
            root.stats(),
            first.budget().stats(),
            second.budget().stats()
        ),
        occupied
    );
    let mut half = a.split_off(512).unwrap();
    a.absorb(&mut half).unwrap();
    drop(half);
    assert_eq!(first.available(), 0);
    drop(a);
    first.trim_unused(1024).unwrap();
    assert_eq!(second.available(), 0);
    assert_eq!(kind(&root.stats(), BudgetKind::Pages), 1024);
    drop(b);
    second.trim_unused(1024).unwrap();
    drop(first);
    drop(second);
    assert_eq!(root.stats().used, 0);
}

#[test]
fn concurrent_mixed_kind_spending_and_controller_resize_preserve_exact_round_balances() {
    use std::sync::mpsc;
    use std::time::Duration;
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Step {
        Hold,
        Churn,
        Release,
    }
    let root = MemoryBudget::new(1_000_000, 100_000).unwrap();
    let mut pool = root
        .elastic_funded_child(BudgetLane::Ordinary, 1024, 512)
        .unwrap();
    let initial = root.stats();
    let kinds = [
        BudgetKind::Pages,
        BudgetKind::Payload,
        BudgetKind::Roots,
        BudgetKind::Pending,
    ];
    std::thread::scope(|scope| {
        let (done, acknowledgements) = mpsc::sync_channel(4);
        let mut commands = Vec::new();
        let mut handles = Vec::new();
        for (index, kind) in kinds.into_iter().enumerate() {
            let (send, receive) = mpsc::sync_channel(1);
            commands.push(send);
            let done = done.clone();
            let source = pool.budget().clone();
            handles.push(scope.spawn(move || {
                let lane = if index.is_multiple_of(2) {
                    BudgetLane::Ordinary
                } else {
                    BudgetLane::Completion
                };
                let mut held: Option<Allocation> = None;
                let mut zero = None;
                while let Ok(step) = receive.recv() {
                    match step {
                        Step::Hold => {
                            assert!(held.is_none());
                            let mut allocation = source.reserve(kind, lane, 128).unwrap().commit();
                            zero = Some(allocation.split_off(0).unwrap());
                            held = Some(allocation);
                        }
                        Step::Churn => {
                            let held = held.as_mut().unwrap();
                            for _ in 0..64 {
                                let mut half = held.split_off(64).unwrap();
                                held.shrink_to(32).unwrap();
                                let mut refill = source.reserve(kind, lane, 32).unwrap().commit();
                                held.absorb(&mut refill).unwrap();
                                half.shrink_to(32).unwrap();
                                let mut refill = source.reserve(kind, lane, 32).unwrap().commit();
                                half.absorb(&mut refill).unwrap();
                                held.absorb(&mut half).unwrap();
                                assert_eq!(held.bytes(), 128);
                            }
                        }
                        Step::Release => {
                            drop(held.take());
                            drop(zero.take());
                        }
                    }
                    if done.send((index, step)).is_err() {
                        break;
                    }
                }
            }));
        }
        drop(done);
        let await_all = |expected| {
            let mut seen = [false; 4];
            for _ in 0..4 {
                let (worker, step) = acknowledgements
                    .recv_timeout(Duration::from_secs(30))
                    .unwrap();
                assert_eq!(step, expected);
                assert!(!seen[worker]);
                seen[worker] = true;
            }
        };
        for _ in 0..16 {
            for command in &commands {
                command.send(Step::Hold).unwrap();
            }
            await_all(Step::Hold);
            assert_eq!((pool.budget().stats().used, pool.available()), (512, 0));
            assert_eq!(pool.budget().stats().ordinary_used, 256);
            let occupied = root.stats();
            assert!(pool.trim_unused(1).is_err());
            assert_eq!(root.stats(), occupied);
            pool.grow(128).unwrap();
            pool.trim_unused(64).unwrap();
            for command in &commands {
                command.send(Step::Churn).unwrap();
            }
            // Workers never own over128 bytes each. The extra64 idle bytes
            // make every resize valid while credit refunds/acquisitions race it.
            for _ in 0..128 {
                pool.grow(64).unwrap();
                pool.trim_unused(64).unwrap();
                bounded(&root.stats());
                bounded(&pool.budget().stats());
            }
            await_all(Step::Churn);
            assert_eq!((pool.funded_capacity(), pool.available()), (576, 64));
            for kind in kinds {
                assert_eq!(kind_bytes(pool.budget(), kind), 128);
                assert_eq!(kind_bytes(&root, kind), 128);
            }
            quiescent(&root);
            quiescent(pool.budget());
            pool.trim_unused(64).unwrap();
            for command in &commands {
                command.send(Step::Release).unwrap();
            }
            await_all(Step::Release);
            assert_eq!((pool.funded_capacity(), pool.available()), (512, 512));
            assert_eq!(pool.budget().stats().used, 0);
            assert_eq!(root.stats(), initial);
        }
        drop(commands);
        for handle in handles {
            handle.join().unwrap();
        }
    });
    pool.trim_unused(512).unwrap();
    drop(pool);
    assert_eq!(root.stats().used, 0);
    fn kind_bytes(source: &MemoryBudget, category: BudgetKind) -> usize {
        kind(&source.stats(), category)
    }
}
