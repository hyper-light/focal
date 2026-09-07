use super::*;
use crate::lifecycle::memory as bytes;

#[test]
fn exact_capture_preflight_allocates_nothing_and_start_rechecks_live_bindings() {
    let mut a = claim(1, 1, &[(Kind::Awaits, 2)]);
    let mut b = claim(2, 2, &[]);
    post(&mut a);
    cancel(&mut b, 9);
    let claims = [&a, &b];
    let plan = bytes::fail_after(0, || {
        Snapshot::prepare_capture(&claims, limits(), usize::MAX)
    })
    .unwrap();
    assert_eq!(plan.construction_heap_allocations(), 4);
    let charge = plan.construction_charge();
    let heap = plan.construction_heap_bytes();
    assert_eq!(
        charge,
        size_of::<Snapshot>() + heap + 4 * 4 * size_of::<usize>()
    );
    bytes::fail_after(8, || {
        assert!(matches!(
            Snapshot::prepare_capture(&claims, limits(), charge - 1),
            Err(ContractError::Capacity)
        ));
        assert_eq!(bytes::remaining_allocations(), Some(8));
    });
    let snapshot = Snapshot::prepare_capture(&claims, limits(), charge)
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(snapshot.retained_charge().unwrap(), charge);
    assert_eq!(snapshot.retained_heap_bytes().unwrap(), heap);
    assert_eq!(
        snapshot.retained_bytes().unwrap(),
        size_of::<Snapshot>() + heap
    );
    assert_eq!(snapshot.heap_allocations().unwrap(), 4);
    bytes::fail_after(0, || {
        assert_eq!(
            snapshot.check_cut(SessionSeq(8)),
            Err(ContractError::InvalidCut)
        );
        snapshot.check_cut(SessionSeq(9)).unwrap();
        snapshot.start(cid(1)).unwrap().check(&a, &[&b]).unwrap();
        snapshot.check_owner(&a, &[&b]).unwrap();
    });
    let original_binding = a.binding();
    cancel(&mut a, 10);
    assert_eq!(snapshot.binding(cid(1)).unwrap(), original_binding);
    assert_eq!(
        snapshot.start(cid(1)).unwrap().check(&a, &[&b]),
        Err(ContractError::StaleRevision)
    );
}

#[test]
fn every_capture_allocation_failure_preserves_inputs_and_complete_retry() {
    let mut a = claim(1, 1, &[(Kind::DependsOn, 2)]);
    let mut b = claim(2, 2, &[(Kind::DependsOn, 3)]);
    let mut c = claim(3, 3, &[]);
    local(&mut a);
    local(&mut b);
    local(&mut c);
    let claims = [&a, &b, &c];
    let original = claims.map(|claim| claim.binding());
    let expected = Snapshot::capture(&claims, limits()).unwrap();
    for after in 0..4 {
        let plan = Snapshot::prepare_capture(&claims, limits(), usize::MAX).unwrap();
        let charge = plan.construction_charge();
        assert!(
            matches!(
                bytes::fail_after(after, || plan.build()),
                Err(ContractError::Capacity)
            ),
            "allocation {after}"
        );
        assert_eq!(claims.map(|claim| claim.binding()), original);
        let retry = Snapshot::prepare_capture(&claims, limits(), charge)
            .unwrap()
            .build()
            .unwrap();
        for id in [cid(1), cid(2), cid(3)] {
            assert!(retry.satisfied(id).unwrap());
            assert_eq!(
                retry.satisfied(id).unwrap(),
                expected.satisfied(id).unwrap()
            );
            assert_eq!(retry.binding(id).unwrap(), expected.binding(id).unwrap());
        }
        assert_eq!(retry.retained_charge().unwrap(), charge);
    }
}

#[test]
fn allocator_excess_is_refused_and_actual_empty_buffer_capacities_are_counted() {
    let a = claim(1, 1, &[]);
    let claims = [&a];
    let plan = Snapshot::prepare_capture(&claims, limits(), usize::MAX).unwrap();
    assert_eq!(plan.construction_heap_allocations(), 2);
    let charge = plan.construction_charge();
    assert!(matches!(
        super::super::capture::with_excess_capacity(|| plan.build()),
        Err(ContractError::Capacity)
    ));
    let mut snapshot = Snapshot::prepare_capture(&claims, limits(), charge)
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(snapshot.retained_charge().unwrap(), charge);
    assert_eq!(snapshot.heap_allocations().unwrap(), 2);
    snapshot.nodes.reserve_exact(8);
    snapshot.edges.reserve_exact(8);
    snapshot.incoming.reserve_exact(8);
    snapshot.satisfied.reserve_exact(8);
    assert!(snapshot.edges.is_empty());
    assert!(snapshot.incoming.is_empty());
    assert_eq!(snapshot.heap_allocations().unwrap(), 4);
    assert!(snapshot.retained_charge().unwrap() > charge);
    assert_eq!(
        snapshot.retained_charge().unwrap(),
        snapshot.retained_bytes().unwrap() + 4 * 4 * size_of::<usize>()
    );
    assert_eq!(snapshot.binding(cid(1)).unwrap(), a.binding());
}

#[test]
fn active_monitor_roots_count_in_capture_allowance_and_never_become_failure_edges() {
    let mut a = claim(1, 1, &[]);
    let mut b = claim(2, 2, &[]);
    post(&mut a);
    post(&mut b);
    register(&mut a, &b, WaitPredicate::Satisfied(cid(2)), 100);
    register(&mut a, &b, WaitPredicate::Terminal(cid(2)), 101);
    register(&mut a, &b, WaitPredicate::Released(cid(2)), 102);
    cancel(&mut b, 30);
    let claims = [&a, &b];
    bytes::fail_after(9, || {
        assert!(matches!(
            Snapshot::prepare_capture(
                &claims,
                Limits {
                    edges: 2,
                    ..limits()
                },
                usize::MAX
            ),
            Err(ContractError::Capacity)
        ));
        assert_eq!(bytes::remaining_allocations(), Some(9));
    });
    let plan = Snapshot::prepare_capture(
        &claims,
        Limits {
            edges: 3,
            ..limits()
        },
        usize::MAX,
    )
    .unwrap();
    let charge = plan.construction_charge();
    let snapshot = plan.build().unwrap();
    assert_eq!(snapshot.edges.len(), 3);
    assert_eq!(snapshot.incoming.len(), 3);
    assert_eq!(snapshot.retained_charge().unwrap(), charge);
    assert!(
        snapshot
            .wait_settled(WaitPredicate::Terminal(cid(2)))
            .unwrap()
    );
    assert!(
        !snapshot
            .wait_settled(WaitPredicate::Satisfied(cid(2)))
            .unwrap()
    );
    assert!(
        !snapshot
            .wait_settled(WaitPredicate::Released(cid(2)))
            .unwrap()
    );
    assert!(matches!(
        snapshot.start(cid(1)),
        Err(ContractError::InvalidTransition)
    ));
    assert!(matches!(
        snapshot.dependency_failure(cid(1)),
        Err(ContractError::InvalidTransition)
    ));
    let missing = [&a];
    assert!(matches!(
        Snapshot::prepare_capture(&missing, limits(), usize::MAX)
            .unwrap()
            .build(),
        Err(ContractError::InvalidTarget)
    ));
}

#[test]
fn plan_preserves_visit_limits_and_refuses_unfounded_local_cycle_satisfaction() {
    let mut a = claim(1, 1, &[(Kind::DependsOn, 1)]);
    local(&mut a);
    let claims = [&a];
    let short = Limits {
        visits: 3,
        ..limits()
    };
    assert!(matches!(
        Snapshot::prepare_capture(&claims, short, usize::MAX)
            .unwrap()
            .build(),
        Err(ContractError::Capacity)
    ));
    assert!(matches!(
        Snapshot::capture(&claims, short),
        Err(ContractError::Capacity)
    ));
    let exact = Limits {
        visits: 4,
        ..limits()
    };
    let plan = Snapshot::prepare_capture(&claims, exact, usize::MAX).unwrap();
    let charge = plan.construction_charge();
    let snapshot = plan.build().unwrap();
    assert!(!snapshot.satisfied(cid(1)).unwrap());
    assert!(
        !Snapshot::capture(&claims, exact)
            .unwrap()
            .satisfied(cid(1))
            .unwrap()
    );
    assert_eq!(snapshot.retained_charge().unwrap(), charge);
}
