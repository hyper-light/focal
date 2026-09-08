use super::*;
use crate::lifecycle::memory as bytes;

fn request() -> MonitorDeadlineRequest {
    MonitorDeadlineRequest {
        id: MonitorId::from_u128(1),
        deadline: deadline(),
        fired_at: 100,
    }
}
fn build_limits() -> BuildLimits {
    BuildLimits {
        bytes: usize::MAX,
        visits: 4096,
    }
}

#[test]
fn earlier_monitor_deadline_requires_its_own_negative_scc_proof_to_expire() {
    let mut def = definition(1);
    def.deadline = Some(Deadline {
        at: 200,
        timer: TimerId::from_u128(50),
        generation: 1,
    });
    let mut owner = ClaimState::generate(Principal::Actor(ISSUER), def).unwrap();
    let target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let original = owner.clone();
    let graph = snapshot(&[&owner, &target]);
    let peers = [&target];
    let plan = bytes::fail_after(0, || {
        Registry::prepare_monitor_deadline(
            &owner,
            request(),
            &graph,
            &peers,
            cut(3),
            build_limits(),
        )
        .unwrap()
    });
    assert!(plan.construction_charge() > 0);
    let MonitorDeadlineDecision::Expire(expiry) = plan.resolve().unwrap() else {
        panic!("negative SCC")
    };
    assert_eq!(expiry.request(), request());
    assert_eq!(expiry.cut(), cut(3));
    let mut copied = owner.clone();
    assert!(
        copied
            .expire(&copied.binding(), deadline(), 100, cut(3))
            .is_err()
    );
    copied
        .expire_monitor(&copied.binding(), &expiry, &peers)
        .unwrap();
    assert_eq!(copied.status(), ClaimStatus::Expired);
    assert_eq!(
        copied.terminal_cut(),
        Some(claim::ClaimTerminalCut::Explicit(cut(3)))
    );
    assert_eq!(copied.deadline(), original.deadline());
    assert_eq!(copied.scopes(), original.scopes());
    assert_eq!(copied.receipt(), original.receipt());
    assert_eq!(copied.latest_response(), original.latest_response());
    assert_eq!(owner, original);
    assert_eq!(target.status(), ClaimStatus::Generated);
}

#[test]
fn exact_timer_identity_time_and_full_source_cut_are_required() {
    let mut owner = claim(1);
    let target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let graph = snapshot(&[&owner, &target]);
    let peers = [&target];
    for bad in [
        MonitorDeadlineRequest {
            id: MonitorId::from_u128(99),
            ..request()
        },
        MonitorDeadlineRequest {
            fired_at: 99,
            ..request()
        },
        MonitorDeadlineRequest {
            deadline: Deadline {
                generation: 2,
                ..deadline()
            },
            ..request()
        },
        MonitorDeadlineRequest {
            deadline: Deadline {
                at: 99,
                ..deadline()
            },
            ..request()
        },
    ] {
        assert!(
            Registry::prepare_monitor_deadline(&owner, bad, &graph, &peers, cut(3), build_limits())
                .is_err()
        );
    }
    assert!(
        Registry::prepare_monitor_deadline(&owner, request(), &graph, &[], cut(3), build_limits())
            .is_err()
    );
    assert!(
        Registry::prepare_monitor_deadline(
            &owner,
            request(),
            &graph,
            &peers,
            cut(1),
            build_limits()
        )
        .is_err()
    );
    let MonitorDeadlineDecision::Expire(expiry) = Registry::prepare_monitor_deadline(
        &owner,
        request(),
        &graph,
        &peers,
        cut(3),
        build_limits(),
    )
    .unwrap()
    .resolve()
    .unwrap() else {
        panic!("expiry")
    };
    let mut copy = owner.clone();
    assert!(copy.expire_monitor(&copy.binding(), &expiry, &[]).is_err());
    assert_eq!(copy, owner);
    cancel(&mut copy, 3);
    let after = copy.clone();
    assert!(
        copy.expire_monitor(&copy.binding(), &expiry, &peers)
            .is_err()
    );
    assert_eq!(copy, after);
}

#[test]
fn settled_or_inactive_monitor_timer_never_invents_an_expiry() {
    let mut owner = claim(1);
    let mut target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    cancel(&mut target, 3);
    let peers = [&target];
    let graph = snapshot(&[&owner, &target]);
    let zero = BuildLimits {
        bytes: 0,
        ..build_limits()
    };
    let plan = Registry::prepare_monitor_deadline(&owner, request(), &graph, &peers, cut(4), zero)
        .unwrap();
    assert_eq!(plan.construction_charge(), 0);
    assert!(matches!(
        bytes::fail_after(0, || plan.resolve()).unwrap(),
        MonitorDeadlineDecision::Settled
    ));
    let transition = Registry::prepare_release_monitor_bounded(
        &owner,
        request().id,
        &graph,
        &peers,
        cut(4),
        build_limits(),
    )
    .unwrap()
    .build()
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &peers)
        .unwrap();
    let graph = snapshot(&[&owner, &target]);
    assert!(matches!(
        Registry::prepare_monitor_deadline(&owner, request(), &graph, &peers, cut(5), zero)
            .unwrap()
            .resolve()
            .unwrap(),
        MonitorDeadlineDecision::Inactive
    ));
    cancel(&mut owner, 5);
    let graph = snapshot(&[&owner, &target]);
    assert!(matches!(
        Registry::prepare_monitor_deadline(&owner, request(), &graph, &peers, cut(6), zero)
            .unwrap()
            .resolve()
            .unwrap(),
        MonitorDeadlineDecision::Inactive
    ));
}

#[test]
fn scc_victim_precedes_expiry_and_an_earlier_claim_deadline_cannot_be_skipped() {
    let mut owner = claim(1);
    let mut target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let graph = snapshot(&[&owner, &target]);
    let roots = [WaitPredicate::Terminal(ClaimId::from_u128(1))];
    let transition = Registry::prepare_register_bounded(
        &target,
        authority(&target, 3),
        Registration {
            id: MonitorId::from_u128(2),
            ..registration(&roots)
        },
        &graph,
        &[&owner],
        build_limits(),
    )
    .unwrap()
    .build()
    .unwrap();
    target
        .apply_scope(&target.binding(), transition, &[&owner])
        .unwrap();
    let peers = [&target];
    let graph = snapshot(&[&owner, &target]);
    let MonitorDeadlineDecision::Deadlock(witness) = Registry::prepare_monitor_deadline(
        &owner,
        request(),
        &graph,
        &peers,
        cut(4),
        build_limits(),
    )
    .unwrap()
    .resolve()
    .unwrap() else {
        panic!("cycle")
    };
    assert_eq!(witness.victim().unwrap(), owner.binding());
    let mut copy = owner.clone();
    copy.break_deadlock(&copy.binding(), &witness, &peers, SessionSeq(4))
        .unwrap();
    assert_eq!(copy.status(), ClaimStatus::Deadlocked);
    assert_eq!(target.status(), ClaimStatus::Generated);

    let mut def = definition(4);
    def.deadline = Some(Deadline {
        at: 90,
        timer: TimerId::from_u128(50),
        generation: 1,
    });
    let mut earlier = ClaimState::generate(Principal::Actor(ISSUER), def).unwrap();
    let open = claim(5);
    register(
        &mut earlier,
        &open,
        &[WaitPredicate::Terminal(ClaimId::from_u128(5))],
    );
    let graph = snapshot(&[&earlier, &open]);
    assert_eq!(
        Registry::prepare_monitor_deadline(
            &earlier,
            request(),
            &graph,
            &[&open],
            cut(3),
            build_limits()
        )
        .unwrap()
        .resolve()
        .unwrap_err(),
        ContractError::InvalidCut
    );
}

#[test]
fn graph_allocation_and_visit_refusals_are_never_negative_cycle_evidence() {
    let mut owner = claim(1);
    let target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let graph = snapshot(&[&owner, &target]);
    let peers = [&target];
    let plan = Registry::prepare_monitor_deadline(
        &owner,
        request(),
        &graph,
        &peers,
        cut(3),
        build_limits(),
    )
    .unwrap();
    let charge = plan.construction_charge();
    assert!(matches!(
        Registry::prepare_monitor_deadline(
            &owner,
            request(),
            &graph,
            &peers,
            cut(3),
            BuildLimits {
                bytes: charge - 1,
                ..build_limits()
            }
        ),
        Err(ContractError::Capacity)
    ));
    for allowed in 0..4 {
        let plan = Registry::prepare_monitor_deadline(
            &owner,
            request(),
            &graph,
            &peers,
            cut(3),
            build_limits(),
        )
        .unwrap();
        assert!(matches!(
            bytes::fail_after(allowed, || plan.resolve()),
            Err(ContractError::Capacity)
        ));
    }
    let spent = build_limits().visits - plan.remaining_visits();
    let plan = Registry::prepare_monitor_deadline(
        &owner,
        request(),
        &graph,
        &peers,
        cut(3),
        BuildLimits {
            visits: spent,
            ..build_limits()
        },
    )
    .unwrap();
    assert!(matches!(plan.resolve(), Err(ContractError::Capacity)));
    assert_eq!(owner.status(), ClaimStatus::Generated);
}

#[test]
fn monitor_timer_source_and_scc_queries_share_the_actual_remaining_transaction_budget() {
    let mut owner = claim(1);
    let target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let graph = snapshot(&[&owner, &target]);
    let peers = [&target];
    let mut measured = graph::VisitBudget::new(4096);
    let plan = Registry::prepare_monitor_deadline_with_visits(
        &owner,
        request(),
        &graph,
        &peers,
        cut(3),
        build_limits(),
        &mut measured,
    )
    .unwrap();
    let preparation = 4096 - measured.remaining();
    let before = measured.remaining();
    assert!(matches!(
        plan.resolve_with_visits(&mut measured).unwrap(),
        MonitorDeadlineDecision::Expire(_)
    ));
    let query = before - measured.remaining();
    assert!(preparation > 0 && query > 0);
    let mut exact = graph::VisitBudget::new(preparation + query);
    let plan = Registry::prepare_monitor_deadline_with_visits(
        &owner,
        request(),
        &graph,
        &peers,
        cut(3),
        build_limits(),
        &mut exact,
    )
    .unwrap();
    assert_eq!(exact.remaining(), query);
    assert!(matches!(
        plan.resolve_with_visits(&mut exact).unwrap(),
        MonitorDeadlineDecision::Expire(_)
    ));
    assert_eq!(exact.remaining(), 0);
    assert!(
        Registry::prepare_monitor_deadline_with_visits(
            &owner,
            request(),
            &graph,
            &peers,
            cut(3),
            build_limits(),
            &mut exact,
        )
        .is_err()
    );
    assert_eq!(exact.remaining(), 0);

    let mut tight = graph::VisitBudget::new(preparation + query - 1);
    let plan = Registry::prepare_monitor_deadline_with_visits(
        &owner,
        request(),
        &graph,
        &peers,
        cut(3),
        build_limits(),
        &mut tight,
    )
    .unwrap();
    let before = tight.remaining();
    assert_eq!(
        plan.resolve_with_visits(&mut tight).unwrap_err(),
        ContractError::Capacity
    );
    assert!(tight.remaining() < before);
    assert_eq!(owner.status(), ClaimStatus::Generated);
    assert!(owner.scopes().iter().all(|scope| scope.active()));
}

#[test]
fn refused_monitor_source_checks_remain_charged_without_mutating_the_source() {
    let mut owner = claim(1);
    let target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let graph = snapshot(&[&owner, &target]);
    let peers = [&target];
    let mut visits = graph::VisitBudget::new(4096);
    let bad = MonitorDeadlineRequest {
        deadline: Deadline {
            generation: 2,
            ..deadline()
        },
        ..request()
    };
    assert_eq!(
        Registry::prepare_monitor_deadline_with_visits(
            &owner,
            bad,
            &graph,
            &peers,
            cut(3),
            build_limits(),
            &mut visits,
        )
        .unwrap_err(),
        ContractError::InvalidCut
    );
    let after = visits.remaining();
    assert!(after < 4096);
    assert_eq!(
        Registry::prepare_monitor_deadline_with_visits(
            &owner,
            bad,
            &graph,
            &peers,
            cut(3),
            build_limits(),
            &mut visits,
        )
        .unwrap_err(),
        ContractError::InvalidCut
    );
    assert_eq!(4096 - after, after - visits.remaining());
    assert_eq!(owner.status(), ClaimStatus::Generated);
    assert!(owner.scopes().iter().all(|scope| scope.active()));
}
