use super::*;
use crate::lifecycle::memory as bytes;

fn build_limits() -> BuildLimits {
    BuildLimits {
        bytes: usize::MAX,
        visits: usize::MAX,
    }
}

#[test]
fn registration_quotes_all_allocations_before_build_and_keeps_complete_source_authority() {
    let mut owner = claim(1);
    let target = claim(2);
    let graph = snapshot(&[&owner, &target]);
    let peers = [&target];
    let roots = [
        WaitPredicate::Released(ClaimId::from_u128(2)),
        WaitPredicate::Terminal(ClaimId::from_u128(2)),
        WaitPredicate::Terminal(ClaimId::from_u128(2)),
    ];
    let request = registration(&roots);
    let auth = authority(&owner, 2);
    let quote = bytes::fail_after(0, || {
        let plan = Registry::prepare_register_bounded(
            &owner,
            auth,
            request,
            &graph,
            &peers,
            build_limits(),
        )
        .unwrap();
        assert_eq!(bytes::remaining_allocations(), Some(0));
        plan
    });
    let exact = BuildLimits {
        bytes: quote.construction_charge(),
        visits: quote.visits(),
    };
    for limited in [
        BuildLimits {
            bytes: exact.bytes - 1,
            ..exact
        },
        BuildLimits {
            visits: exact.visits - 1,
            ..exact
        },
    ] {
        assert_eq!(
            Registry::prepare_register_bounded(&owner, auth, request, &graph, &peers, limited)
                .unwrap_err(),
            ContractError::Capacity
        );
    }
    // New scope vector, requested roots, and complete binding-read vector.
    for allowed in 0..3 {
        let plan = Registry::prepare_register_bounded(&owner, auth, request, &graph, &peers, exact)
            .unwrap();
        assert!(matches!(
            bytes::fail_after(allowed, || plan.build()),
            Err(ContractError::Capacity)
        ));
        assert_eq!(owner.binding().revision, ObjectRevision(1));
    }
    let plan =
        Registry::prepare_register_bounded(&owner, auth, request, &graph, &peers, exact).unwrap();
    assert!(matches!(
        release::with_excess_capacity(|| plan.build()),
        Err(ContractError::Capacity)
    ));
    let transition =
        Registry::prepare_register_bounded(&owner, auth, request, &graph, &peers, exact)
            .unwrap()
            .build()
            .unwrap();
    assert_eq!(transition.construction_charge().unwrap(), exact.bytes);
    let before = owner.clone();
    owner
        .apply_scope(&before.binding(), transition, &peers)
        .unwrap();
    let scope = owner.scopes().iter().next().unwrap();
    assert_eq!(
        scope.roots(),
        &[
            WaitPredicate::Terminal(ClaimId::from_u128(2)),
            WaitPredicate::Released(ClaimId::from_u128(2))
        ]
    );
    assert_eq!(scope.registered(), SessionSeq(2));
    assert_eq!(scope.released(), None);
    assert_eq!(owner.status(), before.status());
    assert_eq!(owner.graph(), before.graph());
    assert_eq!(owner.receipt(), before.receipt());
    assert_eq!(owner.latest_response(), before.latest_response());
    assert_eq!(owner.binding(), before.binding().next().unwrap());
}

#[test]
fn bounded_registration_rejects_stale_graph_incomplete_peers_and_untrusted_authority() {
    let mut owner = claim(1);
    let target = claim(2);
    let graph = snapshot(&[&owner, &target]);
    let peers = [&target];
    let roots = [WaitPredicate::Terminal(ClaimId::from_u128(2))];
    let request = registration(&roots);
    let auth = authority(&owner, 2);
    for invalid in [
        Authority {
            principal: Principal::Node(ISSUER),
            ..auth
        },
        Authority {
            expected: auth.expected.next().unwrap(),
            ..auth
        },
        Authority {
            receipt: Some(ReceiptFence {
                receipt: ReceiptId::from_u128(8),
                epoch: 1,
            }),
            ..auth
        },
        Authority {
            cut: ClaimCut {
                cause: ContentHash([0; 32]),
                ..auth.cut
            },
            ..auth
        },
    ] {
        assert!(
            Registry::prepare_register_bounded(
                &owner,
                invalid,
                request,
                &graph,
                &peers,
                build_limits()
            )
            .is_err()
        );
    }
    assert!(
        Registry::prepare_register_bounded(&owner, auth, request, &graph, &[], build_limits())
            .is_err()
    );
    for deadline in [
        Deadline {
            at: auth.now,
            ..deadline()
        },
        Deadline {
            generation: 0,
            ..deadline()
        },
        Deadline {
            timer: TimerId::from_u128(0),
            ..deadline()
        },
    ] {
        assert!(
            Registry::prepare_register_bounded(
                &owner,
                auth,
                Registration {
                    deadline,
                    ..request
                },
                &graph,
                &peers,
                build_limits()
            )
            .is_err()
        );
    }
    let unknown = [WaitPredicate::Terminal(ClaimId::from_u128(99))];
    assert!(
        Registry::prepare_register_bounded(
            &owner,
            auth,
            registration(&unknown),
            &graph,
            &peers,
            build_limits()
        )
        .is_err()
    );
    let transition =
        Registry::prepare_register_bounded(&owner, auth, request, &graph, &peers, build_limits())
            .unwrap()
            .build()
            .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &peers)
        .unwrap();
    let changed = Registration {
        id: MonitorId::from_u128(2),
        ..request
    };
    assert_eq!(
        Registry::prepare_register_bounded(
            &owner,
            authority(&owner, 3),
            changed,
            &graph,
            &peers,
            build_limits()
        )
        .unwrap_err(),
        ContractError::StaleRevision
    );
}

fn successor() -> ClaimState {
    let mut def = definition(3);
    def.created = SessionSeq(3);
    def.lineage = succession::Lineage::new(
        def.binding,
        Cause::Root(RootCommandId::from_u128(3)),
        &[succession::Correction {
            kind: CorrectionKind::Supersedes,
            predecessor: ObjectRef::claim(def.binding.ledger, ClaimId::from_u128(2)),
        }],
        1,
    )
    .unwrap();
    ClaimState::generate(Principal::Actor(ISSUER), def).unwrap()
}

#[test]
fn bounded_named_rebind_preserves_predicate_kind_and_original_registration() {
    let mut owner = claim(1);
    let predecessor = claim(2);
    register(
        &mut owner,
        &predecessor,
        &[
            WaitPredicate::Satisfied(ClaimId::from_u128(2)),
            WaitPredicate::Released(ClaimId::from_u128(2)),
        ],
    );
    let successor = successor();
    let peers = [&predecessor, &successor];
    let graph = snapshot(&[&owner, &predecessor, &successor]);
    let request = RebindRequest {
        id: MonitorId::from_u128(1),
        predecessor: &predecessor,
        successor: &successor,
    };
    let plan = bytes::fail_after(0, || {
        Registry::prepare_rebind_bounded(
            &owner,
            authority(&owner, 4),
            request,
            &graph,
            &peers,
            build_limits(),
        )
        .unwrap()
    });
    let exact = BuildLimits {
        bytes: plan.construction_charge(),
        visits: plan.visits(),
    };
    for allowed in 0..3 {
        let plan = Registry::prepare_rebind_bounded(
            &owner,
            authority(&owner, 4),
            request,
            &graph,
            &peers,
            exact,
        )
        .unwrap();
        assert!(matches!(
            bytes::fail_after(allowed, || plan.build()),
            Err(ContractError::Capacity)
        ));
    }
    let transition = Registry::prepare_rebind_bounded(
        &owner,
        authority(&owner, 4),
        request,
        &graph,
        &peers,
        exact,
    )
    .unwrap()
    .build()
    .unwrap();
    assert_eq!(transition.construction_charge().unwrap(), exact.bytes);
    owner
        .apply_scope(&owner.binding(), transition, &peers)
        .unwrap();
    let scope = owner.scopes().iter().next().unwrap();
    assert_eq!(
        scope.roots(),
        &[
            WaitPredicate::Satisfied(ClaimId::from_u128(3)),
            WaitPredicate::Released(ClaimId::from_u128(3))
        ]
    );
    assert_eq!(scope.registered(), SessionSeq(2));
    assert_eq!(
        scope.last_rebinding(),
        Some(Rebinding {
            predecessor: ClaimId::from_u128(2),
            successor: ClaimId::from_u128(3),
            cut: cut(4)
        })
    );
    assert_eq!(scope.deadline(), deadline());
    assert_eq!(scope.released(), None);
    assert_eq!(predecessor.binding().revision, ObjectRevision(1));
}

#[test]
fn rebind_refuses_an_unrelated_successor_and_does_not_spend_allocation_capacity() {
    let mut owner = claim(1);
    let predecessor = claim(2);
    register(
        &mut owner,
        &predecessor,
        &[WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let unrelated = claim(3);
    let peers = [&predecessor, &unrelated];
    let graph = snapshot(&[&owner, &predecessor, &unrelated]);
    let before = owner.clone();
    assert!(matches!(
        bytes::fail_after(0, || Registry::prepare_rebind_bounded(
            &owner,
            authority(&owner, 3),
            RebindRequest {
                id: MonitorId::from_u128(1),
                predecessor: &predecessor,
                successor: &unrelated
            },
            &graph,
            &peers,
            build_limits()
        )),
        Err(ContractError::InvalidTarget)
    ));
    assert_eq!(owner, before);
}

#[test]
fn monitor_release_keeps_terminal_satisfied_and_released_predicates_distinct() {
    for (root, allowed) in [
        (WaitPredicate::Terminal(ClaimId::from_u128(2)), true),
        (WaitPredicate::Satisfied(ClaimId::from_u128(2)), false),
        (WaitPredicate::Released(ClaimId::from_u128(2)), false),
    ] {
        let mut owner = claim(1);
        let mut target = claim(2);
        register(&mut owner, &target, &[root]);
        cancel(&mut target, 3);
        cancel(&mut owner, 4);
        let peers = [&target];
        let graph = snapshot(&[&owner, &target]);
        let plan = Registry::prepare_release_monitor_bounded(
            &owner,
            MonitorId::from_u128(1),
            &graph,
            &peers,
            cut(5),
            build_limits(),
        );
        if allowed {
            let plan = plan.unwrap();
            let exact = BuildLimits {
                bytes: plan.construction_charge(),
                visits: plan.visits(),
            };
            let transition = Registry::prepare_release_monitor_bounded(
                &owner,
                MonitorId::from_u128(1),
                &graph,
                &peers,
                cut(5),
                exact,
            )
            .unwrap()
            .build()
            .unwrap();
            let terminal = owner.terminal_cut();
            owner
                .apply_scope(&owner.binding(), transition, &peers)
                .unwrap();
            assert_eq!(owner.terminal_cut(), terminal);
            assert_eq!(
                owner.scopes().iter().next().unwrap().release_cut(),
                Some(cut(5))
            );
            let graph = snapshot(&[&owner, &target]);
            assert!(
                Registry::prepare_release_monitor_bounded(
                    &owner,
                    MonitorId::from_u128(1),
                    &graph,
                    &peers,
                    cut(6),
                    build_limits()
                )
                .is_err()
            );
        } else {
            assert_eq!(plan.unwrap_err(), ContractError::InvalidTransition);
        }
    }
}

#[test]
fn terminal_owner_can_cancel_an_impossible_wait_without_recording_success() {
    let mut owner = claim(1);
    let mut target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Satisfied(ClaimId::from_u128(2))],
    );
    cancel(&mut target, 3);
    cancel(&mut owner, 4);
    let original = owner.clone();
    let graph = snapshot(&[&owner, &target]);
    let peers = [&target];
    assert!(
        Registry::prepare_release_owner_bounded(
            &owner,
            &graph,
            &peers,
            cut(5),
            usize::MAX,
            usize::MAX
        )
        .is_err()
    );
    assert!(
        Registry::prepare_release_monitor_bounded(
            &owner,
            MonitorId::from_u128(1),
            &graph,
            &peers,
            cut(5),
            build_limits()
        )
        .is_err()
    );
    let plan = bytes::fail_after(0, || {
        Registry::prepare_cancel_monitor_bounded(
            &owner,
            authority(&owner, 5),
            MonitorId::from_u128(1),
            &graph,
            &peers,
            build_limits(),
        )
        .unwrap()
    });
    let exact = BuildLimits {
        bytes: plan.construction_charge(),
        visits: plan.visits(),
    };
    let expected = MonitorCancellation {
        terminal: SessionSeq(4),
        cut: cut(5),
    };
    assert_eq!(
        plan.event(),
        Event::MonitorCancelled {
            id: MonitorId::from_u128(1),
            cancellation: expected
        }
    );
    for allowed in 0..3 {
        let plan = Registry::prepare_cancel_monitor_bounded(
            &owner,
            authority(&owner, 5),
            MonitorId::from_u128(1),
            &graph,
            &peers,
            exact,
        )
        .unwrap();
        assert!(matches!(
            bytes::fail_after(allowed, || plan.build()),
            Err(ContractError::Capacity)
        ));
        assert_eq!(owner, original);
    }
    let transition = Registry::prepare_cancel_monitor_bounded(
        &owner,
        authority(&owner, 5),
        MonitorId::from_u128(1),
        &graph,
        &peers,
        exact,
    )
    .unwrap()
    .build()
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &peers)
        .unwrap();
    let scope = owner.scopes().iter().next().unwrap();
    assert!(!scope.active());
    assert_eq!(scope.cancellation(), Some(expected));
    assert_eq!(scope.release_cut(), None);
    assert_eq!(
        scope.roots(),
        original.scopes().iter().next().unwrap().roots()
    );
    assert_eq!(owner.status(), original.status());
    assert_eq!(owner.terminal_cut(), original.terminal_cut());
    assert_eq!(owner.receipt(), original.receipt());
    assert_eq!(owner.latest_response(), original.latest_response());
    assert!(!owner.released());
    // Cancelled roots no longer require retaining the target's graph interior.
    // Their original predicates and provenance remain on the monitor itself.
    let graph = snapshot(&[&owner]);
    assert_eq!(
        graph.check_cut(SessionSeq(4)),
        Err(ContractError::InvalidCut)
    );
    graph.check_cut(SessionSeq(5)).unwrap();
    let transition = Registry::prepare_release_owner_bounded(
        &owner,
        &graph,
        &[],
        cut(6),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &[])
        .unwrap();
    assert!(owner.released());
    assert_eq!(
        owner.scopes().iter().next().unwrap().cancellation(),
        Some(expected)
    );
    assert_eq!(owner.terminal_cut(), original.terminal_cut());
    assert_eq!(target.status(), ClaimStatus::Cancelled);
}

#[test]
fn monitor_cancellation_requires_terminal_owner_issuer_and_exact_source_and_is_once_only() {
    let mut owner = claim(1);
    let target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Released(ClaimId::from_u128(2))],
    );
    let peers = [&target];
    let graph = snapshot(&[&owner, &target]);
    assert!(
        Registry::prepare_cancel_monitor_bounded(
            &owner,
            authority(&owner, 3),
            MonitorId::from_u128(1),
            &graph,
            &peers,
            build_limits()
        )
        .is_err()
    );
    cancel(&mut owner, 3);
    let old_graph = graph;
    let graph = snapshot(&[&owner, &target]);
    let valid = authority(&owner, 4);
    for invalid in [
        Authority {
            principal: Principal::Node(ISSUER),
            ..valid
        },
        Authority {
            principal: Principal::Actor(ParticipantId::from_u128(9)),
            ..valid
        },
        Authority {
            expected: valid.expected.next().unwrap(),
            ..valid
        },
        Authority {
            cut: cut(2),
            ..valid
        },
    ] {
        assert!(
            Registry::prepare_cancel_monitor_bounded(
                &owner,
                invalid,
                MonitorId::from_u128(1),
                &graph,
                &peers,
                build_limits()
            )
            .is_err()
        );
    }
    assert!(
        Registry::prepare_cancel_monitor_bounded(
            &owner,
            valid,
            MonitorId::from_u128(1),
            &old_graph,
            &peers,
            build_limits()
        )
        .is_err()
    );
    let transition = Registry::prepare_cancel_monitor_bounded(
        &owner,
        valid,
        MonitorId::from_u128(1),
        &graph,
        &peers,
        build_limits(),
    )
    .unwrap()
    .build()
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &peers)
        .unwrap();
    let graph = snapshot(&[&owner]);
    assert!(
        Registry::prepare_cancel_monitor_bounded(
            &owner,
            authority(&owner, 5),
            MonitorId::from_u128(1),
            &graph,
            &[],
            build_limits()
        )
        .is_err()
    );
    assert!(
        Registry::prepare_release_monitor_bounded(
            &owner,
            MonitorId::from_u128(1),
            &graph,
            &[],
            cut(5),
            build_limits()
        )
        .is_err()
    );
    assert_eq!(owner.scopes().iter().next().unwrap().release_cut(), None);
}

#[test]
fn cancelling_a_monitor_does_not_discharge_owned_child_work() {
    let mut owner = claim(1);
    let definition = child_definition(&owner, 2, 2);
    let mut child = owner
        .generate_child(
            &owner.binding(),
            Principal::Actor(ISSUER),
            None,
            definition,
            cut(2),
        )
        .unwrap();
    let graph = snapshot(&[&owner, &child]);
    let roots = [WaitPredicate::Satisfied(ClaimId::from_u128(2))];
    let transition = Registry::prepare_register_bounded(
        &owner,
        authority(&owner, 3),
        registration(&roots),
        &graph,
        &[&child],
        build_limits(),
    )
    .unwrap()
    .build()
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &[&child])
        .unwrap();
    cancel_owned(&mut owner, &mut child, 4);
    let graph = snapshot(&[&owner, &child]);
    let transition = Registry::prepare_cancel_monitor_bounded(
        &owner,
        authority(&owner, 5),
        MonitorId::from_u128(1),
        &graph,
        &[&child],
        build_limits(),
    )
    .unwrap()
    .build()
    .unwrap();
    let original_child = child.clone();
    owner
        .apply_scope(&owner.binding(), transition, &[&child])
        .unwrap();
    assert_eq!(child, original_child);
    let graph = snapshot(&[&owner, &child]);
    assert!(
        Registry::prepare_release_owner_bounded(
            &owner,
            &graph,
            &[&child],
            cut(6),
            usize::MAX,
            usize::MAX
        )
        .is_err()
    );
    assert_eq!(owner.scopes().children().len(), 1);
    let graph = snapshot(&[&child]);
    let transition = Registry::prepare_release_owner_bounded(
        &child,
        &graph,
        &[],
        cut(6),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    child
        .apply_scope(&child.binding(), transition, &[])
        .unwrap();
    let graph = snapshot(&[&owner, &child]);
    let transition = Registry::prepare_release_owner_bounded(
        &owner,
        &graph,
        &[&child],
        cut(7),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &[&child])
        .unwrap();
    assert!(owner.released());
    assert!(child.released());
}
