use super::super::{aggregation, claim, graph, succession};
use super::*;
use crate::{
    ContentHash, ObjectId, ObjectRevision, ParticipantId, ReceiptId, RootCommandId, TimerId,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
fn cut(position: u64) -> ClaimCut {
    ClaimCut {
        position: SessionSeq(position),
        cause: ContentHash([7; 32]),
    }
}
fn definition(id: u128) -> claim::ClaimDefinition {
    let mut value = claim::tests::definition(3);
    value.binding.object = ObjectId::from_u128(id);
    value.lineage = succession::Lineage::root(value.binding, RootCommandId::from_u128(id)).unwrap();
    value.acceptance = aggregation::acceptance_for(value.binding, value.issuer);
    value
}
fn claim(id: u128) -> ClaimState {
    ClaimState::generate(Principal::Actor(ISSUER), definition(id)).unwrap()
}
fn limits() -> graph::Limits {
    graph::Limits {
        nodes: 16,
        edges: 64,
        visits: 4096,
    }
}
fn snapshot(rows: &[&ClaimState]) -> Snapshot {
    Snapshot::capture(rows, limits()).unwrap()
}
fn authority(owner: &ClaimState, sequence: u64) -> Authority {
    Authority {
        principal: Principal::Actor(owner.issuer()),
        expected: owner.binding(),
        receipt: owner.receipt().map(|value| value.fence),
        cut: cut(sequence),
        now: 10,
    }
}
fn deadline() -> Deadline {
    Deadline {
        timer: TimerId::from_u128(3),
        generation: 1,
        at: 100,
    }
}
fn registration(roots: &[WaitPredicate]) -> Registration<'_> {
    Registration {
        id: MonitorId::from_u128(1),
        roots,
        deadline: deadline(),
    }
}
fn cancel(owner: &mut ClaimState, sequence: u64) {
    owner
        .apply(
            &owner.binding(),
            Principal::Actor(owner.issuer()),
            claim::ClaimIntent::Cancel { cut: cut(sequence) },
        )
        .unwrap();
}
fn register(owner: &mut ClaimState, target: &ClaimState, roots: &[WaitPredicate]) {
    let graph = snapshot(&[owner, target]);
    let plan = Registry::prepare_register(owner, authority(owner, 2), registration(roots), &graph)
        .unwrap();
    owner
        .apply_scope(&owner.binding(), plan, &[target])
        .unwrap();
}

#[test]
fn registration_canonicalizes_roots_and_preserves_immutable_dependencies() {
    let mut owner = claim(1);
    let target = claim(2);
    let id = ClaimId(target.binding().object.0);
    let roots = [
        WaitPredicate::Released(id),
        WaitPredicate::Terminal(id),
        WaitPredicate::Terminal(id),
    ];
    register(&mut owner, &target, &roots);
    let row = owner.scopes().iter().next().unwrap();
    assert_eq!(
        row.roots(),
        &[WaitPredicate::Terminal(id), WaitPredicate::Released(id)]
    );
    assert_eq!(row.registered(), SessionSeq(2));
    assert_eq!(row.released(), None);
    assert_eq!(row.deadline(), deadline());
    assert!(owner.graph().obligations().is_empty());
    assert_eq!(owner.binding().revision, ObjectRevision(2));
}

#[test]
fn registration_rejects_wrong_writer_receipt_revision_and_unknown_endpoints() {
    let owner = claim(1);
    let target = claim(2);
    let graph = snapshot(&[&owner, &target]);
    let roots = [WaitPredicate::Terminal(ClaimId(target.binding().object.0))];
    let valid = authority(&owner, 2);
    let cases = [
        (
            Authority {
                principal: Principal::Node(ISSUER),
                ..valid
            },
            ContractError::WrongActor,
        ),
        (
            Authority {
                principal: Principal::Actor(ParticipantId::from_u128(8)),
                ..valid
            },
            ContractError::WrongActor,
        ),
        (
            Authority {
                expected: valid.expected.next().unwrap(),
                ..valid
            },
            ContractError::StaleRevision,
        ),
        (
            Authority {
                receipt: Some(ReceiptFence {
                    receipt: ReceiptId::from_u128(1),
                    epoch: 1,
                }),
                ..valid
            },
            ContractError::StaleReceipt,
        ),
        (
            Authority {
                cut: cut(0),
                ..valid
            },
            ContractError::InvalidCut,
        ),
    ];
    for (authority, error) in cases {
        assert_eq!(
            Registry::prepare_register(&owner, authority, registration(&roots), &graph)
                .unwrap_err(),
            error
        );
    }
    let unknown = [WaitPredicate::Terminal(ClaimId::from_u128(99))];
    assert_eq!(
        Registry::prepare_register(&owner, valid, registration(&unknown), &graph).unwrap_err(),
        ContractError::InvalidTarget
    );
}

#[test]
fn parking_requires_a_finite_future_fenced_deadline_and_bounded_unique_id() {
    let mut def = definition(1);
    def.scope_limits = ScopeLimits {
        scopes: 1,
        roots: 1,
        children: 0,
    };
    let mut owner = ClaimState::generate(Principal::Actor(ISSUER), def).unwrap();
    let target = claim(2);
    let roots = [WaitPredicate::Terminal(ClaimId::from_u128(2))];
    let graph = snapshot(&[&owner, &target]);
    for invalid in [
        Deadline {
            at: 10,
            ..deadline()
        },
        Deadline {
            timer: TimerId::default(),
            ..deadline()
        },
        Deadline {
            generation: 0,
            ..deadline()
        },
    ] {
        assert_eq!(
            Registry::prepare_register(
                &owner,
                authority(&owner, 2),
                Registration {
                    deadline: invalid,
                    ..registration(&roots)
                },
                &graph
            )
            .unwrap_err(),
            ContractError::InvalidTarget
        );
    }
    let too_many = [roots[0], WaitPredicate::Released(ClaimId::from_u128(2))];
    assert_eq!(
        Registry::prepare_register(
            &owner,
            authority(&owner, 2),
            registration(&too_many),
            &graph
        )
        .unwrap_err(),
        ContractError::Capacity
    );
    register(&mut owner, &target, &roots);
    let graph = snapshot(&[&owner, &target]);
    assert_eq!(
        Registry::prepare_register(&owner, authority(&owner, 3), registration(&roots), &graph)
            .unwrap_err(),
        ContractError::InvalidTarget
    );
    assert_eq!(
        Registry::prepare_register(
            &owner,
            authority(&owner, 3),
            Registration {
                id: MonitorId::from_u128(2),
                ..registration(&roots)
            },
            &graph
        )
        .unwrap_err(),
        ContractError::Capacity
    );
}

#[test]
fn stale_graph_cannot_release_a_newly_registered_edge_and_peer_changes_abort_atomically() {
    let mut owner = claim(1);
    let mut target = claim(2);
    cancel(&mut target, 2);
    let old_graph = snapshot(&[&owner, &target]);
    let roots = [WaitPredicate::Terminal(ClaimId::from_u128(2))];
    register(&mut owner, &target, &roots);
    assert_eq!(
        Registry::prepare_release_monitor(&owner, MonitorId::from_u128(1), &old_graph, cut(3))
            .unwrap_err(),
        ContractError::StaleRevision
    );
    let graph = snapshot(&[&owner, &target]);
    let plan =
        Registry::prepare_release_monitor(&owner, MonitorId::from_u128(1), &graph, cut(3)).unwrap();
    let target_graph = snapshot(&[&target]);
    let release = Registry::prepare_release_owner(&target, &target_graph, &[], cut(3)).unwrap();
    target.apply_scope(&target.binding(), release, &[]).unwrap();
    let before = owner.clone();
    assert_eq!(
        owner.apply_scope(&owner.binding(), plan, &[&target]),
        Err(ContractError::StaleRevision)
    );
    assert_eq!(owner, before);
}

#[test]
fn settlement_uses_exact_predicate_and_releases_once_with_durable_cut() {
    for (predicate, settles) in [
        (WaitPredicate::Satisfied(ClaimId::from_u128(2)), false),
        (WaitPredicate::Terminal(ClaimId::from_u128(2)), true),
        (WaitPredicate::Released(ClaimId::from_u128(2)), false),
    ] {
        let mut owner = claim(1);
        let mut target = claim(2);
        register(&mut owner, &target, &[predicate]);
        cancel(&mut target, 3);
        let graph = snapshot(&[&owner, &target]);
        let result =
            Registry::prepare_release_monitor(&owner, MonitorId::from_u128(1), &graph, cut(4));
        if settles {
            let plan = result.unwrap();
            assert_eq!(
                plan.event(),
                Event::MonitorReleased {
                    id: MonitorId::from_u128(1),
                    cut: cut(4)
                }
            );
            owner
                .apply_scope(&owner.binding(), plan, &[&target])
                .unwrap();
            let row = owner.scopes().iter().next().unwrap();
            assert_eq!(row.released(), Some(SessionSeq(4)));
            assert_eq!(row.release_cut(), Some(cut(4)));
            let graph = snapshot(&[&owner, &target]);
            assert_eq!(
                Registry::prepare_release_monitor(&owner, row.id(), &graph, cut(5)).unwrap_err(),
                ContractError::InvalidTransition
            );
        } else {
            assert_eq!(result.unwrap_err(), ContractError::InvalidTransition);
        }
    }
}

#[test]
fn runtime_satisfied_wait_blocks_without_becoming_a_failed_prerequisite() {
    let mut owner = claim(1);
    let mut target = claim(2);
    register(
        &mut owner,
        &target,
        &[WaitPredicate::Satisfied(ClaimId::from_u128(2))],
    );
    cancel(&mut target, 3);
    let graph = snapshot(&[&owner, &target]);
    assert!(!graph.satisfied(ClaimId::from_u128(1)).unwrap());
    assert_eq!(
        graph.dependency_failure(ClaimId::from_u128(1)).unwrap_err(),
        ContractError::InvalidTransition
    );
}

#[test]
fn named_rebind_requires_real_compatible_supersedes_and_preserves_other_rows() {
    let mut owner = claim(1);
    let predecessor = claim(2);
    let roots = [
        WaitPredicate::Satisfied(ClaimId::from_u128(2)),
        WaitPredicate::Terminal(ClaimId::from_u128(2)),
        WaitPredicate::Released(ClaimId::from_u128(2)),
    ];
    register(&mut owner, &predecessor, &roots);
    let unrelated = claim(3);
    let graph = snapshot(&[&owner, &predecessor, &unrelated]);
    assert_eq!(
        Registry::prepare_rebind(
            &owner,
            authority(&owner, 3),
            MonitorId::from_u128(1),
            &predecessor,
            &unrelated,
            &graph
        )
        .unwrap_err(),
        ContractError::InvalidTarget
    );
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
    let successor = ClaimState::generate(Principal::Actor(ISSUER), def).unwrap();
    let graph = snapshot(&[&owner, &predecessor, &successor]);
    let plan = Registry::prepare_rebind(
        &owner,
        authority(&owner, 3),
        MonitorId::from_u128(1),
        &predecessor,
        &successor,
        &graph,
    )
    .unwrap();
    owner
        .apply_scope(&owner.binding(), plan, &[&predecessor, &successor])
        .unwrap();
    let row = owner.scopes().iter().next().unwrap();
    assert_eq!(
        row.roots(),
        &[
            WaitPredicate::Satisfied(ClaimId::from_u128(3)),
            WaitPredicate::Terminal(ClaimId::from_u128(3)),
            WaitPredicate::Released(ClaimId::from_u128(3))
        ]
    );
    assert_eq!(row.registered(), SessionSeq(2));
    assert_eq!(row.last_rebinding().unwrap().cut, cut(3));
    assert!(owner.graph().obligations().is_empty());
}

fn child_definition(parent: &ClaimState, id: u128, sequence: u64) -> claim::ClaimDefinition {
    let mut def = definition(id);
    def.created = SessionSeq(sequence);
    def.lineage = succession::Lineage::new(
        def.binding,
        Cause::Claim(ClaimId(parent.binding().object.0)),
        &[],
        0,
    )
    .unwrap();
    def
}

#[test]
fn child_generation_pins_complete_ownership_and_forbids_root_generation_bypass() {
    let mut parent = claim(1);
    assert_eq!(
        ClaimState::generate(Principal::Actor(ISSUER), child_definition(&parent, 2, 2))
            .unwrap_err(),
        ContractError::InvalidTarget
    );
    let definition = child_definition(&parent, 2, 2);
    let child = parent
        .generate_child(
            &parent.binding(),
            Principal::Actor(ISSUER),
            None,
            definition,
            cut(2),
        )
        .unwrap();
    assert_eq!(parent.scopes().children().len(), 1);
    assert_eq!(parent.scopes().children()[0].binding(), child.binding());
    assert_eq!(parent.scopes().children()[0].registered(), SessionSeq(2));
    assert_eq!(
        Snapshot::capture(&[&parent], limits()).unwrap_err(),
        ContractError::InvalidTarget
    );
    assert!(Snapshot::capture(&[&parent, &child], limits()).is_ok());
}

#[test]
fn parent_release_requires_every_owned_child_released_and_is_irreversible() {
    let mut parent = claim(1);
    let definition = child_definition(&parent, 2, 2);
    let mut child = parent
        .generate_child(
            &parent.binding(),
            Principal::Actor(ISSUER),
            None,
            definition,
            cut(2),
        )
        .unwrap();
    cancel(&mut parent, 3);
    cancel(&mut child, 3);
    let graph = snapshot(&[&parent, &child]);
    assert_eq!(
        Registry::prepare_release_owner(&parent, &graph, &[&child], cut(4)).unwrap_err(),
        ContractError::InvalidTransition
    );
    assert_eq!(
        Registry::prepare_release_owner(&parent, &graph, &[], cut(4)).unwrap_err(),
        ContractError::InvalidManifest
    );
    let child_graph = snapshot(&[&child]);
    let plan = Registry::prepare_release_owner(&child, &child_graph, &[], cut(4)).unwrap();
    child.apply_scope(&child.binding(), plan, &[]).unwrap();
    let graph = snapshot(&[&parent, &child]);
    let plan = Registry::prepare_release_owner(&parent, &graph, &[&child], cut(5)).unwrap();
    parent
        .apply_scope(&parent.binding(), plan, &[&child])
        .unwrap();
    assert!(parent.scopes().released());
    assert_eq!(parent.scopes().release_cut(), Some(cut(5)));
    let graph = snapshot(&[&parent, &child]);
    assert_eq!(
        Registry::prepare_release_owner(&parent, &graph, &[&child], cut(6)).unwrap_err(),
        ContractError::InvalidTransition
    );
}

#[test]
fn failed_child_creation_capacity_and_revision_checks_leave_parent_unchanged() {
    let mut def = definition(1);
    def.scope_limits.children = 0;
    let mut parent = ClaimState::generate(Principal::Actor(ISSUER), def).unwrap();
    let before = parent.clone();
    let definition = child_definition(&parent, 2, 2);
    assert_eq!(
        parent
            .generate_child(
                &parent.binding(),
                Principal::Actor(ISSUER),
                None,
                definition,
                cut(2)
            )
            .unwrap_err(),
        ContractError::Capacity
    );
    assert_eq!(parent, before);
    let mut def = definition_for_overflow();
    def.scope_limits.children = 1;
    let mut parent = ClaimState::generate(Principal::Actor(ISSUER), def).unwrap();
    let before = parent.clone();
    let definition = child_definition(&parent, 2, 2);
    assert_eq!(
        parent
            .generate_child(
                &parent.binding(),
                Principal::Actor(ISSUER),
                None,
                definition,
                cut(2)
            )
            .unwrap_err(),
        ContractError::Capacity
    );
    assert_eq!(parent, before);
}
fn definition_for_overflow() -> claim::ClaimDefinition {
    let mut value = definition(1);
    value.binding.revision = ObjectRevision(u64::MAX);
    value.lineage = succession::Lineage::root(value.binding, RootCommandId::from_u128(1)).unwrap();
    value.acceptance = aggregation::acceptance_for(value.binding, value.issuer);
    value
}
