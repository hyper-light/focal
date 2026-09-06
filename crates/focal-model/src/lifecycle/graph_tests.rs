use super::*;
use crate::lifecycle::claim::{
    ClaimCut, ClaimDefinition, ClaimIntent, PostingStanding, PredicateState,
};
use crate::lifecycle::{Principal, succession};
use crate::{ObjectId, RootCommandId, SessionId};

fn limits() -> Limits {
    Limits {
        nodes: 32,
        edges: 128,
        visits: 8192,
    }
}
fn cid(value: u128) -> ClaimId {
    ClaimId::from_u128(value)
}
fn declaration(edges: &[(Kind, u128)]) -> Declaration {
    let mut edges: Vec<_> = edges
        .iter()
        .map(|(kind, target)| Obligation {
            kind: *kind,
            target: cid(*target),
        })
        .collect();
    edges.sort_unstable();
    Declaration::new(&edges, 128).unwrap()
}
fn claim(value: u128, created: u64, edges: &[(Kind, u128)]) -> ClaimState {
    let mut definition: ClaimDefinition = crate::lifecycle::claim::tests::definition(4);
    definition.binding.object = ObjectId::from_u128(value);
    definition.binding.content = ContentHash([u8::try_from(value).unwrap(); 32]);
    definition.created = SessionSeq(created);
    definition.graph = declaration(edges);
    definition.lineage =
        succession::Lineage::root(definition.binding, RootCommandId::from_u128(value)).unwrap();
    definition.acceptance =
        crate::lifecycle::claim::tests::acceptance(definition.binding, definition.issuer);
    ClaimState::generate(Principal::Actor(definition.issuer), definition).unwrap()
}
fn post(claim: &mut ClaimState) {
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            ClaimIntent::Post {
                standing: PostingStanding {
                    binding: claim.binding(),
                    standing: PredicateState::Passed,
                    target: PredicateState::Passed,
                },
            },
        )
        .unwrap();
}
fn cancel(claim: &mut ClaimState, sequence: u64) {
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            ClaimIntent::Cancel {
                cut: ClaimCut {
                    position: SessionSeq(sequence),
                    cause: ContentHash([90; 32]),
                },
            },
        )
        .unwrap();
}
fn local(claim: &mut ClaimState) {
    crate::lifecycle::claim::tests::local_projection_for_graph(claim);
}

#[test]
fn immutable_declarations_reject_unbounded_duplicate_and_zero_endpoints() {
    let one = Obligation {
        kind: Kind::DependsOn,
        target: cid(1),
    };
    assert_eq!(Declaration::new(&[one], 0), Err(ContractError::Capacity));
    assert_eq!(
        Declaration::new(&[one, one], 2),
        Err(ContractError::InvalidManifest)
    );
    assert_eq!(
        Declaration::new(
            &[Obligation {
                target: cid(0),
                ..one
            }],
            1
        ),
        Err(ContractError::InvalidManifest)
    );
    let mut input = [one];
    let declaration = Declaration::new(&input, 1).unwrap();
    input[0].target = cid(2);
    assert_ne!(input.as_slice(), declaration.obligations());
    assert_eq!(declaration.obligations(), &[one]);
}

#[test]
fn complete_closure_and_bounds_are_checked_before_any_witness() {
    let a = claim(1, 1, &[(Kind::DependsOn, 2)]);
    let b = claim(2, 2, &[]);
    assert!(matches!(
        Snapshot::capture(&[&a], limits()),
        Err(ContractError::InvalidTarget)
    ));
    assert!(matches!(
        Snapshot::capture(&[&a, &a], limits()),
        Err(ContractError::InvalidManifest)
    ));
    assert!(matches!(
        Snapshot::capture(&[&b, &a], limits()),
        Err(ContractError::InvalidManifest)
    ));
    assert!(matches!(
        Snapshot::capture(
            &[&a, &b],
            Limits {
                nodes: 1,
                ..limits()
            }
        ),
        Err(ContractError::Capacity)
    ));
    assert!(matches!(
        Snapshot::capture(
            &[&a, &b],
            Limits {
                edges: 0,
                ..limits()
            }
        ),
        Err(ContractError::Capacity)
    ));
    assert!(matches!(
        Snapshot::capture(
            &[&a, &b],
            Limits {
                visits: 1,
                ..limits()
            }
        ),
        Err(ContractError::Capacity)
    ));
    let mut wrong = crate::lifecycle::claim::tests::definition(4);
    wrong.binding.ledger.session = SessionId::from_u128(8);
    wrong.lineage = succession::Lineage::root(wrong.binding, RootCommandId::from_u128(3)).unwrap();
    wrong.acceptance = crate::lifecycle::claim::tests::acceptance(wrong.binding, wrong.issuer);
    let wrong = ClaimState::generate(Principal::Actor(wrong.issuer), wrong).unwrap();
    assert!(matches!(
        Snapshot::capture(&[&a, &wrong], limits()),
        Err(ContractError::WrongLedger)
    ));
}

#[test]
fn failure_propagates_only_depends_on_and_keeps_its_first_terminal_cut() {
    let mut a = claim(1, 1, &[(Kind::DependsOn, 3)]);
    let mut b = claim(2, 2, &[(Kind::Awaits, 3)]);
    let mut failed = claim(3, 3, &[]);
    post(&mut a);
    post(&mut b);
    cancel(&mut failed, 10);
    let graph = Snapshot::capture(&[&a, &b, &failed], limits()).unwrap();
    assert!(graph.start(cid(1)).is_err());
    assert!(graph.start(cid(2)).is_ok());
    assert!(matches!(
        graph.dependency_failure(cid(2)),
        Err(ContractError::InvalidTransition)
    ));
    let failure = graph.dependency_failure(cid(1)).unwrap();
    assert_eq!(failure.path().map(id).collect::<Vec<_>>(), [cid(1), cid(3)]);
    assert_eq!(failure.origin().binding(), failed.binding());
    a.dependency_failed(&a.binding(), &failure, &[&b, &failed], SessionSeq(11))
        .unwrap();
    assert_eq!(a.status(), ClaimStatus::DependencyFailed);
    let original = a.terminal_cut();
    assert_eq!(
        a.dependency_failed(&a.binding(), &failure, &[&b, &failed], SessionSeq(12)),
        Err(ContractError::InvalidTransition)
    );
    assert_eq!(a.terminal_cut(), original);
    assert_eq!(b.status(), ClaimStatus::Posted);
}

#[test]
fn canonical_failure_uses_original_creation_order_and_ignores_unrelated_failure() {
    let a = claim(1, 20, &[(Kind::DependsOn, 2), (Kind::DependsOn, 3)]);
    let mut b = claim(2, 8, &[]);
    let mut c = claim(3, 4, &[]);
    let mut unrelated = claim(4, 1, &[]);
    cancel(&mut b, 30);
    cancel(&mut c, 31);
    cancel(&mut unrelated, 32);
    let graph = Snapshot::capture(&[&a, &b, &c, &unrelated], limits()).unwrap();
    let failure = graph.dependency_failure(cid(1)).unwrap();
    assert_eq!(failure.origin().binding(), c.binding());
    assert_eq!(failure.path().map(id).collect::<Vec<_>>(), [cid(1), cid(3)]);
}

#[test]
fn propagated_failure_preserves_the_original_source_across_an_intermediate_claim() {
    let a = claim(1, 1, &[(Kind::DependsOn, 2)]);
    let mut b = claim(2, 2, &[(Kind::DependsOn, 3)]);
    let mut c = claim(3, 3, &[]);
    cancel(&mut c, 10);
    let first = Snapshot::capture(&[&a, &b, &c], limits()).unwrap();
    let failure = first.dependency_failure(cid(2)).unwrap();
    b.dependency_failed(&b.binding(), &failure, &[&a, &c], SessionSeq(11))
        .unwrap();
    let next = Snapshot::capture(&[&a, &b, &c], limits()).unwrap();
    let propagated = next.dependency_failure(cid(1)).unwrap();
    assert_eq!(propagated.origin().binding(), c.binding());
    assert_eq!(propagated.origin().created(), SessionSeq(3));
    assert_eq!(propagated.origin().terminal(), SessionSeq(10));
}

#[test]
fn least_fixpoint_grows_from_local_roots_and_never_assumes_a_cycle_true() {
    let mut root = claim(1, 1, &[]);
    let mut child = claim(2, 2, &[(Kind::DependsOn, 1)]);
    let mut cycle_a = claim(3, 3, &[(Kind::DependsOn, 4)]);
    let mut cycle_b = claim(4, 4, &[(Kind::DependsOn, 3)]);
    local(&mut root);
    local(&mut child);
    local(&mut cycle_a);
    local(&mut cycle_b);
    let graph = Snapshot::capture(&[&root, &child, &cycle_a, &cycle_b], limits()).unwrap();
    assert!(graph.satisfied(cid(1)).unwrap());
    assert!(graph.satisfied(cid(2)).unwrap());
    assert!(!graph.satisfied(cid(3)).unwrap());
    assert!(!graph.satisfied(cid(4)).unwrap());
    assert!(graph.release(cid(3)).is_err());
    let release = graph.release(cid(2)).unwrap();
    child
        .graph_release(
            &child.binding(),
            &release,
            &[&root, &cycle_a, &cycle_b],
            SessionSeq(20),
        )
        .unwrap();
    assert_eq!(child.status(), ClaimStatus::Satisfied);
    assert_eq!(child.local_sealed_at(), Some(SessionSeq(3)));
}

#[test]
fn graph_proof_rechecks_every_dependency_revision_and_exact_target() {
    let mut a = claim(1, 1, &[(Kind::DependsOn, 2)]);
    let mut b = claim(2, 2, &[]);
    local(&mut a);
    local(&mut b);
    let graph = Snapshot::capture(&[&a, &b], limits()).unwrap();
    let release = graph.release(cid(1)).unwrap();
    assert_eq!(
        b.graph_release(&b.binding(), &release, &[&a], SessionSeq(10)),
        Err(ContractError::WrongObject)
    );
    assert_eq!(
        a.graph_release(&a.binding(), &release, &[], SessionSeq(10)),
        Err(ContractError::InvalidManifest)
    );
    cancel(&mut b, 10);
    let before = a.terminal_cut();
    assert_eq!(
        a.graph_release(&a.binding(), &release, &[&b], SessionSeq(11)),
        Err(ContractError::StaleRevision)
    );
    assert_eq!(a.terminal_cut(), before);
    assert_eq!(a.status(), ClaimStatus::Validating);
}

#[test]
fn deadline_deadlock_chooses_canonical_scc_victim_and_cannot_target_another_claim() {
    let mut a = claim(1, 5, &[(Kind::Awaits, 2)]);
    let mut b = claim(2, 2, &[(Kind::DependsOn, 1)]);
    post(&mut a);
    post(&mut b);
    let graph = Snapshot::capture(&[&a, &b], limits()).unwrap();
    let deadline = a.deadline().unwrap();
    assert!(matches!(
        graph.deadlock(cid(1), deadline, deadline.at - 1),
        Err(ContractError::InvalidCut)
    ));
    assert!(matches!(
        graph.deadlock(
            cid(1),
            Deadline {
                generation: 77,
                ..deadline
            },
            deadline.at
        ),
        Err(ContractError::InvalidCut)
    ));
    let deadlock = graph.deadlock(cid(1), deadline, deadline.at).unwrap();
    assert_eq!(deadlock.victim().unwrap(), b.binding());
    assert_eq!(deadlock.trigger().unwrap(), a.binding());
    assert_eq!(
        deadlock.component().map(id).collect::<Vec<_>>(),
        [cid(1), cid(2)]
    );
    assert_eq!(
        a.break_deadlock(&a.binding(), &deadlock, &[&b], SessionSeq(10)),
        Err(ContractError::WrongObject)
    );
    b.break_deadlock(&b.binding(), &deadlock, &[&a], SessionSeq(10))
        .unwrap();
    assert_eq!(b.status(), ClaimStatus::Deadlocked);
    let Some(ClaimTerminalCut::Graph(cut)) = b.terminal_cut() else {
        panic!("graph provenance");
    };
    assert_eq!(cut.deadline(), Some(deadline));
    assert_eq!(cut.fired_at(), Some(deadline.at));
    assert_eq!(cut.sequence(), SessionSeq(10));
}

#[test]
fn posted_wait_cycle_and_self_cycle_require_real_deadline_facts() {
    let mut a = claim(1, 1, &[(Kind::Awaits, 2)]);
    let mut b = claim(2, 1, &[(Kind::Awaits, 1)]);
    post(&mut a);
    post(&mut b);
    let graph = Snapshot::capture(&[&a, &b], limits()).unwrap();
    assert!(!graph.satisfied(cid(1)).unwrap());
    assert!(!graph.satisfied(cid(2)).unwrap());
    assert_eq!(
        graph
            .deadlock(cid(2), b.deadline().unwrap(), 100)
            .unwrap()
            .victim()
            .unwrap(),
        a.binding()
    );
    let single = claim(3, 2, &[(Kind::DependsOn, 3)]);
    let graph = Snapshot::capture(&[&single], limits()).unwrap();
    assert!(
        graph
            .deadlock(cid(3), single.deadline().unwrap(), 100)
            .is_ok()
    );
    let acyclic = claim(4, 3, &[]);
    let graph = Snapshot::capture(&[&acyclic], limits()).unwrap();
    assert!(matches!(
        graph.deadlock(cid(4), acyclic.deadline().unwrap(), 100),
        Err(ContractError::InvalidTransition)
    ));
}

#[test]
fn failure_ties_use_canonical_path_edges_before_origin_identity() {
    let a = claim(1, 10, &[(Kind::DependsOn, 2), (Kind::DependsOn, 3)]);
    let b = claim(2, 11, &[(Kind::DependsOn, 9)]);
    let c = claim(3, 12, &[(Kind::DependsOn, 4)]);
    let mut low_id = claim(4, 4, &[]);
    let mut first_edge = claim(9, 4, &[]);
    cancel(&mut low_id, 20);
    cancel(&mut first_edge, 20);
    let graph = Snapshot::capture(&[&a, &b, &c, &low_id, &first_edge], limits()).unwrap();
    let failure = graph.dependency_failure(cid(1)).unwrap();
    assert_eq!(failure.origin().binding(), first_edge.binding());
    assert_eq!(
        failure.path().map(id).collect::<Vec<_>>(),
        [cid(1), cid(2), cid(9)]
    );
}

fn register(
    owner: &mut ClaimState,
    peer: &ClaimState,
    root: WaitPredicate,
    monitor: u128,
) -> Deadline {
    use crate::lifecycle::scope;
    let deadline = Deadline {
        timer: crate::TimerId::from_u128(monitor),
        generation: 2,
        at: 40,
    };
    let graph = if owner.binding().object < peer.binding().object {
        Snapshot::capture(&[&*owner, peer], limits()).unwrap()
    } else {
        Snapshot::capture(&[peer, &*owner], limits()).unwrap()
    };
    let transition = scope::Registry::prepare_register(
        owner,
        scope::Authority {
            principal: Principal::Actor(owner.issuer()),
            expected: owner.binding(),
            receipt: None,
            cut: ClaimCut {
                position: SessionSeq(20),
                cause: ContentHash([80; 32]),
            },
            now: 1,
        },
        scope::Registration {
            id: crate::MonitorId::from_u128(monitor),
            roots: &[root],
            deadline,
        },
        &graph,
    )
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &[peer])
        .unwrap();
    deadline
}

#[test]
fn complete_runtime_monitors_participate_in_fixpoint_and_deadline_scc() {
    let mut a = claim(1, 1, &[]);
    let mut b = claim(2, 2, &[]);
    post(&mut a);
    post(&mut b);
    let deadline_a = register(&mut a, &b, WaitPredicate::Terminal(cid(2)), 100);
    register(&mut b, &a, WaitPredicate::Terminal(cid(1)), 101);
    local(&mut a);
    local(&mut b);
    let graph = Snapshot::capture(&[&a, &b], limits()).unwrap();
    assert!(!graph.satisfied(cid(1)).unwrap());
    assert!(!graph.satisfied(cid(2)).unwrap());
    assert!(matches!(
        Snapshot::capture(&[&a], limits()),
        Err(ContractError::InvalidTarget)
    ));
    let deadlock = graph.deadlock(cid(1), deadline_a, 40).unwrap();
    assert_eq!(deadlock.victim().unwrap(), a.binding());
    a.break_deadlock(&a.binding(), &deadlock, &[&b], SessionSeq(30))
        .unwrap();
    assert_eq!(a.status(), ClaimStatus::Deadlocked);
}

#[test]
fn runtime_satisfied_wait_does_not_manufacture_depends_on_failure() {
    let mut a = claim(1, 1, &[]);
    let mut b = claim(2, 2, &[]);
    post(&mut a);
    post(&mut b);
    register(&mut a, &b, WaitPredicate::Satisfied(cid(2)), 100);
    local(&mut a);
    cancel(&mut b, 30);
    let graph = Snapshot::capture(&[&a, &b], limits()).unwrap();
    assert!(!graph.satisfied(cid(1)).unwrap());
    assert!(
        !graph
            .wait_settled(WaitPredicate::Satisfied(cid(2)))
            .unwrap()
    );
    assert!(graph.wait_settled(WaitPredicate::Terminal(cid(2))).unwrap());
    assert!(!graph.wait_settled(WaitPredicate::Released(cid(2))).unwrap());
    assert!(matches!(
        graph.dependency_failure(cid(1)),
        Err(ContractError::InvalidTransition)
    ));
}

#[test]
fn graph_and_scope_consequences_cannot_predate_their_captured_terminal_fact() {
    use crate::lifecycle::scope;
    let mut a = claim(1, 1, &[]);
    let mut b = claim(2, 2, &[]);
    post(&mut a);
    post(&mut b);
    register(&mut a, &b, WaitPredicate::Terminal(cid(2)), 100);
    cancel(&mut b, 100);
    let graph = Snapshot::capture(&[&a, &b], limits()).unwrap();
    assert_eq!(
        graph.check_cut(SessionSeq(99)),
        Err(ContractError::InvalidCut)
    );
    assert_eq!(graph.check_cut(SessionSeq(100)), Ok(()));
    assert!(matches!(
        scope::Registry::prepare_release_monitor(
            &a,
            crate::MonitorId::from_u128(100),
            &graph,
            ClaimCut {
                position: SessionSeq(21),
                cause: ContentHash([81; 32])
            }
        ),
        Err(ContractError::InvalidCut)
    ));
    let release = scope::Registry::prepare_release_monitor(
        &a,
        crate::MonitorId::from_u128(100),
        &graph,
        ClaimCut {
            position: SessionSeq(100),
            cause: ContentHash([81; 32]),
        },
    )
    .unwrap();
    a.apply_scope(&a.binding(), release, &[&b]).unwrap();
    assert_eq!(
        a.scopes().iter().next().unwrap().released(),
        Some(SessionSeq(100))
    );
}
