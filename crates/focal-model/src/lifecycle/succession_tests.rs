use super::*;
use crate::lifecycle::{
    aggregation,
    claim::{ClaimDefinition, ClaimIntent, PostingStanding, PredicateState},
    graph, scope,
};
use crate::{
    ContentHash, LedgerId, ObjectId, ObjectRevision, ParticipantId, SessionId, SessionSeq, TenantId,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const SUBJECT: ParticipantId = ParticipantId::from_u128(2);
fn binding(id: u128) -> Binding {
    Binding {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(1),
        },
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}
fn correction(id: u128) -> Correction {
    Correction {
        kind: CorrectionKind::Supersedes,
        predecessor: ObjectRef::claim(binding(id).ledger, ClaimId::from_u128(id)),
    }
}
fn definition(id: u128, sequence: u64, corrections: &[Correction]) -> ClaimDefinition {
    let binding = binding(id);
    ClaimDefinition {
        binding,
        issuer: ISSUER,
        subject: SUBJECT,
        deadline: None,
        max_responses: 2,
        created: SessionSeq(sequence),
        graph: graph::Declaration::empty(),
        lineage: Lineage::new(
            binding,
            Cause::Root(RootCommandId::from_u128(1)),
            corrections,
            8,
        )
        .unwrap(),
        acceptance: aggregation::acceptance_for(binding, ISSUER),
        scope_limits: scope::ScopeLimits {
            scopes: 4,
            roots: 8,
            children: 4,
        },
    }
}
fn claim(id: u128, sequence: u64, corrections: &[Correction]) -> ClaimState {
    ClaimState::generate(
        Principal::Actor(ISSUER),
        definition(id, sequence, corrections),
    )
    .unwrap()
}
fn cut(sequence: u64) -> ClaimCut {
    ClaimCut {
        position: SessionSeq(sequence),
        cause: ContentHash([8; 32]),
    }
}
fn limits() -> Limits {
    Limits {
        nodes: 8,
        edge_visits: 32,
    }
}
fn prepare(
    predecessor: &ClaimState,
    successor: &ClaimState,
    ancestors: &[&ClaimState],
) -> Result<SuccessionPlan, ContractError> {
    SuccessionPlan::prepare(
        Principal::Actor(ISSUER),
        predecessor,
        successor,
        ancestors,
        cut(successor.created().0),
        limits(),
    )
}

#[test]
fn lineage_rejects_wrong_family_self_cross_ledger_and_duplicate_relations() {
    let b = binding(1);
    assert!(Lineage::new(b, Cause::Claim(ClaimId::from_u128(1)), &[], 0).is_err());
    assert!(Lineage::new(b, Cause::Root(RootCommandId::from_u128(0)), &[], 0).is_err());
    for mut relation in [correction(1), correction(2), correction(3)] {
        if relation.predecessor.id == ObjectId::from_u128(2) {
            relation.predecessor.kind = ObjectKind::Artifact;
        }
        if relation.predecessor.id == ObjectId::from_u128(3) {
            relation.predecessor.ledger.session = SessionId::from_u128(2);
        }
        assert!(Lineage::new(b, Cause::Root(RootCommandId::from_u128(1)), &[relation], 1).is_err());
    }
    assert!(
        Lineage::new(
            b,
            Cause::Root(RootCommandId::from_u128(1)),
            &[correction(2), correction(2)],
            2
        )
        .is_err()
    );
    let ordered = Lineage::new(
        b,
        Cause::Root(RootCommandId::from_u128(1)),
        &[correction(3), correction(2)],
        2,
    )
    .unwrap();
    assert_eq!(ordered.corrections(), &[correction(2), correction(3)]);
}

#[test]
fn checked_successor_supersedes_open_and_preserves_original_terminal_cut() {
    let mut predecessor = claim(1, 1, &[]);
    let successor = claim(2, 2, &[correction(1)]);
    prepare(&predecessor, &successor, &[])
        .unwrap()
        .apply(&mut predecessor, &successor, &[])
        .unwrap();
    assert_eq!(predecessor.status(), ClaimStatus::Superseded);
    assert_eq!(predecessor.local_sealed_at(), Some(SessionSeq(2)));
    assert_eq!(successor.status(), ClaimStatus::Generated);
    let original = predecessor.clone();
    let later = claim(3, 3, &[correction(1)]);
    prepare(&predecessor, &later, &[])
        .unwrap()
        .apply(&mut predecessor, &later, &[])
        .unwrap();
    assert_eq!(predecessor, original);
}

#[test]
fn authority_compatibility_amendment_and_temporal_boundaries_are_checked() {
    let predecessor = claim(1, 1, &[]);
    let successor = claim(2, 2, &[correction(1)]);
    for principal in [Principal::Actor(SUBJECT), Principal::Node(ISSUER)] {
        assert!(matches!(
            SuccessionPlan::prepare(principal, &predecessor, &successor, &[], cut(2), limits()),
            Err(ContractError::WrongActor)
        ));
    }
    let amended = claim(
        3,
        2,
        &[Correction {
            kind: CorrectionKind::Amends,
            ..correction(1)
        }],
    );
    assert!(prepare(&predecessor, &amended, &[]).is_err());
    let mut wrong = definition(4, 2, &[correction(1)]);
    wrong.subject = ParticipantId::from_u128(3);
    let wrong = ClaimState::generate(Principal::Actor(ISSUER), wrong).unwrap();
    assert!(prepare(&predecessor, &wrong, &[]).is_err());
    assert!(
        SuccessionPlan::prepare(
            Principal::Actor(ISSUER),
            &predecessor,
            &successor,
            &[],
            cut(3),
            limits()
        )
        .is_err()
    );
    let same_time = claim(5, 1, &[correction(1)]);
    assert!(prepare(&predecessor, &same_time, &[]).is_err());
}

#[test]
fn effective_publication_rechecks_predecessor_successor_and_all_ancestor_bindings() {
    let ancestor = claim(1, 1, &[]);
    let mut predecessor = claim(2, 2, &[correction(1)]);
    let successor = claim(3, 3, &[correction(2)]);
    assert!(prepare(&predecessor, &successor, &[]).is_err());
    let plan = prepare(&predecessor, &successor, &[&ancestor]).unwrap();
    assert_eq!(plan.reads().len(), 3);
    let before = predecessor.clone();
    assert!(plan.apply(&mut predecessor, &successor, &[]).is_err());
    assert_eq!(predecessor, before);
    let plan = prepare(&predecessor, &successor, &[&ancestor]).unwrap();
    let mut changed = ancestor.clone();
    changed
        .apply(
            &changed.binding(),
            Principal::Actor(ISSUER),
            ClaimIntent::Post {
                standing: PostingStanding {
                    binding: changed.binding(),
                    standing: PredicateState::Passed,
                    target: PredicateState::Passed,
                },
            },
        )
        .unwrap();
    assert!(matches!(
        plan.apply(&mut predecessor, &successor, &[&changed]),
        Err(ContractError::StaleRevision)
    ));
    assert_eq!(predecessor, before);
    let plan = prepare(&predecessor, &successor, &[&ancestor]).unwrap();
    predecessor
        .apply(
            &predecessor.binding(),
            Principal::Actor(ISSUER),
            ClaimIntent::Post {
                standing: PostingStanding {
                    binding: predecessor.binding(),
                    standing: PredicateState::Passed,
                    target: PredicateState::Passed,
                },
            },
        )
        .unwrap();
    assert!(matches!(
        plan.apply(&mut predecessor, &successor, &[&ancestor]),
        Err(ContractError::StaleRevision)
    ));
}

#[test]
fn lineage_walk_rejects_cycles_and_obeys_node_edge_and_revision_bounds() {
    let predecessor = claim(1, 1, &[correction(2)]);
    let successor = claim(2, 2, &[correction(1)]);
    assert!(prepare(&predecessor, &successor, &[]).is_err());
    let predecessor = claim(1, 1, &[]);
    for bound in [
        Limits {
            nodes: 1,
            edge_visits: 32,
        },
        Limits {
            nodes: 8,
            edge_visits: 0,
        },
    ] {
        assert!(matches!(
            SuccessionPlan::prepare(
                Principal::Actor(ISSUER),
                &predecessor,
                &successor,
                &[],
                cut(2),
                bound
            ),
            Err(ContractError::Capacity)
        ));
    }
    let mut maximum = definition(1, 1, &[]);
    maximum.binding.revision = ObjectRevision(u64::MAX);
    maximum.lineage = Lineage::root(maximum.binding, RootCommandId::from_u128(1)).unwrap();
    maximum.acceptance = aggregation::acceptance_for(maximum.binding, ISSUER);
    let maximum = ClaimState::generate(Principal::Actor(ISSUER), maximum).unwrap();
    assert!(matches!(
        prepare(&maximum, &successor, &[]),
        Err(ContractError::Capacity)
    ));
}

#[test]
fn lineage_rejects_a_direct_historical_reference_to_a_future_claim() {
    for kind in [CorrectionKind::Supersedes, CorrectionKind::Amends] {
        let ancestor = claim(1, 3, &[]);
        let predecessor = claim(
            2,
            2,
            &[Correction {
                kind,
                ..correction(1)
            }],
        );
        let successor = claim(3, 4, &[correction(2)]);
        // Every row predates the succession cut, but the predecessor's
        // immutable edge points to an object created after that predecessor.
        assert!(matches!(
            prepare(&predecessor, &successor, &[&ancestor]),
            Err(ContractError::InvalidCut)
        ));
    }
}

#[test]
fn lineage_rejects_future_ancestry_beyond_the_immediate_predecessor() {
    let ancestor = claim(1, 3, &[]);
    let middle = claim(
        2,
        2,
        &[Correction {
            kind: CorrectionKind::Amends,
            ..correction(1)
        }],
    );
    let predecessor = claim(3, 4, &[correction(2)]);
    let successor = claim(4, 5, &[correction(3)]);
    assert!(matches!(
        prepare(&predecessor, &successor, &[&ancestor, &middle]),
        Err(ContractError::InvalidCut)
    ));
}

#[test]
fn lineage_checks_each_edge_even_when_its_target_was_already_visited() {
    let ancestor = claim(1, 3, &[]);
    let predecessor = claim(2, 2, &[correction(1)]);
    // The sorted first edge reaches ancestor directly and finishes its walk.
    // Reusing that black target must still check the predecessor's own edge.
    let successor = claim(3, 4, &[correction(1), correction(2)]);
    assert!(matches!(
        prepare(&predecessor, &successor, &[&ancestor]),
        Err(ContractError::InvalidCut)
    ));
}

#[test]
fn lineage_preserves_valid_cause_ancestry_and_acyclic_same_cut_references() {
    for peer_created in [2, 3] {
        let mut parent = claim(1, 1, &[]);
        let mut child = definition(2, 2, &[]);
        child.lineage =
            Lineage::new(child.binding, Cause::Claim(ClaimId::from_u128(1)), &[], 0).unwrap();
        let mut predecessor = parent
            .generate_child(
                &parent.binding(),
                Principal::Actor(ISSUER),
                None,
                child,
                cut(2),
            )
            .unwrap();
        let peer = claim(
            3,
            peer_created,
            &[Correction {
                kind: CorrectionKind::Amends,
                ..correction(2)
            }],
        );
        let successor = claim(
            4,
            3,
            &[
                correction(2),
                Correction {
                    kind: CorrectionKind::Amends,
                    ..correction(3)
                },
            ],
        );
        let parent_before = parent.clone();
        let peer_before = peer.clone();
        prepare(&predecessor, &successor, &[&parent, &peer])
            .unwrap()
            .apply(&mut predecessor, &successor, &[&parent, &peer])
            .unwrap();
        assert_eq!(predecessor.status(), ClaimStatus::Superseded);
        assert_eq!(predecessor.local_sealed_at(), Some(SessionSeq(3)));
        assert_eq!(parent, parent_before);
        assert_eq!(peer, peer_before);
        assert_eq!(successor.status(), ClaimStatus::Generated);
    }
}

#[test]
fn equal_creation_positions_do_not_hide_a_historical_lineage_cycle() {
    let first = claim(1, 1, &[correction(2)]);
    let second = claim(2, 1, &[correction(1)]);
    let predecessor = claim(3, 2, &[correction(1)]);
    let successor = claim(4, 3, &[correction(3)]);
    assert!(matches!(
        prepare(&predecessor, &successor, &[&first, &second]),
        Err(ContractError::InvalidTarget)
    ));
}
