use super::*;
use crate::lifecycle::{Principal, claim, succession};
use crate::{ObjectId, RootCommandId};

#[path = "graph_visit_tests.rs"]
mod visits;

fn limits() -> Limits {
    Limits {
        nodes: 16,
        edges: 64,
        visits: 4096,
    }
}
fn cid(value: u128) -> ClaimId {
    ClaimId::from_u128(value)
}
fn claim(value: u128, created: u64, edges: &[(Kind, u128)]) -> ClaimState {
    let mut definition = claim::tests::definition(4);
    definition.binding.object = ObjectId::from_u128(value);
    definition.binding.content = ContentHash([u8::try_from(value).unwrap(); 32]);
    definition.created = SessionSeq(created);
    let mut edges = edges
        .iter()
        .map(|(kind, target)| Obligation {
            kind: *kind,
            target: cid(*target),
        })
        .collect::<Vec<_>>();
    edges.sort_unstable();
    definition.graph = Declaration::new(&edges, 64).unwrap();
    definition.lineage =
        succession::Lineage::root(definition.binding, RootCommandId::from_u128(value)).unwrap();
    definition.acceptance = claim::tests::acceptance(definition.binding, definition.issuer);
    ClaimState::generate(Principal::Actor(definition.issuer), definition).unwrap()
}
fn cycle() -> (ClaimState, ClaimState) {
    (
        claim(1, 5, &[(Kind::Awaits, 2)]),
        claim(2, 2, &[(Kind::DependsOn, 1)]),
    )
}
fn query<'a>(
    snapshot: &'a Snapshot,
    trigger: &ClaimState,
    bytes: usize,
) -> Result<Option<Deadlock<'a>>, ContractError> {
    snapshot.deadlock_query_with_budget(
        ClaimId(trigger.binding().object.0),
        trigger.deadline().unwrap(),
        100,
        SessionSeq(10),
        bytes,
    )
}

#[test]
fn bounded_query_preserves_canonical_component_fingerprint_and_witness_consumption() {
    let (a, mut b) = cycle();
    let outside = claim(3, 1, &[(Kind::DependsOn, 1)]);
    let snapshot = Snapshot::capture(&[&a, &b, &outside], limits()).unwrap();
    let result = query(&snapshot, &a, snapshot.deadlock_charge().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.victim().unwrap(), b.binding());
    assert_eq!(result.trigger().unwrap(), a.binding());
    assert_eq!(
        result
            .component()
            .map(|binding| binding.object)
            .collect::<Vec<_>>(),
        [a.binding().object, b.binding().object]
    );
    let expected_fingerprint = snapshot
        .fingerprint(b"focal.lifecycle.deadlock-scc.v1\0", [0, 1].into_iter())
        .unwrap();
    assert_eq!(result.fingerprint, expected_fingerprint);
    let old = snapshot
        .deadlock(cid(1), a.deadline().unwrap(), 100)
        .unwrap();
    assert_eq!(old.fingerprint, result.fingerprint);
    b.break_deadlock(&b.binding(), &result, &[&a, &outside], SessionSeq(11))
        .unwrap();
    let Some(ClaimTerminalCut::Graph(cut)) = b.terminal_cut() else {
        panic!("SCC provenance")
    };
    assert_eq!(cut.fingerprint(), expected_fingerprint);
    assert_eq!(cut.deadline(), a.deadline());
    assert_eq!(cut.origin().binding(), a.binding());
    assert_eq!(cut.sequence(), SessionSeq(11));

    let first = claim(4, 2, &[(Kind::Awaits, 5)]);
    let second = claim(5, 2, &[(Kind::Awaits, 4)]);
    let tied = Snapshot::capture(&[&first, &second], limits()).unwrap();
    assert_eq!(
        query(&tied, &second, usize::MAX)
            .unwrap()
            .unwrap()
            .victim()
            .unwrap(),
        first.binding()
    );
}

#[test]
fn valid_negative_is_distinct_from_bad_deadline_cut_terminal_or_incomplete_source() {
    let open = claim(1, 5, &[]);
    let graph = Snapshot::capture(&[&open], limits()).unwrap();
    assert!(
        query(&graph, &open, graph.deadlock_charge().unwrap())
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        graph.deadlock(cid(1), open.deadline().unwrap(), 100),
        Err(ContractError::InvalidTransition)
    ));
    assert!(matches!(
        graph.deadlock_query_with_budget(
            cid(1),
            open.deadline().unwrap(),
            100,
            SessionSeq(4),
            usize::MAX
        ),
        Err(ContractError::InvalidCut)
    ));
    for deadline in [
        Deadline {
            generation: 99,
            ..open.deadline().unwrap()
        },
        Deadline {
            at: 99,
            ..open.deadline().unwrap()
        },
    ] {
        assert!(matches!(
            graph.deadlock_query_with_budget(cid(1), deadline, 100, SessionSeq(10), usize::MAX),
            Err(ContractError::InvalidCut)
        ));
    }
    assert!(matches!(
        graph.deadlock_query_with_budget(
            cid(1),
            open.deadline().unwrap(),
            99,
            SessionSeq(10),
            usize::MAX
        ),
        Err(ContractError::InvalidCut)
    ));
    assert!(matches!(
        graph.deadlock_query_with_budget(
            cid(2),
            open.deadline().unwrap(),
            100,
            SessionSeq(10),
            usize::MAX
        ),
        Err(ContractError::InvalidTarget)
    ));
    let incomplete = claim(2, 1, &[(Kind::DependsOn, 3)]);
    assert!(matches!(
        Snapshot::capture(&[&incomplete], limits()),
        Err(ContractError::InvalidTarget)
    ));

    let mut satisfied = claim(3, 1, &[]);
    claim::tests::local_projection_for_graph(&mut satisfied);
    let graph = Snapshot::capture(&[&satisfied], limits()).unwrap();
    assert!(graph.satisfied(cid(3)).unwrap());
    assert!(
        graph
            .deadlock_query_with_budget(
                cid(3),
                satisfied.deadline().unwrap(),
                100,
                SessionSeq(100),
                usize::MAX
            )
            .unwrap()
            .is_none()
    );

    let mut terminal = claim(4, 1, &[]);
    terminal
        .apply(
            &terminal.binding(),
            Principal::Actor(terminal.issuer()),
            claim::ClaimIntent::Cancel {
                cut: claim::ClaimCut {
                    position: SessionSeq(6),
                    cause: ContentHash([8; 32]),
                },
            },
        )
        .unwrap();
    let graph = Snapshot::capture(&[&terminal], limits()).unwrap();
    assert!(matches!(
        query(&graph, &terminal, usize::MAX),
        Err(ContractError::InvalidTransition)
    ));
}

#[test]
fn exact_peak_precharges_all_buffers_and_each_allocation_failure_preserves_snapshot() {
    let (a, b) = cycle();
    let graph = Snapshot::capture(&[&a, &b], limits()).unwrap();
    let original = graph.bindings().collect::<Vec<_>>();
    let charge = bytes::fail_after(0, || graph.deadlock_charge()).unwrap();
    bytes::fail_after(4, || {
        assert!(matches!(
            query(&graph, &a, charge - 1),
            Err(ContractError::Capacity)
        ));
        assert_eq!(bytes::remaining_allocations(), Some(4));
        assert!(query(&graph, &a, charge).unwrap().is_some());
        assert_eq!(bytes::remaining_allocations(), Some(0));
    });
    for failed in 0..4 {
        assert!(matches!(
            bytes::fail_after(failed, || query(&graph, &a, charge)),
            Err(ContractError::Capacity)
        ));
        assert_eq!(graph.bindings().collect::<Vec<_>>(), original);
        graph.check_owner(&a, &[&b]).unwrap();
    }
    assert!(query(&graph, &a, charge).unwrap().is_some());
    assert_eq!(
        charge,
        size_of::<Deadlock<'_>>()
            + 2 * buffer::<bool>(2).unwrap()
            + 2 * buffer::<usize>(2).unwrap()
    );
}

#[test]
fn allocator_excess_and_shared_visit_exhaustion_never_become_negative_scc_answers() {
    let (a, b) = cycle();
    let mut graph = Snapshot::capture(&[&a, &b], limits()).unwrap();
    EXCESS.with(|value| assert!(!value.replace(true)));
    assert!(matches!(
        query(&graph, &a, usize::MAX),
        Err(ContractError::Capacity)
    ));
    assert!(!EXCESS.with(std::cell::Cell::get));
    // Each directional walk needs four visits. Seven cannot cover both; eleven
    // cannot also cover component selection and its canonical fingerprint.
    for visits in [7, 11] {
        graph.limits.visits = visits;
        assert!(matches!(
            query(&graph, &a, usize::MAX),
            Err(ContractError::Capacity)
        ));
    }
    graph.limits.visits = 12;
    assert!(query(&graph, &a, usize::MAX).unwrap().is_some());
    graph.satisfied.pop();
    assert!(matches!(
        query(&graph, &a, usize::MAX),
        Err(ContractError::InvalidManifest)
    ));
}

#[test]
fn self_cycle_requires_an_actual_unsettled_traversable_edge() {
    let source = claim(1, 1, &[(Kind::DependsOn, 1)]);
    let mut graph = Snapshot::capture(&[&source], limits()).unwrap();
    assert!(query(&graph, &source, usize::MAX).unwrap().is_some());
    // Exercise the metadata-level settled-edge guard separately from capture:
    // Released is independent of local work satisfaction. A retained self edge
    // alone must not turn a settled release predicate into a deadlock witness.
    graph.edges[0].predicate = Predicate::Released;
    graph.incoming[0].predicate = Predicate::Released;
    graph.nodes[0].released = true;
    assert!(!graph.satisfied(cid(1)).unwrap());
    assert!(graph.settled(graph.edges[0]).unwrap());
    assert!(query(&graph, &source, usize::MAX).unwrap().is_none());
}
