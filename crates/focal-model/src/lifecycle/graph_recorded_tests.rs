use super::*;
use crate::lifecycle::graph::tests as f;

fn retained(claims: &[&ClaimState]) -> (Vec<RecordedNode>, Vec<RecordedEdge>) {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    for claim in claims {
        let origin = if claim.is_terminal() && claim.status() != ClaimStatus::Satisfied {
            match claim.terminal_cut() {
                Some(ClaimTerminalCut::Graph(cut))
                    if cut.kind() == FailureKind::DependencyFailed =>
                {
                    Some(cut.origin().snapshot_v1())
                }
                _ => Some(OriginSnapshotV1 {
                    binding: claim.binding(),
                    created: claim.created(),
                    terminal: claim.local_sealed_at().unwrap(),
                }),
            }
        } else {
            None
        };
        nodes.push(RecordedNode {
            binding: claim.binding(),
            created: claim.created(),
            status: claim.status(),
            local_complete: claim.local_complete(),
            released: claim.scopes().released(),
            deadline: claim.deadline(),
            origin,
        });
        for edge in claim.graph().obligations() {
            edges.push(RecordedEdge {
                source: id(claim.binding()),
                target: match edge.kind {
                    Kind::DependsOn => WaitPredicate::Satisfied(edge.target),
                    Kind::Awaits => WaitPredicate::Terminal(edge.target),
                },
                propagates_failure: edge.kind == Kind::DependsOn,
                runtime: false,
            });
        }
    }
    (nodes, edges)
}
fn graph(nodes: &[RecordedNode], edges: &[RecordedEdge], sequence: u64) -> RecordedGraph {
    RecordedGraph::build(
        nodes,
        edges,
        SessionSeq(sequence),
        Limits {
            nodes: 32,
            edges: 128,
            visits: usize::MAX,
        },
        RecordedGraph::construction_charge(nodes.len(), edges.len()).unwrap(),
        &mut VisitBudget::new(usize::MAX),
    )
    .unwrap()
}

#[test]
fn retained_real_dependency_path_and_earliest_origin_reject_scalar_substitution() {
    let a = f::claim(1, 3, &[(Kind::DependsOn, 2), (Kind::DependsOn, 3)]);
    let mut b = f::claim(2, 2, &[]);
    let mut c = f::claim(3, 1, &[]);
    f::cancel(&mut b, 8);
    f::cancel(&mut c, 9);
    let actual = Snapshot::capture(&[&a, &b, &c], f::limits()).unwrap();
    let cut = actual
        .dependency_failure(f::cid(1))
        .unwrap()
        .cut(SessionSeq(10))
        .unwrap();
    assert_eq!(cut.origin().binding(), c.binding());
    let (mut nodes, edges) = retained(&[&a, &b, &c]);
    let checked = graph(&nodes, &edges, 9);
    let charge = checked.verification_charge().unwrap();
    checked
        .verify_terminal(f::cid(1), cut, charge, &mut VisitBudget::new(usize::MAX))
        .unwrap();
    let mut corrupted = cut.snapshot_v1();
    corrupted.fingerprint.0[0] ^= 1;
    assert!(
        checked
            .verify_terminal(
                f::cid(1),
                TerminalCut::hydrate_v1(corrupted).unwrap(),
                charge,
                &mut VisitBudget::new(usize::MAX)
            )
            .is_err()
    );
    nodes[2].binding.revision.0 += 1;
    let stale = graph(&nodes, &edges, 9);
    assert!(
        stale
            .verify_terminal(f::cid(1), cut, charge, &mut VisitBudget::new(usize::MAX))
            .is_err()
    );
}

#[test]
fn real_scc_trigger_victim_fingerprint_and_negative_expiry_are_verified() {
    let a = f::claim(1, 2, &[(Kind::Awaits, 2)]);
    let b = f::claim(2, 1, &[(Kind::Awaits, 1)]);
    let actual = Snapshot::capture(&[&a, &b], f::limits()).unwrap();
    let deadline = a.deadline().unwrap();
    let cut = actual
        .deadlock(f::cid(1), deadline, deadline.at)
        .unwrap()
        .cut(SessionSeq(10))
        .unwrap();
    let (nodes, edges) = retained(&[&a, &b]);
    let checked = graph(&nodes, &edges, 2);
    let charge = checked.verification_charge().unwrap();
    checked
        .verify_terminal(f::cid(2), cut, charge, &mut VisitBudget::new(usize::MAX))
        .unwrap();
    assert!(
        checked
            .verify_terminal(f::cid(1), cut, charge, &mut VisitBudget::new(usize::MAX))
            .is_err()
    );
    assert!(
        checked
            .verify_expiry(
                f::cid(1),
                deadline,
                deadline.at,
                charge,
                &mut VisitBudget::new(usize::MAX)
            )
            .is_err()
    );
    let acyclic = f::claim(3, 1, &[]);
    let (nodes, edges) = retained(&[&acyclic]);
    let checked = graph(&nodes, &edges, 2);
    checked
        .verify_expiry(
            f::cid(3),
            acyclic.deadline().unwrap(),
            100,
            checked.verification_charge().unwrap(),
            &mut VisitBudget::new(usize::MAX),
        )
        .unwrap();
}

#[test]
fn real_release_fingerprint_retains_complete_snapshot_membership_and_revision() {
    let mut a = f::claim(1, 1, &[]);
    let b = f::claim(2, 2, &[(Kind::Awaits, 1)]);
    f::local(&mut a);
    let snapshot = Snapshot::capture(&[&a, &b], f::limits()).unwrap();
    let cut = crate::lifecycle::claim::ClaimCut {
        position: SessionSeq(10),
        cause: snapshot.release(f::cid(1)).unwrap().fingerprint().unwrap(),
    };
    let (nodes, edges) = retained(&[&a, &b]);
    let checked = graph(&nodes, &edges, 10);
    checked
        .verify_release(f::cid(1), cut, &mut VisitBudget::new(usize::MAX))
        .unwrap();
    let incomplete = graph(&nodes[..1], &[], 10);
    assert!(
        incomplete
            .verify_release(f::cid(1), cut, &mut VisitBudget::new(usize::MAX))
            .is_err()
    );
}

#[test]
fn exact_build_and_proof_work_and_heap_refuse_one_short_and_missing_endpoints() {
    let a = f::claim(1, 1, &[(Kind::DependsOn, 2)]);
    let mut b = f::claim(2, 2, &[]);
    f::cancel(&mut b, 9);
    let actual = Snapshot::capture(&[&a, &b], f::limits()).unwrap();
    let cut = actual
        .dependency_failure(f::cid(1))
        .unwrap()
        .cut(SessionSeq(10))
        .unwrap();
    let (nodes, edges) = retained(&[&a, &b]);
    let charge = RecordedGraph::construction_charge(nodes.len(), edges.len()).unwrap();
    let limits = Limits {
        nodes: 32,
        edges: 128,
        visits: usize::MAX,
    };
    let mut visits = VisitBudget::new(usize::MAX);
    let checked =
        RecordedGraph::build(&nodes, &edges, SessionSeq(9), limits, charge, &mut visits).unwrap();
    let work = usize::MAX - visits.remaining();
    RecordedGraph::build(
        &nodes,
        &edges,
        SessionSeq(9),
        limits,
        charge,
        &mut VisitBudget::new(work),
    )
    .unwrap();
    assert!(
        RecordedGraph::build(
            &nodes,
            &edges,
            SessionSeq(9),
            limits,
            charge,
            &mut VisitBudget::new(work - 1)
        )
        .is_err()
    );
    assert!(
        RecordedGraph::build(
            &nodes,
            &edges,
            SessionSeq(9),
            limits,
            charge - 1,
            &mut VisitBudget::new(usize::MAX)
        )
        .is_err()
    );
    assert!(
        RecordedGraph::build(
            &nodes[..1],
            &edges,
            SessionSeq(9),
            limits,
            charge,
            &mut VisitBudget::new(usize::MAX)
        )
        .is_err()
    );
    let mut visits = VisitBudget::new(usize::MAX);
    checked
        .verify_terminal(
            f::cid(1),
            cut,
            checked.verification_charge().unwrap(),
            &mut visits,
        )
        .unwrap();
    let work = usize::MAX - visits.remaining();
    checked
        .verify_terminal(
            f::cid(1),
            cut,
            checked.verification_charge().unwrap(),
            &mut VisitBudget::new(work),
        )
        .unwrap();
    assert!(
        checked
            .verify_terminal(
                f::cid(1),
                cut,
                checked.verification_charge().unwrap(),
                &mut VisitBudget::new(work - 1)
            )
            .is_err()
    );
}
