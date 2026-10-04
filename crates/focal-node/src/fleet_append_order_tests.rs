//! A follower tells the refusals the order of a leader's appends should
//! have spared from what a loss costs (27 §12, the audit's F42): its
//! answers are judged once its log is durable, after frames stepped since,
//! so a loss is kept as the place it left in the log. The leader's own
//! appends reach the follower's authenticated ingress overtaken, plain or
//! with one of them lost.
use super::*;

fn node_peer(node: u64) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(u128::from(node)),
        tenants: [identity().ledger.tenant].into_iter().collect(),
        role: PeerRole::Node { node_id: node },
    })
    .unwrap()
}
/// The ordered append the leader (node 1) sends follower `to` for one
/// more proposal; what it sends the other follower is not delivered.
fn next_append(fixture: &mut Fixture, id: u128, to: u64) -> RequestEnvelope {
    let input = demo::request(
        &identity(),
        &format!("append-{id}"),
        identity().issuer,
        claim(id),
        vec![],
    );
    assert!(matches!(
        fixture.owners[0].session.propose(&input).unwrap(),
        Submission::Pending(_)
    ));
    fixture.owners[0].drain().unwrap();
    let mut appends = Vec::new();
    while let Ok(frame) = fixture.outgoing[0].try_recv() {
        if frame.target == to && matches!(frame.request.operation, Operation::RaftOrdered { .. }) {
            appends.push(frame.request);
        }
    }
    assert_eq!(appends.len(), 1, "one append for one proposal");
    appends.pop().unwrap()
}
/// `request` as a leader of an older binary sends it: plain.
fn plain(request: &RequestEnvelope) -> RequestEnvelope {
    let Operation::RaftOrdered { group, message, .. } = &request.operation else {
        panic!("an ordered frame");
    };
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        operation: Operation::Raft {
            group: *group,
            message: message.clone(),
        },
        ..request.clone()
    }
}
/// The leader's frame through follower `to`'s authenticated ingress.
fn deliver(fixture: &mut Fixture, to: u64, request: RequestEnvelope) {
    let owner = &mut fixture.owners[usize::try_from(to - 1).unwrap()];
    let verified = verify_request(node_peer(1), request, &owner.limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Completion, 64 * 1024)
        .unwrap()
        .commit();
    let (send, _answered) = oneshot::channel();
    owner.request(verified, send, charge, None, None);
}
/// What follower `to` has refused, and of that what the order should
/// have spared, once it has answered what it stepped.
fn refused(fixture: &mut Fixture, to: u64) -> (u64, u64) {
    let owner = &mut fixture.owners[usize::try_from(to - 1).unwrap()];
    owner.drain().unwrap();
    (owner.appends_rejected, owner.appends_rejected_in_order)
}

#[test]
fn a_follower_counts_a_refusal_as_the_orders_only_where_no_loss_explains_it() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    // The first frame the follower sees of the leader opens its lane: the
    // one expected.
    let opening = next_append(&mut fixture, 19, 2);
    deliver(&mut fixture, 2, opening);
    assert_eq!(refused(&mut fixture, 2), (0, 0));
    // An append that overtook the one before it is held for it, and both
    // are taken in their order.
    let first = next_append(&mut fixture, 20, 2);
    let second = next_append(&mut fixture, 21, 2);
    deliver(&mut fixture, 2, second);
    assert_eq!(fixture.owners[1].frames_held, 1);
    deliver(&mut fixture, 2, first);
    assert_eq!(refused(&mut fixture, 2), (0, 0));
    // One stepped ahead of its turn — sent plain, past the order — is
    // refused, and counted as the order's: the leader sends ordered and
    // had its appends taken in this term, and nothing of it was lost.
    let third = next_append(&mut fixture, 22, 2);
    let fourth = next_append(&mut fixture, 23, 2);
    deliver(&mut fixture, 2, plain(&fourth));
    assert_eq!(refused(&mut fixture, 2), (1, 1));
    deliver(&mut fixture, 2, third);
    assert_eq!(refused(&mut fixture, 2), (1, 1));
    // The ordered copy of the fourth never comes: the fifth is held for it
    // until its patience passes, then let go and refused — the loss's, not
    // the order's.
    let fifth = next_append(&mut fixture, 24, 2);
    deliver(&mut fixture, 2, fifth);
    assert_eq!(fixture.owners[1].frames_held, 2);
    fixture.owners[1].resequencer.expire(u64::MAX).unwrap();
    fixture.owners[1].step_due();
    assert_eq!(fixture.owners[1].frames_let_go, 1);
    assert_eq!(refused(&mut fixture, 2), (2, 1));
}

#[test]
fn a_leader_that_sends_its_appends_plain_is_spared_nothing_by_the_order() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    // A leader of an older binary: its appends come plain, and one that
    // overtook another is refused as it always was, the order's to spare
    // in no term.
    let first = next_append(&mut fixture, 30, 3);
    let second = next_append(&mut fixture, 31, 3);
    let third = next_append(&mut fixture, 32, 3);
    deliver(&mut fixture, 3, plain(&first));
    assert_eq!(refused(&mut fixture, 3), (0, 0));
    deliver(&mut fixture, 3, plain(&third));
    assert_eq!(refused(&mut fixture, 3), (1, 0));
    deliver(&mut fixture, 3, plain(&second));
    assert_eq!(refused(&mut fixture, 3), (1, 0));
}
