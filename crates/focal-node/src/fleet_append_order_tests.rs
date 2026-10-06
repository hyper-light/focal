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

/// A frame let go past its patience that comes after all — stale — and is
/// taken by the log moves the hole past its entries: what is refused behind
/// the hole is the loss's, refused at the log's new end. The loss was noted
/// before the late frame was stepped, where the log ended without it, and
/// the refusals behind it were judged the order's (CI on 8d4f322: a
/// follower took two late frames, and fifty refusals behind them were
/// counted in order).
#[test]
fn a_late_frame_the_log_takes_moves_the_loss_with_it() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let opening = next_append(&mut fixture, 40, 2);
    deliver(&mut fixture, 2, opening);
    assert_eq!(refused(&mut fixture, 2), (0, 0));
    let first = next_append(&mut fixture, 41, 2);
    let second = next_append(&mut fixture, 42, 2);
    let third = next_append(&mut fixture, 43, 2);
    let fourth = next_append(&mut fixture, 44, 2);
    // The second overtakes the first and is held for it; its patience
    // passes, and it is let go and refused: the loss's.
    deliver(&mut fixture, 2, second);
    fixture.owners[1].resequencer.expire(u64::MAX).unwrap();
    fixture.owners[1].step_due();
    assert_eq!(refused(&mut fixture, 2), (1, 0));
    // The third comes in its turn and is refused behind the hole.
    deliver(&mut fixture, 2, third);
    assert_eq!(refused(&mut fixture, 2), (2, 0));
    // The first comes late, stale, and the log takes it.
    let before = fixture.owners[1].session.last_log_index().unwrap();
    deliver(&mut fixture, 2, first);
    assert_eq!(refused(&mut fixture, 2), (2, 0));
    assert_eq!(fixture.owners[1].frames_stale, 1);
    assert_eq!(
        fixture.owners[1].session.last_log_index().unwrap(),
        before + 1,
        "the late frame was taken"
    );
    // The fourth, behind the second and third the log still lacks, is
    // refused at the log's new end: the loss's, not the order's.
    deliver(&mut fixture, 2, fourth);
    assert_eq!(refused(&mut fixture, 2), (3, 0));
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

/// An append the leader gives up before it leaves — no room for its frame
/// here — still takes its place in the order, so its peer sees the gap and
/// takes the refusals behind it for the loss's (27 §12): one given up
/// before it had a sequence left no gap, and the appends after it were
/// refused in an order that showed none (a peer's runs of the lossy path,
/// its core queueing more appends a Ready). And the order is the term's:
/// a later term begins its own.
#[test]
fn an_append_given_up_before_it_leaves_keeps_its_place_in_the_order() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let owner = &mut fixture.owners[0];
    let term = owner.session.scalars().term;
    let append = |term: u64| focal_consensus::Message {
        msg_type: focal_consensus::MessageType::MsgAppend,
        from: 1,
        to: 2,
        term,
        ..focal_consensus::Message::default()
    };
    owner.send(&[append(term)]).unwrap();
    let first = owner.ordered.get(&2).copied().unwrap();
    // No room for the next frame: it is given up, its sequence spent.
    let stats = owner.budget.stats();
    let hog = owner
        .budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            stats.limit - stats.used,
        )
        .unwrap()
        .commit();
    let dropped = owner.dropped;
    owner.send(&[append(term)]).unwrap();
    assert_eq!(owner.dropped, dropped + 1, "the frame was given up");
    drop(hog);
    owner.send(&[append(term)]).unwrap();
    let mut sequences = Vec::new();
    while let Ok(frame) = fixture.outgoing[0].try_recv() {
        if let Operation::RaftOrdered {
            epoch, sequence, ..
        } = frame.request.operation
            && frame.target == 2
        {
            sequences.push((epoch, sequence));
        }
    }
    assert_eq!(
        sequences,
        [(term, first.1), (term, first.1 + 2)],
        "the frame given up left its gap"
    );
    // A later term's appends begin an order of their own.
    let owner = &mut fixture.owners[0];
    owner.send(&[append(term + 1)]).unwrap();
    let frame = fixture.outgoing[0].try_recv().unwrap();
    assert!(matches!(
        frame.request.operation,
        Operation::RaftOrdered { epoch, sequence: 1, .. } if epoch == term + 1
    ));
}
