//! Raft acknowledgments are admitted beside the participants' pending bound
//! (the audit's F56): a participant queue at its bound never refuses the
//! peer traffic its own completion waits on.
use super::*;

fn node_peer(node: u64) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(node as u128),
        tenants: [identity().ledger.tenant].into_iter().collect(),
        role: PeerRole::Node { node_id: node },
    })
    .unwrap()
}
fn summary(fixture: &mut Fixture, id: u128) -> oneshot::Receiver<OwnedResponse> {
    let owner = &mut fixture.owners[0];
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: identity().ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::Summary,
    };
    let verified = verify_request(actor(), request, &owner.client_limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, 64 * 1024)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    owner.request(verified, send, charge, None, None);
    receive
}
/// Every frame owner `from` sent, as (target, request).
fn sent(fixture: &mut Fixture, from: usize) -> Vec<(u64, RequestEnvelope)> {
    let mut frames = Vec::new();
    while let Ok(frame) = fixture.outgoing[from].try_recv() {
        frames.push((frame.target, frame.request));
    }
    frames
}
/// A peer's frame through the leader owner's authenticated ingress — the
/// path a participant's request takes, admission included.
fn ingress(
    fixture: &mut Fixture,
    from: u64,
    request: RequestEnvelope,
) -> oneshot::Receiver<OwnedResponse> {
    let owner = &mut fixture.owners[0];
    let verified = verify_request(node_peer(from), request, &owner.limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Completion, 64 * 1024)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    owner.request(verified, send, charge, None, None);
    receive
}

#[test]
fn a_full_participant_queue_still_admits_the_acknowledgments_it_waits_on() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    fixture.owners[0].config.pending_clients = 1;
    // One participant read takes the only participant slot; it waits for
    // the leader's read barrier, a quorum of heartbeat answers.
    let mut waiting = summary(&mut fixture, 1);
    fixture.owners[0].drain().unwrap();
    assert!(matches!(
        waiting.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    // A second participant is refused: the participants' bound holds.
    let mut second = summary(&mut fixture, 2);
    assert!(matches!(
        second.try_recv().unwrap().envelope().result,
        Response::Error(AccessError::Capacity)
    ));
    // The leader's heartbeats, carrying the read's context, reach the
    // followers; their answers come back through the leader's authenticated
    // ingress — the path a full participant queue used to refuse.
    let heartbeats = sent(&mut fixture, 0);
    assert!(!heartbeats.is_empty(), "the barrier asks the followers");
    for (target, request) in heartbeats {
        let Operation::Raft { message, .. } = &request.operation else {
            panic!("Raft expected")
        };
        fixture.owners[target as usize - 1]
            .session
            .step_authenticated(1, message)
            .unwrap();
    }
    let mut answered = Vec::new();
    for follower in 1..3usize {
        fixture.owners[follower].drain().unwrap();
        for (target, request) in sent(&mut fixture, follower) {
            assert_eq!(target, 1);
            let receiver = ingress(&mut fixture, follower as u64 + 1, request);
            answered.push(receiver);
        }
    }
    assert!(answered.len() >= 2, "both followers answer");
    fixture.owners[0].drain().unwrap();
    for receiver in &mut answered {
        assert!(
            matches!(
                receiver.try_recv().unwrap().envelope().result,
                Response::PeerAccepted
            ),
            "a peer's acknowledgment is admitted beside the full participant queue"
        );
    }
    // With its quorum heard, the participant's read completes.
    for _ in 0..10 {
        fixture.pump();
    }
    assert!(matches!(
        waiting.try_recv().unwrap().envelope().result,
        Response::Summary(_)
    ));
    // The peers' admission is bounded too: the members' in-flight windows.
    let reserve = fixture.owners[0].peer_reserve();
    assert_eq!(
        reserve,
        3 * fixture.owners[0].session.inflight_window(),
        "three voters, each its window"
    );
}
