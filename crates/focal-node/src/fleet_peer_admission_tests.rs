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
        let (Operation::Raft { message, .. } | Operation::RaftOrdered { message, .. }) =
            &request.operation
        else {
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
/// The leader's next ordered append to follower `to`, for one more proposal.
fn next_append(fixture: &mut Fixture, id: u128, to: u64) -> RequestEnvelope {
    let input = demo::request(
        &identity(),
        &format!("waits-{id}"),
        identity().issuer,
        claim(id),
        vec![],
    );
    assert!(matches!(
        fixture.owners[0].session.propose(&input).unwrap(),
        Submission::Pending(_)
    ));
    fixture.owners[0].drain().unwrap();
    let mut appends: Vec<RequestEnvelope> = sent(fixture, 0)
        .into_iter()
        .filter(|(target, request)| {
            *target == to && matches!(request.operation, Operation::RaftOrdered { .. })
        })
        .map(|(_, request)| request)
        .collect();
    assert_eq!(appends.len(), 1, "one append for one proposal");
    appends.pop().unwrap()
}
/// The leader's frame through follower `to`'s authenticated ingress.
fn deliver(
    fixture: &mut Fixture,
    to: u64,
    request: RequestEnvelope,
) -> oneshot::Receiver<OwnedResponse> {
    let owner = &mut fixture.owners[usize::try_from(to - 1).unwrap()];
    let verified = verify_request(node_peer(1), request, &owner.limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Completion, 64 * 1024)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    owner.request(verified, send, charge, None, None);
    receive
}
/// The writes `owner`'s log has completed: what a wait on its persistence
/// is charged in (27 §3.1 P8).
fn writes(owner: &Owner) -> u64 {
    owner
        .session
        .shared_wal()
        .unwrap()
        .stats()
        .map_or(0, |stats| stats.group_commits)
}

/// A leader's append that comes while its follower's write is in flight
/// waits for the write (`WaitingFrame`), and is stepped and answered once
/// the write is durable. Consensus takes no input until then, and the frame
/// was refused, and lost to its leader: on the Linux comparison at 1,000
/// writes a second a follower lost two of five of its frames to its leader,
/// each counted lost, the leader reported unreachable, and what it
/// acknowledged waiting for its next exchange.
#[test]
fn a_frame_that_comes_while_the_write_is_in_flight_waits_for_it() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let first = next_append(&mut fixture, 41, 2);
    let second = next_append(&mut fixture, 42, 2);
    // The follower owner shares its thread as a fleet's does: it does not
    // wait for its writes, and takes work while one is in flight.
    fixture.owners[1].nonblocking = true;
    let pause = fixture.owners[1]
        .session
        .shared_wal()
        .unwrap()
        .pause_for_test()
        .unwrap();
    let mut first_answer = deliver(&mut fixture, 2, first);
    // The first append's step leaves the follower a write; the paused log
    // holds it in flight.
    fixture.owners[1].drain().unwrap();
    assert!(fixture.owners[1].session.persistence_pending());
    let mut second_answer = deliver(&mut fixture, 2, second);
    assert_eq!(fixture.owners[1].waiting_frames.len(), 1, "it waits");
    assert_eq!(fixture.owners[1].frames_waited, 1);
    assert!(
        matches!(
            second_answer.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "a frame that waits is not refused"
    );
    assert!(matches!(
        first_answer.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(pause);
    let owner = &mut fixture.owners[1];
    let mut wait = focal_timing::ProgressDeadline::begin(
        &[writes(owner)],
        u64::MAX,
        crate::test_waits::FROZEN,
    );
    while owner.session.persistence_pending()
        || owner.session.has_ready()
        || owner.drain_owed
        || !owner.waiting_frames.is_empty()
    {
        owner.progress_group().unwrap();
        if let Err(spent) = wait.check(&[writes(owner)]) {
            panic!("the follower never settled: {spent}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    for answer in [&mut first_answer, &mut second_answer] {
        assert!(
            matches!(
                answer.try_recv().unwrap().envelope().result,
                Response::PeerAccepted
            ),
            "both appends are taken and answered at the fence"
        );
    }
    let follower = &fixture.owners[1];
    assert_eq!(
        follower.appends_rejected, 0,
        "nothing was lost to the order"
    );
    assert_eq!(follower.frames_let_go, 0);
}

/// A frame held for one that never comes is let go past its patience, and
/// the patience runs in the owner's periods whether or not the replica's
/// write is in flight: one let go then waits for the write, as one that
/// comes then does, and is stepped and answered once it is durable. This is
/// the path a fleet's owner takes: it hands a session no work while its
/// write is in flight, but lets its held frames go at every period.
#[test]
fn a_frame_let_go_while_the_write_is_in_flight_waits_for_it() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let first = next_append(&mut fixture, 51, 2);
    let _lost = next_append(&mut fixture, 52, 2);
    let third = next_append(&mut fixture, 53, 2);
    fixture.owners[1].nonblocking = true;
    let mut first_answer = deliver(&mut fixture, 2, first);
    // The third overtook the second, which the path lost: it is held.
    let mut third_answer = deliver(&mut fixture, 2, third);
    assert_eq!(fixture.owners[1].resequencer.held(), 1);
    let pause = fixture.owners[1]
        .session
        .shared_wal()
        .unwrap()
        .pause_for_test()
        .unwrap();
    fixture.owners[1].drain().unwrap();
    assert!(fixture.owners[1].session.persistence_pending());
    // The owner's periods pass with the write in flight, until the held
    // frame's patience has: it is let go into the write.
    let owner = &mut fixture.owners[1];
    let mut wait = focal_timing::ProgressDeadline::begin(
        &[owner.pace.periods()],
        1_000,
        crate::test_waits::FROZEN,
    );
    while owner.frames_let_go == 0 {
        owner.progress_group().unwrap();
        if let Err(spent) = wait.check(&[owner.pace.periods()]) {
            panic!("the held frame was never let go: {spent}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(owner.session.persistence_pending());
    assert_eq!(owner.waiting_frames.len(), 1, "it waits for the write");
    assert!(
        matches!(
            third_answer.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "a frame let go into a write in flight is not refused"
    );
    drop(pause);
    let mut wait = focal_timing::ProgressDeadline::begin(
        &[writes(owner)],
        u64::MAX,
        crate::test_waits::FROZEN,
    );
    while owner.session.persistence_pending()
        || owner.session.has_ready()
        || owner.drain_owed
        || !owner.waiting_frames.is_empty()
    {
        owner.progress_group().unwrap();
        if let Err(spent) = wait.check(&[writes(owner)]) {
            panic!("the follower never settled: {spent}");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    for answer in [&mut first_answer, &mut third_answer] {
        assert!(matches!(
            answer.try_recv().unwrap().envelope().result,
            Response::PeerAccepted
        ));
    }
    // The append behind the lost one was refused for the entry it lacks:
    // the loss's, never the order's.
    assert_eq!(owner.appends_rejected, 1);
    assert_eq!(owner.appends_rejected_in_order, 0);
}
