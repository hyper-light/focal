//! A batch of requests an owner takes is written at once (27 §9): its
//! proposals go in one write, not one write each.
use super::*;

/// One participant request opening the issuer's epoch under its own id: a
/// proposal of its own.
fn opening(fixture: &mut Fixture, id: u128) -> Work {
    let owner = &mut fixture.owners[0];
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: identity().ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    };
    let verified = verify_request(actor(), request, &owner.client_limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Pending, BudgetLane::Completion, 64 * 1024)
        .unwrap()
        .commit();
    let (send, _answered) = oneshot::channel();
    Work::Request(
        Box::new(AdmittedRequest {
            verified,
            witness: None,
            native: None,
        }),
        send,
        charge,
    )
}

/// The leader's owner takes four proposals as a batch and writes them in
/// one group commit; taken one by one, each starts a write of its own. An
/// owner that drained for every proposal as it came carried one proposal a
/// write, and a session with a write out is dispatched nothing more, so a
/// group committing six entries a second held 28 requests queued behind it
/// (six fleets at once, 2026-10-03).
#[test]
fn an_owner_writes_the_proposals_of_a_batch_at_once() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let wal = fixture.owners[0].session.shared_wal().unwrap();
    let commits = || wal.stats().unwrap().group_commits;
    // One at a time: each proposal its own write.
    let before = commits();
    let last = fixture.owners[0].session.last_log_index().unwrap();
    for id in 900..902 {
        let work = opening(&mut fixture, id);
        assert!(!fixture.owners[0].accept(work).unwrap());
    }
    assert_eq!(
        fixture.owners[0].session.last_log_index().unwrap() - last,
        2,
        "two proposals"
    );
    assert_eq!(commits() - before, 2, "taken one by one: a write each");
    // A batch: four proposals, one write at the batch's drain.
    let before = commits();
    let last = fixture.owners[0].session.last_log_index().unwrap();
    fixture.owners[0].batching = true;
    for id in 910..914 {
        let work = opening(&mut fixture, id);
        assert!(!fixture.owners[0].accept(work).unwrap());
    }
    assert_eq!(
        commits(),
        before,
        "nothing written before the batch's drain"
    );
    fixture.owners[0].batching = false;
    fixture.owners[0].drain().unwrap();
    assert_eq!(
        fixture.owners[0].session.last_log_index().unwrap() - last,
        4,
        "four proposals"
    );
    assert_eq!(commits() - before, 1, "a batch: one write");
}

fn node_peer(node: u64) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(node as u128),
        tenants: [identity().ledger.tenant].into_iter().collect(),
        role: PeerRole::Node { node_id: node },
    })
    .unwrap()
}
/// Node 2's answer to a heartbeat sent to the follower `owner`, which the
/// core passes over: a follower takes no heartbeat's answer.
fn passed_over(owner: &Owner, id: u128) -> (Work, oneshot::Receiver<OwnedResponse>) {
    let message = focal_consensus::Message {
        msg_type: focal_consensus::MessageType::MsgHeartbeatResponse,
        from: 2,
        to: 3,
        term: owner.session.scalars().term,
        ..Default::default()
    };
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: identity().ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::Raft {
            group: owner.session.group_id(),
            message: focal_consensus::encode_message(&message).unwrap(),
        },
    };
    let verified = verify_request(node_peer(2), request, &owner.limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Completion, 64 * 1024)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    (
        Work::Request(
            Box::new(AdmittedRequest {
                verified,
                witness: None,
                native: None,
            }),
            send,
            charge,
        ),
        receive,
    )
}

/// A peer's frame whose step leaves the replica nothing ready is answered
/// by the drain that ends its batch — an empty poll, as the drain of a frame
/// taken alone was. Before, a batch's end drained only a replica with
/// something ready, and such a frame waited for its deadline: on a path that
/// carries a peer's frames one after another everything behind it waited
/// with it, and fleet_group's quorum test held elections to term 23 with no
/// leader (the gate on 7a4fba1). Both ends of a batch: a replica's own
/// owner's (`Owner::take`), and a grouped owner's, which makes the session
/// due at once and drains it at its pass.
#[test]
fn a_peer_frame_that_leaves_nothing_ready_is_answered_by_its_batchs_drain() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    for _ in 0..10 {
        fixture.pump();
    }
    let follower = &mut fixture.owners[2];
    let (_sender, receiver) = mpsc::sync_channel(1);
    let (work, mut answer) = passed_over(follower, 1);
    assert!(!follower.take(work, &receiver).unwrap());
    assert!(!follower.session.has_ready(), "nothing was left ready");
    assert!(matches!(
        answer.try_recv().unwrap().envelope().result,
        Response::PeerAccepted
    ));
    // A grouped owner's session: every request is taken in a batch.
    follower.batching = true;
    let (work, mut answer) = passed_over(follower, 2);
    assert!(!follower.accept(work).unwrap());
    assert!(!follower.session.has_ready(), "nothing was left ready");
    assert!(answer.try_recv().is_err(), "answered at the batch's drain");
    assert!(follower.group_deadline().unwrap() <= Instant::now());
    assert!(!follower.progress_group().unwrap());
    assert!(matches!(
        answer.try_recv().unwrap().envelope().result,
        Response::PeerAccepted
    ));
}
