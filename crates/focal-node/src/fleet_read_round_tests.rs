use super::*;
use focal_consensus::MessageType;

fn summary(owner: &Owner, id: u128) -> (Work, oneshot::Receiver<OwnedResponse>) {
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: identity().ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::Summary,
    };
    let verified = verify_request(actor(), request, &owner.client_limits).unwrap();
    let bound = crate::ledger_summary::response_bytes(owner.client_limits.max_frame_bytes).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, bound * 32 + 4096)
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
/// The heartbeats that ask for reads among what the leader has sent and
/// not yet had carried: to whom, and for which read.
fn rounds(fixture: &mut Fixture) -> Vec<(u64, Vec<u8>)> {
    let mut rounds = Vec::new();
    let mut frames = Vec::new();
    while let Ok(frame) = fixture.outgoing[0].try_recv() {
        frames.push(frame);
    }
    for frame in frames {
        let (Operation::Raft { message, .. } | Operation::RaftOrdered { message, .. }) =
            &frame.request.operation
        else {
            panic!("Raft expected")
        };
        let decoded = focal_consensus::decode_message(message).unwrap();
        if decoded.msg_type == MessageType::MsgHeartbeat && !decoded.context.is_empty() {
            rounds.push((decoded.to, decoded.context.clone()));
        }
        fixture.owners[frame.target as usize - 1]
            .session
            .step_authenticated(1, message)
            .unwrap();
    }
    rounds
}

/// Reads queued for the owner together are asked for by one round of
/// heartbeats: the owner takes what is queued behind a read as one batch,
/// and the batch's drain sends its round (27 §9). A hundred reads leave in
/// two heartbeats, one to each follower, where a drain after each sent two
/// hundred; a read queued alone leaves in two as well, with its own drain;
/// and every one of them is answered. A write among them is of the batch
/// too: its entry and the reads' one round leave with the same drain.
#[test]
fn reads_queued_together_leave_in_one_round_and_all_are_answered() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    for _ in 0..10 {
        fixture.pump();
    }
    let mut id = 1_000u128;
    for together in [1usize, 32, 100] {
        let (sender, receiver) = mpsc::sync_channel(together);
        let mut replies = Vec::new();
        let mut first = None;
        for _ in 0..together {
            id += 1;
            let (work, reply) = summary(&fixture.owners[0], id);
            replies.push(reply);
            if first.is_none() {
                first = Some(work);
            } else {
                sender.try_send(work).ok().unwrap();
            }
        }
        // As the owner's loop does: the work it was woken by and what is
        // queued behind it, taken as one batch whose drain sends the round.
        assert!(!fixture.owners[0].take(first.unwrap(), &receiver).unwrap());
        assert!(receiver.try_recv().is_err());
        assert_eq!(fixture.owners[0].session.reads_waiting(), together);
        let sent = rounds(&mut fixture);
        assert_eq!(
            sent.iter().map(|(to, _)| *to).collect::<Vec<_>>(),
            vec![2, 3],
            "{together} reads left in {} heartbeats",
            sent.len()
        );
        assert_eq!(sent[0].1, sent[1].1);
        for _ in 0..10 {
            fixture.pump();
        }
        assert_eq!(fixture.owners[0].session.reads_waiting(), 0);
        for mut reply in replies {
            assert!(matches!(
                reply.try_recv().unwrap().envelope().result,
                Response::Summary(_)
            ));
        }
    }
    // A write queued between two reads is taken with them: no drain for the
    // write alone, which sent the first read's round with its entry and left
    // the read behind it for a round of its own.
    let (sender, receiver) = mpsc::sync_channel(2);
    let (read, mut answer) = summary(&fixture.owners[0], 5_000);
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: identity().ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(5_002),
        operation: Operation::Submit {
            expected_revision: None,
            command: claim(77),
        },
    };
    let verified = verify_request(actor(), request, &fixture.owners[0].client_limits).unwrap();
    let charge = fixture.owners[0]
        .budget
        .reserve(BudgetKind::Pending, BudgetLane::Completion, 1024 * 1024)
        .unwrap()
        .commit();
    let (send, mut written) = oneshot::channel();
    sender
        .try_send(Work::Request(
            Box::new(AdmittedRequest {
                verified,
                witness: None,
                native: None,
            }),
            send,
            charge,
        ))
        .ok()
        .unwrap();
    let (late, mut late_answer) = summary(&fixture.owners[0], 5_001);
    sender.try_send(late).ok().unwrap();
    assert!(!fixture.owners[0].take(read, &receiver).unwrap());
    // All three were taken, and both reads went in the batch's one round.
    assert!(receiver.try_recv().is_err());
    assert_eq!(fixture.owners[0].session.reads_waiting(), 2);
    let sent = rounds(&mut fixture);
    assert_eq!(
        sent.iter().map(|(to, _)| *to).collect::<Vec<_>>(),
        vec![2, 3]
    );
    for _ in 0..15 {
        fixture.pump();
    }
    assert!(matches!(
        answer.try_recv().unwrap().envelope().result,
        Response::Summary(_)
    ));
    assert!(matches!(
        written.try_recv().unwrap().envelope().result,
        Response::Submitted(_)
    ));
    assert!(matches!(
        late_answer.try_recv().unwrap().envelope().result,
        Response::Summary(_)
    ));
}

/// The bytes a leader sends a peer ahead of its answers follow what the
/// path to it holds in flight and its round trip: twice the window; a page
/// at least where the path carries a page within a beat; never a page on a
/// path that would take many beats to carry it.
#[test]
fn the_bytes_sent_ahead_follow_what_the_path_carries() {
    let beat = Duration::from_millis(200);
    let page = 4 * 1024 * 1024 + 1024;
    // A path in one room, new: twelve kilobytes in flight, a round trip of
    // a fifth of a millisecond. It carries twelve megabytes a beat: a page.
    assert_eq!(inflight_bytes(12_000, 200_000, beat, page), page);
    // The same path once its window has grown past half a page.
    assert_eq!(inflight_bytes(8 << 20, 200_000, beat, page), 16 << 20);
    // A thin path: three kilobytes in flight, a round trip of six tenths
    // of a second. A page would take it many beats; twice its window.
    assert_eq!(inflight_bytes(3_000, 600_000_000, beat, page), 6_000);
    // A long fat path: its window is what it carries in a round trip.
    assert_eq!(inflight_bytes(12 << 20, 100_000_000, beat, page), 24 << 20);
    // Nothing measured, nothing in flight, and the largest of each: no
    // division by nothing and no overflow.
    assert_eq!(inflight_bytes(5_000, 0, beat, page), 10_000);
    assert_eq!(inflight_bytes(0, 1, beat, page), 0);
    assert_eq!(
        inflight_bytes(u64::MAX, 1, Duration::MAX, u64::MAX),
        u64::MAX
    );
}

/// The owner sets what it was told of its peers' paths, and a leader then
/// sends by it: a hundred kilobytes proposed for a peer whose path holds a
/// few kilobytes leave a few kilobytes at a time.
#[test]
fn an_owner_told_of_a_thin_path_sends_its_peer_by_it() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    for _ in 0..10 {
        fixture.pump();
    }
    let page = fixture.owners[0].session.page_bytes();
    let charge = || {
        MemoryBudget::new(1 << 20, 1 << 16)
            .unwrap()
            .reserve(BudgetKind::Control, BudgetLane::Completion, 1)
            .unwrap()
            .commit()
    };
    // Until it is told, a peer is sent a page ahead of its answers.
    assert_eq!(fixture.owners[0].session.inflight_bytes(2), Some((0, page)));
    assert!(
        !fixture.owners[0]
            .accept(Work::Windows(
                vec![(2, 3_000, 600_000_000), (9, 3_000, 600_000_000)],
                charge()
            ))
            .unwrap()
    );
    assert_eq!(
        fixture.owners[0].session.inflight_bytes(2),
        Some((0, 6_000))
    );
    assert_eq!(fixture.owners[0].session.inflight_bytes(3), Some((0, page)));
    assert_eq!(fixture.owners[0].session.inflight_bytes(9), None);
    // A fast path is given a page at least, and twice its window beyond.
    assert!(
        !fixture.owners[0]
            .accept(Work::Windows(vec![(3, 16 << 20, 200_000)], charge()))
            .unwrap()
    );
    assert_eq!(
        fixture.owners[0].session.inflight_bytes(3),
        Some((0, 32 << 20))
    );
}
