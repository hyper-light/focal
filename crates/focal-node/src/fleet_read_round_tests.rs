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
        let Operation::Raft { message, .. } = &frame.request.operation else {
            panic!("Raft expected")
        };
        let decoded = focal_consensus::decode_message(message).unwrap();
        if decoded.msg_type == MessageType::MsgHeartbeat as i32 && !decoded.context.is_empty() {
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
/// heartbeats: the owner takes what is queued behind a read before the
/// drain that sends its round. A hundred reads leave in two heartbeats, one
/// to each follower, where a drain after each sent two hundred; a read
/// queued alone leaves in two as well, with the drain that follows it; and
/// every one of them is answered.
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
        // As the owner's loop does: the work it was woken by, and what is
        // queued behind it while a read waits for its round.
        assert!(!fixture.owners[0].take(first.unwrap(), &receiver).unwrap());
        assert!(receiver.try_recv().is_err());
        assert_eq!(fixture.owners[0].session.reads_waiting(), together);
        assert!(rounds(&mut fixture).is_empty());
        fixture.owners[0].drain().unwrap();
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
    // A write queued behind a read ends the taking: it is drained for as it
    // was, and its drain sends the round with its entry.
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
    // The write was taken and drained for; the read behind it was not taken.
    assert!(receiver.try_recv().is_ok());
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
    assert!(late_answer.try_recv().is_err());
}
