use super::*;

fn read(fixture: &mut Fixture, id: u128) -> oneshot::Receiver<OwnedResponse> {
    let owner = &mut fixture.owners[0];
    let verified = verify_request(
        actor(),
        RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: identity().ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(id),
            operation: Operation::Reconcile(ReconcileQuery::Epoch {
                epoch: RequestEpoch(1),
            }),
        },
        &owner.client_limits,
    )
    .unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, 4 * 1024 * 1024)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    owner.request(verified, send, charge, None, None);
    receive
}

#[test]
fn reconciliation_waiters_are_bounded_cancelable_and_fenced_by_route_and_term() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let before = fixture.owners[0].session.sequence();
    fixture.owners[0].config.pending_clients = 1;
    let mut first = read(&mut fixture, 1);
    fixture.owners[0].drain().unwrap();
    assert!(
        matches!(first.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "do not release a locally known epoch before this read's quorum"
    );
    let mut full = read(&mut fixture, 2);
    assert!(matches!(
        full.try_recv().unwrap().envelope().result,
        Response::Error(AccessError::Capacity)
    ));
    assert_eq!(fixture.owners[0].pending.len(), 1);
    let held = fixture.owners[0].budget.stats().used;
    drop(first);
    fixture.owners[0].expire_pending();
    assert!(fixture.owners[0].pending.is_empty());
    assert!(fixture.owners[0].budget.stats().used < held);
    // Reused outer request IDs still obtain distinct, fresh contexts; the old
    // canceled Raft read interest cannot complete the newly admitted waiter.
    let mut route_changed = read(&mut fixture, 1);
    fixture.owners[0].config.route_epoch = RouteEpoch(2);
    for _ in 0..10 {
        fixture.pump();
    }
    assert!(matches!(
        route_changed.try_recv().unwrap().envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    fixture.owners[0].config.route_epoch = RouteEpoch(1);
    let mut deadline = read(&mut fixture, 3);
    fixture.owners[0].pending.front_mut().unwrap().deadline = Instant::now();
    fixture.owners[0].expire_pending();
    assert!(matches!(
        deadline.try_recv().unwrap().envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    let mut term_changed = read(&mut fixture, 4);
    let original_term = fixture.owners[0].session.status().term;
    fixture.owners[1].session.campaign().unwrap();
    let mut held = Vec::new();
    for _ in 0..80 {
        // Expire the prior leader's election lease on this isolated majority.
        for owner in &mut fixture.owners[1..] {
            owner.session.tick().unwrap();
        }
        for owner in &mut fixture.owners[1..] {
            owner.drain().unwrap();
        }
        let mut frames = Vec::new();
        for (index, outgoing) in fixture.outgoing.iter_mut().enumerate().skip(1) {
            while let Ok(frame) = outgoing.try_recv() {
                frames.push((index as u64 + 1, frame));
            }
        }
        for (sender, frame) in frames {
            if frame.target == 1 {
                held.push((sender, frame));
                continue;
            }
            let Operation::Raft { message, .. } = frame.request.operation else {
                panic!("Raft")
            };
            fixture.owners[frame.target as usize - 1]
                .session
                .step_authenticated(sender, &message)
                .unwrap();
        }
    }
    assert!(
        fixture.owners[1..]
            .iter()
            .any(|owner| owner.session.is_authoritative())
    );
    for (sender, frame) in held {
        let Operation::Raft { message, .. } = frame.request.operation else {
            panic!("Raft")
        };
        fixture.owners[0]
            .session
            .step_authenticated(sender, &message)
            .unwrap();
    }
    assert!(fixture.owners[0].session.status().term > original_term);
    for _ in 0..20 {
        fixture.pump();
    }
    assert!(matches!(
        term_changed.try_recv().unwrap().envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    assert_eq!(fixture.owners[0].session.sequence(), before);
    for owner in &mut fixture.owners {
        owner.close();
    }
}
