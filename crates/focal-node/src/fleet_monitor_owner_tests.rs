use super::*;

fn read(fixture: &mut Fixture, id: u128) -> oneshot::Receiver<OwnedResponse> {
    let owner = &mut fixture.owners[0];
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: identity().ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::Monitor {
            id: MonitorId::from_u128(400),
        },
    };
    let verified = verify_request(actor(), request, &owner.client_limits).unwrap();
    let bound = crate::monitor_reads::RESPONSE_BYTES;
    assert!(completion_request(&verified));
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Completion, bound * 32 + 4096)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    owner.request(verified, send, charge, None);
    receive
}

#[test]
fn monitor_waiters_release_allowance_on_cancel_deadline_route_change_and_delivery_drop() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let before = fixture.owners[0].session.sequence();
    fixture.owners[0].config.pending_clients = 1;
    let mut first = read(&mut fixture, 1);
    fixture.owners[0].drain().unwrap();
    assert!(matches!(
        first.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let mut full = read(&mut fixture, 2);
    assert!(matches!(
        full.try_recv().unwrap().envelope().result,
        Response::Error(AccessError::Capacity)
    ));
    let held = fixture.owners[0].budget.stats().used;
    drop(first);
    fixture.owners[0].expire_pending();
    assert!(fixture.owners[0].pending.is_empty());
    assert!(fixture.owners[0].budget.stats().used < held);
    let mut expired = read(&mut fixture, 3);
    fixture.owners[0].pending.front_mut().unwrap().deadline = Instant::now();
    fixture.owners[0].expire_pending();
    assert!(matches!(
        expired.try_recv().unwrap().envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    let mut routed = read(&mut fixture, 4);
    fixture.owners[0].config.route_epoch = RouteEpoch(2);
    for _ in 0..10 {
        fixture.pump();
    }
    assert!(matches!(
        routed.try_recv().unwrap().envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    fixture.owners[0].config.route_epoch = RouteEpoch(1);
    let mut delivered = read(&mut fixture, 5);
    for _ in 0..10 {
        fixture.pump();
    }
    let reply = delivered.try_recv().unwrap();
    assert!(matches!(reply.envelope().result, Response::Monitor(_)));
    let held = fixture.owners[0].budget.stats().used;
    drop(reply);
    assert!(fixture.owners[0].budget.stats().used < held);
    assert_eq!(fixture.owners[0].session.sequence(), before);
    let mut stopped = read(&mut fixture, 6);
    fixture.owners[0].close();
    assert!(matches!(
        stopped.try_recv().unwrap().envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    for owner in &mut fixture.owners {
        owner.close();
    }
}
