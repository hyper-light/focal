use super::*;

fn read(fixture: &mut Fixture, id: u128) -> hyper_rt::sync::OneshotReceiver<OwnedResponse> {
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
    let (send, receive) = hyper_rt::sync::oneshot().unwrap();
    owner.request(verified, send, charge, None, None);
    receive
}

#[test]
fn monitor_waiters_release_allowance_on_cancel_deadline_route_change_and_delivery_drop() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let before = fixture.owners[0].session.sequence();
    let unheld = fixture.owners[0].budget.stats().used;
    let mut first = read(&mut fixture, 1);
    fixture.owners[0].drain().unwrap();
    assert!(matches!(first.try_recv(), Ok(None)));
    // What bounds the waiters is the memory each holds while it waits, charged as it is
    // admitted: a second waits beside the first, and each gives its charge back as it goes.
    let mut second = read(&mut fixture, 2);
    fixture.owners[0].drain().unwrap();
    assert!(matches!(second.try_recv(), Ok(None)));
    assert_eq!(fixture.owners[0].pending.len(), 2);
    let held = fixture.owners[0].budget.stats().used;
    assert!(held > unheld);
    drop(first);
    drop(second);
    fixture.owners[0].expire_pending();
    assert!(fixture.owners[0].pending.is_empty());
    assert!(fixture.owners[0].budget.stats().used < held);
    let mut expired = read(&mut fixture, 3);
    fixture.owners[0].pending.front_mut().unwrap().deadline = 0;
    fixture.owners[0].expire_pending();
    assert!(matches!(
        expired.try_recv().unwrap().unwrap().envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    let mut routed = read(&mut fixture, 4);
    fixture.owners[0].config.route_epoch = RouteEpoch(2);
    for _ in 0..10 {
        fixture.pump();
    }
    assert!(matches!(
        routed.try_recv().unwrap().unwrap().envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    fixture.owners[0].config.route_epoch = RouteEpoch(1);
    let mut delivered = read(&mut fixture, 5);
    for _ in 0..10 {
        fixture.pump();
    }
    let reply = delivered.try_recv().unwrap().unwrap();
    assert!(matches!(reply.envelope().result, Response::Monitor(_)));
    let held = fixture.owners[0].budget.stats().used;
    drop(reply);
    assert!(fixture.owners[0].budget.stats().used < held);
    assert_eq!(fixture.owners[0].session.sequence(), before);
    let mut stopped = read(&mut fixture, 6);
    fixture.owners[0].close();
    assert!(matches!(
        stopped.try_recv().unwrap().unwrap().envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    for owner in &mut fixture.owners {
        owner.close();
    }
}
