use super::*;

#[test]
fn monitor_read_appends_ordinals_and_binds_identity_prefix_and_bounded_actual_facts() {
    let ledger = LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    };
    let id = MonitorId::from_u128(3);
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(4),
        operation: Operation::Monitor { id },
    };
    assert_eq!(request.operation.registered_tag(), 23);
    assert_eq!(postcard::to_stdvec(&request.operation).unwrap()[0], 22);
    assert!(!request.operation.is_mutation());
    let page = MonitorPage {
        token: ReadToken {
            ledger,
            route_epoch: RouteEpoch(1),
            sequence: SessionSeq(7),
        },
        applied_index: 10,
        id,
        monitor: Some(Monitor {
            id,
            owner: ClaimId::from_u128(5),
            roots: [WaitPredicate::Terminal(ClaimId::from_u128(6))]
                .into_iter()
                .collect(),
            deadline: Deadline {
                timer: TimerId::from_u128(8),
                generation: 0,
                at: 100,
            },
            registered: SessionSeq(3),
            released: Some(SessionSeq(7)),
        }),
    };
    let limits = WireLimits::default();
    let good = request.reply(Response::Monitor(page.clone()));
    assert_eq!(postcard::to_stdvec(&good.result).unwrap()[0], 19);
    validate_response(&request, &good, None, &limits).unwrap();
    let mut variants = vec![];
    let mut wrong = page.clone();
    wrong.id = MonitorId::from_u128(99);
    variants.push(wrong);
    let mut wrong = page.clone();
    wrong.token.sequence = SessionSeq(6);
    variants.push(wrong);
    let mut wrong = page.clone();
    wrong.token.route_epoch = RouteEpoch(2);
    variants.push(wrong);
    let mut wrong = page.clone();
    wrong.token.ledger.session = SessionId::from_u128(99);
    variants.push(wrong);
    let mut wrong = page.clone();
    wrong.monitor.as_mut().unwrap().released = Some(SessionSeq(2));
    variants.push(wrong);
    let mut wrong = page.clone();
    wrong.monitor.as_mut().unwrap().roots = (1..=257)
        .map(|i| WaitPredicate::Satisfied(ClaimId::from_u128(i)))
        .collect();
    variants.push(wrong);
    for wrong in variants {
        assert!(
            validate_response(
                &request,
                &request.reply(Response::Monitor(wrong)),
                None,
                &limits
            )
            .is_err()
        );
    }
    let mut absent = page;
    absent.monitor = None;
    validate_response(
        &request,
        &request.reply(Response::Monitor(absent)),
        None,
        &limits,
    )
    .unwrap();
    for (role, authorized) in [
        (PeerRole::Actor, true),
        (PeerRole::Runtime, true),
        (PeerRole::Node { node_id: 1 }, false),
    ] {
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal: ParticipantId::from_u128(9),
            tenants: [ledger.tenant].into_iter().collect(),
            role,
        })
        .unwrap();
        assert_eq!(
            verify_request(peer, request.clone(), &limits).is_ok(),
            authorized
        );
    }
}
