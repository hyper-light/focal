use super::*;
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn peer(role: PeerRole) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(3),
        tenants: [ledger().tenant].into_iter().collect(),
        role,
    })
    .unwrap()
}
fn query() -> TraversalRequest {
    TraversalRequest {
        roots: vec![ObjectRef {
            ledger: ledger(),
            kind: ObjectKind::Claim,
            id: ObjectId::from_u128(5),
        }],
        direction: TraversalDirection::Forward,
        edges: vec![],
        max_depth: 2,
        max_nodes: 32,
        max_edges: 128,
        max_items: 8,
        max_visits: 16,
        max_bytes: 64 * 1024,
        cursor: None,
    }
}
fn request(query: TraversalRequest) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(4),
        operation: Operation::Traverse(query),
    }
}
#[test]
fn traversal_is_append_only_actor_read_with_bounded_scoped_input() {
    let limits = WireLimits::default();
    let request = request(query());
    assert_eq!(request.operation.registered_tag(), 20);
    assert_eq!(postcard::to_stdvec(&request.operation).unwrap()[0], 19);
    assert!(!request.operation.is_mutation());
    verify_request(peer(PeerRole::Actor), request.clone(), &limits).unwrap();
    assert!(matches!(
        verify_request(
            peer(PeerRole::Node { node_id: 1 }),
            request.clone(),
            &limits
        ),
        Err(AccessError::Unauthorized)
    ));
    for mutate in [
        |q: &mut TraversalRequest| q.max_items = 0,
        |q: &mut TraversalRequest| q.max_nodes = u32::MAX,
        |q: &mut TraversalRequest| q.max_visits = u32::MAX,
        |q: &mut TraversalRequest| q.max_bytes = u32::MAX,
        |q: &mut TraversalRequest| q.roots[0].ledger.session = SessionId::from_u128(99),
        |q: &mut TraversalRequest| q.roots.push(q.roots[0]),
    ] {
        let mut query = query();
        mutate(&mut query);
        assert!(
            verify_request(peer(PeerRole::Actor), super::tests::request(query), &limits).is_err()
        );
    }
}
#[test]
fn traversal_response_binds_ledger_page_bounds_and_explicit_truncation() {
    let request = request(query());
    let limits = WireLimits::default();
    let page = TraversalPage {
        token: ReadToken {
            ledger: ledger(),
            sequence: SessionSeq(1),
            route_epoch: RouteEpoch(1),
        },
        objects: vec![],
        next: Some(TraversalCursor { bytes: vec![1] }),
        stop: TraversalStop::PageLimit,
        visited: 1,
        total_visits: 1,
    };
    assert_eq!(
        postcard::to_stdvec(&Response::Traversed(page.clone())).unwrap()[0],
        16
    );
    validate_response(
        &request,
        &request.reply(Response::Traversed(page.clone())),
        None,
        &limits,
    )
    .unwrap();
    for mutate in [
        |p: &mut TraversalPage| p.next = None,
        |p: &mut TraversalPage| p.stop = TraversalStop::Complete,
        |p: &mut TraversalPage| p.visited = 17,
        |p: &mut TraversalPage| p.total_visits = 129,
        |p: &mut TraversalPage| p.token.ledger.session = SessionId::from_u128(9),
    ] {
        let mut bad = page.clone();
        mutate(&mut bad);
        assert!(
            validate_response(
                &request,
                &request.reply(Response::Traversed(bad)),
                None,
                &limits
            )
            .is_err()
        );
    }
}
