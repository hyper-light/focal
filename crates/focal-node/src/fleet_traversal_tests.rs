use super::*;

fn query() -> TraversalRequest {
    TraversalRequest {
        roots: (1..=3)
            .map(|id| ObjectRef {
                ledger: identity().ledger,
                kind: ObjectKind::Claim,
                id: ObjectId::from_u128(id),
            })
            .collect(),
        direction: TraversalDirection::Forward,
        edges: vec![TraversalEdge::Requirement],
        max_depth: 1,
        max_nodes: 32,
        max_edges: 128,
        max_items: 1,
        max_visits: 1,
        max_bytes: 64 * 1024,
        cursor: None,
    }
}
fn request(
    fixture: &mut Fixture,
    peer: AuthenticatedPeer,
    query: TraversalRequest,
) -> oneshot::Receiver<OwnedResponse> {
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: identity().ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(987),
        operation: Operation::Traverse(query),
    };
    let owner = &mut fixture.owners[0];
    let verified = verify_request(peer, request, &owner.client_limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, 4 * 1024 * 1024)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    owner.request(verified, send, charge, None, None);
    receive
}
fn page(reply: OwnedResponse) -> TraversalPage {
    match reply.into_envelope().result {
        Response::Traversed(page) => page,
        other => panic!("{other:?}"),
    }
}

#[test]
fn traversal_requires_quorum_and_preserves_exact_pages_across_later_writes() {
    let directory = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(directory.path());
    let mut first = request(&mut fixture, actor(), query());
    for _ in 0..5 {
        fixture.owners[0].drain().unwrap();
    }
    assert!(matches!(
        first.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    for _ in 0..10 {
        fixture.pump();
    }
    let first = page(first.try_recv().unwrap());
    let prefix = first.token;
    assert_eq!(first.objects.len(), 1);
    assert_eq!(first.stop, TraversalStop::PageLimit);
    let input = demo::request(
        &identity(),
        "traversal-later-write",
        identity().issuer,
        claim(4),
        vec![],
    );
    fixture.owners[0].session.propose(&input).unwrap();
    for _ in 0..15 {
        fixture.pump();
    }
    let mut query = query();
    query.cursor = first.next;
    let mut found = first.objects;
    let mut zero_page = false;
    for _ in 0..32 {
        let result = page(
            request(&mut fixture, actor(), query.clone())
                .try_recv()
                .unwrap(),
        );
        let repeated = page(
            request(&mut fixture, actor(), query.clone())
                .try_recv()
                .unwrap(),
        );
        assert_eq!(
            result, repeated,
            "retry preserves objects, visit accounting and child cursor"
        );
        assert_eq!(result.token, prefix);
        zero_page |= result.objects.is_empty();
        found.extend(result.objects);
        query.cursor = result.next;
        if query.cursor.is_none() {
            assert_eq!(result.stop, TraversalStop::Complete);
            break;
        }
    }
    assert!(query.cursor.is_none());
    assert!(
        zero_page,
        "edge work may advance without emitting an object"
    );
    assert_eq!(found.len(), 6);
    let ids: std::collections::BTreeSet<_> = found
        .iter()
        .map(|object| match object {
            ReadObject::Claim { id, .. } => ObjectId(id.0),
            ReadObject::Validation { id, .. } => ObjectId(id.0),
            _ => panic!("unexpected object"),
        })
        .collect();
    assert_eq!(ids.len(), 6);
    assert!(!ids.contains(&ObjectId::from_u128(4)));
}

#[test]
fn traversal_cursor_rejects_tamper_query_principal_route_and_expiry() {
    let directory = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(directory.path());
    let mut received = request(&mut fixture, actor(), query());
    for _ in 0..10 {
        fixture.pump();
    }
    let first = page(received.try_recv().unwrap());
    let mut exact = query();
    exact.cursor = first.next;
    let mut changed = exact.clone();
    changed.edges = vec![TraversalEdge::Evidence];
    assert!(matches!(
        request(&mut fixture, actor(), changed)
            .try_recv()
            .unwrap()
            .into_envelope()
            .result,
        Response::Error(AccessError::Unauthorized)
    ));
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: identity().worker,
        tenants: [identity().ledger.tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap();
    assert!(matches!(
        request(&mut fixture, peer, exact.clone())
            .try_recv()
            .unwrap()
            .into_envelope()
            .result,
        Response::Error(AccessError::Unauthorized)
    ));
    let mut changed = exact.clone();
    changed.cursor.as_mut().unwrap().bytes[2] ^= 1;
    assert!(matches!(
        request(&mut fixture, actor(), changed)
            .try_recv()
            .unwrap()
            .into_envelope()
            .result,
        Response::Error(AccessError::InvalidRequest)
    ));
    let retained = fixture.owners[0].session.memory_stats().used;
    fixture.owners[0].views.expire_test_views();
    assert!(matches!(
        request(&mut fixture, actor(), exact)
            .try_recv()
            .unwrap()
            .into_envelope()
            .result,
        Response::Error(AccessError::SnapshotExpired)
    ));
    assert!(fixture.owners[0].views.traversal_count() == 0);
    assert!(fixture.owners[0].session.memory_stats().used < retained);
}
