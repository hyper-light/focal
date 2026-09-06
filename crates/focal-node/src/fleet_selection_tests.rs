use super::*;
fn request(fixture: &mut Fixture, query: SelectionRequest) -> oneshot::Receiver<OwnedResponse> {
    let owner = &mut fixture.owners[0];
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: identity().ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(999),
        operation: Operation::Select(query),
    };
    let verified = verify_request(actor(), request, &owner.client_limits).unwrap();
    let charge = owner
        .budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, 4 * 1024 * 1024)
        .unwrap()
        .commit();
    let (send, receive) = oneshot::channel();
    owner.request(verified, send, charge, None);
    receive
}
#[test]
fn selection_quorum_prefix_residual_progress_and_changed_query_are_fenced() {
    let root = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(root.path());
    let mut query = SelectionRequest {
        query: list(),
        predicates: SelectionPredicates {
            created_after: Some(SessionSeq(0)),
            scopes: vec![Scope {
                kind: ScopeKind::File,
                key: "never-matches".into(),
            }],
            ..Default::default()
        },
    };
    let mut response = request(&mut fixture, query.clone());
    for _ in 0..5 {
        fixture.owners[0].drain().unwrap();
    }
    assert!(matches!(
        response.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    for _ in 0..10 {
        fixture.pump();
    }
    let page = listed(response.try_recv().unwrap());
    assert!(page.objects.is_empty());
    assert_eq!(page.visited, 1);
    assert!(page.next.is_some());
    let original = page.token;
    let input = demo::request(
        &identity(),
        "selection-later",
        identity().issuer,
        claim(4),
        vec![],
    );
    fixture.owners[0].session.propose(&input).unwrap();
    for _ in 0..15 {
        fixture.pump();
    }
    query.query.cursor = page.next;
    let mut changed = query.clone();
    changed.predicates.scopes[0].key = "other".into();
    assert!(matches!(
        request(&mut fixture, changed)
            .try_recv()
            .unwrap()
            .envelope()
            .result,
        Response::Error(AccessError::Unauthorized)
    ));
    let mut visited = 1;
    while query.query.cursor.is_some() {
        let page = listed(request(&mut fixture, query.clone()).try_recv().unwrap());
        assert_eq!(page.token, original);
        assert!(page.objects.is_empty());
        visited += page.visited;
        query.query.cursor = page.next;
        assert!(visited <= 3);
    }
    assert_eq!(visited, 3);
    for owner in &mut fixture.owners {
        owner.close();
    }
}
