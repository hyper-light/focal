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
fn request(list: ListRequest) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(4),
        operation: Operation::List(list),
    }
}
fn list() -> ListRequest {
    ListRequest {
        filter: ListFilter::new(ObjectKind::Claim),
        cursor: None,
        max_items: 10,
        max_visits: 20,
    }
}
#[test]
fn list_tags_append_without_changing_legacy_read_encoding() {
    let read = Operation::Read(ReadRequest {
        consistency: ReadConsistency::Linearizable,
        query: ReadQuery::Objects(vec![]),
        max_items: 1,
    });
    assert_eq!(postcard::to_stdvec(&read).unwrap(), vec![1, 0, 0, 0, 1]);
    let operation = Operation::List(list());
    assert_eq!(operation.registered_tag(), 14);
    assert_eq!(postcard::to_stdvec(&operation).unwrap()[0], 13);
    assert!(!operation.is_mutation());
    let page = ListPage {
        token: ReadToken {
            ledger: ledger(),
            sequence: SessionSeq(0),
            route_epoch: RouteEpoch(1),
        },
        objects: vec![],
        next: None,
        visited: 0,
    };
    assert_eq!(postcard::to_stdvec(&Response::Listed(page)).unwrap()[0], 10);
}
#[test]
fn list_admission_enforces_family_bounds_and_tenant_actor_authority() {
    let limits = WireLimits::default();
    for kind in [
        ObjectKind::Claim,
        ObjectKind::Testament,
        ObjectKind::Artifact,
        ObjectKind::Validation,
    ] {
        let mut query = list();
        query.filter.kind = kind;
        verify_request(peer(PeerRole::Actor), request(query), &limits).unwrap();
    }
    assert!(matches!(
        verify_request(
            peer(PeerRole::Node { node_id: 1 }),
            request(list()),
            &limits
        ),
        Err(AccessError::Unauthorized)
    ));
    let mut wrong_tenant = request(list());
    wrong_tenant.ledger.tenant = TenantId::from_u128(9);
    assert!(matches!(
        verify_request(peer(PeerRole::Actor), wrong_tenant, &limits),
        Err(AccessError::Unauthorized)
    ));
    for invalid in 0..6 {
        let mut query = list();
        match invalid {
            0 => query.filter.claim = Some(ClaimId::default()),
            1 => query.filter.producer = Some(ParticipantId::from_u128(3)),
            2 => query.filter.phase = Some(ValidationPhase::WholeWork),
            3 => query.filter.testament = Some(TestamentId::from_u128(5)),
            4 => query.cursor = Some(ListCursor { bytes: vec![] }),
            _ => {
                query.cursor = Some(ListCursor {
                    bytes: vec![0; MAX_LIST_CURSOR_BYTES + 1],
                })
            }
        }
        assert!(verify_request(peer(PeerRole::Actor), request(query), &limits).is_err());
    }
    for (items, visits) in [
        (0, 1),
        (1, 0),
        (limits.max_items + 1, 1),
        (1, limits.max_items + 1),
    ] {
        let query = ListRequest {
            max_items: items,
            max_visits: visits,
            ..list()
        };
        assert!(matches!(
            verify_request(peer(PeerRole::Actor), request(query), &limits),
            Err(AccessError::Capacity)
        ));
    }
    let mut artifact = ListFilter::new(ObjectKind::Artifact);
    artifact.artifact_kind = Some("x".repeat(257));
    assert!(artifact.validate().is_err());
    artifact.artifact_kind = Some(String::new());
    assert!(artifact.validate().is_err());
    artifact.artifact_kind = Some("test-report".into());
    assert!(artifact.validate().is_ok());
}
#[test]
fn list_response_allows_empty_continuation_only_after_bounded_progress() {
    let request = request(list());
    let page = ListPage {
        token: ReadToken {
            ledger: ledger(),
            sequence: SessionSeq(9),
            route_epoch: RouteEpoch(1),
        },
        objects: vec![],
        next: Some(ListCursor { bytes: vec![1] }),
        visited: 1,
    };
    let mut response = request.reply(Response::Listed(page.clone()));
    validate_response(&request, &response, None, &WireLimits::default()).unwrap();
    for changed in 0..4 {
        let mut bad = page.clone();
        match changed {
            0 => bad.visited = 0,
            1 => bad.visited = 21,
            2 => bad.token.ledger.session = SessionId::from_u128(8),
            _ => {
                bad.next = Some(ListCursor {
                    bytes: vec![0; MAX_LIST_CURSOR_BYTES + 1],
                })
            }
        }
        response.result = Response::Listed(bad);
        assert!(validate_response(&request, &response, None, &WireLimits::default()).is_err());
    }
    response.result = Response::Listed(page);
    let mut unrelated = request.clone();
    unrelated.operation = Operation::OpenEpoch {
        epoch: RequestEpoch(1),
    };
    assert!(validate_response(&unrelated, &response, None, &WireLimits::default()).is_err());
}
