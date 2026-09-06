use super::*;
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn request() -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(3),
        operation: Operation::Select(SelectionRequest {
            query: ListRequest {
                filter: ListFilter::new(ObjectKind::Claim),
                cursor: None,
                max_items: 1,
                max_visits: 1,
            },
            predicates: SelectionPredicates {
                created_after: Some(SessionSeq(0)),
                ..Default::default()
            },
        }),
    }
}
fn peer(role: PeerRole) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(4),
        tenants: [ledger().tenant].into_iter().collect(),
        role,
    })
    .unwrap()
}
#[test]
fn selection_ingress_rejects_excessive_and_cross_scope_predicates_before_owner_copy() {
    let limits = WireLimits::default();
    verify_request(peer(PeerRole::Actor), request(), &limits).unwrap();
    assert!(matches!(
        verify_request(peer(PeerRole::Node { node_id: 4 }), request(), &limits),
        Err(AccessError::Unauthorized)
    ));
    let mut oversized = request();
    let Operation::Select(query) = &mut oversized.operation else {
        panic!("select")
    };
    query.predicates.scopes = vec![
        Scope {
            kind: ScopeKind::File,
            key: "a".into()
        };
        17
    ];
    assert!(matches!(
        verify_request(peer(PeerRole::Actor), oversized, &limits),
        Err(AccessError::Capacity)
    ));
    let mut oversized = request();
    let Operation::Select(query) = &mut oversized.operation else {
        panic!("select")
    };
    query.predicates.scopes = vec![Scope {
        kind: ScopeKind::File,
        key: "a".repeat(16 * 1024 + 1),
    }];
    assert!(matches!(
        verify_request(peer(PeerRole::Actor), oversized, &limits),
        Err(AccessError::Capacity)
    ));
    let mut scoped = request();
    let Operation::Select(query) = &mut scoped.operation else {
        panic!("select")
    };
    query.predicates.relations = vec![Relation {
        kind: RelationKind::DependsOn,
        target: RelationTarget::Object(ObjectRef {
            ledger: LedgerId {
                session: SessionId::from_u128(99),
                ..ledger()
            },
            kind: ObjectKind::Claim,
            id: ObjectId::from_u128(5),
        }),
    }];
    assert!(matches!(
        verify_request(peer(PeerRole::Actor), scoped, &limits),
        Err(AccessError::InvalidRequest)
    ));
}
#[test]
fn scope_binds_every_added_predicate_page_limit_and_authenticated_principal() {
    let Operation::Select(query) = request().operation else {
        panic!("select")
    };
    let original = selection_scope(&peer(PeerRole::Actor), ledger(), &query).unwrap();
    let mut variants = Vec::new();
    let mut different = query.clone();
    different.query.max_items = 2;
    variants.push(different);
    let mut different = query.clone();
    different.query.max_visits = 2;
    variants.push(different);
    let mut different = query.clone();
    different.predicates.created_through = Some(SessionSeq(20));
    variants.push(different);
    let mut different = query.clone();
    different.predicates.scopes.push(Scope {
        kind: ScopeKind::File,
        key: "a".into(),
    });
    variants.push(different);
    let mut different = query.clone();
    different.predicates.relations.push(Relation {
        kind: RelationKind::Issuer,
        target: RelationTarget::Participant(ParticipantId::from_u128(4)),
    });
    variants.push(different);
    for changed in variants {
        assert_ne!(
            selection_scope(&peer(PeerRole::Actor), ledger(), &changed).unwrap(),
            original
        );
    }
    let other = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(5),
        tenants: [ledger().tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap();
    assert_ne!(selection_scope(&other, ledger(), &query).unwrap(), original);
}
