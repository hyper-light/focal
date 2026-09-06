use super::*;
fn request() -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(3),
        operation: Operation::Read(ReadRequest {
            query: ReadQuery::Scan { after: None },
            consistency: ReadConsistency::Linearizable,
            max_items: 64,
        }),
    }
}
fn actor(request: &RequestEnvelope) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(4),
        tenants: [request.ledger.tenant].into(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
#[test]
fn offline_shape_matches_authenticated_bounds_without_granting_authority() {
    let limits = WireLimits::default();
    let request = request();
    assert!(check_request_shape(&request, &limits).is_ok());
    assert!(verify_request(actor(&request), request.clone(), &limits).is_ok());
    let mut invalid = request.clone();
    if let Operation::Read(read) = &mut invalid.operation {
        read.max_items = limits.max_items + 1;
    }
    assert_eq!(
        check_request_shape(&invalid, &limits),
        Err(AccessError::Capacity)
    );
    assert!(matches!(
        verify_request(actor(&invalid), invalid, &limits),
        Err(AccessError::Capacity)
    ));
    let mut other = actor(&request);
    other.grant.tenants = [TenantId::from_u128(999)].into();
    assert!(matches!(
        verify_request(other, request.clone(), &limits),
        Err(AccessError::Unauthorized)
    ));
    let mut unsupported = request.clone();
    unsupported.operation = Operation::Control {
        group: [1; 16],
        request: vec![1],
    };
    assert_eq!(
        check_request_shape(&unsupported, &limits),
        Err(AccessError::UnsupportedOperation)
    );
    assert!(matches!(
        verify_request(actor(&unsupported), unsupported, &limits),
        Err(AccessError::Unauthorized)
    ));
    let mut invalid = request;
    invalid.protocol = 99;
    assert_eq!(
        check_request_shape(&invalid, &limits),
        Err(AccessError::UnsupportedProtocol)
    );
}
