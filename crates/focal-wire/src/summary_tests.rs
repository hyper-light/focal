use super::*;
use crate::*;
use focal_model::*;

#[test]
fn summary_is_appended_read_only_and_binds_the_observed_ledger_and_route() {
    let ledger = LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    };
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(3),
        operation: Operation::Summary,
    };
    assert!(!request.operation.is_mutation());
    assert_eq!(request.operation.registered_tag(), 22);
    assert_eq!(postcard::to_stdvec(&request.operation).unwrap(), [21]);
    let value = LedgerSummary {
        token: ReadToken {
            ledger,
            sequence: SessionSeq(0),
            route_epoch: RouteEpoch(1),
        },
        applied_index: 1,
        claims: 0,
        testaments: 0,
        artifacts: 0,
        validations: 0,
        evidence_sets: 0,
        validation_runs: 0,
    };
    let response = request.reply(Response::Summary(value));
    assert_eq!(postcard::to_stdvec(&response.result).unwrap()[0], 18);
    let limits = WireLimits::default();
    validate_response(&request, &response, None, &limits).unwrap();
    for role in [PeerRole::Actor, PeerRole::Runtime] {
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal: ParticipantId::from_u128(4),
            tenants: [ledger.tenant].into_iter().collect(),
            role,
        })
        .unwrap();
        verify_request(peer, request.clone(), &limits).unwrap();
    }
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(4),
        tenants: [ledger.tenant].into_iter().collect(),
        role: PeerRole::Node { node_id: 4 },
    })
    .unwrap();
    assert!(matches!(
        verify_request(peer, request.clone(), &limits),
        Err(AccessError::Unauthorized)
    ));
    for changed in [
        LedgerSummary {
            token: ReadToken {
                ledger: LedgerId {
                    session: SessionId::from_u128(99),
                    ..ledger
                },
                ..value.token
            },
            ..value
        },
        LedgerSummary {
            token: ReadToken {
                route_epoch: RouteEpoch(2),
                ..value.token
            },
            ..value
        },
        LedgerSummary {
            applied_index: 0,
            ..value
        },
    ] {
        assert!(matches!(
            validate_response(
                &request,
                &request.reply(Response::Summary(changed)),
                None,
                &limits
            ),
            Err(WireError::InvalidFrame)
        ));
    }
    let different = RequestEnvelope {
        operation: Operation::Reconcile(ReconcileQuery::Epoch {
            epoch: RequestEpoch(1),
        }),
        ..request.clone()
    };
    assert!(validate_response(&different, &response, None, &limits).is_err());
    let wrong_route = ResponseEnvelope {
        route_epoch: RouteEpoch(2),
        ..response
    };
    assert!(validate_response(&request, &wrong_route, None, &limits).is_err());
}
