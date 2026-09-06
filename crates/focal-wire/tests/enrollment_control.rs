#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_model::*;
use focal_wire::*;
use std::collections::BTreeSet;

#[test]
fn enrollment_control_appends_protocol_tag_and_requires_bounded_certificate_bound_node_ingress() {
    let ledger = LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    };
    let mut packet = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(3),
        operation: Operation::EnrollmentControl {
            group: [4; 16],
            genesis: [5; 32],
            request: vec![1, 0],
        },
    };
    assert_eq!(packet.operation.registered_tag(), 13);
    assert_eq!(postcard::to_allocvec(&packet.operation).unwrap()[0], 12);
    let limits = WireLimits {
        max_frame_bytes: 1024 * 1024,
        max_cost: 4 * 1024 * 1024,
        ..WireLimits::default()
    };
    let grant = |role| PeerGrant {
        principal: ParticipantId::from_u128(6),
        tenants: BTreeSet::from([ledger.tenant]),
        role,
    };
    for role in [
        PeerRole::Actor,
        PeerRole::Runtime,
        PeerRole::Evaluator,
        PeerRole::Node { node_id: 1 },
    ] {
        assert!(matches!(
            verify_request(
                AuthenticatedPeer::local(grant(role)).unwrap(),
                packet.clone(),
                &limits
            ),
            Err(AccessError::Unauthorized)
        ));
    }
    // PeerRegistry is an injected post-TLS seam. Real certificate possession and
    // founder authorization are exercised by focal-node's three-replica QUIC test.
    let peers = PeerRegistry::new(1).unwrap();
    let fingerprint = peers
        .register_certificate(
            b"tls-verified-fixture",
            grant(PeerRole::Node { node_id: 1 }),
        )
        .unwrap();
    let peer = peers.authenticate(fingerprint).unwrap();
    assert!(verify_request(peer.clone(), packet.clone(), &limits).is_ok());
    validate_response(
        &packet,
        &packet.reply(Response::Control { response: vec![1] }),
        None,
        &limits,
    )
    .unwrap();
    packet.operation = Operation::EnrollmentControl {
        group: [4; 16],
        genesis: [5; 32],
        request: vec![1; MAX_ENROLLMENT_CONTROL_REQUEST_BYTES + 1],
    };
    assert!(matches!(
        verify_request(peer.clone(), packet.clone(), &limits),
        Err(AccessError::Capacity)
    ));
    packet.operation = Operation::Control {
        group: [4; 16],
        request: vec![1, 0],
    };
    assert!(matches!(
        verify_request(peer, packet, &limits),
        Err(AccessError::Unauthorized)
    ));
}
