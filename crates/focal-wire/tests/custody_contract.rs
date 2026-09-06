#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_model::*;
use focal_wire::*;
use std::collections::BTreeSet;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn content() -> ContentRef {
    ContentRef {
        domain: ContentDomainId(ledger().tenant.0),
        root: ContentHash([3; 32]),
        length: 4,
        class: ContentClass::Evidence,
    }
}
fn peer(role: PeerRole) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(9),
        tenants: BTreeSet::from([ledger().tenant]),
        role,
    })
    .unwrap()
}
fn request(operation: CustodyRequest) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(4),
        operation: Operation::Custody(operation),
    }
}
#[test]
fn custody_requires_node_authentication_and_exact_tenant_namespace() {
    let request = request(CustodyRequest::Verify {
        policy_revision: 1,
        content: content(),
    });
    let limits = WireLimits::default();
    for role in [PeerRole::Actor, PeerRole::Evaluator, PeerRole::Runtime] {
        assert_eq!(
            verify_request(peer(role), request.clone(), &limits).unwrap_err(),
            AccessError::Unauthorized
        );
    }
    verify_request(
        peer(PeerRole::Node { node_id: 7 }),
        request.clone(),
        &limits,
    )
    .unwrap();
    let mut foreign = request;
    if let Operation::Custody(CustodyRequest::Verify { content, .. }) = &mut foreign.operation {
        content.domain = ContentDomainId([5; 16]);
    }
    assert_eq!(
        verify_request(peer(PeerRole::Node { node_id: 7 }), foreign, &limits).unwrap_err(),
        AccessError::Unauthorized
    );
}
#[test]
fn replies_cannot_substitute_policy_content_or_chunk_and_output_is_bounded() {
    let limits = WireLimits::default();
    let probe = request(CustodyRequest::Verify {
        policy_revision: 8,
        content: content(),
    });
    let correct = probe.reply(Response::Custody(CustodyReply::Durable {
        policy_revision: 8,
        content: content(),
    }));
    validate_response(&probe, &correct, None, &limits).unwrap();
    let wrong = probe.reply(Response::Custody(CustodyReply::Durable {
        policy_revision: 7,
        content: content(),
    }));
    assert!(validate_response(&probe, &wrong, None, &limits).is_err());
    assert!(
        validate_response(&probe, &probe.reply(Response::PeerAccepted), None, &limits).is_err()
    );
    let chunk = request(CustodyRequest::ReadChunk {
        transfer: [1; 16],
        index: 2,
        max_bytes: 4,
    });
    let wrong = chunk.reply(Response::Custody(CustodyReply::Chunk {
        index: 3,
        bytes: vec![0; 4],
    }));
    assert!(validate_response(&chunk, &wrong, None, &limits).is_err());
    let too_large = chunk.reply(Response::Custody(CustodyReply::Chunk {
        index: 2,
        bytes: vec![0; 5],
    }));
    assert!(validate_response(&chunk, &too_large, None, &limits).is_err());
    let huge = request(CustodyRequest::Manifest {
        policy_revision: 1,
        content: content(),
        max_bytes: u32::MAX,
    });
    assert_eq!(
        verify_request(peer(PeerRole::Node { node_id: 7 }), huge, &limits).unwrap_err(),
        AccessError::Capacity
    );
}
