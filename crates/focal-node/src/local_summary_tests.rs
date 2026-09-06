use super::*;
use crate::{config::Settings, reads::ReadViews, streams::Streams};

fn read(node: &mut EmbeddedNode, id: u128, operation: Operation) -> ResponseEnvelope {
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: node.identity.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation,
    };
    let principal = AuthenticatedPeer::local(PeerGrant {
        principal: node.identity.issuer,
        tenants: [node.identity.ledger.tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap();
    let limits = WireLimits::default();
    let verified = verify_request(principal, request.clone(), &limits).unwrap();
    let reply = super::dispatch(
        node,
        &mut ReadViews::new(),
        &mut Streams::new().unwrap(),
        verified,
        &limits,
    );
    validate_response(&request, &reply, None, &limits).unwrap();
    reply
}
#[test]
fn local_summary_reads_actual_committed_scalar_counts_without_mutation_and_replays() {
    let root = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().to_owned());
    let mut node = EmbeddedNode::open(&settings).unwrap();
    assert!(matches!(
        read(
            &mut node,
            1,
            Operation::OpenEpoch {
                epoch: RequestEpoch(1)
            }
        )
        .result,
        Response::Submitted(MutationReply::Committed(_))
    ));
    let claim = crate::demo::claim(&node.identity, ClaimId::from_u128(91)).unwrap();
    let requirements = claim.validations.len() as u64;
    assert!(matches!(
        read(
            &mut node,
            2,
            Operation::Submit {
                expected_revision: None,
                command: Command::GenerateClaim { claim }
            }
        )
        .result,
        Response::Submitted(MutationReply::Committed(_))
    ));
    let before = node.session.status().applied_index;
    let sequence = node.session.sequence();
    let Response::Summary(found) = read(&mut node, 3, Operation::Summary).result else {
        panic!("summary")
    };
    assert_eq!(found.claims, 1);
    assert_eq!(found.validations, requirements);
    assert_eq!(found.artifacts, 0);
    assert_eq!(found.token.sequence, sequence);
    assert_eq!(found.applied_index, before);
    assert_eq!(
        read(&mut node, 3, Operation::Summary).result,
        Response::Summary(found)
    );
    assert_eq!(node.session.status().applied_index, before);
    assert_eq!(node.session.sequence(), sequence);
    assert!(
        node.session
            .receipt(&RequestKey {
                principal: node.identity.issuer,
                epoch: RequestEpoch(1),
                id: RequestId::from_u128(3)
            })
            .is_none()
    );
    drop(node);
    let mut node = EmbeddedNode::open(&settings).unwrap();
    let Response::Summary(recovered) = read(&mut node, 3, Operation::Summary).result else {
        panic!("summary")
    };
    assert_eq!(recovered.token, found.token);
    assert_eq!(recovered.claims, 1);
    assert_eq!(recovered.validations, requirements);
    assert!(recovered.applied_index >= before);
}
