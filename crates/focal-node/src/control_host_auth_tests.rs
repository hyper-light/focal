use super::*;
use crate::{
    cluster::NoDirectoryAuthority,
    config::Settings,
    network_bootstrap::{FoundingNetwork, unix_time},
};
use focal_model::ParticipantId;
use std::collections::BTreeSet;

fn cached_peer(network: &FoundingNetwork) -> (PeerRegistry, AuthenticatedPeer) {
    let registry = PeerRegistry::new(1).unwrap();
    let identity = network.directory.identity();
    let fingerprint = registry
        .register_certificate(
            &network.receipt.certificate,
            PeerGrant {
                principal: identity.issuer,
                tenants: BTreeSet::from([network.state.genesis.root_namespace.tenant]),
                role: PeerRole::Node {
                    node_id: identity.node,
                },
            },
        )
        .unwrap();
    let peer = registry.authenticate(fingerprint).unwrap();
    (registry, peer)
}

fn request(
    peer: AuthenticatedPeer,
    namespace: LedgerId,
    id: u128,
    operation: Operation,
) -> VerifiedRequest {
    verify_request(
        peer,
        RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: namespace,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(id),
            operation,
        },
        &ControlHost::wire_limits(),
    )
    .unwrap()
}

fn read_operation(group: [u8; 16]) -> Operation {
    Operation::PeerControl {
        group,
        request: ControlRpc::Read(ControlRead::State)
            .encode(MAX_PEER_CONTROL_REQUEST_BYTES)
            .unwrap(),
    }
}

fn revocation(network: &FoundingNetwork) -> ControlRequest {
    ControlRequest {
        id: ControlRequestId {
            client: [81; 16],
            sequence: 1,
        },
        acknowledged_through: 0,
        command: ControlCommand::Enrollment(
            network
                .control
                .enrollment()
                .unwrap()
                .prepare_revoke(network.receipt.invitation, unix_time().unwrap())
                .unwrap(),
        ),
    }
}

fn assert_read_denied(response: ResponseEnvelope) {
    let Response::Control { response } = response.result else {
        panic!("expected a bounded control rejection");
    };
    assert_eq!(
        ControlReply::decode(
            &response,
            ControlHost::wire_limits().max_frame_bytes as usize
        )
        .unwrap(),
        ControlReply::Rejected(ControlFailure::Unauthorized)
    );
}

#[tokio::test]
async fn cached_enrolled_peer_cannot_dispatch_root_raft_or_read_after_committed_revocation() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().into());
    settings.node.advertise = Some("127.0.0.1:7443".into());
    let network = FoundingNetwork::open(&settings).await.unwrap();
    let namespace = network.state.genesis.root_namespace;
    let (registry, peer) = cached_peer(&network);
    let revoke = revocation(&network);
    let group = network.control.identity().group;
    let mut message = focal_consensus::Message::default();
    message.set_msg_type(focal_consensus::MessageType::MsgHeartbeatResponse);
    message.from = network.directory.identity().node;
    message.to = message.from;
    message.term = network.control.status().term;
    let raft_operation = Operation::Raft {
        group,
        message: message.write_to_bytes().unwrap(),
    };
    // Both requests pass transport verification before revocation. The registry
    // intentionally remains stale: only the owner sees the committed change.
    let stale_raft = request(peer.clone(), namespace, 1, raft_operation.clone());
    let stale_read = request(peer.clone(), namespace, 2, read_operation(group));
    let (host, owner, _outgoing) = ControlHost::spawn(
        network.control,
        NoDirectoryAuthority,
        ControlHostConfig::new(namespace),
        network.budget.clone(),
    )
    .unwrap();
    assert_eq!(
        host.handle(request(peer.clone(), namespace, 3, raft_operation))
            .await
            .result,
        Response::PeerAccepted
    );
    assert!(matches!(
        host.read(peer.clone(), RequestId::from_u128(4), ControlRead::State)
            .await
            .unwrap(),
        ControlReadResult::State(_)
    ));
    let operator = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId(revoke.id.client),
        tenants: BTreeSet::from([namespace.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap();
    let receipt = host.submit(operator, revoke).await.unwrap();
    assert!(receipt.committed_index > 0);
    assert!(
        registry
            .authenticate(peer.certificate_fingerprint().unwrap())
            .is_ok()
    );
    assert_eq!(
        host.handle(stale_raft).await.result,
        Response::Error(AccessError::Unauthorized)
    );
    assert_read_denied(host.handle(stale_read).await);
    host.stop().await.unwrap();
    owner.join().unwrap();
}

#[tokio::test]
async fn root_read_rechecks_enrollment_when_its_quorum_barrier_completes() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().into());
    settings.node.advertise = Some("127.0.0.1:7443".into());
    let network = FoundingNetwork::open(&settings).await.unwrap();
    let namespace = network.state.genesis.root_namespace;
    let (_registry, peer) = cached_peer(&network);
    let revoke = revocation(&network);
    let group = network.control.identity().group;
    let status = network.control.status();
    let (outbound, _outgoing) = async_mpsc::channel(32);
    let (progress, _progress) = watch::channel(ControlProgressState {
        value: ControlProgress {
            identity: network.control.identity(),
            node: status.node_id,
            leader: status.leader_id,
            term: status.term,
            applied_index: network.control.applied_index(),
            revisions: network.control.revisions(),
            dropped_replication: 0,
            stopped: false,
        },
        _allocation: None,
    });
    let mut owner = Owner {
        replica: network.control,
        initial: None,
        verifier: NoDirectoryAuthority,
        config: ControlHostConfig::new(namespace),
        limits: ControlHost::wire_limits(),
        budget: network.budget.clone(),
        pending: VecDeque::new(),
        directory: None,
        authority_refresh: None,
        outbound,
        progress,
        nonce: 0,
        dropped: 0,
    };
    let charge = owner
        .budget
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 8192)
        .unwrap()
        .commit();
    let (reply, mut receive) = oneshot::channel();
    owner.request(
        request(peer, namespace, 1, read_operation(group)),
        reply,
        charge,
    );
    assert_eq!(owner.pending.len(), 1);
    assert!(matches!(
        receive.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        owner
            .replica
            .submit(revoke.clone(), &NoDirectoryAuthority)
            .unwrap(),
        ControlSubmission::Pending(_)
    ));
    // Publish the revoke and the earlier read barrier in one owner turn. The
    // result must check the newly published enrollment before it leaves.
    owner.drain().unwrap();
    assert!(owner.replica.receipt(revoke.id).unwrap().is_some());
    assert!(owner.pending.is_empty());
    assert_read_denied(receive.try_recv().unwrap().response);
}
