use super::*;
use crate::{
    cluster::NoDirectoryAuthority,
    config::Settings,
    network_bootstrap::{FoundingNetwork, unix_time},
};
use focal_control::*;
use focal_directory::*;
use focal_ledger::{Session, SessionLimits, SessionPlacementRequest};
use focal_model::*;
use std::collections::{BTreeMap, BTreeSet};

fn commit(owner: &mut ControlReplica, sequence: u64, command: ControlCommand) {
    let id = ControlRequestId {
        client: [71; 16],
        sequence,
    };
    owner
        .submit(
            ControlRequest {
                id,
                acknowledged_through: sequence - 1,
                command,
            },
            &NoDirectoryAuthority,
        )
        .unwrap();
    for _ in 0..6 {
        owner.drain(&NoDirectoryAuthority).unwrap();
    }
    assert!(owner.receipt(id).unwrap().is_some());
}
#[tokio::test]
async fn only_installed_group_and_real_committed_fence_can_produce_an_accounted_share() {
    let dir = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(dir.path().into());
    settings.node.advertise = Some("127.0.0.1:7443".into());
    let mut network = FoundingNetwork::open(&settings).await.unwrap();
    let identity = network.directory.identity().clone();
    let now = unix_time().unwrap();
    let consensus = focal_consensus::DurableNode::open_on_wal(
        focal_consensus::NodeConfig::single(
            identity.node,
            identity.cluster,
            identity.ledger.session.0,
        ),
        network.wal.clone(),
    )
    .unwrap();
    let mut session =
        Session::from_node(identity.ledger, consensus, SessionLimits::default()).unwrap();
    session.campaign().unwrap();
    for _ in 0..6 {
        session.poll().unwrap();
    }
    let members = BTreeMap::from([(identity.node, 1)]);
    let request = SessionPlacementRequest {
        expected_index: 0,
        expected_configuration_index: session.membership().unwrap().configuration_index,
        operation: OperationId::from_u128(1),
        kind: SessionFenceKind::Created,
        from_route: RouteEpoch(0),
        to_route: RouteEpoch(1),
        membership_epoch: 1,
        placement_epoch: 1,
        placement: PlacementSpec {
            policy: PlacementPolicy {
                durability: DurabilityIntent {
                    survive: FailureClass::Node,
                    max_failures: 0,
                },
                residency: BTreeSet::new(),
                home_regions: BTreeSet::new(),
                required_memory: 0,
            },
            placement: Placement {
                voters: members.clone(),
                materializers: members.clone(),
                content_copies: members.clone(),
                preferred_leader: identity.node,
            },
        },
    };
    session.propose_placement(&request).unwrap();
    assert!(session.placement_witness(&request).unwrap().is_none());
    for _ in 0..5 {
        session.poll().unwrap();
    }
    let witness = session.placement_witness(&request).unwrap().unwrap();
    let budget = MemoryBudget::new(2 * WORKSPACE, WORKSPACE).unwrap();
    let window = ProofWindow {
        issued_at: now,
        expires_at: now + 60,
    };
    assert!(prepare_session_proof(&network.control, &witness, window, now, &budget).is_err());
    commit(
        &mut network.control,
        1,
        ControlCommand::ActivateAuthority(AuthorityActivation::Root {
            expected_root_revision: 0,
            expected_enrollment_revision: 1,
            decided_at: now,
        }),
    );
    let grant = NodeTopologyGrant {
        enrollment: NodeEnrollment {
            node: identity.node,
            generation: 1,
            region: RegionId([0; 16]),
            zone: ZoneId([0; 16]),
            endpoint: settings.node.advertise.clone().unwrap(),
            identity: ContentHash(server_fingerprint(&network.receipt.certificate)),
            authority_epoch: 1,
            attestation: ContentHash([0; 32]),
            eligible: true,
        },
        principal: identity.issuer.0,
        expires_at: now + 300,
    };
    let revision = network.control.authority().unwrap().revision();
    commit(
        &mut network.control,
        2,
        ControlCommand::Authority(AuthorityCommand {
            expected_revision: revision,
            enrollment_revision: 1,
            decided_at: now,
            operation: AuthorityOperation::GrantNode {
                grant,
                expected_generation: None,
            },
        }),
    );
    assert!(prepare_session_proof(&network.control, &witness, window, now, &budget).is_err());
    let revision = network.control.authority().unwrap().revision();
    commit(
        &mut network.control,
        3,
        ControlCommand::Authority(AuthorityCommand {
            expected_revision: revision,
            enrollment_revision: 1,
            decided_at: now,
            operation: AuthorityOperation::BootstrapGroup {
                grant: GroupAuthorityGrant {
                    group: LogGroupId(session.group_id()),
                    genesis: session.placement_genesis().unwrap(),
                    scope: GroupScope::Session(identity.ledger),
                    membership_epoch: 1,
                    voters: members,
                    outgoing_voters: BTreeMap::new(),
                    learners: BTreeMap::new(),
                    expires_at: now + 300,
                },
            },
        }),
    );
    assert!(
        prepare_session_proof(
            &network.control,
            &witness,
            ProofWindow {
                issued_at: now + 1,
                expires_at: now + 60
            },
            now,
            &budget
        )
        .is_err()
    );
    assert_eq!(budget.stats().used, 0);
    let no_projection_room = MemoryBudget::new(WORKSPACE, WORKSPACE).unwrap();
    assert!(matches!(
        prepare_session_proof(&network.control, &witness, window, now, &no_projection_room,),
        Err(PlacementProofError::Capacity)
    ));
    assert_eq!(no_projection_room.stats().used, 0);
    let permit = prepare_session_proof(&network.control, &witness, window, now, &budget).unwrap();
    assert!(budget.stats().used > 0);
    assert!(permit.sign(&network.enrollment_identity).is_err());
    assert_eq!(budget.stats().used, 0);
    let proof = prepare_session_proof(&network.control, &witness, window, now, &budget)
        .unwrap()
        .sign(&network.credentials)
        .unwrap();
    assert!(budget.stats().used > 0);
    network
        .control
        .authority()
        .unwrap()
        .verifier(
            network.control.enrollment().unwrap(),
            std::slice::from_ref(proof.proof()),
            now,
        )
        .unwrap()
        .verify_session_fence(witness.fence())
        .unwrap();
    let mut tampered = proof.proof().clone();
    if let AuthorityFact::Session(fence) = &mut tampered.statement.fact {
        fence.index.0 += 1;
    }
    assert!(
        network
            .control
            .authority()
            .unwrap()
            .verifier(network.control.enrollment().unwrap(), &[tampered], now)
            .unwrap()
            .verify_session_fence(witness.fence())
            .is_err()
    );
    drop(proof);
    assert_eq!(budget.stats().used, 0);

    // Exercise the actual owner queue: a delivered permit must keep its memory
    // admission after the control thread stops and until signing output drops.
    let owner_budget = MemoryBudget::new(64 * WORKSPACE, 16 * WORKSPACE).unwrap();
    let (host, owner, _outbound) = crate::control_host::ControlHost::spawn(
        network.control,
        NoDirectoryAuthority,
        crate::control_host::ControlHostConfig::new(network.state.genesis.root_namespace),
        owner_budget.clone(),
    )
    .unwrap();
    let permit = host
        .prepare_session_proof(
            session.placement_witness(&request).unwrap().unwrap(),
            window,
        )
        .await
        .unwrap();
    assert_eq!(
        permit.statement().fact,
        AuthorityFact::Session(witness.fence().clone())
    );
    assert!(owner_budget.stats().used >= WORKSPACE);
    host.stop().await.unwrap();
    assert!(matches!(
        host.prepare_session_proof(
            session.placement_witness(&request).unwrap().unwrap(),
            window
        )
        .await,
        Err(PlacementProofError::Unavailable)
    ));
    drop(host);
    owner.join().unwrap();
    assert!(owner_budget.stats().used >= WORKSPACE);
    let proof = permit.sign(&network.credentials).unwrap();
    assert!(owner_budget.stats().used > 0);
    assert!(owner_budget.stats().used < WORKSPACE);
    drop(proof);
    assert_eq!(owner_budget.stats().used, 0);
}
