use super::*;
use crate::{
    config::Settings,
    network_bootstrap::{FoundingNetwork, unix_time},
};
use focal_directory::*;

pub(crate) fn commit(owner: &mut ControlReplica, sequence: u64, command: ControlCommand) {
    let id = ControlRequestId {
        client: [113; 16],
        sequence,
    };
    owner
        .submit(
            ControlRequest {
                id,
                acknowledged_through: sequence - 1,
                command,
            },
            &crate::cluster::NoDirectoryAuthority,
        )
        .unwrap();
    for _ in 0..8 {
        owner.drain(&crate::cluster::NoDirectoryAuthority).unwrap();
    }
    assert!(owner.receipt(id).unwrap().is_some());
}

pub(crate) async fn prepared_network(
    directory: &std::path::Path,
) -> (FoundingNetwork, FirstDirectoryPlan) {
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.into());
    settings.node.advertise = Some("127.0.0.1:7443".into());
    let mut network = FoundingNetwork::open(&settings).await.unwrap();
    let identity = network.directory.identity().clone();
    let now = unix_time().unwrap();
    let plan = FirstDirectoryPlan::derive(identity.cluster, identity.node).unwrap();
    assert!(matches!(
        authorize_first_directory(&network.control, plan, now, &network.budget),
        Err(DirectoryBootstrapError::Unauthorized)
    ));
    commit(
        &mut network.control,
        1,
        ControlCommand::Root(RootCommand {
            expected_revision: 0,
            operation: RootOperation::Delegate {
                delegation: plan.delegation(),
            },
        }),
    );
    assert!(
        network
            .control
            .root()
            .unwrap()
            .checkpoint()
            .regions
            .is_empty()
    );
    commit(
        &mut network.control,
        2,
        ControlCommand::ActivateAuthority(AuthorityActivation::Root {
            expected_root_revision: 1,
            expected_enrollment_revision: 1,
            decided_at: now,
        }),
    );
    commit(
        &mut network.control,
        3,
        ControlCommand::Authority(AuthorityCommand {
            expected_revision: 1,
            enrollment_revision: 1,
            decided_at: now,
            operation: AuthorityOperation::GrantNode {
                grant: NodeTopologyGrant {
                    enrollment: NodeEnrollment {
                        node: identity.node,
                        generation: 1,
                        region: RegionId::UNKNOWN,
                        zone: ZoneId([0; 16]),
                        endpoint: "127.0.0.1:7443".into(),
                        identity: ContentHash(network.receipt.public_key),
                        authority_epoch: 1,
                        attestation: ContentHash([0; 32]),
                        eligible: true,
                    },
                    principal: identity.issuer.0,
                    expires_at: now + 300,
                },
                expected_generation: None,
            },
        }),
    );
    assert!(matches!(
        authorize_first_directory(&network.control, plan, now, &network.budget),
        Err(DirectoryBootstrapError::Unauthorized)
    ));
    commit(
        &mut network.control,
        4,
        ControlCommand::Authority(AuthorityCommand {
            expected_revision: 2,
            enrollment_revision: 1,
            decided_at: now,
            operation: AuthorityOperation::BootstrapGroup {
                grant: GroupAuthorityGrant {
                    group: plan.group(),
                    genesis: ContentHash(plan.identity().unwrap().genesis),
                    scope: GroupScope::Partition {
                        partition: plan.partition(),
                        namespace: NamespaceRange::all(),
                    },
                    membership_epoch: 1,
                    voters: BTreeMap::from([(identity.node, 1)]),
                    outgoing_voters: BTreeMap::new(),
                    learners: BTreeMap::new(),
                    expires_at: now + 300,
                },
            },
        }),
    );
    (network, plan)
}

#[tokio::test]
async fn committed_delegation_bootstraps_shared_wal_and_restarts_or_refreshes_exact_client() {
    let directory = tempfile::tempdir().unwrap();
    let (mut network, plan) = prepared_network(directory.path()).await;
    let budget = MemoryBudget::new(256 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let owner_index = network.control.applied_index();
    let permit =
        authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget).unwrap();
    assert!(budget.stats().used > 0);
    let opened = permit.open(network.wal.clone(), &budget).unwrap();
    assert_eq!(opened.plan(), plan);
    assert_eq!(opened.replica().identity(), plan.identity().unwrap());
    assert_eq!(network.control.applied_index(), owner_index);
    assert!(
        opened
            .replica()
            .partition()
            .unwrap()
            .checkpoint()
            .sessions
            .is_empty()
    );
    let mut replica = opened.into_replica();
    let receipt = replica
        .latest_receipt(plan.client().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(receipt.request.sequence, 1);
    let ControlReadResult::Authority(Some(installed)) =
        replica.read_local(&ControlRead::Authority).unwrap()
    else {
        panic!("installed authority");
    };
    assert_eq!(installed.source_index, owner_index);
    replica.checkpoint().unwrap();
    drop(replica);
    assert_eq!(budget.stats().used, 0);

    let opened = authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget)
        .unwrap()
        .open(network.wal.clone(), &budget)
        .unwrap();
    assert_eq!(
        opened
            .replica()
            .latest_receipt(plan.client().unwrap())
            .unwrap(),
        Some(receipt)
    );
    drop(opened);
    let revision = network.control.authority().unwrap().revision();
    commit(
        &mut network.control,
        5,
        ControlCommand::Authority(AuthorityCommand {
            expected_revision: revision,
            enrollment_revision: 1,
            decided_at: unix_time().unwrap(),
            operation: AuthorityOperation::AdvanceClock,
        }),
    );
    let opened = authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget)
        .unwrap()
        .open(network.wal.clone(), &budget)
        .unwrap();
    assert_eq!(
        opened
            .replica()
            .latest_receipt(plan.client().unwrap())
            .unwrap()
            .unwrap()
            .request
            .sequence,
        2
    );
    assert!(matches!(
        opened.replica().receipt(receipt.request),
        Err(ControlError::RetryExpired)
    ));
    let ControlReadResult::Authority(Some(installed)) = opened
        .replica()
        .read_local(&ControlRead::Authority)
        .unwrap()
    else {
        panic!("installed authority");
    };
    assert_eq!(installed.source_index, network.control.applied_index());
    drop(opened);
    assert_eq!(budget.stats().used, 0);
}

#[tokio::test]
async fn directory_permit_rejects_wrong_assignment_revocation_expiry_and_unfunded_export() {
    let directory = tempfile::tempdir().unwrap();
    let (mut network, plan) = prepared_network(directory.path()).await;
    let budget = MemoryBudget::new(256 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let wrong = FirstDirectoryPlan::derive(plan.cluster, plan.founder_node + 1).unwrap();
    assert!(matches!(
        authorize_first_directory(&network.control, wrong, unix_time().unwrap(), &budget),
        Err(DirectoryBootstrapError::Unauthorized)
    ));
    let small = MemoryBudget::new(4096, 1024).unwrap();
    assert!(matches!(
        authorize_first_directory(&network.control, plan, unix_time().unwrap(), &small),
        Err(DirectoryBootstrapError::Capacity)
    ));
    assert_eq!(small.stats().used, 0);
    let mut permit =
        authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget).unwrap();
    permit.expires_at = unix_time().unwrap();
    assert!(matches!(
        permit.open(network.wal.clone(), &budget),
        Err(DirectoryBootstrapError::Unauthorized)
    ));
    assert_eq!(budget.stats().used, 0);
    let revoke = network
        .control
        .enrollment()
        .unwrap()
        .prepare_revoke(network.receipt.invitation, unix_time().unwrap())
        .unwrap();
    commit(&mut network.control, 5, ControlCommand::Enrollment(revoke));
    assert!(matches!(
        authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget),
        Err(DirectoryBootstrapError::Unauthorized)
    ));
    assert_eq!(budget.stats().used, 0);
}

#[tokio::test]
async fn first_directory_planner_uses_owned_root_observation_and_committed_comparisons() {
    use crate::control_host::{ControlHost, ControlHostConfig};
    use focal_model::{ParticipantId, RequestId};
    use focal_wire::{AuthenticatedPeer, PeerGrant, PeerRole};
    use std::collections::BTreeSet;
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().into());
    settings.node.advertise = Some("127.0.0.1:7443".into());
    let mut network = FoundingNetwork::open(&settings).await.unwrap();
    let identity = network.directory.identity().clone();
    let plan = FirstDirectoryPlan::derive(identity.cluster, identity.node).unwrap();
    let now = unix_time().unwrap();
    commit(
        &mut network.control,
        1,
        ControlCommand::ActivateAuthority(AuthorityActivation::Root {
            expected_root_revision: 0,
            expected_enrollment_revision: 1,
            decided_at: now,
        }),
    );
    commit(
        &mut network.control,
        2,
        ControlCommand::Authority(AuthorityCommand {
            expected_revision: 1,
            enrollment_revision: 1,
            decided_at: now,
            operation: AuthorityOperation::GrantNode {
                grant: NodeTopologyGrant {
                    enrollment: NodeEnrollment {
                        node: identity.node,
                        generation: 1,
                        region: RegionId::UNKNOWN,
                        zone: ZoneId([0; 16]),
                        endpoint: "127.0.0.1:7443".into(),
                        identity: ContentHash(network.receipt.public_key),
                        authority_epoch: 1,
                        attestation: ContentHash([0; 32]),
                        eligible: true,
                    },
                    principal: identity.issuer.0,
                    expires_at: now + 300,
                },
                expected_generation: None,
            },
        }),
    );
    let budget = MemoryBudget::new(256 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let (host, owner, _outgoing) = ControlHost::spawn(
        network.control,
        crate::cluster::NoDirectoryAuthority,
        ControlHostConfig::new(network.state.genesis.root_namespace),
        budget.clone(),
    )
    .unwrap();
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId([113; 16]),
        tenants: BTreeSet::from([network.state.genesis.root_namespace.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap();
    let observed = host.observe_root().await.unwrap();
    let command = next_first_directory_command(plan, &observed, now, &budget)
        .unwrap()
        .unwrap();
    assert!(matches!(&command, ControlCommand::VerifiedRoot(command)
        if command.command.expected_revision == 0
        && matches!(&command.command.operation, RootOperation::Delegate { delegation } if *delegation == plan.delegation())));
    host.submit(
        peer.clone(),
        ControlRequest {
            id: ControlRequestId {
                client: [113; 16],
                sequence: 3,
            },
            acknowledged_through: 2,
            command,
        },
    )
    .await
    .unwrap();
    drop(observed);
    let observed = host.observe_root().await.unwrap();
    let command = next_first_directory_command(plan, &observed, now, &budget)
        .unwrap()
        .unwrap();
    assert!(matches!(&command, ControlCommand::Authority(command)
        if matches!(&command.operation, AuthorityOperation::BootstrapGroup { grant }
            if grant.genesis == ContentHash(plan.identity().unwrap().genesis)
            && grant.voters == BTreeMap::from([(identity.node, 1)]))));
    host.submit(
        peer.clone(),
        ControlRequest {
            id: ControlRequestId {
                client: [113; 16],
                sequence: 4,
            },
            acknowledged_through: 3,
            command,
        },
    )
    .await
    .unwrap();
    drop(observed);
    let observed = host.observe_root().await.unwrap();
    assert!(
        next_first_directory_command(plan, &observed, now, &budget)
            .unwrap()
            .is_none()
    );
    let small = MemoryBudget::new(4096, 1024).unwrap();
    assert!(matches!(
        next_first_directory_command(plan, &observed, now, &small),
        Err(DirectoryBootstrapError::Capacity)
    ));
    assert_eq!(small.stats().used, 0);
    let wrong = FirstDirectoryPlan::derive(identity.cluster, identity.node + 1).unwrap();
    assert!(matches!(
        next_first_directory_command(wrong, &observed, now, &budget),
        Err(DirectoryBootstrapError::NotReady)
    ));
    drop(observed);
    let ControlReadResult::State(state) = host
        .read(peer, RequestId::from_u128(99), ControlRead::State)
        .await
        .unwrap()
    else {
        panic!("root state");
    };
    let ControlBootstrap::Root { directory, .. } = state.state else {
        panic!("root state");
    };
    assert!(directory.regions.is_empty());
    assert_eq!(directory.delegations.len(), 1);
    host.stop().await.unwrap();
    owner.join().unwrap();
}

#[tokio::test]
async fn interrupted_activation_recovers_the_logged_request_before_selecting_a_successor() {
    let directory = tempfile::tempdir().unwrap();
    let (network, plan) = prepared_network(directory.path()).await;
    let budget = MemoryBudget::new(256 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let permit =
        authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget).unwrap();
    let mut replica = ControlReplica::open_on_wal(
        plan.options(),
        plan.bootstrap(),
        budget.clone(),
        network.wal.clone(),
    )
    .unwrap();
    establish_barrier(&mut replica, plan).unwrap();
    let request = ControlRequest {
        id: ControlRequestId {
            client: plan.client().unwrap(),
            sequence: 1,
        },
        acknowledged_through: 0,
        command: ControlCommand::ActivateAuthority(AuthorityActivation::Partition {
            expected_partition_revision: 0,
            snapshot: permit.snapshot,
        }),
    };
    replica
        .submit(request.clone(), &crate::cluster::NoDirectoryAuthority)
        .unwrap();
    replica.inject_fault_once(focal_consensus::FaultPoint::AfterFenceInstall);
    assert!(
        replica
            .drain(&crate::cluster::NoDirectoryAuthority)
            .is_err()
    );
    assert!(replica.authority().is_none());
    drop(replica);
    drop(network);
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().into());
    let network = FoundingNetwork::open(&settings).await.unwrap();
    let mut replica = ControlReplica::open_on_wal(
        plan.options(),
        plan.bootstrap(),
        budget.clone(),
        network.wal.clone(),
    )
    .unwrap();
    establish_barrier(&mut replica, plan).unwrap();
    let receipt = replica
        .latest_receipt(plan.client().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(receipt.request, request.id);
    assert!(
        matches!(replica.submit(request, &crate::cluster::NoDirectoryAuthority).unwrap(),
        focal_control::ControlSubmission::Existing(existing) if existing == receipt)
    );
    drop(replica);
    let opened = authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget)
        .unwrap()
        .open(network.wal.clone(), &budget)
        .unwrap();
    assert_eq!(
        opened
            .replica()
            .latest_receipt(plan.client().unwrap())
            .unwrap()
            .unwrap()
            .request
            .sequence,
        2
    );
}

#[path = "directory_bootstrap_owner_tests.rs"]
mod owner_tests;
