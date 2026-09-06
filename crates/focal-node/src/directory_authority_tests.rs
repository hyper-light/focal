use super::*;
use crate::{
    cluster::NoDirectoryAuthority,
    directory_bootstrap::tests::{commit, prepared_network},
    directory_bootstrap::{FirstDirectoryPlan, authorize_first_directory},
    network_bootstrap::unix_time,
};
use focal_directory::{AuthorityCommand, AuthorityOperation};
use focal_enrollment::{
    BootstrapAuthority, EnrollmentLimits, EnrollmentRegistry, EnrollmentRole, InviteOptions,
};

fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 128 * 1024 * 1024).unwrap()
}
fn runtime(plan: FirstDirectoryPlan) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: focal_model::ParticipantId::from_u128(777),
        tenants: [plan.namespace().tenant].into_iter().collect(),
        role: PeerRole::Runtime,
    })
    .unwrap()
}
async fn read_authority(host: &ControlHost, plan: FirstDirectoryPlan) -> ControlAuthoritySnapshot {
    let ControlReadResult::StateAndAuthority {
        snapshot,
        authority: Some(authority),
    } = host
        .read(
            runtime(plan),
            RequestId::from_u128(1),
            ControlRead::StateAndAuthority,
        )
        .await
        .unwrap()
    else {
        panic!("installed authority missing")
    };
    assert_eq!(snapshot.applied_index, authority.applied_index);
    assert_eq!(snapshot.identity, authority.identity);
    authority
}

#[tokio::test]
async fn live_directory_refresh_installs_revocation_retries_exactly_and_rejects_stale_or_expired_permits()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut network, plan) = prepared_network(directory.path()).await;
    let budget = budget();
    let now = unix_time().unwrap();
    drop(network.enrollment_driver);
    let authority = BootstrapAuthority::open_or_create(
        directory.path().join("cluster/network/authority"),
        plan.identity().unwrap().cluster.0,
        vec![network.state.sponsor.server_name.clone()],
        now,
    )
    .unwrap();
    let draft = network
        .control
        .enrollment()
        .unwrap()
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:7443".into(),
                server_name: network.state.sponsor.server_name.clone(),
                role: EnrollmentRole::Client,
                expires_at: now + 60,
            },
            now,
        )
        .unwrap();
    commit(
        &mut network.control,
        5,
        ControlCommand::Enrollment(draft.command().clone()),
    );
    let invitation = draft
        .release(network.control.enrollment().unwrap())
        .unwrap();
    let initial = authorize_first_directory(&network.control, plan, now, &budget).unwrap();
    let stale = authorize_first_directory(&network.control, plan, now, &budget).unwrap();
    let opened = initial.open(network.wal.clone(), &budget).unwrap();
    let expires_at = network
        .control
        .authority()
        .unwrap()
        .group(plan.group())
        .unwrap()
        .expires_at;
    let expired = authorize_first_directory(&network.control, plan, now, &budget).unwrap();
    assert!(matches!(
        expired.prepare_refresh(opened.replica(), expires_at, &budget),
        Err(DirectoryBootstrapError::Unauthorized)
    ));
    let (host, owner, outgoing) = ControlHost::spawn(
        opened.into_replica(),
        NoDirectoryAuthority,
        ControlHostConfig::new(plan.namespace()),
        budget.clone(),
    )
    .unwrap();
    let before = read_authority(&host, plan).await;
    let registry = EnrollmentRegistry::restore(
        &before.enrollment,
        plan.identity().unwrap().cluster.0,
        EnrollmentLimits::default(),
    )
    .unwrap();
    assert!(!registry.invitation_revoked(invitation.id()).unwrap());
    host.submit(
        runtime(plan),
        ControlRequest {
            id: ControlRequestId {
                client: runtime(plan).principal().0,
                sequence: 1,
            },
            acknowledged_through: 0,
            command: ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
                command: focal_directory::PartitionCommand {
                    expected_revision: 0,
                    delegation_epoch: 1,
                    operation: focal_directory::PartitionOperation::Enroll {
                        node: before.authority.nodes[&plan.founder_node()]
                            .enrollment
                            .clone(),
                        expected_generation: None,
                    },
                },
                evidence: ControlEvidence {
                    authority_revision: before.authority.revision,
                    enrollment_revision: registry.revision(),
                    decided_at: unix_time().unwrap(),
                    proofs: vec![],
                },
            }),
        },
    )
    .await
    .unwrap();
    let revoke = network
        .control
        .enrollment()
        .unwrap()
        .prepare_revoke(invitation.id(), now)
        .unwrap();
    commit(&mut network.control, 6, ControlCommand::Enrollment(revoke));
    let permit = authorize_first_directory(&network.control, plan, now, &budget).unwrap();
    let receipt = tokio::time::timeout(Duration::from_secs(5), host.refresh_directory(permit))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.root_index, network.control.applied_index());
    assert_eq!(receipt.receipt.request.sequence, 2);
    assert!(receipt.applied_index >= receipt.receipt.committed_index);
    let installed = read_authority(&host, plan).await;
    let registry = EnrollmentRegistry::restore(
        &installed.enrollment,
        plan.identity().unwrap().cluster.0,
        EnrollmentLimits::default(),
    )
    .unwrap();
    assert!(registry.invitation_revoked(invitation.id()).unwrap());
    assert_eq!(installed.source_index, receipt.root_index);
    assert_eq!(host.progress().revisions.partition, 1);
    assert!(matches!(
        host.refresh_directory(stale).await,
        Err(DirectoryBootstrapError::Unauthorized)
    ));
    let again = host
        .refresh_directory(authorize_first_directory(&network.control, plan, now, &budget).unwrap())
        .await
        .unwrap();
    assert_eq!(again.receipt, receipt.receipt);
    assert_eq!(again.root_index, receipt.root_index);
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop(host);
    drop(outgoing);
    assert_eq!(budget.stats().used, 0);
    let reopened = authorize_first_directory(&network.control, plan, now, &budget)
        .unwrap()
        .open(network.wal.clone(), &budget)
        .unwrap();
    assert_eq!(
        reopened.replica().receipt(receipt.receipt.request).unwrap(),
        Some(receipt.receipt)
    );
    drop(reopened);
    assert_eq!(budget.stats().used, 0);
}

fn owner(
    replica: ControlReplica,
    plan: FirstDirectoryPlan,
    budget: MemoryBudget,
) -> Owner<NoDirectoryAuthority> {
    let status = replica.status();
    let (outbound, _outgoing) = async_mpsc::channel(8);
    let (progress, _changes) = watch::channel(ControlProgressState {
        value: ControlProgress {
            identity: replica.identity(),
            node: status.node_id,
            leader: status.leader_id,
            term: status.term,
            applied_index: replica.applied_index(),
            revisions: replica.revisions(),
            dropped_replication: 0,
            stopped: false,
        },
        _allocation: None,
    });
    Owner {
        replica,
        initial: None,
        verifier: NoDirectoryAuthority,
        config: ControlHostConfig::new(plan.namespace()),
        limits: ControlHost::wire_limits(),
        budget,
        pending: VecDeque::new(),
        directory: None,
        authority_refresh: None,
        outbound,
        progress,
        nonce: 0,
        dropped: 0,
    }
}

#[tokio::test]
async fn canceled_admitted_refresh_keeps_exact_intent_and_recovers_unknown_commit() {
    let directory = tempfile::tempdir().unwrap();
    let (mut network, plan) = prepared_network(directory.path()).await;
    let budget = budget();
    let opened = authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget)
        .unwrap()
        .open(network.wal.clone(), &budget)
        .unwrap();
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
    let permit =
        authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget).unwrap();
    let replacement =
        authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget).unwrap();
    let mut owner = owner(opened.into_replica(), plan, budget.clone());
    let change = Box::new(
        permit
            .prepare_refresh(&owner.replica, unix_time().unwrap(), &budget)
            .unwrap(),
    );
    let request = change.request.as_ref().unwrap().clone();
    assert_eq!(
        owner
            .replica
            .submit(request.clone(), &NoDirectoryAuthority)
            .unwrap(),
        ControlSubmission::Pending(request.id)
    );
    let mut input = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, 2048)
        .unwrap()
        .commit();
    let reply_charge = input.split_off(512).unwrap();
    let (send, receive) = oneshot::channel();
    owner.authority_refresh = Some(PendingAuthority {
        phase: Some(RefreshPhase::Writing(change)),
        context: None,
        term: owner.replica.status().term,
        deadline: Instant::now() + Duration::from_secs(5),
        response: Some(send),
        reply_charge: Some(reply_charge),
        _input: input,
    });
    drop(receive);
    let mut pending = owner.authority_refresh.take().unwrap();
    let events = ControlEvents::default();
    assert!(
        owner
            .advance_authority_refresh(&mut pending, &events)
            .unwrap()
            .is_none()
    );
    assert!(matches!(pending.phase, Some(RefreshPhase::Writing(_))));
    assert!(pending.response.is_none());
    owner.authority_refresh = Some(pending);
    let mut input = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, 2048)
        .unwrap()
        .commit();
    let charge = input.split_off(512).unwrap();
    let (send, receive) = oneshot::channel();
    owner.refresh_directory(Box::new(replacement), send, input, charge);
    let (result, charge) = receive.await.unwrap();
    assert!(matches!(result, Err(DirectoryBootstrapError::Capacity)));
    drop(charge);
    assert!(matches!(
        owner.authority_refresh.as_ref().unwrap().phase,
        Some(RefreshPhase::Writing(_))
    ));
    owner
        .replica
        .inject_fault_once(focal_consensus::FaultPoint::AfterFenceInstall);
    assert!(owner.drain().is_err());
    drop(owner);
    drop(network);
    assert_eq!(budget.stats().used, 0);

    let mut settings = crate::config::Settings::default();
    settings.node.data_dir = Some(directory.path().into());
    let network = crate::network_bootstrap::FoundingNetwork::open(&settings)
        .await
        .unwrap();
    let mut replica = ControlReplica::open_on_wal(
        ControlOptions::new(focal_consensus::NodeConfig::single(
            plan.founder_node(),
            plan.identity().unwrap().cluster.0,
            plan.group().0,
        )),
        plan.bootstrap(),
        budget.clone(),
        network.wal.clone(),
    )
    .unwrap();
    for _ in 0..8 {
        replica.drain(&NoDirectoryAuthority).unwrap();
    }
    replica.campaign().unwrap();
    for _ in 0..8 {
        replica.drain(&NoDirectoryAuthority).unwrap();
    }
    let recovered = replica.receipt(request.id).unwrap().unwrap();
    assert_eq!(recovered.request, request.id);
    assert_eq!(
        replica.submit(request, &NoDirectoryAuthority).unwrap(),
        ControlSubmission::Existing(recovered)
    );
    drop(replica);
    assert_eq!(budget.stats().used, 0);
}
