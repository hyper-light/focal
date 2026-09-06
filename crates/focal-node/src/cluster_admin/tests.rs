use super::*;
use focal_wire::*;
use std::collections::BTreeSet;
#[test]
fn journal_reopens_exact_pending_and_rejects_loss_of_child_or_state() {
    let disk = tempfile::tempdir().unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(disk.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(disk.path().into());
    let node = crate::embedded::EmbeddedNode::open(&settings).unwrap();
    drop(node);
    let admin = ClusterAdmin::open(&settings).unwrap();
    let (mut journal, mut saved) = admin.journal(true).unwrap();
    let node = admin.identity.node;
    saved.latest = Some(Latest {
        operation: 1,
        superseded: false,
        request: ControlRequest {
            id: ControlRequestId {
                client: admin_principal(&admin.identity).0,
                sequence: 1,
            },
            acknowledged_through: 0,
            command: ControlCommand::Membership(ControlMembershipCommand {
                expected_configuration_index: 0,
                expected: MembershipConfiguration {
                    voters: vec![node],
                    ..Default::default()
                },
                change: MembershipChange::AddLearner {
                    node: node.checked_add(1).unwrap(),
                },
            }),
        },
        receipt: None,
    });
    saved.next = 2;
    save(&mut journal, &saved).unwrap();
    assert!(matches!(
        admin.journal(false),
        Err(ClusterAdminError::Journal(
            focal_enrollment::EnrollmentError::Locked
        ))
    ));
    drop(journal);
    assert_eq!(
        admin.inspect().unwrap(),
        AdminResult::Request {
            operation_id: operation_id(node, 1),
            state: "Pending".into()
        }
    );
    let directory = admin.root.join(DIRECTORY);
    fs::remove_file(directory.join("journal.bin")).unwrap();
    fs::remove_file(directory.join("journal.bin.initialized")).unwrap();
    assert!(matches!(
        admin.journal(true),
        Err(ClusterAdminError::Corrupt)
    ));
    fs::remove_dir_all(directory).unwrap();
    assert!(matches!(
        admin.journal(true),
        Err(ClusterAdminError::Corrupt)
    ));
}

#[tokio::test]
async fn reconciliation_proves_supersession_and_recovers_lost_receipt_without_reusing_reference() {
    use std::os::unix::fs::PermissionsExt;
    let disk = tempfile::tempdir().unwrap();
    fs::set_permissions(disk.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(disk.path().into());
    settings.node.advertise = Some("127.0.0.1:7443".into());
    let network = crate::network_bootstrap::FoundingNetwork::open(&settings)
        .await
        .unwrap();
    let identity = network.directory.identity().clone();
    let config = crate::control_host::ControlHostConfig::new(crate::network_state::root_namespace(
        &identity,
    ));
    let (control, owner, _outgoing) = crate::control_host::ControlHost::spawn_recovered(
        network.control,
        crate::cluster::NoDirectoryAuthority,
        config,
        network
            .budget
            .child(64 * 1024 * 1024, 16 * 1024 * 1024)
            .unwrap(),
        network.recovered,
    )
    .unwrap();
    let handler = crate::network_admin::LocalNetworkAdmin::for_node(
        &network.directory,
        network.state.genesis.root,
        network.state.advertise,
        Some(network.enrollment.clone()),
        network
            .budget
            .child(16 * 1024 * 1024, 4 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap()
    .with_control(control.clone())
    .unwrap();
    let server = UnixServer::bind(
        disk.path().join(ADMIN_SOCKET),
        PeerGrant {
            principal: identity.issuer,
            tenants: BTreeSet::from([identity.ledger.tenant]),
            role: PeerRole::Runtime,
        },
        admin_wire_limits(),
    )
    .unwrap();
    let serving = server.serve(handler);
    tokio::pin!(serving);
    let work = async {
        let admin = ClusterAdmin::open(&settings).unwrap();
        let original = admin.configuration().await.unwrap();
        let peer_a = identity.node.checked_add(1).unwrap();
        let peer_b = peer_a.checked_add(1).unwrap();
        let (mut journal, mut saved) = admin.journal(true).unwrap();
        saved.next = 2;
        saved.latest = Some(Latest {
            operation: 1,
            superseded: false,
            receipt: None,
            request: ControlRequest {
                id: ControlRequestId {
                    client: admin_principal(&identity).0,
                    sequence: 1,
                },
                acknowledged_through: 0,
                command: ControlCommand::Membership(ControlMembershipCommand {
                    expected_configuration_index: original.configuration_index,
                    expected: original.configuration.clone(),
                    change: MembershipChange::AddLearner { node: peer_a },
                }),
            },
        });
        save(&mut journal, &saved).unwrap();
        drop(journal);
        let reference = operation_id(identity.node, 1);
        let pending = admin.reconcile(&reference).await.unwrap();
        assert!(matches!(pending,AdminResult::Request {state,..} if state=="Pending"));
        let other = focal_model::ParticipantId::from_u128(990);
        control
            .submit(
                AuthenticatedPeer::local(PeerGrant {
                    principal: other,
                    tenants: BTreeSet::from([identity.ledger.tenant]),
                    role: PeerRole::Runtime,
                })
                .unwrap(),
                ControlRequest {
                    id: ControlRequestId {
                        client: other.0,
                        sequence: 1,
                    },
                    acknowledged_through: 0,
                    command: ControlCommand::Membership(ControlMembershipCommand {
                        expected_configuration_index: original.configuration_index,
                        expected: original.configuration,
                        change: MembershipChange::AddLearner { node: peer_b },
                    }),
                },
            )
            .await
            .unwrap();
        let superseded = admin.reconcile(&reference).await.unwrap();
        assert!(matches!(superseded,AdminResult::Request {state,..} if state=="Superseded"));
        assert!(matches!(
            admin.retry(&reference).await,
            Err(ClusterAdminError::Expired)
        ));
        let completed = admin
            .membership(MembershipChange::Remove { node: peer_b }, None)
            .await
            .unwrap();
        let AdminResult::Committed {
            operation_id: new_reference,
            sequence,
            ..
        } = &completed
        else {
            panic!("committed")
        };
        assert_eq!(*sequence, 1);
        assert_ne!(new_reference, &reference);
        assert!(matches!(
            admin.retry(&reference).await,
            Err(ClusterAdminError::Expired)
        ));
        // Simulate death after server commit but before recording its receipt.
        let (mut journal, mut saved) = admin.journal(false).unwrap();
        saved.latest.as_mut().unwrap().receipt = None;
        saved.next_control = 1;
        save(&mut journal, &saved).unwrap();
        drop(journal);
        assert_eq!(admin.reconcile(new_reference).await.unwrap(), completed);
        assert_eq!(admin.retry(new_reference).await.unwrap(), completed);
    };
    tokio::select! {()=work=>{},result=&mut serving=>panic!("server ended: {result:?}")}
    server.close();
    serving.await.unwrap();
    control.stop().await.unwrap();
    owner.join().unwrap();
}
