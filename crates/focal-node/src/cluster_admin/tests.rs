use super::*;
use focal_wire::*;
use std::collections::BTreeSet;
#[test]
fn journal_reopens_exact_pending_and_rejects_loss_of_child_or_state() {
    let disk = tempfile::tempdir().unwrap();
    crate::set_test_mode(disk.path(), 0o700);
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

#[test]
fn reconciliation_proves_supersession_and_recovers_lost_receipt_without_reusing_reference() {
    focal_rt::block_on(async move {
        let disk = tempfile::tempdir().unwrap();
        crate::set_test_mode(disk.path(), 0o700);
        let mut settings = Settings::default();
        settings.node.data_dir = Some(disk.path().into());
        settings.node.advertise = Some("127.0.0.1:7443".into());
        let network = crate::network_bootstrap::FoundingNetwork::open(&settings)
            .await
            .unwrap();
        let identity = network.directory.identity().clone();
        let config = crate::control_host::ControlHostConfig::new(
            crate::network_state::root_namespace(&identity),
        );
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
        let server = LocalServer::bind(
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
        let mut serving = std::pin::pin!(serving);
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
        match hyper_rt::combine::race2(work, &mut serving).await {
            hyper_rt::combine::Either::First(()) => {}
            hyper_rt::combine::Either::Second(result) => panic!("server ended: {result:?}"),
        }
        server.close();
        serving.await.unwrap();
        control.stop().await.unwrap();
        owner.join().unwrap();
    })
    .unwrap()
}

/// A plan the partition refused is answered by name (24 §16): its reply
/// names no operation and no voters, and is `compare_failed` to the
/// operator — not an inconsistent response, which the macOS drain journey
/// was told when the decoder held the refusal to a plan's shape. A refusal
/// that names voters, and a plan that names none, are inconsistent still.
#[test]
fn a_refused_plan_is_compare_failed_and_a_planned_one_names_its_voters() {
    use crate::network_admin::{SESSION_PLANNED_REPLY_SCHEMA, SessionPlannedReply};
    let tenant = [1; 16];
    let session = [2; 16];
    let reply = |state: u8, operation: [u8; 16], voters: Vec<u64>| SessionPlannedReply {
        schema: SESSION_PLANNED_REPLY_SCHEMA,
        tenant,
        session,
        operation,
        voters,
        state,
        dry_run: false,
    };
    let encode = |reply: &SessionPlannedReply| postcard::to_allocvec(reply).unwrap();
    let refused = planned_reply(
        &encode(&reply(3, [0; 16], Vec::new())),
        tenant,
        session,
        false,
    )
    .unwrap();
    assert_eq!(refused.state, 3);
    assert!(matches!(
        planned_result(refused, "node", 1),
        Err(ClusterAdminError::Control(ControlFailure::CompareFailed))
    ));
    let planned = planned_reply(
        &encode(&reply(0, [3; 16], vec![1, 2, 3])),
        tenant,
        session,
        false,
    )
    .unwrap();
    match planned_result(planned, "zone", 1).unwrap() {
        AdminResult::SessionPlanned { voters, state, .. } => {
            assert_eq!(voters, vec![1, 2, 3]);
            assert_eq!(state, "planned");
        }
        other => panic!("{other:?}"),
    }
    for inconsistent in [
        reply(3, [3; 16], Vec::new()),
        reply(3, [0; 16], vec![1]),
        reply(0, [0; 16], vec![1, 2, 3]),
        reply(1, [3; 16], Vec::new()),
        reply(4, [3; 16], vec![1]),
    ] {
        assert!(
            matches!(
                planned_reply(&encode(&inconsistent), tenant, session, false),
                Err(ClusterAdminError::Invalid)
            ),
            "{inconsistent:?}"
        );
    }
    // The reply answers the request it was made for.
    assert!(matches!(
        planned_reply(&encode(&reply(0, [3; 16], vec![1])), tenant, session, true),
        Err(ClusterAdminError::Invalid)
    ));
}

/// A saved request no answer settled is resumed by a call that asks the same, and only by one:
/// the same membership change (at the configuration index it expected, when the call states
/// one), the same contact retired; a different change, node or kind of command is refused
/// `Pending` as before (`resumes`). A journal's request resumed end to end:
/// `the_same_membership_change_asked_again_resumes_its_unsettled_request`.
#[test]
fn an_unsettled_request_is_resumed_by_the_same_call_and_no_other() {
    let latest = |command: ControlCommand| Latest {
        operation: 1,
        superseded: false,
        receipt: None,
        request: ControlRequest {
            id: ControlRequestId {
                client: [7; 16],
                sequence: 1,
            },
            acknowledged_through: 0,
            command,
        },
    };
    let membership = latest(ControlCommand::Membership(ControlMembershipCommand {
        expected_configuration_index: 5,
        expected: MembershipConfiguration::default(),
        change: MembershipChange::AddLearner { node: 2 },
    }));
    let add = |node, expected_index| Intent::Membership {
        change: MembershipChange::AddLearner { node },
        expected_index,
    };
    assert!(resumes(&membership, &add(2, None)));
    assert!(resumes(&membership, &add(2, Some(5))));
    assert!(!resumes(&membership, &add(2, Some(6))), "another index");
    assert!(!resumes(&membership, &add(3, None)), "another change");
    assert!(!resumes(&membership, &Intent::RetireContact { node: 2 }));
    let retire = latest(ControlCommand::RetireContact(
        focal_control::RetireContactCommand {
            node: 4,
            expected_generation: 9,
        },
    ));
    assert!(resumes(&retire, &Intent::RetireContact { node: 4 }));
    assert!(!resumes(&retire, &Intent::RetireContact { node: 5 }));
    assert!(!resumes(
        &retire,
        &Intent::Eligibility {
            node: 4,
            eligible: false
        }
    ));
    assert!(!resumes(
        &retire,
        &Intent::Revoke {
            id: [1; 16],
            expected_revision: None
        }
    ));
}

/// A membership change saved and never settled (its call ended at a refusal the CLI names "try
/// again", or lost its answer) is resumed by the same change asked again: committed under the
/// request saved for it, its operation and its sequence, never a second time. A different change
/// asked meanwhile is refused `Pending`; once the first is settled, it is admitted.
#[test]
fn the_same_membership_change_asked_again_resumes_its_unsettled_request() {
    focal_rt::block_on(async move {
        let disk = tempfile::tempdir().unwrap();
        crate::set_test_mode(disk.path(), 0o700);
        let mut settings = Settings::default();
        settings.node.data_dir = Some(disk.path().into());
        settings.node.advertise = Some("127.0.0.1:7443".into());
        let network = crate::network_bootstrap::FoundingNetwork::open(&settings)
            .await
            .unwrap();
        let identity = network.directory.identity().clone();
        let config = crate::control_host::ControlHostConfig::new(
            crate::network_state::root_namespace(&identity),
        );
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
        let server = LocalServer::bind(
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
        let mut serving = std::pin::pin!(serving);
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
            assert!(matches!(
                admin
                    .membership(MembershipChange::AddLearner { node: peer_b }, None)
                    .await,
                Err(ClusterAdminError::Pending)
            ));
            let resumed = admin
                .membership(MembershipChange::AddLearner { node: peer_a }, None)
                .await
                .unwrap();
            let AdminResult::Committed {
                operation_id,
                sequence,
                ..
            } = &resumed
            else {
                panic!("committed: {resumed:?}")
            };
            assert_eq!(operation_id, &reference, "the saved request, not a new one");
            assert_eq!(*sequence, 1);
            // Settled, it is answered by its receipt, and the next change is admitted.
            assert_eq!(admin.retry(&reference).await.unwrap(), resumed);
            let next = admin
                .membership(MembershipChange::AddLearner { node: peer_b }, None)
                .await
                .unwrap();
            assert!(
                matches!(&next, AdminResult::Committed { sequence: 2, .. }),
                "{next:?}"
            );
        };
        match hyper_rt::combine::race2(work, &mut serving).await {
            hyper_rt::combine::Either::First(()) => {}
            hyper_rt::combine::Either::Second(result) => panic!("server ended: {result:?}"),
        }
        server.close();
        serving.await.unwrap();
        control.stop().await.unwrap();
        owner.join().unwrap();
    })
    .unwrap()
}

/// An operator's command beside a node that has written its identity and not yet its keys, a
/// founder between the two as an earlier binary founded (one now makes its keys first), finds no
/// keys and makes none: it is refused `not_ready` for the moment, where it made the directory's
/// keys in its node's place.
#[test]
fn an_admin_command_beside_a_founding_directory_makes_no_keys() {
    let disk = tempfile::tempdir().unwrap();
    crate::set_test_mode(disk.path(), 0o700);
    let mut settings = Settings::default();
    settings.node.data_dir = Some(disk.path().into());
    // The founder's directory: its identity written, its keys not yet made.
    crate::node_directory::save(
        &crate::node_files::NodeFiles::new(disk.path(), None),
        &crate::embedded::new_identity().unwrap(),
    )
    .unwrap();
    assert!(settings.found_data_keys(disk.path()).unwrap().is_none());
    let refused = ClusterAdmin::open(&settings).err();
    assert!(
        matches!(refused, Some(ClusterAdminError::Unkeyed(_))),
        "{:?}",
        refused.as_ref().map(ToString::to_string)
    );
    assert_eq!(
        refused.map(|error| error.classification().code),
        Some("not_ready")
    );
    assert!(
        settings.found_data_keys(disk.path()).unwrap().is_none(),
        "the command made the directory's keys"
    );
}
