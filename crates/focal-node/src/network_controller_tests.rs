use super::*;
use crate::{config::Settings, network_bootstrap::FoundingNetwork};

async fn founder(root: &std::path::Path) -> (NetworkState, EnrollmentReceipt, CredentialMaterial) {
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.to_owned());
    settings.node.advertise = Some("127.0.0.1:45678".into());
    let network = FoundingNetwork::open(&settings).await.unwrap();
    (
        network.state.clone(),
        network.receipt.clone(),
        network.credentials.clone(),
    )
}

#[tokio::test]
async fn admission_reopens_exact_request_and_rejects_lost_or_inconsistent_journal() {
    let directory = tempfile::tempdir().unwrap();
    let (state, _, _) = founder(directory.path()).await;
    let mut admission = RootAdmission::open(&state, directory.path()).unwrap();
    let pending = ControlRequest {
        id: ControlRequestId {
            client: admission.principal.0,
            sequence: 9,
        },
        acknowledged_through: 8,
        command: ControlCommand::Membership(ControlMembershipCommand {
            expected_configuration_index: 17,
            expected: focal_consensus::MembershipConfiguration {
                voters: vec![state.node],
                ..Default::default()
            },
            change: MembershipChange::AddLearner { node: 99 },
        }),
    };
    admission.intent.completed = 8;
    admission.intent.pending = Some(pending.clone());
    admission.save().unwrap();
    drop(admission);
    let mut recovered = RootAdmission::open(&state, directory.path()).unwrap();
    assert_eq!(
        postcard::to_stdvec(recovered.intent.pending.as_ref().unwrap()).unwrap(),
        postcard::to_stdvec(&pending).unwrap()
    );
    recovered.intent.completed = 9; // Corrupt the acknowledgement/sequence fence.
    recovered.save().unwrap();
    drop(recovered);
    assert!(matches!(
        RootAdmission::open(&state, directory.path()),
        Err(ControllerError::Identity)
    ));
    std::fs::remove_dir_all(directory.path().join("cluster/root-admission")).unwrap();
    assert!(matches!(
        RootAdmission::open(&state, directory.path()),
        Err(ControllerError::Identity)
    ));
    assert!(!directory.path().join("cluster/root-admission").exists());
}

#[tokio::test]
async fn controller_admission_precedes_private_journal_and_releases_on_drop() {
    let directory = tempfile::tempdir().unwrap();
    let (state, receipt, credentials) = founder(directory.path()).await;
    let small = MemoryBudget::new(1024 * 1024, 512 * 1024).unwrap();
    assert!(matches!(
        NetworkController::new(
            state.clone(),
            receipt.clone(),
            credentials.clone(),
            directory.path().to_owned(),
            small.clone()
        ),
        Err(ControllerError::Capacity)
    ));
    assert_eq!(small.stats().used, 0);
    assert!(!directory.path().join("ROOT-ADMISSION.initialized").exists());
    let budget = MemoryBudget::new(128 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let controller = NetworkController::new(
        state,
        receipt,
        credentials,
        directory.path().to_owned(),
        budget.clone(),
    )
    .unwrap();
    assert_eq!(budget.stats().used, 64 * 1024 * 1024);
    drop(controller);
    assert_eq!(budget.stats().used, 0);
}

#[tokio::test]
async fn grant_projection_removes_revoked_credentials_and_rejects_clock_before_floor() {
    let directory = tempfile::tempdir().unwrap();
    let (state, receipt, _credentials) = founder(directory.path()).await;
    let now = unix_time().unwrap();
    let mut enrollment = genesis_enrollment(&state).unwrap();
    let registry = PeerRegistry::new(4).unwrap();
    seed_peer_registry(&state, &receipt, &enrollment, &registry, now).unwrap();
    let fingerprint = certificate_fingerprint(&receipt.certificate);
    assert_eq!(
        registry.authenticate(fingerprint).unwrap().role(),
        PeerRole::Node {
            node_id: state.node
        }
    );
    assert!(active_grants(&enrollment, &state, receipt.issued_at - 1).is_err());
    let revoked = enrollment.prepare_revoke(receipt.invitation, now).unwrap();
    enrollment.apply_committed(&revoked, 1).unwrap();
    registry
        .replace_grants(active_grants(&enrollment, &state, now).unwrap())
        .unwrap();
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    assert!(matches!(
        seed_peer_registry(&state, &receipt, &enrollment, &registry, now),
        Err(ControllerError::Enrollment(
            focal_enrollment::EnrollmentError::Revoked
        ))
    ));
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
}

#[tokio::test]
async fn delayed_enrollment_authorization_cannot_restore_a_revoked_projection() {
    let directory = tempfile::tempdir().unwrap();
    let (state, receipt, _credentials) = founder(directory.path()).await;
    let registry = PeerRegistry::new(4).unwrap();
    let authorized = node_grant(state.node, receipt.identity.principal, &state);
    let fingerprint = registry
        .register_certificate(&receipt.certificate, authorized.clone())
        .unwrap();
    // This grant was legitimately returned by an earlier authorization read.
    // A newer controller refresh wins before the awaiting join task resumes.
    registry.replace_grants(BTreeMap::new()).unwrap();
    assert_eq!(
        wait_registered_grant(&registry, &receipt, &authorized, Duration::from_millis(10)).await,
        Err(JoinFailure::OutcomeUnknown)
    );
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    let waiting = wait_registered_grant(&registry, &receipt, &authorized, Duration::from_secs(1));
    let mut waiting = std::pin::pin!(waiting);
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    // Only the controller's exact committed projection can unblock the waiter.
    registry
        .replace_grants(BTreeMap::from([(fingerprint, authorized.clone())]))
        .unwrap();
    waiting.await.unwrap();
    assert_eq!(
        registry.authenticate(fingerprint).unwrap().role(),
        authorized.role
    );
}

#[tokio::test]
async fn projection_export_pressure_clears_grants_after_committed_revocation() {
    use crate::{cluster::NoDirectoryAuthority, control_host::ControlHostConfig};
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().to_owned());
    settings.node.advertise = Some("127.0.0.1:45678".into());
    let mut network = FoundingNetwork::open(&settings).await.unwrap();
    let registry = PeerRegistry::new(4).unwrap();
    seed_peer_registry(
        &network.state,
        &network.receipt,
        network.control.enrollment().unwrap(),
        &registry,
        unix_time().unwrap(),
    )
    .unwrap();
    let fingerprint = certificate_fingerprint(&network.receipt.certificate);
    assert!(registry.authenticate(fingerprint).is_ok());
    let revoke = network
        .control
        .enrollment()
        .unwrap()
        .prepare_revoke(network.receipt.invitation, unix_time().unwrap())
        .unwrap();
    let id = ControlRequestId {
        client: [7; 16],
        sequence: 1,
    };
    network
        .control
        .submit(
            ControlRequest {
                id,
                acknowledged_through: 0,
                command: ControlCommand::Enrollment(revoke),
            },
            &NoDirectoryAuthority,
        )
        .unwrap();
    let events = network.control.drain(&NoDirectoryAuthority).unwrap();
    assert!(network.control.receipt(id).unwrap().is_some());
    let budget = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
    let (host, owner, _outgoing) = ControlHost::spawn_recovered(
        network.control,
        NoDirectoryAuthority,
        ControlHostConfig::new(network.state.genesis.root_namespace),
        budget.clone(),
        events,
    )
    .unwrap();
    // Leave admission for the small request, but no room for its state export.
    let pressure = budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used - 512,
        )
        .unwrap();
    assert!(
        observe_projection(&host, &registry)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    drop(pressure);
    let observation = observe_projection(&host, &registry).await.unwrap().unwrap();
    let ControlBootstrap::Root { enrollment, .. } = &observation.snapshot().state else {
        panic!("root snapshot");
    };
    let enrollment = EnrollmentRegistry::restore(
        enrollment,
        network.state.genesis.founder.cluster,
        EnrollmentLimits::default(),
    )
    .unwrap();
    registry
        .replace_grants(active_grants(&enrollment, &network.state, unix_time().unwrap()).unwrap())
        .unwrap();
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    drop(observation);
    host.stop().await.unwrap();
    owner.join().unwrap();
}

#[test]
fn missing_timer_or_stopped_controller_clears_existing_ingress_grants() {
    use crate::{cluster::NoDirectoryAuthority, control_host::ControlHostConfig};
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().to_owned());
    settings.node.advertise = Some("127.0.0.1:45678".into());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let no_time = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let network = runtime.block_on(FoundingNetwork::open(&settings)).unwrap();
    let registry = PeerRegistry::new(4).unwrap();
    let grant = node_grant(
        network.state.node,
        network.receipt.identity.principal,
        &network.state,
    );
    let fingerprint = registry
        .register_certificate(&network.receipt.certificate, grant.clone())
        .unwrap();
    let pool = {
        let _entered = runtime.enter();
        let limits = ControlHost::wire_limits();
        let identity = TlsIdentity::from_pkcs8(
            network.credentials.certificate_chain().to_vec(),
            network.credentials.private_key_der().to_vec(),
        );
        let tls = client_tls(
            identity,
            vec![network.state.sponsor.ca_certificate.clone()],
            &limits,
        )
        .unwrap();
        PeerConnectionPool::new(
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, limits).unwrap(),
            PeerPoolLimits::default(),
        )
        .unwrap()
    };
    let controller_budget = MemoryBudget::new(128 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let controller = NetworkController::new(
        network.state.clone(),
        network.receipt.clone(),
        network.credentials.clone(),
        directory.path().to_owned(),
        controller_budget.clone(),
    )
    .unwrap();
    let (host, owner, _outgoing) = ControlHost::spawn_recovered(
        network.control,
        NoDirectoryAuthority,
        ControlHostConfig::new(network.state.genesis.root_namespace),
        MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
        network.recovered,
    )
    .unwrap();
    assert!(matches!(
        no_time.block_on(controller.run(&pool, &host, &registry, CredentialSwap::detached())),
        Err(ControllerError::Runtime)
    ));
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    assert_eq!(controller_budget.stats().used, 0);
    assert_eq!(
        no_time.block_on(wait_registered_grant(
            &registry,
            &network.receipt,
            &grant,
            Duration::from_secs(1)
        )),
        Err(JoinFailure::OutcomeUnknown)
    );
    runtime.block_on(host.stop()).unwrap();
    owner.join().unwrap();
    registry
        .register_certificate(&network.receipt.certificate, grant)
        .unwrap();
    let controller = NetworkController::new(
        network.state,
        network.receipt,
        network.credentials,
        directory.path().to_owned(),
        controller_budget.clone(),
    )
    .unwrap();
    assert!(matches!(
        runtime.block_on(controller.run(&pool, &host, &registry, CredentialSwap::detached())),
        Err(ControllerError::Stopped)
    ));
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    assert_eq!(controller_budget.stats().used, 0);
    pool.close();
}

#[tokio::test]
async fn cancelling_controller_run_withdraws_live_and_unpolled_peer_projections() {
    use crate::{cluster::NoDirectoryAuthority, control_host::ControlHostConfig};
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().to_owned());
    settings.node.advertise = Some("127.0.0.1:45678".into());
    let network = FoundingNetwork::open(&settings).await.unwrap();
    let registry = PeerRegistry::new(4).unwrap();
    let fingerprint = certificate_fingerprint(&network.receipt.certificate);
    let limits = ControlHost::wire_limits();
    let identity = || {
        TlsIdentity::from_pkcs8(
            network.credentials.certificate_chain().to_vec(),
            network.credentials.private_key_der().to_vec(),
        )
    };
    let roots = vec![network.state.sponsor.ca_certificate.clone()];
    let listener = QuicServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        server_tls(identity(), roots.clone(), &limits).unwrap(),
        registry.clone(),
        limits.clone(),
    )
    .unwrap();
    let pool = PeerConnectionPool::new(
        QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            client_tls(identity(), roots, &limits).unwrap(),
            limits,
        )
        .unwrap(),
        PeerPoolLimits::default(),
    )
    .unwrap();
    let controller_budget = MemoryBudget::new(128 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let (host, owner, _outgoing) = ControlHost::spawn_recovered(
        network.control,
        NoDirectoryAuthority,
        ControlHostConfig::new(network.state.genesis.root_namespace),
        MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
        network.recovered,
    )
    .unwrap();
    let controller = NetworkController::new(
        network.state.clone(),
        network.receipt.clone(),
        network.credentials.clone(),
        directory.path().to_owned(),
        controller_budget.clone(),
    )
    .unwrap();
    let mut running = Box::pin(controller.run(&pool, &host, &registry, CredentialSwap::detached()));
    tokio::select! {
        result = running.as_mut() => panic!("controller terminated before cancellation: {result:?}"),
        _ = async {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let observed = host.observe_root().await.unwrap();
                    if observed.contacts().contacts.records.iter().any(|record| {
                        record.node == network.state.node
                    }) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.unwrap();
        } => {}
    }
    // The actual run published its grant and committed its contact. Cancel it
    // while preserving the listener, pool, owner, and shared registry.
    assert!(registry.authenticate(fingerprint).is_ok());
    assert!(controller_budget.stats().used > 0);
    drop(running);
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    assert_eq!(controller_budget.stats().used, 0);
    assert!(listener.local_addr().is_ok());
    assert!(!host.progress().stopped);

    // Canceling the controller withdraws its grants immediately, but an
    // admitted local journal write still owns its lock until the physical
    // control owner finishes. A later FIFO owner request settles that write
    // and drops its abandoned reply before a replacement reopens the journal.
    drop(host.observe_root().await.unwrap());
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));

    seed_peer_registry(
        &network.state,
        &network.receipt,
        &genesis_enrollment(&network.state).unwrap(),
        &registry,
        unix_time().unwrap(),
    )
    .unwrap();
    let controller = NetworkController::new(
        network.state,
        network.receipt,
        network.credentials,
        directory.path().to_owned(),
        controller_budget.clone(),
    )
    .unwrap();
    let unpolled = controller.run(&pool, &host, &registry, CredentialSwap::detached());
    assert!(registry.authenticate(fingerprint).is_ok());
    drop(unpolled);
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    assert_eq!(controller_budget.stats().used, 0);
    host.stop().await.unwrap();
    owner.join().unwrap();
    listener.close();
    pool.close();
}

#[tokio::test]
async fn contact_announcement_reaches_alternate_after_blackholed_preferred_leader_with_exact_request()
 {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().to_owned());
    settings.node.advertise = Some("127.0.0.1:45678".into());
    let network = FoundingNetwork::open(&settings).await.unwrap();
    let mut controller = NetworkController::new(
        network.state.clone(),
        network.receipt.clone(),
        network.credentials.clone(),
        directory.path().to_owned(),
        MemoryBudget::new(128 * 1024 * 1024, 64 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let limits = ControlHost::wire_limits();
    let identity = || {
        TlsIdentity::from_pkcs8(
            network.credentials.certificate_chain().to_vec(),
            network.credentials.private_key_der().to_vec(),
        )
    };
    let roots = vec![network.state.sponsor.ca_certificate.clone()];
    let peers = PeerRegistry::new(1).unwrap();
    peers
        .register_certificate(
            &network.receipt.certificate,
            node_grant(
                network.state.node,
                network.receipt.identity.principal,
                &network.state,
            ),
        )
        .unwrap();
    let server = QuicServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        server_tls(identity(), roots.clone(), &limits).unwrap(),
        peers,
        limits.clone(),
    )
    .unwrap();
    // Keeping the UDP socket open suppresses an immediate unreachable-port
    // error: this preferred endpoint really waits until its probe is cancelled.
    let blackhole = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let routes = BTreeMap::from([
        (
            99,
            PeerEndpoint {
                address: blackhole.local_addr().unwrap(),
                server_name: network.receipt.identity.server_name.clone(),
                name: None,
            },
        ),
        (
            100,
            PeerEndpoint {
                address: server.local_addr().unwrap(),
                server_name: network.receipt.identity.server_name.clone(),
                name: None,
            },
        ),
    ]);
    let connector = QuicConnector::bind(
        "127.0.0.1:0".parse().unwrap(),
        client_tls(identity(), roots, &limits).unwrap(),
        limits,
    )
    .unwrap();
    let pool = PeerConnectionPool::new(
        connector,
        PeerPoolLimits {
            max_routes: 2,
            max_connections: 2,
            max_inflight: 2,
            attempts: 1,
            // The pool timeout deliberately exceeds the entire controller round.
            // Without a per-probe bound, the alternate cannot be reached in time.
            timeout: Duration::from_secs(10),
            retry_backoff: Duration::ZERO,
            ..PeerPoolLimits::default()
        },
    )
    .unwrap();
    pool.replace_routes(1, routes.clone()).unwrap();
    controller.routes = routes;
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: network.state.genesis.root_namespace,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId(network.receipt.request),
        operation: Operation::NodeContact {
            group: network.state.genesis.root.group,
            sequence: 1,
            acknowledged_through: 0,
            expected_generation: 0,
            advertise: network.state.advertise,
            region: None,
            zone: None,
            endpoint: None,
        },
    };
    let original = postcard::to_stdvec(&request).unwrap();
    // This bounded fixture verifies routing and receipt matching only. The
    // separate contact/control tests prove actual quorum authority and replay.
    let receipt = ControlReceipt {
        request: ControlRequestId {
            client: network.receipt.identity.principal,
            sequence: 1,
        },
        request_hash: [7; 32],
        committed_index: 4,
        committed_term: 2,
        revisions: ControlRevisions::default(),
    };
    let reply = ControlReply::Committed(receipt).encode(4096).unwrap();
    let (seen, mut delivered) = tokio::sync::mpsc::channel(1);
    let principal = ParticipantId(network.receipt.identity.principal);
    let node = network.state.node;
    let serving = server.serve(move |verified: VerifiedRequest| {
        let seen = seen.clone();
        let reply = reply.clone();
        async move {
            assert_eq!(verified.peer().principal(), principal);
            assert_eq!(verified.peer().role(), PeerRole::Node { node_id: node });
            seen.try_send(verified.request().clone()).unwrap();
            verified
                .request()
                .reply(Response::Control { response: reply })
        }
    });
    tokio::pin!(serving);
    tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(5),controller.announce_remote(&pool,&request,99,1)) => {
            result.expect("blackholed preferred leader starved the reachable alternate").unwrap();
        }
        result = &mut serving => panic!("test server ended before delivery: {result:?}"),
    }
    let received = delivered
        .try_recv()
        .expect("reachable alternate was never invoked");
    assert_eq!(postcard::to_stdvec(&received).unwrap(), original);
    assert_eq!(
        check_contact_reply(ControlReply::Committed(receipt), &network.receipt, 1).unwrap(),
        ContactOutcome::Committed
    );
    assert_eq!(controller.contact_cursor, 100);
    assert!(matches!(
        delivered.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    server.close();
    serving.await.unwrap();
    pool.close();
}
