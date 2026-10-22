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

#[test]
fn admission_reopens_exact_request_and_rejects_lost_or_inconsistent_journal() {
    focal_rt::block_on(async move {
        let directory = tempfile::tempdir().unwrap();
        let (state, _, _) = founder(directory.path()).await;
        let mut admission = RootAdmission::open(
            &state,
            directory.path(),
            &focal_seal::test_keys(directory.path()).unwrap(),
        )
        .unwrap();
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
        let mut recovered = RootAdmission::open(
            &state,
            directory.path(),
            &focal_seal::test_keys(directory.path()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            postcard::to_stdvec(recovered.intent.pending.as_ref().unwrap()).unwrap(),
            postcard::to_stdvec(&pending).unwrap()
        );
        recovered.intent.completed = 9; // Corrupt the acknowledgement/sequence fence.
        recovered.save().unwrap();
        drop(recovered);
        assert!(matches!(
            RootAdmission::open(
                &state,
                directory.path(),
                &focal_seal::test_keys(directory.path()).unwrap()
            ),
            Err(ControllerError::Identity)
        ));
        std::fs::remove_dir_all(directory.path().join("cluster/root-admission")).unwrap();
        assert!(matches!(
            RootAdmission::open(
                &state,
                directory.path(),
                &focal_seal::test_keys(directory.path()).unwrap()
            ),
            Err(ControllerError::Identity)
        ));
        assert!(!directory.path().join("cluster/root-admission").exists());
    })
    .unwrap()
}

#[test]
fn controller_admission_precedes_private_journal_and_releases_on_drop() {
    focal_rt::block_on(async move {
        let directory = tempfile::tempdir().unwrap();
        let (state, receipt, credentials) = founder(directory.path()).await;
        // No room to decode the genesis registry: refused before the journal.
        let ControlBootstrap::Root { enrollment, .. } = &state.genesis.bootstrap else {
            panic!("root genesis");
        };
        let decoding = EnrollmentRegistry::restore_bound(enrollment.len()).unwrap();
        let small = MemoryBudget::new(decoding - 1, 0).unwrap();
        assert!(matches!(
            NetworkController::new(
                state.clone(),
                receipt.clone(),
                credentials.clone(),
                directory.path().to_owned(),
                focal_seal::test_keys(directory.path()).unwrap(),
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
            focal_seal::test_keys(directory.path()).unwrap(),
            budget.clone(),
        )
        .unwrap();
        // It holds the founder's genesis enrollment and one route through the
        // sponsor: what they take, never a prepayment.
        let held = budget.stats().used;
        assert!(held > 0 && held < decoding, "{held} of {decoding}");
        assert!(directory.path().join("ROOT-ADMISSION.initialized").exists());
        drop(controller);
        assert_eq!(budget.stats().used, 0);
    })
    .unwrap()
}

#[test]
fn grant_projection_removes_revoked_credentials_and_rejects_clock_before_floor() {
    focal_rt::block_on(async move {
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
    })
    .unwrap()
}

#[test]
fn delayed_enrollment_authorization_cannot_restore_a_revoked_projection() {
    focal_rt::block_on(async move {
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
            wait_registered_grant(&registry, &receipt, &authorized, Duration::from_millis(10))
                .await,
            Err(JoinFailure::OutcomeUnknown)
        );
        assert!(matches!(
            registry.authenticate(fingerprint),
            Err(AccessError::Unauthorized)
        ));
        let waiting =
            wait_registered_grant(&registry, &receipt, &authorized, Duration::from_secs(1));
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
    })
    .unwrap()
}

#[test]
fn projection_export_pressure_clears_grants_after_committed_revocation() {
    focal_rt::block_on(async move {
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
            .replace_grants(
                active_grants(&enrollment, &network.state, unix_time().unwrap()).unwrap(),
            )
            .unwrap();
        assert!(matches!(
            registry.authenticate(fingerprint),
            Err(AccessError::Unauthorized)
        ));
        drop(observation);
        host.stop().await.unwrap();
        owner.join().unwrap();
    })
    .unwrap()
}

#[test]
fn a_refresh_is_charged_what_it_decodes_and_keeps_and_refused_without_it() {
    focal_rt::block_on(async move {
        use crate::{cluster::NoDirectoryAuthority, control_host::ControlHostConfig};
        let directory = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(directory.path().to_owned());
        settings.node.advertise = Some("127.0.0.1:45678".into());
        let network = FoundingNetwork::open(&settings).await.unwrap();
        let registry = PeerRegistry::new(4).unwrap();
        let limits = ControlHost::wire_limits();
        let pool = PeerConnectionPool::new(
            QuicConnector::bind_standalone(
                "127.0.0.1:0".parse().unwrap(),
                client_tls(
                    TlsIdentity::from_pkcs8(
                        network.credentials.certificate_chain().to_vec(),
                        network.credentials.private_key_der().to_vec(),
                    ),
                    vec![network.state.sponsor.ca_certificate.clone()],
                    &limits,
                    focal_wire::net::Sender::Node,
                )
                .unwrap(),
                limits,
            )
            .unwrap(),
            PeerPoolLimits::default(),
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
        let budget = MemoryBudget::new(128 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
        let mut controller = NetworkController::new(
            network.state.clone(),
            network.receipt.clone(),
            network.credentials.clone(),
            directory.path().to_owned(),
            focal_seal::test_keys(directory.path()).unwrap(),
            budget.clone(),
        )
        .unwrap();
        let founded = budget.stats().used;
        let observation = host.observe_root().await.unwrap();
        assert!(observation.enrollment_restore() > 0);
        let fingerprint = certificate_fingerprint(&network.receipt.certificate);
        // No room to decode what was observed: refused, nothing installed.
        let pressure = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                budget.stats().limit - founded - observation.enrollment_restore() + 1,
            )
            .unwrap();
        assert!(matches!(
            controller.refresh(&observation, &pool, &registry, unix_time().unwrap()),
            Err(ControllerError::Capacity)
        ));
        assert!(registry.authenticate(fingerprint).is_err());
        assert_eq!(budget.stats().used, founded + pressure.bytes());
        drop(pressure);
        // With room, the projection is installed and kept charged, and the
        // registry decoded is the observation's, charged while it is held:
        // every reader of the observation reads that one decode.
        controller
            .refresh(&observation, &pool, &registry, unix_time().unwrap())
            .unwrap();
        assert!(registry.authenticate(fingerprint).is_ok());
        let refreshed = budget.stats().used;
        let decoded = observation.enrollment_restore();
        assert!(
            refreshed > founded + decoded,
            "{refreshed} of {founded} and {decoded} decoded"
        );
        // Refreshing again decodes nothing and replaces the projection's
        // charge: nothing grows.
        for _ in 0..8 {
            controller
                .refresh(&observation, &pool, &registry, unix_time().unwrap())
                .unwrap();
            assert_eq!(budget.stats().used, refreshed);
        }
        drop(controller);
        assert_eq!(
            budget.stats().used,
            decoded,
            "the decode is the observation's"
        );
        drop(observation);
        assert_eq!(budget.stats().used, 0);
        host.stop().await.unwrap();
        owner.join().unwrap();
    })
    .unwrap()
}

#[test]
fn off_a_shard_or_stopped_controller_clears_existing_ingress_grants() {
    use crate::{cluster::NoDirectoryAuthority, control_host::ControlHostConfig};
    use futures_util::FutureExt;
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().to_owned());
    settings.node.advertise = Some("127.0.0.1:45678".into());
    let opening = settings.clone();
    let network = focal_rt::block_on(async move { FoundingNetwork::open(&opening).await })
        .unwrap()
        .unwrap();
    let registry = PeerRegistry::new(4).unwrap();
    let grant = node_grant(
        network.state.node,
        network.receipt.identity.principal,
        &network.state,
    );
    let fingerprint = registry
        .register_certificate(&network.receipt.certificate, grant.clone())
        .unwrap();
    let limits = ControlHost::wire_limits();
    let tls = || {
        client_tls(
            TlsIdentity::from_pkcs8(
                network.credentials.certificate_chain().to_vec(),
                network.credentials.private_key_der().to_vec(),
            ),
            vec![network.state.sponsor.ca_certificate.clone()],
            &limits,
            focal_wire::net::Sender::Node,
        )
        .unwrap()
    };
    // A pool bound on a shard that has since ended: the controller off a
    // shard refuses before it touches it.
    let (first_tls, first_limits) = (tls(), limits.clone());
    let ended = focal_rt::block_on(async move {
        let budget = focal_wire::endpoint_budget(&first_limits, None).unwrap();
        PeerConnectionPool::new(
            QuicConnector::bind(
                "127.0.0.1:0".parse().unwrap(),
                first_tls,
                first_limits,
                budget,
            )
            .unwrap(),
            PeerPoolLimits::default(),
        )
        .unwrap()
    })
    .unwrap();
    let controller_budget = MemoryBudget::new(128 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let controller = NetworkController::new(
        network.state.clone(),
        network.receipt.clone(),
        network.credentials.clone(),
        directory.path().to_owned(),
        focal_seal::test_keys(directory.path()).unwrap(),
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
    // Off a shard no wait can be kept: the controller refuses at once, and
    // what it would have served is withdrawn.
    let swap = CredentialSwap::detached().unwrap();
    assert!(matches!(
        controller
            .run(&ended, &host, &registry, swap)
            .now_or_never(),
        Some(Err(ControllerError::Runtime))
    ));
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    assert_eq!(controller_budget.stats().used, 0);
    assert_eq!(
        wait_registered_grant(&registry, &network.receipt, &grant, Duration::from_secs(1))
            .now_or_never(),
        Some(Err(JoinFailure::OutcomeUnknown))
    );
    drop(ended);
    registry
        .register_certificate(&network.receipt.certificate, grant)
        .unwrap();
    let (second_tls, second_registry) = (tls(), registry.clone());
    let controller = NetworkController::new(
        network.state,
        network.receipt,
        network.credentials,
        directory.path().to_owned(),
        focal_seal::test_keys(directory.path()).unwrap(),
        controller_budget.clone(),
    )
    .unwrap();
    // A controller whose host has stopped ends Stopped, on a shard, and
    // withdraws what it served as well.
    let ran = focal_rt::block_on(async move {
        host.stop().await.unwrap();
        owner.join().unwrap();
        let budget = focal_wire::endpoint_budget(&limits, None).unwrap();
        let pool = PeerConnectionPool::new(
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), second_tls, limits, budget)
                .unwrap(),
            PeerPoolLimits::default(),
        )
        .unwrap();
        let swap = CredentialSwap::detached().unwrap();
        let ran = controller.run(&pool, &host, &second_registry, swap).await;
        pool.close();
        ran
    })
    .unwrap();
    assert!(matches!(ran, Err(ControllerError::Stopped)));
    assert!(matches!(
        registry.authenticate(fingerprint),
        Err(AccessError::Unauthorized)
    ));
    assert_eq!(controller_budget.stats().used, 0);
}

#[test]
fn cancelling_controller_run_withdraws_live_and_unpolled_peer_projections() {
    focal_rt::block_on(async move {
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
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
            server_tls(identity(), roots.clone(), &limits).unwrap(),
            registry.clone(),
            limits.clone(),
            MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let pool = PeerConnectionPool::new(
            QuicConnector::bind_standalone(
                "127.0.0.1:0".parse().unwrap(),
                client_tls(identity(), roots, &limits, focal_wire::net::Sender::Node).unwrap(),
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
            focal_seal::test_keys(directory.path()).unwrap(),
            controller_budget.clone(),
        )
        .unwrap();
        let mut running =
            Box::pin(controller.run(&pool, &host, &registry, CredentialSwap::detached().unwrap()));
        match hyper_rt::combine::race2(running.as_mut(), async {
            // Charged to the root owner's periods (27 §3.1 P8).
            crate::test_waits::charged(
                || vec![host.periods()],
                Duration::from_secs(5),
                crate::test_waits::CONTROL_TICK,
                async {
                    loop {
                        let observed = host.observe_root().await.unwrap();
                        if observed
                            .contacts()
                            .contacts
                            .records
                            .iter()
                            .any(|record| record.node == network.state.node)
                        {
                            break;
                        }
                        focal_rt::time::sleep(Duration::from_millis(10))
                            .await
                            .unwrap();
                    }
                },
            )
            .await
            .expect("the controller committed its contact");
        })
        .await
        {
            hyper_rt::combine::Either::First(result) => {
                panic!("controller terminated before cancellation: {result:?}")
            }
            hyper_rt::combine::Either::Second(_) => {}
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
            focal_seal::test_keys(directory.path()).unwrap(),
            controller_budget.clone(),
        )
        .unwrap();
        let unpolled = controller.run(&pool, &host, &registry, CredentialSwap::detached().unwrap());
        assert!(registry.authenticate(fingerprint).is_ok());
        drop(unpolled);
        assert!(matches!(
            registry.authenticate(fingerprint),
            Err(AccessError::Unauthorized)
        ));
        assert_eq!(controller_budget.stats().used, 0);
        host.stop().await.unwrap();
        owner.join().unwrap();
        listener.close().unwrap();
        pool.close();
    })
    .unwrap()
}

#[test]
fn contact_announcement_reaches_alternate_after_blackholed_preferred_leader_with_exact_request() {
    focal_rt::block_on(async move {
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
            focal_seal::test_keys(directory.path()).unwrap(),
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
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
            server_tls(identity(), roots.clone(), &limits).unwrap(),
            peers,
            limits.clone(),
            MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
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
        let connector = QuicConnector::bind_standalone(
            "127.0.0.1:0".parse().unwrap(),
            client_tls(identity(), roots, &limits, focal_wire::net::Sender::Node).unwrap(),
            limits,
        )
        .unwrap();
        // The pool timeout deliberately exceeds the entire controller round.
        // Without a per-probe bound, the alternate cannot be reached in time: a
        // controller that waited it out on the blackholed leader reaches the
        // alternate no sooner, which is what the announce is held to below.
        let pool_timeout = Duration::from_secs(10);
        let pool = PeerConnectionPool::new(
            connector,
            PeerPoolLimits {
                max_routes: 2,
                max_connections: 2,
                max_inflight: 2,
                attempts: 1,
                timeout: pool_timeout,
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
        let (seen, mut delivered) = hyper_rt::sync::channel(1).unwrap();
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
        let mut serving = std::pin::pin!(serving);
        match hyper_rt::combine::race2(
            focal_rt::time::timeout(
                pool_timeout,
                controller.announce_remote(&pool, &request, 99, 1),
            ),
            &mut serving,
        )
        .await
        {
            hyper_rt::combine::Either::First(result) => {
                result
                    .expect("blackholed preferred leader starved the reachable alternate")
                    .unwrap();
            }
            hyper_rt::combine::Either::Second(result) => {
                panic!("test server ended before delivery: {result:?}")
            }
        }
        let received = delivered
            .try_recv()
            .expect("the delivery channel was closed")
            .expect("reachable alternate was never invoked");
        assert_eq!(postcard::to_stdvec(&received).unwrap(), original);
        assert_eq!(
            check_contact_reply(ControlReply::Committed(receipt), &network.receipt, 1).unwrap(),
            ContactOutcome::Committed
        );
        assert_eq!(controller.contact_cursor, 100);
        assert!(matches!(delivered.try_recv(), Ok(None)));
        server.close().unwrap();
        serving.await.unwrap();
        pool.close();
    })
    .unwrap()
}

/// The rule that stops a controller whose credential the committed registry
/// has retired (24 §11) judges the certificate held by what the registry
/// knows of it: listed and authorizing, listed and revoked, replaced and in
/// its grace, replaced and past it, or issued after the registry was observed.
#[test]
fn a_credential_is_retired_by_what_the_registry_knows_of_it_not_by_its_absence() {
    use focal_enrollment::{
        BootstrapAuthority, EnrollmentRole, InviteOptions, JoinKey, JoinPreparation,
        RenewPreparation,
    };
    let dir = tempfile::tempdir().unwrap();
    let cluster = [7; 16];
    let now = unix_time().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        dir.path().join("authority"),
        &focal_seal::test_keys_beside(dir.path().join("authority")).unwrap(),
        cluster,
        vec!["localhost".into()],
        now,
    )
    .unwrap();
    let limits = EnrollmentLimits::default();
    let mut registry = EnrollmentRegistry::new(
        cluster,
        authority.ca_certificate().to_vec(),
        2,
        limits.clone(),
    )
    .unwrap();
    let draft = registry
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: now + 600,
            },
            now,
        )
        .unwrap();
    registry
        .apply_committed(draft.command(), registry.applied_index() + 1)
        .unwrap();
    let invitation = draft.release(&registry).unwrap();
    let key = JoinKey::open_or_create(
        dir.path().join("node"),
        &focal_seal::test_keys_beside(dir.path().join("node")).unwrap(),
        cluster,
    )
    .unwrap();
    // A join request is only issued over a connection the invitation trusts.
    let mut client_config = invitation.client_config().unwrap();
    let mut client = rustls::ClientConnection::new(
        &mut client_config,
        rustls::pki_types::ServerName::try_from(invitation.trust().server_name.clone()).unwrap(),
    )
    .unwrap();
    let mut server_config = authority.server_identity().server_config().unwrap();
    let mut server = rustls::ServerConnection::new(&server_config).unwrap();
    let mut request = None;
    for _ in 0..32 {
        if client.wants_write() {
            let mut bytes = Vec::new();
            client.write_tls(&mut bytes).unwrap();
            server.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            server.process_new_packets(&mut server_config).unwrap();
        }
        if server.wants_write() {
            let mut bytes = Vec::new();
            server.write_tls(&mut bytes).unwrap();
            client.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            client.process_new_packets(&mut client_config).unwrap();
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            request = Some(invitation.request_after_tls(&client, &key, now).unwrap());
            break;
        }
    }
    let request = request.expect("the enrollment TLS handshake finishes");
    let JoinPreparation::Commit(joined) = registry.prepare_join(&authority, &request, now).unwrap()
    else {
        panic!("a first join commits")
    };
    registry
        .apply_committed(&joined, registry.applied_index() + 1)
        .unwrap();
    let first = registry.release(&request, now).unwrap();
    let node = first.identity.node_id.unwrap();
    let material = key
        .complete(&first, authority.issuers().unwrap().trusted(), now)
        .unwrap();
    // The certificate listed, authorizing.
    assert!(!credential_retired(&registry, node, &first, now));
    // A registry that does not list the node decides nothing.
    let fresh = EnrollmentRegistry::new(
        cluster,
        authority.ca_certificate().to_vec(),
        2,
        limits.clone(),
    )
    .unwrap();
    assert!(!credential_retired(&fresh, node, &first, now));
    // The registry as the holder last observed it, before its renewal commits.
    let observed =
        EnrollmentRegistry::restore(&registry.checkpoint().unwrap(), cluster, limits).unwrap();
    let renewal = material.renewal_request(&key, &first).unwrap();
    let at = now + 10;
    let grace = 30;
    let RenewPreparation::Commit(renew) = registry
        .prepare_renew(&authority, &renewal, at, grace)
        .unwrap()
    else {
        panic!("a first renewal commits")
    };
    registry
        .apply_committed(&renew, registry.applied_index() + 1)
        .unwrap();
    let renewed = registry.release_renewal(&renewal, at).unwrap();
    assert!(renewed.revision > first.revision);
    // The holder presents the renewal before it observes the commit: ahead of
    // its observation, not retired.
    assert!(!credential_retired(&observed, node, &renewed, at));
    // Observed, the renewal is the certificate listed.
    assert!(!credential_retired(&registry, node, &renewed, at));
    // The certificate the renewal replaced holds through its grace, not past it.
    assert!(!credential_retired(
        &registry,
        node,
        &first,
        at + grace as i64 - 1
    ));
    assert!(credential_retired(
        &registry,
        node,
        &first,
        at + grace as i64
    ));
    // A revocation retires the certificate listed and the replaced one at once.
    let revoked = registry.prepare_revoke(first.invitation, at).unwrap();
    registry
        .apply_committed(&revoked, registry.applied_index() + 1)
        .unwrap();
    assert!(credential_retired(&registry, node, &renewed, at));
    assert!(credential_retired(&registry, node, &first, at));
}
