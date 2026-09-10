use super::*;
use crate::{
    cluster::{InviteIntent, NoDirectoryAuthority},
    control_host::*,
    embedded::NodeIdentity,
    network_state::{root_group, root_namespace},
    quorum_enrollment::*,
};
use focal_consensus::NodeConfig;
use focal_directory::{RootConfig, RootDirectory};
use focal_enrollment::*;
use focal_model::{RootCommandId, SessionId, TenantId};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

const CLUSTER: [u8; 16] = [191; 16];
fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}
fn now() -> i64 {
    unix_time().unwrap()
}
fn node_identity() -> NodeIdentity {
    NodeIdentity {
        schema: 1,
        cluster: CLUSTER,
        node: 1,
        ledger: LedgerId {
            tenant: TenantId::from_u128(191),
            session: SessionId::from_u128(192),
        },
        issuer: ParticipantId::from_u128(193),
        worker: ParticipantId::from_u128(194),
        evaluator: ParticipantId::from_u128(195),
        root: RootCommandId::from_u128(196),
    }
}
fn tls(material: &CredentialMaterial) -> TlsIdentity {
    TlsIdentity::from_pkcs8(
        material.certificate_chain().to_vec(),
        material.private_key_der().to_vec(),
    )
}
fn control_reply(bytes: &[u8]) -> ControlReply {
    ControlReply::decode(bytes, ControlHost::wire_limits().max_frame_bytes as usize).unwrap()
}
fn rpc_packet(pin: &FounderControlAuthority, rpc: ControlRpc) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: pin.namespace(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(99),
        operation: Operation::EnrollmentControl {
            group: pin.root.group,
            genesis: pin.root.genesis,
            request: rpc.encode(MAX_ENROLLMENT_CONTROL_REQUEST_BYTES).unwrap(),
        },
    }
}
fn runtime(namespace: LedgerId) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(999),
        tenants: BTreeSet::from([namespace.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap()
}
async fn state(adapter: &NetworkEnrollmentControl<'_>) -> ControlSnapshot {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match adapter.read_state(RequestId::from_u128(7)).await {
                Ok(state) => return state,
                Err(error) if retryable(error) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => panic!("root state failed: {error:?}"),
            }
        }
    })
    .await
    .unwrap()
}
fn registry(state: &ControlSnapshot) -> EnrollmentRegistry {
    let ControlBootstrap::Root { enrollment, .. } = &state.state else {
        panic!("root");
    };
    EnrollmentRegistry::restore(enrollment, CLUSTER, EnrollmentLimits::default()).unwrap()
}
struct Running {
    host: ControlHost,
    owner: ControlOwner,
    server: Arc<QuicServer>,
    serving: tokio::task::JoinHandle<Result<(), WireError>>,
    pool: Arc<PeerConnectionPool>,
    sending: tokio::task::JoinHandle<()>,
}

fn drain_seed_replicas(replicas: &mut [(ControlReplica, MemoryBudget)]) {
    for _ in 0..16 {
        let mut messages = Vec::new();
        for (replica, _) in replicas.iter_mut() {
            messages.extend(replica.drain(&NoDirectoryAuthority).unwrap().messages);
        }
        if messages.is_empty() {
            break;
        }
        for message in messages {
            let target = replicas
                .iter_mut()
                .find(|(replica, _)| replica.status().node_id == message.to)
                .unwrap();
            target.0.step(message).unwrap();
        }
    }
}

fn commit_seed_enrollment(
    replicas: &mut [(ControlReplica, MemoryBudget)],
    sequence: u64,
    command: EnrollmentCommand,
) {
    let id = ControlRequestId {
        client: [190; 16],
        sequence,
    };
    replicas[0]
        .0
        .submit(
            ControlRequest {
                id,
                acknowledged_through: sequence - 1,
                command: ControlCommand::Enrollment(command),
            },
            &NoDirectoryAuthority,
        )
        .unwrap();
    drain_seed_replicas(replicas);
    let receipt = replicas[0].0.receipt(id).unwrap().unwrap();
    for (replica, _) in replicas {
        assert_eq!(replica.receipt(id).unwrap(), Some(receipt));
    }
}

fn pinned_join_request(
    authority: &BootstrapAuthority,
    invitation: &Invitation,
    key: &JoinKey,
) -> JoinRequest {
    let mut client = rustls::ClientConnection::new(
        Arc::new(invitation.client_config().unwrap()),
        rustls::pki_types::ServerName::try_from(invitation.trust().server_name.clone()).unwrap(),
    )
    .unwrap();
    let mut server = rustls::ServerConnection::new(Arc::new(
        authority.server_identity().server_config().unwrap(),
    ))
    .unwrap();
    for _ in 0..32 {
        if client.wants_write() {
            let mut bytes = Vec::new();
            client.write_tls(&mut bytes).unwrap();
            server.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            server.process_new_packets().unwrap();
        }
        if server.wants_write() {
            let mut bytes = Vec::new();
            server.write_tls(&mut bytes).unwrap();
            client.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            client.process_new_packets().unwrap();
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            return invitation.request_after_tls(&client, key, now()).unwrap();
        }
    }
    panic!("seed enrollment TLS handshake did not finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn founder_enrollment_follows_remote_quorum_leaders_and_rechecks_genesis_pin_and_revocation()
{
    let disk = tempfile::tempdir().unwrap();
    let ca_path = disk.path().join("ca");
    let authority = BootstrapAuthority::open_or_create(
        &ca_path,
        CLUSTER,
        vec!["root.focal.test".into()],
        now(),
    )
    .unwrap();
    let roots = vec![authority.ca_certificate().to_vec()];
    let founder = node_identity();
    let namespace = root_namespace(&founder);
    let root = RootDirectory::new(
        focal_directory::ClusterId(CLUSTER),
        RootConfig::default(),
        budget(),
    )
    .unwrap();
    let founder_key = JoinKey::open_or_create(disk.path().join("key1"), CLUSTER).unwrap();
    let draft = FoundingEnrollmentDraft::open_or_create(
        disk.path().join("draft1"),
        &authority,
        &founder_key,
        founder.node,
        founder.issuer.0,
        EnrollmentLimits::default(),
        now(),
    )
    .unwrap();
    let bootstrap = ControlBootstrap::root(&root, draft.registry()).unwrap();
    let mut materials = vec![
        founder_key
            .complete(draft.receipt(), authority.ca_certificate(), now())
            .unwrap(),
    ];
    let mut receipts = vec![draft.receipt().clone()];
    let group = root_group(CLUSTER);
    let identity = bootstrap
        .identity(&ControlOptions::new(NodeConfig::single(1, CLUSTER, group)))
        .unwrap();
    let genesis = NetworkGenesis {
        founder,
        root: identity,
        root_namespace: namespace,
        bootstrap: bootstrap.clone(),
    };
    let pin = FounderControlAuthority::from_genesis(&genesis).unwrap();
    // Keep immutable genesis pinned to the one founder, then commit every
    // additional peer enrollment in the same actual three-voter root log.
    // The trusted setup pump is replaced by authenticated TLS below; transport
    // grants never substitute for these shared durable registry decisions.
    let mut seeded = Vec::new();
    for node in 1..=3 {
        let allowance = budget();
        let replica = ControlReplica::open(
            ControlOptions::new(NodeConfig::joining(
                node,
                CLUSTER,
                group,
                vec![1, 2, 3],
                vec![],
            )),
            bootstrap.clone(),
            allowance.clone(),
            disk.path().join(format!("wal{node}")),
        )
        .unwrap();
        seeded.push((replica, allowance));
    }
    seeded[0].0.campaign().unwrap();
    drain_seed_replicas(&mut seeded);
    for node in 2..=3u64 {
        let key = JoinKey::open_or_create(disk.path().join(format!("key{node}")), CLUSTER).unwrap();
        let draft = seeded[0]
            .0
            .enrollment()
            .unwrap()
            .prepare_invitation(
                &authority,
                InviteOptions {
                    endpoint: "127.0.0.1:7443".into(),
                    server_name: "root.focal.test".into(),
                    role: EnrollmentRole::Node,
                    expires_at: now() + 600,
                },
                now(),
            )
            .unwrap();
        commit_seed_enrollment(&mut seeded, (node - 2) * 2 + 1, draft.command().clone());
        let invitation = draft.release(seeded[0].0.enrollment().unwrap()).unwrap();
        let request = pinned_join_request(&authority, &invitation, &key);
        let JoinPreparation::Commit(command) = seeded[0]
            .0
            .enrollment()
            .unwrap()
            .prepare_join(&authority, &request, now())
            .unwrap()
        else {
            panic!("expected a new shared enrollment");
        };
        commit_seed_enrollment(&mut seeded, (node - 2) * 2 + 2, command);
        let receipt = seeded[0]
            .0
            .enrollment()
            .unwrap()
            .release(&request, now())
            .unwrap();
        assert_eq!(receipt.identity.node_id, Some(node));
        materials.push(
            key.complete(&receipt, authority.ca_certificate(), now())
                .unwrap(),
        );
        receipts.push(receipt);
    }
    for (replica, _) in &seeded {
        assert_eq!(replica.identity(), identity);
        assert_eq!(replica.enrollment().unwrap().revision(), 5);
        for receipt in &receipts {
            replica
                .enrollment()
                .unwrap()
                .authorize_certificate(&receipt.certificate, now())
                .unwrap();
        }
    }
    let configuration = seeded[0].0.configuration();
    seeded[0]
        .0
        .transfer(&ControlTransfer {
            expected_configuration_index: configuration.configuration_index,
            expected: configuration.configuration,
            target: 2,
        })
        .unwrap();
    drain_seed_replicas(&mut seeded);
    for (replica, _) in &seeded {
        assert_eq!(replica.status().leader_id, 2);
    }
    let peers = PeerRegistry::new(8).unwrap();
    for receipt in &receipts {
        peers
            .register_certificate(
                &receipt.certificate,
                PeerGrant {
                    principal: ParticipantId(receipt.identity.principal),
                    tenants: BTreeSet::from([namespace.tenant]),
                    role: PeerRole::Node {
                        node_id: receipt.identity.node_id.unwrap(),
                    },
                },
            )
            .unwrap();
    }
    let founder_peer = peers
        .authenticate(certificate_fingerprint(&receipts[0].certificate))
        .unwrap();
    let mut pending = Vec::new();
    let mut routes = BTreeMap::new();
    let limits = WireLimits {
        request_timeout: Duration::from_secs(2),
        ..ControlHost::wire_limits()
    };
    for (index, (material, (replica, allowance))) in materials.iter().zip(seeded).enumerate() {
        let node = index as u64 + 1;
        let mut config = ControlHostConfig::new(namespace);
        config.enrollment_authority = Some(pin.clone());
        config.tick = Duration::from_millis(50);
        config.request_timeout = Duration::from_secs(1);
        let (host, owner, channel) =
            ControlHost::spawn(replica, NoDirectoryAuthority, config, allowance).unwrap();
        let server = Arc::new(
            QuicServer::bind(
                "127.0.0.1:0".parse().unwrap(),
                server_tls(tls(material), roots.clone(), &limits).unwrap(),
                peers.clone(),
                limits.clone(),
            )
            .unwrap(),
        );
        routes.insert(
            node,
            PeerEndpoint {
                address: server.local_addr().unwrap(),
                server_name: receipts[index].identity.server_name.clone(),
            },
        );
        let serving_server = server.clone();
        let handler = host.clone();
        let serving = tokio::spawn(async move { serving_server.serve(handler).await });
        let connector = QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            client_tls(tls(material), roots.clone(), &limits).unwrap(),
            limits.clone(),
        )
        .unwrap();
        let pool = Arc::new(
            PeerConnectionPool::new(
                connector,
                PeerPoolLimits {
                    max_routes: 16,
                    max_connections: 3,
                    max_inflight: 12,
                    attempts: 1,
                    timeout: Duration::from_secs(2),
                    ..PeerPoolLimits::default()
                },
            )
            .unwrap(),
        );
        pending.push((host, owner, channel, server, serving, pool));
    }
    let mut replicas = Vec::new();
    for (host, owner, mut channel, server, serving, pool) in pending {
        pool.replace_routes(1, routes.clone()).unwrap();
        let sending_pool = pool.clone();
        let sending = tokio::spawn(async move {
            while let Some(frame) = channel.recv().await {
                let _ = sending_pool.send(frame.target, &frame.request).await;
                drop(frame);
            }
        });
        replicas.push(Running {
            host,
            owner,
            server,
            serving,
            pool,
            sending,
        });
    }
    // The trusted setup transferred the established seed leader to node 2;
    // bypassing its active lease with another campaign would race node 1.
    let allowance = budget();
    let adapter = NetworkEnrollmentControl::new(
        &replicas[0].pool,
        &replicas[0].host,
        pin.clone(),
        founder_peer.clone(),
        RouteEpoch(1),
        &allowance,
    )
    .unwrap();
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let mut missing = Box::pin(adapter.read_state(RequestId::from_u128(200)));
                assert!(matches!(
                    std::future::Future::poll(
                        missing.as_mut(),
                        &mut std::task::Context::from_waker(std::task::Waker::noop())
                    ),
                    std::task::Poll::Ready(Err(ControlFailure::Unavailable))
                ));
                let no_timer = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                assert!(matches!(
                    no_timer.block_on(adapter.read_state(RequestId::from_u128(201))),
                    Err(ControlFailure::Unavailable)
                ));
            })
            .join()
            .unwrap();
    });
    let initial = state(&adapter).await;
    assert_eq!(initial.identity, identity);
    assert_eq!(registry(&initial).revision(), 5);
    assert_eq!(replicas[1].host.progress().leader, 2);
    assert_ne!(replicas[0].host.progress().leader, 1);
    assert!(replicas[0].pool.stats().delivered > 0);
    let packet = rpc_packet(&pin, ControlRpc::Read(ControlRead::State));
    assert_eq!(postcard::to_allocvec(&packet.operation).unwrap()[0], 12);
    assert_eq!(packet.operation.registered_tag(), 13);
    // A committed Node enrollment and TLS connection still cannot use the
    // founder's sequence authority, even for the public State selector.
    assert_eq!(
        control_reply(
            &replicas[2]
                .pool
                .send_enrollment_control(2, &packet)
                .await
                .unwrap()
        ),
        ControlReply::Rejected(ControlFailure::Unauthorized)
    );
    let mut bad_genesis = packet.clone();
    if let Operation::EnrollmentControl { genesis, .. } = &mut bad_genesis.operation {
        *genesis = [1; 32];
    }
    assert_eq!(
        control_reply(
            &replicas[0]
                .pool
                .send_enrollment_control(2, &bad_genesis)
                .await
                .unwrap()
        ),
        ControlReply::Rejected(ControlFailure::WrongOwner)
    );
    for query in [
        ControlRead::Membership,
        ControlRead::Contacts,
        ControlRead::Receipt(ControlRequestId {
            client: pin.signer.0,
            sequence: 1,
        }),
    ] {
        assert_eq!(
            control_reply(
                &replicas[0]
                    .pool
                    .send_enrollment_control(2, &rpc_packet(&pin, ControlRpc::Read(query)))
                    .await
                    .unwrap()
            ),
            ControlReply::Rejected(ControlFailure::Unauthorized)
        );
    }
    let arbitrary = ControlRequest {
        id: ControlRequestId {
            client: pin.signer.0,
            sequence: 1,
        },
        acknowledged_through: 0,
        command: ControlCommand::Root(focal_directory::RootCommand {
            expected_revision: 0,
            operation: focal_directory::RootOperation::RegisterRegion {
                region: focal_directory::RegionRecord {
                    id: focal_directory::RegionId::from_u128(1),
                    label: "forged".into(),
                    authority_epoch: 1,
                },
                expected_epoch: None,
            },
        }),
    };
    assert_eq!(
        control_reply(
            &replicas[0]
                .pool
                .send_enrollment_control(2, &rpc_packet(&pin, ControlRpc::Submit(arbitrary)))
                .await
                .unwrap()
        ),
        ControlReply::Rejected(ControlFailure::Unauthorized)
    );
    let draft = registry(&initial)
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:7443".into(),
                server_name: "root.focal.test".into(),
                role: EnrollmentRole::Client,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    let forged = ControlRequest {
        id: ControlRequestId {
            client: [5; 16],
            sequence: 1,
        },
        acknowledged_through: 0,
        command: ControlCommand::Enrollment(draft.command().clone()),
    };
    assert_eq!(
        control_reply(
            &replicas[0]
                .pool
                .send_enrollment_control(2, &rpc_packet(&pin, ControlRpc::Submit(forged)))
                .await
                .unwrap()
        ),
        ControlReply::Rejected(ControlFailure::Unauthorized)
    );
    let mut wrong_scope = packet.clone();
    wrong_scope.ledger.tenant = TenantId::from_u128(999);
    assert_eq!(
        replicas[0]
            .pool
            .send_enrollment_control(2, &wrong_scope)
            .await,
        Err(PeerSendError::Rejected(AccessError::Unauthorized))
    );
    assert!(
        verify_request(
            AuthenticatedPeer::local(PeerGrant {
                principal: pin.principal,
                tenants: BTreeSet::from([namespace.tenant]),
                role: PeerRole::Node { node_id: 1 }
            })
            .unwrap(),
            packet,
            &limits
        )
        .is_err()
    );
    let config = QuorumEnrollmentConfig::new(
        identity,
        pin.signer_principal(),
        "root.focal.test".into(),
        BTreeSet::from([namespace.tenant]),
    );
    let staging = disk.path().join("signer");
    let (signer, driver) =
        QuorumEnrollmentHost::create(authority, &staging, config.clone(), allowance.clone())
            .unwrap();
    let intent = InviteIntent {
        endpoint: "127.0.0.1:7443".into(),
        role: EnrollmentRole::Client,
        lifetime_seconds: 600,
    };
    let (result, token) = tokio::join!(driver.run(&adapter), async {
        let invitation = signer
            .invite(RequestId::from_u128(41), intent.clone())
            .await
            .unwrap();
        assert_eq!(registry(&state(&adapter).await).revision(), 6);
        let token = invitation.expose_token().unwrap();
        let operator = runtime(namespace);
        let ControlReadResult::Configuration(configuration) = replicas[1]
            .host
            .read(
                operator.clone(),
                RequestId::from_u128(50),
                ControlRead::Configuration,
            )
            .await
            .unwrap()
        else {
            panic!("configuration");
        };
        replicas[1]
            .host
            .transfer(
                operator,
                RequestId::from_u128(51),
                ControlTransfer {
                    expected_configuration_index: configuration.configuration_index,
                    expected: configuration.configuration,
                    target: 3,
                },
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while replicas[2].host.progress().leader != 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while replicas[0].host.progress().leader != 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let blackhole = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let mut stale = routes.clone();
        stale.get_mut(&2).unwrap().address = blackhole.local_addr().unwrap();
        replicas[0].pool.replace_routes(2, stale).unwrap();
        let cursor = adapter.route_cursor.load(Ordering::Relaxed);
        state(&adapter).await;
        assert_eq!(
            adapter.route_cursor.load(Ordering::Relaxed),
            cursor,
            "known installed leader should bypass dead earlier routes entirely"
        );
        replicas[0].pool.replace_routes(3, routes.clone()).unwrap();
        signer
            .invite(RequestId::from_u128(42), intent.clone())
            .await
            .unwrap();
        assert_eq!(registry(&state(&adapter).await).revision(), 7);
        signer.stop().await.unwrap();
        token
    });
    result.unwrap();
    // The private sequence journal survives signer restart on a follower while
    // the public decisions remain in the replicated root on the new leader.
    let authority = BootstrapAuthority::open_or_create(
        &ca_path,
        CLUSTER,
        vec!["root.focal.test".into()],
        now(),
    )
    .unwrap();
    let (signer, driver) =
        QuorumEnrollmentHost::open(authority, &staging, config, allowance.clone()).unwrap();
    let (result, ()) = tokio::join!(driver.run(&adapter), async {
        assert_eq!(
            signer
                .invite(RequestId::from_u128(41), intent.clone())
                .await
                .unwrap()
                .expose_token()
                .unwrap(),
            token
        );
        // A tenant is admitted once under the founder authority; a retry
        // reads as done, and every certificate's grant names it from then on
        // without a restart (doc 24 §16).
        signer.admit_tenant([9; 16]).await.unwrap();
        let view = state(&adapter).await;
        assert_eq!(registry(&view).revision(), 8);
        assert!(registry(&view).admits_tenant([9; 16]));
        signer.admit_tenant([9; 16]).await.unwrap();
        assert_eq!(registry(&state(&adapter).await).revision(), 8);
        assert!(matches!(
            signer.admit_tenant([0; 16]).await,
            Err(QuorumEnrollmentError::Enrollment(EnrollmentError::Invalid))
        ));
        let granted = signer
            .authorize_certificate(receipts[1].certificate.clone())
            .await
            .unwrap();
        assert_eq!(
            granted.tenants,
            BTreeSet::from([namespace.tenant, TenantId([9; 16])])
        );
        let view = state(&adapter).await;
        assert_eq!(registry(&view).revision(), 8);
        let revoke = registry(&view)
            .prepare_revoke(receipts[0].invitation, now())
            .unwrap();
        let operator = runtime(namespace);
        replicas[2]
            .host
            .submit(
                operator.clone(),
                ControlRequest {
                    id: ControlRequestId {
                        client: operator.principal().0,
                        sequence: 1,
                    },
                    acknowledged_through: 0,
                    command: ControlCommand::Enrollment(revoke),
                },
            )
            .await
            .unwrap();
        // Leave the TLS grant/cache untouched: the owner must reject the pin
        // from its current committed enrollment, including exact old retries.
        assert!(matches!(
            signer.invite(RequestId::from_u128(41), intent).await,
            Err(QuorumEnrollmentError::Control(ControlFailure::Unauthorized))
        ));
        assert_eq!(
            control_reply(
                &replicas[0]
                    .pool
                    .send_enrollment_control(
                        3,
                        &rpc_packet(&pin, ControlRpc::Read(ControlRead::State))
                    )
                    .await
                    .unwrap()
            ),
            ControlReply::Rejected(ControlFailure::Unauthorized)
        );
        signer.stop().await.unwrap();
    });
    result.unwrap();
    let blackhole = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let dead: BTreeMap<_, _> = (100..116)
        .map(|node| {
            (
                node,
                PeerEndpoint {
                    address: blackhole.local_addr().unwrap(),
                    server_name: receipts[2].identity.server_name.clone(),
                },
            )
        })
        .collect();
    replicas[0].pool.replace_routes(4, dead).unwrap();
    let before = allowance.stats();
    for id in [202, 203] {
        let previous = adapter.route_cursor.load(Ordering::Relaxed);
        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(5),
                adapter.read_state(RequestId::from_u128(id))
            )
            .await
            .unwrap(),
            Err(ControlFailure::OutcomeUnknown | ControlFailure::Unavailable)
        ));
        let current = adapter.route_cursor.load(Ordering::Relaxed);
        assert!((100..116).contains(&current));
        if previous >= 100 {
            assert!(current > previous && current <= previous + MAX_ROUTE_PROBES as u64);
        }
        assert_eq!(
            allowance.stats(),
            before,
            "timed-out routing round retained its allocation"
        );
    }
    drop(adapter);
    for replica in replicas {
        replica.host.stop().await.unwrap();
        replica.owner.join().unwrap();
        replica.server.close();
        replica.serving.await.unwrap().unwrap();
        replica.pool.close();
        replica.sending.await.unwrap();
    }
}
