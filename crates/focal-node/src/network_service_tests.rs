use super::*;

#[test]
fn network_service_requires_runtime_before_starting_physical_owners() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().to_path_buf());
    settings.node.listen = Some("127.0.0.1:45679".parse().unwrap());
    settings.node.advertise = Some("127.0.0.1:45679".into());
    let future = NetworkService::open(&settings);
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(ServiceError::Bootstrap(NetworkError::RuntimeRequired)))
    ));
    assert!(!directory.path().join("IDENTITY").exists());
}

struct Running {
    handles: NetworkHandles,
    status: NetworkServiceStatus,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), ServiceError>>,
}
impl Running {
    async fn start(settings: &Settings) -> Self {
        let service = NetworkService::open(settings).await.unwrap();
        let handles = service.handles();
        let (stop, receive) = oneshot::channel();
        let (ready, status) = oneshot::channel();
        let mut ready = Some(ready);
        let task = tokio::spawn(async move {
            service
                .run_until(
                    async {
                        let _ = receive.await;
                        Ok(())
                    },
                    move |status| {
                        if let Some(ready) = ready.take() {
                            let _ = ready.send(status.clone());
                        }
                        Ok(())
                    },
                )
                .await
        });
        let status = tokio::time::timeout(Duration::from_secs(15), status)
            .await
            .expect("service did not publish startup status")
            .unwrap();
        Self {
            handles,
            status,
            stop: Some(stop),
            task,
        }
    }
    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(10), &mut self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
fn settings(root: &Path) -> Settings {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.to_path_buf());
    settings.node.listen = Some(address);
    settings.node.advertise = Some(address.to_string());
    settings
}
fn wire_request(ledger: LedgerId, id: u128, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation,
    }
}

async fn wait_unlocked(settings: &Settings) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(directory) = NodeDirectory::open(settings) {
                drop(directory);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("physical owners failed to release the node directory");
}

#[test]
fn missing_runtime_drivers_fail_before_ingress_and_release_started_owners() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let no_time = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    assert!(matches!(
        no_time.block_on(NetworkService::open(&settings)),
        Err(ServiceError::Runtime)
    ));
    assert!(!directory.path().join("IDENTITY").exists());
    let no_io = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    assert!(matches!(
        no_io.block_on(NetworkService::open(&settings)),
        Err(ServiceError::Wire(WireError::Connection))
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(wait_unlocked(&settings));
    let service = runtime.block_on(NetworkService::open(&settings)).unwrap();
    assert!(matches!(
        no_time.block_on(service.run_until(std::future::pending(), |_| Ok(()))),
        Err(ServiceError::Runtime)
    ));
    runtime.block_on(wait_unlocked(&settings));
    let service = runtime.block_on(NetworkService::open(&settings)).unwrap();
    {
        let future = service.run_until(std::future::pending(), |_| Ok(()));
        let mut future = std::pin::pin!(future);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            future.as_mut().poll(&mut context),
            std::task::Poll::Ready(Err(ServiceError::Bootstrap(NetworkError::RuntimeRequired)))
        ));
    }
    runtime.block_on(wait_unlocked(&settings));
    let service = runtime.block_on(NetworkService::open(&settings)).unwrap();
    let (stop, receive) = oneshot::channel();
    {
        let running = service.run_until(
            async {
                receive.await.unwrap();
                Ok(())
            },
            |_| Ok(()),
        );
        let mut running = std::pin::pin!(running);
        runtime.block_on(std::future::poll_fn(|cx| {
            assert!(running.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        }));
        stop.send(()).unwrap();
        // The initial runtime probe already succeeded. Moving the suspended
        // future makes the cleanup timer fail after the task boundary returns.
        assert!(matches!(
            no_time.block_on(running),
            Err(ServiceError::Runtime)
        ));
    }
    runtime.block_on(wait_unlocked(&settings));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callback_unwind_still_stops_and_joins_all_physical_owners() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let service = NetworkService::open(&settings).await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        service.run_until(std::future::pending(), |_| {
            panic!("injected status callback")
        }),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(ServiceError::Runtime)), "{result:?}");
    drop(NodeDirectory::open(&settings).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_service_retains_lock_until_external_physical_handles_stop() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let mut running = Running::start(&settings).await;
    running.task.abort();
    assert!((&mut running.task).await.unwrap_err().is_cancelled());
    // The run future is gone, but the retained content handle still owns a live
    // store. The reaper must keep LOCK until it joins that store's thread.
    running
        .handles
        .content
        .install_policy(CustodyPolicy {
            ledger: running.status.ledger,
            route_epoch: RouteEpoch(1),
            policy_revision: 1,
            peers: BTreeSet::from([running.status.node]),
        })
        .await
        .unwrap();
    assert!(NodeDirectory::open(&settings).is_err());
    if let Some(ledger) = &running.handles.ledger {
        let _ = ledger.stop().await;
    }
    if let Some(directory) = running.handles.directory.host() {
        let _ = directory.stop().await;
    }
    let _ = running.handles.fleet.shutdown().await;
    let _ = running.handles.control.stop().await;
    running.handles.content.stop().await.unwrap();
    drop(running);
    wait_unlocked(&settings).await;
}

#[tokio::test]
async fn service_recovers_revocation_before_binding_any_ingress() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let mut founding = FoundingNetwork::open(&settings).await.unwrap();
    let revoke = founding
        .control
        .enrollment()
        .unwrap()
        .prepare_revoke(founding.receipt.invitation, unix_time().unwrap())
        .unwrap();
    let id = focal_control::ControlRequestId {
        client: [9; 16],
        sequence: 1,
    };
    founding
        .control
        .submit(
            focal_control::ControlRequest {
                id,
                acknowledged_through: 0,
                command: focal_control::ControlCommand::Enrollment(revoke),
            },
            &NoDirectoryAuthority,
        )
        .unwrap();
    founding.control.drain(&NoDirectoryAuthority).unwrap();
    assert!(founding.control.receipt(id).unwrap().is_some());
    drop(founding);
    assert!(matches!(
        NetworkService::open(&settings).await,
        Err(ServiceError::Bootstrap(NetworkError::Enrollment(
            focal_enrollment::EnrollmentError::Revoked
        )))
    ));
    assert!(!directory.path().join("focal.sock").exists());
    assert!(!directory.path().join(ADMIN_SOCKET).exists());
    drop(std::net::UdpSocket::bind(settings.node.listen.unwrap()).unwrap());
    drop(NodeDirectory::open(&settings).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_preserves_laptop_identity_receipts_content_and_owner_lock() {
    let directory = tempfile::tempdir().unwrap();
    let mut local = Settings::default();
    local.node.data_dir = Some(directory.path().to_path_buf());
    let mut embedded = crate::embedded::EmbeddedNode::open(&local).unwrap();
    let identity = embedded.identity.clone();
    let request = AuthenticatedInput {
        ledger: identity.ledger,
        principal: identity.issuer,
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(identity.root),
            policy_revision: 1,
            logical_time: 0,
            evidence: vec![],
        },
        command: Command::NegotiateEpoch {
            epoch: RequestEpoch(1),
        },
    };
    let focal_ledger::Submission::Committed(receipt) =
        embedded.session.submit_local(&request).unwrap()
    else {
        panic!("local commit required");
    };
    let upload = focal_evidence::UploadId([2; 16]);
    embedded
        .content
        .begin(
            upload,
            ContentDomainId(identity.ledger.tenant.0),
            ContentClass::Evidence,
            5,
            None,
        )
        .unwrap();
    embedded.content.append(upload, 0, b"proof").unwrap();
    let content = embedded.content.seal(upload).unwrap();
    embedded.checkpoint().unwrap();
    drop(embedded);
    let network = settings(directory.path());
    let running = Running::start(&network).await;
    assert_eq!(running.status.condition, "Ready");
    assert_eq!(running.status.node, identity.node);
    assert_eq!(running.handles.fleet.status().installed, 1);
    let first_directory = running
        .handles
        .directory
        .host()
        .expect("ready directory owner");
    let directory_identity = first_directory.progress().identity;
    let directory_index = first_directory.progress().applied_index;
    assert!(directory_index > 0);
    let root = running.handles.control.observe_root().await.unwrap();
    let focal_control::ControlBootstrap::Root {
        directory: delegated,
        ..
    } = &root.snapshot().state
    else {
        panic!("root");
    };
    assert!(
        delegated.regions.is_empty(),
        "a laptop has no invented geographic region"
    );
    assert_eq!(delegated.delegations.len(), 1);
    assert!(
        delegated
            .delegations
            .values()
            .all(|value| value.region == focal_directory::RegionId::UNKNOWN)
    );
    drop(root);
    assert!(NodeDirectory::open(&network).is_err());
    let remote = UnixRemote::new(&running.status.socket, WireLimits::default()).unwrap();
    let directory_reply = remote
        .request(&wire_request(
            running.handles.directory.namespace(),
            77,
            Operation::Control {
                group: directory_identity.group,
                request: focal_control::ControlRpc::Read(ControlRead::State)
                    .encode(4096)
                    .unwrap(),
            },
        ))
        .await
        .unwrap();
    let Response::Control { response } = directory_reply.result else {
        panic!("directory response");
    };
    let focal_control::ControlReply::Read(ControlReadResult::State(directory_state)) =
        focal_control::ControlReply::decode(&response, 128 * 1024).unwrap()
    else {
        panic!("directory state");
    };
    assert_eq!(directory_state.identity, directory_identity);
    assert!(
        matches!(&directory_state.state, focal_control::ControlBootstrap::Partition { directory } if directory.sessions.is_empty())
    );
    let reply = remote
        .request(&wire_request(
            identity.ledger,
            1,
            Operation::OpenEpoch {
                epoch: RequestEpoch(1),
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        reply.result,
        Response::Submitted(MutationReply::Committed(receipt.clone()))
    );
    let data = remote
        .request(&wire_request(
            identity.ledger,
            2,
            Operation::Download {
                content: content.clone(),
                offset: 0,
                max_bytes: 16,
            },
        ))
        .await
        .unwrap();
    assert!(
        matches!(data.result,Response::Content(ContentChunk {ref bytes,..}) if bytes==b"proof"),
        "unexpected content response: {:?}",
        data.result
    );
    let claim_request = wire_request(
        identity.ledger,
        3,
        Operation::Submit {
            expected_revision: None,
            command: Command::GenerateClaim {
                claim: crate::demo::claim(&identity, ClaimId::from_u128(13)).unwrap(),
            },
        },
    );
    let generated = remote.request(&claim_request).await.unwrap();
    assert!(
        matches!(&generated.result, Response::Submitted(MutationReply::Committed(receipt)) if receipt.sequence == SessionSeq(2)),
        "{generated:?}"
    );
    running.stop().await;
    let reopened = Running::start(&network).await;
    let recovered_directory = reopened.handles.directory.host().unwrap();
    assert_eq!(recovered_directory.progress().identity, directory_identity);
    assert!(recovered_directory.progress().applied_index >= directory_index);
    let reply = UnixRemote::new(&reopened.status.socket, WireLimits::default())
        .unwrap()
        .request(&wire_request(
            identity.ledger,
            1,
            Operation::OpenEpoch {
                epoch: RequestEpoch(1),
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        reply.result,
        Response::Submitted(MutationReply::Committed(receipt))
    );
    assert_eq!(
        UnixRemote::new(&reopened.status.socket, WireLimits::default())
            .unwrap()
            .request(&claim_request)
            .await
            .unwrap(),
        generated
    );
    reopened.stop().await;
    drop(NodeDirectory::open(&network).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joined_service_receives_committed_root_learner_and_restarts_without_ledger_policy() {
    let founder_dir = tempfile::tempdir().unwrap();
    let peer_dir = tempfile::tempdir().unwrap();
    let founder_settings = settings(founder_dir.path());
    let founder = Running::start(&founder_settings).await;
    let identity = crate::embedded::decode_identity(&founder_dir.path().join("IDENTITY")).unwrap();
    let admin = UnixRemote::new(
        founder.status.admin_socket.as_ref().unwrap(),
        admin_wire_limits(),
    )
    .unwrap();
    let request = crate::network_admin::AdminCommand::invitation("worker")
        .unwrap()
        .request(&identity)
        .unwrap();
    let Response::Control { response } = admin.request(&request).await.unwrap().result else {
        panic!("private invitation response required");
    };
    let invitation = crate::network_join::NodeInvitation::decode(&response).unwrap();
    let peer_settings = settings(peer_dir.path());
    let listen = peer_settings.node.listen.unwrap();
    let pending =
        crate::network_join::PendingJoin::open(&peer_settings, invitation, listen, listen).unwrap();
    let client = focal_enrollment::EnrollmentClient::bind(
        "127.0.0.1:0".parse().unwrap(),
        focal_enrollment::TransportLimits::default(),
    )
    .unwrap();
    let receipt = pending.redeem(&client, unix_time().unwrap()).await.unwrap();
    let peer_node = receipt.identity.node_id.unwrap();
    drop(pending.install(receipt, unix_time().unwrap()).unwrap());
    let peer = Running::start(&peer_settings).await;
    assert_eq!(peer.status.condition, "CatchingUp");
    assert!(!peer.status.assigned_ledger);
    assert!(peer.handles.ledger.is_none());
    assert_eq!(peer.handles.fleet.status().installed, 0);
    assert!(!peer.handles.fleet.status().stopped);
    assert!(peer.status.admin_socket.is_none());
    assert!(!peer_dir.path().join("POLICY").exists());
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let a = founder.handles.control.observe_root().await.unwrap();
            let b = peer.handles.control.observe_root().await.unwrap();
            if a.configuration()
                .configuration
                .learners
                .contains(&peer_node)
                && b.configuration()
                    .configuration
                    .learners
                    .contains(&peer_node)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("joined root never applied its committed learner configuration");
    peer.stop().await;
    founder.stop().await;
    let founder = Running::start(&founder_settings).await;
    let peer = Running::start(&peer_settings).await;
    let observed = peer.handles.control.observe_root().await.unwrap();
    assert!(
        observed
            .configuration()
            .configuration
            .learners
            .contains(&peer_node)
    );
    assert_eq!(
        observed.configuration().configuration.voters,
        vec![identity.node]
    );
    let authority = observed
        .authority()
        .expect("committed root node capabilities");
    assert_eq!(
        authority.anchor.genesis.0,
        observed.snapshot().identity.genesis
    );
    assert!(authority.applied_index <= observed.snapshot().applied_index);
    for node in [identity.node, peer_node] {
        let grant = authority
            .nodes
            .get(&node)
            .expect("enrolled node capability");
        assert_eq!(grant.enrollment.region.0, [0; 16]);
        assert_eq!(grant.enrollment.zone.0, [0; 16]);
        assert_eq!(grant.enrollment.generation, 1);
        assert!(grant.enrollment.eligible);
    }
    let plan =
        crate::directory_bootstrap::FirstDirectoryPlan::derive(identity.cluster, identity.node)
            .unwrap();
    assert_eq!(authority.groups.len(), 1);
    let directory_grant = authority
        .groups
        .get(&plan.group())
        .expect("first directory group grant");
    assert_eq!(
        directory_grant.voters,
        std::collections::BTreeMap::from([(identity.node, 1)])
    );
    assert!(directory_grant.learners.is_empty());
    assert!(matches!(
        directory_grant.scope,
        focal_directory::GroupScope::Partition { .. }
    ));
    assert!(
        peer.handles.directory.host().is_none(),
        "joining does not assign the founder directory"
    );
    assert!(!peer_dir.path().join("POLICY").exists());
    peer.stop().await;
    founder.stop().await;
}
