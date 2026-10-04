use super::*;
#[path = "client_join_service_tests.rs"]
mod client_join;

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

/// The allowance a starting node has to report ready, in seconds: what a
/// test that commits a credential lifetime derives the lifetime from, since
/// a host's credential, issued at its join, must outlive its start.
pub(crate) const START_ALLOWANCE: u64 = 15;
/// What a supervisor gives a probe before it counts it failed: Kubernetes'
/// default `timeoutSeconds`, which the probes the deployment renderer writes
/// rely on (`deploy/kubernetes`). A probe's answer is a claim about time,
/// held to that timeout here, not a wait on an owner's progress.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
pub(crate) struct Running {
    pub(crate) handles: NetworkHandles,
    pub(crate) status: NetworkServiceStatus,
    /// The node's data handler, for requests without a transport.
    pub(crate) data: DataService,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<ServiceStopped, ServiceError>>,
    /// How the service ended, once it has, for a wait that failed to say
    /// why: a service that ended is what every handle's `Unavailable` means.
    ended: tokio::sync::watch::Receiver<Option<String>>,
}
impl Running {
    /// Boxed by a plain function, as `NetworkService::open_with_socket` is:
    /// a test body's poll frame holds a pointer to the start, not its
    /// state, in a debug build (the renewal journey's body held 628 KiB of
    /// such temporaries, and a Windows test thread of 2 MiB overflowed).
    pub(crate) fn start(
        settings: &TestSettings,
    ) -> std::pin::Pin<Box<impl Future<Output = Self> + '_>> {
        Box::pin(Self::start_inner(settings))
    }
    async fn start_inner(settings: &TestSettings) -> Self {
        Self::from_service(settings.open().await.unwrap()).await
    }
    fn from_service(service: NetworkService) -> std::pin::Pin<Box<impl Future<Output = Self>>> {
        Box::pin(Self::from_service_inner(service))
    }
    async fn from_service_inner(service: NetworkService) -> Self {
        let handles = service.handles();
        let data = service.data.clone();
        let (stop, receive) = oneshot::channel();
        let (ready, status) = oneshot::channel();
        let mut ready = Some(ready);
        let (ended_send, ended) = tokio::sync::watch::channel(None);
        let task = tokio::spawn(async move {
            let outcome = service
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
                .await;
            let _ = ended_send.send(Some(format!("{outcome:?}")));
            outcome
        });
        let mut task = task;
        // Charged to the root owner's periods (27 §3.1 P8): what the
        // allowance holds at its tick, however slowly the machine runs it.
        let status = match crate::test_waits::charged(
            || vec![handles.control.periods()],
            Duration::from_secs(START_ALLOWANCE),
            crate::test_waits::CONTROL_TICK,
            status,
        )
        .await
        .unwrap_or_else(|spent| panic!("service did not publish startup status: {spent}"))
        {
            Ok(status) => status,
            Err(_) => panic!(
                "the service ended before publishing its status: {:?}",
                (&mut task).await
            ),
        };
        Self {
            handles,
            status,
            data,
            stop: Some(stop),
            task,
            ended,
        }
    }
    /// How the service ended, if it has: the word a failed wait prints
    /// beside what its handles answered.
    pub(crate) fn ended(&self) -> Option<String> {
        self.ended.borrow().clone()
    }
    /// Stop and report how the service ended, without unwrapping.
    pub(crate) async fn outcome(mut self) -> Result<(), ServiceError> {
        let _ = self.stop.take().unwrap().send(());
        Self::stopped(&mut self.task).await
    }
    pub(crate) async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        Self::stopped(&mut self.task).await.unwrap();
    }
    /// A stopping service keeps its own word: its cleanup ends within
    /// `SHUTDOWN_DEADLINE`, by finishing or by `ShutdownTimeout`. The wait
    /// here is that deadline and the frozen allowance after it, so what it
    /// catches is a service that did not return at all, and what a slow
    /// stop reports is the service's own `ShutdownTimeout`, not a guess of
    /// the harness about how long a stop takes on this machine.
    async fn stopped(
        task: &mut tokio::task::JoinHandle<Result<ServiceStopped, ServiceError>>,
    ) -> Result<(), ServiceError> {
        match tokio::time::timeout(SHUTDOWN_DEADLINE.saturating_add(FROZEN), task).await {
            Ok(joined) => joined.unwrap().map(|_| ()),
            Err(_) => panic!(
                "the service did not return within its shutdown deadline of {SHUTDOWN_DEADLINE:?} and a frozen allowance of {FROZEN:?}"
            ),
        }
    }
}
/// How long the slowest observed owner may run no period at all before a
/// wait calls it wedged: the only wall-clock bound a wait has (27 §3.1 P8).
pub(crate) const FROZEN: Duration = Duration::from_secs(60);
impl Running {
    /// The periods of the owners this service has run since it started: the
    /// root group's, and its own session's where it founded one.
    pub(crate) fn periods(&self) -> Vec<u64> {
        let mut periods = vec![self.handles.control.periods()];
        periods.extend(self.handles.ledger.as_ref().map(ReplicaHost::periods));
        // Every session copy this node hosts, by the one that has run the
        // fewest periods: what a wait reads is theirs as often as the
        // root's, and each runs at a pace of its own. A copy installed
        // during the wait counts from its start, which is what is waited
        // on (27 §3.1 P8).
        let mut after = None;
        let mut fewest: Option<u64> = None;
        while let Some((ledger, host)) = self.handles.fleet.next_host(after) {
            after = Some(ledger);
            let ran = host.periods();
            fewest = Some(fewest.map_or(ran, |least| least.min(ran)));
        }
        periods.extend(fewest);
        periods
    }
}
fn periods_of(services: &[&Running]) -> Vec<u64> {
    services
        .iter()
        .flat_map(|service| service.periods())
        .collect()
}
/// Poll until `poll` yields, charged to the periods the services' owners
/// run and not to the wall clock: `allowance` is what the wait would take
/// at most on an idle machine, and it stretches with the machine. Pass
/// every service whose state the poll reads.
pub(crate) async fn until<T>(
    what: &str,
    services: &[&Running],
    allowance: Duration,
    poll: impl AsyncFnMut() -> Option<T>,
) -> T {
    match try_until(services, allowance, poll).await {
        Ok(value) => value,
        Err(spent) => panic!(
            "never reached: {what}: {spent}; services ended {:?}",
            services
                .iter()
                .map(|service| service.ended())
                .collect::<Vec<_>>()
        ),
    }
}
/// [`until`], for a caller that reports what it observed when the wait is
/// spent.
pub(crate) async fn try_until<T>(
    services: &[&Running],
    allowance: Duration,
    mut poll: impl AsyncFnMut() -> Option<T>,
) -> Result<T, focal_timing::Spent> {
    let period = services
        .iter()
        .map(|service| service.handles.control.tick_period())
        .max()
        .unwrap_or(Duration::from_millis(100));
    let mut wait = focal_timing::ProgressDeadline::begin(
        &periods_of(services),
        focal_timing::ProgressDeadline::periods(allowance, period),
        FROZEN,
    );
    loop {
        if let Some(value) = poll().await {
            return Ok(value);
        }
        wait.check(&periods_of(services))?;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
pub(crate) struct TestSettings {
    pub(crate) value: Settings,
    // Keep the physical reservation throughout startup, client creation, and
    // restart. Quinn receives a duplicate handle to this same bound socket,
    // never an address obtained by closing a temporary socket.
    pub(crate) socket: std::net::UdpSocket,
}
impl std::ops::Deref for TestSettings {
    type Target = Settings;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl TestSettings {
    pub(crate) fn open(
        &self,
    ) -> std::pin::Pin<Box<impl Future<Output = Result<NetworkService, ServiceError>> + '_>> {
        NetworkService::open_with_socket(&self.value, Some(self.socket.try_clone().unwrap()))
    }
}
pub(crate) fn settings(root: &Path) -> TestSettings {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.to_path_buf());
    settings.node.listen = Some(address);
    settings.node.advertise = Some(address.to_string());
    TestSettings {
        value: settings,
        socket,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserved_listener_survives_client_creation_and_releases_after_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let address = settings.socket.local_addr().unwrap();
    // This was the failing fixture order: choose a server port, then create an
    // ephemeral enrollment client before opening the service. The reservation
    // must stay live throughout both client creation and async node recovery.
    let client = focal_enrollment::EnrollmentClient::bind(
        "127.0.0.1:0".parse().unwrap(),
        focal_enrollment::TransportLimits::default(),
    )
    .unwrap();
    assert_eq!(
        std::net::UdpSocket::bind(address).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    let service = settings.open().await.unwrap();
    assert_eq!(service.status().listen, address);
    // No test reservation may mask a retained driver socket in this assertion.
    drop(settings);
    let running = Running::from_service(service).await;
    running.stop().await;
    let rebound = std::net::UdpSocket::bind(address);
    assert!(
        rebound.is_ok(),
        "completed service retained UDP listener {address}: {rebound:?}"
    );
    drop(client);
}

#[tokio::test]
async fn reserved_listener_must_match_the_persisted_listen_address() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let wrong = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    assert_ne!(
        wrong.local_addr().unwrap(),
        settings.socket.local_addr().unwrap()
    );
    assert!(matches!(
        NetworkService::open_with_socket(&settings, Some(wrong)).await,
        Err(ServiceError::Owner(
            "listener socket address differs from node state"
        ))
    ));
    assert!(!directory.path().join("focal.sock").exists());
    assert!(!directory.path().join(ADMIN_SOCKET).exists());
    drop(NodeDirectory::open(&settings).unwrap());
}

#[tokio::test]
async fn interrupted_listener_shutdown_preserves_the_release_fence() {
    let directory = tempfile::tempdir().unwrap();
    let fixture = settings(directory.path());
    let settings = fixture.value.clone();
    let address = fixture.socket.local_addr().unwrap();
    let mut service = fixture.open().await.unwrap();
    drop(fixture);
    {
        let shutdown = service.listener.shutdown();
        let mut shutdown = std::pin::pin!(shutdown);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        // On this current-thread runtime, Quinn cannot release its driver until
        // we yield. Interrupting the first wait must not discard its completion
        // receiver and make the next shutdown report a false success.
        assert!(shutdown.as_mut().poll(&mut context).is_pending());
    }
    crate::test_waits::charged(
        || vec![service.handles.control.periods()],
        Duration::from_secs(5),
        crate::test_waits::CONTROL_TICK,
        service.listener.shutdown(),
    )
    .await
    .expect("the listener shut down");
    service.listener.shutdown().await;
    drop(std::net::UdpSocket::bind(address).unwrap());
    drop(service);
    wait_unlocked(&settings).await;
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

/// The service dropped, its physical owners release the node directory as
/// they end. No owner is left to charge the wait to: the frozen window is
/// its bound (`crate::test_waits`).
async fn wait_unlocked(settings: &Settings) {
    crate::test_waits::charged(
        Vec::new,
        Duration::from_secs(5),
        crate::test_waits::CONTROL_TICK,
        async {
            loop {
                if let Ok(directory) = NodeDirectory::open(settings) {
                    drop(directory);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
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
        no_time.block_on(settings.open()),
        Err(ServiceError::Runtime)
    ));
    assert!(!directory.path().join("IDENTITY").exists());
    let no_io = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    assert!(matches!(
        no_io.block_on(settings.open()),
        Err(ServiceError::Wire(WireError::Connection))
    ));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(wait_unlocked(&settings));
    let service = runtime.block_on(settings.open()).unwrap();
    assert!(matches!(
        no_time.block_on(service.run_until(std::future::pending(), |_| Ok(()))),
        Err(ServiceError::Runtime)
    ));
    runtime.block_on(wait_unlocked(&settings));
    let service = runtime.block_on(settings.open()).unwrap();
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
    let service = runtime.block_on(settings.open()).unwrap();
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
        let ended = no_time.block_on(running);
        // Whichever driver meets the missing timer first names the failure:
        // the service's own boundary, or the controller's.
        assert!(
            matches!(
                ended,
                Err(ServiceError::Runtime
                    | ServiceError::Controller(
                        crate::network_controller::ControllerError::Runtime
                    ))
            ),
            "{ended:?}"
        );
    }
    runtime.block_on(wait_unlocked(&settings));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callback_unwind_still_stops_and_joins_all_physical_owners() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let service = settings.open().await.unwrap();
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
        settings.open().await,
        Err(ServiceError::Bootstrap(NetworkError::Enrollment(
            focal_enrollment::EnrollmentError::Revoked
        )))
    ));
    assert!(!directory.path().join("focal.sock").exists());
    assert!(!directory.path().join(ADMIN_SOCKET).exists());
    let TestSettings {
        value: settings,
        socket,
    } = settings;
    drop(socket);
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
    assert_eq!(
        peer.status.admin_socket.as_deref(),
        Some(
            peer_settings
                .data_dir()
                .unwrap()
                .join(ADMIN_SOCKET)
                .as_path()
        )
    );
    assert!(!peer_dir.path().join("POLICY").exists());
    until(
        "the joined root applies its committed learner configuration",
        &[&founder, &peer],
        Duration::from_secs(15),
        async || {
            let a = founder.handles.control.observe_root().await.unwrap();
            let b = peer.handles.control.observe_root().await.unwrap();
            (a.configuration()
                .configuration
                .learners
                .contains(&peer_node)
                && b.configuration()
                    .configuration
                    .learners
                    .contains(&peer_node))
            .then_some(())
        },
    )
    .await;
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
    // The first directory group is committed before the founder serves; the
    // placement agent registers the founder's own session group after it.
    assert!(!authority.groups.is_empty());
    assert!(authority.groups.len() <= 2);
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
    until(
        "the placement agent registers the founder's session group",
        &[&founder],
        Duration::from_secs(15),
        async || {
            let observed = founder.handles.control.observe_root().await.unwrap();
            observed
                .authority()
                .is_some_and(|authority| {
                    authority.groups.values().any(|grant| {
                        grant.scope == focal_directory::GroupScope::Session(identity.ledger)
                            && grant.voters
                                == std::collections::BTreeMap::from([(identity.node, 1)])
                    })
                })
                .then_some(())
        },
    )
    .await;
    assert!(!peer_dir.path().join("POLICY").exists());
    peer.stop().await;
    founder.stop().await;
    // Both endpoints handled live QUIC connections. Successful shutdown must
    // release their sockets, not merely their public Endpoint handles. Remove
    // our reservations before checking so a lingering driver cannot be hidden.
    let founder_address = founder_settings.node.listen.unwrap();
    let peer_address = peer_settings.node.listen.unwrap();
    drop(founder_settings);
    drop(peer_settings);
    drop(std::net::UdpSocket::bind(founder_address).unwrap());
    drop(std::net::UdpSocket::bind(peer_address).unwrap());
}

/// A founder that has grown (a host joined) and is restarted on a different
/// listen address behind the *same* advertised endpoint recovers: only the
/// transient transport addresses changed, not its identity, sponsor trust or
/// genesis. (A founder advertised by name behaves the same when the name
/// resolves elsewhere; the listen change here drives the identical path.) Before `FoundingNetwork::bootstrap_blocking` reconciled addresses
/// through `NetworkState::install`, it compared the saved state for exact
/// equality, so every restart that resolved a new address — a rescheduled
/// pod, a fresh lease, a moved VM — failed as "corrupt or incompatible".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_founder_restarted_on_a_new_listen_address_behind_its_advertised_endpoint_recovers() {
    let founder_dir = tempfile::tempdir().unwrap();
    let peer_dir = tempfile::tempdir().unwrap();
    // The advertised endpoint stays fixed across both starts; only the
    // listen address behind it changes.
    let first_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let first_listen = first_socket.local_addr().unwrap();
    let port = first_listen.port();
    let name = first_listen.to_string();
    let mut value = Settings::default();
    value.node.data_dir = Some(founder_dir.path().to_path_buf());
    value.node.listen = Some(first_listen);
    value.node.advertise = Some(name.clone());
    let first = TestSettings {
        value,
        socket: first_socket,
    };
    let founder = Running::start(&first).await;
    let founder_node = founder.status.node;
    assert_eq!(founder.status.listen, first_listen);
    // Grow the founder so its root group is no longer a pristine one voter.
    let peer_settings = settings(peer_dir.path());
    let (peer, node_a) = crate::placement_agent::tests::join_peer(
        &founder,
        founder_dir.path(),
        "host-a",
        &peer_settings,
    )
    .await;
    assert_ne!(node_a, founder_node);
    peer.stop().await;
    founder.stop().await;
    drop(first);
    // Restart on the wildcard interface at the same port: the listen address
    // differs from the saved one while the advertised endpoint is unchanged.
    let second_socket = std::net::UdpSocket::bind(("0.0.0.0", port)).unwrap();
    let second_listen = second_socket.local_addr().unwrap();
    assert_ne!(second_listen, first_listen);
    let mut value = Settings::default();
    value.node.data_dir = Some(founder_dir.path().to_path_buf());
    value.node.listen = Some(second_listen);
    value.node.advertise = Some(name);
    let second = TestSettings {
        value,
        socket: second_socket,
    };
    let restarted = Running::start(&second).await;
    assert_eq!(
        restarted.status.node, founder_node,
        "the same node identity recovered"
    );
    assert_eq!(
        restarted.status.listen, second_listen,
        "the new listen address was adopted, not refused"
    );
    assert_eq!(restarted.status.advertise.port(), port);
    restarted.stop().await;
}

/// The KIND incident, as a regression: a joined host restarted on a new
/// address heals the mesh by itself. Its leader keeps sending to the old,
/// committed address, so the host cannot observe the root; observing the
/// root is what the controller needed before it would announce — a cycle
/// that left every rescheduled host leaderless and every placement on it
/// stalled, while the detector kept reporting it alive. Now the controller
/// announces the new contact through the immutable sponsor route while the
/// root is unobservable: the root asks the old address, finds it silent,
/// commits the move, and the leader reaches the host again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_restarted_on_a_new_address_reannounces_and_regains_its_leader() {
    let founder_dir = tempfile::tempdir().unwrap();
    let host_dir = tempfile::tempdir().unwrap();
    let founder_settings = settings(founder_dir.path());
    let founder = Running::start(&founder_settings).await;
    let founder_node = founder.status.node;
    let first = settings(host_dir.path());
    let first_advertise = first.socket.local_addr().unwrap();
    let (host, host_node) =
        crate::placement_agent::tests::join_peer(&founder, founder_dir.path(), "host-a", &first)
            .await;
    // The root committed the host at its first address.
    let committed = |observation: &crate::control_host::RootObservation| {
        observation
            .contacts()
            .contacts
            .records
            .iter()
            .find(|record| record.node == host_node)
            .map(|record| record.advertise)
    };
    // The announce is asynchronous: the host commits its first contact
    // once its controller observes the root.
    until(
        "the joined host commits its first contact",
        &[&founder, &host],
        Duration::from_secs(30),
        async || {
            let observation = founder.handles.control.observe_root().await.unwrap();
            (committed(&observation) == Some(first_advertise)).then_some(())
        },
    )
    .await;
    host.stop().await;
    drop(first);
    // The host returns on a different address; the root still holds the
    // old one, so nothing reaches the host until it announces the move.
    let second_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let second_advertise = second_socket.local_addr().unwrap();
    assert_ne!(second_advertise, first_advertise);
    let mut value = Settings::default();
    value.node.data_dir = Some(host_dir.path().to_path_buf());
    value.node.listen = Some(second_advertise);
    value.node.advertise = Some(second_advertise.to_string());
    let second = TestSettings {
        value,
        socket: second_socket,
    };
    let host = Running::start(&second).await;
    assert_eq!(
        host.status.node, host_node,
        "the same node identity recovered"
    );
    assert_eq!(host.status.advertise, second_advertise);
    let mut seen = (false, false);
    let healed = try_until(&[&founder, &host], Duration::from_secs(90), async || {
        let moved = founder
            .handles
            .control
            .observe_root()
            .await
            .ok()
            .and_then(|observation| committed(&observation))
            == Some(second_advertise);
        let led = host.handles.control.progress().leader == founder_node;
        seen = (moved, led);
        (moved && led).then_some(())
    })
    .await;
    if let Err(spent) = healed {
        panic!(
            "the moved host never healed: {spent}: contact moved={}, leader regained={}",
            seen.0, seen.1
        );
    }
    host.stop().await;
    founder.stop().await;
}

/// A store bootstrapped at one durability and raised by `deployment apply`
/// restarts from the same static file it was bootstrapped with (a
/// Kubernetes configmap does not follow the committed policy). The committed
/// policy is what the fleet already carries, so the restart must carry it
/// rather than refuse the stale seed: before this, every founder restart
/// after a sanctioned apply crash-looped on `CommittedPolicyChange`, taking
/// the control plane down with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_founder_restarts_from_its_stale_seed_file_after_a_stronger_policy_was_applied() {
    use crate::config::policy::{PolicyRevision, commit, read_committed};
    let dir = tempfile::tempdir().unwrap();
    // The file's seed: single-node durability, the only first-start a lone
    // founder can satisfy.
    let settings = settings(dir.path());
    assert_eq!(
        settings.durability.survive,
        crate::config::FailureDomain::Node
    );
    let founder = Running::start(&settings).await;
    founder.stop().await;
    let root = settings.node.data_dir.clone().unwrap();
    let seeded = read_committed(&root)
        .unwrap()
        .expect("first start committed the seed");
    assert_eq!(seeded.revision, PolicyRevision(1));
    // `deployment apply` commits stronger durability than the seed.
    let mut stronger = seeded.intent.clone();
    stronger.durability.survive = crate::config::FailureDomain::Zone;
    stronger.durability.max_failures = 1;
    let applied = commit(
        &root,
        &stronger,
        PolicyRevision(1),
        crate::embedded::atomic_file,
    )
    .unwrap();
    assert_eq!(applied.revision, PolicyRevision(2));
    // The same stale file starts the founder again: it carries the committed
    // policy instead of refusing it.
    let founder = Running::start(&settings).await;
    let carried = read_committed(&root)
        .unwrap()
        .expect("the committed policy survives the restart");
    assert_eq!(
        carried, applied,
        "the restart neither refused nor regressed the applied policy"
    );
    founder.stop().await;
}

/// A host's liveness is its own process answering on its socket, never the
/// control plane's reachability: with the founder (the only root voter, so
/// the root leader) gone, `probe alive` still holds at once, and the full
/// readiness report — which does wait on the root and the session leaders —
/// returns within its budget rather than hanging. Before this the liveness
/// probe waited on that report and timed out whenever the root leader was
/// down, and the supervisor killed every healthy host (24 §15).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_stays_alive_and_its_readiness_stays_bounded_while_the_root_leader_is_down() {
    use crate::cluster_admin::{ClusterAdmin, ClusterAdminError};
    let founder_dir = tempfile::tempdir().unwrap();
    let host_dir = tempfile::tempdir().unwrap();
    let founder_settings = settings(founder_dir.path());
    let founder = Running::start(&founder_settings).await;
    let host_settings = settings(host_dir.path());
    let (host, _node) = crate::placement_agent::tests::join_peer(
        &founder,
        founder_dir.path(),
        "host-a",
        &host_settings,
    )
    .await;
    let admin = ClusterAdmin::open(&host_settings).unwrap();
    // Alive while the root leader is reachable.
    tokio::time::timeout(PROBE_TIMEOUT, admin.probe("alive"))
        .await
        .expect("alive answers at once")
        .expect("the host is alive");
    // The root leader goes away: the host's own process is untouched.
    founder.stop().await;
    let started = tokio::time::Instant::now();
    tokio::time::timeout(PROBE_TIMEOUT, admin.probe("alive"))
        .await
        .expect("alive answers at once with the root leader down")
        .expect("the host is still alive");
    assert!(
        started.elapsed() < PROBE_TIMEOUT,
        "liveness never waits on the control plane"
    );
    // The readiness report waits on what is unreachable only up to its
    // budget, then reports what it could not see as absent: bounded, and it
    // does not claim the node is ready.
    let started = tokio::time::Instant::now();
    let readiness = tokio::time::timeout(PROBE_TIMEOUT, admin.probe("catching-up")).await;
    assert!(
        readiness.is_ok(),
        "the readiness report returns within its budget with the root leader down"
    );
    assert!(started.elapsed() < PROBE_TIMEOUT);
    assert!(
        matches!(readiness, Ok(Err(ClusterAdminError::ProbeFailed(_)))),
        "a host without a root leader is not catching up: {readiness:?}"
    );
    // Serving asks whether the owners run, never for a quorum: a healthy
    // follower with the root leader down is ready to serve (the audit's
    // F25), so its supervisor keeps it in the endpoints its peers need.
    tokio::time::timeout(PROBE_TIMEOUT, admin.probe("serving"))
        .await
        .expect("serving answers within the readiness budget")
        .expect("a healthy host serves with the root leader down");
    host.stop().await;
}

/// A node one of whose session owners stopped is alive and not serving (the
/// audit's F25): its supervisor's liveness keeps it, its readiness takes it
/// out until the owner runs again — with no leadership or quorum asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_whose_session_owner_stopped_is_alive_and_not_serving() {
    use crate::cluster_admin::{ClusterAdmin, ClusterAdminError};
    let dir = tempfile::tempdir().unwrap();
    let settings = settings(dir.path());
    let founder = Running::start(&settings).await;
    let admin = ClusterAdmin::open(&settings).unwrap();
    tokio::time::timeout(PROBE_TIMEOUT, admin.probe("serving"))
        .await
        .expect("serving answers within the readiness budget")
        .expect("a founder whose owners run serves");
    let (_, session) = founder
        .handles
        .fleet
        .next_host(None)
        .expect("the founder's session");
    session.stop().await.unwrap();
    tokio::time::timeout(PROBE_TIMEOUT, admin.probe("alive"))
        .await
        .expect("alive answers at once")
        .expect("the process answers while a session owner is stopped");
    let serving = tokio::time::timeout(PROBE_TIMEOUT, admin.probe("serving")).await;
    assert!(
        matches!(serving, Ok(Err(ClusterAdminError::ProbeFailed("serving")))),
        "a stopped session owner is not serving: {serving:?}"
    );
    founder.stop().await;
}

/// A joined host measures its path to the root's voter and derives its tick
/// period from it (27 §3.1 P2): on a loopback the measurement is recorded
/// and the period stays the configured one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_host_measures_its_voter_path_and_keeps_the_configured_period_on_a_loopback() {
    let founder_dir = tempfile::tempdir().unwrap();
    let host_dir = tempfile::tempdir().unwrap();
    let founder_settings = settings(founder_dir.path());
    let founder = Running::start(&founder_settings).await;
    let founder_node = founder.status.node;
    let host_settings = settings(host_dir.path());
    let (host, _) = crate::placement_agent::tests::join_peer(
        &founder,
        founder_dir.path(),
        "host-a",
        &host_settings,
    )
    .await;
    let configured = host.handles.control.tick_period();
    let pace = until(
        "the host measures its path to the root voter",
        &[&founder, &host],
        Duration::from_secs(60),
        async || {
            let pace = host.handles.control.current_pace();
            (pace.samples > 0).then_some(pace)
        },
    )
    .await;
    assert!(pace.broadcast_tail_ns > 0);
    // The period in force is the derivation of what was measured: ten tails
    // per election timeout of ten ticks is one tail per tick, never under
    // the configured period nor over the ceiling. On a quiet loopback that
    // is the configured period itself.
    let ceiling = Duration::from_secs(2);
    assert_eq!(
        pace.period,
        Duration::from_nanos(pace.broadcast_tail_ns).clamp(configured, ceiling),
        "{pace:?}"
    );
    assert_eq!(host.handles.control.progress().leader, founder_node);
    // The founder is the root's only voter: it has no voter path to measure.
    let own = founder.handles.control.current_pace();
    assert_eq!(own.samples, 0);
    assert_eq!(own.period, configured);
    host.stop().await;
    founder.stop().await;
}
