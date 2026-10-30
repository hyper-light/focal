use super::*;
use focal_model::*;
use focal_wire::*;
use std::{sync::Mutex, time::Duration};

fn request() -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(7),
        request_id: RequestId::from_u128(99),
        operation: Operation::Submit {
            expected_revision: None,
            command: Command::RecordProgress {
                claim: ClaimId::from_u128(5),
                receipt: ReceiptFence {
                    receipt: ReceiptId::from_u128(6),
                    epoch: 1,
                },
                message: "private-progress-payload".into(),
            },
        },
    }
}
fn receipt(request: &RequestEnvelope) -> MutationReceipt {
    MutationReceipt {
        ledger: request.ledger,
        key: RequestKey {
            principal: ParticipantId::from_u128(3),
            epoch: request.request_epoch,
            id: request.request_id,
        },
        sequence: SessionSeq(12),
        command_hash: ContentHash([9; 32]),
        outcome: CommandResult::Noop,
    }
}
fn policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        max_elapsed: Duration::from_secs(1),
        base_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
    }
}

#[test]
fn authentication_errors_are_distinct_but_cannot_erase_an_unknown_write() {
    focal_rt::block_on(
        authentication_errors_are_distinct_but_cannot_erase_an_unknown_write_on_a_shard(),
    )
    .unwrap();
}
async fn authentication_errors_are_distinct_but_cannot_erase_an_unknown_write_on_a_shard() {
    struct Authentication {
        first_unknown: std::sync::atomic::AtomicBool,
    }
    impl ClientTransport for Authentication {
        fn request<'a>(
            &'a self,
            _: Option<&'a RouteHint>,
            _: &'a RequestEnvelope,
        ) -> TransportFuture<'a> {
            Box::pin(async {
                if self
                    .first_unknown
                    .swap(false, std::sync::atomic::Ordering::Relaxed)
                {
                    Err(WireError::Connection)
                } else {
                    Err(WireError::Authentication)
                }
            })
        }
    }
    for first_unknown in [false, true] {
        let client = Client::new(
            Authentication {
                first_unknown: std::sync::atomic::AtomicBool::new(first_unknown),
            },
            policy(),
            WireLimits::default(),
            1,
        )
        .unwrap();
        let original = request();
        let error = client.request(original.clone()).await.unwrap_err();
        if first_unknown {
            let ClientError::OutcomeUnknown { request } = error else {
                panic!("unknown write lost its identity")
            };
            assert_eq!(*request, original);
        } else {
            assert!(matches!(error, ClientError::Unauthenticated));
            assert_eq!(failure::client(&error).code, "unauthenticated");
        }
    }
    assert_eq!(
        failure::access(&AccessError::Unauthorized).code,
        "unauthorized"
    );
}

/// A panicking transport is contained on a shard: a write's outcome stays unknown under its own
/// identity and a read is a transport failure. Off a shard, where no deadline could be kept, both
/// are refused before the transport is called.
#[test]
fn a_panicking_transport_does_not_unwind_or_replace_write_identity_and_off_a_shard_nothing_is_sent()
{
    struct Panics;
    impl ClientTransport for Panics {
        fn request<'a>(
            &'a self,
            _: Option<&'a RouteHint>,
            _: &'a RequestEnvelope,
        ) -> TransportFuture<'a> {
            Box::pin(async { panic!("injected transport dependency failure") })
        }
    }
    fn read(original: &RequestEnvelope) -> RequestEnvelope {
        let mut read = original.clone();
        read.operation = Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::Objects(vec![]),
            max_items: 1,
        });
        read
    }
    focal_rt::block_on(async {
        let client = Client::new(Panics, policy(), WireLimits::default(), 1).unwrap();
        let original = request();
        let result = client.request(original.clone()).await;
        let Err(ClientError::OutcomeUnknown { request: retained }) = result else {
            panic!("write outcome must remain unknown")
        };
        assert_eq!(*retained, original);
        assert!(matches!(
            client.request(read(&original)).await,
            Err(ClientError::Transport)
        ));
    })
    .unwrap();
    let client = Client::new(Panics, policy(), WireLimits::default(), 1).unwrap();
    let original = request();
    assert!(matches!(
        crate::time::off_a_shard(client.request(original.clone())),
        Err(ClientError::Transport)
    ));
    assert!(matches!(
        crate::time::off_a_shard(client.request(read(&original))),
        Err(ClientError::Transport)
    ));
}

#[test]
fn embedded_transport_checks_managed_syntax_capability_before_dispatch() {
    focal_rt::block_on(
        embedded_transport_checks_managed_syntax_capability_before_dispatch_on_a_shard(),
    )
    .unwrap();
}
async fn embedded_transport_checks_managed_syntax_capability_before_dispatch_on_a_shard() {
    struct Legacy;
    impl RequestHandler for Legacy {
        fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
            assert_eq!(request.request().protocol, PROTOCOL_VERSION);
            Box::pin(async move {
                request
                    .request()
                    .reply(Response::Error(AccessError::Unavailable))
            })
        }
    }
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(3),
        tenants: [request().ledger.tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap();
    let transport = EmbeddedTransport::new(peer, Legacy, WireLimits::default()).unwrap();
    let mut managed = request();
    managed.protocol = MANAGED_PROTOCOL_VERSION;
    managed.request_epoch = RequestEpoch(1);
    managed.operation = Operation::RequestStreamRead {
        cluster: [1; 16],
        query: RequestStreamQuery::Slot { slot: 0 },
    };
    assert!(matches!(
        transport.request(None, &managed).await,
        Err(WireError::Access(AccessError::UnsupportedProtocol))
    ));
    assert!(matches!(
        transport.request(None, &request()).await.unwrap().result,
        Response::Error(AccessError::Unavailable)
    ));
}

#[test]
fn reconciliation_binds_expected_principal_after_transport_shape_validation() {
    focal_rt::block_on(
        reconciliation_binds_expected_principal_after_transport_shape_validation_on_a_shard(),
    )
    .unwrap();
}
async fn reconciliation_binds_expected_principal_after_transport_shape_validation_on_a_shard() {
    struct Observe {
        principal: ParticipantId,
    }
    impl ClientTransport for Observe {
        fn request<'a>(
            &'a self,
            _route: Option<&'a RouteHint>,
            request: &'a RequestEnvelope,
        ) -> TransportFuture<'a> {
            Box::pin(async move {
                let Operation::Reconcile(ReconcileQuery::Epoch { epoch }) = request.operation
                else {
                    panic!("unexpected query")
                };
                Ok(request.reply(Response::Reconciled(ReconcileReply {
                    applied_index: 20,
                    token: ReadToken {
                        ledger: request.ledger,
                        sequence: SessionSeq(12),
                        route_epoch: request.route_epoch,
                    },
                    page: ReconcilePage {
                        schema: RECONCILE_SCHEMA,
                        ledger: request.ledger,
                        principal: self.principal,
                        sequence: SessionSeq(12),
                        result: ReconcileResult::Epoch(EpochReconciliation {
                            epoch,
                            minimum: Some(RequestEpoch(1)),
                            latest_admitted: Some(RequestEpoch(7)),
                            admitted: true,
                        }),
                    },
                })))
            })
        }
    }
    let query = RequestEnvelope {
        operation: Operation::Reconcile(ReconcileQuery::Epoch {
            epoch: RequestEpoch(7),
        }),
        ..request()
    };
    let expected = ParticipantId::from_u128(3);
    let valid = Client::new(
        Observe {
            principal: expected,
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    assert_eq!(
        valid
            .reconcile(query.clone(), expected)
            .await
            .unwrap()
            .page
            .principal,
        expected
    );
    let wrong = Client::new(
        Observe {
            principal: ParticipantId::from_u128(4),
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    assert!(matches!(
        wrong.reconcile(query.clone(), expected).await,
        Err(ClientError::InvalidResponse)
    ));
    assert!(matches!(
        valid.reconcile(query, ParticipantId::default()).await,
        Err(ClientError::Configuration)
    ));
    assert!(matches!(
        valid.reconcile(request(), expected).await,
        Err(ClientError::Configuration)
    ));
}

struct LostReply {
    requests: Mutex<Vec<RequestEnvelope>>,
    always_fail: bool,
}
impl ClientTransport for LostReply {
    fn request<'a>(
        &'a self,
        _route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            assert_eq!(request, &super::tests::request());
            if self.always_fail || requests.len() == 1 {
                return Err(WireError::Timeout);
            }
            Ok(
                request.reply(Response::Submitted(MutationReply::Committed(receipt(
                    request,
                )))),
            )
        })
    }
}
#[test]
fn lost_reply_retries_the_identical_command_and_request_identity() {
    focal_rt::block_on(lost_reply_retries_the_identical_command_and_request_identity_on_a_shard())
        .unwrap();
}
async fn lost_reply_retries_the_identical_command_and_request_identity_on_a_shard() {
    let client = Client::new(
        LostReply {
            requests: Mutex::new(vec![]),
            always_fail: false,
        },
        policy(),
        WireLimits::default(),
        8,
    )
    .unwrap();
    let expected = request();
    let response = client.submit(expected.clone()).await.unwrap();
    assert_eq!(response, MutationReply::Committed(receipt(&expected)));
}
/// Answers `Unavailable` for the first `refusals` requests, then commits.
struct UnavailableThen {
    requests: Mutex<Vec<RequestEnvelope>>,
    refusals: usize,
}
impl ClientTransport for UnavailableThen {
    fn request<'a>(
        &'a self,
        _route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            assert_eq!(request, &super::tests::request());
            if requests.len() <= self.refusals {
                return Ok(request.reply(Response::Error(AccessError::Unavailable)));
            }
            Ok(
                request.reply(Response::Submitted(MutationReply::Committed(receipt(
                    request,
                )))),
            )
        })
    }
}
/// An availability refusal admitted nothing: the identical request is
/// resent with backoff past the attempts kept for lost replies, and one
/// that outlasts the clock is reported as the refusal, never as an unknown
/// outcome or a transport failure.
#[test]
fn availability_refusals_are_resent_within_the_clock_and_reported_as_refusals() {
    focal_rt::block_on(
        availability_refusals_are_resent_within_the_clock_and_reported_as_refusals_on_a_shard(),
    )
    .unwrap();
}
async fn availability_refusals_are_resent_within_the_clock_and_reported_as_refusals_on_a_shard() {
    let client = Client::new(
        UnavailableThen {
            requests: Mutex::new(vec![]),
            refusals: 9,
        },
        RetryPolicy {
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            ..policy()
        },
        WireLimits::default(),
        8,
    )
    .unwrap();
    let expected = request();
    let response = client.submit(expected.clone()).await.unwrap();
    assert_eq!(response, MutationReply::Committed(receipt(&expected)));
    assert_eq!(client.transport().requests.lock().unwrap().len(), 10);
    let short = RetryPolicy {
        max_attempts: 3,
        max_elapsed: Duration::from_millis(200),
        base_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(20),
    };
    let bound = usize::try_from(short.availability_resends()).unwrap();
    let client = Client::new(
        UnavailableThen {
            requests: Mutex::new(vec![]),
            refusals: usize::MAX,
        },
        short,
        WireLimits::default(),
        8,
    )
    .unwrap();
    let error = client.submit(expected.clone()).await.unwrap_err();
    assert!(
        matches!(error, ClientError::Access(AccessError::Unavailable)),
        "{error:?}"
    );
    let sent = client.transport().requests.lock().unwrap().len();
    assert!(sent > 3 && sent <= bound + 1, "{sent} of {bound}");
}
/// An availability refusal is resent for the policy's clock: as many resends as the clock holds
/// at the pauses' expected length, half of each step, which doubles from the base to the cap. The
/// default policy's steps are 20, 40, 80, 160 and 320 ms, then the cap's 500 ms: sixteen pauses
/// expected to take 3.06 s, and 108 more at 250 ms for the clock's other 26.94 s. A fixed 64,
/// drawn over their steps, spanned about 16 s of its 30 s. A policy with no backoff resends as
/// many times as it attempts.
#[test]
fn availability_resends_span_the_policys_clock_at_their_expected_pause() {
    assert_eq!(RetryPolicy::default().availability_resends(), 16 + 108);
    assert_eq!(policy().availability_resends(), policy().max_attempts);
    let one = RetryPolicy {
        max_attempts: 4,
        max_elapsed: Duration::from_millis(10),
        base_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(10),
    };
    // Expected 5 ms a pause: two span the clock.
    assert_eq!(one.availability_resends(), 2);
}
#[test]
fn exhausted_retry_retains_exact_request_and_redacts_error_payload() {
    focal_rt::block_on(
        exhausted_retry_retains_exact_request_and_redacts_error_payload_on_a_shard(),
    )
    .unwrap();
}
async fn exhausted_retry_retains_exact_request_and_redacts_error_payload_on_a_shard() {
    let client = Client::new(
        LostReply {
            requests: Mutex::new(vec![]),
            always_fail: true,
        },
        policy(),
        WireLimits::default(),
        8,
    )
    .unwrap();
    let expected = request();
    let error = client.submit(expected.clone()).await.unwrap_err();
    assert!(!format!("{error:?}").contains("private-progress-payload"));
    let ClientError::OutcomeUnknown { request } = error else {
        panic!("must preserve unknown outcome")
    };
    assert_eq!(*request, expected);
}
struct Redirect {
    seen: Mutex<Vec<(Option<RouteHint>, RequestEnvelope)>>,
}
impl ClientTransport for Redirect {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut seen = self.seen.lock().unwrap();
            seen.push((route.cloned(), request.clone()));
            if route.is_none() {
                Ok(
                    request.reply(Response::Error(AccessError::RouteChanged(RouteHint {
                        epoch: RouteEpoch(2),
                        endpoint: "127.0.0.1:7777".into(),
                        server_name: "localhost".into(),
                    }))),
                )
            } else {
                assert_eq!(request.request_id, RequestId::from_u128(99));
                assert_eq!(request.request_epoch, RequestEpoch(7));
                assert_eq!(request.route_epoch, RouteEpoch(2));
                assert_eq!(request.operation, super::tests::request().operation);
                Ok(
                    request.reply(Response::Submitted(MutationReply::Committed(receipt(
                        request,
                    )))),
                )
            }
        })
    }
}
#[test]
fn route_changes_preserve_retry_identity_and_command() {
    focal_rt::block_on(route_changes_preserve_retry_identity_and_command_on_a_shard()).unwrap();
}
async fn route_changes_preserve_retry_identity_and_command_on_a_shard() {
    let client = Client::new(
        Redirect {
            seen: Mutex::new(vec![]),
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    assert!(matches!(
        client.submit(request()).await.unwrap(),
        MutationReply::Committed(_)
    ));
}

/// A session whose leader moves while the request looks for it: the node
/// asked first names the leader it knew, which has handed leadership on
/// within the same route epoch and names its successor.
struct Handoff {
    seen: std::sync::Arc<Mutex<Vec<Option<String>>>>,
    stale_epoch: bool,
}
impl ClientTransport for Handoff {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut seen = self.seen.lock().unwrap();
            seen.push(route.map(|hint| hint.endpoint.clone()));
            let hint = |epoch: u64, endpoint: &str| {
                Ok(
                    request.reply(Response::Error(AccessError::RouteChanged(RouteHint {
                        epoch: RouteEpoch(epoch),
                        endpoint: endpoint.into(),
                        server_name: "localhost".into(),
                    }))),
                )
            };
            match route.map(|hint| hint.endpoint.as_str()) {
                None => hint(2, "127.0.0.1:7777"),
                Some("127.0.0.1:7777") if self.stale_epoch => hint(1, "127.0.0.1:8888"),
                Some("127.0.0.1:7777") => hint(2, "127.0.0.1:8888"),
                Some(_) => {
                    assert_eq!(request.route_epoch, RouteEpoch(2));
                    Ok(
                        request.reply(Response::Submitted(MutationReply::Committed(receipt(
                            request,
                        )))),
                    )
                }
            }
        })
    }
}
#[test]
fn a_leader_that_moves_within_a_route_epoch_is_followed_and_an_older_epoch_is_refused() {
    focal_rt::block_on(a_leader_that_moves_within_a_route_epoch_is_followed_and_an_older_epoch_is_refused_on_a_shard()).unwrap();
}
async fn a_leader_that_moves_within_a_route_epoch_is_followed_and_an_older_epoch_is_refused_on_a_shard()
 {
    let seen = std::sync::Arc::new(Mutex::new(vec![]));
    let client = Client::new(
        Handoff {
            seen: seen.clone(),
            stale_epoch: false,
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    // The second hint is at the epoch of the first and names another
    // endpoint: the leader moved, and the request follows it there.
    assert!(matches!(
        client.submit(request()).await.unwrap(),
        MutationReply::Committed(_)
    ));
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            None,
            Some("127.0.0.1:7777".to_owned()),
            Some("127.0.0.1:8888".to_owned())
        ]
    );
    // The next request starts where the leader was found.
    assert!(matches!(
        client.submit(request()).await.unwrap(),
        MutationReply::Committed(_)
    ));
    assert_eq!(
        seen.lock().unwrap().last().unwrap().as_deref(),
        Some("127.0.0.1:8888")
    );
    // A hint from an older epoch than the one held is not a route: it is
    // not followed, and the write it answered is reported as the unknown
    // outcome an invalid reply to a mutation is.
    let seen = std::sync::Arc::new(Mutex::new(vec![]));
    let client = Client::new(
        Handoff {
            seen: seen.clone(),
            stale_epoch: true,
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    let error = client.submit(request()).await.unwrap_err();
    assert!(
        matches!(error, ClientError::OutcomeUnknown { .. }),
        "{error:?}"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![None, Some("127.0.0.1:7777".to_owned())]
    );
}

/// How a client's home answers its reads in [`Homed`].
#[derive(Clone, Copy)]
enum HomeReads {
    Serve,
    Behind,
    Unavailable,
    /// Served at route epoch 2; a read at an older one is sent to the leader at 2.
    Stale,
}
/// Each exchange a [`Homed`] session took: (endpoint, mutation, route epoch).
type Exchanges = std::sync::Arc<Mutex<Vec<(Option<String>, bool, u64)>>>;
/// A session seen from a client homed on one of its followers (route `None`): the follower
/// serves reads as `reads` says and sends every write to the leader at 127.0.0.1:7777, which
/// serves both.
struct Homed {
    seen: Exchanges,
    reads: HomeReads,
}
const LEADER: &str = "127.0.0.1:7777";
fn leader_at(epoch: u64) -> RouteHint {
    RouteHint {
        epoch: RouteEpoch(epoch),
        endpoint: LEADER.into(),
        server_name: "localhost".into(),
    }
}
fn native_read() -> RequestEnvelope {
    let base = request();
    RequestEnvelope {
        protocol: NATIVE_PROTOCOL_VERSION,
        operation: Operation::NativeRead(NativeReadRequest {
            consistency: ReadConsistency::AtLeast(ReadToken {
                ledger: base.ledger,
                sequence: SessionSeq(12),
                route_epoch: RouteEpoch(1),
            }),
            query: NativeReadQuery::Objects(Vec::new()),
            max_items: 1,
        }),
        ..base
    }
}
fn page(request: &RequestEnvelope) -> ResponseEnvelope {
    request.reply(Response::NativeRead(NativeReadPage {
        token: ReadToken {
            ledger: request.ledger,
            sequence: SessionSeq(12),
            route_epoch: request.route_epoch,
        },
        native_sequence: SessionSeq(12),
        logical_time: 0,
        objects: Vec::new(),
        next: None,
        visited: 0,
    }))
}
impl ClientTransport for Homed {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mutation = request.operation.is_mutation();
            self.seen.lock().unwrap().push((
                route.map(|hint| hint.endpoint.clone()),
                mutation,
                request.route_epoch.0,
            ));
            let refused = |error| Ok(request.reply(Response::Error(error)));
            match (route.map(|hint| hint.endpoint.as_str()), mutation) {
                (None, true) => refused(AccessError::RouteChanged(leader_at(1))),
                (None, false) => match self.reads {
                    HomeReads::Serve => Ok(page(request)),
                    HomeReads::Behind => refused(AccessError::Behind {
                        published: SessionSeq(11),
                    }),
                    HomeReads::Unavailable => refused(AccessError::Unavailable),
                    HomeReads::Stale if request.route_epoch < RouteEpoch(2) => {
                        refused(AccessError::RouteChanged(leader_at(2)))
                    }
                    HomeReads::Stale => Ok(page(request)),
                },
                (Some(LEADER), true) => Ok(request.reply(Response::Submitted(
                    MutationReply::Committed(receipt(request)),
                ))),
                (Some(LEADER), false) => Ok(page(request)),
                (Some(other), _) => panic!("asked {other}"),
            }
        })
    }
}
fn homed(reads: HomeReads, route: ReadRoute) -> (Client<Homed>, Exchanges) {
    let seen = std::sync::Arc::new(Mutex::new(vec![]));
    let client = Client::new(
        Homed {
            seen: seen.clone(),
            reads,
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap()
    .with_read_route(route);
    (client, seen)
}
fn at(endpoint: Option<&str>, mutation: bool, epoch: u64) -> (Option<String>, bool, u64) {
    (endpoint.map(str::to_owned), mutation, epoch)
}

/// A client homed on a follower reads at its home and writes at the leader its first write
/// found; one that reads where it writes (the default) follows its writes there.
#[test]
fn a_homed_client_reads_at_home_and_writes_at_the_leader() {
    focal_rt::block_on(a_homed_client_reads_at_home_and_writes_at_the_leader_on_a_shard()).unwrap();
}
async fn a_homed_client_reads_at_home_and_writes_at_the_leader_on_a_shard() {
    let (client, seen) = homed(HomeReads::Serve, ReadRoute::Home);
    assert!(matches!(
        client.submit(request()).await.unwrap(),
        MutationReply::Committed(_)
    ));
    client.request(native_read()).await.unwrap();
    assert!(matches!(
        client.submit(request()).await.unwrap(),
        MutationReply::Committed(_)
    ));
    client.request(native_read()).await.unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            at(None, true, 1),
            at(Some(LEADER), true, 1),
            at(None, false, 1),
            at(Some(LEADER), true, 1),
            at(None, false, 1),
        ]
    );
    let (client, seen) = homed(HomeReads::Serve, ReadRoute::Leader);
    client.submit(request()).await.unwrap();
    client.request(native_read()).await.unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            at(None, true, 1),
            at(Some(LEADER), true, 1),
            at(Some(LEADER), false, 1),
        ]
    );
}

/// A homed read its home cannot answer (behind its token, unavailable) is asked of the leader
/// its writes found, once and at once; with no leader found, its home's answer stands.
#[test]
fn a_homed_read_its_home_cannot_answer_is_asked_of_the_leader_once() {
    focal_rt::block_on(
        a_homed_read_its_home_cannot_answer_is_asked_of_the_leader_once_on_a_shard(),
    )
    .unwrap();
}
async fn a_homed_read_its_home_cannot_answer_is_asked_of_the_leader_once_on_a_shard() {
    for reads in [HomeReads::Behind, HomeReads::Unavailable] {
        let (client, seen) = homed(reads, ReadRoute::Home);
        client.submit(request()).await.unwrap();
        client.request(native_read()).await.unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                at(None, true, 1),
                at(Some(LEADER), true, 1),
                at(None, false, 1),
                at(Some(LEADER), false, 1),
            ]
        );
    }
    let (client, seen) = homed(HomeReads::Behind, ReadRoute::Home);
    let error = client.request(native_read()).await.unwrap_err();
    assert!(
        matches!(error, ClientError::Access(AccessError::Behind { .. })),
        "{error:?}"
    );
    assert_eq!(*seen.lock().unwrap(), vec![at(None, false, 1)]);
}

/// A homed read its home answers with a newer epoch is asked at home again at that epoch, and
/// the next reads take it from the start, where the hint's leader is where the writes go.
#[test]
fn a_homed_read_at_a_stale_epoch_is_asked_at_home_again_at_the_newer_one() {
    focal_rt::block_on(
        a_homed_read_at_a_stale_epoch_is_asked_at_home_again_at_the_newer_one_on_a_shard(),
    )
    .unwrap();
}
async fn a_homed_read_at_a_stale_epoch_is_asked_at_home_again_at_the_newer_one_on_a_shard() {
    let (client, seen) = homed(HomeReads::Stale, ReadRoute::Home);
    client.request(native_read()).await.unwrap();
    client.request(native_read()).await.unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        vec![at(None, false, 1), at(None, false, 2), at(None, false, 2)]
    );
}

struct LostAppendReply {
    first: Mutex<Option<RequestEnvelope>>,
    always_fail: bool,
}
impl ClientTransport for LostAppendReply {
    fn request<'a>(
        &'a self,
        _route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut first = self.first.lock().unwrap();
            if let Some(original) = &*first {
                assert_eq!(
                    original, request,
                    "retry must preserve offset and content bytes"
                );
            } else {
                *first = Some(request.clone());
                return Err(WireError::Timeout);
            }
            if self.always_fail {
                return Err(WireError::Timeout);
            }
            let Operation::Upload(UploadRequest::Append { offset, bytes, .. }) = &request.operation
            else {
                panic!("expected append")
            };
            Ok(request.reply(Response::Upload(UploadReply::Offset(
                offset + bytes.len() as u64,
            ))))
        })
    }
}

#[test]
fn upload_retry_preserves_offset_bytes_and_unknown_outcome() {
    focal_rt::block_on(upload_retry_preserves_offset_bytes_and_unknown_outcome_on_a_shard())
        .unwrap();
}
async fn upload_retry_preserves_offset_bytes_and_unknown_outcome_on_a_shard() {
    for always_fail in [false, true] {
        let client = Client::new(
            LostAppendReply {
                first: Mutex::new(None),
                always_fail,
            },
            policy(),
            WireLimits::default(),
            8,
        )
        .unwrap();
        let mut expected = request();
        expected.operation = Operation::Upload(UploadRequest::Append {
            upload: [9; 16],
            offset: 123,
            bytes: b"immutable evidence chunk".to_vec(),
        });
        let result = client.upload(expected.clone()).await;
        if always_fail {
            let ClientError::OutcomeUnknown { request } = result.unwrap_err() else {
                panic!("upload outcome must remain unknown")
            };
            assert_eq!(*request, expected);
        } else {
            assert_eq!(result.unwrap(), UploadReply::Offset(147));
        }
    }
}

#[test]
fn unrepresentable_retry_deadlines_are_rejected_before_scheduling() {
    let policy = RetryPolicy {
        max_elapsed: std::time::Duration::MAX,
        ..Default::default()
    };
    struct Unused;
    impl ClientTransport for Unused {
        fn request<'a>(
            &'a self,
            _: Option<&'a RouteHint>,
            _: &'a RequestEnvelope,
        ) -> TransportFuture<'a> {
            Box::pin(async { Err(focal_wire::WireError::Connection) })
        }
    }
    assert!(matches!(
        Client::new(Unused, policy, WireLimits::default(), 1),
        Err(ClientError::Configuration)
    ));
    use std::future::Future;
    let client = Client::new(Unused, RetryPolicy::default(), WireLimits::default(), 1).unwrap();
    let future = client.request(request());
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(ClientError::Transport))
    ));
}

#[path = "retry_uncertainty_tests.rs"]
mod retry_uncertainty;

// A transport that answers every request with a committed receipt, so the trace
// hook sees a definite `Committed` outcome.
struct AlwaysCommits;
impl ClientTransport for AlwaysCommits {
    fn request<'a>(
        &'a self,
        _route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            Ok(
                request.reply(Response::Submitted(MutationReply::Committed(receipt(
                    request,
                )))),
            )
        })
    }
}

// A transport that refuses with an access error, so the trace hook sees a
// definite `Refused` outcome.
struct AlwaysRefuses;
impl ClientTransport for AlwaysRefuses {
    fn request<'a>(
        &'a self,
        _route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move { Ok(request.reply(Response::Error(AccessError::Unauthorized))) })
    }
}

#[derive(Default)]
struct RecordingSink {
    entries: Mutex<Vec<TraceEntry>>,
}
impl TraceSink for RecordingSink {
    fn record(&self, entry: TraceEntry) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.push(entry);
        }
    }
}

#[test]
fn trace_records_committed_exchange_with_envelope_identity() {
    focal_rt::block_on(trace_records_committed_exchange_with_envelope_identity_on_a_shard())
        .unwrap();
}
async fn trace_records_committed_exchange_with_envelope_identity_on_a_shard() {
    let sink = std::sync::Arc::new(RecordingSink::default());
    struct Shared(std::sync::Arc<RecordingSink>);
    impl TraceSink for Shared {
        fn record(&self, entry: TraceEntry) {
            self.0.record(entry);
        }
    }
    let client = Client::new(AlwaysCommits, policy(), WireLimits::default(), 1)
        .unwrap()
        .with_trace(Box::new(Shared(std::sync::Arc::clone(&sink))));
    let req = request();
    let response = client.request(req.clone()).await.unwrap();
    assert!(matches!(
        response.result,
        Response::Submitted(MutationReply::Committed(_))
    ));
    let entries = sink.entries.lock().unwrap();
    assert_eq!(entries.len(), 1, "exactly one exchange traced");
    let entry = &entries[0];
    assert_eq!(entry.call, 0, "first call numbered zero");
    assert_eq!(entry.ledger, req.ledger);
    assert_eq!(entry.request_epoch, req.request_epoch);
    assert_eq!(entry.request_id, req.request_id);
    assert!(entry.mutation, "Submit is a mutation");
    assert!(
        entry.completed_nanos >= entry.invoked_nanos,
        "monotone stamps"
    );
    assert_eq!(
        entry.outcome,
        TraceOutcome::Committed {
            sequence: SessionSeq(12),
            command_hash: ContentHash([9; 32]),
        }
    );
}

#[test]
fn trace_numbers_calls_monotonically_and_records_refusals() {
    focal_rt::block_on(trace_numbers_calls_monotonically_and_records_refusals_on_a_shard())
        .unwrap();
}
async fn trace_numbers_calls_monotonically_and_records_refusals_on_a_shard() {
    let sink = std::sync::Arc::new(RecordingSink::default());
    struct Shared(std::sync::Arc<RecordingSink>);
    impl TraceSink for Shared {
        fn record(&self, entry: TraceEntry) {
            self.0.record(entry);
        }
    }
    let client = Client::new(AlwaysRefuses, policy(), WireLimits::default(), 1)
        .unwrap()
        .with_trace(Box::new(Shared(std::sync::Arc::clone(&sink))));
    // Two reads: refusals leave nothing admitted and must not become Unknown.
    let read = RequestEnvelope {
        operation: Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::Objects(vec![ObjectRef::claim(
                request().ledger,
                ClaimId::from_u128(5),
            )]),
            max_items: 1,
        }),
        ..request()
    };
    // A refused read surfaces as an error to the caller, but the exchange is
    // still traced (as a definite `Refused`).
    assert!(matches!(
        client.request(read.clone()).await,
        Err(ClientError::Access(_))
    ));
    assert!(matches!(
        client.request(read).await,
        Err(ClientError::Access(_))
    ));
    let entries = sink.entries.lock().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].call, 0);
    assert_eq!(entries[1].call, 1);
    assert!(!entries[0].mutation, "Read is not a mutation");
    assert_eq!(entries[0].outcome, TraceOutcome::Refused);
    assert_eq!(entries[1].outcome, TraceOutcome::Refused);
}

#[test]
fn untraced_client_needs_no_sink() {
    focal_rt::block_on(untraced_client_needs_no_sink_on_a_shard()).unwrap();
}
async fn untraced_client_needs_no_sink_on_a_shard() {
    // The default client has no trace; requests still complete normally.
    let client = Client::new(AlwaysCommits, policy(), WireLimits::default(), 1).unwrap();
    let response = client.request(request()).await.unwrap();
    assert!(matches!(
        response.result,
        Response::Submitted(MutationReply::Committed(_))
    ));
}

// Finding 2 (audit 2026-09-13): a Duplicate domain reply is a committed-on-retry
// proof; it must trace as Committed, not Unknown.
#[test]
fn a_duplicate_domain_reply_traces_as_committed() {
    focal_rt::block_on(a_duplicate_domain_reply_traces_as_committed_on_a_shard()).unwrap();
}
async fn a_duplicate_domain_reply_traces_as_committed_on_a_shard() {
    struct DuplicateReply;
    impl ClientTransport for DuplicateReply {
        fn request<'a>(
            &'a self,
            _: Option<&'a RouteHint>,
            request: &'a RequestEnvelope,
        ) -> TransportFuture<'a> {
            Box::pin(async move {
                Ok(request.reply(Response::Submitted(MutationReply::Domain(
                    DomainOutcome::Duplicate(Box::new(receipt(request))),
                ))))
            })
        }
    }
    struct Shared(std::sync::Arc<RecordingSink>);
    impl TraceSink for Shared {
        fn record(&self, entry: TraceEntry) {
            self.0.record(entry);
        }
    }
    let sink = std::sync::Arc::new(RecordingSink::default());
    let client = Client::new(DuplicateReply, policy(), WireLimits::default(), 1)
        .unwrap()
        .with_trace(Box::new(Shared(std::sync::Arc::clone(&sink))));
    let reply = client.submit(request()).await.unwrap();
    assert!(matches!(
        reply,
        MutationReply::Domain(DomainOutcome::Duplicate(_))
    ));
    let entries = sink.entries.lock().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].outcome,
        TraceOutcome::Committed {
            sequence: SessionSeq(12),
            command_hash: ContentHash([9; 32]),
        }
    );
}

/// A session whose leader is killed, seen from a client homed on one of its followers (route
/// `None`): the home names the leader 127.0.0.1:7777 until it is killed and its successor
/// 127.0.0.1:8888 after; the killed leader answers nothing (its connection fails); the successor
/// commits. A second route (`routes`), where `second` is set, is the session's other follower,
/// which names the leader as the home does; `home_killed` kills the home with the leader.
/// Each request a [`Killed`] session took: (endpoint, request id).
type Sent = std::sync::Arc<Mutex<Vec<(Option<String>, u128)>>>;
struct Killed {
    seen: Sent,
    killed: std::sync::atomic::AtomicBool,
    home_killed: bool,
    second: Vec<RouteHint>,
}
const KILLED_LEADER: &str = "127.0.0.1:7777";
const NEW_LEADER: &str = "127.0.0.1:8888";
fn route_to(endpoint: &str) -> RouteHint {
    RouteHint {
        epoch: RouteEpoch(1),
        endpoint: endpoint.into(),
        server_name: "localhost".into(),
    }
}
impl ClientTransport for Killed {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let endpoint = route.map(|hint| hint.endpoint.clone());
            self.seen
                .lock()
                .unwrap()
                .push((endpoint.clone(), u128::from_be_bytes(request.request_id.0)));
            let killed = self.killed.load(std::sync::atomic::Ordering::SeqCst);
            let leader = if killed { NEW_LEADER } else { KILLED_LEADER };
            match endpoint.as_deref() {
                None if killed && self.home_killed => Err(WireError::Connection),
                None | Some("127.0.0.1:7100") => {
                    Ok(request.reply(Response::Error(AccessError::RouteChanged(route_to(leader)))))
                }
                Some(KILLED_LEADER) if killed => Err(WireError::Connection),
                Some(_) => Ok(request.reply(Response::Submitted(MutationReply::Committed(
                    receipt(request),
                )))),
            }
        })
    }
    fn routes(&self) -> &[RouteHint] {
        &self.second
    }
}

/// A write whose leader was killed goes where the session leads now: the killed leader's route is
/// held failed, the client's home names the successor, and the write, resent there as the same
/// request, commits; the next write goes to the successor at once. Every request kept its route,
/// so the write was resent to the killed leader until its attempts ran out and reported unknown
/// though the session had another leader (twelve of 33 nodes killed, 2026-10-29).
#[test]
fn a_write_whose_leader_was_killed_is_resent_where_the_session_leads_now() {
    focal_rt::block_on(
        a_write_whose_leader_was_killed_is_resent_where_the_session_leads_now_on_a_shard(),
    )
    .unwrap();
}
async fn a_write_whose_leader_was_killed_is_resent_where_the_session_leads_now_on_a_shard() {
    let seen = std::sync::Arc::new(Mutex::new(vec![]));
    let client = Client::new(
        Killed {
            seen: seen.clone(),
            killed: std::sync::atomic::AtomicBool::new(false),
            home_killed: false,
            second: Vec::new(),
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    assert!(matches!(
        client.submit(request()).await.unwrap(),
        MutationReply::Committed(_)
    ));
    client
        .transport()
        .killed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    seen.lock().unwrap().clear();
    let killed = RequestEnvelope {
        request_id: RequestId::from_u128(100),
        ..request()
    };
    assert!(matches!(
        client.submit(killed).await.unwrap(),
        MutationReply::Committed(_)
    ));
    // Sent to the killed leader, then home, then to the successor: one request throughout.
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            (Some(KILLED_LEADER.to_owned()), 100),
            (None, 100),
            (Some(NEW_LEADER.to_owned()), 100)
        ]
    );
    seen.lock().unwrap().clear();
    assert!(matches!(
        client.submit(request()).await.unwrap(),
        MutationReply::Committed(_)
    ));
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(Some(NEW_LEADER.to_owned()), 99)]
    );
}

/// A client whose home is killed with its session's leader is resent by the transport's next
/// route, the session's other follower, which names the successor: given one route, the client
/// could ask no node that knew where the session leads.
#[test]
fn a_write_whose_home_and_leader_were_killed_is_resent_by_the_next_route() {
    focal_rt::block_on(
        a_write_whose_home_and_leader_were_killed_is_resent_by_the_next_route_on_a_shard(),
    )
    .unwrap();
}
async fn a_write_whose_home_and_leader_were_killed_is_resent_by_the_next_route_on_a_shard() {
    let seen = std::sync::Arc::new(Mutex::new(vec![]));
    let home = route_to("127.0.0.1:7000");
    let client = Client::new(
        Killed {
            seen: seen.clone(),
            killed: std::sync::atomic::AtomicBool::new(true),
            home_killed: true,
            second: vec![home, route_to("127.0.0.1:7100")],
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    assert!(matches!(
        client.submit(request()).await.unwrap(),
        MutationReply::Committed(_)
    ));
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            (None, 99),
            (Some("127.0.0.1:7100".to_owned()), 99),
            (Some(NEW_LEADER.to_owned()), 99)
        ]
    );
}

/// A client homed on a node that is gone, its transport given that home and the session's other
/// voter: the home answers nothing while `home_up` is unset, the other voter answers every read. A
/// reach of the home waits on a dial that never ends while it is down, as a dial to a killed node
/// waits out its idle timeout, and is answered once it is up.
struct Downed {
    seen: Sent,
    reaches: Mutex<Vec<String>>,
    home_up: std::sync::atomic::AtomicBool,
    given: Vec<RouteHint>,
}
const DOWNED_HOME: &str = "127.0.0.1:7000";
const OTHER_VOTER: &str = "127.0.0.1:7100";
impl ClientTransport for Downed {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let endpoint = route.map(|hint| hint.endpoint.clone());
            self.seen
                .lock()
                .unwrap()
                .push((endpoint.clone(), u128::from_be_bytes(request.request_id.0)));
            let up = self.home_up.load(std::sync::atomic::Ordering::SeqCst);
            match endpoint.as_deref() {
                None | Some(DOWNED_HOME) if !up => Err(WireError::Connection),
                _ => Ok(page(request)),
            }
        })
    }
    fn routes(&self) -> &[RouteHint] {
        &self.given
    }
    fn reach<'a>(&'a self, route: &'a RouteHint) -> ReachFuture<'a> {
        Box::pin(async move {
            self.reaches.lock().unwrap().push(route.endpoint.clone());
            if route.endpoint == DOWNED_HOME
                && !self.home_up.load(std::sync::atomic::Ordering::SeqCst)
            {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
}

/// A read whose home gave no answer is resent by the session's other voter, and every read after
/// it goes there first until a reach of the home is answered, no read waiting on that reach;
/// then reads go home again. Every request went to its route whatever the last one sent there
/// found, so a client homed on a killed node sent each read there first and waited out its
/// connection's idle timeout before the next route answered (twelve of 33 nodes killed,
/// 2026-10-29: sessions that lost a follower went 7.3 to 7.9 s without a commit).
#[test]
fn a_read_whose_home_gave_no_answer_goes_to_the_next_route_until_its_home_is_reached() {
    focal_rt::block_on(
        a_read_whose_home_gave_no_answer_goes_to_the_next_route_until_its_home_is_reached_on_a_shard(),
    )
    .unwrap();
}
async fn a_read_whose_home_gave_no_answer_goes_to_the_next_route_until_its_home_is_reached_on_a_shard()
 {
    let seen = std::sync::Arc::new(Mutex::new(vec![]));
    let client = Client::new(
        Downed {
            seen: seen.clone(),
            reaches: Mutex::new(Vec::new()),
            home_up: std::sync::atomic::AtomicBool::new(false),
            given: vec![route_to(DOWNED_HOME), route_to(OTHER_VOTER)],
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap()
    .with_read_route(ReadRoute::Home);
    let read = |id| RequestEnvelope {
        request_id: RequestId::from_u128(id),
        ..native_read()
    };
    // The home gives no answer: the read is resent by the other voter.
    client.request(read(1)).await.unwrap();
    // The home is passed over and reached beside the next read, which does not wait on it.
    client.request(read(2)).await.unwrap();
    client
        .transport()
        .home_up
        .store(true, std::sync::atomic::Ordering::SeqCst);
    // A reach of the home is answered beside the third read, and the fourth goes home.
    client.request(read(3)).await.unwrap();
    client.request(read(4)).await.unwrap();
    let other = || Some(OTHER_VOTER.to_owned());
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            (None, 1),
            (other(), 1),
            (other(), 2),
            (other(), 3),
            (None, 4)
        ]
    );
    assert_eq!(
        *client.transport().reaches.lock().unwrap(),
        vec![DOWNED_HOME.to_owned(), DOWNED_HOME.to_owned()]
    );
}

/// A client given one route that gave no answer sends its next request there, with no reach beside
/// it: the request reaches it itself, where a reach would wait on the same dial and take a place
/// among its waiters (a transport of one connection refused the request `Capacity`). Once the
/// route answers, it is no longer passed over.
#[test]
fn a_request_to_its_only_route_that_gave_no_answer_reaches_it_itself() {
    focal_rt::block_on(
        a_request_to_its_only_route_that_gave_no_answer_reaches_it_itself_on_a_shard(),
    )
    .unwrap();
}
async fn a_request_to_its_only_route_that_gave_no_answer_reaches_it_itself_on_a_shard() {
    let seen = std::sync::Arc::new(Mutex::new(vec![]));
    let client = Client::new(
        Downed {
            seen: seen.clone(),
            reaches: Mutex::new(Vec::new()),
            home_up: std::sync::atomic::AtomicBool::new(false),
            given: vec![route_to(DOWNED_HOME)],
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap()
    .with_read_route(ReadRoute::Home);
    let read = |id| RequestEnvelope {
        request_id: RequestId::from_u128(id),
        ..native_read()
    };
    assert!(client.request(read(1)).await.is_err());
    assert!(client.request(read(2)).await.is_err());
    client
        .transport()
        .home_up
        .store(true, std::sync::atomic::Ordering::SeqCst);
    client.request(read(3)).await.unwrap();
    client.request(read(4)).await.unwrap();
    let attempts = usize::try_from(policy().max_attempts).unwrap();
    let mut sent = vec![(None, 1); attempts];
    sent.extend(vec![(None, 2); attempts]);
    sent.extend([(None, 3), (None, 4)]);
    assert_eq!(*seen.lock().unwrap(), sent);
    assert!(client.transport().reaches.lock().unwrap().is_empty());
}
