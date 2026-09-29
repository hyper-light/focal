use super::*;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[path = "managed_protocol_tests.rs"]
mod managed_protocol_tests;
#[path = "managed_tests.rs"]
mod managed_tests;
#[path = "peer_mutations_tests.rs"]
mod peer_mutations_tests;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn grant() -> PeerGrant {
    PeerGrant {
        principal: ParticipantId::from_u128(3),
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Actor,
    }
}
fn request(id: u128) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::Scan { after: None },
            max_items: 10,
        }),
    }
}
fn response(request: &RequestEnvelope) -> ResponseEnvelope {
    request.reply(Response::Read(ReadPage {
        token: ReadToken {
            ledger: request.ledger,
            sequence: SessionSeq(7),
            route_epoch: request.route_epoch,
        },
        objects: vec![],
        next: None,
    }))
}
struct Pki {
    ca: Certificate,
    key: KeyPair,
}
impl Pki {
    fn new() -> Self {
        let mut params = CertificateParams::new(vec![]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap();
        Self { ca, key }
    }
    fn issue(&self, server: bool) -> (Vec<u8>, Vec<u8>) {
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.extended_key_usages = vec![if server {
            ExtendedKeyUsagePurpose::ServerAuth
        } else {
            ExtendedKeyUsagePurpose::ClientAuth
        }];
        let key = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &self.ca, &self.key).unwrap();
        (certificate.der().to_vec(), key.serialize_der())
    }
}
fn limits() -> WireLimits {
    WireLimits {
        request_timeout: Duration::from_secs(3),
        ..Default::default()
    }
}

#[derive(Clone)]
struct AccountedDownload {
    budget: MemoryBudget,
}
impl RequestHandler for AccountedDownload {
    fn handle<'a>(&'a self, _request: &'a VerifiedRequest) -> HandlerFuture<'a> {
        panic!("network dispatch must preserve the accounted handler override")
    }
    fn handle_accounted<'a>(&'a self, request: &'a VerifiedRequest) -> OwnedHandlerFuture<'a> {
        Box::pin(async move {
            let Operation::Download {
                content,
                offset,
                max_bytes,
            } = &request.request().operation
            else {
                panic!("download expected")
            };
            let allocation = self
                .budget
                .reserve(
                    BudgetKind::Query,
                    BudgetLane::Ordinary,
                    (*max_bytes as usize) * 3 + 4096,
                )
                .unwrap()
                .commit();
            let response = request.request().reply(Response::Content(ContentChunk {
                offset: *offset,
                eof: u64::from(*max_bytes) == content.length,
                bytes: vec![7; *max_bytes as usize],
            }));
            OwnedResponse::accounted(response, allocation)
        })
    }
}
fn download_request(bytes: u32) -> RequestEnvelope {
    RequestEnvelope {
        operation: Operation::Download {
            content: ContentRef {
                domain: ContentDomainId(ledger().tenant.0),
                root: ContentHash([9; 32]),
                length: u64::from(bytes),
                class: ContentClass::Evidence,
            },
            offset: 0,
            max_bytes: bytes,
        },
        ..request(1)
    }
}

#[tokio::test]
async fn accounted_dispatch_forwards_dynamic_handler_and_retains_both_allowances() {
    let budget = MemoryBudget::new(4 * 1024 * 1024, 1024).unwrap();
    let handler: Arc<dyn RequestHandler> = Arc::new(AccountedDownload {
        budget: budget.clone(),
    });
    let response = dispatch_accounted(
        &handler,
        AuthenticatedPeer::local(grant()).unwrap(),
        download_request(4096),
        &limits(),
    )
    .await;
    assert!(matches!(response.envelope().result, Response::Content(_)));
    assert_eq!(budget.stats().used, 4096 * 4);
    drop(response);
    assert_eq!(budget.stats().used, 0);
    let first = budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, 4096)
        .unwrap()
        .commit();
    let second = budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, 8192)
        .unwrap()
        .commit();
    let response = OwnedResponse::accounted_pair(
        request(1).reply(Response::Error(AccessError::Unavailable)),
        first,
        Some(second),
    );
    assert_eq!(budget.stats().used, 12288);
    drop(response);
    assert_eq!(budget.stats().used, 0);
}

#[cfg(unix)]
#[tokio::test]
async fn unix_slow_response_keeps_output_charge_until_write_finishes_or_disconnects() {
    use tokio::{io::AsyncWriteExt, net::UnixStream};
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("accounted.sock");
    let budget = MemoryBudget::new(4 * 1024 * 1024, 1024).unwrap();
    let server = Arc::new(UnixServer::bind(&socket, grant(), limits()).unwrap());
    let serving = server.clone();
    let handler: Arc<dyn RequestHandler> = Arc::new(AccountedDownload {
        budget: budget.clone(),
    });
    let task = tokio::spawn(async move { serving.serve(handler).await });
    for disconnect in [false, true] {
        let mut stream = UnixStream::connect(&socket).await.unwrap();
        write_frame(
            &mut stream,
            FrameKind::Hello,
            &Hello {
                versions: vec![PROTOCOL_VERSION],
                max_frame_bytes: limits().max_frame_bytes,
                max_items: limits().max_items,
            },
            4096,
        )
        .await
        .unwrap();
        let _: HelloReply = read_frame(&mut stream, FrameKind::HelloReply, 4096)
            .await
            .unwrap();
        write_frame(
            &mut stream,
            FrameKind::Request,
            &download_request(900 * 1024),
            limits().max_frame_bytes,
        )
        .await
        .unwrap();
        stream.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.stats().used == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(budget.stats().used >= 900 * 1024 * 3);
        if !disconnect {
            let response: ResponseEnvelope =
                read_frame(&mut stream, FrameKind::Response, limits().max_frame_bytes)
                    .await
                    .unwrap();
            assert!(
                matches!(response.result, Response::Content(ContentChunk { bytes, .. }) if bytes.len() == 900 * 1024)
            );
        }
        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.stats().used != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    server.close();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn quic_flow_control_keeps_response_permit_until_ack_or_connection_loss() {
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(4).unwrap();
    registry
        .register_certificate(&certificate, grant())
        .unwrap();
    let budget = MemoryBudget::new(4 * 1024 * 1024, 1024).unwrap();
    let handler: Arc<dyn RequestHandler> = Arc::new(AccountedDownload {
        budget: budget.clone(),
    });
    let (server, task) = server(&pki, registry, handler).await;
    let mut tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &limits(),
    )
    .unwrap();
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window(4096u32.into());
    transport.receive_window(8192u32.into());
    tls.transport_config(Arc::new(transport));
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(tls);
    for disconnect in [false, true] {
        let connection = endpoint
            .connect(server.local_addr().unwrap(), "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, mut receive) = connection.open_bi().await.unwrap();
        write_frame(
            &mut send,
            FrameKind::Hello,
            &Hello {
                versions: vec![PROTOCOL_VERSION],
                max_frame_bytes: limits().max_frame_bytes,
                max_items: limits().max_items,
            },
            4096,
        )
        .await
        .unwrap();
        send.finish().unwrap();
        let _: HelloReply = read_frame(&mut receive, FrameKind::HelloReply, 4096)
            .await
            .unwrap();
        require_end(&mut receive).await.unwrap();
        let (mut send, mut receive) = connection.open_bi().await.unwrap();
        write_frame(
            &mut send,
            FrameKind::Request,
            &download_request(900 * 1024),
            limits().max_frame_bytes,
        )
        .await
        .unwrap();
        send.finish().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.stats().used == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(budget.stats().used >= 900 * 1024 * 3);
        if !disconnect {
            let response: ResponseEnvelope =
                read_frame(&mut receive, FrameKind::Response, limits().max_frame_bytes)
                    .await
                    .unwrap();
            assert!(
                matches!(response.result, Response::Content(ContentChunk { bytes, .. }) if bytes.len() == 900 * 1024)
            );
            require_end(&mut receive).await.unwrap();
        } else {
            connection.close(
                0u8.into(),
                b"test disconnect while response is flow controlled",
            );
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while budget.stats().used != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        connection.close(0u8.into(), b"done");
    }
    server.close();
    task.await.unwrap().unwrap();
}
async fn server(
    pki: &Pki,
    registry: PeerRegistry,
    handler: Arc<dyn RequestHandler>,
) -> (
    Arc<QuicServer>,
    tokio::task::JoinHandle<Result<(), WireError>>,
) {
    let (certificate, key) = pki.issue(true);
    let tls = server_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &limits(),
    )
    .unwrap();
    let server = Arc::new(
        QuicServer::bind("127.0.0.1:0".parse().unwrap(), tls, registry, limits()).unwrap(),
    );
    let running = server.clone();
    let task = tokio::spawn(async move { running.serve(handler).await });
    (server, task)
}
fn connector(pki: &Pki, cert: Vec<u8>, key: Vec<u8>) -> QuicConnector {
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![cert], key),
        vec![pki.ca.der().to_vec()],
        &limits(),
    )
    .unwrap();
    QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, limits()).unwrap()
}

#[tokio::test]
async fn mutual_tls_tenant_isolation_live_revocation_and_independent_streams() {
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let fingerprint = registry
        .register_certificate(&certificate, grant())
        .unwrap();
    let slow_started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let handler: Arc<dyn RequestHandler> = {
        let started = slow_started.clone();
        let release = release.clone();
        let calls = calls.clone();
        Arc::new(move |verified: VerifiedRequest| {
            let started = started.clone();
            let release = release.clone();
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if verified.request().request_id == RequestId::from_u128(1) {
                    started.notify_one();
                    release.notified().await;
                }
                response(verified.request())
            }
        })
    };
    let (server, task) = server(&pki, registry.clone(), handler).await;
    let connector = connector(&pki, certificate, key);
    let remote = connector
        .connect(server.local_addr().unwrap(), "localhost")
        .await
        .unwrap();
    let slow_remote = remote.clone();
    let slow = tokio::spawn(async move { slow_remote.request(&request(1)).await });
    slow_started.notified().await;
    let fast = tokio::time::timeout(Duration::from_secs(1), remote.request(&request(2)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fast, response(&request(2)));
    release.notify_one();
    slow.await.unwrap().unwrap();
    let mut foreign = request(3);
    foreign.ledger.tenant = TenantId::from_u128(999);
    assert_eq!(
        remote.request(&foreign).await.unwrap().result,
        Response::Error(AccessError::Unauthorized)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    registry.revoke(fingerprint).unwrap();
    assert!(remote.request(&request(4)).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.close();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn certificates_outside_the_trusted_ca_never_dispatch() {
    let pki = Pki::new();
    let rogue = Pki::new();
    let (certificate, key) = rogue.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    registry
        .register_certificate(&certificate, grant())
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        observed.fetch_add(1, Ordering::SeqCst);
        async move { response(verified.request()) }
    });
    let (server, task) = server(&pki, registry, handler).await;
    let connector = connector(&pki, certificate, key);
    assert!(
        connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    server.close();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn oversized_header_is_rejected_before_waiting_for_or_allocating_payload() {
    use tokio::io::AsyncWriteExt;
    let (mut writer, mut reader) = tokio::io::duplex(64);
    let mut header = [0; HEADER_BYTES];
    header[..8].copy_from_slice(b"FOCALQ01");
    header[8..10].copy_from_slice(&1u16.to_be_bytes());
    header[10..12].copy_from_slice(&(FrameKind::Request as u16).to_be_bytes());
    header[12..].copy_from_slice(&u32::MAX.to_be_bytes());
    writer.write_all(&header).await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        read_frame::<_, RequestEnvelope>(&mut reader, FrameKind::Request, 1024),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(WireError::Limit)));
}

#[test]
fn authority_spoofing_unknown_fields_and_open_epoch_capability_are_rejected() {
    let peer = AuthenticatedPeer::local(grant()).unwrap();
    let mut request = request(1);
    request.operation = Operation::OpenEpoch {
        epoch: RequestEpoch(1),
    };
    let authority = AuthorityContext {
        runtime: false,
        cause: Cause::Root(RootCommandId::from_u128(4)),
        policy_revision: 1,
        logical_time: 10,
        evidence: vec![],
    };
    let authenticated = verify_request(peer.clone(), request.clone(), &limits())
        .unwrap()
        .into_authenticated(authority)
        .unwrap();
    assert_eq!(authenticated.principal, grant().principal);
    assert!(authenticated.authority.runtime);
    assert_eq!(
        authenticated.command,
        Command::NegotiateEpoch {
            epoch: RequestEpoch(1)
        }
    );
    let mut json = serde_json::to_value(&request).unwrap();
    json["principal"] = serde_json::json!(vec![0u8; 16]);
    assert!(serde_json::from_value::<RequestEnvelope>(json).is_err());
    request.operation = Operation::Submit {
        expected_revision: None,
        command: Command::NegotiateEpoch {
            epoch: RequestEpoch(1),
        },
    };
    assert!(matches!(
        verify_request(peer.clone(), request.clone(), &limits()),
        Err(AccessError::Unauthorized)
    ));
    request.operation = Operation::Submit {
        expected_revision: None,
        command: Command::RevokeClaim {
            claim: ClaimId::from_u128(2),
            reason: "forged runtime".into(),
        },
    };
    assert!(matches!(
        verify_request(peer, request, &limits()),
        Err(AccessError::Unauthorized)
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn unix_watched_grant_governs_connections_accepted_after_it_changes() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("focal.sock");
    let handler: Arc<dyn RequestHandler> =
        Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
    let other = TenantId::from_u128(0x7e);
    let mut narrow = grant();
    narrow.tenants = BTreeSet::from([other]);
    let (sender, receiver) = tokio::sync::watch::channel(narrow);
    let server = Arc::new(UnixServer::bind_watched(&socket, receiver, limits()).unwrap());
    let running = server.clone();
    let handling = handler.clone();
    let task = tokio::spawn(async move { running.serve(handling).await });
    let remote = UnixRemote::new(&socket, limits()).unwrap();
    // The bound grant names another tenant: the request's ledger is refused.
    assert!(matches!(
        remote.request(&request(1)).await.unwrap().result,
        Response::Error(AccessError::Unauthorized)
    ));
    // Widening the grant serves the next connection under the new value
    // without rebinding the socket.
    sender.send_modify(|current| {
        current.tenants.insert(ledger().tenant);
    });
    let expected = dispatch(
        handler.as_ref(),
        AuthenticatedPeer::local(grant()).unwrap(),
        request(2),
        &limits(),
    )
    .await;
    assert_eq!(remote.request(&request(2)).await.unwrap(), expected);
    // Narrowing it again refuses again; the sender outlives every connection.
    sender.send_modify(|current| {
        current.tenants.remove(&ledger().tenant);
    });
    assert!(matches!(
        remote.request(&request(3)).await.unwrap().result,
        Response::Error(AccessError::Unauthorized)
    ));
    server.close();
    task.await.unwrap().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_uses_owner_credentials_and_same_verified_handler() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("focal.sock");
    let handler: Arc<dyn RequestHandler> =
        Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
    let server = Arc::new(UnixServer::bind(&socket, grant(), limits()).unwrap());
    assert_eq!(
        std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let running = server.clone();
    let handling = handler.clone();
    let task = tokio::spawn(async move { running.serve(handling).await });
    let remote = UnixRemote::new(&socket, limits()).unwrap();
    let actual = remote.request(&request(1)).await.unwrap();
    let expected = dispatch(
        handler.as_ref(),
        AuthenticatedPeer::local(grant()).unwrap(),
        request(1),
        &limits(),
    )
    .await;
    assert_eq!(actual, expected);
    server.close();
    task.await.unwrap().unwrap();
    drop(server);
    assert!(!socket.exists());
}

/// The same-user local transport round-trips a request on every platform: a
/// Unix-domain socket on Unix, a named pipe on Windows. Both authenticate the
/// peer through the operating system (`SO_PEERCRED` / pipe token SID) and
/// carry byte-identical frames, so the dispatched response matches a direct
/// dispatch under the same grant.
#[tokio::test]
async fn local_transport_round_trips_between_same_user_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = dir.path().join("focal.sock");
    let handler: Arc<dyn RequestHandler> =
        Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
    let server = Arc::new(UnixServer::bind(&endpoint, grant(), limits()).unwrap());
    let running = server.clone();
    let handling = handler.clone();
    let task = tokio::spawn(async move { running.serve(handling).await });
    let remote = UnixRemote::new(&endpoint, limits()).unwrap();
    let actual = remote.request(&request(1)).await.unwrap();
    let expected = dispatch(
        handler.as_ref(),
        AuthenticatedPeer::local(grant()).unwrap(),
        request(1),
        &limits(),
    )
    .await;
    assert_eq!(actual, expected);
    server.close();
    task.await.unwrap().unwrap();
}

#[test]
fn durable_cursor_scope_and_resolved_positions_survive_wire_roundtrip() {
    let peer = AuthenticatedPeer::local(grant()).unwrap();
    let filter = DeltaFilter::All;
    let scope = stream_scope(&peer, ledger(), &filter).unwrap();
    let cursor = CursorToken {
        key: focal_stream::ConsumerKey {
            ledger: ledger(),
            consumer: ConsumerId::from_u128(8),
        },
        generation: 2,
        scope,
        position: Position::after_delta(DeltaId {
            ledger: ledger(),
            sequence: SessionSeq(7),
            ordinal: 3,
        }),
    };
    let mut request = request(77);
    request.operation = Operation::Stream(StreamRequest::Poll {
        cursor,
        filter: filter.clone(),
        acknowledged: None,
        credits: Credits { items: 0, bytes: 0 },
    });
    verify_request(peer, request.clone(), &limits()).unwrap();
    let mut foreign = grant();
    foreign.principal = ParticipantId::from_u128(99);
    assert!(matches!(
        verify_request(
            AuthenticatedPeer::local(foreign).unwrap(),
            request.clone(),
            &limits()
        ),
        Err(AccessError::Unauthorized)
    ));
    let resolved = CursorToken {
        position: Position::resolved(ledger(), SessionSeq(7)),
        ..cursor
    };
    let reply = request.reply(Response::Stream(StreamReply {
        token: ReadToken {
            ledger: ledger(),
            sequence: SessionSeq(7),
            route_epoch: RouteEpoch(1),
        },
        cursor: resolved,
        acknowledged: cursor,
        seed: None,
        events: vec![StreamEvent::Resolved { cursor: resolved }],
    }));
    let decoded: ResponseEnvelope =
        decode_payload(&encode_payload(&reply, limits().max_frame_bytes).unwrap()).unwrap();
    assert_eq!(decoded, reply);
    validate_response(&request, &decoded, None, &limits()).unwrap();
}

#[tokio::test]
async fn unsupported_protocol_is_rejected_before_dispatch() {
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    registry
        .register_certificate(&certificate, grant())
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        count.fetch_add(1, Ordering::SeqCst);
        async move { response(verified.request()) }
    });
    let (server, task) = server(&pki, registry, handler).await;
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &limits(),
    )
    .unwrap();
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(tls);
    let connection = endpoint
        .connect(server.local_addr().unwrap(), "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    write_frame(
        &mut send,
        FrameKind::Hello,
        &Hello {
            versions: vec![999],
            max_frame_bytes: 1024,
            max_items: 1,
        },
        4096,
    )
    .await
    .unwrap();
    send.finish().unwrap();
    let reply: HelloReply = read_frame(&mut recv, FrameKind::HelloReply, 4096)
        .await
        .unwrap();
    assert!(matches!(
        reply,
        HelloReply::Rejected(AccessError::UnsupportedProtocol)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    server.close();
    task.await.unwrap().unwrap();
}

#[test]
fn upload_scope_and_download_ranges_are_checked() {
    let peer = AuthenticatedPeer::local(grant()).unwrap();
    let upload = [9; 16];
    let scope = upload_scope(&peer, ledger(), upload);
    assert_eq!(scope, upload_scope(&peer, ledger(), upload));
    let mut other_grant = grant();
    other_grant.principal = ParticipantId::from_u128(99);
    let other_peer = AuthenticatedPeer::local(other_grant).unwrap();
    assert_ne!(scope, upload_scope(&other_peer, ledger(), upload));
    assert_ne!(
        scope,
        upload_scope(
            &peer,
            LedgerId {
                session: SessionId::from_u128(99),
                ..ledger()
            },
            upload
        )
    );
    assert_ne!(scope, upload_scope(&peer, ledger(), [8; 16]));

    let content = ContentRef {
        domain: ContentDomainId::from_u128(4),
        root: ContentHash([5; 32]),
        length: 10,
        class: ContentClass::Evidence,
    };
    let mut request = request(42);
    request.operation = Operation::Download {
        content,
        offset: 7,
        max_bytes: 3,
    };
    verify_request(peer.clone(), request.clone(), &limits()).unwrap();
    let valid = request.reply(Response::Content(ContentChunk {
        offset: 7,
        bytes: vec![1, 2, 3],
        eof: true,
    }));
    let decoded =
        decode_payload(&encode_payload(&valid, limits().max_frame_bytes).unwrap()).unwrap();
    validate_response(&request, &decoded, None, &limits()).unwrap();
    for chunk in [
        ContentChunk {
            offset: 6,
            bytes: vec![1, 2, 3],
            eof: false,
        },
        ContentChunk {
            offset: 7,
            bytes: vec![1, 2, 3],
            eof: false,
        },
        ContentChunk {
            offset: 7,
            bytes: vec![1, 2, 3, 4],
            eof: true,
        },
        ContentChunk {
            offset: 7,
            bytes: vec![],
            eof: false,
        },
    ] {
        assert!(
            validate_response(
                &request,
                &request.reply(Response::Content(chunk)),
                None,
                &limits()
            )
            .is_err()
        );
    }
    if let Operation::Download { max_bytes, .. } = &mut request.operation {
        *max_bytes = limits().max_frame_bytes;
    }
    assert!(matches!(
        verify_request(peer, request, &limits()),
        Err(AccessError::Capacity)
    ));
}

#[tokio::test]
async fn oversized_upload_is_denied_before_handler_and_bad_upload_reply_is_unknown() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let handler = move |verified: VerifiedRequest| {
        count.fetch_add(1, Ordering::SeqCst);
        // This incorrectly sized durable prefix cannot confirm an append.
        async move {
            verified
                .request()
                .reply(Response::Upload(UploadReply::Offset(0)))
        }
    };
    let peer = AuthenticatedPeer::local(grant()).unwrap();
    let mut request = request(43);
    request.operation = Operation::Upload(UploadRequest::Append {
        upload: [8; 16],
        offset: 0,
        bytes: vec![1; limits().max_frame_bytes as usize],
    });
    assert_eq!(
        dispatch(&handler, peer.clone(), request.clone(), &limits())
            .await
            .result,
        Response::Error(AccessError::Capacity)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    if let Operation::Upload(UploadRequest::Append { bytes, .. }) = &mut request.operation {
        bytes.truncate(3);
    }
    assert_eq!(
        dispatch(&handler, peer, request, &limits()).await.result,
        Response::Error(AccessError::OutcomeUnknown)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn peer_pool_caches_connections_reconnects_identical_packets_and_fences_routes() {
    use std::{collections::BTreeMap, sync::Mutex};
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let records = seen.clone();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        assert_eq!(verified.peer().role(), PeerRole::Node { node_id: 7 });
        let mut records = records.lock().unwrap();
        let probe = matches!(verified.request().operation, Operation::Probe { .. });
        if !probe {
            records.push(verified.request().clone());
        }
        let lose_first = !probe && records.len() == 1;
        async move {
            verified.request().reply(if probe {
                Response::Probe(vec![1])
            } else if lose_first {
                Response::Error(AccessError::Unavailable)
            } else {
                Response::PeerAccepted
            })
        }
    });
    let (server, task) = server(&pki, registry, handler).await;
    let pool = PeerConnectionPool::new(
        connector(&pki, certificate, key),
        PeerPoolLimits {
            retry_backoff: Duration::ZERO,
            ..PeerPoolLimits::default()
        },
    )
    .unwrap();
    let routes = BTreeMap::from([(
        2,
        PeerEndpoint {
            address: server.local_addr().unwrap(),
            server_name: "localhost".into(),
            name: None,
        },
    )]);
    pool.replace_routes(1, routes.clone()).unwrap();
    let mut packet = request(81);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![7, 8, 9],
    };
    assert_eq!(pool.path(3), None, "an unrouted peer has no path");
    assert_eq!(
        pool.path(2).unwrap().samples(),
        0,
        "a routed peer that answered nothing has no sample"
    );
    pool.send(2, &packet).await.unwrap();
    pool.send(2, &packet).await.unwrap();
    // A replication message is answered once the peer has persisted it:
    // what it takes is the peer's work, and no sample of the path.
    assert_eq!(pool.path(2).unwrap().samples(), 0);
    // Each probe the peer answers is one sample, timed on the connection
    // already open.
    let mut probe = request(84);
    probe.operation = Operation::Probe {
        request: vec![1, 2, 3],
    };
    assert_eq!(pool.send_probe(2, &probe).await, Ok(vec![1]));
    assert_eq!(pool.send_probe(2, &probe).await, Ok(vec![1]));
    let path = pool.path(2).unwrap();
    assert_eq!(path.samples(), 2);
    assert!(path.tail_ns().unwrap() >= path.smoothed_ns());
    assert!(path.smoothed_ns() > 0 && path.smoothed_ns() < 1_000_000_000);
    {
        let recorded = seen.lock().unwrap();
        assert_eq!(recorded.len(), 3);
        assert!(recorded.iter().all(|request| request == &packet));
    }
    assert_eq!(pool.stats().connections_opened, 2);
    assert_eq!(pool.stats().cached_connections, 1);
    // Two replication messages and two probes.
    assert_eq!(pool.stats().delivered, 4);
    pool.replace_routes(1, routes.clone()).unwrap();
    assert_eq!(
        pool.replace_routes(1, BTreeMap::new()),
        Err(PeerSendError::StaleRoutes)
    );
    pool.replace_routes(2, BTreeMap::new()).unwrap();
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::NoRoute));
    assert_eq!(pool.stats().cached_connections, 0);
    assert_eq!(
        pool.replace_routes(1, routes),
        Err(PeerSendError::StaleRoutes)
    );
    pool.close();
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Closed));
    server.close();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn peer_pool_re_resolves_a_named_endpoint_when_its_address_stops_answering() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| async move {
        verified.request().reply(Response::PeerAccepted)
    });
    let (server, task) = server(&pki, registry, handler).await;
    let live = server.local_addr().unwrap();
    // A UDP socket bound and dropped: nothing answers there.
    let stale = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    };
    let pool = PeerConnectionPool::new(
        connector(&pki, certificate, key),
        PeerPoolLimits {
            retry_backoff: Duration::ZERO,
            // The stale dial runs to the connector's own deadline first.
            timeout: Duration::from_secs(10),
            ..PeerPoolLimits::default()
        },
    )
    .unwrap();
    // A name that is not `host:port` with a DNS host is refused with the route.
    for name in ["localhost", "127.0.0.1:1", ":1"] {
        assert_eq!(
            pool.replace_routes(
                1,
                BTreeMap::from([(
                    2,
                    PeerEndpoint {
                        address: stale,
                        server_name: "localhost".into(),
                        name: Some(name.into()),
                    },
                )]),
            ),
            Err(PeerSendError::Configuration),
            "{name}"
        );
    }
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: stale,
                server_name: "localhost".into(),
                name: Some(format!("localhost:{}", live.port())),
            },
        )]),
    )
    .unwrap();
    let mut packet = request(82);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![1],
    };
    // The stale address fails; the name resolves to the live server.
    pool.send(2, &packet).await.unwrap();
    assert_eq!(pool.stats().delivered, 1);
    // Without a name the same stale address is simply lost.
    pool.replace_routes(
        2,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: stale,
                server_name: "localhost".into(),
                name: None,
            },
        )]),
    )
    .unwrap();
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Lost));
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
}

/// A peer that moved behind its name while every caller gives up long before
/// the dead address's deadline (a liveness probe, a bounded control read):
/// the dial runs on its own and the name's fresh address wins, so a later
/// caller finds the connection instead of restarting from the dead address
/// forever (24 §24; the 2026-09-13 all-pods-moved wedge).
#[tokio::test]
async fn peer_pool_reaches_a_peer_that_moved_behind_its_name_while_every_caller_gives_up_early() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| async move {
        verified.request().reply(Response::PeerAccepted)
    });
    let (server, task) = server(&pki, registry, handler).await;
    let live = server.local_addr().unwrap();
    // A UDP socket bound and dropped: nothing answers there, and a dial to it
    // only fails at the connector's deadline.
    let stale = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    };
    let pool = PeerConnectionPool::new(
        connector(&pki, certificate, key),
        PeerPoolLimits {
            attempts: 1,
            retry_backoff: Duration::ZERO,
            timeout: Duration::from_secs(10),
            ..PeerPoolLimits::default()
        },
    )
    .unwrap();
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: stale,
                server_name: "localhost".into(),
                name: Some(format!("localhost:{}", live.port())),
            },
        )]),
    )
    .unwrap();
    let mut packet = request(84);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![1],
    };
    // Every caller allows far less than the dead address's deadline.
    let started = std::time::Instant::now();
    let mut delivered = false;
    while started.elapsed() < Duration::from_secs(5) {
        match tokio::time::timeout(Duration::from_millis(200), pool.send(2, &packet)).await {
            Ok(Ok(_)) => {
                delivered = true;
                break;
            }
            Ok(Err(error)) => panic!("the send failed rather than timing out: {error}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    assert!(delivered, "no caller ever reached the moved peer");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the moved peer was reached only after {:?}",
        started.elapsed()
    );
    let stats = pool.stats();
    assert_eq!(stats.dials, 1, "one dial served every caller");
    assert_eq!(stats.connections_opened, 1);
    assert_eq!(stats.delivered, 1);
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
}

/// Callers that give up before a dial to a dead address decides neither
/// abandon it nor repeat it: one dial serves them all, and its outcome still
/// marks the peer unreachable so the next send fails at once.
#[tokio::test]
async fn peer_pool_callers_that_give_up_share_one_dial_whose_outcome_is_still_recorded() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let stale = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    };
    let cooldown = Duration::from_secs(5);
    let pool = PeerConnectionPool::new(
        connector(&pki, certificate, key),
        PeerPoolLimits {
            attempts: 1,
            retry_backoff: Duration::ZERO,
            timeout: Duration::from_secs(10),
            unreachable_cooldown: cooldown,
            ..PeerPoolLimits::default()
        },
    )
    .unwrap();
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: stale,
                server_name: "localhost".into(),
                name: None,
            },
        )]),
    )
    .unwrap();
    let mut packet = request(85);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![1],
    };
    let mut callers = tokio::task::JoinSet::new();
    let pool = Arc::new(pool);
    for _ in 0..6 {
        let pool = pool.clone();
        let packet = packet.clone();
        callers.spawn(async move {
            tokio::time::timeout(Duration::from_millis(100), pool.send(2, &packet)).await
        });
    }
    while let Some(outcome) = callers.join_next().await {
        // A caller either ran out its own deadline waiting on the dial or was
        // refused at once by the per-peer bound; none was answered by the dial.
        match outcome.unwrap() {
            Err(_) | Ok(Err(PeerSendError::Busy)) => {}
            other => panic!("a caller was answered by the dial: {other:?}"),
        }
    }
    assert_eq!(pool.stats().dials, 1, "the callers shared one dial");
    // The dial decides on its own after the callers left; wait for it.
    let deadline = std::time::Instant::now() + limits().request_timeout + Duration::from_secs(2);
    loop {
        let started = std::time::Instant::now();
        let result = pool.send(2, &packet).await;
        assert_eq!(result, Err(PeerSendError::Lost));
        if started.elapsed() < Duration::from_millis(50) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the abandoned dial never marked the peer unreachable"
        );
    }
    assert_eq!(
        pool.stats().dials,
        1,
        "the recorded outcome spared every later caller a dial"
    );
    pool.close();
}

#[tokio::test]
async fn a_request_in_flight_ends_when_its_route_is_retired_and_not_at_its_deadline() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let gated = started.clone();
    // The peer never answers: only the retirement can end the request.
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        let gated = gated.clone();
        async move {
            gated.notify_one();
            std::future::pending::<()>().await;
            verified.request().reply(Response::PeerAccepted)
        }
    });
    let (server, task) = server(&pki, registry, handler).await;
    let deadline = Duration::from_secs(60);
    let pool = Arc::new(
        PeerConnectionPool::new(
            connector(&pki, certificate, key),
            PeerPoolLimits {
                timeout: deadline,
                attempts: 2,
                ..PeerPoolLimits::default()
            },
        )
        .unwrap(),
    );
    let endpoint = PeerEndpoint {
        address: server.local_addr().unwrap(),
        server_name: "localhost".into(),
        name: None,
    };
    for round in 0..8u64 {
        let revision = round * 2 + 1;
        pool.replace_routes(revision, BTreeMap::from([(2, endpoint.clone())]))
            .unwrap();
        let mut packet = request(200 + round as u128);
        packet.operation = Operation::Raft {
            group: [2; 16],
            message: vec![7, 8, 9],
        };
        let sending = pool.clone();
        let pending = tokio::spawn(async move { sending.send(2, &packet).await });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .unwrap();
        let asked = std::time::Instant::now();
        pool.replace_routes(revision + 1, BTreeMap::new()).unwrap();
        let ended = tokio::time::timeout(deadline / 2, pending)
            .await
            .expect("the request waited out its deadline")
            .unwrap();
        assert!(
            matches!(
                ended,
                Err(PeerSendError::NoRoute | PeerSendError::RouteChanged)
            ),
            "{ended:?}"
        );
        assert!(asked.elapsed() < deadline / 2);
        assert_eq!(pool.stats().inflight, 0);
        assert_eq!(pool.stats().cached_connections, 0);
    }
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
}

/// A route retired while its connection is still being made never leaves a
/// connection behind: whatever the order of the handshake and the
/// retirement, the pool holds none afterwards and the next route works.
#[tokio::test]
async fn a_route_retired_during_its_dial_leaves_no_connection_behind() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| async move {
        verified.request().reply(Response::PeerAccepted)
    });
    let (server, task) = server(&pki, registry, handler).await;
    let pool = Arc::new(
        PeerConnectionPool::new(connector(&pki, certificate, key), PeerPoolLimits::default())
            .unwrap(),
    );
    let endpoint = PeerEndpoint {
        address: server.local_addr().unwrap(),
        server_name: "localhost".into(),
        name: None,
    };
    let mut packet = request(300);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![1],
    };
    let mut retired_in_flight = 0;
    for round in 0..64u64 {
        let revision = round * 2 + 1;
        pool.replace_routes(revision, BTreeMap::from([(2, endpoint.clone())]))
            .unwrap();
        let sending = pool.clone();
        let sent = packet.clone();
        let pending = tokio::spawn(async move { sending.send(2, &sent).await });
        // Yield a varying number of times so the retirement lands at
        // different points of the dial.
        for _ in 0..round % 8 {
            tokio::task::yield_now().await;
        }
        pool.replace_routes(revision + 1, BTreeMap::new()).unwrap();
        let ended = tokio::time::timeout(Duration::from_secs(30), pending)
            .await
            .expect("a send outlived its retired route")
            .unwrap();
        if ended.is_err() {
            retired_in_flight += 1;
        }
        assert_eq!(pool.stats().inflight, 0, "round {round}");
        assert_eq!(pool.stats().cached_connections, 0, "round {round}");
    }
    assert!(retired_in_flight > 0, "no retirement met a send in flight");
    // The pool is whole: a route installed now serves.
    pool.replace_routes(1_000, BTreeMap::from([(2, endpoint)]))
        .unwrap();
    assert_eq!(pool.send(2, &packet).await, Ok(()));
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
}

#[test]
fn the_lanes_of_a_connection_are_derived_from_the_consensus_window_and_the_path() {
    // Thirteen streams carry what the reference path holds (27 §7).
    assert_eq!(content_streams(), 13);
    let limits = WireLimits::for_consensus(128);
    assert_eq!(
        (limits.control_streams, limits.streams_per_connection),
        (128, 142)
    );
    limits.validate().unwrap();
    let pool = PeerPoolLimits::for_consensus(128);
    assert_eq!(pool.per_peer_inflight, 128);
    assert_eq!(
        pool.max_inflight,
        128 * PeerPoolLimits::default().max_connections
    );
    // A control lane wider than the connection is refused.
    let mut wrong = WireLimits::default();
    wrong.control_streams = wrong.streams_per_connection + 1;
    assert!(wrong.validate().is_err());
}

/// A group's messages to a peer beyond its lane wait their turn and are all
/// carried, in order: none is refused for the lane being full at that
/// instant, which cost a vote a whole election timeout.
#[tokio::test]
async fn a_groups_message_waits_its_turn_on_the_lane_instead_of_being_refused() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let arrived = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (counted, released, seen) = (arrived.clone(), release.clone(), order.clone());
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        let (counted, released, seen) = (counted.clone(), released.clone(), seen.clone());
        async move {
            seen.lock().unwrap().push(verified.request().request_id.0);
            counted.add_permits(1);
            released.notified().await;
            verified.request().reply(Response::PeerAccepted)
        }
    });
    let (server, task) = server(&pki, registry, handler).await;
    let pool = Arc::new(
        PeerConnectionPool::new(
            connector(&pki, certificate, key),
            PeerPoolLimits {
                per_peer_inflight: 1,
                attempts: 1,
                ..PeerPoolLimits::default()
            },
        )
        .unwrap(),
    );
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: server.local_addr().unwrap(),
                server_name: "localhost".into(),
                name: None,
            },
        )]),
    )
    .unwrap();
    let mut sends = Vec::new();
    for id in 0..3u128 {
        let mut packet = request(90 + id);
        packet.operation = Operation::Raft {
            group: [2; 16],
            message: vec![7, 8, 9],
        };
        let sending = pool.clone();
        sends.push(tokio::spawn(async move { sending.send(2, &packet).await }));
    }
    // One reaches the peer at a time; the others wait on the lane, refused
    // by no one, and each is released once it has arrived.
    for _ in 0..3 {
        arrived.acquire_many(1).await.unwrap().forget();
        assert_eq!(pool.stats().busy, 0);
        release.notify_one();
    }
    for send in sends {
        assert_eq!(send.await.unwrap(), Ok(()));
    }
    assert_eq!(pool.stats().busy, 0);
    assert_eq!(pool.stats().delivered, 3);
    let seen = order.lock().unwrap().clone();
    assert_eq!(seen.len(), 3, "{seen:?}");
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn peer_pool_saturation_is_bounded_and_route_change_retires_active_connections() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let gated = started.clone();
    let release = Arc::new(tokio::sync::Notify::new());
    let released = release.clone();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        let gated = gated.clone();
        let released = released.clone();
        async move {
            gated.notify_one();
            released.notified().await;
            verified.request().reply(Response::PeerAccepted)
        }
    });
    let (server, task) = server(&pki, registry, handler).await;
    let pool = Arc::new(
        PeerConnectionPool::new(
            connector(&pki, certificate, key),
            PeerPoolLimits {
                max_connections: 1,
                max_inflight: 1,
                attempts: 1,
                ..PeerPoolLimits::default()
            },
        )
        .unwrap(),
    );
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: server.local_addr().unwrap(),
                server_name: "localhost".into(),
                name: None,
            },
        )]),
    )
    .unwrap();
    let mut packet = request(82);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![7, 8, 9],
    };
    let sent = packet.clone();
    let sending = pool.clone();
    let pending = tokio::spawn(async move { sending.send(2, &sent).await });
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Busy));
    assert_eq!(pool.stats().inflight, 1);
    assert_eq!(pool.stats().busy, 1);
    pool.replace_routes(2, BTreeMap::new()).unwrap();
    release.notify_one();
    assert!(pending.await.unwrap().is_err());
    assert_eq!(pool.stats().inflight, 0);
    assert_eq!(pool.stats().cached_connections, 0);
    assert_eq!(
        pool.send(2, &request(83)).await,
        Err(PeerSendError::InvalidRequest)
    );
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
}

/// A path between a client and `server` that carries `bits` in a second
/// each way: a datagram waits its turn behind those before it, and one
/// that finds `QUEUE` waiting is dropped.
async fn narrow(
    server: std::net::SocketAddr,
    bits: u64,
) -> (std::net::SocketAddr, Vec<tokio::task::JoinHandle<()>>) {
    const QUEUE: usize = 32;
    let front = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let back = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let address = front.local_addr().unwrap();
    let client = Arc::new(std::sync::Mutex::new(None::<std::net::SocketAddr>));
    let mut tasks = Vec::new();
    for up in [true, false] {
        let (from, to) = if up {
            (front.clone(), back.clone())
        } else {
            (back.clone(), front.clone())
        };
        let (queue, mut waiting) = tokio::sync::mpsc::channel::<Vec<u8>>(QUEUE);
        let known = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut datagram = vec![0u8; 65_536];
            while let Ok((length, source)) = from.recv_from(&mut datagram).await {
                if up {
                    *known.lock().unwrap() = Some(source);
                }
                let _ = queue.try_send(datagram[..length].to_vec());
            }
        }));
        let known = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut free = tokio::time::Instant::now();
            while let Some(datagram) = waiting.recv().await {
                free = free.max(tokio::time::Instant::now())
                    + Duration::from_nanos(datagram.len() as u64 * 8 * 1_000_000_000 / bits);
                tokio::time::sleep_until(free).await;
                let target = if up {
                    Some(server)
                } else {
                    *known.lock().unwrap()
                };
                if let Some(target) = target {
                    let _ = to.send_to(&datagram, target).await;
                }
            }
        }));
    }
    (address, tasks)
}

/// A megabyte over a path that takes longer to carry it than a request is
/// given is carried, each way: an exchange waits as long as the path takes
/// (`carried`, `read_payload_arriving`), and no longer for a peer that
/// does not answer than the path would have taken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn narrow_path_carries_a_megabyte_that_takes_longer_than_a_request_is_given() {
    let wire = WireLimits {
        request_timeout: Duration::from_secs(1),
        max_frame_bytes: 2 * 1024 * 1024,
        max_cost: 8 * 1024 * 1024,
        ..Default::default()
    };
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let megabyte: Vec<u8> = (0..1024 * 1024_u32).map(|at| (at % 251) as u8).collect();
    let held = megabyte.clone();
    let silent = Arc::new(tokio::sync::Notify::new());
    let never = silent.clone();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        let (held, never) = (held.clone(), never.clone());
        async move {
            let reply = match &verified.request().operation {
                Operation::Custody(CustodyRequest::Chunk { index, bytes, .. }) => {
                    assert_eq!(bytes, &held);
                    CustodyReply::ChunkStored { index: *index }
                }
                Operation::Custody(CustodyRequest::ReadChunk { index, .. }) => {
                    CustodyReply::Chunk {
                        index: *index,
                        bytes: held,
                    }
                }
                _ => {
                    never.notified().await;
                    CustodyReply::Cancelled
                }
            };
            verified.request().reply(Response::Custody(reply))
        }
    });
    let (server_certificate, server_key) = pki.issue(true);
    let tls = server_tls(
        TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let server = Arc::new(
        QuicServer::bind("127.0.0.1:0".parse().unwrap(), tls, registry, wire.clone()).unwrap(),
    );
    let running = server.clone();
    let task = tokio::spawn(async move { running.serve(handler).await });
    // Four megabits in a second: a megabyte takes two seconds and more.
    const BITS: u64 = 4_000_000;
    let (path, relays) = narrow(server.local_addr().unwrap(), BITS).await;
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let connector = QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire.clone()).unwrap();
    let remote = connector.connect(path, "localhost").await.unwrap();
    let custody = |id: u128, operation: CustodyRequest| {
        let mut packet = request(id);
        packet.operation = Operation::Custody(operation);
        packet
    };
    let began = std::time::Instant::now();
    let stored = remote
        .request(&custody(
            1,
            CustodyRequest::Chunk {
                transfer: [1; 16],
                index: 3,
                bytes: megabyte.clone(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        stored.result,
        Response::Custody(CustodyReply::ChunkStored { index: 3 })
    );
    // No faster than the path carries it, which is longer than a request
    // is given.
    let least = Duration::from_millis(megabyte.len() as u64 * 8 * 1_000 / BITS);
    assert!(least > wire.request_timeout);
    let sent = began.elapsed();
    assert!(sent >= least, "{sent:?}");
    let began = std::time::Instant::now();
    let read = remote
        .request(&custody(
            2,
            CustodyRequest::ReadChunk {
                transfer: [1; 16],
                index: 4,
                max_bytes: 1024 * 1024,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        read.result,
        Response::Custody(CustodyReply::Chunk {
            index: 4,
            bytes: megabyte.clone()
        })
    );
    let received = began.elapsed();
    assert!(received >= least, "{received:?}");
    // A peer that does not answer what it was asked: the path has
    // carried what was asked within one wait.
    let began = std::time::Instant::now();
    let unanswered = remote
        .request_within(
            &custody(3, CustodyRequest::Cancel { transfer: [1; 16] }),
            Duration::from_millis(300),
        )
        .await;
    assert!(
        matches!(unanswered, Err(WireError::Timeout)),
        "{unanswered:?}"
    );
    assert!(began.elapsed() >= Duration::from_millis(300));
    silent.notify_waiters();
    remote.close();
    server.close();
    task.await.unwrap().unwrap();
    for relay in relays {
        relay.abort();
    }
}

/// Content to a peer has a lane of its own: a transfer that fills it
/// refuses nothing a group sends there, content that comes to a full lane
/// waits its turn, and what waits is counted and bounded.
#[tokio::test]
async fn content_waits_its_turn_and_refuses_nothing_a_group_sends() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let held = Arc::new(tokio::sync::Semaphore::new(0));
    let arrived = Arc::new(tokio::sync::Semaphore::new(0));
    let (gate, seen) = (held.clone(), arrived.clone());
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        let (gate, seen) = (gate.clone(), seen.clone());
        async move {
            match &verified.request().operation {
                Operation::Custody(CustodyRequest::Cancel { .. }) => {
                    seen.add_permits(1);
                    gate.acquire().await.unwrap().forget();
                    verified
                        .request()
                        .reply(Response::Custody(CustodyReply::Cancelled))
                }
                _ => verified.request().reply(Response::PeerAccepted),
            }
        }
    });
    let (server, task) = server(&pki, registry, handler).await;
    // Five streams: two for what is asked, one for the probe, and two
    // for content.
    let wire = WireLimits {
        streams_per_connection: 5,
        ..limits()
    };
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let connector = QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire).unwrap();
    let pool = Arc::new(
        PeerConnectionPool::new(
            connector,
            PeerPoolLimits {
                max_inflight: 4,
                attempts: 1,
                timeout: Duration::from_secs(30),
                ..PeerPoolLimits::default()
            },
        )
        .unwrap(),
    );
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: server.local_addr().unwrap(),
                server_name: "localhost".into(),
                name: None,
            },
        )]),
    )
    .unwrap();
    let content = |id: u128| {
        let mut packet = request(id);
        packet.operation = Operation::Custody(CustodyRequest::Cancel { transfer: [3; 16] });
        packet
    };
    let mut sent = Vec::new();
    for id in 0..4 {
        let (sending, packet) = (pool.clone(), content(100 + id));
        sent.push(tokio::spawn(async move {
            sending.send_custody(2, &packet).await
        }));
    }
    // Two reached the peer and two wait their turn: the lane is full.
    arrived.acquire_many(2).await.unwrap().forget();
    while pool.stats().bulk_inflight < 4 {
        tokio::task::yield_now().await;
    }
    assert_eq!(arrived.available_permits(), 0);
    // As much content as the pool counts is in flight or waits.
    assert_eq!(
        pool.send_custody(2, &content(110)).await,
        Err(PeerSendError::Busy)
    );
    // What a group sends the peer is sent.
    let mut packet = request(120);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![7, 8, 9],
    };
    assert_eq!(pool.send(2, &packet).await, Ok(()));
    assert_eq!(pool.stats().inflight, 0);
    // Those that waited go as those before them are answered.
    held.add_permits(2);
    arrived.acquire_many(2).await.unwrap().forget();
    held.add_permits(2);
    for pending in sent {
        assert!(matches!(
            pending.await.unwrap(),
            Ok(CustodyReply::Cancelled)
        ));
    }
    assert_eq!(pool.stats().bulk_inflight, 0);
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
}

#[test]
fn missing_tokio_context_returns_errors_before_socket_or_handler_work() {
    use std::future::Future;
    let handler = |verified: VerifiedRequest| async move { response(verified.request()) };
    let peer = AuthenticatedPeer::local(grant()).unwrap();
    let request = request(1);
    let limits = limits();
    assert!(
        WireLimits {
            request_timeout: std::time::Duration::MAX,
            ..limits.clone()
        }
        .validate()
        .is_err()
    );
    let future = dispatch(&handler, peer, request, &limits);
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let std::task::Poll::Ready(reply) = future.as_mut().poll(&mut context) else {
        panic!("missing runtime must be rejected immediately");
    };
    assert_eq!(reply.result, Response::Error(AccessError::Unavailable));
    #[cfg(unix)]
    {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unused.sock");
        assert!(matches!(
            UnixServer::bind(&path, grant(), limits.clone()),
            Err(WireError::Connection)
        ));
        assert!(!path.exists());
    }
}

#[test]
fn peer_control_discovery_is_node_only_scoped_bounded_and_read_only_on_wire() {
    let mut packet = request(99);
    packet.operation = Operation::PeerControl {
        group: [7; 16],
        request: vec![1, 2],
    };
    assert_eq!(packet.operation.registered_tag(), 11);
    assert!(!packet.operation.is_mutation());
    for role in [PeerRole::Actor, PeerRole::Evaluator, PeerRole::Runtime] {
        let mut value = grant();
        value.role = role;
        assert!(matches!(
            verify_request(
                AuthenticatedPeer::local(value).unwrap(),
                packet.clone(),
                &limits()
            ),
            Err(AccessError::Unauthorized)
        ));
    }
    let mut value = grant();
    value.role = PeerRole::Node { node_id: 7 };
    let node = AuthenticatedPeer::local(value).unwrap();
    assert!(verify_request(node.clone(), packet.clone(), &limits()).is_ok());
    let reply = packet.reply(Response::Control {
        response: vec![1, 2],
    });
    validate_response(&packet, &reply, None, &limits()).unwrap();
    // A node peer is infrastructure: its grant's tenants do not scope the
    // sessions it replicates and drives, while a client's grant still does.
    packet.ledger.tenant = TenantId::from_u128(99);
    assert!(verify_request(node.clone(), packet.clone(), &limits()).is_ok());
    let mut scoped = grant();
    scoped.role = PeerRole::Actor;
    let mut read = packet.clone();
    read.operation = Operation::Summary;
    assert!(matches!(
        verify_request(AuthenticatedPeer::local(scoped).unwrap(), read, &limits()),
        Err(AccessError::Unauthorized)
    ));
    packet.ledger = ledger();
    packet.operation = Operation::PeerControl {
        group: [0; 16],
        request: vec![1, 2],
    };
    assert!(matches!(
        verify_request(node.clone(), packet.clone(), &limits()),
        Err(AccessError::InvalidRequest)
    ));
    packet.operation = Operation::PeerControl {
        group: [7; 16],
        request: vec![1; MAX_PEER_CONTROL_REQUEST_BYTES + 1],
    };
    assert!(matches!(
        verify_request(node.clone(), packet.clone(), &limits()),
        Err(AccessError::Capacity)
    ));
    packet.operation = Operation::Control {
        group: [7; 16],
        request: vec![1, 2],
    };
    assert!(matches!(
        verify_request(node, packet, &limits()),
        Err(AccessError::Unauthorized)
    ));
}

#[test]
fn node_contact_requires_a_certificate_and_has_no_operator_authority() {
    let mut packet = request(100);
    packet.operation = Operation::NodeContact {
        group: [7; 16],
        sequence: 1,
        acknowledged_through: 0,
        expected_generation: 0,
        advertise: "127.0.0.1:7443".parse().unwrap(),
        region: Some("region-a".into()),
        zone: Some("zone-1".into()),
        endpoint: Some("node-1.focal.example:7443".into()),
    };
    assert_eq!(packet.operation.registered_tag(), 12);
    assert!(packet.operation.is_mutation());
    for role in [
        PeerRole::Actor,
        PeerRole::Evaluator,
        PeerRole::Runtime,
        PeerRole::Node { node_id: 7 },
    ] {
        let mut value = grant();
        value.role = role;
        assert!(matches!(
            verify_request(
                AuthenticatedPeer::local(value).unwrap(),
                packet.clone(),
                &limits()
            ),
            Err(AccessError::Unauthorized)
        ));
    }
    let registry = PeerRegistry::new(1).unwrap();
    let mut value = grant();
    value.role = PeerRole::Node { node_id: 7 };
    let fingerprint = registry
        .register_certificate(b"transport-verified-test-certificate", value)
        .unwrap();
    let node = registry.authenticate(fingerprint).unwrap();
    assert!(verify_request(node.clone(), packet.clone(), &limits()).is_ok());
    validate_response(
        &packet,
        &packet.reply(Response::Control { response: vec![1] }),
        None,
        &limits(),
    )
    .unwrap();
    for address in [
        "0.0.0.0:7443",
        "127.0.0.1:0",
        "224.0.0.1:7443",
        "255.255.255.255:7443",
        "[ff02::1]:7443",
    ] {
        let mut bad = packet.clone();
        if let Operation::NodeContact { advertise, .. } = &mut bad.operation {
            *advertise = address.parse().unwrap();
        }
        assert!(matches!(
            verify_request(node.clone(), bad, &limits()),
            Err(AccessError::InvalidRequest)
        ));
    }
    // Labels are bounded, non-empty, and a zone needs its region.
    for (region, zone) in [
        (Some(String::new()), None),
        (Some("r".repeat(65)), None),
        (Some("region-a".into()), Some(String::new())),
        (None, Some("zone-1".into())),
    ] {
        let mut bad = packet.clone();
        if let Operation::NodeContact {
            region: r, zone: z, ..
        } = &mut bad.operation
        {
            *r = region;
            *z = zone;
        }
        assert!(matches!(
            verify_request(node.clone(), bad, &limits()),
            Err(AccessError::InvalidRequest)
        ));
    }
    // An advertised name is `host:port` with a DNS host and a nonzero port.
    for name in [
        "",
        "node-1.focal.example",
        "node-1.focal.example:0",
        "127.0.0.1:7443",
        "[::1]:7443",
        ":7443",
        "bad name:7443",
        &format!("{}:7443", "n".repeat(254)),
    ] {
        let mut bad = packet.clone();
        if let Operation::NodeContact { endpoint, .. } = &mut bad.operation {
            *endpoint = Some(name.to_owned());
        }
        assert!(
            matches!(
                verify_request(node.clone(), bad, &limits()),
                Err(AccessError::InvalidRequest)
            ),
            "{name}"
        );
    }
    let mut denied = packet.clone();
    denied.operation = Operation::Control {
        group: [7; 16],
        request: vec![1, 0],
    };
    assert!(matches!(
        verify_request(node, denied, &limits()),
        Err(AccessError::Unauthorized)
    ));
}

#[tokio::test]
async fn multiplexed_connection_cannot_bypass_data_alpn_or_client_identity() {
    use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    for (client_identity, protocol) in [(false, ALPN), (true, b"focal-enroll/1".as_slice())] {
        let pki = Pki::new();
        let (server_certificate, server_key) = pki.issue(true);
        let (client_certificate, client_key) = pki.issue(false);
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(pki.ca.der().to_vec()))
            .unwrap();
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            provider.clone(),
        )
        .allow_unauthenticated()
        .build()
        .unwrap();
        let mut server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![CertificateDer::from(server_certificate)],
                PrivatePkcs8KeyDer::from(server_key).into(),
            )
            .unwrap();
        server_tls.alpn_protocols = vec![protocol.to_vec()];
        let server_tls = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(server_tls).unwrap(),
        ));
        let client_builder = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots);
        let mut client_tls = if client_identity {
            client_builder
                .with_client_auth_cert(
                    vec![CertificateDer::from(client_certificate.clone())],
                    PrivatePkcs8KeyDer::from(client_key).into(),
                )
                .unwrap()
        } else {
            client_builder.with_no_client_auth()
        };
        client_tls.alpn_protocols = vec![protocol.to_vec()];
        let connector = QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_tls).unwrap())),
            limits(),
        )
        .unwrap();
        let registry = PeerRegistry::new(4).unwrap();
        registry
            .register_certificate(&client_certificate, grant())
            .unwrap();
        let server = Arc::new(
            QuicServer::bind(
                "127.0.0.1:0".parse().unwrap(),
                server_tls,
                registry,
                limits(),
            )
            .unwrap(),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let running = server.clone();
        let task = tokio::spawn(async move {
            running
                .serve(move |request: VerifiedRequest| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    async move { response(request.request()) }
                })
                .await
        });
        assert!(
            connector
                .connect(server.local_addr().unwrap(), "localhost")
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        server.close();
        task.await.unwrap().unwrap();
    }
}

#[test]
fn quic_server_bind_contains_missing_driver_failure() {
    let pki = Pki::new();
    for (io, time) in [(false, false), (true, false), (false, true)] {
        let mut builder = tokio::runtime::Builder::new_current_thread();
        if io {
            builder.enable_io();
        }
        if time {
            builder.enable_time();
        }
        let runtime = builder.build().unwrap();
        let (certificate, key) = pki.issue(true);
        let tls = server_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &limits(),
        )
        .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async {
                QuicServer::bind(
                    "127.0.0.1:0".parse().unwrap(),
                    tls,
                    PeerRegistry::new(1).unwrap(),
                    limits(),
                )
            })
        }));
        assert!(matches!(result, Ok(Err(WireError::Connection))));
    }
}
#[test]
fn quic_connector_bind_contains_missing_driver_failure() {
    let pki = Pki::new();
    for (io, time) in [(false, false), (true, false), (false, true)] {
        let mut builder = tokio::runtime::Builder::new_current_thread();
        if io {
            builder.enable_io();
        }
        if time {
            builder.enable_time();
        }
        let runtime = builder.build().unwrap();
        let (certificate, key) = pki.issue(false);
        let tls = client_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &limits(),
        )
        .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async {
                QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, limits())
            })
        }));
        assert!(matches!(result, Ok(Err(WireError::Connection))));
    }
}
#[test]
#[cfg(unix)]
fn unix_bind_contains_missing_driver_failure_without_leaving_socket() {
    for (io, time) in [(false, false), (true, false), (false, true)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.sock");
        let mut builder = tokio::runtime::Builder::new_current_thread();
        if io {
            builder.enable_io();
        }
        if time {
            builder.enable_time();
        }
        let runtime = builder.build().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async { UnixServer::bind(&path, grant(), limits()) })
        }));
        assert!(matches!(result, Ok(Err(WireError::Connection))));
        assert!(!path.exists());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = runtime
            .block_on(async { UnixServer::bind(&path, grant(), limits()) })
            .unwrap();
        assert!(path.exists());
        drop(server);
        assert!(!path.exists());
    }
}

#[test]
#[cfg(unix)]
fn unix_remote_moved_to_timerless_runtime_returns_connection_error() {
    let root = tempfile::tempdir().unwrap();
    let remote = UnixRemote::new(root.path().join("node.sock"), limits()).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(remote.request(&request(1)))
    }));
    assert!(matches!(result, Ok(Err(WireError::Connection))));
}
#[test]
fn quic_connector_moved_to_timerless_runtime_returns_connection_error() {
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let original = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let connector = original.block_on(async { connector(&pki, certificate, key) });
    let timerless = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        timerless.block_on(connector.connect("127.0.0.1:7443".parse().unwrap(), "localhost"))
    }));
    assert!(matches!(result, Ok(Err(WireError::Connection))));
    drop(connector);
    original.shutdown_background();
}
#[test]
fn quic_remote_moved_to_timerless_runtime_returns_connection_error() {
    let original = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (server, task, connector, remote) = original.block_on(async {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(1).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|request: VerifiedRequest| async move { response(request.request()) });
        let (server, task) = server(&pki, registry, handler).await;
        let connector = connector(&pki, certificate, key);
        let remote = connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await
            .unwrap();
        (server, task, connector, remote)
    });
    let timerless = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        timerless.block_on(remote.request(&request(1)))
    }));
    assert!(matches!(result, Ok(Err(WireError::Connection))));
    original.block_on(async {
        remote.close();
        server.close();
        task.await.unwrap().unwrap();
    });
    drop((remote, connector, server));
    original.shutdown_background();
}

#[test]
fn authenticated_connection_moved_to_timerless_runtime_returns_connection_error() {
    let original = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (server, client, connection, peer, registry) = original.block_on(async {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(true);
        let tls = server_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &limits(),
        )
        .unwrap();
        let server = quinn::Endpoint::server(tls, "127.0.0.1:0".parse().unwrap()).unwrap();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(1).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let tls = client_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &limits(),
        )
        .unwrap();
        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client.set_default_client_config(tls);
        let (connection, peer) = tokio::join!(
            async { server.accept().await.unwrap().await.unwrap() },
            async {
                client
                    .connect(server.local_addr().unwrap(), "localhost")
                    .unwrap()
                    .await
                    .unwrap()
            }
        );
        (server, client, connection, peer, registry)
    });
    let timerless = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        timerless.block_on(serve_authenticated_connection(
            connection,
            registry,
            limits(),
            |request: VerifiedRequest| async move { response(request.request()) },
        ))
    }));
    assert!(matches!(result, Ok(Err(WireError::Connection))));
    server.close(0u8.into(), b"test complete");
    client.close(0u8.into(), b"test complete");
    drop((peer, client, server));
    original.shutdown_background();
}

#[test]
fn placement_control_and_session_sign_are_node_only_certificate_bound_and_bounded() {
    let mut packet = request(101);
    packet.operation = Operation::PlacementControl {
        group: [7; 16],
        request: vec![1, 2],
    };
    assert_eq!(packet.operation.registered_tag(), 28);
    assert!(packet.operation.is_mutation());
    let mut sign = request(102);
    sign.operation = Operation::SessionSign {
        group: [7; 16],
        request: vec![3],
    };
    assert_eq!(sign.operation.registered_tag(), 29);
    assert!(!sign.operation.is_mutation());
    for role in [PeerRole::Actor, PeerRole::Evaluator, PeerRole::Runtime] {
        let mut value = grant();
        value.role = role;
        for envelope in [&packet, &sign] {
            assert!(matches!(
                verify_request(
                    AuthenticatedPeer::local(value.clone()).unwrap(),
                    envelope.clone(),
                    &limits()
                ),
                Err(AccessError::Unauthorized)
            ));
        }
    }
    // A trusted local Node grant carries no certificate and cannot speak for
    // a remote identity on either operation.
    let mut value = grant();
    value.role = PeerRole::Node { node_id: 7 };
    let local = AuthenticatedPeer::local(value.clone()).unwrap();
    for envelope in [&packet, &sign] {
        assert!(matches!(
            verify_request(local.clone(), envelope.clone(), &limits()),
            Err(AccessError::Unauthorized)
        ));
    }
    let registry = PeerRegistry::new(1).unwrap();
    let fingerprint = registry
        .register_certificate(b"transport-verified-placement-certificate", value)
        .unwrap();
    let node = registry.authenticate(fingerprint).unwrap();
    assert!(verify_request(node.clone(), packet.clone(), &limits()).is_ok());
    assert!(verify_request(node.clone(), sign.clone(), &limits()).is_ok());
    let reply = packet.reply(Response::Control {
        response: vec![1, 2],
    });
    validate_response(&packet, &reply, None, &limits()).unwrap();
    let reply = sign.reply(Response::Control {
        response: vec![1, 2],
    });
    validate_response(&sign, &reply, None, &limits()).unwrap();
    packet.operation = Operation::PlacementControl {
        group: [0; 16],
        request: vec![1, 2],
    };
    assert!(matches!(
        verify_request(node.clone(), packet.clone(), &limits()),
        Err(AccessError::InvalidRequest)
    ));
    packet.operation = Operation::PlacementControl {
        group: [7; 16],
        request: vec![1; MAX_PLACEMENT_CONTROL_REQUEST_BYTES + 1],
    };
    assert!(matches!(
        verify_request(node.clone(), packet.clone(), &limits()),
        Err(AccessError::Capacity)
    ));
    sign.operation = Operation::SessionSign {
        group: [7; 16],
        request: vec![1; MAX_SESSION_SIGN_REQUEST_BYTES + 1],
    };
    assert!(matches!(
        verify_request(node.clone(), sign.clone(), &limits()),
        Err(AccessError::Capacity)
    ));
    sign.operation = Operation::SessionSign {
        group: [7; 16],
        request: Vec::new(),
    };
    assert!(matches!(
        verify_request(node, sign, &limits()),
        Err(AccessError::InvalidRequest)
    ));
}

/// An unreachable peer is dialed once per cooldown, not once per send: a
/// send within the cooldown fails at once as `Lost` without a dial, so the
/// peer holds no send capacity for a second dial deadline, and the peer is
/// dialed again once the cooldown passes. Without this every send to a dead
/// peer ran its own dial to the deadline, and enough dead peers at once
/// filled the replication driver's send slots, queuing live followers'
/// appends behind them.
#[tokio::test]
async fn peer_pool_dials_an_unreachable_peer_once_per_cooldown_and_fails_the_rest_fast() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    // A UDP socket bound and dropped: nothing answers there.
    let stale = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    };
    let cooldown = Duration::from_millis(600);
    let pool = PeerConnectionPool::new(
        connector(&pki, certificate, key),
        PeerPoolLimits {
            attempts: 1,
            retry_backoff: Duration::ZERO,
            timeout: Duration::from_secs(10),
            unreachable_cooldown: cooldown,
            ..PeerPoolLimits::default()
        },
    )
    .unwrap();
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: stale,
                server_name: "localhost".into(),
                name: None,
            },
        )]),
    )
    .unwrap();
    let mut packet = request(83);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![1],
    };
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Lost));
    assert_eq!(pool.stats().dials, 1, "the first send dialed");
    let started = std::time::Instant::now();
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Lost));
    assert_eq!(
        pool.stats().dials,
        1,
        "a send within the cooldown does not dial"
    );
    assert!(
        started.elapsed() < cooldown,
        "the send failed at once, not after a dial deadline"
    );
    tokio::time::sleep(cooldown).await;
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Lost));
    assert_eq!(
        pool.stats().dials,
        2,
        "the peer is dialed again after the cooldown"
    );
    assert_eq!(pool.stats().connections_opened, 0);
    pool.close();
}

/// What an exchange with a peer is expected to take (27 §3.1 P1): measured
/// over the exchanges it answered, doubled for each one given up on, and
/// what a round's budget is derived from.
#[tokio::test]
async fn an_exchange_given_up_on_lengthens_what_its_peer_is_expected_to_take() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let held = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let holding = held.clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let gated = started.clone();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        let holding = holding.clone();
        let gated = gated.clone();
        async move {
            if holding.load(Ordering::SeqCst) {
                gated.notify_one();
                std::future::pending::<()>().await;
            }
            verified
                .request()
                .reply(Response::Control { response: vec![1] })
        }
    });
    let (server, task) = server(&pki, registry, handler).await;
    let ceiling = Duration::from_secs(30);
    let pool = Arc::new(
        PeerConnectionPool::new(
            connector(&pki, certificate, key),
            PeerPoolLimits {
                timeout: ceiling,
                ..PeerPoolLimits::default()
            },
        )
        .unwrap(),
    );
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address: server.local_addr().unwrap(),
                server_name: "localhost".into(),
                name: None,
            },
        )]),
    )
    .unwrap();
    let ask = |id: u128| {
        let mut packet = request(id);
        packet.operation = Operation::SessionSign {
            group: [2; 16],
            request: vec![1, 2, 3],
        };
        packet
    };
    let period = Duration::from_millis(100);
    // Nothing measured: a round with this peer has the pool's own deadline.
    assert_eq!(pool.exchange_tail(2), None);
    assert_eq!(
        pool.round_budget([2], period),
        focal_timing::RoundBudget::hard(ceiling)
    );
    // No peers: a round of one period.
    assert_eq!(
        pool.round_budget([], period).deadline_ns,
        period.as_nanos() as u64
    );
    for id in 0..8 {
        pool.send_placement(2, &ask(700 + id)).await.unwrap();
    }
    let measured = pool.exchange_tail(2).unwrap();
    assert!(
        measured > Duration::ZERO && measured < ceiling,
        "{measured:?}"
    );
    let budget = pool.round_budget([2], period);
    assert_eq!(budget.deadline_ns, period.max(measured).as_nanos() as u64);
    assert!(budget.max_deadline_ns() <= ceiling.as_nanos() as u64);
    // A replication message measures the path, not what an answer takes.
    let mut replication = request(720);
    replication.operation = Operation::Raft {
        group: [2; 16],
        message: vec![7],
    };
    held.store(true, Ordering::SeqCst);
    // Three exchanges given up on, as a round that ended would: each
    // doubles the expectation.
    let mut expected = measured;
    for id in 0..3 {
        let sending = pool.clone();
        let packet = ask(730 + id);
        let pending = tokio::spawn(async move { sending.send_placement(2, &packet).await });
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .unwrap();
        pending.abort();
        let _ = pending.await;
        expected *= 2;
        assert_eq!(pool.exchange_tail(2), Some(expected));
    }
    // An exchange it answers is a sample again, and ends the doubling.
    held.store(false, Ordering::SeqCst);
    pool.send_placement(2, &ask(740)).await.unwrap();
    let recovered = pool.exchange_tail(2).unwrap();
    assert!(recovered < expected, "{recovered:?} {expected:?}");
    // A route that leaves takes its measurements with it.
    pool.replace_routes(2, BTreeMap::new()).unwrap();
    assert_eq!(pool.exchange_tail(2), None);
    let _ = replication;
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
}

mod admission {
    //! Admission by identity (27 §3.1 P5), over real connections.
    use super::*;

    async fn admitting(
        pki: &Pki,
        registry: PeerRegistry,
        admission: AdmissionLimits,
    ) -> (
        Arc<QuicServer>,
        tokio::task::JoinHandle<Result<(), WireError>>,
    ) {
        let (certificate, key) = pki.issue(true);
        let tls = server_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &limits(),
        )
        .unwrap();
        let server = Arc::new(
            QuicServer::bind_admitting(
                "127.0.0.1:0".parse().unwrap(),
                tls,
                registry,
                limits(),
                admission,
            )
            .unwrap(),
        );
        let running = server.clone();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let task = tokio::spawn(async move { running.serve(handler).await });
        (server, task)
    }
    fn bounds() -> AdmissionLimits {
        AdmissionLimits {
            pending: 8,
            identities: 8,
            per_node: 4,
            per_participant: 2,
        }
    }
    /// What the server holds, once it says so: a connection's end reaches
    /// the server a moment after the client's close.
    async fn held(server: &QuicServer, connections: usize) -> AdmissionStats {
        // Charged to what the listener does: it ends when the listener has
        // changed nothing for the frozen window.
        let mut wait = focal_timing::ProgressDeadline::begin(
            &[server.admission().changes],
            u64::MAX,
            Duration::from_secs(30),
        );
        loop {
            let stats = server.admission();
            if stats.connections == connections && stats.pending == 0 {
                return stats;
            }
            if let Err(spent) = wait.check(&[stats.changes]) {
                panic!("the server never held {connections}: {spent}: {stats:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    async fn serves(remote: &QuicRemote, id: u128) -> bool {
        remote.request(&request(id)).await.is_ok()
    }

    #[test]
    fn pending_places_are_bounded_and_given_back() {
        assert_eq!(
            Admission::new(AdmissionLimits {
                pending: 0,
                ..bounds()
            })
            .err(),
            Some(AdmissionRefusal::InvalidLimits)
        );
        let admission = Admission::new(AdmissionLimits {
            pending: 2,
            ..bounds()
        })
        .unwrap();
        let first = admission.begin().unwrap();
        let second = admission.begin().unwrap();
        assert_eq!(admission.begin().err(), Some(AdmissionRefusal::Pending));
        assert_eq!(admission.stats().pending, 2);
        assert_eq!(admission.stats().refused_pending, 1);
        drop(first);
        let third = admission.begin().unwrap();
        assert_eq!(admission.begin().err(), Some(AdmissionRefusal::Pending));
        drop((second, third));
        assert_eq!(admission.stats().pending, 0);
        assert_eq!(admission.stats().connections, 0);
        assert_eq!(
            AdmissionLimits::for_connections(128),
            AdmissionLimits {
                pending: 32,
                identities: 128,
                per_node: 4,
                per_participant: 16,
            }
        );
        assert_eq!(AdmissionLimits::for_connections(1).pending, 1);
    }

    #[tokio::test]
    async fn a_node_past_its_bound_replaces_its_oldest_connection() {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node = grant();
        node.role = PeerRole::Node { node_id: 7 };
        registry.register_certificate(&certificate, node).unwrap();
        let (server, task) = admitting(&pki, registry, bounds()).await;
        let address = server.local_addr().unwrap();
        let connector = connector(&pki, certificate, key);
        let mut remotes = Vec::new();
        for _ in 0..4 {
            remotes.push(connector.connect(address, "localhost").await.unwrap());
        }
        let stats = held(&server, 4).await;
        assert_eq!((stats.identities, stats.replaced), (1, 0));
        for (index, remote) in remotes.iter().enumerate() {
            assert!(serves(remote, 400 + index as u128).await);
        }
        // The node dials again: it is served, and its oldest connection is
        // the one that ends.
        let fifth = connector.connect(address, "localhost").await.unwrap();
        assert!(serves(&fifth, 410).await);
        let stats = held(&server, 4).await;
        assert_eq!(
            (stats.identities, stats.replaced, stats.admitted),
            (1, 1, 5)
        );
        // The four were used in order, so the first is the one used least.
        assert!(!serves(&remotes[0], 411).await, "the oldest still serves");
        for (index, remote) in remotes.iter().enumerate().skip(1) {
            assert!(serves(remote, 420 + index as u128).await, "{index} ended");
        }
        // Closed connections give their charge back.
        for remote in remotes.iter().skip(1) {
            remote.close();
        }
        fifth.close();
        let stats = held(&server, 0).await;
        assert_eq!(stats.identities, 0);
        server.close();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn an_identity_past_its_bound_loses_the_connection_it_used_least() {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let (server, task) = admitting(&pki, registry, bounds()).await;
        let address = server.local_addr().unwrap();
        let connector = connector(&pki, certificate, key);
        let first = connector.connect(address, "localhost").await.unwrap();
        let second = connector.connect(address, "localhost").await.unwrap();
        held(&server, 2).await;
        // The first is used after the second was opened: the second is now
        // the one used least, and it is the one a third replaces.
        assert!(serves(&first, 500).await);
        let third = connector.connect(address, "localhost").await.unwrap();
        assert!(serves(&third, 501).await);
        let stats = held(&server, 2).await;
        assert_eq!((stats.replaced, stats.identities), (1, 1));
        assert!(
            serves(&first, 502).await,
            "the connection in use was closed"
        );
        assert!(
            !serves(&second, 503).await,
            "the idle connection still serves"
        );
        // Clients that leave without closing: a participant that runs one
        // short-lived client after another is always served.
        for round in 0..40u128 {
            let client = connector.connect(address, "localhost").await.unwrap();
            assert!(serves(&client, 510 + round).await, "round {round}");
            // Dropped without a close: the listener holds it until it is
            // replaced or idles out.
            std::mem::forget(client);
        }
        let stats = held(&server, 2).await;
        assert_eq!(stats.identities, 1);
        assert!(stats.replaced >= 40, "{stats:?}");
        server.close();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn identities_are_bounded_and_one_identity_cannot_take_anothers_place() {
        let pki = Pki::new();
        let registry = PeerRegistry::new(16).unwrap();
        let mut connectors = Vec::new();
        for principal in 1..=3u128 {
            let (certificate, key) = pki.issue(false);
            let mut grant = grant();
            grant.principal = ParticipantId::from_u128(principal);
            registry.register_certificate(&certificate, grant).unwrap();
            connectors.push(connector(&pki, certificate, key));
        }
        let (server, task) = admitting(
            &pki,
            registry,
            AdmissionLimits {
                identities: 2,
                ..bounds()
            },
        )
        .await;
        let address = server.local_addr().unwrap();
        // The first identity dials past its bound: it displaces its own
        // connections, and the second identity still has its place.
        let mut firsts = Vec::new();
        for _ in 0..5 {
            firsts.push(connectors[0].connect(address, "localhost").await.unwrap());
        }
        let c = connectors[1].connect(address, "localhost").await.unwrap();
        let stats = held(&server, 3).await;
        assert_eq!((stats.identities, stats.replaced), (2, 3));
        // A third identity finds no place, and takes none from the others.
        assert!(connectors[2].connect(address, "localhost").await.is_err());
        let stats = held(&server, 3).await;
        assert_eq!(stats.refused_identities, 1);
        assert!(serves(&firsts[4], 600).await && serves(&c, 602).await);
        // An identity that leaves frees its place.
        c.close();
        held(&server, 2).await;
        let d = connectors[2].connect(address, "localhost").await.unwrap();
        assert!(serves(&d, 603).await);
        server.close();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_certificate_that_does_not_authenticate_is_charged_to_no_identity() {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        // Signed by the authority, registered to nobody.
        let registry = PeerRegistry::new(16).unwrap();
        let (server, task) = admitting(&pki, registry, bounds()).await;
        let address = server.local_addr().unwrap();
        let connector = connector(&pki, certificate, key);
        for _ in 0..16 {
            assert!(connector.connect(address, "localhost").await.is_err());
        }
        let stats = held(&server, 0).await;
        assert_eq!((stats.identities, stats.admitted), (0, 0));
        server.close();
        task.await.unwrap().unwrap();
    }
}

#[test]
fn every_operation_has_its_class_and_control_goes_first() {
    assert!(TrafficClass::Control.priority() > TrafficClass::Exchange.priority());
    assert!(TrafficClass::Exchange.priority() > TrafficClass::Bulk.priority());
    assert!(TrafficClass::Control > TrafficClass::Exchange);
    assert!(TrafficClass::Exchange > TrafficClass::Bulk);
    let raft = Operation::Raft {
        group: [1; 16],
        message: vec![1],
    };
    assert_eq!(raft.class(), TrafficClass::Control);
    assert_eq!(Operation::Summary.class(), TrafficClass::Exchange);
    assert_eq!(download_request(1).operation.class(), TrafficClass::Bulk);
}
