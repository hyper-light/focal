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
    fn handle(&self, _request: VerifiedRequest) -> HandlerFuture<'_> {
        panic!("network dispatch must preserve the accounted handler override")
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
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
        records.push(verified.request().clone());
        let lose_first = records.len() == 1;
        async move {
            verified.request().reply(if lose_first {
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
        },
    )]);
    pool.replace_routes(1, routes.clone()).unwrap();
    let mut packet = request(81);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![7, 8, 9],
    };
    pool.send(2, &packet).await.unwrap();
    pool.send(2, &packet).await.unwrap();
    {
        let recorded = seen.lock().unwrap();
        assert_eq!(recorded.len(), 3);
        assert!(recorded.iter().all(|request| request == &packet));
    }
    assert_eq!(pool.stats().connections_opened, 2);
    assert_eq!(pool.stats().cached_connections, 1);
    assert_eq!(pool.stats().delivered, 2);
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
    packet.ledger.tenant = TenantId::from_u128(99);
    assert!(matches!(
        verify_request(node.clone(), packet.clone(), &limits()),
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
        let provider = Arc::new(rustls::crypto::ring::default_provider());
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
