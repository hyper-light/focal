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
/// What a test server's listener funds its bodies from: sixty-four frames
/// of the default limit, a quarter of them kept for the completion lane.
fn budget() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap()
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
        // The response's header arriving is the server's write begun; this
        // client reads no further, so the write cannot finish, and its
        // output charge is still held.
        let header = unfrozen(
            "the response never began",
            read_frame_header(&mut stream, FrameKind::Response, limits().max_frame_bytes),
        )
        .await
        .unwrap();
        assert!(budget.stats().used >= 900 * 1024 * 3);
        if !disconnect {
            let mut buffer = vec![0; header.payload_bytes()];
            let payload = read_frame_payload_into(&mut stream, header, &mut buffer)
                .await
                .unwrap();
            let response: ResponseEnvelope = decode_payload(payload).unwrap();
            assert!(
                matches!(response.result, Response::Content(ContentChunk { bytes, .. }) if bytes.len() == 900 * 1024)
            );
        }
        drop(stream);
        settles("the output charge was never given back", || {
            budget.stats().used == 0
        })
        .await;
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
        // The response's header arriving is the server's write begun; this
        // client reads no further, so the write cannot finish, and its
        // output charge is still held.
        let header = unfrozen(
            "the response never began",
            read_frame_header(&mut receive, FrameKind::Response, limits().max_frame_bytes),
        )
        .await
        .unwrap();
        assert!(budget.stats().used >= 900 * 1024 * 3);
        if !disconnect {
            let mut buffer = vec![0; header.payload_bytes()];
            let payload = read_frame_payload_into(&mut receive, header, &mut buffer)
                .await
                .unwrap();
            let response: ResponseEnvelope = decode_payload(payload).unwrap();
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
        settles("the output charge was never given back", || {
            budget.stats().used == 0
        })
        .await;
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
    server_at(pki, registry, handler, "127.0.0.1:0".parse().unwrap()).await
}
async fn server_at(
    pki: &Pki,
    registry: PeerRegistry,
    handler: Arc<dyn RequestHandler>,
    address: std::net::SocketAddr,
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
    let server = Arc::new(QuicServer::bind(address, tls, registry, limits(), budget()).unwrap());
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
    // The slow request is held until the fast one is answered: a fast one
    // queued behind it would wait for ever.
    let fast = unfrozen(
        "the fast request waited behind the slow one",
        remote.request(&request(2)),
    )
    .await
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

/// A raw connection through the Hello, for streams a client would not
/// send: a request header alone, or a body in pieces.
async fn raw_connection(
    pki: &Pki,
    certificate: Vec<u8>,
    key: Vec<u8>,
    to: std::net::SocketAddr,
) -> quinn::Connection {
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &limits(),
    )
    .unwrap();
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(tls);
    let connection = endpoint.connect(to, "localhost").unwrap().await.unwrap();
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
    connection
}
/// A request frame's header announcing `payload` bytes.
fn request_header(payload: u32) -> [u8; HEADER_BYTES] {
    let mut header = [0; HEADER_BYTES];
    header[..8].copy_from_slice(b"FOCALQ01");
    header[8..10].copy_from_slice(&1u16.to_be_bytes());
    header[10..12].copy_from_slice(&(FrameKind::Request as u16).to_be_bytes());
    header[12..].copy_from_slice(&payload.to_be_bytes());
    header
}
/// A wait on the listener's admission, charged to its changes.
/// How long what a wait observes may make no progress before the wait is
/// over: a wedge, not slowness (27 §3.1 P8). The in-process servers and
/// peers these tests wait on report no period, so where no counter is
/// observed this window is the wait's only bound.
const FROZEN: Duration = Duration::from_secs(60);

/// What `future` yields, unless it yields nothing for [`FROZEN`].
async fn unfrozen<F: std::future::Future>(what: &str, future: F) -> F::Output {
    match tokio::time::timeout(FROZEN, future).await {
        Ok(output) => output,
        Err(_) => panic!("{what}: nothing for {FROZEN:?}"),
    }
}

/// Polls until `settled` holds, unless it does not for [`FROZEN`].
async fn settles(what: &str, settled: impl Fn() -> bool) {
    unfrozen(what, async {
        while !settled() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

async fn admission_settles(
    server: &QuicServer,
    settled: impl Fn(&AdmissionStats) -> bool,
) -> AdmissionStats {
    let mut wait =
        focal_timing::ProgressDeadline::begin(&[server.admission().changes], u64::MAX, FROZEN);
    loop {
        let stats = server.admission();
        if settled(&stats) {
            return stats;
        }
        if let Err(spent) = wait.check(&[stats.changes]) {
            panic!("the admission never settled: {spent}: {stats:?}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The audit's F35: a grant revoked while a request's body is still
/// arriving. The request is refused when it is complete — what is
/// dispatched is authorized by the grant current then — and the
/// revocation closes the certificate's connection, so nothing more is
/// received on it and what waited for the body ends with it.
#[tokio::test]
async fn a_grant_revoked_while_a_body_arrives_dispatches_nothing_and_closes_the_connection() {
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let fingerprint = registry
        .register_certificate(&certificate, grant())
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        observed.fetch_add(1, Ordering::SeqCst);
        async move { response(verified.request()) }
    });
    let (server, task) = server(&pki, registry.clone(), handler).await;
    let connection = raw_connection(&pki, certificate, key, server.local_addr().unwrap()).await;
    assert_eq!(registry.live_connections(fingerprint), 1);
    let body = encode_payload(&request(1), limits().max_frame_bytes).unwrap();
    let (mut send, mut receive) = connection.open_bi().await.unwrap();
    send.write_all(&request_header(body.len() as u32))
        .await
        .unwrap();
    send.write_all(&body[..body.len() / 2]).await.unwrap();
    // The half body is held under its permit before the revocation.
    admission_settles(&server, |stats| stats.bytes == body.len()).await;
    registry.revoke(fingerprint).unwrap();
    // The connection is closed by the revocation: the rest of the body
    // has nowhere to go, and the permit it held is given back.
    // Bounded by the connection's idle timeout (the test limits'): a close
    // that never came would end it as TimedOut, which is refused below.
    let closed = connection.closed().await;
    assert!(
        matches!(
            &closed,
            quinn::ConnectionError::ApplicationClosed(close) if close.reason.as_ref() == b"revoked"
        ),
        "{closed:?}"
    );
    assert!(send.write_all(&body[body.len() / 2..]).await.is_err());
    assert!(receive.read_to_end(64).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(registry.live_connections(fingerprint), 0);
    let stats =
        admission_settles(&server, |stats| stats.bytes == 0 && stats.connections == 0).await;
    assert_eq!(stats.bytes, 0);
    server.close();
    task.await.unwrap().unwrap();
}

/// The audit's F35, the other order: a body that completes after the
/// grant was withdrawn but before the close reaches its stream is refused
/// at dispatch by the grant current then. The registry is asked again
/// once the whole request has arrived; the early check alone would have
/// dispatched it.
#[tokio::test]
async fn a_complete_request_is_authorized_by_the_grant_current_at_dispatch() {
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let fingerprint = registry
        .register_certificate(&certificate, grant())
        .unwrap();
    let revoking = registry.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    // The handler is where a dispatched request lands; the registry's
    // grant is withdrawn by the first request while it runs, and the
    // second request, sent on the same connection before the close, must
    // never land.
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        observed.fetch_add(1, Ordering::SeqCst);
        let revoking = revoking.clone();
        async move {
            if verified.request().request_id == RequestId::from_u128(1) {
                revoking.revoke(fingerprint).unwrap();
            }
            response(verified.request())
        }
    });
    let (server, task) = server(&pki, registry.clone(), handler).await;
    let connector = connector(&pki, certificate, key);
    let remote = connector
        .connect(server.local_addr().unwrap(), "localhost")
        .await
        .unwrap();
    // The first request revokes the grant from inside the handler: its
    // own answer is lost with the connection, and nothing after it is
    // served.
    let first = remote.request(&request(1)).await;
    assert!(first.is_err(), "{first:?}");
    assert!(remote.request(&request(2)).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(registry.live_connections(fingerprint), 0);
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
    // The first message was answered `Unavailable` and asked again: on the
    // connection that carried the refusal, not on a new one.
    assert_eq!(pool.stats().connections_opened, 1);
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

/// One operation that its peer refuses, or cannot say the outcome of, or
/// never answers, takes no other exchange with it (the audit's F37: the
/// pool closed the connection for it, and every exchange with the peer —
/// other groups' messages, probes, content under way — was lost with it,
/// and paid a handshake and a congestion window learned again). A request
/// held by the peer while another is refused is answered; one in flight
/// while another times out is answered; what the caller is told of a
/// refusal is that the peer refused; and one connection is opened through
/// all of it.
#[tokio::test]
async fn an_operation_refused_or_unanswered_takes_no_other_exchange_with_its_connection() {
    use std::collections::BTreeMap;
    use tokio::sync::{Notify, Semaphore};
    const HELD: u128 = 101;
    const REFUSED: u128 = 102;
    const UNKNOWN: u128 = 103;
    const SILENT: u128 = 104;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let held = Arc::new(Notify::new());
    let silent = Arc::new(Notify::new());
    let (release, entered, asked) = (gate.clone(), held.clone(), silent.clone());
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        let (release, entered, asked) = (release.clone(), entered.clone(), asked.clone());
        async move {
            let id = u128::from_be_bytes(verified.request().request_id.0);
            let answer = match id {
                _ if matches!(verified.request().operation, Operation::Probe { .. }) => {
                    Response::Probe(vec![1])
                }
                HELD => {
                    entered.notify_one();
                    release.acquire().await.unwrap().forget();
                    Response::PeerAccepted
                }
                REFUSED => Response::Error(AccessError::Unavailable),
                UNKNOWN => Response::Error(AccessError::OutcomeUnknown),
                SILENT => {
                    asked.notify_one();
                    std::future::pending().await
                }
                _ => Response::PeerAccepted,
            };
            verified.request().reply(answer)
        }
    });
    let (server, task) = server(&pki, registry, handler).await;
    // An exchange is given two seconds; a request is asked twice.
    let wire = WireLimits {
        request_timeout: Duration::from_secs(2),
        ..limits()
    };
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let pool = Arc::new(
        PeerConnectionPool::new(
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire).unwrap(),
            PeerPoolLimits {
                attempts: 2,
                retry_backoff: Duration::ZERO,
                timeout: Duration::from_secs(10),
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
    let message = |id: u128| {
        let mut packet = request(id);
        packet.operation = Operation::Raft {
            group: [2; 16],
            message: vec![7, 8, 9],
        };
        packet
    };
    let send = |id: u128| {
        let pool = pool.clone();
        let packet = message(id);
        tokio::spawn(async move { pool.send(2, &packet).await })
    };
    pool.send(2, &message(100)).await.unwrap();
    assert_eq!(pool.stats().connections_opened, 1);

    // A request the peer holds, while it refuses two others.
    let waiting = send(HELD);
    held.notified().await;
    assert_eq!(
        pool.send(2, &message(REFUSED)).await,
        Err(PeerSendError::Rejected(AccessError::Unavailable))
    );
    assert_eq!(
        pool.send(2, &message(UNKNOWN)).await,
        Err(PeerSendError::Rejected(AccessError::OutcomeUnknown))
    );
    gate.add_permits(1);
    assert_eq!(waiting.await.unwrap(), Ok(()));
    assert_eq!(pool.stats().connections_opened, 1);

    // A request the peer's handler never answers. The peer answers probes
    // meanwhile, as it does a node's liveness. The request ends when the
    // peer gives its handler up and says so, is asked again on the same
    // connection, and ends the same way. A second request is sent a second
    // into the first, held by the peer, and let go once the first has been
    // asked again: it was in flight across that, and is answered.
    let probing = {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut probe = request(200);
            probe.operation = Operation::Probe {
                request: vec![1, 2, 3],
            };
            loop {
                assert_eq!(pool.send_probe(2, &probe).await, Ok(vec![1]));
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    let unanswered = send(SILENT);
    silent.notified().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let waiting = send(HELD);
    held.notified().await;
    // Asked again: its first exchange ended without what it asked for.
    silent.notified().await;
    gate.add_permits(1);
    assert_eq!(waiting.await.unwrap(), Ok(()));
    // Told at last that it is not to be had, or never told: lost to its
    // caller either way, and to no one else.
    let unanswered = unanswered.await.unwrap();
    assert!(
        matches!(
            unanswered,
            Err(PeerSendError::Lost | PeerSendError::Rejected(AccessError::Unavailable))
        ),
        "{unanswered:?}"
    );
    probing.abort();
    let _ = probing.await;
    pool.send(2, &message(105)).await.unwrap();
    assert_eq!(pool.stats().connections_opened, 1);
    assert_eq!(pool.stats().cached_connections, 1);
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
}

/// When an exchange fails on the wire, its connection is closed for it only
/// if the connection is what failed. A stream that timed out, ended early
/// or could not be read while the peer answered other exchanges on the same
/// connection failed alone; with no answer at all since it was sent there
/// is no evidence the connection carries anything; and a connection that
/// has ended, could not be trusted, or on which the peer did not speak the
/// protocol, is closed whatever else was answered.
#[test]
fn a_connection_is_closed_for_an_exchange_only_when_the_connection_failed() {
    use crate::peers::connection_failed;
    let stream = || {
        [
            WireError::Timeout,
            WireError::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            WireError::Allocation,
        ]
    };
    for failure in stream() {
        assert!(!connection_failed(false, &failure, true), "{failure:?}");
        assert!(connection_failed(false, &failure, false), "{failure:?}");
        assert!(connection_failed(true, &failure, true), "{failure:?}");
    }
    for failure in [
        WireError::Connection,
        WireError::Authentication,
        WireError::InvalidFrame,
    ] {
        assert!(connection_failed(false, &failure, true), "{failure:?}");
    }
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
    // Every caller allows far less than the dead address's deadline. The
    // one dial ends by that deadline: a caller is answered through the
    // name's fresh address before it, or the caller after it fails.
    unfrozen("no caller ever reached the moved peer", async {
        loop {
            match tokio::time::timeout(Duration::from_millis(200), pool.send(2, &packet)).await {
                Ok(Ok(_)) => break,
                Ok(Err(error)) => panic!("the send failed rather than timing out: {error}"),
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await;
    // One dial, bounded by the dead address's deadline, delivered: the
    // name's fresh address won it.
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
    // The dial decides on its own after the callers left, by its deadline.
    // A send joins it and ends with its outcome, which marks the peer
    // unreachable before anyone is told; the next send is refused at once.
    for _ in 0..2 {
        let result = unfrozen("a send outlived the dial", pool.send(2, &packet)).await;
        assert_eq!(result, Err(PeerSendError::Lost));
    }
    let stats = pool.stats();
    assert!(
        stats.refused_unreachable >= 1,
        "the abandoned dial never marked the peer unreachable: {stats:?}"
    );
    assert_eq!(
        stats.dials, 1,
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
        unfrozen("the request never reached the peer", started.notified()).await;
        pool.replace_routes(revision + 1, BTreeMap::new()).unwrap();
        let ended = unfrozen("the request outlived its retired route", pending)
            .await
            .unwrap();
        // Ended by the retirement: its deadline would have ended it as Lost.
        assert!(
            matches!(
                ended,
                Err(PeerSendError::NoRoute | PeerSendError::RouteChanged)
            ),
            "{ended:?}"
        );
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
        let ended = unfrozen("a send outlived its retired route", pending)
            .await
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
    unfrozen("the request never reached the peer", started.notified()).await;
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

/// A path between a client and a server, as a relay shapes it.
#[derive(Clone, Copy, Debug)]
struct Shape {
    /// What the path carries in a second toward the server, and back.
    up_bits: u64,
    down_bits: u64,
    /// How long a datagram travels once it has left the bottleneck, each
    /// way, and how much longer at most (drawn for each datagram).
    delay: Duration,
    jitter: Duration,
    /// Datagrams lost in a million, each way.
    loss_ppm: u32,
    /// The datagrams the bottleneck holds; one more is dropped.
    queue: usize,
    /// A time, from the relay's start, in which nothing is carried.
    outage: Option<(Duration, Duration)>,
    seed: u64,
}
impl Shape {
    fn even(bits: u64) -> Self {
        Self {
            up_bits: bits,
            down_bits: bits,
            delay: Duration::ZERO,
            jitter: Duration::ZERO,
            loss_ppm: 0,
            queue: 32,
            outage: None,
            seed: 1,
        }
    }
}

/// A path between a client and `server` shaped as `shape` says: a datagram
/// waits its turn at the bottleneck behind those before it, one that finds
/// the queue full is dropped, and what leaves the bottleneck travels its
/// delay.
async fn shaped(
    server: std::net::SocketAddr,
    shape: Shape,
) -> (std::net::SocketAddr, Vec<tokio::task::JoinHandle<()>>) {
    let front = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let back = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let address = front.local_addr().unwrap();
    let client = Arc::new(std::sync::Mutex::new(None::<std::net::SocketAddr>));
    let began = tokio::time::Instant::now();
    let mut tasks = Vec::new();
    for up in [true, false] {
        let (from, to) = if up {
            (front.clone(), back.clone())
        } else {
            (back.clone(), front.clone())
        };
        let bits = if up { shape.up_bits } else { shape.down_bits };
        let (queue, mut waiting) = tokio::sync::mpsc::channel::<Vec<u8>>(shape.queue);
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
            let mut random = shape.seed ^ if up { 0x9E37_79B9_7F4A_7C15 } else { 0 };
            let mut draw = move || {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                random
            };
            let mut free = tokio::time::Instant::now();
            while let Some(datagram) = waiting.recv().await {
                free = free.max(tokio::time::Instant::now())
                    + Duration::from_nanos(datagram.len() as u64 * 8 * 1_000_000_000 / bits);
                tokio::time::sleep_until(free).await;
                let lost = draw() % 1_000_000 < u64::from(shape.loss_ppm);
                let out = shape.outage.is_some_and(|(from, length)| {
                    let at = began.elapsed();
                    at >= from && at < from + length
                });
                let target = if up {
                    Some(server)
                } else {
                    *known.lock().unwrap()
                };
                let Some(target) = target else { continue };
                if lost || out {
                    continue;
                }
                let travel = shape.delay
                    + Duration::from_nanos(
                        draw() % (shape.jitter.as_nanos() as u64).saturating_add(1),
                    );
                if travel.is_zero() {
                    let _ = to.send_to(&datagram, target).await;
                } else {
                    // No more travel at once than the bottleneck lets out
                    // in the longest travel.
                    let to = to.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(travel).await;
                        let _ = to.send_to(&datagram, target).await;
                    });
                }
            }
        }));
    }
    (address, tasks)
}

/// A path that carries `bits` in a second each way, and nothing else of it
/// shaped.
async fn narrow(
    server: std::net::SocketAddr,
    bits: u64,
) -> (std::net::SocketAddr, Vec<tokio::task::JoinHandle<()>>) {
    shaped(server, Shape::even(bits)).await
}

/// A payload that keeps arriving is never given up, however slowly it
/// comes against the path's round trip, and one that stops is given up
/// within a judgement (`Arriving`, the port's finding). It was priced by
/// its residency — two datagrams a probe timeout of the longest round trip
/// the path showed while it arrived — which a sender limited by its path
/// alone keeps and one that writes as it has, shares its connection or is
/// short of CPU does not: a datagram every five milliseconds on a path of
/// one millisecond, three times slower than that pace, was given up with
/// most of the payload arriving.
#[tokio::test(start_paused = true)]
async fn a_payload_that_keeps_arriving_is_never_given_up_and_one_that_stops_is() {
    const PAYLOAD: usize = 256 * 1024;
    const EVERY: Duration = Duration::from_millis(5);
    let wait = Duration::from_millis(100);
    // A datagram every five milliseconds, `stop_after` of them at most.
    let send = |mut writer: tokio::io::DuplexStream, stop_after: usize| async move {
        use tokio::io::AsyncWriteExt;
        let datagram = [7u8; LEAST_PROGRESS];
        let mut left = PAYLOAD;
        let mut sent = 0usize;
        while left > 0 && sent < stop_after {
            let take = left.min(datagram.len());
            if writer.write_all(&datagram[..take]).await.is_err() {
                return;
            }
            left -= take;
            sent += 1;
            tokio::time::sleep(EVERY).await;
        }
        // Stopped: the stream stays open and silent.
        std::future::pending::<()>().await;
    };
    let read = |stop_after: usize| async move {
        let (writer, mut reader) = tokio::io::duplex(64 * 1024);
        let sender = tokio::spawn(send(writer, stop_after));
        let frame = read_frame_header(
            &mut &request_header(PAYLOAD as u32)[..],
            FrameKind::Request,
            PAYLOAD as u32,
        )
        .await
        .unwrap();
        let began = tokio::time::Instant::now();
        let alone = crate::frame::AloneDelivery::default();
        let arrived: Result<Vec<u8>, WireError> = read_payload_arriving(
            &mut reader,
            frame,
            wait,
            || Duration::from_millis(1),
            &alone,
            0,
        )
        .await;
        sender.abort();
        (arrived.map(|_| ()), began.elapsed(), alone)
    };
    // Sent to the end: the payload arrives, 1.1 s after it began, three
    // times the residency it was given before. It is not a frame's encoding,
    // so it is read as far as its decoding, which the path does not answer
    // for; and the delivery it was declared to has nothing of it left owed.
    let (arrived, took, alone) = read(usize::MAX).await;
    assert!(
        !matches!(arrived, Err(WireError::Timeout)),
        "given up after {took:?}"
    );
    assert!(took >= Duration::from_secs(1), "{took:?}");
    assert_eq!(alone.backlog(0), 0);
    assert_eq!(alone.delivered(0), PAYLOAD as u64);
    // Stopped a third of the way in: given up at the first judgement that
    // brought less than a datagram, one wait after the last datagram, and
    // what was not read is released.
    let stop_after = PAYLOAD / LEAST_PROGRESS / 3;
    let (given_up, after, alone) = read(stop_after).await;
    assert!(matches!(given_up, Err(WireError::Timeout)), "{given_up:?}");
    let sent = EVERY * stop_after as u32;
    assert!(
        after >= sent && after <= sent + wait * 2,
        "{after:?} for {sent:?}"
    );
    assert_eq!(alone.backlog(0), 0);
    assert_eq!(alone.delivered(0), (stop_after * LEAST_PROGRESS) as u64);
}

/// The judgement a body's arrival is charged by, apart from any stream
/// (`Arriving`): what hyper-raft's port of the law tests of its own.
#[test]
fn a_bodys_arrival_is_charged_with_what_arrives_against_what_is_owed() {
    use crate::frame::{Arriving, Moved};
    const PERIOD: Duration = Duration::from_millis(100);
    let moved = |received: u64, delivered: u64| Moved {
        received,
        delivered,
    };
    let start = tokio::time::Instant::now();
    // A megabyte at 12,000 bytes a period, 84 periods: never cut off while
    // bytes keep arriving, however short the path's round trip.
    let mut body = Arriving::begin(start, moved(0, 0), 1_000_000, 1_000_000, PERIOD);
    for period in 1..=83u32 {
        let arrived = 12_000 * u64::from(period);
        body.judge(
            start + PERIOD * period,
            moved(arrived, arrived),
            1_000_000 - arrived,
            PERIOD,
        )
        .unwrap();
        body.arrived(12_000);
    }
    // One that stops arriving ends at the period that brought less than a
    // datagram; one not yet due is not judged.
    let mut body = Arriving::begin(start, moved(0, 0), 24_000, 24_000, PERIOD);
    body.judge(start + PERIOD / 2, moved(0, 0), 24_000, PERIOD)
        .unwrap();
    assert_eq!(
        body.judge(start + PERIOD, moved(100, 100), 24_000, PERIOD),
        Err(crate::frame::GiveUp::Quiet)
    );
    // A body of fewer bytes than a datagram needs only itself.
    let mut tail = Arriving::begin(start, moved(0, 0), 300, 300, PERIOD);
    tail.arrived(300);
    tail.judge(start + PERIOD, moved(300, 300), 0, PERIOD)
        .unwrap();
    // A body the peer withholds while it delivers others: a more urgent
    // class's megabyte is not charged; the other bodies owed are; a period
    // after everything owed was delivered, still busy and still not this
    // body, it ends.
    let mut withheld = Arriving::begin(start, moved(0, 0), 10_000, 60_000, PERIOD);
    withheld
        .judge(start + PERIOD, moved(1_000_000, 0), 60_000, PERIOD)
        .unwrap();
    withheld
        .judge(start + PERIOD * 2, moved(1_050_000, 50_000), 20_000, PERIOD)
        .unwrap();
    withheld
        .judge(start + PERIOD * 3, moved(1_060_000, 60_000), 10_000, PERIOD)
        .unwrap();
    assert_eq!(
        withheld.judge(start + PERIOD * 4, moved(2_000_000, 60_000), 10_000, PERIOD),
        Err(crate::frame::GiveUp::Withheld)
    );
    // The judgement stretches with the path: a period that the longest
    // round trip's probe timeout exceeds is that probe timeout — three
    // round trips and the acknowledgement delay the peer may take.
    assert_eq!(
        crate::frame::judgement(PERIOD, Duration::from_millis(10)),
        PERIOD
    );
    assert_eq!(
        crate::frame::judgement(PERIOD, Duration::from_millis(50)),
        Duration::from_millis(175)
    );
}

/// A megabyte over a path that takes longer to carry it than a request is
/// given is carried, each way: an exchange waits as long as the path takes
/// (`Carriage`, `read_payload_arriving`), and no longer for a peer that
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
        QuicServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            tls,
            registry,
            wire.clone(),
            budget(),
        )
        .unwrap(),
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
    // carried what was asked within one wait. The peer is given its
    // period and what the path takes to carry the first of an answer
    // (`Carriage`); on a path whose round trip the megabyte stretched,
    // that can outlast the peer's own time for its handler (one second
    // here), and then the peer says it gave the request up: `Unavailable`
    // when it had not begun, `OutcomeUnknown` when its handler was given
    // up on (a cancel is a mutation; `dispatch_accounted`). Which of the
    // two is the peer's scheduling (the Windows run saw the second).
    let began = std::time::Instant::now();
    let unanswered = remote
        .request_within(
            &custody(3, CustodyRequest::Cancel { transfer: [1; 16] }),
            Duration::from_millis(300),
        )
        .await;
    assert!(
        matches!(
            &unanswered,
            Err(WireError::Timeout)
                | Ok(ResponseEnvelope {
                    result: Response::Error(AccessError::Unavailable | AccessError::OutcomeUnknown),
                    ..
                })
        ),
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
                budget(),
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
                    budget(),
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
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Lost));
    assert_eq!(
        pool.stats().dials,
        1,
        "a send within the cooldown does not dial"
    );
    assert_eq!(
        pool.stats().refused_unreachable,
        1,
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

/// A liveness probe is never refused for a dial cooldown, and a peer that
/// answers ends its cooldown. A send refused in the cooldown never reached
/// the peer, so for the failure detector it is no probe at all: the detector
/// read the instant `Lost` as a probe unanswered, and a node that came back
/// from a pause, dialed in vain while it was gone, was suspected and
/// declared dead again while it ran (the zone stage on macOS CI). The probe
/// dials; its connection then carries replication at once.
#[tokio::test]
async fn a_probe_dials_through_a_cooldown_and_a_peer_that_answers_ends_it() {
    use std::collections::BTreeMap;
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    // An address nothing answers at yet: a UDP socket bound and dropped.
    let address = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    };
    // A cooldown longer than the test: only an answer can end it.
    let pool = PeerConnectionPool::new(
        connector(&pki, certificate.clone(), key),
        PeerPoolLimits {
            attempts: 1,
            retry_backoff: Duration::ZERO,
            timeout: Duration::from_secs(10),
            unreachable_cooldown: Duration::from_secs(60),
            ..PeerPoolLimits::default()
        },
    )
    .unwrap();
    pool.replace_routes(
        1,
        BTreeMap::from([(
            2,
            PeerEndpoint {
                address,
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
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Lost));
    assert_eq!(
        pool.stats().dials,
        1,
        "the peer was dialed and did not answer"
    );
    // The peer comes back where it was.
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let handler: Arc<dyn RequestHandler> = Arc::new(|verified: VerifiedRequest| async move {
        let answer = if matches!(verified.request().operation, Operation::Probe { .. }) {
            Response::Probe(vec![1])
        } else {
            Response::PeerAccepted
        };
        verified.request().reply(answer)
    });
    let (server, task) = server_at(&pki, registry, handler, address).await;
    // Replication within the cooldown is still spared its dial...
    assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Lost));
    assert_eq!(pool.stats().refused_unreachable, 1);
    assert_eq!(pool.stats().dials, 1);
    // ...but a probe dials, and is answered.
    let mut probe = request(86);
    probe.operation = Operation::Probe {
        request: vec![1, 2, 3],
    };
    assert_eq!(pool.send_probe(2, &probe).await, Ok(vec![1]));
    assert_eq!(
        pool.stats().dials,
        2,
        "the probe dialed through the cooldown"
    );
    // The peer answered: replication goes at once on the probe's connection.
    let mut after = request(87);
    after.operation = packet.operation.clone();
    pool.send(2, &after).await.unwrap();
    assert_eq!(pool.stats().refused_unreachable, 1, "the cooldown ended");
    assert_eq!(pool.stats().connections_opened, 1);
    pool.close();
    server.close();
    task.await.unwrap().unwrap();
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
        unfrozen("the exchange never reached the peer", started.notified()).await;
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
                budget(),
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
            connections: 16,
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
            Admission::new(
                AdmissionLimits {
                    pending: 0,
                    ..bounds()
                },
                budget()
            )
            .err(),
            Some(AdmissionRefusal::InvalidLimits)
        );
        let admission = Admission::new(
            AdmissionLimits {
                pending: 2,
                ..bounds()
            },
            budget(),
        )
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
                connections: 128,
                per_node: 4,
                per_participant: 16,
            }
        );
        assert_eq!(AdmissionLimits::for_connections(1).pending, 1);
    }

    /// The audit's F20: the connections held in all are bounded after the
    /// replacement rule. With the listener full, an identity at its own
    /// bound still reaches its replacement; only a connection that would
    /// be one more is refused, typed and counted.
    #[tokio::test]
    async fn a_full_listener_still_replaces_an_identity_s_own_connection() {
        let pki = Pki::new();
        let (first_certificate, first_key) = pki.issue(false);
        let (second_certificate, second_key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&first_certificate, grant())
            .unwrap();
        let mut second = grant();
        second.principal = ParticipantId::from_u128(2);
        registry
            .register_certificate(&second_certificate, second)
            .unwrap();
        // Three connections in all; a participant holds two.
        let (server, task) = admitting(
            &pki,
            registry,
            AdmissionLimits {
                connections: 3,
                ..bounds()
            },
        )
        .await;
        let address = server.local_addr().unwrap();
        let first = connector(&pki, first_certificate, first_key);
        let second = connector(&pki, second_certificate, second_key);
        let first_a = first.connect(address, "localhost").await.unwrap();
        let first_b = first.connect(address, "localhost").await.unwrap();
        let second_a = second.connect(address, "localhost").await.unwrap();
        assert!(
            serves(&first_a, 1).await && serves(&first_b, 2).await && serves(&second_a, 3).await
        );
        assert_eq!(held(&server, 3).await.connections, 3);
        // The second identity, under its own bound, would be one more:
        // refused at its handshake, and the three it did not displace
        // serve on.
        assert!(second.connect(address, "localhost").await.is_err());
        let stats = held(&server, 3).await;
        assert_eq!(stats.refused_connections, 1);
        assert!(serves(&first_a, 5).await && serves(&second_a, 6).await);
        // The first identity, at its bound, replaces the connection it
        // used least recently although the listener is full.
        let first_c = first.connect(address, "localhost").await.unwrap();
        assert!(serves(&first_c, 7).await);
        let stats = held(&server, 3).await;
        assert_eq!(stats.replaced, 1);
        assert!(!serves(&first_b, 8).await, "the least used was replaced");
        assert!(serves(&first_a, 9).await);
        drop((first_a, first_b, first_c, second_a));
        held(&server, 0).await;
        server.close();
        task.await.unwrap().unwrap();
    }

    /// The audit's F03: a body is permitted before it is allocated, within
    /// its identity's share of the listener's budget. A header announcing
    /// the largest frame holds a permit for it while nothing arrives; a
    /// second such header from the same identity is refused for its share,
    /// typed and counted, and the permit is given back with the stream.
    #[tokio::test]
    async fn a_body_is_permitted_before_it_is_allocated_within_the_identity_s_share() {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let frame = limits().max_frame_bytes;
        // A budget of one frame and a little: one body's permit fits it,
        // and one identity's share of it is the frame.
        let (tls_certificate, tls_key) = pki.issue(true);
        let tls = server_tls(
            TlsIdentity::from_pkcs8(vec![tls_certificate], tls_key),
            vec![pki.ca.der().to_vec()],
            &limits(),
        )
        .unwrap();
        let budget = MemoryBudget::new(frame as usize + 64 * 1024, 0).unwrap();
        let server = Arc::new(
            QuicServer::bind_admitting(
                "127.0.0.1:0".parse().unwrap(),
                tls,
                registry,
                limits(),
                bounds(),
                budget.clone(),
            )
            .unwrap(),
        );
        let running = server.clone();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let task = tokio::spawn(async move { running.serve(handler).await });
        let connection = raw_connection(&pki, certificate, key, server.local_addr().unwrap()).await;
        let (mut first, _first_receive) = connection.open_bi().await.unwrap();
        first.write_all(&request_header(frame)).await.unwrap();
        let stats = admission_settles(&server, |stats| stats.bytes == frame as usize).await;
        assert_eq!(stats.refused_bytes, 0);
        // The permit is what the budget holds for the body, before a byte
        // of it arrived.
        assert!(
            budget.stats().used >= frame as usize,
            "{:?}",
            budget.stats()
        );
        // A second body of a frame would take more than the identity's
        // share: refused before any allocation, the stream reset.
        let (mut second, mut second_receive) = connection.open_bi().await.unwrap();
        second.write_all(&request_header(frame)).await.unwrap();
        let stats = admission_settles(&server, |stats| stats.refused_bytes == 1).await;
        assert_eq!(stats.bytes, frame as usize);
        assert!(second_receive.read_to_end(64).await.is_err());
        // The first stream ends without its body: its permit is given back.
        drop(first);
        let stats = admission_settles(&server, |stats| stats.bytes == 0).await;
        assert_eq!(stats.refused_bytes, 1);
        assert!(budget.stats().used < frame as usize);
        drop(connection);
        server.close();
        task.await.unwrap().unwrap();
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

/// The audit's F60: twenty-four cold calls to one route dial once and share
/// the connection — none is replaced under a dispatched call; a failure
/// reported for a generation no longer cached forgets nothing; a caller that
/// gives up under its own deadline does not abandon the dial.
#[tokio::test]
async fn cold_calls_to_one_route_share_one_dial_and_a_stale_failure_forgets_nothing() {
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    registry
        .register_certificate(&certificate, grant())
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler: Arc<dyn RequestHandler> = {
        let calls = calls.clone();
        Arc::new(move |verified: VerifiedRequest| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                // Long enough for the cold calls to overlap on the wire.
                tokio::time::sleep(Duration::from_millis(20)).await;
                response(verified.request())
            }
        })
    };
    let (server, task) = server(&pki, registry, handler).await;
    let connector = connector(&pki, certificate, key);
    let address = server.local_addr().unwrap().to_string();
    let routes = Arc::new(RouteConnections::new(connector, 1).unwrap());
    let mut waves = tokio::task::JoinSet::new();
    for id in 1..=24u128 {
        let routes = routes.clone();
        let address = address.clone();
        waves.spawn(async move {
            let connected = routes.connect(&address, "localhost").await?;
            connected
                .remote
                .request(&request(id))
                .await
                .map(|reply| (id, reply))
        });
    }
    let mut answered = 0;
    while let Some(joined) = waves.join_next().await {
        let (id, reply) = joined.unwrap().unwrap();
        assert_eq!(reply, response(&request(id)));
        answered += 1;
    }
    assert_eq!(answered, 24);
    assert_eq!(calls.load(Ordering::SeqCst), 24);
    assert_eq!(routes.dials(), 1, "one dial for every cold call");
    let stats = server.admission();
    assert_eq!((stats.admitted, stats.replaced), (1, 0), "{stats:?}");
    // A failure reported for a generation no longer cached forgets nothing;
    // one for the cached generation lets the route be dialed again.
    let current = routes.connect(&address, "localhost").await.unwrap();
    routes.forget(&address, "localhost", current.generation.wrapping_add(1));
    assert_eq!(
        routes
            .connect(&address, "localhost")
            .await
            .unwrap()
            .generation,
        current.generation
    );
    assert_eq!(routes.dials(), 1);
    routes.forget(&address, "localhost", current.generation);
    let fresh = routes.connect(&address, "localhost").await.unwrap();
    assert_ne!(fresh.generation, current.generation);
    assert_eq!(routes.dials(), 2);
    // A caller that gives up under its own deadline does not abandon the
    // dial: the next caller finds it, and no third dial is started.
    routes.forget(&address, "localhost", fresh.generation);
    let gave_up = tokio::time::timeout(
        Duration::from_micros(10),
        routes.connect(&address, "localhost"),
    )
    .await;
    let next = routes.connect(&address, "localhost").await.unwrap();
    assert_eq!(routes.dials(), 3, "gave up: {}", gave_up.is_ok());
    assert!(next.generation > fresh.generation);
    server.close();
    task.await.unwrap().unwrap();
}

/// The audit's F64: a peer pause is spread over its second half — never
/// shorter than half the configured pause, never longer than the whole.
#[test]
fn a_peer_pause_is_spread_over_its_second_half() {
    let pause = Duration::from_millis(600);
    assert_eq!(crate::peers::spread(pause, 0), Duration::from_millis(300));
    let whole = crate::peers::spread(pause, u64::MAX);
    assert!(
        whole <= pause && whole >= pause - Duration::from_nanos(1),
        "{whole:?}"
    );
    let middle = crate::peers::spread(pause, u64::MAX / 2);
    assert!(
        middle >= Duration::from_millis(450) - Duration::from_nanos(1)
            && middle <= Duration::from_millis(450),
        "{middle:?}"
    );
    assert_eq!(
        crate::peers::spread(Duration::ZERO, u64::MAX),
        Duration::ZERO
    );
}

/// A certificate of `key` valid from the given day: what the registry
/// compares is the key and the start of validity (the listener verifies
/// the chain).
fn certificate_valid_from(key: &KeyPair, day: u8) -> Vec<u8> {
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params.not_before = rcgen::date_time_ymd(2026, 1, day);
    params.self_signed(key).unwrap().der().to_vec()
}

#[test]
fn a_renewal_of_an_enrolled_key_is_admitted_until_the_projection_names_or_drops_it() {
    let key = KeyPair::generate().unwrap();
    let known = certificate_valid_from(&key, 10);
    let renewal = certificate_valid_from(&key, 20);
    let older = certificate_valid_from(&key, 5);
    let foreign = certificate_valid_from(&KeyPair::generate().unwrap(), 20);
    let (enrolled, not_before) = certificate_key(&known).unwrap();
    assert_eq!(certificate_key(&renewal).unwrap().0, enrolled);
    assert!(certificate_key(&renewal).unwrap().1 > not_before);
    assert_ne!(certificate_key(&foreign).unwrap().0, enrolled);
    assert!(matches!(
        certificate_key(b"not a certificate"),
        Err(AccessError::Unauthorized)
    ));
    let grant = PeerGrant {
        principal: ParticipantId::from_u128(7),
        tenants: BTreeSet::from([TenantId::from_u128(1)]),
        role: PeerRole::Node { node_id: 3 },
    };
    let keys = |not_before: i64| {
        std::collections::BTreeMap::from([(
            enrolled,
            EnrolledKey {
                grant: grant.clone(),
                not_before,
            },
        )])
    };
    let named = |certificate: &[u8]| {
        std::collections::BTreeMap::from([(certificate_fingerprint(certificate), grant.clone())])
    };
    let registry = PeerRegistry::new(8).unwrap();
    registry
        .replace_projection(named(&known), keys(not_before))
        .unwrap();
    // The certificate the projection names is granted as it always was.
    let peer = registry.authenticate_certificate(&known).unwrap();
    assert!(peer.renewal_of().is_none());
    // A later certificate of the same key is a renewal this node has not
    // applied yet: admitted under the key's grant, and known as such at
    // dispatch.
    let peer = registry.authenticate_certificate(&renewal).unwrap();
    assert_eq!(peer.renewal_of(), Some(enrolled));
    assert_eq!(
        peer.certificate_fingerprint(),
        Some(certificate_fingerprint(&renewal))
    );
    assert_eq!(peer.role(), PeerRole::Node { node_id: 3 });
    assert_eq!(
        registry
            .authenticate(certificate_fingerprint(&renewal))
            .unwrap()
            .renewal_of(),
        Some(enrolled)
    );
    registry.granted(certificate_fingerprint(&renewal)).unwrap();
    // A certificate of the key from before the one named, and one of a key
    // no enrollment holds, are refused.
    for refused in [&older, &foreign] {
        assert!(matches!(
            registry.authenticate_certificate(refused),
            Err(AccessError::Unauthorized)
        ));
    }
    // The projection catches up and names the renewal: it is an ordinary
    // grant, and the certificate it replaced — no longer named, and not
    // later than the one that is — is refused.
    let (_, renewed_from) = certificate_key(&renewal).unwrap();
    registry
        .replace_projection(named(&renewal), keys(renewed_from))
        .unwrap();
    assert!(
        registry
            .authenticate_certificate(&renewal)
            .unwrap()
            .renewal_of()
            .is_none()
    );
    assert!(matches!(
        registry.authenticate_certificate(&known),
        Err(AccessError::Unauthorized)
    ));
    // A renewal admitted while the projection still enrolls its key keeps
    // the grant across a replacement; one whose key the projection drops
    // (the enrollment revoked) loses it.
    let next = certificate_valid_from(&key, 25);
    registry.authenticate_certificate(&next).unwrap();
    registry
        .replace_projection(named(&renewal), keys(renewed_from))
        .unwrap();
    assert_eq!(
        registry
            .authenticate(certificate_fingerprint(&next))
            .unwrap()
            .renewal_of(),
        Some(enrolled)
    );
    registry
        .replace_projection(named(&renewal), std::collections::BTreeMap::new())
        .unwrap();
    assert!(matches!(
        registry.authenticate(certificate_fingerprint(&next)),
        Err(AccessError::Unauthorized)
    ));
    assert!(matches!(
        registry.authenticate_certificate(&next),
        Err(AccessError::Unauthorized)
    ));
    // A projection without keys admits only what it names.
    registry.replace_grants(named(&known)).unwrap();
    assert!(registry.authenticate_certificate(&known).is_ok());
    assert!(matches!(
        registry.authenticate_certificate(&renewal),
        Err(AccessError::Unauthorized)
    ));
}

/// What became of an exchange whose stream its peer never read, while
/// other exchanges went on over the same connection.
struct Unread {
    stalled: Result<ResponseEnvelope, WireError>,
    after: Duration,
    /// The longest round trip the connection measured meanwhile.
    longest: Duration,
    /// The other exchanges answered while it waited.
    answered: u64,
}
const UNREAD_PERIOD: Duration = Duration::from_millis(500);
const UNREAD_OTHER: usize = 4 * 1024;
const UNREAD_EVERY: Duration = Duration::from_millis(100);
/// Ask a peer that answers what is small and never reads what is not, over
/// `shape` or directly, for `stalled`, while four kilobytes are sent to it
/// ten times a second; then once more for something small.
async fn unread(shape: Option<Shape>, stalled: RequestEnvelope) -> Unread {
    use std::sync::atomic::{AtomicU64, Ordering};
    let wire = WireLimits {
        request_timeout: UNREAD_PERIOD,
        max_frame_bytes: 4 * 1024 * 1024,
        max_cost: 16 * 1024 * 1024,
        ..Default::default()
    };
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let (server_certificate, server_key) = pki.issue(true);
    let config = server_tls(
        TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut address = endpoint.local_addr().unwrap();
    let limits = wire.clone();
    let peer = tokio::spawn(async move {
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let hello: Hello = read_frame(&mut recv, FrameKind::Hello, 4096).await.unwrap();
        write_frame(
            &mut send,
            FrameKind::HelloReply,
            &HelloReply::Accepted(limits.negotiate(&hello).unwrap()),
            4096,
        )
        .await
        .unwrap();
        send.finish().unwrap();
        let mut unread = Vec::new();
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let header = read_frame_header(&mut recv, FrameKind::Request, limits.max_frame_bytes)
                .await
                .unwrap();
            if header.payload_bytes() > 1024 * 1024 {
                unread.push((send, recv));
                continue;
            }
            tokio::spawn(async move {
                let alone = crate::frame::AloneDelivery::default();
                let asked: RequestEnvelope = read_payload_arriving(
                    &mut recv,
                    header,
                    UNREAD_PERIOD,
                    || Duration::from_millis(1),
                    &alone,
                    0,
                )
                .await
                .unwrap();
                write_frame(
                    &mut send,
                    FrameKind::Response,
                    &asked.reply(Response::PeerAccepted),
                    4096,
                )
                .await
                .unwrap();
                send.finish().unwrap();
                let _ = send.stopped().await;
            });
        }
    });
    let mut relays = Vec::new();
    if let Some(shape) = shape {
        (address, relays) = shaped(address, shape).await;
    }
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let connector = QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire).unwrap();
    let remote = connector.connect(address, "localhost").await.unwrap();
    fn other(id: u128) -> RequestEnvelope {
        let mut packet = request(id);
        packet.operation = Operation::Raft {
            group: [2; 16],
            message: vec![7; UNREAD_OTHER],
        };
        packet
    }
    let answered = Arc::new(AtomicU64::new(0));
    let others = {
        let (remote, answered) = (remote.clone(), answered.clone());
        tokio::spawn(async move {
            for id in 1_000.. {
                remote
                    .request_within(&other(id), UNREAD_PERIOD)
                    .await
                    .unwrap();
                answered.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(UNREAD_EVERY).await;
            }
        })
    };
    let began = std::time::Instant::now();
    let stalled = remote.request_within(&stalled, UNREAD_PERIOD).await;
    let after = began.elapsed();
    // The round trip the exchange was judged by: the longest any carriage
    // on the connection saw while it waited (a sampler of the test's own
    // misses what the carriage's own waits see — a Windows runner showed
    // 5.85 s against a bound of 5.77 s from a sampled 2.0 ms, 2026-10-02).
    let longest = remote.longest_round_trip();
    let during = answered.load(Ordering::Relaxed);
    // The others are answered after it as before, on the same connection.
    remote
        .request_within(&other(2), UNREAD_PERIOD)
        .await
        .unwrap();
    assert!(!remote.closed());
    assert!(!others.is_finished());
    others.abort();
    assert!(others.await.unwrap_err().is_cancelled());
    remote.close();
    let _ = peer.await;
    for relay in relays {
        relay.abort();
    }
    Unread {
        stalled,
        after,
        longest,
        answered: during,
    }
}

/// A stream its peer does not read ends by what its own bytes are given,
/// whatever else the connection carries meanwhile (the audit's F38).
/// Charged with what the connection sent, it was kept for as long as the
/// other exchanges moved a datagram a period, until they had sent as much
/// as it had to: two megabytes at twenty kilobytes a period here, fifty
/// periods on the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_its_peer_does_not_read_ends_whatever_else_its_connection_carries() {
    const STALLED: usize = 2 * 1024 * 1024;
    let mut packet = request(1);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![7; STALLED],
    };
    let unread = unread(None, packet).await;
    assert!(
        matches!(unread.stalled, Err(WireError::Timeout)),
        "{:?}",
        unread.stalled
    );
    assert!(unread.after >= UNREAD_PERIOD, "{:?}", unread.after);
    // It ends by its own bytes' residency at the longest round trip the
    // path showed, what was held to send beside it being no more than the
    // others' one message at a time, a period more for the wait that finds
    // the time spent — a bound in the path's terms, as the lossy variant
    // states it (a Windows runner's loopback showed a round trip that
    // took the stalled exchange 8.6 s where a fixed eighth of the old wait
    // allowed 6.4 s, 2026-10-02).
    let given = crate::frame::residency(STALLED + 2 * UNREAD_OTHER, unread.longest)
        .max(UNREAD_PERIOD)
        + UNREAD_PERIOD;
    assert!(
        unread.after <= given,
        "{:?} of {given:?} at {:?}",
        unread.after,
        unread.longest
    );
    // And less than the others would have kept it under the old wait: what
    // they send in a period, and the periods it would have taken them to
    // send what the stalled exchange had to. On a loopback the law's bound
    // is the acknowledgement delay's — a probe timeout is three round
    // trips and the 25 ms a peer may hold an acknowledgement (RFC 9002
    // §6.2.1), two datagrams of the least size each — 27 s here for two
    // megabytes on a Windows runner's 1.75 ms loopback, where the old wait
    // was 51 s and the claim of under half of it held only while the
    // probe timeout counted the round trips alone (Windows CI, 2026-10-03).
    let moved = UNREAD_OTHER as u32 * (UNREAD_PERIOD.as_millis() / UNREAD_EVERY.as_millis()) as u32;
    let kept = UNREAD_PERIOD * (STALLED as u32 / moved);
    assert!(kept >= UNREAD_PERIOD * 100);
    assert!(
        given < kept,
        "{given:?} of {kept:?} at {:?}",
        unread.longest
    );
    println!(
        "a stalled stream of {STALLED} bytes ended after {:?}, given {given:?} at a longest round trip of {:?}; the old wait kept it {kept:?}",
        unread.after, unread.longest
    );
    assert!(unread.answered >= 1, "{}", unread.answered);
}

/// The same over a path that loses a datagram in a hundred and delivers
/// out of order, the stalled exchange content and the others a group's
/// messages, which go before it: it ends when what the path is given to
/// carry its bytes is spent — the least a live path delivers, at the longest
/// round trip this one showed — and the others are answered throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_its_peer_does_not_read_ends_on_a_path_that_loses_and_reorders() {
    // More than the megabyte a stream is let send ahead of its reader.
    const STALLED: usize = 1024 * 1024 + 256 * 1024;
    let mut packet = request(1);
    packet.operation = Operation::Custody(CustodyRequest::Chunk {
        transfer: [1; 16],
        index: 3,
        bytes: vec![7; STALLED],
    });
    let shape = Shape {
        delay: Duration::from_millis(1),
        jitter: Duration::from_millis(1),
        loss_ppm: 10_000,
        ..Shape::even(100_000_000)
    };
    let unread = unread(Some(shape), packet).await;
    assert!(
        matches!(unread.stalled, Err(WireError::Timeout)),
        "{:?}",
        unread.stalled
    );
    assert!(unread.after >= UNREAD_PERIOD, "{:?}", unread.after);
    // What was held to send beside it is no more than the others' one
    // message at a time; a period more for the wait that finds the time
    // spent.
    let given = crate::frame::residency(STALLED + 2 * UNREAD_OTHER, unread.longest)
        .max(UNREAD_PERIOD)
        + UNREAD_PERIOD;
    assert!(
        unread.after <= given,
        "{:?} of {given:?} at {:?}",
        unread.after,
        unread.longest
    );
    assert!(unread.answered >= 1, "{}", unread.answered);
}

/// The time a peer is given to answer begins when it has what was asked:
/// when the request's stream is acknowledged whole (the audit's F38).
/// Thirty-two kilobytes over a path that carries eight in a second take
/// four seconds, and the peer answers four tenths of a second after it has
/// them, inside its half-second. Counted from when the connection had
/// *sent* as much as the request, the half-second began while the last
/// window of it, more than a second of this path, was still on its way,
/// and ended before the peer had the request at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peers_time_to_answer_begins_when_it_has_what_was_asked() {
    const BITS: u64 = 64_000;
    const SIZE: usize = 32 * 1024;
    const PERIOD: Duration = Duration::from_millis(500);
    let wire = WireLimits {
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
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| async move {
        tokio::time::sleep(PERIOD * 4 / 5).await;
        verified.request().reply(Response::PeerAccepted)
    });
    let (server_certificate, server_key) = pki.issue(true);
    let tls = server_tls(
        TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let server = Arc::new(
        QuicServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            tls,
            registry,
            wire.clone(),
            budget(),
        )
        .unwrap(),
    );
    let running = server.clone();
    let serving = tokio::spawn(async move { running.serve(handler).await });
    let (path, relays) = shaped(
        server.local_addr().unwrap(),
        Shape {
            delay: Duration::from_millis(10),
            ..Shape::even(BITS)
        },
    )
    .await;
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let connector = QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire).unwrap();
    let remote = connector.connect(path, "localhost").await.unwrap();
    let mut packet = request(1);
    packet.operation = Operation::Raft {
        group: [2; 16],
        message: vec![7; SIZE],
    };
    let began = std::time::Instant::now();
    let answered = remote.request_within(&packet, PERIOD).await;
    let took = began.elapsed();
    assert!(
        matches!(&answered, Ok(answer) if answer.result == Response::PeerAccepted),
        "{answered:?} in {took:?}"
    );
    // No sooner than the path carries it, which is many of its periods.
    let least = Duration::from_millis(SIZE as u64 * 8 * 1_000 / BITS);
    assert!(least >= PERIOD * 8);
    assert!(took >= least, "{took:?}");
    server.close();
    for relay in relays {
        relay.abort();
    }
    let _ = serving.await;
}

/// A pool, as a node's limits are but for `pool`, and a peer that accepts
/// what it is sent, with a path shaped as `shape` between them.
struct SlowRig {
    pool: PeerConnectionPool,
    server: Arc<QuicServer>,
    serving: tokio::task::JoinHandle<Result<(), WireError>>,
    relays: Vec<tokio::task::JoinHandle<()>>,
}
impl SlowRig {
    async fn new(shape: Shape, pool: PeerPoolLimits) -> Self {
        use std::collections::BTreeMap;
        let wire = WireLimits {
            max_frame_bytes: 10 * 1024 * 1024,
            max_cost: 40 * 1024 * 1024,
            ..WireLimits::for_consensus(128)
        };
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node_grant = grant();
        node_grant.role = PeerRole::Node { node_id: 7 };
        registry
            .register_certificate(&certificate, node_grant)
            .unwrap();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(move |verified: VerifiedRequest| async move {
                verified.request().reply(Response::PeerAccepted)
            });
        let (server_certificate, server_key) = pki.issue(true);
        let tls = server_tls(
            TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
            vec![pki.ca.der().to_vec()],
            &wire,
        )
        .unwrap();
        let server = Arc::new(
            QuicServer::bind(
                "127.0.0.1:0".parse().unwrap(),
                tls,
                registry,
                wire.clone(),
                budget(),
            )
            .unwrap(),
        );
        let running = server.clone();
        let serving = tokio::spawn(async move { running.serve(handler).await });
        let (path, relays) = shaped(server.local_addr().unwrap(), shape).await;
        let tls = client_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &wire,
        )
        .unwrap();
        let pool = PeerConnectionPool::new(
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire).unwrap(),
            pool,
        )
        .unwrap();
        pool.replace_routes(
            1,
            BTreeMap::from([(
                2,
                PeerEndpoint {
                    address: path,
                    server_name: "localhost".into(),
                    name: None,
                },
            )]),
        )
        .unwrap();
        Self {
            pool,
            server,
            serving,
            relays,
        }
    }
    fn message(id: u128, size: usize) -> RequestEnvelope {
        let mut packet = request(id);
        packet.operation = Operation::Raft {
            group: [2; 16],
            message: vec![7; size],
        };
        packet
    }
    async fn close(self) {
        self.pool.close();
        self.server.close();
        for relay in self.relays {
            relay.abort();
        }
        let _ = self.serving.await;
    }
}

/// A group's message is carried for as long as its path takes (the audit's
/// F36): sixty-four kilobytes over a path that carries thirty-two in a
/// second, by a pool whose every wait is one second. An exchange was given
/// that one time whatever it carried, so a path that carried less than the
/// message in it carried none of the message, at the first attempt or at
/// any later one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_groups_message_is_carried_for_as_long_as_its_path_takes() {
    const BITS: u64 = 256_000;
    const SIZE: usize = 64 * 1024;
    let limits = PeerPoolLimits {
        timeout: Duration::from_secs(1),
        ..PeerPoolLimits::for_consensus(128)
    };
    let least = Duration::from_millis(SIZE as u64 * 8 * 1_000 / BITS);
    assert!(least >= limits.timeout * 2);
    let rig = SlowRig::new(
        Shape {
            delay: Duration::from_millis(10),
            ..Shape::even(BITS)
        },
        limits.clone(),
    )
    .await;
    let began = std::time::Instant::now();
    assert_eq!(rig.pool.send(2, &SlowRig::message(1, SIZE)).await, Ok(()));
    let took = began.elapsed();
    assert!(took >= least, "{took:?}");
    let stats = rig.pool.stats();
    assert_eq!((stats.dials, stats.connections_opened), (1, 1));
    rig.close().await;
}

/// A dial is given what a handshake is given and not what one exchange is
/// (the audit's F36): over a path that takes longer to connect than the
/// pool's callers wait, the callers are told the peer was not reached, the
/// dial goes on, and the next caller has its connection. The dial was
/// given the callers' time, failed with them, and was begun again from
/// nothing by the next: such a path was never connected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dial_is_given_a_handshakes_time_and_outlives_the_callers_that_asked_for_it() {
    let limits = PeerPoolLimits {
        timeout: Duration::from_millis(500),
        retry_backoff: Duration::from_millis(10),
        ..PeerPoolLimits::for_consensus(128)
    };
    // Sixteen kilobits a second: the two handshakes are some seven
    // kilobytes, three seconds and more.
    let rig = SlowRig::new(
        Shape {
            delay: Duration::from_millis(10),
            ..Shape::even(16_000)
        },
        limits.clone(),
    )
    .await;
    let began = std::time::Instant::now();
    let mut lost = 0_u32;
    // Charged to the dial: one more try for every caller's time it takes,
    // and as many as a handshake is given at most.
    let tries =
        (WireLimits::default().request_timeout.as_millis() * 2 / limits.timeout.as_millis()) as u32;
    loop {
        match rig.pool.send(2, &SlowRig::message(1, 64)).await {
            Ok(()) => break,
            Err(PeerSendError::Lost) => lost += 1,
            Err(other) => panic!("{other:?}"),
        }
        assert!(lost < tries, "{lost} tries in {:?}", began.elapsed());
    }
    assert!(lost >= 1, "connected within a caller's time");
    assert!(began.elapsed() >= limits.timeout * 2);
    let stats = rig.pool.stats();
    assert_eq!((stats.dials, stats.connections_opened), (1, 1));
    rig.close().await;
}

/// What the pool does with a group's message of `size` over a path shaped
/// as `shape`, as a node's limits are: the outcome of sending it, and of
/// sending it again, each given `cap` at most, and what the pool counted.
type SlowCase = (
    Result<Result<(), PeerSendError>, tokio::time::error::Elapsed>,
    Duration,
    Result<Result<(), PeerSendError>, tokio::time::error::Elapsed>,
    Duration,
    PeerPoolStats,
);
async fn slow_case(shape: Shape, size: usize, cap: Duration) -> SlowCase {
    let rig = SlowRig::new(shape, PeerPoolLimits::for_consensus(128)).await;
    let packet = SlowRig::message(u128::from(shape.up_bits) * 1_000_000 + size as u128, size);
    let began = std::time::Instant::now();
    let first = tokio::time::timeout(cap, rig.pool.send(2, &packet)).await;
    let took = began.elapsed();
    // A second message, on the connection the first opened, if it did.
    let again = std::time::Instant::now();
    let second = tokio::time::timeout(cap, rig.pool.send(2, &packet)).await;
    let then = again.elapsed();
    let stats = rig.pool.stats();
    rig.close().await;
    (first, took, second, then, stats)
}
fn slow_row(label: &str, case: &SlowCase) {
    let say =
        |outcome: &Result<Result<(), PeerSendError>, tokio::time::error::Elapsed>| match outcome {
            Ok(Ok(())) => "delivered".to_string(),
            Ok(Err(error)) => format!("{error:?}"),
            Err(_) => "capped".to_string(),
        };
    let (first, took, second, then, stats) = case;
    println!(
        "{label:<44} {:<12} {:>6.1}s  {:<12} {:>6.1}s  {:>5} {:>6}",
        say(first),
        took.as_secs_f64(),
        say(second),
        then.as_secs_f64(),
        stats.dials,
        stats.connections_opened
    );
}
fn slow_cap() -> Duration {
    Duration::from_secs(
        std::env::var("FOCAL_SLOW_PATH_CAP")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(120),
    )
}
const SLOW_HEAD: &str = "path, bytes                                  first            in  second           in  dials opened";

/// What the pool does with a group's message of each size over paths of
/// each rate: a measurement, printed, which asserts nothing.
/// `FOCAL_SLOW_PATH_CAP` is the seconds a send is given (default 120).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement of minutes; run by name"]
async fn slow_paths_measured() {
    let cap = slow_cap();
    let mut cases = Vec::new();
    for bits in [100_u64, 1_000, 8_000, 64_000, 256_000] {
        for size in [64_usize, 4 * 1024, 64 * 1024, 1024 * 1024] {
            let shape = Shape {
                delay: Duration::from_millis(25),
                ..Shape::even(bits)
            };
            cases.push((
                format!("{bits} bit/s, {size}"),
                tokio::spawn(slow_case(shape, size, cap)),
            ));
        }
    }
    println!("{SLOW_HEAD}");
    for (label, case) in cases {
        slow_row(&label, &case.await.unwrap());
    }
}

/// The same over paths that lose, delay unevenly, carry less one way, and
/// stop for a while: a measurement, printed, which asserts nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement of minutes; run by name"]
async fn adverse_paths_measured() {
    let cap = slow_cap();
    let base = |bits: u64| Shape {
        delay: Duration::from_millis(25),
        ..Shape::even(bits)
    };
    let seconds = Duration::from_secs;
    let mut shapes: Vec<(String, Shape)> = Vec::new();
    for bits in [64_000_u64, 256_000] {
        shapes.push((
            format!("{bits} bit/s, loss 2%"),
            Shape {
                loss_ppm: 20_000,
                ..base(bits)
            },
        ));
        shapes.push((
            format!("{bits} bit/s, loss 10%"),
            Shape {
                loss_ppm: 100_000,
                ..base(bits)
            },
        ));
        shapes.push((
            format!("{bits} bit/s, jitter 50 ms"),
            Shape {
                jitter: Duration::from_millis(50),
                ..base(bits)
            },
        ));
        shapes.push((
            format!("{bits} bit/s up, 8000 down"),
            Shape {
                down_bits: 8_000,
                ..base(bits)
            },
        ));
        shapes.push((
            format!("8000 bit/s up, {bits} down"),
            Shape {
                up_bits: 8_000,
                ..base(bits)
            },
        ));
        shapes.push((
            format!("{bits} bit/s, out 3 s at 4 s"),
            Shape {
                outage: Some((seconds(4), seconds(3))),
                ..base(bits)
            },
        ));
        shapes.push((
            format!("{bits} bit/s, out 8 s at 4 s"),
            Shape {
                outage: Some((seconds(4), seconds(8))),
                ..base(bits)
            },
        ));
        shapes.push((
            format!("{bits} bit/s, out 15 s at 4 s"),
            Shape {
                outage: Some((seconds(4), seconds(15))),
                ..base(bits)
            },
        ));
    }
    let mut cases = Vec::new();
    for (label, shape) in shapes {
        for size in [4 * 1024_usize, 64 * 1024, 256 * 1024] {
            cases.push((
                format!("{label}, {size}"),
                tokio::spawn(slow_case(shape, size, cap)),
            ));
        }
    }
    println!("{SLOW_HEAD}");
    for (label, case) in cases {
        slow_row(&label, &case.await.unwrap());
    }
}

/// One exchange with a peer over a path shaped as `shape`, each part of it
/// given `period`: `size` bytes sent to the peer, or asked of it (`down`).
async fn direct_case(
    shape: Shape,
    size: usize,
    down: bool,
    period: Duration,
) -> (Result<(), WireError>, Duration) {
    let wire = WireLimits {
        max_frame_bytes: 10 * 1024 * 1024,
        max_cost: 40 * 1024 * 1024,
        ..WireLimits::for_consensus(128)
    };
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(16).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| async move {
        let reply = match &verified.request().operation {
            Operation::Custody(CustodyRequest::ReadChunk {
                index, max_bytes, ..
            }) => Response::Custody(CustodyReply::Chunk {
                index: *index,
                bytes: vec![7; *max_bytes as usize],
            }),
            _ => Response::PeerAccepted,
        };
        verified.request().reply(reply)
    });
    let (server_certificate, server_key) = pki.issue(true);
    let tls = server_tls(
        TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let server = Arc::new(
        QuicServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            tls,
            registry,
            wire.clone(),
            budget(),
        )
        .unwrap(),
    );
    let running = server.clone();
    let serving = tokio::spawn(async move { running.serve(handler).await });
    let (path, relays) = shaped(server.local_addr().unwrap(), shape).await;
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let connector = QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire).unwrap();
    let outcome = match connector.connect(path, "localhost").await {
        Err(error) => (Err(error), Duration::ZERO),
        Ok(remote) => {
            let mut packet = request(9);
            packet.operation = if down {
                Operation::Custody(CustodyRequest::ReadChunk {
                    transfer: [1; 16],
                    index: 4,
                    max_bytes: size as u32,
                })
            } else {
                Operation::Raft {
                    group: [2; 16],
                    message: vec![7; size],
                }
            };
            let sent = std::time::Instant::now();
            let outcome = remote.request_within(&packet, period).await;
            (outcome.map(|_| ()), sent.elapsed())
        }
    };
    server.close();
    for relay in relays {
        relay.abort();
    }
    let _ = serving.await;
    outcome
}

/// Exchanges each way over the narrowest paths that connect, with and
/// without loss, each part given five seconds: a measurement, printed,
/// which asserts nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement of minutes; run by name"]
async fn narrow_lossy_paths_measured() {
    let mut cases = Vec::new();
    for bits in [4_000_u64, 8_000, 16_000] {
        for loss_ppm in [0_u32, 20_000, 100_000] {
            for size in [16 * 1024_usize, 64 * 1024] {
                for down in [false, true] {
                    let shape = Shape {
                        delay: Duration::from_millis(25),
                        loss_ppm,
                        ..Shape::even(bits)
                    };
                    cases.push((
                        format!(
                            "{bits} bit/s, loss {}%, {size} {}",
                            loss_ppm / 10_000,
                            if down { "asked" } else { "sent" }
                        ),
                        (size as u64 * 8).div_ceil(bits),
                        tokio::spawn(direct_case(shape, size, down, Duration::from_secs(5))),
                    ));
                }
            }
        }
    }
    println!(
        "path, bytes                              outcome              in   at the path's rate"
    );
    for (label, least, case) in cases {
        let (outcome, took) = case.await.unwrap();
        println!(
            "{label:<40} {:<14} {:>7.1}s   {least:>5}s",
            match outcome {
                Ok(()) => "answered".to_string(),
                Err(error) => format!("{error:?}"),
            },
            took.as_secs_f64()
        );
    }
}

/// One exchange over one shaped path, told as it goes: a diagnostic.
/// `FOCAL_SLOW_PATH_BITS`, `_BYTES`, `_PERIOD_MS`, `_LOSS_PPM`, and `_DOWN`
/// for bytes asked of the peer instead of sent to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a diagnostic; run by name"]
async fn slow_path_one() {
    let read = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let bits = read("FOCAL_SLOW_PATH_BITS", 8_000);
    let size = read("FOCAL_SLOW_PATH_BYTES", 65_536) as usize;
    let period = Duration::from_millis(read("FOCAL_SLOW_PATH_PERIOD_MS", 5_000));
    let shape = Shape {
        delay: Duration::from_millis(25),
        loss_ppm: read("FOCAL_SLOW_PATH_LOSS_PPM", 0) as u32,
        ..Shape::even(bits)
    };
    let down = read("FOCAL_SLOW_PATH_DOWN", 0) == 1;
    let began = std::time::Instant::now();
    let (outcome, took) = direct_case(shape, size, down, period).await;
    println!(
        "{bits} bit/s, {size} bytes {}, period {period:?}: {outcome:?} in {took:?} ({:?} in all)",
        if down { "asked" } else { "sent" },
        began.elapsed()
    );
}

/// How a paced peer answers one request: the reply's bytes, written whole
/// in pieces behind the replies before it, or its header alone.
#[derive(Clone, Copy)]
enum PacedReply {
    Whole(u32),
    Withheld(u32),
}
const PACED_PIECE: usize = 64 * 1024;
const PACED_EVERY: Duration = Duration::from_millis(50);
/// A peer that reads every request and answers each download as `policy`
/// says: every reply's header leaves as soon as its request is read, and
/// the bodies leave one after another in the order the requests came, a
/// piece every `PACED_EVERY` — a peer whose owner writes as it has, behind
/// the replies before it. A withheld reply's header leaves and its body
/// never does; the stream stays open.
async fn paced_peer(
    wire: WireLimits,
    policy: impl Fn(u128) -> PacedReply + Send + Sync + 'static,
) -> (QuicRemote, tokio::task::JoinHandle<()>) {
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let (server_certificate, server_key) = pki.issue(true);
    let config = server_tls(
        TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let address = endpoint.local_addr().unwrap();
    let limits = wire.clone();
    let peer = tokio::spawn(async move {
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let hello: Hello = read_frame(&mut recv, FrameKind::Hello, 4096).await.unwrap();
        write_frame(
            &mut send,
            FrameKind::HelloReply,
            &HelloReply::Accepted(limits.negotiate(&hello).unwrap()),
            4096,
        )
        .await
        .unwrap();
        send.finish().unwrap();
        // Bodies in the order their requests came, one at a time.
        let (queue, mut bodies) =
            tokio::sync::mpsc::unbounded_channel::<(quinn::SendStream, Vec<u8>)>();
        let writer = tokio::spawn(async move {
            while let Some((mut send, body)) = bodies.recv().await {
                for piece in body.chunks(PACED_PIECE) {
                    if send.write_all(piece).await.is_err() {
                        break;
                    }
                    tokio::time::sleep(PACED_EVERY).await;
                }
                let _ = send.finish();
                let _ = send.stopped().await;
            }
        });
        let mut withheld = Vec::new();
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let header = read_frame_header(&mut recv, FrameKind::Request, limits.max_frame_bytes)
                .await
                .unwrap();
            let alone = crate::frame::AloneDelivery::default();
            let asked: RequestEnvelope = read_payload_arriving(
                &mut recv,
                header,
                limits.request_timeout,
                || Duration::from_millis(1),
                &alone,
                0,
            )
            .await
            .unwrap();
            let id = u128::from_be_bytes(asked.request_id.0);
            let (bytes, whole) = match policy(id) {
                PacedReply::Whole(bytes) => (bytes, true),
                PacedReply::Withheld(bytes) => (bytes, false),
            };
            let reply = asked.reply(Response::Content(ContentChunk {
                offset: 0,
                eof: true,
                bytes: vec![7; bytes as usize],
            }));
            let body = encode_payload(&reply, limits.max_frame_bytes).unwrap();
            let mut frame = [0u8; HEADER_BYTES];
            frame[..8].copy_from_slice(b"FOCALQ01");
            frame[8..10].copy_from_slice(&1u16.to_be_bytes());
            frame[10..12].copy_from_slice(&(FrameKind::Response as u16).to_be_bytes());
            frame[12..16].copy_from_slice(&(body.len() as u32).to_be_bytes());
            send.write_all(&frame).await.unwrap();
            if whole {
                queue.send((send, body)).unwrap();
            } else {
                withheld.push(send);
            }
        }
        drop(queue);
        let _ = writer.await;
        drop(withheld);
    });
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let connector = QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire).unwrap();
    let remote = connector.connect(address, "localhost").await.unwrap();
    (remote, peer)
}
fn paced_download(id: u128, bytes: u32) -> RequestEnvelope {
    let mut request = download_request(bytes);
    request.request_id = RequestId::from_u128(id);
    request
}

/// A reply queued behind the peer's other replies is not refused while the
/// connection carries them: sixteen downloads of 128 KiB, their headers at
/// once and their bodies one after another at 64 KiB every 50 ms, so the
/// last body begins a second and a half after its header, five times the
/// 300 ms the exchange is given. Before, a body was given its residency
/// from its header — on loopback, the period — and the first judgement
/// after it refused a body not yet begun, however much the connection
/// carried (hyper-raft's port: a 64 KiB reply refused with none of it read
/// while its period brought 464 KB, 3 of 24 loaded macOS runs).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_queued_behind_the_peers_others_is_not_refused_while_they_arrive() {
    const REPLIES: u128 = 16;
    const BYTES: u32 = 128 * 1024;
    let wire = WireLimits {
        request_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    let (remote, peer) = paced_peer(wire.clone(), |_| PacedReply::Whole(BYTES)).await;
    let began = std::time::Instant::now();
    let asked: Vec<_> = (0..REPLIES)
        .map(|id| {
            let remote = remote.clone();
            async move {
                remote
                    .request_within(&paced_download(id, BYTES), wire.request_timeout)
                    .await
            }
        })
        .collect();
    let answers = futures_util::future::join_all(asked).await;
    let took = began.elapsed();
    for (id, answer) in answers.iter().enumerate() {
        match answer {
            Ok(ResponseEnvelope {
                result: Response::Content(chunk),
                ..
            }) => assert_eq!(chunk.bytes.len(), BYTES as usize, "reply {id}"),
            other => panic!("reply {id}: {other:?} after {took:?}"),
        }
    }
    // The bodies took their turns: the last began at least fifteen
    // pieces' pacing after the first.
    assert!(took >= PACED_EVERY * 30, "{took:?}");
    drop(remote);
    peer.abort();
}

/// A peer that answers with a header, declares a body and never sends it,
/// while it keeps the connection busy with the replies to the requests that
/// follow, is still given up, and the others arrive. `Arriving::judge`
/// gives a body up by either of two rules, and the connection says which
/// (`QuicRemote::given_up`): a judgement that brought less than
/// `LEAST_PROGRESS` of the connection (a pause of the peer's own on a
/// machine that starves it — a CI runner that ran the pacing at six times
/// its interval gave the body up half a second before the last of the
/// others, and was right to), or the connection delivering everything the
/// peer owed of the body's class with the body not among it — which the
/// headers' order decides: where the others' headers came after the first
/// judgements, the most the peer owed at any judgement is less than the
/// others' bytes, and their delivery ends the withheld body a judgement
/// after (a macOS runner, 2026-10-03; the rule was taken for unreachable
/// here). A quiet give-up is held to what the connection's received bytes,
/// sampled as the rule reads them, cannot rule out — a judgement before
/// it, the samples' own gaps included — never to the clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_body_the_peer_withholds_while_it_sends_others_is_given_up() {
    const OTHERS: u128 = 12;
    const OTHER_BYTES: u32 = 256 * 1024;
    const WITHHELD_BYTES: u32 = 64 * 1024;
    let wire = WireLimits {
        request_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    let (remote, peer) = paced_peer(wire.clone(), |id| {
        if id == 0 {
            PacedReply::Withheld(WITHHELD_BYTES)
        } else {
            PacedReply::Whole(OTHER_BYTES)
        }
    })
    .await;
    let began = std::time::Instant::now();
    let asked: Vec<_> = (0..=OTHERS)
        .map(|id| {
            let remote = remote.clone();
            async move {
                let bytes = if id == 0 { WITHHELD_BYTES } else { OTHER_BYTES };
                let answer = remote
                    .request_within(&paced_download(id, bytes), wire.request_timeout)
                    .await;
                (answer, began.elapsed())
            }
        })
        .collect();
    // What the connection received, sampled at a sixteenth of a judgement
    // while the exchanges run: how a give-up is told from the connection's
    // quiet (`quiet_before`).
    let sample_every = wire.request_timeout / 16;
    let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
    let sampler = tokio::spawn({
        let remote = remote.clone();
        async move {
            let mut samples = Vec::new();
            loop {
                samples.push((began.elapsed(), remote.received()));
                if samples.len() >= RECEIVED_SAMPLES {
                    break;
                }
                tokio::select! {
                    _ = &mut stopped => break,
                    () = tokio::time::sleep(sample_every) => {}
                }
            }
            samples
        }
    });
    let answers = futures_util::future::join_all(asked).await;
    let _ = stop.send(());
    let samples = sampler.await.unwrap();
    // The others arrive, one after another.
    let mut last_other = Duration::ZERO;
    for (id, (answer, at)) in answers.iter().enumerate().skip(1) {
        assert!(
            matches!(
                answer,
                Ok(ResponseEnvelope {
                    result: Response::Content(_),
                    ..
                })
            ),
            "reply {id}: {answer:?} at {at:?}"
        );
        last_other = last_other.max(*at);
    }
    let pieces = u32::try_from(OTHERS).unwrap() * OTHER_BYTES.div_ceil(PACED_PIECE as u32);
    assert!(last_other >= PACED_EVERY * pieces, "{last_other:?}");
    // The withheld body is given up, by one of the two rules and once; a
    // quiet give-up only where the samples cannot rule out a judgement's
    // quiet before it. And it goes within three judgements of the last of
    // the others.
    let (withheld, at) = &answers[0];
    assert!(
        matches!(withheld, Err(WireError::Timeout)),
        "{withheld:?} at {at:?}"
    );
    let judgement = crate::frame::judgement(wire.request_timeout, remote.longest_round_trip());
    let given_up = remote.given_up();
    assert_eq!(given_up.quiet + given_up.withheld, 1, "{given_up:?}");
    if given_up.quiet == 1 {
        assert!(
            quiet_before(&samples, judgement, *at),
            "given up at {at:?} while the connection carried on (the others' last at {last_other:?}): {:?}",
            samples
                .iter()
                .filter(|(when, _)| *when + judgement * 2 >= *at && *when <= *at + judgement)
                .collect::<Vec<_>>()
        );
    }
    assert!(
        *at <= last_other + judgement * 3,
        "{at:?} long after the others' {last_other:?} (a judgement of {judgement:?})"
    );
    drop(remote);
    peer.abort();
}
/// The sampler's bound: at a sixteenth of a judgement, five minutes of a
/// run whose exchanges end sooner by their own give-ups.
const RECEIVED_SAMPLES: usize = 16_384;
/// The first span of `window` at least, ending by `until`, in which the
/// sampled connection received less than `LEAST_PROGRESS` bytes: the quiet
/// that gives a body up (`Arriving::judge`), as a sampler sees it — a
/// judgement's window has a sample within one interval of either end, so
/// a window of a judgement less two intervals is asked of the samples.
/// Whether the connection's received bytes, sampled at `samples`, leave room
/// for a window of `judgement` ending by `until` in which the connection
/// received less than the least progress: two samples between which it did,
/// whose span, with the gaps to the samples beside them — where the bytes
/// counted at the far sample may have come at its end — reaches a
/// judgement. The samples' own spacing is what they say, however a loaded
/// machine spaced them.
fn quiet_before(samples: &[(Duration, u64)], judgement: Duration, until: Duration) -> bool {
    let least = u64::try_from(crate::frame::LEAST_PROGRESS).unwrap();
    for (first, (from, received_from)) in samples.iter().enumerate() {
        let opens = first
            .checked_sub(1)
            .and_then(|before| samples.get(before))
            .map_or(Duration::ZERO, |(when, _)| *when);
        for (last, (to, received_to)) in samples.iter().enumerate().skip(first) {
            if *to > until || received_to - received_from >= least {
                break;
            }
            let closes = samples
                .get(last + 1)
                .map_or(until, |(when, _)| (*when).min(until));
            if closes.saturating_sub(opens) >= judgement && *to >= *from {
                return true;
            }
        }
    }
    false
}

/// A peer whose issuer succeeded one the verifier knows is admitted through
/// the predecessor's endorsement (24 §11): a CA certificate for the
/// successor's key under the predecessor's signature, presented beside the
/// successor's own — an ordinary intermediate to path building. Without
/// it, or endorsed by a stranger, the chain is refused — in both
/// directions.
#[tokio::test]
async fn a_peer_whose_issuer_the_other_does_not_know_is_admitted_by_the_predecessors_endorsement() {
    fn issuer_params(name: &str) -> CertificateParams {
        let mut params = CertificateParams::new(vec![]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params
    }
    fn constrained(name: &str) -> (Certificate, KeyPair) {
        let key = KeyPair::generate().unwrap();
        let ca = issuer_params(name).self_signed(&key).unwrap();
        (ca, key)
    }
    fn issue(ca: &Certificate, key: &KeyPair, server: bool) -> (Vec<u8>, Vec<u8>) {
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.extended_key_usages = vec![if server {
            ExtendedKeyUsagePurpose::ServerAuth
        } else {
            ExtendedKeyUsagePurpose::ClientAuth
        }];
        let leaf = KeyPair::generate().unwrap();
        let certificate = params.signed_by(&leaf, ca, key).unwrap();
        (certificate.der().to_vec(), leaf.serialize_der())
    }
    // The genesis issuer, with a path length of zero as the cluster's has;
    // its successor, endorsed by it; a stranger.
    let (genesis, genesis_key) = constrained("genesis");
    let (successor, successor_key) = constrained("successor");
    let endorsement = issuer_params("successor")
        .signed_by(&successor_key, &genesis, &genesis_key)
        .unwrap()
        .der()
        .to_vec();
    let (stranger, stranger_key) = constrained("stranger");
    let forged = issuer_params("successor")
        .signed_by(&successor_key, &stranger, &stranger_key)
        .unwrap()
        .der()
        .to_vec();
    let roots = vec![genesis.der().to_vec()];
    // A client issued under the successor, dialing a server that trusts the
    // genesis issuer alone.
    let (client_leaf, client_key) = issue(&successor, &successor_key, false);
    let registry = PeerRegistry::new(16).unwrap();
    registry
        .register_certificate(&client_leaf, grant())
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
        observed.fetch_add(1, Ordering::SeqCst);
        async move { response(verified.request()) }
    });
    let (server_leaf, server_key) = issue(&genesis, &genesis_key, true);
    let tls = server_tls(
        TlsIdentity::from_pkcs8(vec![server_leaf, genesis.der().to_vec()], server_key),
        roots.clone(),
        &limits(),
    )
    .unwrap();
    let server = Arc::new(
        QuicServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            tls,
            registry,
            limits(),
            budget(),
        )
        .unwrap(),
    );
    let running = server.clone();
    let task = tokio::spawn(async move { running.serve(handler).await });
    let address = server.local_addr().unwrap();
    let dial = |chain: Vec<Vec<u8>>| {
        QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            client_tls(
                TlsIdentity::from_pkcs8(chain, client_key.clone()),
                roots.clone(),
                &limits(),
            )
            .unwrap(),
            limits(),
        )
        .unwrap()
    };
    // Without the endorsement: refused before any dispatch.
    assert!(
        dial(vec![client_leaf.clone(), successor.der().to_vec()])
            .connect(address, "localhost")
            .await
            .is_err()
    );
    // Endorsed by a stranger: refused.
    assert!(
        dial(vec![
            client_leaf.clone(),
            successor.der().to_vec(),
            forged.clone()
        ])
        .connect(address, "localhost")
        .await
        .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    // With the endorsement: admitted, and its requests dispatch.
    let remote = dial(vec![
        client_leaf.clone(),
        successor.der().to_vec(),
        endorsement.clone(),
    ])
    .connect(address, "localhost")
    .await
    .unwrap();
    let reply = remote.request(&request(1)).await.unwrap();
    assert_eq!(reply.request_id, request(1).request_id);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.close();
    task.await.unwrap().unwrap();
    // The other direction: a server issued under the successor, dialed by
    // a client that trusts the genesis issuer alone.
    let (server_leaf, server_key) = issue(&successor, &successor_key, true);
    let (client_leaf, client_key) = issue(&genesis, &genesis_key, false);
    let registry = PeerRegistry::new(16).unwrap();
    registry
        .register_certificate(&client_leaf, grant())
        .unwrap();
    let serve = |chain: Vec<Vec<u8>>| {
        let tls = server_tls(
            TlsIdentity::from_pkcs8(chain, server_key.clone()),
            vec![genesis.der().to_vec(), successor.der().to_vec()],
            &limits(),
        )
        .unwrap();
        Arc::new(
            QuicServer::bind(
                "127.0.0.1:0".parse().unwrap(),
                tls,
                registry.clone(),
                limits(),
                budget(),
            )
            .unwrap(),
        )
    };
    let connector = QuicConnector::bind(
        "127.0.0.1:0".parse().unwrap(),
        client_tls(
            TlsIdentity::from_pkcs8(vec![client_leaf, genesis.der().to_vec()], client_key),
            roots.clone(),
            &limits(),
        )
        .unwrap(),
        limits(),
    )
    .unwrap();
    for (chain, admitted) in [
        (vec![server_leaf.clone(), successor.der().to_vec()], false),
        (
            vec![
                server_leaf.clone(),
                successor.der().to_vec(),
                forged.clone(),
            ],
            false,
        ),
        (
            vec![
                server_leaf.clone(),
                successor.der().to_vec(),
                endorsement.clone(),
            ],
            true,
        ),
    ] {
        let server = serve(chain);
        let running = server.clone();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let task = tokio::spawn(async move { running.serve(handler).await });
        let outcome = connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await;
        assert_eq!(outcome.is_ok(), admitted, "{:?}", outcome.as_ref().err());
        if let Ok(remote) = outcome {
            let reply = remote.request(&request(2)).await.unwrap();
            assert_eq!(reply.request_id, request(2).request_id);
        }
        server.close();
        task.await.unwrap().unwrap();
    }
}

/// What path building does with a trust anchor's own length constraint:
/// an endorsement of a successor's key, signed by an anchor issued with a
/// path length of zero, is an ordinary intermediate to a verifier that
/// holds the anchor — the anchor's constraints are not applied (RFC 5280
/// §6.1.1 leaves them to policy; webpki applies none).
#[test]
fn webpki_crosses_a_zero_length_anchor_through_an_endorsement() {
    use rustls::client::danger::ServerCertVerifier;
    fn issuer_params(name: &str) -> CertificateParams {
        let mut params = CertificateParams::new(vec![]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params
    }
    let genesis_key = KeyPair::generate().unwrap();
    let genesis = issuer_params("genesis").self_signed(&genesis_key).unwrap();
    let successor_key = KeyPair::generate().unwrap();
    let successor = issuer_params("successor")
        .self_signed(&successor_key)
        .unwrap();
    let endorsement = issuer_params("successor")
        .signed_by(&successor_key, &genesis, &genesis_key)
        .unwrap();
    let mut leaf_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = leaf_params
        .signed_by(&leaf_key, &successor, &successor_key)
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(genesis.der().clone()).unwrap();
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
    )
    .build()
    .unwrap();
    let now = rustls::pki_types::UnixTime::now();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let with_endorsement = verifier.verify_server_cert(
        leaf.der(),
        &[successor.der().clone(), endorsement.der().clone()],
        &name,
        &[],
        now,
    );
    let without =
        verifier.verify_server_cert(leaf.der(), &[successor.der().clone()], &name, &[], now);
    assert!(with_endorsement.is_ok(), "{with_endorsement:?}");
    assert!(without.is_err(), "{without:?}");
}

/// The ordered profile tops the ladder (27 §12): offered by a handler that
/// steps a peer's frames in their order and asked for by every connector,
/// it is negotiated where both sides have it and the native profile where
/// one does not; a connection that negotiated it admits every request
/// below it, and one that did not admits no ordered frame. A connector made
/// to offer an older binary's profiles never reaches it.
#[tokio::test]
async fn the_ordered_profile_tops_the_ladder_and_an_older_offer_never_reaches_it() {
    let limits = WireLimits::default();
    let hello = |versions: &[u16]| Hello {
        versions: versions.to_vec(),
        max_frame_bytes: limits.max_frame_bytes,
        max_items: limits.max_items,
    };
    let all = hello(&OFFERED_PROTOCOLS);
    assert_eq!(
        limits
            .negotiate_ordered(&all, true, true, true, true)
            .unwrap()
            .protocol,
        ORDERED_PROTOCOL_VERSION
    );
    // A handler that does not step frames in order, or a connector that
    // does not ask for it, stays at the native profile.
    assert_eq!(
        limits
            .negotiate_ordered(&all, true, true, true, false)
            .unwrap()
            .protocol,
        NATIVE_PROTOCOL_VERSION
    );
    let older = hello(&[
        NATIVE_PROTOCOL_VERSION,
        PEER_PROTOCOL_VERSION,
        MANAGED_PROTOCOL_VERSION,
        PROTOCOL_VERSION,
    ]);
    assert_eq!(
        limits
            .negotiate_ordered(&older, true, true, true, true)
            .unwrap()
            .protocol,
        NATIVE_PROTOCOL_VERSION
    );
    assert_eq!(
        limits
            .negotiate_native(&all, true, true, true)
            .unwrap()
            .protocol,
        NATIVE_PROTOCOL_VERSION
    );
    let ordered = Negotiated {
        protocol: ORDERED_PROTOCOL_VERSION,
        max_frame_bytes: 1024,
        max_items: 1,
    };
    for requested in OFFERED_PROTOCOLS {
        assert!(ordered.accepts_protocol(requested), "{requested}");
    }
    assert!(!ordered.accepts_protocol(6));
    let native = Negotiated {
        protocol: NATIVE_PROTOCOL_VERSION,
        ..ordered
    };
    assert!(!native.accepts_protocol(ORDERED_PROTOCOL_VERSION));
    // The ordered frame: a registered tag of its own, carried as control.
    let frame = Operation::RaftOrdered {
        group: [3; 16],
        epoch: 9,
        sequence: 4,
        message: vec![1, 2, 3],
    };
    assert_eq!(frame.registered_tag(), 33);
    assert_eq!(frame.class(), TrafficClass::Control);
    let bytes = postcard::to_allocvec(&frame).unwrap();
    assert_eq!(postcard::from_bytes::<Operation>(&bytes).unwrap(), frame);
    // A connector offers what it is told, within what this binary speaks
    // and always with the base profile.
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let connector = || {
        QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            client_tls(
                TlsIdentity::from_pkcs8(vec![certificate.clone()], key.clone()),
                vec![pki.ca.der().to_vec()],
                &limits,
            )
            .unwrap(),
            limits.clone(),
        )
        .unwrap()
    };
    assert!(connector().offering(&[PROTOCOL_VERSION]).is_ok());
    assert!(
        connector()
            .offering(&[PEER_PROTOCOL_VERSION, PROTOCOL_VERSION])
            .is_ok()
    );
    assert!(connector().offering(&[]).is_err());
    assert!(connector().offering(&[MANAGED_PROTOCOL_VERSION]).is_err());
    assert!(connector().offering(&[PROTOCOL_VERSION, 6]).is_err());
    // A receiver holds an ordered frame to the ordered profile, and a
    // plain frame to the base one; a node sends either.
    let node = || {
        AuthenticatedPeer::local(PeerGrant {
            principal: ParticipantId::from_u128(2),
            tenants: BTreeSet::from([TenantId::from_u128(1)]),
            role: PeerRole::Node { node_id: 2 },
        })
        .unwrap()
    };
    let envelope = |protocol: u16, operation: Operation| RequestEnvelope {
        protocol,
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(77),
        operation,
    };
    let plain = || Operation::Raft {
        group: [3; 16],
        message: vec![1],
    };
    assert!(
        verify_request(
            node(),
            envelope(ORDERED_PROTOCOL_VERSION, frame.clone()),
            &limits
        )
        .is_ok()
    );
    assert!(matches!(
        verify_request(node(), envelope(PROTOCOL_VERSION, frame.clone()), &limits),
        Err(AccessError::UnsupportedProtocol)
    ));
    assert!(verify_request(node(), envelope(PROTOCOL_VERSION, plain()), &limits).is_ok());
    assert!(matches!(
        verify_request(node(), envelope(ORDERED_PROTOCOL_VERSION, plain()), &limits),
        Err(AccessError::UnsupportedProtocol)
    ));
    // An ask of what a copy holds is of the ordered profile and nothing
    // else (the audit's F50); the old ask stays with the base. Both new
    // variants take the tags after the last of their enums.
    // The content's domain is the request's tenant, as every custody
    // request's must be.
    let content = ContentRef {
        domain: ContentDomainId::from_u128(1),
        class: ContentClass::Evidence,
        root: ContentHash([5; 32]),
        length: 16,
    };
    let held = Operation::Custody(CustodyRequest::OpenHeld {
        transfer: [6; 16],
        policy_revision: 1,
        content: content.clone(),
        manifest: vec![7; 8],
    });
    let open = Operation::Custody(CustodyRequest::Open {
        transfer: [6; 16],
        policy_revision: 1,
        content,
        manifest: vec![7; 8],
    });
    assert!(ordered_profile_operation(&held));
    assert!(!ordered_profile_operation(&open));
    assert!(
        verify_request(
            node(),
            envelope(ORDERED_PROTOCOL_VERSION, held.clone()),
            &limits
        )
        .is_ok()
    );
    assert!(matches!(
        verify_request(node(), envelope(PROTOCOL_VERSION, held.clone()), &limits),
        Err(AccessError::UnsupportedProtocol)
    ));
    assert!(verify_request(node(), envelope(PROTOCOL_VERSION, open.clone()), &limits).is_ok());
    assert!(matches!(
        verify_request(
            node(),
            envelope(ORDERED_PROTOCOL_VERSION, open.clone()),
            &limits
        ),
        Err(AccessError::UnsupportedProtocol)
    ));
    let bytes = postcard::to_allocvec(&held).unwrap();
    assert_eq!(postcard::from_bytes::<Operation>(&bytes).unwrap(), held);
    let reply = CustodyReply::OpenedHeld {
        chunks: 70,
        held: vec![u64::MAX, 0b11_1111],
    };
    let bytes = postcard::to_allocvec(&reply).unwrap();
    assert_eq!(postcard::from_bytes::<CustodyReply>(&bytes).unwrap(), reply);
    // The tags: the variants come after every one before them.
    let tag = |bytes: &[u8]| bytes.first().copied().unwrap();
    assert_eq!(
        tag(&postcard::to_allocvec(&CustodyRequest::OpenHeld {
            transfer: [0; 16],
            policy_revision: 0,
            content: ContentRef {
                domain: ContentDomainId::from_u128(0),
                class: ContentClass::Evidence,
                root: ContentHash([0; 32]),
                length: 0,
            },
            manifest: Vec::new(),
        })
        .unwrap()),
        10
    );
    assert_eq!(
        tag(&postcard::to_allocvec(&CustodyReply::OpenedHeld {
            chunks: 0,
            held: Vec::new(),
        })
        .unwrap()),
        9
    );
}

/// A part of a chunk is what the path delivered in an exchange's time
/// (`part_for`, the audit's F49): one window's worth before the peer
/// answered any bulk exchange — not the cold window over the cold round
/// trip stretched over the whole time, which sized a first part at 307 KiB
/// for a path of 128 kbit/s — then the last answered bulk exchange's rate,
/// never more than the law holds in flight over a round trip, a datagram
/// at least and the chunk at most.
#[test]
fn a_part_is_what_the_path_delivered_in_an_exchange_time() {
    use crate::peers::part_for;
    let timeout = Duration::from_millis(1070);
    let chunk = 1024 * 1024;
    // Cold: a window of 11,552 bytes over a 40 ms round trip would have
    // made 307 KiB; the first part is the window.
    assert_eq!(
        part_for(11_552, Duration::from_millis(40), None, timeout, chunk),
        11_552
    );
    // Measured: 11,552 bytes answered in 1 s is 12,360 in 1.07 s.
    assert_eq!(
        part_for(
            11_552,
            Duration::from_millis(40),
            Some((11_552, 1_000_000_000)),
            timeout,
            chunk
        ),
        12_360
    );
    // The law bounds what a measurement claims: a window of 2,948 over a
    // 414 ms round trip holds 7,619 in the time, whatever the last
    // exchange delivered.
    assert_eq!(
        part_for(
            2_948,
            Duration::from_millis(414),
            Some((1_000_000, 1_000_000)),
            timeout,
            chunk
        ),
        7_619
    );
    // A fast path reaches the whole chunk after one window: 11,552 bytes
    // in 3 ms.
    assert_eq!(
        part_for(
            1 << 20,
            Duration::from_micros(500),
            Some((11_552, 3_000_000)),
            timeout,
            chunk
        ),
        chunk
    );
    // A datagram at least, the chunk at most.
    assert_eq!(
        part_for(100, Duration::from_secs(1), None, timeout, chunk),
        crate::frame::LEAST_PROGRESS
    );
    assert_eq!(
        part_for(1 << 30, Duration::from_micros(1), None, timeout, 4096),
        4096
    );
}

/// Every connection focal makes exchanges its keys post-quantum
/// (`crypto`): a client that offers only a classical exchange is refused at
/// the handshake, as is one whose traffic would be sealed with a 128-bit
/// key, while one that offers the hybrid is served; and a focal client
/// refuses a server that speaks only a classical exchange.
#[tokio::test]
async fn a_peer_offering_only_a_classical_key_exchange_is_refused_both_ways() {
    use rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256;
    use rustls::crypto::aws_lc_rs::kx_group::{X25519, X25519MLKEM768};
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(4).unwrap();
    registry
        .register_certificate(&certificate, grant())
        .unwrap();
    let handler: Arc<dyn RequestHandler> =
        Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
    let (server, task) = server(&pki, registry, handler).await;
    let client = |groups: Vec<&'static dyn rustls::crypto::SupportedKxGroup>,
                  suites: Option<Vec<rustls::SupportedCipherSuite>>| {
        let mut provider = rustls::crypto::aws_lc_rs::default_provider();
        provider.kx_groups = groups;
        if let Some(suites) = suites {
            provider.cipher_suites = suites;
        }
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(pki.ca.der().to_vec()))
            .unwrap();
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![CertificateDer::from(certificate.clone())],
                PrivatePkcs8KeyDer::from(key.clone()).into(),
            )
            .unwrap();
        tls.alpn_protocols = vec![ALPN.to_vec()];
        quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
        ))
    };
    let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = server.local_addr().unwrap();
    let refused = endpoint
        .connect_with(client(vec![X25519], None), address, "localhost")
        .unwrap()
        .await;
    assert!(refused.is_err(), "a classical exchange was accepted");
    let short_key = endpoint
        .connect_with(
            client(vec![X25519MLKEM768], Some(vec![TLS13_AES_128_GCM_SHA256])),
            address,
            "localhost",
        )
        .unwrap()
        .await;
    assert!(short_key.is_err(), "a 128-bit traffic key was accepted");
    let served = endpoint
        .connect_with(
            client(vec![X25519MLKEM768, X25519], None),
            address,
            "localhost",
        )
        .unwrap()
        .await;
    assert!(served.is_ok(), "{served:?}");
    // A server that speaks only a classical exchange, to a focal client.
    let (server_certificate, server_key) = pki.issue(true);
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    provider.kx_groups = vec![X25519];
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(server_certificate)],
            PrivatePkcs8KeyDer::from(server_key).into(),
        )
        .unwrap();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let classical = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        )),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let accepting = classical.clone();
    tokio::spawn(async move {
        if let Some(incoming) = accepting.accept().await {
            let _ = incoming.await;
        }
    });
    let focal = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate.clone()], key.clone()),
        vec![pki.ca.der().to_vec()],
        &limits(),
    )
    .unwrap();
    let to_classical = endpoint
        .connect_with(focal, classical.local_addr().unwrap(), "localhost")
        .unwrap()
        .await;
    assert!(
        to_classical.is_err(),
        "a focal client accepted a classical exchange"
    );
    classical.close(0u8.into(), b"done");
    server.close();
    task.await.unwrap().unwrap();
}
