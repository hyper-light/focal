use super::*;
use crate::net::Sender;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::*;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use std::{
    collections::BTreeSet,
    rc::Rc,
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
#[path = "net_tests.rs"]
mod net_tests;
#[path = "raw_peer_tests.rs"]
mod raw_peer;
use raw_peer::{RawPeer, RawServer};
#[path = "peer_mutations_tests.rs"]
mod peer_mutations_tests;

/// A task spawned on the test's shard, its output awaited later (what tokio's `JoinHandle` was):
/// awaited, its output, or an error should the task have been cancelled first.
struct Spawned<T> {
    output: hyper_rt::sync::OneshotReceiver<T>,
    task: hyper_rt::TaskId,
}
fn spawn<T: 'static>(future: impl std::future::Future<Output = T> + 'static) -> Spawned<T> {
    let (sender, output) = hyper_rt::sync::oneshot().unwrap();
    let task = hyper_rt::futures::spawn_detached(async move {
        let _ = sender.send(future.await);
    })
    .unwrap();
    Spawned { output, task }
}
impl<T> Spawned<T> {
    /// The task's output; it panics should the task have been cancelled first.
    async fn join(self) -> T {
        self.await.expect("the task ended without its output")
    }
    /// Cancels the task; what it held is dropped as it ends.
    fn abort(&self) {
        let _ = hyper_rt::futures::cancel(self.task);
    }
}
impl<T> std::future::Future for Spawned<T> {
    type Output = Result<T, hyper_rt::sync::SyncError<()>>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.output).poll(context)
    }
}
/// `period` in the shard clock's nanoseconds.
fn nanos(period: Duration) -> u64 {
    u64::try_from(period.as_nanos()).unwrap_or(u64::MAX)
}
/// A pause on the shard's clock.
async fn pause(period: Duration) {
    hyper_rt::futures::sleep(nanos(period)).await.unwrap();
}
/// What `future` yields within `period` on the shard's clock, or `None` once it is over.
async fn timeout<F: std::future::Future>(period: Duration, future: F) -> Option<F::Output> {
    hyper_rt::futures::within(nanos(period), future)
        .await
        .unwrap()
}
/// Where a test holds its handlers: each waiter takes a ticket as it comes, and passes once as
/// many have been let through (tokio's `Semaphore` with `add_permits`, its permits forgotten).
struct Gate {
    tickets: std::sync::atomic::AtomicU64,
    granted: std::sync::Mutex<hyper_rt::sync::WatchSender>,
    watched: hyper_rt::sync::WatchReceiver,
}
impl Gate {
    fn new() -> Self {
        let (granted, watched) = hyper_rt::sync::watch(0, 64).unwrap();
        Self {
            tickets: std::sync::atomic::AtomicU64::new(0),
            granted: std::sync::Mutex::new(granted),
            watched,
        }
    }
    /// Lets `count` more waiters through, in the order they came.
    fn open(&self, count: u64) {
        let mut granted = self.granted.lock().unwrap();
        let now = granted.borrow();
        granted.send(now + count);
    }
    /// Waits for this waiter's turn.
    async fn pass(&self) {
        let ticket = self.tickets.fetch_add(1, Ordering::SeqCst);
        let mut watched = self.watched.try_clone().unwrap();
        while watched.borrow_and_update() <= ticket {
            watched.changed().await.unwrap();
        }
    }
}

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

#[test]
fn accounted_dispatch_forwards_dynamic_handler_and_retains_both_allowances() {
    focal_rt::block_on(async move {
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
    })
    .unwrap()
}

#[test]
fn a_local_slow_response_keeps_its_output_charge_until_written_or_disconnected() {
    focal_rt::block_on(async move {
        let root = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let socket = root.path().join("accounted.sock");
        let budget = MemoryBudget::new(4 * 1024 * 1024, 1024).unwrap();
        let server = Arc::new(LocalServer::bind(&socket, grant(), limits()).unwrap());
        let serving = server.clone();
        let handler: Arc<dyn RequestHandler> = Arc::new(AccountedDownload {
            budget: budget.clone(),
        });
        let task = spawn(async move { serving.serve(handler).await });
        for disconnect in [false, true] {
            let mut stream = hyper_rt::local::LocalStream::connect(&socket).await.unwrap();
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
            stream.shutdown(hyper_rt::tcp::Shutdown::Write).unwrap();
            // The response's header arriving is the server's write begun; this
            // client reads no further, so the write cannot finish where the
            // kernel holds less than the response, and its output charge is
            // still held. Windows' AF_UNIX takes the whole 900 KB response into
            // the kernel before a byte is read (Windows CI run 38119678828: the
            // charge was given back as the header arrived): there the write has
            // finished, and nothing is held for it.
            let header = unfrozen(
                "the response never began",
                read_frame_header(&mut stream, FrameKind::Response, limits().max_frame_bytes),
            )
            .await
            .unwrap();
            if cfg!(unix) {
                assert!(budget.stats().used >= 900 * 1024 * 3);
            }
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
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A reply its peer does not read stays charged: the server's write is held by the peer's flow
/// control, and what its handler charged for it is given back once the peer has read it whole, or
/// once the peer is gone.
#[test]
fn a_reply_held_by_flow_control_keeps_its_charge_until_read_or_the_connection_is_lost() {
    focal_rt::block_on(async move {
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
        for disconnect in [false, true] {
            let mut raw = RawPeer::connect(
                client_identity(&pki, certificate.clone(), key.clone()),
                server.local_addr().unwrap(),
            )
            .await;
            let request = download_request(900 * 1024);
            let body = encode_payload(&request, limits().max_frame_bytes).unwrap();
            let exchange = raw.ask(&request, body.len());
            raw.write_all(exchange, &body).await.unwrap();
            // The reply begun is the server's write begun; its body is past what this peer's
            // receive window takes unread, so the write cannot finish, and its charge is held.
            unfrozen("the response never began", raw.replied(exchange))
                .await
                .unwrap();
            assert!(budget.stats().used >= 900 * 1024 * 3);
            if disconnect {
                raw.disconnect();
            } else {
                let answer = unfrozen("the response never ended", raw.answer(exchange))
                    .await
                    .unwrap();
                let response: ResponseEnvelope = decode_payload(&answer).unwrap();
                assert!(
                    matches!(response.result, Response::Content(ContentChunk { bytes, .. }) if bytes.len() == 900 * 1024)
                );
            }
            settles("the output charge was never given back", || {
                budget.stats().used == 0
            })
            .await;
            drop(raw);
        }
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}
async fn server(
    pki: &Pki,
    registry: PeerRegistry,
    handler: Arc<dyn RequestHandler>,
) -> (Arc<QuicServer>, Spawned<Result<(), WireError>>) {
    server_at(
        pki,
        registry,
        handler,
        "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
    )
    .await
}
async fn server_at(
    pki: &Pki,
    registry: PeerRegistry,
    handler: Arc<dyn RequestHandler>,
    address: impl Into<Bind>,
) -> (Arc<QuicServer>, Spawned<Result<(), WireError>>) {
    let (certificate, key) = pki.issue(true);
    let tls = server_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &limits(),
    )
    .unwrap();
    let server = Arc::new(QuicServer::bind(address, tls, registry, limits(), budget()).unwrap());
    let running = server.clone();
    let task = spawn(async move { running.serve(handler).await });
    (server, task)
}
fn connector(pki: &Pki, cert: Vec<u8>, key: Vec<u8>) -> QuicConnector {
    QuicConnector::bind(
        "127.0.0.1:0".parse().unwrap(),
        client_identity(pki, cert, key),
        limits(),
        budget(),
    )
    .unwrap()
}
/// The client TLS of `cert`, trusting `pki`'s authority: a participant's.
fn client_identity(pki: &Pki, cert: Vec<u8>, key: Vec<u8>) -> ClientTls {
    client_tls(
        TlsIdentity::from_pkcs8(vec![cert], key),
        vec![pki.ca.der().to_vec()],
        &limits(),
        Sender::Participant,
    )
    .unwrap()
}
/// A connector of a node's identity `cert`, as a node's pool dials.
fn node_connector(pki: &Pki, cert: Vec<u8>, key: Vec<u8>) -> QuicConnector {
    QuicConnector::bind(
        "127.0.0.1:0".parse().unwrap(),
        client_tls(
            TlsIdentity::from_pkcs8(vec![cert], key),
            vec![pki.ca.der().to_vec()],
            &limits(),
            Sender::Node,
        )
        .unwrap(),
        limits(),
        budget(),
    )
    .unwrap()
}

/// A listener bound to its address gives each peer a socket of its own where the platform shares
/// a port (Linux's and macOS's `SO_REUSEPORT`), so each connection brings the receive queue its
/// windows need; one adopted whole holds every peer on its one queue. Do: a client connects to and
/// asks each of two servers, one bound to an address and one to a socket bound first, and the
/// servers' rooms are read. Expect: the first holds the peer's room apart where the platform
/// shares ports, the second never.
#[test]
fn a_listener_bound_to_its_address_gives_each_peer_a_socket_of_its_own() {
    focal_rt::block_on(async move {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(move |verified: VerifiedRequest| async move { response(verified.request()) });
        let adopted = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let shares = cfg!(any(target_os = "linux", target_os = "macos"));
        for (bind, apart) in [
            (
                Bind::from("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()),
                shares,
            ),
            (Bind::from(adopted), false),
        ] {
            let (server_certificate, server_key) = pki.issue(true);
            let tls = server_tls(
                TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
                vec![pki.ca.der().to_vec()],
                &limits(),
            )
            .unwrap();
            let server = Arc::new(
                QuicServer::bind(bind, tls, registry.clone(), limits(), budget()).unwrap(),
            );
            let running = server.clone();
            let handler = handler.clone();
            let task = spawn(async move { running.serve(handler).await });
            let connector = connector(&pki, certificate.clone(), key.clone());
            let remote = connector
                .connect(server.local_addr().unwrap(), "localhost")
                .await
                .unwrap();
            assert_eq!(
                remote.request(&request(1)).await.unwrap(),
                response(&request(1))
            );
            let rooms = server.stats().await.endpoint.receive_rooms;
            assert_eq!(rooms, usize::from(apart), "bound to an address: {apart}");
            drop(remote);
            connector.close().unwrap();
            server.close().unwrap();
            task.join().await.unwrap();
        }
    })
    .unwrap()
}

#[test]
fn mutual_tls_tenant_isolation_live_revocation_and_independent_streams() {
    focal_rt::block_on(async move {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let fingerprint = registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let slow_started = Arc::new(hyper_rt::sync::Notify::new().unwrap());
        let release = Arc::new(hyper_rt::sync::Notify::new().unwrap());
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
                        release.notified().await.unwrap();
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
        let slow = spawn(async move { slow_remote.request(&request(1)).await });
        slow_started.notified().await.unwrap();
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A wait on the listener's admission, charged to its changes.
/// How long what a wait observes may make no progress before the wait is
/// over: a wedge, not slowness (27 §3.1 P8). The in-process servers and
/// peers these tests wait on report no period, so where no counter is
/// observed this window is the wait's only bound.
const FROZEN: Duration = Duration::from_secs(60);

/// What `future` yields, unless it yields nothing for [`FROZEN`].
async fn unfrozen<F: std::future::Future>(what: &str, future: F) -> F::Output {
    match hyper_rt::futures::within(nanos(FROZEN), future)
        .await
        .unwrap()
    {
        Some(output) => output,
        None => panic!("{what}: nothing for {FROZEN:?}"),
    }
}

/// Polls until `settled` holds, unless it does not for [`FROZEN`].
async fn settles(what: &str, settled: impl Fn() -> bool) {
    unfrozen(what, async {
        while !settled() {
            pause(Duration::from_millis(5)).await;
        }
    })
    .await;
}

/// What a listener's connections and exchanges hold of its budget: its bytes less what it keeps
/// whether or not a connection is up, its receive pool and its ring's growth, which each resize as
/// the traffic asks, and less what its work is charged as state, which an observation moves at any
/// quiet moment. A test that compared the whole bytes at two looks raced those: the ring grew or
/// shrank between the looks, or an observation charged what the work kept once the connection had
/// gone (CI 39946268662; reproduced 12 times in 80 on 4 loaded cores).
fn held_by_connections(stats: &AdmissionStats) -> usize {
    stats.bytes.saturating_sub(
        stats
            .pooled
            .saturating_add(stats.ring_charged)
            .saturating_add(stats.state_charged),
    )
}

async fn admission_settles(
    server: &QuicServer,
    settled: impl Fn(&AdmissionStats) -> bool,
) -> AdmissionStats {
    let mut wait = focal_timing::ProgressDeadline::begin(
        &[server.admission().await.changes],
        u64::MAX,
        FROZEN,
    );
    loop {
        let stats = server.admission().await;
        if settled(&stats) {
            return stats;
        }
        if let Err(spent) = wait.check(&[stats.changes]) {
            panic!("the admission never settled: {spent}: {stats:?}");
        }
        pause(Duration::from_millis(5)).await;
    }
}

/// The audit's F35: a grant revoked while a request's body is still
/// arriving. The request is refused when it is complete — what is
/// dispatched is authorized by the grant current then — and the
/// revocation closes the certificate's connection, so nothing more is
/// received on it and what waited for the body ends with it.
#[test]
fn a_grant_revoked_while_a_body_arrives_dispatches_nothing_and_closes_the_connection() {
    focal_rt::block_on(async move {
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
        let mut raw = RawPeer::connect(
            client_identity(&pki, certificate, key),
            server.local_addr().unwrap(),
        )
        .await;
        // What the listener holds with the connection up and nothing asked: its windows.
        let idle = admission_settles(&server, |stats| stats.connections == 1).await;
        let body = encode_payload(&request(1), limits().max_frame_bytes).unwrap();
        let exchange = raw.ask(&request(1), body.len());
        assert_eq!(raw.write(exchange, &body[..body.len() / 2]), body.len() / 2);
        // The body is held whole under its sender's share before the revocation: it is reserved
        // from the listener's budget as its request begins.
        admission_settles(&server, |stats| {
            held_by_connections(stats) == held_by_connections(&idle) + body.len()
        })
        .await;
        registry.revoke(fingerprint).unwrap();
        // The connection is closed by the revocation, the peer told it was refused for its
        // identity: the rest of the body has nowhere to go, and what it held is given back.
        assert_eq!(
            unfrozen("the revocation never closed the connection", raw.closed()).await,
            Some(hyper_transport::Refusal::Identity)
        );
        assert!(raw.try_write(exchange, &body[body.len() / 2..]).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // What it held is given back: no connection or exchange holds anything, the body and the
        // connection's windows alike. What the listener's work keeps with no connection (its
        // tables grown, the sessions its TLS server stored) is charged as state, not to them.
        let stats = admission_settles(&server, |stats| {
            held_by_connections(stats) == 0 && stats.connections == 0
        })
        .await;
        assert_eq!(held_by_connections(&stats), 0, "{stats:?}");
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// The audit's F35, the other order: a body that completes after the
/// grant was withdrawn but before the close reaches its stream is refused
/// at dispatch by the grant current then. The registry is asked again
/// once the whole request has arrived; the early check alone would have
/// dispatched it.
/// A handler slower than the asker's period is answered for, "not yet"
/// (`Unavailable` for a read, `OutcomeUnknown` for a mutation), and the
/// answer arrives within the period the asker gives (its request time, which
/// a node and its askers share), never after the asker has refused the
/// exchange as stalled. Answered at the server's full request time, it came
/// after the asker's period every time: that period began when the asker
/// sent, before the server had the request, and the server's timer fires no
/// earlier than due (evidence_quic's seal across 128 kbit/s, 2026-10-10).
#[test]
fn a_handler_slower_than_the_askers_period_is_answered_within_it() {
    focal_rt::block_on(async move {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let handler: Arc<dyn RequestHandler> = Arc::new(|_: VerifiedRequest| async {
            std::future::pending::<ResponseEnvelope>().await
        });
        let (server, task) = server(&pki, registry.clone(), handler).await;
        let connector = connector(&pki, certificate, key);
        let remote = connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await
            .unwrap();
        for id in 1..=2 {
            let answered = remote.request(&request(id)).await;
            assert!(
                matches!(
                    &answered,
                    Ok(ResponseEnvelope {
                        result: Response::Error(AccessError::Unavailable),
                        ..
                    })
                ),
                "ask {id}: {answered:?}"
            );
        }
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// Each of an identity's connections is greeted on its own: its second connection, greeted and
/// closed, leaves its first one's profile as it was, and the first is served on. Kept per peer,
/// the second's greeting overwrote the first's and its close erased it, and the first's next
/// request was refused as ungreeted (`network_listener`, a node whose probe of the listener closed
/// beside its route's connection, 2026-10-10).
#[test]
fn a_second_connection_of_an_identity_leaves_the_first_greeted() {
    focal_rt::block_on(async move {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let (server, task) = server(&pki, registry, handler).await;
        let address = server.local_addr().unwrap();
        let first_process = connector(&pki, certificate.clone(), key.clone());
        let second_process = connector(&pki, certificate, key);
        let first = first_process.connect(address, "localhost").await.unwrap();
        assert_eq!(
            first.request(&request(1)).await.unwrap(),
            response(&request(1))
        );
        let second = second_process.connect(address, "localhost").await.unwrap();
        assert_eq!(
            second.request(&request(2)).await.unwrap(),
            response(&request(2))
        );
        second.close().unwrap();
        admission_settles(&server, |stats| stats.connections == 1).await;
        assert_eq!(
            first.request(&request(3)).await.unwrap(),
            response(&request(3))
        );
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// Connections of one identity made at once are each greeted and served, though a peer's lifecycle
/// tells its current connection alone: kept only for the told connections, the greetings of the
/// others went unrecorded and their requests were refused as ungreeted (three clients of one
/// identity in cli_native_a4, 2026-10-10).
#[test]
fn connections_of_one_identity_made_at_once_are_each_greeted() {
    focal_rt::block_on(async move {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let (server, task) = server(&pki, registry, handler).await;
        let address = server.local_addr().unwrap();
        let processes: Vec<QuicConnector> = (0..3)
            .map(|_| connector(&pki, certificate.clone(), key.clone()))
            .collect();
        let (a, b, c) = hyper_rt::combine::join3(
            processes[0].connect(address, "localhost"),
            processes[1].connect(address, "localhost"),
            processes[2].connect(address, "localhost"),
        )
        .await;
        let remotes = [a.unwrap(), b.unwrap(), c.unwrap()];
        let (x, y, z) = hyper_rt::combine::join3(
            remotes[0].request(&request(1)),
            remotes[1].request(&request(2)),
            remotes[2].request(&request(3)),
        )
        .await;
        assert_eq!(x.unwrap(), response(&request(1)));
        assert_eq!(y.unwrap(), response(&request(2)));
        assert_eq!(z.unwrap(), response(&request(3)));
        for (at, remote) in remotes.iter().enumerate() {
            let id = 10 + at as u128;
            assert_eq!(
                remote.request(&request(id)).await.unwrap(),
                response(&request(id))
            );
        }
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

#[test]
fn a_complete_request_is_authorized_by_the_grant_current_at_dispatch() {
    focal_rt::block_on(async move {
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
        admission_settles(&server, |stats| stats.connections == 0).await;
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

#[test]
fn certificates_outside_the_trusted_ca_never_dispatch() {
    focal_rt::block_on(async move {
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
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

#[test]
fn a_watched_local_grant_governs_connections_accepted_after_it_changes() {
    focal_rt::block_on(async move {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("focal.sock");
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let other = TenantId::from_u128(0x7e);
        let mut narrow = grant();
        narrow.tenants = BTreeSet::from([other]);
        let watched = LocalGrant::new(narrow);
        let server =
            Arc::new(LocalServer::bind_watched(&socket, watched.clone(), limits()).unwrap());
        let running = server.clone();
        let handling = handler.clone();
        let task = spawn(async move { running.serve(handling).await });
        let remote = LocalRemote::new(&socket, limits()).unwrap();
        // The bound grant names another tenant: the request's ledger is refused.
        assert!(matches!(
            remote.request(&request(1)).await.unwrap().result,
            Response::Error(AccessError::Unauthorized)
        ));
        // Widening the grant serves the next connection under the new value
        // without rebinding the socket.
        assert!(watched.update(|current| current.tenants.insert(ledger().tenant)));
        let expected = dispatch(
            handler.as_ref(),
            AuthenticatedPeer::local(grant()).unwrap(),
            request(2),
            &limits(),
        )
        .await;
        assert_eq!(remote.request(&request(2)).await.unwrap(), expected);
        // Narrowing it again refuses again; the grant outlives every connection.
        assert!(watched.update(|current| current.tenants.remove(&ledger().tenant)));
        assert!(matches!(
            remote.request(&request(3)).await.unwrap().result,
            Response::Error(AccessError::Unauthorized)
        ));
        server.close();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A local socket's path at the most its address holds binds and serves; one
/// byte longer is refused by name by the server and by its client, before
/// anything is bound, where the operating system's refusal had read as an
/// unavailable connection (a node whose data directory was 114 bytes could
/// not start, `wire: connection unavailable`).
#[test]
fn a_local_socket_path_past_its_address_is_refused_by_name() {
    focal_rt::block_on(async move {
        let dir = tempfile::tempdir().unwrap();
        let name = "focal.sock";
        // The directory's own path, a separator, the filler, a separator and
        // the socket's name: the filler takes the socket's path to the bound.
        let fill = SOCKET_PATH_BYTES
            .checked_sub(dir.path().as_os_str().len() + 2 + name.len())
            .expect("the temporary directory leaves room under the bound");
        let at = dir.path().join("d".repeat(fill));
        std::fs::create_dir(&at).unwrap();
        let socket = at.join(name);
        assert_eq!(socket.as_os_str().len(), SOCKET_PATH_BYTES);
        let past = dir.path().join("d".repeat(fill + 1));
        std::fs::create_dir(&past).unwrap();
        let too_long = past.join(name);
        assert_eq!(too_long.as_os_str().len(), SOCKET_PATH_BYTES + 1);
        #[cfg(unix)]
        {
            // The bound is the address's own, as std builds it.
            assert!(std::os::unix::net::SocketAddr::from_pathname(&socket).is_ok());
            assert!(std::os::unix::net::SocketAddr::from_pathname(&too_long).is_err());
        }
        assert!(socket_path_fits(&socket).is_ok());
        let refused = |error: WireError| {
            matches!(
                error,
                WireError::SocketPath { path, most }
                    if path == too_long && most == SOCKET_PATH_BYTES
            )
        };
        assert!(refused(socket_path_fits(&too_long).unwrap_err()));
        assert!(refused(
            LocalServer::bind(&too_long, grant(), limits())
                .err()
                .unwrap()
        ));
        assert!(!too_long.exists());
        assert!(refused(
            LocalRemote::new(&too_long, limits()).err().unwrap()
        ));
        // At the bound the system binds the socket and serves through it.
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let server = Arc::new(LocalServer::bind(&socket, grant(), limits()).unwrap());
        let running = server.clone();
        let handling = handler.clone();
        let task = spawn(async move { running.serve(handling).await });
        let remote = LocalRemote::new(&socket, limits()).unwrap();
        let expected = dispatch(
            handler.as_ref(),
            AuthenticatedPeer::local(grant()).unwrap(),
            request(1),
            &limits(),
        )
        .await;
        assert_eq!(remote.request(&request(1)).await.unwrap(), expected);
        server.close();
        task.join().await.unwrap();
    })
    .unwrap()
}

#[test]
fn a_local_socket_is_its_owners_alone_and_serves_the_same_verified_handler() {
    focal_rt::block_on(async move {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("focal.sock");
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let server = Arc::new(LocalServer::bind(&socket, grant(), limits()).unwrap());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert_eq!(
            focal_platform::fs::owner_at(&socket).unwrap(),
            focal_platform::fs::current_owner().unwrap()
        );
        let running = server.clone();
        let handling = handler.clone();
        let task = spawn(async move { running.serve(handling).await });
        let remote = LocalRemote::new(&socket, limits()).unwrap();
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
        task.join().await.unwrap();
        drop(server);
        assert!(!socket.exists());
    })
    .unwrap()
}

/// The same-user local transport round-trips a request on every platform: a
/// local stream socket on each (`AF_UNIX` on Linux, macOS and Windows, through
/// hyper-rt's local sockets). The kernel names the peer's user, and the frames
/// are byte-identical, so the dispatched response matches a direct dispatch
/// under the same grant.
#[test]
fn local_transport_round_trips_between_same_user_endpoints() {
    focal_rt::block_on(async move {
        let dir = tempfile::tempdir().unwrap();
        let endpoint = dir.path().join("focal.sock");
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let server = Arc::new(LocalServer::bind(&endpoint, grant(), limits()).unwrap());
        let running = server.clone();
        let handling = handler.clone();
        let task = spawn(async move { running.serve(handling).await });
        let remote = LocalRemote::new(&endpoint, limits()).unwrap();
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
        task.join().await.unwrap();
    })
    .unwrap()
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

#[test]
fn unsupported_protocol_is_rejected_before_dispatch() {
    focal_rt::block_on(async move {
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
        let mut raw = RawPeer::dial(
            client_identity(&pki, certificate, key),
            server.local_addr().unwrap(),
        )
        .await;
        let reply = raw
            .greet(&Hello {
                versions: vec![999],
                max_frame_bytes: 1024,
                max_items: 1,
            })
            .await;
        assert!(matches!(
            reply,
            HelloReply::Rejected(AccessError::UnsupportedProtocol)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
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

#[test]
fn oversized_upload_is_denied_before_handler_and_bad_upload_reply_is_unknown() {
    focal_rt::block_on(async move {
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
    })
    .unwrap()
}

/// A message the peer refuses on every attempt is counted lost by what the
/// peer answered: serving no such group (`Unavailable`, as a member whose
/// copy of a new session is not open yet answers its leader) apart from an
/// outcome it could not say. The two had been counted as one, so a
/// comparison's leader showed 13 lost "of unknown outcome" at every start,
/// each a refusal without effect.
#[test]
fn a_peers_refusals_are_counted_by_what_it_answered() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node_grant = grant();
        node_grant.role = PeerRole::Node { node_id: 7 };
        registry
            .register_certificate(&certificate, node_grant)
            .unwrap();
        let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
            let refusal = match &verified.request().operation {
                Operation::Raft { group, .. } if *group == [3; 16] => AccessError::Unavailable,
                _ => AccessError::OutcomeUnknown,
            };
            async move { verified.request().reply(Response::Error(refusal)) }
        });
        let (server, task) = server(&pki, registry, handler).await;
        let pool = PeerConnectionPool::new(
            node_connector(&pki, certificate, key),
            PeerPoolLimits {
                retry_backoff: Duration::ZERO,
                ..PeerPoolLimits::default()
            },
        )
        .unwrap();
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
        let mut unopened = request(91);
        unopened.operation = Operation::Raft {
            group: [3; 16],
            message: vec![1],
        };
        let mut unknown = request(92);
        unknown.operation = Operation::Raft {
            group: [4; 16],
            message: vec![2],
        };
        assert_eq!(
            pool.send(2, &unopened).await,
            Err(PeerSendError::Rejected(AccessError::Unavailable))
        );
        assert_eq!(
            pool.send(2, &unknown).await,
            Err(PeerSendError::Rejected(AccessError::OutcomeUnknown))
        );
        let stats = pool.stats();
        assert_eq!(stats.lost, 2);
        assert_eq!(stats.lost_unavailable, 1);
        assert_eq!(stats.lost_unknown, 1);
        assert_eq!(stats.lost_refused + stats.lost_rejected, 0);
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// Each side's sockets say what they moved: after an exchange, the server's
/// and the pool's both sent and received datagrams, in no more system calls
/// than datagrams (the batching the kernel saw). Once connected, the client's
/// datagrams reach the server by a socket of their own where the port is
/// shared, and the server's counts take them in all the same.
#[test]
fn a_sockets_datagrams_and_calls_are_counted() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
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
        let (server, task) = server(&pki, registry, handler).await;
        let pool = PeerConnectionPool::new(
            node_connector(&pki, certificate, key),
            PeerPoolLimits {
                retry_backoff: Duration::ZERO,
                ..PeerPoolLimits::default()
            },
        )
        .unwrap();
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
        let mut packet = request(93);
        packet.operation = Operation::Raft {
            group: [5; 16],
            message: vec![3],
        };
        pool.send(2, &packet).await.unwrap();
        let connected = server.io().await;
        for id in 94..110 {
            let mut more = request(id);
            more.operation = packet.operation.clone();
            pool.send(2, &more).await.unwrap();
        }
        let server_io = server.io().await;
        assert!(
            server_io.received > connected.received,
            "the server's sockets counted nothing more: {connected:?} then {server_io:?}"
        );
        for io in [server_io, pool.io().await] {
            assert!(io.sent > 0 && io.received > 0, "{io:?}");
            assert!(io.send_calls > 0 && io.send_calls <= io.sent, "{io:?}");
            assert!(
                io.receive_calls > 0 && io.receive_calls <= io.received,
                "{io:?}"
            );
            assert_eq!(io.send_errors + io.outbox_full, 0, "{io:?}");
        }
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A replication handler that steps a peer's frames in their order (so it takes the laned
/// profile), recording each message it is handed.
#[derive(Clone, Default)]
struct Recorded {
    seen: Arc<std::sync::Mutex<Vec<Seen>>>,
}
/// A message a [`Recorded`] handler was handed: its operation's tag and its Raft message.
type Seen = (u16, Vec<u8>);
impl RequestHandler for Recorded {
    fn supports_managed_requests(&self) -> bool {
        true
    }
    fn supports_participant_requests(&self) -> bool {
        true
    }
    fn supports_native_requests(&self) -> bool {
        true
    }
    fn supports_ordered_replication(&self) -> bool {
        true
    }
    fn supports_lane_frames(&self) -> bool {
        true
    }
    fn take_frame(&self, request: VerifiedRequest) -> Result<(), AccessError> {
        self.record(&request);
        Ok(())
    }
    fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
        self.record(request);
        Box::pin(async move { request.request().reply(Response::PeerAccepted) })
    }
}
impl Recorded {
    fn record(&self, request: &VerifiedRequest) {
        let message = match &request.request().operation {
            Operation::Raft { message, .. } | Operation::RaftOrdered { message, .. } => {
                message.clone()
            }
            _ => Vec::new(),
        };
        self.seen
            .lock()
            .unwrap()
            .push((request.request().operation.registered_tag(), message));
    }
}

/// A node's replication goes on its lanes once both sides speak the laned profile: every frame
/// is handed to the handler once, each lane's in the order they were queued — a group's plain
/// frames on the urgent lane, its ordered ones on the bulk lane — and none is answered: past the
/// greeting the pool asks the server nothing.
#[test]
fn a_nodes_replication_goes_on_its_lanes_once_each_and_in_order() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node_grant = grant();
        node_grant.role = PeerRole::Node { node_id: 7 };
        registry
            .register_certificate(&certificate, node_grant)
            .unwrap();
        let handler = Recorded::default();
        let (server, task) = server(&pki, registry, Arc::new(handler.clone())).await;
        let pool = PeerConnectionPool::new(
            node_connector(&pki, certificate, key),
            PeerPoolLimits {
                retry_backoff: Duration::ZERO,
                ..PeerPoolLimits::default()
            },
        )
        .unwrap();
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
        const FRAMES: u8 = 40;
        for index in 0..FRAMES {
            let mut packet = request(1_000 + u128::from(index));
            if index % 2 == 0 {
                packet.operation = Operation::Raft {
                    group: [5; 16],
                    message: vec![index],
                };
            } else {
                packet.protocol = ORDERED_PROTOCOL_VERSION;
                packet.operation = Operation::RaftOrdered {
                    group: [5; 16],
                    epoch: 1,
                    sequence: u64::from(index),
                    message: vec![index],
                };
            }
            pool.send(2, &packet).await.unwrap();
        }
        settles("every frame handed to the handler", || {
            handler.seen.lock().unwrap().len() == usize::from(FRAMES)
        })
        .await;
        let plain_tag = Operation::Raft {
            group: [5; 16],
            message: vec![0],
        }
        .registered_tag();
        let seen = handler.seen.lock().unwrap().clone();
        let lane = |tag_is_plain: bool| -> Vec<u8> {
            seen.iter()
                .filter(|(tag, _)| (*tag == plain_tag) == tag_is_plain)
                .map(|(_, message)| message[0])
                .collect()
        };
        assert_eq!(lane(true), (0..FRAMES).step_by(2).collect::<Vec<_>>());
        assert_eq!(lane(false), (1..FRAMES).step_by(2).collect::<Vec<_>>());
        let served = server.stats().await;
        assert_eq!(served.lane_frames_received, u64::from(FRAMES), "{served:?}");
        assert_eq!(served.lane_frames_dropped, 0, "{served:?}");
        let sent = pool.net_stats().await;
        assert_eq!(sent.lane_frames_sent, u64::from(FRAMES), "{sent:?}");
        assert_eq!(sent.lane_frames_refused, 0, "{sent:?}");
        // No frame was asked as an exchange (the greeting is the connection's own, uncounted).
        assert_eq!(sent.answered, 0, "{sent:?}");
        // What the path measures came back with the frames, as with answers: a leader's window
        // to its member follows it (27 §11).
        assert!(pool.window(2).is_some_and(|window| window > 0));
        // A snapshot is answered once its member took it: asked as an exchange beside the
        // lanes, never queued on one.
        let mut snapshot = request(2_000);
        snapshot.protocol = ORDERED_PROTOCOL_VERSION;
        snapshot.operation = Operation::RaftOrdered {
            group: [5; 16],
            epoch: 1,
            sequence: u64::from(FRAMES),
            message: vec![FRAMES],
        };
        pool.send_as(2, &snapshot, Delivery::Answered)
            .await
            .unwrap();
        let sent = pool.net_stats().await;
        assert_eq!(sent.answered, 1, "{sent:?}");
        assert_eq!(sent.lane_frames_sent, u64::from(FRAMES), "{sent:?}");
        assert_eq!(handler.seen.lock().unwrap().len(), usize::from(FRAMES) + 1);
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A lane that is full holds what comes for it until it has room, instead of refusing it: a burst
/// of frames near the frame bound on a lane of four frames, as many as it queues and as many
/// again, all arrive, once each and in the order they were queued, and some of them waited for
/// room (the endpoint's `LaneRoom` woke them).
#[test]
fn a_full_lane_holds_its_frames_until_it_has_room() {
    focal_rt::block_on(async move {
        const FRAMES: u8 = 8;
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node_grant = grant();
        node_grant.role = PeerRole::Node { node_id: 7 };
        registry
            .register_certificate(&certificate, node_grant)
            .unwrap();
        let handler = Recorded::default();
        let (server, task) = server(&pki, registry, Arc::new(handler.clone())).await;
        // One consensus window of one: each lane queues four frames, and as many wait beside it.
        let narrow = WireLimits {
            control_streams: 1,
            ..limits()
        };
        let connector = QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            client_tls(
                TlsIdentity::from_pkcs8(vec![certificate], key),
                vec![pki.ca.der().to_vec()],
                &narrow,
                Sender::Node,
            )
            .unwrap(),
            narrow,
            budget(),
        )
        .unwrap();
        let remote = Rc::new(
            connector
                .connect(server.local_addr().unwrap(), "localhost")
                .await
                .unwrap(),
        );
        assert!(remote.negotiated().laned());
        let sends: Vec<_> = (0..FRAMES)
            .map(|index| {
                let remote = remote.clone();
                spawn(async move {
                    let mut packet = request(4_000 + u128::from(index));
                    packet.protocol = ORDERED_PROTOCOL_VERSION;
                    let mut message = vec![index; 900 * 1024];
                    message[0] = index;
                    packet.operation = Operation::RaftOrdered {
                        group: [5; 16],
                        epoch: 1,
                        sequence: u64::from(index),
                        message,
                    };
                    remote
                        .send_lane(crate::transport::BULK_LANE, &packet)
                        .await
                        .map(|_| ())
                })
            })
            .collect();
        for send in sends {
            send.join().await.unwrap();
        }
        settles("every frame handed to the handler", || {
            handler.seen.lock().unwrap().len() == usize::from(FRAMES)
        })
        .await;
        let order: Vec<u8> = handler
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|(_, message)| message[0])
            .collect();
        assert_eq!(order, (0..FRAMES).collect::<Vec<_>>());
        let sent = connector.stats().await;
        assert_eq!(sent.lane_frames_sent, u64::from(FRAMES), "{sent:?}");
        assert_eq!(sent.lane_frames_refused, 0, "{sent:?}");
        assert!(sent.lane_frames_parked > 0, "{sent:?}");
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A lane frame its handler refuses (its owner has no room) is let go and counted dropped,
/// never received: nothing answers it, and the sender hears only that it was queued.
#[test]
fn a_lane_frame_its_handler_refuses_is_counted_dropped() {
    #[derive(Clone, Default)]
    struct Refusing;
    impl RequestHandler for Refusing {
        fn supports_managed_requests(&self) -> bool {
            true
        }
        fn supports_participant_requests(&self) -> bool {
            true
        }
        fn supports_native_requests(&self) -> bool {
            true
        }
        fn supports_ordered_replication(&self) -> bool {
            true
        }
        fn supports_lane_frames(&self) -> bool {
            true
        }
        fn take_frame(&self, _request: VerifiedRequest) -> Result<(), AccessError> {
            Err(AccessError::Capacity)
        }
        fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
            Box::pin(async move { request.request().reply(Response::PeerAccepted) })
        }
    }
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node_grant = grant();
        node_grant.role = PeerRole::Node { node_id: 7 };
        registry
            .register_certificate(&certificate, node_grant)
            .unwrap();
        let (server, task) = server(&pki, registry, Arc::new(Refusing)).await;
        let pool = PeerConnectionPool::new(
            node_connector(&pki, certificate, key),
            PeerPoolLimits {
                retry_backoff: Duration::ZERO,
                ..PeerPoolLimits::default()
            },
        )
        .unwrap();
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
        for index in 0..3u8 {
            let mut packet = request(3_000 + u128::from(index));
            packet.operation = Operation::Raft {
                group: [5; 16],
                message: vec![index],
            };
            pool.send(2, &packet).await.unwrap();
        }
        unfrozen("every frame let go", async {
            while server.stats().await.lane_frames_dropped < 3 {
                pause(Duration::from_millis(5)).await;
            }
        })
        .await;
        let served = server.stats().await;
        assert_eq!(served.lane_frames_received, 0, "{served:?}");
        assert_eq!(pool.net_stats().await.answered, 0);
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

#[test]
fn peer_pool_caches_connections_reconnects_identical_packets_and_fences_routes() {
    focal_rt::block_on(async move {
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
            node_connector(&pki, certificate, key),
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
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
#[test]
fn an_operation_refused_or_unanswered_takes_no_other_exchange_with_its_connection() {
    focal_rt::block_on(async move {
        use hyper_rt::sync::Notify;
        use std::collections::BTreeMap;
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
        // One held request at a time waits at the gate.
        let gate = Arc::new(Notify::new().unwrap());
        let held = Arc::new(Notify::new().unwrap());
        let silent = Arc::new(Notify::new().unwrap());
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
                        release.notified().await.unwrap();
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
            Sender::Node,
        )
        .unwrap();
        let pool = Rc::new(
            PeerConnectionPool::new(
                QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire, budget()).unwrap(),
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
            spawn(async move { pool.send(2, &packet).await })
        };
        pool.send(2, &message(100)).await.unwrap();
        assert_eq!(pool.stats().connections_opened, 1);

        // A request the peer holds, while it refuses two others.
        let waiting = send(HELD);
        held.notified().await.unwrap();
        assert_eq!(
            pool.send(2, &message(REFUSED)).await,
            Err(PeerSendError::Rejected(AccessError::Unavailable))
        );
        assert_eq!(
            pool.send(2, &message(UNKNOWN)).await,
            Err(PeerSendError::Rejected(AccessError::OutcomeUnknown))
        );
        gate.notify_one();
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
            spawn(async move {
                let mut probe = request(200);
                probe.operation = Operation::Probe {
                    request: vec![1, 2, 3],
                };
                loop {
                    assert_eq!(pool.send_probe(2, &probe).await, Ok(vec![1]));
                    pause(Duration::from_millis(100)).await;
                }
            })
        };
        let unanswered = send(SILENT);
        silent.notified().await.unwrap();
        pause(Duration::from_secs(1)).await;
        let waiting = send(HELD);
        held.notified().await.unwrap();
        // Asked again: its first exchange ended without what it asked for.
        silent.notified().await.unwrap();
        gate.notify_one();
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
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

#[test]
fn peer_pool_re_resolves_a_named_endpoint_when_its_address_stops_answering() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
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
        let (server, task) = server(&pki, registry, handler).await;
        let live = server.local_addr().unwrap();
        // A UDP socket bound and dropped: nothing answers there.
        let stale = {
            let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            socket.local_addr().unwrap()
        };
        let pool = PeerConnectionPool::new(
            node_connector(&pki, certificate, key),
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A peer that moved behind its name while every caller gives up long before
/// the dead address's deadline (a liveness probe, a bounded control read):
/// the dial runs on its own and the name's fresh address wins, so a later
/// caller finds the connection instead of restarting from the dead address
/// forever (24 §24; the 2026-09-13 all-pods-moved wedge).
#[test]
fn peer_pool_reaches_a_peer_that_moved_behind_its_name_while_every_caller_gives_up_early() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
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
        let (server, task) = server(&pki, registry, handler).await;
        let live = server.local_addr().unwrap();
        // A UDP socket bound and dropped: nothing answers there, and a dial to it
        // only fails at the connector's deadline.
        let stale = {
            let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            socket.local_addr().unwrap()
        };
        let pool = PeerConnectionPool::new(
            node_connector(&pki, certificate, key),
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
                match timeout(Duration::from_millis(200), pool.send(2, &packet)).await {
                    Some(Ok(_)) => break,
                    Some(Err(error)) => panic!("the send failed rather than timing out: {error}"),
                    None => pause(Duration::from_millis(20)).await,
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// Callers that give up before a dial to a dead address decides neither
/// abandon it nor repeat it: one dial serves them all, and its outcome still
/// marks the peer unreachable so the next send fails at once.
#[test]
fn peer_pool_callers_that_give_up_share_one_dial_whose_outcome_is_still_recorded() {
    focal_rt::block_on(async move {
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
        let pool = Rc::new(pool);
        let callers: Vec<_> = (0..6)
            .map(|_| {
                let pool = pool.clone();
                let packet = packet.clone();
                spawn(
                    async move { timeout(Duration::from_millis(100), pool.send(2, &packet)).await },
                )
            })
            .collect();
        for caller in callers {
            // A caller either ran out its own deadline waiting on the dial or was
            // refused at once by the per-peer bound; none was answered by the dial.
            match caller.await.unwrap() {
                None | Some(Err(PeerSendError::Busy)) => {}
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
    })
    .unwrap()
}

#[test]
fn a_request_in_flight_ends_when_its_route_is_retired_and_not_at_its_deadline() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node_grant = grant();
        node_grant.role = PeerRole::Node { node_id: 7 };
        registry
            .register_certificate(&certificate, node_grant)
            .unwrap();
        let started = Arc::new(hyper_rt::sync::Notify::new().unwrap());
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
        let pool = Rc::new(
            PeerConnectionPool::new(
                node_connector(&pki, certificate, key),
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
            let pending = spawn(async move { sending.send(2, &packet).await });
            unfrozen("the request never reached the peer", started.notified())
                .await
                .unwrap();
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A route retired while its connection is still being made never leaves a
/// connection behind: whatever the order of the handshake and the
/// retirement, the pool holds none afterwards and the next route works.
#[test]
fn a_route_retired_during_its_dial_leaves_no_connection_behind() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
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
        let (server, task) = server(&pki, registry, handler).await;
        let pool = Rc::new(
            PeerConnectionPool::new(
                node_connector(&pki, certificate, key),
                PeerPoolLimits::default(),
            )
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
            let pending = spawn(async move { sending.send(2, &sent).await });
            // Yield a varying number of times so the retirement lands at
            // different points of the dial.
            for _ in 0..round % 8 {
                hyper_rt::futures::yield_now().await;
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
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
#[test]
fn a_groups_message_waits_its_turn_on_the_lane_instead_of_being_refused() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node_grant = grant();
        node_grant.role = PeerRole::Node { node_id: 7 };
        registry
            .register_certificate(&certificate, node_grant)
            .unwrap();
        let (counted, mut arrived) = hyper_rt::sync::channel::<()>(3).unwrap();
        let release = Arc::new(hyper_rt::sync::Notify::new().unwrap());
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (released, seen) = (release.clone(), order.clone());
        let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
            let (counted, released, seen) = (counted.clone(), released.clone(), seen.clone());
            async move {
                seen.lock().unwrap().push(verified.request().request_id.0);
                counted.try_send(()).unwrap();
                released.notified().await.unwrap();
                verified.request().reply(Response::PeerAccepted)
            }
        });
        let (server, task) = server(&pki, registry, handler).await;
        let pool = Rc::new(
            PeerConnectionPool::new(
                node_connector(&pki, certificate, key),
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
            sends.push(spawn(async move { sending.send(2, &packet).await }));
        }
        // One reaches the peer at a time; the others wait on the lane, refused
        // by no one, and each is released once it has arrived.
        for _ in 0..3 {
            arrived.recv().await.unwrap();
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

#[test]
fn peer_pool_saturation_is_bounded_and_route_change_retires_active_connections() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node_grant = grant();
        node_grant.role = PeerRole::Node { node_id: 7 };
        registry
            .register_certificate(&certificate, node_grant)
            .unwrap();
        let started = Arc::new(hyper_rt::sync::Notify::new().unwrap());
        let gated = started.clone();
        let release = Arc::new(hyper_rt::sync::Notify::new().unwrap());
        let released = release.clone();
        let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
            let gated = gated.clone();
            let released = released.clone();
            async move {
                gated.notify_one();
                let _ = released.notified().await;
                verified.request().reply(Response::PeerAccepted)
            }
        });
        let (server, task) = server(&pki, registry, handler).await;
        let pool = Rc::new(
            PeerConnectionPool::new(
                node_connector(&pki, certificate, key),
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
        let pending = spawn(async move { sending.send(2, &sent).await });
        unfrozen("the request never reached the peer", started.notified())
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
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
/// delay. Its tasks run on the test's shard until they are aborted.
async fn shaped(
    server: std::net::SocketAddr,
    shape: Shape,
) -> (std::net::SocketAddr, Vec<Spawned<()>>) {
    use std::cell::Cell;
    let front = Rc::new(hyper_rt::udp::UdpSocket::bind(([127, 0, 0, 1], 0)).unwrap());
    let back = Rc::new(hyper_rt::udp::UdpSocket::bind(([127, 0, 0, 1], 0)).unwrap());
    let address = front.local_addr().unwrap();
    let client = Rc::new(Cell::new(None::<std::net::SocketAddr>));
    let began = hyper_rt::futures::now_ns();
    let mut tasks = Vec::new();
    for up in [true, false] {
        let (from, to) = if up {
            (front.clone(), back.clone())
        } else {
            (back.clone(), front.clone())
        };
        let bits = if up { shape.up_bits } else { shape.down_bits };
        let (queue, mut waiting) = hyper_rt::sync::channel::<Vec<u8>>(shape.queue).unwrap();
        let known = client.clone();
        tasks.push(spawn(async move {
            let mut datagram = vec![0u8; 65_536];
            while let Ok((length, source)) = from.recv_from(&mut datagram).await {
                if up {
                    known.set(Some(source));
                }
                let _ = queue.try_send(datagram[..length].to_vec());
            }
        }));
        let known = client.clone();
        tasks.push(spawn(async move {
            let mut random = shape.seed ^ if up { 0x9E37_79B9_7F4A_7C15 } else { 0 };
            let mut draw = move || {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                random
            };
            let mut free = hyper_rt::futures::now_ns();
            while let Ok(datagram) = waiting.recv().await {
                free = free.max(hyper_rt::futures::now_ns())
                    + datagram.len() as u64 * 8 * 1_000_000_000 / bits;
                hyper_rt::futures::sleep(free.saturating_sub(hyper_rt::futures::now_ns()))
                    .await
                    .unwrap();
                let lost = draw() % 1_000_000 < u64::from(shape.loss_ppm);
                let out = shape.outage.is_some_and(|(from, length)| {
                    let at = Duration::from_nanos(hyper_rt::futures::now_ns() - began);
                    at >= from && at < from + length
                });
                let target = if up { Some(server) } else { known.get() };
                let Some(target) = target else { continue };
                if lost || out {
                    continue;
                }
                let travel = shape.delay
                    + Duration::from_nanos(
                        draw() % (shape.jitter.as_nanos() as u64).saturating_add(1),
                    );
                if travel.is_zero() {
                    let _ = to.send_to(&datagram, target);
                } else {
                    // No more travel at once than the bottleneck lets out
                    // in the longest travel.
                    let to = to.clone();
                    hyper_rt::futures::spawn_detached(async move {
                        pause(travel).await;
                        let _ = to.send_to(&datagram, target);
                    })
                    .unwrap();
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
) -> (std::net::SocketAddr, Vec<Spawned<()>>) {
    shaped(server, Shape::even(bits)).await
}

/// A megabyte over a path that takes longer to carry it than a request is
/// given is carried, each way: an exchange waits as long as the path takes
/// (`Carriage`, `read_payload_arriving`), and no longer for a peer that
/// does not answer than the path would have taken.
#[test]
fn narrow_path_carries_a_megabyte_that_takes_longer_than_a_request_is_given() {
    focal_rt::block_on(async move {
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
        let silent = Arc::new(hyper_rt::sync::Notify::new().unwrap());
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
                        let _ = never.notified().await;
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
                "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
                tls,
                registry,
                wire.clone(),
                budget(),
            )
            .unwrap(),
        );
        let running = server.clone();
        let task = spawn(async move { running.serve(handler).await });
        // Four megabits in a second: a megabyte takes two seconds and more.
        const BITS: u64 = 4_000_000;
        let (path, relays) = narrow(server.local_addr().unwrap(), BITS).await;
        let tls = client_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &wire,
            Sender::Node,
        )
        .unwrap();
        let connector =
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire.clone(), budget())
                .unwrap();
        let remote = connector.connect(path, "localhost").await.unwrap();
        let custody = |id: u128, operation: CustodyRequest| {
            let mut packet = request(id);
            packet.operation = Operation::Custody(operation);
            packet
        };
        let began = hyper_rt::futures::now_ns();
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
        let sent = Duration::from_nanos(hyper_rt::futures::now_ns() - began);
        assert!(sent >= least, "{sent:?}");
        let began = hyper_rt::futures::now_ns();
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
        let received = Duration::from_nanos(hyper_rt::futures::now_ns() - began);
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
        let began = hyper_rt::futures::now_ns();
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
                        result: Response::Error(
                            AccessError::Unavailable | AccessError::OutcomeUnknown
                        ),
                        ..
                    })
            ),
            "{unanswered:?}"
        );
        assert!(
            Duration::from_nanos(hyper_rt::futures::now_ns() - began) >= Duration::from_millis(300)
        );
        // One handler waits: the one asked to cancel.
        silent.notify_one();
        remote.close().unwrap();
        server.close().unwrap();
        task.join().await.unwrap();
        for relay in relays {
            relay.abort();
        }
    })
    .unwrap()
}

/// Content to a peer has a lane of its own: a transfer that fills it
/// refuses nothing a group sends there, content that comes to a full lane
/// waits its turn, and what waits is counted and bounded.
#[test]
fn content_waits_its_turn_and_refuses_nothing_a_group_sends() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let mut node_grant = grant();
        node_grant.role = PeerRole::Node { node_id: 7 };
        registry
            .register_certificate(&certificate, node_grant)
            .unwrap();
        let held = Arc::new(Gate::new());
        let (seen, mut arrived) = hyper_rt::sync::channel::<()>(8).unwrap();
        let gate = held.clone();
        let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
            let (gate, seen) = (gate.clone(), seen.clone());
            async move {
                match &verified.request().operation {
                    Operation::Custody(CustodyRequest::Cancel { .. }) => {
                        seen.try_send(()).unwrap();
                        gate.pass().await;
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
            Sender::Node,
        )
        .unwrap();
        let connector =
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire, budget()).unwrap();
        let pool = Rc::new(
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
            sent.push(spawn(async move { sending.send_custody(2, &packet).await }));
        }
        // Two reached the peer and two wait their turn: the lane is full.
        for _ in 0..2 {
            arrived.recv().await.unwrap();
        }
        while pool.stats().bulk_inflight < 4 {
            hyper_rt::futures::yield_now().await;
        }
        assert_eq!(arrived.try_recv(), Ok(None));
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
        held.open(2);
        for _ in 0..2 {
            arrived.recv().await.unwrap();
        }
        held.open(2);
        for pending in sent {
            assert!(matches!(
                pending.await.unwrap(),
                Ok(CustodyReply::Cancelled)
            ));
        }
        assert_eq!(pool.stats().bulk_inflight, 0);
        pool.close();
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// Off a hyper-rt shard, every entry point refuses, typed, before it opens a socket or runs a
/// handler: no timer or socket of a shard's to keep its deadlines with.
#[test]
fn a_missing_runtime_returns_errors_before_socket_or_handler_work() {
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
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unused.sock");
    assert!(matches!(
        LocalServer::bind(&path, grant(), limits.clone()),
        Err(WireError::Connection)
    ));
    assert!(!path.exists());
    let pki = Pki::new();
    let (certificate, key) = pki.issue(true);
    let tls = server_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &limits,
    )
    .unwrap();
    assert!(matches!(
        QuicServer::bind(
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
            tls,
            PeerRegistry::new(4).unwrap(),
            limits.clone(),
            budget(),
        ),
        Err(WireError::Connection)
    ));
    let (certificate, key) = pki.issue(false);
    assert!(matches!(
        QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            client_identity(&pki, certificate, key),
            limits.clone(),
            budget(),
        ),
        Err(WireError::Connection)
    ));
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

#[test]
fn multiplexed_connection_cannot_bypass_data_alpn_or_client_identity() {
    focal_rt::block_on(async move {
        use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
        for (client_identity, protocol) in [(false, ALPN), (true, b"focal-enroll/1".as_slice())] {
            let pki = Pki::new();
            let (server_certificate, server_key) = pki.issue(true);
            let (client_certificate, client_key) = pki.issue(false);
            let provider = crate::crypto_provider();
            let mut roots = rustls::RootCertStore::empty();
            roots
                .add(CertificateDer::from(pki.ca.der().to_vec()))
                .unwrap();
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                roots.clone(),
                provider,
            )
            .allow_unauthenticated()
            .build()
            .unwrap();
            let mut server_tls = rustls::ServerConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_client_cert_verifier(verifier)
                .with_single_cert(
                    vec![CertificateDer::from(server_certificate)],
                    PrivatePkcs8KeyDer::from(server_key).into(),
                )
                .unwrap();
            server_tls.alpn_protocols = vec![protocol.to_vec()];
            let server_tls = ServerTls::from_config(server_tls);
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
                ClientTls::from_config(client_tls, Sender::Participant),
                limits(),
                budget(),
            )
            .unwrap();
            let registry = PeerRegistry::new(4).unwrap();
            registry
                .register_certificate(&client_certificate, grant())
                .unwrap();
            let server = Arc::new(
                QuicServer::bind(
                    "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
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
            let task = spawn(async move {
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
            server.close().unwrap();
            task.join().await.unwrap();
        }
    })
    .unwrap()
}

/// A peer of the first data protocol (`focal/1`: focal's frames on QUIC streams of their own)
/// shares no protocol with this one: its handshake is refused, so no byte it sends is read as a
/// message and its handler is never called, while the same identity on this protocol is served.
#[test]
fn a_peer_of_the_first_data_protocol_is_refused_at_the_handshake() {
    focal_rt::block_on(async move {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(4).unwrap();
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
        let mut first = client_identity(&pki, certificate.clone(), key.clone()).into_config();
        first.alpn_protocols = vec![b"focal/1".to_vec()];
        let old = QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            ClientTls::from_config(first, Sender::Participant),
            limits(),
            budget(),
        )
        .unwrap();
        let refused = old.connect(server.local_addr().unwrap(), "localhost").await;
        assert!(refused.is_err(), "a focal/1 peer was connected");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let current = connector(&pki, certificate, key);
        let remote = current
            .connect(server.local_addr().unwrap(), "localhost")
            .await
            .unwrap();
        assert_eq!(
            remote.request(&request(1)).await.unwrap(),
            response(&request(1))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        remote.close().unwrap();
        old.close().unwrap();
        current.close().unwrap();
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A handle polled off a hyper-rt shard — by a plain thread's executor, say — refuses at once,
/// typed, and queues nothing for its owner: it has no shard's timers or sockets to keep a wait
/// with. On its shard the same handle still serves.
#[test]
fn a_handle_polled_off_its_shard_refuses_at_once_and_still_serves_on_it() {
    focal_rt::block_on(async move {
        use std::future::Future;
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
        let address = server.local_addr().unwrap();
        let remote = connector.connect(address, "localhost").await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let local = LocalRemote::new(root.path().join("node.sock"), limits()).unwrap();
        fn once<F: Future>(future: F) -> std::task::Poll<F::Output> {
            let mut future = std::pin::pin!(future);
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            future.as_mut().poll(&mut context)
        }
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert!(matches!(
                        once(remote.request(&request(1))),
                        std::task::Poll::Ready(Err(WireError::Connection))
                    ));
                    assert!(matches!(
                        once(connector.connect(address, "localhost")),
                        std::task::Poll::Ready(Err(WireError::Connection))
                    ));
                    assert!(matches!(
                        once(local.request(&request(1))),
                        std::task::Poll::Ready(Err(WireError::Connection))
                    ));
                })
                .join()
                .unwrap();
        });
        assert_eq!(
            remote.request(&request(2)).await.unwrap(),
            response(&request(2))
        );
        // The refused dial queued nothing: the connector still holds its one connection.
        assert_eq!(connector.stats().await.endpoint.connections, 1);
        remote.close().unwrap();
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
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
#[test]
fn peer_pool_dials_an_unreachable_peer_once_per_cooldown_and_fails_the_rest_fast() {
    focal_rt::block_on(async move {
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
        pause(cooldown).await;
        assert_eq!(pool.send(2, &packet).await, Err(PeerSendError::Lost));
        assert_eq!(
            pool.stats().dials,
            2,
            "the peer is dialed again after the cooldown"
        );
        assert_eq!(pool.stats().connections_opened, 0);
        pool.close();
    })
    .unwrap()
}

/// A liveness probe is never refused for a dial cooldown, and a peer that
/// answers ends its cooldown. A send refused in the cooldown never reached
/// the peer, so for the failure detector it is no probe at all: the detector
/// read the instant `Lost` as a probe unanswered, and a node that came back
/// from a pause, dialed in vain while it was gone, was suspected and
/// declared dead again while it ran (the zone stage on macOS CI). The probe
/// dials; its connection then carries replication at once.
#[test]
fn a_probe_dials_through_a_cooldown_and_a_peer_that_answers_ends_it() {
    focal_rt::block_on(async move {
        use std::collections::BTreeMap;
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        // An address nothing answers at yet: a UDP socket bound and held unread, then handed to
        // the server that answers there, so the port is never free between. Dropped and its
        // address bound again seconds later, it lost the port to another socket of a busy suite
        // (Linux CI 39887469244: the server's bind failed `Connection`).
        let held = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let address = held.local_addr().unwrap();
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
        // What the unanswered dial left in the socket is no one's now: the server starts from an
        // empty socket, as one bound afresh would.
        held.set_nonblocking(true).unwrap();
        let mut datagram = [0u8; 2048];
        // One dial's datagrams at most, the pool's cooldown sending no other meanwhile.
        for _ in 0..1024 {
            if held.recv_from(&mut datagram).is_err() {
                break;
            }
        }
        let (server, task) = server_at(&pki, registry, handler, held).await;
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// What an exchange with a peer is expected to take (27 §3.1 P1): measured
/// over the exchanges it answered, doubled for each one given up on, and
/// what a round's budget is derived from.
#[test]
fn an_exchange_given_up_on_lengthens_what_its_peer_is_expected_to_take() {
    focal_rt::block_on(async move {
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
        let started = Arc::new(hyper_rt::sync::Notify::new().unwrap());
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
        let pool = Rc::new(
            PeerConnectionPool::new(
                node_connector(&pki, certificate, key),
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
            let pending = spawn(async move { sending.send_placement(2, &packet).await });
            unfrozen("the exchange never reached the peer", started.notified())
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
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

mod admission {
    //! Admission by identity (27 §3.1 P5), over real connections. A client endpoint keeps one
    //! connection to a route, so each connection here is an endpoint of its own — a participant's
    //! processes, one identity among them where a test says so. The admission rules themselves are
    //! hyper-transport's and tested there (`admission::tests`, `tests/exchange.rs`; its ORIGIN.md
    //! names each test ported from here); these hold focal's listener to them end to end.
    use super::*;

    /// The listeners' request timeout here, and so how long a connection must have moved nothing
    /// before it may be replaced (the endpoint's serve period).
    const QUIET: Duration = Duration::from_millis(300);

    fn quick() -> WireLimits {
        WireLimits {
            request_timeout: QUIET,
            ..limits()
        }
    }
    async fn admitting(
        pki: &Pki,
        registry: PeerRegistry,
        admission: AdmissionLimits,
    ) -> (Arc<QuicServer>, Spawned<Result<(), WireError>>) {
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        admitting_with(pki, registry, admission, handler).await
    }
    async fn admitting_with(
        pki: &Pki,
        registry: PeerRegistry,
        admission: AdmissionLimits,
        handler: Arc<dyn RequestHandler>,
    ) -> (Arc<QuicServer>, Spawned<Result<(), WireError>>) {
        admitting_on(pki, registry, admission, handler, budget()).await
    }
    /// [`admitting_with`] on `budget`.
    async fn admitting_on(
        pki: &Pki,
        registry: PeerRegistry,
        admission: AdmissionLimits,
        handler: Arc<dyn RequestHandler>,
        budget: MemoryBudget,
    ) -> (Arc<QuicServer>, Spawned<Result<(), WireError>>) {
        let (certificate, key) = pki.issue(true);
        let tls = server_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &quick(),
        )
        .unwrap();
        let server = Arc::new(
            QuicServer::bind_admitting(
                "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
                tls,
                registry,
                quick(),
                admission,
                budget,
            )
            .unwrap(),
        );
        let running = server.clone();
        let task = spawn(async move { running.serve(handler).await });
        (server, task)
    }
    fn bounds() -> AdmissionLimits {
        AdmissionLimits {
            pending: 8,
            identities: 8,
            connections: 16,
            replace_after: QUIET,
            retry_bytes: None,
        }
    }
    /// A process of the identity of `certificate`: an endpoint of its own, its own connection.
    fn process(pki: &Pki, certificate: &[u8], key: &[u8]) -> QuicConnector {
        connector(pki, certificate.to_vec(), key.to_vec())
    }
    /// A connection of a process of its own, to `address`.
    async fn dial(
        pki: &Pki,
        certificate: &[u8],
        key: &[u8],
        address: std::net::SocketAddr,
    ) -> (QuicConnector, Result<QuicRemote, WireError>) {
        let process = process(pki, certificate, key);
        let remote = process.connect(address, "localhost").await;
        (process, remote)
    }
    /// What the server holds, once it says so: a connection's end reaches
    /// the server a moment after the client's close.
    async fn held(server: &QuicServer, connections: usize) -> AdmissionStats {
        // Charged to what the listener does: it ends when the listener has
        // changed nothing for the frozen window.
        let mut wait = focal_timing::ProgressDeadline::begin(
            &[server.admission().await.changes],
            u64::MAX,
            Duration::from_secs(30),
        );
        loop {
            let stats = server.admission().await;
            // Held, and idle: no request under way, its answer carried.
            if stats.connections == connections && stats.pending == 0 && stats.serving == 0 {
                return stats;
            }
            if let Err(spent) = wait.check(&[stats.changes]) {
                panic!("the server never held {connections}: {spent}: {stats:?}");
            }
            pause(Duration::from_millis(5)).await;
        }
    }
    /// The connections the listener holds have moved nothing for its serve period: what makes one
    /// replaceable is that time passing, so it is waited for, from after their last answer.
    async fn quiet() {
        pause(QUIET).await;
    }
    async fn serves(remote: &QuicRemote, id: u128) -> bool {
        remote.request(&request(id)).await.is_ok()
    }

    #[test]
    fn a_listeners_admission_is_derived_from_its_connections() {
        assert_eq!(
            AdmissionLimits::for_connections(128),
            AdmissionLimits {
                pending: 32,
                identities: 128,
                connections: 128,
                replace_after: Duration::ZERO,
                retry_bytes: None,
            }
        );
        assert_eq!(AdmissionLimits::for_connections(1).pending, 1);
        // A Retry once the handshakes hold msquic's default share of the node's memory.
        assert_eq!(
            AdmissionLimits::for_connections(8)
                .retrying_past(retry_memory(4 << 30))
                .retry_bytes,
            Some((4u64 << 30) / 65_535 * 65)
        );
        assert_eq!(
            AdmissionLimits::for_connections(8)
                .replacing_after(QUIET)
                .replace_after,
            QUIET
        );
    }

    /// A listener admits what its budget funds: the budget past its socket's charge over the least
    /// a connection is charged as it is made, its first window, the reserve on it and its
    /// structures' own bytes with the exchange tables its streams size; at least one, and nothing
    /// but the budget bounds it, whatever count its limits state for its exchanges.
    #[test]
    fn a_listeners_bound_is_what_its_budget_funds() {
        let charge = hyper_transport::connection_charge::<crate::net::FocalClasses>(
            &endpoint_limits(&quick(), &AdmissionLimits::for_connections(1)).unwrap(),
        );
        let socket = endpoint_socket_bytes();
        let charge = usize::try_from(charge).unwrap();
        for connections in [1usize, 2, 128, 1_000, 10_000] {
            let bytes = socket + connections * charge;
            assert_eq!(funded_connections(&quick(), bytes).unwrap(), connections);
            assert_eq!(
                funded_connections(&quick(), bytes - 1).unwrap(),
                (connections - 1).max(1)
            );
        }
        // Its streams' tables are its own: more streams, fewer connections.
        let wide = WireLimits::for_consensus(512);
        assert!(
            funded_connections(&wide, 64 << 20).unwrap()
                < funded_connections(&quick(), 64 << 20).unwrap()
        );
        // No budget gives less than one, and no constant caps what one funds.
        assert_eq!(funded_connections(&quick(), 0).unwrap(), 1);
        assert_eq!(
            funded_connections(&quick(), usize::MAX).unwrap(),
            (usize::MAX - socket) / charge
        );
    }

    /// A listener charges its connections what they hold where it runs, learned there: its owner
    /// runs on a heap of its own (the test, as the focal binary, allocates through mimalloc), its
    /// endpoint observes that heap as its connections double, and once agents hold connections and
    /// go quiet each is charged what one held on average, past the least a connection is charged
    /// before anything is observed, the listener's budget holding the whole. Once they have left,
    /// what the connections were charged goes back with them (hyper-raft docs/transport.md §4x).
    #[test]
    fn a_listener_charges_its_connections_what_they_hold_where_it_runs() {
        const AGENTS: usize = 32;
        focal_rt::block_on(async move {
            let pki = Pki::new();
            let registry = PeerRegistry::new(AGENTS).unwrap();
            let agents: Vec<(Vec<u8>, Vec<u8>)> = (0..AGENTS)
                .map(|agent| {
                    let (certificate, key) = pki.issue(false);
                    let principal = ParticipantId::from_u128(3_000 + agent as u128);
                    registry
                        .register_certificate(
                            &certificate,
                            PeerGrant {
                                principal,
                                ..grant()
                            },
                        )
                        .unwrap();
                    (certificate, key)
                })
                .collect();
            let funded = funded_connections(&quick(), budget().limit()).unwrap();
            let admission = AdmissionLimits::for_connections(funded).replacing_after(QUIET);
            let (server, task) = admitting(&pki, registry, admission).await;
            let address = server.local_addr().unwrap();
            let mut held_by_agents = Vec::with_capacity(AGENTS);
            for (agent, (certificate, key)) in agents.iter().enumerate() {
                let (process, remote) = dial(&pki, certificate, key, address).await;
                let remote = remote.unwrap_or_else(|error| panic!("agent {agent}: {error:?}"));
                assert!(serves(&remote, agent as u128).await, "agent {agent}");
                held_by_agents.push((process, remote));
            }
            held(&server, AGENTS).await;
            let least = hyper_transport::state_charge::<crate::net::FocalClasses>(
                &endpoint_limits(&quick(), &admission).unwrap(),
            );
            // The owner observes as its loop turns; once the connections are quiet it has.
            let mut wait = focal_timing::ProgressDeadline::begin(
                &[server.admission().await.changes],
                u64::MAX,
                Duration::from_secs(30),
            );
            let stats = loop {
                let stats = server.stats().await;
                if stats.endpoint.state.observations >= 2 && stats.endpoint.state.each > least {
                    break stats;
                }
                if let Err(spent) = wait.check(&[server.admission().await.changes]) {
                    panic!(
                        "the listener learned nothing: {spent}: {:?}",
                        stats.endpoint.state
                    );
                }
                pause(QUIET).await;
            };
            let state = stats.endpoint.state;
            assert!(state.base.is_some(), "{state:?}");
            assert_eq!(state.unfunded, 0, "{state:?}");
            assert!(
                state.charged >= AGENTS as u64 * least,
                "{AGENTS} connections charged {} B, each at least {least} B",
                state.charged
            );
            // The listener's budget holds the connections' state with their windows and its socket.
            assert!(stats.budget.ordinary as u64 >= state.charged, "{stats:?}");
            drop(held_by_agents);
            held(&server, 0).await;
            let mut wait = focal_timing::ProgressDeadline::begin(
                &[server.admission().await.changes],
                u64::MAX,
                Duration::from_secs(30),
            );
            loop {
                let state = server.stats().await.endpoint.state;
                if state.charged < AGENTS as u64 * least {
                    break;
                }
                if let Err(spent) = wait.check(&[server.admission().await.changes]) {
                    panic!("the connections' state was not given back: {spent}: {state:?}");
                }
                pause(QUIET).await;
            }
            server.close().unwrap();
            task.join().await.unwrap();
        })
        .unwrap()
    }

    /// A participant alone uses the whole listener, and replaces nothing until it is full; full,
    /// its next connection takes the place of its own idle one used least.
    #[test]
    fn a_participant_alone_uses_the_whole_listener() {
        focal_rt::block_on(async move {
            let pki = Pki::new();
            let (certificate, key) = pki.issue(false);
            let registry = PeerRegistry::new(16).unwrap();
            registry
                .register_certificate(&certificate, grant())
                .unwrap();
            let (server, task) = admitting(
                &pki,
                registry,
                AdmissionLimits {
                    connections: 8,
                    ..bounds()
                },
            )
            .await;
            let address = server.local_addr().unwrap();
            let mut processes = Vec::new();
            for id in 0..8u128 {
                let (process, remote) = dial(&pki, &certificate, &key, address).await;
                let remote = remote.unwrap();
                assert!(serves(&remote, 700 + id).await);
                processes.push((process, remote));
            }
            let stats = held(&server, 8).await;
            assert_eq!((stats.admitted, stats.replaced), (8, 0), "{stats:?}");
            // Full: the ninth replaces the idle one it used least, the first.
            quiet().await;
            let (ninth_process, ninth) = dial(&pki, &certificate, &key, address).await;
            let ninth = ninth.unwrap();
            assert!(serves(&ninth, 710).await);
            assert_eq!(held(&server, 8).await.replaced, 1);
            assert!(
                !serves(&processes[0].1, 711).await,
                "the least used still serves"
            );
            assert!(serves(&processes[7].1, 712).await);
            drop((ninth_process, processes));
            server.close().unwrap();
            task.join().await.unwrap();
        })
        .unwrap()
    }

    /// A thousand agents, each an identity and a process of its own, hold a connection each to one
    /// listener at once, and each is answered while the others are held: the listener admits what
    /// its budget funds of each connection's charge as it is made (`funded_connections`), where it
    /// admitted 128 and the 129th agent took another's connection. Each connection holds its
    /// state's charge of the budget while it is up and gives it back as it goes: once every agent
    /// has left, the listener holds what it held before any came. The listener's budget is an
    /// endpoint's allowance for the agents (`endpoint_budget`): `funded_connections` is a bound the
    /// budget reaches first, so a budget that its count merely exceeds funds no thousand. Bound on
    /// the shared 64 MiB, whose 48 MiB ordinary part held the agents' state with only the receive
    /// ring's least growth to spare, agent 996 was refused for memory whenever load grew the ring
    /// (50.2 MB charged, the ring 1.3 MiB: 3 runs in 20 on a loaded Linux host, once in CI).
    #[test]
    fn a_thousand_agents_each_hold_a_connection_to_one_listener() {
        const AGENTS: usize = 1_000;
        // Each agent's socket, and the listener's own socket for it where the platform shares a
        // port (hyper-rt-transport's driver): past the 256 files macOS starts a process with.
        if let Some(files) = focal_platform::raise_open_files() {
            assert!(
                files >= 2 * AGENTS as u64 + 256,
                "the process may hold {files} files open, fewer than two for each agent"
            );
        }
        focal_rt::block_on(async move {
            let pki = Pki::new();
            let registry = PeerRegistry::new(AGENTS).unwrap();
            let agents: Vec<(Vec<u8>, Vec<u8>)> = (0..AGENTS)
                .map(|agent| {
                    let (certificate, key) = pki.issue(false);
                    let principal = ParticipantId::from_u128(1_000 + agent as u128);
                    registry
                        .register_certificate(
                            &certificate,
                            PeerGrant {
                                principal,
                                ..grant()
                            },
                        )
                        .unwrap();
                    (certificate, key)
                })
                .collect();
            // As a node binds its listener: it admits what its budget funds, the budget an
            // endpoint's allowance for the agents, which funds them under the transport's own
            // bounds (each connection at the most its windows hold, the receive pool and socket,
            // and a body each way on each stream for the completion lane)
            let budget = endpoint_budget(
                &WireLimits {
                    max_connections: AGENTS,
                    ..quick()
                },
                None,
            )
            .unwrap();
            let funded = funded_connections(&quick(), budget.limit()).unwrap();
            assert!(funded > AGENTS, "the budget funds {funded} connections");
            let admission = AdmissionLimits::for_connections(funded).replacing_after(QUIET);
            let handler: Arc<dyn RequestHandler> =
                Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
            let (server, task) = admitting_on(&pki, registry, admission, handler, budget).await;
            let address = server.local_addr().unwrap();
            // What the listener holds apart from its receive pool and its ring's growth, which it
            // keeps whether or not a connection is up: its socket's charge, before any agent comes.
            let own =
                |stats: &AdmissionStats| (stats.bytes - stats.pooled - stats.ring_charged) as u64;
            let before = own(&server.admission().await);
            let mut held_by_agents = Vec::with_capacity(AGENTS);
            for (agent, (certificate, key)) in agents.iter().enumerate() {
                let (process, remote) = dial(&pki, certificate, key, address).await;
                let remote = remote.unwrap_or_else(|error| panic!("agent {agent}: {error:?}"));
                assert!(serves(&remote, agent as u128).await, "agent {agent}");
                held_by_agents.push((process, remote));
            }
            let stats = held(&server, AGENTS).await;
            assert_eq!((stats.identities, stats.replaced), (AGENTS, 0), "{stats:?}");
            let state = hyper_transport::state_charge::<crate::net::FocalClasses>(
                &endpoint_limits(&quick(), &admission).unwrap(),
            );
            let connections = own(&stats) - before;
            assert!(
                connections >= AGENTS as u64 * state,
                "{AGENTS} connections hold {connections} B of the budget, each charged {state} B"
            );
            // Every agent is answered while every other holds its connection.
            for (agent, (_, remote)) in held_by_agents.iter().enumerate() {
                assert!(
                    serves(remote, (AGENTS + agent) as u128).await,
                    "agent {agent} again"
                );
            }
            drop(held_by_agents);
            held(&server, 0).await;
            // A closed connection keeps its state until it has drained (RFC 9000 §10.2), its
            // charge given back then: the wait is charged to the listener's bytes as they come
            // back.
            let mut wait = focal_timing::ProgressDeadline::begin(
                &[own(&server.admission().await)],
                u64::MAX,
                Duration::from_secs(30),
            );
            loop {
                let left = own(&server.admission().await);
                if left == before {
                    break;
                }
                if let Err(spent) = wait.check(&[left]) {
                    panic!(
                        "{left} B held once every agent left, {before} B before any came: {spent}"
                    );
                }
                pause(Duration::from_millis(5)).await;
            }
            server.close().unwrap();
            task.join().await.unwrap();
        })
        .unwrap()
    }

    /// A connection carrying a request is never replaced: an identity at its bound whose every
    /// connection has a request under way is refused a newer one, retryably, and the requests
    /// under way are answered; once a connection has been quiet its serve period, a newer one
    /// replaces it.
    #[test]
    fn a_connection_with_a_request_under_way_is_never_replaced() {
        focal_rt::block_on(async move {
            let pki = Pki::new();
            let (certificate, key) = pki.issue(false);
            let registry = PeerRegistry::new(16).unwrap();
            registry
                .register_certificate(&certificate, grant())
                .unwrap();
            // Requests above id 100 wait at the gate, counted as they reach it.
            let gate = Arc::new(Gate::new());
            let entered = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let (gated, reached) = (gate.clone(), entered.clone());
            let handler: Arc<dyn RequestHandler> = Arc::new(move |verified: VerifiedRequest| {
                let (gate, reached) = (gated.clone(), reached.clone());
                async move {
                    if verified.request().request_id > RequestId::from_u128(100) {
                        reached.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        gate.pass().await;
                    }
                    response(verified.request())
                }
            });
            // A listener of two: the identity's share, alone, is both.
            let (server, task) = admitting_with(
                &pki,
                registry,
                AdmissionLimits {
                    connections: 2,
                    ..bounds()
                },
                handler,
            )
            .await;
            let address = server.local_addr().unwrap();
            let (first_process, first) = dial(&pki, &certificate, &key, address).await;
            let (second_process, second) = dial(&pki, &certificate, &key, address).await;
            let (first, second) = (Arc::new(first.unwrap()), Arc::new(second.unwrap()));
            assert!(serves(&first, 1).await && serves(&second, 2).await);
            // The requests are held longer than the listener's request timeout would allow a
            // handler, so the clients give them that long: the gate holds them, not the timeout.
            let under_way = [first.clone(), second.clone()].map(|remote| {
                spawn(async move {
                    remote
                        .request_within(&request(101), Duration::from_secs(30))
                        .await
                })
            });
            // Both requests are under way once the handler holds them, the wait charged to the
            // listener's own changes.
            let mut wait = focal_timing::ProgressDeadline::begin(
                &[server.admission().await.changes],
                u64::MAX,
                Duration::from_secs(30),
            );
            while entered.load(std::sync::atomic::Ordering::Acquire) < 2 {
                if let Err(spent) = wait.check(&[server.admission().await.changes]) {
                    panic!("the requests never began: {spent}");
                }
                pause(Duration::from_millis(5)).await;
            }
            // However long they have been under way, a third is refused: each held connection
            // has a request under way.
            quiet().await;
            let (third_process, third) = dial(&pki, &certificate, &key, address).await;
            let refused = match &third {
                Ok(remote) => !serves(remote, 3).await,
                Err(_) => true,
            };
            assert!(
                refused,
                "a connection with a request under way was replaced"
            );
            assert_eq!(server.admission().await.refused_busy, 1);
            assert_eq!(server.admission().await.replaced, 0);
            // The requests under way are answered, not lost.
            gate.open(2);
            for request in under_way {
                assert!(request.await.unwrap().is_ok());
            }
            // Quiet now: a newer connection replaces the one used least.
            held(&server, 2).await;
            quiet().await;
            let (fourth_process, fourth) = dial(&pki, &certificate, &key, address).await;
            assert!(serves(&fourth.unwrap(), 4).await);
            assert_eq!(server.admission().await.replaced, 1);
            drop((first_process, second_process, third_process, fourth_process));
            server.close().unwrap();
            let _ = task.await;
        })
        .unwrap()
    }

    /// The audit's F20: the connections held in all are bounded after the replacement rule. A
    /// full listener grows for no one: an identity at its share is refused, and so is one over
    /// it that is not yet at its own bound; each refusal typed and counted, and the connections
    /// held serve on. Room made, the identity at its share is admitted.
    #[test]
    fn a_full_listener_grows_for_no_one_and_admits_again_once_there_is_room() {
        focal_rt::block_on(async move {
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
            // Three connections in all.
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
            let (first_a_process, first_a) =
                dial(&pki, &first_certificate, &first_key, address).await;
            let (first_b_process, first_b) =
                dial(&pki, &first_certificate, &first_key, address).await;
            let (second_a_process, second_a) =
                dial(&pki, &second_certificate, &second_key, address).await;
            let (first_a, first_b, second_a) =
                (first_a.unwrap(), first_b.unwrap(), second_a.unwrap());
            assert!(
                serves(&first_a, 1).await
                    && serves(&first_b, 2).await
                    && serves(&second_a, 3).await
            );
            assert_eq!(held(&server, 3).await.connections, 3);
            quiet().await;
            // The second identity, at its share of the full listener, would be one more.
            let (refused_second, again) =
                dial(&pki, &second_certificate, &second_key, address).await;
            assert!(again.is_err());
            // The first, over its share and under its own bound, would be one more too.
            let (refused_first, again) = dial(&pki, &first_certificate, &first_key, address).await;
            assert!(again.is_err());
            let stats = held(&server, 3).await;
            assert_eq!(
                (stats.refused_connections, stats.replaced),
                (2, 0),
                "{stats:?}"
            );
            assert!(
                serves(&first_a, 5).await
                    && serves(&first_b, 6).await
                    && serves(&second_a, 7).await
            );
            // A connection that closes makes room: the second identity is admitted.
            first_b.close().unwrap();
            held(&server, 2).await;
            let (second_b_process, second_b) =
                dial(&pki, &second_certificate, &second_key, address).await;
            assert!(serves(&second_b.unwrap(), 8).await);
            assert_eq!(held(&server, 3).await.admitted, 4);
            drop((
                first_a_process,
                first_b_process,
                second_a_process,
                refused_second,
                refused_first,
                second_b_process,
            ));
            server.close().unwrap();
            task.join().await.unwrap();
        })
        .unwrap()
    }

    /// The audit's F03: a body is held to its sender's share of the listener's budget before any
    /// of it is reserved. A request announcing the largest frame is reserved its body while
    /// nothing of it arrives; a second such request from the same identity is refused for its
    /// share, typed and counted, and its sender hears the peer's capacity (the connection carries
    /// on); the first one's reservation goes back as its exchange ends.
    #[test]
    fn a_body_is_held_to_its_identitys_share_before_it_is_reserved() {
        focal_rt::block_on(async move {
            let pki = Pki::new();
            let (certificate, key) = pki.issue(false);
            let registry = PeerRegistry::new(16).unwrap();
            registry
                .register_certificate(&certificate, grant())
                .unwrap();
            let frame = limits().max_frame_bytes as usize;
            let (tls_certificate, tls_key) = pki.issue(true);
            let tls = server_tls(
                TlsIdentity::from_pkcs8(vec![tls_certificate], tls_key),
                vec![pki.ca.der().to_vec()],
                &limits(),
            )
            .unwrap();
            // The ordinary lane holds the endpoint's socket and a frame and a half beside it: one
            // peer's share is all of that (a frame at least), so the first body's reservation fits
            // it and a second's would not.
            let budget = MemoryBudget::new(endpoint_socket_bytes() + frame + frame / 2, 0).unwrap();
            let server = Arc::new(
                QuicServer::bind_admitting(
                    "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
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
            let task = spawn(async move { running.serve(handler).await });
            let mut raw = RawPeer::connect(
                client_identity(&pki, certificate, key),
                server.local_addr().unwrap(),
            )
            .await;
            let idle = admission_settles(&server, |stats| stats.connections == 1).await;
            // A request announcing a frame, none of it sent.
            let first = raw.ask(&request(1), frame);
            let stats = admission_settles(&server, |stats| {
                held_by_connections(stats) == held_by_connections(&idle) + frame
            })
            .await;
            assert_eq!(stats.refused_bytes, 0);
            // A second announcing a frame would take more than the identity's share: refused
            // before any reservation, and its sender told it was the peer's capacity.
            let second = raw.ask(&request(2), frame);
            assert_eq!(
                unfrozen("the second body was never refused", raw.answer(second)).await,
                Err(hyper_transport::Refusal::Budget)
            );
            let stats = admission_settles(&server, |stats| stats.refused_bytes == 1).await;
            assert_eq!(
                held_by_connections(&stats),
                held_by_connections(&idle) + frame,
                "{stats:?}"
            );
            // The connection carries on: a request whole is answered on it.
            assert_eq!(
                raw.request(&request(3)).await.unwrap(),
                response(&request(3))
            );
            // The first ends without its body: its reservation goes back, with the connection's.
            raw.disconnect();
            let _ = first;
            let stats = admission_settles(&server, |stats| {
                held_by_connections(stats) == 0 && stats.connections == 0
            })
            .await;
            assert_eq!(stats.refused_bytes, 1);
            drop(raw);
            server.close().unwrap();
            task.join().await.unwrap();
        })
        .unwrap()
    }

    /// An identity at its bound loses the idle connection it used least, never the one it uses;
    /// and a participant that runs one short-lived client after another, each leaving its
    /// connection behind, is served by each in turn once the one before has been quiet.
    #[test]
    fn an_identity_past_its_bound_loses_the_connection_it_used_least() {
        focal_rt::block_on(async move {
            let pki = Pki::new();
            let (certificate, key) = pki.issue(false);
            let registry = PeerRegistry::new(16).unwrap();
            registry
                .register_certificate(&certificate, grant())
                .unwrap();
            // A listener of two: the identity's share, alone, is both.
            let (server, task) = admitting(
                &pki,
                registry,
                AdmissionLimits {
                    connections: 2,
                    ..bounds()
                },
            )
            .await;
            let address = server.local_addr().unwrap();
            let (first_process, first) = dial(&pki, &certificate, &key, address).await;
            let (second_process, second) = dial(&pki, &certificate, &key, address).await;
            let (first, second) = (first.unwrap(), second.unwrap());
            held(&server, 2).await;
            // The first is used after the second was opened: the second is now the one used
            // least, and it is the one a third replaces.
            assert!(serves(&first, 500).await);
            quiet().await;
            let (third_process, third) = dial(&pki, &certificate, &key, address).await;
            assert!(serves(&third.unwrap(), 501).await);
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
            // Clients that leave their connections behind, one after another.
            let mut left = Vec::new();
            for round in 0..8u128 {
                quiet().await;
                let (process, client) = dial(&pki, &certificate, &key, address).await;
                assert!(serves(&client.unwrap(), 510 + round).await, "round {round}");
                // Left up and unused, as a process that exited without closing leaves it until
                // the listener replaces it or it idles out.
                left.push(process);
            }
            let stats = held(&server, 2).await;
            assert_eq!(stats.identities, 1);
            assert!(stats.replaced >= 8, "{stats:?}");
            drop((first_process, second_process, third_process, left));
            server.close().unwrap();
            task.join().await.unwrap();
        })
        .unwrap()
    }

    /// Identities are bounded, and one identity never takes another's place: alone, the first
    /// takes the whole listener and replaces its own; a second, under its share of the full
    /// listener, takes an idle connection of the first, which is over it; a third finds no place
    /// and takes none; one that leaves frees its place.
    #[test]
    fn identities_are_bounded_and_one_identity_cannot_take_anothers_place() {
        focal_rt::block_on(async move {
            let pki = Pki::new();
            let registry = PeerRegistry::new(16).unwrap();
            let mut identities = Vec::new();
            for principal in 1..=3u128 {
                let (certificate, key) = pki.issue(false);
                let mut grant = grant();
                grant.principal = ParticipantId::from_u128(principal);
                registry.register_certificate(&certificate, grant).unwrap();
                identities.push((certificate, key));
            }
            let (server, task) = admitting(
                &pki,
                registry,
                AdmissionLimits {
                    identities: 2,
                    connections: 4,
                    ..bounds()
                },
            )
            .await;
            let address = server.local_addr().unwrap();
            let (first, first_key) = &identities[0];
            let mut firsts = Vec::new();
            for _ in 0..4 {
                let (process, remote) = dial(&pki, first, first_key, address).await;
                firsts.push((process, remote.unwrap()));
            }
            held(&server, 4).await;
            // Alone, the first identity's share is the whole listener: past it, it displaces
            // its own idle connection used least.
            quiet().await;
            let (process, remote) = dial(&pki, first, first_key, address).await;
            firsts.push((process, remote.unwrap()));
            let stats = held(&server, 4).await;
            assert_eq!((stats.identities, stats.replaced), (1, 1));
            // A second identity finds the listener full and is under its share: it takes the
            // idle connection the first, over its share, used least.
            quiet().await;
            let (second, second_key) = &identities[1];
            let (c_process, c) = dial(&pki, second, second_key, address).await;
            let c = c.unwrap();
            let stats = held(&server, 4).await;
            assert_eq!((stats.identities, stats.replaced), (2, 2));
            assert!(
                !serves(&firsts[0].1, 598).await,
                "replaced by its own identity"
            );
            assert!(
                !serves(&firsts[1].1, 599).await,
                "given to the second identity"
            );
            // A third identity finds no place, and takes none from the others.
            let (third, third_key) = &identities[2];
            let (refused_process, refused) = dial(&pki, third, third_key, address).await;
            assert!(refused.is_err());
            let stats = held(&server, 4).await;
            assert_eq!(stats.refused_identities, 1);
            assert!(serves(&firsts[4].1, 600).await && serves(&c, 602).await);
            // An identity that leaves frees its place.
            c.close().unwrap();
            held(&server, 3).await;
            let (d_process, d) = dial(&pki, third, third_key, address).await;
            assert!(serves(&d.unwrap(), 603).await);
            drop((firsts, c_process, refused_process, d_process));
            server.close().unwrap();
            task.join().await.unwrap();
        })
        .unwrap()
    }

    #[test]
    fn a_certificate_that_does_not_authenticate_is_charged_to_no_identity() {
        focal_rt::block_on(async move {
            let pki = Pki::new();
            let (certificate, key) = pki.issue(false);
            // Signed by the authority, registered to nobody.
            let registry = PeerRegistry::new(16).unwrap();
            let (server, task) = admitting(&pki, registry, bounds()).await;
            let address = server.local_addr().unwrap();
            for _ in 0..16 {
                let (_process, dialed) = dial(&pki, &certificate, &key, address).await;
                assert!(
                    matches!(dialed, Err(WireError::Authentication)),
                    "{:?}",
                    dialed.err()
                );
            }
            let stats = held(&server, 0).await;
            assert_eq!((stats.identities, stats.admitted), (0, 0));
            server.close().unwrap();
            task.join().await.unwrap();
        })
        .unwrap()
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
#[test]
fn cold_calls_to_one_route_share_one_dial_and_a_stale_failure_forgets_nothing() {
    focal_rt::block_on(async move {
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
                    pause(Duration::from_millis(20)).await;
                    response(verified.request())
                }
            })
        };
        let (server, task) = server(&pki, registry, handler).await;
        let connector = connector(&pki, certificate, key);
        let address = server.local_addr().unwrap().to_string();
        let routes = Arc::new(RouteConnections::new(connector, 1).unwrap());
        let waves: Vec<_> = (1..=24u128)
            .map(|id| {
                let routes = routes.clone();
                let address = address.clone();
                spawn(async move {
                    let connected = routes.connect(&address, "localhost").await?;
                    connected
                        .remote
                        .request(&request(id))
                        .await
                        .map(|reply| (id, reply))
                })
            })
            .collect();
        let mut answered = 0;
        for joined in waves {
            let (id, reply) = joined.await.unwrap().unwrap();
            assert_eq!(reply, response(&request(id)));
            answered += 1;
        }
        assert_eq!(answered, 24);
        assert_eq!(calls.load(Ordering::SeqCst), 24);
        assert_eq!(routes.dials().await, 1, "one dial for every cold call");
        let stats = server.admission().await;
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
        assert_eq!(routes.dials().await, 1);
        routes.forget(&address, "localhost", current.generation);
        let fresh = routes.connect(&address, "localhost").await.unwrap();
        assert_ne!(fresh.generation, current.generation);
        assert_eq!(routes.dials().await, 2);
        // A caller that gives up under its own deadline does not abandon the
        // dial: the next caller finds it, and no third dial is started.
        routes.forget(&address, "localhost", fresh.generation);
        let gave_up = timeout(
            Duration::from_micros(10),
            routes.connect(&address, "localhost"),
        )
        .await;
        let next = routes.connect(&address, "localhost").await.unwrap();
        assert_eq!(routes.dials().await, 3, "gave up: {}", gave_up.is_some());
        assert!(next.generation > fresh.generation);
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A request that fails alone keeps its connection: one that timed out on a
/// connection answering the caller's other requests is not the connection's
/// failure, so the route is not dialed again (a caller that dialed again
/// while its other requests held the old connection took one connection of
/// its identity more than the listener had room for). A connection that has
/// closed is dialed anew by the next request, which it does not fail.
#[test]
fn a_request_that_fails_alone_keeps_its_connection() {
    focal_rt::block_on(async move {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        // Request 2 is answered after the client's deadline; the rest at once.
        let handler: Arc<dyn RequestHandler> =
            Arc::new(move |verified: VerifiedRequest| async move {
                if verified.request().request_id == RequestId::from_u128(2) {
                    pause(Duration::from_millis(1500)).await;
                }
                response(verified.request())
            });
        let (server, task) = server(&pki, registry, handler).await;
        let quick = WireLimits {
            request_timeout: Duration::from_millis(500),
            ..Default::default()
        };
        let tls = client_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &quick,
            Sender::Participant,
        )
        .unwrap();
        let connector =
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, quick, budget()).unwrap();
        let address = server.local_addr().unwrap().to_string();
        let routes = RouteConnections::new(connector, 4).unwrap();
        assert!(
            routes
                .request(&address, "localhost", &request(1))
                .await
                .is_ok()
        );
        assert!(matches!(
            routes.request(&address, "localhost", &request(2)).await,
            Err(WireError::Timeout)
        ));
        assert!(
            routes
                .request(&address, "localhost", &request(3))
                .await
                .is_ok()
        );
        assert_eq!(
            routes.dials().await,
            1,
            "a request that failed alone forgot its connection"
        );
        assert_eq!(server.admission().await.admitted, 1);
        // The connection closes: the next request dials anew and is answered on it.
        routes
            .connect(&address, "localhost")
            .await
            .unwrap()
            .remote
            .close()
            .unwrap();
        assert!(
            routes
                .request(&address, "localhost", &request(4))
                .await
                .is_ok()
        );
        assert!(
            routes
                .request(&address, "localhost", &request(5))
                .await
                .is_ok()
        );
        assert_eq!(routes.dials().await, 2);
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
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
    /// The other exchanges answered while it waited.
    answered: u64,
}
const UNREAD_PERIOD: Duration = Duration::from_millis(500);
const UNREAD_OTHER: usize = 4 * 1024;
const UNREAD_EVERY: Duration = Duration::from_millis(100);
/// Ask a peer that answers what is small and never reads what is not, over
/// `shape` or directly, for `stalled`, while four kilobytes are sent to it
/// ten times a second; then once more for something small on the same
/// connection.
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
    let registry = PeerRegistry::new(4).unwrap();
    let mut node_grant = grant();
    node_grant.role = PeerRole::Node { node_id: 7 };
    registry
        .register_certificate(&certificate, node_grant)
        .unwrap();
    let (server_certificate, server_key) = pki.issue(true);
    let config = server_tls(
        TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
        vec![pki.ca.der().to_vec()],
        &wire,
    )
    .unwrap();
    let server = RawServer::listen(config, registry, wire.clone());
    let mut address = server.local_addr();
    // What is larger than a megabyte is never read.
    let peer = spawn(server.serve(1024 * 1024));
    let mut relays = Vec::new();
    if let Some(shape) = shape {
        (address, relays) = shaped(address, shape).await;
    }
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &wire,
        Sender::Node,
    )
    .unwrap();
    let connector =
        QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire, budget()).unwrap();
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
    // The first of the others that was not answered, and how.
    let refused = Arc::new(std::sync::Mutex::new(None::<WireError>));
    let others = {
        let (remote, answered, refused) = (remote.clone(), answered.clone(), refused.clone());
        spawn(async move {
            // Until the test aborts it: a bound the test never reaches.
            for id in 1_000..=u128::MAX {
                match remote.request_within(&other(id), UNREAD_PERIOD).await {
                    Ok(_) => answered.fetch_add(1, Ordering::Relaxed),
                    Err(error) => {
                        *refused.lock().unwrap() = Some(error);
                        return;
                    }
                };
                pause(UNREAD_EVERY).await;
            }
        })
    };
    let began = hyper_rt::futures::now_ns();
    let stalled = remote.request_within(&stalled, UNREAD_PERIOD).await;
    let after = Duration::from_nanos(hyper_rt::futures::now_ns() - began);
    let during = answered.load(Ordering::Relaxed);
    // The others are answered after it as before, on the same connection.
    remote
        .request_within(&other(2), UNREAD_PERIOD)
        .await
        .unwrap();
    others.abort();
    let _ = others.await;
    // None of the others was refused, while it waited or after.
    assert!(
        refused.lock().unwrap().is_none(),
        "{:?}",
        refused.lock().unwrap()
    );
    remote.close().unwrap();
    peer.abort();
    for relay in relays {
        relay.abort();
    }
    Unread {
        stalled,
        after,
        answered: during,
    }
}

/// A stream its peer does not read ends by what its own bytes move, whatever
/// else the connection carries meanwhile (the audit's F38): charged with what
/// the connection sent, it was kept for as long as the other exchanges moved a
/// datagram a period, until they had sent as much as it had to — two megabytes
/// at twenty kilobytes a period here, a hundred periods. The endpoint ends an
/// exchange out of the peer's credit with all it sent acknowledged a window
/// after it finds it so (hyper-transport's `progress.rs`, hyper-quic patch Q8).
/// The others are never refused for it: here the unread stream holds the whole
/// of the connection's credit — the budget's window, a dozen kilobytes to
/// start, where the old transport's was 144 MiB (excluded by note 32, T46–T48)
/// — and the others wait for it, held and not judged, and are answered once its
/// reset gives the credit back.
#[test]
fn a_stream_its_peer_does_not_read_ends_whatever_else_its_connection_carries() {
    focal_rt::block_on(async move {
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
        // What the other exchanges move in a period, and the periods they would have kept it.
        let moved =
            UNREAD_OTHER as u32 * (UNREAD_PERIOD.as_millis() / UNREAD_EVERY.as_millis()) as u32;
        let kept = UNREAD_PERIOD * (STALLED as u32 / moved);
        assert!(kept >= UNREAD_PERIOD * 100);
        // Ended within a tenth of that: the periods its own window's bytes took, and the one
        // that moved nothing.
        assert!(unread.after < kept / 10, "{:?} of {kept:?}", unread.after);
        println!(
            "a stalled stream of {STALLED} bytes ended after {:?}, {} others answered meanwhile; the old wait kept it {kept:?}",
            unread.after, unread.answered
        );
    })
    .unwrap()
}

/// The same over a path that loses a datagram in a hundred and delivers
/// out of order, the stalled exchange content and the others a group's
/// messages, which go before it: it ends when its own bytes stop moving, and
/// the others are answered throughout.
#[test]
fn a_stream_its_peer_does_not_read_ends_on_a_path_that_loses_and_reorders() {
    focal_rt::block_on(async move {
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
        // The path carries its window in a fraction of a period: the periods that moved it,
        // and the one that moved nothing, are a handful.
        let moved =
            UNREAD_OTHER as u32 * (UNREAD_PERIOD.as_millis() / UNREAD_EVERY.as_millis()) as u32;
        let kept = UNREAD_PERIOD * (STALLED as u32 / moved);
        assert!(unread.after < kept / 10, "{:?} of {kept:?}", unread.after);
        assert!(unread.answered >= 1, "{}", unread.answered);
    })
    .unwrap()
}

/// The time a peer is given to answer begins when it has what was asked:
/// when the request's stream is acknowledged whole (the audit's F38).
/// Thirty-two kilobytes over a path that carries eight in a second take
/// four seconds, and the peer answers four tenths of a second after it has
/// them, inside its half-second. Counted from when the connection had
/// *sent* as much as the request, the half-second began while the last
/// window of it, more than a second of this path, was still on its way,
/// and ended before the peer had the request at all.
#[test]
fn a_peers_time_to_answer_begins_when_it_has_what_was_asked() {
    focal_rt::block_on(async move {
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
        let handler: Arc<dyn RequestHandler> =
            Arc::new(move |verified: VerifiedRequest| async move {
                pause(PERIOD * 4 / 5).await;
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
                "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
                tls,
                registry,
                wire.clone(),
                budget(),
            )
            .unwrap(),
        );
        let running = server.clone();
        let serving = spawn(async move { running.serve(handler).await });
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
            Sender::Node,
        )
        .unwrap();
        let connector =
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire, budget()).unwrap();
        let remote = connector.connect(path, "localhost").await.unwrap();
        let mut packet = request(1);
        packet.operation = Operation::Raft {
            group: [2; 16],
            message: vec![7; SIZE],
        };
        let began = hyper_rt::futures::now_ns();
        let answered = remote.request_within(&packet, PERIOD).await;
        let took = Duration::from_nanos(hyper_rt::futures::now_ns() - began);
        assert!(
            matches!(&answered, Ok(answer) if answer.result == Response::PeerAccepted),
            "{answered:?} in {took:?}"
        );
        // No sooner than the path carries it, which is many of its periods.
        let least = Duration::from_millis(SIZE as u64 * 8 * 1_000 / BITS);
        assert!(least >= PERIOD * 8);
        assert!(took >= least, "{took:?}");
        server.close().unwrap();
        for relay in relays {
            relay.abort();
        }
        let _ = serving.await;
    })
    .unwrap()
}

/// A pool, as a node's limits are but for `pool`, and a peer that accepts
/// what it is sent, with a path shaped as `shape` between them.
struct SlowRig {
    pool: PeerConnectionPool,
    server: Arc<QuicServer>,
    serving: Spawned<Result<(), WireError>>,
    relays: Vec<Spawned<()>>,
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
                "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
                tls,
                registry,
                wire.clone(),
                budget(),
            )
            .unwrap(),
        );
        let running = server.clone();
        let serving = spawn(async move { running.serve(handler).await });
        let (path, relays) = shaped(server.local_addr().unwrap(), shape).await;
        let tls = client_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &wire,
            Sender::Node,
        )
        .unwrap();
        let pool = PeerConnectionPool::new(
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire, budget()).unwrap(),
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
        self.server.close().unwrap();
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
#[test]
fn a_groups_message_is_carried_for_as_long_as_its_path_takes() {
    focal_rt::block_on(async move {
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
        let began = hyper_rt::futures::now_ns();
        assert_eq!(rig.pool.send(2, &SlowRig::message(1, SIZE)).await, Ok(()));
        let took = Duration::from_nanos(hyper_rt::futures::now_ns() - began);
        assert!(took >= least, "{took:?}");
        let stats = rig.pool.stats();
        assert_eq!((stats.dials, stats.connections_opened), (1, 1));
        rig.close().await;
    })
    .unwrap()
}

/// A dial is given what a handshake is given and not what one exchange is
/// (the audit's F36): over a path that takes longer to connect than the
/// pool's callers wait, the callers are told the peer was not reached, the
/// dial goes on, and the next caller has its connection. The dial was
/// given the callers' time, failed with them, and was begun again from
/// nothing by the next: such a path was never connected.
#[test]
fn a_dial_is_given_a_handshakes_time_and_outlives_the_callers_that_asked_for_it() {
    focal_rt::block_on(async move {
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
        let began = hyper_rt::futures::now_ns();
        let mut lost = 0_u32;
        // Charged to the dial: one more try for every caller's time it takes,
        // and as many as a handshake is given at most.
        let tries = (WireLimits::default().request_timeout.as_millis() * 2
            / limits.timeout.as_millis()) as u32;
        loop {
            match rig.pool.send(2, &SlowRig::message(1, 64)).await {
                Ok(()) => break,
                Err(PeerSendError::Lost) => lost += 1,
                Err(other) => panic!("{other:?}"),
            }
            assert!(
                lost < tries,
                "{lost} tries in {:?}",
                Duration::from_nanos(hyper_rt::futures::now_ns() - began)
            );
        }
        assert!(lost >= 1, "connected within a caller's time");
        assert!(Duration::from_nanos(hyper_rt::futures::now_ns() - began) >= limits.timeout * 2);
        let stats = rig.pool.stats();
        assert_eq!((stats.dials, stats.connections_opened), (1, 1));
        rig.close().await;
    })
    .unwrap()
}

/// What the pool does with a group's message of `size` over a path shaped
/// as `shape`, as a node's limits are: the outcome of sending it, and of
/// sending it again, each given `cap` at most, and what the pool counted.
type SlowCase = (
    Option<Result<(), PeerSendError>>,
    Duration,
    Option<Result<(), PeerSendError>>,
    Duration,
    PeerPoolStats,
);
async fn slow_case(shape: Shape, size: usize, cap: Duration) -> SlowCase {
    let rig = SlowRig::new(shape, PeerPoolLimits::for_consensus(128)).await;
    let packet = SlowRig::message(u128::from(shape.up_bits) * 1_000_000 + size as u128, size);
    let began = hyper_rt::futures::now_ns();
    let first = timeout(cap, rig.pool.send(2, &packet)).await;
    let took = Duration::from_nanos(hyper_rt::futures::now_ns() - began);
    // A second message, on the connection the first opened, if it did.
    let again = hyper_rt::futures::now_ns();
    let second = timeout(cap, rig.pool.send(2, &packet)).await;
    let then = Duration::from_nanos(hyper_rt::futures::now_ns() - again);
    let stats = rig.pool.stats();
    rig.close().await;
    (first, took, second, then, stats)
}
fn slow_row(label: &str, case: &SlowCase) {
    let say = |outcome: &Option<Result<(), PeerSendError>>| match outcome {
        Some(Ok(())) => "delivered".to_string(),
        Some(Err(error)) => format!("{error:?}"),
        None => "capped".to_string(),
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
#[test]
#[ignore = "a measurement of minutes; run by name"]
fn slow_paths_measured() {
    focal_rt::block_on(async move {
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
                    spawn(slow_case(shape, size, cap)),
                ));
            }
        }
        println!("{SLOW_HEAD}");
        for (label, case) in cases {
            slow_row(&label, &case.await.unwrap());
        }
    })
    .unwrap()
}

/// The same over paths that lose, delay unevenly, carry less one way, and
/// stop for a while: a measurement, printed, which asserts nothing.
#[test]
#[ignore = "a measurement of minutes; run by name"]
fn adverse_paths_measured() {
    focal_rt::block_on(async move {
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
                    spawn(slow_case(shape, size, cap)),
                ));
            }
        }
        println!("{SLOW_HEAD}");
        for (label, case) in cases {
            slow_row(&label, &case.await.unwrap());
        }
    })
    .unwrap()
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
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
            tls,
            registry,
            wire.clone(),
            budget(),
        )
        .unwrap(),
    );
    let running = server.clone();
    let serving = spawn(async move { running.serve(handler).await });
    let (path, relays) = shaped(server.local_addr().unwrap(), shape).await;
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.ca.der().to_vec()],
        &wire,
        Sender::Node,
    )
    .unwrap();
    let connector =
        QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, wire, budget()).unwrap();
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
            let sent = hyper_rt::futures::now_ns();
            let outcome = remote.request_within(&packet, period).await;
            (
                outcome.map(|_| ()),
                Duration::from_nanos(hyper_rt::futures::now_ns() - sent),
            )
        }
    };
    server.close().unwrap();
    for relay in relays {
        relay.abort();
    }
    let _ = serving.await;
    outcome
}

/// Exchanges each way over the narrowest paths that connect, with and
/// without loss, each part given five seconds: a measurement, printed,
/// which asserts nothing.
#[test]
#[ignore = "a measurement of minutes; run by name"]
fn narrow_lossy_paths_measured() {
    focal_rt::block_on(async move {
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
                            spawn(direct_case(shape, size, down, Duration::from_secs(5))),
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
    })
    .unwrap()
}

/// One exchange over one shaped path, told as it goes: a diagnostic.
/// `FOCAL_SLOW_PATH_BITS`, `_BYTES`, `_PERIOD_MS`, `_LOSS_PPM`, and `_DOWN`
/// for bytes asked of the peer instead of sent to it.
#[test]
#[ignore = "a diagnostic; run by name"]
fn slow_path_one() {
    focal_rt::block_on(async move {
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
        let began = hyper_rt::futures::now_ns();
        let (outcome, took) = direct_case(shape, size, down, period).await;
        println!(
            "{bits} bit/s, {size} bytes {}, period {period:?}: {outcome:?} in {took:?} ({:?} in all)",
            if down { "asked" } else { "sent" },
            Duration::from_nanos(hyper_rt::futures::now_ns() - began)
        );
    }).unwrap()
}

/// A peer whose issuer succeeded one the verifier knows is admitted through
/// the predecessor's endorsement (24 §11): a CA certificate for the
/// successor's key under the predecessor's signature, presented beside the
/// successor's own — an ordinary intermediate to path building. Without
/// it, or endorsed by a stranger, the chain is refused — in both
/// directions.
#[test]
fn a_peer_whose_issuer_the_other_does_not_know_is_admitted_by_the_predecessors_endorsement() {
    focal_rt::block_on(async move {
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
                "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
                tls,
                registry,
                limits(),
                budget(),
            )
            .unwrap(),
        );
        let running = server.clone();
        let task = spawn(async move { running.serve(handler).await });
        let address = server.local_addr().unwrap();
        let dial = |chain: Vec<Vec<u8>>| {
            QuicConnector::bind(
                "127.0.0.1:0".parse().unwrap(),
                client_tls(
                    TlsIdentity::from_pkcs8(chain, client_key.clone()),
                    roots.clone(),
                    &limits(),
                    Sender::Participant,
                )
                .unwrap(),
                limits(),
                budget(),
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
        server.close().unwrap();
        task.join().await.unwrap();
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
                    "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
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
                Sender::Participant,
            )
            .unwrap(),
            limits(),
            budget(),
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
            let task = spawn(async move { running.serve(handler).await });
            let outcome = connector
                .connect(server.local_addr().unwrap(), "localhost")
                .await;
            assert_eq!(outcome.is_ok(), admitted, "{:?}", outcome.as_ref().err());
            if let Ok(remote) = outcome {
                let reply = remote.request(&request(2)).await.unwrap();
                assert_eq!(reply.request_id, request(2).request_id);
            }
            server.close().unwrap();
            task.join().await.unwrap();
        }
    })
    .unwrap()
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
        roots,
        crate::crypto_provider(),
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
#[test]
fn the_ordered_profile_tops_the_ladder_and_an_older_offer_never_reaches_it() {
    focal_rt::block_on(async move {
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
        // The waiting profile tops the ladder where both sides wait for an inbox, and implies
        // the laned one; a handler that does not wait stays laned.
        let waiting = limits.negotiate_laned(&all, [true; 6]).unwrap();
        assert_eq!(waiting.protocol, WAITING_PROTOCOL_VERSION);
        assert!(waiting.laned());
        assert!(
            [1, 2, 3, 4, 5, 7]
                .into_iter()
                .all(|requested| waiting.accepts_protocol(requested))
        );
        // The laned profile is next where both sides take lanes, and implies the ordered one; a
        // handler that takes no lanes, or an offer without it, stays ordered.
        let laned = limits
            .negotiate_laned(&all, [true, true, true, true, true, false])
            .unwrap();
        assert_eq!(laned.protocol, LANED_PROTOCOL_VERSION);
        assert!(laned.laned());
        assert!(!laned.accepts_protocol(WAITING_PROTOCOL_VERSION));
        assert_eq!(
            limits
                .negotiate_laned(&all, [true, true, true, true, false, false])
                .unwrap()
                .protocol,
            ORDERED_PROTOCOL_VERSION
        );
        // An offer of an older binary, without the waiting profile, stays laned.
        let unwaiting = hello(&OFFERED_PROTOCOLS[1..]);
        assert_eq!(
            limits
                .negotiate_laned(&unwaiting, [true; 6])
                .unwrap()
                .protocol,
            LANED_PROTOCOL_VERSION
        );
        // A local connection, which carries no lanes, is offered the waiting profile by a
        // handler that waits, and the native one by one that does not.
        let local = hello(&[WAITING_PROTOCOL_VERSION]);
        assert_eq!(
            limits.negotiate_local(&local, [true; 4]).unwrap().protocol,
            WAITING_PROTOCOL_VERSION
        );
        assert!(
            limits
                .negotiate_local(&local, [true, true, true, false])
                .is_err()
        );
        assert_eq!(
            limits
                .negotiate_local(&hello(&[NATIVE_PROTOCOL_VERSION]), [true; 4])
                .unwrap()
                .protocol,
            NATIVE_PROTOCOL_VERSION
        );
        let unlaned = hello(&[
            ORDERED_PROTOCOL_VERSION,
            NATIVE_PROTOCOL_VERSION,
            PEER_PROTOCOL_VERSION,
            MANAGED_PROTOCOL_VERSION,
            PROTOCOL_VERSION,
        ]);
        assert_eq!(
            limits
                .negotiate_laned(&unlaned, [true; 6])
                .unwrap()
                .protocol,
            ORDERED_PROTOCOL_VERSION
        );
        assert!(!limits.negotiate_laned(&unlaned, [true; 6]).unwrap().laned());
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
        // A request's protocol is at most the ordered profile's: the laned profile is a
        // connection's, and no request names it. A waiting read's, the waiting profile's, rides
        // only a connection that negotiated it.
        for requested in OFFERED_PROTOCOLS.into_iter().filter(|requested| {
            ![LANED_PROTOCOL_VERSION, WAITING_PROTOCOL_VERSION].contains(requested)
        }) {
            assert!(ordered.accepts_protocol(requested), "{requested}");
            assert!(laned.accepts_protocol(requested), "{requested}");
        }
        assert!(!ordered.accepts_protocol(LANED_PROTOCOL_VERSION));
        assert!(!laned.accepts_protocol(LANED_PROTOCOL_VERSION));
        assert!(!laned.accepts_protocol(7));
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
                    Sender::Participant,
                )
                .unwrap(),
                limits.clone(),
                budget(),
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
        assert!(
            connector()
                .offering(&[PROTOCOL_VERSION, LANED_PROTOCOL_VERSION])
                .is_ok()
        );
        assert!(
            connector()
                .offering(&[PROTOCOL_VERSION, WAITING_PROTOCOL_VERSION])
                .is_ok()
        );
        assert!(connector().offering(&[PROTOCOL_VERSION, 8]).is_err());
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
    })
    .unwrap()
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
#[test]
fn a_peer_offering_only_a_classical_key_exchange_is_refused_both_ways() {
    focal_rt::block_on(async move {
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
        // A test's own provider, for the life of the process: the configurations take it
        // `&'static`.
        let provider = |groups: Vec<&'static dyn rustls::crypto::SupportedKxGroup>,
                        suites: Option<Vec<rustls::SupportedCipherSuite>>|
         -> &'static rustls::crypto::CryptoProvider {
            let mut provider = rustls::crypto::aws_lc_rs::default_provider();
            provider.kx_groups = groups;
            if let Some(suites) = suites {
                provider.cipher_suites = suites;
            }
            Box::leak(Box::new(provider))
        };
        let client = |groups: Vec<&'static dyn rustls::crypto::SupportedKxGroup>,
                      suites: Option<Vec<rustls::SupportedCipherSuite>>| {
            let mut roots = rustls::RootCertStore::empty();
            roots
                .add(CertificateDer::from(pki.ca.der().to_vec()))
                .unwrap();
            let mut tls = rustls::ClientConfig::builder_with_provider(provider(groups, suites))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_root_certificates(roots)
                .with_client_auth_cert(
                    vec![CertificateDer::from(certificate.clone())],
                    PrivatePkcs8KeyDer::from(key.clone()).into(),
                )
                .unwrap();
            tls.alpn_protocols = vec![ALPN.to_vec()];
            QuicConnector::bind(
                "127.0.0.1:0".parse().unwrap(),
                ClientTls::from_config(tls, Sender::Participant),
                limits(),
                budget(),
            )
            .unwrap()
        };
        let address = server.local_addr().unwrap();
        let refused = client(vec![X25519], None)
            .connect(address, "localhost")
            .await;
        assert!(refused.is_err(), "a classical exchange was accepted");
        let short_key = client(vec![X25519MLKEM768], Some(vec![TLS13_AES_128_GCM_SHA256]))
            .connect(address, "localhost")
            .await;
        assert!(short_key.is_err(), "a 128-bit traffic key was accepted");
        let served = client(vec![X25519MLKEM768, X25519], None)
            .connect(address, "localhost")
            .await;
        assert!(served.is_ok(), "{:?}", served.err());
        // A server that speaks only a classical exchange, to a focal client.
        let (server_certificate, server_key) = pki.issue(true);
        let mut tls = rustls::ServerConfig::builder_with_provider(provider(vec![X25519], None))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(server_certificate)],
                PrivatePkcs8KeyDer::from(server_key).into(),
            )
            .unwrap();
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let classical = RawServer::listen(
            ServerTls::from_config(tls),
            PeerRegistry::new(1).unwrap(),
            limits(),
        );
        let classical_address = classical.local_addr();
        let accepting = spawn(classical.serve(0));
        let to_classical = connector(&pki, certificate.clone(), key.clone())
            .connect(classical_address, "localhost")
            .await;
        assert!(
            to_classical.is_err(),
            "a focal client accepted a classical exchange"
        );
        accepting.abort();
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// A peer whose process stalls for longer than a request's deadline keeps its
/// connections: the request that waited ends unanswered, and the next one on
/// the same connection is served once the stall is over. The stall is the
/// server's whole runtime blocked, endpoint and all, as a starved scheduler
/// blocks it; the connection's liveness is its own idle bound, never the
/// request timeout (a busy machine lost every connection it held to a
/// half-second stall when the two were one, 2026-10-07).
///
/// The transport gives a silent peer up within one or two of its windows,
/// each the deadline and the path's probe timeout (hyper-transport's
/// `Progress`): a datagram the peer sent just before it stalled counts as
/// heard in the first. So the stall outlasts two windows of the probe timeout
/// measured before it, and a deadline more. Three deadlines did not on
/// Windows CI, where the probe timeout is near 175 ms: the request was
/// answered at 903 ms, its second window ending at 962 (2026-10-11).
#[test]
fn a_stall_past_the_request_deadline_keeps_the_connection() {
    focal_rt::block_on(async move {
        const DEADLINE: Duration = Duration::from_millis(300);
        let short = WireLimits {
            request_timeout: DEADLINE,
            ..Default::default()
        };
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let (server_certificate, server_key) = pki.issue(true);
        let tls = server_tls(
            TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
            vec![pki.ca.der().to_vec()],
            &short,
        )
        .unwrap();
        let (address_tx, address_rx) = std::sync::mpsc::channel();
        // Told across the threads: the server's shard is a runtime of its own.
        let (stop_tx, stop_rx) = hyper_rt::sync::oneshot::<()>().unwrap();
        let server_limits = short.clone();
        // How long the stall lasts, set once the path is measured, and that it is over.
        let stall = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let over = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (stalling, ended) = (stall.clone(), over.clone());
        let serving = std::thread::spawn(move || {
            let (stall, over) = (stalling, ended);
            focal_rt::block_on(async move {
                let server = Arc::new(
                    QuicServer::bind(
                        "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
                        tls,
                        registry,
                        server_limits,
                        budget(),
                    )
                    .unwrap(),
                );
                address_tx.send(server.local_addr().unwrap()).unwrap();
                let handler: Arc<dyn RequestHandler> =
                    Arc::new(move |verified: VerifiedRequest| {
                        let (stall, over) = (stall.clone(), over.clone());
                        async move {
                            if verified.request().request_id == RequestId::from_u128(1) {
                                // The whole runtime stalls: nothing of this process runs.
                                std::thread::sleep(Duration::from_nanos(
                                    stall.load(Ordering::Acquire),
                                ));
                                over.store(true, Ordering::Release);
                            }
                            response(verified.request())
                        }
                    });
                let running = server.clone();
                let task = spawn(async move { running.serve(handler).await });
                let _ = stop_rx.await;
                server.close().unwrap();
                let _ = task.await;
            })
            .unwrap();
        });
        let address = address_rx.recv().unwrap();
        let tls = client_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.ca.der().to_vec()],
            &short,
            Sender::Participant,
        )
        .unwrap();
        let connector =
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, short, budget()).unwrap();
        let remote = connector.connect(address, "localhost").await.unwrap();
        assert!(remote.request(&request(0)).await.is_ok());
        let window = DEADLINE + remote.path().await.unwrap().probe_timeout;
        let outlasts = window * 2 + DEADLINE;
        stall.store(
            u64::try_from(outlasts.as_nanos()).unwrap(),
            Ordering::Release,
        );
        // The stalled request ends unanswered, not with its connection.
        assert!(remote.request(&request(1)).await.is_err());
        // The fact the next request needs, waited on, the stall's own length its bound.
        let waited = std::time::Instant::now();
        while !over.load(Ordering::Acquire) {
            assert!(waited.elapsed() < outlasts * 4, "the stall never ended");
            pause(DEADLINE / 10).await;
        }
        // A remote is its connection: one that went is not asked again.
        assert!(
            remote.request(&request(2)).await.is_ok(),
            "the same connection serves once the stall is over"
        );
        assert_eq!(
            connector.stats().await.dials,
            1,
            "the connection outlived the stall"
        );
        let _ = stop_tx.send(());
        serving.join().unwrap();
    })
    .unwrap()
}

/// What one small request costs end to end over QUIC on loopback, against a
/// handler that answers at once: the transport's own floor, which every
/// replication hop pays. Run by hand:
/// `cargo test -p focal-wire --release --lib loopback_round_trip -- --ignored --nocapture`.
#[test]
#[ignore]
fn loopback_round_trip_floor() {
    focal_rt::block_on(async move {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let (server, task) = server(&pki, registry, handler).await;
        let connector = connector(&pki, certificate, key);
        let remote = connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await
            .unwrap();
        for id in 0..200u128 {
            remote.request(&request(id)).await.unwrap();
        }
        let mut samples = Vec::with_capacity(2000);
        let mut slow = Vec::new();
        for id in 1000..3000u128 {
            let started = hyper_rt::futures::now_ns();
            remote.request(&request(id)).await.unwrap();
            let took = Duration::from_nanos(hyper_rt::futures::now_ns() - started).as_micros();
            if took > 1000 {
                slow.push((id - 1000, took));
            }
            samples.push(took);
        }
        eprintln!("slow (>1 ms) requests, index and µs: {slow:?}");
        eprintln!(
            "streams per connection: {}",
            limits().streams_per_connection
        );
        samples.sort_unstable();
        let at = |p: f64| samples[((samples.len() - 1) as f64 * p) as usize];
        eprintln!(
            "loopback request/response over QUIC: p50 {} µs, p99 {} µs, p99.9 {} µs, max {} µs",
            at(0.5),
            at(0.99),
            at(0.999),
            samples[samples.len() - 1]
        );
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap()
}

/// The transport parks the inbox read that waits and nothing else (hyper-raft docs/transport.md
/// §4ab): `Operation::NativeWait`'s tag is the one tag that waits, and its kind on the wire is what
/// `FocalClasses::waits` parks; a greeting, and every other operation (a stream poll, whose
/// acknowledgement is work, among them), is served in the pool's account.
#[test]
fn only_the_waiting_inbox_read_is_parked() {
    use hyper_transport::Classes as _;
    let wait = Operation::NativeWait(NativeWaitRequest {
        list: NativeListRequest {
            filter: NativeListFilter::Definitions {
                claim: None,
                evaluator: None,
            },
            cursor: None,
            max_items: 1,
            max_visits: 1,
        },
        wait_ms: 1,
    });
    let tag = wait.registered_tag();
    assert!(crate::net::FocalClasses::waits(
        crate::net::Kind::Operation(tag)
    ));
    let waiting: Vec<u16> = (0..=u16::MAX)
        .filter(|tag| Operation::waits_tag(*tag))
        .collect();
    assert_eq!(waiting, vec![tag]);
    assert!(!crate::net::FocalClasses::waits(crate::net::Kind::Hello));
}

/// The fence query (29 §12.11): a participant's exchange that any authenticated participant may
/// send in the profile every connection accepts, never a node's; answered only by its own kind of
/// response, which carries the level and nothing else.
#[test]
fn the_fence_query_is_a_participants_exchange_answered_by_its_level_alone() {
    let mut query = request(70);
    query.operation = Operation::Fence;
    assert_eq!(query.operation.class(), TrafficClass::Exchange);
    assert_eq!(query.operation.registered_tag(), 35);
    assert_eq!(TrafficClass::of_tag(35), Some(TrafficClass::Exchange));
    assert!(!query.operation.is_mutation());
    assert!(crate::native_profile_operation(&query.operation));
    let actor = AuthenticatedPeer::local(grant()).unwrap();
    assert!(verify_request(actor.clone(), query.clone(), &limits()).is_ok());
    let mut native = query.clone();
    native.protocol = crate::NATIVE_PROTOCOL_VERSION;
    assert!(verify_request(actor, native, &limits()).is_ok());
    let mut value = grant();
    value.role = PeerRole::Node { node_id: 7 };
    assert!(matches!(
        verify_request(
            AuthenticatedPeer::local(value).unwrap(),
            query.clone(),
            &limits()
        ),
        Err(AccessError::Unauthorized)
    ));
    let answer = query.reply(Response::Fence { level: 10 });
    validate_response(&query, &answer, None, &limits()).unwrap();
    let mut summary = request(71);
    summary.operation = Operation::Summary;
    assert!(
        validate_response(
            &summary,
            &summary.reply(Response::Fence { level: 10 }),
            None,
            &limits()
        )
        .is_err()
    );
}
