//! focal-wire over hyper-transport on hyper-rt, end to end: a connector greets a server, asks and is
//! answered; a slow request holds nothing else up; a grant is enforced at dispatch and a revocation
//! closes what it withdrew; a certificate outside the authority never dispatches; a request that
//! arrives before the server is given its handler waits for it; the local socket round-trips.
use super::*;
use focal_memory::MemoryBudget;
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
    fn root(&self) -> Vec<u8> {
        self.ca.der().to_vec()
    }
}
fn limits() -> WireLimits {
    WireLimits {
        request_timeout: Duration::from_secs(3),
        ..Default::default()
    }
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap()
}
fn server(pki: &Pki, registry: PeerRegistry) -> QuicServer {
    let (certificate, key) = pki.issue(true);
    let tls = server_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.root()],
        &limits(),
    )
    .unwrap();
    QuicServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        tls,
        registry,
        limits(),
        budget(),
    )
    .unwrap()
}
fn connector(pki: &Pki, certificate: Vec<u8>, key: Vec<u8>) -> QuicConnector {
    let tls = client_tls(
        TlsIdentity::from_pkcs8(vec![certificate], key),
        vec![pki.root()],
        &limits(),
    )
    .unwrap();
    QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, limits(), budget()).unwrap()
}
fn on_shard<T: 'static>(test: impl std::future::Future<Output = T> + 'static) -> T {
    focal_rt::block_on(test).unwrap()
}

#[test]
fn a_request_is_answered_and_a_slow_one_holds_nothing_else_up() {
    on_shard(async {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        // The slow request waits until the fast one has been answered.
        let fast_done = Arc::new(hyper_rt::sync::Notify::new().unwrap());
        let handler = {
            let calls = calls.clone();
            let fast_done = fast_done.clone();
            move |verified: VerifiedRequest| {
                let calls = calls.clone();
                let fast_done = fast_done.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if verified.request().request_id == RequestId::from_u128(1) {
                        fast_done.notified().await.unwrap();
                    }
                    response(verified.request())
                }
            }
        };
        let server = Arc::new(server(&pki, registry.clone()));
        let serving = server.clone();
        let serve = hyper_rt::futures::spawn(async move {
            serving.serve(handler).await.unwrap();
        })
        .unwrap();
        let connector = connector(&pki, certificate, key);
        let remote = connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await
            .unwrap();
        assert_eq!(remote.negotiated().protocol, PROTOCOL_VERSION);
        let slow_remote = remote.clone();
        let (slow_tx, slow_rx) = hyper_rt::sync::oneshot().unwrap();
        hyper_rt::futures::spawn_detached(async move {
            let _ = slow_tx.send(slow_remote.request(&request(1)).await);
        })
        .unwrap();
        let fast = remote.request(&request(2)).await.unwrap();
        assert_eq!(fast, response(&request(2)));
        fast_done.notify_one();
        let slow = slow_rx.await.unwrap().unwrap();
        assert_eq!(slow, response(&request(1)));
        // Another tenant's ledger is refused by the grant, after dispatch's checks.
        let mut foreign = request(3);
        foreign.ledger.tenant = TenantId::from_u128(999);
        assert_eq!(
            remote.request(&foreign).await.unwrap().result,
            Response::Error(AccessError::Unauthorized)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.close();
        hyper_rt::futures::join(serve).await.unwrap();
    });
}

#[test]
fn a_revocation_refuses_what_follows_and_closes_the_connection() {
    on_shard(async {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let fingerprint = registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = {
            let calls = calls.clone();
            move |verified: VerifiedRequest| {
                calls.fetch_add(1, Ordering::SeqCst);
                let answer = response(verified.request());
                async move { answer }
            }
        };
        let server = Arc::new(server(&pki, registry.clone()));
        let serving = server.clone();
        let serve = hyper_rt::futures::spawn(async move {
            serving.serve(handler).await.unwrap();
        })
        .unwrap();
        let connector = connector(&pki, certificate, key);
        let remote = connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await
            .unwrap();
        assert!(remote.request(&request(1)).await.is_ok());
        registry.revoke(fingerprint).unwrap();
        assert!(remote.request(&request(2)).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // The withdrawn certificate dials again and is refused for its identity.
        let again = connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await;
        assert!(
            matches!(again, Err(WireError::Authentication)),
            "{:?}",
            again.err()
        );
        server.close();
        hyper_rt::futures::join(serve).await.unwrap();
    });
}

#[test]
fn a_certificate_outside_the_authority_never_dispatches() {
    on_shard(async {
        let pki = Pki::new();
        let stranger = Pki::new();
        let (certificate, key) = stranger.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = {
            let calls = calls.clone();
            move |verified: VerifiedRequest| {
                calls.fetch_add(1, Ordering::SeqCst);
                let answer = response(verified.request());
                async move { answer }
            }
        };
        let server = Arc::new(server(&pki, registry));
        let serving = server.clone();
        let serve = hyper_rt::futures::spawn(async move {
            serving.serve(handler).await.unwrap();
        })
        .unwrap();
        // The client trusts the server's authority; the server does not trust the client's.
        let tls = client_tls(
            TlsIdentity::from_pkcs8(vec![certificate], key),
            vec![pki.root()],
            &limits(),
        )
        .unwrap();
        let connector =
            QuicConnector::bind("127.0.0.1:0".parse().unwrap(), tls, limits(), budget()).unwrap();
        let dialed = connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await;
        assert!(
            matches!(dialed, Err(WireError::Authentication)),
            "{:?}",
            dialed.err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        server.close();
        hyper_rt::futures::join(serve).await.unwrap();
    });
}

#[test]
fn a_request_before_the_handler_waits_for_it() {
    on_shard(async {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(16).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let server = Arc::new(server(&pki, registry));
        let connector = connector(&pki, certificate, key);
        let address = server.local_addr().unwrap();
        // Dial and ask before the server is given its handler.
        let (asked_tx, asked_rx) = hyper_rt::sync::oneshot().unwrap();
        hyper_rt::futures::spawn_detached(async move {
            let remote = connector.connect(address, "localhost").await;
            let answered = match remote {
                Ok(remote) => remote.request(&request(5)).await,
                Err(error) => Err(error),
            };
            let _ = asked_tx.send(answered.map(|response| response.request_id));
        })
        .unwrap();
        hyper_rt::futures::sleep(50_000_000).await.unwrap();
        let serving = server.clone();
        let serve = hyper_rt::futures::spawn(async move {
            serving
                .serve(|verified: VerifiedRequest| {
                    let answer = response(verified.request());
                    async move { answer }
                })
                .await
                .unwrap();
        })
        .unwrap();
        assert_eq!(asked_rx.await.unwrap().unwrap(), RequestId::from_u128(5));
        server.close();
        hyper_rt::futures::join(serve).await.unwrap();
    });
}

#[test]
fn the_local_socket_round_trips_between_same_user_endpoints() {
    on_shard(async {
        let directory = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        let socket = directory.path().join("focal.sock");
        let server = Arc::new(LocalServer::bind(&socket, grant(), limits()).unwrap());
        let serving = server.clone();
        let serve = hyper_rt::futures::spawn(async move {
            serving
                .serve(|verified: VerifiedRequest| {
                    let answer = response(verified.request());
                    async move { answer }
                })
                .await
                .unwrap();
        })
        .unwrap();
        let remote = LocalRemote::new(&socket, limits()).unwrap();
        assert_eq!(
            remote.request(&request(9)).await.unwrap(),
            response(&request(9))
        );
        server.close();
        hyper_rt::futures::join(serve).await.unwrap();
    });
}
