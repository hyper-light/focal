//! focal-wire over hyper-transport on hyper-rt, what the transport's move added: a request that
//! arrives before the server is given its handler waits for it (the owner parks it whole).
use super::*;
use crate::net::LeafPins;

#[test]
fn a_request_before_the_handler_waits_for_it() {
    focal_rt::block_on(async {
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
        let connector = connector(&pki, certificate, key);
        let address = server.local_addr().unwrap();
        // Dial and ask before the server is given its handler.
        let asked = spawn(async move {
            let remote = connector.connect(address, "localhost").await?;
            remote
                .request(&request(5))
                .await
                .map(|response| response.request_id)
        });
        pause(Duration::from_millis(50)).await;
        let serving = server.clone();
        let serve = spawn(async move {
            serving
                .serve(|verified: VerifiedRequest| {
                    let answer = response(verified.request());
                    async move { answer }
                })
                .await
        });
        assert_eq!(asked.await.unwrap().unwrap(), RequestId::from_u128(5));
        server.close().unwrap();
        serve.join().await.unwrap();
    })
    .unwrap();
}

/// A close is never lost to a full queue: what the owner's queue cannot take now is given to a task
/// that waits its turn. Do: fill the owner's queue while it cannot run, and close the server. Expect:
/// the owner drains its queue and closes, and `serve` returns; a close the queue refused was dropped,
/// and the server served on.
#[test]
fn a_close_is_never_lost_to_a_full_queue() {
    focal_rt::block_on(async {
        let pki = Pki::new();
        let registry = PeerRegistry::new(4).unwrap();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let (server, task) = server(&pki, registry, handler).await;
        let mut answers = Vec::new();
        loop {
            let (reply, answer) = hyper_rt::sync::oneshot().unwrap();
            if !server
                .network()
                .try_queue(crate::net::Command::Stats { reply })
            {
                break;
            }
            answers.push(answer);
        }
        assert!(!answers.is_empty());
        server.close().unwrap();
        let served = timeout(Duration::from_secs(10), task).await;
        assert!(matches!(served, Some(Ok(Ok(())))), "the close was lost");
    })
    .unwrap();
}

/// What a listener serving the enrollment protocol answers in these tests, counting each request
/// it is handed: a joiner's message echoed with its tag after it; anything else refused.
struct Echoes {
    calls: Arc<AtomicUsize>,
}
impl crate::net::Serve for Echoes {
    fn capabilities(&self) -> [bool; 6] {
        [false; 6]
    }
    fn serve(&self, request: crate::net::Served) -> crate::net::ServeFuture {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            match (request.kind, request.from) {
                (crate::net::Kind::Enrollment(tag), crate::net::Inbound::Joiner) => {
                    let mut body = request.body;
                    body.push(tag);
                    Some(crate::net::Reply::uncharged(body))
                }
                _ => None,
            }
        })
    }
}

/// A message of the enrollment protocol crosses on a connection of its own: ungreeted, answered,
/// and closed after. Do: serve the enrollment protocol beside the data protocol under a bound of
/// its own, and link with pins that accept the server's leaf and with pins that do not, and ask
/// past the bound. Expect: the pinned message is answered and its connection closes with its link;
/// the unpinned server is refused `Unpinned` and the oversized message refused at the bound,
/// neither reaching the service.
#[test]
fn an_enrollment_message_is_answered_on_a_connection_of_its_own_and_only_from_a_pinned_leaf() {
    focal_rt::block_on(async {
        use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
        const ENROLL: &[u8] = b"focal-enroll/test";
        const CONTEXT: &str = "focal.wire.test-leaf.v1";
        let pki = Pki::new();
        let (server_certificate, server_key) = pki.issue(true);
        let provider = crate::crypto_provider();
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(pki.ca.der().to_vec()))
            .unwrap();
        // A joiner has no certificate yet: the listener lets a client go unauthenticated, and its
        // directory names one on the enrollment protocol a joiner.
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(roots.clone(), provider)
                .allow_unauthenticated()
                .build()
                .unwrap();
        let mut server_tls = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![CertificateDer::from(server_certificate.clone())],
                PrivatePkcs8KeyDer::from(server_key).into(),
            )
            .unwrap();
        server_tls.alpn_protocols = vec![ALPN.to_vec(), ENROLL.to_vec()];
        let bound = 4096u32;
        let server = Arc::new(
            QuicServer::bind_serving(
                "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
                ServerTls::from_config(server_tls),
                PeerRegistry::new(4).unwrap(),
                Some(EnrollmentProtocol {
                    alpn: ENROLL,
                    max_message_bytes: bound,
                }),
                limits(),
                AdmissionLimits::for_connections(limits().max_connections),
                budget(),
            )
            .unwrap(),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let running = server.clone();
        let echoes = Echoes {
            calls: calls.clone(),
        };
        let task = spawn(async move { running.serve_with(echoes).await });
        let mut client_tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_tls.alpn_protocols = vec![ENROLL.to_vec()];
        let connector = QuicConnector::bind(
            "127.0.0.1:0".parse().unwrap(),
            ClientTls::from_config(client_tls, Sender::Joiner),
            limits(),
            budget(),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let pinned = LeafPins {
            context: CONTEXT,
            accepted: [None, Some(blake3::derive_key(CONTEXT, &server_certificate))],
        };
        let period = Duration::from_secs(10);
        let link = connector
            .enrollment(address, "localhost", pinned)
            .await
            .unwrap();
        let answer = link.ask(3, b"join".to_vec(), period).await.unwrap();
        assert_eq!(answer, b"join\x03");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Its connection closes once its link is dropped.
        drop(link);
        admission_settles(&server, |stats| stats.connections == 0).await;

        let other = LeafPins {
            context: CONTEXT,
            accepted: [Some([7; 32]), None],
        };
        assert!(matches!(
            connector.enrollment(address, "localhost", other).await,
            Err(WireError::Unpinned)
        ));
        // The same leaf under another domain is another fingerprint.
        let elsewhere = LeafPins {
            context: "focal.wire.other-leaf.v1",
            ..pinned
        };
        assert!(matches!(
            connector.enrollment(address, "localhost", elsewhere).await,
            Err(WireError::Unpinned)
        ));
        let oversized = vec![0u8; usize::try_from(bound).unwrap() + 1];
        let link = connector
            .enrollment(address, "localhost", pinned)
            .await
            .unwrap();
        assert!(matches!(
            link.ask(3, oversized, period).await,
            Err(WireError::Limit)
        ));
        drop(link);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        connector.close().unwrap();
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap();
}

/// A listener that serves no enrollment protocol refuses its kinds: a greeted data peer that
/// sends one is refused before the service sees it.
#[test]
fn an_enrollment_kind_is_refused_where_the_protocol_is_not_served() {
    focal_rt::block_on(async {
        let pki = Pki::new();
        let (certificate, key) = pki.issue(false);
        let registry = PeerRegistry::new(4).unwrap();
        registry
            .register_certificate(&certificate, grant())
            .unwrap();
        let (server_certificate, server_key) = pki.issue(true);
        let tls = server_tls(
            TlsIdentity::from_pkcs8(vec![server_certificate], server_key),
            vec![pki.ca.der().to_vec()],
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
        let calls = Arc::new(AtomicUsize::new(0));
        let running = server.clone();
        let echoes = Echoes {
            calls: calls.clone(),
        };
        let task = spawn(async move { running.serve_with(echoes).await });
        let mut raw = RawPeer::connect(
            client_identity(&pki, certificate, key),
            server.local_addr().unwrap(),
        )
        .await;
        let refused = raw.enrollment(3, b"join").await;
        assert!(refused.is_err(), "{refused:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        server.close().unwrap();
        task.join().await.unwrap();
    })
    .unwrap();
}

/// A server's end is told once its endpoint and socket are gone: the address it held binds again.
#[test]
fn a_server_that_ended_has_freed_its_address() {
    focal_rt::block_on(async {
        let pki = Pki::new();
        let registry = PeerRegistry::new(4).unwrap();
        let handler: Arc<dyn RequestHandler> =
            Arc::new(|verified: VerifiedRequest| async move { response(verified.request()) });
        let (server, task) = server(&pki, registry.clone(), handler.clone()).await;
        let address = server.local_addr().unwrap();
        server.close().unwrap();
        server.ended().await;
        task.join().await.unwrap();
        let (again, task) = server_at(&pki, registry, handler, address).await;
        assert_eq!(again.local_addr().unwrap(), address);
        again.close().unwrap();
        again.ended().await;
        task.join().await.unwrap();
    })
    .unwrap();
}
