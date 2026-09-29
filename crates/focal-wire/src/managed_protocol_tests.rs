use super::*;

#[derive(Clone)]
struct Capable {
    managed: bool,
    calls: tokio::sync::watch::Sender<usize>,
}
impl RequestHandler for Capable {
    fn supports_managed_requests(&self) -> bool {
        self.managed
    }
    fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
        Box::pin(async move {
            self.calls
                .send_modify(|count| *count = count.saturating_add(1));
            response(request.request())
        })
    }
}

#[test]
fn managed_negotiation_preserves_legacy_bytes_and_requires_explicit_support() {
    let hello = Hello {
        versions: vec![1],
        max_frame_bytes: 1024,
        max_items: 8,
    };
    assert_eq!(
        postcard::to_allocvec(&hello).unwrap(),
        vec![1, 1, 128, 8, 8]
    );
    let accepted = limits().negotiate_managed(&hello, true).unwrap();
    assert_eq!(
        postcard::to_allocvec(&HelloReply::Accepted(accepted)).unwrap(),
        vec![0, 1, 128, 8, 8]
    );
    let offered = Hello {
        versions: vec![2, 1],
        ..hello.clone()
    };
    assert_eq!(limits().negotiate(&offered).unwrap().protocol, 1);
    assert_eq!(
        limits().negotiate_managed(&offered, true).unwrap().protocol,
        2
    );
    let required = Hello {
        versions: vec![2],
        ..hello
    };
    assert_eq!(
        limits().negotiate(&required),
        Err(AccessError::UnsupportedProtocol)
    );
    assert!(!accepted.accepts_protocol(2));
    let managed = limits().negotiate_managed(&required, true).unwrap();
    assert!(managed.accepts_protocol(1));
    assert!(managed.accepts_protocol(2));
    assert!(!managed.accepts_protocol(3));
}

#[tokio::test]
async fn quic_capability_is_negotiated_before_request_and_legacy_requests_remain_valid() {
    let pki = Pki::new();
    let (certificate, key) = pki.issue(false);
    let registry = PeerRegistry::new(4).unwrap();
    registry
        .register_certificate(&certificate, grant())
        .unwrap();
    let connector = connector(&pki, certificate, key);
    for capable in [false, true] {
        let (calls, count) = tokio::sync::watch::channel(0);
        let (server, task) = server(
            &pki,
            registry.clone(),
            Arc::new(Capable {
                managed: capable,
                calls,
            }),
        )
        .await;
        let remote = connector
            .connect(server.local_addr().unwrap(), "localhost")
            .await
            .unwrap();
        assert_eq!(remote.negotiated().protocol, if capable { 2 } else { 1 });
        assert_eq!(
            remote.request(&request(1)).await.unwrap(),
            response(&request(1))
        );
        assert_eq!(*count.borrow(), 1);
        if !capable {
            let managed = RequestEnvelope {
                protocol: MANAGED_PROTOCOL_VERSION,
                ..request(2)
            };
            assert!(matches!(
                remote.request(&managed).await,
                Err(WireError::Access(AccessError::UnsupportedProtocol))
            ));
            assert_eq!(
                *count.borrow(),
                1,
                "no unsupported managed request reached ingress"
            );
        }
        remote.close();
        server.close();
        task.await.unwrap().unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn unix_managed_capability_refusal_precedes_request_transmission() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("focal.sock");
    let server = Arc::new(UnixServer::bind(&path, grant(), limits()).unwrap());
    let (calls, count) = tokio::sync::watch::channel(0);
    let running = server.clone();
    let task = tokio::spawn(async move {
        running
            .serve(Capable {
                managed: false,
                calls,
            })
            .await
    });
    let remote = UnixRemote::new(path, limits()).unwrap();
    let managed = RequestEnvelope {
        protocol: MANAGED_PROTOCOL_VERSION,
        ..request(2)
    };
    assert!(matches!(
        remote.request(&managed).await,
        Err(WireError::Access(AccessError::UnsupportedProtocol))
    ));
    assert_eq!(*count.borrow(), 0);
    assert_eq!(
        remote.request(&request(1)).await.unwrap(),
        response(&request(1))
    );
    assert_eq!(*count.borrow(), 1);
    server.close();
    task.await.unwrap().unwrap();
}
