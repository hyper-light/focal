//! focal-wire over hyper-transport on hyper-rt, what the transport's move added: a request that
//! arrives before the server is given its handler waits for it (the owner parks it whole).
use super::*;

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
                "127.0.0.1:0".parse().unwrap(),
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
