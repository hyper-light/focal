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
        server.close();
        serve.join().await.unwrap();
    })
    .unwrap();
}
