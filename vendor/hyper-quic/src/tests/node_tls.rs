//! TLS between nodes: hybrid post-quantum key exchange and 256-bit TLS 1.3 suites only (the
//! owner's decision, 2026-10-04), with Initial packets still on AES-128-GCM (RFC 9001 §5.2).

use std::sync::LazyLock;

use rustls::client::WebPkiServerVerifier;
use rustls::crypto::CryptoProvider;
use rustls::crypto::aws_lc_rs::{cipher_suite, default_provider, kx_group};

use super::*;
use crate::crypto::rustls::{QuicClientConfig, node_provider};

static CLASSICAL_GROUPS: LazyLock<CryptoProvider> = LazyLock::new(|| CryptoProvider {
    kx_groups: vec![kx_group::X25519, kx_group::SECP256R1, kx_group::SECP384R1],
    ..default_provider()
});

static AES_128_ONLY: LazyLock<CryptoProvider> = LazyLock::new(|| CryptoProvider {
    kx_groups: vec![kx_group::X25519MLKEM768],
    cipher_suites: vec![cipher_suite::TLS13_AES_128_GCM_SHA256],
    ..default_provider()
});

static X25519_HYBRID: LazyLock<CryptoProvider> = LazyLock::new(|| CryptoProvider {
    kx_groups: vec![kx_group::X25519MLKEM768],
    cipher_suites: vec![cipher_suite::TLS13_AES_256_GCM_SHA384],
    ..default_provider()
});

static P384_HYBRID: LazyLock<CryptoProvider> = LazyLock::new(|| CryptoProvider {
    kx_groups: vec![kx_group::SECP384R1MLKEM1024],
    cipher_suites: vec![cipher_suite::TLS13_AES_256_GCM_SHA384],
    ..default_provider()
});

static P256_HYBRID_CHACHA: LazyLock<CryptoProvider> = LazyLock::new(|| CryptoProvider {
    kx_groups: vec![kx_group::SECP256R1MLKEM768],
    cipher_suites: vec![cipher_suite::TLS13_CHACHA20_POLY1305_SHA256],
    ..default_provider()
});

fn handshake_data(pair: &mut Pair, ch: ConnectionHandle) -> crate::crypto::rustls::HandshakeData {
    *pair
        .client_conn_mut(ch)
        .crypto_session()
        .handshake_data()
        .unwrap()
        .downcast::<crate::crypto::rustls::HandshakeData>()
        .unwrap()
}

/// A client of `provider`'s groups and suites, otherwise as hyper-quic's own
fn client_offering(provider: &'static CryptoProvider) -> ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CERTIFIED_KEY.cert.der().clone()).unwrap();
    let verifier = WebPkiServerVerifier::builder_with_provider(roots, provider)
        .build()
        .unwrap();
    let inner = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    ClientConfig::new(Box::new(QuicClientConfig::try_from(inner).unwrap()))
}

#[test]
fn nodes_negotiate_secp384r1mlkem1024_and_aes_256_gcm() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let (client_ch, _) = pair.connect();
    let data = handshake_data(&mut pair, client_ch);
    assert_eq!(
        data.negotiated_key_exchange_group,
        Some(rustls::NamedGroup::secp384r1MLKEM1024)
    );
    assert_eq!(
        data.negotiated_cipher_suite,
        Some(rustls::CipherSuite::TLS13_AES_256_GCM_SHA384)
    );
}

/// SecP384r1MLKEM1024 (ML-KEM-1024, CNSA 2.0) is served to a peer that offers only it.
#[test]
fn a_peer_offering_only_secp384r1mlkem1024_is_served() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_ch = pair.begin_connect(client_offering(&P384_HYBRID));
    pair.drive();
    pair.server.assert_accept();
    let data = handshake_data(&mut pair, client_ch);
    assert_eq!(
        data.negotiated_key_exchange_group,
        Some(rustls::NamedGroup::secp384r1MLKEM1024)
    );
}

/// X25519MLKEM768 is served to a peer that offers only it.
#[test]
fn a_peer_offering_only_x25519mlkem768_is_served() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_ch = pair.begin_connect(client_offering(&X25519_HYBRID));
    pair.drive();
    pair.server.assert_accept();
    let data = handshake_data(&mut pair, client_ch);
    assert_eq!(
        data.negotiated_key_exchange_group,
        Some(rustls::NamedGroup::X25519MLKEM768)
    );
}

/// The P-256 hybrid group is accepted when it is the only one a peer offers.
#[test]
fn a_peer_offering_only_secp256r1mlkem768_is_served() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let config = client_offering(&P256_HYBRID_CHACHA);
    let client_ch = pair.begin_connect(config);
    pair.drive();
    pair.server.assert_accept();
    let data = handshake_data(&mut pair, client_ch);
    assert_eq!(
        data.negotiated_key_exchange_group,
        Some(rustls::NamedGroup::secp256r1MLKEM768)
    );
    assert_eq!(
        data.negotiated_cipher_suite,
        Some(rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256)
    );
}

fn refused(config: ClientConfig) {
    let mut pair = Pair::default();
    let client_ch = pair.begin_connect(config);
    pair.drive();
    let lost =
        iter::from_fn(|| pair.client_conn_mut(client_ch).poll()).find_map(|event| match event {
            Event::ConnectionLost { reason } => Some(reason),
            Event::Connected => panic!("a peer outside the node policy connected"),
            _ => None,
        });
    // A TLS handshake_failure alert, carried as a CRYPTO_ERROR (RFC 9001 §4.8)
    assert_matches!(
        lost,
        Some(ConnectionError::ConnectionClosed(close))
            if close.error_code == TransportErrorCode::crypto(0x28)
    );
}

/// A peer offering only classical groups shares none with a node and is refused.
#[test]
fn a_classical_only_peer_is_refused() {
    let _guard = subscribe();
    refused(client_offering(&CLASSICAL_GROUPS));
}

/// A peer offering only a 128-bit suite shares none with a node and is refused.
#[test]
fn a_peer_offering_only_aes_128_is_refused() {
    let _guard = subscribe();
    refused(client_offering(&AES_128_ONLY));
}

/// Initial packets keep AEAD_AES_128_GCM whatever the node suites are (RFC 9001 §5.2).
#[test]
fn initial_packets_keep_aes_128_gcm() {
    let suite = crate::crypto::rustls::initial_suite().unwrap();
    assert_eq!(
        suite.suite.common.suite,
        rustls::CipherSuite::TLS13_AES_128_GCM_SHA256
    );
    assert!(
        node_provider()
            .cipher_suites
            .iter()
            .all(|s| s.suite() != rustls::CipherSuite::TLS13_AES_128_GCM_SHA256)
    );
}

/// A node dialling a classical-only server shares no group with it and is refused the same way.
#[test]
fn a_node_refuses_a_classical_only_server() {
    let _guard = subscribe();
    let mut pair = Pair::new(EndpointConfig::default(), server_config_classical(None));
    let client_ch = pair.begin_connect(client_config());
    pair.drive();
    let lost =
        iter::from_fn(|| pair.client_conn_mut(client_ch).poll()).find_map(|event| match event {
            Event::ConnectionLost { reason } => Some(reason),
            Event::Connected => panic!("a server outside the node policy was reached"),
            _ => None,
        });
    assert_matches!(
        lost,
        Some(ConnectionError::ConnectionClosed(close))
            if close.error_code == TransportErrorCode::crypto(0x28)
    );
}
