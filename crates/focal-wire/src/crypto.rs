//! The cryptography of every connection focal makes, one provider for all:
//! TLS 1.3 over aws-lc-rs, its key exchange post-quantum hybrids only and its
//! traffic sealed only with 256-bit AEADs.
//!
//! - Key exchange: X25519MLKEM768 and SecP256r1MLKEM768, each ML-KEM-768
//!   (FIPS 203) beside an elliptic-curve exchange, as
//!   draft-ietf-tls-ecdhe-mlkem defines them. A hybrid stays secure while
//!   either half does, which is why NIST SP 800-227 and the NCSC and ANSSI
//!   transition guidance run hybrids now: what is recorded today cannot be
//!   read by a quantum computer later. A peer that offers no post-quantum
//!   exchange fails the handshake rather than falling back.
//! - Traffic: TLS 1.3's AES-256-GCM and ChaCha20-Poly1305. Grover's search
//!   halves a key's strength against a quantum adversary, so a 256-bit key
//!   keeps 128 bits (CNSA 2.0 requires AES-256).
//! - QUIC's Initial packets are sealed with AES-128-GCM under keys any
//!   observer derives from the connection id (RFC 9001 §5.2): they hide
//!   nothing, are required by the protocol, and are kept apart from the
//!   negotiated suites above.
//!
//! Authentication still signs with the certificates' classical keys: a
//! signature cannot be forged after the fact, so recorded traffic is safe,
//! and post-quantum signatures follow once certificates carry them.
use crate::WireError;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use std::sync::Arc;

/// focal's TLS provider (the module's rules).
pub fn crypto_provider() -> CryptoProvider {
    let mut provider = aws_lc_rs::default_provider();
    provider.kx_groups = vec![
        aws_lc_rs::kx_group::X25519MLKEM768,
        aws_lc_rs::kx_group::SECP256R1MLKEM768,
    ];
    provider.cipher_suites = vec![
        aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384,
        aws_lc_rs::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
    ];
    provider
}

/// The suite QUIC's Initial packets are sealed with (RFC 9001 §5.2).
fn quic_initial() -> Result<rustls::quic::Suite, WireError> {
    aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256
        .tls13()
        .and_then(|suite| suite.quic_suite())
        .ok_or(WireError::Authentication)
}

/// A QUIC client over `tls`, which [`crypto_provider`] built.
pub fn quic_client(tls: rustls::ClientConfig) -> Result<QuicClientConfig, WireError> {
    QuicClientConfig::with_initial(Arc::new(tls), quic_initial()?)
        .map_err(|_| WireError::Authentication)
}

/// A QUIC server over `tls`, which [`crypto_provider`] built.
pub fn quic_server(tls: rustls::ServerConfig) -> Result<QuicServerConfig, WireError> {
    QuicServerConfig::with_initial(Arc::new(tls), quic_initial()?)
        .map_err(|_| WireError::Authentication)
}
