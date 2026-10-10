//! The cryptography of every connection focal makes, one provider for all: TLS 1.3 over aws-lc-rs
//! (hyper-tls, hyper-raft's conformed rustls), its key exchange post-quantum hybrids only and its
//! traffic sealed only with 256-bit AEADs. It is the shared stack's node provider
//! (`hyper_quic::crypto::rustls::node_provider`), the one every hyper-transport connection
//! between nodes runs:
//!
//! - Key exchange: SecP384r1MLKEM1024 first (ML-KEM-1024, CNSA 2.0's key establishment), whose
//!   share a ClientHello carries, then X25519MLKEM768 and SecP256r1MLKEM768, each ML-KEM (FIPS
//!   203) beside an elliptic-curve exchange, as draft-ietf-tls-ecdhe-mlkem defines them. A hybrid
//!   stays secure while either half does, which is why NIST SP 800-227 and the NCSC and ANSSI
//!   transition guidance run hybrids now: what is recorded today cannot be read by a quantum
//!   computer later. A peer that offers no post-quantum exchange fails the handshake rather than
//!   falling back. focal's own provider offered the two 768 groups; the shared one leads with
//!   ML-KEM-1024 at a quarter of a millisecond of CPU a handshake and no round trip more
//!   (hyper-raft docs/seal.md §10).
//! - Traffic: TLS 1.3's AES-256-GCM and ChaCha20-Poly1305. Grover's search halves a key's
//!   strength against a quantum adversary, so a 256-bit key keeps 128 bits (CNSA 2.0 requires
//!   AES-256).
//! - QUIC's Initial packets are sealed with AES-128-GCM under keys any observer derives from the
//!   connection id (RFC 9001 §5.2): they hide nothing, are required by the protocol, and are kept
//!   apart from the negotiated suites above.
//!
//! Authentication still signs with the certificates' classical keys: a signature cannot be forged
//! after the fact, so recorded traffic is safe, and post-quantum signatures follow once
//! certificates carry them.

/// focal's TLS provider (the module's rules), process-lifetime data every configuration borrows.
pub fn crypto_provider() -> &'static rustls::crypto::CryptoProvider {
    hyper_quic::crypto::rustls::node_provider()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_provider_offers_post_quantum_hybrids_and_256_bit_suites_only() {
        let provider = crypto_provider();
        let groups: Vec<String> = provider
            .kx_groups
            .iter()
            .map(|group| format!("{:?}", group.name()))
            .collect();
        assert!(!groups.is_empty());
        assert!(
            groups.iter().all(|group| group.contains("MLKEM")),
            "{groups:?}"
        );
        let suites: Vec<String> = provider
            .cipher_suites
            .iter()
            .map(|suite| format!("{:?}", suite.suite()))
            .collect();
        assert_eq!(suites.len(), 2, "{suites:?}");
        assert!(
            suites
                .iter()
                .all(|suite| suite.contains("AES_256") || suite.contains("CHACHA20"))
        );
    }
}
