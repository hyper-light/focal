//! Mutual TLS 1.3 for the transport's connections (mantle note 32 T49; node.md §3.2, §3.6;
//! focal `transport.rs` `server_tls`, `client_tls`).
//!
//! Both sides present a certificate from the deployment's authority and verify the other's against
//! it. TLS 1.3 only, as QUIC requires (RFC 9001 §4.2). 0-RTT is off on both sides: "Disabling 0-RTT
//! entirely is the most effective defense against replay attack", and a first message may be a
//! mutation (node.md §3.2). Connections from one configuration resume TLS sessions without early
//! data, which skips the certificate chain with no replay exposure.
//!
//! [`Tls::mutual`] builds that from [`Credentials`]. A project whose trust is more than one fixed
//! set of roots builds its own configurations and hands them over as a [`Tls`] (focal: roots that
//! change as its issuer is succeeded, a client verifier that adopts an endorsed successor, and a
//! second protocol on the same endpoint whose clients enroll without a certificate and are served
//! another one). The endpoint holds every configuration to the rules above whatever built it
//! ([`Tls::prepare`]): early data off on both sides, and a protocol named on each.

use hyper_quic::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use hyper_quic::rustls::{self, RootCertStore, server::WebPkiClientVerifier};
use hyper_quic::{ClientConfig, ServerConfig, TransportConfig};

pub use hyper_quic::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::Refusal;

/// The application protocol both sides negotiate (RFC 7301): a peer that speaks another is
/// refused at the handshake.
pub const ALPN: &[u8] = b"hyper-transport/1";

/// A node's credentials: its certificate chain, its private key, and the authority's roots its
/// peers' certificates are verified against.
pub struct Credentials {
    /// The node's chain, its own certificate first.
    pub chain: Vec<CertificateDer<'static>>,
    /// The node's private key.
    pub key: PrivateKeyDer<'static>,
    /// The roots peers' certificates chain to.
    pub roots: Vec<CertificateDer<'static>>,
}

/// The TLS an endpoint's connections run under: the server side when it accepts connections, and
/// the client side its dials present.
pub struct Tls {
    /// The server side, or `None` for an endpoint that only dials. Its `alpn_protocols` are the
    /// protocols the endpoint serves; the one a connection negotiated is told to the directory
    /// ([`crate::Authenticated::protocol`]).
    pub server: Option<rustls::ServerConfig>,
    /// The client side: what a dial offers, presents, and verifies the server by.
    pub client: rustls::ClientConfig,
}

fn provider() -> &'static rustls::crypto::CryptoProvider {
    hyper_quic::crypto::rustls::node_provider()
}

fn roots(certificates: &[CertificateDer<'static>]) -> Result<RootCertStore, Refusal> {
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate.clone())
            .map_err(|_| Refusal::Configuration)?;
    }
    if roots.is_empty() {
        return Err(Refusal::Configuration);
    }
    Ok(roots)
}

impl Tls {
    /// Mutual TLS from `credentials`: each side presents the chain and requires a certificate
    /// under the roots from the other, on [`ALPN`]; the server side only when `listen`.
    pub fn mutual(credentials: &Credentials, listen: bool) -> Result<Self, Refusal> {
        let server = if listen {
            let verifier =
                WebPkiClientVerifier::builder_with_provider(roots(&credentials.roots)?, provider())
                    .build()
                    .map_err(|_| Refusal::Configuration)?;
            let mut tls = rustls::ServerConfig::builder_with_provider(provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|_| Refusal::Configuration)?
                .with_client_cert_verifier(verifier)
                .with_single_cert(credentials.chain.clone(), credentials.key.clone_key())
                .map_err(|_| Refusal::Configuration)?;
            tls.alpn_protocols = vec![ALPN.to_vec()];
            Some(tls)
        } else {
            None
        };
        let mut client = rustls::ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| Refusal::Configuration)?
            .with_root_certificates(roots(&credentials.roots)?)
            .with_client_auth_cert(credentials.chain.clone(), credentials.key.clone_key())
            .map_err(|_| Refusal::Configuration)?;
        client.alpn_protocols = vec![ALPN.to_vec()];
        Ok(Self { server, client })
    }

    /// The QUIC configurations of this TLS under `transport`, held to the module's rules: early
    /// data off on both sides (a first message may be a mutation), and a protocol named on each
    /// side, so every connection negotiates one (RFC 9001 §8.1: QUIC's application protocol is
    /// agreed by ALPN). [`Refusal::Configuration`] for a side that names none, or that QUIC cannot
    /// run.
    pub(crate) fn prepare(
        self,
        transport: &TransportConfig,
    ) -> Result<(Option<ServerConfig>, ClientConfig), Refusal> {
        let Self { server, mut client } = self;
        let server = match server {
            Some(mut tls) => {
                if tls.alpn_protocols.is_empty() {
                    return Err(Refusal::Configuration);
                }
                tls.max_early_data_size = 0;
                let crypto = QuicServerConfig::try_from(tls).map_err(|_| Refusal::Configuration)?;
                let mut config = ServerConfig::with_crypto(Box::new(crypto));
                config.transport_config(transport.clone());
                Some(config)
            }
            None => None,
        };
        if client.alpn_protocols.is_empty() {
            return Err(Refusal::Configuration);
        }
        client.enable_early_data = false;
        let crypto = QuicClientConfig::try_from(client).map_err(|_| Refusal::Configuration)?;
        let mut config = ClientConfig::new(Box::new(crypto));
        config.transport_config(transport.clone());
        Ok((server, config))
    }
}
