use std::{any::Any, io, str, sync::LazyLock};

use aws_lc_rs::aead;
use bytes::BytesMut;
pub use rustls::Error;
use rustls::NamedGroup;
use rustls::{
    self, CipherSuite,
    client::danger::ServerCertVerifier,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    quic::{Connection, HeaderProtectionKey, KeyChange, PacketKey, Secrets, Suite, Version},
};

use crate::{
    ConnectError, ConnectionId, Side, TransportError, TransportErrorCode,
    crypto::{
        self, CryptoError, ExportKeyingMaterialError, HeaderKey, KeyPair, Keys, SessionConfig,
        UnsupportedVersion,
    },
    transport_parameters::TransportParameters,
};

impl From<Side> for rustls::Side {
    fn from(s: Side) -> Self {
        match s {
            Side::Client => Self::Client,
            Side::Server => Self::Server,
        }
    }
}

/// A rustls TLS session
pub struct TlsSession {
    version: Version,
    got_handshake_data: bool,
    next_secrets: Option<Secrets>,
    inner: Connection,
    suite: Suite,
}

impl TlsSession {
    fn side(&self) -> Side {
        match self.inner {
            Connection::Client(_) => Side::Client,
            Connection::Server(_) => Side::Server,
        }
    }
}

impl crypto::Session for TlsSession {
    fn initial_keys(&self, dst_cid: &ConnectionId, side: Side) -> Keys {
        initial_keys(self.version, *dst_cid, side, &self.suite)
    }

    fn handshake_data(&self) -> Option<Box<dyn Any>> {
        if !self.got_handshake_data {
            return None;
        }
        Some(Box::new(HandshakeData {
            protocol: self.inner.alpn_protocol().map(|x| x.into()),
            server_name: match self.inner {
                Connection::Client(_) => None,
                Connection::Server(ref session) => session.server_name().map(|x| x.into()),
            },
            negotiated_key_exchange_group: self
                .inner
                .negotiated_key_exchange_group()
                .map(|group| group.name()),
            negotiated_cipher_suite: self
                .inner
                .negotiated_cipher_suite()
                .map(|suite| suite.suite()),
        }))
    }

    /// For the rustls `TlsSession`, the `Any` type is `Vec<rustls::pki_types::CertificateDer>`
    fn peer_identity(&self) -> Option<Box<dyn Any>> {
        self.inner.peer_certificates().map(|v| -> Box<dyn Any> {
            Box::new(
                v.iter()
                    .map(|v| v.clone().into_owned())
                    .collect::<Vec<CertificateDer<'static>>>(),
            )
        })
    }

    fn early_crypto(&self) -> Option<(Box<dyn HeaderKey>, Box<dyn crypto::PacketKey>)> {
        let keys = self.inner.zero_rtt_keys()?;
        Some((Box::new(keys.header), Box::new(keys.packet)))
    }

    fn early_data_accepted(&self) -> Option<bool> {
        match self.inner {
            Connection::Client(ref session) => Some(session.is_early_data_accepted()),
            _ => None,
        }
    }

    fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }

    fn read_handshake(
        &mut self,
        config: SessionConfig<'_>,
        buf: &[u8],
    ) -> Result<bool, TransportError> {
        let read = match (&mut self.inner, config) {
            (Connection::Client(session), SessionConfig::Client(config)) => {
                let config: &mut dyn Any = config;
                let Some(config) = config.downcast_mut::<QuicClientConfig>() else {
                    return Err(foreign_config());
                };
                session.read_hs(&mut config.inner, buf)
            }
            (Connection::Server(session), SessionConfig::Server(config)) => {
                let config: &mut dyn Any = config;
                let Some(config) = config.downcast_mut::<QuicServerConfig>() else {
                    return Err(foreign_config());
                };
                session.read_hs(&mut config.inner, buf)
            }
            _ => {
                return Err(TransportError::INTERNAL_ERROR(
                    "TLS session lent the other side's configuration",
                ));
            }
        };
        read.map_err(|e| {
            if let Some(alert) = self.inner.alert() {
                TransportError {
                    code: TransportErrorCode::crypto(alert.into()),
                    frame: None,
                    reason: e.to_string(),
                }
            } else {
                TransportError::PROTOCOL_VIOLATION(format!("TLS error: {e}"))
            }
        })?;
        if !self.got_handshake_data {
            // Hack around the lack of an explicit signal from rustls to reflect ClientHello being
            // ready on incoming connections, or ALPN negotiation completing on outgoing
            // connections.
            let have_server_name = match self.inner {
                Connection::Client(_) => false,
                Connection::Server(ref session) => session.server_name().is_some(),
            };
            if self.inner.alpn_protocol().is_some() || have_server_name || !self.is_handshaking() {
                self.got_handshake_data = true;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn transport_parameters(&self) -> Result<Option<TransportParameters>, TransportError> {
        match self.inner.quic_transport_parameters() {
            None => Ok(None),
            Some(buf) => match TransportParameters::read(self.side(), &mut io::Cursor::new(buf)) {
                Ok(params) => Ok(Some(params)),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn write_handshake(&mut self, buf: &mut Vec<u8>) -> Option<Keys> {
        let keys = match self.inner.write_hs(buf)? {
            KeyChange::Handshake { keys } => keys,
            KeyChange::OneRtt { keys, next } => {
                self.next_secrets = Some(next);
                keys
            }
        };

        Some(Keys {
            header: KeyPair {
                local: Box::new(keys.local.header),
                remote: Box::new(keys.remote.header),
            },
            packet: KeyPair {
                local: Box::new(keys.local.packet),
                remote: Box::new(keys.remote.packet),
            },
        })
    }

    fn next_1rtt_keys(&mut self) -> Option<KeyPair<Box<dyn crypto::PacketKey>>> {
        let secrets = self.next_secrets.as_mut()?;
        let keys = secrets.next_packet_keys();
        Some(KeyPair {
            local: Box::new(keys.local),
            remote: Box::new(keys.remote),
        })
    }

    fn is_valid_retry(&self, orig_dst_cid: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        let tag_start = match payload.len().checked_sub(16) {
            Some(x) => x,
            None => return false,
        };

        // A session's version is one `interpret_version` gave, which has retry keys
        let Some((nonce, key)) = retry_integrity(self.version) else {
            return false;
        };

        // In-memory sizes: one datagram, so no sum saturates
        let mut pseudo_packet = Vec::with_capacity(
            header
                .len()
                .saturating_add(payload.len())
                .saturating_add(orig_dst_cid.len())
                .saturating_add(1),
        );
        pseudo_packet.push(cid_len_byte(orig_dst_cid));
        pseudo_packet.extend_from_slice(orig_dst_cid);
        pseudo_packet.extend_from_slice(header);
        let tag_start = tag_start.saturating_add(pseudo_packet.len());
        pseudo_packet.extend_from_slice(payload);

        let nonce = aead::Nonce::assume_unique_for_key(nonce);
        let Ok(key) = aead::UnboundKey::new(&aead::AES_128_GCM, &key) else {
            return false;
        };
        let key = aead::LessSafeKey::new(key);

        let Some((aad, tag)) = pseudo_packet.split_at_mut_checked(tag_start) else {
            return false;
        };
        key.open_in_place(nonce, aead::Aad::from(aad), tag).is_ok()
    }

    fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), ExportKeyingMaterialError> {
        self.inner
            .export_keying_material(output, label, Some(context))
            .map_err(|_| ExportKeyingMaterialError)?;
        Ok(())
    }
}

/// The Retry integrity key of draft-ietf-quic-tls-29 §5.8
const RETRY_INTEGRITY_KEY_DRAFT: [u8; 16] = [
    0xcc, 0xce, 0x18, 0x7e, 0xd0, 0x9a, 0x09, 0xd0, 0x57, 0x28, 0x15, 0x5a, 0x6c, 0xb9, 0x6b, 0xe1,
];
/// The Retry integrity nonce of draft-ietf-quic-tls-29 §5.8
const RETRY_INTEGRITY_NONCE_DRAFT: [u8; 12] = [
    0xe5, 0x49, 0x30, 0xf9, 0x7f, 0x21, 0x36, 0xf0, 0x53, 0x0a, 0x8c, 0x1c,
];

/// The Retry integrity key of QUIC v1 (RFC 9001 §5.8)
const RETRY_INTEGRITY_KEY_V1: [u8; 16] = [
    0xbe, 0x0c, 0x69, 0x0b, 0x9f, 0x66, 0x57, 0x5a, 0x1d, 0x76, 0x6b, 0x54, 0xe3, 0x68, 0xc8, 0x4e,
];
/// The Retry integrity nonce of QUIC v1 (RFC 9001 §5.8)
const RETRY_INTEGRITY_NONCE_V1: [u8; 12] = [
    0x46, 0x15, 0x99, 0xd3, 0x5d, 0x63, 0x2b, 0xf2, 0x23, 0x98, 0x25, 0xbb,
];

impl crypto::HeaderKey for Box<dyn HeaderProtectionKey> {
    fn decrypt(&self, pn_offset: usize, packet: &mut [u8]) -> Result<(), CryptoError> {
        let (sample, first, pn) = header_protection_parts(packet, pn_offset, self.sample_size())?;
        self.decrypt_in_place(sample, first, pn)
            .map_err(|_| CryptoError)
    }

    fn encrypt(&self, pn_offset: usize, packet: &mut [u8]) -> Result<(), CryptoError> {
        let (sample, first, pn) = header_protection_parts(packet, pn_offset, self.sample_size())?;
        self.encrypt_in_place(sample, first, pn)
            .map_err(|_| CryptoError)
    }

    fn sample_size(&self) -> usize {
        self.sample_len()
    }
}

/// Authentication data for (rustls) TLS session
pub struct HandshakeData {
    /// The negotiated application protocol, if ALPN is in use
    ///
    /// Guaranteed to be set if a nonempty list of protocols was specified for this connection.
    pub protocol: Option<Vec<u8>>,
    /// The server name specified by the client, if any
    ///
    /// Always `None` for outgoing connections
    pub server_name: Option<String>,
    /// The key exchange group negotiated with the peer, once the handshake has chosen one
    pub negotiated_key_exchange_group: Option<NamedGroup>,
    /// The TLS 1.3 cipher suite negotiated with the peer, once the handshake has chosen one
    pub negotiated_cipher_suite: Option<CipherSuite>,
}

/// A QUIC-compatible TLS client configuration
///
/// Quinn implicitly constructs a `QuicClientConfig` with reasonable defaults within
/// [`ClientConfig::with_root_certificates()`][root_certs].
/// Alternatively, `QuicClientConfig`'s [`TryFrom`] implementation can be used to wrap around a
/// custom [`rustls::ClientConfig`], in which case care should be taken around certain points:
///
/// - If `enable_early_data` is not set to true, then sending 0-RTT data will not be possible on
///   outgoing connections.
/// - The [`rustls::ClientConfig`] must have TLS 1.3 support enabled for conversion to succeed.
///
/// The object in the `resumption` field of the inner [`rustls::ClientConfig`] determines whether
/// calling `into_0rtt` on outgoing connections returns `Ok` or `Err`. It typically allows
/// `into_0rtt` to proceed if it recognizes the server name, and defaults to an in-memory cache of
/// 256 server names.
///
/// [root_certs]: crate::config::ClientConfig::with_root_certificates()
pub struct QuicClientConfig {
    pub(crate) inner: rustls::ClientConfig,
    initial: Suite,
}

impl QuicClientConfig {
    /// Initialize a sane QUIC-compatible TLS client configuration
    ///
    /// QUIC requires that TLS 1.3 be enabled. Advanced users can use any [`rustls::ClientConfig`] that
    /// satisfies this requirement.
    pub(crate) fn new(verifier: Box<dyn ServerCertVerifier>) -> Result<Self, rustls::Error> {
        let inner = Self::inner(verifier)?;
        Ok(Self {
            // aws-lc-rs's suite table holds TLS13_AES_128_GCM_SHA256 with its QUIC keys; were it
            // gone the configuration is refused, where upstream panicked
            initial: initial_suite()
                .ok_or(rustls::Error::Internal("no initial cipher suite found"))?,
            inner,
        })
    }

    /// Initialize a QUIC-compatible TLS client configuration with a separate initial cipher suite
    ///
    /// This is useful if you want to avoid the initial cipher suite for traffic encryption.
    pub fn with_initial(
        inner: rustls::ClientConfig,
        initial: Suite,
    ) -> Result<Self, NoInitialCipherSuite> {
        match initial.suite.common.suite {
            CipherSuite::TLS13_AES_128_GCM_SHA256 => Ok(Self { inner, initial }),
            _ => Err(NoInitialCipherSuite { specific: true }),
        }
    }

    pub(crate) fn inner(
        verifier: Box<dyn ServerCertVerifier>,
    ) -> Result<rustls::ClientConfig, rustls::Error> {
        // The default providers support TLS 1.3; one that does not is refused
        let mut config = rustls::ClientConfig::builder_with_provider(node_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();

        config.enable_early_data = true;
        Ok(config)
    }
}

impl crypto::ClientConfig for QuicClientConfig {
    fn start_session(
        &mut self,
        version: u32,
        server_name: &str,
        params: &TransportParameters,
    ) -> Result<Box<dyn crypto::Session>, ConnectError> {
        let version = interpret_version(version)?;
        Ok(Box::new(TlsSession {
            version,
            got_handshake_data: false,
            next_secrets: None,
            inner: rustls::quic::Connection::Client(
                rustls::quic::ClientConnection::new(
                    &mut self.inner,
                    version,
                    ServerName::try_from(server_name)
                        .map_err(|_| ConnectError::InvalidServerName(server_name.into()))?
                        .to_owned(),
                    to_vec(params),
                )
                .map_err(|_| ConnectError::InvalidTlsConfig)?,
            ),
            suite: self.initial,
        }))
    }
}

impl TryFrom<rustls::ClientConfig> for QuicClientConfig {
    type Error = NoInitialCipherSuite;

    fn try_from(inner: rustls::ClientConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            initial: initial_suite().ok_or(NoInitialCipherSuite { specific: false })?,
            inner,
        })
    }
}

/// The initial cipher suite (AES-128-GCM-SHA256) is not available
///
/// When the cipher suite is supplied `with_initial()`, it must be
/// [`CipherSuite::TLS13_AES_128_GCM_SHA256`]. When the cipher suite is derived from a config's
/// [`CryptoProvider`][provider], it is aws-lc-rs's own AES-128-GCM suite whichever suites the
/// provider offers (RFC 9001 §5.2).
///
/// [provider]: rustls::crypto::CryptoProvider
#[derive(Clone, Debug)]
pub struct NoInitialCipherSuite {
    /// Whether the initial cipher suite was supplied by the caller
    specific: bool,
}

impl std::fmt::Display for NoInitialCipherSuite {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self.specific {
            true => "invalid cipher suite specified",
            false => "no initial cipher suite found",
        })
    }
}

impl std::error::Error for NoInitialCipherSuite {}

/// A QUIC-compatible TLS server configuration
///
/// Quinn implicitly constructs a `QuicServerConfig` with reasonable defaults within
/// [`ServerConfig::with_single_cert()`][single]. Alternatively, `QuicServerConfig`'s [`TryFrom`]
/// implementation or `with_initial` method can be used to wrap around a custom
/// [`rustls::ServerConfig`], in which case care should be taken around certain points:
///
/// - If `max_early_data_size` is not set to `u32::MAX`, the server will not be able to accept
///   incoming 0-RTT data. QUIC prohibits `max_early_data_size` values other than 0 or `u32::MAX`.
/// - The `rustls::ServerConfig` must have TLS 1.3 support enabled for conversion to succeed.
///
/// [single]: crate::config::ServerConfig::with_single_cert()
pub struct QuicServerConfig {
    inner: rustls::ServerConfig,
    initial: Suite,
}

impl QuicServerConfig {
    pub(crate) fn new(
        cert_chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self, rustls::Error> {
        let inner = Self::inner(cert_chain, key)?;
        Ok(Self {
            // aws-lc-rs's suite table holds TLS13_AES_128_GCM_SHA256 with its QUIC keys; were it
            // gone the configuration is refused, where upstream panicked
            initial: initial_suite()
                .ok_or(rustls::Error::Internal("no initial cipher suite found"))?,
            inner,
        })
    }

    /// Initialize a QUIC-compatible TLS client configuration with a separate initial cipher suite
    ///
    /// This is useful if you want to avoid the initial cipher suite for traffic encryption.
    pub fn with_initial(
        inner: rustls::ServerConfig,
        initial: Suite,
    ) -> Result<Self, NoInitialCipherSuite> {
        match initial.suite.common.suite {
            CipherSuite::TLS13_AES_128_GCM_SHA256 => Ok(Self { inner, initial }),
            _ => Err(NoInitialCipherSuite { specific: true }),
        }
    }

    /// Initialize a sane QUIC-compatible TLS server configuration
    ///
    /// QUIC requires that TLS 1.3 be enabled, and that the maximum early data size is either 0 or
    /// `u32::MAX`. Advanced users can use any [`rustls::ServerConfig`] that satisfies these
    /// requirements.
    pub(crate) fn inner(
        cert_chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<rustls::ServerConfig, rustls::Error> {
        // The default provider supports TLS 1.3; one that does not is refused
        let mut inner = rustls::ServerConfig::builder_with_provider(node_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)?;

        inner.max_early_data_size = u32::MAX;
        Ok(inner)
    }
}

impl TryFrom<rustls::ServerConfig> for QuicServerConfig {
    type Error = NoInitialCipherSuite;

    fn try_from(inner: rustls::ServerConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            initial: initial_suite().ok_or(NoInitialCipherSuite { specific: false })?,
            inner,
        })
    }
}

impl crypto::ServerConfig for QuicServerConfig {
    fn start_session(
        &self,
        version: u32,
        params: &TransportParameters,
    ) -> Result<Box<dyn crypto::Session>, TransportError> {
        // `start_session()` is never called if `initial_keys()` rejected `version`
        let version = interpret_version(version)
            .map_err(|_| TransportError::INTERNAL_ERROR("session for an unsupported version"))?;
        let connection = rustls::quic::ServerConnection::new(&self.inner, version, to_vec(params))
            .map_err(|_| TransportError::INTERNAL_ERROR("TLS configuration cannot serve QUIC"))?;
        Ok(Box::new(TlsSession {
            version,
            got_handshake_data: false,
            next_secrets: None,
            inner: rustls::quic::Connection::Server(connection),
            suite: self.initial,
        }))
    }

    fn initial_keys(
        &self,
        version: u32,
        dst_cid: &ConnectionId,
    ) -> Result<Keys, UnsupportedVersion> {
        let version = interpret_version(version)?;
        Ok(initial_keys(version, *dst_cid, Side::Server, &self.initial))
    }

    fn retry_tag(
        &self,
        version: u32,
        orig_dst_cid: &ConnectionId,
        packet: &[u8],
    ) -> Result<[u8; 16], CryptoError> {
        // `retry_tag()` is never called if `initial_keys()` rejected `version`
        let version = interpret_version(version).map_err(|_| CryptoError)?;
        let (nonce, key) = retry_integrity(version).ok_or(CryptoError)?;

        // In-memory sizes: one datagram
        let mut pseudo_packet = Vec::with_capacity(
            packet
                .len()
                .saturating_add(orig_dst_cid.len())
                .saturating_add(1),
        );
        pseudo_packet.push(cid_len_byte(orig_dst_cid));
        pseudo_packet.extend_from_slice(orig_dst_cid);
        pseudo_packet.extend_from_slice(packet);

        let nonce = aead::Nonce::assume_unique_for_key(nonce);
        let key = aead::LessSafeKey::new(aead::UnboundKey::new(&aead::AES_128_GCM, &key)?);

        let tag = key.seal_in_place_separate_tag(nonce, aead::Aad::from(pseudo_packet), &mut [])?;
        tag.as_ref().try_into().map_err(|_| CryptoError)
    }
}

/// The error for a session lent a configuration from another crypto implementation
///
/// An endpoint lends each session the configuration that started it, so this is unreachable
/// through the endpoint; it is still a typed error, not an assumption.
fn foreign_config() -> TransportError {
    TransportError::INTERNAL_ERROR("TLS session lent another implementation's configuration")
}

/// The Initial packets' suite, AES-128-GCM with SHA-256, whatever the connection negotiates:
/// "Initial packets use AEAD_AES_128_GCM with keys derived from the Destination Connection ID"
/// (RFC 9001 §5.2). It is taken from the provider's own suite table, not from the suites a
/// configuration offers, so a configuration that offers only 256-bit suites still protects its
/// Initial packets as the RFC requires.
pub(crate) fn initial_suite() -> Option<Suite> {
    match rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256 {
        rustls::SupportedCipherSuite::Tls13(suite) => suite.quic_suite(),
        rustls::SupportedCipherSuite::Tls12(_) => None,
    }
}

/// The key exchange groups between nodes: the hybrid post-quantum groups only
/// (draft-ietf-tls-ecdhe-mlkem). SecP384r1MLKEM1024 first (ML-KEM-1024, CNSA 2.0's key
/// establishment), whose share a ClientHello carries; then X25519MLKEM768 and SecP256r1MLKEM768,
/// served to a peer that offers them. Measured at 500 ms one way it costs no round trip and no
/// latency against X25519MLKEM768 first, and a quarter of a millisecond of CPU a handshake
/// (hyper-raft docs/seal.md §10). A classical-only peer finds no group in common and is refused
/// (the owner's decision, 2026-10-04).
static NODE_KX_GROUPS: [&dyn rustls::crypto::SupportedKxGroup; 3] = [
    rustls::crypto::aws_lc_rs::kx_group::SECP384R1MLKEM1024,
    rustls::crypto::aws_lc_rs::kx_group::X25519MLKEM768,
    rustls::crypto::aws_lc_rs::kx_group::SECP256R1MLKEM768,
];

/// The TLS 1.3 suites between nodes: those with 256-bit keys only, AES-256-GCM first, then
/// ChaCha20-Poly1305 (RFC 8446 §B.4). Initial packets keep AES-128-GCM ([`initial_suite`]).
static NODE_CIPHER_SUITES: [rustls::SupportedCipherSuite; 2] = [
    rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384,
    rustls::crypto::aws_lc_rs::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
];

/// The provider between nodes: aws-lc-rs's, restricted to the hybrid post-quantum groups and the
/// 256-bit suites, as process-lifetime data every configuration borrows
static NODE_PROVIDER: LazyLock<rustls::crypto::CryptoProvider> =
    LazyLock::new(|| rustls::crypto::CryptoProvider {
        cipher_suites: NODE_CIPHER_SUITES.to_vec(),
        kx_groups: NODE_KX_GROUPS.to_vec(),
        ..rustls::crypto::aws_lc_rs::default_provider()
    });

/// The provider every configuration hyper-quic builds uses, and that the application layer's
/// mutual TLS between nodes uses: hybrid post-quantum key exchange and 256-bit TLS 1.3 suites only
pub fn node_provider() -> &'static rustls::crypto::CryptoProvider {
    &NODE_PROVIDER
}

fn to_vec(params: &TransportParameters) -> Vec<u8> {
    let mut bytes = Vec::new();
    params.write(&mut bytes);
    bytes
}

pub(crate) fn initial_keys(
    version: Version,
    dst_cid: ConnectionId,
    side: Side,
    suite: &Suite,
) -> Keys {
    let keys = suite.keys(&dst_cid, side.into(), version);
    Keys {
        header: KeyPair {
            local: Box::new(keys.local.header),
            remote: Box::new(keys.remote.header),
        },
        packet: KeyPair {
            local: Box::new(keys.local.packet),
            remote: Box::new(keys.remote.packet),
        },
    }
}

impl crypto::PacketKey for Box<dyn PacketKey> {
    fn encrypt(&self, packet: u64, buf: &mut [u8], header_len: usize) -> Result<(), CryptoError> {
        let (header, payload_tag) = buf.split_at_mut_checked(header_len).ok_or(CryptoError)?;
        let payload_len = payload_tag
            .len()
            .checked_sub(self.tag_len())
            .ok_or(CryptoError)?;
        let (payload, tag_storage) = payload_tag.split_at_mut(payload_len);
        let tag = self
            .encrypt_in_place(packet, &*header, payload)
            .map_err(|_| CryptoError)?;
        // The storage is `tag_len` bytes, the tag's length
        for (storage, byte) in tag_storage.iter_mut().zip(tag.as_ref()) {
            *storage = *byte;
        }
        Ok(())
    }

    fn decrypt(
        &self,
        packet: u64,
        header: &[u8],
        payload: &mut BytesMut,
    ) -> Result<(), CryptoError> {
        let plain = self
            .decrypt_in_place(packet, header, payload.as_mut())
            .map_err(|_| CryptoError)?;
        let plain_len = plain.len();
        payload.truncate(plain_len);
        Ok(())
    }

    fn tag_len(&self) -> usize {
        (**self).tag_len()
    }

    fn confidentiality_limit(&self) -> u64 {
        (**self).confidentiality_limit()
    }

    fn integrity_limit(&self) -> u64 {
        (**self).integrity_limit()
    }
}

/// The Retry integrity nonce and key of `version` (RFC 9001 §5.8)
fn retry_integrity(version: Version) -> Option<([u8; 12], [u8; 16])> {
    match version {
        Version::V1 => Some((RETRY_INTEGRITY_NONCE_V1, RETRY_INTEGRITY_KEY_V1)),
        Version::V1Draft => Some((RETRY_INTEGRITY_NONCE_DRAFT, RETRY_INTEGRITY_KEY_DRAFT)),
        _ => None,
    }
}

/// A connection ID's length byte; a connection ID is at most 20 bytes
fn cid_len_byte(cid: &ConnectionId) -> u8 {
    u8::try_from(cid.len()).unwrap_or(u8::MAX)
}

/// The sample, first byte and packet number bytes header protection covers (RFC 9001 §5.4):
/// the sample starts four bytes past the packet number's offset, and the packet number is at
/// most four bytes
fn header_protection_parts(
    packet: &mut [u8],
    pn_offset: usize,
    sample_size: usize,
) -> Result<(&[u8], &mut u8, &mut [u8]), CryptoError> {
    let sample_start = pn_offset.checked_add(4).ok_or(CryptoError)?;
    let (header, sample) = packet
        .split_at_mut_checked(sample_start)
        .ok_or(CryptoError)?;
    let sample = sample.get(..sample_size).ok_or(CryptoError)?;
    let (first, rest) = header.split_first_mut().ok_or(CryptoError)?;
    // `rest` starts at offset 1, so the packet number starts at `pn_offset - 1` within it
    let pn_start = pn_offset.checked_sub(1).ok_or(CryptoError)?;
    let pn_end = pn_offset.saturating_add(3).min(rest.len());
    let pn = rest.get_mut(pn_start..pn_end).ok_or(CryptoError)?;
    Ok((sample, first, pn))
}

fn interpret_version(version: u32) -> Result<Version, UnsupportedVersion> {
    match version {
        0xff00_001d..=0xff00_0020 => Ok(Version::V1Draft),
        0x0000_0001 | 0xff00_0021..=0xff00_0022 => Ok(Version::V1),
        _ => Err(UnsupportedVersion),
    }
}
