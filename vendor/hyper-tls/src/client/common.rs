use alloc::boxed::Box;
use alloc::vec::Vec;

use super::ResolvesClientCert;
use crate::log::{debug, trace};
use crate::msgs::enums::ExtensionType;
use crate::msgs::handshake::{CertificateChain, DistinguishedName, ProtocolName, ServerExtensions};
use crate::{compress, sign, CipherSuite, SignatureScheme};

#[derive(Debug)]
pub(super) struct ServerCertDetails<'a> {
    pub(super) cert_chain: CertificateChain<'a>,
    pub(super) ocsp_response: Vec<u8>,
}

impl<'a> ServerCertDetails<'a> {
    pub(super) fn new(cert_chain: CertificateChain<'a>, ocsp_response: Vec<u8>) -> Self {
        Self {
            cert_chain,
            ocsp_response,
        }
    }

    pub(super) fn into_owned(self) -> ServerCertDetails<'static> {
        let Self {
            cert_chain,
            ocsp_response,
        } = self;
        ServerCertDetails {
            cert_chain: cert_chain.into_owned(),
            ocsp_response,
        }
    }
}

pub(super) struct ClientHelloDetails {
    pub(super) alpn_protocols: Vec<ProtocolName>,
    pub(super) sent_extensions: Vec<ExtensionType>,
    pub(super) extension_order_seed: u16,
    pub(super) offered_cert_compression: bool,
    pub(super) offered_cipher_suites: Vec<CipherSuite>,
}

impl ClientHelloDetails {
    pub(super) fn new(alpn_protocols: Vec<ProtocolName>, extension_order_seed: u16) -> Self {
        Self {
            alpn_protocols,
            sent_extensions: Vec::new(),
            extension_order_seed,
            offered_cert_compression: false,
            offered_cipher_suites: Vec::new(),
        }
    }

    pub(super) fn server_sent_unsolicited_extensions(
        &self,
        received_exts: &ServerExtensions<'_>,
        allowed_unsolicited: &[ExtensionType],
    ) -> bool {
        let mut extensions = received_exts.collect_used();
        extensions.extend(
            received_exts
                .unknown_extensions
                .iter()
                .map(|ext| ExtensionType::from(*ext)),
        );
        for ext_type in extensions {
            if !self.sent_extensions.contains(&ext_type) && !allowed_unsolicited.contains(&ext_type)
            {
                trace!("Unsolicited extension {ext_type:?}");
                return true;
            }
        }

        false
    }
}

/// A server's request for client authentication, kept until the client answers it.
///
/// The client resolves its certificate in the call that sends its `Certificate` and
/// `CertificateVerify`: on the server's `Finished` in TLS 1.3, on `ServerHelloDone` in TLS 1.2.
/// The certificate key is borrowed from the configuration, and a connection holds no
/// configuration between calls, so the request is kept instead of the key.
pub(super) struct ClientAuthRequest {
    canames: Option<Vec<DistinguishedName>>,
    sigschemes: Vec<SignatureScheme>,
    auth_context_tls13: Option<Vec<u8>>,
    compressor: Option<&'static dyn compress::CertCompressor>,
}

impl ClientAuthRequest {
    pub(super) fn new(
        canames: Option<Vec<DistinguishedName>>,
        sigschemes: Vec<SignatureScheme>,
        auth_context_tls13: Option<Vec<u8>>,
        compressor: Option<&'static dyn compress::CertCompressor>,
    ) -> Self {
        Self {
            canames,
            sigschemes,
            auth_context_tls13,
            compressor,
        }
    }

    /// Choose the certificate and signer to answer this request with.
    pub(super) fn resolve(self, resolver: &dyn ResolvesClientCert) -> ClientAuthDetails<'_> {
        let Self {
            canames,
            sigschemes,
            auth_context_tls13,
            compressor,
        } = self;
        let acceptable_issuers = canames
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|p| p.as_ref())
            .collect::<Vec<&[u8]>>();

        if let Some(certkey) = resolver.resolve(&acceptable_issuers, &sigschemes) {
            if let Some(signer) = certkey.key.choose_scheme(&sigschemes) {
                debug!("Attempting client auth");
                return ClientAuthDetails::Verify {
                    certkey,
                    signer,
                    auth_context_tls13,
                    compressor,
                };
            }
        }

        debug!("Client auth requested but no cert/sigscheme available");
        ClientAuthDetails::Empty { auth_context_tls13 }
    }
}

pub(super) enum ClientAuthDetails<'a> {
    /// Send an empty `Certificate` and no `CertificateVerify`.
    Empty { auth_context_tls13: Option<Vec<u8>> },
    /// Send a non-empty `Certificate` and a `CertificateVerify`.
    Verify {
        certkey: &'a sign::CertifiedKey,
        signer: Box<dyn sign::Signer + 'a>,
        auth_context_tls13: Option<Vec<u8>>,
        compressor: Option<&'static dyn compress::CertCompressor>,
    },
}
