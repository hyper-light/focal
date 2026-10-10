#![allow(clippy::duplicate_mod)]

use alloc::boxed::Box;
use alloc::string::ToString;
use alloc::vec::Vec;
use alloc::{format, vec};
use aws_lc_rs::signature::{PqdsaKeyPair, PqdsaSigningAlgorithm};
use core::fmt::{self, Debug, Formatter};

use pki_types::{
    alg_id, AlgorithmIdentifier, PrivateKeyDer, PrivatePkcs8KeyDer, SubjectPublicKeyInfoDer,
};

use super::ring_like::rand::SystemRandom;
use super::ring_like::signature::{self, EcdsaKeyPair, Ed25519KeyPair, KeyPair, RsaKeyPair};
use crate::crypto::signer::{public_key_to_spki, Signer, SigningKey};
use crate::enums::{SignatureAlgorithm, SignatureScheme};
use crate::error::Error;

/// Parse `der` as any supported key encoding/type, returning
/// the first which works.
pub fn any_supported_type(der: &PrivateKeyDer<'_>) -> Result<Box<dyn SigningKey>, Error> {
    if let Ok(rsa) = RsaSigningKey::new(der) {
        return Ok(Box::new(rsa));
    }

    if let Ok(ecdsa) = any_ecdsa_type(der) {
        return Ok(ecdsa);
    }

    if let PrivateKeyDer::Pkcs8(pkcs8) = der {
        if let Ok(eddsa) = any_eddsa_type(pkcs8) {
            return Ok(eddsa);
        }

        if let Ok(pqdsa) = PqdsaSigningKey::from_pkcs8(pkcs8) {
            return Ok(Box::new(pqdsa));
        }
    }

    Err(Error::General(
        "failed to parse private key as RSA, ECDSA, or EdDSA".into(),
    ))
}

/// Parse `der` as any ECDSA key type, returning the first which works.
///
/// Both SEC1 (PEM section starting with 'BEGIN EC PRIVATE KEY') and PKCS8
/// (PEM section starting with 'BEGIN PRIVATE KEY') encodings are supported.
pub fn any_ecdsa_type(der: &PrivateKeyDer<'_>) -> Result<Box<dyn SigningKey>, Error> {
    if let Ok(ecdsa_p256) = EcdsaSigningKey::new(
        der,
        SignatureScheme::ECDSA_NISTP256_SHA256,
        &signature::ECDSA_P256_SHA256_ASN1_SIGNING,
    ) {
        return Ok(Box::new(ecdsa_p256));
    }

    if let Ok(ecdsa_p384) = EcdsaSigningKey::new(
        der,
        SignatureScheme::ECDSA_NISTP384_SHA384,
        &signature::ECDSA_P384_SHA384_ASN1_SIGNING,
    ) {
        return Ok(Box::new(ecdsa_p384));
    }

    if let Ok(ecdsa_p521) = EcdsaSigningKey::new(
        der,
        SignatureScheme::ECDSA_NISTP521_SHA512,
        &signature::ECDSA_P521_SHA512_ASN1_SIGNING,
    ) {
        return Ok(Box::new(ecdsa_p521));
    }

    Err(Error::General(
        "failed to parse ECDSA private key as PKCS#8 or SEC1".into(),
    ))
}

/// Parse `der` as any EdDSA key type, returning the first which works.
///
/// Note that, at the time of writing, Ed25519 does not have wide support
/// in browsers.  It is also not supported by the WebPKI, because the
/// CA/Browser Forum Baseline Requirements do not support it for publicly
/// trusted certificates.
pub fn any_eddsa_type(der: &PrivatePkcs8KeyDer<'_>) -> Result<Box<dyn SigningKey>, Error> {
    // TODO: Add support for Ed448
    Ok(Box::new(Ed25519SigningKey::new(
        der,
        SignatureScheme::ED25519,
    )?))
}

/// A `SigningKey` for RSA-PKCS1 or RSA-PSS.
///
/// This is used by the test suite, so it must be `pub`, but it isn't part of
/// the public, stable, API.
#[doc(hidden)]
pub struct RsaSigningKey {
    key: RsaKeyPair,
}

static ALL_RSA_SCHEMES: &[SignatureScheme] = &[
    SignatureScheme::RSA_PSS_SHA512,
    SignatureScheme::RSA_PSS_SHA384,
    SignatureScheme::RSA_PSS_SHA256,
    SignatureScheme::RSA_PKCS1_SHA512,
    SignatureScheme::RSA_PKCS1_SHA384,
    SignatureScheme::RSA_PKCS1_SHA256,
];

impl RsaSigningKey {
    /// Make a new `RsaSigningKey` from a DER encoding, in either
    /// PKCS#1 or PKCS#8 format.
    pub fn new(der: &PrivateKeyDer<'_>) -> Result<Self, Error> {
        let key_pair = match der {
            PrivateKeyDer::Pkcs1(pkcs1) => RsaKeyPair::from_der(pkcs1.secret_pkcs1_der()),
            PrivateKeyDer::Pkcs8(pkcs8) => RsaKeyPair::from_pkcs8(pkcs8.secret_pkcs8_der()),
            _ => {
                return Err(Error::General(
                    "failed to parse RSA private key as either PKCS#1 or PKCS#8".into(),
                ));
            }
        }
        .map_err(|key_rejected| {
            Error::General(format!("failed to parse RSA private key: {key_rejected}"))
        })?;

        Ok(Self { key: key_pair })
    }
}

impl SigningKey for RsaSigningKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn Signer + '_>> {
        ALL_RSA_SCHEMES
            .iter()
            .find(|scheme| offered.contains(scheme))
            .and_then(|scheme| RsaSigner::new(&self.key, *scheme))
    }

    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        Some(public_key_to_spki(
            &alg_id::RSA_ENCRYPTION,
            self.key.public_key(),
        ))
    }

    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::RSA
    }
}

impl Debug for RsaSigningKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RsaSigningKey")
            .field("algorithm", &self.algorithm())
            .finish()
    }
}

struct RsaSigner<'a> {
    key: &'a RsaKeyPair,
    scheme: SignatureScheme,
    encoding: &'static dyn signature::RsaEncoding,
}

impl<'a> RsaSigner<'a> {
    /// `None` for a scheme that is not RSA: `choose_scheme` offers only `ALL_RSA_SCHEMES`.
    fn new(key: &'a RsaKeyPair, scheme: SignatureScheme) -> Option<Box<dyn Signer + 'a>> {
        let encoding: &dyn signature::RsaEncoding = match scheme {
            SignatureScheme::RSA_PKCS1_SHA256 => &signature::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384 => &signature::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512 => &signature::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PSS_SHA256 => &signature::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384 => &signature::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512 => &signature::RSA_PSS_SHA512,
            _ => return None,
        };

        Some(Box::new(Self {
            key,
            scheme,
            encoding,
        }))
    }
}

impl Signer for RsaSigner<'_> {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error> {
        let mut sig = vec![0; self.key.public_modulus_len()];

        let rng = SystemRandom::new();
        self.key
            .sign(self.encoding, &rng, message, &mut sig)
            .map(|_| sig)
            .map_err(|_| Error::General("signing failed".to_string()))
    }

    fn scheme(&self) -> SignatureScheme {
        self.scheme
    }
}

impl Debug for RsaSigner<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RsaSigner")
            .field("scheme", &self.scheme)
            .finish()
    }
}

/// A SigningKey that uses exactly one TLS-level SignatureScheme
/// and one ring-level signature::SigningAlgorithm.
///
/// Compare this to RsaSigningKey, which for a particular key is
/// willing to sign with several algorithms.  This is quite poor
/// cryptography practice, but is necessary because a given RSA key
/// is expected to work in TLS1.2 (PKCS#1 signatures) and TLS1.3
/// (PSS signatures) -- nobody is willing to obtain certificates for
/// different protocol versions.
///
/// Currently this is only implemented for ECDSA keys.
struct EcdsaSigningKey {
    key: EcdsaKeyPair,
    scheme: SignatureScheme,
}

impl EcdsaSigningKey {
    /// Make a new `ECDSASigningKey` from a DER encoding in PKCS#8 or SEC1
    /// format, expecting a key usable with precisely the given signature
    /// scheme.
    fn new(
        der: &PrivateKeyDer<'_>,
        scheme: SignatureScheme,
        sigalg: &'static signature::EcdsaSigningAlgorithm,
    ) -> Result<Self, ()> {
        let key_pair = match der {
            PrivateKeyDer::Sec1(sec1) => {
                EcdsaKeyPair::from_private_key_der(sigalg, sec1.secret_sec1_der())
                    .map_err(|_| ())?
            }
            PrivateKeyDer::Pkcs8(pkcs8) => {
                EcdsaKeyPair::from_pkcs8(sigalg, pkcs8.secret_pkcs8_der()).map_err(|_| ())?
            }
            _ => return Err(()),
        };

        Ok(Self {
            key: key_pair,
            scheme,
        })
    }
}

impl SigningKey for EcdsaSigningKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn Signer + '_>> {
        if offered.contains(&self.scheme) {
            Some(Box::new(EcdsaSigner {
                key: &self.key,
                scheme: self.scheme,
            }))
        } else {
            None
        }
    }

    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        let id = match self.scheme {
            SignatureScheme::ECDSA_NISTP256_SHA256 => alg_id::ECDSA_P256,
            SignatureScheme::ECDSA_NISTP384_SHA384 => alg_id::ECDSA_P384,
            SignatureScheme::ECDSA_NISTP521_SHA512 => alg_id::ECDSA_P521,
            // An ECDSA key is made only for these three schemes.
            _ => return None,
        };

        Some(public_key_to_spki(&id, self.key.public_key()))
    }

    fn algorithm(&self) -> SignatureAlgorithm {
        self.scheme
            .algorithm()
            .unwrap_or(SignatureAlgorithm::Unknown(0))
    }
}

impl Debug for EcdsaSigningKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EcdsaSigningKey")
            .field("algorithm", &self.algorithm())
            .finish()
    }
}

struct EcdsaSigner<'a> {
    key: &'a EcdsaKeyPair,
    scheme: SignatureScheme,
}

impl Signer for EcdsaSigner<'_> {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error> {
        let rng = SystemRandom::new();
        self.key
            .sign(&rng, message)
            .map_err(|_| Error::General("signing failed".into()))
            .map(|sig| sig.as_ref().into())
    }

    fn scheme(&self) -> SignatureScheme {
        self.scheme
    }
}

impl Debug for EcdsaSigner<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EcdsaSigner")
            .field("scheme", &self.scheme)
            .finish()
    }
}

pub(crate) struct PqdsaSigningKey {
    kind: PqdsaKeyKind,
    inner: PqdsaKeyPair,
}

impl PqdsaSigningKey {
    pub(crate) fn from_pkcs8(pkcs8: &PrivatePkcs8KeyDer<'_>) -> Result<Self, Error> {
        for kind in PqdsaKeyKind::iter() {
            let Ok(key_pair) = PqdsaKeyPair::from_pkcs8(kind.to_alg(), pkcs8.secret_pkcs8_der())
            else {
                continue;
            };

            return Ok(Self {
                kind,
                inner: key_pair,
            });
        }

        Err(Error::General(
            "failed to parse private key as ML-DSA".into(),
        ))
    }
}

impl SigningKey for PqdsaSigningKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn Signer + '_>> {
        if !offered.contains(&self.kind.scheme()) {
            return None;
        }

        Some(Box::new(PqdsaSigner {
            key: &self.inner,
            kind: self.kind,
        }))
    }

    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        Some(public_key_to_spki(
            &self.kind.alg_id(),
            self.inner.public_key(),
        ))
    }

    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::Unknown(0)
    }
}

impl Debug for PqdsaSigningKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PqdsaSigningKey")
            .field("scheme", &self.kind.scheme())
            .finish_non_exhaustive()
    }
}

struct PqdsaSigner<'a> {
    key: &'a PqdsaKeyPair,
    kind: PqdsaKeyKind,
}

impl Signer for PqdsaSigner<'_> {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error> {
        let expected_sig_len = self.key.algorithm().signature_len();
        let mut sig = vec![0; expected_sig_len];
        let actual_sig_len = self
            .key
            .sign(message, &mut sig)
            .map_err(|_| Error::General("signing failed".into()))?;

        if actual_sig_len != expected_sig_len {
            return Err(Error::General("unexpected signature length".into()));
        }

        Ok(sig)
    }

    fn scheme(&self) -> SignatureScheme {
        self.kind.scheme()
    }
}

impl Debug for PqdsaSigner<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PqdsaSigner")
            .field("scheme", &self.kind.scheme())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
enum PqdsaKeyKind {
    MlDsa44,
    MlDsa65,
    MlDsa87,
}

impl PqdsaKeyKind {
    fn iter() -> impl Iterator<Item = Self> {
        [Self::MlDsa44, Self::MlDsa65, Self::MlDsa87].into_iter()
    }

    fn to_alg(self) -> &'static PqdsaSigningAlgorithm {
        match self {
            Self::MlDsa44 => &signature::ML_DSA_44_SIGNING,
            Self::MlDsa65 => &signature::ML_DSA_65_SIGNING,
            Self::MlDsa87 => &signature::ML_DSA_87_SIGNING,
        }
    }

    fn scheme(&self) -> SignatureScheme {
        match self {
            Self::MlDsa44 => SignatureScheme::ML_DSA_44,
            Self::MlDsa65 => SignatureScheme::ML_DSA_65,
            Self::MlDsa87 => SignatureScheme::ML_DSA_87,
        }
    }

    fn alg_id(&self) -> AlgorithmIdentifier {
        match self {
            Self::MlDsa44 => alg_id::ML_DSA_44,
            Self::MlDsa65 => alg_id::ML_DSA_65,
            Self::MlDsa87 => alg_id::ML_DSA_87,
        }
    }
}

/// A SigningKey that uses exactly one TLS-level SignatureScheme
/// and one ring-level signature::SigningAlgorithm.
///
/// Compare this to RsaSigningKey, which for a particular key is
/// willing to sign with several algorithms.  This is quite poor
/// cryptography practice, but is necessary because a given RSA key
/// is expected to work in TLS1.2 (PKCS#1 signatures) and TLS1.3
/// (PSS signatures) -- nobody is willing to obtain certificates for
/// different protocol versions.
///
/// Currently this is only implemented for Ed25519 keys.
struct Ed25519SigningKey {
    key: Ed25519KeyPair,
    scheme: SignatureScheme,
}

impl Ed25519SigningKey {
    /// Make a new `Ed25519SigningKey` from a DER encoding in PKCS#8 format,
    /// expecting a key usable with precisely the given signature scheme.
    fn new(der: &PrivatePkcs8KeyDer<'_>, scheme: SignatureScheme) -> Result<Self, Error> {
        match Ed25519KeyPair::from_pkcs8_maybe_unchecked(der.secret_pkcs8_der()) {
            Ok(key_pair) => Ok(Self {
                key: key_pair,
                scheme,
            }),
            Err(e) => Err(Error::General(format!(
                "failed to parse Ed25519 private key: {e}"
            ))),
        }
    }
}

impl SigningKey for Ed25519SigningKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn Signer + '_>> {
        if offered.contains(&self.scheme) {
            Some(Box::new(Ed25519Signer {
                key: &self.key,
                scheme: self.scheme,
            }))
        } else {
            None
        }
    }

    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        Some(public_key_to_spki(&alg_id::ED25519, self.key.public_key()))
    }

    fn algorithm(&self) -> SignatureAlgorithm {
        self.scheme
            .algorithm()
            .unwrap_or(SignatureAlgorithm::Unknown(0))
    }
}

impl Debug for Ed25519SigningKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ed25519SigningKey")
            .field("algorithm", &self.algorithm())
            .finish()
    }
}

struct Ed25519Signer<'a> {
    key: &'a Ed25519KeyPair,
    scheme: SignatureScheme,
}

impl Signer for Ed25519Signer<'_> {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(self.key.sign(message).as_ref().into())
    }

    fn scheme(&self) -> SignatureScheme {
        self.scheme
    }
}

impl Debug for Ed25519Signer<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ed25519Signer")
            .field("scheme", &self.scheme)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use alloc::format;

    use pki_types::{PrivatePkcs1KeyDer, PrivateSec1KeyDer};

    use super::*;
    use crate::pki_types::PrivateKeyDer;

    #[test]
    fn can_load_ecdsa_nistp256_pkcs8() {
        let key =
            PrivatePkcs8KeyDer::from(&include_bytes!("../../testdata/nistp256key.pkcs8.der")[..]);
        assert!(any_eddsa_type(&key).is_err());
        let key = PrivateKeyDer::Pkcs8(key);
        assert!(any_supported_type(&key).is_ok());
        assert!(any_ecdsa_type(&key).is_ok());
    }

    #[test]
    fn can_load_ecdsa_nistp256_sec1() {
        let key = PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(
            &include_bytes!("../../testdata/nistp256key.der")[..],
        ));
        assert!(any_supported_type(&key).is_ok());
        assert!(any_ecdsa_type(&key).is_ok());
    }

    #[test]
    fn can_sign_ecdsa_nistp256() {
        let key = PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(
            &include_bytes!("../../testdata/nistp256key.der")[..],
        ));

        let k = any_supported_type(&key).unwrap();
        assert_eq!(format!("{k:?}"), "EcdsaSigningKey { algorithm: ECDSA }");
        assert_eq!(k.algorithm(), SignatureAlgorithm::ECDSA);

        assert!(k
            .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
            .is_none());
        assert!(k
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP384_SHA384])
            .is_none());
        let s = k
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP256_SHA256])
            .unwrap();
        assert_eq!(
            format!("{s:?}"),
            "EcdsaSigner { scheme: ECDSA_NISTP256_SHA256 }"
        );
        assert_eq!(s.scheme(), SignatureScheme::ECDSA_NISTP256_SHA256);
        // nb. signature is variable length and asn.1-encoded
        assert!(s.sign(b"hello").unwrap().starts_with(&[0x30]));
    }

    #[test]
    fn can_load_ecdsa_nistp384_pkcs8() {
        let key =
            PrivatePkcs8KeyDer::from(&include_bytes!("../../testdata/nistp384key.pkcs8.der")[..]);
        assert!(any_eddsa_type(&key).is_err());
        let key = PrivateKeyDer::Pkcs8(key);
        assert!(any_supported_type(&key).is_ok());
        assert!(any_ecdsa_type(&key).is_ok());
    }

    #[test]
    fn can_load_ecdsa_nistp384_sec1() {
        let key = PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(
            &include_bytes!("../../testdata/nistp384key.der")[..],
        ));
        assert!(any_supported_type(&key).is_ok());
        assert!(any_ecdsa_type(&key).is_ok());
    }

    #[test]
    fn can_sign_ecdsa_nistp384() {
        let key = PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(
            &include_bytes!("../../testdata/nistp384key.der")[..],
        ));

        let k = any_supported_type(&key).unwrap();
        assert_eq!(format!("{k:?}"), "EcdsaSigningKey { algorithm: ECDSA }");
        assert_eq!(k.algorithm(), SignatureAlgorithm::ECDSA);

        assert!(k
            .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
            .is_none());
        assert!(k
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP256_SHA256])
            .is_none());
        let s = k
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP384_SHA384])
            .unwrap();
        assert_eq!(
            format!("{s:?}"),
            "EcdsaSigner { scheme: ECDSA_NISTP384_SHA384 }"
        );
        assert_eq!(s.scheme(), SignatureScheme::ECDSA_NISTP384_SHA384);
        // nb. signature is variable length and asn.1-encoded
        assert!(s.sign(b"hello").unwrap().starts_with(&[0x30]));
    }

    #[test]
    fn can_load_ecdsa_nistp521_pkcs8() {
        let key =
            PrivatePkcs8KeyDer::from(&include_bytes!("../../testdata/nistp521key.pkcs8.der")[..]);
        assert!(any_eddsa_type(&key).is_err());
        let key = PrivateKeyDer::Pkcs8(key);
        assert!(any_supported_type(&key).is_ok());
        assert!(any_ecdsa_type(&key).is_ok());
    }

    #[test]
    fn can_load_ecdsa_nistp521_sec1() {
        let key = PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(
            &include_bytes!("../../testdata/nistp521key.der")[..],
        ));
        assert!(any_supported_type(&key).is_ok());
        assert!(any_ecdsa_type(&key).is_ok());
    }

    #[test]
    fn can_sign_ecdsa_nistp521() {
        let key = PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(
            &include_bytes!("../../testdata/nistp521key.der")[..],
        ));

        let k = any_supported_type(&key).unwrap();
        assert_eq!(format!("{k:?}"), "EcdsaSigningKey { algorithm: ECDSA }");
        assert_eq!(k.algorithm(), SignatureAlgorithm::ECDSA);

        assert!(k
            .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
            .is_none());
        assert!(k
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP256_SHA256])
            .is_none());
        assert!(k
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP384_SHA384])
            .is_none());
        let s = k
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP521_SHA512])
            .unwrap();
        assert_eq!(
            format!("{s:?}"),
            "EcdsaSigner { scheme: ECDSA_NISTP521_SHA512 }"
        );
        assert_eq!(s.scheme(), SignatureScheme::ECDSA_NISTP521_SHA512);
        // nb. signature is variable length and asn.1-encoded
        assert!(s.sign(b"hello").unwrap().starts_with(&[0x30]));
    }

    #[test]
    fn can_load_eddsa_pkcs8() {
        let key = PrivatePkcs8KeyDer::from(&include_bytes!("../../testdata/eddsakey.der")[..]);
        assert!(any_eddsa_type(&key).is_ok());
        let key = PrivateKeyDer::Pkcs8(key);
        assert!(any_supported_type(&key).is_ok());
        assert!(any_ecdsa_type(&key).is_err());
    }

    #[test]
    fn can_sign_eddsa() {
        let key = PrivatePkcs8KeyDer::from(&include_bytes!("../../testdata/eddsakey.der")[..]);

        let k = any_eddsa_type(&key).unwrap();
        assert_eq!(format!("{k:?}"), "Ed25519SigningKey { algorithm: ED25519 }");
        assert_eq!(k.algorithm(), SignatureAlgorithm::ED25519);

        assert!(k
            .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
            .is_none());
        assert!(k
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP256_SHA256])
            .is_none());
        let s = k.choose_scheme(&[SignatureScheme::ED25519]).unwrap();
        assert_eq!(format!("{s:?}"), "Ed25519Signer { scheme: ED25519 }");
        assert_eq!(s.scheme(), SignatureScheme::ED25519);
        assert_eq!(s.sign(b"hello").unwrap().len(), 64);
    }

    #[test]
    fn can_load_rsa2048_pkcs8() {
        let key =
            PrivatePkcs8KeyDer::from(&include_bytes!("../../testdata/rsa2048key.pkcs8.der")[..]);
        assert!(any_eddsa_type(&key).is_err());
        let key = PrivateKeyDer::Pkcs8(key);
        assert!(any_supported_type(&key).is_ok());
        assert!(any_ecdsa_type(&key).is_err());
    }

    #[test]
    fn can_load_rsa2048_pkcs1() {
        let key = PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(
            &include_bytes!("../../testdata/rsa2048key.pkcs1.der")[..],
        ));
        assert!(any_supported_type(&key).is_ok());
        assert!(any_ecdsa_type(&key).is_err());
    }

    #[test]
    fn can_sign_rsa2048() {
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            &include_bytes!("../../testdata/rsa2048key.pkcs8.der")[..],
        ));

        let k = any_supported_type(&key).unwrap();
        assert_eq!(format!("{k:?}"), "RsaSigningKey { algorithm: RSA }");
        assert_eq!(k.algorithm(), SignatureAlgorithm::RSA);

        assert!(k
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP256_SHA256])
            .is_none());
        assert!(k.choose_scheme(&[SignatureScheme::ED25519]).is_none());

        let s = k.choose_scheme(&[SignatureScheme::RSA_PSS_SHA256]).unwrap();
        assert_eq!(format!("{s:?}"), "RsaSigner { scheme: RSA_PSS_SHA256 }");
        assert_eq!(s.scheme(), SignatureScheme::RSA_PSS_SHA256);
        assert_eq!(s.sign(b"hello").unwrap().len(), 256);

        for scheme in &[
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
        ] {
            k.choose_scheme(&[*scheme]).unwrap();
        }
    }

    #[test]
    fn cannot_load_invalid_pkcs8_encoding() {
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(&b"invalid"[..]));
        assert_eq!(
            any_supported_type(&key).err(),
            Some(Error::General(
                "failed to parse private key as RSA, ECDSA, or EdDSA".into()
            ))
        );
        assert_eq!(
            any_ecdsa_type(&key).err(),
            Some(Error::General(
                "failed to parse ECDSA private key as PKCS#8 or SEC1".into()
            ))
        );
        assert_eq!(
            RsaSigningKey::new(&key).err(),
            Some(Error::General(
                "failed to parse RSA private key: InvalidEncoding".into()
            ))
        );
    }
}
