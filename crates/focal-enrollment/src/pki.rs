use crate::{files::PrivateDirectory, *};
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, PublicKeyData,
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use std::path::Path;
use time::OffsetDateTime;
use x509_parser::prelude::{FromDer, X509Certificate, X509CertificationRequest};
use zeroize::{Zeroize, Zeroizing};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SecretBytes(pub(crate) Vec<u8>);
impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Serialize, Deserialize)]
struct AuthorityBundle {
    schema: u16,
    cluster: ClusterId,
    names: Vec<String>,
    ca: Vec<u8>,
    ca_key: SecretBytes,
    server: Vec<u8>,
    server_key: SecretBytes,
    /// The bootstrap server certificate staged to succeed `server` (24
    /// §11): issued, committed in the registry, presented once every
    /// invitation issued before it was staged has closed.
    successor: Option<StagedServer>,
    /// The endorsement of `ca` by the issuer it succeeded (24 §11): a CA
    /// certificate for its key and name under the predecessor's signature,
    /// presented in every chain so a verifier holding the predecessor alone
    /// accepts it. None for the genesis issuer.
    ca_endorsement: Option<Vec<u8>>,
    /// An issuer staged to succeed `ca`: its key, its self-signed
    /// certificate and `ca`'s endorsement of it, until the registry commits
    /// the activation and `activate_issuer` adopts it.
    issuer_successor: Option<StagedIssuerMaterial>,
    /// The issuer `ca` succeeded, while the bootstrap server certificate it
    /// issued is still presented: the chain behind that certificate.
    retired_issuer: Option<RetiredIssuer>,
    /// The genesis issuer's certificate: the cluster's identity (24 §11),
    /// what every identity check compares, unchanged by any succession.
    identity: Vec<u8>,
}
/// The bundle as schema 2 wrote it, before an issuer could be staged.
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct AuthorityBundleV2 {
    schema: u16,
    cluster: ClusterId,
    names: Vec<String>,
    ca: Vec<u8>,
    ca_key: SecretBytes,
    server: Vec<u8>,
    server_key: SecretBytes,
    successor: Option<StagedServer>,
}
#[derive(Serialize, Deserialize)]
struct StagedIssuerMaterial {
    certificate: Vec<u8>,
    key: SecretBytes,
    endorsement: Vec<u8>,
    staged_at: i64,
}
#[derive(Serialize, Deserialize)]
struct RetiredIssuer {
    certificate: Vec<u8>,
    endorsement: Option<Vec<u8>>,
}
/// The bundle as schema 1 wrote it, before a successor could be staged.
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct AuthorityBundleV1 {
    schema: u16,
    cluster: ClusterId,
    names: Vec<String>,
    ca: Vec<u8>,
    ca_key: SecretBytes,
    server: Vec<u8>,
    server_key: SecretBytes,
}
#[derive(Serialize, Deserialize)]
struct StagedServer {
    certificate: Vec<u8>,
    key: SecretBytes,
    staged_at: i64,
}
const AUTHORITY_SCHEMA: u16 = 3;
/// How an issuer's certificate names it: the cluster, and for a successor
/// the identity of its key, so two issuers of one cluster never share a
/// subject and a chain names the one it was issued under.
fn issuer_name(cluster: &ClusterId, key: Option<&KeyPair>) -> String {
    match key {
        None => format!("Focal cluster {}", hex(cluster)),
        Some(key) => format!(
            "Focal cluster {} issuer {}",
            hex(cluster),
            hex(&blake3::hash(&key.subject_public_key_info()).as_bytes()[..8])
        ),
    }
}
/// The parameters of an issuer's certificate: a CA for `lifetime` seconds
/// that signs credentials and nothing below them.
fn issuer_params(
    cluster: &ClusterId,
    key: Option<&KeyPair>,
    now: i64,
    lifetime: u64,
) -> Result<CertificateParams, EnrollmentError> {
    let mut params = params(vec![], now, lifetime)?;
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
        .distinguished_name
        .push(DnType::CommonName, issuer_name(cluster, key));
    Ok(params)
}
/// When a certificate was issued and when it expires, in Unix seconds.
pub fn certificate_validity(certificate: &[u8]) -> Result<(i64, i64), EnrollmentError> {
    if certificate.len() > 4096 {
        return Err(EnrollmentError::Capacity);
    }
    let (_, parsed) =
        X509Certificate::from_der(certificate).map_err(|_| EnrollmentError::Corrupt)?;
    let validity = parsed.validity();
    // `params` sets `not_before` a minute before the issue.
    let issued_at = validity
        .not_before
        .timestamp()
        .checked_add(60)
        .ok_or(EnrollmentError::Corrupt)?;
    Ok((issued_at, validity.not_after.timestamp()))
}

/// CA custody is deliberately separate from replicated public enrollment
/// metadata. Only a node operator holding this private directory may issue.
pub struct BootstrapAuthority {
    bundle: AuthorityBundle,
    key: KeyPair,
    _directory: PrivateDirectory,
}
impl BootstrapAuthority {
    /// Open the authority, or create it under the standard policy: the
    /// bootstrap server certificate for the standard credential lifetime
    /// and the issuer for the standard issuer lifetime (24 §11) — what a
    /// founding without a committed policy of its own takes.
    pub fn open_or_create(
        path: impl AsRef<Path>,
        cluster: ClusterId,
        server_names: Vec<String>,
        now: i64,
    ) -> Result<Self, EnrollmentError> {
        let standard = crate::registry::EnrollmentLimits::default();
        Self::open_or_create_for(
            path,
            cluster,
            server_names,
            standard.credential_lifetime,
            standard.issuer_lifetime,
            now,
        )
    }
    /// Open the authority, or create it: a self-signed CA issued for
    /// `issuer_lifetime` seconds and the bootstrap server certificate the
    /// enrollment endpoint presents, issued for `lifetime` seconds — the
    /// cluster's credential lifetime, the one lifetime for everything the
    /// issuer issues (24 §11).
    pub fn open_or_create_for(
        path: impl AsRef<Path>,
        cluster: ClusterId,
        server_names: Vec<String>,
        lifetime: u64,
        issuer_lifetime: u64,
        now: i64,
    ) -> Result<Self, EnrollmentError> {
        if cluster == [0; 16]
            || server_names.is_empty()
            || server_names.len() > 16
            || server_names.iter().any(|n| n.is_empty() || n.len() > 253)
            || lifetime == 0
            || issuer_lifetime < lifetime
        {
            return Err(EnrollmentError::Invalid);
        }
        let directory = PrivateDirectory::open(path.as_ref())?;
        let bundle: AuthorityBundle = if let Some(bytes) = directory.read("authority.bin")? {
            let (schema, _) = postcard::take_from_bytes::<u16>(&bytes)?;
            if schema == 1 {
                let legacy: AuthorityBundleV1 = decode(&bytes)?;
                if legacy.schema != 1 {
                    return Err(EnrollmentError::Corrupt);
                }
                AuthorityBundle {
                    schema: AUTHORITY_SCHEMA,
                    cluster: legacy.cluster,
                    names: legacy.names,
                    identity: legacy.ca.clone(),
                    ca: legacy.ca,
                    ca_key: legacy.ca_key,
                    server: legacy.server,
                    server_key: legacy.server_key,
                    successor: None,
                    ca_endorsement: None,
                    issuer_successor: None,
                    retired_issuer: None,
                }
            } else if schema == 2 {
                let legacy: AuthorityBundleV2 = decode(&bytes)?;
                if legacy.schema != 2 {
                    return Err(EnrollmentError::Corrupt);
                }
                AuthorityBundle {
                    schema: AUTHORITY_SCHEMA,
                    cluster: legacy.cluster,
                    names: legacy.names,
                    identity: legacy.ca.clone(),
                    ca: legacy.ca,
                    ca_key: legacy.ca_key,
                    server: legacy.server,
                    server_key: legacy.server_key,
                    successor: legacy.successor,
                    ca_endorsement: None,
                    issuer_successor: None,
                    retired_issuer: None,
                }
            } else {
                decode(&bytes)?
            }
        } else {
            let key = KeyPair::generate()?;
            let ca = issuer_params(&cluster, None, now, issuer_lifetime)?.self_signed(&key)?;
            let issuer = Issuer::from_ca_cert_der(ca.der(), &key)?;
            let server_key = KeyPair::generate()?;
            let mut server_params = params(server_names.clone(), now, lifetime)?;
            server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            let server = server_params.signed_by(&server_key, &issuer)?;
            let bundle = AuthorityBundle {
                schema: AUTHORITY_SCHEMA,
                cluster,
                names: server_names.clone(),
                ca: ca.der().to_vec(),
                ca_key: SecretBytes(key.serialize_der()),
                server: server.der().to_vec(),
                server_key: SecretBytes(server_key.serialize_der()),
                successor: None,
                ca_endorsement: None,
                issuer_successor: None,
                retired_issuer: None,
                identity: ca.der().to_vec(),
            };
            let bytes = Zeroizing::new(encode(&bundle)?);
            directory.install_new("authority.bin", &bytes)?;
            bundle
        };
        if bundle.cluster != cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        if bundle.schema != AUTHORITY_SCHEMA || bundle.names != server_names {
            return Err(EnrollmentError::Invalid);
        }
        let key = KeyPair::try_from(bundle.ca_key.0.as_slice())?;
        let (_, ca) =
            X509Certificate::from_der(&bundle.ca).map_err(|_| EnrollmentError::Corrupt)?;
        if ca.public_key().raw != key.subject_public_key_info() {
            return Err(EnrollmentError::Corrupt);
        }
        // The identity is the genesis issuer: the issuer itself until it
        // succeeded itself, a certificate of its own after.
        let (rest, _) =
            X509Certificate::from_der(&bundle.identity).map_err(|_| EnrollmentError::Corrupt)?;
        if !rest.is_empty() || (bundle.identity != bundle.ca && bundle.ca_endorsement.is_none()) {
            return Err(EnrollmentError::Corrupt);
        }
        let server_key = KeyPair::try_from(bundle.server_key.0.as_slice())?;
        let (_, server) =
            X509Certificate::from_der(&bundle.server).map_err(|_| EnrollmentError::Corrupt)?;
        if server.public_key().raw != server_key.subject_public_key_info() {
            return Err(EnrollmentError::Corrupt);
        }
        // The bootstrap server certificate chains to the issuer, or to the
        // one the issuer succeeded while it is still presented.
        let server_verified = match &bundle.retired_issuer {
            Some(retired)
                if !focal_wire::issued_by(&bundle.server, &bundle.ca)
                    .map_err(|_| EnrollmentError::Corrupt)? =>
            {
                let (_, retired_ca) = X509Certificate::from_der(&retired.certificate)
                    .map_err(|_| EnrollmentError::Corrupt)?;
                if let Some(endorsement) = &retired.endorsement
                    && !endorses(endorsement, &retired.certificate)?
                {
                    return Err(EnrollmentError::Corrupt);
                }
                server
                    .verify_signature(Some(retired_ca.public_key()))
                    .is_ok()
            }
            _ => server.verify_signature(Some(ca.public_key())).is_ok(),
        };
        if !server_verified {
            return Err(EnrollmentError::Corrupt);
        }
        if let Some(endorsement) = &bundle.ca_endorsement
            && !endorses(endorsement, &bundle.ca)?
        {
            return Err(EnrollmentError::Corrupt);
        }
        if let Some(staged) = &bundle.issuer_successor {
            let staged_key = KeyPair::try_from(staged.key.0.as_slice())?;
            let (_, certificate) = X509Certificate::from_der(&staged.certificate)
                .map_err(|_| EnrollmentError::Corrupt)?;
            if certificate.public_key().raw != staged_key.subject_public_key_info()
                || staged.staged_at <= 0
                || !certificate.is_ca()
                || !endorses(&staged.endorsement, &staged.certificate)?
                || !focal_wire::issued_by(&staged.endorsement, &bundle.ca)
                    .map_err(|_| EnrollmentError::Corrupt)?
            {
                return Err(EnrollmentError::Corrupt);
            }
        }
        if let Some(staged) = &bundle.successor {
            let staged_key = KeyPair::try_from(staged.key.0.as_slice())?;
            let (_, certificate) = X509Certificate::from_der(&staged.certificate)
                .map_err(|_| EnrollmentError::Corrupt)?;
            if certificate.public_key().raw != staged_key.subject_public_key_info()
                || staged.staged_at <= 0
            {
                return Err(EnrollmentError::Corrupt);
            }
            certificate
                .verify_signature(Some(ca.public_key()))
                .map_err(|_| EnrollmentError::Corrupt)?;
        }
        Ok(Self {
            bundle,
            key,
            _directory: directory,
        })
    }
    fn save(&self) -> Result<(), EnrollmentError> {
        let bytes = Zeroizing::new(encode(&self.bundle)?);
        self._directory.replace("authority.bin", &bytes)
    }
    /// Write the bundle as schema 1 wrote it, for the upgrade test.
    #[cfg(test)]
    pub(crate) fn save_as_schema_one_for_tests(&self) -> Result<(), EnrollmentError> {
        if self.bundle.ca_endorsement.is_some() || self.bundle.retired_issuer.is_some() {
            return Err(EnrollmentError::Invalid);
        }
        if self.bundle.successor.is_some() {
            return Err(EnrollmentError::Invalid);
        }
        let legacy = AuthorityBundleV1 {
            schema: 1,
            cluster: self.bundle.cluster,
            names: self.bundle.names.clone(),
            ca: self.bundle.ca.clone(),
            ca_key: SecretBytes(self.bundle.ca_key.0.clone()),
            server: self.bundle.server.clone(),
            server_key: SecretBytes(self.bundle.server_key.0.clone()),
        };
        let bytes = Zeroizing::new(encode(&legacy)?);
        self._directory.replace("authority.bin", &bytes)
    }
    /// When the bootstrap server certificate was issued and when it expires.
    pub fn server_validity(&self) -> Result<(i64, i64), EnrollmentError> {
        certificate_validity(&self.bundle.server)
    }
    /// The bootstrap server certificate staged to succeed the current one,
    /// and when it was staged.
    pub fn successor(&self) -> Option<(&[u8], i64)> {
        self.bundle
            .successor
            .as_ref()
            .map(|staged| (staged.certificate.as_slice(), staged.staged_at))
    }
    /// Stage a successor to the bootstrap server certificate: a fresh key
    /// and a certificate for the same names under the CA, issued for
    /// `lifetime` seconds, kept beside the current one until activated (24
    /// §11). A successor already staged is answered as it is.
    pub fn stage_successor(&mut self, now: i64, lifetime: u64) -> Result<Vec<u8>, EnrollmentError> {
        if let Some(staged) = &self.bundle.successor {
            return Ok(staged.certificate.clone());
        }
        if lifetime == 0 || now <= 0 {
            return Err(EnrollmentError::Invalid);
        }
        let issuer =
            Issuer::from_ca_cert_der(&CertificateDer::from(self.bundle.ca.as_slice()), &self.key)?;
        let key = KeyPair::generate()?;
        let mut server_params = params(self.bundle.names.clone(), now, lifetime)?;
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let certificate = server_params.signed_by(&key, &issuer)?.der().to_vec();
        self.bundle.successor = Some(StagedServer {
            certificate: certificate.clone(),
            key: SecretBytes(key.serialize_der()),
            staged_at: now,
        });
        self.save()?;
        Ok(certificate)
    }
    /// Present the staged successor from now on: it becomes the bootstrap
    /// server certificate and the one it succeeds is dropped. Refused when
    /// nothing is staged.
    pub fn activate_successor(&mut self) -> Result<CredentialMaterial, EnrollmentError> {
        let staged = self
            .bundle
            .successor
            .take()
            .ok_or(EnrollmentError::NotCommitted)?;
        self.bundle.server = staged.certificate;
        self.bundle.server_key = staged.key;
        self.drop_retired_issuer();
        self.save()?;
        Ok(self.server_identity())
    }
    /// The bootstrap server certificate presented from now on is the staged
    /// successor, and it is issued by the current issuer: the issuer the
    /// current one succeeded is presented no more.
    fn drop_retired_issuer(&mut self) {
        if focal_wire::issued_by(&self.bundle.server, &self.bundle.ca).unwrap_or(false) {
            self.bundle.retired_issuer = None;
        }
    }
    /// The issuer as the registry records it: its certificate and the
    /// endorsement of the issuer it succeeded.
    pub fn issuer_record(&self) -> Result<IssuerRecord, EnrollmentError> {
        IssuerRecord::of(&self.bundle.ca, self.bundle.ca_endorsement.as_deref())
    }
    /// The issuer staged to succeed the current one, as the registry would
    /// record it, with when it was staged.
    pub fn issuer_successor(&self) -> Result<Option<IssuerRecord>, EnrollmentError> {
        self.bundle
            .issuer_successor
            .as_ref()
            .map(|staged| IssuerRecord::of(&staged.certificate, Some(&staged.endorsement)))
            .transpose()
    }
    /// The issuers as this authority holds them: the one issuing, one
    /// staged, and the one succeeded while the bootstrap server certificate
    /// it issued is still presented. The registry's committed set is the
    /// trust; this is what the founder completes its own credentials with
    /// before the registry runs, and what a test holds.
    pub fn issuers(&self) -> Result<IssuerSuccession, EnrollmentError> {
        Ok(IssuerSuccession {
            current: self.issuer_record()?,
            successor: match (&self.bundle.issuer_successor, self.issuer_successor()?) {
                (Some(staged), Some(record)) => Some(StagedIssuer {
                    record,
                    staged_at: staged.staged_at,
                }),
                _ => None,
            },
            retiring: self
                .bundle
                .retired_issuer
                .as_ref()
                .map(|retired| {
                    IssuerRecord::of(&retired.certificate, retired.endorsement.as_deref())
                })
                .transpose()?,
        })
    }
    /// When the issuer was issued and when it expires.
    pub fn issuer_validity(&self) -> Result<(i64, i64), EnrollmentError> {
        certificate_validity(&self.bundle.ca)
    }
    /// Stage a successor to the issuer (24 §11): a fresh key, its
    /// self-signed certificate for `lifetime` seconds, and the current
    /// issuer's endorsement of it — a CA certificate for the same key and
    /// name under the current issuer's signature. Kept beside the current
    /// issuer until the registry commits the activation. A successor
    /// already staged is answered as it is; refused while the issuer the
    /// current one succeeded is still presented behind the bootstrap
    /// server certificate.
    pub fn stage_issuer(
        &mut self,
        now: i64,
        lifetime: u64,
    ) -> Result<IssuerRecord, EnrollmentError> {
        if let Some(staged) = &self.bundle.issuer_successor {
            return IssuerRecord::of(&staged.certificate, Some(&staged.endorsement));
        }
        if lifetime == 0 || now <= 0 {
            return Err(EnrollmentError::Invalid);
        }
        if self.bundle.retired_issuer.is_some() {
            return Err(EnrollmentError::Conflict);
        }
        let key = KeyPair::generate()?;
        let params = issuer_params(&self.bundle.cluster, Some(&key), now, lifetime)?;
        let certificate = params.self_signed(&key)?.der().to_vec();
        let issuer =
            Issuer::from_ca_cert_der(&CertificateDer::from(self.bundle.ca.as_slice()), &self.key)?;
        let endorsement = issuer_params(&self.bundle.cluster, Some(&key), now, lifetime)?
            .signed_by(&key, &issuer)?
            .der()
            .to_vec();
        let record = IssuerRecord::of(&certificate, Some(&endorsement))?;
        self.bundle.issuer_successor = Some(StagedIssuerMaterial {
            certificate,
            key: SecretBytes(key.serialize_der()),
            endorsement,
            staged_at: now,
        });
        self.save()?;
        Ok(record)
    }
    /// Issue under the staged successor from now on: it becomes the issuer,
    /// with its endorsement; the issuer it succeeds stays behind the
    /// bootstrap server certificate it issued until that certificate
    /// succeeds itself. Refused when nothing is staged.
    pub fn activate_issuer(&mut self) -> Result<(), EnrollmentError> {
        let staged = self
            .bundle
            .issuer_successor
            .take()
            .ok_or(EnrollmentError::NotCommitted)?;
        let key = KeyPair::try_from(staged.key.0.as_slice())?;
        let retired = RetiredIssuer {
            certificate: std::mem::replace(&mut self.bundle.ca, staged.certificate),
            endorsement: self.bundle.ca_endorsement.replace(staged.endorsement),
        };
        self.bundle.ca_key = staged.key;
        self.bundle.retired_issuer = Some(retired);
        self.key = key;
        self.drop_retired_issuer();
        self.save()
    }
    pub fn cluster(&self) -> ClusterId {
        self.bundle.cluster
    }
    /// The genesis issuer's certificate: the cluster's identity, what the
    /// registry, every trust and the authority anchor name as `ca_certificate`
    /// and compare by; never the issuer issuing now (`issuer_certificate`).
    pub fn ca_certificate(&self) -> &[u8] {
        &self.bundle.identity
    }
    /// The certificate of the issuer issuing now (24 §11): the genesis
    /// issuer's until it succeeded itself.
    pub fn issuer_certificate(&self) -> &[u8] {
        &self.bundle.ca
    }
    pub fn server_certificate(&self) -> &[u8] {
        &self.bundle.server
    }
    /// The chain behind the bootstrap server certificate: the issuer that
    /// issued it — the current one, or the one it succeeded while this
    /// certificate is still presented — and that issuer's endorsement.
    pub fn server_identity(&self) -> CredentialMaterial {
        let mut certificate_chain = vec![self.bundle.server.clone()];
        match &self.bundle.retired_issuer {
            Some(retired)
                if !focal_wire::issued_by(&self.bundle.server, &self.bundle.ca)
                    .unwrap_or(false) =>
            {
                certificate_chain.push(retired.certificate.clone());
                certificate_chain.extend(retired.endorsement.clone());
            }
            _ => {
                certificate_chain.push(self.bundle.ca.clone());
                certificate_chain.extend(self.bundle.ca_endorsement.clone());
            }
        }
        CredentialMaterial {
            certificate_chain,
            private_key: Zeroizing::new(self.bundle.server_key.0.clone()),
        }
    }
    pub(crate) fn issue(
        &self,
        csr: &[u8],
        identity: &AssignedIdentity,
        now: i64,
        lifetime: u64,
    ) -> Result<Vec<u8>, EnrollmentError> {
        self.issue_identity(csr, identity, now, lifetime, PrincipalSubject::Derived)
    }
    pub(crate) fn issue_founder(
        &self,
        csr: &[u8],
        identity: &AssignedIdentity,
        now: i64,
        lifetime: u64,
    ) -> Result<Vec<u8>, EnrollmentError> {
        self.issue_identity(csr, identity, now, lifetime, PrincipalSubject::Founding)
    }
    /// Issue for a key that did not derive the identity's principal: the
    /// principal was assigned to the key this enrollment began with and is
    /// carried to `csr`'s key by a rotation the previous key authorized.
    pub(crate) fn issue_carried(
        &self,
        csr: &[u8],
        identity: &AssignedIdentity,
        now: i64,
        lifetime: u64,
    ) -> Result<Vec<u8>, EnrollmentError> {
        self.issue_identity(csr, identity, now, lifetime, PrincipalSubject::Carried)
    }
    fn issue_identity(
        &self,
        csr: &[u8],
        identity: &AssignedIdentity,
        now: i64,
        lifetime: u64,
        subject: PrincipalSubject,
    ) -> Result<Vec<u8>, EnrollmentError> {
        let mut request = verified_csr(csr)?;
        // A CSR proves control of a key only. Ignore every requested subject,
        // SAN, CA bit, usage and extension; authorization supplies all names.
        request.params = params(vec![identity.server_name.clone()], now, lifetime)?;
        request
            .params
            .distinguished_name
            .push(DnType::CommonName, identity.server_name.clone());
        match subject {
            PrincipalSubject::Derived => {}
            PrincipalSubject::Founding => {
                // A CA-signed subject binds the original local principal to
                // this one genesis identity. Ordinary CSR issuance cannot
                // request it.
                request.params.distinguished_name.push(
                    DnType::OrganizationalUnitName,
                    format!("focal-genesis-principal:{}", hex(&identity.principal)),
                );
            }
            PrincipalSubject::Carried => {
                // A CA-signed subject binds a principal another key of the
                // same enrollment derived to this rotated key.
                request.params.distinguished_name.push(
                    DnType::OrganizationalUnitName,
                    format!("focal-carried-principal:{}", hex(&identity.principal)),
                );
            }
        }
        request.params.extended_key_usages = match identity.role {
            EnrollmentRole::Node => vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ],
            EnrollmentRole::Client => vec![ExtendedKeyUsagePurpose::ClientAuth],
        };
        let issuer =
            Issuer::from_ca_cert_der(&CertificateDer::from(self.bundle.ca.as_slice()), &self.key)?;
        Ok(request.signed_by(&issuer)?.der().to_vec())
    }
}

/// How a certificate's subject accounts for the principal it names.
#[derive(Clone, Copy)]
enum PrincipalSubject {
    /// The principal is derived from the certificate's own key.
    Derived,
    /// The founder's original principal, bound at genesis.
    Founding,
    /// A principal derived by an earlier key of the same enrollment,
    /// carried to this key by a rotation.
    Carried,
}

#[derive(Clone)]
pub struct CredentialMaterial {
    certificate_chain: Vec<Vec<u8>>,
    private_key: Zeroizing<Vec<u8>>,
}
impl CredentialMaterial {
    pub fn certificate_chain(&self) -> &[Vec<u8>] {
        &self.certificate_chain
    }
    /// Explicit secret export for the node-owned TLS adapter; never log this.
    pub fn private_key_der(&self) -> Zeroizing<Vec<u8>> {
        self.private_key.clone()
    }
    pub fn server_config(&self) -> Result<rustls::ServerConfig, EnrollmentError> {
        // rustls retains this shared provider inside its TLS configuration.
        let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut config = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| EnrollmentError::Crypto)?
            .with_no_client_auth()
            .with_single_cert(
                self.certificate_chain
                    .iter()
                    .cloned()
                    .map(CertificateDer::from)
                    .collect(),
                PrivatePkcs8KeyDer::from(self.private_key.to_vec()).into(),
            )
            .map_err(|_| EnrollmentError::Crypto)?;
        config.alpn_protocols = vec![ENROLLMENT_ALPN.to_vec()];
        config.max_early_data_size = 0;
        Ok(config)
    }
}

#[derive(Serialize, Deserialize)]
struct JoinKeyBundle {
    schema: u16,
    cluster: ClusterId,
    request: JoinId,
    csr: Vec<u8>,
    key: SecretBytes,
}
/// The joining process saves its key and request identity before its first
/// network attempt. Recovery reuses exact CSR bytes and the same request ID.
pub struct JoinKey {
    bundle: JoinKeyBundle,
    _directory: PrivateDirectory,
}
impl JoinKey {
    /// Verify already installed joining material without opening a directory,
    /// generating a key, or repairing persistence. Inputs are decoded private
    /// record payloads, bounded by the existing enrollment message limit.
    /// This proves the saved identity binding, not current registry activation.
    pub fn inspect_saved<'a>(
        key_bytes: &[u8],
        receipt_bytes: &[u8],
        cluster: ClusterId,
        issuers: impl IntoIterator<Item = &'a IssuerRecord>,
        now: i64,
    ) -> Result<(EnrollmentReceipt, Fingerprint), EnrollmentError> {
        let bundle: JoinKeyBundle = decode(key_bytes)?;
        let receipt: EnrollmentReceipt = decode(receipt_bytes)?;
        if cluster == [0; 16] || bundle.cluster != cluster || receipt.identity.cluster != cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        if bundle.schema != 1 || bundle.request == [0; 16] {
            return Err(EnrollmentError::Corrupt);
        }
        let csr = verified_csr(&bundle.csr)?;
        let key = KeyPair::try_from(bundle.key.0.as_slice())?;
        if key.subject_public_key_info() != csr.public_key.subject_public_key_info()
            || receipt.request != bundle.request
            || receipt.csr_hash != hash("focal.enrollment.csr.v1", &bundle.csr)
            || receipt.public_key != csr_key_hash(&bundle.csr)?
            || receipt.issued_at > now
            || receipt.expires_at <= now
        {
            return Err(EnrollmentError::Unauthorized);
        }
        verify_issued(
            &receipt,
            issuers
                .into_iter()
                .map(|issuer| issuer.certificate.as_slice()),
        )?;
        if !identity_bound(&receipt)? {
            return Err(EnrollmentError::Unauthorized);
        }
        Ok((receipt, *blake3::hash(&bundle.csr).as_bytes()))
    }
    /// Open existing joining material for reading beside other readers; the
    /// enrollment cannot be completed through this handle.
    pub fn open_shared(
        path: impl AsRef<Path>,
        cluster: ClusterId,
    ) -> Result<Self, EnrollmentError> {
        if cluster == [0; 16] {
            return Err(EnrollmentError::Invalid);
        }
        let directory = PrivateDirectory::open_shared(path.as_ref())?;
        let bundle: JoinKeyBundle = decode(
            &directory
                .read("join-key.bin")?
                .ok_or(EnrollmentError::Corrupt)?,
        )?;
        if bundle.schema != 1 || bundle.cluster != cluster {
            return Err(EnrollmentError::Corrupt);
        }
        Ok(Self {
            bundle,
            _directory: directory,
        })
    }
    pub fn open_or_create(
        path: impl AsRef<Path>,
        cluster: ClusterId,
    ) -> Result<Self, EnrollmentError> {
        if cluster == [0; 16] {
            return Err(EnrollmentError::Invalid);
        }
        let directory = PrivateDirectory::open(path.as_ref())?;
        let bundle: JoinKeyBundle = if let Some(bytes) = directory.read("join-key.bin")? {
            decode(&bytes)?
        } else {
            let key = KeyPair::generate()?;
            let request = CertificateParams::new(Vec::<String>::new())?.serialize_request(&key)?;
            let bundle = JoinKeyBundle {
                schema: 1,
                cluster,
                request: random()?,
                csr: request.der().to_vec(),
                key: SecretBytes(key.serialize_der()),
            };
            directory.install_new("join-key.bin", &Zeroizing::new(encode(&bundle)?))?;
            bundle
        };
        if bundle.cluster != cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        if bundle.schema != 1 || bundle.request == [0; 16] {
            return Err(EnrollmentError::Corrupt);
        }
        let csr = verified_csr(&bundle.csr)?;
        let key = KeyPair::try_from(bundle.key.0.as_slice())?;
        if key.subject_public_key_info() != csr.public_key.subject_public_key_info() {
            return Err(EnrollmentError::Corrupt);
        }
        Ok(Self {
            bundle,
            _directory: directory,
        })
    }
    pub fn cluster(&self) -> ClusterId {
        self.bundle.cluster
    }
    pub fn request_id(&self) -> JoinId {
        self.bundle.request
    }
    pub fn csr(&self) -> &[u8] {
        &self.bundle.csr
    }
    pub fn complete<'a>(
        &self,
        receipt: &EnrollmentReceipt,
        issuers: impl IntoIterator<Item = &'a IssuerRecord>,
        now: i64,
    ) -> Result<CredentialMaterial, EnrollmentError> {
        if receipt.identity.cluster != self.bundle.cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        if receipt.request != self.bundle.request
            || receipt.csr_hash != hash("focal.enrollment.csr.v1", self.csr())
            || receipt.public_key != csr_key_hash(self.csr())?
            || receipt.expires_at <= now
        {
            return Err(EnrollmentError::Unauthorized);
        }
        let material = CredentialMaterial {
            certificate_chain: issued_chain(receipt, issuers)?,
            private_key: Zeroizing::new(self.bundle.key.0.clone()),
        };
        let bytes = encode(receipt)?;
        match self._directory.read("enrollment.bin")? {
            Some(existing) if *existing != bytes => return Err(EnrollmentError::Conflict),
            Some(_) => (),
            None => self._directory.install_new("enrollment.bin", &bytes)?,
        }
        Ok(material)
    }
    pub fn enrollment(&self) -> Result<Option<EnrollmentReceipt>, EnrollmentError> {
        self._directory
            .read("enrollment.bin")?
            .map(|bytes| decode(&bytes))
            .transpose()
    }
    /// Adopt a rotation (24 §11): install the receipt issued for `next` —
    /// the same identity under the new key and request — over the receipt
    /// and key this directory holds, then clear the staged material so a
    /// later rotation stages a fresh key. The credential returned is the
    /// new key's. Refused when the receipt is not `next`'s or the identity
    /// differs from the one held.
    pub fn rotate_into<'a>(
        &self,
        next: &Self,
        receipt: &EnrollmentReceipt,
        issuers: impl IntoIterator<Item = &'a IssuerRecord>,
        now: i64,
    ) -> Result<CredentialMaterial, EnrollmentError> {
        if receipt.identity.cluster != self.bundle.cluster
            || next.bundle.cluster != self.bundle.cluster
        {
            return Err(EnrollmentError::WrongCluster);
        }
        if receipt.request != next.bundle.request
            || receipt.csr_hash != hash("focal.enrollment.csr.v1", next.csr())
            || receipt.public_key != csr_key_hash(next.csr())?
            || receipt.expires_at <= now
        {
            return Err(EnrollmentError::Unauthorized);
        }
        let certificate_chain = issued_chain(receipt, issuers)?;
        let held = self.enrollment()?.ok_or(EnrollmentError::NotCommitted)?;
        if held.identity != receipt.identity {
            return Err(EnrollmentError::Conflict);
        }
        // The receipt is installed first, then the key it was issued for. A crash
        // between the two leaves this directory holding the new receipt but the
        // previous key; the next start re-adopts and must FINISH by installing the
        // key. So resume idempotently: install the receipt only if it is not
        // already held, and always (re)install the key. A genuine duplicate - the
        // receipt AND its key already installed - is still refused.
        let key_installed = self.key_identity()? == receipt.public_key;
        if held.public_key == receipt.public_key {
            if key_installed {
                return Err(EnrollmentError::Conflict);
            }
        } else {
            self._directory
                .replace("enrollment.bin", &encode(receipt)?)?;
        }
        self._directory
            .replace("join-key.bin", &Zeroizing::new(encode(&next.bundle)?))?;
        next._directory.remove("enrollment.bin")?;
        next._directory.remove("join-key.bin")?;
        Ok(CredentialMaterial {
            certificate_chain,
            private_key: Zeroizing::new(next.bundle.key.0.clone()),
        })
    }
    /// The identity of the key this directory holds, in the domain of
    /// `EnrollmentReceipt::public_key`.
    pub fn key_identity(&self) -> Result<Fingerprint, EnrollmentError> {
        csr_key_hash(self.csr())
    }
    /// Install a renewed receipt of this key over the one held: the same
    /// request and CSR under a fresh certificate that expires later. A held
    /// receipt that is already as new is kept; an older one is refused.
    pub fn renew<'a>(
        &self,
        receipt: &EnrollmentReceipt,
        issuers: impl IntoIterator<Item = &'a IssuerRecord>,
        now: i64,
    ) -> Result<CredentialMaterial, EnrollmentError> {
        if receipt.identity.cluster != self.bundle.cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        if receipt.request != self.bundle.request
            || receipt.csr_hash != hash("focal.enrollment.csr.v1", self.csr())
            || receipt.public_key != csr_key_hash(self.csr())?
            || receipt.expires_at <= now
        {
            return Err(EnrollmentError::Unauthorized);
        }
        let certificate_chain = issued_chain(receipt, issuers)?;
        let held = self.enrollment()?.ok_or(EnrollmentError::NotCommitted)?;
        if held.identity != receipt.identity || held.public_key != receipt.public_key {
            return Err(EnrollmentError::Conflict);
        }
        if held.expires_at > receipt.expires_at {
            return Err(EnrollmentError::Conflict);
        }
        if held != *receipt {
            self._directory
                .replace("enrollment.bin", &encode(receipt)?)?;
        }
        Ok(CredentialMaterial {
            certificate_chain,
            private_key: Zeroizing::new(self.bundle.key.0.clone()),
        })
    }
}
/// The chain a credential presents: its certificate, the issuer that
/// issued it and that issuer's endorsement (24 §11). Verified against the
/// trusted issuers first.
fn issued_chain<'a>(
    receipt: &EnrollmentReceipt,
    issuers: impl IntoIterator<Item = &'a IssuerRecord>,
) -> Result<Vec<Vec<u8>>, EnrollmentError> {
    let issuers: Vec<&IssuerRecord> = issuers.into_iter().collect();
    if issuers.len() > focal_wire::MAX_TRUST_ROOTS {
        return Err(EnrollmentError::Capacity);
    }
    verify_issued(
        receipt,
        issuers.iter().map(|issuer| issuer.certificate.as_slice()),
    )?;
    let issuer = issuers
        .iter()
        .find(|issuer| {
            focal_wire::issued_by(&receipt.certificate, &issuer.certificate).unwrap_or(false)
        })
        .ok_or(EnrollmentError::Unauthorized)?;
    let mut chain = vec![receipt.certificate.clone()];
    chain.extend(issuer.chain().map(<[u8]>::to_vec));
    Ok(chain)
}

fn params(
    names: Vec<String>,
    now: i64,
    lifetime: u64,
) -> Result<CertificateParams, EnrollmentError> {
    let mut params = CertificateParams::new(names)?;
    params.distinguished_name = DistinguishedName::new();
    params.is_ca = IsCa::NoCa;
    params.not_before =
        OffsetDateTime::from_unix_timestamp(now.checked_sub(60).ok_or(EnrollmentError::Invalid)?)
            .map_err(|_| EnrollmentError::Invalid)?;
    let end = now
        .checked_add(i64::try_from(lifetime).map_err(|_| EnrollmentError::Invalid)?)
        .ok_or(EnrollmentError::Invalid)?;
    params.not_after =
        OffsetDateTime::from_unix_timestamp(end).map_err(|_| EnrollmentError::Invalid)?;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.serial_number = Some(random::<16>()?.to_vec().into());
    Ok(params)
}
pub(crate) fn verified_csr(csr: &[u8]) -> Result<CertificateSigningRequestParams, EnrollmentError> {
    if csr.is_empty() || csr.len() > 4096 {
        return Err(EnrollmentError::Capacity);
    }
    let (rest, _) =
        X509CertificationRequest::from_der(csr).map_err(|_| EnrollmentError::Unauthorized)?;
    if !rest.is_empty() {
        return Err(EnrollmentError::Unauthorized);
    }
    CertificateSigningRequestParams::from_der(&csr.into())
        .map_err(|_| EnrollmentError::Unauthorized)
}
pub(crate) fn csr_key_hash(csr: &[u8]) -> Result<Fingerprint, EnrollmentError> {
    Ok(hash(
        "focal.enrollment.public-key.v1",
        &verified_csr(csr)?.public_key.subject_public_key_info(),
    ))
}
/// The identity of the key a certificate carries, in the same domain as
/// `EnrollmentReceipt::public_key`; stable across certificate renewal.
pub fn certificate_key_hash(certificate: &[u8]) -> Result<Fingerprint, EnrollmentError> {
    if certificate.len() > 4096 {
        return Err(EnrollmentError::Capacity);
    }
    let (rest, certificate) =
        X509Certificate::from_der(certificate).map_err(|_| EnrollmentError::Corrupt)?;
    if !rest.is_empty() {
        return Err(EnrollmentError::Corrupt);
    }
    Ok(hash(
        "focal.enrollment.public-key.v1",
        certificate.public_key().raw,
    ))
}
/// Whether `certificate` is an issuer's: a CA certificate that signs
/// certificates.
pub(crate) fn is_issuer(certificate: &[u8]) -> Result<bool, EnrollmentError> {
    if certificate.len() > 4096 {
        return Err(EnrollmentError::Capacity);
    }
    let (rest, certificate) =
        X509Certificate::from_der(certificate).map_err(|_| EnrollmentError::Corrupt)?;
    if !rest.is_empty() {
        return Err(EnrollmentError::Corrupt);
    }
    Ok(certificate.is_ca()
        && certificate
            .key_usage()
            .map_err(|_| EnrollmentError::Corrupt)?
            .is_some_and(|usage| usage.value.key_cert_sign()))
}
/// Whether `endorsement` endorses the issuer `certificate`: a CA
/// certificate for the same key and subject, valid over the same span, so
/// a chain to the issuer verifies under the endorsement's signer as under
/// the issuer itself (24 §11).
pub(crate) fn endorses(endorsement: &[u8], certificate: &[u8]) -> Result<bool, EnrollmentError> {
    if endorsement.len() > 4096 || certificate.len() > 4096 {
        return Err(EnrollmentError::Capacity);
    }
    let (rest, endorsement) =
        X509Certificate::from_der(endorsement).map_err(|_| EnrollmentError::Corrupt)?;
    if !rest.is_empty() {
        return Err(EnrollmentError::Corrupt);
    }
    let (rest, certificate) =
        X509Certificate::from_der(certificate).map_err(|_| EnrollmentError::Corrupt)?;
    if !rest.is_empty() {
        return Err(EnrollmentError::Corrupt);
    }
    Ok(endorsement.is_ca()
        && endorsement.public_key().raw == certificate.public_key().raw
        && endorsement.subject().as_raw() == certificate.subject().as_raw()
        && endorsement.issuer().as_raw() != endorsement.subject().as_raw()
        && endorsement.validity().not_after == certificate.validity().not_after)
}
/// Verify a receipt's certificate: its shape, and its signature under one
/// of `roots` — the issuers the registry trusts.
pub(crate) fn verify_issued<'a>(
    receipt: &EnrollmentReceipt,
    roots: impl IntoIterator<Item = &'a [u8]>,
) -> Result<(), EnrollmentError> {
    verify_issued_shape(receipt)?;
    let (_, certificate) =
        X509Certificate::from_der(&receipt.certificate).map_err(|_| EnrollmentError::Corrupt)?;
    for root in roots {
        if root.len() > 4096 {
            return Err(EnrollmentError::Capacity);
        }
        let (rest, root) = X509Certificate::from_der(root).map_err(|_| EnrollmentError::Corrupt)?;
        if !rest.is_empty() {
            return Err(EnrollmentError::Corrupt);
        }
        if certificate.issuer().as_raw() == root.subject().as_raw()
            && certificate
                .verify_signature(Some(root.public_key()))
                .is_ok()
        {
            return Ok(());
        }
    }
    Err(EnrollmentError::Unauthorized)
}
/// Verify a receipt's certificate by what it is — its key, expiry, name
/// and usages agree with the receipt — and not by who issued it: what an
/// expired receipt is held to, its issuer possibly retired since.
pub(crate) fn verify_issued_shape(receipt: &EnrollmentReceipt) -> Result<(), EnrollmentError> {
    if receipt.certificate.len() > 4096 {
        return Err(EnrollmentError::Capacity);
    }
    let (rest, certificate) =
        X509Certificate::from_der(&receipt.certificate).map_err(|_| EnrollmentError::Corrupt)?;
    if !rest.is_empty() {
        return Err(EnrollmentError::Corrupt);
    }
    if hash(
        "focal.enrollment.public-key.v1",
        certificate.public_key().raw,
    ) != receipt.public_key
        || certificate.validity().not_after.timestamp() != receipt.expires_at
        || certificate.is_ca()
    {
        return Err(EnrollmentError::Corrupt);
    }
    let san = certificate
        .subject_alternative_name()
        .map_err(|_| EnrollmentError::Corrupt)?
        .ok_or(EnrollmentError::Corrupt)?;
    if san.value.general_names.len() != 1
        || san.value.general_names.first()
            != Some(&x509_parser::extensions::GeneralName::DNSName(
                &receipt.identity.server_name,
            ))
    {
        return Err(EnrollmentError::Corrupt);
    }
    let usage = certificate
        .extended_key_usage()
        .map_err(|_| EnrollmentError::Corrupt)?
        .ok_or(EnrollmentError::Corrupt)?;
    if !usage.value.client_auth
        || usage.value.server_auth != (receipt.identity.role == EnrollmentRole::Node)
        || usage.value.any
        || usage.value.code_signing
        || usage.value.email_protection
        || usage.value.time_stamping
        || usage.value.ocsp_signing
        || !usage.value.other.is_empty()
    {
        return Err(EnrollmentError::Corrupt);
    }
    Ok(())
}

/// Whether the receipt's identity is bound to its key: derived from the key
/// itself, or carried to it by a CA-signed subject (the founder's genesis
/// principal, or a principal an earlier key of the same enrollment derived
/// and a rotation carried). Only meaningful after `verify_issued`.
pub(crate) fn identity_bound(receipt: &EnrollmentReceipt) -> Result<bool, EnrollmentError> {
    let derived = crate::registry::assigned(
        receipt.identity.cluster,
        receipt.identity.role,
        receipt.identity.node_id.unwrap_or(1),
        receipt.public_key,
    );
    Ok(receipt.identity == derived || founding_principal(receipt)? || carried_principal(receipt)?)
}
/// A rotated key carries the principal its enrollment's earlier key derived;
/// the CA-signed subject names it. Only meaningful after `verify_issued`.
pub(crate) fn carried_principal(receipt: &EnrollmentReceipt) -> Result<bool, EnrollmentError> {
    let (_, certificate) =
        X509Certificate::from_der(&receipt.certificate).map_err(|_| EnrollmentError::Corrupt)?;
    let expected = format!(
        "focal-carried-principal:{}",
        hex(&receipt.identity.principal)
    );
    let mut units = certificate.subject().iter_organizational_unit();
    Ok(receipt.identity.role == EnrollmentRole::Node
        && units
            .next()
            .is_some_and(|unit| unit.as_str().is_ok_and(|value| value == expected))
        && units.next().is_none())
}
/// The founder's principal was assigned at genesis, not derived from its key;
/// the CA-signed subject binds it, so every certificate the founder's key is
/// issued — the genesis one and each renewal of it — carries the binding, and
/// only the authority can write it (`issue_founder`). Only meaningful after
/// `verify_issued`.
pub(crate) fn founding_principal(receipt: &EnrollmentReceipt) -> Result<bool, EnrollmentError> {
    let (_, certificate) =
        X509Certificate::from_der(&receipt.certificate).map_err(|_| EnrollmentError::Corrupt)?;
    let expected = format!(
        "focal-genesis-principal:{}",
        hex(&receipt.identity.principal)
    );
    let mut units = certificate.subject().iter_organizational_unit();
    Ok(receipt.identity.role == EnrollmentRole::Node
        && units
            .next()
            .is_some_and(|unit| unit.as_str().is_ok_and(|value| value == expected))
        && units.next().is_none())
}
