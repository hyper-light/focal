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
}

/// CA custody is deliberately separate from replicated public enrollment
/// metadata. Only a node operator holding this private directory may issue.
pub struct BootstrapAuthority {
    bundle: AuthorityBundle,
    key: KeyPair,
    _directory: PrivateDirectory,
}
impl BootstrapAuthority {
    pub fn open_or_create(
        path: impl AsRef<Path>,
        cluster: ClusterId,
        server_names: Vec<String>,
        now: i64,
    ) -> Result<Self, EnrollmentError> {
        if cluster == [0; 16]
            || server_names.is_empty()
            || server_names.len() > 16
            || server_names.iter().any(|n| n.is_empty() || n.len() > 253)
        {
            return Err(EnrollmentError::Invalid);
        }
        let directory = PrivateDirectory::open(path.as_ref())?;
        let bundle: AuthorityBundle = if let Some(bytes) = directory.read("authority.bin")? {
            decode(&bytes)?
        } else {
            let key = KeyPair::generate()?;
            let mut ca_params = params(vec![], now, 10 * 365 * 86400)?;
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
            ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            ca_params.distinguished_name.push(
                DnType::CommonName,
                format!("Focal cluster {}", hex(&cluster)),
            );
            let ca = ca_params.self_signed(&key)?;
            let issuer = Issuer::from_ca_cert_der(ca.der(), &key)?;
            let server_key = KeyPair::generate()?;
            let mut server_params = params(server_names.clone(), now, 365 * 86400)?;
            server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            let server = server_params.signed_by(&server_key, &issuer)?;
            let bundle = AuthorityBundle {
                schema: 1,
                cluster,
                names: server_names.clone(),
                ca: ca.der().to_vec(),
                ca_key: SecretBytes(key.serialize_der()),
                server: server.der().to_vec(),
                server_key: SecretBytes(server_key.serialize_der()),
            };
            let bytes = Zeroizing::new(encode(&bundle)?);
            directory.install_new("authority.bin", &bytes)?;
            bundle
        };
        if bundle.cluster != cluster {
            return Err(EnrollmentError::WrongCluster);
        }
        if bundle.schema != 1 || bundle.names != server_names {
            return Err(EnrollmentError::Invalid);
        }
        let key = KeyPair::try_from(bundle.ca_key.0.as_slice())?;
        let (_, ca) =
            X509Certificate::from_der(&bundle.ca).map_err(|_| EnrollmentError::Corrupt)?;
        if ca.public_key().raw != key.subject_public_key_info() {
            return Err(EnrollmentError::Corrupt);
        }
        let server_key = KeyPair::try_from(bundle.server_key.0.as_slice())?;
        let (_, server) =
            X509Certificate::from_der(&bundle.server).map_err(|_| EnrollmentError::Corrupt)?;
        if server.public_key().raw != server_key.subject_public_key_info() {
            return Err(EnrollmentError::Corrupt);
        }
        server
            .verify_signature(Some(ca.public_key()))
            .map_err(|_| EnrollmentError::Corrupt)?;
        Ok(Self {
            bundle,
            key,
            _directory: directory,
        })
    }
    pub fn cluster(&self) -> ClusterId {
        self.bundle.cluster
    }
    pub fn ca_certificate(&self) -> &[u8] {
        &self.bundle.ca
    }
    pub fn server_certificate(&self) -> &[u8] {
        &self.bundle.server
    }
    pub fn server_identity(&self) -> CredentialMaterial {
        CredentialMaterial {
            certificate_chain: vec![self.bundle.server.clone(), self.bundle.ca.clone()],
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
        self.issue_identity(csr, identity, now, lifetime, false)
    }
    pub(crate) fn issue_founder(
        &self,
        csr: &[u8],
        identity: &AssignedIdentity,
        now: i64,
        lifetime: u64,
    ) -> Result<Vec<u8>, EnrollmentError> {
        self.issue_identity(csr, identity, now, lifetime, true)
    }
    fn issue_identity(
        &self,
        csr: &[u8],
        identity: &AssignedIdentity,
        now: i64,
        lifetime: u64,
        founding: bool,
    ) -> Result<Vec<u8>, EnrollmentError> {
        let mut request = verified_csr(csr)?;
        // A CSR proves control of a key only. Ignore every requested subject,
        // SAN, CA bit, usage and extension; authorization supplies all names.
        request.params = params(vec![identity.server_name.clone()], now, lifetime)?;
        request
            .params
            .distinguished_name
            .push(DnType::CommonName, identity.server_name.clone());
        if founding {
            // A CA-signed subject binds the original local principal to this
            // one genesis identity. Ordinary CSR issuance cannot request it.
            request.params.distinguished_name.push(
                DnType::OrganizationalUnitName,
                format!("focal-genesis-principal:{}", hex(&identity.principal)),
            );
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
        let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
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
    pub fn inspect_saved(
        key_bytes: &[u8],
        receipt_bytes: &[u8],
        cluster: ClusterId,
        ca_certificate: &[u8],
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
            || receipt.identity
                != crate::registry::assigned(
                    cluster,
                    receipt.identity.role,
                    receipt.identity.node_id.unwrap_or(1),
                    receipt.public_key,
                )
        {
            return Err(EnrollmentError::Unauthorized);
        }
        verify_issued(&receipt, ca_certificate)?;
        Ok((receipt, *blake3::hash(&bundle.csr).as_bytes()))
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
    pub fn complete(
        &self,
        receipt: &EnrollmentReceipt,
        ca_certificate: &[u8],
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
        verify_issued(receipt, ca_certificate)?;
        let material = CredentialMaterial {
            certificate_chain: vec![receipt.certificate.clone(), ca_certificate.to_vec()],
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
pub(crate) fn verify_issued(receipt: &EnrollmentReceipt, ca: &[u8]) -> Result<(), EnrollmentError> {
    if receipt.certificate.len() > 4096 || ca.len() > 4096 {
        return Err(EnrollmentError::Capacity);
    }
    let (rest, certificate) =
        X509Certificate::from_der(&receipt.certificate).map_err(|_| EnrollmentError::Corrupt)?;
    if !rest.is_empty() {
        return Err(EnrollmentError::Corrupt);
    }
    let (rest, ca) = X509Certificate::from_der(ca).map_err(|_| EnrollmentError::Corrupt)?;
    if !rest.is_empty() {
        return Err(EnrollmentError::Corrupt);
    }
    certificate
        .verify_signature(Some(ca.public_key()))
        .map_err(|_| EnrollmentError::Unauthorized)?;
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

/// This exception is accepted only for the first Node receipt in a new genesis;
/// its identity and principal are bound by the already verified CA signature.
pub(crate) fn founding_principal(receipt: &EnrollmentReceipt) -> Result<bool, EnrollmentError> {
    let (_, certificate) =
        X509Certificate::from_der(&receipt.certificate).map_err(|_| EnrollmentError::Corrupt)?;
    let expected = format!(
        "focal-genesis-principal:{}",
        hex(&receipt.identity.principal)
    );
    let mut units = certificate.subject().iter_organizational_unit();
    Ok(receipt.revision == 1
        && receipt.identity.role == EnrollmentRole::Node
        && units
            .next()
            .is_some_and(|unit| unit.as_str().is_ok_and(|value| value == expected))
        && units.next().is_none())
}
