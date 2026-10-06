//! Client enrollment owns a private key and principal, never a physical node.
use super::*;
pub struct ClientInvitation(NodeInvitation);
impl std::fmt::Debug for ClientInvitation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientInvitation")
            .field("client_name", &self.0.name)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}
impl ClientInvitation {
    pub fn new(
        name: impl Into<String>,
        genesis: NetworkGenesis,
        invitation: Invitation,
    ) -> Result<Self, JoinError> {
        let value = NodeInvitation {
            name: name.into(),
            genesis,
            invitation,
        };
        value.validate_role(EnrollmentRole::Client)?;
        Ok(Self(value))
    }
    pub fn name(&self) -> &str {
        &self.0.name
    }
    pub fn genesis(&self) -> &NetworkGenesis {
        &self.0.genesis
    }
    pub fn invitation(&self) -> &Invitation {
        &self.0.invitation
    }
    /// The data ALPN presents the founder Node certificate, independently of
    /// the bootstrap enrollment server certificate on the same endpoint.
    pub fn data_server_name(&self) -> Result<String, JoinError> {
        self.0.validate_role(EnrollmentRole::Client)?;
        let registry = genesis_registry(&self.0.genesis)?;
        let founder = registry.enrollments().next().ok_or(JoinError::Invalid)?;
        if founder.identity.node_id != Some(self.0.genesis.founder.node)
            || founder.identity.role != EnrollmentRole::Node
        {
            return Err(JoinError::Invalid);
        }
        Ok(founder.identity.server_name.clone())
    }
    pub fn encode(&self) -> Result<Zeroizing<Vec<u8>>, JoinError> {
        self.0.encode_role(EnrollmentRole::Client)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, JoinError> {
        Ok(Self(NodeInvitation::decode_role(
            bytes,
            EnrollmentRole::Client,
        )?))
    }
    pub fn load(path: impl AsRef<Path>) -> Result<Self, JoinError> {
        Self::decode(&read_private(path.as_ref(), MAX_BUNDLE)?)
    }
    pub fn write_new(&self, path: impl AsRef<Path>) -> Result<(), JoinError> {
        write_private_new(path.as_ref(), &self.encode()?)
    }
}
#[derive(Serialize, Deserialize)]
struct ClientJournal {
    schema: u16,
    bundle: PrivateBytes,
    key: Option<KeyPin>,
}
pub struct PendingClientJoin {
    bundle: ClientInvitation,
    key: JoinKey,
    _journal: PrivateJournal,
    directory: std::path::PathBuf,
}
/// The issuers a client adopted beside its journal (24 §11): issuers its
/// verified chains carried endorsed by a root it held, kept so a later
/// succession — endorsed by the adopted issuer, not by the one the
/// invitation carried — still verifies. Bounded by the roots a verifier
/// holds; the newest adoptions stay.
const ADOPTED_FILE: &str = "trust-adopted.bin";
const ADOPTED_TEMPORARY: &str = "trust-adopted.bin.next";
const ADOPTED_MAGIC: &[u8; 8] = b"FCLTRST1";
/// The most the file may be: the roots a verifier holds, each an issuer's
/// certificate and endorsement at the registry's bound, and the envelope.
const MAX_ADOPTED_BYTES: usize = focal_wire::MAX_TRUST_ROOTS * 2 * 4096 + 1024;
#[derive(Serialize, Deserialize)]
struct AdoptedTrust {
    schema: u16,
    issuers: Vec<focal_enrollment::IssuerRecord>,
}
/// The issuers adopted in `directory`, newest last; none when nothing was.
pub fn adopted_issuers(directory: &Path) -> Result<Vec<focal_enrollment::IssuerRecord>, JoinError> {
    let path = directory.join(ADOPTED_FILE);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.len() > MAX_ADOPTED_BYTES as u64 {
        return Err(JoinError::Invalid);
    }
    let bytes = fs::read(&path)?;
    let payload = bytes.get(40..).ok_or(JoinError::Invalid)?;
    if bytes.get(..8) != Some(ADOPTED_MAGIC.as_slice())
        || bytes.get(8..40) != Some(blake3::hash(payload).as_bytes().as_slice())
    {
        return Err(JoinError::Invalid);
    }
    let (adopted, tail): (AdoptedTrust, _) = postcard::take_from_bytes(payload)?;
    if !tail.is_empty()
        || adopted.schema != 1
        || adopted.issuers.len() > focal_wire::MAX_TRUST_ROOTS
    {
        return Err(JoinError::Invalid);
    }
    for issuer in &adopted.issuers {
        // What was written is what the certificate says, or the file is
        // not this one.
        if focal_enrollment::IssuerRecord::of(&issuer.certificate, issuer.endorsement.as_deref())?
            != *issuer
        {
            return Err(JoinError::Invalid);
        }
    }
    Ok(adopted.issuers)
}
/// Adopt an issuer a client's verified chain carried endorsed (24 §11): the issuer's
/// own certificate when the chain carried it, else the endorsement itself,
/// which anchors the same key and name. Written beside the journal and
/// replaced atomically; the oldest adoption leaves when the bound is full.
/// `false` when it was adopted already.
pub fn adopt_issuer(directory: &Path, adopted: &focal_wire::Adopted) -> Result<bool, JoinError> {
    let record = match &adopted.issuer {
        Some(issuer) => {
            focal_enrollment::IssuerRecord::of(issuer, Some(adopted.anchor.as_slice()))?
        }
        None => focal_enrollment::IssuerRecord::of(&adopted.anchor, None)?,
    };
    let mut issuers = adopted_issuers(directory)?;
    if issuers
        .iter()
        .any(|known| known.fingerprint == record.fingerprint)
    {
        return Ok(false);
    }
    while issuers.len() >= focal_wire::MAX_TRUST_ROOTS {
        issuers.remove(0);
    }
    issuers.push(record);
    let payload = postcard::to_stdvec(&AdoptedTrust { schema: 1, issuers })?;
    if payload.len().saturating_add(40) > MAX_ADOPTED_BYTES {
        return Err(JoinError::Capacity);
    }
    let mut bytes = ADOPTED_MAGIC.to_vec();
    bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
    bytes.extend_from_slice(&payload);
    let temporary = directory.join(ADOPTED_TEMPORARY);
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    {
        use std::io::Write;
        let mut file = focal_platform::fs::create_private_new(&temporary, false, true)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    focal_platform::fs::atomic_replace(&temporary, &directory.join(ADOPTED_FILE))?;
    Ok(true)
}
/// An enrolled client's way to the cluster ([`PendingClientJoin::remote_client`]).
pub struct EnrolledClient {
    pub tls: quinn::ClientConfig,
    /// The endorsed issuers the verifier found and the context does not hold
    /// yet, to record once a request succeeded.
    pub adopted: focal_wire::AdoptedRoots,
    pub initial: focal_wire::RouteHint,
    pub build: focal_client::input::BuildContext,
    pub operation: focal_client::pending::OperationContext,
}

impl PendingClientJoin {
    pub fn open(path: impl AsRef<Path>, bundle: ClientInvitation) -> Result<Self, JoinError> {
        Self::build(path.as_ref(), Some(bundle), false)
    }
    pub fn resume(path: impl AsRef<Path>) -> Result<Self, JoinError> {
        Self::build(path.as_ref(), None, false)
    }
    /// Resume a completed enrollment for reading beside other processes of
    /// the same participant. An enrollment that still needs a write is
    /// reported pending; it is completed by `context enroll`, which owns the
    /// directory exclusively.
    pub fn resume_shared(path: impl AsRef<Path>) -> Result<Self, JoinError> {
        Self::build(path.as_ref(), None, true)
    }
    fn build(
        path: &Path,
        expected: Option<ClientInvitation>,
        shared: bool,
    ) -> Result<Self, JoinError> {
        let parent = path.parent().ok_or(JoinError::Invalid)?;
        let parent_owner =
            focal_platform::fs::private_dir_owner(parent)?.ok_or(JoinError::Permissions)?;
        let mut marker = path.as_os_str().to_os_string();
        marker.push(".client-initialized");
        let marker = std::path::PathBuf::from(marker);
        let initialized = match fs::symlink_metadata(&marker) {
            Ok(_) => {
                if !focal_platform::fs::check_private_file(&marker, &parent_owner, 1)? {
                    return Err(JoinError::Permissions);
                }
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if initialized && !path.join("journal.bin").is_file() {
            return Err(JoinError::Invalid);
        }
        if expected.is_none() && !initialized && !path.join("journal.bin").is_file() {
            return Err(JoinError::Pending);
        }
        if shared && !initialized {
            return Err(JoinError::Pending);
        }
        let mut journal = if shared {
            PrivateJournal::open_shared(path)?
        } else {
            PrivateJournal::open(path)?
        };
        let expected = expected.map(|bundle| bundle.encode()).transpose()?;
        let mut state = match journal.read()? {
            Some(bytes) => {
                let (state, tail): (ClientJournal, _) = postcard::take_from_bytes(&bytes)?;
                if !tail.is_empty() || state.schema != 1 {
                    return Err(JoinError::Invalid);
                }
                if expected
                    .as_ref()
                    .is_some_and(|bytes| bytes.as_slice() != state.bundle.0.as_slice())
                {
                    return Err(JoinError::Conflict);
                }
                state
            }
            None => {
                if initialized || path.join("client-key").exists() {
                    return Err(JoinError::Invalid);
                }
                let state = ClientJournal {
                    schema: 1,
                    bundle: PrivateBytes(expected.ok_or(JoinError::Pending)?),
                    key: None,
                };
                save(&mut journal, &state)?;
                state
            }
        };
        let bundle = ClientInvitation::decode(&state.bundle.0)?;
        let pin = blake3::hash(&state.bundle.0);
        if shared {
            if state.key.is_none() {
                return Err(JoinError::Pending);
            }
        } else {
            write_private_new(&marker, pin.as_bytes())?;
        }
        let keypath = path.join("client-key");
        if state.key.is_some() && !keypath.join("join-key.bin").is_file() {
            return Err(JoinError::Invalid);
        }
        let key = if shared {
            JoinKey::open_shared(&keypath, bundle.invitation().cluster())?
        } else {
            JoinKey::open_or_create(&keypath, bundle.invitation().cluster())?
        };
        let keypin = KeyPin {
            request: key.request_id(),
            csr: *blake3::hash(key.csr()).as_bytes(),
        };
        match &state.key {
            Some(existing) if *existing != keypin => return Err(JoinError::Conflict),
            Some(_) => {}
            None => {
                state.key = Some(keypin);
                save(&mut journal, &state)?;
            }
        }
        Ok(Self {
            bundle,
            key,
            directory: path.to_path_buf(),
            _journal: journal,
        })
    }
    /// The directory this join lives in: its journal, key, and the issuers
    /// it adopted.
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    /// The roots this client's verifier holds (24 §11): the issuers its
    /// invitation carried and the ones it adopted since, the invitation's
    /// first, within the bound a verifier holds.
    pub fn trust_roots(&self) -> Result<Vec<Vec<u8>>, JoinError> {
        let mut roots = self.bundle.invitation().trust().root_certificates();
        for issuer in adopted_issuers(&self.directory)? {
            if roots.len() >= focal_wire::MAX_TRUST_ROOTS {
                break;
            }
            if !roots.contains(&issuer.certificate) {
                roots.push(issuer.certificate);
            }
        }
        Ok(roots)
    }
    pub fn invitation(&self) -> &ClientInvitation {
        &self.bundle
    }
    /// What a client needs to reach the cluster as this enrolled identity:
    /// the TLS configuration whose verifier holds [`Self::trust_roots`] and
    /// adopts an endorsed successor issuer (24 §11), the invitation's
    /// endpoint as the first route, and the identity's build and operation
    /// contexts on the founder's ledger. The CLI's enrolled contexts and the
    /// measurement tools open a remote client through this one path.
    pub fn remote_client(
        &self,
        now: i64,
        limits: &focal_wire::WireLimits,
    ) -> Result<EnrolledClient, JoinError> {
        let receipt = self.enrollment()?.ok_or(JoinError::Pending)?;
        let credentials = self.credentials(now)?;
        let founder = &self.bundle.genesis().founder;
        let (tls, adopted) = focal_wire::client_tls_adopting(
            focal_wire::TlsIdentity::from_pkcs8(
                credentials.certificate_chain().to_vec(),
                credentials.private_key_der().to_vec(),
            ),
            self.trust_roots()?,
            limits,
        )?;
        let actor = focal_model::ParticipantId(receipt.identity.principal);
        Ok(EnrolledClient {
            tls,
            adopted,
            initial: focal_wire::RouteHint {
                epoch: RouteEpoch(1),
                endpoint: self.bundle.invitation().trust().endpoint.clone(),
                server_name: self.bundle.data_server_name()?,
            },
            build: focal_client::input::BuildContext {
                ledger: founder.ledger,
                actor,
                root: founder.root,
                policy_revision: 1,
            },
            operation: focal_client::pending::OperationContext {
                cluster: founder.cluster,
                ledger: founder.ledger,
                principal: actor,
            },
        })
    }
    pub fn request_id(&self) -> [u8; 16] {
        self.key.request_id()
    }
    pub fn csr(&self) -> &[u8] {
        self.key.csr()
    }
    pub fn enrollment(&self) -> Result<Option<EnrollmentReceipt>, JoinError> {
        Ok(self.key.enrollment()?)
    }
    pub fn credentials(&self, now: i64) -> Result<CredentialMaterial, JoinError> {
        let receipt = self.key.enrollment()?.ok_or(JoinError::Pending)?;
        self.verify(&receipt, now)
    }
    pub async fn redeem(
        &self,
        client: &EnrollmentClient,
        now: i64,
    ) -> Result<EnrollmentReceipt, JoinError> {
        if let Some(receipt) = self.key.enrollment()? {
            drop(self.verify(&receipt, now)?);
            return Ok(receipt);
        }
        let address =
            crate::network_state::resolve_endpoint(&self.bundle.invitation().trust().endpoint)
                .await
                .map_err(|_| JoinError::Invalid)?;
        let receipt = std::panic::AssertUnwindSafe(client.redeem(
            address,
            self.bundle.invitation(),
            &self.key,
            now,
        ))
        .catch_unwind()
        .await
        .map_err(|_| JoinError::Runtime)??;
        // The receipt is issued at the sponsor's clock after the exchange; a
        // caller's earlier sample must not read a fresh receipt as future-dated.
        let now = now.max(crate::network_bootstrap::unix_time().map_err(|_| JoinError::Runtime)?);
        drop(self.verify(&receipt, now)?);
        Ok(receipt)
    }
    fn verify(
        &self,
        receipt: &EnrollmentReceipt,
        now: i64,
    ) -> Result<CredentialMaterial, JoinError> {
        if receipt.invitation != self.bundle.invitation().id()
            || receipt.issued_at > now
            || receipt.identity.role != EnrollmentRole::Client
            || receipt.identity.node_id.is_some()
            || receipt.identity.principal == [0; 16]
        {
            return Err(JoinError::Invalid);
        }
        Ok(self.key.complete(
            receipt,
            self.bundle.invitation().trust().issuers.iter(),
            now,
        )?)
    }
}
fn save(journal: &mut PrivateJournal, state: &ClientJournal) -> Result<(), JoinError> {
    if postcard::experimental::serialized_size(state)? > MAX_JOURNAL {
        return Err(JoinError::Capacity);
    }
    let bytes = Zeroizing::new(postcard::to_stdvec(state)?);
    journal.replace(&bytes)?;
    Ok(())
}
