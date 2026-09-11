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
            _journal: journal,
        })
    }
    pub fn invitation(&self) -> &ClientInvitation {
        &self.bundle
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
            &self.bundle.invitation().trust().ca_certificate,
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
