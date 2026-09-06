//! Pinned invitation transport and durable joining identity. Enrollment grants
//! a Node certificate only; no root voter, domain replica or Runtime is created.
use crate::{
    config::Settings,
    embedded::NodeError,
    network_state::{NetworkGenesis, NetworkState},
    node_directory::{JoinDirectory, NodeDirectory},
};
use focal_control::{
    ControlBootstrap, ControlRead, ControlReadResult, ControlReply, ControlRpc, ControlSnapshot,
};
use focal_enrollment::{
    CredentialMaterial, EnrollmentClient, EnrollmentError, EnrollmentLimits, EnrollmentReceipt,
    EnrollmentRegistry, EnrollmentRole, Invitation, JoinKey, JoinTransportError, PrivateJournal,
};
use focal_model::{RequestEpoch, RequestId, RouteEpoch};
use focal_wire::{
    Operation, PROTOCOL_VERSION, QuicConnector, RequestEnvelope, Response, TlsIdentity,
};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    net::SocketAddr,
    path::Path,
};
use zeroize::Zeroizing;

const MAGIC: &[u8; 8] = b"FCLINV01";
const CLIENT_MAGIC: &[u8; 8] = b"FCLCLI01";
#[path = "client_join.rs"]
mod client;
pub use client::{ClientInvitation, PendingClientJoin};
#[path = "joined_context.rs"]
mod joined_context;
pub use joined_context::{joined_unix_principal, local_unix_principal};
const MAX_BUNDLE: usize = 48 * 1024;
const MAX_JOURNAL: usize = 60 * 1024;
#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    #[error("node identity: {0}")]
    Node(#[from] NodeError),
    #[error("enrollment: {0}")]
    Enrollment(#[from] EnrollmentError),
    #[error("enrollment transport: {0}")]
    Transport(#[from] JoinTransportError),
    #[error("root discovery: {0}")]
    Wire(#[from] focal_wire::WireError),
    #[error("root metadata: {0}")]
    Control(#[from] focal_control::ControlError),
    #[error("root metadata rejected discovery: {0}")]
    Discovery(#[from] focal_control::ControlFailure),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invitation or saved join state is invalid")]
    Invalid,
    #[error("invitation or join identity conflicts with durable state")]
    Conflict,
    #[error("private invitation file permissions are unsafe")]
    Permissions,
    #[error("join state exceeds its bounded allowance")]
    Capacity,
    #[error("join has no saved committed enrollment; resume the pinned enrollment request")]
    Pending,
    #[error("asynchronous transport dependency failed")]
    Runtime,
}
impl From<postcard::Error> for JoinError {
    fn from(_: postcard::Error) -> Self {
        Self::Invalid
    }
}

/// Secret-bearing operator bundle. Debug never exposes its token or encoded bytes.
pub struct NodeInvitation {
    name: String,
    genesis: NetworkGenesis,
    invitation: Invitation,
}
impl std::fmt::Debug for NodeInvitation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeInvitation")
            .field("node_name", &self.name)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}
#[derive(Serialize)]
struct BundleRef<'a> {
    schema: u16,
    name: &'a str,
    genesis: &'a NetworkGenesis,
    token: &'a str,
}
#[derive(Deserialize)]
struct BundleOwned {
    schema: u16,
    name: String,
    genesis: NetworkGenesis,
    token: PrivateText,
}
struct PrivateText(Zeroizing<String>);
impl<'de> Deserialize<'de> for PrivateText {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        Ok(Self(Zeroizing::new(String::deserialize(decoder)?)))
    }
}
struct PrivateBytes(Zeroizing<Vec<u8>>);
impl Serialize for PrivateBytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_slice().serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for PrivateBytes {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        Ok(Self(Zeroizing::new(Vec::deserialize(decoder)?)))
    }
}
impl NodeInvitation {
    pub fn new(
        name: impl Into<String>,
        genesis: NetworkGenesis,
        invitation: Invitation,
    ) -> Result<Self, JoinError> {
        let value = Self {
            name: name.into(),
            genesis,
            invitation,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn genesis(&self) -> &NetworkGenesis {
        &self.genesis
    }
    pub fn invitation(&self) -> &Invitation {
        &self.invitation
    }
    pub fn validate(&self) -> Result<(), JoinError> {
        self.validate_role(EnrollmentRole::Node)
    }
    fn validate_role(&self, role: EnrollmentRole) -> Result<(), JoinError> {
        if self.name.is_empty()
            || self.name.len() > 63
            || !self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || self.invitation.role() != role
            || self.invitation.cluster() != self.genesis.founder.cluster
        {
            return Err(JoinError::Invalid);
        }
        let address = self
            .invitation
            .trust()
            .endpoint
            .parse::<SocketAddr>()
            .map_err(|_| JoinError::Invalid)?;
        NetworkState {
            schema: 1,
            node: self.genesis.founder.node,
            listen: address,
            advertise: address,
            sponsor: self.invitation.trust().clone(),
            genesis: self.genesis.clone(),
        }
        .validate(&self.genesis.founder)?;
        Ok(())
    }
    pub fn encode(&self) -> Result<Zeroizing<Vec<u8>>, JoinError> {
        self.encode_role(EnrollmentRole::Node)
    }
    fn encode_role(&self, role: EnrollmentRole) -> Result<Zeroizing<Vec<u8>>, JoinError> {
        self.validate_role(role)?;
        let token = self.invitation.expose_token()?;
        let value = BundleRef {
            schema: 1,
            name: &self.name,
            genesis: &self.genesis,
            token: &token,
        };
        let size = postcard::experimental::serialized_size(&value)?;
        if size.checked_add(40).is_none_or(|bytes| bytes > MAX_BUNDLE) {
            return Err(JoinError::Capacity);
        }
        let payload = Zeroizing::new(postcard::to_stdvec(&value)?);
        let mut bytes = Zeroizing::new(Vec::new());
        bytes
            .try_reserve_exact(size.checked_add(40).ok_or(JoinError::Capacity)?)
            .map_err(|_| JoinError::Capacity)?;
        bytes.extend_from_slice(match role {
            EnrollmentRole::Node => MAGIC,
            EnrollmentRole::Client => CLIENT_MAGIC,
        });
        bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, JoinError> {
        Self::decode_role(bytes, EnrollmentRole::Node)
    }
    fn decode_role(bytes: &[u8], role: EnrollmentRole) -> Result<Self, JoinError> {
        if bytes.len() > MAX_BUNDLE {
            return Err(JoinError::Capacity);
        }
        let payload = bytes.get(40..).ok_or(JoinError::Invalid)?;
        let magic = match role {
            EnrollmentRole::Node => MAGIC,
            EnrollmentRole::Client => CLIENT_MAGIC,
        };
        if bytes.get(..8) != Some(magic.as_slice())
            || bytes.get(8..40) != Some(blake3::hash(payload).as_bytes().as_slice())
        {
            return Err(JoinError::Invalid);
        }
        let (value, tail): (BundleOwned, _) = postcard::take_from_bytes(payload)?;
        if !tail.is_empty() || value.schema != 1 {
            return Err(JoinError::Invalid);
        }
        let value = Self {
            name: value.name,
            genesis: value.genesis,
            invitation: Invitation::parse(&value.token.0)?,
        };
        value.validate_role(role)?;
        Ok(value)
    }
    pub fn load(path: impl AsRef<Path>) -> Result<Self, JoinError> {
        Self::decode(&read_private(path.as_ref(), MAX_BUNDLE)?)
    }
    /// Atomic install never overwrites a different file. An exact retry also
    /// resyncs the existing inode and directory before reporting success.
    pub fn write_new(&self, path: impl AsRef<Path>) -> Result<(), JoinError> {
        write_private_new(path.as_ref(), &self.encode()?)
    }
}
#[derive(Serialize, Deserialize)]
struct JoinJournal {
    schema: u16,
    bundle: PrivateBytes,
    listen: SocketAddr,
    advertise: SocketAddr,
    key: Option<KeyPin>,
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct KeyPin {
    request: [u8; 16],
    csr: [u8; 32],
}
/// Holds both the physical directory lease and private journal/key leases.
pub struct PendingJoin {
    bundle: NodeInvitation,
    listen: SocketAddr,
    advertise: SocketAddr,
    key: JoinKey,
    _journal: PrivateJournal,
    directory: JoinDirectory,
}
impl PendingJoin {
    pub fn open(
        settings: &Settings,
        bundle: NodeInvitation,
        listen: SocketAddr,
        advertise: SocketAddr,
    ) -> Result<Self, JoinError> {
        Self::build(settings, Some((bundle, listen, advertise)))
    }
    /// Requires the persisted JOIN record; never creates a replacement invitation.
    pub fn resume(settings: &Settings) -> Result<Self, JoinError> {
        Self::build(settings, None)
    }
    fn build(
        settings: &Settings,
        expected: Option<(NodeInvitation, SocketAddr, SocketAddr)>,
    ) -> Result<Self, JoinError> {
        let directory = JoinDirectory::open(settings)?;
        let join = directory.root().join("JOIN");
        if expected.is_none() && !join.join("journal.bin").is_file() {
            return Err(JoinError::Invalid);
        }
        let mut journal = PrivateJournal::open(&join)?;
        let saved = journal.read()?;
        let expected = expected
            .map(|(bundle, listen, advertise)| {
                Ok::<_, JoinError>((bundle.encode()?, listen, advertise))
            })
            .transpose()?;
        let mut state = if let Some(bytes) = saved {
            let (state, tail): (JoinJournal, _) = postcard::take_from_bytes(&bytes)?;
            if !tail.is_empty() || state.schema != 1 {
                return Err(JoinError::Invalid);
            }
            if expected
                .as_ref()
                .is_some_and(|(bundle, listen, advertise)| {
                    state.bundle.0.as_slice() != bundle.as_slice()
                        || state.listen != *listen
                        || state.advertise != *advertise
                })
            {
                return Err(JoinError::Conflict);
            }
            state
        } else {
            if join.join("node-key").exists() || directory.root().join("IDENTITY").exists() {
                return Err(JoinError::Invalid);
            }
            let (bundle, listen, advertise) = expected.ok_or(JoinError::Invalid)?;
            let state = JoinJournal {
                schema: 1,
                bundle: PrivateBytes(bundle),
                listen,
                advertise,
                key: None,
            };
            validate_journal(&state)?;
            save_journal(&mut journal, &state)?;
            state
        };
        let bundle = validate_journal(&state)?;
        let marker = directory.root().join("JOIN.initialized");
        if marker.exists()
            && crate::embedded::read_bounded(&marker, 64)?.as_slice()
                != b"durable join intent installed"
        {
            return Err(JoinError::Invalid);
        }
        crate::embedded::atomic_file(&marker, b"durable join intent installed")?;
        let keypath = join.join("node-key");
        if state.key.is_some() && !keypath.join("join-key.bin").is_file() {
            return Err(JoinError::Invalid);
        }
        let key = JoinKey::open_or_create(&keypath, bundle.invitation.cluster())?;
        let pin = KeyPin {
            request: key.request_id(),
            csr: *blake3::hash(key.csr()).as_bytes(),
        };
        match &state.key {
            Some(existing) if existing != &pin => return Err(JoinError::Conflict),
            Some(_) => {}
            None => {
                state.key = Some(pin);
                save_journal(&mut journal, &state)?;
            }
        }
        Ok(Self {
            bundle,
            listen: state.listen,
            advertise: state.advertise,
            key,
            _journal: journal,
            directory,
        })
    }
    pub fn invitation(&self) -> &NodeInvitation {
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
    pub async fn redeem(
        &self,
        client: &EnrollmentClient,
        now: i64,
    ) -> Result<EnrollmentReceipt, JoinError> {
        if let Some(receipt) = self.key.enrollment()? {
            self.verify(&receipt, now)?;
            return Ok(receipt);
        }
        let address = self
            .bundle
            .invitation
            .trust()
            .endpoint
            .parse()
            .map_err(|_| JoinError::Invalid)?;
        let receipt = std::panic::AssertUnwindSafe(client.redeem(
            address,
            &self.bundle.invitation,
            &self.key,
            now,
        ))
        .catch_unwind()
        .await
        .map_err(|_| JoinError::Runtime)??;
        // Verification persists the public receipt before a caller can lose it.
        drop(self.verify(&receipt, now)?);
        Ok(receipt)
    }
    fn verify(
        &self,
        receipt: &EnrollmentReceipt,
        now: i64,
    ) -> Result<CredentialMaterial, JoinError> {
        if receipt.invitation != self.bundle.invitation.id()
            || receipt.issued_at > now
            || receipt.identity.role != EnrollmentRole::Node
            || receipt
                .identity
                .node_id
                .is_none_or(|id| id == 0 || id == self.bundle.genesis.founder.node)
        {
            return Err(JoinError::Invalid);
        }
        Ok(self
            .key
            .complete(receipt, &self.bundle.invitation.trust().ca_certificate, now)?)
    }
    pub fn install(self, receipt: EnrollmentReceipt, now: i64) -> Result<JoinedNode, JoinError> {
        let credentials = self.verify(&receipt, now)?;
        let mut identity = self.bundle.genesis.founder.clone();
        identity.node = receipt.identity.node_id.ok_or(JoinError::Invalid)?;
        let state = NetworkState {
            schema: 1,
            node: identity.node,
            listen: self.listen,
            advertise: self.advertise,
            sponsor: self.bundle.invitation.trust().clone(),
            genesis: self.bundle.genesis.clone(),
        };
        state.validate(&identity)?;
        let directory = self.directory.install(identity)?;
        state.install(&directory)?;
        Ok(JoinedNode {
            state,
            credentials,
            receipt,
            directory,
        })
    }
}
/// Durable verified enrollment, without an assigned voter or domain replica.
pub struct JoinedNode {
    pub state: NetworkState,
    pub credentials: CredentialMaterial,
    pub receipt: EnrollmentReceipt,
    pub directory: NodeDirectory,
}
impl JoinedNode {
    pub fn open(settings: &Settings, now: i64) -> Result<Self, JoinError> {
        let pending = PendingJoin::resume(settings)?;
        let receipt = pending.enrollment()?.ok_or(JoinError::Pending)?;
        pending.install(receipt, now)
    }
    /// Discover current root state over the issued Node certificate. The server
    /// must independently grant its active committed Node enrollment. This read
    /// neither submits a metadata mutation nor derives Runtime/voter authority.
    pub async fn discover_root(&self, now: i64) -> Result<ControlSnapshot, JoinError> {
        let operation = async {
            let snapshot = match self.root_request(&self.discovery_request()?).await? {
                ControlReply::Read(ControlReadResult::State(state)) => state,
                ControlReply::Rejected(error) => return Err(error.into()),
                _ => return Err(JoinError::Invalid),
            };
            if snapshot.identity != self.state.genesis.root {
                return Err(JoinError::Invalid);
            }
            let ControlBootstrap::Root {
                directory,
                enrollment,
            } = &snapshot.state
            else {
                return Err(JoinError::Invalid);
            };
            if directory.cluster.0 != self.directory.identity().cluster {
                return Err(JoinError::Invalid);
            }
            let active = EnrollmentRegistry::restore(
                enrollment,
                self.directory.identity().cluster,
                EnrollmentLimits::default(),
            )?;
            if active.ca_certificate() != self.state.sponsor.ca_certificate
                || active.authorize_certificate(&self.receipt.certificate, now)?
                    != self.receipt.identity
            {
                return Err(JoinError::Invalid);
            }
            Ok(snapshot)
        };
        std::panic::AssertUnwindSafe(operation)
            .catch_unwind()
            .await
            .map_err(|_| JoinError::Runtime)?
    }
    /// The first contact intent is entirely recoverable from JOIN and its
    /// verified enrollment. Unknown outcomes reuse this exact sequence/address;
    /// no local acknowledgement can manufacture a root commit.
    pub fn contact_request(&self) -> Result<RequestEnvelope, JoinError> {
        self.state.validate(self.directory.identity())?;
        if self.receipt.identity.node_id != Some(self.state.node)
            || self.receipt.identity.role != EnrollmentRole::Node
        {
            return Err(JoinError::Invalid);
        }
        Ok(RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: self.state.genesis.root_namespace,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId(self.receipt.request),
            operation: Operation::NodeContact {
                group: self.state.genesis.root.group,
                sequence: 1,
                acknowledged_through: 0,
                expected_generation: 0,
                advertise: self.state.advertise,
            },
        })
    }
    pub async fn announce_contact(&self) -> Result<focal_control::ControlReceipt, JoinError> {
        match self.root_request(&self.contact_request()?).await? {
            ControlReply::Committed(receipt)
                if receipt.request.client == self.receipt.identity.principal
                    && receipt.request.sequence == 1
                    && receipt.committed_index > 0
                    && receipt.committed_term > 0 =>
            {
                Ok(receipt)
            }
            ControlReply::Rejected(error) => Err(error.into()),
            _ => Err(JoinError::Invalid),
        }
    }
    /// Public committed contacts are a discovery result, not an authority grant.
    pub async fn discover_contacts(&self) -> Result<focal_control::ContactSnapshot, JoinError> {
        let mut request = self.discovery_request()?;
        request.operation = Operation::PeerControl {
            group: self.state.genesis.root.group,
            request: ControlRpc::Read(ControlRead::Contacts)
                .encode(focal_wire::MAX_PEER_CONTROL_REQUEST_BYTES)?,
        };
        match self.root_request(&request).await? {
            ControlReply::Read(ControlReadResult::Contacts(snapshot))
                if snapshot.identity == self.state.genesis.root
                    && snapshot.contacts.cluster == self.directory.identity().cluster
                    && snapshot.contacts.applied_index <= snapshot.applied_index =>
            {
                Ok(snapshot)
            }
            ControlReply::Rejected(error) => Err(error.into()),
            _ => Err(JoinError::Invalid),
        }
    }
    async fn root_request(&self, request: &RequestEnvelope) -> Result<ControlReply, JoinError> {
        let operation = async {
            let limits = crate::control_host::ControlHost::wire_limits();
            let registry = genesis_registry(&self.state.genesis)?;
            let founder = registry.enrollments().next().ok_or(JoinError::Invalid)?;
            let address: SocketAddr = self
                .state
                .sponsor
                .endpoint
                .parse()
                .map_err(|_| JoinError::Invalid)?;
            let bind = if address.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            }
            .parse()
            .map_err(|_| JoinError::Invalid)?;
            let connector = QuicConnector::bind(
                bind,
                focal_wire::client_tls(
                    TlsIdentity::from_pkcs8(
                        self.credentials.certificate_chain().to_vec(),
                        self.credentials.private_key_der().to_vec(),
                    ),
                    vec![self.state.sponsor.ca_certificate.clone()],
                    &limits,
                )?,
                limits.clone(),
            )?;
            let remote = connector
                .connect(address, &founder.identity.server_name)
                .await?;
            let response = remote.request(request).await;
            remote.close();
            let response = response?;
            if response.ledger != request.ledger
                || response.request_id != request.request_id
                || response.request_epoch != request.request_epoch
                || response.protocol != PROTOCOL_VERSION
            {
                return Err(JoinError::Invalid);
            }
            let response = match response.result {
                Response::Control { response } => response,
                Response::Error(error) => return Err(focal_wire::WireError::Access(error).into()),
                _ => return Err(JoinError::Invalid),
            };
            Ok(ControlReply::decode(
                &response,
                limits.max_frame_bytes as usize,
            )?)
        };
        std::panic::AssertUnwindSafe(operation)
            .catch_unwind()
            .await
            .map_err(|_| JoinError::Runtime)?
    }
    pub fn discovery_request(&self) -> Result<RequestEnvelope, JoinError> {
        Ok(RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: self.state.genesis.root_namespace,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId(self.receipt.request),
            operation: Operation::PeerControl {
                group: self.state.genesis.root.group,
                request: ControlRpc::Read(ControlRead::State)
                    .encode(focal_wire::MAX_PEER_CONTROL_REQUEST_BYTES)?,
            },
        })
    }
}
fn genesis_registry(genesis: &NetworkGenesis) -> Result<EnrollmentRegistry, JoinError> {
    let ControlBootstrap::Root { enrollment, .. } = &genesis.bootstrap else {
        return Err(JoinError::Invalid);
    };
    Ok(EnrollmentRegistry::restore(
        enrollment,
        genesis.founder.cluster,
        EnrollmentLimits::default(),
    )?)
}
fn validate_journal(journal: &JoinJournal) -> Result<NodeInvitation, JoinError> {
    let bundle = NodeInvitation::decode(&journal.bundle.0)?;
    NetworkState {
        schema: 1,
        node: bundle.genesis.founder.node,
        listen: journal.listen,
        advertise: journal.advertise,
        sponsor: bundle.invitation.trust().clone(),
        genesis: bundle.genesis.clone(),
    }
    .validate(&bundle.genesis.founder)?;
    Ok(bundle)
}
fn save_journal(journal: &mut PrivateJournal, state: &JoinJournal) -> Result<(), JoinError> {
    if postcard::experimental::serialized_size(state)? > MAX_JOURNAL {
        return Err(JoinError::Capacity);
    }
    let bytes = Zeroizing::new(postcard::to_stdvec(state)?);
    journal.replace(&bytes)?;
    Ok(())
}
fn read_private(path: &Path, max: usize) -> Result<Zeroizing<Vec<u8>>, JoinError> {
    check_private(path)?;
    let mut file = File::open(path)?;
    if file.metadata()?.len() > max as u64 {
        return Err(JoinError::Capacity);
    }
    let mut bytes = Zeroizing::new(Vec::new());
    bytes
        .try_reserve_exact(max.checked_add(1).ok_or(JoinError::Capacity)?)
        .map_err(|_| JoinError::Capacity)?;
    std::io::Read::by_ref(&mut file)
        .take(
            u64::try_from(max)
                .map_err(|_| JoinError::Capacity)?
                .checked_add(1)
                .ok_or(JoinError::Capacity)?,
        )
        .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(JoinError::Capacity);
    }
    Ok(bytes)
}
fn check_private(path: &Path) -> Result<(), JoinError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file() || metadata.mode() & 0o777 != 0o600 || metadata.nlink() != 1 {
            return Err(JoinError::Permissions);
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(JoinError::Permissions)
    }
}
fn write_private_new(path: &Path, bytes: &[u8]) -> Result<(), JoinError> {
    use fs2::FileExt;
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path.file_name().ok_or(JoinError::Invalid)?;
    let digest = blake3::hash(name.as_encoded_bytes()).to_hex();
    let temporary = parent.join(format!(".focal-invitation-{digest}.pending"));
    let lock_path = parent.join(format!(".focal-invitation-{digest}.lock"));
    if fs::symlink_metadata(&lock_path).is_ok() {
        check_private(&lock_path)?;
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    return Err(JoinError::Permissions);
    let lock = options.open(&lock_path)?;
    check_private(&lock_path)?;
    lock.try_lock_exclusive()?;
    if fs::symlink_metadata(path).is_ok() {
        recover_output_link(path, &temporary)?;
        if read_private(path, MAX_BUNDLE)?.as_slice() != bytes {
            return Err(JoinError::Conflict);
        }
        File::open(path)?.sync_all()?;
        File::open(parent)?.sync_all()?;
        return Ok(());
    }
    // The destination was never installed. A private interrupted temporary is
    // unacknowledged; the held output lock makes its removal unambiguous.
    if fs::symlink_metadata(&temporary).is_ok() {
        check_private(&temporary)?;
        fs::remove_file(&temporary)?;
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::hard_link(&temporary, path)?;
        Ok::<_, std::io::Error>(())
    })();
    drop(file);
    let cleanup = fs::remove_file(&temporary);
    match result {
        Ok(()) => {
            cleanup?;
            File::open(parent)?.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            cleanup?;
            if read_private(path, MAX_BUNDLE)?.as_slice() != bytes {
                return Err(JoinError::Conflict);
            }
            File::open(path)?.sync_all()?;
            File::open(parent)?.sync_all()?;
            Ok(())
        }
        Err(error) => {
            let _ = cleanup;
            Err(error.into())
        }
    }
}
fn recover_output_link(path: &Path, temporary: &Path) -> Result<(), JoinError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let target = fs::symlink_metadata(path)?;
        if target.nlink() == 2 {
            let pending = fs::symlink_metadata(temporary)?;
            if !target.is_file()
                || !pending.is_file()
                || target.mode() & 0o777 != 0o600
                || pending.mode() & 0o777 != 0o600
                || pending.dev() != target.dev()
                || pending.ino() != target.ino()
            {
                return Err(JoinError::Permissions);
            }
            // Recover the hard-link/unlink crash window without following or
            // removing any arbitrary sibling path.
            fs::remove_file(temporary)?;
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, temporary);
        Err(JoinError::Permissions)
    }
}
#[cfg(test)]
#[path = "network_join_tests.rs"]
mod tests;
