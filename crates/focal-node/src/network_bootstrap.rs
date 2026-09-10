//! Founding network expansion preserves the local node/session/WAL identity.
//! A public manifest pins the initial root group; private signing and retry state
//! must exist before that manifest can make networking restartable.
use crate::{
    cluster::NoDirectoryAuthority,
    config::Settings,
    embedded::{NodeError, install_policy},
    network_state::{NetworkGenesis, NetworkState, resolve_addresses, root_group, root_namespace},
    node_directory::NodeDirectory,
    quorum_enrollment::{QuorumEnrollmentConfig, QuorumEnrollmentDriver, QuorumEnrollmentHost},
};
use focal_control::{ControlBootstrap, ControlEvents, ControlOptions, ControlReplica};
use focal_directory::{RootConfig, RootDirectory};
use focal_enrollment::{
    BootstrapAuthority, CredentialMaterial, EnrollmentLimits, FoundingEnrollmentDraft, JoinKey,
    PrivateJournal, ServerTrust, server_fingerprint,
};
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::ParticipantId;
use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("node initialization: {0}")]
    Node(#[from] NodeError),
    #[error("root metadata: {0}")]
    Control(#[from] focal_control::ControlError),
    #[error("directory: {0}")]
    Directory(#[from] focal_directory::DirectoryError),
    #[error("enrollment: {0}")]
    Enrollment(#[from] focal_enrollment::EnrollmentError),
    #[error("enrollment signer: {0}")]
    Signer(#[from] crate::quorum_enrollment::QuorumEnrollmentError),
    #[error("memory admission: {0}")]
    Memory(#[from] focal_memory::MemoryError),
    #[error("WAL: {0}")]
    Wal(#[from] focal_log::LogError),
    #[error("deployment guarantee: {0}")]
    Placement(#[from] crate::placement::PlacementError),
    #[error("encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("network initialization requires a Tokio runtime")]
    RuntimeRequired,
    #[error("an enrolled peer requires the replicated network owner")]
    EnrolledPeer,
    #[error("legacy enrollment bootstrap requires an explicit migration")]
    LegacyBootstrap,
    #[error("expanded root membership requires the replicated network owner")]
    ReplicatedRoot,
    #[error("founding root has not established its current-term quorum barrier")]
    QuorumUnavailable,
    #[error("previously initialized network credentials are missing")]
    MissingCredentials,
    #[error("system clock is outside the supported Unix timestamp range")]
    Clock,
}
pub type NetworkResult<T> = Result<T, NetworkError>;
pub struct FoundingNetwork {
    pub state: NetworkState,
    pub control: ControlReplica,
    pub(crate) recovered: ControlEvents,
    pub credentials: CredentialMaterial,
    pub enrollment_identity: CredentialMaterial,
    pub receipt: focal_enrollment::EnrollmentReceipt,
    pub enrollment: QuorumEnrollmentHost,
    pub enrollment_driver: QuorumEnrollmentDriver,
    pub budget: MemoryBudget,
    pub wal: SharedWal,
    // Bootstrap metadata is bounded and retained with its owner, not separately
    // shared. The signer and control owner account for their own retained state.
    pub(crate) _bootstrap_allocation: Allocation,
    // The physical directory is released after every owned store/driver.
    pub directory: NodeDirectory,
}
impl FoundingNetwork {
    pub async fn open(settings: &Settings) -> NetworkResult<Self> {
        Self::open_inner(settings, true).await
    }
    /// Recover durable state without demanding a local one-voter election.
    /// The foreground service starts transports and establishes quorum readiness.
    pub async fn prepare(settings: &Settings) -> NetworkResult<Self> {
        Self::open_inner(settings, false).await
    }
    async fn open_inner(settings: &Settings, local_readiness: bool) -> NetworkResult<Self> {
        tokio::runtime::Handle::try_current().map_err(|_| NetworkError::RuntimeRequired)?;
        let directory = NodeDirectory::open(settings)?;
        let identity = directory.identity();
        let budget = MemoryBudget::new(1024 * 1024 * 1024, 256 * 1024 * 1024)?;
        // Covers bounded manifest decode, founding PKI/draft buffers and duplicate
        // serialized bootstrap inputs during recovery. Shrunk before publication.
        let mut bootstrap_allocation = budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                4 * 1024 * 1024,
            )?
            .commit();
        let saved = NetworkState::load(&directory)?;
        if saved
            .as_ref()
            .is_some_and(|state| state.genesis.founder.node != identity.node)
        {
            return Err(NetworkError::EnrolledPeer);
        }
        if directory.root().join("cluster/BOOTSTRAP").exists()
            || directory
                .root()
                .join("cluster/BOOTSTRAP.initialized")
                .exists()
        {
            return Err(NetworkError::LegacyBootstrap);
        }
        let (listen, advertise) = match &saved {
            Some(state) => state.startup_addresses(settings).await?,
            None => resolve_addresses(settings).await?,
        };
        let committed = crate::embedded::check_policy(directory.root(), settings)?;
        // The founder pins its policy alone, so the first start must be
        // satisfiable by this node's own facts. A committed policy is the
        // directory's to satisfy: `deployment apply` commits stronger
        // durability than one host provides, and a restart must not refuse
        // what the fleet already carries.
        if committed.is_none() {
            crate::placement::plan(
                &[crate::placement::NodeFacts {
                    id: identity.node,
                    topology: settings.topology.clone(),
                    verified: true,
                    eligible: true,
                }],
                &settings.durability,
                &settings.placement,
            )?;
            install_policy(directory.root(), settings)?;
        }
        let now = unix_time()?;
        let names = vec![format!("cluster-{}.focal.internal", hex(&identity.cluster))];
        let private = directory.root().join("cluster/network");
        let founder_path = private.join("founder/founder.bin");
        // A crash may leave private intent before NETWORK was installed. That
        // intent still pins its existing keys; open_or_create must not replace
        // any missing ancestor after downstream state has become durable.
        let signer_exists = private.join("signer").exists();
        let founder_exists = founder_path.exists();
        if (saved.is_some() || signer_exists) && !founder_path.is_file()
            || (saved.is_some() || signer_exists || founder_exists)
                && (!private.join("authority/authority.bin").is_file()
                    || !private.join("node-key/join-key.bin").is_file())
            || private.join("node-key/join-key.bin").exists()
                && !private.join("authority/authority.bin").is_file()
        {
            return Err(NetworkError::MissingCredentials);
        }
        crate::embedded::durable_dir(&private)?;
        let authority = BootstrapAuthority::open_or_create(
            private.join("authority"),
            identity.cluster,
            names.clone(),
            now,
        )?;
        let key = JoinKey::open_or_create(private.join("node-key"), identity.cluster)?;
        let founder = FoundingEnrollmentDraft::open_or_create(
            private.join("founder"),
            &authority,
            &key,
            identity.node,
            identity.issuer.0,
            EnrollmentLimits::default(),
            now,
        )?;
        let root_directory = RootDirectory::new(
            focal_directory::ClusterId(identity.cluster),
            RootConfig::default(),
            budget.child(64 * 1024 * 1024, 16 * 1024 * 1024)?,
        )?;
        let bootstrap = ControlBootstrap::root(&root_directory, founder.registry())?;
        let options = ControlOptions::new(focal_consensus::NodeConfig::single(
            identity.node,
            identity.cluster,
            root_group(identity.cluster),
        ));
        let control_identity = bootstrap.identity(&options)?;
        let state = NetworkState {
            schema: 1,
            node: identity.node,
            listen,
            advertise,
            sponsor: ServerTrust {
                endpoint: advertise.to_string(),
                server_name: names.first().ok_or(NodeError::Identity)?.clone(),
                ca_certificate: authority.ca_certificate().to_vec(),
                server_fingerprint: server_fingerprint(authority.server_certificate()),
            },
            genesis: NetworkGenesis {
                founder: identity.clone(),
                root: control_identity,
                root_namespace: root_namespace(identity),
                bootstrap: bootstrap.clone(),
            },
        };
        if saved.as_ref().is_some_and(|saved| saved != &state) {
            return Err(NodeError::Identity.into());
        }
        state.validate(identity)?;
        let wal = SharedWal::open_with_budgets(
            directory.root().join("wal"),
            WalOptions::new(WalIdentity {
                cluster: identity.cluster,
                node: identity.node,
                stream: 0,
            }),
            WalWriterLimits::default(),
            budget.child(256 * 1024 * 1024, 64 * 1024 * 1024)?,
            crate::network_service::disk_budget().map_err(NodeError::Content)?,
        )?;
        let mut control = ControlReplica::open_on_wal(
            options,
            bootstrap,
            budget.child(192 * 1024 * 1024, 64 * 1024 * 1024)?,
            wal.clone(),
        )?;
        if control.identity() != state.genesis.root {
            return Err(NodeError::Identity.into());
        }
        // Recovery precedes election; this founding constructor cannot reset an
        // established multi-voter group to one voter after its quorum disappears.
        let mut recovered = control.drain(&NoDirectoryAuthority)?;
        if local_readiness && !recovered.messages.is_empty() {
            return Err(NodeError::Identity.into());
        }
        let status = control.status();
        if local_readiness && (status.voters != [identity.node] || !status.learners.is_empty()) {
            return Err(NetworkError::ReplicatedRoot);
        }
        if local_readiness {
            control.campaign()?;
            for _ in 0..4 {
                if !control.drain(&NoDirectoryAuthority)?.messages.is_empty() {
                    return Err(NodeError::Identity.into());
                }
            }
            const BARRIER: &[u8] = b"focal.network.genesis-ready.v1";
            control.read_index(BARRIER.to_vec())?;
            recovered = control.drain(&NoDirectoryAuthority)?;
            if !recovered.read_states.iter().any(|barrier| {
                barrier.context == BARRIER && barrier.index <= control.applied_index()
            }) {
                return Err(NetworkError::QuorumUnavailable);
            }
        }
        // Recovery may include revocation after genesis. The immutable founding
        // draft cannot override the live committed registry's authorization.
        let enrollment = control.enrollment().ok_or(NodeError::Identity)?;
        if enrollment.authorize_certificate(&founder.receipt().certificate, now)?
            != founder.receipt().identity
        {
            return Err(NodeError::Identity.into());
        }
        let credentials = key.complete(founder.receipt(), authority.ca_certificate(), now)?;
        let enrollment_identity = authority.server_identity();
        let principal = signer_principal(identity.cluster);
        let config = QuorumEnrollmentConfig::new(
            control_identity,
            principal,
            state.sponsor.server_name.clone(),
            BTreeSet::from([identity.ledger.tenant]),
        );
        let signer_path = private.join("signer");
        let initialized = if saved.is_some() {
            true
        } else {
            let journal = PrivateJournal::open(&signer_path)?;
            journal.read()?.is_some()
        };
        let allowance = budget.child(64 * 1024 * 1024, 16 * 1024 * 1024)?;
        let (enrollment, enrollment_driver) = if initialized {
            QuorumEnrollmentHost::open(authority, signer_path, config, allowance)?
        } else {
            QuorumEnrollmentHost::create(authority, signer_path, config, allowance)?
        };
        state.install(&directory)?;
        let receipt = founder.receipt().clone();
        drop(founder);
        drop(key);
        drop(saved);
        drop(root_directory);
        bootstrap_allocation.shrink_to(256 * 1024)?;
        Ok(Self {
            state,
            control,
            recovered,
            credentials,
            enrollment_identity,
            receipt,
            enrollment,
            enrollment_driver,
            budget,
            wal,
            _bootstrap_allocation: bootstrap_allocation,
            directory,
        })
    }
}
pub fn signer_principal(cluster: [u8; 16]) -> ParticipantId {
    let digest = blake3::derive_key("focal.network.signer-principal.v1", &cluster);
    let mut id = [0; 16];
    for (target, source) in id.iter_mut().zip(digest) {
        *target = source;
    }
    ParticipantId(id)
}
pub fn unix_time() -> NetworkResult<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| NetworkError::Clock)?;
    i64::try_from(elapsed.as_secs()).map_err(|_| NetworkError::Clock)
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::new();
    for byte in bytes {
        for half in [byte >> 4, byte & 15] {
            if let Some(digit) = DIGITS.get(usize::from(half)) {
                value.push(char::from(*digit));
            }
        }
    }
    value
}

#[cfg(test)]
#[path = "network_bootstrap_tests.rs"]
mod tests;
