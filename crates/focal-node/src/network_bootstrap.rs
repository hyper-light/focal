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
    BootstrapAuthority, CredentialMaterial, FoundingEnrollmentDraft, JoinKey, PrivateJournal,
    ServerTrust, server_fingerprint,
};
use focal_log::WalIdentity;
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
    /// The node's storage as its start opened it (`storage_start`).
    pub storage: focal_consensus::storage_open::OpenedStorage,
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
        Box::pin(Self::prepare_inner(settings)).await
    }
    async fn prepare_inner(settings: &Settings) -> NetworkResult<Self> {
        Self::open_inner(settings, false).await
    }
    async fn open_inner(settings: &Settings, local_readiness: bool) -> NetworkResult<Self> {
        tokio::runtime::Handle::try_current().map_err(|_| NetworkError::RuntimeRequired)?;
        let directory = NodeDirectory::open(settings)?;
        let identity = directory.identity();
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
        let (listen, advertise, endpoint) = match &saved {
            Some(state) => {
                let startup = state.startup_addresses(settings).await?;
                (startup.listen, startup.advertise, startup.endpoint)
            }
            None => {
                let (listen, advertise) = resolve_addresses(settings).await?;
                (
                    listen,
                    advertise,
                    crate::network_state::advertised_name(settings),
                )
            }
        };
        // The founder bootstrap - PKI generation, WAL and consensus recovery,
        // enrollment - is unbounded CPU- and IO-blocking work. Run it on a
        // blocking thread so it neither stalls the async runtime nor executes on
        // the executor's shallow poll stack (a Windows main thread is 1 MiB).
        let settings = settings.clone();
        tokio::task::spawn_blocking(move || {
            Self::bootstrap_blocking(
                &settings,
                directory,
                saved,
                listen,
                advertise,
                endpoint,
                local_readiness,
            )
        })
        .await
        .map_err(|_| NetworkError::RuntimeRequired)?
    }
    /// The synchronous founder bootstrap, run off the executor by [open_inner].
    fn bootstrap_blocking(
        settings: &Settings,
        directory: NodeDirectory,
        saved: Option<NetworkState>,
        listen: std::net::SocketAddr,
        advertise: std::net::SocketAddr,
        endpoint: Option<String>,
        local_readiness: bool,
    ) -> NetworkResult<Self> {
        let identity = directory.identity();
        let budget = crate::memory_envelope::node_budget(settings)?;
        // Covers bounded manifest decode, founding PKI/draft buffers and duplicate
        // serialized bootstrap inputs during recovery. Shrunk before publication.
        let mut bootstrap_allocation = budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                4 * 1024 * 1024,
            )?
            .commit();
        let committed = crate::embedded::check_policy(directory.root(), settings)?;
        // The founder pins its policy alone, so the first start must be
        // satisfiable by this node's own facts. A committed policy is the
        // directory's to satisfy: `apply deployment` commits stronger
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
        let limits = settings.enrollment_limits();
        // The bootstrap server certificate lasts the cluster's credential
        // lifetime, as everything the cluster issues does, and succeeds
        // itself before it expires (24 §11).
        let authority = BootstrapAuthority::open_or_create_for(
            private.join("authority"),
            identity.cluster,
            names.clone(),
            limits.credential_lifetime,
            limits.issuer_lifetime,
            now,
        )?;
        let key = JoinKey::open_or_create(private.join("node-key"), identity.cluster)?;
        let founder = FoundingEnrollmentDraft::open_or_create(
            private.join("founder"),
            &authority,
            &key,
            identity.node,
            identity.issuer.0,
            limits.clone(),
            // The fence a fresh cluster holds from genesis is what its
            // founder announces (24 §21): the compiled level, or the lower
            // one an operator staged a rollout at — never a level the
            // founder itself would refuse to serve under.
            crate::upgrade::announced_level(),
            now,
        )?;
        // The credential lifetime is the cluster's policy, committed in the
        // registry at genesis; a start that asks for another is a
        // committed-policy change (08 §2), never a silent one.
        if founder.registry().limits().credential_lifetime != limits.credential_lifetime {
            return Err(
                NodeError::Config(crate::config::ConfigError::CommittedPolicyChange {
                    field: "node.credential_lifetime_seconds",
                })
                .into(),
            );
        }
        if founder.registry().limits().issuer_lifetime != limits.issuer_lifetime {
            return Err(
                NodeError::Config(crate::config::ConfigError::CommittedPolicyChange {
                    field: "node.issuer_lifetime_seconds",
                })
                .into(),
            );
        }
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
            schema: crate::network_state::NETWORK_STATE_SCHEMA,
            node: identity.node,
            listen,
            advertise,
            endpoint: endpoint.clone(),
            sponsor: ServerTrust {
                // Invitations name the founder as its operator did (24 §24):
                // a name outlives the address behind it.
                endpoint: endpoint.clone().unwrap_or_else(|| advertise.to_string()),
                server_name: names.first().ok_or(NodeError::Identity)?.clone(),
                ca_certificate: authority.ca_certificate().to_vec(),
                server_fingerprint: server_fingerprint(authority.server_certificate()),
                successor_fingerprint: authority
                    .successor()
                    .map(|(certificate, _)| server_fingerprint(certificate)),
                issuers: founder.registry().issuers().trusted().cloned().collect(),
            },
            genesis: NetworkGenesis {
                founder: identity.clone(),
                root: control_identity,
                root_namespace: root_namespace(identity),
                bootstrap: bootstrap.clone(),
            },
        };
        // A restart may resolve a new listen/advertise address (a rescheduled
        // pod, a fresh DHCP lease, a moved VM): peers reach the founder by its
        // pinned endpoint name, not this transient snapshot. `install` accepts a
        // saved state whose identity (node, sponsor trust, genesis) is unchanged
        // and rewrites only the transport addresses; a real identity change is
        // still refused. A strict equality here would corrupt-fail every
        // restart that resolved a new address.
        if saved.as_ref().is_some_and(|saved| saved != &state) {
            state.install(&directory)?;
        }
        state.validate(identity)?;
        let storage = crate::storage_start::open(
            directory.root(),
            WalIdentity {
                cluster: identity.cluster,
                node: identity.node,
                stream: 0,
            },
            &budget,
            &settings
                .root_key_file(identity.node)
                .map_err(NodeError::Config)?,
        )?;
        let mut control = ControlReplica::open_on_storage(
            options,
            bootstrap,
            budget.child(192 * 1024 * 1024, 64 * 1024 * 1024)?,
            &storage.storage,
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
        // The credential the founder presents is the receipt its key holds:
        // the genesis one at the first start, the latest renewal installed
        // since (24 §11). When the committed registry renewed the same key
        // and the install was lost to a crash, the committed renewal is
        // adopted here, as the controller would adopt it; a rotation the
        // registry committed for a staged key is left to the controller,
        // which adopts it from that key.
        if key.enrollment()?.is_none() {
            key.complete(
                founder.receipt(),
                founder.registry().issuers().trusted(),
                now,
            )?;
        }
        let held = key.enrollment()?.ok_or(NodeError::Identity)?;
        let committed = enrollment
            .enrollments()
            .find(|listed| listed.identity.node_id == Some(identity.node))
            .ok_or(NodeError::Identity)?;
        let receipt = if committed.identity == held.identity
            && committed.public_key == held.public_key
            && committed.expires_at > held.expires_at
        {
            committed.clone()
        } else {
            held
        };
        if receipt.identity != founder.receipt().identity {
            return Err(NodeError::Identity.into());
        }
        // A renewal the founder holds may be one its own replica has yet to
        // apply: the replica follows the root's leader as any member's
        // does, and a restart knows only what its log says committed.
        enrollment.authorize_held(&receipt, now)?;
        let credentials = key.renew(&receipt, enrollment.issuers().trusted(), now)?;
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
            storage,
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
