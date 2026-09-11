//! Foreground network composition. Physical owners retain the node directory
//! until every disk user has joined, including when a caller cancels shutdown.
use crate::{
    cluster::NoDirectoryAuthority,
    config::Settings,
    content_host::{ContentHost, ContentOwner},
    control_host::{ControlHost, ControlHostConfig, ControlOwner, ControlReplicationFrame},
    custody::{CustodyConfig, CustodyPolicy},
    embedded::NodeError,
    evidence_service::{EvidenceCoordinator, EvidenceDriver, EvidencePlacement},
    fleet::{
        FleetManager, FleetReplica, FleetReplication, FleetTenant, ManagedFleetConfig,
        ReplicaConfig, ReplicaFleet, ReplicaHost, ReplicaOwner,
    },
    managed_service::ManagedService,
    network_admin::{ADMIN_SOCKET, LocalNetworkAdmin, admin_wire_limits},
    network_bootstrap::{FoundingNetwork, NetworkError, signer_principal, unix_time},
    network_control::{FounderControlAuthority, NetworkEnrollmentControl},
    network_controller::{NetworkController, RegisteredEnrollment, seed_peer_registry},
    network_join::{JoinError, JoinedNode},
    network_listener::NetworkListener,
    network_state::NetworkState,
    node_directory::NodeDirectory,
    quorum_enrollment::{QuorumEnrollmentDriver, QuorumEnrollmentHost},
    replication::{drive_control_replication, drive_fleet_replication},
};
use focal_consensus::{DurableNode, NodeConfig};
use focal_control::{
    ControlEvents, ControlOptions, ControlRead, ControlReadResult, ControlReplica,
};
use focal_enrollment::{CredentialMaterial, EnrollmentReceipt};
use focal_evidence::{ContentStore, StoreLimits};
use focal_ledger::{Session, SessionLimits};
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};
use focal_memory::{
    Allocation, BudgetKind, BudgetLane, DiskBudget, DiskBudgetConfig, MemoryBudget,
};
use focal_model::*;
use focal_wire::*;
use futures_util::FutureExt;
use serde::Serialize;
use std::{
    collections::BTreeSet,
    future::Future,
    path::{Path, PathBuf},
    sync::mpsc,
    time::Duration,
};
use tokio::sync::{mpsc as async_mpsc, oneshot};

#[path = "network_directory.rs"]
mod network_directory;
use network_directory::DirectoryStartup;
pub use network_directory::{DirectoryHandle, HostRequest, HostedPartition, MAX_HOSTED_PARTITIONS};

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("network bootstrap: {0}")]
    Bootstrap(#[from] NetworkError),
    #[error("joining state: {0}")]
    Join(#[from] JoinError),
    #[error("node directory: {0}")]
    Node(#[from] NodeError),
    #[error("consensus: {0}")]
    Consensus(#[from] focal_consensus::ConsensusError),
    #[error("root metadata: {0}")]
    Control(#[from] focal_control::ControlError),
    #[error("directory startup: {0}")]
    Directory(#[from] crate::directory_bootstrap::DirectoryBootstrapError),
    #[error("root request: {0}")]
    ControlRequest(#[from] focal_control::ControlFailure),
    #[error("ledger: {0}")]
    Ledger(#[from] focal_ledger::LedgerError),
    #[error("replica fleet: {0}")]
    Fleet(#[from] crate::fleet::FleetError),
    #[error("content: {0}")]
    Content(#[from] focal_evidence::ContentError),
    #[error("wire: {0}")]
    Wire(#[from] WireError),
    #[error("replication driver: {0}")]
    Replication(#[from] crate::replication::ReplicationDriverError),
    #[error("peer transport: {0}")]
    Peer(#[from] PeerSendError),
    #[error("admission: {0}")]
    Access(#[from] AccessError),
    #[error("memory: {0}")]
    Memory(#[from] focal_memory::MemoryError),
    #[error("placement agent: {0}")]
    Agent(#[from] crate::placement_agent::AgentError),
    #[error("WAL: {0}")]
    Wal(#[from] focal_log::LogError),
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("network owner stopped: {0}")]
    Owner(&'static str),
    #[error("network controller: {0}")]
    Controller(#[from] crate::network_controller::ControllerError),
    #[error("asynchronous runtime dependency failed")]
    Runtime,
    #[error(
        "shutdown deadline elapsed; physical owners retain the directory until recovery is safe"
    )]
    ShutdownTimeout,
}

#[derive(Clone, Debug, Serialize)]
pub struct NetworkServiceStatus {
    pub condition: &'static str,
    pub ledger: LedgerId,
    pub node: u64,
    pub socket: PathBuf,
    pub admin_socket: Option<PathBuf>,
    pub listen: std::net::SocketAddr,
    pub advertise: std::net::SocketAddr,
    pub assigned_ledger: bool,
}
#[derive(Clone)]
pub struct NetworkHandles {
    pub control: ControlHost,
    pub directory: DirectoryHandle,
    pub ledger: Option<ReplicaHost>,
    pub fleet: FleetManager,
    pub evidence: EvidenceCoordinator,
    pub content: ContentHost,
    pub enrollment: Option<QuorumEnrollmentHost>,
    /// Session-fact signing and quorum collection through the placement agent.
    pub placement: crate::placement_control::PlacementHandle,
    /// This node's own credential: renewal on demand and its current state.
    pub credentials: crate::credential_renewal::CredentialHandle,
    /// The failure detector's published view and the agent's facts to it.
    pub liveness: crate::liveness::LivenessHandle,
    /// The directory's routes as this node caches them.
    pub routes: crate::route_cache_host::RouteCacheHandle,
}
#[derive(Clone)]
pub(crate) struct DataService {
    control: ControlHost,
    root_group: [u8; 16],
    directory: DirectoryHandle,
    ledger: ManagedService,
    /// The failure detector's view: a contact announcement that would move
    /// a node the detector still sees alive at its committed address is
    /// refused (24 §24), so a clone never displaces a live node.
    liveness: crate::liveness::LivenessHandle,
}
/// What the root does with a contact announcement that names another
/// address than the node's committed one (24 §24).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContactAdmission {
    /// Nothing answers as the node at its committed address, or the
    /// address is unchanged: the announcement reaches the root.
    Proceed,
    /// Something still answers as the node there: a second process with
    /// the node's identity, refused for as long as the first serves.
    Clone,
    /// The committed address is being asked: the mover is told to announce
    /// again, and does within a tick.
    Unknown,
}
impl DataService {
    /// Whether `node` may announce `advertise` in place of its committed
    /// contact. The committed address itself is asked: the detector's
    /// verdict is about the node wherever it answers from (a moved node
    /// refutes its suspicion from its new address), while a clone is a
    /// second answer at the old one. The request never waits on that probe:
    /// the driver answers with the verdict it holds or starts the probe
    /// and says so.
    async fn contact_admission(
        &self,
        node: u64,
        advertise: std::net::SocketAddr,
    ) -> ContactAdmission {
        let Ok(observation) = self.control.observe_root().await else {
            return ContactAdmission::Proceed;
        };
        let Some(record) = observation
            .contacts()
            .contacts
            .records
            .iter()
            .find(|record| record.node == node)
        else {
            return ContactAdmission::Proceed;
        };
        if record.advertise == advertise {
            return ContactAdmission::Proceed;
        }
        match self.liveness.confirm(node, record.advertise).await {
            Ok(Some(true)) => ContactAdmission::Clone,
            Ok(Some(false)) => ContactAdmission::Proceed,
            Ok(None) | Err(_) => ContactAdmission::Unknown,
        }
    }
}
impl RequestHandler for DataService {
    fn supports_managed_requests(&self) -> bool {
        true
    }
    fn supports_participant_requests(&self) -> bool {
        true
    }
    fn supports_native_requests(&self) -> bool {
        true
    }
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        Box::pin(async move {
            let group = match &request.request().operation {
                Operation::Control { group, .. }
                | Operation::PeerControl { group, .. }
                | Operation::PlacementControl { group, .. }
                | Operation::NodeContact { group, .. }
                | Operation::EnrollmentControl { group, .. }
                | Operation::Raft { group, .. } => Some(*group),
                _ => None,
            };
            if group == Some(self.root_group) {
                if let Operation::NodeContact { advertise, .. } = &request.request().operation
                    && let PeerRole::Node { node_id } = request.peer().role()
                {
                    let failure = match self.contact_admission(node_id, *advertise).await {
                        ContactAdmission::Proceed => None,
                        ContactAdmission::Clone => {
                            Some(focal_control::ControlFailure::CompareFailed)
                        }
                        ContactAdmission::Unknown => {
                            Some(focal_control::ControlFailure::Unavailable)
                        }
                    };
                    if let Some(failure) = failure {
                        let reply = focal_control::ControlReply::Rejected(failure)
                            .encode(ControlHost::wire_limits().max_frame_bytes as usize);
                        let response = match reply {
                            Ok(response) => Response::Control { response },
                            Err(_) => Response::Error(AccessError::Unavailable),
                        };
                        return OwnedResponse::new(request.request().reply(response));
                    }
                }
                return self.control.handle_accounted(request).await;
            }
            if let Some(group) = group
                && group != self.root_group
                && (group == self.directory.group()
                    || self.directory.host_of_group(group).is_some())
            {
                return match self.directory.host_of_group(group) {
                    Some(directory) => directory.handle_accounted(request).await,
                    None => OwnedResponse::new(
                        request
                            .request()
                            .reply(Response::Error(AccessError::Unavailable)),
                    ),
                };
            }
            self.ledger.handle_accounted(request).await
        })
    }
}

enum PhysicalOwner {
    Control(ControlOwner),
    Ledger(ReplicaOwner),
    Content(ContentOwner),
}
impl PhysicalOwner {
    fn join(self) -> Result<(), ServiceError> {
        match self {
            Self::Control(owner) => owner.join().map_err(ServiceError::ControlRequest),
            Self::Ledger(owner) => owner.join().map_err(ServiceError::Owner),
            Self::Content(owner) => owner.join().map_err(ServiceError::Access),
        }
    }
}
/// Root control, the first partition, the session log and the content store,
/// plus one slot for every partition a split may add on this node.
const OWNER_SLOTS: usize = 4 + network_directory::MAX_HOSTED_PARTITIONS;
struct OwnerGate {
    sender: Option<mpsc::SyncSender<OwnerRegistration>>,
    finished: oneshot::Receiver<Result<(), ServiceError>>,
}
struct OwnerRegistration {
    storage: Option<(NodeDirectory, SharedWal)>,
    owner: Option<PhysicalOwner>,
}
impl OwnerGate {
    fn start(budget: &MemoryBudget) -> Result<Self, ServiceError> {
        let charge = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                2 * 1024 * 1024 + 8192,
            )?
            .commit();
        let mut owners = Vec::new();
        owners
            .try_reserve_exact(OWNER_SLOTS)
            .map_err(|_| ServiceError::Owner("owner registry capacity"))?;
        let (sender, receiver) = mpsc::sync_channel::<OwnerRegistration>(5);
        let (done, finished) = oneshot::channel();
        std::thread::Builder::new()
            .name("focal-node-owners".into())
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let _charge = charge;
                let mut result = Ok(());
                let mut storage = None;
                while let Ok(registration) = receiver.recv() {
                    if let Some(registered) = registration.storage {
                        storage = Some(registered);
                    }
                    let Some(owner) = registration.owner else {
                        continue;
                    };
                    if owners.len() < OWNER_SLOTS {
                        owners.push(owner);
                    } else if let Err(error) = PhysicalOwner::join(owner) {
                        result = Err(error);
                    }
                }
                for owner in owners {
                    if let Err(error) = owner.join() {
                        result = Err(error);
                    }
                }
                if let Some((directory, wal)) = storage {
                    drop(wal);
                    drop(directory);
                }
                let _ = done.send(result);
            })?;
        Ok(Self {
            sender: Some(sender),
            finished,
        })
    }
    fn register(&self, owner: PhysicalOwner) -> Result<(), ServiceError> {
        self.sender
            .as_ref()
            .ok_or(ServiceError::Owner("closed owner registry"))?
            .send(OwnerRegistration {
                owner: Some(owner),
                storage: None,
            })
            .map_err(|_| ServiceError::Owner("owner registry stopped"))
    }
    fn hold(&self, directory: NodeDirectory, wal: SharedWal) -> Result<(), ServiceError> {
        self.sender
            .as_ref()
            .ok_or(ServiceError::Owner("closed owner registry"))?
            .send(OwnerRegistration {
                storage: Some((directory, wal)),
                owner: None,
            })
            .map_err(|_| ServiceError::Owner("owner registry stopped"))
    }
    async fn finish(mut self) -> Result<(), ServiceError> {
        drop(self.sender.take());
        self.finished
            .await
            .map_err(|_| ServiceError::Owner("owner registry stopped"))?
    }
}

pub struct NetworkService {
    status: NetworkServiceStatus,
    handles: NetworkHandles,
    data: DataService,
    listener: NetworkListener,
    local: UnixServer,
    admin: Option<(UnixServer, LocalNetworkAdmin)>,
    pool: PeerConnectionPool,
    registry: PeerRegistry,
    controller: Option<NetworkController>,
    credential_requests: Option<async_mpsc::Receiver<crate::credential_renewal::CredentialRequest>>,
    agent: Option<crate::placement_agent::PlacementAgent>,
    /// The collector agent (26 §5), taken by `run`.
    gc_agent: Option<crate::gc::GcAgent>,
    archive_agent: Option<crate::archive_agent::ArchiveAgent>,
    liveness: Option<crate::liveness::LivenessDriver>,
    routes: Option<crate::route_cache_host::RouteCacheDriver>,
    directory_startup: Option<DirectoryStartup>,
    control_output: Option<async_mpsc::Receiver<ControlReplicationFrame>>,
    ledger_output: Option<FleetReplication>,
    evidence: Option<EvidenceDriver>,
    signer: Option<QuorumEnrollmentDriver>,
    enrollment: Option<RegisteredEnrollment>,
    authority: FounderControlAuthority,
    founder_fingerprint: [u8; 32],
    budget: MemoryBudget,
    _configuration: Allocation,
    /// The cluster this node belongs to, for restoring the enrollment
    /// registry the metrics sampler reads the fence from.
    cluster: [u8; 16],
    /// The WAL writer, for its statistics in the metrics snapshot.
    wal: SharedWal,
    /// The fixed labels of this node's metrics and the latest snapshot the
    /// sampler published (24 §23).
    metrics_labels: crate::metrics::MetricLabels,
    metrics: tokio::sync::watch::Sender<Option<crate::metrics::MetricsSnapshot>>,
    /// The loopback endpoint bound at open when `node.metrics_listen` names one.
    metrics_listener: Option<tokio::net::TcpListener>,
    // All service handles and futures drop before registration closes. The
    // reaper retains LOCK even if a caller cancels run_until or keeps a handle.
    owners: Option<OwnerGate>,
}

struct Prepared {
    state: NetworkState,
    receipt: EnrollmentReceipt,
    credentials: CredentialMaterial,
    signing_identity: Option<CredentialMaterial>,
    enrollment: Option<QuorumEnrollmentHost>,
    signer: Option<QuorumEnrollmentDriver>,
    control: ControlReplica,
    recovered: ControlEvents,
    budget: MemoryBudget,
    wal: SharedWal,
    allocation: Allocation,
    directory: NodeDirectory,
}
impl Prepared {
    async fn open(settings: &Settings) -> Result<Self, ServiceError> {
        Box::pin(Self::open_inner(settings)).await
    }
    async fn open_inner(settings: &Settings) -> Result<Self, ServiceError> {
        settings.validate().map_err(NodeError::from)?;
        if settings
            .data_dir()
            .map_err(NodeError::from)?
            .join("JOIN.initialized")
            .exists()
        {
            let joined = JoinedNode::open(settings, unix_time()?)?;
            let startup = joined.state.startup_addresses(settings).await?;
            let mut state = joined.state;
            state.listen = startup.listen;
            state.advertise = startup.advertise;
            state.endpoint = startup.endpoint;
            if startup.changed {
                state.install(&joined.directory)?;
            }
            let budget = MemoryBudget::new(1024 * 1024 * 1024, 256 * 1024 * 1024)?;
            let allocation = budget
                .reserve(BudgetKind::Recovery, BudgetLane::Completion, 256 * 1024)?
                .commit();
            let wal = SharedWal::open_with_budgets(
                joined.directory.root().join("wal"),
                WalOptions::new(WalIdentity {
                    cluster: state.genesis.founder.cluster,
                    node: state.node,
                    stream: 0,
                }),
                WalWriterLimits::default(),
                budget.child(256 * 1024 * 1024, 64 * 1024 * 1024)?,
                disk_budget()?,
            )?;
            let options = ControlOptions::new(NodeConfig::joining(
                state.node,
                state.genesis.founder.cluster,
                state.genesis.root.group,
                vec![state.genesis.founder.node],
                vec![],
            ));
            let mut control = ControlReplica::open_on_wal(
                options,
                state.genesis.bootstrap.clone(),
                budget.child(192 * 1024 * 1024, 64 * 1024 * 1024)?,
                wal.clone(),
            )?;
            if control.identity() != state.genesis.root {
                return Err(NodeError::Identity.into());
            }
            // Apply the recovered durable prefix before deriving ingress grants.
            // Preserve its owned output for the host's normal transport framing.
            let recovered = control.drain(&NoDirectoryAuthority)?;
            Ok(Self {
                state,
                receipt: joined.receipt,
                credentials: joined.credentials,
                signing_identity: None,
                enrollment: None,
                signer: None,
                control,
                recovered,
                budget,
                wal,
                allocation,
                directory: joined.directory,
            })
        } else {
            let FoundingNetwork {
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
                _bootstrap_allocation,
                directory,
            } = FoundingNetwork::prepare(settings).await?;
            Ok(Self {
                state,
                receipt,
                credentials,
                signing_identity: Some(enrollment_identity),
                enrollment: Some(enrollment),
                signer: Some(enrollment_driver),
                control,
                recovered,
                budget,
                wal,
                allocation: _bootstrap_allocation,
                directory,
            })
        }
    }
}

impl NetworkService {
    pub fn status(&self) -> &NetworkServiceStatus {
        &self.status
    }
    pub fn handles(&self) -> NetworkHandles {
        self.handles.clone()
    }
    /// Open physical owners on a Tokio runtime with IO and time enabled. Their
    /// directory lock remains owned until every worker has joined, even if this
    /// future is canceled after the first worker starts.
    pub async fn open(settings: &Settings) -> Result<Self, ServiceError> {
        Self::open_with_socket(settings, None).await
    }
    // Tests retain ephemeral listener reservations and transfer the actual
    // socket here. Production startup uses the same TLS/listener construction.
    /// The startup state machine (every recovered owner, registry and handle
    /// across its awaits) lives on the heap, so a caller's stack carries one
    /// frame however many services it opens.
    async fn open_with_socket(
        settings: &Settings,
        socket: Option<std::net::UdpSocket>,
    ) -> Result<Self, ServiceError> {
        Box::pin(Self::open_with_socket_inner(settings, socket)).await
    }
    async fn open_with_socket_inner(
        settings: &Settings,
        socket: Option<std::net::UdpSocket>,
    ) -> Result<Self, ServiceError> {
        require_runtime()?;
        let prepared = Prepared::open(settings).await?;
        if let Some(socket) = &socket
            && socket.local_addr().map_err(WireError::from)? != prepared.state.listen
        {
            return Err(ServiceError::Owner(
                "listener socket address differs from node state",
            ));
        }
        let identity = prepared.directory.identity().clone();
        let root = prepared.directory.root().to_path_buf();
        let admin_handler = Some(LocalNetworkAdmin::for_node(
            &prepared.directory,
            prepared.state.genesis.root,
            prepared.state.advertise,
            prepared.enrollment.clone(),
            prepared
                .budget
                .child(16 * 1024 * 1024, 4 * 1024 * 1024)
                .map_err(|_| AccessError::Capacity)?,
        )?);
        // Start the reaper before moving the directory out of Prepared. A
        // thread-spawn failure therefore drops WAL/control before their LOCK.
        let owners = OwnerGate::start(&prepared.budget)?;
        let Prepared {
            state,
            receipt,
            credentials,
            signing_identity,
            enrollment,
            signer,
            control,
            recovered,
            budget,
            wal,
            allocation,
            directory,
        } = prepared;
        owners.hold(directory, wal.clone())?;
        let founder = identity.node == state.genesis.founder.node;
        let (directory, directory_startup) = DirectoryStartup::new(
            founder,
            state.genesis.founder.cluster,
            state.genesis.founder.node,
            wal.clone(),
            budget.child(192 * 1024 * 1024, 64 * 1024 * 1024)?,
            root.clone(),
        )?;
        let authority = FounderControlAuthority::from_genesis(&state.genesis)?;
        let registry = PeerRegistry::new(4096)?;
        seed_peer_registry(
            &state,
            &receipt,
            control.enrollment().ok_or(NodeError::Identity)?,
            &registry,
            unix_time()?,
        )?;
        let founder_fingerprint = founder_fingerprint(&state)?;
        // A sponsor named rather than addressed resolves at each start
        // (24 §24); an unresolvable name is not fatal here.
        let sponsor_address = crate::network_state::resolve_endpoint(&state.sponsor.endpoint)
            .await
            .ok();
        let mut controller = NetworkController::new(
            state.clone(),
            receipt.clone(),
            credentials.clone(),
            root.clone(),
            budget.child(128 * 1024 * 1024, 32 * 1024 * 1024)?,
        )?;
        if let Some(address) = sponsor_address {
            controller = controller.with_sponsor_address(address);
        }
        let mut controller = controller.with_topology(
            settings.topology.clone(),
            settings
                .placement
                .residency
                .iter()
                .chain(&settings.placement.home_regions)
                .chain(&settings.topology.region)
                .cloned()
                .collect(),
        );
        let (credential_handle, credential_requests) =
            crate::credential_renewal::CredentialHandle::channel(4);
        let limits = ControlHost::wire_limits();
        let socket = match socket {
            Some(socket) => socket,
            None => std::net::UdpSocket::bind(state.listen).map_err(WireError::from)?,
        };
        let listener = NetworkListener::from_socket(
            socket,
            &credentials,
            signing_identity.as_ref(),
            &state.sponsor.ca_certificate,
            registry.clone(),
            limits.clone(),
            budget.child(64 * 1024 * 1024, 16 * 1024 * 1024)?,
        )?;
        let bind = if state.listen.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()
        .map_err(|_| ServiceError::Owner("invalid internal bind"))?;
        let connector = QuicConnector::bind(
            bind,
            client_tls(
                TlsIdentity::from_pkcs8(
                    credentials.certificate_chain().to_vec(),
                    credentials.private_key_der().to_vec(),
                ),
                vec![state.sponsor.ca_certificate.clone()],
                &limits,
            )?,
            limits.clone(),
        )?;
        let pool = PeerConnectionPool::new(connector, PeerPoolLimits::default())?;
        let socket = root.join("focal.sock");
        clean_socket(&socket, &root)?;
        // The local grant follows the committed registry: the controller
        // adds every admitted tenant on each refresh (doc 24 §16).
        let (local_grant, local_grant_watch) = tokio::sync::watch::channel(PeerGrant {
            principal: if founder {
                identity.issuer
            } else {
                ParticipantId(receipt.identity.principal)
            },
            tenants: if founder {
                BTreeSet::from([identity.ledger.tenant, directory.namespace().tenant])
            } else {
                BTreeSet::from([identity.ledger.tenant])
            },
            role: if founder {
                PeerRole::Runtime
            } else {
                PeerRole::Actor
            },
        });
        let local = UnixServer::bind_watched(&socket, local_grant_watch, WireLimits::default())?;
        controller.follow_local_grant(local_grant);
        let mut admin = if let Some(handler) = admin_handler {
            let path = root.join(ADMIN_SOCKET);
            clean_socket(&path, &root)?;
            Some((
                UnixServer::bind(
                    path,
                    PeerGrant {
                        principal: identity.issuer,
                        tenants: BTreeSet::from([identity.ledger.tenant]),
                        role: PeerRole::Runtime,
                    },
                    admin_wire_limits(),
                )?,
                handler,
            ))
        } else {
            None
        };
        // The content store draws on the WAL's volume envelope: one
        // watermark guards every durable owner of the data directory.
        let content = ContentStore::open_with_disk(
            root.join("content"),
            StoreLimits {
                max_content_bytes: 64 * 1024 * 1024,
                max_staging_bytes: 128 * 1024 * 1024,
                max_uploads: 16,
                chunk_bytes: 1024 * 1024,
                max_manifest_bytes: 1024 * 1024,
            },
            wal.disk_budget(),
        )?;
        let tenant = budget.child(512 * 1024 * 1024, 128 * 1024 * 1024)?;
        let session = if founder {
            let consensus = DurableNode::open_on_wal_in(
                NodeConfig::single(identity.node, identity.cluster, identity.ledger.session.0),
                wal.clone(),
                &tenant,
            )?;
            Some(Session::from_node_in_hosted(
                identity.ledger,
                consensus,
                SessionLimits::default(),
                &tenant,
                native_hosting(&root, &identity, wal.disk_budget()).map_err(NodeError::Content)?,
            )?)
        } else {
            None
        };
        // The founder's custody and serving scope follow the placement its
        // session has committed; a fresh session starts at the single-node
        // scope the first registration will commit.
        let committed = session.as_ref().and_then(|session| {
            Some((
                session.active_fence()?.clone(),
                session.active_placement()?.clone(),
            ))
        });
        let mut replica_config = ReplicaConfig::new(identity.root);
        let policy = match (&committed, founder) {
            (Some((fence, spec)), _) => {
                replica_config.route_epoch = fence.to_route;
                replica_config.policy_revision = fence.placement_epoch;
                Some(CustodyPolicy {
                    ledger: identity.ledger,
                    route_epoch: fence.to_route,
                    policy_revision: fence.placement_epoch,
                    peers: spec.placement.nodes(),
                })
            }
            (None, true) => Some(CustodyPolicy {
                ledger: identity.ledger,
                route_epoch: RouteEpoch(1),
                policy_revision: 1,
                peers: BTreeSet::from([identity.node]),
            }),
            (None, false) => None,
        };
        let (placement_handle, agent_jobs) = crate::placement_control::PlacementHandle::channel(16);
        let (liveness_handle, liveness_driver) = crate::liveness::LivenessHandle::channel(
            &budget,
            crate::liveness::LivenessConfig::default(),
            identity.node,
            directory.namespace(),
        )?;
        let (route_handle, route_driver) = crate::route_cache_host::RouteCacheHandle::channel(
            &budget,
            focal_directory::RouteCacheConfig::default(),
            identity.node,
            state.genesis.founder.node,
            directory.namespace(),
            state.genesis.founder.cluster,
        )
        .map_err(|_| ServiceError::Owner("route cache"))?;
        let agent = crate::placement_agent::PlacementAgent::new(
            crate::placement_agent::AgentInputs {
                state: state.clone(),
                identity: identity.clone(),
                principal: if founder {
                    identity.issuer
                } else {
                    ParticipantId(receipt.identity.principal)
                },
                settings: settings.clone(),
                credentials,
                root: root.clone(),
                active_custody: policy.as_ref().map(CustodyPolicy::scope),
                node_budget: budget.clone(),
                admission: crate::admission::TenantAdmission::new(
                    budget.clone(),
                    crate::admission::AdmissionPolicy::standard(settings.node.max_tenants),
                    identity.ledger.tenant,
                    tenant.clone(),
                ),
                wal: wal.clone(),
                jobs: agent_jobs,
            },
            budget.child(64 * 1024 * 1024, 16 * 1024 * 1024)?,
        )?;
        let placement = match (&policy, &committed) {
            (Some(policy), Some((_, spec))) => Some(EvidencePlacement::committed(
                policy.scope(),
                spec.placement.voters.keys().copied().collect(),
                spec.placement.content_copies.keys().copied().collect(),
                // The founder alone at startup: its own declared region; the
                // agent installs every node's region as it syncs (24 §22).
                crate::placement_executor::ResidencyFence::new(
                    spec.policy.residency.clone(),
                    std::collections::BTreeMap::from([(
                        identity.node,
                        crate::topology::ids(&settings.topology).0,
                    )]),
                )
                .map_err(|_| AccessError::Capacity)?,
            )?),
            (Some(policy), None) => {
                let facts = [crate::placement::NodeFacts {
                    id: identity.node,
                    topology: settings.topology.clone(),
                    verified: true,
                    eligible: true,
                }];
                // Before its session registers a placement the founder runs
                // alone: a committed durability that needs more hosts (a
                // deployment plan applied before registration) starts at the
                // single-node scope the first registration commits.
                let plan =
                    crate::placement::plan(&facts, &settings.durability, &settings.placement)
                        .or_else(|_| {
                            crate::placement::plan(
                                &facts,
                                &crate::config::Durability::default(),
                                &settings.placement,
                            )
                        })
                        .map_err(NetworkError::from)?;
                Some(EvidencePlacement::verified(
                    policy.scope(),
                    &plan,
                    &facts,
                    &settings.placement,
                )?)
            }
            (None, _) => None,
        };
        let status = NetworkServiceStatus {
            condition: if founder { "Starting" } else { "CatchingUp" },
            ledger: identity.ledger,
            node: identity.node,
            socket,
            admin_socket: admin
                .as_ref()
                .map(|(server, _)| server.path().to_path_buf()),
            listen: listener.local_addr()?,
            advertise: state.advertise,
            assigned_ledger: founder,
        };
        let mut custody_config = CustodyConfig::new(identity.node);
        custody_config.seed_root = Some(root.join("seeds"));
        let (content, content_owner) = ContentHost::spawn(
            content,
            custody_config,
            limits.clone(),
            budget.child(192 * 1024 * 1024, 32 * 1024 * 1024)?,
        )?;
        owners.register(PhysicalOwner::Content(content_owner))?;
        if let Some(policy) = policy {
            content.install_policy(policy).await?;
        }
        let mut control_config = ControlHostConfig::new(state.genesis.root_namespace);
        control_config.enrollment_authority = Some(authority.clone());
        let (control, control_owner, control_output) = ControlHost::spawn_recovered(
            control,
            NoDirectoryAuthority,
            control_config,
            budget.child(128 * 1024 * 1024, 32 * 1024 * 1024)?,
            recovered,
        )?;
        owners.register(PhysicalOwner::Control(control_owner))?;
        // A binary behind the fence this node's root replica has applied
        // does not start serving at all (24 §21); the controller keeps
        // checking as the root advances.
        if let Ok(observation) = control.observe_root().await
            && let focal_control::ControlBootstrap::Root { enrollment, .. } =
                &observation.snapshot().state
            && let Ok(registry) = focal_enrollment::EnrollmentRegistry::restore(
                enrollment,
                identity.cluster,
                focal_enrollment::EnrollmentLimits::default(),
            )
        {
            let announced = crate::upgrade::announced_level();
            if !crate::upgrade::admits(registry.fence(), announced) {
                return Err(ServiceError::Controller(
                    crate::network_controller::ControllerError::Fenced {
                        fence: registry.fence().level,
                        announced,
                    },
                ));
            }
            // A credential the registry no longer authorizes does not start
            // (24 §11; runbooks/expired-credentials).
            if crate::network_controller::credential_retired(
                &registry,
                identity.node,
                &receipt,
                unix_time()?,
            ) {
                return Err(ServiceError::Controller(
                    crate::network_controller::ControllerError::Retired,
                ));
            }
        }
        // Every node has one bounded owner, including nodes awaiting their
        // first assignment. Installation changes routing without creating an
        // executor or physical WAL writer for each logical session.
        let (fleet, owner, ledger_output) = ReplicaFleet::spawn_managed(
            identity.node,
            identity.cluster,
            vec![wal.clone()],
            vec![FleetTenant {
                tenant: identity.ledger.tenant,
                weight: 1,
                budget: tenant,
            }],
            budget.clone(),
            ReplicaHost::wire_limits(),
            ManagedFleetConfig::default(),
        )?;
        owners.register(PhysicalOwner::Ledger(owner))?;
        let ledger = if let Some(session) = session {
            let installed = fleet
                .install(
                    1,
                    FleetReplica {
                        session,
                        config: replica_config,
                    },
                )
                .await
                .map_err(|failure| failure.error)?;
            Some(installed.value().host().clone())
        } else {
            None
        };
        let (coordinator, evidence) = EvidenceCoordinator::channel(
            content.clone(),
            identity.node,
            placement.into_iter().collect(),
            budget.child(128 * 1024 * 1024, 32 * 1024 * 1024)?,
            4,
        )?;
        let (gc_agent, gc_handle) = crate::gc::GcAgent::from_env(identity.node);
        let (archive_agent, archive_handle) = crate::archive_agent::ArchiveAgent::from_env();
        let (metrics, metrics_view) =
            tokio::sync::watch::channel::<Option<crate::metrics::MetricsSnapshot>>(None);
        let metrics_labels = crate::metrics::MetricLabels {
            node: identity.node,
            cluster: crate::cluster_admin::hex(&identity.cluster),
            region: settings.topology.region.clone(),
            zone: settings
                .topology
                .region
                .as_ref()
                .and_then(|_| settings.topology.zone.clone()),
            role: if founder { "founder" } else { "host" },
        };
        let metrics_listener = match settings.node.metrics_listen {
            Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
            None => None,
        };
        if let Some((server, handler)) = admin.take() {
            admin = Some((
                server,
                handler
                    .with_control(control.clone())?
                    .with_fleet(fleet.clone())?
                    .with_content(content.clone())
                    .with_credentials(credential_handle.clone())
                    .with_placement(placement_handle.clone())
                    .with_gc(gc_handle.clone())
                    .with_archive(archive_handle.clone())
                    .with_evidence(coordinator.clone())
                    .with_metrics(metrics_view.clone()),
            ));
        }
        let data = DataService {
            control: control.clone(),
            root_group: state.genesis.root.group,
            directory: directory.clone(),
            liveness: liveness_handle.clone(),
            ledger: ManagedService::new(fleet.clone(), content.clone(), coordinator.clone())
                .with_signing(control.clone(), placement_handle.clone())
                .with_liveness(liveness_handle.clone())
                .with_routes(
                    route_handle.clone(),
                    identity.node,
                    state.advertise,
                    receipt.identity.server_name.clone(),
                ),
        };
        let enrollment_service = enrollment
            .as_ref()
            .map(|signer| RegisteredEnrollment::new(signer.clone(), registry.clone()));
        Ok(Self {
            status,
            handles: NetworkHandles {
                control,
                directory,
                ledger,
                fleet,
                evidence: coordinator,
                content,
                enrollment,
                placement: placement_handle,
                credentials: credential_handle,
                liveness: liveness_handle,
                routes: route_handle,
            },
            data,
            listener,
            local,
            admin,
            pool,
            registry,
            controller: Some(controller),
            credential_requests: Some(credential_requests),
            agent: Some(agent),
            gc_agent: Some(gc_agent),
            archive_agent: Some(archive_agent),
            liveness: Some(liveness_driver),
            routes: Some(route_driver),
            directory_startup,
            control_output: Some(control_output),
            ledger_output: Some(ledger_output),
            evidence: Some(evidence),
            signer,
            enrollment: enrollment_service,
            authority,
            founder_fingerprint,
            budget,
            _configuration: allocation,
            cluster: identity.cluster,
            wal,
            metrics_labels,
            metrics,
            metrics_listener,
            owners: Some(owners),
        })
    }
    /// One metrics sample of everything this node knows about itself (24 §23).
    async fn sample_metrics(&self) -> crate::metrics::MetricsSnapshot {
        fn count(value: usize) -> u64 {
            u64::try_from(value).unwrap_or(u64::MAX)
        }
        use crate::metrics::{
            AgentMetrics, CredentialMetrics, LivenessMetrics, MetricsSnapshot, RootMetrics,
            SessionMetrics,
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
            .unwrap_or(0);
        let (disk, staged_uploads, staged_bytes) = match self.handles.content.disk_stats().await {
            Ok((disk, uploads, bytes)) => (Some(disk), count(uploads), bytes),
            Err(_) => (None, 0, 0),
        };
        let root = self.handles.control.progress();
        let view = self.handles.liveness.view();
        let mut liveness = LivenessMetrics {
            health_score: view.health.score,
            probes_sent: view.counters.probes_sent,
            probes_answered: view.counters.probes_answered,
            probe_timeouts: view.counters.probe_timeouts,
            suspicions: view.counters.suspicions,
            deaths: view.counters.deaths,
            refutations: view.counters.refutations,
            ..LivenessMetrics::default()
        };
        let mut peer_rtts = Vec::new();
        for (node, member) in &view.members {
            if let Some(rtt_ms) = member.rtt_ms {
                peer_rtts.push(crate::metrics::PeerRtt {
                    peer: *node,
                    rtt_ms,
                });
            }
        }
        for member in view.members.values() {
            match member.status {
                crate::liveness::MemberStatus::Alive => {
                    liveness.alive = liveness.alive.saturating_add(1)
                }
                crate::liveness::MemberStatus::Suspect => {
                    liveness.suspect = liveness.suspect.saturating_add(1)
                }
                crate::liveness::MemberStatus::Dead => {
                    liveness.dead = liveness.dead.saturating_add(1)
                }
            }
        }
        let credential = self
            .handles
            .credentials
            .current()
            .await
            .ok()
            .map(|summary| CredentialMetrics {
                expires_at: summary.expires_at,
                renewals: summary.renewals,
                rotations: summary.rotations,
            });
        let (directory, agent) = match (
            self.handles.placement.directory().await,
            self.handles.placement.status().await,
        ) {
            (Ok(directory), Ok(status)) => (
                Some(directory),
                Some(AgentMetrics {
                    root_intents: status.root_intents,
                    partition_intents: status.partition_intents,
                    installed: count(status.installed.len()),
                    last_error: status.last_error.is_some(),
                    last_refusal: status.last_refusal.is_some(),
                    admission: status.admission,
                }),
            ),
            (Ok(directory), Err(_)) => (Some(directory), None),
            (Err(_), _) => (None, None),
        };
        let mut sessions = Vec::new();
        let mut truncated = false;
        let mut after = None;
        while let Some((ledger, host)) = self.handles.fleet.next_host(after) {
            after = Some(ledger);
            if sessions.len() >= crate::metrics::MAX_SESSIONS || sessions.try_reserve(1).is_err() {
                truncated = true;
                break;
            }
            let progress = host.progress();
            let Ok(reply) = host.diagnostics().await else {
                continue;
            };
            let diagnostics = reply.value();
            let listed = directory.as_ref().and_then(|report| {
                report.partitions.iter().find_map(|(_, checkpoint)| {
                    checkpoint.sessions.get(&ledger).map(|descriptor| {
                        let guarantee =
                            focal_directory::effective_guarantee(descriptor, &checkpoint.nodes)
                                .ok();
                        (
                            descriptor.route_epoch.0,
                            descriptor.placement_epoch,
                            descriptor.active.policy.durability.max_failures,
                            guarantee
                                .as_ref()
                                .and_then(|report| report.achieved)
                                .map(|achieved| achieved.max_failures),
                            guarantee
                                .as_ref()
                                .map(|report| count(report.blocked_by.len())),
                        )
                    })
                })
            });
            sessions.push(SessionMetrics {
                tenant: ledger.tenant.to_string(),
                session: ledger.session.to_string(),
                leader: progress.leader,
                term: progress.term,
                committed_index: diagnostics.committed_index,
                applied_index: diagnostics.applied_index,
                sequence: diagnostics.sequence,
                pending: count(diagnostics.pending),
                authoritative: diagnostics.authoritative,
                native_authoritative: diagnostics.native_authoritative,
                log_entries_since_checkpoint: diagnostics.log_entries_since_checkpoint,
                retention: diagnostics.retention.clone(),
                seed_chunks_missing: diagnostics.seed_chunks_missing.map(count),
                custody_objects_missing: diagnostics.custody_objects_missing.map(count),
                delivery_retained: diagnostics.delivery_retained,
                route_epoch: listed.map(|listed| listed.0),
                placement_epoch: listed.map(|listed| listed.1),
                desired_max_failures: listed.map(|listed| listed.2),
                achieved_max_failures: listed.and_then(|listed| listed.3),
                blocked: listed.and_then(|listed| listed.4),
            });
        }
        let fence_level = self
            .handles
            .control
            .observe_root()
            .await
            .ok()
            .and_then(|observation| match &observation.snapshot().state {
                focal_control::ControlBootstrap::Root { enrollment, .. } => {
                    focal_enrollment::EnrollmentRegistry::restore(
                        enrollment,
                        self.cluster,
                        focal_enrollment::EnrollmentLimits::default(),
                    )
                    .ok()
                    .map(|registry| registry.fence().level)
                }
                _ => None,
            })
            .unwrap_or(0);
        MetricsSnapshot {
            sampled_ms: now,
            labels: self.metrics_labels.clone(),
            memory: self.budget.stats(),
            disk,
            staged_uploads,
            staged_bytes,
            wal: self.wal.stats().ok(),
            fleet: self.handles.fleet.status(),
            root: RootMetrics {
                leader: root.leader,
                term: root.term,
                applied_index: root.applied_index,
                stopped: root.stopped,
            },
            peers: self.pool.stats(),
            peer_rtts,
            liveness,
            credential,
            sessions,
            sessions_truncated: truncated,
            agent,
            fence_level,
            announced_level: crate::upgrade::announced_level(),
        }
    }

    /// Drive borrowed ingress and egress on a runtime with IO and time enabled.
    /// Shutdown closes admission and joins disk owners; cancellation keeps the
    /// directory locked while any externally retained physical handle is live.
    pub async fn run_until<F, R>(mut self, shutdown: F, on_status: R) -> Result<(), ServiceError>
    where
        F: Future<Output = std::io::Result<()>>,
        R: FnMut(&NetworkServiceStatus) -> std::io::Result<()>,
    {
        require_runtime()?;
        // The task set's state (every pinned driver future) lives on the heap
        // for the service's life, so the caller's stack carries only this
        // frame however many drivers the service composes.
        let running = std::panic::AssertUnwindSafe(Box::pin(self.run_tasks(shutdown, on_status)))
            .catch_unwind()
            .await
            .unwrap_or(Err(ServiceError::Runtime));
        self.listener.close();
        self.local.close();
        if let Some((server, _)) = &self.admin {
            server.close();
        }
        self.pool.close();
        let cleanup = async {
            let fleet = self
                .handles
                .fleet
                .stop_all()
                .await
                .map_err(ServiceError::from);
            let directory = match self.handles.directory.host() {
                Some(host) => host.stop().await.map_err(ServiceError::from),
                None => Ok(()),
            };
            // Every partition a split added on this node stops with the first.
            let mut hosted = Ok(());
            let first = self.handles.directory.plan().partition();
            for partition in self.handles.directory.hosted() {
                if partition.plan.partition() != first
                    && let Err(error) = partition.host.stop().await
                {
                    hosted = Err(ServiceError::from(error));
                }
            }
            let control = self
                .handles
                .control
                .stop()
                .await
                .map_err(ServiceError::from);
            let content = self
                .handles
                .content
                .stop()
                .await
                .map_err(ServiceError::from);
            let joined = self
                .owners
                .take()
                .ok_or(ServiceError::Owner("missing node owner"))?
                .finish()
                .await;
            joined?;
            fleet?;
            directory?;
            hosted?;
            control?;
            content?;
            self.listener.shutdown().await;
            Ok::<(), ServiceError>(())
        };
        // A future may be moved to another runtime between polls. Contain the
        // timer dependency here too, after the earlier run boundary has ended.
        std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(30), cleanup)
                .await
                .map_err(|_| ServiceError::ShutdownTimeout)?
        })
        .catch_unwind()
        .await
        .map_err(|_| ServiceError::Runtime)??;
        running
    }
    async fn run_tasks<F, R>(&mut self, shutdown: F, mut on_status: R) -> Result<(), ServiceError>
    where
        F: Future<Output = std::io::Result<()>>,
        R: FnMut(&NetworkServiceStatus) -> std::io::Result<()>,
    {
        let controller = self
            .controller
            .take()
            .ok_or(ServiceError::Owner("controller already consumed"))?;
        let output = self
            .control_output
            .take()
            .ok_or(ServiceError::Owner("egress already consumed"))?;
        let control_driver = drive_control_replication(output, &self.pool, 16);
        let directory_startup = self.directory_startup.take();
        let owners = self
            .owners
            .as_ref()
            .ok_or(ServiceError::Owner("missing node owner"))?;
        let directory_driver = async {
            match directory_startup {
                Some(startup) => startup.run(&self.handles.control, &self.pool, owners).await,
                None => std::future::pending().await,
            }
        };
        let ledger_output = self.ledger_output.take();
        let metrics_listener = self.metrics_listener.take();
        let ledger_driver = async {
            match ledger_output {
                Some(output) => drive_fleet_replication(output, &self.pool, 16)
                    .await
                    .map(|_| ()),
                None => std::future::pending().await,
            }
        };
        let evidence = self.evidence.take();
        let evidence_driver = async {
            match evidence {
                Some(driver) => driver.run(&self.pool).await,
                None => std::future::pending().await,
            }
        };
        let signer = self.signer.take();
        let signer_control = if signer.is_some() {
            Some(NetworkEnrollmentControl::new(
                &self.pool,
                &self.handles.control,
                self.authority.clone(),
                self.registry.authenticate(self.founder_fingerprint)?,
                RouteEpoch(1),
                &self.budget,
            )?)
        } else {
            None
        };
        let signer_driver = async {
            match (signer, signer_control.as_ref()) {
                (Some(driver), Some(control)) => driver.run(control).await,
                _ => std::future::pending().await,
            }
        };
        let network = self
            .listener
            .serve(self.data.clone(), self.enrollment.clone());
        let local = self.local.serve(self.data.clone());
        let admin = async {
            match &self.admin {
                Some((server, handler)) => server.serve(handler.clone()).await,
                None => std::future::pending().await,
            }
        };
        let swap = crate::credential_renewal::CredentialSwap {
            listener: Some(self.listener.identity().map_err(ServiceError::Wire)?),
            placement: self.handles.placement.clone(),
            requests: self
                .credential_requests
                .take()
                .ok_or(ServiceError::Owner("credential requests already consumed"))?,
        };
        let controller = controller.run(&self.pool, &self.handles.control, &self.registry, swap);
        let agent = self.agent.take();
        let placement_agent = async {
            match agent {
                Some(agent) => agent.run(&self.handles, &self.pool).await,
                None => std::future::pending().await,
            }
        };
        let archive_agent = self.archive_agent.take();
        let archive_agent = async {
            match archive_agent {
                Some(agent) => agent.run(&self.handles).await,
                None => std::future::pending().await,
            }
        };
        let gc_agent = self.gc_agent.take();
        let gc_agent = async {
            match gc_agent {
                Some(agent) => agent.run(&self.handles).await,
                None => std::future::pending().await,
            }
        };
        let liveness_driver = self.liveness.take();
        let liveness = async {
            match liveness_driver {
                Some(driver) => driver.run(&self.pool).await,
                None => std::future::pending().await,
            }
        };
        let route_driver = self.routes.take();
        let routes = async {
            match route_driver {
                Some(driver) => driver.run(&self.handles, &self.pool).await,
                None => std::future::pending().await,
            }
        };
        let managed_support = crate::managed_support::drive(
            &self.handles.fleet,
            &self.pool,
            &self.budget,
            &self.handles.content,
        );
        // The metrics sampler publishes a fresh snapshot on a fixed cadence
        // (24 §23); the admin socket and the loopback endpoint render the
        // latest one, never sampling on a caller's behalf.
        let metrics_sampler = async {
            loop {
                let snapshot = self.sample_metrics().await;
                self.metrics.send_replace(Some(snapshot));
                tokio::time::sleep(crate::metrics::SAMPLE_INTERVAL).await;
            }
        };
        let metrics_view = self.metrics.subscribe();
        let metrics_endpoint = async {
            match metrics_listener {
                Some(listener) => crate::metrics::serve_loopback(listener, metrics_view).await,
                None => std::future::pending().await,
            }
        };
        let ready = async {
            if let Some(ledger) = &self.handles.ledger {
                loop {
                    let peer = AuthenticatedPeer::local(PeerGrant {
                        principal: signer_principal(self.authority.identity().cluster.0),
                        tenants: BTreeSet::from([self.status.ledger.tenant]),
                        role: PeerRole::Runtime,
                    })?;
                    if matches!(
                        self.handles
                            .control
                            .read(peer, RequestId::from_u128(1), ControlRead::Membership)
                            .await,
                        Ok(ControlReadResult::Membership(_))
                    ) && {
                        // The founder's replica is running with a known
                        // leader: itself, or another voter of the group it
                        // rejoined after a restart (it follows then, and its
                        // local socket routes to the leader as any node's).
                        let progress = ledger.progress();
                        !progress.stopped && progress.leader != 0
                    } && self.handles.directory.host().is_some_and(|host| {
                        let progress = host.progress();
                        !progress.stopped
                            && progress.applied_index > 0
                            && progress.leader == progress.node
                    }) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                let mut status = self.status.clone();
                status.condition = "Ready";
                on_status(&status)?;
            } else {
                on_status(&self.status)?;
            }
            std::future::pending::<Result<(), ServiceError>>().await
        };
        tokio::pin!(
            network,
            local,
            admin,
            controller,
            placement_agent,
            archive_agent,
            gc_agent,
            liveness,
            routes,
            directory_driver,
            managed_support,
            control_driver,
            ledger_driver,
            evidence_driver,
            signer_driver,
            ready,
            shutdown,
            metrics_sampler,
            metrics_endpoint
        );
        tokio::select! {
            result=&mut shutdown=>result.map_err(ServiceError::Io),
            result=&mut ready=>result,
            result=&mut network=>result.map_err(ServiceError::Wire).and(Err(ServiceError::Owner("network listener ended"))),
            result=&mut local=>result.map_err(ServiceError::Wire).and(Err(ServiceError::Owner("local listener ended"))),
            result=&mut admin=>result.map_err(ServiceError::Wire).and(Err(ServiceError::Owner("admin listener ended"))),
            result=&mut controller=>result.map_err(ServiceError::Controller).and(Err(ServiceError::Owner("controller ended"))),
            result=&mut placement_agent=>result.map_err(ServiceError::Agent).and(Err(ServiceError::Owner("placement agent ended"))),
            result=&mut archive_agent=>result.map_err(ServiceError::Access).and(Err(ServiceError::Owner("archive agent ended"))),
            result=&mut gc_agent=>result.map_err(ServiceError::Access).and(Err(ServiceError::Owner("collector agent ended"))),
            result=&mut directory_driver=>result,
            ()=&mut liveness=>Err(ServiceError::Owner("liveness driver ended")),
            ()=&mut routes=>Err(ServiceError::Owner("route cache driver ended")),
            _=&mut managed_support=>Err(ServiceError::Owner("managed capability driver ended")),
            result=&mut control_driver=>result.map_err(ServiceError::from).and(Err(ServiceError::Owner("control egress ended"))),
            result=&mut ledger_driver=>result.map_err(ServiceError::from).and(Err(ServiceError::Owner("ledger egress ended"))),
            _=&mut evidence_driver=>Err(ServiceError::Owner("evidence driver ended")),
            _=&mut signer_driver=>Err(ServiceError::Owner("enrollment driver ended")),
            ()=&mut metrics_sampler=>Err(ServiceError::Owner("metrics sampler ended")),
            result=&mut metrics_endpoint=>result.map_err(ServiceError::Io).and(Err(ServiceError::Owner("metrics endpoint ended"))),
            _=self.handles.control.closed()=>Err(ServiceError::Owner("control owner ended")),
        }
    }
}
fn clean_socket(path: &Path, root: &Path) -> Result<(), ServiceError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};
        match std::fs::symlink_metadata(path) {
            Ok(metadata)
                if metadata.file_type().is_socket()
                    && metadata.uid() == std::fs::metadata(root)?.uid() =>
            {
                std::fs::remove_file(path)?
            }
            Ok(_) => return Err(ServiceError::Owner("refusing non-owned socket path")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        // A named-pipe rendezvous file, not a socket: remove it only when we
        // own it, so a stale one is cleared but another user's is refused.
        let _ = root;
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                if focal_platform::fs::owner_at(path)? != focal_platform::fs::current_owner()? {
                    return Err(ServiceError::Owner(
                        "refusing non-owned local endpoint path",
                    ));
                }
                std::fs::remove_file(path)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}
fn founder_fingerprint(state: &NetworkState) -> Result<[u8; 32], ServiceError> {
    let focal_control::ControlBootstrap::Root { enrollment, .. } = &state.genesis.bootstrap else {
        return Err(NodeError::Identity.into());
    };
    let registry = focal_enrollment::EnrollmentRegistry::restore(
        enrollment,
        state.genesis.founder.cluster,
        focal_enrollment::EnrollmentLimits::default(),
    )
    .map_err(NetworkError::from)?;
    let founder = registry.enrollments().next().ok_or(NodeError::Identity)?;
    Ok(certificate_fingerprint(&founder.certificate))
}

#[cfg(test)]
#[path = "network_service_tests.rs"]
pub(crate) mod tests;

fn require_runtime() -> Result<(), ServiceError> {
    tokio::runtime::Handle::try_current().map_err(|_| NetworkError::RuntimeRequired)?;
    std::panic::catch_unwind(|| drop(tokio::time::sleep(Duration::ZERO)))
        .map_err(|_| ServiceError::Runtime)?;
    Ok(())
}

/// Every node hosts the native engine over its content directory; the
/// committed activation of each ledger decides whether it is used. The reader
/// is lock-free and shares the directory with the exclusive content writer.
pub(crate) fn native_hosting(
    root: &Path,
    identity: &crate::embedded::NodeIdentity,
    disk: focal_memory::DiskBudget,
) -> Result<focal_ledger::NativeHosting, focal_evidence::ContentError> {
    Ok(focal_ledger::NativeHosting {
        limits: native_limits(focal_model::ContentDomainId(identity.ledger.tenant.0))?,
        reader: focal_evidence::ContentReader::open(root.join("content"))?,
        seeds: focal_evidence::SeedStore::open(
            crate::custody::seed_directory(&root.join("seeds"), identity.ledger),
            disk,
        )?,
        range: focal_memory::RangeId(u128::from(identity.node)),
    })
}
/// Free bytes the WAL filesystem must keep before a fresh native candidate is
/// admitted. Unset keeps the standard 64 MiB watermark; zero disables it; a
/// campaign raises it above the volume's free space to force `Capacity` for
/// fresh work while exact retries of committed work still answer.
pub(crate) const DISK_HEADROOM_ENV: &str = "FOCAL_DISK_HEADROOM_BYTES";
/// The largest native Core root a checkpoint carries inline; a larger one
/// travels as seeds (25 §5). Unset keeps the standard 4 MiB; a campaign
/// lowers it so every checkpoint is seeded.
pub(crate) const SEED_INLINE_ENV: &str = "FOCAL_SEED_INLINE_BYTES";
pub(crate) fn seed_inline_bytes() -> Result<usize, focal_evidence::ContentError> {
    match std::env::var_os(SEED_INLINE_ENV) {
        Some(value) => value
            .to_str()
            .and_then(|text| text.trim().parse::<usize>().ok())
            .ok_or(focal_evidence::ContentError::Invalid),
        None => Ok(focal_ledger::native_checkpoint::Limits::default().inline_bytes),
    }
}
/// The operator's disk headroom, or the standard watermark.
pub(crate) fn disk_headroom_bytes() -> Result<u64, focal_evidence::ContentError> {
    match std::env::var_os(DISK_HEADROOM_ENV) {
        Some(value) => value
            .to_str()
            .and_then(|text| text.trim().parse::<u64>().ok())
            .ok_or(focal_evidence::ContentError::Invalid),
        None => Ok(DiskBudgetConfig::default().headroom),
    }
}
/// One disk envelope per node volume: the WAL writer, the content store and
/// the checkpoint rewrites all promise their bytes from it before any
/// acknowledgement, under the standard physical watermark. The operator's
/// `FOCAL_DISK_HEADROOM_BYTES` is the native admission gate for fresh work
/// and leaves this envelope alone, so a campaign that raises the gate above
/// the volume's free space still lets the node recover, serve reads and
/// answer exact retries.
pub(crate) fn disk_budget() -> Result<DiskBudget, focal_evidence::ContentError> {
    DiskBudget::new(DiskBudgetConfig::default()).map_err(|_| focal_evidence::ContentError::Invalid)
}
/// The standard native session limits with the operator's disk headroom.
pub(crate) fn native_limits(
    domain: focal_model::ContentDomainId,
) -> Result<focal_ledger::NativeSessionLimits, focal_evidence::ContentError> {
    let mut limits = focal_ledger::NativeSessionLimits::standard(domain);
    limits.disk_headroom_bytes = disk_headroom_bytes()?;
    limits.checkpoint.inline_bytes = seed_inline_bytes()?;
    // Committed records this node did not author are materialized in
    // dependency waves on up to four workers (doc 25 §2); the result is
    // byte-identical to the serial replay at any count.
    limits.materializer.max_workers = std::thread::available_parallelism()
        .map_or(1, |count| count.get())
        .clamp(1, 4);
    Ok(limits)
}
