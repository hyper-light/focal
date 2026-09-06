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
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
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
pub use network_directory::DirectoryHandle;
use network_directory::DirectoryStartup;

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
}
#[derive(Clone)]
struct DataService {
    control: ControlHost,
    root_group: [u8; 16],
    directory: DirectoryHandle,
    ledger: ManagedService,
}
impl RequestHandler for DataService {
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        Box::pin(async move {
            let group = match &request.request().operation {
                Operation::Control { group, .. }
                | Operation::PeerControl { group, .. }
                | Operation::NodeContact { group, .. }
                | Operation::EnrollmentControl { group, .. }
                | Operation::Raft { group, .. } => Some(*group),
                _ => None,
            };
            if group == Some(self.root_group) {
                return self.control.handle_accounted(request).await;
            }
            if group == Some(self.directory.group()) {
                return match self.directory.host() {
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
            .try_reserve_exact(4)
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
                    if owners.len() < 4 {
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
        settings.validate().map_err(NodeError::from)?;
        if settings
            .data_dir()
            .map_err(NodeError::from)?
            .join("JOIN.initialized")
            .exists()
        {
            let joined = JoinedNode::open(settings, unix_time()?)?;
            let (listen, advertise) = joined.state.startup_addresses(settings).await?;
            let mut state = joined.state;
            state.listen = listen;
            state.advertise = advertise;
            let budget = MemoryBudget::new(1024 * 1024 * 1024, 256 * 1024 * 1024)?;
            let allocation = budget
                .reserve(BudgetKind::Recovery, BudgetLane::Completion, 256 * 1024)?
                .commit();
            let wal = SharedWal::open_with_budget(
                joined.directory.root().join("wal"),
                WalOptions::new(WalIdentity {
                    cluster: state.genesis.founder.cluster,
                    node: state.node,
                    stream: 0,
                }),
                WalWriterLimits::default(),
                budget.child(256 * 1024 * 1024, 64 * 1024 * 1024)?,
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
        require_runtime()?;
        let prepared = Prepared::open(settings).await?;
        let identity = prepared.directory.identity().clone();
        let root = prepared.directory.root().to_path_buf();
        let admin_handler = prepared
            .enrollment
            .as_ref()
            .map(|enrollment| {
                LocalNetworkAdmin::new(
                    &prepared.directory,
                    prepared.state.genesis.root,
                    prepared.state.advertise,
                    enrollment.clone(),
                    prepared
                        .budget
                        .child(16 * 1024 * 1024, 4 * 1024 * 1024)
                        .map_err(|_| AccessError::Capacity)?,
                )
            })
            .transpose()?;
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
        let controller = NetworkController::new(
            state.clone(),
            receipt.clone(),
            root.clone(),
            budget.child(128 * 1024 * 1024, 32 * 1024 * 1024)?,
        )?;
        let limits = ControlHost::wire_limits();
        let listener = NetworkListener::bind(
            state.listen,
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
        let local = UnixServer::bind(
            &socket,
            PeerGrant {
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
            },
            WireLimits::default(),
        )?;
        let admin = if let Some(handler) = admin_handler {
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
        let content = ContentStore::open(
            root.join("content"),
            StoreLimits {
                max_content_bytes: 64 * 1024 * 1024,
                max_staging_bytes: 128 * 1024 * 1024,
                max_uploads: 16,
                chunk_bytes: 1024 * 1024,
                max_manifest_bytes: 1024 * 1024,
            },
        )?;
        let tenant = budget.child(512 * 1024 * 1024, 128 * 1024 * 1024)?;
        let session = if founder {
            let consensus = DurableNode::open_on_wal_in(
                NodeConfig::single(identity.node, identity.cluster, identity.ledger.session.0),
                wal.clone(),
                &tenant,
            )?;
            Some(Session::from_node_in(
                identity.ledger,
                consensus,
                SessionLimits::default(),
                &tenant,
            )?)
        } else {
            None
        };
        let policy = founder.then(|| CustodyPolicy {
            ledger: identity.ledger,
            route_epoch: RouteEpoch(1),
            policy_revision: 1,
            peers: BTreeSet::from([identity.node]),
        });
        let placement = if let Some(policy) = &policy {
            let facts = [crate::placement::NodeFacts {
                id: identity.node,
                topology: settings.topology.clone(),
                verified: true,
                eligible: true,
            }];
            let plan = crate::placement::plan(&facts, &settings.durability, &settings.placement)
                .map_err(NetworkError::from)?;
            Some(EvidencePlacement::verified(
                policy.scope(),
                &plan,
                &facts,
                &settings.placement,
            )?)
        } else {
            None
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
        let (content, content_owner) = ContentHost::spawn(
            content,
            CustodyConfig::new(identity.node),
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
        // Every node has one bounded owner, including nodes awaiting their
        // first assignment. Installation changes routing without creating an
        // executor or physical WAL writer for each logical session.
        let (fleet, owner, ledger_output) = ReplicaFleet::spawn_managed(
            identity.node,
            identity.cluster,
            vec![wal],
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
                        config: ReplicaConfig::new(identity.root),
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
        let data = DataService {
            control: control.clone(),
            root_group: state.genesis.root.group,
            directory: directory.clone(),
            ledger: ManagedService::new(fleet.clone(), content.clone(), coordinator.clone()),
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
            },
            data,
            listener,
            local,
            admin,
            pool,
            registry,
            controller: Some(controller),
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
            owners: Some(owners),
        })
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
        let running = std::panic::AssertUnwindSafe(self.run_tasks(shutdown, on_status))
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
            control?;
            content?;
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
        let controller = controller.run(&self.pool, &self.handles.control, &self.registry);
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
                    ) && ledger.membership().await.is_ok()
                        && self.handles.directory.host().is_some_and(|host| {
                            let progress = host.progress();
                            !progress.stopped
                                && progress.applied_index > 0
                                && progress.leader == progress.node
                        })
                    {
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
            directory_driver,
            control_driver,
            ledger_driver,
            evidence_driver,
            signer_driver,
            ready,
            shutdown
        );
        tokio::select! {
            result=&mut shutdown=>result.map_err(ServiceError::Io),
            result=&mut ready=>result,
            result=&mut network=>result.map_err(ServiceError::Wire).and(Err(ServiceError::Owner("network listener ended"))),
            result=&mut local=>result.map_err(ServiceError::Wire).and(Err(ServiceError::Owner("local listener ended"))),
            result=&mut admin=>result.map_err(ServiceError::Wire).and(Err(ServiceError::Owner("admin listener ended"))),
            result=&mut controller=>result.map_err(ServiceError::Controller).and(Err(ServiceError::Owner("controller ended"))),
            result=&mut directory_driver=>result,
            result=&mut control_driver=>result.map_err(ServiceError::from).and(Err(ServiceError::Owner("control egress ended"))),
            result=&mut ledger_driver=>result.map_err(ServiceError::from).and(Err(ServiceError::Owner("ledger egress ended"))),
            _=&mut evidence_driver=>Err(ServiceError::Owner("evidence driver ended")),
            _=&mut signer_driver=>Err(ServiceError::Owner("enrollment driver ended")),
            _=self.handles.control.closed()=>Err(ServiceError::Owner("control owner ended")),
        }
    }
}
fn clean_socket(path: &Path, root: &Path) -> Result<(), ServiceError> {
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
mod tests;

fn require_runtime() -> Result<(), ServiceError> {
    tokio::runtime::Handle::try_current().map_err(|_| NetworkError::RuntimeRequired)?;
    std::panic::catch_unwind(|| drop(tokio::time::sleep(Duration::ZERO)))
        .map_err(|_| ServiceError::Runtime)?;
    Ok(())
}
