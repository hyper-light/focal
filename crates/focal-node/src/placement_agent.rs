//! The per-node placement agent: it registers this node and the session it
//! founded with the partition directory, reports the node's load and disk
//! headroom, installs the copies a committed plan assigns to it, and answers
//! assignments with readiness it verifies and signs itself. Every command it
//! submits is journaled first and retried with the identical identity, so a
//! restart neither repeats nor loses a decision.
//!
//! The partition owner is reached locally where it is hosted and led by this
//! node, and otherwise over the node-only placement protocol at the node the
//! first partition was delegated to.
use crate::{
    admission::TenantAdmission,
    config::Settings,
    control_host::{ControlHost, RootObservation},
    custody::{CustodyPolicy, CustodyScope},
    embedded::{NodeIdentity, atomic_file},
    evidence_service::EvidencePlacement,
    fleet::{FleetError, FleetReplica, ReplicaConfig, ReplicaHost},
    network_bootstrap::unix_time,
    network_service::NetworkHandles,
    network_state::NetworkState,
    placement_collect::{CollectError, Collected, remote_signature},
    placement_control::{AgentJob, AgentStatus, CollectJob, CollectRequest, SignJob},
    placement_journal::{IntentError, IntentJournal},
    placement_proof::{PlacementProofError, ProofWindow},
    session_registration::{
        FirstSessionPlan, HostedSessionFacts, SessionRegistrationError, control_evidence,
    },
};
use focal_consensus::{ConsensusError, DurableNode, NodeConfig};
use focal_control::{
    ControlAuthoritySnapshot, ControlBootstrap, ControlCommand, ControlFailure, ControlRead,
    ControlReadResult, ControlReceipt, ControlReply, ControlRequest, ControlRpc, ControlSnapshot,
    VerifiedPartitionCommand,
};
use focal_directory::{
    AssignmentPhase, AssignmentProgress, Delegation, NodeLoad, OperationId, PartitionCheckpoint,
    PartitionCommand, PartitionId, PartitionOperation, PlacementPhase, ReplicaReady, SessionChange,
    SessionDescriptor, roles_of,
};
use focal_enrollment::{CredentialMaterial, PrivateJournal};
use focal_ledger::{LedgerError, Session, SessionLimits};
use focal_log::SharedWal;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{
    ContentHash, LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch, TenantId,
};
use focal_wire::{
    AccessError, AuthenticatedPeer, MAX_PEER_CONTROL_REQUEST_BYTES,
    MAX_PLACEMENT_CONTROL_REQUEST_BYTES, Operation, PROTOCOL_VERSION, PeerConnectionPool,
    PeerGrant, PeerRole, PeerSendError, RequestEnvelope,
};
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Duration,
};
use tokio::sync::mpsc;

/// Seconds between routine load reports; a report is also due whenever the
/// node's enrollment generation changes or no report exists.
const LOAD_INTERVAL: i64 = 30;
/// Proof lifetime for self-signed readiness and session facts.
const PROOF_WINDOW: i64 = 60;
const EVIDENCE_TTL: Duration = Duration::from_secs(10);
/// Seconds between readiness attempts for one plan.
const READY_INTERVAL: i64 = 2;
const TICK: Duration = Duration::from_millis(250);
/// Pause after a tick that failed for a reason a later observation may clear.
const BACKOFF: Duration = Duration::from_secs(5);
const REMOTE_TIMEOUT: Duration = Duration::from_secs(5);
/// Copies one node may host through this agent.
const MAX_INSTALLED: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("placement agent state is inconsistent with this node")]
    Identity,
    #[error("placement agent admission exceeded")]
    Capacity,
    #[error("placement agent owner stopped")]
    Stopped,
    #[error("placement agent runtime is unavailable")]
    Runtime,
    #[error("placement intent: {0}")]
    Intent(#[from] IntentError),
    #[error("metadata owner: {0}")]
    Control(#[from] ControlFailure),
    #[error("session registration: {0}")]
    Registration(#[from] SessionRegistrationError),
    #[error("session owner: {0}")]
    Ledger(#[from] LedgerError),
    #[error("readiness proof: {0}")]
    Proof(#[from] PlacementProofError),
    #[error("content custody: {0}")]
    Custody(#[from] AccessError),
    #[error("network clock: {0}")]
    Network(#[from] crate::network_bootstrap::NetworkError),
    #[error("partition host: {0}")]
    Bootstrap(#[from] crate::directory_bootstrap::DirectoryBootstrapError),
    #[error("replica open: {0}")]
    Consensus(#[from] ConsensusError),
    #[error("replica fleet: {0}")]
    Fleet(#[from] FleetError),
    #[error("content hosting: {0}")]
    Content(#[from] focal_evidence::ContentError),
    #[error("peer transport: {0}")]
    Transport(#[from] PeerSendError),
    #[error("install journal: {0}")]
    Journal(#[from] focal_enrollment::EnrollmentError),
    #[error("install journal encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("install journal io: {0}")]
    Io(#[from] std::io::Error),
    #[error("a learner is still behind the committed log")]
    Behind,
    #[error("tenant admission: {0}")]
    Admission(#[from] crate::admission::AdmissionRefusal),
    #[error("session fact collection: {0}")]
    Collect(#[from] CollectError),
}
impl AgentError {
    /// The partition or session owner is mid-transition; the same intent or a
    /// fresh observation answers on the next tick.
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Control(
                ControlFailure::NotLeader { .. }
                    | ControlFailure::NotReady
                    | ControlFailure::Capacity
                    | ControlFailure::Unavailable
                    | ControlFailure::OutcomeUnknown,
            ) | Self::Ledger(
                LedgerError::NotReady { .. } | LedgerError::Capacity | LedgerError::OutcomeUnknown,
            ) | Self::Registration(SessionRegistrationError::NotReady)
                | Self::Proof(PlacementProofError::Unavailable | PlacementProofError::Capacity)
                | Self::Custody(AccessError::Unavailable | AccessError::Capacity)
                | Self::Transport(_)
                | Self::Fleet(FleetError::Capacity | FleetError::Unavailable)
                | Self::Bootstrap(
                    crate::directory_bootstrap::DirectoryBootstrapError::Unavailable
                        | crate::directory_bootstrap::DirectoryBootstrapError::NotReady
                        | crate::directory_bootstrap::DirectoryBootstrapError::Capacity
                )
                | Self::Capacity
                | Self::Behind
                | Self::Collect(_)
        )
    }
}

/// What one tick established, for tests and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentStep {
    /// Nothing to do, or no partition owner answers yet.
    Idle,
    /// A journaled intent was submitted or resolved, or a copy was installed.
    Advanced,
}

struct Journals {
    root: IntentJournal,
    /// One exact-retry journal per partition this node acts on, opened the
    /// first time the root's delegation is visited.
    partitions: BTreeMap<PartitionId, IntentJournal>,
}
/// One partition as this tick observed it: the root's delegation, how it is
/// reached, and its committed state and installed authority.
type Observed = (
    Delegation,
    PartitionAccess,
    ControlSnapshot,
    ControlAuthoritySnapshot,
);
fn observed_partition(
    observed: &[Observed],
    partition: PartitionId,
) -> Option<(
    &PartitionCheckpoint,
    &ControlSnapshot,
    &ControlAuthoritySnapshot,
)> {
    observed
        .iter()
        .find(|(delegation, ..)| delegation.partition == partition)
        .and_then(|(_, _, snapshot, installed)| match &snapshot.state {
            ControlBootstrap::Partition { directory } => Some((directory, snapshot, installed)),
            _ => None,
        })
}

/// A copy this node installed for a committed plan, reopened at every start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InstalledCopy {
    group: [u8; 16],
    bootstrap_voters: Vec<u64>,
    route_epoch: RouteEpoch,
    policy_revision: u64,
    voters: BTreeSet<u64>,
    copies: BTreeSet<u64>,
}
#[derive(Serialize, Deserialize)]
struct InstallRecord {
    schema: u16,
    node: u64,
    installed: BTreeMap<LedgerId, InstalledCopy>,
}
struct InstallJournal {
    journal: Option<PrivateJournal>,
    record: InstallRecord,
}
impl InstallJournal {
    fn open(root: &std::path::Path, node: u64) -> Result<Self, AgentError> {
        let path = root.join("cluster/placement-installs");
        let marker = root.join("PLACEMENT-INSTALLS.initialized");
        if marker.exists() && !path.join("journal.bin").is_file() {
            return Err(AgentError::Identity);
        }
        crate::embedded::durable_dir(&root.join("cluster"))?;
        let mut journal = PrivateJournal::open(path)?;
        let record = match journal.read()? {
            Some(bytes) => {
                let (value, rest): (InstallRecord, _) = postcard::take_from_bytes(&bytes)?;
                if !rest.is_empty() {
                    return Err(AgentError::Identity);
                }
                value
            }
            None => {
                let value = InstallRecord {
                    schema: 1,
                    node,
                    installed: BTreeMap::new(),
                };
                journal.replace(&postcard::to_stdvec(&value)?)?;
                value
            }
        };
        if record.schema != 1 || record.node != node || record.installed.len() > MAX_INSTALLED {
            return Err(AgentError::Identity);
        }
        if !marker.exists() {
            atomic_file(&marker, b"placement install journal initialized")?;
        }
        Ok(Self {
            journal: Some(journal),
            record,
        })
    }
    async fn save_on(&mut self, host: &ControlHost) -> Result<(), AgentError> {
        crate::control_host::save_local_intent(
            host,
            &mut self.journal,
            postcard::to_stdvec(&self.record)?,
        )
        .await
        .map_err(|error| match error {
            crate::control_host::LocalIntentError::Capacity => AgentError::Capacity,
            crate::control_host::LocalIntentError::Unavailable => AgentError::Stopped,
            crate::control_host::LocalIntentError::Persistence(error) => AgentError::Journal(error),
        })
    }
}

/// How this node reaches the partition owner.
#[allow(
    clippy::large_enum_variant,
    reason = "one short-lived access per tick; the host handle is cloned once"
)]
enum PartitionAccess {
    Local(ControlHost),
    Remote { target: u64, group: [u8; 16] },
}

/// What the service hands the agent at startup; every field is trusted
/// local composition, never peer input.
pub struct AgentInputs {
    pub state: NetworkState,
    pub identity: NodeIdentity,
    /// The node's enrolled principal: the identity every metadata owner binds
    /// this agent's requests to, locally and over the wire.
    pub principal: ParticipantId,
    pub settings: Settings,
    pub credentials: CredentialMaterial,
    pub root: PathBuf,
    /// The custody scope installed for the founder's own session, if any.
    pub active_custody: Option<CustodyScope>,
    /// The node's whole allowance, sampled for load reports.
    pub node_budget: MemoryBudget,
    /// The tenants this node hosts and the allowance installed copies of
    /// each are charged to.
    pub admission: TenantAdmission,
    pub wal: SharedWal,
    /// Signing and collection jobs, served with the node credential.
    pub jobs: mpsc::Receiver<AgentJob>,
}

pub struct PlacementAgent {
    state: NetworkState,
    identity: NodeIdentity,
    principal: ParticipantId,
    settings: Settings,
    credentials: CredentialMaterial,
    root: PathBuf,
    custody: BTreeMap<LedgerId, CustodyScope>,
    node_budget: MemoryBudget,
    admission: TenantAdmission,
    wal: SharedWal,
    budget: MemoryBudget,
    jobs: Option<mpsc::Receiver<AgentJob>>,
    journals: Option<Journals>,
    installs: Option<InstallJournal>,
    reopened: bool,
    nonce: u64,
    /// The partition the current step acts on; every partition intent and
    /// load report is keyed by it.
    current_partition: Option<PartitionId>,
    last_load: BTreeMap<PartitionId, (i64, u64)>,
    /// The last readiness attempt per plan, so a refused or pending attempt
    /// does not export a checkpoint on every tick.
    last_ready: Option<(LedgerId, OperationId, i64)>,
    /// The most recent failed tick, kept for diagnostics.
    last_error: Option<AgentError>,
    /// Completed ticks: the progress witness a stuck host cannot advance.
    ticks: u64,
    _allocation: Allocation,
}
impl PlacementAgent {
    pub fn new(inputs: AgentInputs, budget: MemoryBudget) -> Result<Self, AgentError> {
        let AgentInputs {
            state,
            identity,
            principal,
            settings,
            credentials,
            root,
            active_custody,
            node_budget,
            admission,
            wal,
            jobs,
        } = inputs;
        // Covers the bounded partition and authority projections, the
        // journals, one readiness proof and one registration plan at a time.
        let allocation = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                32 * 1024 * 1024,
            )
            .map_err(|_| AgentError::Capacity)?
            .commit();
        let mut custody = BTreeMap::new();
        if let Some(scope) = active_custody {
            custody.insert(scope.ledger, scope);
        }
        Ok(Self {
            state,
            identity,
            principal,
            settings,
            credentials,
            root,
            custody,
            node_budget,
            admission,
            wal,
            budget,
            jobs: Some(jobs),
            journals: None,
            installs: None,
            reopened: false,
            nonce: 0,
            current_partition: None,
            last_load: BTreeMap::new(),
            last_ready: None,
            last_error: None,
            ticks: 0,
            _allocation: allocation,
        })
    }
    /// The client identity of this node's agent on a local metadata owner.
    /// A remote owner binds submissions to the enrolled principal instead,
    /// and each journal records the identity its owner sees.
    pub fn local_client(cluster: [u8; 16], node: u64) -> [u8; 16] {
        let mut hasher = blake3::Hasher::new_derive_key("focal.placement.agent.v1");
        hasher.update(&cluster);
        hasher.update(&node.to_be_bytes());
        let mut value = [0; 16];
        for (target, source) in value.iter_mut().zip(hasher.finalize().as_bytes()) {
            *target = *source;
        }
        value
    }
    /// Drive ticks until the service stops, signing peers' session facts in
    /// between. Placement bookkeeping never ends the node: a refused or failed
    /// tick backs off and the next tick replans from a fresh observation,
    /// while journaled intents stay exact.
    pub async fn run(
        mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
    ) -> Result<(), AgentError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(AgentError::Runtime);
        }
        let mut jobs = self.jobs.take().ok_or(AgentError::Identity)?;
        std::panic::AssertUnwindSafe(async {
            let mut pause = TICK;
            loop {
                tokio::select! {
                    biased;
                    job = jobs.recv() => {
                        match job {
                            None => return Err(AgentError::Stopped),
                            Some(AgentJob::Sign(job)) => {
                                let SignJob { permit, reply } = *job;
                                let _ = reply.send(permit.sign(&self.credentials));
                            }
                            Some(AgentJob::Collect(job)) => {
                                let CollectJob { request, reply } = *job;
                                let result = self.collect(handles, pool, request).await;
                                let _ = reply.send(result);
                            }
                            Some(AgentJob::Status(reply)) => {
                                let usage = handles.fleet.tenant_usage().await.unwrap_or_default();
                                let _ = reply.send(self.status(usage));
                            }
                            Some(AgentJob::Credentials(material)) => {
                                self.credentials = *material;
                            }
                        }
                    }
                    () = tokio::time::sleep(pause) => {
                        pause = match self.tick(handles, pool).await {
                            Ok(_) => {
                                self.last_error = None;
                                TICK
                            }
                            Err(error) => {
                                let retry = error.retryable();
                                self.last_error = Some(error);
                                if retry { TICK } else { BACKOFF }
                            }
                        };
                    }
                }
            }
        })
        .catch_unwind()
        .await
        .unwrap_or(Err(AgentError::Runtime))
    }
    /// One bounded pass; public for the service tests. Every partition the
    /// root delegates is visited in namespace order: hosted partitions this
    /// node leads locally, the rest through the founder; a sealed partition
    /// only reshapes ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §13).
    pub async fn tick(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
    ) -> Result<AgentStep, AgentError> {
        let node = self.state.node;
        let client = Self::local_client(self.state.genesis.founder.cluster, node);
        if self.installs.is_none() {
            self.installs = Some(InstallJournal::open(&self.root, node)?);
        }
        if !self.reopened {
            self.reopen_installed(handles).await?;
            self.reopened = true;
        }
        if self.journals.is_none() {
            self.journals = Some(Journals {
                root: IntentJournal::open(
                    &self.root,
                    "placement-root",
                    self.state.genesis.root,
                    client,
                )?,
                partitions: BTreeMap::new(),
            });
        }
        let root_peer = self.peer(client, self.state.genesis.root_namespace)?;
        let namespace = handles.directory.namespace();
        let partition_peer = self.peer(client, namespace)?;
        {
            let journals = self.journals.as_mut().ok_or(AgentError::Identity)?;
            if journals.root.pending().is_some() {
                let control = &handles.control;
                journals
                    .root
                    .advance(control, |request| control.submit(root_peer, request))
                    .await?;
                return Ok(AgentStep::Advanced);
            }
        }
        let now = unix_time()?;
        let root = match handles.control.observe_root().await {
            Ok(observation) => observation,
            Err(ControlFailure::NotReady | ControlFailure::Capacity) => return Ok(AgentStep::Idle),
            Err(error) => return Err(error.into()),
        };
        let ControlBootstrap::Root {
            directory: root_directory,
            ..
        } = &root.snapshot().state
        else {
            return Err(AgentError::Identity);
        };
        let delegations: Vec<Delegation> = root_directory.delegations.values().copied().collect();
        // Every partition this node can act on, observed at one prefix each.
        let mut observed: Vec<Observed> = Vec::new();
        for delegation in &delegations {
            let access = match handles.directory.host_of(delegation.partition) {
                Some(host) => {
                    let progress = host.progress();
                    if progress.stopped {
                        return Err(AgentError::Stopped);
                    }
                    if progress.applied_index == 0 || progress.leader != node {
                        continue;
                    }
                    PartitionAccess::Local(host)
                }
                None => PartitionAccess::Remote {
                    target: self.state.genesis.founder.node,
                    group: delegation.log_group.0,
                },
            };
            self.open_partition_journal(handles, pool, delegation, &access, client)
                .await?;
            self.current_partition = Some(delegation.partition);
            let journals = self.journals.as_mut().ok_or(AgentError::Identity)?;
            let journal = journals
                .partitions
                .get_mut(&delegation.partition)
                .ok_or(AgentError::Identity)?;
            if journal.pending().is_some() {
                let submit = |request: ControlRequest| {
                    submit_partition(&access, pool, namespace, partition_peer.clone(), request)
                };
                journal.advance(&handles.control, submit).await?;
                return Ok(AgentStep::Advanced);
            }
            if let Some((snapshot, installed)) = self
                .observe_partition(&access, pool, namespace, partition_peer.clone())
                .await?
            {
                observed.try_reserve(1).map_err(|_| AgentError::Capacity)?;
                observed.push((*delegation, access, snapshot, installed));
            }
        }
        if observed.is_empty() {
            return Ok(AgentStep::Idle);
        }
        self.report_liveness_facts(handles, &observed);
        let root_leader = handles.control.progress().leader == node;
        for (delegation, access, snapshot, installed) in &observed {
            let ControlBootstrap::Partition { directory } = &snapshot.state else {
                return Err(AgentError::Identity);
            };
            self.current_partition = Some(delegation.partition);
            if let PartitionAccess::Local(host) = access
                && root_leader
                && let Some(step) = self
                    .reshape(
                        handles,
                        pool,
                        &root,
                        root_directory,
                        delegation,
                        host,
                        directory,
                        &observed,
                        snapshot,
                        installed,
                        now,
                    )
                    .await?
            {
                return Ok(step);
            }
            if directory.sealed.is_some() {
                continue;
            }
            if let Some(ledger) = &handles.ledger
                && delegation.namespace.contains(self.identity.ledger)
                && let Some(step) = self
                    .register_founder(
                        handles, access, pool, ledger, &root, snapshot, installed, now,
                    )
                    .await?
            {
                return Ok(step);
            }
            if let Some(step) = self
                .enroll_self(handles, access, pool, directory, snapshot, installed, now)
                .await?
            {
                return Ok(step);
            }
            if let Some(step) = self
                .report_load(handles, access, pool, directory, snapshot, installed, now)
                .await?
            {
                return Ok(step);
            }
            for descriptor in directory.sessions.values() {
                self.sync_custody(handles, descriptor).await?;
                if let Some(step) = self
                    .install_assignment(handles, access, pool, descriptor, snapshot, installed, now)
                    .await?
                {
                    return Ok(step);
                }
                if let Some(step) = self
                    .report_ready(handles, access, pool, descriptor, snapshot, installed, now)
                    .await?
                {
                    return Ok(step);
                }
            }
            if let PartitionAccess::Local(host) = access {
                let progress = host.progress();
                if progress.leader == progress.node
                    && let Some(step) = self
                        .commit_liveness(
                            handles,
                            directory,
                            snapshot,
                            installed,
                            now,
                            progress.applied_index,
                        )
                        .await?
                {
                    return Ok(step);
                }
                for descriptor in directory.sessions.values() {
                    if let Some(step) = self
                        .control(
                            handles, pool, descriptor, directory, snapshot, installed, &root, now,
                        )
                        .await?
                    {
                        return Ok(step);
                    }
                }
            }
        }
        Ok(AgentStep::Idle)
    }
    /// Open the exact-retry journal of one partition the first time its
    /// delegation is visited: the first partition keeps its historical name,
    /// every later one is named by its identifier.
    async fn open_partition_journal(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        delegation: &Delegation,
        access: &PartitionAccess,
        client: [u8; 16],
    ) -> Result<(), AgentError> {
        if self
            .journals
            .as_ref()
            .is_some_and(|journals| journals.partitions.contains_key(&delegation.partition))
        {
            return Ok(());
        }
        let (identity, partition_client) = match access {
            PartitionAccess::Local(host) => (host.progress().identity, client),
            PartitionAccess::Remote { .. } => (
                self.remote_identity(handles, pool, delegation.log_group.0)
                    .await?,
                self.principal.0,
            ),
        };
        let name = if delegation.partition == handles.directory.plan().partition() {
            "placement-partition".to_owned()
        } else {
            format!(
                "placement-partition-{:032x}",
                u128::from_be_bytes(delegation.partition.0)
            )
        };
        let journal = IntentJournal::open(&self.root, &name, identity, partition_client)?;
        let journals = self.journals.as_mut().ok_or(AgentError::Identity)?;
        if journals.partitions.len()
            >= crate::network_service::MAX_HOSTED_PARTITIONS.saturating_add(1)
        {
            return Err(AgentError::Capacity);
        }
        journals.partitions.insert(delegation.partition, journal);
        Ok(())
    }
    /// Tell the failure detector who its members are (every other eligible
    /// enrolled node of every observed partition, at its highest generation),
    /// how far this node has progressed, and whether admission is refusing
    /// capacity.
    fn report_liveness_facts(&mut self, handles: &NetworkHandles, observed: &[Observed]) {
        self.ticks = self.ticks.saturating_add(1);
        let node = self.state.node;
        let mut generation = 0;
        let mut members: BTreeMap<u64, u64> = BTreeMap::new();
        for (_, _, snapshot, _) in observed {
            let ControlBootstrap::Partition { directory } = &snapshot.state else {
                continue;
            };
            if let Some(record) = directory.nodes.get(&node) {
                generation = generation.max(record.enrollment.generation);
            }
            for (id, record) in &directory.nodes {
                if *id == node || !record.enrollment.eligible {
                    continue;
                }
                let known = members.entry(*id).or_insert(0);
                *known = (*known).max(record.enrollment.generation);
            }
        }
        let report = self
            .admission
            .report(&self.wal.disk_budget().stats(), &BTreeMap::new());
        let overloaded = report.memory_used.saturating_mul(20)
            >= report.memory_limit.saturating_mul(19)
            || report
                .disk_free
                .is_some_and(|free| free < report.disk_headroom);
        handles.liveness.report(crate::liveness::LocalFacts {
            generation,
            members,
            witness: self.ticks,
            overloaded,
        });
    }
    /// On the partition leader: commit the detector's settled verdicts that
    /// the directory does not hold yet, one per tick. A suspicion is not a
    /// verdict; an unconfirmed member has none; an older incarnation than
    /// the committed one is stale and never resubmitted.
    async fn commit_liveness(
        &mut self,
        handles: &NetworkHandles,
        directory: &PartitionCheckpoint,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
        applied_index: u64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let view = handles.liveness.view();
        let mut verdict = None;
        for (node, member) in &view.members {
            let Some(record) = directory.nodes.get(node) else {
                continue;
            };
            if record.enrollment.generation != member.generation || !member.confirmed {
                continue;
            }
            let alive = match member.status {
                crate::liveness::MemberStatus::Alive => true,
                crate::liveness::MemberStatus::Dead => false,
                crate::liveness::MemberStatus::Suspect => continue,
            };
            match record.liveness {
                // Alive is the default; the first committed verdict is a death.
                None if alive => continue,
                None => {}
                Some(current) => {
                    if member.incarnation < current.incarnation
                        || (member.incarnation == current.incarnation && current.alive == alive)
                        || now < current.decided_at
                    {
                        continue;
                    }
                }
            }
            verdict = Some(PartitionOperation::Liveness {
                node: *node,
                generation: member.generation,
                alive,
                incarnation: member.incarnation,
                witness: applied_index.max(1),
                decided_at: now,
            });
            break;
        }
        let Some(operation) = verdict else {
            return Ok(None);
        };
        let command = ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
            command: PartitionCommand {
                expected_revision: directory.revision,
                delegation_epoch: directory.delegation.epoch,
                operation,
            },
            evidence: control_evidence(
                installed,
                self.state.genesis.founder.cluster,
                now,
                &self.budget,
            )?,
        });
        if snapshot.revisions.partition != directory.revision {
            return Err(AgentError::Identity);
        }
        self.intend_partition(handles, command).await.map(Some)
    }
    fn peer(&self, client: [u8; 16], namespace: LedgerId) -> Result<AuthenticatedPeer, AgentError> {
        AuthenticatedPeer::local(PeerGrant {
            principal: ParticipantId(client),
            tenants: BTreeSet::from([namespace.tenant]),
            role: PeerRole::Runtime,
        })
        .map_err(AgentError::Custody)
    }
    fn next_request_id(&mut self) -> Result<RequestId, AgentError> {
        self.nonce = self.nonce.checked_add(1).ok_or(AgentError::Capacity)?;
        let mut id = [0; 16];
        id[..8].copy_from_slice(&self.state.node.to_be_bytes());
        id[8..].copy_from_slice(&self.nonce.to_be_bytes());
        Ok(RequestId(id))
    }
    async fn remote_read(
        &mut self,
        pool: &PeerConnectionPool,
        target: u64,
        group: [u8; 16],
        namespace: LedgerId,
        query: ControlRead,
    ) -> Result<ControlReadResult, AgentError> {
        let request = ControlRpc::Read(query)
            .encode(MAX_PEER_CONTROL_REQUEST_BYTES)
            .map_err(|_| AgentError::Capacity)?;
        let envelope = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: namespace,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: self.next_request_id()?,
            operation: Operation::PeerControl { group, request },
        };
        let bytes = tokio::time::timeout(REMOTE_TIMEOUT, pool.send_peer_control(target, &envelope))
            .await
            .map_err(|_| AgentError::Transport(PeerSendError::Lost))??;
        match ControlReply::decode(&bytes, ControlHost::wire_limits().max_frame_bytes as usize)
            .map_err(|_| AgentError::Identity)?
        {
            ControlReply::Read(value) => Ok(value),
            ControlReply::Rejected(failure) => Err(failure.into()),
            _ => Err(AgentError::Identity),
        }
    }
    async fn remote_identity(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        group: [u8; 16],
    ) -> Result<focal_control::ControlIdentity, AgentError> {
        let target = self.state.genesis.founder.node;
        match self
            .remote_read(
                pool,
                target,
                group,
                handles.directory.namespace(),
                ControlRead::State,
            )
            .await?
        {
            ControlReadResult::State(snapshot) if snapshot.identity.group == group => {
                Ok(snapshot.identity)
            }
            _ => Err(AgentError::Identity),
        }
    }
    async fn observe_partition(
        &mut self,
        access: &PartitionAccess,
        pool: &PeerConnectionPool,
        namespace: LedgerId,
        peer: AuthenticatedPeer,
    ) -> Result<Option<(ControlSnapshot, ControlAuthoritySnapshot)>, AgentError> {
        match access {
            PartitionAccess::Local(host) => {
                let id = self.next_request_id()?;
                match host.read(peer, id, ControlRead::StateAndAuthority).await? {
                    ControlReadResult::StateAndAuthority {
                        snapshot,
                        authority: Some(authority),
                    } => Ok(Some((*snapshot, authority))),
                    ControlReadResult::StateAndAuthority { .. } => Ok(None),
                    _ => Err(AgentError::Identity),
                }
            }
            PartitionAccess::Remote { target, group } => {
                let ControlReadResult::State(snapshot) = self
                    .remote_read(pool, *target, *group, namespace, ControlRead::State)
                    .await?
                else {
                    return Err(AgentError::Identity);
                };
                let ControlReadResult::Authority(authority) = self
                    .remote_read(pool, *target, *group, namespace, ControlRead::Authority)
                    .await?
                else {
                    return Err(AgentError::Identity);
                };
                // Two reads describe one prefix only when nothing was applied
                // in between; a command built across a gap fails its compare
                // and is replanned, so the gap is tolerated, not hidden.
                match authority {
                    Some(authority) if authority.applied_index <= snapshot.applied_index => {
                        Ok(Some((snapshot, authority)))
                    }
                    _ => Ok(None),
                }
            }
        }
    }
    async fn intend_partition(
        &mut self,
        handles: &NetworkHandles,
        command: ControlCommand,
    ) -> Result<AgentStep, AgentError> {
        let partition = self.current_partition.ok_or(AgentError::Identity)?;
        let journals = self.journals.as_mut().ok_or(AgentError::Identity)?;
        journals
            .partitions
            .get_mut(&partition)
            .ok_or(AgentError::Identity)?
            .intend(&handles.control, command)
            .await?;
        Ok(AgentStep::Advanced)
    }
    async fn reopen_installed(&mut self, handles: &NetworkHandles) -> Result<(), AgentError> {
        let installed = self
            .installs
            .as_ref()
            .ok_or(AgentError::Identity)?
            .record
            .installed
            .clone();
        for (ledger, copy) in installed {
            if handles.fleet.current_host(ledger).is_ok() {
                continue;
            }
            self.open_copy(handles, ledger, &copy).await?;
        }
        Ok(())
    }
    /// Open (or reopen) one copy on the shared WAL and hand it to the fleet.
    async fn open_copy(
        &mut self,
        handles: &NetworkHandles,
        ledger: LedgerId,
        copy: &InstalledCopy,
    ) -> Result<ReplicaHost, AgentError> {
        // A copy is charged to its tenant's allowance; the tenant was admitted
        // before the copy was recorded as installed.
        let tenant = self
            .admission
            .budget(ledger.tenant)
            .ok_or(AgentError::Identity)?
            .clone();
        let consensus = DurableNode::open_on_wal_in(
            NodeConfig::joining(
                self.state.node,
                self.state.genesis.founder.cluster,
                copy.group,
                copy.bootstrap_voters.clone(),
                Vec::new(),
            ),
            self.wal.clone(),
            &tenant,
        )?;
        let mut identity = self.identity.clone();
        identity.ledger = ledger;
        let hosting = crate::network_service::native_hosting(&self.root, &identity)?;
        let session = Session::from_node_in_hosted(
            ledger,
            consensus,
            SessionLimits::default(),
            &tenant,
            hosting,
        )?;
        // A reopened copy serves the route its log has committed; a fresh copy
        // has none yet and takes the plan's target scope for its custody.
        let mut config = ReplicaConfig::new(self.identity.root);
        let (scope, voters, copies) = match (session.active_fence(), session.active_placement()) {
            (Some(fence), Some(spec)) => {
                config.route_epoch = fence.to_route;
                config.policy_revision = fence.placement_epoch;
                (
                    CustodyScope {
                        ledger,
                        route_epoch: fence.to_route,
                        policy_revision: fence.placement_epoch,
                    },
                    spec.placement.voters.keys().copied().collect(),
                    spec.placement.content_copies.keys().copied().collect(),
                )
            }
            _ => (
                CustodyScope {
                    ledger,
                    route_epoch: copy.route_epoch,
                    policy_revision: copy.policy_revision,
                },
                copy.voters.clone(),
                copy.copies.clone(),
            ),
        };
        let sequence = handles
            .fleet
            .status()
            .latest_sequence
            .checked_add(1)
            .ok_or(AgentError::Capacity)?;
        let installed = handles
            .fleet
            .install(sequence, FleetReplica { session, config })
            .await
            .map_err(|failure| AgentError::Fleet(failure.error))?;
        self.install_custody(handles, scope, voters, copies).await?;
        Ok(installed.value().host().clone())
    }
    /// Install one custody scope for a hosted ledger, replacing the previous.
    async fn install_custody(
        &mut self,
        handles: &NetworkHandles,
        scope: CustodyScope,
        voters: BTreeSet<u64>,
        copies: BTreeSet<u64>,
    ) -> Result<(), AgentError> {
        let previous = self.custody.get(&scope.ledger).copied();
        if previous == Some(scope) {
            return Ok(());
        }
        handles
            .content
            .install_policy(CustodyPolicy {
                ledger: scope.ledger,
                route_epoch: scope.route_epoch,
                policy_revision: scope.policy_revision,
                peers: voters.union(&copies).copied().collect(),
            })
            .await?;
        handles
            .evidence
            .replace_placement(
                previous,
                EvidencePlacement::committed(scope, voters, copies)?,
            )
            .await?;
        self.custody.insert(scope.ledger, scope);
        Ok(())
    }
    /// Keep a hosted ledger's custody scope at the placement the directory
    /// has activated; a pending plan changes nothing until it activates.
    async fn sync_custody(
        &mut self,
        handles: &NetworkHandles,
        descriptor: &SessionDescriptor,
    ) -> Result<(), AgentError> {
        let node = self.state.node;
        if !handles.fleet.hosts(descriptor.ledger)
            || descriptor.active.placement.generation(node).is_none()
        {
            return Ok(());
        }
        let scope = CustodyScope {
            ledger: descriptor.ledger,
            route_epoch: descriptor.route_epoch,
            policy_revision: descriptor.placement_epoch,
        };
        if self
            .custody
            .get(&descriptor.ledger)
            .is_some_and(|current| current.route_epoch >= scope.route_epoch)
        {
            return Ok(());
        }
        let placement = &descriptor.active.placement;
        self.install_custody(
            handles,
            scope,
            placement.voters.keys().copied().collect(),
            placement.content_copies.keys().copied().collect(),
        )
        .await
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn register_founder(
        &mut self,
        handles: &NetworkHandles,
        access: &PartitionAccess,
        pool: &PeerConnectionPool,
        ledger: &ReplicaHost,
        root: &RootObservation,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let _ = (access, pool);
        let facts: HostedSessionFacts = ledger.registration_facts().await?.value().clone();
        let ControlBootstrap::Partition { directory } = &snapshot.state else {
            return Err(AgentError::Identity);
        };
        // A registered session may since have moved past its creation fence;
        // its registration is complete and later placements are the plan's.
        if directory.sessions.contains_key(&facts.ledger) || !facts.authoritative {
            return Ok(None);
        }
        let plan = FirstSessionPlan::capture_facts(
            &facts,
            &self.state.genesis,
            &self.settings,
            facts.memory_limit,
            &self.budget,
        )?;
        let initial = plan.prepare(root, now, &self.budget)?;
        if let Some(command) = initial.root_command() {
            let journals = self.journals.as_mut().ok_or(AgentError::Identity)?;
            journals
                .root
                .intend(&handles.control, command.clone())
                .await?;
            return Ok(Some(AgentStep::Advanced));
        }
        let witness = ledger
            .propose_placement(initial.placement_request().clone())
            .await?
            .into_witness();
        if let Some(enroll) =
            initial.partition_enrollment(snapshot, installed, now, &self.budget)?
        {
            return self
                .intend_partition(handles, enroll.command().clone())
                .await
                .map(Some);
        }
        let window = ProofWindow {
            issued_at: now,
            expires_at: now.checked_add(PROOF_WINDOW).ok_or(AgentError::Capacity)?,
        };
        // The proof consumes its witness; the exact committed record answers
        // the same request again for the directory command.
        let signed = handles
            .control
            .prepare_session_proof(witness, window)
            .await?
            .sign(&self.credentials)?;
        let witness = ledger
            .propose_placement(initial.placement_request().clone())
            .await?
            .into_witness();
        if let Some(create) =
            initial.create_session(snapshot, installed, &witness, signed, now, &self.budget)?
        {
            return self
                .intend_partition(handles, create.command().clone())
                .await
                .map(Some);
        }
        Ok(None)
    }
    /// Enroll this node in the partition from the root authority's grant.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn enroll_self(
        &mut self,
        handles: &NetworkHandles,
        access: &PartitionAccess,
        pool: &PeerConnectionPool,
        directory: &PartitionCheckpoint,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let _ = (access, pool);
        let node = self.state.node;
        let Some(grant) = installed.authority.nodes.get(&node) else {
            return Ok(None);
        };
        if !grant.enrollment.eligible || grant.expires_at <= now {
            return Ok(None);
        }
        let expected_generation = match directory.nodes.get(&node) {
            Some(existing) if existing.enrollment == grant.enrollment => return Ok(None),
            Some(existing)
                if existing.enrollment.generation < grant.enrollment.generation
                    && grant.enrollment.generation
                        == existing.enrollment.generation.saturating_add(1) =>
            {
                Some(existing.enrollment.generation)
            }
            Some(_) => return Ok(None),
            None if grant.enrollment.generation == 1 => None,
            None => return Ok(None),
        };
        let command = ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
            command: PartitionCommand {
                expected_revision: directory.revision,
                delegation_epoch: directory.delegation.epoch,
                operation: PartitionOperation::Enroll {
                    node: grant.enrollment.clone(),
                    expected_generation,
                },
            },
            evidence: control_evidence(
                installed,
                self.state.genesis.founder.cluster,
                now,
                &self.budget,
            )?,
        });
        if snapshot.revisions.partition != directory.revision {
            return Err(AgentError::Identity);
        }
        self.intend_partition(handles, command).await.map(Some)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn report_load(
        &mut self,
        handles: &NetworkHandles,
        access: &PartitionAccess,
        pool: &PeerConnectionPool,
        directory: &PartitionCheckpoint,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let _ = (access, pool);
        let node = self.state.node;
        let Some(record) = directory.nodes.get(&node) else {
            return Ok(None);
        };
        let generation = record.enrollment.generation;
        let partition = self.current_partition.ok_or(AgentError::Identity)?;
        let due = match (self.last_load.get(&partition).copied(), record.load) {
            (_, None) => true,
            (None, Some(_)) => true,
            (Some((at, reported)), Some(load)) => {
                reported != generation
                    || load.generation != generation
                    || now.saturating_sub(at) >= LOAD_INTERVAL
            }
        };
        if !due {
            return Ok(None);
        }
        let stats = self.node_budget.stats();
        let report = record
            .load
            .map_or(0, |load| load.report)
            .max(u64::try_from(now).unwrap_or(0))
            .checked_add(1)
            .ok_or(AgentError::Capacity)?;
        let load = NodeLoad {
            node,
            generation,
            report,
            available_memory: u64::try_from(stats.limit.saturating_sub(stats.used))
                .unwrap_or(u64::MAX),
            active_weight: u64::try_from(handles.fleet.status().installed).unwrap_or(u64::MAX),
            // Free bytes of the data volume that no queued durable write has
            // been promised, as the disk envelope estimates them.
            disk_available: self.wal.available_bytes().unwrap_or(0),
        };
        let command = ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
            command: PartitionCommand {
                expected_revision: directory.revision,
                delegation_epoch: directory.delegation.epoch,
                operation: PartitionOperation::ReportLoad { load },
            },
            evidence: control_evidence(
                installed,
                self.state.genesis.founder.cluster,
                now,
                &self.budget,
            )?,
        });
        if snapshot.revisions.partition != directory.revision {
            return Err(AgentError::Identity);
        }
        self.last_load.insert(partition, (now, generation));
        self.intend_partition(handles, command).await.map(Some)
    }
    /// Install the copy a plan assigns to this node and report `Installed`.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn install_assignment(
        &mut self,
        handles: &NetworkHandles,
        access: &PartitionAccess,
        pool: &PeerConnectionPool,
        descriptor: &SessionDescriptor,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let _ = (access, pool);
        let node = self.state.node;
        let Some(plan) = &descriptor.pending else {
            return Ok(None);
        };
        if plan.phase == PlacementPhase::Planned {
            return Ok(None);
        }
        let Some(progress) = plan.progress.get(&node) else {
            return Ok(None);
        };
        let ledger = descriptor.ledger;
        if progress.phase == AssignmentPhase::Installed {
            // Caught up means this copy applied everything its leader has
            // committed, as its own consensus state reports it.
            let Ok(host) = handles.fleet.current_host(ledger) else {
                return Ok(None);
            };
            let diagnostics = host.diagnostics().await?;
            let value = diagnostics.value();
            if value.leader == 0
                || value.committed_index == 0
                || value.applied_index < value.committed_index
            {
                return Ok(None);
            }
            let mut reported = AssignmentProgress::assigned(
                node,
                progress.node_generation,
                roles_of(&plan.desired.placement, node),
            );
            reported.attempt = progress.attempt;
            reported.phase = AssignmentPhase::CaughtUp;
            reported.through = focal_model::SessionSeq(host.progress().sequence.0);
            let command = self.session_command(
                descriptor,
                snapshot,
                installed,
                now,
                SessionChange::Progress {
                    operation: plan.operation,
                    progress: reported,
                },
                None,
            )?;
            return self.intend_partition(handles, command).await.map(Some);
        }
        if progress.phase != AssignmentPhase::Assigned {
            return Ok(None);
        }
        if handles.fleet.current_host(ledger).is_err() {
            // The tenant is admitted before its first copy: under the node's
            // allowance and tenant bound, or this node refuses the assignment
            // as a capacity refusal the planner can answer with another node.
            if !handles.fleet.is_admitted(ledger.tenant) {
                match self
                    .admission
                    .admit(ledger.tenant, plan.desired.policy.required_memory)
                {
                    Ok(tenant) => handles.fleet.admit_tenant(tenant).await?,
                    Err(refusal) => {
                        let command = self.session_command(
                            descriptor,
                            snapshot,
                            installed,
                            now,
                            SessionChange::Refuse {
                                operation: plan.operation,
                                refusal: focal_directory::Refusal {
                                    operation: plan.operation,
                                    code: focal_directory::RefusalCode::NodeCapacity,
                                    node: Some(node),
                                    attempt: progress.attempt,
                                    at: now,
                                },
                            },
                            None,
                        )?;
                        self.last_error = Some(AgentError::Admission(refusal));
                        return self.intend_partition(handles, command).await.map(Some);
                    }
                }
            }
            let copy = InstalledCopy {
                group: descriptor.log_group.0,
                // Every session of this deployment was founded by the founder
                // alone; the log replays the membership changes since.
                bootstrap_voters: vec![self.state.genesis.founder.node],
                route_epoch: plan.next_route,
                policy_revision: plan.next_placement,
                voters: plan.desired.placement.voters.keys().copied().collect(),
                copies: plan
                    .desired
                    .placement
                    .content_copies
                    .keys()
                    .copied()
                    .collect(),
            };
            let installs = self.installs.as_mut().ok_or(AgentError::Identity)?;
            if installs.record.installed.len() >= MAX_INSTALLED {
                return Err(AgentError::Capacity);
            }
            installs.record.installed.insert(ledger, copy.clone());
            installs.save_on(&handles.control).await?;
            self.open_copy(handles, ledger, &copy).await?;
            return Ok(Some(AgentStep::Advanced));
        }
        let mut reported = AssignmentProgress::assigned(
            node,
            progress.node_generation,
            roles_of(&plan.desired.placement, node),
        );
        reported.attempt = progress.attempt;
        reported.phase = AssignmentPhase::Installed;
        let command = self.session_command(
            descriptor,
            snapshot,
            installed,
            now,
            SessionChange::Progress {
                operation: plan.operation,
                progress: reported,
            },
            None,
        )?;
        self.intend_partition(handles, command).await.map(Some)
    }
    fn session_command(
        &self,
        descriptor: &SessionDescriptor,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
        change: SessionChange,
        proof: Option<focal_directory::AuthorityProof>,
    ) -> Result<ControlCommand, AgentError> {
        let mut evidence = control_evidence(
            installed,
            self.state.genesis.founder.cluster,
            now,
            &self.budget,
        )?;
        if let Some(proof) = proof {
            evidence
                .proofs
                .try_reserve_exact(1)
                .map_err(|_| AgentError::Capacity)?;
            evidence.proofs.push(proof);
        }
        let ControlBootstrap::Partition { directory } = &snapshot.state else {
            return Err(AgentError::Identity);
        };
        Ok(ControlCommand::VerifiedPartition(
            VerifiedPartitionCommand {
                command: PartitionCommand {
                    expected_revision: snapshot.revisions.partition,
                    delegation_epoch: directory.delegation.epoch,
                    operation: PartitionOperation::Session {
                        ledger: descriptor.ledger,
                        expected_revision: descriptor.revision,
                        change,
                    },
                },
                evidence,
            },
        ))
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn report_ready(
        &mut self,
        handles: &NetworkHandles,
        access: &PartitionAccess,
        pool: &PeerConnectionPool,
        descriptor: &SessionDescriptor,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let _ = (access, pool);
        let node = self.state.node;
        let Some(plan) = &descriptor.pending else {
            return Ok(None);
        };
        if plan.phase == PlacementPhase::Planned {
            return Ok(None);
        }
        let Some(progress) = plan.progress.get(&node) else {
            return Ok(None);
        };
        if progress.phase == AssignmentPhase::Failed {
            return Ok(None);
        }
        let Ok(host) = handles.fleet.current_host(descriptor.ledger) else {
            return Ok(None);
        };
        // Readiness is reported once per verified prefix: first when custody is
        // verified under the new route, again once the copy has applied the
        // committed barrier.
        let needed = match (plan.ready.get(&node), &plan.barrier) {
            (None, _) => true,
            (Some(ready), Some(barrier)) => ready.through < barrier.sequence,
            (Some(_), None) => false,
        };
        if !needed {
            return Ok(None);
        }
        // Custody under the old route is not readiness for the new one: wait
        // for the session log's own cutover record before exporting a
        // checkpoint, and never export more than once per interval per plan.
        let facts = host.registration_facts().await?.value().clone();
        if !facts.placement.as_ref().is_some_and(|fence| {
            fence.kind == focal_directory::SessionFenceKind::Cutover
                && fence.operation == plan.operation
                && fence.to_route == plan.next_route
                && fence.placement_epoch == plan.next_placement
        }) {
            return Ok(None);
        }
        if self.last_ready.is_some_and(|(ledger, operation, at)| {
            ledger == descriptor.ledger
                && operation == plan.operation
                && now.saturating_sub(at) < READY_INTERVAL
        }) {
            return Ok(None);
        }
        self.last_ready = Some((descriptor.ledger, plan.operation, now));
        let evidence = host.checkpoint_evidence(EVIDENCE_TTL).await?;
        let prefix = evidence.prefix().clone();
        if prefix.route != plan.next_route
            || prefix.placement_epoch != plan.next_placement
            || plan
                .barrier
                .as_ref()
                .is_some_and(|barrier| prefix.sequence < barrier.sequence)
        {
            return Ok(None);
        }
        let expected = self.custody.get(&descriptor.ledger).copied();
        let custody = handles.content.verify_prefix(evidence, expected).await?;
        let mut ready = ReplicaReady {
            ledger: descriptor.ledger,
            operation: plan.operation,
            route_epoch: plan.next_route,
            node,
            node_generation: progress.node_generation,
            through: custody.manifest().prefix.sequence,
            custody: custody.digest(),
            attestation: ContentHash([0; 32]),
        };
        let window = ProofWindow {
            issued_at: now,
            expires_at: now.checked_add(PROOF_WINDOW).ok_or(AgentError::Capacity)?,
        };
        let permit = handles
            .control
            .prepare_replica_ready(ready.clone(), descriptor.log_group, window)
            .await?;
        ready.attestation = permit.attestation()?;
        let proof = permit.sign(&self.credentials)?;
        let command = self.session_command(
            descriptor,
            snapshot,
            installed,
            now,
            SessionChange::Ready { ready },
            Some(proof.proof().clone()),
        )?;
        self.intend_partition(handles, command).await.map(Some)
    }
    /// Sign locally when this node is a voter hosting the ledger, then ask
    /// the other voters until a majority agrees on one statement.
    pub(super) async fn collect(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        request: CollectRequest,
    ) -> Result<focal_directory::AuthorityProof, CollectError> {
        let CollectRequest {
            ledger,
            group,
            voters,
            fact,
            window,
        } = request;
        let mut collected = Collected::new(voters.len())?;
        let node = self.state.node;
        if voters.contains(&node) && handles.fleet.hosts(ledger) {
            let permit = crate::placement_control::prepare_session_fact(
                &handles.fleet,
                &handles.control,
                ledger,
                fact.clone(),
                window,
            )
            .await?;
            let local = permit.sign(&self.credentials)?;
            if collected.merge(local.proof().clone())? {
                return collected.finish();
            }
        }
        for voter in voters.iter().copied().filter(|voter| *voter != node) {
            let id = self.next_request_id().map_err(|_| CollectError::Capacity)?;
            let Some(proof) = remote_signature(pool, voter, ledger, group, &fact, window, id).await
            else {
                continue;
            };
            if collected.merge(proof)? {
                return collected.finish();
            }
        }
        collected.finish()
    }
    pub fn status(&self, usage: BTreeMap<TenantId, focal_directory::QueueUsage>) -> AgentStatus {
        AgentStatus {
            admission: self
                .admission
                .report(&self.wal.disk_budget().stats(), &usage),
            node: self.state.node,
            root_intents: self.journals.as_ref().map_or(0, |j| j.root.completed()),
            partition_intents: self.journals.as_ref().map_or(0, |j| {
                j.partitions.values().fold(0_u64, |sum, journal| {
                    sum.saturating_add(journal.completed())
                })
            }),
            installed: self
                .installs
                .as_ref()
                .map(|installs| installs.record.installed.keys().copied().collect())
                .unwrap_or_default(),
            last_error: self.last_error.as_ref().map(|error| error.to_string()),
        }
    }
    pub fn node(&self) -> u64 {
        self.identity.node
    }
    pub fn last_error(&self) -> Option<&AgentError> {
        self.last_error.as_ref()
    }
    pub fn intents(&self) -> Option<(u64, u64)> {
        self.journals.as_ref().map(|journals| {
            (
                journals.root.completed(),
                journals.partitions.values().fold(0_u64, |sum, journal| {
                    sum.saturating_add(journal.completed())
                }),
            )
        })
    }
}

/// Submit one partition request locally or over the placement protocol.
async fn submit_partition(
    access: &PartitionAccess,
    pool: &PeerConnectionPool,
    namespace: LedgerId,
    peer: AuthenticatedPeer,
    request: ControlRequest,
) -> Result<ControlReceipt, ControlFailure> {
    match access {
        PartitionAccess::Local(host) => host.submit(peer, request).await,
        PartitionAccess::Remote { target, group } => {
            let id = RequestId::from_u128(u128::from(request.id.sequence));
            let body = ControlRpc::Submit(request)
                .encode(MAX_PLACEMENT_CONTROL_REQUEST_BYTES)
                .map_err(ControlFailure::from)?;
            let envelope = RequestEnvelope {
                protocol: PROTOCOL_VERSION,
                ledger: namespace,
                route_epoch: RouteEpoch(1),
                request_epoch: RequestEpoch(1),
                request_id: id,
                operation: Operation::PlacementControl {
                    group: *group,
                    request: body,
                },
            };
            let bytes =
                match tokio::time::timeout(REMOTE_TIMEOUT, pool.send_placement(*target, &envelope))
                    .await
                {
                    Ok(Ok(bytes)) => bytes,
                    Ok(Err(PeerSendError::Rejected(AccessError::Unauthorized))) => {
                        return Err(ControlFailure::Unauthorized);
                    }
                    Ok(Err(PeerSendError::Rejected(AccessError::Capacity))) => {
                        return Err(ControlFailure::Capacity);
                    }
                    Ok(Err(_)) | Err(_) => return Err(ControlFailure::OutcomeUnknown),
                };
            match ControlReply::decode(&bytes, ControlHost::wire_limits().max_frame_bytes as usize)
                .map_err(ControlFailure::from)?
            {
                ControlReply::Committed(receipt) => Ok(receipt),
                ControlReply::Rejected(failure) => Err(failure),
                _ => Err(ControlFailure::Invalid),
            }
        }
    }
}

impl crate::fleet::FleetManager {
    /// Whether this node currently hosts a replica of `ledger`.
    pub fn hosts(&self, ledger: LedgerId) -> bool {
        self.current_host(ledger).is_ok()
    }
}

#[path = "placement_controller.rs"]
mod controller;
#[path = "partition_split.rs"]
pub mod split;

#[cfg(test)]
#[path = "placement_agent_tests.rs"]
pub(crate) mod tests;
