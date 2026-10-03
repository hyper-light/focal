//! Partition hosting. The first partition has one root-permitted physical
//! owner on the founder; every partition a split creates on this node is a
//! further control group on the same WAL, bootstrapped from the source's
//! sealed image under its own root permit, recorded durably so a restart
//! reopens it, and reached through the same handle by partition or by group.
//! A member the root seated in a partition's group (the audit's F24) hosts
//! a replica of it the same way, from the group's genesis, and catches up
//! from the founder's log as a root learner does.
use super::*;
use crate::directory_bootstrap::{DirectoryBootstrapError, PartitionPlan};
use focal_directory::{Delegation, PartitionCheckpoint, PartitionId};
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::{collections::BTreeMap, path::PathBuf, pin::Pin};
use tokio::sync::watch;

/// Partitions one node hosts besides the first; the owner registry keeps a
/// slot for each.
pub const MAX_HOSTED_PARTITIONS: usize = 32;
/// Schema 1 recorded a split destination's image; 2 records who hosts and
/// an image where there is one.
const RECORD_SCHEMA: u16 = 3;
const RECORD_DIRECTORY: &str = "cluster/partitions";
/// Root permits are retried this often while the root is not ready.
const PERMIT_PAUSE: Duration = Duration::from_millis(250);
type HostedFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + 'a>>;

#[derive(Clone)]
pub struct HostedPartition {
    pub plan: PartitionPlan,
    pub host: ControlHost,
    /// What the root's authority refresh last did on this replica: the
    /// root index installed, and the refusal of the last attempt where it
    /// did not install (none while it follows and is told so, or installs).
    pub authority: AuthorityRefresh,
}
/// The state of a hosted replica's authority refresh (`refresh_authority`),
/// shown in node health so a group whose authority is stale says why.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthorityRefresh {
    pub installed_index: u64,
    pub refused: Option<String>,
}
/// What the placement agent asks of the host manager.
pub enum HostRequest {
    /// Host `plan` from the sealed `image` it was planned on, or from the
    /// group's genesis where there is none (a seated member's replica);
    /// durable before any group opens, idempotent while the partition is
    /// already hosted.
    Host {
        plan: Box<PartitionPlan>,
        image: Option<Box<PartitionCheckpoint>>,
    },
    /// Forget a partition that merged away: its record is removed so a restart
    /// does not reopen it; the sealed group keeps refusing until shutdown.
    Retire { partition: PartitionId },
}
/// The durable record of a hosted partition: the plan is re-derived from it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct HostedRecord {
    schema: u16,
    cluster: [u8; 16],
    founder_node: u64,
    delegation: Delegation,
    image: Option<PartitionCheckpoint>,
    /// The node that hosts: the founder for a split destination of its
    /// own, a seated member otherwise.
    host: u64,
    /// A seated member of a split destination (schema 3): the group's
    /// genesis and the image's digest, from the root — the member holds no
    /// image (24 §13).
    founded_elsewhere: Option<([u8; 32], focal_model::ContentHash)>,
}
/// What schema 2 recorded: a host, no founding elsewhere.
#[derive(serde::Deserialize)]
struct HostedRecordV2 {
    schema: u16,
    cluster: [u8; 16],
    founder_node: u64,
    delegation: Delegation,
    image: Option<PartitionCheckpoint>,
    host: u64,
}
/// What schema 1 recorded: a split destination on the founder.
#[derive(serde::Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
struct HostedRecordV1 {
    schema: u16,
    cluster: [u8; 16],
    founder_node: u64,
    delegation: Delegation,
    image: PartitionCheckpoint,
}

#[derive(Clone)]
pub struct DirectoryHandle {
    plan: PartitionPlan,
    state: watch::Receiver<Option<ControlHost>>,
    /// The first partition's authority refresh, where this node hosts it.
    first_authority: watch::Receiver<AuthorityRefresh>,
    hosted: watch::Receiver<BTreeMap<PartitionId, HostedPartition>>,
    pending: watch::Receiver<BTreeMap<PartitionId, HostingAttempt>>,
    requests: async_mpsc::Sender<HostRequest>,
}
/// A partition this node was asked to host and has not opened yet: how
/// often its permit was asked for and why it was last refused (F24).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostingAttempt {
    pub group: [u8; 16],
    pub host: u64,
    pub attempts: u32,
    pub last_refusal: Option<String>,
}
impl DirectoryHandle {
    /// The partitions whose hosting is under way, with their last refusal.
    pub fn pending(&self) -> Vec<(PartitionId, HostingAttempt)> {
        let pending = self.pending.borrow();
        let mut all = Vec::new();
        if all.try_reserve_exact(pending.len()).is_ok() {
            all.extend(pending.iter().map(|(id, attempt)| (*id, attempt.clone())));
        }
        all
    }
    pub fn namespace(&self) -> LedgerId {
        self.plan.namespace()
    }
    pub fn group(&self) -> [u8; 16] {
        self.plan.group().0
    }
    pub fn plan(&self) -> PartitionPlan {
        self.plan
    }
    /// Trusted local lookup of the first partition. Clone one physical handle
    /// before any await; never retain the watch borrow while a request or
    /// disk recovery is running.
    pub fn host(&self) -> Option<ControlHost> {
        self.state.borrow().clone()
    }
    /// The first partition is the founder's own slot where this node
    /// founded it, and a hosted replica like any other where a seated
    /// member opened it (F24).
    pub fn host_of(&self, partition: PartitionId) -> Option<ControlHost> {
        if partition == self.plan.partition()
            && let Some(host) = self.host()
        {
            return Some(host);
        }
        self.hosted
            .borrow()
            .get(&partition)
            .map(|hosted| hosted.host.clone())
    }
    pub fn host_of_group(&self, group: [u8; 16]) -> Option<ControlHost> {
        if group == self.group()
            && let Some(host) = self.host()
        {
            return Some(host);
        }
        self.hosted
            .borrow()
            .values()
            .find(|hosted| hosted.plan.group().0 == group)
            .map(|hosted| hosted.host.clone())
    }
    /// Every partition this node hosts and has opened, the first included.
    pub fn hosted(&self) -> Vec<HostedPartition> {
        let mut all = Vec::new();
        if let Some(host) = self.host()
            && all.try_reserve(1).is_ok()
        {
            all.push(HostedPartition {
                plan: self.plan,
                host,
                authority: self.first_authority.borrow().clone(),
            });
        }
        let extra = self.hosted.borrow();
        if all.try_reserve(extra.len()).is_ok() {
            all.extend(extra.values().cloned());
        }
        all
    }
    pub fn is_hosted(&self, partition: PartitionId) -> bool {
        (partition == self.plan.partition() && self.host().is_some())
            || self.hosted.borrow().contains_key(&partition)
    }
    pub fn request(&self, request: HostRequest) -> Result<(), DirectoryBootstrapError> {
        self.requests
            .try_send(request)
            .map_err(|error| match error {
                async_mpsc::error::TrySendError::Full(_) => DirectoryBootstrapError::Capacity,
                async_mpsc::error::TrySendError::Closed(_) => DirectoryBootstrapError::Unavailable,
            })
    }
}

pub(super) struct DirectoryStartup {
    plan: PartitionPlan,
    /// Whether this node founded the first partition: it hosts it from
    /// the start; a member hosts what the root seats it in, when asked.
    founder: bool,
    node: u64,
    state: watch::Sender<Option<ControlHost>>,
    first_authority: watch::Sender<AuthorityRefresh>,
    hosted: watch::Sender<BTreeMap<PartitionId, HostedPartition>>,
    pending: watch::Sender<BTreeMap<PartitionId, HostingAttempt>>,
    requests: async_mpsc::Receiver<HostRequest>,
    wal: SharedWal,
    budget: MemoryBudget,
    root: PathBuf,
}
impl DirectoryStartup {
    pub(super) fn new(
        node: u64,
        cluster: [u8; 16],
        founder: u64,
        wal: SharedWal,
        budget: MemoryBudget,
        root: PathBuf,
    ) -> Result<(DirectoryHandle, Self), DirectoryBootstrapError> {
        let plan = PartitionPlan::derive(cluster, founder)?;
        let (state, receiver) = watch::channel(None);
        let (first_authority, first_authority_receiver) =
            watch::channel(AuthorityRefresh::default());
        let (hosted, hosted_receiver) = watch::channel(BTreeMap::new());
        let (pending, pending_receiver) = watch::channel(BTreeMap::new());
        let (requests, request_receiver) = async_mpsc::channel(8);
        let handle = DirectoryHandle {
            plan,
            state: receiver,
            first_authority: first_authority_receiver,
            hosted: hosted_receiver,
            pending: pending_receiver,
            requests,
        };
        let startup = Self {
            plan,
            founder: node == founder,
            node,
            state,
            first_authority,
            hosted,
            pending,
            requests: request_receiver,
            wal,
            budget,
            root,
        };
        Ok((handle, startup))
    }

    pub(super) async fn run(
        mut self,
        root: &ControlHost,
        pool: &PeerConnectionPool,
        owners: &OwnerGate,
    ) -> Result<(), ServiceError> {
        // The founder hosts the first partition from the start; a member
        // hosts nothing until the root seats it in a group, and keeps
        // hosting what it recorded across a restart.
        let first: HostedFuture<'_> = if self.founder {
            let permit = permit(root, self.plan, None, Some(&self.pending)).await?;
            self.pending.send_modify(|map| {
                map.remove(&self.plan.partition());
            });
            // Spawning, registering, and publishing are synchronous in this poll.
            // Cancellation cannot strand an untracked disk owner between awaits.
            let installed_index = permit.root_index();
            let expires_at = permit.expires_at();
            let (host, owner, output) =
                ControlHost::spawn_directory(permit, self.wal.clone(), self.budget.clone(), None)?;
            owners.register(PhysicalOwner::Control(owner))?;
            self.state.send_replace(Some(host.clone()));
            let plan = self.plan;
            let first_authority = &self.first_authority;
            Box::pin(async move {
                let replication = crate::replication::drive_directory_replication(
                    output,
                    pool,
                    pool.limits().max_inflight,
                );
                let refresh =
                    refresh_authority(plan, root, &host, installed_index, expires_at, |now| {
                        first_authority.send_replace(now);
                    });
                tokio::pin!(replication, refresh);
                tokio::select! {
                    result = &mut replication => result.map_err(ServiceError::from).and(Err(ServiceError::Owner("directory egress ended"))),
                    result = &mut refresh => result,
                }
            })
        } else {
            Box::pin(std::future::pending())
        };
        tokio::pin!(first);
        let mut extras: FuturesUnordered<HostedFuture<'_>> = FuturesUnordered::new();
        let records = load_records(&self.root, self.plan, self.node)?;
        let hosted = &self.hosted;
        let pending = &self.pending;
        let (wal, budget, plan_root) = (&self.wal, &self.budget, &self.root);
        // Every partition whose drive began, hosted or still being
        // permitted: a request repeated meanwhile starts no second one.
        let mut driving = std::collections::BTreeSet::new();
        for (plan, image) in records {
            driving.insert(plan.partition());
            extras.push(Box::pin(drive_hosted(
                plan, image, root, pool, owners, wal, budget, hosted, pending, plan_root,
            )));
        }
        loop {
            tokio::select! {
                result = &mut first => return result,
                Some(result) = extras.next(), if !extras.is_empty() => {
                    // A hosted drive that ended well — its seat gone, its
                    // record forgotten — ends nothing else; one that failed
                    // ends the service, as the first partition's would.
                    result?;
                }
                request = self.requests.recv() => {
                    let Some(request) = request else {
                        return Err(ServiceError::Owner("partition host requests ended"));
                    };
                    match request {
                        HostRequest::Host { plan, image } => {
                            if plan.host() != self.node
                                || driving.contains(&plan.partition())
                                || hosted.borrow().contains_key(&plan.partition())
                                || (self.founder && plan.partition() == self.plan.partition())
                            {
                                continue;
                            }
                            if driving.len() >= MAX_HOSTED_PARTITIONS {
                                continue;
                            }
                            let image = image.map(|image| *image);
                            record(plan_root, *plan, image.as_ref())?;
                            driving.insert(plan.partition());
                            extras.push(Box::pin(drive_hosted(
                                *plan, image, root, pool, owners, wal, budget, hosted, pending,
                                plan_root,
                            )));
                        }
                        HostRequest::Retire { partition } => {
                            let path = record_path(plan_root, partition);
                            match std::fs::remove_file(&path) {
                                Ok(()) => {}
                                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                                Err(error) => return Err(error.into()),
                            }
                            hosted.send_modify(|map| {
                                map.remove(&partition);
                            });
                        }
                    }
                }
            }
        }
    }
}

/// Wait for a root permit; transient refusals retry, anything else ends the
/// service. A bounded number of rounds keeps a hosted partition from
/// waiting forever behind a root that will never authorize it.
async fn permit(
    root: &ControlHost,
    plan: PartitionPlan,
    rounds: Option<u32>,
    pending: Option<&watch::Sender<BTreeMap<PartitionId, HostingAttempt>>>,
) -> Result<crate::directory_bootstrap::PartitionBootstrapPermit, ServiceError> {
    let mut remaining = rounds;
    loop {
        if root.progress().stopped {
            return Err(ServiceError::Owner("root owner ended"));
        }
        match root.prepare_directory(plan).await {
            Ok(permit) => return Ok(permit),
            Err(
                error @ (DirectoryBootstrapError::Unauthorized
                | DirectoryBootstrapError::Unavailable
                | DirectoryBootstrapError::NotReady
                | DirectoryBootstrapError::Capacity),
            ) => {
                // Why the last round was refused, for the node's health: a
                // replica that never opens says so instead of nothing.
                if let Some(pending) = pending {
                    pending.send_modify(|map| {
                        let entry = map.entry(plan.partition()).or_insert(HostingAttempt {
                            group: plan.group().0,
                            host: plan.host(),
                            attempts: 0,
                            last_refusal: None,
                        });
                        entry.attempts = entry.attempts.saturating_add(1);
                        entry.last_refusal = Some(error.to_string());
                    });
                }
                if let Some(left) = &mut remaining {
                    if *left == 0 {
                        return Err(DirectoryBootstrapError::Unavailable.into());
                    }
                    *left = left.saturating_sub(1);
                }
                tokio::time::sleep(PERMIT_PAUSE).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}
#[allow(
    clippy::too_many_arguments,
    reason = "one hosted partition's whole life, borrowing the service's shared owners"
)]
async fn drive_hosted(
    plan: PartitionPlan,
    image: Option<PartitionCheckpoint>,
    root: &ControlHost,
    pool: &PeerConnectionPool,
    owners: &OwnerGate,
    wal: &SharedWal,
    budget: &MemoryBudget,
    hosted: &watch::Sender<BTreeMap<PartitionId, HostedPartition>>,
    pending: &watch::Sender<BTreeMap<PartitionId, HostingAttempt>>,
    records: &Path,
) -> Result<(), ServiceError> {
    let permit = permit(root, plan, Some(2_400), Some(pending)).await?;
    let installed_index = permit.root_index();
    let expires_at = permit.expires_at();
    let (host, owner, output) =
        ControlHost::spawn_directory(permit, wal.clone(), budget.clone(), image)?;
    owners.register(PhysicalOwner::Control(owner))?;
    pending.send_modify(|map| {
        map.remove(&plan.partition());
    });
    hosted.send_modify(|map| {
        map.insert(
            plan.partition(),
            HostedPartition {
                plan,
                host: host.clone(),
                authority: AuthorityRefresh {
                    installed_index,
                    refused: None,
                },
            },
        );
    });
    let replication =
        crate::replication::drive_directory_replication(output, pool, pool.limits().max_inflight);
    tokio::pin!(replication);
    // Whichever replica leads the group refreshes the root's authority in
    // it; one that follows is told so and waits.
    let refresh = refresh_authority(plan, root, &host, installed_index, expires_at, |now| {
        hosted.send_modify(|map| {
            if let Some(record) = map.get_mut(&plan.partition()) {
                record.authority = now;
            }
        });
    });
    tokio::pin!(refresh);
    if plan.host() != plan.founder_node() {
        // A member's replica runs while the root's grant seats this node in
        // the group, and stops when the seat is gone (removed by the
        // operator, the group re-founded or released). The seat's own stop
        // ends the egress and the refresh too, and either may be seen
        // first: once the seat says it is ending, theirs is its end, and
        // the seat's word is waited for.
        let ending = std::sync::atomic::AtomicBool::new(false);
        let seat = watch_seat(plan, root, &host, &ending);
        tokio::pin!(seat);
        let result = tokio::select! {
            biased;
            result = &mut seat => result,
            result = &mut replication => {
                if ending.load(std::sync::atomic::Ordering::Acquire) {
                    seat.await
                } else {
                    result.map_err(ServiceError::from).and(Err(egress_ended(&host)))
                }
            }
            result = &mut refresh => {
                if ending.load(std::sync::atomic::Ordering::Acquire) {
                    seat.await
                } else {
                    result
                }
            }
        };
        hosted.send_modify(|map| {
            map.remove(&plan.partition());
        });
        // A seat that ended is forgotten for restarts, as the founder's own
        // retired hosting is: a record kept would reopen a replica of a
        // group the root no longer seats this node in, and wait on a permit
        // it is never given.
        if result.is_ok() {
            match std::fs::remove_file(record_path(records, plan.partition())) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        return result;
    }
    tokio::select! {
        result = &mut replication => result.map_err(ServiceError::from).and(Err(egress_ended(&host))),
        result = &mut refresh => result,
    }
}
/// Why a hosted partition's egress ended while its seat held: its owner
/// ended, and says why where it failed — the egress hands the owner's
/// frames on and ends with the owner, so the owner's failure is the cause
/// and the egress's end its symptom (a member brought up by snapshot on a
/// starved runner reported only the symptom, macOS CI, 2026-10-02).
fn egress_ended(host: &ControlHost) -> ServiceError {
    match host.progress().failure {
        Some(failure) => ServiceError::ControlOwner(failure),
        None => ServiceError::Owner("hosted partition egress ended"),
    }
}
/// Run a member's replica while the root's committed grant seats its host
/// in the group, checked from the node's own root replica as the root
/// moves, and stop it once the seat is gone — the node removed from the
/// group, the group re-founded, or the group released once the partition
/// it served was merged away and absorbed (24 §13; the grant outlives the
/// delegation until then, since the absorb needs the group's majority):
/// the replica is then no longer authorized and its record is dropped so
/// a restart does not reopen it. Ends `Ok` when the host stops.
async fn watch_seat(
    plan: PartitionPlan,
    root: &ControlHost,
    host: &ControlHost,
    ending: &std::sync::atomic::AtomicBool,
) -> Result<(), ServiceError> {
    let mut seen = 0u64;
    loop {
        if root.progress().stopped || host.progress().stopped {
            return Err(ServiceError::Owner("directory member owner ended"));
        }
        let applied = root.progress().applied_index;
        if applied > seen {
            seen = applied;
            let seated = match root.observe_root().await {
                // An authority not yet observable decides nothing; a group
                // the authority no longer holds, or holds without this
                // node, ends the seat.
                Ok(observation) => observation.authority().is_none_or(|authority| {
                    authority.groups.get(&plan.group()).is_some_and(|grant| {
                        grant.voters.contains_key(&plan.host())
                            || grant.learners.contains_key(&plan.host())
                    })
                }),
                // Not observable now: nothing is decided on it.
                Err(
                    focal_control::ControlFailure::NotReady
                    | focal_control::ControlFailure::Capacity,
                ) => true,
                Err(error) => return Err(error.into()),
            };
            if !seated {
                ending.store(true, std::sync::atomic::Ordering::Release);
                host.stop().await?;
                return Ok(());
            }
        }
        tokio::time::sleep(PERMIT_PAUSE).await;
    }
}
fn record_path(root: &Path, partition: PartitionId) -> PathBuf {
    root.join(RECORD_DIRECTORY).join(format!(
        "{:032x}.partition",
        u128::from_be_bytes(partition.0)
    ))
}
fn record(
    root: &Path,
    plan: PartitionPlan,
    image: Option<&PartitionCheckpoint>,
) -> Result<(), ServiceError> {
    let directory = root.join(RECORD_DIRECTORY);
    std::fs::create_dir_all(&directory)?;
    let bytes = postcard::to_stdvec(&HostedRecord {
        schema: RECORD_SCHEMA,
        cluster: plan.cluster(),
        founder_node: plan.founder_node(),
        delegation: plan.delegation(),
        image: image.cloned(),
        host: plan.host(),
        founded_elsewhere: plan
            .founded_elsewhere()
            .then(|| plan.image().map(|digest| (plan.genesis(), digest)))
            .flatten(),
    })
    .map_err(|_| ServiceError::Owner("hosted partition record"))?;
    crate::embedded::atomic_file(&record_path(root, plan.partition()), &bytes)?;
    Ok(())
}
/// Every recorded partition, re-planned from its record; a record that no
/// longer plans (a foreign cluster, a corrupt image) ends startup rather than
/// silently dropping a partition this node committed to host.
fn load_records(
    root: &Path,
    first: PartitionPlan,
    node: u64,
) -> Result<Vec<(PartitionPlan, Option<PartitionCheckpoint>)>, ServiceError> {
    let directory = root.join(RECORD_DIRECTORY);
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut records = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry
            .path()
            .extension()
            .is_none_or(|extension| extension != "partition")
        {
            continue;
        }
        if records.len() >= MAX_HOSTED_PARTITIONS {
            return Err(ServiceError::Owner("too many hosted partition records"));
        }
        let bytes = std::fs::read(entry.path())?;
        // Schema 1 named no host: the founder's own split destinations;
        // schema 2 named the host and no founding elsewhere.
        let record: HostedRecord = match postcard::from_bytes::<HostedRecord>(&bytes) {
            Ok(record) if record.schema == RECORD_SCHEMA => record,
            _ => match postcard::from_bytes::<HostedRecordV2>(&bytes) {
                Ok(old) if old.schema == 2 => HostedRecord {
                    schema: RECORD_SCHEMA,
                    cluster: old.cluster,
                    founder_node: old.founder_node,
                    delegation: old.delegation,
                    image: old.image,
                    host: old.host,
                    founded_elsewhere: None,
                },
                _ => {
                    let old: HostedRecordV1 = postcard::from_bytes(&bytes)
                        .map_err(|_| ServiceError::Owner("hosted partition record is corrupt"))?;
                    if old.schema != 1 {
                        return Err(ServiceError::Owner("hosted partition record is corrupt"));
                    }
                    HostedRecord {
                        schema: RECORD_SCHEMA,
                        cluster: old.cluster,
                        founder_node: old.founder_node,
                        delegation: old.delegation,
                        image: Some(old.image),
                        host: old.founder_node,
                        founded_elsewhere: None,
                    }
                }
            },
        };
        if record.cluster != first.cluster()
            || record.founder_node != first.founder_node()
            || record.host != node
        {
            return Err(ServiceError::Owner("hosted partition record is foreign"));
        }
        let plan = match (&record.image, record.founded_elsewhere) {
            (Some(image), _) => PartitionPlan::split_destination(
                record.cluster,
                record.founder_node,
                record.delegation,
                image,
            )?,
            // A seated member of a split destination: the group's identity
            // from the root, no image (24 §13).
            (None, Some((genesis, digest))) => PartitionPlan::split_member(
                record.cluster,
                record.founder_node,
                record.delegation,
                genesis,
                digest,
                record.host,
            )?,
            // A seated member's replica of the first partition: from the
            // group's genesis, as the founder's own.
            (None, None) => {
                if record.delegation.partition != first.partition()
                    || record.delegation.log_group != first.group()
                {
                    return Err(ServiceError::Owner("hosted partition record is foreign"));
                }
                first
            }
        }
        .hosted_by(record.host);
        records
            .try_reserve(1)
            .map_err(|_| ServiceError::Owner("hosted partition records"))?;
        records.push((plan, record.image));
    }
    Ok(records)
}

async fn refresh_authority(
    plan: PartitionPlan,
    root: &ControlHost,
    directory: &ControlHost,
    mut installed_index: u64,
    mut expires_at: i64,
    tell: impl Fn(AuthorityRefresh),
) -> Result<(), ServiceError> {
    // What the last attempt did, told to node health when it changes: a
    // replica whose group's authority is stale says why (a permit refused,
    // the install refused, or no attempt yet).
    let mut told = AuthorityRefresh {
        installed_index,
        refused: None,
    };
    loop {
        if root.progress().stopped || directory.progress().stopped {
            return Err(ServiceError::Owner("directory authority owner ended"));
        }
        if root.progress().applied_index > installed_index || unix_time()? >= expires_at {
            // A new permit comes from a fresh root quorum. Existing valid data
            // routes remain usable while metadata changes await that quorum.
            let outcome = match root.prepare_directory(plan).await {
                Ok(permit) => {
                    let valid_until = permit.expires_at();
                    directory
                        .refresh_directory(permit)
                        .await
                        .map(|receipt| (receipt, valid_until))
                        .map_err(|error| (error, "install"))
                }
                Err(error) => Err((error, "permit")),
            };
            let now = match outcome {
                Ok((receipt, valid_until)) => {
                    if receipt.root != root.progress().identity
                        || receipt.root_index < installed_index
                    {
                        return Err(DirectoryBootstrapError::Inconsistent.into());
                    }
                    installed_index = receipt.root_index;
                    expires_at = valid_until;
                    AuthorityRefresh {
                        installed_index,
                        refused: None,
                    }
                }
                Err((
                    error @ (DirectoryBootstrapError::Unauthorized
                    | DirectoryBootstrapError::Unavailable
                    | DirectoryBootstrapError::NotReady
                    | DirectoryBootstrapError::Capacity),
                    step,
                )) => AuthorityRefresh {
                    installed_index,
                    refused: Some(format!("{step}: {error}")),
                },
                Err((error, _)) => return Err(error.into()),
            };
            if now != told {
                tell(now.clone());
                told = now;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
#[allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use focal_directory::{
        ClusterId, DelegationFence, NamespaceKey, NamespaceRange, OperationId, PartitionId,
        PartitionSeal, RegionId,
    };

    /// A record written before hosts were recorded (schema 1) is the
    /// founder's own split destination; one written since names its host,
    /// and a node loads only the records that name it.
    #[test]
    fn hosted_records_of_both_schemas_load_and_name_their_host() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = [3; 16];
        let first = PartitionPlan::derive(cluster, 7).unwrap();
        let moved = NamespaceRange {
            start: NamespaceKey([8; 32]),
            end: None,
        };
        let delegation = Delegation {
            namespace: moved,
            partition: PartitionId([9; 16]),
            region: RegionId::UNKNOWN,
            log_group: focal_directory::LogGroupId([10; 16]),
            epoch: 2,
            activation: Some(DelegationFence {
                cluster: ClusterId(cluster),
                operation: OperationId([11; 16]),
                source: first.partition(),
                destination: PartitionId([9; 16]),
                namespace: moved,
                from_epoch: 1,
                to_epoch: 2,
                sealed_revision: 4,
                checkpoint: focal_model::ContentHash([12; 32]),
                destination_ready: focal_model::ContentHash([13; 32]),
            }),
        };
        let image = PartitionCheckpoint {
            schema: focal_directory::PARTITION_CHECKPOINT_SCHEMA,
            cluster: ClusterId(cluster),
            delegation,
            revision: 4,
            sealed: Some(PartitionSeal {
                operation: OperationId([11; 16]),
                destination: PartitionId([9; 16]),
                next_epoch: 2,
                revision: 4,
                moved,
                source: first.partition(),
            }),
            nodes: std::sync::Arc::new(BTreeMap::new()),
            sessions: std::sync::Arc::new(BTreeMap::new()),
            routes: std::sync::Arc::new(std::collections::VecDeque::new()),
            routes_from: 0,
        };
        let records = dir.path().join(RECORD_DIRECTORY);
        std::fs::create_dir_all(&records).unwrap();
        let old = postcard::to_stdvec(&HostedRecordV1 {
            schema: 1,
            cluster,
            founder_node: 7,
            delegation,
            image: image.clone(),
        })
        .unwrap();
        std::fs::write(records.join("old.partition"), old).unwrap();
        // The founder loads its old record as its own split destination.
        let loaded = load_records(dir.path(), first, 7).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0.host(), 7);
        assert_eq!(loaded[0].0.partition(), PartitionId([9; 16]));
        assert_eq!(loaded[0].1.as_ref(), Some(&image));
        // Another node is not named by it.
        assert!(load_records(dir.path(), first, 8).is_err());
        // A member's replica of the first partition is recorded without an
        // image and loaded as the first plan hosted by it.
        let member = tempfile::tempdir().unwrap();
        record(member.path(), first.hosted_by(8), None).unwrap();
        let loaded = load_records(member.path(), first, 8).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0.host(), 8);
        assert_eq!(loaded[0].0.partition(), first.partition());
        assert_eq!(loaded[0].0.identity().unwrap(), first.identity().unwrap());
        assert!(loaded[0].1.is_none());
        assert!(load_records(member.path(), first, 7).is_err());
    }
}
