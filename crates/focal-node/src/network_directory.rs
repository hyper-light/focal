//! Partition hosting. The first partition has one root-permitted physical
//! owner on the founder; every partition a split creates on this node is a
//! further control group on the same WAL, bootstrapped from the source's
//! sealed image under its own root permit, recorded durably so a restart
//! reopens it, and reached through the same handle by partition or by group.
use super::*;
use crate::directory_bootstrap::{DirectoryBootstrapError, PartitionPlan};
use focal_directory::{Delegation, PartitionCheckpoint, PartitionId};
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::{collections::BTreeMap, path::PathBuf, pin::Pin};
use tokio::sync::watch;

/// Partitions one node hosts besides the first; the owner registry keeps a
/// slot for each.
pub const MAX_HOSTED_PARTITIONS: usize = 32;
const RECORD_SCHEMA: u16 = 1;
const RECORD_DIRECTORY: &str = "cluster/partitions";
/// Root permits are retried this often while the root is not ready.
const PERMIT_PAUSE: Duration = Duration::from_millis(250);
type HostedFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ServiceError>> + Send + 'a>>;

#[derive(Clone)]
pub struct HostedPartition {
    pub plan: PartitionPlan,
    pub host: ControlHost,
}
/// What the placement agent asks of the host manager.
pub enum HostRequest {
    /// Host `plan` from the sealed `image` it was planned on; durable before
    /// any group opens, idempotent while the partition is already hosted.
    Host {
        plan: Box<PartitionPlan>,
        image: Box<PartitionCheckpoint>,
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
    image: PartitionCheckpoint,
}

#[derive(Clone)]
pub struct DirectoryHandle {
    plan: PartitionPlan,
    state: watch::Receiver<Option<ControlHost>>,
    hosted: watch::Receiver<BTreeMap<PartitionId, HostedPartition>>,
    requests: async_mpsc::Sender<HostRequest>,
}
impl DirectoryHandle {
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
    pub fn host_of(&self, partition: PartitionId) -> Option<ControlHost> {
        if partition == self.plan.partition() {
            return self.host();
        }
        self.hosted
            .borrow()
            .get(&partition)
            .map(|hosted| hosted.host.clone())
    }
    pub fn host_of_group(&self, group: [u8; 16]) -> Option<ControlHost> {
        if group == self.group() {
            return self.host();
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
            });
        }
        let extra = self.hosted.borrow();
        if all.try_reserve(extra.len()).is_ok() {
            all.extend(extra.values().cloned());
        }
        all
    }
    pub fn is_hosted(&self, partition: PartitionId) -> bool {
        partition == self.plan.partition() || self.hosted.borrow().contains_key(&partition)
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
    state: watch::Sender<Option<ControlHost>>,
    hosted: watch::Sender<BTreeMap<PartitionId, HostedPartition>>,
    requests: async_mpsc::Receiver<HostRequest>,
    wal: SharedWal,
    budget: MemoryBudget,
    root: PathBuf,
}
impl DirectoryStartup {
    pub(super) fn new(
        assigned: bool,
        cluster: [u8; 16],
        founder: u64,
        wal: SharedWal,
        budget: MemoryBudget,
        root: PathBuf,
    ) -> Result<(DirectoryHandle, Option<Self>), DirectoryBootstrapError> {
        let plan = PartitionPlan::derive(cluster, founder)?;
        let (state, receiver) = watch::channel(None);
        let (hosted, hosted_receiver) = watch::channel(BTreeMap::new());
        let (requests, request_receiver) = async_mpsc::channel(8);
        let handle = DirectoryHandle {
            plan,
            state: receiver,
            hosted: hosted_receiver,
            requests,
        };
        let startup = assigned.then_some(Self {
            plan,
            state,
            hosted,
            requests: request_receiver,
            wal,
            budget,
            root,
        });
        Ok((handle, startup))
    }

    pub(super) async fn run(
        mut self,
        root: &ControlHost,
        pool: &PeerConnectionPool,
        owners: &OwnerGate,
    ) -> Result<(), ServiceError> {
        let permit = permit(root, self.plan, None).await?;
        // Spawning, registering, and publishing are synchronous in this poll.
        // Cancellation cannot strand an untracked disk owner between awaits.
        let installed_index = permit.root_index();
        let expires_at = permit.expires_at();
        let (host, owner, output) =
            ControlHost::spawn_directory(permit, self.wal.clone(), self.budget.clone(), None)?;
        owners.register(PhysicalOwner::Control(owner))?;
        self.state.send_replace(Some(host.clone()));
        let replication = crate::replication::drive_directory_replication(output, pool, 16);
        let refresh = refresh_authority(self.plan, root, &host, installed_index, expires_at);
        let first = async {
            tokio::pin!(replication, refresh);
            tokio::select! {
                result = &mut replication => result.map_err(ServiceError::from).and(Err(ServiceError::Owner("directory egress ended"))),
                result = &mut refresh => result,
            }
        };
        tokio::pin!(first);
        let mut extras: FuturesUnordered<HostedFuture<'_>> = FuturesUnordered::new();
        let records = load_records(&self.root, self.plan)?;
        let hosted = &self.hosted;
        let (wal, budget, plan_root) = (&self.wal, &self.budget, &self.root);
        for (plan, image) in records {
            extras.push(Box::pin(drive_hosted(
                plan, image, root, pool, owners, wal, budget, hosted,
            )));
        }
        loop {
            tokio::select! {
                result = &mut first => return result,
                Some(result) = extras.next(), if !extras.is_empty() => {
                    return result.and(Err(ServiceError::Owner("hosted partition ended")));
                }
                request = self.requests.recv() => {
                    let Some(request) = request else {
                        return Err(ServiceError::Owner("partition host requests ended"));
                    };
                    match request {
                        HostRequest::Host { plan, image } => {
                            if hosted.borrow().contains_key(&plan.partition())
                                || plan.partition() == self.plan.partition()
                            {
                                continue;
                            }
                            if hosted.borrow().len() >= MAX_HOSTED_PARTITIONS {
                                continue;
                            }
                            record(plan_root, *plan, &image)?;
                            extras.push(Box::pin(drive_hosted(
                                *plan, *image, root, pool, owners, wal, budget, hosted,
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
) -> Result<crate::directory_bootstrap::PartitionBootstrapPermit, ServiceError> {
    let mut remaining = rounds;
    loop {
        if root.progress().stopped {
            return Err(ServiceError::Owner("root owner ended"));
        }
        match root.prepare_directory(plan).await {
            Ok(permit) => return Ok(permit),
            Err(
                DirectoryBootstrapError::Unauthorized
                | DirectoryBootstrapError::Unavailable
                | DirectoryBootstrapError::NotReady
                | DirectoryBootstrapError::Capacity,
            ) => {
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
    image: PartitionCheckpoint,
    root: &ControlHost,
    pool: &PeerConnectionPool,
    owners: &OwnerGate,
    wal: &SharedWal,
    budget: &MemoryBudget,
    hosted: &watch::Sender<BTreeMap<PartitionId, HostedPartition>>,
) -> Result<(), ServiceError> {
    let permit = permit(root, plan, Some(2_400)).await?;
    let installed_index = permit.root_index();
    let expires_at = permit.expires_at();
    let (host, owner, output) =
        ControlHost::spawn_directory(permit, wal.clone(), budget.clone(), Some(image))?;
    owners.register(PhysicalOwner::Control(owner))?;
    hosted.send_modify(|map| {
        map.insert(
            plan.partition(),
            HostedPartition {
                plan,
                host: host.clone(),
            },
        );
    });
    let replication = crate::replication::drive_directory_replication(output, pool, 16);
    let refresh = refresh_authority(plan, root, &host, installed_index, expires_at);
    tokio::pin!(replication, refresh);
    tokio::select! {
        result = &mut replication => result.map_err(ServiceError::from).and(Err(ServiceError::Owner("hosted partition egress ended"))),
        result = &mut refresh => result,
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
    image: &PartitionCheckpoint,
) -> Result<(), ServiceError> {
    let directory = root.join(RECORD_DIRECTORY);
    std::fs::create_dir_all(&directory)?;
    let bytes = postcard::to_stdvec(&HostedRecord {
        schema: RECORD_SCHEMA,
        cluster: plan.cluster(),
        founder_node: plan.founder_node(),
        delegation: plan.delegation(),
        image: image.clone(),
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
) -> Result<Vec<(PartitionPlan, PartitionCheckpoint)>, ServiceError> {
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
        let record: HostedRecord = postcard::from_bytes(&bytes)
            .map_err(|_| ServiceError::Owner("hosted partition record is corrupt"))?;
        if record.schema != RECORD_SCHEMA
            || record.cluster != first.cluster()
            || record.founder_node != first.founder_node()
        {
            return Err(ServiceError::Owner("hosted partition record is foreign"));
        }
        let plan = PartitionPlan::split_destination(
            record.cluster,
            record.founder_node,
            record.delegation,
            &record.image,
        )?;
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
) -> Result<(), ServiceError> {
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
                }
                Err(error) => Err(error),
            };
            match outcome {
                Ok((receipt, valid_until)) => {
                    if receipt.root != root.progress().identity
                        || receipt.root_index < installed_index
                    {
                        return Err(DirectoryBootstrapError::Inconsistent.into());
                    }
                    installed_index = receipt.root_index;
                    expires_at = valid_until;
                }
                Err(
                    DirectoryBootstrapError::Unauthorized
                    | DirectoryBootstrapError::Unavailable
                    | DirectoryBootstrapError::NotReady
                    | DirectoryBootstrapError::Capacity,
                ) => {}
                Err(error) => return Err(error.into()),
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
