//! Trusted first-directory bootstrap from committed root metadata. A plan is
//! descriptive; only the actual root owner can mint an activation permit.
use focal_consensus::{NodeConfig, SharedWal, StateRole};
use focal_control::{
    AuthorityActivation, AuthorityInstallation, ControlAuthoritySnapshot, ControlBootstrap,
    ControlCommand, ControlError, ControlIdentity, ControlOptions, ControlRead, ControlReadResult,
    ControlReceipt, ControlReplica, ControlRequest, ControlRequestId, ControlScope,
};
use focal_directory::{
    ClusterId, Delegation, GroupScope, LogGroupId, NamespaceRange, PartitionCheckpoint,
    PartitionId, RegionId,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ContentHash, LedgerId, SessionId, TenantId};
use std::collections::BTreeMap;

const MAX_ACTIVATION_BYTES: usize = 128 * 1024;
const STARTUP_ROUNDS: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum DirectoryBootstrapError {
    #[error("committed root metadata does not authorize this directory bootstrap")]
    Unauthorized,
    #[error("directory bootstrap exceeds its bounded allowance")]
    Capacity,
    #[error("directory bootstrap owner is unavailable")]
    Unavailable,
    #[error("directory bootstrap has not completed its committed activation")]
    NotReady,
    #[error("directory bootstrap state is inconsistent")]
    Inconsistent,
    #[error("directory control: {0}")]
    Control(#[from] ControlError),
}

/// Stable coordinates for one initial interval. These identifiers neither
/// authorize opening a group nor provide a table of individual sessions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionPlan {
    cluster: [u8; 16],
    founder_node: u64,
    partition: PartitionId,
    group: LogGroupId,
    namespace: LedgerId,
    /// The delegation the root holds (or will hold) for this partition.
    delegation: Delegation,
    /// The digest of the sealed image a split destination bootstraps from;
    /// none for the first partition, which starts empty.
    image: Option<ContentHash>,
    /// The control genesis of the bootstrap state, fixed at planning so the
    /// identity never depends on re-deriving the image.
    genesis: [u8; 32],
}
/// The first partition's plan; every later partition uses the same shape.
pub type FirstDirectoryPlan = PartitionPlan;
impl PartitionPlan {
    pub fn derive(cluster: [u8; 16], founder_node: u64) -> Result<Self, DirectoryBootstrapError> {
        if cluster == [0; 16] || founder_node == 0 {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        let partition = PartitionId(derived_id("focal.directory.first-partition.v1", cluster)?);
        let group = LogGroupId(derived_id("focal.directory.first-group.v1", cluster)?);
        let namespace = rpc_namespace(cluster)?;
        let delegation = Delegation {
            namespace: NamespaceRange::all(),
            partition,
            region: RegionId::UNKNOWN,
            log_group: group,
            epoch: 1,
            activation: None,
        };
        let mut plan = Self {
            cluster,
            founder_node,
            partition,
            group,
            namespace,
            delegation,
            image: None,
            genesis: [0; 32],
        };
        plan.genesis = plan.bootstrap(None)?.identity(&plan.options())?.genesis;
        Ok(plan)
    }
    /// The plan of a split's destination: a fresh group on `founder_node`
    /// whose first state is the source's sealed image, under the delegation
    /// the root commits with the split (its activation fence included).
    pub fn split_destination(
        cluster: [u8; 16],
        founder_node: u64,
        delegation: Delegation,
        image: &PartitionCheckpoint,
    ) -> Result<Self, DirectoryBootstrapError> {
        if cluster == [0; 16]
            || founder_node == 0
            || delegation.activation.is_none()
            || image.cluster.0 != cluster
            || image.sealed.as_ref().is_none_or(|seal| {
                seal.destination != delegation.partition || seal.moved != delegation.namespace
            })
        {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        let digest = focal_directory::partition_checkpoint_digest(image)
            .map_err(|_| DirectoryBootstrapError::Inconsistent)?;
        let mut plan = Self {
            cluster,
            founder_node,
            partition: delegation.partition,
            group: delegation.log_group,
            namespace: rpc_namespace(cluster)?,
            delegation,
            image: Some(digest),
            genesis: [0; 32],
        };
        plan.genesis = plan
            .bootstrap(Some(image.clone()))?
            .identity(&plan.options())?
            .genesis;
        Ok(plan)
    }
    pub fn range(&self) -> NamespaceRange {
        self.delegation.namespace
    }
    pub fn image(&self) -> Option<ContentHash> {
        self.image
    }
    /// Whether a partition checkpoint belongs to the group this plan hosts:
    /// the partition and group must be this plan's, whatever the delegation
    /// became through splits, merges and seals since.
    pub fn accepts(&self, checkpoint: &PartitionCheckpoint) -> bool {
        checkpoint.delegation.partition == self.partition
            && checkpoint.delegation.log_group == self.group
    }
    pub fn partition(&self) -> PartitionId {
        self.partition
    }
    pub fn group(&self) -> LogGroupId {
        self.group
    }
    pub fn namespace(&self) -> LedgerId {
        self.namespace
    }
    pub fn cluster(&self) -> [u8; 16] {
        self.cluster
    }
    pub fn founder_node(&self) -> u64 {
        self.founder_node
    }
    pub fn delegation(&self) -> Delegation {
        self.delegation
    }
    /// The bootstrap state: empty for the first partition, the hash-checked
    /// sealed image for a split destination.
    pub fn bootstrap(
        &self,
        image: Option<PartitionCheckpoint>,
    ) -> Result<ControlBootstrap, DirectoryBootstrapError> {
        let directory = match (self.image, image) {
            (None, None) => PartitionCheckpoint {
                schema: focal_directory::PARTITION_CHECKPOINT_SCHEMA,
                cluster: ClusterId(self.cluster),
                delegation: self.delegation,
                revision: 0,
                sealed: None,
                nodes: std::sync::Arc::new(BTreeMap::new()),
                sessions: std::sync::Arc::new(BTreeMap::new()),
                routes: std::sync::Arc::new(std::collections::VecDeque::new()),
                routes_from: 0,
            },
            (Some(expected), Some(image)) => {
                let digest = focal_directory::partition_checkpoint_digest(&image)
                    .map_err(|_| DirectoryBootstrapError::Inconsistent)?;
                if digest != expected || !self.accepts(&image) {
                    return Err(DirectoryBootstrapError::Unauthorized);
                }
                image
            }
            _ => return Err(DirectoryBootstrapError::Unauthorized),
        };
        Ok(ControlBootstrap::Partition { directory })
    }
    pub fn identity(&self) -> Result<ControlIdentity, DirectoryBootstrapError> {
        if self.genesis == [0; 32] {
            return Err(DirectoryBootstrapError::Inconsistent);
        }
        Ok(ControlIdentity {
            cluster: ClusterId(self.cluster),
            group: self.group.0,
            scope: focal_control::ControlScope::Partition(self.partition),
            genesis: self.genesis,
        })
    }
    fn options(&self) -> ControlOptions {
        ControlOptions::new(NodeConfig::single(
            self.founder_node,
            self.cluster,
            self.group.0,
        ))
    }
    fn client(&self) -> Result<[u8; 16], DirectoryBootstrapError> {
        derived_id("focal.directory.authority-install-client.v1", self.cluster)
    }
}

fn rpc_namespace(cluster: [u8; 16]) -> Result<LedgerId, DirectoryBootstrapError> {
    Ok(LedgerId {
        tenant: TenantId(derived_id("focal.directory.rpc-tenant.v1", cluster)?),
        session: SessionId(derived_id("focal.directory.rpc-session.v1", cluster)?),
    })
}
fn derived_id(
    domain: &'static str,
    cluster: [u8; 16],
) -> Result<[u8; 16], DirectoryBootstrapError> {
    let mut hash = blake3::Hasher::new_derive_key(domain);
    hash.update(&cluster);
    let mut id = [0; 16];
    for (target, source) in id.iter_mut().zip(hash.finalize().as_bytes()) {
        *target = *source;
    }
    if id == [0; 16] {
        return Err(DirectoryBootstrapError::Inconsistent);
    }
    Ok(id)
}

/// This value cannot be deserialized, cloned, or constructed from a caller's
/// snapshot. Its admission remains owned through WAL recovery and activation.
pub struct PartitionBootstrapPermit {
    plan: FirstDirectoryPlan,
    snapshot: ControlAuthoritySnapshot,
    prepared_at: i64,
    expires_at: i64,
    _allocation: Allocation,
}

/// A committed destination installation observed behind a fresh destination
/// quorum barrier. Root and destination indexes belong to different logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryAuthorityReceipt {
    pub root: ControlIdentity,
    pub root_index: u64,
    pub receipt: ControlReceipt,
    pub applied_index: u64,
}

pub(crate) struct DirectoryAuthorityChange {
    plan: FirstDirectoryPlan,
    root: ControlIdentity,
    root_index: u64,
    expires_at: i64,
    pub(crate) request: Option<ControlRequest>,
    pub(crate) previous: ControlReceipt,
    // Retain the exact intent and its original root export allowance through
    // cancellation or an unknown proposal outcome, until reconciliation.
    _allocation: Allocation,
}

fn validate_destination(
    replica: &ControlReplica,
    plan: FirstDirectoryPlan,
) -> Result<(), DirectoryBootstrapError> {
    if replica.identity() != plan.identity()? || replica.status().node_id != plan.founder_node {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    let partition = replica
        .partition()
        .ok_or(DirectoryBootstrapError::Unauthorized)?;
    let configuration = replica.configuration().configuration;
    if !plan.accepts(partition.checkpoint())
        || configuration.voters.as_slice() != [plan.founder_node]
        || !configuration.learners.is_empty()
        || !configuration.voters_outgoing.is_empty()
        || !configuration.learners_next.is_empty()
        || configuration.auto_leave
    {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    Ok(())
}

fn installed_authority(
    replica: &ControlReplica,
    budget: &MemoryBudget,
) -> Result<(ControlAuthoritySnapshot, Allocation), DirectoryBootstrapError> {
    let allocation = budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            replica.read_charge(&ControlRead::Authority)?,
        )
        .map_err(|_| DirectoryBootstrapError::Capacity)?
        .commit();
    let ControlReadResult::Authority(Some(snapshot)) =
        replica.read_local(&ControlRead::Authority)?
    else {
        return Err(DirectoryBootstrapError::Inconsistent);
    };
    Ok((snapshot, allocation))
}

impl PartitionBootstrapPermit {
    pub fn plan(&self) -> FirstDirectoryPlan {
        self.plan
    }
    pub fn root_identity(&self) -> ControlIdentity {
        self.snapshot.source
    }
    pub fn root_index(&self) -> u64 {
        self.snapshot.source_index
    }
    pub fn expires_at(&self) -> i64 {
        self.expires_at
    }

    /// Consume only after the destination owner has crossed a fresh ReadIndex.
    /// The original request remains owned until its durable outcome is known.
    pub(crate) fn prepare_refresh(
        self,
        replica: &ControlReplica,
        now: i64,
        budget: &MemoryBudget,
    ) -> Result<DirectoryAuthorityChange, DirectoryBootstrapError> {
        validate_destination(replica, self.plan)?;
        if now < self.prepared_at || now >= self.expires_at {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        if replica.has_pending() {
            return Err(DirectoryBootstrapError::NotReady);
        }
        let (_export, current) = {
            let (current, allocation) = installed_authority(replica, budget)?;
            (allocation, current)
        };
        let client = self.plan.client()?;
        let previous = replica
            .latest_receipt(client)?
            .ok_or(DirectoryBootstrapError::Inconsistent)?;
        if current.source != self.snapshot.source || current.identity != self.plan.identity()? {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        if current.source_index > self.snapshot.source_index {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        if current.source_index == self.snapshot.source_index
            && (current.authority != self.snapshot.authority
                || current.enrollment != self.snapshot.enrollment)
        {
            return Err(DirectoryBootstrapError::Inconsistent);
        }
        let Self {
            plan,
            snapshot,
            expires_at,
            _allocation,
            ..
        } = self;
        let root = snapshot.source;
        let root_index = snapshot.source_index;
        let request = if current.source_index == root_index {
            None
        } else {
            Some(ControlRequest {
                id: ControlRequestId {
                    client,
                    sequence: previous
                        .request
                        .sequence
                        .checked_add(1)
                        .ok_or(DirectoryBootstrapError::Capacity)?,
                },
                acknowledged_through: previous.request.sequence,
                command: ControlCommand::InstallAuthority(AuthorityInstallation {
                    expected_source_index: current.source_index,
                    decided_at: now,
                    snapshot,
                }),
            })
        };
        Ok(DirectoryAuthorityChange {
            plan,
            root,
            root_index,
            expires_at,
            request,
            previous,
            _allocation,
        })
    }
}

impl DirectoryAuthorityChange {
    pub(crate) fn complete(
        &self,
        replica: &ControlReplica,
        receipt: ControlReceipt,
        now: i64,
        budget: &MemoryBudget,
    ) -> Result<DirectoryAuthorityReceipt, DirectoryBootstrapError> {
        validate_destination(replica, self.plan)?;
        if now >= self.expires_at {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        let (_export, snapshot) = {
            let (snapshot, allocation) = installed_authority(replica, budget)?;
            (allocation, snapshot)
        };
        if snapshot.source != self.root
            || snapshot.source_index != self.root_index
            || replica.latest_receipt(self.plan.client()?)? != Some(receipt)
            || receipt.committed_index > replica.applied_index()
        {
            return Err(DirectoryBootstrapError::Inconsistent);
        }
        Ok(DirectoryAuthorityReceipt {
            root: self.root,
            root_index: self.root_index,
            receipt,
            applied_index: replica.applied_index(),
        })
    }
}

pub struct BootstrappedDirectory {
    replica: ControlReplica,
    plan: FirstDirectoryPlan,
}
impl BootstrappedDirectory {
    pub fn plan(&self) -> FirstDirectoryPlan {
        self.plan
    }
    pub fn replica(&self) -> &ControlReplica {
        &self.replica
    }
    pub fn into_replica(self) -> ControlReplica {
        self.replica
    }
}

/// Invoke in the root owner immediately after a fresh ReadIndex barrier. This
/// synchronous validation reads one already-published owner prefix; it cannot
/// turn a previously decoded snapshot or a Node certificate into authority.
pub(crate) fn authorize_first_directory(
    owner: &ControlReplica,
    plan: FirstDirectoryPlan,
    now: i64,
    budget: &MemoryBudget,
) -> Result<PartitionBootstrapPermit, DirectoryBootstrapError> {
    let bytes = owner
        .read_charge(&ControlRead::Authority)?
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or(DirectoryBootstrapError::Capacity)?;
    let allocation = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)
        .map_err(|_| DirectoryBootstrapError::Capacity)?
        .commit();
    drop(owner.read_local(&ControlRead::Configuration)?);
    let status = owner.status();
    if owner.identity().scope != ControlScope::Root
        || owner.identity().cluster.0 != plan.cluster
        || owner.identity().group != crate::network_state::root_group(plan.cluster)
        || status.role != StateRole::Leader
        || status.applied_index != owner.applied_index()
        || status.committed_index != owner.applied_index()
        || owner.applied_index() == 0
        || now < 0
    {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    let root = owner.root().ok_or(DirectoryBootstrapError::Unauthorized)?;
    // The first partition needs a delegation committed to it (whatever its
    // namespace and epoch after splits and merges); a split destination is
    // authorized before the root commits the split (the split needs the
    // destination group's signature), so its delegation may still be absent.
    let delegated = root
        .checkpoint()
        .delegations
        .values()
        .any(|delegation| delegation.partition == plan.partition);
    if !delegated && plan.image.is_none() {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    let authority = owner
        .authority()
        .ok_or(DirectoryBootstrapError::Unauthorized)?;
    let enrollment = owner
        .enrollment()
        .ok_or(DirectoryBootstrapError::Unauthorized)?;
    let node = authority
        .node(plan.founder_node)
        .ok_or(DirectoryBootstrapError::Unauthorized)?;
    let group = authority
        .group(plan.group)
        .ok_or(DirectoryBootstrapError::Unauthorized)?;
    if authority.checkpoint().clock > now
        || authority.applied_index() > owner.applied_index()
        || authority.revision() == 0
        || enrollment.applied_index() > owner.applied_index()
        || group.genesis != ContentHash(plan.identity()?.genesis)
        || group.scope
            != (GroupScope::Partition {
                partition: plan.partition,
                namespace: plan.range(),
            })
        || group.membership_epoch != 1
        || group.voters.len() != 1
        || group.voters.get(&plan.founder_node) != Some(&node.enrollment.generation)
        || !group.outgoing_voters.is_empty()
        || !group.learners.is_empty()
        || !node.enrollment.eligible
        || group.expires_at <= now
        || node.expires_at < group.expires_at
    {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    let certificate = enrollment
        .enrollments()
        .find(|receipt| {
            receipt.identity.node_id == Some(plan.founder_node)
                && receipt.identity.principal == node.principal
                && ContentHash(receipt.public_key) == node.enrollment.identity
        })
        .ok_or(DirectoryBootstrapError::Unauthorized)?;
    let identity = enrollment
        .authorize_certificate(&certificate.certificate, now)
        .map_err(|_| DirectoryBootstrapError::Unauthorized)?;
    if identity.role != focal_enrollment::EnrollmentRole::Node
        || identity.node_id != Some(plan.founder_node)
        || certificate.expires_at < group.expires_at
    {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    let expires_at = group.expires_at;
    let ControlReadResult::Authority(Some(snapshot)) = owner.read_local(&ControlRead::Authority)?
    else {
        return Err(DirectoryBootstrapError::Unauthorized);
    };
    let encoded = postcard::experimental::serialized_size(&snapshot)
        .map_err(|_| DirectoryBootstrapError::Capacity)?;
    if encoded
        .checked_add(4096)
        .is_none_or(|bytes| bytes > MAX_ACTIVATION_BYTES)
    {
        return Err(DirectoryBootstrapError::Capacity);
    }
    if snapshot.identity != owner.identity()
        || snapshot.source != owner.identity()
        || snapshot.source_index != owner.applied_index()
        || snapshot.applied_index != owner.applied_index()
        || snapshot.decided_at > now
    {
        return Err(DirectoryBootstrapError::Inconsistent);
    }
    Ok(PartitionBootstrapPermit {
        plan,
        snapshot,
        prepared_at: now,
        expires_at,
        _allocation: allocation,
    })
}

/// Compute one small next intent from the control owner's opaque observation.
/// The caller retains its bounded controller workspace for the returned command;
/// the temporary enrollment decode has its own admission before allocation.
pub(crate) fn next_first_directory_command(
    plan: FirstDirectoryPlan,
    observation: &crate::control_host::RootObservation,
    now: i64,
    budget: &MemoryBudget,
) -> Result<Option<ControlCommand>, DirectoryBootstrapError> {
    use focal_control::{ControlEvidence, VerifiedRootCommand};
    use focal_directory::{
        AuthorityCommand, AuthorityOperation, GroupAuthorityGrant, RootCommand, RootOperation,
    };
    let snapshot = observation.snapshot();
    if snapshot.identity.scope != ControlScope::Root
        || snapshot.identity.cluster.0 != plan.cluster
        || snapshot.identity.group != crate::network_state::root_group(plan.cluster)
        || snapshot.applied_index == 0
        || now < 0
    {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    let ControlBootstrap::Root {
        directory,
        enrollment,
    } = &snapshot.state
    else {
        return Err(DirectoryBootstrapError::Unauthorized);
    };
    let authority = observation
        .authority()
        .ok_or(DirectoryBootstrapError::NotReady)?;
    if authority.revision == 0
        || authority.applied_index > snapshot.applied_index
        || authority.clock > now
        || authority.anchor.cluster != snapshot.identity.cluster
        || authority.anchor.metadata_group.0 != snapshot.identity.group
        || authority.anchor.genesis != ContentHash(snapshot.identity.genesis)
        || enrollment.len() > MAX_ACTIVATION_BYTES
    {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    let bytes = enrollment
        .len()
        .checked_mul(64)
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or(DirectoryBootstrapError::Capacity)?;
    let _decode = budget
        .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)
        .map_err(|_| DirectoryBootstrapError::Capacity)?
        .commit();
    let enrollment = focal_enrollment::EnrollmentRegistry::restore(
        enrollment,
        plan.cluster,
        focal_enrollment::EnrollmentLimits::default(),
    )
    .map_err(|_| DirectoryBootstrapError::Inconsistent)?;
    if enrollment.applied_index() > snapshot.applied_index {
        return Err(DirectoryBootstrapError::Inconsistent);
    }
    let node = authority
        .nodes
        .get(&plan.founder_node)
        .ok_or(DirectoryBootstrapError::NotReady)?;
    if !node.enrollment.eligible || node.expires_at <= now {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    let receipt = enrollment
        .enrollments()
        .find(|receipt| {
            receipt.identity.node_id == Some(plan.founder_node)
                && receipt.identity.principal == node.principal
                && ContentHash(receipt.public_key) == node.enrollment.identity
        })
        .ok_or(DirectoryBootstrapError::Unauthorized)?;
    let identity = enrollment
        .authorize_certificate(&receipt.certificate, now)
        .map_err(|_| DirectoryBootstrapError::Unauthorized)?;
    if identity.role != focal_enrollment::EnrollmentRole::Node
        || node.expires_at > receipt.expires_at
    {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    let delegation = plan.delegation();
    let delegated = directory
        .delegations
        .values()
        .any(|existing| existing.partition == plan.partition);
    match (delegated, plan.image.is_some()) {
        (true, _) | (false, true) => {}
        (false, false) => {
            if !directory.delegations.is_empty() {
                return Err(DirectoryBootstrapError::Unauthorized);
            }
            return Ok(Some(ControlCommand::VerifiedRoot(VerifiedRootCommand {
                command: RootCommand {
                    expected_revision: directory.revision,
                    operation: RootOperation::Delegate { delegation },
                },
                evidence: ControlEvidence {
                    authority_revision: authority.revision,
                    enrollment_revision: enrollment.revision(),
                    decided_at: now,
                    proofs: Vec::new(),
                },
            })));
        }
    }
    let grant = GroupAuthorityGrant {
        group: plan.group,
        genesis: ContentHash(plan.identity()?.genesis),
        scope: GroupScope::Partition {
            partition: plan.partition,
            namespace: plan.range(),
        },
        membership_epoch: 1,
        voters: BTreeMap::from([(plan.founder_node, node.enrollment.generation)]),
        outgoing_voters: BTreeMap::new(),
        learners: BTreeMap::new(),
        expires_at: node.expires_at,
    };
    if let Some(existing) = authority.groups.get(&plan.group) {
        if existing.group != grant.group
            || existing.genesis != grant.genesis
            || existing.scope != grant.scope
            || existing.membership_epoch != grant.membership_epoch
            || existing.voters != grant.voters
            || !existing.outgoing_voters.is_empty()
            || !existing.learners.is_empty()
            || existing.expires_at <= now
            || existing.expires_at > node.expires_at
        {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        return Ok(None);
    }
    Ok(Some(ControlCommand::Authority(AuthorityCommand {
        expected_revision: authority.revision,
        enrollment_revision: enrollment.revision(),
        decided_at: now,
        operation: AuthorityOperation::BootstrapGroup { grant },
    })))
}

impl PartitionBootstrapPermit {
    /// Open the one assigned initial group on the existing physical WAL. The
    /// caller must hold its bounded owner slot; this function creates no worker
    /// or actor. Expanded/reassigned groups require their placement controller.
    pub fn open(
        self,
        wal: SharedWal,
        budget: &MemoryBudget,
        image: Option<PartitionCheckpoint>,
    ) -> Result<BootstrappedDirectory, DirectoryBootstrapError> {
        let now = crate::network_bootstrap::unix_time()
            .map_err(|_| DirectoryBootstrapError::Unavailable)?;
        if now < self.prepared_at || now >= self.expires_at {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        let Self {
            plan,
            snapshot,
            _allocation: _permit,
            ..
        } = self;
        let source = snapshot.source;
        let source_index = snapshot.source_index;
        let mut replica = ControlReplica::open_on_wal(
            plan.options(),
            plan.bootstrap(image)?,
            budget.clone(),
            wal,
        )?;
        // Single-voter recovery settles any previous unknown activation before
        // inspecting its durable receipt or selecting its successor sequence.
        establish_barrier(&mut replica, plan)?;
        let recovered = replica
            .partition()
            .ok_or(DirectoryBootstrapError::Inconsistent)?;
        if !plan.accepts(recovered.checkpoint()) {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        let _export = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                replica.read_charge(&ControlRead::Authority)?,
            )
            .map_err(|_| DirectoryBootstrapError::Capacity)?
            .commit();
        let ControlReadResult::Authority(current) = replica.read_local(&ControlRead::Authority)?
        else {
            return Err(DirectoryBootstrapError::Inconsistent);
        };
        let client = plan.client()?;
        let previous = replica.latest_receipt(client)?;
        let command = match current {
            None => {
                if previous.is_some() {
                    return Err(DirectoryBootstrapError::Inconsistent);
                }
                ControlCommand::ActivateAuthority(AuthorityActivation::Partition {
                    expected_partition_revision: replica
                        .partition()
                        .ok_or(DirectoryBootstrapError::Inconsistent)?
                        .revision(),
                    snapshot,
                })
            }
            Some(current) => {
                if previous.is_none()
                    || current.source != source
                    || current.identity != plan.identity()?
                    || current.source_index > source_index
                {
                    return Err(DirectoryBootstrapError::Inconsistent);
                }
                if current.source_index == source_index {
                    if current.authority != snapshot.authority
                        || current.enrollment != snapshot.enrollment
                    {
                        return Err(DirectoryBootstrapError::Inconsistent);
                    }
                    return Ok(BootstrappedDirectory { replica, plan });
                }
                ControlCommand::InstallAuthority(AuthorityInstallation {
                    expected_source_index: current.source_index,
                    decided_at: now,
                    snapshot,
                })
            }
        };
        let previous_sequence = previous.map_or(0, |receipt| receipt.request.sequence);
        let id = ControlRequestId {
            client,
            sequence: previous_sequence
                .checked_add(1)
                .ok_or(DirectoryBootstrapError::Capacity)?,
        };
        replica.submit(
            ControlRequest {
                id,
                acknowledged_through: previous_sequence,
                command,
            },
            &crate::cluster::NoDirectoryAuthority,
        )?;
        for _ in 0..STARTUP_ROUNDS {
            drain_single(&mut replica, plan)?;
            if replica.receipt(id)?.is_some() {
                establish_barrier(&mut replica, plan)?;
                return Ok(BootstrappedDirectory { replica, plan });
            }
        }
        Err(DirectoryBootstrapError::NotReady)
    }
}

fn drain_single(
    replica: &mut ControlReplica,
    plan: FirstDirectoryPlan,
) -> Result<(), DirectoryBootstrapError> {
    let events = replica.drain(&crate::cluster::NoDirectoryAuthority)?;
    let configuration = replica.configuration().configuration;
    if configuration.voters.as_slice() != [plan.founder_node]
        || !configuration.learners.is_empty()
        || !configuration.voters_outgoing.is_empty()
        || !configuration.learners_next.is_empty()
        || configuration.auto_leave
        || !events.messages.is_empty()
    {
        return Err(DirectoryBootstrapError::Unauthorized);
    }
    Ok(())
}

fn establish_barrier(
    replica: &mut ControlReplica,
    plan: FirstDirectoryPlan,
) -> Result<(), DirectoryBootstrapError> {
    drain_single(replica, plan)?;
    if replica.status().role != StateRole::Leader {
        replica.campaign()?;
    }
    let context = b"focal.directory.bootstrap.read.v1";
    let mut requested = false;
    for _ in 0..STARTUP_ROUNDS {
        let events = replica.drain(&crate::cluster::NoDirectoryAuthority)?;
        if !events.messages.is_empty() {
            return Err(DirectoryBootstrapError::Unauthorized);
        }
        if requested
            && events
                .read_states
                .iter()
                .any(|read| read.context == context && read.index <= replica.applied_index())
        {
            return Ok(());
        }
        if !requested {
            match replica.read_index(context.to_vec()) {
                Ok(()) => requested = true,
                Err(ControlError::NotReady) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Err(DirectoryBootstrapError::NotReady)
}

#[cfg(test)]
#[path = "directory_bootstrap_tests.rs"]
pub(crate) mod tests;
