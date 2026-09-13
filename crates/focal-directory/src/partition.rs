use crate::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{LedgerId, RouteEpoch};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

/// Where a placement change stands. `Planned` is set by the plan itself;
/// every later phase except `Cutover` is derived from committed assignment
/// progress, and `Cutover` from the committed barrier fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementPhase {
    Planned,
    Preparing,
    Catchup,
    Custody,
    Promoting,
    Cutover,
    Failed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingPlacement {
    pub operation: OperationId,
    pub next_route: RouteEpoch,
    pub next_membership: u64,
    pub next_placement: u64,
    pub desired: PlacementSpec,
    pub phase: PlacementPhase,
    pub ready: BTreeMap<u64, ReplicaReady>,
    pub barrier: Option<SessionFence>,
    /// Load-report epochs the planner relied on, per selected node.
    pub observations: BTreeMap<u64, u64>,
    /// One entry per node of the desired placement once preparation began.
    pub progress: BTreeMap<u64, AssignmentProgress>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDescriptor {
    pub ledger: LedgerId,
    pub log_group: LogGroupId,
    pub revision: u64,
    pub route_epoch: RouteEpoch,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    pub active: PlacementSpec,
    pub authority: SessionFence,
    pub pending: Option<PendingPlacement>,
    /// Copies the last activation left behind; they hold data until retired.
    pub retiring: BTreeMap<u64, AssignmentProgress>,
    /// The newest refusals, oldest first, bounded by `PartitionConfig::max_refusals`.
    pub refusals: Vec<Refusal>,
    /// The node that founded the session's log alone: the exact bootstrap
    /// membership every later copy replays. `None` for sessions created
    /// with several voters at once and for sessions recorded before schema
    /// 6 (all founded by the cluster founder, which hosts fall back to).
    pub founder: Option<u64>,
    /// The session's range members and the replica each is held by, as its
    /// controller last published them from the committed map
    /// ([25](../../../docs/archictecutre/25-parallel-materialization-and-ranges.md)
    /// §9); `None` until a first publication (every member held by the
    /// voters). Schema 7.
    pub holders: Option<RangeHolders>,
}
/// The members of a session's range map at one range epoch, in key order,
/// each with the replica holding it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeHolders {
    pub epoch: u64,
    pub members: Vec<RangeHolder>,
}
/// One published member: its durable identity, the affinity it starts at
/// (`None` for the first), and its holding replica (`None`: the voters).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeHolder {
    pub member: focal_memory::RangeId,
    pub start: Option<[u8; 16]>,
    pub node: Option<u64>,
    pub generation: Option<u64>,
}
/// The most members one publication names: a layout's bound.
pub const MAX_PUBLISHED_HOLDERS: usize = 1024;
/// The current partition checkpoint layout; schema 1 to 6 checkpoints
/// convert on decode ([`PartitionCheckpoint::decode_any`]).
pub const PARTITION_CHECKPOINT_SCHEMA: u16 = 7;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCheckpoint {
    pub schema: u16,
    pub cluster: ClusterId,
    pub delegation: Delegation,
    pub revision: u64,
    pub sealed: Option<PartitionSeal>,
    /// Only relevant enrolled nodes, bounded independently of total fleet size.
    /// Serde-transparent `Arc`: a mutation clones the whole checkpoint to stage
    /// its change, so table-level sharing copies only the tables the command
    /// actually touches (`Arc::make_mut`) and leaves the rest shared.
    pub nodes: Arc<BTreeMap<u64, NodeRecord>>,
    /// Only this delegated interval's sessions, never every fleet session.
    pub sessions: Arc<BTreeMap<LedgerId, SessionDescriptor>>,
    /// The newest route changes, oldest first: which session's route epoch
    /// changed at which revision, so a route cache watching this partition
    /// invalidates exactly what moved (§14). Bounded; `routes_from` is the
    /// revision the log is complete after (older changes were evicted).
    pub routes: Arc<VecDeque<RouteChange>>,
    pub routes_from: u64,
}
/// One session's route epoch changed at one partition revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteChange {
    pub revision: u64,
    pub ledger: LedgerId,
    pub route_epoch: RouteEpoch,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSeal {
    pub operation: OperationId,
    pub destination: PartitionId,
    pub next_epoch: u64,
    pub revision: u64,
    /// The keys leaving this partition: its whole namespace for a transfer
    /// or a merge, the upper part from the split key for a split.
    pub moved: NamespaceRange,
    /// The partition that sealed: itself in its own state, the origin in the
    /// image a destination bootstraps from.
    pub source: PartitionId,
}
/// The group and partition identities a split's destination takes: derived
/// from the cluster and the split operation, so the source's voters, the
/// root and the destination all name the same ones.
pub fn split_group_id(cluster: ClusterId, operation: OperationId) -> LogGroupId {
    LogGroupId(split_identity(
        "focal.directory.split-group.v1",
        cluster,
        operation,
    ))
}
pub fn split_partition_id(cluster: ClusterId, operation: OperationId) -> PartitionId {
    PartitionId(split_identity(
        "focal.directory.split-partition.v1",
        cluster,
        operation,
    ))
}
fn split_identity(domain: &'static str, cluster: ClusterId, operation: OperationId) -> [u8; 16] {
    let mut hash = blake3::Hasher::new_derive_key(domain);
    hash.update(&cluster.0);
    hash.update(&operation.0);
    let mut id = [0; 16];
    for (target, source) in id.iter_mut().zip(hash.finalize().as_bytes()) {
        *target = *source;
    }
    id
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCommand {
    pub expected_revision: u64,
    pub delegation_epoch: u64,
    pub operation: PartitionOperation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionOperation {
    SealForTransfer {
        operation: OperationId,
        destination: PartitionId,
        next_epoch: u64,
    },
    /// Seal the keys at and above `at` for `destination`; the lower part keeps
    /// serving once the root has committed the split and this partition has
    /// released the upper part (§13).
    SealForSplit {
        operation: OperationId,
        destination: PartitionId,
        next_epoch: u64,
        at: NamespaceKey,
    },
    /// The source of a committed split narrows to the delegation the root
    /// holds for it and drops the sessions that left.
    Release {
        delegation: Delegation,
    },
    /// The destination of a committed merge takes the sealed checkpoint of
    /// the partition above it under the delegation the root holds for it.
    Absorb {
        delegation: Delegation,
        moved: Box<PartitionCheckpoint>,
    },
    /// A group bootstrapped on a sealed image (a transfer or split
    /// destination) becomes the partition the root delegated to it; its own
    /// first committed command, so every voter installs the same state.
    Install {
        delegation: Delegation,
    },
    Enroll {
        node: NodeEnrollment,
        expected_generation: Option<u64>,
    },
    ReportLoad {
        load: NodeLoad,
    },
    /// The detector's verdict about one node at its current enrollment
    /// generation, committed by the partition owner's leader (§12).
    Liveness {
        node: u64,
        generation: u64,
        alive: bool,
        incarnation: u64,
        witness: u64,
        decided_at: i64,
    },
    CreateSession {
        ledger: LedgerId,
        log_group: LogGroupId,
        placement: PlacementSpec,
        authority: SessionFence,
    },
    Session {
        ledger: LedgerId,
        expected_revision: u64,
        change: SessionChange,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionChange {
    Plan {
        operation: OperationId,
        desired: PlacementSpec,
        /// Load-report epochs the planner relied on; each must name a node of
        /// the desired placement whose committed report is at least that new.
        observations: BTreeMap<u64, u64>,
    },
    BeginPreparation {
        operation: OperationId,
    },
    Ready {
        ready: ReplicaReady,
    },
    Cutover {
        operation: OperationId,
        authority: SessionFence,
    },
    Activate {
        operation: OperationId,
        authority: SessionFence,
    },
    Abort {
        operation: OperationId,
    },
    /// Controller-observed progress of one assignment; monotone per attempt.
    Progress {
        operation: OperationId,
        progress: AssignmentProgress,
    },
    /// A refusal the controller or an agent recorded; a named node fails its
    /// assignment, an unnamed refusal concerns the plan as a whole.
    Refuse {
        operation: OperationId,
        refusal: Refusal,
    },
    /// Begin draining a copy the activation `operation` left behind.
    Drain {
        operation: OperationId,
        node: u64,
    },
    /// Forget a drained copy once nothing pins it.
    Retire {
        operation: OperationId,
        node: u64,
    },
    /// The session's controller publishes the committed range map's members
    /// and holders at a range epoch; monotone per session, idempotent for
    /// the same publication, and every holder a member of the active
    /// placement at its enrolled generation.
    Holders {
        holders: RangeHolders,
    },
}
#[derive(Debug, Clone, Copy)]
pub struct PartitionConfig {
    pub max_nodes: usize,
    pub max_sessions: usize,
    pub max_members: usize,
    pub max_policy_regions: usize,
    pub max_endpoint_bytes: usize,
    pub max_refusals: usize,
    /// Disk headroom a node must report before it is planned into a placement.
    pub min_disk_available: u64,
    /// Sessions a merge may move in one committed `Absorb` command, which
    /// carries the sealed checkpoint; larger partitions are not merged.
    pub max_absorb_sessions: usize,
    /// Route changes retained for cache invalidation; a cache further behind
    /// than this clears its entries for the partition.
    pub max_route_log: usize,
}
impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            max_nodes: 1024,
            max_sessions: 4096,
            max_members: 31,
            max_policy_regions: 64,
            max_endpoint_bytes: 512,
            max_refusals: 16,
            min_disk_available: 64 * 1024 * 1024,
            max_absorb_sessions: 256,
            max_route_log: 1024,
        }
    }
}
struct PartitionVersion {
    state: PartitionCheckpoint,
    _allocation: Allocation,
}
pub struct DirectoryPartition {
    root: PartitionVersion,
    owner: focal_memory::OwnerId,
    config: PartitionConfig,
    budget: MemoryBudget,
}
pub struct PreparedPartitionUpdate {
    owner: focal_memory::OwnerId,
    base_revision: u64,
    next: PartitionVersion,
}
impl PreparedPartitionUpdate {
    pub fn checkpoint(&self) -> &PartitionCheckpoint {
        &self.next.state
    }
}

impl DirectoryPartition {
    pub fn new(
        cluster: ClusterId,
        delegation: Delegation,
        config: PartitionConfig,
        budget: MemoryBudget,
    ) -> Result<Self, DirectoryError> {
        Self::restore(
            PartitionCheckpoint {
                schema: PARTITION_CHECKPOINT_SCHEMA,
                cluster,
                delegation,
                revision: 0,
                sealed: None,
                nodes: Arc::new(BTreeMap::new()),
                sessions: Arc::new(BTreeMap::new()),
                routes: Arc::new(VecDeque::new()),
                routes_from: 0,
            },
            config,
            budget,
        )
    }
    pub fn restore(
        state: PartitionCheckpoint,
        config: PartitionConfig,
        budget: MemoryBudget,
    ) -> Result<Self, DirectoryError> {
        validate_partition(&state, config)?;
        let allocation = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                partition_charge(&state)?,
            )?
            .commit();
        Ok(Self {
            owner: focal_memory::OwnerId::new()?,
            root: PartitionVersion {
                state,
                _allocation: allocation,
            },
            config,
            budget,
        })
    }
    pub fn checkpoint(&self) -> &PartitionCheckpoint {
        &self.root.state
    }
    /// Install only a hash-verified source checkpoint plus committed root
    /// delegation activation. The source remains durably sealed at its cut.
    pub fn install_transferred(
        mut state: PartitionCheckpoint,
        delegation: Delegation,
        verifier: &impl AuthorityVerifier,
        config: PartitionConfig,
        budget: MemoryBudget,
    ) -> Result<Self, DirectoryError> {
        validate_partition(&state, config)?;
        let fence = check_install(&state, &delegation)?;
        verifier.verify_delegation(&fence)?;
        if fence.checkpoint != partition_checkpoint_digest(&state)? {
            return Err(DirectoryError::CompareFailed);
        }
        state.delegation = delegation;
        state.sealed = None;
        Self::restore(state, config, budget)
    }
    pub fn revision(&self) -> u64 {
        self.root.state.revision
    }
    /// The checkpoint a split's destination installs: this partition's sealed
    /// state carrying only the sessions of the moved range. Deterministic, so
    /// every voter of the source group derives the same image and digest.
    pub fn split_image(&self) -> Result<PartitionCheckpoint, DirectoryError> {
        split_image(&self.root.state)
    }
    pub fn get(&self, ledger: LedgerId) -> Result<Option<&SessionDescriptor>, DirectoryError> {
        if !self.root.state.delegation.namespace.contains(ledger) {
            return Err(DirectoryError::OutsideNamespace);
        }
        Ok(self.root.state.sessions.get(&ledger))
    }
    pub fn lookup(
        &self,
        ledger: LedgerId,
        delegation_epoch: u64,
    ) -> Result<SessionRoute, DirectoryError> {
        if delegation_epoch != self.root.state.delegation.epoch {
            return Err(DirectoryError::StaleEpoch);
        }
        let descriptor = self.get(ledger)?.ok_or(DirectoryError::Missing)?;
        let leader = descriptor.active.placement.preferred_leader;
        Ok(SessionRoute {
            ledger,
            partition: self.root.state.delegation.partition,
            delegation_epoch,
            source_revision: self.revision(),
            route_epoch: descriptor.route_epoch,
            membership_epoch: descriptor.membership_epoch,
            placement_epoch: descriptor.placement_epoch,
            leader,
            leader_generation: *descriptor
                .active
                .placement
                .voters
                .get(&leader)
                .ok_or(DirectoryError::Missing)?,
            activation: descriptor.authority.record_hash,
        })
    }
    /// The route changes committed after `after`, for a cache watching this
    /// partition. When the log no longer reaches back to `after`, the batch
    /// starts at the revision it is complete after instead, which the cache
    /// reads as a gap and clears its entries for this partition.
    pub fn route_changes(&self, after: u64) -> InvalidationBatch {
        let state = &self.root.state;
        let after_revision = after.max(state.routes_from);
        InvalidationBatch {
            partition: state.delegation.partition,
            delegation_epoch: state.delegation.epoch,
            after_revision,
            through_revision: state.revision,
            changes: state
                .routes
                .iter()
                .filter(|change| change.revision > after_revision)
                .map(|change| RouteInvalidation {
                    ledger: change.ledger,
                    route_epoch: change.route_epoch,
                })
                .collect(),
        }
    }
    pub fn prepare(
        &self,
        command: &PartitionCommand,
        verifier: &impl AuthorityVerifier,
    ) -> Result<PreparedPartitionUpdate, DirectoryError> {
        // A sealed partition changes nothing more, except that the source of
        // a split releases the part that left once the root committed it.
        if self.root.state.sealed.is_some()
            && !matches!(
                command.operation,
                PartitionOperation::Release { .. } | PartitionOperation::Install { .. }
            )
        {
            return Err(DirectoryError::StaleEpoch);
        }
        if command.expected_revision != self.revision() {
            return Err(DirectoryError::CompareFailed);
        }
        if command.delegation_epoch != self.root.state.delegation.epoch {
            return Err(DirectoryError::StaleEpoch);
        }
        validate_input(&command.operation, self.config)?;
        let revision = self
            .revision()
            .checked_add(1)
            .ok_or(DirectoryError::CounterExhausted)?;
        let extra = match &command.operation {
            PartitionOperation::Enroll { node, .. } => {
                add(tree_row::<(u64, NodeRecord)>(), node.endpoint.capacity())?
            }
            PartitionOperation::CreateSession { placement, .. } => add(
                tree_row::<(LedgerId, SessionDescriptor)>(),
                placement::spec_charge(placement)?,
            )?,
            PartitionOperation::Session { ledger, change, .. } => {
                partition_session::change_charge(&self.root.state, *ledger, change)?
            }
            PartitionOperation::Absorb { moved, .. } => partition_charge(moved)?,
            PartitionOperation::SealForTransfer { .. }
            | PartitionOperation::SealForSplit { .. }
            | PartitionOperation::Release { .. }
            | PartitionOperation::Install { .. }
            | PartitionOperation::ReportLoad { .. }
            | PartitionOperation::Liveness { .. } => 0,
        };
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                add(partition_charge(&self.root.state)?, extra)?,
            )?
            .commit();
        let mut state = self.root.state.clone();
        // Sessions whose route may change under this command, with the route
        // epoch they had before it: the log records exactly what changed.
        let touched: Vec<(LedgerId, Option<RouteEpoch>)> = match &command.operation {
            PartitionOperation::CreateSession { ledger, .. }
            | PartitionOperation::Session { ledger, .. } => {
                vec![(*ledger, state.sessions.get(ledger).map(|s| s.route_epoch))]
            }
            PartitionOperation::Absorb { moved, .. } => moved
                .sessions
                .keys()
                .map(|ledger| (*ledger, None))
                .collect(),
            _ => Vec::new(),
        };
        match &command.operation {
            PartitionOperation::SealForTransfer {
                operation,
                destination,
                next_epoch,
            } => {
                if *destination == state.delegation.partition
                    || *next_epoch
                        != state
                            .delegation
                            .epoch
                            .checked_add(1)
                            .ok_or(DirectoryError::CounterExhausted)?
                {
                    return Err(DirectoryError::StaleEpoch);
                }
                state.sealed = Some(PartitionSeal {
                    operation: *operation,
                    destination: *destination,
                    next_epoch: *next_epoch,
                    revision,
                    moved: state.delegation.namespace,
                    source: state.delegation.partition,
                });
            }
            PartitionOperation::SealForSplit {
                operation,
                destination,
                next_epoch,
                at,
            } => {
                let namespace = state.delegation.namespace;
                if *destination == state.delegation.partition
                    || *next_epoch
                        != state
                            .delegation
                            .epoch
                            .checked_add(1)
                            .ok_or(DirectoryError::CounterExhausted)?
                {
                    return Err(DirectoryError::StaleEpoch);
                }
                if *at == namespace.start || !namespace.contains_key(*at) {
                    return Err(DirectoryError::Invalid("split key"));
                }
                state.sealed = Some(PartitionSeal {
                    operation: *operation,
                    destination: *destination,
                    next_epoch: *next_epoch,
                    revision,
                    moved: NamespaceRange {
                        start: *at,
                        end: namespace.end,
                    },
                    source: state.delegation.partition,
                });
            }
            PartitionOperation::Release { delegation } => {
                let seal = state.sealed.clone().ok_or(DirectoryError::NotReady)?;
                let old = &state.delegation;
                if seal.moved == old.namespace {
                    // A whole transfer or merge source never serves again.
                    return Err(DirectoryError::WrongOperation);
                }
                let fence = delegation
                    .activation
                    .as_ref()
                    .ok_or(DirectoryError::UnverifiedAuthority)?;
                let kept = NamespaceRange {
                    start: old.namespace.start,
                    end: Some(seal.moved.start),
                };
                if delegation.partition != old.partition
                    || delegation.log_group != old.log_group
                    || delegation.epoch != seal.next_epoch
                    || delegation.namespace != kept
                    || fence.cluster != state.cluster
                    || fence.source != old.partition
                    || fence.destination != seal.destination
                    || fence.namespace != seal.moved
                    || fence.operation != seal.operation
                    || fence.from_epoch != old.epoch
                    || fence.to_epoch != seal.next_epoch
                    || fence.sealed_revision != seal.revision
                {
                    return Err(DirectoryError::StaleEpoch);
                }
                verifier.verify_delegation(fence)?;
                if fence.checkpoint != partition_checkpoint_digest(&split_image_of(&state, &seal)?)?
                {
                    return Err(DirectoryError::CompareFailed);
                }
                Arc::make_mut(&mut state.sessions)
                    .retain(|ledger, _| !seal.moved.contains(*ledger));
                state.delegation = *delegation;
                state.sealed = None;
            }
            PartitionOperation::Install { delegation } => {
                let fence = check_install(&state, delegation)?;
                verifier.verify_delegation(&fence)?;
                // This state is the image itself: the first command a
                // bootstrapped destination applies.
                if fence.checkpoint != partition_checkpoint_digest(&state)? {
                    return Err(DirectoryError::CompareFailed);
                }
                state.delegation = *delegation;
                state.sealed = None;
            }
            PartitionOperation::Absorb { delegation, moved } => {
                let old = &state.delegation;
                let fence = delegation
                    .activation
                    .as_ref()
                    .ok_or(DirectoryError::UnverifiedAuthority)?;
                let seal = moved.sealed.as_ref().ok_or(DirectoryError::NotReady)?;
                let next_epoch = old
                    .epoch
                    .checked_add(1)
                    .ok_or(DirectoryError::CounterExhausted)?;
                let union = NamespaceRange {
                    start: old.namespace.start,
                    end: moved.delegation.namespace.end,
                };
                if moved.schema != PARTITION_CHECKPOINT_SCHEMA
                    || moved.cluster != state.cluster
                    || moved.delegation.partition == old.partition
                    || old.namespace.end != Some(moved.delegation.namespace.start)
                    || seal.moved != moved.delegation.namespace
                    || seal.destination != old.partition
                    || seal.revision != moved.revision
                    || delegation.partition != old.partition
                    || delegation.log_group != old.log_group
                    || delegation.epoch != next_epoch
                    || delegation.namespace != union
                    || fence.cluster != state.cluster
                    || fence.source != moved.delegation.partition
                    || fence.destination != old.partition
                    || fence.namespace != seal.moved
                    || fence.operation != seal.operation
                    || fence.from_epoch != old.epoch
                    || fence.to_epoch != next_epoch
                    || fence.sealed_revision != seal.revision
                {
                    return Err(DirectoryError::StaleEpoch);
                }
                if moved.sessions.len() > self.config.max_absorb_sessions {
                    return Err(DirectoryError::Capacity);
                }
                verifier.verify_delegation(fence)?;
                if fence.checkpoint != partition_checkpoint_digest(moved)? {
                    return Err(DirectoryError::CompareFailed);
                }
                let groups: BTreeSet<LogGroupId> = state
                    .sessions
                    .values()
                    .map(|session| session.log_group)
                    .collect();
                for (ledger, session) in moved.sessions.iter() {
                    if state.sessions.contains_key(ledger) || groups.contains(&session.log_group) {
                        return Err(DirectoryError::Duplicate);
                    }
                }
                for (ledger, session) in moved.sessions.iter() {
                    Arc::make_mut(&mut state.sessions).insert(*ledger, session.clone());
                }
                // A node known to both keeps the record at the higher
                // generation; equal generations keep the destination's.
                for (id, record) in moved.nodes.iter() {
                    let replace = state.nodes.get(id).is_none_or(|mine| {
                        mine.enrollment.generation < record.enrollment.generation
                    });
                    if replace {
                        Arc::make_mut(&mut state.nodes).insert(*id, record.clone());
                    }
                }
                state.delegation = *delegation;
                state.sealed = None;
            }
            PartitionOperation::Enroll {
                node,
                expected_generation,
            } => {
                let previous = state
                    .nodes
                    .get(&node.node)
                    .map(|old| old.enrollment.generation);
                if previous != *expected_generation {
                    return Err(DirectoryError::CompareFailed);
                }
                // Any forward generation move is accepted, not only +1: the root
                // advances a node's generation once per drain/undrain and the
                // authority snapshot that feeds enrollment holds only the latest
                // grant, so an intermediate generation may never reach this
                // partition. The grant is self-contained and authenticated by
                // verify_enrollment below, and the compare-and-set above pins the
                // record this decision was made against, so skipping a generation
                // is safe. Requiring exactly +1 wedged the enrollment permanently.
                if node.generation <= previous.unwrap_or(0) {
                    return Err(DirectoryError::StaleNode);
                }
                if let Some(old) = state.nodes.get(&node.node)
                    && node.authority_epoch < old.enrollment.authority_epoch
                {
                    return Err(DirectoryError::UnverifiedAuthority);
                }
                verifier.verify_enrollment(node)?;
                Arc::make_mut(&mut state.nodes).insert(
                    node.node,
                    NodeRecord {
                        enrollment: node.clone(),
                        load: None,
                        liveness: None,
                    },
                );
            }
            PartitionOperation::ReportLoad { load } => {
                let node = Arc::make_mut(&mut state.nodes)
                    .get_mut(&load.node)
                    .ok_or(DirectoryError::Missing)?;
                if node.enrollment.generation != load.generation
                    || node.load.is_some_and(|old| old.report >= load.report)
                    || load.report == 0
                {
                    return Err(DirectoryError::StaleNode);
                }
                node.load = Some(*load);
            }
            PartitionOperation::Liveness {
                node,
                generation,
                alive,
                incarnation,
                witness,
                decided_at,
            } => {
                let record = Arc::make_mut(&mut state.nodes)
                    .get_mut(node)
                    .ok_or(DirectoryError::Missing)?;
                if record.enrollment.generation != *generation {
                    return Err(DirectoryError::StaleNode);
                }
                // A verdict never goes back: an older incarnation is stale,
                // and the same incarnation may only change the verdict.
                if let Some(current) = record.liveness {
                    if *incarnation < current.incarnation
                        || (*incarnation == current.incarnation && *alive == current.alive)
                        || *decided_at < current.decided_at
                    {
                        return Err(DirectoryError::StaleNode);
                    }
                } else if *alive {
                    return Err(DirectoryError::Duplicate);
                }
                record.liveness = Some(NodeLiveness {
                    alive: *alive,
                    incarnation: *incarnation,
                    witness: *witness,
                    decided_at: *decided_at,
                });
            }
            PartitionOperation::CreateSession {
                ledger,
                log_group,
                placement,
                authority,
            } => {
                if !state.delegation.namespace.contains(*ledger) {
                    return Err(DirectoryError::OutsideNamespace);
                }
                if state.sessions.contains_key(ledger)
                    || state
                        .sessions
                        .values()
                        .any(|session| session.log_group == *log_group)
                {
                    return Err(DirectoryError::Duplicate);
                }
                verify_placement(placement, &state.nodes, self.config.max_members)?;
                if authority.kind != SessionFenceKind::Created
                    || authority.ledger != *ledger
                    || authority.log_group != *log_group
                    || authority.from_route != RouteEpoch(0)
                    || authority.to_route != RouteEpoch(1)
                    || authority.membership_epoch != 1
                    || authority.placement_epoch != 1
                {
                    return Err(DirectoryError::StaleEpoch);
                }
                partition_session::validate_fence(authority, placement)?;
                verifier.verify_session_fence(authority)?;
                // A session a node founds alone records that node: its log's
                // bootstrap membership, which every later copy replays. A
                // session created with several voters at once names none.
                let founder = match placement.placement.voters.keys().collect::<Vec<_>>()[..] {
                    [node] => Some(*node),
                    _ => None,
                };
                Arc::make_mut(&mut state.sessions).insert(
                    *ledger,
                    SessionDescriptor {
                        ledger: *ledger,
                        log_group: *log_group,
                        revision: 1,
                        route_epoch: RouteEpoch(1),
                        membership_epoch: 1,
                        placement_epoch: 1,
                        active: placement.clone(),
                        authority: authority.clone(),
                        pending: None,
                        retiring: BTreeMap::new(),
                        refusals: Vec::new(),
                        founder,
                        holders: None,
                    },
                );
            }
            PartitionOperation::Session {
                ledger,
                expected_revision,
                change,
            } => {
                let session = Arc::make_mut(&mut state.sessions)
                    .get_mut(ledger)
                    .ok_or(DirectoryError::Missing)?;
                if session.revision != *expected_revision {
                    return Err(DirectoryError::CompareFailed);
                }
                partition_session::apply_session(
                    session,
                    change,
                    &state.nodes,
                    self.config,
                    verifier,
                )?;
                session.revision = session
                    .revision
                    .checked_add(1)
                    .ok_or(DirectoryError::CounterExhausted)?;
            }
        }
        for (ledger, before) in touched {
            let after = state.sessions.get(&ledger).map(|s| s.route_epoch);
            if let Some(route_epoch) = after
                && after != before
            {
                let routes = Arc::make_mut(&mut state.routes);
                if routes.len() >= self.config.max_route_log.max(1)
                    && let Some(evicted) = routes.pop_front()
                {
                    state.routes_from = state.routes_from.max(evicted.revision);
                    // An absorb writes several route changes at one revision, so
                    // the log can hold a contiguous same-revision block. Evict the
                    // whole block, not one entry: a revision must never be both the
                    // log floor (routes_from) and still present at the front, which
                    // would wedge validate_partition on the next route change.
                    while routes
                        .front()
                        .is_some_and(|front| front.revision == evicted.revision)
                    {
                        routes.pop_front();
                    }
                }
                routes.push_back(RouteChange {
                    revision,
                    ledger,
                    route_epoch,
                });
            }
        }
        state.revision = revision;
        validate_partition(&state, self.config)?;
        Ok(PreparedPartitionUpdate {
            owner: self.owner,
            base_revision: self.revision(),
            next: PartitionVersion {
                state,
                _allocation: allocation,
            },
        })
    }
    pub fn publish(&mut self, update: PreparedPartitionUpdate) -> Result<(), DirectoryError> {
        if self.owner != update.owner || self.revision() != update.base_revision {
            return Err(DirectoryError::StalePreparation);
        }
        self.root = update.next;
        Ok(())
    }
}

pub fn partition_checkpoint_digest(
    state: &PartitionCheckpoint,
) -> Result<focal_model::ContentHash, DirectoryError> {
    crate::digest(b"focal:directory-partition-checkpoint:v2\0", state)
}
/// The image a split's destination installs from a sealed source state.
pub fn split_image(state: &PartitionCheckpoint) -> Result<PartitionCheckpoint, DirectoryError> {
    let seal = state.sealed.as_ref().ok_or(DirectoryError::NotReady)?;
    split_image_of(state, seal)
}
fn split_image_of(
    state: &PartitionCheckpoint,
    seal: &PartitionSeal,
) -> Result<PartitionCheckpoint, DirectoryError> {
    // The image is the destination from the start: the split's derived group
    // and partition, the provisional delegation the root activates, the
    // source's seal.
    if seal.moved == state.delegation.namespace || seal.source != state.delegation.partition {
        return Err(DirectoryError::WrongOperation);
    }
    let mut image = state.clone();
    Arc::make_mut(&mut image.sessions).retain(|ledger, _| seal.moved.contains(*ledger));
    // The destination's own log starts empty and complete from its first
    // revision: a cache that watched the source re-reads through the root.
    image.routes = Arc::new(VecDeque::new());
    image.routes_from = 0;
    image.delegation = Delegation {
        namespace: seal.moved,
        partition: seal.destination,
        region: state.delegation.region,
        log_group: split_group_id(state.cluster, seal.operation),
        epoch: seal.next_epoch,
        activation: None,
    };
    Ok(image)
}
/// The fence under which a sealed state (a source's own, or an image shaped
/// as its destination) becomes `delegation`.
fn check_install(
    state: &PartitionCheckpoint,
    delegation: &Delegation,
) -> Result<DelegationFence, DirectoryError> {
    let fence = delegation
        .activation
        .ok_or(DirectoryError::UnverifiedAuthority)?;
    let seal = state.sealed.as_ref().ok_or(DirectoryError::NotReady)?;
    let own = &state.delegation;
    // A transfer image still carries the source's delegation at its epoch; a
    // split image already carries the destination's provisional one.
    let shaped = (own.partition == seal.source && own.epoch == fence.from_epoch)
        || (own.partition == seal.destination
            && own.log_group == delegation.log_group
            && own.namespace == seal.moved
            && own.epoch == seal.next_epoch
            && own.activation.is_none());
    if !shaped
        || fence.cluster != state.cluster
        || fence.source != seal.source
        || fence.destination != delegation.partition
        || seal.destination != delegation.partition
        || fence.namespace != seal.moved
        || delegation.namespace != seal.moved
        || fence.from_epoch.checked_add(1) != Some(fence.to_epoch)
        || fence.to_epoch != delegation.epoch
        || seal.next_epoch != delegation.epoch
        || fence.operation != seal.operation
        || seal.revision != state.revision
        || fence.sealed_revision != state.revision
    {
        return Err(DirectoryError::StaleEpoch);
    }
    Ok(fence)
}

fn validate_input(
    operation: &PartitionOperation,
    config: PartitionConfig,
) -> Result<(), DirectoryError> {
    match operation {
        PartitionOperation::Enroll { node, .. } => validate_enrollment(node, config)?,
        PartitionOperation::Release { delegation }
        | PartitionOperation::Install { delegation }
        | PartitionOperation::Absorb { delegation, .. } => {
            delegation.namespace.validate()?;
            if delegation.activation.is_none() {
                return Err(DirectoryError::UnverifiedAuthority);
            }
            if let PartitionOperation::Absorb { moved, .. } = operation {
                moved.delegation.namespace.validate()?;
                if moved.sealed.is_none() {
                    return Err(DirectoryError::NotReady);
                }
                if moved.nodes.len() > config.max_nodes
                    || moved.sessions.len() > config.max_absorb_sessions
                {
                    return Err(DirectoryError::Capacity);
                }
            }
        }
        PartitionOperation::Liveness {
            node,
            generation,
            witness,
            decided_at,
            ..
        } => {
            if *node == 0 || *generation == 0 || *witness == 0 || *decided_at <= 0 {
                return Err(DirectoryError::Invalid("node liveness"));
            }
        }
        PartitionOperation::CreateSession { placement, .. }
        | PartitionOperation::Session {
            change: SessionChange::Plan {
                desired: placement, ..
            },
            ..
        } => validate_spec_size(placement, config)?,
        _ => {}
    }
    Ok(())
}
fn validate_enrollment(
    node: &NodeEnrollment,
    config: PartitionConfig,
) -> Result<(), DirectoryError> {
    if node.node == 0
        || node.generation == 0
        || node.authority_epoch == 0
        || node.endpoint.is_empty()
        || node.endpoint.len() > config.max_endpoint_bytes
        || !types::nonzero_hash(node.identity)
        || !types::nonzero_hash(node.attestation)
    {
        return Err(DirectoryError::Invalid("node enrollment"));
    }
    Ok(())
}
fn validate_spec_size(spec: &PlacementSpec, config: PartitionConfig) -> Result<(), DirectoryError> {
    if spec.policy.home_regions.len() > config.max_policy_regions
        || spec.policy.residency.len() > config.max_policy_regions
        || spec.placement.voters.len() > config.max_members
        || spec.placement.materializers.len() > config.max_members
        || spec.placement.content_copies.len() > config.max_members
    {
        return Err(DirectoryError::Capacity);
    }
    Ok(())
}
fn validate_partition(
    state: &PartitionCheckpoint,
    config: PartitionConfig,
) -> Result<(), DirectoryError> {
    state.delegation.namespace.validate()?;
    // A sealed image shaped as its destination carries a provisional
    // delegation the root has not activated yet; everything else validates
    // as a committed delegation.
    let provisional = state.delegation.activation.is_none()
        && state
            .sealed
            .as_ref()
            .is_some_and(|seal| seal.destination == state.delegation.partition);
    if !provisional {
        control::validate_delegation(&state.delegation, state.cluster)?;
    }
    if state.schema != PARTITION_CHECKPOINT_SCHEMA
        || state.delegation.epoch == 0
        || config.max_nodes == 0
        || config.max_sessions == 0
        || config.max_members == 0
    {
        return Err(DirectoryError::Invalid("partition schema or limits"));
    }
    if state.nodes.len() > config.max_nodes
        || state.sessions.len() > config.max_sessions
        || state.routes.len() > config.max_route_log
    {
        return Err(DirectoryError::Capacity);
    }
    // Entries sit above the floor in revision order; several sessions may
    // change at one revision (an absorb moves many at once).
    if state.routes_from > state.revision {
        return Err(DirectoryError::Invalid("route log"));
    }
    let mut previous = state.routes_from;
    for (index, change) in state.routes.iter().enumerate() {
        if (index == 0 && change.revision <= previous)
            || change.revision < previous
            || change.revision > state.revision
            || change.route_epoch.0 == 0
        {
            return Err(DirectoryError::Invalid("route log"));
        }
        previous = change.revision;
    }
    if state.sealed.as_ref().is_some_and(|seal| {
        let own = &state.delegation;
        let source_shaped = seal.source == own.partition
            && seal.destination != own.partition
            && own.epoch.checked_add(1) == Some(seal.next_epoch)
            && seal.moved.end == own.namespace.end
            && own.namespace.contains_key(seal.moved.start);
        let destination_shaped = seal.destination == own.partition
            && seal.source != own.partition
            && own.epoch == seal.next_epoch
            && own.namespace == seal.moved
            && own.log_group == split_group_id(state.cluster, seal.operation);
        seal.revision != state.revision
            || seal.moved.validate().is_err()
            || !(source_shaped || destination_shaped)
    }) {
        return Err(DirectoryError::StaleEpoch);
    }
    for (id, node) in state.nodes.iter() {
        if *id != node.enrollment.node {
            return Err(DirectoryError::Invalid("node key"));
        }
        validate_enrollment(&node.enrollment, config)?;
        if node.load.is_some_and(|load| {
            load.node != *id || load.generation != node.enrollment.generation || load.report == 0
        }) {
            return Err(DirectoryError::StaleNode);
        }
        if node
            .liveness
            .is_some_and(|liveness| liveness.witness == 0 || liveness.decided_at <= 0)
        {
            return Err(DirectoryError::Invalid("node liveness"));
        }
    }
    let mut groups = BTreeSet::new();
    for (ledger, session) in state.sessions.iter() {
        if *ledger != session.ledger || !state.delegation.namespace.contains(*ledger) {
            return Err(DirectoryError::OutsideNamespace);
        }
        if !groups.insert(session.log_group) {
            return Err(DirectoryError::Duplicate);
        }
        if session.revision == 0
            || session.route_epoch.0 == 0
            || session.membership_epoch == 0
            || session.placement_epoch == 0
        {
            return Err(DirectoryError::StaleEpoch);
        }
        validate_spec_size(&session.active, config)?;
        partition_session::validate_fence(&session.authority, &session.active)?;
        if session.authority.ledger != *ledger
            || session.authority.log_group != session.log_group
            || session.authority.to_route != session.route_epoch
            || session.authority.membership_epoch != session.membership_epoch
            || session.authority.placement_epoch != session.placement_epoch
        {
            return Err(DirectoryError::StaleEpoch);
        }
        if !matches!(
            (session.authority.kind, session.route_epoch.0),
            (SessionFenceKind::Created, 1) | (SessionFenceKind::Activated, 2..)
        ) || session.authority.from_route.0.checked_add(1) != Some(session.authority.to_route.0)
        {
            return Err(DirectoryError::StaleEpoch);
        }
        if !session
            .active
            .placement
            .voters
            .contains_key(&session.active.placement.preferred_leader)
        {
            return Err(DirectoryError::Invalid("leader absent from voters"));
        }
        if session.refusals.len() > config.max_refusals {
            return Err(DirectoryError::Capacity);
        }
        for (node, copy) in &session.retiring {
            if *node != copy.node
                || copy.roles.is_empty()
                || copy.refusal.is_some()
                || session.active.placement.nodes().contains(node)
                || !matches!(
                    copy.phase,
                    AssignmentPhase::Active | AssignmentPhase::Draining
                )
            {
                return Err(DirectoryError::Phase);
            }
        }
        if let Some(plan) = &session.pending {
            validate_spec_size(&plan.desired, config)?;
            partition_session::validate_progress(plan)?;
            if plan.ready.len() > config.max_members.saturating_mul(3)
                || plan.next_route.0
                    != session
                        .route_epoch
                        .0
                        .checked_add(1)
                        .ok_or(DirectoryError::CounterExhausted)?
                || session.placement_epoch.checked_add(1) != Some(plan.next_placement)
                || session.membership_epoch.checked_add(u64::from(
                    plan.desired
                        .placement
                        .adds_voter_over(&session.active.placement),
                )) != Some(plan.next_membership)
                || plan.operation == session.authority.operation
            {
                return Err(DirectoryError::StaleEpoch);
            }
            if plan.phase == PlacementPhase::Planned
                && (!plan.ready.is_empty() || plan.barrier.is_some() || !plan.progress.is_empty())
            {
                return Err(DirectoryError::Phase);
            }
            if plan.phase != PlacementPhase::Planned
                && plan.progress.len() != plan.desired.placement.nodes().len()
            {
                return Err(DirectoryError::Phase);
            }
            if plan.phase != partition_progress::derive_phase(plan) {
                return Err(DirectoryError::Phase);
            }
            if plan
                .observations
                .keys()
                .any(|node| plan.desired.placement.generation(*node).is_none())
            {
                return Err(DirectoryError::Invalid("observation outside placement"));
            }
            for (node, ready) in &plan.ready {
                if *node != ready.node
                    || ready.ledger != *ledger
                    || ready.operation != plan.operation
                    || ready.route_epoch != plan.next_route
                    || plan.desired.placement.generation(*node) != Some(ready.node_generation)
                    || !types::nonzero_hash(ready.custody)
                    || !types::nonzero_hash(ready.attestation)
                {
                    return Err(DirectoryError::StaleEpoch);
                }
            }
            if let Some(barrier) = &plan.barrier {
                partition_session::validate_transition_fence(
                    session,
                    plan.operation,
                    barrier,
                    SessionFenceKind::Cutover,
                )?;
                if barrier.sequence < session.authority.sequence
                    || barrier.index <= session.authority.index
                    || barrier.term < session.authority.term
                {
                    return Err(DirectoryError::StaleEpoch);
                }
            }
        }
    }
    Ok(())
}
fn partition_charge(state: &PartitionCheckpoint) -> Result<usize, DirectoryError> {
    let mut bytes = add(
        add(size_of::<PartitionVersion>(), ALLOCATOR_OVERHEAD)?,
        mul(state.nodes.len(), tree_row::<(u64, NodeRecord)>())?,
    )?;
    bytes = add(
        bytes,
        mul(
            state.sessions.len(),
            tree_row::<(LedgerId, SessionDescriptor)>(),
        )?,
    )?;
    bytes = add(
        bytes,
        add(
            mul(state.routes.capacity(), size_of::<RouteChange>())?,
            ALLOCATOR_OVERHEAD,
        )?,
    )?;
    for node in state.nodes.values() {
        bytes = add(bytes, node.enrollment.endpoint.capacity())?;
    }
    for session in state.sessions.values() {
        bytes = add(bytes, placement::spec_charge(&session.active)?)?;
        bytes = add(
            bytes,
            add(
                mul(session.refusals.capacity(), size_of::<Refusal>())?,
                ALLOCATOR_OVERHEAD,
            )?,
        )?;
        for copy in session.retiring.values() {
            bytes = add(bytes, partition_progress::progress_charge(copy)?)?;
        }
        if let Some(plan) = &session.pending {
            bytes = add(
                bytes,
                add(
                    placement::spec_charge(&plan.desired)?,
                    add(
                        mul(plan.ready.len(), tree_row::<(u64, ReplicaReady)>())?,
                        mul(plan.observations.len(), tree_row::<(u64, u64)>())?,
                    )?,
                )?,
            )?;
            for progress in plan.progress.values() {
                bytes = add(bytes, partition_progress::progress_charge(progress)?)?;
            }
        }
    }
    Ok(bytes)
}
