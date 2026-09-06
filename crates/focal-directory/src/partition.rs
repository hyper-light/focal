use crate::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{LedgerId, RouteEpoch};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementPhase {
    Planned,
    Preparing,
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
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCheckpoint {
    pub schema: u16,
    pub cluster: ClusterId,
    pub delegation: Delegation,
    pub revision: u64,
    pub sealed: Option<PartitionSeal>,
    /// Only relevant enrolled nodes, bounded independently of total fleet size.
    pub nodes: BTreeMap<u64, NodeRecord>,
    /// Only this delegated interval's sessions, never every fleet session.
    pub sessions: BTreeMap<LedgerId, SessionDescriptor>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSeal {
    pub operation: OperationId,
    pub destination: PartitionId,
    pub next_epoch: u64,
    pub revision: u64,
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
    Enroll {
        node: NodeEnrollment,
        expected_generation: Option<u64>,
    },
    ReportLoad {
        load: NodeLoad,
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
}
#[derive(Debug, Clone, Copy)]
pub struct PartitionConfig {
    pub max_nodes: usize,
    pub max_sessions: usize,
    pub max_members: usize,
    pub max_policy_regions: usize,
    pub max_endpoint_bytes: usize,
}
impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            max_nodes: 1024,
            max_sessions: 4096,
            max_members: 31,
            max_policy_regions: 64,
            max_endpoint_bytes: 512,
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
                schema: 1,
                cluster,
                delegation,
                revision: 0,
                sealed: None,
                nodes: BTreeMap::new(),
                sessions: BTreeMap::new(),
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
        let fence = delegation
            .activation
            .as_ref()
            .ok_or(DirectoryError::UnverifiedAuthority)?;
        let seal = state.sealed.as_ref().ok_or(DirectoryError::NotReady)?;
        if fence.cluster != state.cluster
            || fence.source != state.delegation.partition
            || fence.destination != delegation.partition
            || seal.destination != delegation.partition
            || fence.namespace != state.delegation.namespace
            || delegation.namespace != state.delegation.namespace
            || fence.from_epoch != state.delegation.epoch
            || fence.to_epoch != delegation.epoch
            || seal.next_epoch != delegation.epoch
            || fence.operation != seal.operation
            || seal.revision != state.revision
            || fence.sealed_revision != state.revision
        {
            return Err(DirectoryError::StaleEpoch);
        }
        verifier.verify_delegation(fence)?;
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
    pub fn prepare(
        &self,
        command: &PartitionCommand,
        verifier: &impl AuthorityVerifier,
    ) -> Result<PreparedPartitionUpdate, DirectoryError> {
        if self.root.state.sealed.is_some() {
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
            PartitionOperation::Session {
                change: SessionChange::Plan { desired, .. },
                ..
            } => placement::spec_charge(desired)?,
            PartitionOperation::Session {
                change: SessionChange::Ready { .. },
                ..
            } => tree_row::<(u64, ReplicaReady)>(),
            _ => 0,
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
                });
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
                if node.generation
                    != previous
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or(DirectoryError::CounterExhausted)?
                {
                    return Err(DirectoryError::StaleNode);
                }
                if let Some(old) = state.nodes.get(&node.node)
                    && node.authority_epoch < old.enrollment.authority_epoch
                {
                    return Err(DirectoryError::UnverifiedAuthority);
                }
                verifier.verify_enrollment(node)?;
                state.nodes.insert(
                    node.node,
                    NodeRecord {
                        enrollment: node.clone(),
                        load: None,
                    },
                );
            }
            PartitionOperation::ReportLoad { load } => {
                let node = state
                    .nodes
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
                validate_fence(authority, placement)?;
                verifier.verify_session_fence(authority)?;
                state.sessions.insert(
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
                    },
                );
            }
            PartitionOperation::Session {
                ledger,
                expected_revision,
                change,
            } => {
                let session = state
                    .sessions
                    .get_mut(ledger)
                    .ok_or(DirectoryError::Missing)?;
                if session.revision != *expected_revision {
                    return Err(DirectoryError::CompareFailed);
                }
                apply_session(session, change, &state.nodes, self.config, verifier)?;
                session.revision = session
                    .revision
                    .checked_add(1)
                    .ok_or(DirectoryError::CounterExhausted)?;
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
    crate::digest(b"focal:directory-partition-checkpoint:v1\0", state)
}

fn apply_session(
    session: &mut SessionDescriptor,
    change: &SessionChange,
    nodes: &BTreeMap<u64, NodeRecord>,
    config: PartitionConfig,
    verifier: &impl AuthorityVerifier,
) -> Result<(), DirectoryError> {
    match change {
        SessionChange::Plan { operation, desired } => {
            if let Some(pending) = &session.pending {
                return if pending.operation == *operation && pending.desired == *desired {
                    Ok(())
                } else {
                    Err(DirectoryError::Phase)
                };
            }
            if session.authority.operation == *operation {
                return Err(DirectoryError::WrongOperation);
            }
            verify_placement(desired, nodes, config.max_members)?;
            session.pending = Some(PendingPlacement {
                operation: *operation,
                next_route: RouteEpoch(
                    session
                        .route_epoch
                        .0
                        .checked_add(1)
                        .ok_or(DirectoryError::CounterExhausted)?,
                ),
                next_membership: session
                    .membership_epoch
                    .checked_add(u64::from(
                        desired.placement.voters != session.active.placement.voters,
                    ))
                    .ok_or(DirectoryError::CounterExhausted)?,
                next_placement: session
                    .placement_epoch
                    .checked_add(1)
                    .ok_or(DirectoryError::CounterExhausted)?,
                desired: desired.clone(),
                phase: PlacementPhase::Planned,
                ready: BTreeMap::new(),
                barrier: None,
            });
        }
        SessionChange::BeginPreparation { operation } => {
            pending(session, *operation)?.phase = PlacementPhase::Preparing;
        }
        SessionChange::Ready { ready } => {
            if ready.ledger != session.ledger {
                return Err(DirectoryError::OutsideNamespace);
            }
            let plan = pending(session, ready.operation)?;
            if plan.phase != PlacementPhase::Preparing {
                return Err(DirectoryError::Phase);
            }
            if ready.route_epoch != plan.next_route
                || plan.desired.placement.generation(ready.node) != Some(ready.node_generation)
            {
                return Err(DirectoryError::StaleEpoch);
            }
            if nodes
                .get(&ready.node)
                .is_none_or(|node| node.enrollment.generation != ready.node_generation)
            {
                return Err(DirectoryError::StaleNode);
            }
            if plan
                .ready
                .get(&ready.node)
                .is_some_and(|old| old.through > ready.through)
            {
                return Err(DirectoryError::StaleEpoch);
            }
            if !types::nonzero_hash(ready.attestation) || !types::nonzero_hash(ready.custody) {
                return Err(DirectoryError::Custody);
            }
            verifier.verify_replica_ready(ready)?;
            plan.ready.insert(ready.node, ready.clone());
        }
        SessionChange::Cutover {
            operation,
            authority,
        } => {
            validate_transition_fence(session, *operation, authority, SessionFenceKind::Cutover)?;
            if authority.sequence <= session.authority.sequence
                || authority.index <= session.authority.index
                || authority.term < session.authority.term
            {
                return Err(DirectoryError::StaleEpoch);
            }
            verifier.verify_session_fence(authority)?;
            let plan = pending(session, *operation)?;
            if plan.phase != PlacementPhase::Preparing {
                return Err(DirectoryError::Phase);
            }
            if plan
                .barrier
                .as_ref()
                .is_some_and(|previous| previous != authority)
            {
                return Err(DirectoryError::CompareFailed);
            }
            plan.barrier = Some(authority.clone());
        }
        SessionChange::Activate {
            operation,
            authority,
        } => {
            if session.pending.is_none() && session.authority == *authority {
                return Ok(());
            }
            validate_transition_fence(session, *operation, authority, SessionFenceKind::Activated)?;
            let plan = session.pending.as_ref().ok_or(DirectoryError::Phase)?;
            let barrier = plan.barrier.as_ref().ok_or(DirectoryError::NotReady)?;
            if authority.sequence <= barrier.sequence
                || authority.index <= barrier.index
                || authority.term < barrier.term
            {
                return Err(DirectoryError::StaleEpoch);
            }
            verify_placement(&plan.desired, nodes, config.max_members)?;
            for node in plan.desired.placement.nodes() {
                let ready = plan.ready.get(&node).ok_or(DirectoryError::NotReady)?;
                if ready.through < barrier.sequence {
                    return Err(DirectoryError::NotReady);
                }
            }
            verifier.verify_session_fence(authority)?;
            let plan = session.pending.take().ok_or(DirectoryError::Phase)?;
            session.route_epoch = plan.next_route;
            session.membership_epoch = plan.next_membership;
            session.placement_epoch = plan.next_placement;
            session.active = plan.desired;
            session.authority = authority.clone();
        }
        SessionChange::Abort { operation } => {
            if pending(session, *operation)?.barrier.is_some() {
                return Err(DirectoryError::Phase);
            }
            session.pending = None;
        }
    }
    Ok(())
}
fn pending(
    session: &mut SessionDescriptor,
    operation: OperationId,
) -> Result<&mut PendingPlacement, DirectoryError> {
    let plan = session.pending.as_mut().ok_or(DirectoryError::Phase)?;
    if plan.operation != operation {
        return Err(DirectoryError::WrongOperation);
    }
    Ok(plan)
}
fn validate_transition_fence(
    session: &SessionDescriptor,
    operation: OperationId,
    fence: &SessionFence,
    kind: SessionFenceKind,
) -> Result<(), DirectoryError> {
    let pending = session.pending.as_ref().ok_or(DirectoryError::Phase)?;
    if pending.operation != operation || fence.operation != operation {
        return Err(DirectoryError::WrongOperation);
    }
    if fence.kind != kind
        || fence.ledger != session.ledger
        || fence.log_group != session.log_group
        || fence.from_route != session.route_epoch
        || fence.to_route != pending.next_route
        || fence.membership_epoch != pending.next_membership
        || fence.placement_epoch != pending.next_placement
    {
        return Err(DirectoryError::StaleEpoch);
    }
    validate_fence(fence, &pending.desired)
}
fn validate_fence(fence: &SessionFence, spec: &PlacementSpec) -> Result<(), DirectoryError> {
    if fence.sequence.0 == 0
        || fence.index.0 == 0
        || fence.term.0 == 0
        || !types::nonzero_hash(fence.record_hash)
    {
        return Err(DirectoryError::UnverifiedAuthority);
    }
    if fence.placement_digest != placement_digest(spec)? {
        return Err(DirectoryError::CompareFailed);
    }
    Ok(())
}
fn validate_input(
    operation: &PartitionOperation,
    config: PartitionConfig,
) -> Result<(), DirectoryError> {
    match operation {
        PartitionOperation::Enroll { node, .. } => validate_enrollment(node, config)?,
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
    control::validate_delegation(&state.delegation, state.cluster)?;
    if state.schema != 1
        || state.delegation.epoch == 0
        || config.max_nodes == 0
        || config.max_sessions == 0
        || config.max_members == 0
    {
        return Err(DirectoryError::Invalid("partition schema or limits"));
    }
    if state.nodes.len() > config.max_nodes || state.sessions.len() > config.max_sessions {
        return Err(DirectoryError::Capacity);
    }
    if state.sealed.as_ref().is_some_and(|seal| {
        seal.revision != state.revision
            || seal.destination == state.delegation.partition
            || state.delegation.epoch.checked_add(1) != Some(seal.next_epoch)
    }) {
        return Err(DirectoryError::StaleEpoch);
    }
    for (id, node) in &state.nodes {
        if *id != node.enrollment.node {
            return Err(DirectoryError::Invalid("node key"));
        }
        validate_enrollment(&node.enrollment, config)?;
        if node.load.is_some_and(|load| {
            load.node != *id || load.generation != node.enrollment.generation || load.report == 0
        }) {
            return Err(DirectoryError::StaleNode);
        }
    }
    let mut groups = BTreeSet::new();
    for (ledger, session) in &state.sessions {
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
        validate_fence(&session.authority, &session.active)?;
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
        if let Some(plan) = &session.pending {
            validate_spec_size(&plan.desired, config)?;
            if plan.ready.len() > config.max_members.saturating_mul(3)
                || plan.next_route.0
                    != session
                        .route_epoch
                        .0
                        .checked_add(1)
                        .ok_or(DirectoryError::CounterExhausted)?
                || session.placement_epoch.checked_add(1) != Some(plan.next_placement)
                || session.membership_epoch.checked_add(u64::from(
                    plan.desired.placement.voters != session.active.placement.voters,
                )) != Some(plan.next_membership)
                || plan.operation == session.authority.operation
            {
                return Err(DirectoryError::StaleEpoch);
            }
            if plan.phase == PlacementPhase::Planned
                && (!plan.ready.is_empty() || plan.barrier.is_some())
            {
                return Err(DirectoryError::Phase);
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
                validate_transition_fence(
                    session,
                    plan.operation,
                    barrier,
                    SessionFenceKind::Cutover,
                )?;
                if barrier.sequence <= session.authority.sequence
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
    for node in state.nodes.values() {
        bytes = add(bytes, node.enrollment.endpoint.capacity())?;
    }
    for session in state.sessions.values() {
        bytes = add(bytes, placement::spec_charge(&session.active)?)?;
        if let Some(plan) = &session.pending {
            bytes = add(
                bytes,
                add(
                    placement::spec_charge(&plan.desired)?,
                    mul(plan.ready.len(), tree_row::<(u64, ReplicaReady)>())?,
                )?,
            )?;
        }
    }
    Ok(bytes)
}
