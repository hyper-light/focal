//! The schema 1 partition checkpoint, kept so that control checkpoints written
//! before assignment progress existed still restore. A restored plan derives
//! its progress from the readiness it had recorded; nothing is invented beyond
//! what the schema 1 rules already implied.
use crate::*;
use focal_model::{LedgerId, RouteEpoch};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLoadV1 {
    pub node: u64,
    pub generation: u64,
    pub report: u64,
    pub available_memory: u64,
    pub active_weight: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecordV1 {
    pub enrollment: NodeEnrollment,
    pub load: Option<NodeLoadV1>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementPhaseV1 {
    Planned,
    Preparing,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingPlacementV1 {
    pub operation: OperationId,
    pub next_route: RouteEpoch,
    pub next_membership: u64,
    pub next_placement: u64,
    pub desired: PlacementSpec,
    pub phase: PlacementPhaseV1,
    pub ready: BTreeMap<u64, ReplicaReady>,
    pub barrier: Option<SessionFence>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDescriptorV1 {
    pub ledger: LedgerId,
    pub log_group: LogGroupId,
    pub revision: u64,
    pub route_epoch: RouteEpoch,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    pub active: PlacementSpec,
    pub authority: SessionFence,
    pub pending: Option<PendingPlacementV1>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCheckpointV1 {
    pub schema: u16,
    pub cluster: ClusterId,
    pub delegation: Delegation,
    pub revision: u64,
    pub sealed: Option<PartitionSealV3>,
    pub nodes: BTreeMap<u64, NodeRecordV1>,
    pub sessions: BTreeMap<LedgerId, SessionDescriptorV1>,
}

impl From<NodeLoadV1> for NodeLoad {
    fn from(value: NodeLoadV1) -> Self {
        Self {
            node: value.node,
            generation: value.generation,
            report: value.report,
            available_memory: value.available_memory,
            active_weight: value.active_weight,
            disk_available: 0,
            capability: 0,
        }
    }
}
impl TryFrom<PendingPlacementV1> for PendingPlacement {
    type Error = DirectoryError;
    fn try_from(value: PendingPlacementV1) -> Result<Self, DirectoryError> {
        let mut progress = BTreeMap::new();
        if value.phase == PlacementPhaseV1::Preparing {
            for (node, generation) in value
                .desired
                .placement
                .nodes()
                .into_iter()
                .filter_map(|node| Some((node, value.desired.placement.generation(node)?)))
            {
                let roles = roles_of(&value.desired.placement, node);
                let mut entry = AssignmentProgress::assigned(node, generation, roles);
                if let Some(ready) = value.ready.get(&node) {
                    entry.phase = AssignmentPhase::CustodyVerified;
                    entry.through = ready.through;
                    entry.custody_epoch = value.next_placement;
                    // Schema 1 accepted the cutover fence before readiness. The
                    // fence proves the log committed the next membership epoch,
                    // which is what a promoted voter reports; a voter that never
                    // reported readiness cannot be represented under that fence.
                    if value.barrier.is_some() && entry.roles.contains(&AssignmentRole::Voter) {
                        entry.phase = AssignmentPhase::Promoted;
                    }
                } else if value.barrier.is_some() && entry.roles.contains(&AssignmentRole::Voter) {
                    return Err(DirectoryError::Phase);
                }
                progress.insert(node, entry);
            }
        }
        let mut plan = Self {
            operation: value.operation,
            next_route: value.next_route,
            next_membership: value.next_membership,
            next_placement: value.next_placement,
            desired: value.desired,
            phase: match value.phase {
                PlacementPhaseV1::Planned => PlacementPhase::Planned,
                PlacementPhaseV1::Preparing => PlacementPhase::Preparing,
            },
            ready: value.ready,
            barrier: value.barrier,
            observations: BTreeMap::new(),
            progress,
        };
        plan.phase = partition_progress::derive_phase(&plan);
        Ok(plan)
    }
}
/// The session descriptor of schemas 2 to 5: no founding node recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDescriptorV5 {
    pub ledger: LedgerId,
    pub log_group: LogGroupId,
    pub revision: u64,
    pub route_epoch: RouteEpoch,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    pub active: PlacementSpec,
    pub authority: SessionFence,
    pub pending: Option<PendingPlacement>,
    pub retiring: BTreeMap<u64, AssignmentProgress>,
    pub refusals: Vec<Refusal>,
}
impl TryFrom<SessionDescriptorV1> for SessionDescriptorV5 {
    type Error = DirectoryError;
    fn try_from(value: SessionDescriptorV1) -> Result<Self, DirectoryError> {
        Ok(Self {
            ledger: value.ledger,
            log_group: value.log_group,
            revision: value.revision,
            route_epoch: value.route_epoch,
            membership_epoch: value.membership_epoch,
            placement_epoch: value.placement_epoch,
            active: value.active,
            authority: value.authority,
            pending: value.pending.map(PendingPlacement::try_from).transpose()?,
            retiring: BTreeMap::new(),
            refusals: Vec::new(),
        })
    }
}
/// Before schema 6 every session was founded by the cluster founder; the
/// descriptor records no node, and hosts fall back to the genesis founder.
impl From<SessionDescriptorV5> for SessionDescriptor {
    fn from(value: SessionDescriptorV5) -> Self {
        Self {
            ledger: value.ledger,
            log_group: value.log_group,
            revision: value.revision,
            route_epoch: value.route_epoch,
            membership_epoch: value.membership_epoch,
            placement_epoch: value.placement_epoch,
            active: value.active,
            authority: value.authority,
            pending: value.pending,
            retiring: value.retiring,
            refusals: value.refusals,
            founder: None,
            holders: None,
        }
    }
}
/// The session descriptor of schema 6: a founding node, but no published
/// range holders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDescriptorV6 {
    pub ledger: LedgerId,
    pub log_group: LogGroupId,
    pub revision: u64,
    pub route_epoch: RouteEpoch,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    pub active: PlacementSpec,
    pub authority: SessionFence,
    pub pending: Option<PendingPlacement>,
    pub retiring: BTreeMap<u64, AssignmentProgress>,
    pub refusals: Vec<Refusal>,
    pub founder: Option<u64>,
}
/// Before schema 7 no session published its range holders; every member
/// was held by the voters, which `None` still means.
impl From<SessionDescriptorV6> for SessionDescriptor {
    fn from(value: SessionDescriptorV6) -> Self {
        Self {
            ledger: value.ledger,
            log_group: value.log_group,
            revision: value.revision,
            route_epoch: value.route_epoch,
            membership_epoch: value.membership_epoch,
            placement_epoch: value.placement_epoch,
            active: value.active,
            authority: value.authority,
            pending: value.pending,
            retiring: value.retiring,
            refusals: value.refusals,
            founder: value.founder,
            holders: None,
        }
    }
}
/// The descriptor as schema 6 recorded it (the holders dropped), for
/// encoding a checkpoint at that schema.
impl From<SessionDescriptor> for SessionDescriptorV6 {
    fn from(value: SessionDescriptor) -> Self {
        Self {
            ledger: value.ledger,
            log_group: value.log_group,
            revision: value.revision,
            route_epoch: value.route_epoch,
            membership_epoch: value.membership_epoch,
            placement_epoch: value.placement_epoch,
            active: value.active,
            authority: value.authority,
            pending: value.pending,
            retiring: value.retiring,
            refusals: value.refusals,
            founder: value.founder,
        }
    }
}
/// The descriptor as schemas 2 to 5 recorded it (the founding node dropped),
/// for encoding a checkpoint at those schemas.
impl From<SessionDescriptor> for SessionDescriptorV5 {
    fn from(value: SessionDescriptor) -> Self {
        Self {
            ledger: value.ledger,
            log_group: value.log_group,
            revision: value.revision,
            route_epoch: value.route_epoch,
            membership_epoch: value.membership_epoch,
            placement_epoch: value.placement_epoch,
            active: value.active,
            authority: value.authority,
            pending: value.pending,
            retiring: value.retiring,
            refusals: value.refusals,
        }
    }
}
fn current_sessions(
    sessions: BTreeMap<LedgerId, SessionDescriptorV5>,
) -> BTreeMap<LedgerId, SessionDescriptor> {
    sessions
        .into_iter()
        .map(|(ledger, session)| (ledger, SessionDescriptor::from(session)))
        .collect()
}
/// The seal of schemas 1–3: a whole-namespace transfer, so the moved range
/// is the delegation's namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSealV3 {
    pub operation: OperationId,
    pub destination: PartitionId,
    pub next_epoch: u64,
    pub revision: u64,
}
impl PartitionSealV3 {
    pub fn into_current(self, namespace: NamespaceRange, source: PartitionId) -> PartitionSeal {
        PartitionSeal {
            operation: self.operation,
            destination: self.destination,
            next_epoch: self.next_epoch,
            revision: self.revision,
            moved: namespace,
            source,
        }
    }
}
/// The schema 2 node record: no liveness verdict yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecordV2 {
    pub enrollment: NodeEnrollment,
    pub load: Option<NodeLoad>,
}
/// The schema 2 partition checkpoint: assignment progress, but no liveness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCheckpointV2 {
    pub schema: u16,
    pub cluster: ClusterId,
    pub delegation: Delegation,
    pub revision: u64,
    pub sealed: Option<PartitionSealV3>,
    pub nodes: BTreeMap<u64, NodeRecordV2>,
    pub sessions: BTreeMap<LedgerId, SessionDescriptorV5>,
}
impl TryFrom<PartitionCheckpointV1> for PartitionCheckpointV2 {
    type Error = DirectoryError;
    fn try_from(value: PartitionCheckpointV1) -> Result<Self, DirectoryError> {
        Ok(Self {
            schema: 2,
            cluster: value.cluster,
            delegation: value.delegation,
            revision: value.revision,
            sealed: value.sealed,
            nodes: value
                .nodes
                .into_iter()
                .map(|(id, node)| {
                    (
                        id,
                        NodeRecordV2 {
                            enrollment: node.enrollment,
                            load: node.load.map(NodeLoad::from),
                        },
                    )
                })
                .collect(),
            sessions: value
                .sessions
                .into_iter()
                .map(|(ledger, session)| Ok((ledger, SessionDescriptorV5::try_from(session)?)))
                .collect::<Result<_, DirectoryError>>()?,
        })
    }
}
/// A schema 2 checkpoint knows no liveness verdict: every node restores as
/// alive until the detector commits one.
impl From<PartitionCheckpointV2> for PartitionCheckpointV4 {
    fn from(value: PartitionCheckpointV2) -> Self {
        let namespace = value.delegation.namespace;
        let source = value.delegation.partition;
        Self {
            schema: 4,
            cluster: value.cluster,
            delegation: value.delegation,
            revision: value.revision,
            sealed: value
                .sealed
                .map(|seal| seal.into_current(namespace, source)),
            nodes: value
                .nodes
                .into_iter()
                .map(|(id, node)| {
                    (
                        id,
                        NodeRecord {
                            enrollment: node.enrollment,
                            load: node.load,
                            liveness: None,
                        },
                    )
                })
                .collect(),
            sessions: value.sessions,
        }
    }
}
impl From<PartitionCheckpointV2> for PartitionCheckpoint {
    fn from(value: PartitionCheckpointV2) -> Self {
        PartitionCheckpointV4::from(value).into()
    }
}
/// The schema 3 partition checkpoint: liveness verdicts, but a seal that
/// can only move the whole namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCheckpointV3 {
    pub schema: u16,
    pub cluster: ClusterId,
    pub delegation: Delegation,
    pub revision: u64,
    pub sealed: Option<PartitionSealV3>,
    pub nodes: BTreeMap<u64, NodeRecord>,
    pub sessions: BTreeMap<LedgerId, SessionDescriptorV5>,
}
impl From<PartitionCheckpointV3> for PartitionCheckpointV4 {
    fn from(value: PartitionCheckpointV3) -> Self {
        let namespace = value.delegation.namespace;
        let source = value.delegation.partition;
        Self {
            schema: 4,
            cluster: value.cluster,
            delegation: value.delegation,
            revision: value.revision,
            sealed: value
                .sealed
                .map(|seal| seal.into_current(namespace, source)),
            nodes: value.nodes,
            sessions: value.sessions,
        }
    }
}
impl From<PartitionCheckpointV3> for PartitionCheckpoint {
    fn from(value: PartitionCheckpointV3) -> Self {
        PartitionCheckpointV4::from(value).into()
    }
}
/// The schema 4 partition checkpoint: seals that move part of a namespace,
/// but no route-change log yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCheckpointV4 {
    pub schema: u16,
    pub cluster: ClusterId,
    pub delegation: Delegation,
    pub revision: u64,
    pub sealed: Option<PartitionSeal>,
    pub nodes: BTreeMap<u64, NodeRecord>,
    pub sessions: BTreeMap<LedgerId, SessionDescriptorV5>,
}
/// A schema 4 checkpoint kept no route log: a cache watching it starts at
/// the current revision, and anything older reads as a gap.
impl From<PartitionCheckpointV4> for PartitionCheckpoint {
    fn from(value: PartitionCheckpointV4) -> Self {
        Self {
            schema: PARTITION_CHECKPOINT_SCHEMA,
            cluster: value.cluster,
            delegation: value.delegation,
            revision: value.revision,
            sealed: value.sealed,
            nodes: value.nodes,
            sessions: current_sessions(value.sessions),
            routes: std::collections::VecDeque::new(),
            routes_from: value.revision,
        }
    }
}
/// The schema 5 partition checkpoint: a route log, but no founding node per
/// session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCheckpointV5 {
    pub schema: u16,
    pub cluster: ClusterId,
    pub delegation: Delegation,
    pub revision: u64,
    pub sealed: Option<PartitionSeal>,
    pub nodes: BTreeMap<u64, NodeRecord>,
    pub sessions: BTreeMap<LedgerId, SessionDescriptorV5>,
    pub routes: std::collections::VecDeque<crate::partition::RouteChange>,
    pub routes_from: u64,
}
impl From<PartitionCheckpointV5> for PartitionCheckpoint {
    fn from(value: PartitionCheckpointV5) -> Self {
        Self {
            schema: PARTITION_CHECKPOINT_SCHEMA,
            cluster: value.cluster,
            delegation: value.delegation,
            revision: value.revision,
            sealed: value.sealed,
            nodes: value.nodes,
            sessions: current_sessions(value.sessions),
            routes: value.routes,
            routes_from: value.routes_from,
        }
    }
}
/// The schema 6 partition checkpoint: founding nodes, but no published range
/// holders per session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCheckpointV6 {
    pub schema: u16,
    pub cluster: ClusterId,
    pub delegation: Delegation,
    pub revision: u64,
    pub sealed: Option<PartitionSeal>,
    pub nodes: BTreeMap<u64, NodeRecord>,
    pub sessions: BTreeMap<LedgerId, SessionDescriptorV6>,
    pub routes: std::collections::VecDeque<crate::partition::RouteChange>,
    pub routes_from: u64,
}
impl From<PartitionCheckpointV6> for PartitionCheckpoint {
    fn from(value: PartitionCheckpointV6) -> Self {
        Self {
            schema: PARTITION_CHECKPOINT_SCHEMA,
            cluster: value.cluster,
            delegation: value.delegation,
            revision: value.revision,
            sealed: value.sealed,
            nodes: value.nodes,
            sessions: value
                .sessions
                .into_iter()
                .map(|(ledger, session)| (ledger, SessionDescriptor::from(session)))
                .collect(),
            routes: value.routes,
            routes_from: value.routes_from,
        }
    }
}
impl TryFrom<PartitionCheckpointV1> for PartitionCheckpoint {
    type Error = DirectoryError;
    fn try_from(value: PartitionCheckpointV1) -> Result<Self, DirectoryError> {
        Ok(PartitionCheckpointV2::try_from(value)?.into())
    }
}
impl PartitionCheckpoint {
    /// Decode a checkpoint at any schema. Schemas 1 to 6 convert as above; the
    /// result still passes every current validation before it is installed.
    pub fn decode_any(bytes: &[u8]) -> Result<Self, DirectoryError> {
        let (schema, _) = postcard::take_from_bytes::<u16>(bytes)
            .map_err(|_| DirectoryError::Invalid("partition checkpoint schema"))?;
        let (checkpoint, rest) = match schema {
            1 => {
                let (value, rest) = postcard::take_from_bytes::<PartitionCheckpointV1>(bytes)
                    .map_err(|_| DirectoryError::Invalid("partition checkpoint v1"))?;
                (Self::try_from(value)?, rest)
            }
            2 => {
                let (value, rest) = postcard::take_from_bytes::<PartitionCheckpointV2>(bytes)
                    .map_err(|_| DirectoryError::Invalid("partition checkpoint v2"))?;
                (Self::from(value), rest)
            }
            3 => {
                let (value, rest) = postcard::take_from_bytes::<PartitionCheckpointV3>(bytes)
                    .map_err(|_| DirectoryError::Invalid("partition checkpoint v3"))?;
                (Self::from(value), rest)
            }
            4 => {
                let (value, rest) = postcard::take_from_bytes::<PartitionCheckpointV4>(bytes)
                    .map_err(|_| DirectoryError::Invalid("partition checkpoint v4"))?;
                (Self::from(value), rest)
            }
            5 => {
                let (value, rest) = postcard::take_from_bytes::<PartitionCheckpointV5>(bytes)
                    .map_err(|_| DirectoryError::Invalid("partition checkpoint v5"))?;
                (Self::from(value), rest)
            }
            6 => {
                let (value, rest) = postcard::take_from_bytes::<PartitionCheckpointV6>(bytes)
                    .map_err(|_| DirectoryError::Invalid("partition checkpoint v6"))?;
                (Self::from(value), rest)
            }
            7 => postcard::take_from_bytes::<Self>(bytes)
                .map_err(|_| DirectoryError::Invalid("partition checkpoint v7"))?,
            _ => return Err(DirectoryError::Invalid("partition checkpoint schema")),
        };
        if !rest.is_empty() {
            return Err(DirectoryError::Invalid(
                "partition checkpoint trailing bytes",
            ));
        }
        Ok(checkpoint)
    }
}
