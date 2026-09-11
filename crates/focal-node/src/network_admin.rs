//! Physical-owner administration over kernel-authenticated Unix ingress.
//! Signing remains founder-only; reads and mutations use existing bounded owners.
use crate::{
    cluster::InviteIntent,
    embedded::NodeIdentity,
    network_join::NodeInvitation,
    network_state::{NetworkState, root_namespace},
    node_directory::NodeDirectory,
    quorum_enrollment::{QuorumEnrollmentError, QuorumEnrollmentHost},
};
use focal_control::{
    ControlCommand, ControlFailure, ControlIdentity, ControlRead, ControlReply, ControlRequest,
    ControlScope, ControlTransfer,
};
use focal_enrollment::{EnrollmentError, EnrollmentRole};
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{RequestEpoch, RequestId, RouteEpoch};
use focal_wire::*;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

pub const ADMIN_SOCKET: &str = "focal-admin.sock";
const MAGIC: &[u8] = b"FCLADMIN1";
const MAX_COMMAND: usize = 60 * 1024;
/// The lease a repair's export holds; a walk that outlives it resumes.
const REPAIR_TTL: std::time::Duration = std::time::Duration::from_secs(30);
const WORKSPACE: usize = 2 * 1024 * 1024;
#[path = "replica_admin_protocol.rs"]
mod replicas;
pub use replicas::{ReplicaAdminCommand, ReplicaAdminReply, ReplicaAdminStatus};
#[path = "operator_admin.rs"]
pub(crate) mod operator;
pub use operator::OperatorRead;

pub fn admin_wire_limits() -> WireLimits {
    WireLimits {
        max_frame_bytes: 64 * 1024,
        max_cost: 1024 * 1024,
        max_items: 1,
        max_connections: 8,
        streams_per_connection: 1,
        request_timeout: Duration::from_secs(20),
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub enum AdminCommand {
    Invite {
        name: String,
    },
    Read(AdminRead),
    Membership(Box<ControlRequest>),
    Transfer(ControlTransfer),
    Revocation(Box<ControlRequest>),
    /// A node's eligibility grant prepared by [`AdminRead::PrepareEligibility`]
    /// (24 §19): the only authority operation the administrator commits.
    Authority(Box<ControlRequest>),
    InviteClient {
        name: String,
    },
    Replica(Box<ReplicaAdminCommand>),
    Operator(OperatorRead),
    /// Renew this node's own credential now.
    RenewCredential,
    /// Rotate this node's own credential to a fresh key now (24 §11).
    RotateCredential,
    /// The placement view and the controller's next actions.
    Placement,
    /// Admit a tenant the cluster serves; founder only, exact on retry
    /// ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §16).
    AdmitTenant {
        tenant: [u8; 16],
    },
    /// The tenants the cluster serves.
    Tenants,
    /// Create an application session on this node for a served tenant, or
    /// find the one the same name already denotes.
    CreateSession {
        tenant: [u8; 16],
        name: String,
    },
    /// Plan a session's placement under a requested durability
    /// (`survive`: 0 node, 1 zone, 2 region).
    PlanSession {
        tenant: [u8; 16],
        session: [u8; 16],
        survive: u8,
        max_failures: u16,
        /// Propose and report without journaling a plan.
        dry_run: bool,
    },
    /// Move one member of a session's range group to a node (25 §6).
    MoveRange {
        tenant: [u8; 16],
        session: [u8; 16],
        member: [u8; 16],
        node: u64,
    },
    /// Bring a quarantined content object back (26 §5).
    GcRestore {
        domain: [u8; 16],
        root: [u8; 32],
    },
    /// Write a backup of a hosted session at its committed prefix (26 §6).
    BackupCreate {
        tenant: [u8; 16],
        session: [u8; 16],
        output: String,
    },
    /// Restore a session from a verified backup onto this node (26 §6).
    Restore {
        input: String,
        new_incarnation: bool,
    },
    /// Repair a hosted session's custody on this node (24 §20): re-verify
    /// every object its committed prefix names, recopy what is missing from
    /// another required copy, complete the other required copies.
    Repair {
        tenant: [u8; 16],
        session: [u8; 16],
        /// Resume the walk after this artifact.
        after: Option<[u8; 16]>,
        /// The objects one call examines at most.
        limit: u32,
    },
    /// The committed upgrade fence, this binary's level and every enrolled
    /// node's reported level (24 §21).
    UpgradeStatus,
    /// Raise the upgrade fence to `level`; founder only, exact on retry.
    ActivateFence {
        level: u32,
    },
}
/// The reply to [`AdminCommand::UpgradeStatus`] and
/// [`AdminCommand::ActivateFence`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeReply {
    pub schema: u16,
    pub applied_index: u64,
    pub revision: u64,
    pub fence: focal_enrollment::UpgradeFence,
    pub binary: u32,
    pub announced: u32,
    /// Every node the directory lists, with the capability it last
    /// reported (zero: none reported).
    pub nodes: Vec<(u64, u32)>,
    pub changed: bool,
}
pub const UPGRADE_REPLY_SCHEMA: u16 = 1;
/// The reply to [`AdminCommand::Repair`].
#[derive(Debug, Serialize, Deserialize)]
pub struct RepairedReply {
    pub schema: u16,
    pub repair: focal_client::admin::AdminRepair,
}
pub const REPAIRED_REPLY_SCHEMA: u16 = 1;
/// The reply to [`AdminCommand::Restore`].
#[derive(Debug, Serialize, Deserialize)]
pub struct RestoredReply {
    pub schema: u16,
    pub restored: focal_client::admin::AdminRestore,
}
pub const RESTORED_REPLY_SCHEMA: u16 = 1;
/// The reply to [`AdminCommand::BackupCreate`].
#[derive(Debug, Serialize, Deserialize)]
pub struct BackupCreatedReply {
    pub schema: u16,
    pub backup: focal_client::admin::AdminBackup,
}
pub const BACKUP_CREATED_REPLY_SCHEMA: u16 = 1;
/// The reply to [`AdminCommand::GcRestore`].
#[derive(Debug, Serialize, Deserialize)]
pub struct GcRestoreReply {
    pub schema: u16,
    pub restored: bool,
}
pub const GC_RESTORE_REPLY_SCHEMA: u16 = 1;
/// The transfer an operator's move request denotes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeMovedReply {
    pub schema: u16,
    pub tenant: [u8; 16],
    pub session: [u8; 16],
    pub member: [u8; 16],
    pub node: u64,
    pub operation: [u8; 16],
}
pub const RANGE_MOVED_REPLY_SCHEMA: u16 = 1;
/// The plan an operator's request denotes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionPlannedReply {
    pub schema: u16,
    pub tenant: [u8; 16],
    pub session: [u8; 16],
    pub operation: [u8; 16],
    pub voters: Vec<u64>,
    /// `planned`, `pending` or `satisfied`.
    pub state: u8,
    pub dry_run: bool,
}
pub const SESSION_PLANNED_REPLY_SCHEMA: u16 = 2;
/// The tenants the cluster serves: the founder's own and every admitted one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantsReply {
    pub schema: u16,
    pub applied_index: u64,
    pub revision: u64,
    pub founder: [u8; 16],
    pub admitted: Vec<[u8; 16]>,
}
pub const TENANTS_REPLY_SCHEMA: u16 = 1;
/// An application session created on this node, or found again by name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionCreatedReply {
    pub schema: u16,
    pub tenant: [u8; 16],
    pub session: [u8; 16],
    pub group: [u8; 16],
    pub node: u64,
    pub existing: bool,
}
pub const SESSION_CREATED_REPLY_SCHEMA: u16 = 1;
fn encode_reply<T: Serialize>(reply: &T) -> Result<Vec<u8>, AccessError> {
    let len = postcard::experimental::serialized_size(reply).map_err(|_| AccessError::Capacity)?;
    if len > MAX_COMMAND {
        return Err(AccessError::Capacity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(len)
        .map_err(|_| AccessError::Capacity)?;
    bytes.resize(len, 0);
    postcard::to_slice(reply, &mut bytes).map_err(|_| AccessError::InvalidRequest)?;
    Ok(bytes)
}
/// The operator's placement view ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §15).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementReply {
    pub schema: u16,
    pub placement: focal_client::admin::AdminPlacement,
    pub actions: Vec<focal_client::admin::AdminPlannedAction>,
}
pub const PLACEMENT_REPLY_SCHEMA: u16 = 1;
/// The view fits one admin frame: sessions and nodes beyond these bounds are
/// reported as truncated.
const MAX_REPORT_SESSIONS: usize = 48;
const MAX_REPORT_NODES: usize = 128;
fn hex(bytes: &[u8]) -> String {
    let mut text = String::new();
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
    }
    text
}
/// Project the agent's last observation for the operator: committed facts
/// only, with each session's guarantee measured against the live nodes.
/// The labels behind the directory's identities (24 §22): every registered
/// region's label and the topology each node announced with its contact.
#[derive(Debug, Default)]
pub(crate) struct TopologyLabels {
    pub regions: std::collections::BTreeMap<focal_directory::RegionId, String>,
    pub contacts: std::collections::BTreeMap<u64, ContactLabels>,
    /// The nodes whose credential the enrollment registry still authorizes;
    /// `None` when the registry could not be read.
    pub credentialed: Option<std::collections::BTreeSet<u64>>,
}
/// What a node's committed contact says about it (24 §22, §24).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ContactLabels {
    pub region: Option<String>,
    pub zone: Option<String>,
    pub advertise: Option<String>,
    pub endpoint: Option<String>,
}
impl TopologyLabels {
    fn region(&self, region: focal_directory::RegionId) -> Option<String> {
        if region == focal_directory::RegionId::UNKNOWN {
            return None;
        }
        Some(
            self.regions
                .get(&region)
                .cloned()
                .unwrap_or_else(|| hex(&region.0)),
        )
    }
}
pub(crate) fn placement_reply(
    report: crate::placement_control::DirectoryReport,
    labels: &TopologyLabels,
) -> PlacementReply {
    use focal_client::admin::{
        AdminAssignmentProgress, AdminPartition, AdminPendingPlacement, AdminPlacement,
        AdminPlacementNode, AdminPlannedAction, AdminSeal, AdminSessionPlacement,
    };
    let mut partitions = Vec::new();
    let mut actions = Vec::new();
    for (delegation, checkpoint) in report.partitions {
        let (split_at, merge_at) = crate::placement_agent::split::thresholds(checkpoint.cluster.0);
        let partition = hex(&delegation.partition.0);
        let nodes: Vec<AdminPlacementNode> = checkpoint
            .nodes
            .values()
            .take(MAX_REPORT_NODES)
            .map(|record| AdminPlacementNode {
                node: record.enrollment.node,
                generation: record.enrollment.generation,
                eligible: record.enrollment.eligible,
                alive: record.is_alive(),
                incarnation: record.liveness.map(|liveness| liveness.incarnation),
                available_memory: record.load.map(|load| load.available_memory),
                active_weight: record.load.map(|load| load.active_weight),
                disk_available: record.load.map(|load| load.disk_available),
                capability: record.load.map(|load| load.capability),
                region: labels
                    .contacts
                    .get(&record.enrollment.node)
                    .and_then(|contact| contact.region.clone())
                    .or_else(|| labels.region(record.enrollment.region)),
                zone: labels
                    .contacts
                    .get(&record.enrollment.node)
                    .and_then(|contact| contact.zone.clone()),
                advertise: labels
                    .contacts
                    .get(&record.enrollment.node)
                    .and_then(|contact| contact.advertise.clone()),
                endpoint: labels
                    .contacts
                    .get(&record.enrollment.node)
                    .and_then(|contact| contact.endpoint.clone()),
                credential: match &labels.credentialed {
                    Some(nodes) if nodes.contains(&record.enrollment.node) => "active".into(),
                    Some(_) => "retired".into(),
                    None => "unknown".into(),
                },
            })
            .collect();
        let mut sessions = Vec::new();
        for (ledger, descriptor) in checkpoint.sessions.iter().take(MAX_REPORT_SESSIONS) {
            let guarantee =
                focal_directory::effective_guarantee(descriptor, &checkpoint.nodes).ok();
            let placement = &descriptor.active.placement;
            sessions.push(AdminSessionPlacement {
                tenant: ledger.tenant.to_string(),
                session: ledger.session.to_string(),
                route_epoch: descriptor.route_epoch.0,
                membership_epoch: descriptor.membership_epoch,
                placement_epoch: descriptor.placement_epoch,
                preferred_leader: placement.preferred_leader,
                founder: descriptor.founder,
                range_epoch: descriptor.holders.as_ref().map(|holders| holders.epoch),
                holders: descriptor
                    .holders
                    .as_ref()
                    .map(|holders| {
                        holders
                            .members
                            .iter()
                            .map(|holder| focal_client::admin::AdminRangeHolder {
                                member: hex(&holder.member.0.to_le_bytes()),
                                start: holder.start.map(|start| hex(&start)),
                                node: holder.node,
                                generation: holder.generation,
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                voters: placement.voters.keys().copied().collect(),
                materializers: placement.materializers.keys().copied().collect(),
                content_copies: placement.content_copies.keys().copied().collect(),
                survive: format!("{:?}", descriptor.active.policy.durability.survive),
                max_failures: descriptor.active.policy.durability.max_failures,
                residency: descriptor
                    .active
                    .policy
                    .residency
                    .iter()
                    .filter_map(|region| labels.region(*region))
                    .collect(),
                home_regions: descriptor
                    .active
                    .policy
                    .home_regions
                    .iter()
                    .filter_map(|region| labels.region(*region))
                    .collect(),
                achieved_survive: guarantee
                    .as_ref()
                    .and_then(|report| report.achieved)
                    .map(|achieved| format!("{:?}", achieved.survive)),
                achieved_max_failures: guarantee
                    .as_ref()
                    .and_then(|report| report.achieved)
                    .map(|achieved| achieved.max_failures),
                blocked_by: guarantee
                    .as_ref()
                    .map(|report| {
                        report
                            .blocked_by
                            .iter()
                            .map(|blocker| match blocker.node {
                                Some(node) => format!("{:?} on node {node}", blocker.reason),
                                None => format!("{:?}", blocker.reason),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                phase: guarantee
                    .as_ref()
                    .and_then(|report| report.phase)
                    .map(|phase| format!("{phase:?}")),
                pending: descriptor
                    .pending
                    .as_ref()
                    .map(|plan| AdminPendingPlacement {
                        operation: hex(&plan.operation.0),
                        phase: format!("{:?}", plan.phase),
                        voters: plan.desired.placement.voters.keys().copied().collect(),
                        progress: plan
                            .progress
                            .values()
                            .map(|progress| AdminAssignmentProgress {
                                node: progress.node,
                                phase: format!("{:?}", progress.phase),
                                attempt: progress.attempt,
                                through: progress.through.0,
                                refusal: progress.refusal.map(|code| format!("{code:?}")),
                            })
                            .collect(),
                    }),
                retiring: descriptor.retiring.keys().copied().collect(),
            });
            for action in
                crate::placement_agent::controller::planned_actions(descriptor, &checkpoint.nodes)
            {
                actions.push(AdminPlannedAction {
                    partition: partition.clone(),
                    tenant: Some(ledger.tenant.to_string()),
                    session: Some(ledger.session.to_string()),
                    action,
                });
            }
        }
        match &checkpoint.sealed {
            Some(seal) if seal.moved == checkpoint.delegation.namespace => {
                actions.push(AdminPlannedAction {
                    partition: partition.clone(),
                    tenant: None,
                    session: None,
                    action: format!(
                        "sealed for partition {}: merge or transfer in progress (operation {})",
                        hex(&seal.destination.0),
                        hex(&seal.operation.0)
                    ),
                });
            }
            Some(seal) => {
                actions.push(AdminPlannedAction {
                    partition: partition.clone(),
                    tenant: None,
                    session: None,
                    action: format!(
                        "split in progress toward partition {} (operation {})",
                        hex(&seal.destination.0),
                        hex(&seal.operation.0)
                    ),
                });
            }
            None if checkpoint.sessions.len() >= split_at => {
                actions.push(AdminPlannedAction {
                    partition: partition.clone(),
                    tenant: None,
                    session: None,
                    action: format!(
                        "split this partition at the median of its {} sessions (threshold {split_at})",
                        checkpoint.sessions.len()
                    ),
                });
            }
            None if checkpoint.sessions.len() <= merge_at
                && checkpoint.delegation.namespace.end.is_some() =>
            {
                actions.push(AdminPlannedAction {
                    partition: partition.clone(),
                    tenant: None,
                    session: None,
                    action: format!(
                        "merge with the partition above when it holds at most {merge_at} sessions"
                    ),
                });
            }
            None => {}
        }
        partitions.push(AdminPartition {
            partition,
            group: hex(&delegation.log_group.0),
            namespace_start: hex(&delegation.namespace.start.0),
            namespace_end: delegation.namespace.end.map(|key| hex(&key.0)),
            epoch: delegation.epoch,
            revision: checkpoint.revision,
            sealed: checkpoint.sealed.as_ref().map(|seal| AdminSeal {
                operation: hex(&seal.operation.0),
                destination: hex(&seal.destination.0),
                moved_start: hex(&seal.moved.start.0),
                moved_end: seal.moved.end.map(|key| hex(&key.0)),
                next_epoch: seal.next_epoch,
            }),
            nodes,
            sessions,
            truncated: checkpoint.sessions.len() > MAX_REPORT_SESSIONS
                || checkpoint.nodes.len() > MAX_REPORT_NODES,
        });
    }
    PlacementReply {
        schema: PLACEMENT_REPLY_SCHEMA,
        placement: AdminPlacement {
            observed_at: report.observed_at,
            partitions,
        },
        actions,
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AdminRead {
    Membership,
    Configuration,
    Contacts,
    Invitations {
        after: Option<[u8; 16]>,
        limit: u16,
        expected_revision: Option<u64>,
    },
    Invitation {
        id: [u8; 16],
    },
    PrepareRevocation {
        id: [u8; 16],
    },
    Reconcile {
        sequence: u64,
    },
    /// Prepare the authority command that sets a node's placement
    /// eligibility (24 §19); the founder is never drained.
    PrepareEligibility {
        node: u64,
        eligible: bool,
    },
}
impl AdminCommand {
    pub fn invitation(name: impl Into<String>) -> Result<Self, AccessError> {
        let name = name.into();
        validate_name(&name)?;
        Ok(Self::Invite { name })
    }
    pub fn encode(&self) -> Result<Vec<u8>, AccessError> {
        self.validate()?;
        let len = postcard::experimental::serialized_size(self)
            .map_err(|_| AccessError::InvalidRequest)?;
        let total = MAGIC
            .len()
            .checked_add(len)
            .filter(|len| *len <= MAX_COMMAND)
            .ok_or(AccessError::Capacity)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total)
            .map_err(|_| AccessError::Capacity)?;
        bytes.extend_from_slice(MAGIC);
        bytes.resize(total, 0);
        postcard::to_slice(
            self,
            bytes
                .get_mut(MAGIC.len()..)
                .ok_or(AccessError::InvalidRequest)?,
        )
        .map_err(|_| AccessError::InvalidRequest)?;
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, AccessError> {
        if bytes.len() > MAX_COMMAND {
            return Err(AccessError::Capacity);
        }
        let payload = bytes
            .strip_prefix(MAGIC)
            .ok_or(AccessError::InvalidRequest)?;
        let (command, tail): (Self, _) =
            postcard::take_from_bytes(payload).map_err(|_| AccessError::InvalidRequest)?;
        if !tail.is_empty() {
            return Err(AccessError::InvalidRequest);
        }
        command.validate()?;
        Ok(command)
    }
    pub fn request(&self, identity: &NodeIdentity) -> Result<RequestEnvelope, AccessError> {
        self.validate()?;
        Ok(RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: root_namespace(identity),
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: match self {
                Self::Invite { name } => invitation_request_id(identity.cluster, name)?,
                Self::InviteClient { name } => {
                    client_invitation_request_id(identity.cluster, name)?
                }
                _ => admin_request_id(&self.encode()?)?,
            },
            operation: Operation::Control {
                group: crate::network_state::root_group(identity.cluster),
                request: self.encode()?,
            },
        })
    }
    fn validate(&self) -> Result<(), AccessError> {
        match self {
            Self::Operator(read) => read.validate(),
            Self::Replica(command) => command.validate(),
            Self::RenewCredential | Self::RotateCredential | Self::Placement | Self::Tenants => {
                Ok(())
            }
            Self::AdmitTenant { tenant } if *tenant == [0; 16] => Err(AccessError::InvalidRequest),
            Self::AdmitTenant { .. } => Ok(()),
            Self::UpgradeStatus => Ok(()),
            Self::ActivateFence { level: 0 } => Err(AccessError::InvalidRequest),
            Self::ActivateFence { .. } => Ok(()),
            Self::CreateSession { tenant, name } => {
                if *tenant == [0; 16] {
                    return Err(AccessError::InvalidRequest);
                }
                validate_name(name)
            }
            Self::PlanSession {
                tenant,
                session,
                survive,
                max_failures,
                ..
            } => {
                if *tenant == [0; 16] || *session == [0; 16] || *survive > 2 || *max_failures > 255
                {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
            Self::MoveRange {
                tenant,
                session,
                member,
                node,
            } => {
                if *tenant == [0; 16] || *session == [0; 16] || *member == [0; 16] || *node == 0 {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
            Self::GcRestore { domain, root } => {
                if *domain == [0; 16] || *root == [0; 32] {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
            Self::BackupCreate {
                tenant,
                session,
                output,
            } => {
                if *tenant == [0; 16]
                    || *session == [0; 16]
                    || output.is_empty()
                    || output.len() > 4096
                    || !std::path::Path::new(output).is_absolute()
                {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
            Self::Restore { input, .. } => {
                if input.is_empty()
                    || input.len() > 4096
                    || !std::path::Path::new(input).is_absolute()
                {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
            Self::Repair {
                tenant,
                session,
                after,
                limit,
            } => {
                if *tenant == [0; 16]
                    || *session == [0; 16]
                    || after.is_some_and(|after| after == [0; 16])
                    || *limit == 0
                    || *limit > crate::evidence_service::MAX_REPAIR_OBJECTS
                {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
            Self::Invite { name } | Self::InviteClient { name } => validate_name(name),
            Self::Read(AdminRead::Invitations { limit, .. }) if *limit == 0 || *limit > 64 => {
                Err(AccessError::InvalidRequest)
            }
            Self::Read(AdminRead::Reconcile { sequence: 0 }) => Err(AccessError::InvalidRequest),
            Self::Read(AdminRead::PrepareEligibility { node: 0, .. }) => {
                Err(AccessError::InvalidRequest)
            }
            Self::Authority(request) => {
                let ControlCommand::Authority(command) = &request.command else {
                    return Err(AccessError::Unauthorized);
                };
                let focal_directory::AuthorityOperation::GrantNode {
                    grant,
                    expected_generation: Some(expected),
                } = &command.operation
                else {
                    return Err(AccessError::Unauthorized);
                };
                if request.id.sequence == 0
                    || request.acknowledged_through >= request.id.sequence
                    || grant.enrollment.node == 0
                    || Some(grant.enrollment.generation) != expected.checked_add(1)
                    || grant.enrollment.attestation != focal_model::ContentHash([0; 32])
                {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
            Self::Read(AdminRead::Invitation { id } | AdminRead::PrepareRevocation { id })
                if *id == [0; 16] =>
            {
                Err(AccessError::InvalidRequest)
            }
            Self::Read(_) => Ok(()),
            Self::Revocation(request) => {
                let ControlCommand::Enrollment(command) = &request.command else {
                    return Err(AccessError::Unauthorized);
                };
                if command.revoked_invitation().is_none()
                    || request.id.sequence == 0
                    || request.acknowledged_through >= request.id.sequence
                {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
            Self::Membership(request) => {
                let ControlCommand::Membership(command) = &request.command else {
                    return Err(AccessError::Unauthorized);
                };
                if request.id.sequence == 0 || request.acknowledged_through >= request.id.sequence {
                    return Err(AccessError::InvalidRequest);
                }
                command
                    .expected
                    .validate()
                    .map_err(|_| AccessError::InvalidRequest)?;
                command
                    .change
                    .apply_to(&command.expected)
                    .map_err(|_| AccessError::InvalidRequest)?;
                Ok(())
            }
            Self::Transfer(request) => {
                request
                    .expected
                    .validate()
                    .map_err(|_| AccessError::InvalidRequest)?;
                if request.target == 0 || !request.expected.voters.contains(&request.target) {
                    return Err(AccessError::InvalidRequest);
                }
                Ok(())
            }
        }
    }
}
fn admin_request_id(bytes: &[u8]) -> Result<RequestId, AccessError> {
    let digest = blake3::derive_key("focal.node.local-admin.request.v1", bytes);
    let mut id = [0; 16];
    for (target, byte) in id.iter_mut().zip(digest) {
        *target = byte;
    }
    if id == [0; 16] {
        return Err(AccessError::InvalidRequest);
    }
    Ok(RequestId(id))
}
/// A fixed private control sequence namespace, minted only at the trusted
/// local administrator boundary. It is not a network Runtime credential.
pub fn admin_principal(identity: &NodeIdentity) -> focal_model::ParticipantId {
    let mut hash = blake3::Hasher::new_derive_key("focal.node.local-admin.principal.v1");
    hash.update(&identity.cluster);
    hash.update(&identity.node.to_be_bytes());
    hash.update(&identity.issuer.0);
    let mut bytes = [0; 16];
    for (target, byte) in bytes.iter_mut().zip(hash.finalize().as_bytes()) {
        *target = *byte;
    }
    focal_model::ParticipantId(bytes)
}
pub fn invitation_request_id(cluster: [u8; 16], name: &str) -> Result<RequestId, AccessError> {
    validate_name(name)?;
    if cluster == [0; 16] {
        return Err(AccessError::InvalidRequest);
    }
    let mut hash = blake3::Hasher::new_derive_key("focal.node.named-invitation.v1");
    hash.update(&cluster);
    hash.update(name.as_bytes());
    let mut id = [0; 16];
    for (target, byte) in id.iter_mut().zip(hash.finalize().as_bytes()) {
        *target = *byte;
    }
    if id == [0; 16] {
        return Err(AccessError::InvalidRequest);
    }
    Ok(RequestId(id))
}
pub fn client_invitation_request_id(
    cluster: [u8; 16],
    name: &str,
) -> Result<RequestId, AccessError> {
    validate_name(name)?;
    if cluster == [0; 16] {
        return Err(AccessError::InvalidRequest);
    }
    let mut hash = blake3::Hasher::new_derive_key("focal.node.named-client-invitation.v1");
    hash.update(&cluster);
    hash.update(name.as_bytes());
    let mut bytes = [0; 16];
    for (target, byte) in bytes.iter_mut().zip(hash.finalize().as_bytes()) {
        *target = *byte;
    }
    if bytes == [0; 16] {
        return Err(AccessError::InvalidRequest);
    }
    Ok(RequestId(bytes))
}
fn validate_name(name: &str) -> Result<(), AccessError> {
    if name.is_empty()
        || name.len() > 63
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AccessError::InvalidRequest);
    }
    Ok(())
}

/// Clones contain bounded paths and existing channel/budget handles, never a
/// copied genesis, a new queue, or an additional shared ownership wrapper.
#[derive(Clone)]
pub struct LocalNetworkAdmin {
    directory: PathBuf,
    identity: NodeIdentity,
    root: ControlIdentity,
    advertise: SocketAddr,
    /// The name this node advertises (24 §24), when it has one.
    endpoint: Option<String>,
    listen: SocketAddr,
    enrollment: Option<QuorumEnrollmentHost>,
    control: Option<crate::control_host::ControlHost>,
    fleet: Option<crate::fleet::FleetManager>,
    content: Option<crate::content_host::ContentHost>,
    credentials: Option<crate::credential_renewal::CredentialHandle>,
    placement: Option<crate::placement_control::PlacementHandle>,
    /// The collector's published state (26 §5).
    gc: Option<crate::gc::GcHandle>,
    archive: Option<crate::archive_agent::ArchiveHandle>,
    /// The evidence coordinator, for repairs (24 §20).
    evidence: Option<crate::evidence_service::EvidenceCoordinator>,
    /// The latest metrics snapshot the service sampled (24 §23).
    metrics: Option<tokio::sync::watch::Receiver<Option<crate::metrics::MetricsSnapshot>>>,
    budget: MemoryBudget,
}
impl LocalNetworkAdmin {
    pub fn new(
        directory: &NodeDirectory,
        root: ControlIdentity,
        advertise: SocketAddr,
        enrollment: QuorumEnrollmentHost,
        budget: MemoryBudget,
    ) -> Result<Self, AccessError> {
        let state = NetworkState::load(directory)
            .map_err(|_| AccessError::Unavailable)?
            .ok_or(AccessError::Unavailable)?;
        if root.scope != ControlScope::Root
            || state.genesis.root != root
            || state.genesis.founder != *directory.identity()
            || state.advertise != advertise
        {
            return Err(AccessError::Unauthorized);
        }
        Ok(Self {
            directory: directory.root().to_path_buf(),
            identity: directory.identity().clone(),
            root,
            advertise,
            endpoint: state.endpoint.clone(),
            listen: state.listen,
            enrollment: Some(enrollment),
            control: None,
            fleet: None,
            content: None,
            credentials: None,
            placement: None,
            gc: None,
            archive: None,
            evidence: None,
            metrics: None,
            budget,
        })
    }
    /// Every physical node may expose its own OS-authorized root owner. Node
    /// certificates never receive this authority and invitations remain founder-only.
    pub fn for_node(
        directory: &NodeDirectory,
        root: ControlIdentity,
        advertise: SocketAddr,
        enrollment: Option<QuorumEnrollmentHost>,
        budget: MemoryBudget,
    ) -> Result<Self, AccessError> {
        let state = NetworkState::load(directory)
            .map_err(|_| AccessError::Unavailable)?
            .ok_or(AccessError::Unavailable)?;
        if root.scope != ControlScope::Root
            || state.genesis.root != root
            || state.advertise != advertise
            || (enrollment.is_some() && state.genesis.founder != *directory.identity())
        {
            return Err(AccessError::Unauthorized);
        }
        Ok(Self {
            directory: directory.root().to_path_buf(),
            identity: directory.identity().clone(),
            root,
            advertise,
            endpoint: state.endpoint.clone(),
            listen: state.listen,
            enrollment,
            control: None,
            fleet: None,
            content: None,
            credentials: None,
            placement: None,
            gc: None,
            archive: None,
            evidence: None,
            metrics: None,
            budget,
        })
    }
    pub fn with_credentials(
        mut self,
        credentials: crate::credential_renewal::CredentialHandle,
    ) -> Self {
        self.credentials = Some(credentials);
        self
    }
    /// Serve the placement view from the running placement agent.
    pub fn with_placement(mut self, placement: crate::placement_control::PlacementHandle) -> Self {
        self.placement = Some(placement);
        self
    }
    /// Serve the collector's state from the running agent.
    pub fn with_gc(mut self, gc: crate::gc::GcHandle) -> Self {
        self.gc = Some(gc);
        self
    }
    /// Serve the archive agent's state from the running agent.
    pub fn with_archive(mut self, archive: crate::archive_agent::ArchiveHandle) -> Self {
        self.archive = Some(archive);
        self
    }
    pub fn with_evidence(mut self, evidence: crate::evidence_service::EvidenceCoordinator) -> Self {
        self.evidence = Some(evidence);
        self
    }
    pub fn with_metrics(
        mut self,
        metrics: tokio::sync::watch::Receiver<Option<crate::metrics::MetricsSnapshot>>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }
    /// Repair a hosted session's custody on this node (24 §20): the replica
    /// exports its committed prefix and the evidence coordinator walks it.
    async fn repair(
        &self,
        tenant: [u8; 16],
        session: [u8; 16],
        after: Option<[u8; 16]>,
        limit: u32,
    ) -> Result<Vec<u8>, AccessError> {
        let fleet = self.fleet.as_ref().ok_or(AccessError::Unavailable)?;
        let evidence = self.evidence.as_ref().ok_or(AccessError::Unavailable)?;
        let ledger = focal_model::LedgerId {
            tenant: focal_model::TenantId(tenant),
            session: focal_model::SessionId(session),
        };
        if ledger.tenant.is_zero() || ledger.session.is_zero() {
            return Err(AccessError::InvalidRequest);
        }
        let host = fleet
            .current_host(ledger)
            .map_err(|_| AccessError::Unavailable)?;
        let snapshot = host
            .checkpoint_evidence(REPAIR_TTL)
            .await
            .map_err(|error| match error {
                focal_ledger::LedgerError::Capacity | focal_ledger::LedgerError::Memory(_) => {
                    AccessError::Capacity
                }
                _ => AccessError::Unavailable,
            })?;
        let report = evidence
            .repair(snapshot, after.map(focal_model::ArtifactId), limit)
            .await?;
        let mut unrecoverable = Vec::new();
        unrecoverable
            .try_reserve_exact(report.unrecoverable.len())
            .map_err(|_| AccessError::Capacity)?;
        for object in &report.unrecoverable {
            unrecoverable.push(focal_client::admin::AdminUnrecoverableObject {
                artifact: object.artifact.to_string(),
                root: object.reference.root.to_string(),
                length: object.reference.length,
                asked: object.asked,
            });
        }
        encode_reply(&RepairedReply {
            schema: REPAIRED_REPLY_SCHEMA,
            repair: focal_client::admin::AdminRepair {
                tenant: ledger.tenant.to_string(),
                session: ledger.session.to_string(),
                node: self.identity.node,
                sequence: report.sequence.0,
                index: report.index.0,
                artifacts: report.artifacts,
                objects: report.objects,
                verified: report.verified,
                repaired: report.repaired,
                pushed: report.pushed,
                unrecoverable,
                unrecoverable_count: report.unrecoverable_count,
                restore_required: report.unrecoverable_count > 0,
                complete: report.complete,
                next_after: report.next_after.map(|artifact| artifact.to_string()),
            },
        })
    }
    /// Write a backup of a hosted session (26 §6): the replica exports its
    /// durable prefix, the files are written under the operator's directory.
    async fn backup_create(
        &self,
        tenant: [u8; 16],
        session: [u8; 16],
        output: String,
    ) -> Result<Vec<u8>, AccessError> {
        let fleet = self.fleet.as_ref().ok_or(AccessError::Unavailable)?;
        let ledger = focal_model::LedgerId {
            tenant: focal_model::TenantId(tenant),
            session: focal_model::SessionId(session),
        };
        if ledger.tenant.is_zero() || ledger.session.is_zero() || output.is_empty() {
            return Err(AccessError::InvalidRequest);
        }
        let backup = crate::backup::create(
            fleet,
            &self.directory,
            ledger,
            PathBuf::from(output),
            &self.budget,
        )
        .await?;
        encode_reply(&BackupCreatedReply {
            schema: BACKUP_CREATED_REPLY_SCHEMA,
            backup,
        })
    }
    /// Restore a session from a backup (26 §6): the tenant must be served,
    /// the incarnation decision is taken against the committed enrollment
    /// registry, and the placement agent does the rest.
    async fn restore(
        &self,
        input: String,
        new_incarnation: bool,
        id: RequestId,
    ) -> Result<Vec<u8>, AccessError> {
        let input = PathBuf::from(input);
        let manifest =
            crate::backup::read_manifest(&input).map_err(|_| AccessError::InvalidRequest)?;
        let (_, registry) = self.read_registry(id).await?;
        let tenant = manifest.prefix.ledger.tenant;
        if tenant != self.identity.ledger.tenant && !registry.admits_tenant(tenant.0) {
            return Err(AccessError::Unauthorized);
        }
        let decision = crate::backup::decide(
            &manifest,
            self.identity.cluster,
            self.identity.node,
            &registry,
        );
        let restored = self
            .placement
            .as_ref()
            .ok_or(AccessError::Unavailable)?
            .restore_session(crate::backup::RestoreRequest {
                input: input.clone(),
                new_incarnation,
                decision,
            })
            .await
            .map_err(|error| {
                use crate::placement_agent::AgentError;
                match error {
                    AgentError::Capacity
                    | AgentError::Admission(_)
                    | AgentError::Fleet(crate::fleet::FleetError::Capacity) => {
                        AccessError::Capacity
                    }
                    AgentError::Restore(_) | AgentError::Backup(_) | AgentError::Identity => {
                        AccessError::InvalidRequest
                    }
                    _ => AccessError::Unavailable,
                }
            })?;
        encode_reply(&RestoredReply {
            schema: RESTORED_REPLY_SCHEMA,
            restored: restored.admin(&input),
        })
    }
    /// Bring a quarantined object back (26 §5) through the content writer.
    async fn gc_restore(&self, domain: [u8; 16], root: [u8; 32]) -> Result<Vec<u8>, AccessError> {
        let content = self.content.as_ref().ok_or(AccessError::Unavailable)?;
        let restored = content
            .restore_quarantined(
                focal_model::ContentDomainId(domain),
                focal_model::ContentHash(root),
            )
            .await?;
        let reply = GcRestoreReply {
            schema: GC_RESTORE_REPLY_SCHEMA,
            restored,
        };
        let len =
            postcard::experimental::serialized_size(&reply).map_err(|_| AccessError::Capacity)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| AccessError::Capacity)?;
        bytes.resize(len, 0);
        postcard::to_slice(&reply, &mut bytes).map_err(|_| AccessError::InvalidRequest)?;
        Ok(bytes)
    }
    async fn placement(&self) -> Result<Vec<u8>, AccessError> {
        let handle = self.placement.as_ref().ok_or(AccessError::Unavailable)?;
        let report = handle
            .directory()
            .await
            .map_err(|_| AccessError::Unavailable)?;
        let reply = placement_reply(report, &self.topology_labels().await);
        let len =
            postcard::experimental::serialized_size(&reply).map_err(|_| AccessError::Capacity)?;
        if len > MAX_COMMAND {
            return Err(AccessError::Capacity);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| AccessError::Capacity)?;
        bytes.resize(len, 0);
        postcard::to_slice(&reply, &mut bytes).map_err(|_| AccessError::InvalidRequest)?;
        Ok(bytes)
    }
    /// The region labels the root registered and the topology every node
    /// announced (24 §22), from this node's applied root replica; empty when
    /// the root cannot be observed, so a view degrades to identities.
    pub(crate) async fn topology_labels(&self) -> TopologyLabels {
        let mut labels = TopologyLabels::default();
        let Some(control) = self.control.as_ref() else {
            return labels;
        };
        let Ok(observation) = control.observe_root().await else {
            return labels;
        };
        if let focal_control::ControlBootstrap::Root {
            directory,
            enrollment,
            ..
        } = &observation.snapshot().state
        {
            for (id, region) in &directory.regions {
                labels.regions.insert(*id, region.label.clone());
            }
            // Which nodes the registry still authorizes: a revoked or
            // expired credential shows as retired (runbooks/expired-credentials).
            labels.credentialed = focal_enrollment::EnrollmentRegistry::restore(
                enrollment,
                self.identity.cluster,
                focal_enrollment::EnrollmentLimits::default(),
            )
            .ok()
            .zip(crate::network_bootstrap::unix_time().ok())
            .map(|(registry, now)| {
                registry
                    .enrollments()
                    .filter(|receipt| {
                        registry
                            .authorize_certificate(&receipt.certificate, now)
                            .is_ok()
                    })
                    .filter_map(|receipt| receipt.identity.node_id)
                    .collect()
            });
        }
        for contact in &observation.contacts().contacts.records {
            labels.contacts.insert(
                contact.node,
                ContactLabels {
                    region: contact.region.clone(),
                    zone: contact.zone.clone(),
                    advertise: Some(contact.advertise.to_string()),
                    endpoint: contact.endpoint.clone(),
                },
            );
        }
        labels
    }
    /// The root owner's committed enrollment registry, read as this node's
    /// runtime principal: one bounded root checkpoint.
    /// The enrollment registry as this node's root replica applied it.
    async fn local_registry(&self) -> Result<focal_enrollment::EnrollmentRegistry, AccessError> {
        let control = self.control.as_ref().ok_or(AccessError::Unavailable)?;
        let observation = control
            .observe_root()
            .await
            .map_err(|_| AccessError::Unavailable)?;
        let focal_control::ControlBootstrap::Root { enrollment, .. } =
            &observation.snapshot().state
        else {
            return Err(AccessError::Unavailable);
        };
        focal_enrollment::EnrollmentRegistry::restore(
            enrollment,
            self.identity.cluster,
            focal_enrollment::EnrollmentLimits::default(),
        )
        .map_err(|_| AccessError::Unavailable)
    }
    async fn read_registry(
        &self,
        id: RequestId,
    ) -> Result<(u64, focal_enrollment::EnrollmentRegistry), AccessError> {
        let control = self.control.as_ref().ok_or(AccessError::Unavailable)?;
        let principal = admin_principal(&self.identity);
        if principal.is_zero() {
            return Err(AccessError::Unauthorized);
        }
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal,
            tenants: std::collections::BTreeSet::from([self.identity.ledger.tenant]),
            role: PeerRole::Runtime,
        })
        .map_err(|_| AccessError::Unauthorized)?;
        let result =
            control
                .read(peer, id, ControlRead::State)
                .await
                .map_err(|error| match error {
                    ControlFailure::Capacity => AccessError::Capacity,
                    ControlFailure::Unauthorized => AccessError::Unauthorized,
                    _ => AccessError::Unavailable,
                })?;
        let focal_control::ControlReadResult::State(snapshot) = result else {
            return Err(AccessError::Unavailable);
        };
        let focal_control::ControlBootstrap::Root { enrollment, .. } = &snapshot.state else {
            return Err(AccessError::Unauthorized);
        };
        let registry = focal_enrollment::EnrollmentRegistry::restore(
            enrollment.as_slice(),
            self.identity.cluster,
            focal_enrollment::EnrollmentLimits::default(),
        )
        .map_err(|_| AccessError::Unavailable)?;
        Ok((snapshot.applied_index, registry))
    }
    /// The tenants the cluster serves (doc 24 §16).
    async fn tenants(&self, id: RequestId) -> Result<Vec<u8>, AccessError> {
        let (applied_index, registry) = self.read_registry(id).await?;
        encode_reply(&TenantsReply {
            schema: TENANTS_REPLY_SCHEMA,
            applied_index,
            revision: registry.revision(),
            founder: self.identity.ledger.tenant.0,
            admitted: registry.tenants().collect(),
        })
    }
    /// Every node the directory lists with the capability it last reported
    /// (24 §21); a node listed by several partitions reports one level.
    async fn node_capabilities(&self) -> Result<Vec<(u64, u32)>, AccessError> {
        let handle = self.placement.as_ref().ok_or(AccessError::Unavailable)?;
        let report = handle
            .directory()
            .await
            .map_err(|_| AccessError::Unavailable)?;
        let mut nodes: std::collections::BTreeMap<u64, u32> = std::collections::BTreeMap::new();
        for (_, checkpoint) in &report.partitions {
            for record in checkpoint.nodes.values() {
                let level = record.load.map_or(0, |load| load.capability);
                let entry = nodes.entry(record.enrollment.node).or_insert(0);
                *entry = (*entry).max(level);
            }
        }
        let mut listed = Vec::new();
        listed
            .try_reserve_exact(nodes.len())
            .map_err(|_| AccessError::Capacity)?;
        listed.extend(nodes);
        Ok(listed)
    }
    /// The upgrade fence as the root committed it, this binary's levels and
    /// every node's reported level (24 §21).
    async fn upgrade_status(&self, id: RequestId, changed: bool) -> Result<Vec<u8>, AccessError> {
        let (applied_index, registry) = match self.read_registry(id).await {
            Ok(read) => read,
            Err(AccessError::Unavailable) => (0, self.local_registry().await?),
            Err(error) => return Err(error),
        };
        encode_reply(&UpgradeReply {
            schema: UPGRADE_REPLY_SCHEMA,
            applied_index,
            revision: registry.revision(),
            fence: registry.fence(),
            binary: crate::upgrade::CAPABILITY_LEVEL,
            announced: crate::upgrade::announced_level(),
            nodes: self.node_capabilities().await?,
            changed,
        })
    }
    /// Raise the upgrade fence through the founder's enrollment authority
    /// (24 §21), once every node the directory lists has reported at least
    /// `level`; a fence already there is answered as done.
    async fn activate_fence(&self, level: u32, id: RequestId) -> Result<Vec<u8>, AccessError> {
        let enrollment = self.enrollment.as_ref().ok_or(AccessError::Unauthorized)?;
        let (_, registry) = self.read_registry(id).await?;
        if level < registry.fence().level {
            return Err(AccessError::InvalidRequest);
        }
        if level == registry.fence().level {
            return self.upgrade_status(id, false).await;
        }
        let nodes = self.node_capabilities().await?;
        if nodes.is_empty() || nodes.iter().any(|(_, capability)| *capability < level) {
            return Err(AccessError::Unavailable);
        }
        let before = registry.fence();
        let fence = enrollment
            .activate_fence(level)
            .await
            .map_err(enrollment_error)?;
        self.upgrade_status(id, fence != before).await
    }
    /// Admit a tenant through the founder's enrollment authority, then answer
    /// with the committed tenants; an admitted tenant is answered as done.
    async fn admit_tenant(&self, tenant: [u8; 16], id: RequestId) -> Result<Vec<u8>, AccessError> {
        self.enrollment
            .as_ref()
            .ok_or(AccessError::Unauthorized)?
            .admit_tenant(tenant)
            .await
            .map_err(enrollment_error)?;
        self.tenants(id).await
    }
    /// Create an application session on this node for the founder's tenant
    /// or an admitted one; the same name is the same session.
    async fn create_session(
        &self,
        tenant: [u8; 16],
        name: String,
        id: RequestId,
    ) -> Result<Vec<u8>, AccessError> {
        let tenant = focal_model::TenantId(tenant);
        if tenant != self.identity.ledger.tenant {
            // Admission is monotone (tenants are admitted, never removed), so
            // a node that cannot read the root through a quorum — a host
            // whose root replica learns — checks its own applied registry;
            // a lagging one refuses until the admission reaches it.
            let registry = match self.read_registry(id).await {
                Ok((_, registry)) => registry,
                Err(AccessError::Unavailable) => self.local_registry().await?,
                Err(error) => return Err(error),
            };
            if !registry.admits_tenant(tenant.0) {
                return Err(AccessError::Unauthorized);
            }
        }
        let created = self
            .placement
            .as_ref()
            .ok_or(AccessError::Unavailable)?
            .create_session(tenant, name)
            .await
            .map_err(|error| {
                use crate::placement_agent::AgentError;
                match error {
                    AgentError::Capacity
                    | AgentError::Admission(_)
                    | AgentError::Fleet(crate::fleet::FleetError::Capacity) => {
                        AccessError::Capacity
                    }
                    AgentError::Identity => AccessError::InvalidRequest,
                    _ => AccessError::Unavailable,
                }
            })?;
        encode_reply(&SessionCreatedReply {
            schema: SESSION_CREATED_REPLY_SCHEMA,
            tenant: created.ledger.tenant.0,
            session: created.ledger.session.0,
            group: created.group,
            node: created.node,
            existing: created.existing,
        })
    }
    /// Move one member of a session's range group to a node through the
    /// agent (25 §6); the reply names the transfer the request denotes.
    async fn move_range(
        &self,
        tenant: [u8; 16],
        session: [u8; 16],
        member: [u8; 16],
        node: u64,
    ) -> Result<Vec<u8>, AccessError> {
        if node == 0 || member == [0; 16] {
            return Err(AccessError::InvalidRequest);
        }
        let ledger = focal_model::LedgerId {
            tenant: focal_model::TenantId(tenant),
            session: focal_model::SessionId(session),
        };
        let operation = self
            .placement
            .as_ref()
            .ok_or(AccessError::Unavailable)?
            .move_range(
                ledger,
                focal_memory::RangeId(u128::from_le_bytes(member)),
                node,
            )
            .await
            .map_err(|error| {
                use crate::placement_agent::AgentError;
                match error {
                    AgentError::Capacity => AccessError::Capacity,
                    AgentError::Identity | AgentError::Registration(_) => {
                        AccessError::InvalidRequest
                    }
                    AgentError::Residency(_) => AccessError::Unauthorized,
                    AgentError::Ledger(focal_ledger::LedgerError::PlacementConflict) => {
                        AccessError::InvalidRequest
                    }
                    _ => AccessError::Unavailable,
                }
            })?;
        encode_reply(&RangeMovedReply {
            schema: RANGE_MOVED_REPLY_SCHEMA,
            tenant,
            session,
            member,
            node,
            operation: operation.0,
        })
    }
    /// Plan a session's placement under a requested durability through the
    /// agent (doc 24 §17); the reply names the plan the request denotes.
    async fn plan_session(
        &self,
        tenant: [u8; 16],
        session: [u8; 16],
        survive: u8,
        max_failures: u16,
        dry_run: bool,
    ) -> Result<Vec<u8>, AccessError> {
        let ledger = focal_model::LedgerId {
            tenant: focal_model::TenantId(tenant),
            session: focal_model::SessionId(session),
        };
        let durability = focal_directory::DurabilityIntent {
            survive: match survive {
                0 => focal_directory::FailureClass::Node,
                1 => focal_directory::FailureClass::Zone,
                2 => focal_directory::FailureClass::Region,
                _ => return Err(AccessError::InvalidRequest),
            },
            max_failures,
        };
        let planned = self
            .placement
            .as_ref()
            .ok_or(AccessError::Unavailable)?
            .plan_session(ledger, durability, dry_run)
            .await
            .map_err(|error| {
                use crate::placement_agent::AgentError;
                match error {
                    AgentError::Capacity => AccessError::Capacity,
                    AgentError::Registration(_) | AgentError::Identity => {
                        AccessError::InvalidRequest
                    }
                    _ => AccessError::Unavailable,
                }
            })?;
        encode_reply(&SessionPlannedReply {
            schema: SESSION_PLANNED_REPLY_SCHEMA,
            tenant,
            session,
            operation: planned.operation.0,
            voters: planned.voters,
            state: match planned.state {
                crate::placement_control::PlanState::Planned => 0,
                crate::placement_control::PlanState::Pending => 1,
                crate::placement_control::PlanState::Satisfied => 2,
            },
            dry_run,
        })
    }
    async fn rotate_credential(&self) -> Result<Vec<u8>, AccessError> {
        use crate::credential_renewal::CredentialReply;
        let handle = self.credentials.as_ref().ok_or(AccessError::Unavailable)?;
        let reply = match handle.rotate().await {
            Ok(summary) => CredentialReply::Renewed(summary),
            Err(error) => CredentialReply::Failed(error),
        };
        encode_credential_reply(&reply)
    }
    async fn renew_credential(&self) -> Result<Vec<u8>, AccessError> {
        use crate::credential_renewal::CredentialReply;
        let handle = self.credentials.as_ref().ok_or(AccessError::Unavailable)?;
        let reply = match handle.renew().await {
            Ok(summary) => CredentialReply::Renewed(summary),
            Err(error) => CredentialReply::Failed(error),
        };
        encode_credential_reply(&reply)
    }
}
fn encode_credential_reply(
    reply: &crate::credential_renewal::CredentialReply,
) -> Result<Vec<u8>, AccessError> {
    {
        let len =
            postcard::experimental::serialized_size(&reply).map_err(|_| AccessError::Capacity)?;
        if len > MAX_COMMAND {
            return Err(AccessError::Capacity);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| AccessError::Capacity)?;
        bytes.resize(len, 0);
        postcard::to_slice(reply, &mut bytes).map_err(|_| AccessError::InvalidRequest)?;
        Ok(bytes)
    }
}
impl LocalNetworkAdmin {
    pub fn with_control(
        mut self,
        control: crate::control_host::ControlHost,
    ) -> Result<Self, AccessError> {
        let progress = control.progress();
        if progress.identity != self.root || progress.node != self.identity.node {
            return Err(AccessError::Unauthorized);
        }
        self.control = Some(control);
        Ok(self)
    }
    async fn invite(&self, verified: &VerifiedRequest) -> Result<Vec<u8>, AccessError> {
        let peer = verified.peer();
        let request = verified.request();
        if peer.role() != PeerRole::Runtime
            || peer.certificate_fingerprint().is_some()
            || peer.principal() != self.identity.issuer
            || request.ledger != root_namespace(&self.identity)
            || request.route_epoch != RouteEpoch(1)
            || request.request_epoch != RequestEpoch(1)
        {
            return Err(AccessError::Unauthorized);
        }
        let Operation::Control {
            group,
            request: bytes,
        } = &request.operation
        else {
            return Err(AccessError::Unauthorized);
        };
        if *group != self.root.group {
            return Err(AccessError::Unauthorized);
        }
        let command = AdminCommand::decode(bytes)?;
        if request.request_id != command.request(&self.identity)?.request_id {
            return Err(AccessError::InvalidRequest);
        }
        if let AdminCommand::Operator(read) = command {
            return self.operator_read(read).await;
        }
        if let AdminCommand::Replica(command) = command {
            return self.replica_command(*command).await;
        }
        if let AdminCommand::RenewCredential = command {
            return self.renew_credential().await;
        }
        if let AdminCommand::RotateCredential = command {
            return self.rotate_credential().await;
        }
        if let AdminCommand::Placement = command {
            return self.placement().await;
        }
        if let AdminCommand::GcRestore { domain, root } = command {
            return self.gc_restore(domain, root).await;
        }
        if let AdminCommand::BackupCreate {
            tenant,
            session,
            output,
        } = command
        {
            return self.backup_create(tenant, session, output).await;
        }
        if let AdminCommand::Restore {
            input,
            new_incarnation,
        } = command
        {
            return self
                .restore(input, new_incarnation, request.request_id)
                .await;
        }
        if let AdminCommand::Repair {
            tenant,
            session,
            after,
            limit,
        } = command
        {
            return self.repair(tenant, session, after, limit).await;
        }
        if let AdminCommand::Tenants = command {
            return self.tenants(request.request_id).await;
        }
        if let AdminCommand::UpgradeStatus = command {
            return self.upgrade_status(request.request_id, false).await;
        }
        if let AdminCommand::ActivateFence { level } = command {
            return self.activate_fence(level, request.request_id).await;
        }
        if let AdminCommand::AdmitTenant { tenant } = command {
            return self.admit_tenant(tenant, request.request_id).await;
        }
        if let AdminCommand::CreateSession { tenant, name } = command {
            return self.create_session(tenant, name, request.request_id).await;
        }
        if let AdminCommand::PlanSession {
            tenant,
            session,
            survive,
            max_failures,
            dry_run,
        } = command
        {
            return self
                .plan_session(tenant, session, survive, max_failures, dry_run)
                .await;
        }
        if let AdminCommand::MoveRange {
            tenant,
            session,
            member,
            node,
        } = command
        {
            return self.move_range(tenant, session, member, node).await;
        }
        if !matches!(
            command,
            AdminCommand::Invite { .. } | AdminCommand::InviteClient { .. }
        ) {
            return self.control_command(command, request.request_id).await;
        }
        let (name, role, id) = match command {
            AdminCommand::Invite { name } => {
                let id = invitation_request_id(self.identity.cluster, &name)?;
                (name, EnrollmentRole::Node, id)
            }
            AdminCommand::InviteClient { name } => {
                let id = client_invitation_request_id(self.identity.cluster, &name)?;
                (name, EnrollmentRole::Client, id)
            }
            _ => return Err(AccessError::InvalidRequest),
        };
        let state = NetworkState::load_from(&self.directory, &self.identity)
            .map_err(|_| AccessError::Unavailable)?
            .ok_or(AccessError::Unavailable)?;
        if state.genesis.root != self.root
            || state.genesis.founder != self.identity
            || state.advertise != self.advertise
        {
            return Err(AccessError::Unauthorized);
        }
        let invitation = self
            .enrollment
            .as_ref()
            .ok_or(AccessError::Unauthorized)?
            .invite(
                id,
                InviteIntent {
                    // The founder as its operator named it (24 §24).
                    endpoint: self
                        .endpoint
                        .clone()
                        .unwrap_or_else(|| self.advertise.to_string()),
                    role,
                    lifetime_seconds: 3600,
                },
            )
            .await
            .map_err(enrollment_error)?;
        let mut bytes = match role {
            EnrollmentRole::Node => NodeInvitation::new(name, state.genesis, invitation)
                .map_err(|_| AccessError::Unavailable)?
                .encode()
                .map_err(|_| AccessError::Capacity)?,
            EnrollmentRole::Client => {
                crate::network_join::ClientInvitation::new(name, state.genesis, invitation)
                    .map_err(|_| AccessError::Unavailable)?
                    .encode()
                    .map_err(|_| AccessError::Capacity)?
            }
        };
        Ok(std::mem::take(&mut *bytes))
    }
    /// The founding node of this cluster, from the saved network state.
    fn founder(&self) -> Result<u64, AccessError> {
        let state = NetworkState::load_from(&self.directory, &self.identity)
            .map_err(|_| AccessError::Unavailable)?
            .ok_or(AccessError::Unavailable)?;
        if state.genesis.root != self.root {
            return Err(AccessError::Unauthorized);
        }
        Ok(state.genesis.founder.node)
    }
    async fn control_command(
        &self,
        command: AdminCommand,
        id: RequestId,
    ) -> Result<Vec<u8>, AccessError> {
        let control = self.control.as_ref().ok_or(AccessError::Unavailable)?;
        let principal = admin_principal(&self.identity);
        if principal.is_zero() {
            return Err(AccessError::Unauthorized);
        }
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal,
            tenants: std::collections::BTreeSet::from([self.identity.ledger.tenant]),
            role: PeerRole::Runtime,
        })
        .map_err(|_| AccessError::Unauthorized)?;
        let result = match command {
            AdminCommand::Read(query) => control
                .read(
                    peer,
                    id,
                    match query {
                        AdminRead::Membership => ControlRead::Membership,
                        AdminRead::Configuration => ControlRead::Configuration,
                        AdminRead::Contacts => ControlRead::Contacts,
                        AdminRead::Invitations {
                            after,
                            limit,
                            expected_revision,
                        } => ControlRead::InvitationPage {
                            after,
                            limit,
                            expected_revision,
                        },
                        AdminRead::Invitation { id } => ControlRead::Invitation { id },
                        AdminRead::PrepareRevocation { id } => {
                            ControlRead::PrepareRevocation { id }
                        }
                        AdminRead::Reconcile { sequence } => ControlRead::AdminReceipt {
                            id: focal_control::ControlRequestId {
                                client: principal.0,
                                sequence,
                            },
                        },
                        AdminRead::PrepareEligibility { node, eligible } => {
                            // The founder holds the enrollment authority and
                            // the root's bootstrap identity: it is not drained.
                            if node == self.founder()? {
                                return Err(AccessError::InvalidRequest);
                            }
                            ControlRead::PrepareEligibility { node, eligible }
                        }
                    },
                )
                .await
                .map(ControlReply::Read),
            AdminCommand::Membership(request)
            | AdminCommand::Revocation(request)
            | AdminCommand::Authority(request) => {
                if request.id.client != principal.0 {
                    return Err(AccessError::Unauthorized);
                }
                control
                    .submit(peer, *request)
                    .await
                    .map(ControlReply::Committed)
            }
            AdminCommand::Transfer(request) => {
                let target = request.target;
                control
                    .transfer(peer, id, request)
                    .await
                    .map(|()| ControlReply::TransferInitiated { target })
            }
            AdminCommand::Invite { .. }
            | AdminCommand::InviteClient { .. }
            | AdminCommand::Operator(_)
            | AdminCommand::Replica(_)
            | AdminCommand::RenewCredential
            | AdminCommand::RotateCredential
            | AdminCommand::Placement
            | AdminCommand::AdmitTenant { .. }
            | AdminCommand::Tenants
            | AdminCommand::CreateSession { .. }
            | AdminCommand::PlanSession { .. }
            | AdminCommand::MoveRange { .. }
            | AdminCommand::GcRestore { .. }
            | AdminCommand::BackupCreate { .. }
            | AdminCommand::Restore { .. }
            | AdminCommand::Repair { .. }
            | AdminCommand::UpgradeStatus
            | AdminCommand::ActivateFence { .. } => {
                return Err(AccessError::Unauthorized);
            }
        };
        result
            .unwrap_or_else(ControlReply::Rejected)
            .encode(admin_wire_limits().max_frame_bytes as usize)
            .map_err(|_| AccessError::Capacity)
    }
}
impl RequestHandler for LocalNetworkAdmin {
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
        Box::pin(async move {
            let mut reply = request
                .request()
                .reply(Response::Error(AccessError::Capacity));
            let Ok(reservation) =
                self.budget
                    .reserve(BudgetKind::Control, BudgetLane::Ordinary, WORKSPACE)
            else {
                return OwnedResponse::new(reply);
            };
            let mut allocation = reservation.commit();
            reply.result = match self.invite(&request).await {
                Ok(response) => Response::Control { response },
                Err(error) => Response::Error(error),
            };
            drop(request);
            let bytes = postcard::experimental::serialized_size(&reply)
                .ok()
                .and_then(|bytes| bytes.checked_mul(4))
                .and_then(|bytes| bytes.checked_add(4096));
            if let Some(bytes) = bytes {
                let _ = allocation.shrink_to(bytes);
            }
            OwnedResponse::accounted(reply, allocation)
        })
    }
}
fn enrollment_error(error: QuorumEnrollmentError) -> AccessError {
    match error {
        QuorumEnrollmentError::Enrollment(EnrollmentError::Capacity)
        | QuorumEnrollmentError::Control(ControlFailure::Capacity) => AccessError::Capacity,
        QuorumEnrollmentError::Control(ControlFailure::OutcomeUnknown) => {
            AccessError::OutcomeUnknown
        }
        QuorumEnrollmentError::Stopped => AccessError::OutcomeUnknown,
        QuorumEnrollmentError::Identity => AccessError::Unauthorized,
        QuorumEnrollmentError::IntentConflict => AccessError::InvalidRequest,
        QuorumEnrollmentError::Enrollment(
            EnrollmentError::Invalid
            | EnrollmentError::WrongCluster
            | EnrollmentError::Unauthorized
            | EnrollmentError::Expired
            | EnrollmentError::Revoked
            | EnrollmentError::Used,
        ) => AccessError::InvalidRequest,
        // A private journal or signing failure may happen after the root's
        // decision committed. The named intent must be retried unchanged.
        QuorumEnrollmentError::Enrollment(_) => AccessError::OutcomeUnknown,
        QuorumEnrollmentError::Control(_) => AccessError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Settings, network_bootstrap::FoundingNetwork};
    use focal_model::ParticipantId;
    use std::collections::BTreeSet;
    #[test]
    fn named_invitation_identity_is_stable_bounded_and_cluster_scoped() {
        assert_eq!(
            invitation_request_id([1; 16], "worker-2").unwrap(),
            invitation_request_id([1; 16], "worker-2").unwrap()
        );
        assert_ne!(
            invitation_request_id([1; 16], "worker-2").unwrap(),
            invitation_request_id([2; 16], "worker-2").unwrap()
        );
        assert_ne!(
            invitation_request_id([1; 16], "worker-2").unwrap(),
            invitation_request_id([1; 16], "worker-3").unwrap()
        );
        assert_ne!(
            invitation_request_id([1; 16], "worker-2").unwrap(),
            client_invitation_request_id([1; 16], "worker-2").unwrap()
        );
        for name in ["", "two words", "node\nname", &"x".repeat(64)] {
            assert!(AdminCommand::invitation(name).is_err());
        }
        let command = AdminCommand::invitation("worker-2").unwrap();
        let bytes = command.encode().unwrap();
        assert!(AdminCommand::decode(&bytes).is_ok());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(AdminCommand::decode(&trailing).is_err());
        assert!(AdminCommand::decode(&[0; MAX_COMMAND + 1]).is_err());
        assert!(
            AdminCommand::Read(AdminRead::Reconcile { sequence: 0 })
                .encode()
                .is_err()
        );
        assert_eq!(
            enrollment_error(QuorumEnrollmentError::Enrollment(EnrollmentError::Io(
                std::io::Error::other("private detail")
            ))),
            AccessError::OutcomeUnknown
        );
    }
    #[tokio::test]
    async fn admin_rejects_remote_runtime_wrong_issuer_and_generic_control() {
        let disk = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(disk.path().to_owned());
        settings.node.advertise = Some("127.0.0.1:7443".into());
        let network = FoundingNetwork::open(&settings).await.unwrap();
        let budget = MemoryBudget::new(4 * 1024 * 1024, 1024 * 1024).unwrap();
        let admin = LocalNetworkAdmin::new(
            &network.directory,
            network.state.genesis.root,
            network.state.advertise,
            network.enrollment.clone(),
            budget.clone(),
        )
        .unwrap();
        let request = AdminCommand::invitation("worker-2")
            .unwrap()
            .request(network.directory.identity())
            .unwrap();
        let tenant = request.ledger.tenant;
        let grant = |principal| PeerGrant {
            principal,
            tenants: BTreeSet::from([tenant]),
            role: PeerRole::Runtime,
        };
        let mut wrong = network.directory.identity().issuer.0;
        wrong[0] ^= 1;
        let local = AuthenticatedPeer::local(grant(ParticipantId(wrong))).unwrap();
        assert_eq!(
            admin
                .handle(verify_request(local, request.clone(), &admin_wire_limits()).unwrap())
                .await
                .result,
            Response::Error(AccessError::Unauthorized)
        );
        let peers = PeerRegistry::new(1).unwrap();
        let certificate = &network.credentials.certificate_chain()[0];
        let fingerprint = peers
            .register_certificate(certificate, grant(network.directory.identity().issuer))
            .unwrap();
        let remote = peers.authenticate(fingerprint).unwrap();
        assert_eq!(
            admin
                .handle(verify_request(remote, request.clone(), &admin_wire_limits()).unwrap())
                .await
                .result,
            Response::Error(AccessError::Unauthorized)
        );
        let mut wrong_command = request;
        if let Operation::Control { request, .. } = &mut wrong_command.operation {
            *request = vec![1, 0];
        }
        let local = AuthenticatedPeer::local(grant(network.directory.identity().issuer)).unwrap();
        let response = admin
            .handle_accounted(verify_request(local, wrong_command, &admin_wire_limits()).unwrap())
            .await;
        assert!(budget.stats().used > 0);
        assert!(budget.stats().used < WORKSPACE);
        assert_eq!(
            response.envelope().result,
            Response::Error(AccessError::InvalidRequest)
        );
        drop(response);
        assert_eq!(budget.stats().used, 0);
    }
}
