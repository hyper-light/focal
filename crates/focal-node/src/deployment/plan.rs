//! The plan artifact (`FCLPLAN1`) and its composition from observations.
use super::{DeploymentError, Guarantee, GuaranteeLevel, hex, survive_name};
use crate::config::{
    FailureDomain,
    policy::{CommittedPolicy, PolicyIntent},
};
use serde::{Deserialize, Serialize};

/// `FCLPLAN1`: magic, postcard body, then the BLAKE3 hash of both.
pub const MAGIC: &[u8; 8] = b"FCLPLAN1";
/// Schema 3 records the directory partition groups beside the root and
/// plans their voters (F24); a plan written by an older binary is refused.
pub const SCHEMA: u16 = 3;
/// A plan file never exceeds this; the directory view it is built from is
/// itself bounded.
pub const MAX_PLAN_BYTES: usize = 4 * 1024 * 1024;
/// Sessions one plan names at most.
pub const MAX_SESSIONS: usize = 4096;
/// The partition groups one plan seats: every partition a node may host and
/// the first (`network_directory::MAX_HOSTED_PARTITIONS` + 1).
pub const MAX_PARTITIONS: usize = 33;
const PLAN_ID_CONTEXT: &str = "focal.deployment.plan.v1";

/// The deployment a plan was made for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentIdentity {
    pub cluster: [u8; 16],
    pub node: u64,
}
/// The epochs a session's directory descriptor carried when observed; a
/// change to any of them makes a plan built on it stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEpochs {
    pub route: u64,
    pub membership: u64,
    pub placement: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedSession {
    pub tenant: [u8; 16],
    pub session: [u8; 16],
    pub epochs: SessionEpochs,
    pub voters: Vec<u64>,
    /// The session's desired durability in the directory.
    pub desired: GuaranteeLevel,
    /// What the directory reports as achieved; absent while unknown.
    pub achieved: Option<GuaranteeLevel>,
    /// The pending placement's operation, when one is under way.
    pub pending: Option<[u8; 16]>,
    pub blocked_by: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedNode {
    pub node: u64,
    pub generation: u64,
    pub alive: bool,
    pub eligible: bool,
    pub disk_available: Option<u64>,
    /// The failure-domain labels the node announced (24 §22).
    pub region: Option<String>,
    pub zone: Option<String>,
}
/// The root group as observed (the audit's F24): its members and what its
/// voters tolerate of each failure class, measured by the directory as a
/// session's placement is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedControl {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub configuration_index: u64,
    pub tolerates_node: Option<u16>,
    pub tolerates_zone: Option<u16>,
    pub tolerates_region: Option<u16>,
    pub blocked_by: Vec<String>,
    /// The directory's partition groups, as the root's authority grants
    /// them (F24): the control plane is the root and these together.
    pub partitions: Vec<ObservedPartition>,
}
/// One partition group of the directory as observed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedPartition {
    pub partition: [u8; 16],
    pub group: [u8; 16],
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub tolerates_node: Option<u16>,
    pub tolerates_zone: Option<u16>,
    pub tolerates_region: Option<u16>,
    pub blocked_by: Vec<String>,
}
fn group_level(
    tolerates: Option<u16>,
    blocked_by: &[String],
    survive: FailureDomain,
) -> GuaranteeLevel {
    GuaranteeLevel {
        survive,
        max_failures: if blocked_by.is_empty() {
            tolerates.unwrap_or(0)
        } else {
            0
        },
    }
}
impl ObservedPartition {
    pub fn tolerates(&self, survive: FailureDomain) -> Option<u16> {
        match survive {
            FailureDomain::Node => self.tolerates_node,
            FailureDomain::Zone => self.tolerates_zone,
            FailureDomain::Region => self.tolerates_region,
        }
    }
    pub fn level(&self, survive: FailureDomain) -> GuaranteeLevel {
        group_level(self.tolerates(survive), &self.blocked_by, survive)
    }
}
impl ObservedControl {
    pub fn tolerates(&self, survive: FailureDomain) -> Option<u16> {
        match survive {
            FailureDomain::Node => self.tolerates_node,
            FailureDomain::Zone => self.tolerates_zone,
            FailureDomain::Region => self.tolerates_region,
        }
    }
    /// The level the root's voters provide in the class asked for: none
    /// where a voter's domain is unknown or a voter is blocked.
    pub fn root_level(&self, survive: FailureDomain) -> GuaranteeLevel {
        group_level(self.tolerates(survive), &self.blocked_by, survive)
    }
    /// The level the control plane provides: the weakest of the root's
    /// and every partition group's, since a session is placed and routed
    /// by both (F24).
    pub fn level(&self, survive: FailureDomain) -> GuaranteeLevel {
        self.partitions
            .iter()
            .map(|partition| partition.level(survive))
            .fold(self.root_level(survive), |weakest, level| {
                weakest.weaker(level)
            })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observed {
    pub policy_revision: u64,
    pub policy_hash: [u8; 32],
    pub policy: PolicyIntent,
    pub sessions: Vec<ObservedSession>,
    pub nodes: Vec<ObservedNode>,
    /// The root group, where the node observed it.
    pub control: Option<ObservedControl>,
}
/// One ordered change of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Change {
    /// Commit the requested policy as the next revision.
    CommitPolicy {
        from_revision: u64,
        to_revision: u64,
    },
    /// Seat the root group's voters the requested durability needs (F24):
    /// each one not voting yet is promoted through the root, one exact
    /// request each, once the root holds it as a learner and it has caught
    /// up. Before the sessions: a session's data is only as available as
    /// the metadata that routes to and places it.
    PlanRoot {
        voters: Vec<u64>,
        /// The root configuration the voters were planned against.
        expected_configuration_index: u64,
    },
    /// Seat a directory partition group's voters (F24), after the root and
    /// before the sessions: each planned voter the group does not hold is
    /// admitted as a learner, hosts a replica of the group once the root's
    /// grant seats it, and is promoted once it has caught up — one exact
    /// request each, through the node that leads the group.
    PlanPartition {
        partition: [u8; 16],
        group: [u8; 16],
        voters: Vec<u64>,
        /// The group's configuration the voters were planned against.
        expected_configuration_index: u64,
    },
    /// Request the session's placement under the requested durability; the
    /// operation is the exact identity the request denotes.
    PlanSession {
        tenant: [u8; 16],
        session: [u8; 16],
        durability: GuaranteeLevel,
        operation: [u8; 16],
        voters: Vec<u64>,
        expected: SessionEpochs,
        /// A plan for this request was already under way when observed.
        pending: bool,
    },
    /// The session's active placement already provides the durability.
    NoChange { tenant: [u8; 16], session: [u8; 16] },
}
/// A session the requested durability cannot be planned for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocked {
    pub tenant: [u8; 16],
    pub session: [u8; 16],
    pub reason: String,
}
/// Everything the plan identity covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanBody {
    pub deployment: DeploymentIdentity,
    pub observed: Observed,
    pub requested: PolicyIntent,
    pub policy_hash: [u8; 32],
    pub changes: Vec<Change>,
    /// What the sessions' data survives.
    pub guarantee: Guarantee,
    pub blocked: Vec<Blocked>,
    /// What the root group survives (F24): the control plane's own promise,
    /// stated beside the data's and never implied by it.
    pub control_guarantee: Guarantee,
    /// Why the root group cannot be seated under the request, when it cannot.
    pub blocked_control: Option<String>,
}
/// The answer a dry-run plan produced for one partition group (F24).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProposal {
    pub partition: [u8; 16],
    pub group: [u8; 16],
    pub proposal: ControlProposal,
}
/// The answer a dry-run root plan produced (`inspect placement`'s solver).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlProposal {
    Planned {
        voters: Vec<u64>,
        configuration_index: u64,
    },
    /// The root's voters already tolerate the requested failures.
    Satisfied,
    Refused(String),
    /// No directory to ask: the node's own facts were checked instead.
    Unobserved,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentPlan {
    pub schema: u16,
    /// Derived from the body alone: the same observation and request make
    /// the same plan whenever it is computed.
    pub plan_id: [u8; 16],
    pub created_ms: u64,
    /// When the placement agent last observed the directory (unix seconds);
    /// zero for a node without a directory.
    pub observed_at: i64,
    pub body: PlanBody,
}

/// The answer a dry-run placement request produced for one observed session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Proposal {
    Planned {
        operation: [u8; 16],
        voters: Vec<u64>,
    },
    Pending {
        operation: [u8; 16],
        voters: Vec<u64>,
    },
    Satisfied,
    Refused(String),
}
/// What planning observed, without any proposal yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub deployment: DeploymentIdentity,
    pub committed: CommittedPolicy,
    pub observed_at: i64,
    pub sessions: Vec<ObservedSession>,
    pub nodes: Vec<ObservedNode>,
    pub control: Option<ObservedControl>,
}

fn encode_body(body: &PlanBody) -> Result<Vec<u8>, DeploymentError> {
    postcard::to_stdvec(body).map_err(|error| DeploymentError::Encoding(error.to_string()))
}
pub fn plan_id(body: &PlanBody) -> Result<[u8; 16], DeploymentError> {
    let bytes = encode_body(body)?;
    let key = blake3::derive_key(PLAN_ID_CONTEXT, &bytes);
    let mut id = [0u8; 16];
    id.copy_from_slice(key.get(..16).ok_or(DeploymentError::Capacity)?);
    Ok(id)
}
pub fn level(intent: &PolicyIntent) -> GuaranteeLevel {
    GuaranteeLevel {
        survive: intent.durability.survive,
        max_failures: intent.durability.max_failures,
    }
}

/// Compose a plan: the policy commit first when the request differs from the
/// committed intent, then the root group's voters (`control`), then one
/// change per observed session in observation order (`proposals` answers
/// them in the same order). Sessions whose request was refused are listed
/// as blocked and the guarantee after the plan stays the guarantee before
/// it; a root that cannot be seated is `blocked_control` and the control
/// guarantee after stays what it was.
pub fn compose(
    observation: &Observation,
    requested: &PolicyIntent,
    proposals: &[Proposal],
    control: &ControlProposal,
    created_ms: u64,
) -> Result<DeploymentPlan, DeploymentError> {
    compose_with_partitions(observation, requested, proposals, control, &[], created_ms)
}
/// `compose`, with the directory's partition groups planned after the
/// root (F24): one change per partition group the solver seats, in the
/// order given; a group that cannot be seated blocks the control plane's
/// promise as a refused root does.
pub fn compose_with_partitions(
    observation: &Observation,
    requested: &PolicyIntent,
    proposals: &[Proposal],
    control: &ControlProposal,
    partitions: &[PartitionProposal],
    created_ms: u64,
) -> Result<DeploymentPlan, DeploymentError> {
    if observation.sessions.len() > MAX_SESSIONS
        || proposals.len() != observation.sessions.len()
        || partitions.len() > MAX_PARTITIONS
    {
        return Err(DeploymentError::Capacity);
    }
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(
            observation
                .sessions
                .len()
                .saturating_add(2)
                .saturating_add(partitions.len()),
        )
        .map_err(|_| DeploymentError::Capacity)?;
    let mut blocked = Vec::new();
    let committed = &observation.committed;
    let policy_hash = requested.hash()?;
    if committed.intent.differing_field(requested).is_some() {
        changes.push(Change::CommitPolicy {
            from_revision: committed.revision.0,
            to_revision: committed.revision.0.saturating_add(1),
        });
    }
    let target = level(requested);
    // The control plane: what the root's voters tolerate now in the class
    // asked for (the committed level where no root was observed, as for
    // the sessions), and what they will once seated.
    let control_before = observation.control.as_ref().map_or_else(
        || level(&committed.intent),
        |root| root.level(target.survive),
    );
    let mut blocked_control = None;
    match control {
        ControlProposal::Planned {
            voters,
            configuration_index,
        } => {
            let mut copied = Vec::new();
            copied
                .try_reserve_exact(voters.len())
                .map_err(|_| DeploymentError::Capacity)?;
            copied.extend_from_slice(voters);
            changes.push(Change::PlanRoot {
                voters: copied,
                expected_configuration_index: *configuration_index,
            });
        }
        ControlProposal::Satisfied | ControlProposal::Unobserved => {}
        ControlProposal::Refused(reason) => blocked_control = Some(reason.clone()),
    }
    for partition in partitions {
        match &partition.proposal {
            ControlProposal::Planned {
                voters,
                configuration_index,
            } => {
                let mut copied = Vec::new();
                copied
                    .try_reserve_exact(voters.len())
                    .map_err(|_| DeploymentError::Capacity)?;
                copied.extend_from_slice(voters);
                changes.push(Change::PlanPartition {
                    partition: partition.partition,
                    group: partition.group,
                    voters: copied,
                    expected_configuration_index: *configuration_index,
                });
            }
            ControlProposal::Satisfied | ControlProposal::Unobserved => {}
            ControlProposal::Refused(reason) => {
                if blocked_control.is_none() {
                    blocked_control =
                        Some(format!("partition {}: {reason}", hex(&partition.partition)));
                }
            }
        }
    }
    let control_after = if blocked_control.is_some() {
        control_before
    } else {
        target
    };
    let mut before = None;
    for (session, proposal) in observation.sessions.iter().zip(proposals) {
        let achieved = session.achieved.unwrap_or(GuaranteeLevel::NONE);
        before = Some(before.map_or(achieved, |level: GuaranteeLevel| level.weaker(achieved)));
        match proposal {
            Proposal::Satisfied => changes.push(Change::NoChange {
                tenant: session.tenant,
                session: session.session,
            }),
            Proposal::Planned { operation, voters } | Proposal::Pending { operation, voters } => {
                let mut copied = Vec::new();
                copied
                    .try_reserve_exact(voters.len())
                    .map_err(|_| DeploymentError::Capacity)?;
                copied.extend_from_slice(voters);
                changes.push(Change::PlanSession {
                    tenant: session.tenant,
                    session: session.session,
                    durability: target,
                    operation: *operation,
                    voters: copied,
                    expected: session.epochs,
                    pending: matches!(proposal, Proposal::Pending { .. }),
                });
            }
            Proposal::Refused(reason) => {
                blocked
                    .try_reserve_exact(1)
                    .map_err(|_| DeploymentError::Capacity)?;
                blocked.push(Blocked {
                    tenant: session.tenant,
                    session: session.session,
                    reason: reason.clone(),
                });
            }
        }
    }
    // Without sessions the committed policy is what the node runs at: the
    // startup solver validated it against the node's own facts.
    let before = before.unwrap_or_else(|| level(&committed.intent));
    let after = if blocked.is_empty() { target } else { before };
    let body = PlanBody {
        deployment: observation.deployment,
        observed: Observed {
            policy_revision: committed.revision.0,
            policy_hash: committed.hash,
            policy: committed.intent.clone(),
            sessions: observation.sessions.clone(),
            nodes: observation.nodes.clone(),
            control: observation.control.clone(),
        },
        requested: requested.clone(),
        policy_hash,
        changes,
        guarantee: Guarantee {
            before,
            during: before,
            after,
        },
        blocked,
        control_guarantee: Guarantee {
            before: control_before,
            during: control_before,
            after: control_after,
        },
        blocked_control,
    };
    Ok(DeploymentPlan {
        schema: SCHEMA,
        plan_id: plan_id(&body)?,
        created_ms,
        observed_at: observation.observed_at,
        body,
    })
}

impl DeploymentPlan {
    pub fn encode(&self) -> Result<Vec<u8>, DeploymentError> {
        let body = postcard::to_stdvec(self)
            .map_err(|error| DeploymentError::Encoding(error.to_string()))?;
        let total = MAGIC
            .len()
            .saturating_add(body.len())
            .saturating_add(blake3::OUT_LEN);
        if total > MAX_PLAN_BYTES {
            return Err(DeploymentError::Capacity);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total)
            .map_err(|_| DeploymentError::Capacity)?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&body);
        let hash = blake3::hash(&bytes);
        bytes.extend_from_slice(hash.as_bytes());
        Ok(bytes)
    }
    /// Decode and verify a plan: magic, trailer hash, schema and identity.
    pub fn decode(bytes: &[u8]) -> Result<Self, DeploymentError> {
        if bytes.len() > MAX_PLAN_BYTES {
            return Err(DeploymentError::Capacity);
        }
        let body_end = bytes
            .len()
            .checked_sub(blake3::OUT_LEN)
            .ok_or(DeploymentError::Corrupt("plan is shorter than its trailer"))?;
        let (prefix, trailer) = bytes
            .split_at_checked(body_end)
            .ok_or(DeploymentError::Corrupt("plan is shorter than its trailer"))?;
        if blake3::hash(prefix).as_bytes() != trailer {
            return Err(DeploymentError::Corrupt(
                "plan hash does not match its bytes",
            ));
        }
        let body = prefix
            .strip_prefix(MAGIC.as_slice())
            .ok_or(DeploymentError::Corrupt("not a plan file"))?;
        let (plan, tail): (Self, &[u8]) = postcard::take_from_bytes(body)
            .map_err(|_| DeploymentError::Corrupt("plan body does not decode"))?;
        if !tail.is_empty() {
            return Err(DeploymentError::Corrupt("trailing bytes after the plan"));
        }
        if plan.schema != SCHEMA {
            return Err(DeploymentError::Corrupt("unsupported plan schema"));
        }
        if plan.plan_id != plan_id(&plan.body)? {
            return Err(DeploymentError::Corrupt(
                "plan identity does not match its body",
            ));
        }
        if plan.body.changes.len()
            > MAX_SESSIONS
                .saturating_add(2)
                .saturating_add(MAX_PARTITIONS)
            || plan.body.observed.sessions.len() > MAX_SESSIONS
        {
            return Err(DeploymentError::Capacity);
        }
        Ok(plan)
    }
    /// The plan hash a journal binds to: the hash of the encoded artifact.
    pub fn hash(&self) -> Result<[u8; 32], DeploymentError> {
        Ok(*blake3::hash(&self.encode()?).as_bytes())
    }
    /// Whether the plan changes anything at all.
    pub fn is_empty(&self) -> bool {
        self.body
            .changes
            .iter()
            .all(|change| matches!(change, Change::NoChange { .. }))
    }
    pub fn view(&self) -> PlanView {
        PlanView {
            kind: "deployment_plan",
            plan_id: hex(&self.plan_id),
            created_ms: self.created_ms,
            observed_at: self.observed_at,
            deployment: DeploymentView {
                cluster: hex(&self.body.deployment.cluster),
                node: self.body.deployment.node,
            },
            observed: ObservedView {
                policy_revision: self.body.observed.policy_revision,
                policy_hash: hex(&self.body.observed.policy_hash),
                policy: self.body.observed.policy.clone(),
                sessions: self
                    .body
                    .observed
                    .sessions
                    .iter()
                    .map(|session| ObservedSessionView {
                        tenant: hex(&session.tenant),
                        session: hex(&session.session),
                        route_epoch: session.epochs.route,
                        membership_epoch: session.epochs.membership,
                        placement_epoch: session.epochs.placement,
                        voters: session.voters.clone(),
                        desired: LevelView::of(session.desired),
                        achieved: session.achieved.map(LevelView::of),
                        pending: session.pending.map(|operation| hex(&operation)),
                        blocked_by: session.blocked_by.clone(),
                    })
                    .collect(),
                nodes: self.body.observed.nodes.clone(),
                control: self
                    .body
                    .observed
                    .control
                    .as_ref()
                    .map(ObservedControlView::of),
            },
            requested: self.body.requested.clone(),
            policy_hash: hex(&self.body.policy_hash),
            changes: self.body.changes.iter().map(ChangeView::of).collect(),
            guarantee: GuaranteeView {
                before: LevelView::of(self.body.guarantee.before),
                during: LevelView::of(self.body.guarantee.during),
                after: LevelView::of(self.body.guarantee.after),
            },
            control_guarantee: GuaranteeView {
                before: LevelView::of(self.body.control_guarantee.before),
                during: LevelView::of(self.body.control_guarantee.during),
                after: LevelView::of(self.body.control_guarantee.after),
            },
            blocked_control: self.body.blocked_control.clone(),
            blocked: self
                .body
                .blocked
                .iter()
                .map(|blocked| BlockedView {
                    tenant: hex(&blocked.tenant),
                    session: hex(&blocked.session),
                    reason: blocked.reason.clone(),
                })
                .collect(),
        }
    }
}

/// The plan as operators read it: identities in hex, levels by name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlanView {
    pub kind: &'static str,
    pub plan_id: String,
    pub created_ms: u64,
    pub observed_at: i64,
    pub deployment: DeploymentView,
    pub observed: ObservedView,
    pub requested: PolicyIntent,
    pub policy_hash: String,
    pub changes: Vec<ChangeView>,
    pub guarantee: GuaranteeView,
    pub blocked: Vec<BlockedView>,
    pub control_guarantee: GuaranteeView,
    pub blocked_control: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeploymentView {
    pub cluster: String,
    pub node: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservedView {
    pub policy_revision: u64,
    pub policy_hash: String,
    pub policy: PolicyIntent,
    pub sessions: Vec<ObservedSessionView>,
    pub nodes: Vec<ObservedNode>,
    pub control: Option<ObservedControlView>,
}
/// The observed control plane, its identities in hex.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservedControlView {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub configuration_index: u64,
    pub tolerates_node: Option<u16>,
    pub tolerates_zone: Option<u16>,
    pub tolerates_region: Option<u16>,
    pub blocked_by: Vec<String>,
    pub partitions: Vec<ObservedPartitionView>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservedPartitionView {
    pub partition: String,
    pub group: String,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub tolerates_node: Option<u16>,
    pub tolerates_zone: Option<u16>,
    pub tolerates_region: Option<u16>,
    pub blocked_by: Vec<String>,
}
impl ObservedControlView {
    fn of(control: &ObservedControl) -> Self {
        Self {
            voters: control.voters.clone(),
            learners: control.learners.clone(),
            configuration_index: control.configuration_index,
            tolerates_node: control.tolerates_node,
            tolerates_zone: control.tolerates_zone,
            tolerates_region: control.tolerates_region,
            blocked_by: control.blocked_by.clone(),
            partitions: control
                .partitions
                .iter()
                .map(|partition| ObservedPartitionView {
                    partition: hex(&partition.partition),
                    group: hex(&partition.group),
                    voters: partition.voters.clone(),
                    learners: partition.learners.clone(),
                    tolerates_node: partition.tolerates_node,
                    tolerates_zone: partition.tolerates_zone,
                    tolerates_region: partition.tolerates_region,
                    blocked_by: partition.blocked_by.clone(),
                })
                .collect(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservedSessionView {
    pub tenant: String,
    pub session: String,
    pub route_epoch: u64,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    pub voters: Vec<u64>,
    pub desired: LevelView,
    pub achieved: Option<LevelView>,
    pub pending: Option<String>,
    pub blocked_by: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LevelView {
    pub survive: &'static str,
    pub max_failures: u16,
}
impl LevelView {
    pub fn of(level: GuaranteeLevel) -> Self {
        Self {
            survive: survive_name(level.survive),
            max_failures: level.max_failures,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GuaranteeView {
    pub before: LevelView,
    pub during: LevelView,
    pub after: LevelView,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum ChangeView {
    CommitPolicy {
        from_revision: u64,
        to_revision: u64,
    },
    PlanRoot {
        voters: Vec<u64>,
        expected_configuration_index: u64,
    },
    PlanPartition {
        partition: String,
        group: String,
        voters: Vec<u64>,
        expected_configuration_index: u64,
    },
    PlanSession {
        tenant: String,
        session: String,
        survive: &'static str,
        max_failures: u16,
        operation: String,
        voters: Vec<u64>,
        expected_route_epoch: u64,
        expected_membership_epoch: u64,
        expected_placement_epoch: u64,
        pending: bool,
    },
    NoChange {
        tenant: String,
        session: String,
    },
}
impl ChangeView {
    pub fn of(change: &Change) -> Self {
        match change {
            Change::CommitPolicy {
                from_revision,
                to_revision,
            } => Self::CommitPolicy {
                from_revision: *from_revision,
                to_revision: *to_revision,
            },
            Change::PlanRoot {
                voters,
                expected_configuration_index,
            } => Self::PlanRoot {
                voters: voters.clone(),
                expected_configuration_index: *expected_configuration_index,
            },
            Change::PlanPartition {
                partition,
                group,
                voters,
                expected_configuration_index,
            } => Self::PlanPartition {
                partition: hex(partition),
                group: hex(group),
                voters: voters.clone(),
                expected_configuration_index: *expected_configuration_index,
            },
            Change::PlanSession {
                tenant,
                session,
                durability,
                operation,
                voters,
                expected,
                pending,
            } => Self::PlanSession {
                tenant: hex(tenant),
                session: hex(session),
                survive: survive_name(durability.survive),
                max_failures: durability.max_failures,
                operation: hex(operation),
                voters: voters.clone(),
                expected_route_epoch: expected.route,
                expected_membership_epoch: expected.membership,
                expected_placement_epoch: expected.placement,
                pending: *pending,
            },
            Change::NoChange { tenant, session } => Self::NoChange {
                tenant: hex(tenant),
                session: hex(session),
            },
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockedView {
    pub tenant: String,
    pub session: String,
    pub reason: String,
}
