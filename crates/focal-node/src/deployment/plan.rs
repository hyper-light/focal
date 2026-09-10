//! The plan artifact (`FCLPLAN1`) and its composition from observations.
use super::{DeploymentError, Guarantee, GuaranteeLevel, hex, survive_name};
use crate::config::policy::{CommittedPolicy, PolicyIntent};
use serde::{Deserialize, Serialize};

/// `FCLPLAN1`: magic, postcard body, then the BLAKE3 hash of both.
pub const MAGIC: &[u8; 8] = b"FCLPLAN1";
pub const SCHEMA: u16 = 1;
/// A plan file never exceeds this; the directory view it is built from is
/// itself bounded.
pub const MAX_PLAN_BYTES: usize = 4 * 1024 * 1024;
/// Sessions one plan names at most.
pub const MAX_SESSIONS: usize = 4096;
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
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observed {
    pub policy_revision: u64,
    pub policy_hash: [u8; 32],
    pub policy: PolicyIntent,
    pub sessions: Vec<ObservedSession>,
    pub nodes: Vec<ObservedNode>,
}
/// One ordered change of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Change {
    /// Commit the requested policy as the next revision.
    CommitPolicy {
        from_revision: u64,
        to_revision: u64,
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
    pub guarantee: Guarantee,
    pub blocked: Vec<Blocked>,
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
/// committed intent, then one change per observed session in observation
/// order (`proposals` answers them in the same order). Sessions whose
/// request was refused are listed as blocked and the guarantee after the
/// plan stays the guarantee before it.
pub fn compose(
    observation: &Observation,
    requested: &PolicyIntent,
    proposals: &[Proposal],
    created_ms: u64,
) -> Result<DeploymentPlan, DeploymentError> {
    if observation.sessions.len() > MAX_SESSIONS || proposals.len() != observation.sessions.len() {
        return Err(DeploymentError::Capacity);
    }
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(observation.sessions.len().saturating_add(1))
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
        if plan.body.changes.len() > MAX_SESSIONS.saturating_add(1)
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
            },
            requested: self.body.requested.clone(),
            policy_hash: hex(&self.body.policy_hash),
            changes: self.body.changes.iter().map(ChangeView::of).collect(),
            guarantee: GuaranteeView {
                before: LevelView::of(self.body.guarantee.before),
                during: LevelView::of(self.body.guarantee.during),
                after: LevelView::of(self.body.guarantee.after),
            },
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
