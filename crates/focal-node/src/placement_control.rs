//! Node-to-node placement protocol. `PlacementControl` carries a partition
//! command an enrolled node may make about itself; `SessionSign` asks a node
//! for its signature over one session fact it can witness from its own
//! hosted replica. Neither grants planning, fencing or another node's facts.
use crate::{
    control_host::ControlHost,
    fleet::FleetManager,
    placement_proof::{AccountedAuthorityProof, PlacementProofError, ProofWindow},
};
use focal_control::{ControlCommand, ControlFailure, ControlRpc};
use focal_directory::{
    AssignmentPhase, AuthorityProof, PartitionOperation, SessionChange, SessionFenceKind,
};
use focal_ledger::SessionPlacementRequest;
use focal_wire::{MAX_PLACEMENT_CONTROL_REQUEST_BYTES, PeerRole, VerifiedRequest};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

/// One session fact a voter can witness from its own hosted replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionFact {
    /// A committed placement record, answered exactly by the replica.
    Placement(SessionPlacementRequest),
    /// The group's next membership as the replica applied it.
    Membership {
        next: focal_directory::GroupAuthorityGrant,
        record: crate::placement_proof::MembershipRecord,
    },
}
/// The body of `Operation::SessionSign`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSignRequest {
    pub schema: u16,
    pub fact: SessionFact,
    pub window: ProofWindow,
}
/// The reply carried in `Response::Control` for `SessionSign`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionSignReply {
    Signed(Box<AuthorityProof>),
    Refused(ControlFailure),
}

/// Decode a `PlacementControl` body for the authenticated Node peer. Reads
/// are the bounded discovery selectors; a submit must be a verified partition
/// command about the peer itself.
pub(crate) fn decode_placement_control(
    verified: &VerifiedRequest,
    bytes: &[u8],
) -> Result<ControlRpc, ControlFailure> {
    let PeerRole::Node { node_id } = verified.peer().role() else {
        return Err(ControlFailure::Unauthorized);
    };
    if bytes.is_empty() || bytes.len() > MAX_PLACEMENT_CONTROL_REQUEST_BYTES {
        return Err(ControlFailure::Capacity);
    }
    let (tag, _) = postcard::take_from_bytes::<u32>(bytes).map_err(|_| ControlFailure::Invalid)?;
    match tag {
        1 => ControlRpc::decode_read_only(bytes, MAX_PLACEMENT_CONTROL_REQUEST_BYTES)
            .map(ControlRpc::Read)
            .map_err(|_| ControlFailure::Unauthorized),
        0 => {
            let rpc = ControlRpc::decode(bytes, MAX_PLACEMENT_CONTROL_REQUEST_BYTES)
                .map_err(ControlFailure::from)?;
            let ControlRpc::Submit(request) = &rpc else {
                return Err(ControlFailure::Unauthorized);
            };
            // The request client is the sender's enrolled principal, or the
            // client its root intents are named by (distinct from its
            // partition intents, which share this owner's receipt space).
            let principal = verified.peer().principal().0;
            if request.id.client != principal && request.id.client != root_intent_client(principal)
            {
                return Err(ControlFailure::Unauthorized);
            }
            // A node bootstraps the group of a session it created alone: a
            // grant naming only itself (24 §16); every other root authority
            // change is the controller's.
            if let ControlCommand::Authority(focal_directory::AuthorityCommand {
                operation: focal_directory::AuthorityOperation::BootstrapGroup { grant },
                ..
            }) = &request.command
            {
                let own = matches!(grant.scope, focal_directory::GroupScope::Session(_))
                    && grant.membership_epoch == 1
                    && grant.voters.len() == 1
                    && grant.voters.contains_key(&node_id)
                    && grant.outgoing_voters.is_empty()
                    && grant.learners.is_empty();
                if !own {
                    return Err(ControlFailure::Unauthorized);
                }
                return Ok(rpc);
            }
            let ControlCommand::VerifiedPartition(command) = &request.command else {
                return Err(ControlFailure::Unauthorized);
            };
            let own = match &command.command.operation {
                PartitionOperation::Enroll { node, .. } => node.node == node_id,
                PartitionOperation::ReportLoad { load } => load.node == node_id,
                // A verdict about a node is the partition leader's own
                // detector's to commit, never a node's word over the wire.
                PartitionOperation::Liveness { .. }
                | PartitionOperation::SealForSplit { .. }
                | PartitionOperation::Release { .. }
                | PartitionOperation::Absorb { .. }
                | PartitionOperation::Install { .. } => false,
                PartitionOperation::Session { change, .. } => match change {
                    SessionChange::Ready { ready } => ready.node == node_id,
                    // A node reports its own installation and, from its own
                    // applied and committed indexes, its catch-up; custody is
                    // proven by readiness and promotion decided by the controller.
                    SessionChange::Progress { progress, .. } => {
                        progress.node == node_id
                            && matches!(
                                progress.phase,
                                AssignmentPhase::Assigned
                                    | AssignmentPhase::Installed
                                    | AssignmentPhase::CaughtUp
                            )
                    }
                    _ => false,
                },
                // A node registers a session it created alone: a placement
                // whose only voter, materializer and copy is the sender, under
                // the creation fence the partition verifies by proof (24 §16).
                PartitionOperation::CreateSession {
                    placement,
                    authority,
                    ..
                } => {
                    authority.kind == SessionFenceKind::Created
                        && placement.placement.voters.len() == 1
                        && placement.placement.voters.contains_key(&node_id)
                        && placement
                            .placement
                            .materializers
                            .keys()
                            .all(|member| *member == node_id)
                        && placement
                            .placement
                            .content_copies
                            .keys()
                            .all(|member| *member == node_id)
                }
                PartitionOperation::SealForTransfer { .. } => false,
            };
            if !own {
                return Err(ControlFailure::Unauthorized);
            }
            Ok(rpc)
        }
        _ => Err(ControlFailure::Unauthorized),
    }
}

/// The client a node's root intents are named by when it submits them
/// through the root leader (24 §16): derived from its enrolled principal so
/// it never collides with the partition intents the same owner retains.
pub fn root_intent_client(principal: [u8; 16]) -> [u8; 16] {
    let hash = blake3::derive_key("focal.placement.root-intents.v1", &principal);
    let mut value = [0; 16];
    for (target, source) in value.iter_mut().zip(hash) {
        *target = source;
    }
    value
}
/// One signing job handed to the node's credential owner.
pub struct SignJob {
    pub permit: crate::placement_proof::SessionProofPermit,
    pub reply: oneshot::Sender<Result<AccountedAuthorityProof, PlacementProofError>>,
}
/// Assemble a voter-majority proof of one session fact.
pub struct CollectJob {
    pub request: CollectRequest,
    pub reply: oneshot::Sender<Result<AuthorityProof, crate::placement_collect::CollectError>>,
}
/// One statement to be signed by a majority of a session group's voters.
#[derive(Debug, Clone)]
pub struct CollectRequest {
    pub ledger: focal_model::LedgerId,
    pub group: [u8; 16],
    pub voters: Vec<u64>,
    pub fact: SessionFact,
    pub window: ProofWindow,
}
/// A bounded diagnostic view of the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentStatus {
    pub node: u64,
    pub root_intents: u64,
    pub partition_intents: u64,
    pub installed: Vec<focal_model::LedgerId>,
    pub last_error: Option<String>,
    /// The last intent the owner refused before admission (24 §7).
    pub last_refusal: Option<String>,
    /// The tenants this node hosts, their queues, and the node's memory and
    /// volume envelopes.
    pub admission: crate::admission::AdmissionReport,
}
pub enum AgentJob {
    Sign(Box<SignJob>),
    Collect(Box<CollectJob>),
    Status(oneshot::Sender<AgentStatus>),
    /// The node's renewed credential: every later signature uses it.
    Credentials(Box<focal_enrollment::CredentialMaterial>),
    /// Every partition the agent last observed, for the operator's view.
    Directory(oneshot::Sender<DirectoryReport>),
    /// Create (or find again) an application session on this node.
    CreateSession(Box<CreateSessionJob>),
    /// Plan a session's placement under a requested durability.
    PlanSession(Box<PlanSessionJob>),
    /// Move one member of a session's range group to a node (25 §6).
    MoveRange(Box<MoveRangeJob>),
    /// Restore a session from a verified backup onto this node (26 §6).
    RestoreSession(Box<RestoreSessionJob>),
}
/// One operator request to restore a session from a backup.
pub struct RestoreSessionJob {
    pub request: crate::backup::RestoreRequest,
    pub reply:
        oneshot::Sender<Result<crate::backup::RestoredSession, crate::placement_agent::AgentError>>,
}
/// One operator request to move a member: answered by the authority's next
/// controller pass over the session, which knows the node's generation.
pub struct MoveRangeJob {
    pub ledger: focal_model::LedgerId,
    pub member: focal_memory::RangeId,
    pub node: u64,
    pub reply:
        oneshot::Sender<Result<focal_ranges::TransferId, crate::placement_agent::AgentError>>,
}
/// One operator request to place a session under a durability: answered by
/// the agent's next pass over the partition holding the session.
pub struct PlanSessionJob {
    pub ledger: focal_model::LedgerId,
    pub durability: focal_directory::DurabilityIntent,
    /// Propose and report without journaling a plan (doc 08 §9).
    pub dry_run: bool,
    pub reply: oneshot::Sender<Result<PlannedSession, crate::placement_agent::AgentError>>,
}
/// How a plan request was answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanState {
    /// A plan under the requested durability was journaled for the partition.
    Planned,
    /// The session already has a pending plan; this is it.
    Pending,
    /// The active placement already provides the requested durability.
    Satisfied,
}
/// The plan an operator's request denotes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedSession {
    pub ledger: focal_model::LedgerId,
    pub operation: focal_directory::OperationId,
    pub voters: Vec<u64>,
    pub state: PlanState,
}
/// One application session to create on this node ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §16).
pub struct CreateSessionJob {
    pub tenant: focal_model::TenantId,
    pub name: String,
    pub reply: oneshot::Sender<Result<CreatedSession, crate::placement_agent::AgentError>>,
}
/// The session a create request names, whether this call opened it or an
/// earlier one did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedSession {
    pub ledger: focal_model::LedgerId,
    pub group: [u8; 16],
    pub node: u64,
    /// The session already existed on this node: an exact retry.
    pub existing: bool,
}
/// The longest session name a create request carries.
pub const MAX_SESSION_NAME: usize = 128;
/// The session identity a name denotes within a cluster and tenant: the
/// same name is the same session, so a repeated request is an exact retry.
pub fn created_session_id(
    cluster: [u8; 16],
    tenant: focal_model::TenantId,
    name: &str,
) -> Option<focal_model::SessionId> {
    if name.is_empty() || name.len() > MAX_SESSION_NAME || tenant.is_zero() {
        return None;
    }
    let mut hasher = blake3::Hasher::new_derive_key("focal.session.created.v1");
    hasher.update(&cluster);
    hasher.update(&tenant.0);
    hasher.update(&u32::try_from(name.len()).ok()?.to_le_bytes());
    hasher.update(name.as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(hasher.finalize().as_bytes().get(..16)?);
    if id == [0; 16] {
        return None;
    }
    Some(focal_model::SessionId(id))
}
/// The partitions the agent acts on as it last observed them, with the
/// root's delegation of each.
#[derive(Debug, Clone, Default)]
pub struct DirectoryReport {
    pub observed_at: i64,
    pub partitions: Vec<(
        focal_directory::Delegation,
        focal_directory::PartitionCheckpoint,
    )>,
}
/// A bounded handle to the placement agent; every clone shares one queue.
#[derive(Clone)]
pub struct PlacementHandle {
    sender: mpsc::Sender<AgentJob>,
}
impl PlacementHandle {
    pub fn channel(depth: usize) -> (Self, mpsc::Receiver<AgentJob>) {
        let (sender, receiver) = mpsc::channel(depth.max(1));
        (Self { sender }, receiver)
    }
    fn send(&self, job: AgentJob) -> Result<(), PlacementProofError> {
        self.sender.try_send(job).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => PlacementProofError::Capacity,
            mpsc::error::TrySendError::Closed(_) => PlacementProofError::Unavailable,
        })
    }
    /// Hand the agent the node's renewed credential.
    pub fn replace_credentials(
        &self,
        material: focal_enrollment::CredentialMaterial,
    ) -> Result<(), PlacementProofError> {
        self.send(AgentJob::Credentials(Box::new(material)))
    }
    /// Create an application session on this node, or find the one the same
    /// name already denotes.
    pub async fn create_session(
        &self,
        tenant: focal_model::TenantId,
        name: String,
    ) -> Result<CreatedSession, crate::placement_agent::AgentError> {
        let (reply, receive) = oneshot::channel();
        self.send(AgentJob::CreateSession(Box::new(CreateSessionJob {
            tenant,
            name,
            reply,
        })))
        .map_err(|error| match error {
            PlacementProofError::Capacity => crate::placement_agent::AgentError::Capacity,
            _ => crate::placement_agent::AgentError::Stopped,
        })?;
        receive
            .await
            .map_err(|_| crate::placement_agent::AgentError::Stopped)?
    }
    /// Restore a session from a verified backup onto this node (26 §6).
    pub async fn restore_session(
        &self,
        request: crate::backup::RestoreRequest,
    ) -> Result<crate::backup::RestoredSession, crate::placement_agent::AgentError> {
        let (reply, receive) = oneshot::channel();
        self.send(AgentJob::RestoreSession(Box::new(RestoreSessionJob {
            request,
            reply,
        })))
        .map_err(|error| match error {
            PlacementProofError::Capacity => crate::placement_agent::AgentError::Capacity,
            _ => crate::placement_agent::AgentError::Stopped,
        })?;
        receive
            .await
            .map_err(|_| crate::placement_agent::AgentError::Stopped)?
    }
    /// Plan a session's placement under a durability; answered by the agent's
    /// next pass over the partition holding the session.
    pub async fn plan_session(
        &self,
        ledger: focal_model::LedgerId,
        durability: focal_directory::DurabilityIntent,
        dry_run: bool,
    ) -> Result<PlannedSession, crate::placement_agent::AgentError> {
        let (reply, receive) = oneshot::channel();
        self.send(AgentJob::PlanSession(Box::new(PlanSessionJob {
            ledger,
            durability,
            dry_run,
            reply,
        })))
        .map_err(|error| match error {
            PlacementProofError::Capacity => crate::placement_agent::AgentError::Capacity,
            _ => crate::placement_agent::AgentError::Stopped,
        })?;
        receive
            .await
            .map_err(|_| crate::placement_agent::AgentError::Stopped)?
    }
    /// Move one member of a session's range group to `node` (25 §6); the
    /// reply names the transfer the request denotes.
    pub async fn move_range(
        &self,
        ledger: focal_model::LedgerId,
        member: focal_memory::RangeId,
        node: u64,
    ) -> Result<focal_ranges::TransferId, crate::placement_agent::AgentError> {
        let (reply, receive) = oneshot::channel();
        self.send(AgentJob::MoveRange(Box::new(MoveRangeJob {
            ledger,
            member,
            node,
            reply,
        })))
        .map_err(|error| match error {
            PlacementProofError::Capacity => crate::placement_agent::AgentError::Capacity,
            _ => crate::placement_agent::AgentError::Stopped,
        })?;
        receive
            .await
            .map_err(|_| crate::placement_agent::AgentError::Stopped)?
    }
    /// The partitions as the agent last observed them.
    pub async fn directory(&self) -> Result<DirectoryReport, PlacementProofError> {
        let (reply, receive) = oneshot::channel();
        self.send(AgentJob::Directory(reply))?;
        receive.await.map_err(|_| PlacementProofError::Unavailable)
    }
    /// The agent's current diagnostic view.
    pub async fn status(&self) -> Result<AgentStatus, PlacementProofError> {
        let (reply, receive) = oneshot::channel();
        self.send(AgentJob::Status(reply))?;
        receive.await.map_err(|_| PlacementProofError::Unavailable)
    }
    /// Sign a permit with the node credential.
    pub async fn sign(
        &self,
        permit: crate::placement_proof::SessionProofPermit,
    ) -> Result<AccountedAuthorityProof, PlacementProofError> {
        let (reply, receive) = oneshot::channel();
        self.send(AgentJob::Sign(Box::new(SignJob { permit, reply })))?;
        receive
            .await
            .map_err(|_| PlacementProofError::Unavailable)?
    }
    /// Collect a voter-majority proof of one session fact: this node signs
    /// locally when it is a voter, every other voter over `SessionSign`.
    pub async fn collect(
        &self,
        request: CollectRequest,
    ) -> Result<AuthorityProof, crate::placement_collect::CollectError> {
        let (reply, receive) = oneshot::channel();
        self.send(AgentJob::Collect(Box::new(CollectJob { request, reply })))
            .map_err(crate::placement_collect::CollectError::Proof)?;
        receive.await.map_err(|_| {
            crate::placement_collect::CollectError::Proof(PlacementProofError::Unavailable)
        })?
    }
}

/// Sign one session fact this node witnesses from its own hosted replica of
/// the ledger: a placement record is answered exactly by the replica, a
/// membership is checked against the configuration the replica applied; the
/// root owner validates the fact against the installed authority and the
/// credential owner signs.
pub async fn sign_session_fact(
    fleet: &FleetManager,
    control: &ControlHost,
    signer: &PlacementHandle,
    ledger: focal_model::LedgerId,
    fact: SessionFact,
    window: ProofWindow,
) -> Result<AccountedAuthorityProof, PlacementProofError> {
    let permit = prepare_session_fact(fleet, control, ledger, fact, window).await?;
    signer.sign(permit).await
}

/// Witness `fact` locally and prepare the permit; the caller signs it.
pub async fn prepare_session_fact(
    fleet: &FleetManager,
    control: &ControlHost,
    ledger: focal_model::LedgerId,
    fact: SessionFact,
    window: ProofWindow,
) -> Result<crate::placement_proof::SessionProofPermit, PlacementProofError> {
    let host = fleet
        .current_host(ledger)
        .map_err(|_| PlacementProofError::Unauthorized)?;
    let owner_error = |error: focal_ledger::LedgerError| match error {
        focal_ledger::LedgerError::Capacity => PlacementProofError::Capacity,
        focal_ledger::LedgerError::NotReady { .. } | focal_ledger::LedgerError::OutcomeUnknown => {
            PlacementProofError::Unavailable
        }
        _ => PlacementProofError::Unauthorized,
    };
    match fact {
        SessionFact::Placement(request) => {
            request
                .validate()
                .map_err(|_| PlacementProofError::Unauthorized)?;
            let witness = host
                .placement_witness(request)
                .await
                .map_err(owner_error)?
                .into_witness();
            control.prepare_session_proof(witness, window).await
        }
        SessionFact::Membership { next, record } => {
            // The replica's applied configuration, read on its owner thread:
            // a follower witnesses committed changes without a quorum read.
            let facts = host
                .registration_facts()
                .await
                .map_err(owner_error)?
                .value()
                .clone();
            let view = facts.membership;
            let configuration = &view.configuration;
            let same = |applied: &[u64], granted: &std::collections::BTreeMap<u64, u64>| {
                applied.iter().copied().eq(granted.keys().copied())
            };
            let latest = view
                .latest
                .as_ref()
                .ok_or(PlacementProofError::Unauthorized)?;
            if next.group.0 != ledger.session.0
                || !same(&configuration.voters, &next.voters)
                || !same(&configuration.voters_outgoing, &next.outgoing_voters)
                || !same(&configuration.learners, &next.learners)
                || !configuration.learners_next.is_empty()
                || configuration.auto_leave
                || latest.index != record.index.0
                || latest.term != record.term.0
                || latest.request_hash != record.record_hash.0
                || latest.configuration != *configuration
            {
                return Err(PlacementProofError::Unauthorized);
            }
            control.prepare_membership_proof(next, record, window).await
        }
    }
}
