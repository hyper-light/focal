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
use focal_directory::{AssignmentPhase, AuthorityProof, PartitionOperation, SessionChange};
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
            if request.id.client != verified.peer().principal().0 {
                return Err(ControlFailure::Unauthorized);
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
                PartitionOperation::SealForTransfer { .. }
                | PartitionOperation::CreateSession { .. } => false,
            };
            if !own {
                return Err(ControlFailure::Unauthorized);
            }
            Ok(rpc)
        }
        _ => Err(ControlFailure::Unauthorized),
    }
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
