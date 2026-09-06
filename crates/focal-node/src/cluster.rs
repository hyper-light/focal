//! Founding root metadata and enrollment owner. Public decisions use a distinct
//! Raft group on the node's shared WAL; private CA/draft custody stays local.
use crate::embedded::{EmbeddedNode, NodeIdentity, atomic_file, durable_dir, read_bounded};
use focal_consensus::NodeConfig;
use focal_control::*;
use focal_directory::{AuthorityVerifier, DirectoryError, RootConfig, RootDirectory};
use focal_enrollment::*;
use focal_log::SharedWal;
use focal_memory::MemoryBudget;
use focal_model::RequestId;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::mpsc,
    thread::JoinHandle,
};
use tokio::sync::oneshot;

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("cluster metadata: {0}")]
    Control(#[from] ControlError),
    #[error("cluster enrollment: {0}")]
    Enrollment(#[from] EnrollmentError),
    #[error("cluster disk operation: {0}")]
    Io(#[from] std::io::Error),
    #[error("cluster initialization: {0}")]
    Node(#[from] crate::embedded::NodeError),
    #[error("cluster bootstrap is corrupt or belongs to another owner")]
    Bootstrap,
    #[error("operation outcome is unknown; retry its original request identity")]
    OutcomeUnknown,
    #[error("cluster bootstrap owner requires its original single-voter root group")]
    ReplicatedRoot,
    #[error("invitation request ID conflicts with its saved intent")]
    IntentConflict,
}
/// This owner exposes enrollment only. It cannot manufacture infrastructure,
/// session-fence or custody attestations for directory commands.
pub(crate) struct NoDirectoryAuthority;
impl AuthorityVerifier for NoDirectoryAuthority {
    fn verify_enrollment(&self, _: &focal_directory::NodeEnrollment) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_session_fence(
        &self,
        _: &focal_directory::SessionFence,
    ) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_replica_ready(
        &self,
        _: &focal_directory::ReplicaReady,
    ) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
    fn verify_delegation(
        &self,
        _: &focal_directory::DelegationFence,
    ) -> Result<(), DirectoryError> {
        Err(DirectoryError::UnverifiedAuthority)
    }
}
#[derive(Serialize, Deserialize)]
struct Genesis {
    schema: u16,
    cluster: ClusterId,
    founder: u64,
    group: [u8; 16],
    bootstrap: ControlBootstrap,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InviteIntent {
    /// Exact reachability delivered to the joiner; never learned from redirects.
    pub endpoint: String,
    pub role: EnrollmentRole,
    pub lifetime_seconds: u64,
}
pub struct RootEnrollment {
    authority: BootstrapAuthority,
    control: ControlReplica,
    staging: PathBuf,
    server_name: String,
    failed: bool,
}
impl RootEnrollment {
    pub fn for_node(node: &EmbeddedNode, now: i64) -> Result<Self, ClusterError> {
        Self::open(node.root(), &node.identity, node.wal.clone(), now)
    }
    pub fn open(
        root: &Path,
        identity: &NodeIdentity,
        wal: SharedWal,
        now: i64,
    ) -> Result<Self, ClusterError> {
        let directory = root.join("cluster");
        durable_dir(&directory)?;
        let server_name = format!("cluster-{}.focal.internal", hex(&identity.cluster));
        let authority = BootstrapAuthority::open_or_create(
            directory.join("authority"),
            identity.cluster,
            vec![server_name.clone()],
            now,
        )?;
        let group = group_id(identity.cluster);
        let budget = MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024)
            .map_err(|_| ClusterError::Bootstrap)?;
        let path = directory.join("BOOTSTRAP");
        let marker = directory.join("BOOTSTRAP.initialized");
        let genesis = if path.exists() {
            let bytes = read_bounded(&path, 8 * 1024 * 1024)?;
            let payload = bytes.get(40..).ok_or(ClusterError::Bootstrap)?;
            if bytes.get(..8) != Some(b"FCLROOT1".as_slice())
                || Some(blake3::hash(payload).as_bytes().as_slice()) != bytes.get(8..40)
            {
                return Err(ClusterError::Bootstrap);
            }
            postcard::from_bytes::<Genesis>(payload).map_err(|_| ClusterError::Bootstrap)?
        } else {
            if marker.exists() {
                return Err(ClusterError::Bootstrap);
            }
            // The existing founding node ID is reserved. A newly enrolled peer
            // must never receive that identity merely because it joined first.
            let next_node = identity
                .node
                .checked_add(1)
                .ok_or(ClusterError::Bootstrap)?;
            let registry = EnrollmentRegistry::new(
                identity.cluster,
                authority.ca_certificate().to_vec(),
                next_node,
                EnrollmentLimits::default(),
            )?;
            let directory = RootDirectory::new(
                focal_directory::ClusterId(identity.cluster),
                RootConfig::default(),
                budget.clone(),
            )
            .map_err(|_| ClusterError::Bootstrap)?;
            let genesis = Genesis {
                schema: 1,
                cluster: identity.cluster,
                founder: identity.node,
                group,
                bootstrap: ControlBootstrap::root(&directory, &registry)?,
            };
            let bytes = postcard::to_stdvec(&genesis).map_err(|_| ClusterError::Bootstrap)?;
            let mut framed = b"FCLROOT1".to_vec();
            framed.extend_from_slice(blake3::hash(&bytes).as_bytes());
            framed.extend_from_slice(&bytes);
            atomic_file(&path, &framed)?;
            genesis
        };
        if genesis.schema != 1
            || genesis.cluster != identity.cluster
            || genesis.founder != identity.node
            || genesis.group != group
        {
            return Err(ClusterError::Bootstrap);
        }
        if !marker.exists() {
            atomic_file(&marker, b"initialized")?;
        }
        let mut control = ControlReplica::open_on_wal(
            ControlOptions::new(NodeConfig::single(identity.node, identity.cluster, group)),
            genesis.bootstrap,
            budget,
            wal,
        )?;
        control.drain(&NoDirectoryAuthority)?;
        if control
            .enrollment()
            .is_none_or(|registry| registry.ca_certificate() != authority.ca_certificate())
        {
            return Err(ClusterError::Bootstrap);
        }
        let status = control.status();
        if status.voters != [identity.node] || !status.learners.is_empty() {
            return Err(ClusterError::ReplicatedRoot);
        }
        control.campaign()?;
        for _ in 0..4 {
            if !control.drain(&NoDirectoryAuthority)?.messages.is_empty() {
                return Err(ClusterError::ReplicatedRoot);
            }
        }
        let staging = directory.join("invitations");
        durable_dir(&staging)?;
        Ok(Self {
            authority,
            control,
            staging,
            server_name,
            failed: false,
        })
    }
    pub fn server_identity(&self) -> CredentialMaterial {
        self.authority.server_identity()
    }
    pub fn server_name(&self) -> &str {
        &self.server_name
    }
    pub fn registry(&self) -> Result<&EnrollmentRegistry, ClusterError> {
        if self.failed {
            return Err(ClusterError::OutcomeUnknown);
        }
        self.control.enrollment().ok_or(ClusterError::Bootstrap)
    }
    pub fn invite(
        &mut self,
        request: RequestId,
        intent: &InviteIntent,
        now: i64,
    ) -> Result<Invitation, ClusterError> {
        if self.failed {
            return Err(ClusterError::OutcomeUnknown);
        }
        if request.is_zero()
            || !intent
                .endpoint
                .parse::<std::net::SocketAddr>()
                .is_ok_and(|address| address.port() != 0 && !address.ip().is_unspecified())
        {
            return Err(ClusterError::Bootstrap);
        }
        let encoded = postcard::to_stdvec(intent).map_err(|_| ClusterError::Bootstrap)?;
        let intent_hash = blake3::derive_key("focal.cluster.invite-intent.v1", &encoded);
        let path = self.staging.join(hex(&request.0));
        let pending = if path.join("invitation.bin").exists()
            || path.join("invitation.bin.initialized").exists()
        {
            PendingInvitation::open(&path, self.authority.cluster())?
        } else {
            let expires_at = now
                .checked_add(
                    i64::try_from(intent.lifetime_seconds).map_err(|_| ClusterError::Bootstrap)?,
                )
                .ok_or(ClusterError::Bootstrap)?;
            self.registry()?
                .prepare_invitation(
                    &self.authority,
                    InviteOptions {
                        endpoint: intent.endpoint.clone(),
                        server_name: self.server_name.clone(),
                        role: intent.role,
                        expires_at,
                    },
                    now,
                )?
                .persist(&path, intent_hash)?
        };
        if pending.intent_hash() != intent_hash {
            return Err(ClusterError::IntentConflict);
        }
        self.commit(pending.id(), 1, pending.command().clone())?;
        Ok(pending.release(self.registry()?)?)
    }
    pub fn redeem(
        &mut self,
        request: &JoinRequest,
        now: i64,
    ) -> Result<EnrollmentReceipt, ClusterError> {
        let preparation = self
            .registry()?
            .prepare_join(&self.authority, request, now)?;
        if let JoinPreparation::Commit(command) = preparation {
            self.commit(request.invitation_id(), 2, command)?;
        }
        Ok(self.registry()?.release(request, now)?)
    }
    pub fn revoke(&mut self, invitation: InvitationId, now: i64) -> Result<(), ClusterError> {
        if self.registry()?.invitation_revoked(invitation)? {
            return Ok(());
        }
        let client = control_client(invitation);
        let consumed = self
            .control
            .receipt(ControlRequestId {
                client,
                sequence: 2,
            })?
            .is_some();
        let command = self.registry()?.prepare_revoke(invitation, now)?;
        self.commit(invitation, if consumed { 3 } else { 2 }, command)?;
        Ok(())
    }
    fn commit(
        &mut self,
        invitation: InvitationId,
        sequence: u64,
        command: EnrollmentCommand,
    ) -> Result<(), ClusterError> {
        let request = ControlRequest {
            id: ControlRequestId {
                client: control_client(invitation),
                sequence,
            },
            acknowledged_through: 0,
            command: ControlCommand::Enrollment(command),
        };
        let result = (|| -> Result<(), ClusterError> {
            if let ControlSubmission::Existing(_) = self
                .control
                .submit(request.clone(), &NoDirectoryAuthority)?
            {
                return Ok(());
            }
            for _ in 0..4 {
                let events = self.control.drain(&NoDirectoryAuthority)?;
                if !events.messages.is_empty() {
                    return Err(ClusterError::OutcomeUnknown);
                }
                if self.control.receipt(request.id)?.is_some() {
                    return Ok(());
                }
            }
            Err(ClusterError::OutcomeUnknown)
        })();
        // An ambiguous control write forbids another private preparation from
        // replacing its pending identity. Reopen and resolve the original log.
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn checkpoint(&mut self) -> Result<(), ClusterError> {
        self.control.checkpoint()?;
        Ok(())
    }
}
fn group_id(cluster: ClusterId) -> [u8; 16] {
    let h = blake3::derive_key("focal.cluster.root-group.v1", &cluster);
    first_half(h)
}
fn control_client(invitation: InvitationId) -> [u8; 16] {
    let h = blake3::derive_key("focal.cluster.invitation-client.v1", &invitation);
    first_half(h)
}
fn first_half(bytes: [u8; 32]) -> [u8; 16] {
    let mut id = [0; 16];
    for (target, source) in id.iter_mut().zip(bytes) {
        *target = source;
    }
    id
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

enum Work {
    Redeem(JoinRequest, oneshot::Sender<JoinResponse>),
    Stop(oneshot::Sender<Result<(), ClusterError>>),
}
/// Only the invitation redemption surface is network-facing. Operator invitation
/// creation requires direct access to the trusted root owner before spawning.
#[derive(Clone)]
pub struct EnrollmentHost {
    sender: mpsc::SyncSender<Work>,
}
pub struct EnrollmentOwner(JoinHandle<()>);
impl EnrollmentOwner {
    pub fn join(self) -> Result<(), ClusterError> {
        self.0.join().map_err(|_| ClusterError::OutcomeUnknown)
    }
}
impl EnrollmentHost {
    pub fn spawn(mut root: RootEnrollment) -> Result<(Self, EnrollmentOwner), ClusterError> {
        let (sender, receiver) = mpsc::sync_channel(16);
        let thread = std::thread::Builder::new()
            .name("focal-enrollment-owner".into())
            .spawn(move || {
                while let Ok(work) = receiver.recv() {
                    match work {
                        Work::Redeem(request, response) => {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .ok()
                                .and_then(|t| i64::try_from(t.as_secs()).ok());
                            let result = now
                                .ok_or(ClusterError::Bootstrap)
                                .and_then(|now| root.redeem(&request, now));
                            let result = match result {
                                Ok(receipt) => JoinResponse::Enrolled(receipt),
                                Err(ClusterError::Enrollment(error)) => {
                                    JoinResponse::Rejected(JoinFailure::from(&error))
                                }
                                Err(_) => JoinResponse::Rejected(JoinFailure::OutcomeUnknown),
                            };
                            let _ = response.send(result);
                        }
                        Work::Stop(response) => {
                            let _ = response.send(root.checkpoint());
                            break;
                        }
                    }
                }
            })?;
        Ok((Self { sender }, EnrollmentOwner(thread)))
    }
    /// A full queue returns Capacity without enqueueing; drain ingress and retry.
    pub async fn stop(&self) -> Result<(), ClusterError> {
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Stop(send))
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => ClusterError::Enrollment(EnrollmentError::Capacity),
                mpsc::TrySendError::Disconnected(_) => ClusterError::OutcomeUnknown,
            })?;
        receive.await.map_err(|_| ClusterError::OutcomeUnknown)?
    }
}
impl JoinHandler for EnrollmentHost {
    fn handle(&self, request: JoinRequest) -> JoinFuture<'_> {
        Box::pin(async move {
            let (send, receive) = oneshot::channel();
            match self.sender.try_send(Work::Redeem(request, send)) {
                Ok(()) => receive
                    .await
                    .unwrap_or(JoinResponse::Rejected(JoinFailure::OutcomeUnknown)),
                Err(mpsc::TrySendError::Full(_)) => JoinResponse::Rejected(JoinFailure::Capacity),
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    JoinResponse::Rejected(JoinFailure::Unavailable)
                }
            }
        })
    }
}
