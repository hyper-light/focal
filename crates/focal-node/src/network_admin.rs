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
    Invite { name: String },
    Read(AdminRead),
    Membership(Box<ControlRequest>),
    Transfer(ControlTransfer),
    Revocation(Box<ControlRequest>),
    InviteClient { name: String },
    Replica(Box<ReplicaAdminCommand>),
    Operator(OperatorRead),
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
            Self::Invite { name } | Self::InviteClient { name } => validate_name(name),
            Self::Read(AdminRead::Invitations { limit, .. }) if *limit == 0 || *limit > 64 => {
                Err(AccessError::InvalidRequest)
            }
            Self::Read(AdminRead::Reconcile { sequence: 0 }) => Err(AccessError::InvalidRequest),
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
    listen: SocketAddr,
    enrollment: Option<QuorumEnrollmentHost>,
    control: Option<crate::control_host::ControlHost>,
    fleet: Option<crate::fleet::FleetManager>,
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
            listen: state.listen,
            enrollment: Some(enrollment),
            control: None,
            fleet: None,
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
            listen: state.listen,
            enrollment,
            control: None,
            fleet: None,
            budget,
        })
    }
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
                    endpoint: self.advertise.to_string(),
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
                    },
                )
                .await
                .map(ControlReply::Read),
            AdminCommand::Membership(request) | AdminCommand::Revocation(request) => {
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
            | AdminCommand::Replica(_) => {
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
