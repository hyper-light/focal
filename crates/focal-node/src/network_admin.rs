//! Founder-only local invitation administration. Kernel-authenticated Unix
//! ingress calls the existing bounded signer directly; no extra actor or queue.
use crate::{
    cluster::InviteIntent,
    embedded::NodeIdentity,
    network_join::NodeInvitation,
    network_state::{NetworkState, root_namespace},
    node_directory::NodeDirectory,
    quorum_enrollment::{QuorumEnrollmentError, QuorumEnrollmentHost},
};
use focal_control::{ControlFailure, ControlIdentity, ControlScope};
use focal_enrollment::{EnrollmentError, EnrollmentRole};
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{RequestEpoch, RequestId, RouteEpoch};
use focal_wire::*;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

pub const ADMIN_SOCKET: &str = "focal-admin.sock";
const MAGIC: &[u8] = b"FCLADMIN1";
const MAX_COMMAND: usize = 128;
const WORKSPACE: usize = 2 * 1024 * 1024;

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
}
impl AdminCommand {
    pub fn invitation(name: impl Into<String>) -> Result<Self, AccessError> {
        let name = name.into();
        validate_name(&name)?;
        Ok(Self::Invite { name })
    }
    pub fn encode(&self) -> Result<Vec<u8>, AccessError> {
        let Self::Invite { name } = self;
        validate_name(name)?;
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
        let Self::Invite { name } = &command;
        if !tail.is_empty() {
            return Err(AccessError::InvalidRequest);
        }
        validate_name(name)?;
        Ok(command)
    }
    pub fn request(&self, identity: &NodeIdentity) -> Result<RequestEnvelope, AccessError> {
        let Self::Invite { name } = self;
        Ok(RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: root_namespace(identity),
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: invitation_request_id(identity.cluster, name)?,
            operation: Operation::Control {
                group: crate::network_state::root_group(identity.cluster),
                request: self.encode()?,
            },
        })
    }
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
    enrollment: QuorumEnrollmentHost,
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
            enrollment,
            budget,
        })
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
        let AdminCommand::Invite { name } = AdminCommand::decode(bytes)?;
        let id = invitation_request_id(self.identity.cluster, &name)?;
        if request.request_id != id {
            return Err(AccessError::InvalidRequest);
        }
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
            .invite(
                id,
                InviteIntent {
                    endpoint: self.advertise.to_string(),
                    role: EnrollmentRole::Node,
                    lifetime_seconds: 3600,
                },
            )
            .await
            .map_err(enrollment_error)?;
        let bundle = NodeInvitation::new(name, state.genesis, invitation)
            .map_err(|_| AccessError::Unavailable)?;
        let mut bytes = bundle.encode().map_err(|_| AccessError::Capacity)?;
        Ok(std::mem::take(&mut *bytes))
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
