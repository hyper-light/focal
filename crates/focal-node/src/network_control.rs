//! Founder-only enrollment decisions over the replicated root owner. Routing
//! reads only installed contacts; replies never introduce endpoint authority.
use crate::{
    control_host::ControlHost,
    network_bootstrap::{signer_principal, unix_time},
    network_state::NetworkGenesis,
    quorum_enrollment::{ControlFuture, EnrollmentControl},
};
use focal_control::*;
use focal_enrollment::{EnrollmentLimits, EnrollmentRegistry};
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch};
use focal_wire::*;
use futures_util::FutureExt;
use std::{
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const MAX_ROUTE_PROBES: usize = 8;
const ROUTE_ROUND_TIMEOUT: Duration = Duration::from_secs(4);
const ROUTE_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Constructed only by the trusted owner from its immutable genesis manifest.
/// No serialized request or live contact table can replace this pin. A changed
/// certificate requires an explicit future genesis-authority migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FounderControlAuthority {
    root: ControlIdentity,
    namespace: LedgerId,
    node: u64,
    principal: ParticipantId,
    certificate: [u8; 32],
    signer: ParticipantId,
}
impl FounderControlAuthority {
    pub fn from_genesis(genesis: &NetworkGenesis) -> Result<Self, ControlFailure> {
        genesis
            .validate(&genesis.founder)
            .map_err(|_| ControlFailure::Unauthorized)?;
        let ControlBootstrap::Root { enrollment, .. } = &genesis.bootstrap else {
            return Err(ControlFailure::WrongOwner);
        };
        let registry = EnrollmentRegistry::restore(
            enrollment,
            genesis.root.cluster.0,
            EnrollmentLimits::default(),
        )
        .map_err(|_| ControlFailure::Unauthorized)?;
        let receipt = registry
            .enrollments()
            .next()
            .ok_or(ControlFailure::Unauthorized)?;
        Ok(Self {
            root: genesis.root,
            namespace: genesis.root_namespace,
            node: genesis.founder.node,
            principal: genesis.founder.issuer,
            certificate: certificate_fingerprint(&receipt.certificate),
            signer: signer_principal(genesis.root.cluster.0),
        })
    }
    pub fn identity(&self) -> ControlIdentity {
        self.root
    }
    pub fn namespace(&self) -> LedgerId {
        self.namespace
    }
    pub fn signer_principal(&self) -> ParticipantId {
        self.signer
    }
    fn verify_peer(&self, peer: &AuthenticatedPeer) -> Result<(), ControlFailure> {
        if peer.role() != (PeerRole::Node { node_id: self.node })
            || peer.principal() != self.principal
            || peer.certificate_fingerprint() != Some(self.certificate)
        {
            return Err(ControlFailure::Unauthorized);
        }
        Ok(())
    }
    pub(crate) fn authorize_current(&self, replica: &ControlReplica) -> Result<(), ControlFailure> {
        if replica.identity() != self.root {
            return Err(ControlFailure::WrongOwner);
        }
        let registry = replica.enrollment().ok_or(ControlFailure::WrongOwner)?;
        authorize_node_contact(
            registry,
            self.node,
            self.principal.0,
            self.certificate,
            unix_time().map_err(|_| ControlFailure::Unavailable)?,
        )
        .map_err(|_| ControlFailure::Unauthorized)?;
        Ok(())
    }
    pub(crate) fn decode(
        &self,
        replica: &ControlReplica,
        verified: &VerifiedRequest,
    ) -> Result<ControlRpc, ControlFailure> {
        self.verify_peer(verified.peer())?;
        self.authorize_current(replica)?;
        let Operation::EnrollmentControl {
            group,
            genesis,
            request,
        } = &verified.request().operation
        else {
            return Err(ControlFailure::Unauthorized);
        };
        if *group != self.root.group
            || *genesis != self.root.genesis
            || verified.request().ledger != self.namespace
        {
            return Err(ControlFailure::WrongOwner);
        }
        // Reject any other command discriminant before decoding its collections.
        // ControlRpc Submit=0, Read=1; ControlCommand Enrollment=1. These existing
        // append-only ordinals are asserted by protocol tests.
        let (rpc, rest) =
            postcard::take_from_bytes::<u32>(request).map_err(|_| ControlFailure::Invalid)?;
        match rpc {
            0 => {
                let (id, rest) = postcard::take_from_bytes::<ControlRequestId>(rest)
                    .map_err(|_| ControlFailure::Invalid)?;
                let (_, rest) =
                    postcard::take_from_bytes::<u64>(rest).map_err(|_| ControlFailure::Invalid)?;
                let (command, _) =
                    postcard::take_from_bytes::<u32>(rest).map_err(|_| ControlFailure::Invalid)?;
                if command != 1 || id.client != self.signer.0 {
                    return Err(ControlFailure::Unauthorized);
                }
            }
            1 if rest == [0] => (),
            _ => return Err(ControlFailure::Unauthorized),
        }
        let rpc = ControlRpc::decode(
            request,
            MAX_ENROLLMENT_CONTROL_REQUEST_BYTES.min(replica.limits().max_command_bytes),
        )
        .map_err(ControlFailure::from)?;
        if !allowed(&rpc, self.signer) {
            return Err(ControlFailure::Unauthorized);
        }
        Ok(rpc)
    }
}
fn allowed(rpc: &ControlRpc, signer: ParticipantId) -> bool {
    match rpc {
        ControlRpc::Read(ControlRead::State) => true,
        ControlRpc::Submit(request) => {
            request.id.client == signer.0
                && matches!(request.command, ControlCommand::Enrollment(_))
        }
        _ => false,
    }
}

/// Borrowed by QuorumEnrollmentDriver::run. The enrollment handle is already a
/// bounded, cloneable actor handle, so this transport adds neither an actor nor
/// shared ownership. Caller-owned response reservations survive returned state.
pub struct NetworkEnrollmentControl<'a> {
    pool: &'a PeerConnectionPool,
    local: &'a ControlHost,
    authority: FounderControlAuthority,
    peer: AuthenticatedPeer,
    route_epoch: RouteEpoch,
    budget: &'a MemoryBudget,
    route_cursor: AtomicU64,
}
impl<'a> NetworkEnrollmentControl<'a> {
    pub fn new(
        pool: &'a PeerConnectionPool,
        local: &'a ControlHost,
        authority: FounderControlAuthority,
        peer: AuthenticatedPeer,
        route_epoch: RouteEpoch,
        budget: &'a MemoryBudget,
    ) -> Result<Self, ControlFailure> {
        authority.verify_peer(&peer)?;
        if local.progress().identity != authority.root || route_epoch.0 == 0 {
            return Err(ControlFailure::WrongOwner);
        }
        Ok(Self {
            pool,
            local,
            authority,
            peer,
            route_epoch,
            budget,
            route_cursor: AtomicU64::new(0),
        })
    }
    async fn call(&self, id: RequestId, rpc: ControlRpc) -> Result<ControlReply, ControlFailure> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(ControlFailure::Unavailable);
        }
        // A borrowed adapter may move to a runtime without a timer driver.
        // Contain timer/transport dependency panics without repolling an unwind.
        AssertUnwindSafe(async {
            tokio::time::timeout(ROUTE_ROUND_TIMEOUT, self.call_round(id, rpc))
                .await
                .unwrap_or(Err(ControlFailure::OutcomeUnknown))
        })
        .catch_unwind()
        .await
        .unwrap_or(Err(ControlFailure::Unavailable))
    }
    async fn call_round(
        &self,
        id: RequestId,
        rpc: ControlRpc,
    ) -> Result<ControlReply, ControlFailure> {
        if !allowed(&rpc, self.authority.signer) {
            return Err(ControlFailure::Unauthorized);
        }
        let request_size =
            postcard::experimental::serialized_size(&rpc).map_err(|_| ControlFailure::Invalid)?;
        if request_size > MAX_ENROLLMENT_CONTROL_REQUEST_BYTES {
            return Err(ControlFailure::Capacity);
        }
        // Cover the encoded request, transport framing and temporary decoded
        // response. QuorumEnrollmentDriver reserves the exported registry's
        // lifetime independently before invoking this adapter.
        let bytes = (ControlHost::wire_limits().max_frame_bytes as usize)
            .checked_mul(4)
            .and_then(|n| n.checked_add(request_size.saturating_mul(4)))
            .ok_or(ControlFailure::Capacity)?;
        let _charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Ordinary, bytes)
            .map_err(|_| ControlFailure::Capacity)?
            .commit();
        let packet = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: self.authority.namespace,
            route_epoch: self.route_epoch,
            request_epoch: RequestEpoch(1),
            request_id: id,
            operation: Operation::EnrollmentControl {
                group: self.authority.root.group,
                genesis: self.authority.root.genesis,
                request: rpc
                    .encode(MAX_ENROLLMENT_CONTROL_REQUEST_BYTES)
                    .map_err(ControlFailure::from)?,
            },
        };
        let mut failure = ControlFailure::Unavailable;
        let progress = self.local.progress();
        let mut preferred = (progress.leader != 0).then_some(progress.leader);
        if progress.leader == progress.node && !progress.stopped {
            let verified = verify_request(
                self.peer.clone(),
                packet.clone(),
                &ControlHost::wire_limits(),
            )
            .map_err(peer_access)?;
            let response = self.local.handle_accounted(verified).await;
            let result = match &response.envelope().result {
                Response::Control { response } => decode_reply(response),
                Response::Error(error) => Err(peer_access(error.clone())),
                _ => Err(ControlFailure::Invalid),
            };
            match result {
                Ok(reply) => return Ok(reply),
                Err(error) if retryable(error) => {
                    if let ControlFailure::NotLeader { leader } = error {
                        preferred = Some(leader);
                    }
                    failure = error;
                }
                Err(error) => return Err(error),
            }
        }
        let mut attempted = [0; MAX_ROUTE_PROBES];
        for slot in 0..MAX_ROUTE_PROBES {
            let Some(target) = self.next_target(&mut preferred, &attempted)? else {
                break;
            };
            if target == progress.node || attempted.contains(&target) {
                continue;
            }
            *attempted.get_mut(slot).ok_or(ControlFailure::Invalid)? = target;
            let result = tokio::time::timeout(
                ROUTE_PROBE_TIMEOUT,
                self.pool.send_enrollment_control(target, &packet),
            )
            .await
            .map_err(|_| ControlFailure::OutcomeUnknown)
            .and_then(|result| result.map_err(peer_failure))
            .and_then(|bytes| decode_reply(&bytes));
            match result {
                Ok(reply) => return Ok(reply),
                Err(error) if retryable(error) => {
                    if let ControlFailure::NotLeader { leader } = error {
                        preferred = Some(leader);
                    }
                    failure = error;
                }
                Err(error) => return Err(error),
            }
        }
        Err(failure)
    }
    fn next_target(
        &self,
        preferred: &mut Option<u64>,
        attempted: &[u64],
    ) -> Result<Option<u64>, ControlFailure> {
        if let Some(node) = preferred.take()
            && node != 0
            && !attempted.contains(&node)
            && self
                .pool
                .next_route_target(node.saturating_sub(1))
                .map_err(peer_failure)?
                == Some(node)
        {
            return Ok(Some(node));
        }
        let after = self.route_cursor.load(Ordering::Relaxed);
        let next = self.pool.next_route_target(after).map_err(peer_failure)?;
        let next = match next {
            Some(node) => Some(node),
            None if after != 0 => self.pool.next_route_target(0).map_err(peer_failure)?,
            None => None,
        };
        if let Some(node) = next {
            // Advance before awaiting the network: cancellation/unknown outcomes
            // cannot wedge the sole signer behind the same dead route forever.
            self.route_cursor.store(node, Ordering::Relaxed);
        }
        Ok(next)
    }
}
impl EnrollmentControl for NetworkEnrollmentControl<'_> {
    fn identity(&self) -> ControlIdentity {
        self.authority.root
    }
    fn principal(&self) -> ParticipantId {
        self.authority.signer_principal()
    }
    fn read_state(&self, id: RequestId) -> ControlFuture<'_, ControlSnapshot> {
        Box::pin(async move {
            match self.call(id, ControlRpc::Read(ControlRead::State)).await? {
                ControlReply::Read(ControlReadResult::State(state))
                    if state.identity == self.authority.root =>
                {
                    Ok(state)
                }
                _ => Err(ControlFailure::WrongOwner),
            }
        })
    }
    fn submit(&self, request: ControlRequest) -> ControlFuture<'_, ControlReceipt> {
        Box::pin(async move {
            let identity = request.id;
            match self
                .call(
                    RequestId::from_u128(u128::from(identity.sequence)),
                    ControlRpc::Submit(request),
                )
                .await?
            {
                ControlReply::Committed(receipt) if receipt.request == identity => Ok(receipt),
                _ => Err(ControlFailure::Invalid),
            }
        })
    }
}
fn decode_reply(bytes: &[u8]) -> Result<ControlReply, ControlFailure> {
    match ControlReply::decode(bytes, ControlHost::wire_limits().max_frame_bytes as usize)
        .map_err(ControlFailure::from)?
    {
        ControlReply::Rejected(error) => Err(error),
        reply => Ok(reply),
    }
}
fn retryable(error: ControlFailure) -> bool {
    matches!(
        error,
        ControlFailure::Unavailable
            | ControlFailure::OutcomeUnknown
            | ControlFailure::NotLeader { .. }
            | ControlFailure::NotReady
            | ControlFailure::Capacity
    )
}
fn peer_access(error: AccessError) -> ControlFailure {
    match error {
        AccessError::Unauthorized => ControlFailure::Unauthorized,
        AccessError::Capacity => ControlFailure::Capacity,
        AccessError::OutcomeUnknown => ControlFailure::OutcomeUnknown,
        _ => ControlFailure::Unavailable,
    }
}
fn peer_failure(error: PeerSendError) -> ControlFailure {
    match error {
        PeerSendError::Rejected(error) => peer_access(error),
        PeerSendError::InvalidRequest | PeerSendError::Configuration => ControlFailure::Invalid,
        PeerSendError::Busy => ControlFailure::Capacity,
        _ => ControlFailure::OutcomeUnknown,
    }
}

#[cfg(test)]
#[path = "network_control_tests.rs"]
mod tests;
