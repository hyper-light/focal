//! One data handler follows the managed fleet's current installations. Selection
//! is synchronous; each request keeps its selected incarnation through async
//! evidence custody and the actual owner dispatch.
use crate::{
    content_host::ContentHost,
    evidence_service::{EvidenceCoordinator, FleetService},
    fleet::FleetManager,
};
use focal_model::{LedgerId, RouteEpoch};
use focal_wire::*;

/// The retained manager handle keeps fleet ownership alive while this service is
/// reachable. Explicit FleetManager::shutdown stops every incarnation and leaves
/// accepted, unfinished mutations unknown; it does not promise per-session
/// checkpoints. ReplicaOwner::join waits for physical teardown. No worker or
/// per-request task is created by this adapter.
#[derive(Clone)]
pub struct ManagedService {
    fleet: FleetManager,
    content: ContentHost,
    evidence: EvidenceCoordinator,
    signing: Option<(
        crate::control_host::ControlHost,
        crate::placement_control::PlacementHandle,
    )>,
    liveness: Option<crate::liveness::LivenessHandle>,
    /// The directory's routes, with this node's own identity for hints that
    /// point back at it.
    routes: Option<(
        crate::route_cache_host::RouteCacheHandle,
        u64,
        String,
        String,
    )>,
}
impl ManagedService {
    pub fn new(fleet: FleetManager, content: ContentHost, evidence: EvidenceCoordinator) -> Self {
        Self {
            fleet,
            content,
            evidence,
            signing: None,
            liveness: None,
            routes: None,
        }
    }
    /// Redirect requests for ledgers this node does not lead, and stale
    /// clients, from the directory's routes ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §14).
    pub fn with_routes(
        mut self,
        routes: crate::route_cache_host::RouteCacheHandle,
        node: u64,
        advertise: std::net::SocketAddr,
        server_name: String,
    ) -> Self {
        self.routes = Some((routes, node, advertise.to_string(), server_name));
        self
    }
    /// Where a request for `ledger` at the client's claimed route epoch must
    /// go instead of this node's replica, if anywhere. Without a replica the
    /// directory's leader hint is all this node knows (that node redirects
    /// again if it only follows); with one, the replica's own leader is the
    /// truth and the directory supplies the current epoch.
    async fn redirect(
        &self,
        ledger: LedgerId,
        claimed: RouteEpoch,
        mutation: bool,
        replica: Option<&crate::fleet::ReplicaHost>,
    ) -> Option<RouteHint> {
        let (routes, node, endpoint, server_name) = self.routes.as_ref()?;
        let Some(replica) = replica else {
            // Nothing here serves it: whoever leads does, unless that is this
            // node without a replica yet, which stays unavailable.
            return routes.resolve(ledger).await?.hint;
        };
        let epoch = routes
            .resolve(ledger)
            .await
            .map_or(claimed, |resolved| resolved.route.route_epoch.max(claimed));
        if claimed >= epoch && !mutation {
            // A current read is served here, whoever leads.
            return None;
        }
        let leader = replica
            .diagnostics()
            .await
            .map(|diagnostics| diagnostics.value().leader)
            .unwrap_or(0);
        let to_leader = async |epoch: RouteEpoch| {
            if leader == *node || leader == 0 {
                Some(RouteHint {
                    epoch,
                    endpoint: endpoint.clone(),
                    server_name: server_name.clone(),
                })
            } else {
                routes.hint(leader, epoch).await
            }
        };
        if claimed < epoch {
            // A stale client learns the current epoch at the current leader.
            return to_leader(epoch).await;
        }
        if mutation && leader != 0 && leader != *node {
            // A follower never proposes on a client's behalf.
            return to_leader(epoch).await;
        }
        None
    }
    /// Answer liveness probes from enrolled peers out of the driver's state.
    pub fn with_liveness(mut self, liveness: crate::liveness::LivenessHandle) -> Self {
        self.liveness = Some(liveness);
        self
    }
    async fn probe(&self, request: &VerifiedRequest) -> Response {
        let Some(liveness) = &self.liveness else {
            return Response::Error(AccessError::UnsupportedOperation);
        };
        // The probe speaks for the enrolled node identity the certificate
        // authorized; a runtime or client peer has none.
        let PeerRole::Node { node_id } = request.peer().role() else {
            return Response::Error(AccessError::Unauthorized);
        };
        let Operation::Probe { request: body } = &request.request().operation else {
            return Response::Error(AccessError::InvalidRequest);
        };
        match liveness.answer(node_id, body).await {
            Ok(reply) => Response::Probe(reply),
            Err(crate::liveness::ProbeError::Invalid) => {
                Response::Error(AccessError::InvalidRequest)
            }
            Err(crate::liveness::ProbeError::Capacity) => Response::Error(AccessError::Capacity),
            Err(crate::liveness::ProbeError::Unavailable) => {
                Response::Error(AccessError::Unavailable)
            }
        }
    }
    /// Let enrolled peers ask this node to sign session facts it witnesses.
    pub fn with_signing(
        mut self,
        control: crate::control_host::ControlHost,
        signer: crate::placement_control::PlacementHandle,
    ) -> Self {
        self.signing = Some((control, signer));
        self
    }
    async fn sign(&self, request: &VerifiedRequest) -> Response {
        let Some((control, signer)) = &self.signing else {
            return Response::Error(AccessError::UnsupportedOperation);
        };
        let Operation::SessionSign {
            group,
            request: body,
        } = &request.request().operation
        else {
            return Response::Error(AccessError::InvalidRequest);
        };
        let ledger = request.request().ledger;
        if *group != ledger.session.0 {
            return Response::Error(AccessError::InvalidRequest);
        }
        let Ok(decoded) =
            postcard::from_bytes::<crate::placement_control::SessionSignRequest>(body)
        else {
            return Response::Error(AccessError::InvalidRequest);
        };
        if decoded.schema != 1 {
            return Response::Error(AccessError::InvalidRequest);
        }
        let reply = match crate::placement_control::sign_session_fact(
            &self.fleet,
            control,
            signer,
            ledger,
            decoded.fact,
            decoded.window,
        )
        .await
        {
            Ok(proof) => {
                crate::placement_control::SessionSignReply::Signed(Box::new(proof.proof().clone()))
            }
            Err(error) => crate::placement_control::SessionSignReply::Refused(match error {
                crate::placement_proof::PlacementProofError::Capacity => {
                    focal_control::ControlFailure::Capacity
                }
                crate::placement_proof::PlacementProofError::Unavailable => {
                    focal_control::ControlFailure::Unavailable
                }
                _ => focal_control::ControlFailure::Unauthorized,
            }),
        };
        match postcard::to_stdvec(&reply) {
            Ok(response) if !response.is_empty() => Response::Control { response },
            _ => Response::Error(AccessError::Capacity),
        }
    }
}
impl ManagedService {
    /// One session control call (24 §9) from the partition leader that
    /// drives the session's placement, answered by the replica this node
    /// hosts when it leads the session's log.
    async fn session_control(&self, request: &VerifiedRequest) -> Response {
        use crate::session_control::{SESSION_CONTROL_SCHEMA, SessionControlRequest};
        let Some((control, signer)) = &self.signing else {
            return Response::Error(AccessError::UnsupportedOperation);
        };
        let Operation::SessionControl {
            group,
            request: body,
        } = &request.request().operation
        else {
            return Response::Error(AccessError::InvalidRequest);
        };
        let ledger = request.request().ledger;
        if *group != ledger.session.0 {
            return Response::Error(AccessError::InvalidRequest);
        }
        let PeerRole::Node { node_id: requester } = request.peer().role() else {
            return Response::Error(AccessError::Unauthorized);
        };
        let Ok(decoded) = postcard::from_bytes::<SessionControlRequest>(body) else {
            return Response::Error(AccessError::InvalidRequest);
        };
        if decoded.schema != SESSION_CONTROL_SCHEMA {
            return Response::Error(AccessError::InvalidRequest);
        }
        // Only a voter of the group that owns the session's partition drives
        // the session's log from outside it.
        if !crate::session_control::authorized_controller(control, ledger, requester).await {
            return Response::Error(AccessError::Unauthorized);
        }
        let _ = signer;
        let Some((_, node, _, _)) = &self.routes else {
            return Response::Error(AccessError::UnsupportedOperation);
        };
        let reply = crate::session_control::serve(&self.fleet, *node, ledger, decoded.call).await;
        match postcard::to_stdvec(&reply) {
            Ok(response) if !response.is_empty() => Response::Control { response },
            _ => Response::Error(AccessError::Capacity),
        }
    }
    /// One range-movement fact of this node's own hosted replica (25 §6),
    /// stated for the session authority over this authenticated connection.
    async fn range_control(&self, request: &VerifiedRequest) -> Response {
        let Operation::RangeControl {
            group,
            request: body,
        } = &request.request().operation
        else {
            return Response::Error(AccessError::InvalidRequest);
        };
        let ledger = request.request().ledger;
        if *group != ledger.session.0 {
            return Response::Error(AccessError::InvalidRequest);
        }
        let Ok(decoded) = postcard::from_bytes::<crate::fleet::RangeControlRequest>(body) else {
            return Response::Error(AccessError::InvalidRequest);
        };
        if decoded.schema != crate::fleet::RANGE_CONTROL_SCHEMA {
            return Response::Error(AccessError::InvalidRequest);
        }
        let reply = match self.fleet.current_host(ledger) {
            Ok(host) => match host.range_fact(decoded.fact).await {
                Ok(fact) => crate::fleet::RangeControlReply::Fact(Box::new(fact)),
                Err(error) => crate::fleet::RangeControlReply::Refused(match error {
                    focal_ledger::LedgerError::Capacity => focal_control::ControlFailure::Capacity,
                    focal_ledger::LedgerError::NotReady { leader } => {
                        focal_control::ControlFailure::NotLeader { leader }
                    }
                    _ => focal_control::ControlFailure::Unavailable,
                }),
            },
            Err(_) => {
                crate::fleet::RangeControlReply::Refused(focal_control::ControlFailure::Unavailable)
            }
        };
        match postcard::to_stdvec(&reply) {
            Ok(response) if !response.is_empty() => Response::Control { response },
            _ => Response::Error(AccessError::Capacity),
        }
    }
}
impl RequestHandler for ManagedService {
    fn supports_managed_requests(&self) -> bool {
        true
    }
    fn supports_participant_requests(&self) -> bool {
        true
    }
    fn supports_native_requests(&self) -> bool {
        true
    }
    fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted<'a>(&'a self, request: &'a VerifiedRequest) -> OwnedHandlerFuture<'a> {
        Box::pin(async move {
            let fail = |error| OwnedResponse::new(request.request().reply(Response::Error(error)));
            // VerifiedRequest already checks this at transport ingress. Repeat
            // the borrowed check before any existence-dependent routing result.
            if !request
                .peer()
                .permits_tenant(request.request().ledger.tenant)
            {
                return fail(AccessError::Unauthorized);
            }
            // A committed content-copy placement need not include a local Raft
            // replica. Its existing policy and authenticated Node checks own
            // custody authority; an application routing table does not grant it.
            if matches!(request.request().operation, Operation::Custody(_)) {
                return self.content.handle_accounted(request).await;
            }
            if matches!(request.request().operation, Operation::SessionSign { .. }) {
                let result = self.sign(request).await;
                return OwnedResponse::new(request.request().reply(result));
            }
            if matches!(request.request().operation, Operation::Probe { .. }) {
                let result = self.probe(request).await;
                return OwnedResponse::new(request.request().reply(result));
            }
            if matches!(request.request().operation, Operation::RangeControl { .. }) {
                let result = self.range_control(request).await;
                return OwnedResponse::new(request.request().reply(result));
            }
            if matches!(
                request.request().operation,
                Operation::SessionControl { .. }
            ) {
                let result = self.session_control(request).await;
                return OwnedResponse::new(request.request().reply(result));
            }
            let ledger = request.request().ledger;
            let claimed = request.request().route_epoch;
            let mutation = request.request().operation.is_mutation();
            // Nodes address replicas directly (replication, custody, support);
            // only clients are routed by the directory.
            let client = !matches!(request.peer().role(), PeerRole::Node { .. });
            let replica = match self.fleet.current_host(ledger) {
                Ok(replica) => replica,
                Err(_) => {
                    if !client {
                        return fail(AccessError::Unavailable);
                    }
                    return match self.redirect(ledger, claimed, mutation, None).await {
                        Some(hint) => fail(AccessError::RouteChanged(hint)),
                        None => fail(AccessError::Unavailable),
                    };
                }
            };
            if client
                && let Some(hint) = self
                    .redirect(ledger, claimed, mutation, Some(&replica))
                    .await
            {
                return fail(AccessError::RouteChanged(hint));
            }
            // Preserve the existing receipt-first artifact path. This owned
            // host survives lookup but can never target a replacement incarnation.
            let service = FleetService {
                replica,
                content: self.content.clone(),
                evidence: self.evidence.clone(),
            };
            service.handle_accounted(request).await
        })
    }
}
