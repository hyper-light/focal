//! One data handler follows the managed fleet's current installations. Selection
//! is synchronous; each request keeps its selected incarnation through async
//! evidence custody and the actual owner dispatch.
use crate::{
    content_host::ContentHost,
    evidence_service::{EvidenceCoordinator, FleetService},
    fleet::FleetManager,
};
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
}
impl ManagedService {
    pub fn new(fleet: FleetManager, content: ContentHost, evidence: EvidenceCoordinator) -> Self {
        Self {
            fleet,
            content,
            evidence,
            signing: None,
            liveness: None,
        }
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
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted(&self, request: VerifiedRequest) -> OwnedHandlerFuture<'_> {
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
                let result = self.sign(&request).await;
                return OwnedResponse::new(request.request().reply(result));
            }
            if matches!(request.request().operation, Operation::Probe { .. }) {
                let result = self.probe(&request).await;
                return OwnedResponse::new(request.request().reply(result));
            }
            let replica = match self.fleet.current_host(request.request().ledger) {
                Ok(replica) => replica,
                Err(_) => return fail(AccessError::Unavailable),
            };
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
