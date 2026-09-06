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
}
impl ManagedService {
    pub fn new(fleet: FleetManager, content: ContentHost, evidence: EvidenceCoordinator) -> Self {
        Self {
            fleet,
            content,
            evidence,
        }
    }
}
impl RequestHandler for ManagedService {
    fn supports_managed_requests(&self) -> bool {
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
