//! Authenticated reachability announcements. The root owner commits this narrow
//! intent through its control log; only committed snapshots may become routes.
use crate::control_host::ControlHost;
use focal_control::{
    ControlCommand, ControlFailure, ControlReplica, ControlRequest, ControlRequestId, ControlScope,
    NodeContactCommand, authorize_node_contact, validate_contact_address,
};
use focal_wire::{
    AccessError, HandlerFuture, Operation, OwnedHandlerFuture, OwnedResponse, PeerRole,
    RequestHandler, Response, VerifiedRequest,
};

/// The same bounded control owner serves announcements and publishes receipts.
/// This adapter deliberately exposes no generic Runtime command submission.
#[derive(Clone)]
pub struct NodeContactHost {
    control: ControlHost,
}
impl NodeContactHost {
    pub fn new(control: ControlHost) -> Result<Self, ControlFailure> {
        if control.progress().identity.scope != ControlScope::Root {
            return Err(ControlFailure::WrongOwner);
        }
        Ok(Self { control })
    }
}
impl RequestHandler for NodeContactHost {
    fn handle<'a>(&'a self, request: &'a VerifiedRequest) -> HandlerFuture<'a> {
        Box::pin(async move { self.handle_accounted(request).await.into_envelope() })
    }
    fn handle_accounted<'a>(&'a self, request: &'a VerifiedRequest) -> OwnedHandlerFuture<'a> {
        Box::pin(async move {
            if !matches!(request.request().operation, Operation::NodeContact { .. }) {
                return OwnedResponse::new(
                    request
                        .request()
                        .reply(Response::Error(AccessError::Unauthorized)),
                );
            }
            self.control.handle_accounted(request).await
        })
    }
}

/// Called inside the single control owner after namespace, route and group
/// checks. The active enrollment is rechecked even for an existing receipt:
/// certificate revocation cannot be bypassed with a cached connection grant.
pub(crate) fn prepare_contact(
    replica: &ControlReplica,
    verified: &VerifiedRequest,
    now: i64,
) -> Result<ControlRequest, ControlFailure> {
    if replica.identity().scope != ControlScope::Root {
        return Err(ControlFailure::WrongOwner);
    }
    let Operation::NodeContact {
        group,
        sequence,
        acknowledged_through,
        expected_generation,
        advertise,
        region,
        zone,
        endpoint,
    } = &verified.request().operation
    else {
        return Err(ControlFailure::Unauthorized);
    };
    let (group, sequence, acknowledged_through, expected_generation, advertise) = (
        *group,
        *sequence,
        *acknowledged_through,
        *expected_generation,
        *advertise,
    );
    if group != replica.identity().group {
        return Err(ControlFailure::WrongOwner);
    }
    let PeerRole::Node { node_id } = verified.peer().role() else {
        return Err(ControlFailure::Unauthorized);
    };
    let certificate_fingerprint = verified
        .peer()
        .certificate_fingerprint()
        .ok_or(ControlFailure::Unauthorized)?;
    let principal = verified.peer().principal().0;
    validate_contact_address(advertise).map_err(ControlFailure::from)?;
    if sequence == 0 || acknowledged_through >= sequence {
        return Err(ControlFailure::RetryOrder);
    }
    let enrollment = replica.enrollment().ok_or(ControlFailure::WrongOwner)?;
    authorize_node_contact(enrollment, node_id, principal, certificate_fingerprint, now)
        .map_err(|_| ControlFailure::Unauthorized)?;
    Ok(ControlRequest {
        id: ControlRequestId {
            client: principal,
            sequence,
        },
        acknowledged_through,
        command: ControlCommand::NodeContact(NodeContactCommand {
            node: node_id,
            principal,
            certificate_fingerprint,
            advertise,
            expected_generation,
            decided_at: now,
            region: region.clone(),
            zone: zone.clone(),
            endpoint: endpoint.clone(),
        }),
    })
}

#[cfg(test)]
#[path = "network_contacts_tests.rs"]
mod tests;
