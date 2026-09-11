//! Session control between nodes (24 §9): the partition leader that drives a
//! session's placement asks the node that leads the session's log to state
//! the session's facts, or to apply one membership change or placement
//! record to that log. The request travels over the requester's own
//! authenticated connection; the log applies it under its own committed
//! rules, and the request is honoured only from a voter of the group that
//! owns the partition holding the session.
use crate::{
    control_host::ControlHost, fleet::FleetManager, session_registration::HostedSessionFacts,
};
use focal_control::{ControlBootstrap, ControlFailure};
use focal_ledger::{
    LedgerError, MembershipView, SessionMembershipRequest, SessionPlacementRequest,
};
use focal_model::LedgerId;
use serde::{Deserialize, Serialize};

pub const SESSION_CONTROL_SCHEMA: u16 = 1;

/// One call the session's leader answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionCall {
    /// The session's registration facts as the leader applied them.
    Facts,
    /// Apply one membership change under its expected configuration.
    Membership(SessionMembershipRequest),
    /// Propose one placement record (a cutover or an activation).
    Placement(SessionPlacementRequest),
}
/// The body of `Operation::SessionControl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionControlRequest {
    pub schema: u16,
    pub call: SessionCall,
}
/// The reply carried in `Response::Control` for `SessionControl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionControlReply {
    Facts(Box<HostedSessionFacts>),
    Membership(Box<MembershipView>),
    Placed,
    /// `NotLeader { leader }` names where the log leads, when known.
    Refused(ControlFailure),
}

fn refusal(error: LedgerError) -> ControlFailure {
    match error {
        LedgerError::Capacity => ControlFailure::Capacity,
        LedgerError::NotReady { leader } => ControlFailure::NotLeader { leader },
        LedgerError::OutcomeUnknown => ControlFailure::OutcomeUnknown,
        LedgerError::MembershipConflict | LedgerError::PlacementConflict => {
            ControlFailure::CompareFailed
        }
        _ => ControlFailure::Rejected,
    }
}

/// Whether `requester` votes in the group that owns the partition whose
/// namespace holds `ledger`, by this node's own observation of the root.
pub async fn authorized_controller(
    control: &ControlHost,
    ledger: LedgerId,
    requester: u64,
) -> bool {
    let Ok(observation) = control.observe_root().await else {
        return false;
    };
    let ControlBootstrap::Root { directory, .. } = &observation.snapshot().state else {
        return false;
    };
    // A partition the root group itself owns is judged by the root's own
    // applied configuration, which every root replica holds; another
    // owner group by the installed authority's grant for it.
    let root_group = observation.snapshot().identity.group;
    directory
        .delegations
        .values()
        .filter(|delegation| delegation.namespace.contains(ledger))
        .any(|delegation| {
            if delegation.log_group.0 == root_group {
                return observation
                    .configuration()
                    .configuration
                    .voters
                    .contains(&requester);
            }
            observation.authority().is_some_and(|authority| {
                authority
                    .groups
                    .get(&delegation.log_group)
                    .is_some_and(|group| group.voters.contains_key(&requester))
            })
        })
}

/// Answer one call for the session `ledger` from this node's hosted
/// replica; a replica that does not lead answers `NotLeader`.
pub async fn serve(
    fleet: &FleetManager,
    node: u64,
    ledger: LedgerId,
    call: SessionCall,
) -> SessionControlReply {
    let Ok(host) = fleet.current_host(ledger) else {
        return SessionControlReply::Refused(ControlFailure::Unavailable);
    };
    let progress = host.progress();
    if progress.leader != node {
        return SessionControlReply::Refused(ControlFailure::NotLeader {
            leader: progress.leader,
        });
    }
    match call {
        SessionCall::Facts => match host.registration_facts().await {
            Ok(reply) => SessionControlReply::Facts(Box::new(reply.value().clone())),
            Err(error) => SessionControlReply::Refused(refusal(error)),
        },
        SessionCall::Membership(request) => match host.change_membership(request).await {
            Ok(reply) => SessionControlReply::Membership(Box::new(reply.view().clone())),
            Err(error) => SessionControlReply::Refused(refusal(error)),
        },
        SessionCall::Placement(request) => match host.propose_placement(request).await {
            Ok(_) => SessionControlReply::Placed,
            Err(error) => SessionControlReply::Refused(refusal(error)),
        },
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn calls_and_replies_round_trip() {
        let request = SessionControlRequest {
            schema: SESSION_CONTROL_SCHEMA,
            call: SessionCall::Facts,
        };
        let bytes = postcard::to_stdvec(&request).unwrap();
        assert_eq!(
            postcard::from_bytes::<SessionControlRequest>(&bytes).unwrap(),
            request
        );
        let reply = SessionControlReply::Refused(ControlFailure::NotLeader { leader: 7 });
        let bytes = postcard::to_stdvec(&reply).unwrap();
        assert_eq!(
            postcard::from_bytes::<SessionControlReply>(&bytes).unwrap(),
            reply
        );
        assert!(matches!(
            refusal(LedgerError::NotReady { leader: 3 }),
            ControlFailure::NotLeader { leader: 3 }
        ));
        assert!(matches!(
            refusal(LedgerError::PlacementConflict),
            ControlFailure::CompareFailed
        ));
    }
}
