//! Native frame admission for the local owner thread. The frame is admitted
//! exactly as journaled by the client; only its fixed header is inspected here
//! before the owner decodes it under the session's limits.
use crate::{embedded::EmbeddedNode, host::access};
use focal_core::native::{NativeContext, NativeError, NativeOwnerError};
use focal_ledger::{
    FailureClass, LedgerError, NativeCustody, NativeSessionError, NativeSubmission, Session,
};
use focal_memory::BudgetLane;
use focal_model::lifecycle::{ContractError, Principal};
use focal_model::*;
use focal_wire::*;
use std::time::{SystemTime, UNIX_EPOCH};

/// Owner polls after a fresh proposal before the reply reports a pending ticket.
pub(crate) const COMMIT_POLLS: usize = 8;

/// Command tags whose bodies carry an artifact payload and therefore need
/// custody verification by the exclusive content writer (21 §4).
pub(crate) const fn artifact_bearing(command: u8) -> bool {
    matches!(command, 4 | 6 | 7 | 13 | 15 | 19)
}
/// Command tags that begin a validation phase against an artifact
/// (`BeginIncrement`, `BeginWork`): their target's custody obligation is
/// read before the owner admits them (doc 04 §7, R8).
pub(crate) const fn evaluates_artifact(command: u8) -> bool {
    matches!(command, 14 | 18)
}
/// The artifact a phase-beginning frame evaluates, decoded under the
/// session's own limits; other frames yield `None`.
pub(crate) fn evaluation_artifact_of_frame(
    limits: &focal_ledger::NativeSessionLimits,
    frame: &[u8],
) -> Result<Option<ArtifactId>, AccessError> {
    let decode = limits
        .decode_limits()
        .map_err(|_| AccessError::Unavailable)?;
    decode
        .with_request(limits.recovery.native, frame, |decoded, _| {
            decoded.into_evaluation_artifact()
        })
        .map_err(|_| AccessError::InvalidRequest)?
        .map_err(|_| AccessError::InvalidRequest)
}
/// Completion-class commands ride the completion lane like their legacy
/// counterparts; creation, posting and monitors stay ordinary.
pub(crate) fn lane(frame: &[u8]) -> BudgetLane {
    match inspect_native_frame(frame) {
        Ok(header) if matches!(header.command, 0..=2 | 23..=27) => BudgetLane::Ordinary,
        Ok(_) => BudgetLane::Completion,
        Err(_) => BudgetLane::Ordinary,
    }
}
/// The artifact an artifact-bearing frame carries and the request that
/// authored it, decoded under the session's own limits so the content host can
/// verify custody before the replicated owner admits the frame.
pub(crate) fn artifact_of_frame(
    limits: &focal_ledger::NativeSessionLimits,
    frame: &[u8],
) -> Result<
    Option<(
        RequestKey,
        focal_model::lifecycle::artifact_descriptor::ArtifactDescriptor,
    )>,
    AccessError,
> {
    let decode = limits
        .decode_limits()
        .map_err(|_| AccessError::Unavailable)?;
    decode
        .with_request(limits.recovery.native, frame, |decoded, _| {
            decoded.into_artifact()
        })
        .map_err(|_| AccessError::InvalidRequest)?
        .map_err(|_| AccessError::InvalidRequest)
}
/// Milliseconds since the Unix epoch, never behind the last committed record.
pub(crate) fn logical_time(session: &Session) -> Result<u64, AccessError> {
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AccessError::Unavailable)?
            .as_millis(),
    )
    .map_err(|_| AccessError::Unavailable)?;
    let committed = session
        .native_core()
        .map(|core| core.native_logical_time())
        .unwrap_or(0);
    Ok(now.max(committed))
}
pub(crate) fn context(
    peer: &AuthenticatedPeer,
    session: &Session,
) -> Result<NativeContext, AccessError> {
    Ok(NativeContext {
        principal: Principal::Actor(peer.principal()),
        logical_time: logical_time(session)?,
    })
}
fn code(error: ContractError) -> NativeErrorCode {
    match error {
        ContractError::WrongActor => NativeErrorCode::WrongActor,
        ContractError::WrongLedger => NativeErrorCode::WrongLedger,
        ContractError::WrongObject => NativeErrorCode::WrongObject,
        ContractError::ContentConflict => NativeErrorCode::ContentConflict,
        ContractError::StaleRevision => NativeErrorCode::StaleRevision,
        ContractError::StaleReceipt => NativeErrorCode::StaleReceipt,
        ContractError::StaleEvaluation => NativeErrorCode::StaleEvaluation,
        ContractError::InvalidTransition => NativeErrorCode::InvalidTransition,
        ContractError::InvalidTarget => NativeErrorCode::InvalidTarget,
        ContractError::InvalidManifest => NativeErrorCode::InvalidManifest,
        ContractError::MissingEvidence => NativeErrorCode::MissingEvidence,
        ContractError::InvalidPolicy => NativeErrorCode::InvalidPolicy,
        ContractError::Capacity => NativeErrorCode::Capacity,
        ContractError::ConflictingCause => NativeErrorCode::ConflictingCause,
        ContractError::InvalidCut => NativeErrorCode::InvalidCut,
        ContractError::RequestHistoryExpired => NativeErrorCode::RequestHistoryExpired,
        ContractError::EpochNotAdmitted => NativeErrorCode::EpochNotAdmitted,
    }
}
fn native_kind(error: &NativeError) -> NativeRefusalKind {
    match error {
        NativeError::Contract(error) => NativeRefusalKind::Refused(code(*error)),
        NativeError::RequestConflict => NativeRefusalKind::Conflict,
        NativeError::Capacity(_) | NativeError::Memory(_) => NativeRefusalKind::Capacity,
        NativeError::Evidence(error) => match error {
            focal_evidence::NativeEvidenceError::WrongRequest
            | focal_evidence::NativeEvidenceError::Schema(_)
            | focal_evidence::NativeEvidenceError::Contract(_)
            | focal_evidence::NativeEvidenceError::VerificationBudgetChanged => {
                NativeRefusalKind::InvalidInput
            }
            focal_evidence::NativeEvidenceError::Content(_)
            | focal_evidence::NativeEvidenceError::Memory(_) => NativeRefusalKind::Capacity,
        },
    }
}
/// Deterministic request refusals become closed wire refusals; everything else
/// stays a transport-level condition the client retries or reconciles.
pub(crate) fn refusal(error: &NativeSessionError) -> Option<NativeRefusal> {
    if error.class() != FailureClass::Request {
        return None;
    }
    let kind = match error {
        NativeSessionError::Native(error)
        | NativeSessionError::Owner(NativeOwnerError::Native(error)) => native_kind(error),
        NativeSessionError::Owner(NativeOwnerError::Input(_) | NativeOwnerError::Record(_)) => {
            NativeRefusalKind::InvalidInput
        }
        _ => NativeRefusalKind::Refused(NativeErrorCode::Unsupported),
    };
    Some(NativeRefusal {
        kind,
        detail: error.to_string(),
    })
}
pub(crate) fn failure(error: LedgerError) -> Result<NativeMutationReply, AccessError> {
    match error {
        LedgerError::Native(error) => match refusal(&error) {
            Some(refusal) => Ok(NativeMutationReply::Refused(refusal)),
            // A bound refused: nothing was admitted, and the client keeps the exact frame
            // for its retry, as for a bare capacity refusal; the refusal says which bound
            // it was (the session's, the owner's memory, the proposal queue), so an operator
            // reads it where it was an unnamed `capacity`.
            None if error.class() == FailureClass::Retryable => {
                Ok(NativeMutationReply::Refused(NativeRefusal {
                    kind: NativeRefusalKind::Capacity,
                    detail: error.to_string(),
                }))
            }
            None => Err(match error.class() {
                FailureClass::Retryable => AccessError::Capacity,
                FailureClass::Authority | FailureClass::FailClosed => AccessError::Unavailable,
                FailureClass::Request => AccessError::InvalidRequest,
            }),
        },
        LedgerError::NativeUnsupported => Err(AccessError::UnsupportedOperation),
        other => Err(access(other)),
    }
}
/// A native frame the local owner proposed and has not seen commit. The
/// owner parks it rather than wait for it here, so the frames that arrive
/// while one flush is in flight are proposed before the next and share it
/// (group commit).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Proposal {
    pub(crate) key: RequestKey,
    pub(crate) outcome: focal_core::native::NativeOutcome,
}
/// Admit one frame on the embedded owner. Artifact payloads are verified and
/// sealed by the exclusive content writer before the owner records them. A
/// fresh proposal is left in `proposed`, and its answer is its commit.
pub(crate) fn admit_local(
    node: &mut EmbeddedNode,
    peer: &AuthenticatedPeer,
    envelope: &RequestEnvelope,
    frame: &[u8],
    proposed: &mut Option<Proposal>,
) -> Result<Response, AccessError> {
    let header = native_frame_admissible(frame, peer, envelope)?;
    let context = context(peer, &node.session)?;
    crate::fault::hit(crate::fault::FaultSite::BeforePropose);
    let result =
        node.session
            .propose_native_frame(context, frame, NativeCustody::Store(&mut node.content));
    match result {
        Ok(NativeSubmission::Committed(outcome)) => Ok(committed_reply(outcome)),
        Ok(NativeSubmission::Pending { outcome, .. }) => {
            *proposed = Some(Proposal {
                key: header.key,
                outcome,
            });
            // Never sent: the parked proposal is answered by its commit.
            Err(AccessError::OutcomeUnknown)
        }
        Err(error) => failure(error).map(Response::Native),
    }
}
/// The answer of a parked proposal once the session holds its outcome;
/// `None` while it does not yet.
pub(crate) fn settled(
    session: &Session,
    proposal: Proposal,
) -> Result<Option<Response>, AccessError> {
    match session.native_outcome(proposal.key).map_err(access)? {
        Some(committed) if committed == proposal.outcome => Ok(Some(committed_reply(committed))),
        _ => Ok(None),
    }
}
/// The answer of a proposal whose commit its polls did not see: the ticket
/// the client resends the exact frame under.
pub(crate) fn ticket(proposal: Proposal) -> Response {
    Response::Native(NativeMutationReply::Pending(NativeTicket {
        key: proposal.key,
        intent: proposal.outcome.intent,
    }))
}
fn committed_reply(outcome: focal_core::native::NativeOutcome) -> Response {
    crate::fault::hit(crate::fault::FaultSite::AfterCommitBeforeReply);
    Response::Native(NativeMutationReply::Committed(
        crate::native_documents::outcome(outcome),
    ))
}
