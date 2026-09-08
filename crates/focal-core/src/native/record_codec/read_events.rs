//! Complete retained fact decoding. Values are historical DTOs: decoding neither
//! reauthorizes a participant nor certifies ledger membership, revision chains,
//! semantic stamps or publication coordinates. Recovery proves those separately.
use super::{
    bytes::{Cursor, Error},
    fixed, read_fields as f,
};
use crate::native::{
    NativeClaimEvent, NativeEvaluationEventKind, NativeEvent, NativeEventKind, NativeFact,
    NativeMonitorEvent,
};
use focal_model::{ClaimId, MonitorId};

#[cfg(test)]
#[path = "read_events_tests.rs"]
mod tests;

/// Full ledgers are present in the encoded bindings. The importer cross-checks
/// each against its retained root ledger before packing the immutable event.
pub(super) fn event(c: &mut Cursor<'_>) -> Result<NativeEvent, Error> {
    let invocation = fixed::read_invocation(c)?;
    let sequence = f::sequence(c)?;
    let ordinal = c.u32()?;
    let fact = match c.u8()? {
        0 => NativeFact::ResultTestament {
            claim: ClaimId(c.fixed()?),
            before: f::optional_binding(c)?,
            after: f::binding(c)?,
            state: f::result_testament_state(c)?,
        },
        1 => NativeFact::Missing {
            key: f::result_key(c)?,
        },
        2 => NativeFact::Registrations {
            claim: f::binding(c)?,
        },
        3 => NativeFact::Delivery {
            key: f::result_key(c)?,
        },
        4 => NativeFact::Work {
            claim: ClaimId(c.fixed()?),
            before: f::optional_binding(c)?,
            after: f::binding(c)?,
            state: f::work_state(c)?,
        },
        5 => NativeFact::Diagnostic {
            claim: ClaimId(c.fixed()?),
            binding: f::binding(c)?,
            reason: f::failure(c)?,
        },
        6 => NativeFact::Response {
            claim: ClaimId(c.fixed()?),
            before: f::optional_binding(c)?,
            after: f::binding(c)?,
            state: f::response_state(c)?,
        },
        7 => NativeFact::Receipt {
            claim: f::binding(c)?,
            fence: f::receipt(c)?,
            holder: f::participant(c)?,
        },
        8 => NativeFact::ReceiptAdopted {
            claim: f::binding(c)?,
            previous: f::entitlement(c)?,
            replacement: f::entitlement(c)?,
            cause: f::hash(c)?,
        },
        9 => NativeFact::Artifact {
            binding: f::binding(c)?,
        },
        10 => NativeFact::Accepted {
            key: f::result_key(c)?,
        },
        11 => NativeFact::Claim(NativeClaimEvent {
            kind: claim_kind(c)?,
            owned_child: f::optional_binding(c)?,
            before: f::optional_binding(c)?,
            after: f::binding(c)?,
            status: f::claim_status(c)?,
            graph: f::optional(c, |c| {
                Ok(crate::native::NativeGraphCapture {
                    before_ordinal: c.u32()?,
                })
            })?,
        }),
        12 => NativeFact::Definition {
            binding: f::binding(c)?,
            claim: ClaimId(c.fixed()?),
            index: c.u32()?,
            intent: f::hash(c)?,
        },
        13 => NativeFact::Evaluation {
            kind: evaluation_kind(c)?,
            key: f::evaluation(c)?,
            before: f::optional_binding(c)?,
            after: f::binding(c)?,
            state: f::evaluation_state(c)?,
            phase: f::phase(c)?,
            attempt: f::optional(c, f::attempt)?,
            fence: f::optional_fence(c)?,
        },
        _ => return Err(Error::InvalidTag("native event fact")),
    };
    Ok(NativeEvent {
        invocation,
        sequence,
        ordinal,
        fact,
    })
}
pub(super) fn evaluation_kind(c: &mut Cursor<'_>) -> Result<NativeEvaluationEventKind, Error> {
    Ok(match c.u8()? {
        0 => NativeEvaluationEventKind::MissingTarget,
        1 => NativeEvaluationEventKind::Materialized,
        2 => NativeEvaluationEventKind::Begun,
        3 => NativeEvaluationEventKind::Reported,
        4 => NativeEvaluationEventKind::AuthorityFenced,
        5 => NativeEvaluationEventKind::Sealed,
        _ => return Err(Error::InvalidTag("evaluation event kind")),
    })
}
pub(super) fn claim_kind(c: &mut Cursor<'_>) -> Result<NativeEventKind, Error> {
    Ok(match c.u8()? {
        0 => NativeEventKind::Monitor(monitor(c)?),
        1 => NativeEventKind::OwnerReleased,
        2 => NativeEventKind::Validating,
        3 => NativeEventKind::LocallyComplete,
        4 => NativeEventKind::ValidationIncomplete,
        5 => NativeEventKind::ValidationFailed,
        6 => NativeEventKind::ValidationErrored,
        7 => NativeEventKind::DependencyFailed,
        8 => NativeEventKind::Created,
        9 => NativeEventKind::ChildRegistered,
        10 => NativeEventKind::Superseded,
        11 => NativeEventKind::Cancelled,
        12 => NativeEventKind::Posted,
        13 => NativeEventKind::PostFailed,
        14 => NativeEventKind::Received,
        15 => NativeEventKind::ReceiptAdopted,
        16 => NativeEventKind::Satisfied,
        17 => NativeEventKind::TestamentGenerated,
        18 => NativeEventKind::TestamentAcknowledged,
        19 => NativeEventKind::ResponseObserved,
        20 => NativeEventKind::Expired,
        21 => NativeEventKind::Deadlocked,
        _ => return Err(Error::InvalidTag("claim event kind")),
    })
}
pub(super) fn monitor(c: &mut Cursor<'_>) -> Result<NativeMonitorEvent, Error> {
    Ok(match c.u8()? {
        0 => NativeMonitorEvent::Registered {
            id: MonitorId(c.fixed()?),
            cut: f::claim_cut(c)?,
        },
        1 => NativeMonitorEvent::Rebound {
            id: MonitorId(c.fixed()?),
            change: f::rebinding(c)?,
        },
        2 => NativeMonitorEvent::Released {
            id: MonitorId(c.fixed()?),
            cut: f::claim_cut(c)?,
        },
        3 => NativeMonitorEvent::Cancelled {
            id: MonitorId(c.fixed()?),
            cancellation: f::cancellation(c)?,
        },
        _ => return Err(Error::InvalidTag("monitor event kind")),
    })
}
