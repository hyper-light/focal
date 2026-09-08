//! Exact original publication facts. All private compact revisions expand using
//! the record's ledger, and are encoded without cloning retained events.
use super::*;
use bytes::{Error, Sink, write_raw as raw, write_u8, write_u32, write_u64};
use lifecycle_fields as f;

pub(super) fn event(s: &mut impl Sink, event: NativeEvent) -> Result<(), Error> {
    fixed::invocation(s, event.invocation)?;
    write_u64(s, event.sequence.0)?;
    write_u32(s, event.ordinal)?;
    match event.fact {
        NativeFact::ResultTestament {
            claim,
            before,
            after,
            state,
        } => {
            write_u8(s, 0)?;
            raw(s, &claim.0)?;
            f::optional_binding(s, before)?;
            types::binding(s, after)?;
            f::result_testament_state(s, state)
        }
        NativeFact::Missing { key } => {
            write_u8(s, 1)?;
            fixed::result_key(s, key)
        }
        NativeFact::Registrations { claim } => {
            write_u8(s, 2)?;
            types::binding(s, claim)
        }
        NativeFact::Delivery { key } => {
            write_u8(s, 3)?;
            fixed::result_key(s, key)
        }
        NativeFact::Work {
            claim,
            before,
            after,
            state,
        } => {
            write_u8(s, 4)?;
            raw(s, &claim.0)?;
            f::optional_binding(s, before)?;
            types::binding(s, after)?;
            f::work_state(s, state)
        }
        NativeFact::Diagnostic {
            claim,
            binding,
            reason,
        } => {
            write_u8(s, 5)?;
            raw(s, &claim.0)?;
            types::binding(s, binding)?;
            types::failure(s, reason)
        }
        NativeFact::Response {
            claim,
            before,
            after,
            state,
        } => {
            write_u8(s, 6)?;
            raw(s, &claim.0)?;
            f::optional_binding(s, before)?;
            types::binding(s, after)?;
            f::response_state(s, state)
        }
        NativeFact::Receipt {
            claim,
            fence,
            holder,
        } => {
            write_u8(s, 7)?;
            types::binding(s, claim)?;
            types::receipt(s, fence)?;
            raw(s, &holder.0)
        }
        NativeFact::ReceiptAdopted {
            claim,
            previous,
            replacement,
            cause,
        } => {
            write_u8(s, 8)?;
            types::binding(s, claim)?;
            f::entitlement(s, previous)?;
            f::entitlement(s, replacement)?;
            raw(s, &cause.0)
        }
        NativeFact::Artifact { binding } => {
            write_u8(s, 9)?;
            types::binding(s, binding)
        }
        NativeFact::Accepted { key } => {
            write_u8(s, 10)?;
            fixed::result_key(s, key)
        }
        NativeFact::Claim(v) => {
            s.visit(1)?;
            v.check_graph_capture(event.ordinal)
                .map_err(|_| Error::InvalidTag("graph capture"))?;
            write_u8(s, 11)?;
            claim_kind(s, v.kind)?;
            f::optional_binding(s, v.owned_child)?;
            f::optional_binding(s, v.before)?;
            types::binding(s, v.after)?;
            f::claim_status(s, v.status)?;
            f::optional(s, v.graph, |sink, value| {
                bytes::write_u32(sink, value.before_ordinal)
            })
        }
        NativeFact::Definition {
            binding,
            claim,
            index,
            intent,
        } => {
            write_u8(s, 12)?;
            types::binding(s, binding)?;
            raw(s, &claim.0)?;
            write_u32(s, index)?;
            raw(s, &intent.0)
        }
        NativeFact::Evaluation {
            kind,
            key,
            before,
            after,
            state,
            phase,
            attempt,
            fence,
        } => {
            write_u8(s, 13)?;
            write_u8(
                s,
                match kind {
                    NativeEvaluationEventKind::MissingTarget => 0,
                    NativeEvaluationEventKind::Materialized => 1,
                    NativeEvaluationEventKind::Begun => 2,
                    NativeEvaluationEventKind::Reported => 3,
                    NativeEvaluationEventKind::AuthorityFenced => 4,
                    NativeEvaluationEventKind::Sealed => 5,
                },
            )?;
            types::evaluation(s, key)?;
            f::optional_binding(s, before)?;
            types::binding(s, after)?;
            f::evaluation_state(s, state)?;
            f::phase(s, phase)?;
            match attempt {
                None => write_u8(s, 0)?,
                Some(attempt) => {
                    write_u8(s, 1)?;
                    types::attempt(s, attempt)?;
                }
            }
            f::optional_fence(s, fence)
        }
    }
}
fn claim_kind(s: &mut impl Sink, kind: NativeEventKind) -> Result<(), Error> {
    let tag = match kind {
        NativeEventKind::Monitor(_) => 0,
        NativeEventKind::OwnerReleased => 1,
        NativeEventKind::Validating => 2,
        NativeEventKind::LocallyComplete => 3,
        NativeEventKind::ValidationIncomplete => 4,
        NativeEventKind::ValidationFailed => 5,
        NativeEventKind::ValidationErrored => 6,
        NativeEventKind::DependencyFailed => 7,
        NativeEventKind::Created => 8,
        NativeEventKind::ChildRegistered => 9,
        NativeEventKind::Superseded => 10,
        NativeEventKind::Cancelled => 11,
        NativeEventKind::Posted => 12,
        NativeEventKind::PostFailed => 13,
        NativeEventKind::Received => 14,
        NativeEventKind::ReceiptAdopted => 15,
        NativeEventKind::Satisfied => 16,
        NativeEventKind::TestamentGenerated => 17,
        NativeEventKind::TestamentAcknowledged => 18,
        NativeEventKind::ResponseObserved => 19,
        NativeEventKind::Expired => 20,
        NativeEventKind::Deadlocked => 21,
    };
    write_u8(s, tag)?;
    if let NativeEventKind::Monitor(event) = kind {
        match event {
            NativeMonitorEvent::Registered { id, cut } => {
                write_u8(s, 0)?;
                raw(s, &id.0)?;
                f::claim_cut(s, cut)?;
            }
            NativeMonitorEvent::Rebound { id, change } => {
                write_u8(s, 1)?;
                raw(s, &id.0)?;
                f::rebinding(s, change)?;
            }
            NativeMonitorEvent::Released { id, cut } => {
                write_u8(s, 2)?;
                raw(s, &id.0)?;
                f::claim_cut(s, cut)?;
            }
            NativeMonitorEvent::Cancelled { id, cancellation } => {
                write_u8(s, 3)?;
                raw(s, &id.0)?;
                f::cancellation(s, cancellation)?;
            }
        }
    }
    Ok(())
}
