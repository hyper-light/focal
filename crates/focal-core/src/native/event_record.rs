//! Projections of committed native events into the shared event vocabulary
//! (`focal_model::native_event`) and into version-2 deltas. Everything here
//! copies committed facts; nothing decodes a frame, admits a mutation or
//! interprets a fact as a satisfaction claim. The wire documents and the
//! delta stream are built from these same functions so both carry one shape.
use super::{
    EvaluationKey, EvaluationTarget, NativeClaimEvent, NativeEvaluationEventKind as EventKind,
    NativeEvent, NativeEventKind, NativeFact, NativeInvocation, NativeMonitorEvent,
    NativeResultKey,
};
use focal_model::lifecycle::{Binding, audit, claim, evidence, validation};
use focal_model::native_event::*;
use focal_model::{
    ClaimId, Delta, DeltaFact, DeltaId, LedgerId, LifecycleAction, NATIVE_DELTA_SCHEMA,
    ParticipantId, SessionSeq,
};

#[cfg(test)]
#[path = "event_record_tests.rs"]
mod tests;

pub fn binding(value: Binding) -> NativeBinding {
    NativeBinding {
        object: value.object,
        content: value.content,
        revision: value.revision,
    }
}
pub fn cut(value: claim::ClaimCut) -> NativeCut {
    NativeCut {
        position: value.position,
        cause: value.cause,
    }
}
pub fn evaluation_target(target: EvaluationTarget) -> NativeEvaluationTarget {
    match target {
        EvaluationTarget::Admission => NativeEvaluationTarget::Admission,
        EvaluationTarget::Increment { artifact } => NativeEvaluationTarget::Increment { artifact },
        EvaluationTarget::Work {
            response,
            slot,
            artifact,
        } => NativeEvaluationTarget::Work {
            response,
            slot,
            artifact,
        },
        EvaluationTarget::MissingSlot { response, slot } => {
            NativeEvaluationTarget::MissingSlot { response, slot }
        }
        EvaluationTarget::Delivery { response } => NativeEvaluationTarget::Delivery { response },
    }
}
pub fn evaluation_target_of(target: NativeEvaluationTarget) -> EvaluationTarget {
    match target {
        NativeEvaluationTarget::Admission => EvaluationTarget::Admission,
        NativeEvaluationTarget::Increment { artifact } => EvaluationTarget::Increment { artifact },
        NativeEvaluationTarget::Work {
            response,
            slot,
            artifact,
        } => EvaluationTarget::Work {
            response,
            slot,
            artifact,
        },
        NativeEvaluationTarget::MissingSlot { response, slot } => {
            EvaluationTarget::MissingSlot { response, slot }
        }
        NativeEvaluationTarget::Delivery { response } => EvaluationTarget::Delivery { response },
    }
}
pub fn evaluation_key(key: EvaluationKey) -> NativeEvaluationKey {
    NativeEvaluationKey {
        claim: key.claim,
        validation: key.validation,
        target: evaluation_target(key.target),
        generation: key.generation,
    }
}
pub fn evaluation_key_of(key: NativeEvaluationKey) -> EvaluationKey {
    EvaluationKey {
        claim: key.claim,
        validation: key.validation,
        target: evaluation_target_of(key.target),
        generation: key.generation,
    }
}
pub fn result_ref(key: NativeResultKey) -> NativeResultRef {
    NativeResultRef {
        evaluation: evaluation_key(key.evaluation),
        revision: key.revision,
    }
}
pub fn result_key_of(key: NativeResultRef) -> NativeResultKey {
    NativeResultKey {
        evaluation: evaluation_key_of(key.evaluation),
        revision: key.revision,
    }
}
pub fn invocation(value: NativeInvocation) -> NativeInvocationRef {
    match value {
        NativeInvocation::Request(key) => NativeInvocationRef::Request(key),
        NativeInvocation::EvaluationDeadline(key) => NativeInvocationRef::EvaluationDeadline {
            evaluation: evaluation_key(key.evaluation),
            timer: key.timer,
            generation: key.generation,
        },
        NativeInvocation::ClaimDeadline(key) => NativeInvocationRef::ClaimDeadline {
            claim: key.claim,
            timer: key.timer,
            generation: key.generation,
        },
        NativeInvocation::MonitorDeadline(key) => NativeInvocationRef::MonitorDeadline {
            claim: key.claim,
            monitor: key.monitor,
            timer: key.timer,
            generation: key.generation,
        },
        NativeInvocation::Import => NativeInvocationRef::Import,
        NativeInvocation::Retirement(root) => NativeInvocationRef::Retirement { root },
    }
}
pub fn phase(value: validation::Phase) -> NativePhase {
    match value {
        validation::Phase::Programmatic => NativePhase::Programmatic,
        validation::Phase::Quality => NativePhase::Quality,
        validation::Phase::Delivery => NativePhase::Delivery,
        validation::Phase::MissingTarget => NativePhase::MissingTarget,
    }
}
pub fn validation_state(value: validation::State) -> NativeValidationState {
    use validation::State as S;
    match value {
        S::Ready => NativeValidationState::Ready,
        S::Validating => NativeValidationState::Validating,
        S::ValidatingQualityBar => NativeValidationState::ValidatingQualityBar,
        S::Validated => NativeValidationState::Validated,
        S::ValidationIncomplete => NativeValidationState::ValidationIncomplete,
        S::ValidationFailed => NativeValidationState::ValidationFailed,
        S::ValidationFailedNotRequired => NativeValidationState::ValidationFailedNotRequired,
        S::Errored => NativeValidationState::Errored,
        S::ErroredNotRequired => NativeValidationState::ErroredNotRequired,
        S::QualityBarValidationFailed => NativeValidationState::QualityBarValidationFailed,
        S::QualityBarValidationFailedNotRequired => {
            NativeValidationState::QualityBarValidationFailedNotRequired
        }
    }
}
pub fn attempt(value: validation::Attempt) -> NativeAttempt {
    NativeAttempt {
        phase: phase(value.phase),
        index: value.index,
        handler: value.handler,
        version: value.version,
        evaluator: value.evaluator,
        definition: value.definition,
    }
}
pub fn fence(value: validation::AuthorityFence) -> NativeAuthorityFence {
    NativeAuthorityFence {
        reason: match value.reason {
            validation::FenceReason::Cancellation => NativeFenceReason::Cancellation,
            validation::FenceReason::Revocation => NativeFenceReason::Revocation,
            validation::FenceReason::Supersession => NativeFenceReason::Supersession,
            validation::FenceReason::Expiry => NativeFenceReason::Expiry,
            validation::FenceReason::ReceiptAdoption => NativeFenceReason::ReceiptAdoption,
            validation::FenceReason::Evaluation => NativeFenceReason::Evaluation,
            validation::FenceReason::Deadline(deadline) => NativeFenceReason::Deadline(deadline),
        },
        cause: value.cause,
    }
}
pub fn entitlement(value: claim::ReceiptEntitlement) -> NativeEntitlement {
    NativeEntitlement {
        holder: value.holder,
        fence: value.fence,
    }
}
pub fn failure(value: evidence::EvidenceFailure) -> NativeEvidenceFailure {
    match value {
        evidence::EvidenceFailure::Work => NativeEvidenceFailure::Work,
        evidence::EvidenceFailure::Production => NativeEvidenceFailure::Production,
        evidence::EvidenceFailure::Structure => NativeEvidenceFailure::Structure,
        evidence::EvidenceFailure::Metadata => NativeEvidenceFailure::Metadata,
    }
}
pub fn work_state(value: evidence::WorkArtifactState) -> NativeWorkArtifactState {
    use evidence::WorkArtifactState as S;
    match value {
        S::Generated => NativeWorkArtifactState::Generated,
        S::GenerationFailed => NativeWorkArtifactState::GenerationFailed,
        S::Received => NativeWorkArtifactState::Received,
        S::ReceiptFailed => NativeWorkArtifactState::ReceiptFailed,
        S::Attached => NativeWorkArtifactState::Attached,
        S::Validating => NativeWorkArtifactState::Validating,
        S::Validated => NativeWorkArtifactState::Validated,
        S::ValidationFailed => NativeWorkArtifactState::ValidationFailed,
    }
}
pub fn response_state(value: evidence::ResponseState) -> NativeResponseState {
    use evidence::ResponseState as S;
    match value {
        S::Generated => NativeResponseState::Generated,
        S::Posted => NativeResponseState::Posted,
        S::Received => NativeResponseState::Received,
        S::Validating => NativeResponseState::Validating,
        S::Validated => NativeResponseState::Validated,
        S::ValidationIncomplete => NativeResponseState::ValidationIncomplete,
        S::ValidationFailed => NativeResponseState::ValidationFailed,
        S::ValidationErrored => NativeResponseState::ValidationErrored,
    }
}
pub fn testament_state(value: audit::ResultTestamentState) -> NativeResultTestamentState {
    match value {
        audit::ResultTestamentState::Generated => NativeResultTestamentState::Generated,
        audit::ResultTestamentState::Posted => NativeResultTestamentState::Posted,
    }
}
fn event_kind(value: NativeEventKind) -> NativeEventKindRecord {
    use NativeEventKind as K;
    match value {
        K::Monitor(event) => NativeEventKindRecord::Monitor(match event {
            NativeMonitorEvent::Registered { id, cut: value } => {
                NativeMonitorEventRecord::Registered {
                    id,
                    cut: cut(value),
                }
            }
            NativeMonitorEvent::Rebound { id, change } => NativeMonitorEventRecord::Rebound {
                id,
                predecessor: change.predecessor,
                successor: change.successor,
                cut: cut(change.cut),
            },
            NativeMonitorEvent::Released { id, cut: value } => NativeMonitorEventRecord::Released {
                id,
                cut: cut(value),
            },
            NativeMonitorEvent::Cancelled { id, cancellation } => {
                NativeMonitorEventRecord::Cancelled {
                    id,
                    terminal: cancellation.terminal,
                    cut: cut(cancellation.cut),
                }
            }
        }),
        K::OwnerReleased => NativeEventKindRecord::OwnerReleased,
        K::Validating => NativeEventKindRecord::Validating,
        K::LocallyComplete => NativeEventKindRecord::LocallyComplete,
        K::ValidationIncomplete => NativeEventKindRecord::ValidationIncomplete,
        K::ValidationFailed => NativeEventKindRecord::ValidationFailed,
        K::ValidationErrored => NativeEventKindRecord::ValidationErrored,
        K::DependencyFailed => NativeEventKindRecord::DependencyFailed,
        K::Created => NativeEventKindRecord::Created,
        K::ChildRegistered => NativeEventKindRecord::ChildRegistered,
        K::Superseded => NativeEventKindRecord::Superseded,
        K::Cancelled => NativeEventKindRecord::Cancelled,
        K::Posted => NativeEventKindRecord::Posted,
        K::PostFailed => NativeEventKindRecord::PostFailed,
        K::Received => NativeEventKindRecord::Received,
        K::ReceiptAdopted => NativeEventKindRecord::ReceiptAdopted,
        K::Satisfied => NativeEventKindRecord::Satisfied,
        K::TestamentGenerated => NativeEventKindRecord::TestamentGenerated,
        K::TestamentAcknowledged => NativeEventKindRecord::TestamentAcknowledged,
        K::ResponseObserved => NativeEventKindRecord::ResponseObserved,
        K::Expired => NativeEventKindRecord::Expired,
        K::Deadlocked => NativeEventKindRecord::Deadlocked,
        K::Imported(sequence) => NativeEventKindRecord::Imported(sequence),
    }
}
fn claim_event(event: NativeClaimEvent) -> NativeClaimEventRecord {
    NativeClaimEventRecord {
        kind: event_kind(event.kind),
        graph_before_ordinal: event.graph.map(|capture| capture.before_ordinal),
        owned_child: event.owned_child.map(binding),
        before: event.before.map(binding),
        after: binding(event.after),
        status: event.status,
    }
}
/// The committed event in the shared vocabulary; sequence and ordinal are the
/// native record position.
pub fn event_record(value: NativeEvent) -> NativeEventRecord {
    use NativeFact as F;
    let fact = match value.fact {
        F::ResultTestament {
            claim,
            before,
            after,
            state,
        } => NativeFactRecord::ResultTestament {
            claim,
            before: before.map(binding),
            after: binding(after),
            state: testament_state(state),
        },
        F::Missing { key } => NativeFactRecord::Missing {
            key: result_ref(key),
        },
        F::Registrations { claim } => NativeFactRecord::Registrations {
            claim: binding(claim),
        },
        F::Delivery { key } => NativeFactRecord::Delivery {
            key: result_ref(key),
        },
        F::Work {
            claim,
            before,
            after,
            state,
        } => NativeFactRecord::Work {
            claim,
            before: before.map(binding),
            after: binding(after),
            state: work_state(state),
        },
        F::Diagnostic {
            claim,
            binding: value,
            reason,
        } => NativeFactRecord::Diagnostic {
            claim,
            binding: binding(value),
            reason: failure(reason),
        },
        F::Response {
            claim,
            before,
            after,
            state,
        } => NativeFactRecord::Response {
            claim,
            before: before.map(binding),
            after: binding(after),
            state: response_state(state),
        },
        F::Receipt {
            claim,
            fence,
            holder,
        } => NativeFactRecord::Receipt {
            claim: binding(claim),
            fence,
            holder,
        },
        F::ReceiptAdopted {
            claim,
            previous,
            replacement,
            cause,
        } => NativeFactRecord::ReceiptAdopted {
            claim: binding(claim),
            previous: entitlement(previous),
            replacement: entitlement(replacement),
            cause,
        },
        F::Artifact { binding: value } => NativeFactRecord::Artifact {
            binding: binding(value),
        },
        F::Accepted { key } => NativeFactRecord::Accepted {
            key: result_ref(key),
        },
        F::Claim(event) => NativeFactRecord::Claim(claim_event(event)),
        F::Definition {
            binding: value,
            claim,
            index,
            intent,
        } => NativeFactRecord::Definition {
            binding: binding(value),
            claim,
            index,
            intent,
        },
        F::Evaluation {
            kind,
            key,
            before,
            after,
            state,
            phase: value_phase,
            attempt: value_attempt,
            fence: value_fence,
        } => NativeFactRecord::Evaluation {
            kind: match kind {
                EventKind::MissingTarget => NativeEvaluationEventKind::MissingTarget,
                EventKind::Materialized => NativeEvaluationEventKind::Materialized,
                EventKind::Begun => NativeEvaluationEventKind::Begun,
                EventKind::Reported => NativeEvaluationEventKind::Reported,
                EventKind::AuthorityFenced => NativeEvaluationEventKind::AuthorityFenced,
                EventKind::Sealed => NativeEvaluationEventKind::Sealed,
            },
            key: evaluation_key(key),
            before: before.map(binding),
            after: binding(after),
            state: validation_state(state),
            phase: phase(value_phase),
            attempt: value_attempt.map(attempt),
            fence: value_fence.map(fence),
        },
    };
    NativeEventRecord {
        invocation: invocation(value.invocation),
        sequence: value.sequence,
        ordinal: value.ordinal,
        fact,
    }
}

/// The claim a fact concerns: the one a claim-filtered stream selects on.
/// A registered artifact belongs to no claim until work names it.
pub fn delta_claim(fact: &NativeFact) -> Option<ClaimId> {
    use NativeFact as F;
    let of = |binding: Binding| ClaimId(binding.object.0);
    match fact {
        F::ResultTestament { claim, .. }
        | F::Work { claim, .. }
        | F::Diagnostic { claim, .. }
        | F::Response { claim, .. }
        | F::Definition { claim, .. } => Some(*claim),
        F::Missing { key } | F::Delivery { key } | F::Accepted { key } => {
            Some(key.evaluation.claim)
        }
        F::Registrations { claim } | F::Receipt { claim, .. } | F::ReceiptAdopted { claim, .. } => {
            Some(of(*claim))
        }
        F::Claim(event) => Some(of(event.after)),
        F::Evaluation { key, .. } => Some(key.claim),
        F::Artifact { .. } => None,
    }
}
fn action_of_validation(state: validation::State) -> LifecycleAction {
    use validation::State as S;
    match state {
        S::Ready => LifecycleAction::ValidationScheduled,
        S::Validating | S::ValidatingQualityBar => LifecycleAction::Validating,
        S::Validated => LifecycleAction::ValidationVerdict,
        S::ValidationIncomplete => LifecycleAction::ValidationIncomplete,
        S::ValidationFailed
        | S::ValidationFailedNotRequired
        | S::QualityBarValidationFailed
        | S::QualityBarValidationFailedNotRequired => LifecycleAction::ValidationFailed,
        S::Errored | S::ErroredNotRequired => LifecycleAction::ValidationErrored,
    }
}
/// The nearest legacy lifecycle action for coarse consumers of the stream.
/// It never adds meaning the record lacks: the record is the authority.
pub fn delta_action(fact: &NativeFact) -> LifecycleAction {
    use NativeEventKind as K;
    use NativeFact as F;
    match fact {
        F::Claim(event) => match event.kind {
            K::Created => LifecycleAction::Generated,
            K::Posted => LifecycleAction::Posted,
            K::PostFailed => LifecycleAction::PostFailed,
            K::Received => LifecycleAction::Received,
            K::ReceiptAdopted => LifecycleAction::ReceiptAdopted,
            K::ChildRegistered | K::ResponseObserved => LifecycleAction::Progressed,
            K::TestamentGenerated => LifecycleAction::TestamentGenerated,
            K::TestamentAcknowledged => LifecycleAction::TestamentAcknowledged,
            K::Validating => LifecycleAction::Validating,
            K::Satisfied => LifecycleAction::Satisfied,
            K::ValidationIncomplete => LifecycleAction::ValidationIncomplete,
            K::ValidationFailed => LifecycleAction::ValidationFailed,
            K::ValidationErrored => LifecycleAction::ValidationErrored,
            K::Cancelled => LifecycleAction::Cancelled,
            K::Expired => LifecycleAction::Expired,
            K::Superseded => LifecycleAction::Superseded,
            K::DependencyFailed => LifecycleAction::DependencyFailed,
            K::Deadlocked => LifecycleAction::Deadlocked,
            K::LocallyComplete => LifecycleAction::LocalCompleted,
            K::OwnerReleased => LifecycleAction::ScopeReleased,
            K::Monitor(NativeMonitorEvent::Registered { .. }) => LifecycleAction::ScopeRegistered,
            K::Monitor(NativeMonitorEvent::Rebound { .. }) => LifecycleAction::ScopeRebound,
            K::Monitor(
                NativeMonitorEvent::Released { .. } | NativeMonitorEvent::Cancelled { .. },
            ) => LifecycleAction::ScopeReleased,
            K::Imported(_) => LifecycleAction::from(event.status),
        },
        F::ResultTestament { .. } => LifecycleAction::TestamentGenerated,
        F::Missing { .. } | F::Delivery { .. } | F::Accepted { .. } => {
            LifecycleAction::ValidationVerdict
        }
        F::Registrations { .. } | F::Definition { .. } => LifecycleAction::ValidationScheduled,
        F::Evaluation { state, .. } => action_of_validation(*state),
        F::Work { state, .. } => {
            use evidence::WorkArtifactState as S;
            match state {
                S::Generated | S::Attached | S::Received => LifecycleAction::ArtifactAttached,
                S::GenerationFailed => LifecycleAction::TestamentGenerationFailed,
                S::ReceiptFailed => LifecycleAction::ReceiptFailed,
                S::Validating => LifecycleAction::Validating,
                S::Validated => LifecycleAction::ValidationVerdict,
                S::ValidationFailed => LifecycleAction::ValidationFailed,
            }
        }
        F::Diagnostic { .. } | F::Artifact { .. } => LifecycleAction::ArtifactAttached,
        F::Response { state, .. } => {
            use evidence::ResponseState as S;
            match state {
                S::Generated | S::Posted => LifecycleAction::TestamentGenerated,
                S::Received => LifecycleAction::TestamentAcknowledged,
                S::Validating => LifecycleAction::Validating,
                S::Validated => LifecycleAction::ValidationVerdict,
                S::ValidationIncomplete => LifecycleAction::ValidationIncomplete,
                S::ValidationFailed => LifecycleAction::ValidationFailed,
                S::ValidationErrored => LifecycleAction::ValidationErrored,
            }
        }
        F::Receipt { .. } => LifecycleAction::Received,
        F::ReceiptAdopted { .. } => LifecycleAction::ReceiptAdopted,
    }
}
/// The principal a fact is attributed to: the request's authenticated
/// principal, or the zero participant for trusted timers and the import.
pub fn delta_actor(invocation: NativeInvocation) -> ParticipantId {
    match invocation {
        NativeInvocation::Request(key) => key.principal,
        NativeInvocation::EvaluationDeadline(_)
        | NativeInvocation::ClaimDeadline(_)
        | NativeInvocation::MonitorDeadline(_)
        | NativeInvocation::Import
        | NativeInvocation::Retirement(_) => ParticipantId::default(),
    }
}
/// One version-2 delta at the ledger's stream position `sequence` (the
/// Session maps native sequences onto its continuous stream line, 23 §6).
pub fn delta(ledger: LedgerId, sequence: SessionSeq, event: NativeEvent) -> Delta {
    Delta {
        schema: NATIVE_DELTA_SCHEMA,
        id: DeltaId {
            ledger,
            sequence,
            ordinal: event.ordinal,
        },
        action: delta_action(&event.fact),
        actor: delta_actor(event.invocation),
        claim: delta_claim(&event.fact),
        fact: DeltaFact::Native(Box::new(event_record(event))),
    }
}
