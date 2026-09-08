//! Direct native publications retain the operation that produced them.
//! Derived graph, monitor-release and cohort-seal facts may accompany several
//! operations and are checked against their actual successor witnesses instead.
use super::*;

#[cfg(test)]
#[path = "replay_validate_operation_tests.rs"]
mod tests;

pub(super) fn check(operation: NativeOperation, fact: NativeFact) -> Result<(), NativeError> {
    use NativeOperation as Op;
    let valid = match fact {
        NativeFact::Claim(event) => match event.kind {
            NativeEventKind::Created
            | NativeEventKind::ChildRegistered
            | NativeEventKind::Superseded => operation == Op::Create,
            NativeEventKind::Posted => operation == Op::Post,
            NativeEventKind::Received => operation == Op::AcquireReceipt,
            NativeEventKind::ReceiptAdopted => operation == Op::AdoptReceipt,
            NativeEventKind::TestamentGenerated => operation == Op::CloseResponse,
            NativeEventKind::TestamentAcknowledged => operation == Op::ReceiveResponse,
            NativeEventKind::ResponseObserved => matches!(
                operation,
                Op::CloseResponse | Op::PostResponse | Op::ReceiveResponse
            ),
            NativeEventKind::Validating => matches!(operation, Op::EnterWholeWork | Op::BeginWork),
            NativeEventKind::LocallyComplete => matches!(
                operation,
                Op::EnterWholeWork | Op::BeginWork | Op::ReportWork
            ),
            NativeEventKind::Cancelled => operation == Op::Cancel,
            NativeEventKind::Expired => {
                matches!(operation, Op::ClaimDeadline | Op::MonitorDeadline)
            }
            NativeEventKind::OwnerReleased => operation == Op::ReleaseScope,
            NativeEventKind::Monitor(NativeMonitorEvent::Registered { .. }) => {
                operation == Op::RegisterMonitor
            }
            NativeEventKind::Monitor(NativeMonitorEvent::Rebound { .. }) => {
                operation == Op::RebindMonitor
            }
            NativeEventKind::Monitor(NativeMonitorEvent::Cancelled { .. }) => {
                matches!(operation, Op::CancelMonitor | Op::MonitorDeadline)
            }
            _ => true,
        },
        NativeFact::Definition { .. } => operation == Op::Create,
        NativeFact::Receipt { .. } => operation == Op::AcquireReceipt,
        NativeFact::ReceiptAdopted { .. } => operation == Op::AdoptReceipt,
        NativeFact::Artifact { .. } => matches!(
            operation,
            Op::SubmitWork
                | Op::FailWorkProduction
                | Op::RejectWork
                | Op::SubmitDiagnostic
                | Op::ReportAdmission
                | Op::ReportIncrement
                | Op::ReportWork
        ),
        NativeFact::Diagnostic { .. } => operation == Op::SubmitDiagnostic,
        NativeFact::Delivery { .. } => operation == Op::ReceiveResponse,
        NativeFact::Missing { .. } => matches!(operation, Op::EnterWholeWork | Op::BeginWork),
        NativeFact::Accepted { key } => report(operation, key.evaluation.target),
        NativeFact::Evaluation { kind, key, .. } => match kind {
            NativeEvaluationEventKind::Materialized => match key.target {
                EvaluationTarget::Admission => operation == Op::Post,
                EvaluationTarget::Increment { .. } => operation == Op::SubmitWork,
                EvaluationTarget::Work { .. }
                | EvaluationTarget::MissingSlot { .. }
                | EvaluationTarget::Delivery { .. } => operation == Op::ReceiveResponse,
            },
            NativeEvaluationEventKind::Begun => match key.target {
                EvaluationTarget::Admission => operation == Op::BeginAdmission,
                EvaluationTarget::Increment { .. } => operation == Op::BeginIncrement,
                EvaluationTarget::Work { .. } => operation == Op::BeginWork,
                _ => false,
            },
            NativeEvaluationEventKind::Reported => report(operation, key.target),
            NativeEvaluationEventKind::MissingTarget => {
                matches!(operation, Op::EnterWholeWork | Op::BeginWork)
            }
            // Extras::evaluation emits the same authority-fence fact for
            // explicit evaluator expiry, receipt adoption, owner cancellation,
            // supersession, and either native claim-expiry timer. The recorded
            // fence reason and original claim cut are checked independently.
            NativeEvaluationEventKind::AuthorityFenced => matches!(
                operation,
                Op::EvaluationDeadline
                    | Op::AdoptReceipt
                    | Op::Cancel
                    | Op::Create
                    | Op::ClaimDeadline
                    | Op::MonitorDeadline
            ),
            NativeEvaluationEventKind::Sealed => true,
        },
        NativeFact::Work { state, .. } => match state {
            WorkArtifactState::Generated => operation == Op::SubmitWork,
            WorkArtifactState::GenerationFailed => operation == Op::FailWorkProduction,
            WorkArtifactState::Received => operation == Op::ReceiveWork,
            WorkArtifactState::ReceiptFailed => operation == Op::RejectWork,
            WorkArtifactState::Attached => operation == Op::CloseResponse,
            WorkArtifactState::Validating => {
                matches!(operation, Op::EnterWholeWork | Op::BeginWork)
            }
            WorkArtifactState::Validated | WorkArtifactState::ValidationFailed => matches!(
                operation,
                Op::EnterWholeWork | Op::BeginWork | Op::ReportWork
            ),
        },
        NativeFact::Response { state, .. } => match state {
            ResponseState::Generated => operation == Op::CloseResponse,
            ResponseState::Posted => operation == Op::PostResponse,
            ResponseState::Received => operation == Op::ReceiveResponse,
            ResponseState::Validating => matches!(operation, Op::EnterWholeWork | Op::BeginWork),
            _ => matches!(
                operation,
                Op::EnterWholeWork | Op::BeginWork | Op::ReportWork
            ),
        },
        NativeFact::ResultTestament { state, .. } => match state {
            focal_model::lifecycle::audit::ResultTestamentState::Generated => {
                operation == Op::GenerateResultTestament
            }
            focal_model::lifecycle::audit::ResultTestamentState::Posted => {
                operation == Op::PostResultTestament
            }
        },
        NativeFact::Registrations { .. } => true,
    };
    require(valid)
}
fn report(operation: NativeOperation, target: EvaluationTarget) -> bool {
    match target {
        EvaluationTarget::Admission => operation == NativeOperation::ReportAdmission,
        EvaluationTarget::Increment { .. } => operation == NativeOperation::ReportIncrement,
        EvaluationTarget::Work { .. } => operation == NativeOperation::ReportWork,
        _ => false,
    }
}
