//! R4.0 coverage table: every native operation of the committed prefix with
//! its authoring actor, exposure, result and retry identity. The match is
//! exhaustive so appending an owner operation forces an explicit decision.
use super::{WireProfile, native_catalog};
use focal_wire::NativeOperationKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeActor {
    Issuer,
    Subject,
    Evaluator,
    /// The current owner of a claim: issuer before a receipt, holder after.
    Owner,
    Internal,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeExposure {
    /// Exposed through a native descriptor (CLI, request files, MCP).
    AuthoredTool,
    /// A trusted node timer; never a participant frame.
    InternalTimer,
    /// The wire accepts the frame but no authored surface exists. Empty
    /// since the R4.5 verbs; kept so a future owner operation must choose.
    WireOnly,
    /// The legacy import record; produced only by offline activation.
    Activation,
    /// A retirement every replica applies; never a tool (26 §4).
    Retirement,
    /// A seal of closed outcomes every replica applies; never a tool (F12).
    Seal,
    /// A participant frame the client's own journal protocol issues (F12:
    /// the request generation floor), never an authored tool.
    ClientProtocol,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeCoverage {
    pub operation: NativeOperationKind,
    /// The `FCNINPUT1` command tags of participant frames producing this
    /// operation (codec numbering; creation has the projection and authored
    /// forms). Empty for trusted timers and the import.
    pub tags: &'static [u8],
    /// Descriptor name when exposed.
    pub name: Option<&'static str>,
    pub actor: NativeActor,
    /// Human CLI path when exposed; empty until the verb exists.
    pub cli: &'static str,
    pub result: &'static str,
    /// Reads the client performs before compiling the frame.
    pub reads: &'static str,
    pub exposure: NativeExposure,
}
const RETRY: &str = "Journal the exact frame under its n1: reference before send; resend the identical bytes after an unknown outcome; the owner resolves the request key before admission.";
/// The shared retry rule for every authored native mutation.
pub const NATIVE_RETRY: &str = RETRY;

const fn row(
    operation: NativeOperationKind,
    name: Option<&'static str>,
    actor: NativeActor,
    cli: &'static str,
    result: &'static str,
    reads: &'static str,
    exposure: NativeExposure,
) -> NativeCoverage {
    NativeCoverage {
        operation,
        tags: frame_tags(operation),
        name,
        actor,
        cli,
        result,
        reads,
        exposure,
    }
}
/// `FCNINPUT1` command bytes (`input_codec::encode::tag`) per operation.
pub const fn frame_tags(operation: NativeOperationKind) -> &'static [u8] {
    use NativeOperationKind as K;
    match operation {
        K::Create => &[0, 27],
        K::Cancel => &[1],
        K::Post => &[2],
        K::BeginAdmission => &[3],
        K::ReportAdmission => &[4],
        K::AcquireReceipt => &[5],
        K::SubmitWork => &[6],
        K::SubmitDiagnostic => &[7],
        K::ReceiveWork => &[8],
        K::CloseResponse => &[9],
        K::PostResponse => &[10],
        K::ReceiveResponse => &[11],
        K::FailWorkProduction => &[12],
        K::RejectWork => &[13],
        K::BeginIncrement => &[14],
        K::ReportIncrement => &[15],
        K::SealIncrementTargets => &[16],
        K::EnterWholeWork => &[17],
        K::BeginWork => &[18],
        K::ReportWork => &[19],
        K::GenerateResultTestament => &[20],
        K::PostResultTestament => &[21],
        K::AdoptReceipt => &[22],
        K::ReleaseScope => &[23],
        K::RegisterMonitor => &[24],
        K::RebindMonitor => &[25],
        K::CancelMonitor => &[26],
        K::AdvanceEpochFloor => &[28],
        K::MonitorDeadline
        | K::EvaluationDeadline
        | K::ClaimDeadline
        | K::Import
        | K::Retire
        | K::Seal => &[],
    }
}
pub const fn native_coverage(operation: NativeOperationKind) -> NativeCoverage {
    use NativeActor as A;
    use NativeExposure as E;
    use NativeOperationKind as K;
    match operation {
        K::Create => row(
            operation,
            Some("claim.submit"),
            A::Issuer,
            "focal submit claim",
            "Committed receipt with the creation result (requested and resolved ids)",
            "Parent claim when authored as a child",
            E::AuthoredTool,
        ),
        K::Post => row(
            operation,
            Some("claim.post"),
            A::Issuer,
            "focal post claim",
            "Committed receipt; claim Posted",
            "Claim binding",
            E::AuthoredTool,
        ),
        K::Cancel => row(
            operation,
            Some("claim.cancel"),
            A::Issuer,
            "focal cancel claim",
            "Committed receipt; claim Cancelled",
            "Claim binding",
            E::AuthoredTool,
        ),
        K::AcquireReceipt => row(
            operation,
            Some("receipt.acquire"),
            A::Subject,
            "focal acquire receipt",
            "Committed receipt; claim Received with the receipt fence",
            "Claim binding",
            E::AuthoredTool,
        ),
        K::SubmitWork => row(
            operation,
            Some("artifact.submit"),
            A::Subject,
            "focal submit artifact",
            "Committed receipt; work artifact recorded for the slot",
            "Claim binding, receipt fence and cycle",
            E::AuthoredTool,
        ),
        K::SubmitDiagnostic => row(
            operation,
            Some("artifact.diagnostic"),
            A::Subject,
            "focal submit diagnostic",
            "Committed receipt; diagnostic recorded",
            "Claim binding, receipt fence and cycle",
            E::AuthoredTool,
        ),
        K::CloseResponse => row(
            operation,
            Some("testament.submit"),
            A::Subject,
            "focal submit testament",
            "Committed receipt; response Generated",
            "Claim binding",
            E::AuthoredTool,
        ),
        K::PostResponse => row(
            operation,
            Some("testament.post"),
            A::Subject,
            "focal post testament",
            "Committed receipt; response Posted",
            "Claim and response bindings",
            E::AuthoredTool,
        ),
        K::ReceiveResponse => row(
            operation,
            Some("testament.receive"),
            A::Issuer,
            "focal receive testament",
            "Committed receipt; evaluations materialized",
            "Claim and response bindings",
            E::AuthoredTool,
        ),
        K::BeginWork => row(
            operation,
            Some("validation.begin"),
            A::Evaluator,
            "focal begin validation",
            "Committed receipt; evaluation Validating",
            "Claim binding and the current evaluation",
            E::AuthoredTool,
        ),
        K::ReportWork => row(
            operation,
            Some("validation.report"),
            A::Evaluator,
            "focal report verdict",
            "Committed receipt; accepted result and derived claim state",
            "Claim binding and the begun evaluation's attempt",
            E::AuthoredTool,
        ),
        K::RegisterMonitor => row(
            operation,
            Some("monitor.register"),
            A::Issuer,
            "focal register monitor",
            "Committed receipt; monitor registered under the claim",
            "Claim binding and its current receipt fence",
            E::AuthoredTool,
        ),
        K::RebindMonitor => row(
            operation,
            Some("monitor.rebind"),
            A::Issuer,
            "focal rebind monitor",
            "Committed receipt; monitor root rebound to the successor",
            "Claim, predecessor and successor bindings; current receipt fence",
            E::AuthoredTool,
        ),
        K::CancelMonitor => row(
            operation,
            Some("monitor.cancel"),
            A::Issuer,
            "focal cancel monitor",
            "Committed receipt; monitor cancelled",
            "Claim binding and its current receipt fence",
            E::AuthoredTool,
        ),
        K::ReleaseScope => row(
            operation,
            Some("claim.release_scope"),
            A::Issuer,
            "focal release scope",
            "Committed receipt; owned scope released",
            "Claim binding",
            E::AuthoredTool,
        ),
        K::GenerateResultTestament => row(
            operation,
            Some("audit.generate"),
            A::Issuer,
            "focal generate audit",
            "Committed receipt; result testament Generated",
            "Claim binding",
            E::AuthoredTool,
        ),
        K::PostResultTestament => row(
            operation,
            Some("audit.post"),
            A::Issuer,
            "focal post audit",
            "Committed receipt; result testament Posted",
            "Result testament binding",
            E::AuthoredTool,
        ),
        K::EnterWholeWork => row(
            operation,
            Some("validation.enter_whole_work"),
            A::Issuer,
            "focal enter whole-work",
            "Committed receipt; increment cohort closed and whole work entered",
            "Claim binding and the received testament binding",
            E::AuthoredTool,
        ),
        K::SealIncrementTargets => row(
            operation,
            Some("validation.seal_increments"),
            A::Issuer,
            "focal seal increments",
            "Committed receipt; increment targets sealed",
            "Claim binding",
            E::AuthoredTool,
        ),
        K::BeginIncrement => row(
            operation,
            Some("validation.begin"),
            A::Evaluator,
            "focal begin validation",
            "Committed receipt; increment evaluation Validating",
            "Claim binding and the current increment evaluation (phase increment)",
            E::AuthoredTool,
        ),
        K::ReportIncrement => row(
            operation,
            Some("validation.report"),
            A::Evaluator,
            "focal report verdict",
            "Committed receipt; increment result accepted",
            "Claim binding and the begun increment attempt (phase increment)",
            E::AuthoredTool,
        ),
        K::FailWorkProduction => row(
            operation,
            Some("artifact.fail"),
            A::Subject,
            "focal fail slot",
            "Committed receipt; slot recorded GenerationFailed with the diagnostic",
            "Claim binding and the committed production diagnostic",
            E::AuthoredTool,
        ),
        K::RejectWork => row(
            operation,
            Some("artifact.reject"),
            A::Issuer,
            "focal reject artifact",
            "Committed receipt; work ReceiptFailed with the rejection diagnostic",
            "Claim binding, the work artifact binding and its descriptor",
            E::AuthoredTool,
        ),
        K::ReceiveWork => row(
            operation,
            Some("artifact.receive"),
            A::Issuer,
            "focal receive artifact",
            "Committed receipt; work Received",
            "Claim binding and the work artifact binding",
            E::AuthoredTool,
        ),
        K::AdoptReceipt => row(
            operation,
            Some("receipt.adopt"),
            A::Issuer,
            "focal adopt receipt",
            "Committed receipt; new holder entitled one epoch later",
            "Claim binding and its current receipt fence",
            E::AuthoredTool,
        ),
        K::BeginAdmission => row(
            operation,
            Some("validation.begin"),
            A::Evaluator,
            "focal begin validation",
            "Committed receipt; admission evaluation Validating",
            "Claim binding and the current admission evaluation (phase admission)",
            E::AuthoredTool,
        ),
        K::ReportAdmission => row(
            operation,
            Some("validation.report"),
            A::Evaluator,
            "focal report verdict",
            "Committed receipt; admission result accepted",
            "Claim binding and the begun admission attempt (phase admission)",
            E::AuthoredTool,
        ),
        K::MonitorDeadline => row(
            operation,
            None,
            A::Internal,
            "",
            "Monitor released by its timer",
            "",
            E::InternalTimer,
        ),
        K::EvaluationDeadline => row(
            operation,
            None,
            A::Internal,
            "",
            "Evaluation fenced by its timer",
            "",
            E::InternalTimer,
        ),
        K::ClaimDeadline => row(
            operation,
            None,
            A::Internal,
            "",
            "Claim expired by its timer",
            "",
            E::InternalTimer,
        ),
        K::Import => row(
            operation,
            None,
            A::Internal,
            "",
            "Legacy history imported",
            "",
            E::Activation,
        ),
        K::Retire => row(
            operation,
            None,
            A::Internal,
            "",
            "A family of claims retired to the archive",
            "",
            E::Retirement,
        ),
        K::AdvanceEpochFloor => row(
            operation,
            None,
            A::Issuer,
            "",
            "Committed receipt; the principal's generations below the floor are closed",
            "The client's own journal: every operation of the closed generations delivered",
            E::ClientProtocol,
        ),
        K::Seal => row(
            operation,
            None,
            A::Internal,
            "",
            "Closed outcomes sealed into a bundle under custody",
            "",
            E::Seal,
        ),
    }
}
pub const OPERATIONS: usize = NativeOperationKind::ALL.len();
pub fn native_coverage_table() -> [NativeCoverage; OPERATIONS] {
    let mut rows = [native_coverage(NativeOperationKind::Import); OPERATIONS];
    for (slot, kind) in rows.iter_mut().zip(NativeOperationKind::ALL) {
        *slot = native_coverage(kind);
    }
    rows
}
impl NativeCoverage {
    pub fn descriptor(&self) -> Option<&'static super::OperationDescriptor> {
        let descriptor = native_catalog::find_native(self.name?)?;
        (descriptor.wire == WireProfile::Native).then_some(descriptor)
    }
}
