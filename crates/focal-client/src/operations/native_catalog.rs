//! Native descriptors (version 2). They share names with the V1 catalog; a
//! host exposes exactly one catalog per ledger, chosen by the active engine.
use super::{
    Capability, InputKind, OperationDescriptor, ResultKind, RetryIdentity, Surface, WireProfile,
};
use crate::input::MAX_INPUT_BYTES;
use focal_model::ObjectKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeInputKind {
    Claim,
    ClaimTarget,
    Receipt,
    WorkArtifact,
    Diagnostic,
    Response,
    ResponseTarget,
    Evaluation,
    Report,
    /// One object identity: `{ "id": ID }`.
    ObjectId,
    /// No fields: `{}`.
    Empty,
    ClaimList,
    ArtifactList,
    ValidationList,
    EvaluationList,
    TestamentList,
    ReceiptList,
    MonitorList,
    EventList,
    AdoptReceipt,
    FailWork,
    ArtifactTarget,
    RejectWork,
    Audit,
    AuditTarget,
    Monitor,
    MonitorRebind,
    MonitorTarget,
    Context,
    Challenge,
    Consult,
    Correction,
    FollowUp,
    Wait,
}
macro_rules! native_descriptor {
    ($symbol:ident,$name:literal,$input:ident,$family:expr,$destructive:expr,$description:literal) => {
        pub(super) const $symbol: OperationDescriptor = OperationDescriptor {
            name: $name,
            version: 2,
            description: $description,
            capability: Capability::Actor,
            mutation: true,
            destructive: $destructive,
            result_kind: ResultKind::Mutation,
            max_input_bytes: MAX_INPUT_BYTES,
            wire: WireProfile::Native,
            retry: RetryIdentity::NativeN1,
            surface: Surface::Application,
            cli_path: None,
            input: InputKind::Native(NativeInputKind::$input),
            family: $family,
        };
    };
}
macro_rules! native_read {
    ($symbol:ident,$name:literal,$input:ident,$family:expr,$description:literal) => {
        pub(super) const $symbol: OperationDescriptor = OperationDescriptor {
            name: $name,
            version: 2,
            description: $description,
            capability: Capability::Actor,
            mutation: false,
            destructive: false,
            result_kind: ResultKind::Read,
            max_input_bytes: MAX_INPUT_BYTES,
            wire: WireProfile::Native,
            retry: RetryIdentity::NativeN1,
            surface: Surface::Application,
            cli_path: None,
            input: InputKind::Native(NativeInputKind::$input),
            family: $family,
        };
    };
}
macro_rules! native_list {
    ($symbol:ident,$name:literal,$input:ident,$family:expr,$description:literal) => {
        pub(super) const $symbol: OperationDescriptor = OperationDescriptor {
            name: $name,
            version: 2,
            description: $description,
            capability: Capability::Actor,
            mutation: false,
            destructive: false,
            result_kind: ResultKind::List,
            max_input_bytes: MAX_INPUT_BYTES,
            wire: WireProfile::Native,
            retry: RetryIdentity::NativeN1,
            surface: Surface::Application,
            cli_path: None,
            input: InputKind::Native(NativeInputKind::$input),
            family: $family,
        };
    };
}
native_read!(
    NATIVE_CLAIM_LINEAGE,
    "claim.lineage",
    ObjectId,
    Some(ObjectKind::Claim),
    "Read one claim's lineage as a bounded page of committed claims: the claim itself with its content, its cause ancestors up the caused_by chain, the corrections that invalidate it, the consultations that refine it and the children it caused. Every object is an exact fixed-prefix claim read at or after the first read's token; roles follow from each claim's cause and relations."
);
native_read!(
    NATIVE_CLAIM_WAIT,
    "claim.wait",
    Wait,
    Some(ObjectKind::Claim),
    "Observe one native claim until a predicate holds: `testament` (the issuer has received a closing testament), `satisfied`, `terminal` or `released`. At most 31 fresh reads under one deadline of at most 30 seconds, one second apart, keeping only the latest observation; it creates no monitor, timer or identity."
);
native_list!(
    NATIVE_CLAIM_LIST,
    "claim.list",
    ClaimList,
    Some(ObjectKind::Claim),
    "List claims of the native prefix by issuer, subject, status, action, one authored scope, one authored relation to a claim, or creation after a prefix. One indexed predicate is scanned; the rest filter within max_visits, so a page may be empty and still continue. Follow the cursor until it is absent."
);
native_list!(
    NATIVE_ARTIFACT_LIST,
    "artifact.list",
    ArtifactList,
    Some(ObjectKind::Artifact),
    "List artifacts of the native prefix by producer, kind, payload schema hash or one cited input object. Bounded by max_visits with an opaque continuation cursor."
);
native_list!(
    NATIVE_VALIDATION_LIST,
    "validation.list",
    ValidationList,
    Some(ObjectKind::Validation),
    "List validation definitions of the native prefix by claim (in registration order) or designated evaluator. Bounded by max_visits with an opaque continuation cursor."
);
native_list!(
    NATIVE_EVALUATION_LIST,
    "evaluation.list",
    EvaluationList,
    Some(ObjectKind::Validation),
    "List current evaluations of the native prefix by claim, definition, designated evaluator or accepted verdict. A verdict selects the verdict index; a claim or definition scans that claim's evaluation keys. Bounded by max_visits with an opaque continuation cursor."
);
native_list!(
    NATIVE_TESTAMENT_LIST,
    "testament.list",
    TestamentList,
    Some(ObjectKind::Testament),
    "List the closed testaments (response cycles) of one claim, latest cycle first. Bounded by max_visits with an opaque continuation cursor."
);
native_list!(
    NATIVE_RECEIPT_LIST,
    "receipt.list",
    ReceiptList,
    Some(ObjectKind::Claim),
    "List receipts of the native prefix by holder or claim, in receipt identity order. Bounded by max_visits with an opaque continuation cursor."
);
native_list!(
    NATIVE_MONITOR_LIST,
    "monitor.list",
    MonitorList,
    Some(ObjectKind::Claim),
    "List the monitors registered on one claim in registration order, including retired ones with their disposition. Bounded by max_visits with an opaque continuation cursor."
);
native_list!(
    NATIVE_EVENT_LIST,
    "event.list",
    EventList,
    None,
    "List the native publication history after one event position (sequence and ordinal) in publication order. Bounded by max_visits with an opaque continuation cursor; empty records advance the position without an object."
);
native_read!(
    NATIVE_CLAIM_GET,
    "claim.get",
    ObjectId,
    Some(ObjectKind::Claim),
    "Read one claim of the native prefix with its content, scopes, response cycles and current evaluations at one linearizable fixed prefix. Statuses use the frozen numeric vocabulary."
);
native_read!(
    NATIVE_TESTAMENT_GET,
    "testament.get",
    ObjectId,
    Some(ObjectKind::Testament),
    "Read one closed testament (the response record with its manifest and diagnostics) and, when generated, its result testament, at one fixed prefix."
);
native_read!(
    NATIVE_ARTIFACT_GET,
    "artifact.get",
    ObjectId,
    Some(ObjectKind::Artifact),
    "Read one artifact record with its work or diagnostic role at one fixed prefix. Payload bytes are not returned; the record carries the content hash."
);
native_read!(
    NATIVE_VALIDATION_GET,
    "validation.get",
    ObjectId,
    Some(ObjectKind::Validation),
    "Read one validation definition and every evaluation of it at a prefix no older than the definition's, so the page never shows a definition newer than its evaluations."
);
native_read!(
    NATIVE_VALIDATION_CONTEXT,
    "validation.context",
    Context,
    Some(ObjectKind::Validation),
    "Read everything an evaluator needs for one declaration at one prefix: the claim, the definition, the selected registration and evaluation (by phase, slot or target like validation.begin), the target's manifest with custody, the accepted results after a revision cursor, and the delivery result of the same response."
);
native_read!(
    NATIVE_LEDGER_STANDING,
    "ledger.standing",
    Empty,
    None,
    "Read the authenticated principal's standing on the selected native ledger: principal, role, content profile, native prefix position and logical time. This is the engine probe every native host performs."
);
native_descriptor!(
    NATIVE_CLAIM_SUBMIT,
    "claim.submit",
    Claim,
    Some(ObjectKind::Claim),
    false,
    "Create one complete authored claim with its acceptance policy: description, subject, scopes, relations, declarations (one required whole-work delivery check plus any programmatic or agentic checks) and slot manifest. Creation is committed at a native prefix position; it does not post the claim or start work."
);
native_descriptor!(
    NATIVE_CLAIM_POST,
    "claim.post",
    ClaimTarget,
    Some(ObjectKind::Claim),
    false,
    "Post a created claim you issued, fenced by its committed binding read before the send. A stale binding is refused, never silently rebased."
);
native_descriptor!(
    NATIVE_CLAIM_CANCEL,
    "claim.cancel",
    ClaimTarget,
    Some(ObjectKind::Claim),
    true,
    "Cancel a claim you issued under its committed binding. Cancellation fences open evaluations and owned children; it records no failure testimony."
);
native_descriptor!(
    NATIVE_CLAIM_CHALLENGE,
    "claim.challenge",
    Challenge,
    Some(ObjectKind::Claim),
    false,
    "Author a challenge: a claim obliging its subject to prove or redo the stated work under explicit acceptance requirements, optionally disputing one exact committed artifact (`artifact`, `ID` or `ID@HASH`; the hash is read from the ledger when omitted) and carrying the immutable follow-up policy that decides who may correct or follow it up. An authored shape of claim.submit: same frame, same n1: identity, same receipt."
);
native_descriptor!(
    NATIVE_CLAIM_CONSULT,
    "claim.consult",
    Consult,
    Some(ObjectKind::Claim),
    false,
    "Author a consultation: a claim asking its subject for work answering a query under a quality bar, with the follow-up policy that bounds later consultations refining it. An authored shape of claim.submit."
);
native_descriptor!(
    NATIVE_CLAIM_CORRECT,
    "claim.correct",
    Correction,
    Some(ObjectKind::Claim),
    false,
    "Author the correction of a challenge whose verdict failed: it invalidates that committed challenge and reviews the exact report artifact of its terminal Fail, Incomplete or Error verdict (`verdict`, `ID` or `ID@HASH`). The owner admits it only under the challenge's policy (corrective_allowed, escalation, single_issuer) and at the current registration generation. Its occurrence identity derives from the challenge, the verdict and the author, so a repeated delivery resolves to one correction. An authored shape of claim.submit."
);
native_descriptor!(
    NATIVE_CLAIM_FOLLOW_UP,
    "claim.follow_up",
    FollowUp,
    Some(ObjectKind::Claim),
    false,
    "Author a follow-up consultation refining a committed consultation (`refines`), addressed to that consultation's subject unless a target is given. The refined claim's policy names who may file it and bounds how many follow-ups it takes. Its occurrence identity derives from the refined claim, the query and the author. An authored shape of claim.submit."
);
native_descriptor!(
    NATIVE_RECEIPT_ACQUIRE,
    "receipt.acquire",
    Receipt,
    Some(ObjectKind::Claim),
    false,
    "Acquire the receipt of a posted claim addressed to you. The receipt fence is the authority for every later work artifact and testament in this cycle."
);
native_descriptor!(
    NATIVE_ARTIFACT_SUBMIT,
    "artifact.submit",
    WorkArtifact,
    Some(ObjectKind::Artifact),
    false,
    "Submit work output for one manifest slot of the current cycle under your receipt. The payload is carried inline in the frame and verified by the node's content custody before the owner records it."
);
native_descriptor!(
    NATIVE_ARTIFACT_DIAGNOSTIC,
    "artifact.diagnostic",
    Diagnostic,
    Some(ObjectKind::Artifact),
    false,
    "Submit a diagnostic explaining failed or impossible work (reason work, production, structure or metadata) under your receipt, for citation by a non-complete testament."
);
native_descriptor!(
    NATIVE_TESTAMENT_SUBMIT,
    "testament.submit",
    Response,
    Some(ObjectKind::Testament),
    false,
    "Close the current work cycle with your explicit testimony: summary, confidence, outcome, the exact slot manifest of submitted work and any cited diagnostics. Every non-complete outcome needs a diagnostic. Closing records testimony; it does not establish acceptance."
);
native_descriptor!(
    NATIVE_TESTAMENT_POST,
    "testament.post",
    ResponseTarget,
    Some(ObjectKind::Testament),
    false,
    "Post your closed testament to the issuer under its committed binding."
);
native_descriptor!(
    NATIVE_TESTAMENT_RECEIVE,
    "testament.receive",
    ResponseTarget,
    Some(ObjectKind::Testament),
    false,
    "Receive a posted testament as the claim issuer. Receipt materializes the whole-work evaluations declared for the manifest; it is not acceptance."
);
native_descriptor!(
    NATIVE_VALIDATION_BEGIN,
    "validation.begin",
    Evaluation,
    Some(ObjectKind::Validation),
    false,
    "Begin the current whole-work evaluation of one declaration as its designated evaluator, fenced by the evaluation's committed binding. Beginning records the attempt; it invokes no tool."
);
native_descriptor!(
    NATIVE_VALIDATION_REPORT,
    "validation.report",
    Report,
    Some(ObjectKind::Validation),
    false,
    "Report the begun attempt's verdict (pass, fail, incomplete or error) with its typed result artifact bound to the exact target, generation and attempt. Error and incomplete verdicts carry an error report; the owner derives acceptance."
);
native_descriptor!(
    NATIVE_CLAIM_RELEASE_SCOPE,
    "claim.release_scope",
    ClaimTarget,
    Some(ObjectKind::Claim),
    false,
    "Release the owned scope of one of your terminal claims so its dependents and monitors settle. Refused while the claim is active or already released."
);
native_descriptor!(
    NATIVE_RECEIPT_ADOPT,
    "receipt.adopt",
    AdoptReceipt,
    Some(ObjectKind::Claim),
    false,
    "As the issuer, replace the claim's current holder: the committed receipt is fenced as the previous entitlement, the named participant receives a new receipt one epoch later, and old testimony stays attributed to its holder."
);
native_descriptor!(
    NATIVE_ARTIFACT_FAIL,
    "artifact.fail",
    FailWork,
    Some(ObjectKind::Artifact),
    false,
    "As the holder, record that one manifest slot cannot be produced, citing your committed production diagnostic (artifact.diagnostic with reason production) as the slot's failed work product."
);
native_descriptor!(
    NATIVE_ARTIFACT_RECEIVE,
    "artifact.receive",
    ArtifactTarget,
    Some(ObjectKind::Artifact),
    false,
    "As the issuer, receive one generated work artifact, fenced by its committed binding."
);
native_descriptor!(
    NATIVE_ARTIFACT_REJECT,
    "artifact.reject",
    RejectWork,
    Some(ObjectKind::Artifact),
    false,
    "As the issuer, reject one generated or received work artifact for a structure or metadata failure with your own diagnostic artifact (defaults to the builtin error report schema) bound to that exact work product."
);
native_descriptor!(
    NATIVE_VALIDATION_SEAL_INCREMENTS,
    "validation.seal_increments",
    ClaimTarget,
    Some(ObjectKind::Validation),
    false,
    "As the issuer, seal the claim's registered increment targets so no further increment evaluation is registered; begun checks still complete."
);
native_descriptor!(
    NATIVE_VALIDATION_ENTER_WHOLE_WORK,
    "validation.enter_whole_work",
    ResponseTarget,
    Some(ObjectKind::Validation),
    false,
    "As the issuer, close the increment cohort of the received testament and enter whole-work evaluation of its manifest."
);
native_descriptor!(
    NATIVE_AUDIT_GENERATE,
    "audit.generate",
    Audit,
    Some(ObjectKind::Testament),
    false,
    "As the issuer of a closed claim, generate its result testament: the audit of every accepted result, delivery and missing position at this prefix."
);
native_descriptor!(
    NATIVE_AUDIT_POST,
    "audit.post",
    AuditTarget,
    Some(ObjectKind::Testament),
    false,
    "As the issuer, post a generated result testament, fenced by its committed binding."
);
native_descriptor!(
    NATIVE_MONITOR_REGISTER,
    "monitor.register",
    Monitor,
    Some(ObjectKind::Claim),
    false,
    "As the issuer, register a durable wait monitor on your claim over committed claims (satisfied, terminal or released) with a logical-time deadline; the claim's current receipt is fenced."
);
native_descriptor!(
    NATIVE_MONITOR_REBIND,
    "monitor.rebind",
    MonitorRebind,
    Some(ObjectKind::Claim),
    false,
    "As the issuer, rebind one monitor's root from a predecessor claim to its committed successor."
);
native_descriptor!(
    NATIVE_MONITOR_CANCEL,
    "monitor.cancel",
    MonitorTarget,
    Some(ObjectKind::Claim),
    true,
    "As the issuer, cancel one of your claim's monitors; its registration and disposition stay readable."
);
/// Name order is part of catalog pagination and digest stability.
pub fn native_descriptors() -> &'static [OperationDescriptor] {
    &[
        NATIVE_ARTIFACT_DIAGNOSTIC,
        NATIVE_ARTIFACT_FAIL,
        NATIVE_ARTIFACT_GET,
        NATIVE_ARTIFACT_LIST,
        NATIVE_ARTIFACT_RECEIVE,
        NATIVE_ARTIFACT_REJECT,
        NATIVE_ARTIFACT_SUBMIT,
        NATIVE_AUDIT_GENERATE,
        NATIVE_AUDIT_POST,
        NATIVE_CLAIM_CANCEL,
        NATIVE_CLAIM_CHALLENGE,
        NATIVE_CLAIM_CONSULT,
        NATIVE_CLAIM_CORRECT,
        NATIVE_CLAIM_FOLLOW_UP,
        NATIVE_CLAIM_GET,
        NATIVE_CLAIM_LINEAGE,
        NATIVE_CLAIM_LIST,
        NATIVE_CLAIM_POST,
        NATIVE_CLAIM_RELEASE_SCOPE,
        NATIVE_CLAIM_SUBMIT,
        NATIVE_CLAIM_WAIT,
        NATIVE_EVALUATION_LIST,
        NATIVE_EVENT_LIST,
        NATIVE_LEDGER_STANDING,
        NATIVE_MONITOR_CANCEL,
        NATIVE_MONITOR_LIST,
        NATIVE_MONITOR_REBIND,
        NATIVE_MONITOR_REGISTER,
        NATIVE_RECEIPT_ACQUIRE,
        NATIVE_RECEIPT_ADOPT,
        NATIVE_RECEIPT_LIST,
        NATIVE_TESTAMENT_GET,
        NATIVE_TESTAMENT_LIST,
        NATIVE_TESTAMENT_POST,
        NATIVE_TESTAMENT_RECEIVE,
        NATIVE_TESTAMENT_SUBMIT,
        NATIVE_VALIDATION_BEGIN,
        NATIVE_VALIDATION_CONTEXT,
        NATIVE_VALIDATION_ENTER_WHOLE_WORK,
        NATIVE_VALIDATION_GET,
        NATIVE_VALIDATION_LIST,
        NATIVE_VALIDATION_REPORT,
        NATIVE_VALIDATION_SEAL_INCREMENTS,
    ]
}
pub fn find_native(name: &str) -> Option<&'static OperationDescriptor> {
    native_descriptors().iter().find(|value| value.name == name)
}
/// The frame operation an authored shape compiles to: the peer verbs are
/// typed documents of `claim.submit` (the same `CreateAuthored` frame, `n1:`
/// identity and receipt), so the coverage table claims them through that
/// descriptor rather than through a frame tag of their own.
pub fn authored_shape(descriptor: &OperationDescriptor) -> Option<&'static str> {
    match descriptor.input {
        InputKind::Native(
            NativeInputKind::Challenge
            | NativeInputKind::Consult
            | NativeInputKind::Correction
            | NativeInputKind::FollowUp,
        ) => Some(NATIVE_CLAIM_SUBMIT.name),
        _ => None,
    }
}
