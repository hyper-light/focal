//! Authored documents for the native engine (wire profile 4). These DTOs are
//! shared by the CLI, JSON/YAML request files and MCP; the host-side compiler
//! in `focal-native-client` turns them into `FCNINPUT1` frames. Human
//! spellings stay separate from the model's frozen numeric encodings.
use super::{InputError, LimitedJson, OperationDescriptor, native_catalog};
use crate::input::{InputFormat, parse_document};
use serde::{Deserialize, Serialize};

/// The native verbs share names with their V1 counterparts at descriptor
/// version 2: a host selects the catalog by the ledger's active engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "input", deny_unknown_fields)]
pub enum NativeAuthoredOperation {
    #[serde(rename = "claim.submit")]
    ClaimSubmit(NativeClaimDocument),
    #[serde(rename = "claim.post")]
    ClaimPost(NativeClaimTargetDocument),
    #[serde(rename = "claim.cancel")]
    ClaimCancel(NativeClaimTargetDocument),
    #[serde(rename = "receipt.acquire")]
    ReceiptAcquire(NativeReceiptDocument),
    #[serde(rename = "artifact.submit")]
    ArtifactSubmit(NativeWorkArtifactDocument),
    #[serde(rename = "artifact.diagnostic")]
    ArtifactDiagnostic(NativeDiagnosticDocument),
    #[serde(rename = "testament.submit")]
    TestamentSubmit(NativeResponseDocument),
    #[serde(rename = "testament.post")]
    TestamentPost(NativeResponseTargetDocument),
    #[serde(rename = "testament.receive")]
    TestamentReceive(NativeResponseTargetDocument),
    #[serde(rename = "validation.begin")]
    ValidationBegin(NativeEvaluationDocument),
    #[serde(rename = "validation.report")]
    ValidationReport(NativeReportDocument),
    #[serde(rename = "claim.release_scope")]
    ClaimReleaseScope(NativeClaimTargetDocument),
    #[serde(rename = "receipt.adopt")]
    ReceiptAdopt(NativeAdoptReceiptDocument),
    #[serde(rename = "artifact.fail")]
    ArtifactFail(NativeFailWorkDocument),
    #[serde(rename = "artifact.receive")]
    ArtifactReceive(NativeArtifactTargetDocument),
    #[serde(rename = "artifact.reject")]
    ArtifactReject(NativeRejectWorkDocument),
    #[serde(rename = "validation.seal_increments")]
    ValidationSealIncrements(NativeClaimTargetDocument),
    #[serde(rename = "validation.enter_whole_work")]
    ValidationEnterWholeWork(NativeResponseTargetDocument),
    #[serde(rename = "audit.generate")]
    AuditGenerate(NativeAuditDocument),
    #[serde(rename = "audit.post")]
    AuditPost(NativeAuditTargetDocument),
    #[serde(rename = "monitor.register")]
    MonitorRegister(NativeMonitorDocument),
    #[serde(rename = "monitor.rebind")]
    MonitorRebind(NativeMonitorRebindDocument),
    #[serde(rename = "monitor.cancel")]
    MonitorCancel(NativeMonitorTargetDocument),
    #[serde(rename = "claim.challenge")]
    ClaimChallenge(Box<NativeChallengeDocument>),
    #[serde(rename = "claim.consult")]
    ClaimConsult(Box<NativeConsultDocument>),
    #[serde(rename = "claim.correct")]
    ClaimCorrect(Box<NativeCorrectionDocument>),
    #[serde(rename = "claim.follow_up")]
    ClaimFollowUp(Box<NativeFollowUpDocument>),
}
impl NativeAuthoredOperation {
    pub fn descriptor(&self) -> &'static OperationDescriptor {
        match self {
            Self::ClaimChallenge(_) => &native_catalog::NATIVE_CLAIM_CHALLENGE,
            Self::ClaimConsult(_) => &native_catalog::NATIVE_CLAIM_CONSULT,
            Self::ClaimCorrect(_) => &native_catalog::NATIVE_CLAIM_CORRECT,
            Self::ClaimFollowUp(_) => &native_catalog::NATIVE_CLAIM_FOLLOW_UP,
            Self::ClaimReleaseScope(_) => &native_catalog::NATIVE_CLAIM_RELEASE_SCOPE,
            Self::ReceiptAdopt(_) => &native_catalog::NATIVE_RECEIPT_ADOPT,
            Self::ArtifactFail(_) => &native_catalog::NATIVE_ARTIFACT_FAIL,
            Self::ArtifactReceive(_) => &native_catalog::NATIVE_ARTIFACT_RECEIVE,
            Self::ArtifactReject(_) => &native_catalog::NATIVE_ARTIFACT_REJECT,
            Self::ValidationSealIncrements(_) => &native_catalog::NATIVE_VALIDATION_SEAL_INCREMENTS,
            Self::ValidationEnterWholeWork(_) => {
                &native_catalog::NATIVE_VALIDATION_ENTER_WHOLE_WORK
            }
            Self::AuditGenerate(_) => &native_catalog::NATIVE_AUDIT_GENERATE,
            Self::AuditPost(_) => &native_catalog::NATIVE_AUDIT_POST,
            Self::MonitorRegister(_) => &native_catalog::NATIVE_MONITOR_REGISTER,
            Self::MonitorRebind(_) => &native_catalog::NATIVE_MONITOR_REBIND,
            Self::MonitorCancel(_) => &native_catalog::NATIVE_MONITOR_CANCEL,
            Self::ClaimSubmit(_) => &native_catalog::NATIVE_CLAIM_SUBMIT,
            Self::ClaimPost(_) => &native_catalog::NATIVE_CLAIM_POST,
            Self::ClaimCancel(_) => &native_catalog::NATIVE_CLAIM_CANCEL,
            Self::ReceiptAcquire(_) => &native_catalog::NATIVE_RECEIPT_ACQUIRE,
            Self::ArtifactSubmit(_) => &native_catalog::NATIVE_ARTIFACT_SUBMIT,
            Self::ArtifactDiagnostic(_) => &native_catalog::NATIVE_ARTIFACT_DIAGNOSTIC,
            Self::TestamentSubmit(_) => &native_catalog::NATIVE_TESTAMENT_SUBMIT,
            Self::TestamentPost(_) => &native_catalog::NATIVE_TESTAMENT_POST,
            Self::TestamentReceive(_) => &native_catalog::NATIVE_TESTAMENT_RECEIVE,
            Self::ValidationBegin(_) => &native_catalog::NATIVE_VALIDATION_BEGIN,
            Self::ValidationReport(_) => &native_catalog::NATIVE_VALIDATION_REPORT,
        }
    }
    pub fn name(&self) -> &'static str {
        self.descriptor().name
    }
    /// Stable field order and expanded serde defaults; no identity generation.
    /// The journal binds these bytes to the operation identity before any
    /// identity is minted, so a retry with different content is refused.
    pub fn canonical_intent(&self) -> Result<Vec<u8>, InputError> {
        let mut output = LimitedJson(Vec::new());
        serde_json::to_writer(&mut output, self).map_err(|_| InputError::Capacity)?;
        Ok(output.0)
    }
}
/// Exact reads of the native prefix, version 2 of the shared read names. Each
/// read is served from one fixed prefix; `validation.get` chains two reads so
/// the evaluations are never older than the definition they belong to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "input", deny_unknown_fields)]
pub enum NativeReadOperation {
    #[serde(rename = "claim.get")]
    ClaimGet(NativeObjectDocument),
    #[serde(rename = "testament.get")]
    TestamentGet(NativeObjectDocument),
    #[serde(rename = "artifact.get")]
    ArtifactGet(NativeObjectDocument),
    #[serde(rename = "validation.get")]
    ValidationGet(NativeObjectDocument),
    #[serde(rename = "validation.context")]
    ValidationContext(NativeContextDocument),
    #[serde(rename = "ledger.standing")]
    Standing(NativeEmptyDocument),
    #[serde(rename = "claim.lineage")]
    ClaimLineage(NativeObjectDocument),
    #[serde(rename = "claim.wait")]
    ClaimWait(NativeWaitDocument),
}
/// The evaluator's view of one declaration at one prefix: the evaluation
/// selected like `validation.begin` does (by `phase`, `slot` or `target`,
/// and optionally an exact `generation`), with its target's manifest, the
/// accepted results after `results_after` and the delivery result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeContextDocument {
    pub validation: String,
    #[serde(default = "whole_work")]
    pub phase: String,
    #[serde(default)]
    pub slot: Option<u32>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub generation: Option<u64>,
    #[serde(default)]
    pub results_after: Option<u64>,
    #[serde(default = "sixteen")]
    pub limit: u32,
}
fn sixteen() -> u32 {
    16
}
impl NativeReadOperation {
    pub fn descriptor(&self) -> &'static OperationDescriptor {
        match self {
            Self::ClaimGet(_) => &native_catalog::NATIVE_CLAIM_GET,
            Self::TestamentGet(_) => &native_catalog::NATIVE_TESTAMENT_GET,
            Self::ArtifactGet(_) => &native_catalog::NATIVE_ARTIFACT_GET,
            Self::ValidationGet(_) => &native_catalog::NATIVE_VALIDATION_GET,
            Self::ValidationContext(_) => &native_catalog::NATIVE_VALIDATION_CONTEXT,
            Self::Standing(_) => &native_catalog::NATIVE_LEDGER_STANDING,
            Self::ClaimLineage(_) => &native_catalog::NATIVE_CLAIM_LINEAGE,
            Self::ClaimWait(_) => &native_catalog::NATIVE_CLAIM_WAIT,
        }
    }
    pub fn name(&self) -> &'static str {
        self.descriptor().name
    }
}
/// One object identity in hexadecimal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeObjectDocument {
    pub id: String,
}

/// The issuer replaces the claim's current holder: the committed receipt is
/// the previous fence, `holder` the new participant, `id` the new receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeAdoptReceiptDocument {
    pub claim: String,
    pub holder: String,
    #[serde(default)]
    pub id: Option<String>,
}
/// The holder records that one slot cannot be produced, citing its own
/// committed production diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFailWorkDocument {
    pub claim: String,
    pub slot: u32,
    /// The committed production diagnostic's artifact id.
    pub diagnostic: String,
    /// Optional pin of its content hash; the committed hash is bound either way.
    #[serde(default)]
    pub hash: Option<String>,
}
/// One work artifact of the claim, addressed by the issuer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeArtifactTargetDocument {
    pub claim: String,
    pub artifact: String,
}
/// The issuer rejects a generated or received work artifact for a
/// `structure` or `metadata` failure with its own diagnostic artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRejectWorkDocument {
    pub claim: String,
    pub artifact: String,
    pub reason: String,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub schema_hash: Option<String>,
    #[serde(default)]
    pub metadata: Vec<u8>,
    pub payload: NativePayloadDocument,
    #[serde(default)]
    pub inputs: Vec<NativeObjectReferenceDocument>,
    #[serde(default)]
    pub visibility: Vec<String>,
}
/// The issuer generates the claim's result testament (its audit).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeAuditDocument {
    pub claim: String,
    #[serde(default)]
    pub id: Option<String>,
}
/// One result testament.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeAuditTargetDocument {
    pub testament: String,
}
/// A wait monitor over committed claims registered on the owner's claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMonitorDocument {
    pub claim: String,
    #[serde(default)]
    pub id: Option<String>,
    pub roots: Vec<NativeWaitRootDocument>,
    pub deadline: NativeDeadlineDocument,
}
/// `predicate` is `satisfied`, `terminal` or `released`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWaitRootDocument {
    pub predicate: String,
    pub claim: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMonitorRebindDocument {
    pub claim: String,
    pub monitor: String,
    pub predecessor: String,
    pub successor: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMonitorTargetDocument {
    pub claim: String,
    pub monitor: String,
}

/// Bounded lists over the native prefix (doc 22 §7), version 2 of the shared
/// list names plus the families the native engine adds. The node selects the
/// most selective indexed predicate; the rest filter residually within
/// `max_visits`, so a page may be empty and still carry a cursor. Only an
/// absent cursor ends a list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "input", deny_unknown_fields)]
pub enum NativeListOperation {
    #[serde(rename = "claim.list")]
    ClaimList(NativeClaimListDocument),
    #[serde(rename = "artifact.list")]
    ArtifactList(NativeArtifactListDocument),
    #[serde(rename = "validation.list")]
    ValidationList(NativeValidationListDocument),
    #[serde(rename = "evaluation.list")]
    EvaluationList(NativeEvaluationListDocument),
    #[serde(rename = "testament.list")]
    TestamentList(NativeTestamentListDocument),
    #[serde(rename = "receipt.list")]
    ReceiptList(NativeReceiptListDocument),
    #[serde(rename = "monitor.list")]
    MonitorList(NativeMonitorListDocument),
    #[serde(rename = "event.list")]
    EventList(NativeEventListDocument),
}
impl NativeListOperation {
    pub fn descriptor(&self) -> &'static OperationDescriptor {
        match self {
            Self::ClaimList(_) => &native_catalog::NATIVE_CLAIM_LIST,
            Self::ArtifactList(_) => &native_catalog::NATIVE_ARTIFACT_LIST,
            Self::ValidationList(_) => &native_catalog::NATIVE_VALIDATION_LIST,
            Self::EvaluationList(_) => &native_catalog::NATIVE_EVALUATION_LIST,
            Self::TestamentList(_) => &native_catalog::NATIVE_TESTAMENT_LIST,
            Self::ReceiptList(_) => &native_catalog::NATIVE_RECEIPT_LIST,
            Self::MonitorList(_) => &native_catalog::NATIVE_MONITOR_LIST,
            Self::EventList(_) => &native_catalog::NATIVE_EVENT_LIST,
        }
    }
    pub fn name(&self) -> &'static str {
        self.descriptor().name
    }
    /// The page bounds every list document carries.
    pub fn page(&self) -> &NativeListPageDocument {
        match self {
            Self::ClaimList(document) => &document.page,
            Self::ArtifactList(document) => &document.page,
            Self::ValidationList(document) => &document.page,
            Self::EvaluationList(document) => &document.page,
            Self::TestamentList(document) => &document.page,
            Self::ReceiptList(document) => &document.page,
            Self::MonitorList(document) => &document.page,
            Self::EventList(document) => &document.page,
        }
    }
    pub fn page_mut(&mut self) -> &mut NativeListPageDocument {
        match self {
            Self::ClaimList(document) => &mut document.page,
            Self::ArtifactList(document) => &mut document.page,
            Self::ValidationList(document) => &mut document.page,
            Self::EvaluationList(document) => &mut document.page,
            Self::TestamentList(document) => &mut document.page,
            Self::ReceiptList(document) => &mut document.page,
            Self::MonitorList(document) => &mut document.page,
            Self::EventList(document) => &mut document.page,
        }
    }
}
fn hundred() -> u32 {
    100
}
fn thousand_twenty_four() -> u32 {
    1024
}
/// Page bounds shared by every list: the opaque continuation of the previous
/// page in hexadecimal, the most objects to return and the most rows the node
/// may visit (matching or not) while filling the page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeListPageDocument {
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default = "hundred")]
    pub limit: u32,
    #[serde(default = "thousand_twenty_four")]
    pub max_visits: u32,
}
impl Default for NativeListPageDocument {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: hundred(),
            max_visits: thousand_twenty_four(),
        }
    }
}
/// Claims by issuer, subject, status, action, one authored scope, one
/// authored relation to a claim, or creation after a prefix. Participants
/// are hexadecimal or `self`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeClaimListDocument {
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub scope: Option<NativeScopeDocument>,
    #[serde(default)]
    pub relation: Option<NativeRelationDocument>,
    #[serde(default)]
    pub created_after: Option<u64>,
    #[serde(flatten)]
    pub page: NativeListPageDocument,
}
/// Artifacts by producer, kind, payload schema hash or one cited input.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeArtifactListDocument {
    #[serde(default)]
    pub producer: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub input: Option<String>,
    #[serde(flatten)]
    pub page: NativeListPageDocument,
}
/// Validation definitions by claim or designated evaluator.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeValidationListDocument {
    #[serde(default)]
    pub claim: Option<String>,
    #[serde(default)]
    pub evaluator: Option<String>,
    #[serde(flatten)]
    pub page: NativeListPageDocument,
}
/// Evaluations by claim, definition, designated evaluator or accepted verdict.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEvaluationListDocument {
    #[serde(default)]
    pub claim: Option<String>,
    #[serde(default)]
    pub validation: Option<String>,
    #[serde(default)]
    pub evaluator: Option<String>,
    #[serde(default)]
    pub verdict: Option<String>,
    #[serde(flatten)]
    pub page: NativeListPageDocument,
}
/// The testaments (response cycles) of one claim, latest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTestamentListDocument {
    pub claim: String,
    #[serde(flatten)]
    pub page: NativeListPageDocument,
}
/// Receipts by holder or claim.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReceiptListDocument {
    #[serde(default)]
    pub holder: Option<String>,
    #[serde(default)]
    pub claim: Option<String>,
    #[serde(flatten)]
    pub page: NativeListPageDocument,
}
/// The monitors registered on one claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMonitorListDocument {
    pub claim: String,
    #[serde(flatten)]
    pub page: NativeListPageDocument,
}
/// The publication history after one event position.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEventListDocument {
    #[serde(default)]
    pub after: Option<NativeEventPositionDocument>,
    #[serde(flatten)]
    pub page: NativeListPageDocument,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEventPositionDocument {
    pub sequence: u64,
    pub ordinal: u32,
}

/// Parse one native list by descriptor name.
pub fn parse_native_list_json(name: &str, bytes: &[u8]) -> Result<NativeListOperation, InputError> {
    macro_rules! document {
        ($variant:ident) => {
            parse_document(bytes, InputFormat::Json).map(NativeListOperation::$variant)
        };
    }
    match name {
        "claim.list" => document!(ClaimList),
        "artifact.list" => document!(ArtifactList),
        "validation.list" => document!(ValidationList),
        "evaluation.list" => document!(EvaluationList),
        "testament.list" => document!(TestamentList),
        "receipt.list" => document!(ReceiptList),
        "monitor.list" => document!(MonitorList),
        "event.list" => document!(EventList),
        _ => Err(InputError::Invalid("unknown native list operation")),
    }
}
/// No fields; unknown keys are refused so a misdirected document fails loudly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEmptyDocument {}

/// Parse one native read by descriptor name.
pub fn parse_native_read_json(name: &str, bytes: &[u8]) -> Result<NativeReadOperation, InputError> {
    macro_rules! document {
        ($variant:ident) => {
            parse_document(bytes, InputFormat::Json).map(NativeReadOperation::$variant)
        };
    }
    match name {
        "claim.get" => document!(ClaimGet),
        "testament.get" => document!(TestamentGet),
        "artifact.get" => document!(ArtifactGet),
        "validation.get" => document!(ValidationGet),
        "validation.context" => document!(ValidationContext),
        "ledger.standing" => document!(Standing),
        "claim.lineage" => document!(ClaimLineage),
        "claim.wait" => document!(ClaimWait),
        _ => Err(InputError::Invalid("unknown native read operation")),
    }
}

/// Parse one native document by descriptor name. Unknown names refuse; a name
/// that only the V1 catalog exposes is not silently redirected.
pub fn parse_native_json(name: &str, bytes: &[u8]) -> Result<NativeAuthoredOperation, InputError> {
    macro_rules! document {
        ($variant:ident) => {
            parse_document(bytes, InputFormat::Json).map(NativeAuthoredOperation::$variant)
        };
    }
    match name {
        "claim.submit" => document!(ClaimSubmit),
        "claim.post" => document!(ClaimPost),
        "claim.cancel" => document!(ClaimCancel),
        "receipt.acquire" => document!(ReceiptAcquire),
        "artifact.submit" => document!(ArtifactSubmit),
        "artifact.diagnostic" => document!(ArtifactDiagnostic),
        "testament.submit" => document!(TestamentSubmit),
        "testament.post" => document!(TestamentPost),
        "testament.receive" => document!(TestamentReceive),
        "validation.begin" => document!(ValidationBegin),
        "validation.report" => document!(ValidationReport),
        "claim.release_scope" => document!(ClaimReleaseScope),
        "receipt.adopt" => document!(ReceiptAdopt),
        "artifact.fail" => document!(ArtifactFail),
        "artifact.receive" => document!(ArtifactReceive),
        "artifact.reject" => document!(ArtifactReject),
        "validation.seal_increments" => document!(ValidationSealIncrements),
        "validation.enter_whole_work" => document!(ValidationEnterWholeWork),
        "audit.generate" => document!(AuditGenerate),
        "audit.post" => document!(AuditPost),
        "monitor.register" => document!(MonitorRegister),
        "monitor.rebind" => document!(MonitorRebind),
        "monitor.cancel" => document!(MonitorCancel),
        "claim.challenge" => document!(ClaimChallenge),
        "claim.consult" => document!(ClaimConsult),
        "claim.correct" => document!(ClaimCorrect),
        "claim.follow_up" => document!(ClaimFollowUp),
        _ => Err(InputError::Invalid(
            "unknown or unexposed native application operation",
        )),
    }
}

fn work() -> String {
    "work".into()
}
fn required() -> String {
    "required".into()
}
fn whole_work() -> String {
    "whole_work".into()
}
fn one() -> u64 {
    1
}
fn one_u32() -> u32 {
    1
}
fn four() -> u32 {
    4
}

/// A complete authored claim. Issuer, subject, action and cause relations are
/// derived from the authenticated actor, `target`, `action` and `parent`;
/// authoring them explicitly is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeClaimDocument {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub occurrence: Option<String>,
    pub description: String,
    pub target: String,
    #[serde(default = "work")]
    pub action: String,
    #[serde(default)]
    pub scopes: Vec<NativeScopeDocument>,
    #[serde(default)]
    pub relations: Vec<NativeRelationDocument>,
    #[serde(default)]
    pub deadline: Option<NativeDeadlineDocument>,
    pub validations: Vec<NativeValidationDocument>,
    #[serde(default)]
    pub slots: Vec<NativeSlotDocument>,
    #[serde(default = "four")]
    pub max_responses: u32,
    #[serde(default)]
    pub scope_limits: NativeScopeLimitsDocument,
    /// A child claim caused by this committed parent; the parent's current
    /// binding and receipt are read from the ledger and pinned as its owner.
    #[serde(default)]
    pub parent: Option<String>,
    /// The immutable follow-up policy of a challenge or consultation
    /// (descriptor schema 2). Omitted on ordinary claims.
    #[serde(default)]
    pub policy: Option<NativePeerPolicyDocument>,
}
/// How a challenge or consultation may be followed up; `escalation` is
/// `none` (the issuer only), `holder` (or the current receipt holder) or
/// `evaluator` (or a designated evaluator of the claim).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePeerPolicyDocument {
    #[serde(default)]
    pub corrective_allowed: bool,
    #[serde(default)]
    pub max_follow_ups: u16,
    #[serde(default)]
    pub single_issuer: bool,
    #[serde(default = "escalation_none")]
    pub escalation: String,
}
fn escalation_none() -> String {
    "none".into()
}

/// A challenge: the subject must prove or redo the stated work under the
/// acceptance requirements; `artifact` names the exact committed artifact
/// disputed (`ID` or `ID@HASH`; an omitted hash is read from the ledger), and
/// `policy` is the immutable follow-up policy (corrections need
/// `corrective_allowed`). Compiles to a `claim.submit` with action
/// `challenge`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeChallengeDocument {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub occurrence: Option<String>,
    pub description: String,
    pub target: String,
    #[serde(default)]
    pub artifact: Option<String>,
    #[serde(default)]
    pub scopes: Vec<NativeScopeDocument>,
    #[serde(default)]
    pub relations: Vec<NativeRelationDocument>,
    #[serde(default)]
    pub deadline: Option<NativeDeadlineDocument>,
    pub validations: Vec<NativeValidationDocument>,
    #[serde(default)]
    pub slots: Vec<NativeSlotDocument>,
    #[serde(default = "four")]
    pub max_responses: u32,
    #[serde(default)]
    pub scope_limits: NativeScopeLimitsDocument,
    #[serde(default)]
    pub parent: Option<String>,
    pub policy: NativePeerPolicyDocument,
}
/// A consultation: the subject answers `description` (the query) under the
/// declared quality bar; `policy` bounds and authorizes the follow-up
/// consultations that may later refine it. Compiles to a `claim.submit` with
/// action `consultation`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeConsultDocument {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub occurrence: Option<String>,
    pub description: String,
    pub target: String,
    #[serde(default)]
    pub scopes: Vec<NativeScopeDocument>,
    #[serde(default)]
    pub relations: Vec<NativeRelationDocument>,
    #[serde(default)]
    pub deadline: Option<NativeDeadlineDocument>,
    pub validations: Vec<NativeValidationDocument>,
    #[serde(default)]
    pub slots: Vec<NativeSlotDocument>,
    #[serde(default = "four")]
    pub max_responses: u32,
    #[serde(default)]
    pub scope_limits: NativeScopeLimitsDocument,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub policy: Option<NativePeerPolicyDocument>,
}
/// A correction of `challenge`, resting on `verdict`: the report artifact of
/// that challenge's terminal Fail, Incomplete or Error verdict (`ID` or
/// `ID@HASH`; an omitted hash is read from the ledger). Compiles to a
/// `claim.submit` with action `correction`, an `invalidates` relation to the
/// challenge and a `reviews` relation to the verdict at its hash; the
/// occurrence identity derives from the challenge, the verdict and the
/// author unless one is given.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCorrectionDocument {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub occurrence: Option<String>,
    pub challenge: String,
    pub verdict: String,
    pub description: String,
    /// The participant obliged to correct; the challenge's subject when
    /// omitted.
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub scopes: Vec<NativeScopeDocument>,
    #[serde(default)]
    pub relations: Vec<NativeRelationDocument>,
    #[serde(default)]
    pub deadline: Option<NativeDeadlineDocument>,
    pub validations: Vec<NativeValidationDocument>,
    #[serde(default)]
    pub slots: Vec<NativeSlotDocument>,
    #[serde(default = "four")]
    pub max_responses: u32,
    #[serde(default)]
    pub scope_limits: NativeScopeLimitsDocument,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub policy: Option<NativePeerPolicyDocument>,
}
/// A follow-up consultation refining the committed consultation `refines`,
/// addressed to that consultation's subject unless `target` is given.
/// Compiles to a `claim.submit` with action `consultation` and a `refines`
/// relation; the occurrence identity derives from the refined claim, the
/// query and the author unless one is given.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFollowUpDocument {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub occurrence: Option<String>,
    pub refines: String,
    pub description: String,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub scopes: Vec<NativeScopeDocument>,
    #[serde(default)]
    pub relations: Vec<NativeRelationDocument>,
    #[serde(default)]
    pub deadline: Option<NativeDeadlineDocument>,
    pub validations: Vec<NativeValidationDocument>,
    #[serde(default)]
    pub slots: Vec<NativeSlotDocument>,
    #[serde(default = "four")]
    pub max_responses: u32,
    #[serde(default)]
    pub scope_limits: NativeScopeLimitsDocument,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub policy: Option<NativePeerPolicyDocument>,
}
/// Observe one claim until `until` holds: `testament`, `satisfied`,
/// `terminal` or `released`, within `timeout_ms` (1..=30000).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWaitDocument {
    pub claim: String,
    pub until: super::NativeWaitUntil,
    #[serde(default = "wait_timeout")]
    pub timeout_ms: u32,
}
fn wait_timeout() -> u32 {
    30_000
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeScopeDocument {
    pub kind: String,
    pub key: String,
}
/// An authored relation to a committed claim of the same ledger
/// (`claim:ID`), or for `reviews` and `derived_from` to exact evidence
/// (`artifact:ID@HASH`): `kind` is
/// one of depends_on, awaits, supersedes, amends, refines, conflicts_with,
/// derived_from or reviews and `target` is `claim:ID`. Issuer, subject,
/// claim_action and caused_by are derived from the context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRelationDocument {
    pub kind: String,
    pub target: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDeadlineDocument {
    /// Logical milliseconds since the Unix epoch.
    pub at: u64,
    #[serde(default)]
    pub timer: Option<String>,
    #[serde(default = "one")]
    pub generation: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeScopeLimitsDocument {
    pub scopes: u32,
    pub roots: u32,
    pub children: u32,
}
impl Default for NativeScopeLimitsDocument {
    fn default() -> Self {
        Self {
            scopes: 4,
            roots: 16,
            children: 8,
        }
    }
}
/// One declaration of the claim's acceptance policy. Its position in the
/// `validations` array is its declaration index. A `receipt` declaration is
/// the mandatory whole-work delivery check and takes no handlers; every other
/// kind names its handlers and target explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeValidationDocument {
    #[serde(default)]
    pub id: Option<String>,
    pub kind: String,
    #[serde(default = "whole_work")]
    pub phase: String,
    #[serde(default = "required")]
    pub mode: String,
    pub description: String,
    #[serde(default)]
    pub target: Option<NativeTargetDocument>,
    #[serde(default)]
    pub evaluator: Option<String>,
    #[serde(default)]
    pub handlers: Vec<NativeHandlerDocument>,
    #[serde(default)]
    pub required_policy: Option<String>,
    #[serde(default)]
    pub quality: Option<NativePhaseDocument>,
    #[serde(default)]
    pub quality_bar: Option<String>,
    #[serde(default)]
    pub contributed_by: Vec<String>,
    #[serde(default)]
    pub policy_revision: Option<u64>,
    pub deadline: NativeDeadlineDocument,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeTargetDocument {
    Delivery,
    Admission,
    Increment,
    Slot { index: u32, name: String },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeHandlerDocument {
    pub id: String,
    pub version: String,
    #[serde(default)]
    pub agentic: bool,
    #[serde(default = "one_u32")]
    pub attempts: u32,
    #[serde(default)]
    pub proof_schema: Option<String>,
    #[serde(default)]
    pub diagnostic_schema: Option<String>,
}
/// The agentic quality phase that follows a programmatic check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePhaseDocument {
    pub evaluator: String,
    pub handlers: Vec<NativeHandlerDocument>,
    #[serde(default)]
    pub required_policy: Option<String>,
}
/// One work slot of the acceptance manifest and the checks bound to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSlotDocument {
    pub slot: u32,
    #[serde(default = "required")]
    pub mode: String,
    /// Virtual declaration index of the missing-slot obligation. It must not
    /// name an authored declaration and is unique per slot; it defaults to
    /// the number of declarations plus the slot number.
    #[serde(default)]
    pub missing: Option<u32>,
    #[serde(default)]
    pub checks: Vec<NativeCheckDocument>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCheckDocument {
    pub declaration: u32,
    #[serde(default = "required")]
    pub mode: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeClaimTargetDocument {
    pub claim: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReceiptDocument {
    pub claim: String,
    #[serde(default)]
    pub id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativePayloadDocument {
    Inline { bytes: Vec<u8> },
    Text { text: String },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeObjectReferenceDocument {
    pub kind: String,
    pub id: String,
}
/// Work output for one slot of the current cycle under the actor's receipt.
/// `kind` and `schema_hash` default to the builtin test report schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWorkArtifactDocument {
    pub claim: String,
    pub slot: u32,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub schema_hash: Option<String>,
    #[serde(default)]
    pub metadata: Vec<u8>,
    pub payload: NativePayloadDocument,
    #[serde(default)]
    pub inputs: Vec<NativeObjectReferenceDocument>,
    #[serde(default)]
    pub visibility: Vec<String>,
}
/// A diagnostic for failed or impossible work: `reason` is one of `work`,
/// `production`, `structure` or `metadata`. Defaults to the builtin error
/// report schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDiagnosticDocument {
    pub claim: String,
    pub reason: String,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub schema_hash: Option<String>,
    #[serde(default)]
    pub metadata: Vec<u8>,
    pub payload: NativePayloadDocument,
    #[serde(default)]
    pub inputs: Vec<NativeObjectReferenceDocument>,
    #[serde(default)]
    pub visibility: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeArtifactReferenceDocument {
    pub id: String,
    pub hash: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSlotBindingDocument {
    pub slot: u32,
    pub artifact: NativeArtifactReferenceDocument,
}
/// The respondent's explicit testimony closing the current work cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResponseDocument {
    pub claim: String,
    #[serde(default)]
    pub id: Option<String>,
    pub summary: String,
    pub confidence: String,
    pub outcome: String,
    #[serde(default)]
    pub manifest: Vec<NativeSlotBindingDocument>,
    #[serde(default)]
    pub diagnostics: Vec<NativeArtifactReferenceDocument>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResponseTargetDocument {
    pub claim: String,
    pub testament: String,
}
/// Begin the current whole-work evaluation of `validation` under `claim`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEvaluationDocument {
    pub claim: String,
    pub validation: String,
    #[serde(default)]
    pub slot: Option<u32>,
    /// `whole_work` (the default), `admission` or `increment`: which current
    /// evaluation of the declaration this names.
    #[serde(default = "whole_work")]
    pub phase: String,
    /// The increment's work artifact when several increments are current.
    #[serde(default)]
    pub target: Option<String>,
}
/// The evaluator's fenced report for the begun evaluation, with its result
/// artifact. `verdict` is `pass`, `fail`, `incomplete` or `error`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReportDocument {
    pub claim: String,
    pub validation: String,
    #[serde(default)]
    pub slot: Option<u32>,
    #[serde(default = "whole_work")]
    pub phase: String,
    #[serde(default)]
    pub target: Option<String>,
    pub verdict: String,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub schema_hash: Option<String>,
    #[serde(default)]
    pub metadata: Vec<u8>,
    pub payload: NativePayloadDocument,
    #[serde(default)]
    pub inputs: Vec<NativeObjectReferenceDocument>,
    #[serde(default)]
    pub visibility: Vec<String>,
}
