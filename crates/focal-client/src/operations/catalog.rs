use super::*;
use focal_wire::{AccessError, ListPage, MutationReply, ReadPage, ReconcileReply};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    Actor,
    Evaluator,
    Runtime,
    Node,
    FounderNode,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultKind {
    Mutation,
    Read,
    List,
    Reconcile,
}
/// Which negotiated wire profile carries the operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireProfile {
    /// Frozen V1 command envelopes (protocols 1–3).
    V1,
    /// `FCNINPUT1` frames on the native profile (protocol 4).
    Native,
}
/// Which durable client identity makes a retry exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryIdentity {
    /// Managed request streams: `m1:` operation references.
    ManagedM1,
    /// The native operation journal: `n1:` references carrying the exact frame.
    NativeN1,
    /// The root administration journal: `a1:` references.
    AdminA1,
    /// The application-replica administration journal: `r1:` references.
    ReplicaR1,
    /// Repeating the exact arguments resumes the same durable work (a named
    /// watch, a caller-identified upload).
    Exact,
    /// Every call is a fresh intent; only a read is safe to repeat.
    Fresh,
}
/// Which adapter surface serves the operation. One registry names every
/// tool the CLI and the MCP adapter expose; the host filters by the
/// standing it can prove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    /// Participant application operations on the active engine.
    Application,
    /// Durable named watches.
    Watch,
    /// Chunked payload transfer and retrieval.
    Transfer,
    /// Local physical-node administration.
    Administration,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InputKind {
    Native(super::native_catalog::NativeInputKind),
    MonitorRegister,
    MonitorGet,
    Summary,
    ClaimGet,
    ClaimWait,
    Validator,
    Traversal,
    Claim,
    ClaimBatch,
    Testament,
    Artifact,
    ArtifactRegister,
    TestamentReceive,
    IncrementValidation,
    ValidationVerdict,
    Supersede,
    ClaimId,
    Progress,
    Cancel,
    Receipt,
    Evidence,
    Get,
    List,
    RequestEpoch,
    RequestStatus,
    /// A reviewed JSON Schema literal for a surface whose input is not one
    /// of the authored documents.
    Literal(&'static str),
}
#[derive(Debug, Clone, Copy)]
pub struct OperationDescriptor {
    pub name: &'static str,
    pub version: u16,
    pub description: &'static str,
    pub capability: Capability,
    pub mutation: bool,
    pub destructive: bool,
    pub result_kind: ResultKind,
    pub max_input_bytes: usize,
    pub wire: WireProfile,
    pub retry: RetryIdentity,
    pub surface: Surface,
    /// The human CLI command that performs the same operation, as the
    /// space-separated subcommand path under `focal`; absent when the CLI
    /// performs it only implicitly.
    pub cli_path: Option<&'static str>,
    pub(super) input: InputKind,
    pub(super) family: Option<ObjectKind>,
}
impl OperationDescriptor {
    pub const fn read_only(&self) -> bool {
        !self.mutation
    }
    /// Fresh authored mutations can generate new identities. Only a saved
    /// durable operation ID supplies retry idempotency, not the tool name.
    pub const fn idempotent(&self) -> bool {
        !self.mutation
    }
    /// Whether repeating the same call (with its durable reference or exact
    /// arguments) resumes the same work instead of creating new intent.
    pub const fn repeatable(&self) -> bool {
        !self.mutation || !matches!(self.retry, RetryIdentity::Fresh)
    }
    pub fn input_schema(&self) -> Result<serde_json::Value, InputError> {
        super::schema::input(self)
    }
    pub fn output_schema(&self) -> Result<serde_json::Value, InputError> {
        super::schema::output()
    }
}
macro_rules! descriptor {
    ($symbol:ident,$name:literal,$input:ident,$family:expr,$kind:ident,$mutation:expr,$destructive:expr,$description:literal) => {
        pub(super) const $symbol: OperationDescriptor = OperationDescriptor {
            name: $name,
            version: 1,
            description: $description,
            capability: Capability::Actor,
            mutation: $mutation,
            destructive: $destructive,
            result_kind: ResultKind::$kind,
            max_input_bytes: MAX_INPUT_BYTES,
            wire: WireProfile::V1,
            retry: RetryIdentity::ManagedM1,
            surface: Surface::Application,
            cli_path: None,
            input: InputKind::$input,
            family: $family,
        };
    };
}
descriptor!(
    CLAIM_WAIT,
    "claim.wait",
    ClaimWait,
    Some(ObjectKind::Claim),
    Read,
    false,
    false,
    "Observe fresh claim state until satisfied, terminal or released for at most 30 seconds. Pending is an observer deadline; Unmet records terminal non-satisfaction. No monitor, timer, work or request journal is created."
);
descriptor!(
    MONITOR_GET,
    "monitor.get",
    MonitorGet,
    None,
    Read,
    false,
    false,
    "Read an exact committed monitor at a fresh quorum prefix. Missing, pending and released are distinct facts; release does not establish a particular timeout or success reason."
);
descriptor!(
    MONITOR_REGISTER,
    "monitor.register",
    MonitorRegister,
    None,
    Mutation,
    true,
    false,
    "Register bounded durable wait predicates under an active claim you issued, with an explicit timer, generation and deadline. This records a monitor; it does not launch work or turn a client timeout into a committed timer event."
);
descriptor!(
    LEDGER_SUMMARY,
    "ledger.summary",
    Summary,
    None,
    Read,
    false,
    false,
    "Read scalar committed counts for claims, testaments, artifacts, validations, evidence sets and validation runs after a fresh quorum barrier in the selected ledger. No graph download, historical lease or global totals."
);
descriptor!(
    LEDGER_TRAVERSE,
    "ledger.traverse",
    Traversal,
    None,
    Read,
    false,
    false,
    "Read bounded breadth-first graph pages at one immutable prefix. Preserve all query fields with the opaque cursor; cumulative truncation is explicit and never a complete graph."
);
descriptor!(
    VALIDATOR_LIST,
    "validator.list",
    Validator,
    None,
    List,
    false,
    false,
    "List recorded external handler contracts grouped by immutable validation requirement. All filters are optional; preserve filters and limits with cursors. Focal does not install or execute these handlers."
);
descriptor!(
    VALIDATOR_GET,
    "validator.get",
    Validator,
    None,
    List,
    false,
    false,
    "Inspect a pinned handler ID/version and the requirements that reference it. Returns a bounded binding page, not an installed implementation or an execution grant."
);
descriptor!(
    CLAIM_SUBMIT_BATCH,
    "claim.submit_batch",
    ClaimBatch,
    None,
    Mutation,
    true,
    false,
    "Generate 1..64 immutable claims atomically under one saved operation identity. Cross-claim dependencies are permitted; posting is a separate operation."
);
descriptor!(
    ARTIFACT_REGISTER,
    "artifact.register",
    ArtifactRegister,
    None,
    Mutation,
    true,
    false,
    "Register independently generated evidence under the authenticated producer, including external validation reports. Does not attach it to a work testament."
);
descriptor!(
    TESTAMENT_RECEIVE,
    "testament.receive",
    TestamentReceive,
    None,
    Mutation,
    true,
    false,
    "Claim issuer records receipt of the exact current closing testament. Delivery validation is distinct from quality acceptance."
);
descriptor!(
    VALIDATION_BEGIN,
    "validation.begin",
    ClaimId,
    None,
    Mutation,
    true,
    false,
    "Claim issuer begins whole-work validation of the acknowledged testament. This records runs; participants invoke their own tools."
);
descriptor!(
    VALIDATION_BEGIN_INCREMENT,
    "validation.begin_increment",
    IncrementValidation,
    None,
    Mutation,
    true,
    false,
    "Claim issuer begins a pinned incremental check against an attached artifact and the current evidence manifest. Participants invoke the validator externally."
);
descriptor!(
    VALIDATION_COMPLETE,
    "validation.complete",
    ClaimId,
    None,
    Mutation,
    true,
    false,
    "Claim issuer requests deterministic aggregation of committed whole-work results. It cannot supply a desired outcome."
);
descriptor!(
    VALIDATION_SUBMIT,
    "validation.submit",
    ValidationVerdict,
    None,
    Mutation,
    true,
    false,
    "Designated participant submits its actual external verdict with pinned run, attempt, handler, manifest, receipt and evidence. Focal does not execute the validator."
);
descriptor!(
    CLAIM_SUPERSEDE,
    "claim.supersede",
    Supersede,
    None,
    Mutation,
    true,
    true,
    "Generate a compatible immutable successor, preserving prior proof and explicitly linking the predecessor."
);
descriptor!(
    CLAIM_SUBMIT,
    "claim.submit",
    Claim,
    None,
    Mutation,
    true,
    false,
    "Generate a claim with immutable pinned validation requirements. Posting is a separate operation."
);
descriptor!(
    TESTAMENT_SUBMIT,
    "testament.submit",
    Testament,
    None,
    Mutation,
    true,
    false,
    "The respondent submits its own account after work completes or fails, with an exact artifact manifest. Non-complete outcomes require durable error artifacts. Receipt and claimant validation are separate operations."
);
descriptor!(
    ARTIFACT_SUBMIT,
    "artifact.submit",
    Artifact,
    None,
    Mutation,
    true,
    false,
    "Attach an artifact under the authenticated holder's receipt and evidence set. Server custody checks remain authoritative."
);
descriptor!(
    CLAIM_POST,
    "claim.post",
    ClaimId,
    None,
    Mutation,
    true,
    false,
    "Post an existing generated claim for dispatch."
);
descriptor!(
    CLAIM_PROGRESS,
    "claim.progress",
    Progress,
    None,
    Mutation,
    true,
    false,
    "Record progress under the current receipt fence."
);
descriptor!(
    CLAIM_CANCEL,
    "claim.cancel",
    Cancel,
    None,
    Mutation,
    true,
    true,
    "Request committed claim cancellation with a reason; canceling a client wait is different."
);
descriptor!(
    RECEIPT_ACQUIRE,
    "receipt.acquire",
    Receipt,
    None,
    Mutation,
    true,
    false,
    "Acquire a claim receipt for the authenticated actor at the supplied nonzero receipt epoch."
);
descriptor!(
    EVIDENCE_BEGIN,
    "evidence.begin",
    Evidence,
    None,
    Mutation,
    true,
    false,
    "Open an evidence set under an existing current receipt fence."
);
descriptor!(
    CLAIM_GET,
    "claim.get",
    ClaimGet,
    Some(ObjectKind::Claim),
    Read,
    false,
    false,
    "Read one exact claim ID, or prove one unique match for conjunctive claim filters at a fresh fixed prefix. Ambiguous, incomplete and absent results are distinct."
);
descriptor!(
    TESTAMENT_GET,
    "testament.get",
    Get,
    Some(ObjectKind::Testament),
    Read,
    false,
    false,
    "Read an exact testament and its immutable manifest at a fixed prefix."
);
descriptor!(
    ARTIFACT_GET,
    "artifact.get",
    Get,
    Some(ObjectKind::Artifact),
    Read,
    false,
    false,
    "Read an exact artifact descriptor and payload reference. This operation does not fetch large bytes."
);
descriptor!(
    VALIDATION_CONTEXT,
    "validation.context",
    Get,
    Some(ObjectKind::Validation),
    Read,
    false,
    false,
    "Read a validation, its claim, current closing testament and a page of recorded runs at one fixed prefix. This grants no execution lease and does not infer an artifact target."
);
descriptor!(
    VALIDATION_GET,
    "validation.get",
    Get,
    Some(ObjectKind::Validation),
    Read,
    false,
    false,
    "Read a validation requirement and a bounded fixed-prefix page of actual run and verdict records."
);
descriptor!(
    CLAIM_LIST,
    "claim.list",
    List,
    Some(ObjectKind::Claim),
    List,
    false,
    false,
    "List a bounded page of claims; optional source means issuer and target means subject."
);
descriptor!(
    TESTAMENT_LIST,
    "testament.list",
    List,
    Some(ObjectKind::Testament),
    List,
    false,
    false,
    "List a bounded page of testaments, optionally filtered by claim."
);
descriptor!(
    ARTIFACT_LIST,
    "artifact.list",
    List,
    Some(ObjectKind::Artifact),
    List,
    false,
    false,
    "List artifacts with optional conjunctive filters; testament selects its immutable attachment manifest."
);
descriptor!(
    VALIDATION_LIST,
    "validation.list",
    List,
    Some(ObjectKind::Validation),
    List,
    false,
    false,
    "List validation requirements with optional conjunctive filters. Actual executions use validation.get."
);
descriptor!(
    REQUEST_EPOCH,
    "request.epoch",
    RequestEpoch,
    None,
    Reconcile,
    false,
    false,
    "Read the authenticated principal's committed epoch admission, minimum retained epoch and latest admission. This never opens or advances an epoch."
);
descriptor!(
    REQUEST_STATUS,
    "request.status",
    RequestStatus,
    None,
    Reconcile,
    false,
    false,
    "Look up this principal's exact mutation request key at an authoritative prefix. Unknown or below-floor history is not proof that the request never committed or cannot still commit."
);
pub fn descriptors() -> &'static [OperationDescriptor] {
    // Name order is part of catalog pagination and digest stability.
    &[
        ARTIFACT_GET,
        ARTIFACT_LIST,
        ARTIFACT_REGISTER,
        ARTIFACT_SUBMIT,
        CLAIM_CANCEL,
        CLAIM_GET,
        CLAIM_LIST,
        CLAIM_POST,
        CLAIM_PROGRESS,
        CLAIM_SUBMIT,
        CLAIM_SUBMIT_BATCH,
        CLAIM_SUPERSEDE,
        CLAIM_WAIT,
        EVIDENCE_BEGIN,
        LEDGER_SUMMARY,
        LEDGER_TRAVERSE,
        MONITOR_GET,
        MONITOR_REGISTER,
        RECEIPT_ACQUIRE,
        REQUEST_EPOCH,
        REQUEST_STATUS,
        TESTAMENT_GET,
        TESTAMENT_LIST,
        TESTAMENT_RECEIVE,
        TESTAMENT_SUBMIT,
        VALIDATION_BEGIN,
        VALIDATION_BEGIN_INCREMENT,
        VALIDATION_COMPLETE,
        VALIDATION_CONTEXT,
        VALIDATION_GET,
        VALIDATION_LIST,
        VALIDATION_SUBMIT,
        VALIDATOR_GET,
        VALIDATOR_LIST,
    ]
}
pub fn find(name: &str) -> Option<&'static OperationDescriptor> {
    descriptors().iter().find(|value| value.name == name)
}
pub fn parse_json(name: &str, bytes: &[u8]) -> Result<AuthoredOperation, InputError> {
    macro_rules! document {
        ($variant:ident) => {
            parse_document(bytes, InputFormat::Json).map(AuthoredOperation::$variant)
        };
    }
    match name {
        "validator.list" => document!(ValidatorList),
        "validator.get" => document!(ValidatorGet),
        "ledger.summary" => document!(LedgerSummary),
        "monitor.register" => document!(MonitorRegister),
        "monitor.get" => document!(MonitorGet),
        "ledger.traverse" => document!(LedgerTraverse),
        "claim.submit" => document!(ClaimSubmit),
        "claim.submit_batch" => document!(ClaimSubmitBatch),
        "testament.submit" => document!(TestamentSubmit),
        "artifact.submit" => document!(ArtifactSubmit),
        "artifact.register" => document!(ArtifactRegister),
        "testament.receive" => document!(TestamentReceive),
        "validation.begin" => document!(ValidationBegin),
        "validation.begin_increment" => document!(ValidationBeginIncrement),
        "validation.complete" => document!(ValidationComplete),
        "validation.submit" => document!(ValidationSubmit),
        "claim.supersede" => document!(ClaimSupersede),
        "claim.post" => document!(ClaimPost),
        "claim.progress" => document!(ClaimProgress),
        "claim.cancel" => document!(ClaimCancel),
        "receipt.acquire" => document!(ReceiptAcquire),
        "evidence.begin" => document!(EvidenceBegin),
        "claim.get" => document!(ClaimGet),
        "claim.wait" => document!(ClaimWait),
        "testament.get" => document!(TestamentGet),
        "artifact.get" => document!(ArtifactGet),
        "validation.get" => document!(ValidationGet),
        "validation.context" => document!(ValidationContext),
        "claim.list" => document!(ClaimList),
        "testament.list" => document!(TestamentList),
        "artifact.list" => document!(ArtifactList),
        "validation.list" => document!(ValidationList),
        "request.epoch" => document!(RequestEpoch),
        "request.status" => document!(RequestStatus),
        _ => Err(InputError::Invalid(
            "unknown or unexposed application operation",
        )),
    }
}

/// Shared machine result. Schemas fully describe this envelope and variant
/// shape; nested frozen wire values remain validated by the transport's typed
/// response validator rather than a second hand-maintained model schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationResult {
    pub schema_version: u16,
    pub operation_id: Option<String>,
    pub condition: String,
    pub result: OperationOutput,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperationOutput {
    ClaimWait {
        result: crate::claim_wait::ClaimWaitResult,
    },
    Monitor {
        page: focal_wire::MonitorPage,
    },
    Summary {
        summary: focal_wire::LedgerSummary,
    },
    Watch {
        status: crate::watch::WatchStatus,
        delivery: Option<Box<crate::watch::WatchDelivery>>,
    },
    Watches {
        names: Vec<String>,
    },
    Traversal {
        page: focal_wire::TraversalPage,
    },
    Mutation {
        reply: MutationReply,
    },
    Read {
        page: ReadPage,
    },
    List {
        page: ListPage,
    },
    Reconcile {
        reply: ReconcileReply,
    },
    Error {
        code: String,
        detail: String,
    },
    Managed {
        receipt: focal_model::ManagedReceipt,
    },
    ManagedRequest {
        state: String,
    },
    ManagedRequests {
        operation_ids: Vec<String>,
    },
    ManagedReconcile {
        reply: focal_wire::RequestStreamReadReply,
    },
    Administration {
        result: crate::admin::AdminResult,
    },
    ValidationContext {
        context: Box<crate::validation_context::ValidationContext>,
    },
    Upload {
        progress: crate::artifact_transfer::UploadProgress,
    },
    ArtifactPayload {
        artifact: focal_model::ArtifactId,
        token: focal_wire::ReadToken,
        content_hash: focal_model::ContentHash,
        chunk: focal_wire::ContentChunk,
    },
    /// A committed native receipt with the identities the frame minted.
    Native {
        receipt: focal_wire::NativeReceipt,
        created: Vec<crate::native_store::NativeIdentity>,
    },
    NativeRead {
        page: Box<focal_wire::NativeReadPage>,
    },
    NativeList {
        page: Box<focal_wire::NativeListPage>,
    },
    /// A closed native refusal; the exact journaled frame stays retained.
    NativeRefused {
        refusal: focal_wire::NativeRefusal,
    },
    NativeWait {
        result: super::NativeWaitResult,
    },
}
impl ApplicationResult {
    pub fn is_error(&self) -> bool {
        matches!(
            &self.result,
            OperationOutput::Error { .. }
                | OperationOutput::NativeRefused { .. }
                | OperationOutput::Mutation {
                    reply: MutationReply::Pending(_)
                        | MutationReply::Domain(DomainOutcome::Refuse { .. })
                }
        )
    }
    pub fn validate_metadata(&self) -> Result<(), InputError> {
        if !matches!(self.schema_version, 1 | 2)
            || self.condition.is_empty()
            || self.condition.len() > 64
            || self
                .operation_id
                .as_ref()
                .is_some_and(|id| id.is_empty() || id.len() > 128)
        {
            return Err(InputError::Invalid("application result metadata"));
        }
        if let OperationOutput::Error { code, detail } = &self.result
            && (code.is_empty() || code.len() > 64 || detail.len() > 16 * 1024)
        {
            return Err(InputError::Capacity);
        }
        Ok(())
    }
}
impl From<AccessError> for OperationOutput {
    fn from(error: AccessError) -> Self {
        Self::Error {
            code: "access".into(),
            detail: error.to_string(),
        }
    }
}

/// The descriptor of a watch, transfer or administration tool by name.
pub fn find_surface(name: &str) -> Option<&'static OperationDescriptor> {
    super::watch_descriptors()
        .iter()
        .chain(super::transfer_descriptors())
        .chain(super::admin_descriptors())
        .find(|descriptor| descriptor.name == name)
}
/// Which surface serves `name`; the application surface covers both engines'
/// catalogues and the recovery tools are the adapter's own.
pub fn surface_of(name: &str) -> Option<Surface> {
    if let Some(descriptor) = find_surface(name) {
        return Some(descriptor.surface);
    }
    if find(name).is_some() || super::find_native(name).is_some() {
        return Some(Surface::Application);
    }
    None
}
