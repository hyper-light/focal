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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InputKind {
    Claim,
    Testament,
    Artifact,
    ClaimId,
    Progress,
    Cancel,
    Receipt,
    Evidence,
    Get,
    List,
    RequestEpoch,
    RequestStatus,
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
            input: InputKind::$input,
            family: $family,
        };
    };
}
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
    "Close an evidence set with a receipt-fenced testament and exact artifact manifest."
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
    Get,
    Some(ObjectKind::Claim),
    Read,
    false,
    false,
    "Read an exact claim ID at an authoritative or previously pinned prefix."
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
        ARTIFACT_SUBMIT,
        CLAIM_CANCEL,
        CLAIM_GET,
        CLAIM_LIST,
        CLAIM_POST,
        CLAIM_PROGRESS,
        CLAIM_SUBMIT,
        EVIDENCE_BEGIN,
        RECEIPT_ACQUIRE,
        REQUEST_EPOCH,
        REQUEST_STATUS,
        TESTAMENT_GET,
        TESTAMENT_LIST,
        TESTAMENT_SUBMIT,
        VALIDATION_GET,
        VALIDATION_LIST,
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
        "claim.submit" => document!(ClaimSubmit),
        "testament.submit" => document!(TestamentSubmit),
        "artifact.submit" => document!(ArtifactSubmit),
        "claim.post" => document!(ClaimPost),
        "claim.progress" => document!(ClaimProgress),
        "claim.cancel" => document!(ClaimCancel),
        "receipt.acquire" => document!(ReceiptAcquire),
        "evidence.begin" => document!(EvidenceBegin),
        "claim.get" => document!(ClaimGet),
        "testament.get" => document!(TestamentGet),
        "artifact.get" => document!(ArtifactGet),
        "validation.get" => document!(ValidationGet),
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
    Mutation { reply: MutationReply },
    Read { page: ReadPage },
    List { page: ListPage },
    Reconcile { reply: ReconcileReply },
    Error { code: String, detail: String },
}
impl ApplicationResult {
    pub fn is_error(&self) -> bool {
        matches!(
            &self.result,
            OperationOutput::Error { .. }
                | OperationOutput::Mutation {
                    reply: MutationReply::Pending(_)
                        | MutationReply::Domain(DomainOutcome::Refuse { .. })
                }
        )
    }
    pub fn validate_metadata(&self) -> Result<(), InputError> {
        if self.schema_version != 1
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
