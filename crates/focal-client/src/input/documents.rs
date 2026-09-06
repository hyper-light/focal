use serde::{Deserialize, Serialize};

/// Human spellings are separate from the model's frozen numeric enum encodings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimDocument {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub occurrence: Option<String>,
    pub description: String,
    pub target: String,
    #[serde(default = "work")]
    pub action: String,
    #[serde(default)]
    pub scopes: Vec<ScopeDocument>,
    #[serde(default)]
    pub relations: Vec<ClaimRelationDocument>,
    #[serde(default)]
    pub deadline: Option<DeadlineDocument>,
    pub validations: Vec<ValidationDocument>,
}
fn work() -> String {
    "work".into()
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeDocument {
    pub kind: String,
    pub key: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRelationDocument {
    pub kind: String,
    pub target: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadlineDocument {
    pub timer: String,
    pub generation: u64,
    pub at: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationDocument {
    #[serde(default)]
    pub id: Option<String>,
    pub kind: String,
    pub phase: String,
    pub mode: String,
    pub description: String,
    pub evaluator: String,
    #[serde(default)]
    pub quality_bar: Option<String>,
    #[serde(default)]
    pub handlers: Vec<HandlerDocument>,
    #[serde(default)]
    pub evidence_schemas: Vec<String>,
    #[serde(default)]
    pub contributed_by: Vec<String>,
    #[serde(default)]
    pub policy_revision: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandlerDocument {
    pub id: String,
    pub version: String,
    pub agentic: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptDocument {
    pub id: String,
    pub epoch: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReferenceDocument {
    pub id: String,
    pub hash: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestamentDocument {
    #[serde(default)]
    pub id: Option<String>,
    pub claim: String,
    pub receipt: ReceiptDocument,
    pub evidence_set: String,
    pub manifest: Vec<ArtifactReferenceDocument>,
    pub summary: String,
    pub confidence: String,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDocument {
    pub claim: String,
    pub receipt: ReceiptDocument,
    pub evidence_set: String,
    #[serde(default)]
    pub id: Option<String>,
    pub kind: String,
    pub schema_hash: String,
    #[serde(default)]
    pub metadata: Vec<u8>,
    pub payload: PayloadDocument,
    #[serde(default)]
    pub inputs: Vec<ObjectReferenceDocument>,
    #[serde(default)]
    pub visibility: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PayloadDocument {
    Inline { bytes: Vec<u8> },
    Text { text: String },
    Content { reference: ContentReferenceDocument },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentReferenceDocument {
    pub domain: String,
    pub root: String,
    pub length: u64,
    pub class: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectReferenceDocument {
    pub kind: String,
    pub id: String,
}
