use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Cause {
    Root(RootCommandId),
    Claim(ClaimId),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RelationTarget {
    Participant(ParticipantId),
    Object(ObjectRef),
    Action(ActionType),
    Root(RootCommandId),
    /// An exact committed artifact at its descriptor hash: the evidence a
    /// challenge disputes or a correction cites (descriptor schema 2; never
    /// V1 content).
    Evidence(ArtifactRef),
}
/// How a challenge or consult may be followed up, authored immutably with the
/// claim (descriptor schema 2). Absent on schema-1 claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PeerPolicy {
    /// A failed challenge verdict may cause an authorized corrective claim.
    pub corrective_allowed: bool,
    /// How many follow-up consultations this claim may cause.
    pub max_follow_ups: u16,
    /// At most one correction may be caused by this claim.
    pub single_issuer: bool,
    /// Who, besides the issuer, may author the follow-up.
    pub escalation: Escalation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Escalation {
    /// Only the claim's issuer.
    None,
    /// The issuer or the claim's current receipt holder.
    Holder,
    /// The issuer, the holder or a designated evaluator of the claim.
    Evaluator,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Relation {
    pub kind: RelationKind,
    pub target: RelationTarget,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Scope {
    pub kind: ScopeKind,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimContent {
    pub ledger: LedgerId,
    pub schema: u16,
    pub occurrence: OccurrenceId,
    pub description: String,
    pub relations: BTreeSet<Relation>,
    pub scopes: BTreeSet<Scope>,
    /// Ordered requirement IDs and pinned immutable specification hashes.
    pub requirements: Vec<RequirementRef>,
    pub deadline: Option<Deadline>,
}
impl ClaimContent {
    pub fn issuer(&self) -> Option<ParticipantId> {
        crate::semantics_v1::issuer(self)
    }
    pub fn subject(&self) -> Option<ParticipantId> {
        crate::semantics_v1::subject(self)
    }
    pub fn action(&self) -> Option<ActionType> {
        crate::semantics_v1::action(self)
    }
    pub fn cause(&self) -> Option<Cause> {
        crate::semantics_v1::cause(self)
    }
    pub fn dependencies(&self, kind: RelationKind) -> impl Iterator<Item = ClaimId> + '_ {
        crate::semantics_v1::dependencies(self, kind)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RequirementRef {
    pub id: ValidationId,
    pub specification: ContentHash,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deadline {
    pub timer: TimerId,
    pub generation: u64,
    pub at: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewClaim {
    pub id: ClaimId,
    pub content: ClaimContent,
    pub validations: Vec<NewValidation>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewValidation {
    pub id: ValidationId,
    pub content: ValidationContent,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationContent {
    pub ledger: LedgerId,
    pub schema: u16,
    pub claim: ClaimId,
    pub kind: ValidationKind,
    pub phase: ValidationPhase,
    pub mode: ValidationMode,
    pub description: String,
    pub quality_bar: Option<String>,
    pub evaluator: ParticipantId,
    pub handlers: Vec<HandlerRef>,
    pub evidence_schemas: BTreeSet<ContentHash>,
    pub contributed_by: BTreeSet<ParticipantId>,
    pub policy_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerRef {
    pub id: ValidatorId,
    pub version: ContentHash,
    pub agentic: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRef {
    pub domain: ContentDomainId,
    pub root: ContentHash,
    pub length: u64,
    pub class: ContentClass,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactPayload {
    Inline(Vec<u8>),
    Content(ContentRef),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactContent {
    pub ledger: LedgerId,
    pub schema: u16,
    pub kind: String,
    pub schema_hash: ContentHash,
    pub metadata: Vec<u8>,
    pub payload: ArtifactPayload,
    pub producer: ParticipantId,
    pub receipt: Option<ReceiptFence>,
    pub inputs: BTreeSet<ObjectRef>,
    pub visibility: BTreeSet<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewArtifact {
    pub id: ArtifactId,
    pub content: ArtifactContent,
}
/// Trusted ingress evidence, obtained only after verification and durable custody.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceAttestation {
    pub descriptor_hash: ContentHash,
    pub custody_revision: u64,
    pub durable: bool,
    pub schema_valid: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub id: ArtifactId,
    pub hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestamentContent {
    pub ledger: LedgerId,
    pub schema: u16,
    pub claim: ClaimId,
    pub receipt: ReceiptFence,
    pub evidence_set: EvidenceSetId,
    pub artifacts: Vec<ArtifactRef>,
    pub summary: String,
    pub confidence: Confidence,
    pub outcome: OutcomeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptFence {
    pub receipt: ReceiptId,
    pub epoch: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub fence: ReceiptFence,
    pub holder: ParticipantId,
    pub acquired: SessionSeq,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusFact {
    pub status: ClaimStatus,
    pub sequence: SessionSeq,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimLifecycle {
    pub status: ClaimStatus,
    pub revision: ObjectRevision,
    pub created: SessionSeq,
    pub history: Vec<StatusFact>,
    pub receipt: Option<Receipt>,
    pub evidence_set: Option<EvidenceSetId>,
    pub testament: Option<TestamentId>,
    pub local_complete: bool,
    pub released: bool,
    pub terminal_witness: Option<ClaimId>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestamentLifecycle {
    pub created: SessionSeq,
    pub acknowledged: Option<SessionSeq>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLifecycle {
    pub created: SessionSeq,
    pub custody_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationLifecycle {
    pub created: SessionSeq,
    pub latest_epoch: u64,
}
/// Public consumers can read, but cannot mutate, authoritative content/lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredObject<C, L> {
    content: C,
    content_hash: ContentHash,
    lifecycle: L,
}
impl<C, L> StoredObject<C, L> {
    pub fn new(content: C, content_hash: ContentHash, lifecycle: L) -> Self {
        Self {
            content,
            content_hash,
            lifecycle,
        }
    }
    pub fn content(&self) -> &C {
        &self.content
    }
    pub fn content_hash(&self) -> ContentHash {
        self.content_hash
    }
    pub fn lifecycle(&self) -> &L {
        &self.lifecycle
    }
    /// Creates a new lifecycle version; only a state owner can publish this value.
    pub fn with_lifecycle(&self, lifecycle: L) -> Self
    where
        C: Clone,
    {
        Self {
            content: self.content.clone(),
            content_hash: self.content_hash,
            lifecycle,
        }
    }
}
pub type Claim = StoredObject<ClaimContent, ClaimLifecycle>;
pub type Testament = StoredObject<TestamentContent, TestamentLifecycle>;
pub type Validation = StoredObject<ValidationContent, ValidationLifecycle>;
pub type Artifact = StoredObject<ArtifactContent, ArtifactLifecycle>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ValidationRunId {
    pub validation: ValidationId,
    pub target_hash: ContentHash,
    pub phase: ValidationPhase,
    pub epoch: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationRun {
    pub id: ValidationRunId,
    pub claim: ClaimId,
    pub evaluator: ParticipantId,
    pub manifest: ContentHash,
    pub handler_index: u32,
    pub quality_phase: bool,
    pub attempts: Vec<VerdictRecord>,
    pub final_verdict: Option<VerdictValue>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerdictRecord {
    pub run: ValidationRunId,
    pub evaluator: ParticipantId,
    pub handler: HandlerRef,
    pub attempt: u32,
    pub manifest: ContentHash,
    pub value: VerdictValue,
    pub evidence: Vec<ArtifactRef>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceSet {
    pub id: EvidenceSetId,
    pub claim: ClaimId,
    pub receipt: ReceiptFence,
    pub artifacts: Vec<ArtifactRef>,
    pub closed: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WaitPredicate {
    Satisfied(ClaimId),
    Terminal(ClaimId),
    Released(ClaimId),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Monitor {
    pub id: MonitorId,
    pub owner: ClaimId,
    pub roots: BTreeSet<WaitPredicate>,
    pub deadline: Deadline,
    pub registered: SessionSeq,
    pub released: Option<SessionSeq>,
}
