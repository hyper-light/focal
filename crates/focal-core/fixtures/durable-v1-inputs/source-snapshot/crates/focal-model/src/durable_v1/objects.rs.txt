//! Frozen V1 content and stored-object shapes. Admission remains the reducer's
//! responsibility: decoding preserves historical fields without normalizing
//! identifiers, recomputing hashes or inventing lifecycle facts.
use super::{Ref, V1, Value};
use crate::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};
use std::collections::BTreeSet;

v1_struct!(Relation {
    kind: RelationKind,
    target: RelationTarget,
});
v1_struct!(Scope {
    kind: ScopeKind,
    key: String,
});
v1_struct!(RequirementRef {
    id: ValidationId,
    specification: ContentHash,
});
v1_struct!(Deadline {
    timer: TimerId,
    generation: u64,
    at: u64,
});
v1_struct!(ClaimContent {
    ledger: LedgerId,
    schema: u16,
    occurrence: OccurrenceId,
    description: String,
    relations: BTreeSet<Relation>,
    scopes: BTreeSet<Scope>,
    requirements: Vec<RequirementRef>,
    deadline: Option<Deadline>,
});
v1_struct!(HandlerRef {
    id: ValidatorId,
    version: ContentHash,
    agentic: bool,
});
v1_struct!(ValidationContent {
    ledger: LedgerId,
    schema: u16,
    claim: ClaimId,
    kind: ValidationKind,
    phase: ValidationPhase,
    mode: ValidationMode,
    description: String,
    quality_bar: Option<String>,
    evaluator: ParticipantId,
    handlers: Vec<HandlerRef>,
    evidence_schemas: BTreeSet<ContentHash>,
    contributed_by: BTreeSet<ParticipantId>,
    policy_revision: u64,
});
v1_struct!(ContentRef {
    domain: ContentDomainId,
    root: ContentHash,
    length: u64,
    class: ContentClass,
});
v1_struct!(ReceiptFence {
    receipt: ReceiptId,
    epoch: u64,
});
v1_struct!(ArtifactContent {
    ledger: LedgerId,
    schema: u16,
    kind: String,
    schema_hash: ContentHash,
    metadata: Vec<u8>,
    payload: ArtifactPayload,
    producer: ParticipantId,
    receipt: Option<ReceiptFence>,
    inputs: BTreeSet<ObjectRef>,
    visibility: BTreeSet<String>,
});
v1_struct!(ArtifactRef {
    id: ArtifactId,
    hash: ContentHash,
});
v1_struct!(TestamentContent {
    ledger: LedgerId,
    schema: u16,
    claim: ClaimId,
    receipt: ReceiptFence,
    evidence_set: EvidenceSetId,
    artifacts: Vec<ArtifactRef>,
    summary: String,
    confidence: Confidence,
    outcome: OutcomeKind,
});

// These orders are Postcard enum ordinals, not the numeric vocabulary codes
// inside their payloads. Never delegate to RelationTarget's live Serde derive.
#[derive(Serialize)]
enum RelationTargetRefV1<'a> {
    Participant(Ref<'a, ParticipantId>),
    Object(Ref<'a, ObjectRef>),
    Action(Ref<'a, ActionType>),
    Root(Ref<'a, RootCommandId>),
}
#[derive(Deserialize)]
enum RelationTargetV1 {
    Participant(Value<ParticipantId>),
    Object(Value<ObjectRef>),
    Action(Value<ActionType>),
    Root(Value<RootCommandId>),
}
impl V1 for RelationTarget {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value = match self {
            Self::Participant(value) => RelationTargetRefV1::Participant(Ref(value)),
            Self::Object(value) => RelationTargetRefV1::Object(Ref(value)),
            Self::Action(value) => RelationTargetRefV1::Action(Ref(value)),
            Self::Root(value) => RelationTargetRefV1::Root(Ref(value)),
        };
        value.serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match RelationTargetV1::deserialize(deserializer)? {
            RelationTargetV1::Participant(Value(value)) => Self::Participant(value),
            RelationTargetV1::Object(Value(value)) => Self::Object(value),
            RelationTargetV1::Action(Value(value)) => Self::Action(value),
            RelationTargetV1::Root(Value(value)) => Self::Root(value),
        })
    }
}

#[derive(Serialize)]
enum ArtifactPayloadRefV1<'a> {
    Inline(Ref<'a, Vec<u8>>),
    Content(Ref<'a, ContentRef>),
}
#[derive(Deserialize)]
enum ArtifactPayloadV1 {
    Inline(Value<Vec<u8>>),
    Content(Value<ContentRef>),
}
impl V1 for ArtifactPayload {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value = match self {
            Self::Inline(value) => ArtifactPayloadRefV1::Inline(Ref(value)),
            Self::Content(value) => ArtifactPayloadRefV1::Content(Ref(value)),
        };
        value.serialize(serializer)
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match ArtifactPayloadV1::deserialize(deserializer)? {
            ArtifactPayloadV1::Inline(Value(value)) => Self::Inline(value),
            ArtifactPayloadV1::Content(Value(value)) => Self::Content(value),
        })
    }
}

#[derive(Deserialize)]
#[serde(bound(deserialize = ""))]
struct StoredObjectV1<C: V1, L: V1> {
    content: Value<C>,
    content_hash: Value<ContentHash>,
    lifecycle: Value<L>,
}
impl<C: V1, L: V1> V1 for StoredObject<C, L> {
    fn serialize_v1<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut object = serializer.serialize_struct("StoredObject", 3)?;
        object.serialize_field("content", &Ref(self.content()))?;
        object.serialize_field("content_hash", &Ref(&self.content_hash()))?;
        object.serialize_field("lifecycle", &Ref(self.lifecycle()))?;
        object.end()
    }
    fn deserialize_v1<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let StoredObjectV1 {
            content: Value(content),
            content_hash: Value(content_hash),
            lifecycle: Value(lifecycle),
        } = StoredObjectV1::deserialize(deserializer)?;
        // Move each final allocation once; an opaque saved hash is a historical
        // fact, not permission to reconstruct a different content identity.
        Ok(Self::new(content, content_hash, lifecycle))
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
