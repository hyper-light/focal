//! Frozen V1 authored object inputs and trusted legacy/managed envelopes.
//!
//! Decoding preserves recorded authority, custody evidence, and request identity.
//! It neither authenticates a new caller nor applies present-day admission rules.
use super::{Ref, V1, Value};
use crate::{
    ArtifactContent, ArtifactId, AuthenticatedInput, AuthorityContext, Cause, ClaimContent,
    ClaimId, Command, ContentHash, EvidenceAttestation, LedgerId, ManagedAuthenticatedInput,
    ManagedRequestKey, NewArtifact, NewClaim, NewValidation, ObjectRevision, ParticipantId,
    RequestEpoch, RequestId, RequestStreamIdentity, RootCommandId, ValidationContent, ValidationId,
};
use serde::{Deserialize, Serialize};

v1_struct!(NewClaim {
    id: ClaimId,
    content: ClaimContent,
    validations: Vec<NewValidation>,
});
v1_struct!(NewValidation {
    id: ValidationId,
    content: ValidationContent,
});
v1_struct!(NewArtifact {
    id: ArtifactId,
    content: ArtifactContent,
});
v1_struct!(EvidenceAttestation {
    descriptor_hash: ContentHash,
    custody_revision: u64,
    durable: bool,
    schema_valid: bool,
});
v1_struct!(AuthorityContext {
    runtime: bool,
    cause: Cause,
    policy_revision: u64,
    logical_time: u64,
    evidence: Vec<EvidenceAttestation>,
});
v1_struct!(AuthenticatedInput {
    ledger: LedgerId,
    principal: ParticipantId,
    request_epoch: RequestEpoch,
    request_id: RequestId,
    expected_revision: Option<ObjectRevision>,
    authority: AuthorityContext,
    command: Command,
});
v1_struct!(RequestStreamIdentity {
    cluster: [u8; 16],
    ledger: LedgerId,
    principal: ParticipantId,
    slot: u32,
    generation: u64,
});
v1_struct!(ManagedRequestKey {
    stream: RequestStreamIdentity,
    ordinal: u64,
    id: RequestId,
});
v1_struct!(ManagedAuthenticatedInput {
    key: ManagedRequestKey,
    expected_revision: Option<ObjectRevision>,
    authority: AuthorityContext,
    command: Command,
});

// These are fixed Postcard enum ordinals, separate from typed relation targets.
#[derive(Serialize)]
enum CauseRefV1<'a> {
    Root(Ref<'a, RootCommandId>), // 0
    Claim(Ref<'a, ClaimId>),      // 1
}

#[derive(Deserialize)]
enum CauseValueV1 {
    Root(Value<RootCommandId>), // 0
    Claim(Value<ClaimId>),      // 1
}

impl V1 for Cause {
    fn serialize_v1<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Root(root) => CauseRefV1::Root(Ref(root)),
            Self::Claim(claim) => CauseRefV1::Claim(Ref(claim)),
        }
        .serialize(serializer)
    }

    fn deserialize_v1<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match CauseValueV1::deserialize(deserializer)? {
            CauseValueV1::Root(root) => Self::Root(root.0),
            CauseValueV1::Claim(claim) => Self::Claim(claim.0),
        })
    }
}

#[cfg(test)]
#[path = "inputs_tests.rs"]
mod tests;
