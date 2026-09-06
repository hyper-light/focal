use crate::*;
use focal_model::*;
use serde::{Deserialize, Serialize};

pub const MAX_LIST_CURSOR_BYTES: usize = 512;

/// Optional predicates are conjunctive. Each family accepts only its meaningful
/// predicates; unsupported combinations are errors rather than ignored input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListFilter {
    pub kind: ObjectKind,
    pub claim: Option<ClaimId>,
    pub testament: Option<TestamentId>,
    pub source: Option<ParticipantId>,
    pub target: Option<ParticipantId>,
    pub status: Option<ClaimStatus>,
    pub action: Option<ActionType>,
    pub producer: Option<ParticipantId>,
    pub artifact_kind: Option<String>,
    pub schema: Option<ContentHash>,
    pub evaluator: Option<ParticipantId>,
    pub validation_kind: Option<ValidationKind>,
    pub phase: Option<ValidationPhase>,
    pub mode: Option<ValidationMode>,
}
impl ListFilter {
    pub fn new(kind: ObjectKind) -> Self {
        Self {
            kind,
            claim: None,
            testament: None,
            source: None,
            target: None,
            status: None,
            action: None,
            producer: None,
            artifact_kind: None,
            schema: None,
            evaluator: None,
            validation_kind: None,
            phase: None,
            mode: None,
        }
    }
    pub fn validate(&self) -> Result<(), AccessError> {
        if self.claim.is_some_and(|id| id.is_zero())
            || self.testament.is_some_and(|id| id.is_zero())
            || [self.source, self.target, self.producer, self.evaluator]
                .into_iter()
                .flatten()
                .any(|id| id.is_zero())
            || self
                .schema
                .is_some_and(|hash| hash == ContentHash::default())
            || self
                .artifact_kind
                .as_ref()
                .is_some_and(|kind| kind.is_empty() || kind.len() > 256)
        {
            return Err(AccessError::InvalidRequest);
        }
        let claim_only = self.source.is_some()
            || self.target.is_some()
            || self.status.is_some()
            || self.action.is_some();
        let artifact_only = self.testament.is_some()
            || self.producer.is_some()
            || self.artifact_kind.is_some()
            || self.schema.is_some();
        let validation_only = self.evaluator.is_some()
            || self.validation_kind.is_some()
            || self.phase.is_some()
            || self.mode.is_some();
        if (claim_only && self.kind != ObjectKind::Claim)
            || (artifact_only && self.kind != ObjectKind::Artifact)
            || (validation_only && self.kind != ObjectKind::Validation)
        {
            return Err(AccessError::InvalidRequest);
        }
        Ok(())
    }
}

/// Opaque authenticated server continuation. Callers preserve its bytes exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListCursor {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListRequest {
    pub filter: ListFilter,
    pub cursor: Option<ListCursor>,
    pub max_items: u32,
    pub max_visits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListPage {
    pub token: ReadToken,
    pub objects: Vec<ReadObject>,
    /// May be present when objects is empty: residual filtering consumed a
    /// bounded source page and this cursor resumes after its last visited key.
    pub next: Option<ListCursor>,
    pub visited: u32,
}

pub fn list_scope(
    peer: &AuthenticatedPeer,
    ledger: LedgerId,
    filter: &ListFilter,
) -> Result<ContentHash, AccessError> {
    filter.validate()?;
    let role = match peer.role() {
        PeerRole::Actor => 1u8,
        PeerRole::Evaluator => 2,
        PeerRole::Runtime => 3,
        PeerRole::Node { .. } => 4,
    };
    let bytes = postcard::to_stdvec(&(peer.principal(), role, ledger, filter))
        .map_err(|_| AccessError::InvalidRequest)?;
    Ok(ContentHash(blake3::derive_key(
        "focal.list.authenticated-scope.v1",
        &bytes,
    )))
}

#[cfg(test)]
#[path = "list_tests.rs"]
mod tests;
