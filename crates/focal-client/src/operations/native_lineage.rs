//! One claim's lineage as one observation at one prefix (the audit's F10):
//! the claim, its `caused_by` ancestors and its followers, every object read
//! exact at the first read's token, and what the observation's bounds left
//! out named beside what they took, so that a bounded sample is never
//! mistaken for a complete lineage.
use focal_model::{ClaimId, RelationKind, SessionSeq};
use focal_wire::{NativeListCursor, NativeObject, ReadToken};
use serde::{Deserialize, Serialize};

/// One claim's lineage at one prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeLineage {
    /// The prefix every object was read at: the first read's token, every
    /// later read exact at it.
    pub token: ReadToken,
    pub native_sequence: SessionSeq,
    pub logical_time: u64,
    /// The claim itself, fully expanded.
    pub claim: NativeObject,
    /// Its `caused_by` ancestors, nearest first, each with its content.
    pub ancestors: Vec<NativeObject>,
    /// The next ancestor the observation did not follow, its depth spent:
    /// the chain continues above it.
    pub ancestors_beyond: Option<ClaimId>,
    /// The ancestor not readable at this prefix (retired, say), where the
    /// chain stops short of its root.
    pub ancestors_missing: Option<ClaimId>,
    /// The committed claims that invalidate, refine or are caused by the
    /// claim, each with its content.
    pub followers: Vec<NativeObject>,
    /// What each relation's list left beyond the observation: followers
    /// listed but not read (past the related bound) and the list's own
    /// continuation where it did not reach its end. Empty when the
    /// followers are all of them.
    pub followers_beyond: Vec<NativeFollowersBeyond>,
    pub visited: u32,
}
/// What one relation's list left beyond the observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeFollowersBeyond {
    pub kind: RelationKind,
    /// Followers the list named that the observation did not read, past
    /// its bound.
    pub listed_not_read: u32,
    /// The list's continuation, where it did not reach its end within its
    /// visits; `claim.list` with the same relation filter resumes there.
    pub cursor: Option<NativeListCursor>,
}
impl NativeLineage {
    /// Whether the observation is the whole lineage at its prefix.
    pub fn is_complete(&self) -> bool {
        self.ancestors_beyond.is_none()
            && self.ancestors_missing.is_none()
            && self.followers_beyond.is_empty()
    }
}
