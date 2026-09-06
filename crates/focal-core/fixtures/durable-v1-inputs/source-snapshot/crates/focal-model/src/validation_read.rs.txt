//! Read projections; these do not alter persisted command or object schemas.
use crate::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ValidationResultPosition {
    pub run: ValidationRunId,
    /// None is the run summary. Some is the zero-based position in the committed
    /// attempts vector, equal to VerdictRecord::attempt. This is independent of
    /// handler_index, which may skip unused fallbacks before the quality phase.
    pub attempt: Option<u32>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationRunSummary {
    pub id: ValidationRunId,
    pub claim: ClaimId,
    pub evaluator: ParticipantId,
    pub manifest: ContentHash,
    pub handler_index: u32,
    pub quality_phase: bool,
    pub attempt_count: u32,
    pub final_verdict: Option<VerdictValue>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationResultValue {
    Run(ValidationRunSummary),
    Attempt(VerdictRecord),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationResult {
    pub position: ValidationResultPosition,
    pub value: ValidationResultValue,
}
