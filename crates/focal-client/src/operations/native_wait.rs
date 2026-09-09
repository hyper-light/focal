//! The native claim observer's vocabulary: the predicate a wait is for and
//! the compact result it returns. The observation is the same shape as the
//! V1 observer's; `testament` is the native engine's additional predicate.
use crate::claim_wait::{ClaimObservation, ClaimWaitCondition};
use focal_model::ClaimStatus;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeWaitUntil {
    /// The issuer has received a closing testament (the claim reached
    /// `TestamentAcknowledged` or a later attained phase).
    Testament,
    Satisfied,
    Terminal,
    Released,
}
impl NativeWaitUntil {
    /// Whether the predicate holds for one observed claim state.
    pub fn met(self, status: ClaimStatus, released: bool) -> bool {
        match self {
            Self::Testament => matches!(
                status,
                ClaimStatus::TestamentAcknowledged
                    | ClaimStatus::Validating
                    | ClaimStatus::Satisfied
                    | ClaimStatus::ValidationIncomplete
                    | ClaimStatus::ValidationFailed
                    | ClaimStatus::ValidationErrored
            ),
            Self::Satisfied => status == ClaimStatus::Satisfied,
            Self::Terminal => status.is_terminal(),
            Self::Released => released,
        }
    }
    /// Whether a terminal claim can no longer meet the predicate.
    pub fn unmet_when_terminal(self) -> bool {
        matches!(self, Self::Testament | Self::Satisfied)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWaitResult {
    pub condition: ClaimWaitCondition,
    pub until: NativeWaitUntil,
    pub observation: ClaimObservation,
    pub probes: u32,
}
