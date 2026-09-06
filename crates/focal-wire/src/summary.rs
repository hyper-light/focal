use crate::{Operation, ReadToken, RequestEnvelope, ResponseEnvelope, WireError};
use serde::{Deserialize, Serialize};
/// Current committed counts in the selected ledger after a fresh quorum read.
/// These are object/run counts, not lifecycle outcomes or cluster-wide totals.
/// The token identifies the observed prefix; no historical lease is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerSummary {
    pub token: ReadToken,
    pub applied_index: u64,
    pub claims: u64,
    pub testaments: u64,
    pub artifacts: u64,
    pub validations: u64,
    pub evidence_sets: u64,
    pub validation_runs: u64,
}
pub(crate) fn validate(
    request: &RequestEnvelope,
    response: &ResponseEnvelope,
    value: &LedgerSummary,
) -> Result<(), WireError> {
    if !matches!(request.operation, Operation::Summary)
        || value.token.ledger != request.ledger
        || value.token.route_epoch != request.route_epoch
        || value.token.route_epoch != response.route_epoch
        || value.token.route_epoch.0 == 0
        || value.applied_index == 0
    {
        return Err(WireError::InvalidFrame);
    }
    Ok(())
}

#[cfg(test)]
#[path = "summary_tests.rs"]
mod tests;
