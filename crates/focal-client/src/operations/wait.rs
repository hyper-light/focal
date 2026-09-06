use super::*;
use crate::claim_wait::ClaimWaitUntil;
use focal_wire::{ReadConsistency, ReadQuery};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimWaitDocument {
    pub claim: String,
    pub until: ClaimWaitUntil,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u32,
}
fn default_timeout() -> u32 {
    30_000
}
impl ClaimWaitDocument {
    pub fn build(
        self,
        context: &BuildContext,
    ) -> Result<(ReadRequest, ClaimWaitUntil, u32), InputError> {
        context.validate()?;
        if !(1..=30_000).contains(&self.timeout_ms) {
            return Err(InputError::Invalid("timeout_ms must be in 1..=30000"));
        }
        let reference = ObjectRef::claim(context.ledger, ClaimId(parse_id(&self.claim)?));
        let mut references = Vec::new();
        references
            .try_reserve_exact(1)
            .map_err(|_| InputError::Capacity)?;
        references.push(reference);
        Ok((
            ReadRequest {
                consistency: ReadConsistency::Linearizable,
                query: ReadQuery::Objects(references),
                max_items: 1,
            },
            self.until,
            self.timeout_ms,
        ))
    }
}
