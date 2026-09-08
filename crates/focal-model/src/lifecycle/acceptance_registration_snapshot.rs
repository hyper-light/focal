//! Private materialization-frame restoration. The actual immutable declaration
//! and current independent evaluation supply semantic identity; no stamp is read
//! from an imported summary and no issuer/evaluator action is replayed.
use super::super::registration::RegistrationMemberSnapshotV1;
use super::*;
use crate::lifecycle::validation::EvaluationState;
impl RegisteredEvaluation {
    pub(in crate::lifecycle) fn hydrate_member_v1(
        value: RegistrationMemberSnapshotV1,
        declaration: &Declaration,
        state: EvaluationState,
    ) -> Result<Self, ContractError> {
        declaration.binding().check(&value.binding)?;
        if value.declaration_index != declaration.declaration_index()
            || value.mode != declaration.mode()
        {
            return Err(ContractError::InvalidPolicy);
        }
        let row = Self {
            definition: declaration.definition_stamp(),
            binding: value.binding,
            target: value.target,
            generation: value.generation,
            receipt: value.receipt,
            index: value.declaration_index,
            mode: value.mode,
        };
        row.check_state(state, declaration)?;
        Ok(row)
    }
}
