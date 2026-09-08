//! Independent evaluation restoration from its exact immutable declaration.
use super::{bytes::{Cursor, Error}, read_fields, read_source::{Meter, model_error}};
use crate::native::{EvaluationKey, NativeError, OwnedEvaluation, Row};
use focal_model::lifecycle::{ContractError, validation};

pub(super) struct Input(validation::EvaluationSnapshotV1);
pub(super) struct Plan { value: validation::EvaluationState }
impl Input {
    pub(super) fn read(cursor: &mut Cursor<'_>) -> Result<Self, Error> {
        read_fields::evaluation_snapshot(cursor).map(Self)
    }
    pub(super) fn prepare(self, key: EvaluationKey, declaration: &validation::Declaration,
        meter: &Meter) -> Result<Plan, NativeError> {
        meter.charge(8).map_err(model_error)?;
        if declaration.claim() != key.claim || declaration.binding().object.0 != key.validation.0 {
            return Err(ContractError::InvalidTarget.into());
        }
        let visits = validation::EvaluationState::hydration_visits(declaration)?;
        meter.charge(visits).map_err(model_error)?;
        let value = validation::EvaluationState::hydrate_v1(declaration, self.0, visits)?;
        if EvaluationKey::of(declaration.claim(), &value) != key {
            return Err(ContractError::InvalidTarget.into());
        }
        Ok(Plan { value })
    }
}
impl Plan {
    pub(super) fn heap_bytes(&self) -> usize { OwnedEvaluation::container_charge() }
    pub(super) fn build(self, allowance: usize) -> Result<(Row, usize), NativeError> {
        if self.heap_bytes() > allowance { return Err(NativeError::Capacity("evaluation recovery allowance")); }
        let value = OwnedEvaluation::new(self.value)?;
        let actual = value.heap_charge()?;
        if actual > allowance { return Err(NativeError::Capacity("evaluation recovery capacity")); }
        Ok((Row::Evaluation(value), actual))
    }
}
