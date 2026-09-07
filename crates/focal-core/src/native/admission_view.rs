//! A borrowed admission projection over the effective owner and, optionally,
//! the one new report being prepared. There is no separately mutable aggregate.
use super::*;
use focal_model::lifecycle::aggregation;

pub(super) struct AdmissionRows<'a, 'b> {
    pub view: &'a View<'b>,
    pub sequence: SessionSeq,
    pub claim: ClaimId,
    pub report: Option<(&'a validation::EvaluationState, &'a NativeAccepted)>,
}
impl aggregation::AdmissionView for AdmissionRows<'_, '_> {
    fn prefix(&self) -> SessionSeq {
        self.sequence
    }
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.view.get(Key::Definition(id)))
    }
    fn evaluation(
        &self,
        row: aggregation::RegisteredEvaluation,
    ) -> Option<&validation::EvaluationState> {
        let key = transactions::key_for_registered(self.claim, row);
        if let Some((next, _)) = self.report
            && EvaluationKey::of(self.claim, next) == key
        {
            Some(next)
        } else {
            as_evaluation(self.view.get(Key::Evaluation(key)))
        }
    }
    fn accepted(
        &self,
        result: &validation::AcceptedResult,
    ) -> Option<aggregation::PublishedAdmissionResult<'_>> {
        let key = NativeResultKey::of(*result);
        let row = if let Some((_, accepted)) = self.report
            && NativeResultKey::of(accepted.result()) == key
        {
            accepted
        } else {
            as_result(self.view.get(Key::Accepted(key)))?
        };
        Some(aggregation::PublishedAdmissionResult {
            result: row.result_ref(),
            sequence: row.sequence(),
            ordinal: row.ordinal(),
        })
    }
}
