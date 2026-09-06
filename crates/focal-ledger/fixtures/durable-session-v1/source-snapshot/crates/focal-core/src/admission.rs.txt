//! Current proposal policy, separate from historical domain execution.
use crate::*;

/// Additional admission policy for newly proposed work only. Historical intents
/// replay through the frozen reducer, preserving their original results.
pub(crate) fn validate_receipt_admission(
    state: &access::WriteState<'_>,
    command: &Command,
) -> Result<(), DomainOutcome> {
    fn validation(value: &ValidationContent) -> Result<(), DomainOutcome> {
        if value.kind == ValidationKind::Receipt
            && (value.phase != ValidationPhase::WholeWork
                || value.quality_bar.is_some()
                || !value.handlers.is_empty()
                || !value.evidence_schemas.is_empty())
        {
            return Err(refuse(
                ErrorCode::InvalidSchema,
                "receipt is whole-work delivery only; use a separate test, inspection or contract validation for evidence and quality",
            ));
        }
        Ok(())
    }
    fn claim(value: &NewClaim) -> Result<(), DomainOutcome> {
        value
            .validations
            .iter()
            .try_for_each(|v| validation(&v.content))
    }
    match command {
        Command::GenerateClaim { claim: value }
        | Command::SupersedeClaim {
            successor: value, ..
        } => claim(value),
        Command::GenerateClaimBatch { claims } => claims.iter().try_for_each(claim),
        Command::AcknowledgeTestament { claim, .. }
        | Command::BeginWholeWorkValidation { claim }
        | Command::CompleteWholeWork { claim } => {
            if let Some(value) = state.claims.get(claim) {
                for requirement in &value.content().requirements {
                    if let Some(value) = state.validations.get(&requirement.id) {
                        validation(value.content())?;
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
