//! Native storage checks. Full definitions stay in their own immutable rows;
//! this owner manifest keeps only their exact private semantic stamps.
use super::*;
use crate::lifecycle::validation::{EvaluationState, State};

impl AcceptancePolicy {
    /// Exact all-definition correspondence, independent of input ordering. The
    /// immutable policy already proves complete slot/check coverage. Matching
    /// every stored declaration one-for-one therefore preserves that coverage
    /// without allocating or reconstructing a second AcceptancePolicy.
    ///
    /// Native Core supplies the actual complete definition cohort. Requiring
    /// equal cardinality alone is insufficient: duplicates cannot fill omissions.
    pub fn check_declarations<'a, I>(&self, declarations: I) -> Result<(), ContractError>
    where
        I: IntoIterator<Item = &'a Declaration>,
        I::IntoIter: Clone,
    {
        let rows = declarations.into_iter();
        let mut count = 0usize;
        for declaration in rows.clone() {
            if count == self.declarations.len() {
                return Err(ContractError::InvalidPolicy);
            }
            self.check_declaration(declaration)?;
            count = count.checked_add(1).ok_or(ContractError::Capacity)?;
        }
        if count != self.declarations.len() {
            return Err(ContractError::InvalidPolicy);
        }
        for required in &self.declarations {
            let mut matching = false;
            for declaration in rows.clone() {
                if declaration.binding().object == required.binding.object {
                    if matching {
                        return Err(ContractError::InvalidPolicy);
                    }
                    matching = true;
                }
            }
            if !matching {
                return Err(ContractError::InvalidPolicy);
            }
        }
        Ok(())
    }

    pub fn check_declaration(
        &self,
        declaration: &Declaration,
    ) -> Result<DeclaredObligation, ContractError> {
        if declaration.claim() != ClaimId(self.claim.object.0)
            || declaration.binding().ledger != self.claim.ledger
            || declaration.issuer() != self.issuer
        {
            return Err(ContractError::InvalidPolicy);
        }
        let record = self.declaration(ValidationId(declaration.binding().object.0))?;
        record.binding.check(&declaration.binding())?;
        if record.definition != declaration.definition_stamp()
            || record.index != declaration.declaration_index()
            || record.mode != declaration.mode()
            || record.target != declared_target(declaration.target())
        {
            return Err(ContractError::InvalidPolicy);
        }
        Ok(record)
    }

    /// A compact, reference-free registration fact. Native ownership stores it
    /// together with the new EvaluationState. It carries no copied policy and
    /// cannot be constructed by a transport caller selecting summary fields.
    pub fn checked_registration(
        &self,
        evaluation: &Evaluation<'_>,
    ) -> Result<RegisteredEvaluation, ContractError> {
        if evaluation.state() != State::Ready
            || evaluation.has_begun()
            || evaluation.sealed().is_some()
            || evaluation.fence().is_some()
            || evaluation.last_result().is_some()
        {
            return Err(ContractError::InvalidTransition);
        }
        let record = self.declaration(evaluation.validation())?;
        // Materialization begins at exactly the immutable declaration revision.
        record.binding.check(&evaluation.binding())?;
        if evaluation.claim() != ClaimId(self.claim.object.0)
            || evaluation.ledger() != self.claim.ledger
            || record.definition != evaluation.definition_stamp()
            || record.index != evaluation.declaration_index()
            || record.mode != evaluation.mode()
        {
            return Err(ContractError::InvalidPolicy);
        }
        let target = match evaluation.target() {
            Target::Artifact { slot, .. } | Target::MissingSlot { slot, .. } => {
                ObligationTarget::Slot(slot)
            }
            Target::Delivery { .. } => ObligationTarget::Delivery,
            Target::Admission { claim } => {
                same_content(self.claim, claim)?;
                ObligationTarget::Admission
            }
            Target::Increment { claim, .. } => {
                same_content(self.claim, claim)?;
                ObligationTarget::Increment
            }
        };
        if target != record.target {
            return Err(ContractError::InvalidTarget);
        }
        Ok(RegisteredEvaluation {
            definition: evaluation.definition_stamp(),
            binding: evaluation.binding(),
            target: evaluation.target(),
            generation: evaluation.generation(),
            receipt: evaluation.receipt(),
            index: record.index,
            mode: record.mode,
        })
    }
}

fn declared_target(target: TargetDeclaration<'_>) -> ObligationTarget {
    match target {
        TargetDeclaration::WholeWorkSlot { index, .. } => ObligationTarget::Slot(index),
        TargetDeclaration::Delivery => ObligationTarget::Delivery,
        TargetDeclaration::Admission => ObligationTarget::Admission,
        TargetDeclaration::Increment => ObligationTarget::Increment,
    }
}

impl RegisteredEvaluation {
    /// Resolve the exact independent row named by this retained registration.
    /// Later evaluation revisions are allowed; target, generation, receipt and
    /// complete definition semantics never drift with those lifecycle updates.
    pub fn check_state(
        self,
        state: EvaluationState,
        declaration: &Declaration,
    ) -> Result<(), ContractError> {
        let evaluation = state.bind(declaration)?;
        same_content(self.binding, evaluation.binding())?;
        if evaluation.binding().revision < self.binding.revision {
            return Err(ContractError::StaleRevision);
        }
        if self.definition != evaluation.definition_stamp()
            || self.target != evaluation.target()
            || self.generation != evaluation.generation()
            || self.receipt != evaluation.receipt()
            || self.index != evaluation.declaration_index()
            || self.mode != evaluation.mode()
        {
            return Err(ContractError::StaleEvaluation);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "acceptance_support_tests.rs"]
mod tests;
