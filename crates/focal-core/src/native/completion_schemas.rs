//! Pinned schema bounds for one externally evaluated declaration. The owner
//! retains this set with its completion grant; it is not a verifier or a grant
//! to report a result. Registry lookups are allocation-free, trusted owner facts.
use super::{ContentHash, ContractError, NativeError, validation};
use focal_evidence::{NativeEvidenceError, NativeSchemaVerifier, NativeVerificationBudget};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError};

#[cfg(test)]
#[path = "completion_schemas_tests.rs"]
mod tests;

#[derive(Debug)]
pub(super) struct SchemaSet {
    budgets: Vec<NativeVerificationBudget>,
    // Drop the owned buffer before returning its accounting credit. The Grant
    // container accounts for this struct's inline fields, exactly once.
    _allocation: Allocation,
}

impl SchemaSet {
    pub(super) fn new(
        declaration: &validation::Declaration,
        source: &MemoryBudget,
        schemas: &impl NativeSchemaVerifier,
        max_visits: usize,
    ) -> Result<Self, NativeError> {
        // Resolve all contracts before seeking memory. Each pass inspects at
        // most max_visits handler references; retry counts do not expand them.
        let mut count = 0usize;
        for schema in declaration.evidence_schemas() {
            count = count
                .checked_add(1)
                .filter(|count| *count <= max_visits)
                .ok_or(NativeError::Capacity("completion schema visits"))?;
            NativeVerificationBudget::for_schema(schema, schemas)?;
        }
        if count == 0 {
            return Err(ContractError::InvalidPolicy.into());
        }
        let bytes = super::prepare::array::<NativeVerificationBudget>(count)?;
        let mut allocation = source
            .reserve(BudgetKind::Index, BudgetLane::Ordinary, bytes)?
            .commit();
        let mut budgets = Vec::new();
        budgets
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let actual = super::prepare::array::<NativeVerificationBudget>(budgets.capacity())?;
        if actual > bytes {
            return Err(MemoryError::Capacity {
                requested: actual,
                available: bytes,
            }
            .into());
        }
        for schema in declaration.evidence_schemas() {
            let quote = NativeVerificationBudget::for_schema(schema, schemas)?;
            if budgets.len() == budgets.capacity() {
                return Err(NativeError::Capacity("completion schema buffer"));
            }
            budgets.push(quote);
        }
        budgets.sort_unstable_by_key(|quote| quote.schema());
        // A trusted registry must return one immutable contract per hash. Do
        // not silently deduplicate contradictory answers within construction.
        if budgets.windows(2).any(|pair| {
            pair.first()
                .zip(pair.last())
                .is_some_and(|(left, right)| left.schema() == right.schema() && left != right)
        }) {
            return Err(NativeEvidenceError::VerificationBudgetChanged.into());
        }
        budgets.dedup_by_key(|quote| quote.schema());
        allocation.shrink_to(actual)?;
        Ok(Self {
            budgets,
            _allocation: allocation,
        })
    }

    pub(super) fn budget_for(
        &self,
        schema: ContentHash,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<&NativeVerificationBudget, NativeError> {
        let index = self
            .budgets
            .binary_search_by_key(&schema, |quote| quote.schema())
            .map_err(|_| ContractError::InvalidPolicy)?;
        let quote = self
            .budgets
            .get(index)
            .ok_or(ContractError::InvalidPolicy)?;
        quote.check_schema(schema, schemas)?;
        Ok(quote)
    }

    /// Owned quote buffer and allocator metadata; excludes inline Grant fields.
    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> usize {
        self._allocation.bytes()
    }
}
