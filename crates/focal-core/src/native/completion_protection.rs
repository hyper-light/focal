//! Reverse membership of graph components covered by live Work grants.
//! Typed keys keep participant/evaluation identity separate from the protected
//! claim. The owner journals insertions; retiring credit keeps membership until
//! publication so a pending suffix can restore it without allocation.
use super::super::completion_index::{CompletionIndex, IndexGrowth};
use super::super::prepare::{array, within};
use super::super::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};

#[derive(Debug)]
pub(in crate::native) struct GraphMembers {
    ids: Vec<ClaimId>,
    _allocation: Allocation,
}
impl GraphMembers {
    pub(in crate::native) fn ids(&self) -> &[ClaimId] {
        &self.ids
    }

    pub(in crate::native) fn copy_from(
        source: &MemoryBudget,
        members: &[&ClaimState],
    ) -> Result<Self, NativeError> {
        let reservation = source.reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            array::<ClaimId>(members.len())?,
        )?;
        let mut ids = Vec::new();
        ids.try_reserve_exact(members.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(
            array::<ClaimId>(ids.capacity())?,
            array::<ClaimId>(members.len())?,
        )?;
        for claim in members {
            let id = ClaimId(claim.binding().object.0);
            if id.is_zero() || ids.last().is_some_and(|previous| *previous >= id) {
                return Err(ContractError::InvalidManifest.into());
            }
            if ids.len() == ids.capacity() {
                return Err(ContractError::Capacity.into());
            }
            ids.push(id);
        }
        if ids.is_empty() {
            return Err(ContractError::InvalidManifest.into());
        }
        Ok(Self {
            ids,
            _allocation: reservation.commit(),
        })
    }

    pub(super) fn contains(&self, id: ClaimId) -> bool {
        self.ids.binary_search(&id).is_ok()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    member: ClaimId,
    evaluation: EvaluationKey,
}

#[derive(Debug)]
struct Added {
    key: Key,
    growth: Option<IndexGrowth<(), Key>>,
}

#[derive(Debug)]
pub(super) struct Journal {
    added: Vec<Added>,
    _allocation: Allocation,
}

#[derive(Debug)]
pub(super) struct Protections {
    index: CompletionIndex<(), Key>,
}
impl Protections {
    pub(super) fn new() -> Self {
        Self {
            index: CompletionIndex::new(),
        }
    }

    pub(super) fn check_health(&self) -> Result<(), NativeError> {
        self.index.check_health()
    }

    pub(super) fn install(
        &mut self,
        source: &MemoryBudget,
        evaluation: EvaluationKey,
        members: &GraphMembers,
        limit: usize,
    ) -> Result<Journal, NativeError> {
        self.check_health()?;
        within(super::add(self.index.len(), members.ids.len())?, limit)?;
        if !members.contains(evaluation.claim) {
            return Err(ContractError::InvalidManifest.into());
        }
        let reservation = source.reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            array::<Added>(members.ids.len())?,
        )?;
        let mut added = Vec::new();
        added
            .try_reserve_exact(members.ids.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(
            array::<Added>(added.capacity())?,
            array::<Added>(members.ids.len())?,
        )?;
        let mut journal = Journal {
            added,
            _allocation: reservation.commit(),
        };
        let result = (|| {
            for member in &members.ids {
                let key = Key {
                    member: *member,
                    evaluation,
                };
                if self.index.get(key).is_some() {
                    return Err(ContractError::ContentConflict.into());
                }
                if journal.added.len() == journal.added.capacity() {
                    return Err(ContractError::Capacity.into());
                }
                let growth = self.index.grow(source, limit)?;
                if let Err(error) = self.index.insert(key, (), 0) {
                    if let Some(growth) = growth {
                        self.index.restore_growth(growth)?;
                    }
                    return Err(error);
                }
                journal.added.push(Added { key, growth });
            }
            self.check_health()
        })();
        if let Err(error) = result {
            self.rollback(journal)?;
            return Err(error);
        }
        Ok(journal)
    }

    pub(super) fn rollback(&mut self, mut journal: Journal) -> Result<(), NativeError> {
        while let Some(added) = journal.added.pop() {
            if let Some(growth) = &added.growth {
                self.index.check_remove_restore_growth(growth, added.key)?;
            }
            self.index.remove(added.key)?;
            if let Some(growth) = added.growth {
                self.index.restore_growth(growth)?;
            }
        }
        self.check_health()
    }

    pub(super) fn remove(
        &mut self,
        evaluation: EvaluationKey,
        members: &GraphMembers,
    ) -> Result<(), NativeError> {
        for member in &members.ids {
            if self
                .index
                .get(Key {
                    member: *member,
                    evaluation,
                })
                .is_none()
            {
                return Err(ContractError::InvalidCut.into());
            }
        }
        self.check_health()?;
        for member in &members.ids {
            self.index.remove(Key {
                member: *member,
                evaluation,
            })?;
        }
        self.check_health()
    }

    pub(super) fn affected(&self, member: ClaimId) -> impl Iterator<Item = EvaluationKey> + '_ {
        self.index
            .iter_from(Key {
                member,
                evaluation: EvaluationKey {
                    claim: ClaimId::from_u128(0),
                    validation: ValidationId::from_u128(0),
                    target: EvaluationTarget::Admission,
                    generation: 0,
                },
            })
            .take_while(move |(key, _)| key.member == member)
            .map(|(key, _)| key.evaluation)
    }
}

#[cfg(test)]
#[path = "completion_protection_tests.rs"]
mod tests;
