//! Detached registry replacements for final claim rows. Empty and single-row
//! plans stay inline. Canonical appends take constant lookup work; geometric
//! growth makes total pair moves linear over the append run.
//! An out-of-order insertion uses binary search and shifts at most `len` pairs.
//! `maximum` bounds that work; final consumption walks both sorted sets once.
//! Insertion and final-row validation each check the incoming owner's complete
//! policy identity once. Future multi-claim writers must budget those per-claim
//! declaration/slot/check traversals in addition to the collection operations.
use crate::native::prepare::{Scratch, array};
use crate::native::{ClaimId, ContractError, NativeError, RegistrationSet};
use focal_memory::MemoryError;
use focal_model::lifecycle::claim::ClaimState;

type Pair = (ClaimId, RegistrationSet);

enum Storage {
    Empty,
    One(Pair),
    Many(Vec<Pair>),
}

pub(in crate::native) struct RegistryOverrides {
    storage: Storage,
}

impl RegistryOverrides {
    pub(in crate::native) fn new() -> Self {
        Self {
            storage: Storage::Empty,
        }
    }

    pub(in crate::native) fn len(&self) -> usize {
        self.rows().len()
    }

    pub(in crate::native) fn is_empty(&self) -> bool {
        self.rows().is_empty()
    }

    /// Borrow a staged replacement without changing its ownership or order.
    pub(in crate::native) fn get(&self, claim: ClaimId) -> Option<&RegistrationSet> {
        self.rows()
            .binary_search_by_key(&claim, |pair| pair.0)
            .ok()
            .and_then(|index| self.rows().get(index))
            .map(|pair| &pair.1)
    }

    fn rows(&self) -> &[Pair] {
        match &self.storage {
            Storage::Empty => &[],
            Storage::One(pair) => std::slice::from_ref(pair),
            Storage::Many(rows) => rows,
        }
    }

    /// Nested registry storage is already owned and charged by its producer.
    /// Only the pair buffer is charged here. A replacement buffer's entire
    /// charge coexists with the old buffer; refusal preserves this collection
    /// and the caller's scratch counter exactly.
    pub(in crate::native) fn insert(
        &mut self,
        claim: &ClaimState,
        registry: RegistrationSet,
        maximum: usize,
        scratch: &mut Scratch,
    ) -> Result<(), NativeError> {
        registry.check(claim)?;
        let id = ClaimId(claim.binding().object.0);
        let len = self.len();
        let count = len
            .checked_add(1)
            .ok_or(NativeError::Capacity("registry overrides"))?;
        if count > maximum {
            return Err(NativeError::Capacity("registry overrides"));
        }
        let index = if self.rows().last().is_none_or(|last| last.0 < id) {
            len
        } else {
            match self.rows().binary_search_by_key(&id, |pair| pair.0) {
                Ok(_) => return Err(ContractError::InvalidTarget.into()),
                Err(index) => index,
            }
        };
        if len == 0 {
            self.storage = Storage::One((id, registry));
            return Ok(());
        }
        if let Storage::Many(rows) = &mut self.storage
            && rows.len() < rows.capacity()
        {
            // Binary search proved index <= len, and spare capacity is owned.
            rows.insert(index, (id, registry));
            return Ok(());
        }
        let capacity = len.checked_mul(2).unwrap_or(maximum).min(maximum);
        let quote = array::<Pair>(capacity)?;
        let mut charged = Scratch {
            used: scratch.used,
            max: scratch.max,
        };
        charged.charge(quote)?;
        let mut replacement = Vec::new();
        #[cfg(test)]
        let requested = tests::allocation_capacity(capacity)?;
        #[cfg(not(test))]
        let requested = capacity;
        replacement
            .try_reserve_exact(requested)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if array::<Pair>(replacement.capacity())? > quote {
            return Err(NativeError::Capacity("registry override capacity"));
        }
        // All fallible checks precede ownership transfer. The replacement has
        // room for every old pair and the new one, so neither operation grows it.
        match std::mem::replace(&mut self.storage, Storage::Empty) {
            Storage::Empty => {}
            Storage::One(pair) => replacement.push(pair),
            Storage::Many(rows) => replacement.extend(rows),
        }
        replacement.insert(index, (id, registry));
        self.storage = Storage::Many(replacement);
        *scratch = charged;
        Ok(())
    }

    /// Prove exact final-row ownership before allocation or publication. Final
    /// claim IDs must be strictly ordered, including when there are no overrides.
    pub(in crate::native) fn check_rows(&self, rows: &[ClaimState]) -> Result<(), NativeError> {
        let mut previous = None;
        let mut pending = self.rows().iter().peekable();
        for row in rows {
            let id = row.binding().object;
            if previous.is_some_and(|previous| previous >= id) {
                return Err(ContractError::InvalidTarget.into());
            }
            previous = Some(id);
            let Some((owner, registry)) = pending.peek() else {
                continue;
            };
            match ClaimId(id.0).cmp(owner) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Greater => {
                    return Err(ContractError::InvalidTarget.into());
                }
                std::cmp::Ordering::Equal => {
                    registry.check(row)?;
                    pending.next();
                }
            }
        }
        if pending.next().is_some() {
            return Err(ContractError::InvalidTarget.into());
        }
        Ok(())
    }
}

pub(in crate::native) enum IntoIter {
    Empty,
    One(Option<Pair>),
    Many(std::vec::IntoIter<Pair>),
}

impl Iterator for IntoIter {
    type Item = Pair;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::One(pair) => pair.take(),
            Self::Many(rows) => rows.next(),
        }
    }
}

impl IntoIterator for RegistryOverrides {
    type Item = Pair;
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self.storage {
            Storage::Empty => IntoIter::Empty,
            Storage::One(pair) => IntoIter::One(Some(pair)),
            Storage::Many(rows) => IntoIter::Many(rows.into_iter()),
        }
    }
}

#[cfg(test)]
#[path = "registry_overrides_tests.rs"]
mod tests;
