//! Two bounded in-place sorts: collector-canonical tokens, then key-sorted row
//! replacements. Token indexes are rebound between sorts, never left stale.
use super::*;
use crate::native::completion_book::compare_seals;
use std::cmp::Ordering;

impl CohortSeals {
    pub(super) fn canonicalize(&mut self) -> Result<(), NativeError> {
        self.sort(true)?;
        let mut previous = None;
        for (index, token) in self.tokens.iter().enumerate() {
            self.visits.take(1)?;
            if previous.is_some_and(|old| compare_seals(old, token) != Ordering::Less) {
                return Err(ContractError::InvalidManifest.into());
            }
            self.updates
                .get_mut(index)
                .ok_or(ContractError::InvalidManifest)?
                .token_index = index;
            previous = Some(token);
        }
        self.sort(false)?;
        let mut previous = None;
        for update in &self.updates {
            self.visits.take(1)?;
            if previous.is_some_and(|old| old >= update.key) {
                return Err(ContractError::InvalidManifest.into());
            }
            previous = Some(update.key);
        }
        Ok(())
    }

    fn compare(
        &mut self,
        left: usize,
        right: usize,
        tokens: bool,
    ) -> Result<Ordering, NativeError> {
        self.visits.take(1)?;
        if tokens {
            let left = self.tokens.get(left).ok_or(ContractError::Capacity)?;
            let right = self.tokens.get(right).ok_or(ContractError::Capacity)?;
            Ok(compare_seals(left, right))
        } else {
            let left = self.updates.get(left).ok_or(ContractError::Capacity)?;
            let right = self.updates.get(right).ok_or(ContractError::Capacity)?;
            Ok(left.key.cmp(&right.key))
        }
    }

    fn exchange(&mut self, left: usize, right: usize, tokens: bool) -> Result<(), NativeError> {
        self.visits.take(1)?;
        let a = *self.updates.get(left).ok_or(ContractError::Capacity)?;
        let b = *self.updates.get(right).ok_or(ContractError::Capacity)?;
        *self.updates.get_mut(left).ok_or(ContractError::Capacity)? = b;
        *self.updates.get_mut(right).ok_or(ContractError::Capacity)? = a;
        if tokens {
            let a = *self.tokens.get(left).ok_or(ContractError::Capacity)?;
            let b = *self.tokens.get(right).ok_or(ContractError::Capacity)?;
            *self.tokens.get_mut(left).ok_or(ContractError::Capacity)? = b;
            *self.tokens.get_mut(right).ok_or(ContractError::Capacity)? = a;
        }
        Ok(())
    }

    fn sift(&mut self, mut root: usize, end: usize, tokens: bool) -> Result<(), NativeError> {
        loop {
            let left = root
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .ok_or(ContractError::Capacity)?;
            if left >= end {
                return Ok(());
            }
            let right = add(left, 1)?;
            let child = if right < end && self.compare(left, right, tokens)?.is_lt() {
                right
            } else {
                left
            };
            if !self.compare(root, child, tokens)?.is_lt() {
                return Ok(());
            }
            self.exchange(root, child, tokens)?;
            root = child;
        }
    }

    fn sort(&mut self, tokens: bool) -> Result<(), NativeError> {
        let length = self.updates.len();
        let mut root = length.checked_div(2).ok_or(ContractError::Capacity)?;
        while root != 0 {
            root = root.checked_sub(1).ok_or(ContractError::Capacity)?;
            self.sift(root, length, tokens)?;
        }
        let mut end = length;
        while end > 1 {
            end = end.checked_sub(1).ok_or(ContractError::Capacity)?;
            self.exchange(0, end, tokens)?;
            self.sift(0, end, tokens)?;
        }
        Ok(())
    }
}
