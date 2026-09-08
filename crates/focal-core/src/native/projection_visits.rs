//! One owner-local traversal allowance for a borrowed owner projection.
//! Every nested cursor shares it; starting another iterator cannot replenish it.
use super::ContractError;
use std::cell::Cell;

pub(super) struct Visits {
    remaining: Cell<usize>,
    exhausted: Cell<bool>,
}
impl Visits {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            remaining: Cell::new(limit),
            exhausted: Cell::new(false),
        }
    }
    pub(super) fn charge(&self, count: usize) -> Result<(), ContractError> {
        self.check()?;
        match self.remaining.get().checked_sub(count) {
            Some(remaining) => {
                self.remaining.set(remaining);
                Ok(())
            }
            None => {
                self.exhausted.set(true);
                Err(ContractError::Capacity)
            }
        }
    }
    /// Infallible lookup adapters return None on exhaustion. The owner calls
    /// this before interpreting the model result so capacity is never reported
    /// as absent evidence or an incomplete participant result.
    pub(super) fn check(&self) -> Result<(), ContractError> {
        if self.exhausted.get() {
            Err(ContractError::Capacity)
        } else {
            Ok(())
        }
    }
    #[cfg(test)]
    pub(super) fn remaining(&self) -> usize {
        self.remaining.get()
    }
}
