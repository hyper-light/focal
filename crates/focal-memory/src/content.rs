use crate::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, checked_add,
};
use std::sync::Arc;

struct Content<T> {
    value: T,
    _allocation: Allocation,
}

/// One accounted authored value shared by multiple lifecycle/page versions.
/// Domain content types must contain no interior mutability. Cloning this
/// handle copies no authored bytes, and the final reader releases its charge.
pub struct ImmutableContent<T>(Arc<Content<T>>);

impl<T> ImmutableContent<T> {
    pub fn new(
        value: T,
        heap_bytes: usize,
        budget: &MemoryBudget,
        lane: BudgetLane,
    ) -> Result<Self, MemoryError> {
        let bytes = checked_add(
            ALLOCATOR_OVERHEAD,
            checked_add(
                checked_add(
                    size_of::<Content<T>>(),
                    crate::checked_mul(2, size_of::<usize>())?,
                )?,
                heap_bytes,
            )?,
        )?;
        let allocation = budget.reserve(BudgetKind::Payload, lane, bytes)?.commit();
        Ok(Self(Arc::new(Content {
            value,
            _allocation: allocation,
        })))
    }

    pub fn get(&self) -> &T {
        &self.0.value
    }

    pub fn allocated_bytes(&self) -> usize {
        self.0._allocation.bytes()
    }
}

impl<T> Clone for ImmutableContent<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ImmutableContent<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get().fmt(f)
    }
}

impl<T: PartialEq> PartialEq for ImmutableContent<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl<T: Eq> Eq for ImmutableContent<T> {}

/// Authored content and mutable lifecycle have distinct ownership. A new
/// version shares the immutable content and receives a new lifecycle value.
#[derive(Debug, PartialEq, Eq)]
pub struct VersionedRecord<C, L> {
    pub content: ImmutableContent<C>,
    pub lifecycle: L,
}

impl<C, L: Clone> Clone for VersionedRecord<C, L> {
    fn clone(&self) -> Self {
        Self {
            content: self.content.clone(),
            lifecycle: self.lifecycle.clone(),
        }
    }
}

impl<C, L> VersionedRecord<C, L> {
    pub fn with_lifecycle(&self, lifecycle: L) -> Self {
        Self {
            content: self.content.clone(),
            lifecycle,
        }
    }
}
