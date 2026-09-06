//! Fallible indirection for large native rows. Private singleton Vecs provide
//! owned storage without infallible allocation, unsafe code or shared pointers.
//! Entry already charges the inline Vec header; these charges cover allocated
//! elements, nested heaps and allocator metadata.
use super::history::StoredEvent;
use focal_memory::MemoryError;
use focal_model::lifecycle::{
    aggregation::RegistrationSet,
    claim::ClaimState,
    validation::{Declaration, EvaluationState},
};

const ALLOCATION: usize = 4 * size_of::<usize>();
const CLAIM_CONTAINER: usize = size_of::<ClaimRow>() + ALLOCATION;
const DECLARATION_CONTAINER: usize = size_of::<Declaration>() + ALLOCATION;
const EVALUATION_CONTAINER: usize = size_of::<EvaluationState>() + ALLOCATION;
const EVENT_CONTAINER: usize = size_of::<StoredEvent>() + ALLOCATION;

#[derive(Debug)]
struct ClaimRow {
    state: ClaimState,
    registrations: RegistrationSet,
}
#[derive(Debug)]
pub(super) struct OwnedClaim(Vec<ClaimRow>);
#[derive(Debug)]
pub(super) struct OwnedDeclaration(Vec<Declaration>);
#[derive(Debug)]
pub(super) struct OwnedEvaluation(Vec<EvaluationState>);
#[derive(Debug)]
pub(super) struct OwnedEvent(Vec<StoredEvent>);

fn add(left: usize, right: usize) -> Result<usize, MemoryError> {
    left.checked_add(right)
        .ok_or(MemoryError::CounterExhausted("native row heap charge"))
}
fn container_heap<T>(capacity: usize) -> Result<usize, MemoryError> {
    add(
        capacity
            .checked_mul(size_of::<T>())
            .ok_or(MemoryError::CounterExhausted("native row capacity"))?,
        if capacity == 0 { 0 } else { ALLOCATION },
    )
}
fn within(actual: usize, allowance: usize) -> Result<(), MemoryError> {
    if actual > allowance {
        Err(MemoryError::Capacity {
            requested: actual,
            available: allowance,
        })
    } else {
        Ok(())
    }
}
/// Caller holds the requested element charge before allocation. Refuse larger
/// reported capacity instead of performing a hidden shrink or reallocation.
fn singleton<T>(value: T, allowance: usize) -> Result<Vec<T>, MemoryError> {
    within(container_heap::<T>(1)?, allowance)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(1)
        .map_err(|_| MemoryError::AllocationFailed)?;
    within(container_heap::<T>(rows.capacity())?, allowance)?;
    rows.push(value);
    Ok(rows)
}
fn get<T>(rows: &[T]) -> Option<&T> {
    match rows {
        [row] => Some(row),
        _ => None,
    }
}
fn nested_heap(bytes: usize, allocations: usize) -> Result<usize, MemoryError> {
    add(
        bytes,
        allocations
            .checked_mul(ALLOCATION)
            .ok_or(MemoryError::CounterExhausted("native row allocator charge"))?,
    )
}
fn claim_heap(claim: &ClaimState) -> Result<usize, MemoryError> {
    nested_heap(
        claim
            .retained_heap_bytes()
            .map_err(|_| MemoryError::AllocationFailed)?,
        claim
            .heap_allocations()
            .map_err(|_| MemoryError::AllocationFailed)?,
    )
}
fn registrations_heap(registrations: &RegistrationSet) -> Result<usize, MemoryError> {
    nested_heap(
        registrations
            .retained_heap_bytes()
            .map_err(|_| MemoryError::AllocationFailed)?,
        registrations
            .heap_allocations()
            .map_err(|_| MemoryError::AllocationFailed)?,
    )
}
fn declaration_heap(declaration: &Declaration) -> Result<usize, MemoryError> {
    nested_heap(
        declaration
            .retained_heap_bytes()
            .map_err(|_| MemoryError::AllocationFailed)?,
        declaration
            .heap_allocations()
            .map_err(|_| MemoryError::AllocationFailed)?,
    )
}

impl OwnedClaim {
    /// Additional reservation for moving an already charged claim and set into
    /// one container. Their existing nested buffers move unchanged.
    pub(super) const fn container_charge() -> usize {
        CLAIM_CONTAINER
    }
    pub(super) fn new(
        state: ClaimState,
        registrations: RegistrationSet,
    ) -> Result<Self, MemoryError> {
        add(
            Self::container_charge(),
            add(claim_heap(&state)?, registrations_heap(&registrations)?)?,
        )?;
        Ok(Self(singleton(
            ClaimRow {
                state,
                registrations,
            },
            Self::container_charge(),
        )?))
    }
    pub(super) fn claim(&self) -> Option<&ClaimState> {
        get(&self.0).map(|row| &row.state)
    }
    pub(super) fn registrations(&self) -> Option<&RegistrationSet> {
        get(&self.0).map(|row| &row.registrations)
    }
    /// Native-owner access during private preparation. The owner precharges
    /// growth and publishes state and registration changes atomically.
    #[cfg(test)]
    pub(super) fn parts_mut(&mut self) -> Option<(&mut ClaimState, &mut RegistrationSet)> {
        match self.0.as_mut_slice() {
            [row] => Some((&mut row.state, &mut row.registrations)),
            _ => None,
        }
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        let row = get(&self.0).ok_or(MemoryError::MissingKey)?;
        add(
            container_heap::<ClaimRow>(self.0.capacity())?,
            add(
                claim_heap(&row.state)?,
                registrations_heap(&row.registrations)?,
            )?,
        )
    }
    /// Storage precharges the complete old row before copying a retained neighbor.
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let row = get(&self.0).ok_or(MemoryError::MissingKey)?;
        let state = row
            .state
            .try_copy(
                row.state
                    .retained_bytes()
                    .map_err(|_| MemoryError::AllocationFailed)?,
            )
            .map_err(|_| MemoryError::AllocationFailed)?;
        let registrations = row
            .registrations
            .try_copy(
                row.registrations
                    .retained_bytes()
                    .map_err(|_| MemoryError::AllocationFailed)?,
            )
            .map_err(|_| MemoryError::AllocationFailed)?;
        let copied = Self::new(state, registrations)?;
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

impl OwnedDeclaration {
    pub(super) const fn container_charge() -> usize {
        DECLARATION_CONTAINER
    }
    pub(super) fn new(declaration: Declaration) -> Result<Self, MemoryError> {
        add(Self::container_charge(), declaration_heap(&declaration)?)?;
        Ok(Self(singleton(declaration, Self::container_charge())?))
    }
    pub(super) fn get(&self) -> Option<&Declaration> {
        get(&self.0)
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        let declaration = self.get().ok_or(MemoryError::MissingKey)?;
        add(
            container_heap::<Declaration>(self.0.capacity())?,
            declaration_heap(declaration)?,
        )
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let declaration = self.get().ok_or(MemoryError::MissingKey)?;
        let charge = declaration
            .retained_bytes()
            .map_err(|_| MemoryError::AllocationFailed)?;
        let copied = Self::new(
            declaration
                .try_copy(charge)
                .map_err(|_| MemoryError::AllocationFailed)?,
        )?;
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

impl OwnedEvaluation {
    pub(super) const fn container_charge() -> usize {
        EVALUATION_CONTAINER
    }
    pub(super) fn new(evaluation: EvaluationState) -> Result<Self, MemoryError> {
        Ok(Self(singleton(evaluation, Self::container_charge())?))
    }
    pub(super) fn get(&self) -> Option<&EvaluationState> {
        get(&self.0)
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        self.get().ok_or(MemoryError::MissingKey)?;
        container_heap::<EvaluationState>(self.0.capacity())
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let copied = Self::new(*self.get().ok_or(MemoryError::MissingKey)?)?;
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

impl OwnedEvent {
    pub(super) const fn container_charge() -> usize {
        EVENT_CONTAINER
    }
    pub(super) fn new(event: StoredEvent) -> Result<Self, MemoryError> {
        Ok(Self(singleton(event, Self::container_charge())?))
    }
    pub(super) fn get(&self) -> Option<&StoredEvent> {
        get(&self.0)
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        self.get().ok_or(MemoryError::MissingKey)?;
        container_heap::<StoredEvent>(self.0.capacity())
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let copied = Self::new(*self.get().ok_or(MemoryError::MissingKey)?)?;
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

#[cfg(test)]
#[path = "owned_tests.rs"]
mod tests;
