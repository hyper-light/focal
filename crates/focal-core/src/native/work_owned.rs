//! Fallible owned work-lifecycle rows. Immutable descriptor buffers live once in
//! NativeArtifact; these independent rows contain only bounded Copy model state.
use focal_memory::MemoryError;
use focal_model::{
    ArtifactId,
    lifecycle::evidence::{ResponseDiagnostic, WorkArtifact},
};

const ALLOCATION: usize = 4 * size_of::<usize>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeWork {
    pub state: WorkArtifact,
    pub(super) next: Option<ArtifactId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeDiagnostic {
    pub diagnostic: ResponseDiagnostic,
    pub(super) next: Option<ArtifactId>,
}

#[derive(Debug)]
pub(super) struct OwnedWork(Vec<NativeWork>);
#[derive(Debug)]
pub(super) struct OwnedDiagnostic(Vec<NativeDiagnostic>);

fn charge<T>(capacity: usize) -> Result<usize, MemoryError> {
    capacity
        .checked_mul(size_of::<T>())
        .and_then(|bytes| bytes.checked_add(if capacity == 0 { 0 } else { ALLOCATION }))
        .ok_or(MemoryError::CounterExhausted("native work heap charge"))
}
fn within(actual: usize, maximum: usize) -> Result<(), MemoryError> {
    if actual > maximum {
        Err(MemoryError::Capacity {
            requested: actual,
            available: maximum,
        })
    } else {
        Ok(())
    }
}
fn singleton<T>(value: T) -> Result<Vec<T>, MemoryError> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(1)
        .map_err(|_| MemoryError::AllocationFailed)?;
    within(charge::<T>(rows.capacity())?, charge::<T>(1)?)?;
    rows.push(value);
    Ok(rows)
}
fn one<T>(rows: &[T]) -> Option<&T> {
    match rows {
        [row] => Some(row),
        _ => None,
    }
}

macro_rules! owned {
    ($owner:ident, $row:ty) => {
        impl $owner {
            const CONTAINER: usize = size_of::<$row>() + ALLOCATION;
            pub(super) const fn container_charge() -> usize {
                Self::CONTAINER
            }
            pub(super) fn new(value: $row) -> Result<Self, MemoryError> {
                Ok(Self(singleton(value)?))
            }
            pub(super) fn get(&self) -> Option<&$row> {
                one(&self.0)
            }
            pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
                self.get().ok_or(MemoryError::MissingKey)?;
                charge::<$row>(self.0.capacity())
            }
            pub(super) fn copy(&self) -> Result<Self, MemoryError> {
                let copied = Self::new(*self.get().ok_or(MemoryError::MissingKey)?)?;
                within(copied.heap_charge()?, self.heap_charge()?)?;
                Ok(copied)
            }
        }
    };
}
owned!(OwnedWork, NativeWork);
owned!(OwnedDiagnostic, NativeDiagnostic);
