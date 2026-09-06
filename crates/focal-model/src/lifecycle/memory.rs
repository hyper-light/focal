//! Native owned-row copy mechanics. Size estimates count Rust inline storage
//! and Vec capacities, not allocator metadata. The effective owner supplies the
//! surrounding reservation; these helpers neither publish nor reacquire it.
use super::ContractError;

pub(super) fn add(a: usize, b: usize) -> Result<usize, ContractError> {
    a.checked_add(b).ok_or(ContractError::Capacity)
}
pub(super) fn array<T>(count: usize) -> Result<usize, ContractError> {
    count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(ContractError::Capacity)
}
pub(super) fn total<T>(heap: usize) -> Result<usize, ContractError> {
    add(std::mem::size_of::<T>(), heap)
}
pub(super) fn allocation<T>(capacity: usize) -> usize {
    usize::from(capacity != 0 && std::mem::size_of::<T>() != 0)
}
pub(super) fn fits(charge: usize, max_bytes: usize) -> Result<(), ContractError> {
    if charge > max_bytes {
        Err(ContractError::Capacity)
    } else {
        Ok(())
    }
}
pub(super) fn reserve<T>(count: usize) -> Result<Vec<T>, ContractError> {
    #[cfg(test)]
    if count != 0 && std::mem::size_of::<T>() != 0 {
        test_allocation()?;
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| ContractError::Capacity)?;
    Ok(values)
}
pub(super) fn copy<T: Copy>(values: &[T]) -> Result<Vec<T>, ContractError> {
    let mut copied = reserve(values.len())?;
    copied.extend_from_slice(values);
    Ok(copied)
}

#[cfg(test)]
std::thread_local! {
    static COPY_FAILURE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}
#[cfg(test)]
fn test_allocation() -> Result<(), ContractError> {
    COPY_FAILURE.with(|remaining| match remaining.get() {
        Some(0) => Err(ContractError::Capacity),
        Some(value) => {
            remaining.set(Some(value - 1));
            Ok(())
        }
        None => Ok(()),
    })
}
#[cfg(test)]
pub(super) fn fail_after<T>(allocations: usize, action: impl FnOnce() -> T) -> T {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            COPY_FAILURE.with(|state| state.set(self.0));
        }
    }
    let _restore = Restore(COPY_FAILURE.with(|state| state.replace(Some(allocations))));
    action()
}
#[cfg(test)]
pub(super) fn remaining_allocations() -> Option<usize> {
    COPY_FAILURE.with(std::cell::Cell::get)
}
