use crate::MemoryError;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_OWNER: AtomicU64 = AtomicU64::new(0);

/// Process-local ownership provenance. This stamp is never serialized and is
/// unrelated to durable object identity, clocks, or transaction ordering. It
/// lets exclusively owned prepared states remain Send without shared roots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerId(u64);
impl OwnerId {
    pub fn new() -> Result<Self, MemoryError> {
        next(&NEXT_OWNER).map(Self)
    }
}
fn next(counter: &AtomicU64) -> Result<u64, MemoryError> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| MemoryError::CounterExhausted("owner identity"))?
        .checked_add(1)
        .ok_or(MemoryError::CounterExhausted("owner identity"))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identity_exhaustion_is_a_typed_error() {
        let counter = AtomicU64::new(u64::MAX);
        assert_eq!(
            next(&counter),
            Err(MemoryError::CounterExhausted("owner identity"))
        );
        assert_ne!(OwnerId::new().unwrap(), OwnerId::new().unwrap());
    }
}
