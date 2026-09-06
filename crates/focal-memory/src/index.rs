use crate::{
    ALLOCATOR_OVERHEAD, Allocation, Arena, ArenaId, BudgetKind, BudgetLane, Handle, MemoryBudget,
    MemoryError, checked_add, checked_mul,
};
use std::collections::BTreeMap;

/// Deterministically ordered stable-key -> typed local-handle index. Keys
/// represent persisted object identity; the values never leave the range owner.
pub struct StableIndex<K, T> {
    arena: ArenaId,
    budget: MemoryBudget,
    entries: BTreeMap<K, (Handle<T>, Allocation)>,
}

impl<K: Ord, T> StableIndex<K, T> {
    pub fn new(arena: ArenaId, budget: MemoryBudget) -> Self {
        Self {
            arena,
            budget,
            entries: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn insert(
        &mut self,
        key: K,
        handle: Handle<T>,
        key_heap_bytes: usize,
        lane: BudgetLane,
    ) -> Result<(), MemoryError> {
        if handle.arena_id() != self.arena {
            return Err(MemoryError::WrongArena);
        }
        if self.entries.contains_key(&key) {
            return Err(MemoryError::DuplicateKey);
        }
        // Charge a whole conservatively sized tree node per entry. The ordered
        // std implementation is intentionally hidden behind this interface.
        let charge = checked_add(
            key_heap_bytes,
            checked_add(
                ALLOCATOR_OVERHEAD,
                checked_mul(
                    16,
                    checked_add(size_of::<(K, Handle<T>, Allocation)>(), size_of::<usize>())?,
                )?,
            )?,
        )?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Index, lane, charge)?
            .commit();
        self.entries.insert(key, (handle, allocation));
        Ok(())
    }

    pub fn get(&self, key: &K) -> Option<Handle<T>> {
        self.entries.get(key).map(|entry| entry.0)
    }

    pub fn resolve<'a>(&self, key: &K, arena: &'a Arena<T>) -> Result<&'a T, MemoryError> {
        if arena.id() != self.arena {
            return Err(MemoryError::WrongArena);
        }
        arena.get(self.get(key).ok_or(MemoryError::MissingKey)?)
    }

    pub fn remove(&mut self, key: &K) -> Result<Handle<T>, MemoryError> {
        self.entries
            .remove(key)
            .map(|entry| entry.0)
            .ok_or(MemoryError::MissingKey)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, Handle<T>)> {
        self.entries.iter().map(|(key, entry)| (key, entry.0))
    }
}
