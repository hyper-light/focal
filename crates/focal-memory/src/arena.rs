use crate::{
    ALLOCATOR_OVERHEAD, Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, checked_add,
    checked_mul,
};
use std::marker::PhantomData;

/// A process-local, non-reused range incarnation. The allocator/composition
/// layer supplies uniqueness; this pure crate does not invent random identities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArenaId(pub u128);

pub struct Handle<T> {
    arena: ArenaId,
    slot: u32,
    generation: u64,
    marker: PhantomData<fn() -> T>,
}

impl<T> Handle<T> {
    pub fn arena_id(self) -> ArenaId {
        self.arena
    }
    pub fn slot(self) -> u32 {
        self.slot
    }
    pub fn generation(self) -> u64 {
        self.generation
    }
}

impl<T> Copy for Handle<T> {}
impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> PartialEq for Handle<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.arena, self.slot, self.generation) == (other.arena, other.slot, other.generation)
    }
}
impl<T> Eq for Handle<T> {}
impl<T> std::fmt::Debug for Handle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("arena", &self.arena)
            .field("slot", &self.slot)
            .field("generation", &self.generation)
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ArenaConfig {
    pub page_slots: u32,
    pub max_slots: u32,
}

impl Default for ArenaConfig {
    fn default() -> Self {
        Self {
            page_slots: 128,
            max_slots: u32::MAX,
        }
    }
}

struct Slot<T> {
    value: Option<T>,
    payload: Option<Allocation>,
    generation: u64,
    next_free: Option<u32>,
}

struct Page<T> {
    slots: Box<[Slot<T>]>,
    _allocation: Allocation,
}

/// Safe generational arena with fixed-capacity slot pages. Empty pages retain
/// their generations until this incarnation is dropped; their charge remains
/// visible. Payload bytes are released on removal. No handle survives restart.
pub struct Arena<T> {
    id: ArenaId,
    config: ArenaConfig,
    budget: MemoryBudget,
    pages: Vec<Page<T>>,
    root_allocation: Option<Allocation>,
    free: Option<u32>,
    capacity: u32,
    len: u32,
    retired_slots: u32,
}

impl<T> Arena<T> {
    pub fn new(
        id: ArenaId,
        config: ArenaConfig,
        budget: MemoryBudget,
    ) -> Result<Self, MemoryError> {
        if config.page_slots == 0 || config.max_slots == 0 || config.page_slots > config.max_slots {
            return Err(MemoryError::InvalidConfiguration(
                "invalid arena page or slot limit",
            ));
        }
        Ok(Self {
            id,
            config,
            budget,
            pages: Vec::new(),
            root_allocation: None,
            free: None,
            capacity: 0,
            len: 0,
            retired_slots: 0,
        })
    }

    pub fn id(&self) -> ArenaId {
        self.id
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn capacity(&self) -> usize {
        self.capacity as usize
    }
    pub fn retired_slots(&self) -> usize {
        self.retired_slots as usize
    }

    /// `payload_bytes` includes heap capacity owned by the value, in addition
    /// to its inline `size_of::<T>()` already charged by the slot page.
    pub fn insert(
        &mut self,
        value: T,
        payload_bytes: usize,
        lane: BudgetLane,
    ) -> Result<Handle<T>, MemoryError> {
        let payload = self
            .budget
            .reserve(BudgetKind::Payload, lane, payload_bytes)?;
        if self.free.is_none() {
            self.add_page(lane)?;
        }
        let slot_index = self
            .free
            .ok_or(MemoryError::CounterExhausted("arena slots"))?;
        let next_len = self
            .len
            .checked_add(1)
            .ok_or(MemoryError::CounterExhausted("arena length"))?;
        let slot = self.slot_mut(slot_index).ok_or(MemoryError::StaleHandle)?;
        let next_free = slot.next_free;
        let generation = slot.generation;
        slot.value = Some(value);
        slot.payload = Some(payload.commit());
        slot.next_free = None;
        self.free = next_free;
        self.len = next_len;
        Ok(Handle {
            arena: self.id,
            slot: slot_index,
            generation,
            marker: PhantomData,
        })
    }

    pub fn get(&self, handle: Handle<T>) -> Result<&T, MemoryError> {
        self.validate(handle)?;
        self.slot(handle.slot)
            .and_then(|slot| slot.value.as_ref())
            .ok_or(MemoryError::StaleHandle)
    }

    /// Replacement reserves the new payload before releasing the old one, so
    /// rejection leaves both the value and its charge unchanged.
    pub fn replace(
        &mut self,
        handle: Handle<T>,
        value: T,
        payload_bytes: usize,
        lane: BudgetLane,
    ) -> Result<T, MemoryError> {
        self.validate(handle)?;
        let allocation = self
            .budget
            .reserve(BudgetKind::Payload, lane, payload_bytes)?
            .commit();
        let slot = self.slot_mut(handle.slot).ok_or(MemoryError::StaleHandle)?;
        let previous = slot.value.replace(value).ok_or(MemoryError::StaleHandle)?;
        slot.payload = Some(allocation);
        Ok(previous)
    }

    /// Returned values transfer out of storage ownership; the receiving caller
    /// is responsible for its own queue/response allocation allowance.
    pub fn remove(&mut self, handle: Handle<T>) -> Result<T, MemoryError> {
        self.validate(handle)?;
        let next_len = self.len.checked_sub(1).ok_or(MemoryError::StaleHandle)?;
        let next_retired = self
            .retired_slots
            .checked_add(1)
            .ok_or(MemoryError::CounterExhausted("retired slots"))?;
        let old_free = self.free;
        let slot = self.slot_mut(handle.slot).ok_or(MemoryError::StaleHandle)?;
        let value = slot.value.take().ok_or(MemoryError::StaleHandle)?;
        slot.payload = None;
        if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            slot.next_free = old_free;
            self.free = Some(handle.slot);
        } else {
            // Never wrap: this slot is permanently retired, even if that means
            // subsequent insertion must fail the arena's slot capacity check.
            slot.next_free = None;
            self.retired_slots = next_retired;
        }
        self.len = next_len;
        Ok(value)
    }

    fn validate(&self, handle: Handle<T>) -> Result<(), MemoryError> {
        if handle.arena != self.id {
            return Err(MemoryError::WrongArena);
        }
        match self.slot(handle.slot) {
            Some(slot) if slot.generation == handle.generation && slot.value.is_some() => Ok(()),
            _ => Err(MemoryError::StaleHandle),
        }
    }

    fn slot(&self, index: u32) -> Option<&Slot<T>> {
        self.pages
            .get(index.checked_div(self.config.page_slots)? as usize)
            .and_then(|page| {
                page.slots
                    .get(index.checked_rem(self.config.page_slots)? as usize)
            })
    }

    fn slot_mut(&mut self, index: u32) -> Option<&mut Slot<T>> {
        self.pages
            .get_mut(index.checked_div(self.config.page_slots)? as usize)
            .and_then(|page| {
                page.slots
                    .get_mut(index.checked_rem(self.config.page_slots)? as usize)
            })
    }

    fn add_page(&mut self, lane: BudgetLane) -> Result<(), MemoryError> {
        let count = self
            .config
            .page_slots
            .min(self.config.max_slots.saturating_sub(self.capacity));
        if count == 0 {
            return Err(if self.retired_slots > 0 {
                MemoryError::CounterExhausted("arena generations")
            } else {
                MemoryError::Capacity {
                    requested: 1,
                    available: 0,
                }
            });
        }
        let next_capacity = self
            .capacity
            .checked_add(count)
            .ok_or(MemoryError::CounterExhausted("arena capacity"))?;
        let page_bytes = checked_add(
            ALLOCATOR_OVERHEAD,
            checked_mul(count as usize, size_of::<Slot<T>>())?,
        )?;
        let page_charge = self.budget.reserve(BudgetKind::Arena, lane, page_bytes)?;
        let new_len = checked_add(self.pages.len(), 1)?;
        let root_bytes = checked_add(
            ALLOCATOR_OVERHEAD,
            checked_mul(new_len, size_of::<Page<T>>())?,
        )?;
        let root_charge = self.budget.reserve(BudgetKind::Arena, lane, root_bytes)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(count as usize)
            .map_err(|_| MemoryError::AllocationFailed)?;
        for offset in 0..count {
            let index = self
                .capacity
                .checked_add(offset)
                .ok_or(MemoryError::CounterExhausted("arena index"))?;
            let next_free = if offset.checked_add(1).is_some_and(|next| next < count) {
                Some(
                    index
                        .checked_add(1)
                        .ok_or(MemoryError::CounterExhausted("arena next slot"))?,
                )
            } else {
                None
            };
            slots.push(Slot {
                value: None,
                payload: None,
                generation: 1,
                next_free,
            });
        }
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(new_len)
            .map_err(|_| MemoryError::AllocationFailed)?;
        // No fallible work or clone calls after changing the old page directory.
        pages.append(&mut self.pages);
        pages.push(Page {
            slots: slots.into_boxed_slice(),
            _allocation: page_charge.commit(),
        });
        self.pages = pages;
        self.root_allocation = Some(root_charge.commit());
        self.free = Some(self.capacity);
        self.capacity = next_capacity;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maximum_generation_is_retired_instead_of_wrapping() {
        let budget = MemoryBudget::new(100_000, 0).unwrap();
        let mut arena = Arena::new(
            ArenaId(1),
            ArenaConfig {
                page_slots: 1,
                max_slots: 1,
            },
            budget,
        )
        .unwrap();
        let first = arena.insert(7, 0, BudgetLane::Ordinary).unwrap();
        arena.slot_mut(first.slot).unwrap().generation = u64::MAX;
        let last = Handle {
            generation: u64::MAX,
            ..first
        };
        assert_eq!(arena.remove(last).unwrap(), 7);
        assert_eq!(arena.get(last), Err(MemoryError::StaleHandle));
        assert_eq!(arena.retired_slots(), 1);
        assert_eq!(
            arena.insert(8, 0, BudgetLane::Ordinary),
            Err(MemoryError::CounterExhausted("arena generations"))
        );
    }
}
