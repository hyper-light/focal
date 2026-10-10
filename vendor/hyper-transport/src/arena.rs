//! A bounded table of values under generational identifiers (CLAUDE.md §1: arenas with
//! generational handles). An identifier names one value for ever: a freed slot's generation moves
//! on, so a stale identifier finds nothing, and a slot whose generation is exhausted is retired
//! instead of reused (mantle note 32 T4: identifiers that cannot wrap into an acknowledged one).
//!
//! A value's events wait in the endpoint's queue until the owner polls them. A slot is not freed
//! while events of its value wait, even once the value is removed: the identifier names nothing
//! from the removal on, but the slot stays taken until the last of its events is polled, so the
//! events waiting never name more values than the table holds (the endpoint's event bound,
//! `docs/transport.md` §4a).

use crate::Refusal;

/// The bits of an identifier that name the slot; the rest are its generation.
const SLOT_BITS: u32 = 32;

#[derive(Debug)]
struct Slot<T> {
    generation: u32,
    value: Option<T>,
    /// Events of this generation's value the owner has not polled.
    held: u32,
}

/// A table of at most `capacity` values.
#[derive(Debug)]
pub(crate) struct Arena<T> {
    slots: Vec<Slot<T>>,
    free: Vec<u32>,
    capacity: usize,
    /// Slots taken: holding a value, or held by the events of one removed.
    taken: usize,
    /// Slots holding a value.
    live: usize,
}

impl<T> Arena<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            capacity,
            taken: 0,
            live: 0,
        }
    }
    /// Store `value`, or refuse: [`Refusal::Exchanges`] at the bound, [`Refusal::Exhausted`]
    /// when every slot's generations are spent.
    pub(crate) fn insert(&mut self, value: T) -> Result<u64, Refusal> {
        if self.taken >= self.capacity {
            return Err(Refusal::Exchanges);
        }
        let index = match self.free.pop() {
            Some(index) => index,
            None => {
                let index = u32::try_from(self.slots.len()).map_err(|_| Refusal::Exhausted)?;
                self.slots.push(Slot {
                    generation: 0,
                    value: None,
                    held: 0,
                });
                index
            }
        };
        let slot = self
            .slots
            .get_mut(usize::try_from(index).map_err(|_| Refusal::Exhausted)?);
        let slot = slot.ok_or(Refusal::Exhausted)?;
        slot.value = Some(value);
        self.taken = self.taken.saturating_add(1);
        self.live = self.live.saturating_add(1);
        Ok((u64::from(slot.generation) << SLOT_BITS) | u64::from(index))
    }
    fn split(id: u64) -> Option<(usize, u32)> {
        let index = usize::try_from(id & u64::from(u32::MAX)).ok()?;
        let generation = u32::try_from(id >> SLOT_BITS).ok()?;
        Some((index, generation))
    }
    /// The slot `id` names, whether or not its value is still there.
    fn slot_mut(&mut self, id: u64) -> Option<(usize, &mut Slot<T>)> {
        let (index, generation) = Self::split(id)?;
        let slot = self.slots.get_mut(index)?;
        (slot.generation == generation).then_some((index, slot))
    }
    pub(crate) fn get(&self, id: u64) -> Option<&T> {
        let (index, generation) = Self::split(id)?;
        let slot = self.slots.get(index)?;
        (slot.generation == generation)
            .then_some(slot.value.as_ref())
            .flatten()
    }
    pub(crate) fn get_mut(&mut self, id: u64) -> Option<&mut T> {
        self.slot_mut(id)?.1.value.as_mut()
    }
    /// Take the value out; its identifier names nothing from now on. Its slot is freed now, or,
    /// while events of the value wait, when the last of them is polled ([`Arena::release`]).
    pub(crate) fn remove(&mut self, id: u64) -> Option<T> {
        let (index, slot) = self.slot_mut(id)?;
        let value = slot.value.take()?;
        let held = slot.held > 0;
        self.live = self.live.saturating_sub(1);
        if !held {
            self.free_slot(index);
        }
        Some(value)
    }
    /// One more event of value `id` waits for the owner; `false`, and nothing held, when `id`
    /// names no value.
    pub(crate) fn hold(&mut self, id: u64) -> bool {
        self.hold_if(id, |_| true)
    }
    /// One more event of value `id` waits for the owner if `wanted`, given the value, says so;
    /// whether one does.
    pub(crate) fn hold_if(&mut self, id: u64, wanted: impl FnOnce(&mut T) -> bool) -> bool {
        let Some((_, slot)) = self.slot_mut(id) else {
            return false;
        };
        let Some(value) = slot.value.as_mut() else {
            return false;
        };
        if !wanted(value) {
            return false;
        }
        slot.held = slot.held.saturating_add(1);
        true
    }
    /// The owner polled one of `id`'s events: the slot is freed with the last of them once its
    /// value is removed.
    pub(crate) fn release(&mut self, id: u64) {
        self.release_with(id, |_| {});
    }
    /// [`Arena::release`], first letting `polled` see the value, if it is still there.
    pub(crate) fn release_with(&mut self, id: u64, polled: impl FnOnce(&mut T)) {
        let Some((index, slot)) = self.slot_mut(id) else {
            return;
        };
        if let Some(value) = slot.value.as_mut() {
            polled(value);
        }
        slot.held = slot.held.saturating_sub(1);
        if slot.held == 0 && slot.value.is_none() {
            self.free_slot(index);
        }
    }
    /// A slot no value or event holds goes back, under its next generation; one whose generations
    /// are spent is retired.
    fn free_slot(&mut self, index: usize) {
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        self.taken = self.taken.saturating_sub(1);
        if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            if let Ok(index) = u32::try_from(index) {
                self.free.push(index);
            }
        }
    }
    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().filter_map(|slot| slot.value.as_ref())
    }
    /// The values held.
    pub(crate) fn len(&self) -> usize {
        self.live
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stale_identifier_names_nothing_and_the_bound_refuses() {
        let mut arena = Arena::new(2);
        let first = arena.insert("a").unwrap();
        let second = arena.insert("b").unwrap();
        assert_eq!(arena.insert("c"), Err(Refusal::Exchanges));
        assert_eq!(arena.remove(first), Some("a"));
        assert_eq!(arena.get(first), None);
        let third = arena.insert("c").unwrap();
        assert_ne!(third, first, "the slot is reused under a new generation");
        assert_eq!(
            (arena.get(third), arena.get(second)),
            (Some(&"c"), Some(&"b"))
        );
        assert_eq!(arena.remove(first), None);
        assert_eq!((arena.len(), arena.values().count()), (2, 2));
    }

    #[test]
    fn an_exhausted_slot_is_retired() {
        let mut arena = Arena::new(1);
        let id = arena.insert(1).unwrap();
        arena.slots[0].generation = u32::MAX;
        let last = (u64::from(u32::MAX) << SLOT_BITS) | (id & u64::from(u32::MAX));
        assert_eq!(arena.remove(last), Some(1));
        assert!(
            arena.free.is_empty(),
            "a slot past its last generation is not reused"
        );
        let next = arena.insert(2).unwrap();
        assert_eq!(next & u64::from(u32::MAX), 1);
    }

    /// A removed value's slot stays taken while its events wait, and is reused only once the
    /// last of them is polled: the table's bound holds the values and the events of the removed.
    #[test]
    fn a_slot_held_by_waiting_events_is_freed_with_the_last() {
        let mut arena = Arena::new(1);
        let id = arena.insert("a").unwrap();
        assert!(arena.hold(id) && arena.hold(id));
        assert_eq!(arena.remove(id), Some("a"));
        assert_eq!(arena.get(id), None, "the identifier names nothing");
        assert!(!arena.hold(id), "nothing more is held for a removed value");
        assert_eq!(arena.len(), 0);
        assert_eq!(arena.insert("b"), Err(Refusal::Exchanges), "still taken");
        arena.release(id);
        assert_eq!(
            arena.insert("b"),
            Err(Refusal::Exchanges),
            "one event waits"
        );
        arena.release(id);
        let next = arena.insert("b").unwrap();
        assert_ne!(next, id, "reused under its next generation");
        arena.release(id);
        assert_eq!(arena.get(next), Some(&"b"), "a stale release frees nothing");
    }
}
