//! The transport's bytes, charged to the node's memory budget, its buffers reused.
//!
//! hyper-transport asks its owner for every byte it holds: a connection's receive window
//! (accounting only), a message's head, a body read, a lane's frame. Each grant is charged to the
//! budget as `Pending` before anything is allocated and given back with its reservation, so a
//! node never holds transport bytes its budget refused. A frame's buffer is taken from a free list
//! of its size class (powers of two from 256 bytes, and above them one class of exactly the largest
//! grant the bus's classes make, derived from the transport's own `frame_grant`, so no buffer is
//! sized by a constant the transport could outgrow) and returned to it when the reservation is,
//! so a publish or a delivery in steady state allocates nothing: focal-wire's budget allocated a
//! buffer a grant. Each class keeps warm what its own demand has recently needed, the rule the
//! segment pool keeps ([`WorkingSet`]).

use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use hyper_transport::{Budget, Lane, Refusal, Reservation};

use crate::log::WorkingSet;
use crate::wire::{self, Bus, Class};
use hyper_transport::Classes;

/// Derived: the smallest buffer class, its bytes' power of two: the least any of the bus's messages
/// asks, a subscribe's head ([`wire::Subscribe::BYTES`]; a reply's and a gap's are 24 bytes, a
/// batch at least `batch::MIN_BATCH`), rounded up.
const SMALLEST_SHIFT: u32 = (wire::Subscribe::BYTES as u64)
    .next_power_of_two()
    .trailing_zeros();
/// Every class of the bus's messages, whose bounds the buffers are sized from.
const BUS_CLASSES: [Class; 3] = [Class::Control, Class::Deliver, Class::Publish];
/// The tag of a reservation that holds no buffer (a connection's window).
const WINDOW: u8 = 0;

/// The buffer classes: powers of two from the smallest below `top`, then `top` itself, the largest
/// grant the transport makes for the bus's classes (`frame_grant` of the largest frame bound): what
/// queuing the largest frame asks, its prefix with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Sizes {
    /// The largest grant, and the last class's size.
    top: u64,
    /// The last class's index: the count of power-of-two classes below `top`.
    last: usize,
}

impl Sizes {
    /// The bus's sizes, from its classes' bounds.
    fn of_bus() -> Self {
        let top = BUS_CLASSES
            .into_iter()
            .map(|class| hyper_transport::frame_grant(Bus::frame_bound(class)))
            .max()
            .unwrap_or(0);
        Self::new(top)
    }

    /// The sizes whose largest grant is `top`.
    fn new(top: u64) -> Self {
        let smallest = 1_u64 << SMALLEST_SHIFT;
        let mut last = 0_usize;
        let mut size = smallest;
        // Bounded: one turn for each power of two below a `u64`.
        for _ in 0..u64::BITS {
            if size >= top {
                break;
            }
            last = last.saturating_add(1);
            size = size.saturating_mul(2);
        }
        Self { top, last }
    }

    /// The class whose buffers hold `bytes`, and that class's size: `None` past the largest grant.
    fn class_of(&self, bytes: u64) -> Option<(usize, usize)> {
        if bytes > self.top {
            return None;
        }
        let power = bytes
            .max(1)
            .checked_next_power_of_two()?
            .max(1 << SMALLEST_SHIFT);
        if power >= self.top {
            return Some((self.last, usize::try_from(self.top).ok()?));
        }
        let class = usize::try_from(power.trailing_zeros().checked_sub(SMALLEST_SHIFT)?).ok()?;
        Some((class, usize::try_from(power).ok()?))
    }

    /// The size of class `class`'s buffers.
    fn size_of(&self, class: usize) -> usize {
        if class >= self.last {
            return usize::try_from(self.top).unwrap_or(usize::MAX);
        }
        1_usize
            .checked_shl(
                u32::try_from(class)
                    .unwrap_or(u32::MAX)
                    .saturating_add(SMALLEST_SHIFT),
            )
            .unwrap_or(0)
    }
}

/// A class's buffers kept for reuse, how many it has out, and what it keeps warm.
#[derive(Debug, Default)]
struct Shelf {
    warm: Vec<Vec<u8>>,
    out: usize,
    working: WorkingSet,
}

/// One endpoint's bytes, charged to the node's budget.
#[derive(Debug)]
pub struct BusBudget {
    budget: MemoryBudget,
    /// Everything the endpoint holds: grants and the warm buffers, one allocation absorbed into.
    held: Option<Allocation>,
    sizes: Sizes,
    /// One shelf a class: `sizes.last` and one.
    shelves: Vec<Shelf>,
    /// Grants refused for want of budget or memory.
    refused: u64,
    /// Buffers allocated, and returned to the system: a measurement's counts.
    allocated: u64,
    released: u64,
    /// Reservations given back, whatever became of their bytes: what tells an owner whose frame
    /// found no memory that some may be there now (`Node::starving`).
    returns: u64,
}

/// What a [`BusBudget`] holds and has done.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BusBudgetStats {
    /// Bytes charged: grants out and warm buffers.
    pub held: usize,
    /// Grants refused.
    pub refused: u64,
    /// Buffers allocated over the budget's life.
    pub allocated: u64,
    /// Buffers returned to the system.
    pub released: u64,
    /// Reservations given back, warm or returned to the system.
    pub returns: u64,
}

impl BusBudget {
    /// An endpoint's budget, charged to `budget`.
    pub fn new(budget: MemoryBudget) -> Self {
        let sizes = Sizes::of_bus();
        let mut shelves = Vec::new();
        shelves.resize_with(sizes.last.saturating_add(1), Shelf::default);
        Self {
            budget,
            held: None,
            sizes,
            shelves,
            refused: 0,
            allocated: 0,
            released: 0,
            returns: 0,
        }
    }

    /// What it holds and has done.
    pub fn stats(&self) -> BusBudgetStats {
        BusBudgetStats {
            held: self.held.as_ref().map_or(0, Allocation::bytes),
            refused: self.refused,
            allocated: self.allocated,
            released: self.released,
            returns: self.returns,
        }
    }

    fn charge(&mut self, bytes: usize) -> Result<(), Refusal> {
        let reservation =
            match self
                .budget
                .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bytes)
            {
                Ok(reservation) => reservation,
                // Every class's warm buffers give their bytes back before a grant is refused: what a
                // class keeps for its next frame goes first to a frame that needs it now.
                Err(_) if self.shed() > 0 => self
                    .budget
                    .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bytes)
                    .map_err(|_| self.refuse())?,
                Err(_) => return Err(self.refuse()),
            };
        let mut granted = reservation.commit();
        match &mut self.held {
            Some(held) => held.absorb(&mut granted).map_err(|_| self.refuse_now()),
            held @ None => {
                *held = Some(granted);
                Ok(())
            }
        }
    }

    fn discharge(&mut self, bytes: usize) {
        if let Some(held) = &mut self.held {
            let keep = held.bytes().saturating_sub(bytes);
            // Shrinking one's own allocation gives bytes back; it cannot be refused.
            let _ = held.shrink_to(keep);
        }
    }

    /// Returns every class's warm buffers to the system, their bytes to the budget; how many.
    fn shed(&mut self) -> usize {
        let sizes = self.sizes;
        let mut shed = 0_usize;
        let mut bytes = 0_usize;
        for (class, shelf) in self.shelves.iter_mut().enumerate() {
            let size = sizes.size_of(class);
            let count = shelf.warm.len();
            shelf.warm.clear();
            shed = shed.saturating_add(count);
            bytes = bytes.saturating_add(size.saturating_mul(count));
        }
        self.released = self
            .released
            .saturating_add(u64::try_from(shed).unwrap_or(u64::MAX));
        self.discharge(bytes);
        shed
    }

    fn refuse(&mut self) -> Refusal {
        self.refused = self.refused.saturating_add(1);
        Refusal::Budget
    }

    fn refuse_now(&self) -> Refusal {
        Refusal::Budget
    }
}

impl Budget<Class> for BusBudget {
    fn reserve(&mut self, bytes: u64, lane: Lane<Class>) -> Result<Reservation, Refusal> {
        match lane {
            Lane::Window => {
                let Ok(size) = usize::try_from(bytes) else {
                    return Err(self.refuse());
                };
                self.charge(size)?;
                Ok(Reservation::tagged(bytes, Vec::new(), WINDOW))
            }
            Lane::Class(_) => {
                let Some((class, size)) = self.sizes.class_of(bytes) else {
                    return Err(self.refuse());
                };
                let tag = u8::try_from(class.saturating_add(1)).map_err(|_| self.refuse_now())?;
                let warm = self
                    .shelves
                    .get_mut(class)
                    .and_then(|shelf| shelf.warm.pop());
                let buffer = match warm {
                    // A warm buffer is charged already.
                    Some(buffer) => buffer,
                    None => {
                        let mut buffer = Vec::new();
                        if buffer.try_reserve_exact(size).is_err() {
                            return Err(self.refuse());
                        }
                        self.charge(size)?;
                        self.allocated = self.allocated.saturating_add(1);
                        buffer
                    }
                };
                if let Some(shelf) = self.shelves.get_mut(class) {
                    shelf.out = shelf.out.saturating_add(1);
                    shelf.working.took(shelf.out);
                }
                Ok(Reservation::tagged(bytes, buffer, tag))
            }
        }
    }

    fn release(&mut self, reservation: Reservation) {
        self.returns = self.returns.saturating_add(1);
        let tag = reservation.tag();
        if tag == WINDOW {
            let bytes = usize::try_from(reservation.granted()).unwrap_or(usize::MAX);
            self.discharge(bytes);
            return;
        }
        let class = usize::from(tag.saturating_sub(1));
        let size = self.sizes.size_of(class);
        let Some(shelf) = self.shelves.get_mut(class) else {
            return;
        };
        shelf.out = shelf.out.saturating_sub(1);
        let keep = shelf.working.gave(shelf.out);
        if shelf.warm.len() < keep {
            let mut buffer = reservation.into_buffer();
            buffer.clear();
            shelf.warm.push(buffer);
        } else {
            drop(reservation);
            self.released = self.released.saturating_add(1);
            self.discharge(size);
        }
        // Demand fell: what a class keeps warm falls with it.
        let mut shed = 0_usize;
        if let Some(shelf) = self.shelves.get_mut(class) {
            while shelf.warm.len() > keep {
                shelf.warm.pop();
                shed = shed.saturating_add(1);
            }
        }
        if shed > 0 {
            self.released = self
                .released
                .saturating_add(u64::try_from(shed).unwrap_or(u64::MAX));
            self.discharge(size.saturating_mul(shed));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::wire::BATCH_BOUND;

    #[test]
    fn classes_cover_every_grant_the_transport_makes_for_the_bus() {
        let sizes = Sizes::of_bus();
        let largest = hyper_transport::frame_grant(BATCH_BOUND);
        assert_eq!(sizes.top, largest, "the largest frame and its prefix");
        // The smallest class holds a subscribe's head, 17 bytes, rounded up.
        assert_eq!(sizes.class_of(1), Some((0, 32)));
        assert_eq!(sizes.class_of(17), Some((0, 32)));
        assert_eq!(sizes.class_of(32), Some((0, 32)));
        assert_eq!(sizes.class_of(33), Some((1, 64)));
        assert_eq!(sizes.class_of(BATCH_BOUND), Some((15, 1 << 20)));
        // A batch at the bound, with the transport's prefix: the last class, of exactly its size.
        let last = usize::try_from(largest).unwrap();
        assert_eq!(sizes.class_of(largest), Some((16, last)));
        assert_eq!(sizes.size_of(16), last);
        assert_eq!(sizes.class_of(largest + 1), None);
    }

    /// The bug the derivation closed: a batch at the bound was refused for its prefix's bytes.
    #[test]
    fn a_batch_at_the_bound_with_its_prefix_is_held() {
        let budget = MemoryBudget::new(64 << 20, 0).unwrap();
        let mut bus = BusBudget::new(budget.clone());
        let frame = bus
            .reserve(
                hyper_transport::frame_grant(BATCH_BOUND),
                Lane::Class(Class::Publish),
            )
            .unwrap();
        bus.release(frame);
        drop(bus);
        assert_eq!(budget.stats().used, 0);
    }

    #[test]
    fn a_frame_buffer_is_reused_and_every_byte_is_given_back() {
        let budget = MemoryBudget::new(64 << 20, 0).unwrap();
        let mut bus = BusBudget::new(budget.clone());
        let first = bus.reserve(1_200, Lane::Class(Class::Publish)).unwrap();
        assert_eq!(budget.stats().used, 2_048);
        bus.release(first);
        for _ in 0..1_000 {
            let frame = bus.reserve(1_500, Lane::Class(Class::Deliver)).unwrap();
            bus.release(frame);
        }
        // One buffer served every frame of its class.
        assert_eq!(bus.stats().allocated, 1);
        let window = bus.reserve(70_000, Lane::Window).unwrap();
        assert_eq!(budget.stats().used, 2_048 + 70_000);
        bus.release(window);
        drop(bus);
        assert_eq!(budget.stats().used, 0);
    }

    #[test]
    fn a_grant_past_the_budget_or_the_frame_bound_is_refused() {
        let budget = MemoryBudget::new(4_096, 0).unwrap();
        let mut bus = BusBudget::new(budget);
        let held = bus.reserve(4_000, Lane::Class(Class::Publish)).unwrap();
        assert_eq!(
            bus.reserve(97, Lane::Class(Class::Publish)).err(),
            Some(Refusal::Budget)
        );
        assert_eq!(
            bus.reserve(
                hyper_transport::frame_grant(BATCH_BOUND) + 1,
                Lane::Class(Class::Publish)
            )
            .err(),
            Some(Refusal::Budget)
        );
        assert_eq!(bus.stats().refused, 2);
        bus.release(held);
    }
}
