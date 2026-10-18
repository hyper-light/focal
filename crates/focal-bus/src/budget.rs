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
//! buffer a grant. A class keeps warm, charged, every buffer it has used, so it holds no more than
//! its own peak, which the budget allowed when it was reached, and a grant refused for want of
//! budget takes back what it needs, the smallest classes first ([`BusBudget::shed_for`]). The
//! segment pool's working set (`crate::log::WorkingSet`) handed a burst's buffers back once a
//! period peaked lower, and bursty traffic alternates its peaks: at 1,000,000 a second a node
//! allocated a 1 MiB buffer for every three to six publishes, each faulted in and cleared afresh as
//! a delivery was copied into it (86 % of its page faults, perf), where keeping them took ten in a
//! run, 9.6 % more messages a second and 7.2 % less CPU a message.

use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use hyper_transport::{Budget, Lane, Refusal, Reservation};

use crate::wire::{self, Bus, Class};
use hyper_transport::Classes;

/// Derived: the smallest buffer class, its bytes' power of two: the least any of the bus's messages
/// asks, a subscribe's head ([`wire::Subscribe::BYTES`]; a reply's and a gap's are 24 bytes, a
/// batch at least `batch::MIN_BATCH`), rounded up.
const SMALLEST_SHIFT: u32 = (wire::Subscribe::BYTES as u64)
    .next_power_of_two()
    .trailing_zeros();
/// Every class of the bus's messages, whose bounds the buffers are sized from.
const BUS_CLASSES: [Class; 4] = [
    Class::Control,
    Class::Replicate,
    Class::Deliver,
    Class::Publish,
];
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

/// A class's buffers kept warm for reuse.
#[derive(Debug, Default)]
struct Shelf {
    warm: Vec<Vec<u8>>,
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
                // Warm buffers give their bytes back before a grant is refused: what a class keeps
                // for its next frame goes first to a frame that needs it now, but only as much as
                // that frame needs.
                Err(_) if self.shed_for(bytes) > 0 => self
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

    /// Returns warm buffers to the system until the budget could grant `bytes`, the smallest
    /// classes first, their bytes to the budget; how many. A class keeps warm what its own next
    /// frames need, and the largest buffers are the dearest to allocate again, every page faulted
    /// and cleared afresh: every class shed at any refusal, so once a node's window filled its
    /// memory, a control frame's grant gave up the 1 MiB buffers its publishes and deliveries
    /// reused, and at 1,000,000 a second it allocated one for every three publishes.
    fn shed_for(&mut self, bytes: usize) -> usize {
        let sizes = self.sizes;
        let short = bytes.saturating_sub(self.budget.headroom(BudgetLane::Ordinary));
        let mut shed = 0_usize;
        let mut freed = 0_usize;
        // Bounded: one turn for each class and one for each warm buffer shed.
        for (class, shelf) in self.shelves.iter_mut().enumerate() {
            let size = sizes.size_of(class);
            while freed < short && shelf.warm.pop().is_some() {
                shed = shed.saturating_add(1);
                freed = freed.saturating_add(size);
            }
            if freed >= short {
                break;
            }
        }
        self.released = self
            .released
            .saturating_add(u64::try_from(shed).unwrap_or(u64::MAX));
        self.discharge(freed);
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

/// The most one of the transport's frames asks of its budget: the largest grant the bus's classes
/// make (`frame_grant` of the largest frame bound), the last buffer class's size.
pub fn largest_grant() -> u64 {
    Sizes::of_bus().top
}

/// What a transport's budget could grant one reservation now. A frame that waits for memory is
/// given the room of the node's window only while this is less than the frame may ask
/// ([`crate::node::Node::relieve_transport`]): past it, the frame waits for something no memory
/// gives.
pub trait Headroom {
    /// The bytes one reservation could take now.
    fn headroom(&self) -> u64;
}

impl Headroom for BusBudget {
    /// The budget's headroom, and the warm buffers every class gives back before a grant is
    /// refused.
    fn headroom(&self) -> u64 {
        let sizes = self.sizes;
        let warm = self
            .shelves
            .iter()
            .enumerate()
            .fold(0_usize, |sum, (class, shelf)| {
                sum.saturating_add(sizes.size_of(class).saturating_mul(shelf.warm.len()))
            });
        let room = self
            .budget
            .headroom(BudgetLane::Ordinary)
            .saturating_add(warm);
        u64::try_from(room).unwrap_or(u64::MAX)
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
        // Kept warm, charged, for the class's next frames; a list that cannot grow gives it back.
        if shelf.warm.try_reserve(1).is_ok() {
            let mut buffer = reservation.into_buffer();
            buffer.clear();
            shelf.warm.push(buffer);
        } else {
            drop(reservation);
            self.released = self.released.saturating_add(1);
            self.discharge(size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::wire::{BATCH_BOUND, RAFT_BOUND};

    #[test]
    fn classes_cover_every_grant_the_transport_makes_for_the_bus() {
        let sizes = Sizes::of_bus();
        // The largest frame is a group's append of the largest batch.
        let largest = hyper_transport::frame_grant(RAFT_BOUND);
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

    /// The headroom is what one grant can take: the budget's room and the warm buffers given back
    /// before a grant is refused, so a frame is never told memory is short while a warm buffer
    /// would hold it.
    #[test]
    fn the_headroom_is_what_one_grant_can_take() {
        let budget = MemoryBudget::new(8_192, 0).unwrap();
        let mut bus = BusBudget::new(budget.clone());
        assert_eq!(bus.headroom(), 8_192);
        let frame = bus.reserve(4_000, Lane::Class(Class::Publish)).unwrap();
        assert_eq!(bus.headroom(), 4_096);
        bus.release(frame);
        // Kept warm, the buffer is still charged, and still given to a grant that needs it.
        assert_eq!(budget.stats().used, 4_096);
        assert_eq!(bus.headroom(), 8_192);
        let whole = bus.reserve(8_192, Lane::Class(Class::Deliver)).unwrap();
        assert_eq!(bus.headroom(), 0);
        assert_eq!(
            bus.reserve(1, Lane::Class(Class::Control)).err(),
            Some(Refusal::Budget)
        );
        bus.release(whole);
        drop(bus);
        assert_eq!(budget.stats().used, 0);
    }

    /// A burst's buffers stay warm through a quieter stretch, so the next burst allocates nothing:
    /// bursts of three frames of the largest class, a stretch of one at a time between them. The
    /// working-set rule gave two back after the first quiet period, and every burst allocated them
    /// afresh.
    #[test]
    fn a_bursts_buffers_stay_warm_for_the_next_burst() {
        let top = largest_grant();
        let budget = MemoryBudget::new(64 << 20, 0).unwrap();
        let mut bus = BusBudget::new(budget.clone());
        for _ in 0..4 {
            let burst: Vec<_> = (0..3)
                .map(|_| bus.reserve(top, Lane::Class(Class::Publish)).unwrap())
                .collect();
            for frame in burst {
                bus.release(frame);
            }
            for _ in 0..8 {
                let frame = bus.reserve(top, Lane::Class(Class::Deliver)).unwrap();
                bus.release(frame);
            }
        }
        let stats = bus.stats();
        assert_eq!(
            stats.allocated, 3,
            "the first burst's three served every frame: {stats:?}"
        );
        assert_eq!(stats.released, 0, "{stats:?}");
        drop(bus);
        assert_eq!(budget.stats().used, 0);
    }

    /// A grant refused for want of budget sheds only the warm buffers it needs, the smallest
    /// classes first: the largest buffer, kept warm for the next publish, is not given up for a
    /// small frame's bytes.
    #[test]
    fn a_refused_grant_sheds_only_what_it_needs_the_smallest_first() {
        let top = usize::try_from(largest_grant()).unwrap();
        let budget = MemoryBudget::new(top + 4_096 + 2_048, 0).unwrap();
        let mut bus = BusBudget::new(budget.clone());
        let big = bus
            .reserve(top as u64, Lane::Class(Class::Publish))
            .unwrap();
        let small = bus.reserve(4_000, Lane::Class(Class::Control)).unwrap();
        let tiny = bus.reserve(1_500, Lane::Class(Class::Control)).unwrap();
        for frame in [big, small, tiny] {
            bus.release(frame);
        }
        // Every buffer warm, the budget full.
        assert_eq!(budget.stats().used, top + 4_096 + 2_048);
        assert_eq!(bus.headroom(), (top + 4_096 + 2_048) as u64);
        // A frame of a class with nothing warm: the 2,048-byte buffer alone gives it room.
        let other = bus.reserve(1_000, Lane::Class(Class::Deliver)).unwrap();
        assert_eq!(
            bus.stats().released,
            1,
            "one buffer shed: {:?}",
            bus.stats()
        );
        assert_eq!(budget.stats().used, top + 4_096 + 1_024);
        // The largest is still warm: taking it allocates nothing.
        let allocated = bus.stats().allocated;
        let again = bus
            .reserve(top as u64, Lane::Class(Class::Publish))
            .unwrap();
        assert_eq!(
            bus.stats().allocated,
            allocated,
            "the largest buffer was kept warm"
        );
        bus.release(other);
        bus.release(again);
        drop(bus);
        assert_eq!(budget.stats().used, 0);
    }
}
