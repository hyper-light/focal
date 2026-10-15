//! A work queue's window: the claims delivered and not yet acknowledged, between the floor below
//! which every claim is acknowledged and the next claim not yet delivered.
//!
//! Kafka converged on the same shape for per-record acknowledgement (KIP-932, share groups: the
//! in-flight records between the share-partition start and end offsets, each with its state,
//! delivery count and acquisition lock, the window bounded). Redis keeps a radix-tree entry for each
//! pending message of a consumer group and JetStream a map entry; here an offset in the window costs
//! one byte (acknowledged, and the times it was delivered) and four (when its lease ends, in
//! milliseconds from the window's start), and an acknowledged prefix leaves the window as it forms.
//!
//! The window is bounded: a full window delivers nothing new until acknowledgements free its head,
//! which holds delivery and never refuses a publish. A claim delivered `max_deliveries` times whose
//! lease ends again is given up as dead, for its owner to send to the stream's dead letters.

use std::collections::VecDeque;

use crate::BusError;

const ACKED: u8 = 0x80;
const DELIVERIES: u8 = 0x7F;

/// What [`AckWindow::take`] gives out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Take {
    /// A claim not delivered before.
    Fresh(u64),
    /// A claim whose lease ended unacknowledged, delivered again: its `deliveries`th delivery.
    Again {
        /// The claim's offset.
        offset: u64,
        /// The times it has now been delivered.
        deliveries: u8,
    },
    /// A claim delivered the most times allowed, its last lease ended: dead.
    Dead(u64),
}

/// What an acknowledgement did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ack {
    /// Acknowledged; the floor is now `floor`.
    Acked {
        /// Every offset below it is acknowledged.
        floor: u64,
    },
    /// Acknowledged before (or given up as dead): nothing changed.
    Already,
    /// Not delivered yet: nothing changed.
    Undelivered,
}

/// A consumer group's window over one stream.
#[derive(Debug)]
pub struct AckWindow {
    floor: u64,
    states: VecDeque<u8>,
    leases: VecDeque<u32>,
    capacity: usize,
    max_deliveries: u8,
    epoch_ns: u64,
    /// The earliest lease end among claims delivered and not acknowledged; `u32::MAX` for none.
    next_expiry_ms: u32,
}

impl AckWindow {
    /// A window from `floor` holding at most `capacity` claims in flight, each delivered at most
    /// `max_deliveries` times (1 to 127), its leases timed from `epoch_ns`.
    pub fn new(
        floor: u64,
        capacity: usize,
        max_deliveries: u8,
        epoch_ns: u64,
    ) -> Result<Self, BusError> {
        if capacity == 0 || max_deliveries == 0 || max_deliveries > DELIVERIES {
            return Err(BusError::Configuration(
                "a window holds a claim and delivers it 1 to 127 times",
            ));
        }
        let mut states = VecDeque::new();
        let mut leases = VecDeque::new();
        states
            .try_reserve_exact(capacity)
            .map_err(|_| BusError::Memory(focal_memory::MemoryError::AllocationFailed))?;
        leases
            .try_reserve_exact(capacity)
            .map_err(|_| BusError::Memory(focal_memory::MemoryError::AllocationFailed))?;
        Ok(Self {
            floor,
            states,
            leases,
            capacity,
            max_deliveries,
            epoch_ns,
            next_expiry_ms: u32::MAX,
        })
    }

    /// Every offset below it is acknowledged.
    pub const fn floor(&self) -> u64 {
        self.floor
    }

    /// Claims delivered and not yet acknowledged, or acknowledged behind one that is not.
    pub fn in_flight(&self) -> usize {
        self.states.len()
    }

    /// The next offset the window has not delivered.
    pub fn next(&self) -> u64 {
        self.floor
            .saturating_add(u64::try_from(self.states.len()).unwrap_or(u64::MAX))
    }

    fn millis(&self, now_ns: u64) -> u32 {
        u32::try_from(now_ns.saturating_sub(self.epoch_ns) / 1_000_000).unwrap_or(u32::MAX)
    }

    /// The claim to deliver at `now_ns` with a lease of `lease_ns`: one whose lease ended first,
    /// then the next of the stream below `end` while the window has room; nothing when neither.
    pub fn take(&mut self, now_ns: u64, lease_ns: u64, end: u64) -> Option<Take> {
        let now = self.millis(now_ns);
        let until = now.saturating_add(u32::try_from(lease_ns / 1_000_000).unwrap_or(u32::MAX));
        if now >= self.next_expiry_ms
            && let Some(taken) = self.expired(now, until)
        {
            return Some(taken);
        }
        if self.states.len() >= self.capacity {
            return None;
        }
        let offset = self.next();
        if offset >= end {
            return None;
        }
        self.states.push_back(1);
        self.leases.push_back(until);
        self.next_expiry_ms = self.next_expiry_ms.min(until);
        Some(Take::Fresh(offset))
    }

    /// The oldest claim whose lease ended by `now`, delivered again or given up as dead; the
    /// earliest lease end recomputed over the window.
    fn expired(&mut self, now: u32, until: u32) -> Option<Take> {
        let mut taken = None;
        let mut earliest = u32::MAX;
        let mut offset = self.floor;
        for (state, lease) in self.states.iter_mut().zip(self.leases.iter_mut()) {
            let pending = *state & ACKED == 0;
            if pending && taken.is_none() && *lease <= now {
                let deliveries = *state & DELIVERIES;
                if deliveries >= self.max_deliveries {
                    *state |= ACKED;
                    taken = Some(Take::Dead(offset));
                } else {
                    let deliveries = deliveries.saturating_add(1);
                    *state = deliveries;
                    *lease = until;
                    taken = Some(Take::Again { offset, deliveries });
                }
            }
            if *state & ACKED == 0 {
                earliest = earliest.min(*lease);
            }
            offset = offset.saturating_add(1);
        }
        self.next_expiry_ms = earliest;
        if matches!(taken, Some(Take::Dead(_))) {
            self.advance();
        }
        taken
    }

    /// Acknowledges `offset`.
    pub fn ack(&mut self, offset: u64) -> Ack {
        let Some(at) = offset
            .checked_sub(self.floor)
            .and_then(|at| usize::try_from(at).ok())
        else {
            return Ack::Already;
        };
        let Some(state) = self.states.get_mut(at) else {
            return Ack::Undelivered;
        };
        if *state & ACKED != 0 {
            return Ack::Already;
        }
        *state |= ACKED;
        self.advance();
        Ack::Acked { floor: self.floor }
    }

    /// Moves the floor past the acknowledged prefix.
    fn advance(&mut self) {
        while self.states.front().is_some_and(|state| state & ACKED != 0) {
            self.states.pop_front();
            self.leases.pop_front();
            self.floor = self.floor.saturating_add(1);
        }
        if self.states.is_empty() {
            self.next_expiry_ms = u32::MAX;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    #[test]
    fn claims_are_delivered_in_order_until_the_window_is_full() {
        let mut window = AckWindow::new(10, 3, 5, 0).unwrap();
        assert_eq!(window.take(0, 1_000 * MS, 100), Some(Take::Fresh(10)));
        assert_eq!(window.take(0, 1_000 * MS, 100), Some(Take::Fresh(11)));
        assert_eq!(window.take(0, 1_000 * MS, 100), Some(Take::Fresh(12)));
        assert_eq!(window.take(0, 1_000 * MS, 100), None);
        // Acknowledging out of order moves the floor only past the acknowledged prefix.
        assert_eq!(window.ack(11), Ack::Acked { floor: 10 });
        assert_eq!(window.ack(11), Ack::Already);
        assert_eq!(window.ack(10), Ack::Acked { floor: 12 });
        assert_eq!(window.ack(9), Ack::Already);
        assert_eq!(window.ack(13), Ack::Undelivered);
        assert_eq!(window.take(0, 1_000 * MS, 14), Some(Take::Fresh(13)));
        assert_eq!(
            window.take(0, 1_000 * MS, 14),
            None,
            "the stream ends at 14"
        );
    }

    #[test]
    fn an_ended_lease_delivers_again_until_the_claim_is_dead() {
        let mut window = AckWindow::new(0, 4, 2, 0).unwrap();
        assert_eq!(window.take(0, 10 * MS, 2), Some(Take::Fresh(0)));
        assert_eq!(window.take(MS, 10 * MS, 2), Some(Take::Fresh(1)));
        // Before the lease ends nothing is delivered again.
        assert_eq!(window.take(9 * MS, 10 * MS, 2), None);
        assert_eq!(
            window.take(10 * MS, 10 * MS, 2),
            Some(Take::Again {
                offset: 0,
                deliveries: 2
            })
        );
        assert_eq!(
            window.take(11 * MS, 10 * MS, 2),
            Some(Take::Again {
                offset: 1,
                deliveries: 2
            })
        );
        window.ack(1);
        // Offset 0's second lease ends: delivered twice already, it is dead and leaves the window.
        assert_eq!(window.take(20 * MS, 10 * MS, 2), Some(Take::Dead(0)));
        assert_eq!(window.floor(), 2);
        assert_eq!(window.in_flight(), 0);
    }
}
