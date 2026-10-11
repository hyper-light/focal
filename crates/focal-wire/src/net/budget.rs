//! The transport's bytes, held against the node's memory budget (`focal_memory::MemoryBudget`), as
//! focal-wire held the request bodies it admitted (the audit's F03): every byte hyper-transport
//! reserves — a connection's receive window, a message's head, a body the owner reads, a frame — is
//! a `Pending` charge before it is allocated, given back when the reservation is. The endpoint's
//! socket is charged the most it holds (`hyper_rt::udp::Io::held_bound`: its receive buffers and its
//! outbox) when the endpoint is made, and given back when it is dropped.
//!
//! The control class (consensus, liveness, the fleet's own control) is funded from the completion
//! lane, which ordinary work cannot spend, so participants' bodies and content never starve what keeps
//! a group led and known; everything else is ordinary. A reservation carries its lane back as its
//! tag (`hyper_transport::Reservation::tagged`), so its bytes return where they were taken.

use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use hyper_transport::{Budget, Lane, Refusal, Reservation};

use crate::TrafficClass;

/// The tag of a reservation charged to the ordinary lane.
const ORDINARY: u8 = 1;
/// The tag of a reservation charged to the completion lane.
const COMPLETION: u8 = 2;

/// The bytes one endpoint holds, charged to the node's budget.
#[derive(Debug)]
pub(crate) struct NetBudget {
    budget: MemoryBudget,
    /// The endpoint's socket, charged its bound for the endpoint's life.
    socket: Allocation,
    /// What the endpoint holds on each lane, one allocation each: a grant is absorbed into it, a
    /// release shrinks it.
    ordinary: Option<Allocation>,
    completion: Option<Allocation>,
    /// Grants refused for want of budget.
    refused: u64,
}

/// What an endpoint holds of the budget, and what it was refused.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetBudgetStats {
    /// Bytes the endpoint's socket is charged, on the ordinary lane.
    pub socket: usize,
    /// Bytes held on the ordinary lane.
    pub ordinary: usize,
    /// Bytes held on the completion lane.
    pub completion: usize,
    /// Grants refused.
    pub refused: u64,
}

impl NetBudget {
    /// The budget of an endpoint whose socket holds at most `socket` bytes, which are charged now
    /// and held while the endpoint lives; refused when the budget cannot fund them.
    pub(crate) fn new(
        budget: MemoryBudget,
        socket: usize,
    ) -> Result<Self, focal_memory::MemoryError> {
        let socket = budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, socket)?
            .commit();
        Ok(Self {
            budget,
            socket,
            ordinary: None,
            completion: None,
            refused: 0,
        })
    }

    /// What the lane a class's bodies are funded from holds for the endpoint's peers in all, bytes:
    /// the budget's limit for the control class, which may spend the completion reserve, and the
    /// rest for the others, less the socket's charge, which is the endpoint's own and no peer's
    /// share of it.
    pub(crate) fn capacity(&self, class: TrafficClass) -> u64 {
        let stats = self.budget.stats();
        let lane = match class {
            TrafficClass::Control => stats.limit,
            TrafficClass::Exchange | TrafficClass::Bulk => {
                stats.limit.saturating_sub(stats.completion_reserve)
            }
        };
        u64::try_from(lane.saturating_sub(self.socket.bytes())).unwrap_or(u64::MAX)
    }

    pub(crate) fn stats(&self) -> NetBudgetStats {
        NetBudgetStats {
            socket: self.socket.bytes(),
            ordinary: self.ordinary.as_ref().map_or(0, Allocation::bytes),
            completion: self.completion.as_ref().map_or(0, Allocation::bytes),
            refused: self.refused,
        }
    }

    fn held(&mut self, tag: u8) -> &mut Option<Allocation> {
        if tag == COMPLETION {
            &mut self.completion
        } else {
            &mut self.ordinary
        }
    }

    fn refuse(&mut self) -> Refusal {
        self.refused = self.refused.saturating_add(1);
        Refusal::Budget
    }
}

impl Budget<TrafficClass> for NetBudget {
    fn reserve(&mut self, bytes: u64, lane: Lane<TrafficClass>) -> Result<Reservation, Refusal> {
        let (tag, budget_lane, buffered) = match lane {
            // Accounting only: QUIC buffers the window, the endpoint charges it.
            Lane::Window => (ORDINARY, BudgetLane::Ordinary, false),
            Lane::Class(TrafficClass::Control) => (COMPLETION, BudgetLane::Completion, true),
            Lane::Class(TrafficClass::Exchange | TrafficClass::Bulk) => {
                (ORDINARY, BudgetLane::Ordinary, true)
            }
        };
        let Ok(size) = usize::try_from(bytes) else {
            return Err(self.refuse());
        };
        // The buffer first, fallibly: a grant is never charged for memory that could not be had.
        let mut buffer = Vec::new();
        if buffered && buffer.try_reserve_exact(size).is_err() {
            return Err(self.refuse());
        }
        let Ok(reservation) = self.budget.reserve(BudgetKind::Pending, budget_lane, size) else {
            return Err(self.refuse());
        };
        let mut granted = reservation.commit();
        match self.held(tag) {
            Some(held) => {
                if held.absorb(&mut granted).is_err() {
                    // Allocations of one budget, kind and lane always absorb each other; `granted`
                    // gives its bytes back as it drops.
                    return Err(self.refuse());
                }
            }
            held @ None => *held = Some(granted),
        }
        Ok(Reservation::tagged(bytes, buffer, tag))
    }

    fn release(&mut self, reservation: Reservation) {
        let granted = usize::try_from(reservation.granted()).unwrap_or(usize::MAX);
        if granted == 0 {
            return;
        }
        if let Some(held) = self.held(reservation.tag()) {
            let left = held.bytes().saturating_sub(granted);
            // Shrinking never grows, so it cannot be refused.
            let _ = held.shrink_to(left);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use focal_memory::BudgetKind;

    fn budget(limit: usize, completion_reserve: usize) -> MemoryBudget {
        MemoryBudget::new(limit, completion_reserve).unwrap()
    }

    #[test]
    fn every_byte_is_charged_on_its_lane_and_given_back_there() {
        let memory = budget(1 << 20, 64 << 10);
        let mut net = NetBudget::new(memory.clone(), 0).unwrap();
        let control = net
            .reserve(1000, Lane::Class(TrafficClass::Control))
            .unwrap();
        let bulk = net.reserve(3000, Lane::Class(TrafficClass::Bulk)).unwrap();
        let window = net.reserve(5000, Lane::Window).unwrap();
        assert_eq!(
            (control.room(), bulk.room(), window.room()),
            (1000, 3000, 5000)
        );
        assert_eq!(
            net.stats(),
            NetBudgetStats {
                socket: 0,
                ordinary: 8000,
                completion: 1000,
                refused: 0
            }
        );
        assert_eq!(memory.stats().used_by(BudgetKind::Pending), 9000);
        net.release(control);
        net.release(window);
        assert_eq!((net.stats().completion, net.stats().ordinary), (0, 3000));
        net.release(bulk);
        assert_eq!(memory.stats().used_by(BudgetKind::Pending), 0);
        // An empty reservation, as hyper-transport makes for an empty head, gives back nothing.
        net.release(Reservation::default());
        assert_eq!(net.stats().ordinary, 0);
    }

    #[test]
    fn ordinary_bytes_never_take_the_completion_reserve_and_control_still_can() {
        let memory = budget(64 << 10, 16 << 10);
        let mut net = NetBudget::new(memory, 0).unwrap();
        let held = net
            .reserve(48 << 10, Lane::Class(TrafficClass::Exchange))
            .unwrap();
        assert_eq!(
            net.reserve(1, Lane::Class(TrafficClass::Bulk)).err(),
            Some(Refusal::Budget)
        );
        let control = net
            .reserve(16 << 10, Lane::Class(TrafficClass::Control))
            .unwrap();
        assert_eq!(net.stats().refused, 1);
        net.release(held);
        net.release(control);
        assert_eq!((net.stats().ordinary, net.stats().completion), (0, 0));
    }

    /// The socket is charged its bound when the endpoint is made, held while it lives and given back
    /// with it; a budget that cannot fund the socket refuses the endpoint.
    #[test]
    fn the_socket_is_charged_its_bound_for_the_endpoints_life() {
        let memory = budget(1 << 20, 64 << 10);
        let socket = hyper_rt::udp::Io { batch: 4 }.held_bound();
        let net = NetBudget::new(memory.clone(), socket).unwrap();
        assert_eq!(net.stats().socket, socket);
        // What the endpoint's peers share is the lane less the socket.
        let lane = (1usize << 20) - (64 << 10) - socket;
        assert_eq!(
            net.capacity(TrafficClass::Exchange),
            u64::try_from(lane).unwrap()
        );
        assert_eq!(
            net.capacity(TrafficClass::Control),
            u64::try_from((1usize << 20) - socket).unwrap()
        );
        assert_eq!(memory.stats().used_by(BudgetKind::Pending), socket);
        drop(net);
        assert_eq!(memory.stats().used_by(BudgetKind::Pending), 0);
        let small = budget(socket - 1, 0);
        assert!(NetBudget::new(small.clone(), socket).is_err());
        assert_eq!(small.stats().used, 0);
    }
}
