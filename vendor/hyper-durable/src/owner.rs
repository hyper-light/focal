//! The replicas one owner thread runs (`docs/durable.md` §7): an arena of them by generational
//! handle, and their turns by deficit round robin with a quantum of one `Ready`.
//!
//! No thread per group, no shared ownership, no lock. The node runs a fixed set of owner threads
//! (mantle's shards, focal's session owners); this crate spawns none (`CLAUDE.md` §1, sans-io),
//! and each owner thread holds one [`Owner`]. The owner's embedder gives it one waker for each
//! slot of its arena, made once: a waker that tells the owner's thread which slot's write was
//! answered. The thread calls [`Owner::woken`] with that slot, steps what arrives into its
//! replicas ([`Owner::get_mut`], then [`Owner::schedule`]), and takes a [`Owner::turn`]. Nothing
//! polls on a timer: a replica is driven when something was stepped into it or the log woke it,
//! so one flush serves every group the owner submitted before it (the commit group,
//! `docs/research/durable.md` §2).
//!
//! A turn drives each replica queued once, for at most one `Ready` (the replica's own bound,
//! [`Replica::drive`]); one with more to do goes to the back of the queue. Every quantum is at
//! least the largest unit of work, one `Ready`, so the round robin is fair within one unit
//! (Shreedhar and Varghese, "Efficient Fair Queuing Using Deficit Round Robin", SIGCOMM 1995,
//! Theorem 4.5: "if for all i, Quantum_i ≥ Max"), and a turn's work is O(1) a replica.
//!
//! **Liveness** (timing steps L-2 and L-3, `docs/timing.md` §2.8–§2.9). The node keeps one
//! `hyper_liveness::Liveness`, a stream to each node it shares a group with, on the thread that
//! owns its plane socket or on an owner's; the groups send no liveness of their own. The owner
//! keeps its replicas' pairs attached ([`Owner::pairs`]), takes each change the stream reports to
//! every replica that has a member on that node ([`Owner::believe`]; a member's id is its node's,
//! as in focal and the E2E), derives each group's timing from what the stream measured and charges
//! each pair its groups' expected election ([`Owner::measure`]), and feeds the stream each
//! replica's durable writes from [`Driven::flushed`]. A change reaches the replicas of that node
//! only: the node's standing changed, which is rare, and an idle group is touched by nothing else.
//!
//! **Ticks** (`docs/durable.md` §8). A replica that elects on ticks takes none of this: its owner
//! gives it a tick once a period ([`Replica::tick`]), and the stream's words and timing are
//! refused by it, which [`Owner::believe`] and [`Owner::measure`] pass over.
use std::collections::VecDeque;
use std::task::Waker;
use std::time::Duration;

use hyper_liveness::{Change, Liveness, PeerId};
use hyper_timing::Trust;

use crate::budget::Budget;
use crate::machine::StateMachine;
use crate::replica::{Driven, Output, Replica, ReplicaError};
use crate::store::LogStore;

/// A replica's place in its owner's arena. A handle to a slot since emptied, or filled again,
/// finds nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Handle {
    slot: u32,
    generation: u32,
}

impl Handle {
    /// The slot: the one the owner's waker for it names.
    pub fn slot(&self) -> usize {
        usize::try_from(self.slot).unwrap_or(usize::MAX)
    }
}

struct Slot<R> {
    generation: u32,
    replica: Option<R>,
    queued: bool,
    /// The peers the node's liveness stream was told this replica shares a group with.
    attached: Vec<PeerId>,
}

/// One owner thread's replicas.
pub struct Owner<L: LogStore, M: StateMachine, B: Budget> {
    slots: Vec<Slot<Replica<L, M, B>>>,
    free: Vec<u32>,
    wakers: Vec<Waker>,
    /// Slots to drive, in turn: each at most once.
    queue: VecDeque<u32>,
}

/// The arena is full: the replica is given back, boxed, as it is large and the path rare.
#[derive(Debug, thiserror::Error)]
#[error("the owner holds as many replicas as it has slots")]
pub struct Full<R>(pub Box<R>);

impl<L: LogStore, M: StateMachine, B: Budget> Owner<L, M, B> {
    /// An owner with one slot for each waker: the waker of slot `i` tells the owner's thread
    /// that slot `i` was woken. The arena and its queue are reserved here and never grow.
    pub fn new(wakers: Vec<Waker>) -> Self {
        let slots = wakers
            .iter()
            .map(|_| Slot {
                generation: 0,
                replica: None,
                queued: false,
                attached: Vec::new(),
            })
            .collect();
        let count = u32::try_from(wakers.len()).unwrap_or(u32::MAX);
        Self {
            slots,
            free: (0..count).rev().collect(),
            queue: VecDeque::with_capacity(wakers.len()),
            wakers,
        }
    }

    /// The replicas the owner holds at most.
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// The replicas it holds.
    pub fn len(&self) -> usize {
        self.slots.len().saturating_sub(self.free.len())
    }

    /// Whether it holds none.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Takes `replica` into a free slot, queued for its first drive.
    pub fn insert(&mut self, replica: Replica<L, M, B>) -> Result<Handle, Full<Replica<L, M, B>>> {
        let Some(slot) = self.free.pop() else {
            return Err(Full(Box::new(replica)));
        };
        let Some(entry) = usize::try_from(slot)
            .ok()
            .and_then(|at| self.slots.get_mut(at))
        else {
            return Err(Full(Box::new(replica)));
        };
        entry.replica = Some(replica);
        let handle = Handle {
            slot,
            generation: entry.generation,
        };
        self.schedule(handle);
        Ok(handle)
    }

    /// Gives back the replica at `handle`; its slot's next replica gets a new generation.
    pub fn remove(&mut self, handle: Handle) -> Option<Replica<L, M, B>> {
        let entry = self.entry_mut(handle)?;
        let replica = entry.replica.take()?;
        entry.generation = entry.generation.wrapping_add(1);
        entry.attached.clear();
        self.free.push(handle.slot);
        Some(replica)
    }

    fn entry_mut(&mut self, handle: Handle) -> Option<&mut Slot<Replica<L, M, B>>> {
        self.slots
            .get_mut(handle.slot())
            .filter(|entry| entry.generation == handle.generation && entry.replica.is_some())
    }

    /// The replica at `handle`.
    pub fn get(&self, handle: Handle) -> Option<&Replica<L, M, B>> {
        self.slots
            .get(handle.slot())
            .filter(|entry| entry.generation == handle.generation)
            .and_then(|entry| entry.replica.as_ref())
    }

    /// The replica at `handle`, to step something into; [`Owner::schedule`] it after.
    pub fn get_mut(&mut self, handle: Handle) -> Option<&mut Replica<L, M, B>> {
        self.entry_mut(handle)
            .and_then(|entry| entry.replica.as_mut())
    }

    /// Queues the replica at `handle` for the next turn.
    pub fn schedule(&mut self, handle: Handle) {
        if self.entry_mut(handle).is_some() {
            self.queue_slot(handle.slot);
        }
    }

    /// The waker of `slot` was woken: its replica is queued for the next turn. A slot since
    /// emptied queues nothing.
    pub fn woken(&mut self, slot: usize) {
        if let Ok(slot) = u32::try_from(slot) {
            self.queue_slot(slot);
        }
    }

    fn queue_slot(&mut self, slot: u32) {
        let Some(entry) = usize::try_from(slot)
            .ok()
            .and_then(|at| self.slots.get_mut(at))
        else {
            return;
        };
        if entry.replica.is_some() && !entry.queued {
            entry.queued = true;
            // The queue holds each slot once, and was reserved for every slot.
            self.queue.push_back(slot);
        }
    }

    /// Whether a replica waits for its turn.
    pub fn has_work(&self) -> bool {
        !self.queue.is_empty()
    }

    /// Keeps the node's liveness stream told which peers the replica at `handle` shares a group
    /// with: its configuration's other members, attached as they join and detached as they
    /// leave; all of them detached once the replica is gone (`hyper_liveness::Liveness::attach`
    /// counts the groups a pair shares). A peer it is given is told to it as the stream believes
    /// it then (`Replica::suspect`, `Replica::trust`): no change the stream reported while it
    /// was not attached reached it. Called when the replica is inserted, after a drive that may
    /// have changed its configuration, and before it is removed with `gone`. At most the members
    /// its configuration may name a replica (`hyper_raft::Limits::members`).
    pub fn pairs(
        &mut self,
        handle: Handle,
        liveness: &mut Liveness,
        gone: bool,
    ) -> Result<(), hyper_liveness::Refusal> {
        let Some(entry) = self
            .slots
            .get_mut(handle.slot())
            .filter(|entry| entry.generation == handle.generation)
        else {
            return Ok(());
        };
        let mut now: Vec<PeerId> = match (&entry.replica, gone) {
            (Some(replica), false) => replica.peers().collect(),
            _ => Vec::new(),
        };
        now.sort_unstable();
        now.dedup();
        for peer in &now {
            if entry.attached.binary_search(peer).is_err() {
                liveness.attach(*peer)?;
                // What the stream believes of it now: a change it reported while the replica
                // shared no group with the peer reached only the replicas that did (`believe`),
                // and a pair begun anew reports nothing while it trusts. A refusal fences the
                // replica, as there.
                if let Some(replica) = entry.replica.as_mut() {
                    let _ = if liveness.trust(*peer) == Some(Trust::Suspected) {
                        replica.suspect(*peer)
                    } else {
                        replica.trust(*peer)
                    };
                }
            }
        }
        for peer in &entry.attached {
            if now.binary_search(peer).is_err() {
                liveness.detach(*peer)?;
            }
        }
        entry.attached = now;
        Ok(())
    }

    /// A change the node's liveness stream reported, taken to every replica with a member on that
    /// node, each queued for its next turn: a suspicion, trust again, or the node's restart, a new
    /// incarnation the core trusts and holds to lead nothing it led (`Replica::restarted`). A
    /// replica that refuses it is fenced and reopened by its owner, which tells the new one what
    /// the stream believes ([`Replica::believe_all`](crate::Replica::believe_all)).
    pub fn believe(&mut self, change: &Change) {
        let peer = change.peer();
        for at in 0..self.slots.len() {
            let Some(entry) = self.slots.get_mut(at) else {
                continue;
            };
            let Some(replica) = entry.replica.as_mut() else {
                continue;
            };
            if entry.attached.binary_search(&peer).is_err() {
                continue;
            }
            let _ = match change {
                Change::Suspected(_) => replica.suspect(peer),
                Change::Trusted { .. } => replica.trust(peer),
                Change::Restarted { .. } => replica.restarted(peer),
            };
            if let Ok(slot) = u32::try_from(at) {
                self.queue_slot(slot);
            }
        }
    }

    /// Each group's timing from what the node's liveness stream measured
    /// ([`Replica::measure`](crate::Replica::measure)), and each pair charged the mean expected
    /// election `T_E` of the groups it shares (`Liveness::set_election`: the mean minimizes their
    /// summed unavailability, which is linear in `T_E`). A mistake about any member can cost its
    /// group an election, at most: a follower that suspects its leader campaigns, and a leader
    /// that suspects too many of its followers steps down. Every pair is charged, not only the
    /// leaders', so every pair's detector configures (`docs/timing.md` §2.9: a pair never
    /// charged is never configured). Called when the stream's estimates move: a pair configured
    /// again, a granularity or a flush mean that changed.
    pub fn measure(&mut self, liveness: &mut Liveness) {
        // Each peer's groups' elections: at most one entry a peer the node keeps.
        let mut charged: Vec<(PeerId, Duration, u32)> = Vec::new();
        for entry in &mut self.slots {
            let Some(replica) = entry.replica.as_mut() else {
                continue;
            };
            let Ok(Some(span)) = replica.measure(liveness) else {
                continue;
            };
            for peer in &entry.attached {
                match charged.iter_mut().find(|(at, ..)| at == peer) {
                    Some((_, sum, count)) => {
                        *sum = sum.saturating_add(span.election);
                        *count = count.saturating_add(1);
                    }
                    None => charged.push((*peer, span.election, 1)),
                }
            }
        }
        for (peer, sum, count) in charged {
            let _ = liveness.set_election(peer, sum.checked_div(count).unwrap_or(sum));
        }
    }

    /// Drives each queued replica once, for at most one `Ready`, handing its output to `each`
    /// with its outcome; one with more to do is queued again behind the others. `out` is the
    /// owner's buffer, emptied before each drive. Returns the replicas driven.
    pub fn turn(
        &mut self,
        now: u64,
        out: &mut Output<M::Answer>,
        mut each: impl FnMut(Handle, Result<Driven, ReplicaError>, &mut Output<M::Answer>),
    ) -> usize {
        let queued = self.queue.len();
        let mut driven = 0usize;
        for _ in 0..queued {
            let Some(slot) = self.queue.pop_front() else {
                break;
            };
            let at = usize::try_from(slot).unwrap_or(usize::MAX);
            let (Some(entry), Some(waker)) = (self.slots.get_mut(at), self.wakers.get(at)) else {
                continue;
            };
            entry.queued = false;
            let Some(replica) = entry.replica.as_mut() else {
                continue;
            };
            out.clear();
            let outcome = replica.drive(now, waker, out);
            let more = outcome.as_ref().is_ok_and(|d| d.more);
            let handle = Handle {
                slot,
                generation: entry.generation,
            };
            if more {
                entry.queued = true;
                self.queue.push_back(slot);
            }
            driven = driven.saturating_add(1);
            each(handle, outcome, out);
        }
        driven
    }
}
