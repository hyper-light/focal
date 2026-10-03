//! A peer's bulk frames, stepped in the order they left it (27 §12, the
//! audit's F42). Every frame to a peer is its own exchange on its own
//! stream, and the path completes the streams in any order: an append that
//! overtook the one before it was refused by the core and the member
//! probed — a round trip lost, for nothing lost. A sender counts its bulk
//! frames to each peer (`Operation::RaftOrdered`: an epoch, the sender's
//! incarnation, and a sequence within it), and a receiver keeps, per
//! source, the sequence it expects next and the frames that came ahead of
//! it, stepping each when its turn comes.
//!
//! What is held is bounded three ways. A frame is held no longer than its
//! own patience — the sender's round trip to this receiver as its pool
//! measured it when the frame left: a frame that has waited longer did not
//! overtake its predecessor on the path, the predecessor was lost, and the
//! leader that was told so is probing. No more are held for one source than
//! the source may have in flight: its lane. And no more sources have a lane
//! than a configuration may name. Past any bound what is held is stepped in
//! its order and the core judges it as it did, so a lost frame costs what it
//! cost and never holds the frames behind it longer than the path could have
//! reordered them. A newer epoch from a source resets its lane; what the
//! older epoch held is stepped first, in its order.
use std::collections::{BTreeMap, VecDeque};

/// Where a frame stands among its source's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// The frame is the one expected: step it, then what was held behind
    /// it ([`Resequencer::step_ready`]).
    Step,
    /// The frame is behind what was stepped already, or of an epoch that
    /// ended: step it at once; the core knows a stale message.
    Stale,
    /// The frame came ahead of one not yet seen: hold it
    /// ([`Resequencer::hold`]).
    Hold,
}
/// The resequencer has no lane for another source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity;

struct Held<T> {
    what: T,
    /// The owner's period past which the frame is stepped whether or not
    /// the one before it came.
    until: u64,
}
struct Lane<T> {
    epoch: u64,
    expected: u64,
    held: BTreeMap<u64, Held<T>>,
}
/// Per source, the sequence expected next and the frames held ahead of it.
pub struct Resequencer<T> {
    lanes: BTreeMap<u64, Lane<T>>,
    /// Frames to step now, in their order, before anything else of their
    /// source: what an ended epoch, a full lane or a passed patience let go.
    due: VecDeque<T>,
    lane_bound: usize,
    sources_bound: usize,
}
impl<T> Resequencer<T> {
    /// `lane_bound` frames held per source at most — what a source may have
    /// in flight — and `sources_bound` sources at most.
    pub fn new(lane_bound: usize, sources_bound: usize) -> Self {
        Self {
            lanes: BTreeMap::new(),
            due: VecDeque::new(),
            lane_bound: lane_bound.max(1),
            sources_bound: sources_bound.max(1),
        }
    }
    /// Where a frame of `sequence` in `epoch` from `source` stands. A newer
    /// epoch resets the source's lane and lets what the older held go
    /// ([`Self::take_due`], to be stepped before this frame); the first
    /// frame seen of a lane is the one expected.
    pub fn admit(&mut self, source: u64, epoch: u64, sequence: u64) -> Result<Admission, Capacity> {
        if !self.lanes.contains_key(&source) {
            if self.lanes.len() >= self.sources_bound {
                return Err(Capacity);
            }
            self.lanes.insert(
                source,
                Lane {
                    epoch,
                    expected: sequence,
                    held: BTreeMap::new(),
                },
            );
        }
        let Some(lane) = self.lanes.get_mut(&source) else {
            return Err(Capacity);
        };
        if epoch < lane.epoch {
            return Ok(Admission::Stale);
        }
        if epoch > lane.epoch {
            // The source started over: what its older incarnation sent and
            // this one holds goes first, in its order.
            let held = std::mem::take(&mut lane.held);
            if self.due.try_reserve(held.len()).is_err() {
                return Err(Capacity);
            }
            self.due.extend(held.into_values().map(|held| held.what));
            lane.epoch = epoch;
            lane.expected = sequence;
        }
        if sequence < lane.expected {
            return Ok(Admission::Stale);
        }
        if sequence == lane.expected {
            lane.expected = lane.expected.saturating_add(1);
            return Ok(Admission::Step);
        }
        Ok(Admission::Hold)
    }
    /// Hold `what`, a frame [`Self::admit`] answered `Hold` for, until the
    /// owner's period `until`. A lane that is full lets everything it held
    /// go instead ([`Self::take_due`]), the frame with it, in sequence: all
    /// of it is stepped in its order, and the core judges it as it did. A
    /// frame there is no room to hold or let go is given back.
    pub fn hold(&mut self, source: u64, sequence: u64, what: T, until: u64) -> Result<(), T> {
        let Some(lane) = self.lanes.get_mut(&source) else {
            return Err(what);
        };
        if lane.held.len() < self.lane_bound && !lane.held.contains_key(&sequence) {
            lane.held.insert(sequence, Held { what, until });
            return Ok(());
        }
        if self
            .due
            .try_reserve(lane.held.len().saturating_add(1))
            .is_err()
        {
            return Err(what);
        }
        let held = std::mem::take(&mut lane.held);
        let mut last = sequence;
        let mut what = Some(what);
        for (held_sequence, held) in held {
            if held_sequence > sequence
                && let Some(what) = what.take()
            {
                self.due.push_back(what);
            }
            last = last.max(held_sequence);
            self.due.push_back(held.what);
        }
        if let Some(what) = what.take() {
            self.due.push_back(what);
        }
        lane.expected = last.saturating_add(1);
        Ok(())
    }
    /// The next frame to step now, in its order, before anything else is
    /// admitted or stepped: what an ended epoch, a full lane or a passed
    /// patience let go.
    pub fn take_due(&mut self) -> Option<T> {
        self.due.pop_front()
    }
    /// The frame held behind the one just stepped from `source`, if it was
    /// held: its turn has come.
    pub fn step_ready(&mut self, source: u64) -> Option<T> {
        let lane = self.lanes.get_mut(&source)?;
        let held = lane.held.remove(&lane.expected)?;
        lane.expected = lane.expected.saturating_add(1);
        Some(held.what)
    }
    /// At the owner's period `now`: every frame held past its patience,
    /// and what was held consecutively behind it, in order ([`Self::take_due`]).
    /// The one before it was lost, and the frames are stepped as they came.
    pub fn expire(&mut self, now: u64) -> Result<(), Capacity> {
        for lane in self.lanes.values_mut() {
            loop {
                let Some((&oldest, held)) = lane.held.iter().next() else {
                    break;
                };
                if held.until > now && oldest != lane.expected {
                    break;
                }
                let Some(held) = lane.held.remove(&oldest) else {
                    break;
                };
                if self.due.try_reserve(1).is_err() {
                    lane.held.insert(oldest, held);
                    return Err(Capacity);
                }
                self.due.push_back(held.what);
                lane.expected = oldest.saturating_add(1);
            }
        }
        Ok(())
    }
    /// Drop the lanes of the sources `keep` does not name — members that
    /// left the configuration — and give back what they held, for the owner
    /// to answer as it answers a stranger.
    pub fn prune(
        &mut self,
        mut keep: impl FnMut(u64) -> bool,
        gone: &mut Vec<T>,
    ) -> Result<(), Capacity> {
        let departed: Vec<u64> = self
            .lanes
            .keys()
            .copied()
            .filter(|source| !keep(*source))
            .collect();
        for source in departed {
            if let Some(lane) = self.lanes.remove(&source) {
                if gone.try_reserve(lane.held.len()).is_err() {
                    self.lanes.insert(source, lane);
                    return Err(Capacity);
                }
                gone.extend(lane.held.into_values().map(|held| held.what));
            }
        }
        Ok(())
    }
    /// The frames held, over every source.
    pub fn held(&self) -> usize {
        self.lanes
            .values()
            .fold(0usize, |sum, lane| sum.saturating_add(lane.held.len()))
            .saturating_add(self.due.len())
    }
    /// The sources with a lane.
    pub fn sources(&self) -> usize {
        self.lanes.len()
    }
}

#[cfg(test)]
#[path = "resequence_tests.rs"]
mod tests;
