//! Membership updates ride the probes: a bounded buffer of the newest
//! verdict per node, each rebroadcast at most λ·log(n+1) times, the least
//! disseminated first.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAX_UPDATES: usize = 64;
pub const MAX_PIGGYBACK: usize = 8;
const LAMBDA: f64 = 3.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberStatus {
    Alive,
    Suspect,
    Dead,
}
impl MemberStatus {
    fn precedence(self) -> u8 {
        match self {
            Self::Alive => 0,
            Self::Suspect => 1,
            Self::Dead => 2,
        }
    }
}
/// One verdict about one node at one incarnation, with who said it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivenessUpdate {
    pub node: u64,
    pub generation: u64,
    pub incarnation: u64,
    pub status: MemberStatus,
    pub origin: u64,
}
impl LivenessUpdate {
    /// Whether `self` supersedes `other` about the same node: a later
    /// incarnation always, the same incarnation only with a stronger status.
    pub fn supersedes(&self, other: &Self) -> bool {
        self.node == other.node
            && (self.generation > other.generation
                || (self.generation == other.generation
                    && (self.incarnation > other.incarnation
                        || (self.incarnation == other.incarnation
                            && self.status.precedence() > other.status.precedence()))))
    }
}
#[derive(Debug, Clone)]
struct Entry {
    update: LivenessUpdate,
    broadcasts: u32,
    max_broadcasts: u32,
}
#[derive(Debug, Default)]
pub struct GossipBuffer {
    entries: BTreeMap<u64, Entry>,
}
impl GossipBuffer {
    fn max_broadcasts(members: usize) -> u32 {
        let n = u32::try_from(members).unwrap_or(u32::MAX);
        let value = (LAMBDA * (f64::from(n) + 1.0).ln()).ceil();
        if value.is_finite() && value >= 1.0 {
            if value >= f64::from(u32::MAX) {
                u32::MAX
            } else {
                // The ceiling of a finite positive value within u32 range.
                value as u32
            }
        } else {
            1
        }
    }
    /// Record an update to disseminate. An older or weaker verdict about a
    /// node the buffer already carries is dropped; a superseding one starts
    /// its own broadcasts. Returns whether the buffer changed.
    pub fn add(&mut self, update: LivenessUpdate, members: usize) -> bool {
        if let Some(existing) = self.entries.get(&update.node)
            && !update.supersedes(&existing.update)
        {
            return false;
        }
        if !self.entries.contains_key(&update.node) && self.entries.len() >= MAX_UPDATES {
            // Evict the most disseminated entry to make room.
            let evict = self
                .entries
                .iter()
                .max_by_key(|(_, entry)| entry.broadcasts)
                .map(|(node, _)| *node);
            match evict {
                Some(node) => {
                    self.entries.remove(&node);
                }
                None => return false,
            }
        }
        self.entries.insert(
            update.node,
            Entry {
                update,
                broadcasts: 0,
                max_broadcasts: Self::max_broadcasts(members),
            },
        );
        true
    }
    /// The newest verdict the buffer carries about a node.
    pub fn latest(&self, node: u64) -> Option<LivenessUpdate> {
        self.entries.get(&node).map(|entry| entry.update)
    }
    /// Up to `MAX_PIGGYBACK` updates for the next message, the least
    /// broadcast first, each counted as broadcast once more; an update that
    /// reached its count leaves the buffer.
    pub fn piggyback(&mut self) -> Vec<LivenessUpdate> {
        // Charge the working set, then select the MAX_PIGGYBACK least-broadcast
        // updates with a partial select (O(n)) rather than a full sort (O(n log
        // n)); the few selected are then ordered least-broadcast first, so the
        // result is identical to a full sort followed by take(MAX_PIGGYBACK).
        let mut order: Vec<(u32, u64)> = Vec::new();
        if order.try_reserve_exact(self.entries.len()).is_err() {
            return Vec::new();
        }
        order.extend(
            self.entries
                .iter()
                .filter(|(_, entry)| entry.broadcasts < entry.max_broadcasts)
                .map(|(node, entry)| (entry.broadcasts, *node)),
        );
        if order.len() > MAX_PIGGYBACK {
            order.select_nth_unstable(MAX_PIGGYBACK);
            order.truncate(MAX_PIGGYBACK);
        }
        order.sort_unstable();
        let mut selected = Vec::new();
        if selected.try_reserve_exact(order.len()).is_err() {
            return selected;
        }
        for (_, node) in order.into_iter().take(MAX_PIGGYBACK) {
            if let Some(entry) = self.entries.get_mut(&node) {
                selected.push(entry.update);
                entry.broadcasts = entry.broadcasts.saturating_add(1);
                if entry.broadcasts >= entry.max_broadcasts {
                    self.entries.remove(&node);
                }
            }
        }
        selected
    }
    pub fn remove(&mut self, node: u64) {
        self.entries.remove(&node);
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
