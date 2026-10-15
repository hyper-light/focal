//! Each connection's exchanges by when their wait is next judged: an indexed binary min-heap of
//! (due, id), so a progress pass judges the exchanges that are due without visiting the rest.
//!
//! A pass judged every exchange on its connection, though `judge` does nothing for one not yet due
//! (src/progress.rs `Carry::due`); beside 1,023 open exchanges that visit was most of what was left
//! of an exchange's cost once the sums and the visit set stopped folding over all of them
//! (hyper-transport `benches/lookup.rs`, docs/benchmarks.md, 2026-10-05). A due time moves whenever
//! a wait restarts or is judged, and an exchange may end long before it is due, so each entry's
//! index is kept where it can be found by the exchange's arena slot: moving or removing one costs
//! O(log n), and a walk for the due entries stops below any entry not yet due, so it costs the due
//! entries and those bordering them.
//!
//! The indices live in one array for every connection, indexed by arena slot: an exchange is on one
//! connection, its slot is below the exchange bound (the arena makes a new slot only while fewer
//! than the bound are taken), and the array is sized to the bound once.

use std::time::Instant;

/// The arena slot of exchange `id`: its low 32 bits (src/arena.rs, `SLOT_BITS`).
fn slot(id: u64) -> Option<usize> {
    usize::try_from(id & u64::from(u32::MAX)).ok()
}

fn position(positions: &[Option<usize>], id: u64) -> Option<usize> {
    slot(id).and_then(|slot| positions.get(slot).copied().flatten())
}

fn record(positions: &mut [Option<usize>], id: u64, at: Option<usize>) {
    if let Some(entry) = slot(id).and_then(|slot| positions.get_mut(slot)) {
        *entry = at;
    }
}

/// Sets exchange `id`'s entry in `heap` to `due`: moved to its place, added, or removed when it
/// has no due time.
pub(crate) fn set(
    heap: &mut Vec<(Instant, u64)>,
    positions: &mut [Option<usize>],
    id: u64,
    due: Option<Instant>,
) {
    match (position(positions, id), due) {
        (Some(at), Some(due)) => {
            if let Some(entry) = heap.get_mut(at) {
                entry.0 = due;
            }
            let at = sift_up(heap, positions, at);
            sift_down(heap, positions, at);
        }
        (Some(_), None) => remove(heap, positions, id),
        (None, Some(due)) => {
            let at = heap.len();
            heap.push((due, id));
            sift_up(heap, positions, at);
        }
        (None, None) => {}
    }
}

/// Removes exchange `id`'s entry from `heap`, if it has one.
pub(crate) fn remove(heap: &mut Vec<(Instant, u64)>, positions: &mut [Option<usize>], id: u64) {
    let Some(at) = position(positions, id) else {
        return;
    };
    record(positions, id, None);
    let last = heap.len().saturating_sub(1);
    if heap.is_empty() || at > last {
        return;
    }
    heap.swap(at, last);
    heap.pop();
    if at < heap.len() {
        let at = sift_up(heap, positions, at);
        sift_down(heap, positions, at);
    }
}

/// Appends to `into` the ids of every entry of `heap` due at `now`, in no particular order; `walk`
/// is the walk's frontier, at most the heap's length.
pub(crate) fn due(
    heap: &[(Instant, u64)],
    now: Instant,
    walk: &mut Vec<usize>,
    into: &mut Vec<u64>,
) {
    walk.clear();
    if heap.first().is_some_and(|(due, _)| *due <= now) {
        walk.push(0);
    }
    while let Some(at) = walk.pop() {
        let Some((_, id)) = heap.get(at) else {
            continue;
        };
        into.push(*id);
        let left = at.saturating_mul(2).saturating_add(1);
        for child in [left, left.saturating_add(1)] {
            if heap.get(child).is_some_and(|(due, _)| *due <= now) {
                walk.push(child);
            }
        }
    }
}

/// Records the index of the entry at `at`.
fn place(heap: &[(Instant, u64)], positions: &mut [Option<usize>], at: usize) {
    if let Some((_, id)) = heap.get(at) {
        record(positions, *id, Some(at));
    }
}

/// Moves the entry at `at` toward the root while it is earlier than its parent; returns where it
/// ends.
fn sift_up(heap: &mut [(Instant, u64)], positions: &mut [Option<usize>], mut at: usize) -> usize {
    place(heap, positions, at);
    while at > 0 {
        let parent = at.saturating_sub(1) / 2;
        if heap.get(at) >= heap.get(parent) {
            break;
        }
        heap.swap(at, parent);
        place(heap, positions, at);
        place(heap, positions, parent);
        at = parent;
    }
    at
}

/// Moves the entry at `at` toward the leaves while a child is earlier.
fn sift_down(heap: &mut [(Instant, u64)], positions: &mut [Option<usize>], mut at: usize) {
    loop {
        let left = at.saturating_mul(2).saturating_add(1);
        let mut least = at;
        for child in [left, left.saturating_add(1)] {
            if child < heap.len() && heap.get(child) < heap.get(least) {
                least = child;
            }
        }
        if least == at {
            return;
        }
        heap.swap(at, least);
        place(heap, positions, at);
        place(heap, positions, least);
        at = least;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    /// Exchange ids over 16 slots, with generations, as the arena makes them.
    const SLOTS: u64 = 16;

    /// One step of a history, from a generated (kind, id, time): kind 0 sets the id's due time
    /// to the time, kind 1 clears it, kind 2 ends the exchange early, kind 3 is a pass at the
    /// time. Times are milliseconds from the start.
    #[derive(Clone, Copy, Debug)]
    enum Step {
        Set(u64, Option<u64>),
        Remove(u64),
        Due(u64),
    }

    fn step((kind, id, time): (u8, u64, u64)) -> Step {
        match kind {
            0 => Step::Set(id, Some(time)),
            1 => Step::Set(id, None),
            2 => Step::Remove(id),
            _ => Step::Due(time),
        }
    }

    /// The heap order holds, and every entry's recorded index is where it is.
    fn sound(heap: &[(Instant, u64)], positions: &[Option<usize>]) -> bool {
        heap.iter().enumerate().all(|(at, entry)| {
            let parent = at.saturating_sub(1) / 2;
            (at == 0 || heap.get(parent).is_some_and(|above| above <= entry))
                && position(positions, entry.1) == Some(at)
        })
    }

    proptest! {
        /// The heap against a map from id to due time (the model): after every step of a
        /// generated history of due times set, moved earlier and later, cleared, and exchanges
        /// ended early, the ids due at any time are the model's, the heap order holds, and every
        /// index is where its entry is.
        #[test]
        fn the_heap_finds_what_is_due_as_a_map_does(steps in proptest::collection::vec((0u8..4, 0..SLOTS, 0u64..64), 1..200)) {
            let start = hyper_sim::Anchor::new().instant(0).unwrap();
            let at = |millis: u64| start + Duration::from_millis(millis);
            let mut heap = Vec::new();
            let mut positions = vec![None; usize::try_from(SLOTS).unwrap()];
            let mut walk = Vec::new();
            let mut model: BTreeMap<u64, Instant> = BTreeMap::new();
            for generated in steps {
                match step(generated) {
                    Step::Set(id, due) => {
                        set(&mut heap, &mut positions, id, due.map(at));
                        match due {
                            Some(due) => model.insert(id, at(due)),
                            None => model.remove(&id),
                        };
                    }
                    Step::Remove(id) => {
                        remove(&mut heap, &mut positions, id);
                        model.remove(&id);
                    }
                    Step::Due(now) => {
                        let mut found = Vec::new();
                        due(&heap, at(now), &mut walk, &mut found);
                        found.sort_unstable();
                        let expected: Vec<u64> = model
                            .iter()
                            .filter(|(_, due)| **due <= at(now))
                            .map(|(id, _)| *id)
                            .collect();
                        prop_assert_eq!(found, expected);
                    }
                }
                prop_assert!(sound(&heap, &positions), "heap {:?} positions {:?}", heap, positions);
                prop_assert_eq!(heap.len(), model.len());
            }
        }
    }
}
