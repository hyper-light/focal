//! Each node's one timer, as a sans-io crate states it (`wake()`, `poll_timeout()`): an indexed
//! binary min-heap of the nodes by `(time, node)`, so re-arming is `O(log n)` in place and holds
//! exactly one entry a node — no stale entries to skip, and the bound is the node count. A node
//! once armed keeps its entry: disarmed, its time is [`DISARMED`], so the fire-and-re-arm a timer
//! does on every period moves one entry down and up rather than out and back in.

/// An armed node: when its timer fires, in virtual time, and which node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Armed {
    pub(crate) at: u64,
    pub(crate) node: u32,
}

/// The time of a disarmed node's entry: after every time a timer can be armed at, which
/// [`crate::World::wake`] keeps below it.
pub(crate) const DISARMED: u64 = u64::MAX;

#[derive(Clone, Debug, Default)]
pub(crate) struct Wakes {
    heap: Vec<Armed>,
    /// Each node's place in `heap`, if armed.
    place: Vec<Option<usize>>,
}

impl Wakes {
    /// Room for node `node`.
    pub(crate) fn add_node(&mut self) {
        self.place.push(None);
    }

    pub(crate) fn min(&self) -> Option<u64> {
        self.heap
            .first()
            .map(|armed| armed.at)
            .filter(|at| *at != DISARMED)
    }

    /// The armed nodes, in heap order.
    pub(crate) fn armed(&self) -> impl Iterator<Item = &Armed> {
        self.heap.iter().filter(|armed| armed.at != DISARMED)
    }

    /// The node firing at `at`, if it is the only one: the step without a tie.
    pub(crate) fn alone_at(&self, at: u64) -> Option<Armed> {
        let root = self.heap.first().filter(|root| root.at == at)?;
        let tied = children(0).any(|child| self.heap.get(child).is_some_and(|c| c.at == at));
        (!tied).then_some(*root)
    }

    /// Node `node` armed at `at`, or disarmed.
    pub(crate) fn set(&mut self, node: u32, at: Option<u64>) {
        let Some(index) = usize::try_from(node).ok() else {
            return;
        };
        let current = self.place.get(index).copied().flatten();
        let at = at.unwrap_or(DISARMED);
        match current {
            None if at == DISARMED => {}
            None => {
                let place = self.heap.len();
                self.heap.push(Armed { at, node });
                self.put(node, place);
                self.up(place);
            }
            Some(place) => {
                if let Some(armed) = self.heap.get_mut(place) {
                    armed.at = at;
                }
                let place = self.up(place);
                self.down(place);
            }
        }
    }

    /// Every armed node firing at `at`, in node order, into `into`; `stack` is scratch.
    pub(crate) fn collect_at(&self, at: u64, into: &mut Vec<Armed>, stack: &mut Vec<usize>) {
        let start = into.len();
        stack.clear();
        stack.push(0);
        while let Some(place) = stack.pop() {
            let Some(armed) = self.heap.get(place) else {
                continue;
            };
            // A heap's children are no earlier than their parent: below a later one, none is at.
            if armed.at != at {
                continue;
            }
            into.push(*armed);
            for child in children(place) {
                stack.push(child);
            }
        }
        if let Some(found) = into.get_mut(start..) {
            found.sort_unstable();
        }
    }

    fn put(&mut self, node: u32, place: usize) {
        if let Some(slot) = usize::try_from(node)
            .ok()
            .and_then(|index| self.place.get_mut(index))
        {
            *slot = Some(place);
        }
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.heap.swap(a, b);
        for place in [a, b] {
            if let Some(node) = self.heap.get(place).map(|armed| armed.node) {
                self.put(node, place);
            }
        }
    }

    fn up(&mut self, mut place: usize) -> usize {
        while let Some(parent) = place.checked_sub(1).map(|p| p / 2) {
            match (self.heap.get(place), self.heap.get(parent)) {
                (Some(child), Some(above)) if child < above => {
                    self.swap(place, parent);
                    place = parent;
                }
                _ => break,
            }
        }
        place
    }

    fn down(&mut self, mut place: usize) {
        loop {
            let mut least = place;
            for child in children(place) {
                if let (Some(c), Some(l)) = (self.heap.get(child), self.heap.get(least))
                    && c < l
                {
                    least = child;
                }
            }
            if least == place {
                return;
            }
            self.swap(place, least);
            place = least;
        }
    }
}

/// The children of heap place `place`, where they fit in `usize`.
fn children(place: usize) -> impl Iterator<Item = usize> {
    let first = place.checked_mul(2).and_then(|n| n.checked_add(1));
    let second = first.and_then(|n| n.checked_add(1));
    first.into_iter().chain(second)
}
