//! The newest spillable rows, held (doc 31 M3). A claim waiting to be delivered and an authored
//! definition are spilled to the payload log only once newer ones push them out of the young
//! generation, or the session's memory runs short, as Kafka serves a log's tail from memory and
//! lets its backlog go to disk: a message taken while it is young is never spilled, so it is
//! never read back. A read back decodes and checks the whole row as a replay does: at 6,000
//! writes a second, every delivery spilled at its post and read back at its receipt, that was a
//! quarter of the owner's time.
//!
//! The generation holds at most a quarter of the session's memory in rows (at the bytes a held
//! claim takes; 1,048,576 rows at most), its keys' buffer grown as it fills and charged as it
//! grows, never shrunk. Where the session's ordinary headroom falls below an eighth of its
//! memory the oldest rows are spilled as rows are published (a bounded number a publication),
//! so memory wanted elsewhere is given back first by the backlog. A key pushed out is spilled if
//! its row still may be (`OwnedClaim::spill` decides); a row published again while young may hold
//! two places, and the older one only spills it sooner.
//!
//! A spill can be put off: the row's page is shared with a frozen image or a lease (a checkpoint
//! encoding, a read), or the log refuses payloads for now. Its key is kept, behind the others, and
//! tried again by a later publication once they are, so a row is never held for good for an
//! obstacle that passed. The generation tracks at most twice the rows it holds by choice: those,
//! and as many again whose spill was put off. A key it cannot keep at that bound (or for want of
//! memory to grow into) whose spill is put off is counted, and found again: while any is, a walk
//! of the rows, a bounded step a publication while memory is short, spills the held rows it
//! meets, so a row lost from the generation is reclaimed once its obstacle passes.
//!
//! A key is held as the id it names (a claim's, a definition's): 17 bytes, where a row's key
//! takes 112.

use super::{ClaimId, Key, ValidationId};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError};
use std::collections::VecDeque;

/// The memory a held waiting claim takes, about: what sizes the generation.
const HELD_BYTES: usize = 8 << 10;
/// The generation takes at most this share of the session's memory: a quarter.
const SHARE: usize = 4;
/// The generation's bounds, whatever the memory: from a few hundred rows to 1,048,576.
const MIN_KEYS: usize = 256;
const MAX_KEYS: usize = 1 << 20;
/// Below this share of the session's memory free in its ordinary lane (an eighth), the oldest
/// rows are spilled as rows are published.
const LOW_WATER: usize = 8;
/// The most rows one publication spills for memory, beyond as many as it admitted: what bounds
/// its pause.
pub(super) const EVICT_PER_PUBLISH: usize = 64;
/// The most spills one publication puts off before it stops trying: an obstacle that stands for
/// every row (a frozen image shares every page, a refusing log refuses every payload) costs a
/// publication this many tries, no more.
pub(super) const PUT_OFF_PER_PUBLISH: usize = 8;
/// The rows a walk step visits at most, of every kind, and the held rows it spills at most.
pub(super) const WALK_VISITS: usize = 256;
pub(super) const WALK_SPILLS: usize = 32;

/// A tracked row's key, as small as it may be: the claim or the definition it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Tracked {
    Claim(ClaimId),
    Definition(ValidationId),
}

impl Tracked {
    /// The key of a row the generation may track: a claim's or a definition's.
    pub(super) fn of(key: Key) -> Option<Self> {
        match key {
            Key::Claim(id) => Some(Self::Claim(id)),
            Key::Definition(id) => Some(Self::Definition(id)),
            _ => None,
        }
    }
    pub(super) fn key(self) -> Key {
        match self {
            Self::Claim(id) => Key::Claim(id),
            Self::Definition(id) => Key::Definition(id),
        }
    }
}

/// Keys the generation lost track of whose rows may still be held: those the walk under way
/// answers for, those lost since it began, and where it stands (its last row; none at the rows'
/// start).
#[derive(Default)]
struct Lost {
    walking: u64,
    since: u64,
    at: Option<Key>,
}

/// Whether `budget` is short of memory: its ordinary headroom below its low water, an eighth of
/// its allowance. There a session's young generation spills its oldest rows and its finished
/// families retire without waiting out their grace.
pub(super) fn short(budget: &MemoryBudget) -> bool {
    budget.headroom(BudgetLane::Ordinary) < budget.limit().checked_div(LOW_WATER).unwrap_or(0)
}

pub(super) struct Young {
    /// Oldest first, those whose spill was put off behind the rest.
    keys: VecDeque<Tracked>,
    /// The rows it holds by choice, and the keys it tracks at most: twice as many.
    bound: usize,
    cap: usize,
    /// The ordinary headroom below which the oldest are spilled.
    low_water: usize,
    /// What the keys' buffer is charged, grown as it grows.
    charge: Option<Allocation>,
    /// Spills put off and kept to try again, and keys it could not keep: diagnostics.
    put_off: u64,
    untracked: u64,
    lost: Lost,
}

/// What one publication may take from the generation: as many keys as it admitted and
/// [`EVICT_PER_PUBLISH`] more, until [`PUT_OFF_PER_PUBLISH`] of their spills are put off.
pub(super) struct Pass {
    takes: usize,
    put_off: usize,
}

impl Pass {
    pub(super) fn new(admitted: usize) -> Self {
        Self {
            takes: admitted.saturating_add(EVICT_PER_PUBLISH),
            put_off: 0,
        }
    }
}

impl Young {
    /// No generation: each spillable row is spilled where it is published (a state without a
    /// payload log never spills).
    pub(super) fn none() -> Self {
        Self {
            keys: VecDeque::new(),
            bound: 0,
            cap: 0,
            low_water: 0,
            charge: None,
            put_off: 0,
            untracked: 0,
            lost: Lost::default(),
        }
    }
    /// A generation sized from `budget`, its buffer allocated and charged now.
    pub(super) fn for_budget(budget: &MemoryBudget) -> Result<Self, MemoryError> {
        let bound = budget
            .limit()
            .checked_div(SHARE)
            .and_then(|share| share.checked_div(HELD_BYTES))
            .unwrap_or(0)
            .clamp(MIN_KEYS, MAX_KEYS);
        Self::bounded(budget, bound)
    }
    /// A generation of at most `bound` keys charged to `budget` as its buffer grows; for a bound
    /// of none, one that holds no row (each spills where it is published) and finds again those
    /// whose spill was put off.
    pub(super) fn bounded(budget: &MemoryBudget, bound: usize) -> Result<Self, MemoryError> {
        if bound == 0 {
            return Ok(Self {
                low_water: budget.limit().checked_div(LOW_WATER).unwrap_or(0),
                ..Self::none()
            });
        }
        Ok(Self {
            keys: VecDeque::new(),
            bound,
            cap: bound.saturating_mul(2),
            low_water: budget.limit().checked_div(LOW_WATER).unwrap_or(0),
            charge: Some(
                budget
                    .reserve(BudgetKind::Index, BudgetLane::Ordinary, 0)?
                    .commit(),
            ),
            put_off: 0,
            untracked: 0,
            lost: Lost::default(),
        })
    }
    /// `key` joins the generation, newest; the oldest it pushes out are taken by the pass that
    /// follows ([`Self::next`]). With no generation, or no room to keep it in, `key` is returned to
    /// spill now.
    pub(super) fn admit(&mut self, key: Key) -> Option<Key> {
        let Some(tracked) = Tracked::of(key) else {
            return Some(key);
        };
        if self.bound == 0 {
            return Some(key);
        }
        if self.keys.len() == self.keys.capacity() && !self.grow() {
            return Some(key);
        }
        self.keys.push_back(tracked);
        None
    }
    /// The oldest key to spill, within `pass`: while the generation tracks more keys than it
    /// holds rows by choice, or the ordinary headroom of `budget` is below its low water.
    pub(super) fn next(&mut self, budget: &MemoryBudget, pass: &mut Pass) -> Option<Key> {
        if self.bound == 0 || pass.takes == 0 || pass.put_off >= PUT_OFF_PER_PUBLISH {
            return None;
        }
        if self.keys.len() <= self.bound && budget.headroom(BudgetLane::Ordinary) >= self.low_water
        {
            return None;
        }
        pass.takes = pass.takes.saturating_sub(1);
        self.keys.pop_front().map(Tracked::key)
    }
    /// A key [`Self::next`] gave whose spill was put off, kept behind the others to try again: in
    /// the place it was taken from, so no growth.
    pub(super) fn put_off(&mut self, key: Key, pass: &mut Pass) {
        pass.put_off = pass.put_off.saturating_add(1);
        self.put_off = self.put_off.saturating_add(1);
        match Tracked::of(key) {
            Some(tracked) if self.keys.len() < self.keys.capacity() => self.keys.push_back(tracked),
            _ => self.lost(),
        }
    }
    /// A key the generation could not keep whose spill was put off: counted, and found again by
    /// a walk ([`Self::walk_from`]).
    pub(super) fn lost(&mut self) {
        self.untracked = self.untracked.saturating_add(1);
        self.lost.since = self.lost.since.saturating_add(1);
    }
    /// A row a walk found held whose spill was put off: found again by a later walk.
    pub(super) fn found_put_off(&mut self) {
        self.put_off = self.put_off.saturating_add(1);
        self.lost.since = self.lost.since.saturating_add(1);
    }
    /// Whether a walk step is due: some key is lost and the ordinary headroom of `budget` is below
    /// the low water (a session with room to spare holds the rows).
    pub(super) fn walk_due(&self, budget: &MemoryBudget) -> bool {
        (self.lost.walking > 0 || self.lost.since > 0)
            && budget.headroom(BudgetLane::Ordinary) < self.low_water
    }
    /// Where the next walk step begins, after this row (none: at the rows' start), a walk begun
    /// for every key lost so far where none is under way.
    pub(super) fn walk_from(&mut self) -> Option<Key> {
        if self.lost.walking == 0 {
            self.lost.walking = std::mem::take(&mut self.lost.since);
            self.lost.at = None;
        }
        self.lost.at
    }
    /// A walk step ended after `last`, or, with none, at the rows' end: the walk is done, and
    /// what was lost since it began is the next one's.
    pub(super) fn walked(&mut self, last: Option<Key>) {
        match last {
            Some(last) => self.lost.at = Some(last),
            None => {
                self.lost.walking = 0;
                self.lost.at = None;
            }
        }
    }
    /// Spills put off and kept to try again, and keys it could not keep, since it was made.
    pub(super) fn counts(&self) -> (u64, u64) {
        (self.put_off, self.untracked)
    }
    /// The buffer doubled (to the cap at most), its charge admitted first.
    fn grow(&mut self) -> bool {
        let Some(charge) = self.charge.as_mut() else {
            return false;
        };
        let have = self.keys.capacity();
        let want = have.saturating_mul(2).max(64).min(self.cap);
        if want <= have {
            return false;
        }
        let Some(more) = want
            .checked_sub(have)
            .and_then(|keys| keys.checked_mul(size_of::<Tracked>()))
        else {
            return false;
        };
        if charge.grow(more).is_err() {
            return false;
        }
        if self
            .keys
            .try_reserve_exact(want.saturating_sub(self.keys.len()))
            .is_err()
        {
            let held = charge.bytes().saturating_sub(more);
            // Given back exactly what was admitted for it.
            let _ = charge.shrink_to(held);
            return false;
        }
        true
    }
    /// The keys it holds now.
    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }
    #[cfg(test)]
    pub(super) fn bound(&self) -> usize {
        self.bound
    }
}
