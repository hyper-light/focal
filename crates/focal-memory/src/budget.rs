use crate::MemoryError;
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

#[path = "budget_elastic.rs"]
mod elastic;
pub use elastic::ElasticFundedPool;

#[cfg(test)]
#[path = "budget_elastic_tests.rs"]
mod elastic_tests;
#[cfg(test)]
#[path = "budget_interleaving_tests.rs"]
mod interleaving_tests;

/// The bound a reservation is held to at each level of its walk. Ordinary work cannot consume
/// the reserved completion/control allowance.
///
/// - `Ordinary`: fresh work, held to a level's ordinary limit (its limit less the reserve kept for
///   completing what was admitted) and to its limit.
/// - `Completion`: work completing what was admitted, held to the limit.
/// - `Committed`: what a committed fact requires where it applies, held to the level's hold limit,
///   never less than its limit. A replica applies what its leader admitted under the leader's
///   limits; where those are wider than its own, its hold is what lets it follow (doc 31 M3).
///
/// Work is funded by a pool of its own lane or of one before it: completion funding cannot pay
/// for ordinary work, and committed work is never prepaid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BudgetLane {
    Ordinary,
    Completion,
    Committed,
}

/// A byte: an allocation carries one ([`Allocation`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BudgetKind {
    Arena,
    Payload,
    Index,
    Pages,
    Roots,
    ReadPins,
    Pending,
    Query,
    Monitor,
    Dedup,
    Timer,
    Recovery,
    Control,
    /// Unspent prepaid capacity and its non-spendable pool bookkeeping.
    Reserved,
}

const KIND_COUNT: usize = BudgetKind::Reserved as usize + 1;

impl BudgetKind {
    /// Every kind, in the order the counters keep them.
    pub const ALL: [Self; KIND_COUNT] = [
        Self::Arena,
        Self::Payload,
        Self::Index,
        Self::Pages,
        Self::Roots,
        Self::ReadPins,
        Self::Pending,
        Self::Query,
        Self::Monitor,
        Self::Dedup,
        Self::Timer,
        Self::Recovery,
        Self::Control,
        Self::Reserved,
    ];
    /// The kind's name, as a metric's label gives it.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Arena => "arena",
            Self::Payload => "payload",
            Self::Index => "index",
            Self::Pages => "pages",
            Self::Roots => "roots",
            Self::ReadPins => "read_pins",
            Self::Pending => "pending",
            Self::Query => "query",
            Self::Monitor => "monitor",
            Self::Dedup => "dedup",
            Self::Timer => "timer",
            Self::Recovery => "recovery",
            Self::Control => "control",
            Self::Reserved => "reserved",
        }
    }
}

// Named counters make category access total, including during Drop.
#[derive(Debug)]
struct KindCounters {
    arena: AtomicUsize,
    payload: AtomicUsize,
    index: AtomicUsize,
    pages: AtomicUsize,
    roots: AtomicUsize,
    readpins: AtomicUsize,
    pending: AtomicUsize,
    query: AtomicUsize,
    monitor: AtomicUsize,
    dedup: AtomicUsize,
    timer: AtomicUsize,
    recovery: AtomicUsize,
    control: AtomicUsize,
    reserved: AtomicUsize,
}
impl KindCounters {
    fn new() -> Self {
        Self {
            arena: AtomicUsize::new(0),
            payload: AtomicUsize::new(0),
            index: AtomicUsize::new(0),
            pages: AtomicUsize::new(0),
            roots: AtomicUsize::new(0),
            readpins: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            query: AtomicUsize::new(0),
            monitor: AtomicUsize::new(0),
            dedup: AtomicUsize::new(0),
            timer: AtomicUsize::new(0),
            recovery: AtomicUsize::new(0),
            control: AtomicUsize::new(0),
            reserved: AtomicUsize::new(0),
        }
    }
    fn get(&self, kind: BudgetKind) -> &AtomicUsize {
        match kind {
            BudgetKind::Arena => &self.arena,
            BudgetKind::Payload => &self.payload,
            BudgetKind::Index => &self.index,
            BudgetKind::Pages => &self.pages,
            BudgetKind::Roots => &self.roots,
            BudgetKind::ReadPins => &self.readpins,
            BudgetKind::Pending => &self.pending,
            BudgetKind::Query => &self.query,
            BudgetKind::Monitor => &self.monitor,
            BudgetKind::Dedup => &self.dedup,
            BudgetKind::Timer => &self.timer,
            BudgetKind::Recovery => &self.recovery,
            BudgetKind::Control => &self.control,
            BudgetKind::Reserved => &self.reserved,
        }
    }
    /// Every counter at zero, as a level's life begins.
    fn clear(&self) {
        for kind in BudgetKind::ALL {
            self.get(kind).store(0, Ordering::Relaxed);
        }
    }
    fn snapshot(&self) -> [usize; KIND_COUNT] {
        [
            self.arena.load(Ordering::Acquire),
            self.payload.load(Ordering::Acquire),
            self.index.load(Ordering::Acquire),
            self.pages.load(Ordering::Acquire),
            self.roots.load(Ordering::Acquire),
            self.readpins.load(Ordering::Acquire),
            self.pending.load(Ordering::Acquire),
            self.query.load(Ordering::Acquire),
            self.monitor.load(Ordering::Acquire),
            self.dedup.load(Ordering::Acquire),
            self.timer.load(Ordering::Acquire),
            self.recovery.load(Ordering::Acquire),
            self.control.load(Ordering::Acquire),
            self.reserved.load(Ordering::Acquire),
        ]
    }
}

/// The most levels a budget tree is deep, its root included.
const MAX_DEPTH: usize = 8;
/// Levels in a segment of the level table.
const SEGMENT: usize = 256;
/// Segments the level table holds at most: `SEGMENT` × `SEGMENTS` levels.
const SEGMENTS: usize = 4096;
/// A root level's parent.
const NO_PARENT: u32 = u32::MAX;

/// The levels of every budget in the process. A segment is made as the table
/// first needs it and kept for the process's life: a level's slot is referred
/// to by its budgets and permits for as long as they live, then recycled,
/// never freed. So the table holds its peak of live levels, in whole
/// segments, and at most `SEGMENT` × `SEGMENTS` of them: a budget past that is
/// refused `Capacity`. Budgets and permits are references to their level's
/// slot, so none of them holds a count of its own.
static LEVELS: Levels = Levels::new();

struct Levels {
    segments: [OnceLock<Box<[Level]>>; SEGMENTS],
    /// The first slot never used.
    fresh: AtomicUsize,
    /// Slots whose level ended, used again first.
    free: Mutex<Vec<u32>>,
}

impl Levels {
    const fn new() -> Self {
        Self {
            segments: [const { OnceLock::new() }; SEGMENTS],
            fresh: AtomicUsize::new(0),
            free: Mutex::new(Vec::new()),
        }
    }

    fn level(&'static self, index: u32) -> Option<&'static Level> {
        let index = usize::try_from(index).ok()?;
        self.segments
            .get(index.checked_div(SEGMENT)?)?
            .get()?
            .get(index.checked_rem(SEGMENT)?)
    }

    /// A slot for a new level: one whose level ended, or else the first never
    /// used, its segment made if it is the first of one.
    fn take(&'static self) -> Result<&'static Level, MemoryError> {
        let full = MemoryError::Capacity {
            requested: 1,
            available: 0,
        };
        let recycled = self.free.lock().ok().and_then(|mut free| free.pop());
        let index = match recycled {
            Some(index) => usize::try_from(index).map_err(|_| full.clone())?,
            None => self
                .fresh
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                    next.checked_add(1).filter(|next| {
                        SEGMENT
                            .checked_mul(SEGMENTS)
                            .is_some_and(|all| *next <= all)
                    })
                })
                .map_err(|_| full.clone())?,
        };
        let segment = index
            .checked_div(SEGMENT)
            .and_then(|segment| self.segments.get(segment))
            .ok_or(full.clone())?;
        if segment.get().is_none() {
            match Self::segment(index.saturating_sub(index.checked_rem(SEGMENT).unwrap_or(0))) {
                // Another thread may have made it first: its segment stands.
                Ok(slots) => {
                    let _ = segment.set(slots);
                }
                Err(error) => {
                    if let Ok(index) = u32::try_from(index) {
                        self.recycle(index);
                    }
                    return Err(error);
                }
            }
        }
        segment
            .get()
            .and_then(|slots| slots.get(index.checked_rem(SEGMENT)?))
            .ok_or(full)
    }

    /// The segment of slots from `first`.
    fn segment(first: usize) -> Result<Box<[Level]>, MemoryError> {
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(SEGMENT)
            .map_err(|_| MemoryError::AllocationFailed)?;
        for offset in 0..SEGMENT {
            let index = first
                .checked_add(offset)
                .and_then(|index| u32::try_from(index).ok())
                .ok_or(MemoryError::InvalidConfiguration(
                    "budget level index overflow",
                ))?;
            slots.push(Level::new(index));
        }
        Ok(slots.into_boxed_slice())
    }

    /// A slot whose level ended, for the next level. A slot the free list has
    /// no room for is not used again (the table's bound still holds).
    fn recycle(&self, index: u32) {
        if let Ok(mut free) = self.free.lock()
            && free.try_reserve(1).is_ok()
        {
            free.push(index);
        }
    }
}

/// One level of a budget tree: its allowance, its counters and how it is
/// funded, in a slot of [`LEVELS`]. A level holds no handle on any other: its
/// parent is a slot. It lives while it is held (`holds`: its budget handles,
/// its permits of zero bytes and its child levels) or charged (`total`, which
/// every permit of more bytes is part of, and for an elastic level its credit
/// out), and the release that leaves it neither ends it ([`end`]). Aligned to
/// 128 bytes, the cache line of Apple silicon and the pair x86's
/// adjacent-line prefetch moves, so two budgets' counters never share one.
#[derive(Debug)]
#[repr(align(128))]
struct Level {
    /// The slot's place in the table.
    index: u32,
    /// The slot's generation, shifted left once, with the low bit set while
    /// a level lives in it: the release that ends a level moves it to the
    /// next generation, so a release from an earlier life never ends a later
    /// one.
    state: AtomicU64,
    /// Budget handles, permits of zero bytes and child levels.
    holds: AtomicUsize,
    limit: AtomicUsize,
    ordinary_limit: AtomicUsize,
    /// What committed work may take ([`BudgetLane::Committed`]): at least `limit`.
    hold_limit: AtomicUsize,
    total: AtomicUsize,
    ordinary: AtomicUsize,
    kinds: KindCounters,
    depth: AtomicU8,
    /// The parent's slot, or [`NO_PARENT`].
    parent: AtomicU32,
    /// The reservations made at this level that a level of their walk
    /// refused (`reserve_tree`), and the last one's lane, request and what
    /// was then available ([`MemoryBudget::refusals`]).
    refusals: AtomicU64,
    refused_lane: AtomicU8,
    refused_requested: AtomicUsize,
    refused_available: AtomicUsize,
    // A single owner-level pool holds its backing through the lifetime of every
    // issued allocation and descendant budget. Never one pool per domain row.
    // While in use, ancestor kinds are reclassified but its total stays held.
    backing: Backing,
}

/// What a new level is: its allowance, its depth and how it is funded.
struct Terms {
    limit: usize,
    ordinary_limit: usize,
    hold_limit: usize,
    depth: u8,
    funding: Funding,
}

enum Funding {
    /// Charged to its ancestors as it goes.
    Unfunded,
    /// Prepaid capacity charged to the parent as `Reserved` in `lane`, held
    /// while the level lives.
    Fixed { lane: BudgetLane, bytes: usize },
    /// Prepaid credit its pool grows and trims (`ElasticFundedPool`).
    Elastic {
        lane: BudgetLane,
        held: usize,
        available: usize,
        metadata: usize,
    },
}

const UNFUNDED: u8 = 0;
const FIXED_ORDINARY: u8 = 1;
const FIXED_COMPLETION: u8 = 2;
const ELASTIC_ORDINARY: u8 = 3;
const ELASTIC_COMPLETION: u8 = 4;

/// How a level is funded, set as its life begins.
#[derive(Debug)]
struct Backing {
    /// Its form and lane, one byte read once a level: [`UNFUNDED`], or fixed
    /// or elastic in the ordinary or completion lane.
    funding: AtomicU8,
    /// What the parent holds for this level: the fixed prepayment, or the
    /// elastic aggregate with its metadata. Only an elastic pool's unique
    /// controller changes it; the level's end gives it back.
    held: AtomicUsize,
    /// Elastic credit not yet issued.
    available: AtomicUsize,
    /// The elastic pool's non-spendable bookkeeping.
    metadata: AtomicUsize,
}

impl Backing {
    fn new() -> Self {
        Self {
            funding: AtomicU8::new(UNFUNDED),
            held: AtomicUsize::new(0),
            available: AtomicUsize::new(0),
            metadata: AtomicUsize::new(0),
        }
    }

    fn set(&self, funding: &Funding) {
        let (form, held, available, metadata) = match *funding {
            Funding::Unfunded => (UNFUNDED, 0, 0, 0),
            Funding::Fixed { lane, bytes } => (
                // Committed work is never prepaid (`funded_child` refuses it), so a funded
                // level's lane is one of the other two.
                match lane {
                    BudgetLane::Ordinary => FIXED_ORDINARY,
                    BudgetLane::Completion | BudgetLane::Committed => FIXED_COMPLETION,
                },
                bytes,
                0,
                0,
            ),
            Funding::Elastic {
                lane,
                held,
                available,
                metadata,
            } => (
                match lane {
                    BudgetLane::Ordinary => ELASTIC_ORDINARY,
                    BudgetLane::Completion | BudgetLane::Committed => ELASTIC_COMPLETION,
                },
                held,
                available,
                metadata,
            ),
        };
        self.held.store(held, Ordering::Relaxed);
        self.available.store(available, Ordering::Relaxed);
        self.metadata.store(metadata, Ordering::Relaxed);
        self.funding.store(form, Ordering::Relaxed);
    }

    /// Its funding lane, if funded, and whether elastically.
    fn funding(&self) -> (Option<BudgetLane>, bool) {
        match self.funding.load(Ordering::Relaxed) {
            FIXED_ORDINARY => (Some(BudgetLane::Ordinary), false),
            FIXED_COMPLETION => (Some(BudgetLane::Completion), false),
            ELASTIC_ORDINARY => (Some(BudgetLane::Ordinary), true),
            ELASTIC_COMPLETION => (Some(BudgetLane::Completion), true),
            _ => (None, false),
        }
    }

    fn lane(&self) -> Option<BudgetLane> {
        self.funding().0
    }

    fn is_elastic(&self) -> bool {
        self.funding().1
    }

    /// What the parent holds for this level, given back when the level ends.
    fn held(&self) -> Option<(BudgetLane, usize)> {
        Some((self.lane()?, self.held.load(Ordering::Acquire)))
    }

    fn held_bytes(&self) -> usize {
        self.held.load(Ordering::Acquire)
    }

    fn metadata(&self) -> usize {
        self.metadata.load(Ordering::Relaxed)
    }

    /// Exclusively acquire already-funded credit before publishing local usage
    /// or reclassifying ancestors. Failure changes no counter.
    fn acquire(&self, bytes: usize) -> Result<(), MemoryError> {
        self.available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |available| {
                available.checked_sub(bytes)
            })
            .map(|_| ())
            .map_err(|available| MemoryError::Capacity {
                requested: bytes,
                available,
            })
    }

    /// Restore exact acquired credit only after local usage and every ancestor
    /// category have been refunded. Newly grown credit likewise has full parent
    /// backing before this publication. Those ownership rules bound the sum by
    /// funded capacity, including concurrent returns and in-flight
    /// acquisitions. Whether every credit is back: the level may then end.
    fn release(&self, bytes: usize) -> bool {
        // Read while this credit still holds the level.
        let capacity = self.held_bytes().saturating_sub(self.metadata());
        self.available
            .fetch_add(bytes, Ordering::SeqCst)
            .checked_add(bytes)
            .is_some_and(|available| available == capacity)
    }

    /// Whether no credit is out: a level that is not elastic has none.
    fn settled(&self) -> bool {
        !self.is_elastic()
            || self.available.load(Ordering::SeqCst)
                == self.held_bytes().saturating_sub(self.metadata())
    }
}

impl Level {
    fn new(index: u32) -> Self {
        Self {
            index,
            state: AtomicU64::new(0),
            holds: AtomicUsize::new(0),
            limit: AtomicUsize::new(0),
            ordinary_limit: AtomicUsize::new(0),
            hold_limit: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
            ordinary: AtomicUsize::new(0),
            kinds: KindCounters::new(),
            depth: AtomicU8::new(0),
            parent: AtomicU32::new(NO_PARENT),
            refusals: AtomicU64::new(0),
            refused_lane: AtomicU8::new(0),
            refused_requested: AtomicUsize::new(0),
            refused_available: AtomicUsize::new(0),
            backing: Backing::new(),
        }
    }

    /// The slot's next life: a level of `terms` under `parent`, held once, by
    /// the budget handle its creator is given. Every field is set before the
    /// handle is, and other threads see it through how the handle reaches
    /// them.
    fn begin(&self, terms: &Terms, parent: Option<&Level>) {
        self.limit.store(terms.limit, Ordering::Relaxed);
        self.ordinary_limit
            .store(terms.ordinary_limit, Ordering::Relaxed);
        self.hold_limit.store(terms.hold_limit, Ordering::Relaxed);
        self.total.store(0, Ordering::Relaxed);
        self.ordinary.store(0, Ordering::Relaxed);
        self.kinds.clear();
        self.depth.store(terms.depth, Ordering::Relaxed);
        self.parent.store(
            parent.map_or(NO_PARENT, |parent| parent.index),
            Ordering::Relaxed,
        );
        self.backing.set(&terms.funding);
        self.refusals.store(0, Ordering::Relaxed);
        self.refused_lane.store(0, Ordering::Relaxed);
        self.refused_requested.store(0, Ordering::Relaxed);
        self.refused_available.store(0, Ordering::Relaxed);
        self.holds.store(1, Ordering::Relaxed);
        self.state.fetch_or(1, Ordering::Release);
    }

    fn parent(&self) -> Option<&'static Level> {
        match self.parent.load(Ordering::Relaxed) {
            NO_PARENT => None,
            index => LEVELS.level(index),
        }
    }

    /// One level's share of [`reserve_tree`]: its lane check, its elastic
    /// credit, its counters and its kind, all given back if one refuses.
    fn reserve_local(
        &self,
        kind: BudgetKind,
        lane: BudgetLane,
        bytes: usize,
    ) -> Result<(), MemoryError> {
        let (funding, elastic) = self.backing.funding();
        if lane == BudgetLane::Ordinary && funding == Some(BudgetLane::Completion) {
            return Err(MemoryError::InvalidConfiguration(
                "completion funding cannot admit ordinary work",
            ));
        }
        if elastic {
            // This credit, rather than a limit/usage snapshot, excludes a
            // concurrent trim before any usage or category becomes visible.
            self.backing.acquire(bytes)?;
            #[cfg(test)]
            if bytes != 0 {
                interleaving_tests::pause(interleaving_tests::Step::Acquired, self);
            }
        }
        if lane == BudgetLane::Ordinary
            && let Err(error) = reserve_counter(
                &self.ordinary,
                self.ordinary_limit.load(Ordering::Relaxed),
                bytes,
            )
        {
            if elastic {
                self.backing.release(bytes);
            }
            return Err(error);
        }
        let bound = match lane {
            BudgetLane::Committed => self.hold_limit.load(Ordering::Relaxed),
            BudgetLane::Ordinary | BudgetLane::Completion => self.limit.load(Ordering::Relaxed),
        };
        if let Err(error) = reserve_counter(&self.total, bound, bytes) {
            if lane == BudgetLane::Ordinary {
                self.ordinary.fetch_sub(bytes, Ordering::AcqRel);
            }
            if elastic {
                self.backing.release(bytes);
            }
            return Err(error);
        }
        // A category can never exceed total, so this addition cannot overflow.
        self.kinds.get(kind).fetch_add(bytes, Ordering::AcqRel);
        Ok(())
    }

    /// One level's share of [`release_tree`]. What may let the level end comes
    /// last, after which the release changes it no more: its total, or for an
    /// elastic level its credit, published after its category and lane
    /// bookkeeping. A level that release leaves uncharged, and finds unheld,
    /// is recorded in `ended`, with the life it was in; one still held ends,
    /// if it does, as its last hold goes.
    fn release_local(
        &'static self,
        kind: BudgetKind,
        lane: BudgetLane,
        bytes: usize,
        elastic: bool,
        ended: &mut Ended,
    ) {
        let live = self.state.load(Ordering::Relaxed);
        self.kinds.get(kind).fetch_sub(bytes, Ordering::AcqRel);
        if lane == BudgetLane::Ordinary {
            self.ordinary.fetch_sub(bytes, Ordering::AcqRel);
        }
        // Return reusable capacity last, after its category/lane bookkeeping.
        let total = self.total.fetch_sub(bytes, Ordering::SeqCst);
        let last = if elastic {
            #[cfg(test)]
            if bytes != 0 {
                interleaving_tests::pause(interleaving_tests::Step::RefundReady, self);
            }
            self.backing.release(bytes)
        } else {
            total == bytes
        };
        // Read after the release's last change, as a hold's release reads the
        // charge after its own (`end`): one of the two sees the other.
        if last && self.holds.load(Ordering::SeqCst) == 0 {
            ended.record(self, live);
        }
    }
}

/// The levels a release may have left unheld and uncharged, each with the
/// life it was in: each is ended, if it is, once the release is done
/// ([`end`]). A release passes [`MAX_DEPTH`] levels at most.
struct Ended {
    levels: [Option<(&'static Level, u64)>; MAX_DEPTH],
    count: usize,
}

impl Ended {
    const fn new() -> Self {
        Self {
            levels: [None; MAX_DEPTH],
            count: 0,
        }
    }

    fn record(&mut self, level: &'static Level, live: u64) {
        if let Some(slot) = self.levels.get_mut(self.count) {
            *slot = Some((level, live));
            self.count = self.count.saturating_add(1);
        }
    }

    fn end(self) {
        for (level, live) in self.levels.into_iter().take(self.count).flatten() {
            end(level, live);
        }
    }
}

/// Ends `level`, last seen in life `live`, if nothing holds or charges it
/// and no other release ended it first; then each ancestor its end leaves
/// unheld, a level a turn. A funded level's backing goes back to its parent,
/// its hold on the parent is let go, and its slot is recycled. The release of
/// a level's last hold and of its last byte or credit each look at the
/// other's count after their own (all sequentially consistent, so at least
/// one sees both at zero), and only the one that moves the level's state on
/// ends it.
fn end(mut level: &'static Level, mut live: u64) {
    loop {
        if level.holds.load(Ordering::SeqCst) != 0
            || level.total.load(Ordering::SeqCst) != 0
            || !level.backing.settled()
        {
            return;
        }
        let Some(next) = live
            .checked_shr(1)
            .and_then(|generation| generation.checked_add(1))
            .and_then(|generation| generation.checked_mul(2))
        else {
            return;
        };
        if level
            .state
            .compare_exchange(live, next, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let parent = level.parent();
        let backing = level.backing.held();
        LEVELS.recycle(level.index);
        let Some(parent) = parent else {
            return;
        };
        // This level's hold keeps its parent, and each ancestor's hold its
        // own parent, so the parent is live and nothing ends below.
        let parent_live = parent.state.load(Ordering::Relaxed);
        if let Some((lane, bytes)) = backing {
            release_tree(parent, BudgetKind::Reserved, lane, bytes, &mut Ended::new());
        }
        if parent.holds.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        level = parent;
        live = parent_live;
    }
}

/// Lets one hold on `level` go, and ends the level if that was its last.
fn release_hold(level: &'static Level) {
    let live = level.state.load(Ordering::Relaxed);
    if level.holds.fetch_sub(1, Ordering::SeqCst) == 1 {
        end(level, live);
    }
}

/// `level` and its ancestors, nearest first.
fn lineage(level: &'static Level) -> impl Iterator<Item = &'static Level> {
    std::iter::successors(Some(level), |level| level.parent()).take(MAX_DEPTH)
}

/// Take `bytes` of `kind` on `lane` at `level` and each of its ancestors, a
/// level a turn: a funded level ends the walk by moving its ancestors' credit
/// into the charge's kind instead of charging them. A level that refuses
/// gives back what the levels below it took, from `level` up, as
/// `release_tree` does.
fn reserve_tree(
    level: &'static Level,
    kind: BudgetKind,
    lane: BudgetLane,
    bytes: usize,
) -> Result<(), MemoryError> {
    let mut current = level;
    let mut taken: usize = 0;
    loop {
        if let Err(error) = current.reserve_local(kind, lane, bytes) {
            release_levels(level, kind, lane, bytes, taken);
            if let MemoryError::Capacity {
                requested,
                available,
            } = error
            {
                // Its fields before its count: a reader that sees the count
                // sees the request it counts, or a later one.
                level.refused_lane.store(
                    match lane {
                        BudgetLane::Ordinary => 0,
                        BudgetLane::Completion => 1,
                        BudgetLane::Committed => 2,
                    },
                    Ordering::Relaxed,
                );
                level.refused_requested.store(requested, Ordering::Relaxed);
                level.refused_available.store(available, Ordering::Relaxed);
                level.refusals.fetch_add(1, Ordering::Release);
            }
            return Err(error);
        }
        taken = taken.saturating_add(1);
        let Some(parent) = current.parent() else {
            return Ok(());
        };
        if current.backing.lane().is_some() {
            // Local capacity is exclusively acquired before ancestor credit
            // changes category. No ancestor total or lane admission occurs.
            reclassify_tree(parent, BudgetKind::Reserved, kind, bytes);
            return Ok(());
        }
        if taken >= MAX_DEPTH {
            release_levels(level, kind, lane, bytes, taken);
            return Err(MemoryError::InvalidConfiguration(
                "memory budget hierarchy exceeds eight levels",
            ));
        }
        current = parent;
    }
}
/// Gives back what the first `levels` levels from `level` up took for a
/// reservation a level above them refused. Whoever reserves holds `level`,
/// so none of them ends.
fn release_levels(
    level: &'static Level,
    kind: BudgetKind,
    lane: BudgetLane,
    bytes: usize,
    levels: usize,
) {
    let mut ended = Ended::new();
    lineage(level).take(levels).for_each(|current| {
        let elastic = current.backing.is_elastic();
        current.release_local(kind, lane, bytes, elastic, &mut ended);
    });
}
/// Give back `bytes` of `kind` on `lane` taken at `level` and its ancestors,
/// recording in `ended` each level it may leave unheld and uncharged. Each
/// level's parent is read before the level's own release, after which the
/// level may end; the parent stays, charged with these bytes until its own
/// release, or holding a funded level whose charge it is.
fn release_tree(
    level: &'static Level,
    kind: BudgetKind,
    lane: BudgetLane,
    bytes: usize,
    ended: &mut Ended,
) {
    let mut current = level;
    for _ in 0..MAX_DEPTH {
        let parent = current.parent();
        let (funding, elastic) = current.backing.funding();
        if funding.is_some() {
            // Restore ancestor credit BEFORE local capacity becomes reusable.
            // Otherwise another thread could acquire it while Reserved is
            // still empty, underflowing the category transfer.
            if let Some(parent) = parent {
                reclassify_tree(parent, kind, BudgetKind::Reserved, bytes);
            }
            current.release_local(kind, lane, bytes, elastic, ended);
            return;
        }
        current.release_local(kind, lane, bytes, elastic, ended);
        let Some(parent) = parent else {
            return;
        };
        current = parent;
    }
}
/// Move an already-held charge through every category ledger from `level`
/// up. This must cross funded boundaries without changing their totals or
/// backing lanes. Subtract before adding so even concurrent transfers cannot
/// overfill a category. The caller owns the entire source charge at every
/// ancestor.
fn reclassify_tree(level: &'static Level, from: BudgetKind, to: BudgetKind, bytes: usize) {
    if from == to || bytes == 0 {
        return;
    }
    for current in lineage(level) {
        current.kinds.get(from).fetch_sub(bytes, Ordering::AcqRel);
        current.kinds.get(to).fetch_add(bytes, Ordering::AcqRel);
    }
}

/// Shareable accounting only: there is no shared mutable graph behind this
/// handle. Reservations from independent owners use atomic bounded counters.
/// A funded child's local usage counts issued permits while its ancestors
/// continue to count the whole prepaid reservation in its original funding lane.
pub struct MemoryBudget {
    level: &'static Level,
}

impl Clone for MemoryBudget {
    fn clone(&self) -> Self {
        // This handle holds the level, so its holds stay above zero.
        self.level.holds.fetch_add(1, Ordering::Relaxed);
        Self { level: self.level }
    }
}

impl Drop for MemoryBudget {
    fn drop(&mut self) {
        release_hold(self.level);
    }
}

impl std::fmt::Debug for MemoryBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Do not format the ancestry: nested funded pools would duplicate it
        // at each level.
        f.debug_struct("MemoryBudget")
            .field("stats", &self.stats())
            .field("depth", &self.level.depth.load(Ordering::Relaxed))
            .field("funding_lane", &self.level.backing.lane())
            .finish()
    }
}

/// A budget's capacity refusals ([`MemoryBudget::refusals`]): their count,
/// and the last one's lane, request and what was then available. Where two
/// threads' reservations at one budget are refused at once, the last one's
/// fields may be either one's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetRefusals {
    pub count: u64,
    pub lane: BudgetLane,
    pub requested: usize,
    pub available: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BudgetStats {
    pub limit: usize,
    pub completion_reserve: usize,
    pub used: usize,
    pub ordinary_used: usize,
    pub by_kind: [usize; KIND_COUNT],
}

impl BudgetStats {
    /// The bytes charged under `kind` when these statistics were taken.
    pub fn used_by(&self, kind: BudgetKind) -> usize {
        let index = match kind {
            BudgetKind::Arena => 0,
            BudgetKind::Payload => 1,
            BudgetKind::Index => 2,
            BudgetKind::Pages => 3,
            BudgetKind::Roots => 4,
            BudgetKind::ReadPins => 5,
            BudgetKind::Pending => 6,
            BudgetKind::Query => 7,
            BudgetKind::Monitor => 8,
            BudgetKind::Dedup => 9,
            BudgetKind::Timer => 10,
            BudgetKind::Recovery => 11,
            BudgetKind::Control => 12,
            BudgetKind::Reserved => 13,
        };
        self.by_kind.get(index).copied().unwrap_or(0)
    }
}

impl MemoryBudget {
    /// Immutable allowance; reading it does not sample concurrent usage counters.
    pub fn limit(&self) -> usize {
        self.level.limit.load(Ordering::Relaxed)
    }

    pub fn new(limit: usize, completion_reserve: usize) -> Result<Self, MemoryError> {
        Self::create(limit, completion_reserve, limit, None)
    }
    /// Reservations charge this child and its ancestors up to any funded pool;
    /// that pool's backing is already held above it. A parent's refusal rolls
    /// back the entire tentative reservation. Local allowances can be larger
    /// than a parent; the strictest live limit and funding-lane restriction win.
    pub fn child(&self, limit: usize, completion_reserve: usize) -> Result<Self, MemoryError> {
        Self::create(limit, completion_reserve, limit, Some(self))
    }
    /// [`Self::child`] whose committed work ([`BudgetLane::Committed`]) may take up to `hold`,
    /// at least `limit`: a replica that admits within `limit` and applies what a leader with a
    /// wider one admitted (doc 31 M3). Its ancestors still bound what it takes.
    pub fn child_holding(
        &self,
        limit: usize,
        completion_reserve: usize,
        hold: usize,
    ) -> Result<Self, MemoryError> {
        Self::create(limit, completion_reserve, hold, Some(self))
    }
    fn create(
        limit: usize,
        completion_reserve: usize,
        hold: usize,
        parent: Option<&Self>,
    ) -> Result<Self, MemoryError> {
        if limit == 0 || completion_reserve > limit || hold < limit {
            return Err(MemoryError::InvalidConfiguration(
                "invalid memory allowance",
            ));
        }
        let depth = match parent {
            Some(parent) => parent.child_depth()?,
            None => 1,
        };
        Self::begin(
            &Terms {
                limit,
                ordinary_limit: limit.saturating_sub(completion_reserve),
                hold_limit: hold,
                depth,
                funding: Funding::Unfunded,
            },
            parent,
        )
    }
    /// A child's depth: one below this budget, eight levels at most.
    fn child_depth(&self) -> Result<u8, MemoryError> {
        self.level
            .depth
            .load(Ordering::Relaxed)
            .checked_add(1)
            .filter(|depth| usize::from(*depth) <= MAX_DEPTH)
            .ok_or(MemoryError::InvalidConfiguration(
                "memory budget hierarchy exceeds eight levels",
            ))
    }
    /// The budget of a new level of `terms` under `parent`, which the level
    /// holds.
    fn begin(terms: &Terms, parent: Option<&Self>) -> Result<Self, MemoryError> {
        let level = LEVELS.take()?;
        if let Some(parent) = parent {
            parent.level.holds.fetch_add(1, Ordering::Relaxed);
        }
        level.begin(terms, parent.map(|parent| parent.level));
        Ok(Self { level })
    }

    /// Reserve a reusable owner pool before accepting responsibility. Future
    /// allocations spend its held capacity without seeking ancestor admission;
    /// dropping or shrinking them returns credit to this pool, not other owners.
    /// `capacity` is spendable; pool bookkeeping is additionally charged here.
    ///
    /// Ordinary funding can pay for subsequent Completion work while its entire
    /// original ordinary charge stays held. Completion funding cannot pay for
    /// Ordinary work, including through descendant budgets. One pool can serve
    /// many evaluations; their individual entitlements still require an exclusive
    /// owner-managed book. A pool alone does not prevent competing pending forks.
    ///
    /// This fixed pool retains its complete backing until the last budget handle,
    /// descendant and issued allocation disappears. A small retained result can
    /// therefore keep unused capacity held. It has no implicit trim or close and
    /// provides accounting capacity, not physical allocation or disk durability.
    pub fn funded_child(
        &self,
        funding_lane: BudgetLane,
        capacity: usize,
    ) -> Result<Self, MemoryError> {
        if capacity == 0 {
            return Err(MemoryError::InvalidConfiguration("empty funded pool"));
        }
        if funding_lane == BudgetLane::Committed {
            return Err(MemoryError::InvalidConfiguration(
                "committed work is never prepaid",
            ));
        }
        let depth = self.child_depth()?;
        let bytes = crate::checked_add(capacity, child_metadata())?;
        reserve_tree(self.level, BudgetKind::Reserved, funding_lane, bytes)?;
        Self::begin(
            &Terms {
                limit: capacity,
                ordinary_limit: match funding_lane {
                    BudgetLane::Ordinary => capacity,
                    BudgetLane::Completion | BudgetLane::Committed => 0,
                },
                hold_limit: capacity,
                depth,
                funding: Funding::Fixed {
                    lane: funding_lane,
                    bytes,
                },
            },
            Some(self),
        )
        .inspect_err(|_| {
            // This budget holds its level, so nothing ends here.
            release_tree(
                self.level,
                BudgetKind::Reserved,
                funding_lane,
                bytes,
                &mut Ended::new(),
            );
        })
    }

    pub fn reserve(
        &self,
        kind: BudgetKind,
        lane: BudgetLane,
        bytes: usize,
    ) -> Result<Reservation, MemoryError> {
        reserve_tree(self.level, kind, lane, bytes)?;
        if bytes == 0 {
            // A permit of no bytes holds its level, as a budget handle does.
            self.level.holds.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Reservation(Allocation {
            level: self.level,
            bytes,
            kind,
            lane,
        }))
    }
    /// Includes this budget itself. Used by owners to reject an independently
    /// budgeted component that would bypass their node/tenant resource limit.
    pub fn is_within(&self, ancestor: &Self) -> bool {
        lineage(self.level).any(|level| std::ptr::eq(level, ancestor.level))
    }
    /// Immutable ceiling for one reservation across this budget and every
    /// ancestor. This is not currently free space and grants no reservation.
    pub fn reservation_limit(&self, lane: BudgetLane) -> usize {
        lineage(self.level)
            .map(|level| match lane {
                BudgetLane::Ordinary => level.ordinary_limit.load(Ordering::Relaxed),
                BudgetLane::Completion => level.limit.load(Ordering::Relaxed),
                BudgetLane::Committed => level.hold_limit.load(Ordering::Relaxed),
            })
            .min()
            .unwrap_or(usize::MAX)
    }

    /// The most one reservation on `lane` could take now: at this budget and
    /// each ancestor a reservation's walk reaches (`reserve_tree`, ending at
    /// a funded pool), the least that any level's limits and an elastic
    /// pool's credit leave. A snapshot for refusing early, never a grant: a
    /// concurrent reservation or release moves it before the next one.
    pub fn headroom(&self, lane: BudgetLane) -> usize {
        let mut headroom = usize::MAX;
        for level in lineage(self.level) {
            let (funding, elastic) = level.backing.funding();
            if lane == BudgetLane::Ordinary && funding == Some(BudgetLane::Completion) {
                return 0;
            }
            if elastic {
                headroom = headroom.min(level.backing.available.load(Ordering::Acquire));
            }
            if lane == BudgetLane::Ordinary {
                headroom = headroom.min(
                    level
                        .ordinary_limit
                        .load(Ordering::Relaxed)
                        .saturating_sub(level.ordinary.load(Ordering::Acquire)),
                );
            }
            let bound = match lane {
                BudgetLane::Committed => &level.hold_limit,
                BudgetLane::Ordinary | BudgetLane::Completion => &level.limit,
            };
            headroom = headroom.min(
                bound
                    .load(Ordering::Relaxed)
                    .saturating_sub(level.total.load(Ordering::Acquire)),
            );
            if funding.is_some() {
                break;
            }
        }
        headroom
    }

    /// How many of the reservations made at this budget were refused for
    /// capacity, at it or at an ancestor, and the last one's lane, request
    /// and what was then available. A caller that reads the count before its
    /// work and finds it grown by its own refusal knows the budget refused
    /// it, not another bound that reports `Capacity`.
    pub fn refusals(&self) -> BudgetRefusals {
        let level = self.level;
        let count = level.refusals.load(Ordering::Acquire);
        BudgetRefusals {
            count,
            lane: match level.refused_lane.load(Ordering::Relaxed) {
                0 => BudgetLane::Ordinary,
                1 => BudgetLane::Completion,
                _ => BudgetLane::Committed,
            },
            requested: level.refused_requested.load(Ordering::Relaxed),
            available: level.refused_available.load(Ordering::Relaxed),
        }
    }

    /// Counters are individually exact. A concurrent allocation/drop can make
    /// this diagnostic snapshot non-atomic across categories.
    pub fn stats(&self) -> BudgetStats {
        let level = self.level;
        let limit = level.limit.load(Ordering::Relaxed);
        BudgetStats {
            limit,
            completion_reserve: limit.saturating_sub(level.ordinary_limit.load(Ordering::Relaxed)),
            used: level.total.load(Ordering::Acquire),
            ordinary_used: level.ordinary.load(Ordering::Acquire),
            by_kind: level.kinds.snapshot(),
        }
    }
}

/// What a funded child's bookkeeping takes, charged with its capacity: its
/// level's slot.
fn child_metadata() -> usize {
    size_of::<Level>()
}

fn reserve_counter(counter: &AtomicUsize, limit: usize, bytes: usize) -> Result<(), MemoryError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(bytes).filter(|next| *next <= limit)
        })
        .map(|_| ())
        .map_err(|used| MemoryError::Capacity {
            requested: bytes,
            available: limit.saturating_sub(used),
        })
}

/// Admission owns this before publication/durability. Dropping it returns every
/// byte to its original source, including early returns and unwinding. For a
/// funded source the ancestor reservation remains held for reuse by that pool.
#[derive(Debug)]
pub struct Reservation(Allocation);

impl Reservation {
    pub fn bytes(&self) -> usize {
        self.0.bytes
    }

    pub fn commit(self) -> Allocation {
        self.0
    }
}

/// Ownership of an accounted allocation. Dropping it returns capacity to its
/// source: ordinary ancestry or the nearest funded pool's still-held backing.
/// Attach it to the immutable page or other object whose lifetime it measures.
/// Its bytes keep its level, or, with none, a hold of its own.
pub struct Allocation {
    level: &'static Level,
    bytes: usize,
    kind: BudgetKind,
    lane: BudgetLane,
}

impl std::fmt::Debug for Allocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Allocation")
            .field("kind", &self.kind)
            .field("lane", &self.lane)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Allocation {
    /// Check a transfer against its exact accounting owner. An ancestor,
    /// descendant, equal-sized independent budget, category or lane cannot
    /// substitute for the permit already held by the caller.
    pub(crate) fn matches_exact(
        &self,
        source: &MemoryBudget,
        kind: BudgetKind,
        lane: BudgetLane,
        bytes: usize,
    ) -> bool {
        std::ptr::eq(self.level, source.level)
            && self.kind == kind
            && self.lane == lane
            && self.bytes == bytes
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    /// Transfer an identical budget/category/lane permit without new admission.
    /// A mismatch leaves both owners unchanged; success empties `other`.
    pub fn absorb(&mut self, other: &mut Self) -> Result<(), MemoryError> {
        if !std::ptr::eq(self.level, other.level)
            || self.kind != other.kind
            || self.lane != other.lane
        {
            return Err(MemoryError::InvalidConfiguration(
                "allocation owners differ",
            ));
        }
        let total = self
            .bytes
            .checked_add(other.bytes)
            .ok_or(MemoryError::InvalidConfiguration("allocation sum overflow"))?;
        if self.bytes != 0 && other.bytes != 0 {
            // `other` is left with no bytes, so it holds the level itself;
            // otherwise a hold moves with the bytes or none is needed.
            self.level.holds.fetch_add(1, Ordering::Relaxed);
        }
        self.bytes = total;
        other.bytes = 0;
        Ok(())
    }
    /// Grow this permit by `bytes`, admitted against its own budget, kind and
    /// lane as a fresh reservation of them is (every ancestor's bound and
    /// lane): refused, the permit is unchanged. What a value funded for what it
    /// holds while it waits takes when it is about to hold more, and gives back
    /// with `shrink_to` once it holds less.
    pub fn grow(&mut self, bytes: usize) -> Result<(), MemoryError> {
        if bytes == 0 {
            return Ok(());
        }
        self.bytes
            .checked_add(bytes)
            .ok_or(MemoryError::InvalidConfiguration("allocation sum overflow"))?;
        reserve_tree(self.level, self.kind, self.lane, bytes)?;
        let mut more = Self {
            level: self.level,
            bytes,
            kind: self.kind,
            lane: self.lane,
        };
        self.absorb(&mut more)
    }
    /// Transfer part of an existing allowance to another owned value. Counters
    /// at every ancestor remain unchanged; each resulting owner releases only
    /// its part. This performs no new admission or heap allocation.
    pub fn split_off(&mut self, bytes: usize) -> Result<Self, MemoryError> {
        let remaining = self
            .bytes
            .checked_sub(bytes)
            .ok_or(MemoryError::InvalidConfiguration(
                "allocation split exceeds owned allowance",
            ))?;
        if bytes == 0 || remaining == 0 {
            // A permit the split leaves without bytes holds the level itself.
            self.level.holds.fetch_add(1, Ordering::Relaxed);
        }
        self.bytes = remaining;
        Ok(Self {
            level: self.level,
            bytes,
            kind: self.kind,
            lane: self.lane,
        })
    }
    /// Return temporary working capacity after its payload has been discarded,
    /// while retaining the remainder across an owned result/channel handoff.
    /// Funded capacity returns to its pool. Growing requires separate admission
    /// and cannot be hidden in this method.
    pub fn shrink_to(&mut self, bytes: usize) -> Result<(), MemoryError> {
        let released = self
            .bytes
            .checked_sub(bytes)
            .ok_or(MemoryError::InvalidConfiguration(
                "allocation cannot grow without admission",
            ))?;
        if released == 0 {
            return Ok(());
        }
        if bytes == 0 {
            // Held before its bytes go, so the level never stands unheld.
            self.level.holds.fetch_add(1, Ordering::Relaxed);
        }
        // The permit keeps the level, by its bytes or its hold, and the level
        // its ancestors, so nothing ends here.
        release_tree(
            self.level,
            self.kind,
            self.lane,
            released,
            &mut Ended::new(),
        );
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        if self.bytes == 0 {
            release_hold(self.level);
            return;
        }
        let mut ended = Ended::new();
        release_tree(self.level, self.kind, self.lane, self.bytes, &mut ended);
        ended.end();
    }
}

#[cfg(test)]
mod resizing_tests {
    use super::*;
    /// A permit grows only as its budget admits, in its own kind and lane, and gives the growth
    /// back as it shrinks and drops: from bytes and from none, refused at the bound without a
    /// change, the level's usage exact after each.
    #[test]
    fn a_permit_grows_as_its_budget_admits_and_gives_it_back() {
        let budget = MemoryBudget::new(1000, 0).unwrap();
        let mut permit = budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 100)
            .unwrap()
            .commit();
        permit.grow(400).unwrap();
        assert_eq!(permit.bytes(), 500);
        assert_eq!(budget.stats().used, 500);
        assert!(matches!(
            permit.grow(501).unwrap_err(),
            MemoryError::Capacity { .. }
        ));
        assert_eq!(permit.bytes(), 500, "a refused growth changes nothing");
        assert_eq!(budget.stats().used, 500);
        permit.shrink_to(50).unwrap();
        assert_eq!(budget.stats().used, 50);
        drop(permit);
        assert_eq!(budget.stats().used, 0);
        // An empty permit grows from nothing and gives everything back.
        let mut empty = budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 0)
            .unwrap()
            .commit();
        empty.grow(300).unwrap();
        assert_eq!(budget.stats().used, 300);
        drop(empty);
        assert_eq!(budget.stats().used, 0);
        drop(budget);
    }
    /// A budget handle is one pointer and an allocation three words (its
    /// level, its bytes, and its kind and lane a byte each): every charged
    /// page, node and buffer carries one. A word-wide kind made it 32 bytes.
    #[test]
    fn a_budget_is_one_pointer_and_an_allocation_three_words() {
        assert_eq!(size_of::<MemoryBudget>(), size_of::<usize>());
        assert_eq!(size_of::<Allocation>(), 3 * size_of::<usize>());
        assert_eq!(size_of::<Reservation>(), size_of::<Allocation>());
    }
    /// A funded level's last bytes, dropped on several threads, and its last
    /// handle, dropped at the same moment, end it exactly once: its backing
    /// goes back to its parent once, never twice (the parent's counters would
    /// wrap) and never not at all (they would stay above where they began),
    /// for an elastic pool, whose credit is the last a release returns, and
    /// for a fixed one, whose total is.
    #[test]
    fn a_level_let_go_on_several_threads_at_once_ends_exactly_once() {
        let root = MemoryBudget::new(1 << 30, 0).unwrap();
        let baseline = root.stats();
        for round in 0..2_000 {
            let (pool, child) = if round % 2 == 0 {
                let pool = root
                    .elastic_funded_child(BudgetLane::Ordinary, 1 << 20, 1 << 16)
                    .unwrap();
                let child = pool.budget().clone();
                (Some(pool), child)
            } else {
                let child = root.funded_child(BudgetLane::Ordinary, 1 << 16).unwrap();
                (None, child)
            };
            let permits: Vec<Allocation> = (0..4)
                .map(|_| {
                    child
                        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 64)
                        .unwrap()
                        .commit()
                })
                .collect();
            let start = std::sync::Barrier::new(permits.len() + 1);
            std::thread::scope(|scope| {
                for permit in permits {
                    let start = &start;
                    scope.spawn(move || {
                        start.wait();
                        drop(permit);
                    });
                }
                start.wait();
                drop(child);
                drop(pool);
            });
            assert_eq!(root.stats(), baseline, "round {round}");
        }
    }
    /// A level's slot is used again once the level ends, and a budget made in
    /// it starts clean: limits, counters, lineage and funding are its own.
    #[test]
    fn an_ended_levels_slot_starts_the_next_budget_clean() {
        let root = MemoryBudget::new(1 << 20, 0).unwrap();
        let funded = root.funded_child(BudgetLane::Completion, 4096).unwrap();
        let held = funded
            .reserve(BudgetKind::Pages, BudgetLane::Completion, 100)
            .unwrap()
            .commit();
        let slot: *const Level = funded.level;
        drop(funded);
        drop(held);
        assert_eq!(root.stats().used, 0);
        // Other tests take slots too: make budgets until this one's comes back.
        let mut made = Vec::new();
        let reused = loop {
            let next = MemoryBudget::new(512, 128).unwrap();
            if std::ptr::eq(next.level, slot) {
                break next;
            }
            made.push(next);
            assert!(
                made.len() < 1 << 16,
                "the ended level's slot was never used again"
            );
        };
        let stats = reused.stats();
        assert_eq!(
            (stats.limit, stats.completion_reserve, stats.used),
            (512, 128, 0)
        );
        assert_eq!(reused.reservation_limit(BudgetLane::Committed), 512);
        assert!(stats.by_kind.iter().all(|bytes| *bytes == 0));
        assert!(!reused.is_within(&root));
        assert_eq!(reused.level.backing.lane(), None);
        let ordinary = reused
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 384)
            .unwrap();
        assert!(
            reused
                .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 1)
                .is_err()
        );
        drop(ordinary);
    }
    /// A replica admits within its limits and applies within its hold (doc 31 M3): fresh work
    /// stops at the ordinary limit, completing work at the limit, and what a committed fact
    /// requires at the hold, each refusal counted in its own lane; the ceilings and the headroom
    /// say the same, and an ancestor still bounds what the hold allows.
    #[test]
    fn committed_work_takes_the_hold_and_admitted_work_its_limits() {
        let root = MemoryBudget::new(1_000, 100).unwrap();
        let replica = root.child_holding(400, 50, 800).unwrap();
        assert_eq!(replica.reservation_limit(BudgetLane::Ordinary), 350);
        assert_eq!(replica.reservation_limit(BudgetLane::Completion), 400);
        assert_eq!(replica.reservation_limit(BudgetLane::Committed), 800);
        let fresh = replica
            .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 350)
            .unwrap();
        assert!(
            replica
                .reserve(BudgetKind::Pages, BudgetLane::Ordinary, 1)
                .is_err()
        );
        assert_eq!(replica.refusals().lane, BudgetLane::Ordinary);
        let completing = replica
            .reserve(BudgetKind::Pages, BudgetLane::Completion, 50)
            .unwrap();
        assert_eq!(replica.headroom(BudgetLane::Completion), 0);
        assert!(
            replica
                .reserve(BudgetKind::Pages, BudgetLane::Completion, 1)
                .is_err()
        );
        assert_eq!(replica.headroom(BudgetLane::Committed), 400);
        let applied = replica
            .reserve(BudgetKind::Pages, BudgetLane::Committed, 400)
            .unwrap();
        assert_eq!(replica.stats().used, 800);
        assert_eq!(root.stats().used, 800);
        let refused = replica.refusals().count;
        assert!(
            replica
                .reserve(BudgetKind::Pages, BudgetLane::Committed, 1)
                .is_err()
        );
        let refusal = replica.refusals();
        assert_eq!(refusal.count, refused + 1);
        assert_eq!(refusal.lane, BudgetLane::Committed);
        // Committed work is never charged to the ordinary counter: what a replica applies leaves
        // its fresh admissions where they were.
        assert_eq!(replica.stats().ordinary_used, 350);
        drop((fresh, completing, applied));
        assert_eq!(root.stats().used, 0);
        // A hold beyond an ancestor's limit is held to the ancestor's.
        let wide = root.child_holding(400, 50, 5_000).unwrap();
        assert_eq!(wide.reservation_limit(BudgetLane::Committed), 1_000);
        let all = wide
            .reserve(BudgetKind::Pages, BudgetLane::Committed, 1_000)
            .unwrap();
        assert!(
            wide.reserve(BudgetKind::Pages, BudgetLane::Committed, 1)
                .is_err()
        );
        drop(all);
        // A budget made without a hold holds committed work to its limit.
        let plain = root.child(400, 50).unwrap();
        assert_eq!(plain.reservation_limit(BudgetLane::Committed), 400);
    }
    /// A hold below the limit is no allowance, and committed work is never prepaid: a pool funded
    /// in that lane is refused, fixed or elastic.
    #[test]
    fn a_hold_below_the_limit_and_a_committed_prepayment_are_refused() {
        let root = MemoryBudget::new(1_000, 0).unwrap();
        assert!(matches!(
            root.child_holding(400, 50, 399),
            Err(MemoryError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            root.funded_child(BudgetLane::Committed, 64),
            Err(MemoryError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            root.elastic_funded_child(BudgetLane::Committed, 64, 0),
            Err(MemoryError::InvalidConfiguration(_))
        ));
        assert_eq!(root.stats().used, 0, "a refused pool charges nothing");
        // Committed work spends a pool funded in either other lane.
        let pool = root.funded_child(BudgetLane::Completion, 64).unwrap();
        let spent = pool
            .reserve(BudgetKind::Pages, BudgetLane::Committed, 64)
            .unwrap();
        drop((spent, pool));
    }
    #[test]
    fn immutable_reservation_ceiling_includes_every_ancestor_and_ignores_usage() {
        let parent = MemoryBudget::new(100, 30).unwrap();
        let child = parent.child(200, 150).unwrap();
        assert_eq!(child.reservation_limit(BudgetLane::Ordinary), 50);
        assert_eq!(child.reservation_limit(BudgetLane::Completion), 100);
        let pressure = parent
            .reserve(BudgetKind::Payload, BudgetLane::Completion, 99)
            .unwrap();
        assert_eq!(child.reservation_limit(BudgetLane::Ordinary), 50);
        assert_eq!(child.reservation_limit(BudgetLane::Completion), 100);
        assert!(
            child
                .reserve(BudgetKind::Payload, BudgetLane::Completion, 2)
                .is_err()
        );
        drop(pressure);
    }
    #[test]
    fn split_transfers_ownership_without_readmission_or_changing_ancestor_totals() {
        let parent = MemoryBudget::new(100, 0).unwrap();
        let child = parent.child(100, 0).unwrap();
        let mut first = child
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 100)
            .unwrap()
            .commit();
        assert!(first.split_off(101).is_err());
        assert_eq!(first.bytes(), 100);
        let mut second = first.split_off(60).unwrap();
        assert_eq!(first.bytes(), 40);
        assert_eq!(parent.stats().used, 100);
        assert_eq!(child.stats().used, 100);
        drop(first);
        assert_eq!(parent.stats().used, 60);
        second.shrink_to(20).unwrap();
        assert_eq!(child.stats().used, 20);
        assert_eq!(parent.stats().used, 20);
        drop(second);
        assert_eq!(parent.stats().used, 0);
    }
    #[test]
    fn hierarchy_enforces_each_ancestor_and_rolls_back_failed_admission() {
        let node = MemoryBudget::new(100, 20).unwrap();
        let tenant = node.child(90, 10).unwrap();
        let first = tenant.child(70, 10).unwrap();
        let second = tenant.child(70, 10).unwrap();
        let independent = MemoryBudget::new(100, 20).unwrap();
        assert!(first.is_within(&node) && first.is_within(&tenant));
        assert!(!first.is_within(&second) && !first.is_within(&independent));
        let mut held = first
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 60)
            .unwrap()
            .commit();
        assert!(
            first
                .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 1)
                .is_err()
        );
        assert!(
            second
                .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 21)
                .is_err()
        );
        assert_eq!(second.stats().used, 0);
        assert_eq!(tenant.stats().used, 60);
        assert_eq!(node.stats().used, 60);
        let completion = second
            .reserve(BudgetKind::Control, BudgetLane::Completion, 30)
            .unwrap()
            .commit();
        assert_eq!(node.stats().used, 90);
        assert!(
            second
                .reserve(BudgetKind::Control, BudgetLane::Completion, 1)
                .is_err()
        );
        let spare = node
            .reserve(BudgetKind::Control, BudgetLane::Completion, 10)
            .unwrap()
            .commit();
        assert_eq!(node.stats().used, 100);
        held.shrink_to(10).unwrap();
        assert_eq!(first.stats().used, 10);
        assert_eq!(tenant.stats().used, 40);
        assert_eq!(node.stats().used, 50);
        drop(tenant);
        drop(first);
        drop(second);
        assert_eq!(node.stats().used, 50);
        drop(held);
        drop(completion);
        drop(spare);
        assert_eq!(node.stats().used, 0);
        assert_eq!(node.stats().ordinary_used, 0);
        assert!(node.stats().by_kind.iter().all(|bytes| *bytes == 0));
    }
    #[test]
    fn concurrent_siblings_share_one_bound_without_allocation_wrappers() {
        let node = MemoryBudget::new(200, 20).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let child = node.child(100, 10).unwrap();
                let parent = &node;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        if let Ok(held) =
                            child.reserve(BudgetKind::Payload, BudgetLane::Ordinary, 37)
                        {
                            assert!(parent.stats().used <= 200);
                            std::thread::yield_now();
                            drop(held);
                        }
                    }
                });
            }
        });
        assert_eq!(node.stats().used, 0);
        let mut deepest = node.clone();
        for _ in 1..8 {
            deepest = deepest.child(200, 20).unwrap();
        }
        assert!(deepest.child(200, 20).is_err());
    }
    #[test]
    fn owned_shrink_releases_only_discarded_capacity_and_cannot_grow() {
        let budget = MemoryBudget::new(100, 10).unwrap();
        let mut first = budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 90)
            .unwrap()
            .commit();
        assert!(first.shrink_to(91).is_err());
        assert_eq!(budget.stats().used, 90);
        first.shrink_to(40).unwrap();
        let second = budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 50)
            .unwrap()
            .commit();
        assert_eq!(budget.stats().used, 90);
        drop(first);
        assert_eq!(budget.stats().used, 50);
        drop(second);
        assert_eq!(budget.stats().used, 0);
        assert_eq!(budget.stats().ordinary_used, 0);
        assert!(budget.stats().by_kind.iter().all(|bytes| *bytes == 0));
    }
}
