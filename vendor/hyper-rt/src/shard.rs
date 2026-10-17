//! What a task sees of its shard: the desk (docs/runtime.md §3.4).
//!
//! A shard is split in two so that a poll can never reach the loop's state. The loop's state (the task
//! futures, the timing wheel, the driver, the counters) is owned by value by the loop's frame
//! ([`crate::shard_loop::Shard`]) and lent `&mut` down the step; no task holds a reference to it. What a
//! polled task may ask of its shard, it asks here, through [`ShardContext`]: spawn, cancel, detach, join,
//! arm and disarm a timer, register readiness interest, wake a task. Each request is an **intent** written
//! into a structure sized at the shard's build, which the loop applies between polls and before it parks.
//! Nothing nests and nothing is borrowed: every field is a `Cell` (or a [`CellRing`] / [`CellStack`] of
//! them), values moved in and out, and the only refusals are a full or an empty structure.
//!
//! Two requests must be answered at once, so they are not intents: a spawn returns its [`TaskId`], and a
//! sleep its [`TimerId`], before the loop runs again. Each identifier is popped from a free stack the loop
//! refills (slates kept the free lists inside its `RefCell`-borrowed slab).
//!
//! A task reaches the desk through the thread's current-shard pointer, valid for the span of a step
//! ([`crate::registry::with_current`]).

use std::cell::Cell;
use std::task::Poll;

use crate::cells::{CellRing, CellStack};
use crate::error::RtError;
use crate::mem::Encoded;
use crate::queue::LocalQueue;
use crate::registry::{self, Entry, SlotHolder};
use crate::task::{BoxedFuture, Outcome, TaskFuture};

/// A shard's process-wide id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId(pub u16);

/// A task's id: its packed word (shard, slot, generation).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskId(pub Encoded);

impl TaskId {
    /// The owning shard.
    pub fn shard(&self) -> ShardId {
        ShardId(self.0.shard())
    }
}

/// A timer's id: its slot on the shard's wheel and the slot's generation when it was taken, so a sleep
/// that outlived its timer (fired, the slot reused) cannot disarm the next holder's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimerId {
    slot: u32,
    generation: u32,
}

/// Where a task slot is in its life, as the desk publishes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// No task: the slot is on the free stack (or retired).
    Free,
    /// A task lives in it (possibly not yet installed by the loop).
    Live,
    /// The task is terminal; its outcome waits for a joiner.
    Done,
}

/// Format: "no task is being polled" in [`ShardContext`]'s current word (no task word is all ones: its
/// generation field would be `Encoded::ANY_GENERATION` and its shard `u16::MAX`, which the registry never
/// issues).
pub(crate) const NO_TASK: u64 = u64::MAX;

/// What a task asked of its slot since the loop last looked (bits of [`TaskCell::asks`]).
pub(crate) mod ask {
    /// A future waits in the slot's incoming cell to be installed.
    pub(crate) const INSTALL: u8 = 1;
    /// Cancellation was requested.
    pub(crate) const CANCEL: u8 = 1 << 1;
    /// The task was detached.
    pub(crate) const DETACH: u8 = 1 << 2;
    /// A joiner took the task's outcome: the slot may be reaped.
    pub(crate) const JOINED: u8 = 1 << 3;
    /// A poller waits in the slot's poller cell to be registered.
    pub(crate) const POLLER: u8 = 1 << 4;
}

/// A future waiting to be installed in its slot, with how it was spawned.
pub(crate) struct Incoming {
    pub(crate) future: TaskFuture,
    pub(crate) parent: Option<u32>,
    pub(crate) joinable: bool,
    pub(crate) service: bool,
}

/// A poller's readiness question: a task that owns a ring the loop cannot see (a client's command ring in
/// shared memory, slates §4.3) asks to be woken whenever it says yes.
pub type PollerReady = Box<dyn Fn() -> bool>;

/// One task slot's desk side.
pub(crate) struct TaskCell {
    pub(crate) generation: Cell<u32>,
    pub(crate) phase: Cell<Phase>,
    pub(crate) outcome: Cell<Option<Outcome>>,
    /// The waiting joiner's task word, if one waits.
    pub(crate) joiner: Cell<Option<Encoded>>,
    /// What the task asked since the loop last looked ([`ask`]).
    pub(crate) asks: Cell<u8>,
    /// Whether the slot is on the dirty ring.
    pub(crate) dirty: Cell<bool>,
    pub(crate) incoming: Cell<Option<Incoming>>,
    pub(crate) poller: Cell<Option<PollerReady>>,
    /// Whether the slot waits on the timer-waiter ring.
    pub(crate) waits_for_timer: Cell<bool>,
    /// A service owns cancellation cleanup until its future returns Ready.
    pub(crate) service: Cell<bool>,
    /// Level-triggered: every cancellation path sets it before waking this task.
    pub(crate) cancelled: Cell<bool>,
    /// Direct children are cancelled before a service begins cleanup, once per admission.
    pub(crate) cancel_children: Cell<bool>,
    /// Whether the task was spawned under a parent: linking it at its install and unlinking it at its reap
    /// are the loop's, so its install and its reap are asked of the loop; a task with no parent is listed at
    /// its spawn, installed at its first poll, and reaped by its join.
    pub(crate) parented: Cell<bool>,
}

/// Where a timer slot is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimerPhase {
    /// On the free stack.
    Free,
    /// Taken by a sleep; its deadline waits to be armed.
    Claimed,
    /// Armed on the wheel.
    Armed,
}

/// One timer slot's desk side.
pub(crate) struct TimerCell {
    pub(crate) generation: Cell<u32>,
    pub(crate) phase: Cell<TimerPhase>,
    pub(crate) deadline_ns: Cell<u64>,
    pub(crate) word: Cell<u64>,
    pub(crate) dirty: Cell<bool>,
}

/// A readiness registration a task asked for, or the withdrawal of one whose wait dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Interest {
    pub(crate) raw: i32,
    pub(crate) writable: bool,
    pub(crate) word: Encoded,
    /// The wait's own slot: two waits of one task on one handle and direction are two waits.
    pub(crate) ticket: Ticket,
    /// A wait whose future dropped before it fired, leaving the table.
    pub(crate) withdraw: bool,
}

/// One readiness wait's name: its slot in the desk's wait table and that slot's generation when the wait
/// took it, so a fire, a refusal or a withdrawal meant for an ended wait never reaches the slot's next one
/// (mantle's final review, findings 1 and 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ticket {
    /// The registration of the shard whose desk holds the wait: a `Ready` may be dropped, or polled again,
    /// on another shard or off any, and a ticket is honoured only by its own desk (mantle's final review,
    /// second pass, finding 4: by slot and generation alone, another shard's live wait was taken for it).
    /// Every runtime shard's desk is registered, so a ticket always names one (the third pass, D: an
    /// optional owner let two unregistered desks accept each other's tickets).
    owner: crate::registry::SlotHolder,
    slot: u32,
    generation: u32,
}

impl Ticket {
    /// The registration of the shard whose desk holds the wait.
    pub(crate) fn owner(self) -> crate::registry::SlotHolder {
        self.owner
    }

    /// The wait's slot.
    pub(crate) fn slot(self) -> u32 {
        self.slot
    }

    /// The slot's generation when the wait took it.
    pub(crate) fn generation(self) -> u32 {
        self.generation
    }
}

#[cfg(test)]
impl Ticket {
    /// A ticket for a table test that has no desk.
    pub(crate) fn for_test(slot: u32) -> Ticket {
        Ticket {
            owner: crate::registry::SlotHolder::for_test(),
            slot,
            generation: 0,
        }
    }
}

/// Where a readiness wait stands. Only a fire makes a wait ready: a task woken for another reason (a timer
/// that a race also waits on, a sibling of a join) finds its wait still `Armed` and keeps waiting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WaitPhase {
    Free,
    /// Registered, not yet fired.
    Armed,
    /// The handle fired in the wait's direction (or a simulated socket became ready).
    Fired,
    /// The loop could not register it; the refusal is in the cell.
    Refused,
    /// Its future dropped while armed: the loop withdraws it from the table and frees the slot. A member
    /// dropped while on its group's list stays there, abandoned, until the group takes it off.
    Abandoned,
    /// The slot holds a group's list of the members that fired (`crate::readiness::ReadyGroup`), and
    /// registers nothing itself.
    Group,
}

/// No wait: the end of a group's list.
const NO_WAIT: u32 = u32::MAX;

/// One slot of the desk's wait table.
struct WaitCell {
    generation: Cell<u32>,
    phase: Cell<WaitPhase>,
    refusal: Cell<Option<RtError>>,
    /// What the wait waits on (`None`: a simulated socket) and for whom, so the shard can end a wait
    /// dropped elsewhere from its slot alone.
    raw: Cell<Option<i32>>,
    writable: Cell<bool>,
    word: Cell<Option<Encoded>>,
    /// The group a member joins when it fires or is refused: the group's slot and that slot's
    /// generation when the member was armed; `None` for a lone wait.
    group: Cell<Option<(u32, u32)>>,
    /// The key the member's owner gave it, which its group hands back when it fired.
    key: Cell<u64>,
    /// Whether the member is on its group's list, which only the group takes it off.
    listed: Cell<bool>,
    /// The next wait on the list (`NO_WAIT`: none): a member's successor, or a group's first.
    next: Cell<u32>,
    /// A group's last member listed (`NO_WAIT`: none).
    last: Cell<u32>,
}

/// The values a shard keeps for its life, filled before its first step and immutable afterwards (slates'
/// per-shard singletons: a socket's demultiplexer, a fleet identity), dropped with the shard last-in
/// first-out.
#[derive(Default)]
pub(crate) struct KeptValues(Vec<Box<dyn std::any::Any>>);

impl Drop for KeptValues {
    fn drop(&mut self) {
        while let Some(value) = self.0.pop() {
            drop(value);
        }
    }
}

/// A value a shard keeps for its life, named by its shard's registration and its place among the kept
/// values (slates' `Kept`, AUD-29-08): `Copy` and `'static`, so tasks share it freely and it can be held
/// anywhere without harm. The value is reached only inside [`Kept::with`], which lends `&T` for a closure's
/// span while the owning shard runs on this thread, and answers `None` for a shard that ended or is not
/// the one running here.
///
/// The lend cannot leave its closure:
///
/// ```compile_fail,E0521
/// fn escape(kept: hyper_rt::shard::Kept<String>) -> Option<&'static String> {
///   kept.with(|value| value)
/// }
/// ```
pub struct Kept<T: 'static> {
    context: SlotHolder,
    index: usize,
    value: std::marker::PhantomData<fn() -> T>,
}

impl<T> Clone for Kept<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Kept<T> {}

impl<T> PartialEq for Kept<T> {
    fn eq(&self, other: &Self) -> bool {
        self.context == other.context && self.index == other.index
    }
}

impl<T> Eq for Kept<T> {}

impl<T> std::fmt::Debug for Kept<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Kept")
            .field("shard", &self.context.shard())
            .field("index", &self.index)
            .finish()
    }
}

impl<T: 'static> Kept<T> {
    /// Runs `f` on the value, on the shard running on this thread: `None` when no shard runs here or the one
    /// running is not the value's (another shard's, or a later one on the same registry slot).
    pub fn with<R>(self, f: impl FnOnce(&T) -> R) -> Option<R> {
        registry::with_current(|context| self.with_in(context, f)).flatten()
    }

    /// Runs `f` on the value through `context` (an owner between steps): `None` when `context` is not the
    /// value's shard.
    pub fn with_in<R>(self, context: &ShardContext, f: impl FnOnce(&T) -> R) -> Option<R> {
        if context.incarnation != Some(self.context) {
            return None;
        }
        let value = context.kept.0.get(self.index)?.downcast_ref::<T>()?;
        Some(f(value))
    }
}

/// The shard as its tasks see it: the desk.
///
/// Laid out in order (`repr(C)`), from a 128-byte line (Apple silicon's, the largest we target): what a
/// poll, a yield and [`ShardContext::wanted`] read fills the first line, the run queue and the counters a
/// poll counts the second, then the intents a task pushes, and last what only admission, the root, spawns
/// and the simulation touch. A consumer's operations run between polls and evict the loop's lines, so a
/// switch pays for every line it touches (docs/runtime.md §3.10): two, where the compiler's own order
/// spread the same fields over six with the frames between them.
#[repr(C, align(128))]
pub struct ShardContext {
    // The first line: what a poll, a yield and `wanted` read.
    /// The word of the task being polled, or [`NO_TASK`]: its slot for the local wake's test, its whole word
    /// for a future that asks which task polls it (`crate::waker::polling_task`), with nothing to pack.
    pub(crate) current: Cell<u64>,
    /// The shard's clock at the start of the current poll (or step), published by the loop.
    pub(crate) now_ns: Cell<u64>,
    /// The loop's last reading of the driver's clock with the counter's ticks then, which a live reading
    /// carries forward (`machine::clock::Carried`); `None` where the shard reads no counter, and the driver's
    /// clock takes every reading.
    pub(crate) carried: Cell<Option<crate::machine::clock::Carried>>,
    /// The latest live reading handed out, so none is earlier than one before it: each step reads the
    /// driver's clock afresh, and a scale off by a fraction could carry a reading past the next step's.
    latest_ns: Cell<u64>,
    /// The earliest time the loop has timed work of its own, kept by the loop as it changes
    /// (docs/runtime.md §3.10): the wheel's next occupied tick, the driver's next harvest while readiness
    /// waits are registered, or 0 while the driver or a poller has work for every turn; `u64::MAX` with
    /// none. [`ShardContext::wanted`] compares the clock against it.
    pub(crate) due_ns: Cell<u64>,
    /// When the polling task last asked [`ShardContext::wanted`] and was told no, by the shard's clock: the
    /// last time the shard let it go on, from which the loop judges how long it held the shard (a long poll
    /// is one held past twice the quantum since then, docs/runtime.md §3.11).
    pub(crate) checked_ns: Cell<u64>,
    /// The step quantum now, published by the loop.
    pub(crate) quantum_ns: Cell<u64>,
    /// The driver's clock, read live.
    pub(crate) clock: crate::driver::Clock,
    pub(crate) entry: Option<&'static Entry>,
    /// The shard id.
    pub id: u16,
    /// Whether a task asked the loop for something since the loop last applied the desk's intents: set by
    /// every push onto `dirty_tasks`, `dirty_timers` and `interests`, so the loop's look after each poll is
    /// one load when nothing was asked.
    pub(crate) asked: Cell<bool>,
    /// Whether the poll running now served a client's work: the loop then moves `activity_ns` to the
    /// poll's end.
    pub(crate) activity_noted: Cell<bool>,
    /// Whether the shard's driver is the simulation's.
    pub(crate) is_sim: bool,
    /// Admission is paused while owned futures are cancelled, and closed at shutdown.
    pub(crate) accepting: Cell<bool>,
    // The second: the run queue, and the counters a poll counts (`LiveCounters` leads with them).
    /// The local run queue.
    pub(crate) local: LocalQueue,
    /// The loop's counters, which it counts here in place (`crate::shard_loop::LiveCounters`).
    pub(crate) live: crate::shard_loop::LiveCounters,
    // What a task asks the loop: the intents and their tables.
    pub(crate) tasks: Box<[TaskCell]>,
    pub(crate) free_tasks: CellStack<u32>,
    pub(crate) dirty_tasks: CellRing<u32>,
    pub(crate) timers: Box<[TimerCell]>,
    pub(crate) free_timers: CellStack<u32>,
    pub(crate) dirty_timers: CellRing<u32>,
    pub(crate) timer_waiters: CellRing<u32>,
    /// Registrations and withdrawals for the loop, drained after every poll. Twice the wait slots: one
    /// poll pushes at most two intents per slot — a slot a fired wait frees on the desk is taken once more,
    /// registered and abandoned, and is then held `Abandoned` until the loop has drained — so a push is
    /// never refused and a withdrawal never lost (mantle's final review, finding 2).
    pub(crate) interests: CellRing<Interest>,
    /// The readiness waits, one slot each, bounded by `interests_per_shard`.
    waits: Box<[WaitCell]>,
    free_waits: CellStack<u32>,
    // What admission, the root, the clock's overrun, spawns and the simulation touch.
    /// When the server last served a client's work here, opening the idle window (`note_activity`).
    pub(crate) activity_ns: Cell<Option<u64>>,
    /// The measured scheduler overrun, published by the loop.
    pub(crate) scheduler_overrun_ns: Cell<u64>,
    /// The timer wheel's tick: a deadline is kept on the tick after it.
    timer_tick_ns: u64,
    /// Spawns refused because no slot was free (folded into the loop's counters).
    pub(crate) refused_spawns: Cell<u64>,
    /// Sleeps that found every timer taken and waited for one (folded into the loop's counters).
    pub(crate) timer_waits: Cell<u64>,
    pub(crate) incarnation: Option<SlotHolder>,
    pub(crate) kept: KeptValues,
    /// The output of a [`crate::runtime::LocalRuntime::block_on`] root, moved here by the root as it finishes
    /// and taken by the runtime: written once, taken once.
    pub(crate) root_output: Cell<Option<Box<dyn std::any::Any>>>,
    /// The task a running `block_on` waits on, so the shard can tell it that task's poll unwound.
    pub(crate) root: Cell<Option<TaskId>>,
    /// Set by the shard when the root's poll unwound and its task ended `Outcome::Unwound`: no output will
    /// come, and `block_on` is refused rather than wait for one.
    pub(crate) root_unwound: Cell<bool>,
    /// One past the highest task generation issued: what the registry slot's next holder starts from. On the
    /// desk, so a join that frees its task's slot raises it as the loop's frees do.
    pub(crate) generation_high: Cell<u32>,
    /// The frames the shard's own spawns write their futures into (`crate::frames`).
    pub(crate) frames: crate::frames::Frames,
    /// A simulated shard's sockets (docs/runtime.md §11); `None` on an OS driver.
    pub(crate) sim: Option<crate::sim::SimSockets>,
}

impl std::fmt::Debug for ShardContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardContext")
            .field("id", &self.id)
            .finish()
    }
}

/// How a desk is sized: from the runtime's configuration.
pub(crate) struct DeskShape {
    pub(crate) tasks: usize,
    pub(crate) timers: usize,
    pub(crate) interests: usize,
    pub(crate) generation_base: u32,
    pub(crate) timer_tick_ns: u64,
}

impl ShardContext {
    /// A desk for shard `id`.
    pub(crate) fn new(
        id: u16,
        shape: &DeskShape,
        is_sim: bool,
        clock: crate::driver::Clock,
    ) -> Self {
        let slot_ids = |count: usize| {
            (0..count)
                .rev()
                .filter_map(|slot| u32::try_from(slot).ok())
                .collect::<Vec<u32>>()
        };
        let task_ids = slot_ids(shape.tasks);
        let timer_ids = slot_ids(shape.timers);
        let wait_ids = slot_ids(shape.interests);
        Self {
            id,
            local: LocalQueue::new(shape.tasks),
            frames: crate::frames::Frames::new(shape.tasks),
            tasks: (0..shape.tasks)
                .map(|_| TaskCell {
                    generation: Cell::new(shape.generation_base),
                    phase: Cell::new(Phase::Free),
                    outcome: Cell::new(None),
                    joiner: Cell::new(None),
                    asks: Cell::new(0),
                    dirty: Cell::new(false),
                    incoming: Cell::new(None),
                    poller: Cell::new(None),
                    waits_for_timer: Cell::new(false),
                    service: Cell::new(false),
                    cancelled: Cell::new(false),
                    cancel_children: Cell::new(false),
                    parented: Cell::new(false),
                })
                .collect(),
            free_tasks: CellStack::full_of(task_ids.into_iter()),
            dirty_tasks: CellRing::new(shape.tasks),
            timers: (0..shape.timers)
                .map(|_| TimerCell {
                    generation: Cell::new(0),
                    phase: Cell::new(TimerPhase::Free),
                    deadline_ns: Cell::new(0),
                    word: Cell::new(0),
                    dirty: Cell::new(false),
                })
                .collect(),
            free_timers: CellStack::full_of(timer_ids.into_iter()),
            dirty_timers: CellRing::new(shape.timers),
            timer_waiters: CellRing::new(shape.tasks),
            interests: CellRing::new(shape.interests.saturating_mul(2)),
            waits: (0..shape.interests)
                .map(|_| WaitCell {
                    generation: Cell::new(0),
                    phase: Cell::new(WaitPhase::Free),
                    refusal: Cell::new(None),
                    raw: Cell::new(None),
                    writable: Cell::new(false),
                    word: Cell::new(None),
                    group: Cell::new(None),
                    key: Cell::new(0),
                    listed: Cell::new(false),
                    next: Cell::new(NO_WAIT),
                    last: Cell::new(NO_WAIT),
                })
                .collect(),
            free_waits: CellStack::full_of(wait_ids.into_iter()),
            asked: Cell::new(false),
            current: Cell::new(NO_TASK),
            accepting: Cell::new(true),
            now_ns: Cell::new(0),
            activity_ns: Cell::new(None),
            activity_noted: Cell::new(false),
            quantum_ns: Cell::new(1),
            due_ns: Cell::new(u64::MAX),
            checked_ns: Cell::new(0),
            scheduler_overrun_ns: Cell::new(0),
            timer_tick_ns: shape.timer_tick_ns,
            refused_spawns: Cell::new(0),
            timer_waits: Cell::new(0),
            is_sim,
            clock,
            carried: Cell::new(None),
            latest_ns: Cell::new(0),
            entry: registry::entry(id),
            incarnation: registry::entry(id).map(Entry::holder),
            kept: KeptValues::default(),
            root_output: Cell::new(None),
            root: Cell::new(None),
            root_unwound: Cell::new(false),
            generation_high: Cell::new(shape.generation_base),
            live: crate::shard_loop::LiveCounters::default(),
            sim: None,
        }
    }

    /// Moves a `block_on` root's output to where its runtime takes it.
    pub(crate) fn put_root_output(&self, output: Box<dyn std::any::Any>) {
        self.root_output.set(Some(output));
    }

    /// Takes a `block_on` root's output, once it finished.
    pub(crate) fn take_root_output(&self) -> Option<Box<dyn std::any::Any>> {
        self.root_output.take()
    }

    /// `task` is the root a `block_on` waits on, until [`ShardContext::end_root`].
    pub(crate) fn begin_root(&self, task: TaskId) {
        self.root.set(Some(task));
        self.root_unwound.set(false);
    }

    /// The `block_on` waiting on its root returns.
    pub(crate) fn end_root(&self) {
        self.root.set(None);
        self.root_unwound.set(false);
    }

    /// Task `task`'s poll unwound and it ended `Outcome::Unwound`: if it is the root, its `block_on` is told.
    pub(crate) fn note_unwound(&self, task: TaskId) {
        if self.root.get() == Some(task) {
            self.root_unwound.set(true);
        }
    }

    /// Whether the root's poll unwound.
    pub(crate) fn root_unwound(&self) -> bool {
        self.root_unwound.get()
    }

    /// Keeps `value` for the shard's life and hands back its [`Kept`] handle: for an owner between steps
    /// (the runtime that built the shard), before or between its runs. Refused (`NotOnShardThread`) for a
    /// shard with no registration to name.
    pub(crate) fn keep<T: 'static>(&mut self, value: T) -> Result<Kept<T>, RtError> {
        let context = self.incarnation.ok_or(RtError::NotOnShardThread)?;
        let index = self.kept.0.len();
        self.kept.0.push(Box::new(value));
        Ok(Kept {
            context,
            index,
            value: std::marker::PhantomData,
        })
    }

    // ------------------------------------------------------------------ what tasks read

    /// The shard's clock in nanoseconds, read live: from the counter carrying the loop's last reading of
    /// its driver's clock forward where the shard reads one (`machine::clock::counter_ticks`), else from the
    /// driver's clock (the time the loop last published where the driver has none). Never earlier than a
    /// reading before it.
    pub fn now_ns(&self) -> u64 {
        let now = match self.clock {
            crate::driver::Clock::Since(epoch) => {
                match (self.carried.get(), crate::machine::clock::counter_ticks()) {
                    (Some(carried), Some(ticks)) => carried.now_ns(ticks),
                    _ => crate::driver::nanos_since(epoch),
                }
            }
            crate::driver::Clock::Sim(shared) => return shared.now_ns(),
            crate::driver::Clock::Published => return self.now_ns.get(),
        };
        let now = now.max(self.latest_ns.get());
        self.latest_ns.set(now);
        now
    }

    /// The task being polled on this shard right now.
    pub fn current_task(&self) -> Option<TaskId> {
        let word = self.current.get();
        (word != NO_TASK).then(|| TaskId(Encoded::from_word(word)))
    }

    /// The slot of the task being polled, if one is.
    pub(crate) fn current_slot(&self) -> Option<u32> {
        let word = self.current.get();
        (word != NO_TASK).then(|| Encoded::from_word(word).slot())
    }

    /// The step quantum (docs/runtime.md §3.4): the online wake estimate while the shard tracks one, else its
    /// configured step budget. A cooperative operation sizes its slices by it.
    pub fn quantum_ns(&self) -> u64 {
        self.quantum_ns.get()
    }

    /// Whether anything other than the task polling now wants this shard (docs/runtime.md §3.10): a task
    /// listed or yielded, an intent the loop has yet to apply, a wake or a control message from another
    /// thread, or a timer or the driver's harvest falling due. The test a task that runs operations back
    /// to back makes between them ([`crate::futures::maybe_yield`]): no system call, the desk's fields, the
    /// entry's two flags and the counter. A simulated shard always answers true, so its schedules stay a
    /// step a yield. An answer of no is the shard letting the task go on: the loop judges the poll's hold
    /// from the last one (`checked_ns`).
    pub fn wanted(&self) -> bool {
        if self.is_sim
            || !self.local.is_empty()
            || self.asked.get()
            || self
                .entry
                .is_some_and(|entry| entry.wakes.is_pending() || entry.control_pending.is_pending())
        {
            return true;
        }
        let now = self.now_ns();
        if now >= self.due_ns.get() {
            return true;
        }
        // Told no: the task may go on, and the loop judges its hold from here (`checked_ns`).
        self.checked_ns.set(now);
        false
    }

    /// The online wake estimate, nanoseconds (the quantum).
    pub fn wake_cost_ns(&self) -> u64 {
        self.quantum_ns()
    }

    /// The measured scheduler overrun, nanoseconds: how late the shard's steps have run after its waits.
    pub fn scheduler_overrun_ns(&self) -> u64 {
        self.scheduler_overrun_ns.get()
    }

    /// The timer wheel's tick, nanoseconds: a sleep ends on the tick after its deadline, so up to a tick
    /// later than due by design (and later still by the scheduler's overrun).
    pub fn timer_tick_ns(&self) -> u64 {
        self.timer_tick_ns
    }

    /// The shard's counters as they stand now: the loop counts on the desk in place, so a task reads what the
    /// loop has counted up to its poll.
    pub fn counters(&self) -> crate::shard_loop::Counters {
        let mut counters = self.live.snapshot();
        counters.admission_refused = counters
            .admission_refused
            .saturating_add(self.refused_spawns.get());
        counters.timer_waits = counters.timer_waits.saturating_add(self.timer_waits.get());
        counters.scheduler_overrun_ns = self.scheduler_overrun_ns.get();
        counters.wake_cost_ns = self.quantum_ns.get();
        counters
    }

    /// Whether this shard runs the simulation driver.
    pub fn driver_is_sim(&self) -> bool {
        self.is_sim
    }

    /// Notes that a client's request was just served here (slates §4.7): the shard spins out the idle window
    /// before it parks, so the client's next request within it costs no kernel wake. Every path that hands a
    /// request to a task marks it (docs/runtime.md §3.4). Marked during a poll, the window opens where that
    /// poll ends; marked between polls, it opens at the time the loop last published.
    pub fn note_activity(&self) {
        self.activity_ns.set(Some(self.now_ns.get()));
        self.activity_noted.set(true);
    }

    /// Marks one period of forward progress of an application loop on this shard, for an observer on any
    /// thread (`registry::Pulse::progress`).
    pub fn beat_progress(&self) {
        if let Some(entry) = self.entry {
            entry.pulse.beat();
        }
    }

    // ------------------------------------------------------------------ what tasks ask

    /// Owner-published terminal driver state; channel task wakes remain live.
    pub(crate) fn driver_lost(&self) -> bool {
        self.entry.is_some_and(|entry| {
            entry
                .cleanup_mode
                .load(std::sync::atomic::Ordering::Acquire)
        })
    }

    /// Spawns a joinable task under `parent` (a slot of this shard), answered at once with its id; the loop
    /// installs it before the next poll. Refused `TooManyTasks` when no slot is free.
    pub fn spawn_local(&self, future: BoxedFuture, parent: Option<u32>) -> Result<TaskId, RtError> {
        self.claim(Incoming {
            future: TaskFuture::Boxed(future),
            parent,
            joinable: true,
            service: false,
        })
    }

    /// Spawns a detached task on this shard: nobody joins it, and its slot is freed when it ends.
    pub fn spawn_detached(&self, future: BoxedFuture) -> Result<TaskId, RtError> {
        self.claim(Incoming {
            future: TaskFuture::Boxed(future),
            parent: None,
            joinable: false,
            service: false,
        })
    }

    /// Spawns a joinable service. Cancellation wakes the future; it must await
    /// `futures::cancelled`, retire its external ownership, and return Ready.
    pub(crate) fn spawn_service_local(
        &self,
        future: BoxedFuture,
        parent: Option<u32>,
    ) -> Result<TaskId, RtError> {
        self.claim(Incoming {
            future: TaskFuture::Boxed(future),
            parent,
            joinable: true,
            service: true,
        })
    }

    /// Spawns `future` written into a frame of this shard's (`crate::frames`): under `parent` (a slot of this
    /// shard), joinable or detached, a service or not. The spawns a task makes on its own shard
    /// (`crate::futures`); refused as a claim is, and `Mem` when no frame could be had.
    pub(crate) fn spawn_framed<F: std::future::Future<Output = ()> + 'static>(
        &self,
        future: F,
        parent: Option<u32>,
        joinable: bool,
        service: bool,
    ) -> Result<TaskId, RtError> {
        let future = TaskFuture::Framed(self.frames.put(future)?);
        self.claim(Incoming {
            future,
            parent,
            joinable,
            service,
        })
    }

    /// Claims a free slot for `incoming`: a task with a parent is installed by the loop when it next applies
    /// the desk's intents, which links it under its parent before that parent's poll is recorded; one with no
    /// parent is listed now, and the loop installs it as it first polls it.
    pub(crate) fn claim(&self, incoming: Incoming) -> Result<TaskId, RtError> {
        if !self.accepting.get()
            || self.entry.is_some_and(|entry| {
                !entry.accepting.load(std::sync::atomic::Ordering::Acquire)
                    || entry.stop.load(std::sync::atomic::Ordering::Acquire)
                    || entry.exited.load(std::sync::atomic::Ordering::Acquire)
            })
        {
            return Err(RtError::ShardGone { shard: self.id });
        }
        let capacity = self.tasks.len();
        let Some(slot) = self.free_tasks.pop() else {
            self.refused_spawns
                .set(self.refused_spawns.get().saturating_add(1));
            return Err(RtError::TooManyTasks { capacity });
        };
        let Some(cell) = self.task(slot) else {
            return Err(RtError::TooManyTasks { capacity });
        };
        let generation = cell.generation.get();
        let Some(word) = Encoded::pack(self.id, slot, generation) else {
            return Err(RtError::TooManyTasks { capacity });
        };
        cell.phase.set(Phase::Live);
        cell.service.set(incoming.service);
        cell.cancelled.set(false);
        cell.cancel_children.set(false);
        cell.outcome.set(None);
        cell.joiner.set(None);
        let parented = incoming.parent.is_some();
        cell.parented.set(parented);
        cell.incoming.set(Some(incoming));
        if parented {
            self.ask(slot, ask::INSTALL);
        } else {
            self.local.push(slot);
        }
        // Counted at its admission, here, whenever the loop installs it (`Counters::spawns`).
        self.live
            .spawns
            .set(self.live.spawns.get().saturating_add(1));
        Ok(TaskId(word))
    }

    /// Gives a slot back on the desk's side: emptied, a new generation (so no word minted for its last task
    /// names the next), its intents dropped (an intent asked of its last task, still on the dirty ring, must
    /// not reach the next one), and onto the free stack, unless its generations are spent (the slot then
    /// retires, AUD-29-11). A slot already free is left as it is: freeing it twice would list it twice.
    pub(crate) fn free_slot(&self, slot: u32) {
        let Some(cell) = self.task(slot) else {
            return;
        };
        if cell.phase.get() == Phase::Free {
            return;
        }
        let next = cell.generation.get().saturating_add(1);
        cell.generation.set(next);
        cell.phase.set(Phase::Free);
        cell.outcome.set(None);
        cell.joiner.set(None);
        cell.asks.set(0);
        self.generation_high
            .set(self.generation_high.get().max(next));
        if next < Encoded::TASK_GENERATION_LIMIT {
            let _ = self.free_tasks.push(slot);
        }
    }

    /// Requests a task's cancellation; it terminates at its next poll boundary. Refused for a stale id.
    pub fn cancel(&self, id: TaskId) -> Result<(), RtError> {
        let slot = self.live(id)?;
        if let Some(cell) = self.task(slot) {
            cell.cancelled.set(true);
        }
        self.ask(slot, ask::CANCEL);
        self.local.push(slot);
        Ok(())
    }

    /// Detaches a joinable task: its slot is reaped at termination (or now, if terminal).
    pub fn detach(&self, id: TaskId) -> Result<(), RtError> {
        let slot = self.live(id)?;
        self.ask(slot, ask::DETACH);
        Ok(())
    }

    /// Polls a join: `Ready(outcome)` once the task is terminal (its slot is reaped then), else `Pending`
    /// with the polling task recorded as the joiner. Refused for a stale id, and for a joiner that is not a
    /// task of this shard (`NotOnShardThread`): the joiner is woken by its word.
    pub fn poll_join(&self, id: TaskId, joiner: Option<Encoded>) -> Poll<Result<Outcome, RtError>> {
        let slot = match self.live(id) {
            Ok(slot) => slot,
            Err(refusal) => return Poll::Ready(Err(refusal)),
        };
        let Some(cell) = self.task(slot) else {
            return Poll::Ready(Err(stale(id)));
        };
        if cell.phase.get() == Phase::Done {
            if cell.asks.get() & ask::JOINED != 0 {
                return Poll::Ready(Err(stale(id)));
            }
            let outcome = cell.outcome.get().unwrap_or(Outcome::Cancelled);
            if cell.parented.get() {
                // Unlinking it from its parent is the loop's.
                self.ask(slot, ask::JOINED);
            } else {
                // The loop's side of a done task holds nothing to let go (its future dropped as it ended): its
                // slot is the desk's to free, now.
                self.free_slot(slot);
            }
            return Poll::Ready(Ok(outcome));
        }
        match joiner {
            Some(word) => {
                cell.joiner.set(Some(word));
                Poll::Pending
            }
            None => Poll::Ready(Err(RtError::NotOnShardThread)),
        }
    }

    /// Takes a timer slot for `word` at `deadline_ns`; the loop arms it before the shard next waits. `None`
    /// when every timer is taken (the sleep then waits for one to free, [`Self::wait_for_timer`]).
    pub fn arm_timer(&self, deadline_ns: u64, word: u64) -> Option<TimerId> {
        if self.driver_lost() {
            return None;
        }
        let slot = self.free_timers.pop()?;
        let cell = self.timer(slot)?;
        cell.phase.set(TimerPhase::Claimed);
        cell.deadline_ns.set(deadline_ns);
        cell.word.set(word);
        self.timer_dirty(slot, cell);
        Some(TimerId {
            slot,
            generation: cell.generation.get(),
        })
    }

    /// Lets a timer go: it is free again at once (a sleep later in the same poll may take it), and the loop
    /// takes its old deadline off the wheel before the slot is armed again. A timer that already fired (its
    /// slot freed, perhaps taken again) is left alone.
    pub fn disarm_timer(&self, id: TimerId) {
        let Some(cell) = self.timer(id.slot) else {
            return;
        };
        if cell.generation.get() != id.generation
            || !matches!(cell.phase.get(), TimerPhase::Claimed | TimerPhase::Armed)
        {
            return;
        }
        self.release_timer(id.slot, cell);
        self.timer_dirty(id.slot, cell);
    }

    /// Gives timer slot `slot` back: a new generation (so its last holder cannot touch the next), onto the
    /// free stack, and the oldest task waiting for a timer woken.
    pub(crate) fn release_timer(&self, slot: u32, cell: &TimerCell) {
        cell.generation.set(cell.generation.get().wrapping_add(1));
        cell.phase.set(TimerPhase::Free);
        let _ = self.free_timers.push(slot);
        while let Some(waiter) = self.timer_waiters.pop() {
            let Some(task) = self.task(waiter) else {
                continue;
            };
            task.waits_for_timer.set(false);
            if task.phase.get() == Phase::Live {
                self.wake_local(waiter);
                break;
            }
        }
    }

    /// Whether the timer `id` names is still this sleep's and has not fired.
    pub fn timer_pending(&self, id: TimerId) -> bool {
        self.timer(id.slot).is_some_and(|cell| {
            cell.generation.get() == id.generation
                && matches!(cell.phase.get(), TimerPhase::Claimed | TimerPhase::Armed)
        })
    }

    /// Queues the task `word` names to be woken when a timer frees (a sleep that found every timer taken,
    /// AUD-29-39): it waits, never completing before its deadline. A task queued already is not queued twice.
    pub fn wait_for_timer(&self, word: Encoded) -> Result<(), RtError> {
        if self.driver_lost() {
            return Err(RtError::DriverLost);
        }
        let slot = word.slot();
        let cell = self
            .task(slot)
            .filter(|cell| {
                cell.generation.get() == word.generation() && cell.phase.get() == Phase::Live
            })
            .ok_or(RtError::StaleTask {
                slot,
                generation: word.generation(),
            })?;
        if !cell.waits_for_timer.replace(true) {
            if self.timer_waiters.push(slot).is_err() {
                cell.waits_for_timer.set(false);
                return Err(RtError::Capacity {
                    what: "timer waiters",
                    bound: self.timer_waiters.capacity(),
                });
            }
            self.timer_waits
                .set(self.timer_waits.get().saturating_add(1));
        }
        Ok(())
    }

    /// Takes a wait slot for a readiness wait: its ticket, or `Capacity` when `interests_per_shard` waits
    /// are outstanding.
    pub(crate) fn take_wait(
        &self,
        raw: Option<i32>,
        writable: bool,
        word: Encoded,
    ) -> Result<Ticket, RtError> {
        // Both native and simulated readiness enter here; no new ticket survives
        // driver loss, including simulated sockets that bypass register_interest.
        if self.driver_lost() {
            return Err(RtError::DriverLost);
        }
        let owner = self.incarnation.ok_or(RtError::NotOnShardThread)?;
        let slot = self.free_waits.pop().ok_or(RtError::Capacity {
            what: "readiness waits",
            bound: self.waits.len(),
        })?;
        let cell = self.wait_cell(slot).ok_or(RtError::Capacity {
            what: "readiness waits",
            bound: self.waits.len(),
        })?;
        cell.phase.set(WaitPhase::Armed);
        cell.raw.set(raw);
        cell.writable.set(writable);
        cell.word.set(Some(word));
        Ok(Ticket {
            owner,
            slot,
            generation: cell.generation.get(),
        })
    }

    fn wait_cell(&self, slot: u32) -> Option<&WaitCell> {
        self.waits.get(usize::try_from(slot).ok()?)
    }

    /// The cell `ticket` names, while that wait holds it: a ticket of another desk names nothing here.
    fn wait_of(&self, ticket: Ticket) -> Option<&WaitCell> {
        if Some(ticket.owner) != self.incarnation {
            return None;
        }
        self.wait_cell(ticket.slot)
            .filter(|cell| cell.generation.get() == ticket.generation)
    }

    /// Whether `ticket` is this desk's.
    pub(crate) fn owns(&self, ticket: Ticket) -> bool {
        Some(ticket.owner) == self.incarnation
    }

    /// Ends the waits of this shard dropped on another shard or off any, from their marks on the registry
    /// entry ([`crate::registry::Entry::mark_abandoned`]): each as if dropped here. Only the slots whose
    /// summary bit is set are visited, O(slots / 64 + marked). A mark of an ended generation names nothing.
    /// Returns the marks visited.
    pub(crate) fn sweep_abandoned(&self) -> u64 {
        let (Some(entry), Some(owner)) = (self.entry, self.incarnation) else {
            return 0;
        };
        let mut visited = 0u64;
        for (index, summary) in entry.abandoned_summary.iter().enumerate() {
            let mut bits = summary.swap(0, std::sync::atomic::Ordering::AcqRel);
            while bits != 0 {
                let bit = bits.trailing_zeros();
                bits &= bits.wrapping_sub(1);
                let Some(slot) = u32::try_from(index)
                    .ok()
                    .and_then(|index| index.checked_mul(u64::BITS))
                    .and_then(|base| base.checked_add(bit))
                else {
                    continue;
                };
                visited = visited.saturating_add(1);
                self.sweep_slot(entry, owner, slot);
            }
        }
        visited
    }

    /// Ends the wait in `slot` if its mark names its live generation.
    fn sweep_slot(&self, entry: &Entry, owner: crate::registry::SlotHolder, slot: u32) {
        let Some(mark) = usize::try_from(slot)
            .ok()
            .and_then(|slot| entry.abandoned.get(slot))
        else {
            return;
        };
        let marked = mark.swap(0, std::sync::atomic::Ordering::AcqRel);
        let Some(generation) = marked
            .checked_sub(1)
            .and_then(|generation| u32::try_from(generation).ok())
        else {
            return;
        };
        let ticket = Ticket {
            owner,
            slot,
            generation,
        };
        let Some(cell) = self.wait_of(ticket) else {
            return;
        };
        let Some(word) = cell.word.get() else {
            return;
        };
        self.abandon_interest(cell.raw.get(), cell.writable.get(), word, ticket);
    }

    /// Gives a wait's slot back: its generation moves on, so the ended wait's ticket names nothing.
    fn free_wait(&self, ticket: Ticket) {
        if let Some(cell) = self.wait_of(ticket) {
            cell.generation.set(cell.generation.get().wrapping_add(1));
            cell.phase.set(WaitPhase::Free);
            cell.refusal.set(None);
            cell.group.set(None);
            cell.key.set(0);
            cell.listed.set(false);
            cell.next.set(NO_WAIT);
            cell.last.set(NO_WAIT);
            // The slot came off this stack, which holds every slot: room for it.
            let _ = self.free_waits.push(ticket.slot);
        }
    }

    /// Registers one-shot interest in `raw`'s readability (or writability) for the task `word` names: the
    /// wait's ticket. When the handle fires in that direction the loop marks the wait fired and wakes the
    /// task. Refused `Capacity` when every wait slot is taken.
    pub fn register_interest(
        &self,
        raw: i32,
        writable: bool,
        word: Encoded,
    ) -> Result<Ticket, RtError> {
        if self.driver_lost() {
            return Err(RtError::DriverLost);
        }
        let ticket = self.take_wait(Some(raw), writable, word)?;
        self.push_interest(raw, writable, word, ticket)
    }

    /// Queues the wait `ticket` names for the loop to arm on `raw`: its ticket, or `Capacity` (its slot
    /// given back) were the registrations' ring full.
    fn push_interest(
        &self,
        raw: i32,
        writable: bool,
        word: Encoded,
        ticket: Ticket,
    ) -> Result<Ticket, RtError> {
        let pushed = self.interests.push(Interest {
            raw,
            writable,
            word,
            ticket,
            withdraw: false,
        });
        self.asked.set(true);
        if pushed.is_err() {
            // Unreachable by the ring's sizing (see `interests`); refused rather than lost.
            self.free_wait(ticket);
            return Err(RtError::Capacity {
                what: "readiness registrations",
                bound: self.interests.capacity(),
            });
        }
        Ok(ticket)
    }

    /// What became of the wait `ticket` names: `None` while it waits; `Some(Ok)` once it fired, or
    /// `Some(Err)` when it was refused — both end the wait and give its slot back. A ticket that names
    /// nothing reads as fired: the caller retries its call, which says what holds.
    pub fn interest_outcome(&self, ticket: Ticket) -> Option<Result<(), RtError>> {
        let Some(cell) = self.wait_of(ticket) else {
            return Some(Ok(()));
        };
        if self.driver_lost() {
            self.free_wait(ticket);
            return Some(Err(RtError::DriverLost));
        }
        let outcome = match cell.phase.get() {
            WaitPhase::Armed => return None,
            WaitPhase::Refused => Err(cell.refusal.take().unwrap_or(RtError::Capacity {
                what: "readiness registrations",
                bound: self.waits.len(),
            })),
            WaitPhase::Fired | WaitPhase::Free | WaitPhase::Abandoned => Ok(()),
            // A group's slot is no lone wait's: it reads as fired, and only its group frees it.
            WaitPhase::Group => return Some(Ok(())),
        };
        self.free_wait(ticket);
        Some(outcome)
    }

    /// Ends a wait whose future dropped. An armed OS wait is marked abandoned and its withdrawal queued; the
    /// loop takes it out of the table and frees the slot. A fired or refused wait, or a simulated one (whose
    /// socket fires through `fire_wait`, which a stale ticket misses), gives its slot back now.
    pub fn abandon_interest(
        &self,
        raw: Option<i32>,
        writable: bool,
        word: Encoded,
        ticket: Ticket,
    ) {
        let Some(cell) = self.wait_of(ticket) else {
            return;
        };
        match (cell.phase.get(), raw) {
            (WaitPhase::Group, _) => self.group_close(ticket),
            // On its group's list, whose links run through it: left there, abandoned, for the group to
            // free as it takes it off (`group_next`, `group_close`).
            (WaitPhase::Fired | WaitPhase::Refused, _) if cell.listed.get() => {
                cell.phase.set(WaitPhase::Abandoned);
            }
            (WaitPhase::Armed, Some(raw)) => {
                cell.phase.set(WaitPhase::Abandoned);
                let pushed = self.interests.push(Interest {
                    raw,
                    writable,
                    word,
                    ticket,
                    withdraw: true,
                });
                self.asked.set(true);
                // Unreachable by the ring's sizing (see `interests`). Were it refused, the slot would stay
                // abandoned, held against the bound, until the handle fires and `fire_wait` frees it.
                let _ = pushed;
            }
            (WaitPhase::Abandoned, _) => {}
            _ => self.free_wait(ticket),
        }
    }

    /// The loop (or a simulated socket) reports the wait `ticket` names ready: an armed wait is marked
    /// fired and its task woken; an abandoned one is freed. True when a task was woken.
    pub(crate) fn fire_wait(&self, ticket: Ticket, word: Encoded) -> bool {
        let Some(cell) = self.wait_of(ticket) else {
            return false;
        };
        match cell.phase.get() {
            WaitPhase::Armed => {
                cell.phase.set(WaitPhase::Fired);
                self.list(ticket.slot, cell);
                self.wake_local(word.slot());
                true
            }
            WaitPhase::Abandoned => {
                self.free_wait(ticket);
                false
            }
            _ => false,
        }
    }

    /// The loop refused to register the wait `ticket` names: an armed wait carries the refusal to its task,
    /// woken; an abandoned one is freed.
    pub(crate) fn refuse_wait(&self, ticket: Ticket, word: Encoded, refusal: RtError) {
        let Some(cell) = self.wait_of(ticket) else {
            return;
        };
        match cell.phase.get() {
            WaitPhase::Armed => {
                cell.phase.set(WaitPhase::Refused);
                cell.refusal.set(Some(refusal));
                self.list(ticket.slot, cell);
                self.wake_local(word.slot());
            }
            WaitPhase::Abandoned => self.free_wait(ticket),
            _ => {}
        }
    }

    /// Puts the member in `slot`, which just fired or was refused, last on its group's list, while the
    /// group it was armed in lives; a lone wait, or a member whose group ended, joins none.
    fn list(&self, slot: u32, cell: &WaitCell) {
        let Some((group, generation)) = cell.group.get() else {
            return;
        };
        let Some(head) = self.wait_cell(group).filter(|head| {
            head.phase.get() == WaitPhase::Group && head.generation.get() == generation
        }) else {
            return;
        };
        cell.next.set(NO_WAIT);
        cell.listed.set(true);
        match self.wait_cell(head.last.get()) {
            Some(last) => last.next.set(slot),
            None => head.next.set(slot),
        }
        head.last.set(slot);
    }

    /// Opens a group of readiness waits for the task `word` names (`crate::readiness::ReadyGroup`): a
    /// wait slot of its own holding the list of its members that fired. Refused `Capacity` when every
    /// slot is taken.
    pub(crate) fn group_open(&self, word: Encoded) -> Result<Ticket, RtError> {
        let ticket = self.take_wait(None, false, word)?;
        if let Some(cell) = self.wait_of(ticket) {
            cell.phase.set(WaitPhase::Group);
        }
        Ok(ticket)
    }

    /// Whether `group` names a group open on this desk.
    pub(crate) fn group_live(&self, group: Ticket) -> bool {
        self.wait_of(group)
            .is_some_and(|cell| cell.phase.get() == WaitPhase::Group)
    }

    /// Arms a member of `group`: a one-shot wait on `raw` (`None`: a simulated socket, which its caller
    /// registers) for the task `word` names, keyed `key`, which joins the group's list when it fires or
    /// is refused. Its ticket; `Capacity` when every slot is taken, `NotOnShardThread` for a group that
    /// is not open here.
    pub(crate) fn group_member(
        &self,
        group: Ticket,
        raw: Option<i32>,
        writable: bool,
        word: Encoded,
        key: u64,
    ) -> Result<Ticket, RtError> {
        if self.driver_lost() {
            return Err(RtError::DriverLost);
        }
        if !self.group_live(group) {
            return Err(RtError::NotOnShardThread);
        }
        let ticket = self.take_wait(raw, writable, word)?;
        if let Some(cell) = self.wait_of(ticket) {
            cell.group.set(Some((group.slot, group.generation)));
            cell.key.set(key);
        }
        match raw {
            Some(raw) => self.push_interest(raw, writable, word, ticket),
            None => Ok(ticket),
        }
    }

    /// Takes the first member off `group`'s list: its key and outcome (`Ok` once its handle fired, the
    /// refusal once its registration was refused), its wait ended and its slot given back. `None` when
    /// the list is empty or the group is not open here; members abandoned while listed are freed on
    /// the way. `DriverLost` once the shard's driver is gone.
    pub(crate) fn group_next(
        &self,
        group: Ticket,
    ) -> Result<Option<crate::readiness::Fired>, RtError> {
        if self.driver_lost() {
            return Err(RtError::DriverLost);
        }
        let Some(head) = self
            .wait_of(group)
            .filter(|cell| cell.phase.get() == WaitPhase::Group)
        else {
            return Ok(None);
        };
        // Bounded: each turn takes one member off the list, which holds at most the table's slots.
        for _ in 0..self.waits.len() {
            let Some((slot, cell)) = self.unlist_first(head) else {
                return Ok(None);
            };
            let ticket = Ticket {
                owner: group.owner,
                slot,
                generation: cell.generation.get(),
            };
            let outcome = match cell.phase.get() {
                WaitPhase::Fired => Ok(()),
                WaitPhase::Refused => Err(cell.refusal.take().unwrap_or(RtError::Capacity {
                    what: "readiness registrations",
                    bound: self.waits.len(),
                })),
                // Abandoned while listed: its owner is gone.
                _ => {
                    self.free_wait(ticket);
                    continue;
                }
            };
            let key = cell.key.get();
            self.free_wait(ticket);
            return Ok(Some((key, outcome)));
        }
        Ok(None)
    }

    /// Takes the first member off the list `head` holds: its slot and cell.
    fn unlist_first(&self, head: &WaitCell) -> Option<(u32, &WaitCell)> {
        let slot = head.next.get();
        let Some(cell) = self.wait_cell(slot) else {
            head.next.set(NO_WAIT);
            head.last.set(NO_WAIT);
            return None;
        };
        let after = cell.next.get();
        head.next.set(after);
        if after == NO_WAIT {
            head.last.set(NO_WAIT);
        }
        cell.listed.set(false);
        cell.next.set(NO_WAIT);
        Some((slot, cell))
    }

    /// Closes `group`: the members on its list are let go, their slots given back, and its own slot
    /// with them. A member still armed stays armed until its owner drops it; firing, it joins no list.
    pub(crate) fn group_close(&self, group: Ticket) {
        let Some(head) = self
            .wait_of(group)
            .filter(|cell| cell.phase.get() == WaitPhase::Group)
        else {
            return;
        };
        // Bounded: each turn lets one listed member go, of at most the table's slots. Each is fired,
        // refused or abandoned: none is in the loop's table any more.
        for _ in 0..self.waits.len() {
            let Some((slot, cell)) = self.unlist_first(head) else {
                break;
            };
            self.free_wait(Ticket {
                owner: group.owner,
                slot,
                generation: cell.generation.get(),
            });
        }
        self.free_wait(group);
    }

    /// Whether the wait `ticket` names was abandoned before the loop registered it (the loop then frees it
    /// instead of arming the handle for no one).
    pub(crate) fn wait_abandoned(&self, ticket: Ticket) -> bool {
        self.wait_of(ticket)
            .is_some_and(|cell| cell.phase.get() == WaitPhase::Abandoned)
    }

    /// The loop withdrew an abandoned wait from its table: the slot is given back.
    pub(crate) fn withdrawn(&self, ticket: Ticket) {
        if self.wait_abandoned(ticket) {
            self.free_wait(ticket);
        }
    }

    /// Wait slots held now (a test's view of the table's bound).
    pub fn waits_held(&self) -> usize {
        self.waits.len().saturating_sub(self.free_waits.len())
    }

    /// Registers `task` as a poller: the loop wakes it whenever `ready` says so (slates' client rings).
    /// Refused when the task is not live on this shard.
    pub fn register_poller(&self, task: TaskId, ready: PollerReady) -> Result<(), RtError> {
        let slot = self.live(task)?;
        if let Some(cell) = self.task(slot) {
            cell.poller.set(Some(ready));
            self.ask(slot, ask::POLLER);
        }
        Ok(())
    }

    /// Wakes a task of this shard by its slot. A task that wakes itself in its own poll (a yield, or any
    /// future with more to do) waits for the loop's next reading of its inputs (`crate::queue`). Inlined: every
    /// waker of this shard's tasks on its own thread ends here.
    #[inline]
    pub(crate) fn wake_local(&self, slot: u32) {
        let current = self.current.get();
        if current != NO_TASK && Encoded::from_word(current).slot() == slot {
            self.local.defer(slot);
        } else {
            self.local.push(slot);
        }
    }

    // ------------------------------------------------------------------ helpers

    pub(crate) fn task(&self, slot: u32) -> Option<&TaskCell> {
        self.tasks.get(usize::try_from(slot).ok()?)
    }

    pub(crate) fn timer(&self, slot: u32) -> Option<&TimerCell> {
        self.timers.get(usize::try_from(slot).ok()?)
    }

    /// The slot of a live or terminal task `id` names, or its refusal.
    fn live(&self, id: TaskId) -> Result<u32, RtError> {
        if id.0.shard() != self.id {
            return Err(stale(id));
        }
        let slot = id.0.slot();
        match self.task(slot) {
            Some(cell)
                if cell.generation.get() == id.0.generation()
                    && cell.phase.get() != Phase::Free =>
            {
                Ok(slot)
            }
            _ => Err(stale(id)),
        }
    }

    /// Records `what` on the slot and puts it on the dirty ring once.
    pub(crate) fn ask(&self, slot: u32, what: u8) {
        let Some(cell) = self.task(slot) else {
            return;
        };
        cell.asks.set(cell.asks.get() | what);
        // The ring holds one entry per slot and a slot is on it at most once (its dirty flag), so it never
        // refuses; were it to, the flag stays clear and the next ask lists the slot again.
        if !cell.dirty.replace(true) && self.dirty_tasks.push(slot).is_err() {
            cell.dirty.set(false);
        }
        self.asked.set(true);
    }

    fn timer_dirty(&self, slot: u32, cell: &TimerCell) {
        if !cell.dirty.replace(true) && self.dirty_timers.push(slot).is_err() {
            cell.dirty.set(false);
        }
        self.asked.set(true);
    }
}

/// The refusal for an id that names no live task.
pub(crate) fn stale(id: TaskId) -> RtError {
    RtError::StaleTask {
        slot: id.0.slot(),
        generation: id.0.generation(),
    }
}

/// Pins a boxed future for admission.
pub fn boxed<F: std::future::Future<Output = ()> + 'static>(future: F) -> BoxedFuture {
    Box::pin(future)
}
