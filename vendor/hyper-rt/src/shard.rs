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
use crate::task::{BoxedFuture, Outcome};

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
    pub(crate) future: BoxedFuture,
    pub(crate) parent: Option<u32>,
    pub(crate) joinable: bool,
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
    /// Its future dropped while armed: the loop withdraws it from the table and frees the slot.
    Abandoned,
}

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
pub struct ShardContext {
    /// The shard id.
    pub id: u16,
    /// The local run queue.
    pub(crate) local: LocalQueue,
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
    /// The task being polled.
    pub(crate) current_task: Cell<Option<u32>>,
    /// Admission is paused while owned futures are cancelled, and closed at shutdown.
    pub(crate) accepting: Cell<bool>,
    /// The shard's clock at the start of the current poll (or step), published by the loop.
    pub(crate) now_ns: Cell<u64>,
    /// When the server last served a client's work here, opening the idle window (`note_activity`).
    pub(crate) activity_ns: Cell<Option<u64>>,
    /// The step quantum now, published by the loop.
    pub(crate) quantum_ns: Cell<u64>,
    /// The measured scheduler overrun, published by the loop.
    pub(crate) scheduler_overrun_ns: Cell<u64>,
    /// Spawns refused because no slot was free (folded into the loop's counters).
    pub(crate) refused_spawns: Cell<u64>,
    /// Sleeps that found every timer taken and waited for one (folded into the loop's counters).
    pub(crate) timer_waits: Cell<u64>,
    /// Whether the shard's driver is the simulation's.
    pub(crate) is_sim: bool,
    /// The driver's clock, read live.
    pub(crate) clock: crate::driver::Clock,
    pub(crate) entry: Option<&'static Entry>,
    pub(crate) incarnation: Option<SlotHolder>,
    pub(crate) kept: KeptValues,
    /// The output of a [`crate::runtime::LocalRuntime::block_on`] root, moved here by the root as it finishes
    /// and taken by the runtime: written once, taken once.
    pub(crate) root_output: Cell<Option<Box<dyn std::any::Any>>>,
    /// The loop's counters as of the start of the current step, published for tasks that report them.
    pub(crate) counters: Cell<crate::shard_loop::Counters>,
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
                })
                .collect(),
            free_waits: CellStack::full_of(wait_ids.into_iter()),
            current_task: Cell::new(None),
            accepting: Cell::new(true),
            now_ns: Cell::new(0),
            activity_ns: Cell::new(None),
            quantum_ns: Cell::new(1),
            scheduler_overrun_ns: Cell::new(0),
            refused_spawns: Cell::new(0),
            timer_waits: Cell::new(0),
            is_sim,
            clock,
            entry: registry::entry(id),
            incarnation: registry::entry(id).map(Entry::holder),
            kept: KeptValues::default(),
            root_output: Cell::new(None),
            counters: Cell::new(crate::shard_loop::Counters::default()),
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

    /// The shard's clock in nanoseconds, read live from its driver's clock (the time the loop last published
    /// where the driver has none).
    pub fn now_ns(&self) -> u64 {
        match self.clock {
            crate::driver::Clock::Since(epoch) => crate::driver::nanos_since(epoch),
            crate::driver::Clock::Sim(shared) => shared.now_ns(),
            crate::driver::Clock::Published => self.now_ns.get(),
        }
    }

    /// The task being polled on this shard right now.
    pub fn current_task(&self) -> Option<TaskId> {
        let slot = self.current_task.get()?;
        let generation = self.task(slot)?.generation.get();
        Encoded::pack(self.id, slot, generation).map(TaskId)
    }

    /// The step quantum (docs/runtime.md §3.4): the online wake estimate while the shard tracks one, else its
    /// configured step budget. A cooperative operation sizes its slices by it.
    pub fn quantum_ns(&self) -> u64 {
        self.quantum_ns.get()
    }

    /// The online wake estimate, nanoseconds (the quantum).
    pub fn wake_cost_ns(&self) -> u64 {
        self.quantum_ns()
    }

    /// The measured scheduler overrun, nanoseconds: how late the shard's steps have run after its waits.
    pub fn scheduler_overrun_ns(&self) -> u64 {
        self.scheduler_overrun_ns.get()
    }

    /// The shard's counters as of the start of the current step.
    pub fn counters(&self) -> crate::shard_loop::Counters {
        self.counters.get()
    }

    /// Whether this shard runs the simulation driver.
    pub fn driver_is_sim(&self) -> bool {
        self.is_sim
    }

    /// Notes that a client's request was just served here (slates §4.7): the shard spins out the idle window
    /// from now before it parks, so the client's next request within it costs no kernel wake. Every path that
    /// hands a request to a task marks it (docs/runtime.md §3.4).
    pub fn note_activity(&self) {
        self.activity_ns.set(Some(self.now_ns.get()));
    }

    /// Marks one period of forward progress of an application loop on this shard, for an observer on any
    /// thread (`registry::Pulse::progress`).
    pub fn beat_progress(&self) {
        if let Some(entry) = self.entry {
            entry.pulse.beat();
        }
    }

    // ------------------------------------------------------------------ what tasks ask

    /// Spawns a joinable task under `parent` (a slot of this shard), answered at once with its id; the loop
    /// installs it before the next poll. Refused `TooManyTasks` when no slot is free.
    pub fn spawn_local(&self, future: BoxedFuture, parent: Option<u32>) -> Result<TaskId, RtError> {
        self.claim(Incoming {
            future,
            parent,
            joinable: true,
        })
    }

    /// Spawns a detached task on this shard: nobody joins it, and its slot is freed when it ends.
    pub fn spawn_detached(&self, future: BoxedFuture) -> Result<TaskId, RtError> {
        self.claim(Incoming {
            future,
            parent: None,
            joinable: false,
        })
    }

    /// Claims a free slot for `incoming` and asks the loop to install it.
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
        cell.outcome.set(None);
        cell.joiner.set(None);
        cell.incoming.set(Some(incoming));
        self.ask(slot, ask::INSTALL);
        Ok(TaskId(word))
    }

    /// Requests a task's cancellation; it terminates at its next poll boundary. Refused for a stale id.
    pub fn cancel(&self, id: TaskId) -> Result<(), RtError> {
        let slot = self.live(id)?;
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
            self.ask(slot, ask::JOINED);
            return Poll::Ready(Ok(cell.outcome.get().unwrap_or(Outcome::Cancelled)));
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
        let ticket = self.take_wait(Some(raw), writable, word)?;
        let pushed = self.interests.push(Interest {
            raw,
            writable,
            word,
            ticket,
            withdraw: false,
        });
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
        let outcome = match cell.phase.get() {
            WaitPhase::Armed => return None,
            WaitPhase::Refused => Err(cell.refusal.take().unwrap_or(RtError::Capacity {
                what: "readiness registrations",
                bound: self.waits.len(),
            })),
            WaitPhase::Fired | WaitPhase::Free | WaitPhase::Abandoned => Ok(()),
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
            (WaitPhase::Armed, Some(raw)) => {
                cell.phase.set(WaitPhase::Abandoned);
                let pushed = self.interests.push(Interest {
                    raw,
                    writable,
                    word,
                    ticket,
                    withdraw: true,
                });
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
                self.wake_local(word.slot());
            }
            WaitPhase::Abandoned => self.free_wait(ticket),
            _ => {}
        }
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

    /// Wakes a task of this shard by its slot.
    pub(crate) fn wake_local(&self, slot: u32) {
        self.local.push(slot);
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
    }

    fn timer_dirty(&self, slot: u32, cell: &TimerCell) {
        if !cell.dirty.replace(true) && self.dirty_timers.push(slot).is_err() {
            cell.dirty.set(false);
        }
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
