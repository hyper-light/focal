//! The shard's loop: one thread, one task table, one run queue, one timing wheel, one driver, and the
//! loop that ties them (slates §4.3, "Loop"; docs/runtime.md §3.4).
//!
//! Each step: drain the control channel and the wake bitmap (spawns, cancels, shutdown, wakes from other
//! threads) and the driver's completions into the run queue; expire timers; poll ready tasks, at most a
//! batch of them, applying what each poll asked of the desk before the next; then, if nothing is ready,
//! spin out the idle window that client activity opened, and park in the driver until a kick, a completion
//! or the next deadline. Cancellation guarantees a terminal completion: the future is dropped at the next
//! poll boundary, the task's children are cancelled and joined, and whoever joins it sees `Cancelled`.
//! The watchdog counts polls that exceed the step quantum (the expected wake) and attributes each to its
//! task or to the host ([`crate::attribution`]).
//!
//! **Ownership.** [`Shard`] owns everything here by value: its desk (boxed, so the address the thread's
//! current-shard pointer names is stable) and its loop state. A step lends the desk to the tasks it polls
//! and keeps the loop state to itself; between polls it applies the desk's intents. There is no `unsafe`,
//! no `RefCell` and nothing a nested call could find borrowed.

use std::pin::Pin;
use std::sync::mpsc::Receiver;
use std::task::{Context, Poll};

use crate::attribution::{self, Attribution, Tracker};
use crate::control::Control;
use crate::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use crate::error::RtError;
use crate::interests::{self, Interests, Readiness};
use crate::machine::wake::WakeEstimate;
use crate::mem::Encoded;
use crate::parking::{Parked, Woken};
use crate::registry::{self, Entry};
use crate::runtime::RuntimeConfig;
use crate::shard::{
    DeskShape, Incoming, Kept, Phase, ShardContext, ShardId, TaskId, TimerPhase, ask,
};
use crate::task::{Admission, BoxedFuture, NO_LINK, Outcome, SpawnRequest, State, TaskSlot};
use crate::timer::Wheel;
use crate::waker::waker_for;

/// What one loop iteration did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepOutcome {
    /// Whether any message, timer or task was processed.
    pub did_work: bool,
    /// The next timer deadline, in the driver's nanoseconds.
    pub next_deadline_ns: Option<u64>,
    /// Whether the shard finished its shutdown and left the loop.
    pub exit: bool,
}

/// The shard's counters (slates' GAPS §7 tripwires read them).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Loop iterations.
    pub steps: u64,
    /// Task polls.
    pub polls: u64,
    /// Polls past the step quantum that were their task's own: past it on the CPU, or waiting inside a call.
    pub long_steps: u64,
    /// Of `long_steps`, the polls that waited inside a call.
    pub blocked_steps: u64,
    /// Polls past the quantum by the wall clock that the host held.
    pub preempted_steps: u64,
    /// Polls past the quantum by the wall clock that could not be attributed.
    pub unattributed_steps: u64,
    /// The longest poll by the wall clock, in nanoseconds.
    pub longest_step_ns: u64,
    /// Kicked parks whose kick-to-running latency fed the online wake estimate.
    pub wake_samples: u64,
    /// Kick stamps that predated their park's announcement, dropped.
    pub wake_stale: u64,
    /// Kicks that found the shard not yet asleep, so no wake was measured.
    pub wake_unslept: u64,
    /// The online wake estimate, nanoseconds.
    pub wake_cost_ns: u64,
    /// Tasks admitted.
    pub spawns: u64,
    /// Tasks whose future returned.
    pub completed: u64,
    /// Tasks whose future was dropped.
    pub cancelled: u64,
    /// Wakes that arrived from other threads (the wake bitmap).
    pub wakes_foreign: u64,
    /// Of those, wakes for a slot with no task when drained: the task ended after the wake was sent. Not
    /// queued. A wake that raced its task's end *and* the slot's reuse wakes the new occupant once,
    /// spuriously, and cannot be told apart (a bit carries no generation); this counter bounds the
    /// visible half of that race (slates-dc's review item 4).
    pub wakes_to_free_slots: u64,
    /// Control messages received.
    pub controls: u64,
    /// Timers fired.
    pub timers_fired: u64,
    /// Sleeps that found every timer taken and waited for one to free (AUD-29-39).
    pub timer_waits: u64,
    /// Driver waits.
    pub waits: u64,
    /// Non-blocking driver polls (`harvest_io`): each a system call, made only while a readiness wait is
    /// registered.
    pub harvests: u64,
    /// Driver completions delivered.
    pub completions: u64,
    /// Times the driver was lost.
    pub driver_lost: u64,
    /// Driver errors other than loss.
    pub driver_errors: u64,
    /// Admissions refused because no task slot was free.
    pub admission_refused: u64,
    /// Spawn requests drained after shutdown began and refused unadmitted.
    pub refused_at_shutdown: u64,
    /// Idle spins that ended with work arriving.
    pub spin_hits: u64,
    /// Pollers woken because their ring had something.
    pub poller_wakes: u64,
    /// Idle spins that ran out and parked.
    pub spin_misses: u64,
    /// Idle spins ended by a timer falling due.
    pub spin_deadlines: u64,
    /// The measured scheduler overrun, nanoseconds.
    pub scheduler_overrun_ns: u64,
    /// 1 when the OS refused the core the shard's placement fixed it to.
    pub pin_refused: u64,
    /// Readiness registrations the driver refused (each handed to its task's next poll).
    pub interests_refused: u64,
    /// Marks of waits dropped away from this shard that its sweeps visited: one per marked slot, never a
    /// pass over every slot.
    pub abandons_swept: u64,
}

/// Shape: the exponential-forgetting shift of the measured scheduler overrun — an overrun not renewed
/// decays by `1/8` (`>> 3`) at each later wait, so a starvation spike lingers about eight waits and then
/// recovers as the load lifts (slates' value; its derivation is owed, docs/runtime.md §15).
const OVERRUN_FORGET_SHIFT: u32 = 3;

/// What a shard is built from: everything is `Send`, so the runtime assembles seeds on its own thread and
/// each shard thread builds its shard from one.
pub struct ShardSeed {
    /// The registered id.
    pub id: u16,
    /// The registration that holds the slot, for retiring exactly it.
    pub(crate) holder: registry::SlotHolder,
    /// Builds the driver on the shard's thread.
    pub driver: DriverSeed,
    /// The kick the driver answers to.
    pub kick: Kick,
    /// The control channel's receiving end.
    pub control: Receiver<Control>,
    /// The configuration.
    pub config: RuntimeConfig,
}

impl std::fmt::Debug for ShardSeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardSeed").field("id", &self.id).finish()
    }
}

impl ShardSeed {
    /// Registers a shard over `driver` in the process registry and returns its seed.
    pub(crate) fn register(
        config: &RuntimeConfig,
        driver: DriverSeed,
        kick: registry::RegisterKick,
    ) -> Result<ShardSeed, RtError> {
        config.validate_shape()?;
        let (holder, control) = registry::register_slot(
            config.tasks_per_shard,
            config.ring_entries,
            config.interests_per_shard,
            kick,
        )?;
        let id = holder.shard();
        let kick = registry::with_entry(id, |entry| entry.kick).unwrap_or(Kick::None);
        Ok(ShardSeed {
            id,
            holder,
            driver,
            kick,
            control,
            config: config.clone(),
        })
    }
}

/// A poller the loop asks each step.
struct Poller {
    slot: u32,
    generation: u32,
    ready: crate::shard::PollerReady,
}

/// The loop's state, owned by the loop's frame alone.
struct Core {
    tasks: Box<[TaskSlot]>,
    timers: Wheel,
    driver: Box<dyn Driver>,
    control: Option<Receiver<Control>>,
    config: RuntimeConfig,
    counters: Counters,
    shutting_down: bool,
    exited: bool,
    driver_lost: bool,
    fired: Vec<(u32, u64)>,
    completions: Vec<Completion>,
    /// Who waits on which handle, in which direction (`crate::interests`).
    waiting: Interests,
    pollers: Vec<Poller>,
    /// One past the highest task generation issued: what the registry slot's next holder starts from.
    generation_high: u32,
    /// The absolute deadline the shard last waited for and has not stepped since.
    waited_for_ns: Option<u64>,
    /// The online wake estimate (§4.1, §4.3), when the configuration carries a measured prior.
    wake: Option<WakeEstimate>,
    idle_ratio: u64,
    fixed_quantum_ns: u64,
    fixed_spin_ns: u64,
    /// The published time of the last driver retrieval: busy native turns share one quantum.
    last_driver_ns: u64,
    /// Whether the driver's clock is real time (the simulation's is not).
    real_time: bool,
    attribution: Tracker,
}

/// A shard: its desk and its loop, owned together by whoever runs it (a worker thread, a
/// [`crate::runtime::LocalRuntime`], a [`crate::sim::SimRuntime`]).
pub struct Shard {
    desk: Box<ShardContext>,
    core: Core,
}

impl std::fmt::Debug for Shard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard").field("id", &self.desk.id).finish()
    }
}

impl Drop for Shard {
    /// A shard's futures are dropped with the desk entered, so a destructor that wakes or cancels a sibling
    /// reaches this shard, and the slot's next holder starts its generations past this shard's.
    fn drop(&mut self) {
        self.desk.accepting.set(false);
        if let Some(entry) = self.desk.entry {
            entry
                .accepting
                .store(false, std::sync::atomic::Ordering::Release);
        }
        self.close_control();
        let entered = registry::enter(&self.desk);
        for task in self.core.tasks.iter_mut() {
            drop(task.future.take());
        }
        drop(entered);
        registry::note_arena_generation(self.desk.id, self.core.generation_high);
        registry::note_exited(self.desk.id);
    }
}

impl Shard {
    /// Builds the shard from its seed on the calling thread (which builds the driver too).
    pub fn build(seed: ShardSeed) -> Result<Shard, RtError> {
        let config = seed.config;
        let mut driver = (seed.driver)(seed.kick)?;
        driver.reserve_handles(config.interests_per_shard)?;
        let is_sim = driver.kind() == DriverKind::Simulation;
        let generation_base = registry::entry(seed.id).map_or(0, |entry| entry.generation_base);
        let desk = Box::new(ShardContext::new(
            seed.id,
            &DeskShape {
                tasks: config.tasks_per_shard,
                timers: config.timers_per_shard,
                interests: config.interests_per_shard,
                generation_base,
            },
            is_sim,
            driver.clock(),
        ));
        let times_wakes = config.wake_tracking.is_some() && !is_sim;
        if times_wakes && let Some(entry) = registry::entry(seed.id) {
            entry.parking.time_wakes();
        }
        let now_ns = driver.now_ns();
        let core = Core {
            tasks: (0..config.tasks_per_shard)
                .map(|_| TaskSlot::empty())
                .collect(),
            timers: Wheel::new(config.timer_tick_ns, config.timers_per_shard, now_ns),
            control: Some(seed.control),
            fired: Vec::with_capacity(config.timers_per_shard),
            completions: Vec::with_capacity(config.ring_entries),
            waiting: Interests::new(config.interests_per_shard)?,
            pollers: Vec::new(),
            counters: Counters::default(),
            shutting_down: false,
            exited: false,
            driver_lost: false,
            generation_high: generation_base,
            waited_for_ns: None,
            wake: config
                .wake_tracking
                .map(|tracking| WakeEstimate::new(tracking.prior_ns, tracking.shift)),
            idle_ratio: config
                .wake_tracking
                .map_or(1, |tracking| tracking.idle_ratio),
            fixed_quantum_ns: config.step_budget_ns,
            fixed_spin_ns: config.spin_ns,
            last_driver_ns: now_ns,
            real_time: !is_sim,
            attribution: Tracker::default(),
            config,
            driver,
        };
        let shard = Shard { desk, core };
        shard.publish_clock();
        shard.desk.quantum_ns.set(shard.quantum_ns());
        Ok(shard)
    }

    /// The shard id.
    pub fn id(&self) -> ShardId {
        ShardId(self.desk.id)
    }

    /// The shard's desk: what its tasks see.
    pub fn context(&self) -> &ShardContext {
        &self.desk
    }

    /// Keeps `value` for the shard's life (before or between its runs) and hands back its handle.
    pub fn keep<T: 'static>(&mut self, value: T) -> Result<Kept<T>, RtError> {
        self.desk.keep(value)
    }

    /// Gives a simulated shard its sockets (before its first step).
    pub(crate) fn attach_sim(&mut self, sockets: crate::sim::SimSockets) {
        self.desk.sim = Some(sockets);
    }

    /// Whether the shard left its loop.
    pub fn exited(&self) -> bool {
        self.core.exited
    }

    /// Whether termination was caused by driver loss, rather than an orderly shutdown.
    pub(crate) fn driver_lost(&self) -> bool {
        self.core.driver_lost
    }

    /// A loop that exits without handing its output to the caller retires that owned value now.
    pub(crate) fn discard_root_output(&self) {
        let entered = registry::enter(&self.desk);
        drop(self.desk.take_root_output());
        drop(entered);
    }

    /// The counters.
    pub fn counters(&self) -> Counters {
        let mut c = self.core.counters;
        c.admission_refused = c
            .admission_refused
            .saturating_add(self.desk.refused_spawns.get());
        c.timer_waits = c.timer_waits.saturating_add(self.desk.timer_waits.get());
        c.scheduler_overrun_ns = self.desk.scheduler_overrun_ns.get();
        c.wake_cost_ns = self.quantum_ns();
        c
    }

    /// Tasks not yet terminal (a finished one waiting for its joiner is not counted).
    pub fn running_tasks(&self) -> usize {
        self.desk
            .tasks
            .iter()
            .filter(|cell| cell.phase.get() == Phase::Live)
            .count()
    }

    /// Occupied task slots: running tasks and finished ones waiting for a joiner.
    pub fn live_tasks(&self) -> usize {
        self.desk
            .tasks
            .iter()
            .filter(|cell| cell.phase.get() != Phase::Free)
            .count()
    }

    /// One past the highest task generation issued (the registry slot's next holder starts there).
    pub fn arena_generation_high(&self) -> u32 {
        self.core.generation_high
    }

    /// Records that the OS refused the core this shard's placement fixed it to.
    pub fn note_pin_refused(&mut self) {
        self.core.counters.pin_refused = 1;
    }

    /// The driver's clock.
    pub fn now_ns(&self) -> u64 {
        self.core.driver.now_ns()
    }

    /// The step quantum now: the online wake estimate while the shard tracks one, else the configured budget.
    pub fn quantum_ns(&self) -> u64 {
        self.core
            .wake
            .map_or(self.core.fixed_quantum_ns, |estimate| estimate.mean_ns())
            .max(1)
    }

    /// The idle spin window now: the wake estimate times the idle ratio while tracking, else the configured
    /// spin.
    fn spin_window_ns(&self) -> u64 {
        self.core.wake.map_or(self.core.fixed_spin_ns, |estimate| {
            estimate.mean_ns().saturating_mul(self.core.idle_ratio)
        })
    }

    fn publish_clock(&self) {
        self.desk.now_ns.set(self.core.driver.now_ns());
    }

    // ------------------------------------------------------------------ admission

    /// Admits a spawn request (from any thread's message, or the runtime that owns the shard): detached. The
    /// request's receipt, if it carries one, is answered with the outcome.
    pub fn spawn_request(&mut self, request: SpawnRequest) -> Result<TaskId, RtError> {
        let SpawnRequest {
            future,
            parent,
            receipt,
        } = request;
        if self.core.shutting_down {
            self.core.counters.refused_at_shutdown =
                self.core.counters.refused_at_shutdown.saturating_add(1);
            receipt.answer(Admission::Terminated);
            return Err(RtError::ShardGone {
                shard: self.desk.id,
            });
        }
        let parent = parent
            .filter(|p| p.shard() == self.desk.id)
            .map(|p| p.slot());
        let future: BoxedFuture = future;
        let admitted = self.desk.claim(Incoming {
            future,
            parent,
            joinable: false,
        });
        if admitted.is_ok() {
            self.apply();
        }
        receipt.answer(match &admitted {
            Ok(task) => Admission::Admitted(*task),
            Err(refusal) => Admission::Refused(refusal.clone()),
        });
        admitted
    }

    /// Spawns a joinable local task from the shard's owner (between runs).
    pub fn spawn_local(&mut self, future: BoxedFuture) -> Result<TaskId, RtError> {
        if self.core.shutting_down || self.core.exited {
            return Err(RtError::ShardGone {
                shard: self.desk.id,
            });
        }
        let id = self.desk.spawn_local(future, None)?;
        self.apply();
        Ok(id)
    }

    // ------------------------------------------------------------------ the desk's intents

    /// Applies everything the desk was asked since the loop last looked: tasks to install, cancel, detach and
    /// reap; pollers to register; timers to arm, disarm and free; readiness to register.
    fn apply(&mut self) {
        while let Some(slot) = self.desk.dirty_tasks.pop() {
            self.apply_task(slot);
        }
        while let Some(slot) = self.desk.dirty_timers.pop() {
            self.apply_timer(slot);
        }
        while let Some(interest) = self.desk.interests.pop() {
            self.apply_interest(interest);
        }
    }

    fn apply_task(&mut self, slot: u32) {
        let Some(cell) = self.desk.task(slot) else {
            return;
        };
        cell.dirty.set(false);
        let asks = cell.asks.replace(0);
        if asks & ask::INSTALL != 0
            && let Some(incoming) = cell.incoming.take()
        {
            self.install(slot, incoming);
        }
        if asks & ask::POLLER != 0
            && let Some(ready) = self.desk.task(slot).and_then(|cell| cell.poller.take())
        {
            let generation = self.desk.task(slot).map_or(0, |cell| cell.generation.get());
            self.core.pollers.push(Poller {
                slot,
                generation,
                ready,
            });
        }
        if asks & ask::CANCEL != 0
            && let Some(task) = self.task_mut(slot)
            && task.state != State::Done
        {
            task.cancel_requested = true;
            self.desk.wake_local(slot);
        }
        if asks & ask::DETACH != 0
            && let Some(task) = self.task_mut(slot)
        {
            task.joinable = false;
            if task.state == State::Done {
                self.reap(slot);
            }
        }
        if asks & ask::JOINED != 0 {
            self.reap(slot);
        }
    }

    fn install(&mut self, slot: u32, incoming: Incoming) {
        let Incoming {
            future,
            parent,
            joinable,
        } = incoming;
        let parent = parent.filter(|p| self.live(*p));
        if let Some(task) = self.task_mut(slot) {
            *task = TaskSlot::new(future, parent, joinable);
            task.state = State::Queued;
        }
        if let Some(p) = parent {
            link_child(&mut self.core.tasks, p, slot);
        }
        self.core.counters.spawns = self.core.counters.spawns.saturating_add(1);
        self.desk.wake_local(slot);
    }

    fn apply_timer(&mut self, slot: u32) {
        let Some(cell) = self.desk.timer(slot) else {
            return;
        };
        cell.dirty.set(false);
        // Whatever the slot held on the wheel before is let go first: a timer its sleep released may already
        // have been claimed again in the same poll (the refusal says it was not armed, or fired).
        let _ = self.core.timers.disarm(slot);
        if cell.phase.get() == TimerPhase::Claimed {
            let word = cell.word.get();
            if self
                .core
                .timers
                .arm(slot, cell.deadline_ns.get(), word)
                .is_ok()
            {
                cell.phase.set(TimerPhase::Armed);
            } else {
                // The wheel holds exactly the desk's timer slots and the slot was just let go, so arming cannot be
                // refused; were it, the sleep is woken to see its timer gone and take another.
                self.free_timer(slot);
                self.wake_word(Encoded::from_word(word));
            }
        }
    }

    /// Gives a timer slot back (the desk's rule, [`ShardContext::release_timer`]).
    fn free_timer(&mut self, slot: u32) {
        if let Some(cell) = self.desk.timer(slot) {
            self.desk.release_timer(slot, cell);
        }
    }

    // ------------------------------------------------------------------ the loop

    /// Runs the loop on the calling thread until shutdown completes. When idle within the window a client's
    /// last activity opened, the shard spins out the window checking its inboxes and driver before it parks;
    /// outside every window it parks at once.
    pub fn run(&mut self) {
        loop {
            let outcome = self.step();
            if outcome.exit {
                break;
            }
            if outcome.did_work {
                continue;
            }
            if self.spin_after_activity(outcome.next_deadline_ns) {
                continue;
            }
            self.park(outcome.next_deadline_ns);
        }
    }

    /// Runs available tasks and messages, parking for timers as needed. Empty external waits remain live
    /// when the final nonblocking turn finds nothing ready; returns when idle or exited.
    pub fn run_until_idle(&mut self) {
        loop {
            let outcome = self.step();
            if outcome.exit {
                break;
            }
            if outcome.did_work {
                continue;
            }
            if self.has_inbound() || self.wake_ready_pollers() {
                continue;
            }
            match outcome.next_deadline_ns {
                Some(deadline) => {
                    self.park(Some(deadline));
                }
                None => break,
            }
        }
    }

    /// Every nonblocking turn services registered driver work. Native busy turns share the measured
    /// quantum; idle turns retrieve immediately. Simulation has no elapsed CPU time to cross a quantum,
    /// so a registered deterministic event is retrieved on its next turn without advancing virtual time.
    fn harvest_turn(&mut self) -> bool {
        if self.core.shutting_down {
            return false;
        }
        let now = self.desk.now_ns.get();
        if !self.core.real_time
            || self.desk.local.is_empty()
            || self.core.driver.has_pending()
            || now.saturating_sub(self.core.last_driver_ns) >= self.quantum_ns()
        {
            self.harvest_io(now)
        } else {
            false
        }
    }

    /// Adds a task's readiness wait to the table and arms the handle for every direction now waited for, or
    /// withdraws a wait its future dropped. A refusal (the table's bound, the driver's) is handed to the task.
    fn apply_interest(&mut self, interest: crate::shard::Interest) {
        let word = interest.word.word();
        if self.core.driver_lost {
            if interest.withdraw {
                self.desk.withdrawn(interest.ticket);
            } else {
                self.desk
                    .refuse_wait(interest.ticket, interest.word, RtError::DriverLost);
            }
            return;
        }
        if interest.withdraw {
            // The handle stays armed for what is left; a direction no one waits for fires once, to no one.
            let left = self
                .core
                .waiting
                .remove(interest.raw, interest.writable, interest.ticket);
            if left.is_empty() {
                self.core.driver.disarm(interest.raw);
            }
            self.desk.withdrawn(interest.ticket);
            return;
        }
        if self.desk.wait_abandoned(interest.ticket) {
            // Dropped before it was registered: nothing to arm; its withdrawal, queued behind, finds it gone.
            self.desk.withdrawn(interest.ticket);
            return;
        }
        let armed = self
            .core
            .waiting
            .add(interest.raw, interest.writable, word, interest.ticket)
            .and_then(|wanted| {
                self.core
                    .driver
                    .arm(interest.raw, wanted, interests::tag_of(interest.raw))
                    .inspect_err(|_| {
                        let _ = self.core.waiting.remove(
                            interest.raw,
                            interest.writable,
                            interest.ticket,
                        );
                    })
            });
        if let Err(refusal) = armed {
            self.core.counters.interests_refused =
                self.core.counters.interests_refused.saturating_add(1);
            if refusal == RtError::DriverLost {
                self.lose_driver();
            } else {
                self.desk
                    .refuse_wait(interest.ticket, interest.word, refusal);
            }
        }
    }

    /// Queues the tasks the driver's completions name: a task's own word (a no-op's), or a handle whose
    /// readiness fires its waits in the directions that fired — each marked fired on the desk, so only a
    /// fire makes a wait ready — the handle armed again for those left.
    fn deliver(&mut self, completions: &mut Vec<Completion>) {
        for completion in completions.drain(..) {
            self.core.counters.completions = self.core.counters.completions.saturating_add(1);
            let Some(raw) = interests::handle_of(completion.user_data) else {
                self.desk
                    .wake_local(Encoded::from_word(completion.user_data).slot());
                continue;
            };
            let desk = &self.desk;
            let fired = Readiness::from_bits(completion.result);
            let left = self.core.waiting.fire(raw, fired, |word, ticket| {
                desk.fire_wait(ticket, Encoded::from_word(word));
            });
            if !left.is_empty()
                && let Err(refusal) = self.core.driver.arm(raw, left, interests::tag_of(raw))
            {
                self.core.counters.interests_refused =
                    self.core.counters.interests_refused.saturating_add(1);
                if refusal == RtError::DriverLost {
                    self.lose_driver();
                    break;
                } else {
                    self.core.waiting.fire(raw, left, |word, ticket| {
                        desk.refuse_wait(ticket, Encoded::from_word(word), refusal.clone());
                    });
                }
            }
        }
    }

    /// Harvests the driver's ready completions without blocking and queues their tasks; true when a
    /// completion was queued. With no readiness wait registered the driver holds nothing for a task (the
    /// shard arms the driver only for its waits, `apply_interest` and `deliver`), so no system call is made:
    /// a kick left behind returns the next park at once, as it would have.
    fn harvest_io(&mut self, now_ns: u64) -> bool {
        if self.core.shutting_down
            || (self.core.waiting.is_empty() && !self.core.driver.has_pending())
        {
            return false;
        }
        self.core.last_driver_ns = now_ns;
        self.core.counters.harvests = self.core.counters.harvests.saturating_add(1);
        self.receive_completions(Some(0))
    }

    /// One result policy for busy harvest, spin and idle park. Fatal loss takes precedence
    /// over partial completions, before any task is fired or dead driver rearmed.
    fn receive_completions(&mut self, timeout_ns: Option<u64>) -> bool {
        let mut completions = std::mem::take(&mut self.core.completions);
        let result = self.core.driver.wait(timeout_ns, &mut completions);
        if result == Err(RtError::DriverLost) {
            completions.clear();
            self.core.completions = completions;
            self.lose_driver();
            return true;
        }
        if result.is_err() {
            self.core.counters.driver_errors = self.core.counters.driver_errors.saturating_add(1);
        }
        let harvested = !completions.is_empty();
        self.deliver(&mut completions);
        completions.clear();
        self.core.completions = completions;
        harvested || self.core.driver_lost
    }

    /// Spins out the rest of the idle window the last client activity opened; true when something arrived or
    /// a timer fell due during the spin.
    fn spin_after_activity(&mut self, deadline_ns: Option<u64>) -> bool {
        let Some(activity) = self.desk.activity_ns.get() else {
            return false;
        };
        let window_end = activity.saturating_add(self.spin_window_ns());
        if self.now_ns() >= window_end {
            return false;
        }
        self.core.attribution.wait_began();
        if let Some(entry) = self.desk.entry {
            entry.pulse.record_spinning(true);
        }
        let found = self.spin_for_work(window_end, deadline_ns);
        if let Some(entry) = self.desk.entry {
            entry.pulse.record_spinning(false);
        }
        if found {
            // harvest_io already recorded any retrieval; an inbox hit is not one.
            self.wait_ended(false);
        }
        found
    }

    /// The spin: true when work arrived or `deadline_ns` fell due before `spin_end`. Each turn asks the
    /// inboxes and the pollers, which cost no system call; the driver (a socket's readiness is work as much
    /// as a wake) is asked at the spin's start and then once a quantum, the calibrated wake's cost, so a
    /// readiness waits no longer than a wake would have while the spin makes no system call a turn. The
    /// preceding step's retrieval and the spin share their quantum, so idle entry does not ask twice.
    fn spin_for_work(&mut self, spin_end: u64, deadline_ns: Option<u64>) -> bool {
        let mut next_harvest_ns = self.core.last_driver_ns.saturating_add(self.quantum_ns());
        loop {
            let now = self.now_ns();
            let harvest_due = now >= next_harvest_ns;
            if self.has_inbound()
                || self.core.driver.has_pending()
                || self.wake_ready_pollers()
                || (harvest_due && self.harvest_io(now))
            {
                self.core.counters.spin_hits = self.core.counters.spin_hits.saturating_add(1);
                return true;
            }
            let now = self.now_ns();
            if let Some(deadline) = deadline_ns
                && now >= deadline
            {
                self.core.counters.spin_deadlines =
                    self.core.counters.spin_deadlines.saturating_add(1);
                self.core.waited_for_ns = Some(deadline);
                return true;
            }
            if now >= spin_end {
                self.core.counters.spin_misses = self.core.counters.spin_misses.saturating_add(1);
                return false;
            }
            if harvest_due {
                next_harvest_ns = now.saturating_add(self.quantum_ns());
            }
            std::hint::spin_loop();
        }
    }

    /// Whether anything waits in the shard's inboxes (no system call).
    fn has_inbound(&self) -> bool {
        !self.desk.local.is_empty()
            || self
                .desk
                .entry
                .is_some_and(|entry| entry.wakes.is_pending() || entry.control_pending.is_pending())
    }

    /// One loop iteration without blocking.
    pub fn step(&mut self) -> StepOutcome {
        if self.core.exited {
            return StepOutcome {
                did_work: false,
                next_deadline_ns: None,
                exit: true,
            };
        }
        if self.core.real_time {
            let read = account_now(self.now_ns());
            self.core.attribution.step_began(|| read);
        }
        self.core.counters.steps = self.core.counters.steps.saturating_add(1);
        if let Some(entry) = self.desk.entry {
            entry.pulse.record(
                self.core.counters.steps,
                self.core.counters.spawns,
                self.core.counters.completed,
                self.core.counters.admission_refused,
                self.core.counters.longest_step_ns,
            );
        }
        if let Some(deadline) = self.core.waited_for_ns.take() {
            self.note_wait_overrun(deadline);
        }
        self.publish_clock();
        self.desk.counters.set(self.counters());
        let mut did_work = self.drain_control();
        did_work |= self.drain_wakes();
        did_work |= self.expire_timers();
        did_work |= self.wake_ready_pollers();
        self.apply();
        did_work |= self.harvest_turn();
        let batch = self.desk.local.batch(self.core.config.batch.max(1));
        for _ in 0..batch {
            let Some(slot) = self.desk.local.pop() else {
                break;
            };
            did_work = true;
            self.poll_slot(slot);
            self.apply();
        }
        // Running tasks, not occupied slots: a finished task that waits for a joiner holds its slot, and after
        // shutdown nobody joins it, so waiting on it would never end (found by `tcp::serve`'s handlers).
        let exit = self.core.shutting_down && self.running_tasks() == 0;
        if exit {
            self.core.exited = true;
            self.close_control();
            registry::note_arena_generation(self.desk.id, self.core.generation_high);
            registry::note_exited(self.desk.id);
        }
        self.core.attribution.step_ended(did_work);
        StepOutcome {
            did_work,
            next_deadline_ns: self.core.timers.next_deadline_ns(),
            exit,
        }
    }

    /// Parks in the driver until a kick, a completion or `deadline_ns`. The parking announcement comes first
    /// and the inbox re-check second (the protocol and its loom model: [`crate::parking`]).
    pub fn park(&mut self, deadline_ns: Option<u64>) {
        if self.core.exited {
            return;
        }
        if self.core.driver_lost {
            self.finish_cancelled();
            return;
        }
        self.apply();
        if self.core.driver_lost {
            self.finish_cancelled();
            return;
        }
        self.core.waited_for_ns = deadline_ns;
        self.core.attribution.wait_began();
        let mut lost = false;
        let retrieved = match self.desk.entry {
            Some(entry) => {
                let learns = self.core.real_time && self.core.wake.is_some();
                let mut switches_before_wait = None;
                let pending = self.has_inbound();
                let parked = entry.parking.park_unless_pending(
                    || pending || entry.wakes.is_pending() || entry.control_pending.is_pending(),
                    || {
                        if learns {
                            switches_before_wait = attribution::voluntary_switches_now();
                        }
                        lost = self.wait_in_driver(deadline_ns);
                    },
                );
                if let Parked::Waited(Some(woken)) = parked {
                    self.note_wake(entry, woken, switches_before_wait);
                }
                matches!(parked, Parked::Waited(_))
            }
            None => {
                lost = self.wait_in_driver(deadline_ns);
                true
            }
        };
        self.wait_ended(retrieved);
        if lost {
            self.fail_all();
        }
    }

    /// Folds a park's measured wake into the online estimate (slates §4.3).
    fn note_wake(&mut self, entry: &Entry, woken: Woken, switches_before_wait: Option<u64>) {
        let Some(mut estimate) = self.core.wake else {
            return;
        };
        if !self.core.real_time {
            return;
        }
        if woken.stale() {
            self.core.counters.wake_stale = self.core.counters.wake_stale.saturating_add(1);
            return;
        }
        let Some(latency) = woken.latency_ns() else {
            if woken.early() {
                self.core.counters.wake_unslept = self.core.counters.wake_unslept.saturating_add(1);
            }
            return;
        };
        if let (Some(before), Some(after)) =
            (switches_before_wait, attribution::voluntary_switches_now())
            && after == before
        {
            self.core.counters.wake_unslept = self.core.counters.wake_unslept.saturating_add(1);
            return;
        }
        estimate.record(latency);
        self.core.wake = Some(estimate);
        let mean = estimate.mean_ns();
        self.core.counters.wake_samples = self.core.counters.wake_samples.saturating_add(1);
        self.core.counters.wake_cost_ns = mean;
        self.desk.quantum_ns.set(self.quantum_ns());
        entry.pulse.record_wake_cost(mean);
    }

    /// Folds one finished wait into the measured scheduler overrun: the shard waited for `deadline_ns` and
    /// this step is the first to run after it.
    fn note_wait_overrun(&mut self, deadline_ns: u64) {
        let overrun = self.now_ns().saturating_sub(deadline_ns);
        let held = self.desk.scheduler_overrun_ns.get();
        let forgotten = held.saturating_sub(held >> OVERRUN_FORGET_SHIFT);
        let measured = forgotten.max(overrun);
        self.desk.scheduler_overrun_ns.set(measured);
        if let Some(entry) = self.desk.entry {
            entry.pulse.record_scheduler_overrun(measured);
        }
    }

    /// A wait ended: while a long poll has gone unattributed, a window opens now. Only an actual
    /// driver wait renews the retrieval age; a skipped park or inbox-only spin cannot postpone I/O.
    fn wait_ended(&mut self, retrieved: bool) {
        let now = self.now_ns();
        if retrieved {
            self.core.last_driver_ns = now;
        }
        if self.core.real_time {
            let read = account_now(now);
            self.core.attribution.wait_ended(|| read);
        }
    }

    /// Attributes a poll against the step quantum: within it by the wall clock it is within; past it, the
    /// attribution windows decide whose it was ([`crate::attribution`]).
    fn attribute_poll(&mut self, poll_started_ns: u64, ended_ns: u64) -> Option<Attribution> {
        let quantum = self.quantum_ns();
        if ended_ns.saturating_sub(poll_started_ns) <= quantum {
            return None;
        }
        if !self.core.real_time {
            return Some(Attribution::Long);
        }
        let end = attribution::thread_account();
        Some(
            self.core
                .attribution
                .long_poll(poll_started_ns, ended_ns, end, quantum),
        )
    }

    /// The driver's blocking wait until a kick, a completion or `deadline_ns`, its completions queued; true
    /// when the driver was lost.
    fn wait_in_driver(&mut self, deadline_ns: Option<u64>) -> bool {
        self.core.counters.waits = self.core.counters.waits.saturating_add(1);
        if let Some(entry) = self.desk.entry {
            entry.pulse.record_waits(self.core.counters.waits);
        }
        let timeout = deadline_ns.map(|d| d.saturating_sub(self.core.driver.now_ns()));
        let _ = self.receive_completions(timeout);
        self.core.driver_lost
    }

    /// Cancels every task with a terminal completion and exits: the driver is gone. Bounded by twice the
    /// tasks live when it began, plus one: each task needs at most a poll to drop and one to complete.
    fn fail_all(&mut self) {
        self.finish_cancelled();
        self.core.exited = true;
        self.close_control();
        registry::note_exited(self.desk.id);
    }

    /// Closes admission before cancellation; called outside nested task polling, or marks
    /// cancellation for the remainder of the current step when an initial arm loses the driver.
    fn lose_driver(&mut self) {
        if self.core.driver_lost {
            return;
        }
        self.core.driver_lost = true;
        self.core.counters.driver_lost = self.core.counters.driver_lost.saturating_add(1);
        self.core.shutting_down = true;
        self.desk.accepting.set(false);
        if let Some(entry) = self.desk.entry {
            entry
                .accepting
                .store(false, std::sync::atomic::Ordering::Release);
            entry.stop.store(true, std::sync::atomic::Ordering::Release);
        }
        self.close_control();
        self.cancel_all();
    }

    fn close_control(&mut self) {
        let control = self.core.control.take();
        let entered = registry::enter(&self.desk);
        drop(control);
        drop(entered);
    }

    fn finish_cancelled(&mut self) {
        let mut bound = self.live_tasks().saturating_mul(2).saturating_add(1);
        while self.running_tasks() > 0 && bound > 0 {
            let outcome = self.step();
            bound = bound.saturating_sub(1);
            if outcome.exit {
                break;
            }
        }
    }

    fn drain_control(&mut self) -> bool {
        let Some(entry) = self.desk.entry else {
            return false;
        };
        // Take (read and clear in one atomic step) before draining: a send that lands during the drain
        // publishes the flag again and is seen on the next step at the latest.
        if !entry.control_pending.take() {
            return false;
        }
        let batch = self.core.config.batch.max(1);
        let mut drained: usize = 0;
        while drained < batch {
            let Some(message) = self
                .core
                .control
                .as_ref()
                .and_then(|control| control.try_recv().ok())
            else {
                break;
            };
            drained = drained.saturating_add(1);
            self.core.counters.controls = self.core.counters.controls.saturating_add(1);
            self.handle_control(message);
        }
        if drained == batch {
            // A whole batch drained may have left messages behind it: re-arm the flag (slates'
            // docs/bugs/2026-09-17-control-drain-forgets-a-burst-past-one-batch.md).
            entry.control_pending.rearm();
        }
        // Waits of this shard dropped elsewhere: their marks, set before the control mark this drain took.
        if entry
            .abandons_marked
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            let visited = self.desk.sweep_abandoned();
            self.core.counters.abandons_swept =
                self.core.counters.abandons_swept.saturating_add(visited);
            drained = drained.saturating_add(1);
        }
        // The runtime's stop, after this batch's messages: a Shutdown at the back of the batch that took no
        // channel slot (`registry::request_stop`). Messages drained later are refused as at any shutdown.
        if entry.stop.load(std::sync::atomic::Ordering::Acquire) && !self.core.shutting_down {
            self.handle_control(Control::Shutdown);
            return true;
        }
        drained > 0
    }

    /// Takes every wake other threads sent since the last drain onto the run queue.
    fn drain_wakes(&mut self) -> bool {
        let Some(entry) = self.desk.entry else {
            return false;
        };
        let desk = &self.desk;
        let mut free: u64 = 0;
        let woken = entry.wakes.drain(|slot| {
            if desk
                .task(slot)
                .is_none_or(|cell| cell.phase.get() == Phase::Free)
            {
                free = free.saturating_add(1);
            } else {
                desk.wake_local(slot);
            }
        });
        self.core.counters.wakes_to_free_slots =
            self.core.counters.wakes_to_free_slots.saturating_add(free);
        self.core.counters.wakes_foreign = self
            .core
            .counters
            .wakes_foreign
            .saturating_add(u64::try_from(woken).unwrap_or(u64::MAX));
        woken > 0
    }

    /// Wakes every poller whose ring is ready; a poller whose task ended is dropped.
    fn wake_ready_pollers(&mut self) -> bool {
        let desk = &self.desk;
        self.core.pollers.retain(|poller| {
            desk.task(poller.slot).is_some_and(|cell| {
                cell.generation.get() == poller.generation && cell.phase.get() == Phase::Live
            })
        });
        let mut any = false;
        for poller in &self.core.pollers {
            if (poller.ready)() {
                any = true;
                self.core.counters.poller_wakes = self.core.counters.poller_wakes.saturating_add(1);
                desk.wake_local(poller.slot);
            }
        }
        any
    }

    fn handle_control(&mut self, message: Control) {
        match message {
            Control::Spawn(request) => {
                if let Err(RtError::TooManyTasks { .. }) = self.spawn_request(*request) {
                    self.core.counters.admission_refused =
                        self.core.counters.admission_refused.saturating_add(1);
                }
            }
            Control::Cancel(word) => {
                let _ = self.desk.cancel(TaskId(word));
            }
            Control::Shutdown => {
                self.core.shutting_down = true;
                self.desk.accepting.set(false);
                if let Some(entry) = self.desk.entry {
                    entry
                        .accepting
                        .store(false, std::sync::atomic::Ordering::Release);
                    entry.stop.store(true, std::sync::atomic::Ordering::Release);
                }
                self.cancel_all();
            }
        }
    }

    fn expire_timers(&mut self) -> bool {
        let now = self.core.driver.now_ns();
        let mut fired = std::mem::take(&mut self.core.fired);
        self.core.timers.advance(now, &mut fired);
        let any = !fired.is_empty();
        for (slot, word) in fired.drain(..) {
            self.core.counters.timers_fired = self.core.counters.timers_fired.saturating_add(1);
            self.wake_word(Encoded::from_word(word));
            // A timer its sleep released in the meantime is freed when the release is applied; an armed one is
            // freed here.
            if self
                .desk
                .timer(slot)
                .is_some_and(|cell| cell.phase.get() == TimerPhase::Armed)
            {
                self.free_timer(slot);
            }
        }
        self.core.fired = fired;
        any
    }

    fn poll_slot(&mut self, slot: u32) {
        let Some(generation) = self.desk.task(slot).map(|cell| cell.generation.get()) else {
            return;
        };
        let Some(task) = slot_mut(&mut self.core.tasks, slot) else {
            return;
        };
        if matches!(task.state, State::Finishing | State::Done | State::Running) {
            return;
        }
        let Some(mut future) = task.future.take() else {
            return;
        };
        if task.cancel_requested {
            task.state = State::Finishing;
            self.drop_entered(future);
            self.finish(slot, Outcome::Cancelled);
            return;
        }
        task.state = State::Running;
        let word = Encoded::pack(self.desk.id, slot, generation).unwrap_or(Encoded::from_word(0));
        let waker = waker_for(word);
        let mut cx = Context::from_waker(&waker);
        self.desk.current_task.set(Some(slot));
        let start = self.now_ns();
        self.desk.now_ns.set(start);
        let entered = registry::enter(&self.desk);
        let poll = future.as_mut().poll(&mut cx);
        drop(entered);
        let ended = self.now_ns();
        self.desk.current_task.set(None);
        // What the poll asked takes effect before its result is recorded: a parent that spawned children and
        // finished in one poll has them installed, linked and so cancelled with it.
        self.apply();
        let attributed = self.attribute_poll(start, ended);
        self.after_poll(PollDone {
            slot,
            future,
            elapsed: ended.saturating_sub(start),
            done: matches!(poll, Poll::Ready(())),
            attributed,
        });
    }

    /// Drops a task's future with the desk entered: its destructors may wake or cancel other tasks of this
    /// shard, or let their timers go.
    fn drop_entered(&self, future: BoxedFuture) {
        let entered = registry::enter(&self.desk);
        drop(future);
        drop(entered);
    }

    /// Wakes the task `word` names: on this shard directly, elsewhere through the registry.
    fn wake_word(&self, word: Encoded) {
        if word.shard() == self.desk.id {
            self.desk.wake_local(word.slot());
        } else {
            registry::wake(word);
        }
    }

    /// Records the poll and either stores the future back or finishes the task.
    fn after_poll(&mut self, poll: PollDone) {
        let PollDone {
            slot,
            future,
            elapsed,
            done,
            attributed,
        } = poll;
        let long = attributed.is_some_and(Attribution::is_tasks);
        self.core.counters.polls = self.core.counters.polls.saturating_add(1);
        if let Some(attributed) = attributed {
            count_long_poll(&mut self.core.counters, attributed);
        }
        self.core.counters.longest_step_ns = self.core.counters.longest_step_ns.max(elapsed);
        let Some(task) = slot_mut(&mut self.core.tasks, slot) else {
            return;
        };
        task.polls = task.polls.saturating_add(1);
        if long {
            task.long_steps = task.long_steps.saturating_add(1);
        }
        task.longest_step_ns = task.longest_step_ns.max(elapsed);
        if done || task.cancel_requested {
            self.drop_entered(future);
            self.finish(
                slot,
                if done {
                    Outcome::Completed
                } else {
                    Outcome::Cancelled
                },
            );
            return;
        }
        task.future = Some(future);
        task.state = State::Idle;
    }

    /// Moves a task to `Finishing`, cancels its live children and reaps the done ones, and completes it when
    /// none is live.
    fn finish(&mut self, slot: u32, outcome: Outcome) {
        let (children, first_child) = match self.task_mut(slot) {
            Some(task) => {
                task.state = State::Finishing;
                task.outcome = Some(outcome);
                (task.children, task.first_child)
            }
            None => return,
        };
        match outcome {
            Outcome::Completed => {
                self.core.counters.completed = self.core.counters.completed.saturating_add(1);
            }
            Outcome::Cancelled => {
                self.core.counters.cancelled = self.core.counters.cancelled.saturating_add(1);
            }
        }
        let mut child = first_child;
        // Bounded by the task table: a child list never holds more than every other slot.
        let mut budget = self.core.tasks.len();
        while child != NO_LINK && budget > 0 {
            budget = budget.saturating_sub(1);
            let (next, done) = match self.task_mut(child) {
                Some(task) => {
                    task.joinable = false;
                    task.cancel_requested = true;
                    (task.next_sibling, task.state == State::Done)
                }
                None => break,
            };
            if done {
                unlink_child(&mut self.core.tasks, slot, child);
                self.free_slot(child);
            } else {
                self.desk.wake_local(child);
            }
            child = next;
        }
        if children == 0 {
            self.complete(slot);
        }
    }

    /// Marks a task terminal, publishes its outcome and wakes its joiner, unlinks it from its parent when
    /// detached, reaps it if detached, and completes the parent if it was waiting on this child.
    fn complete(&mut self, slot: u32) {
        let mut current = Some(slot);
        let mut budget = self.core.tasks.len();
        while let Some(slot) = current
            && budget > 0
        {
            budget = budget.saturating_sub(1);
            current = None;
            let (parent, joinable, outcome) = match self.task_mut(slot) {
                Some(task) => {
                    task.state = State::Done;
                    (task.parent, task.joinable, task.outcome)
                }
                None => break,
            };
            if let Some(cell) = self.desk.task(slot) {
                cell.phase.set(Phase::Done);
                cell.outcome.set(outcome);
                if let Some(joiner) = cell.joiner.take() {
                    self.wake_word(joiner);
                }
            }
            if let Some(p) = parent {
                if !joinable {
                    unlink_child(&mut self.core.tasks, p, slot);
                }
                if let Some(parent_task) = self.task_mut(p) {
                    parent_task.children = parent_task.children.saturating_sub(1);
                    if parent_task.state == State::Finishing && parent_task.children == 0 {
                        current = Some(p);
                    }
                }
            }
            if !joinable {
                self.free_slot(slot);
            }
        }
    }

    /// Reaps a terminal task: unlinks it from its parent and frees its slot.
    fn reap(&mut self, slot: u32) {
        let Some(task) = self.task_mut(slot) else {
            return;
        };
        if task.state != State::Done {
            return;
        }
        if let Some(parent) = task.parent {
            unlink_child(&mut self.core.tasks, parent, slot);
        }
        self.free_slot(slot);
    }

    /// Gives a slot back: emptied, a new generation (so no word minted for its last task names the next),
    /// and onto the free stack, unless its generations are spent (the slot then retires, AUD-29-11).
    fn free_slot(&mut self, slot: u32) {
        if let Some(task) = self.task_mut(slot) {
            *task = TaskSlot::empty();
        }
        let Some(cell) = self.desk.task(slot) else {
            return;
        };
        let next = cell.generation.get().saturating_add(1);
        cell.generation.set(next);
        cell.phase.set(Phase::Free);
        cell.outcome.set(None);
        cell.joiner.set(None);
        self.core.generation_high = self.core.generation_high.max(next);
        if next < Encoded::TASK_GENERATION_LIMIT {
            let _ = self.desk.free_tasks.push(slot);
        }
    }

    /// Cancels every live task: what a `block_on` does once its root finished, so nothing it left running
    /// outlives the call.
    pub fn cancel_everything(&mut self) {
        let accepting = self.desk.accepting.replace(false);
        let foreign_accepting = self.desk.entry.is_some_and(|entry| {
            entry
                .accepting
                .swap(false, std::sync::atomic::Ordering::AcqRel)
        });
        self.cancel_all();
        self.finish_cancelled();
        if accepting && !self.core.shutting_down && !self.core.exited {
            self.desk.accepting.set(true);
            if foreign_accepting && let Some(entry) = self.desk.entry {
                entry
                    .accepting
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
    }

    fn cancel_all(&mut self) {
        for (slot, task) in self.core.tasks.iter_mut().enumerate() {
            if let Ok(slot) = u32::try_from(slot)
                && self
                    .desk
                    .task(slot)
                    .is_some_and(|cell| cell.phase.get() == Phase::Live)
                && task.state != State::Done
            {
                task.cancel_requested = true;
                self.desk.wake_local(slot);
            }
        }
    }

    fn live(&self, slot: u32) -> bool {
        self.desk
            .task(slot)
            .is_some_and(|cell| cell.phase.get() == Phase::Live)
    }

    fn task_mut(&mut self, slot: u32) -> Option<&mut TaskSlot> {
        self.core.tasks.get_mut(usize::try_from(slot).ok()?)
    }
}

/// One finished poll.
struct PollDone {
    slot: u32,
    future: Pin<Box<dyn std::future::Future<Output = ()> + 'static>>,
    elapsed: u64,
    done: bool,
    attributed: Option<Attribution>,
}

/// The thread's account and the shard clock now: an attribution window's start.
fn account_now(now_ns: u64) -> Option<(attribution::ThreadAccount, u64)> {
    attribution::thread_account().map(|account| (account, now_ns))
}

/// Counts a poll past the step quantum by the wall clock under whoever held it.
fn count_long_poll(counters: &mut Counters, attributed: Attribution) {
    if attributed.is_tasks() {
        counters.long_steps = counters.long_steps.saturating_add(1);
    }
    if attributed == Attribution::Blocked {
        counters.blocked_steps = counters.blocked_steps.saturating_add(1);
    }
    if attributed == Attribution::Preempted {
        counters.preempted_steps = counters.preempted_steps.saturating_add(1);
    }
    if attributed.is_unattributed() {
        counters.unattributed_steps = counters.unattributed_steps.saturating_add(1);
    }
}

fn slot_mut(tasks: &mut [TaskSlot], slot: u32) -> Option<&mut TaskSlot> {
    tasks.get_mut(usize::try_from(slot).ok()?)
}

fn link_child(tasks: &mut [TaskSlot], parent: u32, child: u32) {
    let old_first = match slot_mut(tasks, parent) {
        Some(p) => {
            let old = p.first_child;
            p.first_child = child;
            p.children = p.children.saturating_add(1);
            old
        }
        None => return,
    };
    if let Some(c) = slot_mut(tasks, child) {
        c.next_sibling = old_first;
        c.prev_sibling = NO_LINK;
    }
    if old_first != NO_LINK
        && let Some(next) = slot_mut(tasks, old_first)
    {
        next.prev_sibling = child;
    }
}

fn unlink_child(tasks: &mut [TaskSlot], parent: u32, child: u32) {
    let (prev, next) = match slot_mut(tasks, child) {
        Some(c) => {
            let links = (c.prev_sibling, c.next_sibling);
            c.prev_sibling = NO_LINK;
            c.next_sibling = NO_LINK;
            links
        }
        None => return,
    };
    if prev == NO_LINK {
        if let Some(p) = slot_mut(tasks, parent)
            && p.first_child == child
        {
            p.first_child = next;
        }
    } else if let Some(pv) = slot_mut(tasks, prev) {
        pv.next_sibling = next;
    }
    if next != NO_LINK
        && let Some(nx) = slot_mut(tasks, next)
    {
        nx.prev_sibling = prev;
    }
}
