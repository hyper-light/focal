//! The shard's loop: one thread, one task table, one run queue, one timing wheel, one driver, and the
//! loop that ties them (slates §4.3, "Loop"; docs/runtime.md §3.4).
//!
//! Each step: drain the control queue and the wake bitmap (spawns, cancels, shutdown, wakes from other
//! threads) and the driver's completions into the run queue; expire timers; list the tasks that woke
//! themselves behind all of it; poll ready tasks, at most a batch of them, applying what each poll asked of
//! the desk before the next, and reading those inputs again once the polls have run the latency budget
//! since they were last read and listed tasks of their own, or when only tasks that woke themselves
//! remain; then, if nothing is ready, spin out the idle
//! window that client activity opened, and park in the driver until a kick, a completion or the next
//! deadline. Cancellation guarantees a terminal completion: the future is dropped at the next
//! poll boundary, the task's children are cancelled and joined, and whoever joins it sees `Cancelled`.
//! The watchdog counts polls that exceed the step quantum (the expected wake) and attributes each to its
//! task or to the host ([`crate::attribution`]).
//!
//! **Ownership.** [`Shard`] owns everything here by value: its desk (boxed, so the address the thread's
//! current-shard pointer names is stable) and its loop state. A step lends the desk to the tasks it polls
//! and keeps the loop state to itself; between polls it applies the desk's intents. There is no `unsafe`,
//! no `RefCell` and nothing a nested call could find borrowed.

use std::task::{Context, Poll};

use crate::attribution::{self, Attribution, Tracker};
use crate::control::Control;
use crate::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use crate::error::RtError;
use crate::interests::{self, Interests, Readiness};
use crate::machine::wake::WakeTrack;
use crate::mem::Encoded;
use crate::park_cost::ParkCost;
use crate::parking::{Parked, Woken};
use crate::registry::{self, Entry};
use crate::runtime::RuntimeConfig;
use crate::shard::{
    DeskShape, Incoming, Kept, Phase, ShardContext, ShardId, TaskId, TimerPhase, ask,
};
use crate::task::{
    Admission, BoxedFuture, NO_LINK, Outcome, SpawnRequest, State, TaskFuture, TaskSlot,
};
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

/// Where a step's batch stands between two polls.
struct BatchMark {
    /// The last poll's end, the next one's start; `None` where the inputs did work since, so the next start
    /// is read afresh.
    mark: Option<u64>,
    /// When the batch last read its inputs: the step's start, until it reads them again.
    inputs_read_ns: Option<u64>,
    /// The run queue's listings then ([`crate::queue::LocalQueue::listed`]).
    listed_at_read: u64,
}

/// Declares the shard's counters twice over, from one list: [`Counters`], the copy a reader takes, and
/// `LiveCounters`, a `Cell` each on the desk, where the loop counts in place.
macro_rules! counters {
    ($($(#[$doc:meta])* $field:ident,)*) => {
        /// The shard's counters (slates' GAPS §7 tripwires read them).
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
        pub struct Counters {
            $($(#[$doc])* pub $field: u64,)*
        }

        /// The shard's counters as the loop keeps them: on the desk, a `Cell` each, counted in place, so a
        /// reader's copy is taken when it reads and a step copies nothing (a step used to publish the whole
        /// set to the desk, 312 bytes twice over: 15 ns of the 135 ns runtime's calibration item, two steps).
        /// In order (`repr(C)`), leading with what a poll counts, so those land on the desk's second line
        /// beside the run queue (`ShardContext`'s layout).
        #[derive(Debug, Default)]
        #[repr(C)]
        pub(crate) struct LiveCounters {
            $(pub(crate) $field: std::cell::Cell<u64>,)*
        }

        impl LiveCounters {
            /// The counts now, as the loop has kept them.
            pub(crate) fn snapshot(&self) -> Counters {
                Counters { $($field: self.$field.get(),)* }
            }
        }
    };
}

counters! {
    /// Task polls.
    polls,
    /// The longest poll by the wall clock, in nanoseconds.
    longest_step_ns,
    /// Loop iterations.
    steps,
    /// Polls past the step quantum that were their task's own: past it on the CPU, or waiting inside a call.
    long_steps,
    /// Of `long_steps`, the polls that waited inside a call.
    blocked_steps,
    /// Polls past the quantum by the wall clock that the host held.
    preempted_steps,
    /// Polls past the quantum by the wall clock that could not be attributed.
    unattributed_steps,
    /// Kicked parks whose kick-to-running latency fed the online wake estimate.
    wake_samples,
    /// Kick stamps that predated their park's announcement, dropped.
    wake_stale,
    /// Kicks that found the shard not yet asleep, so no wake was measured.
    wake_unslept,
    /// The online wake estimate, nanoseconds.
    wake_cost_ns,
    /// The online cost of blocking: the CPU one park/wake cycle costs, the shard's park and its sender's
    /// kick, nanoseconds (`park_cost`); a tracking shard's idle spin lasts this long. 0 before the first
    /// measured park, and where the OS keeps no fine per-thread clock.
    park_cost_ns,
    /// Parks whose CPU was measured into [`Counters::park_cost_ns`].
    park_samples,
    /// Tasks admitted.
    spawns,
    /// Tasks whose future returned.
    completed,
    /// Tasks whose future was dropped.
    cancelled,
    /// Tasks whose poll unwound ([`Outcome::Unwound`]), and futures whose drop unwound: each caught by
    /// the shard's boundary around the task's code.
    unwound,
    /// Wakes that arrived from other threads (the wake bitmap).
    wakes_foreign,
    /// Of those, wakes for a slot with no task when drained: the task ended after the wake was sent. Not
    /// queued. A wake that raced its task's end *and* the slot's reuse wakes the new occupant once,
    /// spuriously, and cannot be told apart (a bit carries no generation); this counter bounds the
    /// visible half of that race (slates-dc's review item 4).
    wakes_to_free_slots,
    /// Control messages received.
    controls,
    /// Timers fired.
    timers_fired,
    /// Sleeps that found every timer taken and waited for one to free (AUD-29-39).
    timer_waits,
    /// Driver waits.
    waits,
    /// Non-blocking driver polls (`harvest_io`): each a system call, made only while a readiness wait is
    /// registered.
    harvests,
    /// Reads of the thread's CPU account (`attribution::thread_account`), each a system call: made only
    /// while poll attribution is armed, and at a poll past the step quantum.
    thread_accounts,
    /// Reads of the thread's CPU clock to learn what a park costs, each a system call: two for each park
    /// the shard waits in while it learns ([`Counters::park_samples`]), and none otherwise.
    park_cost_reads,
    /// Driver completions delivered.
    completions,
    /// Times the driver was lost.
    driver_lost,
    /// Driver errors other than loss.
    driver_errors,
    /// Admissions refused because no task slot was free.
    admission_refused,
    /// Spawn requests drained after shutdown began and refused unadmitted.
    refused_at_shutdown,
    /// Idle spins that ended with work arriving.
    spin_hits,
    /// Pollers woken because their ring had something.
    poller_wakes,
    /// Idle spins that ran out and parked.
    spin_misses,
    /// Idle spins ended by a timer falling due.
    spin_deadlines,
    /// The measured scheduler overrun, nanoseconds.
    scheduler_overrun_ns,
    /// 1 when the OS refused the core the shard's placement fixed it to.
    pin_refused,
    /// Readiness registrations the driver refused (each handed to its task's next poll).
    interests_refused,
    /// Marks of waits dropped away from this shard that its sweeps visited: one per marked slot, never a
    /// pass over every slot.
    abandons_swept,
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
        let holder = registry::register_runtime_slot(
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

/// The loop's state, owned by the loop's frame alone. Laid out in order (`repr(C)`), as the desk is
/// (`ShardContext`): what every poll and batch reads first, then what keeping `due_ns` and a long poll's
/// judgment read, and last what parks and the configuration hold.
#[repr(C)]
struct Core {
    // First: what a poll, a batch and the input read's test read.
    tasks: Box<[TaskSlot]>,
    /// The configured poll count a step's batch may make (`config.batch`, at least one), kept here so a
    /// batch reads it from the first line rather than from the configuration's.
    batch: usize,
    /// The consumer's latency objective (`config.latency_objective_ns`), kept here for the batch's budget.
    latency_objective_ns: Option<u64>,
    /// The published time of the last driver retrieval: busy native turns share one quantum.
    last_driver_ns: u64,
    /// The last step found the shard idle and left its driver to what follows: a park's wait, which
    /// returns at once with what is ready, or the ask a spin or [`Shard::run_until_idle`] makes first
    /// where it does not park. A loop that does neither finds the driver asked once a quantum, as a
    /// busy turn does.
    harvest_owed: bool,
    /// The counter's scale where the shard reads a counter on real time (`machine::clock::counter_scale`):
    /// each step's reading of the driver's clock is carried forward by it (`ShardContext::carried`).
    counter_scale: Option<u64>,
    fixed_quantum_ns: u64,
    /// Whether the driver's clock is real time (the simulation's is not).
    real_time: bool,
    driver_lost: bool,
    exited: bool,
    shutting_down: bool,
    /// Who waits on which handle, in which direction (`crate::interests`).
    waiting: Interests,
    // Then: what keeping `due_ns` and judging a long poll read.
    pollers: Vec<Poller>,
    driver: Box<dyn Driver>,
    timers: Wheel,
    /// The step's long polls left for one reading at its end ([`Tracker::defers`]): at most the step's poll
    /// budget, reserved at build, so judging by step allocates nothing.
    deferred: Vec<Deferred>,
    attribution: Tracker,
    // Last: what a park, a wake's estimate and the configuration's other fields hold.
    /// The online wake estimate (§4.1, §4.3), when the configuration carries a measured prior.
    wake: Option<WakeTrack>,
    /// The CPU a park costs this shard's thread, and a kick its senders' (`crate::park_cost`): a tracking
    /// shard's idle spin. Learned only while the wake is tracked on a real clock.
    park_cost: ParkCost,
    kick_cost: ParkCost,
    /// The parking word's kick sums last folded into `kick_cost`.
    kicks_seen: (u64, u64),
    idle_ratio: u64,
    fixed_spin_ns: u64,
    /// The absolute deadline the shard last waited for and has not stepped since.
    waited_for_ns: Option<u64>,
    config: RuntimeConfig,
    fired: Vec<(u32, u64)>,
    completions: Vec<Completion>,
}

/// A long poll left for the step's end: how far it ran past the quantum by the wall clock.
#[derive(Clone, Copy, Debug)]
struct Deferred {
    overrun_ns: u64,
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
        // A caught foreign poll panic leaves this existing marker set and its
        // already-unwound future absent. Retire that metadata before driving peers.
        let interrupted = self.desk.current_slot();
        self.desk.current.set(crate::shard::NO_TASK);
        if let Some(slot) = interrupted
            && self
                .task_mut(slot)
                .is_some_and(|task| task.state == State::Running && task.future.is_none())
        {
            self.finish(slot, Outcome::Cancelled);
        }
        // An apply/drop unwind can occur after current_task was cleared. Only an
        // actual unwind permits this exceptional classification; ordinary missing
        // ownership is never silently treated as completed service cleanup.
        if std::thread::panicking() {
            for index in 0..self.core.tasks.len() {
                let Ok(slot) = u32::try_from(index) else {
                    continue;
                };
                let interrupted = self.task_mut(slot).is_some_and(|task| {
                    task.future.is_none()
                        && (task.state == State::Running
                            || (task.state == State::Finishing && task.outcome.is_none()))
                });
                if interrupted {
                    self.finish(slot, Outcome::Cancelled);
                }
            }
        }
        self.core.shutting_down = true;
        self.cancel_all();
        // A service may still own accepted physical I/O. Its future is never forcibly
        // dropped because a poll count expired; only its Ready proves retirement.
        self.finish_cancelled();
        let entered = registry::enter(&self.desk);
        for task in self.core.tasks.iter_mut() {
            if let Some(future) = task.future.take() {
                // A future's drop is its task's code: inside an unwind boundary of the shard's own.
                let _ = crate::unwind::contained(move || drop(future));
            }
        }
        // A future still on the desk, its task never installed, is its task's code as much.
        for cell in self.desk.tasks.iter() {
            if let Some(incoming) = cell.incoming.take() {
                let _ = crate::unwind::contained(move || drop(incoming));
            }
        }
        drop(entered);
        registry::note_arena_generation(self.desk.id, self.desk.generation_high.get());
        registry::note_exited(self.desk.id);
    }
}

impl Shard {
    /// Builds the shard from its seed on the calling thread (which builds the driver too).
    pub fn build(seed: ShardSeed) -> Result<Shard, RtError> {
        let config = seed.config;
        let mut driver = (seed.driver)(seed.kick)?;
        driver.reserve_handles(config.interests_per_shard)?;
        // A timed wait may end at most a tick late: the wheel's own lateness (§3.6).
        driver.set_timer_slack_ns(config.timer_tick_ns.max(1))?;
        registry::bind_cleanup_thread(seed.holder)?;
        let is_sim = driver.kind() == DriverKind::Simulation;
        let generation_base = registry::entry(seed.id).map_or(0, |entry| entry.generation_base);
        let desk = Box::new(ShardContext::new(
            seed.id,
            &DeskShape {
                tasks: config.tasks_per_shard,
                timers: config.timers_per_shard,
                interests: config.interests_per_shard,
                generation_base,
                // The wheel's own tick (`Wheel::new` takes at least a nanosecond).
                timer_tick_ns: config.timer_tick_ns.max(1),
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
            fired: Vec::with_capacity(config.timers_per_shard),
            completions: Vec::with_capacity(config.ring_entries),
            waiting: Interests::new(config.interests_per_shard)?,
            pollers: Vec::new(),
            shutting_down: false,
            exited: false,
            driver_lost: false,
            waited_for_ns: None,
            wake: config.wake_tracking.map(|tracking| {
                WakeTrack::new(
                    tracking.prior_ns,
                    1u64.checked_shl(tracking.shift).unwrap_or(u64::MAX),
                )
            }),
            park_cost: ParkCost::new(),
            kick_cost: ParkCost::new(),
            kicks_seen: (0, 0),
            idle_ratio: config
                .wake_tracking
                .map_or(1, |tracking| tracking.idle_ratio),
            fixed_quantum_ns: config.step_budget_ns,
            fixed_spin_ns: config.spin_ns,
            // Only the OS's clock is carried: a simulated or published clock is the driver's to keep.
            counter_scale: match desk.clock {
                crate::driver::Clock::Since(_) => crate::machine::clock::counter_scale(),
                crate::driver::Clock::Sim(_) | crate::driver::Clock::Published => None,
            },
            last_driver_ns: now_ns,
            harvest_owed: false,
            real_time: !is_sim,
            attribution: Tracker::default(),
            deferred: Vec::with_capacity(config.batch.max(1)),
            batch: config.batch.max(1),
            latency_objective_ns: config.latency_objective_ns,
            config,
            driver,
        };
        let shard = Shard { desk, core };
        shard.publish_clock();
        shard.desk.quantum_ns.set(shard.quantum_ns());
        shard.refresh_due();
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
        self.desk.counters()
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
        self.desk.generation_high.get()
    }

    /// Records that the OS refused the core this shard's placement fixed it to.
    pub fn note_pin_refused(&mut self) {
        self.desk.live.pin_refused.set(1);
    }

    /// The driver's clock.
    pub fn now_ns(&self) -> u64 {
        if self.core.driver_lost {
            self.desk.now_ns.get()
        } else {
            self.core.driver.now_ns()
        }
    }

    /// The step quantum now: the online wake estimate while the shard tracks one, else the configured budget.
    pub fn quantum_ns(&self) -> u64 {
        self.core
            .wake
            .as_ref()
            .map_or(self.core.fixed_quantum_ns, WakeTrack::mean_ns)
            .max(1)
    }

    /// The budget after which a step's batch reads its inputs again (docs/runtime.md §3.3): the step
    /// quantum, or the consumer's latency objective where that is tighter.
    fn batch_budget_ns(&self) -> u64 {
        // The desk's copy of the quantum, which every change to the estimate publishes (`note_wake`): the
        // desk's first line, where the estimate itself is a cold 128 bytes.
        let quantum = self.desk.quantum_ns.get();
        self.core
            .latency_objective_ns
            .map_or(quantum, |objective| objective.min(quantum).max(1))
    }

    /// The idle spin window now: while the shard tracks its costs, what blocking costs it times the idle
    /// ratio (none before its first measured park: until then it parks at once); else the configured spin.
    /// Spin-then-block is 2-competitive in CPU when the spin lasts as long as blocking costs [A: Karlin, Li,
    /// Manasse, Owicki, SOSP'91]; sizing it by the wake's latency instead made it as long as the host's
    /// scheduler queue (`crate::park_cost`).
    fn spin_window_ns(&self) -> u64 {
        if self.core.wake.is_none() {
            return self.core.fixed_spin_ns;
        }
        self.blocking_cost_ns()
            .unwrap_or(0)
            .saturating_mul(self.core.idle_ratio)
    }

    /// What one park/wake cycle costs in CPU, the shard's park and its sender's kick, once a park has been
    /// measured.
    fn blocking_cost_ns(&self) -> Option<u64> {
        let park = self.core.park_cost.mean_ns()?;
        Some(park.saturating_add(self.core.kick_cost.mean_ns().unwrap_or(0)))
    }

    fn publish_clock(&self) {
        self.desk.now_ns.set(self.now_ns());
    }

    /// The step's reading of the driver's clock, published, and the counter's ticks beside it, which carry it
    /// forward to the step's later readings (docs/runtime.md §3.9, "a poll's end from the counter"); none
    /// once the driver is lost, or where the shard reads no counter.
    fn publish_and_carry_clock(&self) {
        self.publish_clock();
        let carried = match (self.core.counter_scale, self.core.driver_lost) {
            (Some(scale), false) => crate::machine::clock::counter_ticks().map(|ticks| {
                crate::machine::clock::Carried::new(self.desk.now_ns.get(), ticks, scale)
            }),
            _ => None,
        };
        self.desk.carried.set(carried);
    }

    /// The driver's clock now within a step: its step's reading carried forward by the counter where the
    /// shard reads one, else read again.
    fn carried_now_ns(&self) -> u64 {
        self.carried_from(self.desk.carried.get())
    }

    /// [`Shard::carried_now_ns`] from the step's reading `carried`, which a batch reads once: only a step's
    /// start sets it.
    #[inline]
    fn carried_from(&self, carried: Option<crate::machine::clock::Carried>) -> u64 {
        match (carried, crate::machine::clock::counter_ticks()) {
            (Some(carried), Some(ticks)) => carried.now_ns(ticks),
            _ => self.now_ns(),
        }
    }

    // ------------------------------------------------------------------ admission

    /// Admits a spawn request (from any thread's message, or the runtime that owns the shard): detached. The
    /// request's receipt, if it carries one, is answered with the outcome.
    pub fn spawn_request(&mut self, request: SpawnRequest) -> Result<TaskId, RtError> {
        self.spawn_request_with(request, false)
    }

    fn spawn_request_with(
        &mut self,
        request: SpawnRequest,
        service: bool,
    ) -> Result<TaskId, RtError> {
        let SpawnRequest {
            future,
            parent,
            receipt,
        } = request;
        if self.core.shutting_down {
            count(&self.desk.live.refused_at_shutdown, 1);
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
            future: TaskFuture::Boxed(future),
            parent,
            joinable: false,
            service,
        });
        if let Ok(task) = &admitted {
            self.install_listed(task.0.slot());
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
        self.install_listed(id.0.slot());
        self.apply();
        Ok(id)
    }

    /// Owner-side service admission, before the future can first be polled.
    pub(crate) fn spawn_service_local(&mut self, future: BoxedFuture) -> Result<TaskId, RtError> {
        if self.core.shutting_down || self.core.exited {
            return Err(RtError::ShardGone {
                shard: self.desk.id,
            });
        }
        let id = self.desk.spawn_service_local(future, None)?;
        self.install_listed(id.0.slot());
        self.apply();
        Ok(id)
    }

    // ------------------------------------------------------------------ the desk's intents

    /// Applies everything the desk was asked since the loop last looked: tasks to install, cancel, detach and
    /// reap; pollers to register; timers to arm, disarm and free; readiness to register. Only when a task
    /// asked (the desk's `asked`, set by every intent's push): the loop looks after every poll, and most polls
    /// ask nothing.
    #[inline]
    fn apply(&mut self) {
        if self.desk.asked.get() {
            self.apply_asked();
        }
    }

    /// [`Shard::apply`]'s work, once a task asked. An intent pushed while it runs is drained by the same
    /// loops, and leaves the flag set for one look that finds nothing.
    #[inline(never)]
    fn apply_asked(&mut self) {
        self.desk.asked.set(false);
        while let Some(slot) = self.desk.dirty_tasks.pop() {
            self.apply_task(slot);
        }
        while let Some(slot) = self.desk.dirty_timers.pop() {
            self.apply_timer(slot);
        }
        while let Some(interest) = self.desk.interests.pop() {
            self.apply_interest(interest);
        }
        self.refresh_due();
    }

    /// Keeps the desk's `due_ns`, the earliest time the loop has timed work of its own (docs/runtime.md
    /// §3.10), after every change to it (an input read, an applied intent): 0 while the driver holds a
    /// completion or a poller is registered, both looked at every turn; else the earlier of the wheel's next
    /// occupied tick, which is never later than its next deadline, and, while readiness waits are
    /// registered, the driver's next harvest a quantum after its last; `u64::MAX` with neither, or once the
    /// driver is lost or the shard has exited.
    fn refresh_due(&self) {
        let due = if self.core.driver_lost || self.core.exited {
            u64::MAX
        } else if self.core.driver.has_pending() || !self.core.pollers.is_empty() {
            0
        } else {
            let timer = self.core.timers.next_deadline_ns().unwrap_or(u64::MAX);
            let harvest = if self.core.waiting.is_empty() {
                u64::MAX
            } else {
                self.core.last_driver_ns.saturating_add(self.quantum_ns())
            };
            timer.min(harvest)
        };
        self.desk.due_ns.set(due);
    }

    fn apply_task(&mut self, slot: u32) {
        let Some(cell) = self.desk.task(slot) else {
            return;
        };
        cell.dirty.set(false);
        let asks = cell.asks.replace(0);
        // Asked to install it, or asked anything of a task listed at its spawn and not yet polled: its future
        // goes into its slot first, so what follows acts on it.
        if let Some(incoming) = cell.incoming.take() {
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
        if asks & ask::CANCEL != 0 {
            self.request_cancel(slot);
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
        self.install_slot(slot, incoming);
        self.desk.wake_local(slot);
    }

    /// Installs, now, a task whose future waits on the desk: the owner's spawns and the spawn requests it
    /// admits are installed before they return, and a pass that retires tasks without polling them finds
    /// every task in its slot. Listed as an install lists it: a task listed at its spawn already is, and its
    /// pending flag collapses the second listing, while one with a parent, whose install was asked of the
    /// loop instead, is listed here, its install's only listing (the ask finds its future gone).
    fn install_listed(&mut self, slot: u32) {
        if let Some(incoming) = self.desk.task(slot).and_then(|cell| cell.incoming.take()) {
            self.install(slot, incoming);
        }
    }

    /// Moves `incoming`'s future into `slot`, links it under its parent, and carries a cancellation asked of it
    /// before its install; not listed (the caller lists it, or polls it now).
    fn install_slot(&mut self, slot: u32, incoming: Incoming) {
        let Incoming {
            future,
            parent,
            joinable,
            service,
        } = incoming;
        let parent = parent.filter(|p| self.live(*p));
        let (generation, cancelled) = self.desk.task(slot).map_or((0, false), |cell| {
            (cell.generation.get(), cell.cancelled.get())
        });
        let word = Encoded::pack(self.desk.id, slot, generation).map_or(0, Encoded::word);
        if let Some(task) = self.task_mut(slot) {
            *task = TaskSlot::new(future, parent, joinable, word, service);
            task.state = State::Queued;
            task.cancel_requested = cancelled;
        }
        if let Some(p) = parent {
            link_child(&mut self.core.tasks, p, slot);
        }
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
            self.park_owned(outcome.next_deadline_ns);
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
            // The idle step left the driver to a park: the final nonblocking turn asks it here.
            if self.core.harvest_owed && self.harvest_io(self.desk.now_ns.get()) {
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
    /// quantum; an idle turn leaves the driver to the park that follows, whose wait returns at once with
    /// what is ready (`harvest_owed`): asked here as well, an idle turn made two system calls where the
    /// park's one does. Simulation has no elapsed CPU time to cross a quantum, so a registered
    /// deterministic event is retrieved on its next turn without advancing virtual time.
    fn harvest_turn(&mut self) -> bool {
        if self.core.driver_lost || self.core.exited {
            return false;
        }
        // With no readiness wait registered and nothing pending in the driver there is nothing to harvest
        // (`harvest_io` says the same): asked once, where the turn and the harvest asked the driver twice.
        let pending = self.core.driver.has_pending();
        if self.core.waiting.is_empty() && !pending {
            return false;
        }
        let now = self.desk.now_ns.get();
        if !self.core.real_time
            || pending
            || now.saturating_sub(self.core.last_driver_ns) >= self.quantum_ns()
        {
            return self.harvest_io(now);
        }
        if self.desk.local.is_empty() {
            self.core.harvest_owed = true;
        }
        false
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
            count(&self.desk.live.interests_refused, 1);
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
            count(&self.desk.live.completions, 1);
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
                count(&self.desk.live.interests_refused, 1);
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
        if self.core.driver_lost
            || self.core.exited
            || (self.core.waiting.is_empty() && !self.core.driver.has_pending())
        {
            return false;
        }
        self.core.last_driver_ns = now_ns;
        self.core.harvest_owed = false;
        count(&self.desk.live.harvests, 1);
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
            count(&self.desk.live.driver_errors, 1);
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
        if self.core.driver_lost {
            return false;
        }
        let Some(activity) = self.desk.activity_ns.get() else {
            return false;
        };
        let window_end = activity.saturating_add(self.spin_window_ns());
        if self.carried_now_ns() >= window_end {
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
        // The idle step left the driver to what follows: the spin asks it at its start.
        let mut next_harvest_ns = if self.core.harvest_owed {
            0
        } else {
            self.core.last_driver_ns.saturating_add(self.quantum_ns())
        };
        // A turn's time is the step's reading carried by the counter, as a poll's end is (§3.9): a turn that
        // read the OS's clock spent most of itself there, and a wake waited for the turn to notice it. A deadline
        // it finds a little early costs a step, which reads the clock afresh.
        let carried = self.desk.carried.get();
        loop {
            let now = self.carried_from(carried);
            let harvest_due = now >= next_harvest_ns;
            if self.has_inbound()
                || self.core.driver.has_pending()
                || self.wake_ready_pollers()
                || (harvest_due && self.harvest_io(now))
            {
                count(&self.desk.live.spin_hits, 1);
                return true;
            }
            // Re-read only after the driver was asked (a system call); otherwise this turn's reading stands.
            let now = if harvest_due {
                self.carried_from(carried)
            } else {
                now
            };
            if let Some(deadline) = deadline_ns
                && now >= deadline
            {
                count(&self.desk.live.spin_deadlines, 1);
                self.core.waited_for_ns = Some(deadline);
                return true;
            }
            if now >= spin_end {
                count(&self.desk.live.spin_misses, 1);
                return false;
            }
            if harvest_due {
                next_harvest_ns = now.saturating_add(self.quantum_ns());
            }
            std::hint::spin_loop();
        }
    }

    /// Whether anything waits in the shard's inboxes (no system call).
    pub(crate) fn has_inbound(&self) -> bool {
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
        // The step's one reading of the clock: the overrun of a wait just ended, the attribution window, the
        // timers and the harvest's quantum all take it, and the counter carries it to the step's polls.
        self.publish_and_carry_clock();
        // Only where the step's start opens a window: the account is a system call, and every step paid it
        // before the tracker threw an unarmed read away; judging by step, the last step's reading is open.
        if self.core.real_time && self.core.attribution.reads_at_step_start() {
            let read = account_now(self.desk.now_ns.get());
            count(&self.desk.live.thread_accounts, 1);
            self.core.attribution.step_began(|| read);
        }
        count(&self.desk.live.steps, 1);
        if let Some(entry) = self.desk.entry {
            entry.pulse.record(
                self.desk.live.steps.get(),
                self.desk.live.spawns.get(),
                self.desk.live.completed.get(),
                self.desk.live.admission_refused.get(),
                self.desk.live.longest_step_ns.get(),
            );
        }
        if let Some(deadline) = self.core.waited_for_ns.take() {
            self.note_wait_overrun(deadline);
        }
        let harvests = self.desk.live.harvests.get();
        let mut did_work = self.read_inputs();
        // One clock read a poll: each poll's end is the next one's start, so the bookkeeping between two polls
        // is counted to the second. The first poll starts from the step's reading when the step's drains,
        // timers, pollers and driver did nothing since (a few loads); after any of them, from a fresh one.
        let mark = (!did_work && self.desk.live.harvests.get() == harvests)
            .then(|| self.desk.now_ns.get());
        did_work |= self.poll_batch(mark);
        self.judge_deferred();
        // Running tasks, not occupied slots: a finished task that waits for a joiner holds its slot, and after
        // shutdown nobody joins it, so waiting on it would never end (found by `tcp::serve`'s handlers).
        let exit = self.exit_if_drained();
        self.core.attribution.step_ended(did_work);
        StepOutcome {
            did_work,
            next_deadline_ns: if self.core.driver_lost {
                None
            } else {
                self.core.timers.next_deadline_ns()
            },
            exit,
        }
    }

    /// The step's polls, from `mark`, the clock its first poll starts from where the step read nothing
    /// since; whether it polled any.
    ///
    /// The configured poll budget bounds this phase, including wakes produced by earlier polls. The FIFO
    /// keeps already-ready tasks ahead of those wakes; a local request/reply handoff need not repeat the
    /// whole loop for each end. The latency budget is a time (docs/runtime.md §3.3), and the count is it
    /// over a trivial poll's cost: once the polls since the inputs were last read have run the budget and
    /// listed tasks of their own (a wake of this shard's, a spawn), which go ahead of every wake still
    /// unread, the batch reads the inputs again before its next poll, and the step goes on to its count.
    /// Polls that list nothing leave the order as reading would: what arrives goes behind the tasks
    /// already listed either way.
    fn poll_batch(&mut self, mark: Option<u64>) -> bool {
        registry::with_current_cell(self, |shard, current| shard.poll_batch_in(mark, current))
    }

    /// [`Shard::poll_batch`] with the thread's current-shard cell lent for its polls.
    fn poll_batch_in(
        &mut self,
        mark: Option<u64>,
        current: Option<registry::CurrentCell<'_>>,
    ) -> bool {
        let batch = self.core.batch;
        let budget_ns = self.batch_budget_ns();
        // The quantum a poll is judged by: the wake estimate, which moves only when the shard parks, so one
        // reading serves the batch; and the step's clock reading, which only a step's start sets.
        let quantum_ns = self.desk.quantum_ns.get();
        let carried = self.desk.carried.get();
        let mut at = BatchMark {
            mark,
            inputs_read_ns: None,
            listed_at_read: self.desk.local.listed(),
        };
        let mut polled_any = false;
        for polled in 1..=batch {
            let Some(slot) = self.next_in_batch(&mut at) else {
                break;
            };
            polled_any = true;
            let start = match at.mark {
                Some(start) => start,
                None => self.carried_from(carried),
            };
            let since = *at.inputs_read_ns.get_or_insert(start);
            let ended = self.poll_slot(slot, start, quantum_ns, carried, current);
            at.mark = Some(ended);
            self.apply();
            if polled < batch
                && ended.saturating_sub(since) >= budget_ns
                && self.desk.local.listed() != at.listed_at_read
            {
                self.read_inputs_within(&mut at, ended);
            }
        }
        polled_any
    }

    /// The batch's next task: the ring's oldest, or, where only tasks that woke themselves remain, the
    /// first after the inputs are read again, which lists them behind what those made ready.
    fn next_in_batch(&mut self, at: &mut BatchMark) -> Option<u32> {
        if let Some(slot) = self.desk.local.pop() {
            return Some(slot);
        }
        if !self.desk.local.has_yielded() {
            return None;
        }
        let now = match at.mark {
            Some(now) => now,
            None => self.carried_now_ns(),
        };
        self.read_inputs_within(at, now);
        self.desk.local.pop()
    }

    /// The inputs read again within the batch at `now_ns`, the step's time from here; where they did work,
    /// the next poll's start is read afresh.
    fn read_inputs_within(&mut self, at: &mut BatchMark, now_ns: u64) {
        self.desk.now_ns.set(now_ns);
        let harvests = self.desk.live.harvests.get();
        if self.read_inputs() || self.desk.live.harvests.get() != harvests {
            at.mark = None;
        }
        at.inputs_read_ns = Some(now_ns);
        at.listed_at_read = self.desk.local.listed();
    }

    /// The loop's inputs onto the run queue: control, wakes from other threads, timers due, ready pollers and
    /// the driver's completions; then the tasks that woke themselves, behind all of them (`crate::queue`).
    /// Whether any input did work.
    fn read_inputs(&mut self) -> bool {
        if !self.inputs_pending() {
            self.desk.local.release_yielded();
            return false;
        }
        let mut did_work = self.drain_control();
        did_work |= self.drain_wakes();
        did_work |= self.expire_timers();
        did_work |= self.wake_ready_pollers();
        self.apply();
        did_work |= self.harvest_turn();
        self.desk.local.release_yielded();
        self.refresh_due();
        did_work
    }

    /// Whether the inputs may hold work (docs/runtime.md §3.10), from what tells without touching them: an
    /// intent asked, the time against `due_ns`, an idle shard with readiness waits (whose turn asks the
    /// driver at once), and the entry's flags for control and wakes from other threads. The simulation reads
    /// every turn, as its harvest does.
    fn inputs_pending(&self) -> bool {
        !self.core.real_time
            || self.desk.asked.get()
            || self.desk.now_ns.get() >= self.desk.due_ns.get()
            || (self.desk.local.is_empty() && !self.core.waiting.is_empty())
            || self
                .desk
                .entry
                .is_none_or(|entry| entry.wakes.is_pending() || entry.control_pending.is_pending())
    }

    /// Parks in the driver until a kick, a completion or `deadline_ns`. The parking announcement comes first
    /// and the inbox re-check second (the protocol and its loom model: [`crate::parking`]).
    pub fn park(&mut self, deadline_ns: Option<u64>) {
        self.park_with_cleanup(deadline_ns, false);
    }

    /// The owning run/block_on/Drop loop may await terminal channel completions.
    /// Manual and simulation park must instead return with cooperative work retained.
    pub(crate) fn park_owned(&mut self, deadline_ns: Option<u64>) {
        self.park_with_cleanup(deadline_ns, true);
    }

    fn park_with_cleanup(&mut self, deadline_ns: Option<u64>, block_cleanup: bool) {
        if self.core.exited {
            return;
        }
        // A wait closes the window, so nothing left for a step's end may wait past it.
        self.judge_deferred();
        if self.core.driver_lost {
            self.finish_hard_cancelled();
            if block_cleanup && !self.core.exited {
                self.park_cleanup();
            }
            return;
        }
        self.apply();
        if self.core.driver_lost {
            self.finish_hard_cancelled();
            if block_cleanup && !self.core.exited {
                self.park_cleanup();
            }
            return;
        }
        self.core.waited_for_ns = deadline_ns;
        self.core.attribution.wait_began();
        let (retrieved, lost) = match self.desk.entry {
            Some(entry) => self.park_announced(entry, deadline_ns),
            None => (true, self.wait_in_driver(deadline_ns)),
        };
        self.wait_ended(retrieved);
        if lost {
            self.fail_all();
            self.finish_hard_cancelled();
        }
    }

    /// The registered shard's park: announced, then a wait in the driver unless an inbox already holds work
    /// (the protocol of [`crate::parking`]), learning from a wait what the wake took and what the park
    /// cost. Whether it waited, and whether the driver was lost.
    fn park_announced(&mut self, entry: &'static Entry, deadline_ns: Option<u64>) -> (bool, bool) {
        let learns = self.core.real_time && self.core.wake.is_some();
        let mut switches_before_wait = None;
        let mut lost = false;
        // The CPU clock is read (a system call) only by a learning shard, only around a wait it makes, and
        // the second time only after a first. A tuple of the two readings read it after every park, learning
        // or not: that doubled the cost of the loop item the runtime calibrates its poll batch by, which
        // halved every shard's batch (`benchmark-results/rtloop-async-fill-bisect-20261010`).
        let mut cpu_before = None;
        let pending = self.has_inbound();
        let parked = entry.parking.park_unless_pending(
            || pending || entry.wakes.is_pending() || entry.control_pending.is_pending(),
            || {
                if learns {
                    switches_before_wait = attribution::voluntary_switches_now();
                    cpu_before = attribution::thread_cpu_now();
                    count(&self.desk.live.park_cost_reads, 1);
                }
                lost = self.wait_in_driver(deadline_ns);
            },
        );
        let Parked::Waited(woken) = parked else {
            return (false, lost);
        };
        if let Some(woken) = woken {
            self.note_wake(entry, woken, switches_before_wait);
        }
        if let Some(before) = cpu_before {
            count(&self.desk.live.park_cost_reads, 1);
            if let Some(after) = attribution::thread_cpu_now() {
                self.note_park_cost(entry, after.saturating_sub(before));
            }
        }
        (true, lost)
    }

    /// Driver loss closed admission and marked every live task. A single arena
    /// pass can retire ordinary cancellation without polling a cooperative future,
    /// invoking step recursively, or waiting for a receiver. Service ownership and
    /// any hard parent's live service children remain until their own Ready.
    fn finish_hard_cancelled(&mut self) {
        for index in 0..self.core.tasks.len() {
            let Ok(slot) = u32::try_from(index) else {
                break;
            };
            self.install_listed(slot);
            let ordinary = self.desk.task(slot).is_some_and(|cell| !cell.service.get());
            let ready = self.task_mut(slot).is_some_and(|task| {
                task.cancel_requested
                    && task.future.is_some()
                    && matches!(task.state, State::Idle | State::Queued)
            });
            if ordinary && ready {
                let start = self.now_ns();
                let quantum_ns = self.quantum_ns();
                let carried = self.desk.carried.get();
                registry::with_current_cell(self, |shard, current| {
                    shard.poll_slot(slot, start, quantum_ns, carried, current)
                });
            }
        }
        self.judge_deferred();
        self.apply();
        self.exit_if_drained();
    }

    fn exit_if_drained(&mut self) -> bool {
        let exit = self.core.shutting_down && self.running_tasks() == 0;
        if exit {
            self.core.exited = true;
            self.close_control();
            registry::note_arena_generation(self.desk.id, self.desk.generation_high.get());
            registry::note_exited(self.desk.id);
        }
        exit
    }

    /// Event-driven terminal park after the owner has observed native driver loss.
    /// The ordinary wake bitmap is published before this same announce/recheck protocol;
    /// the thread permit replaces only the unusable kernel kick/wait, never the payload.
    fn park_cleanup(&self) {
        let Some(entry) = self.desk.entry else {
            return;
        };
        entry.parking.park_unless_pending(
            || self.has_inbound(),
            || {
                std::thread::park();
            },
        );
    }

    /// Folds a park's measured wake into the online estimate (slates §4.3).
    fn note_wake(&mut self, entry: &Entry, woken: Woken, switches_before_wait: Option<u64>) {
        if self.core.wake.is_none() || !self.core.real_time {
            return;
        }
        if woken.stale() {
            count(&self.desk.live.wake_stale, 1);
            return;
        }
        let Some(latency) = woken.latency_ns() else {
            if woken.early() {
                count(&self.desk.live.wake_unslept, 1);
            }
            return;
        };
        if let (Some(before), Some(after)) =
            (switches_before_wait, attribution::voluntary_switches_now())
            && after == before
        {
            count(&self.desk.live.wake_unslept, 1);
            return;
        }
        let Some(estimate) = self.core.wake.as_mut() else {
            return;
        };
        estimate.record(latency);
        let mean = estimate.mean_ns();
        count(&self.desk.live.wake_samples, 1);
        self.desk.live.wake_cost_ns.set(mean);
        self.desk.quantum_ns.set(self.quantum_ns());
        entry.pulse.record_wake_cost(mean);
    }

    /// Folds one waited park's CPU on this thread into the cost of blocking, and the kicks its senders
    /// measured since the last fold.
    fn note_park_cost(&mut self, entry: &Entry, park_cpu_ns: u64) {
        self.core.park_cost.record(park_cpu_ns);
        let (sum, kicks) = entry.parking.kick_cpu();
        let (seen_sum, seen_kicks) = self.core.kicks_seen;
        if let Some(cost) = kicks
            .checked_sub(seen_kicks)
            .filter(|fresh| *fresh > 0)
            .and_then(|fresh| sum.saturating_sub(seen_sum).checked_div(fresh))
        {
            self.core.kick_cost.record(cost);
            self.core.kicks_seen = (sum, kicks);
        }
        // What blocking costs, as the counters report it, moves only here.
        self.desk
            .live
            .park_cost_ns
            .set(self.blocking_cost_ns().unwrap_or(0));
        self.desk
            .live
            .park_samples
            .set(self.core.park_cost.samples());
    }

    /// Folds one finished wait into the measured scheduler overrun: the shard waited for `deadline_ns` and
    /// this step, whose clock was just published, is the first to run after it.
    fn note_wait_overrun(&mut self, deadline_ns: u64) {
        let overrun = self.desk.now_ns.get().saturating_sub(deadline_ns);
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
            self.core.harvest_owed = false;
        }
        if self.core.real_time && self.core.attribution.armed() {
            let read = account_now(now);
            count(&self.desk.live.thread_accounts, 1);
            self.core.attribution.wait_ended(|| read);
        }
    }

    /// Attributes a poll's hold, from `poll_started_ns` (its start or its last go-on, docs/runtime.md §3.11)
    /// to `ended_ns`, against `quantum` (twice the step quantum): within it by the wall clock it is within;
    /// past it, the attribution windows decide whose it was ([`crate::attribution`]), at once or, judging by
    /// step, at the step's end ([`Shard::judge_deferred`]), for which it returns `None` now.
    #[cold]
    fn attribute_poll(
        &mut self,
        poll_started_ns: u64,
        ended_ns: u64,
        quantum: u64,
    ) -> Option<Attribution> {
        let wall_ns = ended_ns.saturating_sub(poll_started_ns);
        if wall_ns <= quantum {
            return None;
        }
        if !self.core.real_time {
            return Some(Attribution::Long);
        }
        if self.core.attribution.defers()
            && self.core.deferred.len() < self.core.deferred.capacity()
        {
            self.core.deferred.push(Deferred {
                overrun_ns: wall_ns.saturating_sub(quantum),
            });
            self.core.attribution.long_poll_deferred();
            return None;
        }
        let end = attribution::thread_account();
        count(&self.desk.live.thread_accounts, 1);
        Some(
            self.core
                .attribution
                .long_poll(poll_started_ns, ended_ns, end, quantum),
        )
    }

    /// The long polls left for the step's end, judged at one reading of the thread's account
    /// ([`Tracker::close_step`], [`attribution::judge_in_window`]) and counted as the polls' own attribution
    /// would have been.
    fn judge_deferred(&mut self) {
        if self.core.deferred.is_empty() {
            return;
        }
        let now = self.now_ns();
        let end = attribution::thread_account();
        count(&self.desk.live.thread_accounts, 1);
        let off_cpu_ns = self.core.attribution.close_step(now, end);
        let mut undecided = false;
        for deferred in self.core.deferred.drain(..) {
            let attributed = match (off_cpu_ns, end) {
                (_, None) => Attribution::NoClock,
                (None, Some(_)) => Attribution::NoWindow,
                (Some(off_cpu_ns), Some(_)) => {
                    attribution::judge_in_window(deferred.overrun_ns, off_cpu_ns)
                }
            };
            undecided |= matches!(attributed, Attribution::NoWindow | Attribution::Ambiguous);
            count_long_poll(&self.desk.live, attributed);
        }
        if undecided {
            self.core.attribution.undecided();
        }
    }

    /// The driver's blocking wait until a kick, a completion or `deadline_ns`, its completions queued; true
    /// when the driver was lost.
    fn wait_in_driver(&mut self, deadline_ns: Option<u64>) -> bool {
        count(&self.desk.live.waits, 1);
        if let Some(entry) = self.desk.entry {
            entry.pulse.record_waits(self.desk.live.waits.get());
        }
        let timeout = deadline_ns.map(|d| d.saturating_sub(self.core.driver.now_ns()));
        let _ = self.receive_completions(timeout);
        self.core.driver_lost
    }

    /// Driver loss has already marked cancellation. The normal loop keeps owning
    /// service futures until their event-driven cleanup returns Ready, then exits.
    fn fail_all(&mut self) {
        self.cancel_all();
    }

    /// Closes admission before cancellation; called outside nested task polling, or marks
    /// cancellation for the remainder of the current step when an initial arm loses the driver.
    fn lose_driver(&mut self) {
        if self.core.driver_lost {
            return;
        }
        self.core.driver_lost = true;
        count(&self.desk.live.driver_lost, 1);
        self.core.shutting_down = true;
        self.desk.accepting.set(false);
        if let Some(entry) = self.desk.entry {
            entry
                .accepting
                .store(false, std::sync::atomic::Ordering::Release);
            entry.stop.store(true, std::sync::atomic::Ordering::Release);
            // No native wait can still be parked: loss was observed by this owner.
            entry
                .cleanup_mode
                .store(true, std::sync::atomic::Ordering::Release);
        }
        self.close_control();
        self.cancel_all();
    }

    /// Closes the control queue: what is queued is dropped with the desk entered, as a task's future is.
    fn close_control(&mut self) {
        let Some(entry) = self.desk.entry else {
            return;
        };
        let entered = registry::enter(&self.desk);
        entry.close_control();
        drop(entered);
    }

    fn finish_cancelled(&mut self) {
        while self.running_tasks() > 0 {
            let outcome = self.step();
            if outcome.exit {
                break;
            }
            if !outcome.did_work {
                self.park_owned(outcome.next_deadline_ns);
            }
        }
    }

    fn drain_control(&mut self) -> bool {
        let Some(entry) = self.desk.entry else {
            return false;
        };
        // Take (read and clear in one atomic step) before draining: a send that lands during the drain
        // publishes the flag again and is seen on the next step at the latest. A load first, so a step with
        // no message writes nothing to the line its senders write.
        if !entry.control_pending.is_pending() || !entry.control_pending.take() {
            return false;
        }
        let batch = self.core.config.batch.max(1);
        let mut drained: usize = 0;
        while drained < batch {
            let Some(message) = entry.take_control() else {
                break;
            };
            drained = drained.saturating_add(1);
            count(&self.desk.live.controls, 1);
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
            count(&self.desk.live.abandons_swept, visited);
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
        count(&self.desk.live.wakes_to_free_slots, free);
        count(
            &self.desk.live.wakes_foreign,
            u64::try_from(woken).unwrap_or(u64::MAX),
        );
        woken > 0
    }

    /// Wakes every poller whose ring is ready; a poller whose task ended is dropped.
    fn wake_ready_pollers(&mut self) -> bool {
        if self.core.pollers.is_empty() {
            return false;
        }
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
                count(&self.desk.live.poller_wakes, 1);
                desk.wake_local(poller.slot);
            }
        }
        any
    }

    fn handle_control(&mut self, message: Control) {
        match message {
            Control::Spawn(request) => {
                if let Err(RtError::TooManyTasks { .. }) = self.spawn_request(*request) {
                    count(&self.desk.live.admission_refused, 1);
                }
            }
            Control::SpawnService(request) => {
                if let Err(RtError::TooManyTasks { .. }) = self.spawn_request_with(*request, true) {
                    count(&self.desk.live.admission_refused, 1);
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
        if self.core.driver_lost {
            return false;
        }
        // The step's published time: a timer that fell due since then fires at the next step.
        let now = self.desk.now_ns.get();
        let mut fired = std::mem::take(&mut self.core.fired);
        self.core.timers.advance(now, &mut fired);
        let any = !fired.is_empty();
        for (slot, word) in fired.drain(..) {
            count(&self.desk.live.timers_fired, 1);
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

    /// Polls the task in `slot`, timed from `start` and judged against `quantum_ns`, entering the desk through
    /// `current`; the time the poll ended, or `start` when nothing ran. Inlined into the batch's loop, so a
    /// poll does not save and restore the loop's registers around it.
    #[inline(always)]
    fn poll_slot(
        &mut self,
        slot: u32,
        start: u64,
        quantum_ns: u64,
        carried: Option<crate::machine::clock::Carried>,
        current: Option<registry::CurrentCell<'_>>,
    ) -> u64 {
        let Some((mut future, service, word)) = self.take_to_poll(slot, current) else {
            return start;
        };
        // Never dropped: the vtable's drop does nothing (`crate::waker`), and an indirect call to it a poll is
        // all a drop would add.
        let waker = std::mem::ManuallyDrop::new(waker_for(word));
        let mut cx = Context::from_waker(&waker);
        self.desk.current.set(word.word());
        self.desk.now_ns.set(start);
        let entered = registry::enter_in(current, &self.desk);
        // The poll is the task's code: inside an unwind boundary of the shard's own, so a task that
        // panics ends there, typed, and costs the shard and its other tasks nothing (CLAUDE.md §1).
        let poll = crate::unwind::contained(|| future.poll(&mut cx));
        drop(entered);
        let ended = self.carried_from(carried).max(start);
        self.desk.current.set(crate::shard::NO_TASK);
        // A request served in this poll opens the idle window from where the poll ended, not where it began:
        // the spin covers the moments after the shard goes idle, however long the serving took.
        if self.desk.activity_noted.get() {
            self.desk.activity_noted.set(false);
            self.desk.activity_ns.set(Some(ended));
        }
        // What the poll asked takes effect before its result is recorded: a parent that spawned children and
        // finished in one poll has them installed, linked and so cancelled with it.
        self.apply();
        let elapsed = ended.saturating_sub(start);
        // How long the poll held the shard unasked (docs/runtime.md §3.11): from its start, or from the last
        // time it asked `wanted` and was let go on; long past twice the quantum, which a task that asks once a
        // quantum and yields when told to never reaches.
        let held_from = start.max(self.desk.checked_ns.get());
        let threshold = quantum_ns.saturating_mul(2);
        let attributed = if ended.saturating_sub(held_from) > threshold {
            self.attribute_poll(held_from, ended, threshold)
        } else {
            None
        };
        let Some(poll) = poll else {
            self.unwound(slot, future, word, current);
            return ended;
        };
        self.record_poll(elapsed, attributed);
        self.put_back_or_finish(
            slot,
            future,
            matches!(poll, Poll::Ready(())),
            service,
            current,
        );
        ended
    }

    /// The future of the task in `slot`, taken out of its slot for a poll, with whether the task is a service and
    /// its word; `None` when the slot holds no task to poll (finishing, done, being polled, or empty), and for
    /// an ordinary task whose cancellation was asked, which ends here instead, at its poll boundary.
    #[inline(always)]
    fn take_to_poll(
        &mut self,
        slot: u32,
        current: Option<registry::CurrentCell<'_>>,
    ) -> Option<(TaskFuture, bool, Encoded)> {
        // A task listed at its spawn, with no parent, is installed as it is first polled (`ShardContext::claim`).
        if slot_mut(&mut self.core.tasks, slot)?.state == State::Done {
            let incoming = self.desk.task(slot)?.incoming.take()?;
            self.install_slot(slot, incoming);
        }
        // Fixed while the slot holds the task: the poll and what it asks cannot move them.
        let task = slot_mut(&mut self.core.tasks, slot)?;
        let (service, word) = (task.service, Encoded::from_word(task.word));
        if service
            && self
                .desk
                .task(slot)
                .is_some_and(|cell| cell.cancelled.get())
        {
            self.cancel_children_once(slot);
        }
        let task = slot_mut(&mut self.core.tasks, slot)?;
        if matches!(task.state, State::Finishing | State::Done | State::Running) {
            return None;
        }
        let future = task.future.take()?;
        if task.cancel_requested && !service {
            task.state = State::Finishing;
            self.drop_entered(future, current);
            self.finish(slot, Outcome::Cancelled);
            return None;
        }
        task.state = State::Running;
        Some((future, service, word))
    }

    /// Counts a poll that ran `elapsed` and was `attributed`.
    #[inline(always)]
    fn record_poll(&mut self, elapsed: u64, attributed: Option<Attribution>) {
        count(&self.desk.live.polls, 1);
        if let Some(attributed) = attributed {
            count_long_poll(&self.desk.live, attributed);
        }
        // A store only when the longest grew, which most polls leave as it was.
        if elapsed > self.desk.live.longest_step_ns.get() {
            self.desk.live.longest_step_ns.set(elapsed);
        }
    }

    /// After a poll: the future back in its slot, or, when it returned or an ordinary task's cancellation was
    /// asked while it ran, the task finished.
    #[inline(always)]
    fn put_back_or_finish(
        &mut self,
        slot: u32,
        future: TaskFuture,
        done: bool,
        service: bool,
        current: Option<registry::CurrentCell<'_>>,
    ) {
        let Some(task) = slot_mut(&mut self.core.tasks, slot) else {
            return;
        };
        if done || (task.cancel_requested && !service) {
            let cancelled = task.cancel_requested;
            self.drop_entered(future, current);
            self.finish(
                slot,
                if cancelled && service || !done {
                    Outcome::Cancelled
                } else {
                    Outcome::Completed
                },
            );
            return;
        }
        task.future = Some(future);
        task.state = State::Idle;
    }

    /// A poll that unwound: its future is in no state to poll again, so it is dropped and the task ends.
    #[cold]
    fn unwound(
        &mut self,
        slot: u32,
        future: TaskFuture,
        word: Encoded,
        current: Option<registry::CurrentCell<'_>>,
    ) {
        if let Some(task) = slot_mut(&mut self.core.tasks, slot) {
            task.state = State::Finishing;
        }
        self.drop_entered(future, current);
        self.finish(slot, Outcome::Unwound);
        self.desk.note_unwound(TaskId(word));
    }

    /// Drops a task's future with the desk entered: its destructors may wake or cancel other tasks of this
    /// shard, or let their timers go. The drop is the task's code, inside an unwind boundary of the shard's
    /// own; one that unwinds is counted.
    fn drop_entered(&mut self, future: TaskFuture, current: Option<registry::CurrentCell<'_>>) {
        let entered = registry::enter_in(current, &self.desk);
        let desk = &*self.desk;
        // A frame goes back to the shard's store for its next spawn of that size, its future's drop
        // returning or unwinding (`crate::frames`).
        let dropped = crate::unwind::contained(move || match future {
            TaskFuture::Boxed(future) => drop(future),
            TaskFuture::Framed(future) => desk.frames.release(future),
        });
        drop(entered);
        if dropped.is_none() {
            count(&self.desk.live.unwound, 1);
        }
    }

    /// Wakes the task `word` names: on this shard directly, elsewhere through the registry.
    fn wake_word(&self, word: Encoded) {
        if word.shard() == self.desk.id {
            self.desk.wake_local(word.slot());
        } else {
            registry::wake(word);
        }
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
                count(&self.desk.live.completed, 1);
            }
            Outcome::Cancelled => {
                count(&self.desk.live.cancelled, 1);
            }
            Outcome::Unwound => {
                count(&self.desk.live.unwound, 1);
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
            if let Some(cell) = self.desk.task(child) {
                cell.cancelled.set(true);
            }
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
        self.desk.free_slot(slot);
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

    /// Every cancellation writer publishes the same level before scheduling the task.
    fn request_cancel(&mut self, slot: u32) {
        if let Some(task) = self.task_mut(slot)
            && task.state != State::Done
        {
            task.cancel_requested = true;
            if let Some(cell) = self.desk.task(slot) {
                cell.cancelled.set(true);
            }
            self.desk.wake_local(slot);
        }
    }

    /// Direct children are scheduled for cancellation before a service can await them.
    /// A cancelled service child performs its own direct pass when polled: no recursive
    /// traversal stack, additional allocation, or unbounded linked-list walk is introduced.
    fn cancel_children_once(&mut self, slot: u32) {
        let Some(cell) = self.desk.task(slot) else {
            return;
        };
        if cell.cancel_children.replace(true) {
            return;
        }
        let mut child = self.task_mut(slot).map_or(NO_LINK, |task| task.first_child);
        let mut budget = self.core.tasks.len();
        while child != NO_LINK && budget > 0 {
            budget = budget.saturating_sub(1);
            let Some(next) = self.task_mut(child).map(|task| task.next_sibling) else {
                break;
            };
            self.request_cancel(child);
            child = next;
        }
    }

    fn cancel_all(&mut self) {
        for (slot, task) in self.core.tasks.iter_mut().enumerate() {
            let Ok(slot) = u32::try_from(slot) else {
                continue;
            };
            let Some(cell) = self.desk.task(slot) else {
                continue;
            };
            if cell.phase.get() != Phase::Live {
                continue;
            }
            // A task listed at its spawn and not yet polled has no slot of the loop's to mark: its install
            // carries the desk's mark (`install_slot`).
            if task.state != State::Done {
                task.cancel_requested = true;
            }
            cell.cancelled.set(true);
            self.desk.wake_local(slot);
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

/// The thread's account and the shard clock now: an attribution window's start.
fn account_now(now_ns: u64) -> Option<(attribution::ThreadAccount, u64)> {
    attribution::thread_account().map(|account| (account, now_ns))
}

/// Counts a poll past the step quantum by the wall clock under whoever held it.
fn count_long_poll(counters: &LiveCounters, attributed: Attribution) {
    if attributed.is_tasks() {
        count(&counters.long_steps, 1);
    }
    if attributed == Attribution::Blocked {
        count(&counters.blocked_steps, 1);
    }
    if attributed == Attribution::Preempted {
        count(&counters.preempted_steps, 1);
    }
    if attributed.is_unattributed() {
        count(&counters.unattributed_steps, 1);
    }
}

/// Adds `by` to a counter, saturating: a counter never wraps back to small.
fn count(counter: &std::cell::Cell<u64>, by: u64) {
    counter.set(counter.get().saturating_add(by));
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
