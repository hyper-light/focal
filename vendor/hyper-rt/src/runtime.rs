//! The runtime: the configuration derived from the machine's calibration (docs/runtime.md §10), the OS
//! runtime that owns one thread per shard, and the local runtime that runs one shard on the calling thread
//! and can run a future to completion there ([`LocalRuntime::block_on`], §4).

use std::future::Future;
use std::thread::JoinHandle;

use crate::control::Control;
use crate::derived;
use crate::driver::{DriverSeed, Kick, Prepared, os_driver};
use crate::error::RtError;
use crate::machine::Derived;
use crate::machine::calibration::{Calibration, Constants};
use crate::machine::probes::Pinning;
use crate::registry;
use crate::shard::{Kept, ShardContext, ShardId, TaskId};
use crate::shard_loop::{Counters, Shard, ShardSeed, StepOutcome};
use crate::task::{AdmissionReceipt, SpawnRequest};

/// Submits a detached task to the shard `holder` names, from any thread and without a runtime handle
/// (a submitter that may outlive the runtime — an observation still pending while its daemon stops),
/// and returns the receipt of its admission; refused `ShardGone` when the slot is free or held by a
/// later registration, `ControlFull` when the holder's control channel is full.
pub fn submit_to_holder<F: Future<Output = ()> + Send + 'static>(
    holder: registry::SlotHolder,
    future: F,
) -> Result<AdmissionReceipt, RtError> {
    let (request, receipt) = SpawnRequest::with_receipt(Box::pin(future), None);
    registry::send_control_to_holder(holder, Control::Spawn(Box::new(request)))?;
    Ok(receipt)
}

/// Requests a task's cancellation from any thread without a runtime handle (the message
/// [`Runtime::cancel`] sends); refused when the task's shard is gone or its control channel is full.
/// A task that already ended is a stale word the shard ignores.
pub fn cancel_task(task: TaskId) -> Result<(), RtError> {
    registry::send_control(task.0.shard(), Control::Cancel(task.0))
}

/// The runtime's configuration; every number is derived or measured by the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Shards to run.
    pub shards: u16,
    /// The admission limit per shard (Little's law on the measured request rate and p99 service
    /// time; see [`admission_limit`]).
    pub tasks_per_shard: usize,
    /// Timers a shard may hold at once.
    pub timers_per_shard: usize,
    /// Readiness registrations a shard's tasks may queue between two of its loop's drains: the most socket
    /// waits its tasks start in one poll each (docs/runtime.md §3.4); past it a registration is refused
    /// `Capacity`.
    pub interests_per_shard: usize,
    /// Entries in each inbound ring.
    pub ring_entries: usize,
    /// The step budget the watchdog counts against, in nanoseconds.
    pub step_budget_ns: u64,
    /// The timing wheel's tick, in nanoseconds.
    pub timer_tick_ns: u64,
    /// The bound on items processed per loop phase.
    pub batch: usize,
    /// Whether to pin shard threads to cores.
    pub pin: bool,
    /// The core ids shard threads are pinned to, in shard order (empty: the OS chooses).
    pub cores: Vec<u32>,
    /// The base page in bytes, for sizing the task slab's segments.
    pub page_bytes: usize,
    /// How long an idle shard spins checking its rings before parking, while a client is active
    /// (the 2-competitive bound: the measured wake cost).
    pub spin_ns: u64,
    /// The wake estimate each shard refines from its own kicked parks (§4.1, §4.3): the boot probe's mean
    /// as the prior, its window, and the idle window's multiple of it. While a shard tracks, its step
    /// quantum and idle spin follow the estimate rather than the fixed `step_budget_ns` and `spin_ns`;
    /// `None` is a hand-written configuration with no measured prior to track (a test harness's fixed
    /// quanta, an internal helper runtime), which keeps its fixed values.
    pub wake_tracking: Option<WakeTracking>,
}

/// What a shard needs to refine its wake estimate after boot (§4.1: the boot probe's mean converges
/// slowly under a heavy tail — a virtual machine's needs 38,000–75,000 wakes where the probe's budget buys
/// a few thousand — so the estimate a shard sizes its spin and its quantum by keeps learning from the
/// wakes it actually pays).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WakeTracking {
    /// The boot probe's mean wake, nanoseconds: the estimate's seed.
    pub prior_ns: u64,
    /// The estimate's weighting shift, about `2^shift` wakes ([`crate::machine::wake::WakeLatency::estimate_shift`]).
    pub shift: u32,
    /// The idle spin window as a multiple of the estimate (1 for the runtime's own spin-then-park; the
    /// daemon's idle window sets its ratio).
    pub idle_ratio: u64,
}

/// The readiness registrations a shard of `tasks` tasks may queue between drains: each task waits on at
/// most one read and one write per poll in the common case, so twice its tasks (a task that waits on more
/// handles at once in one poll is the consumer's to configure).
pub fn interests_for(tasks: usize) -> usize {
    tasks.saturating_mul(2)
}

/// A shard's control-queue depth from the derived `control_entries` (Little's law at the overflow target), at
/// least one: on a machine whose wake p99 is no longer than a syscall's median the law asks for one.
fn control_depth(derived: u64) -> usize {
    usize::try_from(derived).unwrap_or(usize::MAX).max(1)
}

impl RuntimeConfig {
    /// Checks the representation bounds before a driver, queue or arena is acquired. Zero-sized
    /// arenas keep their existing refusal behavior; indices must fit their actual encoded fields.
    pub(crate) fn validate_shape(&self) -> Result<(), RtError> {
        if self.tasks_per_shard.checked_sub(1).is_some_and(|last| {
            u32::try_from(last).map_or(true, |last| last > crate::mem::Encoded::MAX_SLOT)
        }) {
            return Err(RtError::BadConfig {
                what: "task capacity exceeds the encoded slot field",
            });
        }
        for (count, what) in [
            (
                self.timers_per_shard,
                "timer capacity exceeds its u32 index field",
            ),
            (
                self.interests_per_shard,
                "readiness capacity exceeds its u32 index field",
            ),
            (
                self.ring_entries,
                "driver ring capacity exceeds its u32 entry field",
            ),
        ] {
            if u32::try_from(count).is_err() {
                return Err(RtError::BadConfig { what });
            }
        }
        Ok(())
    }

    /// A configuration from the machine's calibration and the constants the consumer's policy derived from
    /// it ([`Calibration::constants`]): the shard count and their cores from the placement, the tick, step
    /// budget, spin window and control depth from the derived constants, and the batch bound against the
    /// derived budget (see [`RuntimeConfig::calibrate_batch`]). The task and timer limits come from the
    /// caller's measurements (Little's law, [`admission_limit`]).
    pub fn from_calibration(
        calibration: &Calibration,
        constants: &Constants,
        tasks_per_shard: usize,
        timers_per_shard: usize,
    ) -> Self {
        let ring_entries = control_depth(constants.control_entries.get());
        let cores = constants
            .placement
            .fixed
            .as_ref()
            .map(|fixed| fixed.shards.clone())
            .unwrap_or_default();
        let mut config = Self {
            shards: constants.placement.shards.get(),
            tasks_per_shard,
            timers_per_shard,
            interests_per_shard: interests_for(tasks_per_shard),
            ring_entries,
            step_budget_ns: constants.spin_ns.get(),
            timer_tick_ns: constants.tick_ns.get(),
            batch: ring_entries,
            // Fixed only to cores the process owns; none means the OS places the shards.
            pin: !cores.is_empty(),
            cores,
            page_bytes: usize::try_from(calibration.facts.page.base).unwrap_or(1),
            spin_ns: constants.spin_ns.get(),
            wake_tracking: Some(WakeTracking {
                prior_ns: constants.spin_ns.get(),
                shift: calibration.wake.estimate_shift(),
                idle_ratio: 1,
            }),
        };
        config.batch = config
            .calibrate_batch(constants.batch_budget_ns.get())
            .get();
        config
    }

    /// The batch bound: the latency budget divided by the measured cost of one loop item (a task
    /// poll of a trivial future through the whole loop), measured here and now on a simulated
    /// shard, so the bound is never a guess (§4.3, "batch bound = latency budget / measured
    /// per-item cost").
    pub fn calibrate_batch(&self, latency_budget_ns: u64) -> Derived<usize> {
        let per_item_ns = measured_item_cost_ns(self);
        derived!(
            usize::try_from(
                latency_budget_ns
                    .checked_div(per_item_ns.max(1))
                    .unwrap_or(0)
            )
            .unwrap_or(usize::MAX)
            .clamp(1, self.ring_entries.max(1)),
            "latency budget / measured per-item loop cost, clamped to [1, ring entries]",
            [
                "rt.latency_budget_ns",
                "rt.item_cost_ns (measured at start)",
                "rt.ring_entries"
            ]
        )
    }

    /// Task slots per slab segment: one base page of slots.
    pub fn segment_tasks(&self) -> usize {
        derived!(
            self.page_bytes
                .checked_div(std::mem::size_of::<crate::task::TaskSlot>())
                .unwrap_or(1)
                .max(1),
            "base page / task slot size",
            ["page.base"]
        )
        .get()
    }
}

/// Logs, once per process, a shard whose fixed core the OS refused where the OS pins (Linux, Windows): the
/// placement names only cores the process may run on, so a refusal means its mask changed after the facts
/// were read — a fault to show, not to swallow; the shard then runs where the scheduler places it and its
/// counters say so ([`Counters::pin_refused`]). macOS takes affinity as a hint that Apple silicon refuses by
/// design, so there a refusal is counted, not logged.
fn log_pin_refused(_shard: u16, _core: u32) {
    // A library reports through what it returns: the refusal is the shard's counter
    // (`Counters::pin_refused`), which the consumer reads and logs as it chooses.
}

/// Measures the cost of one loop item on a simulated shard: spawn a trivial task, run it to
/// completion, reap it. The simulation driver has no OS resources, so this costs microseconds.
fn measured_item_cost_ns(config: &RuntimeConfig) -> u64 {
    let probe = RuntimeConfig {
        shards: 1,
        ..config.clone()
    };
    let Ok(mut sim) = crate::sim::SimRuntime::new(&probe, 0) else {
        return 1;
    };
    let shard = sim.shard_ids().first().copied();
    let Some(shard) = shard else { return 1 };
    let started = crate::machine::clock::monotonic_ns();
    /// Shape: enough items to amortize the clock reads (two per batch) below one percent.
    const ITEMS: u64 = 4096;
    for _ in 0..ITEMS {
        let _ = sim.spawn_on(shard, async {});
        sim.run_until_idle();
    }
    crate::machine::clock::monotonic_ns()
        .saturating_sub(started)
        .checked_div(ITEMS)
        .unwrap_or(0)
}

/// Little's law: the tasks in flight at a measured request rate and p99 service time.
pub fn admission_limit(requests_per_second: u64, p99_service_ns: u64) -> Derived<usize> {
    /// Format: nanoseconds per second.
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let l = u128::from(requests_per_second)
        .saturating_mul(u128::from(p99_service_ns))
        .checked_div(NANOS_PER_SECOND)
        .unwrap_or(0);
    derived!(
        usize::try_from(l).unwrap_or(usize::MAX).max(1),
        "request rate × p99 service time (Little's law)",
        ["rt.request_rate", "rt.service_p99_ns"]
    )
}

/// The slot's kick for an OS driver: its descriptor (Unix) or its completion port (Windows), owned by
/// the slot and closed when the slot retires the registration.
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
pub(crate) fn register_kick(fd: Option<std::os::fd::OwnedFd>) -> registry::RegisterKick {
    match fd {
        Some(fd) => registry::RegisterKick::Descriptor(fd, Kick::Kqueue),
        None => registry::RegisterKick::Kick(Kick::None),
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn register_kick(fd: Option<std::os::fd::OwnedFd>) -> registry::RegisterKick {
    match fd {
        Some(fd) => registry::RegisterKick::Descriptor(fd, Kick::Eventfd),
        None => registry::RegisterKick::Kick(Kick::None),
    }
}

#[cfg(windows)]
pub(crate) fn register_kick(port: Option<crate::iocp::Port>) -> registry::RegisterKick {
    match port {
        Some(port) => registry::RegisterKick::Port(port),
        None => registry::RegisterKick::Kick(Kick::None),
    }
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn register_kick(_fd: Option<()>) -> registry::RegisterKick {
    registry::RegisterKick::Kick(Kick::None)
}

/// One shard's worker: its id and the thread running it, whose result is the shard's counters or why it
/// could not run.
struct Worker {
    id: u16,
    holder: registry::SlotHolder,
    thread: JoinHandle<Result<Counters, RtError>>,
}

/// The OS runtime: one thread per shard. It owns its workers from start to a terminal state (AUD-29-12):
/// `start` is transactional — every shard's context is built and acknowledged before it returns, and any
/// failure stops and joins the workers that did start and gives every registry slot back before the typed
/// refusal returns — and a runtime dropped without [`Runtime::shutdown`] stops and joins its workers in
/// `Drop`. Until 2026-09-30 a worker whose context failed to build returned default counters under a
/// successful start, a failure part-way through `start` detached the threads already spawned and kept
/// their slots, and dropping the value detached every worker.
pub struct Runtime {
    workers: Vec<Worker>,
    ids: Vec<ShardId>,
    notes: Vec<String>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("shards", &self.ids)
            .finish()
    }
}

impl Drop for Runtime {
    /// A runtime dropped without [`Runtime::shutdown`] stops, cancels and joins its workers and gives their
    /// slots back, exactly as `shutdown` would; a worker's failure is reported on the error stream, the one
    /// place a drop can put it.
    fn drop(&mut self) {
        if self.workers.is_empty() {
            return;
        }
        if stop(std::mem::take(&mut self.workers)).is_err() {
            registry::note_unreported_failure();
        }
    }
}

/// A shard's worker body: builds the shard on this thread (pinned first, so its tables are first touched
/// on the shard's own core), acknowledges the build — or its refusal — on `ready`, then runs the loop.
/// The acknowledgement's sender is dropped with it, so a worker that ends before acknowledging is seen as
/// gone rather than awaited.
fn run_worker(
    seed: ShardSeed,
    core: Option<u32>,
    ready: std::sync::mpsc::SyncSender<(u16, Result<(), RtError>)>,
) -> Result<Counters, RtError> {
    let id = seed.id;
    let pinned = core.map(|core| (core, crate::machine::probes::pin_current_thread(core)));
    let mut shard = match Shard::build(seed) {
        Ok(shard) => shard,
        Err(error) => {
            let _ = ready.send((id, Err(error.clone())));
            return Err(error);
        }
    };
    // The shard's CPU clock, for observers that count their budget in its own time.
    registry::record_cpu_clock(id);
    let _ = ready.send((id, Ok(())));
    drop(ready);
    if let Some((core, Pinning::Refused)) = pinned {
        shard.note_pin_refused();
        log_pin_refused(id, core);
    }
    shard.run();
    let counters = shard.counters();
    let lost = shard.driver_lost();
    drop(shard);
    registry::note_reclaimed();
    if lost {
        Err(RtError::DriverLost)
    } else {
        Ok(counters)
    }
}

/// Stops every worker (a shutdown message, retried while the shard's control channel is full), joins them
/// all, and then gives every slot back — after every join, never before, since a shard's pair rings are lent
/// to its peers. The counters in shard order, or the first worker's failure (every worker is still joined
/// and every slot still given back).
fn stop(workers: Vec<Worker>) -> Result<Vec<Counters>, RtError> {
    for worker in &workers {
        // The stop takes no slot of the bounded control channel, so a full channel cannot hold it back and
        // nothing here waits (§15 item 9: the send used to be retried with `yield_now`, unbounded). Before
        // 2026-09-17 a refused Shutdown was dropped and the join waited for good (`tests/admission.rs`).
        // `ShardGone`: the shard's slot is already free, and the join below reports how its thread ended.
        let _ = registry::request_stop(worker.id);
    }
    let holders: Vec<registry::SlotHolder> = workers.iter().map(|worker| worker.holder).collect();
    let mut counters = Vec::with_capacity(workers.len());
    let mut failure = None;
    for worker in workers {
        let id = worker.id;
        match worker.thread.join() {
            Ok(Ok(shard)) => counters.push(shard),
            Ok(Err(error)) => {
                failure.get_or_insert(error);
            }
            Err(_) => {
                failure.get_or_insert(RtError::WorkerFailed { shard: id });
            }
        }
    }
    for holder in holders {
        registry::unregister(holder);
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(counters),
    }
}

/// Gives back the slots of seeds that never became workers.
fn release_seeds(seeds: Vec<ShardSeed>) {
    let holders: Vec<registry::SlotHolder> = seeds.iter().map(|seed| seed.holder).collect();
    drop(seeds);
    for holder in holders {
        registry::unregister(holder);
    }
}

/// Registers `config.shards` seeds over drivers `prepare` makes, pairing their rings; on any refusal every
/// slot claimed so far is given back.
fn register_seeds(
    config: &RuntimeConfig,
    prepare: &mut dyn FnMut() -> Result<Prepared, RtError>,
    notes: &mut Vec<String>,
) -> Result<Vec<ShardSeed>, RtError> {
    let mut seeds = Vec::new();
    for _ in 0..config.shards {
        let registered = prepare().and_then(|prepared| {
            notes.extend(prepared.notes);
            ShardSeed::register(config, prepared.seed, register_kick(prepared.kick_fd))
        });
        match registered {
            Ok(seed) => seeds.push(seed),
            Err(error) => {
                release_seeds(seeds);
                return Err(error);
            }
        }
    }
    Ok(seeds)
}

impl Runtime {
    /// Starts `config.shards` shard threads on the OS drivers.
    pub fn start(config: &RuntimeConfig) -> Result<Runtime, RtError> {
        config.validate_shape()?;
        let entries = u32::try_from(config.ring_entries).map_err(|_| RtError::BadConfig {
            what: "driver ring capacity exceeds its u32 entry field",
        })?;
        Self::start_with(config, &mut || os_driver(entries))
    }

    /// Starts `config.shards` shard threads over the drivers `prepare` makes, one call per shard: the OS
    /// drivers ([`Runtime::start`]) or a test double, as [`LocalRuntime::with_driver`] takes one. Returns once
    /// every shard's context is built on its own thread; any refusal — a driver `prepare` refuses, a
    /// registration, a thread the OS will not spawn, a context that fails to build — stops and joins the
    /// workers already started, gives every slot back, and returns that refusal.
    pub fn start_with(
        config: &RuntimeConfig,
        prepare: &mut dyn FnMut() -> Result<Prepared, RtError>,
    ) -> Result<Runtime, RtError> {
        config.validate_shape()?;
        let mut notes = Vec::new();
        let seeds = register_seeds(config, prepare, &mut notes)?;
        let ids: Vec<ShardId> = seeds.iter().map(|s| ShardId(s.id)).collect();
        let (ready, acknowledged) = std::sync::mpsc::sync_channel(seeds.len().max(1));
        let mut workers = Vec::with_capacity(seeds.len());
        let mut seeds = seeds.into_iter().enumerate();
        while let Some((index, seed)) = seeds.next() {
            let core = if config.pin {
                config.cores.get(index).copied()
            } else {
                None
            };
            let id = seed.id;
            let holder = seed.holder;
            let ready = ready.clone();
            #[allow(
                clippy::disallowed_methods,
                reason = "the runtime owns one thread per shard (docs/runtime.md §3)"
            )]
            let spawned = std::thread::Builder::new()
                .name(format!("hyper-rt-shard-{id}"))
                .spawn(move || run_worker(seed, core, ready));
            match spawned {
                Ok(thread) => workers.push(Worker { id, holder, thread }),
                Err(error) => {
                    // The seed moved into the refused closure and is dropped with it; its slot, and the rest, go back.
                    registry::unregister(holder);
                    release_seeds(seeds.map(|(_, seed)| seed).collect());
                    let refusal = RtError::DriverRefused {
                        call: "thread spawn",
                        code: error.raw_os_error(),
                    };
                    roll_back(workers, &refusal);
                    return Err(refusal);
                }
            }
        }
        drop(ready);
        if let Err(error) = await_readiness(&acknowledged, &workers) {
            roll_back(workers, &error);
            return Err(error);
        }
        Ok(Runtime {
            workers,
            ids,
            notes,
        })
    }

    /// The shard ids, in order.
    pub fn shard_ids(&self) -> &[ShardId] {
        &self.ids
    }

    /// The driver notes (which driver, which flags, which fallbacks).
    pub fn notes(&self) -> &[String] {
        &self.notes
    }

    /// Spawns a detached task on a shard from any thread; refused when the shard's control
    /// channel is full (its admission limit) or the shard is gone. `Ok` is a **submission**: the request
    /// is in the shard's control channel, and whether the shard admits it is answered only to a receipt
    /// ([`Self::spawn_on_with_receipt`]).
    pub fn spawn_on<F: Future<Output = ()> + Send + 'static>(
        &self,
        shard: ShardId,
        future: F,
    ) -> Result<(), RtError> {
        let request = Box::new(SpawnRequest::new(Box::pin(future), None));
        registry::send_control(shard.0, Control::Spawn(request))
    }

    /// Spawns a detached task on a shard from any thread and returns the receipt of its admission
    /// ([`AdmissionReceipt`]): refused here when the shard's control channel is full or the shard is gone;
    /// otherwise the receipt reports, once the shard drains the request, whether the task was admitted
    /// (and as which task), refused (the arena full), or terminated unadmitted (the shard shutting down).
    pub fn spawn_on_with_receipt<F: Future<Output = ()> + Send + 'static>(
        &self,
        shard: ShardId,
        future: F,
    ) -> Result<AdmissionReceipt, RtError> {
        let (request, receipt) = SpawnRequest::with_receipt(Box::pin(future), None);
        registry::send_control(shard.0, Control::Spawn(Box::new(request)))?;
        Ok(receipt)
    }

    /// The registration holding `shard`'s slot, for a submitter that must reach this runtime's shard and
    /// never a later holder of its slot ([`submit_to_holder`]); `None` when the shard is gone.
    pub fn holder_of(&self, shard: ShardId) -> Option<registry::SlotHolder> {
        registry::holder_of(shard.0)
    }

    /// Requests a task's cancellation from any thread.
    pub fn cancel(&self, task: TaskId) -> Result<(), RtError> {
        registry::send_control(task.0.shard(), Control::Cancel(task.0))
    }

    /// Shuts every shard down (cancelling what runs), joins every worker and gives every slot back; the
    /// counters in shard order, or the first worker's failure (typed; every worker is still joined and every
    /// slot still given back).
    pub fn shutdown(mut self) -> Result<Vec<Counters>, RtError> {
        stop(std::mem::take(&mut self.workers))
    }
}

/// Stops and joins the workers of a start that failed with `refusal`, giving every slot back. A failure of
/// the rollback other than the refusal itself (a sibling that panicked meanwhile) is reported on the error
/// stream, since the start returns the refusal that caused it.
fn roll_back(workers: Vec<Worker>, refusal: &RtError) {
    if let Err(error) = stop(workers)
        && error != *refusal
    {
        registry::note_unreported_failure();
    }
}

/// Waits for every worker to acknowledge its build: the first refusal, or `WorkerFailed` for a worker that
/// ended without acknowledging (its sender dropped with it), so no failed shard is ever advertised.
fn await_readiness(
    acknowledged: &std::sync::mpsc::Receiver<(u16, Result<(), RtError>)>,
    workers: &[Worker],
) -> Result<(), RtError> {
    for _ in workers {
        match acknowledged.recv() {
            Ok((_, Ok(()))) => {}
            Ok((_, Err(error))) => return Err(error),
            Err(_) => {
                let silent = workers
                    .iter()
                    .find(|worker| worker.thread.is_finished())
                    .map_or(u16::MAX, |worker| worker.id);
                return Err(RtError::WorkerFailed { shard: silent });
            }
        }
    }
    Ok(())
}

/// One shard on the calling thread with the OS driver (tests, benchmarks, a command's own work).
pub struct LocalRuntime {
    /// Declared before `slot`, so it drops first: its futures are dropped while the registry slot (and the
    /// kick it owns) still exists.
    shard: Shard,
    notes: Vec<String>,
    slot: SlotGuard,
}

/// Gives a registry slot back when dropped: after the shard that held it, by field order.
#[derive(Debug)]
pub(crate) struct SlotGuard(registry::SlotHolder);

impl SlotGuard {
    /// Guards the slot `holder` holds.
    pub(crate) fn new(holder: registry::SlotHolder) -> Self {
        Self(holder)
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        registry::unregister(self.0);
        registry::note_reclaimed();
    }
}

impl std::fmt::Debug for LocalRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalRuntime")
            .field("shard", &self.shard.id())
            .finish()
    }
}

impl LocalRuntime {
    /// Builds the shard on the OS driver.
    pub fn new(config: &RuntimeConfig) -> Result<LocalRuntime, RtError> {
        config.validate_shape()?;
        let entries = u32::try_from(config.ring_entries).map_err(|_| RtError::BadConfig {
            what: "driver ring capacity exceeds its u32 entry field",
        })?;
        let prepared = os_driver(entries)?;
        let seed = ShardSeed::register(config, prepared.seed, register_kick(prepared.kick_fd))?;
        let holder = seed.holder;
        let shard = Shard::build(seed).inspect_err(|_| registry::unregister(holder))?;
        Ok(LocalRuntime {
            shard,
            notes: prepared.notes,
            slot: SlotGuard(holder),
        })
    }

    /// Builds the shard over a given driver (a test double).
    pub fn with_driver(
        config: &RuntimeConfig,
        driver: DriverSeed,
        kick: Kick,
    ) -> Result<LocalRuntime, RtError> {
        config.validate_shape()?;
        let seed = ShardSeed::register(config, driver, registry::RegisterKick::Kick(kick))?;
        let holder = seed.holder;
        let shard = Shard::build(seed).inspect_err(|_| registry::unregister(holder))?;
        Ok(LocalRuntime {
            shard,
            notes: Vec::new(),
            slot: SlotGuard(holder),
        })
    }

    /// The shard.
    pub fn shard_id(&self) -> ShardId {
        ShardId(self.slot.0.shard())
    }

    /// The driver notes.
    pub fn notes(&self) -> &[String] {
        &self.notes
    }

    /// Spawns a joinable task.
    pub fn spawn<F: Future<Output = ()> + 'static>(
        &mut self,
        future: F,
    ) -> Result<TaskId, RtError> {
        self.shard.spawn_local(crate::shard::boxed(future))
    }

    /// Keeps `value` for the shard's life and hands back its handle.
    pub fn keep<T: 'static>(&mut self, value: T) -> Result<Kept<T>, RtError> {
        self.shard.keep(value)
    }

    /// Runs available work and timers until idle. Pending external waits remain live for a later call.
    pub fn run_until_idle(&mut self) {
        self.shard.run_until_idle();
    }

    /// Runs `future` to completion on this thread as the shard's root task and returns its output. Whatever
    /// the root spawned and left running is cancelled and joined before this returns: nothing outlives the
    /// call. Refused when the root cannot be admitted, and `ShardGone` when the shard's driver is lost
    /// before the root finishes.
    pub fn block_on<T: 'static>(
        &mut self,
        future: impl Future<Output = T> + 'static,
    ) -> Result<T, RtError> {
        let root = self.shard.spawn_local(crate::shard::boxed(async move {
            let output = future.await;
            let _ = registry::with_current(|desk| desk.put_root_output(Box::new(output)));
        }))?;
        self.shard.context().detach(root)?;
        loop {
            if let Some(output) = self.shard.context().take_root_output() {
                self.shard.cancel_everything();
                return output.downcast::<T>().map(|output| *output).map_err(|_| {
                    RtError::BadConfig {
                        what: "a root output of another type",
                    }
                });
            }
            let outcome = self.shard.step();
            if outcome.exit {
                self.shard.discard_root_output();
                return Err(RtError::ShardGone {
                    shard: self.slot.0.shard(),
                });
            }
            if !outcome.did_work {
                self.shard.park(outcome.next_deadline_ns);
            }
        }
    }

    /// One loop iteration without blocking.
    pub fn step(&mut self) -> StepOutcome {
        self.shard.step()
    }

    /// Parks in the driver until a kick, a completion or `deadline_ns`.
    pub fn park(&mut self, deadline_ns: Option<u64>) {
        self.shard.park(deadline_ns);
    }

    /// The shard's counters.
    pub fn counters(&self) -> Counters {
        self.shard.counters()
    }

    /// Live tasks.
    pub fn live_tasks(&self) -> usize {
        self.shard.live_tasks()
    }

    /// Whether the shard left its loop.
    pub fn exited(&self) -> bool {
        self.shard.exited()
    }

    /// The shard's desk, for reads in tests and between runs; lent for this runtime's borrow:
    ///
    /// ```compile_fail,E0515
    /// use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
    /// use hyper_rt::shard::ShardContext;
    /// fn escape(config: &RuntimeConfig) -> Option<&'static ShardContext> {
    ///   let runtime = LocalRuntime::new(config).ok()?;
    ///   Some(runtime.context())
    /// }
    /// ```
    pub fn context(&self) -> &ShardContext {
        self.shard.context()
    }
}
