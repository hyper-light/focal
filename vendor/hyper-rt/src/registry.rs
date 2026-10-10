//! The process-wide shard registry: how a wake finds its target (slates §4.3; docs/runtime.md §3.2).
//!
//! Each shard registers into one of [`MAX_SHARDS`] **slots** and gives it back when its runtime shuts
//! down: a slot holds the shard's wake bitmap (for wakes from other threads), the sending end of its
//! control channel, its kick, its parking word and its pulse, plus a `generation` word that is **even
//! while live and odd while free**. The slot's memory is process-static and never freed, so a waker that
//! outlives its shard reads a live-or-free word, never freed memory: a wake to a free slot is dropped and
//! counted, and one to a slot a later shard reused wakes a task of that shard spuriously at most, which
//! every future tolerates. The kick descriptor is owned by the slot and closed at unregistration.
//!
//! Registration takes the lowest free slot under one atomic exchange on the slot's generation
//! (free → claimed), so concurrent runtimes never share a slot; lookup is one `Acquire` load of the
//! generation and a parity check.
//!
//! Routing: on the owning shard's thread, inside a step, a wake goes straight to the desk's run queue;
//! from anywhere else it sets the task's bit in the target's wake bitmap and kicks the target only if it
//! is parked. No wake ever waits: the bitmap holds every slot (slates' rings made a sender spin while a
//! ring was full).
#![allow(unsafe_code)]

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};

use crate::mem::Encoded;

use crate::control::Control;
use crate::driver::Kick;
#[cfg(unix)]
use crate::driver::KickFd;
use crate::error::RtError;
use crate::parking::Parking;
use crate::retire::Pinned;
use crate::shard::ShardContext;
use crate::wakes::WakeBitmap;

/// Shape: the bound on shard ids per process: more than any host's core count, few enough that
/// the registry is a small static table and a packed word's top bits stay free.
#[cfg(target_pointer_width = "64")]
pub const MAX_SHARDS: usize = 1024;
/// Derived: on a 32-bit target, the shards its waker's data pointer can name beside a slot
/// ([`crate::waker::MAX_SHARDS_32`]); a further shard is refused `TooManyShards` at registration.
#[cfg(not(target_pointer_width = "64"))]
pub const MAX_SHARDS: usize = crate::waker::MAX_SHARDS_32;

/// A registered shard: its foreign wake ring, its control channel and its kick.
#[derive(Debug)]
pub struct Entry {
    /// The registration that owns this entry; checked under the reader pin.
    holder: SlotHolder,
    /// The wakes other threads send this shard (docs/runtime.md §3.2).
    pub wakes: WakeBitmap,
    /// The control channel's sending end.
    pub control: SyncSender<Control>,
    /// The task-arena generation the shard that holds this slot starts its handles from: one past the
    /// highest generation the previous holder issued, so a wake word minted for that shard can never
    /// match a task of this one (the slot-reuse safety, see the module doc). Zero for a first use.
    pub generation_base: u32,
    /// Set by a sender, cleared by the shard once the channel is drained: the shard polls the
    /// channel only when this says something was sent, one atomic load per step otherwise.
    pub control_pending: crate::parking::ControlFlag,
    /// Set by the runtime's stop ([`request_stop`]), read by the shard's next control drain: a shutdown
    /// that takes no slot of the bounded control channel, so stopping never waits for the channel to have
    /// room (docs/runtime.md §15 item 9, closed 2026-10-06; it used to retry a full channel with
    /// `yield_now` and no bound).
    pub stop: AtomicBool,
    /// Admission is closed during owned cancellation and after shutdown begins.
    pub(crate) accepting: AtomicBool,
    /// The generational kick that wakes this registration's driver.
    pub kick: Kick,
    /// The kick descriptor, closed after the owning contexts and foreign borrows end (Unix).
    #[cfg(unix)]
    pub(crate) kick_fd: Option<std::os::fd::OwnedFd>,
    /// The completion port, closed after the owning contexts and foreign borrows end (Windows).
    #[cfg(windows)]
    pub(crate) kick_port: Option<crate::iocp::Port>,
    /// A simulated shard's flags, owned until its contexts and foreign kick borrows end.
    /// A copied kick carries the registration, never a reference to these flags.
    pub sim_shared: Option<Box<crate::sim::SimShared>>,
    /// The shard thread's CPU-clock handle, recorded as it starts (zero until then, and where the platform has none):
    /// what an observer reads to count its budget in the shard's own time ([`shard_cpu`]).
    pub cpu_clock: AtomicU64,
    /// The shard's parking announcement and the kicks it saved: a sender kicks only a parked
    /// shard, so a message to a spinning shard costs no syscall (§4.7 "Wake strategy"; the
    /// protocol and its loom model live in [`crate::parking`]).
    pub parking: Parking,
    /// The shard's forward-progress pulse, for an observer on any thread (see [`Pulse`]).
    pub pulse: Pulse,
    /// Set by the shard at its loop's exit ([`note_exited`]): its rings will never be drained again, so
    /// a sender that finds one full stops spinning and counts the wake stale instead of waiting for a
    /// consumer that is gone (the livelock the registry stress test found on 2026-09-14).
    pub exited: AtomicBool,
    /// Readiness waits of this shard dropped on another shard or off any: per wait slot, the dropped wait's
    /// generation plus one (zero: none), swept by the shard's next control drain (mantle's final review,
    /// third pass, A). A slot's generation is abandoned at most once, so a mark never overwrites one still
    /// owed, and the marks need no room the control channel could lack: a dropped wait is never refused.
    pub(crate) abandoned: Box<[AtomicU64]>,
    /// One bit per wait slot, set with its mark: the sweep visits only the marked slots, O(slots / 64 +
    /// marked), not every mark (mantle's final review, fourth pass, E: tens of thousands of slots a shard at
    /// agentic scale made one foreign drop a pass over as many cache lines on the owner's loop).
    pub(crate) abandoned_summary: Box<[AtomicU64]>,
    /// Set with a mark; the shard sweeps the marks when it takes this.
    pub(crate) abandons_marked: AtomicBool,
}

impl Entry {
    /// Marks the wait in `slot` at `generation` abandoned and asks the shard to sweep, as a control message
    /// would: the mark, then the flag, then the control mark and a kick. False only for a slot past the
    /// marks, which no ticket of this shard names.
    pub(crate) fn mark_abandoned(&self, slot: u32, generation: u32) -> bool {
        let Some(mark) = usize::try_from(slot)
            .ok()
            .and_then(|slot| self.abandoned.get(slot))
        else {
            return false;
        };
        mark.store(u64::from(generation).saturating_add(1), Ordering::Release);
        // The slot's summary bit after its mark: a sweep that takes the bit then finds the mark.
        let (word, bit) = (slot / u64::BITS, slot % u64::BITS);
        let Some(summary) = usize::try_from(word)
            .ok()
            .and_then(|word| self.abandoned_summary.get(word))
        else {
            return false;
        };
        summary.fetch_or(1u64 << bit, Ordering::AcqRel);
        self.abandons_marked.store(true, Ordering::Release);
        self.control_pending.publish();
        self.parking.kick_if_parked(|| self.kick.kick());
        true
    }
}

/// A shard's forward-progress pulse, readable from any thread with no shard round-trip (§4.14; the same
/// discipline as the fleet coordinator's period count in `slates-server`): the loop's step count, its
/// driver-wait count, the tasks it has admitted and completed, the admissions it has **refused** because
/// its arena was full, and its longest single poll — stored by the owning shard from its own `Counters`
/// (which live behind the shard's single-threaded borrow) once per step. An observer reads them to tell a
/// shard that is stepping — alive, however slowly under CPU load — from one that has stopped: parked with
/// no kick (a wedge), or held inside one long poll (`longest_step_ns` climbs); and to tell a shard whose
/// task arena is saturating (`admission_refused` climbs, so a new operation's task cannot be spawned) from
/// one merely slow. It is the instrument a stall diagnosis needs precisely when the shard would not answer
/// a query. The only writer is the shard; `Relaxed` on every side, statistics (R2).
///
/// Shape: on its own cache line (the largest line we target, Apple silicon's 128 bytes) — the owning shard
/// stores every step, so the line must be shared with no word another thread writes (the control flag,
/// the ring's tail) or the shard would pay a transfer per step; a foreign read moves the line once.
#[repr(align(128))]
#[derive(Debug, Default)]
pub struct Pulse {
    steps: AtomicU64,
    waits: AtomicU64,
    spawns: AtomicU64,
    completed: AtomicU64,
    admission_refused: AtomicU64,
    longest_step_ns: AtomicU64,
    /// An application loop's own forward-progress count on this shard (the fleet coordinator's period
    /// count), bumped by the shard and read by an observer on any thread — kept on the entry so it
    /// needs no allocation of its own and outlives the shard as the entry does.
    progress: AtomicU64,
    /// The shard's measured scheduler overrun (`ShardContext::scheduler_overrun_ns`), mirrored here at
    /// each wait it folds in, so a stall diagnosis on another thread can tell a shard the operating
    /// system is not scheduling (this climbs) from one held inside its own work (it does not).
    scheduler_overrun_ns: AtomicU64,
    /// The shard's online wake estimate (`ShardContext::wake_cost_ns`), mirrored at each wake it folds
    /// in, for a reader on another thread (the daemon's status, a stall diagnosis).
    wake_cost_ns: AtomicU64,
    /// Whether the shard is in its idle spin now, mirrored as it enters and leaves one: an observer tells
    /// a spinning shard from a running or parked one (a stall diagnosis; a test that must ring during the
    /// spin waits on this, not on a guessed settle time — mantle's review, finding 10c).
    spinning: AtomicBool,
}

impl Pulse {
    /// The owning shard marks entering (`true`) and leaving (`false`) its idle spin. Release: an observer
    /// that acquires `true` after a fact the shard published before the spin sees this spin, not an
    /// earlier one.
    pub fn record_spinning(&self, spinning: bool) {
        self.spinning.store(spinning, Ordering::Release);
    }

    /// Whether the shard is in its idle spin now (see [`Pulse::record_spinning`]).
    pub fn spinning(&self) -> bool {
        self.spinning.load(Ordering::Acquire)
    }

    /// The owning shard mirrors its measured scheduler overrun after folding a wait into it.
    pub fn record_scheduler_overrun(&self, overrun_ns: u64) {
        self.scheduler_overrun_ns
            .store(overrun_ns, Ordering::Relaxed);
    }

    /// The shard's measured scheduler overrun, nanoseconds (see [`Pulse::record_scheduler_overrun`]).
    pub fn scheduler_overrun_ns(&self) -> u64 {
        self.scheduler_overrun_ns.load(Ordering::Relaxed)
    }

    /// The owning shard mirrors its online wake estimate after folding a wake into it.
    pub fn record_wake_cost(&self, wake_cost_ns: u64) {
        self.wake_cost_ns.store(wake_cost_ns, Ordering::Relaxed);
    }

    /// The shard's online wake estimate, nanoseconds; 0 until its first measured wake (see
    /// [`Pulse::record_wake_cost`]).
    pub fn wake_cost_ns(&self) -> u64 {
        self.wake_cost_ns.load(Ordering::Relaxed)
    }

    /// The owning shard's application loop marks one period of forward progress.
    pub fn beat(&self) {
        self.progress.fetch_add(1, Ordering::Relaxed);
    }

    /// Periods the shard's application loop has completed (see [`Pulse::beat`]).
    pub fn progress(&self) -> u64 {
        self.progress.load(Ordering::Relaxed)
    }

    /// The owning shard records its step and task counts after a step (one plain store each, a line it owns).
    pub fn record(
        &self,
        steps: u64,
        spawns: u64,
        completed: u64,
        admission_refused: u64,
        longest_step_ns: u64,
    ) {
        self.steps.store(steps, Ordering::Relaxed);
        self.spawns.store(spawns, Ordering::Relaxed);
        self.completed.store(completed, Ordering::Relaxed);
        self.admission_refused
            .store(admission_refused, Ordering::Relaxed);
        self.longest_step_ns
            .store(longest_step_ns, Ordering::Relaxed);
    }

    /// The owning shard records its driver-wait count as it enters a wait.
    pub fn record_waits(&self, waits: u64) {
        self.waits.store(waits, Ordering::Relaxed);
    }

    /// Loop iterations the shard has run.
    pub fn steps(&self) -> u64 {
        self.steps.load(Ordering::Relaxed)
    }

    /// Driver waits the shard has entered.
    pub fn waits(&self) -> u64 {
        self.waits.load(Ordering::Relaxed)
    }

    /// Tasks the shard has admitted to its arena.
    pub fn spawns(&self) -> u64 {
        self.spawns.load(Ordering::Relaxed)
    }

    /// Tasks whose future returned on the shard.
    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    /// Admissions the shard refused because its task arena was full (the operation's task could not spawn).
    pub fn admission_refused(&self) -> u64 {
        self.admission_refused.load(Ordering::Relaxed)
    }

    /// The shard's longest single poll, nanoseconds (a step longer than a peer's wake starves the shard).
    pub fn longest_step_ns(&self) -> u64 {
        self.longest_step_ns.load(Ordering::Relaxed)
    }
}

/// One registry slot: its generation word and the entry it currently holds. The generation is odd
/// while the slot is free (initially 1) and even while a shard holds it; claiming a slot is one
/// `compare_exchange` from its free value to that value plus one, so two registrations never take
/// one slot. Claiming precedes publication; the entry pointer stays null until initialization
/// finishes, then is published with Release and read under a counted borrow.
/// Shape: one slot per cache line (the largest line we target, Apple silicon's 128 bytes): the
/// generation and the reader count are written by every foreign waker of that shard, so two shards'
/// slots on one line would bounce it between their wakers (vorpal measured four global atomics doubling
/// kernel-scale CPU on ping-pong). 1,024 slots make a 128 KiB static.
#[repr(align(128))]
struct Slot {
    generation: AtomicU32,
    /// The entry, read by foreign readers inside [`with_entry`] under counted pins and freed by the last
    /// of them after its retirement, or by the retirement itself when none reads (`crate::retire`).
    entry: Pinned<Entry>,
    /// The highest task-arena generation a holder of this slot has issued, carried to the next
    /// holder as its base (see [`Entry::generation_base`]).
    arena_generation: AtomicU32,
    /// Wakes that found the slot free (a waker outliving its shard), for a tripwire.
    stale_wakes: AtomicU64,
}

impl Slot {
    #[cfg(not(loom))]
    const fn new() -> Slot {
        Slot {
            generation: AtomicU32::new(1),
            entry: Pinned::new(),
            arena_generation: AtomicU32::new(0),
            stale_wakes: AtomicU64::new(0),
        }
    }
}

#[cfg(not(loom))]
static SLOTS: [Slot; MAX_SHARDS] = [const { Slot::new() }; MAX_SHARDS];

/// Under loom the entry's atomics are loom's, which are not `const`: the table is built on first use.
/// loom's models drive `crate::retire` itself and never touch the table; this only keeps the crate
/// building under `--cfg loom`.
#[cfg(loom)]
static SLOTS: std::sync::LazyLock<[Slot; MAX_SHARDS]> =
    std::sync::LazyLock::new(|| std::array::from_fn(|_| Slot::new()));

impl Slot {
    #[cfg(loom)]
    fn new() -> Slot {
        Slot {
            generation: AtomicU32::new(1),
            entry: Pinned::new(),
            arena_generation: AtomicU32::new(0),
            stale_wakes: AtomicU64::new(0),
        }
    }

    /// Frees a retired entry and publishes the slot free: the retirement's finish, run once, by the
    /// retirement or by the entry's last reader. The generation is still the holder's (even) value —
    /// nothing claims a slot before this store.
    fn free(&self, entry: Box<Entry>) {
        let live = self.generation.load(Ordering::Acquire);
        drop(entry);
        self.generation
            .store(live.wrapping_add(1), Ordering::Release);
    }
}

thread_local! {
  /// The context of the shard running on this thread, or null: set only for the span of an [`Entered`]
  /// guard, which borrows that context for its life (AUD-29-08).
  static CURRENT: Cell<*const ShardContext> = const { Cell::new(std::ptr::null()) };
}

/// A registration held by code outside the runtime (a registry test racing claims and retirements): the
/// proof that its holder owns the slot and built no context over it, since a context is built only from a
/// runtime's own seed. Dropping it retires the slot ([`unregister`]). Until 2026-09-30 `unregister` was a
/// public safe function taking any shard id, so safe code could retire a running shard's slot and free the
/// entry its context holds (AUD-29-08).
#[derive(Debug)]
pub struct Registration {
    holder: SlotHolder,
}

impl Registration {
    /// The registered shard id.
    pub fn shard(&self) -> u16 {
        self.holder.shard
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        unregister(self.holder);
    }
}

/// Claims a slot as [`register_slot`] does, for a holder outside the runtime: the slot is retired when the
/// returned [`Registration`] drops. Retiring a slot — a running shard's included — is not reachable from
/// outside the runtime:
///
/// ```compile_fail,E0603
/// hyper_rt::registry::unregister(hyper_rt::registry::holder_of(0).unwrap());
/// ```
pub fn register(
    wake_slots: usize,
    control_bound: usize,
    kick: RegisterKick,
) -> Result<(Registration, Receiver<Control>), RtError> {
    // A holder outside the runtime runs no desk, so no readiness waits.
    let (holder, control) = register_slot(wake_slots, control_bound, 0, kick)?;
    Ok((Registration { holder }, control))
}

/// Registers a new shard with a wake bitmap for `wake_slots` task slots, a control channel bounded at
/// `control_bound`, and its kick; returns the id and the control channel's receiving end. Takes the
/// lowest free slot; refuses `TooManyShards` when every slot holds a live shard.
pub(crate) fn register_slot(
    wake_slots: usize,
    control_bound: usize,
    waits: usize,
    kick: RegisterKick,
) -> Result<(SlotHolder, Receiver<Control>), RtError> {
    let max = u16::try_from(MAX_SHARDS).unwrap_or(u16::MAX);
    for (index, slot) in SLOTS.iter().enumerate() {
        let free = slot.generation.load(Ordering::Acquire);
        if free & 1 == 0 {
            continue;
        }
        // A slot whose identities are spent is never claimed again (AUD-29-11): its own word would wrap and
        // a stale holder would name the next shard, or its task arena has issued every generation a wake word
        // carries and a successor would have none to issue. It is retired, like a slab slot at its limit.
        if slot.arena_generation.load(Ordering::Acquire) > Encoded::TASK_GENERATION_LIMIT {
            continue;
        }
        // Claim: free (odd) → claimed (the next even). A loser sees the even value and moves on.
        let Some(live) = free.checked_add(1) else {
            continue;
        };
        if slot
            .generation
            .compare_exchange(free, live, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            continue;
        }
        let id = u16::try_from(index).unwrap_or(u16::MAX);
        let (control, receiver) = sync_channel(control_bound.max(1));
        let wakes = WakeBitmap::new(wake_slots);
        let holder = SlotHolder {
            shard: id,
            generation: live,
        };
        let mut entry = Box::new(Entry {
            holder,
            wakes,
            control,
            generation_base: slot.arena_generation.load(Ordering::Acquire),
            control_pending: crate::parking::ControlFlag::new(),
            stop: AtomicBool::new(false),
            accepting: AtomicBool::new(true),
            kick: Kick::None,
            #[cfg(unix)]
            kick_fd: None,
            #[cfg(windows)]
            kick_port: None,
            sim_shared: None,
            cpu_clock: AtomicU64::new(0),
            parking: Parking::new(),
            pulse: Pulse::default(),
            exited: AtomicBool::new(false),
            abandoned: (0..waits).map(|_| AtomicU64::new(0)).collect(),
            abandoned_summary: (0..waits.div_ceil(64)).map(|_| AtomicU64::new(0)).collect(),
            abandons_marked: AtomicBool::new(false),
        });
        entry.kick = match kick {
            RegisterKick::Kick(kick) => kick,
            #[cfg(unix)]
            RegisterKick::Descriptor(fd, form) => {
                entry.kick_fd = Some(fd);
                form(KickFd::new(holder))
            }
            #[cfg(windows)]
            RegisterKick::Port(port) => {
                entry.kick_port = Some(port);
                Kick::Iocp(crate::driver::KickPort::new(holder))
            }
            RegisterKick::Sim(shared) => {
                entry.sim_shared = Some(shared);
                Kick::Sim(holder)
            }
        };
        // Retirement takes the pointer and frees it through its last reader before publishing a free slot.
        slot.entry.publish(entry);
        return Ok((holder, receiver));
    }
    Err(RtError::TooManyShards { max })
}

/// What a registration hands the slot for its kick: a ready [`Kick`] (none), the simulation's shared
/// flags, or an OS object the slot takes ownership of and mints the kick over (Unix: an eventfd or a
/// kqueue descriptor with its kick form; Windows: the completion port).
pub enum RegisterKick {
    /// A kick that owns nothing the slot must close.
    Kick(Kick),
    /// A descriptor the slot owns; the function mints the kick over the slot's owned form of it.
    #[cfg(unix)]
    Descriptor(std::os::fd::OwnedFd, fn(KickFd) -> Kick),
    /// A completion port the slot owns (Windows); the kick is minted over the slot's owned port.
    #[cfg(windows)]
    Port(crate::iocp::Port),
    /// A simulated shard's driver flags, owned by the slot; the kick is minted over them.
    Sim(Box<crate::sim::SimShared>),
}

/// Records the highest task-arena generation a shard issued (the shard's own thread, at its loop's
/// exit), so the slot's next holder starts past it.
pub fn note_arena_generation(shard: u16, high: u32) {
    if let Some(slot) = SLOTS.get(usize::from(shard)) {
        slot.arena_generation.fetch_max(high, Ordering::AcqRel);
    }
}

/// Shards dropped by their owners since the process started (a shut-down runtime's per-shard state given
/// back): a test's non-vacuity counter.
static CONTEXTS_RECLAIMED: AtomicU64 = AtomicU64::new(0);

/// Worker failures found where nothing could return them: a runtime dropped without `shutdown`, or a
/// second failure found while rolling back a refused start (the start returns the first).
static UNREPORTED_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Counts a worker failure nothing could return.
pub(crate) fn note_unreported_failure() {
    UNREPORTED_FAILURES.fetch_add(1, Ordering::Relaxed);
}

/// Worker failures found where nothing could return them, since the process started: what a consumer
/// that drops runtimes without `shutdown` reads to learn of them.
pub fn unreported_failures() -> u64 {
    UNREPORTED_FAILURES.load(Ordering::Relaxed)
}

/// Counts one shard dropped by its owner.
pub(crate) fn note_reclaimed() {
    CONTEXTS_RECLAIMED.fetch_add(1, Ordering::Relaxed);
}

/// Shards dropped by their owners since the process started: a test's non-vacuity counter.
pub fn contexts_reclaimed() -> u64 {
    CONTEXTS_RECLAIMED.load(Ordering::Relaxed)
}

/// Retires the registration `holder` names after all of its runtime's contexts have ended: removes the
/// entry from lookup, and its resources are dropped and the free generation published by whichever ends
/// last — this call, when no foreign reader holds the entry, or the last reader's unpin (`crate::retire`).
/// It never waits: a descheduled reader delays only the free, and a retirement from inside a reader's own
/// call ends (mantle's review, finding 10b). A new registration cannot claim the slot before the free
/// (§4.3). A holder whose slot has moved on — retired already, or held by a later registration — retires
/// nothing (mantle's final review: by shard number alone, a second call after the slot's reuse would have
/// retired its new holder).
pub(crate) fn unregister(holder: SlotHolder) {
    let Some(slot) = SLOTS.get(usize::from(holder.shard)) else {
        return;
    };
    if slot.generation.load(Ordering::Acquire) != holder.generation {
        return;
    }
    note_exited(holder.shard);
    slot.entry.retire(|entry| slot.free(entry));
}

/// Runs `f` on the live entry of `shard` — the form every **foreign** reader uses (a wake or a control
/// message from another thread, an observer reading the pulse or copying the kick): the slot pins the
/// entry for the call's span. A retired entry is freed only once its pointer was removed and no pin is
/// counted — by this reader, when it is the last — so `f` never sees freed memory however long its
/// thread is descheduled. `None` for a free slot (a wake to it is stale). The shard's own context holds an
/// unguarded reference to its entry instead ([`entry`]): its slot cannot be re-registered while it
/// lives, since unregistration follows its thread's join.
pub fn with_entry<R>(shard: u16, f: impl FnOnce(&Entry) -> R) -> Option<R> {
    let slot = SLOTS.get(usize::from(shard))?;
    if slot.generation.load(Ordering::Acquire) & 1 == 1 {
        return None;
    }
    slot.entry.read(f, |entry| slot.free(entry))
}

/// Records the calling thread's CPU clock on shard `shard`'s entry (the shard's own thread, as it starts).
pub fn record_cpu_clock(shard: u16) {
    if let Some(handle) = crate::thread_clock::current() {
        let _ = with_entry(shard, |entry| {
            entry.cpu_clock.store(handle, Ordering::Release)
        });
    }
}

/// The CPU clock of the shard `holder` names: `None` when that registration no longer holds its slot, its thread
/// recorded no clock, or the platform has none. An observer reads it to tell a starved shard, still consuming CPU, from
/// a wedged one (§4.14).
pub fn shard_cpu(holder: SlotHolder) -> Option<crate::thread_clock::CpuReading> {
    with_entry(holder.shard, |entry| {
        (entry.holder() == holder).then(|| entry.cpu_clock.load(Ordering::Acquire))
    })
    .flatten()
    .filter(|handle| *handle != 0)
    .and_then(crate::thread_clock::read)
}

/// A pinned read of one particular registration, never a replacement in the same slot.
pub(crate) fn with_holder<R>(holder: SlotHolder, f: impl FnOnce(&Entry) -> R) -> Option<R> {
    with_entry(holder.shard, |entry| {
        (entry.holder == holder).then(|| f(entry))
    })
    .flatten()
}

/// Marks `shard`'s entry exited (the shard's own thread, at its loop's exit): a sender that finds its
/// ring full stops spinning, since nothing will drain it again.
pub fn note_exited(shard: u16) {
    let _ = with_entry(shard, |entry| entry.exited.store(true, Ordering::Release));
}

/// The entry of a live shard, unguarded: for the shard's **own** thread (its context keeps the
/// reference for its life; its slot cannot be re-registered before its thread has ended and joined)
/// and for a runtime building its shards before any of them runs. A foreign reader uses
/// [`with_entry`]. `None` for a free slot.
pub(crate) fn entry(shard: u16) -> Option<&'static Entry> {
    let slot = SLOTS.get(usize::from(shard))?;
    if slot.generation.load(Ordering::Acquire) & 1 == 1 {
        return None;
    }
    let entry = slot.entry.unguarded();
    if entry.is_null() {
        return None;
    }
    // SAFETY: the caller is the owning shard or its bootstrap thread (the contract above).
    // Unregistration cannot run until all owning contexts ended, so this entry outlives its
    // own shard's reference. Foreign threads must use with_entry, which pins reclamation.
    Some(unsafe { &*entry })
}

/// The wakes that found their target's slot free (a waker outlived its shard); a tripwire, never a
/// fault: the word it carried had no task to reach.
pub fn stale_wakes(shard: u16) -> u64 {
    SLOTS
        .get(usize::from(shard))
        .map_or(0, |slot| slot.stale_wakes.load(Ordering::Relaxed))
}

/// The span a shard runs on this thread: from [`enter`] to the guard's drop, [`with_current`] lends its
/// context, and the drop restores whichever context an enclosing span had entered (a step of one runtime
/// inside a task of another). The guard borrows the context, so the context outlives every lend.
pub(crate) struct Entered<'context> {
    /// The shard an enclosing span had entered, or null: restored at the drop.
    previous: *const ShardContext,
    context: std::marker::PhantomData<&'context ShardContext>,
}

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        let _ = CURRENT.try_with(|current| current.set(self.previous));
    }
}

/// Makes `context` this thread's running shard for the guard's span (the loop's `run`, `run_until_idle`,
/// `step` and `park`). Before 2026-09-30 a step published a `&'static` context and left it published after
/// it returned, so a lend could outlive the owner that freed it. The restore costs a bare simulated step 11
/// instructions (5,630 against 5,619, callgrind, 2026-09-30); skipping the writes when the shard is already
/// current cost bare steps 13 more (5,643) for a saving on the worker's `run` no row measures — rejected.
pub(crate) fn enter(context: &ShardContext) -> Entered<'_> {
    let previous = CURRENT
        .try_with(|current| current.replace(std::ptr::from_ref(context)))
        .unwrap_or(std::ptr::null());
    Entered {
        previous,
        context: std::marker::PhantomData,
    }
}

/// Runs `f` with this thread's running shard's context, or `None` outside a shard's span. The reference
/// cannot leave `f`:
///
/// ```compile_fail,E0521
/// fn escape() -> Option<&'static hyper_rt::shard::ShardContext> {
///   hyper_rt::registry::with_current(|context| context)
/// }
/// ```
pub fn with_current<R>(f: impl FnOnce(&ShardContext) -> R) -> Option<R> {
    let current = CURRENT.try_with(Cell::get).ok()?;
    // SAFETY: the cell is non-null only inside an `Entered` span, and holds the context that span's guard
    // borrows (or, after an inner span ended, the enclosing span's context, whose guard borrows it for
    // longer). A borrowed context cannot be freed: its owner (`LocalRuntime`, `SimRuntime`, a worker's own
    // frame) is borrowed for the guard's life, and `reclaim_context` runs only on an owner that is not.
    let context = unsafe { current.as_ref() }?;
    Some(f(context))
}

/// The current shard's id, if this thread runs one.
pub fn current_shard() -> Option<u16> {
    with_current(|ctx| ctx.id)
}

/// Wakes the task named by `word` from wherever the caller is: inside a step of its own shard, onto the
/// desk's run queue; from anywhere else, its bit in the shard's wake bitmap, and a kick only if the shard
/// is parked. Never waits. A wake to a shard that exited or a free slot is counted stale and dropped.
pub fn wake(word: Encoded) {
    let target = word.shard();
    let local = with_current(|ctx| {
        let own = ctx.id == target;
        if own {
            ctx.wake_local(word.slot());
        }
        own
    });
    if local == Some(true) {
        return;
    }
    let landed = with_entry(target, |entry| {
        if entry.exited.load(Ordering::Acquire) || !entry.wakes.set(word.slot()) {
            return false;
        }
        entry.parking.kick_if_parked(|| entry.kick.kick());
        true
    });
    if landed != Some(true) {
        count_stale(target);
    }
}

/// Counts a wake that found no live consumer (a stale waker, or a holder that exited): a tripwire,
/// never a fault.
pub(crate) fn count_stale(target: u16) {
    if let Some(slot) = SLOTS.get(usize::from(target)) {
        slot.stale_wakes.fetch_add(1, Ordering::Relaxed);
    }
}

/// Sends a control message to a shard from any thread and kicks it; refused when the shard's
/// control channel is full or the shard is gone.
pub fn send_control(target: u16, message: Control) -> Result<(), RtError> {
    with_entry(target, |entry| send_control_to(entry, target, message))
        .ok_or(RtError::ShardGone { shard: target })?
}

/// Asks shard `target` to shut down without a control-channel slot: sets its stop flag, then publishes
/// the control mark and kicks it as a message would, so a parked shard wakes and its next drain applies
/// the stop after the messages it drains in that batch. Never waits. `ShardGone` for a free slot.
pub(crate) fn request_stop(target: u16) -> Result<(), RtError> {
    with_entry(target, |entry| {
        // The flag before the mark: the drain that takes the mark (AcqRel) then sees the flag.
        entry.stop.store(true, Ordering::Release);
        entry.control_pending.publish();
        entry.parking.kick_if_parked(|| entry.kick.kick());
    })
    .ok_or(RtError::ShardGone { shard: target })
}

/// The send itself, on a counted entry (see [`send_control`]).
fn send_control_to(entry: &Entry, target: u16, message: Control) -> Result<(), RtError> {
    if !entry.accepting.load(Ordering::Acquire)
        || entry.stop.load(Ordering::Acquire)
        || entry.exited.load(Ordering::Acquire)
    {
        return Err(RtError::ShardGone { shard: target });
    }
    match entry.control.try_send(message) {
        Ok(()) => {
            entry.control_pending.publish();
            entry.parking.kick_if_parked(|| entry.kick.kick());
            Ok(())
        }
        Err(TrySendError::Full(_)) => Err(RtError::ControlFull { shard: target }),
        Err(TrySendError::Disconnected(_)) => Err(RtError::ShardGone { shard: target }),
    }
}

/// A live shard named together with the registration that holds its slot: what a foreign submitter
/// keeps when its work must reach *that* shard and never a later holder of the slot. A slot freed at
/// unregistration is claimed by the next shard to register (the lowest free slot), so a message
/// addressed by id alone after the shard exited would reach a stranger — an observation of one daemon
/// landing on the next daemon to start. [`send_control_to_holder`] refuses `ShardGone` instead once
/// the slot is free or held by a later registration. Read from a started runtime
/// ([`crate::Runtime::holder_of`]): a registration in progress has claimed its generation before it
/// published its entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SlotHolder {
    shard: u16,
    generation: u32,
}

#[cfg(test)]
impl SlotHolder {
    /// A holder for a table test that has no registration.
    pub(crate) fn for_test() -> SlotHolder {
        SlotHolder {
            shard: u16::MAX,
            generation: 0,
        }
    }
}

impl Entry {
    /// The registration that owns this entry.
    pub(crate) fn holder(&self) -> SlotHolder {
        self.holder
    }
}

impl SlotHolder {
    /// The shard id.
    pub fn shard(&self) -> u16 {
        self.shard
    }
}

/// The registration currently holding `shard`'s slot; `None` for a free slot.
pub fn holder_of(shard: u16) -> Option<SlotHolder> {
    with_entry(shard, |entry| entry.holder)
}

/// Sends a control message to the shard `holder` names, from any thread, and kicks it; refused
/// `ShardGone` when the slot is free or held by a later registration (see [`SlotHolder`]) and
/// `ControlFull` when the holder's control channel is full.
pub fn send_control_to_holder(holder: SlotHolder, message: Control) -> Result<(), RtError> {
    with_holder(holder, |entry| {
        send_control_to(entry, holder.shard, message)
    })
    .ok_or(RtError::ShardGone {
        shard: holder.shard,
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AC-0.7, finding 10b: pause a foreign descriptor borrow, then retire its shard. Expect: the
    /// retirement returns with the borrow still held (it never waits), the descriptor stays open
    /// for the borrow's span, the borrow's end frees it, and a copied kick refuses access after
    /// retirement and reuse.
    #[cfg(unix)]
    #[test]
    fn retirement_returns_and_a_borrowed_kick_stays_open_until_the_borrow_ends() {
        use std::sync::mpsc::sync_channel;

        let (descriptor, _write) = rustix::pipe::pipe().unwrap();
        #[cfg(target_os = "linux")]
        let form = Kick::Eventfd;
        #[cfg(any(target_os = "macos", target_os = "freebsd"))]
        let form = Kick::Kqueue;
        let (shard_holder, _control) =
            register_slot(2, 1, 0, RegisterKick::Descriptor(descriptor, form)).unwrap();
        let shard = shard_holder.shard();
        let kick = with_entry(shard, |entry| entry.kick).unwrap();
        let descriptor = match kick {
            #[cfg(target_os = "linux")]
            Kick::Eventfd(descriptor) => descriptor,
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            Kick::Kqueue(descriptor) => descriptor,
            _ => panic!("registered descriptor"),
        };
        let (entered, borrowing) = sync_channel(0);
        let (release, released) = sync_channel(0);
        std::thread::scope(|scope| {
            let borrower = scope.spawn(move || {
                descriptor.with(|fd| {
                    // The number, read while the borrow is fresh; checked below by number alone, so a
                    // free under the borrow shows as a closed descriptor, never as a read of freed memory.
                    let raw = std::os::fd::AsRawFd::as_raw_fd(fd);
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                    // SAFETY: F_GETFD reads the descriptor table only, no memory of this process.
                    unsafe { libc::fcntl(raw, libc::F_GETFD) != -1 }
                })
            });
            borrowing.recv().unwrap();
            // The borrow is held until `release`: a retirement that waited for it would never return.
            unregister(shard_holder);
            assert!(
                with_entry(shard, |_| ()).is_none(),
                "the retired entry is out of lookup"
            );
            release.send(()).unwrap();
            assert_eq!(
                borrower.join().unwrap(),
                Some(true),
                "the descriptor stayed open for its borrow"
            );
        });
        assert_eq!(descriptor.with(|_| ()), None);
        let (replacement_holder, _control) =
            register_slot(2, 1, 0, RegisterKick::Kick(Kick::None)).unwrap();
        assert_eq!(
            descriptor.with(|_| ()),
            None,
            "a stale kick cannot borrow a replacement"
        );
        unregister(replacement_holder);
    }

    /// Mantle's final review, finding 5. Do: retire a slot twice while a foreign reader holds its entry,
    /// then let the reader go. Expect: the slot stays claimed through both retirements, and the reader's end
    /// frees it once (one generation step, to the free value). The second retirement used to publish the slot
    /// free under the reader, and the reader's free then made it look live with no entry.
    #[test]
    fn a_second_retirement_under_a_reader_does_nothing() {
        use std::sync::mpsc::sync_channel;

        let (shard_holder, _control) =
            register_slot(2, 1, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let shard = shard_holder.shard();
        let slot = SLOTS.get(usize::from(shard)).unwrap();
        let live = slot.generation.load(Ordering::Acquire);
        let (entered, inside) = sync_channel(0);
        let (release, released) = sync_channel::<()>(0);
        std::thread::scope(|scope| {
            let reader = scope.spawn(move || {
                with_entry(shard, |_| {
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                })
            });
            inside.recv().unwrap();
            unregister(shard_holder);
            unregister(shard_holder);
            let under_the_reader = slot.generation.load(Ordering::Acquire);
            // The reader is let go before anything is judged, so a failure ends the test, not the scope.
            release.send(()).unwrap();
            assert_eq!(reader.join().unwrap(), Some(()));
            assert_eq!(
                under_the_reader, live,
                "the slot stays claimed while its reader holds the entry"
            );
        });
        // Freed by the reader. Another test running beside this one may claim the free slot at once, so the
        // state is judged, not the count: an odd generation is free; an even one is a registration with its
        // entry. The bug's state is neither: even, and no entry (a second free under the reader).
        let after = slot.generation.load(Ordering::Acquire);
        assert_ne!(after, live, "freed by the reader");
        assert!(
            after & 1 == 1 || !slot.entry.unguarded().is_null(),
            "a claimed slot holds its entry (generation {after})"
        );
    }

    /// Mantle's final review: a retirement names its registration, not only its slot. Do: retire a live
    /// slot with the holder of an earlier registration of it. Expect: nothing is retired; the live holder's
    /// entry stays, and its own retirement then frees the slot.
    #[test]
    fn a_stale_holder_retires_nothing() {
        let (live, _control) = register_slot(2, 1, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let stale = SlotHolder {
            shard: live.shard,
            generation: live.generation.wrapping_sub(2),
        };
        unregister(stale);
        assert!(
            with_holder(live, |_| ()).is_some(),
            "the live registration was not retired by a stale holder"
        );
        unregister(live);
        assert!(with_entry(live.shard, |_| ()).is_none());
    }

    /// Do: register a slot, mark its holder exited (what the shard does at its loop's exit) and wake a
    /// task of it from a foreign thread's path. Expect: the wake is counted stale at once, never delivered
    /// to a shard that will not drain it.
    #[test]
    fn a_wake_to_an_exited_holder_is_counted_stale() {
        let (id_holder, _receiver) =
            register_slot(4, 2, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let id = id_holder.shard();
        let stale_before = stale_wakes(id);
        note_exited(id);
        wake(Encoded::pack(id, 1, 0).unwrap());
        assert_eq!(stale_wakes(id) - stale_before, 1);
        assert!(!entry(id).unwrap().wakes.is_pending());
        unregister(id_holder);
    }

    // Every registration below is given back at the test's end, so no test leaves a live holder behind for
    // another to wake.

    #[test]
    fn registration_hands_out_distinct_ids_and_entries() {
        let (a_holder, _ra) = register_slot(8, 4, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let a = a_holder.shard();
        let (b_holder, _rb) = register_slot(8, 4, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let b = b_holder.shard();
        assert_ne!(a, b);
        assert!(entry(a).is_some());
        assert!(entry(b).is_some());
        unregister(a_holder);
        unregister(b_holder);
    }

    #[test]
    fn a_wake_from_a_foreign_thread_lands_in_the_target_bitmap() {
        let (id_holder, _receiver) =
            register_slot(8, 4, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let id = id_holder.shard();
        let word = Encoded::pack(id, 5, 1).unwrap();
        std::thread::scope(|scope| {
            scope.spawn(|| wake(word));
        });
        let mut woken = Vec::new();
        entry(id).unwrap().wakes.drain(|slot| woken.push(slot));
        assert_eq!(woken, vec![5]);
        assert_eq!(current_shard(), None);
        unregister(id_holder);
    }

    /// A wake past the target's slots (a stale word from a larger runtime that held the registry slot) is
    /// counted stale, not delivered.
    #[test]
    fn a_wake_past_the_bitmap_is_counted_stale() {
        let (id_holder, _receiver) =
            register_slot(4, 4, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let id = id_holder.shard();
        let before = stale_wakes(id);
        wake(Encoded::pack(id, 1_000, 1).unwrap());
        assert_eq!(stale_wakes(id) - before, 1);
        unregister(id_holder);
    }

    #[test]
    fn control_is_refused_when_the_channel_is_full_or_the_shard_is_gone() {
        let (id_holder, receiver) =
            register_slot(2, 1, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let id = id_holder.shard();
        send_control(id, Control::Shutdown).unwrap();
        assert!(matches!(
            send_control(id, Control::Shutdown),
            Err(RtError::ControlFull { .. })
        ));
        drop(receiver);
        assert!(matches!(
            send_control(id, Control::Shutdown),
            Err(RtError::ShardGone { .. })
        ));
        assert!(matches!(
            send_control(u16::MAX, Control::Shutdown),
            Err(RtError::ShardGone { .. })
        ));
        unregister(id_holder);
    }

    /// A submission pinned to a slot's registration is refused as gone once the slot is free, and once
    /// a later registration holds it — never delivered to the new holder.
    #[test]
    fn a_holder_pinned_send_is_refused_once_the_slot_changes_hands() {
        let (id_holder, _receiver) =
            register_slot(2, 2, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let id = id_holder.shard();
        let holder = holder_of(id).unwrap();
        assert_eq!(holder.shard(), id);
        send_control_to_holder(holder, Control::Shutdown).unwrap();
        unregister(id_holder);
        assert_ne!(holder_of(id), Some(holder), "the old registration ended");
        assert!(matches!(
            send_control_to_holder(holder, Control::Shutdown),
            Err(RtError::ShardGone { .. })
        ));
        let (again_holder, _receiver) =
            register_slot(2, 2, 0, RegisterKick::Kick(Kick::none())).unwrap();
        let again = again_holder.shard();
        if again == id {
            assert!(matches!(
                send_control_to_holder(holder, Control::Shutdown),
                Err(RtError::ShardGone { .. })
            ));
            assert_ne!(holder_of(again), Some(holder));
        }
        unregister(again_holder);
    }
}
