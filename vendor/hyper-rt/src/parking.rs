//! Parking a shard and kicking it awake: the protocol between a shard about to wait in its driver
//! and whoever sends it a message from another thread (§4.3, "wake from another shard enqueues
//! (slot, generation) on the target's ring and kicks the driver"; §4.7 "Wake strategy": a sender
//! kicks only a parked shard, so a message to a spinning shard costs no syscall, and the saving
//! is counted). The shard announces that it is about to park, then re-checks its inboxes; a
//! sender publishes its message, then checks the announcement. Each side writes and then reads,
//! the store-buffering shape, so the two reads must be ordered against the two writes by one
//! total order or both can miss: the shard sees no message and parks, the sender sees no
//! announcement and skips the kick, and the message waits for a wake that never comes.
//!
//! Each side therefore fences with `SeqCst` between its write and its read. That is the C++20
//! fence rule ([B: `[atomics.order]`, the fence–fence case]: with a `SeqCst` fence after the
//! write on one thread and a `SeqCst` fence before the read on the other, whichever fence comes
//! first in the total order, the later thread's read observes the earlier thread's write), and
//! it holds whatever ordering the message's own publication used — the multi-producer ring
//! publishes with `Release`, the control flag with `SeqCst`. A `SeqCst` store and load on the
//! announcement alone were not enough: the ring's `Release` publication takes no part in the
//! announcement's total order, so the abstract model lets both reads miss, and x86 realizes it
//! by reordering the plain store of the publication past the plain load of the announcement
//! through its store buffer (a `SeqCst` load is a plain `mov` there; only a `SeqCst` store gets
//! the locked instruction). loom found the lost wake in the first executions it explored
//! (`docs/bugs/2026-09-13-parked-shard-loses-a-foreign-wake.md`); arm64 orders a store-release
//! before a later load-acquire and was never exposed.
//!
//! Under `--cfg loom` the model in this module drives exactly this code: one sender publishing
//! into the shard's multi-producer ring against one shard parking, with loom's `Notify` standing
//! in for the driver's kick (sticky like an eventfd count or an `EVFILT_USER` trigger, spurious
//! like a real `kevent` return); a lost wake is a shard blocked with no runnable thread left,
//! which loom reports as a deadlock (AC-0.7).

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};

/// The shard's announcement that it is parked, the count of kicks the announcement saved, and the
/// stamp of the first kick sent to the current park (the shard's online wake estimate, §4.3).
#[derive(Debug)]
pub struct Parking {
    parked: AtomicBool,
    kicks_skipped: AtomicU64,
    /// When the first kick of the current park was sent (host monotonic nanoseconds), 0 when none was;
    /// the shard takes it after its wait. A measurement word, outside the protocol: its orders are
    /// `Relaxed`, and a stamp lost to a race costs one sample, never a wake.
    kicked_at: AtomicU64,
    /// Whether this shard times its wakes ([`Parking::time_wakes`]): only then does a kick stamp the host
    /// clock and a park read it. A simulated shard, or one with no estimate to feed, reads no clock on
    /// either side (D-20: the simulation makes no OS call; its instruction counts are gated on that).
    timed: AtomicBool,
}

/// A shard's control-pending flag: the sender's publication marker for its control channel, so the shard
/// learns a control message waits without polling the channel every step, and so a parking shard's re-check
/// sees one ([`Parking::park_unless_pending`]). A sender publishes after pushing its message; the shard takes
/// the mark before draining, and re-arms it when a batch-limited drain may have left messages behind.
#[derive(Debug, Default)]
pub struct ControlFlag {
    pending: AtomicBool,
}

impl ControlFlag {
    /// Nothing pending.
    pub fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
        }
    }

    /// The sender's half, after its message is pushed: marks the channel pending — a read-modify-write, not a
    /// store. Consecutive RMWs form one release sequence, so the shard's acquiring [`take`](Self::take) of the
    /// latest mark synchronizes with **every** sender whose mark it absorbs, and the drain that follows sees
    /// each of their messages. A plain store from a second sender would end the first sender's release
    /// sequence (C++20 `[intro.races]`: a release sequence continues only through RMWs), leaving the first
    /// sender's message unordered before the drain: loom's first interleaving drained neither message this
    /// way — the flag cleared, both kicks skipped, the shard parked for good.
    pub fn publish(&self) {
        let _ = self.pending.swap(true, Ordering::SeqCst);
    }

    /// The shard's half, before draining: whether a message may wait, clearing the mark — one atomic
    /// read-modify-write. The read and the clear must be one step: an RMW reads the latest mark in the flag's
    /// modification order, so either it reads a concurrent sender's publication (and acquires its message,
    /// which the drain that follows then finds) or that publication lands after it and stays set for the next
    /// step. Until 2026-09-28 this was a load and then a store of `false`: the load could read an earlier
    /// sender's mark, the store then overwrote a later sender's, and the drain could miss that sender's
    /// message — a `Shutdown` stranded in the channel while the shard parked for good, since the later
    /// sender's kick had coalesced into the wake the shard already took (loom: the first interleaving
    /// explored; `docs/bugs/2026-09-28-a-cleared-control-flag-stranded-a-shutdown.md`).
    pub fn take(&self) -> bool {
        self.pending.swap(false, Ordering::AcqRel)
    }

    /// The shard re-arms the mark after a drain that stopped at its batch bound (messages may remain).
    pub fn rearm(&self) {
        self.pending.store(true, Ordering::Release);
    }

    /// The shard's park re-check: whether a message may wait.
    pub fn is_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }
}

/// What a park did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parked {
    /// A message was already pending: the announcement was withdrawn without waiting.
    Pending,
    /// The shard waited; with what the park learned about its wake when the shard times its wakes.
    Waited(Option<Woken>),
}

/// What a park that waited learned about its wake: when it announced itself parked, when it entered its
/// wait, when the wait returned, and the stamp of the kick sent to it, if one was (§4.3, the shard's
/// online wake estimate).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Woken {
    /// When the shard announced it was parking (host monotonic nanoseconds).
    pub announced_ns: u64,
    /// When it entered the driver's blocking wait, after the re-check.
    pub waiting_ns: u64,
    /// When its wait returned — the moment it ran again.
    pub returned_ns: u64,
    /// The first kick's stamp during this park, if a kick came.
    pub kicked_ns: Option<u64>,
}

impl Woken {
    /// The kick-to-running latency this park measured: the kick's stamp falls between the wait's entry and
    /// its return, so the kick found the shard asleep (or about to be) — the event the boot probe times.
    /// `None` when no kick came (a timer or a completion woke it), when the stamp predates the announcement
    /// (a sender that saw an earlier park — [`Woken::stale`]), when it landed while the park was being set up
    /// (the wait then returned without sleeping — [`Woken::early`]; timing it would feed a wake that never
    /// happened into the estimate and bias it low), or when it came after the wait had already returned (a
    /// kick that did not wake this park).
    pub fn latency_ns(&self) -> Option<u64> {
        let kicked = self.kicked_ns?;
        (self.waiting_ns <= kicked && kicked <= self.returned_ns)
            .then(|| self.returned_ns.saturating_sub(kicked))
    }

    /// Whether the stamp found predates this park's announcement: a sender that read an earlier park's
    /// announcement and stamped after it was withdrawn (counted, never measured).
    pub fn stale(&self) -> bool {
        self.kicked_ns
            .is_some_and(|kicked| kicked < self.announced_ns)
    }

    /// Whether the kick landed between the announcement and the wait's entry: the wait found it pending and
    /// returned without sleeping (counted, never measured).
    pub fn early(&self) -> bool {
        self.kicked_ns
            .is_some_and(|kicked| self.announced_ns <= kicked && kicked < self.waiting_ns)
    }
}

impl Default for Parking {
    fn default() -> Self {
        Self::new()
    }
}

impl Parking {
    /// Not parked, nothing saved yet, wakes untimed.
    pub fn new() -> Self {
        Self {
            parked: AtomicBool::new(false),
            kicks_skipped: AtomicU64::new(0),
            kicked_at: AtomicU64::new(0),
            timed: AtomicBool::new(false),
        }
    }

    /// The shard times its wakes from now on: a tracking shard on a real clock says so once, as it is
    /// built, before any sender can see it parked.
    pub fn time_wakes(&self) {
        self.timed.store(true, Ordering::Relaxed);
    }

    /// The sender's half, called after the message is published (a word in the ring, the control
    /// flag set): kicks the shard when it has announced parking, else counts the kick saved. The
    /// fence orders the publication before the read of the announcement (the module doc). When the
    /// shard times its wakes, the first kick of a park stamps when it was sent, so the woken shard can
    /// time its own wake.
    pub fn kick_if_parked(&self, kick: impl FnOnce()) {
        fence(Ordering::SeqCst);
        if self.parked.load(Ordering::SeqCst) {
            if self.timed.load(Ordering::Relaxed) {
                let _ = self.kicked_at.compare_exchange(
                    0,
                    crate::machine::clock::monotonic_ns(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            }
            kick();
        } else {
            self.kicks_skipped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The shard's half: announces parking, then asks `pending` whether a message already waits.
    /// When one does, withdraws the announcement and returns [`Parked::Pending`] without waiting;
    /// otherwise runs `wait` (the driver's blocking wait, which a kick ends), withdraws the announcement
    /// afterwards, and returns [`Parked::Waited`] — with what the park learned about its wake when the
    /// shard times its wakes, and no clock read at all when it does not. The fence orders the
    /// announcement before the re-check. The announcement's time is read before the announcement itself,
    /// so every stamp a sender takes after reading it is at least that time; the wait's entry time is read
    /// just before `wait`, so a stamp before it is a kick the wait will find pending.
    pub fn park_unless_pending(
        &self,
        pending: impl FnOnce() -> bool,
        wait: impl FnOnce(),
    ) -> Parked {
        let timed = self.timed.load(Ordering::Relaxed);
        let now = || timed.then(crate::machine::clock::monotonic_ns);
        let announced_ns = now();
        self.parked.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        if pending() {
            self.parked.store(false, Ordering::SeqCst);
            return Parked::Pending;
        }
        let waiting_ns = now();
        wait();
        let returned_ns = now();
        let kicked = if timed {
            self.kicked_at.swap(0, Ordering::Relaxed)
        } else {
            0
        };
        self.parked.store(false, Ordering::SeqCst);
        Parked::Waited(match (announced_ns, waiting_ns, returned_ns) {
            (Some(announced_ns), Some(waiting_ns), Some(returned_ns)) => Some(Woken {
                announced_ns,
                waiting_ns,
                returned_ns,
                kicked_ns: (kicked != 0).then_some(kicked),
            }),
            _ => None,
        })
    }

    /// Kicks skipped because the shard had not announced parking (the saving, counted).
    pub fn kicks_skipped(&self) -> u64 {
        self.kicks_skipped.load(Ordering::Relaxed)
    }

    /// Whether the shard is announcing itself parked right now — an **observer's snapshot** (a stall
    /// diagnosis reading it beside the shard's pulse, `registry::Pulse`), never part of the protocol above:
    /// a sender must go through [`kick_if_parked`](Self::kick_if_parked), whose fence is what makes the
    /// answer safe to act on. `Relaxed`: a snapshot that may be a moment stale is what an observer wants.
    pub fn parked(&self) -> bool {
        self.parked.load(Ordering::Relaxed)
    }
}

// Two attributes rather than `all(test, loom)`: clippy's test-context rule (unwrap allowed in
// tests) recognizes only a bare `cfg(test)`, and an unwrap here is a failed model, as it should be.
#[cfg(test)]
#[cfg(loom)]
mod loom_tests {
    use std::sync::atomic::{AtomicU64, Ordering as StdOrdering};

    // loom's `Notify` is the model's stand-in for the driver's kick: test scaffolding under
    // `cfg(loom)` only (D-8 exception 3, a test harness); the two owners are this model's sender
    // and shard.
    use crate::mem::loom_bounds;
    use crate::wakes::WakeBitmap;
    use loom::sync::Notify;

    use super::*;

    /// Shape: the shard's wake bitmap in the model: two task slots, one per sender of the control model.
    const SLOTS: usize = 2;
    /// Format: the slot the sender wakes; the model checks that its wake arrives.
    const WORD: u32 = 0;

    /// Takes every wake waiting in `bitmap`: the slots woken.
    fn take_wakes(bitmap: &WakeBitmap) -> Vec<u32> {
        let mut woken = Vec::new();
        bitmap.drain(|slot| woken.push(slot));
        woken
    }

    /// Kicks delivered, across every explored interleaving.
    static KICKS: AtomicU64 = AtomicU64::new(0);
    /// Kicks skipped because the shard was not parked, across every explored interleaving.
    static SKIPS: AtomicU64 = AtomicU64::new(0);
    /// Waits the shard entered, across every explored interleaving.
    static WAITS: AtomicU64 = AtomicU64::new(0);
    /// Timed parks whose kick's stamp fell inside the wait (a measured wake), across every explored
    /// interleaving of the timed model.
    static MEASURED: AtomicU64 = AtomicU64::new(0);

    /// AC-0.7 (the kick-if-parked protocol of `registry::wake` and `Shard::park`): a sender sets a
    /// task's bit in the shard's wake bitmap and kicks only if the shard announced parking; the shard
    /// announces, re-checks the bitmap, and waits only when it saw nothing. In
    /// every interleaving the shard receives the word: it either saw it before waiting, or was
    /// kicked out of its wait. A lost wake leaves the shard blocked with nothing left to run,
    /// which loom reports as a deadlock. Some interleaving kicked, some skipped the kick, and some
    /// made the shard wait, so neither half of the protocol is vacuous.
    #[test]
    fn a_word_published_while_the_shard_parks_is_never_lost() {
        loom_bounds::explore("parking: one sender against one parking shard", || {
            let bitmap: &'static WakeBitmap = Box::leak(Box::new(WakeBitmap::new(SLOTS)));
            let parking: &'static Parking = Box::leak(Box::new(Parking::new()));
            let kick: &'static Notify = Box::leak(Box::new(Notify::new()));
            let sender = loom::thread::spawn(move || {
                // `registry::wake`'s order: set the bit, then kick if the shard is parked.
                assert!(bitmap.set(WORD));
                parking.kick_if_parked(|| {
                    KICKS.fetch_add(1, StdOrdering::Relaxed);
                    kick.notify();
                });
            });
            let mut waited = false;
            // The shard's loop: drain the bitmap, and park unless a wake is pending.
            let word = loop {
                if let Some(word) = take_wakes(bitmap).first().copied() {
                    break word;
                }
                if matches!(
                    parking.park_unless_pending(|| bitmap.is_pending(), || kick.wait()),
                    Parked::Waited(_)
                ) {
                    waited = true;
                }
            };
            assert_eq!(word, WORD);
            sender.join().unwrap();
            if waited {
                WAITS.fetch_add(1, StdOrdering::Relaxed);
            }
            SKIPS.fetch_add(parking.kicks_skipped(), StdOrdering::Relaxed);
        });
        assert!(
            KICKS.load(StdOrdering::Relaxed) > 0,
            "some interleaving kicked"
        );
        assert!(
            SKIPS.load(StdOrdering::Relaxed) > 0,
            "some interleaving skipped the kick"
        );
        assert!(
            WAITS.load(StdOrdering::Relaxed) > 0,
            "some interleaving made the shard wait"
        );
    }

    /// Delivered control messages, across every explored interleaving of the control model.
    static CONTROL_WAITS: AtomicU64 = AtomicU64::new(0);

    /// AC-0.7 (the control path of `registry::send_control_to` and `shard::drain_control`): two senders each
    /// push one control message into the shard's channel, publish the [`ControlFlag`], and kick the shard if
    /// it announced parking; the shard takes the flag and drains the channel, and parks unless the flag is
    /// pending. In every interleaving both messages are drained: a message whose publication the shard's
    /// take cleared is either drained by that same drain or re-marked. A lost message leaves the shard parked
    /// with nothing left to run, which loom reports as a deadlock. Some interleaving made the shard wait.
    #[test]
    fn a_control_message_published_while_the_shard_drains_is_never_lost() {
        loom_bounds::explore(
            "parking: two control senders against one draining, parking shard",
            || {
                let channel: &'static WakeBitmap = Box::leak(Box::new(WakeBitmap::new(SLOTS)));
                let flag: &'static ControlFlag = Box::leak(Box::new(ControlFlag::new()));
                let parking: &'static Parking = Box::leak(Box::new(Parking::new()));
                let kick: &'static Notify = Box::leak(Box::new(Notify::new()));
                let send = move |word: u32| {
                    // `send_control_to`'s order: push, publish the flag, then kick if the shard is parked.
                    assert!(channel.set(word));
                    flag.publish();
                    parking.kick_if_parked(|| kick.notify());
                };
                let first = loom::thread::spawn(move || send(WORD));
                let second = loom::thread::spawn(move || send(WORD + 1));
                let mut received = 0;
                let mut waited = false;
                // The shard's loop: a step drains when the flag is taken; a step that drained loops again; an idle
                // step parks unless the flag is pending.
                while received < 2 {
                    let mut drained = 0;
                    if flag.take() {
                        drained = take_wakes(channel).len();
                    }
                    received += drained;
                    if received == 2 {
                        break;
                    }
                    if drained > 0 {
                        continue;
                    }
                    if matches!(
                        parking.park_unless_pending(|| flag.is_pending(), || kick.wait()),
                        Parked::Waited(_)
                    ) {
                        waited = true;
                    }
                }
                first.join().unwrap();
                second.join().unwrap();
                if waited {
                    CONTROL_WAITS.fetch_add(1, StdOrdering::Relaxed);
                }
            },
        );
        assert!(
            CONTROL_WAITS.load(StdOrdering::Relaxed) > 0,
            "some interleaving made the shard wait"
        );
    }

    /// §4.3 (A-31): the same protocol with the shard timing its wakes. The kick's stamp is a measurement
    /// word outside the protocol, so the word still arrives in every interleaving; and a park that waits
    /// never reads a stale stamp, because a sender stamps only after publishing its word, so a stamp always
    /// finds the word published and any later park's re-check skips the wait. Some interleaving measured a
    /// wake (the stamp fell inside the wait), so the stamp's path is not vacuous. One word cannot reach a
    /// stamp left over from an earlier park: that classification (`Woken::stale`, `Woken::early`), and the
    /// announcement's time being read before the announcement, are held by the unit tests below — moving
    /// that read after the announcement leaves this model passing (checked 2026-09-25).
    #[test]
    fn a_timed_park_never_loses_the_word_and_reads_only_its_own_stamp() {
        loom_bounds::explore(
            "parking: one sender against one timed parking shard",
            || {
                let bitmap: &'static WakeBitmap = Box::leak(Box::new(WakeBitmap::new(SLOTS)));
                let parking: &'static Parking = Box::leak(Box::new(Parking::new()));
                parking.time_wakes();
                let kick: &'static Notify = Box::leak(Box::new(Notify::new()));
                let sender = loom::thread::spawn(move || {
                    assert!(bitmap.set(WORD));
                    parking.kick_if_parked(|| kick.notify());
                });
                let word = loop {
                    if let Some(word) = take_wakes(bitmap).first().copied() {
                        break word;
                    }
                    if let Parked::Waited(woken) =
                        parking.park_unless_pending(|| bitmap.is_pending(), || kick.wait())
                    {
                        let woken = woken.expect("a timed park that waited reports its wake");
                        assert!(
                            !woken.stale(),
                            "a park read another park's stamp: {woken:?}"
                        );
                        if woken.latency_ns().is_some() {
                            MEASURED.fetch_add(1, StdOrdering::Relaxed);
                        }
                    }
                };
                assert_eq!(word, WORD);
                sender.join().unwrap();
            },
        );
        assert!(
            MEASURED.load(StdOrdering::Relaxed) > 0,
            "some interleaving measured a wake"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Format: a park announced at 1 µs, waiting at 1.5 µs and returned at 5 µs, kicked at `kicked_ns`.
    fn woken(kicked_ns: Option<u64>) -> Woken {
        Woken {
            announced_ns: 1_000,
            waiting_ns: 1_500,
            returned_ns: 5_000,
            kicked_ns,
        }
    }

    /// The shard's online wake estimate (§4.3) takes a sample only from a kick that woke this park asleep:
    /// its stamp between the wait's entry and its return. A timer's or a completion's wake (no stamp), a
    /// stamp from before the announcement, one from the park's setup (the wait found it pending and never
    /// slept), and a kick that landed after the wait had returned yield none.
    #[test]
    fn a_wake_is_measured_only_from_the_kick_that_ended_this_park() {
        assert_eq!(woken(Some(2_000)).latency_ns(), Some(3_000));
        assert_eq!(woken(Some(1_500)).latency_ns(), Some(3_500));
        assert_eq!(woken(None).latency_ns(), None);
        assert_eq!(woken(Some(1_000)).latency_ns(), None);
        assert_eq!(woken(Some(900)).latency_ns(), None);
        assert_eq!(woken(Some(5_001)).latency_ns(), None);
    }

    /// A stamp the park did not measure is told apart: before the announcement it is stale (a sender that
    /// saw an earlier park), from the park's setup it is early; a missing stamp and a late one are neither.
    #[test]
    fn an_unmeasured_stamp_is_told_stale_or_early() {
        let class = |kicked_ns| {
            let park = woken(kicked_ns);
            (park.stale(), park.early())
        };
        assert_eq!(class(Some(900)), (true, false));
        assert_eq!(class(Some(1_000)), (false, true));
        assert_eq!(class(Some(1_499)), (false, true));
        assert_eq!(class(Some(1_500)), (false, false));
        assert_eq!(class(None), (false, false));
        assert_eq!(class(Some(5_001)), (false, false));
    }

    /// Parks `parking` until another thread kicks it (once it has announced the park); what the park did.
    fn kicked_park(parking: &Parking) -> Parked {
        std::thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            let parked = &parking;
            let kicker = scope.spawn(move || {
                while !parked.parked() {
                    std::thread::yield_now();
                }
                parked.kick_if_parked(|| {
                    let _ = tx.send(());
                });
            });
            let parked = parking.park_unless_pending(
                || false,
                || {
                    let _ = rx.recv();
                },
            );
            let _ = kicker.join();
            parked
        })
    }

    /// A park kicked by another thread learns the kick's stamp when its shard times its wakes, and an
    /// untimed park before it left no stamp to misread; a park with a message already pending never waits
    /// and learns nothing.
    #[test]
    #[cfg_attr(miri, ignore)] // a timed park reads the host's boot clock, which Miri does not model (EINVAL)
    fn a_kicked_park_learns_its_kicks_stamp() {
        let parking = Parking::new();
        assert_eq!(kicked_park(&parking), Parked::Waited(None));
        parking.time_wakes();
        assert_eq!(parking.park_unless_pending(|| true, || {}), Parked::Pending);
        let Parked::Waited(Some(woken)) = kicked_park(&parking) else {
            panic!("a timed park that waited learns its wake");
        };
        assert!(woken.kicked_ns.is_some(), "{woken:?}");
        assert!(
            woken.latency_ns().is_some(),
            "the stamp is this park's, none left by the untimed one: {woken:?}"
        );
        assert_eq!(parking.kicks_skipped(), 0);
    }

    /// D-20: a shard that does not time its wakes — a simulated one — parks and is kicked exactly as
    /// before, and neither side reads the host clock: the park waited and learned nothing. Runs under Miri
    /// too, where a host clock read would fail (Miri does not model the boot clock), so a pass there is
    /// the proof that none happened. (The simulation's instruction counts run under callgrind, where the
    /// first host clock read faulted inside rustix's vDSO setup: CI run 36189146259.)
    #[test]
    fn an_untimed_park_waits_and_reads_no_clock() {
        let parking = Parking::new();
        assert_eq!(parking.park_unless_pending(|| true, || {}), Parked::Pending);
        assert_eq!(kicked_park(&parking), Parked::Waited(None));
        assert_eq!(parking.kicks_skipped(), 0);
    }
}
