//! The owner's wake for a job back: the handshake between the owner, about to sleep on its inbox
//! with work waiting on the job out, and the thread that gives the job back (`device.rs`).
//!
//! A job comes back through the owner's returns, which wake no one, and its thread wakes the owner
//! (`Message::Returned`) when the completion leaves the owner something to answer. A job that
//! answered everything it held, a confirmation that held, wakes the owner only when the owner
//! waits on it: submissions for the next frame, I/O, waiters for room, a claim, a close
//! (`Owner::waits_on_device`). The owner says so through a slot of one. The slot holds a plug while
//! the owner does not wait; the owner opens it, by taking what it holds, before it sleeps with work
//! waiting on the job out; the job's thread, once the job is back, claims the wake by filling the
//! slot, and only a thread that fills it wakes the owner. The owner plugs the slot again as it
//! takes each job back.
//!
//! Each side writes, then reads what the other wrote: the owner opens the slot, then looks at its
//! returns; the job's thread puts the job in the returns, then tries the slot. That is the
//! store-buffering shape, where both reads can miss both writes: the owner finds no job and
//! sleeps, the thread finds the slot plugged and wakes no one, and the owner sleeps with the job
//! back (hyper-rt `parking.rs`, the same handshake between a shard and its senders). Each side
//! therefore fences with `SeqCst` between its write and its read: with a `SeqCst` fence after the
//! write on one thread and before the read on the other, whichever fence comes first in the single
//! total order, the later thread's read observes the earlier thread's write (C++20
//! [atomics.order], the fence–fence case), whatever orderings the channels use inside.
//!
//! A thread can claim late: preempted between giving its job back and trying the slot, it fills
//! the slot the owner opened for a later job, and wakes the owner for a job already heard. So the
//! owner opens the slot before every sleep while it waits on the job out, not once a job: a claim
//! it did not need is taken as the plug would be, and the job out claims the slot opened again.
//!
//! The token channel this replaces deadlocked on such threads (2026-10-23, linux/arm64 CI, the
//! power-loss property test; the stacks of every thread, `scripts/hunt-power-loss.sh`): every
//! job's thread sent the job's sequence, blocking, on a channel of two after giving the job back,
//! and the owner emptied it only as it took a job back or as the I/O thread's watch for a job gave
//! it back. Two threads preempted there filled it after the owner had emptied it, and the next
//! job's thread, the I/O thread, blocked on it with the owner asleep and the channel's receiver
//! waiting in the I/O thread's own queue, behind the job it was doing (`Carrier::run` at the
//! token's send, `Owner::next` in the inbox's receive, the test in `Log::with_file` for a look
//! that never ran).

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

#[cfg(all(test, loom))]
use loom::sync::atomic::{Ordering, fence};
#[cfg(not(all(test, loom)))]
use std::sync::atomic::{Ordering, fence};

/// What the slot holds at most: the plug, or the claim that filled it.
const SLOT: usize = 1;

/// The owner's ends of its wake's slot: where it takes what the slot holds, and a sender of its own
/// to plug it again.
pub(crate) struct Wake {
    slot: Receiver<()>,
    plug: SyncSender<()>,
}

impl Wake {
    /// The owner's ends, the slot plugged, and the sender each thread that does jobs claims
    /// through (`device::Lines`).
    pub(crate) fn new() -> (Self, SyncSender<()>) {
        let (plug, slot) = sync_channel(SLOT);
        // Plugged until the owner first waits on a job: a new slot is empty, so this fills it.
        let _ = plug.try_send(());
        let claim = plug.clone();
        (Self { slot, plug }, claim)
    }

    /// Opens the wake before the owner sleeps with work waiting on the job out, by taking what the
    /// slot holds: the plug, or the claim of a thread that woke the owner or is about to. The owner
    /// looks at its returns after this, before it sleeps.
    pub(crate) fn open(&self) {
        open(|| self.slot.try_recv().is_ok());
    }

    /// Plugs the wake as the owner takes a job back: no thread wakes the owner for the job back,
    /// nor for the next while the owner does not wait on it. A slot a claim filled is plugged
    /// already.
    pub(crate) fn close(&self) {
        let _ = self.plug.try_send(());
    }
}

/// The owner's half: `take` opens the slot, then the fence, after which the owner's look at its
/// returns sees any job a thread gave back before that thread's own fence. Whether `take` took
/// anything.
fn open(take: impl FnOnce() -> bool) -> bool {
    let took = take();
    fence(Ordering::SeqCst);
    took
}

/// A job's thread's half, once the job is in the owner's returns: the fence, then `fill` tries the
/// slot. Whether this thread filled it, and so wakes the owner.
pub(crate) fn claim(fill: impl FnOnce() -> bool) -> bool {
    fence(Ordering::SeqCst);
    fill()
}

#[cfg(all(test, loom))]
mod loom_tests {
    //! The handshake under loom: the owner's loop (`Owner::lead`: `arrived`, `watch`, `next`) and a
    //! job's return (`Carrier::run`) step for step, through `open` and `claim` as the log calls
    //! them, with the returns, the slot and the inbox as loom's atomics and channel. The slot's
    //! fill reads before it writes, as a channel's send reads the slot's stamp before it moves the
    //! tail. Every write to the returns and the slot is a read-modify-write, as a channel's send
    //! and receive are on its tail and head: loom keeps a partial modification order, and a plain
    //! store racing the owner's swaps let it give a swap a value that no execution of the C++
    //! model gives it (a swap reads the write just before its own), so it reported a lost wake
    //! that cannot happen. A lost wake is the owner waiting on its inbox with every other thread
    //! done, which loom reports as a deadlock.
    // loom's channels and atomics are the model's stand-ins for the log's channels: test
    // scaffolding under `cfg(all(test, loom))` only; the owners are this model's threads, each value leaked
    // for the model's run as hyper-rt's models do.
    use super::*;
    use loom::sync::atomic::{AtomicBool, AtomicU64};
    use loom::sync::mpsc::{Receiver as Inbox, Sender as Words, channel};

    /// The owner's returns: the sequence of the job back, or 0. One job is out at a time.
    struct Returns(AtomicU64);

    impl Returns {
        fn give(&self, sequence: u64) {
            self.0.swap(sequence, Ordering::AcqRel);
        }

        fn take(&self) -> u64 {
            self.0.swap(0, Ordering::AcqRel)
        }
    }

    /// The wake's slot: full (the plug, or a claim) or open.
    struct Slot(AtomicBool);

    impl Slot {
        fn take(&self) -> bool {
            self.0.swap(false, Ordering::AcqRel)
        }

        fn fill(&self) -> bool {
            !self.0.load(Ordering::Acquire)
                && self
                    .0
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
        }
    }

    /// Everything the model's threads share, leaked for the model's run.
    struct Log {
        returns: Returns,
        slot: Slot,
    }

    fn log() -> &'static Log {
        Box::leak(Box::new(Log {
            returns: Returns(AtomicU64::new(0)),
            // The owner took the last job back and plugged the slot.
            slot: Slot(AtomicBool::new(true)),
        }))
    }

    /// A job's thread once the job is done: the job back, then the owner woken if the completion
    /// leaves it something to answer (`tell`) or the thread claims the wake.
    fn give_back(log: &Log, inbox: &Words<u64>, sequence: u64, tell: bool) {
        log.returns.give(sequence);
        late(log, inbox, sequence, tell);
    }

    /// The rest of a job's return, for a thread whose job is back already.
    fn late(log: &Log, inbox: &Words<u64>, sequence: u64, tell: bool) {
        if tell || claim(|| log.slot.fill()) {
            let _ = inbox.send(sequence);
        }
    }

    /// The owner, its work waiting on every job out, until the job of `last` is back: each job back
    /// is heard and the slot plugged; `start` is told of each job heard but the last, so the next
    /// may go out. Before each sleep it opens the wake and looks once more.
    fn owner(log: &Log, inbox: &Inbox<u64>, last: u64, start: Option<&Words<u64>>) {
        loop {
            let back = log.returns.take();
            if back != 0 {
                log.slot.fill();
                if back == last {
                    return;
                }
                if let Some(start) = start {
                    let _ = start.send(back);
                }
                continue;
            }
            open(|| log.slot.take());
            if log.returns.0.load(Ordering::Acquire) != 0 {
                continue;
            }
            // A lost wake waits here for good.
            let _ = inbox.recv();
        }
    }

    /// Do: a thread whose job the owner heard long ago claims late, while the job out comes back
    /// with nothing for the owner to answer (a confirmation that held). Expect: the owner hears
    /// the job out, in every interleaving: the late claim takes the slot the owner opened for the
    /// job out, and the owner opens it again before it sleeps.
    #[test]
    fn a_late_claim_never_takes_the_wake_of_the_job_out() {
        loom::model(|| {
            let log = log();
            let (words, inbox) = channel();
            let late_words = words.clone();
            let late_thread = loom::thread::spawn(move || late(log, &late_words, 1, false));
            let out = loom::thread::spawn(move || give_back(log, &words, 2, false));
            owner(log, &inbox, 2, None);
            let _ = (late_thread.join(), out.join());
        });
    }

    /// Do: as above, the job out a look, whose completion the owner must answer. Expect: the owner
    /// hears it, whatever the late claim does.
    #[test]
    fn a_late_claim_beside_a_job_that_tells_loses_nothing() {
        loom::model(|| {
            let log = log();
            let (words, inbox) = channel();
            let late_words = words.clone();
            let late_thread = loom::thread::spawn(move || late(log, &late_words, 1, false));
            let out = loom::thread::spawn(move || give_back(log, &words, 2, true));
            owner(log, &inbox, 2, None);
            let _ = (late_thread.join(), out.join());
        });
    }

    /// Do: two jobs one after the other, each a confirmation that held, the second going out only
    /// once the owner has heard the first, whose thread may still be claiming. Expect: the owner
    /// hears both, in every interleaving.
    #[test]
    fn jobs_in_turn_each_wake_the_owner_that_waits() {
        loom::model(|| {
            let log = log();
            let (words, inbox) = channel();
            let (start, started) = channel::<u64>();
            let first_words = words.clone();
            let first = loom::thread::spawn(move || give_back(log, &first_words, 1, false));
            let second = loom::thread::spawn(move || {
                if started.recv().is_ok() {
                    give_back(log, &words, 2, false);
                }
            });
            owner(log, &inbox, 2, Some(&start));
            let _ = (first.join(), second.join());
        });
    }
}
