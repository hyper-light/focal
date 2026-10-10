//! The installation protocol of the Unix signal handlers (docs/runtime.md §6.1; mantle's final review of
//! hyper-rt, finding 3), apart so loom drives it: installing a kind's handler at a subscription, and the
//! signal thread restoring a kind's default action when it arrived with no subscriber.
//!
//! The two used to race. The restore cleared the kind's `installed` bit and set `SIG_DFL`. A subscription
//! that installed its handler between the clear and the restore was overwritten (scenario A). A
//! subscription that found the bit still set skipped installing, and the restore then took the handler
//! away (scenario B). Either way a live subscriber was left with the default action, and the next signal
//! of the kind ended the process. hyper-rt bans locks, so the order comes from the read-modify-writes on
//! `installed` instead:
//!
//! - **A subscription** publishes its mask first, then RMWs the bit, and calls `ours` when it was clear.
//! - **The restore** RMWs the bit clear, calls `restore_default`, then reads the subscribers. If any
//!   wants the kind, it sets the bit, calls `ours` (whatever the bit said), and hands the signal to them.
//!   Only otherwise does it raise.
//!
//! **Why no subscriber is left on the default**: the kernel serializes `sigaction` calls of a process
//! (Linux takes `sighand->siglock` in `do_sigaction`; XNU takes the proc's signal lock), so one call
//! happens before the other.
//!
//! - **A subscription whose `ours` precedes the restore's `restore_default`** published its mask before
//!   that call, so the restore's read after it sees the mask, and the restore reinstalls.
//! - **A subscription whose `ours` follows** sets the disposition last.
//! - **A subscription that skipped `ours`** found the bit set before the restore cleared it, so its RMW
//!   precedes the restore's in the word's modification order. The restore's acquire then sees its mask,
//!   and the restore reinstalls.
//!
//! The model below makes the kernel's dispositions one word of acquire-release RMWs, as the kernel's lock
//! does. Only the signal thread restores, so restores never race each other.

#[cfg(loom)]
use loom::sync::atomic::{AtomicU32, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU32, Ordering};

use crate::error::RtError;

/// The kernel's half: a kind's disposition set to the runtime's handler or back to the default, and a
/// signal raised.
pub(crate) trait Dispositions {
    /// Installs the runtime's handler for the kind `bit` names.
    fn ours(&self, bit: u32) -> Result<(), RtError>;
    /// Restores the kind's default action.
    fn restore_default(&self, bit: u32);
    /// Raises the kind on the calling thread (its default action then runs).
    fn raise(&self, bit: u32);
}

/// The single bits of `mask`, lowest first.
fn bits(mask: u32) -> impl Iterator<Item = u32> {
    (0..u32::BITS)
        .filter_map(|shift| 1u32.checked_shl(shift))
        .filter(move |bit| mask & bit != 0)
}

/// Installs the handlers of the kinds in `mask` whose bit is clear, for a subscription whose mask is
/// already published. A refusal clears the bit again.
pub(crate) fn install(
    installed: &AtomicU32,
    mask: u32,
    kernel: &impl Dispositions,
) -> Result<(), RtError> {
    for bit in bits(mask) {
        if installed.fetch_or(bit, Ordering::AcqRel) & bit != 0 {
            continue;
        }
        if let Err(refused) = kernel.ours(bit) {
            installed.fetch_and(!bit, Ordering::AcqRel);
            return Err(refused);
        }
    }
    Ok(())
}

/// The signal thread's restore, for the kinds in `unwanted`, which arrived with no subscriber: each gets its
/// default action back and is raised, unless a subscription came meanwhile, which keeps the handler. Returns
/// the kinds kept, which the caller hands to their subscribers. `subscribed` reads the union of the
/// subscribers' masks.
pub(crate) fn restore(
    installed: &AtomicU32,
    unwanted: u32,
    subscribed: impl Fn() -> u32,
    kernel: &impl Dispositions,
) -> u32 {
    let mut kept = 0;
    for bit in bits(unwanted) {
        installed.fetch_and(!bit, Ordering::AcqRel);
        kernel.restore_default(bit);
        if subscribed() & bit != 0 {
            installed.fetch_or(bit, Ordering::AcqRel);
            // A refusal here leaves the bit set over the default action: the next subscriber skips installing
            // and has no handler. A refused `sigaction` of a valid signal and handler does not occur
            // (POSIX.1-2017 lists EINVAL only for a bad number or SIGKILL/SIGSTOP); the bit is cleared so a
            // later subscription tries again.
            if kernel.ours(bit).is_err() {
                installed.fetch_and(!bit, Ordering::AcqRel);
            }
            kept |= bit;
        } else {
            kernel.raise(bit);
        }
    }
    kept
}

#[cfg(loom)]
#[cfg_attr(
    loom,
    allow(clippy::unwrap_used, clippy::disallowed_types, clippy::panic)
)]
mod loom_tests {
    // `cfg(loom)` only (D-8 exception 3, a test harness): loom's `Arc` shares the model's cells between its
    // threads.
    use super::*;
    use loom::sync::Arc;

    /// Format: the kind the model signals.
    const KIND: u32 = 1 << 1;

    /// The kernel's dispositions as one word of "ours" bits; an RMW per call, as `sigaction` is serialized
    /// per signal in the kernel.
    struct Kernel {
        ours: AtomicU32,
        raised: AtomicU32,
    }

    impl Dispositions for Kernel {
        fn ours(&self, bit: u32) -> Result<(), RtError> {
            self.ours.fetch_or(bit, Ordering::AcqRel);
            Ok(())
        }
        fn restore_default(&self, bit: u32) {
            self.ours.fetch_and(!bit, Ordering::AcqRel);
        }
        fn raise(&self, bit: u32) {
            self.raised.fetch_or(bit, Ordering::AcqRel);
        }
    }

    /// Do: a subscription to `KIND` against a restore of `KIND`, every interleaving, from a start where the
    /// handler is installed or not. Expect: once both are done, the subscriber has the handler; and the
    /// restore either raised or kept the kind, never both.
    #[test]
    fn a_subscription_racing_a_restore_keeps_its_handler() {
        for preinstalled in [false, true] {
            loom::model(move || {
                let kernel = Arc::new(Kernel {
                    ours: AtomicU32::new(0),
                    raised: AtomicU32::new(0),
                });
                let installed = Arc::new(AtomicU32::new(0));
                let mask = Arc::new(AtomicU32::new(0));
                if preinstalled {
                    installed.store(KIND, Ordering::Release);
                    kernel.ours.store(KIND, Ordering::Release);
                }
                let subscriber = {
                    let (kernel, installed, mask) = (
                        Arc::clone(&kernel),
                        Arc::clone(&installed),
                        Arc::clone(&mask),
                    );
                    loom::thread::spawn(move || {
                        mask.store(KIND, Ordering::Release);
                        install(&installed, KIND, &*kernel).unwrap();
                    })
                };
                let kept = restore(&installed, KIND, || mask.load(Ordering::Acquire), &*kernel);
                subscriber.join().unwrap();
                assert_eq!(
                    kernel.ours.load(Ordering::Acquire) & KIND,
                    KIND,
                    "a live subscriber was left with the default action"
                );
                let raised = kernel.raised.load(Ordering::Acquire) & KIND != 0;
                assert!(
                    raised != (kept & KIND != 0),
                    "raised and kept at once, or neither"
                );
            });
        }
    }

    /// The second pass's finding 5. Do: a signal of `KIND` arrived with no subscriber, and the signal thread
    /// restores it (keeping it and delivering it to a subscriber it then sees) against a subscription
    /// claiming a slot for `KIND`, every interleaving. Expect: the signal is raised, or it ends up pending on
    /// the subscription, never neither.
    #[test]
    fn a_signal_kept_for_a_new_subscription_is_never_erased() {
        use crate::signal::{CLAIMING, SlotWord, claim_slot, deliver_to};
        loom::model(|| {
            let kernel = Arc::new(Kernel {
                ours: AtomicU32::new(0),
                raised: AtomicU32::new(0),
            });
            let installed = Arc::new(AtomicU32::new(0));
            let mask = Arc::new(AtomicU32::new(0));
            let pending = Arc::new(AtomicU32::new(0));
            let subscriber = {
                let (kernel, installed, mask, pending) = (
                    Arc::clone(&kernel),
                    Arc::clone(&installed),
                    Arc::clone(&mask),
                    Arc::clone(&pending),
                );
                loom::thread::spawn(move || {
                    assert!(claim_slot(&*mask, &*pending, KIND, || {}));
                    install(&installed, KIND, &*kernel).unwrap();
                })
            };
            let kept = restore(&installed, KIND, || mask.get() & !CLAIMING, &*kernel);
            if kept != 0 {
                deliver_to(&*mask, &*pending, kept);
            }
            subscriber.join().unwrap();
            let raised = kernel.raised.load(Ordering::Acquire) & KIND != 0;
            let delivered = pending.load(Ordering::Acquire) & KIND != 0;
            assert!(
                raised || delivered,
                "the signal was neither raised nor delivered"
            );
        });
    }
}
