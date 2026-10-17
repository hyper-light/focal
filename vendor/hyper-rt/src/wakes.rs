//! The wakes another thread sends a shard (docs/runtime.md §3.2): one bit per task slot, in 64-bit words, as
//! the Linux block layer keeps its tags (`lib/sbitmap.c`, read at `v6.12`), with a summary bit per word and
//! one pending flag above them, which are this crate's own: `sbitmap` keeps no summary, and spreads its words
//! one to a cache line where these are packed (`docs/research/runtime-sources.md` §2).
//!
//! A wake is a `fetch_or` on the slot's bit, then a `fetch_or` on its word's summary bit when that word went
//! from empty to non-empty or the summary bit reads clear, then a swap of the pending flag;
//! so most wakes write one cache line, not the summary word every waker of 4,096 slots shares (slates-dc's
//! review, 2026-10-06), and no wake waits on another waker to mark the summary for it. The shard
//! drains by swapping the flag, then each marked summary word, then each marked word, to zero: summary
//! before word, so a wake landing between them either is in the word the drain takes or marks the summary
//! again for the next drain (loom-modelled below). A drain starts where the last one stopped and wraps, so
//! under a step's batch bound high slots do not always queue behind low ones. The bitmap is allocated once at the shard's build and holds every slot, so it cannot fill and no
//! sender ever waits: slates' rings made a sender spin until the consumer drained a full one, a loop with no
//! counted bound. Duplicates collapse, as the run queue's pending flags collapse them.
//!
//! A bit names a slot, not a generation: a wake that raced its task's end and the slot's reuse wakes the new
//! occupant once, spuriously, which every future tolerates. Ordering: a producer's three stores are
//! `Release` read-modify-writes and the consumer's swaps are `Acquire`, so a bit set before the flag is seen
//! by the drain that takes the flag; a bit set after the drain took the flag sets it again and is drained
//! next time (DERIVED; loom-modelled against a parking shard in `parking.rs` under `--cfg loom`).

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
// The drain cursor is the owner's alone; it is atomic only so the bitmap stays `Sync` for its wakers.
use std::sync::atomic::AtomicUsize;

/// Format: slots per word.
const BITS: usize = 64;

/// Derived: the words that share the head's line with the pending flag and the first summary word: what is
/// left of a 128-byte line (Apple silicon's, the largest the runtime targets) after those two, `(128 - 16) / 8`;
/// slots `0..896`. A task's slot is the lowest free one when it is spawned (the free stack is last in, first
/// out), so a shard's tasks sit there unless it holds more than that many at once.
const HEAD_WORDS: usize = 14;

/// The bitmap's first line: the flag every wake sets and every drain takes, the summary word of the first 64
/// words, and the first [`HEAD_WORDS`] words. A wake of a slot in it, and the drain that takes it, move one
/// line between the waker's core and the shard's, where the flag, the summary and the word on lines of their
/// own moved three (the two-shard request and reply's wake, counted by line).
#[repr(C, align(128))]
#[derive(Debug)]
struct Head {
    pending: AtomicBool,
    summary: AtomicU64,
    words: [AtomicU64; HEAD_WORDS],
}

/// A shard's wake bitmap.
#[derive(Debug)]
pub struct WakeBitmap {
    head: Head,
    /// Words past the head's.
    words: Box<[AtomicU64]>,
    /// Summary words past the head's first.
    summary: Box<[AtomicU64]>,
    /// The words in all, the head's included: a slot past them is refused.
    word_count: usize,
    /// The word the next drain starts at.
    cursor: AtomicUsize,
}

impl WakeBitmap {
    /// A bitmap for slots `0..slots`.
    pub fn new(slots: usize) -> Self {
        let word_count = slots.div_ceil(BITS);
        Self {
            head: Head {
                pending: AtomicBool::new(false),
                summary: AtomicU64::new(0),
                words: std::array::from_fn(|_| AtomicU64::new(0)),
            },
            words: (0..word_count.saturating_sub(HEAD_WORDS))
                .map(|_| AtomicU64::new(0))
                .collect(),
            summary: (0..word_count.div_ceil(BITS).saturating_sub(1))
                .map(|_| AtomicU64::new(0))
                .collect(),
            word_count,
            cursor: AtomicUsize::new(0),
        }
    }

    /// Word `word` of the bitmap, the head's or the rest's; `None` past the bitmap.
    fn word(&self, word: usize) -> Option<&AtomicU64> {
        if word >= self.word_count {
            return None;
        }
        match word.checked_sub(HEAD_WORDS) {
            None => self.head.words.get(word),
            Some(rest) => self.words.get(rest),
        }
    }

    /// Summary word `group` (of words `64 × group ..`), the head's for the first.
    fn summary(&self, group: usize) -> Option<&AtomicU64> {
        match group.checked_sub(1) {
            None => Some(&self.head.summary),
            Some(rest) => self.summary.get(rest),
        }
    }

    /// The summary words in all.
    fn groups(&self) -> usize {
        self.word_count.div_ceil(BITS)
    }

    /// Marks `slot` woken; false for a slot past the bitmap (a stale or foreign word, ignored).
    pub fn set(&self, slot: u32) -> bool {
        let Ok(slot) = usize::try_from(slot) else {
            return false;
        };
        let (word, bit) = (slot / BITS, slot % BITS);
        let (Some(target), Some(summary)) = (self.word(word), self.summary(word / BITS)) else {
            return false;
        };
        // The waker that took the word from empty marks the summary. A later one marks it too unless it reads it
        // marked: the first waker may not have reached its summary yet, and a drain that runs meanwhile clears
        // no summary bit for the word, so it would miss this waker's bit until the first waker's mark. A wake
        // must not wait on another waker's progress (hyper-rt's registry_contention test found the own wake
        // unseen by the drain right after it, 7 runs in 1,000, 2026-10-07). The read costs no write to the
        // summary's line while it is marked, which keeps most wakes to one line.
        let mark = 1u64 << (word % BITS);
        if target.fetch_or(1u64 << bit, Ordering::AcqRel) == 0
            || summary.load(Ordering::Acquire) & mark == 0
        {
            summary.fetch_or(mark, Ordering::AcqRel);
        }
        // A read-modify-write, where a plain `Release` store would do: the parking protocol's `SeqCst` fences
        // order the store before the shard's re-check ([atomics.fences]), and the parking models pass with it.
        // The swap was taken for a loom 0.7.2 false alarm, reduced in docs/research/runtime-sources.md §4; it
        // stays until a measurement shows the store cheaper on this path.
        self.head.pending.swap(true, Ordering::AcqRel);
        true
    }

    /// Whether a wake may be waiting (one atomic load: the parking protocol's re-check).
    pub fn is_pending(&self) -> bool {
        self.head.pending.load(Ordering::Acquire)
    }

    /// Hands every woken slot to `woken`, clearing it; returns how many. Starts at the word after the last
    /// one the previous drain handed over, and wraps.
    pub fn drain(&self, mut woken: impl FnMut(u32)) -> usize {
        // A load first: a drain that finds nothing pending writes nothing to the line every waker writes. A
        // flag set after the load is the next drain's, as one set after the swap would be; a parking shard
        // looks again after it announces (`crate::parking`), so nothing waits on this drain.
        if !self.head.pending.load(Ordering::Acquire)
            || !self.head.pending.swap(false, Ordering::AcqRel)
        {
            return 0;
        }
        let words = self.word_count;
        let start = self.cursor.load(Ordering::Relaxed).min(words);
        let (start_high, start_low) = (start / BITS, start % BITS);
        let groups = self.groups();
        let mut count: usize = 0;
        let mut last = None;
        // The start group's words below the cursor come last, after the wrap.
        let mut deferred: u64 = 0;
        for step in 0..groups {
            let high = start_high
                .saturating_add(step)
                .checked_rem(groups)
                .unwrap_or(0);
            let Some(summary) = self.summary(high) else {
                continue;
            };
            let mut marked = summary.swap(0, Ordering::AcqRel);
            if step == 0 {
                let below = 1u64
                    .checked_shl(u32::try_from(start_low).unwrap_or(0))
                    .map_or(u64::MAX, |bit| bit.wrapping_sub(1));
                deferred = marked & below;
                marked &= !below;
            }
            count = count.saturating_add(self.drain_group(high, marked, &mut woken, &mut last));
        }
        count = count.saturating_add(self.drain_group(start_high, deferred, &mut woken, &mut last));
        if let Some(last) = last {
            self.cursor.store(
                last.saturating_add(1).checked_rem(words).unwrap_or(0),
                Ordering::Relaxed,
            );
        }
        count
    }

    /// Drains the words `marked` names in summary group `high`; `last` becomes the last word handed over.
    fn drain_group(
        &self,
        high: usize,
        mut marked: u64,
        woken: &mut impl FnMut(u32),
        last: &mut Option<usize>,
    ) -> usize {
        let mut count: usize = 0;
        while marked != 0 {
            let low = usize::try_from(marked.trailing_zeros()).unwrap_or(0);
            marked &= marked.wrapping_sub(1);
            let word = high.saturating_mul(BITS).saturating_add(low);
            let Some(target) = self.word(word) else {
                continue;
            };
            let mut bits = target.swap(0, Ordering::AcqRel);
            if bits != 0 {
                *last = Some(word);
            }
            while bits != 0 {
                let bit = usize::try_from(bits.trailing_zeros()).unwrap_or(0);
                bits &= bits.wrapping_sub(1);
                if let Ok(slot) = u32::try_from(word.saturating_mul(BITS).saturating_add(bit)) {
                    woken(slot);
                    count = count.saturating_add(1);
                }
            }
        }
        count
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn wakes_collapse_and_drain_once_in_slot_order() {
        let bitmap = WakeBitmap::new(5_000);
        for slot in [4_999, 3, 64, 3, 0, 4_999] {
            assert!(bitmap.set(slot));
        }
        assert!(!bitmap.set(5_000 + 64 * 64), "past the bitmap");
        assert!(bitmap.is_pending());
        let mut seen = Vec::new();
        assert_eq!(bitmap.drain(|slot| seen.push(slot)), 4);
        assert_eq!(seen, vec![0, 3, 64, 4_999]);
        assert!(!bitmap.is_pending());
        assert_eq!(bitmap.drain(|_| panic!("nothing is left")), 0);
    }

    /// A drain starts after the word the last one stopped at: with slots in words 0, 1 and 2 woken and the
    /// cursor after word 1, word 2 comes first, then the wrap to word 0, then word 1.
    #[test]
    fn a_drain_starts_where_the_last_one_stopped() {
        let bitmap = WakeBitmap::new(64 * 3);
        bitmap.set(64);
        assert_eq!(bitmap.drain(|_| {}), 1, "the cursor now follows word 1");
        for slot in [5, 70, 130] {
            bitmap.set(slot);
        }
        let mut seen = Vec::new();
        bitmap.drain(|slot| seen.push(slot));
        assert_eq!(seen, vec![130, 5, 70]);
    }

    /// The cursor rotates across summary groups too (more than 4,096 slots).
    #[test]
    fn a_drain_wraps_across_summary_groups() {
        let bitmap = WakeBitmap::new(64 * 64 * 2);
        bitmap.set(64 * 64 + 1);
        bitmap.drain(|_| {});
        for slot in [0, 64 * 64 + 1, 64 * 64 + 64] {
            bitmap.set(slot);
        }
        let mut seen = Vec::new();
        bitmap.drain(|slot| seen.push(slot));
        assert_eq!(seen, vec![64 * 64 + 64, 0, 64 * 64 + 1]);
    }

    /// Wakes from many threads at once all arrive, none twice per drain, while the owner drains as they land.
    #[test]
    fn concurrent_wakes_are_never_lost() {
        let bitmap = WakeBitmap::new(4_096);
        let mut seen = vec![0u32; 4_096];
        std::thread::scope(|scope| {
            for thread in 0..4u32 {
                let bitmap = &bitmap;
                scope.spawn(move || {
                    for slot in (thread..4_096).step_by(4) {
                        bitmap.set(slot);
                    }
                });
            }
            for _ in 0..1_000 {
                bitmap.drain(|slot| seen[usize::try_from(slot).unwrap()] += 1);
            }
        });
        bitmap.drain(|slot| seen[usize::try_from(slot).unwrap()] += 1);
        assert!(seen.iter().all(|count| *count >= 1), "every wake arrived");
    }
}

#[cfg(test)]
#[cfg(loom)]
mod loom_tests {
    use super::*;
    use crate::mem::loom_bounds;

    /// slates-dc's review items 2 and 3 (2026-10-06): two wakers of one word, only the first of which
    /// finds the word empty and marks the summary, against a drain that clears the summary before the word.
    /// In every interleaving each wake is either handed over by the concurrent drain, or still marked with
    /// the pending flag set, so the shard's park re-check sees it and the next drain hands it over: no wake
    /// is stranded under a clear summary or a clear flag.
    #[test]
    fn wakes_sharing_a_word_survive_a_concurrent_drain() {
        loom_bounds::explore("wakes: two wakers of one word against a drain", || {
            let bitmap: &'static WakeBitmap = Box::leak(Box::new(WakeBitmap::new(2)));
            let wakers: Vec<_> = (0..2u32)
                .map(|slot| loom::thread::spawn(move || assert!(bitmap.set(slot))))
                .collect();
            let mut seen = [false; 2];
            bitmap.drain(|slot| seen[usize::try_from(slot).unwrap()] = true);
            for waker in wakers {
                waker.join().unwrap();
            }
            if seen.contains(&false) {
                assert!(bitmap.is_pending(), "an undrained wake keeps the flag set");
            }
            bitmap.drain(|slot| seen[usize::try_from(slot).unwrap()] = true);
            assert_eq!(seen, [true, true], "every wake handed over");
        });
    }

    /// The registry's contention test (2026-10-07): a waker that sets its bit and then drains as the shard
    /// sees it at once, though another waker of the same word took the word from empty and has not yet
    /// marked the summary. Before the summary was also marked by a waker that reads it clear, the drain
    /// found no summary bit and handed nothing over until the other waker's mark.
    #[test]
    fn a_wake_is_seen_by_the_next_drain_whatever_another_waker_has_done() {
        loom_bounds::explore("wakes: own wake then drain, beside another waker", || {
            let bitmap: &'static WakeBitmap = Box::leak(Box::new(WakeBitmap::new(2)));
            let other = loom::thread::spawn(move || assert!(bitmap.set(1)));
            assert!(bitmap.set(0));
            let mut own = false;
            bitmap.drain(|slot| own |= slot == 0);
            other.join().unwrap();
            assert!(own, "the drain after the wake handed it over");
        });
    }
}
