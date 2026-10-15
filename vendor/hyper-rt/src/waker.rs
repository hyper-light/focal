//! The waker: a `RawWaker` whose data pointer is the task's packed handle word, with a vtable
//! whose `clone` and `drop` are no-ops (§4.3; [B: `RawWakerVTable` docs]: "these functions must
//! all be thread-safe", which a `Copy` word satisfies with no reference count).
#![allow(unsafe_code)]

use std::task::{RawWaker, RawWakerVTable, Waker};

use crate::mem::Encoded;

use crate::registry;
use crate::shard::ShardContext;

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

/// The waker for a task's packed word.
pub fn waker_for(word: Encoded) -> Waker {
    // SAFETY: the vtable's functions are thread-safe (they only read the word and route it) and
    // the data pointer is an integer, never dereferenced.
    unsafe { Waker::from_raw(raw(word)) }
}

/// The packed word a waker made by `waker_for` carries, or `None` for a foreign waker. On a 32-bit target it
/// is the slot-only word ([`Encoded::ANY_GENERATION`]).
pub fn word_of(waker: &Waker) -> Option<Encoded> {
    if std::ptr::eq(waker.vtable(), &VTABLE) {
        Some(decode(waker.data()))
    } else {
        None
    }
}

/// The full word of the task polling with `waker` on this shard thread: the shard's current task, when the
/// waker is that task's own (its slot, and its generation unless the waker carries the slot-only word). `None`
/// for a foreign waker, off a shard, or a waker of another task. A future that registers its task with the
/// shard (a timer, a driver interest, a timer wait) takes the task's identity from here, never from the
/// waker's word, which on a 32-bit target names the slot only.
pub(crate) fn polling_task(waker: &Waker) -> Option<Encoded> {
    let carried = word_of(waker)?;
    let current = registry::with_current(ShardContext::current_task)
        .flatten()?
        .0;
    let same_task = current.shard() == carried.shard()
        && current.slot() == carried.slot()
        && (carried.generation() == current.generation()
            || carried.generation() == Encoded::ANY_GENERATION);
    same_task.then_some(current)
}

fn raw(word: Encoded) -> RawWaker {
    RawWaker::new(std::ptr::without_provenance(encode(word)), &VTABLE)
}

/// A 64-bit data pointer carries the whole word.
#[cfg(target_pointer_width = "64")]
fn encode(word: Encoded) -> usize {
    usize::try_from(word.word()).unwrap_or(usize::MAX)
}

/// The word a 64-bit data pointer carries.
#[cfg(target_pointer_width = "64")]
fn decode(data: *const ()) -> Encoded {
    Encoded::from_word(u64::try_from(data.addr()).unwrap_or(u64::MAX))
}

/// Format: on a 32-bit target, the slot's bits below the shard's in the data pointer.
#[cfg(not(target_pointer_width = "64"))]
const SLOT_BITS_32: u32 = 24;

/// A 32-bit data pointer cannot carry the 64-bit word (`docs/bugs/2026-10-01-a-32-bit-waker-lost-its-task.md`:
/// it became 0): it carries the shard and the slot, and the wake is a slot wake. The registry holds at most
/// [`MAX_SHARDS_32`] shards on a 32-bit target (`registry::MAX_SHARDS`), so the shard always fits.
#[cfg(not(target_pointer_width = "64"))]
fn encode(word: Encoded) -> usize {
    let packed = (u32::from(word.shard()) << SLOT_BITS_32) | (word.slot() & Encoded::MAX_SLOT);
    usize::try_from(packed).unwrap_or(0)
}

/// The slot-only word a 32-bit data pointer carries.
#[cfg(not(target_pointer_width = "64"))]
fn decode(data: *const ()) -> Encoded {
    let packed = u32::try_from(data.addr()).unwrap_or(0);
    let shard = u16::try_from(packed >> SLOT_BITS_32).unwrap_or(u16::MAX);
    let slot = packed & Encoded::MAX_SLOT;
    Encoded::pack(shard, slot, Encoded::ANY_GENERATION).unwrap_or(Encoded::from_word(0))
}

/// Derived: the shards a 32-bit target's waker can name — the data pointer's bits above the slot's.
#[cfg(not(target_pointer_width = "64"))]
pub const MAX_SHARDS_32: usize = 1 << (u32::BITS - SLOT_BITS_32);

unsafe fn clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &VTABLE)
}

unsafe fn wake(data: *const ()) {
    registry::wake(decode(data));
}

unsafe fn wake_by_ref(data: *const ()) {
    registry::wake(decode(data));
}

unsafe fn drop(_data: *const ()) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_waker_carries_its_word_and_clones_for_free() {
        let word = Encoded::pack(9, 1234, 56).unwrap();
        let waker = waker_for(word);
        // A 64-bit waker carries the whole word; a 32-bit one the slot-only word for the same shard and slot.
        let carried = if cfg!(target_pointer_width = "64") {
            word
        } else {
            Encoded::pack(9, 1234, Encoded::ANY_GENERATION).unwrap()
        };
        assert_eq!(word_of(&waker), Some(carried));
        let cloned = waker.clone();
        assert!(waker.will_wake(&cloned));
        assert_eq!(word_of(&cloned), Some(carried));
        assert_eq!(word_of(Waker::noop()), None);
    }
}
