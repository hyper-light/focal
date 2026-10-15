//! The two shapes a shard's desk is built from (docs/runtime.md §3.4): a bounded first-in-first-out
//! ring and a bounded stack, both of `Cell`s, both sized once at the shard's build and never grown.
//!
//! A value is moved in by `set` and moved out by `take`: no reference into either structure ever
//! escapes a call, so there is no borrow to check and nothing to refuse but "full" and "empty". They
//! are single-threaded (`Cell` is `!Sync`): only the shard's own thread touches a desk.

use std::cell::Cell;

/// A value alone on its cache line: the largest line the runtime targets, Apple silicon's 128 bytes (64 on
/// the x86_64 and other aarch64 cores it runs on). A word one set of threads writes, kept off a line another
/// thread reads, moves only between the threads that write it.
#[repr(align(128))]
#[derive(Debug, Default)]
pub(crate) struct Padded<T>(pub(crate) T);

/// A bounded first-in-first-out ring of `T`: values are copied in and out, so a pop reads its slot and
/// leaves it as it was, and the indices wrap by a comparison, never a division (a remainder a push and a
/// pop was a hardware divide on the run queue's every listing and taking).
pub(crate) struct CellRing<T: Copy> {
    slots: Box<[Cell<Option<T>>]>,
    /// The index of the oldest value.
    head: Cell<usize>,
    /// Values held.
    len: Cell<usize>,
}

impl<T: Copy> std::fmt::Debug for CellRing<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellRing")
            .field("capacity", &self.slots.len())
            .field("len", &self.len.get())
            .finish()
    }
}

impl<T: Copy> CellRing<T> {
    /// A ring that holds at most `capacity` values.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            slots: (0..capacity).map(|_| Cell::new(None)).collect(),
            head: Cell::new(0),
            len: Cell::new(0),
        }
    }

    /// The values it can hold.
    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Appends `value`, or hands it back when the ring is full.
    pub(crate) fn push(&self, value: T) -> Result<(), T> {
        let len = self.len.get();
        let capacity = self.slots.len();
        if len >= capacity {
            return Err(value);
        }
        // `head` and `len` are each below the capacity, so their sum is below twice it: one subtraction wraps it.
        let Some(end) = self.head.get().checked_add(len) else {
            return Err(value);
        };
        let at = if end >= capacity {
            end.wrapping_sub(capacity)
        } else {
            end
        };
        let Some(slot) = self.slots.get(at) else {
            return Err(value);
        };
        slot.set(Some(value));
        self.len.set(len.saturating_add(1));
        Ok(())
    }

    /// Takes the oldest value.
    pub(crate) fn pop(&self) -> Option<T> {
        let len = self.len.get();
        if len == 0 {
            return None;
        }
        let head = self.head.get();
        let value = self.slots.get(head)?.get();
        // `head` is below the capacity, so the next index is at most the capacity: it wraps by a comparison.
        let next = head.wrapping_add(1);
        self.head
            .set(if next >= self.slots.len() { 0 } else { next });
        self.len.set(len.wrapping_sub(1));
        value
    }
}

/// A bounded stack of `Copy` values.
pub(crate) struct CellStack<T: Copy> {
    slots: Box<[Cell<T>]>,
    len: Cell<usize>,
}

impl<T: Copy> std::fmt::Debug for CellStack<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellStack")
            .field("capacity", &self.slots.len())
            .field("len", &self.len.get())
            .finish()
    }
}

impl<T: Copy> CellStack<T> {
    /// A stack holding `values`, the last of them on top.
    pub(crate) fn full_of(values: impl ExactSizeIterator<Item = T>) -> Self {
        let slots: Box<[Cell<T>]> = values.map(Cell::new).collect();
        let len = slots.len();
        Self {
            slots,
            len: Cell::new(len),
        }
    }

    /// Values held now.
    pub(crate) fn len(&self) -> usize {
        self.len.get()
    }

    /// Takes the most recently pushed value.
    pub(crate) fn pop(&self) -> Option<T> {
        let len = self.len.get().checked_sub(1)?;
        let value = self.slots.get(len)?.get();
        self.len.set(len);
        Some(value)
    }

    /// Puts `value` back, or hands it back when the stack is full.
    pub(crate) fn push(&self, value: T) -> Result<(), T> {
        let len = self.len.get();
        let Some(slot) = self.slots.get(len) else {
            return Err(value);
        };
        slot.set(value);
        self.len.set(len.saturating_add(1));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ring_is_first_in_first_out_and_refuses_past_its_capacity() {
        let ring = CellRing::new(3);
        for value in 0..3 {
            ring.push(value).unwrap();
        }
        assert_eq!(ring.push(9), Err(9));
        assert_eq!(ring.pop(), Some(0));
        ring.push(3).unwrap();
        assert_eq!(
            std::iter::from_fn(|| ring.pop()).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(ring.len.get(), 0);
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn a_ring_of_no_capacity_refuses_everything() {
        let ring = CellRing::new(0);
        assert_eq!(ring.push(1), Err(1));
        assert_eq!(ring.pop(), None);
    }

    #[test]
    fn a_stack_hands_back_what_was_pushed_last_and_refuses_past_its_capacity() {
        let stack = CellStack::full_of([1u32, 2, 3].into_iter());
        assert_eq!(stack.pop(), Some(3));
        assert_eq!(stack.pop(), Some(2));
        stack.push(7).unwrap();
        assert_eq!(stack.pop(), Some(7));
        assert_eq!(stack.pop(), Some(1));
        assert_eq!(stack.pop(), None);
        for value in 0..3 {
            stack.push(value).unwrap();
        }
        assert_eq!(stack.push(9), Err(9));
    }
}
