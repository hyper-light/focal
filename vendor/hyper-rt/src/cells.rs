//! The two shapes a shard's desk is built from (docs/runtime.md §3.4): a bounded first-in-first-out
//! ring and a bounded stack, both of `Cell`s, both sized once at the shard's build and never grown.
//!
//! A value is moved in by `set` and moved out by `take`: no reference into either structure ever
//! escapes a call, so there is no borrow to check and nothing to refuse but "full" and "empty". They
//! are single-threaded (`Cell` is `!Sync`): only the shard's own thread touches a desk.

use std::cell::Cell;

/// A bounded first-in-first-out ring of `T`.
pub(crate) struct CellRing<T> {
    slots: Box<[Cell<Option<T>>]>,
    /// The index of the oldest value.
    head: Cell<usize>,
    /// Values held.
    len: Cell<usize>,
}

impl<T> std::fmt::Debug for CellRing<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellRing")
            .field("capacity", &self.slots.len())
            .field("len", &self.len.get())
            .finish()
    }
}

impl<T> CellRing<T> {
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

    /// Values held.
    pub(crate) fn len(&self) -> usize {
        self.len.get()
    }

    /// Whether it holds nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    /// Appends `value`, or hands it back when the ring is full.
    pub(crate) fn push(&self, value: T) -> Result<(), T> {
        let len = self.len.get();
        let capacity = self.slots.len();
        if len >= capacity {
            return Err(value);
        }
        let Some(at) = self
            .head
            .get()
            .checked_add(len)
            .and_then(|end| end.checked_rem(capacity))
        else {
            return Err(value);
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
        let value = self.slots.get(head)?.take();
        let next = head
            .checked_add(1)
            .and_then(|next| next.checked_rem(self.slots.len()))
            .unwrap_or(0);
        self.head.set(next);
        self.len.set(len.saturating_sub(1));
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
        assert!(ring.is_empty());
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
