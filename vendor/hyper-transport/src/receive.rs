//! Where received datagrams are put before the QUIC layer takes them.
//!
//! hyper-quic takes each datagram as a `BytesMut` of its own, and keeps views of it while it holds
//! stream data the owner has not read. Cutting every datagram from one buffer grown with `reserve`
//! grew that buffer a datagram at a time: 2 to 4 reallocations a round of an exchange with a body.
//! Allocating every datagram on its own is an allocation a datagram. So datagrams are cut from
//! chunks of [`RECEIVE_CHUNK`] bytes, which are never grown, and a chunk none of whose views is
//! left is reclaimed whole and cut again.
//!
//! A view pins its whole chunk. A peer whose data sits unread (out of order, on a stream read
//! slowly) can leave chunks each pinned by one small datagram, up to `65,527 / d` times the bytes
//! those datagrams carry for datagrams of `d` bytes: about 54 at QUIC's 1,200-byte floor. So the
//! chunks are a bounded pool charged to the node's budget:
//!
//! - at most `limit` chunks are ever held, each reserved from the budget (on [`Lane::Window`]: it
//!   is memory QUIC holds for peers) before it is allocated and held until the endpoint is dropped;
//!   the chunk memory a peer can cause is at most `limit × 65,527` bytes per endpoint, whatever the
//!   peer sends, and every byte of it is charged;
//! - once every chunk is pinned, or the budget refuses another, a datagram is copied into a buffer
//!   of its own size (counted, [`Receive::copied`]): what it then holds is the datagram's own bytes,
//!   which the connection's receive window bounds and its grant already charges.

use bytes::BytesMut;

use crate::budget::{Budget, Lane, Reservation};

/// A chunk's bytes: one datagram of the largest size QUIC allows, `max_udp_payload_size`'s
/// ceiling of 65,527 bytes (RFC 9000 §18.2), so that a chunk holds any datagram, and about 45 of a
/// 1,452-byte path's.
pub const RECEIVE_CHUNK: usize = 65_527;

/// The chunks datagrams are cut from.
#[derive(Debug)]
pub(crate) struct Receive {
    /// The chunk datagrams are being cut from.
    active: Option<BytesMut>,
    /// Chunks cut to their end, whose views may still be held.
    retired: Vec<BytesMut>,
    /// The budget's grant for each chunk held.
    grants: Vec<Reservation>,
    limit: usize,
    copied: u64,
}

impl Receive {
    /// A pool of at most `limit` chunks.
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            active: None,
            retired: Vec::with_capacity(limit),
            grants: Vec::with_capacity(limit),
            limit,
            copied: 0,
        }
    }

    /// The chunks held.
    pub(crate) fn chunks(&self) -> usize {
        self.grants.len()
    }

    /// The datagrams copied into buffers of their own because no chunk could take them.
    pub(crate) fn copied(&self) -> u64 {
        self.copied
    }

    /// `bytes` as a buffer of their own for the QUIC layer: cut from a chunk where one has room
    /// or can be reclaimed, from a new chunk while the pool and the budget allow one, and copied
    /// on their own otherwise.
    pub(crate) fn take<K>(&mut self, bytes: &[u8], budget: &mut impl Budget<K>) -> BytesMut {
        let length = bytes.len();
        if self.room(length, budget)
            && let Some(active) = self.active.as_mut()
        {
            active.extend_from_slice(bytes);
            return active.split_to(length);
        }
        self.copied = self.copied.saturating_add(1);
        BytesMut::from(bytes)
    }

    /// Whether the active chunk can take `length` bytes without growing, after reclaiming it,
    /// switching to a retired chunk that is free, or adding a chunk.
    fn room<K>(&mut self, length: usize, budget: &mut impl Budget<K>) -> bool {
        if length > RECEIVE_CHUNK {
            return false;
        }
        if self
            .active
            .as_mut()
            .is_some_and(|active| active.try_reclaim(length))
        {
            return true;
        }
        if let Some(full) = self.active.take() {
            self.retired.push(full);
        }
        let free = self
            .retired
            .iter_mut()
            .position(|chunk| chunk.try_reclaim(length));
        if let Some(at) = free {
            self.active = Some(self.retired.swap_remove(at));
            return true;
        }
        self.grow(budget)
    }

    /// A new chunk, if the pool and the budget allow one.
    fn grow<K>(&mut self, budget: &mut impl Budget<K>) -> bool {
        if self.grants.len() >= self.limit {
            return false;
        }
        let Ok(chunk) = u64::try_from(RECEIVE_CHUNK) else {
            return false;
        };
        let Ok(grant) = budget.reserve(chunk, Lane::Window) else {
            return false;
        };
        self.grants.push(grant);
        self.active = Some(BytesMut::with_capacity(RECEIVE_CHUNK));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::Fixed;

    const DATAGRAM: usize = 1_200;

    /// A peer that leaves one small datagram of every chunk unread: each chunk is pinned by one
    /// view. The pool holds no more than its bound, every chunk is charged to the budget, and the
    /// datagrams past it are copied into buffers of their own size instead of growing the pool.
    #[test]
    fn single_unread_datagrams_pin_no_more_than_the_pool() {
        const LIMIT: usize = 4;
        let mut budget = Fixed::new(1 << 30, 0);
        let mut pool = Receive::new(LIMIT);
        let datagram = [7u8; DATAGRAM];
        let per_chunk = RECEIVE_CHUNK / DATAGRAM;
        let mut pinned = Vec::new();
        for index in 0..per_chunk * 20 {
            let view = pool.take::<()>(&datagram, &mut budget);
            assert_eq!(&view[..], &datagram[..]);
            // The first datagram of each chunk stays unread; the rest are dropped at once.
            if index % per_chunk == 0 {
                pinned.push(view);
            }
        }
        assert_eq!(pool.chunks(), LIMIT);
        assert_eq!(budget.used(), u64::try_from(LIMIT * RECEIVE_CHUNK).unwrap());
        assert!(pool.copied() > 0, "past the pool, datagrams are copied");
        // A copied datagram holds its own bytes, not a chunk.
        let copied = pool.take::<()>(&datagram, &mut budget);
        assert!(copied.capacity() < RECEIVE_CHUNK);
        // Once the unread views are read, the chunks are reclaimed and cut again: nothing grows.
        drop(pinned);
        drop(copied);
        let before = pool.copied();
        for _ in 0..per_chunk * LIMIT {
            drop(pool.take::<()>(&datagram, &mut budget));
        }
        assert_eq!((pool.chunks(), pool.copied()), (LIMIT, before));
    }

    #[test]
    fn a_budget_that_refuses_a_chunk_leaves_datagrams_copied() {
        let mut budget = Fixed::new(u64::try_from(RECEIVE_CHUNK).unwrap() - 1, 0);
        let mut pool = Receive::new(8);
        let view = pool.take::<()>(&[1u8; DATAGRAM], &mut budget);
        assert_eq!((pool.chunks(), pool.copied(), budget.used()), (0, 1, 0));
        assert_eq!(view.len(), DATAGRAM);
    }

    #[test]
    fn a_datagram_larger_than_a_chunk_is_copied() {
        let mut budget = Fixed::new(1 << 30, 0);
        let mut pool = Receive::new(8);
        let view = pool.take::<()>(&vec![2u8; RECEIVE_CHUNK + 1], &mut budget);
        assert_eq!((view.len(), pool.copied()), (RECEIVE_CHUNK + 1, 1));
    }
}
