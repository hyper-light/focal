//! Buffers whose address and length meet a device's direct-I/O alignment.
//!
//! Direct I/O transfers straight between the device and user memory, so the buffer address,
//! the file offset and the length must each be a multiple of the alignment the device and
//! file system require (Linux open(2), NOTES on O_DIRECT; Windows "File Buffering",
//! FILE_FLAG_NO_BUFFERING). The buffer is carved out of an ordinary allocation that is
//! `align - 1` bytes longer than needed, so no `unsafe` allocation is involved.

use std::num::NonZeroUsize;

/// The largest alignment accepted. Direct-I/O alignments are logical block sizes and page
/// sizes (512 B to 64 KiB on every platform mantle targets); the bound caps the slack a
/// buffer can waste.
pub const MAX_ALIGNMENT: usize = 1 << 20;

/// The largest single buffer. A buffer holds one I/O, and the bound turns a corrupt length
/// into a refusal instead of an allocation. The largest I/O mantle issues is a chunk record
/// that fills its segment, and the chunk store refuses a segment larger than this before any
/// I/O (`mantle_chunk::layout::Geometry::plan`); every other I/O is bounded by a frame or a
/// batch, far below it.
pub const MAX_BUFFER: usize = 1 << 30;

/// What a buffer refuses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BufError {
    /// An alignment that is not a power of two up to [`MAX_ALIGNMENT`].
    #[error("alignment {0} is not a power of two in 1..={MAX_ALIGNMENT}")]
    Alignment(usize),
    /// A buffer past [`MAX_BUFFER`].
    #[error("buffer of {0} bytes exceeds the {MAX_BUFFER}-byte bound")]
    TooLarge(usize),
    /// More bytes than the buffer has room left for.
    #[error("{requested} bytes do not fit in the {available} bytes left")]
    Full {
        /// Bytes asked to add.
        requested: usize,
        /// Bytes of room left.
        available: usize,
    },
    /// A length past the buffer's capacity.
    #[error("length {len} exceeds capacity {capacity}")]
    Length {
        /// The length asked for.
        len: usize,
        /// The buffer's capacity.
        capacity: usize,
    },
}

/// A power-of-two alignment in bytes, at most [`MAX_ALIGNMENT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Alignment(NonZeroUsize);

impl Alignment {
    /// One byte: no constraint.
    pub const BYTE: Self = Self(NonZeroUsize::MIN);

    /// The alignment of `bytes`, a power of two up to [`MAX_ALIGNMENT`].
    pub fn new(bytes: usize) -> Result<Self, BufError> {
        match NonZeroUsize::new(bytes) {
            Some(n) if n.is_power_of_two() && bytes <= MAX_ALIGNMENT => Ok(Self(n)),
            _ => Err(BufError::Alignment(bytes)),
        }
    }

    /// The alignment in bytes.
    pub fn get(self) -> usize {
        self.0.get()
    }

    fn mask(self) -> usize {
        // A power of two is at least 1, so this cannot wrap.
        self.0.get().wrapping_sub(1)
    }

    /// Whether `n` is a multiple of the alignment.
    pub fn is_aligned(self, n: usize) -> bool {
        n & self.mask() == 0
    }

    /// Whether `n` is a multiple of the alignment.
    pub fn is_aligned_u64(self, n: u64) -> bool {
        // usize is at most 64 bits on every supported target, so the widening is lossless.
        n & (self.mask() as u64) == 0
    }

    /// `n` rounded down to a multiple of the alignment.
    pub fn down(self, n: usize) -> usize {
        n & !self.mask()
    }

    /// `n` rounded down to a multiple of the alignment.
    pub fn down_u64(self, n: u64) -> u64 {
        n & !(self.mask() as u64)
    }

    /// `n` rounded up to a multiple of the alignment, or `None` past `usize::MAX`.
    pub fn up(self, n: usize) -> Option<usize> {
        n.checked_add(self.mask()).map(|m| m & !self.mask())
    }

    /// `n` rounded up to a multiple of the alignment, or `None` past `u64::MAX`.
    pub fn up_u64(self, n: u64) -> Option<u64> {
        let mask = self.mask() as u64;
        n.checked_add(mask).map(|m| m & !mask)
    }

    /// The larger of two alignments; both are powers of two, so it satisfies each.
    pub fn max(self, other: Self) -> Self {
        if other.0 > self.0 { other } else { self }
    }
}

/// A fixed-capacity byte buffer whose first byte and capacity are aligned.
///
/// `len` bytes are in use; the capacity is a multiple of the alignment. [`AlignedBuf::padded`]
/// yields the in-use bytes extended with zeros to the next alignment boundary, which is what a
/// direct write of a record that does not end on a boundary transfers.
pub struct AlignedBuf {
    storage: Vec<u8>,
    start: usize,
    capacity: usize,
    len: usize,
    align: Alignment,
}

impl std::fmt::Debug for AlignedBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuf")
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .field("align", &self.align.get())
            .finish()
    }
}

impl AlignedBuf {
    /// A buffer of no capacity, allocating nothing.
    pub const fn empty() -> Self {
        Self {
            storage: Vec::new(),
            start: 0,
            capacity: 0,
            len: 0,
            align: Alignment::BYTE,
        }
    }

    /// A zero-filled buffer of at least `capacity` bytes, rounded up to the alignment. A
    /// buffer of no capacity allocates nothing, not even the slack alignment takes (audit S11).
    pub fn zeroed(capacity: usize, align: Alignment) -> Result<Self, BufError> {
        let capacity = align.up(capacity).ok_or(BufError::TooLarge(capacity))?;
        if capacity > MAX_BUFFER {
            return Err(BufError::TooLarge(capacity));
        }
        if capacity == 0 {
            return Ok(Self {
                align,
                ..Self::empty()
            });
        }
        // capacity <= 2^30 and mask < 2^20, so the sum cannot overflow.
        let total = capacity
            .checked_add(align.mask())
            .ok_or(BufError::TooLarge(capacity))?;
        let storage = vec![0u8; total];
        let misalignment = storage.as_ptr().addr() & align.mask();
        let start = align.get().wrapping_sub(misalignment) & align.mask();
        Ok(Self {
            storage,
            start,
            capacity,
            len: 0,
            align,
        })
    }

    /// The alignment the buffer meets.
    pub fn alignment(&self) -> Alignment {
        self.align
    }

    /// Bytes the buffer allocated: its capacity and the slack its alignment took.
    fn allocated(&self) -> usize {
        self.storage.len()
    }

    /// Bytes the buffer holds at most: a multiple of its alignment.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Bytes in use.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no byte is in use.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bytes of room left.
    pub fn remaining(&self) -> usize {
        // len <= capacity is an invariant every mutator keeps.
        self.capacity.saturating_sub(self.len)
    }

    /// The in-use bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.window(self.len)
    }

    /// The whole aligned capacity, for reading into.
    pub fn as_mut_capacity(&mut self) -> &mut [u8] {
        let (start, end) = (self.start, self.start.saturating_add(self.capacity));
        self.storage.get_mut(start..end).unwrap_or_default()
    }

    /// The in-use bytes, mutable.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let (start, end) = (self.start, self.start.saturating_add(self.len));
        self.storage.get_mut(start..end).unwrap_or_default()
    }

    /// The in-use bytes followed by zeros up to the next alignment boundary.
    pub fn padded(&mut self) -> Result<&[u8], BufError> {
        let padded = self
            .align
            .up(self.len)
            .ok_or(BufError::TooLarge(self.len))?;
        let (start, len) = (self.start, self.len);
        let tail = self
            .storage
            .get_mut(start.saturating_add(len)..start.saturating_add(padded))
            .ok_or(BufError::Length {
                len: padded,
                capacity: self.capacity,
            })?;
        tail.fill(0);
        Ok(self.window(padded))
    }

    /// Declares the first `len` bytes in use, e.g. after a read filled them.
    pub fn set_len(&mut self, len: usize) -> Result<(), BufError> {
        if len > self.capacity {
            return Err(BufError::Length {
                len,
                capacity: self.capacity,
            });
        }
        self.len = len;
        Ok(())
    }

    /// Declares no byte in use, keeping the capacity.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Appends `bytes`, refused past the capacity.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufError> {
        let available = self.remaining();
        if bytes.len() > available {
            return Err(BufError::Full {
                requested: bytes.len(),
                available,
            });
        }
        let from = self.start.saturating_add(self.len);
        let to = from.saturating_add(bytes.len());
        let dst = self.storage.get_mut(from..to).ok_or(BufError::Full {
            requested: bytes.len(),
            available,
        })?;
        dst.copy_from_slice(bytes);
        self.len = self.len.saturating_add(bytes.len());
        Ok(())
    }

    /// Appends `n` zero bytes.
    pub fn extend_zeros(&mut self, n: usize) -> Result<(), BufError> {
        let available = self.remaining();
        if n > available {
            return Err(BufError::Full {
                requested: n,
                available,
            });
        }
        let from = self.start.saturating_add(self.len);
        let to = from.saturating_add(n);
        let dst = self.storage.get_mut(from..to).ok_or(BufError::Full {
            requested: n,
            available,
        })?;
        dst.fill(0);
        self.len = self.len.saturating_add(n);
        Ok(())
    }

    fn window(&self, len: usize) -> &[u8] {
        self.storage
            .get(self.start..self.start.saturating_add(len))
            .unwrap_or_default()
    }
}

/// Aligned buffers kept for reuse, so steady I/O does not allocate.
///
/// A new buffer is zero-filled, since safe Rust hands out only initialized memory, and
/// allocators zero large blocks by returning their pages to the OS and faulting fresh ones
/// back in: glibc serves them with `mmap` (mallopt(3), M_MMAP_THRESHOLD), and macOS's
/// allocator calls `madvise` on each one, which took 12% of a chunk reader's time and stalled
/// reads for milliseconds (mantle docs/measurements/2026-09-28-chunk-store-benchmark.md). A pool
/// hands back a buffer whose bytes are already initialized.
///
/// Free buffers are kept by capacity. A request takes the smallest free buffer that holds
/// it and is at most twice its size, so a small read never ties up a large buffer; otherwise
/// it allocates exactly what it needs. At most `limit` bytes are kept free, none in a
/// buffer larger than `largest`; a buffer returned past either bound is freed. The free buffers
/// are one list in order of capacity, which once grown takes and gives with no allocation: they
/// are at most `limit` over the alignment, a few for the sizes a log or a volume reads.
///
/// A pool has one owner, the thread that issues the I/O its buffers carry: it takes a buffer
/// and gives it back, and a buffer handed to another thread comes back by message.
pub struct Pool {
    align: Alignment,
    limit: usize,
    largest: usize,
    /// Free buffers, smallest capacity first.
    free: Vec<AlignedBuf>,
    held: usize,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("align", &self.align.get())
            .field("limit", &self.limit)
            .field("largest", &self.largest)
            .field("held", &self.held)
            .finish()
    }
}

impl Pool {
    /// A pool of buffers aligned to `align`, keeping at most `limit` bytes free and no buffer
    /// larger than `largest`.
    pub fn new(align: Alignment, limit: usize, largest: usize) -> Self {
        Self {
            align,
            limit,
            largest,
            free: Vec::new(),
            held: 0,
        }
    }

    /// The alignment of every buffer the pool hands out.
    pub fn alignment(&self) -> Alignment {
        self.align
    }

    /// An empty buffer of at least `capacity` bytes, to be given back with [`Pool::give`].
    pub fn take(&mut self, capacity: usize) -> Result<AlignedBuf, BufError> {
        let needed = self
            .align
            .up(capacity)
            .ok_or(BufError::TooLarge(capacity))?;
        let mut buf = match self.reuse(needed) {
            Some(buf) => buf,
            None => AlignedBuf::zeroed(needed, self.align)?,
        };
        buf.clear();
        Ok(buf)
    }

    /// The smallest free buffer of `needed` bytes at least and twice that at most.
    fn reuse(&mut self, needed: usize) -> Option<AlignedBuf> {
        let at = self.free.partition_point(|b| b.capacity() < needed);
        let fits = self
            .free
            .get(at)
            .is_some_and(|b| b.capacity() <= needed.saturating_mul(2));
        if !fits {
            return None;
        }
        let buf = self.free.remove(at);
        self.held = self.held.saturating_sub(buf.allocated());
        Some(buf)
    }

    /// Bytes held in free buffers.
    pub fn held(&self) -> usize {
        self.held
    }

    /// Keeps `buf` for reuse while the pool's limit holds what it allocated. A buffer of no
    /// capacity has nothing to reuse and is not kept.
    pub fn give(&mut self, buf: AlignedBuf) {
        let capacity = buf.capacity();
        if capacity == 0 || capacity > self.largest || buf.alignment() != self.align {
            return;
        }
        let allocated = buf.allocated();
        if self.held.saturating_add(allocated) <= self.limit {
            self.held = self.held.saturating_add(allocated);
            let at = self.free.partition_point(|b| b.capacity() <= capacity);
            self.free.insert(at, buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn alignment_rejects_non_powers_of_two_and_oversize() {
        assert!(Alignment::new(0).is_err());
        assert!(Alignment::new(3).is_err());
        assert!(Alignment::new(MAX_ALIGNMENT * 2).is_err());
        assert_eq!(Alignment::new(4096).unwrap().get(), 4096);
    }

    #[test]
    fn rounding() {
        let a = Alignment::new(512).unwrap();
        assert_eq!(a.up(0), Some(0));
        assert_eq!(a.up(1), Some(512));
        assert_eq!(a.up(512), Some(512));
        assert_eq!(a.up(usize::MAX), None);
        assert_eq!(a.down(1023), 512);
        assert_eq!(a.up_u64(u64::MAX - 10), None);
        assert!(a.is_aligned_u64(1 << 40));
    }

    #[test]
    fn padded_zeroes_the_tail_even_after_reuse() {
        let mut buf = AlignedBuf::zeroed(8192, Alignment::new(4096).unwrap()).unwrap();
        buf.extend_from_slice(&[0xAB; 5000]).unwrap();
        buf.clear();
        buf.extend_from_slice(&[1, 2, 3]).unwrap();
        let padded = buf.padded().unwrap();
        assert_eq!(padded.len(), 4096);
        assert_eq!(&padded[..3], &[1, 2, 3]);
        assert!(padded[3..].iter().all(|&b| b == 0));
    }

    #[test]
    fn overflow_is_refused() {
        let mut buf = AlignedBuf::zeroed(512, Alignment::new(512).unwrap()).unwrap();
        buf.extend_from_slice(&[0; 500]).unwrap();
        assert_eq!(
            buf.extend_from_slice(&[0; 13]),
            Err(BufError::Full {
                requested: 13,
                available: 12
            })
        );
        assert!(buf.set_len(513).is_err());
        assert!(AlignedBuf::zeroed(MAX_BUFFER + 1, Alignment::BYTE).is_err());
    }

    #[test]
    fn a_pool_reuses_buffers_within_its_bounds() {
        let align = Alignment::new(4096).unwrap();
        // What a buffer of `capacity` allocates: its capacity and the slack its alignment
        // takes, which the pool counts against its limit.
        let allocates = |capacity: usize| capacity + 4095;
        let mut pool = Pool::new(align, 64 << 10, 32 << 10);
        let first = {
            let mut b = pool.take(5000).unwrap();
            assert!(b.capacity() >= 5000 && b.is_empty());
            b.extend_from_slice(&[7; 100]).unwrap();
            let addr = b.as_mut_capacity().as_ptr().addr();
            pool.give(b);
            addr
        };
        assert_eq!(pool.held(), allocates(8192));
        // The same buffer comes back, emptied, for a request it fits within twice over.
        let b = pool.take(4097).unwrap();
        assert_eq!(b.as_slice().len(), 0);
        let again = b.as_slice().as_ptr().addr();
        assert_eq!(again, first);
        assert_eq!(pool.held(), 0);
        pool.give(b);
        assert_eq!(pool.held(), allocates(8192));
        // Buffers past `largest`, or past `limit` in total, are freed rather than kept.
        let large = pool.take(40 << 10).unwrap();
        pool.give(large);
        assert_eq!(pool.held(), allocates(8192));

        // A request under half the size of every free buffer gets a buffer of its own.
        let mut pool = Pool::new(align, 64 << 10, 32 << 10);
        let big = pool.take(20_000).unwrap();
        pool.give(big);
        assert_eq!(pool.held(), allocates(20480));
        let small = pool.take(100).unwrap();
        assert_eq!(small.capacity(), 4096);
        assert_eq!(pool.held(), allocates(20480));
        pool.give(small);
        assert_eq!(pool.held(), allocates(20480) + allocates(4096));
        let held: Vec<_> = (0..10).map(|_| pool.take(16 << 10).unwrap()).collect();
        for b in held {
            pool.give(b);
        }
        assert!(pool.held() <= 64 << 10);
    }

    /// A buffer of no capacity allocates nothing and is never kept: however many are taken
    /// and given back, a pool of no room holds none (audit S11).
    #[test]
    fn empty_buffers_allocate_nothing_and_are_not_kept() {
        let align = Alignment::new(4096).unwrap();
        let empty = AlignedBuf::zeroed(0, align).unwrap();
        assert_eq!((empty.capacity(), empty.allocated()), (0, 0));
        assert_eq!(empty.alignment(), align);
        let mut pool = Pool::new(align, 0, 1 << 20);
        for _ in 0..1_000 {
            let b = pool.take(0).unwrap();
            pool.give(b);
        }
        assert_eq!(pool.held(), 0);
        assert!(pool.free.is_empty());
    }

    proptest! {
        #[test]
        fn start_and_capacity_are_aligned(cap in 0usize..300_000, shift in 0u32..17) {
            let align = Alignment::new(1 << shift).unwrap();
            let mut buf = AlignedBuf::zeroed(cap, align).unwrap();
            prop_assert!(buf.capacity() >= cap);
            prop_assert!(align.is_aligned(buf.capacity()));
            let region = buf.as_mut_capacity();
            prop_assert_eq!(region.len() % align.get(), 0);
            // The first byte is aligned; an empty buffer has none, and its address is the
            // empty slice's (`file::tests::an_empty_buffer_transfers_nothing_at_every_alignment`).
            if !region.is_empty() {
                prop_assert_eq!(region.as_ptr().addr() % align.get(), 0);
            }
        }
    }
}
