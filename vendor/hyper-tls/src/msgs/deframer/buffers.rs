use alloc::vec::Vec;
use core::mem;
use core::ops::Range;
use std::io;

use crate::error::Error;
use crate::msgs::message::MAX_WIRE_SIZE;

/// The refusal for a range or slice that is not where the deframer put it: every range comes
/// from `locate` over the same buffer, so this is a defect, not peer input (upstream unwrapped
/// or asserted).
const MISPLACED: Error = Error::Internal("deframer range outside its buffer");

/// Conversion from a slice within a larger buffer into
/// a `Range` offset within.
#[derive(Debug)]
pub(crate) struct Locator {
    bounds: Range<*const u8>,
}

impl Locator {
    #[inline]
    pub(crate) fn new(slice: &[u8]) -> Self {
        Self {
            bounds: slice.as_ptr_range(),
        }
    }

    #[inline]
    pub(crate) fn locate(&self, slice: &[u8]) -> Result<Range<usize>, Error> {
        if !self.fully_contains(slice) {
            return Err(MISPLACED);
        }
        let start = (slice.as_ptr() as usize)
            .checked_sub(self.bounds.start as usize)
            .ok_or(MISPLACED)?;
        let end = start.checked_add(slice.len()).ok_or(MISPLACED)?;
        Ok(Range { start, end })
    }

    #[inline]
    pub(crate) fn fully_contains(&self, slice: &[u8]) -> bool {
        let bounds = slice.as_ptr_range();
        bounds.start >= self.bounds.start && bounds.end <= self.bounds.end
    }
}

/// Conversion from a `Range` offset to the original slice.
pub(crate) struct Delocator<'b> {
    slice: &'b [u8],
}

impl<'b> Delocator<'b> {
    #[inline]
    pub(crate) fn new(slice: &'b [u8]) -> Self {
        Self { slice }
    }

    #[inline]
    pub(crate) fn slice_from_range(&'_ self, range: &Range<usize>) -> Result<&'b [u8], Error> {
        // `range` came from `locate()` for the same buffer.
        self.slice.get(range.clone()).ok_or(MISPLACED)
    }

    #[inline]
    pub(crate) fn locator(self) -> Locator {
        Locator::new(self.slice)
    }
}

/// Reordering the underlying buffer based on ranges.
pub(crate) struct Coalescer<'b> {
    slice: &'b mut [u8],
}

impl<'b> Coalescer<'b> {
    #[inline]
    pub(crate) fn new(slice: &'b mut [u8]) -> Self {
        Self { slice }
    }

    #[inline]
    pub(crate) fn copy_within(
        &mut self,
        from: Range<usize>,
        to: Range<usize>,
    ) -> Result<(), Error> {
        // `copy_within` panics unless both ranges lie in the slice; they are spans this
        // deframer located, but upstream only asserted that in debug builds.
        if from.len() != to.len()
            || self.slice.get(from.clone()).is_none()
            || self.slice.get(to.clone()).is_none()
        {
            return Err(MISPLACED);
        }
        self.slice.copy_within(from, to.start);
        Ok(())
    }

    #[inline]
    pub(crate) fn delocator(self) -> Delocator<'b> {
        Delocator::new(self.slice)
    }
}

/// Accounting structure tracking progress in parsing a single buffer.
#[derive(Clone, Debug)]
pub(crate) struct BufferProgress {
    /// Prefix of the buffer that has been processed so far.
    ///
    /// `processed` may exceed `discard`, that means we have parsed
    /// some buffer, but are still using it.  This happens due to
    /// in-place decryption of incoming records, and in-place
    /// reassembly of handshake messages.
    ///
    /// 0 <= processed <= len
    processed: usize,

    /// Prefix of the buffer that can be removed.
    ///
    /// If `discard` exceeds `processed`, that means we are ignoring
    /// data without processing it.
    ///
    /// 0 <= discard <= len
    discard: usize,
}

impl BufferProgress {
    pub(super) fn new(processed: usize) -> Self {
        Self {
            processed,
            discard: 0,
        }
    }

    #[inline]
    pub(crate) fn add_discard(&mut self, discard: usize) {
        // A prefix length of one buffer: bounded by its length, so this never saturates.
        self.discard = self.discard.saturating_add(discard);
    }

    #[inline]
    pub(crate) fn add_processed(&mut self, processed: usize) {
        // As `add_discard`.
        self.processed = self.processed.saturating_add(processed);
    }

    #[inline]
    pub(crate) fn take_discard(&mut self) -> usize {
        // the caller is about to discard `discard` bytes
        // from the front of the buffer.  adjust `processed`
        // down by the same amount.
        self.processed = self.processed.saturating_sub(self.discard);
        mem::take(&mut self.discard)
    }

    #[inline]
    pub(crate) fn processed(&self) -> usize {
        self.processed
    }
}

#[derive(Default, Debug)]
pub(crate) struct DeframerVecBuffer {
    /// Buffer of data read from the socket, in the process of being parsed into messages.
    ///
    /// For buffer size management, checkout out the [`DeframerVecBuffer::prepare_read()`] method.
    buf: Vec<u8>,

    /// What size prefix of `buf` is used.
    used: usize,
}

impl DeframerVecBuffer {
    /// Discard `taken` bytes from the start of our buffer.
    pub(crate) fn discard(&mut self, taken: usize) {
        #[allow(clippy::comparison_chain)]
        if taken < self.used {
            /* Before:
             * +----------+----------+----------+
             * | taken    | pending  |xxxxxxxxxx|
             * +----------+----------+----------+
             * 0          ^ taken    ^ self.used
             *
             * After:
             * +----------+----------+----------+
             * | pending  |xxxxxxxxxxxxxxxxxxxxx|
             * +----------+----------+----------+
             * 0          ^ self.used
             */

            if let Some(pending) = self.buf.get(taken..self.used) {
                let pending = pending.len();
                self.buf.copy_within(taken..self.used, 0);
                self.used = pending;
            }
        } else if taken >= self.used {
            self.used = 0;
        }
    }

    pub(crate) fn filled_mut(&mut self) -> &mut [u8] {
        // `used` never exceeds the buffer: `read` and `extend` grow it within.
        let used = self.used.min(self.buf.len());
        self.buf.split_at_mut(used).0
    }

    pub(crate) fn filled(&self) -> &[u8] {
        let used = self.used.min(self.buf.len());
        self.buf.split_at(used).0
    }
}

impl DeframerVecBuffer {
    /// Read some bytes from `rd`, and add them to the buffer.
    pub(crate) fn read(&mut self, rd: &mut dyn io::Read, in_handshake: bool) -> io::Result<usize> {
        if let Err(err) = self.prepare_read(in_handshake) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, err));
        }

        // Try to do the largest reads possible. Note that if
        // we get a message with a length field out of range here,
        // we do a zero length read.  That looks like an EOF to
        // the next layer up, which is fine.
        let room = self
            .buf
            .get_mut(self.used..)
            .ok_or_else(|| io::Error::other("deframer buffer cursor past its end"))?;
        let new_bytes = rd.read(room)?;
        self.used = self
            .used
            .checked_add(new_bytes)
            .filter(|used| *used <= self.buf.len())
            .ok_or_else(|| {
                io::Error::other("reader reported more bytes than it was given room for")
            })?;
        Ok(new_bytes)
    }

    /// Resize the internal `buf` if necessary for reading more bytes.
    fn prepare_read(&mut self, is_joining_hs: bool) -> Result<(), &'static str> {
        /// TLS allows for handshake messages of up to 16MB.  We
        /// restrict that to 64KB to limit potential for denial-of-
        /// service.
        const MAX_HANDSHAKE_SIZE: u32 = 0xffff;

        /// The read size: one page on the common platforms, upstream's choice.
        const READ_SIZE: usize = 4096;

        // We allow a maximum of 64k of buffered data for handshake messages only. Enforce this
        // by varying the maximum allowed buffer size here based on whether a prefix of a
        // handshake payload is currently being buffered. Given that the first read of such a
        // payload will only ever be 4k bytes, the next time we come around here we allow a
        // larger buffer size. Once the large message and any following handshake messages in
        // the same flight have been consumed, `pop()` will call `discard()` to reset `used`.
        // At this point, the buffer resizing logic below should reduce the buffer size.
        let allow_max = match is_joining_hs {
            true => MAX_HANDSHAKE_SIZE as usize,
            false => MAX_WIRE_SIZE,
        };

        if self.used >= allow_max {
            return Err("message buffer full");
        }

        // If we can and need to increase the buffer size to allow a 4k read, do so. After
        // dealing with a large handshake message (exceeding `OutboundOpaqueMessage::MAX_WIRE_SIZE`),
        // make sure to reduce the buffer size again (large messages should be rare).
        // Also, reduce the buffer size if there are neither full nor partial messages in it,
        // which usually means that the other side suspended sending data.
        // `used` is below `allow_max` here, so the sum does not saturate.
        let need_capacity = Ord::min(allow_max, self.used.saturating_add(READ_SIZE));
        if need_capacity > self.buf.len() {
            self.buf.resize(need_capacity, 0);
        } else if self.used == 0 || self.buf.len() > allow_max {
            self.buf.resize(need_capacity, 0);
            self.buf.shrink_to(need_capacity);
        }

        Ok(())
    }

    /// Append `bytes` to the end of this buffer.
    ///
    /// Return a `Range` saying where it went.
    pub(crate) fn extend(&mut self, bytes: &[u8]) -> Result<Range<usize>, Error> {
        let start = self.used;
        let end = start.checked_add(bytes.len()).ok_or(MISPLACED)?;
        if self.buf.len() < end {
            self.buf.resize(end, 0);
        }
        self.buf
            .get_mut(start..end)
            .ok_or(MISPLACED)?
            .copy_from_slice(bytes);
        self.used = end;
        Ok(Range { start, end })
    }
}

/// A borrowed version of [`DeframerVecBuffer`] that tracks discard operations
#[derive(Debug)]
pub(crate) struct DeframerSliceBuffer<'a> {
    // a fully initialized buffer that will be deframed
    buf: &'a mut [u8],
    // number of bytes to discard from the front of `buf` at a later time
    discard: usize,
}

impl<'a> DeframerSliceBuffer<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, discard: 0 }
    }

    /// Tracks a pending discard operation of `num_bytes`
    pub(crate) fn queue_discard(&mut self, num_bytes: usize) {
        // A prefix length of `buf`: bounded by its length, so this never saturates.
        self.discard = self.discard.saturating_add(num_bytes);
    }

    pub(crate) fn pending_discard(&self) -> usize {
        self.discard
    }

    pub(crate) fn filled_mut(&mut self) -> &mut [u8] {
        // A discard past the end leaves nothing to deframe.
        let discard = self.discard.min(self.buf.len());
        self.buf.split_at_mut(discard).1
    }
}
