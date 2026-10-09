//! A whole file sealed by STREAM (`docs/seal.md` §4), written and read through a
//! [`BlockFile`]: the writer and reader every consumer would otherwise build again around
//! [`FileSealer`] and [`FileOpener`].
//!
//! # Layout
//!
//! ```text
//! stream:  header (HEADER) ‖ segment 0 ‖ tag 0 ‖ … ‖ segment n−1 (last) ‖ tag n−1   = L bytes
//! file:    stream ‖ zeros ‖ L (u64, little-endian)                                    = P bytes
//! ```
//!
//! The stream is exactly STREAM's (`stream::sealed_len` of the plaintext). The file pads it to
//! the file's alignment, so a file opened for direct I/O takes every transfer whole, and states
//! the stream's length `L` in its last eight bytes, with `P` the least multiple of the alignment
//! at or past `L + 8`. `L` is in the clear and needs no MAC of its own: STREAM marks the last
//! segment in its nonce, so a stated length other than the true one either puts a tag where none
//! was sealed or takes for last a segment that was not sealed last, and the open fails. The reader
//! also refuses padding that is not zero and a length that does not round to `P`.
//!
//! # Durability
//!
//! A file is written once, to a name no reader opens, and [`SealedWriter::finish`] flushes it
//! (`BlockFile::sync_data`: the platform's full flush). The caller then renames it into place and
//! flushes the directory (`hyper_block::file::rename_durable`; Pillai et al., OSDI 2014), so a
//! crash leaves the old file or the new one whole, never a part.
//!
//! # Memory
//!
//! One aligned buffer per writer or reader, made when it is, reused for every segment: no
//! allocation after setup. Its size is the segment, its tag, the footer and two alignments.

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment};

use crate::keys::WrappingKey;
use crate::stream::{FileOpener, FileSealer, HEADER, Header};
use crate::{SealError, TAG};

/// Bytes of the footer: the stream's length, a `u64`.
const FOOTER: usize = 8;

/// Why a sealed file was not written or read.
#[derive(Debug, thiserror::Error)]
pub enum SealedFileError {
    /// A seal or open failed: tampering, the wrong key, or a size outside the construction.
    #[error(transparent)]
    Seal(#[from] SealError),
    /// The device refused a transfer or a flush. A failed flush leaves what was written of unknown
    /// durability (Rebello et al., ATC 2020): the file is not to be trusted or renamed into place.
    #[error(transparent)]
    Disk(#[from] DiskError),
    /// The file's layout is not one a writer makes: its length, footer or padding. Bytes changed
    /// or cut by someone, never a crash, since a file is renamed into place only once flushed.
    #[error("a sealed file's layout is damaged: {0}")]
    Layout(&'static str),
    /// A buffer past what this machine or `hyper_block::buf` allows, or a write to a file that
    /// already holds bytes.
    #[error("a sealed file's bound: {0}")]
    Bound(&'static str),
}

/// The buffer a writer or reader keeps: a segment and its tag, the footer, and an alignment
/// either side, rounded to the alignment.
fn working_buffer(segment: u32, align: Alignment) -> Result<AlignedBuf, SealedFileError> {
    let bytes = usize::try_from(segment)
        .ok()
        .and_then(|s| s.checked_add(TAG))
        .and_then(|s| s.checked_add(FOOTER))
        .and_then(|s| s.checked_add(align.get().checked_mul(2)?))
        .and_then(|s| align.up(s))
        .ok_or(SealedFileError::Bound(
            "a segment past what this machine addresses",
        ))?;
    AlignedBuf::zeroed(bytes, align)
        .map_err(|_| SealedFileError::Bound("a segment past the largest buffer"))
}

fn as_u64(n: usize) -> Result<u64, SealedFileError> {
    u64::try_from(n).map_err(|_| SealedFileError::Bound("a length past a u64"))
}

fn as_usize(n: u64) -> Result<usize, SealedFileError> {
    usize::try_from(n)
        .map_err(|_| SealedFileError::Bound("a length past what this machine addresses"))
}

/// Writes a new sealed file, in order, through `F`.
pub struct SealedWriter<F: BlockFile> {
    file: F,
    sealer: FileSealer,
    align: Alignment,
    segment: usize,
    buf: AlignedBuf,
    /// Sealed stream bytes at the front of `buf` not yet written: fewer than an alignment once a
    /// segment is done.
    pending: usize,
    /// Plaintext of the current segment, after `pending` in `buf`.
    filled: usize,
    /// File bytes written, always a multiple of the alignment.
    written: u64,
    /// Stream bytes so far, header included.
    stream: u64,
}

impl<F: BlockFile> SealedWriter<F> {
    /// A writer of a new file in `file`, which must be empty, under a new data key wrapped by
    /// `parent`, with segments of `segment` plaintext bytes (at least `stream::MIN_SEGMENT`).
    pub fn new(file: F, parent: &WrappingKey, segment: u32) -> Result<Self, SealedFileError> {
        if !file.is_empty()? {
            return Err(SealedFileError::Bound(
                "a sealed file is written once, into an empty file",
            ));
        }
        let (sealer, header) = FileSealer::new(parent, segment)?;
        let align = file.alignment();
        let mut buf = working_buffer(segment, align)?;
        let encoded = header.encode();
        buf.as_mut_capacity()
            .get_mut(..HEADER)
            .ok_or(SealedFileError::Bound("a buffer shorter than a header"))?
            .copy_from_slice(&encoded);
        let mut writer = Self {
            file,
            sealer,
            align,
            segment: usize::try_from(segment).map_err(|_| {
                SealedFileError::Bound("a segment past what this machine addresses")
            })?,
            buf,
            pending: HEADER,
            filled: 0,
            written: 0,
            stream: as_u64(HEADER)?,
        };
        writer.drain()?;
        Ok(writer)
    }

    /// Writes the aligned prefix of the pending bytes and moves the rest to the front.
    fn drain(&mut self) -> Result<(), SealedFileError> {
        let whole = self.align.down(self.pending);
        if whole == 0 {
            return Ok(());
        }
        let out = self
            .buf
            .as_mut_capacity()
            .get(..whole)
            .ok_or(SealedFileError::Bound("pending bytes past the buffer"))?;
        self.file.write_all_at(out, self.written)?;
        self.written = self
            .written
            .checked_add(as_u64(whole)?)
            .ok_or(SealedFileError::Bound("a file past a u64"))?;
        self.buf
            .as_mut_capacity()
            .copy_within(whole..self.pending, 0);
        self.pending = self
            .pending
            .checked_sub(whole)
            .ok_or(SealedFileError::Bound("pending bytes miscounted"))?;
        Ok(())
    }

    /// Seals the plaintext in the buffer as the next segment and queues it and its tag.
    fn seal_segment(&mut self, last: bool) -> Result<(), SealedFileError> {
        let end = self
            .pending
            .checked_add(self.filled)
            .ok_or(SealedFileError::Bound("a segment past the buffer"))?;
        let tag_end = end
            .checked_add(TAG)
            .ok_or(SealedFileError::Bound("a segment past the buffer"))?;
        let capacity = self.buf.as_mut_capacity();
        let plain = capacity
            .get_mut(self.pending..end)
            .ok_or(SealedFileError::Bound("a segment past the buffer"))?;
        let tag = self.sealer.seal(plain, last)?;
        capacity
            .get_mut(end..tag_end)
            .ok_or(SealedFileError::Bound("a tag past the buffer"))?
            .copy_from_slice(&tag);
        let sealed = as_u64(
            self.filled
                .checked_add(TAG)
                .ok_or(SealedFileError::Bound("a segment"))?,
        )?;
        self.stream = self
            .stream
            .checked_add(sealed)
            .ok_or(SealedFileError::Bound("a stream past a u64"))?;
        self.pending = tag_end;
        self.filled = 0;
        Ok(())
    }

    /// Appends `bytes` to the file's plaintext. A full segment is sealed only once more bytes
    /// follow it, so the last segment is always sealed as last, however the plaintext divides.
    pub fn write(&mut self, mut bytes: &[u8]) -> Result<(), SealedFileError> {
        while !bytes.is_empty() {
            if self.filled == self.segment {
                self.seal_segment(false)?;
                self.drain()?;
            }
            let room = self
                .segment
                .checked_sub(self.filled)
                .ok_or(SealedFileError::Bound("a segment overfilled"))?;
            let take = room.min(bytes.len());
            let (now, rest) = bytes.split_at(take);
            let from = self
                .pending
                .checked_add(self.filled)
                .ok_or(SealedFileError::Bound("a segment past the buffer"))?;
            let to = from
                .checked_add(take)
                .ok_or(SealedFileError::Bound("a segment past the buffer"))?;
            self.buf
                .as_mut_capacity()
                .get_mut(from..to)
                .ok_or(SealedFileError::Bound("a segment past the buffer"))?
                .copy_from_slice(now);
            self.filled = self
                .filled
                .checked_add(take)
                .ok_or(SealedFileError::Bound("a segment overfilled"))?;
            bytes = rest;
        }
        Ok(())
    }

    /// The file being written, for a test that crashes the writer before it finishes.
    #[cfg(test)]
    pub(crate) fn file_for_test(&self) -> &F {
        &self.file
    }

    /// Seals the last segment, writes the padding and the footer, and flushes the file. The file
    /// is whole and durable when this returns; the caller then renames it into place.
    pub fn finish(mut self) -> Result<F, SealedFileError> {
        self.seal_segment(true)?;
        let stream = self.stream;
        let footer_at = self
            .pending
            .checked_add(FOOTER)
            .and_then(|end| self.align.up(end))
            .and_then(|end| end.checked_sub(FOOTER))
            .ok_or(SealedFileError::Bound("a footer past the buffer"))?;
        let end = footer_at
            .checked_add(FOOTER)
            .ok_or(SealedFileError::Bound("a footer past the buffer"))?;
        let capacity = self.buf.as_mut_capacity();
        capacity
            .get_mut(self.pending..footer_at)
            .ok_or(SealedFileError::Bound("padding past the buffer"))?
            .fill(0);
        capacity
            .get_mut(footer_at..end)
            .ok_or(SealedFileError::Bound("a footer past the buffer"))?
            .copy_from_slice(&stream.to_le_bytes());
        self.pending = end;
        self.drain()?;
        if self.pending != 0 {
            return Err(SealedFileError::Bound(
                "a footer that does not end on the alignment",
            ));
        }
        self.file.sync_data()?;
        Ok(self.file)
    }
}

/// Where a file's stream lies, read from its length and footer.
#[derive(Debug, Clone, Copy)]
struct Shape {
    segment: u64,
    /// Segments, the last included.
    count: u64,
    /// Plaintext bytes of the last segment.
    last: u64,
    plain: u64,
}

impl Shape {
    /// The stream of `stream` bytes in segments of `segment`, or a layout refusal.
    fn of(stream: u64, segment: u32) -> Result<Self, SealedFileError> {
        let segment = u64::from(segment);
        let tag = as_u64(TAG)?;
        let full = segment
            .checked_add(tag)
            .ok_or(SealedFileError::Layout("a segment past a u64"))?;
        let body = stream
            .checked_sub(as_u64(HEADER)?)
            .filter(|body| *body >= tag)
            .ok_or(SealedFileError::Layout(
                "a stream shorter than a header and one tag",
            ))?;
        let count = body.div_ceil(full);
        let before = count
            .checked_sub(1)
            .and_then(|n| n.checked_mul(full))
            .ok_or(SealedFileError::Layout("a stream of no segment"))?;
        let last = body
            .checked_sub(before)
            .and_then(|stored| stored.checked_sub(tag))
            .filter(|last| *last <= segment)
            .ok_or(SealedFileError::Layout(
                "a last segment shorter than its tag",
            ))?;
        // An empty plaintext is one empty segment; otherwise the last holds at least a byte.
        if last == 0 && count > 1 {
            return Err(SealedFileError::Layout(
                "an empty last segment after others",
            ));
        }
        let plain = count
            .checked_sub(1)
            .and_then(|n| n.checked_mul(segment))
            .and_then(|n| n.checked_add(last))
            .ok_or(SealedFileError::Layout("a plaintext past a u64"))?;
        Ok(Self {
            segment,
            count,
            last,
            plain,
        })
    }

    /// The file offset of segment `index`'s first stored byte.
    fn offset(&self, index: u64) -> Option<u64> {
        let full = self.segment.checked_add(u64::try_from(TAG).ok()?)?;
        index
            .checked_mul(full)?
            .checked_add(u64::try_from(HEADER).ok()?)
    }

    /// Plaintext bytes of segment `index`.
    fn len(&self, index: u64) -> u64 {
        if index.saturating_add(1) == self.count {
            self.last
        } else {
            self.segment
        }
    }
}

/// Reads any plaintext range of a sealed file through `F`, opening only the segments it covers.
pub struct SealedReader<F: BlockFile> {
    file: F,
    opener: FileOpener,
    align: Alignment,
    shape: Shape,
    buf: AlignedBuf,
}

impl<F: BlockFile> SealedReader<F> {
    /// The sealed file in `file`, its data key unwrapped by `parent`. Its header, footer, padding
    /// and length are checked here; each segment when it is read. `most` is the largest segment
    /// the caller writes: the header's segment size is authenticated only by segment 0, so a
    /// larger one is refused before its buffer is made, never allocated on a forged header's word.
    pub fn open(file: F, parent: &WrappingKey, most: u32) -> Result<Self, SealedFileError> {
        let align = file.alignment();
        let len = file.len()?;
        let a = as_u64(align.get())?;
        if len == 0 || len.checked_rem(a) != Some(0) {
            return Err(SealedFileError::Layout(
                "a length that is not a whole number of blocks",
            ));
        }
        // The header, in the first blocks.
        let header_span = align
            .up(HEADER)
            .ok_or(SealedFileError::Layout("a header past the alignment bound"))?;
        if as_u64(header_span)? > len {
            return Err(SealedFileError::Layout("a file shorter than its header"));
        }
        let mut probe = working_buffer(crate::stream::MIN_SEGMENT, align)?;
        let probe_bytes = probe
            .as_mut_capacity()
            .get_mut(..header_span)
            .ok_or(SealedFileError::Bound("a header past the probe"))?;
        file.read_exact_at(probe_bytes, 0)?;
        let header = Header::decode(
            probe_bytes
                .get(..HEADER)
                .ok_or(SealedFileError::Layout("a header cut short"))?,
        )?;
        if header.segment > most {
            return Err(SealedFileError::Layout(
                "a segment larger than the caller writes",
            ));
        }
        // The footer, and the padding before it: less than an alignment of zeros and the footer,
        // so within the last `up(alignment + footer)` bytes, or the whole file where it is shorter.
        let tail_span = align
            .get()
            .checked_add(FOOTER)
            .and_then(|span| align.up(span))
            .ok_or(SealedFileError::Layout("a tail past the alignment bound"))?
            .min(as_usize(len)?);
        let tail_at = len
            .checked_sub(as_u64(tail_span)?)
            .ok_or(SealedFileError::Layout("a tail before the file"))?;
        let tail = probe
            .as_mut_capacity()
            .get_mut(..tail_span)
            .ok_or(SealedFileError::Bound("a tail past the probe"))?;
        file.read_exact_at(tail, tail_at)?;
        let footer_at = tail_span
            .checked_sub(FOOTER)
            .ok_or(SealedFileError::Layout("a tail shorter than its footer"))?;
        let stated: [u8; FOOTER] = tail
            .get(footer_at..)
            .and_then(|f| f.try_into().ok())
            .ok_or(SealedFileError::Layout("a footer cut short"))?;
        let stream = u64::from_le_bytes(stated);
        let rounded = stream
            .checked_add(as_u64(FOOTER)?)
            .and_then(|end| align.up_u64(end));
        if rounded != Some(len) {
            return Err(SealedFileError::Layout(
                "a stated length that does not round to the file",
            ));
        }
        // Every byte between the stream and the footer is zero. The stream ends after the tail's
        // start: the gap is less than an alignment and the tail is at least one.
        let gap_from = as_usize(stream.checked_sub(tail_at).ok_or(SealedFileError::Layout(
            "a stream that ends before the file's tail",
        ))?)?;
        if tail
            .get(gap_from..footer_at)
            .is_none_or(|pad| pad.iter().any(|b| *b != 0))
        {
            return Err(SealedFileError::Layout("padding that is not zero"));
        }
        let shape = Shape::of(stream, header.segment)?;
        let opener = FileOpener::new(parent, &header)?;
        drop(probe);
        let buf = working_buffer(header.segment, align)?;
        Ok(Self {
            file,
            opener,
            align,
            shape,
            buf,
        })
    }

    /// The file's plaintext length.
    pub fn len(&self) -> u64 {
        self.shape.plain
    }

    /// Whether the file's plaintext is empty.
    pub fn is_empty(&self) -> bool {
        self.shape.plain == 0
    }

    /// Fills `out` with the plaintext from `offset`, opening each segment it covers. A range past
    /// the plaintext's end is refused, never read short.
    pub fn read_at(&mut self, out: &mut [u8], offset: u64) -> Result<(), SealedFileError> {
        let end = offset
            .checked_add(as_u64(out.len())?)
            .filter(|end| *end <= self.shape.plain)
            .ok_or(SealedFileError::Bound("a read past the plaintext"))?;
        if out.is_empty() {
            return Ok(());
        }
        let mut at = offset;
        let mut done = 0usize;
        while at < end {
            let index = at
                .checked_div(self.shape.segment)
                .ok_or(SealedFileError::Layout("a segment of no bytes"))?;
            let within = at
                .checked_rem(self.shape.segment)
                .ok_or(SealedFileError::Layout("a segment of no bytes"))?;
            let plain = self.open_segment(index)?;
            let from = as_usize(within)?;
            let take = plain
                .len()
                .checked_sub(from)
                .map(|left| left.min(out.len().saturating_sub(done)))
                .ok_or(SealedFileError::Layout("a read inside no segment"))?;
            let to = from
                .checked_add(take)
                .ok_or(SealedFileError::Bound("a read past the segment"))?;
            let dst_end = done
                .checked_add(take)
                .ok_or(SealedFileError::Bound("a read past its buffer"))?;
            out.get_mut(done..dst_end)
                .ok_or(SealedFileError::Bound("a read past its buffer"))?
                .copy_from_slice(
                    plain
                        .get(from..to)
                        .ok_or(SealedFileError::Bound("a read past the segment"))?,
                );
            done = dst_end;
            at = at
                .checked_add(as_u64(take)?)
                .ok_or(SealedFileError::Bound("a read past a u64"))?;
            if take == 0 {
                return Err(SealedFileError::Layout("a segment that yields nothing"));
            }
        }
        Ok(())
    }

    /// Reads and opens segment `index` into the buffer; returns its plaintext.
    fn open_segment(&mut self, index: u64) -> Result<&[u8], SealedFileError> {
        if index >= self.shape.count {
            return Err(SealedFileError::Bound("a segment past the file"));
        }
        let last = index.saturating_add(1) == self.shape.count;
        let len = as_usize(self.shape.len(index))?;
        let start = self
            .shape
            .offset(index)
            .ok_or(SealedFileError::Layout("a segment past a u64"))?;
        let stored_end = start
            .checked_add(as_u64(
                len.checked_add(TAG)
                    .ok_or(SealedFileError::Bound("a segment"))?,
            )?)
            .ok_or(SealedFileError::Layout("a segment past a u64"))?;
        let window_at = self.align.down_u64(start);
        let window_end = self
            .align
            .up_u64(stored_end)
            .ok_or(SealedFileError::Layout("a segment past the alignment"))?;
        let window = as_usize(
            window_end
                .checked_sub(window_at)
                .ok_or(SealedFileError::Layout("a window inverted"))?,
        )?;
        let capacity = self.buf.as_mut_capacity();
        let bytes = capacity
            .get_mut(..window)
            .ok_or(SealedFileError::Bound("a segment past the buffer"))?;
        self.file.read_exact_at(bytes, window_at)?;
        let skip = as_usize(
            start
                .checked_sub(window_at)
                .ok_or(SealedFileError::Layout("a window"))?,
        )?;
        let plain_end = skip
            .checked_add(len)
            .ok_or(SealedFileError::Bound("a segment past the buffer"))?;
        let tag_end = plain_end
            .checked_add(TAG)
            .ok_or(SealedFileError::Bound("a tag past the buffer"))?;
        let tag: [u8; TAG] = bytes
            .get(plain_end..tag_end)
            .and_then(|t| t.try_into().ok())
            .ok_or(SealedFileError::Layout("a tag cut short"))?;
        let plain = bytes
            .get_mut(skip..plain_end)
            .ok_or(SealedFileError::Bound("a segment past the buffer"))?;
        self.opener.open(index, last, plain, &tag)?;
        Ok(plain)
    }

    /// The file, given back.
    pub fn into_inner(self) -> F {
        self.file
    }
}

#[cfg(test)]
#[path = "sealed_file_tests.rs"]
mod tests;
