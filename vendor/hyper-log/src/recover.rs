//! Creating a log and opening one (docs/design/raft-log.md §6).

use std::collections::HashMap;

use hyper_block::block::BlockFile;
use hyper_block::buf::{AlignedBuf, Alignment, MAX_BUFFER};

use hyper_seal::log::FrameMac;

use crate::format::{self, FRAME_HEADER_BYTES, FrameHeader, MAC_LEN, Owned, SegmentHeader};
use crate::seal::Sealer;
use crate::state::{self, Live, Place, Replayed, Slot};
use crate::state::{Head, Segments, State};
use crate::{Config, LogError, Recovery};

/// What a frame's position holds.
pub(crate) enum Found<'a> {
    /// A verified frame: its header, its bytes and its padded length.
    Frame(FrameHeader, &'a [u8], u64),
    /// No frame of this segment: its frames end before here.
    End,
    /// A frame of this segment whose checksum fails, or that runs past the segment or file.
    Invalid,
}

/// Which segment a frame must name to be one of its frames.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Segment {
    pub(crate) log: u128,
    pub(crate) incarnation: u64,
    pub(crate) nonce: u64,
}

/// Reads a log file's frames through a window of one segment's bytes, the most any frame
/// takes. A frame the window holds is verified there, and the window moves to the start of one
/// it does not hold, which then lies wholly within it. A walk of a segment, or a search of its
/// blocks, then reads it in one read, or two where the walk began part way; each frame took a
/// read of its first block, another of the whole frame and a look at the file's length, and a
/// search read the segment a block at a time (audit P07).
pub(crate) struct Reader<'a, F> {
    file: &'a F,
    align: Alignment,
    /// The file's length when the reader was made: nothing writes the file while a reader
    /// walks it.
    len: u64,
    window: AlignedBuf,
    /// The file offset of the window's first byte, and the bytes it holds.
    start: u64,
    held: u64,
    /// A sealed log's framing MAC: every frame it reads is checked against it once its CRC holds.
    mac: Option<FrameMac>,
}

impl<'a, F: BlockFile> Reader<'a, F> {
    /// A reader of `file` whose window holds `segment_bytes`, checking a sealed log's frames
    /// against `mac`.
    pub(crate) fn new(
        file: &'a F,
        align: Alignment,
        segment_bytes: u64,
        mac: Option<FrameMac>,
    ) -> Result<Self, LogError> {
        let size = usize::try_from(segment_bytes).map_err(|_| LogError::Config("segment"))?;
        Ok(Self {
            file,
            align,
            len: file.len()?,
            window: AlignedBuf::zeroed(size, align).map_err(|e| LogError::Disk(e.into()))?,
            start: 0,
            held: 0,
            mac,
        })
    }

    /// File bytes `[at, at + n)`, `n` at most the window's size, moving the window to `at` when
    /// it does not hold them. `None` when they run past the file.
    fn bytes(&mut self, at: u64, n: u64) -> Result<Option<&[u8]>, LogError> {
        let Some(end) = at.checked_add(n).filter(|&e| e <= self.len) else {
            return Ok(None);
        };
        if at < self.start || end > self.start.saturating_add(self.held) {
            let capacity = u64::try_from(self.window.capacity()).unwrap_or(0);
            if n > capacity {
                return Ok(None);
            }
            // Whole blocks: the file's end may fall within one, past the last frame.
            let want = self
                .align
                .down_u64(capacity.min(self.len.saturating_sub(at)));
            self.held = 0;
            self.window
                .set_len(usize::try_from(want).unwrap_or(0))
                .map_err(|e| LogError::Disk(e.into()))?;
            self.file.read_exact_at(self.window.as_mut_slice(), at)?;
            self.start = at;
            self.held = want;
        }
        let from = usize::try_from(at.saturating_sub(self.start)).unwrap_or(usize::MAX);
        let to = usize::try_from(end.saturating_sub(self.start)).unwrap_or(usize::MAX);
        Ok(self.window.as_slice().get(from..to))
    }

    /// The header in the block at `offset`, if one decodes there, not yet verified.
    fn header(&mut self, offset: u64) -> Result<Option<FrameHeader>, LogError> {
        let block = block_of(self.align)?;
        Ok(self.bytes(offset, block)?.and_then(FrameHeader::decode))
    }

    /// The frame of `segment` at `offset`, reading no further than `end`, the segment's end.
    pub(crate) fn frame_at(
        &mut self,
        segment: Segment,
        offset: u64,
        end: u64,
    ) -> Result<Found<'_>, LogError> {
        let block = block_of(self.align)?;
        let Some(first_end) = offset.checked_add(block) else {
            return Ok(Found::End);
        };
        if first_end > end || first_end > self.len {
            return Ok(Found::End);
        }
        let Some(header) = self.header(offset)? else {
            return Ok(Found::End);
        };
        if header.log != segment.log
            || header.incarnation != segment.incarnation
            || header.nonce != segment.nonce
        {
            return Ok(Found::End);
        }
        if !self.ours(&header)? {
            return Ok(Found::End);
        }
        let Some(frame_len) = header.frame_len().and_then(|l| u64::try_from(l).ok()) else {
            return Ok(Found::Invalid);
        };
        let Some(padded) = self.align.up_u64(frame_len) else {
            return Ok(Found::Invalid);
        };
        let fits = offset
            .checked_add(padded)
            .is_some_and(|e| e <= end && e <= self.len);
        if !fits {
            return Ok(Found::Invalid);
        }
        let mac = self.mac.clone();
        match self.bytes(offset, padded)? {
            Some(bytes) if header.verifies(bytes) => {
                if let Some(mac) = &mac {
                    check_frame_mac(mac, &header, bytes)?;
                }
                Ok(Found::Frame(header, bytes, padded))
            }
            _ => Ok(Found::Invalid),
        }
    }

    /// Whether a frame of this log's ID is of its kind. A sealed log writes no frame of format 3:
    /// one of its own there was put there by someone else. An unsealed log reads a sealed frame as
    /// no frame of its own.
    fn ours(&self, header: &FrameHeader) -> Result<bool, LogError> {
        match (self.mac.is_some(), header.sealed) {
            (true, false) => Err(LogError::Tampered("an unsealed frame in a sealed log")),
            (false, true) => Ok(false),
            _ => Ok(true),
        }
    }

    /// Whether `segment` holds a valid frame after the block at `from`, up to `end`, with a
    /// sequence past `after`.
    fn later_frame(
        &mut self,
        segment: Segment,
        from: u64,
        end: u64,
        after: Option<u64>,
    ) -> Result<bool, LogError> {
        let block = block_of(self.align)?;
        let mut offset = from;
        loop {
            offset = match offset.checked_add(block) {
                Some(next) if next < end => next,
                _ => return Ok(false),
            };
            if let Found::Frame(header, ..) = self.frame_at(segment, offset, end)?
                && after.is_none_or(|seq| header.sequence > seq)
            {
                return Ok(true);
            }
        }
    }

    /// Whether a valid frame of log `log` from a segment newer than incarnation `above` lies
    /// in the blocks after the one at `first`, up to `end`, whatever segment header the slot
    /// holds.
    fn newer_frame(
        &mut self,
        log: u128,
        first: u64,
        end: u64,
        above: u64,
    ) -> Result<bool, LogError> {
        let block = block_of(self.align)?;
        let mut offset = first;
        loop {
            offset = match offset.checked_add(block) {
                Some(next)
                    if next < end && next.checked_add(block).is_some_and(|e| e <= self.len) =>
                {
                    next
                }
                _ => return Ok(false),
            };
            let Some(header) = self.header(offset)? else {
                continue;
            };
            if header.log != log || header.incarnation <= above {
                continue;
            }
            let segment = Segment {
                log,
                incarnation: header.incarnation,
                nonce: header.nonce,
            };
            if let Found::Frame(..) = self.frame_at(segment, offset, end)? {
                return Ok(true);
            }
        }
    }
}

/// Whether a persist record read whole is its log's: in a sealed log, sealed and its MAC holding
/// (checked once its CRC held, so a mismatch is tampering); in an unsealed one, unsealed.
fn check_record(
    record: &format::Persist,
    bytes: &[u8],
    sealer: Option<&Sealer>,
) -> Result<(), LogError> {
    match (sealer, record.sealed) {
        (Some(sealer), true) => {
            let len = record
                .encoded_len()
                .ok_or(LogError::Tampered("a persist record's length"))?;
            let end = len
                .checked_add(MAC_LEN)
                .ok_or(LogError::Tampered("a persist record's length"))?;
            let (Some(covered), Some(mac)) = (bytes.get(..len), bytes.get(len..end)) else {
                return Err(LogError::Tampered("a persist record cut short of its MAC"));
            };
            sealer.verify(covered, mac)
        }
        (Some(_), false) => Err(LogError::Tampered(
            "an unsealed persist record in a sealed log",
        )),
        (None, true) => Err(LogError::Foreign(
            "a sealed persist record in an unsealed log",
        )),
        (None, false) => Ok(()),
    }
}

/// Whether a sealed frame's MAC holds over its header and payload: checked once its CRC holds, so
/// a mismatch is tampering, never a torn write.
fn check_frame_mac(mac: &FrameMac, header: &FrameHeader, bytes: &[u8]) -> Result<(), LogError> {
    let at = header
        .mac_at()
        .ok_or(LogError::Tampered("a frame's length"))?;
    let end = at
        .checked_add(MAC_LEN)
        .ok_or(LogError::Tampered("a frame's length"))?;
    let (Some(covered), Some(tag)) = (bytes.get(..at), bytes.get(at..end)) else {
        return Err(LogError::Tampered("a frame cut short of its MAC"));
    };
    let expected = crate::seal::frame_mac(mac, covered, header.records)?;
    crate::seal::same_mac(&expected, tag)
}

fn block_of(align: Alignment) -> Result<u64, LogError> {
    u64::try_from(align.get()).map_err(|_| LogError::Config("block"))
}

pub(crate) fn check(config: &Config, align: Alignment, sealed: bool) -> Result<(), LogError> {
    let block = block_of(align)?;
    let blocks = config.segment_bytes.checked_div(block).unwrap_or(0);
    if !align.is_aligned_u64(config.segment_bytes) || blocks < 4 {
        return Err(LogError::Config(
            "a segment is a multiple of the alignment, four blocks at least",
        ));
    }
    // Recovery reads a segment through one window, and a frame may fill a segment.
    if config.segment_bytes > u64::try_from(MAX_BUFFER).unwrap_or(u64::MAX) {
        return Err(LogError::Config(
            "a segment fits one I/O buffer (hyper_block::buf::MAX_BUFFER)",
        ));
    }
    if u64::from(config.max_segments) < 3 {
        return Err(LogError::Config(
            "three segments at least: the head, its successor and one to reclaim into",
        ));
    }
    if config.max_groups == 0 || config.queue_submissions == 0 {
        return Err(LogError::Config(
            "room for one group and one submission at least",
        ));
    }
    let slot = persist_slot(config, align, sealed)?;
    if slot
        .checked_mul(2)
        .is_none_or(|area| area > config.segment_bytes)
    {
        return Err(LogError::Config(
            "a segment holds two persist records of the log's groups",
        ));
    }
    Ok(())
}

/// Bytes the file keeps before its first segment slot for persist records: one segment's
/// length, so that every frame lies at least that far from them (docs/design/raft-log.md §2).
pub(crate) fn persist_area(config: &Config) -> u64 {
    config.segment_bytes
}

/// Bytes of one persist slot: a record of the most groups a frame can carry, and a sealed log's
/// MAC after it, padded to the block. The record of the frame of sequence `s` goes in slot
/// `s mod 2`, so the record of the frame before it survives a torn write of this one.
pub(crate) fn persist_slot(
    config: &Config,
    align: Alignment,
    sealed: bool,
) -> Result<u64, LogError> {
    format::persist_len(config.max_groups)
        .and_then(|len| len.checked_add(if sealed { MAC_LEN } else { 0 }))
        .and_then(|len| u64::try_from(len).ok())
        .and_then(|len| align.up_u64(len))
        .ok_or(LogError::Config("persist records past u64"))
}

/// The file offset of the persist slot of the frame of `sequence`.
pub(crate) fn persist_at(slot: u64, sequence: u64) -> u64 {
    if sequence.is_multiple_of(2) { 0 } else { slot }
}

/// Writes `record` at `at`, the start of a persist slot, padded to the block, a sealed log's MAC
/// after it.
pub(crate) fn write_record<F: BlockFile>(
    file: &F,
    record: &format::Persist,
    at: u64,
    sealer: Option<&Sealer>,
) -> Result<(), LogError> {
    let mut bytes = record
        .encode()
        .ok_or(LogError::TooLarge(record.groups.len()))?;
    if let Some(sealer) = sealer {
        let mac = sealer.mac(&bytes)?;
        bytes.extend_from_slice(&mac);
    }
    let mut buf =
        AlignedBuf::zeroed(bytes.len(), file.alignment()).map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_from_slice(&bytes)
        .map_err(|e| LogError::Disk(e.into()))?;
    let padded = buf.padded().map_err(|e| LogError::Disk(e.into()))?;
    file.write_all_at(padded, at)?;
    Ok(())
}

/// A lost frame's persist record as copied into the other slot before its restore is written:
/// the slot's offset and the record (docs/design/raft-log.md §6).
type RecordCopy = (u64, format::Persist);

/// A group's state to write back as it was after a frame that no longer reads, which its
/// persist record describes, before the log serves anyone (docs/design/raft-log.md §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Restore {
    pub(crate) group: u128,
    pub(crate) update: crate::Update,
    pub(crate) uncertain: Option<format::Start>,
    /// The group is marked damaged; the update is empty.
    pub(crate) damaged: bool,
}

/// Writes segment 0, incarnation 1: its header and an empty first frame. A sealed log's header
/// carries its first session's key frame, and both end with their MACs.
pub(crate) fn create<F: BlockFile>(
    file: &F,
    config: &Config,
    id: u128,
    mut sealer: Option<&mut Sealer>,
) -> Result<State, LogError> {
    let align = file.alignment();
    check(config, align, sealer.is_some())?;
    if !file.is_empty()? {
        return Err(LogError::Foreign("a new log needs an empty file"));
    }
    let block = align.get();
    let nonce = random_nonce()?;
    let at = persist_area(config);
    let first_frame = at
        .checked_add(u64::try_from(block).map_err(|_| LogError::Config("block"))?)
        .ok_or(LogError::Config("frame"))?;
    let key = match sealer.as_deref_mut() {
        Some(sealer) => Some(sealer.begin(1, first_frame)?),
        None => None,
    };
    let header = SegmentHeader {
        log: id,
        incarnation: 1,
        nonce,
        segment_bytes: config.segment_bytes,
        key,
    };
    let frame = format::Frame {
        log: id,
        incarnation: 1,
        nonce,
        sequence: 0,
        tail: 1,
        records: 0,
        sealed: sealer.is_some(),
    };
    let mut frame = frame.header(&[]).ok_or(LogError::Config("frame"))?.to_vec();
    let mut header = header.encode();
    if let Some(sealer) = sealer.as_deref() {
        let mac = sealer.mac(&header)?;
        header.extend_from_slice(&mac);
        let mac = sealer.mac_frame(&frame, 0)?;
        frame.extend_from_slice(&mac);
    }
    if header.len() > block {
        return Err(LogError::Config("a segment header past its block"));
    }
    let total = block
        .checked_add(frame.len())
        .ok_or(LogError::Config("frame"))?;
    let mut buf = AlignedBuf::zeroed(total, align).map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_from_slice(&header)
        .map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_zeros(block.saturating_sub(buf.len()))
        .map_err(|e| LogError::Disk(e.into()))?;
    buf.extend_from_slice(&frame)
        .map_err(|e| LogError::Disk(e.into()))?;
    let bytes = buf.padded().map_err(|e| LogError::Disk(e.into()))?;
    let written = u64::try_from(bytes.len()).map_err(|_| LogError::Config("frame"))?;
    file.write_all_at(bytes, at)?;
    file.sync_data()?;
    Ok(State {
        groups: HashMap::new(),
        damaged: HashMap::new(),
        live: Live::with_slots(1),
        segments: Segments {
            incarnation: vec![1],
            nonce: vec![nonce],
            live: [0].into(),
            free: Default::default(),
        },
        head: Head {
            slot: 0,
            incarnation: 1,
            nonce,
            offset: at.checked_add(written).ok_or(LogError::Config("frame"))?,
        },
        next_sequence: 1,
        next_incarnation: 2,
        durable: 0,
        durable_tail: 1,
    })
}

pub(crate) fn random_nonce() -> Result<u64, LogError> {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).map_err(|_| LogError::Config("no randomness for a nonce"))?;
    Ok(u64::from_le_bytes(bytes))
}

/// Called with each verified frame of a segment: its offset, header, bytes and padded length.
type Visit<'a> = dyn FnMut(u64, &FrameHeader, &[u8], u64) -> Result<(), LogError> + 'a;

/// The last valid frame: its segment's incarnation, its offset, sequence and tail, and the
/// offset after it.
#[derive(Debug, Clone, Copy)]
struct Last {
    incarnation: u64,
    offset: u64,
    sequence: u64,
    tail: u64,
    after: u64,
}

/// The file as an open finds it: its block, its length, the persist area before its segment
/// slots, and how many slots it has.
#[derive(Debug, Clone, Copy)]
struct Shape {
    id: u128,
    block: u64,
    len: u64,
    segment: u64,
    area: u64,
    slots: u32,
}

impl Shape {
    fn of<F: BlockFile>(file: &F, config: &Config, id: u128) -> Result<Self, LogError> {
        let block = block_of(file.alignment())?;
        let len = file.len()?;
        if len == 0 {
            return Err(LogError::Foreign("the file is empty"));
        }
        let segment = config.segment_bytes;
        let area = persist_area(config);
        let slots = len
            .checked_sub(area)
            .filter(|&rest| rest > 0)
            .ok_or(LogError::Foreign("no segment past the persist area"))?
            .div_ceil(segment);
        let slots = u32::try_from(slots)
            .ok()
            .filter(|&s| s <= config.max_segments)
            .ok_or(LogError::Foreign("more segments than the log's quota"))?;
        Ok(Self {
            id,
            block,
            len,
            segment,
            area,
            slots,
        })
    }

    /// Where slot `slot` begins.
    fn start_of(&self, slot: u32) -> Result<u64, LogError> {
        u64::from(slot)
            .checked_mul(self.segment)
            .and_then(|s| s.checked_add(self.area))
            .ok_or(LogError::Damaged("an offset past u64"))
    }

    /// Where slot `slot` ends.
    fn end_of(&self, slot: u32) -> Result<u64, LogError> {
        self.start_of(slot)?
            .checked_add(self.segment)
            .ok_or(LogError::Damaged("an offset past u64"))
    }

    /// Where slot `slot`'s first frame goes, after its header block.
    fn first_frame(&self, slot: u32) -> Result<u64, LogError> {
        self.start_of(slot)?
            .checked_add(self.block)
            .ok_or(LogError::Damaged("an offset past u64"))
    }
}

/// Every segment header that reads as this log's: each slot's incarnation, 0 where none reads,
/// and nonce, which slot holds each incarnation, and the highest.
struct Headers {
    incarnation: Vec<u64>,
    nonce: Vec<u64>,
    by_incarnation: HashMap<u64, u32>,
    highest: u64,
}

impl Headers {
    /// The slot of incarnation `inc`, which must be live.
    fn slot(&self, inc: u64) -> Result<u32, LogError> {
        self.by_incarnation
            .get(&inc)
            .copied()
            .ok_or(LogError::Damaged("a live segment is missing"))
    }

    /// Which segment the frames of incarnation `inc` in `slot` name.
    fn segment(&self, id: u128, slot: u32, inc: u64) -> Segment {
        Segment {
            log: id,
            incarnation: inc,
            nonce: self
                .nonce
                .get(usize::try_from(slot).unwrap_or(usize::MAX))
                .copied()
                .unwrap_or(0),
        }
    }

    /// Whether slot `slot`'s header read as this log's.
    fn reads(&self, slot: u32) -> bool {
        usize::try_from(slot)
            .ok()
            .and_then(|i| self.incarnation.get(i))
            .is_some_and(|&inc| inc != 0)
    }
}

/// Step 1: every segment header. In a sealed log each header's MAC is checked once its CRC holds,
/// and the session it opened is known from its first frame on.
fn headers<F: BlockFile>(
    file: &F,
    shape: &Shape,
    mut sealer: Option<&mut Sealer>,
) -> Result<Headers, LogError> {
    let align = file.alignment();
    let count = usize::try_from(shape.slots).unwrap_or(0);
    let mut heads = Headers {
        incarnation: vec![0u64; count],
        nonce: vec![0u64; count],
        by_incarnation: HashMap::new(),
        highest: 0,
    };
    let mut block = AlignedBuf::zeroed(align.get(), align).map_err(|e| LogError::Disk(e.into()))?;
    block
        .set_len(align.get())
        .map_err(|e| LogError::Disk(e.into()))?;
    for slot in 0..shape.slots {
        let at = shape.start_of(slot)?;
        if at
            .checked_add(shape.block)
            .is_none_or(|end| end > shape.len)
        {
            continue;
        }
        file.read_exact_at(block.as_mut_slice(), at)?;
        let Some(h) = SegmentHeader::decode(block.as_slice()) else {
            continue;
        };
        if h.log != shape.id || h.segment_bytes != shape.segment || h.incarnation == 0 {
            continue;
        }
        match (sealer.as_deref_mut(), h.key) {
            (Some(sealer), Some(key)) => {
                let end = format::SEALED_SEGMENT_HEADER_LEN.saturating_add(MAC_LEN);
                let (Some(covered), Some(mac)) = (
                    block.as_slice().get(..format::SEALED_SEGMENT_HEADER_LEN),
                    block.as_slice().get(format::SEALED_SEGMENT_HEADER_LEN..end),
                ) else {
                    return Err(LogError::Tampered("a segment header cut short of its MAC"));
                };
                sealer.verify(covered, mac)?;
                sealer.found(h.incarnation, shape.first_frame(slot)?, &key)?;
            }
            (Some(_), None) => {
                return Err(LogError::Tampered(
                    "an unsealed segment header in a sealed log",
                ));
            }
            (None, Some(_)) => continue,
            (None, None) => {}
        }
        if heads.by_incarnation.insert(h.incarnation, slot).is_some() {
            return Err(LogError::Damaged("two segments share an incarnation"));
        }
        let i = usize::try_from(slot).unwrap_or(usize::MAX);
        if let (Some(e), Some(n)) = (heads.incarnation.get_mut(i), heads.nonce.get_mut(i)) {
            *e = h.incarnation;
            *n = h.nonce;
        }
    }
    heads.highest = heads
        .by_incarnation
        .keys()
        .copied()
        .max()
        .ok_or(LogError::Foreign("no segment of this log"))?;
    Ok(heads)
}

/// Walks the frames of the segment of incarnation `inc`, visiting each that verifies: where
/// they stop, and whether what stops them is a frame of the segment that does not verify.
fn frames_of<F: BlockFile>(
    reader: &mut Reader<'_, F>,
    shape: &Shape,
    heads: &Headers,
    inc: u64,
    visit: &mut Visit<'_>,
) -> Result<(u64, bool), LogError> {
    let slot = heads.slot(inc)?;
    let which = heads.segment(shape.id, slot, inc);
    let end = shape.end_of(slot)?;
    let mut offset = shape.first_frame(slot)?;
    loop {
        match reader.frame_at(which, offset, end)? {
            Found::Frame(header, bytes, padded) => {
                visit(offset, &header, bytes, padded)?;
                offset = offset
                    .checked_add(padded)
                    .ok_or(LogError::Damaged("an offset past u64"))?;
            }
            Found::End => return Ok((offset, false)),
            Found::Invalid => return Ok((offset, true)),
        }
    }
}

/// 2. The last valid frame, in the highest segment that holds one.
fn last_frame<F: BlockFile>(
    reader: &mut Reader<'_, F>,
    shape: &Shape,
    heads: &Headers,
) -> Result<Last, LogError> {
    let mut inc = heads.highest;
    loop {
        let mut found: Option<Last> = None;
        let (stop, _) = frames_of(
            reader,
            shape,
            heads,
            inc,
            &mut |offset, header, _, padded| {
                found = Some(Last {
                    incarnation: inc,
                    offset,
                    sequence: header.sequence,
                    tail: header.tail,
                    after: offset.saturating_add(padded),
                });
                Ok(())
            },
        )?;
        // A frame is written only after the one before it is flushed, so a valid frame past
        // the point where the frames stop proves the frame there was acknowledged, whatever
        // part of it no longer reads: its checksum, or its magic, format or identity
        // (06 §A3). Only with none past it is the stop the log's end.
        let slot = heads.slot(inc)?;
        let end = shape.end_of(slot)?;
        let after = found.map(|f| f.sequence);
        if reader.later_frame(heads.segment(shape.id, slot, inc), stop, end, after)? {
            return Err(LogError::Damaged("an acknowledged frame does not verify"));
        }
        if let Some(last) = found {
            return Ok(last);
        }
        inc = inc
            .checked_sub(1)
            .filter(|i| heads.by_incarnation.contains_key(i))
            .ok_or(LogError::Damaged("no segment holds a valid frame"))?;
    }
}

/// A slot whose header no longer reads as this log's may hold a newer segment whose header
/// was damaged. A segment's header is written with its first frame and flushed with it, so a
/// valid frame of this log past that first frame, in a segment newer than any whose header
/// reads, proves the header was durable and is now damaged. The first frame alone is an
/// opening that may never have been flushed, the torn tail's case.
fn lost_headers<F: BlockFile>(
    reader: &mut Reader<'_, F>,
    shape: &Shape,
    heads: &Headers,
) -> Result<(), LogError> {
    for slot in 0..shape.slots {
        if heads.reads(slot) {
            continue;
        }
        let first = shape.first_frame(slot)?;
        if reader.newer_frame(shape.id, first, shape.end_of(slot)?, heads.highest)? {
            return Err(LogError::Damaged(
                "a segment's header does not read, but its frames do",
            ));
        }
    }
    Ok(())
}

/// What replaying the live segments rebuilt: each group, the frames replayed, and the bytes
/// each live segment's frames take, padded, by slot: one entry for each of the log's
/// segments at most.
struct Replay {
    groups: HashMap<u128, Replayed>,
    frames: u64,
    used: HashMap<u32, u64>,
}

/// 3. Replays the live segments, the tail to the highest, in sequence order.
fn replay_live<F: BlockFile>(
    reader: &mut Reader<'_, F>,
    shape: &Shape,
    heads: &Headers,
    last: &Last,
    mut sealer: Option<&mut Sealer>,
) -> Result<Replay, LogError> {
    let mut replay = Replay {
        groups: HashMap::new(),
        frames: 0,
        used: HashMap::new(),
    };
    let mut expected: Option<u64> = None;
    for inc in last.tail..=heads.highest {
        let slot = heads.slot(inc)?;
        let mut held = 0u64;
        let (end, invalid) = frames_of(
            reader,
            shape,
            heads,
            inc,
            &mut |offset, header, bytes, padded| {
                if expected.is_some_and(|e| e != header.sequence) {
                    return Err(LogError::Damaged("a frame is missing"));
                }
                expected = header.sequence.checked_add(1);
                held = held.saturating_add(1);
                let at = Frame {
                    slot,
                    incarnation: inc,
                    offset,
                    padded,
                };
                replay_frame(&mut replay, at, header, bytes, sealer.as_deref_mut())
            },
        )?;
        // A segment's header is flushed with its first frame, so a live segment before the
        // last frame's holds a frame at least. With none, its frames were written over or
        // damaged where they begin: the torn opening of a later frame in its reused slot, where
        // the last frame, which freed it, no longer reads. Sequences are checked only from the
        // first frame replayed, so without this the segment's pieces vanished unreported
        // (mantle docs/design/raft-log.md §6, step 4).
        if held == 0 && inc < last.incarnation {
            return Err(LogError::Damaged("a live segment holds no frame"));
        }
        // Damage unless it lies after the last valid frame: then it is the torn tail.
        let before_last = inc < last.incarnation || (inc == last.incarnation && end < last.offset);
        if invalid && before_last {
            return Err(LogError::Damaged("an acknowledged frame does not verify"));
        }
    }
    if expected != last.sequence.checked_add(1) {
        return Err(LogError::Damaged("a frame is missing"));
    }
    Ok(replay)
}

/// Where a frame being replayed lies: its segment's slot and incarnation, its offset, and its
/// padded length.
#[derive(Debug, Clone, Copy)]
struct Frame {
    slot: u32,
    incarnation: u64,
    offset: u64,
    padded: u64,
}

/// Replays one verified frame. In a sealed log its key records are known first, in order, then
/// its proposals are opened, which the state keeps, and its entries' lengths are their
/// plaintexts': the log reads an entry's bytes back when asked, and opens them then.
fn replay_frame(
    replay: &mut Replay,
    at: Frame,
    header: &FrameHeader,
    bytes: &[u8],
    sealer: Option<&mut Sealer>,
) -> Result<(), LogError> {
    replay.frames = replay.frames.saturating_add(1);
    let payload = bytes
        .get(format::FRAME_HEADER_LEN..header.mac_at().unwrap_or(0))
        .ok_or(LogError::Damaged("a frame shorter than its header says"))?;
    let mut records = format::records(payload, header.records)
        .ok_or(LogError::Damaged("a verified frame does not decode"))?;
    let base = at
        .offset
        .checked_add(FRAME_HEADER_BYTES)
        .ok_or(LogError::Damaged("an offset past u64"))?;
    let used = replay.used.entry(at.slot).or_insert(0);
    *used = used.saturating_add(at.padded);
    let tag = match sealer {
        Some(sealer) => {
            open_records(sealer, at.incarnation, base, &mut records)?;
            format::TAG_LEN
        }
        None => 0,
    };
    replay_records(&mut replay.groups, at.slot, base, records, tag)
}

/// A sealed frame's records made plain where the state keeps them: each key record's session known
/// from its offset on, then each proposal opened.
fn open_records(
    sealer: &mut Sealer,
    incarnation: u64,
    base: u64,
    records: &mut [Owned],
) -> Result<(), LogError> {
    let offset = |at: usize, past: usize| {
        u64::try_from(at.checked_add(past)?)
            .ok()
            .and_then(|at| base.checked_add(at))
    };
    for record in records.iter_mut() {
        match record {
            Owned::Key { at, frame } => {
                let from = offset(*at, 0).ok_or(LogError::Damaged("an offset past u64"))?;
                sealer.found(incarnation, from, frame)?;
            }
            Owned::Proposal { group, proposal } => {
                let from = offset(proposal.at, format::PROPOSAL_FIELDS_LEN)
                    .ok_or(LogError::Damaged("an offset past u64"))?;
                proposal.bytes = sealer.open(
                    incarnation,
                    from,
                    *group,
                    proposal.index,
                    proposal.term,
                    &proposal.bytes,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// The groups replay rebuilt, as they run: those whose records run unbroken, those with a
/// record missing, which recover from their peers, and those an earlier recovery fenced as
/// damaged, with where the fence is.
type Finished = (HashMap<u128, state::Group>, Vec<u128>, HashMap<u128, Place>);

/// 4. The groups as they run.
fn finish(replayed: HashMap<u128, Replayed>) -> Finished {
    let mut groups = HashMap::new();
    let mut damaged = Vec::new();
    let mut fenced = HashMap::new();
    for (group, r) in replayed {
        if let Some(at) = r.damaged {
            fenced.insert(group, at);
            continue;
        }
        match r.finish() {
            Some(g) => {
                groups.insert(group, g);
            }
            None => damaged.push(group),
        }
    }
    (groups, damaged, fenced)
}

/// Each segment slot's live pieces and bytes, and the bytes its frames take.
fn live_of(
    slots: usize,
    groups: &HashMap<u128, state::Group>,
    fenced: &HashMap<u128, Place>,
    used: &HashMap<u32, u64>,
) -> Live {
    let mut live = Live::with_slots(slots);
    for g in groups.values() {
        for (place, bytes) in g.pieces() {
            live.add(place, bytes);
        }
    }
    for &place in fenced.values() {
        live.add(place, state::DAMAGED_BYTES);
    }
    for (&slot, &bytes) in used {
        live.wrote(slot, bytes);
    }
    live
}

/// Step 5: the block where the next frame goes is erased; returns its offset. A frame there was never
/// acknowledged but may be partly durable, its header whole while its last sectors, or the
/// file's end, are not; left alone, a later crash could complete it with the zeros of a newer
/// frame's padding and bring it back. Erased, it can neither return nor be taken for damage once
/// frames follow it elsewhere.
///
/// The restore's frame takes the lost frame's sequence, so its persist record goes in the lost
/// frame's slot, before its flush, and may tear there while the frame does not become durable.
/// The lost frame's record, and whether it was confirmed, are copied to the other slot first
/// and flushed with the erasure (`copy`, when there is a restore): what restores the lost frame
/// then survives until the restore itself is durable. The other slot holds nothing recovery
/// still needs: the record of the frame before, which reads whole, or of the frame after, which
/// never became durable and so was never answered, and whose confirmation of the lost frame the
/// copy carries.
fn erase<F: BlockFile>(
    file: &F,
    shape: &Shape,
    heads: &Headers,
    last: &Last,
    copy: Option<RecordCopy>,
    sealer: Option<&Sealer>,
) -> Result<u64, LogError> {
    let mut flush = false;
    if let Some((at, record)) = copy {
        write_record(file, &record, at, sealer)?;
        flush = true;
    }
    let head_slot = heads.slot(heads.highest)?;
    let head_offset = if heads.highest == last.incarnation {
        last.after
    } else {
        shape.first_frame(head_slot)?
    };
    // Where the head is a newer segment than the last frame's, an opening whose frame never
    // became durable, the block after the last frame is erased as well. A frame there was
    // flushed before that opening was written, and no longer reads: the lost frame. Left in
    // the tail, a sweep of that segment took it for a live frame damaged, and the restore's
    // own frame fenced the log (mantle docs/design/raft-log.md §6, step 6).
    let mut erasures = vec![(head_offset, shape.end_of(head_slot)?)];
    if heads.highest != last.incarnation {
        erasures.push((last.after, shape.end_of(heads.slot(last.incarnation)?)?));
    }
    let align = file.alignment();
    let mut zeros = AlignedBuf::zeroed(align.get(), align).map_err(|e| LogError::Disk(e.into()))?;
    zeros
        .set_len(align.get())
        .map_err(|e| LogError::Disk(e.into()))?;
    for (at, end) in erasures {
        if at.checked_add(shape.block).is_some_and(|stop| stop <= end) {
            file.write_all_at(zeros.as_slice(), at)?;
            flush = true;
        }
    }
    if flush {
        file.sync_data()?;
    }
    Ok(head_offset)
}

pub(crate) fn open<F: BlockFile>(
    file: &F,
    config: &Config,
    id: u128,
    mut sealer: Option<&mut Sealer>,
) -> Result<(State, Recovery, Vec<Restore>), LogError> {
    check(config, file.alignment(), sealer.is_some())?;
    let shape = Shape::of(file, config, id)?;
    let heads = headers(file, &shape, sealer.as_deref_mut())?;
    let mac = sealer.as_deref().map(Sealer::frame_mac);
    let mut reader = Reader::new(file, file.alignment(), shape.segment, mac)?;
    let last = last_frame(&mut reader, &shape, &heads)?;
    lost_headers(&mut reader, &shape, &heads)?;
    if last.tail > last.incarnation {
        return Err(LogError::Damaged(
            "a frame names a tail after its own segment",
        ));
    }
    let replay = replay_live(&mut reader, &shape, &heads, &last, sealer.as_deref_mut())?;
    let (groups, mut damaged, fenced) = finish(replay.groups);
    // 4b. A frame after the last valid one may have been flushed and damaged since: its
    // persist record, written in the same flush, says what it held (AGL+18 §3.3.3).
    let (mut restores, copy) = restores(
        file,
        config,
        id,
        last.sequence,
        (&groups, &fenced),
        &mut damaged,
        sealer.as_deref(),
    )?;
    damaged.sort_unstable();
    damaged.dedup();
    // A group newly found damaged is fenced by a record written before the log serves
    // anyone, so that the fence outlives what showed the damage: the frame is overwritten
    // by the next, and a record missing may be swept away (mantle audit S16).
    restores.extend(damaged.iter().map(|&group| Restore {
        group,
        update: crate::Update::default(),
        uncertain: None,
        damaged: true,
    }));
    let live = live_of(heads.incarnation.len(), &groups, &fenced, &replay.used);
    let copy = copy.filter(|_| !restores.is_empty());
    let head_offset = erase(file, &shape, &heads, &last, copy, sealer.as_deref())?;
    let opened = Opened {
        groups,
        damaged,
        fenced,
        live,
        head_offset,
        frames: replay.frames,
    };
    let (state, recovery) = opened.into_state(heads, &last, shape.slots, &restores)?;
    Ok((state, recovery, restores))
}

/// What an open found, before it is the log's state.
struct Opened {
    groups: HashMap<u128, state::Group>,
    damaged: Vec<u128>,
    fenced: HashMap<u128, Place>,
    live: Live,
    head_offset: u64,
    frames: u64,
}

impl Opened {
    /// The log's state, its head after the last frame, and what the open reports.
    fn into_state(
        self,
        heads: Headers,
        last: &Last,
        slots: u32,
        restores: &[Restore],
    ) -> Result<(State, Recovery), LogError> {
        let highest = heads.highest;
        let head_slot = heads.slot(highest)?;
        let head_nonce = heads.segment(0, head_slot, highest).nonce;
        let live_segments = (last.tail..=highest)
            .filter_map(|inc| heads.by_incarnation.get(&inc).copied())
            .collect();
        let free = (0..slots)
            .filter(|slot| {
                let inc = heads
                    .incarnation
                    .get(usize::try_from(*slot).unwrap_or(usize::MAX))
                    .copied()
                    .unwrap_or(0);
                inc < last.tail
            })
            .map(|slot| (slot, 0))
            .collect();
        let restored = restores
            .iter()
            .filter(|r| !r.damaged)
            .map(|r| r.group)
            .collect();
        let state = State {
            damaged: self
                .damaged
                .iter()
                .map(|&group| (group, None))
                .chain(self.fenced.iter().map(|(&group, &at)| (group, Some(at))))
                .collect(),
            groups: self.groups,
            live: self.live,
            segments: Segments {
                incarnation: heads.incarnation,
                nonce: heads.nonce,
                live: live_segments,
                free,
            },
            head: Head {
                slot: head_slot,
                incarnation: highest,
                nonce: head_nonce,
                offset: self.head_offset,
            },
            next_sequence: last
                .sequence
                .checked_add(1)
                .ok_or(LogError::Damaged("sequences past u64"))?,
            next_incarnation: highest
                .checked_add(1)
                .ok_or(LogError::Damaged("incarnations past u64"))?,
            durable: last.sequence,
            durable_tail: last.tail,
        };
        let mut all: Vec<u128> = self
            .damaged
            .into_iter()
            .chain(self.fenced.into_keys())
            .collect();
        all.sort_unstable();
        let recovery = Recovery {
            frames: self.frames,
            damaged: all,
            restored,
        };
        Ok((state, recovery))
    }
}

/// What the frame after the last valid one held, from its persist record, as the updates that
/// restore it. A persist record is written in its frame's flush, so it may survive a frame
/// torn by a crash as well as one damaged since its flush; what tells the two apart is a
/// confirmation that the frame was flushed: the next frame's record, which is written only
/// after that flush, or the one the writer writes when no frame follows at once. A frame's
/// updates are answered only once it is confirmed.
///
/// A confirmed frame was acknowledged, and damaged since (AGL+18 §3.3.3). Each of its groups
/// gets back the start and hard state the frame left, its entries cut back to where the
/// frame's began, and, where the frame wrote entries, the mark that the log may lack them; a
/// group whose frame held proposals, which a persist record does not carry, is damaged and
/// recovers from its peers. An unconfirmed frame was never acknowledged, and is the torn tail,
/// but for its term and vote, which are kept, since raising a term or keeping a vote is always
/// safe. Its commit is not kept, since it may name an entry only the frame held. A fence the
/// frame put on a damaged group is kept, confirmed or not.
///
/// The frame's record is the one in its own slot or, where that one no longer reads, the copy
/// in the other slot that an open restoring the frame made before the restore's frame took the
/// lost frame's sequence, and so its slot (§6). Where the restore's frame tore after its record
/// landed, that record is the one read: it holds the restore, and restoring from it leaves
/// each group as restoring from the lost frame's record does. Also returns the copy to make
/// before a restore is written, when the record read is in its own slot: where the copy goes,
/// and the record saying whether the frame was confirmed.
fn restores<F: BlockFile>(
    file: &F,
    config: &Config,
    id: u128,
    last: u64,
    (groups, fenced): (&HashMap<u128, state::Group>, &HashMap<u128, Place>),
    damaged: &mut Vec<u128>,
    sealer: Option<&Sealer>,
) -> Result<(Vec<Restore>, Option<RecordCopy>), LogError> {
    let Some(sequence) = last.checked_add(1) else {
        return Ok((Vec::new(), None));
    };
    let align = file.alignment();
    let slot = persist_slot(config, align, sealer.is_some())?;
    let size = usize::try_from(slot).map_err(|_| LogError::Config("a persist slot"))?;
    let mut records = Vec::with_capacity(2);
    for at in [0, slot] {
        let mut bytes = AlignedBuf::zeroed(size, align).map_err(|e| LogError::Disk(e.into()))?;
        bytes.set_len(size).map_err(|e| LogError::Disk(e.into()))?;
        file.read_exact_at(bytes.as_mut_slice(), at)?;
        if let Some(p) = format::Persist::decode(bytes.as_slice()).filter(|p| p.log == id) {
            check_record(&p, bytes.as_slice(), sealer)?;
            records.push((at, p));
        }
    }
    let confirmed = records.iter().any(|(_, p)| p.confirms >= sequence);
    let own = persist_at(slot, sequence);
    let other = if own == 0 { slot } else { 0 };
    let chosen = [own, other].into_iter().find_map(|from| {
        records
            .iter()
            .find(|(at, p)| *at == from && p.sequence == sequence)
            .map(|(_, p)| (from, p.clone()))
    });
    let Some((from, record)) = chosen else {
        return Ok((Vec::new(), None));
    };
    let copy = (from == own).then(|| {
        (
            other,
            format::Persist {
                confirms: if confirmed { sequence } else { record.confirms },
                ..record.clone()
            },
        )
    });
    let empty = state::Group::default();
    let mut out = Vec::new();
    for p in record.groups {
        if damaged.contains(&p.group) || (fenced.contains_key(&p.group) && !p.removed) {
            continue;
        }
        // A fence the frame wrote is kept whether or not the frame was confirmed: marking a
        // group damaged is always safe, and the frame it fenced may be gone.
        if p.damaged || (confirmed && p.proposals) {
            damaged.push(p.group);
            continue;
        }
        let g = groups.get(&p.group).unwrap_or(&empty);
        let restore = if confirmed {
            restore_confirmed(&p, g)?
        } else {
            restore_torn(&p, g)
        };
        out.extend(restore);
    }
    Ok((out, copy))
}

/// An unconfirmed frame's group: the torn tail, keeping only a later term or a vote given, at
/// the group's own commit.
fn restore_torn(p: &format::Persisted, g: &state::Group) -> Option<Restore> {
    let h = p.hard_state?;
    let current = g.hard.map(|(h, _)| h);
    let later = current.is_none_or(|c| h.term > c.term || (h.term == c.term && c.vote == 0));
    later.then(|| Restore {
        group: p.group,
        update: crate::Update {
            hard_state: Some(format::HardState {
                commit: current.map_or(0, |c| c.commit),
                ..h
            }),
            ..crate::Update::default()
        },
        uncertain: None,
        damaged: false,
    })
}

/// A confirmed frame's group, the frame damaged since it was acknowledged: its removal, or the
/// start and hard state the frame left, its entries cut back to where the frame's began, and,
/// where the frame wrote entries, the mark that the log may lack them. `None` where that
/// changes nothing.
fn restore_confirmed(p: &format::Persisted, g: &state::Group) -> Result<Option<Restore>, LogError> {
    if p.removed {
        return Ok(Some(Restore {
            group: p.group,
            update: crate::Update {
                remove: true,
                ..crate::Update::default()
            },
            uncertain: None,
            damaged: false,
        }));
    }
    let current = g.hard.map(|(h, _)| h);
    let mut update = crate::Update::default();
    let mut last_index = g.last().ok_or(LogError::Damaged("an index past u64"))?;
    if let Some(start) = p.start.filter(|s| s.index >= g.start.index) {
        update.start = Some(start);
        last_index = last_index.max(start.index);
    }
    let mut uncertain = g.uncertain.map(|(mark, _)| mark);
    if let Some(w) = p.entries {
        let from = w.first.max(
            update
                .start
                .map_or(g.start.index, |s| s.index)
                .saturating_add(1),
        );
        update.entries = Some(crate::Entries {
            first: from,
            entries: Vec::new(),
        });
        last_index = last_index.min(from.saturating_sub(1));
        uncertain = lacking(uncertain, w)?;
    }
    if let Some(mark) = p.uncertain {
        uncertain = Some(merge(uncertain, mark));
    }
    let newer = p
        .hard_state
        .filter(|h| current.is_none_or(|c| h.term >= c.term));
    // A commit past the entries the log still holds would name entries it lacks; the
    // leader tells the replica its commit again.
    let hard = newer.or(current).map(|h| format::HardState {
        commit: h.commit.min(last_index),
        ..h
    });
    if hard != current {
        update.hard_state = hard;
    }
    if update == crate::Update::default() && uncertain == g.uncertain.map(|(m, _)| m) {
        return Ok(None);
    }
    Ok(Some(Restore {
        group: p.group,
        update,
        uncertain,
        damaged: false,
    }))
}

/// The mark `mark` widened to the entries `w` a lost frame wrote, if it wrote any.
fn lacking(
    mark: Option<format::Start>,
    w: format::Written,
) -> Result<Option<format::Start>, LogError> {
    if w.count == 0 {
        return Ok(mark);
    }
    let lost = format::Start {
        index: w
            .first
            .checked_add(w.count)
            .and_then(|end| end.checked_sub(1))
            .ok_or(LogError::Damaged("an index past u64"))?,
        term: w.term,
    };
    Ok(Some(merge(mark, lost)))
}

/// A mark covering both marks.
fn merge(mark: Option<format::Start>, other: format::Start) -> format::Start {
    match mark {
        Some(m) => format::Start {
            index: m.index.max(other.index),
            term: m.term.max(other.term),
        },
        None => other,
    }
}

/// Applies one frame's records, in order, to the groups being rebuilt. `base` is the file
/// offset of the frame's payload in segment `slot`.
fn replay_records(
    groups: &mut HashMap<u128, Replayed>,
    slot: u32,
    base: u64,
    records: Vec<Owned>,
    tag: usize,
) -> Result<(), LogError> {
    let place = |at: usize| -> Result<Place, LogError> {
        Ok(Place {
            slot,
            offset: u64::try_from(at)
                .ok()
                .and_then(|at| base.checked_add(at))
                .ok_or(LogError::Damaged("an offset past u64"))?,
        })
    };
    for record in records {
        match record {
            Owned::Entries {
                group,
                first,
                entries,
            } => {
                let g = groups.entry(group).or_default();
                g.truncate(first);
                let held = u64::try_from(entries.len()).unwrap_or(u64::MAX);
                let last = first
                    .checked_add(held)
                    .and_then(|end| end.checked_sub(1))
                    .ok_or(LogError::Damaged("an index past u64"))?;
                insert(g, entries, &place, tag)?;
                g.reach(last);
            }
            Owned::Relocated { group, entries, .. } => {
                insert(groups.entry(group).or_default(), entries, &place, tag)?;
            }
            Owned::HardState { at, group, state } => {
                groups.entry(group).or_default().hard = Some((state, place(at)?));
            }
            Owned::Start { at, group, start } => {
                let g = groups.entry(group).or_default();
                g.start = start;
                g.start_at = Some(place(at)?);
                let last = g.last.max(start.index);
                g.reach(last);
            }
            // A sealed log's session key: what opens the records after it, not a group's piece.
            Owned::Key { .. } => {}
            Owned::Proposal { group, proposal } => {
                groups.entry(group).or_default().proposals.insert(
                    proposal.index,
                    state::Proposal {
                        term: proposal.term,
                        place: place(proposal.at)?,
                        bytes: proposal.bytes,
                    },
                );
            }
            Owned::Removed { group } => {
                groups.remove(&group);
            }
            Owned::Uncertain { at, group, mark } => {
                groups.entry(group).or_default().uncertain = Some((mark, place(at)?));
            }
            Owned::Released { at, group, through } => {
                groups
                    .entry(group)
                    .or_default()
                    .release(through, place(at)?);
            }
            Owned::Damaged { at, group } => {
                *groups.entry(group).or_default() = Replayed {
                    damaged: Some(place(at)?),
                    ..Replayed::default()
                };
            }
        }
    }
    Ok(())
}

fn insert(
    g: &mut Replayed,
    entries: Vec<format::Decoded>,
    place: &impl Fn(usize) -> Result<Place, LogError>,
    tag: usize,
) -> Result<(), LogError> {
    for e in entries {
        let plain = e
            .bytes
            .len()
            .checked_sub(tag)
            .ok_or(LogError::Damaged("a sealed entry shorter than its tag"))?;
        let len = u32::try_from(plain).map_err(|_| LogError::Damaged("an entry past u32"))?;
        g.entries.insert(
            e.index,
            Slot {
                term: e.term,
                place: place(e.at)?,
                len,
                cached: None,
            },
        );
    }
    Ok(())
}
