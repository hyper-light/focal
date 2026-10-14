//! A native session's cold streams (doc 31 M2, A2). Every replica appends the batch of each
//! version-2 retirement record it applies to its data stream, and a mark to its marks stream:
//! the record's sequence and where its batch begins. The records decide what enters, so every
//! replica's streams hold the same bytes at the same offsets. A retired family is read back
//! through its stub's sequence: the marks find the batch, the batch's header the frame.
use focal_core::native::retirement::{COLD_COUNT_BYTES, COLD_ENTRY_BYTES, ColdEntry, cold_header};
use focal_evidence::{
    ColdGeometry, ColdReader, ColdRoom, ColdState, ColdStore, ColdStreams, ContentError,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, DiskBudget, MemoryBudget};
use focal_model::{ContentHash, SessionSeq};
use std::path::{Path, PathBuf};

/// A cold stream's failure, as the session reports a store's.
pub(crate) fn cold_error(error: ContentError) -> super::NativeSessionError {
    super::NativeSessionError::Native(focal_evidence::NativeEvidenceError::from(error).into())
}

/// The bytes of one mark: a record's sequence and its batch's offset (u64 each, little-endian).
const MARK_BYTES: usize = 16;

/// Where a session's cold streams live, the volume they are promised from, and how fresh ones
/// are cut ([`ColdGeometry::STANDARD`] outside tests).
#[derive(Clone)]
pub struct ColdSource {
    pub root: PathBuf,
    pub disk: DiskBudget,
    pub geometry: ColdGeometry,
}

/// What a cold family's frame names of the ledger's content (doc 31 M2, A2): the content roots of
/// its artifacts held as objects, and those of its inline artifacts' sealed objects, which custody
/// keeps for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdContent {
    pub content: Vec<ContentHash>,
    pub inline: Vec<ContentHash>,
}

/// A retired family's frame read from a cold stream, charged while it is held.
#[derive(Debug)]
pub struct ColdFrame {
    bytes: Vec<u8>,
    _charge: Allocation,
}
impl ColdFrame {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// What one batch and its mark take of the streams, promised before the record applies
/// ([`NativeCold::room`]).
pub(crate) struct NativeColdRoom {
    data: ColdRoom,
    marks: ColdRoom,
}

/// The most bytes a batch's header takes: its count, and an entry for each of the most families
/// one record retires. A header naming more is refused unread.
const MAX_HEADER: usize =
    COLD_COUNT_BYTES + focal_core::native::retirement_batch::MAX_RETIRE_FAMILIES * COLD_ENTRY_BYTES;

/// Where the frame of digest `digest` lies in the batch of the record at `sequence`, and how long
/// it is: `None` when no batch was appended at `sequence`, refused when the batch holds no such
/// frame. The marks (`marks_length` bytes of them) are searched, then the batch's header read,
/// through `data` and `marks`, each a stream's bytes at an offset; what they read is at most a
/// mark or a header at a time ([`MAX_HEADER`]).
fn locate(
    marks_length: u64,
    data: &dyn Fn(u64, usize) -> Result<Vec<u8>, ContentError>,
    marks: &dyn Fn(u64, usize) -> Result<Vec<u8>, ContentError>,
    sequence: SessionSeq,
    digest: ContentHash,
) -> Result<Option<(u64, usize)>, ContentError> {
    let size = u64::try_from(MARK_BYTES).map_err(|_| ContentError::Capacity)?;
    let mut low = 0u64;
    let mut high = marks_length
        .checked_div(size)
        .ok_or(ContentError::Capacity)?;
    let mut offset = None;
    // At most 64 probes: the marks are fewer than 2^64.
    for _ in 0..u64::BITS {
        if low >= high {
            break;
        }
        let middle = low.saturating_add(high.saturating_sub(low).checked_div(2).unwrap_or(0));
        let at = middle.checked_mul(size).ok_or(ContentError::Capacity)?;
        let entry = marks(at, MARK_BYTES)?;
        match word(&entry, 0)?.cmp(&sequence.0) {
            std::cmp::Ordering::Equal => {
                offset = Some(word(&entry, 8)?);
                break;
            }
            std::cmp::Ordering::Less => low = middle.saturating_add(1),
            std::cmp::Ordering::Greater => high = middle,
        }
    }
    let Some(offset) = offset else {
        return Ok(None);
    };
    let count = data(offset, COLD_COUNT_BYTES)?;
    let entries = usize::from(u16::from_le_bytes(
        count
            .as_slice()
            .try_into()
            .map_err(|_| ContentError::Corrupt)?,
    ));
    let header = entries
        .checked_mul(COLD_ENTRY_BYTES)
        .and_then(|n| n.checked_add(COLD_COUNT_BYTES))
        .ok_or(ContentError::Capacity)?;
    if header > MAX_HEADER {
        return Err(ContentError::Corrupt);
    }
    let (entries, frames): (Vec<ColdEntry>, usize) =
        cold_header(&data(offset, header)?).map_err(|_| ContentError::Corrupt)?;
    let mut at = offset
        .checked_add(u64::try_from(frames).map_err(|_| ContentError::Capacity)?)
        .ok_or(ContentError::Capacity)?;
    for entry in entries {
        if entry.digest == digest {
            let length = usize::try_from(entry.length).map_err(|_| ContentError::Capacity)?;
            return Ok(Some((at, length)));
        }
        at = at
            .checked_add(u64::from(entry.length))
            .ok_or(ContentError::Capacity)?;
    }
    Err(ContentError::Corrupt)
}

/// What looking for a frame holds at most, beside the frame: a whole chunk and a manifest chunk
/// read at a time, a header as read and as its entries.
fn locating(geometry: ColdGeometry) -> Result<usize, ContentError> {
    geometry.read_bytes(MAX_HEADER.saturating_mul(2))
}

/// Read the frame at `at`, `length` bytes, through `read`, charged to `charge` (which holds what
/// finding it took): grown to what the read holds while it reads, then to the frame's bytes.
fn read_frame(
    geometry: ColdGeometry,
    mut charge: Allocation,
    read: &dyn Fn(u64, usize) -> Result<Vec<u8>, ContentError>,
    at: u64,
    length: usize,
) -> Result<ColdFrame, ContentError> {
    charge
        .grow(geometry.read_bytes(length)?.saturating_sub(charge.bytes()))
        .map_err(|_| ContentError::Capacity)?;
    let bytes = read(at, length)?;
    charge
        .shrink_to(bytes.capacity())
        .map_err(|_| ContentError::Capacity)?;
    Ok(ColdFrame {
        bytes,
        _charge: charge,
    })
}

/// The frame of digest `digest` in the batch of the record at `sequence`, read from `streams`
/// as a checkpoint names them and from whole chunks the readers supply, the data stream's and the
/// marks': what a backup reads a cold family from, with no writer. `None` when no batch was
/// appended at `sequence`. What it reads is charged to `budget` before it is read, and the frame
/// while it is held.
pub fn frame_in(
    streams: &ColdStreams,
    data: &dyn Fn(ContentHash, usize) -> Result<Vec<u8>, ContentError>,
    marks: &dyn Fn(ContentHash, usize) -> Result<Vec<u8>, ContentError>,
    sequence: SessionSeq,
    digest: ContentHash,
    budget: &MemoryBudget,
) -> Result<Option<ColdFrame>, ContentError> {
    let geometry = streams.geometry;
    let charge = budget
        .reserve(BudgetKind::Query, BudgetLane::Ordinary, locating(geometry)?)
        .map_err(|_| ContentError::Capacity)?
        .commit();
    let in_data = |offset: u64, length: usize| streams.data.read(geometry, offset, length, data);
    let in_marks = |offset: u64, length: usize| streams.marks.read(geometry, offset, length, marks);
    let Some((at, length)) = locate(streams.marks.length, &in_data, &in_marks, sequence, digest)?
    else {
        return Ok(None);
    };
    read_frame(geometry, charge, &in_data, at, length).map(Some)
}

/// A session's cold streams as its engine holds them: kept or not, and open once used.
pub(crate) enum ColdSlot {
    /// This replica keeps no cold streams.
    None,
    /// Kept, and opened at their first use: as a checkpoint named them, or fresh. An engine built
    /// beside the one it replaces (an install) never takes the directory while the other has it.
    Closed(Option<Box<ColdStreams>>),
    /// Boxed: a writer holds two stream hashers, some 4 KiB, which an engine need not carry
    /// inline.
    Open(Box<NativeCold>),
}
impl ColdSlot {
    pub(crate) fn is_kept(&self) -> bool {
        !matches!(self, Self::None)
    }
    /// The writer, opened now if it was not: none when this replica keeps no streams. An open
    /// refused (the directory still held, no room) leaves the streams as they were named, closed.
    pub(crate) fn writer(
        &mut self,
        source: Option<&ColdSource>,
        memory: &MemoryBudget,
    ) -> Result<Option<&mut NativeCold>, ContentError> {
        if let Self::Closed(streams) = self {
            let source = source.ok_or(ContentError::Invalid)?;
            let cold = NativeCold::open(source, memory, streams.as_deref())?;
            *self = Self::Open(Box::new(cold));
        }
        Ok(match self {
            Self::Open(cold) => Some(cold),
            _ => None,
        })
    }
    /// The streams as a checkpoint names them now, once anything was ever appended: from the
    /// writer when it is open, else as the checkpoint they were restored from named them, with
    /// nothing pending.
    pub(crate) fn capture(
        &self,
        source: Option<&ColdSource>,
        budget: &MemoryBudget,
    ) -> Result<Option<CapturedCold>, ContentError> {
        match self {
            Self::Open(cold) if !cold.is_empty() => cold.capture(budget).map(Some),
            Self::Closed(Some(streams)) if streams.marks.length > 0 => {
                let source = source.ok_or(ContentError::Invalid)?;
                let permit = budget
                    .reserve(
                        BudgetKind::Recovery,
                        BudgetLane::Committed,
                        held(streams).ok_or(ContentError::Capacity)?,
                    )
                    .map_err(|_| ContentError::Capacity)?
                    .commit();
                Ok(Some(CapturedCold {
                    streams: ColdStreams::clone(streams),
                    data: (source.root.join("data"), Vec::new()),
                    marks: (source.root.join("marks"), Vec::new()),
                    _permit: permit,
                }))
            }
            _ => Ok(None),
        }
    }
    /// A checkpoint naming `streams` made what it names durable.
    pub(crate) fn made_durable(&mut self, streams: &ColdStreams) -> Result<(), ContentError> {
        match self {
            Self::Open(cold) => cold.made_durable(streams),
            _ => Ok(()),
        }
    }
}

/// The bytes the names and open chunks of `streams` hold.
fn held(streams: &ColdStreams) -> Option<usize> {
    let mut bytes = 0usize;
    for state in [&streams.data, &streams.marks] {
        bytes = state
            .manifests
            .len()
            .checked_add(state.names.len())?
            .checked_mul(size_of::<ContentHash>())?
            .checked_add(state.open.len())?
            .checked_add(bytes)?;
    }
    Some(bytes)
}

/// The writer of a session's two cold streams.
pub(crate) struct NativeCold {
    geometry: ColdGeometry,
    data: ColdStore,
    marks: ColdStore,
}

/// The cold streams as a checkpoint captured them on the owner, and the chunks of each that its
/// encoding thread makes durable before the checkpoint may be a recovery base.
pub(crate) struct CapturedCold {
    pub(crate) streams: ColdStreams,
    data: (PathBuf, Vec<ContentHash>),
    marks: (PathBuf, Vec<ContentHash>),
    _permit: Allocation,
}
impl CapturedCold {
    /// Make every chunk the capture names that is not durable yet durable, on this thread.
    pub(crate) fn make_durable(&self) -> Result<(), ContentError> {
        ColdStore::make_durable(&self.data.0, &self.data.1)?;
        ColdStore::make_durable(&self.marks.0, &self.marks.1)
    }
}

fn mark(sequence: SessionSeq, offset: u64) -> [u8; MARK_BYTES] {
    let mut mark = [0; MARK_BYTES];
    let (head, tail) = mark.split_at_mut(8);
    head.copy_from_slice(&sequence.0.to_le_bytes());
    tail.copy_from_slice(&offset.to_le_bytes());
    mark
}
fn word(bytes: &[u8], at: usize) -> Result<u64, ContentError> {
    let end = at.checked_add(8).ok_or(ContentError::Capacity)?;
    Ok(u64::from_le_bytes(
        bytes
            .get(at..end)
            .and_then(|word| word.try_into().ok())
            .ok_or(ContentError::Corrupt)?,
    ))
}

impl NativeCold {
    /// Open the streams under `source`, as `streams` names them (fresh ones when none).
    ///
    /// Restored streams keep the geometry they were cut with; fresh ones take the source's.
    pub(crate) fn open(
        source: &ColdSource,
        memory: &MemoryBudget,
        streams: Option<&ColdStreams>,
    ) -> Result<Self, ContentError> {
        let geometry = streams.map_or(source.geometry, |streams| streams.geometry);
        Ok(Self {
            geometry,
            data: ColdStore::open(
                source.root.join("data"),
                source.disk.clone(),
                memory,
                geometry,
                streams.map(|streams| &streams.data),
            )?,
            marks: ColdStore::open(
                source.root.join("marks"),
                source.disk.clone(),
                memory,
                geometry,
                streams.map(|streams| &streams.marks),
            )?,
        })
    }
    /// Whether anything was ever appended: a checkpoint names streams only once there is.
    pub(crate) fn is_empty(&self) -> bool {
        self.marks.length() == 0
    }
    /// Promise what a batch of `bytes` and its mark take ([`ColdStore::room`]): refused, nothing
    /// is promised; granted, appending them fails only on IO.
    pub(crate) fn room(&mut self, bytes: usize) -> Result<NativeColdRoom, ContentError> {
        Ok(NativeColdRoom {
            data: self.data.room(bytes)?,
            marks: self.marks.room(MARK_BYTES)?,
        })
    }
    /// Append the batch of the record at `sequence`, and its mark, within `room`.
    pub(crate) fn append(
        &mut self,
        sequence: SessionSeq,
        batch: &[u8],
        room: NativeColdRoom,
    ) -> Result<(), ContentError> {
        let offset = self.data.length();
        self.data.append(batch, room.data)?;
        self.marks.append(&mark(sequence, offset), room.marks)
    }
    /// The streams as a checkpoint names them now, charged to `budget` before they are copied and
    /// while the checkpoint holds them.
    pub(crate) fn capture(&self, budget: &MemoryBudget) -> Result<CapturedCold, ContentError> {
        let held = self
            .data
            .captured_bytes()?
            .checked_add(self.marks.captured_bytes()?)
            .ok_or(ContentError::Capacity)?;
        let permit = budget
            .reserve(BudgetKind::Recovery, BudgetLane::Committed, held)
            .map_err(|_| ContentError::Capacity)?
            .commit();
        let (data, data_pending) = self.data.capture()?;
        let (marks, marks_pending) = self.marks.capture()?;
        Ok(CapturedCold {
            streams: ColdStreams {
                geometry: self.geometry,
                data,
                marks,
            },
            data: (self.data.root().to_path_buf(), data_pending),
            marks: (self.marks.root().to_path_buf(), marks_pending),
            _permit: permit,
        })
    }
    /// A checkpoint naming `streams` made what it names durable.
    pub(crate) fn made_durable(&mut self, streams: &ColdStreams) -> Result<(), ContentError> {
        let (chunks, manifests) = streams.data.named(streams.geometry)?;
        self.data.made_durable(chunks, manifests);
        let (chunks, manifests) = streams.marks.named(streams.geometry)?;
        self.marks.made_durable(chunks, manifests);
        Ok(())
    }
    /// The frame of digest `digest` in the batch of the record at `sequence`: `None` when no
    /// version-2 record was applied there (its families retired under custody), refused when the
    /// batch holds no such frame. What each read holds is charged to `budget` before it is
    /// read, and the frame's bytes while it is held.
    pub(crate) fn frame(
        &self,
        sequence: SessionSeq,
        digest: ContentHash,
        budget: &MemoryBudget,
    ) -> Result<Option<ColdFrame>, ContentError> {
        let charge = budget
            .reserve(
                BudgetKind::Query,
                BudgetLane::Ordinary,
                locating(self.geometry)?,
            )
            .map_err(|_| ContentError::Capacity)?
            .commit();
        let data = |offset: u64, length: usize| self.data.read(offset, length);
        let marks = |offset: u64, length: usize| self.marks.read(offset, length);
        let Some((at, length)) = locate(self.marks.length(), &data, &marks, sequence, digest)?
        else {
            return Ok(None);
        };
        read_frame(self.geometry, charge, &data, at, length).map(Some)
    }
    /// What `streams` names that is not local, data's then marks': what an install pulls.
    pub(crate) fn missing(
        root: &Path,
        streams: &ColdStreams,
    ) -> Result<(Vec<ContentHash>, Vec<ContentHash>), ContentError> {
        let missing = |part: &str, state: &ColdState| -> Result<Vec<ContentHash>, ContentError> {
            let directory = root.join(part);
            if !directory.is_dir() {
                std::fs::create_dir_all(&directory)?;
            }
            state.missing(streams.geometry, &ColdReader::open(directory)?)
        };
        Ok((
            missing("data", &streams.data)?,
            missing("marks", &streams.marks)?,
        ))
    }
    /// Take a chunk a peer sent for an install into the data stream (`data`) or the marks under
    /// `source`: a file named by its hash, so no writer is needed.
    pub(crate) fn install(
        source: &ColdSource,
        data: bool,
        hash: ContentHash,
        bytes: &[u8],
    ) -> Result<(), ContentError> {
        focal_evidence::install_cold_chunk(
            &source.root.join(if data { "data" } else { "marks" }),
            &source.disk,
            hash,
            bytes,
        )
    }
}
