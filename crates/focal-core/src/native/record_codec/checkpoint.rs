//! Complete native Core root bytes, distinct from mutation and Session formats.
//!
//! Encoding streams retained immutable rows without cloning values, acquiring a
//! root handle or staging the ledger. Structural inspection provides borrowed
//! untrusted bodies; it is not native model hydration, a custody proof or an
//! activation promise. Session membership, placement, request streams and the
//! Raft/native-prefix mapping belong to a future enclosing checkpoint layer.
use super::*;
use bytes::Cursor;
use focal_memory::{BudgetKind, BudgetLane};

#[cfg(test)]
#[path = "checkpoint_tests.rs"]
mod tests;

pub const MAGIC: [u8; 8] = *b"FCNROOTS";
// Version 5 orders rows by the storage layout (25 §3). Version 6 records the
// range layout the rows are held in (25 §4): the members in key order, each
// with its durable identity and the least key it holds.
pub const VERSION: u16 = 6;
pub(in crate::native) const HASH_DOMAIN: &str = "focal.native.checkpoint.v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointHeader {
    pub ledger: LedgerId,
    pub profile: NativeContentProfile,
    /// Original process-local incarnation. Recovery must establish its mapping
    /// to the freshly restored owner, not reuse it as that owner's identity.
    pub range: RangeId,
    pub prefix: SessionSeq,
    /// Full retained row count; this is not a u32 mutation/collection length.
    pub rows: u64,
    pub hash: ContentHash,
}

/// A borrowed complete-root encoding plan. Each measurement/write pass uses
/// its quoted work. The caller separately funds its output buffering through
/// checkpoint persistence/retention and accounts any surrounding Session data.
/// This plan does not acquire any buffer, permit, root handle or snapshot.
pub struct EncodingPlan<'a> {
    core: &'a Core<NativeState>,
    quote: EncodingQuote,
}

/// Preserve an output adapter's original error without boxing, cloning or
/// replacing it with a codec error. Neither variant acknowledges durability.
#[derive(Debug, PartialEq, Eq)]
pub enum WriteError<E> {
    Output(E),
    Codec(CodecError),
}

impl<E: std::fmt::Display> std::fmt::Display for WriteError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(error) => write!(formatter, "checkpoint output failed: {error}"),
            Self::Codec(error) => write!(formatter, "checkpoint encoding failed: {error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for WriteError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Output(error) => Some(error),
            Self::Codec(error) => Some(error),
        }
    }
}

struct CallbackSink<'a, F, E> {
    meter: CountingSink,
    output: &'a mut F,
    error: Option<E>,
}

impl<F, E> Sink for CallbackSink<'_, F, E>
where
    F: FnMut(&[u8]) -> Result<(), E>,
{
    fn write(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        // Debit complete output byte/work bounds before calling user code.
        self.meter.write(bytes)?;
        match (self.output)(bytes) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.error = Some(error);
                Err(CodecError::InvalidTag("checkpoint output"))
            }
        }
    }

    fn visit(&mut self, amount: usize) -> Result<(), CodecError> {
        self.meter.visit(amount)
    }
}

impl<'a> EncodingPlan<'a> {
    pub fn prepare(
        core: &'a Core<NativeState>,
        limits: EncodingLimits,
    ) -> Result<Self, CodecError> {
        if core.state.rows.len() > limits.rows {
            return Err(CodecError::Capacity);
        }
        let mut sink = CountingSink::new(limits.bytes, limits.visits);
        let hash = frame(&mut sink, core)?;
        Ok(Self {
            core,
            quote: EncodingQuote {
                bytes: sink.len(),
                visits: sink.visits_used(),
                rows: core.state.rows.len(),
                hash,
            },
        })
    }

    pub fn quote(&self) -> EncodingQuote {
        self.quote
    }

    /// Identity of the actual borrowed root measured by this plan. Enclosing
    /// checkpoint layers can bind their metadata before writing any bytes.
    pub fn header(&self) -> Result<CheckpointHeader, CodecError> {
        Ok(CheckpointHeader {
            ledger: self.core.state.ledger,
            profile: self.core.state.profile,
            range: self.core.state.rows.id(),
            prefix: self.core.native_sequence(),
            rows: u64::try_from(self.quote.rows).map_err(|_| CodecError::Capacity)?,
            hash: self.quote.hash,
        })
    }

    /// A wrong-sized destination is refused before any byte is changed. Output
    /// allocation/accounting are caller responsibilities, separate from Core.
    pub fn write_into(&self, output: &mut [u8]) -> Result<ContentHash, CodecError> {
        if output.len() != self.quote.bytes {
            return Err(CodecError::Capacity);
        }
        let mut sink = SliceSink::new(output, self.quote.visits);
        let hash = frame(&mut sink, self.core)?;
        if sink.len() != self.quote.bytes
            || sink.visits_used() != self.quote.visits
            || hash != self.quote.hash
        {
            return Err(CodecError::InvalidTag("checkpoint source"));
        }
        sink.finish()?;
        Ok(hash)
    }

    /// Stream the exact measured bytes without allocating an encoded-root
    /// buffer. The callback must consume each complete borrowed chunk before
    /// returning success. It should coalesce the many small field writes in a
    /// separately funded bounded output buffer and split large borrowed chunks
    /// as necessary; callback I/O/work and retention are separately accounted.
    ///
    /// Byte order, digest and codec work equal `write_into`. No callback runs
    /// after refusal. A failed output can leave a written prefix, which the
    /// caller must discard or truncate before retry. Even successful encoding
    /// does not acknowledge persistence, flush the callback's buffer or publish
    /// a checkpoint. The caller establishes those durability fences separately.
    pub fn write_with<E>(
        &self,
        mut output: impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<ContentHash, WriteError<E>> {
        let mut sink = CallbackSink {
            meter: CountingSink::new(self.quote.bytes, self.quote.visits),
            output: &mut output,
            error: None,
        };
        let result = frame(&mut sink, self.core);
        if let Some(error) = sink.error.take() {
            return Err(WriteError::Output(error));
        }
        let hash = result.map_err(WriteError::Codec)?;
        if sink.meter.len() != self.quote.bytes
            || sink.meter.visits_used() != self.quote.visits
            || hash != self.quote.hash
        {
            return Err(WriteError::Codec(CodecError::InvalidTag(
                "checkpoint source",
            )));
        }
        Ok(hash)
    }
}

/// Integrity-checked outer frame whose row bodies remain semantically untrusted.
/// No allocation, root restoration, publication or participant action occurs.
pub struct StructuralCheckpoint<'a> {
    header: CheckpointHeader,
    /// The encoded range layout: `members` boundaries, checked in order.
    layout: &'a [u8],
    members: usize,
    rows: &'a [u8],
    row_limit: usize,
    quote: InspectionQuote,
}

impl<'a> StructuralCheckpoint<'a> {
    pub fn inspect(bytes: &'a [u8], limits: InspectionLimits) -> Result<Self, CodecError> {
        if bytes.len() > limits.bytes {
            return Err(CodecError::Capacity);
        }
        let at = bytes.len().checked_sub(32).ok_or(CodecError::Truncated)?;
        let (payload, trailer) = bytes.split_at_checked(at).ok_or(CodecError::Truncated)?;
        // Debit digest setup/finalization, every hashed byte and the checksum
        // read/comparison before parsing variable row data.
        let hash_visits = add(payload.len(), 322)?;
        let available = limits
            .visits
            .checked_sub(hash_visits)
            .ok_or(CodecError::Capacity)?;
        let mut cursor = Cursor::new(payload, limits.bytes, available)?;
        if cursor.fixed::<8>()? != MAGIC || cursor.u16()? != VERSION {
            return Err(CodecError::InvalidTag("checkpoint format"));
        }
        let profile = match cursor.u8()? {
            0 => NativeContentProfile::ProjectionOnly,
            1 => NativeContentProfile::AuthoredV1,
            _ => return Err(CodecError::InvalidTag("checkpoint profile")),
        };
        let ledger = fixed::read_ledger(&mut cursor)?;
        let range = RangeId(u128::from_le_bytes(cursor.fixed()?));
        let prefix = SessionSeq(cursor.u64()?);
        let raw_count = cursor.u64()?;
        cursor.visit(1)?;
        let count = usize::try_from(raw_count).map_err(|_| CodecError::Capacity)?;
        if count > limits.rows {
            return Err(CodecError::Capacity);
        }
        if ledger.tenant.is_zero() || ledger.session.is_zero() || (prefix.0 == 0) != (count == 0) {
            return Err(CodecError::InvalidTag("checkpoint frame"));
        }
        let layout_start = cursor.offset();
        let (members, _) = read_layout(&mut cursor, |_| Ok(()))?;
        let layout = payload
            .get(layout_start..cursor.offset())
            .ok_or(CodecError::Truncated)?;
        let start = cursor.offset();
        let mut previous = None;
        let mut meta = false;
        let mut outcome = false;
        // Include every row-loop advance and its final exhaustion probe before
        // scanning. The shared borrowed row iterator charges these on next().
        cursor.visit(add(count, 1)?)?;
        for _ in 0..count {
            let row = inspect::read_row(&mut cursor, limits.row_bytes)?;
            cursor.visit(1)?;
            if row.deleted() || previous.is_some_and(|last| last >= row.key) {
                return Err(CodecError::InvalidTag("checkpoint row"));
            }
            previous = Some(row.key);
            meta |= row.key == Key::Meta;
            outcome |= matches!(row.key, Key::Outcome(_));
        }
        if prefix.0 != 0 && (!meta || !outcome) {
            return Err(CodecError::InvalidTag("checkpoint accounting rows"));
        }
        let rows = payload
            .get(start..cursor.offset())
            .ok_or(CodecError::Truncated)?;
        let visits = add(cursor.visits_used(), hash_visits)?;
        cursor.finish()?;
        let mut hasher = blake3::Hasher::new_derive_key(HASH_DOMAIN);
        hasher.update(payload);
        let hash = ContentHash(*hasher.finalize().as_bytes());
        if trailer != hash.0 {
            return Err(CodecError::InvalidTag("checkpoint checksum"));
        }
        Ok(Self {
            header: CheckpointHeader {
                ledger,
                profile,
                range,
                prefix,
                rows: raw_count,
                hash,
            },
            layout,
            members,
            rows,
            row_limit: limits.row_bytes,
            quote: InspectionQuote {
                bytes: bytes.len(),
                visits,
                rows: count,
            },
        })
    }

    pub fn header(&self) -> CheckpointHeader {
        self.header
    }
    pub fn quote(&self) -> InspectionQuote {
        self.quote
    }
    /// How many range members the frame's layout names.
    pub fn members(&self) -> usize {
        self.members
    }
    /// The range layout the rows are held in, decoded again from the
    /// inspected bytes into a layout of at most `max` members charged to
    /// `budget` (25 §4).
    pub fn layout(
        &self,
        max: usize,
        budget: &MemoryBudget,
    ) -> Result<ranges::RangeLayout, NativeError> {
        if self.members > max.min(ranges::MAX_LAYOUT_MEMBERS) {
            return Err(NativeError::Capacity("range layout members"));
        }
        let allocation = budget
            .reserve(
                BudgetKind::Roots,
                BudgetLane::Ordinary,
                prepare::array::<ranges::RangeBoundary>(self.members)?,
            )?
            .commit();
        let mut members = Vec::new();
        members
            .try_reserve_exact(self.members)
            .map_err(|_| MemoryError::AllocationFailed)?;
        // The re-read charges the layout's work once more plus every fixed
        // field it takes; both are bounded by the codec ceiling.
        let visits = layout_visits(self.members)
            .and_then(|work| work.checked_mul(2).ok_or(CodecError::Capacity))
            .and_then(|work| {
                self.members
                    .checked_mul(24)
                    .and_then(|fields| work.checked_add(fields))
                    .and_then(|work| work.checked_add(64))
                    .ok_or(CodecError::Capacity)
            })
            .map_err(read_source::model_error)?;
        let mut cursor = Cursor::new(self.layout, self.layout.len(), visits)
            .map_err(read_source::model_error)?;
        let (decoded, epoch) = read_layout(&mut cursor, |boundary| {
            if members.len() >= members.capacity() {
                return Err(CodecError::Capacity);
            }
            members.push(boundary);
            Ok(())
        })
        .map_err(read_source::model_error)?;
        if decoded != self.members || cursor.remaining() != 0 {
            return Err(ContractError::InvalidManifest.into());
        }
        ranges::RangeLayout::new(epoch, members, allocation, max)
    }

    /// Each scan has a separate explicit work budget. A phased decoder must
    /// also account body parsing, model hydration and dependency lookups; this
    /// iterator cannot attest that an opaque body is valid native state.
    pub fn rows(&self, max_visits: usize) -> Result<RecordRows<'a>, CodecError> {
        RecordRows::new(self.rows, self.quote.rows, self.row_limit, max_visits)
    }
}

fn iteration_work() -> Result<usize, CodecError> {
    // A next/initial seek may walk the bounded persistent directory height.
    // Include key comparisons and the terminating probe without a root pin or
    // per-page/row offset index. The fixed-width key has no variable heap.
    usize::try_from(usize::BITS)
        .map_err(|_| CodecError::Capacity)?
        .checked_add(1)
        .and_then(|n| n.checked_mul(64))
        .ok_or(CodecError::Capacity)
}

fn frame(sink: &mut impl Sink, core: &Core<NativeState>) -> Result<ContentHash, CodecError> {
    let state = &core.state;
    frame_entries(
        sink,
        RootFrame {
            ledger: state.ledger,
            profile: state.profile,
            range: state.rows.id(),
            prefix: state.rows.prefix(),
            count: state.rows.len(),
            layout: state.rows.layout().members(),
            layout_epoch: state.rows.layout().epoch(),
        },
        state.rows.entries().map(|entry| (entry.key, &entry.value)),
    )
}

/// The content hash of the Core's rows alone, under a fixed range identity
/// and a canonical one-member layout: two replicas at one prefix agree
/// exactly when their rows are byte-identical, whatever range each opened
/// its store as and however each lays its rows out (doc 25 §2, §4).
pub fn rows_digest(
    core: &Core<NativeState>,
    limits: EncodingLimits,
) -> Result<ContentHash, CodecError> {
    let state = &core.state;
    if state.rows.len() > limits.rows {
        return Err(CodecError::Capacity);
    }
    let mut sink = CountingSink::new(limits.bytes, limits.visits);
    frame_entries(
        &mut sink,
        RootFrame {
            ledger: state.ledger,
            profile: state.profile,
            range: RangeId(1),
            prefix: state.rows.prefix(),
            count: state.rows.len(),
            layout: &[ranges::RangeBoundary {
                id: RangeId(1),
                start: None,
            }],
            layout_epoch: 0,
        },
        state.rows.entries().map(|entry| (entry.key, &entry.value)),
    )
}

/// A digest of one member's rows at the group's prefix (25 §6): the root
/// frame of those rows alone under a canonical single layout, so two holders
/// of the member agree exactly when their rows are byte-identical. A member
/// holding no rows digests its prefix under its own domain.
pub fn member_digest(
    core: &Core<NativeState>,
    index: usize,
    limits: EncodingLimits,
) -> Result<ContentHash, CodecError> {
    let state = &core.state;
    let count = state
        .rows
        .member_entries(index)
        .ok_or(CodecError::InvalidTag("checkpoint member"))?
        .count();
    if count > limits.rows {
        return Err(CodecError::Capacity);
    }
    let prefix = state.rows.prefix();
    if count == 0 {
        return Ok(ContentHash(blake3::derive_key(
            "focal.native.member-digest.empty.v1",
            &prefix.to_le_bytes(),
        )));
    }
    let entries = state
        .rows
        .member_entries(index)
        .ok_or(CodecError::InvalidTag("checkpoint member"))?;
    let mut sink = CountingSink::new(limits.bytes, limits.visits);
    frame_entries(
        &mut sink,
        RootFrame {
            ledger: state.ledger,
            profile: state.profile,
            range: RangeId(1),
            prefix,
            count,
            layout: &[ranges::RangeBoundary {
                id: RangeId(1),
                start: None,
            }],
            layout_epoch: 0,
        },
        entries.map(|entry| (entry.key, &entry.value)),
    )
}

/// Fixed root frame fields shared by a retained Core and an import image.
#[derive(Debug, Clone, Copy)]
pub(in crate::native) struct RootFrame<'l> {
    pub(in crate::native) ledger: LedgerId,
    pub(in crate::native) profile: NativeContentProfile,
    pub(in crate::native) range: RangeId,
    pub(in crate::native) prefix: u64,
    pub(in crate::native) count: usize,
    /// The members of the range layout in key order and its epoch (25 §4).
    pub(in crate::native) layout: &'l [ranges::RangeBoundary],
    pub(in crate::native) layout_epoch: u64,
}

/// The work of parsing or writing a layout of `members`: each member's fixed
/// fields and the pairwise identity check.
fn layout_visits(members: usize) -> Result<usize, CodecError> {
    members
        .checked_mul(members)
        .and_then(|pairs| pairs.checked_add(members.checked_mul(8)?))
        .and_then(|work| work.checked_add(16))
        .ok_or(CodecError::Capacity)
}

/// Check a layout's members in order: one to the codec ceiling, the first
/// from the least key, every other start above the previous, distinct
/// identities.
fn check_layout(members: &[ranges::RangeBoundary]) -> Result<(), CodecError> {
    if members.is_empty() || members.len() > ranges::MAX_LAYOUT_MEMBERS {
        return Err(CodecError::InvalidTag("checkpoint layout members"));
    }
    let mut previous: Option<ranges::Affinity> = None;
    for (index, member) in members.iter().enumerate() {
        match (index, member.start) {
            (0, None) => {}
            (0, Some(_)) | (_, None) => return Err(CodecError::InvalidTag("checkpoint layout")),
            (_, Some(start)) => {
                if previous.is_some_and(|last| start <= last) {
                    return Err(CodecError::InvalidTag("checkpoint layout order"));
                }
                previous = Some(start);
            }
        }
        if members
            .get(..index)
            .is_some_and(|earlier| earlier.iter().any(|other| other.id == member.id))
        {
            return Err(CodecError::InvalidTag("checkpoint layout identity"));
        }
    }
    Ok(())
}

fn write_layout(
    sink: &mut impl Sink,
    epoch: u64,
    members: &[ranges::RangeBoundary],
) -> Result<(), CodecError> {
    check_layout(members)?;
    sink.visit(layout_visits(members.len())?)?;
    write_count(sink, members.len())?;
    write_u64(sink, epoch)?;
    for member in members {
        write_raw(sink, &member.id.0.to_le_bytes())?;
        match member.start {
            None => write_u8(sink, 0)?,
            Some(start) => {
                write_u8(sink, 1)?;
                write_raw(sink, &start)?;
            }
        }
    }
    Ok(())
}

/// Read a layout, handing each member to `each` in order and checking the
/// same rules `write_layout` enforces; returns the member count and epoch.
fn read_layout(
    cursor: &mut Cursor<'_>,
    mut each: impl FnMut(ranges::RangeBoundary) -> Result<(), CodecError>,
) -> Result<(usize, u64), CodecError> {
    let members = cursor.count(ranges::MAX_LAYOUT_MEMBERS)?;
    if members == 0 {
        return Err(CodecError::InvalidTag("checkpoint layout members"));
    }
    let epoch = cursor.u64()?;
    cursor.visit(layout_visits(members)?)?;
    let mut previous: Option<ranges::Affinity> = None;
    // Identities are checked against a window of the ones already read;
    // `RangeLayout::new` repeats the complete check over the decoded vector.
    let mut seen = [RangeId(0); 8];
    let mut seen_count = 0usize;
    for index in 0..members {
        let id = RangeId(u128::from_le_bytes(cursor.fixed()?));
        let start = match cursor.u8()? {
            0 => None,
            1 => Some(cursor.fixed::<16>()?),
            _ => return Err(CodecError::InvalidTag("checkpoint layout start")),
        };
        match (index, start) {
            (0, None) => {}
            (0, Some(_)) | (_, None) => return Err(CodecError::InvalidTag("checkpoint layout")),
            (_, Some(start)) => {
                if previous.is_some_and(|last| start <= last) {
                    return Err(CodecError::InvalidTag("checkpoint layout order"));
                }
                previous = Some(start);
            }
        }
        if id.0 == 0 {
            return Err(CodecError::InvalidTag("checkpoint layout identity"));
        }
        // A small inline window catches every duplicate among nearby
        // members without allocating; a strictly increasing start already
        // orders the members, and `RangeLayout::new` repeats the full check
        // over the decoded vector.
        if seen
            .get(..seen_count.min(seen.len()))
            .is_some_and(|window| window.contains(&id))
        {
            return Err(CodecError::InvalidTag("checkpoint layout identity"));
        }
        let slot_index = seen_count.checked_rem(seen.len()).unwrap_or(0);
        if let Some(slot) = seen.get_mut(slot_index) {
            *slot = id;
        }
        seen_count = seen_count.saturating_add(1);
        each(ranges::RangeBoundary { id, start })?;
    }
    Ok((members, epoch))
}

/// Encode sorted rows as one complete root image. Import translation writes
/// its rows through this exact frame so the image restores like a checkpoint
/// and hashes identically on every replica (23 §5.1).
pub(in crate::native) fn encode_rows(
    frame: RootFrame<'_>,
    rows: &[(Key, Row)],
    limits: EncodingLimits,
) -> Result<(Vec<u8>, ContentHash), CodecError> {
    if rows.len() > limits.rows || frame.count != rows.len() {
        return Err(CodecError::Capacity);
    }
    let mut counting = CountingSink::new(limits.bytes, limits.visits);
    let expected = frame_entries(
        &mut counting,
        frame,
        rows.iter().map(|(key, row)| (*key, row)),
    )?;
    let (bytes, visits) = (counting.len(), counting.visits_used());
    let mut output = Vec::new();
    output
        .try_reserve_exact(bytes)
        .map_err(|_| CodecError::Capacity)?;
    if output.capacity() != bytes {
        return Err(CodecError::Capacity);
    }
    output.resize(bytes, 0);
    let mut sink = SliceSink::new(&mut output, visits);
    let hash = frame_entries(&mut sink, frame, rows.iter().map(|(key, row)| (*key, row)))?;
    if sink.len() != bytes || hash != expected {
        return Err(CodecError::InvalidTag("import image"));
    }
    sink.finish()?;
    Ok((output, hash))
}

/// An archive bundle's frame (26 §4): the rows of one retired family in
/// key order under a header naming the ledger, the retention prefix, the
/// root and every member, digested like a checkpoint root. It carries no
/// accounting rows: it is never restored as a core, only read.
pub const ARCHIVE_MAGIC: [u8; 8] = *b"FCNARCHV";
pub const ARCHIVE_VERSION: u16 = 1;
/// Whether every element is strictly greater than the one before it.
fn strictly_ascending<T: Ord>(items: &[T]) -> bool {
    items
        .iter()
        .zip(items.iter().skip(1))
        .all(|(earlier, later)| earlier < later)
}

pub(super) const ARCHIVE_HASH_DOMAIN: &str = "focal.native.archive.v1";
pub(in crate::native) struct ArchiveFrame<'m> {
    pub ledger: LedgerId,
    pub profile: NativeContentProfile,
    pub through: SessionSeq,
    pub root: ClaimId,
    pub members: &'m [ClaimId],
    /// The content roots of the family's artifacts held as content objects,
    /// sorted: the proof the bundle keeps under custody (26 §5).
    pub content: &'m [ContentHash],
    /// The content roots of the sealed objects of the family's artifacts
    /// held inline, sorted.
    pub inline: &'m [ContentHash],
    pub count: usize,
}
pub(in crate::native) fn archive_frame<'a>(
    sink: &mut impl Sink,
    frame: ArchiveFrame<'_>,
    mut entries: impl Iterator<Item = (Key, &'a Row)>,
) -> Result<ContentHash, CodecError> {
    if frame.ledger.tenant.is_zero()
        || frame.ledger.session.is_zero()
        || frame.through.0 == 0
        || frame.count == 0
        || frame.members.is_empty()
        || !frame.members.contains(&frame.root)
    {
        return Err(CodecError::InvalidTag("archive frame"));
    }
    let count = u64::try_from(frame.count).map_err(|_| CodecError::Capacity)?;
    let members = u32::try_from(frame.members.len()).map_err(|_| CodecError::Capacity)?;
    let content = u32::try_from(frame.content.len()).map_err(|_| CodecError::Capacity)?;
    let inline = u32::try_from(frame.inline.len()).map_err(|_| CodecError::Capacity)?;
    if !strictly_ascending(frame.content) || !strictly_ascending(frame.inline) {
        return Err(CodecError::InvalidTag("archive content order"));
    }
    sink.visit(add(
        add(add(256, frame.members.len())?, frame.content.len())?,
        frame.inline.len(),
    )?)?;
    let mut hashed = HashSink {
        sink,
        hash: blake3::Hasher::new_derive_key(ARCHIVE_HASH_DOMAIN),
    };
    write_raw(&mut hashed, &ARCHIVE_MAGIC)?;
    write_u16(&mut hashed, ARCHIVE_VERSION)?;
    write_u8(
        &mut hashed,
        match frame.profile {
            NativeContentProfile::ProjectionOnly => 0,
            NativeContentProfile::AuthoredV1 => 1,
        },
    )?;
    types::ledger(&mut hashed, frame.ledger)?;
    write_u64(&mut hashed, frame.through.0)?;
    write_raw(&mut hashed, &frame.root.0)?;
    write_u32(&mut hashed, members)?;
    for member in frame.members {
        write_raw(&mut hashed, &member.0)?;
    }
    write_u32(&mut hashed, content)?;
    for root in frame.content {
        write_raw(&mut hashed, &root.0)?;
    }
    write_u32(&mut hashed, inline)?;
    for root in frame.inline {
        write_raw(&mut hashed, &root.0)?;
    }
    write_u64(&mut hashed, count)?;
    let iteration = iteration_work()?;
    let mut previous = None;
    for _ in 0..frame.count {
        hashed.visit(iteration)?;
        let (key, value) = entries
            .next()
            .ok_or(CodecError::InvalidTag("archive row count"))?;
        if previous.is_some_and(|last| last >= key) {
            return Err(CodecError::InvalidTag("archive key order"));
        }
        rows::family(key, value)?;
        previous = Some(key);
        put_row(&mut hashed, key, value, frame.ledger)?;
    }
    if entries.next().is_some() {
        return Err(CodecError::InvalidTag("archive row count"));
    }
    let digest = ContentHash(*hashed.hash.finalize().as_bytes());
    write_raw(hashed.sink, &digest.0)?;
    Ok(digest)
}
fn frame_entries<'a>(
    sink: &mut impl Sink,
    frame: RootFrame<'_>,
    mut entries: impl Iterator<Item = (Key, &'a Row)>,
) -> Result<ContentHash, CodecError> {
    let count = frame.count;
    let prefix = frame.prefix;
    if frame.ledger.tenant.is_zero()
        || frame.ledger.session.is_zero()
        || (prefix == 0) != (count == 0)
    {
        return Err(CodecError::InvalidTag("checkpoint frame"));
    }
    let count_u64 = u64::try_from(count).map_err(|_| CodecError::Capacity)?;
    sink.visit(256)?;
    let mut hashed = HashSink {
        sink,
        hash: blake3::Hasher::new_derive_key(HASH_DOMAIN),
    };
    write_raw(&mut hashed, &MAGIC)?;
    write_u16(&mut hashed, VERSION)?;
    write_u8(
        &mut hashed,
        match frame.profile {
            NativeContentProfile::ProjectionOnly => 0,
            NativeContentProfile::AuthoredV1 => 1,
        },
    )?;
    types::ledger(&mut hashed, frame.ledger)?;
    write_raw(&mut hashed, &frame.range.0.to_le_bytes())?;
    write_u64(&mut hashed, prefix)?;
    write_u64(&mut hashed, count_u64)?;
    write_layout(&mut hashed, frame.layout_epoch, frame.layout)?;
    let iteration = iteration_work()?;
    hashed.visit(iteration)?;
    let mut previous = None;
    let mut meta = false;
    let mut outcome = false;
    for _ in 0..count {
        hashed.visit(iteration)?;
        let (key, value) = entries
            .next()
            .ok_or(CodecError::InvalidTag("checkpoint row count"))?;
        if previous.is_some_and(|last| last >= key) {
            return Err(CodecError::InvalidTag("checkpoint key order"));
        }
        rows::family(key, value)?;
        previous = Some(key);
        meta |= key == Key::Meta;
        outcome |= matches!(key, Key::Outcome(_));
        put_row(&mut hashed, key, value, frame.ledger)?;
    }
    hashed.visit(iteration)?;
    if entries.next().is_some() {
        return Err(CodecError::InvalidTag("checkpoint row count"));
    }
    if prefix != 0 && (!meta || !outcome) {
        return Err(CodecError::InvalidTag("checkpoint accounting rows"));
    }
    let digest = ContentHash(*hashed.hash.finalize().as_bytes());
    write_raw(hashed.sink, &digest.0)?;
    Ok(digest)
}

fn put_row(sink: &mut impl Sink, key: Key, row: &Row, ledger: LedgerId) -> Result<(), CodecError> {
    write_u8(sink, 1)?;
    fixed::key(sink, key)?;
    let mut measure = RowSize { sink, bytes: 0 };
    rows::value(&mut measure, row, ledger)?;
    let size = measure.bytes;
    if size == 0 {
        return Err(CodecError::InvalidTag("empty checkpoint body"));
    }
    write_count(sink, size)?;
    rows::value(sink, row, ledger)
}
