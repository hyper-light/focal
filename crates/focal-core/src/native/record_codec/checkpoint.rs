//! Complete native Core root bytes, distinct from mutation and Session formats.
//!
//! Encoding streams retained immutable rows without cloning values, acquiring a
//! root handle or staging the ledger. Structural inspection provides borrowed
//! untrusted bodies; it is not native model hydration, a custody proof or an
//! activation promise. Session membership, placement, request streams and the
//! Raft/native-prefix mapping belong to a future enclosing checkpoint layer.
use super::*;
use bytes::Cursor;

#[cfg(test)]
#[path = "checkpoint_tests.rs"]
mod tests;

pub const MAGIC: [u8; 8] = *b"FCNROOTS";
pub const VERSION: u16 = 4;
pub(super) const HASH_DOMAIN: &str = "focal.native.checkpoint.v2";

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
        },
        state.rows.entries().map(|entry| (entry.key, &entry.value)),
    )
}

/// Fixed root frame fields shared by a retained Core and an import image.
#[derive(Debug, Clone, Copy)]
pub(in crate::native) struct RootFrame {
    pub(in crate::native) ledger: LedgerId,
    pub(in crate::native) profile: NativeContentProfile,
    pub(in crate::native) range: RangeId,
    pub(in crate::native) prefix: u64,
    pub(in crate::native) count: usize,
}

/// Encode sorted rows as one complete root image. Import translation writes
/// its rows through this exact frame so the image restores like a checkpoint
/// and hashes identically on every replica (23 §5.1).
pub(in crate::native) fn encode_rows(
    frame: RootFrame,
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

fn frame_entries<'a>(
    sink: &mut impl Sink,
    frame: RootFrame,
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
