//! Enclosing native-genesis Session checkpoints. This format is separate from
//! both the native Core root and every frozen legacy Session format. It records
//! physical group identity, exact applied Raft coordinates, membership and the
//! native recording-range mapping. Cursor/delta/managed-stream/placement state
//! is explicitly disabled; populated legacy Session import is not supported.
use focal_consensus::MembershipConfiguration;
use focal_core::Core;
use focal_core::native::input_codec as input;
use focal_core::native::{NativeContentProfile, NativeState, record_codec};
use focal_evidence::{ContentError, SEED_CHUNK_BYTES, SeedReader, SeedStore};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, RangeId};
use focal_model::{ContentHash, LedgerId, SessionSeq};
use record_codec::checkpoint as root;

#[path = "native_checkpoint_fields.rs"]
mod fields;
#[path = "native_checkpoint_read.rs"]
mod read;
pub use read::{AssembledCore, Checkpoint, SeedError, SeedManifest};

pub const MAGIC: [u8; 8] = *b"FCNSESS1";
// Version 2 records the prefix that holds no native record yet (zero at
// genesis, one after an import), so the recording-range invariant stays exact.
// Version 3 retains the Raft index of the committed activation record, so a
// restored replica reports the exact activation position instead of a bound.
// Version 4 carries the nested Core root either inline or as a seed
// manifest (25 §5): a table of content-addressed chunks the author sealed
// in its seed store, so a checkpoint of any size rides one small snapshot.
pub const VERSION: u16 = 4;
const HASH_DOMAIN: &str = "focal.native.session.checkpoint.v1";
const ALLOCATION: usize = 4 * size_of::<usize>();

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("native Session checkpoint capacity exceeded")]
    Capacity,
    #[error("truncated native Session checkpoint")]
    Truncated,
    #[error("invalid native Session checkpoint: {0}")]
    Invalid(&'static str),
    #[error("native Core checkpoint: {0}")]
    Core(#[from] record_codec::CodecError),
    #[error("native Session checkpoint memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("checkpoint output refused")]
    Output,
    #[error("seeded checkpoint needs its seed store")]
    Seeded,
    #[error("checkpoint seed store: {0}")]
    Seeds(#[from] ContentError),
}

/// One sealed chunk of a seeded checkpoint's Core root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedChunk {
    pub hash: ContentHash,
    pub length: u32,
}
/// A chunk table entry: the hash and the length.
const SEED_CHUNK_ENTRY: usize = 36;

/// How a plan carries the nested Core root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Form {
    Inline,
    Seeded { chunks: usize },
}

/// The compiled native-genesis decoder covers this envelope, the input frame
/// codec and both nested native codecs; it is the one durable capability identity. It does not advertise a legacy successor or authorize import.
pub fn format_hash() -> ContentHash {
    let mut hash = blake3::Hasher::new_derive_key("focal.native.session.decoder.v1");
    hash.update(b"native-genesis;membership;applied-index-term;recording-range;disabled-cursor-delta-managed-placement;legacy-import-v1;input-frames");
    hash.update(&MAGIC);
    hash.update(&VERSION.to_le_bytes());
    hash.update(&input::MAGIC);
    hash.update(&input::VERSION.to_le_bytes());
    hash.update(&record_codec::MAGIC);
    hash.update(&record_codec::VERSION.to_le_bytes());
    hash.update(&root::MAGIC);
    hash.update(&root::VERSION.to_le_bytes());
    ContentHash(*hash.finalize().as_bytes())
}

/// Stable group identity is independent of each process's fresh RangeId.
pub fn genesis(
    cluster: [u8; 16],
    group: [u8; 16],
    ledger: LedgerId,
    profile: NativeContentProfile,
    decoder: ContentHash,
) -> ContentHash {
    let mut hash = blake3::Hasher::new_derive_key("focal.native.session.genesis.v1");
    hash.update(&cluster);
    hash.update(&group);
    hash.update(&ledger.tenant.0);
    hash.update(&ledger.session.0);
    hash.update(&[fields::profile(profile)]);
    hash.update(&decoder.0);
    ContentHash(*hash.finalize().as_bytes())
}

/// Only native genesis is representable. `durable_floor` must come from the
/// actual durable consensus owner; constructing these values proves no fsync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Activation {
    pub decoder: ContentHash,
    pub durable_floor: ContentHash,
    pub genesis: ContentHash,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AncillaryProfile {
    NativeOnlyV1,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    pub cluster: [u8; 16],
    pub group: [u8; 16],
    pub applied_raft: u64,
    pub applied_term: u64,
    pub configuration_index: u64,
    /// Actual producer of the last committed native record, retained across
    /// restart; this can differ from the current Core checkpoint's RangeId.
    pub recording_range: Option<RangeId>,
    /// Raft term that published the recorded producer range.
    pub recording_term: u64,
    /// The native prefix that holds no record: zero at genesis, one when the
    /// prefix was imported from legacy history (23 §5). A recording range
    /// exists exactly when the prefix has advanced past this floor.
    pub records_floor: u64,
    /// Raft index of the committed activation record this prefix descends
    /// from; never zero and never beyond the applied index.
    pub activation_index: u64,
    pub activation: Activation,
    pub ancillary: AncillaryProfile,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub metadata: Metadata,
    pub ledger: LedgerId,
    pub profile: NativeContentProfile,
    /// Original range of the nested Core root, never the restored incarnation.
    pub range: RangeId,
    pub prefix: SessionSeq,
    pub core_hash: ContentHash,
    pub core_bytes: u64,
    pub hash: ContentHash,
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub bytes: usize,
    /// Cumulative enclosing/nested parsing or preparation work. An inspected
    /// checkpoint retains the unused allowance for membership comparisons.
    pub visits: usize,
    pub members: usize,
    pub rows: usize,
    pub row_bytes: usize,
    /// The largest Core root carried inline; a larger one is seeded (25 §5).
    pub inline_bytes: usize,
    /// The largest Core root a seeded checkpoint may assemble on install.
    pub assembled_bytes: usize,
    /// The largest movement section (25 §6) a frame may carry.
    pub movement_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 8 * 1024 * 1024,
            visits: 256 * 1024 * 1024,
            members: 2048,
            rows: 100_000,
            row_bytes: 8 * 1024 * 1024,
            inline_bytes: 4 * 1024 * 1024,
            assembled_bytes: 256 * 1024 * 1024,
            movement_bytes: 1024 * 1024,
        }
    }
}
impl Limits {
    /// The most chunks a seeded checkpoint may name.
    pub fn max_seed_chunks(&self) -> usize {
        self.assembled_bytes.div_ceil(SEED_CHUNK_BYTES).max(1)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quote {
    pub bytes: usize,
    pub output_charge: usize,
    /// Complete work for one subsequent encoding pass, including nested Core.
    pub visits: usize,
    /// Includes preparing the Core plan before the enclosing measurement pass.
    pub preparation_visits: usize,
    pub hash: ContentHash,
}

/// The retention section (26 §3): the prefix the archive reports holding
/// every proof through, and the families retired through the checkpoint's
/// prefix (26 §4); carried once either is above zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionSection {
    pub archived_through: SessionSeq,
    pub retired_families: u64,
}
pub struct EncodingPlan<'a> {
    core: root::EncodingPlan<'a>,
    metadata: Metadata,
    configuration: &'a MembershipConfiguration,
    /// The movement section (25 §6), when the session carries one.
    movement: Option<&'a [u8]>,
    /// The retention section (26 §3), when the session carries one.
    retention: Option<RetentionSection>,
    limits: Limits,
    quote: Quote,
    form: Form,
}

/// Bytes and their original output permit move together into persistence. The
/// buffer is dropped before its permit, including on refused persistence.
pub struct EncodedCheckpoint {
    bytes: Vec<u8>,
    allocation: Allocation,
}
impl EncodedCheckpoint {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn charge(&self) -> usize {
        self.allocation.bytes()
    }
    pub fn into_parts(self) -> (Vec<u8>, Allocation) {
        (self.bytes, self.allocation)
    }
}
#[derive(Debug)]
pub enum WriteError<E> {
    Output(E),
    Codec(Error),
}
impl<E: std::fmt::Display> std::fmt::Display for WriteError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(error) => write!(f, "checkpoint output: {error}"),
            Self::Codec(error) => error.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for WriteError<E> {}

impl<'a> EncodingPlan<'a> {
    pub fn prepare(
        core: &'a Core<NativeState>,
        metadata: Metadata,
        configuration: &'a MembershipConfiguration,
        limits: Limits,
    ) -> Result<Self, Error> {
        Self::prepare_with(core, metadata, configuration, None, limits)
    }
    /// A plan carrying a movement section (25 §6) after the configuration.
    pub fn prepare_with(
        core: &'a Core<NativeState>,
        metadata: Metadata,
        configuration: &'a MembershipConfiguration,
        movement: Option<&'a [u8]>,
        limits: Limits,
    ) -> Result<Self, Error> {
        Self::prepare_with_sections(core, metadata, configuration, movement, None, limits)
    }
    /// A plan carrying a movement section (25 §6) and a retention section
    /// (26 §3) after the configuration.
    pub fn prepare_with_sections(
        core: &'a Core<NativeState>,
        metadata: Metadata,
        configuration: &'a MembershipConfiguration,
        movement: Option<&'a [u8]>,
        retention: Option<RetentionSection>,
        limits: Limits,
    ) -> Result<Self, Error> {
        if movement.is_some_and(|bytes| bytes.is_empty() || bytes.len() > limits.movement_bytes) {
            return Err(Error::Invalid("movement section"));
        }
        let core = root::EncodingPlan::prepare(
            core,
            record_codec::EncodingLimits {
                bytes: limits.inline_bytes.max(limits.assembled_bytes),
                visits: limits.visits,
                rows: limits.rows,
            },
        )?;
        let available = limits
            .visits
            .checked_sub(core.quote().visits)
            .ok_or(Error::Capacity)?;
        let core_bytes = core.quote().bytes;
        let form = if core_bytes <= limits.inline_bytes {
            Form::Inline
        } else {
            let chunks = core_bytes.div_ceil(SEED_CHUNK_BYTES);
            if chunks > limits.max_seed_chunks() {
                return Err(Error::Capacity);
            }
            Form::Seeded { chunks }
        };
        let mut discard = |_: &[u8]| -> Result<(), std::convert::Infallible> { Ok(()) };
        let mut sink = fields::Sink::new(limits.bytes, available, &mut discard);
        // A seeded frame is measured over a placeholder table of the same
        // shape; its digest is known once the chunks are sealed.
        let head = FrameHead {
            metadata,
            configuration,
            movement,
            retention,
            members: limits.members,
        };
        let hash = match form {
            Form::Inline => frame(&mut sink, &core, head)?,
            Form::Seeded { chunks } => {
                let mut sealed = |index: usize| SeedChunk {
                    hash: ContentHash([0; 32]),
                    length: chunk_length(core_bytes, index),
                };
                frame_seeded(&mut sink, &core, head, chunks, &mut sealed)?;
                ContentHash([0; 32])
            }
        };
        let bytes = sink.length;
        let visits = sink.used();
        Ok(Self {
            metadata,
            configuration,
            movement,
            retention,
            limits,
            quote: Quote {
                bytes,
                output_charge: fields::add(bytes, ALLOCATION)?,
                visits,
                preparation_visits: fields::add(core.quote().visits, visits)?,
                hash,
            },
            core,
            form,
        })
    }
    pub fn quote(&self) -> Quote {
        self.quote
    }
    fn head(&self) -> FrameHead<'a> {
        FrameHead {
            metadata: self.metadata,
            configuration: self.configuration,
            movement: self.movement,
            retention: self.retention,
            members: self.limits.members,
        }
    }
    /// Whether the Core root exceeds the inline bound and travels as seeds.
    pub fn seeded(&self) -> bool {
        matches!(self.form, Form::Seeded { .. })
    }
    pub fn header(&self) -> Result<Header, Error> {
        header(
            self.core.header()?,
            self.metadata,
            self.core.quote().bytes,
            self.quote.hash,
        )
    }
    pub fn write_into(&self, output: &mut [u8]) -> Result<ContentHash, Error> {
        if self.seeded() {
            return Err(Error::Seeded);
        }
        if output.len() != self.quote.bytes {
            return Err(Error::Capacity);
        }
        let mut offset = 0usize;
        self.write_with(|bytes| {
            let end = fields::add(offset, bytes.len())?;
            output
                .get_mut(offset..end)
                .ok_or(Error::Capacity)?
                .copy_from_slice(bytes);
            offset = end;
            Ok(())
        })
        .map_err(|error| match error {
            WriteError::Output(error) | WriteError::Codec(error) => error,
        })
    }
    /// The callback consumes complete borrowed chunks synchronously. It owns
    /// any separately funded coalescing buffer and its durability barrier.
    pub fn write_with<E>(
        &self,
        mut output: impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<ContentHash, WriteError<E>> {
        if self.seeded() {
            return Err(WriteError::Codec(Error::Seeded));
        }
        let mut sink = fields::Sink::new(self.quote.bytes, self.quote.visits, &mut output);
        let result = frame(&mut sink, &self.core, self.head());
        if let Some(error) = sink.error.take() {
            return Err(WriteError::Output(error));
        }
        let hash = result.map_err(WriteError::Codec)?;
        if hash != self.quote.hash
            || sink.length != self.quote.bytes
            || sink.used() != self.quote.visits
        {
            return Err(WriteError::Codec(Error::Invalid("encoding source")));
        }
        Ok(hash)
    }
    /// Encode into a funded buffer; a seeded plan first seals its chunks in
    /// `seeds` (25 §5), streaming the Core root through one chunk buffer, and
    /// the buffer then holds the manifest that names them.
    pub fn encode_in_seeded(
        &self,
        budget: &MemoryBudget,
        seeds: &mut SeedStore,
    ) -> Result<EncodedCheckpoint, Error> {
        let Form::Seeded { chunks } = self.form else {
            return self.encode_in(budget);
        };
        let core_bytes = self.core.quote().bytes;
        let table_bytes = fields::add(fields::mul(chunks, size_of::<SeedChunk>())?, ALLOCATION)?;
        let _table_charge = budget.reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            fields::add(table_bytes, fields::add(SEED_CHUNK_BYTES, ALLOCATION)?)?,
        )?;
        let mut table: Vec<SeedChunk> = Vec::new();
        table
            .try_reserve_exact(chunks)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .try_reserve_exact(SEED_CHUNK_BYTES)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let mut sealed = 0usize;
        let mut seal = |buffer: &mut Vec<u8>, table: &mut Vec<SeedChunk>| -> Result<(), Error> {
            if buffer.is_empty() {
                return Ok(());
            }
            if table.len() >= chunks {
                return Err(Error::Invalid("seed chunk count"));
            }
            let hash = seeds.install(buffer)?;
            table.push(SeedChunk {
                hash,
                length: u32::try_from(buffer.len()).map_err(|_| Error::Capacity)?,
            });
            sealed = fields::add(sealed, buffer.len())?;
            buffer.clear();
            Ok(())
        };
        self.core
            .write_with(|mut bytes| -> Result<(), Error> {
                while !bytes.is_empty() {
                    let room = SEED_CHUNK_BYTES.saturating_sub(buffer.len());
                    let take = room.min(bytes.len());
                    let (head, tail) = bytes.split_at_checked(take).ok_or(Error::Truncated)?;
                    buffer.extend_from_slice(head);
                    bytes = tail;
                    if buffer.len() == SEED_CHUNK_BYTES {
                        seal(&mut buffer, &mut table)?;
                    }
                }
                Ok(())
            })
            .map_err(|error| match error {
                root::WriteError::Output(error) => error,
                root::WriteError::Codec(error) => Error::Core(error),
            })?;
        seal(&mut buffer, &mut table)?;
        if sealed != core_bytes || table.len() != chunks {
            return Err(Error::Invalid("seeded core length"));
        }
        let allocation = budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                self.quote.output_charge,
            )?
            .commit();
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.quote.bytes)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if fields::add(bytes.capacity(), ALLOCATION)? > allocation.bytes() {
            return Err(Error::Capacity);
        }
        // The sink borrows the output buffer through the closure; both end
        // with this block so the buffer can be inspected and moved.
        let length = {
            let mut output = |chunk: &[u8]| -> Result<(), Error> {
                if fields::add(bytes.len(), chunk.len())? > self.quote.bytes {
                    return Err(Error::Capacity);
                }
                bytes.extend_from_slice(chunk);
                Ok(())
            };
            let mut sink = fields::Sink::new(self.quote.bytes, self.quote.visits, &mut output);
            let mut lookup = |index: usize| {
                table.get(index).copied().unwrap_or(SeedChunk {
                    hash: ContentHash([0; 32]),
                    length: 0,
                })
            };
            let result = frame_seeded(&mut sink, &self.core, self.head(), chunks, &mut lookup);
            if let Some(error) = sink.error.take() {
                return Err(error);
            }
            result?;
            sink.length
        };
        if length != self.quote.bytes || bytes.len() != self.quote.bytes {
            return Err(Error::Invalid("encoded length"));
        }
        Ok(EncodedCheckpoint { bytes, allocation })
    }
    pub fn encode_in(&self, budget: &MemoryBudget) -> Result<EncodedCheckpoint, Error> {
        if self.seeded() {
            return Err(Error::Seeded);
        }
        let allocation = budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                self.quote.output_charge,
            )?
            .commit();
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.quote.bytes)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if fields::add(bytes.capacity(), ALLOCATION)? > allocation.bytes() {
            return Err(Error::Capacity);
        }
        self.write_with(|chunk| {
            if fields::add(bytes.len(), chunk.len())? > self.quote.bytes {
                return Err(Error::Capacity);
            }
            bytes.extend_from_slice(chunk);
            Ok(())
        })
        .map_err(|error| match error {
            WriteError::Output(error) | WriteError::Codec(error) => error,
        })?;
        if bytes.len() != self.quote.bytes {
            return Err(Error::Invalid("encoded length"));
        }
        Ok(EncodedCheckpoint { bytes, allocation })
    }
}

fn header(
    core: root::CheckpointHeader,
    metadata: Metadata,
    bytes: usize,
    hash: ContentHash,
) -> Result<Header, Error> {
    Ok(Header {
        metadata,
        ledger: core.ledger,
        profile: core.profile,
        range: core.range,
        prefix: core.prefix,
        core_hash: core.hash,
        core_bytes: u64::try_from(bytes).map_err(|_| Error::Capacity)?,
        hash,
    })
}
fn validate(value: Header) -> Result<(), Error> {
    let meta = value.metadata;
    if meta.cluster == [0; 16]
        || meta.group == [0; 16]
        || value.ledger.tenant.is_zero()
        || value.ledger.session.is_zero()
        || (meta.applied_raft == 0) != (meta.applied_term == 0)
        || meta.configuration_index > meta.applied_raft
        || value.prefix.0 > meta.applied_raft
        || meta.records_floor > 1
        || meta.records_floor > value.prefix.0
        || meta.activation_index == 0
        || meta.activation_index > meta.applied_raft
        || (value.prefix.0 == meta.records_floor) != meta.recording_range.is_none()
        || (meta.recording_term == 0) != meta.recording_range.is_none()
        || meta.recording_term > meta.applied_term
        || meta.activation.decoder != format_hash()
        || meta.activation.durable_floor != meta.activation.decoder
        || meta.activation.genesis
            != genesis(
                meta.cluster,
                meta.group,
                value.ledger,
                value.profile,
                meta.activation.decoder,
            )
    {
        return Err(Error::Invalid(
            "identity, applied position or native genesis",
        ));
    }
    Ok(())
}

/// The length of chunk `index` of a `core_bytes` root cut into full chunks
/// and a remainder.
fn chunk_length(core_bytes: usize, index: usize) -> u32 {
    let start = index.saturating_mul(SEED_CHUNK_BYTES);
    let length = core_bytes.saturating_sub(start).min(SEED_CHUNK_BYTES);
    u32::try_from(length).unwrap_or(u32::MAX)
}

/// Everything a frame's head is written from: the metadata, the membership
/// configuration, the optional movement section and the member bound.
#[derive(Clone, Copy)]
struct FrameHead<'a> {
    metadata: Metadata,
    configuration: &'a MembershipConfiguration,
    movement: Option<&'a [u8]>,
    retention: Option<RetentionSection>,
    members: usize,
}

fn frame<F, E>(
    sink: &mut fields::Sink<'_, F, E>,
    core: &root::EncodingPlan<'_>,
    head: FrameHead<'_>,
) -> Result<ContentHash, Error>
where
    F: FnMut(&[u8]) -> Result<(), E>,
{
    let value = frame_head(sink, core, head)?;
    sink.u8(0)?;
    sink.u64(value.core_bytes)?;
    sink.raw(&value.core_hash.0)?;
    // The nested encoder's complete work is additional to outer hashing/output.
    sink.visit(core.quote().visits)?;
    core.write_with(|bytes| sink.raw(bytes))
        .map_err(|error| match error {
            root::WriteError::Output(error) => error,
            root::WriteError::Codec(error) => Error::Core(error),
        })?;
    let hash = ContentHash(*sink.hash.finalize().as_bytes());
    sink.trailer(&hash.0)?;
    Ok(hash)
}

/// The seeded frame: the same head, then the chunk table instead of the root.
fn frame_seeded<F, E>(
    sink: &mut fields::Sink<'_, F, E>,
    core: &root::EncodingPlan<'_>,
    head: FrameHead<'_>,
    chunks: usize,
    sealed: &mut dyn FnMut(usize) -> SeedChunk,
) -> Result<ContentHash, Error>
where
    F: FnMut(&[u8]) -> Result<(), E>,
{
    let value = frame_head(sink, core, head)?;
    sink.u8(1)?;
    sink.u64(value.core_bytes)?;
    sink.raw(&value.core_hash.0)?;
    sink.u32(u32::try_from(chunks).map_err(|_| Error::Capacity)?)?;
    sink.visit(fields::mul(chunks, 4)?)?;
    let mut total = 0u64;
    for index in 0..chunks {
        let chunk = sealed(index);
        if chunk.length == 0
            || usize::try_from(chunk.length).map_err(|_| Error::Capacity)? > SEED_CHUNK_BYTES
        {
            return Err(Error::Invalid("seed chunk length"));
        }
        sink.raw(&chunk.hash.0)?;
        sink.u32(chunk.length)?;
        total = total
            .checked_add(u64::from(chunk.length))
            .ok_or(Error::Capacity)?;
    }
    if total != value.core_bytes {
        return Err(Error::Invalid("seed chunk lengths"));
    }
    let hash = ContentHash(*sink.hash.finalize().as_bytes());
    sink.trailer(&hash.0)?;
    Ok(hash)
}

/// Every field before the Core root, shared by both forms.
fn frame_head<F, E>(
    sink: &mut fields::Sink<'_, F, E>,
    core: &root::EncodingPlan<'_>,
    head: FrameHead<'_>,
) -> Result<Header, Error>
where
    F: FnMut(&[u8]) -> Result<(), E>,
{
    let FrameHead {
        metadata,
        configuration,
        movement,
        retention,
        members: member_limit,
    } = head;
    sink.visit(2048)?; // Fixed fields, format/genesis hashes, digest setup/finalize.
    let value = header(
        core.header()?,
        metadata,
        core.quote().bytes,
        ContentHash([0; 32]),
    )?;
    validate(value)?;
    fields::check_configuration(configuration, member_limit, |work| sink.visit(work))?;
    sink.raw(&MAGIC)?;
    sink.u16(VERSION)?;
    sink.raw(&metadata.cluster)?;
    sink.raw(&metadata.group)?;
    sink.raw(&value.ledger.tenant.0)?;
    sink.raw(&value.ledger.session.0)?;
    sink.u8(fields::profile(value.profile))?;
    sink.raw(&value.range.0.to_le_bytes())?;
    sink.u64(value.prefix.0)?;
    sink.u64(metadata.applied_raft)?;
    sink.u64(metadata.applied_term)?;
    sink.u64(metadata.configuration_index)?;
    match metadata.recording_range {
        Some(range) => {
            sink.u8(1)?;
            sink.raw(&range.0.to_le_bytes())?;
        }
        None => sink.u8(0)?,
    }
    sink.u64(metadata.recording_term)?;
    sink.u64(metadata.records_floor)?;
    sink.u64(metadata.activation_index)?;
    sink.u8(0)?; // Native genesis; no replicated legacy-to-native transition.
    sink.raw(&metadata.activation.decoder.0)?;
    sink.raw(&metadata.activation.durable_floor.0)?;
    sink.raw(&metadata.activation.genesis.0)?;
    // Ancillary byte 0 says whether a movement section follows the
    // configuration (25 §6), byte 1 whether a retention section follows
    // that (26 §3); the other four stay reserved.
    match metadata.ancillary {
        AncillaryProfile::NativeOnlyV1 => {
            sink.raw(&[
                u8::from(movement.is_some()),
                u8::from(retention.is_some()),
                0,
                0,
                0,
                0,
            ])?;
        }
    }
    fields::write_configuration(sink, configuration)?;
    if let Some(bytes) = movement {
        sink.u32(u32::try_from(bytes.len()).map_err(|_| Error::Capacity)?)?;
        sink.visit(bytes.len())?;
        sink.raw(bytes)?;
    }
    if let Some(section) = retention {
        sink.u64(section.archived_through.0)?;
        sink.u64(section.retired_families)?;
    }
    Ok(value)
}

#[cfg(test)]
#[path = "native_checkpoint_tests.rs"]
mod tests;
