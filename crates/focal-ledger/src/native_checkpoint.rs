//! Enclosing native-genesis Session checkpoints. This format is separate from
//! both the native Core root and every frozen legacy Session format. It records
//! physical group identity, exact applied Raft coordinates, membership and the
//! native recording-range mapping. Cursor/delta/managed-stream/placement state
//! is explicitly disabled; populated legacy Session import is not supported.
use focal_consensus::MembershipConfiguration;
use focal_core::native::{NativeContentProfile, NativeState, record_codec};
use focal_core::Core;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError, RangeId};
use focal_model::{ContentHash, LedgerId, SessionSeq};
use record_codec::checkpoint as root;

#[path = "native_checkpoint_fields.rs"]
mod fields;
#[path = "native_checkpoint_read.rs"]
mod read;
pub use read::Checkpoint;

pub const MAGIC: [u8; 8] = *b"FCNSESS1";
pub const VERSION: u16 = 1;
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
}

/// The compiled native-genesis decoder covers this envelope and both nested
/// native codecs. It does not advertise a legacy successor or authorize import.
pub fn format_hash() -> ContentHash {
    let mut hash = blake3::Hasher::new_derive_key("focal.native.session.decoder.v1");
    hash.update(b"native-genesis;membership;applied-index-term;recording-range;disabled-cursor-delta-managed-placement;no-legacy-import");
    hash.update(&MAGIC); hash.update(&VERSION.to_le_bytes());
    hash.update(&record_codec::MAGIC); hash.update(&record_codec::VERSION.to_le_bytes());
    hash.update(&root::MAGIC); hash.update(&root::VERSION.to_le_bytes());
    ContentHash(*hash.finalize().as_bytes())
}

/// Stable group identity is independent of each process's fresh RangeId.
pub fn genesis(
    cluster: [u8; 16], group: [u8; 16], ledger: LedgerId,
    profile: NativeContentProfile, decoder: ContentHash,
) -> ContentHash {
    let mut hash = blake3::Hasher::new_derive_key("focal.native.session.genesis.v1");
    hash.update(&cluster); hash.update(&group);
    hash.update(&ledger.tenant.0); hash.update(&ledger.session.0);
    hash.update(&[fields::profile(profile)]); hash.update(&decoder.0);
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
pub enum AncillaryProfile { NativeOnlyV1 }
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
}
impl Default for Limits {
    fn default() -> Self {
        Self { bytes: 8 * 1024 * 1024, visits: 256 * 1024 * 1024,
            members: 2048, rows: 100_000, row_bytes: 8 * 1024 * 1024 }
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

pub struct EncodingPlan<'a> {
    core: root::EncodingPlan<'a>,
    metadata: Metadata,
    configuration: &'a MembershipConfiguration,
    limits: Limits,
    quote: Quote,
}

/// Bytes and their original output permit move together into persistence. The
/// buffer is dropped before its permit, including on refused persistence.
pub struct EncodedCheckpoint { bytes: Vec<u8>, allocation: Allocation }
impl EncodedCheckpoint {
    pub fn bytes(&self) -> &[u8] { &self.bytes }
    pub fn charge(&self) -> usize { self.allocation.bytes() }
    pub fn into_parts(self) -> (Vec<u8>, Allocation) { (self.bytes, self.allocation) }
}
#[derive(Debug)]
pub enum WriteError<E> { Output(E), Codec(Error) }
impl<E: std::fmt::Display> std::fmt::Display for WriteError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self { Self::Output(error) => write!(f, "checkpoint output: {error}"), Self::Codec(error) => error.fmt(f) }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for WriteError<E> {}

impl<'a> EncodingPlan<'a> {
    pub fn prepare(core: &'a Core<NativeState>, metadata: Metadata,
        configuration: &'a MembershipConfiguration, limits: Limits) -> Result<Self, Error> {
        let core = root::EncodingPlan::prepare(core, record_codec::EncodingLimits {
            bytes: limits.bytes, visits: limits.visits, rows: limits.rows,
        })?;
        let available = limits.visits.checked_sub(core.quote().visits).ok_or(Error::Capacity)?;
        let mut discard = |_: &[u8]| -> Result<(), std::convert::Infallible> { Ok(()) };
        let mut sink = fields::Sink::new(limits.bytes, available, &mut discard);
        let hash = frame(&mut sink, &core, metadata, configuration, limits.members)?;
        let bytes = sink.length;
        let visits = sink.used();
        Ok(Self { metadata, configuration, limits,
            quote: Quote { bytes, output_charge: fields::add(bytes, ALLOCATION)?, visits,
                preparation_visits: fields::add(core.quote().visits, visits)?, hash }, core })
    }
    pub fn quote(&self) -> Quote { self.quote }
    pub fn header(&self) -> Result<Header, Error> {
        header(self.core.header()?, self.metadata, self.core.quote().bytes, self.quote.hash)
    }
    pub fn write_into(&self, output: &mut [u8]) -> Result<ContentHash, Error> {
        if output.len() != self.quote.bytes { return Err(Error::Capacity); }
        let mut offset = 0usize;
        self.write_with(|bytes| {
            let end = fields::add(offset, bytes.len())?;
            output.get_mut(offset..end).ok_or(Error::Capacity)?.copy_from_slice(bytes);
            offset = end; Ok(())
        }).map_err(|error| match error { WriteError::Output(error) | WriteError::Codec(error) => error })
    }
    /// The callback consumes complete borrowed chunks synchronously. It owns
    /// any separately funded coalescing buffer and its durability barrier.
    pub fn write_with<E>(&self, mut output: impl FnMut(&[u8]) -> Result<(), E>) -> Result<ContentHash, WriteError<E>> {
        let mut sink = fields::Sink::new(self.quote.bytes, self.quote.visits, &mut output);
        let result = frame(&mut sink, &self.core, self.metadata, self.configuration, self.limits.members);
        if let Some(error) = sink.error.take() { return Err(WriteError::Output(error)); }
        let hash = result.map_err(WriteError::Codec)?;
        if hash != self.quote.hash || sink.length != self.quote.bytes || sink.used() != self.quote.visits {
            return Err(WriteError::Codec(Error::Invalid("encoding source")));
        }
        Ok(hash)
    }
    pub fn encode_in(&self, budget: &MemoryBudget) -> Result<EncodedCheckpoint, Error> {
        let allocation = budget.reserve(BudgetKind::Recovery, BudgetLane::Completion, self.quote.output_charge)?.commit();
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(self.quote.bytes).map_err(|_| MemoryError::AllocationFailed)?;
        if fields::add(bytes.capacity(), ALLOCATION)? > allocation.bytes() { return Err(Error::Capacity); }
        self.write_with(|chunk| {
            if fields::add(bytes.len(), chunk.len())? > self.quote.bytes { return Err(Error::Capacity); }
            bytes.extend_from_slice(chunk); Ok(())
        }).map_err(|error| match error { WriteError::Output(error) | WriteError::Codec(error) => error })?;
        if bytes.len() != self.quote.bytes { return Err(Error::Invalid("encoded length")); }
        Ok(EncodedCheckpoint { bytes, allocation })
    }
}

fn header(core: root::CheckpointHeader, metadata: Metadata, bytes: usize, hash: ContentHash) -> Result<Header, Error> {
    Ok(Header { metadata, ledger: core.ledger, profile: core.profile, range: core.range, prefix: core.prefix,
        core_hash: core.hash, core_bytes: u64::try_from(bytes).map_err(|_| Error::Capacity)?, hash })
}
fn validate(value: Header) -> Result<(), Error> {
    let meta = value.metadata;
    if meta.cluster == [0; 16] || meta.group == [0; 16] || value.ledger.tenant.is_zero() || value.ledger.session.is_zero()
        || (meta.applied_raft == 0) != (meta.applied_term == 0) || meta.configuration_index > meta.applied_raft
        || value.prefix.0 > meta.applied_raft || (value.prefix.0 == 0) != meta.recording_range.is_none()
        || (meta.recording_term == 0) != meta.recording_range.is_none() || meta.recording_term > meta.applied_term
        || meta.activation.decoder != format_hash() || meta.activation.durable_floor != meta.activation.decoder
        || meta.activation.genesis != genesis(meta.cluster, meta.group, value.ledger, value.profile, meta.activation.decoder)
    { return Err(Error::Invalid("identity, applied position or native genesis")); }
    Ok(())
}

fn frame<F, E>(sink: &mut fields::Sink<'_, F, E>, core: &root::EncodingPlan<'_>, metadata: Metadata,
    configuration: &MembershipConfiguration, member_limit: usize) -> Result<ContentHash, Error>
where F: FnMut(&[u8]) -> Result<(), E> {
    sink.visit(2048)?; // Fixed fields, format/genesis hashes, digest setup/finalize.
    let value = header(core.header()?, metadata, core.quote().bytes, ContentHash([0; 32]))?;
    validate(value)?;
    fields::check_configuration(configuration, member_limit, |work| sink.visit(work))?;
    sink.raw(&MAGIC)?; sink.u16(VERSION)?;
    sink.raw(&metadata.cluster)?; sink.raw(&metadata.group)?;
    sink.raw(&value.ledger.tenant.0)?; sink.raw(&value.ledger.session.0)?;
    sink.u8(fields::profile(value.profile))?;
    sink.raw(&value.range.0.to_le_bytes())?; sink.u64(value.prefix.0)?;
    sink.u64(metadata.applied_raft)?; sink.u64(metadata.applied_term)?; sink.u64(metadata.configuration_index)?;
    match metadata.recording_range {
        Some(range) => { sink.u8(1)?; sink.raw(&range.0.to_le_bytes())?; },
        None => sink.u8(0)?,
    }
    sink.u64(metadata.recording_term)?;
    sink.u8(0)?; // Native genesis; no replicated legacy-to-native transition.
    sink.raw(&metadata.activation.decoder.0)?; sink.raw(&metadata.activation.durable_floor.0)?; sink.raw(&metadata.activation.genesis.0)?;
    match metadata.ancillary { AncillaryProfile::NativeOnlyV1 => sink.raw(&[0, 0, 0, 0, 0, 0])?, }
    fields::write_configuration(sink, configuration)?;
    sink.u64(value.core_bytes)?; sink.raw(&value.core_hash.0)?;
    // The nested encoder's complete work is additional to outer hashing/output.
    sink.visit(core.quote().visits)?;
    core.write_with(|bytes| sink.raw(bytes)).map_err(|error| match error {
        root::WriteError::Output(error) => error, root::WriteError::Codec(error) => Error::Core(error),
    })?;
    let hash = ContentHash(*sink.hash.finalize().as_bytes());
    sink.trailer(&hash.0)?; Ok(hash)
}

#[cfg(test)]
#[path = "native_checkpoint_tests.rs"]
mod tests;
