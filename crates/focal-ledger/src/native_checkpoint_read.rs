use super::*;
use fields::{Configuration, Cursor, add, mul};
use std::cell::Cell;

#[derive(Clone, Copy)]
struct Members<'a> {
    bytes: &'a [u8],
    count: usize,
}
struct ConfigurationView<'a> {
    parts: [Members<'a>; 4],
    auto_leave: bool,
}
impl Configuration for ConfigurationView<'_> {
    fn count(&self, part: usize) -> Result<usize, Error> {
        self.parts
            .get(part)
            .map(|value| value.count)
            .ok_or(Error::Invalid("membership part"))
    }
    fn member(&self, part: usize, index: usize) -> Result<u64, Error> {
        let part = self
            .parts
            .get(part)
            .ok_or(Error::Invalid("membership part"))?;
        let start = mul(index, 8)?;
        let bytes = part
            .bytes
            .get(start..add(start, 8)?)
            .ok_or(Error::Invalid("membership index"))?;
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_| Error::Truncated)?,
        ))
    }
    fn auto_leave(&self) -> bool {
        self.auto_leave
    }
}

/// Everything before the Core root, parsed once for either form.
struct Head<'a> {
    header: Header,
    configuration: ConfigurationView<'a>,
    /// `0` inline, `1` seeded.
    form: u8,
    /// The inline Core root, or the seed chunk table.
    body: &'a [u8],
    chunks: usize,
    /// The movement section (25 §6), empty when the frame carries none.
    movement: &'a [u8],
    /// The retention section (26 §3), when the frame carries one.
    retention: Option<RetentionSection>,
    remaining: usize,
}

fn parse(bytes: &[u8], limits: Limits) -> Result<Head<'_>, Error> {
    if bytes.len() > limits.bytes {
        return Err(Error::Capacity);
    }
    let end = bytes.len().checked_sub(32).ok_or(Error::Truncated)?;
    let (payload, trailer) = bytes.split_at_checked(end).ok_or(Error::Truncated)?;
    let hash_work = add(payload.len(), 2402)?;
    let remaining = limits
        .visits
        .checked_sub(hash_work)
        .ok_or(Error::Capacity)?;
    let mut cursor = Cursor::new(payload, remaining);
    if cursor.fixed::<8>()? != MAGIC || cursor.u16()? != VERSION {
        return Err(Error::Invalid("native Session format"));
    }
    let cluster = cursor.fixed()?;
    let group = cursor.fixed()?;
    let ledger = LedgerId {
        tenant: focal_model::TenantId(cursor.fixed()?),
        session: focal_model::SessionId(cursor.fixed()?),
    };
    let profile = match cursor.u8()? {
        0 => NativeContentProfile::ProjectionOnly,
        1 => NativeContentProfile::AuthoredV1,
        _ => return Err(Error::Invalid("content profile")),
    };
    let range = RangeId(u128::from_le_bytes(cursor.fixed()?));
    let prefix = SessionSeq(cursor.u64()?);
    let applied_raft = cursor.u64()?;
    let applied_term = cursor.u64()?;
    let configuration_index = cursor.u64()?;
    let recording_range = match cursor.u8()? {
        0 => None,
        1 => Some(RangeId(u128::from_le_bytes(cursor.fixed()?))),
        _ => return Err(Error::Invalid("recording range option")),
    };
    let recording_term = cursor.u64()?;
    let records_floor = cursor.u64()?;
    let activation_index = cursor.u64()?;
    if cursor.u8()? != 0 {
        return Err(Error::Invalid("non-genesis activation"));
    }
    let activation = Activation {
        decoder: ContentHash(cursor.fixed()?),
        durable_floor: ContentHash(cursor.fixed()?),
        genesis: ContentHash(cursor.fixed()?),
    };
    let ancillary_bytes = cursor.fixed::<6>()?;
    let (with_movement, with_retention) = match ancillary_bytes {
        [movement @ (0 | 1), retention @ (0 | 1), 0, 0, 0, 0] => (movement == 1, retention == 1),
        _ => return Err(Error::Invalid("legacy ancillary state is unsupported")),
    };
    let ancillary = AncillaryProfile::NativeOnlyV1;
    cursor.charge(5)?;
    let mut parts = [Members {
        bytes: &[],
        count: 0,
    }; 4];
    let mut member_count = 0usize;
    for part in &mut parts {
        let count = usize::try_from(cursor.u32()?).map_err(|_| Error::Capacity)?;
        member_count = add(member_count, count)?;
        if count > 1024 || member_count > limits.members || member_count > 2048 {
            return Err(Error::Capacity);
        }
        *part = Members {
            bytes: cursor.take(mul(count, 8)?)?,
            count,
        };
    }
    let configuration = ConfigurationView {
        parts,
        auto_leave: cursor.boolean()?,
    };
    fields::check_configuration(&configuration, limits.members, |work| cursor.charge(work))?;
    let movement: &[u8] = if with_movement {
        let length = usize::try_from(cursor.u32()?).map_err(|_| Error::Capacity)?;
        if length == 0 || length > limits.movement_bytes {
            return Err(Error::Invalid("movement section"));
        }
        cursor.charge(length)?;
        cursor.take(length)?
    } else {
        &[]
    };
    let retention = if with_retention {
        Some(RetentionSection {
            archived_through: SessionSeq(cursor.u64()?),
            retired_families: cursor.u64()?,
        })
    } else {
        None
    };
    let form = cursor.u8()?;
    let encoded_core_bytes = cursor.u64()?;
    let count = usize::try_from(encoded_core_bytes).map_err(|_| Error::Capacity)?;
    let core_hash = ContentHash(cursor.fixed()?);
    let (body, chunks) = match form {
        0 => (cursor.take(count)?, 0),
        1 => {
            let chunks = usize::try_from(cursor.u32()?).map_err(|_| Error::Capacity)?;
            if chunks == 0
                || chunks > limits.max_seed_chunks()
                || count > limits.assembled_bytes
                || count.div_ceil(SEED_CHUNK_BYTES) != chunks
            {
                return Err(Error::Invalid("seed chunk table"));
            }
            cursor.charge(mul(chunks, 4)?)?;
            (cursor.take(mul(chunks, SEED_CHUNK_ENTRY)?)?, chunks)
        }
        _ => return Err(Error::Invalid("core root form")),
    };
    cursor.finish()?;
    let mut hash = blake3::Hasher::new_derive_key(HASH_DOMAIN);
    hash.update(payload);
    let hash = ContentHash(*hash.finalize().as_bytes());
    if hash.0 != trailer {
        return Err(Error::Invalid("Session checksum"));
    }
    let header = Header {
        metadata: Metadata {
            cluster,
            group,
            applied_raft,
            applied_term,
            configuration_index,
            recording_range,
            recording_term,
            records_floor,
            activation_index,
            activation,
            ancillary,
        },
        ledger,
        profile,
        range,
        prefix,
        core_hash,
        core_bytes: encoded_core_bytes,
        hash,
    };
    validate(header)?;
    Ok(Head {
        header,
        configuration,
        form,
        body,
        chunks,
        movement,
        retention,
        remaining: cursor.remaining,
    })
}

/// Why a seeded Core root could not be assembled.
#[derive(Debug, thiserror::Error)]
pub enum SeedError {
    /// A named chunk is not in the seed store yet; pull it and retry.
    #[error("checkpoint seed chunk {0} is not local")]
    Missing(ContentHash),
    #[error("checkpoint seed store: {0}")]
    Seeds(ContentError),
    #[error("checkpoint seed assembly memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("checkpoint seed table: {0}")]
    Invalid(&'static str),
}

/// The Core root of a seeded checkpoint, assembled and charged.
pub struct AssembledCore {
    bytes: Vec<u8>,
    _allocation: Allocation,
}
impl AssembledCore {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// The checksummed frame of a seeded checkpoint: its header and the table
/// of chunks that make up its Core root.
pub struct SeedManifest<'a> {
    header: Header,
    table: &'a [u8],
    chunks: usize,
    movement: &'a [u8],
    retention: Option<RetentionSection>,
}
impl<'a> SeedManifest<'a> {
    pub fn header(&self) -> Header {
        self.header
    }
    /// The movement section the frame carries (25 §6), empty when none.
    pub fn movement(&self) -> &'a [u8] {
        self.movement
    }
    /// The retention section the frame carries (26 §3), when any.
    pub fn retention(&self) -> Option<RetentionSection> {
        self.retention
    }
    pub fn len(&self) -> usize {
        self.chunks
    }
    pub fn is_empty(&self) -> bool {
        self.chunks == 0
    }
    /// The chunks in order.
    pub fn chunks(&self) -> impl Iterator<Item = Result<SeedChunk, Error>> + '_ {
        (0..self.chunks).map(move |index| {
            let start = mul(index, SEED_CHUNK_ENTRY)?;
            let entry = self
                .table
                .get(start..add(start, SEED_CHUNK_ENTRY)?)
                .ok_or(Error::Truncated)?;
            let hash: [u8; 32] = entry
                .get(..32)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or(Error::Truncated)?;
            let length: [u8; 4] = entry
                .get(32..36)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or(Error::Truncated)?;
            Ok(SeedChunk {
                hash: ContentHash(hash),
                length: u32::from_le_bytes(length),
            })
        })
    }
    /// The chunks `seeds` lacks, in order; empty when every chunk is local.
    pub fn missing(&self, seeds: &SeedReader) -> Result<Vec<ContentHash>, Error> {
        let mut missing = Vec::new();
        for chunk in self.chunks() {
            let chunk = chunk?;
            if !seeds.contains(chunk.hash) {
                missing.try_reserve(1).map_err(|_| Error::Capacity)?;
                missing.push(chunk.hash);
            }
        }
        Ok(missing)
    }
    /// Assemble the Core root from `seeds`, every chunk verified by its hash
    /// and its recorded length, into one buffer charged to `budget`.
    pub fn assemble(
        &self,
        seeds: &SeedReader,
        budget: &MemoryBudget,
    ) -> Result<AssembledCore, SeedError> {
        self.assemble_with(|hash, length| seeds.read(hash, length), budget)
    }
    /// `assemble` over any chunk source: a backup's seed files (26 §6) or a
    /// replica's store.
    pub fn assemble_with(
        &self,
        read: impl Fn(ContentHash, usize) -> Result<Vec<u8>, ContentError>,
        budget: &MemoryBudget,
    ) -> Result<AssembledCore, SeedError> {
        let total = usize::try_from(self.header.core_bytes)
            .map_err(|_| SeedError::Invalid("core length"))?;
        let allocation = budget
            .reserve(
                BudgetKind::Recovery,
                BudgetLane::Completion,
                add(total, ALLOCATION).map_err(|_| SeedError::Invalid("core length"))?,
            )?
            .commit();
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total)
            .map_err(|_| MemoryError::AllocationFailed)?;
        for chunk in self.chunks() {
            let chunk = chunk.map_err(|_| SeedError::Invalid("chunk table"))?;
            let length =
                usize::try_from(chunk.length).map_err(|_| SeedError::Invalid("chunk length"))?;
            let read = read(chunk.hash, length).map_err(|error| match error {
                ContentError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
                    SeedError::Missing(chunk.hash)
                }
                other => SeedError::Seeds(other),
            })?;
            if read.len() != length
                || bytes
                    .len()
                    .checked_add(length)
                    .is_none_or(|next| next > total)
            {
                return Err(SeedError::Invalid("chunk length"));
            }
            bytes.extend_from_slice(&read);
        }
        if bytes.len() != total {
            return Err(SeedError::Invalid("core length"));
        }
        Ok(AssembledCore {
            bytes,
            _allocation: allocation,
        })
    }
}

/// Checksummed enclosing metadata and a structurally checked borrowed Core root.
/// Core row hydration and local artifact custody remain mandatory before publish.
/// No raw metadata is defaulted from an absent legacy envelope.
pub struct Checkpoint<'a> {
    header: Header,
    core: root::StructuralCheckpoint<'a>,
    core_bytes: &'a [u8],
    configuration: ConfigurationView<'a>,
    movement: &'a [u8],
    retention: Option<RetentionSection>,
    remaining: Cell<usize>,
    initial_visits: usize,
    members: usize,
}
impl<'a> Checkpoint<'a> {
    /// Parse the frame up to its form (25 §5): a seeded frame yields the
    /// manifest its Core root is assembled from; `None` is an inline frame,
    /// inspected through to its root by `inspect`. The header is validated
    /// either way.
    pub fn describe(bytes: &'a [u8], limits: Limits) -> Result<Option<SeedManifest<'a>>, Error> {
        let head = parse(bytes, limits)?;
        match head.form {
            0 => Ok(None),
            _ => Ok(Some(SeedManifest {
                header: head.header,
                table: head.body,
                chunks: head.chunks,
                movement: head.movement,
                retention: head.retention,
            })),
        }
    }
    /// An inline frame; a seeded one is refused with `Error::Seeded`.
    pub fn inspect(bytes: &'a [u8], limits: Limits) -> Result<Self, Error> {
        let head = parse(bytes, limits)?;
        if head.form != 0 {
            return Err(Error::Seeded);
        }
        let body = head.body;
        Self::finish(head, body, limits)
    }
    /// A seeded frame with its Core root assembled by the caller from the
    /// chunks the manifest names.
    pub fn inspect_seeded(bytes: &'a [u8], core: &'a [u8], limits: Limits) -> Result<Self, Error> {
        let head = parse(bytes, limits)?;
        if head.form != 1 {
            return Err(Error::Invalid("core root form"));
        }
        if u64::try_from(core.len()).map_err(|_| Error::Capacity)? != head.header.core_bytes {
            return Err(Error::Invalid("assembled core length"));
        }
        Self::finish(head, core, limits)
    }
    fn finish(head: Head<'a>, core_bytes: &'a [u8], limits: Limits) -> Result<Self, Error> {
        let Head {
            header,
            configuration,
            remaining,
            movement,
            retention,
            ..
        } = head;
        let count = usize::try_from(header.core_bytes).map_err(|_| Error::Capacity)?;
        let core = root::StructuralCheckpoint::inspect(
            core_bytes,
            record_codec::InspectionLimits {
                bytes: count,
                visits: remaining,
                rows: limits.rows,
                row_bytes: limits.row_bytes,
            },
        )?;
        let remaining = remaining
            .checked_sub(core.quote().visits)
            .ok_or(Error::Capacity)?;
        let actual = core.header();
        if actual.ledger != header.ledger
            || actual.profile != header.profile
            || actual.range != header.range
            || actual.prefix != header.prefix
            || actual.hash != header.core_hash
        {
            return Err(Error::Invalid("nested Core identity"));
        }
        Ok(Self {
            header,
            core,
            core_bytes,
            configuration,
            movement,
            retention,
            remaining: Cell::new(remaining),
            initial_visits: limits.visits,
            members: limits.members,
        })
    }
    pub fn header(&self) -> Header {
        self.header
    }
    /// The movement section the frame carries (25 §6), empty when none.
    pub fn movement(&self) -> &'a [u8] {
        self.movement
    }
    /// The retention section the frame carries (26 §3), when any.
    pub fn retention(&self) -> Option<RetentionSection> {
        self.retention
    }
    pub fn core(&self) -> &root::StructuralCheckpoint<'a> {
        &self.core
    }
    pub fn core_bytes(&self) -> &'a [u8] {
        self.core_bytes
    }
    pub fn visits_used(&self) -> usize {
        self.initial_visits.saturating_sub(self.remaining.get())
    }
    pub fn remaining_visits(&self) -> usize {
        self.remaining.get()
    }
    fn charge(&self, amount: usize) -> Result<(), Error> {
        self.remaining.set(
            self.remaining
                .get()
                .checked_sub(amount)
                .ok_or(Error::Capacity)?,
        );
        Ok(())
    }
    /// Compare directly against consensus's actual applied snapshot configuration.
    /// Repeated checks spend the same retained allowance; no decoded Vec is built.
    pub fn configuration_matches(&self, actual: &MembershipConfiguration) -> Result<(), Error> {
        fields::check_configuration(actual, self.members, |work| self.charge(work))?;
        self.charge(5)?;
        if self.configuration.auto_leave != actual.auto_leave {
            return Err(Error::Invalid("snapshot membership"));
        }
        for part in 0..4 {
            let count = self.configuration.count(part)?;
            self.charge(add(mul(count, 4)?, 1)?)?;
            if count != actual.count(part)? {
                return Err(Error::Invalid("snapshot membership"));
            }
            for index in 0..count {
                if self.configuration.member(part, index)? != actual.member(part, index)? {
                    return Err(Error::Invalid("snapshot membership"));
                }
            }
        }
        Ok(())
    }
}
