//! The archive bundle reader (26 §4): the structural check of an `FCNARCHV`
//! frame — magic, version, profile, ledger, the prefix it claims, the root,
//! every member, the row count, every row's key order and family, and the
//! trailing digest — before anything in it is trusted. A bundle is never
//! restored as a core; it is read, and what it says about itself is
//! verified against what it holds.
use super::checkpoint::{ARCHIVE_HASH_DOMAIN, ARCHIVE_MAGIC, ARCHIVE_VERSION};
use super::inspect::{EncodedRow, InspectionLimits, RecordRows};
use super::*;
use bytes::Cursor;

/// What an archive bundle declares about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveHeader {
    pub ledger: LedgerId,
    pub profile: NativeContentProfile,
    /// The prefix the bundle claims: every event of the family is at or
    /// below it.
    pub through: SessionSeq,
    pub root: ClaimId,
    /// The root first, then every owned claim the bundle holds.
    pub members: Vec<ClaimId>,
    /// The content roots of the family's artifacts held as content objects,
    /// sorted: what custody must keep for the bundle's proof (26 §5).
    pub content: Vec<ContentHash>,
    /// The content roots of the objects every replica sealed at admission
    /// for the family's artifacts held inline, sorted.
    pub inline: Vec<ContentHash>,
    /// Rows in the bundle.
    pub count: usize,
}

/// A structurally verified bundle: its header, its digest, and its rows.
pub struct StructuralArchive<'a> {
    header: ArchiveHeader,
    digest: ContentHash,
    rows: &'a [u8],
    row_limit: usize,
    visits: usize,
}

impl<'a> StructuralArchive<'a> {
    pub fn inspect(bytes: &'a [u8], limits: InspectionLimits) -> Result<Self, CodecError> {
        if bytes.len() > limits.bytes {
            return Err(CodecError::Capacity);
        }
        let at = bytes.len().checked_sub(32).ok_or(CodecError::Truncated)?;
        let (payload, trailer) = bytes.split_at_checked(at).ok_or(CodecError::Truncated)?;
        let hash_visits = payload.len().checked_add(322).ok_or(CodecError::Capacity)?;
        let available = limits
            .visits
            .checked_sub(hash_visits)
            .ok_or(CodecError::Capacity)?;
        let mut hasher = blake3::Hasher::new_derive_key(ARCHIVE_HASH_DOMAIN);
        hasher.update(payload);
        let digest = ContentHash(*hasher.finalize().as_bytes());
        if digest.0 != trailer {
            return Err(CodecError::InvalidTag("archive digest"));
        }
        let mut cursor = Cursor::new(payload, limits.bytes, available)?;
        if cursor.fixed::<8>()? != ARCHIVE_MAGIC || cursor.u16()? != ARCHIVE_VERSION {
            return Err(CodecError::InvalidTag("archive format"));
        }
        let profile = match cursor.u8()? {
            0 => NativeContentProfile::ProjectionOnly,
            1 => NativeContentProfile::AuthoredV1,
            _ => return Err(CodecError::InvalidTag("archive profile")),
        };
        let ledger = fixed::read_ledger(&mut cursor)?;
        let through = SessionSeq(cursor.u64()?);
        let root = ClaimId(cursor.fixed()?);
        let member_count = usize::try_from(cursor.u32()?).map_err(|_| CodecError::Capacity)?;
        if member_count == 0 || member_count > super::super::retirement::MAX_FAMILY_MEMBERS {
            return Err(CodecError::InvalidTag("archive members"));
        }
        cursor.visit(member_count)?;
        let mut members = Vec::new();
        members
            .try_reserve_exact(member_count)
            .map_err(|_| CodecError::Capacity)?;
        for _ in 0..member_count {
            let member = ClaimId(cursor.fixed()?);
            if member.is_zero() || members.contains(&member) {
                return Err(CodecError::InvalidTag("archive member"));
            }
            members.push(member);
        }
        let content_count = usize::try_from(cursor.u32()?).map_err(|_| CodecError::Capacity)?;
        if content_count > super::super::retirement::MAX_FAMILY_ROWS {
            return Err(CodecError::InvalidTag("archive content"));
        }
        cursor.visit(content_count)?;
        let mut content = Vec::new();
        content
            .try_reserve_exact(content_count)
            .map_err(|_| CodecError::Capacity)?;
        for _ in 0..content_count {
            let root = ContentHash(cursor.fixed()?);
            if root.0 == [0; 32] || content.last().is_some_and(|last| *last >= root) {
                return Err(CodecError::InvalidTag("archive content order"));
            }
            content.push(root);
        }
        let inline_count = usize::try_from(cursor.u32()?).map_err(|_| CodecError::Capacity)?;
        if inline_count > super::super::retirement::MAX_FAMILY_ROWS {
            return Err(CodecError::InvalidTag("archive inline"));
        }
        cursor.visit(inline_count)?;
        let mut inline = Vec::new();
        inline
            .try_reserve_exact(inline_count)
            .map_err(|_| CodecError::Capacity)?;
        for _ in 0..inline_count {
            let root = ContentHash(cursor.fixed()?);
            if root.0 == [0; 32] || inline.last().is_some_and(|last| *last >= root) {
                return Err(CodecError::InvalidTag("archive inline order"));
            }
            inline.push(root);
        }
        let count = usize::try_from(cursor.u64()?).map_err(|_| CodecError::Capacity)?;
        if ledger.tenant.is_zero()
            || ledger.session.is_zero()
            || through.0 == 0
            || root.is_zero()
            || members.first() != Some(&root)
            || count == 0
            || count > limits.rows
        {
            return Err(CodecError::InvalidTag("archive frame"));
        }
        let start = cursor.offset();
        let mut previous = None;
        cursor.visit(count.checked_add(1).ok_or(CodecError::Capacity)?)?;
        for _ in 0..count {
            let row = inspect::read_row(&mut cursor, limits.row_bytes)?;
            cursor.visit(1)?;
            if row.deleted() || previous.is_some_and(|last| last >= row.key) {
                return Err(CodecError::InvalidTag("archive row"));
            }
            if matches!(
                row.key,
                Key::Meta | Key::Outcome(_) | Key::CreationResult(_)
            ) {
                return Err(CodecError::InvalidTag("archive accounting row"));
            }
            previous = Some(row.key);
        }
        if cursor.remaining() != 0 {
            return Err(CodecError::TrailingBytes);
        }
        let rows = payload.get(start..).ok_or(CodecError::Truncated)?;
        Ok(Self {
            header: ArchiveHeader {
                ledger,
                profile,
                through,
                root,
                members,
                content,
                inline,
                count,
            },
            digest,
            rows,
            row_limit: limits.row_bytes,
            visits: limits.visits,
        })
    }
    pub fn header(&self) -> &ArchiveHeader {
        &self.header
    }
    /// The bundle's digest: what a retirement record and a continuation
    /// name it by.
    pub fn digest(&self) -> ContentHash {
        self.digest
    }
    /// The rows in key order, each already structurally checked.
    pub(in crate::native) fn rows(&self) -> Result<RecordRows<'a>, CodecError> {
        RecordRows::new(self.rows, self.header.count, self.row_limit, self.visits)
    }
    /// The rows counted by family, for a bundle's summary.
    pub fn family_counts(&self) -> Result<Vec<(RowFamily, usize)>, CodecError> {
        let mut counts: Vec<(RowFamily, usize)> = Vec::new();
        for row in self.rows()? {
            let row: EncodedRow<'_> = row?;
            match counts
                .iter_mut()
                .find(|(family, _)| *family == row.family())
            {
                Some((_, count)) => *count = count.checked_add(1).ok_or(CodecError::Capacity)?,
                None => {
                    counts
                        .try_reserve_exact(1)
                        .map_err(|_| CodecError::Capacity)?;
                    counts.push((row.family(), 1));
                }
            }
        }
        Ok(counts)
    }
}
