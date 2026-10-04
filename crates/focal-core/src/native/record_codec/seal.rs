//! The seal bundle reader (the audit's F12): the structural check of an
//! `FCNSEAL1` frame — magic, version, profile, ledger, and either a seal's
//! prefix, ordinal, principals and rows or a fold's members and ranges,
//! then the trailing digest — before anything in it is trusted, and the
//! outcome a seal holds for one invocation. A bundle is never restored; it
//! is read, and what it says about itself is verified against what it holds.
use super::checkpoint::{SEAL_HASH_DOMAIN, SEAL_MAGIC, SEAL_VERSION};
use super::inspect::{InspectionLimits, InspectionQuote};
use super::*;
use crate::native::epochs::SealedRange;
use crate::native::seal::{SealRow, SealedPrincipal};
use bytes::Cursor;

/// What a seal bundle declares about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealHeader {
    Seal {
        ledger: LedgerId,
        profile: NativeContentProfile,
        /// The prefix the seal was derived at: every outcome it holds is at
        /// or below it.
        through: SessionSeq,
        ordinal: u64,
        /// By principal: the generations whose outcomes the seal holds.
        principals: Vec<SealedPrincipal>,
        /// Rows in the bundle.
        count: usize,
    },
    Fold {
        ledger: LedgerId,
        profile: NativeContentProfile,
        first: u64,
        last: u64,
        /// The rows folded, by ordinal.
        members: Vec<(u64, SealRow)>,
        /// Every sealed range that pointed into a member, by principal.
        ranges: Vec<(ParticipantId, SealedRange)>,
    },
}
impl SealHeader {
    pub fn ledger(&self) -> LedgerId {
        match self {
            Self::Seal { ledger, .. } | Self::Fold { ledger, .. } => *ledger,
        }
    }
    /// The member seal holding `principal`'s `epoch`, in a fold; a seal
    /// answers for itself.
    pub fn member_of(&self, principal: ParticipantId, epoch: RequestEpoch) -> Option<u64> {
        match self {
            Self::Seal {
                ordinal,
                principals,
                ..
            } => principals
                .iter()
                .any(|entry| {
                    entry.principal == principal && entry.first <= epoch && epoch <= entry.last
                })
                .then_some(*ordinal),
            Self::Fold { ranges, .. } => ranges
                .iter()
                .find(|(p, range)| *p == principal && range.first <= epoch && epoch <= range.last)
                .map(|(_, range)| range.seal),
        }
    }
    /// A fold's member row by ordinal.
    pub fn member(&self, ordinal: u64) -> Option<SealRow> {
        match self {
            Self::Fold { members, .. } => members
                .iter()
                .find(|(member, _)| *member == ordinal)
                .map(|(_, row)| *row),
            Self::Seal { .. } => None,
        }
    }
}

/// A structurally verified seal bundle: its header, its digest and its rows.
pub struct StructuralSeal<'a> {
    header: SealHeader,
    digest: ContentHash,
    rows: &'a [u8],
    count: usize,
    row_limit: usize,
    visits: usize,
    quote: InspectionQuote,
}
impl<'a> StructuralSeal<'a> {
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
        let mut hasher = blake3::Hasher::new_derive_key(SEAL_HASH_DOMAIN);
        hasher.update(payload);
        let digest = ContentHash(*hasher.finalize().as_bytes());
        if digest.0 != trailer {
            return Err(CodecError::InvalidTag("seal digest"));
        }
        let mut cursor = Cursor::new(payload, limits.bytes, available)?;
        if cursor.fixed::<8>()? != SEAL_MAGIC || cursor.u16()? != SEAL_VERSION {
            return Err(CodecError::InvalidTag("seal format"));
        }
        let profile = match cursor.u8()? {
            0 => NativeContentProfile::ProjectionOnly,
            1 => NativeContentProfile::AuthoredV1,
            _ => return Err(CodecError::InvalidTag("seal profile")),
        };
        let ledger = fixed::read_ledger(&mut cursor)?;
        if ledger.tenant.is_zero() || ledger.session.is_zero() {
            return Err(CodecError::InvalidTag("seal ledger"));
        }
        let (header, count) = match cursor.u8()? {
            0 => {
                let through = SessionSeq(cursor.u64()?);
                let ordinal = cursor.u64()?;
                let principal_count = cursor.count(limits.rows)?;
                let mut principals = Vec::new();
                principals
                    .try_reserve_exact(principal_count)
                    .map_err(|_| CodecError::Capacity)?;
                for _ in 0..principal_count {
                    let entry = SealedPrincipal {
                        principal: ParticipantId(cursor.fixed()?),
                        first: RequestEpoch(cursor.u64()?),
                        last: RequestEpoch(cursor.u64()?),
                    };
                    if entry.principal.is_zero()
                        || entry.first.0 == 0
                        || entry.first > entry.last
                        || principals
                            .last()
                            .is_some_and(|last: &SealedPrincipal| last.principal >= entry.principal)
                    {
                        return Err(CodecError::InvalidTag("seal principals"));
                    }
                    principals.push(entry);
                }
                let count = usize::try_from(cursor.u64()?).map_err(|_| CodecError::Capacity)?;
                if through.0 == 0 || ordinal == 0 || count == 0 || count > limits.rows {
                    return Err(CodecError::InvalidTag("seal frame"));
                }
                (
                    SealHeader::Seal {
                        ledger,
                        profile,
                        through,
                        ordinal,
                        principals,
                        count,
                    },
                    count,
                )
            }
            1 => {
                let first = cursor.u64()?;
                let last = cursor.u64()?;
                let member_count =
                    usize::try_from(cursor.u32()?).map_err(|_| CodecError::Capacity)?;
                if first == 0 || first >= last || member_count < 2 || member_count > limits.rows {
                    return Err(CodecError::InvalidTag("fold frame"));
                }
                cursor.visit(member_count)?;
                let mut members: Vec<(u64, SealRow)> = Vec::new();
                members
                    .try_reserve_exact(member_count)
                    .map_err(|_| CodecError::Capacity)?;
                for _ in 0..member_count {
                    let ordinal = cursor.u64()?;
                    let row = fixed::read_seal_row(&mut cursor)?;
                    if !row.valid(ordinal)
                        || ordinal > last
                        || members
                            .last()
                            .is_some_and(|(previous, _)| *previous >= ordinal)
                    {
                        return Err(CodecError::InvalidTag("fold member"));
                    }
                    members.push((ordinal, row));
                }
                if members.first().is_none_or(|(_, row)| row.first != first)
                    || members.last().is_none_or(|(ordinal, _)| *ordinal != last)
                {
                    return Err(CodecError::InvalidTag("fold span"));
                }
                let range_count = cursor.count(limits.rows)?;
                let mut ranges: Vec<(ParticipantId, SealedRange)> = Vec::new();
                ranges
                    .try_reserve_exact(range_count)
                    .map_err(|_| CodecError::Capacity)?;
                for _ in 0..range_count {
                    let principal = ParticipantId(cursor.fixed()?);
                    let range = SealedRange {
                        first: RequestEpoch(cursor.u64()?),
                        last: RequestEpoch(cursor.u64()?),
                        seal: cursor.u64()?,
                    };
                    if principal.is_zero()
                        || range.first.0 == 0
                        || range.first > range.last
                        || !(first..=last).contains(&range.seal)
                        || ranges.last().is_some_and(|(p, previous)| {
                            (*p, previous.last.0) >= (principal, range.first.0)
                        })
                    {
                        return Err(CodecError::InvalidTag("fold ranges"));
                    }
                    ranges.push((principal, range));
                }
                if cursor.u64()? != 0 {
                    return Err(CodecError::InvalidTag("fold rows"));
                }
                (
                    SealHeader::Fold {
                        ledger,
                        profile,
                        first,
                        last,
                        members,
                        ranges,
                    },
                    0,
                )
            }
            _ => return Err(CodecError::InvalidTag("seal kind")),
        };
        let start = cursor.offset();
        let mut previous = None;
        cursor.visit(count.checked_add(1).ok_or(CodecError::Capacity)?)?;
        for _ in 0..count {
            let row = inspect::read_row(&mut cursor, limits.row_bytes)?;
            cursor.visit(1)?;
            if row.deleted()
                || previous.is_some_and(|last| last >= row.key)
                || !matches!(row.key, Key::Outcome(_) | Key::CreationResult(_))
            {
                return Err(CodecError::InvalidTag("seal row"));
            }
            previous = Some(row.key);
        }
        if cursor.remaining() != 0 {
            return Err(CodecError::TrailingBytes);
        }
        let visits = cursor
            .visits_used()
            .checked_add(hash_visits)
            .ok_or(CodecError::Capacity)?;
        let rows = payload.get(start..).ok_or(CodecError::Truncated)?;
        Ok(Self {
            header,
            digest,
            rows,
            count,
            row_limit: limits.row_bytes,
            visits: limits.visits,
            quote: InspectionQuote {
                bytes: bytes.len(),
                visits,
                rows: count,
            },
        })
    }
    pub fn header(&self) -> &SealHeader {
        &self.header
    }
    pub fn digest(&self) -> ContentHash {
        self.digest
    }
    pub fn quote(&self) -> InspectionQuote {
        self.quote
    }
    /// The outcome the seal holds for `invocation`, decoded from its row;
    /// `None` when the seal holds no such row. One pass over the rows,
    /// bounded by the inspection's visits.
    pub fn outcome(
        &self,
        invocation: NativeInvocation,
    ) -> Result<Option<NativeOutcome>, CodecError> {
        let wanted = Key::Outcome(invocation);
        let mut cursor = Cursor::new(self.rows, self.rows.len(), self.visits)?;
        for _ in 0..self.count {
            let row = inspect::read_row(&mut cursor, self.row_limit)?;
            cursor.visit(1)?;
            if row.key == wanted {
                let mut body = Cursor::new(row.body(), row.body().len(), self.visits)?;
                let outcome = fixed::read_outcome(&mut body)?;
                body.finish()?;
                if outcome.invocation != invocation {
                    return Err(CodecError::InvalidTag("seal outcome"));
                }
                return Ok(Some(outcome));
            }
            if row.key > wanted {
                break;
            }
        }
        Ok(None)
    }
}
