//! Allocation-free outer integrity and key-order inspection. Row bodies remain
//! untrusted borrowed bytes until the complete model/native importer checks them.
use super::*;
use bytes::{Cursor, Error};

#[derive(Debug, Clone, Copy)]
pub struct InspectionLimits {
    pub bytes: usize,
    pub visits: usize,
    pub rows: usize,
    pub row_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InspectionQuote {
    pub bytes: usize,
    pub visits: usize,
    pub rows: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub ledger: LedgerId,
    pub range: RangeId,
    pub profile: NativeContentProfile,
    pub base: SessionSeq,
    pub outcome: NativeOutcome,
    pub hash: ContentHash,
}
pub struct StructuralRecord<'a> {
    header: RecordHeader,
    rows: &'a [u8],
    row_limit: usize,
    quote: InspectionQuote,
}
impl<'a> StructuralRecord<'a> {
    pub fn inspect(bytes: &'a [u8], limits: InspectionLimits) -> Result<Self, Error> {
        if bytes.len() > limits.bytes {
            return Err(Error::Capacity);
        }
        let at = bytes.len().checked_sub(32).ok_or(Error::Truncated)?;
        let (payload, trailer) = bytes.split_at_checked(at).ok_or(Error::Truncated)?;
        // One payload hash, fixed domain setup/finalization, and checksum read
        // and comparison. Reserve all of it before reading any variable body.
        let hash_visits = add(payload.len(), 322)?;
        let available = limits
            .visits
            .checked_sub(hash_visits)
            .ok_or(Error::Capacity)?;
        let mut c = Cursor::new(payload, limits.bytes, available)?;
        if c.fixed::<8>()? != MAGIC || c.u16()? != VERSION {
            return Err(Error::InvalidTag("record format"));
        }
        let profile = match c.u8()? {
            0 => NativeContentProfile::ProjectionOnly,
            1 => NativeContentProfile::AuthoredV1,
            _ => return Err(Error::InvalidTag("record profile")),
        };
        let ledger = fixed::read_ledger(&mut c)?;
        let range = RangeId(u128::from_le_bytes(c.fixed()?));
        let base = SessionSeq(c.u64()?);
        let outcome_start = c.offset();
        let outcome = fixed::read_outcome(&mut c)?;
        let original_outcome = payload
            .get(outcome_start..c.offset())
            .ok_or(Error::Truncated)?;
        if ledger.tenant.is_zero()
            || ledger.session.is_zero()
            || outcome.ledger != ledger
            || base.0.checked_add(1) != Some(outcome.sequence.0)
        {
            return Err(Error::InvalidTag("record frame"));
        }
        let count = c.count(limits.rows)?;
        if count == 0 {
            return Err(Error::InvalidTag("empty record"));
        }
        let rows_start = c.offset();
        let mut previous = None;
        let mut meta = false;
        let mut recorded = false;
        c.visit(add(count, 1)?)?;
        for _ in 0..count {
            let row = read_row(&mut c, limits.row_bytes)?;
            c.visit(1)?;
            if previous.is_some_and(|last| last >= row.key) {
                return Err(Error::InvalidTag("record key order"));
            }
            previous = Some(row.key);
            match row.key {
                Key::Meta => {
                    if row.deleted {
                        return Err(Error::InvalidTag("deleted metadata"));
                    }
                    meta = true;
                }
                Key::Outcome(key) if key == outcome.invocation => {
                    c.visit(add(1, row.body.len())?)?;
                    if row.deleted || row.body != original_outcome {
                        return Err(Error::InvalidTag("record outcome"));
                    }
                    recorded = true;
                }
                _ => {}
            }
        }
        if !meta || !recorded {
            return Err(Error::InvalidTag("record accounting rows"));
        }
        let visits = add(c.visits_used(), hash_visits)?;
        let rows = payload
            .get(rows_start..c.offset())
            .ok_or(Error::Truncated)?;
        c.finish()?;
        let mut hasher = blake3::Hasher::new_derive_key(HASH_DOMAIN);
        hasher.update(payload);
        let hash = ContentHash(*hasher.finalize().as_bytes());
        if trailer != hash.0 {
            return Err(Error::InvalidTag("record checksum"));
        }
        Ok(Self {
            header: RecordHeader {
                ledger,
                range,
                profile,
                base,
                outcome,
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
    pub fn header(&self) -> RecordHeader {
        self.header
    }
    pub fn quote(&self) -> InspectionQuote {
        self.quote
    }
    /// Each scan has its own explicit work allowance. A phased importer must
    /// account all scans, body decoding and dependency lookups in its enclosing
    /// recovery budget; this iterator never allocates a row index or owned body.
    pub fn rows(&self, max_visits: usize) -> Result<RecordRows<'a>, Error> {
        RecordRows::new(self.rows, self.quote.rows, self.row_limit, max_visits)
    }
}

/// An exact key and borrowed body. The family label does not attest that the
/// body decodes as that family or satisfies its references and history.
pub struct EncodedRow<'a> {
    pub(super) key: Key,
    family: RowFamily,
    deleted: bool,
    body: &'a [u8],
}
impl<'a> EncodedRow<'a> {
    pub fn family(&self) -> RowFamily {
        self.family
    }
    pub fn deleted(&self) -> bool {
        self.deleted
    }
    pub fn body(&self) -> &'a [u8] {
        self.body
    }
}
pub struct RecordRows<'a> {
    cursor: Cursor<'a>,
    left: usize,
    row_limit: usize,
    stopped: bool,
}
impl RecordRows<'_> {
    pub fn visits_used(&self) -> usize {
        self.cursor.visits_used()
    }
}
impl<'a> RecordRows<'a> {
    pub(super) fn new(
        rows: &'a [u8],
        count: usize,
        row_limit: usize,
        max_visits: usize,
    ) -> Result<Self, Error> {
        Ok(Self {
            cursor: Cursor::new(rows, rows.len(), max_visits)?,
            left: count,
            row_limit,
            stopped: false,
        })
    }
}
impl<'a> Iterator for RecordRows<'a> {
    type Item = Result<EncodedRow<'a>, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped {
            return None;
        }
        if let Err(error) = self.cursor.visit(1) {
            self.stopped = true;
            return Some(Err(error));
        }
        if self.left == 0 {
            self.stopped = true;
            return if self.cursor.remaining() == 0 {
                None
            } else {
                Some(Err(Error::TrailingBytes))
            };
        }
        self.left = match self.left.checked_sub(1) {
            Some(left) => left,
            None => {
                self.stopped = true;
                return Some(Err(Error::Capacity));
            }
        };
        let row = read_row(&mut self.cursor, self.row_limit);
        if row.is_err() {
            self.stopped = true;
        }
        Some(row)
    }
}
impl std::iter::FusedIterator for RecordRows<'_> {}

pub(super) fn read_row<'a>(c: &mut Cursor<'a>, max_bytes: usize) -> Result<EncodedRow<'a>, Error> {
    let deleted = match c.u8()? {
        0 => true,
        1 => false,
        _ => return Err(Error::InvalidTag("mutation kind")),
    };
    let key = fixed::read_key(c)?;
    let family = fixed::family(key)?;
    let size = c.count(max_bytes)?;
    if deleted != (size == 0) {
        return Err(Error::InvalidTag("mutation body length"));
    }
    let body = c.take(size)?;
    Ok(EncodedRow {
        key,
        family,
        deleted,
        body,
    })
}
