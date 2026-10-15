//! A retirement's record (doc 31 M2). It carries what every replica cannot derive alone, the
//! batch's families by their roots and the bundles that hold them, with the header a native
//! record carries (the producer range, the prefix it extends and the outcome it makes), never
//! the rows: every replica derives the families from its own rows at the record's place,
//! checks them against the outcome's intent (the batch's digest) and deletes them alike. A
//! retirement of hundreds of rows is a record of a few dozen bytes a family.
use super::*;
use crate::native::retirement_batch::{RETIRE_COLD, RETIRE_CUSTODY, RetireBody, RetiredBundle};
use bytes::{Cursor, Error};

pub const RETIRE_MAGIC: [u8; 8] = *b"FCRETIRE";
const RETIRE_HASH_DOMAIN: &str = "focal.native.retire-record.v1";

/// Write the record of `prepared`, a retirement's candidate retiring `retired`: its header as a
/// native record's, then each family's root and bundle.
pub(super) fn frame<S: Sink>(
    sink: &mut S,
    prepared: &NativePrepared,
    body: &RetireBody,
) -> Result<(), Error> {
    let retired = &body.retired;
    if body.version != RETIRE_CUSTODY && body.version != RETIRE_COLD {
        return Err(Error::InvalidTag("retirement record version"));
    }
    let outcome = prepared.outcome;
    if outcome.ledger.tenant.is_zero()
        || outcome.ledger.session.is_zero()
        || outcome.operation != NativeOperation::Retire
        || retired.is_empty()
        || outcome.sequence.0 != prepared.fragments.prefix()
        || prepared.fragments.base_prefix().checked_add(1) != Some(outcome.sequence.0)
    {
        return Err(Error::InvalidTag("retirement record frame"));
    }
    write_raw(sink, &RETIRE_MAGIC)?;
    write_u16(sink, body.version)?;
    write_u8(
        sink,
        match prepared.content_profile() {
            NativeContentProfile::ProjectionOnly => 0,
            NativeContentProfile::AuthoredV1 => 1,
        },
    )?;
    types::ledger(sink, outcome.ledger)?;
    write_raw(sink, &prepared.fragments.id().0.to_le_bytes())?;
    write_u64(sink, prepared.fragments.base_prefix())?;
    fixed::outcome(sink, outcome)?;
    write_count(sink, retired.len())?;
    for one in retired {
        write_raw(sink, &one.root.0)?;
        write_raw(sink, &one.bundle.0)?;
        write_u64(sink, one.bytes)?;
    }
    Ok(())
}

/// What the record's digest is taken under.
pub(super) fn hasher() -> blake3::Hasher {
    blake3::Hasher::new_derive_key(RETIRE_HASH_DOMAIN)
}

/// A retirement's record, read: its header as a native record's, and its families.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireRecord {
    header: RecordHeader,
    version: u16,
    retired: Vec<RetiredBundle>,
}

impl RetireRecord {
    /// Read and check a record: its format, its digest, a header whose outcome is a
    /// retirement's at the sequence after its base, and between one and
    /// `max_families` families.
    pub fn inspect(
        bytes: &[u8],
        limits: InspectionLimits,
        max_families: usize,
    ) -> Result<Self, Error> {
        if bytes.len() > limits.bytes {
            return Err(Error::Capacity);
        }
        let at = bytes.len().checked_sub(32).ok_or(Error::Truncated)?;
        let (payload, trailer) = bytes.split_at_checked(at).ok_or(Error::Truncated)?;
        let hash_visits = payload.len().checked_add(322).ok_or(Error::Capacity)?;
        let available = limits
            .visits
            .checked_sub(hash_visits)
            .ok_or(Error::Capacity)?;
        let mut c = Cursor::new(payload, limits.bytes, available)?;
        if c.fixed::<8>()? != RETIRE_MAGIC {
            return Err(Error::InvalidTag("retirement record format"));
        }
        let version = c.u16()?;
        if version != RETIRE_CUSTODY && version != RETIRE_COLD {
            return Err(Error::InvalidTag("retirement record format"));
        }
        let profile = match c.u8()? {
            0 => NativeContentProfile::ProjectionOnly,
            1 => NativeContentProfile::AuthoredV1,
            _ => return Err(Error::InvalidTag("retirement record profile")),
        };
        let ledger = fixed::read_ledger(&mut c)?;
        let range = RangeId(u128::from_le_bytes(c.fixed()?));
        let base = SessionSeq(c.u64()?);
        let outcome = fixed::read_outcome(&mut c)?;
        if ledger.tenant.is_zero()
            || ledger.session.is_zero()
            || outcome.ledger != ledger
            || outcome.operation != NativeOperation::Retire
            || base.0.checked_add(1) != Some(outcome.sequence.0)
        {
            return Err(Error::InvalidTag("retirement record frame"));
        }
        let count = c.count(max_families)?;
        if count == 0 {
            return Err(Error::InvalidTag("empty retirement record"));
        }
        let mut retired = Vec::new();
        retired
            .try_reserve_exact(count)
            .map_err(|_| Error::Capacity)?;
        for _ in 0..count {
            let root = ClaimId(c.fixed()?);
            let bundle = ContentHash(c.fixed()?);
            let length = c.u64()?;
            if bundle.0 == [0; 32] || length == 0 {
                return Err(Error::InvalidTag("retirement record bundle"));
            }
            retired.push(RetiredBundle {
                root,
                bundle,
                bytes: length,
            });
        }
        // The outcome names the batch by its first family's root.
        if retired
            .first()
            .map(|first| NativeInvocation::Retirement(first.root))
            != Some(outcome.invocation)
        {
            return Err(Error::InvalidTag("retirement record outcome"));
        }
        c.finish()?;
        let mut hasher = hasher();
        hasher.update(payload);
        let hash = ContentHash(*hasher.finalize().as_bytes());
        if trailer != hash.0 {
            return Err(Error::InvalidTag("retirement record checksum"));
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
            version,
            retired,
        })
    }
    pub fn header(&self) -> RecordHeader {
        self.header
    }
    /// Whether its bundles are custody objects ([`RETIRE_CUSTODY`]) or cold frames
    /// ([`RETIRE_COLD`]).
    pub fn version(&self) -> u16 {
        self.version
    }
    /// The families it retires, in its batch's order.
    pub fn retired(&self) -> &[RetiredBundle] {
        &self.retired
    }
}
