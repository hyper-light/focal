use super::*;
use fields::{Configuration, Cursor, add, mul};
use std::cell::Cell;

#[derive(Clone, Copy)]
struct Members<'a> { bytes: &'a [u8], count: usize }
struct ConfigurationView<'a> { parts: [Members<'a>; 4], auto_leave: bool }
impl Configuration for ConfigurationView<'_> {
    fn count(&self, part: usize) -> Result<usize, Error> {
        self.parts.get(part).map(|value| value.count).ok_or(Error::Invalid("membership part"))
    }
    fn member(&self, part: usize, index: usize) -> Result<u64, Error> {
        let part = self.parts.get(part).ok_or(Error::Invalid("membership part"))?;
        let start = mul(index, 8)?;
        let bytes = part.bytes.get(start..add(start, 8)?).ok_or(Error::Invalid("membership index"))?;
        Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| Error::Truncated)?))
    }
    fn auto_leave(&self) -> bool { self.auto_leave }
}

/// Checksummed enclosing metadata and a structurally checked borrowed Core root.
/// Core row hydration and local artifact custody remain mandatory before publish.
/// No raw metadata is defaulted from an absent legacy envelope.
pub struct Checkpoint<'a> {
    header: Header,
    core: root::StructuralCheckpoint<'a>,
    core_bytes: &'a [u8],
    configuration: ConfigurationView<'a>,
    remaining: Cell<usize>,
    initial_visits: usize,
    members: usize,
}
impl<'a> Checkpoint<'a> {
    pub fn inspect(bytes: &'a [u8], limits: Limits) -> Result<Self, Error> {
        if bytes.len() > limits.bytes { return Err(Error::Capacity); }
        let end = bytes.len().checked_sub(32).ok_or(Error::Truncated)?;
        let (payload, trailer) = bytes.split_at_checked(end).ok_or(Error::Truncated)?;
        let hash_work = add(payload.len(), 2402)?;
        let remaining = limits.visits.checked_sub(hash_work).ok_or(Error::Capacity)?;
        let mut cursor = Cursor::new(payload, remaining);
        if cursor.fixed::<8>()? != MAGIC || cursor.u16()? != VERSION { return Err(Error::Invalid("native Session format")); }
        let cluster = cursor.fixed()?; let group = cursor.fixed()?;
        let ledger = LedgerId { tenant: focal_model::TenantId(cursor.fixed()?), session: focal_model::SessionId(cursor.fixed()?) };
        let profile = match cursor.u8()? { 0 => NativeContentProfile::ProjectionOnly,
            1 => NativeContentProfile::AuthoredV1, _ => return Err(Error::Invalid("content profile")) };
        let range = RangeId(u128::from_le_bytes(cursor.fixed()?));
        let prefix = SessionSeq(cursor.u64()?);
        let applied_raft = cursor.u64()?; let applied_term = cursor.u64()?; let configuration_index = cursor.u64()?;
        let recording_range = match cursor.u8()? { 0 => None,
            1 => Some(RangeId(u128::from_le_bytes(cursor.fixed()?))), _ => return Err(Error::Invalid("recording range option")) };
        let recording_term = cursor.u64()?;
        if cursor.u8()? != 0 { return Err(Error::Invalid("non-genesis activation")); }
        let activation = Activation { decoder: ContentHash(cursor.fixed()?), durable_floor: ContentHash(cursor.fixed()?), genesis: ContentHash(cursor.fixed()?) };
        if cursor.fixed::<6>()? != [0; 6] { return Err(Error::Invalid("legacy ancillary state is unsupported")); }
        let ancillary = AncillaryProfile::NativeOnlyV1;
        cursor.charge(5)?;
        let mut parts = [Members { bytes: &[], count: 0 }; 4];
        let mut member_count = 0usize;
        for part in &mut parts {
            let count = usize::try_from(cursor.u32()?).map_err(|_| Error::Capacity)?;
            member_count = add(member_count, count)?;
            if count > 1024 || member_count > limits.members || member_count > 2048 { return Err(Error::Capacity); }
            *part = Members { bytes: cursor.take(mul(count, 8)?)?, count };
        }
        let configuration = ConfigurationView { parts, auto_leave: cursor.boolean()? };
        fields::check_configuration(&configuration, limits.members, |work| cursor.charge(work))?;
        let encoded_core_bytes = cursor.u64()?;
        let count = usize::try_from(encoded_core_bytes).map_err(|_| Error::Capacity)?;
        let core_hash = ContentHash(cursor.fixed()?);
        let core_bytes = cursor.take(count)?;
        cursor.finish()?;
        let core = root::StructuralCheckpoint::inspect(core_bytes, record_codec::InspectionLimits {
            bytes: count, visits: cursor.remaining, rows: limits.rows, row_bytes: limits.row_bytes,
        })?;
        cursor.charge(core.quote().visits)?;
        let actual = core.header();
        if actual.ledger != ledger || actual.profile != profile || actual.range != range
            || actual.prefix != prefix || actual.hash != core_hash
        { return Err(Error::Invalid("nested Core identity")); }
        let metadata = Metadata { cluster, group, applied_raft, applied_term, configuration_index,
            recording_range, recording_term, activation, ancillary };
        let mut hash = blake3::Hasher::new_derive_key(HASH_DOMAIN); hash.update(payload);
        let hash = ContentHash(*hash.finalize().as_bytes());
        if hash.0 != trailer { return Err(Error::Invalid("Session checksum")); }
        let header = Header { metadata, ledger, profile, range, prefix, core_hash, core_bytes: encoded_core_bytes, hash };
        validate(header)?;
        Ok(Self { header, core, core_bytes, configuration, remaining: Cell::new(cursor.remaining),
            initial_visits: limits.visits, members: limits.members })
    }
    pub fn header(&self) -> Header { self.header }
    pub fn core(&self) -> &root::StructuralCheckpoint<'a> { &self.core }
    pub fn core_bytes(&self) -> &'a [u8] { self.core_bytes }
    pub fn visits_used(&self) -> usize { self.initial_visits.saturating_sub(self.remaining.get()) }
    pub fn remaining_visits(&self) -> usize { self.remaining.get() }
    fn charge(&self, amount: usize) -> Result<(), Error> {
        self.remaining.set(self.remaining.get().checked_sub(amount).ok_or(Error::Capacity)?); Ok(())
    }
    /// Compare directly against consensus's actual applied snapshot configuration.
    /// Repeated checks spend the same retained allowance; no decoded Vec is built.
    pub fn configuration_matches(&self, actual: &MembershipConfiguration) -> Result<(), Error> {
        fields::check_configuration(actual, self.members, |work| self.charge(work))?;
        self.charge(5)?;
        if self.configuration.auto_leave != actual.auto_leave { return Err(Error::Invalid("snapshot membership")); }
        for part in 0..4 {
            let count = self.configuration.count(part)?;
            self.charge(add(mul(count, 4)?, 1)?)?;
            if count != actual.count(part)? { return Err(Error::Invalid("snapshot membership")); }
            for index in 0..count {
                if self.configuration.member(part, index)? != actual.member(part, index)? { return Err(Error::Invalid("snapshot membership")); }
            }
        }
        Ok(())
    }
}
