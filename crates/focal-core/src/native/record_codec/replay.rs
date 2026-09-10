//! Prepare one committed native mutation against an exact restored predecessor.
//! Only changed rows and retained neighbors of touched pages are constructed.
//! The enclosing log establishes provenance; this module never executes commands.
use super::*;
use focal_evidence::{NativeCustodyReader, NativeSchemaVerifier};
use focal_memory::{Allocation, BudgetKind, BudgetLane, Change, Entry};
use read_source::Meter;

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;

struct EntrySlot<'bytes> {
    encoded: EncodedRow<'bytes>,
    value: Option<Row>,
    heap_bytes: usize,
}

/// One buffer and one aggregate permit, including all constructed row heaps.
/// Encoded positions are never copied into a second dependency array. Fields
/// drop in this order so every row stays funded through destruction.
struct Entries<'bytes> {
    slots: Vec<EntrySlot<'bytes>>,
    allocation: Allocation,
}
impl<'bytes> replay_index::EncodedRows<'bytes> for Entries<'bytes> {
    fn len(&self) -> usize {
        self.slots.len()
    }
    fn encoded(&self, index: usize) -> Option<&EncodedRow<'bytes>> {
        self.slots.get(index).map(|slot| &slot.encoded)
    }
}
impl<'bytes> Entries<'bytes> {
    fn read(
        record: &StructuralRecord<'bytes>,
        limits: NativeLimits,
        budget: &MemoryBudget,
        meters: &recovery::Meters,
    ) -> Result<Self, NativeError> {
        let count = record.quote().rows;
        if count == 0 || count > limits.range.max_batch_entries {
            return Err(NativeError::Capacity("record mutation count"));
        }
        let bytes = prepare::array::<EntrySlot<'bytes>>(count)?;
        prepare::within(bytes, limits.preparation_bytes)?;
        let allocation = budget
            .reserve(BudgetKind::Pending, BudgetLane::Completion, bytes)?
            .commit();
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        prepare::within(
            prepare::array::<EntrySlot<'bytes>>(slots.capacity())?,
            bytes,
        )?;
        meters
            .parsing
            .charge(record.quote().visits)
            .map_err(read_evidence::codec)?;
        meters
            .lookup
            .charge(count.checked_add(1).ok_or(ContractError::Capacity)?)
            .map_err(read_evidence::codec)?;
        let mut previous = None;
        for encoded in record
            .rows(record.quote().visits)
            .map_err(read_evidence::codec)?
        {
            let encoded = encoded.map_err(read_evidence::codec)?;
            if previous.is_some_and(|key| key >= encoded.key) || slots.len() >= count {
                return Err(ContractError::InvalidManifest.into());
            }
            previous = Some(encoded.key);
            slots.push(EntrySlot {
                encoded,
                value: None,
                heap_bytes: 0,
            });
        }
        if slots.len() != count {
            return Err(ContractError::InvalidManifest.into());
        }
        Ok(Self { slots, allocation })
    }
    /// The caller charges one bounded binary lookup before invoking this method.
    fn position(&self, key: Key) -> Option<usize> {
        self.slots
            .binary_search_by_key(&key, |slot| slot.encoded.key)
            .ok()
    }
}

/// The rows a replay reads beneath the record's own: the published Core, or
/// a materializer's view of that Core plus the records staged before this
/// one (doc 25). Every base read of a replay goes through this seam, so a
/// materializer can answer from earlier staged rows and record what was read.
pub(super) trait BaseRows {
    fn base_row(&self, key: Key) -> Option<&Row>;
    /// The prefix the record must extend.
    fn base_prefix(&self) -> u64;
}
impl BaseRows for Core<NativeState> {
    fn base_row(&self, key: Key) -> Option<&Row> {
        self.state.rows.get(&key)
    }
    fn base_prefix(&self) -> u64 {
        self.state.rows.prefix()
    }
}
struct Overlay<'a, 'bytes, B> {
    base: &'a B,
    entries: &'a Entries<'bytes>,
}
impl<B: BaseRows> replay_validate::Overlay for Overlay<'_, '_, B> {
    fn before(&self, key: Key) -> Option<&Row> {
        self.base.base_row(key)
    }
    fn after(&self, key: Key) -> Option<&Row> {
        match self.entries.position(key) {
            Some(index) => self
                .entries
                .slots
                .get(index)
                .and_then(|slot| slot.value.as_ref()),
            None => self.base.base_row(key),
        }
    }
    fn changes(&self) -> impl ExactSizeIterator<Item = (Key, Option<&Row>)> {
        self.entries
            .slots
            .iter()
            .map(|slot| (slot.encoded.key, slot.value.as_ref()))
    }
    fn changes_from(&self, key: Key) -> impl Iterator<Item = (Key, Option<&Row>)> {
        let start = self
            .entries
            .slots
            .partition_point(|slot| slot.encoded.key < key);
        self.entries
            .slots
            .iter()
            .skip(start)
            .map(|slot| (slot.encoded.key, slot.value.as_ref()))
    }
}
struct Objects<'a, 'bytes, B> {
    overlay: Overlay<'a, 'bytes, B>,
    header: RecordHeader,
    index: Option<&'a replay_index::Index>,
    parsing: &'a Meter,
}
impl<B: BaseRows> read_dispatch::Objects for Objects<'_, '_, B> {
    fn ledger(&self) -> LedgerId {
        self.header.ledger
    }
    fn prefix(&self) -> SessionSeq {
        self.header.outcome.sequence
    }
    fn get(&self, key: Key, meter: &Meter) -> Result<Option<&Row>, NativeError> {
        meter
            .charge(
                read_index::lookup_work()?
                    .checked_mul(2)
                    .ok_or(ContractError::Capacity)?,
            )
            .map_err(read_evidence::codec)?;
        Ok(replay_validate::Overlay::after(&self.overlay, key))
    }
    fn claim_dependency(
        &self,
        id: ClaimId,
        meter: &Meter,
    ) -> Result<read_dispatch::ClaimDependency<'_>, NativeError> {
        meter
            .charge(
                read_index::lookup_work()?
                    .checked_mul(2)
                    .ok_or(ContractError::Capacity)?,
            )
            .map_err(read_evidence::codec)?;
        match self.overlay.entries.position(Key::Claim(id)) {
            Some(index) => {
                let slot = self
                    .overlay
                    .entries
                    .slots
                    .get(index)
                    .ok_or(ContractError::InvalidManifest)?;
                if slot.encoded.deleted() {
                    return Err(ContractError::MissingEvidence.into());
                }
                Ok(read_dispatch::ClaimDependency::Raw(slot.encoded.body()))
            }
            None => {
                let claim = as_claim(self.overlay.base.base_row(Key::Claim(id)))
                    .ok_or(ContractError::MissingEvidence)?;
                Ok(read_dispatch::ClaimDependency::Retained(claim))
            }
        }
    }
    fn artifact_origin(
        &self,
        id: ArtifactId,
        meter: &Meter,
    ) -> Result<read_dispatch::ArtifactOrigin, NativeError> {
        self.index.ok_or(ContractError::InvalidManifest)?.artifact(
            self.overlay.entries,
            id,
            self.parsing,
            meter,
        )
    }
}

struct Decoder<'a, S, R, B> {
    base: &'a B,
    budget: &'a MemoryBudget,
    header: RecordHeader,
    limits: read_dispatch::Limits,
    meters: &'a recovery::Meters,
    custody: recovery::Custody<'a, S, R>,
}
impl<S: NativeSchemaVerifier, R: NativeCustodyReader, B: BaseRows> Decoder<'_, S, R, B> {
    fn phase(
        &self,
        entries: &mut Entries<'_>,
        index: Option<&replay_index::Index>,
        phase: usize,
    ) -> Result<usize, NativeError> {
        self.meters
            .lookup
            .charge(
                entries
                    .slots
                    .len()
                    .checked_add(1)
                    .ok_or(ContractError::Capacity)?,
            )
            .map_err(read_evidence::codec)?;
        let mut count = 0usize;
        for position in 0..entries.slots.len() {
            let slot = entries
                .slots
                .get(position)
                .ok_or(ContractError::InvalidManifest)?;
            if slot.encoded.deleted() || read_index::phase(slot.encoded.key)? != phase {
                continue;
            }
            if slot.value.is_some() {
                return Err(ContractError::InvalidManifest.into());
            }
            let objects = Objects {
                overlay: Overlay {
                    base: self.base,
                    entries,
                },
                header: self.header,
                index,
                parsing: &self.meters.parsing,
            };
            let context = read_dispatch::Context {
                objects: &objects,
                custody: &self.custody,
                workspace: self.budget,
                workspace_lane: BudgetLane::Completion,
                parsing: &self.meters.parsing,
                source: &self.meters.source,
                model: &self.meters.model,
                lookup: &self.meters.lookup,
                limits: self.limits,
            };
            let quote = read_dispatch::prepare(&slot.encoded, &context)?;
            prepare::within(
                prepare::add(size_of::<Entry<Key, Row>>(), quote.heap_bytes)?,
                self.limits.native.range.max_entry_bytes,
            )?;
            prepare::within(
                prepare::add(entries.allocation.bytes(), quote.heap_bytes)?,
                self.limits.native.preparation_bytes,
            )?;
            let mut funding = if quote.heap_bytes == 0 {
                None
            } else {
                Some(
                    self.budget
                        .reserve(
                            BudgetKind::Pending,
                            BudgetLane::Completion,
                            quote.heap_bytes,
                        )?
                        .commit(),
                )
            };
            let (row, actual) = read_dispatch::with_build(
                &slot.encoded,
                &context,
                quote,
                quote.heap_bytes,
                |row, heap| Ok((row, heap)),
            )?;
            // Admission precedes construction; absorbing replaces the temporary
            // permit with the transaction's sole owner before retaining the row.
            if let Some(funding) = &mut funding {
                entries.allocation.absorb(funding)?;
            }
            let slot = entries
                .slots
                .get_mut(position)
                .ok_or(ContractError::InvalidManifest)?;
            slot.value = Some(row);
            slot.heap_bytes = actual;
            count = count.checked_add(1).ok_or(ContractError::Capacity)?;
        }
        Ok(count)
    }
}

/// Decode and validate one exact successor without publishing it. `original`
/// is the source RangeId established by the enclosing trusted checkpoint/log
/// chain; the current Core may have a fresh local owner incarnation. Caller-
/// owned record bytes must stay funded for this call. Admission and publication
/// are separate, so a refusal cannot alter the current prefix or existing pins.
///
/// The native storage limits always come from `core`; `limits` supplies bounded
/// descriptor construction and one cumulative recovery work allowance. Success
/// does not authenticate a log, acknowledge disk/Raft durability, reconstruct
/// owner completion credits, or activate a network decoder.
pub fn prepare<S: NativeSchemaVerifier, R: NativeCustodyReader>(
    core: &Core<NativeState>,
    record: &StructuralRecord<'_>,
    original: RangeId,
    limits: recovery::Limits,
    store: &R,
    schemas: &S,
) -> Result<NativePrepared, NativeError> {
    let staged = stage(
        core,
        &core.state.budget,
        core.limits,
        core.state.ledger,
        core.state.profile,
        record,
        original,
        limits,
        store,
        schemas,
    )?;
    install(core, staged)
}

/// A decoded and validated record before its pages exist: the complete
/// change set, the captured write set and their funding. Built against any
/// [`BaseRows`] view, so a materializer can stage records ahead of the
/// published prefix and install them in order (doc 25).
pub struct StagedRecord {
    pub(super) header: RecordHeader,
    pub(super) changes: Vec<Change<Key, Row>>,
    pub(super) writes: mutation::WriteSet,
    pub(super) allocation: Allocation,
    pub(super) slots_bytes: usize,
    pub(super) remaining: recovery::Work,
}
impl StagedRecord {
    pub fn header(&self) -> &RecordHeader {
        &self.header
    }
    /// The staged row for `key`: `Some(None)` for a deletion, `None` when
    /// the record does not write the key.
    pub(super) fn row(&self, key: Key) -> Option<Option<&Row>> {
        let index = self
            .changes
            .binary_search_by(|change| change.key().cmp(&key))
            .ok()?;
        Some(match self.changes.get(index)? {
            Change::Put(entry) => Some(&entry.value),
            Change::Delete(_) => None,
        })
    }
}

/// Decode and validate one record against `base` without touching the
/// Core's pages. `budget`, `native`, `ledger` and `profile` describe the
/// Core the record will be installed into.
#[allow(
    clippy::too_many_arguments,
    reason = "one bounded staging pass over borrowed inputs"
)]
pub(super) fn stage<S: NativeSchemaVerifier, R: NativeCustodyReader, B: BaseRows>(
    base: &B,
    budget: &MemoryBudget,
    native: NativeLimits,
    ledger: LedgerId,
    profile: NativeContentProfile,
    record: &StructuralRecord<'_>,
    original: RangeId,
    mut limits: recovery::Limits,
    store: &R,
    schemas: &S,
) -> Result<StagedRecord, NativeError> {
    let header = record.header();
    if header.ledger != ledger
        || header.profile != profile
        || header.range != original
        || header.base.0 != base.base_prefix()
        || header.base.0.checked_add(1) != Some(header.outcome.sequence.0)
    {
        return Err(ContractError::InvalidManifest.into());
    }
    limits.native = native;
    let meters = recovery::Meters::new(limits.work);
    let mut entries = Entries::read(record, limits.native, budget, &meters)?;
    let decoder = Decoder {
        base,
        budget,
        header,
        limits: limits.dispatch(),
        meters: &meters,
        custody: recovery::Custody::new(store, schemas, budget, &meters.model),
    };
    let first = decoder.phase(&mut entries, None, 0)?;
    let index = replay_index::Index::build(
        &entries,
        header,
        native,
        budget,
        &meters.parsing,
        &meters.lookup,
    )?;
    if index.counts.first().copied() != Some(first) {
        return Err(ContractError::InvalidManifest.into());
    }
    for phase in 1..read_index::PHASES {
        let count = decoder.phase(&mut entries, Some(&index), phase)?;
        if index.counts.get(phase).copied() != Some(count) {
            return Err(ContractError::InvalidManifest.into());
        }
    }
    let overlay = Overlay {
        base,
        entries: &entries,
    };
    replay_validate::validate(&replay_validate::ReplayRead {
        overlay: &overlay,
        ledger: header.ledger,
        profile: header.profile,
        base: header.base,
        outcome: header.outcome,
        limits: native,
        meter: &meters.model,
        budget,
        encoded: &entries,
        index: &index,
        parsing: &meters.parsing,
    })?;
    drop(index);

    // The mutation vector has its own precharge until it joins Entries' permit.
    // Storage later takes the exact vector/payload portion; the encoded slot
    // buffer keeps its independent remainder through construction.
    let change_bytes = prepare::array::<Change<Key, Row>>(entries.slots.len())?;
    prepare::within(
        prepare::add(entries.allocation.bytes(), change_bytes)?,
        native.preparation_bytes,
    )?;
    let mut changes_allocation = budget
        .reserve(BudgetKind::Pending, BudgetLane::Completion, change_bytes)?
        .commit();
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(entries.slots.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    prepare::within(
        prepare::array::<Change<Key, Row>>(changes.capacity())?,
        change_bytes,
    )?;
    meters
        .lookup
        .charge(
            entries
                .slots
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(256))
                .ok_or(ContractError::Capacity)?,
        )
        .map_err(read_evidence::codec)?;
    for slot in &mut entries.slots {
        let change = match (slot.encoded.deleted(), slot.value.take()) {
            (false, Some(row)) => Change::Put(Entry::new(slot.encoded.key, row, slot.heap_bytes)),
            (true, None) => Change::Delete(slot.encoded.key),
            _ => return Err(ContractError::InvalidManifest.into()),
        };
        if changes.len() == changes.capacity() {
            return Err(ContractError::Capacity.into());
        }
        changes.push(change);
    }
    entries.allocation.absorb(&mut changes_allocation)?;
    let write_bytes = mutation::bytes(changes.len())?;
    let writes = mutation::WriteSet::capture(
        header.profile,
        changes.len(),
        changes.iter(),
        write_bytes,
        budget
            .reserve(BudgetKind::Pending, BudgetLane::Completion, write_bytes)?
            .commit(),
    )?;
    let slots_bytes = prepare::array::<EntrySlot<'_>>(entries.slots.capacity())?;
    let Entries { slots, allocation } = entries;
    drop(slots);
    Ok(StagedRecord {
        header,
        changes,
        writes,
        allocation,
        slots_bytes,
        remaining: recovery::Work {
            parsing: meters.parsing.remaining(),
            source: meters.source.remaining(),
            model: meters.model.remaining(),
            lookup: meters.lookup.remaining(),
        },
    })
}

/// The record's meta row alone, decoded from its bytes: a fixed row that
/// reads nothing, so a materializer can hand every stage its predecessor's
/// meta without waiting for that predecessor (doc 25 §2).
pub(super) fn stage_meta(
    record: &StructuralRecord<'_>,
    ledger: LedgerId,
    visits: usize,
) -> Result<Row, NativeError> {
    let quote = record.quote();
    for row in record.rows(quote.visits).map_err(read_evidence::codec)? {
        let row = row.map_err(read_evidence::codec)?;
        if row.key != Key::Meta || row.deleted() {
            continue;
        }
        let body = row.body();
        let mut cursor =
            bytes::Cursor::new(body, body.len(), visits).map_err(read_evidence::codec)?;
        let meta = read_rows::read_fixed(Key::Meta, &mut cursor, ledger)?
            .ok_or(ContractError::InvalidManifest)?;
        cursor.finish().map_err(read_evidence::codec)?;
        return Ok(meta);
    }
    Err(ContractError::InvalidManifest.into())
}

/// Build the staged record's pages against the Core's published root. The
/// record must extend the Core's current prefix; the remaining recovery work
/// of its staging pays for the page copies.
pub(super) fn install(
    core: &Core<NativeState>,
    staged: StagedRecord,
) -> Result<NativePrepared, NativeError> {
    let StagedRecord {
        header,
        changes,
        writes,
        mut allocation,
        slots_bytes,
        remaining,
    } = staged;
    if header.ledger != core.state.ledger
        || header.profile != core.state.profile
        || header.base.0 != core.state.rows.prefix()
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let meters = recovery::Meters::new(remaining);
    // Prepay sorting, bounded directory descent and retained-entry sizing on
    // touched leaves. This bound scales with the mutation and configured page
    // size, never the complete ledger. Nested copies are charged individually.
    let work = changes
        .len()
        .checked_add(1)
        .and_then(|n| n.checked_mul(core.limits.range.page_entries.checked_add(1)?))
        .and_then(|n| n.checked_mul(const { (usize::BITS as usize + 1) * 512 }))
        .ok_or(ContractError::Capacity)?;
    meters.lookup.charge(work).map_err(read_evidence::codec)?;
    // Dividing the input among a group's members takes a vector per touched
    // member beyond the one the record funded (25 §4).
    let extra = core.state.rows.input_extra_bytes(changes.len())?;
    if extra != 0 {
        let mut extra_allocation = core
            .state
            .budget
            .reserve(BudgetKind::Pending, BudgetLane::Completion, extra)?
            .commit();
        allocation.absorb(&mut extra_allocation)?;
    }
    let plan = core.state.rows.plan_batch(
        &core.state.budget,
        header.outcome.sequence.0,
        changes,
        BudgetLane::Completion,
        usize::MAX,
    )?;
    let failure = std::cell::Cell::new(None);
    let input_bytes = plan.input_pending_bytes();
    let available = allocation
        .bytes()
        .checked_sub(slots_bytes)
        .ok_or(ContractError::Capacity)?;
    prepare::within(input_bytes, available)?;
    // The plan takes the existing exact input permit. Its drop-ordered owner
    // retains that charge through every refusal and through destination-page
    // construction; the incoming vector/payloads are not admitted a second time.
    let input = allocation.split_off(input_bytes)?;
    let fragments = plan
        .build_in_funded_with(&core.state.budget, input, |row| {
            let work = match read_dispatch::objects::copy_work(row) {
                Ok(work) => work,
                Err(error) => {
                    failure.set(Some(error));
                    return Err(MemoryError::InvalidConfiguration(
                        "native replay copy work exhausted",
                    ));
                }
            };
            if let Err(error) = meters.model.charge(work) {
                failure.set(Some(read_evidence::codec(error)));
                return Err(MemoryError::InvalidConfiguration(
                    "native replay copy work exhausted",
                ));
            }
            prepare::copy(row)
        })
        .map_err(|error| failure.take().unwrap_or_else(|| error.into()))?;
    writes.check(&fragments)?;
    Ok(NativePrepared {
        fragments,
        outcome: header.outcome,
        writes,
    })
}
