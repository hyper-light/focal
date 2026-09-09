//! Transaction-local recorded dependencies and original event coordinates.
//! The caller owns one canonical encoded/decoded slot array. This index retains
//! only bounded scalar keys and slot offsets, never another row/body array or
//! any reference-counted owner. No operation scans the preceding ledger.
use super::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane};
use focal_model::lifecycle::aggregation::PublicationPosition;
use read_source::{Meter, model_error};

#[cfg(test)]
#[path = "replay_index_tests.rs"]
mod tests;

/// The actual importer owns these immutable encoded slots in canonical key
/// order. Only their separately stored decoded value may change during phases.
/// In particular a deleted/unbuilt changed key must never fall through to the
/// preceding root when locate returns its slot.
pub(super) trait EncodedRows<'bytes> {
    fn len(&self) -> usize;
    fn encoded(&self, index: usize) -> Option<&EncodedRow<'bytes>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Object {
    Row(Key),
    Registrations(ClaimId),
    Monitor(MonitorId),
    Child(ClaimId),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EventKey {
    object: Object,
    revision: u64,
    ordinal: u32,
}
#[derive(Debug, Clone, Copy)]
struct EventSlot {
    key: EventKey,
    slot: usize,
}

pub(super) struct Index {
    // Buffer must disappear before its funding on every refusal and success.
    events: Vec<EventSlot>,
    _allocation: Allocation,
    header: RecordHeader,
    /// Put counts by the existing recovery dependency phases. Deletions remain
    /// explicit canonical slots; they require no body construction phase.
    pub(super) counts: [usize; read_index::PHASES],
}
fn invalid() -> NativeError {
    ContractError::InvalidManifest.into()
}
fn add(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_add(b).ok_or(ContractError::Capacity.into())
}
fn mul(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_mul(b).ok_or(ContractError::Capacity.into())
}
fn charge(meter: &Meter, amount: usize) -> Result<(), NativeError> {
    meter
        .charge(amount)
        .map_err(|error| model_error(error).into())
}
fn levels(count: usize) -> Result<usize, NativeError> {
    usize::try_from(
        usize::BITS
            .checked_sub(count.leading_zeros())
            .ok_or(ContractError::Capacity)?,
    )
    .map_err(|_| ContractError::Capacity.into())
}
fn lookup(meter: &Meter, count: usize) -> Result<(), NativeError> {
    charge(meter, mul(add(levels(count)?, 1)?, 256)?)
}

/// One canonical search. Some(index) means changed even when that slot has no
/// decoded value yet or explicitly deletes the previous value.
pub(super) fn locate<'bytes>(
    rows: &(impl EncodedRows<'bytes> + ?Sized),
    key: Key,
    meter: &Meter,
) -> Result<Option<usize>, NativeError> {
    lookup(meter, rows.len())?;
    let mut lower = 0usize;
    let mut upper = rows.len();
    while lower < upper {
        let middle = add(
            lower,
            upper
                .checked_sub(lower)
                .ok_or_else(invalid)?
                .checked_div(2)
                .ok_or_else(invalid)?,
        )?;
        let actual = rows.encoded(middle).ok_or_else(invalid)?;
        match actual.key.cmp(&key) {
            std::cmp::Ordering::Less => lower = add(middle, 1)?,
            std::cmp::Ordering::Greater => upper = middle,
            std::cmp::Ordering::Equal => return Ok(Some(middle)),
        }
    }
    Ok(None)
}

fn primary(event: NativeEvent) -> EventKey {
    let (object, revision, ordinal) = match event.fact {
        NativeFact::Claim(value) => (
            Object::Row(Key::Claim(ClaimId(value.after.object.0))),
            value.after.revision.0,
            0,
        ),
        NativeFact::Work { after, .. } => (
            Object::Row(Key::Work(ArtifactId(after.object.0))),
            after.revision.0,
            0,
        ),
        NativeFact::Response { after, .. } => (
            Object::Row(Key::Response(TestamentId(after.object.0))),
            after.revision.0,
            0,
        ),
        NativeFact::ResultTestament { after, .. } => (
            Object::Row(Key::ResultTestament(TestamentId(after.object.0))),
            after.revision.0,
            0,
        ),
        NativeFact::Evaluation { key, after, .. } => {
            (Object::Row(Key::Evaluation(key)), after.revision.0, 0)
        }
        NativeFact::Artifact { binding } => (
            Object::Row(Key::Artifact(ArtifactId(binding.object.0))),
            binding.revision.0,
            0,
        ),
        NativeFact::Diagnostic { binding, .. } => (
            Object::Row(Key::Diagnostic(ArtifactId(binding.object.0))),
            binding.revision.0,
            0,
        ),
        NativeFact::Definition { binding, .. } => (
            Object::Row(Key::Definition(ValidationId(binding.object.0))),
            binding.revision.0,
            0,
        ),
        NativeFact::Accepted { key } => (Object::Row(Key::Accepted(key)), key.revision.0, 0),
        NativeFact::Delivery { key } => (Object::Row(Key::DeliveryResult(key)), key.revision.0, 0),
        NativeFact::Missing { key } => (Object::Row(Key::MissingResult(key)), key.revision.0, 0),
        NativeFact::Receipt { fence, .. } => {
            (Object::Row(Key::Receipt(fence.receipt)), fence.epoch, 0)
        }
        NativeFact::ReceiptAdopted { replacement, .. } => (
            Object::Row(Key::Receipt(replacement.fence.receipt)),
            replacement.fence.epoch,
            0,
        ),
        NativeFact::Registrations { claim } => (
            Object::Registrations(ClaimId(claim.object.0)),
            event.sequence.0,
            event.ordinal,
        ),
    };
    EventKey {
        object,
        revision,
        ordinal,
    }
}
fn secondary(event: NativeEvent) -> Option<EventKey> {
    match event.fact {
        NativeFact::Delivery { key } => Some(EventKey {
            object: Object::Row(Key::Evaluation(key.evaluation)),
            revision: key.revision.0,
            ordinal: 0,
        }),
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::Monitor(monitor),
            ..
        }) => Some(EventKey {
            object: Object::Monitor(monitor.id()),
            revision: event.sequence.0,
            ordinal: event.ordinal,
        }),
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::ChildRegistered,
            owned_child: Some(child),
            ..
        }) => Some(EventKey {
            object: Object::Child(ClaimId(child.object.0)),
            revision: 0,
            ordinal: 0,
        }),
        _ => None,
    }
}
fn target(key: EventKey) -> Key {
    match key.object {
        Object::Row(key) => key,
        Object::Registrations(claim) | Object::Child(claim) => Key::Claim(claim),
        Object::Monitor(id) => Key::Monitor(id),
    }
}
fn read<'bytes>(
    rows: &(impl EncodedRows<'bytes> + ?Sized),
    slot: usize,
    header: RecordHeader,
    parsing: &Meter,
    work: &Meter,
) -> Result<NativeEvent, NativeError> {
    charge(work, 256)?;
    let encoded = rows.encoded(slot).ok_or_else(invalid)?;
    let Key::Event(sequence, ordinal) = encoded.key else {
        return Err(invalid());
    };
    if encoded.deleted() || sequence != header.outcome.sequence || ordinal >= header.outcome.events
    {
        return Err(invalid());
    }
    let (event, used) = parsing
        .read(encoded.body(), read_events::event)
        .map_err(model_error)?;
    if used != encoded.body().len()
        || event.sequence != sequence
        || event.ordinal != ordinal
        || event.invocation != header.outcome.invocation
    {
        return Err(invalid());
    }
    Ok(event)
}
fn push(rows: &mut Vec<EventSlot>, row: EventSlot) -> Result<(), NativeError> {
    if rows.len() == rows.capacity() {
        return Err(ContractError::Capacity.into());
    }
    rows.push(row);
    Ok(())
}
fn swap(rows: &mut [EventSlot], left: usize, right: usize) -> Result<(), NativeError> {
    let a = *rows.get(left).ok_or_else(invalid)?;
    let b = *rows.get(right).ok_or_else(invalid)?;
    *rows.get_mut(left).ok_or_else(invalid)? = b;
    *rows.get_mut(right).ok_or_else(invalid)? = a;
    Ok(())
}
fn sift(rows: &mut [EventSlot], mut root: usize, end: usize) -> Result<(), NativeError> {
    loop {
        let left = add(mul(root, 2)?, 1)?;
        if left >= end {
            return Ok(());
        }
        let right = add(left, 1)?;
        let child = if right < end
            && rows.get(right).ok_or_else(invalid)?.key > rows.get(left).ok_or_else(invalid)?.key
        {
            right
        } else {
            left
        };
        if rows.get(root).ok_or_else(invalid)?.key >= rows.get(child).ok_or_else(invalid)?.key {
            return Ok(());
        }
        swap(rows, root, child)?;
        root = child;
    }
}
fn sort(rows: &mut [EventSlot]) -> Result<(), NativeError> {
    let length = rows.len();
    for root in (0..length.checked_div(2).ok_or_else(invalid)?).rev() {
        sift(rows, root, length)?;
    }
    for end in (1..length).rev() {
        swap(rows, 0, end)?;
        sift(rows, 0, end)?;
    }
    Ok(())
}

impl Index {
    pub(super) fn build<'bytes>(
        rows: &(impl EncodedRows<'bytes> + ?Sized),
        header: RecordHeader,
        limits: NativeLimits,
        budget: &MemoryBudget,
        parsing: &Meter,
        work: &Meter,
    ) -> Result<Self, NativeError> {
        let count = rows.len();
        let events = usize::try_from(header.outcome.events).map_err(|_| ContractError::Capacity)?;
        if count == 0
            || count > limits.range.max_batch_entries
            || events > limits.events
            || events > count
            || header.ledger != header.outcome.ledger
            || header.base.0.checked_add(1) != Some(header.outcome.sequence.0)
        {
            return Err(ContractError::Capacity.into());
        }
        charge(work, mul(add(count, 1)?, 384)?)?;
        let capacity = mul(events, 2)?;
        // Three sift schedules, each at most height child/root comparisons;
        // fixed-size EventKey comparison+move work fits this prepaid bound.
        charge(work, mul(mul(capacity, add(levels(capacity)?, 1)?)?, 1024)?)?;
        let bytes = crate::native::prepare::array::<EventSlot>(capacity)?;
        let allocation = budget
            .reserve(BudgetKind::Recovery, BudgetLane::Completion, bytes)?
            .commit();
        let mut indexed = Vec::new();
        indexed
            .try_reserve_exact(capacity)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if indexed.capacity() != capacity {
            return Err(MemoryError::AllocationFailed.into());
        }
        let mut counts = [0usize; read_index::PHASES];
        let mut previous = None;
        let mut ordinal = 0u32;
        for slot in 0..count {
            let encoded = rows.encoded(slot).ok_or_else(invalid)?;
            if previous.is_some_and(|previous| previous >= encoded.key) {
                return Err(invalid());
            }
            previous = Some(encoded.key);
            let phase = read_index::phase(encoded.key)?;
            if !encoded.deleted() {
                let count = counts.get_mut(phase).ok_or_else(invalid)?;
                *count = add(*count, 1)?;
            }
            if let Key::Event(sequence, actual) = encoded.key {
                if sequence != header.outcome.sequence || actual != ordinal {
                    return Err(invalid());
                }
                let event = read(rows, slot, header, parsing, work)?;
                ordinal = ordinal.checked_add(1).ok_or(ContractError::Capacity)?;
                let key = primary(event);
                // Every primary fact describes a changed final row. Historical
                // base fallback cannot make an orphan new fact look complete.
                let destination = locate(rows, target(key), work)?.ok_or_else(invalid)?;
                if rows.encoded(destination).ok_or_else(invalid)?.deleted() {
                    return Err(invalid());
                }
                push(&mut indexed, EventSlot { key, slot })?;
                if let Some(key) = secondary(event) {
                    if matches!(event.fact, NativeFact::Delivery { .. }) {
                        let destination = locate(rows, target(key), work)?.ok_or_else(invalid)?;
                        if rows.encoded(destination).ok_or_else(invalid)?.deleted() {
                            return Err(invalid());
                        }
                    }
                    push(&mut indexed, EventSlot { key, slot })?;
                }
            }
        }
        if ordinal != header.outcome.events {
            return Err(invalid());
        }
        sort(&mut indexed)?;
        charge(work, mul(add(indexed.len(), 1)?, 256)?)?;
        let mut previous = None;
        for event in &indexed {
            if previous == Some(event.key) {
                return Err(invalid());
            }
            previous = Some(event.key);
        }
        Ok(Self {
            events: indexed,
            _allocation: allocation,
            header,
            counts,
        })
    }
    fn range(&self, object: Object, work: &Meter) -> Result<&[EventSlot], NativeError> {
        lookup(work, self.events.len())?;
        let lower = self
            .events
            .partition_point(|event| event.key.object < object);
        lookup(work, self.events.len())?;
        let upper = self
            .events
            .partition_point(|event| event.key.object <= object);
        self.events.get(lower..upper).ok_or_else(invalid)
    }
    fn each<'bytes>(
        &self,
        rows: &(impl EncodedRows<'bytes> + ?Sized),
        object: Object,
        parsing: &Meter,
        work: &Meter,
        mut visit: impl FnMut(NativeEvent) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        let selected = self.range(object, work)?;
        charge(work, add(selected.len(), 1)?)?;
        for slot in selected {
            visit(read(rows, slot.slot, self.header, parsing, work)?)?;
        }
        Ok(())
    }
    pub(super) fn events<'bytes>(
        &self,
        rows: &(impl EncodedRows<'bytes> + ?Sized),
        object: Key,
        parsing: &Meter,
        work: &Meter,
        visit: impl FnMut(NativeEvent) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        self.each(rows, Object::Row(object), parsing, work, visit)
    }
    pub(super) fn registrations<'bytes>(
        &self,
        rows: &(impl EncodedRows<'bytes> + ?Sized),
        claim: ClaimId,
        parsing: &Meter,
        work: &Meter,
        visit: impl FnMut(NativeEvent) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        self.each(rows, Object::Registrations(claim), parsing, work, visit)
    }
    pub(super) fn monitors<'bytes>(
        &self,
        rows: &(impl EncodedRows<'bytes> + ?Sized),
        id: MonitorId,
        parsing: &Meter,
        work: &Meter,
        visit: impl FnMut(NativeEvent) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        self.each(rows, Object::Monitor(id), parsing, work, visit)
    }
    pub(super) fn first<'bytes>(
        &self,
        rows: &(impl EncodedRows<'bytes> + ?Sized),
        object: Key,
        parsing: &Meter,
        work: &Meter,
    ) -> Result<Option<NativeEvent>, NativeError> {
        self.range(Object::Row(object), work)?
            .first()
            .map(|slot| read(rows, slot.slot, self.header, parsing, work))
            .transpose()
    }
    pub(super) fn child<'bytes>(
        &self,
        rows: &(impl EncodedRows<'bytes> + ?Sized),
        id: ClaimId,
        parsing: &Meter,
        work: &Meter,
    ) -> Result<Option<NativeEvent>, NativeError> {
        match self.range(Object::Child(id), work)? {
            [] => Ok(None),
            [slot] => read(rows, slot.slot, self.header, parsing, work).map(Some),
            _ => Err(invalid()),
        }
    }
    pub(super) fn artifact<'bytes>(
        &self,
        rows: &(impl EncodedRows<'bytes> + ?Sized),
        id: ArtifactId,
        parsing: &Meter,
        work: &Meter,
    ) -> Result<read_index::Origin, NativeError> {
        let [slot] = self.range(Object::Row(Key::Artifact(id)), work)? else {
            return Err(invalid());
        };
        let event = read(rows, slot.slot, self.header, parsing, work)?;
        let NativeFact::Artifact { binding } = event.fact else {
            return Err(invalid());
        };
        let NativeInvocation::Request(request) = event.invocation else {
            return Err(invalid());
        };
        if binding.ledger != self.header.ledger || binding.object.0 != id.0 || id.is_zero() {
            return Err(invalid());
        }
        Ok(read_index::Origin {
            request: read_index::ArtifactRequest::Request(request),
            binding,
            position: PublicationPosition {
                sequence: event.sequence,
                ordinal: event.ordinal,
            },
        })
    }
}
