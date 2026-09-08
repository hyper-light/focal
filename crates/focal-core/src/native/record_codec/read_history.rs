//! Caller-funded scalar secondary history index. One event stream captures
//! object keys and original publication coordinates; bodies stay in their rows.
//! An in-place bounded sort enables object-local walks without repeated root scans.
use super::read_validate::{ValidationRead, invalid};
use super::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane};
use focal_model::lifecycle::aggregation::PublicationPosition;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Object {
    Row(Key),
    Registrations(ClaimId),
    Monitor(MonitorId),
    ChildRegistration(ClaimId),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct HistoryKey {
    object: Object,
    revision: u64,
    ordinal: u32,
}
#[derive(Debug, Clone, Copy)]
struct IndexedEvent {
    key: HistoryKey,
    position: PublicationPosition,
}
pub(super) struct HistoryIndex {
    rows: Vec<IndexedEvent>,
    _allocation: Allocation,
}
fn lookup(read: &ValidationRead<'_, '_>) -> Result<(), NativeError> {
    read.charge(
        usize::try_from(usize::BITS)
            .map_err(|_| invalid())?
            .checked_add(1)
            .and_then(|n| n.checked_mul(256))
            .ok_or_else(invalid)?,
    )
}
fn key(event: NativeEvent) -> HistoryKey {
    let (object, revision, ordinal) = match event.fact {
        NativeFact::Claim(claim) => (
            Object::Row(Key::Claim(ClaimId(claim.after.object.0))),
            claim.after.revision.0,
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
    HistoryKey {
        object,
        revision,
        ordinal,
    }
}
// In-place heapsort has no allocator or error-hiding comparison callback.
// Each sift performs at most height comparisons of two children and the root.
// The three linear sift schedules fit n*(height+1)*1024 fixed-key work units.
fn sort_visits(count: usize) -> Result<usize, NativeError> {
    let height = usize::BITS
        .checked_sub(count.leading_zeros())
        .ok_or_else(invalid)?;
    count
        .checked_mul(
            usize::try_from(height)
                .map_err(|_| invalid())?
                .checked_add(1)
                .ok_or_else(invalid)?,
        )
        .and_then(|n| n.checked_mul(1024))
        .ok_or_else(invalid)
}
fn swap(rows: &mut [IndexedEvent], a: usize, b: usize) -> Result<(), NativeError> {
    let left = *rows.get(a).ok_or_else(invalid)?;
    let right = *rows.get(b).ok_or_else(invalid)?;
    *rows.get_mut(a).ok_or_else(invalid)? = right;
    *rows.get_mut(b).ok_or_else(invalid)? = left;
    Ok(())
}
fn sift(rows: &mut [IndexedEvent], mut root: usize, end: usize) -> Result<(), NativeError> {
    loop {
        let left = root
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(invalid)?;
        if left >= end {
            return Ok(());
        }
        let right = left.checked_add(1).ok_or_else(invalid)?;
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
fn sort(rows: &mut [IndexedEvent]) -> Result<(), NativeError> {
    let length = rows.len();
    for root in (0..length.checked_div(2).ok_or_else(invalid)?).rev() {
        sift(rows, root, length)?;
    }
    for end in (1..rows.len()).rev() {
        swap(rows, 0, end)?;
        sift(rows, 0, end)?;
    }
    Ok(())
}
impl HistoryIndex {
    pub(super) fn build(read: &ValidationRead<'_, '_>) -> Result<Self, NativeError> {
        let Row::Meta(meta) = read.require(Key::Meta)? else {
            return Err(invalid());
        };
        let count = meta.events;
        if count > read.limits.events {
            return Err(NativeError::Capacity("recovery history events"));
        }
        let capacity = count.checked_mul(2).ok_or_else(invalid)?;
        read.charge(sort_visits(capacity)?)?;
        let charge = capacity
            .checked_mul(size_of::<IndexedEvent>())
            .and_then(|bytes| {
                bytes.checked_add(if count == 0 {
                    0
                } else {
                    crate::native::prepare::ALLOCATION
                })
            })
            .ok_or_else(invalid)?;
        // The temporary peak is capacity=2*events scalar key/coordinate entries
        // plus one allocator header: at most one monitor secondary entry per fact. Event
        // bodies remain borrowed from the detached recovered range, and heapsort
        // uses constant stack state. The permit lasts through root validation.
        let allocation = read
            .budget
            .reserve(BudgetKind::Recovery, BudgetLane::Completion, charge)?
            .commit();
        let mut rows = Vec::new();
        rows.try_reserve_exact(capacity)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if rows.capacity() != capacity {
            return Err(MemoryError::AllocationFailed.into());
        }
        let mut observed = 0_usize;
        lookup(read)?;
        for row in read.root.entries_from(&Key::Event(SessionSeq(0), 0), false) {
            read.charge(128)?;
            let Key::Event(sequence, ordinal) = row.key else {
                break;
            };
            if observed >= count {
                return Err(invalid());
            }
            observed = observed.checked_add(1).ok_or_else(invalid)?;
            let Row::Event(stored) = &row.value else {
                return Err(invalid());
            };
            let event = stored.get().ok_or_else(invalid)?.expand(read.ledger);
            if event.sequence != sequence
                || event.ordinal != ordinal
                || sequence.0 == 0
                || sequence > read.prefix
            {
                return Err(invalid());
            }
            let key = key(event);
            if let Object::Row(object) = key.object {
                read.require(object)?;
            }
            rows.push(IndexedEvent {
                key,
                position: PublicationPosition { sequence, ordinal },
            });
            if let NativeFact::Claim(NativeClaimEvent {
                kind: NativeEventKind::Monitor(monitor),
                ..
            }) = event.fact
            {
                rows.push(IndexedEvent {
                    key: HistoryKey {
                        object: Object::Monitor(monitor.id()),
                        revision: sequence.0,
                        ordinal,
                    },
                    position: PublicationPosition { sequence, ordinal },
                });
            }
            if let NativeFact::Claim(NativeClaimEvent {
                kind: NativeEventKind::ChildRegistered,
                owned_child: Some(child),
                ..
            }) = event.fact
            {
                rows.push(IndexedEvent {
                    key: HistoryKey {
                        object: Object::ChildRegistration(ClaimId(child.object.0)),
                        revision: 0,
                        ordinal: 0,
                    },
                    position: PublicationPosition { sequence, ordinal },
                });
            }
        }
        read.charge(1)?;
        if observed != count {
            return Err(invalid());
        }
        sort(&mut rows)?;
        read.charge(
            rows.len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(256))
                .ok_or_else(invalid)?,
        )?;
        let mut previous = None;
        for row in &rows {
            if previous == Some(row.key) {
                return Err(invalid());
            }
            previous = Some(row.key);
        }
        Ok(Self {
            rows,
            _allocation: allocation,
        })
    }
    fn lower(&self, object: Object, read: &ValidationRead<'_, '_>) -> Result<usize, NativeError> {
        lookup(read)?;
        let key = HistoryKey {
            object,
            revision: 0,
            ordinal: 0,
        };
        Ok(self.rows.partition_point(|entry| entry.key < key))
    }
    fn walk(
        &self,
        object: Object,
        read: &ValidationRead<'_, '_>,
        mut visit: impl FnMut(NativeEvent) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        let first = self.lower(object, read)?;
        for entry in self.rows.get(first..).ok_or_else(invalid)? {
            read.charge(256)?;
            if entry.key.object != object {
                break;
            }
            visit(read.event(entry.position.sequence, entry.position.ordinal)?)?;
        }
        read.charge(1)
    }
    pub(super) fn events(
        &self,
        object: Key,
        read: &ValidationRead<'_, '_>,
        visit: impl FnMut(NativeEvent) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        self.walk(Object::Row(object), read, visit)
    }
    pub(super) fn registrations(
        &self,
        claim: ClaimId,
        read: &ValidationRead<'_, '_>,
        visit: impl FnMut(NativeEvent) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        self.walk(Object::Registrations(claim), read, visit)
    }
    pub(super) fn monitors(
        &self,
        id: MonitorId,
        read: &ValidationRead<'_, '_>,
        visit: impl FnMut(NativeEvent) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        self.walk(Object::Monitor(id), read, visit)
    }
    pub(super) fn child(
        &self,
        id: ClaimId,
        read: &ValidationRead<'_, '_>,
    ) -> Result<Option<NativeEvent>, NativeError> {
        let object = Object::ChildRegistration(id);
        let first = self.lower(object, read)?;
        read.charge(256)?;
        match self
            .rows
            .get(first)
            .filter(|entry| entry.key.object == object)
        {
            Some(entry) => read
                .event(entry.position.sequence, entry.position.ordinal)
                .map(Some),
            None => Ok(None),
        }
    }
    pub(super) fn revision(
        &self,
        object: Key,
        revision: u64,
        read: &ValidationRead<'_, '_>,
    ) -> Result<Option<NativeEvent>, NativeError> {
        lookup(read)?;
        let key = HistoryKey {
            object: Object::Row(object),
            revision,
            ordinal: 0,
        };
        match self.rows.binary_search_by_key(&key, |entry| entry.key) {
            Ok(index) => {
                let entry = self.rows.get(index).ok_or_else(invalid)?;
                read.event(entry.position.sequence, entry.position.ordinal)
                    .map(Some)
            }
            Err(_) => Ok(None),
        }
    }
    /// The whole-root chain pass verifies publication order agrees with these
    /// revision keys before this historical view may contribute to acceptance.
    pub(super) fn at_or_before(
        &self,
        object: Key,
        sequence: SessionSeq,
        read: &ValidationRead<'_, '_>,
    ) -> Result<Option<NativeEvent>, NativeError> {
        let object = Object::Row(object);
        let start = self.lower(object, read)?;
        lookup(read)?;
        let end = self
            .rows
            .partition_point(|entry| entry.key.object <= object);
        let rows = self.rows.get(start..end).ok_or_else(invalid)?;
        lookup(read)?;
        let count = rows.partition_point(|entry| entry.position.sequence <= sequence);
        match count.checked_sub(1).and_then(|index| rows.get(index)) {
            Some(entry) => read
                .event(entry.position.sequence, entry.position.ordinal)
                .map(Some),
            None => Ok(None),
        }
    }
    pub(super) fn first(
        &self,
        object: Key,
        read: &ValidationRead<'_, '_>,
    ) -> Result<Option<NativeEvent>, NativeError> {
        let first = self.lower(Object::Row(object), read)?;
        read.charge(256)?;
        match self
            .rows
            .get(first)
            .filter(|entry| entry.key.object == Object::Row(object))
        {
            Some(entry) => read
                .event(entry.position.sequence, entry.position.ordinal)
                .map(Some),
            None => Ok(None),
        }
    }
}
