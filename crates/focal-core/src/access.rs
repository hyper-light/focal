//! Actual reducer accesses. No table exposes its underlying map or implements
//! Deref; obtaining a mutable row first records a conservative whole-row write.
use crate::overlay::{Backing, RowVersion, RowWrites};
use crate::*;
use std::cell::{Cell, RefCell};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StateTable {
    Claims,
    Validations,
    Artifacts,
    Testaments,
    EvidenceSets,
    Runs,
    Monitors,
    Identities,
    Epochs,
    Receipts,
}

/// A row includes all of its owned fields. Claim rows include lifecycle,
/// adjacency, required-validation references and deadlines; monitor rows include
/// registrations and roots; run rows include attempt counters and aggregates.
/// A scan covers the entire table, including currently absent matching rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AccessKey {
    Ledger,
    Sequence,
    Limits,
    Scan(StateTable),
    Count(StateTable),
    Claim(ClaimId),
    Validation(ValidationId),
    Artifact(ArtifactId),
    Testament(TestamentId),
    EvidenceSet(EvidenceSetId),
    Run(ValidationRunId),
    Monitor(MonitorId),
    Identity(ObjectKind, ContentHash),
    Epoch(ParticipantId),
    Receipt(RequestKey),
}
impl AccessKey {
    pub fn table(self) -> Option<StateTable> {
        Some(match self {
            Self::Scan(table) | Self::Count(table) => table,
            Self::Claim(_) => StateTable::Claims,
            Self::Validation(_) => StateTable::Validations,
            Self::Artifact(_) => StateTable::Artifacts,
            Self::Testament(_) => StateTable::Testaments,
            Self::EvidenceSet(_) => StateTable::EvidenceSets,
            Self::Run(_) => StateTable::Runs,
            Self::Monitor(_) => StateTable::Monitors,
            Self::Identity(..) => StateTable::Identities,
            Self::Epoch(_) => StateTable::Epochs,
            Self::Receipt(_) => StateTable::Receipts,
            Self::Ledger | Self::Sequence | Self::Limits => return None,
        })
    }
    pub fn covers(self, other: Self) -> bool {
        self == other || matches!(self, Self::Scan(table) if other.table() == Some(table))
    }
    pub fn overlaps(self, other: Self) -> bool {
        self.covers(other) || other.covers(self)
    }
}

/// A bounded complete over-approximation of accesses actually performed.
/// `session_exclusive` means all reads and writes, and takes precedence over the
/// two sets. Budget exhaustion collapses to that representation without failing
/// the domain operation or silently dropping accesses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessFootprint {
    pub ledger: LedgerId,
    pub base: SessionSeq,
    pub reads: BTreeSet<AccessKey>,
    pub writes: BTreeSet<AccessKey>,
    pub session_exclusive: bool,
}
impl AccessFootprint {
    pub fn covers(&self, actual: &Self) -> bool {
        self.ledger == actual.ledger
            && self.base == actual.base
            && (self.session_exclusive
                || (!actual.session_exclusive
                    && actual.reads.iter().all(|key| set_covers(&self.reads, *key))
                    && actual
                        .writes
                        .iter()
                        .all(|key| set_covers(&self.writes, *key))))
    }
    /// RAW, WAR or WAW overlap. Read/read overlap alone is harmless. Prefix
    /// equality is audited separately by the caller when assembling an epoch.
    pub fn conflicts(&self, other: &Self) -> bool {
        self.ledger == other.ledger
            && (self.session_exclusive
                || other.session_exclusive
                || self.writes.iter().any(|left| {
                    set_overlaps(&other.reads, *left) || set_overlaps(&other.writes, *left)
                })
                || self
                    .reads
                    .iter()
                    .any(|left| set_overlaps(&other.writes, *left)))
    }
}

fn set_covers(set: &BTreeSet<AccessKey>, key: AccessKey) -> bool {
    set.contains(&key)
        || key
            .table()
            .is_some_and(|table| set.contains(&AccessKey::Scan(table)))
}
fn set_overlaps(set: &BTreeSet<AccessKey>, key: AccessKey) -> bool {
    set_covers(set, key)
        || matches!(key, AccessKey::Scan(table) if set.iter().any(|other| other.table() == Some(table)))
}

#[derive(Debug)]
pub struct TrackedResult<T, E> {
    pub result: Result<T, E>,
    pub accesses: AccessFootprint,
}

pub(crate) struct Recorder {
    footprint: RefCell<AccessFootprint>,
    max_entries: usize,
    disabled: bool,
    collapsed: Cell<bool>,
    base: SessionSeq,
    versions: bool,
    observations: RefCell<BTreeSet<ReadObservation>>,
    byte_limit: Option<usize>,
    used_bytes: Cell<usize>,
    failed: Cell<bool>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct ReadObservation {
    pub key: AccessKey,
    pub version: SessionSeq,
    pub count: Option<usize>,
}
impl Recorder {
    pub(crate) fn bounded(ledger: LedgerId, base: SessionSeq, max_bytes: usize) -> Self {
        Self {
            byte_limit: Some(max_bytes),
            ..Self::disabled(ledger, base)
        }
    }
    pub(crate) fn epoch(
        ledger: LedgerId,
        base: SessionSeq,
        max_entries: usize,
        max_bytes: usize,
    ) -> Self {
        Self {
            versions: true,
            byte_limit: Some(max_bytes),
            ..Self::new(ledger, base, max_entries)
        }
    }
    pub(crate) fn failed(&self) -> bool {
        self.failed.get()
    }
    pub(crate) fn finish_epoch(mut self) -> (AccessFootprint, BTreeSet<ReadObservation>, usize) {
        let mut observations = std::mem::take(self.observations.get_mut());
        let bytes = self.used_bytes.get();
        let footprint = self.finish();
        if footprint.session_exclusive {
            observations.clear();
        }
        (footprint, observations, bytes)
    }
    pub(crate) fn new(ledger: LedgerId, base: SessionSeq, max_entries: usize) -> Self {
        Self {
            footprint: RefCell::new(AccessFootprint {
                ledger,
                base,
                reads: BTreeSet::new(),
                writes: BTreeSet::new(),
                session_exclusive: false,
            }),
            max_entries,
            disabled: false,
            collapsed: Cell::new(false),
            base,
            versions: false,
            observations: RefCell::new(BTreeSet::new()),
            byte_limit: None,
            used_bytes: Cell::new(0),
            failed: Cell::new(false),
        }
    }
    pub(crate) fn disabled(ledger: LedgerId, base: SessionSeq) -> Self {
        Self {
            disabled: true,
            ..Self::new(ledger, base, 0)
        }
    }
    fn record(&self, key: AccessKey, write: bool) {
        if self.disabled || self.collapsed.get() {
            return;
        }
        // The borrow never escapes this method. A future reentrant caller still
        // cannot panic or produce a partial footprint: it forces safe collapse.
        let Ok(mut footprint) = self.footprint.try_borrow_mut() else {
            self.collapsed.set(true);
            return;
        };
        let exists = if write {
            footprint.writes.contains(&key)
        } else {
            footprint.reads.contains(&key)
        };
        if exists {
            return;
        }
        if footprint
            .reads
            .len()
            .checked_add(footprint.writes.len())
            .is_none_or(|size| size >= self.max_entries)
        {
            footprint.reads.clear();
            footprint.writes.clear();
            self.collapsed.set(true);
            return;
        }
        if write {
            footprint.writes.insert(key);
        } else {
            footprint.reads.insert(key);
        }
    }
    pub(crate) fn read(&self, key: AccessKey) {
        self.record(key, false);
    }
    pub(crate) fn base(&self) -> SessionSeq {
        self.base
    }
    pub(crate) fn observe(&self, key: AccessKey, version: SessionSeq) {
        self.observe_value(key, version, None);
    }
    fn observe_value(&self, key: AccessKey, version: SessionSeq, count: Option<usize>) {
        self.read(key);
        if !self.versions || self.collapsed.get() {
            return;
        }
        let Ok(mut observed) = self.observations.try_borrow_mut() else {
            self.collapsed.set(true);
            return;
        };
        let observation = ReadObservation {
            key,
            version,
            count,
        };
        if !observed.contains(&observation) && observed.len() >= self.max_entries {
            observed.clear();
            self.collapsed.set(true);
            return;
        }
        observed.insert(observation);
    }
    pub(crate) fn invalidate(&self) {
        self.failed.set(true);
    }
    pub(crate) fn reserve_value<T: Serialize>(&self, value: &T) -> bool {
        if self.byte_limit.is_none() {
            return true;
        }
        let size = postcard::experimental::serialized_size(value)
            .ok()
            .and_then(|size| size.checked_mul(32))
            .and_then(|size| size.checked_add(512));
        match size {
            Some(size) => self.reserve_bytes(size),
            None => {
                self.invalidate();
                false
            }
        }
    }
    pub(crate) fn reserve_bytes(&self, bytes: usize) -> bool {
        let Some(limit) = self.byte_limit else {
            return true;
        };
        let size = self.used_bytes.get().checked_add(bytes);
        if let Some(size) = size
            && size <= limit
        {
            self.used_bytes.set(size);
            true
        } else {
            self.failed.set(true);
            false
        }
    }
    pub(crate) fn write(&self, key: AccessKey) {
        self.record(key, true);
    }
    pub(crate) fn finish(self) -> AccessFootprint {
        let mut footprint = self.footprint.into_inner();
        if self.collapsed.get() || self.disabled {
            footprint.reads.clear();
            footprint.writes.clear();
            footprint.session_exclusive = true;
        }
        footprint
    }
}

pub(crate) trait TableKey: Copy + Ord {
    const TABLE: StateTable;
    fn access(self) -> AccessKey;
}
macro_rules! key {
    ($key:ty, $table:ident, $variant:ident) => {
        impl TableKey for $key {
            const TABLE: StateTable = StateTable::$table;
            fn access(self) -> AccessKey {
                AccessKey::$variant(self)
            }
        }
    };
}
key!(ClaimId, Claims, Claim);
key!(ValidationId, Validations, Validation);
key!(ArtifactId, Artifacts, Artifact);
key!(TestamentId, Testaments, Testament);
key!(EvidenceSetId, EvidenceSets, EvidenceSet);
key!(ValidationRunId, Runs, Run);
key!(MonitorId, Monitors, Monitor);
key!(ParticipantId, Epochs, Epoch);
key!(RequestKey, Receipts, Receipt);
impl TableKey for (ObjectKind, ContentHash) {
    const TABLE: StateTable = StateTable::Identities;
    fn access(self) -> AccessKey {
        AccessKey::Identity(self.0, self.1)
    }
}

pub(crate) struct ReadOnly;
pub(crate) struct Writable;
pub(crate) struct Table<'a, K, V, M> {
    storage: Backing<'a, K, V>,
    recorder: &'a Recorder,
    mode: std::marker::PhantomData<M>,
}
impl<'a, K: TableKey, V: Clone + Serialize, M> Table<'a, K, V, M> {
    pub(crate) fn get_owned(&self, key: &K) -> Result<Option<V>, DomainOutcome> {
        let Some(value) = self.get(key) else {
            return Ok(None);
        };
        if !self.recorder.reserve_value(value) {
            return Err(refuse(ErrorCode::Capacity, "owned row workspace exhausted"));
        }
        Ok(Some(value.clone()))
    }
    fn new(storage: Backing<'a, K, V>, recorder: &'a Recorder) -> Self {
        Self {
            storage,
            recorder,
            mode: std::marker::PhantomData,
        }
    }
    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        let row = self.storage.get(key, self.recorder.base());
        self.recorder.observe(
            key.access(),
            row.map_or(self.recorder.base(), |(_, _, version)| version),
        );
        row.map(|(_, value, _)| value)
    }
    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
    pub(crate) fn len(&self) -> usize {
        let value = self.storage.len().unwrap_or_else(|| {
            self.recorder.invalidate();
            usize::MAX
        });
        self.recorder.observe_value(
            AccessKey::Count(K::TABLE),
            self.storage.watermark(self.recorder.base(), true),
            Some(value),
        );
        value
    }
    pub(crate) fn iter(&self) -> Rows<'_, 'a, K, V, M> {
        self.recorder.observe(
            AccessKey::Scan(K::TABLE),
            self.storage.watermark(self.recorder.base(), false),
        );
        Rows {
            table: self,
            after: None,
            native: self.storage.native_iter(),
        }
    }
    pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(key, _)| key)
    }
    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, value)| value)
    }
}
impl<K: TableKey, V: Clone + Serialize> Table<'_, K, V, Writable> {
    pub(crate) fn insert(&mut self, key: K, value: V) {
        let existed = self.get(&key).is_some();
        self.recorder.write(key.access());
        if !existed {
            self.recorder.write(AccessKey::Count(K::TABLE));
        }
        self.storage.insert(key, value, self.recorder);
    }
    pub(crate) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let exists = self.get(key).is_some();
        if exists {
            self.recorder.write(key.access());
        }
        self.storage.get_mut(key, self.recorder)
    }
}
pub(crate) struct Rows<'r, 'a, K, V, M> {
    table: &'r Table<'a, K, V, M>,
    after: Option<K>,
    native: Option<std::collections::btree_map::Iter<'r, K, V>>,
}
impl<'r, K: TableKey, V: Clone + Serialize, M> Iterator for Rows<'r, '_, K, V, M> {
    type Item = (&'r K, &'r V);
    fn next(&mut self) -> Option<Self::Item> {
        let (key, value, version) = if let Some(native) = &mut self.native {
            let (key, value) = native.next()?;
            (key, value, self.table.recorder.base())
        } else {
            self.table
                .storage
                .next(self.after, self.table.recorder.base())?
        };
        self.after = Some(*key);
        self.table.recorder.observe(key.access(), version);
        Some((key, value))
    }
}
impl<'r, 'a, K: TableKey, V: Clone + Serialize, M> IntoIterator for &'r Table<'a, K, V, M> {
    type Item = (&'r K, &'r V);
    type IntoIter = Rows<'r, 'a, K, V, M>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
enum Prefix<'a> {
    Borrowed(&'a mut SessionSeq),
    Assigned(SessionSeq),
}
type WriteTable<'a, K, V> = Table<'a, K, V, Writable>;
pub(crate) struct WriteState<'a> {
    ledger: LedgerId,
    sequence: Prefix<'a>,
    recorder: &'a Recorder,
    pub(crate) claims: WriteTable<'a, ClaimId, Claim>,
    pub(crate) validations: WriteTable<'a, ValidationId, Validation>,
    pub(crate) artifacts: WriteTable<'a, ArtifactId, Artifact>,
    pub(crate) testaments: WriteTable<'a, TestamentId, Testament>,
    pub(crate) evidence_sets: WriteTable<'a, EvidenceSetId, EvidenceSet>,
    pub(crate) runs: WriteTable<'a, ValidationRunId, ValidationRun>,
    pub(crate) monitors: WriteTable<'a, MonitorId, Monitor>,
    pub(crate) identities: WriteTable<'a, (ObjectKind, ContentHash), ObjectId>,
    pub(crate) epochs: WriteTable<'a, ParticipantId, EpochWindow>,
    pub(crate) receipts: WriteTable<'a, RequestKey, MutationReceipt>,
}
impl<'a> WriteState<'a> {
    pub(crate) fn scratch(&self, items: usize) -> Result<(), DomainOutcome> {
        if items
            .checked_mul(128)
            .is_some_and(|bytes| self.recorder.reserve_bytes(bytes))
        {
            Ok(())
        } else {
            self.recorder.invalidate();
            Err(refuse(
                ErrorCode::Capacity,
                "reducer scratch workspace exhausted",
            ))
        }
    }
    pub(crate) fn check_capacity(&self) -> Result<(), DomainOutcome> {
        if self.recorder.failed() {
            Err(refuse(ErrorCode::Capacity, "reducer workspace exhausted"))
        } else {
            Ok(())
        }
    }
    pub(crate) fn charge<T: Serialize>(&self, value: &T) -> Result<(), DomainOutcome> {
        if self.recorder.reserve_value(value) {
            Ok(())
        } else {
            Err(refuse(
                ErrorCode::Capacity,
                "owned row/output workspace exhausted",
            ))
        }
    }
    pub(crate) fn new(state: &'a mut State, recorder: &'a Recorder) -> Self {
        Self {
            ledger: state.ledger,
            sequence: Prefix::Borrowed(&mut state.sequence),
            recorder,
            claims: Table::new(Backing::Write(&mut state.claims), recorder),
            validations: Table::new(Backing::Write(&mut state.validations), recorder),
            artifacts: Table::new(Backing::Write(&mut state.artifacts), recorder),
            testaments: Table::new(Backing::Write(&mut state.testaments), recorder),
            evidence_sets: Table::new(Backing::Write(&mut state.evidence_sets), recorder),
            runs: Table::new(Backing::Write(&mut state.runs), recorder),
            monitors: Table::new(Backing::Write(&mut state.monitors), recorder),
            identities: Table::new(Backing::Write(&mut state.identities), recorder),
            epochs: Table::new(Backing::Write(&mut state.epochs), recorder),
            receipts: Table::new(Backing::Write(&mut state.receipts), recorder),
        }
    }
    pub(crate) fn overlay(
        base: &'a State,
        prior: &'a [Option<RowVersion>],
        sequence: SessionSeq,
        recorder: &'a Recorder,
    ) -> Self {
        Self {
            ledger: base.ledger,
            sequence: Prefix::Assigned(sequence),
            recorder,
            claims: Table::new(
                Backing::Overlay {
                    base: &base.claims,
                    prior,
                    select: |rows| &rows.claims,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            validations: Table::new(
                Backing::Overlay {
                    base: &base.validations,
                    prior,
                    select: |rows| &rows.validations,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            artifacts: Table::new(
                Backing::Overlay {
                    base: &base.artifacts,
                    prior,
                    select: |rows| &rows.artifacts,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            testaments: Table::new(
                Backing::Overlay {
                    base: &base.testaments,
                    prior,
                    select: |rows| &rows.testaments,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            evidence_sets: Table::new(
                Backing::Overlay {
                    base: &base.evidence_sets,
                    prior,
                    select: |rows| &rows.evidence_sets,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            runs: Table::new(
                Backing::Overlay {
                    base: &base.runs,
                    prior,
                    select: |rows| &rows.runs,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            monitors: Table::new(
                Backing::Overlay {
                    base: &base.monitors,
                    prior,
                    select: |rows| &rows.monitors,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            identities: Table::new(
                Backing::Overlay {
                    base: &base.identities,
                    prior,
                    select: |rows| &rows.identities,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            epochs: Table::new(
                Backing::Overlay {
                    base: &base.epochs,
                    prior,
                    select: |rows| &rows.epochs,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            receipts: Table::new(
                Backing::Overlay {
                    base: &base.receipts,
                    prior,
                    select: |rows| &rows.receipts,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
        }
    }
    pub(crate) fn into_writes(self) -> Option<RowWrites> {
        let mut structural = BTreeSet::new();
        let mut added = BTreeMap::new();
        let (claims, changed) = self.claims.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::Claims);
            added.insert(StateTable::Claims, changed);
        }
        let (validations, changed) = self.validations.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::Validations);
            added.insert(StateTable::Validations, changed);
        }
        let (artifacts, changed) = self.artifacts.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::Artifacts);
            added.insert(StateTable::Artifacts, changed);
        }
        let (testaments, changed) = self.testaments.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::Testaments);
            added.insert(StateTable::Testaments, changed);
        }
        let (evidence_sets, changed) = self.evidence_sets.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::EvidenceSets);
            added.insert(StateTable::EvidenceSets, changed);
        }
        let (runs, changed) = self.runs.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::Runs);
            added.insert(StateTable::Runs, changed);
        }
        let (monitors, changed) = self.monitors.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::Monitors);
            added.insert(StateTable::Monitors, changed);
        }
        let (identities, changed) = self.identities.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::Identities);
            added.insert(StateTable::Identities, changed);
        }
        let (epochs, changed) = self.epochs.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::Epochs);
            added.insert(StateTable::Epochs, changed);
        }
        let (receipts, changed) = self.receipts.storage.into_writes()?;
        if changed != 0 {
            structural.insert(StateTable::Receipts);
            added.insert(StateTable::Receipts, changed);
        }
        Some(RowWrites {
            claims,
            validations,
            artifacts,
            testaments,
            evidence_sets,
            runs,
            monitors,
            identities,
            epochs,
            receipts,
            structural,
            added,
        })
    }
    pub(crate) fn ledger(&self) -> LedgerId {
        self.recorder.read(AccessKey::Ledger);
        self.ledger
    }
    pub(crate) fn set_sequence(&mut self, sequence: SessionSeq) {
        self.recorder.write(AccessKey::Sequence);
        match &mut self.sequence {
            Prefix::Borrowed(target) => **target = sequence,
            Prefix::Assigned(target) => *target = sequence,
        }
    }
}

/// Admission can inspect only its immutable dependencies. The reducer receives
/// the mutable adapter after the bounded draft has been allocated.
pub(crate) struct ReadState<'a> {
    ledger: LedgerId,
    sequence: SessionSeq,
    recorder: &'a Recorder,
    pub(crate) claims: Table<'a, ClaimId, Claim, ReadOnly>,
    pub(crate) epochs: Table<'a, ParticipantId, EpochWindow, ReadOnly>,
    pub(crate) receipts: Table<'a, RequestKey, MutationReceipt, ReadOnly>,
}
impl<'a> ReadState<'a> {
    pub(crate) fn new(state: &'a State, recorder: &'a Recorder) -> Self {
        Self {
            ledger: state.ledger,
            sequence: state.sequence,
            recorder,
            claims: Table::new(Backing::Read(&state.claims), recorder),
            epochs: Table::new(Backing::Read(&state.epochs), recorder),
            receipts: Table::new(Backing::Read(&state.receipts), recorder),
        }
    }
    pub(crate) fn overlay(
        state: &'a State,
        prior: &'a [Option<RowVersion>],
        sequence: SessionSeq,
        recorder: &'a Recorder,
    ) -> Self {
        Self {
            ledger: state.ledger,
            sequence,
            recorder,
            claims: Table::new(
                Backing::Overlay {
                    base: &state.claims,
                    prior,
                    select: |rows| &rows.claims,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            epochs: Table::new(
                Backing::Overlay {
                    base: &state.epochs,
                    prior,
                    select: |rows| &rows.epochs,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
            receipts: Table::new(
                Backing::Overlay {
                    base: &state.receipts,
                    prior,
                    select: |rows| &rows.receipts,
                    own: BTreeMap::new(),
                    sequence,
                    added: 0,
                },
                recorder,
            ),
        }
    }
    pub(crate) fn ledger(&self) -> LedgerId {
        self.recorder.read(AccessKey::Ledger);
        self.ledger
    }
    pub(crate) fn sequence(&self) -> SessionSeq {
        self.recorder.read(AccessKey::Sequence);
        self.sequence
    }
}

pub(crate) trait GraphRead {
    fn claims(&self) -> impl Iterator<Item = (&ClaimId, &Claim)>;
    fn claim(&self, id: &ClaimId) -> Option<&Claim>;
    fn monitors(&self) -> impl Iterator<Item = &Monitor>;
    fn scratch(&self, items: usize) -> Result<(), DomainOutcome>;
}
impl GraphRead for State {
    fn scratch(&self, _items: usize) -> Result<(), DomainOutcome> {
        Ok(())
    }
    fn claims(&self) -> impl Iterator<Item = (&ClaimId, &Claim)> {
        self.claims.iter()
    }
    fn claim(&self, id: &ClaimId) -> Option<&Claim> {
        self.claims.get(id)
    }
    fn monitors(&self) -> impl Iterator<Item = &Monitor> {
        self.monitors.values()
    }
}
impl GraphRead for WriteState<'_> {
    fn scratch(&self, items: usize) -> Result<(), DomainOutcome> {
        self.scratch(items)
    }
    fn claims(&self) -> impl Iterator<Item = (&ClaimId, &Claim)> {
        self.claims.iter()
    }
    fn claim(&self, id: &ClaimId) -> Option<&Claim> {
        self.claims.get(id)
    }
    fn monitors(&self) -> impl Iterator<Item = &Monitor> {
        self.monitors.values()
    }
}

#[cfg(test)]
mod recorder_tests {
    use super::*;
    fn ledger() -> LedgerId {
        LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        }
    }
    #[test]
    fn repeated_accesses_consume_one_slot_and_borrow_reentry_collapses() {
        let recorder = Recorder::new(ledger(), SessionSeq(0), 1);
        for _ in 0..1000 {
            recorder.read(AccessKey::Sequence);
        }
        let trace = recorder.finish();
        assert!(!trace.session_exclusive);
        assert_eq!(trace.reads, BTreeSet::from([AccessKey::Sequence]));

        let recorder = Recorder::new(ledger(), SessionSeq(0), 100);
        let held = recorder.footprint.try_borrow_mut().unwrap();
        recorder.read(AccessKey::Sequence);
        drop(held);
        let trace = recorder.finish();
        assert!(trace.session_exclusive);
        assert!(trace.reads.is_empty() && trace.writes.is_empty());
    }
}
