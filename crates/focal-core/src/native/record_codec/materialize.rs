//! Deterministic parallel materialization of committed native records
//! (doc 25 §2). A batch of consecutive committed records at one base prefix
//! is staged by bounded workers against a view of the base plus the records
//! staged before each one; every read a stage makes is recorded, a barrier
//! compares each read against the complete set of earlier writers, and a
//! violation discards that record and its whole speculative suffix, which is
//! then staged serially. Installation and publication are serial and in
//! order. The result is byte-identical to serial replay at every worker
//! count; parallelism only changes when the work happens.
//!
//! Records carry their exact write sets, so a stage never executes a
//! command: it decodes rows, verifies custody and validates the record
//! against the rows it reads. The reads are the only speculation.
use super::replay::{self, BaseRows, StagedRecord};
use super::*;
use focal_evidence::{NativeCustodyReader, NativeSchemaVerifier};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

/// Bounds of one materializer: how many records a batch may hold, how many
/// workers stage them, each worker's stack, the reads one stage may record
/// before it is treated as unverifiable, and the dependency edges a batch
/// may carry before it runs serially.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializerLimits {
    pub max_workers: usize,
    pub max_batch: usize,
    pub worker_stack_bytes: usize,
    pub max_trace: usize,
    pub max_edges: usize,
    /// Plan no dependency edges at all, so every ordering the barrier can
    /// detect is exercised: a footprint-omission campaign, never production.
    pub assume_independent: bool,
}
impl Default for MaterializerLimits {
    fn default() -> Self {
        Self {
            max_workers: 1,
            max_batch: 64,
            worker_stack_bytes: 2 * 1024 * 1024,
            max_trace: 8192,
            max_edges: 65_536,
            assume_independent: false,
        }
    }
}
impl MaterializerLimits {
    pub fn validate(self) -> Result<Self, NativeError> {
        if self.max_workers == 0
            || self.max_workers > 64
            || self.max_batch == 0
            || self.max_batch > 1024
            || self.worker_stack_bytes < 256 * 1024
            || self.worker_stack_bytes > 64 * 1024 * 1024
            || self.max_trace == 0
            || self.max_edges == 0
        {
            return Err(NativeError::Capacity("materializer limits"));
        }
        Ok(self)
    }
}
/// What one batch did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BatchReport {
    pub records: usize,
    /// Records installed and published, a prefix of the batch.
    pub applied: usize,
    pub waves: usize,
    /// Largest wave staged at once; one means the batch ran serially.
    pub max_parallel: usize,
    pub edges: usize,
    /// Reads the barrier found stale; each discarded a speculative suffix.
    pub violations: usize,
    pub serial_fallback: bool,
}
/// The batch's outcomes (a prefix of the records, in order) and the first
/// failure, if any, at its record index.
pub struct BatchOutcome {
    pub outcomes: Vec<NativeOutcome>,
    pub failure: Option<(usize, NativeError)>,
    pub report: BatchReport,
}

/// The object a key belongs to for dependency planning: two records touching
/// the same object are ordered; the barrier orders everything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Affinity {
    Claim([u8; 16]),
    Artifact([u8; 16]),
    Validation([u8; 16]),
    Testament([u8; 16]),
    Monitor([u8; 16]),
}
fn affinity(key: Key) -> Option<Affinity> {
    Some(match key {
        Key::IncomingHead(c)
        | Key::IncomingLink(c, _)
        | Key::MonitorHead(c)
        | Key::MonitorLink(c, _)
        | Key::Claim(c)
        | Key::RetiredCycleHead(c)
        | Key::Retired(c)
        | Key::ClaimResultTestament(c)
        | Key::ClaimContent(c)
        | Key::ByIssuer(_, c)
        | Key::BySubject(_, c)
        | Key::ByStatus(_, c)
        | Key::ByAction(_, c)
        | Key::ByScope(_, _, c)
        | Key::ByRelation(_, _, c) => Affinity::Claim(c.0),
        Key::Cycle(cycle) | Key::RetiredCycle(cycle) | Key::WorkSlot(cycle, _) => {
            Affinity::Claim(cycle.claim.0)
        }
        Key::Evaluation(evaluation) => Affinity::Claim(evaluation.claim.0),
        Key::Accepted(result)
        | Key::DeliveryResult(result)
        | Key::MissingResult(result)
        | Key::ByVerdict(_, result) => Affinity::Claim(result.evaluation.claim.0),
        Key::DueTimer(_, target) => match target {
            TimerTarget::Claim(c) | TimerTarget::Monitor(c, _) => Affinity::Claim(c.0),
            TimerTarget::Evaluation(evaluation) => Affinity::Claim(evaluation.claim.0),
        },
        Key::Monitor(monitor) => Affinity::Monitor(monitor.0),
        Key::Definition(v) | Key::ByEvaluator(_, v) | Key::LegacyDefinition(v) => {
            Affinity::Validation(v.0)
        }
        Key::LegacyRun(v, _) => Affinity::Validation(v.0),
        Key::Artifact(a)
        | Key::Work(a)
        | Key::Diagnostic(a)
        | Key::ByProducer(_, a)
        | Key::ByArtifactKind(_, a)
        | Key::BySchema(_, a)
        | Key::ArtifactInput(_, a) => Affinity::Artifact(a.0),
        Key::Response(t) | Key::ResultTestament(t) | Key::LegacyTestament(t) => {
            Affinity::Testament(t.0)
        }
        // Entry-local rows (one writer per sequence), identity rows (a
        // conflict is the same key, which the key edges order) and the
        // per-record meta row, whose predecessor every stage receives.
        Key::Meta
        | Key::Event(..)
        | Key::Outcome(_)
        | Key::CreationResult(_)
        | Key::ByCreated(..)
        | Key::ClaimIdentity(..)
        | Key::DefinitionIdentity(..)
        | Key::ArtifactIdentity(_)
        | Key::Receipt(_)
        | Key::LegacyEvidenceSet(_)
        | Key::ByObject(..)
        | Key::End => return None,
    })
}
/// The declared footprint of one record: its exact write keys and the
/// objects they belong to, both sorted. `keys` names every written key (the
/// barrier's truth); `shared` omits the meta row every record writes and
/// every stage receives from its predecessor, so it never orders records.
struct Footprint {
    keys: Vec<Key>,
    shared: Vec<Key>,
    cells: Vec<Affinity>,
    writes_meta: bool,
}
fn footprint(record: &StructuralRecord<'_>) -> Result<Footprint, NativeError> {
    let quote = record.quote();
    let mut keys = Vec::new();
    keys.try_reserve_exact(quote.rows)
        .map_err(|_| MemoryError::AllocationFailed)?;
    let mut cells = Vec::new();
    cells
        .try_reserve_exact(quote.rows)
        .map_err(|_| MemoryError::AllocationFailed)?;
    let mut writes_meta = false;
    for row in record.rows(quote.visits).map_err(read_evidence::codec)? {
        let row = row.map_err(read_evidence::codec)?;
        if keys.len() == keys.capacity() {
            return Err(ContractError::InvalidManifest.into());
        }
        keys.push(row.key);
        writes_meta |= row.key == Key::Meta;
        if let Some(cell) = affinity(row.key) {
            cells.push(cell);
        }
    }
    keys.sort_unstable();
    cells.sort_unstable();
    cells.dedup();
    let mut shared = Vec::new();
    shared
        .try_reserve_exact(keys.len())
        .map_err(|_| MemoryError::AllocationFailed)?;
    shared.extend(keys.iter().copied().filter(|key| *key != Key::Meta));
    Ok(Footprint {
        keys,
        shared,
        cells,
        writes_meta,
    })
}
fn intersects<T: Ord>(left: &[T], right: &[T]) -> bool {
    let (mut i, mut j) = (0usize, 0usize);
    while let (Some(a), Some(b)) = (left.get(i), right.get(j)) {
        match a.cmp(b) {
            std::cmp::Ordering::Less => i = i.saturating_add(1),
            std::cmp::Ordering::Greater => j = j.saturating_add(1),
            std::cmp::Ordering::Equal => return true,
        }
    }
    false
}
/// The greatest index below `before` in an ascending list.
fn latest_before(indices: &[usize], before: usize) -> Option<usize> {
    let position = indices.partition_point(|index| *index < before);
    position
        .checked_sub(1)
        .and_then(|p| indices.get(p).copied())
}
/// The version a read observed: the base, or the staged record at an index.
type Observed = Option<usize>;

/// One stage's view of the rows beneath its record: the published base plus
/// every earlier record staged so far, with each read remembered.
struct TaskBase<'a> {
    core: &'a Core<NativeState>,
    staged: &'a [Option<StagedRecord>],
    /// Completed writers per key, ascending, as of the wave start.
    visible: &'a BTreeMap<Key, Vec<usize>>,
    index: usize,
    /// The meta row of the latest earlier record, decoded ahead of the
    /// waves so no stage waits on its predecessor for it.
    meta_before: Option<&'a Row>,
    meta_writer: Observed,
    trace: Option<RefCell<Vec<(Key, Observed)>>>,
    overflow: Cell<bool>,
}
impl TaskBase<'_> {
    fn note(&self, key: Key, observed: Observed) {
        if let Some(trace) = &self.trace {
            let mut trace = trace.borrow_mut();
            if trace.len() == trace.capacity() {
                self.overflow.set(true);
            } else {
                trace.push((key, observed));
            }
        }
    }
}
impl BaseRows for TaskBase<'_> {
    fn base_row(&self, key: Key) -> Option<&Row> {
        if key == Key::Meta && self.meta_writer.is_some() {
            self.note(key, self.meta_writer);
            return self.meta_before;
        }
        let writer = self
            .visible
            .get(&key)
            .and_then(|writers| latest_before(writers, self.index));
        self.note(key, writer);
        match writer {
            Some(writer) => self
                .staged
                .get(writer)
                .and_then(Option::as_ref)
                .and_then(|record| record.row(key))
                .unwrap_or(None),
            None => self.core.base_row(key),
        }
    }
    fn base_prefix(&self) -> u64 {
        self.core.base_prefix().saturating_add(self.index as u64)
    }
}

struct Batch<'a, 'bytes> {
    records: &'a [(StructuralRecord<'bytes>, RangeId)],
    footprints: Vec<Footprint>,
    /// Predecessors per record.
    deps: Vec<Vec<usize>>,
    /// Every writer per key, ascending: the truth the barrier checks against.
    writers: BTreeMap<Key, Vec<usize>>,
    metas: Vec<Option<Row>>,
    staged: Vec<Option<StagedRecord>>,
    visible: BTreeMap<Key, Vec<usize>>,
    report: BatchReport,
}
type Finished = (
    usize,
    Result<StagedRecord, NativeError>,
    Vec<(Key, Observed)>,
    bool,
);

/// Stage, install and publish `records` in order. Each record must extend
/// the one before it (the first extends the Core's prefix) and carry the
/// producer range the log established for it. Outcomes are returned for the
/// applied prefix; the first refusal is returned at its index and nothing
/// after it is applied, so a retryable refusal resumes there.
pub fn materialize_batch<S: NativeSchemaVerifier + Sync, R: NativeCustodyReader + Sync>(
    core: &mut Core<NativeState>,
    records: &[(StructuralRecord<'_>, RangeId)],
    limits: recovery::Limits,
    store: &R,
    schemas: &S,
    materializer: MaterializerLimits,
) -> BatchOutcome {
    let mut report = BatchReport {
        records: records.len(),
        ..BatchReport::default()
    };
    let mut outcomes = Vec::new();
    let failure = match stage_all(
        core,
        records,
        limits,
        store,
        schemas,
        materializer,
        &mut report,
    ) {
        Ok((mut staged, failure)) => {
            let mut failure = failure;
            if outcomes.try_reserve_exact(staged.len()).is_err() {
                failure = Some((0, MemoryError::AllocationFailed.into()));
            } else {
                for (index, slot) in staged.iter_mut().enumerate() {
                    if failure.as_ref().is_some_and(|(at, _)| *at <= index) {
                        break;
                    }
                    let Some(record) = slot.take() else { break };
                    let prepared = match replay::install(core, record) {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            failure = Some((index, error));
                            break;
                        }
                    };
                    match core.publish_native(prepared) {
                        Ok(outcome) => outcomes.push(outcome),
                        Err(refused) => {
                            failure = Some((index, refused.error.into()));
                            break;
                        }
                    }
                }
            }
            failure
        }
        Err(error) => Some((0, error)),
    };
    report.applied = outcomes.len();
    BatchOutcome {
        outcomes,
        failure,
        report,
    }
}

/// Stage every record; returns the staged records (a complete prefix up to
/// the first failure) and the first failure.
#[allow(
    clippy::type_complexity,
    reason = "one bounded return of the staged prefix and the first refusal"
)]
fn stage_all<S: NativeSchemaVerifier + Sync, R: NativeCustodyReader + Sync>(
    core: &Core<NativeState>,
    records: &[(StructuralRecord<'_>, RangeId)],
    limits: recovery::Limits,
    store: &R,
    schemas: &S,
    materializer: MaterializerLimits,
    report: &mut BatchReport,
) -> Result<(Vec<Option<StagedRecord>>, Option<(usize, NativeError)>), NativeError> {
    let materializer = materializer.validate()?;
    if records.is_empty() || records.len() > materializer.max_batch {
        return Err(NativeError::Capacity("materializer batch"));
    }
    let count = records.len();
    let mut footprints = Vec::new();
    footprints
        .try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    for (record, _) in records {
        footprints.push(footprint(record)?);
    }
    // Dependencies: same object or same key. Edge storage is bounded; a batch
    // beyond the bound runs serially, which needs no edges at all.
    let mut deps = Vec::new();
    deps.try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    let mut edges = 0usize;
    let mut serial = materializer.max_workers == 1 || count == 1;
    for i in 0..count {
        let mut before = Vec::new();
        if !serial && !materializer.assume_independent {
            let mine = footprints.get(i).ok_or(ContractError::InvalidManifest)?;
            for j in 0..i {
                let theirs = footprints.get(j).ok_or(ContractError::InvalidManifest)?;
                if intersects(&mine.cells, &theirs.cells)
                    || intersects(&mine.shared, &theirs.shared)
                {
                    if edges == materializer.max_edges {
                        serial = true;
                        break;
                    }
                    if before.try_reserve(1).is_err() {
                        return Err(MemoryError::AllocationFailed.into());
                    }
                    before.push(j);
                    edges = edges.saturating_add(1);
                }
            }
        }
        deps.push(before);
    }
    report.edges = edges;
    let mut writers: BTreeMap<Key, Vec<usize>> = BTreeMap::new();
    for (index, footprint) in footprints.iter().enumerate() {
        for key in &footprint.keys {
            let list = writers.entry(*key).or_default();
            if list.try_reserve(1).is_err() {
                return Err(MemoryError::AllocationFailed.into());
            }
            list.push(index);
        }
    }
    // The meta rows ahead of the waves: every stage reads its predecessor's.
    let mut metas = Vec::new();
    metas
        .try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    for ((record, _), footprint) in records.iter().zip(&footprints) {
        metas.push(if footprint.writes_meta {
            Some(replay::stage_meta(
                record,
                core.state.ledger,
                limits.work.parsing,
            )?)
        } else {
            None
        });
    }
    let mut staged = Vec::new();
    staged
        .try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    staged.resize_with(count, || None);
    let mut batch = Batch {
        records,
        footprints,
        deps,
        writers,
        metas,
        staged,
        visible: BTreeMap::new(),
        report: *report,
    };
    let workers = if serial { 1 } else { materializer.max_workers };
    let result = batch.run(core, limits, store, schemas, workers, materializer);
    *report = batch.report;
    result.map(|failure| (batch.staged, failure))
}

impl Batch<'_, '_> {
    fn complete(&self, index: usize) -> bool {
        self.staged.get(index).is_some_and(Option::is_some)
    }
    fn ready(&self, running: &[usize], workers: usize) -> Result<Vec<usize>, NativeError> {
        let mut ready = Vec::new();
        ready
            .try_reserve_exact(workers)
            .map_err(|_| MemoryError::AllocationFailed)?;
        for index in 0..self.records.len() {
            if ready.len() == workers {
                break;
            }
            if self.complete(index) || running.contains(&index) {
                continue;
            }
            let deps = self.deps.get(index).ok_or(ContractError::InvalidManifest)?;
            if deps.iter().all(|dep| self.complete(*dep)) {
                ready.push(index);
            }
        }
        Ok(ready)
    }
    /// The meta row a stage at `index` reads as its predecessor's.
    fn meta_before(&self, index: usize) -> (Option<&Row>, Observed) {
        let writer = self
            .writers
            .get(&Key::Meta)
            .and_then(|writers| latest_before(writers, index));
        match writer {
            Some(writer) => (
                self.metas.get(writer).and_then(Option::as_ref),
                Some(writer),
            ),
            None => (None, None),
        }
    }
    fn task<'a>(
        &'a self,
        core: &'a Core<NativeState>,
        index: usize,
        traced: bool,
        max_trace: usize,
    ) -> Result<TaskBase<'a>, NativeError> {
        let (meta_before, meta_writer) = self.meta_before(index);
        let trace = if traced {
            let mut trace = Vec::new();
            trace
                .try_reserve_exact(max_trace)
                .map_err(|_| MemoryError::AllocationFailed)?;
            Some(RefCell::new(trace))
        } else {
            None
        };
        Ok(TaskBase {
            core,
            staged: &self.staged,
            visible: &self.visible,
            index,
            meta_before,
            meta_writer,
            trace,
            overflow: Cell::new(false),
        })
    }
    /// A read was stale when the version it observed is not the latest
    /// writer before the reader.
    fn violated(&self, index: usize, trace: &[(Key, Observed)], overflow: bool) -> bool {
        if overflow {
            return true;
        }
        trace.iter().any(|(key, observed)| {
            let expected = self
                .writers
                .get(key)
                .and_then(|writers| latest_before(writers, index));
            *observed != expected
        })
    }
    fn admit(&mut self, index: usize, record: StagedRecord) -> Result<(), NativeError> {
        for key in &self
            .footprints
            .get(index)
            .ok_or(ContractError::InvalidManifest)?
            .keys
        {
            let list = self.visible.entry(*key).or_default();
            let at = list.partition_point(|i| *i < index);
            if list.try_reserve(1).is_err() {
                return Err(MemoryError::AllocationFailed.into());
            }
            list.insert(at, index);
        }
        *self
            .staged
            .get_mut(index)
            .ok_or(ContractError::InvalidManifest)? = Some(record);
        Ok(())
    }
    /// Discard every staged record from `from` on; their reads may have
    /// depended on a stale version.
    fn discard_from(&mut self, from: usize) {
        for slot in self.staged.iter_mut().skip(from) {
            *slot = None;
        }
        for list in self.visible.values_mut() {
            list.retain(|index| *index < from);
        }
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed inputs; nothing is retained beyond the batch"
    )]
    fn run<S: NativeSchemaVerifier + Sync, R: NativeCustodyReader + Sync>(
        &mut self,
        core: &Core<NativeState>,
        limits: recovery::Limits,
        store: &R,
        schemas: &S,
        workers: usize,
        materializer: MaterializerLimits,
    ) -> Result<Option<(usize, NativeError)>, NativeError> {
        let count = self.records.len();
        let mut completed = 0usize;
        let mut failure: Option<(usize, NativeError)> = None;
        while completed < count {
            let ready = self.ready(&[], workers)?;
            if ready.is_empty() {
                return Err(ContractError::InvalidManifest.into());
            }
            self.report.waves = self.report.waves.saturating_add(1);
            self.report.max_parallel = self.report.max_parallel.max(ready.len());
            let traced = ready.len() > 1;
            let finished: Vec<Finished> = if let [index] = ready.as_slice() {
                let index = *index;
                let base = self.task(core, index, false, materializer.max_trace)?;
                let (record, range) = self
                    .records
                    .get(index)
                    .ok_or(ContractError::InvalidManifest)?;
                let result = replay::stage(
                    &base,
                    &core.state.budget,
                    core.limits,
                    core.state.ledger,
                    core.state.profile,
                    record,
                    *range,
                    limits,
                    store,
                    schemas,
                );
                vec![(index, result, Vec::new(), false)]
            } else {
                let mut bases = Vec::new();
                bases
                    .try_reserve_exact(ready.len())
                    .map_err(|_| MemoryError::AllocationFailed)?;
                for index in &ready {
                    bases.push((
                        *index,
                        self.task(core, *index, traced, materializer.max_trace)?,
                    ));
                }
                let records = self.records;
                std::thread::scope(|scope| -> Result<Vec<Finished>, NativeError> {
                    let mut handles = Vec::new();
                    handles
                        .try_reserve_exact(bases.len())
                        .map_err(|_| MemoryError::AllocationFailed)?;
                    // Each stage owns its view; the shared inputs are read only.
                    for (index, base) in bases {
                        let handle = std::thread::Builder::new()
                            .name(format!("focal-materialize-{index}"))
                            .stack_size(materializer.worker_stack_bytes)
                            .spawn_scoped(scope, move || {
                                let (record, range) = match records.get(index) {
                                    Some(entry) => entry,
                                    None => {
                                        return (
                                            index,
                                            Err(ContractError::InvalidManifest.into()),
                                            Vec::new(),
                                            true,
                                        );
                                    }
                                };
                                let result = replay::stage(
                                    &base,
                                    &core.state.budget,
                                    core.limits,
                                    core.state.ledger,
                                    core.state.profile,
                                    record,
                                    *range,
                                    limits,
                                    store,
                                    schemas,
                                );
                                let trace = base
                                    .trace
                                    .as_ref()
                                    .map(|trace| trace.take())
                                    .unwrap_or_default();
                                (index, result, trace, base.overflow.get())
                            })
                            .map_err(|_| NativeError::Capacity("materializer worker"))?;
                        handles.push(handle);
                    }
                    let mut finished = Vec::new();
                    finished
                        .try_reserve_exact(handles.len())
                        .map_err(|_| MemoryError::AllocationFailed)?;
                    let mut unwound = false;
                    for handle in handles {
                        match handle.join() {
                            Ok(done) => finished.push(done),
                            Err(_) => unwound = true,
                        }
                    }
                    if unwound {
                        return Err(NativeError::Capacity("materializer worker unwound"));
                    }
                    Ok(finished)
                })?
            };
            let mut violation: Option<usize> = None;
            let mut done = Vec::new();
            done.try_reserve_exact(finished.len())
                .map_err(|_| MemoryError::AllocationFailed)?;
            for (index, result, trace, overflow) in finished {
                match result {
                    Ok(record) => {
                        if traced && self.violated(index, &trace, overflow) {
                            violation = Some(violation.map_or(index, |v: usize| v.min(index)));
                            self.report.violations = self.report.violations.saturating_add(1);
                        }
                        done.push((index, record));
                    }
                    Err(error) => {
                        // A stage that read a stale version may have refused
                        // for that reason alone: it is speculation to discard
                        // and redo in order, never a verdict on the record.
                        if traced && self.violated(index, &trace, overflow) {
                            violation = Some(violation.map_or(index, |v: usize| v.min(index)));
                            self.report.violations = self.report.violations.saturating_add(1);
                        } else if failure.as_ref().is_none_or(|(at, _)| index < *at) {
                            failure = Some((index, error));
                        }
                    }
                }
            }
            for (index, record) in done {
                if violation.is_some_and(|from| index >= from) {
                    continue;
                }
                self.admit(index, record)?;
                completed = completed.saturating_add(1);
            }
            if let Some(from) = violation {
                // Everything at or after the stale read is speculation on it.
                self.discard_from(from);
                completed = self.staged.iter().filter(|slot| slot.is_some()).count();
                self.report.serial_fallback = true;
                return self.finish_serially(core, limits, store, schemas, failure, completed);
            }
            if failure.is_some() {
                // Stage whatever precedes the failure so the applied prefix is
                // exact, then stop there.
                return self.finish_serially(core, limits, store, schemas, failure, completed);
            }
        }
        Ok(failure)
    }
    /// Stage the remaining records one at a time in order, each over every
    /// record before it; stop at the first failure (or the known one).
    fn finish_serially<S: NativeSchemaVerifier + Sync, R: NativeCustodyReader + Sync>(
        &mut self,
        core: &Core<NativeState>,
        limits: recovery::Limits,
        store: &R,
        schemas: &S,
        mut failure: Option<(usize, NativeError)>,
        mut completed: usize,
    ) -> Result<Option<(usize, NativeError)>, NativeError> {
        let count = self.records.len();
        while completed < count {
            let Some(index) = (0..count).find(|index| !self.complete(*index)) else {
                break;
            };
            if failure.as_ref().is_some_and(|(at, _)| index >= *at) {
                break;
            }
            let base = self.task(core, index, false, 1)?;
            let (record, range) = self
                .records
                .get(index)
                .ok_or(ContractError::InvalidManifest)?;
            let result = replay::stage(
                &base,
                &core.state.budget,
                core.limits,
                core.state.ledger,
                core.state.profile,
                record,
                *range,
                limits,
                store,
                schemas,
            );
            match result {
                Ok(staged) => {
                    self.admit(index, staged)?;
                    completed = completed.saturating_add(1);
                }
                Err(error) => {
                    failure = Some((index, error));
                    break;
                }
            }
        }
        // A failure leaves nothing staged at or after it.
        if let Some((at, _)) = &failure {
            self.discard_from(*at);
        }
        Ok(failure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_of_one_object_share_an_affinity_and_entry_local_keys_have_none() {
        let claim = ClaimId::from_u128(7);
        let cycle = NativeCycleKey {
            claim,
            receipt: ReceiptId::from_u128(9),
            epoch: 1,
            cycle: 1,
        };
        assert_eq!(affinity(Key::Claim(claim)), Some(Affinity::Claim(claim.0)));
        assert_eq!(affinity(Key::Cycle(cycle)), Some(Affinity::Claim(claim.0)));
        assert_eq!(
            affinity(Key::ByStatus(3, claim)),
            Some(Affinity::Claim(claim.0))
        );
        assert_eq!(
            affinity(Key::DueTimer(5, TimerTarget::Claim(claim))),
            Some(Affinity::Claim(claim.0))
        );
        let artifact = ArtifactId::from_u128(11);
        assert_eq!(
            affinity(Key::Work(artifact)),
            Some(Affinity::Artifact(artifact.0))
        );
        assert_eq!(affinity(Key::Meta), None);
        assert_eq!(affinity(Key::Event(SessionSeq(4), 0)), None);
        assert_eq!(affinity(Key::ClaimIdentity(1, ContentHash([1; 32]))), None);
    }

    #[test]
    fn sorted_intersection_and_latest_writer_lookups_are_exact() {
        assert!(intersects(&[1, 3, 5], &[5, 7]));
        assert!(!intersects(&[1, 3, 5], &[2, 4, 6]));
        assert!(!intersects::<u8>(&[], &[1]));
        assert_eq!(latest_before(&[0, 2, 5], 5), Some(2));
        assert_eq!(latest_before(&[0, 2, 5], 6), Some(5));
        assert_eq!(latest_before(&[0, 2, 5], 0), None);
        assert_eq!(latest_before(&[], 3), None);
    }

    #[test]
    fn materializer_limits_are_bounded() {
        assert!(MaterializerLimits::default().validate().is_ok());
        for bad in [
            MaterializerLimits {
                max_workers: 0,
                ..MaterializerLimits::default()
            },
            MaterializerLimits {
                max_workers: 65,
                ..MaterializerLimits::default()
            },
            MaterializerLimits {
                max_batch: 0,
                ..MaterializerLimits::default()
            },
            MaterializerLimits {
                worker_stack_bytes: 1024,
                ..MaterializerLimits::default()
            },
            MaterializerLimits {
                max_trace: 0,
                ..MaterializerLimits::default()
            },
        ] {
            assert!(bad.validate().is_err());
        }
    }
}
