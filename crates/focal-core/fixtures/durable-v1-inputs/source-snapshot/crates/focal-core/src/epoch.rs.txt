//! Ordered epochs with immutable prior-row versions and audited owned outputs.
use crate::access::{ReadObservation, Recorder, WriteState};
use crate::overlay::RowVersion;
use crate::*;
use std::io::{self, Write};

/// Workspace bounds include retained inputs, two sets of owned output versions,
/// access records, dependency edges, queues and explicitly sized worker stacks.
/// Row copies, outputs and reducer worklists consume the per-entry byte allowance;
/// structural traversal counts also remain governed by `Core::limits`.
#[derive(Debug, Clone, Copy)]
pub struct EpochLimits {
    pub max_commands: usize,
    pub max_workers: usize,
    pub max_edges: usize,
    pub max_bytes: usize,
    pub max_trace_entries: usize,
    pub worker_stack_bytes: usize,
}
impl Default for EpochLimits {
    fn default() -> Self {
        Self {
            max_commands: 64,
            max_workers: 4,
            max_edges: 4096,
            max_bytes: 256 * 1024 * 1024,
            max_trace_entries: 4096,
            worker_stack_bytes: 2 * 1024 * 1024,
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum EpochError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error("epoch workspace capacity exhausted: {0}")]
    Capacity(&'static str),
    #[error("epoch base state or ordered input provenance changed")]
    Provenance,
    #[error("epoch worker dependency failed: {0}")]
    Worker(&'static str),
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EpochReport {
    pub commands: usize,
    pub dependency_edges: usize,
    /// Largest executed wave; a singleton runs inline on the owner.
    pub max_parallel: usize,
    pub waves: usize,
    /// All speculative output was discarded and rerun in log order.
    pub serial_fallback: bool,
}
#[derive(Debug)]
struct PlannedEntry {
    input: PreparedMutation,
    expected: [u8; 32],
    declaration: AccessFootprint,
    complete: bool,
}
/// A plan is tied to the exact base state, limits, ordered inputs and assigned
/// session prefixes. It is local execution metadata, never a new persisted format.
#[derive(Debug)]
pub struct EpochPlan {
    base: SessionSeq,
    base_hash: [u8; 32],
    entries: Vec<PlannedEntry>,
    limits: EpochLimits,
    entry_bytes: usize,
    #[cfg(test)]
    fault: Option<TestFault>,
}
#[derive(Debug)]
struct Executed {
    version: RowVersion,
    result: ApplyResult,
    accesses: AccessFootprint,
    digest: [u8; 32],
}
/// Only an audited executor can construct this value. Publication consumes it.
#[derive(Debug)]
pub struct EpochOutput {
    base: SessionSeq,
    base_hash: [u8; 32],
    versions: Vec<Option<RowVersion>>,
    results: Vec<ApplyResult>,
    seal: [u8; 32],
    report: EpochReport,
}
impl EpochOutput {
    pub(crate) fn matches_pending_rows(&self, rows: &[Option<RowVersion>]) -> bool {
        rows.len() >= self.versions.len()
            && rows
                .iter()
                .zip(&self.versions)
                .all(|(left, right)| left == right)
    }
    pub fn report(&self) -> EpochReport {
        self.report
    }
    pub fn results(&self) -> &[ApplyResult] {
        &self.results
    }
    pub fn patch(&self, index: usize) -> Option<RowPatch<'_>> {
        let version = self.versions.get(index)?.as_ref()?;
        Some(RowPatch {
            ledger: self.results.get(index)?.receipt.ledger,
            base: SessionSeq(version.sequence.0.checked_sub(1)?),
            version,
        })
    }
    /// Borrow an immutable projection prefix. Publication still requires the
    /// complete base/seal audit through Core::validate_epoch.
    pub fn view_before<'a>(
        &'a self,
        core: &'a Core,
        index: usize,
    ) -> Result<CoreView<'a>, EpochError> {
        let prior = self.versions.get(..index).ok_or(EpochError::Provenance)?;
        if core.sequence() != self.base {
            return Err(EpochError::Provenance);
        }
        let sequence = prior
            .last()
            .and_then(Option::as_ref)
            .map_or(self.base, |version| version.sequence);
        Ok(CoreView {
            state: &core.state,
            prior,
            tail: None,
            sequence,
        })
    }
}

impl Core {
    /// Verify single-command apply workspace before consensus admission. The
    /// caller owns the surrounding host reservation; no pending row is mutated.
    pub fn audit_pending_stage(
        &self,
        pending: &PendingState,
        staged: &StagedMutation,
        limits: EpochLimits,
    ) -> Result<(), EpochError> {
        pending.validate_next(self, staged)?;
        let bytes = workspace(std::slice::from_ref(staged.prepared()), limits)?;
        let output = execute_entry(
            self,
            &pending.rows,
            staged.prepared(),
            limits.max_trace_entries,
            bytes,
        )?;
        if output.result != *staged.result() || output.version != staged.version {
            return Err(EpochError::Provenance);
        }
        Ok(())
    }
    /// Bind committed prepared commands to an exact immutable base and compute
    /// actual serial accesses/outputs without cloning that base per command.
    /// Admission must already have run against the sequencer's pending prefix.
    pub fn plan_epoch(
        &self,
        inputs: Vec<PreparedMutation>,
        limits: EpochLimits,
    ) -> Result<EpochPlan, EpochError> {
        let entry_bytes = workspace(&inputs, limits)?;
        let base_hash = hash(self)?;
        let mut versions = slots(inputs.len())?;
        let mut entries = vector(inputs.len())?;
        for (index, input) in inputs.into_iter().enumerate() {
            validate_input(self, index, &input)?;
            let prior = versions.get(..index).ok_or(EpochError::Provenance)?;
            let output = execute_entry(self, prior, &input, limits.max_trace_entries, entry_bytes)?;
            let slot = versions.get_mut(index).ok_or(EpochError::Provenance)?;
            *slot = Some(output.version);
            entries.push(PlannedEntry {
                input,
                expected: output.digest,
                complete: !output.accesses.session_exclusive,
                declaration: output.accesses,
            });
        }
        Ok(EpochPlan {
            base: self.sequence(),
            base_hash,
            entries,
            limits,
            entry_bytes,
            #[cfg(test)]
            fault: None,
        })
    }
    /// Validate all provenance and output bytes before changing any row. The
    /// exclusive owner publishes the complete prefix; no partial epoch escapes.
    pub fn validate_epoch(&self, output: &EpochOutput) -> Result<SessionSeq, EpochError> {
        if self.sequence() != output.base
            || hash(self)? != output.base_hash
            || hash(&(&output.versions, &output.results))? != output.seal
            || output.versions.len() != output.results.len()
        {
            return Err(EpochError::Provenance);
        }
        let mut final_sequence = output.base;
        for (version, result) in output.versions.iter().zip(&output.results) {
            let version = version.as_ref().ok_or(EpochError::Provenance)?;
            final_sequence = next(final_sequence)?;
            if version.sequence != final_sequence
                || result.receipt.sequence != final_sequence
                || result.receipt.ledger != self.state.ledger
                || version.rows.receipts.get(&result.receipt.key) != Some(&result.receipt)
            {
                return Err(EpochError::Provenance);
            }
        }
        Ok(final_sequence)
    }
    pub fn publish_epoch(&mut self, output: EpochOutput) -> Result<Vec<ApplyResult>, EpochError> {
        let final_sequence = self.validate_epoch(&output)?;
        for version in output.versions.into_iter().flatten() {
            version.rows.publish(&mut self.state);
        }
        self.state.sequence = final_sequence;
        Ok(output.results)
    }
}
impl EpochPlan {
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn accesses(&self, index: usize) -> Option<&AccessFootprint> {
        self.entries.get(index).map(|entry| &entry.declaration)
    }
    /// Supply a scheduler declaration. Incomplete declarations can only cause
    /// whole-epoch serial fallback; actual reads/versions/results are audited.
    pub fn declare(
        &mut self,
        index: usize,
        declaration: AccessFootprint,
    ) -> Result<(), EpochError> {
        // Do not let a caller bypass the trace/workspace bound with an oversized hint.
        if declaration
            .reads
            .len()
            .checked_add(declaration.writes.len())
            .is_none_or(|size| size > self.limits.max_trace_entries)
        {
            return Err(EpochError::Capacity("declaration"));
        }
        self.entries
            .get_mut(index)
            .ok_or(EpochError::Provenance)?
            .declaration = declaration;
        Ok(())
    }
    /// Execute bounded deterministic ready waves. Earlier indices alone are
    /// visible, even when a later independent command has already completed.
    pub fn execute(self, core: &Core) -> Result<EpochOutput, EpochError> {
        self.execute_order(core, false)
    }
    fn execute_order(self, core: &Core, reverse: bool) -> Result<EpochOutput, EpochError> {
        if core.sequence() != self.base || hash(core)? != self.base_hash {
            return Err(EpochError::Provenance);
        }
        let force_serial = self
            .entries
            .iter()
            .any(|entry| !entry.complete || entry.declaration.session_exclusive);
        let (dependencies, edge_count) = if force_serial {
            (Vec::new(), 0)
        } else {
            dependencies(&self.entries, self.limits.max_edges)?
        };
        let mut versions = slots(self.entries.len())?;
        let mut results = slots(self.entries.len())?;
        let mut report = EpochReport {
            commands: self.entries.len(),
            dependency_edges: edge_count,
            ..EpochReport::default()
        };
        let mut completed = 0usize;
        let mut fallback = force_serial;
        while completed < self.entries.len() && !fallback {
            let mut ready = vector(self.limits.max_workers.min(self.entries.len()))?;
            for position in 0..self.entries.len() {
                let index = if reverse {
                    self.entries
                        .len()
                        .checked_sub(position)
                        .and_then(|n| n.checked_sub(1))
                        .ok_or(EpochError::Provenance)?
                } else {
                    position
                };
                let empty = versions.get(index).ok_or(EpochError::Provenance)?.is_none();
                let deps = dependencies.get(index).ok_or(EpochError::Provenance)?;
                if empty
                    && deps
                        .iter()
                        .all(|dependency| versions.get(*dependency).is_some_and(Option::is_some))
                {
                    ready.push(index);
                    if ready.len() == self.limits.max_workers {
                        break;
                    }
                }
            }
            if ready.is_empty() {
                return Err(EpochError::Worker("dependency cycle or missing output"));
            }
            report.max_parallel = report.max_parallel.max(ready.len());
            report.waves = report
                .waves
                .checked_add(1)
                .ok_or(EpochError::Capacity("wave counter"))?;
            let wave = if let [index] = ready.as_slice() {
                let index = *index;
                let entry = self.entries.get(index).ok_or(EpochError::Provenance)?;
                let prior = versions.get(..index).ok_or(EpochError::Provenance)?;
                #[cfg(test)]
                if self.fault == Some(TestFault::Spawn(index)) {
                    return Err(EpochError::Worker("injected thread creation failure"));
                }
                let output = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    #[cfg(test)]
                    if self.fault == Some(TestFault::Panic(index)) {
                        panic!("injected worker unwind");
                    }
                    execute_entry(
                        core,
                        prior,
                        &entry.input,
                        self.limits.max_trace_entries,
                        self.entry_bytes,
                    )
                }))
                .map_err(|_| EpochError::Worker("worker unwound"))?;
                let mut finished = vector(1)?;
                finished.push((index, output));
                finished
            } else {
                std::thread::scope(|scope| {
                    let mut handles = vector(ready.len())?;
                    let mut finished = vector(ready.len())?;
                    let mut failure = None;
                    for index in ready {
                        let (Some(entry), Some(prior)) =
                            (self.entries.get(index), versions.get(..index))
                        else {
                            failure = Some(EpochError::Provenance);
                            break;
                        };
                        #[cfg(test)]
                        if self.fault == Some(TestFault::Spawn(index)) {
                            failure = Some(EpochError::Worker("injected thread creation failure"));
                            break;
                        }
                        #[cfg(test)]
                        let fault = self.fault;
                        match std::thread::Builder::new()
                            .stack_size(self.limits.worker_stack_bytes)
                            .spawn_scoped(scope, move || {
                                #[cfg(test)]
                                if fault == Some(TestFault::Panic(index)) {
                                    panic!("injected worker unwind");
                                }
                                execute_entry(
                                    core,
                                    prior,
                                    &entry.input,
                                    self.limits.max_trace_entries,
                                    self.entry_bytes,
                                )
                            }) {
                            Ok(handle) => handles.push((index, handle)),
                            Err(_) => {
                                failure = Some(EpochError::Worker("thread creation"));
                                break;
                            }
                        }
                    }
                    // Always join every created worker, including after a failure.
                    for (index, handle) in handles {
                        match handle.join() {
                            Ok(result) => finished.push((index, result)),
                            Err(_) => failure = Some(EpochError::Worker("worker unwound")),
                        }
                    }
                    if let Some(error) = failure {
                        Err(error)
                    } else {
                        Ok(finished)
                    }
                })?
            };
            for (index, result) in wave {
                match result {
                    Ok(output) => {
                        let planned = self.entries.get(index).ok_or(EpochError::Provenance)?;
                        if !planned.declaration.covers(&output.accesses)
                            || planned.expected != output.digest
                        {
                            fallback = true;
                        }
                        *versions.get_mut(index).ok_or(EpochError::Provenance)? =
                            Some(output.version);
                        *results.get_mut(index).ok_or(EpochError::Provenance)? =
                            Some(output.result);
                        completed = completed
                            .checked_add(1)
                            .ok_or(EpochError::Capacity("completion counter"))?;
                    }
                    Err(EpochError::Core(CoreError::Determinism(_))) => fallback = true,
                    Err(error) => return Err(error),
                }
            }
        }
        if fallback {
            // Drop all speculative versions/results before reusing their budget.
            versions.iter_mut().for_each(|slot| *slot = None);
            results.iter_mut().for_each(|slot| *slot = None);
            for (index, entry) in self.entries.iter().enumerate() {
                let prior = versions.get(..index).ok_or(EpochError::Provenance)?;
                let output = execute_entry(
                    core,
                    prior,
                    &entry.input,
                    self.limits.max_trace_entries,
                    self.entry_bytes,
                )?;
                if output.digest != entry.expected {
                    return Err(EpochError::Provenance);
                }
                *versions.get_mut(index).ok_or(EpochError::Provenance)? = Some(output.version);
                *results.get_mut(index).ok_or(EpochError::Provenance)? = Some(output.result);
            }
            report.serial_fallback = true;
        }
        let mut ordered = vector(results.len())?;
        for result in results {
            ordered.push(result.ok_or(EpochError::Provenance)?);
        }
        let seal = hash(&(&versions, &ordered))?;
        Ok(EpochOutput {
            base: self.base,
            base_hash: self.base_hash,
            versions,
            results: ordered,
            seal,
            report,
        })
    }
}

fn execute_entry(
    core: &Core,
    prior: &[Option<RowVersion>],
    input: &PreparedMutation,
    max_trace_entries: usize,
    max_bytes: usize,
) -> Result<Executed, EpochError> {
    let sequence = next(input.base)?;
    let recorder = Recorder::epoch(
        core.state.ledger,
        core.sequence(),
        max_trace_entries,
        max_bytes,
    );
    // Sequence is an immutable assigned input, audited separately from row reads.
    recorder.read(AccessKey::Sequence);
    recorder.read(AccessKey::Limits);
    let state = WriteState::overlay(&core.state, prior, sequence, &recorder);
    let execution = reduce::execute_on(state, &core.limits, &input.input, sequence);
    if recorder.failed() {
        return Err(EpochError::Capacity("owned rows or output"));
    }
    let (mut draft, outcome, deltas, effects) = execution.map_err(CoreError::Determinism)?;
    let key = RequestKey {
        principal: input.input.principal,
        epoch: input.input.request_epoch,
        id: input.input.request_id,
    };
    let receipt = MutationReceipt {
        ledger: draft.ledger(),
        key,
        sequence,
        command_hash: input.command_hash,
        outcome,
    };
    if !recorder.reserve_value(&receipt) {
        return Err(EpochError::Capacity("receipt output"));
    }
    draft.receipts.insert(key, receipt.clone());
    draft.set_sequence(sequence);
    let rows = draft.into_writes().ok_or(EpochError::Provenance)?;
    if recorder.failed() {
        return Err(EpochError::Capacity("receipt row"));
    }
    let version = RowVersion { sequence, rows };
    let result = ApplyResult {
        receipt,
        deltas,
        effects,
    };
    let (accesses, observations, _) = recorder.finish_epoch();
    let digest = hash(&(&version, &result, &accesses, &observations))?;
    Ok(Executed {
        version,
        result,
        accesses,
        digest,
    })
}
fn next(sequence: SessionSeq) -> Result<SessionSeq, EpochError> {
    Ok(SessionSeq(
        sequence.0.checked_add(1).ok_or(CoreError::Exhausted)?,
    ))
}
fn validate_input(core: &Core, index: usize, input: &PreparedMutation) -> Result<(), EpochError> {
    let offset = u64::try_from(index).map_err(|_| EpochError::Capacity("command index"))?;
    let expected = SessionSeq(
        core.sequence()
            .0
            .checked_add(offset)
            .ok_or(CoreError::Exhausted)?,
    );
    if input.schema != SCHEMA_MAJOR {
        return Err(CoreError::UnsupportedSchema(input.schema).into());
    }
    if input.base != expected
        || input.input.ledger != core.state.ledger
        || input.footprint.ledger != core.state.ledger
        || command_hash(&input.input).map_err(CoreError::from)? != input.command_hash
    {
        return Err(EpochError::Provenance);
    }
    next(expected)?;
    Ok(())
}
fn dependencies(
    entries: &[PlannedEntry],
    maximum: usize,
) -> Result<(Vec<Vec<usize>>, usize), EpochError> {
    let mut dependencies = vector(entries.len())?;
    let mut edges = 0usize;
    for (index, entry) in entries.iter().enumerate() {
        let mut predecessors = Vec::new();
        for (previous, earlier) in entries.iter().take(index).enumerate() {
            if conflict(&earlier.declaration, &entry.declaration) {
                edges = edges
                    .checked_add(1)
                    .filter(|count| *count <= maximum)
                    .ok_or(EpochError::Capacity("dependency edges"))?;
                predecessors
                    .try_reserve(1)
                    .map_err(|_| EpochError::Capacity("dependency allocation"))?;
                predecessors.push(previous);
            }
        }
        dependencies.push(predecessors);
    }
    Ok((dependencies, edges))
}
fn conflict(left: &AccessFootprint, right: &AccessFootprint) -> bool {
    if left.session_exclusive || right.session_exclusive {
        return true;
    }
    left.writes
        .iter()
        .any(|key| overlaps(&right.reads, *key, false))
        || left
            .reads
            .iter()
            .any(|key| overlaps(&right.writes, *key, false))
        || left
            .writes
            .iter()
            .any(|key| overlaps(&right.writes, *key, true))
}
fn overlaps(set: &BTreeSet<AccessKey>, key: AccessKey, writes: bool) -> bool {
    if key == AccessKey::Sequence {
        return false;
    }
    let commuting_count = writes && matches!(key, AccessKey::Count(_));
    (!commuting_count && set.contains(&key))
        || key
            .table()
            .is_some_and(|table| set.contains(&AccessKey::Scan(table)))
        || matches!(key, AccessKey::Scan(table) if set.iter().any(|other| other.table() == Some(table)))
}

fn workspace(inputs: &[PreparedMutation], limits: EpochLimits) -> Result<usize, EpochError> {
    if inputs.is_empty()
        || inputs.len() > limits.max_commands
        || limits.max_workers == 0
        || limits.worker_stack_bytes < 256 * 1024
        || limits.worker_stack_bytes > 64 * 1024 * 1024
    {
        return Err(EpochError::Capacity("invalid epoch dimensions"));
    }
    let input_bytes = postcard::experimental::serialized_size(inputs)
        .map_err(CoreError::from)?
        .checked_mul(32)
        .ok_or(EpochError::Capacity("input size"))?;
    let trace_bytes = limits
        .max_trace_entries
        .checked_mul(std::mem::size_of::<ReadObservation>().max(std::mem::size_of::<AccessKey>()))
        .and_then(|n| n.checked_mul(8))
        .and_then(|n| n.checked_mul(inputs.len()))
        .ok_or(EpochError::Capacity("access record size"))?;
    // Queue/root metadata has a structural bound independent of serialized row
    // payloads; include both planning and execution containers conservatively.
    let metadata = [
        std::mem::size_of::<PreparedMutation>(),
        std::mem::size_of::<PlannedEntry>(),
        std::mem::size_of::<Option<RowVersion>>(),
        std::mem::size_of::<Option<RowVersion>>(),
        std::mem::size_of::<Option<ApplyResult>>(),
        std::mem::size_of::<ApplyResult>(),
        std::mem::size_of::<Executed>(),
        std::mem::size_of::<Executed>(),
        2048,
    ]
    .into_iter()
    .try_fold(0usize, |sum, size| sum.checked_add(size))
    .ok_or(EpochError::Capacity("queue metadata"))?;
    let concurrent_workers = limits.max_workers.min(inputs.len());
    let fixed = limits
        .worker_stack_bytes
        .checked_mul(if concurrent_workers > 1 {
            concurrent_workers
        } else {
            0
        })
        .and_then(|n| {
            limits
                .max_edges
                .checked_mul(32)
                .and_then(|edges| n.checked_add(edges))
        })
        .and_then(|n| {
            inputs
                .len()
                .checked_mul(metadata)
                .and_then(|queues| n.checked_add(queues))
        })
        .and_then(|n| n.checked_add(trace_bytes))
        .and_then(|n| n.checked_add(input_bytes))
        .ok_or(EpochError::Capacity("workspace dimensions"))?;
    let remaining = limits
        .max_bytes
        .checked_sub(fixed)
        .ok_or(EpochError::Capacity("fixed workspace"))?;
    let divisor = inputs
        .len()
        .checked_mul(2)
        .ok_or(EpochError::Capacity("version workspace"))?;
    remaining
        .checked_div(divisor)
        .filter(|n| *n > 0)
        .ok_or(EpochError::Capacity("row workspace"))
}
fn vector<T>(capacity: usize) -> Result<Vec<T>, EpochError> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(capacity)
        .map_err(|_| EpochError::Capacity("vector allocation"))?;
    Ok(result)
}
fn slots<T>(count: usize) -> Result<Vec<Option<T>>, EpochError> {
    let mut result = vector(count)?;
    result.resize_with(count, || None);
    Ok(result)
}
struct HashWriter(blake3::Hasher);
impl Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn hash<T: Serialize + ?Sized>(value: &T) -> Result<[u8; 32], EpochError> {
    let writer =
        postcard::to_io(&value, HashWriter(blake3::Hasher::new())).map_err(CoreError::from)?;
    Ok(*writer.0.finalize().as_bytes())
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestFault {
    Panic(usize),
    Spawn(usize),
}

#[cfg(test)]
#[path = "epoch_tests.rs"]
mod tests;
