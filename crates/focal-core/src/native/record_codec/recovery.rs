//! Detached restoration of one complete native Core checkpoint. The input
//! bytes remain caller-owned and funded. No intermediate owner is published,
//! no participant action is replayed, and this API makes no Session/Raft promise.
use super::*;
use focal_evidence::{ContentStore, NativeLocalCustody, NativeSchemaVerifier, NativeVerificationBudget};
use focal_memory::{BudgetLane, RangeHydrationLimits};
use focal_model::lifecycle::{aggregation, artifact_descriptor, claim_descriptor,
    evidence::ResponseLimits, validation_descriptor};
use read_source::Meter;
use std::cell::Cell;

#[path = "recovery_source.rs"]
mod source;

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
#[cfg(test)]
#[path = "recovery_authored_tests.rs"]
mod authored_tests;

/// Node-derived construction bounds, separate from participant protocol fields.
/// Deployment profiles supply these; recovery introduces no human CLI concepts.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub native: NativeLimits,
    pub acceptance: aggregation::Limits,
    pub artifact: artifact_descriptor::Limits,
    pub claim: claim_descriptor::Limits,
    pub declaration: validation::Limits,
    pub validation: validation_descriptor::Limits,
    pub response: ResponseLimits,
    pub creation_objects: usize,
    pub work: Work,
}
/// One cumulative allowance across index construction, every row preparation,
/// repeated construction pass, dependency lookup and final root validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Work {
    pub parsing: usize,
    pub source: usize,
    pub model: usize,
    pub lookup: usize,
}
struct Meters { parsing: Meter, source: Meter, model: Meter, lookup: Meter }
impl Meters {
    fn new(work: Work) -> Self {
        Self { parsing: Meter::new(work.parsing), source: Meter::new(work.source),
            model: Meter::new(work.model), lookup: Meter::new(work.lookup) }
    }
}
impl Limits {
    fn dispatch(self) -> read_dispatch::Limits {
        read_dispatch::Limits { native: self.native, acceptance: self.acceptance,
            artifact: self.artifact, claim: self.claim, declaration: self.declaration,
            validation: self.validation, response: self.response, creation_objects: self.creation_objects }
    }
}
struct Custody<'a, S> { store: &'a ContentStore, schemas: &'a S, budget: &'a MemoryBudget, work: &'a Meter }
impl<S: NativeSchemaVerifier> read_evidence::Custody for Custody<'_, S> {
    fn recover(&self, request: RequestKey, descriptor: &artifact_descriptor::ArtifactDescriptor,
        pointer: artifact_descriptor::ContentPointer, local_revision: u64) -> Result<NativeLocalCustody, NativeError> {
        let verification = NativeVerificationBudget::for_schema(descriptor.schema_hash(), self.schemas)?;
        // Prepay bounded content-tree reads/hashes, byte comparison and schema
        // traversal under the pinned verifier's declared maximum. Verifier
        // implementations remain trusted bounded native code, not agent jobs.
        let visits = verification.maximum_bytes().checked_mul(64)
            .and_then(|n| n.checked_add(verification.peak_bytes()))
            .and_then(|n| n.checked_add(4096))
            .ok_or(NativeError::Capacity("custody recovery work"))?;
        self.work.charge(visits).map_err(read_evidence::codec)?;
        // The row reservation already covers its final inline custody value.
        // Verification's independent permit covers tree/schema scratch until
        // the genuine token is moved into that already-funded row.
        let verified = self.store.recover_native_artifact(request, descriptor, pointer,
            local_revision, self.budget, self.schemas)?;
        Ok(verified.custody())
    }
}

struct Shared<'a, 'bytes, C> {
    index: &'a read_index::Index<'bytes>,
    header: checkpoint::CheckpointHeader,
    limits: read_dispatch::Limits,
    meters: &'a Meters,
    budget: &'a MemoryBudget,
    custody: &'a C,
    failure: Cell<Option<NativeError>>,
}
impl<C> Shared<'_, '_, C> {
    fn refuse(&self, error: NativeError) -> MemoryError {
        let original = match self.failure.take() { Some(original) => original, None => error };
        self.failure.set(Some(original));
        MemoryError::InvalidConfiguration("native checkpoint restoration refused")
    }
    fn error(&self, error: MemoryError) -> NativeError {
        match self.failure.take() { Some(error) => error, None => error.into() }
    }
}

/// Restore the exact recorded prefix under a fresh owner incarnation. The
/// caller establishes checkpoint provenance and the original-to-fresh RangeId
/// mapping in its enclosing Session recovery protocol. Evidence must already
/// exist in this local ContentStore and pass its pinned schema verification.
///
/// On any refusal, all provisional roots, rows, indices and reservations drop.
/// Success proves intrinsic/cross-row consistency of this Core snapshot; it
/// neither activates a wire decoder nor acknowledges a replicated log entry.
pub fn restore<S: NativeSchemaVerifier>(
    checkpoint: &checkpoint::StructuralCheckpoint<'_>, range: RangeId, mut limits: Limits,
    budget: MemoryBudget, store: &ContentStore, schemas: &S,
) -> Result<Core<NativeState>, NativeError> {
    let header = checkpoint.header();
    limits.native = checked_native_limits(header.ledger, limits.native)?;
    let meters = Meters::new(limits.work);
    let custody = Custody { store, schemas, budget: &budget, work: &meters.model };
    let index = read_index::Index::build(checkpoint, limits.native, &budget, &meters.parsing, &meters.lookup)?;
    let shared = Shared { index: &index, header, limits: limits.dispatch(), meters: &meters,
        budget: &budget, custody: &custody, failure: Cell::new(None) };
    let expected_entries = usize::try_from(header.rows).map_err(|_| NativeError::Capacity("checkpoint row count"))?;
    let mut owner = RangeStore::begin_hydration_partitioned(range, limits.native.range,
        budget.clone(), page_partition, RangeHydrationLimits { expected_entries, max_phases: read_index::PHASES })?;
    for phase in 0..read_index::PHASES {
        let count = *index.counts.get(phase).ok_or(ContractError::InvalidManifest)?;
        if count == 0 { continue; }
        let scan_work = checkpoint.quote().visits;
        meters.parsing.charge(scan_work).map_err(read_evidence::codec)?;
        let rows = checkpoint.rows(scan_work).map_err(read_evidence::codec)?;
        let sources = source::Sources::new(rows, phase, &shared);
        owner = owner.insert_sources(count, sources, |row| {
            // Range reconstruction copies only retained neighbors of changed
            // pages. Prepay the node's bounded maximum row traversal before the
            // existing fallible copier touches any nested collection.
            let work = limits.native.range.max_entry_bytes.checked_mul(8)
                .and_then(|n| n.checked_add(4096))
                .ok_or(MemoryError::CounterExhausted("recovery neighbor copy work"))?;
            meters.model.charge(work).map_err(|error| shared.refuse(read_evidence::codec(error)))?;
            prepare::copy(row)
        }).map_err(|error| shared.error(error))?;
    }
    let rows = owner.finish(header.prefix.0, |view| {
        read_validate::validate(view, header.ledger, header.profile, header.prefix,
            limits.native, &meters.model, &budget).map_err(|error| shared.refuse(error))
    }).map_err(|error| shared.error(error))?;
    // All decoder/index/workspace borrows end before exposing the sole owner.
    drop(shared);
    drop(index);
    Ok(Core { state: NativeState { ledger: header.ledger, profile: header.profile,
        rows, budget }, limits: limits.native })
}
