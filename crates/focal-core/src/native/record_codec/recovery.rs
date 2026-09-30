//! Detached restoration of one complete native Core checkpoint. The input
//! bytes remain caller-owned and funded. No intermediate owner is published,
//! no participant action is replayed, and this API makes no Session/Raft promise.
use super::*;
use focal_evidence::{
    NativeCustodyReader, NativeLocalCustody, NativeSchemaVerifier, NativeVerificationBudget,
};
use focal_memory::{BudgetLane, RangeHydrationLimits};
use focal_model::lifecycle::{
    aggregation, artifact_descriptor, claim_descriptor, evidence::ResponseLimits,
    validation_descriptor,
};
use read_source::Meter;
use std::cell::Cell;

#[path = "recovery_source.rs"]
mod source;

#[cfg(test)]
#[path = "recovery_authored_tests.rs"]
mod authored_tests;
#[cfg(test)]
#[path = "recovery_owner_tests.rs"]
mod owner_tests;
#[cfg(test)]
#[path = "recovery_tests.rs"]
pub(in crate::native) mod tests;

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
/// The scans of a checkpoint's bytes a restore charges whole: the index scan
/// and one scan per hydration phase, each the inspection's visits.
const PARSING_SCANS: usize = read_index::PHASES.saturating_add(1);
/// The parsing of the row and event bodies themselves, per body byte: the
/// codec's cursor visits a body's fields and bytes several times over
/// (measured at 18 visits per byte on the workflows at authored maxima).
const PARSE_PER_BYTE: usize = 32;
/// Per-row and per-byte ceilings of a restore's work, measured over the
/// recorded workflows at authored maxima
/// (`bound_tests::a_restore_s_work_stays_within_the_envelope_its_checkpoint_declares`,
/// which pins them: a restore that outgrows one fails there, and the ceiling
/// is raised from the measurement, never guessed). Model work per row is the
/// row's own validation and its visits as a target — a row is looked up from
/// its outcome, its claim, its head and the history index, each a bounded
/// directory descent.
const SOURCE_PER_ROW: usize = 4096;
const SOURCE_PER_BYTE: usize = 64;
const MODEL_PER_ROW: usize = 65_536;
const MODEL_PER_BYTE: usize = 256;
const LOOKUP_PER_ROW: usize = 65_536;
impl Work {
    /// The work a restore may spend on a checkpoint of the declared shape (the
    /// audit's F57): the scans of its bytes, a ceiling per row and per byte,
    /// and the history sort at its bound (every event a primary entry and at
    /// most one secondary). A checkpoint the configuration admits fits by
    /// construction; a body that costs more than its declared rows and bytes
    /// allow is refused. Saturating: a ceiling past `usize` is still a bound.
    pub fn for_shape(visits: usize, bytes: usize, rows: usize) -> Self {
        let sort = read_history::sort_visits(rows.saturating_mul(2)).unwrap_or(usize::MAX);
        Self {
            parsing: visits
                .saturating_mul(PARSING_SCANS)
                .saturating_add(bytes.saturating_mul(PARSE_PER_BYTE)),
            source: rows
                .saturating_mul(SOURCE_PER_ROW)
                .saturating_add(bytes.saturating_mul(SOURCE_PER_BYTE)),
            model: rows
                .saturating_mul(MODEL_PER_ROW)
                .saturating_add(bytes.saturating_mul(MODEL_PER_BYTE))
                .saturating_add(sort),
            lookup: rows.saturating_mul(LOOKUP_PER_ROW),
        }
    }
    /// The ceiling for the largest checkpoint a configuration admits:
    /// `for_shape` at the inspection's visits, bytes and rows.
    pub fn for_limits(visits: usize, bytes: usize, rows: usize) -> Self {
        Self::for_shape(visits, bytes, rows)
    }
    pub(super) fn min(self, other: Self) -> Self {
        Self {
            parsing: self.parsing.min(other.parsing),
            source: self.source.min(other.source),
            model: self.model.min(other.model),
            lookup: self.lookup.min(other.lookup),
        }
    }
    /// What was spent of `self` once the meters ran.
    fn used(self, meters: &Meters) -> Self {
        Self {
            parsing: self.parsing.saturating_sub(meters.parsing.remaining()),
            source: self.source.saturating_sub(meters.source.remaining()),
            model: self.model.saturating_sub(meters.model.remaining()),
            lookup: self.lookup.saturating_sub(meters.lookup.remaining()),
        }
    }
    /// The model work one artifact row may add: a custody recovery under the
    /// largest verification any schema may declare. Charged once the index
    /// has counted the artifact rows, since a checkpoint's shape does not
    /// name its families.
    pub fn custody_per_artifact() -> usize {
        NativeVerificationBudget::ceiling().recovery_work()
    }
    fn extended(self, model: usize) -> Self {
        Self {
            model: self.model.saturating_add(model),
            ..self
        }
    }
    #[cfg(test)]
    pub(crate) fn extended_for_test(self, model: usize) -> Self {
        self.extended(model)
    }
    /// Whether every allowance of `self` is at most `other`'s.
    pub fn within(self, other: Self) -> bool {
        self.parsing <= other.parsing
            && self.source <= other.source
            && self.model <= other.model
            && self.lookup <= other.lookup
    }
}
/// What a restore was allowed and what it spent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Envelope {
    pub allowed: Work,
    pub used: Work,
}
pub(super) struct Meters {
    pub(super) parsing: Meter,
    pub(super) source: Meter,
    pub(super) model: Meter,
    pub(super) lookup: Meter,
}
impl Meters {
    pub(super) fn new(work: Work) -> Self {
        Self {
            parsing: Meter::new(work.parsing),
            source: Meter::new(work.source),
            model: Meter::new(work.model),
            lookup: Meter::new(work.lookup),
        }
    }
}
impl Limits {
    pub(super) fn dispatch(self) -> read_dispatch::Limits {
        read_dispatch::Limits {
            native: self.native,
            acceptance: self.acceptance,
            artifact: self.artifact,
            claim: self.claim,
            declaration: self.declaration,
            validation: self.validation,
            response: self.response,
            creation_objects: self.creation_objects,
        }
    }
}
pub(super) struct Custody<'a, S, R> {
    store: &'a R,
    schemas: &'a S,
    budget: &'a MemoryBudget,
    work: &'a Meter,
}
impl<'a, S, R> Custody<'a, S, R> {
    pub(super) fn new(
        store: &'a R,
        schemas: &'a S,
        budget: &'a MemoryBudget,
        work: &'a Meter,
    ) -> Self {
        Self {
            store,
            schemas,
            budget,
            work,
        }
    }
}
impl<S: NativeSchemaVerifier, R: NativeCustodyReader> read_evidence::Custody for Custody<'_, S, R> {
    fn recover(
        &self,
        request: RequestKey,
        descriptor: &artifact_descriptor::ArtifactDescriptor,
        pointer: artifact_descriptor::ContentPointer,
        local_revision: u64,
    ) -> Result<NativeLocalCustody, NativeError> {
        let verification =
            NativeVerificationBudget::for_schema(descriptor.schema_hash(), self.schemas)?;
        // Prepay bounded content-tree reads/hashes, byte comparison and schema
        // traversal under the pinned verifier's declared maximum
        // (`recovery_work`, the term the envelope carries per artifact row).
        // Verifier implementations remain trusted bounded native code, not
        // agent jobs.
        self.work
            .charge(verification.recovery_work())
            .map_err(read_evidence::codec)?;
        // The row reservation already covers its final inline custody value.
        // Verification's independent permit covers tree/schema scratch until
        // the genuine token is moved into that already-funded row.
        let verified = self.store.recover_native_artifact(
            request,
            descriptor,
            pointer,
            local_revision,
            self.budget,
            self.schemas,
        )?;
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
        let original = match self.failure.take() {
            Some(original) => original,
            None => error,
        };
        self.failure.set(Some(original));
        MemoryError::InvalidConfiguration("native checkpoint restoration refused")
    }
    fn error(&self, error: MemoryError) -> NativeError {
        match self.failure.take() {
            Some(error) => error,
            None => error.into(),
        }
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
/// Restore a core from its checkpoint, funded by the completion allowance:
/// what a restore completes is admitted work — a committed checkpoint a
/// replica installs, an activation a replica applies — and ordinary credit,
/// which live admission may hold entirely, is never its condition.
pub fn restore<S: NativeSchemaVerifier, R: NativeCustodyReader>(
    checkpoint: &checkpoint::StructuralCheckpoint<'_>,
    range: RangeId,
    limits: Limits,
    budget: MemoryBudget,
    store: &R,
    schemas: &S,
) -> Result<Core<NativeState>, NativeError> {
    restore_in(
        checkpoint,
        range,
        limits,
        budget,
        store,
        schemas,
        BudgetLane::Completion,
    )
}
/// [`restore`] with every root, index, layout, hydration page and assembled
/// member charged to `lane`.
pub fn restore_in<S: NativeSchemaVerifier, R: NativeCustodyReader>(
    checkpoint: &checkpoint::StructuralCheckpoint<'_>,
    range: RangeId,
    limits: Limits,
    budget: MemoryBudget,
    store: &R,
    schemas: &S,
    lane: BudgetLane,
) -> Result<Core<NativeState>, NativeError> {
    restore_measured(checkpoint, range, limits, budget, store, schemas, lane).map(|(core, _)| core)
}
/// [`restore_in`], with the work it spent: what the envelope's measurement
/// pins itself to. The allowance is the envelope of this checkpoint's
/// declared shape under the configuration's ceiling: an admitted checkpoint
/// always fits, and a body that costs more than its shape allows is refused.
pub(crate) fn restore_measured<S: NativeSchemaVerifier, R: NativeCustodyReader>(
    checkpoint: &checkpoint::StructuralCheckpoint<'_>,
    range: RangeId,
    limits: Limits,
    budget: MemoryBudget,
    store: &R,
    schemas: &S,
    lane: BudgetLane,
) -> Result<(Core<NativeState>, Envelope), NativeError> {
    let quote = checkpoint.quote();
    let work = Work::for_shape(quote.visits, quote.bytes, quote.rows).min(limits.work);
    restore_with_work(
        checkpoint, range, limits, budget, store, schemas, lane, work,
    )
}
/// The restore under an explicit allowance, spending reported.
#[allow(clippy::too_many_arguments)] // The restore's inputs, each its own owner's.
pub(crate) fn restore_with_work<S: NativeSchemaVerifier, R: NativeCustodyReader>(
    checkpoint: &checkpoint::StructuralCheckpoint<'_>,
    range: RangeId,
    mut limits: Limits,
    budget: MemoryBudget,
    store: &R,
    schemas: &S,
    lane: BudgetLane,
    work: Work,
) -> Result<(Core<NativeState>, Envelope), NativeError> {
    let header = checkpoint.header();
    limits.native = checked_native_limits(header.ledger, limits.native)?;
    // The rows are restored into one store, validated as a whole, then laid
    // out per the recorded layout (25 §4): members keep their durable
    // identities while the producer identity is the caller's fresh one.
    let layout = checkpoint.layout(limits.native.max_ranges, &budget, lane)?;
    let first_member = layout
        .members()
        .first()
        .map(|member| member.id)
        .ok_or(ContractError::InvalidManifest)?;
    let hydrated = hydrate_frame(
        checkpoint,
        header,
        first_member,
        limits,
        &budget,
        store,
        schemas,
        lane,
        work,
        |view, meter, budget| {
            read_validate::validate(
                view,
                header.ledger,
                header.profile,
                header.prefix,
                limits.native,
                meter,
                budget,
            )
        },
    )?;
    let (rows, envelope) = hydrated.assemble(range, layout, &budget, lane)?;
    Ok((
        Core {
            state: NativeState {
                ledger: header.ledger,
                profile: header.profile,
                rows,
                budget,
            },
            limits: limits.native,
        },
        envelope,
    ))
}
/// Rows hydrated from a frame and validated as a whole, not yet laid out:
/// what a checkpoint restore lays out per its recorded layout and an
/// archive bundle's hydration (`archive::ArchiveCore`) lays out as one
/// member.
pub(super) struct Hydrated {
    rows: RangeStore<Key, Row>,
    meters: Meters,
    allowed: Work,
}
impl Hydrated {
    pub(super) fn assemble(
        self,
        range: RangeId,
        layout: ranges::RangeLayout,
        budget: &MemoryBudget,
        lane: BudgetLane,
    ) -> Result<(ranges::NativeRanges, Envelope), NativeError> {
        let meters = self.meters;
        let rows =
            ranges::NativeRanges::from_store(range, layout, self.rows, budget, lane, |row| {
                let work = read_dispatch::objects::copy_work(row)
                    .map_err(|_| MemoryError::InvalidConfiguration("native row copy"))?;
                meters
                    .model
                    .charge(work)
                    .map_err(|_| MemoryError::InvalidConfiguration("native row copy work"))?;
                prepare::copy(row)
            })?;
        let used = self.allowed.used(&meters);
        Ok((
            rows,
            Envelope {
                allowed: self.allowed,
                used,
            },
        ))
    }
}
/// The phased hydration of a frame's rows into one store: the dependency
/// index, every phase in order under the meters, and `validate` over the
/// whole before the store is given up. A checkpoint and an archive bundle
/// hydrate the same way; what they are validated as differs.
#[allow(clippy::too_many_arguments)] // The hydration's inputs, each its own owner's.
pub(super) fn hydrate_frame<'a, F, S, R>(
    frame: &F,
    header: checkpoint::CheckpointHeader,
    store_id: RangeId,
    limits: Limits,
    budget: &MemoryBudget,
    store: &R,
    schemas: &S,
    lane: BudgetLane,
    work: Work,
    validate: impl FnOnce(
        focal_memory::RangeHydrationView<'_, Key, Row>,
        &Meter,
        &MemoryBudget,
    ) -> Result<(), NativeError>,
) -> Result<Hydrated, NativeError>
where
    F: inspect::RowFrame<'a>,
    S: NativeSchemaVerifier,
    R: NativeCustodyReader,
{
    let meters = Meters::new(work);
    let custody = Custody {
        store,
        schemas,
        budget,
        work: &meters.model,
    };
    let index = read_index::Index::build(
        frame,
        limits.native,
        budget,
        &meters.parsing,
        &meters.lookup,
        lane,
    )?;
    // The artifact rows are counted now: each may recover its custody under
    // the largest verification a schema may declare.
    let artifacts = index
        .counts
        .get(read_index::ARTIFACT_PHASE)
        .copied()
        .unwrap_or(0);
    let custody_work = artifacts.saturating_mul(Work::custody_per_artifact());
    // The custody term extends the shape's envelope, never past the
    // configuration's ceiling.
    let allowed = work.extended(custody_work).min(limits.work);
    meters
        .model
        .extend(allowed.model.saturating_sub(work.model));
    let shared = Shared {
        index: &index,
        header,
        limits: limits.dispatch(),
        meters: &meters,
        budget,
        custody: &custody,
        failure: Cell::new(None),
    };
    let expected_entries =
        usize::try_from(header.rows).map_err(|_| NativeError::Capacity("checkpoint row count"))?;
    let mut owner = RangeStore::begin_hydration_partitioned_in(
        store_id,
        limits.native.range,
        budget.clone(),
        page_partition,
        RangeHydrationLimits {
            expected_entries,
            max_phases: read_index::PHASES,
        },
        lane,
    )?;
    for phase in 0..read_index::PHASES {
        let count = *index
            .counts
            .get(phase)
            .ok_or(ContractError::InvalidManifest)?;
        if count == 0 {
            continue;
        }
        let scan_work = frame.quote().visits;
        meters
            .parsing
            .charge(scan_work)
            .map_err(read_evidence::codec)?;
        let rows = frame.rows(scan_work).map_err(read_evidence::codec)?;
        let sources = source::Sources::new(rows, phase, &shared);
        owner = owner
            .insert_sources(count, sources, |row| {
                // Range reconstruction copies only retained neighbors of changed
                // pages. Prepay the copied row's bounded traversal before the
                // existing fallible copier touches any nested collection.
                let work =
                    read_dispatch::objects::copy_work(row).map_err(|error| shared.refuse(error))?;
                meters
                    .model
                    .charge(work)
                    .map_err(|error| shared.refuse(read_evidence::codec(error)))?;
                prepare::copy(row)
            })
            .map_err(|error| shared.error(error))?;
    }
    let rows = owner
        .finish(header.prefix.0, |view| {
            validate(view, &meters.model, budget).map_err(|error| shared.refuse(error))
        })
        .map_err(|error| shared.error(error))?;
    // All decoder/index/workspace borrows end before exposing the sole owner.
    drop(shared);
    drop(index);
    Ok(Hydrated {
        rows,
        meters,
        allowed,
    })
}
