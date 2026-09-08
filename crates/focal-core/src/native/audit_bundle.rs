//! Claimant-authored closure of an actual sealed audit. The response collection,
//! work acceptance and evaluator-owned result artifacts are independent rows.
use super::prepare::{ALLOCATION, Extra, Extras, Scratch, add, array};
use super::*;
use focal_model::lifecycle::aggregation::PublicationPosition;
use focal_model::lifecycle::audit::{ResultTestament, ResultTestamentState};
use focal_model::{ObjectId, ObjectRevision};

#[path = "audit_bundle_reads.rs"]
mod reads;
#[cfg(test)]
#[path = "audit_bundle_tests.rs"]
mod tests;

/// One immutable complete cohort and its original publication witnesses.
/// Generation and posting are separate ledger facts, not response deliveries.
#[derive(Debug)]
pub struct NativeResultTestament {
    testament: ResultTestament,
    publications: Vec<NativeAuditPublication>,
    captured_at: SessionSeq,
    generated_revision: ObjectRevision,
    generated_at: PublicationPosition,
    posted_at: Option<PublicationPosition>,
}

/// Retained generation and publication facts supplied by the recovery importer.
/// These coordinates are checked against authenticated event rows by that importer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RecoveredCoordinates {
    pub(super) generated: Binding,
    pub(super) captured_at: SessionSeq,
    pub(super) generated_at: PublicationPosition,
    pub(super) posted_at: Option<PublicationPosition>,
}
impl NativeResultTestament {
    pub fn testament(&self) -> &ResultTestament {
        &self.testament
    }
    pub fn publications(&self) -> &[NativeAuditPublication] {
        &self.publications
    }
    pub fn publication(&self, result: validation::AcceptedResult) -> Option<PublicationPosition> {
        let key = NativeResultKey::of(result);
        self.publications
            .binary_search_by_key(&key, |row| row.key)
            .ok()
            .and_then(|at| self.publications.get(at))
            .map(|row| row.position)
    }
    pub fn captured_at(&self) -> SessionSeq {
        self.captured_at
    }
    pub fn generated_at(&self) -> PublicationPosition {
        self.generated_at
    }
    /// Original recorded generation identity, preserved through posting rather
    /// than inferred by subtracting from the current revision.
    pub fn generated_binding(&self) -> Binding {
        Binding {
            revision: self.generated_revision,
            ..self.testament.binding()
        }
    }
    pub fn posted_at(&self) -> Option<PublicationPosition> {
        self.posted_at
    }

    fn check(&self) -> Result<(), ContractError> {
        if self.generated_at.sequence <= self.captured_at
            || self.testament.results().len() != self.publications.len()
        {
            return Err(ContractError::InvalidCut);
        }
        let expected = match self.testament.state() {
            ResultTestamentState::Generated => self.generated_binding(),
            ResultTestamentState::Posted => self.generated_binding().next()?,
        };
        self.testament.binding().check(&expected)?;
        match (self.testament.state(), self.posted_at) {
            (ResultTestamentState::Generated, None) => Ok(()),
            (ResultTestamentState::Posted, Some(at)) if at > self.generated_at => Ok(()),
            _ => Err(ContractError::InvalidCut),
        }
    }
    fn heap_charge(&self) -> Result<usize, NativeError> {
        add(
            add(
                self.testament.retained_heap_bytes()?,
                self.testament
                    .heap_allocations()?
                    .checked_mul(ALLOCATION)
                    .ok_or(NativeError::Capacity("audit allocations"))?,
            )?,
            array::<NativeAuditPublication>(self.publications.capacity())?,
        )
    }
    fn try_copy(&self) -> Result<Self, NativeError> {
        self.check()?;
        let testament = self.testament.try_copy(self.testament.copy_charge()?)?;
        let mut publications = Vec::new();
        publications
            .try_reserve_exact(self.publications.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        if publications.capacity() > self.publications.capacity() {
            return Err(MemoryError::AllocationFailed.into());
        }
        publications.extend_from_slice(&self.publications);
        Ok(Self {
            testament,
            publications,
            captured_at: self.captured_at,
            generated_revision: self.generated_revision,
            generated_at: self.generated_at,
            posted_at: self.posted_at,
        })
    }
}

#[derive(Debug)]
pub(super) struct OwnedResultTestament(Vec<NativeResultTestament>);
const RESULT_TESTAMENT_CONTAINER: usize = size_of::<NativeResultTestament>() + ALLOCATION;
impl OwnedResultTestament {
    pub(super) const fn container_charge() -> usize {
        RESULT_TESTAMENT_CONTAINER
    }
    fn new(record: NativeResultTestament) -> Result<Self, NativeError> {
        record.check()?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(1)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if rows.capacity() != 1 {
            return Err(MemoryError::AllocationFailed.into());
        }
        rows.push(record);
        Ok(Self(rows))
    }
    pub(super) fn get(&self) -> Option<&NativeResultTestament> {
        match self.0.as_slice() {
            [record] => Some(record),
            _ => None,
        }
    }
    pub(super) fn heap_charge(&self) -> Result<usize, NativeError> {
        add(
            Self::container_charge(),
            self.get()
                .ok_or(ContractError::MissingEvidence)?
                .heap_charge()?,
        )
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let source = self.get().ok_or(MemoryError::MissingKey)?;
        let copy = source
            .try_copy()
            .map_err(|_| MemoryError::AllocationFailed)?;
        let copied = Self::new(copy).map_err(|_| MemoryError::AllocationFailed)?;
        if copied
            .heap_charge()
            .map_err(|_| MemoryError::AllocationFailed)?
            > self
                .heap_charge()
                .map_err(|_| MemoryError::AllocationFailed)?
        {
            return Err(MemoryError::AllocationFailed);
        }
        Ok(copied)
    }

    /// Conservative fixed-field/hash work for the recovery consistency pass.
    /// Includes actual canonical audit hashing, every publication key and each
    /// result's binary search. The importer debits this before invoking recover.
    pub(super) fn recovery_visits(
        members: usize,
        results: usize,
        publications: usize,
    ) -> Result<usize, NativeError> {
        let search = usize::try_from(usize::BITS)
            .map_err(|_| ContractError::Capacity)?
            .checked_add(1)
            .and_then(|value| value.checked_mul(256))
            .ok_or(ContractError::Capacity)?;
        add(
            1024,
            add(
                members.checked_mul(2048).ok_or(ContractError::Capacity)?,
                add(
                    results
                        .checked_mul(add(1024, search)?)
                        .ok_or(ContractError::Capacity)?,
                    publications
                        .checked_mul(256)
                        .ok_or(ContractError::Capacity)?,
                )?,
            )?,
        )
    }

    /// Install already hydrated historical values without invoking generation,
    /// posting or participant authority. Funding for every nested buffer and the
    /// final singleton must already be held. Original event/result membership is
    /// verified by the enclosing importer; this boundary additionally binds the
    /// native content hash to the complete cohort and publication coordinates.
    pub(super) fn recover(
        testament: ResultTestament,
        publications: Vec<NativeAuditPublication>,
        coordinates: RecoveredCoordinates,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<(Self, usize), NativeError> {
        let cohort = testament.cohort();
        let visits = Self::recovery_visits(
            cohort.members().len(),
            cohort.results().len(),
            publications.len(),
        )?;
        if visits > max_visits {
            return Err(ContractError::Capacity.into());
        }
        if coordinates.generated.revision != ObjectRevision(1)
            || coordinates.generated.object.is_zero()
            || coordinates.generated.content == ContentHash([0; 32])
            || coordinates.generated.ledger != cohort.claim_binding().ledger
            || coordinates.captured_at < testament.sealed_at()
            || publications.len() != cohort.results().len()
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let mut previous = None;
        for publication in &publications {
            if previous.is_some_and(|key| key >= publication.key)
                || publication.position.sequence.0 == 0
                || publication.position.sequence > coordinates.captured_at
            {
                return Err(ContractError::InvalidCut.into());
            }
            previous = Some(publication.key);
        }
        for result in cohort.results() {
            let key = NativeResultKey::of(*result);
            if publications
                .binary_search_by_key(&key, |publication| publication.key)
                .is_err()
            {
                return Err(ContractError::MissingEvidence.into());
            }
        }
        if content_hash(cohort, &publications, coordinates.captured_at)?
            != coordinates.generated.content
        {
            return Err(ContractError::ContentConflict.into());
        }
        let record = NativeResultTestament {
            testament,
            publications,
            captured_at: coordinates.captured_at,
            generated_revision: coordinates.generated.revision,
            generated_at: coordinates.generated_at,
            posted_at: coordinates.posted_at,
        };
        record.generated_binding().check(&coordinates.generated)?;
        record.check()?;
        let actual = add(Self::container_charge(), record.heap_charge()?)?;
        if actual > max_bytes {
            return Err(ContractError::Capacity.into());
        }
        let owned = Self::new(record)?;
        let actual = owned.heap_charge()?;
        if actual > max_bytes {
            return Err(ContractError::Capacity.into());
        }
        Ok((owned, actual))
    }
}

pub(super) fn as_result_testament(row: Option<&Row>) -> Option<&NativeResultTestament> {
    match row {
        Some(Row::ResultTestament(row)) => row.get(),
        _ => None,
    }
}
pub(super) fn index(row: Option<&Row>) -> Option<TestamentId> {
    match row {
        Some(Row::ClaimResultTestament(id)) => Some(*id),
        _ => None,
    }
}

fn result_key(hash: &mut blake3::Hasher, key: NativeResultKey) {
    hash.update(&key.evaluation.claim.0);
    hash.update(&key.evaluation.validation.0);
    hash.update(&key.evaluation.generation.to_le_bytes());
    match key.evaluation.target {
        EvaluationTarget::Admission => {
            hash.update(&[0]);
        }
        EvaluationTarget::Increment { artifact } => {
            hash.update(&[1]);
            hash.update(&artifact.0);
        }
        EvaluationTarget::Work {
            response,
            slot,
            artifact,
        } => {
            hash.update(&[2]);
            hash.update(&response.0);
            hash.update(&slot.to_le_bytes());
            hash.update(&artifact.0);
        }
        EvaluationTarget::MissingSlot { response, slot } => {
            hash.update(&[3]);
            hash.update(&response.0);
            hash.update(&slot.to_le_bytes());
        }
        EvaluationTarget::Delivery { response } => {
            hash.update(&[4]);
            hash.update(&response.0);
        }
    }
    hash.update(&key.revision.0.to_le_bytes());
}

fn content_hash(
    cohort: &focal_model::lifecycle::audit::AuditCohort,
    publications: &[NativeAuditPublication],
    captured_at: SessionSeq,
) -> Result<ContentHash, NativeError> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/result-testament-content/1");
    hash.update(&cohort.content_fingerprint()?.0);
    hash.update(&captured_at.0.to_le_bytes());
    hash.update(
        &u64::try_from(publications.len())
            .map_err(|_| ContractError::Capacity)?
            .to_le_bytes(),
    );
    for publication in publications {
        result_key(&mut hash, publication.key);
        hash.update(&publication.position.sequence.0.to_le_bytes());
        hash.update(&publication.position.ordinal.to_le_bytes());
    }
    Ok(ContentHash(*hash.finalize().as_bytes()))
}

fn generate(
    audit: NativeAudit,
    id: TestamentId,
    principal: Principal,
    generated_at: PublicationPosition,
) -> Result<NativeResultTestament, NativeError> {
    let (cohort, publications, captured_at, visits_left) = audit.into_parts();
    // Includes complete/order verification and hashing in both layers. Every
    // member and accepted result is visited a fixed number of times; no sort or
    // ledger scan is repeated here after the audited construction.
    let visits = add(
        cohort
            .members()
            .len()
            .checked_mul(6)
            .ok_or(ContractError::Capacity)?,
        add(
            publications
                .len()
                .checked_mul(6)
                .ok_or(ContractError::Capacity)?,
            1,
        )?,
    )?;
    if visits > visits_left {
        return Err(NativeError::Capacity("audit bundle visits"));
    }
    let binding = Binding {
        ledger: cohort.claim_binding().ledger,
        object: ObjectId(id.0),
        content: content_hash(&cohort, &publications, captured_at)?,
        revision: ObjectRevision(1),
    };
    let testament = ResultTestament::generate_canonical(binding, principal, cohort)?;
    let record = NativeResultTestament {
        testament,
        publications,
        captured_at,
        generated_revision: binding.revision,
        generated_at,
        posted_at: None,
    };
    record.check()?;
    Ok(record)
}

fn stage(
    row: OwnedResultTestament,
    before: Option<Binding>,
    extras: &mut Extras,
) -> Result<(), NativeError> {
    let record = row.get().ok_or(ContractError::MissingEvidence)?;
    let testament = record.testament();
    let after = testament.binding();
    let fact = NativeFact::ResultTestament {
        claim: testament.claim(),
        before,
        after,
        state: testament.state(),
    };
    let heap = row.heap_charge()?;
    extras.push(Extra {
        key: Key::ResultTestament(TestamentId(after.object.0)),
        row: Row::ResultTestament(row),
        heap,
        fact: Some(fact),
    })
}

#[allow(clippy::too_many_arguments)] // Bounded internal owner transaction context.
pub(super) fn prepare(
    command: NativeCommand,
    context: NativeContext,
    sequence: SessionSeq,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let position = PublicationPosition {
        sequence,
        ordinal: 0,
    };
    match command {
        NativeCommand::GenerateResultTestament { claim, id } => {
            let parent_id = ClaimId(claim.object.0);
            let parent = view.claim(parent_id).ok_or(ContractError::InvalidTarget)?;
            parent.binding().check(&claim)?;
            context.principal.require_actor(parent.issuer())?;
            if id.is_zero()
                || view.get(Key::ResultTestament(id)).is_some()
                || view.get(Key::Response(id)).is_some()
                || view.get(Key::ClaimResultTestament(parent_id)).is_some()
            {
                return Err(ContractError::InvalidTarget.into());
            }
            transactions::increment(
                &mut meta.result_testaments,
                1,
                limits.claims,
                "result testaments",
            )?;
            scratch.charge(OwnedResultTestament::container_charge())?;
            let audit = super::audit::build(view, parent_id, limits, scratch)?;
            let record = generate(audit, id, context.principal, position)?;
            stage(OwnedResultTestament::new(record)?, None, extras)?;
            extras.push(Extra {
                key: Key::ClaimResultTestament(parent_id),
                row: Row::ClaimResultTestament(id),
                heap: 0,
                fact: None,
            })?;
        }
        NativeCommand::PostResultTestament { expected } => {
            let id = TestamentId(expected.object.0);
            let source = as_result_testament(view.get(Key::ResultTestament(id)))
                .ok_or(ContractError::InvalidTarget)?;
            let testament = source.testament();
            testament.binding().check(&expected)?;
            context.principal.require_actor(testament.issuer())?;
            if testament.state() != ResultTestamentState::Generated
                || index(view.get(Key::ClaimResultTestament(testament.claim()))) != Some(id)
                || view.get(Key::Response(id)).is_some()
            {
                return Err(ContractError::InvalidTransition.into());
            }
            // Posting uses the frozen object, independent of a later parent
            // revision or responsibility change. Its original witnesses stay.
            let visits = add(
                testament.members().len(),
                add(testament.results().len(), source.publications.len())?,
            )?;
            if visits > limits.plan_edges {
                return Err(NativeError::Capacity("audit copy visits"));
            }
            let charge = add(
                OwnedResultTestament::container_charge(),
                source.heap_charge()?,
            )?;
            scratch.charge(charge)?;
            let mut record = source.try_copy()?;
            record.testament.post(context.principal, &expected)?;
            record.posted_at = Some(position);
            let row = OwnedResultTestament::new(record)?;
            if row.heap_charge()? > charge {
                return Err(MemoryError::AllocationFailed.into());
            }
            stage(row, Some(expected), extras)?;
        }
        _ => return Err(ContractError::InvalidTransition.into()),
    }
    Ok(transactions::Plan {
        rows: Vec::new(),
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    })
}
