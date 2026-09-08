//! Borrowed result-testament bodies and per-row funded restoration. The model
//! accepts slice DTOs, so build uses three explicitly quoted temporary arrays
//! for members, results and borrowed declarations. Publication storage moves
//! directly into the restored row. No ledger-wide buffer or authority replay.
use super::{
    bytes::{Cursor, Error},
    read_fields as f,
    read_source::{Meter, Span, Values},
};
use crate::native::{
    NativeAuditPublication, NativeError, NativeEvent, NativeFact, NativeResultKey,
    audit_bundle::{OwnedResultTestament, RecoveredCoordinates},
};
use focal_model::lifecycle::{
    Binding, ContractError,
    aggregation::PublicationPosition,
    audit::{self, AuditMemberSnapshotV1, ResultTestament, ResultTestamentState},
    claim::ClaimState,
    validation::{self, AcceptedResult, AcceptedResultSnapshotV1, Declaration},
};
use focal_model::{ClaimId, ObjectRevision, SessionSeq, ValidationId};

const ALLOCATION: usize = 4 * size_of::<usize>();

#[cfg(test)]
#[path = "read_audit_tests.rs"]
mod tests;

/// Dependencies borrow one authenticated root. Implementors debit each lookup
/// through the shared meter before traversal. Complete membership and outcome
/// closure remain final root validation obligations.
pub(super) trait AuditObjects {
    fn prefix(&self) -> SessionSeq;
    fn claim(&self, id: ClaimId, meter: &Meter) -> Result<&ClaimState, Error>;
    fn definition(&self, id: ValidationId, meter: &Meter) -> Result<&Declaration, Error>;
    fn result(
        &self,
        key: NativeResultKey,
        meter: &Meter,
    ) -> Result<(AcceptedResult, PublicationPosition), Error>;
    fn event(&self, position: PublicationPosition, meter: &Meter) -> Result<NativeEvent, Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AuditQuote {
    pub(super) workspace_bytes: usize,
    pub(super) retained_bytes: usize,
}
#[derive(Debug, Clone, Copy)]
pub(super) struct AuditBody<'a> {
    pub(super) generated: Binding,
    pub(super) snapshot: audit::ResultTestamentSnapshotV1,
    pub(super) members: Span<'a>,
    pub(super) results: Span<'a>,
    pub(super) coordinates: RecoveredCoordinates,
    pub(super) publications: Span<'a>,
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b).ok_or(Error::Capacity)
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b).ok_or(Error::Capacity)
}
fn buffer<T>(count: usize) -> Result<usize, Error> {
    add(
        mul(count, size_of::<T>())?,
        if count == 0 { 0 } else { ALLOCATION },
    )
}
fn model(error: ContractError) -> Error {
    match error {
        ContractError::Capacity => Error::Capacity,
        _ => Error::InvalidTag("audit model"),
    }
}
fn native(error: NativeError) -> Error {
    match error {
        NativeError::Contract(error) => model(error),
        NativeError::Capacity(_) => Error::Capacity,
        NativeError::Memory(_) => Error::Allocation,
        _ => Error::InvalidTag("native audit"),
    }
}
fn reserve<T>(count: usize) -> Result<Vec<T>, Error> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| Error::Allocation)?;
    // Refuse allocator growth immediately while the complete quote is held.
    if values.capacity() > count {
        return Err(Error::Allocation);
    }
    Ok(values)
}
fn push<T>(values: &mut Vec<T>, value: T) -> Result<(), Error> {
    if values.len() >= values.capacity() {
        return Err(Error::Capacity);
    }
    values.push(value);
    Ok(())
}
fn publication(c: &mut Cursor<'_>) -> Result<NativeAuditPublication, Error> {
    Ok(NativeAuditPublication {
        key: f::result_key(c)?,
        position: f::position(c)?,
    })
}
fn as_u64(value: usize) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error::Capacity)
}

impl<'a> AuditBody<'a> {
    /// Structural framing only. The enclosing row parser must call finish().
    pub(super) fn read(c: &mut Cursor<'a>) -> Result<Self, Error> {
        let generated = f::binding(c)?;
        let binding = f::binding(c)?;
        let state = f::result_testament_state(c)?;
        let claim = f::binding(c)?;
        let issuer = f::participant(c)?;
        let sequence = f::sequence(c)?;
        let result_capacity = u64::from(c.u32()?);
        let members = Span::read_with(c, f::audit_member)?;
        let results = Span::read_with(c, f::accepted_result)?;
        let coordinates = RecoveredCoordinates {
            generated,
            captured_at: f::sequence(c)?,
            generated_at: f::position(c)?,
            posted_at: f::optional_position(c)?,
        };
        let publications = Span::read_with(c, publication)?;
        Ok(Self {
            generated,
            snapshot: audit::ResultTestamentSnapshotV1 {
                binding,
                state,
                cohort: audit::AuditCohortSnapshotV1 {
                    claim,
                    issuer,
                    sequence,
                    members: as_u64(members.count)?,
                    results: as_u64(results.count)?,
                    result_capacity,
                },
            },
            members,
            results,
            coordinates,
            publications,
        })
    }
    pub(super) fn quote(&self) -> Result<AuditQuote, Error> {
        let capacity =
            usize::try_from(self.snapshot.cohort.result_capacity).map_err(|_| Error::Capacity)?;
        if self.results.count > capacity || self.publications.count != self.results.count {
            return Err(Error::InvalidTag("audit result counts"));
        }
        let workspace_bytes = add(
            buffer::<AuditMemberSnapshotV1>(self.members.count)?,
            add(
                buffer::<AcceptedResultSnapshotV1>(self.results.count)?,
                buffer::<&Declaration>(self.members.count)?,
            )?,
        )?;
        let retained_bytes = add(
            OwnedResultTestament::container_charge(),
            add(
                buffer::<audit::AuditMember>(self.members.count)?,
                add(
                    buffer::<AcceptedResult>(capacity)?,
                    buffer::<NativeAuditPublication>(self.publications.count)?,
                )?,
            )?,
        )?;
        Ok(AuditQuote {
            workspace_bytes,
            retained_bytes,
        })
    }
    /// Both quoted reservations are held before entry. Refusal drops provisional
    /// arrays/rows; dependency storage is only borrowed throughout this build.
    #[cfg(test)]
    pub(super) fn build(
        &self,
        objects: &impl AuditObjects,
        allowance: AuditQuote,
        meter: &Meter,
    ) -> Result<(OwnedResultTestament, usize), Error> {
        self.build_with_meters(objects, allowance, meter, meter)
    }
    pub(super) fn build_with_meters(
        &self,
        objects: &impl AuditObjects,
        allowance: AuditQuote,
        source: &Meter,
        meter: &Meter,
    ) -> Result<(OwnedResultTestament, usize), Error> {
        meter.charge(64)?;
        let quote = self.quote()?;
        if quote.workspace_bytes > allowance.workspace_bytes
            || quote.retained_bytes > allowance.retained_bytes
        {
            return Err(Error::Capacity);
        }
        if self.generated != self.coordinates.generated
            || self.generated.revision != ObjectRevision(1)
            || self.coordinates.captured_at < self.snapshot.cohort.sequence
            || self.coordinates.generated_at.sequence > objects.prefix()
            || self
                .coordinates
                .posted_at
                .is_some_and(|at| at.sequence > objects.prefix())
        {
            return Err(Error::InvalidTag("audit publication frame"));
        }
        generation_events(self, objects, meter)?;
        let claim = objects.claim(ClaimId(self.snapshot.cohort.claim.object.0), meter)?;
        let mut members = reserve::<AuditMemberSnapshotV1>(self.members.count)?;
        let mut declarations = reserve::<&Declaration>(self.members.count)?;
        let mut results = reserve::<AcceptedResultSnapshotV1>(self.results.count)?;
        let actual_workspace = add(
            buffer::<AuditMemberSnapshotV1>(members.capacity())?,
            add(
                buffer::<&Declaration>(declarations.capacity())?,
                buffer::<AcceptedResultSnapshotV1>(results.capacity())?,
            )?,
        )?;
        if actual_workspace > quote.workspace_bytes {
            return Err(Error::Capacity);
        }
        // Covers preparation and the second intrinsic construction pass. Debit
        // before either pass, including refusal, instead of resetting budgets.
        let mut model_visits = 64usize;
        for member in Values::new(self.members, source, f::audit_member) {
            meter.charge(16)?;
            let member = member.map_err(model)?;
            let declaration = objects.definition(member.key.validation, meter)?;
            let work = audit::AuditMember::hydration_visits(declaration, member).map_err(model)?;
            model_visits = add(model_visits, add(20, mul(2, work)?)?)?;
            push(&mut members, member)?;
            push(&mut declarations, declaration)?;
        }
        for result in Values::new(self.results, source, f::accepted_result) {
            meter.charge(16)?;
            let result = result.map_err(model)?;
            let declaration = objects.definition(result.validation, meter)?;
            let work = AcceptedResult::hydration_visits(declaration).map_err(model)?;
            model_visits = add(model_visits, add(16, mul(2, work)?)?)?;
            push(&mut results, result)?;
        }
        let capacity =
            usize::try_from(self.snapshot.cohort.result_capacity).map_err(|_| Error::Capacity)?;
        let model_bytes = add(
            size_of::<ResultTestament>(),
            add(
                buffer::<audit::AuditMember>(members.len())?,
                buffer::<AcceptedResult>(capacity)?,
            )?,
        )?;
        meter.charge(model_visits)?;
        let plan = ResultTestament::prepare_hydration_v1(
            claim,
            self.generated,
            self.snapshot,
            &members,
            &results,
            &declarations,
            audit::Limits {
                evaluations: members.len(),
                results: capacity,
            },
            model_bytes,
            model_visits,
        )
        .map_err(model)?;
        if plan.construction_charge() > model_bytes {
            return Err(Error::Capacity);
        }
        let testament = plan.build().map_err(model)?;
        let mut publications = reserve::<NativeAuditPublication>(self.publications.count)?;
        let mut previous = None;
        for publication in Values::new(self.publications, source, publication) {
            meter.charge(32)?;
            let publication = publication.map_err(model)?;
            if previous.is_some_and(|key| key >= publication.key)
                || publication.position.sequence.0 == 0
                || publication.position.sequence > self.coordinates.captured_at
            {
                return Err(Error::InvalidTag("audit publication order"));
            }
            previous = Some(publication.key);
            let (result, position) = objects.result(publication.key, meter)?;
            if NativeResultKey::of(result) != publication.key || position != publication.position {
                return Err(Error::InvalidTag("audit result publication"));
            }
            let event = objects.event(position, meter)?;
            let fact = match result.phase() {
                validation::Phase::Delivery => NativeFact::Delivery {
                    key: publication.key,
                },
                validation::Phase::MissingTarget => NativeFact::Missing {
                    key: publication.key,
                },
                validation::Phase::Programmatic | validation::Phase::Quality => {
                    NativeFact::Accepted {
                        key: publication.key,
                    }
                }
            };
            if event.sequence != position.sequence
                || event.ordinal != position.ordinal
                || event.fact != fact
            {
                return Err(Error::InvalidTag("audit result event"));
            }
            push(&mut publications, publication)?;
        }
        // Match full accepted values, never only their keys or a later cursor.
        meter.charge(add(testament.results().len(), 1)?)?;
        for result in testament.results() {
            meter.charge(32)?;
            let (actual, _) = objects.result(NativeResultKey::of(*result), meter)?;
            if actual != *result {
                return Err(Error::InvalidTag("audit result body"));
            }
        }
        let visits = OwnedResultTestament::recovery_visits(
            testament.cohort().members().len(),
            testament.results().len(),
            publications.len(),
        )
        .map_err(native)?;
        meter.charge(visits)?;
        OwnedResultTestament::recover(
            testament,
            publications,
            self.coordinates,
            quote.retained_bytes,
            visits,
        )
        .map_err(native)
    }
}

fn generation_events(
    body: &AuditBody<'_>,
    objects: &impl AuditObjects,
    meter: &Meter,
) -> Result<(), Error> {
    meter.charge(64)?;
    let claim = ClaimId(body.snapshot.cohort.claim.object.0);
    let generated = objects.event(body.coordinates.generated_at, meter)?;
    if generated.sequence != body.coordinates.generated_at.sequence
        || generated.ordinal != body.coordinates.generated_at.ordinal
        || generated.fact
            != (NativeFact::ResultTestament {
                claim,
                before: None,
                after: body.generated,
                state: ResultTestamentState::Generated,
            })
    {
        return Err(Error::InvalidTag("audit generated event"));
    }
    match (body.snapshot.state, body.coordinates.posted_at) {
        (ResultTestamentState::Generated, None) => {}
        (ResultTestamentState::Posted, Some(position)) => {
            let posted = objects.event(position, meter)?;
            if posted.sequence != position.sequence
                || posted.ordinal != position.ordinal
                || posted.fact
                    != (NativeFact::ResultTestament {
                        claim,
                        before: Some(body.generated),
                        after: body.snapshot.binding,
                        state: ResultTestamentState::Posted,
                    })
            {
                return Err(Error::InvalidTag("audit posted event"));
            }
        }
        _ => return Err(Error::InvalidTag("audit publication state")),
    }
    Ok(())
}
