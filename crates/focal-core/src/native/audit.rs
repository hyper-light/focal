//! An audit is derived from one actual sealed owner prefix. It does not retain
//! a second mutable evaluation index or invent results for suppressed members.
use super::prepare::{add, array};
use super::*;
use focal_memory::{BudgetKind, BudgetLane};
use focal_model::lifecycle::aggregation::PublicationPosition;
use focal_model::lifecycle::audit::{self as model, AuditCohort};

#[path = "audit_history.rs"]
mod history;

#[cfg(test)]
#[path = "audit_tests.rs"]
mod tests;

/// Original publication identity, independent of canonical bundle ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeAuditPublication {
    pub key: NativeResultKey,
    pub position: PublicationPosition,
}

/// Borrowed through a funded callback. An incomplete cohort remains inspectable
/// while already-begun peers finish; inspection never closes or posts testimony.
#[derive(Debug)]
pub struct NativeAudit {
    cohort: AuditCohort,
    publications: Vec<NativeAuditPublication>,
    captured_at: SessionSeq,
    visits_left: usize,
}

impl NativeAudit {
    pub fn cohort(&self) -> &AuditCohort {
        &self.cohort
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
    pub(super) fn into_parts(
        self,
    ) -> (AuditCohort, Vec<NativeAuditPublication>, SessionSeq, usize) {
        (
            self.cohort,
            self.publications,
            self.captured_at,
            self.visits_left,
        )
    }
}

struct Visits(usize);
impl Visits {
    fn take(&mut self, count: usize) -> Result<(), NativeError> {
        self.0 = self
            .0
            .checked_sub(count)
            .ok_or(NativeError::Capacity("audit visits"))?;
        Ok(())
    }
}

fn reserve<T>(count: usize) -> Result<Vec<T>, NativeError> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    if rows.capacity() > count {
        return Err(MemoryError::AllocationFailed.into());
    }
    Ok(rows)
}

fn push<T>(rows: &mut Vec<T>, value: T) -> Result<(), NativeError> {
    if rows.len() == rows.capacity() {
        return Err(NativeError::Capacity("audit source rows"));
    }
    rows.push(value);
    Ok(())
}

pub(super) fn with_audit<T>(
    view: &View<'_>,
    claim: ClaimId,
    limits: NativeLimits,
    project: impl FnOnce(&NativeAudit) -> T,
) -> Result<T, NativeError> {
    construct(
        view,
        claim,
        limits,
        |bytes| {
            Ok(view
                .state
                .budget
                .reserve(BudgetKind::Query, BudgetLane::Ordinary, bytes)?)
        },
        |audit| Ok(project(&audit)),
    )
}

/// The transaction already owns its complete construction allowance. Move the
/// source-derived buffers into its new row without borrowing temporary Query
/// permits or copying the complete audit a second time.
pub(super) fn build(
    view: &View<'_>,
    claim: ClaimId,
    mut limits: NativeLimits,
    scratch: &mut super::prepare::Scratch,
) -> Result<NativeAudit, NativeError> {
    limits.preparation_bytes = scratch.remaining()?;
    construct(view, claim, limits, |bytes| scratch.charge(bytes), Ok)
}

fn construct<T, Permit>(
    view: &View<'_>,
    claim: ClaimId,
    limits: NativeLimits,
    mut charge: impl FnMut(usize) -> Result<Permit, NativeError>,
    project: impl FnOnce(NativeAudit) -> Result<T, NativeError>,
) -> Result<T, NativeError> {
    let mut visits = Visits(limits.plan_edges);
    visits.take(2)?;
    let parent = view.claim(claim).ok_or(ContractError::InvalidTarget)?;
    let registry = view
        .owned_claim(claim)?
        .registrations()
        .ok_or(ContractError::InvalidPolicy)?;
    if registry.rows().len() > limits.evaluations_per_claim {
        return Err(NativeError::Capacity("audit evaluations"));
    }
    // Account for the capability's complete policy check before traversing it.
    visits.take(add(
        parent
            .acceptance()
            .declarations()
            .len()
            .checked_mul(2)
            .ok_or(ContractError::Capacity)?,
        add(parent.acceptance().slot_count(), 1)?,
    )?)?;
    let targets = registry.audit_targets(parent)?;
    let counts = history::scan(
        view,
        claim,
        registry,
        limits,
        &mut visits,
        |_| Ok(()),
        |_, _| Ok(()),
    )?;
    let bytes = add(
        array::<validation::Evaluation<'_>>(counts.evaluations)?,
        add(
            array::<validation::AcceptedResult>(counts.results)?,
            array::<NativeAuditPublication>(counts.results)?,
        )?,
    )?;
    let remaining_bytes = limits
        .preparation_bytes
        .checked_sub(bytes)
        .ok_or(NativeError::Capacity("audit construction bytes"))?;
    // Each debit precedes the buffers it owns, so both ordinary errors and a
    // callback unwind destroy payloads before returning their capacity.
    let reservation = charge(bytes)?;
    let mut evaluations = reserve(counts.evaluations)?;
    let mut results = reserve(counts.results)?;
    let mut publications = reserve(counts.results)?;
    let copied = history::scan(
        view,
        claim,
        registry,
        limits,
        &mut visits,
        |evaluation| push(&mut evaluations, evaluation),
        |result, position| {
            push(&mut results, result)?;
            push(
                &mut publications,
                NativeAuditPublication {
                    key: NativeResultKey::of(result),
                    position,
                },
            )
        },
    )?;
    if copied != counts {
        return Err(ContractError::InvalidManifest.into());
    }
    let plan = AuditCohort::prepare_native(
        targets,
        &evaluations,
        &results,
        model::Limits {
            evaluations: limits.evaluations_per_claim,
            results: limits.results,
        },
        remaining_bytes,
        visits.0,
    )?;
    visits.take(plan.visits())?;
    let cohort_reservation = charge(plan.construction_charge())?;
    let cohort = plan.build()?;
    history::sort(&mut publications, &mut visits)?;
    let audit = NativeAudit {
        cohort,
        publications,
        captured_at: view.prefix(),
        visits_left: visits.0,
    };
    let result = project(audit)?;
    drop(cohort_reservation);
    drop(results);
    drop(evaluations);
    drop(reservation);
    Ok(result)
}

impl Core<NativeState> {
    pub fn with_native_audit<T>(
        &self,
        claim: ClaimId,
        project: impl FnOnce(&NativeAudit) -> T,
    ) -> Result<T, NativeError> {
        with_audit(
            &View {
                state: &self.state,
                tail: None,
            },
            claim,
            self.limits,
            project,
        )
    }
}
