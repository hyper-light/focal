//! Complete native claim, evaluation and claimant audit row values. Collection
//! order is the actual retained order. No allocation, sorting, authority replay
//! or full-ledger lookup occurs here. Counts are u32 and refuse overflow.
//!
//! Private definition/acceptance/report stamps are reconstructed from the
//! encoded immutable bodies and exact retained references during hydration;
//! they are not supplied as substitutes for those bodies. Root validation must
//! independently prove all reference membership and publication coordinates.
use super::{bytes, lifecycle_fields as fields, types};
use crate::native::{OwnedClaim, OwnedEvaluation, OwnedResultTestament};
use bytes::{
    Error, Sink, write_count as count, write_raw as raw, write_u8, write_u16, write_u32, write_u64,
};
use focal_model::lifecycle::scope::{RegistrySnapshotSource, ScopeSnapshotSource};
use focal_model::lifecycle::{aggregation, audit, claim, graph, scope, succession, validation};
use focal_model::{Cause, ObjectKind, ObjectRef, WaitPredicate};

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;

/// Current scalar state, immutable graph/lineage/acceptance, response history,
/// complete scope registry and complete independent evaluation registration.
pub(super) fn claim(sink: &mut impl Sink, row: &OwnedClaim) -> Result<(), Error> {
    sink.visit(1)?;
    let value = row.claim().ok_or(Error::InvalidTag("claim row"))?;
    let registrations = row
        .registrations()
        .ok_or(Error::InvalidTag("claim registrations"))?;
    let snapshot = value.snapshot_v1();
    types::binding(sink, snapshot.binding)?;
    raw(sink, &snapshot.issuer.0)?;
    raw(sink, &snapshot.subject.0)?;
    write_u64(sink, snapshot.created.0)?;
    fields::claim_origin(sink, snapshot.origin)?;
    fields::claim_status(sink, snapshot.status)?;
    fields::optional(sink, snapshot.receipt, fields::entitlement)?;
    write_u32(sink, snapshot.max_responses)?;
    fields::optional_deadline(sink, snapshot.deadline)?;
    write_u8(sink, u8::from(snapshot.local_complete))?;
    fields::optional_sequence(sink, snapshot.local_sealed_at)?;
    fields::optional(
        sink,
        snapshot.terminal_cut,
        |sink, terminal| match terminal {
            claim::ClaimTerminalSnapshotV1::Explicit(cut) => {
                write_u8(sink, 0)?;
                fields::claim_cut(sink, cut)
            }
            claim::ClaimTerminalSnapshotV1::Required(cut) => {
                write_u8(sink, 1)?;
                fields::terminal(sink, cut)
            }
            claim::ClaimTerminalSnapshotV1::Graph(cut) => {
                write_u8(sink, 2)?;
                fields::graph_terminal(sink, cut)
            }
        },
    )?;
    graph(sink, value.graph())?;
    lineage(sink, value.lineage())?;
    acceptance(sink, value.acceptance())?;
    count(sink, snapshot.responses)?;
    let mut responses = value.response_snapshots_v1();
    for _ in 0..snapshot.responses {
        sink.visit(1)?;
        let response = responses
            .next()
            .ok_or(Error::InvalidTag("claim response count"))?;
        raw(sink, &response.link.testament.0)?;
        raw(sink, &response.link.content.0)?;
        types::receipt(sink, response.link.receipt)?;
        write_u32(sink, response.link.cycle)?;
        fields::optional(sink, response.link.prior, |sink, id| raw(sink, &id.0))?;
        write_u8(sink, u8::from(response.posted))?;
        write_u8(sink, u8::from(response.received))?;
    }
    sink.visit(1)?;
    if responses.next().is_some() {
        return Err(Error::InvalidTag("claim response count"));
    }
    scopes(sink, value.scopes())?;
    registration(sink, registrations)
}

fn graph(sink: &mut impl Sink, value: &graph::Declaration) -> Result<(), Error> {
    count(sink, value.obligations().len())?;
    for obligation in value.obligations() {
        sink.visit(1)?;
        write_u8(
            sink,
            match obligation.kind {
                graph::Kind::DependsOn => 0,
                graph::Kind::Awaits => 1,
            },
        )?;
        raw(sink, &obligation.target.0)?;
    }
    sink.visit(1)
}
fn object_ref(sink: &mut impl Sink, value: ObjectRef) -> Result<(), Error> {
    types::ledger(sink, value.ledger)?;
    write_u16(
        sink,
        match value.kind {
            ObjectKind::Claim => 1,
            ObjectKind::Testament => 2,
            ObjectKind::Validation => 3,
            ObjectKind::Artifact => 4,
        },
    )?;
    raw(sink, &value.id.0)
}
fn lineage(sink: &mut impl Sink, value: &succession::Lineage) -> Result<(), Error> {
    types::binding(sink, value.binding())?;
    match value.cause() {
        Cause::Root(id) => {
            write_u8(sink, 0)?;
            raw(sink, &id.0)?;
        }
        Cause::Claim(id) => {
            write_u8(sink, 1)?;
            raw(sink, &id.0)?;
        }
    }
    count(sink, value.corrections().len())?;
    for correction in value.corrections() {
        sink.visit(1)?;
        write_u8(
            sink,
            match correction.kind {
                succession::CorrectionKind::Supersedes => 0,
                succession::CorrectionKind::Amends => 1,
            },
        )?;
        object_ref(sink, correction.predecessor)?;
    }
    sink.visit(1)
}
fn acceptance(sink: &mut impl Sink, value: &aggregation::AcceptancePolicy) -> Result<(), Error> {
    types::binding(sink, value.claim())?;
    raw(sink, &value.issuer().0)?;
    count(sink, value.slot_count())?;
    for slot in value.slots() {
        sink.visit(1)?;
        write_u32(sink, slot.slot)?;
        write_u32(sink, slot.missing_declaration_index)?;
        types::mode(sink, slot.mode)?;
        count(sink, slot.checks.len())?;
        for check in slot.checks {
            sink.visit(1)?;
            write_u32(sink, check.declaration_index)?;
            raw(sink, &check.validation.0)?;
            types::mode(sink, check.mode)?;
        }
        sink.visit(1)?;
    }
    sink.visit(1)?;
    count(sink, value.declarations().len())?;
    for declaration in value.declarations() {
        sink.visit(1)?;
        types::binding(sink, declaration.binding())?;
        write_u32(sink, declaration.index())?;
        types::mode(sink, declaration.mode())?;
        match declaration.target() {
            aggregation::ObligationTarget::Slot(slot) => {
                write_u8(sink, 0)?;
                write_u32(sink, slot)?;
            }
            aggregation::ObligationTarget::Delivery => write_u8(sink, 1)?,
            aggregation::ObligationTarget::Admission => write_u8(sink, 2)?,
            aggregation::ObligationTarget::Increment => write_u8(sink, 3)?,
        }
    }
    sink.visit(1)
}

fn predicate(sink: &mut impl Sink, value: WaitPredicate) -> Result<(), Error> {
    let (tag, id) = match value {
        WaitPredicate::Satisfied(id) => (0, id),
        WaitPredicate::Terminal(id) => (1, id),
        WaitPredicate::Released(id) => (2, id),
    };
    write_u8(sink, tag)?;
    raw(sink, &id.0)
}
fn scopes(sink: &mut impl Sink, value: &scope::Registry) -> Result<(), Error> {
    sink.visit(1)?;
    let source = value.snapshot_v1();
    let snapshot = source.fields();
    types::binding(sink, snapshot.owner)?;
    types::scope_limits(sink, snapshot.limits)?;
    fields::optional(sink, snapshot.released, fields::claim_cut)?;
    write_u64(sink, snapshot.last_cut.0)?;
    count(sink, snapshot.scopes)?;
    let mut scopes = source.scopes();
    for _ in 0..snapshot.scopes {
        sink.visit(1)?;
        let source = scopes
            .next()
            .ok_or(Error::InvalidTag("scope count"))?
            .map_err(|_| Error::InvalidTag("scope source"))?;
        let scope = ScopeSnapshotSource::fields(&source);
        raw(sink, &scope.id.0)?;
        types::deadline(sink, scope.deadline)?;
        write_u64(sink, scope.registered.0)?;
        fields::optional(sink, scope.disposition, |sink, value| match value {
            scope::MonitorDisposition::Released(cut) => {
                write_u8(sink, 0)?;
                fields::claim_cut(sink, cut)
            }
            scope::MonitorDisposition::Cancelled(cancellation) => {
                write_u8(sink, 1)?;
                fields::cancellation(sink, cancellation)
            }
        })?;
        fields::optional(sink, scope.last_rebinding, fields::rebinding)?;
        count(sink, scope.roots)?;
        let mut roots = ScopeSnapshotSource::roots(&source);
        for _ in 0..scope.roots {
            sink.visit(1)?;
            let root = roots
                .next()
                .ok_or(Error::InvalidTag("scope root count"))?
                .map_err(|_| Error::InvalidTag("scope root source"))?;
            predicate(sink, root)?;
        }
        sink.visit(1)?;
        if roots.next().is_some() {
            return Err(Error::InvalidTag("scope root count"));
        }
    }
    sink.visit(1)?;
    if scopes.next().is_some() {
        return Err(Error::InvalidTag("scope count"));
    }
    count(sink, snapshot.children)?;
    let mut children = source.children();
    for _ in 0..snapshot.children {
        sink.visit(1)?;
        let child = children
            .next()
            .ok_or(Error::InvalidTag("owned child count"))?
            .map_err(|_| Error::InvalidTag("owned child source"))?;
        types::binding(sink, child.binding)?;
        write_u64(sink, child.registered.0)?;
    }
    sink.visit(1)?;
    if children.next().is_some() {
        return Err(Error::InvalidTag("owned child count"));
    }
    Ok(())
}
fn registration(sink: &mut impl Sink, value: &aggregation::RegistrationSet) -> Result<(), Error> {
    sink.visit(1)?;
    let snapshot = value.snapshot_v1();
    types::binding(sink, snapshot.claim)?;
    count(sink, snapshot.max_rows)?;
    write_u8(sink, u8::from(snapshot.sealed))?;
    write_u8(sink, u8::from(snapshot.increments_sealed))?;
    fields::optional_sequence(sink, snapshot.sealed_at)?;
    count(sink, snapshot.rows)?;
    let mut members = value.member_snapshots_v1();
    for _ in 0..snapshot.rows {
        sink.visit(1)?;
        let member = members
            .next()
            .ok_or(Error::InvalidTag("registration count"))?;
        types::binding(sink, member.binding)?;
        types::target(sink, member.target)?;
        write_u64(sink, member.generation)?;
        types::optional_receipt(sink, member.receipt)?;
        write_u32(sink, member.declaration_index)?;
        types::mode(sink, member.mode)?;
    }
    sink.visit(1)?;
    if members.next().is_some() {
        return Err(Error::InvalidTag("registration count"));
    }
    Ok(())
}

pub(super) fn evaluation(sink: &mut impl Sink, value: &OwnedEvaluation) -> Result<(), Error> {
    sink.visit(1)?;
    let snapshot = value
        .get()
        .ok_or(Error::InvalidTag("evaluation row"))?
        .snapshot_v1()
        .map_err(|_| Error::Capacity)?;
    evaluation_snapshot(sink, snapshot)
}
fn evaluation_snapshot(
    sink: &mut impl Sink,
    value: validation::EvaluationSnapshotV1,
) -> Result<(), Error> {
    types::binding(sink, value.binding)?;
    types::target(sink, value.target)?;
    write_u64(sink, value.generation)?;
    types::optional_receipt(sink, value.receipt)?;
    fields::evaluation_state(sink, value.state)?;
    fields::phase(sink, value.phase)?;
    write_u64(sink, value.handler)?;
    write_u32(sink, value.handler_attempt)?;
    write_u32(sink, value.attempt)?;
    write_u8(sink, u8::from(value.begun))?;
    fields::optional_suppression(sink, value.suppression)?;
    fields::optional_hash(sink, value.sealed)?;
    fields::optional_fence(sink, value.fence)?;
    fields::optional_artifact(sink, value.programmatic_evidence)?;
    fields::optional(sink, value.last_result, fields::accepted_snapshot)
}

pub(super) fn result_testament(
    sink: &mut impl Sink,
    row: &OwnedResultTestament,
) -> Result<(), Error> {
    sink.visit(1)?;
    let value = row.get().ok_or(Error::InvalidTag("result testament row"))?;
    let snapshot = value
        .testament()
        .snapshot_v1()
        .map_err(|_| Error::Capacity)?;
    types::binding(sink, value.generated_binding())?;
    types::binding(sink, snapshot.binding)?;
    fields::result_testament_state(sink, snapshot.state)?;
    let cohort = value.testament().cohort();
    types::binding(sink, snapshot.cohort.claim)?;
    raw(sink, &snapshot.cohort.issuer.0)?;
    write_u64(sink, snapshot.cohort.sequence.0)?;
    // Reserved result slots survive restoration of an open-origin cohort.
    write_u32(
        sink,
        u32::try_from(snapshot.cohort.result_capacity).map_err(|_| Error::Capacity)?,
    )?;
    count(sink, cohort.members().len())?;
    for member in cohort.members() {
        sink.visit(1)?;
        audit_member(sink, member.snapshot_v1())?;
    }
    sink.visit(1)?;
    count(sink, cohort.results().len())?;
    for result in cohort.results() {
        sink.visit(1)?;
        fields::accepted_result(sink, *result)?;
    }
    sink.visit(1)?;
    write_u64(sink, value.captured_at().0)?;
    fields::position(sink, value.generated_at())?;
    fields::optional_position(sink, value.posted_at())?;
    count(sink, value.publications().len())?;
    for publication in value.publications() {
        sink.visit(1)?;
        fields::result_key(sink, publication.key)?;
        fields::position(sink, publication.position)?;
    }
    sink.visit(1)
}
fn audit_member(sink: &mut impl Sink, value: audit::AuditMemberSnapshotV1) -> Result<(), Error> {
    raw(sink, &value.key.validation.0)?;
    types::target(sink, value.key.target)?;
    write_u64(sink, value.key.generation)?;
    write_u32(sink, value.declaration_index)?;
    types::binding(sink, value.binding)?;
    types::optional_receipt(sink, value.receipt)?;
    write_u8(sink, u8::from(value.begun))?;
    fields::evaluation_state(sink, value.state)?;
    fields::optional_suppression(sink, value.suppression)?;
    fields::optional_fence(sink, value.fence)?;
    fields::optional(sink, value.last_result, fields::accepted_snapshot)?;
    fields::optional_hash(sink, value.sealed)
}
