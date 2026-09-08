//! Complete descriptor bodies for the dormant native input format. Derived
//! hashes/stamps must be recomputed by semantic decoding; external references
//! remain pins. Structural inspection alone supplies no content identity proof.
//! This module does not admit requests, resolve authority or serialize native rows.
use super::bytes::{
    Error, Sink, write_count as count, write_raw as raw, write_text as text, write_u8, write_u16,
    write_u32, write_u64,
};
use super::types;
use focal_model::lifecycle::{
    aggregation::SlotPolicy,
    artifact_descriptor::ArtifactDescriptor,
    claim_descriptor::ClaimDescriptor,
    creation, graph,
    succession::CorrectionKind,
    validation::{Declaration, PhasePolicyView, ProgramView, TargetDeclaration},
    validation_descriptor::ValidationDescriptor,
};
use focal_model::{
    ActionType, Cause, Deadline, LedgerId, ObjectKind, ObjectRef, RelationKind, RelationTarget,
    ScopeKind, ValidationKind, ValidationPhase,
};

#[path = "descriptors_artifact.rs"]
mod artifact_body;

#[cfg(test)]
#[path = "descriptors_validation_tests.rs"]
mod validation_tests;

#[cfg(test)]
#[path = "descriptors_claim_tests.rs"]
mod claim_tests;

/// Debit every iterator advance, including its terminal probe, before walking
/// a retained slice or exact-size view. Primitive field writes debit separately.
fn iterations(sink: &mut impl Sink, count: usize) -> Result<(), Error> {
    sink.visit(count.checked_add(1).ok_or(Error::Capacity)?)
}

fn ledger(sink: &mut impl Sink, value: LedgerId) -> Result<(), Error> {
    raw(sink, &value.tenant.0)?;
    raw(sink, &value.session.0)
}

fn optional_deadline(sink: &mut impl Sink, value: Option<Deadline>) -> Result<(), Error> {
    match value {
        None => write_u8(sink, 0),
        Some(deadline) => {
            write_u8(sink, 1)?;
            types::deadline(sink, deadline)
        }
    }
}

fn object_ref(sink: &mut impl Sink, value: ObjectRef) -> Result<(), Error> {
    ledger(sink, value.ledger)?;
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

fn action(sink: &mut impl Sink, value: ActionType) -> Result<(), Error> {
    write_u16(
        sink,
        match value {
            ActionType::Work => 1,
            ActionType::Consultation => 2,
            ActionType::Challenge => 3,
            ActionType::Feedback => 4,
            ActionType::Approval => 5,
            ActionType::Summon => 6,
            ActionType::Handoff => 7,
            ActionType::Evaluation => 8,
            ActionType::Correction => 9,
            ActionType::Teardown => 10,
        },
    )
}

fn relation_kind(sink: &mut impl Sink, value: RelationKind) -> Result<(), Error> {
    write_u16(
        sink,
        match value {
            RelationKind::Issuer => 1,
            RelationKind::Subject => 2,
            RelationKind::Evaluator => 3,
            RelationKind::ClaimAction => 4,
            RelationKind::Supersedes => 5,
            RelationKind::DependsOn => 6,
            RelationKind::Awaits => 7,
            RelationKind::CausedBy => 8,
            RelationKind::Refines => 9,
            RelationKind::ConflictsWith => 10,
            RelationKind::DerivedFrom => 11,
            RelationKind::Reviews => 12,
            RelationKind::Amends => 13,
            RelationKind::ContributedBy => 14,
            RelationKind::Invalidates => 15,
        },
    )
}

fn relation_target(sink: &mut impl Sink, value: &RelationTarget) -> Result<(), Error> {
    match value {
        RelationTarget::Participant(id) => {
            write_u8(sink, 0)?;
            raw(sink, &id.0)
        }
        RelationTarget::Object(value) => {
            write_u8(sink, 1)?;
            object_ref(sink, *value)
        }
        RelationTarget::Action(value) => {
            write_u8(sink, 2)?;
            action(sink, *value)
        }
        RelationTarget::Root(id) => {
            write_u8(sink, 3)?;
            raw(sink, &id.0)
        }
    }
}

fn scope_kind(sink: &mut impl Sink, value: ScopeKind) -> Result<(), Error> {
    write_u16(
        sink,
        match value {
            ScopeKind::File => 1,
            ScopeKind::Symbol => 2,
            ScopeKind::Api => 3,
            ScopeKind::TestSurface => 4,
            ScopeKind::Component => 5,
            ScopeKind::UxSurface => 6,
        },
    )
}

fn slots<'a>(
    sink: &mut impl Sink,
    values: impl ExactSizeIterator<Item = SlotPolicy<'a>>,
) -> Result<(), Error> {
    count(sink, values.len())?;
    iterations(sink, values.len())?;
    for slot in values {
        write_u32(sink, slot.slot)?;
        write_u32(sink, slot.missing_declaration_index)?;
        types::mode(sink, slot.mode)?;
        count(sink, slot.checks.len())?;
        iterations(sink, slot.checks.len())?;
        for check in slot.checks {
            write_u32(sink, check.declaration_index)?;
            raw(sink, &check.validation.0)?;
            types::mode(sink, check.mode)?;
        }
    }
    Ok(())
}

/// ledger, ID, schema, occurrence, instruction, relations, work scopes,
/// requirement pins, output slots, optional authored deadline.
pub(in crate::native) fn claim(sink: &mut impl Sink, value: &ClaimDescriptor) -> Result<(), Error> {
    ledger(sink, value.ledger())?;
    raw(sink, &value.id().0)?;
    write_u16(sink, value.schema())?;
    raw(sink, &value.occurrence().0)?;
    text(sink, value.description())?;
    count(sink, value.relations().len())?;
    iterations(sink, value.relations().len())?;
    for relation in value.relations() {
        relation_kind(sink, relation.kind)?;
        relation_target(sink, &relation.target)?;
    }
    let scopes = value.scopes();
    count(sink, scopes.len())?;
    iterations(sink, scopes.len())?;
    for scope in scopes {
        scope_kind(sink, scope.kind)?;
        text(sink, scope.key)?;
    }
    count(sink, value.requirements().len())?;
    iterations(sink, value.requirements().len())?;
    for requirement in value.requirements() {
        raw(sink, &requirement.id.0)?;
        raw(sink, &requirement.specification.0)?;
    }
    slots(sink, value.slots())?;
    optional_deadline(sink, value.deadline())
}

fn kind(sink: &mut impl Sink, value: ValidationKind) -> Result<(), Error> {
    write_u16(
        sink,
        match value {
            ValidationKind::Receipt => 1,
            ValidationKind::Test => 2,
            ValidationKind::Inspection => 3,
            ValidationKind::Integration => 4,
            ValidationKind::Contract => 5,
            ValidationKind::Design => 6,
            ValidationKind::Regression => 7,
        },
    )
}

fn declared_phase(sink: &mut impl Sink, value: ValidationPhase) -> Result<(), Error> {
    write_u16(
        sink,
        match value {
            ValidationPhase::Admission => 1,
            ValidationPhase::Increment => 2,
            ValidationPhase::WholeWork => 3,
        },
    )
}

fn target_declaration(sink: &mut impl Sink, value: TargetDeclaration<'_>) -> Result<(), Error> {
    match value {
        TargetDeclaration::WholeWorkSlot { index, name } => {
            write_u8(sink, 0)?;
            write_u32(sink, index)?;
            text(sink, name)
        }
        TargetDeclaration::Delivery => write_u8(sink, 1),
        TargetDeclaration::Admission => write_u8(sink, 2),
        TargetDeclaration::Increment => write_u8(sink, 3),
    }
}

fn phase_policy(sink: &mut impl Sink, policy: PhasePolicyView<'_>) -> Result<(), Error> {
    raw(sink, &policy.evaluator().0)?;
    raw(sink, &policy.definition().0)?;
    match policy.required_policy() {
        None => write_u8(sink, 0)?,
        Some(policy) => {
            write_u8(sink, 1)?;
            raw(sink, &policy.0)?;
        }
    }
    let handlers = policy.handlers();
    count(sink, handlers.len())?;
    iterations(sink, handlers.len())?;
    for step in handlers {
        raw(sink, &step.handler.id.0)?;
        raw(sink, &step.handler.version.0)?;
        write_u8(sink, u8::from(step.handler.agentic))?;
        write_u32(sink, step.attempts)?;
        raw(sink, &step.proof_schema.0)?;
        raw(sink, &step.diagnostic_schema.0)?;
    }
    Ok(())
}

fn program(sink: &mut impl Sink, value: ProgramView<'_>) -> Result<(), Error> {
    match value {
        ProgramView::Delivery => write_u8(sink, 0),
        ProgramView::Programmatic { check, quality } => {
            write_u8(sink, 1)?;
            phase_policy(sink, check)?;
            match quality {
                None => write_u8(sink, 0),
                Some(quality) => {
                    write_u8(sink, 1)?;
                    phase_policy(sink, quality)
                }
            }
        }
        ProgramView::Agentic { check } => {
            write_u8(sink, 2)?;
            phase_policy(sink, check)
        }
    }
}

fn declaration_fields(sink: &mut impl Sink, value: &Declaration) -> Result<(), Error> {
    raw(sink, &value.claim().0)?;
    raw(sink, &value.issuer().0)?;
    write_u32(sink, value.declaration_index())?;
    kind(sink, value.kind())?;
    declared_phase(sink, value.declared_phase())?;
    types::mode(sink, value.mode())?;
    target_declaration(sink, value.target())?;
    program(sink, value.program())?;
    types::deadline(sink, value.deadline())
}

/// Complete legacy declaration binding followed by its actual authored policy.
pub(in crate::native) fn declaration(
    sink: &mut impl Sink,
    value: &Declaration,
) -> Result<(), Error> {
    types::binding(sink, value.binding())?;
    declaration_fields(sink, value)
}

/// ledger, ID, schema, actual declaration fields, instruction, quality standard,
/// contributors and policy revision. Own content/specification hashes are derived.
pub(in crate::native) fn validation(
    sink: &mut impl Sink,
    value: &ValidationDescriptor,
) -> Result<(), Error> {
    let binding = value.binding();
    ledger(sink, binding.ledger)?;
    raw(sink, &binding.object.0)?;
    write_u16(sink, value.schema())?;
    declaration_fields(sink, value.declaration())?;
    text(sink, value.description())?;
    match value.quality_bar() {
        None => write_u8(sink, 0)?,
        Some(quality) => {
            write_u8(sink, 1)?;
            text(sink, quality)?;
        }
    }
    count(sink, value.contributed_by().len())?;
    iterations(sink, value.contributed_by().len())?;
    for contributor in value.contributed_by() {
        raw(sink, &contributor.0)?;
    }
    write_u64(sink, value.policy_revision())
}

pub(in crate::native) fn artifact(
    sink: &mut impl Sink,
    value: &ArtifactDescriptor,
) -> Result<(), Error> {
    artifact_body::encode(sink, value)
}

/// Legacy projection input, never a serialized ClaimState. The outer encoder
/// checks full acceptance correspondence against the separately encoded actual
/// declarations; these slots are not an opaque policy-summary substitute.
pub(in crate::native) fn projection(
    sink: &mut impl Sink,
    value: &creation::Proposal,
) -> Result<(), Error> {
    let definition = &value.definition;
    types::binding(sink, definition.binding)?;
    raw(sink, &definition.issuer.0)?;
    raw(sink, &definition.subject.0)?;
    optional_deadline(sink, definition.deadline)?;
    write_u32(sink, definition.max_responses)?;
    count(sink, definition.graph.obligations().len())?;
    iterations(sink, definition.graph.obligations().len())?;
    for obligation in definition.graph.obligations() {
        write_u8(
            sink,
            match obligation.kind {
                graph::Kind::DependsOn => 0,
                graph::Kind::Awaits => 1,
            },
        )?;
        raw(sink, &obligation.target.0)?;
    }
    types::binding(sink, definition.lineage.binding())?;
    match definition.lineage.cause() {
        Cause::Root(id) => {
            write_u8(sink, 0)?;
            raw(sink, &id.0)?;
        }
        Cause::Claim(id) => {
            write_u8(sink, 1)?;
            raw(sink, &id.0)?;
        }
    }
    count(sink, definition.lineage.corrections().len())?;
    iterations(sink, definition.lineage.corrections().len())?;
    for correction in definition.lineage.corrections() {
        write_u8(
            sink,
            match correction.kind {
                CorrectionKind::Supersedes => 0,
                CorrectionKind::Amends => 1,
            },
        )?;
        object_ref(sink, correction.predecessor)?;
    }
    types::binding(sink, definition.acceptance.claim())?;
    raw(sink, &definition.acceptance.issuer().0)?;
    slots(sink, definition.acceptance.slots())?;
    types::scope_limits(sink, definition.scope_limits)?;
    types::owner(sink, value.owner)
}
