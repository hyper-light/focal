//! Complete claim row inputs. Encoded summaries identify actual retained
//! declarations, responses and evaluation rows; they never mint model stamps.
use super::{
    bytes::{Cursor, Error},
    read_fields as fields, read_scopes,
    read_source::{Meter, Span, Values, model_error},
};
use crate::native::EvaluationKey;
use focal_model::lifecycle::{
    Binding, ContractError, aggregation, claim, evidence, graph, succession, validation,
};
use focal_model::{
    Cause, ClaimId, ObjectId, ObjectKind, ObjectRef, ParticipantId, RootCommandId, TestamentId,
    ValidationId,
};

#[path = "read_claim_build.rs"]
mod build;
pub(super) use build::policy_shape;
#[path = "read_claim_sources.rs"]
mod sources;
pub(super) use build::Limits;
pub(super) use sources::Acceptance;

pub(super) trait Objects {
    fn declaration(&self, id: ValidationId) -> Result<&validation::Declaration, ContractError>;
    fn response(&self, id: TestamentId) -> Result<&evidence::Response, ContractError>;
    fn evaluation(&self, key: EvaluationKey)
    -> Result<&validation::EvaluationState, ContractError>;
}

#[derive(Clone, Copy)]
pub(super) struct Declared {
    pub(super) binding: Binding,
    pub(super) index: u32,
    pub(super) mode: focal_model::ValidationMode,
    pub(super) target: validation::CheckedDeclarationTarget,
}
#[derive(Clone, Copy)]
pub(super) struct Slot<'a> {
    pub(super) fields: focal_model::lifecycle::claim_descriptor::ClaimSlotFields,
    pub(super) checks: Span<'a>,
}
pub(super) struct Input<'a> {
    pub(super) fields: claim::ClaimSnapshotV1,
    pub(super) obligations: Span<'a>,
    pub(super) lineage: Binding,
    pub(super) cause: Cause,
    pub(super) corrections: Span<'a>,
    pub(super) acceptance: Binding,
    pub(super) acceptance_issuer: ParticipantId,
    pub(super) slots: Span<'a>,
    pub(super) declarations: Span<'a>,
    pub(super) responses: Span<'a>,
    pub(super) scopes: read_scopes::Registry<'a>,
    pub(super) registrations: aggregation::RegistrationSnapshotV1,
    pub(super) members: Span<'a>,
}
impl<'a> Input<'a> {
    pub(super) fn read(c: &mut Cursor<'a>) -> Result<Self, Error> {
        let mut header = claim::ClaimSnapshotV1 {
            binding: fields::binding(c)?,
            issuer: fields::participant(c)?,
            subject: fields::participant(c)?,
            created: fields::sequence(c)?,
            origin: fields::claim_origin(c)?,
            status: fields::claim_status(c)?,
            receipt: fields::optional(c, fields::entitlement)?,
            max_responses: c.u32()?,
            deadline: fields::optional_deadline(c)?,
            local_complete: fields::boolean(c)?,
            local_sealed_at: fields::optional_sequence(c)?,
            terminal_cut: fields::optional(c, fields::claim_terminal)?,
            responses: 0,
        };
        let obligations = Span::read_fixed(c, 17)?;
        let lineage = fields::binding(c)?;
        let cause = match c.u8()? {
            0 => Cause::Root(RootCommandId(c.fixed()?)),
            1 => Cause::Claim(ClaimId(c.fixed()?)),
            _ => return Err(Error::InvalidTag("claim cause")),
        };
        let corrections = Span::read_fixed(c, 51)?;
        let acceptance = fields::binding(c)?;
        let acceptance_issuer = fields::participant(c)?;
        let slots = Span::read_with(c, slot)?;
        let declarations = Span::read_with(c, declaration)?;
        let responses = Span::read_with(c, fields::claim_response)?;
        header.responses = responses.count;
        let scopes = read_scopes::Registry::read(c)?;
        let registration_claim = fields::binding(c)?;
        let max_rows = c.count(usize::MAX)?;
        let sealed = fields::boolean(c)?;
        let increments_sealed = fields::boolean(c)?;
        let sealed_at = fields::optional_sequence(c)?;
        let members = Span::read_with(c, fields::registration_member)?;
        Ok(Self {
            fields: header,
            obligations,
            lineage,
            cause,
            corrections,
            acceptance,
            acceptance_issuer,
            slots,
            declarations,
            responses,
            scopes,
            registrations: aggregation::RegistrationSnapshotV1 {
                claim: registration_claim,
                rows: members.count,
                max_rows,
                sealed,
                increments_sealed,
                sealed_at,
            },
            members,
        })
    }
    pub(super) fn acceptance_source<'m, D: Objects + ?Sized>(
        &self,
        objects: &'m D,
        meter: &'m Meter,
    ) -> Acceptance<'m, 'a, D> {
        Acceptance {
            slots: self.slots,
            declarations: self.declarations,
            objects,
            meter,
        }
    }
    pub(super) fn response_source<'m, D: Objects + ?Sized>(
        &self,
        objects: &'m D,
        meter: &'m Meter,
    ) -> sources::Responses<'m, 'a, D> {
        sources::Responses {
            span: self.responses,
            objects,
            meter,
        }
    }
    pub(super) fn registration_source<'m, D: Objects + ?Sized>(
        &self,
        objects: &'m D,
        meter: &'m Meter,
    ) -> sources::Registrations<'m, 'a, D> {
        sources::Registrations {
            claim: ClaimId(self.fields.binding.object.0),
            span: self.members,
            objects,
            meter,
        }
    }
}

pub(super) fn obligation(c: &mut Cursor<'_>) -> Result<graph::Obligation, Error> {
    let kind = match c.u8()? {
        0 => graph::Kind::DependsOn,
        1 => graph::Kind::Awaits,
        _ => return Err(Error::InvalidTag("dependency kind")),
    };
    Ok(graph::Obligation {
        kind,
        target: ClaimId(c.fixed()?),
    })
}
pub(super) fn correction(c: &mut Cursor<'_>) -> Result<succession::Correction, Error> {
    let kind = match c.u8()? {
        0 => succession::CorrectionKind::Supersedes,
        1 => succession::CorrectionKind::Amends,
        _ => return Err(Error::InvalidTag("correction kind")),
    };
    let ledger = fields::ledger(c)?;
    let object_kind = match c.u16()? {
        1 => ObjectKind::Claim,
        2 => ObjectKind::Testament,
        3 => ObjectKind::Validation,
        4 => ObjectKind::Artifact,
        _ => return Err(Error::InvalidTag("object kind")),
    };
    Ok(succession::Correction {
        kind,
        predecessor: ObjectRef {
            ledger,
            kind: object_kind,
            id: ObjectId(c.fixed()?),
        },
    })
}
fn check(c: &mut Cursor<'_>) -> Result<aggregation::CheckPolicy, Error> {
    Ok(aggregation::CheckPolicy {
        declaration_index: c.u32()?,
        validation: ValidationId(c.fixed()?),
        mode: fields::mode(c)?,
    })
}
fn slot<'a>(c: &mut Cursor<'a>) -> Result<Slot<'a>, Error> {
    let slot = c.u32()?;
    let missing_declaration_index = c.u32()?;
    let mode = fields::mode(c)?;
    let checks = Span::read_fixed(c, 21)?;
    Ok(Slot {
        fields: focal_model::lifecycle::claim_descriptor::ClaimSlotFields {
            slot,
            missing_declaration_index,
            mode,
            checks: checks.count,
        },
        checks,
    })
}
fn declaration(c: &mut Cursor<'_>) -> Result<Declared, Error> {
    let binding = fields::binding(c)?;
    let index = c.u32()?;
    let mode = fields::mode(c)?;
    let target = match c.u8()? {
        0 => validation::CheckedDeclarationTarget::Slot(c.u32()?),
        1 => validation::CheckedDeclarationTarget::Delivery,
        2 => validation::CheckedDeclarationTarget::Admission,
        3 => validation::CheckedDeclarationTarget::Increment,
        _ => return Err(Error::InvalidTag("acceptance target")),
    };
    Ok(Declared {
        binding,
        index,
        mode,
        target,
    })
}
