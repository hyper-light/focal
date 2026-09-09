//! Borrowed authored claim fields and sequential nested collections. One meter
//! covers every model callback, including repeated policy-correspondence scans.
use super::bytes::Cursor;
use super::source_bytes::{Meter, SourceCursor, Span, Values};
use super::*;
use focal_model::lifecycle::{aggregation::CheckPolicy, claim_descriptor as model};
use focal_model::{
    ActionType, ObjectId, ObjectKind, ObjectRef, OccurrenceId, Relation, RelationKind,
    RelationTarget, RequirementRef, RootCommandId, ScopeKind, SessionId, TenantId, ValidationMode,
};

fn mode(value: u8) -> Result<ValidationMode, CodecError> {
    match value {
        0 => Ok(ValidationMode::Required),
        1 => Ok(ValidationMode::Observe),
        _ => Err(CodecError::InvalidTag("validation mode")),
    }
}
fn relation(cursor: &mut SourceCursor<'_, '_>) -> Result<Relation, CodecError> {
    let kind = match cursor.u16()? {
        1 => RelationKind::Issuer,
        2 => RelationKind::Subject,
        3 => RelationKind::Evaluator,
        4 => RelationKind::ClaimAction,
        5 => RelationKind::Supersedes,
        6 => RelationKind::DependsOn,
        7 => RelationKind::Awaits,
        8 => RelationKind::CausedBy,
        9 => RelationKind::Refines,
        10 => RelationKind::ConflictsWith,
        11 => RelationKind::DerivedFrom,
        12 => RelationKind::Reviews,
        13 => RelationKind::Amends,
        14 => RelationKind::ContributedBy,
        15 => RelationKind::Invalidates,
        _ => return Err(CodecError::InvalidTag("relation kind")),
    };
    let target = match cursor.u8()? {
        0 => RelationTarget::Participant(ParticipantId(cursor.fixed()?)),
        1 => {
            let ledger = LedgerId {
                tenant: TenantId(cursor.fixed()?),
                session: SessionId(cursor.fixed()?),
            };
            let kind = match cursor.u16()? {
                1 => ObjectKind::Claim,
                2 => ObjectKind::Testament,
                3 => ObjectKind::Validation,
                4 => ObjectKind::Artifact,
                _ => return Err(CodecError::InvalidTag("object kind")),
            };
            RelationTarget::Object(ObjectRef {
                ledger,
                kind,
                id: ObjectId(cursor.fixed()?),
            })
        }
        4 => RelationTarget::Evidence(focal_model::ArtifactRef {
            id: focal_model::ArtifactId(cursor.fixed()?),
            hash: focal_model::ContentHash(cursor.fixed()?),
        }),
        2 => RelationTarget::Action(match cursor.u16()? {
            1 => ActionType::Work,
            2 => ActionType::Consultation,
            3 => ActionType::Challenge,
            4 => ActionType::Feedback,
            5 => ActionType::Approval,
            6 => ActionType::Summon,
            7 => ActionType::Handoff,
            8 => ActionType::Evaluation,
            9 => ActionType::Correction,
            10 => ActionType::Teardown,
            _ => return Err(CodecError::InvalidTag("claim action")),
        }),
        3 => RelationTarget::Root(RootCommandId(cursor.fixed()?)),
        _ => return Err(CodecError::InvalidTag("relation target")),
    };
    Ok(Relation { kind, target })
}
fn scope<'a>(cursor: &mut SourceCursor<'_, 'a>) -> Result<model::ScopeSpec<'a>, CodecError> {
    let kind = match cursor.u16()? {
        1 => ScopeKind::File,
        2 => ScopeKind::Symbol,
        3 => ScopeKind::Api,
        4 => ScopeKind::TestSurface,
        5 => ScopeKind::Component,
        6 => ScopeKind::UxSurface,
        _ => return Err(CodecError::InvalidTag("scope kind")),
    };
    Ok(model::ScopeSpec {
        kind,
        key: cursor.text(cursor.remaining())?,
    })
}
fn requirement(cursor: &mut SourceCursor<'_, '_>) -> Result<RequirementRef, CodecError> {
    Ok(RequirementRef {
        id: ValidationId(cursor.fixed()?),
        specification: ContentHash(cursor.fixed()?),
    })
}
fn check(cursor: &mut SourceCursor<'_, '_>) -> Result<CheckPolicy, CodecError> {
    Ok(CheckPolicy {
        declaration_index: cursor.u32()?,
        validation: ValidationId(cursor.fixed()?),
        mode: mode(cursor.u8()?)?,
    })
}

#[derive(Debug)]
pub(super) struct SlotView<'m, 'a> {
    fields: model::ClaimSlotFields,
    checks: Span<'a>,
    meter: &'m Meter,
}
pub(super) fn slot<'m, 'a>(
    cursor: &mut SourceCursor<'m, 'a>,
) -> Result<SlotView<'m, 'a>, CodecError> {
    let slot = cursor.u32()?;
    let missing_declaration_index = cursor.u32()?;
    let mode = mode(cursor.u8()?)?;
    let count = cursor.count(cursor.remaining())?;
    let bytes = cursor.take(count.checked_mul(21).ok_or(CodecError::Capacity)?)?;
    Ok(SlotView {
        fields: model::ClaimSlotFields {
            slot,
            missing_declaration_index,
            mode,
            checks: count,
        },
        checks: Span { bytes, count },
        meter: cursor.meter(),
    })
}
impl model::ClaimSlotSource for SlotView<'_, '_> {
    type Checks<'s>
        = Values<'s, 's, CheckPolicy>
    where
        Self: 's;
    fn fields(&self) -> model::ClaimSlotFields {
        self.fields
    }
    fn checks(&self) -> Self::Checks<'_> {
        Values::new(self.checks, self.meter, check)
    }
}

#[derive(Debug)]
pub(super) struct ClaimView<'a> {
    fields: model::ClaimFields<'a>,
    relations: Span<'a>,
    scopes: Span<'a>,
    requirements: Span<'a>,
    slots: Span<'a>,
    meter: Meter,
    single_pass_visits: usize,
}

/// Delimit a variable-width collection using the same scalar decoder used by
/// model callbacks. Both cursor consumption and semantic reads are metered.
fn span<'a, T>(
    cursor: &mut Cursor<'a>,
    meter: &Meter,
    read: fn(&mut SourceCursor<'_, 'a>) -> Result<T, CodecError>,
) -> Result<Span<'a>, CodecError> {
    let count = cursor.count(cursor.remaining())?;
    let bytes = cursor.unread();
    let mut source = SourceCursor::new(bytes, meter)?;
    for _ in 0..count {
        read(&mut source)?;
    }
    let bytes = cursor.take(source.offset())?;
    Ok(Span { bytes, count })
}

impl<'a> ClaimView<'a> {
    /// This is a local descriptor body, without request framing or authority.
    /// Parsing and every later model-source traversal have distinct allowances.
    pub(super) fn read(
        cursor: &mut Cursor<'a>,
        max_source_visits: usize,
    ) -> Result<Self, CodecError> {
        let ledger = fixed::ledger(cursor)?;
        let id = ClaimId(cursor.fixed()?);
        let schema = cursor.u16()?;
        let occurrence = OccurrenceId(cursor.fixed()?);
        let description = cursor.text(cursor.remaining())?;
        let meter = Meter::new(max_source_visits);
        let relations = span(cursor, &meter, relation)?;
        let scopes = span(cursor, &meter, scope)?;
        let requirements = Span::read(cursor, 48)?;
        let slot_count = cursor.count(cursor.remaining())?;
        let mut nested_checks = 0usize;
        let mut source = SourceCursor::new(cursor.unread(), &meter)?;
        for _ in 0..slot_count {
            let value = slot(&mut source)?;
            nested_checks = nested_checks
                .checked_add(value.checks.count)
                .ok_or(CodecError::Capacity)?;
        }
        let slots = Span {
            count: slot_count,
            bytes: cursor.take(source.offset())?,
        };
        let deadline = match cursor.u8()? {
            0 => None,
            1 => Some(fixed::deadline(cursor)?),
            _ => return Err(CodecError::InvalidTag("option")),
        };
        // Schema 2 appends the optional follow-up policy; schema 1 has none.
        let policy = if schema >= 2 {
            match cursor.u8()? {
                0 => None,
                1 => {
                    let corrective_allowed = match cursor.u8()? {
                        0 => false,
                        1 => true,
                        _ => return Err(CodecError::InvalidTag("policy flag")),
                    };
                    let max_follow_ups = cursor.u16()?;
                    let single_issuer = match cursor.u8()? {
                        0 => false,
                        1 => true,
                        _ => return Err(CodecError::InvalidTag("policy flag")),
                    };
                    let escalation = match cursor.u8()? {
                        0 => focal_model::Escalation::None,
                        1 => focal_model::Escalation::Holder,
                        2 => focal_model::Escalation::Evaluator,
                        _ => return Err(CodecError::InvalidTag("policy escalation")),
                    };
                    Some(focal_model::PeerPolicy {
                        corrective_allowed,
                        max_follow_ups,
                        single_issuer,
                        escalation,
                    })
                }
                _ => return Err(CodecError::InvalidTag("option")),
            }
        } else {
            None
        };
        let parsed = max_source_visits
            .checked_sub(meter.remaining())
            .ok_or(CodecError::Capacity)?;
        let single_pass_visits = requirements
            .count
            .checked_mul(50)
            .and_then(|n| {
                nested_checks
                    .checked_mul(24)
                    .and_then(|checks| n.checked_add(checks))
            })
            .and_then(|n| n.checked_add(parsed))
            .ok_or(CodecError::Capacity)?;
        Ok(Self {
            fields: model::ClaimFields {
                ledger,
                id,
                schema,
                occurrence,
                description,
                deadline,
                policy,
            },
            relations,
            scopes,
            requirements,
            slots,
            meter,
            single_pass_visits,
        })
    }
    pub(super) fn set_visits(&mut self, visits: usize) {
        self.meter.reset(visits);
    }
    pub(super) fn remaining_visits(&self) -> usize {
        self.meter.remaining()
    }
    pub(super) fn single_pass_visits(&self) -> usize {
        self.single_pass_visits
    }
    pub(super) fn slots_from<'m>(&self, meter: &'m Meter) -> Values<'m, 'a, SlotView<'m, 'a>> {
        Values::new(self.slots, meter, slot)
    }
    pub(super) fn requirements_from<'m>(&self, meter: &'m Meter) -> Values<'m, 'a, RequirementRef> {
        Values::new(self.requirements, meter, requirement)
    }
}
impl<'a> model::ClaimSource<'a> for ClaimView<'a> {
    type Relations<'s>
        = Values<'s, 'a, Relation>
    where
        Self: 's;
    type Scopes<'s>
        = Values<'s, 'a, model::ScopeSpec<'a>>
    where
        Self: 's;
    type Requirements<'s>
        = Values<'s, 'a, RequirementRef>
    where
        Self: 's;
    type Slot<'s>
        = SlotView<'s, 'a>
    where
        Self: 's;
    type Slots<'s>
        = Values<'s, 'a, SlotView<'s, 'a>>
    where
        Self: 's;
    fn fields(&self) -> model::ClaimFields<'a> {
        self.fields
    }
    fn relation_count(&self) -> usize {
        self.relations.count
    }
    fn scope_count(&self) -> usize {
        self.scopes.count
    }
    fn requirement_count(&self) -> usize {
        self.requirements.count
    }
    fn slot_count(&self) -> usize {
        self.slots.count
    }
    fn relations(&self) -> Self::Relations<'_> {
        Values::new(self.relations, &self.meter, relation)
    }
    fn scopes(&self) -> Self::Scopes<'_> {
        Values::new(self.scopes, &self.meter, scope)
    }
    fn requirements(&self) -> Self::Requirements<'_> {
        Values::new(self.requirements, &self.meter, requirement)
    }
    fn slots(&self) -> Self::Slots<'_> {
        Values::new(self.slots, &self.meter, slot)
    }
}
