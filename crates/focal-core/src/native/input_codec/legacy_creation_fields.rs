use super::*;
use focal_model::lifecycle::claim_descriptor::ClaimSlotSource;
use focal_model::{RequestEpoch, RequestId};

pub(super) fn obligation(
    cursor: &mut SourceCursor<'_, '_>,
) -> Result<graph::Obligation, CodecError> {
    let kind = match cursor.u8()? {
        0 => graph::Kind::DependsOn,
        1 => graph::Kind::Awaits,
        _ => return Err(CodecError::InvalidTag("dependency kind")),
    };
    Ok(graph::Obligation {
        kind,
        target: ClaimId(cursor.fixed()?),
    })
}
pub(super) fn correction(
    cursor: &mut SourceCursor<'_, '_>,
) -> Result<succession::Correction, CodecError> {
    let kind = match cursor.u8()? {
        0 => succession::CorrectionKind::Supersedes,
        1 => succession::CorrectionKind::Amends,
        _ => return Err(CodecError::InvalidTag("correction kind")),
    };
    let ledger = LedgerId {
        tenant: focal_model::TenantId(cursor.fixed()?),
        session: focal_model::SessionId(cursor.fixed()?),
    };
    let object_kind = match cursor.u16()? {
        1 => ObjectKind::Claim,
        2 => ObjectKind::Testament,
        3 => ObjectKind::Validation,
        4 => ObjectKind::Artifact,
        _ => return Err(CodecError::InvalidTag("object kind")),
    };
    Ok(succession::Correction {
        kind,
        predecessor: ObjectRef {
            ledger,
            kind: object_kind,
            id: ObjectId(cursor.fixed()?),
        },
    })
}

pub(super) struct Projection<'a> {
    pub(super) binding: Binding,
    pub(super) issuer: ParticipantId,
    pub(super) subject: ParticipantId,
    pub(super) deadline: Option<Deadline>,
    pub(super) max_responses: u32,
    pub(super) obligations: Span<'a>,
    pub(super) lineage: Binding,
    pub(super) cause: Cause,
    pub(super) corrections: Span<'a>,
    pub(super) acceptance: Binding,
    pub(super) acceptance_issuer: ParticipantId,
    pub(super) slots: Span<'a>,
    pub(super) scope_limits: scope::ScopeLimits,
    pub(super) owner: Option<creation::Owner>,
    pub(super) checks: usize,
}
fn optional(cursor: &mut Cursor<'_>) -> Result<bool, CodecError> {
    match cursor.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CodecError::InvalidTag("option")),
    }
}
impl<'a> Projection<'a> {
    pub(super) fn read(cursor: &mut Cursor<'a>, work: &Work) -> Result<Self, DecodeError> {
        let binding = fixed::binding(cursor)?;
        let issuer = ParticipantId(cursor.fixed()?);
        let subject = ParticipantId(cursor.fixed()?);
        let deadline = if optional(cursor)? {
            Some(fixed::deadline(cursor)?)
        } else {
            None
        };
        let max_responses = cursor.u32()?;
        let obligations = Span::read(cursor, 17)?;
        let lineage = fixed::binding(cursor)?;
        let cause = match cursor.u8()? {
            0 => Cause::Root(RootCommandId(cursor.fixed()?)),
            1 => Cause::Claim(ClaimId(cursor.fixed()?)),
            _ => return Err(CodecError::InvalidTag("claim cause").into()),
        };
        let corrections = Span::read(cursor, 51)?;
        let acceptance = fixed::binding(cursor)?;
        let acceptance_issuer = ParticipantId(cursor.fixed()?);
        let count = cursor.count(cursor.remaining())?;
        let mut decoded = SourceCursor::new(cursor.unread(), &work.source)?;
        let mut checks = 0;
        for _ in 0..count {
            let slot = super::super::claim_source::slot(&mut decoded)?;
            checks = sum(checks, slot.fields().checks)?;
        }
        let slots = Span {
            count,
            bytes: cursor.take(decoded.offset())?,
        };
        let scope_limits = scope::ScopeLimits {
            scopes: cursor.count(usize::MAX)?,
            roots: cursor.count(usize::MAX)?,
            children: cursor.count(usize::MAX)?,
        };
        let owner = if optional(cursor)? {
            Some(creation::Owner {
                expected: fixed::binding(cursor)?,
                receipt: fixed::optional_receipt(cursor)?,
            })
        } else {
            None
        };
        Ok(Self {
            binding,
            issuer,
            subject,
            deadline,
            max_responses,
            obligations,
            lineage,
            cause,
            corrections,
            acceptance,
            acceptance_issuer,
            slots,
            scope_limits,
            owner,
            checks,
        })
    }
    pub(super) fn check(
        &self,
        header: InputHeader,
        native: NativeLimits,
        work: &Work,
    ) -> Result<(), DecodeError> {
        work.structure.charge(32)?;
        if self.binding.ledger != header.ledger
            || self.binding.ledger.tenant.is_zero()
            || self.binding.ledger.session.is_zero()
        {
            return Err(ContractError::WrongLedger.into());
        }
        if self.binding.object.is_zero()
            || self.issuer.is_zero()
            || self.subject.is_zero()
            || self.max_responses == 0
            || self
                .deadline
                .is_some_and(|value| value.timer.is_zero() || value.generation == 0)
        {
            return Err(ContractError::InvalidTarget.into());
        }
        if self.binding.revision != ObjectRevision(1) {
            return Err(ContractError::StaleRevision.into());
        }
        self.lineage.check(&self.binding)?;
        Binding {
            revision: self.binding.revision,
            ..self.acceptance
        }
        .check(&self.binding)?;
        if self.acceptance_issuer != self.issuer
            || header.request.map(|request| request.principal) != Some(self.issuer)
        {
            return Err(ContractError::WrongActor.into());
        }
        match (&self.cause, self.owner) {
            (Cause::Root(_), None) => {}
            (Cause::Claim(parent), Some(owner))
                if owner.expected.ledger == self.binding.ledger
                    && owner.expected.object.0 == parent.0 => {}
            _ => return Err(ContractError::InvalidTarget.into()),
        }
        work.model(&work.structure, |visits| {
            graph::Declaration::check_sorted_values(
                Values::new(self.obligations, &work.source, obligation),
                self.obligations.count,
                native.plan_edges,
                visits,
            )
        })?;
        work.model(&work.structure, |visits| {
            succession::Lineage::check_sorted_values(
                self.lineage,
                &self.cause,
                Values::new(self.corrections, &work.source, correction),
                self.corrections.count,
                native.plan_edges,
                visits,
            )
        })?;
        Ok(())
    }
    pub(super) fn hash(
        &self,
        acceptance: ContentHash,
        hash: &mut creation::CreationIntent,
        work: &Work,
    ) -> Result<(), DecodeError> {
        // Upper byte count of fixed fields, optional owner/deadline and domain;
        // graph and lineage references add their exact fixed-width hash fields.
        work.structure.charge(sum(
            512,
            sum(
                product(self.obligations.count, 17)?,
                product(self.corrections.count, 49)?,
            )?,
        )?)?;
        hash.push(
            &creation::ProposalIntentFields {
                binding: self.binding,
                issuer: self.issuer,
                subject: self.subject,
                deadline: self.deadline,
                max_responses: self.max_responses,
                lineage_binding: self.lineage,
                cause: &self.cause,
                acceptance,
                scope_limits: self.scope_limits,
                owner: self.owner,
            },
            self.obligations.count,
            Values::new(self.obligations, &work.source, obligation),
            self.corrections.count,
            Values::new(self.corrections, &work.source, correction),
        )?;
        Ok(())
    }
}

pub(super) struct Frame<'a> {
    pub(super) projections: Span<'a>,
    pub(super) declarations: Span<'a>,
}
impl<'a> Frame<'a> {
    pub(super) fn read(
        bytes: &'a [u8],
        expected: InputHeader,
        native: NativeLimits,
        work: &Work,
    ) -> Result<Self, DecodeError> {
        work.cursor(bytes, |cursor| {
            if cursor.fixed::<8>()? != MAGIC
                || cursor.u16()? != VERSION
                || cursor.u8()? != 0
                || cursor.u8()? != 0
            {
                return Err(CodecError::InvalidTag("legacy creation header").into());
            }
            let ledger = fixed::ledger(cursor)?;
            let request = RequestKey {
                principal: ParticipantId(cursor.fixed()?),
                epoch: RequestEpoch(cursor.u64()?),
                id: RequestId(cursor.fixed()?),
            };
            if cursor.u8()? != 0 || ledger != expected.ledger || Some(request) != expected.request {
                return Err(CodecError::InvalidTag("legacy creation header").into());
            }
            let count = cursor.count(native.plan_nodes)?;
            if count == 0 {
                return Err(CodecError::Capacity.into());
            }
            let start = cursor.offset();
            for _ in 0..count {
                Projection::read(cursor, work)?;
            }
            let projections = Span {
                count,
                bytes: bytes
                    .get(start..cursor.offset())
                    .ok_or(CodecError::Truncated)?,
            };
            let count = cursor.count(native.range.max_batch_entries / 2)?;
            let start = cursor.offset();
            for _ in 0..count {
                DeclarationBodyInput::read(cursor)?;
            }
            let declarations = Span {
                count,
                bytes: bytes
                    .get(start..cursor.offset())
                    .ok_or(CodecError::Truncated)?,
            };
            if cursor.remaining() != 0 {
                return Err(CodecError::TrailingBytes.into());
            }
            Ok(Self {
                projections,
                declarations,
            })
        })
    }
}
pub(super) struct Projections<'w, 'a> {
    tail: &'a [u8],
    left: usize,
    work: &'w Work,
}
impl<'w, 'a> Projections<'w, 'a> {
    pub(super) fn new(span: Span<'a>, work: &'w Work) -> Self {
        Self {
            tail: span.bytes,
            left: span.count,
            work,
        }
    }
}
impl<'a> Iterator for Projections<'_, 'a> {
    type Item = Result<Projection<'a>, DecodeError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return if self.tail.is_empty() {
                None
            } else {
                self.tail = &[];
                Some(Err(CodecError::TrailingBytes.into()))
            };
        }
        let result = self.work.cursor(self.tail, |cursor| {
            let projection = Projection::read(cursor, self.work)?;
            self.tail = self
                .tail
                .get(cursor.offset()..)
                .ok_or(CodecError::Truncated)?;
            self.left = difference(self.left, 1)?;
            Ok(projection)
        });
        if result.is_err() {
            self.left = 0;
            self.tail = &[];
        }
        Some(result)
    }
}
