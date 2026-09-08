use super::super::read_source::{Meter, Span, model_error};
use super::*;
use crate::native::response_owned::OwnedResponse;
use focal_model::lifecycle::{
    Binding,
    aggregation::PublicationPosition,
    evidence::{
        FailedWorkSnapshotV1, Response, ResponseArtifacts, ResponseDiagnosticSnapshotV1,
        ResponseHydrationPlan, ResponseIdentity, ResponseLimits, ResponseSnapshotFieldsV1,
        ResponseSnapshotSource, SlotBinding,
    },
};
use focal_model::{Confidence, ObjectRevision, OutcomeKind, TestamentId};

pub(in crate::native::record_codec) struct ResponseInput<'a> {
    fields: ResponseSnapshotFieldsV1<'a>,
    manifest: Span<'a>,
    failed: Span<'a>,
    diagnostics: Span<'a>,
    received: Option<PublicationPosition>,
    entered: Option<PublicationPosition>,
    meter: Meter,
}
pub(in crate::native::record_codec) struct ResponsePlan<'s, 'a, A: ResponseArtifacts> {
    model: ResponseHydrationPlan<'s, ResponseInput<'a>, A>,
    source: &'s ResponseInput<'a>,
    generated: Binding,
    quote: Quote,
}
fn manifest_value(c: &mut Cursor<'_>) -> Result<SlotBinding, Error> {
    Ok(SlotBinding {
        slot: c.u32()?,
        artifact: fields::artifact_ref(c)?,
    })
}
pub(in crate::native::record_codec) fn response<'a>(
    c: &mut Cursor<'a>,
    max_summary: usize,
    max_source_visits: usize,
) -> Result<ResponseInput<'a>, NativeError> {
    let read = |c: &mut Cursor<'a>| -> Result<ResponseInput<'a>, Error> {
        let current = fields::binding(c)?;
        let generated = Binding {
            revision: ObjectRevision(c.u64()?),
            ..current
        };
        let identity = ResponseIdentity {
            binding: current,
            claim: ClaimId(c.fixed()?),
            receipt: fields::receipt(c)?,
            cycle: c.u32()?,
            prior: fields::optional(c, |c| Ok(TestamentId(c.fixed()?)))?,
        };
        let respondent = fields::participant(c)?;
        let state = fields::response_state(c)?;
        let summary = c.text(max_summary)?;
        let confidence = match c.u8()? {
            0 => Confidence::Hint,
            1 => Confidence::Tentative,
            2 => Confidence::Committed,
            3 => Confidence::Consensus,
            _ => return Err(Error::InvalidTag("confidence")),
        };
        let outcome = match c.u8()? {
            0 => OutcomeKind::Complete,
            1 => OutcomeKind::Partial,
            2 => OutcomeKind::Refused,
            3 => OutcomeKind::Impossible,
            4 => OutcomeKind::Interrupted,
            5 => OutcomeKind::Failed,
            _ => return Err(Error::InvalidTag("response outcome")),
        };
        let manifest = Span::read_fixed(c, 52)?;
        let failed = Span::read_fixed(c, 142)?;
        let diagnostics = Span::read_fixed(c, 141)?;
        let terminal = fields::optional(c, fields::response_terminal)?;
        let received = fields::optional_position(c)?;
        let entered = fields::optional_position(c)?;
        Ok(ResponseInput {
            fields: ResponseSnapshotFieldsV1 {
                generated,
                identity,
                respondent,
                state,
                summary,
                confidence,
                outcome,
                manifest_count: manifest.count,
                failed_work_count: failed.count,
                diagnostic_count: diagnostics.count,
                terminal,
            },
            manifest,
            failed,
            diagnostics,
            received,
            entered,
            meter: Meter::new(max_source_visits),
        })
    };
    read(c).map_err(codec)
}
impl<'a> ResponseInput<'a> {
    pub(in crate::native::record_codec) fn fields(&self) -> ResponseSnapshotFieldsV1<'a> {
        self.fields
    }
    fn at<T>(
        &self,
        span: Span<'a>,
        width: usize,
        index: usize,
        read: fn(&mut Cursor<'a>) -> Result<T, Error>,
    ) -> Result<T, ContractError> {
        let value = (|| {
            self.meter.charge(4)?;
            if index >= span.count {
                return Err(Error::Truncated);
            }
            let start = index.checked_mul(width).ok_or(Error::Capacity)?;
            let end = start.checked_add(width).ok_or(Error::Capacity)?;
            let bytes = span.bytes.get(start..end).ok_or(Error::Truncated)?;
            let (value, consumed) = self.meter.read(bytes, read)?;
            if consumed != width {
                return Err(Error::TrailingBytes);
            }
            Ok(value)
        })();
        value.map_err(model_error)
    }
    pub(in crate::native::record_codec) fn prepare<'s, A: ResponseArtifacts>(
        &'s self,
        policy: &'s AcceptancePolicy,
        artifacts: &'s A,
        limits: ResponseLimits,
        max_model_visits: usize,
    ) -> Result<ResponsePlan<'s, 'a, A>, NativeError> {
        let before = self.meter.remaining();
        let model =
            Response::prepare_hydration_v1(self, policy, artifacts, limits, max_model_visits)?;
        // One raw collection copy. Actual-owned verification in model build
        // reads the newly built buffers, not these borrowed encoded spans.
        let source_build = add(
            add(mul(self.manifest.count, 59)?, mul(self.failed.count, 156)?)?,
            mul(self.diagnostics.count, 155)?,
        )?;
        fits(source_build, self.meter.remaining())?;
        let heap_bytes = add(
            OwnedResponse::container_charge(),
            add(
                model.construction_heap_bytes(),
                mul(
                    model.construction_heap_allocations(),
                    crate::native::prepare::ALLOCATION,
                )?,
            )?,
        )?;
        let quote = Quote {
            heap_bytes,
            allocations: add(1, model.construction_heap_allocations())?,
            model_inspection_visits: model.inspection_visits(),
            model_build_visits: add(model.build_visits(), 32)?,
            source_inspection_visits: before
                .checked_sub(self.meter.remaining())
                .ok_or(NativeError::Capacity("response source work"))?,
            source_build_visits: source_build,
        };
        Ok(ResponsePlan {
            model,
            source: self,
            generated: self.fields.generated,
            quote,
        })
    }
}
impl ResponseSnapshotSource for ResponseInput<'_> {
    fn fields(&self) -> Result<ResponseSnapshotFieldsV1<'_>, ContractError> {
        Ok(self.fields)
    }
    fn manifest(&self, index: usize) -> Result<SlotBinding, ContractError> {
        self.at(self.manifest, 52, index, manifest_value)
    }
    fn failed_work(&self, index: usize) -> Result<FailedWorkSnapshotV1, ContractError> {
        self.at(self.failed, 142, index, super::scalar::failed_value)
    }
    fn diagnostic(&self, index: usize) -> Result<ResponseDiagnosticSnapshotV1, ContractError> {
        self.at(
            self.diagnostics,
            141,
            index,
            super::scalar::diagnostic_value,
        )
    }
}
impl<A: ResponseArtifacts> ResponsePlan<'_, '_, A> {
    pub(in crate::native::record_codec) fn quote(&self) -> Quote {
        self.quote
    }
    pub(in crate::native::record_codec) fn build(
        self,
        max_bytes: usize,
        max_model_visits: usize,
    ) -> Result<Row, NativeError> {
        fits(self.quote.heap_bytes, max_bytes)?;
        fits(self.quote.model_build_visits, max_model_visits)?;
        fits(
            self.quote.source_build_visits,
            self.source.meter.remaining(),
        )?;
        let before = self.source.meter.remaining();
        let charge = self.model.construction_charge();
        let visits = self.model.build_visits();
        let response = self.model.build(charge, visits)?;
        if before.checked_sub(self.source.meter.remaining()) != Some(self.quote.source_build_visits)
        {
            return Err(NativeError::Capacity("response source quote"));
        }
        let owned = OwnedResponse::hydrate(
            response,
            self.generated,
            self.source.received,
            self.source.entered,
        )?;
        let actual = owned.heap_charge()?;
        finish(Row::Response(owned), actual, self.quote, max_bytes)
    }
}
