//! Borrowed artifact-bearing commands. No typed reference/string scratch arrays
//! are allocated before the caller reserves the final input charge.
use super::super::artifact_intent::{ArtifactCommand, ReportKind};
use super::bytes::Cursor;
use super::*;
use focal_model::{
    ObjectRef,
    lifecycle::artifact_descriptor::{
        self as model, ArtifactFields, ArtifactSource, ArtifactSourcePlan,
    },
};
use std::cell::Cell;

#[cfg(test)]
#[path = "artifact_tests.rs"]
mod tests;

#[derive(Debug)]
struct Source<'a> {
    fields: ArtifactFields<'a>,
    inputs: &'a [u8],
    input_count: usize,
    labels: &'a [u8],
    label_count: usize,
    remaining: Cell<usize>,
}
fn debit(remaining: &Cell<usize>, amount: usize) -> Result<(), ContractError> {
    let next = remaining
        .get()
        .checked_sub(amount)
        .ok_or(ContractError::Capacity)?;
    remaining.set(next);
    Ok(())
}
fn model_error(error: CodecError) -> ContractError {
    match error {
        CodecError::Capacity => ContractError::Capacity,
        _ => ContractError::InvalidManifest,
    }
}
struct Inputs<'s, 'a> {
    bytes: &'a [u8],
    left: usize,
    quota: &'s Cell<usize>,
}
impl Iterator for Inputs<'_, '_> {
    type Item = Result<ObjectRef, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        let value = (|| {
            debit(self.quota, 54)?;
            let (bytes, tail) = self
                .bytes
                .split_at_checked(50)
                .ok_or(ContractError::InvalidManifest)?;
            let mut cursor = Cursor::new(bytes, 50, 54).map_err(model_error)?;
            let value = super::artifact_fields::object_ref(&mut cursor).map_err(model_error)?;
            cursor.finish().map_err(model_error)?;
            self.bytes = tail;
            self.left = self.left.checked_sub(1).ok_or(ContractError::Capacity)?;
            Ok(value)
        })();
        if value.is_err() {
            self.left = 0;
        }
        Some(value)
    }
}
struct Labels<'s, 'a> {
    bytes: &'a [u8],
    left: usize,
    quota: &'s Cell<usize>,
}
impl<'a> Iterator for Labels<'_, 'a> {
    type Item = Result<&'a str, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        let value = (|| {
            debit(self.quota, 6)?;
            let mut cursor =
                Cursor::new(self.bytes, self.bytes.len(), usize::MAX).map_err(model_error)?;
            let len = cursor.count(cursor.remaining()).map_err(model_error)?;
            let work = len
                .checked_mul(2)
                .and_then(|n| n.checked_add(2))
                .ok_or(ContractError::Capacity)?;
            debit(self.quota, work)?;
            let bytes = cursor.take(len).map_err(model_error)?;
            let label = std::str::from_utf8(bytes).map_err(|_| ContractError::InvalidManifest)?;
            self.bytes = self
                .bytes
                .get(cursor.offset()..)
                .ok_or(ContractError::InvalidManifest)?;
            self.left = self.left.checked_sub(1).ok_or(ContractError::Capacity)?;
            Ok(label)
        })();
        if value.is_err() {
            self.left = 0;
        }
        Some(value)
    }
}
impl<'a> ArtifactSource<'a> for Source<'a> {
    type Inputs<'s>
        = Inputs<'s, 'a>
    where
        Self: 's;
    type Visibility<'s>
        = Labels<'s, 'a>
    where
        Self: 's;
    fn fields(&self) -> ArtifactFields<'a> {
        self.fields
    }
    fn input_count(&self) -> usize {
        self.input_count
    }
    fn visibility_count(&self) -> usize {
        self.label_count
    }
    fn inputs(&self) -> Self::Inputs<'_> {
        Inputs {
            bytes: self.inputs,
            left: self.input_count,
            quota: &self.remaining,
        }
    }
    fn visibility(&self) -> Self::Visibility<'_> {
        Labels {
            bytes: self.labels,
            left: self.label_count,
            quota: &self.remaining,
        }
    }
}

/// Structurally decoded fields still require model preparation below and the
/// normal owner authority/custody checks. The source stays borrowed through build.
#[derive(Debug)]
pub struct ArtifactInputView<'a> {
    header: InputHeader,
    command: ArtifactCommand,
    source: Source<'a>,
    parse_visits: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactInputQuote {
    pub bytes: usize,
    pub allocations: usize,
    pub parse_visits: usize,
    pub source_inspection_visits: usize,
    pub source_build_visits: usize,
    pub model_inspection_visits: usize,
    pub model_build_visits: usize,
    /// Fixed command hashing and wrapper checks; distinct from model/source work.
    pub native_inspection_visits: usize,
    pub native_build_visits: usize,
}
#[derive(Debug)]
pub struct ArtifactFramePlan<'s, 'a> {
    header: InputHeader,
    command: ArtifactCommand,
    descriptor: ArtifactSourcePlan<'s, 'a, Source<'a>>,
    source: &'s Source<'a>,
    quote: ArtifactInputQuote,
    intent: ContentHash,
}
impl<'a> StructuralInput<'a> {
    /// Parse the six artifact-bearing commands under a distinct complete-pass
    /// limit. The already checked header selects no owner/timer authority.
    pub fn artifact_input(
        &self,
        max_visits: usize,
    ) -> Result<Option<ArtifactInputView<'a>>, DecodeError> {
        let header = self.header();
        let FrameKind::Request {
            command: tag @ (4 | 6 | 7 | 13 | 15 | 19),
        } = header.kind
        else {
            return Ok(None);
        };
        let bytes = self.bytes();
        let mut cursor = Cursor::new(bytes, bytes.len(), max_visits)?;
        cursor.take(85)?;
        let claim = fixed::binding(&mut cursor)?;
        let command = match tag {
            6 => ArtifactCommand::SubmitWork {
                claim,
                slot: cursor.u32()?,
            },
            7 => ArtifactCommand::SubmitDiagnostic {
                claim,
                reason: super::artifact_fields::failure(&mut cursor)?,
            },
            13 => ArtifactCommand::RejectWork {
                claim,
                expected: fixed::binding(&mut cursor)?,
                reason: super::artifact_fields::failure(&mut cursor)?,
            },
            4 | 15 | 19 => {
                let key = fixed::evaluation(&mut cursor)?;
                let expected = fixed::binding(&mut cursor)?;
                let report = super::artifact_fields::report(&mut cursor)?;
                let kind = match tag {
                    4 => ReportKind::Admission,
                    15 => ReportKind::Increment,
                    _ => ReportKind::Work,
                };
                ArtifactCommand::Report {
                    kind,
                    claim,
                    key,
                    expected,
                    report,
                }
            }
            _ => return Err(CodecError::InvalidTag("artifact command").into()),
        };
        let fields = super::artifact_fields::fields(&mut cursor)?;
        let input_count = cursor.count(cursor.remaining())?;
        let length = input_count.checked_mul(50).ok_or(CodecError::Capacity)?;
        let inputs = cursor.take(length)?;
        let label_count = cursor.count(cursor.remaining())?;
        let start = cursor.offset();
        for _ in 0..label_count {
            cursor.text(cursor.remaining())?;
        }
        let labels = bytes
            .get(start..cursor.offset())
            .ok_or(CodecError::Truncated)?;
        let parse_visits = cursor.visits_used();
        cursor.finish()?;
        Ok(Some(ArtifactInputView {
            header,
            command,
            source: Source {
                fields,
                inputs,
                input_count,
                labels,
                label_count,
                remaining: Cell::new(0),
            },
            parse_visits,
        }))
    }
}
impl<'a> ArtifactInputView<'a> {
    pub fn header(&self) -> InputHeader {
        self.header
    }
    pub fn parse_visits(&self) -> usize {
        self.parse_visits
    }
    /// Model and encoded-source work have distinct limits. The source allowance
    /// is shared across preparation AND construction and is never reset by build.
    pub fn prepare(
        &mut self,
        native: NativeLimits,
        limits: model::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
        max_native_visits: usize,
    ) -> Result<ArtifactFramePlan<'_, 'a>, DecodeError> {
        let plan = self.prepare_for_frame(
            native,
            limits,
            max_model_visits,
            max_source_visits,
            max_native_visits,
        )?;
        plan.check_build_capacity()?;
        Ok(plan)
    }

    /// Owner retry inspection needs only the completed content proof. Fresh
    /// admission and actual construction separately require the final source
    /// pass, before allocating any buffers. The public standalone API retains
    /// its stronger known-build-headroom guarantee.
    pub(super) fn prepare_for_frame(
        &mut self,
        native: NativeLimits,
        limits: model::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
        max_native_visits: usize,
    ) -> Result<ArtifactFramePlan<'_, 'a>, DecodeError> {
        // A fixed upper bound covers the largest command header/request hash,
        // scalar checks and quote arithmetic before any variable model work.
        const NATIVE_INSPECTION: usize = 4096;
        if max_native_visits < NATIVE_INSPECTION {
            return Err(CodecError::Capacity.into());
        }
        if self.source.fields.ledger != self.header.ledger {
            return Err(ContractError::WrongLedger.into());
        }
        self.source.remaining.set(max_source_visits);
        let descriptor =
            model::ArtifactDescriptor::prepare_source(&self.source, limits, max_model_visits)?;
        let source_visits = max_source_visits
            .checked_sub(self.source.remaining.get())
            .ok_or(ContractError::Capacity)?;
        let allocations = descriptor
            .construction_heap_allocations()
            .checked_add(1)
            .ok_or(ContractError::Capacity)?;
        let bookkeeping = descriptor
            .construction_heap_allocations()
            .checked_mul(super::super::prepare::ALLOCATION)
            .ok_or(ContractError::Capacity)?;
        let bytes = super::super::prepare::add(
            NativeArtifactInput::container_charge(),
            super::super::prepare::add(descriptor.construction_heap_bytes(), bookkeeping)?,
        )?;
        super::super::prepare::within(bytes, native.preparation_bytes)?;
        let request = self
            .header
            .request
            .ok_or(CodecError::InvalidTag("actor request"))?;
        let mut hash = super::super::intent::request_hasher(self.header.ledger, request);
        self.command
            .hash_into(&mut hash, descriptor.intent_fingerprint())?;
        let intent = ContentHash(*hash.finalize().as_bytes());
        // NativeArtifactInput::new and heap_charge each scan label capacities
        // and allocation presence. Sixteen visits per label conservatively
        // cover those four scans; fixed hashing/checks use the same 4096 bound.
        let native_build_visits = self
            .source
            .label_count
            .checked_mul(16)
            .and_then(|n| n.checked_add(NATIVE_INSPECTION))
            .ok_or(ContractError::Capacity)?;
        let quote = ArtifactInputQuote {
            bytes,
            allocations,
            parse_visits: self.parse_visits,
            source_inspection_visits: source_visits,
            source_build_visits: source_visits,
            model_inspection_visits: descriptor.inspection_visits(),
            model_build_visits: descriptor.build_visits(),
            native_inspection_visits: NATIVE_INSPECTION,
            native_build_visits,
        };
        Ok(ArtifactFramePlan {
            header: self.header,
            command: self.command,
            descriptor,
            source: &self.source,
            quote,
            intent,
        })
    }
}
impl ArtifactFramePlan<'_, '_> {
    pub(in crate::native) fn command(&self) -> ArtifactCommand {
        self.command
    }
    pub(in crate::native) fn descriptor(
        &self,
    ) -> &impl super::super::report_artifact::ArtifactView {
        &self.descriptor
    }
    pub(in crate::native) fn check_build_capacity(&self) -> Result<(), DecodeError> {
        if self.source.remaining.get() < self.quote.source_build_visits {
            return Err(ContractError::Capacity.into());
        }
        Ok(())
    }
    pub fn header(&self) -> InputHeader {
        self.header
    }
    pub fn quote(&self) -> ArtifactInputQuote {
        self.quote
    }
    pub fn intent(&self) -> ContentHash {
        self.intent
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_model_visits: usize,
        max_native_visits: usize,
    ) -> Result<NativeInput, DecodeError> {
        super::super::prepare::within(self.quote.bytes, max_bytes)?;
        self.check_build_capacity()?;
        if max_native_visits < self.quote.native_build_visits {
            return Err(CodecError::Capacity.into());
        }
        let source_before = self.source.remaining.get();
        let descriptor_bytes = self.descriptor.construction_charge();
        let descriptor = self.descriptor.build(descriptor_bytes, max_model_visits)?;
        if source_before.checked_sub(self.source.remaining.get())
            != Some(self.quote.source_build_visits)
        {
            return Err(ContractError::Capacity.into());
        }
        let artifact = NativeArtifactInput::new(descriptor)?;
        super::super::prepare::within(artifact.heap_charge()?, self.quote.bytes)?;
        let input = NativeInput {
            request: self
                .header
                .request
                .ok_or(CodecError::InvalidTag("actor request"))?,
            command: self.command.build(artifact),
        };
        if super::super::intent::fingerprint(self.header.ledger, &input)? != self.intent {
            return Err(ContractError::ContentConflict.into());
        }
        Ok(input)
    }
}

/// Standalone retained descriptor body. This decoder has no command header,
/// request identity, participant authority or custody capability.
#[derive(Debug)]
pub(in crate::native) struct ArtifactBodyInput<'a> {
    source: Source<'a>,
}
#[derive(Debug)]
pub(in crate::native) struct ArtifactBodyPlan<'s, 'a> {
    source: &'s Source<'a>,
    descriptor: ArtifactSourcePlan<'s, 'a, Source<'a>>,
    quote: super::creation_content::BodyConstructionQuote,
}
impl<'a> ArtifactBodyInput<'a> {
    pub(in crate::native) fn read(cursor: &mut Cursor<'a>) -> Result<Self, DecodeError> {
        let fields = super::artifact_fields::fields(cursor)?;
        let input_count = cursor.count(cursor.remaining())?;
        let inputs = cursor.take(input_count.checked_mul(50).ok_or(CodecError::Capacity)?)?;
        let label_count = cursor.count(cursor.remaining())?;
        let start = cursor.unread();
        let before = cursor.offset();
        cursor.visit(label_count.checked_add(1).ok_or(CodecError::Capacity)?)?;
        for _ in 0..label_count { cursor.text(cursor.remaining())?; }
        let length = cursor.offset().checked_sub(before).ok_or(CodecError::Capacity)?;
        let labels = start.get(..length).ok_or(CodecError::Truncated)?;
        Ok(Self { source: Source { fields, inputs, input_count, labels, label_count, remaining: Cell::new(0) } })
    }
    pub(in crate::native) fn fields(&self) -> ArtifactFields<'a> { self.source.fields }
    pub(in crate::native) fn ownership_visits(&self) -> Result<usize, DecodeError> {
        self.source.label_count.checked_mul(16).and_then(|n| n.checked_add(128))
            .ok_or_else(|| CodecError::Capacity.into())
    }
    pub(in crate::native) fn prepare(&mut self, limits: model::Limits, max_model_visits: usize,
        max_source_visits: usize) -> Result<ArtifactBodyPlan<'_, 'a>, DecodeError> {
        self.source.remaining.set(max_source_visits);
        let descriptor = model::ArtifactDescriptor::prepare_source(&self.source, limits, max_model_visits)?;
        let source_visits = max_source_visits.checked_sub(self.source.remaining.get()).ok_or(CodecError::Capacity)?;
        if self.source.remaining.get() < source_visits { return Err(CodecError::Capacity.into()); }
        let allocations = descriptor.construction_heap_allocations();
        let bytes = allocations.checked_mul(super::super::prepare::ALLOCATION)
            .and_then(|extra| extra.checked_add(descriptor.construction_charge())).ok_or(CodecError::Capacity)?;
        let quote = super::creation_content::BodyConstructionQuote {
            bytes, allocations, model_inspection_visits: descriptor.inspection_visits(),
            model_build_visits: descriptor.build_visits(), source_inspection_visits: source_visits,
            source_build_visits: source_visits,
        };
        Ok(ArtifactBodyPlan { source: &self.source, descriptor, quote })
    }
}
impl ArtifactBodyPlan<'_, '_> {
    pub(in crate::native) fn quote(&self) -> super::creation_content::BodyConstructionQuote { self.quote }
    pub(in crate::native) fn fields(&self) -> ArtifactFields<'_> { self.descriptor.fields() }
    pub(in crate::native) fn content_hash(&self) -> ContentHash { self.descriptor.content_hash() }
    pub(in crate::native) fn build(self, max_bytes: usize, max_model_visits: usize) -> Result<model::ArtifactDescriptor, DecodeError> {
        if max_bytes < self.quote.bytes || max_model_visits < self.quote.model_build_visits
            || self.source.remaining.get() < self.quote.source_build_visits { return Err(CodecError::Capacity.into()); }
        let before = self.source.remaining.get();
        let charge = self.descriptor.construction_charge();
        let value = self.descriptor.build(charge, max_model_visits)?;
        if before.checked_sub(self.source.remaining.get()) != Some(self.quote.source_build_visits) {
            return Err(CodecError::Capacity.into());
        }
        Ok(value)
    }
}
