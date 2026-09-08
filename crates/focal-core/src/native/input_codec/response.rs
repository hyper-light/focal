//! Dynamic response and monitor input plans. Byte collections remain borrowed
//! until the caller supplies the complete final-buffer allowance. Identity is
//! the existing native request preimage; this is not authority or owner funding.
use super::super::prepare::{add, within};
use super::super::response_input::{
    NativeMonitorSource, NativeMonitorSourcePlan, NativeResponseSource, NativeResponseSourcePlan,
    NativeSourceQuote,
};
use super::bytes::Cursor;
use super::*;
use focal_model::lifecycle::evidence::SlotBinding;
use focal_model::{ArtifactRef, Confidence, OutcomeKind};

#[cfg(test)]
#[path = "response_tests.rs"]
mod tests;

const HEADER_BYTES: usize = 85;
const REQUEST_HASH_VISITS: usize = 1 + b"focal/native/request-intent/1".len() + 4 * 17 + 9;
const BINDING_HASH_VISITS: usize = 3 * 17 + 33 + 9;
const MONITOR_PREFIX_VISITS: usize =
    REQUEST_HASH_VISITS + 2 + BINDING_HASH_VISITS + 2 + 17 + 9 + 35 + 1;
const MONITOR_ROOT_VISITS: usize = 20 + size_of::<WaitPredicate>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DynamicInputQuote {
    pub bytes: usize,
    pub allocations: usize,
    /// Complete typed pass and identity work, separate from initial inspection.
    pub prepare_visits: usize,
    pub build_visits: usize,
}
fn quoted(source: NativeSourceQuote, prepare_visits: usize) -> DynamicInputQuote {
    DynamicInputQuote {
        bytes: source.bytes,
        allocations: source.allocations,
        prepare_visits,
        build_visits: source.build_visits,
    }
}
fn invalid() -> NativeError {
    ContractError::InvalidManifest.into()
}
fn product(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_mul(b)
        .ok_or(NativeError::Capacity("encoded collection"))
}
fn remaining(maximum: usize, used: usize) -> Result<usize, NativeError> {
    maximum
        .checked_sub(used)
        .ok_or(NativeError::Capacity("decode visits"))
}
fn record(
    bytes: &[u8],
    width: usize,
    count: usize,
    index: usize,
) -> Result<Option<&[u8]>, NativeError> {
    if index >= count {
        return Ok(None);
    }
    let start = product(index, width)?;
    Ok(Some(
        bytes.get(start..add(start, width)?).ok_or_else(invalid)?,
    ))
}
fn fixed<const N: usize>(bytes: &[u8], start: usize) -> Result<[u8; N], NativeError> {
    bytes
        .get(start..add(start, N)?)
        .ok_or_else(invalid)?
        .try_into()
        .map_err(|_| invalid())
}
fn artifact_ref(bytes: &[u8], start: usize) -> Result<ArtifactRef, NativeError> {
    Ok(ArtifactRef {
        id: ArtifactId(fixed(bytes, start)?),
        hash: ContentHash(fixed(bytes, add(start, 16)?)?),
    })
}

#[derive(Debug)]
struct EncodedResponse<'a> {
    summary: &'a str,
    confidence: Confidence,
    outcome: OutcomeKind,
    manifest: &'a [u8],
    manifest_count: usize,
    diagnostics: &'a [u8],
    diagnostic_count: usize,
}
impl NativeResponseSource for EncodedResponse<'_> {
    fn summary(&self) -> &str {
        self.summary
    }
    fn confidence(&self) -> Confidence {
        self.confidence
    }
    fn outcome(&self) -> OutcomeKind {
        self.outcome
    }
    fn manifest_len(&self) -> usize {
        self.manifest_count
    }
    fn diagnostic_len(&self) -> usize {
        self.diagnostic_count
    }
    fn manifest(&self, index: usize) -> Result<Option<SlotBinding>, NativeError> {
        record(self.manifest, 52, self.manifest_count, index)?
            .map(|bytes| {
                Ok(SlotBinding {
                    slot: u32::from_le_bytes(fixed(bytes, 0)?),
                    artifact: artifact_ref(bytes, 4)?,
                })
            })
            .transpose()
    }
    fn diagnostic(&self, index: usize) -> Result<Option<ArtifactRef>, NativeError> {
        record(self.diagnostics, 48, self.diagnostic_count, index)?
            .map(|bytes| artifact_ref(bytes, 0))
            .transpose()
    }
}

#[derive(Debug)]
pub struct ResponseFramePlan<'a> {
    header: InputHeader,
    claim: Binding,
    response: Binding,
    body: NativeResponseSourcePlan<EncodedResponse<'a>>,
    intent: ContentHash,
    quote: DynamicInputQuote,
}
impl ResponseFramePlan<'_> {
    pub(in crate::native) fn body(&self) -> &NativeResponseSourcePlan<impl NativeResponseSource> {
        &self.body
    }
    pub fn header(&self) -> InputHeader {
        self.header
    }
    pub fn claim(&self) -> Binding {
        self.claim
    }
    pub fn response(&self) -> Binding {
        self.response
    }
    pub fn summary(&self) -> &str {
        self.body.source().summary
    }
    pub fn intent(&self) -> ContentHash {
        self.intent
    }
    pub fn quote(&self) -> DynamicInputQuote {
        self.quote
    }
    pub fn build(self, max_bytes: usize, max_visits: usize) -> Result<NativeInput, DecodeError> {
        let request = self.header.request.ok_or_else(invalid)?;
        let report = self.body.build(max_bytes, max_visits)?;
        Ok(NativeInput {
            request,
            command: NativeCommand::CloseResponse {
                claim: self.claim,
                response: self.response,
                report,
            },
        })
    }
}

#[derive(Debug)]
struct EncodedRoots<'a> {
    bytes: &'a [u8],
    count: usize,
}
impl NativeMonitorSource for EncodedRoots<'_> {
    fn len(&self) -> usize {
        self.count
    }
    fn root(&self, index: usize) -> Result<Option<WaitPredicate>, NativeError> {
        record(self.bytes, 17, self.count, index)?
            .map(|bytes| {
                let [tag] = fixed(bytes, 0)?;
                let id = ClaimId(fixed(bytes, 1)?);
                match tag {
                    0 => Ok(WaitPredicate::Satisfied(id)),
                    1 => Ok(WaitPredicate::Terminal(id)),
                    2 => Ok(WaitPredicate::Released(id)),
                    _ => Err(invalid()),
                }
            })
            .transpose()
    }
}

#[derive(Debug)]
pub struct MonitorFramePlan<'a> {
    header: InputHeader,
    expected: Binding,
    receipt: Option<ReceiptFence>,
    id: MonitorId,
    deadline: Deadline,
    roots: NativeMonitorSourcePlan<EncodedRoots<'a>>,
    intent: ContentHash,
    quote: DynamicInputQuote,
}
impl MonitorFramePlan<'_> {
    pub fn header(&self) -> InputHeader {
        self.header
    }
    pub fn expected(&self) -> Binding {
        self.expected
    }
    pub fn receipt(&self) -> Option<ReceiptFence> {
        self.receipt
    }
    pub fn id(&self) -> MonitorId {
        self.id
    }
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }
    pub fn intent(&self) -> ContentHash {
        self.intent
    }
    pub fn quote(&self) -> DynamicInputQuote {
        self.quote
    }
    pub fn build(self, max_bytes: usize, max_visits: usize) -> Result<NativeInput, DecodeError> {
        let request = self.header.request.ok_or_else(invalid)?;
        let roots = self.roots.build(max_bytes, max_visits)?;
        Ok(NativeInput {
            request,
            command: NativeCommand::RegisterMonitor {
                expected: self.expected,
                receipt: self.receipt,
                id: self.id,
                roots,
                deadline: self.deadline,
            },
        })
    }
}

impl<'a> StructuralInput<'a> {
    /// The initial complete structural inspection is a separate bounded pass.
    /// This pass borrows the immutable bytes and retains no parser allocation.
    /// Other command families return None without a second traversal.
    pub fn prepare_response(
        &self,
        limits: NativeLimits,
        max_visits: usize,
    ) -> Result<Option<ResponseFramePlan<'a>>, DecodeError> {
        let header = self.header();
        if header.kind != (FrameKind::Request { command: 9 }) {
            return Ok(None);
        }
        let request = header.request.ok_or_else(invalid)?;
        let bytes = self.bytes();
        let mut cursor = Cursor::new(bytes, bytes.len(), max_visits)?;
        // Header provenance comes from StructuralInput's private complete scan;
        // the borrowed slice cannot mutate between these passes.
        cursor.take(HEADER_BYTES)?;
        let claim = super::fixed::binding(&mut cursor)?;
        let response = super::fixed::binding(&mut cursor)?;
        let summary = cursor.text(limits.response_summary_bytes)?;
        let confidence = match cursor.u8()? {
            0 => Confidence::Hint,
            1 => Confidence::Tentative,
            2 => Confidence::Committed,
            3 => Confidence::Consensus,
            _ => return Err(CodecError::InvalidTag("confidence").into()),
        };
        let outcome = match cursor.u8()? {
            0 => OutcomeKind::Complete,
            1 => OutcomeKind::Partial,
            2 => OutcomeKind::Refused,
            3 => OutcomeKind::Impossible,
            4 => OutcomeKind::Interrupted,
            5 => OutcomeKind::Failed,
            _ => return Err(CodecError::InvalidTag("response outcome").into()),
        };
        let manifest_count = cursor.count(super::super::response_budget::work_limit(limits)?)?;
        let manifest = cursor.take(product(manifest_count, 52)?)?;
        let diagnostic_count = cursor.count(limits.diagnostics_per_cycle)?;
        let diagnostics = cursor.take(product(diagnostic_count, 48)?)?;
        let parsed = cursor.visits_used();
        cursor.finish()?;
        let body = NativeResponseSourcePlan::prepare(
            EncodedResponse {
                summary,
                confidence,
                outcome,
                manifest,
                manifest_count,
                diagnostics,
                diagnostic_count,
            },
            limits,
            remaining(max_visits, parsed)?,
        )?;
        let source_quote = body.quote();
        let prefix = REQUEST_HASH_VISITS + 2 + 2 * BINDING_HASH_VISITS;
        let visited = add(
            add(parsed, source_quote.prepare_visits)?,
            add(prefix, source_quote.hash_visits)?,
        )?;
        within(visited, max_visits)?;
        let mut hash = super::super::intent::request_hasher(header.ledger, request);
        hash.update(&[9]);
        super::super::intent::hash_binding(&mut hash, claim);
        super::super::intent::hash_binding(&mut hash, response);
        body.hash_into(&mut hash, source_quote.hash_visits)?;
        Ok(Some(ResponseFramePlan {
            header,
            claim,
            response,
            body,
            intent: ContentHash(*hash.finalize().as_bytes()),
            quote: quoted(source_quote, visited),
        }))
    }

    pub fn prepare_monitor(
        &self,
        limits: NativeLimits,
        max_visits: usize,
    ) -> Result<Option<MonitorFramePlan<'a>>, DecodeError> {
        let header = self.header();
        if header.kind != (FrameKind::Request { command: 24 }) {
            return Ok(None);
        }
        let request = header.request.ok_or_else(invalid)?;
        let bytes = self.bytes();
        let mut cursor = Cursor::new(bytes, bytes.len(), max_visits)?;
        cursor.take(HEADER_BYTES)?;
        let expected = super::fixed::binding(&mut cursor)?;
        let receipt = super::fixed::optional_receipt(&mut cursor)?;
        let id = MonitorId(cursor.fixed()?);
        let count = cursor.count(limits.plan_edges)?;
        let values = cursor.take(product(count, 17)?)?;
        let deadline = super::fixed::deadline(&mut cursor)?;
        let parsed = cursor.visits_used();
        cursor.finish()?;
        let roots = NativeMonitorSourcePlan::prepare(
            EncodedRoots {
                bytes: values,
                count,
            },
            limits,
            remaining(max_visits, parsed)?,
        )?;
        let source_quote = roots.quote();
        // Existing request header, command tag, binding, optional receipt,
        // monitor ID, root count, deadline and one terminal iterator read.
        let prefix = add(
            MONITOR_PREFIX_VISITS,
            if receipt.is_some() { 26 } else { 0 },
        )?;
        let hash_visits = add(prefix, product(count, MONITOR_ROOT_VISITS)?)?;
        let visited = add(add(parsed, source_quote.prepare_visits)?, hash_visits)?;
        within(visited, max_visits)?;
        let mut hash = super::super::intent::request_hasher(header.ledger, request);
        super::super::intent::hash_monitor_registration(
            &mut hash,
            expected,
            receipt,
            id,
            roots.roots(),
            count,
            deadline,
        )?;
        Ok(Some(MonitorFramePlan {
            header,
            expected,
            receipt,
            id,
            deadline,
            roots,
            intent: ContentHash(*hash.finalize().as_bytes()),
            quote: quoted(source_quote, visited),
        }))
    }
}
