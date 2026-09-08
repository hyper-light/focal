use super::*;

/// Fixed-width collections are indexed without constructing decoding arrays.
/// Each method must be repeatable. The model bounds calls; source parsing and
/// row lookup work must also debit the importer's shared work allowance.
pub trait ResponseSnapshotSource {
    fn fields(&self) -> Result<ResponseSnapshotFieldsV1<'_>, ContractError>;
    fn manifest(&self, index: usize) -> Result<SlotBinding, ContractError>;
    fn failed_work(&self, index: usize) -> Result<FailedWorkSnapshotV1, ContractError>;
    fn diagnostic(&self, index: usize) -> Result<ResponseDiagnosticSnapshotV1, ContractError>;
}
/// Resolve actual immutable descriptors and previously restored work rows.
/// No supplied hash, detached descriptor or current execution authority is a
/// substitute for those rows. Artifact IDs must be checked even when hashes match.
pub trait ResponseArtifacts {
    fn artifact(&self, reference: ArtifactRef) -> Result<&ArtifactDescriptor, ContractError>;
    fn work(&self, id: ArtifactId) -> Result<&WorkArtifact, ContractError>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseSnapshotFieldsV1<'a> {
    pub generated: Binding,
    pub identity: ResponseIdentity,
    pub respondent: ParticipantId,
    pub state: ResponseState,
    pub summary: &'a str,
    pub confidence: Confidence,
    pub outcome: OutcomeKind,
    pub manifest_count: usize,
    pub failed_work_count: usize,
    pub diagnostic_count: usize,
    pub terminal: Option<ResponseTerminalSnapshotV1>,
}
/// Allocation-free borrowed export. The native exporter resolves `generated`
/// from the original Generated event, not from the current revision or a guess.
#[derive(Debug, Clone, Copy)]
pub struct ResponseSnapshotV1<'a> {
    response: &'a Response,
    fields: ResponseSnapshotFieldsV1<'a>,
}
impl ResponseSnapshotSource for ResponseSnapshotV1<'_> {
    fn fields(&self) -> Result<ResponseSnapshotFieldsV1<'_>, ContractError> {
        Ok(self.fields)
    }
    fn manifest(&self, index: usize) -> Result<SlotBinding, ContractError> {
        self.response
            .manifest
            .get(index)
            .copied()
            .ok_or(ContractError::InvalidManifest)
    }
    fn failed_work(&self, index: usize) -> Result<FailedWorkSnapshotV1, ContractError> {
        self.response
            .failed_work
            .get(index)
            .copied()
            .map(FailedWork::snapshot_v1)
            .ok_or(ContractError::InvalidManifest)
    }
    fn diagnostic(&self, index: usize) -> Result<ResponseDiagnosticSnapshotV1, ContractError> {
        self.response
            .report
            .diagnostics
            .get(index)
            .copied()
            .map(ResponseDiagnostic::snapshot_v1)
            .ok_or(ContractError::InvalidManifest)
    }
}
fn fields(
    response: &Response,
    generated: Binding,
) -> Result<ResponseSnapshotFieldsV1<'_>, ContractError> {
    Ok(ResponseSnapshotFieldsV1 {
        generated,
        identity: response.identity,
        respondent: response.respondent,
        state: response.state,
        summary: &response.report.summary,
        confidence: response.report.confidence,
        outcome: response.report.outcome,
        manifest_count: response.manifest.len(),
        failed_work_count: response.failed_work.len(),
        diagnostic_count: response.report.diagnostics.len(),
        terminal: response
            .terminal
            .map(ResponseTerminalSnapshotV1::from_outcome)
            .transpose()?,
    })
}
fn original(value: ResponseSnapshotFieldsV1<'_>) -> ResponseIdentity {
    ResponseIdentity {
        binding: value.generated,
        ..value.identity
    }
}
fn failed(value: FailedWorkSnapshotV1) -> FailedWork {
    FailedWork {
        binding: value.binding,
        slot: value.slot,
        state: value.state,
        diagnostic: value.diagnostic,
    }
}
fn diagnostic(value: ResponseDiagnosticSnapshotV1) -> ResponseDiagnostic {
    ResponseDiagnostic {
        ledger: value.ledger,
        claim: value.claim,
        receipt: value.receipt,
        cycle: value.cycle,
        producer: value.producer,
        diagnostic: value.diagnostic,
    }
}
fn stamp(
    source: &impl ResponseSnapshotSource,
    value: ResponseSnapshotFieldsV1<'_>,
) -> Result<ReportStamp, ContractError> {
    report_stamp_body(
        original(value),
        value.respondent,
        ReportBody {
            summary: value.summary,
            confidence: value.confidence,
            outcome: value.outcome,
            manifest_count: value.manifest_count,
            manifest: (0..value.manifest_count).map(|index| source.manifest(index)),
            failed_count: value.failed_work_count,
            failed: (0..value.failed_work_count).map(|index| source.failed_work(index).map(failed)),
            diagnostic_count: value.diagnostic_count,
            diagnostics: (0..value.diagnostic_count)
                .map(|index| source.diagnostic(index).map(diagnostic)),
        },
    )
}
fn mul(left: usize, right: usize) -> Result<usize, ContractError> {
    left.checked_mul(right).ok_or(ContractError::Capacity)
}
fn work_count(value: ResponseSnapshotFieldsV1<'_>) -> Result<usize, ContractError> {
    bytes::add(value.manifest_count, value.failed_work_count)
}
fn stamp_visits(value: ResponseSnapshotFieldsV1<'_>) -> Result<usize, ContractError> {
    // Hash field framing and bytes: the scalar header is <512 units; each
    // manifest/failed/diagnostic entry is <128/512/128 respectively, including
    // its indexed callback and the terminal iterator probe. Summary is UTF-8 bytes.
    let mut cost = bytes::add(512, value.summary.len())?;
    cost = bytes::add(cost, mul(128, value.manifest_count)?)?;
    cost = bytes::add(cost, mul(512, value.failed_work_count)?)?;
    bytes::add(cost, mul(128, value.diagnostic_count)?)
}
fn inspection_visits(
    value: ResponseSnapshotFieldsV1<'_>,
    policy: &AcceptancePolicy,
) -> Result<usize, ContractError> {
    let work = work_count(value)?;
    // Fixed scalar/frame/terminal checks; per-row descriptor and immutable work
    // checks; exact slot binary searches; all duplicate/collision comparisons.
    // Pair loops include both indexed callbacks and their scalar comparisons.
    let mut cost = bytes::add(512, mul(2, value.summary.len())?)?;
    cost = bytes::add(
        cost,
        mul(work, bytes::add(512, mul(2, slot_visits(policy)?)?)?)?,
    )?;
    cost = bytes::add(cost, mul(value.diagnostic_count, 192)?)?;
    cost = bytes::add(cost, mul(mul(work, work)?, 8)?)?;
    cost = bytes::add(cost, mul(mul(work, value.diagnostic_count)?, 8)?)?;
    bytes::add(cost, stamp_visits(value)?)
}
fn state(value: ResponseSnapshotFieldsV1<'_>) -> Result<Option<ResponseOutcome>, ContractError> {
    // The lower model permits an original revision/content of zero. Preserve
    // it exactly; the native importer separately enforces its allocation and
    // content policy against the authenticated original Generated event.
    require(!value.generated.object.is_zero() && !value.identity.binding.object.is_zero())?;
    same_content(value.generated, value.identity.binding)?;
    frame(
        value.identity.binding.ledger,
        value.identity.claim,
        value.identity.receipt,
        value.identity.cycle,
        value.respondent,
    )?;
    if let Some(prior) = value.identity.prior {
        require(!prior.is_zero() && prior.0 != value.identity.binding.object.0)?;
    }
    require((value.identity.cycle == 1) == value.identity.prior.is_none())?;
    let steps = match value.state {
        ResponseState::Generated => 0,
        ResponseState::Posted => 1,
        ResponseState::Received => 2,
        ResponseState::Validating => 3,
        _ => 4,
    };
    require(
        value.generated.revision.0.checked_add(steps) == Some(value.identity.binding.revision.0),
    )?;
    let terminal = value
        .terminal
        .map(ResponseTerminalSnapshotV1::restore)
        .transpose()?;
    if let Some(ResponseOutcome::Blocked(cut)) = terminal {
        require(
            cut.cause().key().target
                == aggregation::CauseTarget::Response(TestamentId(value.identity.binding.object.0)),
        )?;
    }
    let valid = match (value.state, terminal) {
        (ResponseState::Validated, Some(ResponseOutcome::Validated { .. })) => true,
        (ResponseState::ValidationIncomplete, Some(ResponseOutcome::Blocked(cut))) => {
            cut.cause().kind() == BlockingKind::Incomplete
        }
        (ResponseState::ValidationFailed, Some(ResponseOutcome::Blocked(cut))) => {
            cut.cause().kind() == BlockingKind::Failed
        }
        (ResponseState::ValidationErrored, Some(ResponseOutcome::Blocked(cut))) => {
            cut.cause().kind() == BlockingKind::Errored
        }
        (
            ResponseState::Generated
            | ResponseState::Posted
            | ResponseState::Received
            | ResponseState::Validating,
            None,
        ) => true,
        _ => false,
    };
    require(valid)?;
    Ok(terminal)
}
fn same_frame(
    value: ResponseSnapshotFieldsV1<'_>,
    work: &WorkArtifact,
) -> Result<(), ContractError> {
    require(
        work.binding.ledger == value.identity.binding.ledger
            && work.claim == value.identity.claim
            && work.receipt == value.identity.receipt
            && work.cycle == value.identity.cycle
            && work.producer == value.respondent,
    )
}
fn checked_work(
    policy: &AcceptancePolicy,
    artifacts: &impl ResponseArtifacts,
    work: &WorkArtifact,
) -> Result<(), ContractError> {
    let source = artifacts.artifact(work.reference())?;
    let failure = if work.state == WorkArtifactState::ReceiptFailed {
        Some(
            artifacts.artifact(
                work.diagnostic
                    .ok_or(ContractError::MissingEvidence)?
                    .artifact,
            )?,
        )
    } else {
        None
    };
    let restored = WorkArtifact::hydrate_v1(
        policy,
        work.snapshot_v1()?,
        source,
        failure,
        WorkArtifact::hydration_visits(policy)?,
    )?;
    require(restored == *work)
}
fn inspect<'a>(
    source: &'a impl ResponseSnapshotSource,
    policy: &AcceptancePolicy,
    artifacts: &impl ResponseArtifacts,
    limits: ResponseLimits,
    visits: &mut VisitBudget,
) -> Result<(ResponseSnapshotFieldsV1<'a>, ReportStamp), ContractError> {
    visits.charge(1)?;
    let value = source.fields()?;
    if work_count(value)? > limits.artifacts
        || value.diagnostic_count > limits.diagnostics
        || value.summary.len() > limits.summary_bytes
    {
        return Err(ContractError::Capacity);
    }
    visits.charge(inspection_visits(value, policy)?)?;
    state(value)?;
    require(
        !value.summary.trim().is_empty()
            && (value.outcome == OutcomeKind::Complete || value.diagnostic_count != 0),
    )?;
    require(
        policy.claim().ledger == value.identity.binding.ledger
            && policy.claim().object.0 == value.identity.claim.0,
    )?;
    let mut previous = None;
    for index in 0..value.manifest_count {
        let entry = source.manifest(index)?;
        require(previous.is_none_or(|slot| slot < entry.slot) && policy.has_slot(entry.slot))?;
        previous = Some(entry.slot);
        let work = artifacts.work(entry.artifact.id)?;
        same_frame(value, work)?;
        checked_work(policy, artifacts, work)?;
        require(
            work.slot == entry.slot
                && work.reference() == entry.artifact
                && work.attachment == Some(value.generated)
                && matches!(
                    work.state,
                    WorkArtifactState::Attached
                        | WorkArtifactState::Validating
                        | WorkArtifactState::Validated
                        | WorkArtifactState::ValidationFailed
                ),
        )?;
        descriptor(
            artifacts.artifact(entry.artifact)?,
            value.identity.binding.ledger,
            entry.artifact,
            value.identity.receipt,
            value.respondent,
            WorkProvenance {
                claim: value.identity.claim,
                cycle: value.identity.cycle,
                role: WorkRole::Output { slot: entry.slot },
            },
        )?;
        for other in 0..index {
            require(source.manifest(other)?.artifact.id != entry.artifact.id)?;
        }
    }
    previous = None;
    for index in 0..value.failed_work_count {
        let entry = source.failed_work(index)?;
        require(previous.is_none_or(|slot| slot < entry.slot) && policy.has_slot(entry.slot))?;
        previous = Some(entry.slot);
        let work = artifacts.work(ArtifactId(entry.binding.object.0))?;
        same_frame(value, work)?;
        checked_work(policy, artifacts, work)?;
        FailedWork::hydrate_v1(entry, work)?;
        for other in 0..index {
            require(source.failed_work(other)?.binding.object != entry.binding.object)?;
        }
        for other in 0..value.manifest_count {
            let other = source.manifest(other)?;
            require(other.slot != entry.slot && other.artifact.id.0 != entry.binding.object.0)?;
        }
    }
    let mut previous = None;
    for index in 0..value.diagnostic_count {
        let entry = source.diagnostic(index)?;
        require(previous.is_none_or(|id| id < entry.diagnostic.artifact.id))?;
        previous = Some(entry.diagnostic.artifact.id);
        require(
            entry.ledger == value.identity.binding.ledger
                && entry.claim == value.identity.claim
                && entry.receipt == value.identity.receipt
                && entry.cycle == value.identity.cycle
                && entry.producer == value.respondent,
        )?;
        ResponseDiagnostic::hydrate_v1(
            entry,
            artifacts.artifact(entry.diagnostic.artifact)?,
            ResponseDiagnostic::HYDRATION_VISITS,
        )?;
        for other in 0..value.manifest_count {
            require(source.manifest(other)?.artifact.id != entry.diagnostic.artifact.id)?;
        }
        for other in 0..value.failed_work_count {
            let other = source.failed_work(other)?;
            if other.binding.object.0 == entry.diagnostic.artifact.id.0 {
                require(
                    other.state == WorkArtifactState::GenerationFailed
                        && other.diagnostic == entry.diagnostic
                        && other.binding.content == entry.diagnostic.artifact.hash,
                )?;
            }
        }
    }
    let stamp = stamp(source, value)?;
    require(source.fields()? == value)?;
    Ok((value, stamp))
}

pub struct ResponseHydrationPlan<'a, S, A> {
    source: &'a S,
    policy: &'a AcceptancePolicy,
    artifacts: &'a A,
    limits: ResponseLimits,
    fields: ResponseSnapshotFieldsV1<'a>,
    stamp: ReportStamp,
    charge: usize,
    heap: usize,
    allocations: usize,
    inspection: usize,
    build: usize,
}
impl Response {
    /// Exact conservative allowance shared by retained-row exporters and the
    /// checked snapshot operation. Does not inspect or rehash report buffers.
    pub fn snapshot_visits(&self) -> Result<usize, ContractError> {
        bytes::add(192, stamp_visits(fields(self, self.identity.binding)?)?)
    }
    pub fn snapshot_v1(
        &self,
        generated: Binding,
        max_visits: usize,
    ) -> Result<ResponseSnapshotV1<'_>, ContractError> {
        let fields = fields(self, generated)?;
        let mut visits = VisitBudget::new(max_visits);
        visits.charge(self.snapshot_visits()?)?;
        state(fields)?;
        let snapshot = ResponseSnapshotV1 {
            response: self,
            fields,
        };
        require(stamp(&snapshot, fields)? == self.stamp)?;
        Ok(snapshot)
    }
    pub fn prepare_hydration_v1<'a, S: ResponseSnapshotSource, A: ResponseArtifacts>(
        source: &'a S,
        policy: &'a AcceptancePolicy,
        artifacts: &'a A,
        limits: ResponseLimits,
        max_visits: usize,
    ) -> Result<ResponseHydrationPlan<'a, S, A>, ContractError> {
        let mut visits = VisitBudget::new(max_visits);
        let (fields, stamp) = inspect(source, policy, artifacts, limits, &mut visits)?;
        let heap = bytes::add(
            fields.summary.len(),
            bytes::add(
                bytes::array::<SlotBinding>(fields.manifest_count)?,
                bytes::add(
                    bytes::array::<FailedWork>(fields.failed_work_count)?,
                    bytes::array::<ResponseDiagnostic>(fields.diagnostic_count)?,
                )?,
            )?,
        )?;
        let charge = bytes::total::<Response>(heap)?;
        bytes::fits(charge, limits.construction_bytes)?;
        let allocations = bytes::add(
            bytes::add(
                usize::from(!fields.summary.is_empty()),
                usize::from(fields.manifest_count != 0),
            )?,
            bytes::add(
                usize::from(fields.failed_work_count != 0),
                usize::from(fields.diagnostic_count != 0),
            )?,
        )?;
        let inspection = max_visits
            .checked_sub(visits.remaining())
            .ok_or(ContractError::Capacity)?;
        // Scalar state restoration, capacity reconciliation, four reserves and
        // fields before/after copy; one callback and final write per fixed-width
        // row, summary copy and three complete summary equality comparisons,
        // then actual-owned reinspection.
        let copying = bytes::add(
            512,
            bytes::add(
                mul(4, fields.summary.len())?,
                mul(2, bytes::add(work_count(fields)?, fields.diagnostic_count)?)?,
            )?,
        )?;
        let build = bytes::add(inspection, copying)?;
        Ok(ResponseHydrationPlan {
            source,
            policy,
            artifacts,
            limits,
            fields,
            stamp,
            charge,
            heap,
            allocations,
            inspection,
            build,
        })
    }
}
impl<S: ResponseSnapshotSource, A: ResponseArtifacts> ResponseHydrationPlan<'_, S, A> {
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    pub fn construction_heap_bytes(&self) -> usize {
        self.heap
    }
    pub fn construction_heap_allocations(&self) -> usize {
        self.allocations
    }
    pub fn inspection_visits(&self) -> usize {
        self.inspection
    }
    pub fn build_visits(&self) -> usize {
        self.build
    }
    pub fn build(self, max_bytes: usize, max_visits: usize) -> Result<Response, ContractError> {
        bytes::fits(self.charge, max_bytes)?;
        if self.build > max_visits {
            return Err(ContractError::Capacity);
        }
        require(self.source.fields()? == self.fields)?;
        let mut summary = bytes::reserve::<u8>(self.fields.summary.len())?;
        bytes::fits(summary.capacity(), self.fields.summary.len())?;
        let mut manifest = bytes::reserve::<SlotBinding>(self.fields.manifest_count)?;
        bytes::fits(manifest.capacity(), self.fields.manifest_count)?;
        let mut failed_work = bytes::reserve::<FailedWork>(self.fields.failed_work_count)?;
        bytes::fits(failed_work.capacity(), self.fields.failed_work_count)?;
        let mut diagnostics = bytes::reserve::<ResponseDiagnostic>(self.fields.diagnostic_count)?;
        bytes::fits(diagnostics.capacity(), self.fields.diagnostic_count)?;
        let actual_heap = bytes::add(
            summary.capacity(),
            bytes::add(
                bytes::array::<SlotBinding>(manifest.capacity())?,
                bytes::add(
                    bytes::array::<FailedWork>(failed_work.capacity())?,
                    bytes::array::<ResponseDiagnostic>(diagnostics.capacity())?,
                )?,
            )?,
        )?;
        bytes::fits(
            bytes::total::<Response>(actual_heap)?,
            max_bytes
                .min(self.limits.construction_bytes)
                .min(self.charge),
        )?;
        summary.extend_from_slice(self.fields.summary.as_bytes());
        for index in 0..self.fields.manifest_count {
            manifest.push(self.source.manifest(index)?);
        }
        for index in 0..self.fields.failed_work_count {
            failed_work.push(failed(self.source.failed_work(index)?));
        }
        for index in 0..self.fields.diagnostic_count {
            diagnostics.push(diagnostic(self.source.diagnostic(index)?));
        }
        require(self.source.fields()? == self.fields)?;
        let response = Response {
            identity: self.fields.identity,
            respondent: self.fields.respondent,
            state: self.fields.state,
            manifest,
            failed_work,
            report: ReportedWork {
                summary: String::from_utf8(summary).map_err(|_| ContractError::InvalidManifest)?,
                confidence: self.fields.confidence,
                outcome: self.fields.outcome,
                diagnostics,
            },
            stamp: self.stamp,
            terminal: state(self.fields)?,
        };
        let snapshot = ResponseSnapshotV1 {
            fields: fields(&response, self.fields.generated)?,
            response: &response,
        };
        let mut visits = VisitBudget::new(self.inspection);
        let (actual_fields, actual_stamp) = inspect(
            &snapshot,
            self.policy,
            self.artifacts,
            self.limits,
            &mut visits,
        )?;
        require(actual_fields == self.fields && actual_stamp == self.stamp)?;
        bytes::fits(
            bytes::total::<Response>(response.retained_heap_bytes()?)?,
            max_bytes
                .min(self.limits.construction_bytes)
                .min(self.charge),
        )?;
        Ok(response)
    }
}
