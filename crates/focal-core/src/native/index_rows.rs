//! Secondary index rows (doc 22 §7). Every index row is a unit value whose key
//! is derived from exactly one primary row: the claim's issuer, subject,
//! status, action, authored scopes, relations and creation prefix; the
//! artifact's producer, kind, schema and inputs; the declaration's evaluators;
//! and an accepted result's verdict. Artifacts, declarations and testaments
//! carry no creation row: their primary key ranges are already ordered and a
//! time-ordered read goes through the event log (`Events { after }`).
//! The leader derives them from the same rows it writes, replay
//! validation rederives them from the replayed primaries and requires the
//! record to carry exactly those changes, and checkpoint validation requires
//! every retained primary row to be covered and every index row to match.
//!
//! Two families change after creation. A status transition deletes the old
//! status key and writes the new one, so a scan over one status never visits
//! a claim that left it. A due-timer row exists exactly while its trusted
//! timer has not been delivered or its target is still live: a claim's
//! deadline until its timer is consumed, a monitor's deadline while the
//! monitor is active and its timer has not been consumed, and an
//! evaluation's declaration deadline while the evaluation is neither
//! terminal nor fenced and its timer has not been consumed. Consumption is
//! the retained outcome row of the timer's invocation, so a delivered timer
//! whose target did not change still deletes its row, and the node's sweep
//! over due timers never revisits a delivered one. A claim's terminal
//! transition leaves its timer row in place: the timer fires once on the
//! terminal claim, records its outcome and retires the row, so ordinary
//! transitions never pay for claim timers. All other families are written
//! once at creation.
use super::*;
use focal_memory::{Change, Entry};
use focal_model::lifecycle::artifact_descriptor::{self, ArtifactDescriptor};
use focal_model::lifecycle::claim_descriptor::ClaimDescriptor;
use focal_model::lifecycle::scope;
use focal_model::lifecycle::validation::{PhasePolicyView, ProgramView};
use focal_model::{Deadline, ObjectId, ObjectKind, RelationTarget};

/// One derived index change: a unit row to write or a stale key to delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IndexChange {
    Put(Key),
    Delete(Key),
}
impl IndexChange {
    pub(super) fn key(self) -> Key {
        match self {
            Self::Put(key) | Self::Delete(key) => key,
        }
    }
    pub(super) fn into_change(self) -> Change<Key, Row> {
        match self {
            Self::Put(key) => Change::Put(Entry::new(key, Row::Index, 0)),
            Self::Delete(key) => Change::Delete(key),
        }
    }
}

/// Whether a timer's invocation has a retained outcome before and after the
/// plan being derived. The leader answers from its view and the invocation
/// it is admitting; replay answers from the record's before and after
/// states; a checkpoint answers from its retained rows alone.
pub(super) trait TimerOutcomes {
    fn before(&self, invocation: NativeInvocation) -> Result<bool, NativeError>;
    fn after(&self, invocation: NativeInvocation) -> Result<bool, NativeError>;
}
/// No timer has been consumed: the import image and fresh derivations.
pub(super) struct NeverConsumed;
impl TimerOutcomes for NeverConsumed {
    fn before(&self, _: NativeInvocation) -> Result<bool, NativeError> {
        Ok(false)
    }
    fn after(&self, _: NativeInvocation) -> Result<bool, NativeError> {
        Ok(false)
    }
}
/// The leader's answer: outcomes retained at its prefix plus the invocation
/// this plan records.
struct Admitting<'a, 'b> {
    view: &'a View<'b>,
    current: NativeInvocation,
}
impl TimerOutcomes for Admitting<'_, '_> {
    fn before(&self, invocation: NativeInvocation) -> Result<bool, NativeError> {
        Ok(self.view.get(Key::Outcome(invocation)).is_some())
    }
    fn after(&self, invocation: NativeInvocation) -> Result<bool, NativeError> {
        Ok(invocation == self.current || self.before(invocation)?)
    }
}

/// The due-timer row of a claim's own deadline, if it has one whose timer
/// has not been consumed at the side `consumed` answers for.
fn claim_timer(
    claim: &ClaimState,
    consumed: impl FnOnce(NativeInvocation) -> Result<bool, NativeError>,
) -> Result<Option<Key>, NativeError> {
    let Some(deadline) = claim.deadline() else {
        return Ok(None);
    };
    let target = TimerTarget::Claim(ClaimId(claim.binding().object.0));
    if consumed(target.invocation(deadline))? {
        return Ok(None);
    }
    Ok(Some(Key::DueTimer(deadline.at, target)))
}
/// The due-timer row of one monitor, if it is still active and its timer has
/// not been consumed at the side `consumed` answers for.
fn monitor_timer(
    claim: ClaimId,
    scope: &scope::Scope,
    consumed: impl FnOnce(NativeInvocation) -> Result<bool, NativeError>,
) -> Result<Option<Key>, NativeError> {
    if !scope.active() {
        return Ok(None);
    }
    let target = TimerTarget::Monitor(claim, scope.id());
    if consumed(target.invocation(scope.deadline()))? {
        return Ok(None);
    }
    Ok(Some(Key::DueTimer(scope.deadline().at, target)))
}
/// Emit the change between one row's timer before and after the plan.
fn timer_change(
    before: Option<Key>,
    after: Option<Key>,
    sink: &mut dyn FnMut(IndexChange) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    match (before, after) {
        (Some(old), Some(new)) if old == new => Ok(()),
        (Some(old), Some(new)) => {
            sink(IndexChange::Delete(old))?;
            sink(IndexChange::Put(new))
        }
        (Some(old), None) => sink(IndexChange::Delete(old)),
        (None, Some(new)) => sink(IndexChange::Put(new)),
        (None, None) => Ok(()),
    }
}

/// The scope key of a scope index row: a fixed-width digest of the authored
/// key so the key space stays fixed-width regardless of the key's length.
pub fn scope_key_hash(key: &str) -> ContentHash {
    let mut hash = blake3::Hasher::new_derive_key("focal.native.index.scope-key.v1");
    hash.update(key.as_bytes());
    ContentHash(*hash.finalize().as_bytes())
}
/// The artifact kind of a kind index row, digested the same way.
pub fn artifact_kind_hash(kind: &str) -> ContentHash {
    let mut hash = blake3::Hasher::new_derive_key("focal.native.index.artifact-kind.v1");
    hash.update(kind.as_bytes());
    ContentHash(*hash.finalize().as_bytes())
}

/// Index rows one claim without authored content adds: issuer, subject,
/// creation, status and, with a deadline, its due timer.
pub(super) const CLAIM_ROWS: usize = 5;
/// The most due-timer rows one plan can change beyond its creations: one per
/// new claim (its own deadline), one per changed evaluation row and one per
/// event (every monitor disposition is one recorded event). A delivered
/// timer's own consumption is one more, added by `timer_bound` for the
/// three timer operations.
pub(super) fn timer_rows(
    new_claims: usize,
    evaluation_rows: usize,
    events: usize,
) -> Result<usize, NativeError> {
    prepare::add(prepare::add(new_claims, evaluation_rows)?, events)
}
/// The due-timer rows one operation can change, by what it can do: begins,
/// receipts, work products, diagnostics, closes, posts and audits settle no
/// timer; registrations add one per new evaluation; a report settles its own
/// evaluation's (its cohort and graph consequences are added when they are
/// known); a creation adds one per claim; a monitor registration one; the
/// three timers consume their own; everything else that can fence
/// evaluations or dispose of monitors is bounded by its extras and events.
pub(super) fn timer_bound(
    operation: NativeOperation,
    claim_rows: usize,
    extra_rows: usize,
    events: usize,
) -> Result<usize, NativeError> {
    Ok(match operation {
        NativeOperation::BeginAdmission
        | NativeOperation::BeginIncrement
        | NativeOperation::AcquireReceipt
        | NativeOperation::SubmitDiagnostic
        | NativeOperation::FailWorkProduction
        | NativeOperation::RejectWork
        | NativeOperation::ReceiveWork
        | NativeOperation::CloseResponse
        | NativeOperation::PostResponse
        | NativeOperation::SealIncrementTargets
        | NativeOperation::PostResultTestament
        | NativeOperation::GenerateResultTestament => 0,
        NativeOperation::ReportAdmission
        | NativeOperation::ReportIncrement
        | NativeOperation::ReportWork
        | NativeOperation::RegisterMonitor
        | NativeOperation::RebindMonitor => 1,
        NativeOperation::Post
        | NativeOperation::SubmitWork
        | NativeOperation::EnterWholeWork
        | NativeOperation::BeginWork => extra_rows,
        NativeOperation::Create => claim_rows,
        NativeOperation::ClaimDeadline
        | NativeOperation::EvaluationDeadline
        | NativeOperation::MonitorDeadline => prepare::add(timer_rows(0, extra_rows, events)?, 1)?,
        _ => timer_rows(0, extra_rows, events)?,
    })
}
/// Status rows a claim transition writes: the stale key's deletion and the
/// current key.
pub(super) const STATUS_ROWS: usize = 2;
/// Index rows one accepted result adds beyond its artifact.
pub(super) const ACCEPTED_ROWS: usize = 1;
/// The smallest failed admission report: eleven primary rows, a result
/// artifact without inputs (three rows), its verdict and the parent claim's
/// status move.
pub(super) const MINIMUM_FAILED_REPORT_ROWS: usize = 11 + 3 + ACCEPTED_ROWS + STATUS_ROWS;
/// The most inputs one artifact admitted under `limits` can cite: the
/// configured allowance, never above the model's fixed ceiling.
pub(super) fn input_bound(limits: NativeLimits) -> usize {
    limits.artifact_inputs.min(artifact_descriptor::MAX_INPUTS)
}
/// The most inputs a promised artifact may cite so that `fixed` other rows
/// and its own index rows still fit one batch: a small batch narrows the
/// promise instead of refusing every report.
pub(super) fn cap_inputs(inputs: usize, fixed: usize, batch: usize) -> usize {
    inputs.min(batch.saturating_sub(fixed))
}
/// Index rows one artifact with `inputs` inputs adds: producer, kind, schema
/// and one per input.
pub(super) fn artifact_rows(inputs: usize) -> Result<usize, NativeError> {
    prepare::add(3, inputs)
}
/// Index rows one report adds: its result artifact and the verdict row.
pub(super) fn report_rows(inputs: usize) -> Result<usize, NativeError> {
    prepare::add(artifact_rows(inputs)?, ACCEPTED_ROWS)
}
/// The most index rows one operation can write with `claim_rows` changed
/// claim rows, `extra_rows` other rows, `events` and artifacts of at most
/// `inputs` inputs, capped by the batch. Creation is bounded by the batch
/// alone: its scopes and relations are authored content whose index rows
/// count toward the same batch.
pub(super) fn bound(
    operation: NativeOperation,
    claim_rows: usize,
    extra_rows: usize,
    events: usize,
    inputs: usize,
    batch: usize,
) -> Result<usize, NativeError> {
    let status = prepare::add(
        prepare::add(claim_rows, claim_rows)?,
        timer_bound(operation, claim_rows, extra_rows, events)?,
    )?;
    let creation = match operation {
        NativeOperation::Create => batch,
        NativeOperation::SubmitWork
        | NativeOperation::SubmitDiagnostic
        | NativeOperation::FailWorkProduction
        | NativeOperation::RejectWork => artifact_rows(inputs)?,
        NativeOperation::ReportAdmission
        | NativeOperation::ReportIncrement
        | NativeOperation::ReportWork => report_rows(inputs)?,
        _ => 0,
    };
    Ok(prepare::add(status, creation)?.min(batch))
}

pub(super) fn is_index(key: Key) -> bool {
    matches!(
        key,
        Key::ByIssuer(..)
            | Key::BySubject(..)
            | Key::ByStatus(..)
            | Key::ByAction(..)
            | Key::ByScope(..)
            | Key::ByRelation(..)
            | Key::ByProducer(..)
            | Key::ByArtifactKind(..)
            | Key::BySchema(..)
            | Key::ArtifactInput(..)
            | Key::ByEvaluator(..)
            | Key::ByVerdict(..)
            | Key::ByCreated(..)
            | Key::DueTimer(..)
    )
}

/// The index changes one claim row implies. `before` is the claim's row at
/// the previous prefix (absent at creation) and `content` its authored body
/// when the ledger carries one; scopes and relations index only authored
/// content, so an imported projection-only claim carries the participant,
/// status and creation rows alone.
pub(super) fn claim(
    before: Option<&ClaimState>,
    after: &ClaimState,
    content: Option<&ClaimDescriptor>,
    outcomes: &dyn TimerOutcomes,
    sink: &mut dyn FnMut(IndexChange) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    let id = ClaimId(after.binding().object.0);
    if before.is_none() {
        sink(IndexChange::Put(Key::ByIssuer(after.issuer(), id)))?;
        sink(IndexChange::Put(Key::BySubject(after.subject(), id)))?;
        sink(IndexChange::Put(Key::ByCreated(
            ObjectKind::Claim.code(),
            after.created(),
            ObjectId(id.0),
        )))?;
        if let Some(content) = content {
            sink(IndexChange::Put(Key::ByAction(content.action().code(), id)))?;
            for scope in content.scopes() {
                sink(IndexChange::Put(Key::ByScope(
                    scope.kind.code(),
                    scope_key_hash(scope.key),
                    id,
                )))?;
            }
            for relation in content.relations() {
                // The target column holds the target object's identity: a
                // claim's, or an exact evidence artifact's.
                let target = match &relation.target {
                    RelationTarget::Object(target)
                        if target.kind == ObjectKind::Claim && target.id.0 != id.0 =>
                    {
                        Some(ClaimId(target.id.0))
                    }
                    RelationTarget::Evidence(evidence) => Some(ClaimId(evidence.id.0)),
                    _ => None,
                };
                if let Some(target) = target {
                    sink(IndexChange::Put(Key::ByRelation(
                        relation.kind.code(),
                        target,
                        id,
                    )))?;
                }
            }
        }
    }
    let status = after.status().code();
    match before {
        Some(old) if old.status().code() == status => {}
        Some(old) => {
            sink(IndexChange::Delete(Key::ByStatus(old.status().code(), id)))?;
            sink(IndexChange::Put(Key::ByStatus(status, id)))?;
        }
        None => sink(IndexChange::Put(Key::ByStatus(status, id)))?,
    }
    let previous = match before {
        Some(old) => claim_timer(old, |invocation| outcomes.before(invocation))?,
        None => None,
    };
    timer_change(
        previous,
        claim_timer(after, |invocation| outcomes.after(invocation))?,
        sink,
    )?;
    // Monitors the claim retained before the plan, then those it gained.
    if let Some(old) = before {
        for scope in old.scopes().iter() {
            let previous = monitor_timer(id, scope, |invocation| outcomes.before(invocation))?;
            let current = match after.scopes().monitor(scope.id()) {
                Some(scope) => monitor_timer(id, scope, |invocation| outcomes.after(invocation))?,
                None => None,
            };
            timer_change(previous, current, sink)?;
        }
    }
    for scope in after.scopes().iter() {
        if before.is_some_and(|old| old.scopes().monitor(scope.id()).is_some()) {
            continue;
        }
        let current = monitor_timer(id, scope, |invocation| outcomes.after(invocation))?;
        timer_change(None, current, sink)?;
    }
    Ok(())
}

/// The due-timer row of one evaluation at one side of a plan.
fn evaluation_timer(
    key: EvaluationKey,
    state: &validation::EvaluationState,
    deadline: Deadline,
    consumed: impl FnOnce(NativeInvocation) -> Result<bool, NativeError>,
) -> Result<Option<Key>, NativeError> {
    if state.state().is_terminal() || state.fence().is_some() {
        return Ok(None);
    }
    let target = TimerTarget::Evaluation(key);
    if consumed(target.invocation(deadline))? {
        return Ok(None);
    }
    Ok(Some(Key::DueTimer(deadline.at, target)))
}
/// The index changes one evaluation row implies against its previous state:
/// its due timer while it is live.
pub(super) fn evaluation(
    key: EvaluationKey,
    before: Option<&validation::EvaluationState>,
    after: &validation::EvaluationState,
    declaration: &validation::Declaration,
    outcomes: &dyn TimerOutcomes,
    sink: &mut dyn FnMut(IndexChange) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    let deadline = after.bind(declaration)?.deadline();
    let previous = match before {
        Some(state) => evaluation_timer(key, state, deadline, |invocation| {
            outcomes.before(invocation)
        })?,
        None => None,
    };
    let current = evaluation_timer(key, after, deadline, |invocation| {
        outcomes.after(invocation)
    })?;
    timer_change(previous, current, sink)
}

/// The index changes one new artifact row implies.
pub(super) fn artifact(
    descriptor: &ArtifactDescriptor,
    sink: &mut dyn FnMut(IndexChange) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    let id = descriptor.id();
    sink(IndexChange::Put(Key::ByProducer(descriptor.producer(), id)))?;
    sink(IndexChange::Put(Key::ByArtifactKind(
        artifact_kind_hash(descriptor.kind()),
        id,
    )))?;
    sink(IndexChange::Put(Key::BySchema(
        descriptor.schema_hash(),
        id,
    )))?;
    for input in descriptor.inputs() {
        sink(IndexChange::Put(Key::ArtifactInput(input.id, id)))?;
    }
    Ok(())
}

/// The index changes one new declaration implies.
pub(super) fn definition(
    declaration: &validation::Declaration,
    sink: &mut dyn FnMut(IndexChange) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    let id = ValidationId(declaration.binding().object.0);
    // Every principal the declaration designates: the issuer for a delivery
    // program, the check evaluator and any quality evaluator otherwise.
    let (first, second) = match declaration.program() {
        ProgramView::Delivery => (declaration.issuer(), None),
        ProgramView::Programmatic { check, quality } => {
            (check.evaluator(), quality.map(PhasePolicyView::evaluator))
        }
        ProgramView::Agentic { check } => (check.evaluator(), None),
    };
    sink(IndexChange::Put(Key::ByEvaluator(first, id)))?;
    if let Some(second) = second
        && second != first
    {
        sink(IndexChange::Put(Key::ByEvaluator(second, id)))?;
    }
    Ok(())
}

/// The index change one new accepted result implies.
pub(super) fn accepted(
    key: NativeResultKey,
    verdict: focal_model::VerdictValue,
    sink: &mut dyn FnMut(IndexChange) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    sink(IndexChange::Put(Key::ByVerdict(verdict.code(), key)))
}

/// Which primary row an index key is derived from, so a validator can fetch
/// it by exact key without scanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Primary {
    Claim(ClaimId),
    Artifact(ArtifactId),
    Definition(ValidationId),
    Result(NativeResultKey),
    Evaluation(EvaluationKey),
}
pub(super) fn primary(key: Key) -> Option<Primary> {
    Some(match key {
        Key::ByIssuer(_, claim)
        | Key::BySubject(_, claim)
        | Key::ByStatus(_, claim)
        | Key::ByAction(_, claim)
        | Key::ByScope(_, _, claim)
        | Key::ByRelation(_, _, claim) => Primary::Claim(claim),
        Key::ByProducer(_, artifact)
        | Key::ByArtifactKind(_, artifact)
        | Key::BySchema(_, artifact)
        | Key::ArtifactInput(_, artifact) => Primary::Artifact(artifact),
        Key::ByEvaluator(_, validation) => Primary::Definition(validation),
        Key::ByVerdict(_, key) => Primary::Result(key),
        Key::ByCreated(family, _, object) => match ObjectKind::from_code(family)? {
            ObjectKind::Claim => Primary::Claim(ClaimId(object.0)),
            ObjectKind::Testament | ObjectKind::Validation | ObjectKind::Artifact => {
                return None;
            }
        },
        Key::DueTimer(_, TimerTarget::Claim(claim))
        | Key::DueTimer(_, TimerTarget::Monitor(claim, _)) => Primary::Claim(claim),
        Key::DueTimer(_, TimerTarget::Evaluation(key)) => Primary::Evaluation(key),
        _ => return None,
    })
}

/// A bounded collector of derived changes that refuses to grow past `max`.
pub(super) struct Collector {
    pub(super) changes: Vec<IndexChange>,
}
impl Collector {
    pub(super) fn with_capacity(max: usize) -> Result<Self, NativeError> {
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(max)
            .map_err(|_| MemoryError::AllocationFailed)?;
        Ok(Self { changes })
    }
    pub(super) fn push(&mut self, change: IndexChange) -> Result<(), NativeError> {
        if self.changes.len() == self.changes.capacity() {
            return Err(NativeError::Capacity("index rows"));
        }
        self.changes.push(change);
        Ok(())
    }
}

/// The authored body of `id` as this plan sees it: a body written by this
/// same plan (creation) or the retained body of an existing claim.
fn content_of<'a>(
    id: ClaimId,
    view: &'a View<'_>,
    extras: &'a prepare::Extras,
) -> Option<&'a ClaimDescriptor> {
    extras
        .rows
        .iter()
        .find_map(|extra| match (extra.key, &extra.row) {
            (Key::ClaimContent(owner), Row::ClaimContent(body)) if owner == id => body.get(),
            _ => None,
        })
        .or_else(|| authored_reads::content(view.get(Key::ClaimContent(id))))
}

/// The declaration of `id` as this plan sees it: written by this plan or
/// retained.
fn declaration_of<'a>(
    id: ValidationId,
    view: &'a View<'_>,
    extras: &'a prepare::Extras,
) -> Option<&'a validation::Declaration> {
    extras
        .rows
        .iter()
        .find_map(|extra| match (extra.key, &extra.row) {
            (Key::Definition(found), Row::Definition(owned)) if found == id => owned.get(),
            _ => None,
        })
        .or_else(|| view.definition(id).ok())
}

/// Derive every index change one plan implies, in plan order: claim rows
/// against their previous state, then the new artifact, declaration, result
/// and testament rows and the changed evaluation rows among the extras, then
/// the consumption of the delivered timer when its target row did not change.
fn derive_into(
    rows: &[ClaimState],
    extras: &prepare::Extras,
    view: &View<'_>,
    invocation: NativeInvocation,
    seals: Option<&super::cohort_seals::CohortSeals>,
    limits: NativeLimits,
    sink: &mut dyn FnMut(IndexChange) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    let outcomes = Admitting {
        view,
        current: invocation,
    };
    for row in rows {
        let id = ClaimId(row.binding().object.0);
        claim(
            view.claim(id),
            row,
            content_of(id, view, extras),
            &outcomes,
            sink,
        )?;
    }
    for extra in &extras.rows {
        match (extra.key, &extra.row) {
            (Key::Evaluation(key), Row::Evaluation(owned)) => {
                // A cohort seal replaces the staged row; the record carries
                // the sealed state, so derive against that.
                let sealed = seals.and_then(|seals| seals.sealed(key));
                let value = match &sealed {
                    Some(state) => state,
                    None => owned.get().ok_or(ContractError::InvalidManifest)?,
                };
                let declaration = declaration_of(key.validation, view, extras)
                    .ok_or(ContractError::InvalidTarget)?;
                evaluation(
                    key,
                    view.evaluation(key).ok(),
                    value,
                    declaration,
                    &outcomes,
                    sink,
                )?;
            }
            (Key::Artifact(id), Row::Artifact(owned)) if view.get(Key::Artifact(id)).is_none() => {
                let value = owned.get().ok_or(ContractError::InvalidManifest)?;
                if value.descriptor().inputs().len() > input_bound(limits) {
                    return Err(NativeError::Capacity("artifact inputs"));
                }
                artifact(value.descriptor(), sink)?;
            }
            (Key::Definition(id), Row::Definition(owned))
                if view.get(Key::Definition(id)).is_none() =>
            {
                let value = owned.get().ok_or(ContractError::InvalidManifest)?;
                definition(value, sink)?;
            }
            (Key::Accepted(key), Row::Accepted(owned))
                if view.get(Key::Accepted(key)).is_none() =>
            {
                let value = owned.get().ok_or(ContractError::InvalidManifest)?;
                accepted(key, value.result().verdict(), sink)?;
            }
            _ => (),
        }
    }
    if let Some(seals) = seals {
        seals.for_each_new_sealed(&mut |key, state| {
            let declaration =
                declaration_of(key.validation, view, extras).ok_or(ContractError::InvalidTarget)?;
            evaluation(
                key,
                view.evaluation(key).ok(),
                &state,
                declaration,
                &outcomes,
                sink,
            )
        })?;
    }
    // A delivered timer whose target row is unchanged still consumes its
    // due-timer row.
    match invocation {
        NativeInvocation::MonitorDeadline(NativeMonitorDeadlineKey { claim: id, .. })
        | NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey { claim: id, .. })
            if !rows.iter().any(|row| row.binding().object.0 == id.0) =>
        {
            if let Some(state) = view.claim(id) {
                claim(
                    Some(state),
                    state,
                    content_of(id, view, extras),
                    &outcomes,
                    sink,
                )?;
            }
        }
        NativeInvocation::EvaluationDeadline(key)
            if !extras
                .rows
                .iter()
                .any(|extra| extra.key == Key::Evaluation(key.evaluation)) =>
        {
            if let Ok(state) = view.evaluation(key.evaluation) {
                let declaration = view.definition(key.evaluation.validation)?;
                evaluation(
                    key.evaluation,
                    Some(state),
                    state,
                    declaration,
                    &outcomes,
                    sink,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Count, then collect, the index changes of one plan. Two passes over the
/// same derivation keep the collector exactly sized; the count is bounded by
/// the batch limit before anything is allocated.
pub(super) fn derive_plan(
    rows: &[ClaimState],
    extras: &prepare::Extras,
    view: &View<'_>,
    invocation: NativeInvocation,
    seals: Option<&super::cohort_seals::CohortSeals>,
    limits: NativeLimits,
) -> Result<Collector, NativeError> {
    let mut count = 0usize;
    derive_into(rows, extras, view, invocation, seals, limits, &mut |_| {
        count = prepare::add(count, 1)?;
        Ok(())
    })?;
    if count > limits.range.max_batch_entries {
        return Err(NativeError::Capacity("index rows"));
    }
    let mut collector = Collector::with_capacity(count)?;
    derive_into(
        rows,
        extras,
        view,
        invocation,
        seals,
        limits,
        &mut |change| collector.push(change),
    )?;
    if collector.changes.len() != count {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(collector)
}

#[cfg(test)]
#[path = "due_timer_tests.rs"]
mod due_timer_tests;
