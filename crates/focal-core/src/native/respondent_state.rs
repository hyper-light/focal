//! Source-derived mandatory respondent reporting allowances. These scalar
//! projections are neither execution authority nor independent lifecycle state.
use super::*;
use focal_model::ObjectRevision;
use focal_model::lifecycle::{
    artifact_descriptor::{WorkProvenance, WorkRole},
    evidence::{Parent, ResponseState},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RespondentKey {
    pub(super) claim: ClaimId,
    pub(super) receipt: ReceiptId,
    pub(super) epoch: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct RespondentCredit {
    pub(super) diagnostics: u32,
    pub(super) closes: u32,
    pub(super) posts: u32,
}
impl RespondentCredit {
    pub(super) fn actions(self) -> Result<usize, NativeError> {
        let mut total = 0usize;
        for count in [self.diagnostics, self.closes, self.posts] {
            total = total
                .checked_add(usize::try_from(count).map_err(|_| ContractError::Capacity)?)
                .ok_or(ContractError::Capacity)?;
        }
        Ok(total)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RespondentSpend {
    Diagnostic,
    Close,
    Post,
}

/// Borrowed request coordinates only. They must be resolved against the actual
/// effective source; neither these values nor a checked body confer a loan.
#[derive(Debug, Clone, Copy)]
pub(super) enum RespondentRequest {
    Diagnostic { claim: Binding },
    Close { claim: Binding, response: Binding },
    Post { claim: Binding, expected: Binding },
}

impl RespondentRequest {
    pub(super) fn claim(self) -> Binding {
        match self {
            Self::Diagnostic { claim } | Self::Close { claim, .. } | Self::Post { claim, .. } => {
                claim
            }
        }
    }

    fn from_command(command: &NativeCommand) -> Option<Self> {
        match command {
            NativeCommand::SubmitDiagnostic {
                claim,
                reason: EvidenceFailure::Work,
                ..
            } => Some(Self::Diagnostic { claim: *claim }),
            NativeCommand::CloseResponse {
                claim, response, ..
            } => Some(Self::Close {
                claim: *claim,
                response: *response,
            }),
            NativeCommand::PostResponse { claim, expected } => Some(Self::Post {
                claim: *claim,
                expected: *expected,
            }),
            _ => None,
        }
    }
}

struct Visits(usize);
impl Visits {
    fn take(&mut self, count: usize) -> Result<(), NativeError> {
        self.0 = self
            .0
            .checked_sub(count)
            .ok_or(NativeError::Capacity("respondent state visits"))?;
        Ok(())
    }
}
fn within(actual: usize, maximum: usize) -> Result<(), NativeError> {
    if actual > maximum {
        Err(NativeError::Capacity("respondent state membership"))
    } else {
        Ok(())
    }
}

fn receipt(
    view: &View<'_>,
    parent: &Parent,
    fence: ReceiptFence,
    visits: &mut Visits,
) -> Result<NativeReceipt, NativeError> {
    visits.take(1)?;
    let row =
        as_receipt(view.get(Key::Receipt(fence.receipt))).ok_or(ContractError::StaleReceipt)?;
    if fence.receipt.is_zero()
        || fence.epoch == 0
        || row.claim != parent.claim
        || row.fence != fence
        || row.holder.is_zero()
        || row.acquired.0 == 0
        || row.acquired > view.prefix()
    {
        return Err(ContractError::StaleReceipt.into());
    }
    Ok(row)
}

/// Check the complete singly linked diagnostic set before allowing its first
/// Work diagnostic to replace an unspent mandatory diagnostic allowance. Fixed
/// count plus exact termination also rejects repeated links and cycles.
fn current_diagnostic(
    view: &View<'_>,
    parent: &Parent,
    closes: u32,
    limits: NativeLimits,
    visits: &mut Visits,
) -> Result<bool, NativeError> {
    visits.take(1)?;
    let cycle = match view.get(Key::Cycle(NativeCycleKey::of(parent))) {
        None => return Ok(false),
        Some(Row::Cycle(cycle)) => cycle,
        Some(_) => return Err(ContractError::InvalidManifest.into()),
    };
    if closes == 0
        || cycle.response.is_some()
        || cycle.work_head.is_some() != (cycle.work_count != 0)
        || cycle.diagnostic_head.is_some() != (cycle.diagnostic_count != 0)
    {
        return Err(ContractError::InvalidManifest.into());
    }
    within(
        cycle.work_count,
        super::response_budget::work_limit(limits)?,
    )?;
    within(cycle.diagnostic_count, limits.diagnostics_per_cycle)?;
    visits.take(
        cycle
            .work_count
            .checked_mul(3)
            .ok_or(ContractError::Capacity)?,
    )?;
    let mut next_work = cycle.work_head;
    for _ in 0..cycle.work_count {
        let id = next_work.ok_or(ContractError::InvalidManifest)?;
        let work = super::response_reads::as_work(view.get(Key::Work(id)))
            .ok_or(ContractError::MissingEvidence)?;
        let state = &work.state;
        if state.reference().id != id
            || state.binding().ledger != parent.ledger
            || state.claim() != parent.claim
            || state.receipt() != parent.receipt
            || state.cycle() != parent.next_cycle
            || state.producer() != parent.holder
            || state.attachment().is_some()
            || !matches!(
                state.state(),
                focal_model::lifecycle::evidence::WorkArtifactState::Generated
                    | focal_model::lifecycle::evidence::WorkArtifactState::Received
                    | focal_model::lifecycle::evidence::WorkArtifactState::GenerationFailed
                    | focal_model::lifecycle::evidence::WorkArtifactState::ReceiptFailed
            )
            || !matches!(view.get(Key::WorkSlot(NativeCycleKey::of(parent), state.slot())),
                Some(Row::WorkSlot(found)) if *found == id)
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let artifact =
            as_artifact(view.get(Key::Artifact(id))).ok_or(ContractError::MissingEvidence)?;
        let descriptor = artifact.descriptor();
        // Work lifecycle revisions advance independently of the immutable
        // descriptor; validate its exact content identity and provenance.
        if descriptor.id() != id
            || descriptor.content_hash() != state.binding().content
            || descriptor.ledger() != parent.ledger
            || descriptor.producer() != parent.holder
            || descriptor.receipt() != Some(parent.receipt)
        {
            return Err(ContractError::MissingEvidence.into());
        }
        next_work = work.next;
    }
    if next_work.is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    let cost = cycle
        .diagnostic_count
        .checked_mul(3)
        .ok_or(ContractError::Capacity)?;
    // Charge all bounded row lookups before entering the membership loop.
    visits.take(cost)?;
    let mut next = cycle.diagnostic_head;
    let mut work = false;
    for _ in 0..cycle.diagnostic_count {
        let id = next.ok_or(ContractError::InvalidManifest)?;
        let row = super::response_reads::as_diagnostic(view.get(Key::Diagnostic(id)))
            .ok_or(ContractError::MissingEvidence)?;
        let diagnostic = row.diagnostic;
        diagnostic.check_parent(parent)?;
        if diagnostic.artifact().id != id {
            return Err(ContractError::InvalidManifest.into());
        }
        let artifact =
            as_artifact(view.get(Key::Artifact(id))).ok_or(ContractError::MissingEvidence)?;
        let descriptor = artifact.descriptor();
        if descriptor.id() != id
            || descriptor.content_hash() != diagnostic.artifact().hash
            || descriptor.ledger() != parent.ledger
            || descriptor.producer() != parent.holder
            || descriptor.receipt() != Some(parent.receipt)
            || descriptor.kind() != "error"
            || descriptor.schema() == 0
            || descriptor.schema_hash() == ContentHash([0; 32])
            || descriptor.result_provenance().is_some()
            || descriptor.work_provenance()
                != Some(WorkProvenance {
                    claim: parent.claim,
                    cycle: parent.next_cycle,
                    role: WorkRole::Diagnostic {
                        reason: diagnostic.diagnostic().reason,
                    },
                })
            || artifact.custody().local_revision() == 0
            || !matches!(view.get(Key::ArtifactIdentity(descriptor.content_hash())),
                Some(Row::ArtifactIdentity(allocated)) if *allocated == id)
        {
            return Err(ContractError::MissingEvidence.into());
        }
        work |= diagnostic.diagnostic().reason == EvidenceFailure::Work;
        next = row.next;
    }
    if next.is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    if !work && cycle.diagnostic_count >= limits.diagnostics_per_cycle.min(limits.plan_edges) {
        return Err(NativeError::Capacity("reserved respondent diagnostic slot"));
    }
    Ok(work)
}

/// Count real Generated responses of this receipt through the claim's complete
/// descending lineage. Every earlier response also matches the exact indexed
/// claim history and immutable report stamp, including old adopted receipts.
fn generated_posts(
    view: &View<'_>,
    claim: &ClaimState,
    parent: &Parent,
    limits: NativeLimits,
    visits: &mut Visits,
) -> Result<u32, NativeError> {
    let count = claim.response_count();
    within(count, limits.responses)?;
    within(
        count,
        usize::try_from(claim.max_responses()).map_err(|_| ContractError::Capacity)?,
    )?;
    // One response, its receipt, cycle and constant-time claim-history check.
    // The receipt helper consumes its own one-visit part below.
    within(
        count.checked_mul(4).ok_or(ContractError::Capacity)?,
        visits.0,
    )?;
    visits.take(count.checked_mul(3).ok_or(ContractError::Capacity)?)?;
    let mut next = claim.latest_response().map(|link| link.testament);
    let mut cycle = u32::try_from(count).map_err(|_| ContractError::Capacity)?;
    let mut posts = 0u32;
    for _ in 0..count {
        let id = next.ok_or(ContractError::InvalidManifest)?;
        let record = super::response_reads::as_response_record(view.get(Key::Response(id)))
            .ok_or(ContractError::MissingEvidence)?;
        let response = record.response();
        let identity = response.identity();
        if identity.binding.object.0 != id.0
            || identity.binding.ledger != parent.ledger
            || identity.binding.revision.0 == 0
            || identity.binding.content == ContentHash([0; 32])
            || identity.claim != parent.claim
            || identity.cycle != cycle
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let (posted, received) = claim.recorded_response(response)?;
        let generated = response.state() == ResponseState::Generated;
        if generated == posted
            || (generated && identity.binding.revision != ObjectRevision(1))
            || (response.state() == ResponseState::Posted && received)
            || (response.state().delivered() && !received)
            || record.received().is_some() != received
            || record
                .received()
                .is_some_and(|at| at.sequence.0 == 0 || at.sequence > view.prefix())
            || record.entered().is_some_and(|at| {
                at.sequence.0 == 0
                    || at.sequence > view.prefix()
                    || record.received().is_none_or(|received| {
                        (at.sequence, at.ordinal) <= (received.sequence, received.ordinal)
                    })
            })
        {
            return Err(ContractError::InvalidCut.into());
        }
        let allocation = receipt(view, parent, identity.receipt, visits)?;
        if allocation.fence.epoch > parent.receipt.epoch
            || allocation.acquired < claim.created()
            || response.respondent() != allocation.holder
        {
            return Err(ContractError::StaleReceipt.into());
        }
        let key = NativeCycleKey {
            claim: parent.claim,
            receipt: identity.receipt.receipt,
            epoch: identity.receipt.epoch,
            cycle,
        };
        if !matches!(view.get(Key::Cycle(key)), Some(Row::Cycle(row)) if row.response == Some(id)) {
            return Err(ContractError::InvalidManifest.into());
        }
        if identity.receipt == parent.receipt {
            if allocation.holder != parent.holder {
                return Err(ContractError::StaleReceipt.into());
            }
            if generated {
                // Preserve space for the actual holder's later posting; reading
                // this responsibility never constructs an actor principal.
                identity.binding.next()?;
                posts = posts.checked_add(1).ok_or(ContractError::Capacity)?;
            }
        }
        next = identity.prior;
        cycle = cycle.checked_sub(1).ok_or(ContractError::Capacity)?;
    }
    if next.is_some() || cycle != 0 {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(posts)
}

/// Resolve the actual effective row; the supplied claim contributes only its
/// exact identity/binding. It cannot substitute mutable lifecycle fields.
pub(super) fn read(
    view: &View<'_>,
    claim: &ClaimState,
    limits: NativeLimits,
) -> Result<Option<(RespondentKey, RespondentCredit)>, NativeError> {
    let id = ClaimId(claim.binding().object.0);
    let source = view.claim(id).ok_or(ContractError::InvalidTarget)?;
    source.binding().check(&claim.binding())?;
    let claim = source;
    if claim.receipt().is_none() || claim.is_terminal() || claim.local_complete() {
        return Ok(None);
    }
    let parent = Parent::from_claim(claim)?;
    if parent.require_open_response().is_err() {
        return Ok(None);
    }
    if parent.ledger != view.ledger() {
        return Err(ContractError::WrongLedger.into());
    }
    if parent.claim != id || id.is_zero() || parent.issuer.is_zero() {
        return Err(ContractError::WrongObject.into());
    }
    if claim.created().0 == 0 || claim.created() > view.prefix() {
        return Err(ContractError::InvalidCut.into());
    }
    let mut visits = Visits(limits.plan_edges);
    visits.take(1)?;
    let allocation = receipt(view, &parent, parent.receipt, &mut visits)?;
    if allocation.holder != parent.holder || allocation.acquired < claim.created() {
        return Err(ContractError::StaleReceipt.into());
    }
    let count = u32::try_from(claim.response_count()).map_err(|_| ContractError::Capacity)?;
    let closes = claim
        .max_responses()
        .checked_sub(count)
        .ok_or(ContractError::InvalidManifest)?;
    let has_diagnostic = current_diagnostic(view, &parent, closes, limits, &mut visits)?;
    let generated = generated_posts(view, claim, &parent, limits, &mut visits)?;
    Ok(Some((
        RespondentKey {
            claim: id,
            receipt: parent.receipt.receipt,
            epoch: parent.receipt.epoch,
        },
        RespondentCredit {
            diagnostics: closes
                .checked_sub(u32::from(has_diagnostic))
                .ok_or(ContractError::InvalidManifest)?,
            closes,
            posts: closes
                .checked_add(generated)
                .ok_or(ContractError::Capacity)?,
        },
    )))
}

/// Select a possible held action only after authenticating the source actor and
/// exact target. The normal checked writer still validates every authored byte,
/// custody, manifest and mutation before any credit can be spent.
pub(super) fn spend(
    view: &View<'_>,
    claim: &ClaimState,
    context: NativeContext,
    command: &NativeCommand,
    limits: NativeLimits,
) -> Result<Option<(RespondentKey, RespondentSpend)>, NativeError> {
    let Some(request) = RespondentRequest::from_command(command) else {
        return Ok(None);
    };
    let selected = spend_request(view, claim, context, request, limits)?;
    if matches!(selected, Some((_, RespondentSpend::Diagnostic)))
        && super::work_artifacts::authorize(view, context, command, limits)?.is_none()
    {
        return Err(ContractError::InvalidTransition.into());
    }
    Ok(selected)
}

/// Allocation-free scalar selection shared with borrowed input preparation.
/// Diagnostic content/provenance and response dimensions are checked by the
/// caller before returning a usable selection; the final writer checks again.
pub(super) fn spend_request(
    view: &View<'_>,
    claim: &ClaimState,
    context: NativeContext,
    request: RespondentRequest,
    limits: NativeLimits,
) -> Result<Option<(RespondentKey, RespondentSpend)>, NativeError> {
    claim.binding().check(&request.claim())?;
    let Some((key, credit)) = read(view, claim, limits)? else {
        return Ok(None);
    };
    let source = view.claim(key.claim).ok_or(ContractError::InvalidTarget)?;
    let parent = Parent::from_claim(source)?;
    context.principal.require_actor(parent.holder)?;
    let action = match request {
        RespondentRequest::Diagnostic { .. } => {
            if credit.closes == 0 || credit.diagnostics != credit.closes {
                return Ok(None);
            }
            RespondentSpend::Diagnostic
        }
        RespondentRequest::Close { response, .. } => {
            if credit.closes == 0 {
                return Ok(None);
            }
            if response.ledger != parent.ledger {
                return Err(ContractError::WrongLedger.into());
            }
            if response.object.is_zero()
                || response.revision != ObjectRevision(1)
                || response.content.0 == [0; 32]
                || view
                    .get(Key::Response(TestamentId(response.object.0)))
                    .is_some()
                || view
                    .get(Key::ResultTestament(TestamentId(response.object.0)))
                    .is_some()
            {
                return Err(ContractError::InvalidTarget.into());
            }
            RespondentSpend::Close
        }
        RespondentRequest::Post { expected, .. } => {
            let response = super::response_reads::as_response(
                view.get(Key::Response(TestamentId(expected.object.0))),
            )
            .ok_or(ContractError::InvalidTarget)?;
            source.recorded_response(response)?;
            response.plan_post(&expected, &parent, context.principal)?;
            RespondentSpend::Post
        }
    };
    Ok(Some((key, action)))
}
