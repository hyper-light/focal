//! Preserve the real creation/control plan while appending graph consequences.
//! The capability is built only after the corresponding checked model plan.
use super::prepare::{Extras, Scratch, add, heap, within};
use super::*;
use focal_model::lifecycle::{claim::ClaimCut, graph};

pub(super) struct ControlGraphProof {
    range: RangeId,
    prefix: SessionSeq,
    operation: NativeOperation,
    original: Vec<ClaimState>,
    events: Vec<NativeFact>,
}
impl ControlGraphProof {
    pub(super) fn originals(&self) -> &[ClaimState] {
        &self.original
    }
    pub(super) fn prefix(&self) -> &[NativeFact] {
        &self.events
    }
}

#[allow(clippy::too_many_arguments)] // A checked existing model plan and its single source prefix.
pub(super) fn prepare(
    mut rows: Vec<ClaimState>,
    view: &View<'_>,
    operation: NativeOperation,
    cut: ClaimCut,
    limits: NativeLimits,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<Vec<ClaimState>, NativeError> {
    if !matches!(
        operation,
        NativeOperation::Create | NativeOperation::Cancel | NativeOperation::Post
    ) || extras.journal.is_some()
        || extras.control_graph.is_some()
    {
        return Err(ContractError::InvalidTransition.into());
    }
    if rows.is_empty() || view.meta().monitors == 0 {
        return Ok(rows);
    }
    rows.sort_unstable_by_key(|row| row.binding().object);
    let (original, discovery) = graph_effects::control_closure(view, &rows, limits, scratch)?;
    if !original
        .iter()
        .any(|row| row.scopes().iter().any(|scope| scope.active()))
    {
        drop(original);
        return Ok(rows);
    }
    let count = add(
        crate::native::claim_changes::event_count(&rows, view, operation)?,
        extras.events(),
    )?;
    let mut events = scratch.reserve(count)?;
    let mut workspace = scratch.reserve(rows.len())?;
    crate::native::claim_changes::visit_history(
        &rows,
        extras,
        view,
        operation,
        &mut workspace,
        |fact| {
            if events.len() == events.capacity() {
                return Err(NativeError::Capacity("control original history"));
            }
            events.push(fact);
            Ok(())
        },
    )?;
    if events.len() != count {
        return Err(ContractError::InvalidManifest.into());
    }
    let mut journal = scratch.reserve(limits.range.max_batch_entries)?;
    for fact in &events {
        if journal.len() == journal.capacity() {
            return Err(NativeError::Capacity("control history"));
        }
        journal.push(*fact);
    }
    let mut changed = scratch.reserve(original.len())?;
    for row in &rows {
        let charge = heap(row)?;
        scratch.charge(charge)?;
        let copied = row.try_copy(row.retained_bytes()?)?;
        within(heap(&copied)?, charge)?;
        if changed.len() == changed.capacity() {
            return Err(ContractError::Capacity.into());
        }
        changed.push(copied);
    }
    for extra in &mut extras.rows {
        extra.fact = None;
    }
    extras.journal = Some(journal);
    let mut visits = graph::VisitBudget::new(limits.plan_edges);
    visits.charge(discovery)?;
    graph_effects::settle_scopes(
        view,
        &original,
        &mut changed,
        cut,
        limits,
        extras,
        scratch,
        &mut visits,
    )?;
    drop(original);
    extras.control_graph = Some(ControlGraphProof {
        range: view.state.rows.id(),
        prefix: view.prefix(),
        operation,
        original: rows,
        events,
    });
    Ok(changed)
}

pub(super) fn check(
    rows: &[ClaimState],
    extras: &Extras,
    view: &View<'_>,
    outcome: NativeOutcome,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let proof = extras
        .control_graph
        .as_ref()
        .ok_or(ContractError::InvalidTransition)?;
    if proof.range != view.state.rows.id()
        || proof.prefix != view.prefix()
        || proof.prefix.0.checked_add(1) != Some(outcome.sequence.0)
        || proof.operation != outcome.operation
    {
        return Err(ContractError::InvalidCut.into());
    }
    let journal = extras
        .journal
        .as_deref()
        .ok_or(ContractError::InvalidTransition)?;
    if journal.get(..proof.events.len()) != Some(proof.events.as_slice()) {
        return Err(ContractError::InvalidManifest.into());
    }
    let mut visits = graph::VisitBudget::new(limits.plan_edges);
    for source in &proof.original {
        visits.charge(add(rows.len(), 1)?)?;
        if !rows
            .iter()
            .any(|row| row.binding().object == source.binding().object)
        {
            return Err(ContractError::InvalidManifest.into());
        }
    }
    // Peers introduced by graph propagation are protected just as the original
    // control rows are. The latter use their already-checked control state as
    // baseline (including real child registrations and supersession).
    for next in rows {
        visits.charge(add(proof.original.len(), 1)?)?;
        let source = proof
            .original
            .iter()
            .find(|source| source.binding().object == next.binding().object)
            .or_else(|| view.claim(ClaimId(next.binding().object.0)))
            .ok_or(ContractError::InvalidTarget)?;
        source.binding().check(&Binding {
            revision: source.binding().revision,
            ..next.binding()
        })?;
        visits.charge(add(
            source.graph().obligations().len(),
            source.scopes().children().len(),
        )?)?;
        if next.binding().revision < source.binding().revision
            || next.created() != source.created()
            || next.graph() != source.graph()
            || next.lineage() != source.lineage()
            || next.receipt() != source.receipt()
            || next.response_count() != source.response_count()
            || next.latest_response() != source.latest_response()
            || next.local_complete() != source.local_complete()
            || next.scopes().limits() != source.scopes().limits()
            || next.scopes().children() != source.scopes().children()
            || next.scopes().release_cut() != source.scopes().release_cut()
            || (source.local_sealed_at().is_some()
                && next.local_sealed_at() != source.local_sealed_at())
            || (source.is_terminal()
                && (next.status() != source.status()
                    || next.terminal_cut() != source.terminal_cut()))
            || (!next.is_terminal() && next.terminal_cut() != source.terminal_cut())
        {
            return Err(ContractError::InvalidTransition.into());
        }
        if !source.is_terminal() && next.is_terminal() {
            use focal_model::lifecycle::claim::ClaimTerminalCut;
            let valid = match (next.status(), next.terminal_cut()) {
                (ClaimStatus::DependencyFailed, Some(ClaimTerminalCut::Graph(cut))) => {
                    cut.kind() == graph::FailureKind::DependencyFailed
                        && cut.sequence() == outcome.sequence
                        && cut.fingerprint() != ContentHash([0; 32])
                }
                (ClaimStatus::Satisfied, Some(ClaimTerminalCut::Explicit(cut))) => {
                    next.local_complete()
                        && cut.position == outcome.sequence
                        && cut.cause != ContentHash([0; 32])
                }
                _ => false,
            };
            if !valid
                || next.local_sealed_at() != source.local_sealed_at().or(Some(outcome.sequence))
            {
                return Err(ContractError::InvalidCut.into());
            }
        }
    }
    for fact in journal
        .get(proof.events.len()..)
        .ok_or(ContractError::InvalidManifest)?
    {
        visits.charge(1)?;
        let NativeFact::Claim(event) = fact else {
            return Err(ContractError::InvalidTransition.into());
        };
        if !matches!(
            event.kind,
            NativeEventKind::DependencyFailed
                | NativeEventKind::Satisfied
                | NativeEventKind::Monitor(NativeMonitorEvent::Released { .. })
        ) {
            return Err(ContractError::InvalidTransition.into());
        }
    }
    Ok(())
}
