//! A real Posted-to-PostFailed model transition precedes all graph effects.
//! Its private proof preserves the exact accepted report and intermediate claim
//! identity while monitor releases may advance the final terminal row further.
use super::prepare::{Extras, Scratch, add, heap, within};
use super::*;
use focal_model::lifecycle::{
    aggregation,
    claim::{ClaimCut, ClaimTerminalCut},
};

#[derive(Debug, Clone, Copy)]
pub(super) struct Proof {
    source: Binding,
    failed: Binding,
    terminal: ClaimTerminalCut,
    key: EvaluationKey,
    previous: validation::EvaluationState,
    next: validation::EvaluationState,
    accepted: NativeAccepted,
    artifact: Binding,
    facts: validation::EvidenceFacts,
    request: RequestKey,
    logical_time: u64,
}

pub(super) struct Failure {
    root: ClaimState,
    proof: Proof,
}

#[derive(Clone, Copy)]
pub(super) struct ReportFrame {
    pub key: EvaluationKey,
    pub previous: validation::EvaluationState,
    pub next: validation::EvaluationState,
    pub accepted: NativeAccepted,
    pub facts: validation::EvidenceFacts,
    pub request: RequestKey,
    pub logical_time: u64,
}

impl Failure {
    pub(super) fn apply(
        parent: &ClaimState,
        expected: Binding,
        decision: &aggregation::AdmissionDecision<'_>,
        frame: ReportFrame,
        cut: ClaimCut,
        scratch: &mut Scratch,
    ) -> Result<Self, NativeError> {
        parent.binding().check(&expected)?;
        if parent.status() != ClaimStatus::Posted
            || frame.key.target != EvaluationTarget::Admission
            || frame.key.claim.0 != expected.object.0
            || frame.accepted.sequence() != cut.position
            || frame.accepted.ordinal() != 2
            || frame.next.last_result() != Some(frame.accepted.result())
            || EvaluationKey::of(frame.key.claim, &frame.previous) != frame.key
            || EvaluationKey::of(frame.key.claim, &frame.next) != frame.key
            || NativeResultKey::of(frame.accepted.result()).evaluation != frame.key
            || frame.previous.binding().next()? != frame.next.binding()
            || frame.request.principal != frame.accepted.attempt().evaluator
        {
            return Err(ContractError::InvalidTransition.into());
        }
        let charge = heap(parent)?;
        scratch.charge(charge)?;
        scratch.charge(Proof::charge())?;
        let mut root = parent.try_copy(parent.retained_bytes()?)?;
        within(heap(&root)?, charge)?;
        root.apply_admission(&expected, decision)?;
        let terminal = root.terminal_cut().ok_or(ContractError::InvalidCut)?;
        let proof = Proof {
            source: expected,
            failed: root.binding(),
            terminal,
            key: frame.key,
            previous: frame.previous,
            next: frame.next,
            accepted: frame.accepted,
            artifact: frame.facts.binding,
            facts: frame.facts,
            request: frame.request,
            logical_time: frame.logical_time,
        };
        proof.check_parent(expected, parent.status(), &root)?;
        if root.local_sealed_at() != Some(cut.position) {
            return Err(ContractError::InvalidCut.into());
        }
        Ok(Self { root, proof })
    }

    pub(super) fn prepare(
        self,
        view: &View<'_>,
        cut: ClaimCut,
        limits: NativeLimits,
        extras: &mut Extras,
        scratch: &mut Scratch,
    ) -> Result<Vec<ClaimState>, NativeError> {
        if extras.admission_graph.is_some()
            || extras.control_graph.is_some()
            || extras.journal.is_some()
        {
            return Err(ContractError::InvalidTransition.into());
        }
        let prefix = self.proof.prefix();
        let rows = graph_effects::prepare_admission(
            view, self.root, cut, limits, prefix, extras, scratch,
        )?;
        extras.admission_graph = Some(self.proof);
        Ok(rows)
    }
}

impl Proof {
    pub(super) const fn charge() -> usize {
        size_of::<Self>()
    }
    pub(super) fn source(self) -> Binding {
        self.source
    }
    pub(super) fn failed(self) -> Binding {
        self.failed
    }
    pub(super) fn terminal(self) -> ClaimTerminalCut {
        self.terminal
    }
    pub(super) fn event(&self) -> NativeClaimEvent {
        NativeClaimEvent {
            graph: None,
            kind: NativeEventKind::PostFailed,
            owned_child: None,
            before: Some(self.source()),
            after: self.failed(),
            status: ClaimStatus::PostFailed,
        }
    }

    pub(super) fn check_parent(
        &self,
        before: Binding,
        status: ClaimStatus,
        final_state: &ClaimState,
    ) -> Result<(), NativeError> {
        self.source().check(&before)?;
        self.source().next()?.check(&self.failed())?;
        self.failed().check(&Binding {
            revision: self.failed().revision,
            ..final_state.binding()
        })?;
        let ClaimTerminalCut::Required(cut) = self.terminal() else {
            return Err(ContractError::InvalidCut.into());
        };
        if status != ClaimStatus::Posted
            || final_state.status() != ClaimStatus::PostFailed
            || final_state.binding().revision < self.failed().revision
            || final_state.terminal_cut() != Some(self.terminal())
            || final_state.local_sealed_at() != Some(cut.sequence())
            || cut.sequence().0 == 0
            || cut.cause().key().target != aggregation::CauseTarget::Admission
        {
            return Err(ContractError::InvalidTransition.into());
        }
        Ok(())
    }

    fn prefix(&self) -> [NativeFact; 4] {
        [
            NativeFact::Artifact {
                binding: self.artifact,
            },
            NativeFact::Evaluation {
                kind: NativeEvaluationEventKind::Reported,
                key: self.key,
                before: Some(self.previous.binding()),
                after: self.next.binding(),
                state: self.next.state(),
                phase: self.next.phase(),
                attempt: Some(self.accepted.attempt()),
                fence: self.next.fence(),
            },
            NativeFact::Accepted {
                key: NativeResultKey::of(self.accepted.result()),
            },
            NativeFact::Claim(self.event()),
        ]
    }
}

/// Promote only the actual three report facts. The graph's existing preflight
/// supplies an exact prefix-plus-consequence capacity before any journal buffer.
pub(super) fn promote(
    prefix: &[NativeFact; 4],
    capacity: usize,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<usize, NativeError> {
    if extras.journal.is_some() || extras.rows.len() != 4 || extras.events() != 3 {
        return Err(ContractError::InvalidManifest.into());
    }
    let mut facts = extras.rows.iter().filter_map(|row| row.fact);
    for expected in prefix.iter().take(3) {
        if facts.next() != Some(*expected) {
            return Err(ContractError::InvalidCut.into());
        }
    }
    if facts.next().is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    let before = scratch.used;
    let mut journal = scratch.reserve(capacity)?;
    for fact in prefix {
        if journal.len() == journal.capacity() {
            return Err(NativeError::Capacity("admission graph history"));
        }
        journal.push(*fact);
    }
    for row in &mut extras.rows {
        row.fact = None;
    }
    extras.journal = Some(journal);
    scratch
        .used
        .checked_sub(before)
        .ok_or(ContractError::Capacity.into())
}

fn staged(extras: &Extras, key: Key) -> Result<&Row, NativeError> {
    let mut matching = extras.rows.iter().filter(|row| row.key == key);
    let found = matching.next().ok_or(ContractError::MissingEvidence)?;
    if matching.next().is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(&found.row)
}

/// The complete protected source supplies immutable graph dimensions; authored
/// scope capacities cover later children and retained inactive monitor rows.
/// `events` includes the four original report/failure facts.
pub(super) fn checker_visits_bound(
    claims: &[&ClaimState],
    extras: usize,
    events: usize,
) -> Result<usize, NativeError> {
    let mut visits = add(
        claims.len(),
        extras.checked_mul(4).ok_or(ContractError::Capacity)?,
    )?;
    for claim in claims {
        let scopes = claim.scopes().limits();
        visits = add(
            visits,
            add(
                claim.graph().obligations().len(),
                add(scopes.children, add(scopes.scopes, scopes.roots)?)?,
            )?,
        )?;
    }
    add(
        visits,
        events
            .checked_sub(4)
            .and_then(|suffix| suffix.checked_mul(claims.len().checked_add(1)?))
            .ok_or(ContractError::Capacity)?,
    )
}

/// The proof was built from the actual report transition. Verify its exact
/// retained rows and original coordinates; no report is rerun or synthesized.
pub(super) fn check(
    rows: &[ClaimState],
    extras: &Extras,
    view: &View<'_>,
    outcome: NativeOutcome,
    limits: NativeLimits,
    index_rows: usize,
) -> Result<(), NativeError> {
    let proof = extras
        .admission_graph
        .as_ref()
        .ok_or(ContractError::InvalidTransition)?;
    let journal = extras
        .journal
        .as_deref()
        .ok_or(ContractError::InvalidTransition)?;
    if outcome.operation != NativeOperation::ReportAdmission
        || outcome.invocation != NativeInvocation::Request(proof.request)
        || outcome.logical_time != proof.logical_time
        || outcome.sequence != proof.accepted.sequence()
        || view.prefix().0.checked_add(1) != Some(outcome.sequence.0)
        || journal.get(..4) != Some(proof.prefix().as_slice())
        || extras.rows.len() != add(4, index_rows)?
        || extras.control_graph.is_some()
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let mut visits = limits.plan_edges;
    debit(
        &mut visits,
        add(
            rows.len(),
            extras
                .rows
                .len()
                .checked_mul(4)
                .ok_or(ContractError::Capacity)?,
        )?,
    )?;
    let parent = view
        .claim(proof.key.claim)
        .ok_or(ContractError::InvalidTarget)?;
    let root = rows
        .binary_search_by_key(&proof.source.object, |row| row.binding().object)
        .ok()
        .and_then(|index| rows.get(index))
        .ok_or(ContractError::InvalidTarget)?;
    proof.check_parent(parent.binding(), parent.status(), root)?;
    if view.evaluation(proof.key)? != &proof.previous {
        return Err(ContractError::StaleEvaluation.into());
    }
    let result_key = NativeResultKey::of(proof.accepted.result());
    let evidence = proof
        .accepted
        .result()
        .evidence()
        .ok_or(ContractError::MissingEvidence)?;
    let next = as_evaluation(Some(staged(extras, Key::Evaluation(proof.key))?))
        .ok_or(ContractError::MissingEvidence)?;
    let accepted = as_result(Some(staged(extras, Key::Accepted(result_key))?))
        .ok_or(ContractError::MissingEvidence)?;
    let artifact = as_artifact(Some(staged(extras, Key::Artifact(evidence.id))?))
        .ok_or(ContractError::MissingEvidence)?;
    if next != &proof.next
        || accepted != &proof.accepted
        || !artifact
            .facts()
            .is_some_and(|facts| same_facts(facts, proof.facts))
        || artifact.descriptor().binding() != proof.artifact
        || artifact.descriptor().id() != evidence.id
        || proof.artifact.content != evidence.hash
        || accepted.ordinal() != 2
        || !matches!(staged(extras, Key::ArtifactIdentity(proof.artifact.content))?, Row::ArtifactIdentity(id) if *id == evidence.id)
        || view.get(Key::Accepted(result_key)).is_some()
        || view.get(Key::Artifact(evidence.id)).is_some()
        || view
            .get(Key::ArtifactIdentity(proof.artifact.content))
            .is_some()
    {
        return Err(ContractError::MissingEvidence.into());
    }
    let mut previous = None;
    for next in rows {
        if previous.is_some_and(|id| id >= next.binding().object) {
            return Err(ContractError::InvalidManifest.into());
        }
        previous = Some(next.binding().object);
        let source = view
            .claim(ClaimId(next.binding().object.0))
            .ok_or(ContractError::InvalidTarget)?;
        source.binding().check(&Binding {
            revision: source.binding().revision,
            ..next.binding()
        })?;
        debit(
            &mut visits,
            add(
                source.graph().obligations().len(),
                source.scopes().children().len(),
            )?,
        )?;
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
            || (!next.is_terminal()
                && (next.status() != source.status()
                    || next.terminal_cut() != source.terminal_cut()))
        {
            return Err(ContractError::InvalidTransition.into());
        }
        // The central index replay checks real scope transitions whenever an
        // index suffix exists. Without one, no scope mutation is authorized.
        if index_rows == 0 {
            for scope in source.scopes().iter() {
                debit(&mut visits, add(1, scope.roots().len())?)?;
            }
            if next.scopes() != source.scopes() {
                return Err(ContractError::InvalidTransition.into());
            }
        }
        if source.binding().object == proof.source.object {
            continue; // The private model proof checked the initial Required cut.
        }
        if !source.is_terminal() && next.is_terminal() {
            let valid = match (next.status(), next.terminal_cut()) {
                (ClaimStatus::DependencyFailed, Some(ClaimTerminalCut::Graph(cut))) => {
                    cut.kind() == focal_model::lifecycle::graph::FailureKind::DependencyFailed
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
    for fact in journal.get(4..).ok_or(ContractError::InvalidManifest)? {
        debit(&mut visits, add(rows.len(), 1)?)?;
        let NativeFact::Claim(event) = fact else {
            return Err(ContractError::InvalidTransition.into());
        };
        let row = rows
            .binary_search_by_key(&event.after.object, |row| row.binding().object)
            .ok()
            .and_then(|index| rows.get(index))
            .ok_or(ContractError::InvalidTarget)?;
        match event.kind {
            NativeEventKind::Monitor(NativeMonitorEvent::Released { cut, .. })
                if cut.position == outcome.sequence && cut.cause != ContentHash([0; 32]) => {}
            NativeEventKind::DependencyFailed => {
                let Some(ClaimTerminalCut::Graph(cut)) = row.terminal_cut() else {
                    return Err(ContractError::InvalidCut.into());
                };
                if cut.sequence() != outcome.sequence
                    || cut.kind() != focal_model::lifecycle::graph::FailureKind::DependencyFailed
                    || cut.fingerprint() == ContentHash([0; 32])
                {
                    return Err(ContractError::InvalidCut.into());
                }
            }
            NativeEventKind::Satisfied
                if matches!(row.terminal_cut(), Some(ClaimTerminalCut::Explicit(cut))
                    if row.local_complete() && cut.position == outcome.sequence
                        && cut.cause != ContentHash([0; 32])) => {}
            _ => return Err(ContractError::InvalidTransition.into()),
        }
    }
    Ok(())
}
fn debit(visits: &mut usize, count: usize) -> Result<(), NativeError> {
    *visits = visits
        .checked_sub(count)
        .ok_or(NativeError::Capacity("admission graph history visits"))?;
    Ok(())
}

fn same_facts(a: validation::EvidenceFacts, b: validation::EvidenceFacts) -> bool {
    a.binding == b.binding
        && a.claim == b.claim
        && a.validation == b.validation
        && a.target == b.target
        && a.generation == b.generation
        && a.attempt == b.attempt
        && a.producer == b.producer
        && a.value == b.value
        && a.kind == b.kind
        && a.schema == b.schema
        && a.custody_revision == b.custody_revision
}

#[cfg(test)]
#[path = "admission_graph_tests.rs"]
mod tests;
