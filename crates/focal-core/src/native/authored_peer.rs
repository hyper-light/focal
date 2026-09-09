//! Peer follow-up rules (R5): how a correction cites the challenge it
//! invalidates and the failed verdict it rests on, and how a consultation's
//! follow-ups stay within the authored policy of the consultation they refine.
//! The rules read only committed facts of this ledger and allocate nothing;
//! the leader applies them at admission and every replica at replay.

use super::*;
use focal_model::lifecycle::validation::AcceptedResult;
use focal_model::{ActionType, ArtifactRef, Escalation, PeerPolicy, RelationKind, VerdictValue};

/// What a correction cites: the challenge it invalidates and the failed
/// verdict evidence it reviews, as authored in the descriptor's relations.
struct Citation {
    challenge: Option<ClaimId>,
    challenges: usize,
    evidence: Option<ArtifactRef>,
}

fn citation(content: &ClaimDescriptor, visits: &mut VisitBudget) -> Result<Citation, NativeError> {
    let mut cited = Citation {
        challenge: None,
        challenges: 0,
        evidence: None,
    };
    for relation in content.relations() {
        visits.charge(1)?;
        match (relation.kind, &relation.target) {
            (RelationKind::Invalidates, RelationTarget::Object(target)) => {
                cited.challenges = add(cited.challenges, 1)?;
                cited.challenge = Some(ClaimId(target.id.0));
            }
            (RelationKind::Invalidates, _) => return Err(ContractError::InvalidTarget.into()),
            (RelationKind::Reviews, RelationTarget::Evidence(evidence)) => {
                if cited.evidence.replace(*evidence).is_some() {
                    // One correction rests on exactly one cited verdict.
                    return Err(ContractError::InvalidTarget.into());
                }
            }
            _ => {}
        }
    }
    Ok(cited)
}

/// The committed follow-up policy of `claim`, if it was authored with one.
fn policy(view: &View<'_>, claim: ClaimId) -> Option<PeerPolicy> {
    super::super::authored_reads::content(view.get(Key::ClaimContent(claim)))
        .and_then(|content| content.policy())
}

/// Committed claims of `action` that relate to `target` through `kind`,
/// counted from the relation index; each visited row is charged.
fn related_count(
    view: &View<'_>,
    kind: RelationKind,
    target: ClaimId,
    action: ActionType,
    visits: &mut VisitBudget,
) -> Result<usize, NativeError> {
    let mut count = 0usize;
    for child in view.relation_sources(kind, target) {
        visits.charge(1)?;
        let related = super::super::authored_reads::content(view.get(Key::ClaimContent(child)))
            .ok_or(ContractError::InvalidPolicy)?;
        if related.action() == action {
            count = add(count, 1)?;
        }
    }
    Ok(count)
}

/// Claims of `action` in the same batch that relate to `target` through
/// `kind`, `content` itself included.
fn batch_count<'a>(
    batch: impl Iterator<Item = &'a ClaimDescriptor>,
    kind: RelationKind,
    target: ClaimId,
    action: ActionType,
    visits: &mut VisitBudget,
) -> Result<usize, NativeError> {
    let mut count = 0usize;
    for sibling in batch {
        visits.charge(1)?;
        if sibling.action() != action {
            continue;
        }
        for relation in sibling.relations() {
            visits.charge(1)?;
            if relation.kind == kind
                && matches!(&relation.target, RelationTarget::Object(object) if object.id.0 == target.0)
            {
                count = add(count, 1)?;
                break;
            }
        }
    }
    Ok(count)
}

/// Whether `principal` may author a follow-up of `claim` under `escalation`:
/// its issuer always; its current receipt holder unless follow-ups are
/// reserved to the issuer; and, for a correction, the evaluator who reported
/// the cited verdict when the policy escalates to evaluators.
fn authorized(
    principal: Principal,
    claim: &ClaimState,
    escalation: Escalation,
    reporter: Option<ParticipantId>,
) -> bool {
    let Principal::Actor(actor) = principal else {
        return false;
    };
    actor == claim.issuer()
        || (escalation != Escalation::None
            && claim
                .receipt()
                .is_some_and(|receipt| receipt.holder == actor))
        || (escalation == Escalation::Evaluator && reporter == Some(actor))
}

/// The terminal negative verdict of `challenge` that `evidence` reports, at
/// the challenge's current registration generation.
fn cited_verdict(
    view: &View<'_>,
    challenge: ClaimId,
    evidence: ArtifactRef,
    visits: &mut VisitBudget,
) -> Result<AcceptedResult, NativeError> {
    let mut found = None;
    for (key, state) in view.evaluations_of(challenge) {
        visits.charge(1)?;
        let Some(result) = state.last_result() else {
            continue;
        };
        if result.evidence() != Some(evidence) {
            continue;
        }
        if !matches!(
            result.verdict(),
            VerdictValue::Fail | VerdictValue::Incomplete | VerdictValue::Error
        ) || !state.state().is_terminal()
        {
            // A passing verdict, or an error the evaluator may still retry,
            // is no ground for a correction.
            return Err(ContractError::InvalidTransition.into());
        }
        found = Some((key, result));
        break;
    }
    let (key, result) = found.ok_or(ContractError::MissingEvidence)?;
    // The verdict must belong to the challenge's current registration: a
    // re-registered (adopted) challenge is judged again before it is corrected.
    let registry = view
        .owned_claim(challenge)?
        .registrations()
        .ok_or(ContractError::StaleEvaluation)?;
    let current = registry
        .rows()
        .iter()
        .any(|row| super::super::transactions::key_for_registered(challenge, *row) == key);
    visits.charge(registry.rows().len())?;
    if !current {
        return Err(ContractError::StaleEvaluation.into());
    }
    Ok(result)
}

/// Checks one created claim's peer rules against the committed prefix and
/// its batch siblings. `batch` yields every claim of the batch, `content`
/// included.
pub(super) fn check_one<'a>(
    principal: Principal,
    content: &ClaimDescriptor,
    batch: impl Iterator<Item = &'a ClaimDescriptor> + Clone,
    view: &View<'_>,
    visits: &mut VisitBudget,
) -> Result<(), NativeError> {
    let cited = citation(content, visits)?;
    match content.action() {
        ActionType::Correction => {
            // A correction invalidates exactly one committed challenge whose
            // authored policy allows it, rests on that challenge's terminal
            // failed verdict, and is authored by an authorized participant.
            if cited.challenges != 1 {
                return Err(ContractError::InvalidTarget.into());
            }
            let challenge = cited.challenge.ok_or(ContractError::InvalidTarget)?;
            let state = view.claim(challenge).ok_or(ContractError::InvalidTarget)?;
            let challenged =
                super::super::authored_reads::content(view.get(Key::ClaimContent(challenge)))
                    .ok_or(ContractError::InvalidTarget)?;
            if challenged.action() != ActionType::Challenge {
                return Err(ContractError::InvalidTarget.into());
            }
            let policy = challenged
                .policy()
                .filter(|policy| policy.corrective_allowed)
                .ok_or(ContractError::InvalidPolicy)?;
            let evidence = cited.evidence.ok_or(ContractError::MissingEvidence)?;
            let verdict = cited_verdict(view, challenge, evidence, visits)?;
            if !authorized(principal, state, policy.escalation, verdict.reporter()) {
                return Err(ContractError::WrongActor.into());
            }
            if policy.single_issuer {
                let committed = related_count(
                    view,
                    RelationKind::Invalidates,
                    challenge,
                    ActionType::Correction,
                    visits,
                )?;
                let batched = batch_count(
                    batch.clone(),
                    RelationKind::Invalidates,
                    challenge,
                    ActionType::Correction,
                    visits,
                )?;
                if add(committed, batched)? > 1 {
                    return Err(ContractError::ConflictingCause.into());
                }
            }
        }
        _ => {
            if cited.challenges != 0 {
                // Only a correction invalidates.
                return Err(ContractError::InvalidTarget.into());
            }
        }
    }
    if content.action() == ActionType::Consultation {
        // A follow-up consultation refines the consultation it continues;
        // the refined claim's authored policy bounds and authorizes it.
        for relation in content.relations() {
            visits.charge(1)?;
            let (RelationKind::Refines, RelationTarget::Object(target)) =
                (relation.kind, &relation.target)
            else {
                continue;
            };
            let parent = ClaimId(target.id.0);
            let Some(policy) = policy(view, parent) else {
                continue;
            };
            let state = view.claim(parent).ok_or(ContractError::InvalidTarget)?;
            if !authorized(principal, state, policy.escalation, None) {
                return Err(ContractError::WrongActor.into());
            }
            let committed = related_count(
                view,
                RelationKind::Refines,
                parent,
                ActionType::Consultation,
                visits,
            )?;
            let batched = batch_count(
                batch.clone(),
                RelationKind::Refines,
                parent,
                ActionType::Consultation,
                visits,
            )?;
            if add(committed, batched)? > usize::from(policy.max_follow_ups) {
                return Err(ContractError::InvalidPolicy.into());
            }
        }
    }
    Ok(())
}

/// Admission: every claim of the batch against the effective prefix. Each
/// claim's citation walk and index scans are bounded by `plan_edges` of their
/// own, like every other bounded owner scan; they do not draw on the exact
/// creation allowance the plan is quoted against.
pub(super) fn check_batch(
    principal: Principal,
    claims: &[NativeAuthoredProposal],
    view: &View<'_>,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    for claim in claims {
        let mut visits = VisitBudget::new(limits.plan_edges);
        check_one(
            principal,
            &claim.content,
            claims.iter().map(|claim| &claim.content),
            view,
            &mut visits,
        )?;
    }
    Ok(())
}
