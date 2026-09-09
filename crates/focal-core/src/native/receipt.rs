//! First receipt acquisition resolves complete stored eligibility at one prefix.
//! A receipt is responsibility, not a respondent report or acceptance verdict.
use super::admission_view::AdmissionRows;
use super::prepare::{Extra, Extras, Scratch, heap};
use super::*;
use focal_model::lifecycle::{aggregation, claim::ClaimCut, graph};
use focal_model::{Cause, WaitPredicate};

struct Closure<'a> {
    claims: Vec<&'a ClaimState>,
    seen: Vec<ClaimId>,
    remaining: usize,
    max: usize,
}
impl<'a> Closure<'a> {
    fn visit(&mut self) -> Result<(), NativeError> {
        self.remaining = self
            .remaining
            .checked_sub(1)
            .ok_or(NativeError::Capacity("receipt graph visits"))?;
        Ok(())
    }
    fn insert(&mut self, view: &'a View<'_>, id: ClaimId) -> Result<(), NativeError> {
        self.visit()?;
        let position = match self.seen.binary_search(&id) {
            Ok(_) => return Ok(()),
            Err(index) => index,
        };
        if self.claims.len() >= self.max
            || self.claims.len() == self.claims.capacity()
            || self.seen.len() == self.seen.capacity()
            || position > self.seen.len()
        {
            return Err(NativeError::Capacity("receipt graph nodes"));
        }
        let claim = view.claim(id).ok_or(ContractError::InvalidTarget)?;
        if claim.binding().ledger != view.ledger() {
            return Err(ContractError::WrongLedger.into());
        }
        if claim.created().0 == 0 || claim.created() > view.prefix() {
            return Err(ContractError::InvalidCut.into());
        }
        // binary_search supplies a valid insertion point and both buffers were
        // reserved before discovery. Neither operation can grow their allocation.
        self.seen.insert(position, id);
        self.claims.push(claim);
        Ok(())
    }
}

/// Follow only actual outgoing declarations, active runtime roots and owned
/// registries. The ledger's unrelated rows are never scanned or copied.
fn closure<'a>(
    view: &'a View<'_>,
    root: ClaimId,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<Vec<&'a ClaimState>, NativeError> {
    let mut closure = Closure {
        claims: scratch.reserve(limits.plan_nodes)?,
        seen: scratch.reserve(limits.plan_nodes)?,
        remaining: limits.plan_edges,
        max: limits.plan_nodes,
    };
    closure.insert(view, root)?;
    let mut cursor = 0usize;
    while let Some(parent) = closure.claims.get(cursor).copied() {
        let parent_id = ClaimId(parent.binding().object.0);
        for obligation in parent.graph().obligations() {
            closure.insert(view, obligation.target)?;
        }
        for scope in parent.scopes().iter() {
            closure.visit()?;
            if !scope.active() {
                continue;
            }
            for root in scope.roots() {
                let target = match *root {
                    WaitPredicate::Satisfied(id)
                    | WaitPredicate::Terminal(id)
                    | WaitPredicate::Released(id) => id,
                };
                closure.insert(view, target)?;
            }
        }
        for owned in parent.scopes().children() {
            let child = view.claim(owned.id()).ok_or(ContractError::InvalidTarget)?;
            let actual = child.binding();
            owned.binding().check(&Binding {
                revision: owned.binding().revision,
                ..actual
            })?;
            if actual.revision < owned.binding().revision {
                return Err(ContractError::StaleRevision.into());
            }
            if child.lineage().cause() != &Cause::Claim(parent_id) {
                return Err(ContractError::InvalidTarget.into());
            }
            if owned.registered() != child.created() || child.created() < parent.created() {
                return Err(ContractError::InvalidCut.into());
            }
            closure.insert(view, owned.id())?;
        }
        cursor = cursor
            .checked_add(1)
            .ok_or(NativeError::Capacity("receipt graph cursor"))?;
    }
    closure
        .claims
        .sort_unstable_by_key(|claim| claim.binding().object);
    Ok(closure.claims)
}

fn peers<'a>(
    output: &mut Vec<&'a ClaimState>,
    claims: &[&'a ClaimState],
    target: ClaimId,
) -> Result<(), NativeError> {
    output.clear();
    for claim in claims {
        if claim.binding().object.0 == target.0 {
            continue;
        }
        if output.len() == output.capacity() {
            return Err(NativeError::Capacity("receipt graph peers"));
        }
        output.push(*claim);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Shared bounded owner transaction context.
pub(super) fn prepare(
    expected: Binding,
    receipt: ReceiptId,
    context: NativeContext,
    cut: ClaimCut,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let id = ClaimId(expected.object.0);
    let old = view.claim(id).ok_or(ContractError::InvalidTarget)?;
    old.binding().check(&expected)?;
    context.principal.require_actor(old.subject())?;
    if old.status() != ClaimStatus::Posted || old.receipt().is_some() {
        return Err(ContractError::InvalidTransition.into());
    }
    super::response_budget::check_receipt_shape(
        super::response_budget::delivery_count(old, limits)?,
        super::work_checks::count(old, limits)?,
        limits,
    )?;
    super::response_budget::work_limit(limits)?;
    super::response_budget::check_increment_shape(
        super::increments::count(old, limits)?,
        0,
        limits,
    )?;
    if old
        .deadline()
        .is_some_and(|deadline| context.logical_time >= deadline.at)
    {
        return Err(ContractError::InvalidTransition.into());
    }
    if receipt.is_zero() || view.get(Key::Receipt(receipt)).is_some() {
        return Err(ContractError::StaleReceipt.into());
    }
    let registry = view
        .owned_claim(id)?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    super::response_budget::check_registration_capacity(old, registry, limits)?;
    let admission_rows = AdmissionRows {
        view,
        sequence: view.prefix(),
        claim: id,
        report: None,
    };
    let admission = aggregation::project_admission(
        old,
        registry,
        &admission_rows,
        aggregation::AdmissionLimits {
            declarations: limits.definitions,
            evaluations: limits.evaluations_per_claim,
            visits: limits.plan_edges,
        },
    )?;
    if admission.outcome() != aggregation::AdmissionOutcome::Passed {
        return Err(ContractError::InvalidTransition.into());
    }
    let claims = closure(view, id, limits, scratch)?;
    let plan = graph::Snapshot::prepare_capture(
        &claims,
        graph::Limits {
            nodes: limits.plan_nodes,
            edges: limits.plan_edges,
            visits: limits.plan_edges,
        },
        scratch.remaining()?,
    )?;
    let charge = plan.construction_charge();
    scratch.charge(charge)?;
    let graph = plan.build()?;
    if graph.retained_charge()? > charge {
        return Err(NativeError::Capacity("receipt graph capacity"));
    }
    graph.check_cut(view.prefix())?;
    let start = graph.start(id)?;
    let peer_count = claims
        .len()
        .checked_sub(1)
        .ok_or(ContractError::InvalidTarget)?;
    let mut peer_rows = scratch.reserve::<&ClaimState>(peer_count)?;
    let mut rows = scratch.reserve::<ClaimState>(claims.len())?;
    // The least-fixpoint witness may establish a dependency's satisfaction before
    // its stored status advances. Publish those actual checked consequences in
    // this same candidate; never grant receipt from an unrecorded success flag.
    for source in &claims {
        let source_id = ClaimId(source.binding().object.0);
        if source.status() == ClaimStatus::Satisfied || !graph.satisfied(source_id)? {
            continue;
        }
        let release = graph.release(source_id)?;
        peers(&mut peer_rows, &claims, source_id)?;
        scratch.charge(heap(source)?)?;
        let mut changed = source.try_copy(source.retained_bytes()?)?;
        changed.graph_release(&source.binding(), &release, &peer_rows, cut.position)?;
        rows.push(changed);
    }
    peers(&mut peer_rows, &claims, id)?;
    scratch.charge(heap(old)?)?;
    let mut claim = old.try_copy(old.retained_bytes()?)?;
    let fence = ReceiptFence { receipt, epoch: 1 };
    claim.acquire_receipt(
        &expected,
        context.principal,
        fence,
        &admission,
        &start,
        &peer_rows,
    )?;
    let after = claim.binding();
    rows.push(claim);
    transactions::increment(&mut meta.receipts, 1, limits.receipts, "receipts")?;
    extras.push(Extra {
        key: Key::Receipt(receipt),
        row: Row::Receipt(NativeReceipt {
            claim: id,
            fence,
            holder: old.subject(),
            acquired: cut.position,
        }),
        heap: 0,
        fact: Some(NativeFact::Receipt {
            claim: after,
            fence,
            holder: old.subject(),
        }),
    })?;
    Ok(transactions::Plan {
        rows,
        registry: transactions::RegistryOverrides::new(),
        created: 0,
    })
}
