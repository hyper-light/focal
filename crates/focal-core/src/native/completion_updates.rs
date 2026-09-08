//! Candidate-local completion journal. All keys come from actual evaluation
//! events, sorted by key and original ordinal; no whole-book scan occurs. Live
//! grants use pinned registration ordinals; other event keys share one bounded
//! source-registry pass per affected claim. Collector loops, sort comparisons
//! and moves all consume the same event visit allowance before book mutation.
//! Automatic preparation supplies a precharged list of changed model
//! capabilities. Unchanged capabilities are deliberately not accepted here.
use super::*;
use focal_model::lifecycle::validation::{EvaluationState, SealTransition};

#[derive(Debug, Clone, Copy)]
pub(in crate::native) struct ReportAdvance {
    pub(in crate::native) key: EvaluationKey,
    pub(in crate::native) before: Binding,
    pub(in crate::native) usage: CompletionUse,
}

/// The checked owner chooses this after obtaining its actual report/deadline
/// loan. Held loans always borrow this book's one completion pool; external
/// controls retain their original source and lane without cloning a budget.
pub(in crate::native) enum JournalFunding<'a> {
    HeldCompletion,
    External {
        source: &'a MemoryBudget,
        lane: BudgetLane,
    },
}

#[derive(Clone, Copy)]
struct Frame {
    ordinal: u32,
    key: EvaluationKey,
    kind: NativeEvaluationEventKind,
    before: Binding,
    after: Binding,
    state: validation::State,
    phase: validation::Phase,
    attempt: Option<validation::Attempt>,
    fence: Option<validation::AuthorityFence>,
}

struct Parent<'a> {
    claim: &'a ClaimState,
    registry: &'a RegistrationSet,
}

fn take(visits: &mut usize, count: usize) -> Result<(), NativeError> {
    *visits = visits
        .checked_sub(count)
        .ok_or(NativeError::Capacity("completion event visits"))?;
    Ok(())
}

fn swap(
    rows: &mut [Update],
    left: usize,
    right: usize,
    visits: &mut usize,
) -> Result<(), NativeError> {
    take(visits, 1)?;
    let a = *rows.get(left).ok_or(ContractError::Capacity)?;
    let b = *rows.get(right).ok_or(ContractError::Capacity)?;
    *rows.get_mut(left).ok_or(ContractError::Capacity)? = b;
    *rows.get_mut(right).ok_or(ContractError::Capacity)? = a;
    Ok(())
}

fn order(
    rows: &[Update],
    left: usize,
    right: usize,
    visits: &mut usize,
) -> Result<std::cmp::Ordering, NativeError> {
    take(visits, 1)?;
    let a = rows.get(left).ok_or(ContractError::Capacity)?;
    let b = rows.get(right).ok_or(ContractError::Capacity)?;
    Ok((a.key, a.ordinal).cmp(&(b.key, b.ordinal)))
}

fn sift(
    rows: &mut [Update],
    mut root: usize,
    end: usize,
    visits: &mut usize,
) -> Result<(), NativeError> {
    loop {
        let left = root
            .checked_mul(2)
            .and_then(|index| index.checked_add(1))
            .ok_or(ContractError::Capacity)?;
        if left >= end {
            return Ok(());
        }
        let right = left.checked_add(1).ok_or(ContractError::Capacity)?;
        let child = if right < end && order(rows, left, right, visits)?.is_lt() {
            right
        } else {
            left
        };
        if !order(rows, root, child, visits)?.is_lt() {
            return Ok(());
        }
        swap(rows, root, child, visits)?;
        root = child;
    }
}

// A fallible in-place heap sort permits refusal at the actual comparison/move
// limit. Sorting only mutates this newly reserved candidate buffer, never grants.
fn sort(rows: &mut [Update], visits: &mut usize) -> Result<(), NativeError> {
    let length = rows.len();
    let mut root = length.checked_div(2).ok_or(ContractError::Capacity)?;
    while root != 0 {
        root = root.checked_sub(1).ok_or(ContractError::Capacity)?;
        sift(rows, root, length, visits)?;
    }
    let mut end = length;
    while end > 1 {
        end = end.checked_sub(1).ok_or(ContractError::Capacity)?;
        swap(rows, 0, end, visits)?;
        sift(rows, 0, end, visits)?;
    }
    Ok(())
}

fn lower_bound(
    rows: &[Update],
    key: EvaluationKey,
    visits: &mut usize,
) -> Result<usize, NativeError> {
    let mut left = 0usize;
    let mut right = rows.len();
    while left < right {
        take(visits, 1)?;
        let half = right
            .checked_sub(left)
            .and_then(|n| n.checked_div(2))
            .ok_or(ContractError::Capacity)?;
        let middle = left.checked_add(half).ok_or(ContractError::Capacity)?;
        if rows.get(middle).ok_or(ContractError::Capacity)?.key < key {
            left = middle.checked_add(1).ok_or(ContractError::Capacity)?;
        } else {
            right = middle;
        }
    }
    Ok(left)
}

// Called once per sorted claim group. Live grants retain their checked physical
// registration ordinal. Other keys are resolved together in one registry pass;
// missing or duplicate membership never becomes an audit-only authorization.
fn parent<'a>(
    source: &'a View<'_>,
    rows: &mut [Update],
    visits: &mut usize,
) -> Result<Parent<'a>, NativeError> {
    let id = rows
        .first()
        .ok_or(ContractError::InvalidManifest)?
        .key
        .claim;
    let claim = source.claim(id).ok_or(ContractError::InvalidTarget)?;
    let registry = source
        .owned_claim(id)?
        .registrations()
        .ok_or(ContractError::InvalidTarget)?;
    // Each immutable slot check has exactly one declaration. Include both the
    // declaration and check fingerprint passes before traversing that policy.
    let policy_visits = add(
        add(
            claim.acceptance().declarations().len(),
            claim.acceptance().declarations().len(),
        )?,
        add(claim.acceptance().slot_count(), 1)?,
    )?;
    take(visits, policy_visits)?;
    registry.check(claim)?;
    let mut lookup = false;
    for row in rows.iter() {
        take(visits, 1)?;
        if row.key.claim != id {
            return Err(ContractError::InvalidManifest.into());
        }
        lookup |= row.before.remaining_reports == 0;
    }
    if lookup {
        for (index, registered) in registry.rows().iter().enumerate() {
            take(visits, 1)?;
            let key = transactions::key_for_registered(id, *registered);
            let at = lower_bound(rows, key, visits)?;
            if let Some(row) = rows
                .get_mut(at)
                .filter(|row| row.key == key && row.before.remaining_reports == 0)
                && row.registration_index.replace(index).is_some()
            {
                return Err(ContractError::InvalidManifest.into());
            }
        }
    }
    Ok(Parent { claim, registry })
}

fn frame(
    prepared: &NativePrepared,
    ordinal: u32,
    begin: Option<&prepare::BeginTransition<'_>>,
) -> Result<Option<Frame>, NativeError> {
    let event = CompletionBook::event(prepared, ordinal)?;
    if event.sequence != prepared.outcome.sequence
        || event.ordinal != ordinal
        || event.invocation != prepared.outcome.invocation
    {
        return Err(ContractError::InvalidCut.into());
    }
    let NativeFact::Evaluation {
        kind,
        key,
        before,
        after,
        state,
        phase,
        attempt,
        fence,
    } = event.fact
    else {
        return Ok(None);
    };
    if kind == NativeEvaluationEventKind::Begun && begin.is_none_or(|begin| begin.key() != key) {
        // Suppression is also an authorized Begin outcome. An absent attempt
        // never excuses a missing proof or permits an unchecked state change.
        // Structural missing-target entry has its own event kind.
        return Err(ContractError::InvalidTransition.into());
    }
    let collected = match kind {
        NativeEvaluationEventKind::Reported
        | NativeEvaluationEventKind::AuthorityFenced
        | NativeEvaluationEventKind::Sealed
        | NativeEvaluationEventKind::MissingTarget => true,
        NativeEvaluationEventKind::Begun => begin.is_some_and(|begin| begin.key() == key),
        _ => false,
    };
    if !collected {
        return Ok(None);
    }
    Ok(Some(Frame {
        ordinal,
        kind,
        key,
        before: before.ok_or(ContractError::InvalidCut)?,
        after,
        state,
        phase,
        attempt,
        fence,
    }))
}

type BindingOrder = (
    LedgerId,
    focal_model::ObjectId,
    ContentHash,
    focal_model::ObjectRevision,
);

fn binding_order(binding: Binding) -> BindingOrder {
    (
        binding.ledger,
        binding.object,
        binding.content,
        binding.revision,
    )
}

// Keep complete target bindings: lookup IDs alone would discard the pinned
// content/revision of an actual response or product. This is private ordering,
// not a serialized discriminant or a new model identity.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum TargetOrder {
    Admission(BindingOrder),
    Increment(BindingOrder, BindingOrder),
    Artifact(BindingOrder, u32, BindingOrder),
    MissingSlot(BindingOrder, u32),
    Delivery(BindingOrder),
}

fn target_order(target: validation::Target) -> TargetOrder {
    use validation::Target;
    match target {
        Target::Admission { claim } => TargetOrder::Admission(binding_order(claim)),
        Target::Increment { claim, artifact } => {
            TargetOrder::Increment(binding_order(claim), binding_order(artifact))
        }
        Target::Artifact {
            response,
            slot,
            artifact,
        } => TargetOrder::Artifact(binding_order(response), slot, binding_order(artifact)),
        Target::MissingSlot { response, slot } => {
            TargetOrder::MissingSlot(binding_order(response), slot)
        }
        Target::Delivery { response } => TargetOrder::Delivery(binding_order(response)),
    }
}

fn seal_order(seal: &SealTransition) -> (BindingOrder, TargetOrder, u64) {
    let previous = seal.previous();
    (
        binding_order(seal.before()),
        target_order(previous.target()),
        previous.generation(),
    )
}

/// Shared canonical order for the funded writer and borrowed collector index.
/// Callers performing a sort account for every comparison and move themselves.
pub(in crate::native) fn compare_seals(
    left: &SealTransition,
    right: &SealTransition,
) -> std::cmp::Ordering {
    seal_order(left).cmp(&seal_order(right))
}

/// Borrowed, once-validated index over the caller's already charged tokens.
struct SealIndex<'a> {
    rows: &'a [SealTransition],
}

impl<'a> SealIndex<'a> {
    fn new(rows: &'a [SealTransition], visits: &mut usize) -> Result<Self, NativeError> {
        let mut previous = None;
        for seal in rows {
            take(visits, 1)?;
            let order = seal_order(seal);
            if !seal.changed() || previous.as_ref().is_some_and(|old| old >= &order) {
                return Err(ContractError::InvalidManifest.into());
            }
            previous = Some(order);
        }
        Ok(Self { rows })
    }

    fn token(
        &self,
        key: EvaluationKey,
        before: Binding,
        target: validation::Target,
        visits: &mut usize,
    ) -> Result<&'a SealTransition, NativeError> {
        if before.object.0 != key.validation.0 || EvaluationTarget::of(target) != key.target {
            return Err(ContractError::InvalidManifest.into());
        }
        // The target comes from the actual retained source/candidate state.
        // Claim membership is checked separately against its real declaration
        // and registry; no claim ID is manufactured from a partial target.
        let wanted = (binding_order(before), target_order(target), key.generation);
        let mut left = 0usize;
        let mut right = self.rows.len();
        while left < right {
            take(visits, 1)?;
            let half = right.checked_sub(left).ok_or(ContractError::Capacity)? / 2;
            let middle = left.checked_add(half).ok_or(ContractError::Capacity)?;
            let seal = self.rows.get(middle).ok_or(ContractError::Capacity)?;
            match seal_order(seal).cmp(&wanted) {
                std::cmp::Ordering::Less => {
                    left = middle.checked_add(1).ok_or(ContractError::Capacity)?;
                }
                std::cmp::Ordering::Greater => right = middle,
                std::cmp::Ordering::Equal => return Ok(seal),
            }
        }
        Err(ContractError::InvalidManifest.into())
    }
}

fn at_binding(
    key: EvaluationKey,
    binding: Binding,
    prepared: &NativePrepared,
    begin: Option<&prepare::BeginTransition<'_>>,
    seals: &SealIndex<'_>,
    visits: &mut usize,
) -> Result<EvaluationState, NativeError> {
    let final_state = *prepared
        .evaluation(key)
        .ok_or(ContractError::InvalidTarget)?;
    if final_state.binding() == binding {
        return Ok(final_state);
    }
    if let Some(begin) = begin.filter(|begin| begin.key() == key)
        && begin.next().binding() == binding
    {
        return Ok(begin.next());
    }
    let seal = seals.token(key, binding, final_state.target(), visits)?;
    Ok(seal.previous())
}

fn check_frame(
    event: Frame,
    previous: EvaluationState,
    next: EvaluationState,
    definition: &validation::Declaration,
) -> Result<(), NativeError> {
    if event.before != previous.binding()
        || event.after != previous.binding().next()?
        || event.after != next.binding()
        || event.key != EvaluationKey::of(event.key.claim, &next)
        || event.state != next.state()
        || (event.kind != NativeEvaluationEventKind::MissingTarget && event.phase != next.phase())
        || event.fence != next.fence()
        || previous.target() != next.target()
        || previous.generation() != next.generation()
        || previous.receipt() != next.receipt()
    {
        return Err(ContractError::StaleEvaluation.into());
    }
    let view = next.bind(definition)?;
    if view.claim() != event.key.claim {
        return Err(ContractError::WrongObject.into());
    }
    Ok(())
}

/// First entry settles actual manifest absence before the cohort can seal.
/// Resolve the source and the recorded entry through indexed reads; never use
/// a seal's intermediate state as an unchecked replacement for the source.
fn check_missing(
    source: &View<'_>,
    prepared: &NativePrepared,
    event: Frame,
    previous: EvaluationState,
    next: EvaluationState,
    definition: &validation::Declaration,
    visits: &mut usize,
) -> Result<(), NativeError> {
    if !matches!(
        prepared.outcome.operation,
        NativeOperation::EnterWholeWork | NativeOperation::BeginWork
    ) {
        return Err(ContractError::InvalidTransition.into());
    }
    take(visits, 4)?;
    crate::native::object_journal::check_missing_fact(
        event.key,
        &previous,
        &next,
        definition,
        NativeFact::Evaluation {
            kind: event.kind,
            key: event.key,
            before: Some(event.before),
            after: event.after,
            state: event.state,
            phase: event.phase,
            attempt: event.attempt,
            fence: event.fence,
        },
    )?;
    let validation::Target::MissingSlot { response, slot } = previous.target() else {
        return Err(ContractError::InvalidTarget.into());
    };
    let id = TestamentId(response.object.0);
    let old = crate::native::response_reads::as_response_record(source.get(Key::Response(id)))
        .ok_or(ContractError::MissingEvidence)?;
    let final_record =
        crate::native::response_reads::as_response_record(prepared.range.get(&Key::Response(id)))
            .ok_or(ContractError::MissingEvidence)?;
    let actual = old.response().identity();
    let mut retained = final_record.response().identity();
    retained.binding = actual.binding;
    let received = old.received().ok_or(ContractError::InvalidCut)?;
    let entered = final_record.entered().ok_or(ContractError::InvalidCut)?;
    Binding {
        revision: response.revision,
        ..actual.binding
    }
    .check(&response)?;
    if old.response().state() != ResponseState::Received
        || old.entered().is_some()
        || actual.binding.revision < response.revision
        || actual.claim != event.key.claim
        || actual.binding.ledger != previous.binding().ledger
        || actual.cycle == 0
        || u64::from(actual.cycle) != previous.generation()
        || Some(actual.receipt) != previous.receipt()
        || retained != actual
        || final_record.received() != Some(received)
        || received.sequence.0 == 0
        || received >= entered
        || entered.sequence != prepared.outcome.sequence
        || entered.ordinal >= event.ordinal
        || !matches!(definition.target(), validation::TargetDeclaration::WholeWorkSlot { index, .. } if index == slot)
    {
        return Err(ContractError::InvalidTarget.into());
    }
    // Charge the binary-search ceiling before inspecting this immutable
    // manifest. No per-evaluation traversal of the whole entry journal occurs.
    let depth = usize::try_from(
        usize::BITS
            .checked_sub(old.response().manifest().len().leading_zeros())
            .ok_or(ContractError::Capacity)?,
    )
    .map_err(|_| ContractError::Capacity)?;
    take(visits, depth)?;
    if old
        .response()
        .manifest()
        .binary_search_by_key(&slot, |member| member.slot)
        .is_ok()
    {
        return Err(ContractError::InvalidTarget.into());
    }
    let entry = CompletionBook::event(prepared, entered.ordinal)?;
    if entry.sequence != entered.sequence
        || entry.ordinal != entered.ordinal
        || entry.invocation != prepared.outcome.invocation
        || entry.fact
            != (NativeFact::Response {
                claim: event.key.claim,
                before: Some(actual.binding),
                after: actual.binding.next()?,
                state: ResponseState::Validating,
            })
    {
        return Err(ContractError::InvalidCut.into());
    }
    if let Some(result) = next.last_result() {
        take(visits, 3)?;
        let key = NativeResultKey::of(result);
        let missing =
            crate::native::response_reads::as_missing(prepared.range.get(&Key::MissingResult(key)))
                .ok_or(ContractError::MissingEvidence)?;
        if source.get(Key::MissingResult(key)).is_some()
            || missing.result() != result
            || missing.sequence() != prepared.outcome.sequence
            || missing.ordinal() <= event.ordinal
            || missing.ordinal() >= prepared.outcome.events
        {
            return Err(ContractError::InvalidCut.into());
        }
        let publication = CompletionBook::event(prepared, missing.ordinal())?;
        if publication.sequence != missing.sequence()
            || publication.ordinal != missing.ordinal()
            || publication.invocation != prepared.outcome.invocation
            || publication.fact != (NativeFact::Missing { key })
        {
            return Err(ContractError::InvalidCut.into());
        }
    }
    Ok(())
}

fn check_report(
    prepared: &NativePrepared,
    event: Frame,
    previous: EvaluationState,
    next: EvaluationState,
    definition: &validation::Declaration,
) -> Result<(), NativeError> {
    let attempt = previous.bind(definition)?.current_attempt()?;
    let result = next.last_result().ok_or(ContractError::MissingEvidence)?;
    let accepted = prepared
        .result(NativeResultKey::of(result))
        .ok_or(ContractError::MissingEvidence)?;
    if previous.fence().is_some()
        || previous.state().is_terminal()
        || event.attempt != Some(attempt)
        || accepted.attempt() != attempt
        || accepted.sequence() != prepared.outcome.sequence
        || accepted.ordinal() >= prepared.outcome.events
        || accepted.ordinal() <= event.ordinal
        || accepted.result() != result
        || result.binding() != event.after
        || result.resulting_state() != next.state()
        || result.target() != next.target()
        || result.generation() != next.generation()
        || result.receipt() != next.receipt()
        || result.validation() != event.key.validation
        || result.claim() != event.key.claim
        || !next.has_begun()
        || next.fence() != previous.fence()
        || next.sealed() != previous.sealed()
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let publication = CompletionBook::event(prepared, accepted.ordinal())?;
    if publication.fact
        != (NativeFact::Accepted {
            key: NativeResultKey::of(result),
        })
        || publication.sequence != accepted.sequence()
        || publication.ordinal != accepted.ordinal()
        || publication.invocation != prepared.outcome.invocation
    {
        return Err(ContractError::InvalidCut.into());
    }
    Ok(())
}

impl CompletionBook {
    fn seed(&self, source: &View<'_>, event: Frame, ordinal: u32) -> Result<Update, NativeError> {
        let state = source.evaluation(event.key)?;
        let credit = self
            .entries
            .get(event.key)
            .map(|grant| grant.credit)
            .unwrap_or(Credit {
                binding: state.binding(),
                remaining_reports: 0,
                failure_available: false,
            });
        Ok(Update {
            key: event.key,
            before: credit,
            after: credit,
            ordinal,
            registration_index: self
                .entries
                .get(event.key)
                .filter(|grant| grant.credit.remaining_reports != 0)
                .map(|grant| grant.registration_index),
        })
    }

    #[allow(clippy::too_many_arguments)] // Exact source, candidate and checked model capabilities.
    fn resolve_updates(
        &self,
        source: &View<'_>,
        parent: &Parent<'_>,
        prepared: &NativePrepared,
        records: &[Update],
        report: Option<ReportAdvance>,
        begin: Option<&prepare::BeginTransition<'_>>,
        seals: &SealIndex<'_>,
        visits: &mut usize,
    ) -> Result<Option<Update>, NativeError> {
        let first = *records.first().ok_or(ContractError::InvalidManifest)?;
        let key = first.key;
        let mut current = *source.evaluation(key)?;
        let definition = source.definition(key.validation)?;
        let source_claim = parent.claim;
        take(visits, source_claim.acceptance().declarations().len())?;
        source_claim.acceptance().check_declaration(definition)?;
        let row = parent
            .registry
            .rows()
            .get(
                first
                    .registration_index
                    .ok_or(ContractError::InvalidTarget)?,
            )
            .ok_or(ContractError::InvalidTarget)?;
        if transactions::key_for_registered(key.claim, *row) != key {
            return Err(ContractError::InvalidTarget.into());
        }
        row.check_state(current, definition)?;
        let grant = self.entries.get(key);
        let live = grant.is_some_and(|grant| grant.credit.remaining_reports != 0);
        let starting = begin.filter(|begin| begin.key() == key);
        if let Some(grant) = grant.filter(|_| live) {
            if let Some(begin) = starting {
                if !begin.next().has_begun() || begin.previous() != current {
                    return Err(ContractError::ContentConflict.into());
                }
                grant.credit.binding.check(&begin.next().binding())?;
            } else {
                grant.credit.binding.check(&current.binding())?;
            }
            if first.registration_index != Some(grant.registration_index) {
                return Err(ContractError::InvalidTarget.into());
            }
        } else if starting.is_some_and(|begin| begin.next().has_begun())
            || (current.has_begun() && !current.state().is_terminal() && current.fence().is_none())
        {
            return Err(ContractError::StaleEvaluation.into());
        }
        let mut next_credit = first.before;
        let mut reported = false;
        let mut begun = false;
        for record in records {
            take(visits, 1)?;
            if record.key != key {
                return Err(ContractError::InvalidManifest.into());
            }
            let event =
                frame(prepared, record.ordinal, begin)?.ok_or(ContractError::InvalidManifest)?;
            let next = at_binding(key, event.after, prepared, begin, seals, visits)?;
            check_frame(event, current, next, definition)?;
            match event.kind {
                NativeEvaluationEventKind::MissingTarget => {
                    if live || reported || begun {
                        return Err(ContractError::InvalidTransition.into());
                    }
                    check_missing(source, prepared, event, current, next, definition, visits)?;
                }
                NativeEvaluationEventKind::Begun => {
                    let start = starting.ok_or(ContractError::InvalidTransition)?;
                    let attempt = if next.has_begun() {
                        Some(next.bind(definition)?.current_attempt()?)
                    } else {
                        None
                    };
                    if begun
                        || current != start.previous()
                        || next != start.next()
                        || event.attempt != attempt
                    {
                        return Err(ContractError::InvalidTransition.into());
                    }
                    begun = true;
                }
                NativeEvaluationEventKind::Reported => {
                    let marker = report
                        .filter(|report| report.key == key)
                        .ok_or(ContractError::InvalidTransition)?;
                    if reported
                        || marker.before != current.binding()
                        || !live
                        || next_credit.remaining_reports == 0
                    {
                        return Err(ContractError::StaleEvaluation.into());
                    }
                    check_report(prepared, event, current, next, definition)?;
                    let usage =
                        crate::native::completion_envelope::ReportParent::capture(source_claim)
                            .completion_use_recorded(prepared)?;
                    if usage == CompletionUse::AdmissionFailure {
                        take(visits, 1)?;
                    }
                    if usage != marker.usage
                        || (usage == CompletionUse::AdmissionFailure
                            && !next_credit.failure_available)
                    {
                        return Err(ContractError::InvalidTransition.into());
                    }
                    next_credit.remaining_reports = next_credit
                        .remaining_reports
                        .checked_sub(1)
                        .ok_or(ContractError::InvalidTransition)?;
                    next_credit.failure_available &= usage != CompletionUse::AdmissionFailure;
                    if !next.state().is_terminal()
                        && next.fence().is_none()
                        && next_credit.remaining_reports == 0
                    {
                        return Err(ContractError::InvalidTransition.into());
                    }
                    reported = true;
                }
                NativeEvaluationEventKind::Sealed => {
                    let seal = seals.token(key, event.before, current.target(), visits)?;
                    seal.check(&current, &next)?;
                    let claim = prepared
                        .claim(key.claim)
                        .ok_or(ContractError::InvalidTarget)?;
                    if claim
                        .local_sealed_at()
                        .is_none_or(|at| at > prepared.outcome.sequence)
                    {
                        return Err(ContractError::InvalidCut.into());
                    }
                    take(visits, claim.acceptance().declarations().len())?;
                    let expected = current.seal_claim(definition, &current.binding(), claim)?;
                    expected.check(&current, &next)?;
                    let attempt = if next.has_begun() {
                        Some(next.bind(definition)?.current_attempt()?)
                    } else {
                        None
                    };
                    if event.attempt != attempt {
                        return Err(ContractError::StaleEvaluation.into());
                    }
                }
                NativeEvaluationEventKind::AuthorityFenced => {
                    if current.state().is_terminal()
                        || current.fence().is_some()
                        || next.state() != current.state()
                        || next.phase() != current.phase()
                        || next.has_begun() != current.has_begun()
                        || next.last_result() != current.last_result()
                        || next.sealed() != current.sealed()
                        || next.fence().is_none()
                    {
                        return Err(ContractError::InvalidTransition.into());
                    }
                    let attempt = if next.has_begun() {
                        Some(next.bind(definition)?.current_attempt()?)
                    } else {
                        None
                    };
                    if event.attempt != attempt {
                        return Err(ContractError::StaleEvaluation.into());
                    }
                }
                _ => return Err(ContractError::InvalidTransition.into()),
            }
            if live {
                next_credit.binding = next.binding();
                if next.state().is_terminal() || next.fence().is_some() {
                    next_credit.remaining_reports = 0;
                    next_credit.failure_available = false;
                }
            }
            current = next;
        }
        if prepared.evaluation(key) != Some(&current)
            || (report.is_some_and(|report| report.key == key) && !reported)
            || (starting.is_some() && !begun)
        {
            return Err(ContractError::InvalidManifest.into());
        }
        // An older pending retirement owns any retained zero-credit entry.
        // Later audit-only seals must not rebind the entry it will remove.
        if !live || next_credit == first.before {
            return Ok(None);
        }
        Ok(Some(Update {
            after: next_credit,
            ..first
        }))
    }

    /// Build one journal for all report, structural-entry, seal and fence events.
    /// All capabilities and exact state chains are validated before mutation.
    /// The caller owns/precharges seal tokens; only changed tokens are accepted.
    /// Tokens must be strictly ordered by complete before binding
    /// (ledger/object/content/revision), then complete target in this order:
    /// Admission(claim), Increment(claim/artifact), Artifact(response/slot/artifact),
    /// MissingSlot(response/slot), Delivery(response), then generation. Every
    /// nested binding uses the same field order. The writer must charge its own
    /// sorting work; this collector validates once and searches without allocation.
    pub(in crate::native) fn apply_prepared(
        &mut self,
        source: &View<'_>,
        prepared: &NativePrepared,
        report: Option<ReportAdvance>,
        begin: Option<&prepare::BeginTransition<'_>>,
        seals: &[SealTransition],
        funding: JournalFunding<'_>,
    ) -> Result<Journal, NativeError> {
        self.check_health()?;
        if let JournalFunding::External { source, .. } = funding
            && !source.is_within(&self.parent)
        {
            return Err(MemoryError::InvalidConfiguration(
                "completion journal source is outside owner budget",
            )
            .into());
        }
        if source.ledger() != prepared.outcome.ledger
            || source.prefix().0.checked_add(1) != Some(prepared.outcome.sequence.0)
        {
            return Err(ContractError::InvalidCut.into());
        }
        source.check_successor(prepared)?;
        if let Some(begin) = begin {
            if report.is_some() {
                return Err(ContractError::InvalidTransition.into());
            }
            begin.check(prepared)?;
        }
        let mut visits = self.limits.plan_edges;
        let mut count = 0usize;
        let mut single = None;
        let mut reports = 0usize;
        let mut sealed = 0usize;
        let mut begun = 0usize;
        for ordinal in 0..prepared.outcome.events {
            take(&mut visits, 1)?;
            if let Some(event) = frame(prepared, ordinal, begin)? {
                count = add(count, 1)?;
                reports = add(
                    reports,
                    usize::from(event.kind == NativeEvaluationEventKind::Reported),
                )?;
                sealed = add(
                    sealed,
                    usize::from(event.kind == NativeEvaluationEventKind::Sealed),
                )?;
                begun = add(
                    begun,
                    usize::from(event.kind == NativeEvaluationEventKind::Begun),
                )?;
                single = Some(self.seed(source, event, ordinal)?);
            }
        }
        if reports != usize::from(report.is_some())
            || sealed != seals.len()
            || begun != usize::from(begin.is_some())
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let seals = SealIndex::new(seals, &mut visits)?;
        if count == 0 {
            return Ok(Journal::empty());
        }
        if count == 1 {
            let mut update = single.ok_or(ContractError::InvalidManifest)?;
            let parent = parent(source, std::slice::from_mut(&mut update), &mut visits)?;
            return match self.resolve_updates(
                source,
                &parent,
                prepared,
                std::slice::from_ref(&update),
                report,
                begin,
                &seals,
                &mut visits,
            )? {
                Some(update) => self.apply_single_update(update),
                None => Ok(Journal::empty()),
            };
        }
        let bytes = journal_bytes(count)?;
        let reservation = match funding {
            JournalFunding::HeldCompletion => {
                self.source()
                    .reserve(BudgetKind::Pending, BudgetLane::Completion, bytes)?
            }
            JournalFunding::External { source, lane } => {
                source.reserve(BudgetKind::Pending, lane, bytes)?
            }
        };
        let mut updates = Vec::new();
        updates
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(array::<Update>(updates.capacity())?, bytes)?;
        for ordinal in 0..prepared.outcome.events {
            take(&mut visits, 1)?;
            if let Some(event) = frame(prepared, ordinal, begin)? {
                if updates.len() == updates.capacity() {
                    return Err(NativeError::Capacity("completion journal"));
                }
                updates.push(self.seed(source, event, ordinal)?);
            }
        }
        sort(&mut updates, &mut visits)?;
        let mut read = 0usize;
        let mut written = 0usize;
        while let Some(first) = updates.get(read).copied() {
            take(&mut visits, 1)?;
            let mut claim_end = read.checked_add(1).ok_or(ContractError::Capacity)?;
            while let Some(row) = updates.get(claim_end) {
                take(&mut visits, 1)?;
                if row.key.claim != first.key.claim {
                    break;
                }
                claim_end = claim_end.checked_add(1).ok_or(ContractError::Capacity)?;
            }
            let parent = parent(
                source,
                updates
                    .get_mut(read..claim_end)
                    .ok_or(ContractError::Capacity)?,
                &mut visits,
            )?;
            while read < claim_end {
                take(&mut visits, 1)?;
                let first = updates.get(read).ok_or(ContractError::Capacity)?;
                let mut end = read.checked_add(1).ok_or(ContractError::Capacity)?;
                while end < claim_end {
                    take(&mut visits, 1)?;
                    if updates.get(end).ok_or(ContractError::Capacity)?.key != first.key {
                        break;
                    }
                    end = end.checked_add(1).ok_or(ContractError::Capacity)?;
                }
                let records = updates
                    .get(read..end)
                    .ok_or(ContractError::InvalidManifest)?;
                if let Some(update) = self.resolve_updates(
                    source,
                    &parent,
                    prepared,
                    records,
                    report,
                    begin,
                    &seals,
                    &mut visits,
                )? {
                    *updates.get_mut(written).ok_or(ContractError::Capacity)? = update;
                    written = written.checked_add(1).ok_or(ContractError::Capacity)?;
                }
                read = end;
            }
        }
        updates.truncate(written);
        if updates.is_empty() {
            return Ok(Journal::empty());
        }
        let mut totals = self.totals;
        for update in &updates {
            take(&mut visits, 1)?;
            let grant = self.grant(update.key)?;
            if grant.credit != update.before {
                return Err(ContractError::StaleEvaluation.into());
            }
            totals = totals.replace(
                demand(grant.envelope, update.before)?,
                demand(grant.envelope, update.after)?,
            )?;
        }
        let journals = add(self.journals, 1)?;
        let revision = self.next_revision()?;
        let before = self.totals;
        take(&mut visits, updates.len())?;
        // No allocations or ordinary refusal remain after this point. Structural
        // index failure poisons the book and the exclusive owner fails closed.
        for update in &updates {
            let workspace = if update.after.remaining_reports == 0 {
                0
            } else {
                self.grant(update.key)?.envelope.workspace_bytes()
            };
            self.entries
                .replace_weight(update.key, workspace, |grant| grant.credit = update.after)?;
        }
        self.totals = totals;
        self.totals.workspace = self.maximum_workspace();
        self.check_health()?;
        self.journals = journals;
        let before_revision = self.revision;
        self.revision = revision;
        Ok(Journal {
            owner: Some(self.owner),
            before_revision,
            after_revision: revision,
            change: Change::Many {
                updates,
                _allocation: reservation.commit(),
            },
            before,
        })
    }
}

#[cfg(test)]
#[path = "completion_seal_lookup_tests.rs"]
mod seal_lookup_tests;

#[cfg(test)]
#[path = "completion_missing_tests.rs"]
mod missing_tests;
