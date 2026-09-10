//! Complete per-evaluation history from bounded key seeks, never attempt-derived
//! row addresses or an unbounded ledger scan. All reads use the same root.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Counts {
    pub(super) evaluations: usize,
    pub(super) results: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    Accepted,
    Delivery,
    Missing,
}
impl Family {
    fn key(self, key: NativeResultKey) -> Key {
        match self {
            Self::Accepted => Key::Accepted(key),
            Self::Delivery => Key::DeliveryResult(key),
            Self::Missing => Key::MissingResult(key),
        }
    }
    fn member(self, key: Key) -> Option<NativeResultKey> {
        match (self, key) {
            (Self::Accepted, Key::Accepted(key))
            | (Self::Delivery, Key::DeliveryResult(key))
            | (Self::Missing, Key::MissingResult(key)) => Some(key),
            _ => None,
        }
    }
    fn of(target: validation::Target) -> Self {
        match target {
            validation::Target::Delivery { .. } => Self::Delivery,
            validation::Target::MissingSlot { .. } => Self::Missing,
            _ => Self::Accepted,
        }
    }
}

fn entries_from<'a>(
    view: &'a View<'_>,
    key: &Key,
) -> impl Iterator<Item = &'a focal_memory::Entry<Key, Row>> {
    let committed = view
        .tail
        .is_none()
        .then(|| view.state.rows.entries_from(key, false));
    let pending = view
        .tail
        .map(|tail| tail.fragments.entries_from(key, false));
    pending
        .into_iter()
        .flatten()
        .chain(committed.into_iter().flatten())
}

fn published(
    view: &View<'_>,
    family: Family,
    key: NativeResultKey,
    row: &Row,
    visits: &mut Visits,
) -> Result<(validation::AcceptedResult, PublicationPosition), NativeError> {
    let (result, sequence, ordinal, fact) = match (family, row) {
        (Family::Accepted, Row::Accepted(row)) => {
            let row = row.get().ok_or(ContractError::MissingEvidence)?;
            let result = row.result();
            visits.take(1)?;
            let evidence = result.evidence().ok_or(ContractError::MissingEvidence)?;
            let artifact = as_artifact(view.get(Key::Artifact(evidence.id)))
                .ok_or(ContractError::MissingEvidence)?;
            let facts = artifact.facts().ok_or(ContractError::MissingEvidence)?;
            if artifact.descriptor().id() != evidence.id
                || artifact.descriptor().content_hash() != evidence.hash
                || row.artifact().reference() != evidence
                || facts.claim != result.claim()
                || facts.validation != result.validation()
                || facts.target != result.target()
                || facts.generation != result.generation()
                || facts.attempt != row.attempt()
                || facts.producer != row.artifact().producer()
                || facts.value != result.verdict()
            {
                return Err(ContractError::MissingEvidence.into());
            }
            (
                result,
                row.sequence(),
                row.ordinal(),
                NativeFact::Accepted { key },
            )
        }
        (Family::Delivery, Row::DeliveryResult(row)) => {
            let row = row.get().ok_or(ContractError::MissingEvidence)?;
            (
                row.result(),
                row.sequence(),
                row.ordinal(),
                NativeFact::Delivery { key },
            )
        }
        (Family::Missing, Row::MissingResult(row)) => {
            let row = row.get().ok_or(ContractError::MissingEvidence)?;
            (
                row.result(),
                row.sequence(),
                row.ordinal(),
                NativeFact::Missing { key },
            )
        }
        _ => return Err(ContractError::InvalidTarget.into()),
    };
    if NativeResultKey::of(result) != key
        || Family::of(result.target()) != family
        || sequence.0 == 0
        || sequence > view.prefix()
    {
        return Err(ContractError::InvalidCut.into());
    }
    visits.take(2)?;
    let event = match view.get(Key::Event(sequence, ordinal)) {
        Some(Row::Event(event)) => event.get().map(|row| row.expand(view.ledger())),
        _ => None,
    }
    .ok_or(ContractError::MissingEvidence)?;
    let outcome = as_outcome(view.get(Key::Outcome(event.invocation)))
        .ok_or(ContractError::MissingEvidence)?;
    if event.fact != fact
        || event.sequence != sequence
        || event.ordinal != ordinal
        || outcome.invocation != event.invocation
        || outcome.ledger != view.ledger()
        || outcome.sequence != sequence
        || ordinal >= outcome.events
    {
        return Err(ContractError::InvalidCut.into());
    }
    Ok((result, PublicationPosition { sequence, ordinal }))
}

pub(super) fn scan<'a>(
    view: &'a View<'_>,
    claim: ClaimId,
    registry: &RegistrationSet,
    limits: NativeLimits,
    visits: &mut Visits,
    mut member: impl FnMut(validation::Evaluation<'a>) -> Result<(), NativeError>,
    mut result: impl FnMut(validation::AcceptedResult, PublicationPosition) -> Result<(), NativeError>,
) -> Result<Counts, NativeError> {
    let mut counts = Counts {
        evaluations: 0,
        results: 0,
    };
    for registered in registry.rows() {
        visits.take(3)?;
        // Resolve the row under the actual parent identity. The model also
        // checks declaration ownership and complete registered membership.
        let definition = view.definition(ValidationId(registered.binding().object.0))?;
        let key = transactions::key_for_registered(claim, *registered);
        let state = view.evaluation(key)?;
        registered.check_state(*state, definition)?;
        let evaluation = state.bind(definition)?;
        member(evaluation)?;
        counts.evaluations = add(counts.evaluations, 1)?;
        let expected_family = Family::of(state.target());
        let maximum = usize::try_from(evaluation.attempt_bound().max(1))
            .map_err(|_| ContractError::Capacity)?;
        let mut member_results = 0usize;
        let mut previous = None;
        for family in [Family::Accepted, Family::Delivery, Family::Missing] {
            visits.take(1)?;
            let lower = family.key(NativeResultKey {
                evaluation: key,
                revision: focal_model::ObjectRevision(0),
            });
            for entry in entries_from(view, &lower) {
                visits.take(1)?;
                let Some(address) = family
                    .member(entry.key)
                    .filter(|address| address.evaluation == key)
                else {
                    break;
                };
                if family != expected_family
                    || member_results >= maximum
                    || counts.results >= limits.results
                {
                    return Err(ContractError::InvalidManifest.into());
                }
                let (accepted, position) = published(view, family, address, &entry.value, visits)?;
                if let Some((revision, prior)) = previous
                    && (address.revision <= revision || position <= prior)
                {
                    return Err(ContractError::InvalidCut.into());
                }
                previous = Some((address.revision, position));
                result(accepted, position)?;
                member_results = add(member_results, 1)?;
                counts.results = add(counts.results, 1)?;
            }
        }
    }
    Ok(counts)
}

fn order(
    rows: &[NativeAuditPublication],
    a: usize,
    b: usize,
    visits: &mut Visits,
) -> Result<std::cmp::Ordering, NativeError> {
    visits.take(1)?;
    Ok(rows
        .get(a)
        .ok_or(ContractError::Capacity)?
        .key
        .cmp(&rows.get(b).ok_or(ContractError::Capacity)?.key))
}
fn swap(
    rows: &mut [NativeAuditPublication],
    a: usize,
    b: usize,
    visits: &mut Visits,
) -> Result<(), NativeError> {
    visits.take(1)?;
    let x = *rows.get(a).ok_or(ContractError::Capacity)?;
    let y = *rows.get(b).ok_or(ContractError::Capacity)?;
    *rows.get_mut(a).ok_or(ContractError::Capacity)? = y;
    *rows.get_mut(b).ok_or(ContractError::Capacity)? = x;
    Ok(())
}
fn sift(
    rows: &mut [NativeAuditPublication],
    mut root: usize,
    end: usize,
    visits: &mut Visits,
) -> Result<(), NativeError> {
    loop {
        let left = root
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
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
pub(super) fn sort(
    rows: &mut [NativeAuditPublication],
    visits: &mut Visits,
) -> Result<(), NativeError> {
    let mut root = rows.len() / 2;
    while root != 0 {
        root = root.checked_sub(1).ok_or(ContractError::Capacity)?;
        sift(rows, root, rows.len(), visits)?;
    }
    let mut end = rows.len();
    while end > 1 {
        end = end.checked_sub(1).ok_or(ContractError::Capacity)?;
        swap(rows, 0, end, visits)?;
        sift(rows, 0, end, visits)?;
    }
    for pair in rows.windows(2) {
        visits.take(1)?;
        if let [a, b] = pair
            && a.key >= b.key
        {
            return Err(ContractError::InvalidManifest.into());
        }
    }
    Ok(())
}
