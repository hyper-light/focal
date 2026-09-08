//! Complete frozen audit membership and publication coverage. Point/range reads
//! use existing ordered keys; no full-ledger scan is repeated for each bundle.
use super::*;
use focal_model::{ObjectRevision, lifecycle::aggregation::PublicationPosition};
use read_validate::{ValidationRead, invalid};

pub(super) fn audit(
    read: &ValidationRead<'_, '_>,
    id: TestamentId,
    value: &NativeResultTestament,
) -> Result<(), NativeError> {
    read.charge(64)?;
    let testament = value.testament();
    let cohort = testament.cohort();
    if testament.binding().object.0 != id.0
        || testament.binding().ledger != read.ledger
        || value.captured_at() > read.prefix
        || value.generated_at().sequence > read.prefix
        || value
            .posted_at()
            .is_some_and(|at| at.sequence > read.prefix)
    {
        return Err(invalid());
    }
    let claim_id = testament.claim();
    let Row::Claim(owner) = read.require(Key::Claim(claim_id))? else {
        return Err(invalid());
    };
    let claim = owner.claim().ok_or_else(invalid)?;
    let registrations = owner.registrations().ok_or_else(invalid)?;
    let header = registrations.snapshot_v1();
    if !header.sealed
        || !header.increments_sealed
        || header.sealed_at != Some(cohort.sealed_at())
        || claim.local_sealed_at() != Some(cohort.sealed_at())
        || claim.issuer() != cohort.issuer()
        || registrations.rows().len() != cohort.members().len()
        || !cohort.complete()
    {
        return Err(invalid());
    }
    match read.require(Key::ClaimResultTestament(claim_id))? {
        Row::ClaimResultTestament(actual) if *actual == id => {}
        _ => return Err(invalid()),
    }
    let mut publications = 0usize;
    read.charge(1)?;
    for member in cohort.members() {
        read.charge(32)?;
        let snapshot = member.snapshot_v1();
        let key = EvaluationKey {
            claim: claim_id,
            validation: snapshot.key.validation,
            target: EvaluationTarget::of(snapshot.key.target),
            generation: snapshot.key.generation,
        };
        let Row::Evaluation(actual) = read.require(Key::Evaluation(key))? else {
            return Err(invalid());
        };
        let actual = actual.get().ok_or_else(invalid)?;
        if snapshot.binding.ledger != actual.binding().ledger
            || snapshot.binding.object != actual.binding().object
            || snapshot.binding.content != actual.binding().content
            || snapshot.binding.revision > actual.binding().revision
            || snapshot.key.target != actual.target()
            || snapshot.receipt != actual.receipt()
            || !member.complete()
        {
            return Err(invalid());
        }
        // The root validator proves every independent evaluation's exact
        // registration. Unique model-checked cohort keys plus equal cardinality
        // therefore close both missing-member and extra-member cases.
        for family in [Family::Accepted, Family::Delivery, Family::Missing] {
            publications = publications
                .checked_add(coverage(read, value, key, family)?)
                .ok_or(NativeError::Capacity("audit coverage count"))?;
        }
    }
    if publications != value.publications().len() || publications != testament.results().len() {
        return Err(invalid());
    }
    Ok(())
}
#[derive(Clone, Copy)]
enum Family {
    Accepted,
    Delivery,
    Missing,
}
impl Family {
    fn start(self, evaluation: EvaluationKey) -> Key {
        let result = NativeResultKey {
            evaluation,
            revision: ObjectRevision(0),
        };
        match self {
            Self::Accepted => Key::Accepted(result),
            Self::Delivery => Key::DeliveryResult(result),
            Self::Missing => Key::MissingResult(result),
        }
    }
    fn key(self, key: Key) -> Option<NativeResultKey> {
        match (self, key) {
            (Self::Accepted, Key::Accepted(key))
            | (Self::Delivery, Key::DeliveryResult(key))
            | (Self::Missing, Key::MissingResult(key)) => Some(key),
            _ => None,
        }
    }
}
fn coverage(
    read: &ValidationRead<'_, '_>,
    value: &NativeResultTestament,
    evaluation: EvaluationKey,
    family: Family,
) -> Result<usize, NativeError> {
    let traversal = read_index::lookup_work()?;
    read.charge(traversal)?;
    let start = family.start(evaluation);
    let mut rows = read.root.entries_from(&start, false);
    let mut count = 0usize;
    loop {
        read.charge(traversal)?;
        let Some(entry) = rows.next() else {
            break;
        };
        let Some(key) = family.key(entry.key) else {
            break;
        };
        if key.evaluation != evaluation {
            break;
        }
        let (result, position) = match &entry.value {
            Row::Accepted(row) => {
                let row = row.get().ok_or_else(invalid)?;
                (
                    row.result(),
                    PublicationPosition {
                        sequence: row.sequence(),
                        ordinal: row.ordinal(),
                    },
                )
            }
            Row::DeliveryResult(row) => {
                let row = row.get().ok_or_else(invalid)?;
                (
                    row.result(),
                    PublicationPosition {
                        sequence: row.sequence(),
                        ordinal: row.ordinal(),
                    },
                )
            }
            Row::MissingResult(row) => {
                let row = row.get().ok_or_else(invalid)?;
                (
                    row.result(),
                    PublicationPosition {
                        sequence: row.sequence(),
                        ordinal: row.ordinal(),
                    },
                )
            }
            _ => return Err(invalid()),
        };
        if position.sequence > value.captured_at() {
            continue;
        }
        read.charge(
            usize::try_from(usize::BITS)
                .map_err(|_| invalid())?
                .checked_add(1)
                .and_then(|n| n.checked_mul(8))
                .ok_or(NativeError::Capacity("audit publication lookup"))?,
        )?;
        if NativeResultKey::of(result) != key || value.publication(result) != Some(position) {
            return Err(invalid());
        }
        count = count
            .checked_add(1)
            .ok_or(NativeError::Capacity("audit coverage count"))?;
    }
    Ok(count)
}
