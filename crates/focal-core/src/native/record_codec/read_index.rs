//! Temporary, funded dependency index for detached checkpoint restoration.
//! Claim bodies remain borrowed; artifact origins come from their actual
//! recorded publication. Bounded batches enter the existing paged range store,
//! avoiding a ledger-sized vector or a full event scan per evidence object.
use super::*;
use focal_memory::{BudgetKind, BudgetLane, Change, Entry};
use focal_model::lifecycle::aggregation::PublicationPosition;
use read_source::{Meter, model_error};

pub(super) const PHASES: usize = 8;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IndexKey {
    Claim(ClaimId),
    Artifact(ArtifactId),
}
#[derive(Debug, Clone, Copy)]
enum Value<'a> {
    Claim(&'a [u8]),
    Artifact(Origin),
}
/// Who proved an artifact's custody: a participant request, or the import
/// translation, whose key derives from the descriptor's producer (23 §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ArtifactRequest {
    Request(RequestKey),
    Import,
}
impl ArtifactRequest {
    pub(super) fn resolve(
        self,
        ledger: LedgerId,
        id: ArtifactId,
        producer: ParticipantId,
    ) -> RequestKey {
        match self {
            Self::Request(key) => key,
            Self::Import => import_request(ledger, id, producer),
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub(super) struct Origin {
    pub(super) request: ArtifactRequest,
    pub(super) binding: Binding,
    pub(super) position: PublicationPosition,
}
pub(super) struct Index<'a> {
    rows: RangeStore<IndexKey, Value<'a>>,
    pub(super) counts: [usize; PHASES],
}
fn invalid() -> NativeError {
    ContractError::InvalidManifest.into()
}
pub(super) fn phase(key: Key) -> Result<usize, NativeError> {
    Ok(match key {
        Key::IncomingHead(_)
        | Key::IncomingLink(..)
        | Key::Monitor(_)
        | Key::MonitorHead(_)
        | Key::MonitorLink(..)
        | Key::Meta
        | Key::ArtifactIdentity(_)
        | Key::Receipt(_)
        | Key::Cycle(_)
        | Key::RetiredCycleHead(_)
        | Key::Retired(_)
        | Key::RetiredCycle(_)
        | Key::WorkSlot(..)
        | Key::ClaimResultTestament(_)
        | Key::Outcome(_)
        | Key::Event(..)
        | Key::ClaimIdentity(..)
        | Key::DefinitionIdentity(..)
        | Key::CreationResult(_)
        | Key::LegacyTestament(_)
        | Key::LegacyEvidenceSet(_)
        | Key::LegacyRun(..)
        | Key::LegacyDefinition(_)
        | Key::ByIssuer(..)
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
        | Key::ByObject(..) => 0,
        Key::Definition(_) | Key::ClaimContent(_) => 1,
        Key::Artifact(_) => 2,
        Key::Diagnostic(_) | Key::Work(_) => 3,
        Key::Evaluation(_) | Key::Accepted(_) | Key::DeliveryResult(_) | Key::MissingResult(_) => 4,
        Key::Response(_) => 5,
        Key::Claim(_) => 6,
        Key::ResultTestament(_) => 7,
        Key::End => return Err(invalid()),
    })
}
pub(super) fn lookup_work() -> Result<usize, NativeError> {
    usize::try_from(usize::BITS)
        .map_err(|_| invalid())?
        .checked_add(1)
        .and_then(|n| n.checked_mul(64))
        .ok_or(NativeError::Capacity("recovery index work"))
}
impl<'a> Index<'a> {
    pub(super) fn build(
        checkpoint: &checkpoint::StructuralCheckpoint<'a>,
        limits: NativeLimits,
        budget: &MemoryBudget,
        parsing: &Meter,
        lookup: &Meter,
    ) -> Result<Self, NativeError> {
        // The index never exports owner capabilities and has a different key/
        // value type from the restored ledger. Its local RangeId is not the
        // recorded or fresh ledger incarnation.
        let mut rows = RangeStore::new_partitioned(
            RangeId(0),
            0,
            limits.range,
            budget.clone(),
            |key| match key {
                IndexKey::Claim(_) => 0,
                IndexKey::Artifact(_) => 1,
            },
        )?;
        let capacity = limits
            .range
            .page_entries
            .min(limits.range.max_batch_entries);
        if capacity == 0 {
            return Err(NativeError::Capacity("recovery index batch"));
        }
        let staging_bytes = capacity
            .checked_mul(size_of::<Change<IndexKey, Value<'a>>>())
            .and_then(|n| n.checked_add(crate::native::prepare::ALLOCATION))
            .ok_or(NativeError::Capacity("recovery index staging"))?;
        let _staging =
            budget.reserve(BudgetKind::Recovery, BudgetLane::Completion, staging_bytes)?;
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(capacity)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if changes.capacity() != capacity {
            return Err(NativeError::Capacity("recovery index capacity"));
        }
        let mut counts = [0usize; PHASES];
        let maximum = limits
            .claims
            .checked_add(limits.artifacts)
            .ok_or(NativeError::Capacity("recovery index size"))?;
        // A complete scan quote includes more than the row cursor (frame/hash
        // work). Prepay that conservative bound before the first iterator step;
        // nested body readers separately share the remaining parsing allowance.
        let scan_work = checkpoint.quote().visits;
        parsing.charge(scan_work).map_err(model_error)?;
        let mut scan = checkpoint.rows(scan_work).map_err(model_error)?;
        for encoded in scan.by_ref() {
            lookup.charge(4).map_err(model_error)?;
            let encoded = encoded.map_err(model_error)?;
            let slot = counts.get_mut(phase(encoded.key)?).ok_or_else(invalid)?;
            *slot = slot
                .checked_add(1)
                .ok_or(NativeError::Capacity("recovery phase count"))?;
            let entry = match encoded.key {
                Key::Claim(id) => Some(Entry::new(
                    IndexKey::Claim(id),
                    Value::Claim(encoded.body()),
                    0,
                )),
                Key::Event(sequence, ordinal) => {
                    let (event, used) = parsing
                        .read(encoded.body(), read_events::event)
                        .map_err(model_error)?;
                    if used != encoded.body().len()
                        || event.sequence != sequence
                        || event.ordinal != ordinal
                    {
                        return Err(invalid());
                    }
                    if let NativeFact::Artifact { binding } = event.fact {
                        let request = match event.invocation {
                            NativeInvocation::Request(request) => ArtifactRequest::Request(request),
                            NativeInvocation::Import => ArtifactRequest::Import,
                            _ => return Err(invalid()),
                        };
                        if binding.ledger != checkpoint.header().ledger || binding.object.is_zero()
                        {
                            return Err(invalid());
                        }
                        Some(Entry::new(
                            IndexKey::Artifact(ArtifactId(binding.object.0)),
                            Value::Artifact(Origin {
                                request,
                                binding,
                                position: PublicationPosition { sequence, ordinal },
                            }),
                            0,
                        ))
                    } else {
                        None
                    }
                }
                _ => None,
            };
            if let Some(entry) = entry {
                let next = rows
                    .len()
                    .checked_add(changes.len())
                    .and_then(|n| n.checked_add(1))
                    .ok_or(NativeError::Capacity("recovery index size"))?;
                if next > maximum {
                    return Err(NativeError::Capacity("recovery index size"));
                }
                lookup.charge(lookup_work()?).map_err(model_error)?;
                if rows.get(&entry.key).is_some() {
                    return Err(invalid());
                }
                if changes.len() >= changes.capacity() {
                    return Err(NativeError::Capacity("recovery index staging"));
                }
                changes.push(Change::Put(entry));
                if changes.len() == capacity {
                    install(&mut rows, changes, limits.range, lookup)?;
                    changes = Vec::new();
                    changes
                        .try_reserve_exact(capacity)
                        .map_err(|_| MemoryError::AllocationFailed)?;
                    if changes.capacity() != capacity {
                        return Err(NativeError::Capacity("recovery index capacity"));
                    }
                }
            }
        }
        if !changes.is_empty() {
            install(&mut rows, changes, limits.range, lookup)?;
        }
        if rows.len() > maximum {
            return Err(NativeError::Capacity("recovery index size"));
        }
        Ok(Self { rows, counts })
    }
    pub(super) fn claim(&self, id: ClaimId, meter: &Meter) -> Result<&'a [u8], NativeError> {
        meter.charge(lookup_work()?).map_err(model_error)?;
        match self.rows.get(&IndexKey::Claim(id)) {
            Some(Value::Claim(bytes)) => Ok(*bytes),
            _ => Err(invalid()),
        }
    }
    pub(super) fn artifact(&self, id: ArtifactId, meter: &Meter) -> Result<Origin, NativeError> {
        meter.charge(lookup_work()?).map_err(model_error)?;
        match self.rows.get(&IndexKey::Artifact(id)) {
            Some(Value::Artifact(value)) => Ok(*value),
            _ => Err(invalid()),
        }
    }
}
fn install<'a>(
    rows: &mut RangeStore<IndexKey, Value<'a>>,
    changes: Vec<Change<IndexKey, Value<'a>>>,
    config: RangeConfig,
    meter: &Meter,
) -> Result<(), NativeError> {
    let n = changes.len();
    // Bound sorting, repeated directory descents and touched-page copies before
    // storage preparation. Each retained index entry is fixed-size and Copy.
    let per_entry = lookup_work()?
        .checked_mul(8)
        .and_then(|n| n.checked_add(config.page_entries.checked_mul(16)?))
        .ok_or(NativeError::Capacity("recovery index work"))?;
    let work = n
        .checked_mul(n)
        .and_then(|v| v.checked_add(n.checked_mul(per_entry)?))
        .ok_or(NativeError::Capacity("recovery index work"))?;
    meter.charge(work).map_err(model_error)?;
    let prefix = rows
        .prefix()
        .checked_add(1)
        .ok_or(NativeError::Capacity("recovery index prefix"))?;
    let prepared =
        rows.prepare_batch_with(prefix, changes, BudgetLane::Completion, |value| Ok(*value))?;
    rows.publish(prepared)?;
    Ok(())
}
