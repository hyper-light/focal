//! Validate a recorded successor against its already validated predecessor.
//! The only enumeration is the bounded captured mutation and its new events;
//! immutable predecessor rows are reached by exact keys. This does not rerun
//! commands, authenticate participants, or infer authority from record hashes.
use super::read_source::Meter;
use super::*;
use focal_model::ArtifactRef;

#[path = "replay_validate_counts.rs"]
mod counts;
#[path = "replay_validate_operation.rs"]
mod operation;
#[cfg(test)]
pub(super) fn check_operation_test(
    operation: NativeOperation,
    fact: NativeFact,
) -> Result<(), NativeError> {
    self::operation::check(operation, fact)
}
#[path = "replay_validate_audit.rs"]
mod audit;
#[path = "replay_validate_claim.rs"]
mod claims;
#[path = "replay_validate_graph.rs"]
mod graph_proofs;

/// The caller retains one canonical captured write set. `after` must distinguish
/// a staged deletion/pending row from an absent write before consulting `before`.
/// Every put must be completely decoded before final validation is called.
pub(super) trait Overlay {
    fn before(&self, key: Key) -> Option<&Row>;
    fn after(&self, key: Key) -> Option<&Row>;
    fn changes(&self) -> impl ExactSizeIterator<Item = (Key, Option<&Row>)>;
    fn changes_from(&self, key: Key) -> impl Iterator<Item = (Key, Option<&Row>)>;
}

pub(super) fn invalid() -> NativeError {
    ContractError::InvalidManifest.into()
}
pub(super) fn require(condition: bool) -> Result<(), NativeError> {
    if condition { Ok(()) } else { Err(invalid()) }
}
pub(super) fn add(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_add(b).ok_or(ContractError::Capacity.into())
}
pub(super) fn mul(a: usize, b: usize) -> Result<usize, NativeError> {
    a.checked_mul(b).ok_or(ContractError::Capacity.into())
}

pub(super) struct ReplayRead<'a, 'bytes, O> {
    pub(super) overlay: &'a O,
    pub(super) encoded: &'a dyn replay_index::EncodedRows<'bytes>,
    pub(super) index: &'a replay_index::Index,
    pub(super) parsing: &'a Meter,
    pub(super) ledger: LedgerId,
    pub(super) profile: NativeContentProfile,
    pub(super) base: SessionSeq,
    pub(super) outcome: NativeOutcome,
    pub(super) limits: NativeLimits,
    pub(super) meter: &'a Meter,
    pub(super) budget: &'a MemoryBudget,
}
impl<O: Overlay> ReplayRead<'_, '_, O> {
    pub(super) fn events(
        &self,
        key: Key,
        callback: impl FnMut(NativeEvent) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        self.index
            .events(self.encoded, key, self.parsing, self.meter, callback)
    }
    pub(super) fn only(&self, key: Key) -> Result<NativeEvent, NativeError> {
        let mut found = None;
        self.events(key, |event| {
            require(found.replace(event).is_none())?;
            Ok(())
        })?;
        found.ok_or_else(invalid)
    }
    pub(super) fn charge(&self, visits: usize) -> Result<(), NativeError> {
        self.meter.charge(visits).map_err(read_evidence::codec)
    }
    pub(super) fn changed(&self, key: Key) -> Result<bool, NativeError> {
        Ok(replay_index::locate(self.encoded, key, self.meter)?.is_some())
    }
    pub(super) fn before(&self, key: Key) -> Result<Option<&Row>, NativeError> {
        self.charge(const { (usize::BITS as usize + 1) * 64 })?;
        Ok(self.overlay.before(key))
    }
    pub(super) fn get(&self, key: Key) -> Result<Option<&Row>, NativeError> {
        self.charge(const { (usize::BITS as usize + 1) * 128 })?;
        Ok(self.overlay.after(key))
    }
    pub(super) fn require(&self, key: Key) -> Result<&Row, NativeError> {
        self.get(key)?.ok_or_else(invalid)
    }
    pub(super) fn claim(&self, id: ClaimId) -> Result<&ClaimState, NativeError> {
        as_claim(Some(self.require(Key::Claim(id))?)).ok_or_else(invalid)
    }
    pub(super) fn definition(
        &self,
        id: ValidationId,
    ) -> Result<&validation::Declaration, NativeError> {
        as_definition(Some(self.require(Key::Definition(id))?)).ok_or_else(invalid)
    }
    pub(super) fn artifact(&self, id: ArtifactId) -> Result<&NativeArtifact, NativeError> {
        as_artifact(Some(self.require(Key::Artifact(id))?)).ok_or_else(invalid)
    }
    pub(super) fn event(&self, ordinal: u32) -> Result<NativeEvent, NativeError> {
        let Row::Event(value) = self.require(Key::Event(self.outcome.sequence, ordinal))? else {
            return Err(invalid());
        };
        let value = value.get().ok_or_else(invalid)?.expand(self.ledger);
        require(
            value.sequence == self.outcome.sequence
                && value.invocation == self.outcome.invocation
                && value.ordinal == ordinal,
        )?;
        Ok(value)
    }
}
impl<O: Overlay> read_validate_attempts::Read for ReplayRead<'_, '_, O> {
    fn charge(&self, visits: usize) -> Result<(), NativeError> {
        Self::charge(self, visits)
    }
    fn schema(&self, reference: ArtifactRef) -> Result<ContentHash, NativeError> {
        let descriptor = self.artifact(reference.id)?.descriptor();
        require(descriptor.id() == reference.id && descriptor.content_hash() == reference.hash)?;
        Ok(descriptor.schema_hash())
    }
}

#[path = "replay_validate_links.rs"]
mod links;
#[path = "replay_validate_objects.rs"]
mod objects;
#[path = "replay_validate_references.rs"]
mod references;
#[path = "replay_validate_scope.rs"]
pub(super) mod scopes;

/// Complete validation occurs while all decoded writes remain unpublished.
/// Refusal leaves the caller's predecessor and durable acknowledgement intact.
pub(super) fn validate<O: Overlay>(read: &ReplayRead<'_, '_, O>) -> Result<(), NativeError> {
    counts::validate(read)?;
    let mut linked = links::Counts::default();
    let mut new_evaluations = 0usize;
    read.charge(add(read.overlay.changes().len(), 1)?)?;
    for (key, row) in read.overlay.changes() {
        let row = row.ok_or_else(invalid)?;
        references::check(key, row, read)?;
        links::check(key, row, read, &mut linked)?;
        match (key, row) {
            (Key::Work(id), Row::Work(value)) => {
                objects::work(id, value.get().ok_or_else(invalid)?, read)?
            }
            (Key::Response(id), Row::Response(value)) => {
                objects::response(id, value.record().ok_or_else(invalid)?, read)?
            }
            (Key::Evaluation(key), Row::Evaluation(value)) => {
                objects::evaluation(key, value.get().ok_or_else(invalid)?, read)?;
                if read.before(Key::Evaluation(key))?.is_none() {
                    new_evaluations = add(new_evaluations, 1)?;
                }
            }
            (Key::ResultTestament(id), Row::ResultTestament(value)) => {
                objects::audit(id, value.get().ok_or_else(invalid)?, read)?
            }
            _ => (),
        }
    }
    linked.finish()?;
    affected_claims(read, new_evaluations)
}

fn owner(key: Key, row: &Row) -> Result<Option<ClaimId>, NativeError> {
    Ok(match (key, row) {
        (Key::Claim(id), _) | (Key::ClaimContent(id), _) | (Key::ClaimResultTestament(id), _) => {
            Some(id)
        }
        (Key::Evaluation(key), _) => Some(key.claim),
        (Key::Accepted(key) | Key::DeliveryResult(key) | Key::MissingResult(key), _) => {
            Some(key.evaluation.claim)
        }
        (Key::Definition(_), Row::Definition(value)) => {
            Some(value.get().ok_or_else(invalid)?.claim())
        }
        (Key::Receipt(_), Row::Receipt(value)) => Some(value.claim),
        (Key::Cycle(key) | Key::RetiredCycle(key), _) => Some(key.claim),
        (Key::Work(_), Row::Work(value)) => Some(value.get().ok_or_else(invalid)?.state.claim()),
        (Key::Diagnostic(_), Row::Diagnostic(value)) => {
            Some(value.get().ok_or_else(invalid)?.diagnostic.claim())
        }
        (Key::Response(_), Row::Response(value)) => {
            Some(value.get().ok_or_else(invalid)?.identity().claim)
        }
        (Key::ResultTestament(_), Row::ResultTestament(value)) => {
            Some(value.get().ok_or_else(invalid)?.testament().claim())
        }
        (Key::Monitor(_), Row::Monitor(value)) => Some(ClaimId(value.owner.object.0)),
        _ => None,
    })
}
fn each_affected<O: Overlay>(
    read: &ReplayRead<'_, '_, O>,
    mut accept: impl FnMut(ClaimId) -> Result<(), NativeError>,
) -> Result<(), NativeError> {
    read.charge(mul(add(read.overlay.changes().len(), 1)?, 256)?)?;
    for (key, row) in read.overlay.changes() {
        let row = row.ok_or_else(invalid)?;
        if let Some(id) = owner(key, row)? {
            accept(id)?;
        }
        let (Key::Claim(id), Row::Claim(row)) = (key, row) else {
            continue;
        };
        let next = row.claim().ok_or_else(invalid)?;
        let previous = as_claim(read.before(key)?);
        if previous.is_some_and(|old| {
            old.status() == next.status() && old.scopes().released() == next.scopes().released()
        }) {
            continue;
        }
        // Only directly indexed subscribers to a changed predicate are read.
        // Any omitted first dependent must fail its own canonical projection;
        // replay never scans unrelated claims to rediscover a graph frontier.
        if let Some(Row::IncomingHead(head)) = read.get(Key::IncomingHead(id))? {
            if head.count > read.limits.plan_edges {
                return Err(ContractError::Capacity.into());
            }
            read.charge(add(head.count, 1)?)?;
            let mut current = head.head;
            for _ in 0..head.count {
                let dependent = current.ok_or_else(invalid)?;
                let Row::IncomingLink(link) = read.require(Key::IncomingLink(id, dependent))?
                else {
                    return Err(invalid());
                };
                accept(dependent)?;
                current = link.next;
            }
            require(current.is_none())?;
        }
        if let Some(Row::MonitorHead(head)) = read.get(Key::MonitorHead(id))? {
            if head.count > read.limits.plan_edges {
                return Err(ContractError::Capacity.into());
            }
            read.charge(add(head.count, 1)?)?;
            let mut current = head.head;
            let mut previous = None;
            for _ in 0..head.count {
                let monitor = current.ok_or_else(invalid)?;
                let Row::MonitorLink(Some(link)) = read.require(Key::MonitorLink(id, monitor))?
                else {
                    return Err(invalid());
                };
                require(link.previous == previous)?;
                accept(link.owner)?;
                previous = Some(monitor);
                current = link.next;
            }
            require(current.is_none())?;
        }
    }
    Ok(())
}
fn affected_claims<O: Overlay>(
    read: &ReplayRead<'_, '_, O>,
    new_evaluations: usize,
) -> Result<(), NativeError> {
    let mut count = 0usize;
    each_affected(read, |_| {
        count = add(count, 1)?;
        Ok(())
    })?;
    let quote = prepare::array::<ClaimId>(count)?;
    prepare::within(quote, read.limits.preparation_bytes)?;
    let sort = mul(mul(add(count, 1)?, const { usize::BITS as usize + 1 })?, 64)?;
    read.charge(sort)?;
    let _allocation = read
        .budget
        .reserve(
            focal_memory::BudgetKind::Recovery,
            focal_memory::BudgetLane::Completion,
            quote,
        )?
        .commit();
    let mut affected = Vec::new();
    affected
        .try_reserve_exact(count)
        .map_err(|_| MemoryError::AllocationFailed)?;
    prepare::within(prepare::array::<ClaimId>(affected.capacity())?, quote)?;
    each_affected(read, |id| {
        require(affected.len() < count && affected.len() < affected.capacity())?;
        affected.push(id);
        Ok(())
    })?;
    require(affected.len() == count)?;
    affected.sort_unstable();
    affected.dedup();
    let mut growth = 0usize;
    for id in affected {
        read.charge(256)?;
        let Row::Claim(value) = read.require(Key::Claim(id))? else {
            return Err(invalid());
        };
        let before = match read.before(Key::Claim(id))? {
            Some(Row::Claim(row)) => row.registrations().ok_or_else(invalid)?.rows().len(),
            None => 0,
            _ => return Err(invalid()),
        };
        let after = value.registrations().ok_or_else(invalid)?.rows().len();
        growth = add(growth, after.checked_sub(before).ok_or_else(invalid)?)?;
        claims::validate(id, value, read)?;
        graph_proofs::validate(id, read)?;
    }
    require(growth == new_evaluations)
}
