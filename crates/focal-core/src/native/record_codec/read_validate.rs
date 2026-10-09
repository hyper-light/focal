//! Cross-row validation of a detached decoded root before publication.
//! Checks establish retained consistency, never participant authentication from
//! the checkpoint checksum. All reads and scratch use the enclosing allowance.
use super::*;
use focal_memory::RangeHydrationView;
use focal_memory::{Allocation, BudgetKind, BudgetLane};

#[path = "read_validate_admission.rs"]
mod admission;
#[path = "read_validate_links.rs"]
mod links;
#[path = "read_validate_objects.rs"]
mod objects;
#[cfg(test)]
#[path = "read_validate_tests.rs"]
mod tests;

fn sum(left: usize, right: usize) -> Result<usize, NativeError> {
    left.checked_add(right)
        .ok_or_else(|| ContractError::Capacity.into())
}
fn increment(value: &mut usize) -> Result<(), NativeError> {
    *value = sum(*value, 1)?;
    Ok(())
}

pub(super) fn invalid() -> NativeError {
    ContractError::InvalidManifest.into()
}

pub(super) struct ValidationRead<'v, 'r> {
    pub(super) root: &'v RangeHydrationView<'r, Key, Row>,
    pub(super) ledger: LedgerId,
    pub(super) profile: NativeContentProfile,
    pub(super) prefix: SessionSeq,
    pub(super) limits: NativeLimits,
    pub(super) meter: &'v read_source::Meter,
    pub(super) budget: &'v MemoryBudget,
}

/// One (sequence, logical time) pair per resident outcome proves the
/// sequences unique and at most the prefix and the owner clock
/// nondecreasing along them. Every sequence once had its outcome; the ones
/// missing here left into a seal or a retirement bundle (F12), which the
/// meta counts (`outcomes - sealed` resident rows). The vector drops before
/// its permit on success and refusal paths.
struct Sequences {
    values: Vec<(u64, u64)>,
    _allocation: Allocation,
}
impl Sequences {
    fn new(count: usize, read: &ValidationRead<'_, '_>) -> Result<Self, NativeError> {
        let quote = prepare::array::<(u64, u64)>(count)?;
        read.charge(sum(
            count
                .checked_mul(size_of::<(u64, u64)>())
                .ok_or(ContractError::Capacity)?,
            2,
        )?)?;
        let allocation = read
            .budget
            .reserve(BudgetKind::Recovery, BudgetLane::Completion, quote)?
            .commit();
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if prepare::array::<(u64, u64)>(values.capacity())? > quote || values.capacity() < count {
            return Err(MemoryError::AllocationFailed.into());
        }
        Ok(Self {
            values,
            _allocation: allocation,
        })
    }
    fn mark(
        &mut self,
        sequence: SessionSeq,
        logical_time: u64,
        prefix: SessionSeq,
        read: &ValidationRead<'_, '_>,
    ) -> Result<(), NativeError> {
        read.charge(16)?;
        if sequence.0 == 0 || sequence > prefix || self.values.len() == self.values.capacity() {
            return Err(invalid());
        }
        self.values.push((sequence.0, logical_time));
        Ok(())
    }
    /// Unique sequences, all present, and a clock that never goes back.
    fn finish(mut self, count: usize, read: &ValidationRead<'_, '_>) -> Result<(), NativeError> {
        read.charge(
            sum(self.values.len(), 1)?
                .checked_mul(usize::try_from(usize::BITS).map_err(|_| ContractError::Capacity)?)
                .ok_or(ContractError::Capacity)?,
        )?;
        if self.values.len() != count {
            return Err(invalid());
        }
        self.values.sort_unstable();
        for pair in self.values.windows(2) {
            let [(previous, before), (next, after)] = pair else {
                return Err(invalid());
            };
            if previous >= next || before > after {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Counts {
    meta: Meta,
    seals: usize,
    contents: usize,
    claim_identities: usize,
    definition_identities: usize,
    incoming_links: usize,
    active_monitor_links: usize,
    retired_cycles: usize,
    scope_monitors: usize,
    declared_links: usize,
    active_roots: usize,
    registrations: usize,
    declared_definitions: usize,
    works: usize,
    diagnostics: usize,
    receipt_epochs: usize,
}
impl Counts {
    fn row(&mut self, row: &Row) -> Result<(), NativeError> {
        match row {
            Row::Claim(_) => increment(&mut self.meta.claims),
            Row::Definition(_) => increment(&mut self.meta.definitions),
            Row::Evaluation(_) => increment(&mut self.meta.evaluations),
            Row::Artifact(_) => increment(&mut self.meta.artifacts),
            Row::Accepted(_) | Row::MissingResult(_) | Row::DeliveryResult(_) => {
                increment(&mut self.meta.results)
            }
            Row::Receipt(_) => increment(&mut self.meta.receipts),
            Row::Response(_) => increment(&mut self.meta.responses),
            Row::ResultTestament(_) => increment(&mut self.meta.result_testaments),
            Row::Monitor(_) => increment(&mut self.meta.monitors),
            Row::MonitorLink(value) => {
                increment(&mut self.meta.monitor_links)?;
                if value.is_some() {
                    increment(&mut self.active_monitor_links)?;
                }
                Ok(())
            }
            Row::CreationResult(_) => increment(&mut self.meta.creation_results),
            Row::Outcome(_) => increment(&mut self.meta.outcomes),
            Row::Epochs(_) => increment(&mut self.meta.principals),
            Row::Seal(_) => increment(&mut self.seals),
            Row::Event(_) => increment(&mut self.meta.events),
            Row::ClaimContent(_) => increment(&mut self.contents),
            Row::ClaimIdentity(_) => increment(&mut self.claim_identities),
            Row::DefinitionIdentity(_) => increment(&mut self.definition_identities),
            Row::IncomingLink(_) => increment(&mut self.incoming_links),
            Row::RetiredCycle(_) => increment(&mut self.retired_cycles),
            Row::Work(_) => increment(&mut self.works),
            Row::Diagnostic(_) => increment(&mut self.diagnostics),
            Row::LegacyTestament(_)
            | Row::LegacyEvidenceSet(_)
            | Row::LegacyRun(_)
            | Row::LegacyDefinition(_) => increment(&mut self.meta.legacy),
            _ => Ok(()),
        }
    }
    fn check(&self, expected: Meta, read: &ValidationRead<'_, '_>) -> Result<(), NativeError> {
        let pairs = [
            (self.meta.claims, expected.claims, read.limits.claims),
            // Resident outcomes (F12): the lifetime count less what left.
            (
                self.meta.outcomes,
                expected.outcomes.saturating_sub(expected.sealed),
                read.limits.outcomes,
            ),
            (
                self.meta.principals,
                expected.principals,
                read.limits.principals,
            ),
            (self.seals, self.seals, read.limits.seals),
            (self.meta.events, expected.events, read.limits.events),
            (
                self.meta.definitions,
                expected.definitions,
                read.limits.definitions,
            ),
            (
                self.meta.evaluations,
                expected.evaluations,
                read.limits.evaluations,
            ),
            (
                self.meta.artifacts,
                expected.artifacts,
                read.limits.artifacts,
            ),
            (self.meta.results, expected.results, read.limits.results),
            (self.meta.receipts, expected.receipts, read.limits.receipts),
            (
                self.meta.responses,
                expected.responses,
                read.limits.responses,
            ),
            (
                self.meta.result_testaments,
                expected.result_testaments,
                read.limits.claims,
            ),
            (self.meta.monitors, expected.monitors, read.limits.monitors),
            (
                self.meta.monitor_links,
                expected.monitor_links,
                read.limits.monitor_links,
            ),
            (
                self.meta.creation_results,
                expected.creation_results,
                read.limits.outcomes,
            ),
            (self.meta.legacy, expected.legacy, read.limits.legacy_rows),
        ];
        read.charge(sum(pairs.len(), 1)?)?;
        for (actual, expected, limit) in pairs {
            if actual != expected {
                return Err(invalid());
            }
            if actual > limit {
                return Err(ContractError::Capacity.into());
            }
        }
        if u64::try_from(expected.outcomes).map_err(|_| ContractError::Capacity)? != read.prefix.0
            || expected.sealed > expected.outcomes
        {
            return Err(invalid());
        }
        if read.profile == NativeContentProfile::AuthoredV1 {
            if self.contents != self.meta.claims
                || self.claim_identities != self.meta.claims
                || self.definition_identities != self.meta.definitions
                || self.meta.creation_results > self.meta.outcomes
            {
                return Err(invalid());
            }
        } else if self.contents != 0
            || self.claim_identities != 0
            || self.definition_identities != 0
            || self.meta.creation_results != 0
        {
            return Err(invalid());
        }
        Ok(())
    }
}

/// The complete detached owner remains borrowed until counts, object references,
/// publication history and derived indices all agree. Scratch permits disappear
/// before the caller receives a publishable owner. Prefix zero is empty genesis.
#[allow(clippy::too_many_arguments)]
pub(super) fn validate(
    root: RangeHydrationView<'_, Key, Row>,
    ledger: LedgerId,
    profile: NativeContentProfile,
    prefix: SessionSeq,
    limits: NativeLimits,
    meter: &read_source::Meter,
    budget: &MemoryBudget,
) -> Result<(), NativeError> {
    let read = ValidationRead {
        root: &root,
        ledger,
        profile,
        prefix,
        limits,
        meter,
        budget,
    };
    read.charge(64)?;
    if ledger.tenant.is_zero() || ledger.session.is_zero() {
        return Err(ContractError::WrongLedger.into());
    }
    if prefix.0 == 0 {
        return if root.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    let expected = match read.require(Key::Meta)? {
        Row::Meta(value) => **value,
        _ => return Err(invalid()),
    };
    let mut counts = Counts::default();
    read.charge(
        sum(root.len(), 1)?
            .checked_mul(256)
            .ok_or(ContractError::Capacity)?,
    )?;
    for entry in root.entries() {
        mutation::check_family(entry.key, &entry.value)?;
        counts.row(&entry.value)?;
    }
    counts.check(expected, &read)?;
    let mut sequences = Sequences::new(counts.meta.outcomes, &read)?;
    let history = super::read_validate_evidence::HistoryIndex::build(&read)?;
    let (mut incoming, mut monitors, mut retired, mut total_events, mut last_time) =
        (0usize, 0usize, 0usize, 0usize, 0u64);
    let (mut works, mut diagnostics) = (0usize, 0usize);
    let (mut archived_events, mut retired_events) = (0usize, 0usize);
    let expected_seals = u64::try_from(expected.seals).map_err(|_| ContractError::Capacity)?;
    let mut next_seal = 1u64;
    read.charge(sum(root.len(), 1)?)?;
    for entry in root.entries() {
        let key = entry.key;
        let row = &entry.value;
        read.charge(1024)?;
        match (key, row) {
            (Key::Outcome(invocation), Row::Outcome(value)) => {
                read_rows::check_fixed(key, row, ledger)?;
                sequences.mark(value.sequence, value.logical_time, prefix, &read)?;
                if value.sequence > prefix || value.logical_time > expected.logical_time {
                    return Err(invalid());
                }
                // Resident is what is open (F12): a request's generation at
                // or above its principal's sealed floor; a timer's claim
                // not retired (its outcome left with the family).
                match invocation {
                    NativeInvocation::Request(request) => {
                        let Some(Row::Epochs(window)) = read.get(Key::Epochs(request.principal))?
                        else {
                            return Err(invalid());
                        };
                        if request.epoch.0 < window.sealed.0 {
                            return Err(invalid());
                        }
                    }
                    NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey { claim, .. })
                    | NativeInvocation::MonitorDeadline(NativeMonitorDeadlineKey {
                        claim, ..
                    }) => {
                        if read.get(Key::Retired(claim))?.is_some() {
                            return Err(invalid());
                        }
                    }
                    NativeInvocation::EvaluationDeadline(deadline) => {
                        if read.get(Key::Retired(deadline.evaluation.claim))?.is_some() {
                            return Err(invalid());
                        }
                    }
                    NativeInvocation::Import
                    | NativeInvocation::Retirement(_)
                    | NativeInvocation::Seal(_) => {}
                }
                if value.sequence == prefix {
                    last_time = value.logical_time;
                }
                let count = usize::try_from(value.events).map_err(|_| ContractError::Capacity)?;
                total_events = sum(total_events, count)?;
                read.charge(sum(count, 1)?)?;
                // An event whose object retired to the archive left with it
                // (26 §4); the retired continuations account for every one.
                for ordinal in 0..value.events {
                    match read.get(Key::Event(value.sequence, ordinal))? {
                        Some(Row::Event(row)) => {
                            let event = row.get().ok_or_else(invalid)?.expand(ledger);
                            if event.invocation != invocation
                                || event.sequence != value.sequence
                                || event.ordinal != ordinal
                            {
                                return Err(invalid());
                            }
                        }
                        Some(_) => return Err(invalid()),
                        None => archived_events = sum(archived_events, 1)?,
                    }
                }
                if profile == NativeContentProfile::AuthoredV1
                    && value.operation == NativeOperation::Create
                    && !matches!(
                        read.require(Key::CreationResult(invocation))?,
                        Row::CreationResult(_)
                    )
                {
                    return Err(invalid());
                }
            }
            (Key::Event(sequence, ordinal), Row::Event(value)) => {
                let event = value.get().ok_or_else(invalid)?.expand(ledger);
                if event.sequence != sequence || event.ordinal != ordinal {
                    return Err(invalid());
                }
                match read.get(Key::Outcome(event.invocation))? {
                    Some(Row::Outcome(outcome)) => {
                        if outcome.sequence != sequence || ordinal >= outcome.events {
                            return Err(invalid());
                        }
                    }
                    Some(_) => return Err(invalid()),
                    // An event of a live object whose request's outcome was
                    // sealed (F12): its generation is below the sealed floor.
                    None => match event.invocation {
                        NativeInvocation::Request(request) => {
                            let Some(Row::Epochs(window)) =
                                read.get(Key::Epochs(request.principal))?
                            else {
                                return Err(invalid());
                            };
                            if request.epoch.0 >= window.sealed.0 || sequence > prefix {
                                return Err(invalid());
                            }
                        }
                        _ => return Err(invalid()),
                    },
                }
            }
            (Key::IncomingHead(target), Row::IncomingHead(head)) => {
                read_rows::check_fixed(key, row, ledger)?;
                incoming = sum(incoming, links::incoming(target, **head, &read)?)?;
            }
            (Key::MonitorHead(target), Row::MonitorHead(head)) => {
                read_rows::check_fixed(key, row, ledger)?;
                monitors = sum(monitors, links::monitors(target, **head, &read)?)?;
            }
            (Key::RetiredCycleHead(target), Row::RetiredCycleHead(head)) => {
                read_rows::check_fixed(key, row, ledger)?;
                retired = sum(retired, links::retired(target, **head, &read)?)?;
            }
            // A principal's window (F12): valid in itself, its ranges naming
            // seals that exist, and holding the resident request outcomes
            // of its open generations, counted under its affinity.
            (Key::Epochs(principal), Row::Epochs(window)) => {
                read_rows::check_fixed(key, row, ledger)?;
                if entry.heap_bytes != sum(window.heap_charge()?, entry.value.boxed_heap())?
                    || window
                        .ranges()
                        .iter()
                        .any(|range| range.seal == 0 || range.seal > expected_seals)
                {
                    return Err(invalid());
                }
                read.charge(sum(window.ranges().len(), 8)?)?;
                let mut held = [0u32; OPEN_EPOCHS];
                let first = Key::Outcome(NativeInvocation::Request(RequestKey {
                    principal,
                    epoch: window.floor,
                    id: RequestId([0; 16]),
                }));
                for entry in read.root.entries_from(&first, false) {
                    read.charge(1)?;
                    let Key::Outcome(NativeInvocation::Request(request)) = entry.key else {
                        break;
                    };
                    if request.principal != principal || request.epoch >= window.next() {
                        break;
                    }
                    let at = usize::try_from(request.epoch.0.saturating_sub(window.floor.0))
                        .map_err(|_| ContractError::Capacity)?;
                    let slot = held.get_mut(at).ok_or_else(invalid)?;
                    *slot = slot.checked_add(1).ok_or_else(invalid)?;
                }
                for (slot, count) in held.iter().zip(window.counts.iter()) {
                    if *slot != count.outcomes {
                        return Err(invalid());
                    }
                }
            }
            // A seal's row (F12): the rows cover the ordinals 1..=seals
            // exactly once, in order.
            (Key::Seal(ordinal), Row::Seal(value)) => {
                read_rows::check_fixed(key, row, ledger)?;
                read.charge(2)?;
                if value.first != next_seal || value.sealed_at > prefix {
                    return Err(invalid());
                }
                next_seal = ordinal.checked_add(1).ok_or_else(invalid)?;
            }
            // A retired claim's continuation (26 §4): its rows are gone and
            // the archive holds them; nothing here is a live claim.
            (Key::Retired(claim), Row::Retired(value)) => {
                read_rows::check_fixed(key, row, ledger)?;
                read.charge(2)?;
                if value.retired_at > read.prefix
                    || read.root.get(&Key::Claim(claim)).is_some()
                    || read.root.get(&Key::ClaimContent(claim)).is_some()
                {
                    return Err(invalid());
                }
                let count = usize::try_from(value.events).map_err(|_| ContractError::Capacity)?;
                retired_events = sum(retired_events, count)?;
            }
            (Key::Cycle(key), Row::Cycle(value)) => {
                read_rows::check_fixed(Key::Cycle(key), row, ledger)?;
                let (work_count, diagnostic_count) = links::cycle(key, **value, &read)?;
                works = sum(works, work_count)?;
                diagnostics = sum(diagnostics, diagnostic_count)?;
            }
            (Key::Claim(id), Row::Claim(value)) => {
                objects::claim(id, value, &read, &history, &mut counts)?
            }
            (Key::Definition(id), Row::Definition(value)) => {
                objects::definition(id, value, &read, &history)?
            }
            (Key::Evaluation(key), Row::Evaluation(value)) => {
                objects::evaluation(key, value, &read, &history)?
            }
            (
                Key::ClaimContent(_)
                | Key::ClaimIdentity(..)
                | Key::DefinitionIdentity(..)
                | Key::CreationResult(_),
                _,
            ) => objects::authored(key, row, &read)?,
            (
                Key::LegacyTestament(_)
                | Key::LegacyEvidenceSet(_)
                | Key::LegacyRun(..)
                | Key::LegacyDefinition(_),
                _,
            ) => objects::legacy(key, row, &read)?,
            (Key::ResultTestament(id), Row::ResultTestament(value)) => {
                super::read_validate_audit::audit(&read, id, value.get().ok_or_else(invalid)?)?
            }
            (Key::ClaimResultTestament(claim), Row::ClaimResultTestament(id)) => {
                read_rows::check_fixed(key, row, ledger)?;
                read.claim(claim)?;
                let value = match read.require(Key::ResultTestament(*id))? {
                    Row::ResultTestament(value) => value.get().ok_or_else(invalid)?,
                    _ => return Err(invalid()),
                };
                if value.testament().claim() != claim {
                    return Err(invalid());
                }
            }
            (
                Key::Artifact(_)
                | Key::ArtifactIdentity(_)
                | Key::Accepted(_)
                | Key::DeliveryResult(_)
                | Key::MissingResult(_)
                | Key::Work(_)
                | Key::Diagnostic(_)
                | Key::Response(_)
                | Key::WorkSlot(..),
                _,
            ) => super::read_validate_evidence::validate(key, row, &read, &history)?,
            (
                Key::Receipt(_)
                | Key::Monitor(_)
                | Key::MonitorLink(..)
                | Key::IncomingLink(..)
                | Key::RetiredCycle(_),
                _,
            ) => {
                read_rows::check_fixed(key, row, ledger)?;
                links::row(key, row, &read, &history)?;
            }
            (Key::Meta, Row::Meta(_)) => (),
            (
                Key::ByIssuer(..)
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
                | Key::ByObject(..),
                Row::Index,
            ) => super::read_validate_index::check_row(key, row, &read)?,
            _ => return Err(invalid()),
        }
    }
    sequences.finish(counts.meta.outcomes, &read)?;
    // Every event ever published is resident, left with a retired member
    // (its continuation counts it) or belongs to an outcome that left (the
    // meta counts those, F12); what is missing among the resident outcomes'
    // events is at most what retired.
    if next_seal.checked_sub(1) != Some(expected_seals)
        || sum(total_events, expected.sealed_events)? != sum(counts.meta.events, retired_events)?
        || archived_events > retired_events
    {
        return Err(invalid());
    }
    if incoming != counts.incoming_links
        || incoming != counts.declared_links
        || monitors != counts.active_monitor_links
        || monitors != counts.active_roots
        || retired != counts.retired_cycles
        || counts.scope_monitors != counts.meta.monitors
        || works != counts.works
        || diagnostics != counts.diagnostics
        || counts.registrations != counts.meta.evaluations
        || counts.declared_definitions != counts.meta.definitions
        || counts.receipt_epochs != counts.meta.receipts
        || last_time != expected.logical_time
    {
        return Err(invalid());
    }
    Ok(())
}
impl ValidationRead<'_, '_> {
    pub(super) fn charge(&self, amount: usize) -> Result<(), NativeError> {
        self.meter
            .charge(amount)
            .map_err(|error| read_source::model_error(error).into())
    }
    pub(super) fn get(&self, key: Key) -> Result<Option<&Row>, NativeError> {
        // Fixed-width Key ordering and a bounded directory binary search.
        self.charge(const { (usize::BITS as usize + 1) * 64 })?;
        Ok(self.root.get(&key))
    }
    pub(super) fn require(&self, key: Key) -> Result<&Row, NativeError> {
        self.get(key)?.ok_or_else(invalid)
    }
    pub(super) fn claim(&self, id: ClaimId) -> Result<&ClaimState, NativeError> {
        match self.require(Key::Claim(id))? {
            Row::Claim(row) => row.claim().ok_or_else(invalid),
            _ => Err(invalid()),
        }
    }
    pub(super) fn definition(
        &self,
        id: ValidationId,
    ) -> Result<&validation::Declaration, NativeError> {
        match self.require(Key::Definition(id))? {
            Row::Definition(row) => row.get().ok_or_else(invalid),
            _ => Err(invalid()),
        }
    }
    pub(super) fn artifact(&self, id: ArtifactId) -> Result<&NativeArtifact, NativeError> {
        match self.require(Key::Artifact(id))? {
            Row::Artifact(row) => row.get().ok_or_else(invalid),
            _ => Err(invalid()),
        }
    }
    pub(super) fn event(
        &self,
        sequence: SessionSeq,
        ordinal: u32,
    ) -> Result<NativeEvent, NativeError> {
        match self.require(Key::Event(sequence, ordinal))? {
            Row::Event(row) => Ok(row.get().ok_or_else(invalid)?.expand(self.ledger)),
            _ => Err(invalid()),
        }
    }
}
