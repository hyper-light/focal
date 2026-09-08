//! Cross-row validation of a detached decoded root before publication.
//! Checks establish retained consistency, never participant authentication from
//! the checkpoint checksum. All reads and scratch use the enclosing allowance.
use super::*;
use focal_memory::RangeHydrationView;
use focal_memory::{Allocation, BudgetKind, BudgetLane};

#[path = "read_validate_links.rs"]
mod links;
#[path = "read_validate_objects.rs"]
mod objects;
#[cfg(test)]
#[path = "read_validate_tests.rs"]
mod tests;

fn sum(left: usize, right: usize) -> Result<usize, NativeError> {
    left.checked_add(right).ok_or_else(|| ContractError::Capacity.into())
}
fn increment(value: &mut usize) -> Result<(), NativeError> { *value = sum(*value, 1)?; Ok(()) }

pub(super) fn invalid() -> NativeError { ContractError::InvalidManifest.into() }

pub(super) struct ValidationRead<'v, 'r> {
    pub(super) root: &'v RangeHydrationView<'r, Key, Row>,
    pub(super) ledger: LedgerId,
    pub(super) profile: NativeContentProfile,
    pub(super) prefix: SessionSeq,
    pub(super) limits: NativeLimits,
    pub(super) meter: &'v read_source::Meter,
    pub(super) budget: &'v MemoryBudget,
}

/// The one scalar bit per retained native outcome proves unique, contiguous
/// sequences without rescanning invocation-ordered rows for every sequence.
/// The vector drops before its permit on every success and refusal path.
struct Sequences { bits: Vec<u8>, _allocation: Allocation }
impl Sequences {
    fn new(count: usize, read: &ValidationRead<'_, '_>) -> Result<Self, NativeError> {
        let bytes = count.checked_div(8).and_then(|n| n.checked_add(usize::from(!count.is_multiple_of(8))))
            .ok_or(ContractError::Capacity)?;
        let quote = prepare::array::<u8>(bytes)?;
        read.charge(sum(bytes, 1)?)?;
        let allocation = read.budget.reserve(BudgetKind::Recovery, BudgetLane::Completion, quote)?.commit();
        let mut bits = Vec::new();
        bits.try_reserve_exact(bytes).map_err(|_| MemoryError::AllocationFailed)?;
        if prepare::array::<u8>(bits.capacity())? > quote || bits.capacity() < bytes {
            return Err(MemoryError::AllocationFailed.into());
        }
        bits.resize(bytes, 0);
        Ok(Self { bits, _allocation: allocation })
    }
    fn mark(&mut self, sequence: SessionSeq, count: usize, read: &ValidationRead<'_, '_>) -> Result<(), NativeError> {
        read.charge(8)?;
        let index = usize::try_from(sequence.0.checked_sub(1).ok_or_else(invalid)?)
            .map_err(|_| ContractError::Capacity)?;
        if index >= count { return Err(invalid()); }
        let bit = 1u8.checked_shl(u32::try_from(index % 8).map_err(|_| ContractError::Capacity)?)
            .ok_or(ContractError::Capacity)?;
        let slot = self.bits.get_mut(index / 8).ok_or_else(invalid)?;
        if *slot & bit != 0 { return Err(invalid()); }
        *slot |= bit;
        Ok(())
    }
}

#[derive(Default)]
struct Counts {
    meta: Meta,
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
}
impl Counts {
    fn row(&mut self, row: &Row) -> Result<(), NativeError> {
        match row {
            Row::Claim(_) => increment(&mut self.meta.claims),
            Row::Definition(_) => increment(&mut self.meta.definitions),
            Row::Evaluation(_) => increment(&mut self.meta.evaluations),
            Row::Artifact(_) => increment(&mut self.meta.artifacts),
            Row::Accepted(_) | Row::MissingResult(_) | Row::DeliveryResult(_) => increment(&mut self.meta.results),
            Row::Receipt(_) => increment(&mut self.meta.receipts),
            Row::Response(_) => increment(&mut self.meta.responses),
            Row::ResultTestament(_) => increment(&mut self.meta.result_testaments),
            Row::Monitor(_) => increment(&mut self.meta.monitors),
            Row::MonitorLink(value) => {
                increment(&mut self.meta.monitor_links)?;
                if value.is_some() { increment(&mut self.active_monitor_links)?; }
                Ok(())
            }
            Row::CreationResult(_) => increment(&mut self.meta.creation_results),
            Row::Outcome(_) => increment(&mut self.meta.outcomes),
            Row::Event(_) => increment(&mut self.meta.events),
            Row::ClaimContent(_) => increment(&mut self.contents),
            Row::ClaimIdentity(_) => increment(&mut self.claim_identities),
            Row::DefinitionIdentity(_) => increment(&mut self.definition_identities),
            Row::IncomingLink(_) => increment(&mut self.incoming_links),
            Row::RetiredCycle(_) => increment(&mut self.retired_cycles),
            Row::Work(_) => increment(&mut self.works),
            Row::Diagnostic(_) => increment(&mut self.diagnostics),
            _ => Ok(()),
        }
    }
    fn check(&self, expected: Meta, read: &ValidationRead<'_, '_>) -> Result<(), NativeError> {
        let pairs = [
            (self.meta.claims, expected.claims, read.limits.claims),
            (self.meta.outcomes, expected.outcomes, read.limits.outcomes),
            (self.meta.events, expected.events, read.limits.events),
            (self.meta.definitions, expected.definitions, read.limits.definitions),
            (self.meta.evaluations, expected.evaluations, read.limits.evaluations),
            (self.meta.artifacts, expected.artifacts, read.limits.artifacts),
            (self.meta.results, expected.results, read.limits.results),
            (self.meta.receipts, expected.receipts, read.limits.receipts),
            (self.meta.responses, expected.responses, read.limits.responses),
            (self.meta.result_testaments, expected.result_testaments, read.limits.claims),
            (self.meta.monitors, expected.monitors, read.limits.monitors),
            (self.meta.monitor_links, expected.monitor_links, read.limits.monitor_links),
            (self.meta.creation_results, expected.creation_results, read.limits.outcomes),
        ];
        read.charge(sum(pairs.len(), 1)?)?;
        for (actual, expected, limit) in pairs {
            if actual != expected { return Err(invalid()); }
            if actual > limit { return Err(ContractError::Capacity.into()); }
        }
        if u64::try_from(self.meta.outcomes).map_err(|_| ContractError::Capacity)? != read.prefix.0 {
            return Err(invalid());
        }
        if read.profile == NativeContentProfile::AuthoredV1 {
            if self.contents != self.meta.claims || self.claim_identities != self.meta.claims
                || self.definition_identities != self.meta.definitions || self.meta.creation_results > self.meta.outcomes
            { return Err(invalid()); }
        } else if self.contents != 0 || self.claim_identities != 0 || self.definition_identities != 0
            || self.meta.creation_results != 0 { return Err(invalid()); }
        Ok(())
    }
}

/// The complete detached owner remains borrowed until counts, object references,
/// publication history and derived indices all agree. Scratch permits disappear
/// before the caller receives a publishable owner. Prefix zero is empty genesis.
#[allow(clippy::too_many_arguments)]
pub(super) fn validate(
    root: RangeHydrationView<'_, Key, Row>, ledger: LedgerId, profile: NativeContentProfile,
    prefix: SessionSeq, limits: NativeLimits, meter: &read_source::Meter, budget: &MemoryBudget,
) -> Result<(), NativeError> {
    let read = ValidationRead { root: &root, ledger, profile, prefix, limits, meter, budget };
    read.charge(64)?;
    if ledger.tenant.is_zero() || ledger.session.is_zero() { return Err(ContractError::WrongLedger.into()); }
    if prefix.0 == 0 { return if root.is_empty() { Ok(()) } else { Err(invalid()) }; }
    let expected = match read.require(Key::Meta)? { Row::Meta(value) => *value, _ => return Err(invalid()) };
    let mut counts = Counts::default();
    read.charge(sum(root.len(), 1)?)?;
    for entry in root.entries() {
        mutation::check_family(entry.key, &entry.value)?;
        counts.row(&entry.value)?;
    }
    counts.check(expected, &read)?;
    let mut sequences = Sequences::new(counts.meta.outcomes, &read)?;
    let history = super::read_validate_evidence::HistoryIndex::build(&read)?;
    let (mut incoming, mut monitors, mut retired, mut total_events, mut last_time) = (0usize, 0usize, 0usize, 0usize, 0u64);
    let (mut works, mut diagnostics) = (0usize, 0usize);
    read.charge(sum(root.len(), 1)?)?;
    for entry in root.entries() {
        let key = entry.key;
        let row = &entry.value;
        read.charge(1024)?;
        match (key, row) {
            (Key::Outcome(_), Row::Outcome(value)) => {
                read_rows::check_fixed(key, row, ledger)?;
                sequences.mark(value.sequence, counts.meta.outcomes, &read)?;
                if value.sequence > prefix || value.logical_time > expected.logical_time { return Err(invalid()); }
                if value.sequence == prefix { last_time = value.logical_time; }
                let count = usize::try_from(value.events).map_err(|_| ContractError::Capacity)?;
                total_events = sum(total_events, count)?;
                read.charge(sum(count, 1)?)?;
                for ordinal in 0..value.events {
                    let event = read.event(value.sequence, ordinal)?;
                    if event.invocation != value.invocation || event.sequence != value.sequence || event.ordinal != ordinal {
                        return Err(invalid());
                    }
                }
                if profile == NativeContentProfile::AuthoredV1 && value.operation == NativeOperation::Create {
                    if !matches!(read.require(Key::CreationResult(value.invocation))?, Row::CreationResult(_)) { return Err(invalid()); }
                }
            }
            (Key::Event(sequence, ordinal), Row::Event(value)) => {
                let event = value.get().ok_or_else(invalid)?.expand(ledger);
                let outcome = match read.require(Key::Outcome(event.invocation))? {
                    Row::Outcome(value) => value, _ => return Err(invalid()),
                };
                if event.sequence != sequence || event.ordinal != ordinal || outcome.sequence != sequence || ordinal >= outcome.events {
                    return Err(invalid());
                }
            }
            (Key::IncomingHead(target), Row::IncomingHead(head)) => {
                read_rows::check_fixed(key, row, ledger)?;
                incoming = sum(incoming, links::incoming(target, *head, &read)?)?;
            }
            (Key::MonitorHead(target), Row::MonitorHead(head)) => {
                read_rows::check_fixed(key, row, ledger)?;
                monitors = sum(monitors, links::monitors(target, *head, &read)?)?;
            }
            (Key::RetiredCycleHead(target), Row::RetiredCycleHead(head)) => {
                read_rows::check_fixed(key, row, ledger)?;
                retired = sum(retired, links::retired(target, *head, &read)?)?;
            }
            (Key::Cycle(key), Row::Cycle(value)) => {
                read_rows::check_fixed(Key::Cycle(key), row, ledger)?;
                let (work_count, diagnostic_count) = links::cycle(key, *value, &read)?;
                works = sum(works, work_count)?;
                diagnostics = sum(diagnostics, diagnostic_count)?;
            }
            (Key::Claim(id), Row::Claim(value)) => objects::claim(id, value, &read, &history, &mut counts)?,
            (Key::Definition(id), Row::Definition(value)) => objects::definition(id, value, &read, &history)?,
            (Key::Evaluation(key), Row::Evaluation(value)) => objects::evaluation(key, value, &read, &history)?,
            (Key::ClaimContent(_) | Key::ClaimIdentity(..) | Key::DefinitionIdentity(..) | Key::CreationResult(_), _) => objects::authored(key, row, &read)?,
            (Key::ResultTestament(id), Row::ResultTestament(value)) => super::read_validate_audit::audit(&read, id, value.get().ok_or_else(invalid)?)?,
            (Key::ClaimResultTestament(claim), Row::ClaimResultTestament(id)) => {
                read_rows::check_fixed(key, row, ledger)?;
                read.claim(claim)?;
                let value = match read.require(Key::ResultTestament(*id))? { Row::ResultTestament(value) => value.get().ok_or_else(invalid)?, _ => return Err(invalid()) };
                if value.testament().claim() != claim { return Err(invalid()); }
            }
            (Key::Artifact(_) | Key::ArtifactIdentity(_) | Key::Accepted(_) | Key::DeliveryResult(_) | Key::MissingResult(_)
                | Key::Work(_) | Key::Diagnostic(_) | Key::Response(_) | Key::WorkSlot(..), _) =>
                super::read_validate_evidence::validate(key, row, &read, &history)?,
            (Key::Receipt(_) | Key::Monitor(_) | Key::MonitorLink(..) | Key::IncomingLink(..) | Key::RetiredCycle(_), _) => {
                read_rows::check_fixed(key, row, ledger)?;
                links::row(key, row, &read, &history)?;
            }
            (Key::Meta, Row::Meta(_)) => (),
            _ => return Err(invalid()),
        }
    }
    if incoming != counts.incoming_links || incoming != counts.declared_links
        || monitors != counts.active_monitor_links || monitors != counts.active_roots
        || retired != counts.retired_cycles || counts.scope_monitors != counts.meta.monitors
        || works != counts.works || diagnostics != counts.diagnostics
        || counts.registrations != counts.meta.evaluations || counts.declared_definitions != counts.meta.definitions
        || total_events != counts.meta.events || last_time != expected.logical_time { return Err(invalid()); }
    Ok(())
}
impl ValidationRead<'_, '_> {
    pub(super) fn charge(&self, amount: usize) -> Result<(), NativeError> {
        self.meter.charge(amount).map_err(|error| read_source::model_error(error).into())
    }
    pub(super) fn get(&self, key: Key) -> Result<Option<&Row>, NativeError> {
        // Fixed-width Key ordering and a bounded directory binary search.
        self.charge((usize::BITS as usize + 1) * 64)?;
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
    pub(super) fn definition(&self, id: ValidationId) -> Result<&validation::Declaration, NativeError> {
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
    pub(super) fn event(&self, sequence: SessionSeq, ordinal: u32) -> Result<NativeEvent, NativeError> {
        match self.require(Key::Event(sequence, ordinal))? {
            Row::Event(row) => Ok(row.get().ok_or_else(invalid)?.expand(self.ledger)),
            _ => Err(invalid()),
        }
    }
}
