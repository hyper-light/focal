//! The storage order of native keys (doc 25 §3): the rows of one object sit
//! together, the rows of an index family sit together per bucket, and the
//! entry-local rows sit under one control affinity. Every ordered structure
//! (the row store, records, checkpoints, scans) uses this one order, so a
//! contiguous span of keys is a contiguous span of one object's state, which
//! is what a range can hold and move as a unit.
//!
//! The order is (affinity, family, fields): fields in declaration order with
//! every nested enum tagged, so it is a total order that agrees with equality
//! and keeps every prefix scan of one family within one affinity contiguous.
use super::*;

/// The affinity of the entry-local and ledger-wide rows.
const CONTROL: [u8; 16] = [0; 16];

/// Where an object's rows live (25 §6): the affinity a read for it routes
/// by, so a holder of the member at that affinity serves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeLocation {
    Claim(ClaimId),
    Artifact(ArtifactId),
    Definition(ValidationId),
    Testament(TestamentId),
    Monitor(MonitorId),
    /// Receipts sit together in one table.
    Receipt,
    /// A principal's outcomes and creation results.
    Principal(ParticipantId),
    /// Entry-local and ledger-wide rows: events, timers, the standing.
    Control,
}
pub(super) fn location_affinity(location: NativeLocation) -> [u8; 16] {
    match location {
        NativeLocation::Claim(id) => id.0,
        NativeLocation::Artifact(id) => id.0,
        NativeLocation::Definition(id) => id.0,
        NativeLocation::Testament(id) => id.0,
        NativeLocation::Monitor(id) => id.0,
        NativeLocation::Receipt => affinity(&Key::Receipt(ReceiptId::from_u128(1))),
        NativeLocation::Principal(id) => id.0,
        NativeLocation::Control => CONTROL,
    }
}
/// The greatest affinity, for the end sentinel alone.
const LAST: [u8; 16] = [0xff; 16];

/// A bucket affinity for an index family: the family tag and up to fourteen
/// discriminator bytes, so one family's rows for one discriminator sit
/// together and scans of them are contiguous.
fn bucket(tag: u16, discriminator: &[u8]) -> [u8; 16] {
    let mut affinity = [0u8; 16];
    let [low, high] = tag.to_le_bytes();
    affinity[0] = 0xb0;
    affinity[1] = low;
    affinity[2] = high;
    for (target, source) in affinity.iter_mut().skip(3).zip(discriminator) {
        *target = *source;
    }
    affinity
}
fn hash_bucket(tag: u16, hash: &ContentHash) -> [u8; 16] {
    bucket(tag, hash.0.get(..13).unwrap_or(&[]))
}
fn low(hash: &ContentHash) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    for (target, source) in bytes.iter_mut().zip(hash.0.iter()) {
        *target = *source;
    }
    bytes
}
fn high(hash: &ContentHash) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    for (target, source) in bytes.iter_mut().zip(hash.0.iter().skip(16)) {
        *target = *source;
    }
    bytes
}

/// The family of a key: its variant, numbered in declaration order.
pub(super) fn family(key: &Key) -> u16 {
    match key {
        Key::IncomingHead(_) => 0,
        Key::IncomingLink(..) => 1,
        Key::Monitor(_) => 2,
        Key::MonitorHead(_) => 3,
        Key::MonitorLink(..) => 4,
        Key::MissingResult(_) => 5,
        Key::Meta => 6,
        Key::Claim(_) => 7,
        Key::Definition(_) => 8,
        Key::Evaluation(_) => 9,
        Key::Artifact(_) => 10,
        Key::ArtifactIdentity(_) => 11,
        Key::Accepted(_) => 12,
        Key::DeliveryResult(_) => 13,
        Key::Receipt(_) => 14,
        Key::Cycle(_) => 15,
        Key::RetiredCycleHead(_) => 16,
        Key::RetiredCycle(_) => 17,
        Key::Work(_) => 18,
        Key::WorkSlot(..) => 19,
        Key::Diagnostic(_) => 20,
        Key::Response(_) => 21,
        Key::ResultTestament(_) => 22,
        Key::ClaimResultTestament(_) => 23,
        Key::Outcome(_) => 24,
        Key::Event(..) => 25,
        Key::ClaimContent(_) => 26,
        Key::ClaimIdentity(..) => 27,
        Key::DefinitionIdentity(..) => 28,
        Key::CreationResult(_) => 29,
        Key::LegacyTestament(_) => 30,
        Key::LegacyEvidenceSet(_) => 31,
        Key::LegacyRun(..) => 32,
        Key::LegacyDefinition(_) => 33,
        Key::ByIssuer(..) => 34,
        Key::BySubject(..) => 35,
        Key::ByStatus(..) => 36,
        Key::ByAction(..) => 37,
        Key::ByScope(..) => 38,
        Key::ByRelation(..) => 39,
        Key::ByProducer(..) => 40,
        Key::ByArtifactKind(..) => 41,
        Key::BySchema(..) => 42,
        Key::ArtifactInput(..) => 43,
        Key::ByEvaluator(..) => 44,
        Key::ByVerdict(..) => 45,
        Key::ByCreated(..) => 46,
        Key::DueTimer(..) => 47,
        Key::ByObject(..) => 48,
        Key::Retired(_) => 49,
        Key::End => u16::MAX,
    }
}

/// Where a key lives: the object that owns it, a bucket of its index family,
/// or the control affinity.
pub(super) fn affinity(key: &Key) -> [u8; 16] {
    match key {
        Key::IncomingHead(c)
        | Key::IncomingLink(c, _)
        | Key::MonitorHead(c)
        | Key::MonitorLink(c, _)
        | Key::Claim(c)
        | Key::RetiredCycleHead(c)
        | Key::ClaimResultTestament(c)
        | Key::ClaimContent(c)
        | Key::Retired(c) => c.0,
        Key::Cycle(cycle) | Key::RetiredCycle(cycle) | Key::WorkSlot(cycle, _) => cycle.claim.0,
        Key::Evaluation(evaluation) => evaluation.claim.0,
        Key::MissingResult(result) | Key::Accepted(result) | Key::DeliveryResult(result) => {
            result.evaluation.claim.0
        }
        Key::Monitor(monitor) => monitor.0,
        Key::Definition(v) | Key::LegacyDefinition(v) | Key::LegacyRun(v, _) => v.0,
        Key::Artifact(a) | Key::Work(a) | Key::Diagnostic(a) => a.0,
        // Receipts are standalone fixed rows read by identity and listed in
        // identity order; they sit together as one table.
        Key::Receipt(_) => bucket(family(key), &[]),
        Key::Response(t) | Key::ResultTestament(t) | Key::LegacyTestament(t) => t.0,
        Key::LegacyEvidenceSet(set) => set.0,
        Key::Outcome(invocation) | Key::CreationResult(invocation) => match invocation {
            NativeInvocation::Request(request) => request.principal.0,
            NativeInvocation::EvaluationDeadline(key) => key.evaluation.claim.0,
            NativeInvocation::ClaimDeadline(key) => key.claim.0,
            NativeInvocation::MonitorDeadline(key) => key.claim.0,
            NativeInvocation::Import => CONTROL,
            NativeInvocation::Retirement(root) => root.0,
        },
        Key::ArtifactIdentity(hash) => low(hash),
        Key::ClaimIdentity(_, hash) | Key::DefinitionIdentity(_, hash) => low(hash),
        Key::ByIssuer(participant, _)
        | Key::BySubject(participant, _)
        | Key::ByProducer(participant, _)
        | Key::ByEvaluator(participant, _) => participant.0,
        Key::ByStatus(status, _) => bucket(family(key), &status.to_le_bytes()),
        Key::ByAction(action, _) => bucket(family(key), &action.to_le_bytes()),
        Key::ByVerdict(verdict, _) => bucket(family(key), &verdict.to_le_bytes()),
        Key::ByScope(kind, hash, _) => {
            let mut discriminator = [0u8; 14];
            let [low_byte, high_byte] = kind.to_le_bytes();
            discriminator[0] = low_byte;
            discriminator[1] = high_byte;
            for (target, source) in discriminator.iter_mut().skip(2).zip(hash.0.iter()) {
                *target = *source;
            }
            bucket(family(key), &discriminator)
        }
        Key::ByRelation(_, target, _) => target.0,
        Key::ByArtifactKind(hash, _) | Key::BySchema(hash, _) => hash_bucket(family(key), hash),
        Key::ArtifactInput(object, _) => object.0,
        Key::ByCreated(kind, ..) | Key::ByObject(kind, _) => {
            bucket(family(key), &kind.to_le_bytes())
        }
        Key::Meta | Key::Event(..) | Key::DueTimer(..) => CONTROL,
        Key::End => LAST,
    }
}

/// The complete order of one key: affinity, family, then every field in
/// declaration order with nested enums tagged. Distinct keys never share it.
/// Fields are one slot sequence in declaration order, so a time precedes
/// the identities after it and a tag precedes the fields it selects.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Slot {
    Empty,
    Id([u8; 16]),
    N(u64),
}
const SLOTS: usize = 10;
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct OrderKey {
    affinity: [u8; 16],
    family: u16,
    slots: [Slot; SLOTS],
}
struct Fields {
    slots: [Slot; SLOTS],
    next: usize,
}
impl Fields {
    fn new() -> Self {
        Self {
            slots: [Slot::Empty; SLOTS],
            next: 0,
        }
    }
    fn push(mut self, slot: Slot) -> Self {
        if let Some(target) = self.slots.get_mut(self.next) {
            *target = slot;
        }
        self.next = self.next.saturating_add(1);
        self
    }
    fn id(self, value: [u8; 16]) -> Self {
        self.push(Slot::Id(value))
    }
    fn n(self, value: u64) -> Self {
        self.push(Slot::N(value))
    }
    fn hash(self, hash: &ContentHash) -> Self {
        self.id(low(hash)).id(high(hash))
    }
    fn evaluation(self, key: &EvaluationKey) -> Self {
        let fields = self.id(key.claim.0).id(key.validation.0);
        let fields = match key.target {
            EvaluationTarget::Admission => fields.n(0),
            EvaluationTarget::Increment { artifact } => fields.n(1).id(artifact.0),
            EvaluationTarget::Work {
                response,
                slot,
                artifact,
            } => fields.n(2).id(response.0).n(u64::from(slot)).id(artifact.0),
            EvaluationTarget::MissingSlot { response, slot } => {
                fields.n(3).id(response.0).n(u64::from(slot))
            }
            EvaluationTarget::Delivery { response } => fields.n(4).id(response.0),
        };
        fields.n(key.generation)
    }
    fn result(self, key: &NativeResultKey) -> Self {
        self.evaluation(&key.evaluation).n(key.revision.0)
    }
    fn cycle(self, key: &NativeCycleKey) -> Self {
        self.id(key.claim.0)
            .id(key.receipt.0)
            .n(key.epoch)
            .n(u64::from(key.cycle))
    }
    fn invocation(self, invocation: &NativeInvocation) -> Self {
        match invocation {
            NativeInvocation::Request(request) => self
                .n(0)
                .id(request.principal.0)
                .n(request.epoch.0)
                .id(request.id.0),
            NativeInvocation::EvaluationDeadline(key) => self
                .n(1)
                .evaluation(&key.evaluation)
                .id(key.timer.0)
                .n(key.generation),
            NativeInvocation::ClaimDeadline(key) => {
                self.n(2).id(key.claim.0).id(key.timer.0).n(key.generation)
            }
            NativeInvocation::MonitorDeadline(key) => self
                .n(3)
                .id(key.claim.0)
                .id(key.monitor.0)
                .id(key.timer.0)
                .n(key.generation),
            NativeInvocation::Import => self.n(4),
            NativeInvocation::Retirement(root) => self.n(5).id(root.0),
        }
    }
    fn timer(self, target: &TimerTarget) -> Self {
        match target {
            TimerTarget::Claim(claim) => self.n(0).id(claim.0),
            TimerTarget::Evaluation(evaluation) => self.n(1).evaluation(evaluation),
            TimerTarget::Monitor(claim, monitor) => self.n(2).id(claim.0).id(monitor.0),
        }
    }
    fn finish(self, key: &Key) -> OrderKey {
        OrderKey {
            affinity: affinity(key),
            family: family(key),
            slots: self.slots,
        }
    }
}
fn order_key(key: &Key) -> OrderKey {
    let fields = Fields::new();
    let fields = match key {
        Key::IncomingHead(c)
        | Key::MonitorHead(c)
        | Key::Claim(c)
        | Key::RetiredCycleHead(c)
        | Key::Retired(c)
        | Key::ClaimResultTestament(c)
        | Key::ClaimContent(c) => fields.id(c.0),
        Key::IncomingLink(c, from) => fields.id(c.0).id(from.0),
        Key::Monitor(m) => fields.id(m.0),
        Key::MonitorLink(c, m) => fields.id(c.0).id(m.0),
        Key::MissingResult(result) | Key::Accepted(result) | Key::DeliveryResult(result) => {
            fields.result(result)
        }
        Key::Meta => fields,
        Key::Definition(v) | Key::LegacyDefinition(v) => fields.id(v.0),
        Key::Evaluation(evaluation) => fields.evaluation(evaluation),
        Key::Artifact(a) | Key::Work(a) | Key::Diagnostic(a) => fields.id(a.0),
        Key::ArtifactIdentity(hash) => fields.hash(hash),
        Key::Receipt(r) => fields.id(r.0),
        Key::Cycle(cycle) | Key::RetiredCycle(cycle) => fields.cycle(cycle),
        Key::WorkSlot(cycle, slot) => fields.cycle(cycle).n(u64::from(*slot)),
        Key::Response(t) | Key::ResultTestament(t) | Key::LegacyTestament(t) => fields.id(t.0),
        Key::Outcome(invocation) | Key::CreationResult(invocation) => fields.invocation(invocation),
        Key::Event(sequence, ordinal) => fields.n(sequence.0).n(u64::from(*ordinal)),
        Key::ClaimIdentity(kind, hash) | Key::DefinitionIdentity(kind, hash) => {
            fields.n(u64::from(*kind)).hash(hash)
        }
        Key::LegacyEvidenceSet(set) => fields.id(set.0),
        Key::LegacyRun(v, ordinal) => fields.id(v.0).n(u64::from(*ordinal)),
        Key::ByIssuer(p, c) | Key::BySubject(p, c) => fields.id(p.0).id(c.0),
        Key::ByStatus(code, c) | Key::ByAction(code, c) => fields.n(u64::from(*code)).id(c.0),
        Key::ByScope(kind, hash, c) => fields.n(u64::from(*kind)).hash(hash).id(c.0),
        Key::ByRelation(kind, target, source) => {
            fields.n(u64::from(*kind)).id(target.0).id(source.0)
        }
        Key::ByProducer(p, a) => fields.id(p.0).id(a.0),
        Key::ByArtifactKind(hash, a) | Key::BySchema(hash, a) => fields.hash(hash).id(a.0),
        Key::ArtifactInput(object, a) => fields.id(object.0).id(a.0),
        Key::ByEvaluator(p, v) => fields.id(p.0).id(v.0),
        Key::ByVerdict(verdict, result) => fields.n(u64::from(*verdict)).result(result),
        Key::ByCreated(kind, sequence, object) => {
            fields.n(u64::from(*kind)).n(sequence.0).id(object.0)
        }
        Key::DueTimer(time, target) => fields.n(*time).timer(target),
        Key::ByObject(kind, object) => fields.n(u64::from(*kind)).id(object.0),
        Key::End => fields,
    };
    fields.finish(key)
}
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        order_key(self).cmp(&order_key(other))
    }
}

#[cfg(test)]
#[path = "layout_order_tests.rs"]
mod tests;
