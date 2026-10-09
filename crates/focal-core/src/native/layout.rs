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
        Key::Epochs(_) => 50,
        Key::Seal(_) => 51,
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
            // Retirements' and seals' own outcomes sit together under the
            // control affinity: a seal walks them as one range (F12).
            NativeInvocation::Import
            | NativeInvocation::Retirement(_)
            | NativeInvocation::Seal(_) => CONTROL,
        },
        Key::Epochs(_) | Key::Seal(_) => CONTROL,
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
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Slot {
    Empty,
    Id([u8; 16]),
    N(u64),
}
#[cfg(test)]
const SLOTS: usize = 10;
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct OrderKey {
    affinity: [u8; 16],
    family: u16,
    slots: [Slot; SLOTS],
}
#[cfg(test)]
struct Fields {
    slots: [Slot; SLOTS],
    next: usize,
}
#[cfg(test)]
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
            NativeInvocation::Seal(ordinal) => self.n(6).n(*ordinal),
        }
    }
    fn timer(self, target: &TimerTarget) -> Self {
        match target {
            TimerTarget::Claim(claim) => self.n(0).id(claim.0),
            TimerTarget::Evaluation(evaluation) => self.n(1).evaluation(evaluation),
            TimerTarget::Monitor(claim, monitor) => self.n(2).id(claim.0).id(monitor.0),
        }
    }
}

/// The complete order of one key as one value: what [`Key`]'s order is, compared field by field.
#[cfg(test)]
fn order_key(key: &Key) -> OrderKey {
    OrderKey {
        affinity: affinity(key),
        family: family(key),
        slots: slots(key),
    }
}
/// A key's fields in declaration order, nested enums tagged: its order within its affinity and family.
#[cfg(test)]
fn slots(key: &Key) -> [Slot; SLOTS] {
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
        Key::Epochs(principal) => fields.id(principal.0),
        Key::Seal(ordinal) => fields.n(*ordinal),
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
    fields.slots
}
impl focal_memory::RangeKey for Key {
    /// The affinity, read as a big-endian integer: the first field of the
    /// order, so a key with a lesser prefix is a lesser key.
    fn order_prefix(&self) -> u128 {
        u128::from_be_bytes(affinity(self))
    }
}
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Key {
    /// Affinity, then family, then the fields, decided as early as it can be: most keys a search compares
    /// already differ in affinity, so the fields are compared only for keys of one affinity and family,
    /// in place ([`same_family`]), never building either key's slots. Affinities and identities compare
    /// as big-endian integers: the byte order, in one comparison rather than a `memcmp` call. A
    /// 2026-10-08 profile of authored claim creation spent half the owner's CPU ordering keys: every
    /// exact lookup ends comparing a key with itself, and the families the field comparison did not
    /// cover built both keys' ten slots to do it.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        u128::from_be_bytes(affinity(self))
            .cmp(&u128::from_be_bytes(affinity(other)))
            .then_with(|| family(self).cmp(&family(other)))
            .then_with(|| same_family(self, other))
    }
}

/// An ordering decided field by field: the first field that differs decides, as [`slots`] lays them out.
#[derive(Clone, Copy)]
struct Fold(std::cmp::Ordering);
impl Fold {
    fn start() -> Self {
        Self(std::cmp::Ordering::Equal)
    }
    fn id(self, a: &[u8; 16], b: &[u8; 16]) -> Self {
        if self.0.is_ne() {
            return self;
        }
        Self(u128::from_be_bytes(*a).cmp(&u128::from_be_bytes(*b)))
    }
    fn n(self, a: u64, b: u64) -> Self {
        if self.0.is_ne() {
            return self;
        }
        Self(a.cmp(&b))
    }
    fn hash(self, a: &ContentHash, b: &ContentHash) -> Self {
        self.id(&low(a), &low(b)).id(&high(a), &high(b))
    }
    fn evaluation(self, a: &EvaluationKey, b: &EvaluationKey) -> Self {
        use EvaluationTarget as T;
        let fold = self
            .id(&a.claim.0, &b.claim.0)
            .id(&a.validation.0, &b.validation.0);
        let fold = match (a.target, b.target) {
            (T::Admission, T::Admission) => fold.n(0, 0),
            (T::Increment { artifact: x }, T::Increment { artifact: y }) => {
                fold.n(1, 1).id(&x.0, &y.0)
            }
            (
                T::Work {
                    response: r,
                    slot: s,
                    artifact: x,
                },
                T::Work {
                    response: q,
                    slot: t,
                    artifact: y,
                },
            ) => fold
                .n(2, 2)
                .id(&r.0, &q.0)
                .n(u64::from(s), u64::from(t))
                .id(&x.0, &y.0),
            (
                T::MissingSlot {
                    response: r,
                    slot: s,
                },
                T::MissingSlot {
                    response: q,
                    slot: t,
                },
            ) => fold.n(3, 3).id(&r.0, &q.0).n(u64::from(s), u64::from(t)),
            (T::Delivery { response: r }, T::Delivery { response: q }) => {
                fold.n(4, 4).id(&r.0, &q.0)
            }
            (x, y) => fold.n(target_tag(&x), target_tag(&y)),
        };
        fold.n(a.generation, b.generation)
    }
    fn result(self, a: &NativeResultKey, b: &NativeResultKey) -> Self {
        self.evaluation(&a.evaluation, &b.evaluation)
            .n(a.revision.0, b.revision.0)
    }
    fn cycle(self, a: &NativeCycleKey, b: &NativeCycleKey) -> Self {
        self.id(&a.claim.0, &b.claim.0)
            .id(&a.receipt.0, &b.receipt.0)
            .n(a.epoch, b.epoch)
            .n(u64::from(a.cycle), u64::from(b.cycle))
    }
    fn invocation(self, a: &NativeInvocation, b: &NativeInvocation) -> Self {
        use NativeInvocation as I;
        match (a, b) {
            (I::Request(x), I::Request(y)) => self
                .n(0, 0)
                .id(&x.principal.0, &y.principal.0)
                .n(x.epoch.0, y.epoch.0)
                .id(&x.id.0, &y.id.0),
            (I::EvaluationDeadline(x), I::EvaluationDeadline(y)) => self
                .n(1, 1)
                .evaluation(&x.evaluation, &y.evaluation)
                .id(&x.timer.0, &y.timer.0)
                .n(x.generation, y.generation),
            (I::ClaimDeadline(x), I::ClaimDeadline(y)) => self
                .n(2, 2)
                .id(&x.claim.0, &y.claim.0)
                .id(&x.timer.0, &y.timer.0)
                .n(x.generation, y.generation),
            (I::MonitorDeadline(x), I::MonitorDeadline(y)) => self
                .n(3, 3)
                .id(&x.claim.0, &y.claim.0)
                .id(&x.monitor.0, &y.monitor.0)
                .id(&x.timer.0, &y.timer.0)
                .n(x.generation, y.generation),
            (I::Import, I::Import) => self.n(4, 4),
            (I::Retirement(x), I::Retirement(y)) => self.n(5, 5).id(&x.0, &y.0),
            (I::Seal(x), I::Seal(y)) => self.n(6, 6).n(*x, *y),
            (x, y) => self.n(invocation_tag(x), invocation_tag(y)),
        }
    }
    fn timer(self, a: &TimerTarget, b: &TimerTarget) -> Self {
        use TimerTarget as T;
        match (a, b) {
            (T::Claim(x), T::Claim(y)) => self.n(0, 0).id(&x.0, &y.0),
            (T::Evaluation(x), T::Evaluation(y)) => self.n(1, 1).evaluation(x, y),
            (T::Monitor(c, m), T::Monitor(d, o)) => self.n(2, 2).id(&c.0, &d.0).id(&m.0, &o.0),
            (x, y) => self.n(timer_tag(x), timer_tag(y)),
        }
    }
}
fn target_tag(target: &EvaluationTarget) -> u64 {
    match target {
        EvaluationTarget::Admission => 0,
        EvaluationTarget::Increment { .. } => 1,
        EvaluationTarget::Work { .. } => 2,
        EvaluationTarget::MissingSlot { .. } => 3,
        EvaluationTarget::Delivery { .. } => 4,
    }
}
fn invocation_tag(invocation: &NativeInvocation) -> u64 {
    match invocation {
        NativeInvocation::Request(_) => 0,
        NativeInvocation::EvaluationDeadline(_) => 1,
        NativeInvocation::ClaimDeadline(_) => 2,
        NativeInvocation::MonitorDeadline(_) => 3,
        NativeInvocation::Import => 4,
        NativeInvocation::Retirement(_) => 5,
        NativeInvocation::Seal(_) => 6,
    }
}
fn timer_tag(target: &TimerTarget) -> u64 {
    match target {
        TimerTarget::Claim(_) => 0,
        TimerTarget::Evaluation(_) => 1,
        TimerTarget::Monitor(..) => 2,
    }
}

/// Two keys of one affinity and family compared field by field, the values [`slots`] lays out in
/// the same order. A family is one variant, so its keys have one shape but for their nested enums,
/// which compare by tag first, as their slots do. Keys of different families are never compared
/// here (the family decides them); were they, they would compare equal, which the family's
/// comparison before this one rules out.
fn same_family(a: &Key, b: &Key) -> std::cmp::Ordering {
    use Key as K;
    let f = Fold::start();
    match (a, b) {
        (K::IncomingHead(x), K::IncomingHead(y))
        | (K::MonitorHead(x), K::MonitorHead(y))
        | (K::Claim(x), K::Claim(y))
        | (K::RetiredCycleHead(x), K::RetiredCycleHead(y))
        | (K::Retired(x), K::Retired(y))
        | (K::ClaimResultTestament(x), K::ClaimResultTestament(y))
        | (K::ClaimContent(x), K::ClaimContent(y)) => f.id(&x.0, &y.0),
        (K::IncomingLink(c, x), K::IncomingLink(d, y)) => f.id(&c.0, &d.0).id(&x.0, &y.0),
        (K::Monitor(x), K::Monitor(y)) => f.id(&x.0, &y.0),
        (K::MonitorLink(c, x), K::MonitorLink(d, y)) => f.id(&c.0, &d.0).id(&x.0, &y.0),
        (K::MissingResult(x), K::MissingResult(y))
        | (K::Accepted(x), K::Accepted(y))
        | (K::DeliveryResult(x), K::DeliveryResult(y)) => f.result(x, y),
        (K::Meta, K::Meta) | (K::End, K::End) => f,
        (K::Definition(x), K::Definition(y)) | (K::LegacyDefinition(x), K::LegacyDefinition(y)) => {
            f.id(&x.0, &y.0)
        }
        (K::Evaluation(x), K::Evaluation(y)) => f.evaluation(x, y),
        (K::Artifact(x), K::Artifact(y))
        | (K::Work(x), K::Work(y))
        | (K::Diagnostic(x), K::Diagnostic(y)) => f.id(&x.0, &y.0),
        (K::ArtifactIdentity(x), K::ArtifactIdentity(y)) => f.hash(x, y),
        (K::Receipt(x), K::Receipt(y)) => f.id(&x.0, &y.0),
        (K::Cycle(x), K::Cycle(y)) | (K::RetiredCycle(x), K::RetiredCycle(y)) => f.cycle(x, y),
        (K::WorkSlot(x, s), K::WorkSlot(y, t)) => f.cycle(x, y).n(u64::from(*s), u64::from(*t)),
        (K::Response(x), K::Response(y))
        | (K::ResultTestament(x), K::ResultTestament(y))
        | (K::LegacyTestament(x), K::LegacyTestament(y)) => f.id(&x.0, &y.0),
        (K::Outcome(x), K::Outcome(y)) | (K::CreationResult(x), K::CreationResult(y)) => {
            f.invocation(x, y)
        }
        (K::Epochs(x), K::Epochs(y)) => f.id(&x.0, &y.0),
        (K::Seal(x), K::Seal(y)) => f.n(*x, *y),
        (K::Event(s, o), K::Event(t, p)) => f.n(s.0, t.0).n(u64::from(*o), u64::from(*p)),
        (K::ClaimIdentity(k, x), K::ClaimIdentity(l, y))
        | (K::DefinitionIdentity(k, x), K::DefinitionIdentity(l, y)) => {
            f.n(u64::from(*k), u64::from(*l)).hash(x, y)
        }
        (K::LegacyEvidenceSet(x), K::LegacyEvidenceSet(y)) => f.id(&x.0, &y.0),
        (K::LegacyRun(v, o), K::LegacyRun(w, p)) => {
            f.id(&v.0, &w.0).n(u64::from(*o), u64::from(*p))
        }
        (K::ByIssuer(p, c), K::ByIssuer(q, d)) | (K::BySubject(p, c), K::BySubject(q, d)) => {
            f.id(&p.0, &q.0).id(&c.0, &d.0)
        }
        (K::ByStatus(k, c), K::ByStatus(l, d)) | (K::ByAction(k, c), K::ByAction(l, d)) => {
            f.n(u64::from(*k), u64::from(*l)).id(&c.0, &d.0)
        }
        (K::ByScope(k, x, c), K::ByScope(l, y, d)) => {
            f.n(u64::from(*k), u64::from(*l)).hash(x, y).id(&c.0, &d.0)
        }
        (K::ByRelation(k, t, s), K::ByRelation(l, u, r)) => f
            .n(u64::from(*k), u64::from(*l))
            .id(&t.0, &u.0)
            .id(&s.0, &r.0),
        (K::ByProducer(p, a), K::ByProducer(q, b)) => f.id(&p.0, &q.0).id(&a.0, &b.0),
        (K::ByArtifactKind(x, a), K::ByArtifactKind(y, b))
        | (K::BySchema(x, a), K::BySchema(y, b)) => f.hash(x, y).id(&a.0, &b.0),
        (K::ArtifactInput(o, a), K::ArtifactInput(p, b)) => f.id(&o.0, &p.0).id(&a.0, &b.0),
        (K::ByEvaluator(p, v), K::ByEvaluator(q, w)) => f.id(&p.0, &q.0).id(&v.0, &w.0),
        (K::ByVerdict(k, x), K::ByVerdict(l, y)) => f.n(u64::from(*k), u64::from(*l)).result(x, y),
        (K::ByCreated(k, s, o), K::ByCreated(l, t, p)) => {
            f.n(u64::from(*k), u64::from(*l)).n(s.0, t.0).id(&o.0, &p.0)
        }
        (K::DueTimer(x, s), K::DueTimer(y, t)) => f.n(*x, *y).timer(s, t),
        (K::ByObject(k, o), K::ByObject(l, p)) => f.n(u64::from(*k), u64::from(*l)).id(&o.0, &p.0),
        _ => f,
    }
    .0
}

#[cfg(test)]
#[path = "layout_order_tests.rs"]
mod tests;
