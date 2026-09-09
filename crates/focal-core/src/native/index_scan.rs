//! Bounded scans over the secondary index families and the primary key
//! spaces of the committed native prefix. A scan is an ordered iterator that
//! starts after an optional resume position and stops at the end of its key
//! prefix; the caller bounds how many entries it consumes, so every list is
//! `O(visited)` and never materializes the family.
use super::*;
use focal_model::{ActionType, ObjectId, ObjectKind, RelationKind, ScopeKind, VerdictValue};

/// One indexed predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIndexScan {
    Issuer(ParticipantId),
    Subject(ParticipantId),
    Status(ClaimStatus),
    Action(ActionType),
    Scope {
        kind: ScopeKind,
        key: ContentHash,
    },
    Relation {
        kind: RelationKind,
        target: ClaimId,
    },
    Producer(ParticipantId),
    ArtifactKind(ContentHash),
    Schema(ContentHash),
    ArtifactInput(ObjectId),
    Evaluator(ParticipantId),
    Verdict(VerdictValue),
    /// Objects of one family created after `after` (exclusive).
    Created {
        family: ObjectKind,
        after: SessionSeq,
    },
    /// Trusted timers due at or before `through` that have not been consumed.
    Due {
        through: u64,
    },
}

/// One indexed hit; also the resume position of the scan that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIndexHit {
    Claim(ClaimId),
    Artifact(ArtifactId),
    Definition(ValidationId),
    Result(NativeResultKey),
    Created {
        family: ObjectKind,
        sequence: SessionSeq,
        id: ObjectId,
    },
    Timer {
        at: u64,
        target: TimerTarget,
    },
}

const MIN_ID: [u8; 16] = [0; 16];
const MAX_ID: [u8; 16] = [0xff; 16];

/// The exact key range `[first, last]` of one scan, and the key a resume hit
/// maps to. A resume hit of the wrong family is ignored (scan restarts).
fn range(scan: NativeIndexScan) -> (Key, Key) {
    match scan {
        NativeIndexScan::Issuer(p) => (
            Key::ByIssuer(p, ClaimId(MIN_ID)),
            Key::ByIssuer(p, ClaimId(MAX_ID)),
        ),
        NativeIndexScan::Subject(p) => (
            Key::BySubject(p, ClaimId(MIN_ID)),
            Key::BySubject(p, ClaimId(MAX_ID)),
        ),
        NativeIndexScan::Status(status) => (
            Key::ByStatus(status.code(), ClaimId(MIN_ID)),
            Key::ByStatus(status.code(), ClaimId(MAX_ID)),
        ),
        NativeIndexScan::Action(action) => (
            Key::ByAction(action.code(), ClaimId(MIN_ID)),
            Key::ByAction(action.code(), ClaimId(MAX_ID)),
        ),
        NativeIndexScan::Scope { kind, key } => (
            Key::ByScope(kind.code(), key, ClaimId(MIN_ID)),
            Key::ByScope(kind.code(), key, ClaimId(MAX_ID)),
        ),
        NativeIndexScan::Relation { kind, target } => (
            Key::ByRelation(kind.code(), target, ClaimId(MIN_ID)),
            Key::ByRelation(kind.code(), target, ClaimId(MAX_ID)),
        ),
        NativeIndexScan::Producer(p) => (
            Key::ByProducer(p, ArtifactId(MIN_ID)),
            Key::ByProducer(p, ArtifactId(MAX_ID)),
        ),
        NativeIndexScan::ArtifactKind(hash) => (
            Key::ByArtifactKind(hash, ArtifactId(MIN_ID)),
            Key::ByArtifactKind(hash, ArtifactId(MAX_ID)),
        ),
        NativeIndexScan::Schema(hash) => (
            Key::BySchema(hash, ArtifactId(MIN_ID)),
            Key::BySchema(hash, ArtifactId(MAX_ID)),
        ),
        NativeIndexScan::ArtifactInput(input) => (
            Key::ArtifactInput(input, ArtifactId(MIN_ID)),
            Key::ArtifactInput(input, ArtifactId(MAX_ID)),
        ),
        NativeIndexScan::Evaluator(p) => (
            Key::ByEvaluator(p, ValidationId(MIN_ID)),
            Key::ByEvaluator(p, ValidationId(MAX_ID)),
        ),
        NativeIndexScan::Verdict(verdict) => (
            Key::ByVerdict(verdict.code(), min_result()),
            Key::ByVerdict(verdict.code(), max_result()),
        ),
        NativeIndexScan::Created { family, after } => (
            Key::ByCreated(
                family.code(),
                SessionSeq(after.0.saturating_add(1)),
                ObjectId(MIN_ID),
            ),
            Key::ByCreated(family.code(), SessionSeq(u64::MAX), ObjectId(MAX_ID)),
        ),
        NativeIndexScan::Due { through } => (
            Key::DueTimer(0, TimerTarget::Claim(ClaimId(MIN_ID))),
            Key::DueTimer(
                through,
                TimerTarget::Monitor(ClaimId(MAX_ID), MonitorId(MAX_ID)),
            ),
        ),
    }
}
fn min_result() -> NativeResultKey {
    NativeResultKey {
        evaluation: EvaluationKey {
            claim: ClaimId(MIN_ID),
            validation: ValidationId(MIN_ID),
            target: EvaluationTarget::Admission,
            generation: 0,
        },
        revision: focal_model::ObjectRevision(0),
    }
}
fn max_result() -> NativeResultKey {
    NativeResultKey {
        evaluation: EvaluationKey {
            claim: ClaimId(MAX_ID),
            validation: ValidationId(MAX_ID),
            target: EvaluationTarget::Delivery {
                response: TestamentId(MAX_ID),
            },
            generation: u64::MAX,
        },
        revision: focal_model::ObjectRevision(u64::MAX),
    }
}
fn resume(scan: NativeIndexScan, hit: NativeIndexHit) -> Option<Key> {
    Some(match (scan, hit) {
        (NativeIndexScan::Issuer(p), NativeIndexHit::Claim(id)) => Key::ByIssuer(p, id),
        (NativeIndexScan::Subject(p), NativeIndexHit::Claim(id)) => Key::BySubject(p, id),
        (NativeIndexScan::Status(status), NativeIndexHit::Claim(id)) => {
            Key::ByStatus(status.code(), id)
        }
        (NativeIndexScan::Action(action), NativeIndexHit::Claim(id)) => {
            Key::ByAction(action.code(), id)
        }
        (NativeIndexScan::Scope { kind, key }, NativeIndexHit::Claim(id)) => {
            Key::ByScope(kind.code(), key, id)
        }
        (NativeIndexScan::Relation { kind, target }, NativeIndexHit::Claim(id)) => {
            Key::ByRelation(kind.code(), target, id)
        }
        (NativeIndexScan::Producer(p), NativeIndexHit::Artifact(id)) => Key::ByProducer(p, id),
        (NativeIndexScan::ArtifactKind(hash), NativeIndexHit::Artifact(id)) => {
            Key::ByArtifactKind(hash, id)
        }
        (NativeIndexScan::Schema(hash), NativeIndexHit::Artifact(id)) => Key::BySchema(hash, id),
        (NativeIndexScan::ArtifactInput(input), NativeIndexHit::Artifact(id)) => {
            Key::ArtifactInput(input, id)
        }
        (NativeIndexScan::Evaluator(p), NativeIndexHit::Definition(id)) => Key::ByEvaluator(p, id),
        (NativeIndexScan::Verdict(verdict), NativeIndexHit::Result(key)) => {
            Key::ByVerdict(verdict.code(), key)
        }
        (
            NativeIndexScan::Created { family, .. },
            NativeIndexHit::Created {
                family: hit_family,
                sequence,
                id,
            },
        ) if family == hit_family => Key::ByCreated(family.code(), sequence, id),
        (NativeIndexScan::Due { .. }, NativeIndexHit::Timer { at, target }) => {
            Key::DueTimer(at, target)
        }
        _ => return None,
    })
}
fn hit(key: Key) -> Option<NativeIndexHit> {
    Some(match key {
        Key::ByIssuer(_, id)
        | Key::BySubject(_, id)
        | Key::ByStatus(_, id)
        | Key::ByAction(_, id)
        | Key::ByScope(_, _, id)
        | Key::ByRelation(_, _, id) => NativeIndexHit::Claim(id),
        Key::ByProducer(_, id)
        | Key::ByArtifactKind(_, id)
        | Key::BySchema(_, id)
        | Key::ArtifactInput(_, id) => NativeIndexHit::Artifact(id),
        Key::ByEvaluator(_, id) => NativeIndexHit::Definition(id),
        Key::ByVerdict(_, key) => NativeIndexHit::Result(key),
        Key::ByCreated(family, sequence, id) => NativeIndexHit::Created {
            family: ObjectKind::from_code(family)?,
            sequence,
            id,
        },
        Key::DueTimer(at, target) => NativeIndexHit::Timer { at, target },
        _ => return None,
    })
}

impl Core<NativeState> {
    /// Hits of one indexed predicate in key order, starting after `after`.
    /// The caller bounds consumption; nothing is allocated per hit.
    pub fn native_index_scan(
        &self,
        scan: NativeIndexScan,
        after: Option<NativeIndexHit>,
    ) -> impl Iterator<Item = NativeIndexHit> + '_ {
        let (first, last) = range(scan);
        let (start, exclusive) = match after.and_then(|hit| resume(scan, hit)) {
            Some(key) if key >= first && key <= last => (key, true),
            _ => (first, false),
        };
        self.state
            .rows
            .entries_from(&start, exclusive)
            .take_while(move |entry| entry.key <= last)
            .filter(|entry| matches!(entry.value, Row::Index))
            .filter_map(|entry| hit(entry.key))
    }
    /// Claim identities in key order after `after`.
    pub fn native_claims_from(&self, after: Option<ClaimId>) -> impl Iterator<Item = ClaimId> + '_ {
        let start = Key::Claim(after.unwrap_or(ClaimId(MIN_ID)));
        self.state
            .rows
            .entries_from(&start, after.is_some())
            .take_while(|entry| matches!(entry.key, Key::Claim(_)))
            .filter_map(|entry| match entry.key {
                Key::Claim(id) => Some(id),
                _ => None,
            })
    }
    /// Artifact identities in key order after `after`.
    pub fn native_artifacts_from(
        &self,
        after: Option<ArtifactId>,
    ) -> impl Iterator<Item = ArtifactId> + '_ {
        let start = Key::Artifact(after.unwrap_or(ArtifactId(MIN_ID)));
        self.state
            .rows
            .entries_from(&start, after.is_some())
            .take_while(|entry| matches!(entry.key, Key::Artifact(_)))
            .filter_map(|entry| match entry.key {
                Key::Artifact(id) => Some(id),
                _ => None,
            })
    }
    /// Declaration identities in key order after `after`.
    pub fn native_definitions_from(
        &self,
        after: Option<ValidationId>,
    ) -> impl Iterator<Item = ValidationId> + '_ {
        let start = Key::Definition(after.unwrap_or(ValidationId(MIN_ID)));
        self.state
            .rows
            .entries_from(&start, after.is_some())
            .take_while(|entry| matches!(entry.key, Key::Definition(_)))
            .filter_map(|entry| match entry.key {
                Key::Definition(id) => Some(id),
                _ => None,
            })
    }
    /// Receipts in identity order after `after`.
    pub fn native_receipts_from(
        &self,
        after: Option<ReceiptId>,
    ) -> impl Iterator<Item = (ReceiptId, NativeReceipt)> + '_ {
        let start = Key::Receipt(after.unwrap_or(ReceiptId(MIN_ID)));
        self.state
            .rows
            .entries_from(&start, after.is_some())
            .take_while(|entry| matches!(entry.key, Key::Receipt(_)))
            .filter_map(|entry| match (entry.key, &entry.value) {
                (Key::Receipt(id), Row::Receipt(receipt)) => Some((id, *receipt)),
                _ => None,
            })
    }
    /// Evaluation keys in key order (claim first) after `after`, optionally
    /// restricted to one claim.
    pub fn native_evaluations_from(
        &self,
        claim: Option<ClaimId>,
        after: Option<EvaluationKey>,
    ) -> impl Iterator<Item = EvaluationKey> + '_ {
        let start = Key::Evaluation(after.unwrap_or(EvaluationKey {
            claim: claim.unwrap_or(ClaimId(MIN_ID)),
            validation: ValidationId(MIN_ID),
            target: EvaluationTarget::Admission,
            generation: 0,
        }));
        self.state
            .rows
            .entries_from(&start, after.is_some())
            .take_while(move |entry| match entry.key {
                Key::Evaluation(key) => claim.is_none_or(|claim| key.claim == claim),
                _ => false,
            })
            .filter_map(|entry| match entry.key {
                Key::Evaluation(key) => Some(key),
                _ => None,
            })
    }
}
