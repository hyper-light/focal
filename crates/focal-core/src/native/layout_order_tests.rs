use super::{affinity, family, order_key};
use crate::native::*;
use std::cmp::Ordering;

fn claim(n: u8) -> ClaimId {
    ClaimId([n; 16])
}
fn evaluation(claim_id: ClaimId, n: u8) -> EvaluationKey {
    EvaluationKey {
        claim: claim_id,
        validation: ValidationId([n; 16]),
        target: EvaluationTarget::Work {
            response: TestamentId([n; 16]),
            slot: u32::from(n),
            artifact: ArtifactId([n; 16]),
        },
        generation: u64::from(n),
    }
}
/// A corpus with every family and several keys per object, plus keys that
/// differ from a neighbour in exactly one field.
fn corpus() -> Vec<Key> {
    let mut keys = Vec::new();
    for n in 1..=3u8 {
        let c = claim(n);
        let hash = ContentHash([n; 32]);
        let cycle = NativeCycleKey {
            claim: c,
            receipt: ReceiptId([n; 16]),
            epoch: u64::from(n),
            cycle: u32::from(n),
        };
        let eval = evaluation(c, n);
        let result = NativeResultKey {
            evaluation: eval,
            revision: focal_model::ObjectRevision(u64::from(n)),
        };
        let request = RequestKey {
            principal: ParticipantId([n; 16]),
            epoch: RequestEpoch(u64::from(n)),
            id: RequestId([n; 16]),
        };
        keys.extend([
            Key::IncomingHead(c),
            Key::IncomingLink(c, claim(n.wrapping_add(7))),
            Key::Monitor(focal_model::MonitorId([n; 16])),
            Key::MonitorHead(c),
            Key::MonitorLink(c, focal_model::MonitorId([n; 16])),
            Key::MissingResult(result),
            Key::Claim(c),
            Key::Definition(ValidationId([n; 16])),
            Key::Evaluation(eval),
            Key::Evaluation(EvaluationKey {
                target: EvaluationTarget::Admission,
                ..eval
            }),
            Key::Evaluation(EvaluationKey {
                generation: 99,
                ..eval
            }),
            Key::Artifact(ArtifactId([n; 16])),
            Key::ArtifactIdentity(hash),
            Key::Accepted(result),
            Key::DeliveryResult(result),
            Key::Receipt(ReceiptId([n; 16])),
            Key::Cycle(cycle),
            Key::Cycle(NativeCycleKey { cycle: 9, ..cycle }),
            Key::RetiredCycleHead(c),
            Key::RetiredCycle(cycle),
            Key::Work(ArtifactId([n; 16])),
            Key::WorkSlot(cycle, u32::from(n)),
            Key::WorkSlot(cycle, 77),
            Key::Diagnostic(ArtifactId([n; 16])),
            Key::Response(TestamentId([n; 16])),
            Key::ResultTestament(TestamentId([n; 16])),
            Key::ClaimResultTestament(c),
            Key::Outcome(NativeInvocation::Request(request)),
            Key::Outcome(NativeInvocation::Request(RequestKey {
                epoch: RequestEpoch(50),
                ..request
            })),
            Key::Outcome(NativeInvocation::EvaluationDeadline(NativeDeadlineKey {
                evaluation: eval,
                timer: TimerId([n; 16]),
                generation: u64::from(n),
            })),
            Key::Outcome(NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey {
                claim: c,
                timer: TimerId([n; 16]),
                generation: u64::from(n),
            })),
            Key::Outcome(NativeInvocation::MonitorDeadline(
                NativeMonitorDeadlineKey {
                    claim: c,
                    monitor: focal_model::MonitorId([n; 16]),
                    timer: TimerId([n; 16]),
                    generation: u64::from(n),
                },
            )),
            Key::Event(SessionSeq(u64::from(n)), 0),
            Key::Event(SessionSeq(u64::from(n)), 1),
            Key::ClaimContent(c),
            Key::ClaimIdentity(1, hash),
            Key::ClaimIdentity(2, hash),
            Key::DefinitionIdentity(1, hash),
            Key::CreationResult(NativeInvocation::Request(request)),
            Key::LegacyTestament(TestamentId([n; 16])),
            Key::LegacyEvidenceSet(focal_model::EvidenceSetId([n; 16])),
            Key::LegacyRun(ValidationId([n; 16]), u32::from(n)),
            Key::LegacyDefinition(ValidationId([n; 16])),
            Key::ByIssuer(ParticipantId([n; 16]), c),
            Key::BySubject(ParticipantId([n; 16]), c),
            Key::ByStatus(2, c),
            Key::ByStatus(3, c),
            Key::ByAction(1, c),
            Key::ByScope(1, hash, c),
            Key::ByRelation(1, c, claim(n.wrapping_add(9))),
            Key::ByRelation(2, c, claim(n.wrapping_add(9))),
            Key::ByProducer(ParticipantId([n; 16]), ArtifactId([n; 16])),
            Key::ByArtifactKind(hash, ArtifactId([n; 16])),
            Key::BySchema(hash, ArtifactId([n; 16])),
            Key::ArtifactInput(focal_model::ObjectId([n; 16]), ArtifactId([n; 16])),
            Key::ByEvaluator(ParticipantId([n; 16]), ValidationId([n; 16])),
            Key::ByVerdict(1, result),
            Key::ByCreated(1, SessionSeq(u64::from(n)), focal_model::ObjectId([n; 16])),
            Key::DueTimer(u64::from(n), TimerTarget::Claim(c)),
            Key::DueTimer(u64::from(n), TimerTarget::Evaluation(eval)),
            Key::DueTimer(
                u64::from(n),
                TimerTarget::Monitor(c, focal_model::MonitorId([n; 16])),
            ),
        ]);
    }
    keys.push(Key::Meta);
    keys.push(Key::Outcome(NativeInvocation::Import));
    keys.push(Key::End);
    keys
}

#[test]
fn the_order_is_total_agrees_with_equality_and_ends_at_the_sentinel() {
    let keys = corpus();
    for (i, a) in keys.iter().enumerate() {
        for (j, b) in keys.iter().enumerate() {
            let order = a.cmp(b);
            assert_eq!(order == Ordering::Equal, a == b, "{a:?} vs {b:?}");
            assert_eq!(b.cmp(a), order.reverse(), "{a:?} vs {b:?}");
            if a == b {
                assert_eq!(i, j, "duplicate corpus key {a:?}");
            }
        }
    }
    let mut sorted = keys.clone();
    sorted.sort();
    assert!(sorted.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(sorted.last(), Some(&Key::End));
    // Transitivity on every triple of the corpus.
    for a in &keys {
        for b in &keys {
            for c in &keys {
                if a < b && b < c {
                    assert!(a < c, "{a:?} < {b:?} < {c:?}");
                }
            }
        }
    }
}

/// The comparison that stops at the first deciding field is the full order key's, on every pair of the corpus.
#[test]
fn the_early_comparison_is_the_order_keys() {
    let keys = corpus();
    for a in &keys {
        for b in &keys {
            assert_eq!(a.cmp(b), order_key(a).cmp(&order_key(b)), "{a:?} vs {b:?}");
        }
    }
}

#[test]
fn one_objects_rows_are_contiguous_and_family_scans_stay_ordered() {
    let mut sorted = corpus();
    sorted.sort();
    // Every key owned by claim 2 sits in one block.
    let positions: Vec<usize> = sorted
        .iter()
        .enumerate()
        .filter(|(_, key)| affinity(key) == [2; 16] && family(key) != u16::MAX)
        .map(|(index, _)| index)
        .collect();
    assert!(positions.len() > 10);
    assert_eq!(
        positions.last().copied(),
        positions.first().map(|first| first + positions.len() - 1),
        "claim 2's rows are interleaved with other objects"
    );
    // Within the claim, its cycles are contiguous and ordered by their fields.
    let cycles: Vec<&Key> = sorted
        .iter()
        .filter(|key| matches!(key, Key::Cycle(cycle) if cycle.claim == claim(2)))
        .collect();
    assert_eq!(cycles.len(), 2);
    assert!(matches!(
        (cycles[0], cycles[1]),
        (Key::Cycle(first), Key::Cycle(second)) if first.cycle < second.cycle
    ));
    // Evaluations of one claim are contiguous and start at the admission target.
    let evaluations: Vec<&Key> = sorted
        .iter()
        .filter(|key| matches!(key, Key::Evaluation(e) if e.claim == claim(2)))
        .collect();
    assert_eq!(evaluations.len(), 3);
    assert!(matches!(
        evaluations[0],
        Key::Evaluation(e) if e.target == EvaluationTarget::Admission
    ));
    // A status bucket holds every claim of that status together, and every
    // due timer sits in time order under the control affinity.
    let status_two: Vec<usize> = sorted
        .iter()
        .enumerate()
        .filter(|(_, key)| matches!(key, Key::ByStatus(2, _)))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(status_two.len(), 3);
    assert_eq!(status_two[2] - status_two[0], 2);
    let timers: Vec<u64> = sorted
        .iter()
        .filter_map(|key| match key {
            Key::DueTimer(time, _) => Some(*time),
            _ => None,
        })
        .collect();
    assert!(timers.windows(2).all(|pair| pair[0] <= pair[1]));
    let meta = sorted.iter().position(|key| *key == Key::Meta).unwrap();
    let first_timer = sorted
        .iter()
        .position(|key| matches!(key, Key::DueTimer(..)))
        .unwrap();
    assert!(meta < first_timer, "control rows order by family");
    assert_eq!(affinity(&Key::Meta), [0; 16]);
}

/// Every evaluation key of a corpus that varies every field: claims and
/// validations whose ids are not monotonic in their number, every target
/// variant with ids that disagree with the variant order, and generations
/// spanning the range.
fn evaluation_corpus() -> Vec<EvaluationKey> {
    // Ids chosen so numeric order, byte order and insertion order disagree.
    let ids: [[u8; 16]; 3] = [[9; 16], [2; 16], [0x7f; 16]];
    let mut keys = Vec::new();
    for claim_id in ids {
        for validation_id in ids {
            for response in ids {
                for artifact in ids {
                    for slot in [3u32, 0, 1] {
                        let targets = [
                            EvaluationTarget::Admission,
                            EvaluationTarget::Increment {
                                artifact: ArtifactId(artifact),
                            },
                            EvaluationTarget::Work {
                                response: TestamentId(response),
                                slot,
                                artifact: ArtifactId(artifact),
                            },
                            EvaluationTarget::MissingSlot {
                                response: TestamentId(response),
                                slot,
                            },
                            EvaluationTarget::Delivery {
                                response: TestamentId(response),
                            },
                        ];
                        for target in targets {
                            for generation in [0u64, 1, 2, u64::MAX] {
                                keys.push(EvaluationKey {
                                    claim: ClaimId(claim_id),
                                    validation: ValidationId(validation_id),
                                    target,
                                    generation,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    keys.sort();
    keys.dedup();
    keys
}

/// The storage order of an evaluation row is the derived order of its core
/// key, which is the derived order of its wire key: a cursor compared in
/// either vocabulary names the same position in the scan (F07), and the
/// conversion between the two loses nothing.
#[test]
fn evaluation_rows_are_stored_in_key_order_and_the_wire_key_orders_the_same() {
    use super::event_record::{evaluation_key, evaluation_key_of};
    let keys = evaluation_corpus();
    assert!(keys.len() > 1_000, "{}", keys.len());
    for a in &keys {
        assert_eq!(evaluation_key_of(evaluation_key(*a)), *a, "{a:?}");
        for b in &keys {
            let core = a.cmp(b);
            assert_eq!(
                Key::Evaluation(*a).cmp(&Key::Evaluation(*b)),
                core,
                "storage order disagrees with the key order: {a:?} vs {b:?}"
            );
            assert_eq!(
                evaluation_key(*a).cmp(&evaluation_key(*b)),
                core,
                "wire order disagrees with the key order: {a:?} vs {b:?}"
            );
        }
    }
    // One declaration's keys are one contiguous span of its claim's span, so a
    // scan from the first key of a (claim, validation) that stops at the first
    // key of another sees exactly that declaration's rows.
    let mut stored: Vec<Key> = keys.iter().map(|key| Key::Evaluation(*key)).collect();
    stored.sort();
    for pair in stored.windows(2) {
        let (Key::Evaluation(a), Key::Evaluation(b)) = (pair[0], pair[1]) else {
            panic!("evaluation keys");
        };
        assert!((a.claim, a.validation) <= (b.claim, b.validation));
    }
    let claim = ClaimId([2; 16]);
    let validation = ValidationId([9; 16]);
    let span: Vec<usize> = stored
        .iter()
        .enumerate()
        .filter(|(_, key)| {
            matches!(key, Key::Evaluation(key) if key.claim == claim && key.validation == validation)
        })
        .map(|(index, _)| index)
        .collect();
    assert!(!span.is_empty());
    assert_eq!(
        span.last().copied(),
        span.first().map(|first| first + span.len() - 1),
        "a declaration's evaluations are interleaved with another's"
    );
}

/// The keys `same_family` compares field by field, several of each family differing in each field
/// in turn, each pair ordered as their slots order them: the fast comparison is the order.
#[test]
fn the_field_comparison_of_one_family_is_the_slot_order() {
    let ids = |n: u8| [n; 16];
    let request = |p: u8, e: u64, i: u8| RequestKey {
        principal: ParticipantId(ids(p)),
        epoch: RequestEpoch(e),
        id: RequestId(ids(i)),
    };
    let mut keys = Vec::new();
    for a in [1u8, 2, 200] {
        for b in [1u8, 3, 255] {
            let (x, y) = (claim(a), claim(b));
            keys.extend([
                Key::Claim(x),
                Key::IncomingHead(x),
                Key::ClaimContent(x),
                Key::IncomingLink(x, y),
                Key::MonitorLink(x, focal_model::MonitorId(ids(b))),
                Key::ByIssuer(ParticipantId(ids(a)), y),
                Key::BySubject(ParticipantId(ids(a)), y),
                Key::ByStatus(u16::from(a), y),
                Key::ByAction(u16::from(a), y),
                Key::ByCreated(
                    u16::from(a),
                    SessionSeq(u64::from(b)),
                    focal_model::ObjectId(ids(b)),
                ),
                Key::ByCreated(
                    u16::from(a),
                    SessionSeq(u64::from(b)),
                    focal_model::ObjectId(ids(a)),
                ),
                Key::ByObject(u16::from(a), focal_model::ObjectId(ids(b))),
                Key::Event(SessionSeq(u64::from(a)), u32::from(b)),
                Key::Outcome(NativeInvocation::Request(request(a, u64::from(b), b))),
                Key::Outcome(NativeInvocation::Request(request(a, u64::from(b), a))),
                Key::Outcome(NativeInvocation::Request(request(b, u64::from(a), a))),
                Key::CreationResult(NativeInvocation::Request(request(a, u64::from(b), b))),
                Key::CreationResult(NativeInvocation::Request(request(b, 7, a))),
            ]);
        }
    }
    for a in &keys {
        for b in &keys {
            assert_eq!(a.cmp(b), order_key(a).cmp(&order_key(b)), "{a:?} vs {b:?}");
        }
    }
}
