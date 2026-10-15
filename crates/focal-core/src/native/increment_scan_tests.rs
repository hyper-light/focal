//! The key-order scan of one declaration's evaluations (F07): registration
//! order is submission order, storage order is key order, and a page cursor
//! is a key, so the scan the evaluation page walks must be the sorted keys of
//! exactly that declaration, resumable exclusively after any key.
use super::*;

/// Three cycles of two increments each, submitted with artifact ids that
/// decrease over time so registration order is the reverse of key order and
/// generations are not monotonic along the keys. Both declarations register
/// on every artifact, so their rows interleave in registration order.
fn cycles(f: &mut Fixture) -> Vec<ArtifactId> {
    let mut artifacts = Vec::new();
    for cycle in 0..3u128 {
        let first = f.work(906 - cycle * 2, 0);
        let second = f.work(905 - cycle * 2, 1);
        artifacts.push(first.artifact.id);
        artifacts.push(second.artifact.id);
        let response = 950 + cycle;
        f.commit(
            SUBJECT,
            f.close(response, OutcomeKind::Complete, vec![first, second], vec![]),
        );
        f.commit(
            SUBJECT,
            NativeCommand::PostResponse {
                claim: f.claim(),
                expected: f.response(response),
            },
        );
        f.commit(
            ISSUER,
            NativeCommand::ReceiveResponse {
                claim: f.claim(),
                expected: f.response(response),
            },
        );
    }
    artifacts
}

fn declaration_keys(f: &Fixture, index: u32) -> Vec<EvaluationKey> {
    let validation = ValidationId::from_u128(200 + u128::from(index));
    let view = f.owner.committed();
    view.registrations(ClaimId::from_u128(1))
        .unwrap()
        .rows()
        .iter()
        .filter(|row| ValidationId(row.binding().object.0) == validation)
        .map(|row| EvaluationKey {
            claim: ClaimId::from_u128(1),
            validation,
            target: EvaluationTarget::of(row.target()),
            generation: row.generation(),
        })
        .collect()
}

fn scan(f: &Fixture, index: u32, after: Option<EvaluationKey>) -> Vec<EvaluationKey> {
    f.owner
        .committed_core()
        .native_declaration_evaluations_from(
            ClaimId::from_u128(1),
            ValidationId::from_u128(200 + u128::from(index)),
            after,
        )
        .collect()
}

#[test]
fn a_declarations_evaluations_scan_in_key_order_and_resume_exclusively_after_any_key() {
    let mut f = fixture(&[
        (ValidationMode::Required, Program::Programmatic),
        (ValidationMode::Observe, Program::Direct),
    ]);
    let submitted = cycles(&mut f);
    assert_eq!(submitted.len(), 6);
    assert!(
        submitted.windows(2).all(|pair| pair[0] > pair[1]),
        "ids must decrease over time"
    );
    for index in [1, 2] {
        let registered = declaration_keys(&f, index);
        assert_eq!(registered.len(), 6, "declaration {index}");
        // Registration order is submission order, which is not key order.
        let mut sorted = registered.clone();
        sorted.sort();
        assert_ne!(sorted, registered);
        assert!(sorted.windows(2).all(|pair| pair[0] < pair[1]));
        for key in &sorted {
            assert!(f.owner.committed().evaluation(*key).is_some());
        }
        // The scan is exactly the sorted keys: none of the other declaration's
        // rows, which interleave with these in registration order.
        assert_eq!(scan(&f, index, None), sorted, "declaration {index}");
        // Resuming after every key yields exactly the keys that follow it.
        for (position, key) in sorted.iter().enumerate() {
            assert_eq!(
                scan(&f, index, Some(*key)),
                sorted[position + 1..],
                "resume after {key:?}"
            );
        }
        // A cursor that names no row still resumes at the keys after it: one
        // between two rows (a generation no row has), one before the first
        // and one after the last.
        let between = EvaluationKey {
            generation: 99,
            ..sorted[0]
        };
        assert!(f.owner.committed().evaluation(between).is_none());
        assert_eq!(scan(&f, index, Some(between)), sorted[1..]);
        let before = EvaluationKey {
            target: EvaluationTarget::Admission,
            generation: 0,
            ..sorted[0]
        };
        assert_eq!(scan(&f, index, Some(before)), sorted);
        let after = EvaluationKey {
            target: EvaluationTarget::Delivery {
                response: TestamentId::from_u128(u128::MAX),
            },
            generation: u64::MAX,
            ..sorted[0]
        };
        assert!(scan(&f, index, Some(after)).is_empty());
        // A cursor of another declaration or claim never repositions the scan
        // into that declaration: the scan restarts at its own first key.
        let other_validation = EvaluationKey {
            validation: ValidationId::from_u128(200 + u128::from(3 - index)),
            ..sorted[0]
        };
        assert_eq!(scan(&f, index, Some(other_validation)), sorted);
        let other_claim = EvaluationKey {
            claim: ClaimId::from_u128(2),
            ..sorted[5]
        };
        assert_eq!(scan(&f, index, Some(other_claim)), sorted);
    }
    // The claim's whole span holds both declarations' rows and the delivery
    // evaluations, each declaration's keys contiguous within it.
    let whole: Vec<EvaluationKey> = f
        .owner
        .committed_core()
        .native_claim_evaluations_from(ClaimId::from_u128(1), None)
        .collect();
    assert!(whole.windows(2).all(|pair| pair[0] < pair[1]));
    let mut expected = declaration_keys(&f, 1);
    expected.extend(declaration_keys(&f, 2));
    expected.sort();
    let increments: Vec<EvaluationKey> = whole
        .iter()
        .copied()
        .filter(|key| matches!(key.target, EvaluationTarget::Increment { .. }))
        .collect();
    assert_eq!(increments, expected);
    assert_eq!(
        whole
            .iter()
            .filter(|key| matches!(key.target, EvaluationTarget::Delivery { .. }))
            .count(),
        3
    );
}
