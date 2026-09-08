use super::*;
use focal_model::{ArtifactId, ContentHash};

const MANIFEST: [SlotBinding; 1] = [SlotBinding {
    slot: 7,
    artifact: ArtifactRef {
        id: ArtifactId([1; 16]),
        hash: ContentHash([2; 32]),
    },
}];
const DIAGNOSTICS: [ArtifactRef; 2] = [
    ArtifactRef {
        id: ArtifactId([3; 16]),
        hash: ContentHash([4; 32]),
    },
    ArtifactRef {
        id: ArtifactId([5; 16]),
        hash: ContentHash([6; 32]),
    },
];

fn spec() -> NativeResponseSpec<'static> {
    NativeResponseSpec {
        summary: "step failed\n",
        confidence: Confidence::Committed,
        outcome: OutcomeKind::Failed,
        manifest: &MANIFEST,
        diagnostics: &DIAGNOSTICS,
    }
}

// Fixed preimage from the prior owned-input hash contract. Explicit counts and
// discriminants keep a shared borrowed/owned implementation from masking drift.
fn original_hash(confidence: u8, outcome: u8) -> blake3::Hash {
    let mut hash = blake3::Hasher::new();
    hash.update(b"\x0c\0\0\0\0\0\0\0step failed\n");
    hash.update(&[confidence, outcome]);
    hash.update(b"\x01\0\0\0\0\0\0\0\x07\0\0\0");
    hash.update(&[1; 16]);
    hash.update(&[2; 32]);
    hash.update(b"\x02\0\0\0\0\0\0\0");
    hash.update(&[3; 16]);
    hash.update(&[4; 32]);
    hash.update(&[5; 16]);
    hash.update(&[6; 32]);
    hash.finalize()
}

#[test]
fn borrowed_and_built_reports_preserve_all_authored_outcomes_and_original_intent() {
    for (confidence, confidence_tag) in [
        (Confidence::Hint, 0),
        (Confidence::Tentative, 1),
        (Confidence::Committed, 2),
        (Confidence::Consensus, 3),
    ] {
        for (outcome, outcome_tag) in [
            (OutcomeKind::Complete, 0),
            (OutcomeKind::Partial, 1),
            (OutcomeKind::Refused, 2),
            (OutcomeKind::Impossible, 3),
            (OutcomeKind::Interrupted, 4),
            (OutcomeKind::Failed, 5),
        ] {
            let spec = NativeResponseSpec {
                confidence,
                outcome,
                ..spec()
            };
            let plan = NativeResponseInput::prepare(spec, NativeLimits::default()).unwrap();
            assert_eq!(plan.spec().summary.as_ptr(), spec.summary.as_ptr());
            assert_eq!(plan.spec().manifest.as_ptr(), spec.manifest.as_ptr());
            assert_eq!(plan.spec().diagnostics.as_ptr(), spec.diagnostics.as_ptr());
            let mut borrowed_hash = blake3::Hasher::new();
            plan.spec().hash_into(&mut borrowed_hash).unwrap();
            assert_eq!(
                borrowed_hash.finalize(),
                original_hash(confidence_tag, outcome_tag)
            );
            let quote = plan.construction_bytes();
            let input = plan.build(quote).unwrap();
            assert_eq!(input.heap_charge().unwrap(), quote);
            assert_eq!(input.summary, spec.summary);
            assert_eq!(input.confidence, confidence);
            assert_eq!(input.outcome, outcome);
            assert_eq!(input.manifest, spec.manifest);
            assert_eq!(input.diagnostics, spec.diagnostics);
            assert_ne!(input.summary.as_ptr(), spec.summary.as_ptr());
            assert_ne!(input.manifest.as_ptr(), spec.manifest.as_ptr());
            assert_ne!(input.diagnostics.as_ptr(), spec.diagnostics.as_ptr());
            let mut owned_hash = blake3::Hasher::new();
            input.hash_into(&mut owned_hash).unwrap();
            assert_eq!(owned_hash.finalize(), borrowed_hash.finalize());
            input.check_limits(NativeLimits::default()).unwrap();
        }
    }
}

#[test]
fn construction_refuses_each_dimension_and_insufficient_allowance_before_building() {
    for limits in [
        NativeLimits {
            response_summary_bytes: 11,
            ..NativeLimits::default()
        },
        NativeLimits {
            work_artifacts_per_cycle: 0,
            ..NativeLimits::default()
        },
        NativeLimits {
            diagnostics_per_cycle: 1,
            ..NativeLimits::default()
        },
    ] {
        assert!(matches!(
            NativeResponseInput::prepare(spec(), limits),
            Err(NativeError::Capacity(_))
        ));
    }
    let plan = NativeResponseInput::prepare(spec(), NativeLimits::default()).unwrap();
    let insufficient = plan.construction_bytes().checked_sub(1).unwrap();
    assert!(matches!(
        plan.build(insufficient),
        Err(NativeError::Capacity(_))
    ));
    assert!(matches!(
        NativeResponseInput::prepare(
            spec(),
            NativeLimits {
                preparation_bytes: insufficient,
                ..NativeLimits::default()
            }
        ),
        Err(NativeError::Capacity(_))
    ));
    assert!(charge(usize::MAX, 0, 0).is_err());
    assert!(charge(0, usize::MAX, 0).is_err());
    assert!(charge(0, 0, usize::MAX).is_err());
}

#[test]
fn compact_plan_does_not_hide_existing_owned_capacity_from_admission() {
    let mut input = NativeResponseInput {
        summary: String::with_capacity(256),
        confidence: Confidence::Committed,
        outcome: OutcomeKind::Failed,
        manifest: Vec::with_capacity(16),
        diagnostics: Vec::with_capacity(16),
    };
    input.summary.push_str(spec().summary);
    input.manifest.extend_from_slice(&MANIFEST);
    input.diagnostics.extend_from_slice(&DIAGNOSTICS);
    let plan = NativeResponseInput::prepare(input.as_spec(), NativeLimits::default()).unwrap();
    let quote = plan.construction_bytes();
    assert!(input.heap_charge().unwrap() > quote);
    let limits = NativeLimits {
        preparation_bytes: quote,
        ..NativeLimits::default()
    };
    assert!(matches!(
        input.check_limits(limits),
        Err(NativeError::Capacity(_))
    ));
    let compact = plan.build(quote).unwrap();
    compact.check_limits(limits).unwrap();
    let mut original_hash = blake3::Hasher::new();
    let mut compact_hash = blake3::Hasher::new();
    input.hash_into(&mut original_hash).unwrap();
    compact.hash_into(&mut compact_hash).unwrap();
    assert_eq!(original_hash.finalize(), compact_hash.finalize());
}

#[test]
fn construction_preserves_supplied_order_and_does_not_invent_failure_evidence() {
    let empty = NativeResponseSpec {
        summary: "",
        confidence: Confidence::Hint,
        outcome: OutcomeKind::Failed,
        manifest: &[],
        diagnostics: &[],
    };
    let plan = NativeResponseInput::prepare(empty, NativeLimits::default()).unwrap();
    assert_eq!(plan.construction_bytes(), 0);
    let input = plan.build(0).unwrap();
    assert_eq!(input.heap_charge().unwrap(), 0);
    assert_eq!(input.outcome, OutcomeKind::Failed);
    assert!(input.summary.is_empty());
    assert!(input.manifest.is_empty());
    assert!(input.diagnostics.is_empty());
    // Whether this testimony is admissible belongs to the actual cycle owner.
    // Input construction must not silently sort, deduplicate or invent facts.
    let diagnostics = [DIAGNOSTICS[1], DIAGNOSTICS[0], DIAGNOSTICS[1]];
    let spec = NativeResponseSpec {
        diagnostics: &diagnostics,
        ..spec()
    };
    let plan = NativeResponseInput::prepare(spec, NativeLimits::default()).unwrap();
    let quote = plan.construction_bytes();
    assert_eq!(plan.build(quote).unwrap().diagnostics, diagnostics);
}
