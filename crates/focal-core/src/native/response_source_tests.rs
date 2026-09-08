use super::*;
use focal_model::{ArtifactId, ClaimId};
use std::cell::Cell;

fn artifact(id: u8) -> ArtifactRef {
    ArtifactRef {
        id: ArtifactId([id; 16]),
        hash: ContentHash([id; 32]),
    }
}
fn spec<'a>(manifest: &'a [SlotBinding], diagnostics: &'a [ArtifactRef]) -> NativeResponseSpec<'a> {
    NativeResponseSpec {
        summary: "failed: é",
        confidence: Confidence::Committed,
        outcome: OutcomeKind::Failed,
        manifest,
        diagnostics,
    }
}
fn hash(source: &impl NativeResponseSource) -> ContentHash {
    let mut hash = blake3::Hasher::new();
    hash_into(source, &mut hash).unwrap();
    ContentHash(*hash.finalize().as_bytes())
}

#[test]
fn source_plans_preserve_all_outcomes_order_original_hash_and_exact_final_buffers() {
    let manifest = [
        SlotBinding {
            slot: 9,
            artifact: artifact(1),
        },
        SlotBinding {
            slot: 3,
            artifact: artifact(2),
        },
    ];
    let diagnostics = [artifact(4), artifact(3), artifact(4)];
    for confidence in [
        Confidence::Hint,
        Confidence::Tentative,
        Confidence::Committed,
        Confidence::Consensus,
    ] {
        for outcome in [
            OutcomeKind::Complete,
            OutcomeKind::Partial,
            OutcomeKind::Refused,
            OutcomeKind::Impossible,
            OutcomeKind::Interrupted,
            OutcomeKind::Failed,
        ] {
            let spec = NativeResponseSpec {
                confidence,
                outcome,
                ..spec(&manifest, &diagnostics)
            };
            let plan =
                NativeResponseSourcePlan::prepare(spec, NativeLimits::default(), 1 << 20).unwrap();
            let quote = plan.quote();
            assert_eq!(plan.source().summary.as_ptr(), spec.summary.as_ptr());
            assert_eq!(plan.fingerprint(), hash(&spec));
            assert_eq!(quote.bytes, charge(spec.summary.len(), 2, 3).unwrap());
            assert_eq!(quote.allocations, 3);
            assert_eq!(
                NativeResponseSourcePlan::prepare(
                    spec,
                    NativeLimits::default(),
                    quote.prepare_visits
                )
                .unwrap()
                .quote(),
                quote
            );
            assert!(
                NativeResponseSourcePlan::prepare(
                    spec,
                    NativeLimits::default(),
                    quote.prepare_visits - 1
                )
                .is_err()
            );
            let mut expected = blake3::Hasher::new();
            expected.update(b"prefix");
            hash_into(&spec, &mut expected).unwrap();
            let mut actual = blake3::Hasher::new();
            actual.update(b"prefix");
            plan.hash_into(&mut actual, quote.hash_visits).unwrap();
            assert_eq!(actual.finalize(), expected.finalize());
            let built = plan.build(quote.bytes, quote.build_visits).unwrap();
            assert_eq!(built.heap_charge().unwrap(), quote.bytes);
            assert_eq!(hash(&built.as_spec()), hash(&spec));
            assert_eq!(built.manifest, manifest);
            assert_eq!(built.diagnostics, diagnostics);
        }
    }
}

struct Mutable {
    summary: Cell<&'static str>,
    slot: Cell<SlotBinding>,
    count: Cell<usize>,
    absent: Cell<bool>,
    extra: Cell<bool>,
    fail_diagnostic: Cell<bool>,
    reads: Cell<usize>,
}
impl Mutable {
    fn new() -> Self {
        Self {
            summary: Cell::new("failure"),
            slot: Cell::new(SlotBinding {
                slot: 1,
                artifact: artifact(1),
            }),
            count: Cell::new(1),
            absent: Cell::new(false),
            extra: Cell::new(false),
            fail_diagnostic: Cell::new(false),
            reads: Cell::new(0),
        }
    }
}
impl NativeResponseSource for Mutable {
    fn summary(&self) -> &str {
        self.summary.get()
    }
    fn confidence(&self) -> Confidence {
        Confidence::Committed
    }
    fn outcome(&self) -> OutcomeKind {
        OutcomeKind::Failed
    }
    fn manifest_len(&self) -> usize {
        self.count.get()
    }
    fn diagnostic_len(&self) -> usize {
        1
    }
    fn manifest(&self, index: usize) -> Result<Option<SlotBinding>, NativeError> {
        self.reads.set(self.reads.get() + 1);
        Ok(
            if (index == 0 && !self.absent.get()) || (index == 1 && self.extra.get()) {
                Some(self.slot.get())
            } else {
                None
            },
        )
    }
    fn diagnostic(&self, index: usize) -> Result<Option<ArtifactRef>, NativeError> {
        self.reads.set(self.reads.get() + 1);
        if self.fail_diagnostic.get() {
            return Err(invalid());
        }
        Ok((index == 0).then(|| artifact(2)))
    }
}

#[test]
fn a_changed_generic_source_cannot_change_the_captured_body_or_a_caller_hasher() {
    for change in 0..5 {
        let source = Mutable::new();
        let plan =
            NativeResponseSourcePlan::prepare(&source, NativeLimits::default(), 1 << 20).unwrap();
        let quote = plan.quote();
        match change {
            0 => source.slot.set(SlotBinding {
                slot: 7,
                artifact: artifact(1),
            }),
            1 => source.summary.set("changed"),
            2 => source.count.set(2),
            3 => source.absent.set(true),
            _ => source.extra.set(true),
        }
        let mut target = blake3::Hasher::new();
        target.update(b"unchanged caller prefix");
        let before = target.finalize();
        assert!(plan.hash_into(&mut target, quote.hash_visits).is_err());
        assert_eq!(target.finalize(), before);
        assert!(plan.build(quote.bytes, quote.build_visits).is_err());
    }
}

#[test]
fn source_failures_after_partial_construction_drop_work_and_allow_an_exact_retry() {
    let source = Mutable::new();
    let plan =
        NativeResponseSourcePlan::prepare(&source, NativeLimits::default(), 1 << 20).unwrap();
    let quote = plan.quote();
    let identity = plan.fingerprint();
    source.fail_diagnostic.set(true);
    assert!(plan.build(quote.bytes, quote.build_visits).is_err());
    source.fail_diagnostic.set(false);
    let retry =
        NativeResponseSourcePlan::prepare(&source, NativeLimits::default(), quote.prepare_visits)
            .unwrap();
    assert_eq!(retry.fingerprint(), identity);
    let built = retry.build(quote.bytes, quote.build_visits).unwrap();
    assert_eq!(hash(&built.as_spec()), identity);
    assert_eq!(built.diagnostics, [artifact(2)]);
}

#[test]
fn insufficient_bytes_visits_and_overflow_refuse_before_reading_source_rows() {
    let source = Mutable::new();
    let plan =
        NativeResponseSourcePlan::prepare(&source, NativeLimits::default(), 1 << 20).unwrap();
    let quote = plan.quote();
    source.reads.set(0);
    assert!(plan.build(quote.bytes - 1, quote.build_visits).is_err());
    assert_eq!(source.reads.get(), 0);
    let plan =
        NativeResponseSourcePlan::prepare(&source, NativeLimits::default(), quote.prepare_visits)
            .unwrap();
    source.reads.set(0);
    assert!(plan.build(quote.bytes, quote.build_visits - 1).is_err());
    assert_eq!(source.reads.get(), 0);
    assert!(
        NativeResponseSourcePlan::prepare(
            &source,
            NativeLimits {
                response_summary_bytes: 1,
                ..NativeLimits::default()
            },
            usize::MAX
        )
        .is_err()
    );
    assert_eq!(source.reads.get(), 0);
    assert!(
        Shape {
            summary: usize::MAX,
            manifest: 0,
            diagnostics: 0
        }
        .quote()
        .is_err()
    );
    assert!(
        Shape {
            summary: 0,
            manifest: usize::MAX,
            diagnostics: 0
        }
        .quote()
        .is_err()
    );
    let empty = NativeResponseSpec {
        summary: "",
        confidence: Confidence::Hint,
        outcome: OutcomeKind::Failed,
        manifest: &[],
        diagnostics: &[],
    };
    let plan = NativeResponseSourcePlan::prepare(empty, NativeLimits::default(), 1024).unwrap();
    let quote = plan.quote();
    assert_eq!((quote.bytes, quote.allocations), (0, 0));
    assert!(
        plan.build(0, quote.build_visits)
            .unwrap()
            .diagnostics
            .is_empty()
    );
}

struct MutableRoots {
    value: Cell<WaitPredicate>,
    absent: Cell<bool>,
    extra: Cell<bool>,
}
impl NativeMonitorSource for MutableRoots {
    fn len(&self) -> usize {
        1
    }
    fn root(&self, index: usize) -> Result<Option<WaitPredicate>, NativeError> {
        Ok(
            if (index == 0 && !self.absent.get()) || (index == 1 && self.extra.get()) {
                Some(self.value.get())
            } else {
                None
            },
        )
    }
}

#[test]
fn monitor_sources_preserve_root_order_and_reject_changed_or_nonterminating_bodies() {
    let roots = [
        WaitPredicate::Released(ClaimId::from_u128(3)),
        WaitPredicate::Satisfied(ClaimId::from_u128(1)),
        WaitPredicate::Terminal(ClaimId::from_u128(2)),
        WaitPredicate::Released(ClaimId::from_u128(3)),
    ];
    let plan = NativeMonitorSourcePlan::prepare(roots.as_slice(), NativeLimits::default(), 1 << 20)
        .unwrap();
    let quote = plan.quote();
    assert_eq!(quote.bytes, array::<WaitPredicate>(4).unwrap());
    assert_eq!(quote.allocations, 1);
    plan.check(quote.prepare_visits).unwrap();
    assert!(plan.check(quote.prepare_visits - 1).is_err());
    assert_eq!(plan.build(quote.bytes, quote.build_visits).unwrap(), roots);
    for change in 0..3 {
        let source = MutableRoots {
            value: Cell::new(roots[0]),
            absent: Cell::new(false),
            extra: Cell::new(false),
        };
        let plan =
            NativeMonitorSourcePlan::prepare(&source, NativeLimits::default(), 1 << 20).unwrap();
        let quote = plan.quote();
        match change {
            0 => source.value.set(roots[1]),
            1 => source.absent.set(true),
            _ => source.extra.set(true),
        }
        assert!(plan.check(quote.prepare_visits).is_err());
        assert!(plan.build(quote.bytes, quote.build_visits).is_err());
    }
    let plan = NativeMonitorSourcePlan::prepare(
        roots.as_slice(),
        NativeLimits::default(),
        quote.prepare_visits,
    )
    .unwrap();
    assert!(plan.build(quote.bytes, quote.build_visits - 1).is_err());
    let plan = NativeMonitorSourcePlan::prepare(
        roots.as_slice(),
        NativeLimits::default(),
        quote.prepare_visits,
    )
    .unwrap();
    assert!(plan.build(quote.bytes - 1, quote.build_visits).is_err());
    assert!(
        NativeMonitorSourcePlan::prepare(
            roots.as_slice(),
            NativeLimits {
                plan_edges: 3,
                ..NativeLimits::default()
            },
            usize::MAX
        )
        .is_err()
    );
}
