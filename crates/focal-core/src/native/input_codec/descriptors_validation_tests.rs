use super::super::bytes::{CountingSink, Cursor, SliceSink};
use super::*;
use focal_model::lifecycle::{
    Principal,
    validation::{self as policy, HandlerPolicy, PhasePolicy, Program},
    validation_descriptor::{Limits, ValidationSpec},
};
use focal_model::{
    ClaimId, ContentHash, HandlerRef, ParticipantId, SessionId, TenantId, TimerId, ValidationId,
    ValidationMode, ValidatorId,
};

const ISSUER: ParticipantId = ParticipantId([1; 16]);
const CONTRIBUTORS: [ParticipantId; 2] = [ISSUER, ParticipantId([2; 16])];
const CHECK: HandlerRef = HandlerRef {
    id: ValidatorId([10; 16]),
    version: ContentHash([11; 32]),
    agentic: false,
};
const FALLBACK: HandlerRef = HandlerRef {
    id: ValidatorId([12; 16]),
    version: ContentHash([13; 32]),
    agentic: false,
};
const AGENT: HandlerRef = HandlerRef {
    id: ValidatorId([14; 16]),
    version: ContentHash([15; 32]),
    agentic: true,
};
const CHECKS: [HandlerPolicy<'static>; 2] = [
    HandlerPolicy {
        handler: &CHECK,
        attempts: 2,
        proof_schema: ContentHash([16; 32]),
        diagnostic_schema: ContentHash([17; 32]),
    },
    HandlerPolicy {
        handler: &FALLBACK,
        attempts: 1,
        proof_schema: ContentHash([18; 32]),
        diagnostic_schema: ContentHash([19; 32]),
    },
];
const AGENTS: [HandlerPolicy<'static>; 1] = [HandlerPolicy {
    handler: &AGENT,
    attempts: 2,
    proof_schema: ContentHash([20; 32]),
    diagnostic_schema: ContentHash([21; 32]),
}];

fn check() -> PhasePolicy<'static> {
    PhasePolicy {
        evaluator: ParticipantId([3; 16]),
        definition: ContentHash([22; 32]),
        handlers: &CHECKS,
        required_policy: Some(ContentHash([23; 32])),
    }
}
fn quality() -> PhasePolicy<'static> {
    PhasePolicy {
        evaluator: ParticipantId([4; 16]),
        definition: ContentHash([24; 32]),
        handlers: &AGENTS,
        required_policy: None,
    }
}
fn spec() -> ValidationSpec<'static> {
    ValidationSpec {
        ledger: LedgerId {
            tenant: TenantId([5; 16]),
            session: SessionId([6; 16]),
        },
        id: ValidationId([7; 16]),
        schema: 1,
        claim: ClaimId([8; 16]),
        issuer: ISSUER,
        declaration_index: 9,
        kind: ValidationKind::Inspection,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        target: TargetDeclaration::WholeWorkSlot {
            index: 1,
            name: "résultat",
        },
        program: Program::Programmatic {
            check: check(),
            quality: Some(quality()),
        },
        deadline: Deadline {
            timer: TimerId([26; 16]),
            generation: 2,
            at: 100,
        },
        description: "Inspect the exact evidence; retain errors. é",
        quality_bar: Some("Explain the result with cited proof. 🦀"),
        contributed_by: &CONTRIBUTORS,
        policy_revision: 3,
    }
}
fn built(spec: ValidationSpec<'_>) -> ValidationDescriptor {
    let plan = ValidationDescriptor::prepare(
        Principal::Actor(spec.issuer),
        spec,
        Limits {
            declaration: policy::Limits {
                handlers: 4,
                attempts: 8,
                slot_bytes: 64,
            },
            description_bytes: 256,
            quality_bar_bytes: 256,
            contributors: 4,
            construction_bytes: 4096,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    plan.build(charge).unwrap()
}
fn encoded(value: &ValidationDescriptor) -> (Vec<u8>, usize) {
    let mut counter = CountingSink::new(usize::MAX, usize::MAX);
    validation(&mut counter, value).unwrap();
    let mut bytes = vec![0; counter.len()];
    let mut sink = SliceSink::new(&mut bytes, counter.visits_used());
    validation(&mut sink, value).unwrap();
    sink.finish().unwrap();
    (bytes, counter.visits_used())
}

fn read_phase(cursor: &mut Cursor<'_>, expected: PhasePolicy<'_>) {
    assert_eq!(cursor.fixed::<16>().unwrap(), expected.evaluator.0);
    assert_eq!(cursor.fixed::<32>().unwrap(), expected.definition.0);
    match expected.required_policy {
        Some(pin) => {
            assert_eq!(cursor.u8().unwrap(), 1);
            assert_eq!(cursor.fixed::<32>().unwrap(), pin.0);
        }
        None => assert_eq!(cursor.u8().unwrap(), 0),
    }
    assert_eq!(cursor.count(4).unwrap(), expected.handlers.len());
    for step in expected.handlers {
        assert_eq!(cursor.fixed::<16>().unwrap(), step.handler.id.0);
        assert_eq!(cursor.fixed::<32>().unwrap(), step.handler.version.0);
        assert_eq!(cursor.u8().unwrap(), u8::from(step.handler.agentic));
        assert_eq!(cursor.u32().unwrap(), step.attempts);
        assert_eq!(cursor.fixed::<32>().unwrap(), step.proof_schema.0);
        assert_eq!(cursor.fixed::<32>().unwrap(), step.diagnostic_schema.0);
    }
}
fn read_fields(cursor: &mut Cursor<'_>, expected: ValidationSpec<'_>) {
    assert_eq!(cursor.fixed::<16>().unwrap(), expected.claim.0);
    assert_eq!(cursor.fixed::<16>().unwrap(), expected.issuer.0);
    assert_eq!(cursor.u32().unwrap(), expected.declaration_index);
    assert_eq!(cursor.u16().unwrap(), expected.kind.code());
    assert_eq!(cursor.u16().unwrap(), expected.phase.code());
    assert_eq!(
        cursor.u8().unwrap(),
        match expected.mode {
            ValidationMode::Required => 0,
            ValidationMode::Observe => 1,
        }
    );
    match expected.target {
        TargetDeclaration::WholeWorkSlot { index, name } => {
            assert_eq!(cursor.u8().unwrap(), 0);
            assert_eq!(cursor.u32().unwrap(), index);
            assert_eq!(cursor.text(64).unwrap(), name);
        }
        TargetDeclaration::Delivery => assert_eq!(cursor.u8().unwrap(), 1),
        TargetDeclaration::Admission => assert_eq!(cursor.u8().unwrap(), 2),
        TargetDeclaration::Increment => assert_eq!(cursor.u8().unwrap(), 3),
    }
    match expected.program {
        Program::Delivery => assert_eq!(cursor.u8().unwrap(), 0),
        Program::Agentic { check } => {
            assert_eq!(cursor.u8().unwrap(), 2);
            read_phase(cursor, check);
        }
        Program::Programmatic { check, quality } => {
            assert_eq!(cursor.u8().unwrap(), 1);
            read_phase(cursor, check);
            match quality {
                Some(quality) => {
                    assert_eq!(cursor.u8().unwrap(), 1);
                    read_phase(cursor, quality);
                }
                None => assert_eq!(cursor.u8().unwrap(), 0),
            }
        }
    }
    assert_eq!(cursor.fixed::<16>().unwrap(), expected.deadline.timer.0);
    assert_eq!(cursor.u64().unwrap(), expected.deadline.generation);
    assert_eq!(cursor.u64().unwrap(), expected.deadline.at);
}
fn read_descriptor(bytes: &[u8], expected: ValidationSpec<'_>) {
    let mut cursor = Cursor::new(bytes, bytes.len(), usize::MAX).unwrap();
    assert_eq!(cursor.fixed::<16>().unwrap(), expected.ledger.tenant.0);
    assert_eq!(cursor.fixed::<16>().unwrap(), expected.ledger.session.0);
    assert_eq!(cursor.fixed::<16>().unwrap(), expected.id.0);
    assert_eq!(cursor.u16().unwrap(), 1);
    read_fields(&mut cursor, expected);
    assert_eq!(cursor.text(256).unwrap(), expected.description);
    match expected.quality_bar {
        Some(bar) => {
            assert_eq!(cursor.u8().unwrap(), 1);
            assert_eq!(cursor.text(256).unwrap(), bar);
        }
        None => assert_eq!(cursor.u8().unwrap(), 0),
    }
    assert_eq!(cursor.count(4).unwrap(), expected.contributed_by.len());
    for contributor in expected.contributed_by {
        assert_eq!(cursor.fixed::<16>().unwrap(), contributor.0);
    }
    assert_eq!(cursor.u64().unwrap(), expected.policy_revision);
    cursor.finish().unwrap();
}

#[test]
fn complete_validation_bodies_preserve_actual_programs_targets_and_provenance() {
    for (target, phase) in [
        (spec().target, ValidationPhase::WholeWork),
        (TargetDeclaration::Admission, ValidationPhase::Admission),
        (TargetDeclaration::Increment, ValidationPhase::Increment),
    ] {
        for program in [
            spec().program,
            Program::Programmatic {
                check: check(),
                quality: None,
            },
            Program::Agentic { check: quality() },
        ] {
            for mode in [ValidationMode::Required, ValidationMode::Observe] {
                let source = ValidationSpec {
                    target,
                    phase,
                    program,
                    mode,
                    ..spec()
                };
                read_descriptor(&encoded(&built(source)).0, source);
            }
        }
    }
    let source = ValidationSpec {
        target: TargetDeclaration::Delivery,
        program: Program::Delivery,
        kind: ValidationKind::Receipt,
        quality_bar: None,
        contributed_by: &[],
        ..spec()
    };
    read_descriptor(&encoded(&built(source)).0, source);
}

#[test]
fn legacy_declaration_retains_its_binding_but_authored_body_omits_derived_hashes() {
    let value = built(spec());
    let mut counter = CountingSink::new(usize::MAX, usize::MAX);
    declaration(&mut counter, value.declaration()).unwrap();
    let mut bytes = vec![0; counter.len()];
    let mut sink = SliceSink::new(&mut bytes, counter.visits_used());
    declaration(&mut sink, value.declaration()).unwrap();
    sink.finish().unwrap();
    let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
    let binding = value.binding();
    assert_eq!(cursor.fixed::<16>().unwrap(), binding.ledger.tenant.0);
    assert_eq!(cursor.fixed::<16>().unwrap(), binding.ledger.session.0);
    assert_eq!(cursor.fixed::<16>().unwrap(), binding.object.0);
    assert_eq!(cursor.fixed::<32>().unwrap(), binding.content.0);
    assert_eq!(cursor.u64().unwrap(), binding.revision.0);
    read_fields(&mut cursor, spec());
    cursor.finish().unwrap();

    let (original, _) = encoded(&value);
    let changed = built(ValidationSpec {
        policy_revision: 4,
        ..spec()
    });
    let (changed_bytes, _) = encoded(&changed);
    assert_ne!(value.content_hash(), changed.content_hash());
    assert_ne!(value.specification_hash(), changed.specification_hash());
    let split = original.len() - 8;
    assert_eq!(&original[..split], &changed_bytes[..split]);
    assert_eq!(&original[split..], &3u64.to_le_bytes());
    assert_eq!(&changed_bytes[split..], &4u64.to_le_bytes());
}

#[test]
fn full_policy_encoding_enforces_exact_byte_and_visit_limits() {
    let value = built(spec());
    let original = value.intent_fingerprint();
    let (bytes, visits) = encoded(&value);
    let mut exact = CountingSink::new(bytes.len(), visits);
    validation(&mut exact, &value).unwrap();
    assert_eq!((exact.len(), exact.visits_used()), (bytes.len(), visits));
    let mut short_bytes = CountingSink::new(bytes.len() - 1, visits);
    assert_eq!(validation(&mut short_bytes, &value), Err(Error::Capacity));
    let mut short_visits = CountingSink::new(bytes.len(), visits - 1);
    assert_eq!(validation(&mut short_visits, &value), Err(Error::Capacity));
    assert_eq!(value.intent_fingerprint(), original);
}
