use super::*;
use focal_model::lifecycle::{Principal, validation as v};
use focal_model::{
    ClaimId, ContentHash, Deadline, HandlerRef, ObjectId, TimerId, ValidationKind, ValidationMode,
    ValidationPhase, ValidatorId,
};

fn registrations(claim: &ClaimState) -> RegistrationSet {
    RegistrationSet::new(claim, 16, size_of::<RegistrationSet>()).unwrap()
}
fn claim_row() -> OwnedClaim {
    let claim = crate::native::tests::owned_claim_fixture();
    let registrations = registrations(&claim);
    OwnedClaim::new(claim, registrations).unwrap()
}
fn definition(claim: &ClaimState) -> Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(71),
        version: ContentHash([71; 32]),
        agentic: false,
    };
    Declaration::new(
        Principal::Actor(claim.issuer()),
        v::DeclarationSpec {
            binding: focal_model::lifecycle::Binding {
                object: ObjectId::from_u128(70),
                ..claim.binding()
            },
            claim: ClaimId(claim.binding().object.0),
            issuer: claim.issuer(),
            declaration_index: 70,
            kind: ValidationKind::Inspection,
            phase: ValidationPhase::Admission,
            mode: ValidationMode::Required,
            target: v::TargetDeclaration::Admission,
            program: v::Program::Programmatic {
                check: v::PhasePolicy {
                    evaluator: claim.issuer(),
                    definition: ContentHash([72; 32]),
                    required_policy: None,
                    handlers: &[v::HandlerPolicy {
                        handler: &handler,
                        attempts: 2,
                        proof_schema: ContentHash([73; 32]),
                        diagnostic_schema: ContentHash([74; 32]),
                    }],
                },
                quality: None,
            },
            deadline: Deadline {
                timer: TimerId::from_u128(75),
                generation: 1,
                at: 100,
            },
        },
        v::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

#[test]
fn moving_claim_only_allocates_its_indirection_and_retains_nested_buffers() {
    let claim = crate::native::tests::owned_claim_fixture();
    let declarations = claim.acceptance().declarations().as_ptr();
    let nested = claim_heap(&claim).unwrap();
    let binding = claim.binding();
    let registrations = registrations(&claim);
    let row = OwnedClaim::new(claim, registrations).unwrap();
    assert_eq!(row.claim().unwrap().binding(), binding);
    assert_eq!(
        row.claim().unwrap().acceptance().declarations().as_ptr(),
        declarations
    );
    assert_eq!(
        row.heap_charge().unwrap(),
        OwnedClaim::container_charge() + nested
    );
    assert_eq!(size_of::<OwnedClaim>(), size_of::<Vec<ClaimRow>>());
    assert!(size_of::<OwnedClaim>() < size_of::<ClaimState>());
    assert!(row.registrations().unwrap().rows().is_empty());
    row.registrations()
        .unwrap()
        .check(row.claim().unwrap())
        .unwrap();
}

#[test]
fn copied_claim_and_registration_seals_are_independent_and_fully_charged() {
    let mut original = claim_row();
    let copied = original.copy().unwrap();
    assert_eq!(copied.claim(), original.claim());
    assert_eq!(copied.registrations(), original.registrations());
    assert_ne!(copied.0.as_ptr(), original.0.as_ptr());
    assert_ne!(
        copied.claim().unwrap().acceptance().declarations().as_ptr(),
        original
            .claim()
            .unwrap()
            .acceptance()
            .declarations()
            .as_ptr()
    );
    assert!(copied.heap_charge().unwrap() <= original.heap_charge().unwrap());
    let (claim, registrations) = original.parts_mut().unwrap();
    registrations.seal_targets(claim).unwrap();
    assert!(original.registrations().unwrap().is_sealed());
    assert!(!copied.registrations().unwrap().is_sealed());
    let binding = original.claim().unwrap().binding();
    drop(original);
    assert_eq!(copied.claim().unwrap().binding(), binding);
}

#[test]
fn owned_definition_and_evaluation_copy_outlive_sources_and_rebind_exactly() {
    let claim = crate::native::tests::owned_claim_fixture();
    let declaration = definition(&claim);
    let fingerprint = declaration.intent_fingerprint();
    let nested = declaration_heap(&declaration).unwrap();
    assert!(nested > 0);
    let original = OwnedDeclaration::new(declaration).unwrap();
    let definition = original.get().unwrap();
    let evaluation = v::Evaluation::materialize(
        Principal::Actor(claim.issuer()),
        definition,
        v::Materialization {
            binding: definition.binding(),
            target: v::Target::Admission {
                claim: claim.binding(),
            },
            slot_name: None,
            generation: 1,
            receipt: None,
        },
    )
    .unwrap()
    .into_state();
    let evaluation = OwnedEvaluation::new(evaluation).unwrap();
    let copied_definition = original.copy().unwrap();
    let copied_evaluation = evaluation.copy().unwrap();
    assert_eq!(
        original.heap_charge().unwrap(),
        OwnedDeclaration::container_charge() + nested
    );
    assert!(copied_definition.heap_charge().unwrap() <= original.heap_charge().unwrap());
    assert_eq!(
        evaluation.heap_charge().unwrap(),
        OwnedEvaluation::container_charge()
    );
    assert_eq!(copied_evaluation.get(), evaluation.get());
    assert_ne!(copied_definition.0.as_ptr(), original.0.as_ptr());
    assert_ne!(copied_evaluation.0.as_ptr(), evaluation.0.as_ptr());
    drop(original);
    drop(evaluation);
    assert_eq!(
        copied_definition.get().unwrap().intent_fingerprint(),
        fingerprint
    );
    assert_eq!(
        copied_evaluation
            .get()
            .unwrap()
            .bind(copied_definition.get().unwrap())
            .unwrap()
            .state(),
        v::State::Ready
    );
}

#[test]
fn invalid_containers_and_charge_overflow_are_errors() {
    let mut invalid = OwnedClaim(Vec::new());
    assert!(invalid.claim().is_none());
    assert!(invalid.registrations().is_none());
    assert!(invalid.parts_mut().is_none());
    assert!(matches!(invalid.copy(), Err(MemoryError::MissingKey)));
    assert!(matches!(
        invalid.heap_charge(),
        Err(MemoryError::MissingKey)
    ));
    let invalid = OwnedDeclaration(Vec::new());
    assert!(matches!(invalid.copy(), Err(MemoryError::MissingKey)));
    assert!(matches!(
        invalid.heap_charge(),
        Err(MemoryError::MissingKey)
    ));
    let invalid = OwnedEvaluation(Vec::new());
    assert!(matches!(invalid.copy(), Err(MemoryError::MissingKey)));
    assert!(matches!(
        invalid.heap_charge(),
        Err(MemoryError::MissingKey)
    ));
    let invalid = OwnedEvent(Vec::new());
    assert!(invalid.get().is_none());
    assert!(matches!(invalid.copy(), Err(MemoryError::MissingKey)));
    assert!(matches!(
        invalid.heap_charge(),
        Err(MemoryError::MissingKey)
    ));
    assert!(matches!(
        container_heap::<ClaimRow>(usize::MAX),
        Err(MemoryError::CounterExhausted(_))
    ));
    assert!(matches!(
        container_heap::<Declaration>(usize::MAX),
        Err(MemoryError::CounterExhausted(_))
    ));
    assert!(matches!(
        container_heap::<EvaluationState>(usize::MAX),
        Err(MemoryError::CounterExhausted(_))
    ));
    assert!(matches!(
        container_heap::<StoredEvent>(usize::MAX),
        Err(MemoryError::CounterExhausted(_))
    ));
}

#[test]
fn owned_event_copy_preserves_exact_history_after_original_is_dropped() {
    use crate::native::{NativeClaimEvent, NativeEvent, NativeEventKind, NativeFact};
    use focal_model::{RequestEpoch, RequestId, RequestKey, SessionSeq};
    let claim = crate::native::tests::owned_claim_fixture();
    let ledger = claim.binding().ledger;
    let expected = NativeEvent {
        request: RequestKey {
            principal: claim.issuer(),
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(80),
        },
        sequence: SessionSeq(1),
        ordinal: 0,
        fact: NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::Created,
            before: None,
            after: claim.binding(),
            owned_child: None,
            status: claim.status(),
        }),
    };
    let original = OwnedEvent::new(StoredEvent::pack(expected).unwrap()).unwrap();
    let copied = original.copy().unwrap();
    assert_eq!(
        original.heap_charge().unwrap(),
        OwnedEvent::container_charge()
    );
    assert_eq!(
        copied.heap_charge().unwrap(),
        original.heap_charge().unwrap()
    );
    assert_ne!(original.0.as_ptr(), copied.0.as_ptr());
    assert_eq!(size_of::<OwnedEvent>(), size_of::<Vec<StoredEvent>>());
    assert!(size_of::<OwnedEvent>() < size_of::<StoredEvent>());
    assert_eq!(original.get().unwrap().expand(ledger), expected);
    drop(original);
    assert_eq!(copied.get().unwrap().expand(ledger), expected);
}
