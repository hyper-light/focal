use super::*;
use focal_model::lifecycle::{Principal, validation as v};
use focal_model::lifecycle::{
    claim_descriptor as authored_claim, validation_descriptor as authored_validation,
};
use focal_model::{
    ClaimId, ContentHash, Deadline, HandlerRef, ObjectId, TimerId, ValidationKind, ValidationMode,
    ValidationPhase, ValidatorId,
};

#[derive(Clone, Copy)]
enum AllocationFault {
    Fail,
    Excess,
}
std::thread_local! {
    static ALLOCATION_FAULT: std::cell::Cell<Option<AllocationFault>> = const { std::cell::Cell::new(None) };
    static ALLOCATION_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
pub(super) fn allocation_capacity(requested: usize) -> Result<usize, MemoryError> {
    ALLOCATION_ATTEMPTS.set(ALLOCATION_ATTEMPTS.get() + 1);
    match ALLOCATION_FAULT.get() {
        None => Ok(requested),
        Some(AllocationFault::Fail) => Err(MemoryError::AllocationFailed),
        Some(AllocationFault::Excess) => requested
            .checked_add(1)
            .ok_or(MemoryError::AllocationFailed),
    }
}
fn with_fault<T>(fault: AllocationFault, action: impl FnOnce() -> T) -> T {
    struct Reset(Option<AllocationFault>);
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATION_FAULT.set(self.0);
        }
    }
    let _reset = Reset(ALLOCATION_FAULT.replace(Some(fault)));
    action()
}
fn assert_fault<T>(fault: AllocationFault, result: Result<T, MemoryError>) {
    assert!(matches!(
        (fault, result),
        (AllocationFault::Fail, Err(MemoryError::AllocationFailed))
            | (AllocationFault::Excess, Err(MemoryError::Capacity { .. }))
    ));
}

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
    assert!(!std::ptr::eq(
        copied_definition.get().unwrap(),
        original.get().unwrap()
    ));
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
    let invalid = OwnedDeclaration(DeclarationStorage::Legacy(Vec::new()));
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

fn authored_definition(claim: &ClaimState) -> ValidationDescriptor {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(71),
        version: ContentHash([71; 32]),
        agentic: false,
    };
    let handlers = [v::HandlerPolicy {
        handler: &handler,
        attempts: 2,
        proof_schema: ContentHash([73; 32]),
        diagnostic_schema: ContentHash([74; 32]),
    }];
    let contributors = [claim.issuer()];
    let plan = ValidationDescriptor::prepare(
        Principal::Actor(claim.issuer()),
        authored_validation::ValidationSpec {
            ledger: claim.binding().ledger,
            id: focal_model::ValidationId::from_u128(70),
            schema: 1,
            claim: ClaimId(claim.binding().object.0),
            issuer: claim.issuer(),
            declaration_index: 70,
            kind: ValidationKind::Inspection,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: v::TargetDeclaration::WholeWorkSlot {
                index: 1,
                name: "test results",
            },
            program: v::Program::Programmatic {
                check: v::PhasePolicy {
                    evaluator: claim.issuer(),
                    definition: ContentHash([72; 32]),
                    required_policy: Some(ContentHash([76; 32])),
                    handlers: &handlers,
                },
                quality: None,
            },
            deadline: Deadline {
                timer: TimerId::from_u128(75),
                generation: 1,
                at: 100,
            },
            description: "Inspect success or failure using the exact test output.",
            quality_bar: Some("Retain the failed assertion and error output."),
            contributed_by: &contributors,
            policy_revision: 3,
        },
        authored_validation::Limits {
            declaration: v::Limits {
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

fn authored_content(claim: &ClaimState) -> ClaimDescriptor {
    use focal_model::{
        ActionType, OccurrenceId, Relation, RelationKind, RelationTarget, RequirementRef,
        RootCommandId, ScopeKind,
    };
    let descriptor = authored_definition(claim);
    let relations = [
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(claim.issuer()),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(claim.subject()),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(ActionType::Work),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId::from_u128(90)),
        },
    ];
    let scopes = [authored_claim::ScopeSpec {
        kind: ScopeKind::Component,
        key: "focal",
    }];
    let requirements = [RequirementRef {
        id: focal_model::ValidationId::from_u128(70),
        specification: descriptor.specification_hash(),
    }];
    let checks = [focal_model::lifecycle::aggregation::CheckPolicy {
        declaration_index: 70,
        validation: requirements[0].id,
        mode: ValidationMode::Required,
    }];
    let slots = [focal_model::lifecycle::aggregation::SlotPolicy {
        slot: 1,
        missing_declaration_index: 71,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let plan = ClaimDescriptor::prepare(
        authored_claim::ClaimSpec {
            ledger: claim.binding().ledger,
            id: ClaimId(claim.binding().object.0),
            schema: 1,
            occurrence: OccurrenceId::from_u128(91),
            description: "Fix this regression and report the tests, including failures.",
            relations: &relations,
            scopes: &scopes,
            requirements: &requirements,
            slots: &slots,
            deadline: Some(Deadline {
                timer: TimerId::from_u128(92),
                generation: 1,
                at: 100,
            }),
        },
        authored_claim::Limits {
            description_bytes: 256,
            relations: 8,
            scopes: 4,
            scope_key_bytes: 64,
            requirements: 4,
            slots: 4,
            checks: 8,
            construction_bytes: 4096,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    plan.build(charge).unwrap()
}

fn handler_address(declaration: &Declaration) -> *const HandlerRef {
    match declaration.program() {
        v::ProgramView::Programmatic { check, .. } => {
            check.handlers().next().unwrap().handler as *const HandlerRef
        }
        _ => panic!("expected authored programmatic handler"),
    }
}

fn same_authored(actual: &OwnedDeclaration, expected: &OwnedDeclaration) {
    let actual = actual.descriptor().unwrap();
    let expected = expected.descriptor().unwrap();
    assert_eq!(actual.binding(), expected.binding());
    assert_eq!(actual.intent_fingerprint(), expected.intent_fingerprint());
    assert_eq!(actual.specification_hash(), expected.specification_hash());
    assert_eq!(actual.description(), expected.description());
    assert_eq!(actual.quality_bar(), expected.quality_bar());
    assert_eq!(actual.contributed_by(), expected.contributed_by());
    assert_eq!(actual.policy_revision(), expected.policy_revision());
    assert_eq!(
        actual.declaration().intent_fingerprint(),
        expected.declaration().intent_fingerprint()
    );
}

#[test]
fn authored_declaration_moves_existing_buffers_once_and_legacy_heap_stays_compact() {
    let claim = crate::native::tests::owned_claim_fixture();
    let legacy = definition(&claim);
    let legacy_handler = handler_address(&legacy);
    let legacy_heap = declaration_heap(&legacy).unwrap();
    let legacy = OwnedDeclaration::new(legacy).unwrap();
    assert!(legacy.descriptor().is_none());
    assert_eq!(
        legacy.heap_charge().unwrap(),
        size_of::<Declaration>() + ALLOCATION + legacy_heap
    );
    assert_eq!(handler_address(legacy.get().unwrap()), legacy_handler);

    let descriptor = authored_definition(&claim);
    let fingerprint = descriptor.intent_fingerprint();
    let handler = handler_address(descriptor.declaration());
    let description = descriptor.description().as_ptr();
    let quality = descriptor.quality_bar().unwrap().as_ptr();
    let contributors = descriptor.contributed_by().as_ptr();
    let nested = authored_declaration_heap(&descriptor).unwrap();
    let before = ALLOCATION_ATTEMPTS.get();
    let owned = OwnedDeclaration::new_authored(descriptor).unwrap();
    assert_eq!(ALLOCATION_ATTEMPTS.get() - before, 1);
    let descriptor = owned.descriptor().unwrap();
    assert_eq!(descriptor.intent_fingerprint(), fingerprint);
    assert!(std::ptr::eq(owned.get().unwrap(), descriptor.declaration()));
    assert_eq!(handler_address(owned.get().unwrap()), handler);
    assert_eq!(descriptor.description().as_ptr(), description);
    assert_eq!(descriptor.quality_bar().unwrap().as_ptr(), quality);
    assert_eq!(descriptor.contributed_by().as_ptr(), contributors);
    assert_eq!(
        owned.heap_charge().unwrap(),
        OwnedDeclaration::authored_container_charge() + nested
    );
}

#[test]
fn authored_claim_content_preserves_all_buffers_and_original_responsibility_profile() {
    use focal_model::{ReceiptFence, ReceiptId};
    let claim = crate::native::tests::owned_claim_fixture();
    let descriptor = authored_content(&claim);
    let fingerprint = descriptor.intent_fingerprint();
    let description = descriptor.description().as_ptr();
    let relations = descriptor.relations().as_ptr();
    let requirements = descriptor.requirements().as_ptr();
    let checks = descriptor.slots().next().unwrap().checks.as_ptr();
    let nested = claim_content_heap(&descriptor).unwrap();
    let scope_limits = ScopeLimits {
        scopes: 2,
        roots: 4,
        children: 8,
    };
    let owner = Some(Owner {
        expected: claim.binding(),
        receipt: Some(ReceiptFence {
            receipt: ReceiptId::from_u128(95),
            epoch: 7,
        }),
    });
    let before = ALLOCATION_ATTEMPTS.get();
    let owned = OwnedClaimContent::new(descriptor, 4, scope_limits, owner).unwrap();
    assert_eq!(ALLOCATION_ATTEMPTS.get() - before, 1);
    let descriptor = owned.get().unwrap();
    assert!(std::ptr::eq(descriptor, owned.descriptor().unwrap()));
    assert_eq!(descriptor.intent_fingerprint(), fingerprint);
    assert_eq!(descriptor.description().as_ptr(), description);
    assert_eq!(descriptor.relations().as_ptr(), relations);
    assert_eq!(descriptor.requirements().as_ptr(), requirements);
    assert_eq!(descriptor.slots().next().unwrap().checks.as_ptr(), checks);
    assert_eq!(
        owned.profile(),
        Some(ClaimContentProfile {
            max_responses: 4,
            scope_limits,
            owner
        })
    );
    assert_eq!(
        owned.heap_charge().unwrap(),
        OwnedClaimContent::container_charge() + nested
    );
}

#[test]
fn authored_copies_compact_container_capacity_preserve_profiles_and_outlive_sources() {
    let claim = crate::native::tests::owned_claim_fixture();
    let mut original = OwnedDeclaration::new_authored(authored_definition(&claim)).unwrap();
    let DeclarationStorage::Authored(rows) = &mut original.0 else {
        panic!("authored fixture");
    };
    rows.reserve_exact(4);
    let old_heap = original.heap_charge().unwrap();
    let original_descriptor = original.descriptor().unwrap();
    let fingerprint = original_descriptor.intent_fingerprint();
    let specification = original_descriptor.specification_hash();
    let copied = original.copy().unwrap();
    assert!(copied.heap_charge().unwrap() < old_heap);
    same_authored(&copied, &original);
    assert_ne!(
        copied.descriptor().unwrap().description().as_ptr(),
        original_descriptor.description().as_ptr()
    );
    assert_ne!(
        handler_address(copied.get().unwrap()),
        handler_address(original.get().unwrap())
    );
    drop(original);
    assert_eq!(
        copied.descriptor().unwrap().intent_fingerprint(),
        fingerprint
    );
    assert_eq!(
        copied.descriptor().unwrap().specification_hash(),
        specification
    );

    let profile = ClaimContentProfile {
        max_responses: 8,
        scope_limits: ScopeLimits {
            scopes: 1,
            roots: 2,
            children: 3,
        },
        owner: Some(Owner {
            expected: claim.binding(),
            receipt: None,
        }),
    };
    let mut original = OwnedClaimContent::new(
        authored_content(&claim),
        profile.max_responses,
        profile.scope_limits,
        profile.owner,
    )
    .unwrap();
    original.0.reserve_exact(4);
    let old_heap = original.heap_charge().unwrap();
    let fingerprint = original.get().unwrap().intent_fingerprint();
    let copied = original.copy().unwrap();
    assert!(copied.heap_charge().unwrap() < old_heap);
    assert_eq!(copied.get(), original.get());
    assert_eq!(copied.profile(), original.profile());
    assert_ne!(
        copied.get().unwrap().description().as_ptr(),
        original.get().unwrap().description().as_ptr()
    );
    assert_ne!(
        copied.get().unwrap().relations().as_ptr(),
        original.get().unwrap().relations().as_ptr()
    );
    assert_ne!(
        copied
            .get()
            .unwrap()
            .slots()
            .next()
            .unwrap()
            .checks
            .as_ptr(),
        original
            .get()
            .unwrap()
            .slots()
            .next()
            .unwrap()
            .checks
            .as_ptr()
    );
    drop(original);
    assert_eq!(copied.get().unwrap().intent_fingerprint(), fingerprint);
    assert_eq!(copied.profile(), Some(profile));
}

#[test]
fn authored_container_allocation_refusal_preserves_copy_sources_and_allows_retry() {
    let claim = crate::native::tests::owned_claim_fixture();
    let original = OwnedDeclaration::new_authored(authored_definition(&claim)).unwrap();
    let content = OwnedClaimContent::new(
        authored_content(&claim),
        4,
        ScopeLimits {
            scopes: 0,
            roots: 0,
            children: 0,
        },
        None,
    )
    .unwrap();
    let fingerprint = original.descriptor().unwrap().intent_fingerprint();
    let content_fingerprint = content.get().unwrap().intent_fingerprint();
    let handler = handler_address(original.get().unwrap());
    let description = content.get().unwrap().description().as_ptr();
    for fault in [AllocationFault::Fail, AllocationFault::Excess] {
        assert_fault(fault, with_fault(fault, || original.copy()));
        assert_fault(fault, with_fault(fault, || content.copy()));
        assert_fault(
            fault,
            with_fault(fault, || {
                OwnedDeclaration::new_authored(authored_definition(&claim))
            }),
        );
        assert_fault(
            fault,
            with_fault(fault, || {
                OwnedClaimContent::new(
                    authored_content(&claim),
                    4,
                    ScopeLimits {
                        scopes: 0,
                        roots: 0,
                        children: 0,
                    },
                    None,
                )
            }),
        );
        assert_eq!(
            original.descriptor().unwrap().intent_fingerprint(),
            fingerprint
        );
        assert_eq!(handler_address(original.get().unwrap()), handler);
        assert_eq!(
            content.get().unwrap().intent_fingerprint(),
            content_fingerprint
        );
        assert_eq!(content.get().unwrap().description().as_ptr(), description);
        same_authored(&original.copy().unwrap(), &original);
        let retry = content.copy().unwrap();
        assert_eq!(retry.get(), content.get());
        assert_eq!(retry.profile(), content.profile());
    }
    for invalid in [
        OwnedDeclaration(DeclarationStorage::Legacy(Vec::new())),
        OwnedDeclaration(DeclarationStorage::Authored(Vec::new())),
    ] {
        assert!(invalid.get().is_none());
        assert!(invalid.descriptor().is_none());
        assert!(matches!(
            invalid.heap_charge(),
            Err(MemoryError::MissingKey)
        ));
        assert!(matches!(invalid.copy(), Err(MemoryError::MissingKey)));
    }
    let invalid = OwnedClaimContent(Vec::new());
    assert!(invalid.get().is_none());
    assert!(invalid.profile().is_none());
    assert!(matches!(
        invalid.heap_charge(),
        Err(MemoryError::MissingKey)
    ));
    assert!(matches!(invalid.copy(), Err(MemoryError::MissingKey)));
    assert!(matches!(
        container_heap::<ClaimContentRow>(usize::MAX),
        Err(MemoryError::CounterExhausted(_))
    ));
    assert!(matches!(
        container_heap::<ValidationDescriptor>(usize::MAX),
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
        invocation: RequestKey {
            principal: claim.issuer(),
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(80),
        }
        .into(),
        sequence: SessionSeq(1),
        ordinal: 0,
        fact: NativeFact::Claim(NativeClaimEvent {
            graph: None,
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
