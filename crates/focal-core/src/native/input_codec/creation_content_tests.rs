use super::super::bytes::Sink;
use super::super::descriptors;
use super::*;
use focal_model::lifecycle::aggregation::{CheckPolicy, SlotPolicy};
use focal_model::{
    ActionType, HandlerRef, OccurrenceId, Relation, RelationKind, RelationTarget, RequirementRef,
    RootCommandId, ScopeKind, SessionId, TenantId, TimerId, ValidationKind, ValidationMode,
    ValidationPhase, ValidatorId,
};

const ISSUER: ParticipantId = ParticipantId([1; 16]);
const CHECK: HandlerRef = HandlerRef {
    id: ValidatorId([2; 16]),
    version: ContentHash([3; 32]),
    agentic: false,
};
const AGENT: HandlerRef = HandlerRef {
    id: ValidatorId([4; 16]),
    version: ContentHash([5; 32]),
    agentic: true,
};
const HANDLERS: [declaration::HandlerPolicy<'static>; 1] = [declaration::HandlerPolicy {
    handler: &CHECK,
    attempts: 2,
    proof_schema: ContentHash([6; 32]),
    diagnostic_schema: ContentHash([7; 32]),
}];
const AGENTS: [declaration::HandlerPolicy<'static>; 1] = [declaration::HandlerPolicy {
    handler: &AGENT,
    attempts: 1,
    proof_schema: ContentHash([8; 32]),
    diagnostic_schema: ContentHash([9; 32]),
}];
fn inspection() -> BodyInspectionLimits {
    BodyInspectionLimits {
        bytes: 65_536,
        parse_visits: 1_000_000,
        source_visits: 1_000_000,
    }
}
fn claim_limits() -> claim::Limits {
    claim::Limits {
        description_bytes: 256,
        relations: 8,
        scopes: 8,
        scope_key_bytes: 64,
        requirements: 8,
        slots: 8,
        checks: 8,
        construction_bytes: 65_536,
    }
}
fn validation_limits() -> validation::Limits {
    validation::Limits {
        declaration: declaration::Limits {
            handlers: 8,
            attempts: 16,
            slot_bytes: 64,
        },
        description_bytes: 256,
        quality_bar_bytes: 256,
        contributors: 8,
        construction_bytes: 65_536,
    }
}
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId([1; 16]),
        session: SessionId([2; 16]),
    }
}
fn deadline() -> Deadline {
    Deadline {
        timer: TimerId([3; 16]),
        generation: 4,
        at: 9876,
    }
}
fn claim_value() -> claim::ClaimDescriptor {
    let relations = [
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(ISSUER),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(ParticipantId([10; 16])),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(ActionType::Challenge),
        },
        Relation {
            kind: RelationKind::DependsOn,
            target: RelationTarget::Object(focal_model::ObjectRef::claim(
                ledger(),
                ClaimId([11; 16]),
            )),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId([12; 16])),
        },
    ];
    let requirements = [
        RequirementRef {
            id: ValidationId([14; 16]),
            specification: ContentHash([15; 32]),
        },
        RequirementRef {
            id: ValidationId([13; 16]),
            specification: ContentHash([16; 32]),
        },
    ];
    let checks = [
        CheckPolicy {
            declaration_index: 2,
            validation: requirements[1].id,
            mode: ValidationMode::Required,
        },
        CheckPolicy {
            declaration_index: 3,
            validation: requirements[0].id,
            mode: ValidationMode::Observe,
        },
    ];
    let slots = [
        SlotPolicy {
            slot: 1,
            missing_declaration_index: 1,
            mode: ValidationMode::Required,
            checks: &checks,
        },
        SlotPolicy {
            slot: 2,
            missing_declaration_index: 4,
            mode: ValidationMode::Observe,
            checks: &[],
        },
    ];
    let scopes = [
        claim::ScopeSpec {
            kind: ScopeKind::File,
            key: "src/é.rs",
        },
        claim::ScopeSpec {
            kind: ScopeKind::UxSurface,
            key: "settings",
        },
    ];
    let plan = claim::ClaimDescriptor::prepare(
        claim::ClaimSpec {
            ledger: ledger(),
            id: ClaimId([20; 16]),
            schema: 1,
            occurrence: OccurrenceId([21; 16]),
            description: "Fix this regression; supply logs even on failure.",
            relations: &relations,
            scopes: &scopes,
            requirements: &requirements,
            slots: &slots,
            deadline: Some(deadline()),
            policy: None,
        },
        claim_limits(),
    )
    .unwrap();
    let bytes = plan.construction_charge();
    plan.build(bytes).unwrap()
}
fn phase(agentic: bool) -> declaration::PhasePolicy<'static> {
    declaration::PhasePolicy {
        evaluator: ISSUER,
        definition: ContentHash([22; 32]),
        handlers: if agentic { &AGENTS } else { &HANDLERS },
        required_policy: Some(ContentHash([23; 32])),
    }
}
fn validation_value(program: u8, target: u8) -> validation::ValidationDescriptor {
    let policy = match program {
        0 => declaration::Program::Delivery,
        1 => declaration::Program::Programmatic {
            check: phase(false),
            quality: None,
        },
        2 => declaration::Program::Programmatic {
            check: phase(false),
            quality: Some(phase(true)),
        },
        _ => declaration::Program::Agentic { check: phase(true) },
    };
    let target = if program == 0 {
        declaration::TargetDeclaration::Delivery
    } else {
        match target {
            0 => declaration::TargetDeclaration::WholeWorkSlot {
                index: 1,
                name: "résultat",
            },
            1 => declaration::TargetDeclaration::Admission,
            _ => declaration::TargetDeclaration::Increment,
        }
    };
    let phase = match target {
        declaration::TargetDeclaration::Admission => ValidationPhase::Admission,
        declaration::TargetDeclaration::Increment => ValidationPhase::Increment,
        _ => ValidationPhase::WholeWork,
    };
    let contributors = [ISSUER, ParticipantId([24; 16])];
    let plan = validation::ValidationDescriptor::prepare(
        Principal::Actor(ISSUER),
        validation::ValidationSpec {
            ledger: ledger(),
            id: ValidationId([25; 16]),
            schema: 1,
            claim: ClaimId([20; 16]),
            issuer: ISSUER,
            declaration_index: 2,
            kind: if program == 0 {
                ValidationKind::Receipt
            } else {
                ValidationKind::Inspection
            },
            phase,
            mode: ValidationMode::Required,
            target,
            program: policy,
            deadline: deadline(),
            description: "Assess exact evidence, including errors. 🦀",
            quality_bar: if program == 0 {
                None
            } else {
                Some("Explain the result with evidence.")
            },
            contributed_by: &contributors,
            policy_revision: 9,
        },
        validation_limits(),
    )
    .unwrap();
    let bytes = plan.construction_charge();
    plan.build(bytes).unwrap()
}
struct TestSink(Vec<u8>);
impl Sink for TestSink {
    fn write(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
    fn visit(&mut self, _: usize) -> Result<(), CodecError> {
        Ok(())
    }
}
fn claim_bytes(value: &claim::ClaimDescriptor) -> Vec<u8> {
    let mut sink = TestSink(Vec::new());
    descriptors::claim(&mut sink, value).unwrap();
    sink.0
}
fn validation_bytes(value: &validation::ValidationDescriptor) -> Vec<u8> {
    let mut sink = TestSink(Vec::new());
    descriptors::validation(&mut sink, value).unwrap();
    sink.0
}
fn declaration_bytes(value: &declaration::Declaration) -> Vec<u8> {
    let mut sink = TestSink(Vec::new());
    descriptors::declaration(&mut sink, value).unwrap();
    sink.0
}

#[test]
fn claim_body_retains_authored_order_nested_checks_and_exact_quotas() {
    let expected = claim_value();
    let bytes = claim_bytes(&expected);
    let mut input = ClaimBodyInput::inspect(&bytes, inspection()).unwrap();
    let parsed = input.parse_quote();
    let plan = input
        .prepare(claim_limits(), usize::MAX, usize::MAX)
        .unwrap();
    assert_eq!(plan.fields().id, expected.id());
    assert_eq!(plan.content_hash(), expected.content_hash());
    assert_eq!(plan.intent_fingerprint(), expected.intent_fingerprint());
    let quote = plan.quote();
    assert_eq!(
        plan.build(quote.bytes, quote.model_build_visits).unwrap(),
        expected
    );
    let mut input = ClaimBodyInput::inspect(
        &bytes,
        BodyInspectionLimits {
            bytes: bytes.len(),
            parse_visits: parsed.parse_visits,
            source_visits: parsed.source_visits,
        },
    )
    .unwrap();
    let plan = input
        .prepare(
            claim_limits(),
            quote.model_inspection_visits,
            quote.source_inspection_visits + quote.source_build_visits,
        )
        .unwrap();
    assert_eq!(plan.quote(), quote);
    assert_eq!(
        plan.build(quote.bytes, quote.model_build_visits).unwrap(),
        expected
    );
}

#[test]
fn all_validation_programs_and_targets_preserve_full_and_specification_identities() {
    for program in 0..4 {
        for target in 0..3 {
            let expected = validation_value(program, target);
            let bytes = validation_bytes(&expected);
            let mut input = ValidationBodyInput::inspect(&bytes, inspection()).unwrap();
            let plan = input
                .prepare(
                    Principal::Actor(ISSUER),
                    validation_limits(),
                    usize::MAX,
                    usize::MAX,
                )
                .unwrap();
            assert_eq!(plan.fields().id, ValidationId(expected.binding().object.0));
            assert_eq!(plan.content_hash(), expected.content_hash());
            assert_eq!(plan.specification_hash(), expected.specification_hash());
            assert_eq!(plan.intent_fingerprint(), expected.intent_fingerprint());
            let quote = plan.quote();
            let built = plan.build(quote.bytes, quote.model_build_visits).unwrap();
            assert_eq!(validation_bytes(&built), bytes);
            assert_eq!(built.intent_fingerprint(), expected.intent_fingerprint());
            assert_eq!(built.specification_hash(), expected.specification_hash());
            let parsed = input.parse_quote();
            let mut input = ValidationBodyInput::inspect(
                &bytes,
                BodyInspectionLimits {
                    bytes: bytes.len(),
                    parse_visits: parsed.parse_visits,
                    source_visits: 0,
                },
            )
            .unwrap();
            let plan = input
                .prepare(
                    Principal::Actor(ISSUER),
                    validation_limits(),
                    quote.model_inspection_visits,
                    quote.source_inspection_visits + quote.source_build_visits,
                )
                .unwrap();
            assert_eq!(plan.quote(), quote);
            let built = plan.build(quote.bytes, quote.model_build_visits).unwrap();
            assert_eq!(validation_bytes(&built), bytes);
            assert_eq!(built.content_hash(), expected.content_hash());
        }
    }
}

#[test]
fn legacy_declarations_keep_binding_stamp_and_handler_policy() {
    for program in 0..4 {
        for target in 0..3 {
            let expected = validation_value(program, target);
            let expected = expected.declaration();
            let bytes = declaration_bytes(expected);
            let mut input = DeclarationBodyInput::inspect(&bytes, inspection()).unwrap();
            let plan = input
                .prepare(
                    Principal::Actor(ISSUER),
                    validation_limits().declaration,
                    usize::MAX,
                    usize::MAX,
                )
                .unwrap();
            let quote = plan.quote();
            assert_eq!(plan.fields().binding, expected.binding());
            assert_eq!(plan.intent_fingerprint(), expected.intent_fingerprint());
            let built = plan.build(quote.bytes, quote.model_build_visits).unwrap();
            assert_eq!(declaration_bytes(&built), bytes);
            assert_eq!(built.intent_fingerprint(), expected.intent_fingerprint());
            let parsed = input.parse_quote();
            let mut input = DeclarationBodyInput::inspect(
                &bytes,
                BodyInspectionLimits {
                    bytes: bytes.len(),
                    parse_visits: parsed.parse_visits,
                    source_visits: 0,
                },
            )
            .unwrap();
            let plan = input
                .prepare(
                    Principal::Actor(ISSUER),
                    validation_limits().declaration,
                    quote.model_inspection_visits,
                    quote.source_inspection_visits + quote.source_build_visits,
                )
                .unwrap();
            let built = plan.build(quote.bytes, quote.model_build_visits).unwrap();
            assert_eq!(declaration_bytes(&built), bytes);
            assert_eq!(built.intent_fingerprint(), expected.intent_fingerprint());
        }
    }
}

#[test]
fn source_budget_must_cover_build_before_any_plan_is_returned() {
    let bytes = claim_bytes(&claim_value());
    let mut claim = ClaimBodyInput::inspect(&bytes, inspection()).unwrap();
    let quote = claim
        .prepare(claim_limits(), usize::MAX, usize::MAX)
        .unwrap()
        .quote();
    assert!(
        claim
            .prepare(
                claim_limits(),
                usize::MAX,
                quote.source_inspection_visits + quote.source_build_visits - 1
            )
            .is_err()
    );
    let value = validation_value(2, 0);
    let bytes = validation_bytes(&value);
    let mut validation = ValidationBodyInput::inspect(&bytes, inspection()).unwrap();
    let quote = validation
        .prepare(
            Principal::Actor(ISSUER),
            validation_limits(),
            usize::MAX,
            usize::MAX,
        )
        .unwrap()
        .quote();
    assert!(
        validation
            .prepare(
                Principal::Actor(ISSUER),
                validation_limits(),
                usize::MAX,
                quote.source_inspection_visits + quote.source_build_visits - 1
            )
            .is_err()
    );
    let bytes = declaration_bytes(value.declaration());
    let mut declaration = DeclarationBodyInput::inspect(&bytes, inspection()).unwrap();
    let quote = declaration
        .prepare(
            Principal::Actor(ISSUER),
            validation_limits().declaration,
            usize::MAX,
            usize::MAX,
        )
        .unwrap()
        .quote();
    assert!(
        declaration
            .prepare(
                Principal::Actor(ISSUER),
                validation_limits().declaration,
                usize::MAX,
                quote.source_inspection_visits + quote.source_build_visits - 1
            )
            .is_err()
    );
}

#[test]
fn byte_and_work_refusal_leave_sources_retryable() {
    let bytes = claim_bytes(&claim_value());
    let mut input = ClaimBodyInput::inspect(&bytes, inspection()).unwrap();
    let quote = input
        .prepare(claim_limits(), usize::MAX, usize::MAX)
        .unwrap()
        .quote();
    assert!(
        input
            .prepare(
                claim_limits(),
                quote.model_inspection_visits - 1,
                usize::MAX
            )
            .is_err()
    );
    let plan = input
        .prepare(claim_limits(), usize::MAX, usize::MAX)
        .unwrap();
    assert!(
        plan.build(quote.bytes - 1, quote.model_build_visits)
            .is_err()
    );
    let plan = input
        .prepare(claim_limits(), usize::MAX, usize::MAX)
        .unwrap();
    assert!(
        plan.build(quote.bytes, quote.model_build_visits - 1)
            .is_err()
    );
    let plan = input
        .prepare(claim_limits(), usize::MAX, usize::MAX)
        .unwrap();
    assert_eq!(
        plan.build(quote.bytes, quote.model_build_visits).unwrap(),
        claim_value()
    );
}

#[test]
fn truncated_trailing_and_semantically_invalid_bodies_refuse() {
    let claim = claim_bytes(&claim_value());
    let value = validation_value(2, 0);
    let validation = validation_bytes(&value);
    let declaration = declaration_bytes(value.declaration());
    for end in 0..claim.len() {
        assert!(ClaimBodyInput::inspect(&claim[..end], inspection()).is_err());
    }
    for end in 0..validation.len() {
        assert!(ValidationBodyInput::inspect(&validation[..end], inspection()).is_err());
    }
    for end in 0..declaration.len() {
        assert!(DeclarationBodyInput::inspect(&declaration[..end], inspection()).is_err());
    }
    let mut trailing = claim.clone();
    trailing.push(0);
    assert!(ClaimBodyInput::inspect(&trailing, inspection()).is_err());
    // A schema-2 header on a schema-1 body is truncated: schema 2 carries a
    // policy section after the deadline. With the section present and
    // absent-marked, the body is a valid schema-2 claim without a policy;
    // an unknown schema is refused at preparation.
    let mut schema = claim;
    schema[48..50].copy_from_slice(&2u16.to_le_bytes());
    assert!(ClaimBodyInput::inspect(&schema, inspection()).is_err());
    schema.push(0);
    let mut input = ClaimBodyInput::inspect(&schema, inspection()).unwrap();
    assert!(
        input
            .prepare(claim_limits(), usize::MAX, usize::MAX)
            .is_ok()
    );
    schema[48..50].copy_from_slice(&3u16.to_le_bytes());
    let mut input = ClaimBodyInput::inspect(&schema, inspection()).unwrap();
    assert!(
        input
            .prepare(claim_limits(), usize::MAX, usize::MAX)
            .is_err()
    );
    let mut input = ValidationBodyInput::inspect(&validation, inspection()).unwrap();
    assert!(
        input
            .prepare(
                Principal::Actor(ParticipantId([99; 16])),
                validation_limits(),
                usize::MAX,
                usize::MAX
            )
            .is_err()
    );
    let mut input = DeclarationBodyInput::inspect(&declaration, inspection()).unwrap();
    assert!(
        input
            .prepare(
                Principal::Actor(ParticipantId([99; 16])),
                validation_limits().declaration,
                usize::MAX,
                usize::MAX
            )
            .is_err()
    );
}

#[test]
fn schema_two_bodies_round_trip_the_policy_and_exact_evidence_after_the_deadline() {
    use focal_model::{ArtifactId, ArtifactRef, Escalation, PeerPolicy};
    let ledger = LedgerId {
        tenant: TenantId([11; 16]),
        session: SessionId([12; 16]),
    };
    let evidence = ArtifactRef {
        id: ArtifactId([40; 16]),
        hash: ContentHash([41; 32]),
    };
    let mut relations = vec![
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(ParticipantId([1; 16])),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(ParticipantId([2; 16])),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(ActionType::Challenge),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId([3; 16])),
        },
        Relation {
            kind: RelationKind::Reviews,
            target: RelationTarget::Evidence(evidence),
        },
    ];
    relations.sort();
    let policy = PeerPolicy {
        corrective_allowed: true,
        max_follow_ups: 2,
        single_issuer: true,
        escalation: Escalation::Holder,
    };
    for policy in [None, Some(policy)] {
        let plan = claim::ClaimDescriptor::prepare(
            claim::ClaimSpec {
                ledger,
                id: ClaimId([20; 16]),
                schema: 2,
                occurrence: OccurrenceId([21; 16]),
                description: "Prove the report was produced by the suite.",
                relations: &relations,
                scopes: &[],
                requirements: &[],
                slots: &[],
                deadline: None,
                policy,
            },
            claim_limits(),
        )
        .unwrap();
        let charge = plan.construction_charge();
        let expected = plan.build(charge).unwrap();
        let bytes = claim_bytes(&expected);
        // The policy section is the trailer: presence byte, then the fields.
        match policy {
            None => assert_eq!(bytes.last(), Some(&0)),
            Some(_) => assert_eq!(&bytes[bytes.len() - 6..], &[1, 1, 2, 0, 1, 1]),
        }
        let mut input = ClaimBodyInput::inspect(&bytes, inspection()).unwrap();
        let plan = input
            .prepare(claim_limits(), usize::MAX, usize::MAX)
            .unwrap();
        assert_eq!(plan.fields().policy, policy);
        let quote = plan.quote();
        let built = plan.build(quote.bytes, quote.model_build_visits).unwrap();
        assert_eq!(built, expected);
        assert_eq!(built.policy(), policy);
        assert!(built.relations().iter().any(|relation| {
            relation.kind == RelationKind::Reviews
                && relation.target == RelationTarget::Evidence(evidence)
        }));
        // Without the trailer the schema-2 body is truncated; the same body
        // declared as schema 1 refuses the evidence target.
        let truncated = &bytes[..bytes.len() - if policy.is_some() { 6 } else { 1 }];
        assert!(ClaimBodyInput::inspect(truncated, inspection()).is_err());
    }
    let mut legacy = relations.clone();
    legacy.retain(|relation| !matches!(relation.target, RelationTarget::Evidence(_)));
    assert!(
        claim::ClaimDescriptor::prepare(
            claim::ClaimSpec {
                ledger,
                id: ClaimId([20; 16]),
                schema: 1,
                occurrence: OccurrenceId([21; 16]),
                description: "Prove the report was produced by the suite.",
                relations: &relations,
                scopes: &[],
                requirements: &[],
                slots: &[],
                deadline: None,
                policy: None,
            },
            claim_limits(),
        )
        .is_err()
    );
}
