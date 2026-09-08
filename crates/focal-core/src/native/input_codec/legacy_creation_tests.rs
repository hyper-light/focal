use super::*;
use focal_model::{
    HandlerRef, RequestEpoch, RequestId, SessionId, TenantId, TimerId, ValidationKind,
    ValidationMode, ValidationPhase, ValidatorId,
};

const ISSUER: ParticipantId = ParticipantId([1; 16]);
const HANDLER: HandlerRef = HandlerRef {
    id: ValidatorId([2; 16]),
    version: ContentHash([3; 32]),
    agentic: false,
};
const AGENT: HandlerRef = HandlerRef {
    id: ValidatorId([4; 16]),
    version: ContentHash([5; 32]),
    agentic: true,
};
const PROGRAM: [validation::HandlerPolicy<'static>; 1] = [validation::HandlerPolicy {
    handler: &HANDLER,
    attempts: 2,
    proof_schema: ContentHash([6; 32]),
    diagnostic_schema: ContentHash([7; 32]),
}];
const QUALITY: [validation::HandlerPolicy<'static>; 1] = [validation::HandlerPolicy {
    handler: &AGENT,
    attempts: 3,
    proof_schema: ContentHash([8; 32]),
    diagnostic_schema: ContentHash([9; 32]),
}];
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId([10; 16]),
        session: SessionId([11; 16]),
    }
}
fn binding(id: u128) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([12; 32]),
        revision: ObjectRevision(1),
    }
}
fn work() -> LegacyCreationWork {
    LegacyCreationWork {
        parsing: 100_000_000,
        source: 100_000_000,
        declarations: 100_000_000,
        acceptance: 100_000_000,
        structure: 100_000_000,
    }
}
fn native() -> NativeLimits {
    NativeLimits {
        plan_nodes: 8,
        plan_edges: 256,
        preparation_bytes: 4 * 1024 * 1024,
        ..NativeLimits::default()
    }
}
fn limits() -> LegacyCreationLimits {
    LegacyCreationLimits {
        declaration: validation::Limits {
            handlers: 8,
            attempts: 16,
            slot_bytes: 128,
        },
        acceptance: aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 16,
            max_updates: 16,
        },
        bytes: 4 * 1024 * 1024,
        work: work(),
    }
}
fn phase(agent: bool) -> validation::PhasePolicy<'static> {
    validation::PhasePolicy {
        evaluator: ISSUER,
        definition: ContentHash([13; 32]),
        required_policy: Some(ContentHash([14; 32])),
        handlers: if agent { &QUALITY } else { &PROGRAM },
    }
}
fn definition(claim: u128, index: u32) -> validation::Declaration {
    let (target, declared, kind, program) = match index {
        0 => (
            validation::TargetDeclaration::Delivery,
            ValidationPhase::WholeWork,
            ValidationKind::Receipt,
            validation::Program::Delivery,
        ),
        1 => (
            validation::TargetDeclaration::WholeWorkSlot {
                index: 2,
                name: "résultat",
            },
            ValidationPhase::WholeWork,
            ValidationKind::Inspection,
            validation::Program::Programmatic {
                check: phase(false),
                quality: Some(phase(true)),
            },
        ),
        2 => (
            validation::TargetDeclaration::Admission,
            ValidationPhase::Admission,
            ValidationKind::Contract,
            validation::Program::Agentic { check: phase(true) },
        ),
        _ => (
            validation::TargetDeclaration::Increment,
            ValidationPhase::Increment,
            ValidationKind::Test,
            validation::Program::Programmatic {
                check: phase(false),
                quality: None,
            },
        ),
    };
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: binding(100 + claim * 10 + u128::from(index)),
            claim: ClaimId::from_u128(claim),
            issuer: ISSUER,
            declaration_index: index,
            kind,
            phase: declared,
            mode: ValidationMode::Required,
            target,
            program,
            deadline: Deadline {
                timer: TimerId::from_u128(500 + claim * 10 + u128::from(index)),
                generation: 2,
                at: 1000,
            },
        },
        limits().declaration,
    )
    .unwrap()
}
fn proposal(id: u128) -> (creation::Proposal, Vec<validation::Declaration>) {
    let declarations: Vec<_> = [3, 0, 2, 1]
        .into_iter()
        .map(|index| definition(id, index))
        .collect();
    let checks = [aggregation::CheckPolicy {
        declaration_index: 1,
        validation: ValidationId::from_u128(101 + id * 10),
        mode: ValidationMode::Required,
    }];
    let slots = [
        aggregation::SlotPolicy {
            slot: 2,
            missing_declaration_index: 10,
            mode: ValidationMode::Required,
            checks: &checks,
        },
        aggregation::SlotPolicy {
            slot: 3,
            missing_declaration_index: 11,
            mode: ValidationMode::Observe,
            checks: &[],
        },
    ];
    let acceptance = aggregation::AcceptancePolicy::new(
        binding(id),
        ISSUER,
        &slots,
        &declarations,
        limits().acceptance,
    )
    .unwrap();
    let graph = graph::Declaration::new(
        &[graph::Obligation {
            kind: if id == 1 {
                graph::Kind::DependsOn
            } else {
                graph::Kind::Awaits
            },
            target: ClaimId::from_u128(if id == 1 { 2 } else { 1 }),
        }],
        4,
    )
    .unwrap();
    let lineage = if id == 1 {
        succession::Lineage::root(binding(id), RootCommandId::from_u128(90)).unwrap()
    } else {
        succession::Lineage::new(
            binding(id),
            Cause::Claim(ClaimId::from_u128(1)),
            &[
                succession::Correction {
                    kind: succession::CorrectionKind::Supersedes,
                    predecessor: ObjectRef::claim(ledger(), ClaimId::from_u128(700)),
                },
                succession::Correction {
                    kind: succession::CorrectionKind::Amends,
                    predecessor: ObjectRef::claim(ledger(), ClaimId::from_u128(701)),
                },
            ],
            4,
        )
        .unwrap()
    };
    (
        creation::Proposal {
            definition: ClaimDefinition {
                binding: binding(id),
                issuer: ISSUER,
                subject: ParticipantId([20; 16]),
                deadline: Some(Deadline {
                    timer: TimerId::from_u128(800 + id),
                    generation: 1,
                    at: 2000,
                }),
                max_responses: 3,
                created: SessionSeq(42),
                graph,
                lineage,
                acceptance,
                scope_limits: scope::ScopeLimits {
                    scopes: 3,
                    roots: 4,
                    children: 5,
                },
            },
            owner: if id == 1 {
                None
            } else {
                Some(creation::Owner {
                    expected: binding(1),
                    receipt: Some(ReceiptFence {
                        receipt: ReceiptId([21; 16]),
                        epoch: 3,
                    }),
                })
            },
        },
        declarations,
    )
}
fn input() -> NativeInput {
    let (first, mut declarations) = proposal(1);
    let (second, other) = proposal(2);
    declarations.extend(other);
    declarations.swap(0, 7);
    NativeInput {
        request: RequestKey {
            principal: ISSUER,
            epoch: RequestEpoch(2),
            id: RequestId([22; 16]),
        },
        command: NativeCommand::Create {
            claims: vec![second, first],
            declarations,
        },
    }
}
fn encode(input: &NativeInput) -> Vec<u8> {
    let plan = EncodingPlan::prepare(
        InputFrame::Request {
            ledger: ledger(),
            profile: NativeContentProfile::ProjectionOnly,
            input,
        },
        EncodingLimits {
            bytes: usize::MAX,
            visits: usize::MAX,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}
fn inspect(bytes: &[u8]) -> StructuralInput<'_> {
    StructuralInput::inspect(
        bytes,
        InspectionLimits {
            bytes: bytes.len(),
            visits: usize::MAX,
            items: usize::MAX,
            text_bytes: usize::MAX,
            blob_bytes: usize::MAX,
        },
    )
    .unwrap()
}

#[test]
fn complete_legacy_frame_reconstructs_global_cohort_and_preserves_original_native_intent() {
    let original = input();
    let bytes = encode(&original);
    let structural = inspect(&bytes);
    let plan = structural
        .prepare_legacy_creation(native(), limits())
        .unwrap()
        .unwrap();
    assert_eq!(plan.header(), structural.header());
    assert_eq!(
        plan.intent_fingerprint(),
        crate::native::intent::fingerprint(ledger(), &original).unwrap()
    );
    let quote = plan.quote();
    let built = plan.build(quote.bytes, quote.construction).unwrap();
    assert_eq!(encode(&built), bytes);
    assert_eq!(
        crate::native::intent::fingerprint(ledger(), &built).unwrap(),
        crate::native::intent::fingerprint(ledger(), &original).unwrap()
    );
    let NativeCommand::Create {
        claims,
        declarations,
    } = &built.command
    else {
        panic!("wrong command")
    };
    assert_eq!(claims.len(), 2);
    assert_eq!(declarations.len(), 8);
    assert!(
        claims
            .iter()
            .all(|claim| claim.definition.created == SessionSeq(0))
    );
    assert_eq!(claims[0].definition.lineage.corrections().len(), 2);
    assert_eq!(claims[0].owner.unwrap().receipt.unwrap().epoch, 3);
    for claim in claims {
        assert_eq!(
            claim
                .definition
                .acceptance
                .slots()
                .nth(1)
                .unwrap()
                .checks
                .len(),
            0
        );
        claim
            .definition
            .acceptance
            .check_declarations(
                declarations
                    .iter()
                    .filter(|value| value.claim().0 == claim.definition.binding.object.0),
            )
            .unwrap();
    }
    let exact = structural
        .prepare_legacy_creation(
            native(),
            LegacyCreationLimits {
                bytes: quote.bytes,
                work: quote.preparation,
                ..limits()
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(exact.quote(), quote);
    assert_eq!(
        encode(&exact.build(quote.bytes, quote.construction).unwrap()),
        bytes
    );
}

#[test]
fn each_cumulative_domain_and_final_byte_boundary_refuses_then_allows_exact_retry() {
    let bytes = encode(&input());
    let structural = inspect(&bytes);
    let quote = structural
        .prepare_legacy_creation(native(), limits())
        .unwrap()
        .unwrap()
        .quote();
    for index in 0..5 {
        let reduce = |mut value: LegacyCreationWork| {
            let count = match index {
                0 => &mut value.parsing,
                1 => &mut value.source,
                2 => &mut value.declarations,
                3 => &mut value.acceptance,
                _ => &mut value.structure,
            };
            assert!(*count > 0);
            *count -= 1;
            value
        };
        assert!(
            structural
                .prepare_legacy_creation(
                    native(),
                    LegacyCreationLimits {
                        work: reduce(quote.preparation),
                        ..limits()
                    }
                )
                .is_err(),
            "prepare domain {index}"
        );
        let plan = structural
            .prepare_legacy_creation(native(), limits())
            .unwrap()
            .unwrap();
        assert!(
            plan.build(quote.bytes, reduce(quote.construction)).is_err(),
            "build domain {index}"
        );
    }
    assert!(
        structural
            .prepare_legacy_creation(
                native(),
                LegacyCreationLimits {
                    bytes: quote.bytes - 1,
                    ..limits()
                }
            )
            .is_err()
    );
    let plan = structural
        .prepare_legacy_creation(native(), limits())
        .unwrap()
        .unwrap();
    assert!(plan.build(quote.bytes - 1, quote.construction).is_err());
    let plan = structural
        .prepare_legacy_creation(
            native(),
            LegacyCreationLimits {
                bytes: quote.bytes,
                work: quote.preparation,
                ..limits()
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        encode(&plan.build(quote.bytes, quote.construction).unwrap()),
        bytes
    );
}

fn offset(bytes: &[u8], child: &[u8]) -> usize {
    (child.as_ptr() as usize)
        .checked_sub(bytes.as_ptr() as usize)
        .unwrap()
}
#[test]
fn local_projection_and_global_definition_errors_refuse_before_owned_construction() {
    let original = encode(&input());
    let structural = inspect(&original);
    let work = Work::new(work());
    let frame = fields::Frame::read(&original, structural.header(), native(), &work).unwrap();
    let first = fields::Projections::new(frame.projections, &work)
        .next()
        .unwrap()
        .unwrap();
    let proposal = offset(&original, frame.projections.bytes);
    let obligation = offset(&original, first.obligations.bytes);
    let correction = offset(&original, first.corrections.bytes);
    let declaration = offset(&original, frame.declarations.bytes);
    let mut cases = Vec::new();
    let mut stale = original.clone();
    stale[proposal + 80..proposal + 88].copy_from_slice(&2u64.to_le_bytes());
    cases.push(stale);
    let mut zero_target = original.clone();
    zero_target[obligation + 1..obligation + 17].fill(0);
    cases.push(zero_target);
    let mut wrong_kind = original.clone();
    wrong_kind[correction + 33..correction + 35].copy_from_slice(&2u16.to_le_bytes());
    cases.push(wrong_kind);
    let mut orphan = original.clone();
    orphan[declaration + 88..declaration + 104].copy_from_slice(&999u128.to_be_bytes());
    cases.push(orphan);
    let mut wrong_actor = original.clone();
    wrong_actor[44..60].copy_from_slice(&[99; 16]);
    cases.push(wrong_actor);
    for bytes in cases {
        let inspected = inspect(&bytes);
        assert!(
            inspected
                .prepare_legacy_creation(native(), limits())
                .is_err()
        );
    }
    let plan = structural
        .prepare_legacy_creation(native(), limits())
        .unwrap()
        .unwrap();
    let quote = plan.quote();
    assert_eq!(
        encode(&plan.build(quote.bytes, quote.construction).unwrap()),
        original
    );
}

#[test]
fn streaming_creation_writer_refuses_incomplete_or_extra_values_without_changing_identity() {
    let (proposal, _) = proposal(2);
    let value = &proposal.definition;
    let fields = creation::ProposalIntentFields {
        binding: value.binding,
        issuer: value.issuer,
        subject: value.subject,
        deadline: value.deadline,
        max_responses: value.max_responses,
        lineage_binding: value.lineage.binding(),
        cause: value.lineage.cause(),
        acceptance: value.acceptance.intent_fingerprint(),
        scope_limits: value.scope_limits,
        owner: proposal.owner,
    };
    let mut writer = creation::CreationIntent::new(1).unwrap();
    assert!(
        writer
            .push(&fields, 1, std::iter::empty(), 0, std::iter::empty())
            .is_err()
    );
    assert!(
        writer
            .push(
                &fields,
                0,
                value.graph.obligations().iter().copied().map(Ok),
                0,
                std::iter::empty()
            )
            .is_err()
    );
    writer
        .push(
            &fields,
            value.graph.obligations().len(),
            value.graph.obligations().iter().copied().map(Ok),
            value.lineage.corrections().len(),
            value.lineage.corrections().iter().copied().map(Ok),
        )
        .unwrap();
    assert_eq!(
        writer.finish().unwrap(),
        creation::intent_fingerprint(&[proposal]).unwrap()
    );
    assert!(creation::CreationIntent::new(1).unwrap().finish().is_err());
}

#[test]
fn graph_terminal_probe_requires_its_own_budget_before_calling_the_source() {
    let calls = std::cell::Cell::new(0);
    let empty = || {
        std::iter::from_fn(|| {
            calls.set(calls.get() + 1);
            None::<Result<graph::Obligation, ContractError>>
        })
    };
    assert_eq!(
        graph::Declaration::check_sorted_values(empty(), 0, 0, &mut graph::VisitBudget::new(0)),
        Err(ContractError::Capacity)
    );
    assert_eq!(calls.get(), 0);
    graph::Declaration::check_sorted_values(empty(), 0, 0, &mut graph::VisitBudget::new(1))
        .unwrap();
    assert_eq!(calls.get(), 1);

    calls.set(0);
    let one = std::iter::from_fn(|| {
        let before = calls.get();
        calls.set(before + 1);
        (before == 0).then_some(Ok(graph::Obligation {
            kind: graph::Kind::DependsOn,
            target: ClaimId::from_u128(2),
        }))
    });
    assert_eq!(
        graph::Declaration::check_sorted_values(one, 1, 1, &mut graph::VisitBudget::new(1)),
        Err(ContractError::Capacity)
    );
    assert_eq!(calls.get(), 1);
}

#[test]
fn lineage_terminal_probe_cannot_reuse_the_header_or_last_value_budget() {
    let calls = std::cell::Cell::new(0);
    let cause = Cause::Root(RootCommandId::from_u128(90));
    let empty = || {
        std::iter::from_fn(|| {
            calls.set(calls.get() + 1);
            None::<Result<succession::Correction, ContractError>>
        })
    };
    assert_eq!(
        succession::Lineage::check_sorted_values(
            binding(1),
            &cause,
            empty(),
            0,
            0,
            &mut graph::VisitBudget::new(1)
        ),
        Err(ContractError::Capacity)
    );
    assert_eq!(calls.get(), 0);
    succession::Lineage::check_sorted_values(
        binding(1),
        &cause,
        empty(),
        0,
        0,
        &mut graph::VisitBudget::new(2),
    )
    .unwrap();
    assert_eq!(calls.get(), 1);

    calls.set(0);
    let one = std::iter::from_fn(|| {
        let before = calls.get();
        calls.set(before + 1);
        (before == 0).then_some(Ok(succession::Correction {
            kind: succession::CorrectionKind::Amends,
            predecessor: ObjectRef::claim(ledger(), ClaimId::from_u128(2)),
        }))
    });
    assert_eq!(
        succession::Lineage::check_sorted_values(
            binding(1),
            &cause,
            one,
            1,
            1,
            &mut graph::VisitBudget::new(2)
        ),
        Err(ContractError::Capacity)
    );
    assert_eq!(calls.get(), 1);
}
