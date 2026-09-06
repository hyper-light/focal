//! Execution qualification uses the original admitted mixed history. The broad
//! synthetic codec corpora are deliberately not treated as reducer histories.
use crate::*;

const BASE: &[u8] = include_bytes!("../fixtures/durable-v1/base.cp1");
const OPEN: &[u8] = include_bytes!("../fixtures/durable-v1/open-evidence.cp1");
const CLOSED: &[u8] = include_bytes!("../fixtures/durable-v1/closed-evidence.cp1");
const FINAL: &[u8] = include_bytes!("../fixtures/durable-v1/final.cp1");
const BYTES: usize = 64 * 1024 * 1024;

macro_rules! historical {
    ($number:literal) => {
        (
            include_bytes!(concat!("../fixtures/durable-v1/", $number, ".entry")).as_slice(),
            include_bytes!(concat!("../fixtures/durable-v1/", $number, ".result")).as_slice(),
        )
    };
}
const HISTORY: &[(&[u8], &[u8])] = &[
    historical!("00"),
    historical!("01"),
    historical!("02"),
    historical!("03"),
    historical!("04"),
    historical!("05"),
    historical!("06"),
    historical!("07"),
    historical!("08"),
];

#[derive(Clone, Copy, Debug)]
enum Route {
    Direct,
    Epoch,
    Fallback,
}

fn limits() -> EpochLimits {
    EpochLimits {
        max_commands: 16,
        max_workers: 4,
        max_edges: 1024,
        max_bytes: BYTES,
        max_trace_entries: 4096,
        worker_stack_bytes: 512 * 1024,
    }
}

fn execute_epoch(
    core: &mut Core,
    inputs: Vec<PreparedMutation>,
    fallback: bool,
) -> Vec<ApplyResult> {
    let before = core.encode_checkpoint().unwrap();
    let mut plan = core.plan_epoch(inputs, limits()).unwrap();
    if fallback {
        // Omit real reads to force the executor's audit to discard speculation.
        for index in 0..plan.len() {
            let mut declaration = plan.accesses(index).unwrap().clone();
            declaration.reads.clear();
            plan.declare(index, declaration).unwrap();
        }
    }
    let output = plan.execute(core).unwrap();
    assert_eq!(output.report().serial_fallback, fallback);
    core.validate_epoch(&output).unwrap();
    assert_eq!(core.encode_checkpoint().unwrap(), before);
    core.publish_epoch(output).unwrap()
}

fn legacy_apply(core: &mut Core, prepared: PreparedMutation, route: Route) -> ApplyResult {
    match route {
        Route::Direct => core
            .apply(SessionSeq(core.sequence().0 + 1), prepared)
            .unwrap(),
        Route::Epoch | Route::Fallback => {
            let mut results = execute_epoch(core, vec![prepared], matches!(route, Route::Fallback));
            assert_eq!(results.len(), 1);
            results.pop().unwrap()
        }
    }
}

fn check_prefix(core: &Core, index: usize) {
    match index {
        4 => assert_eq!(core.encode_checkpoint().unwrap(), OPEN),
        5 => assert_eq!(core.encode_checkpoint().unwrap(), CLOSED),
        8 => assert_eq!(core.encode_checkpoint().unwrap(), FINAL),
        _ => {}
    }
}

fn legacy_input(entry: &[u8]) -> AuthenticatedInput {
    if let Some(body) = entry.strip_prefix(b"FOCALOP1") {
        PreparedMutation::decode_v1(body).unwrap().input
    } else {
        let input = PreparedManagedMutation::decode_v1(entry.strip_prefix(b"FOCALMD1").unwrap())
            .unwrap()
            .input;
        AuthenticatedInput {
            ledger: input.key.stream.ledger,
            principal: input.key.stream.principal,
            request_epoch: RequestEpoch(1),
            request_id: input.key.id,
            expected_revision: input.expected_revision,
            authority: input.authority,
            command: input.command,
        }
    }
}

fn managed_input(input: &AuthenticatedInput, ordinal: u64) -> ManagedAuthenticatedInput {
    ManagedAuthenticatedInput {
        key: ManagedRequestKey {
            stream: RequestStreamIdentity {
                cluster: [9; 16],
                ledger: input.ledger,
                principal: input.principal,
                slot: 0,
                generation: 1,
            },
            ordinal,
            id: input.request_id,
        },
        expected_revision: input.expected_revision,
        authority: input.authority.clone(),
        command: input.command.clone(),
    }
}

fn recorded(core: &Core, input: AuthenticatedInput) -> PreparedMutation {
    PreparedMutation {
        schema: 1,
        base: core.sequence(),
        command_hash: command_hash(&input).unwrap(),
        footprint: Footprint {
            ledger: input.ledger,
            session_exclusive: true,
        },
        input,
    }
}

#[test]
fn original_history_preserves_results_and_checkpoints_through_each_execution_route() {
    for route in [Route::Direct, Route::Epoch, Route::Fallback] {
        let mut core = Core::decode_checkpoint(BASE).unwrap();
        for (index, (entry, expected)) in HISTORY.iter().enumerate() {
            if let Some(body) = entry.strip_prefix(b"FOCALOP1") {
                let prepared = PreparedMutation::decode_v1(body).unwrap();
                let result = legacy_apply(&mut core, prepared, route);
                assert_eq!(
                    postcard::to_stdvec(&result).unwrap(),
                    *expected,
                    "{route:?}, entry {index}"
                );
            } else {
                let prepared =
                    PreparedManagedMutation::decode_v1(entry.strip_prefix(b"FOCALMD1").unwrap())
                        .unwrap();
                let before = core.encode_checkpoint().unwrap();
                let pending = PendingState::new();
                let admitted = core
                    .stage_managed_pending_bounded(&pending, &prepared.input, BYTES)
                    .unwrap();
                assert_eq!(admitted.prepared(), &prepared);
                assert_eq!(postcard::to_stdvec(admitted.result()).unwrap(), *expected);
                core.audit_managed_pending_stage(&pending, &admitted, BYTES)
                    .unwrap();
                let replay = core.replay_managed_bounded(&prepared, BYTES).unwrap();
                assert_eq!(admitted.result(), replay.result());
                core.audit_managed_pending_stage(&pending, &replay, BYTES)
                    .unwrap();
                core.validate_managed(&replay).unwrap();
                assert_eq!(core.encode_checkpoint().unwrap(), before);
                let result = core.publish_managed(replay).unwrap();
                assert_eq!(postcard::to_stdvec(&result).unwrap(), *expected);
            }
            check_prefix(&core, index);
            core = Core::decode_checkpoint(&core.encode_checkpoint().unwrap()).unwrap();
        }
    }
}

#[test]
fn original_history_stages_one_owned_pending_prefix_then_replays_identically() {
    let mut core = Core::decode_checkpoint(BASE).unwrap();
    let mut pending = PendingState::new();
    pending.reserve(HISTORY.len()).unwrap();
    for (index, (entry, expected)) in HISTORY.iter().enumerate() {
        if let Some(body) = entry.strip_prefix(b"FOCALOP1") {
            let prepared = PreparedMutation::decode_v1(body).unwrap();
            let staged = core
                .stage_pending_bounded(&pending, &prepared.input, BYTES)
                .unwrap();
            assert_eq!(staged.prepared(), &prepared);
            core.audit_pending_stage(&pending, &staged, limits())
                .unwrap();
            let (actual, result) = pending.accept(&core, staged).unwrap();
            assert_eq!(actual, prepared);
            assert_eq!(postcard::to_stdvec(&result).unwrap(), *expected);
        } else {
            let prepared =
                PreparedManagedMutation::decode_v1(entry.strip_prefix(b"FOCALMD1").unwrap())
                    .unwrap();
            let staged = core
                .stage_managed_pending_bounded(&pending, &prepared.input, BYTES)
                .unwrap();
            assert_eq!(staged.prepared(), &prepared);
            core.audit_managed_pending_stage(&pending, &staged, BYTES)
                .unwrap();
            let (actual, result) = pending.accept_managed(&core, staged).unwrap();
            assert_eq!(actual, prepared);
            assert_eq!(postcard::to_stdvec(&result).unwrap(), *expected);
        }
        assert_eq!(pending.len(), index + 1);
        assert_eq!(core.encode_checkpoint().unwrap(), BASE);
    }
    assert_eq!(
        pending
            .view(&core)
            .unwrap()
            .claim(&ClaimId::from_u128(100))
            .unwrap()
            .lifecycle()
            .status,
        ClaimStatus::Satisfied
    );
    for (index, (entry, expected)) in HISTORY.iter().enumerate() {
        if let Some(body) = entry.strip_prefix(b"FOCALOP1") {
            let result = legacy_apply(
                &mut core,
                PreparedMutation::decode_v1(body).unwrap(),
                Route::Epoch,
            );
            assert_eq!(postcard::to_stdvec(&result).unwrap(), *expected);
        } else {
            let prepared =
                PreparedManagedMutation::decode_v1(entry.strip_prefix(b"FOCALMD1").unwrap())
                    .unwrap();
            let replay = core.replay_managed_bounded(&prepared, BYTES).unwrap();
            let result = core.publish_managed(replay).unwrap();
            assert_eq!(postcard::to_stdvec(&result).unwrap(), *expected);
        }
        pending.drop_prefix(1, &core).unwrap();
        check_prefix(&core, index);
    }
    assert!(pending.is_empty());
}

#[test]
fn stronger_historical_receipt_replays_without_reentering_current_admission() {
    // Differential extension of the real workflow, matching the already tested
    // historical Receipt behavior. This is not a new original-writer fixture.
    let mut inputs: Vec<_> = HISTORY
        .iter()
        .map(|(entry, _)| legacy_input(entry))
        .collect();
    let Command::GenerateClaim { claim } = &mut inputs[0].command else {
        panic!("original generate")
    };
    let receipt = &mut claim.validations[0].content;
    receipt.quality_bar = Some("historically ignored quality".into());
    receipt.handlers = vec![
        HandlerRef {
            id: ValidatorId::from_u128(1),
            version: ContentHash([1; 32]),
            agentic: false,
        },
        HandlerRef {
            id: ValidatorId::from_u128(2),
            version: ContentHash([2; 32]),
            agentic: true,
        },
    ];
    receipt.evidence_schemas.insert(ContentHash([9; 32]));
    claim.content.requirements[0].specification = receipt.specification_hash().unwrap();
    let mut direct = Core::decode_checkpoint(BASE).unwrap();
    let mut managed = Core::decode_checkpoint(BASE).unwrap();
    let original_receipts = direct.snapshot().receipts.clone();
    let mut records = Vec::new();
    let mut expected = Vec::new();
    for (index, input) in inputs.into_iter().enumerate() {
        let scoped = managed_input(&input, index as u64 + 1);
        if matches!(
            input.command,
            Command::GenerateClaim { .. }
                | Command::AcknowledgeTestament { .. }
                | Command::BeginWholeWorkValidation { .. }
                | Command::CompleteWholeWork { .. }
        ) {
            assert!(matches!(
                direct.prepare(&input),
                Err(DomainOutcome::Refuse {
                    code: ErrorCode::InvalidSchema,
                    ..
                })
            ));
            assert!(matches!(
                managed.stage_managed_pending_bounded(&PendingState::new(), &scoped, BYTES),
                Err(StagingError::Domain(DomainOutcome::Refuse {
                    code: ErrorCode::InvalidSchema,
                    ..
                }))
            ));
        }
        let prepared = recorded(&direct, input);
        records.push(prepared.clone());
        let result = legacy_apply(&mut direct, prepared, Route::Direct);
        assert!(
            !result
                .effects
                .iter()
                .any(|effect| matches!(effect, EffectIntent::ExecuteValidation { .. }))
        );
        let prepared = PreparedManagedMutation {
            schema: 1,
            base: managed.sequence(),
            command_hash: managed_command_hash(&scoped).unwrap(),
            footprint: Footprint {
                ledger: scoped.key.stream.ledger,
                session_exclusive: true,
            },
            input: scoped,
        };
        let replay = managed.replay_managed_bounded(&prepared, BYTES).unwrap();
        managed
            .audit_managed_pending_stage(&PendingState::new(), &replay, BYTES)
            .unwrap();
        let actual = managed.publish_managed(replay).unwrap();
        assert_eq!(actual.outcome, result.receipt.outcome);
        assert_eq!(actual.deltas, result.deltas);
        assert_eq!(actual.effects, result.effects);
        let mut state = direct.snapshot().clone();
        state.receipts = original_receipts.clone();
        assert_eq!(&state, managed.snapshot());
        expected.push(result);
        direct = Core::decode_checkpoint(&direct.encode_checkpoint().unwrap()).unwrap();
        managed = Core::decode_checkpoint(&managed.encode_checkpoint().unwrap()).unwrap();
    }
    for fallback in [false, true] {
        let mut epoch = Core::decode_checkpoint(BASE).unwrap();
        assert_eq!(
            execute_epoch(&mut epoch, records.clone(), fallback),
            expected
        );
        assert_eq!(
            epoch.encode_checkpoint().unwrap(),
            direct.encode_checkpoint().unwrap()
        );
    }
    for core in [&direct, &managed] {
        assert_eq!(
            core.snapshot().claims[&ClaimId::from_u128(100)]
                .lifecycle()
                .status,
            ClaimStatus::Satisfied
        );
    }
}

#[test]
fn unknown_prepared_schemas_reject_direct_epoch_and_managed_execution_without_publication() {
    for schema in [0, 2, u16::MAX] {
        let mut core = Core::decode_checkpoint(BASE).unwrap();
        let mut prepared =
            PreparedMutation::decode_v1(HISTORY[0].0.strip_prefix(b"FOCALOP1").unwrap()).unwrap();
        prepared.schema = schema;
        prepared.base = SessionSeq(u64::MAX);
        prepared.command_hash = ContentHash([0; 32]);
        assert!(
            matches!(core.apply(SessionSeq(u64::MAX), prepared.clone()), Err(CoreError::UnsupportedSchema(found)) if found == schema)
        );
        assert!(
            matches!(core.apply_serial(SessionSeq(u64::MAX), prepared.clone()), Err(CoreError::UnsupportedSchema(found)) if found == schema)
        );
        assert!(
            matches!(core.apply_tracked(SessionSeq(u64::MAX), prepared.clone(), 4096).result, Err(CoreError::UnsupportedSchema(found)) if found == schema)
        );
        let first =
            PreparedMutation::decode_v1(HISTORY[0].0.strip_prefix(b"FOCALOP1").unwrap()).unwrap();
        assert!(
            matches!(core.plan_epoch(vec![first, prepared], limits()), Err(EpochError::Core(CoreError::UnsupportedSchema(found))) if found == schema)
        );
        let mut prepared =
            PreparedManagedMutation::decode_v1(HISTORY[1].0.strip_prefix(b"FOCALMD1").unwrap())
                .unwrap();
        prepared.schema = schema;
        prepared.base = SessionSeq(u64::MAX);
        prepared.command_hash = ContentHash([0; 32]);
        assert!(
            matches!(core.replay_managed_bounded(&prepared, 1), Err(CoreError::UnsupportedSchema(found)) if found == schema)
        );
        assert_eq!(core.encode_checkpoint().unwrap(), BASE);
    }
}

#[test]
fn unknown_legacy_candidate_schema_rejects_pending_acceptance_and_audit() {
    let core = Core::decode_checkpoint(BASE).unwrap();
    let input = legacy_input(HISTORY[0].0);
    let mut pending = PendingState::new();
    pending.reserve(1).unwrap();
    for schema in [0, 2, u16::MAX] {
        let mut staged = core.stage_pending_bounded(&pending, &input, BYTES).unwrap();
        staged.prepared.schema = schema;
        assert!(
            matches!(pending.validate_next(&core, &staged), Err(CoreError::UnsupportedSchema(found)) if found == schema)
        );
        assert!(
            matches!(core.audit_pending_stage(&pending, &staged, limits()), Err(EpochError::Core(CoreError::UnsupportedSchema(found))) if found == schema)
        );
        assert!(
            matches!(pending.accept(&core, staged), Err(CoreError::UnsupportedSchema(found)) if found == schema)
        );
        assert!(pending.is_empty());
        assert_eq!(core.encode_checkpoint().unwrap(), BASE);
    }
}

#[test]
fn historical_candidates_cannot_publish_against_changed_owner_limits() {
    let core = Core::decode_checkpoint(BASE).unwrap();
    let mut foreign = Core::decode_checkpoint(BASE).unwrap();
    foreign.limits.max_objects -= 1;
    let before = foreign.encode_checkpoint().unwrap();
    let input = legacy_input(HISTORY[0].0);
    let mut pending = PendingState::new();
    pending.reserve(1).unwrap();
    let staged = core.stage_pending_bounded(&pending, &input, BYTES).unwrap();
    assert!(matches!(
        pending.validate_next(&foreign, &staged),
        Err(CoreError::StalePreparation)
    ));
    assert!(
        core.audit_pending_stage(&pending, &staged, limits())
            .is_ok()
    );
    assert!(
        foreign
            .audit_pending_stage(&pending, &staged, limits())
            .is_err()
    );
    let prepared = staged.prepared().clone();
    assert!(pending.accept(&foreign, staged).is_err());
    assert!(pending.is_empty());
    let output = core
        .plan_epoch(vec![prepared], limits())
        .unwrap()
        .execute(&core)
        .unwrap();
    assert!(matches!(
        foreign.publish_epoch(output),
        Err(EpochError::Provenance)
    ));
    let scoped = managed_input(&input, 1);
    let staged = core
        .stage_managed_pending_bounded(&pending, &scoped, BYTES)
        .unwrap();
    assert!(
        foreign
            .audit_managed_pending_stage(&pending, &staged, BYTES)
            .is_err()
    );
    assert!(foreign.validate_managed(&staged).is_err());
    assert!(foreign.publish_managed(staged).is_err());
    assert_eq!(foreign.encode_checkpoint().unwrap(), before);
    assert_eq!(core.encode_checkpoint().unwrap(), BASE);
}
