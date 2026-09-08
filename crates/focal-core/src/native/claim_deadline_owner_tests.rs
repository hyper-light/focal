use super::*;
use focal_model::lifecycle::{claim::ClaimTerminalCut, graph};
use focal_model::{ClaimStatus, Confidence, OutcomeKind, SessionSeq};

#[path = "claim_deadline_journal_tests.rs"]
mod journal_tests;

#[path = "admission_graph_owner_tests.rs"]
mod admission_graph_tests;

type ClaimSpec<'a> = (
    u128,
    u64,
    &'a [(ValidationMode, bool)],
    &'a [(graph::Kind, u128)],
);

fn claim_key(claim: u128, index: u32) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(claim),
        validation: ValidationId::from_u128(claim * 100 + u128::from(index)),
        target: EvaluationTarget::Admission,
        generation: 1,
    }
}

fn authored_posted(specs: &[ClaimSpec<'_>]) -> Core<NativeState> {
    let mut core = Core::new_native(
        binding(1).ledger,
        RangeId(7901),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: 65_536,
            preparation_bytes: 2 * 1024 * 1024,
            evaluations_per_claim: 32,
            range: RangeConfig {
                page_entries: 4,
                max_batch_entries: 256,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        MemoryBudget::new(192 * 1024 * 1024, 24 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for &(id, at, requirements, edges) in specs {
        let input = creation(1, id, requirements, None);
        let NativeCommand::Create {
            claims: mut authored,
            declarations: definitions,
        } = input.command
        else {
            panic!("create");
        };
        authored[0].definition.deadline = Some(Deadline {
            timer: TimerId::from_u128(10_000 + id),
            generation: 1,
            at,
        });
        let obligations: Vec<_> = edges
            .iter()
            .map(|&(kind, target)| graph::Obligation {
                kind,
                target: ClaimId::from_u128(target),
            })
            .collect();
        authored[0].definition.graph = graph::Declaration::new(&obligations, 16).unwrap();
        claims.extend(authored);
        declarations.extend(definitions);
    }
    publish(
        &mut core,
        10,
        NativeInput {
            request: request(ISSUER, 1),
            command: NativeCommand::Create {
                claims,
                declarations,
            },
        },
    );
    for &(id, _, _, _) in specs {
        publish(&mut core, 20, post(20 + id, binding(id)));
    }
    core
}

fn input(owner: &NativeOwner, claim: u128) -> NativeClaimDeadlineInput {
    let claim = ClaimId::from_u128(claim);
    NativeClaimDeadlineInput {
        claim,
        deadline: owner.effective().claim(claim).unwrap().deadline().unwrap(),
    }
}

fn fire(
    owner: &mut NativeOwner,
    input: NativeClaimDeadlineInput,
    time: u64,
) -> (NativeCandidate, NativeOutcome) {
    match owner.prepare_claim_deadline(input, time).unwrap() {
        NativeStaging::Prepared { candidate, outcome } => (candidate, outcome),
        existing => panic!("expected fresh claim deadline, got {existing:?}"),
    }
}

fn assert_sealed(
    owner: &NativeOwner,
    outcome: NativeOutcome,
    key: EvaluationKey,
    previous: validation::EvaluationState,
) {
    let view = owner.effective();
    let definition = view.definition(key.validation).unwrap();
    let next = view.evaluation(key).unwrap();
    let seal = previous
        .seal_claim(
            definition,
            &previous.binding(),
            view.claim(key.claim).unwrap(),
        )
        .unwrap();
    assert!(seal.changed());
    seal.check(&previous, next).unwrap();
    assert_eq!(*next, seal.next());
    assert_eq!(next.fence(), previous.fence());
    assert_eq!(next.last_result(), previous.last_result());
    let attempt = previous.has_begun().then(|| {
        previous
            .bind(definition)
            .unwrap()
            .current_attempt()
            .unwrap()
    });
    let expected = NativeFact::Evaluation {
        kind: NativeEvaluationEventKind::Sealed,
        key,
        before: Some(previous.binding()),
        after: next.binding(),
        state: next.state(),
        phase: next.phase(),
        attempt,
        fence: next.fence(),
    };
    let seals: Vec<_> = (0..outcome.events)
        .map(|ordinal| view.event(outcome.sequence, ordinal).unwrap().fact)
        .filter(|fact| {
            matches!(fact, NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Sealed, key: actual, ..
        } if *actual == key)
        })
        .collect();
    assert_eq!(seals, vec![expected]);
}

fn begin_claim(owner: &mut NativeOwner, claim: u128, index: u32, request_id: u128) {
    let key = claim_key(claim, index);
    let command = NativeCommand::BeginAdmission {
        claim: owner.effective().claim(key.claim).unwrap().binding(),
        key,
        expected: owner.effective().evaluation(key).unwrap().binding(),
    };
    let candidate = stage(
        owner,
        NativeInput {
            request: request(EVALUATOR, request_id),
            command,
        },
        30,
    );
    owner.publish_after_durable(candidate).unwrap();
}

fn report_claim(
    owner: &NativeOwner,
    claim: u128,
    index: u32,
    id: u128,
    value: VerdictValue,
) -> NativeInput {
    let key = claim_key(claim, index);
    let view = owner.effective();
    let old = view.evaluation(key).unwrap();
    let attempt = old
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let artifact = descriptor(artifact_spec(30_000 + id, attempt.evaluator, value))
        .with_result_provenance(ResultProvenance {
            claim: key.claim,
            validation: key.validation,
            target: old.target(),
            generation: old.generation(),
            attempt,
            value,
        })
        .unwrap();
    NativeInput {
        request: request(attempt.evaluator, id),
        command: NativeCommand::ReportAdmission {
            claim: view.claim(key.claim).unwrap().binding(),
            key,
            expected: old.binding(),
            report: validation::Report {
                generation: old.generation(),
                attempt,
                value,
                evidence: ArtifactRef {
                    id: artifact.id(),
                    hash: artifact.content_hash(),
                },
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    }
}

fn report_at(
    owner: &mut NativeOwner,
    store: &mut Store,
    input: NativeInput,
    time: u64,
) -> NativeCandidate {
    match owner
        .prepare_with_custody(
            context(input.request.principal, time),
            input,
            &mut store.content,
            DOMAIN,
            &BuiltinNativeSchemas,
        )
        .unwrap()
    {
        NativeStaging::Prepared { candidate, .. } => candidate,
        existing => panic!("expected report, got {existing:?}"),
    }
}

fn response(owner: &mut NativeOwner) -> TestamentId {
    let claim = ClaimId::from_u128(1);
    let received = NativeInput {
        request: request(SUBJECT, 90),
        command: NativeCommand::AcquireReceipt {
            expected: owner.effective().claim(claim).unwrap().binding(),
            receipt: ReceiptId::from_u128(701),
        },
    };
    let candidate = stage(owner, received, 120);
    owner.publish_after_durable(candidate).unwrap();
    let closed = NativeInput {
        request: request(SUBJECT, 91),
        command: NativeCommand::CloseResponse {
            claim: owner.effective().claim(claim).unwrap().binding(),
            response: binding(900),
            report: NativeResponseInput {
                summary: "The respondent finished and reported its outcome.".into(),
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
                manifest: vec![],
                diagnostics: vec![],
            },
        },
    };
    let candidate = stage(owner, closed, 130);
    owner.publish_after_durable(candidate).unwrap();
    let id = TestamentId::from_u128(900);
    let posted = NativeInput {
        request: request(SUBJECT, 92),
        command: NativeCommand::PostResponse {
            claim: owner.effective().claim(claim).unwrap().binding(),
            expected: owner.effective().response(id).unwrap().identity().binding,
        },
    };
    let candidate = stage(owner, posted, 140);
    owner.publish_after_durable(candidate).unwrap();
    let acknowledged = NativeInput {
        request: request(ISSUER, 93),
        command: NativeCommand::ReceiveResponse {
            claim: owner.effective().claim(claim).unwrap().binding(),
            expected: owner.effective().response(id).unwrap().identity().binding,
        },
    };
    let candidate = stage(owner, acknowledged, 150);
    owner.publish_after_durable(candidate).unwrap();
    id
}

#[test]
fn acyclic_claim_expiry_fences_ready_and_begun_checks_without_repainting_evidence_or_response() {
    let core = authored_posted(&[(
        1,
        500,
        &[
            (ValidationMode::Observe, true),
            (ValidationMode::Observe, false),
            (ValidationMode::Observe, false),
        ],
        &[],
    )]);
    let mut owner = NativeOwner::new(core).unwrap();
    begin_claim(&mut owner, 1, 1, 31);
    begin_claim(&mut owner, 1, 2, 32);
    let mut store = Store::new();
    let report = report_claim(&owner, 1, 1, 41, VerdictValue::Error);
    let candidate = report_at(&mut owner, &mut store, report, 100);
    owner.publish_after_durable(candidate).unwrap();
    let report = report_claim(&owner, 1, 2, 42, VerdictValue::Pass);
    let candidate = report_at(&mut owner, &mut store, report, 110);
    owner.publish_after_durable(candidate).unwrap();
    let response = response(&mut owner);
    let record = owner.committed().response_record(response).unwrap();
    let response_before = (
        record.response().identity(),
        record.response().state(),
        record.received(),
        record.entered(),
        record.response().summary().to_owned(),
    );
    let previous: Vec<_> = (1..=3)
        .map(|index| *owner.committed().evaluation(claim_key(1, index)).unwrap())
        .collect();
    let accepted: Vec<_> = previous
        .iter()
        .filter_map(|state| state.last_result())
        .map(|result| {
            *owner
                .committed()
                .result(NativeResultKey::of(result))
                .unwrap()
        })
        .collect();
    let timer = input(&owner, 1);
    let (candidate, outcome) = fire(&mut owner, timer, 500);
    assert_eq!(outcome.operation, NativeOperation::ClaimDeadline);
    assert_eq!(
        outcome.invocation,
        NativeInvocation::ClaimDeadline(timer.key())
    );
    assert_eq!((outcome.artifacts, outcome.results), (0, 0));
    assert_eq!(
        owner.effective().claim(timer.claim).unwrap().status(),
        ClaimStatus::Expired
    );
    assert!(
        matches!(owner.effective().event(outcome.sequence, 0).unwrap().fact,
        NativeFact::Claim(event) if event.kind == NativeEventKind::Expired && event.status == ClaimStatus::Expired)
    );
    for index in [1, 3] {
        let old = previous[(index - 1) as usize];
        let key = claim_key(1, index);
        let view = owner.effective();
        let fenced = old
            .expire_claim(
                view.definition(key.validation).unwrap(),
                view.claim(timer.claim).unwrap(),
            )
            .unwrap();
        assert_eq!(fenced.binding(), old.binding().next().unwrap());
        assert_sealed(&owner, outcome, key, fenced);
        let next = view.evaluation(key).unwrap();
        assert_eq!(next.binding(), fenced.binding().next().unwrap());
        assert_eq!(next.state(), old.state());
        assert_eq!(next.last_result(), old.last_result());
        assert_eq!(
            next.fence().unwrap().reason,
            validation::FenceReason::Expiry
        );
    }
    assert_eq!(
        *owner.effective().evaluation(claim_key(1, 2)).unwrap(),
        previous[1]
    );
    assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), Some(0));
    for row in &accepted {
        assert_eq!(
            owner.effective().result(NativeResultKey::of(row.result())),
            Some(row)
        );
        assert!(
            owner
                .effective()
                .artifact(row.result().evidence().unwrap().id)
                .is_some()
        );
    }
    let record = owner.effective().response_record(response).unwrap();
    assert_eq!(
        (
            record.response().identity(),
            record.response().state(),
            record.received(),
            record.entered(),
            record.response().summary().to_owned()
        ),
        response_before
    );
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), None);
}

fn cycle_core() -> Core<NativeState> {
    authored_posted(&[
        (
            1,
            1000,
            &[(ValidationMode::Required, false)],
            &[(graph::Kind::Awaits, 2)],
        ),
        (2, 500, &[], &[(graph::Kind::Awaits, 1)]),
        (
            3,
            1000,
            &[(ValidationMode::Required, false)],
            &[(graph::Kind::DependsOn, 1)],
        ),
        (4, 1000, &[], &[(graph::Kind::Awaits, 1)]),
    ])
}

#[test]
fn due_cycle_chooses_canonical_distinct_victim_and_preserves_begun_business_failure_reports() {
    let mut owner = NativeOwner::new(cycle_core()).unwrap();
    begin_claim(&mut owner, 1, 1, 31);
    begin_claim(&mut owner, 3, 1, 33);
    let one = ClaimId::from_u128(1);
    let two = ClaimId::from_u128(2);
    let three = ClaimId::from_u128(3);
    let four = ClaimId::from_u128(4);
    assert_eq!(
        owner.committed().claim(one).unwrap().created(),
        owner.committed().claim(two).unwrap().created()
    );
    let first = *owner.committed().evaluation(claim_key(1, 1)).unwrap();
    let dependent = *owner.committed().evaluation(claim_key(3, 1)).unwrap();
    let timer = input(&owner, 2);
    let (candidate, outcome) = fire(&mut owner, timer, 500);
    assert_eq!(
        owner.effective().claim(one).unwrap().status(),
        ClaimStatus::Deadlocked
    );
    assert_eq!(
        owner.effective().claim(two).unwrap().status(),
        ClaimStatus::Expired
    );
    assert_eq!(
        owner.effective().claim(three).unwrap().status(),
        ClaimStatus::DependencyFailed
    );
    assert_eq!(
        owner.effective().claim(four).unwrap().status(),
        ClaimStatus::Posted
    );
    assert!(
        matches!(owner.effective().event(outcome.sequence, 0).unwrap().fact,
        NativeFact::Claim(event) if event.kind == NativeEventKind::Deadlocked && event.after.object.0 == one.0)
    );
    let Some(ClaimTerminalCut::Graph(cut)) = owner.effective().claim(one).unwrap().terminal_cut()
    else {
        panic!("deadlock cut");
    };
    assert_eq!(cut.deadline(), Some(timer.deadline));
    assert_eq!(cut.fired_at(), Some(500));
    assert_eq!(cut.origin().binding().object.0, two.0);
    assert_sealed(&owner, outcome, claim_key(1, 1), first);
    assert_sealed(&owner, outcome, claim_key(3, 1), dependent);
    assert!(owner.book.remaining_reports(claim_key(1, 1)).unwrap() > 0);
    assert!(owner.book.remaining_reports(claim_key(3, 1)).unwrap() > 0);
    owner.publish_after_durable(candidate).unwrap();
    // The original firing completes its trigger; re-delivery preserves that cut.
    let trigger_cut = owner.committed().claim(two).unwrap().terminal_cut();
    assert!(
        matches!(owner.prepare_claim_deadline(timer, 900).unwrap(), NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
    assert_eq!(
        owner.effective().claim(two).unwrap().status(),
        ClaimStatus::Expired
    );
    assert_eq!(
        owner.effective().claim(two).unwrap().terminal_cut(),
        trigger_cut
    );
    let wait = NativeInput {
        request: request(SUBJECT, 61),
        command: NativeCommand::AcquireReceipt {
            expected: owner.effective().claim(four).unwrap().binding(),
            receipt: ReceiptId::from_u128(704),
        },
    };
    let released = stage(&mut owner, wait, 501);
    owner.publish_after_durable(released).unwrap();
    assert_eq!(
        owner.committed().claim(four).unwrap().status(),
        ClaimStatus::Received
    );
    let mut store = Store::new();
    for claim in [1, 3] {
        let report = report_claim(&owner, claim, 1, 70 + claim, VerdictValue::Pass);
        let reported = report_at(&mut owner, &mut store, report, 600);
        owner.publish_after_durable(reported).unwrap();
        assert_eq!(
            owner
                .committed()
                .evaluation(claim_key(claim, 1))
                .unwrap()
                .state(),
            validation::State::Validated
        );
    }
    assert_eq!(
        owner.committed().claim(one).unwrap().status(),
        ClaimStatus::Deadlocked
    );
    assert_eq!(
        owner.committed().claim(three).unwrap().status(),
        ClaimStatus::DependencyFailed
    );
}

fn overlapping_cycles_core() -> Core<NativeState> {
    authored_posted(&[
        (
            1,
            1000,
            &[(ValidationMode::Required, false)],
            &[(graph::Kind::Awaits, 3)],
        ),
        (
            2,
            1000,
            &[(ValidationMode::Required, false)],
            &[(graph::Kind::Awaits, 3)],
        ),
        (
            3,
            500,
            &[(ValidationMode::Observe, true)],
            &[(graph::Kind::Awaits, 1), (graph::Kind::Awaits, 2)],
        ),
        (4, 1000, &[], &[(graph::Kind::DependsOn, 2)]),
        (5, 1000, &[], &[(graph::Kind::Awaits, 2)]),
    ])
}

#[test]
fn one_firing_breaks_each_remaining_trigger_cycle_then_expires_trigger_atomically() {
    let core = overlapping_cycles_core();
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    for claim in 1..=3 {
        begin_claim(&mut owner, claim, 1, 30 + claim);
    }
    let original: Vec<_> = (1..=3)
        .map(|claim| *owner.committed().evaluation(claim_key(claim, 1)).unwrap())
        .collect();
    let credit: Vec<_> = (1..=3)
        .map(|claim| owner.book.remaining_reports(claim_key(claim, 1)))
        .collect();
    let timer = input(&owner, 3);
    let trigger_binding = owner.committed().claim(timer.claim).unwrap().binding();
    let pinned = owner.pin(0, 5000).unwrap();
    let budget_before = parent.stats();
    let source_before = owner.book.source().stats();
    let (candidate, outcome) = fire(&mut owner, timer, 500);
    let mut fingerprints = Vec::new();
    for victim in 1..=2 {
        let claim = owner.effective().claim(ClaimId::from_u128(victim)).unwrap();
        assert_eq!(claim.status(), ClaimStatus::Deadlocked);
        let Some(ClaimTerminalCut::Graph(cut)) = claim.terminal_cut() else {
            panic!("deadlock");
        };
        assert_eq!(cut.kind(), graph::FailureKind::Deadlocked);
        assert_eq!(cut.deadline(), Some(timer.deadline));
        assert_eq!(cut.fired_at(), Some(500));
        assert_eq!(cut.sequence(), outcome.sequence);
        assert_eq!(cut.origin().binding(), trigger_binding);
        fingerprints.push(cut.fingerprint());
        assert_sealed(
            &owner,
            outcome,
            claim_key(victim, 1),
            original[(victim - 1) as usize],
        );
        assert_eq!(
            owner.book.remaining_reports(claim_key(victim, 1)),
            credit[(victim - 1) as usize]
        );
    }
    assert_ne!(fingerprints[0], fingerprints[1]);
    assert_eq!(
        owner.effective().claim(timer.claim).unwrap().status(),
        ClaimStatus::Expired
    );
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(4))
            .unwrap()
            .status(),
        ClaimStatus::DependencyFailed
    );
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(5))
            .unwrap()
            .status(),
        ClaimStatus::Posted
    );
    assert_eq!(
        owner
            .effective()
            .evaluation(claim_key(3, 1))
            .unwrap()
            .fence()
            .unwrap()
            .reason,
        validation::FenceReason::Expiry
    );
    assert_eq!(owner.book.remaining_reports(claim_key(3, 1)), Some(0));
    let history: Vec<_> = (0..outcome.events)
        .filter_map(|ordinal| {
            match owner
                .effective()
                .event(outcome.sequence, ordinal)
                .unwrap()
                .fact
            {
                NativeFact::Claim(event) => Some((ClaimId(event.after.object.0), event.kind)),
                _ => None,
            }
        })
        .collect();
    assert_eq!(
        history,
        vec![
            (ClaimId::from_u128(1), NativeEventKind::Deadlocked),
            (ClaimId::from_u128(2), NativeEventKind::Deadlocked),
            (ClaimId::from_u128(4), NativeEventKind::DependencyFailed),
            (timer.claim, NativeEventKind::Expired),
        ]
    );
    for claim in 1..=5 {
        assert_eq!(
            owner
                .committed()
                .claim(ClaimId::from_u128(claim))
                .unwrap()
                .status(),
            ClaimStatus::Posted
        );
    }
    assert_eq!(owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(parent.stats(), budget_before);
    assert_eq!(owner.book.source().stats(), source_before);
    for claim in 1..=3 {
        assert_eq!(
            *owner.effective().evaluation(claim_key(claim, 1)).unwrap(),
            original[(claim - 1) as usize]
        );
        assert_eq!(
            owner.book.remaining_reports(claim_key(claim, 1)),
            credit[(claim - 1) as usize]
        );
    }
    let (replacement, repeated) = fire(&mut owner, timer, 500);
    assert_ne!(replacement, candidate);
    assert_eq!(repeated, outcome);
    owner.publish_after_durable(replacement).unwrap();
    let terminal_cut = owner.committed().claim(timer.claim).unwrap().terminal_cut();
    let pressure = exhaust(&parent);
    assert!(
        matches!(owner.prepare_claim_deadline(timer, 0).unwrap(), NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
    assert_eq!(
        owner.committed().claim(timer.claim).unwrap().terminal_cut(),
        terminal_cut
    );
    assert_eq!(owner.book.remaining_reports(claim_key(3, 1)), None);
    for claim in 1..=3 {
        assert_eq!(
            pinned
                .with_evaluation(claim_key(claim, 1), 0, |state| *state)
                .unwrap(),
            Some(original[(claim - 1) as usize])
        );
    }
    drop(pressure);
    owner.release(&pinned).unwrap();
    drop(pinned);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}

#[test]
fn cumulative_scc_preparation_refusal_after_private_progress_publishes_nothing() {
    let core = overlapping_cycles_core();
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    begin_claim(&mut owner, 3, 1, 33);
    let timer = input(&owner, 3);
    let original = *owner.effective().evaluation(claim_key(3, 1)).unwrap();
    let original_credit = owner.book.remaining_reports(claim_key(3, 1));
    let original_limits = owner.core.limits;
    // Quote the real preparation against the same effective rows and exact
    // timer cut, then refuse its last cumulative charge. Inspecting its private
    // journal proves that this refusal follows a real canonical victim step.
    let tight = {
        let view = owner.effective();
        let cut = focal_model::lifecycle::claim::ClaimCut {
            position: SessionSeq(view.sequence().0 + 1),
            cause: crate::native::intent::claim_deadline_fingerprint(view.ledger(), timer).unwrap(),
        };
        let prepare = |max| {
            let limits = NativeLimits {
                preparation_bytes: max,
                ..original_limits
            };
            let construction = crate::native::prepare_budget::ConstructionBudget::for_operation(
                NativeOperation::ClaimDeadline,
                limits,
            )
            .unwrap();
            let mut scratch = crate::native::prepare::Scratch {
                used: 0,
                max: construction.scratch_bytes,
            };
            let mut extras = crate::native::prepare::Extras::new(
                construction.extras_count,
                construction.extras_bytes,
            )
            .unwrap();
            let resolved = crate::native::claim_deadlines::resolve(&view.0, timer, 500).unwrap();
            let result = crate::native::claim_deadlines::prepare(
                &view.0,
                resolved,
                timer,
                500,
                cut,
                limits,
                &mut extras,
                &mut scratch,
            );
            (result, extras, scratch.used)
        };
        let (complete, extras, used) = prepare(original_limits.preparation_bytes);
        assert!(complete.is_ok());
        assert!(
            extras
                .journal
                .as_ref()
                .unwrap()
                .iter()
                .filter(|fact| matches!(fact,
            NativeFact::Claim(event) if event.kind == NativeEventKind::Deadlocked))
                .count()
                >= 2
        );
        drop(complete);
        drop(extras);
        let tight = used.checked_sub(1).unwrap();
        let (refused, extras, _) = prepare(tight);
        assert!(matches!(
            refused,
            Err(NativeError::Capacity(_)) | Err(NativeError::Contract(ContractError::Capacity))
        ));
        assert!(
            extras
                .journal
                .as_ref()
                .unwrap()
                .iter()
                .any(|fact| matches!(fact,
            NativeFact::Claim(event) if event.kind == NativeEventKind::Deadlocked))
        );
        tight
    };
    owner.core.limits.preparation_bytes = tight;
    let before = parent.stats();
    let source_before = owner.book.source().stats();
    let prefix = owner.effective().sequence();
    assert!(owner.prepare_claim_deadline(timer, 500).is_err());
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(owner.effective().sequence(), prefix);
    assert_eq!(parent.stats(), before);
    assert_eq!(owner.book.source().stats(), source_before);
    assert_eq!(
        *owner.effective().evaluation(claim_key(3, 1)).unwrap(),
        original
    );
    assert_eq!(
        owner.book.remaining_reports(claim_key(3, 1)),
        original_credit
    );
    for claim in 1..=5 {
        assert_eq!(
            owner
                .effective()
                .claim(ClaimId::from_u128(claim))
                .unwrap()
                .status(),
            ClaimStatus::Posted
        );
    }
    owner.core.limits = original_limits;
    let (candidate, _) = fire(&mut owner, timer, 500);
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        owner.committed().claim(timer.claim).unwrap().status(),
        ClaimStatus::Expired
    );
}

#[test]
fn early_stale_and_changed_claim_timers_refuse_but_exact_retry_ignores_backwards_clock_and_pressure()
 {
    let core = authored_posted(&[(1, 500, &[], &[])]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let timer = input(&owner, 1);
    let before = parent.stats();
    assert!(owner.prepare_claim_deadline(timer, 499).is_err());
    for deadline in [
        Deadline {
            at: 501,
            ..timer.deadline
        },
        Deadline {
            generation: 2,
            ..timer.deadline
        },
        Deadline {
            timer: TimerId::from_u128(9999),
            ..timer.deadline
        },
    ] {
        assert!(
            owner
                .prepare_claim_deadline(NativeClaimDeadlineInput { deadline, ..timer }, 600)
                .is_err()
        );
    }
    assert_eq!(parent.stats(), before);
    let (candidate, outcome) = fire(&mut owner, timer, 500);
    let pressure = exhaust(&parent);
    let full = parent.stats();
    assert!(
        matches!(owner.prepare_claim_deadline(timer, 0).unwrap(), NativeStaging::Existing { outcome: actual, candidate: Some(ticket) } if actual == outcome && ticket == candidate)
    );
    assert_eq!(parent.stats(), full);
    owner.publish_after_durable(candidate).unwrap();
    assert!(
        matches!(owner.prepare_claim_deadline(timer, 0).unwrap(), NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
    assert!(matches!(
        owner.prepare_claim_deadline(
            NativeClaimDeadlineInput {
                deadline: Deadline {
                    at: 501,
                    ..timer.deadline
                },
                ..timer
            },
            600
        ),
        Err(NativeOwnerError::Native(NativeError::RequestConflict))
    ));
    drop(pressure);
    let later = stage(&mut owner, creation(81, 2, &[], None), 700);
    owner.publish_after_durable(later).unwrap();
    assert!(owner.effective().sequence() > outcome.sequence);
    assert!(
        matches!(owner.prepare_claim_deadline(timer, 1).unwrap(), NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
}

#[test]
fn terminal_claim_timer_records_consumption_without_rewriting_original_cut_or_check_evidence() {
    let core = authored_posted(&[(1, 500, &[(ValidationMode::Required, false)], &[])]);
    let mut owner = NativeOwner::new(core).unwrap();
    begin_claim(&mut owner, 1, 1, 31);
    let mut store = Store::new();
    let report = report_claim(&owner, 1, 1, 41, VerdictValue::Fail);
    let reported = report_at(&mut owner, &mut store, report, 100);
    let original = owner.effective().claim(ClaimId::from_u128(1)).unwrap();
    let original_binding = original.binding();
    let original_cut = original.terminal_cut();
    assert_eq!(original.status(), ClaimStatus::PostFailed);
    let evaluation = *owner.effective().evaluation(claim_key(1, 1)).unwrap();
    let result = evaluation.last_result().unwrap();
    let timer = input(&owner, 1);
    let (deadline, outcome) = fire(&mut owner, timer, 500);
    assert_eq!(
        (outcome.events, outcome.artifacts, outcome.results),
        (0, 0, 0)
    );
    assert_eq!(
        owner.effective().claim(timer.claim).unwrap().binding(),
        original_binding
    );
    assert_eq!(
        owner.effective().claim(timer.claim).unwrap().terminal_cut(),
        original_cut
    );
    assert_eq!(
        *owner.effective().evaluation(claim_key(1, 1)).unwrap(),
        evaluation
    );
    assert_eq!(
        owner
            .effective()
            .result(NativeResultKey::of(result))
            .unwrap()
            .result(),
        result
    );
    owner.publish_after_durable(reported).unwrap();
    owner.publish_after_durable(deadline).unwrap();
}

#[test]
fn pending_report_then_claim_expiry_and_suffix_discard_restore_credit_evidence_and_effective_time()
{
    let core = authored_posted(&[(1, 500, &[(ValidationMode::Required, true)], &[])]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    begin_claim(&mut owner, 1, 1, 31);
    let old = *owner.effective().evaluation(claim_key(1, 1)).unwrap();
    let credit = owner.book.remaining_reports(claim_key(1, 1));
    let report = report_claim(&owner, 1, 1, 41, VerdictValue::Error);
    let retry = copy_report(&report);
    let timer = input(&owner, 1);
    let before = parent.stats();
    let source_before = owner.book.source().stats();
    let mut store = Store::new();
    let reported = report_at(&mut owner, &mut store, report, 100);
    let evaluated = *owner.effective().evaluation(claim_key(1, 1)).unwrap();
    let result = evaluated.last_result().unwrap();
    let (deadline, _) = fire(&mut owner, timer, 500);
    assert_eq!(
        owner
            .effective()
            .evaluation(claim_key(1, 1))
            .unwrap()
            .last_result(),
        Some(result)
    );
    assert_eq!(
        owner
            .effective()
            .evaluation(claim_key(1, 1))
            .unwrap()
            .fence()
            .unwrap()
            .reason,
        validation::FenceReason::Expiry
    );
    assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), Some(0));
    assert_eq!(owner.discard_from(deadline).unwrap(), 1);
    assert_eq!(
        *owner.effective().evaluation(claim_key(1, 1)).unwrap(),
        evaluated
    );
    assert_eq!(owner.effective().logical_time(), 100);
    let (deadline, _) = fire(&mut owner, timer, 500);
    assert_eq!(owner.discard_from(reported).unwrap(), 2);
    assert!(matches!(
        owner.discard_from(deadline),
        Err(NativeOwnerError::UnknownCandidate)
    ));
    assert_eq!(*owner.effective().evaluation(claim_key(1, 1)).unwrap(), old);
    assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), credit);
    assert_eq!(owner.book.source().stats(), source_before);
    assert_eq!(parent.stats(), before);
    assert!(
        owner
            .effective()
            .result(NativeResultKey::of(result))
            .is_none()
    );
    assert!(
        owner
            .effective()
            .artifact(result.evidence().unwrap().id)
            .is_none()
    );
    let replacement = report_at(&mut owner, &mut store, retry, 100);
    owner.publish_after_durable(replacement).unwrap();
}

#[test]
fn pending_claim_expiry_refuses_report_until_discard_restores_its_live_grant() {
    let core = authored_posted(&[(1, 500, &[(ValidationMode::Required, false)], &[])]);
    let mut owner = NativeOwner::new(core).unwrap();
    begin_claim(&mut owner, 1, 1, 31);
    let report = report_claim(&owner, 1, 1, 41, VerdictValue::Pass);
    let retry = copy_report(&report);
    let timer = input(&owner, 1);
    let credit = owner.book.remaining_reports(claim_key(1, 1));
    let (deadline, _) = fire(&mut owner, timer, 500);
    let mut store = Store::new();
    assert!(
        owner
            .prepare_with_custody(
                context(EVALUATOR, 501),
                report,
                &mut store.content,
                DOMAIN,
                &BuiltinNativeSchemas
            )
            .is_err()
    );
    assert_eq!(owner.pending_len(), 1);
    assert_eq!(owner.discard_from(deadline).unwrap(), 1);
    assert_eq!(owner.book.remaining_reports(claim_key(1, 1)), credit);
    let reported = report_at(&mut owner, &mut store, retry, 100);
    owner.publish_after_durable(reported).unwrap();
}

#[test]
fn graph_closure_and_byte_capacity_refusals_never_fall_back_to_expiry() {
    let mut core = cycle_core();
    // Reopen the existing actual owner under a stricter graph planning limit.
    // Rows and graph declarations remain exactly as authored and published.
    core.limits.plan_nodes = 1;
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let timer = input(&owner, 2);
    let before = parent.stats();
    let sequence = owner.effective().sequence();
    assert!(owner.prepare_claim_deadline(timer, 500).is_err());
    assert_eq!(owner.effective().sequence(), sequence);
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(parent.stats(), before);
    for id in 1..=4 {
        assert_eq!(
            owner
                .effective()
                .claim(ClaimId::from_u128(id))
                .unwrap()
                .status(),
            ClaimStatus::Posted
        );
    }

    let core = cycle_core();
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    let timer = input(&owner, 2);
    let pressure = exhaust(&parent);
    let full = parent.stats();
    assert!(owner.prepare_claim_deadline(timer, 500).is_err());
    assert_eq!(parent.stats(), full);
    assert_eq!(owner.pending_len(), 0);
    assert_eq!(
        owner.effective().claim(timer.claim).unwrap().status(),
        ClaimStatus::Posted
    );
    drop(pressure);
    let (candidate, _) = fire(&mut owner, timer, 500);
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::Deadlocked
    );
    assert_ne!(owner.committed().sequence(), SessionSeq(0));
}

#[test]
fn multi_grant_claim_expiry_retires_and_rewinds_through_completion_when_ordinary_is_full() {
    let core = authored_posted(&[(1, 500, &[(ValidationMode::Observe, true); 3], &[])]);
    let parent = core.state.budget.clone();
    let mut owner = NativeOwner::new(core).unwrap();
    for index in 1..=3 {
        begin_claim(&mut owner, 1, index, 30 + u128::from(index));
    }
    let originals: Vec<_> = (1..=3)
        .map(|index| *owner.committed().evaluation(claim_key(1, index)).unwrap())
        .collect();
    let credits: Vec<_> = (1..=3)
        .map(|index| owner.book.remaining_reports(claim_key(1, index)).unwrap())
        .collect();
    assert!(credits.iter().all(|credit| *credit > 0));
    let pinned = owner.pin(0, 5000).unwrap();
    let timer = input(&owner, 1);
    let available = parent.stats();
    let ordinary_bytes = available.limit - available.completion_reserve - available.ordinary_used;
    let pressure = parent
        .reserve(BudgetKind::Payload, BudgetLane::Ordinary, ordinary_bytes)
        .unwrap();
    let full = parent.stats();
    assert_eq!(full.ordinary_used, full.limit - full.completion_reserve);
    assert!(full.used < full.limit);
    assert!(
        parent
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 1)
            .is_err()
    );
    let completion_probe = parent
        .reserve(BudgetKind::Pending, BudgetLane::Completion, 1)
        .unwrap();
    drop(completion_probe);
    let source_before = owner.book.source().stats();

    // Three real grants require the multi-update retirement journal. The
    // candidate and that journal must both fit through the control lane.
    let (candidate, outcome) = fire(&mut owner, timer, 500);
    assert_eq!(
        owner.effective().claim(timer.claim).unwrap().status(),
        ClaimStatus::Expired
    );
    assert_eq!(parent.stats().ordinary_used, full.ordinary_used);
    assert_eq!((outcome.artifacts, outcome.results), (0, 0));
    for index in 1..=3 {
        assert_eq!(owner.book.remaining_reports(claim_key(1, index)), Some(0));
        assert_eq!(
            owner
                .effective()
                .evaluation(claim_key(1, index))
                .unwrap()
                .fence()
                .unwrap()
                .reason,
            validation::FenceReason::Expiry
        );
    }
    assert_eq!(owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(parent.stats(), full);
    assert_eq!(owner.book.source().stats(), source_before);
    for index in 1..=3 {
        assert_eq!(
            *owner.effective().evaluation(claim_key(1, index)).unwrap(),
            originals[(index - 1) as usize]
        );
        assert_eq!(
            owner.book.remaining_reports(claim_key(1, index)),
            Some(credits[(index - 1) as usize])
        );
    }

    let (replacement, _) = fire(&mut owner, timer, 500);
    owner.publish_after_durable(replacement).unwrap();
    for index in 1..=3 {
        assert_eq!(owner.book.remaining_reports(claim_key(1, index)), None);
        assert_eq!(
            pinned
                .with_evaluation(claim_key(1, index), 0, |state| *state)
                .unwrap(),
            Some(originals[(index - 1) as usize])
        );
    }
    owner.release(&pinned).unwrap();
    drop(pinned);
    drop(pressure);
    drop(owner);
    assert_eq!(parent.stats().used, 0);
}
#[path = "monitor_deadline_tests.rs"]
mod monitor_tests;
