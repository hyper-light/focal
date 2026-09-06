use super::*;

#[test]
fn monitor_documents_preserve_explicit_timer_and_reject_ambiguous_or_spoofed_input() {
    let json = br#"{"monitor":"00000000000000000000000000000001","owner":"00000000000000000000000000000002","roots":[{"predicate":"terminal","claim":"00000000000000000000000000000003"}],"deadline":{"timer":"00000000000000000000000000000004","generation":7,"at":123456}}"#;
    let document: MonitorRegisterDocument = parse_document(json, InputFormat::Json).unwrap();
    let yaml = "monitor: '00000000000000000000000000000001'\nowner: '00000000000000000000000000000002'\nroots:\n  - predicate: terminal\n    claim: '00000000000000000000000000000003'\ndeadline:\n  timer: '00000000000000000000000000000004'\n  generation: 7\n  at: 123456\n";
    let yaml_document = parse_document(yaml.as_bytes(), InputFormat::Yaml).unwrap();
    assert_eq!(document, yaml_document);
    let context = BuildContext {
        ledger: LedgerId {
            tenant: TenantId::from_u128(8),
            session: SessionId::from_u128(9),
        },
        actor: ParticipantId::from_u128(10),
        root: RootCommandId::from_u128(11),
        policy_revision: 1,
    };
    let command = document
        .clone()
        .build(&context, &mut || {
            panic!("explicit monitor ID must not generate identity")
        })
        .unwrap();
    let Command::RegisterMonitor {
        monitor,
        owner,
        roots,
        deadline,
    } = &command
    else {
        panic!("monitor")
    };
    assert_eq!(*monitor, MonitorId::from_u128(1));
    assert_eq!(*owner, ClaimId::from_u128(2));
    assert_eq!(
        *deadline,
        Deadline {
            timer: TimerId::from_u128(4),
            generation: 7,
            at: 123456
        }
    );
    assert_eq!(roots.len(), 1);
    let parsed = parse_json("monitor.register", json)
        .unwrap()
        .build(&context, &mut || panic!("id"))
        .unwrap();
    assert_eq!(parsed, PlannedOperation::Mutation(command));
    let mut duplicate = document.clone();
    duplicate.roots.push(duplicate.roots[0].clone());
    assert!(duplicate.build(&context, &mut || panic!("id")).is_err());
    let mut missing = document.clone();
    missing.deadline.generation = 0;
    assert!(missing.build(&context, &mut || panic!("id")).is_err());
    let mut forged = serde_json::to_value(&document).unwrap();
    forged["principal"] = serde_json::json!("00000000000000000000000000000002");
    assert!(parse_json("monitor.register", &serde_json::to_vec(&forged).unwrap()).is_err());
    let mut core = focal_core::Core::new(context.ledger, Limits::default());
    let mut input = AuthenticatedInput {
        ledger: context.ledger,
        principal: context.actor,
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(context.root),
            policy_revision: 1,
            logical_time: 0,
            evidence: vec![],
        },
        command: Command::NegotiateEpoch {
            epoch: RequestEpoch(1),
        },
    };
    let prepared = core.prepare(&input).unwrap();
    core.apply_serial(SessionSeq(1), prepared).unwrap();
    input.request_id = RequestId::from_u128(2);
    input.authority.runtime = false;
    input.command = document
        .clone()
        .build(&context, &mut || panic!("id"))
        .unwrap();
    assert!(
        core.prepare(&input).is_err(),
        "a real owner claim is required"
    );
    let owner: ClaimDocument = serde_json::from_value(serde_json::json!({"id":"00000000000000000000000000000002","occurrence":"00000000000000000000000000000005","target":"self","action":"handoff","description":"Own this continuation","validations":[{"id":"00000000000000000000000000000006","kind":"receipt","phase":"whole_work","mode":"required","description":"Delivery","evaluator":"self"}]})).unwrap();
    input.command = owner.build(&context, &mut || panic!("id")).unwrap();
    let prepared = core.prepare(&input).unwrap();
    core.apply_serial(SessionSeq(2), prepared).unwrap();
    input.request_id = RequestId::from_u128(3);
    input.principal = ParticipantId::from_u128(20);
    input.authority.runtime = true;
    input.command = Command::NegotiateEpoch {
        epoch: RequestEpoch(1),
    };
    let prepared = core.prepare(&input).unwrap();
    core.apply_serial(SessionSeq(3), prepared).unwrap();
    let mut valid = document;
    valid.roots = vec![WaitPredicateDocument::Terminal {
        claim: format!("{:032x}", 2),
    }];
    input.command = valid.build(&context, &mut || panic!("id")).unwrap();
    input.request_id = RequestId::from_u128(4);
    input.authority.runtime = false;
    assert!(matches!(
        core.prepare(&input),
        Err(DomainOutcome::Refuse {
            code: ErrorCode::WrongActor,
            ..
        })
    ));
    input.principal = context.actor;
    input.authority.logical_time = 123456;
    assert!(matches!(
        core.prepare(&input),
        Err(DomainOutcome::Refuse {
            code: ErrorCode::MissingDeadline,
            ..
        })
    ));
    input.authority.logical_time = 1;
    let prepared = core.prepare(&input).unwrap();
    core.apply_serial(SessionSeq(4), prepared).unwrap();
    assert_eq!(
        core.snapshot().monitors[&MonitorId::from_u128(1)]
            .deadline
            .generation,
        7
    );
}
