use super::*;
use clap::{CommandFactory, Parser};

fn command(args: &[&str]) -> Commands {
    let args =
        super::super::Args::try_parse_from(std::iter::once("focal").chain(args.iter().copied()))
            .unwrap();
    let super::super::Commands::Manual(command) = args.command else {
        panic!("manual command expected");
    };
    *command
}
fn claim_document(args: &[&str]) -> ClaimDocument {
    let Commands::Submit {
        command: SubmitCommand::Claim(args),
    } = command(args)
    else {
        panic!("claim expected");
    };
    documents::claim(args).unwrap().0
}
fn context() -> BuildContext {
    BuildContext {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        actor: ParticipantId::from_u128(3),
        root: RootCommandId::from_u128(4),
        policy_revision: 1,
    }
}
#[test]
fn complete_command_tree_and_legacy_request_are_unambiguous() {
    super::super::Args::command().debug_assert();
    for family in ["claims", "testaments", "artifacts", "validations"] {
        assert!(matches!(command(&["list", family]), Commands::List { .. }));
    }
    for args in [
        vec!["focal", "request", "request.json"],
        vec!["focal", "request", "retry", "/tmp/operation"],
        vec!["focal", "request", "inspect", "/tmp/operation"],
        vec!["focal", "request", "inspect", "/tmp/operation", "--remote"],
        vec![
            "focal",
            "request",
            "status",
            "--request-id",
            "00000000000000000000000000000001",
        ],
        vec!["focal", "request", "epoch"],
    ] {
        assert!(matches!(
            super::super::Args::try_parse_from(args).unwrap().command,
            super::super::Commands::Request(_)
        ));
    }
    for args in [
        vec!["focal", "request"],
        vec!["focal", "list", "claims", "--limit", "0"],
        vec!["focal", "list", "claims", "--limit", "257"],
        vec!["focal", "submit", "claim", "--json", "{}", "--yaml", "{}"],
        vec!["focal", "submit", "claim", "--input-format", "json"],
    ] {
        assert!(super::super::Args::try_parse_from(args).is_err());
    }
}
#[test]
fn actual_flags_json_and_yaml_compile_to_identical_wire_bytes() {
    let validation = r#"{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receipt of testament","evaluator":"self"}"#;
    let flags = claim_document(&[
        "submit",
        "claim",
        "--target",
        "self",
        "--action",
        "handoff",
        "--description",
        "Review report",
        "--validation-json",
        validation,
    ]);
    let json = serde_json::to_string(&flags).unwrap();
    let from_json = claim_document(&["submit", "claim", "--json", &json]);
    let from_yaml = claim_document(&[
        "submit",
        "claim",
        "--yaml",
        "target: self\naction: handoff\ndescription: Review report\nvalidations:\n  - kind: receipt\n    phase: whole_work\n    mode: required\n    description: Receipt of testament\n    evaluator: self\n",
    ]);
    let mut encoded = Vec::new();
    for document in [flags, from_json, from_yaml] {
        let mut next = 100u128;
        let focal_client::operations::PlannedOperation::Mutation(command) =
            focal_client::operations::AuthoredOperation::ClaimSubmit(document)
                .build(&context(), &mut || {
                    next += 1;
                    Ok(next.to_be_bytes())
                })
                .unwrap()
        else {
            panic!("mutation expected")
        };
        encoded.push(postcard::to_stdvec(&command).unwrap());
    }
    assert_eq!(encoded[0], encoded[1]);
    assert_eq!(encoded[1], encoded[2]);
}

#[test]
fn lifecycle_flags_share_registry_json_commands_and_keep_existing_text_semantics() {
    use focal_client::operations::*;
    let claim = "00000000000000000000000000000010";
    let receipt = "00000000000000000000000000000011";
    for (args, tool, input) in [
        (
            vec!["claim", "post", claim],
            "claim.post",
            serde_json::json!({"claim":claim}),
        ),
        (
            vec![
                "claim",
                "progress",
                claim,
                "--receipt",
                receipt,
                "--receipt-epoch",
                "3",
                "--message",
                "",
            ],
            "claim.progress",
            serde_json::json!({"claim":claim,"receipt":{"id":receipt,"epoch":3},"message":""}),
        ),
        (
            vec!["claim", "cancel", claim, "--reason", ""],
            "claim.cancel",
            serde_json::json!({"claim":claim,"reason":""}),
        ),
        (
            vec!["receipt", "acquire", claim, "--epoch", "2"],
            "receipt.acquire",
            serde_json::json!({"claim":claim,"epoch":2}),
        ),
        (
            vec![
                "evidence",
                "begin",
                "--claim",
                claim,
                "--receipt",
                receipt,
                "--receipt-epoch",
                "3",
            ],
            "evidence.begin",
            serde_json::json!({"claim":claim,"receipt":{"id":receipt,"epoch":3}}),
        ),
    ] {
        let (flags, _) = authored::mutation(command(&args)).unwrap();
        let json = parse_json(tool, &serde_json::to_vec(&input).unwrap()).unwrap();
        assert_eq!(
            flags.canonical_intent().unwrap(),
            json.canonical_intent().unwrap()
        );
        let left = flags
            .build(&context(), &mut || Ok([7; 16]))
            .unwrap()
            .into_wire(Some(ObjectRevision(9)))
            .unwrap();
        let right = json
            .build(&context(), &mut || Ok([7; 16]))
            .unwrap()
            .into_wire(Some(ObjectRevision(9)))
            .unwrap();
        assert_eq!(
            postcard::to_stdvec(&left).unwrap(),
            postcard::to_stdvec(&right).unwrap()
        );
    }
}

#[test]
fn list_flags_share_registry_predicates_and_preserve_cli_page_defaults() {
    use focal_client::operations::*;
    let claim = "00000000000000000000000000000010";
    for (args, tool, kind, input) in [
        (
            vec![
                "list", "claims", "--source", "self", "--target", claim, "--status", "posted",
                "--action", "work", "--cursor", "aB00",
            ],
            "claim.list",
            ObjectKind::Claim,
            serde_json::json!({"source":"self","target":claim,"status":"posted","action":"work","cursor":"aB00","limit":100}),
        ),
        (
            vec!["list", "testaments", "--claim", claim],
            "testament.list",
            ObjectKind::Testament,
            serde_json::json!({"claim":claim,"limit":100}),
        ),
        (
            vec![
                "list",
                "artifacts",
                "--testament",
                claim,
                "--producer",
                "self",
                "--kind",
                "text",
            ],
            "artifact.list",
            ObjectKind::Artifact,
            serde_json::json!({"testament":claim,"producer":"self","kind":"text","limit":100}),
        ),
        (
            vec![
                "list",
                "validations",
                "--claim",
                claim,
                "--evaluator",
                "self",
                "--kind",
                "receipt",
                "--phase",
                "whole_work",
                "--mode",
                "required",
            ],
            "validation.list",
            ObjectKind::Validation,
            serde_json::json!({"claim":claim,"evaluator":"self","kind":"receipt","phase":"whole_work","mode":"required","limit":100}),
        ),
    ] {
        let Commands::List { command: parsed } = command(&args) else {
            panic!("list")
        };
        let args = match parsed {
            ListCommand::Claims(args)
            | ListCommand::Testaments(args)
            | ListCommand::Artifacts(args)
            | ListCommand::Validations(args)
            | ListCommand::Evaluations(args)
            | ListCommand::Receipts(args)
            | ListCommand::Monitors(args)
            | ListCommand::Events(args) => args,
        };
        let mut document = authored::filters(args.filters);
        document.limit = args.limit;
        document.cursor = args.cursor;
        let flags = document.build(kind, &context()).unwrap();
        let PlannedOperation::List(json) = parse_json(tool, &serde_json::to_vec(&input).unwrap())
            .unwrap()
            .build(&context(), &mut || Err(InputError::Identity))
            .unwrap()
        else {
            panic!("list")
        };
        assert_eq!(flags, json);
        assert_eq!(flags.max_items, 100);
    }
}
#[test]
fn authored_documents_never_merge_with_flags_or_accept_wire_authority() {
    for args in [
        vec!["submit", "claim", "--json", "{}", "--target", "self"],
        vec!["submit", "claim", "--yaml", "issuer: self"],
        vec![
            "submit",
            "claim",
            "--json",
            r#"{"description":"a","description":"b"}"#,
        ],
    ] {
        let Commands::Submit {
            command: SubmitCommand::Claim(args),
        } = command(&args)
        else {
            panic!("claim expected");
        };
        assert!(documents::claim(args).is_err());
    }
    let file = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
    std::fs::write(
        file.path(),
        "target: self\naction: handoff\ndescription: From file\nvalidations: []\n",
    )
    .unwrap();
    let from_file = claim_document(&["submit", "claim", "--file", file.path().to_str().unwrap()]);
    assert_eq!(from_file.description, "From file");
    assert!(from_file.build(&context(), &mut random_id).is_err());
}

#[test]
fn joined_markers_reject_manual_context_before_journaling_or_transmission() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(directory.path().to_owned());
    let node = focal_node::embedded::EmbeddedNode::open(&settings).unwrap();
    let identity = node.identity.clone();
    drop(node);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        Context::open(&settings, None).unwrap().build.actor,
        identity.issuer
    );
    for marker in ["JOIN", "JOIN.initialized"] {
        let path = directory.path().join(marker);
        std::fs::write(&path, b"joined state must not be decoded by a client").unwrap();
        let result = run(
            &runtime,
            &settings,
            command(&["claim", "post", "00000000000000000000000000000001"]),
            None,
        );
        assert!(
            matches!(result, Err(CliError::Input(message)) if message.contains("local node client context"))
        );
        assert!(!directory.path().join("client").exists());
        assert_eq!(
            decode_identity(&directory.path().join("IDENTITY")).unwrap(),
            identity
        );
        std::fs::remove_file(path).unwrap();
    }
    // Even an incomplete dangling marker is evidence of joined ownership. A
    // symlink marker is a Unix-only shape (creating one needs privilege on
    // Windows); the private-file checks reject a reparse point there anyway.
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            directory.path().join("absent"),
            directory.path().join("JOIN"),
        )
        .unwrap();
        assert!(matches!(
            Context::open(&settings, None),
            Err(CliError::Input(_))
        ));
        assert!(!directory.path().join("client").exists());
    }
}

#[test]
fn atomic_claim_batch_flags_json_yaml_share_one_intent_and_reject_mixed_input() {
    let claim = r#"{"id":"00000000000000000000000000000010","occurrence":"00000000000000000000000000000011","description":"Batch claim","target":"self","action":"handoff","validations":[{"id":"00000000000000000000000000000012","kind":"receipt","phase":"whole_work","mode":"required","description":"Receive response","evaluator":"self"}]}"#;
    let batch = format!("{{\"claims\":[{claim}]}}");
    let yaml = format!("claims:\n  - {claim}\n");
    let cases = [
        vec!["submit", "claims", "--claim-json", claim],
        vec!["submit", "claims", "--json", &batch],
        vec!["submit", "claims", "--yaml", &yaml],
    ];
    let mut intents = Vec::new();
    let mut wires = Vec::new();
    for args in cases {
        let (authored, _) = authored::mutation(command(&args)).unwrap();
        authored.preflight(&context()).unwrap();
        intents.push(authored.canonical_intent().unwrap());
        wires.push(
            authored
                .build(&context(), &mut || Err(InputError::Identity))
                .unwrap()
                .into_wire(None)
                .unwrap(),
        );
    }
    assert!(intents.windows(2).all(|pair| pair[0] == pair[1]));
    assert!(wires.windows(2).all(|pair| pair[0] == pair[1]));
    assert!(
        authored::mutation(command(&[
            "submit",
            "claims",
            "--json",
            &batch,
            "--claim-json",
            claim
        ]))
        .is_err()
    );
}

mod native_adapters {
    use super::*;
    use focal_client::operations::{
        NativeAuthoredOperation, NativeExposure, NativePayloadDocument, native_coverage_table,
    };

    fn native_document(args: &[&str]) -> NativeAuthoredOperation {
        match command(args) {
            Commands::Submit {
                command: SubmitCommand::Claim(args),
            } => NativeAuthoredOperation::ClaimSubmit(native_documents::claim(args).unwrap().0),
            Commands::Submit {
                command: SubmitCommand::Testament(args),
            } => NativeAuthoredOperation::TestamentSubmit(
                native_documents::testament(args).unwrap().0,
            ),
            Commands::Submit {
                command: SubmitCommand::Artifact(args),
            } => NativeAuthoredOperation::ArtifactSubmit(native_documents::work(args).unwrap().0),
            Commands::Artifact {
                command: ArtifactCommand::Submit(args),
            } => NativeAuthoredOperation::ArtifactSubmit(native_documents::work(*args).unwrap().0),
            Commands::Testament {
                command: TestamentCommand::Submit(args),
            } => NativeAuthoredOperation::TestamentSubmit(
                native_documents::testament(*args).unwrap().0,
            ),
            Commands::Artifact {
                command: ArtifactCommand::Diagnostic(args),
            } => NativeAuthoredOperation::ArtifactDiagnostic(
                native_documents::diagnostic(*args).unwrap().0,
            ),
            Commands::Testament {
                command: TestamentCommand::Post(args),
            } => NativeAuthoredOperation::TestamentPost(
                native_documents::response_target(args).unwrap().0,
            ),
            Commands::Validation {
                command: ValidationCommand::Begin(args),
            } => NativeAuthoredOperation::ValidationBegin(native_documents::begin(args).unwrap().0),
            Commands::Validation {
                command: ValidationCommand::Report(args),
            } => NativeAuthoredOperation::ValidationReport(
                native_documents::report(*args).unwrap().0,
            ),
            Commands::Receipt {
                command: ReceiptCommand::Acquire(args),
            } => {
                NativeAuthoredOperation::ReceiptAcquire(native_documents::receipt(args).unwrap().0)
            }
            Commands::Claim {
                command: ClaimCommand::Cancel(args),
            } => NativeAuthoredOperation::ClaimCancel(native_documents::cancel(args).unwrap().0),
            Commands::Claim {
                command: ClaimCommand::ReleaseScope(args),
            } => NativeAuthoredOperation::ClaimReleaseScope(
                native_documents::claim_target(args).unwrap().0,
            ),
            Commands::Receipt {
                command: ReceiptCommand::Adopt(args),
            } => NativeAuthoredOperation::ReceiptAdopt(native_documents::adopt(args).unwrap().0),
            Commands::Artifact {
                command: ArtifactCommand::Fail(args),
            } => NativeAuthoredOperation::ArtifactFail(native_documents::fail(args).unwrap().0),
            Commands::Artifact {
                command: ArtifactCommand::Receive(args),
            } => NativeAuthoredOperation::ArtifactReceive(
                native_documents::artifact_target(args).unwrap().0,
            ),
            Commands::Artifact {
                command: ArtifactCommand::Reject(args),
            } => {
                NativeAuthoredOperation::ArtifactReject(native_documents::reject(*args).unwrap().0)
            }
            Commands::Validation {
                command: ValidationCommand::SealIncrements(args),
            } => NativeAuthoredOperation::ValidationSealIncrements(
                native_documents::seal_increments(args).unwrap().0,
            ),
            Commands::Validation {
                command: ValidationCommand::EnterWholeWork(args),
            } => NativeAuthoredOperation::ValidationEnterWholeWork(
                native_documents::response_target(args).unwrap().0,
            ),
            Commands::Audit {
                command: AuditCommand::Generate(args),
            } => NativeAuthoredOperation::AuditGenerate(native_documents::audit(args).unwrap().0),
            Commands::Audit {
                command: AuditCommand::Post(args),
            } => {
                NativeAuthoredOperation::AuditPost(native_documents::audit_target(args).unwrap().0)
            }
            Commands::Monitor {
                command: super::super::monitor::MonitorCommand::Register(args),
            } => NativeAuthoredOperation::MonitorRegister(
                super::super::monitor::native_register(*args).unwrap().0,
            ),
            Commands::Monitor {
                command: super::super::monitor::MonitorCommand::Rebind(args),
            } => NativeAuthoredOperation::MonitorRebind(
                super::super::monitor::native_rebind(args).unwrap().0,
            ),
            Commands::Monitor {
                command: super::super::monitor::MonitorCommand::Cancel(args),
            } => NativeAuthoredOperation::MonitorCancel(
                super::super::monitor::native_cancel(args).unwrap().0,
            ),
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn the_remaining_native_verbs_adapt_flags_and_documents_identically() {
        let check = |flags: &[&str], name: &str| {
            let operation = native_document(flags);
            assert_eq!(operation.name(), name, "{flags:?}");
            let json = serde_json::to_string(&operation).unwrap();
            let json: serde_json::Value = serde_json::from_str(&json).unwrap();
            let body = json
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap()
                .to_string();
            let root: Vec<&str> = flags[..2].to_vec();
            let mut through_json = root.clone();
            through_json.extend(["--json", &body]);
            let parsed = native_document(&through_json);
            assert_eq!(
                parsed.canonical_intent().unwrap(),
                operation.canonical_intent().unwrap(),
                "{flags:?}"
            );
            operation
        };
        check(&["claim", "release-scope", ID], "claim.release_scope");
        let adopt = check(
            &["receipt", "adopt", ID, "--holder", "self"],
            "receipt.adopt",
        );
        let NativeAuthoredOperation::ReceiptAdopt(document) = adopt else {
            panic!()
        };
        assert_eq!(
            (document.claim.as_str(), document.holder.as_str()),
            (ID, "self")
        );
        let fail = check(
            &[
                "artifact",
                "fail",
                "--claim",
                ID,
                "--slot",
                "2",
                "--diagnostic",
                &format!("{OTHER}:{HASH}"),
            ],
            "artifact.fail",
        );
        let NativeAuthoredOperation::ArtifactFail(document) = fail else {
            panic!()
        };
        assert_eq!(document.slot, 2);
        assert_eq!(document.diagnostic, OTHER);
        assert_eq!(document.hash.as_deref(), Some(HASH));
        let NativeAuthoredOperation::ArtifactFail(unpinned) = native_document(&[
            "artifact",
            "fail",
            "--claim",
            ID,
            "--slot",
            "0",
            "--diagnostic",
            OTHER,
        ]) else {
            panic!()
        };
        assert_eq!(unpinned.hash, None);
        check(
            &["artifact", "receive", OTHER, "--claim", ID],
            "artifact.receive",
        );
        let reject = check(
            &[
                "artifact",
                "reject",
                OTHER,
                "--claim",
                ID,
                "--reason",
                "metadata",
                "--text",
                "{}",
                "--visibility",
                "team",
            ],
            "artifact.reject",
        );
        let NativeAuthoredOperation::ArtifactReject(document) = reject else {
            panic!()
        };
        assert_eq!(document.reason, "metadata");
        assert_eq!(document.visibility, ["team"]);
        check(
            &["validation", "seal-increments", "--claim", ID],
            "validation.seal_increments",
        );
        check(
            &["validation", "enter-whole-work", OTHER, "--claim", ID],
            "validation.enter_whole_work",
        );
        let begin = check(
            &[
                "validation",
                "begin",
                "--claim",
                ID,
                "--validation",
                OTHER,
                "--phase",
                "increment",
                "--target",
                OTHER,
            ],
            "validation.begin",
        );
        let NativeAuthoredOperation::ValidationBegin(document) = begin else {
            panic!()
        };
        assert_eq!(document.phase, "increment");
        assert_eq!(document.target.as_deref(), Some(OTHER));
        let NativeAuthoredOperation::ValidationReport(report) = native_document(&[
            "validation",
            "report",
            "--claim",
            ID,
            "--validation",
            OTHER,
            "--phase",
            "admission",
            "--verdict",
            "fail",
            "--text",
            "{}",
        ]) else {
            panic!()
        };
        assert_eq!(report.phase, "admission");
        check(&["audit", "generate", "--claim", ID], "audit.generate");
        check(&["audit", "post", OTHER], "audit.post");
        let monitor = check(
            &[
                "monitor",
                "register",
                "--owner",
                ID,
                "--root",
                &format!("satisfied:{OTHER}"),
                "--root",
                &format!("released:{ID}"),
                "--at",
                "4102444800000",
            ],
            "monitor.register",
        );
        let NativeAuthoredOperation::MonitorRegister(document) = monitor else {
            panic!()
        };
        assert_eq!(document.roots.len(), 2);
        assert_eq!(document.roots[1].predicate, "released");
        assert_eq!(document.deadline.generation, 1);
        assert_eq!(document.deadline.timer, None);
        check(
            &[
                "monitor",
                "rebind",
                OTHER,
                "--owner",
                ID,
                "--predecessor",
                OTHER,
                "--successor",
                ID,
            ],
            "monitor.rebind",
        );
        check(
            &["monitor", "cancel", OTHER, "--owner", ID],
            "monitor.cancel",
        );
        // Unknown predicates and missing deadlines are refused before compilation.
        let Commands::Monitor {
            command: super::super::monitor::MonitorCommand::Register(args),
        } = command(&[
            "monitor",
            "register",
            "--owner",
            ID,
            "--root",
            &format!("done:{OTHER}"),
            "--at",
            "5",
        ])
        else {
            panic!()
        };
        assert!(super::super::monitor::native_register(*args).is_err());
        let Commands::Monitor {
            command: super::super::monitor::MonitorCommand::Register(args),
        } = command(&[
            "monitor",
            "register",
            "--owner",
            ID,
            "--root",
            &format!("satisfied:{OTHER}"),
        ])
        else {
            panic!()
        };
        assert!(super::super::monitor::native_register(*args).is_err());
    }
    const ID: &str = "00000000000000000000000000000010";
    const OTHER: &str = "00000000000000000000000000000011";
    const HASH: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    #[test]
    fn native_flags_and_documents_compile_to_the_same_authored_operations() {
        let validation = r#"{"kind":"receipt","description":"Deliver.","deadline":{"at":10}}"#;
        let flags = native_document(&[
            "submit",
            "claim",
            "--description",
            "Do the work.",
            "--target",
            ID,
            "--scope",
            "file:src/lib.rs",
            "--relation",
            &format!("reviews:{OTHER}"),
            "--validation-json",
            validation,
            "--slot-json",
            r#"{"slot":0}"#,
            "--parent",
            OTHER,
            "--max-responses",
            "2",
        ]);
        let NativeAuthoredOperation::ClaimSubmit(document) = &flags else {
            panic!()
        };
        assert_eq!(document.relations[0].target, format!("claim:{OTHER}"));
        assert_eq!(document.parent.as_deref(), Some(OTHER));
        assert_eq!(document.max_responses, 2);
        assert_eq!(document.slots.len(), 1);
        // The same document through --json yields identical canonical intent.
        let json = serde_json::to_string(document).unwrap();
        let parsed = native_document(&["submit", "claim", "--json", &json]);
        assert_eq!(
            parsed.canonical_intent().unwrap(),
            flags.canonical_intent().unwrap()
        );
        // Field flags and a document never merge.
        let Commands::Submit {
            command: SubmitCommand::Claim(args),
        } = command(&["submit", "claim", "--json", &json, "--description", "x"])
        else {
            panic!()
        };
        assert!(native_documents::claim(args).is_err());

        let testament = native_document(&[
            "submit",
            "testament",
            "--claim",
            ID,
            "--summary",
            "Done.",
            "--confidence",
            "committed",
            "--outcome",
            "complete",
            "--slot",
            &format!("0={OTHER}:{HASH}"),
            "--diagnostic",
            &format!("{OTHER}:{HASH}"),
        ]);
        let NativeAuthoredOperation::TestamentSubmit(document) = testament else {
            panic!()
        };
        assert_eq!(document.manifest[0].slot, 0);
        assert_eq!(document.diagnostics[0].id, OTHER);
        // V1-only fences are refused, not silently dropped.
        let Commands::Submit {
            command: SubmitCommand::Testament(args),
        } = command(&[
            "submit",
            "testament",
            "--claim",
            ID,
            "--summary",
            "Done.",
            "--confidence",
            "committed",
            "--outcome",
            "complete",
            "--receipt",
            OTHER,
            "--receipt-epoch",
            "1",
        ])
        else {
            panic!()
        };
        assert!(native_documents::testament(args).is_err());

        let work = native_document(&[
            "artifact",
            "submit",
            "--claim",
            ID,
            "--slot",
            "1",
            "--text",
            "{}",
            "--visibility",
            "team",
        ]);
        let NativeAuthoredOperation::ArtifactSubmit(document) = work else {
            panic!()
        };
        assert_eq!(document.slot, 1);
        assert!(matches!(
            document.payload,
            NativePayloadDocument::Text { .. }
        ));
        let diagnostic = native_document(&[
            "artifact",
            "diagnostic",
            "--claim",
            ID,
            "--reason",
            "work",
            "--text",
            "{}",
        ]);
        assert!(matches!(
            diagnostic,
            NativeAuthoredOperation::ArtifactDiagnostic(_)
        ));
        let post = native_document(&["testament", "post", OTHER, "--claim", ID]);
        let NativeAuthoredOperation::TestamentPost(document) = post else {
            panic!()
        };
        assert_eq!(
            (document.claim.as_str(), document.testament.as_str()),
            (ID, OTHER)
        );
        let begin = native_document(&[
            "validation",
            "begin",
            "--claim",
            ID,
            "--validation",
            OTHER,
            "--slot",
            "0",
        ]);
        let NativeAuthoredOperation::ValidationBegin(document) = begin else {
            panic!()
        };
        assert_eq!(document.slot, Some(0));
        let report = native_document(&[
            "validation",
            "report",
            "--claim",
            ID,
            "--validation",
            OTHER,
            "--verdict",
            "pass",
            "--text",
            "{}",
        ]);
        assert!(matches!(
            report,
            NativeAuthoredOperation::ValidationReport(_)
        ));
        let acquire = native_document(&["receipt", "acquire", ID]);
        assert!(matches!(
            acquire,
            NativeAuthoredOperation::ReceiptAcquire(_)
        ));
        let cancel = native_document(&["claim", "cancel", ID]);
        assert!(matches!(cancel, NativeAuthoredOperation::ClaimCancel(_)));
        let Commands::Claim {
            command: ClaimCommand::Cancel(args),
        } = command(&["claim", "cancel", ID, "--reason", "no"])
        else {
            panic!()
        };
        assert!(native_documents::cancel(args).is_err());
    }

    #[test]
    fn every_exposed_coverage_row_names_a_command_in_the_clap_tree() {
        let tree = super::super::command_tree::command();
        for row in native_coverage_table() {
            if row.exposure != NativeExposure::AuthoredTool {
                assert!(row.cli.is_empty());
                continue;
            }
            let mut path = row.cli.split(' ');
            assert_eq!(path.next(), Some("focal"));
            let mut node = &tree;
            for segment in path {
                node = node
                    .get_subcommands()
                    .find(|sub| sub.get_name() == segment)
                    .unwrap_or_else(|| panic!("{}: no subcommand {segment}", row.cli));
            }
            assert!(node.get_subcommands().next().is_none(), "{}", row.cli);
        }
    }
}
