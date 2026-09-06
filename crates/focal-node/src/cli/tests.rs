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
            | ListCommand::Validations(args) => args,
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
    // Even an incomplete dangling marker is evidence of joined ownership.
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
