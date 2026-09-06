use super::*;
use clap::Parser;
use focal_model::{LedgerId, ParticipantId, RootCommandId, SessionId, TenantId};

fn authored(args: &[&str]) -> Result<(AuthoredOperation, MutationOptions)> {
    let parsed = crate::Args::try_parse_from(std::iter::once("focal").chain(args.iter().copied()))
        .map_err(|error| CliError::Input(error.to_string()))?;
    let crate::Commands::Manual(command) = parsed.command else {
        panic!("manual command");
    };
    super::super::authored::mutation(*command)
}
fn context() -> BuildContext {
    BuildContext {
        ledger: LedgerId {
            tenant: TenantId([1; 16]),
            session: SessionId([2; 16]),
        },
        actor: ParticipantId([3; 16]),
        root: RootCommandId([4; 16]),
        policy_revision: 1,
    }
}

#[test]
fn peer_validation_flags_json_yaml_preserve_exact_fences_and_canonical_intent() {
    let id = "00000000000000000000000000000001";
    let hash = "1111111111111111111111111111111111111111111111111111111111111111";
    let evidence = format!("{id}:{hash}");
    let (flags, options) = authored(&[
        "submit",
        "validation",
        "--validation",
        id,
        "--target-hash",
        hash,
        "--phase",
        "whole_work",
        "--epoch",
        "2",
        "--handler",
        id,
        "--handler-version",
        hash,
        "--agentic=false",
        "--attempt",
        "0",
        "--manifest",
        hash,
        "--receipt",
        id,
        "--receipt-epoch",
        "3",
        "--value",
        "fail",
        "--evidence",
        &evidence,
        "--expected-revision",
        "9",
    ])
    .unwrap();
    assert_eq!(options.expected_revision, Some(9));
    flags.preflight(&context()).unwrap();
    let canonical: serde_json::Value =
        serde_json::from_slice(&flags.canonical_intent().unwrap()).unwrap();
    let json = serde_json::to_string(&canonical["input"]).unwrap();
    let yaml = format!(
        "validation: '{id}'\ntarget_hash: '{hash}'\nphase: whole_work\nepoch: 2\nhandler:\n  id: '{id}'\n  version: '{hash}'\n  agentic: false\nattempt: 0\nmanifest: '{hash}'\nreceipt:\n  id: '{id}'\n  epoch: 3\nvalue: fail\nevidence:\n  - id: '{id}'\n    hash: '{hash}'\n"
    );
    for (flag, document) in [("--json", json.as_str()), ("--yaml", yaml.as_str())] {
        let (value, _) = authored(&["submit", "validation", flag, document]).unwrap();
        assert_eq!(
            value.canonical_intent().unwrap(),
            flags.canonical_intent().unwrap()
        );
    }
    let AuthoredOperation::ValidationSubmit(document) = flags else {
        panic!("verdict");
    };
    assert_eq!(document.receipt.unwrap().epoch, 3);
    assert_eq!(document.value, "fail");
}

#[test]
fn register_and_supersede_reuse_shared_authored_documents() {
    let schema = focal_evidence::test_report_schema().to_string();
    let payload = r#"{"passed":1,"failed":0,"skipped":0}"#;
    let (registered, _) = authored(&[
        "artifact",
        "register",
        "--kind",
        "test-report",
        "--schema-hash",
        &schema,
        "--text",
        payload,
    ])
    .unwrap();
    registered.preflight(&context()).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_slice(&registered.canonical_intent().unwrap()).unwrap();
    let document = serde_json::to_string(&parsed["input"]).unwrap();
    assert_eq!(
        authored(&["artifact", "register", "--json", &document])
            .unwrap()
            .0,
        registered
    );
    let predecessor = "00000000000000000000000000000009";
    let receipt = r#"{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive corrected response","evaluator":"self"}"#;
    let (successor, _) = authored(&[
        "claim",
        "supersede",
        predecessor,
        "--target",
        "self",
        "--action",
        "handoff",
        "--description",
        "Corrected report",
        "--validation-json",
        receipt,
    ])
    .unwrap();
    successor.preflight(&context()).unwrap();
    let AuthoredOperation::ClaimSupersede(value) = &successor else {
        panic!("successor");
    };
    assert_eq!(value.predecessor, predecessor);
    let document = serde_json::to_string(&value.successor).unwrap();
    assert_eq!(
        authored(&["claim", "supersede", predecessor, "--json", &document])
            .unwrap()
            .0,
        successor
    );
}

#[test]
fn peer_verbs_do_not_invent_revision_or_trusted_authority_and_document_conflicts_fail() {
    let id = "00000000000000000000000000000001";
    for (args, name) in [
        (
            vec!["testament", "receive", id, "--claim", id],
            "testament.receive",
        ),
        (
            vec!["validation", "begin", "--claim", id],
            "validation.begin",
        ),
        (
            vec!["validation", "complete", "--claim", id],
            "validation.complete",
        ),
    ] {
        let (value, options) = authored(&args).unwrap();
        assert_eq!(value.descriptor().name, name);
        assert_eq!(options.expected_revision, None);
        value.preflight(&context()).unwrap();
    }
    assert!(authored(&["submit", "validation", "--json", "{}", "--agentic=false"]).is_err());
    assert!(authored(&["submit", "validation", "--receipt", id]).is_err());
    assert!(
        authored(&[
            "artifact",
            "register",
            "--json",
            "{}",
            "--visibility",
            "private"
        ])
        .is_err()
    );
    assert!(authored(&["artifact", "register", "--json", r#"{"runtime":true}"#]).is_err());
    assert!(
        authored(&[
            "claim",
            "supersede",
            id,
            "--json",
            "{}",
            "--description",
            "not merged"
        ])
        .is_err()
    );
}

#[test]
fn scalar_lifecycle_flags_json_yaml_and_files_have_identical_authored_and_wire_intent() {
    let id = "00000000000000000000000000000001";
    let second = "00000000000000000000000000000002";
    let cases: Vec<(Vec<&str>, Vec<&str>)> = vec![
        (vec!["claim", "post", id], vec!["claim", "post"]),
        (
            vec![
                "claim",
                "progress",
                id,
                "--receipt",
                second,
                "--receipt-epoch",
                "3",
                "--message",
                "",
            ],
            vec!["claim", "progress"],
        ),
        (
            vec!["claim", "cancel", id, "--reason", "Changed plan"],
            vec!["claim", "cancel"],
        ),
        (vec!["receipt", "acquire", id], vec!["receipt", "acquire"]),
        (
            vec![
                "evidence",
                "begin",
                "--claim",
                id,
                "--receipt",
                second,
                "--receipt-epoch",
                "3",
            ],
            vec!["evidence", "begin"],
        ),
        (
            vec!["testament", "receive", second, "--claim", id],
            vec!["testament", "receive"],
        ),
        (
            vec!["validation", "begin", "--claim", id],
            vec!["validation", "begin"],
        ),
        (
            vec!["validation", "complete", "--claim", id],
            vec!["validation", "complete"],
        ),
    ];
    for (flags, base) in cases {
        let (operation, _) = authored(&flags).unwrap();
        let intent = operation.canonical_intent().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&intent).unwrap();
        let json = serde_json::to_string(&value["input"]).unwrap();
        let yaml = serde_saphyr::to_string(&value["input"]).unwrap();
        let file = tempfile::Builder::new().suffix(".yaml").tempfile().unwrap();
        std::fs::write(file.path(), &yaml).unwrap();
        let reference = operation
            .clone()
            .build(&context(), &mut || Ok([7; 16]))
            .unwrap()
            .into_wire(Some(focal_model::ObjectRevision(9)))
            .unwrap();
        for (mode, document) in [
            ("--json", json.as_str()),
            ("--yaml", yaml.as_str()),
            ("--file", file.path().to_str().unwrap()),
        ] {
            let mut args = base.clone();
            args.extend([mode, document, "--expected-revision", "9"]);
            let (parsed, options) = authored(&args).unwrap();
            assert_eq!(options.expected_revision, Some(9));
            assert_eq!(
                parsed.canonical_intent().unwrap(),
                intent,
                "{base:?} {mode}"
            );
            let wire = parsed
                .build(&context(), &mut || Ok([7; 16]))
                .unwrap()
                .into_wire(Some(focal_model::ObjectRevision(9)))
                .unwrap();
            assert_eq!(
                postcard::to_stdvec(&wire).unwrap(),
                postcard::to_stdvec(&reference).unwrap()
            );
        }
        let mut mixed = flags.clone();
        mixed.extend(["--json", &json]);
        assert!(authored(&mixed).is_err(), "{mixed:?}");
        let mut unknown = base.clone();
        unknown.extend(["--json", "{\"runtime\":true}"]);
        assert!(authored(&unknown).is_err());
        let mut duplicate = base.clone();
        duplicate.extend(["--json", "{\"claim\":\"a\",\"claim\":\"b\"}"]);
        assert!(authored(&duplicate).is_err());
    }
}
