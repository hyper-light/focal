use super::*;
use focal_client::input::BuildContext;
use focal_model::{LedgerId, ParticipantId, RootCommandId, SessionId, TenantId};

#[test]
fn available_examples_roundtrip_and_build_through_real_authored_contracts() {
    let context = BuildContext {
        ledger: LedgerId {
            tenant: TenantId([1; 16]),
            session: SessionId([2; 16]),
        },
        actor: ParticipantId([3; 16]),
        root: RootCommandId([4; 16]),
        policy_revision: 1,
    };
    for descriptor in operations::descriptors() {
        if raw_example(descriptor.name).is_none() {
            continue;
        }
        let value = example(descriptor.name).unwrap();
        let bytes = serde_json::to_vec(&value).unwrap();
        let authored = operations::parse_json(descriptor.name, &bytes).unwrap();
        authored
            .preflight(&context)
            .unwrap_or_else(|error| panic!("{}: {error}", descriptor.name));
        assert_eq!(authored.descriptor().name, descriptor.name);
        assert_eq!(value, example(descriptor.name).unwrap());
        let canonical = authored.canonical_intent().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&canonical).unwrap();
        assert_eq!(parsed["input"], value);
    }
    assert!(example("not.a.released.operation").is_err());
}

#[test]
fn discovery_schema_is_exact_registry_projection_and_builtins_stay_compatible() {
    let mut bytes = Vec::new();
    list(OutputFormat::Json, &mut bytes).unwrap();
    let catalog: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        catalog["operations"].as_array().unwrap().len(),
        operations::descriptors().len()
    );
    for descriptor in operations::descriptors() {
        for (direction, expected) in [
            (Direction::Input, descriptor.input_schema().unwrap()),
            (Direction::Output, descriptor.output_schema().unwrap()),
        ] {
            let mut bytes = Vec::new();
            get(descriptor.name, Some(direction), &mut bytes).unwrap();
            let actual: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(actual, expected);
        }
    }
    let mut bytes = Vec::new();
    get("test-report", None, &mut bytes).unwrap();
    let schema: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        schema["hash"],
        focal_evidence::test_report_schema().to_string()
    );
    assert_eq!(
        schema["example"],
        serde_json::json!({"passed":1,"failed":0,"skipped":0})
    );
    assert!(get("test-report", Some(Direction::Output), &mut Vec::new()).is_err());
}

struct Broken;
impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed output"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn completion_uses_actual_tree_and_output_errors_remain_fallible() {
    for shell in Shell::value_variants() {
        let mut bytes = LimitedWriter::new(Vec::new(), MAX_DISCOVERY_BYTES);
        completion_to(*shell, &mut bytes).unwrap();
        let script = String::from_utf8(bytes.inner).unwrap();
        assert!(script.contains("schema"), "{shell:?}");
        assert!(script.contains("completion"), "{shell:?}");
        if matches!(shell, Shell::Bash | Shell::Zsh) {
            assert!(script.contains("validation.context"), "{shell:?}");
            assert!(script.contains("error-report"), "{shell:?}");
        }
        assert!(script.contains("operation-id"), "{shell:?}");
        assert!(script.contains("input-format"), "{shell:?}");
        let error = completion_to(*shell, &mut Broken).unwrap_err();
        assert!(
            matches!(error, CliError::Io(ref error) if error.kind() == io::ErrorKind::BrokenPipe)
        );
    }
    let mut bounded = LimitedWriter::new(Vec::new(), 3);
    bounded.write_all(b"abc").unwrap();
    assert!(bounded.write_all(b"d").is_err());
    assert_eq!(bounded.inner, b"abc");
    assert!(completion_to(Shell::Bash, &mut LimitedWriter::new(Vec::new(), 1)).is_err());
}

#[test]
fn error_schema_discovery_exposes_exact_pinned_contract_and_valid_example() {
    let mut bytes = Vec::new();
    get("error-report", None, &mut bytes).unwrap();
    let schema: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(schema["name"], "focal.error_report.v1");
    assert_eq!(
        schema["hash"],
        focal_evidence::error_report_schema().to_string()
    );
    assert_eq!(
        schema["descriptor"].as_str().unwrap().as_bytes(),
        focal_evidence::ERROR_REPORT_SCHEMA
    );
    assert_eq!(schema["max_payload_bytes"], 65536);
    focal_evidence::verify_builtin_schema(
        focal_evidence::error_report_schema(),
        &serde_json::to_vec(&schema["example"]).unwrap(),
    )
    .unwrap();
    assert!(get("error-report", Some(Direction::Input), &mut Vec::new()).is_err());
    let mut bytes = Vec::new();
    list(OutputFormat::Json, &mut bytes).unwrap();
    let catalog: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        catalog["builtins"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "error-report")
    );
}
