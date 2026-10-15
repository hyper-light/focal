use super::*;
use focal_client::input::BuildContext;
use focal_client::operations::example_ids;
use focal_model::{LedgerId, ParticipantId, RootCommandId, SessionId, TenantId};
use serde_json::Value;

fn document(value: &Value) -> super::super::args::DocumentInput {
    super::super::args::DocumentInput {
        json: Some(value.to_string()),
        ..Default::default()
    }
}

#[test]
fn every_engine_example_is_served_by_offline_selection_and_builds_through_its_engine() {
    let context = BuildContext {
        ledger: LedgerId {
            tenant: TenantId([1; 16]),
            session: SessionId([2; 16]),
        },
        actor: ParticipantId([3; 16]),
        root: RootCommandId([4; 16]),
        policy_revision: 1,
    };
    for wire in [WireProfile::V1, WireProfile::Native] {
        let native = wire == WireProfile::Native;
        for descriptor in operations::application(wire) {
            let value = example(native, descriptor.name).unwrap();
            assert_eq!(value, operations::example(wire, descriptor.name).unwrap());
            let bytes = serde_json::to_vec(&value).unwrap();
            match operations::decode_application(wire, descriptor.name, &bytes).unwrap() {
                ApplicationDocument::V1(authored) => {
                    authored
                        .preflight(&context)
                        .unwrap_or_else(|error| panic!("{}: {error}", descriptor.name));
                    assert_eq!(authored.descriptor().name, descriptor.name);
                }
                decoded => assert_eq!(decoded.name(), descriptor.name),
            }
            // Shape-only validation accepts exactly what example printed.
            validate_offline(descriptor.name, document(&value), native)
                .unwrap_or_else(|error| panic!("{}: {error}", descriptor.name));
        }
    }
    // Without --native a shared name is the V1 example and a native-only name
    // the native one; with it a shared name is the native example.
    assert_eq!(example(false, "claim.submit").unwrap()["target"], "self");
    assert_eq!(
        example(false, "claim.challenge").unwrap()["target"],
        example_ids::SUBJECT
    );
    assert_eq!(
        example(true, "claim.submit").unwrap()["target"],
        example_ids::SUBJECT
    );
    assert!(
        example(true, "claim.submit").unwrap()["validations"][0]
            .get("evidence_schemas")
            .is_none()
    );
    // A V1-only name under --native is an explicit refusal, not a redirect.
    let refused = example(true, "claim.submit_batch").unwrap_err();
    assert!(
        matches!(
            refused,
            CliError::Input(ref message)
                if message == "claim.submit_batch is not available on the native engine yet; it arrives with the native index families"
        ),
        "{refused}"
    );
    let batch = example(false, "claim.submit_batch").unwrap();
    assert!(matches!(
        validate_offline("claim.submit_batch", document(&batch), true),
        Err(CliError::Input(_))
    ));
    // The V1 claim example is refused by the native engine's decoder.
    let v1 = example(false, "claim.submit").unwrap();
    assert!(matches!(
        validate_offline("claim.submit", document(&v1), true),
        Err(CliError::Document(_))
    ));
    assert!(example(false, "not.a.released.operation").is_err());
    assert!(example(true, "not.a.released.operation").is_err());
}

#[test]
fn discovery_schema_is_exact_registry_projection_and_builtins_stay_compatible() {
    for wire in [WireProfile::V1, WireProfile::Native] {
        let native = wire == WireProfile::Native;
        let mut bytes = Vec::new();
        list(wire, OutputFormat::Json, &mut bytes).unwrap();
        let catalog: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(catalog["engine"], engine_name(wire));
        let entries = catalog["operations"].as_array().unwrap();
        assert_eq!(entries.len(), operations::application(wire).len());
        assert_eq!(entries.len(), if native { 44 } else { 34 });
        for entry in entries {
            assert_eq!(entry["engine"], engine_name(wire), "{entry}");
            assert_eq!(entry["version"], if native { 2 } else { 1 }, "{entry}");
            assert_eq!(entry["example_available"], true, "{entry}");
        }
        let mut table = Vec::new();
        list(wire, OutputFormat::Table, &mut table).unwrap();
        let table = String::from_utf8(table).unwrap();
        assert!(table.contains("ENGINE"));
        assert_eq!(table.contains("claim.challenge"), native);
        assert_eq!(table.contains("claim.submit_batch"), !native);
        for descriptor in operations::application(wire) {
            for (direction, expected) in [
                (Direction::Input, descriptor.input_schema().unwrap()),
                (Direction::Output, descriptor.output_schema().unwrap()),
            ] {
                let mut bytes = Vec::new();
                get(descriptor.name, Some(direction), native, &mut bytes).unwrap();
                let actual: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(actual, expected, "{}", descriptor.name);
            }
        }
    }
    // A shared name selects V1 without --native and native with it; a
    // native-only name needs no flag.
    let mut bytes = Vec::new();
    get("claim.submit", None, false, &mut bytes).unwrap();
    let v1: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v1["$id"], "urn:focal:operation:claim.submit:input:1");
    let mut bytes = Vec::new();
    get("claim.challenge", None, false, &mut bytes).unwrap();
    let challenge: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        challenge["$id"],
        "urn:focal:operation:claim.challenge:input:2"
    );
    assert!(matches!(
        get("claim.submit_batch", None, true, &mut Vec::new()),
        Err(CliError::Input(_))
    ));
    let mut bytes = Vec::new();
    get("test-report", None, false, &mut bytes).unwrap();
    let schema: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        schema["hash"],
        focal_evidence::test_report_schema().to_string()
    );
    assert_eq!(
        schema["example"],
        serde_json::json!({"passed":1,"failed":0,"skipped":0})
    );
    assert!(
        get(
            "test-report",
            Some(Direction::Output),
            false,
            &mut Vec::new()
        )
        .is_err()
    );
    assert!(get("test-report", None, true, &mut Vec::new()).is_err());
    // The coverage table says which descriptors have an example: all of them.
    let mut bytes = Vec::new();
    coverage(OutputFormat::Json, &mut bytes).unwrap();
    let table: Value = serde_json::from_slice(&bytes).unwrap();
    for row in table["operations"].as_array().unwrap() {
        assert_eq!(row["example"], !row["descriptor"].is_null(), "{row}");
    }
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
            // Names of both catalogues complete for example and validate.
            assert!(script.contains("claim.challenge"), "{shell:?}");
            assert!(script.contains("claim.submit_batch"), "{shell:?}");
        }
        assert!(script.contains("operation-id"), "{shell:?}");
        assert!(script.contains("input-format"), "{shell:?}");
        assert!(script.contains("native"), "{shell:?}");
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
    get("error-report", None, false, &mut bytes).unwrap();
    let schema: Value = serde_json::from_slice(&bytes).unwrap();
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
    assert!(
        get(
            "error-report",
            Some(Direction::Input),
            false,
            &mut Vec::new()
        )
        .is_err()
    );
    let mut bytes = Vec::new();
    list(WireProfile::V1, OutputFormat::Json, &mut bytes).unwrap();
    let catalog: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        catalog["builtins"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "error-report")
    );
}
