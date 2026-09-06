use super::*;

fn verify(bytes: &[u8]) -> Result<(), BuiltinSchemaError> {
    verify_builtin_schema(error_report_schema(), bytes)
}

#[test]
fn builtins_have_fixed_distinct_descriptors_and_bounds() {
    assert_eq!(
        crate::TEST_REPORT_SCHEMA,
        br#"focal.test_report.v1:{passed:u64,failed:u64,skipped:u64};deny_unknown_fields"#
    );
    assert_eq!(ERROR_REPORT_SCHEMA,
        br#"focal.error_report.v1:{code:string[utf8_bytes=1..128,nonblank],message:string[utf8_bytes=1..4096,nonblank],details?:string[utf8_bytes=0..32768]|null};blank_codepoints=0009-000d,0020,0085,00a0,1680,2000-200a,2028-2029,202f,205f,3000;json_bytes<=65536;deny_unknown_fields;deny_duplicate_fields"#);
    assert_ne!(test_report_schema(), error_report_schema());
    assert_eq!(builtin_schema_limit(test_report_schema()), Ok(1024 * 1024));
    assert_eq!(builtin_schema_limit(error_report_schema()), Ok(65536));
    assert_eq!(
        builtin_schema_limit(ContentHash::default()),
        Err(BuiltinSchemaError::Unsupported)
    );
    assert_eq!(
        verify_builtin_schema(ContentHash::default(), &vec![0; 65537]),
        Err(BuiltinSchemaError::Unsupported)
    );
}

#[test]
fn error_reports_accept_language_agnostic_accounts_without_invented_results() {
    for input in [
        br#"{"code":"tool_unavailable","message":"The tool could not run"}"#.as_slice(),
        br#"{"message":"Work was refused","details":null,"code":"refused"}"#,
        br#"{"code":"interrupted","message":"Caller stopped the attempt","details":""}"#,
        br#"{"code":"unavailable","message":"A quoted \"name\" and\na newline","details":"\u03bb"}"#,
    ] {
        assert_eq!(verify(input), Ok(()));
    }
    assert!(
        verify_builtin_schema(
            test_report_schema(),
            br#"{"code":"tool_unavailable","message":"No report"}"#
        )
        .is_err()
    );
    assert!(verify(br#"{"passed":0,"failed":1,"skipped":0}"#).is_err());
}

#[test]
fn error_reports_reject_omissions_unknown_duplicates_blank_and_wrong_types() {
    for input in [
        br#"{}"#.as_slice(),
        br#"{"code":"missing_message"}"#,
        br#"{"message":"missing code"}"#,
        br#"{"code":"","message":"reason"}"#,
        br#"{"code":"x","message":" \t\r\n\u2003\u00a0"}"#,
        br#"{"code":"x","message":null}"#,
        br#"{"code":1,"message":"reason"}"#,
        br#"{"code":"x","message":"reason","details":{}}"#,
        br#"{"code":"x","message":"reason","success":true}"#,
        br#"{"code":"x","message":"reason","verdict":"pass"}"#,
        br#"{"code":"x","code":"y","message":"reason"}"#,
        br#"{"code":"x","message":"reason","message":"other"}"#,
        br#"{"code":"x","message":"reason","details":null,"details":null}"#,
        br#"{"code":"x","message":"reason"}{}"#,
        br#"[]"#,
        br#"["tool_unavailable","The tool could not run",null]"#,
        br#"["tool_unavailable","The tool could not run"]"#,
        b"\xff",
    ] {
        assert_eq!(verify(input), Err(BuiltinSchemaError::Invalid), "{input:?}");
    }
}

#[test]
fn error_text_bounds_count_decoded_utf8_bytes_not_characters_or_escapes() {
    let baseline = serde_json::json!({"code":"c".repeat(128),"message":"m".repeat(4096),"details":"d".repeat(32768)});
    assert_eq!(verify(&serde_json::to_vec(&baseline).unwrap()), Ok(()));
    for (name, limit) in [("code", 128), ("message", 4096), ("details", 32768)] {
        let mut value = baseline.clone();
        value[name] = serde_json::Value::String("x".repeat(limit + 1));
        assert_eq!(
            verify(&serde_json::to_vec(&value).unwrap()),
            Err(BuiltinSchemaError::Invalid),
            "{name}"
        );
    }
    let mut value = serde_json::json!({"code":"é".repeat(64),"message":"reason"});
    assert_eq!(verify(&serde_json::to_vec(&value).unwrap()), Ok(()));
    value["code"] = serde_json::Value::String("é".repeat(65));
    assert_eq!(
        verify(&serde_json::to_vec(&value).unwrap()),
        Err(BuiltinSchemaError::Invalid)
    );
    let escaped = format!(
        r#"{{"code":"{}","message":"reason"}}"#,
        "\\u00e9".repeat(64)
    );
    assert_eq!(verify(escaped.as_bytes()), Ok(()));
}

#[test]
fn total_byte_preflight_preserves_original_test_report_whitespace_acceptance() {
    for (schema, report, maximum) in [
        (
            test_report_schema(),
            br#"{"passed":0,"failed":0,"skipped":0}"#.as_slice(),
            TEST_REPORT_MAX_BYTES,
        ),
        (
            error_report_schema(),
            br#"{"code":"x","message":"reason"}"#.as_slice(),
            ERROR_REPORT_MAX_BYTES,
        ),
    ] {
        let mut bytes = report.to_vec();
        bytes.resize(maximum, b' ');
        assert_eq!(verify_builtin_schema(schema, &bytes), Ok(()));
        bytes.push(b' ');
        assert_eq!(
            verify_builtin_schema(schema, &bytes),
            Err(BuiltinSchemaError::Capacity)
        );
    }
    for report in [
        br#"{"passed":18446744073709551615,"failed":1,"skipped":0}"#.as_slice(),
        br#"{"skipped":99,"failed":0,"passed":0}"#,
        b"[1,0,0]",
    ] {
        assert_eq!(verify_builtin_schema(test_report_schema(), report), Ok(()));
    }
    for report in [
        br#"{"passed":1,"failed":0}"#.as_slice(),
        br#"{"passed":1,"failed":0,"skipped":0,"extra":1}"#,
        br#"{"passed":1,"passed":2,"failed":0,"skipped":0}"#,
    ] {
        assert_eq!(
            verify_builtin_schema(test_report_schema(), report),
            Err(BuiltinSchemaError::Invalid)
        );
    }
}

#[test]
fn packaged_mcp_only_diagnostic_contract_pins_the_real_hash_and_valid_example() {
    let instructions = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../skills/references/workflow-contract.md"
    ));
    let section = instructions
        .split_once("### Built-in error-report v1\n")
        .unwrap()
        .1;
    let hash = section
        .split_once("Schema hash: `")
        .unwrap()
        .1
        .split_once('`')
        .unwrap()
        .0;
    assert_eq!(hash, error_report_schema().to_string());
    let example = section
        .split_once("```json\n")
        .unwrap()
        .1
        .split_once("\n```")
        .unwrap()
        .0;
    assert!(example.len() <= 16 * 1024);
    assert_eq!(verify(example.as_bytes()), Ok(()));
    let document: serde_json::Value = serde_json::from_str(example).unwrap();
    assert_eq!(document["code"], "tool_unavailable");
    assert!(document.get("passed").is_none());
    assert!(document.get("failed").is_none());
}
