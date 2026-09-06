use super::*;

#[test]
fn saved_cli_upload_inspection_and_unknown_cancel_preserve_exact_identity_across_restart() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let server = start(root.path());
    let operation = cli(root.path(), &["request", "reserve"])["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(server);
    let source = root.path().join("proof source.json");
    std::fs::write(&source, payload()).unwrap();
    let pending = run(
        root.path(),
        &[
            "artifact",
            "register",
            "--kind",
            "test-report",
            "--schema-hash",
            &focal_evidence::test_report_schema().to_string(),
            "--payload-file",
            source.to_str().unwrap(),
            "--operation-id",
            &operation,
            "--format",
            "json",
        ],
    );
    assert_eq!(
        pending.status.code(),
        Some(7),
        "{}",
        String::from_utf8_lossy(&pending.stderr)
    );
    let pending: Value = serde_json::from_slice(&pending.stdout).unwrap();
    let upload = pending["upload_id"].as_str().unwrap();
    std::fs::remove_file(&source).unwrap();
    let state = root
        .path()
        .join("CLI.uploads")
        .join(upload)
        .join("upload.bin");
    let before = std::fs::read(&state).unwrap();
    let inspected = cli(root.path(), &["artifact", "upload", "inspect", upload]);
    assert_eq!(inspected["condition"], "Uploading");
    assert_eq!(inspected["cancel_requested"], false);
    assert_eq!(inspected["progress"]["received"], 0);
    assert_eq!(std::fs::read(&state).unwrap(), before);
    let yaml = run(
        root.path(),
        &["artifact", "upload", "inspect", upload, "--format", "yaml"],
    );
    assert!(yaml.status.success());
    let yaml: Value = serde_saphyr::from_str(std::str::from_utf8(&yaml.stdout).unwrap()).unwrap();
    assert_eq!(yaml, inspected);
    assert_eq!(std::fs::read(&state).unwrap(), before);
    let cancel = run(
        root.path(),
        &["artifact", "upload", "cancel", upload, "--format", "json"],
    );
    assert_eq!(
        cancel.status.code(),
        Some(7),
        "{}",
        String::from_utf8_lossy(&cancel.stderr)
    );
    let text = String::from_utf8(cancel.stderr).unwrap();
    assert!(text.contains("--data-dir"));
    assert!(text.contains(root.path().to_str().unwrap()));
    assert!(text.contains("--client-context 'local'"));
    assert!(text.contains(&format!("artifact upload cancel {upload} --origin cli")));
    let result: Value = serde_json::from_slice(&cancel.stdout).unwrap();
    assert_eq!(result["condition"], "CancelPending");
    assert_eq!(result["cancel_requested"], true);
    assert_eq!(result["progress"]["cancel_acknowledged"], false);
    let cancelled_bytes = std::fs::read(&state).unwrap();
    let again = run(
        root.path(),
        &["artifact", "upload", "cancel", upload, "--format", "json"],
    );
    assert_eq!(again.status.code(), Some(7));
    assert_eq!(std::fs::read(&state).unwrap(), cancelled_bytes);
    // A failed diagnostic sink cannot replace the unknown cancellation with an
    // unrelated I/O exit or change its exact saved request.
    let (writer, reader) = std::os::unix::net::UnixStream::pair().unwrap();
    drop(reader);
    let writer: std::os::fd::OwnedFd = writer.into();
    let no_stderr = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root.path())
        .args(["artifact", "upload", "cancel", upload, "--format", "json"])
        .stderr(writer)
        .output()
        .unwrap();
    assert_eq!(no_stderr.status.code(), Some(7));
    assert_eq!(
        serde_json::from_slice::<Value>(&no_stderr.stdout).unwrap()["condition"],
        "CancelPending"
    );
    assert_eq!(std::fs::read(&state).unwrap(), cancelled_bytes);
    assert_eq!(
        cli(root.path(), &["artifact", "upload", "inspect", upload])["condition"],
        "CancelPending"
    );
    let (writer, reader) = std::os::unix::net::UnixStream::pair().unwrap();
    drop(reader);
    let writer: std::os::fd::OwnedFd = writer.into();
    let no_stdout = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root.path())
        .args(["artifact", "upload", "cancel", upload, "--format", "json"])
        .stdout(writer)
        .output()
        .unwrap();
    assert_eq!(no_stdout.status.code(), Some(7));
    assert!(String::from_utf8_lossy(&no_stdout.stderr).contains("outcome_unknown"));
    assert!(
        String::from_utf8_lossy(&no_stdout.stderr)
            .contains(&format!("artifact upload cancel {upload}"))
    );
    assert_eq!(std::fs::read(&state).unwrap(), cancelled_bytes);
    let server = start(root.path());
    let acknowledged = cli(root.path(), &["artifact", "upload", "cancel", upload]);
    assert_eq!(acknowledged["condition"], "CancelAcknowledged");
    assert_eq!(acknowledged["progress"]["cancelled"], true);
    assert_eq!(acknowledged["progress"]["cancel_acknowledged"], true);
    assert!(acknowledged["progress"]["reference"].is_null());
    let retry = run(
        root.path(),
        &[
            "request",
            "retry",
            "--operation-id",
            &operation,
            "--format",
            "json",
        ],
    );
    assert_eq!(
        retry.status.code(),
        Some(130),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert!(
        cli(root.path(), &["list", "artifacts"])["results"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    drop(server);
    let _server = start(root.path());
    assert_eq!(
        cli(root.path(), &["artifact", "upload", "inspect", upload])["progress"],
        acknowledged["progress"]
    );
    assert_eq!(
        cli(root.path(), &["artifact", "upload", "cancel", upload])["progress"],
        acknowledged["progress"]
    );
}

#[test]
fn cli_can_inspect_and_cancel_mcp_upload_without_removing_committed_artifact_content() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let server = start(root.path());
    let absent = run(
        root.path(),
        &["artifact", "upload", "inspect", &id(1), "--format", "json"],
    );
    assert!(!absent.status.success());
    assert!(!root.path().join("CLI.uploads").exists());
    assert!(!root.path().join("CLI.uploads.lock").exists());
    let bytes = br#"{"passed":1,"failed":0,"skipped":0}"#;
    let upload = id(700);
    let mut mcp = Mcp::start(root.path(), true);
    mcp.success("upload.begin",json!({"upload_id":upload,"length":bytes.len(),"digest":blake3::hash(bytes).to_hex().to_string()}));
    mcp.success(
        "upload.append",
        json!({"upload_id":upload,"offset":0,"bytes_hex":hex(bytes)}),
    );
    let sealed = mcp.success("upload.seal", json!({"upload_id":upload}));
    let reference: focal_model::ContentRef =
        serde_json::from_value(sealed["result"]["progress"]["reference"].clone()).unwrap();
    let artifact = id(701);
    mcp.success("artifact.register",json!({"operation_id":id(702),"id":artifact,"kind":"test-report","schema_hash":focal_evidence::test_report_schema().to_string(),"payload":{"type":"content","reference":{"domain":reference.domain.to_string(),"root":reference.root.to_string(),"length":reference.length,"class":"evidence"}}}));
    drop(mcp);
    let inspected = cli(
        root.path(),
        &["artifact", "upload", "inspect", &upload, "--origin", "mcp"],
    );
    assert_eq!(inspected["condition"], "Sealed");
    assert_eq!(
        inspected["progress"]["reference"],
        sealed["result"]["progress"]["reference"]
    );
    let cancelled = cli(
        root.path(),
        &["artifact", "upload", "cancel", &upload, "--origin", "mcp"],
    );
    assert_eq!(cancelled["condition"], "CancelAcknowledged");
    assert_eq!(cancelled["progress"]["cancelled"], false);
    assert_eq!(
        cancelled["progress"]["reference"],
        inspected["progress"]["reference"]
    );
    drop(server);
    let _server = start(root.path());
    let destination = root.path().join("retained proof.json");
    cli(
        root.path(),
        &[
            "get",
            "artifact",
            &artifact,
            "--output",
            destination.to_str().unwrap(),
        ],
    );
    assert_eq!(std::fs::read(destination).unwrap(), bytes);
    let mut mcp = Mcp::start(root.path(), false);
    let repeated = mcp.success("upload.cancel", json!({"upload_id":upload}));
    assert_eq!(repeated["result"]["progress"], cancelled["progress"]);
}
