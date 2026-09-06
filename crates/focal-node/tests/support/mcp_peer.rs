use super::*;
use focal_client::validation_context::ValidationContext;
use focal_model::*;

impl Mcp {
    fn peer_mutation(&mut self, name: &str, args: Value) -> Value {
        let id = self.reserve();
        let result = self.managed_mutation(name, &id, args);
        let ack = self.success("request.acknowledge", json!({"operation_id":id}));
        assert!(matches!(
            ack["condition"].as_str(),
            Some("Consumed" | "Retired")
        ));
        result
    }
}

fn reference(value: &Value) -> ArtifactRef {
    let receipt: ManagedReceipt =
        serde_json::from_value(value["result"]["receipt"].clone()).unwrap();
    let ManagedReceiptOutcome::Domain(CommandResult::Artifact(reference)) = receipt.outcome else {
        panic!("artifact receipt")
    };
    reference
}
fn proof(hash: &str, text: &str) -> Value {
    json!({"kind":"test-report","schema_hash":hash,"payload":{"type":"text","text":text}})
}
#[test]
fn managed_mcp_peer_increment_and_whole_work_preserve_exact_fences_across_restart() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    let hash = focal_evidence::test_report_schema().to_string();
    let handler = json!({"id":id(210),"version":"11".repeat(32),"agentic":false});
    let mut authored = claim();
    authored["validations"].as_array_mut().unwrap().push(json!({"id":id(211),"kind":"test","phase":"increment","mode":"required","description":"Check one increment","evaluator":"self","handlers":[handler],"evidence_schemas":[hash]}));
    mcp.peer_mutation("claim.submit", authored);
    let contracts = mcp.success("validator.list", json!({"claim":id(100)}));
    assert_eq!(contracts["condition"], "RecordedContracts");
    assert_eq!(
        contracts["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let pinned = mcp.success(
        "validator.get",
        json!({"id":id(210),"version":"11".repeat(32),"agentic":false}),
    );
    assert_eq!(
        pinned["result"]["page"]["objects"],
        contracts["result"]["page"]["objects"]
    );
    mcp.peer_mutation("claim.post", json!({"claim":id(100)}));
    mcp.peer_mutation(
        "receipt.acquire",
        json!({"claim":id(100),"id":id(201),"epoch":1}),
    );
    let receipt = json!({"id":id(201),"epoch":1});
    mcp.peer_mutation(
        "evidence.begin",
        json!({"claim":id(100),"receipt":receipt,"id":id(202)}),
    );
    let report = r#"{"passed":2,"failed":0,"skipped":0}"#;
    let mut artifact = proof(&hash, report);
    artifact["claim"] = json!(id(100));
    artifact["receipt"] = receipt.clone();
    artifact["evidence_set"] = json!(id(202));
    let artifact = reference(&mcp.peer_mutation("artifact.submit", artifact));
    let manifest = manifest_hash(&[artifact]).unwrap().to_string();
    let begin = mcp.reserve();
    let request = json!({"claim":id(100),"validation":id(211),"target_hash":artifact.hash.to_string(),"manifest":manifest});
    let begun = mcp.managed_mutation("validation.begin_increment", &begin, request.clone());
    let context = mcp.success("validation.context", json!({"id":id(211)}));
    let context: ValidationContext =
        serde_json::from_value(context["result"]["context"].clone()).unwrap();
    assert!(context.testament.is_none());
    let run = context
        .records
        .iter()
        .find_map(|r| match &r.value {
            ValidationResultValue::Run(run) => Some(run),
            _ => None,
        })
        .unwrap();
    assert!(run.final_verdict.is_none());
    assert_eq!(run.attempt_count, 0);
    let evidence = reference(&mcp.peer_mutation("artifact.register", proof(&hash, report)));
    mcp.peer_mutation("validation.submit",json!({"validation":id(211),"target_hash":run.id.target_hash.to_string(),"phase":"increment","epoch":run.id.epoch,"handler":handler,"attempt":0,"manifest":manifest,"receipt":receipt,"value":"pass","evidence":[{"id":evidence.id.to_string(),"hash":evidence.hash.to_string()}]}));
    mcp.peer_mutation("testament.submit",json!({"id":id(203),"claim":id(100),"receipt":receipt,"evidence_set":id(202),"manifest":[{"id":artifact.id.to_string(),"hash":artifact.hash.to_string()}],"summary":"Checked increment","confidence":"committed","outcome":"complete"}));
    mcp.peer_mutation(
        "testament.receive",
        json!({"claim":id(100),"testament":id(203)}),
    );
    mcp.peer_mutation("validation.begin", json!({"claim":id(100)}));
    mcp.peer_mutation("validation.complete", json!({"claim":id(100)}));
    let mut batched = claim();
    batched["id"] = json!(id(500));
    batched["occurrence"] = json!(id(501));
    batched["validations"][0]["id"] = json!(id(502));
    let batch_id = mcp.reserve();
    let batch_request = json!({"claims":[batched]});
    let batch = mcp.managed_mutation("claim.submit_batch", &batch_id, batch_request.clone());
    // The claim revision has advanced. Exact retry must retain the original
    // prepared increment fence instead of rebinding to its current revision.
    mcp.finish();
    drop(server);
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), false);
    assert_eq!(
        mcp.managed_mutation("validation.begin_increment", &begin, request),
        begun
    );
    assert_eq!(
        mcp.managed_mutation("claim.submit_batch", &batch_id, batch_request),
        batch
    );
    let result = mcp.success("validation.context", json!({"id":id(211)}));
    let context: ValidationContext =
        serde_json::from_value(result["result"]["context"].clone()).unwrap();
    assert_eq!(context.claim.lifecycle().status, ClaimStatus::Satisfied);
    assert!(context.records.iter().any(|r|matches!(&r.value,ValidationResultValue::Attempt(value) if value.value==VerdictValue::Pass)));
    mcp.finish();
}

fn validation_view(mcp: &mut Mcp, validation: &str) -> ValidationContext {
    let value = mcp.success("validation.context", json!({"id":validation}));
    serde_json::from_value(value["result"]["context"].clone()).unwrap()
}

fn artifact_view(mcp: &mut Mcp, reference: ArtifactRef) -> Artifact {
    let value = mcp.success("artifact.get", json!({"id":reference.id.to_string()}));
    let object: focal_wire::ReadObject =
        serde_json::from_value(value["result"]["page"]["objects"][0].clone()).unwrap();
    let focal_wire::ReadObject::Artifact { id, value } = object else {
        panic!("artifact read")
    };
    assert_eq!(id, reference.id);
    assert_eq!(value.content_hash(), reference.hash);
    value
}

#[test]
fn managed_mcp_respondent_failure_and_external_proof_survive_both_process_restarts() {
    use focal_evidence::{TestReportValidator, Validator};
    use std::os::unix::fs::PermissionsExt;

    // One authenticated participant performs the self-handoff roles here.
    // Distinct-principal and adopted-receipt fences are covered by Core tests.
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    let schema = focal_evidence::test_report_schema().to_string();
    let validation = id(411);
    let handler = json!({"id":id(410),"version":"33".repeat(32),"agentic":false});
    let mut authored = claim();
    authored["description"] = json!("Respond with the actual work result and its evidence");
    authored["validations"].as_array_mut().unwrap().push(json!({
        "id":validation,"kind":"test","phase":"whole_work","mode":"required",
        "description":"The participant checks whether the requested test run completed",
        "evaluator":"self","handlers":[handler],"evidence_schemas":[schema]
    }));
    mcp.peer_mutation("claim.submit", authored);
    mcp.peer_mutation("claim.post", json!({"claim":id(100)}));
    mcp.peer_mutation(
        "receipt.acquire",
        json!({"claim":id(100),"id":id(401),"epoch":1}),
    );
    let received = validation_view(&mut mcp, &validation);
    assert_eq!(received.claim.lifecycle().status, ClaimStatus::Received);
    assert!(received.testament.is_none());
    assert!(received.records.is_empty());
    assert_eq!(
        mcp.success("testament.list", json!({}))["result"]["page"]["objects"],
        json!([])
    );

    let receipt = json!({"id":id(401),"epoch":1});
    mcp.peer_mutation(
        "evidence.begin",
        json!({"claim":id(100),"receipt":receipt,"id":id(402)}),
    );
    let summary =
        "The required tool could not run; work ended without satisfying the requested check";
    let mut testament = json!({
        "id":id(403),"claim":id(100),"receipt":receipt,"evidence_set":id(402),
        "manifest":[],"summary":summary,"confidence":"tentative","outcome":"failed"
    });
    let rejected_id = mcp.reserve();
    let mut empty = testament.clone();
    empty["operation_id"] = json!(rejected_id);
    assert_eq!(
        mcp.call("testament.submit", empty)["result"]["isError"],
        true
    );
    assert!(validation_view(&mut mcp, &validation).testament.is_none());
    // Retire the refused request slot; this does not close or fail the claim.
    mcp.success("request.seal", json!({"operation_id":rejected_id}));
    mcp.success("request.acknowledge", json!({"operation_id":rejected_id}));

    let error_report = r#"{"code":"tool_unavailable","message":"The required tool could not run","details":"No test result was produced"}"#;
    let mut diagnostic = proof(
        &focal_evidence::error_report_schema().to_string(),
        error_report,
    );
    diagnostic["kind"] = json!("error");
    diagnostic["claim"] = json!(id(100));
    diagnostic["receipt"] = receipt.clone();
    diagnostic["evidence_set"] = json!(id(402));
    let diagnostic = reference(&mcp.peer_mutation("artifact.submit", diagnostic));
    let diagnostic_row = artifact_view(&mut mcp, diagnostic);
    assert_eq!(diagnostic_row.content().kind, "error");
    assert!(diagnostic_row.lifecycle().custody_revision > 0);
    assert_eq!(
        diagnostic_row.content().payload,
        ArtifactPayload::Inline(error_report.as_bytes().to_vec())
    );
    testament["manifest"] =
        json!([{"id":diagnostic.id.to_string(),"hash":diagnostic.hash.to_string()}]);
    let submitted_id = mcp.reserve();
    let submitted = mcp.managed_mutation("testament.submit", &submitted_id, testament.clone());
    let closed = validation_view(&mut mcp, &validation);
    assert_eq!(
        closed.claim.lifecycle().status,
        ClaimStatus::TestamentGenerated
    );
    assert!(!closed.claim.lifecycle().local_complete);
    let response = closed.testament.as_ref().unwrap();
    assert_eq!(response.value.content().outcome, OutcomeKind::Failed);
    assert_eq!(response.value.content().summary, summary);
    assert_eq!(response.value.content().confidence, Confidence::Tentative);
    assert_eq!(response.value.content().artifacts, [diagnostic]);
    assert!(response.value.lifecycle().acknowledged.is_none());
    assert!(closed.records.is_empty());

    mcp.peer_mutation(
        "testament.receive",
        json!({"claim":id(100),"testament":id(403)}),
    );
    mcp.peer_mutation("validation.begin", json!({"claim":id(100)}));
    let begun = validation_view(&mut mcp, &validation);
    assert_eq!(begun.claim.lifecycle().status, ClaimStatus::Validating);
    let run = begun
        .records
        .iter()
        .find_map(|record| match &record.value {
            ValidationResultValue::Run(run) => Some(run),
            _ => None,
        })
        .unwrap();
    assert_eq!(run.attempt_count, 0);
    assert!(run.final_verdict.is_none());
    assert_eq!(run.manifest, manifest_hash(&[diagnostic]).unwrap());
    assert_eq!(run.id.target_hash, response.value.content_hash());

    // Programmatic evaluation executes in this participant process, never the daemon.
    // This is a failed acceptance check of the durable error report, not a claim
    // that the unavailable work tool produced test results.
    let ArtifactPayload::Inline(diagnostic_bytes) = &diagnostic_row.content().payload else {
        panic!("the submitted diagnostic must remain inline");
    };
    let reported_error: Value = serde_json::from_slice(diagnostic_bytes).unwrap();
    assert_eq!(reported_error["code"], "tool_unavailable");
    assert_eq!(reported_error["details"], "No test result was produced");
    let report = serde_json::to_string(&json!({
        "passed":0,
        "failed":usize::from(reported_error["code"] == "tool_unavailable"),
        "skipped":0
    }))
    .unwrap();
    let verdict = TestReportValidator
        .evaluate(report.as_bytes(), None)
        .unwrap();
    assert_eq!(verdict.value, VerdictValue::Fail);
    let evidence = reference(&mcp.peer_mutation("artifact.register", proof(&schema, &report)));
    let evidence_row = artifact_view(&mut mcp, evidence);
    assert_eq!(evidence_row.content().receipt, None);
    assert_ne!(evidence, diagnostic);
    mcp.peer_mutation(
        "validation.submit",
        json!({
            "validation":validation,"target_hash":run.id.target_hash.to_string(),
            "phase":"whole_work","epoch":run.id.epoch,"handler":handler,"attempt":0,
            "manifest":run.manifest.to_string(),"receipt":receipt,"value":"fail",
            "evidence":[{"id":evidence.id.to_string(),"hash":evidence.hash.to_string()}]
        }),
    );
    mcp.peer_mutation("validation.complete", json!({"claim":id(100)}));
    let completed = validation_view(&mut mcp, &validation);
    assert_eq!(
        completed.claim.lifecycle().status,
        ClaimStatus::ValidationFailed
    );
    assert!(!completed.claim.lifecycle().local_complete);
    assert!(
        completed
            .records
            .iter()
            .any(|record| matches!(&record.value,
        ValidationResultValue::Attempt(attempt)
        if attempt.value == VerdictValue::Fail && attempt.evidence == [evidence]
            && attempt.run == run.id && attempt.manifest == run.manifest))
    );
    assert_eq!(
        completed.testament.as_ref().unwrap().value.content(),
        response.value.content()
    );

    mcp.finish();
    drop(server);
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), false);
    assert_eq!(
        mcp.managed_mutation("testament.submit", &submitted_id, testament),
        submitted
    );
    let recovered = validation_view(&mut mcp, &validation);
    assert_eq!(recovered.claim, completed.claim);
    assert_eq!(recovered.testament, completed.testament);
    assert_eq!(recovered.records, completed.records);
    assert_eq!(artifact_view(&mut mcp, diagnostic), diagnostic_row);
    assert_eq!(artifact_view(&mut mcp, evidence), evidence_row);
    mcp.success("request.acknowledge", json!({"operation_id":submitted_id}));
    mcp.finish();
}
