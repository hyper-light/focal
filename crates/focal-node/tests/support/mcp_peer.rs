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
