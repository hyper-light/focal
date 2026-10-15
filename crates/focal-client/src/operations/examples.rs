//! The V1 engine's examples: one authored input per released V1 descriptor.
//! These are authored inputs, not fabricated server outcomes. Object
//! references are valid illustrative IDs that the caller must replace with
//! actual results. A descriptor without an example is explicitly unavailable,
//! never a fabricated request. `SCHEMA` stands for the built-in test-report
//! schema hash and is substituted when the example is generated.
pub(super) fn v1_raw_example(name: &str) -> Option<&'static str> {
    match name {
        "monitor.register" => Some(
            r#"{"owner":"00000000000000000000000000000001","roots":[{"predicate":"satisfied","claim":"00000000000000000000000000000002"}],"deadline":{"timer":"00000000000000000000000000000003","generation":1,"at":4102444800}}"#,
        ),
        "claim.wait" => Some(
            r#"{"claim":"00000000000000000000000000000001","until":"satisfied","timeout_ms":1000}"#,
        ),
        "monitor.get" => Some(r#"{"id":"00000000000000000000000000000004"}"#),
        "claim.submit" => Some(
            r#"{"target":"self","action":"handoff","description":"Deliver the checked report","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the report testament","evaluator":"self"}]}"#,
        ),
        "claim.submit_batch" => Some(
            r#"{"claims":[{"target":"self","action":"handoff","description":"Deliver the checked report","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the report testament","evaluator":"self"}]}]}"#,
        ),
        "claim.post" | "validation.begin" | "validation.complete" => {
            Some(r#"{"claim":"00000000000000000000000000000001"}"#)
        }
        "validation.begin_increment" => Some(
            r#"{"claim":"00000000000000000000000000000010","validation":"00000000000000000000000000000012","target_hash":"1111111111111111111111111111111111111111111111111111111111111111","manifest":"2222222222222222222222222222222222222222222222222222222222222222"}"#,
        ),
        "claim.cancel" => Some(
            r#"{"claim":"00000000000000000000000000000001","reason":"Work is no longer needed"}"#,
        ),
        "claim.progress" => Some(
            r#"{"claim":"00000000000000000000000000000001","receipt":{"id":"00000000000000000000000000000002","epoch":1},"message":"Report prepared"}"#,
        ),
        "receipt.acquire" => Some(r#"{"claim":"00000000000000000000000000000001","epoch":1}"#),
        "evidence.begin" => Some(
            r#"{"claim":"00000000000000000000000000000001","receipt":{"id":"00000000000000000000000000000002","epoch":1}}"#,
        ),
        "testament.submit" => Some(
            r#"{"claim":"00000000000000000000000000000001","receipt":{"id":"00000000000000000000000000000002","epoch":1},"evidence_set":"00000000000000000000000000000003","manifest":[],"summary":"Closing the response; supply the actual manifest when evidence is required","confidence":"committed","outcome":"complete"}"#,
        ),
        "artifact.submit" => Some(
            r#"{"claim":"00000000000000000000000000000001","receipt":{"id":"00000000000000000000000000000002","epoch":1},"evidence_set":"00000000000000000000000000000003","kind":"test-report","schema_hash":"SCHEMA","payload":{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}}"#,
        ),
        "artifact.register" => Some(
            r#"{"id":"00000000000000000000000000000004","kind":"test-report","schema_hash":"SCHEMA","payload":{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}}"#,
        ),
        "testament.receive" => Some(
            r#"{"claim":"00000000000000000000000000000001","testament":"00000000000000000000000000000004"}"#,
        ),
        "validation.submit" => Some(
            r#"{"validation":"00000000000000000000000000000005","target_hash":"1111111111111111111111111111111111111111111111111111111111111111","phase":"whole_work","epoch":1,"handler":{"id":"00000000000000000000000000000006","version":"2222222222222222222222222222222222222222222222222222222222222222","agentic":false},"attempt":0,"manifest":"3333333333333333333333333333333333333333333333333333333333333333","receipt":{"id":"00000000000000000000000000000002","epoch":1},"value":"pass","evidence":[{"id":"00000000000000000000000000000007","hash":"4444444444444444444444444444444444444444444444444444444444444444"}]}"#,
        ),
        "claim.supersede" => Some(
            r#"{"predecessor":"00000000000000000000000000000001","successor":{"id":"00000000000000000000000000000005","occurrence":"00000000000000000000000000000006","target":"self","action":"handoff","description":"Deliver the corrected report","validations":[{"id":"00000000000000000000000000000007","kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the corrected report testament","evaluator":"self"}]}}"#,
        ),
        "claim.get" | "testament.get" | "artifact.get" | "validation.get"
        | "validation.context" => Some(r#"{"id":"00000000000000000000000000000001"}"#),
        "validator.get" => Some(
            r#"{"id":"00000000000000000000000000000006","version":"2222222222222222222222222222222222222222222222222222222222222222"}"#,
        ),
        "validator.list" | "ledger.summary" => Some("{}"),
        "ledger.traverse" => Some(
            r#"{"roots":["claim:00000000000000000000000000000001"],"edges":["requirement"],"depth":1,"limit":32}"#,
        ),
        "claim.list" | "testament.list" | "artifact.list" | "validation.list" => Some("{}"),
        "request.epoch" => Some(r#"{"epoch":1}"#),
        "request.status" => Some(r#"{"epoch":1,"request_id":"00000000000000000000000000000001"}"#),
        _ => None,
    }
}
