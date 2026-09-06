use super::{InputError, OperationDescriptor, catalog::InputKind};
use focal_model::ObjectKind;
use serde_json::{Map, Value};

// Finite, local JSON Schema 2020-12 definitions. Serde DTOs remain the decoder;
// tests check each released authored field/default against these descriptors.
// Semantic requirements (actor, receipt, pinned contracts, supported predicate
// combinations) are checked by shared builders and authenticated server ingress.
const DEFINITIONS: &str = r##"{
  "id":{"type":"string","pattern":"^[0-9a-fA-F]{32}$","not":{"const":"00000000000000000000000000000000"}},
  "hash":{"type":"string","pattern":"^[0-9a-fA-F]{64}$","not":{"const":"0000000000000000000000000000000000000000000000000000000000000000"}},
  "participant":{"anyOf":[{"$ref":"#/$defs/id"},{"const":"self"}]},
  "text":{"type":"string","minLength":1,"maxLength":16384},
  "u64":{"type":"integer","minimum":0,"maximum":18446744073709551615},
  "positive":{"type":"integer","minimum":1,"maximum":18446744073709551615},
  "scope":{"type":"object","additionalProperties":false,"required":["kind","key"],"properties":{"kind":{"enum":["file","symbol","api","test_surface","component","ux_surface"]},"key":{"$ref":"#/$defs/text"}}},
  "relation":{"type":"object","additionalProperties":false,"required":["kind","target"],"properties":{"kind":{"enum":["supersedes","depends_on","awaits","refines","conflicts_with","derived_from","reviews","amends"]},"target":{"$ref":"#/$defs/id"}}},
  "deadline":{"type":"object","additionalProperties":false,"required":["timer","generation","at"],"properties":{"timer":{"$ref":"#/$defs/id"},"generation":{"$ref":"#/$defs/positive"},"at":{"$ref":"#/$defs/positive"}}},
  "handler":{"type":"object","additionalProperties":false,"required":["id","version","agentic"],"properties":{"id":{"$ref":"#/$defs/id"},"version":{"$ref":"#/$defs/hash"},"agentic":{"type":"boolean"}}},
  "receipt":{"type":"object","additionalProperties":false,"required":["id","epoch"],"properties":{"id":{"$ref":"#/$defs/id"},"epoch":{"$ref":"#/$defs/positive"}}},
  "artifact_reference":{"type":"object","additionalProperties":false,"required":["id","hash"],"properties":{"id":{"$ref":"#/$defs/id"},"hash":{"$ref":"#/$defs/hash"}}},
  "object_reference":{"type":"object","additionalProperties":false,"required":["kind","id"],"properties":{"kind":{"enum":["claim","testament","artifact","validation"]},"id":{"$ref":"#/$defs/id"}}},
  "validation":{"type":"object","additionalProperties":false,"required":["kind","phase","mode","description","evaluator"],"properties":{
    "id":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]},"kind":{"enum":["receipt","test","inspection","integration","contract","design","regression"]},"phase":{"enum":["admission","increment","whole_work"]},"mode":{"enum":["observe","required"]},"description":{"$ref":"#/$defs/text"},"evaluator":{"$ref":"#/$defs/participant"},
    "quality_bar":{"anyOf":[{"$ref":"#/$defs/text"},{"type":"null"}]},"handlers":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/handler"},"default":[]},"evidence_schemas":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/hash"},"default":[]},"contributed_by":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/participant"},"default":[]},"policy_revision":{"anyOf":[{"$ref":"#/$defs/positive"},{"type":"null"}]}
  }},
  "claim":{"type":"object","additionalProperties":false,"required":["description","target","validations"],"properties":{
    "id":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]},"occurrence":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]},"description":{"$ref":"#/$defs/text"},"target":{"$ref":"#/$defs/participant"},"action":{"enum":["work","consultation","challenge","feedback","approval","summon","handoff","evaluation","correction","teardown"],"default":"work"},"scopes":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/scope"},"default":[]},"relations":{"type":"array","maxItems":252,"items":{"$ref":"#/$defs/relation"},"default":[]},"deadline":{"anyOf":[{"$ref":"#/$defs/deadline"},{"type":"null"}]},"validations":{"type":"array","minItems":1,"maxItems":64,"items":{"$ref":"#/$defs/validation"}}
  }},
  "testament":{"type":"object","additionalProperties":false,"required":["claim","receipt","evidence_set","manifest","summary","confidence","outcome"],"properties":{
    "id":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]},"claim":{"$ref":"#/$defs/id"},"receipt":{"$ref":"#/$defs/receipt"},"evidence_set":{"$ref":"#/$defs/id"},"manifest":{"type":"array","maxItems":1024,"items":{"$ref":"#/$defs/artifact_reference"}},"summary":{"$ref":"#/$defs/text"},"confidence":{"enum":["hint","tentative","committed","consensus"]},"outcome":{"enum":["complete","partial","refused","impossible","interrupted","failed"]}
  }},
  "bytes":{"type":"array","maxItems":16384,"items":{"type":"integer","minimum":0,"maximum":255}},
  "content_reference":{"type":"object","additionalProperties":false,"required":["domain","root","length","class"],"properties":{"domain":{"$ref":"#/$defs/id"},"root":{"$ref":"#/$defs/hash"},"length":{"$ref":"#/$defs/u64"},"class":{"enum":["document","evidence","checkpoint"]}}},
  "payload":{"oneOf":[
    {"type":"object","additionalProperties":false,"required":["type","bytes"],"properties":{"type":{"const":"inline"},"bytes":{"$ref":"#/$defs/bytes"}}},
    {"type":"object","additionalProperties":false,"required":["type","text"],"properties":{"type":{"const":"text"},"text":{"type":"string","maxLength":16384}}},
    {"type":"object","additionalProperties":false,"required":["type","reference"],"properties":{"type":{"const":"content"},"reference":{"$ref":"#/$defs/content_reference"}}}
  ]},
  "artifact":{"type":"object","additionalProperties":false,"required":["claim","receipt","evidence_set","kind","schema_hash","payload"],"properties":{
    "claim":{"$ref":"#/$defs/id"},"receipt":{"$ref":"#/$defs/receipt"},"evidence_set":{"$ref":"#/$defs/id"},"id":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]},"kind":{"type":"string","minLength":1,"maxLength":128,"pattern":"^[a-z0-9_./-]+$"},"schema_hash":{"$ref":"#/$defs/hash"},"metadata":{"$ref":"#/$defs/bytes","default":[]},"payload":{"$ref":"#/$defs/payload"},"inputs":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/object_reference"},"default":[]},"visibility":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/text"},"default":[]}
  }},
  "claim_id":{"type":"object","additionalProperties":false,"required":["claim"],"properties":{"claim":{"$ref":"#/$defs/id"}}},
  "progress":{"type":"object","additionalProperties":false,"required":["claim","receipt","message"],"properties":{"claim":{"$ref":"#/$defs/id"},"receipt":{"$ref":"#/$defs/receipt"},"message":{"type":"string","maxLength":16384}}},
  "cancel":{"type":"object","additionalProperties":false,"required":["claim","reason"],"properties":{"claim":{"$ref":"#/$defs/id"},"reason":{"type":"string","maxLength":16384}}},
  "acquire":{"type":"object","additionalProperties":false,"required":["claim","epoch"],"properties":{"claim":{"$ref":"#/$defs/id"},"epoch":{"$ref":"#/$defs/positive"},"id":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]}}},
  "evidence":{"type":"object","additionalProperties":false,"required":["claim","receipt"],"properties":{"claim":{"$ref":"#/$defs/id"},"receipt":{"$ref":"#/$defs/receipt"},"id":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]}}},
  "request_epoch":{"type":"object","additionalProperties":false,"required":["epoch"],"properties":{"epoch":{"$ref":"#/$defs/positive"}}},
  "request_status":{"type":"object","additionalProperties":false,"required":["epoch","request_id"],"properties":{"epoch":{"$ref":"#/$defs/positive"},"request_id":{"$ref":"#/$defs/id"}}},
  "prefix":{"type":"object","additionalProperties":false,"required":["sequence","route_epoch"],"properties":{"sequence":{"$ref":"#/$defs/u64"},"route_epoch":{"$ref":"#/$defs/positive"}}},
  "position":{"type":"object","additionalProperties":false,"required":["target_hash","phase","epoch"],"properties":{"target_hash":{"$ref":"#/$defs/hash"},"phase":{"enum":["admission","increment","whole_work"]},"epoch":{"$ref":"#/$defs/positive"},"attempt":{"type":["integer","null"],"minimum":0,"maximum":4294967295}}},
  "get":{"type":"object","additionalProperties":false,"required":["id"],"properties":{"id":{"$ref":"#/$defs/id"},"prefix":{"anyOf":[{"$ref":"#/$defs/prefix"},{"type":"null"}]},"after":{"anyOf":[{"$ref":"#/$defs/position"},{"type":"null"}]},"limit":{"type":"integer","minimum":1,"maximum":1024,"default":64}}},
  "list":{"type":"object","additionalProperties":false,"properties":{
    "claim":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]},"testament":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]},"source":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}]},"target":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}]},"status":{"enum":["generated","posted","received","progressed","testament_generated","testament_acknowledged","validating","satisfied","post_failed","receipt_failed","testament_generation_failed","validation_incomplete","validation_failed","validation_errored","cancelled","expired","revoked","superseded","dependency_failed","deadlocked",null]},"action":{"enum":["work","consultation","challenge","feedback","approval","summon","handoff","evaluation","correction","teardown",null]},"producer":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}]},"kind":{"type":["string","null"],"minLength":1,"maxLength":256},"schema_hash":{"anyOf":[{"$ref":"#/$defs/hash"},{"type":"null"}]},"evaluator":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}]},"phase":{"enum":["admission","increment","whole_work",null]},"mode":{"enum":["observe","required",null]},"cursor":{"type":["string","null"],"pattern":"^(?:[0-9a-fA-F]{2})+$","maxLength":1024},"limit":{"type":"integer","minimum":1,"maximum":1024,"default":64},"max_visits":{"type":"integer","minimum":1,"maximum":1024,"default":1024}
  }}
}"##;
pub(super) fn input(descriptor: &OperationDescriptor) -> Result<Value, InputError> {
    let mut definitions: Map<String, Value> = serde_json::from_str(DEFINITIONS)
        .map_err(|_| InputError::Invalid("released schema definition"))?;
    super::selection::extend_schema(&mut definitions)?;
    let mut registered = definitions
        .get("artifact")
        .and_then(Value::as_object)
        .cloned()
        .ok_or(InputError::Invalid("artifact schema"))?;
    if let Some(Value::Object(fields)) = registered.get_mut("properties") {
        for field in ["claim", "receipt", "evidence_set"] {
            fields.remove(field);
        }
    }
    if let Some(Value::Array(required)) = registered.get_mut("required") {
        required
            .retain(|field| !matches!(field.as_str(), Some("claim" | "receipt" | "evidence_set")));
    }
    definitions.insert("artifact_register".into(), Value::Object(registered));
    definitions.insert("increment_validation".into(), serde_json::json!({"type":"object","additionalProperties":false,"required":["claim","validation","target_hash","manifest"],"properties":{"claim":{"$ref":"#/$defs/id"},"validation":{"$ref":"#/$defs/id"},"target_hash":{"$ref":"#/$defs/hash"},"manifest":{"$ref":"#/$defs/hash"}}}));
    definitions.insert("testament_receive".into(),serde_json::json!({"type":"object","additionalProperties":false,"required":["claim","testament"],"properties":{"claim":{"$ref":"#/$defs/id"},"testament":{"$ref":"#/$defs/id"}}}));
    definitions.insert("supersede".into(),serde_json::json!({"type":"object","additionalProperties":false,"required":["predecessor","successor"],"properties":{"predecessor":{"$ref":"#/$defs/id"},"successor":{"$ref":"#/$defs/claim"}}}));
    definitions.insert("validation_verdict".into(),serde_json::json!({"type":"object","additionalProperties":false,"required":["validation","target_hash","phase","epoch","handler","attempt","manifest","value","evidence"],"properties":{
        "validation":{"$ref":"#/$defs/id"},"target_hash":{"$ref":"#/$defs/hash"},"phase":{"enum":["admission","increment","whole_work"]},"epoch":{"$ref":"#/$defs/positive"},"handler":{"$ref":"#/$defs/handler"},"attempt":{"type":"integer","minimum":0,"maximum":4294967295u64},"manifest":{"$ref":"#/$defs/hash"},"receipt":{"anyOf":[{"$ref":"#/$defs/receipt"},{"type":"null"}],"default":null},"value":{"enum":["pass","fail","error","incomplete"]},"evidence":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/artifact_reference"}}
    }}));
    definitions.insert("traversal".into(),serde_json::from_str(r##"{"type":"object","additionalProperties":false,"required":["roots"],"properties":{
      "roots":{"type":"array","minItems":1,"maxItems":32,"items":{"type":"string","pattern":"^(claim|testament|artifact|validation):[0-9a-fA-F]{32}$"}},
      "direction":{"enum":["forward","reverse"],"default":"forward"},
      "edges":{"type":"array","maxItems":32,"default":[],"items":{"enum":["issuer","subject","evaluator","claim_action","supersedes","depends_on","awaits","caused_by","refines","conflicts_with","derived_from","reviews","amends","contributed_by","invalidates","requirement","testament_of","evidence","artifact_input","validation_of"]}},
      "depth":{"type":"integer","minimum":0,"maximum":32,"default":8},"max_nodes":{"type":"integer","minimum":1,"maximum":4096,"default":4096},"max_edges":{"type":"integer","minimum":1,"maximum":16384,"default":16384},
      "limit":{"type":"integer","minimum":1,"maximum":1024,"default":64},"max_visits":{"type":"integer","minimum":1,"maximum":1024,"default":1024},"max_bytes":{"type":"integer","minimum":1024,"maximum":1048576,"default":1048576},
      "cursor":{"type":["string","null"],"minLength":2,"maxLength":512,"pattern":"^([0-9a-fA-F]{2})+$","default":null}
    }}"##).map_err(|_|InputError::Invalid("traversal schema"))?);
    let mut validator = definitions
        .get("list")
        .and_then(Value::as_object)
        .cloned()
        .ok_or(InputError::Invalid("validator schema"))?;
    if let Some(Value::Object(fields)) = validator.get_mut("properties") {
        fields.retain(|key, _| {
            matches!(
                key.as_str(),
                "claim"
                    | "evaluator"
                    | "kind"
                    | "phase"
                    | "mode"
                    | "cursor"
                    | "limit"
                    | "max_visits"
                    | "schema_hash"
            )
        });
        for (name, definition) in [("id", "id"), ("version", "hash")] {
            fields.insert(name.into(), serde_json::json!({"anyOf":[{"$ref":format!("#/$defs/{definition}")},{"type":"null"}]}));
        }
        fields.insert(
            "agentic".into(),
            serde_json::json!({"type":["boolean","null"]}),
        );
        if descriptor.name == "validator.get" {
            fields.insert("id".into(), serde_json::json!({"$ref":"#/$defs/id"}));
            fields.insert("version".into(), serde_json::json!({"$ref":"#/$defs/hash"}));
        }
    }
    if descriptor.name == "validator.get" {
        validator.insert("required".into(), serde_json::json!(["id", "version"]));
    }
    definitions.insert("validator".into(), Value::Object(validator));
    definitions.insert("claim_batch".into(), serde_json::json!({"type":"object","additionalProperties":false,"required":["claims"],"properties":{"claims":{"type":"array","minItems":1,"maxItems":64,"items":{"$ref":"#/$defs/claim"}}}}));
    let mut claim_get = definitions
        .get("get")
        .and_then(Value::as_object)
        .cloned()
        .ok_or(InputError::Invalid("claim get schema"))?;
    claim_get.remove("required");
    let list_fields = definitions
        .get("list")
        .and_then(|v| v.get("properties"))
        .and_then(Value::as_object)
        .ok_or(InputError::Invalid("list schema"))?;
    if let Some(Value::Object(fields)) = claim_get.get_mut("properties") {
        for field in [
            "claim",
            "source",
            "target",
            "status",
            "action",
            "max_visits",
            "scopes",
            "relations",
            "caused_by",
            "created_after",
            "created_through",
        ] {
            fields.insert(
                field.into(),
                list_fields
                    .get(field)
                    .ok_or(InputError::Invalid("claim selector field"))?
                    .clone(),
            );
        }
        fields.insert(
            "id".into(),
            serde_json::json!({"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}]}),
        );
        fields.insert("after".into(), null_schema());
    }
    claim_get.insert("description".into(),Value::String("Supply an exact id or at least one non-null claim/source/target/status/action filter, never both. prefix is supported only with exact id. Filtered lookup proves uniqueness at a fresh fixed prefix; limit is retained for exact-read compatibility, max_visits bounds each filtered page.".into()));
    claim_get.insert("oneOf".into(),serde_json::json!([
        {"required":["id"],"properties":{"id":{"$ref":"#/$defs/id"},"claim":{"type":"null"},"source":{"type":"null"},"target":{"type":"null"},"status":{"type":"null"},"action":{"type":"null"},"max_visits":{"const":1024}}},
        {"properties":{"id":{"type":"null"},"prefix":{"type":"null"},"limit":{"const":64}},"anyOf":[
            {"required":["claim"],"properties":{"claim":{"$ref":"#/$defs/id"}}},
            {"required":["source"],"properties":{"source":{"$ref":"#/$defs/participant"}}},
            {"required":["target"],"properties":{"target":{"$ref":"#/$defs/participant"}}},
            {"required":["status"],"properties":{"status":{"type":"string"}}},
            {"required":["action"],"properties":{"action":{"type":"string"}}}
        ]}
    ]));
    super::selection::extend_claim_get(&mut claim_get)?;
    definitions.insert("claim_get".into(), Value::Object(claim_get));
    definitions.insert(
        "summary".into(),
        serde_json::json!({"type":"object","additionalProperties":false,"properties":{}}),
    );
    definitions.insert("monitor_register".into(), serde_json::json!({"type":"object","additionalProperties":false,"required":["owner","roots","deadline"],"properties":{"monitor":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}],"default":null},"owner":{"$ref":"#/$defs/id"},"deadline":{"$ref":"#/$defs/deadline"},"roots":{"type":"array","minItems":1,"maxItems":256,"uniqueItems":true,"items":{"type":"object","additionalProperties":false,"required":["predicate","claim"],"properties":{"predicate":{"enum":["satisfied","terminal","released"]},"claim":{"$ref":"#/$defs/id"}}}}}}));
    definitions.insert("claim_wait".into(), serde_json::json!({"type":"object","additionalProperties":false,"required":["claim","until"],"properties":{"claim":{"$ref":"#/$defs/id"},"until":{"enum":["satisfied","terminal","released"]},"timeout_ms":{"type":"integer","minimum":1,"maximum":30000,"default":30000}}}));
    definitions.insert("monitor_get".into(), serde_json::json!({"type":"object","additionalProperties":false,"required":["id"],"properties":{"id":{"$ref":"#/$defs/id"}}}));
    let key = match descriptor.input {
        InputKind::ClaimWait => "claim_wait",
        InputKind::MonitorGet => "monitor_get",
        InputKind::MonitorRegister => "monitor_register",
        InputKind::Summary => "summary",
        InputKind::ClaimGet => "claim_get",
        InputKind::ClaimBatch => "claim_batch",
        InputKind::Validator => "validator",
        InputKind::Traversal => "traversal",
        InputKind::Claim => "claim",
        InputKind::Testament => "testament",
        InputKind::Artifact => "artifact",
        InputKind::ArtifactRegister => "artifact_register",
        InputKind::TestamentReceive => "testament_receive",
        InputKind::IncrementValidation => "increment_validation",
        InputKind::ValidationVerdict => "validation_verdict",
        InputKind::Supersede => "supersede",
        InputKind::ClaimId => "claim_id",
        InputKind::Progress => "progress",
        InputKind::Cancel => "cancel",
        InputKind::Receipt => "acquire",
        InputKind::Evidence => "evidence",
        InputKind::Get => "get",
        InputKind::List => "list",
        InputKind::RequestEpoch => "request_epoch",
        InputKind::RequestStatus => "request_status",
    };
    let mut schema = definitions
        .get(key)
        .and_then(Value::as_object)
        .cloned()
        .ok_or(InputError::Invalid("released input schema"))?;
    let properties = schema
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .ok_or(InputError::Invalid("released schema fields"))?;
    if descriptor.input == InputKind::List {
        for (name, property) in properties.iter_mut() {
            let allowed = match name.as_str() {
                "source" | "target" | "status" | "action" | "scopes" | "relations"
                | "caused_by" => descriptor.family == Some(ObjectKind::Claim),
                "testament" | "producer" | "schema_hash" | "inputs" => {
                    descriptor.family == Some(ObjectKind::Artifact)
                }
                "outcome" | "confidence" => descriptor.family == Some(ObjectKind::Testament),
                "evaluator" | "phase" | "mode" => descriptor.family == Some(ObjectKind::Validation),
                "kind" => matches!(
                    descriptor.family,
                    Some(ObjectKind::Artifact | ObjectKind::Validation)
                ),
                _ => true,
            };
            if !allowed {
                *property = if matches!(name.as_str(), "scopes" | "relations" | "inputs") {
                    serde_json::json!({"type":"array","maxItems":0,"default":[]})
                } else {
                    null_schema()
                };
            }
        }
    }
    if descriptor.input == InputKind::Get && descriptor.family != Some(ObjectKind::Validation) {
        properties.insert("after".into(), null_schema());
    }
    schema.insert(
        "$schema".into(),
        Value::String("https://json-schema.org/draft/2020-12/schema".into()),
    );
    schema.insert(
        "$id".into(),
        Value::String(format!(
            "urn:focal:operation:{}:input:{}",
            descriptor.name, descriptor.version
        )),
    );
    schema.insert("$defs".into(), Value::Object(definitions));
    Ok(Value::Object(schema))
}
fn null_schema() -> Value {
    Value::Object(Map::from_iter([(
        "type".into(),
        Value::String("null".into()),
    )]))
}
pub(super) fn output() -> Result<Value, InputError> {
    serde_json::from_str(r##"{
      "$schema":"https://json-schema.org/draft/2020-12/schema","$id":"urn:focal:application-result:1",
      "type":"object","additionalProperties":false,"required":["schema_version","operation_id","condition","result"],
      "properties":{
        "schema_version":{"const":1},"operation_id":{"type":["string","null"],"minLength":1,"maxLength":128},"condition":{"type":"string","minLength":1,"maxLength":64},
        "result":{"oneOf":[
          {"type":"object","additionalProperties":false,"required":["kind","result"],"properties":{"kind":{"const":"claim_wait"},"result":{"type":"object","additionalProperties":false,"required":["condition","until","observation","probes"],"properties":{"condition":{"enum":["Met","Pending","Unmet"]},"until":{"enum":["satisfied","terminal","released"]},"probes":{"type":"integer","minimum":1,"maximum":31},"observation":{"type":"object","additionalProperties":false,"required":["token","id","status","revision","local_complete","released"],"properties":{"token":{"type":"object","description":"Latest actual quorum read token, not a retained cursor lease."},"id":{"type":"array","minItems":16,"maxItems":16,"items":{"type":"integer","minimum":0,"maximum":255}},"status":{"type":"integer","minimum":1,"maximum":20},"revision":{"type":"integer","minimum":0},"local_complete":{"type":"boolean"},"released":{"type":"boolean"}}}}}}},
          {"type":"object","additionalProperties":false,"required":["kind","page"],"properties":{"kind":{"const":"monitor"},"page":{"type":"object","additionalProperties":false,"required":["token","applied_index","id","monitor"],"properties":{"token":{"type":"object"},"applied_index":{"type":"integer","minimum":1},"id":{"description":"Frozen MonitorId encoding"},"monitor":{"type":["object","null"],"description":"Actual bounded immutable registration and optional committed release sequence; no inferred release reason."}}}}},
          {"type":"object","additionalProperties":false,"required":["kind","summary"],"properties":{"kind":{"const":"summary"},"summary":{"type":"object","additionalProperties":false,"required":["token","applied_index","claims","testaments","artifacts","validations","evidence_sets","validation_runs"],"properties":{"token":{"type":"object","description":"Observed current ledger/sequence/route; this read retains no historical lease."},"applied_index":{"type":"integer","minimum":1},"claims":{"type":"integer","minimum":0},"testaments":{"type":"integer","minimum":0},"artifacts":{"type":"integer","minimum":0},"validations":{"type":"integer","minimum":0},"evidence_sets":{"type":"integer","minimum":0},"validation_runs":{"type":"integer","minimum":0}}}}},
          {"type":"object","additionalProperties":false,"required":["kind","status","delivery"],"properties":{"kind":{"const":"watch"},"status":{"type":"object","description":"Bounded durable watch state: immutable options, delivered/acknowledged counters, pending request and retained delivery ID."},"delivery":{"type":["object","null"],"description":"Exact retained delivery {id,number,page}; acknowledge this ID only after consuming the entire page. Seed data and tail events are distinct; lifecycle events are never inferred."}}},
          {"type":"object","additionalProperties":false,"required":["kind","names"],"properties":{"kind":{"const":"watches"},"names":{"type":"array","maxItems":16,"items":{"type":"string","minLength":1,"maxLength":64}}}},
          {"type":"object","additionalProperties":false,"required":["kind","page"],"properties":{"kind":{"const":"traversal"},"page":{"type":"object","additionalProperties":false,"required":["token","objects","next","stop","visited","total_visits"],"properties":{"token":{"type":"object"},"objects":{"type":"array","maxItems":1024,"items":{"type":"object"}},"next":{"type":["object","null"]},"stop":{"enum":["Complete","PageLimit","DepthLimit","NodeLimit","EdgeLimit","StateLimit"]},"visited":{"type":"integer","minimum":0,"maximum":1024},"total_visits":{"type":"integer","minimum":0,"maximum":16384}}}}},
          {"type":"object","additionalProperties":false,"required":["kind","reply"],"properties":{"kind":{"const":"mutation"},"reply":{"type":"object","description":"Typed frozen MutationReply; nested domain semantics are validated by focal-wire, not expanded by this schema."}}},
          {"type":"object","additionalProperties":false,"required":["kind","page"],"properties":{"kind":{"const":"read"},"page":{"$ref":"#/$defs/read_page"}}},
          {"type":"object","additionalProperties":false,"required":["kind","page"],"properties":{"kind":{"const":"list"},"page":{"$ref":"#/$defs/list_page"}}},
          {"type":"object","additionalProperties":false,"required":["kind","reply"],"properties":{"kind":{"const":"reconcile"},"reply":{"$ref":"#/$defs/reconcile_reply"}}},
          {"type":"object","additionalProperties":false,"required":["kind","code","detail"],"properties":{"kind":{"const":"error"},"code":{"type":"string","minLength":1,"maxLength":64},"detail":{"type":"string","maxLength":16384}}},
          {"type":"object","additionalProperties":false,"required":["kind","receipt"],"properties":{"kind":{"const":"managed"},"receipt":{"type":"object","description":"Exact validated ManagedReceipt, including the original key, intent, family outcome and committed index."}}},
          {"type":"object","additionalProperties":false,"required":["kind","state"],"properties":{"kind":{"const":"managed_request"},"state":{"enum":["Reserved","Pending","Consumed","Retired"]}}},
          {"type":"object","additionalProperties":false,"required":["kind","operation_ids"],"properties":{"kind":{"const":"managed_requests"},"operation_ids":{"type":"array","maxItems":256,"items":{"type":"string","minLength":78,"maxLength":78}}}},
          {"type":"object","additionalProperties":false,"required":["kind","reply"],"properties":{"kind":{"const":"managed_reconcile"},"reply":{"type":"object","description":"Authenticated RequestStreamReadReply, bound to the managed key and fresh quorum prefix; inspection never changes the journal."}}},
          {"type":"object","additionalProperties":false,"required":["kind","result"],"properties":{"kind":{"const":"administration"},"result":{"$ref":"#/$defs/administration"}}},
          {"type":"object","additionalProperties":false,"required":["kind","context"],"properties":{"kind":{"const":"validation_context"},"context":{"$ref":"#/$defs/validation_context"}}},
          {"type":"object","additionalProperties":false,"required":["kind","progress"],"properties":{"kind":{"const":"upload"},"progress":{"type":"object","description":"Durable local staging and actual server received offset; reference is present only after the server custody gate sealed the complete declared stream."}}},
          {"type":"object","additionalProperties":false,"required":["kind","artifact","token","content_hash","chunk"],"properties":{"kind":{"const":"artifact_payload"},"artifact":{"type":"array","minItems":16,"maxItems":16},"token":{"type":"object"},"content_hash":{"type":"array","minItems":32,"maxItems":32},"chunk":{"type":"object","description":"Verified bounded byte chunk, exact offset and EOF, bound to the immutable artifact's canonical content hash and observed token."}}}
        ]}
      },
      "$defs":{
        "node_identity":{"type":"object","additionalProperties":false,"required":["node","cluster","tenant","session","issuer","root"],"properties":{"node":{"type":"integer","minimum":1},"cluster":{"type":"string","pattern":"^[0-9a-f]{32}$"},"tenant":{"type":"string","pattern":"^[0-9a-f]{32}$"},"session":{"type":"string","pattern":"^[0-9a-f]{32}$"},"issuer":{"type":"string","pattern":"^[0-9a-f]{32}$"},"root":{"type":"string","pattern":"^[0-9a-f]{32}$"}}},
        "node_health":{"type":"object","additionalProperties":false,"required":["node","root_stopped","root_leader","root_term","root_applied_index","fleet_stopped","installed","running"],"properties":{"node":{"type":"integer","minimum":1},"root_stopped":{"type":"boolean"},"root_leader":{"type":"integer","minimum":0},"root_term":{"type":"integer","minimum":0},"root_applied_index":{"type":"integer","minimum":0},"fleet_stopped":{"type":"boolean"},"installed":{"type":"integer","minimum":0},"running":{"type":"integer","minimum":0}}},
        "node_configuration":{"type":"object","additionalProperties":false,"required":["node","network_schema","listen","advertise","root_group","root_tenant","root_session"],"properties":{"node":{"type":"integer","minimum":1},"network_schema":{"const":1},"listen":{"type":"string","maxLength":64},"advertise":{"type":"string","maxLength":64},"root_group":{"type":"string","pattern":"^[0-9a-f]{32}$"},"root_tenant":{"type":"string","pattern":"^[0-9a-f]{32}$"},"root_session":{"type":"string","pattern":"^[0-9a-f]{32}$"}}},
        "replica_diagnostics":{"type":"object","additionalProperties":false,"required":["node","cluster","session","group","leader","term","committed_index","applied_index","sequence","pending","authoritative","persistence_pending","checkpoint_pending","compiled_managed_decoder","required_decoder","managed_active"],"properties":{"node":{"type":"integer","minimum":1},"cluster":{"type":"string","pattern":"^[0-9a-f]{32}$"},"session":{"type":"string","pattern":"^[0-9a-f]{32}$"},"group":{"type":"string","pattern":"^[0-9a-f]{32}$"},"leader":{"type":"integer","minimum":0},"term":{"type":"integer","minimum":0},"committed_index":{"type":"integer","minimum":0},"applied_index":{"type":"integer","minimum":0},"sequence":{"type":"integer","minimum":0},"pending":{"type":"integer","minimum":0},"authoritative":{"type":"boolean"},"persistence_pending":{"type":"boolean"},"checkpoint_pending":{"type":"boolean"},"compiled_managed_decoder":{"type":"string","pattern":"^[0-9a-f]{64}$"},"required_decoder":{"type":["string","null"],"pattern":"^[0-9a-f]{64}$"},"managed_active":{"type":"boolean"}}},
        "replica_membership":{"type":"object","additionalProperties":false,"required":["cluster","tenant","session","group","configuration_index","voters","learners","voters_outgoing","learners_next","auto_leave"],"properties":{"cluster":{"type":"string","pattern":"^[0-9a-f]{32}$"},"tenant":{"type":"string","pattern":"^[0-9a-f]{32}$"},"session":{"type":"string","pattern":"^[0-9a-f]{32}$"},"group":{"type":"string","pattern":"^[0-9a-f]{32}$"},"configuration_index":{"type":"integer","minimum":0},"voters":{"type":"array","maxItems":1024,"items":{"type":"integer","minimum":1}},"learners":{"type":"array","maxItems":1024,"items":{"type":"integer","minimum":1}},"voters_outgoing":{"type":"array","maxItems":1024,"items":{"type":"integer","minimum":1}},"learners_next":{"type":"array","maxItems":1024,"items":{"type":"integer","minimum":1}},"auto_leave":{"type":"boolean"}}},
        "administration":{"oneOf":[
          {"type":"object","additionalProperties":false,"required":["kind","identity"],"properties":{"kind":{"const":"node_identity"},"identity":{"$ref":"#/$defs/node_identity"}}},
          {"type":"object","additionalProperties":false,"required":["kind","health"],"properties":{"kind":{"const":"node_health"},"health":{"$ref":"#/$defs/node_health"}}},
          {"type":"object","additionalProperties":false,"required":["kind","configuration"],"properties":{"kind":{"const":"node_configuration"},"configuration":{"$ref":"#/$defs/node_configuration"}}},
          {"type":"object","additionalProperties":false,"required":["kind","diagnostics"],"properties":{"kind":{"const":"replica_diagnostics"},"diagnostics":{"$ref":"#/$defs/replica_diagnostics"}}},
          {"type":"object","additionalProperties":false,"required":["kind","session","group","target"],"properties":{"kind":{"const":"replica_transfer_initiated"},"session":{"type":"string","pattern":"^[0-9a-f]{32}$"},"group":{"type":"string","pattern":"^[0-9a-f]{32}$"},"target":{"type":"integer","minimum":1}}},
          {"type":"object","additionalProperties":false,"required":["kind","node","management_sequence","replicas","next"],"properties":{"kind":{"const":"replicas"},"node":{"type":"integer","minimum":1},"management_sequence":{"type":"integer","minimum":0},"replicas":{"type":"array","maxItems":64,"items":{"type":"object","description":"Typed AdminReplicaStatus: local installed group and progress, not quorum or placement authority."}},"next":{"type":["string","null"]}}},
          {"type":"object","additionalProperties":false,"required":["kind","membership"],"properties":{"kind":{"const":"replica_membership"},"membership":{"$ref":"#/$defs/replica_membership"}}},
          {"type":"object","additionalProperties":false,"required":["kind","operation_id","request_id","request_hash","committed_index","committed_term","membership"],"properties":{"kind":{"const":"replica_committed"},"operation_id":{"type":"string","pattern":"^r1:[0-9a-f]{16}:[0-9a-f]{32}$"},"request_id":{"type":"string","pattern":"^[0-9a-f]{32}$"},"request_hash":{"type":"string","pattern":"^[0-9a-f]{64}$"},"committed_index":{"type":"integer","minimum":1},"committed_term":{"type":"integer","minimum":1},"membership":{"$ref":"#/$defs/replica_membership"}}},
          {"type":"object","additionalProperties":false,"required":["kind","operation_id","session","group","state"],"properties":{"kind":{"const":"replica_request"},"operation_id":{"type":"string","pattern":"^r1:[0-9a-f]{16}:[0-9a-f]{32}$"},"session":{"type":"string","pattern":"^[0-9a-f]{32}$"},"group":{"type":"string","pattern":"^[0-9a-f]{32}$"},"state":{"enum":["Pending","FencedOutcomeUnknown"]}}},
          {"type":"object","additionalProperties":false,"required":["kind","name","output"],"properties":{"kind":{"const":"invitation_written"},"name":{"type":"string","minLength":1,"maxLength":63},"output":{"type":"string","minLength":1,"maxLength":4096}}},
          {"type":"object","additionalProperties":false,"required":["kind","cluster","group","applied_index","revision","entries","next"],"properties":{"kind":{"const":"invitations"},"cluster":{"type":"string","pattern":"^[0-9a-f]{32}$"},"group":{"type":"string","pattern":"^[0-9a-f]{32}$"},"applied_index":{"type":"integer","minimum":1},"revision":{"type":"integer","minimum":0},"entries":{"type":"array","maxItems":64,"items":{"type":"object","description":"Redacted typed AdminInvitation and optional issued AdminCredential. No token, token hash or private credential is returned."}},"next":{"type":["string","null"]}}},
          {"type":"object","additionalProperties":false,"required":["kind","node","leader","term","applied_index","voters","learners"],"properties":{"kind":{"const":"membership"},"node":{"type":"integer","minimum":1},"leader":{"type":"integer","minimum":0},"term":{"type":"integer","minimum":1},"applied_index":{"type":"integer","minimum":1},"voters":{"type":"array","maxItems":1024,"items":{"type":"integer","minimum":1}},"learners":{"type":"array","maxItems":1024,"items":{"type":"integer","minimum":1}}}},
          {"type":"object","additionalProperties":false,"required":["kind","configuration"],"properties":{"kind":{"const":"configuration"},"configuration":{"type":"object","description":"Exact typed AdminConfiguration: cluster, group, genesis, applied/configuration indexes, voters, learners, outgoing voters, next learners, auto_leave. No data-placement guarantee is implied."}}},
          {"type":"object","additionalProperties":false,"required":["kind","cluster","group","applied_index","revision","nodes"],"properties":{"kind":{"const":"contacts"},"cluster":{"type":"string","pattern":"^[0-9a-f]{32}$"},"group":{"type":"string","pattern":"^[0-9a-f]{32}$"},"applied_index":{"type":"integer","minimum":0},"revision":{"type":"integer","minimum":0},"nodes":{"type":"array","maxItems":1024,"items":{"type":"object","description":"Typed committed contact announcement, not a live credential or placement grant."}}}},
          {"type":"object","additionalProperties":false,"required":["kind","operation_id","client","sequence","request_hash","committed_index","committed_term"],"properties":{"kind":{"const":"committed"},"operation_id":{"type":"string","pattern":"^a1:[0-9a-f]{16}:[0-9a-f]{16}$"},"client":{"type":"string","pattern":"^[0-9a-f]{32}$"},"sequence":{"type":"integer","minimum":1},"request_hash":{"type":"string","pattern":"^[0-9a-f]{64}$"},"committed_index":{"type":"integer","minimum":1},"committed_term":{"type":"integer","minimum":1}}},
          {"type":"object","additionalProperties":false,"required":["kind","target"],"properties":{"kind":{"const":"transfer_initiated"},"target":{"type":"integer","minimum":1}}},
          {"type":"object","additionalProperties":false,"required":["kind","operation_id","state"],"properties":{"kind":{"const":"request"},"operation_id":{"type":"string","pattern":"^a1:[0-9a-f]{16}:[0-9a-f]{16}$"},"state":{"enum":["Pending","Superseded"]}}}
        ]},
        "validation_context":{"type":"object","additionalProperties":false,"required":["token","validation_id","validation","claim","testament","records","next"],"properties":{"token":{"type":"object"},"validation_id":{"type":"array","minItems":16,"maxItems":16,"items":{"type":"integer","minimum":0,"maximum":255}},"validation":{"type":"object","description":"Pinned immutable requirement and its own stored lifecycle."},"claim":{"type":"object","description":"Owning claim at token; its status is not a testament or artifact status."},"testament":{"type":["object","null"],"description":"Current closing testament {id,value} at token, if one exists. It is not necessarily the target of the paged historical runs."},"records":{"type":"array","maxItems":1024,"items":{"type":"object"}},"next":{"type":["object","null"],"description":"Continue this validation with the same token as prefix and this position as after; this read grants no lease or unique artifact target."}}},
        "read_page":{"type":"object","additionalProperties":false,"required":["token","objects","next"],"properties":{"token":{"type":"object"},"objects":{"type":"array","maxItems":1024,"items":{"type":"object"}},"next":{"type":["object","null"]}}},
        "list_page":{"type":"object","additionalProperties":false,"required":["token","objects","next","visited"],"properties":{"token":{"type":"object"},"objects":{"type":"array","maxItems":1024,"items":{"type":"object"}},"next":{"type":["object","null"]},"visited":{"type":"integer","minimum":0,"maximum":1024}}},
        "reconcile_reply":{"type":"object","additionalProperties":false,"required":["token","applied_index","page"],"properties":{"token":{"type":"object"},"applied_index":{"type":"integer","minimum":1},"page":{"type":"object","additionalProperties":false,"required":["schema","ledger","principal","sequence","result"],"properties":{"schema":{"const":1},"ledger":{"type":"object"},"principal":{"type":"array","minItems":16,"maxItems":16,"items":{"type":"integer","minimum":0,"maximum":255}},"sequence":{"type":"integer","minimum":0},"result":{"type":"object","description":"Typed ReconcileResult with Epoch or Receipt; receipt resolution is Committed, CommittedCursor, BelowFloor or Unknown. Nested frozen model semantics and exact query/principal/prefix binding are enforced by focal-wire."}}}}}
      }
    }"##).map_err(|_|InputError::Invalid("released output schema"))
}
