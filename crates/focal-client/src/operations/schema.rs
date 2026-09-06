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
    let definitions: Map<String, Value> = serde_json::from_str(DEFINITIONS)
        .map_err(|_| InputError::Invalid("released schema definition"))?;
    let key = match descriptor.input {
        InputKind::Claim => "claim",
        InputKind::Testament => "testament",
        InputKind::Artifact => "artifact",
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
                "source" | "target" | "status" | "action" => {
                    descriptor.family == Some(ObjectKind::Claim)
                }
                "testament" | "producer" | "schema_hash" => {
                    descriptor.family == Some(ObjectKind::Artifact)
                }
                "evaluator" | "phase" | "mode" => descriptor.family == Some(ObjectKind::Validation),
                "kind" => matches!(
                    descriptor.family,
                    Some(ObjectKind::Artifact | ObjectKind::Validation)
                ),
                _ => true,
            };
            if !allowed {
                *property = null_schema();
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
          {"type":"object","additionalProperties":false,"required":["kind","reply"],"properties":{"kind":{"const":"mutation"},"reply":{"type":"object","description":"Typed frozen MutationReply; nested domain semantics are validated by focal-wire, not expanded by this schema."}}},
          {"type":"object","additionalProperties":false,"required":["kind","page"],"properties":{"kind":{"const":"read"},"page":{"$ref":"#/$defs/read_page"}}},
          {"type":"object","additionalProperties":false,"required":["kind","page"],"properties":{"kind":{"const":"list"},"page":{"$ref":"#/$defs/list_page"}}},
          {"type":"object","additionalProperties":false,"required":["kind","reply"],"properties":{"kind":{"const":"reconcile"},"reply":{"$ref":"#/$defs/reconcile_reply"}}},
          {"type":"object","additionalProperties":false,"required":["kind","code","detail"],"properties":{"kind":{"const":"error"},"code":{"type":"string","minLength":1,"maxLength":64},"detail":{"type":"string","maxLength":16384}}}
        ]}
      },
      "$defs":{
        "read_page":{"type":"object","additionalProperties":false,"required":["token","objects","next"],"properties":{"token":{"type":"object"},"objects":{"type":"array","maxItems":1024,"items":{"type":"object"}},"next":{"type":["object","null"]}}},
        "list_page":{"type":"object","additionalProperties":false,"required":["token","objects","next","visited"],"properties":{"token":{"type":"object"},"objects":{"type":"array","maxItems":1024,"items":{"type":"object"}},"next":{"type":["object","null"]},"visited":{"type":"integer","minimum":0,"maximum":1024}}},
        "reconcile_reply":{"type":"object","additionalProperties":false,"required":["token","applied_index","page"],"properties":{"token":{"type":"object"},"applied_index":{"type":"integer","minimum":1},"page":{"type":"object","additionalProperties":false,"required":["schema","ledger","principal","sequence","result"],"properties":{"schema":{"const":1},"ledger":{"type":"object"},"principal":{"type":"array","minItems":16,"maxItems":16,"items":{"type":"integer","minimum":0,"maximum":255}},"sequence":{"type":"integer","minimum":0},"result":{"type":"object","description":"Typed ReconcileResult with Epoch or Receipt; receipt resolution is Committed, CommittedCursor, BelowFloor or Unknown. Nested frozen model semantics and exact query/principal/prefix binding are enforced by focal-wire."}}}}}
      }
    }"##).map_err(|_|InputError::Invalid("released output schema"))
}
