//! JSON Schema 2020-12 for native authored inputs. Serde DTOs remain the
//! decoder; tests check every released field and default against these.
use super::{InputError, OperationDescriptor, native_catalog::NativeInputKind};
use serde_json::{Map, Value};

const DEFINITIONS: &str = r##"{
  "id":{"type":"string","pattern":"^[0-9a-fA-F]{32}$","not":{"const":"00000000000000000000000000000000"}},
  "hash":{"type":"string","pattern":"^[0-9a-fA-F]{64}$","not":{"const":"0000000000000000000000000000000000000000000000000000000000000000"}},
  "participant":{"anyOf":[{"$ref":"#/$defs/id"},{"const":"self"}]},
  "text":{"type":"string","minLength":1,"maxLength":16384},
  "u32":{"type":"integer","minimum":0,"maximum":4294967295},
  "positive":{"type":"integer","minimum":1,"maximum":18446744073709551615},
  "optional_id":{"anyOf":[{"$ref":"#/$defs/id"},{"type":"null"}],"default":null},
  "scope":{"type":"object","additionalProperties":false,"required":["kind","key"],"properties":{"kind":{"enum":["file","symbol","api","test_surface","component","ux_surface"]},"key":{"type":"string","minLength":1,"maxLength":1024}}},
  "relation":{"type":"object","additionalProperties":false,"required":["kind","target"],"properties":{"kind":{"enum":["supersedes","depends_on","awaits","refines","conflicts_with","derived_from","reviews","amends","invalidates"]},"target":{"type":"string","pattern":"^(claim:[0-9a-fA-F]{32}|artifact:[0-9a-fA-F]{32}(@[0-9a-fA-F]{64})?)$","description":"A committed claim, or for reviews and derived_from the exact evidence artifact at its descriptor hash (a list filter may omit the hash); a correction invalidates exactly one challenge and reviews exactly one verdict artifact."}}},
  "peer_policy":{"type":"object","additionalProperties":false,"description":"How a challenge or consultation may be followed up; authored immutably with the claim (descriptor schema 2).","properties":{"corrective_allowed":{"type":"boolean","default":false},"max_follow_ups":{"type":"integer","minimum":0,"maximum":1024,"default":0},"single_issuer":{"type":"boolean","default":false},"escalation":{"enum":["none","holder","evaluator"],"default":"none"}}},
  "deadline":{"type":"object","additionalProperties":false,"required":["at"],"properties":{"at":{"$ref":"#/$defs/positive"},"timer":{"$ref":"#/$defs/optional_id"},"generation":{"$ref":"#/$defs/positive","default":1}}},
  "scope_limits":{"type":"object","additionalProperties":false,"required":["scopes","roots","children"],"properties":{"scopes":{"$ref":"#/$defs/u32"},"roots":{"$ref":"#/$defs/u32"},"children":{"$ref":"#/$defs/u32"}},"default":{"scopes":4,"roots":16,"children":8}},
  "handler":{"type":"object","additionalProperties":false,"required":["id","version"],"properties":{"id":{"$ref":"#/$defs/id"},"version":{"$ref":"#/$defs/hash"},"agentic":{"type":"boolean","default":false},"attempts":{"type":"integer","minimum":1,"maximum":64,"default":1},"proof_schema":{"anyOf":[{"$ref":"#/$defs/hash"},{"type":"null"}],"default":null},"diagnostic_schema":{"anyOf":[{"$ref":"#/$defs/hash"},{"type":"null"}],"default":null}}},
  "phase":{"type":"object","additionalProperties":false,"required":["evaluator","handlers"],"properties":{"evaluator":{"$ref":"#/$defs/participant"},"handlers":{"type":"array","minItems":1,"maxItems":64,"items":{"$ref":"#/$defs/handler"}},"required_policy":{"anyOf":[{"$ref":"#/$defs/hash"},{"type":"null"}],"default":null}}},
  "target":{"oneOf":[
    {"type":"object","additionalProperties":false,"required":["type"],"properties":{"type":{"const":"delivery"}}},
    {"type":"object","additionalProperties":false,"required":["type"],"properties":{"type":{"const":"admission"}}},
    {"type":"object","additionalProperties":false,"required":["type"],"properties":{"type":{"const":"increment"}}},
    {"type":"object","additionalProperties":false,"required":["type","index","name"],"properties":{"type":{"const":"slot"},"index":{"$ref":"#/$defs/u32"},"name":{"type":"string","minLength":1,"maxLength":256}}}
  ]},
  "validation":{"type":"object","additionalProperties":false,"required":["kind","description","deadline"],"properties":{
    "id":{"$ref":"#/$defs/optional_id"},"kind":{"enum":["receipt","test","inspection","integration","contract","design","regression"]},"phase":{"enum":["admission","increment","whole_work"],"default":"whole_work"},"mode":{"enum":["observe","required"],"default":"required"},"description":{"$ref":"#/$defs/text"},
    "target":{"anyOf":[{"$ref":"#/$defs/target"},{"type":"null"}],"default":null},"evaluator":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}],"default":null},"handlers":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/handler"},"default":[]},"required_policy":{"anyOf":[{"$ref":"#/$defs/hash"},{"type":"null"}],"default":null},
    "quality":{"anyOf":[{"$ref":"#/$defs/phase"},{"type":"null"}],"default":null},"quality_bar":{"anyOf":[{"$ref":"#/$defs/text"},{"type":"null"}],"default":null},"contributed_by":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/participant"},"default":[]},"policy_revision":{"anyOf":[{"$ref":"#/$defs/positive"},{"type":"null"}],"default":null},"deadline":{"$ref":"#/$defs/deadline"}
  }},
  "check":{"type":"object","additionalProperties":false,"required":["declaration"],"properties":{"declaration":{"$ref":"#/$defs/u32"},"mode":{"enum":["observe","required"],"default":"required"}}},
  "slot":{"type":"object","additionalProperties":false,"required":["slot"],"properties":{"slot":{"$ref":"#/$defs/u32"},"mode":{"enum":["observe","required"],"default":"required"},"missing":{"anyOf":[{"$ref":"#/$defs/u32"},{"type":"null"}],"default":null},"checks":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/check"},"default":[]}}},
  "claim":{"type":"object","description":"A complete authored claim. Issuer, subject, action and cause relations are derived from the authenticated actor, target, action and parent. Exactly one required whole-work receipt (delivery) declaration is mandatory.","additionalProperties":false,"required":["description","target","validations"],"properties":{
    "id":{"$ref":"#/$defs/optional_id"},"occurrence":{"$ref":"#/$defs/optional_id"},"description":{"$ref":"#/$defs/text"},"target":{"$ref":"#/$defs/participant"},"action":{"enum":["work","consultation","challenge","feedback","approval","summon","handoff","evaluation","correction","teardown"],"default":"work"},
    "scopes":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/scope"},"default":[]},"relations":{"type":"array","maxItems":248,"items":{"$ref":"#/$defs/relation"},"default":[]},"deadline":{"anyOf":[{"$ref":"#/$defs/deadline"},{"type":"null"}],"default":null},
    "validations":{"type":"array","minItems":1,"maxItems":64,"items":{"$ref":"#/$defs/validation"}},"slots":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/slot"},"default":[]},"max_responses":{"type":"integer","minimum":1,"maximum":4294967295,"default":4},"scope_limits":{"$ref":"#/$defs/scope_limits"},"parent":{"$ref":"#/$defs/optional_id"},"policy":{"anyOf":[{"$ref":"#/$defs/peer_policy"},{"type":"null"}],"default":null}
  }},
  "evidence_reference":{"type":"string","pattern":"^[0-9a-fA-F]{32}(@[0-9a-fA-F]{64})?$","description":"A committed artifact by identity, optionally pinned at its descriptor hash; an omitted hash is read from the ledger before the frame is compiled."},
  "challenge":{"type":"object","description":"A challenge: the subject must prove or redo the stated work under the acceptance requirements. Compiles to claim.submit with action challenge; the policy is mandatory and immutable.","additionalProperties":false,"required":["description","target","validations","policy"],"properties":{"id":{"$ref":"#/$defs/optional_id"},"occurrence":{"$ref":"#/$defs/optional_id"},"scopes":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/scope"},"default":[]},"relations":{"type":"array","maxItems":248,"items":{"$ref":"#/$defs/relation"},"default":[]},"deadline":{"anyOf":[{"$ref":"#/$defs/deadline"},{"type":"null"}],"default":null},"validations":{"type":"array","minItems":1,"maxItems":64,"items":{"$ref":"#/$defs/validation"}},"slots":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/slot"},"default":[]},"max_responses":{"type":"integer","minimum":1,"maximum":4294967295,"default":4},"scope_limits":{"$ref":"#/$defs/scope_limits"},"parent":{"$ref":"#/$defs/optional_id"},"description":{"$ref":"#/$defs/text"},"target":{"$ref":"#/$defs/participant"},"artifact":{"anyOf":[{"$ref":"#/$defs/evidence_reference"},{"type":"null"}],"default":null},"policy":{"$ref":"#/$defs/peer_policy"}}},
  "consult":{"type":"object","description":"A consultation: the subject answers the query (description) under the declared quality bar. Compiles to claim.submit with action consultation.","additionalProperties":false,"required":["description","target","validations"],"properties":{"id":{"$ref":"#/$defs/optional_id"},"occurrence":{"$ref":"#/$defs/optional_id"},"scopes":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/scope"},"default":[]},"relations":{"type":"array","maxItems":248,"items":{"$ref":"#/$defs/relation"},"default":[]},"deadline":{"anyOf":[{"$ref":"#/$defs/deadline"},{"type":"null"}],"default":null},"validations":{"type":"array","minItems":1,"maxItems":64,"items":{"$ref":"#/$defs/validation"}},"slots":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/slot"},"default":[]},"max_responses":{"type":"integer","minimum":1,"maximum":4294967295,"default":4},"scope_limits":{"$ref":"#/$defs/scope_limits"},"parent":{"$ref":"#/$defs/optional_id"},"description":{"$ref":"#/$defs/text"},"target":{"$ref":"#/$defs/participant"},"policy":{"anyOf":[{"$ref":"#/$defs/peer_policy"},{"type":"null"}],"default":null}}},
  "correction":{"type":"object","description":"The correction of a challenge whose verdict failed: invalidates the challenge and reviews the exact verdict report. Compiles to claim.submit with action correction; admitted only under the challenge's policy.","additionalProperties":false,"required":["challenge","verdict","description","validations"],"properties":{"id":{"$ref":"#/$defs/optional_id"},"occurrence":{"$ref":"#/$defs/optional_id"},"scopes":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/scope"},"default":[]},"relations":{"type":"array","maxItems":248,"items":{"$ref":"#/$defs/relation"},"default":[]},"deadline":{"anyOf":[{"$ref":"#/$defs/deadline"},{"type":"null"}],"default":null},"validations":{"type":"array","minItems":1,"maxItems":64,"items":{"$ref":"#/$defs/validation"}},"slots":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/slot"},"default":[]},"max_responses":{"type":"integer","minimum":1,"maximum":4294967295,"default":4},"scope_limits":{"$ref":"#/$defs/scope_limits"},"parent":{"$ref":"#/$defs/optional_id"},"challenge":{"$ref":"#/$defs/id"},"verdict":{"$ref":"#/$defs/evidence_reference"},"description":{"$ref":"#/$defs/text"},"target":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}],"default":null},"policy":{"anyOf":[{"$ref":"#/$defs/peer_policy"},{"type":"null"}],"default":null}}},
  "follow_up":{"type":"object","description":"A follow-up consultation refining a committed consultation, addressed to its subject unless a target is given. Compiles to claim.submit with action consultation and a refines relation.","additionalProperties":false,"required":["refines","description","validations"],"properties":{"id":{"$ref":"#/$defs/optional_id"},"occurrence":{"$ref":"#/$defs/optional_id"},"scopes":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/scope"},"default":[]},"relations":{"type":"array","maxItems":248,"items":{"$ref":"#/$defs/relation"},"default":[]},"deadline":{"anyOf":[{"$ref":"#/$defs/deadline"},{"type":"null"}],"default":null},"validations":{"type":"array","minItems":1,"maxItems":64,"items":{"$ref":"#/$defs/validation"}},"slots":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/slot"},"default":[]},"max_responses":{"type":"integer","minimum":1,"maximum":4294967295,"default":4},"scope_limits":{"$ref":"#/$defs/scope_limits"},"parent":{"$ref":"#/$defs/optional_id"},"refines":{"$ref":"#/$defs/id"},"description":{"$ref":"#/$defs/text"},"target":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}],"default":null},"policy":{"anyOf":[{"$ref":"#/$defs/peer_policy"},{"type":"null"}],"default":null}}},
  "wait":{"type":"object","description":"Observe one claim until the predicate holds within timeout_ms (1..=30000).","additionalProperties":false,"required":["claim","until"],"properties":{"claim":{"$ref":"#/$defs/id"},"until":{"enum":["testament","satisfied","terminal","released"]},"timeout_ms":{"type":"integer","minimum":1,"maximum":30000,"default":30000}}},
  "claim_target":{"type":"object","additionalProperties":false,"required":["claim"],"properties":{"claim":{"$ref":"#/$defs/id"}}},
  "object":{"type":"object","description":"One object identity of the native prefix.","additionalProperties":false,"required":["id"],"properties":{"id":{"$ref":"#/$defs/id"}}},
  "empty":{"type":"object","description":"No input; the authenticated context selects the ledger and principal.","additionalProperties":false,"properties":{}},
  "receipt":{"type":"object","additionalProperties":false,"required":["claim"],"properties":{"claim":{"$ref":"#/$defs/id"},"id":{"$ref":"#/$defs/optional_id"}}},
  "claim_list":{"type":"object","additionalProperties":false,"properties":{"issuer":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}],"default":null},"subject":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}],"default":null},"status":{"anyOf":[{"enum":["generated","posted","received","progressed","testament_generated","testament_acknowledged","validating","satisfied","post_failed","receipt_failed","testament_generation_failed","validation_incomplete","validation_failed","validation_errored","cancelled","expired","revoked","superseded","dependency_failed","deadlocked"]},{"type":"null"}],"default":null},"action":{"anyOf":[{"enum":["work","consultation","challenge","feedback","approval","summon","handoff","evaluation","correction","teardown"]},{"type":"null"}],"default":null},"scope":{"anyOf":[{"$ref":"#/$defs/scope"},{"type":"null"}],"default":null},"relation":{"anyOf":[{"$ref":"#/$defs/relation"},{"type":"null"}],"default":null},"created_after":{"anyOf":[{"$ref":"#/$defs/positive"},{"type":"null"}],"default":null},"cursor":{"anyOf":[{"type":"string","pattern":"^([0-9a-f]{2}){1,256}$"},{"type":"null"}],"default":null,"description":"Opaque continuation of the previous page in hexadecimal; keep the filter unchanged."},"limit":{"type":"integer","minimum":1,"maximum":256,"default":100},"max_visits":{"type":"integer","minimum":1,"maximum":65536,"default":1024,"description":"Most rows visited, matching or not, while filling the page."}}},
  "artifact_list":{"type":"object","additionalProperties":false,"properties":{"producer":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}],"default":null},"kind":{"anyOf":[{"type":"string","minLength":1,"maxLength":256},{"type":"null"}],"default":null},"schema":{"anyOf":[{"$ref":"#/$defs/hash"},{"type":"null"}],"default":null},"input":{"$ref":"#/$defs/optional_id"},"cursor":{"anyOf":[{"type":"string","pattern":"^([0-9a-f]{2}){1,256}$"},{"type":"null"}],"default":null,"description":"Opaque continuation of the previous page in hexadecimal; keep the filter unchanged."},"limit":{"type":"integer","minimum":1,"maximum":256,"default":100},"max_visits":{"type":"integer","minimum":1,"maximum":65536,"default":1024,"description":"Most rows visited, matching or not, while filling the page."}}},
  "validation_list":{"type":"object","additionalProperties":false,"properties":{"claim":{"$ref":"#/$defs/optional_id"},"evaluator":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}],"default":null},"cursor":{"anyOf":[{"type":"string","pattern":"^([0-9a-f]{2}){1,256}$"},{"type":"null"}],"default":null,"description":"Opaque continuation of the previous page in hexadecimal; keep the filter unchanged."},"limit":{"type":"integer","minimum":1,"maximum":256,"default":100},"max_visits":{"type":"integer","minimum":1,"maximum":65536,"default":1024,"description":"Most rows visited, matching or not, while filling the page."}}},
  "evaluation_list":{"type":"object","additionalProperties":false,"properties":{"claim":{"$ref":"#/$defs/optional_id"},"validation":{"$ref":"#/$defs/optional_id"},"evaluator":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}],"default":null},"verdict":{"anyOf":[{"enum":["pass","fail","incomplete","error"]},{"type":"null"}],"default":null},"cursor":{"anyOf":[{"type":"string","pattern":"^([0-9a-f]{2}){1,256}$"},{"type":"null"}],"default":null,"description":"Opaque continuation of the previous page in hexadecimal; keep the filter unchanged."},"limit":{"type":"integer","minimum":1,"maximum":256,"default":100},"max_visits":{"type":"integer","minimum":1,"maximum":65536,"default":1024,"description":"Most rows visited, matching or not, while filling the page."}}},
  "testament_list":{"type":"object","additionalProperties":false,"required":["claim"],"properties":{"claim":{"$ref":"#/$defs/id"},"cursor":{"anyOf":[{"type":"string","pattern":"^([0-9a-f]{2}){1,256}$"},{"type":"null"}],"default":null,"description":"Opaque continuation of the previous page in hexadecimal; keep the filter unchanged."},"limit":{"type":"integer","minimum":1,"maximum":256,"default":100},"max_visits":{"type":"integer","minimum":1,"maximum":65536,"default":1024,"description":"Most rows visited, matching or not, while filling the page."}}},
  "receipt_list":{"type":"object","additionalProperties":false,"properties":{"holder":{"anyOf":[{"$ref":"#/$defs/participant"},{"type":"null"}],"default":null},"claim":{"$ref":"#/$defs/optional_id"},"cursor":{"anyOf":[{"type":"string","pattern":"^([0-9a-f]{2}){1,256}$"},{"type":"null"}],"default":null,"description":"Opaque continuation of the previous page in hexadecimal; keep the filter unchanged."},"limit":{"type":"integer","minimum":1,"maximum":256,"default":100},"max_visits":{"type":"integer","minimum":1,"maximum":65536,"default":1024,"description":"Most rows visited, matching or not, while filling the page."}}},
  "monitor_list":{"type":"object","additionalProperties":false,"required":["claim"],"properties":{"claim":{"$ref":"#/$defs/id"},"cursor":{"anyOf":[{"type":"string","pattern":"^([0-9a-f]{2}){1,256}$"},{"type":"null"}],"default":null,"description":"Opaque continuation of the previous page in hexadecimal; keep the filter unchanged."},"limit":{"type":"integer","minimum":1,"maximum":256,"default":100},"max_visits":{"type":"integer","minimum":1,"maximum":65536,"default":1024,"description":"Most rows visited, matching or not, while filling the page."}}},
  "event_list":{"type":"object","additionalProperties":false,"properties":{"after":{"anyOf":[{"type":"object","additionalProperties":false,"required":["sequence","ordinal"],"properties":{"sequence":{"$ref":"#/$defs/positive"},"ordinal":{"$ref":"#/$defs/u32"}}},{"type":"null"}],"default":null},"cursor":{"anyOf":[{"type":"string","pattern":"^([0-9a-f]{2}){1,256}$"},{"type":"null"}],"default":null,"description":"Opaque continuation of the previous page in hexadecimal; keep the filter unchanged."},"limit":{"type":"integer","minimum":1,"maximum":256,"default":100},"max_visits":{"type":"integer","minimum":1,"maximum":65536,"default":1024,"description":"Most rows visited, matching or not, while filling the page."}}},
  "bytes":{"type":"array","maxItems":16384,"items":{"type":"integer","minimum":0,"maximum":255}},
  "payload":{"oneOf":[
    {"type":"object","additionalProperties":false,"required":["type","bytes"],"properties":{"type":{"const":"inline"},"bytes":{"type":"array","maxItems":262144,"items":{"type":"integer","minimum":0,"maximum":255}}}},
    {"type":"object","additionalProperties":false,"required":["type","text"],"properties":{"type":{"const":"text"},"text":{"type":"string","maxLength":262144}}}
  ]},
  "object_reference":{"type":"object","additionalProperties":false,"required":["kind","id"],"properties":{"kind":{"enum":["claim","testament","artifact","validation"]},"id":{"$ref":"#/$defs/id"}}},
  "artifact_fields":{"id":{"$ref":"#/$defs/optional_id"},"kind":{"anyOf":[{"type":"string","minLength":1,"maxLength":128,"pattern":"^[a-z0-9_./-]+$"},{"type":"null"}],"default":null},"schema_hash":{"anyOf":[{"$ref":"#/$defs/hash"},{"type":"null"}],"default":null},"metadata":{"$ref":"#/$defs/bytes","default":[]},"payload":{"$ref":"#/$defs/payload"},"inputs":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/object_reference"},"default":[]},"visibility":{"type":"array","maxItems":64,"items":{"type":"string","minLength":1,"maxLength":128},"default":[]}},
  "work_artifact":{"type":"object","description":"Work output for one manifest slot of the current cycle under the actor's receipt. kind and schema_hash must be given together; they default to the builtin test report schema.","additionalProperties":false,"required":["claim","slot","payload"],"properties":{"claim":{"$ref":"#/$defs/id"},"slot":{"$ref":"#/$defs/u32"}}},
  "diagnostic":{"type":"object","description":"A diagnostic for failed or impossible work under the actor's receipt; defaults to the builtin error report schema.","additionalProperties":false,"required":["claim","reason","payload"],"properties":{"claim":{"$ref":"#/$defs/id"},"reason":{"enum":["work","production","structure","metadata"]}}},
  "artifact_reference":{"type":"object","additionalProperties":false,"required":["id","hash"],"properties":{"id":{"$ref":"#/$defs/id"},"hash":{"$ref":"#/$defs/hash"}}},
  "slot_binding":{"type":"object","additionalProperties":false,"required":["slot","artifact"],"properties":{"slot":{"$ref":"#/$defs/u32"},"artifact":{"$ref":"#/$defs/artifact_reference"}}},
  "response":{"type":"object","description":"The respondent's explicit testimony closing the current work cycle. Every non-complete outcome must cite at least one diagnostic.","additionalProperties":false,"required":["claim","summary","confidence","outcome"],"allOf":[{"if":{"properties":{"outcome":{"enum":["partial","refused","impossible","interrupted","failed"]}},"required":["outcome"]},"then":{"properties":{"diagnostics":{"minItems":1}}}}],"properties":{
    "claim":{"$ref":"#/$defs/id"},"id":{"$ref":"#/$defs/optional_id"},"summary":{"$ref":"#/$defs/text"},"confidence":{"enum":["hint","tentative","committed","consensus"]},"outcome":{"enum":["complete","partial","refused","impossible","interrupted","failed"]},"manifest":{"type":"array","maxItems":256,"items":{"$ref":"#/$defs/slot_binding"},"default":[]},"diagnostics":{"type":"array","maxItems":64,"items":{"$ref":"#/$defs/artifact_reference"},"default":[]}
  }},
  "response_target":{"type":"object","additionalProperties":false,"required":["claim","testament"],"properties":{"claim":{"$ref":"#/$defs/id"},"testament":{"$ref":"#/$defs/id"}}},
  "evaluation":{"type":"object","description":"The current evaluation of one declaration under the claim: the whole-work slot evaluation by default, or the admission or increment evaluation named by phase (an increment optionally by its work artifact).","additionalProperties":false,"required":["claim","validation"],"properties":{"claim":{"$ref":"#/$defs/id"},"validation":{"$ref":"#/$defs/id"},"slot":{"anyOf":[{"$ref":"#/$defs/u32"},{"type":"null"}],"default":null},"phase":{"enum":["whole_work","admission","increment"],"default":"whole_work"},"target":{"$ref":"#/$defs/optional_id"}}},
  "report":{"type":"object","description":"The evaluator's fenced report for the begun attempt with its typed result artifact; error and incomplete verdicts default to the builtin error report schema.","additionalProperties":false,"required":["claim","validation","verdict","payload"],"properties":{"claim":{"$ref":"#/$defs/id"},"validation":{"$ref":"#/$defs/id"},"slot":{"anyOf":[{"$ref":"#/$defs/u32"},{"type":"null"}],"default":null},"phase":{"enum":["whole_work","admission","increment"],"default":"whole_work"},"target":{"$ref":"#/$defs/optional_id"},"verdict":{"enum":["pass","fail","incomplete","error"]}}},
  "adopt_receipt":{"type":"object","description":"The issuer replaces the current holder; the committed receipt is the previous fence.","additionalProperties":false,"required":["claim","holder"],"properties":{"claim":{"$ref":"#/$defs/id"},"holder":{"$ref":"#/$defs/participant"},"id":{"$ref":"#/$defs/optional_id"}}},
  "fail_work":{"type":"object","description":"One slot cannot be produced; cite your committed production diagnostic.","additionalProperties":false,"required":["claim","slot","diagnostic"],"properties":{"claim":{"$ref":"#/$defs/id"},"slot":{"$ref":"#/$defs/u32"},"diagnostic":{"$ref":"#/$defs/id"},"hash":{"anyOf":[{"$ref":"#/$defs/hash"},{"type":"null"}],"default":null}}},
  "artifact_target":{"type":"object","additionalProperties":false,"required":["claim","artifact"],"properties":{"claim":{"$ref":"#/$defs/id"},"artifact":{"$ref":"#/$defs/id"}}},
  "reject_work":{"type":"object","description":"The issuer's diagnostic for a structure or metadata failure of one exact work product; defaults to the builtin error report schema.","additionalProperties":false,"required":["claim","artifact","reason","payload"],"properties":{"claim":{"$ref":"#/$defs/id"},"artifact":{"$ref":"#/$defs/id"},"reason":{"enum":["structure","metadata"]}}},
  "audit":{"type":"object","additionalProperties":false,"required":["claim"],"properties":{"claim":{"$ref":"#/$defs/id"},"id":{"$ref":"#/$defs/optional_id"}}},
  "audit_target":{"type":"object","additionalProperties":false,"required":["testament"],"properties":{"testament":{"$ref":"#/$defs/id"}}},
  "wait_root":{"type":"object","additionalProperties":false,"required":["predicate","claim"],"properties":{"predicate":{"enum":["satisfied","terminal","released"]},"claim":{"$ref":"#/$defs/id"}}},
  "monitor":{"type":"object","description":"A durable wait monitor on your claim over committed claims, with a logical-time deadline.","additionalProperties":false,"required":["claim","roots","deadline"],"properties":{"claim":{"$ref":"#/$defs/id"},"id":{"$ref":"#/$defs/optional_id"},"roots":{"type":"array","minItems":1,"maxItems":64,"items":{"$ref":"#/$defs/wait_root"}},"deadline":{"$ref":"#/$defs/deadline"}}},
  "monitor_rebind":{"type":"object","additionalProperties":false,"required":["claim","monitor","predecessor","successor"],"properties":{"claim":{"$ref":"#/$defs/id"},"monitor":{"$ref":"#/$defs/id"},"predecessor":{"$ref":"#/$defs/id"},"successor":{"$ref":"#/$defs/id"}}},
  "monitor_target":{"type":"object","additionalProperties":false,"required":["claim","monitor"],"properties":{"claim":{"$ref":"#/$defs/id"},"monitor":{"$ref":"#/$defs/id"}}},
  "context":{"type":"object","description":"The evaluator's view of one declaration at one prefix; the evaluation is selected like validation.begin.","additionalProperties":false,"required":["validation"],"properties":{"validation":{"$ref":"#/$defs/id"},"phase":{"enum":["whole_work","admission","increment"],"default":"whole_work"},"slot":{"anyOf":[{"$ref":"#/$defs/u32"},{"type":"null"}],"default":null},"target":{"$ref":"#/$defs/optional_id"},"generation":{"anyOf":[{"$ref":"#/$defs/positive"},{"type":"null"}],"default":null},"results_after":{"anyOf":[{"$ref":"#/$defs/positive"},{"type":"null"}],"default":null},"limit":{"type":"integer","minimum":1,"maximum":256,"default":16}}}
}"##;

pub(super) fn input(
    descriptor: &OperationDescriptor,
    kind: NativeInputKind,
) -> Result<Value, InputError> {
    let mut definitions: Map<String, Value> = serde_json::from_str(DEFINITIONS)
        .map_err(|_| InputError::Invalid("released native schema definition"))?;
    let artifact_fields = definitions
        .remove("artifact_fields")
        .and_then(|value| match value {
            Value::Object(fields) => Some(fields),
            _ => None,
        })
        .ok_or(InputError::Invalid("native artifact fields"))?;
    for name in ["work_artifact", "diagnostic", "report", "reject_work"] {
        let fields = definitions
            .get_mut(name)
            .and_then(|value| value.get_mut("properties"))
            .and_then(Value::as_object_mut)
            .ok_or(InputError::Invalid("native artifact schema"))?;
        for (field, schema) in &artifact_fields {
            fields.insert(field.clone(), schema.clone());
        }
    }
    let key = match kind {
        NativeInputKind::Claim => "claim",
        NativeInputKind::ClaimTarget => "claim_target",
        NativeInputKind::Receipt => "receipt",
        NativeInputKind::WorkArtifact => "work_artifact",
        NativeInputKind::Diagnostic => "diagnostic",
        NativeInputKind::Response => "response",
        NativeInputKind::ResponseTarget => "response_target",
        NativeInputKind::Evaluation => "evaluation",
        NativeInputKind::Report => "report",
        NativeInputKind::ObjectId => "object",
        NativeInputKind::Empty => "empty",
        NativeInputKind::ClaimList => "claim_list",
        NativeInputKind::ArtifactList => "artifact_list",
        NativeInputKind::ValidationList => "validation_list",
        NativeInputKind::EvaluationList => "evaluation_list",
        NativeInputKind::TestamentList => "testament_list",
        NativeInputKind::ReceiptList => "receipt_list",
        NativeInputKind::MonitorList => "monitor_list",
        NativeInputKind::EventList => "event_list",
        NativeInputKind::AdoptReceipt => "adopt_receipt",
        NativeInputKind::FailWork => "fail_work",
        NativeInputKind::ArtifactTarget => "artifact_target",
        NativeInputKind::RejectWork => "reject_work",
        NativeInputKind::Audit => "audit",
        NativeInputKind::AuditTarget => "audit_target",
        NativeInputKind::Monitor => "monitor",
        NativeInputKind::MonitorRebind => "monitor_rebind",
        NativeInputKind::MonitorTarget => "monitor_target",
        NativeInputKind::Context => "context",
        NativeInputKind::Challenge => "challenge",
        NativeInputKind::Consult => "consult",
        NativeInputKind::Correction => "correction",
        NativeInputKind::FollowUp => "follow_up",
        NativeInputKind::Wait => "wait",
    };
    let mut schema = definitions
        .get(key)
        .and_then(Value::as_object)
        .cloned()
        .ok_or(InputError::Invalid("released native input schema"))?;
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
