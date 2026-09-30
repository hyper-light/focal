//! The native engine's examples: one authored document per native descriptor,
//! the single source every surface (CLI discovery, request files, MCP,
//! tests) draws on. Each is a complete document of the descriptor's own
//! contract, so what `schema example NAME --native` prints is what `submit`
//! and the native tools accept without hand-editing.
//!
//! The documents are illustrative, not fabricated outcomes: the object
//! identities (claim `…10`, validation `…11`, artifact `…15`, testament
//! `…16`, result testament `…17`, monitor `…18`, the claims `…40` and `…42`
//! a peer verb or a monitor names, the verdict report `…41`) stand for
//! committed objects the caller replaces with real ones, the subject `…02`,
//! evaluator `…03` and holder `…04` for enrolled participants. A claim's
//! target is never `self`: the owner refuses to post a claim whose issuer is
//! its subject. Every deadline is [`EXAMPLE_DEADLINE_MS`], far enough ahead
//! that the owner's wall clock never finds it expired.

/// 2100-01-01T00:00:00Z in milliseconds since the Unix epoch: the deadline
/// every native example carries (`deadline.at` is logical milliseconds, and
/// the node's logical time is its wall clock).
pub const EXAMPLE_DEADLINE_MS: u64 = 4_102_444_800_000;

/// The illustrative identities the examples name, for tests and hosts that
/// substitute committed ones (a quickstart replaces `claim` with the claim
/// it created).
pub mod example_ids {
    pub const CLAIM: &str = "00000000000000000000000000000010";
    pub const VALIDATION: &str = "00000000000000000000000000000011";
    pub const ARTIFACT: &str = "00000000000000000000000000000015";
    pub const ARTIFACT_HASH: &str =
        "1515151515151515151515151515151515151515151515151515151515151515";
    pub const TESTAMENT: &str = "00000000000000000000000000000016";
    pub const RESULT_TESTAMENT: &str = "00000000000000000000000000000017";
    pub const MONITOR: &str = "00000000000000000000000000000018";
    pub const SUBJECT: &str = "00000000000000000000000000000002";
    pub const EVALUATOR: &str = "00000000000000000000000000000003";
    pub const HOLDER: &str = "00000000000000000000000000000004";
    pub const HANDLER: &str = "00000000000000000000000000000077";
    pub const HANDLER_VERSION: &str =
        "7777777777777777777777777777777777777777777777777777777777777777";
    /// The committed challenge a correction invalidates, the consultation a
    /// follow-up refines, the claim a monitor waits on.
    pub const OTHER: &str = "00000000000000000000000000000040";
    /// The report artifact of that challenge's failed verdict.
    pub const VERDICT: &str = "00000000000000000000000000000041";
    pub const VERDICT_HASH: &str =
        "4141414141414141414141414141414141414141414141414141414141414141";
    pub const SUCCESSOR: &str = "00000000000000000000000000000042";
}

/// The raw example of one native descriptor. The receipt declaration, the
/// slot-0 test declaration and the two payloads are spelled out where they
/// occur (a `concat!` takes literals only); the tests pin them equal.
pub(super) fn native_raw_example(name: &str) -> Option<&'static str> {
    Some(match name {
        // ---- creation: the claim and its authored peer shapes ------------
        "claim.submit" => concat!(
            r#"{"description":"Deliver the checked report.","target":"00000000000000000000000000000002","scopes":[{"kind":"file","key":"report.json"}],"validations":["#,
            r#"{"kind":"receipt","description":"Receive the report testament.","deadline":{"at":4102444800000}}"#,
            "]}"
        ),
        "claim.challenge" => concat!(
            r#"{"description":"Prove the report covers the edge cases.","target":"00000000000000000000000000000002","artifact":"00000000000000000000000000000015@1515151515151515151515151515151515151515151515151515151515151515","validations":["#,
            r#"{"kind":"receipt","description":"Receive the report testament.","deadline":{"at":4102444800000}}"#,
            ",",
            r#"{"kind":"test","description":"The suite passes.","target":{"type":"slot","index":0,"name":"report"},"evaluator":"00000000000000000000000000000003","handlers":[{"id":"00000000000000000000000000000077","version":"7777777777777777777777777777777777777777777777777777777777777777"}],"deadline":{"at":4102444800000}}"#,
            r#"],"slots":[{"slot":0,"checks":[{"declaration":1}]}],"policy":{"corrective_allowed":true,"max_follow_ups":1,"single_issuer":true,"escalation":"evaluator"}}"#
        ),
        "claim.consult" => concat!(
            r#"{"description":"Which cases does the parser leave undefined?","target":"00000000000000000000000000000002","validations":["#,
            r#"{"kind":"receipt","description":"Receive the report testament.","deadline":{"at":4102444800000}}"#,
            r#"],"policy":{"max_follow_ups":2,"escalation":"holder"}}"#
        ),
        "claim.correct" => concat!(
            r#"{"challenge":"00000000000000000000000000000040","verdict":"00000000000000000000000000000041@4141414141414141414141414141414141414141414141414141414141414141","description":"Redo the inspection with the missing cases.","validations":["#,
            r#"{"kind":"receipt","description":"Receive the report testament.","deadline":{"at":4102444800000}}"#,
            "]}"
        ),
        "claim.follow_up" => concat!(
            r#"{"refines":"00000000000000000000000000000040","description":"And the unicode cases?","validations":["#,
            r#"{"kind":"receipt","description":"Receive the report testament.","deadline":{"at":4102444800000}}"#,
            "]}"
        ),
        // ---- the issuer's claim verbs ----------------------------------------
        "claim.post"
        | "claim.cancel"
        | "claim.release_scope"
        | "validation.seal_increments"
        | "audit.generate" => r#"{"claim":"00000000000000000000000000000010"}"#,
        "receipt.adopt" => {
            r#"{"claim":"00000000000000000000000000000010","holder":"00000000000000000000000000000004"}"#
        }
        "artifact.receive" => {
            r#"{"claim":"00000000000000000000000000000010","artifact":"00000000000000000000000000000015"}"#
        }
        "artifact.reject" => concat!(
            r#"{"claim":"00000000000000000000000000000010","artifact":"00000000000000000000000000000015","reason":"structure","payload":"#,
            r#"{"type":"text","text":"{\"code\":\"malformed\",\"message\":\"Not a test report.\"}"}"#,
            "}"
        ),
        "testament.receive" | "validation.enter_whole_work" => {
            r#"{"claim":"00000000000000000000000000000010","testament":"00000000000000000000000000000016"}"#
        }
        "audit.post" => r#"{"testament":"00000000000000000000000000000017"}"#,
        "monitor.register" => {
            r#"{"claim":"00000000000000000000000000000010","roots":[{"predicate":"satisfied","claim":"00000000000000000000000000000040"}],"deadline":{"at":4102444800000}}"#
        }
        "monitor.rebind" => {
            r#"{"claim":"00000000000000000000000000000010","monitor":"00000000000000000000000000000018","predecessor":"00000000000000000000000000000040","successor":"00000000000000000000000000000042"}"#
        }
        "monitor.cancel" => {
            r#"{"claim":"00000000000000000000000000000010","monitor":"00000000000000000000000000000018"}"#
        }
        // ---- the respondent's cycle ------------------------------------------
        "receipt.acquire" => r#"{"claim":"00000000000000000000000000000010"}"#,
        "artifact.submit" => concat!(
            r#"{"claim":"00000000000000000000000000000010","slot":0,"payload":"#,
            r#"{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}"#,
            "}"
        ),
        "artifact.diagnostic" => concat!(
            r#"{"claim":"00000000000000000000000000000010","reason":"work","payload":"#,
            r#"{"type":"text","text":"{\"code\":\"tool_unavailable\",\"message\":\"The required tool could not run\"}"}"#,
            "}"
        ),
        "artifact.fail" => {
            r#"{"claim":"00000000000000000000000000000010","slot":0,"diagnostic":"00000000000000000000000000000015"}"#
        }
        "testament.submit" => {
            r#"{"claim":"00000000000000000000000000000010","summary":"Suite passed; the report is attached.","confidence":"committed","outcome":"complete","manifest":[{"slot":0,"artifact":{"id":"00000000000000000000000000000015","hash":"1515151515151515151515151515151515151515151515151515151515151515"}}]}"#
        }
        "testament.post" => {
            r#"{"claim":"00000000000000000000000000000010","testament":"00000000000000000000000000000016"}"#
        }
        // ---- the evaluator's verbs -----------------------------------------------
        "validation.begin" => {
            r#"{"claim":"00000000000000000000000000000010","validation":"00000000000000000000000000000011"}"#
        }
        "validation.report" => concat!(
            r#"{"claim":"00000000000000000000000000000010","validation":"00000000000000000000000000000011","verdict":"pass","payload":"#,
            r#"{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}"#,
            "}"
        ),
        // ---- exact reads ---------------------------------------------------------
        "claim.get" | "claim.lineage" => r#"{"id":"00000000000000000000000000000010"}"#,
        "testament.get" => r#"{"id":"00000000000000000000000000000016"}"#,
        "artifact.get" => r#"{"id":"00000000000000000000000000000015"}"#,
        "validation.get" => r#"{"id":"00000000000000000000000000000011"}"#,
        "validation.context" => r#"{"validation":"00000000000000000000000000000011"}"#,
        "archive.get" => {
            r#"{"claim":"00000000000000000000000000000010","object":{"artifact":{"id":"00000000000000000000000000000015"}}}"#
        }
        "ledger.standing" => "{}",
        "claim.wait" => {
            r#"{"claim":"00000000000000000000000000000010","until":"testament","timeout_ms":30000}"#
        }
        // ---- bounded lists -------------------------------------------------------
        "claim.list" => r#"{"issuer":"self","status":"posted"}"#,
        "artifact.list" => r#"{"producer":"self"}"#,
        "validation.list" | "evaluation.list" | "testament.list" | "monitor.list" => {
            r#"{"claim":"00000000000000000000000000000010"}"#
        }
        "receipt.list" => r#"{"holder":"self"}"#,
        "event.list" => "{}",
        _ => return None,
    })
}
