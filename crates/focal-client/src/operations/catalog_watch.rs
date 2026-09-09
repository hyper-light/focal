use super::{
    Capability, InputKind, OperationDescriptor, ResultKind, RetryIdentity, Surface, WireProfile,
};
use crate::input::MAX_INPUT_BYTES;

/// Durable named watches: the same four tools on both engines.
const WATCH_OPEN: OperationDescriptor = OperationDescriptor {
    name: "watch.open",
    version: 1,
    description: "Create or resume a caller-named durable watch and return one retained page. Repeat exact options after output loss. No claim filter means all claims; family only selects presentation. This never acknowledges data.",
    capability: Capability::Actor,
    mutation: true,
    destructive: false,
    result_kind: ResultKind::Mutation,
    max_input_bytes: MAX_INPUT_BYTES,
    wire: WireProfile::V1,
    retry: RetryIdentity::Exact,
    surface: Surface::Watch,
    cli_path: Some("watch all"),
    input: InputKind::Literal(
        r#"{"type":"object","additionalProperties":false,"required":["name"],"properties":{"name":{"type":"string","minLength":1,"maxLength":64,"pattern":"^[A-Za-z0-9_.-]+$"},"claims":{"type":"array","maxItems":256,"items":{"type":"string","pattern":"^[0-9a-fA-F]{32}$"}},"family":{"enum":["claim","testament","artifact","validation",null]},"seed":{"type":"boolean","default":true},"max_items":{"type":"integer","minimum":1,"maximum":256,"default":64},"max_bytes":{"type":"integer","minimum":4096,"maximum":65536,"default":65536}}}"#,
    ),
    family: None,
};
const WATCH_NEXT: OperationDescriptor = OperationDescriptor {
    name: "watch.next",
    version: 1,
    description: "Return the exact unacknowledged page, or durably save the next page before returning it. Seed continuation preserves the original prefix; expiry is explicit. No lifecycle events are synthesized.",
    capability: Capability::Actor,
    mutation: true,
    destructive: false,
    result_kind: ResultKind::Mutation,
    max_input_bytes: MAX_INPUT_BYTES,
    wire: WireProfile::V1,
    retry: RetryIdentity::Exact,
    surface: Surface::Watch,
    cli_path: Some("watch resume"),
    input: InputKind::Literal(
        r#"{"type":"object","additionalProperties":false,"required":["name"],"properties":{"name":{"type":"string","minLength":1,"maxLength":64}}}"#,
    ),
    family: None,
};
const WATCH_ACKNOWLEDGE: OperationDescriptor = OperationDescriptor {
    name: "watch.acknowledge",
    version: 1,
    description: "Confirm consumption of the entire retained delivery. This persists a local consumed frontier; the next watch.next commits cursor acknowledgment and retires its managed receipt. Repeat the exact delivery ID after response loss. It never acknowledges a testament or validates work.",
    capability: Capability::Actor,
    mutation: true,
    destructive: false,
    result_kind: ResultKind::Mutation,
    max_input_bytes: MAX_INPUT_BYTES,
    wire: WireProfile::V1,
    retry: RetryIdentity::Exact,
    surface: Surface::Watch,
    cli_path: None,
    input: InputKind::Literal(
        r#"{"type":"object","additionalProperties":false,"required":["name","delivery_id"],"properties":{"name":{"type":"string","minLength":1,"maxLength":64},"delivery_id":{"type":"string","pattern":"^[0-9a-fA-F]{64}$"}}}"#,
    ),
    family: None,
};
const WATCH_INSPECT: OperationDescriptor = OperationDescriptor {
    name: "watch.inspect",
    version: 1,
    description: "Read saved watch status and any retained delivery. Omit name to list the bounded set of saved watches. Inspection does not consume or acknowledge a page.",
    capability: Capability::Actor,
    mutation: false,
    destructive: false,
    result_kind: ResultKind::Read,
    max_input_bytes: MAX_INPUT_BYTES,
    wire: WireProfile::V1,
    retry: RetryIdentity::Exact,
    surface: Surface::Watch,
    cli_path: Some("watch inspect"),
    input: InputKind::Literal(
        r#"{"type":"object","additionalProperties":false,"properties":{"name":{"type":"string","minLength":1,"maxLength":64}}}"#,
    ),
    family: None,
};
pub const WATCH_TOOL_COUNT: usize = 4;
const WATCH: [OperationDescriptor; WATCH_TOOL_COUNT] =
    [WATCH_OPEN, WATCH_NEXT, WATCH_ACKNOWLEDGE, WATCH_INSPECT];
/// Name order is part of catalogue pagination.
pub fn watch_descriptors() -> &'static [OperationDescriptor] {
    &WATCH
}
