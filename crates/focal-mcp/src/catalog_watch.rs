use crate::{ProtocolError, Tool};
pub(crate) const TOOL_COUNT: usize = 4;
pub(crate) fn append(tools: &mut Vec<Tool>) -> Result<(), ProtocolError> {
    tools
        .try_reserve_exact(TOOL_COUNT)
        .map_err(|_| ProtocolError::Capacity)?;

    for (name, description, schema, read) in [
        (
            "watch.open",
            "Create or resume a caller-named durable watch and return one retained page. Repeat exact options after output loss. No claim filter means all claims; family only selects presentation. This never acknowledges data.",
            r#"{"type":"object","additionalProperties":false,"required":["name"],"properties":{"name":{"type":"string","minLength":1,"maxLength":64,"pattern":"^[A-Za-z0-9_.-]+$"},"claims":{"type":"array","maxItems":256,"items":{"type":"string","pattern":"^[0-9a-fA-F]{32}$"}},"family":{"enum":["claim","testament","artifact","validation",null]},"seed":{"type":"boolean","default":true},"max_items":{"type":"integer","minimum":1,"maximum":256,"default":64},"max_bytes":{"type":"integer","minimum":4096,"maximum":65536,"default":65536}}}"#,
            false,
        ),
        (
            "watch.next",
            "Return the exact unacknowledged page, or durably save the next page before returning it. Seed continuation preserves the original prefix; expiry is explicit. No lifecycle events are synthesized.",
            r#"{"type":"object","additionalProperties":false,"required":["name"],"properties":{"name":{"type":"string","minLength":1,"maxLength":64}}}"#,
            false,
        ),
        (
            "watch.acknowledge",
            "Confirm consumption of the entire retained delivery. This persists a local consumed frontier; the next watch.next commits cursor acknowledgment and retires its managed receipt. Repeat the exact delivery ID after response loss. It never acknowledges a testament or validates work.",
            r#"{"type":"object","additionalProperties":false,"required":["name","delivery_id"],"properties":{"name":{"type":"string","minLength":1,"maxLength":64},"delivery_id":{"type":"string","pattern":"^[0-9a-fA-F]{64}$"}}}"#,
            false,
        ),
        (
            "watch.inspect",
            "Read saved watch status and any retained delivery. Omit name to list the bounded set of saved watches. Inspection does not consume or acknowledge a page.",
            r#"{"type":"object","additionalProperties":false,"properties":{"name":{"type":"string","minLength":1,"maxLength":64}}}"#,
            true,
        ),
    ] {
        tools.push(Tool {
            name: name.into(),
            description: description.into(),
            input_schema: serde_json::from_str(schema).map_err(|_| ProtocolError::Limits)?,
            output_schema: crate::catalog::output_schema(name)?,
            read_only: read,
            destructive: false,
            idempotent: true,
        });
    }
    Ok(())
}
