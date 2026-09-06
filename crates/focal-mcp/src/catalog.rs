//! The MCP adapter adds only correlation/recovery fields to the shared DTO schema.
use crate::{ProtocolError, Tool};
use focal_client::operations;
use serde_json::{Map, Value};
#[path = "catalog_schema.rs"]
mod schema;

pub(crate) fn catalog() -> Result<Vec<Tool>, ProtocolError> {
    let mut tools = Vec::new();
    tools
        .try_reserve_exact(
            operations::descriptors()
                .len()
                .checked_add(6)
                .ok_or(ProtocolError::Capacity)?,
        )
        .map_err(|_| ProtocolError::Capacity)?;
    for descriptor in operations::descriptors() {
        let mut input = descriptor
            .input_schema()
            .map_err(|_| ProtocolError::Limits)?;
        schema::prune_definitions(&mut input)?;
        if descriptor.mutation {
            let object = input.as_object_mut().ok_or(ProtocolError::Limits)?;
            let properties = object
                .get_mut("properties")
                .and_then(Value::as_object_mut)
                .ok_or(ProtocolError::Limits)?;
            properties.insert("operation_id".into(), operation_id_schema());
            properties.insert(
                "expected_revision".into(),
                serde_json::json!({"type":"integer","minimum":0}),
            );
            match object
                .entry("required")
                .or_insert_with(|| Value::Array(Vec::new()))
            {
                Value::Array(required) => required.push(Value::String("operation_id".into())),
                _ => return Err(ProtocolError::Limits),
            }
        }
        tools.push(Tool {
            name: descriptor.name.into(),
            description: descriptor.description.into(),
            input_schema: input,
            output_schema: output_schema(descriptor.name)?,
            read_only: descriptor.read_only(),
            destructive: descriptor.destructive,
            // The adapter requires the durable operation ID on every mutation;
            // repeating that exact ID/intent resumes its saved request.
            idempotent: true,
        });
    }
    for (name, description, read_only) in [
        (
            "request.inspect",
            "Inspect saved operation state; remote:true queries the owner's retained receipt at a fresh quorum barrier and verifies the saved command without changing the journal.",
            true,
        ),
        (
            "request.retry",
            "Resume the exact saved operation by its durable ID after an unknown outcome.",
            false,
        ),
    ] {
        let mut properties = Map::new();
        properties.insert("operation_id".into(), operation_id_schema());
        if read_only {
            properties.insert(
                "remote".into(),
                serde_json::json!({"type":"boolean","default":false}),
            );
        }
        tools.push(Tool {name:name.into(),description:description.into(),
            input_schema:serde_json::json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":properties,"required":["operation_id"],"additionalProperties":false}),
            // A retry can finish a previously saved claim cancellation.
            output_schema:output_schema(name)?,read_only,destructive:!read_only,idempotent:true});
    }
    for (name, description, read_only, destructive, idempotent, takes_id) in [
        (
            "request.reserve",
            "Reserve a durable managed operation ID before calling a mutation tool. This executes no business work. Reservation is not idempotent: if its reply is lost, use request.pending to discover the reserved ID instead of reserving another.",
            false,
            false,
            false,
            false,
        ),
        (
            "request.pending",
            "List this MCP store's bounded outstanding managed operation IDs, including reservations whose replies were lost. Listing does not submit, acknowledge or seal work.",
            true,
            false,
            true,
            false,
        ),
        (
            "request.acknowledge",
            "Confirm you have consumed this managed operation's original result. Retire only the contiguous prefix of explicitly acknowledged results; retired IDs cannot execute again and their full results are no longer retained.",
            false,
            true,
            true,
            true,
        ),
        (
            "request.seal",
            "Resolve an unknown managed request: return its earlier committed result, or commit a fence preventing that exact request from executing. This does not cancel a business claim. Inspect the returned result, then explicitly acknowledge it when consumed.",
            false,
            true,
            true,
            true,
        ),
    ] {
        let mut properties = Map::new();
        if takes_id {
            properties.insert("operation_id".into(), managed_id_schema());
        }
        let required = if takes_id {
            vec!["operation_id"]
        } else {
            Vec::new()
        };
        tools.push(Tool {
            name:name.into(), description:description.into(),
            input_schema:serde_json::json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":properties,"required":required,"additionalProperties":false}),
            output_schema:output_schema(name)?, read_only,destructive,idempotent,
        });
    }
    Ok(tools)
}
fn operation_id_schema() -> Value {
    serde_json::json!({"anyOf":[
        {"type":"string","pattern":"^[0-9a-f]{32}$","minLength":32,"maxLength":32,"not":{"const":"00000000000000000000000000000000"}},
        managed_id_schema()
    ],"description":"Use the managed ID returned by request.reserve, and preserve it for exact retries until explicitly acknowledged. Existing unqualified32hex legacy IDs retain their original permanent-binding semantics; neither ID is a JSON-RPC request ID."})
}
fn managed_id_schema() -> Value {
    serde_json::json!({"type":"string","minLength":78,"maxLength":78,
        "pattern":"^m1:[0-9a-f]{8}:[0-9a-f]{16}:[0-9a-f]{16}:[0-9a-f]{32}$",
        "not":{"anyOf":[
            {"pattern":"^m1:[0-9a-f]{8}:0000000000000000:"},
            {"pattern":"^m1:[0-9a-f]{8}:[0-9a-f]{16}:0000000000000000:"},
            {"pattern":":00000000000000000000000000000000$"}
        ]},"description":"A previously reserved managed ID; supplying a missing or retired ID never creates fresh work."})
}

/// Preserve the common result envelope while retaining only this adapter's
/// possible result families. Frozen nested DTOs and their limits are unchanged.
pub(crate) fn output_schema(name: &str) -> Result<Value, ProtocolError> {
    let kinds: &[&str] = if name.starts_with("cluster.") {
        &["administration", "error"]
    } else {
        match name {
            "upload.begin" | "upload.append" | "upload.seal" | "upload.cancel" => {
                &["upload", "error"]
            }
            "artifact.download" => &["artifact_payload", "error"],
            "watch.inspect" => &["watch", "watches", "error"],
            "watch.open" | "watch.next" | "watch.acknowledge" => &["watch", "error"],
            "request.inspect" => &[
                "mutation",
                "managed",
                "managed_request",
                "managed_reconcile",
                "reconcile",
                "error",
            ],
            "request.retry" => &["mutation", "managed", "managed_request", "error"],
            "request.reserve" | "request.acknowledge" => &["managed_request", "error"],
            "request.pending" => &["managed_requests", "error"],
            "request.seal" => &["managed", "error"],
            "claim.wait" => &["claim_wait", "error"],
            "validation.context" => &["validation_context", "error"],
            "ledger.summary" => &["summary", "error"],
            "monitor.get" => &["monitor", "error"],
            "ledger.traverse" => &["traversal", "error"],
            _ => match operations::find(name)
                .ok_or(ProtocolError::Limits)?
                .result_kind
            {
                operations::ResultKind::Mutation => &["mutation", "managed", "error"],
                operations::ResultKind::Read => &["read", "error"],
                operations::ResultKind::List => &["list", "error"],
                operations::ResultKind::Reconcile => &["reconcile", "error"],
            },
        }
    };
    let mut output = operations::descriptors()
        .first()
        .ok_or(ProtocolError::Limits)?
        .output_schema()
        .map_err(|_| ProtocolError::Limits)?;
    schema::specialize_output(&mut output, kinds)?;
    // A per-tool schema has a distinct identity from the shared full union.
    output.as_object_mut().ok_or(ProtocolError::Limits)?.insert(
        "$id".into(),
        Value::String(format!("urn:focal:mcp:{name}:output:1")),
    );
    Ok(output)
}
