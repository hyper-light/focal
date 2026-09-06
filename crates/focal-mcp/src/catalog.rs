//! The MCP adapter adds only correlation/recovery fields to the shared DTO schema.
use crate::{ProtocolError, Tool};
use focal_client::operations;
use serde_json::{Map, Value};

pub(crate) fn catalog() -> Result<Vec<Tool>, ProtocolError> {
    let mut tools = Vec::new();
    tools
        .try_reserve_exact(
            operations::descriptors()
                .len()
                .checked_add(2)
                .ok_or(ProtocolError::Capacity)?,
        )
        .map_err(|_| ProtocolError::Capacity)?;
    for descriptor in operations::descriptors() {
        let mut input = descriptor
            .input_schema()
            .map_err(|_| ProtocolError::Limits)?;
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
            output_schema: descriptor
                .output_schema()
                .map_err(|_| ProtocolError::Limits)?,
            read_only: descriptor.read_only(),
            destructive: descriptor.destructive,
            // The adapter requires the durable operation ID on every mutation;
            // repeating that exact ID/intent resumes its saved request.
            idempotent: true,
        });
    }
    let output = operations::descriptors()
        .first()
        .ok_or(ProtocolError::Limits)?
        .output_schema()
        .map_err(|_| ProtocolError::Limits)?;
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
            output_schema:output.clone(),read_only,destructive:!read_only,idempotent:true});
    }
    Ok(tools)
}
fn operation_id_schema() -> Value {
    serde_json::json!({"type":"string","pattern":"^[0-9a-f]{32}$","minLength":32,"maxLength":32,"not":{"const":"00000000000000000000000000000000"},
        "description":"Caller-selected nonzero opaque ID, independent of JSON-RPC request ID. Preserve it for retry after cancellation or lost replies."})
}
