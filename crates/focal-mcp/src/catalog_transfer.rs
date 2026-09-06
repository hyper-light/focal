use crate::{ProtocolError, Tool};
use serde_json::json;
pub(crate) const TOOL_COUNT: usize = 5;

pub(crate) fn append(tools: &mut Vec<Tool>) -> Result<(), ProtocolError> {
    tools
        .try_reserve_exact(TOOL_COUNT)
        .map_err(|_| ProtocolError::Capacity)?;

    let id = json!({"type":"string","pattern":"^[0-9a-f]{32}$","not":{"const":"00000000000000000000000000000000"}});
    let hash = json!({"type":"string","pattern":"^[0-9a-fA-F]{64}$"});
    for (name, description, required, properties, read, destructive) in [
        (
            "upload.begin",
            "Bind a caller-known transfer ID to the exact length, BLAKE3 raw-stream digest, class and authenticated context. Repeat exact metadata after response loss; this does not attach an artifact.",
            vec!["upload_id", "length", "digest"],
            json!({"upload_id":id,"length":{"type":"integer","minimum":0,"maximum":67108864},"digest":hash,"class":{"enum":["document","evidence","checkpoint"],"default":"evidence"}}),
            false,
            false,
        ),
        (
            "upload.append",
            "Persist then transmit up to 64 KiB of exact bytes encoded as hexadecimal. Retry the same upload ID, offset and bytes; local staging and server received offsets are separate. This never attaches an artifact.",
            vec!["upload_id", "offset", "bytes_hex"],
            json!({"upload_id":id,"offset":{"type":"integer","minimum":0},"bytes_hex":{"type":"string","minLength":2,"maxLength":131072,"pattern":"^(?:[0-9a-fA-F]{2})+$"}}),
            false,
            false,
        ),
        (
            "upload.seal",
            "Resume saved transfer requests and seal only the complete digest-verified stream through the server's current custody quorum. Use the returned reference in artifact.submit; the reference alone is not a committed attachment.",
            vec!["upload_id"],
            json!({"upload_id":id}),
            false,
            false,
        ),
        (
            "upload.cancel",
            "Stop local transfer work and durably fence this scoped upload ID before removing server staging. Exact cancellation is retryable; immutable content, artifacts and claims remain.",
            vec!["upload_id"],
            json!({"upload_id":id}),
            false,
            true,
        ),
        (
            "artifact.download",
            "Read a verified payload chunk after reading the exact artifact. Continue with the returned token, content hash and next offset until EOF. Does not fetch another prefix if the saved token expires.",
            vec!["id"],
            json!({"id":id,"token":{"type":["object","null"],"description":"Exact ReadToken from the preceding reply; required for a nonzero offset."},"offset":{"type":"integer","minimum":0,"default":0},"max_bytes":{"type":"integer","minimum":1,"maximum":65536,"default":65536}}),
            true,
            false,
        ),
    ] {
        tools.push(Tool{name:name.into(),description:description.into(),input_schema:json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":format!("urn:focal:transfer:{name}:input:1"),"type":"object","properties":properties,"required":required,"additionalProperties":false}),output_schema:crate::catalog::output_schema(name)?,read_only:read,destructive,idempotent:true});
    }
    Ok(())
}
