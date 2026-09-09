use super::{
    Capability, InputKind, OperationDescriptor, ResultKind, RetryIdentity, Surface, WireProfile,
};
use crate::input::MAX_INPUT_BYTES;

/// Chunked payload transfer on the V1 engine (the native engine takes payloads inline).
const UPLOAD_BEGIN: OperationDescriptor = OperationDescriptor {
    name: "upload.begin",
    version: 1,
    description: "Bind a caller-known transfer ID to the exact length, BLAKE3 raw-stream digest, class and authenticated context. Repeat exact metadata after response loss; this does not attach an artifact.",
    capability: Capability::Actor,
    mutation: true,
    destructive: false,
    result_kind: ResultKind::Mutation,
    max_input_bytes: MAX_INPUT_BYTES,
    wire: WireProfile::V1,
    retry: RetryIdentity::Exact,
    surface: Surface::Transfer,
    cli_path: None,
    input: InputKind::Literal(
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"urn:focal:transfer:upload.begin:input:1","type":"object","properties":{"upload_id":{"type":"string","pattern":"^[0-9a-f]{32}$","not":{"const":"00000000000000000000000000000000"}},"length":{"type":"integer","minimum":0,"maximum":67108864},"digest":{"type":"string","pattern":"^[0-9a-fA-F]{64}$"},"class":{"enum":["document","evidence","checkpoint"],"default":"evidence"}},"required":["upload_id","length","digest"],"additionalProperties":false}"#,
    ),
    family: None,
};
const UPLOAD_APPEND: OperationDescriptor = OperationDescriptor {
    name: "upload.append",
    version: 1,
    description: "Persist then transmit up to 64 KiB of exact bytes encoded as hexadecimal. Retry the same upload ID, offset and bytes; local staging and server received offsets are separate. This never attaches an artifact.",
    capability: Capability::Actor,
    mutation: true,
    destructive: false,
    result_kind: ResultKind::Mutation,
    max_input_bytes: MAX_INPUT_BYTES,
    wire: WireProfile::V1,
    retry: RetryIdentity::Exact,
    surface: Surface::Transfer,
    cli_path: None,
    input: InputKind::Literal(
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"urn:focal:transfer:upload.append:input:1","type":"object","properties":{"upload_id":{"type":"string","pattern":"^[0-9a-f]{32}$","not":{"const":"00000000000000000000000000000000"}},"offset":{"type":"integer","minimum":0},"bytes_hex":{"type":"string","minLength":2,"maxLength":131072,"pattern":"^(?:[0-9a-fA-F]{2})+$"}},"required":["upload_id","offset","bytes_hex"],"additionalProperties":false}"#,
    ),
    family: None,
};
const UPLOAD_SEAL: OperationDescriptor = OperationDescriptor {
    name: "upload.seal",
    version: 1,
    description: "Resume saved transfer requests and seal only the complete digest-verified stream through the server's current custody quorum. Use the returned reference in artifact.submit; the reference alone is not a committed attachment.",
    capability: Capability::Actor,
    mutation: true,
    destructive: false,
    result_kind: ResultKind::Mutation,
    max_input_bytes: MAX_INPUT_BYTES,
    wire: WireProfile::V1,
    retry: RetryIdentity::Exact,
    surface: Surface::Transfer,
    cli_path: None,
    input: InputKind::Literal(
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"urn:focal:transfer:upload.seal:input:1","type":"object","properties":{"upload_id":{"type":"string","pattern":"^[0-9a-f]{32}$","not":{"const":"00000000000000000000000000000000"}}},"required":["upload_id"],"additionalProperties":false}"#,
    ),
    family: None,
};
const UPLOAD_CANCEL: OperationDescriptor = OperationDescriptor {
    name: "upload.cancel",
    version: 1,
    description: "Stop local transfer work and durably fence this scoped upload ID before removing server staging. Exact cancellation is retryable; immutable content, artifacts and claims remain.",
    capability: Capability::Actor,
    mutation: true,
    destructive: true,
    result_kind: ResultKind::Mutation,
    max_input_bytes: MAX_INPUT_BYTES,
    wire: WireProfile::V1,
    retry: RetryIdentity::Exact,
    surface: Surface::Transfer,
    cli_path: Some("artifact upload cancel"),
    input: InputKind::Literal(
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"urn:focal:transfer:upload.cancel:input:1","type":"object","properties":{"upload_id":{"type":"string","pattern":"^[0-9a-f]{32}$","not":{"const":"00000000000000000000000000000000"}}},"required":["upload_id"],"additionalProperties":false}"#,
    ),
    family: None,
};
const ARTIFACT_DOWNLOAD: OperationDescriptor = OperationDescriptor {
    name: "artifact.download",
    version: 1,
    description: "Read a verified payload chunk after reading the exact artifact. Continue with the returned token, content hash and next offset until EOF. Does not fetch another prefix if the saved token expires.",
    capability: Capability::Actor,
    mutation: false,
    destructive: false,
    result_kind: ResultKind::Read,
    max_input_bytes: MAX_INPUT_BYTES,
    wire: WireProfile::V1,
    retry: RetryIdentity::Exact,
    surface: Surface::Transfer,
    cli_path: Some("get artifact"),
    input: InputKind::Literal(
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"urn:focal:transfer:artifact.download:input:1","type":"object","properties":{"id":{"type":"string","pattern":"^[0-9a-f]{32}$","not":{"const":"00000000000000000000000000000000"}},"token":{"type":["object","null"],"description":"Exact ReadToken from the preceding reply; required for a nonzero offset."},"offset":{"type":"integer","minimum":0,"default":0},"max_bytes":{"type":"integer","minimum":1,"maximum":65536,"default":65536}},"required":["id"],"additionalProperties":false}"#,
    ),
    family: None,
};
pub const TRANSFER_TOOL_COUNT: usize = 5;
const TRANSFER: [OperationDescriptor; TRANSFER_TOOL_COUNT] = [
    UPLOAD_BEGIN,
    UPLOAD_APPEND,
    UPLOAD_SEAL,
    UPLOAD_CANCEL,
    ARTIFACT_DOWNLOAD,
];
/// Name order is part of catalogue pagination.
pub fn transfer_descriptors() -> &'static [OperationDescriptor] {
    &TRANSFER
}
