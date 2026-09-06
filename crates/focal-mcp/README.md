# Focal MCP adapter

The protocol owner implements the local stdio profiles `2026-07-28` and
`2025-11-25`. It does not grant application authority or interpret a successful
JSON-RPC exchange as ledger commitment. The backend uses Focal's authenticated
client and durable operation store; its caller-selected operation IDs are separate
from JSON-RPC correlation IDs.

`Protocol::new(limits, budget, server_info, tools)` takes an immutable, bounded
catalog for one authenticated host context. Build that catalog under an allowance
before transferring ownership; the constructor measures and charges its retained
schema trees. Creating another protocol owner creates a fresh cursor secret.
Changing principal, ledger or visible capabilities requires a new owner/catalog;
ordinary calls must still pass authenticated server ingress.

`Protocol::receive(bytes)` parses one complete message. `receive_at(bytes, now_ms)`
injects monotone process-local time for deterministic hosts/tests. Use one clock
origin consistently. Modern requests carry their own protocol metadata; legacy
requests use explicit initialization. Discovery, paginated tool listing, tool
dispatch, cancellation and legacy ping share the same bounded owner. No optional
MCP extensions, reverse requests, resources or prompts are advertised.

`Action::Call` transfers an owned `ToolCall` and its allocation to the host.
Keep that value alive through execution. Its opaque `CallToken` fences completion
independently of the JSON-RPC ID. `complete(token, &result, is_error)` returns an
owned newline-terminated frame; `fail` returns an unstructured tool error when the
host cannot produce its normal result. Cancellation retains the active slot until
completion/failure, suppresses that reply, and never cancels a business claim or
retires a durable journal. Repeated/stale completion returns no frame.

`FrameDecoder::push` consumes at most one newline-terminated frame and returns the
consumed byte count. Retry the unused input suffix; do not collect an unbounded
batch. The decoder reserves its maximum frame allowance before accumulating bytes.
Oversized and unterminated-at-EOF messages fail closed. Input frames, tool calls
and encoded frames keep their own allowances, including after the protocol owner
is dropped. Their debug representations omit payloads. Output growth is geometric
and capped; the encoded frame retains its capacity charge until writing/discard.

The protocol owns a fixed completion-lane parsing workspace, bounded active-ID
storage and the catalog. Tool argument trees additionally require ordinary-lane
admission. Completing a tool temporarily needs up to twice `max_response_bytes`
for compatibility text and final JSON, plus existing parser, input and queued
output allowances. The host must size its reserve accordingly. A serialization
failure leaves the active token available for `fail` or cancellation; it cannot
manufacture a successful business result. The serde callback is unwind-contained;
allocator aborts, stack exhaustion and process termination are outside recovery,
as elsewhere in the workspace's failure policy.

Catalog cursors bind the complete catalog, owner secret, page position and expiry
with keyed BLAKE3. They require no cursor cache and reject tampering, another owner,
expiry and restart. These are distinct from Focal's fixed-prefix domain cursors,
which the backend passes through without collecting the entire ledger.

## Specification provenance and checks

The primary-source decision is recorded in
[architecture research](../../docs/archictecutre/14-mcp-protocol-research.md).
Official JSON schemas, authoritative TypeScript schemas and the upstream license
are vendored under [fixtures](fixtures/provenance.json) at commit
[`e76e9c572c6f2bfcb730357101acc90f2f802e02`](https://github.com/modelcontextprotocol/modelcontextprotocol/commit/e76e9c572c6f2bfcb730357101acc90f2f802e02),
verified on 2026-09-05. The manifest records source URLs, byte lengths, SHA-256 and
BLAKE3 digests; Rust tests check byte lengths and BLAKE3 without fetching anything.
The fixtures retain their upstream license, separate from Focal source licensing.

Protocol tests cover both lifecycle profiles, direct modern calls, errors, malformed
and bounded input, fragmented UTF-8/CRLF, cancellation and stale tokens, cursor
fences, output ownership, ordinary-memory pressure and serialization failure.
Emitted messages are checked against the official schema constructs they reach.
The test helper is not a general JSON Schema implementation or a claim that the
entire MCP protocol surface has been qualified. Backend, process and real-client
interoperability tests remain separate acceptance evidence.

Run `bash scripts/cargo.sh test -p focal-mcp --offline` from the repository root.
