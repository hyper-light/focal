# MCP protocol selection and local adapter

Research date: **2026-09-05**. The protocol decision below is now implemented by the bounded local [Rust MCP adapter](../../crates/focal-mcp/README.md), over the shared operation registry and durable operation store. [The executable guide](../mcp.md) describes its released surface. The broader qualification and workflow gates remain open in [13](13-cli-and-agent-implementation-plan.md); the tests described in the implementation record do not imply full MCP ecosystem interoperability.

## 1. Pin the current protocol, with explicit compatibility

Use **MCP `2026-07-28`** as the primary protocol. The official versioning page identifies it as current, and the versioned schema agrees. A current revision can receive compatible changes without changing its date, so implementation must also record the exact upstream schema commit and digest in its fixtures. This research verifies the protocol date; it does not pin or qualify an SDK release. [Official revision status](https://modelcontextprotocol.io/docs/2026-07-28/learn/versioning), [authoritative schema](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/schema/2026-07-28/schema.ts)

Support exactly **`2025-11-25`** as a legacy compatibility profile in the same stdio process. This is a Focal interoperability choice: the official matrix says legacy clients cannot fall forward to a modern-only server. Select behavior from the request opening, without another operator setting. Modern requests carry their own version and capabilities; they do not inherit a previous request's negotiation. A legacy `initialize` selects the legacy lifecycle. Do not accept older revisions implicitly. [Compatibility rules](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning)

| Profile | Required behavior |
| --- | --- |
| `2026-07-28` | No initialization handshake. Validate `params._meta` protocol version and client capabilities on each request. Implement `server/discover`; a client may call a tool without discovering first. Unsupported versions return `-32022` with `data.supported` and `data.requested`. |
| `2025-11-25` | Handle `initialize`, return the supported legacy version, capabilities and server identity, then accept `notifications/initialized`. If the requested legacy version is unsupported, counteroffer `2025-11-25`; never claim that an initialization handshake established modern semantics. Include legacy `ping` handling. |

The modern behavior above follows [version negotiation](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning); the compatibility behavior follows the [legacy lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle) and [ping response contract](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/ping). Keep separate typed response encoders and lifecycle tests; do not sprinkle version-dependent optional fields through domain code.

Modern discovery returns `supportedVersions: ["2026-07-28", "2025-11-25"]`, `capabilities: {"tools": {}}`, and server identity in `_meta["io.modelcontextprotocol/serverInfo"]`. Include `resultType: "complete"`. Discovery does not confer authentication. [Discovery contract](https://modelcontextprotocol.io/specification/2026-07-28/server/discover)

Modern discovery and tool-list responses require caching hints. Initially use `ttlMs: 0` and `cacheScope: "private"` on every page because visibility depends on the caller's grants. These are freshness hints, never permission to bypass authorization. [Caching requirements](https://modelcontextprotocol.io/specification/2026-07-28/server/utilities/caching)

## 2. Choose a bounded owned adapter

Implement a small `focal-mcp` adapter over `focal-client`, with a future `focal mcp serve` entry point. Reuse workspace Serde, JSON, errors, memory accounting and existing authenticated transport. Keep JSON-RPC correlation, protocol profiles and tool dispatch here; keep request identity, builders, receipts and retries in the shared client. No HTTP transport, task extension, sampling, elicitation, resources, prompts or skill extension is advertised in this first profile. Those require separate implementation and tests; the current specification treats tasks and skills as extensions. [Protocol scope](https://modelcontextprotocol.io/specification/2026-07-28)

This decision follows inspection of the **official Rust SDK**, not an assumption that an SDK necessarily needs HTTP. Its inspected `main` workspace identifies itself as `rmcp 3.2.0`; server/stdio features can be selected without HTTP. However, the server feature also brings schema/macro-related dependencies. [Workspace manifest](https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/Cargo.toml), [crate features and dependencies](https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/crates/rmcp/Cargo.toml)

The inspected default `AsyncRwTransport` reads a complete line with `read_until` into a growing vector before decoding, and shares its writer through `Arc<Mutex<...>>`. Its codec has a length option, but that does not bound this reader's initial accumulation. The service layer also uses shared service/peer ownership and poisoned-lock `expect` paths. [Transport source](https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/crates/rmcp/src/transport/async_rw.rs), [service source](https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/main/crates/rmcp/src/service.rs)

**Inference and tradeoff:** a custom transport plus an audited SDK integration could work, but would still require ownership, allocation and panic-boundary adaptation. The initial owned adapter gives Focal direct control of these boundaries with fewer dependencies. The cost is maintaining two small protocol profiles and their conformance tests. This is not a performance comparison or a claim about every SDK configuration. Revisit the choice if supported protocol surface grows; do not silently replace the bounded implementation with SDK defaults.

Use one owned coordinator, an owned writer and bounded in-flight records. Journal filesystem work must run outside the async network executor, through one bounded owner seam, not a thread/task per tool invocation. Channels transfer owned requests and their allocations. No new shared `Arc` state is necessary for protocol maps or writers; existing cross-thread budget internals retain their documented purpose. Input/cancellation processing must remain responsive while a journal or remote commit is pending.

## 3. Transport and failure contract

Stdio carries one UTF-8 JSON-RPC message per line, without embedded raw newlines. Stdout contains protocol messages only; diagnostics go to stderr. Modern servers send responses and permitted notifications, not reverse JSON-RPC requests. EOF initiates prompt shutdown. [Stdio binding](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio)

Focal implementation requirements:

- Reserve frame capacity **before** reading; stop at the byte limit without allocating the rest of an unterminated line. Close the transport on an oversized frame rather than draining an attacker-controlled endless line. Handle fragmented UTF-8, CRLF, trailing partial frames and malformed JSON explicitly.
- Validate nesting, value counts, strings, IDs and duplicate JSON keys before building unrestricted JSON trees. Separate envelope limits from the existing authored-input limits. Reject batches and malformed request/notification shapes; unknown notifications receive no reply.
- Preserve string and integer IDs exactly; distinguish `1` from `"1"`. Modern IDs are non-null and unique while in flight. An active duplicate must never replace its original completion/cancellation entry. Invalid or unavailable IDs use the protocol error response rules. [JSON-RPC requirements](https://modelcontextprotocol.io/specification/2026-07-28/basic)
- Bound pending count, input/output bytes, metadata, catalogs, queued writes, disk journals and maximum wait time. Reserve response and completion capacity before admitting ordinary work. Hold every output allocation until the corresponding bytes are written or discarded, including both structured data and its text representation.
- Use checked size arithmetic, fallible growth and fallible writes. Treat broken stdout/stderr as I/O errors, never `println!`/`eprintln!` panics. Partial stdout writes are fatal to the transport; never append a second JSON error to repair half a frame. Redact credentials, invitation tokens and authored payloads in diagnostics.

| Failure | Response |
| --- | --- |
| Invalid JSON / invalid JSON-RPC shape | `-32700` / `-32600`, with the appropriate null or parsed correlation ID. |
| Unknown RPC method / malformed method parameters | `-32601` / `-32602`. |
| Unsupported modern version | `-32022`, supported/requested version data. |
| Missing required modern client capability | `-32021`, required-capability data; the initial tools need no optional client capability. |
| Unknown tool name | Protocol invalid-parameters error. |
| Known tool's argument, authorization, capacity or business failure | Typed tool result with `isError: true`; explain whether admission is known, unknown or committed. |
| Completed domain validation with a failing verdict | Successful read of a failing verdict, not a fabricated protocol error. |

Standard and MCP-reserved error ranges have defined meanings; do not invent `-32001` for Focal busy responses. [Error-code contract](https://modelcontextprotocol.io/specification/2026-07-28/basic) Tool execution errors belong in tool results, while unknown tools and malformed calls are protocol errors. [Tool errors](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)

Handle `notifications/cancelled` by correlating the active request. Stop preventable work and suppress further messages for that canceled request as required by the modern stdio binding. Unknown/completed IDs and races must be harmless. A committed or ambiguously admitted Focal mutation cannot be undone by canceling its wait: retain the bounded journal owner through the uncertain write, then recover the same operation. Explicit claim cancellation is a different durable command. [Cancellation semantics](https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/cancellation), [stdio cancellation requirement](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio)

## 4. Shared registry, schemas and durable identity

P17.1's [shared registry](../../crates/focal-client/src/operations/README.md) now inventories the model/wire surface and releases the implemented authored operations. Each implemented operation needs a stable tool name/version, strict argument DTO, input/output schema, required capability, mutation classification, size limits and recovery contract. Generate schemas/reference fragments from that source; do not introduce independently maintained MCP versions of the CLI builders. The existing [client builders and journals](../../crates/focal-client/README.md) supply the reusable foundation.

Tool input schemas describe objects; publish JSON Schema 2020-12 and bounded local definitions. Do not fetch remote schema references. Tool annotations are hints, not authority. Modern results require `resultType`; Focal will return object-shaped `structuredContent` in both profiles even though the modern schema also permits other JSON values. [Schema definitions](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/schema/2026-07-28/schema.ts), [schema processing rules](https://modelcontextprotocol.io/specification/2026-07-28/basic)

Publish `outputSchema` for a common versioned result envelope containing operation reference, outcome, typed receipt or error, object references, evidence, and read prefix/cursor where applicable. Provide the same JSON as text content for compatibility, accounting for both representations. Validate every emitted result against its released schema. [Structured tool results](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)

Implemented local mutation sequence:

1. Require a caller-selected durable `operation_id` before first dispatch, distinct from the JSON-RPC ID. Use a validated opaque ID under a private configured journal root, never an arbitrary model-supplied filesystem path. This closes the lost-first-response problem where the caller otherwise cannot know the server-generated recovery handle.
2. Bind the operation to cluster, principal, ledger, operation/schema version and supplied intent. Expand generated object IDs once; persist the complete envelopes before transmission. For an existing ID, open and validate the saved intent before any new ID allocation. Different intent conflicts; it never overwrites the journal.
3. Reuse `OperationJournal::next_request()` and `record_reply()`. Add a shared open-or-validate seam rather than making MCP's own retry store. `request.inspect` and `request.retry` resume the saved expanded operation; they do not rebuild an authored document.
4. On timeout, report an explicit unknown outcome and that same operation ID when a response remains possible. Cancellation may suppress the response, but the caller already knows the ID. A later successful call returns the original durable receipt.

These are Focal design requirements, not MCP guarantees. Current manual journals use epoch one and never advance the floor. MCP must not introduce automatic epoch advancement or garbage collection that could retire another process's unknown request; P17.11 remains required before reclaiming that history. Journal quotas must refuse new work safely rather than delete uncertain operations. [Current journal contract](../../crates/focal-client/README.md), [retry and epoch requirements](13-cli-and-agent-implementation-plan.md)

Bind the launched adapter to actual local credentials and selected ledger. Reauthorize at authenticated ingress on every operation; MCP `clientInfo`, capability declarations and tool visibility cannot mint Actor, Evaluator or Runtime standing. A Node enrollment certificate is not tenant Runtime authority. Root/session cause and custody attestations remain owner-produced. Artifact bytes and descriptions are untrusted data, not instructions to execute. [Focal trust contract](12-agent-tools-and-workflows.md)

## 5. Two different pagination contracts

MCP `tools/list` uses an opaque cursor and server-selected page size; an empty string is a valid cursor, while absence/null signals completion. Invalid cursors return invalid parameters. [MCP pagination](https://modelcontextprotocol.io/specification/2026-07-28/server/utilities/pagination)

For Focal's catalog, bind a bounded authenticated cursor to registry digest, grant projection, order and expiry. Keep deterministic tool ordering and reject stale cursors after an authorization/catalog change. No server-side unbounded cursor map is needed. The catalog exposes only implemented operations currently allowed for discovery; every call still checks authority. [Tool discovery rules](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)

Domain list/read tools separately pass Focal's existing fixed-prefix query cursor in their typed arguments/results. Do not translate it into an MCP catalog offset, collect an entire ledger, or mix prefixes. Preserve zero-match pages with continuation, invalid/expired cursor outcomes, immutable testament-manifest membership, and paged validation run/attempt records. Artifact reads return bounded metadata/references; large bytes use the existing verified transfer path, not an unbounded base64 tool result. [Delivered read and transfer contract](../manual-cli.md)

## 6. Implementation and acceptance sequence

The boxes below retain their **complete acceptance scope**. The local adapter has protocol, durable-store, cancellation/restart and real executable workflow tests; external client distributions, replicated adapter journeys and the broader capability/transfer surface remain unqualified. Consult [implementation status](09-implementation-status.md) for the exact executed evidence:

- [ ] Pin official modern/legacy schema fixtures by commit and digest; add typed envelope/profile tests. Cover direct modern tool calls, discovery, missing metadata, version mismatch, changed per-request capabilities, legacy initialization/counteroffer/initialized/ping, and profile isolation. Advertise only the released surface.
- [ ] Implement bounded stdio framing and owned output. Test byte-by-byte reads, huge unterminated input, depth/node/key limits, bad UTF-8, batches, invalid IDs, active duplicates, malformed notifications, broken pipes, partial writes, EOF and deadline shutdown. Verify stdout contains only complete protocol lines and stderr is redacted.
- [ ] Build registry parity checks against CLI/client DTOs and release schemas. Test known-tool argument errors separately from malformed calls, output-schema conformance, unauthorized discovery/calls, and inability to forge trusted context. Run schema checks without remote reference resolution.
- [ ] Implement private operation-ID mapping and open-or-validate journal reuse. Test changed intent, concurrent same-ID lock contention, canceled waits before/after admission, lost first reply, restart, disk pressure and failed local fsync. Confirm no new generated IDs or request keys on retry.
- [ ] Exercise real one-voter and three-voter owners. A mutation tool must not return committed success before quorum commitment. Replay one saved operation through CLI, embedded client and MCP and compare identity, receipt, lifecycle and evidence manifest.
- [ ] Exercise catalog pagination/tampering/grant changes and domain zero-match, expired-prefix, manifest and validation-result pages. Measure retained heap as well as encoded response size, including duplicate text/structured content. Completion/cancellation remains possible under ordinary pressure.
- [ ] Run a real compatible MCP client subprocess against each supported profile, recording the exact client version. Complete the existing receipt/evidence/testament workflow and recover it after forced process loss. Protocol unit fixtures alone do not establish interoperability.
- [ ] Add thin skills with exact tool and CLI alternatives, examples validated against the shared registry, preconditions, bounded wait/recovery and explicit unsupported actions. The existing [manual CLI guide](../manual-cli.md) is the fallback; `focal mcp serve` now supplies the local process transport. Do not claim `skill://` resources or a Skills extension merely because Markdown skills exist. Challenge/consult automation still depends on P20 and trusted child-claim authorization.

The smallest deployable increment is this local adapter over the existing authenticated client. Remote MCP, broader extensions and autonomous continuation policies are later, separately qualified increments; none is required to configure a laptop tool process.

## 7. Local implementation boundaries

The upstream modern and legacy JSON and TypeScript schemas are pinned at commit `e76e9c572c6f2bfcb730357101acc90f2f802e02`; [fixture provenance](../../crates/focal-mcp/fixtures/provenance.json) records source URLs, lengths, SHA-256 and BLAKE3 digests. Offline tests check fixture identity and the schema constructs reached by emitted protocol messages. The schema test helper deliberately does not claim general JSON Schema conformance.

The foreground process owns one blocking input reader, one ledger worker with a current-thread network runtime, and one output writer. Bounded channels hold input and completed frames. The 128 MiB session allowance reserves 80 MiB for completion; a ledger operation requires a separate 32 MiB admission within that budget. Input frames are capped at 278,528 bytes, authored DTOs/intent at 256 KiB, and encoded responses at 16 MiB. One business call is active at a time; protocol control and cancellation remain readable during a network wait. Full output queues close the transport with an explicit error. These are admission limits, not measured total process RSS or disk preallocation.

Returned frames and retained calls own their allocations through output or discard. A cancelled call retains its protocol slot until its worker retires, suppressing stale completion before that JSON-RPC ID can be reused. EOF cancels waiting and permits three seconds for owner completion; an OS read/write/fsync still stalled at the deadline is retained by its thread until the foreground process exits. This API is not an embedded reusable server pool. No new explicit `Arc` wrapper is used.

The operation store has private per-operation locks and a short catalog lock. Its independent bootstrap marker binds cluster/principal/ledger and makes loss of initialized state fail closed. Intent includes expected revision; comparison precedes ID generation. The catalog claims quota and the ID before expansion, then saves complete exact envelopes before creating the ordinary CLI journal. An incomplete claim without complete saved envelopes is never regenerated. Completed and unknown records share conservative aggregate quotas; safe history retirement remains P17.11.

Input schemas and the common result envelope come from the shared registry. Nested frozen wire payload schemas currently describe their structural object boundary; the Rust wire validator validates their contents. Full generated nested schemas, external SDK interoperability, remote credential contexts, grants projected into discovery, verified large-content MCP transfer, and automated challenge/consult follow-ups remain open.
