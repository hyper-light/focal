# MCP tools and repository skills

`focal mcp serve` runs a real foreground stdio MCP adapter over the authenticated local Focal service. It exposes 18 shared application operations and two durable recovery tools. The two [repository skills](../skills/manifest.json) guide those calls; they are instruction files, not an agent interpreter or a background workflow engine.

## Start the service and adapter

Build the binary from this checkout:

```sh
bash scripts/cargo.sh build -p focal-node --bin focal --locked --offline
```

Start a service in one terminal, using a new private data directory:

```sh
target/debug/focal --data-dir /tmp/focal-mcp-example start
```

Configure a stdio MCP client to execute the following command, with the same absolute data directory. Keep its stdin open while awaiting tool results:

```sh
target/debug/focal --data-dir /tmp/focal-mcp-example mcp serve
```

Use an absolute binary path in a client whose working directory differs from this checkout. The adapter's stdout contains newline-delimited JSON-RPC only; diagnostics use stderr. One process serves one foreground connection. EOF cancels outstanding waits and begins bounded shutdown. The service remains a separate process and owns the ledger.

The current adapter selects the laptop/founder's local identity and ledger. It rejects joined-node markers before creating client operation state; a joined node has a different authenticated Unix principal. Remote client contexts and arbitrary ledger selection are not implemented in this adapter. The data directory must be owner-private (`0700`); the node creates new directories with that mode.

## Discover the actual schemas

The adapter implements request-scoped `2026-07-28` metadata and a compatibility profile for `2025-11-25`. The latter can be exercised with this non-mutating shell example after node initialization:

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"focal-example","version":"1"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  | target/debug/focal --data-dir /tmp/focal-mcp-example mcp serve
```

`tools/list` is paginated: send its `nextCursor` in a subsequent `tools/list` call until absent. These catalogue cursors belong to this connection; domain list cursors are different. The modern profile uses `params._meta` on each request, for example:

```json
{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{},"io.modelcontextprotocol/clientInfo":{"name":"focal-example","version":"1"}}}}
```

Use the discovered input/output schemas rather than generating fields from a tool's name. Application descriptors currently use version 1, encoded in their input schema `$id`. The [manifest](../skills/manifest.json) pins required names and versions and the instruction content digests. The recovery tools use the adapter's version-1 operation-ID recovery contract.

Nested results retain the frozen model encoding (byte-array IDs/hashes and numeric vocabularies). Convert identifiers to hexadecimal for authored fields. The implemented `focal schema get domain-registry` command prints the vocabulary mapping; its schema lookup is a local CLI operation, not an advertised MCP tool.

| Purpose | Actual tools |
| --- | --- |
| Claim operations | `claim.submit`, `claim.post`, `claim.progress`, `claim.cancel` |
| Receipt and evidence | `receipt.acquire`, `evidence.begin`, `artifact.submit`, `testament.submit` |
| Exact object/result reads | `claim.get`, `testament.get`, `artifact.get`, `validation.get` |
| Bounded queries | `claim.list`, `testament.list`, `artifact.list`, `validation.list` |
| Saved operations | `request.inspect`, `request.retry` |
| Owner receipt/epoch observations | `request.status`, `request.epoch` |

Every list accepts optional family-appropriate filters, including an unfiltered bounded page. Claims support claim/source/target/status/action; testaments support claim; artifacts support claim/testament/producer/kind/schema hash; validations support claim/evaluator/kind/phase/mode. Filters combine with AND. Unsupported filters fail. `validation.get` reads actual run and verdict records; it does not schedule execution. Defaults are 64 returned objects and at most 1,024 visited records per list page. Preserve `page.next.bytes` exactly as hexadecimal `cursor`, even after an empty page. See [the shared workflow contract](../skills/references/workflow-contract.md#preserve-read-scope) for validation result continuation and wire-value conversion.

## Submit and recover an operation

Each mutation requires a caller-chosen, nonzero 32-character lowercase hexadecimal `operation_id`. Preserve it before sending. It is independent of the JSON-RPC request ID, which can change on reconnection. Separate mutations require separate operation IDs.

After compatibility initialization, this call generates one local handoff claim. The example operation ID identifies precisely this intent; use a different ID for different work:

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"claim.submit","arguments":{"operation_id":"b731bdba83db4de7bf4b690ac18e4001","target":"self","action":"handoff","description":"Deliver the checked report","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the report testament","evaluator":"self"}]}}}
```

The result's `structuredContent` contains the versioned application result. A verified committed receipt identifies the generated claim. Generation and posting are separate: use the returned claim ID with `claim.post` and a new operation ID. The receipt-only example establishes a delivery requirement; substantive acceptance needs real pinned quality validators.

If a reply is lost or the client wait ends, reconnect and inspect or retry the original operation:

```json
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"request.inspect","arguments":{"operation_id":"b731bdba83db4de7bf4b690ac18e4001"}}}
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"request.retry","arguments":{"operation_id":"b731bdba83db4de7bf4b690ac18e4001"}}}
```

`request.inspect` reports saved pending/completed state; `request.retry` resumes exact saved requests. Unknown IDs do not create work. Repeating `claim.submit` with the same ID and normalized intent also resumes it. Changed authored input, expected revision, operation name/version or authenticated context produces a conflict/error before ID generation. Busy, capacity and domain failures are explicit; preserve the recovery ID.

For a fresh owner observation, pass `remote: true` to `request.inspect`. This verifies any retained business receipt against the saved command hash and existing receipt, leaving the journal unchanged. Without a journal, `request.status` takes `{ "epoch": 1, "request_id": "WIRE_REQUEST_ID" }`; `request.epoch` takes `{ "epoch": 1 }`. Use the wire request ID from the journal/receipt, which differs from the MCP operation ID and JSON-RPC ID. These tools accept no principal selector: authentication and the selected ledger bind the scope. Each successful remote query obtains a fresh quorum barrier and reports both the domain sequence and applied Raft index.

The result distinguishes a retained domain or cursor commit, `BelowFloor` and `Unknown`. The retained result wins below the epoch floor. `BelowFloor` prevents new admission at the observed prefix but does not establish that the request never committed. `Unknown` leaves an earlier proposal able to commit. These are successful read results (`isError: false`); quorum loss is an operational error. They do not clear saved receipts, advance local recovery, permit replacement intent or authorize history retirement. Use `request.retry` with the original operation ID to persist exact recovery progress. A cursor-family result for a saved domain business request is rejected as a key-family conflict.

For deliberate business cancellation use `claim.cancel`. This MCP notification only cancels client interest in JSON-RPC request 3:

```json
{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":3,"reason":"Client is leaving this wait"}}
```

An admitted mutation can still commit; cancellation does not erase its journal or imply claim cancellation. Reinspect before deciding what happened. Closing stdin has the same requirement to preserve operation IDs. The adapter has no durable parked continuation that will resume an agent automatically.

## Evidence workflow

Follow [focal-evidence](../skills/focal-evidence/SKILL.md): acquire the claim's current receipt, open an evidence set, attach actual artifacts, then close a testament with its exact ordered artifact ID/descriptor-hash manifest. Every step is a separate durable operation. Copy the receipt ID and epoch together and reuse the same fence. The descriptor hash is distinct from a content-manifest root or a digest of raw payload bytes.

The service currently verifies the built-in test-report schema. Obtain its descriptor and exact hash through the implemented CLI:

```sh
target/debug/focal --data-dir /tmp/focal-mcp-example schema get test-report
```

Use that hash in the discovered `artifact.submit` schema with `kind: "test-report"` and a text payload such as `{"passed":1,"failed":0,"skipped":0}`. Inline text/bytes and metadata are each limited to 16 KiB. Larger artifacts require an already durable content reference; upload/download and schema registration are not advertised MCP tools. `artifact.get` returns the descriptor/reference, not streamed large content. The existing [manual CLI](manual-cli.md#deliver-artifacts-and-a-testament) documents the other available host paths.

A committed testament close means `TestamentGenerated`. It does not run acknowledgment, validation or whole-work completion. Inspect the actual lifecycle and validator records. Foreground autonomous worker/evaluator integration, parented peer routing, automatic challenge correction/consult follow-up, and durable agent continuations remain incomplete. The skills describe how to preserve their obligations and report the actual gap; they do not return simulated success.

## Persistence, capacity and packaging

The adapter stores operations under `DATA_DIR/MCP.operations`. Private external `MCP.operations.lock` and `MCP.operations.initialized` files prevent a lost store from silently resetting operation IDs. Startup is automatic only on first use. Existing markers choose open-only recovery; incomplete initialization or missing store/marker/lock returns an error. Preserve these files together with node identity and operation journals. Deleting them is not a recovery procedure.

The store saves normalized intent and complete expanded envelopes before transmission, with checksums, exclusive locks, atomic record publication and file/directory fsync. An operation claimed before complete preparation remains incomplete instead of regenerating IDs. Completed preparation can finish journal creation after a crash. The catalogue lock is released before network waits; each active operation retains its own journal lock. Epoch one is shared without advancing another operation's deduplication floor.

Intent is limited to 256 KiB; serialized requests and receipts are each limited to one MiB. The default store caps are 256 IDs and 512 MiB of conservative reservations for full journal generations, whichever fills first. Incomplete and completed IDs remain counted; no automatic eviction or quota migration exists. The foreground transport admits one active tool call and bounds its input, output, queues and shutdown. It reports pressure instead of accepting an unbounded backlog.

For CLI recovery of an MCP operation, the journal path is `DATA_DIR/MCP.operations/OPERATION_ID/journal` (the library also exposes `OperationStore::operation_path`). Use the same node context:

```sh
target/debug/focal --data-dir /tmp/focal-mcp-example request inspect \
  /tmp/focal-mcp-example/MCP.operations/b731bdba83db4de7bf4b690ac18e4001/journal --format json
```

The analogous `request retry PATH` resumes it. Path-based CLI journals retain their existing format; the MCP tools accept operation IDs rather than arbitrary paths.

Keep both skill directories, `skills/references` and `skills/manifest.json` together when packaging; their relative links are intentional. Register each `SKILL.md` with the agent host and the executable command with its MCP client. This repository does not install either into an external agent automatically, publish `skill://` resources, or claim a complete generated skill runtime. Contract tests pin the required live descriptors, actual recovery tools and file digests. Protocol fixtures and the repository's Rust stdio harness provide qualification; an external SDK/client interoperability run is not claimed here.
