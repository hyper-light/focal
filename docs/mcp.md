# MCP tools and repository skills

The [cluster administration guide](cluster-admin.md) lists the conditional local operator tools, their CLI equivalents, and the distinct root/application recovery guarantees.

`focal mcp serve` runs a real foreground stdio MCP adapter over the selected authenticated Focal connection. It exposes shared application operations, managed recovery, five payload-transfer tools and four durable-watch tools. Local network administration adds a separate operator catalogue when that backend is available; discover all pages instead of assuming a fixed tool count. Four [repository skills](../skills/manifest.json) cover claims, evidence, external validation and cluster operations. They are instruction files; Focal does not execute an agent interpreter or background workflow engine.

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

Without a selected profile, the adapter uses the established local service identity and ledger. Named client contexts select a Unix connection or authenticated QUIC client; `--client-context NAME` selects one invocation and `context use NAME` selects a saved default. Each connection retains its own request and upload history, and authenticated standing still comes from the server. See [connection setup](archictecutre/19-cli-mcp-implementation.md) for enrollment, context files and limitations. A physical joined node’s local metadata is not a substitute for an authenticated client context. The data directory must be owner-private (`0700`); the node creates new directories with that mode.

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

Use the discovered input/output schemas rather than generating fields from a tool's name. Application descriptors currently use version 1, encoded in their input schema `$id`. The [manifest](../skills/manifest.json) pins required names and versions and the instruction content digests. Saved transfer state is also available through `focal artifact upload inspect UPLOAD_ID --origin mcp`; `artifact upload cancel` with the same origin retries the same durable cancellation used by `upload.cancel`. The six recovery tools use recovery contract version 2. Packaged skills pin their own instruction version, application operations use version 1, and the five transfer tools use transfer contract version 1.

Nested results retain the frozen model encoding (byte-array IDs/hashes and numeric vocabularies). Convert identifiers to hexadecimal for authored fields. The implemented `focal schema get domain-registry` command prints the vocabulary mapping; its schema lookup is a local CLI operation, not an advertised MCP tool.

| Purpose | Actual tools |
| --- | --- |
| Claim operations | `claim.submit`, `claim.submit_batch`, `claim.post`, `claim.progress`, `claim.cancel`, `claim.supersede` |
| Receipt and evidence | `receipt.acquire`, `evidence.begin`, `artifact.register`, `artifact.submit`, `testament.submit`, `testament.receive` |
| Peer validation | `validation.begin`, `validation.begin_increment`, `validation.submit`, `validation.complete` |
| Payload transfer | `upload.begin`, `upload.append`, `upload.seal`, `upload.cancel`, `artifact.download` |
| Exact object/result reads | `claim.get`, `testament.get`, `artifact.get`, `validation.get` |
| Coherent validation inspection | `validation.context` |
| Recorded validator contracts | `validator.list`, `validator.get` |
| Bounded claim observation | `claim.wait` |
| Durable dependency predicates | `monitor.register`, `monitor.get` |
| Selected-ledger counters | `ledger.summary` |
| Bounded queries | `claim.list`, `testament.list`, `artifact.list`, `validation.list`, `ledger.traverse` |
| Durable observation delivery | `watch.open`, `watch.next`, `watch.acknowledge`, `watch.inspect` |
| Managed reservation and consumption | `request.reserve`, `request.pending`, `request.acknowledge`, `request.seal` |
| Saved operation recovery | `request.inspect`, `request.retry` |
| Legacy receipt/epoch observations | `request.status`, `request.epoch` |

Every list accepts optional family-appropriate filters, including an unfiltered bounded page. Claims support claim/source/target/status/action, scopes, typed relations and cause; testaments support claim/outcome/confidence; artifacts support claim/testament/producer/kind/schema hash and inputs; validations support claim/evaluator/kind/phase/mode. All four accept creation-prefix bounds. Filters combine with AND. Unsupported filters fail. `validation.get` reads actual run and verdict records; it does not schedule execution. Defaults are 64 returned objects and at most 1,024 visited records per list page. Preserve `page.next.bytes` exactly as hexadecimal `cursor`, even after an empty page. See [the shared workflow contract](../skills/references/workflow-contract.md#preserve-read-scope) for validation result continuation and wire-value conversion.

`claim.wait` accepts `claim`, `until` (`satisfied`, `terminal` or `released`) and an optional `timeout_ms` from 1 to 30,000. It observes fresh committed state for that bounded interval and returns `Met`, `Pending`, or `Unmet`. `Pending` is a client observation deadline; `Unmet` means the claim became terminal without satisfying the requested satisfaction predicate. This read reserves no mutation ID and creates no timer or monitor. For participant-owned durable dependency predicates, use the separate [monitor contract](monitors.md); monitor release alone does not identify success or timeout.

`validation.context` accepts the same `id`, optional `prefix`/`after`, and `limit` as `validation.get`. It returns `result.kind = "validation_context"` and `result.context` with the pinned requirement, owning claim, optional current closing testament, run/verdict page and exact token. Component reads share that token; a missing required parent or mismatched specification fails the whole result. An expired prefix is returned as `snapshot_expired`, never silently replaced with a newer snapshot. It is read-only, takes no `operation_id` and reserves no managed ordinal. Context describes recorded facts; it is not an assignment or permission to run a validator. Historical runs retain their original targets even when the current testament differs. Artifact payloads are retrieved separately. See the [shared continuation instructions](../skills/references/workflow-contract.md#preserve-read-scope).

`ledger.summary` accepts `{}` and returns `condition = "Observed"`, `result.kind = "summary"`, and six scalar committed counts plus the observed `token` and `applied_index`. A fresh quorum read precedes the counts; it does not download the graph. Counts cover retained claims, testaments, artifacts, validations, evidence sets and validation runs in this ledger only. They do not aggregate lifecycle statuses or cluster totals. There is no filter, cursor or saved-prefix input, and this read retains no historical snapshot lease. It takes no `operation_id` and reserves no managed ordinal. The CLI equivalent is `focal ledger summary --format json` (also table and YAML).

## Reserve, submit and consume a managed operation

Call `request.reserve` with empty arguments before a mutation. The adapter automatically establishes ownership of a managed request stream; no epoch or slot configuration is needed. Retain the returned `structuredContent.operation_id`, an `m1:slot:generation:ordinal:request_id` identifier, before sending business work. It is independent of the JSON-RPC request ID. Each separate mutation requires a separate reservation.

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"request.reserve","arguments":{}}}
```

Reservation executes no business command and is **not idempotent**. If its reply is lost, call `request.pending` with empty arguments to discover outstanding IDs. It lists reservations, pending requests and retained results without submitting or acknowledging them. While stream initialization is pending it reports `Initializing`; a subsequent reservation call resumes that saved initialization.

After compatibility initialization and reservation, this call generates one local handoff claim. Replace `RESERVED_OPERATION_ID` with the exact returned ID; the placeholder itself is not accepted:

```json
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"claim.submit","arguments":{"operation_id":"RESERVED_OPERATION_ID","target":"self","action":"handoff","description":"Deliver the checked report","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the report testament","evaluator":"self"}]}}}
```

The result's `structuredContent` contains the versioned application result and exact managed receipt. Retain the generated claim ID and original outcome. Generation and posting are separate: reserve another ID for `claim.post`. This receipt-only example establishes a delivery requirement; substantive acceptance needs real pinned quality validators.

Once the result has been consumed and its needed identifiers retained, explicitly acknowledge it:

```json
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"request.acknowledge","arguments":{"operation_id":"RESERVED_OPERATION_ID"}}}
```

Acknowledgment permits retirement only over a contiguous prefix of consumed outcomes. An earlier unresolved or unconsumed ID protects itself and later results. `Consumed` means the mark is saved but an earlier gap still prevents retirement; `Retired` means the exact ID cannot execute again and its complete historical result is no longer promised. Printing or inspecting an MCP result, cancellation and process exit never implicitly acknowledge it. A cleanup failure preserves the saved control for exact recovery.

## Recover uncertainty and preserve old IDs

If a business reply is lost or a wait ends, reconnect and inspect or retry the original ID:

```json
{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"request.inspect","arguments":{"operation_id":"RESERVED_OPERATION_ID"}}}
{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"request.retry","arguments":{"operation_id":"RESERVED_OPERATION_ID"}}}
```

`request.inspect` reports saved reserved, pending, committed or retired state. `request.retry` resumes the exact saved request. A reservation without authored intent cannot be retried as business work; submit its intended mutation or explicitly seal it. Unknown or retired IDs never create new work. Repeating a mutation with the same ID and normalized intent resumes it. Changed authored input, expected revision, operation name/version or authenticated context conflicts before object ID generation. Busy, capacity and domain failures are explicit; preserve the ID and its saved intent.

To resolve uncertain admission deliberately, call `request.seal` with the original managed ID. It returns the earlier committed outcome if one exists, otherwise commits a fence preventing that exact request from executing. It can also close an unused reservation without inventing a business command. Read and retain the returned outcome before acknowledging it. A refused, informed or timed-out request is not itself a durable seal. Business cancellation uses the separate `claim.cancel` operation.

For a fresh owner observation, pass `remote: true` to `request.inspect`. Managed IDs query their exact stream generation and ordinal; any retained receipt is checked against the saved intent and local result. The read leaves local recovery unchanged. `Retired` and `StreamClosed` fence execution without supplying the original historical outcome. `Unknown` allows an earlier proposal still to commit. Use exact retry or deliberate sealing to resolve uncertainty; absence never erases a saved result.

Previously supplied nonzero 32-character lowercase hexadecimal operation IDs keep their permanent legacy journal bindings. These IDs still work with mutations, inspect and retry; they are not managed reservations and cannot be acknowledged or sealed through these tools. Legacy remote inspection queries the business key even if local epoch admission is pending. Without a journal, `request.status` takes `{ "epoch": 1, "request_id": "WIRE_REQUEST_ID" }`, and `request.epoch` takes `{ "epoch": 1 }`. These two tools address legacy epoch keys, not `m1` IDs. The wire request ID differs from the operation ID and JSON-RPC ID.

Legacy observations distinguish retained domain/cursor commits, `BelowFloor` and `Unknown`. A retained result wins below the epoch floor. `BelowFloor` prevents new admission at the observed prefix but does not establish that the request never committed. These are successful reads; quorum loss is an operational error. They neither advance local recovery nor permit replacement intent. A cursor-family result for a saved domain request is rejected. All remote observations bind the authenticated principal and selected ledger and report a fresh quorum prefix, including domain sequence and applied Raft index.

For deliberate business cancellation use `claim.cancel`. This MCP notification only cancels client interest in JSON-RPC request 4:

```json
{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":4,"reason":"Client is leaving this wait"}}
```

An admitted mutation can still commit; cancellation preserves its recovery obligations. Closing stdin has the same requirement to retain operation IDs. The adapter has no durable parked continuation that resumes an agent automatically.

## Evidence workflow

Follow [focal-evidence](../skills/focal-evidence/SKILL.md): acquire the claim's current receipt, open an evidence set, attach actual artifacts, then close a testament with its exact ordered artifact ID/descriptor-hash manifest. Every step is a separate durable operation. Copy the receipt ID and epoch together and reuse the same fence. The descriptor hash is distinct from a content-manifest root or a digest of raw payload bytes.

The service currently verifies the built-in test-report schema. Obtain its descriptor and exact hash through the implemented CLI:

```sh
target/debug/focal --data-dir /tmp/focal-mcp-example schema get test-report
```

Use that hash in the discovered `artifact.submit` schema with `kind: "test-report"` and a text payload such as `{"passed":1,"failed":0,"skipped":0}`. Inline text/bytes and metadata are each limited to 16 KiB. The installed test-report validator accepts a content-backed JSON payload up to 1 MiB. Content transfers are independently bounded at 64 MiB; successful storage does not grant schema admission beyond the actual validator’s limit. Schema registration is not an MCP operation. `artifact.get` returns the descriptor/reference; `artifact.download` returns bounded payload pages. The [manual CLI](manual-cli.md#deliver-artifacts-and-a-testament) automatically stages larger `--payload-file` inputs and uses the same upload journal before attachment.

A committed testament close means `TestamentGenerated`. The issuer separately uses `testament.receive`, then `validation.begin` for whole-work runs or `validation.begin_increment` for a saved increment requirement and actual attached target. `validation.context`/`validation.get` expose the recorded run fences. The designated evaluator executes its tool, skill or code externally, registers actual proof with `artifact.register`, and supplies exact artifact ID/hash pairs to `validation.submit`. Finally the issuer may call `validation.complete`; Core checks stored results and graph constraints. These are real protocol-3 operations with owner authorization, not caller assertions of success. Each mutation has its own durable operation ID.

Independent artifact and testament lifecycle history, owner-checked child claims and automated corrective/follow-up helpers remain incomplete, as detailed in the [peer validation contract](archictecutre/16-peer-validation-contract.md). The implemented peer surface exposes the existing single-response reducer. Focal neither launches agents nor executes supplied tools. Participants author ordinary corrective/follow-up claims and retain their actual committed outcomes.

## Transfer a payload and retrieve every byte

Generate and retain a nonzero lowercase 32-hex `upload_id` before the first call. It is independent of a managed operation ID. Calculate the BLAKE3 digest of the complete raw byte stream and its length. Call:

```json
{"name":"upload.begin","arguments":{"upload_id":"RETAINED_UPLOAD_ID","length":300000,"digest":"RAW_STREAM_BLAKE3_HEX","class":"evidence"}}
```

The placeholders must be replaced with real values. Append contiguous byte chunks, each at most 65,536 bytes, using `upload.append` with the same `upload_id`, byte `offset`, and `bytes_hex` (two hex characters per byte). Bytes are durably staged before network transmission. Exact old chunks can be retried; changed bytes, length, digest, class or authenticated context conflict. Replies report `staged` and `received` separately. Repeat the exact `upload.begin` metadata after response loss to inspect saved progress; it never allocates another transfer ID.

After the complete stream is staged, call `upload.seal` with `upload_id`. It resumes pending requests and obtains the server’s actual custody-gated `reference`. Convert its domain/root byte arrays to hexadecimal and its content class to the discovered authored spelling when supplying a `payload` of type `content` to `artifact.submit` or `artifact.register`. Reserve a separate business operation ID for that domain mutation. A sealed reference alone is not an attached or registered artifact. The raw byte digest is distinct from the content-manifest root and artifact descriptor hash.

A lost reply, MCP cancellation or process exit preserves the exact transfer. On the current supporting server, `upload.cancel` publishes a checksummed terminal record before removing staging; delayed Begin/Append/Seal cannot revive that scoped upload ID, including after restart. Immutable content and artifact facts remain. `cancel_acknowledged` is a legacy-compatible RPC acknowledgment, not an authenticated cross-version capability proof. The server bounds live plus terminal upload IDs at 65,536, reserves terminal completion space for admitted uploads, and retains terminal metadata with backups. Time-based reclamation and physical deletion are unsupported; future reclamation needs a generation-fenced upload namespace and capability. The adapter retains local bytes/identity after seal or cancel. Removing a store is not a retry procedure.

For retrieval, call `artifact.download` with artifact `id`, optional `max_bytes` (1–65,536), and offset zero. Save its token and descriptor `content_hash`. Subsequent calls supply the same token and the next byte offset; nonzero offsets without a token are rejected. Append the returned `chunk.bytes` only after checking the expected offset; stop at `chunk.eof`. An expired prefix fails explicitly. The server verifies stored manifest/chunks; it does not return a raw digest equal to the manifest root. The complete client helper also bounds the whole retrieval deadline; CLI publication writes a private same-directory temporary file, fsyncs, and publishes without overwriting an existing path.

Both protocol profiles are covered by a real 300 KiB progressive upload, MCP restart, exact/conflicting retry, artifact attachment and complete paged download. The CLI regression stages offline, deletes the original source, restarts the service and completes the exact pending attachment from owned bytes. These tests qualify the installed test-report schema bound, not arbitrary external schemas.

## Operator-only administration

A local network operator may additionally discover `cluster.*` tools. They include root and application configuration reads, learner admission/promotion/removal and joint completion, leadership-transfer initiation, invitation/credential inspection and revocation, node and client invitations, passive diagnostics, and exact administrative request inspection/retry/reconciliation. Use their discovered schemas, [administration guide](cluster-admin.md) and [cluster skill](../skills/focal-cluster/SKILL.md). This catalogue is conditional and is required only by the cluster skill. Actor or physical Node identity alone grants no administrative authority.

Administration has its own `a1:…` recovery references. A contact is not voter membership, promotion requires actual catch-up, credential revocation does not remove membership or drain placement, and successful leader transfer reports initiation. Invitation output files are private operator-side artifacts; transfer tools accept bytes and never read arbitrary server-side paths.

## Persistence, capacity and packaging

The human CLI automatically owns `DATA_DIR/CLI.requests`, allocates IDs for ordinary mutations, and marks delivery only after successful output and flush. MCP owns a separate `DATA_DIR/MCP.requests` stream and requires explicit result acknowledgment. An unconsumed MCP window therefore does not exhaust the CLI's request window. Both use the same synchronous coordinator and persisted allocator; neither adds a service thread or holds its managed catalogue lock across a network wait.

For MCP, preserve `MCP.requests`, `MCP.requests.managed-owner` and `MCP.requests.managed-lock` together with the node identity. The external initialized marker prevents a lost child store from silently reusing stream authority. Recovery resumes saved registration, initialization and completion controls; missing or corrupt initialized state fails closed. Deleting these files is not a recovery procedure. Requests, normalized intent, generated IDs, exact receipts and delivery marks use checksums, exclusive locks, atomic publication and file/directory fsync before they authorize the next step.

A default managed stream retains at most 32 outstanding outcomes/reservations, within a 256 MiB conservative disk budget. The client library accepts configured windows up to 256 when its disk allowance and server policy permit; this is not a CLI or MCP window flag. Intent is limited to 256 KiB, and serialized requests and receipts to one MiB each. Only committed contiguous acknowledgment permits body reclamation. The bounded allocator scans slots 0–63 in order and selects the first observed vacancy: typical first use needs one slot read and one registration, then the business call. Sparse external slot allocation, exhausted server rows or generation exhaustion returns a typed failure. There is no automatic close, stream rotation or generic discovery across arbitrary principals and slots.

Legacy 32-character IDs continue under `DATA_DIR/MCP.operations`. External `MCP.operations.lock` and `MCP.operations.initialized` retain their existing initialization and loss protections. Its default bounds remain 256 permanent IDs and 512 MiB of conservative reservations; completed IDs stay counted and are not migrated or evicted automatically. Epoch one remains shared without advancing another operation's deduplication floor.

Upload history lives beside each selected context’s operation stores in `CLI.uploads` or `MCP.uploads`, with external `.lock` and `.initialized` files. Preserve the directory and both files. Checksummed metadata binds scope, declared digest/class/length, exact pending request, staged prefix and any returned reference. Defaults permit 64 retained transfer IDs within 256 MiB of conservative reservations, including complete declared payloads and metadata. Completed/cancelled transfers remain counted; automatic reclamation is not implemented. Catalogue locks cover registration, while a returned upload journal exclusively owns that transfer across network waits. Unrelated uploads use independent locks; there is no new upload worker or queue.

For CLI recovery, `focal request inspect --operation-id M1_ID` and `focal request retry --operation-id M1_ID` find the existing local managed store without creating another namespace. A legacy MCP journal remains `DATA_DIR/MCP.operations/OPERATION_ID/journal`:

```sh
target/debug/focal --data-dir /tmp/focal-mcp-example request inspect \
  /tmp/focal-mcp-example/MCP.operations/b731bdba83db4de7bf4b690ac18e4001/journal --format json
```

The analogous `request retry PATH` resumes it. Path-based journals keep their existing format; MCP recovery tools accept IDs rather than arbitrary paths. The foreground adapter admits one active tool call and bounds transport input, output, queues and shutdown, reporting pressure rather than an unbounded backlog.

Keep all four skill directories, `skills/references` and `skills/manifest.json` together when packaging; their relative links are intentional. Register each `SKILL.md` with the agent host and the executable command with its MCP client. Use [focal-validation](../skills/focal-validation/SKILL.md) for pinned external execution and verdict submission. This repository does not install skills into an external agent automatically, publish `skill://` resources, or claim a generated skill runtime. Contract tests pin live descriptors, recovery and administration contracts, and file digests. Protocol fixtures and the repository's Rust stdio harness provide qualification; an external SDK/client interoperability run is not claimed here.


## Consume a durable watch

When a watch store is configured, discover `watch.open`, `watch.next`, `watch.acknowledge` and `watch.inspect`. Choose a stable name before opening:

```json
{"name":"watch.open","arguments":{"name":"evidence","family":"artifact","claims":["REAL_CLAIM_ID"]}}
```

Replace the placeholder with the actual claim ID; omit `claims` for the entire ledger. `watch.open` is an exact-options retry and returns one retained delivery. `watch.next` repeats that delivery until consumed. After the sink has durably accepted the complete page, call `watch.acknowledge` with the name and the returned `delivery.id` encoded as 64 hexadecimal digits. Its `Consumed` result confirms the durable local consumed frontier. The next `watch.next` commits source cursor acknowledgment and retires the exact managed receipt before advancing. Repeating the last acknowledgment is harmless. `watch.inspect` with a name reads saved status/data; omitting the name lists at most 16 saved names. Inspection is not consumption.

MCP cancellation or response loss preserves the pending action/page. A partial seed resumes at the exact saved token and last visited object key; filtered empty pages progress. Consume every seed page before completing the seed. A source lease lost during a seed produces an explicit expiry instead of silently changing the prefix. Choose a new watch name to reseed deliberately. A saved delivery can still be consumed after source restart. Each page is bounded at 64 KiB and at most 256 source items/visits; an oversized individual object is a typed capacity limit. Seed objects and original tail deltas are distinct facts, with resolved/resync markers retained; watch ACK is neither testament receipt nor validation.

The [CLI watch instructions](manual-cli.md#watch-delivered-ledger-changes) describe shared storage, bounds and recovery. These four adapter tools use their own private per-watch allocator and do not require business operation IDs. No background agent or subscription daemon is created. Tool output schemas are per-tool subsets of the stable `ApplicationResult` envelope, with IDs `urn:focal:mcp:TOOL:output:1`; this narrows discovery schemas without changing `schema_version` or runtime result encoding.

Singular `claim.get` accepts an exact ID or optional claim/source/target/status/action, scope, typed relation/cause and creation-prefix filters. A filtered read returns one match only after the same-prefix scan proves uniqueness; zero matches, ambiguity, expiry and an exhausted visit bound are distinct typed outcomes. Continue broad surveys with `claim.list`; never pick the first match as a unique result. `max_visits` is bounded at1–1024.

The four `*.list` tools accept optional `created_after` (exclusive) and
`created_through` (inclusive) committed creation `SessionSeq` bounds. Additional
family predicates are:

| Tool | Optional predicates |
| --- | --- |
| `claim.list` | `scopes: [{kind,key}]`, `relations: [{kind,target}]`, `caused_by` |
| `artifact.list` | `inputs: [{kind,id}]` |
| `testament.list` | `outcome`, `confidence` |
| `validation.list` | Creation bounds alongside its existing requirement filters |

For list relations, use an explicit target such as `participant:self`,
`claim:ID`, `root:ID`, or `action:work`; object targets can also name a testament,
artifact, or validation. `caused_by` accepts `claim:ID` or `root:ID`. Each supplied
scope/relation/input must match, and each collection is limited to 16 (the cause
alias also consumes one relation). Every field is optional; family-inapplicable
fields are rejected. Discover the exact enum spellings in each tool schema.

The returned list shape and bounded pagination are unchanged. Preserve all
predicates and page limits with the opaque cursor, including after an empty
page with `next`. Later writes cannot enter that snapshot; expiry or a server
restart requires an explicit new query. Creation bounds do not claim to filter
lifecycle activity. Generic `changed_since` and validation-result predicates
remain unsupported; use `validation.get`/`validation.context` for recorded runs.
