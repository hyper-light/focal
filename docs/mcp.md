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

Use the discovered input/output schemas rather than generating fields from a tool's name. Application descriptors use version 1 on a V1 ledger and version 2 on a native ledger (see [native engine tools](#native-engine-tools)), encoded in their input schema `$id`. The [manifest](../skills/manifest.json) pins required names and versions and the instruction content digests. Saved transfer state is also available through `focal artifact upload inspect UPLOAD_ID --origin mcp`; `artifact upload cancel` with the same origin retries the same durable cancellation used by `upload.cancel`. The six recovery tools use recovery contract version 2. Packaged skills pin their own instruction version, application operations use version 1, and the five transfer tools use transfer contract version 1.

Nested results retain the frozen model encoding (byte-array IDs/hashes and numeric vocabularies). Convert identifiers to hexadecimal for authored fields. The implemented `focal schema get domain-registry` command prints the vocabulary mapping; its schema lookup is a local CLI operation, not an advertised MCP tool.

The respondent must call `testament.submit` after work completes or fails;
`receipt.acquire` creates no testament. Non-Complete reports require a durable
`kind: "error"` artifact in the exact manifest. Obtain the pinned diagnostic
descriptor, hash and example with `focal schema get error-report`, then submit
that payload through the ordinary `artifact.submit` tool. A tool failure can use:

```json
{"code":"tool_unavailable","message":"The required tool could not run","details":"No test result was produced"}
```

The same payload contract works in every participant language and framework. A real failed-test
report can instead use the existing test-report schema with its actual counts.

The claimant receives the testament and its designated evaluator inspects the
exact error bytes, runs its own check, and submits separately registered proof
with the resulting verdict. Successfully recording a failure is a successful
mutation, not proof of claim satisfaction. The native lifecycle target keeps
diagnostics separate from work slots; the current wire profile still uses the
single exact closing manifest. Built-in payload-schema lookup is currently CLI
discovery; this MCP adapter does not advertise a schema resource or lookup tool.

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

The service verifies the pinned test-report and error-report schemas. Obtain their
descriptors and exact hashes through the CLI:

```sh
focal schema get test-report
focal schema get error-report
```

Use the test-report hash in the discovered `artifact.submit` schema with `kind: "test-report"` and a text payload such as `{"passed":1,"failed":0,"skipped":0}`. Inline text/bytes and metadata are each limited to 16 KiB. The installed test-report validator accepts a content-backed JSON payload up to 1 MiB. Content transfers are independently bounded at 64 MiB; successful storage does not grant schema admission beyond the actual validator’s limit. Schema registration is not an MCP operation. `artifact.get` returns the descriptor/reference; `artifact.download` returns bounded payload pages. The [manual CLI](manual-cli.md#deliver-artifacts-and-a-testament) automatically stages larger `--payload-file` inputs and uses the same upload journal before attachment.

For unavailable tools, refusals or interruptions, submit a truthful error-report
with `kind: "error"`; no test counts are required. Its JSON object has nonblank
`code` (at most 128 UTF-8 bytes), nonblank `message` (4096 bytes), and optional
`details` (a string up to 32768 bytes or `null`). The complete payload is at most
64 KiB. Unknown/duplicate fields and positional arrays are rejected. The
[packaged reporting contract](../skills/references/workflow-contract.md#built-in-error-report-v1)
pins the exact hash and example for participants without CLI access. MCP exposes
no payload-schema discovery resource. An older server's refusal requires retaining
the real diagnostic and operation ID, never substituting invented test counts.

The current respondent authors the testament after work completes or fails;
acquiring its execution receipt creates no testimony. Every non-`complete`
outcome requires a durable error artifact in the exact response manifest. The
requester then receives that account, and its designated evaluator checks the
actual work and diagnostic evidence before supplying a separate verdict.

On the native engine a non-`complete` `testament.submit` must cite at least one of the respondent's own committed `artifact.diagnostic` results in `diagnostics` and is refused with `invalid_input` without one; the requester reads the diagnostic bytes with `artifact.get`. A check whose slot the frozen manifest lacks is refused by `validation.begin` and `validation.report` (`not_found`), and `validation.enter_whole_work` assesses it as `ValidationIncomplete` without any manufactured verdict. An evaluator that cannot run its handler reports `verdict: "error"`; the error report stays as evidence and, while the handler's declared `attempts` remain, the evaluation stays open on the next attempt (`attempt_index` counts from zero) for a further `validation.report`.

The adapter runs beside the human CLI on the same data directory: both read the context catalogue and enrolled credentials under shared locks and journal on their own native stores, so neither excludes the other. Cancelling a tool call (`notifications/cancelled`) drops only the adapter's wait: the operation keeps its durable reference, `request.pending` lists it, and `request.retry` returns the committed result or resends the exact frame, never a second business mutation. A capacity refusal from the node admitted nothing; the adapter resends with backoff and then reports `capacity`, and the reference stays `Pending` until a later `request.retry` succeeds (see the CLI guide for `FOCAL_DISK_HEADROOM_BYTES`).

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

## Native engine tools

On a ledger activated on the native engine (`focal cluster replicas
activate-native`, [cluster-admin.md](cluster-admin.md)) the adapter serves a
different catalogue. At startup it probes the engine once with a standing read
under the native wire profile, on the same runtime every later call uses; a V1
answer keeps the catalogue above, a native answer replaces the application,
managed, transfer and watch tools with the native descriptors and four
recovery tools. Discover the catalogue instead of assuming either; a native
ledger lists `ledger.standing` and never lists `request.reserve` or
`ledger.traverse`.

| Purpose | Native tools |
| --- | --- |
| Claim lifecycle | `claim.submit`, `claim.post`, `claim.cancel` (version 2 documents) |
| Peer workflows | `claim.challenge`, `claim.consult`, `claim.correct`, `claim.follow_up` (authored shapes of `claim.submit`: same frame, identity and receipt), `claim.lineage` (one composed page: the claim, its cause ancestors, its corrections, refinements and children), `claim.wait` (`testament`, `satisfied`, `terminal` or `released`; result kind `native_wait`) |
| Respondent cycle | `receipt.acquire`, `artifact.submit`, `artifact.diagnostic`, `artifact.fail`, `testament.submit`, `testament.post` |
| Issuer and evaluator | `testament.receive`, `artifact.receive`, `artifact.reject`, `validation.begin`, `validation.report` (admission, increment or whole-work evaluations by `phase`), `validation.seal_increments`, `validation.enter_whole_work`, `receipt.adopt`, `claim.release_scope`, `audit.generate`, `audit.post` |
| Durable waits | `monitor.register`, `monitor.rebind`, `monitor.cancel` |
| Exact fixed-prefix reads | `claim.get`, `testament.get`, `artifact.get`, `validation.get`, `validation.context` (the evaluator's composed view: claim, definition, selected registration and evaluation, manifest with custody, results after a revision cursor, delivery result), `ledger.standing` |
| Bounded lists | `claim.list`, `artifact.list`, `validation.list`, `evaluation.list`, `testament.list`, `receipt.list`, `monitor.list`, `event.list` |
| Journal recovery | `request.inspect`, `request.retry`, `request.pending`, `request.acknowledge` |

A native `claim.submit` may name `parent`, the committed claim the new claim
is caused by: the adapter reads the parent's current binding and receipt and
pins them in the frame, and the owner admits the child only from the parent's
issuer or its current receipt holder while the parent is live, registering the
child on the parent; a forged, foreign, stale or terminal parent and any other
actor are refused with typed outcomes. Cancelling the parent cancels its
pending children.

A native `claim.submit` may also cite exact evidence and declare a follow-up
policy: a `reviews` or `derived_from` relation may target `artifact:ID@HASH`
(the artifact at its committed descriptor hash; an unknown artifact, another
hash, a pending artifact or any other relation kind is refused), and
`policy` (`corrective_allowed`, `max_follow_ups`, `single_issuer`,
`escalation`) is authored immutably with the claim and returned by
`claim.get`. Either selects descriptor schema 2; other claims keep schema 1.
A correction is a `claim.submit` with `action: correction`, one
`invalidates` relation naming the challenge and one `reviews` relation
naming the report artifact of its failed verdict at its hash; a follow-up
consultation is a `claim.submit` with `action: consultation` and a `refines`
relation naming the consultation it continues. The owner admits both only
under the followed claim's policy (who may file, whether corrections are
allowed, one issuer, how many follow-ups) and, for a correction, only on
the terminal Fail, Incomplete or Error verdict at the current generation;
refusals come back as `native_refused` results whose
`refusal.kind.Refused` names the owner's code: `WrongActor`, `InvalidPolicy`,
`InvalidTarget`, `MissingEvidence`, `InvalidTransition`, `StaleEvaluation`
or `ConflictingCause`. The typed tools package these shapes: `claim.challenge`
(`artifact` as `ID` or `ID@HASH`, a mandatory `policy`), `claim.consult`,
`claim.correct` (`challenge`, `verdict` as `ID` or `ID@HASH`, `target`
defaulting to the challenge's subject, an occurrence identity derived from
the challenge, the verdict and the author so a repeated delivery resolves to
one correction) and `claim.follow_up` (`refines`, `target` defaulting to the
refined consultation's subject, an identity derived from the refined claim
and the query). A projection-only ledger withholds them with `claim.submit`.
`claim.lineage` reads one claim's lineage as a `native_read` page and
`claim.wait` observes a claim as `native_wait`; the `focal-peers` skill
sequences them.

Native tools take no reservation. Every mutation compiles its document with
the same compiler the CLI uses, reads the objects it binds to once at a fixed
prefix, journals the exact `FCNINPUT` frame under an `n1:` reference in the
adapter's own journal (`DATA_DIR/client/mcp-native`) and only then sends it.
`operation_id` is optional on a native mutation: omit it and the returned
`operation_id` is the minted reference; supply the same `n1:` reference with
the same input to resume an exact retry, and a reference already bound to
different input is refused with `operation_conflict` before any identity is
minted. Results use `schema_version` 2: a committed frame returns
`condition = "Committed"` with `result.kind = "native"` (the owner's receipt
and the identities the frame minted), a closed refusal returns `isError`
with `result.kind = "native_refused"` and its category (invalid input,
unauthorized, not found, stale, conflict, capacity or the owner's code), and a
frame the owner still holds as a pending candidate returns `OutcomeUnknown`.
Reads return `result.kind = "native_read"` pages whose nested documents keep
the frozen encodings (byte-array identities, numeric statuses). Lists return
`condition = "Listed"` with `result.kind = "native_list"`: `page.objects`,
`page.visited` (rows examined, matching or not) and `page.next`, an opaque
cursor to pass back as the hexadecimal `cursor` argument with the same
filter. One indexed predicate is scanned (a relation or scope, then a
participant, then the action or status, then `created_after` for claims; an
artifact's `input`, then `producer`, `schema`, `kind`; a `verdict` for
evaluations) and the rest filter within `max_visits` (default 1,024), so a
page may be empty and still carry `next`; only an absent `next` ends the
list. Participants accept `self`. A cursor reused under another filter, by
another principal, tampered with, or issued before a node restart is
refused.

```json
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"claim.submit","arguments":{"target":"ALICE_PRINCIPAL_HEX","description":"Run the suite and deliver the report.","validations":[{"kind":"receipt","description":"Record delivery.","deadline":{"at":4102444800000}},{"kind":"test","description":"The suite passes.","target":{"type":"slot","index":0,"name":"report"},"evaluator":"self","handlers":[{"id":"HANDLER_HEX","version":"VERSION_HEX"}],"deadline":{"at":4102444800000}}],"slots":[{"slot":0,"checks":[{"declaration":1}]}]}}}
{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"request.acknowledge","arguments":{"operation_id":"RETURNED_N1_REFERENCE"}}}
```

A committed result stays listed by `request.pending` until it is explicitly
acknowledged, exactly as managed results do, so a reply lost between the
adapter and the agent is found again after either restarts. `request.inspect`
returns the journaled receipt, the recorded refusal, or `Pending`;
`request.retry` resends the exact frame until the owner commits it and returns
the bound receipt without a send once it has. `request.inspect` with
`remote: true` reads the owner's committed outcome for the request key
instead of the journal; the human CLI's `focal request inspect --operation-id
n1:… --remote` performs the same read, so either adapter can observe an
operation the other journaled under the same context. Refusals are recorded
as reported and leave the pending list; the frame stays inspectable. There
is no `request.seal` on a native ledger: the owner resolves the request key
before admission, so an exact retry can never execute twice.

The respondent's evidence is carried inline in the frame (`payload` as
`{"type":"text"}` or `{"type":"inline"}`, at most 256 KiB), verified by the
node's content custody before the owner records it; the chunked upload tools
and `artifact.download` are not offered until the native content transfer
arrives. Every owner operation a participant can author is a tool: the
evaluation tools select the admission or increment evaluation with `phase`
(`whole_work` by default) and `target` (the increment's work artifact);
`artifact.fail` cites the holder's committed production diagnostic by id
(optionally pinning its hash); `artifact.reject` records a `structure` or
`metadata` failure with the issuer's diagnostic; `receipt.adopt` names the
new holder and fences the committed receipt; `audit.generate` and
`audit.post` produce the closed claim's result testament (read it with
`testament.get`); `monitor.register` takes `roots` of
`{predicate: satisfied|terminal|released, claim}` and a logical-time
`deadline`, `monitor.rebind` a committed superseding successor, and
`monitor.cancel` works once the owning claim is terminal. Discover each
document with `schema.get` (version 2). Every `deadline` is logical
milliseconds since the Unix epoch; the node's clock fires claim, evaluation
and monitor deadlines once a second without a tool call, and the reads show
the outcome. `claim.wait`, watches and `ledger.summary` wait for the native
deltas; the CLI manual's
[native engine verbs](manual-cli.md#native-engine-verbs) section shows the
same cycle, the remaining verbs and lists through flags.

## Operator-only administration

Every tool the adapter lists comes from one registry in the client crate: the application descriptors of the active engine, the four recovery tools, and the shared watch, transfer and administration descriptors (`focal_client::operations::{watch_descriptors, transfer_descriptors, admin_descriptors}`), each naming its capability, surface and the CLI path that performs the same operation. `tools/list` is one pass over that registry filtered by what the adapter can prove (engine, uploads, watches, local node ownership); a tool the adapter did not list is refused when called directly, both at the protocol layer and by the dispatcher.

A local network operator may additionally discover `cluster.*` tools. They include root and application configuration reads, learner admission/promotion/removal and joint completion, leadership-transfer initiation, invitation/credential inspection and revocation, node and client invitations, passive diagnostics, and exact administrative request inspection/retry/reconciliation. Use their discovered schemas, [administration guide](cluster-admin.md) and [cluster skill](../skills/focal-cluster/SKILL.md). This catalogue is conditional and is required only by the cluster skill. Actor or physical Node identity alone grants no administrative authority.

Administration has its own `a1:…` recovery references. A contact is not voter membership, promotion requires actual catch-up, credential revocation does not remove membership or drain placement, and successful leader transfer reports initiation. Invitation output files are private operator-side artifacts; transfer tools accept bytes and never read arbitrary server-side paths.

## Persistence, capacity and packaging

The human CLI automatically owns `DATA_DIR/CLI.requests`, allocates IDs for ordinary mutations, and marks delivery only after successful output and flush. MCP owns a separate `DATA_DIR/MCP.requests` stream and requires explicit result acknowledgment. An unconsumed MCP window therefore does not exhaust the CLI's request window. Both use the same synchronous coordinator and persisted allocator; neither adds a service thread or holds its managed catalogue lock across a network wait.

For MCP, preserve `MCP.requests`, `MCP.requests.managed-owner` and `MCP.requests.managed-lock` together with the node identity. The external initialized marker prevents a lost child store from silently reusing stream authority. Recovery resumes saved registration, initialization and completion controls; missing or corrupt initialized state fails closed. Deleting these files is not a recovery procedure. Requests, normalized intent, generated IDs, exact receipts and delivery marks use checksums, exclusive locks, atomic publication and file/directory fsync before they authorize the next step.

A default managed stream retains at most 32 outstanding outcomes/reservations, within a 256 MiB conservative disk budget. The client library accepts configured windows up to 256 when its disk allowance and server policy permit; this is not a CLI or MCP window flag. Intent is limited to 256 KiB, and serialized requests and receipts to one MiB each. Only committed contiguous acknowledgment permits body reclamation. The bounded allocator scans slots 0–63 in order and selects the first observed vacancy: typical first use needs one slot read and one registration, then the business call. Sparse external slot allocation, exhausted server rows or generation exhaustion returns a typed failure. A generation issues at most 65,536 IDs; when every one is acknowledged the adapter closes it, removes its store and registers the next generation on the same slot automatically (`FOCAL_MANAGED_ROTATION=N` lowers the bound; it is saved with the coordinator on first use). IDs of a closed generation report `Retired`. There is no generic discovery across arbitrary principals and slots.

Legacy 32-character IDs continue under `DATA_DIR/MCP.operations`. External `MCP.operations.lock` and `MCP.operations.initialized` retain their existing initialization and loss protections. Its default bounds remain 256 permanent IDs and 512 MiB of conservative reservations; completed IDs stay counted and are not migrated or evicted automatically. Epoch one remains shared without advancing another operation's deduplication floor.

Upload history lives beside each selected context’s operation stores in `CLI.uploads` or `MCP.uploads`, with external `.lock` and `.initialized` files. Preserve the directory and both files. Checksummed metadata binds scope, declared digest/class/length, exact pending request, staged prefix and any returned reference. Defaults permit 64 retained transfer IDs within 256 MiB of conservative reservations, including complete declared payloads and metadata. Completed/cancelled transfers remain counted; automatic reclamation is not implemented. Catalogue locks cover registration, while a returned upload journal exclusively owns that transfer across network waits. Unrelated uploads use independent locks; there is no new upload worker or queue.

For CLI recovery, `focal request inspect --operation-id M1_ID` and `focal request retry --operation-id M1_ID` find the existing local managed store without creating another namespace. A legacy MCP journal remains `DATA_DIR/MCP.operations/OPERATION_ID/journal`:

```sh
target/debug/focal --data-dir /tmp/focal-mcp-example request inspect \
  /tmp/focal-mcp-example/MCP.operations/b731bdba83db4de7bf4b690ac18e4001/journal --format json
```

The analogous `request retry PATH` resumes it. Path-based journals keep their existing format; MCP recovery tools accept IDs rather than arbitrary paths. The foreground adapter admits one active tool call and bounds transport input, output, queues and shutdown, reporting pressure rather than an unbounded backlog.

Keep all four skill directories, `skills/references` and `skills/manifest.json` together when packaging; their relative links are intentional. Register each `SKILL.md` with the agent host and the executable command with its MCP client. Use [focal-validation](../skills/focal-validation/SKILL.md) for pinned external execution and verdict submission. Each skill carries an "On a native ledger" branch and the shared contract a [native engine](../skills/references/workflow-contract.md#native-engine) section; the manifest (schema 3) pins the version-1 operations, the version-2 native operations (`required_native_operations`, together covering every native descriptor), the recovery, transfer, watch and administration contracts, and every file digest (`cargo test -p focal-client --test skill_contract -- --ignored --nocapture print_skill_digests` prints the digests to re-pin after an edit). This repository does not install skills into an external agent automatically, publish `skill://` resources, or claim a generated skill runtime. Contract tests pin live descriptors, recovery and administration contracts, and file digests. Protocol fixtures and the repository's Rust stdio harness provide qualification; an external SDK/client interoperability run is not claimed here.


## Consume a durable watch

When a watch store is configured, discover `watch.open`, `watch.next`, `watch.acknowledge` and `watch.inspect`. Choose a stable name before opening:

```json
{"name":"watch.open","arguments":{"name":"evidence","family":"artifact","claims":["REAL_CLAIM_ID"]}}
```

Replace the placeholder with the actual claim ID; omit `claims` for the entire ledger. `watch.open` is an exact-options retry and returns one retained delivery. `watch.next` repeats that delivery until consumed. After the sink has durably accepted the complete page, call `watch.acknowledge` with the name and the returned `delivery.id` encoded as 64 hexadecimal digits. Its `Consumed` result confirms the durable local consumed frontier. The next `watch.next` commits source cursor acknowledgment and retires the exact managed receipt before advancing. Repeating the last acknowledgment is harmless. `watch.inspect` with a name reads saved status/data; omitting the name lists at most 16 saved names. Inspection is not consumption.

MCP cancellation or response loss preserves the pending action/page. A partial seed resumes at the exact saved token and last visited object key; filtered empty pages progress. Consume every seed page before completing the seed. A source lease lost during a seed produces an explicit expiry instead of silently changing the prefix. Choose a new watch name to reseed deliberately. A saved delivery can still be consumed after source restart. Each page is bounded at 64 KiB and at most 256 source items/visits; an oversized individual object is a typed capacity limit. Seed objects and original tail deltas are distinct facts, with resolved/resync markers retained; watch ACK is neither testament receipt nor validation.

On a native ledger the four tools are offered next to the version-2 catalogue and behave the same: `watch.open` records the native engine in the watch's options, the seed is read through native reads (a `claims` filter reads each claim with its responses and evaluations; otherwise the family is listed) and delivered as `NativeSeed` pages, and tail deltas carry `schema` 2 with a `Native` fact holding the exact committed event record, the nearest legacy `action`, the `actor` and the `claim`. Objects read for the seed may also arrive as deltas when they were committed between the pinned snapshot and the read; deduplicate by object binding.

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
artifact, or validation, and on a native ledger `artifact:ID` or
`artifact:ID@HASH` lists the reviews and derivations of that exact evidence.
`caused_by` accepts `claim:ID` or `root:ID`. Each supplied
scope/relation/input must match, and each collection is limited to 16 (the cause
alias also consumes one relation). Every field is optional; family-inapplicable
fields are rejected. Discover the exact enum spellings in each tool schema.

The returned list shape and bounded pagination are unchanged. Preserve all
predicates and page limits with the opaque cursor, including after an empty
page with `next`. Later writes cannot enter that snapshot; expiry or a server
restart requires an explicit new query. Creation bounds do not claim to filter
lifecycle activity. Generic `changed_since` and validation-result predicates
remain unsupported; use `validation.get`/`validation.context` for recorded runs.
