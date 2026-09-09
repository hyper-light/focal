# Manual CLI and agent interfaces: implementation extension

Status: required implementation scope, 2026-09-06. This extends the active goal, **implement the plan fully**, through P20. It does not replace the storage, ownership, distribution, or deployment gates in P00–P16. A command in this document is a required interface unless explicitly marked implemented; it is not a claim that the binary already accepts it.

The user requires a Rust CLI for claims, testaments, artifacts, validations, and cluster operation; equivalent flag, JSON, and YAML inputs; optional list filters; skills and MCP access to those operations; and challenge/consult workflows with durable evidence and appropriate follow-up claims. The primary-source audits are [11](11-cli-spec-research.md) and [12](12-agent-tools-and-workflows.md). These are part of the implementation contract, together with the lifecycle rules in [02](02-domain-and-lifecycle.md).

Current delivered increment: shared strict input builders and operation inventory, durable managed request journals, indexed optional-filter pages, fixed-prefix validation results/context, peer lifecycle mutations, resumable content transfers, named Unix/QUIC/enrolled contexts, watches, traversal, summary and local cluster administration are implemented. Ordinary CLI mutations register and reserve internally; MCP exposes explicit reservation and consumed-result acknowledgment over a separate managed store. Schema discovery, examples and shell completion use the actual shared contracts. See [the CLI guide](../manual-cli.md), [the MCP guide and skills](../mcp.md), and [implementation evidence](09-implementation-status.md). P17/P18 remain open for independent lifecycle migration, trusted child causes, safe stream rotation, remaining deployment and administrative services; P19 remains open for complete workflow/capability/remote interoperability gates; P20 remains required. Existing unchecked tasks below retain their complete acceptance scope.

The [peer validation and four-family lifecycle contract](16-peer-validation-contract.md) specifies the required participant authority, evidence binding and persisted-model migration. It is implementation scope, not a claim that those new APIs or lifecycle fields already exist.

## 1. Shared operation contract

The CLI, MCP server, and skills use the same typed application operations and authenticated client. They must not implement separate reducers, write graph state, manufacture custody attestations, or assign lifecycle status. Focal is strictly peer-to-peer: participants own their agents, tools, skills and execution; Focal never launches agent processes, schedules model jobs or invokes providers. The issuer invokes validation externally, the respondent supplies testament/artifacts, and the issuer/designated peer submits the result. A request for another peer to evaluate is an ordinary claim/testament exchange. A human command is allowed to orchestrate several durable operations, but that orchestration must have explicit progress and recovery state.

Keep three representations distinct:

1. Human input: flags or a versioned JSON/YAML document using readable IDs and explicit authored fields.
2. Validated operation: typed IDs, bounded collections, resolved local context, stable mutation identity, and an explicit operation variant.
3. Wire request: the existing versioned envelope and command/read payload, with authority supplied by trusted ingress.

JSON and YAML are encodings of the same input schema. They are not unvalidated wire-envelope injection. The existing `focal request FILE` remains the explicit low-level envelope interface. Conflicting input modes, unknown fields, duplicate map keys, malformed IDs, trailing documents, excessive aliases/nesting, overlong input, and invalid enum values fail before network mutation. Do not change the frozen binary encoding of existing model IDs to make human JSON convenient.

Default context comes from the local node/client configuration. Laptop use needs no explicit tenant, session, route epoch, certificate path, or request epoch. Remote contexts add endpoint and authenticated identity, then a ledger selection only when there is more than one authorized ledger. A claim's `--target` is its subject participant, not a network endpoint. `--source` selects the issuer when filtering claims; it never impersonates the authenticated caller. If such a filter does not identify exactly one claim, singular `get claim` reports ambiguity and suggests `list claims`.

## 2. Command and result conventions

Required roots are `submit`, `get`, `list`, `watch`, `claim`, `receipt`, `evidence`, `validation`, `validator`, `artifact`, `audit` (the native result testament: `generate`, `post`), `ledger`, `context`, `cluster`, `deployment`, `request`, `schema`, `completion`, and `mcp`. Existing `start`, `join`, `demo`, `status`, and `identity` remain compatible. `ledger` groups traversal, summary and durable cursor inspection; `watch` and any `ledger watch` alias share one implementation. `validator` exposes pinned handler contracts separately from validation requirements/results. Group lifecycle commands by their object; do not invent a generic status setter.

For submit commands, support exactly one authored-input mode:

```text
focal submit claim --target <participant> --description <text> ...
focal submit claim --json '<authored claim document>'
focal submit claim --yaml '<authored claim document>'
focal submit claim --file claim.yaml
focal submit claim --file - --input-format json
focal submit testament --claim <claim> --receipt <receipt> ...
focal submit testament --json '<testament document>'
focal submit testament --yaml '<testament document>'
```

The examples specify command grammar, not permission to omit the claim's actual validation contract or a testament's evidence/receipt fence. `--json '{}'` and `--yaml '{}'` exercise the correct parser and return required-field errors; neither creates an empty valid claim. Detailed field matrices and source mappings live in [11](11-cli-spec-research.md).

All list filters are optional. An unfiltered list means the selected authorized ledger, with a bounded page; it never means an unbounded global scan. Combine supplied filters with AND. Repeated values of one filter form an explicitly documented set. Unsupported combinations produce a typed error, not silent filter omission. Required examples include:

```text
focal list claims
focal list claims --source <participant> --target <participant> --status <status>
focal list testaments
focal list testaments --claim <claim>
focal list artifacts
focal list artifacts --testament <testament>
focal list validations
focal list validations --claim <claim>
focal get claim <claim>
focal get claim --source <participant> --target <participant>
focal get testament <testament>
focal get artifact <artifact>
focal get validation <validation>
```

`get artifact` returns its descriptor and payload location/availability. Explicit `--output FILE` or a byte-output mode retrieves and verifies the artifact bytes. Binary output must not share stdout with status text. `list artifacts --testament` resolves the immutable attachment manifest at the same read prefix; it does not collect artifacts merely because they mention the same claim. Validation output separates the immutable requirement, execution runs, attempts, final verdict, and evidence. A configured validator is an executable handler/version; it is not a validation result.

Human output should be compact and stable enough to inspect, with JSON as the automation contract. Add JSON Lines for streaming and YAML output only through a bounded supported serializer. Keep diagnostics on stderr. Machine output includes condition, IDs, committed receipt or exact unknown request identity, read prefix, pagination state, and schema version. It must distinguish durable acceptance, dispatch, testament acknowledgment, validation completion, and graph satisfaction.

Pagination tokens bind ledger, principal/authorization scope, filters, sort order, exact prefix, last visited index key, and expiry. The continuation advances past visited nonmatching rows, not merely the last returned object. Emit a continuation even when a bounded filtered scan returns no matches but has not finished. Never label a truncated page as a complete list. `--all` is explicit incremental streaming with bounded memory and cancellation; it must not accumulate all rows. A list over multiple ledgers is a separate authorized fan-out operation with per-ledger prefixes and partial-result semantics.

Mutations persist the fully expanded operation and request key before transmission. Generated IDs, selected context, receipt epoch, expected revision, and artifact digest remain fixed after an unknown outcome. Normal CLI use prints a copyable `focal request retry --operation-id m1:…` command. `request pending` discovers unresolved IDs; `request inspect --operation-id` observes without resubmission or acknowledgment. Explicit legacy `--operation PATH` and positional-path `request retry/inspect` remain supported; an ID flag is never interpreted as a filesystem path. Do not use command text as an idempotency key, generate a new key on reconnect, infer non-admission from a timeout, or mark a request complete because an MCP/CLI process exited. Committed cancellation of business work is a different operation from canceling a client wait.

## 3. P17 — Typed application operations and bounded queries

Dependencies: existing P01/P02 model and reducer, P03 graph, P05 evidence, P07 authenticated protocol/client. This work can proceed alongside the remaining placement work. It must preserve old command ordinals and persisted bytes.

Proposed files: `crates/focal-client/src/operations/`, `input/`, `pending/`, `query/`; narrow append-only changes in `crates/focal-wire/src/message.rs`; query indexes/projections in `crates/focal-graph/` and node read owners. Final module names follow the existing crate boundaries; there is no requirement for a new runtime or database.

- [x] P17.1 Inventory every existing model command, read family, stream action, content transfer, and administrative operation. Record actor/Runtime/Node standing, required parentage and receipt, mutation result, retry semantics, and transport exposure. Turn this inventory into an exhaustive shared registry checked against command/query variants.
- [x] P17.2 Define versioned human DTOs for authored claims, validation requirements, artifact descriptors and testament closure. IDs accept one documented string form; canonical typed conversion preserves model semantics. Reject lifecycle, authority-context, custody-verdict, and forged actor fields. Bound raw input before decoding and validate aggregate decoded size before cloning/encoding.
- [ ] P17.3 Compile claims into `GenerateClaim` or an explicitly named generate-and-post workflow. Resolve issuer from authentication, subject from target, root/claim cause from actual context, and validation specification hashes from the immutable definitions. Preserve explicit relation direction and distinguish required dependencies from descriptive links. Never imply that merely generating a claim dispatches it.
- [ ] P17.4 Compile receipt acquisition, evidence-set begin, artifact upload/attachment, testament closure, progress, acknowledgment, revocation/cancellation, and supersession into their existing legal commands. Give every multi-step workflow a durable local journal and receipt-first recovery. Pin upload identity, immutable source bytes/digest, durable offset and evidence scope across retries; a file edited during upload is not the same operation. Retain independently valid artifacts after a later step fails; report their IDs instead of deleting acknowledged content.
- [ ] P17.5 Add bounded typed filters for each object family: exact IDs, source/issuer, target/subject, claim, testament attachment, lifecycle status where meaningful, action, scope, relation, validation phase/kind/evaluator/result, artifact kind/producer/schema, and creation/change prefix. Mark unsupported predicates explicitly. Index common predicates; bound visits for residual predicates. Do not implement scale-facing queries by fetching an entire ledger into the client.
- [x] P17.6 Extend the read API with validation run/attempt/verdict projections and relationships needed for artifact manifests. The current four-family object response does not expose complete validation results. Include the actual execution/receipt/policy fences in results and distinguish absent requirement from a requirement with no run yet.
- [ ] P17.7 Implement fixed-prefix query cursors and server authorization on every page. Reject modified filters, another principal/tenant, changed route semantics, expired snapshots, and retention gaps with typed results. Bound retained cursors per client/tenant and release them on expiry and cancellation. Keep an empty filtered page distinct from exhaustion.
- [x] P17.8 Implement local pending-operation storage with exclusive ownership, private permissions, bounded records, atomic disk persistence and initialized-state loss detection. Route filesystem persistence through an owned blocking seam, not the async executor. Recover exact requests before regenerating defaults. Use fresh authoritative fencing evidence or a committed cancellation protocol before replacing an unknown intent; a generic rejection is not sufficient proof.
- [ ] P17.9 Add stable error categories and exit/result mappings for invalid input, unauthenticated, unauthorized, not found, ambiguous, conflict, busy/capacity, resync, guarantee unsatisfied, and unknown outcome. Preserve ordinary Inform/Yield as domain results. A failed validation is stored evidence, not automatically a transport/process failure.
- [x] P17.10 Add authenticated, read-only mutation-receipt and request-epoch reconciliation queries. The appended reconciliation operation covers both domain and cursor receipts, binds the authenticated principal/ledger/key and fresh quorum prefix, and reports the applied Raft index for metadata commits. CLI/MCP remote journal inspection verifies the exact saved business command without changing recovery state. Retained results, below-floor admission fencing and pending/unknown absence are distinct; historical noncommit is never inferred from absence. See [implementation and qualification](09-implementation-status.md#authenticated-request-reconciliation).
- [ ] P17.11 Define durable ownership of each principal's request-epoch stream across concurrent CLI/MCP/client processes. Serialize allocation and floor advancement or introduce independently fenced registered streams. One process must never reclaim an epoch containing another process's unknown request. Bound journal retention, receipt acknowledgment and garbage collection using durable completion/fencing evidence; process exit does not acknowledge work. Orders 1–8 below are implemented (registered streams, the shared coordinator, ordinal windows, acknowledgment, sealing, and since 2026-09-09 bounded close with automatic rotation, registry eviction under a persisted slot-generation watermark and privacy-safe occupancy: [15](15-managed-request-streams.md), [09](09-implementation-status.md) "Bounded generations"); the item stays open for order 9's mixed-version network qualification and the managed batching measurement.
- [x] P17.12 Add a narrowly scoped child-claim/continuation submission seam. Current node ingress supplies a root cause, so converting a DTO to `Cause::Claim` alone cannot authorize nested consults or corrective lineage. Validate parent existence, relationship/rank, current actor/receipt and committed policy inside the owner, then construct trusted cause metadata. Keep arbitrary `AuthorityContext` fields unavailable to CLI/MCP callers and test forged/stale/cross-ledger parentage. Native implementation (2026-09-09): a claim document names its `parent`; the client reads the parent's committed binding and current receipt and pins both in the frame; the owner admits the child only from the parent's issuer or its current receipt holder, only while the parent is live and unreleased, only at the exact binding and receipt (`creation.rs`, `scope::Registry::prepare_child`), on the same ledger, and derives the cause itself; the native context carries only the principal and the trusted time. Evidence: `cli_native_children.rs`, `native_host_tests.rs`, the MCP A1 test. Since 2026-09-09 the parent's authored policy escalation decides who besides the issuer may cite it (`none`, `holder`, `evaluator`), and corrections and follow-up consultations link to terminal claims by `invalidates`/`refines` under the owner's peer rules ([09](09-implementation-status.md) "Corrections and follow-ups under the authored policy (R5.2)").

The checked items above are implemented for the current persisted model; [19](19-cli-mcp-implementation.md) records their actual interface and compatibility boundaries. They do not activate the new lifecycle storage formats in [18](18-lifecycle-storage-upgrade.md).

Acceptance: golden flag/JSON/YAML-to-operation equivalence; existing wire fixtures unchanged; malformed and adversarial input cannot panic or overrun configured budgets; real service tests for every read predicate and empty/intermediate/final pages; crash tests before send, after commit/lost reply, and during journal replacement; historical validation results survive restart and are returned at the requested prefix.

### P17.11 implementation order and compatibility gates

The existing epoch window belongs to a principal, not a process. A local catalogue lock cannot establish that another machine, another operation store or an offline raw-wire client has no unknown request. The current reducer admits consecutive epochs and retains receipts when it advances the floor; domain and cursor outcomes occupy the same request-key namespace in separate tables. [Current epoch reducer](../../crates/focal-core/src/reduce.rs), [request-key model](../../crates/focal-model/src/command.rs), [cursor admission](../../crates/focal-ledger/src/cursor_session.rs)

Implement independently registered **managed request streams** alongside the unchanged legacy namespace. Registration and ordinal allocation are automatic client mechanics. Laptop users continue to submit claims without configuring topics, epochs, stream IDs or acknowledgment floors. Different stores or machines receive independent streams; processes sharing one store use a short durable allocator lock, released before network waits. Internal count/byte limits return explicit capacity errors and preserve completion capacity.

The concrete identity, lifecycle, crash ordering and compatibility contract is in
[15 — Managed request streams](15-managed-request-streams.md). The client namespace
is canonical `m1:<slot:8hex>:<generation:16hex>:<ordinal:16hex>:<request_id:32hex>`
with lowercase, fixed-width fields and persisted cluster/ledger/principal scope.
Only a durable reservation creates an ID; a missing ID never expands new work.
Stream revisions count control changes only, so concurrent ordinary receipts
cannot invalidate a saved ACK intent. The owner nonce is CAS identity, not a
separate security principal. Automatic local ownership is implemented by the
shared coordinator. Uncontended first use takes a slot read and a Register RPC;
ordinary CLI invocations require no reservation ceremony. CLI output must be
flushed before the durable delivery mark, and only a contiguous delivered prefix
can be ACKed. MCP requires explicit reservation/discovery and consumption ACK;
returning a tool result or canceling its wait does not acknowledge consumption.
The two adapters use separate bounded stores. The implementation remains in
progress until all gates below, including bounded close/rotation, complete
recovery qualification and managed batching, are qualified.

| Order | Concrete change | Required evidence before proceeding |
| --- | --- | --- |
| 1 | Freeze new stream identity, managed key, receipt and control DTOs. Scope identity to cluster, ledger, authenticated principal, bounded slot and monotone generation; bind each ordinal to an independent request ID and intent hash. Add scoped reducer entry points while retaining legacy entry points and serialized structures. | Golden legacy command, input, receipt, WAL and checkpoint bytes stay unchanged. Managed authority cannot change the actor or nominate another principal. |
| 2 | Add a bounded registry to the existing session owner. Register against a known vacant slot/generation with a persisted owner nonce. Commit registration by exact compare-and-set; retry the same registration after an unknown reply. | Concurrent registrations select one owner. Delayed registration cannot allocate a replacement after that slot has been reused. Full ordinary capacity still admits required completion controls. |
| 3 | Add one bounded ordinal window shared by domain and cursor admission. Track each occupied ordinal's request ID, hash, family and committed outcome. Enforce the window above its own acknowledgment floor. Reserve every changed row before proposal/publication. | Reusing an ordinal with another ID, hash or family fails. One full or stalled stream does not exhaust another stream's available window. Speculative preparation cannot bypass committed stream fences. |
| 4 | Add a versioned private client allocator and managed operation journal. Persist ordinal allocation, issuance frontier, normalized intent, generated IDs and exact envelope atomically before transmission. Use the existing owned filesystem seam and initialized-state loss protection. | Concurrent CLI/MCP processes never allocate the same ordinal. Crash cuts before/after preparation preserve either an unissued reservation or the exact prepared request; neither regenerates acknowledged identity. |
| 5 | Persist known receipts locally, then acknowledge a bounded contiguous manifest binding their original outcomes. Commit the stream floor and receipt removal atomically in Raft, covering both outcome tables. Acknowledgment uses reserved control capacity rather than needing another ordinary receipt slot. | Lost ACK replies retry safely. Full windows can drain. A cursor-command ACK does not release the consumer's separate delta-retention obligation. Receipt removal cannot publish without the committed floor. |
| 6 | Add committed sealing for unresolved ordinals. Seal the exact identity and intent: return the already-committed outcome or record an admission fence. Treat seals as ordered execution boundaries over effective pending state. | A pending proposal before the seal either supplies its committed outcome or cannot execute after the seal. Leadership change, delayed retries and prepared reducer/cursor rows cannot bypass the fence. Ordinary refusal and timeout never substitute for sealing. |
| 7 | Close a stream only after issuance has stopped durably and every ordinal through the saved frontier has a persisted result/ACK or a committed seal. Reuse bounded slots by increasing generation; generation exhaustion fails closed. | Process exit, lease expiry and client-wait cancellation cannot close streams. Old-generation registration, mutations, ACKs and seals are rejected after reuse without retaining unbounded stream tombstones. |
| 8 | Add managed reconciliation and explicit versioned client operation-ID namespaces. Preserve the original operation-ID/intent binding through compaction; retired namespaces return a typed fenced/expired result and cannot become fresh operations. | A lost old result cannot become a second business mutation after journal compaction, namespace retirement, adapter restart or reconnection. Existing unqualified operation IDs keep their old semantics and bounded capacity. |
| 9 | Version new log/checkpoint entries and activate managed proposals only after every voter can replay them. Add CLI/MCP discovery, recovery and migration examples around the same shared operations. | Mixed-version rejection is explicit. Existing raw-wire and epoch-one journals replay/retry unchanged. Recovery never infers legacy ownership from whichever local directories happen to remain. |

Client operation-ID retention is a separate contract from server receipt retention. The legacy [OperationStore](../../crates/focal-client/src/operation_store.rs) expands new requests whenever an ID is absent and bounds the catalogue at 256 IDs by default. Deleting a completed binding and accepting that ID later would execute a fresh request. Compacting full journals into immutable bindings is safe but does not by itself enable indefinite recycling. The managed namespace format, retirement response and compatibility decoder must be fixed in step 1 before implementing step 8; automatically switching an existing ID into a new default namespace is forbidden. Legacy IDs remain capacity-bounded and never silently expire or become reusable. A durable archive would be an alternative retention contract requiring its own lookup and recovery guarantees; the bounded managed path does not depend on adding an external archive service.

Acceptance includes simultaneous stores sharing one principal, never-transmitted gaps, canceled waits, both receipt families, ACK progress at capacity, lost registration/ACK/seal/close replies, leader changes around sealing, checkpoint plus WAL-tail recovery, and slot reuse with delayed old traffic. Add a client-ID collision/reuse test after compaction, and an explicit legacy raw request that remains recoverable while another managed stream is retired. P17.10's existing `Unknown` and `BelowFloor` meanings remain unchanged: a stronger negative answer requires the new committed seal.

## 4. P18 — Complete manual CLI and deployment commands

Dependencies: P17 for operation contracts; existing P08/P09/P13/P15 capability determines which cluster/deployment commands can execute. A command must not report stronger durability before that capability's activation gates pass.

Proposed files: `crates/focal-node/src/cli/` and a small `main.rs` dispatch boundary, using `focal-client`; command fixtures under `crates/focal-node/tests/cli_*`; `docs/cli/`, `examples/cli/`, shell completion artifacts. Shared DTOs/builders belong in the client layer rather than inside a binary-only parser.

- [x] P18.1 Add the noun-based `submit/get/list` commands with ergonomic flags and strict JSON/YAML/file/stdin input. Preserve existing startup/raw-request commands. Separate authored `--json/--yaml` inputs from `--format json/yaml/table` output. Make every list relationship filter optional, with tested unfiltered behavior for all four families.
- [ ] P18.2 Add lifecycle verbs, receipts, evidence sets, artifact upload/download/verification, validation requirement/run inspection, and bounded watch commands from the operation registry. Convenience workflows must expose pending or partial completion truthfully and resume from the same durable operation reference. Download to an exclusively created temporary file, verify complete byte length/digest, then atomically publish `--output`; never leave partial bytes at the final path or overwrite an existing file implicitly. Preserve a bounded resumable transfer record when supported.
- [x] P18.3 Add `schema`, examples and shell completions generated from the same operation descriptors. Help names required fields and their domain meaning; it must not require users to know numeric enum tags, internal shards, or Rust serialization layouts.
- [ ] P18.4 Add named client contexts with local defaults, endpoint/trust/identity selection, selected ledger and output preferences. Store credentials in private files and print redacted effective configuration. Authentication and authorization precede object lookup and namespace discovery. Do not accept a target agent ID as permission to act as that agent.
- [ ] P18.5 Finish cluster inventory/status and administration: founder creation/start, invitation creation/inspection/revocation, join/retry/status, member status, committed promotion/removal, leadership transfer, drain, credential rotation/revocation and safe recovery. Expose desired versus committed state, request identity, and the next required operator action. Existing invitation/join commands retain their identity-preserving journal behavior.
- [ ] P18.6 Finish `deployment explain/plan/apply/status` with durable plan IDs and exact desired-policy revisions. Bind a plan to observed metadata and show stale-plan rejection. Include progress and resume after disconnect. Preflight must expose unmet guarantees; apply cannot silently relax them. Keep offline `deployment explain` clearly separate from activation.
- [ ] P18.7 Add health/diagnostics, effective config, checkpoint/backup/restore verification, supported format/capability inspection and upgrade status. Dangerous or irreversible operations require explicit scope and a reviewable plan; ordinary reads and routine retry do not acquire unnecessary confirmation prompts.
- [ ] P18.8 Execute six command journeys: laptop, several machines, Kubernetes, multi-AZ, multi-region, global delegation. Use the same domain commands throughout. Track additional required concepts/flags per transition under P16; machine inventory discovery never invents geography or proof of custody.
- [ ] P18.9 Validate real binary behavior: exit codes, stderr/stdout separation, broken pipe, stdin size and encoding errors, paths with spaces, interrupted output/download, exact retry, read-only commands while service is live, and cancellation during shutdown. Include a Unix socket service test and authenticated QUIC context test. No command may need direct write access to a running ledger's files.

Acceptance: every supported row in the command registry has a binary integration test and example; equivalent flags/JSON/YAML produce the same semantic command and canonical authored hashes; no mandatory list filter; an empty query returns an empty bounded page; singular filtered reads detect ambiguity; bytes are digest-verified; cluster operations never conflate enrollment, membership, placement, custody, and activation. Unsupported planned operations remain visibly unimplemented in the status record, never successful no-ops.

## 5. P19 — MCP tools and reusable skills

Dependencies: P17 and the verified workflow policy in P20. P18 supplies a fallback CLI for environments without an MCP client. Detailed tool and skill proposals are in [12](12-agent-tools-and-workflows.md).

Implemented local adapter: `crates/focal-mcp/`; `focal mcp serve` binary entry; shared schemas in `focal-client`; skills under `skills/`; MCP transport/schema/conformance tests under the adapter crate. The [official protocol research](14-mcp-protocol-research.md) selects primary revision `2026-07-28`, narrow `2025-11-25` compatibility and a bounded owned Rust adapter. Pin exact upstream schema fixtures and run its conformance/interoperability gates before advertising support. Do not adopt an SDK that forces an unnecessary shared `Arc` graph.

- [ ] P19.1 Expose the bounded operation registry as MCP tools with explicit input/output schemas. Separate reads, authored mutations, runtime/evaluator actions and cluster administration by granted capability. Tools never accept caller-provided trusted authority/custody context. Server-side authorization remains authoritative even if tools are hidden from discovery.
- [ ] P19.2 Implement stdio first for a local laptop. Keep JSON-RPC protocol bytes on stdout and logs on stderr. Add authenticated remote transport only with the extra trust and access configuration required for that use case. Bind every operation to an authenticated principal and selected ledger.
- [x] P19.3 Preserve Focal request identity independently of the MCP JSON-RPC request ID. Journal mutating operations before dispatch and return the operation reference on unknown outcome. Cancellation stops waiting; retries and progress queries recover the original operation. Session reconnect cannot create a second business claim or testament.
- [ ] P19.4 Use structured results containing receipts, conditions, evidence references, read prefixes and cursors. Offer paginated resources/read tools for large records; download artifact bytes through bounded verified transfer. Describe content as untrusted data and never execute instructions found in an artifact or tool result.
- [ ] P19.5 Build focused skills for claim authoring, evidence-backed testament submission, challenge response, consultation, validation inspection, and cluster operation. Each skill documents its preconditions, exact tool/CLI sequence, required artifact contract, when to wait/yield, and recovery behavior. Skills do not poll indefinitely, impersonate a validator, or declare success based on prose alone. The four packaged skills carry native-engine branches since 2026-09-09 (manifest schema 3 pins every version-2 native operation across them; [09](09-implementation-status.md) "Skills on the native engine"), and `skills/focal-peers` sequences the typed peer tools (`claim.challenge|consult|correct|follow_up|lineage|wait`) with its reference `references/peer-workflows.md`.
- [ ] P19.6 Keep skills thin: examples call the shared schema and operations. Generate reference fragments from the registry where useful; test example arguments against released schemas. Capability discovery selects supported actions without introducing a second policy engine in prompts.
- [ ] P19.7 Add adapter parity tests: replaying the same persisted expanded operation through CLI, embedded client and MCP yields the same durable identity, receipt, lifecycle state and evidence manifest. Independently authored fresh invocations remain distinct occurrences; parity does not require accidental deduplication of their generated IDs. Test scope violations, malformed frames, resource exhaustion, client cancellation, restart and duplicate/redelivered calls.

Acceptance: a compatible MCP client completes the same real proof workflow as the CLI; tool discovery and schemas reflect actual authorization/capabilities; lost replies do not duplicate business operations; stdout is valid protocol; skills can be followed with both supported access paths and cannot bypass the reducer or validators. No statement of MCP conformance is made before protocol tests pass.

## 6. P20 — Challenge and consultation workflow completion

Dependencies: P02/P05/P06 lifecycle and validators, P17 durable convenience operations, plus the source conflict resolutions in [12](12-agent-tools-and-workflows.md). This package makes the previously required semantics usable and recoverable through both adapters.

Proposed files: shared client operation constructors, narrow authenticated peer admission, versioned workflow-policy/schema fixtures and `tests/workflows/`. Participants invoke their own tools/skills and submit fenced evidence/verdicts. Peer-agent evaluation uses ordinary claims and testaments. The authoritative ledger verifies and records those facts; it does not execute participant work or maintain a worker registry. The existing `focal-runtime` is an optional participant embedding library, not the home of a required Focal agent scheduler. There is no independent mutable challenge or consult database.

- [x] P20.1 (native engine, 2026-09-09: `claim.challenge|consult|correct|follow_up` are typed authored shapes of `claim.submit` with exact evidence targets, immutable policy, `invalidates`/`refines` lineage and derived identities; [09](09-implementation-status.md) "Peer verbs, lineage, the testament wait and the peer skill") Define challenge and consult as explicit claim intent/lineage with issuer, respondent, parent/cause, scope, acceptance requirements and evidence schemas. Integrate the owner-validated child-cause seam in P17.12; the existing root-only ingress cause cannot simply be replaced by an untrusted field. Resolve the current action vocabulary against the source audit before appending any variant. Preserve existing ordinal values and historical decoders. Avoid overloading a display string to carry execution authority.
- [ ] P20.2 Challenge creation commits the acceptance contract describing what evidence would substantiate the challenged claim. The respondent must acquire the current receipt, perform work and submit artifacts attached to a testament. The challenging agent's assertion alone is not proof; the response's prose alone is not validation.
- [ ] P20.3 Require actual participant-produced programmatic Pass before a quality phase when the requirement declares both phases. Explicitly agentic-only checks enter their declared phase directly; never invent a programmatic result, and keep structural admission mandatory. The issuer or designated evaluator invokes its own tool/skill; another evaluating agent receives an ordinary claim and returns testament/artifacts. Pin evaluator/implementation/version, target/manifest hashes, receipt epoch, policy revision and attempt. Only accepted authenticated fenced verdicts can advance the lifecycle. Expose the narrow peer submission seam without granting generic Runtime authority or adding a Focal job launcher. Distinguish evidence insufficiency from tool failure, capacity, unavailable content and canceled execution.
- [ ] P20.4 Let the authorized participant judge a qualifying challenge failure and author corrective claims according to the committed workflow policy. Bind parent, failure verdict/evidence, target and corrected acceptance contract. Derive or persist one stable decision/issuance identity so retries, restart and concurrent participant clients cannot issue duplicate corrections. Focal enforces the submitted decision; a daemon, subscriber or observer cannot create corrective work by itself. Reject unauthorized corrective standing and stale verdicts.
- [ ] P20.5 A consultation requests work satisfying the caller's query and declared output/artifact contract. Preserve parentage for nested consultations and aggregate delivery through graph satisfaction. For an unsatisfactory or incomplete response, the authorized participant normally authors a linked follow-up consult rather than a corrective claim. Any escalation must follow an explicit authorized policy and retain its evidence; do not reinterpret all consult failures as challenge failures.
- [ ] P20.6 Define timeout, cancellation, waiver/closure, renewal and escalation rules explicitly. Ordinary client cancellation is not business failure. A transport timeout must not issue a corrective claim. Trusted ledger timer inputs carry generation/receipt fences; actual work must not appear complete while required nested work or artifact custody remains outstanding.
- [ ] P20.7 (partly, 2026-09-09: `claim.wait` with the `testament` predicate and `claim.lineage` on the native engine; monitors carry the durable wake identity; adoption and late-report races stay qualified by A3) Provide participant-owned parked continuation recovery and stable wake/effect identity; reconcile out-of-order acknowledgment, late testament, nested consult completion, receipt adoption, validator retries and terminal-state races. Avoid a second callback-only completion authority. Result delivery cannot repaint an already terminal claim or re-run completed external work without its idempotency fence.
- [ ] P20.8 Provide CLI convenience verbs and MCP operations for starting/responding/inspecting/waiting on these workflows. Responses describe generated, posted, received, evidence submitted, validating, terminal and graph-satisfied state precisely. Expose linked follow-up/corrective claims and artifacts so an agent can continue from durable facts.
- [ ] P20.9 Test end-to-end happy and failure paths on a laptop and replicated deployment: valid proof, missing/wrong-schema artifact, invalid digest, failed deterministic check, quality failure, inconclusive evaluator, nested consult, stale receipt, expired timer, lost reply and restart. Assert exactly one policy-authorized follow-up per decision, immutable lineage and no fabricated satisfaction.

Acceptance: the user can challenge an agent, obtain a proof-bearing testament and inspect validated results/corrections; the user can consult an agent, obtain the requested work and follow-up consultation without default punitive correction. All completion decisions derive from committed lifecycle/evidence facts and survive process loss. No artifact-free challenge success or timeout-only corrective issuance is possible.

### Required independent object lifecycles

The target transition/authority decisions are now specified in [17](17-lifecycle-state-and-authority.md), with the required persisted-format transition detailed in [18](18-lifecycle-storage-upgrade.md). These contracts precede lifecycle mutation exposure. The additive `validation.context` shared client/MCP read and `focal get validation ID --context` CLI path inspect the current requirement, owning claim, current closing testament and a run/verdict page at one exact snapshot. This read uses existing wire operations and does not imply L2–L8 completion, an execution lease or a unique artifact target. Both adapters preserve the same continuation and never allocate managed mutation ordinals for this observation.

P17/P20 must finish the separate claim, testament, artifact and validation
lifecycle model before claiming upstream lifecycle equivalence. Current
`TestamentLifecycle` contains creation/acknowledgment, `ArtifactLifecycle`
creation/custody revision, and `ValidationLifecycle` creation/latest epoch. Runs
and attempts add actual verdict history, but do not supply the absent family
state machines. Implement explicit per-family transition authority and histories,
artifact-target validation binding, testament posting/receipt/outcomes, and atomic
child-to-parent propagation. Add fixed-prefix projections and replay/migration
coverage for each family; do not reconstruct nonexistent historical transitions
from the current claim status.

The initial D-03 single active close differs from Sylk's multiple posted
responses. Define that aggregation and identity contract explicitly, preserve
immutable prior proof and the existing graph least-fixpoint, and gate any new
persisted model through a real decoder transition. This is a model/compatibility
gap, not an adapter display change. See the [current/source matrix](02-domain-and-lifecycle.md#35-four-coordinated-lifecycle-families-implementation-gap).

### Source lifecycle traceability, 2026-09-06

This audit reads Hecate's current
[ledger contract](../../../hecate/docs/architecture/LEDGER.md),
[core contract](../../../hecate/docs/specs/LEDGER_CORE.md) and
[agent offices](../../../hecate/docs/architecture/AGENTS.md) first, then Sylk's
[lifecycle detail](../../../sylk/docs/CLAIMS_AND_TESTAMENTS_LIFECYCLE.md) where
Hecate inherits behavior. It does not import Sylk's implementation shortcuts as
authority or replace the explicit Focal decisions in [02](02-domain-and-lifecycle.md).

| Source requirement | Current Rust path and evidence | Remaining boundary |
| --- | --- | --- |
| Hecate Ledger §3: generated is not posted; progress cannot complete work | Core Generate/Post/Acquire/Progress; `generated_cannot_receive_and_terminal_progress_is_inform` and `every_terminal_state_rejects_progress_without_mutation` | A convenience workflow must keep each step and receipt explicit; `submit claim` alone generates |
| Hecate Ledger §§2–3: stream artifacts, freeze testament, let validations decide | Exact evidence-set manifest at Close; custody and receipt fences; `artifact_close_is_immutable_and_requires_verified_custody`; managed/legacy complete-workflow replay equivalence | Client request ACK retires a request receipt. It never substitutes for testament acknowledgment, validation or satisfaction |
| Hecate Ledger §2 and Core §4: Receipt is delivery only; stronger quality needs actual evaluation | New admission rejects Receipt handlers, evidence schemas and quality bars; separate deterministic/agentic Test requirement tests enforce ordering and error-only fallback | Historical committed malformed Receipt results replay unchanged; new Ack/whole-work execution of those malformed contracts is refused rather than silently passing |
| Hecate Ledger §2/Core §2: trusted cause, standing and policy | Core checks authenticated cause, actor, pinned policy and receipt; `Invalidates` fails closed without rank policy | Current authored builder and node ingress stamp root cause. P17.12 needs actual parent/receipt authorization; adding a DTO parent alone cannot confer it |
| Hecate Agents §2.4: challenge concrete activity/artifact with evidence; Sylk §10: response acknowledgment is not challenge satisfaction | The generic Challenge action currently has only generic claim constraints, including mandatory Receipt | P20 must add authorized target/proof schemas, acceptance validations, deadlines and continuation policy. Merely naming the action Challenge does not enforce those properties |
| Hecate Architect office: corrective author; Sylk lifecycle §5.3: remediation preserves failed proof | Immutable supersession and verdict fences exist; `terminal_supersession_preserves_old_proof_and_verdicts` | No complete policy-authorized corrective/follow-up issuer exists yet. Challenge correction and consultation follow-up must follow their different declared policies and exact issuance identities |

The current implementation anchors are the
[reducer](../../crates/focal-core/src/reduce.rs),
[validation runtime state](../../crates/focal-core/src/validation.rs),
[authored builder](../../crates/focal-client/src/input/build.rs),
[Core lifecycle tests](../../crates/focal-core/src/tests.rs) and
[managed replay tests](../../crates/focal-core/src/managed_tests.rs).

Focal decision **D-06** intentionally permits a non-agent issuer to request a
quality bar from a designated agentic evaluator. Reintroducing a blanket
participant-category prohibition would contradict that decision. The remaining
work is authenticated designation and evidence-bound submission by the actual evaluator, not
trusting an authored `agentic` handler flag as authority. Similarly, Sylk permits
receipt-only simple consultation; Focal's substantive consultation contract and
ordinary follow-up policy in P20 must be explicit. Missing proof, failed proof,
validator Error and unknown transport outcome stay distinct. No timeout alone
authorizes a corrective, and no monitor becomes an autonomous corrective author.

## 7. Delivery order and evidence

First land the source/operation inventory and strict input/query contracts. Then deliver an actual laptop CLI workflow, including unrestricted-within-ledger listing and exact pending-operation recovery. Add MCP and skills over that verified path. In parallel, finish the existing founder placement/custody/activation work, then extend the same commands to real multi-node journeys. Do not defer ownership, no-panic gates, authentication, pagination or retries until after adapters ship.

For each increment update [09](09-implementation-status.md) with executable command names, precise limitations, tests and measured resource bounds. The active goal is complete only after the applicable acceptance gates in P00–P20 pass, including deployment and scale qualification; writing this extension or exposing a parser alone does not complete it.


## Native binary delivery requirement

P15/P18 require a prebuilt cross-platform server and client. The same `focal`
executable supplies server, CLI and MCP. Source builds are an optional contributor
path. [20](20-binary-distribution.md) defines the implemented six-platform Unix
release pipeline and the remaining native Windows filesystem/transport port,
clean-machine installation, actual platform execution and tagged-publication
gates. A configured workflow is not proof that its platform runs have passed.
