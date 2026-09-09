# CLI and MCP implementation contracts

This records implemented interface work against [the CLI inventory](11-cli-spec-research.md)
and [the complete interface plan](13-cli-and-agent-implementation-plan.md).
It does not replace the independent lifecycle target in
[17](17-lifecycle-state-and-authority.md) or its required storage migration in
[18](18-lifecycle-storage-upgrade.md). The existing reducer still has one closing
testament per claim and the live decoder/reducer remains V1. Native Rust monitor
commands, direct root subscriptions, fixed-point settlement and trusted monitor
timers now exist in the separate RAM owner (§6.12 of document 18). Posted Required
Admission also reserves its complete graph failure and uses shared grant protection
(§6.7). Native receipt acquisition/adoption also fund the respondent's bounded
mandatory diagnostic, authored close and posting history through that owner.
These paths have not been connected to the running CLI/MCP dispatch.
Exposing existing operations is not completion of that migration.

## Peer operations

The shared Rust authored-operation registry is the contract used by human
flags, strict JSON/YAML documents, offline schema discovery, and MCP tools.
Normal mutations use independently registered durable request streams. Human
commands allocate and save their operation reference automatically. MCP clients
reserve a reference before mutation, and acknowledge result consumption
separately. Unknown outcomes retain the original expanded command.

| CLI | MCP | Actual transition and standing |
|---|---|---|
| `submit claim` | `claim.submit` | Generate an immutable claim and its pinned requirements as the authenticated issuer. |
| `submit claims` | `claim.submit_batch` | Atomically generate 1–64 immutable claims, including intra-batch dependencies, under one durable request identity. |
| `claim post ID` | `claim.post` | Post an admitted generated claim. |
| `receipt acquire ID` | `receipt.acquire` | Acquire responsibility as the allowed subject; create no testament. |
| `evidence begin --claim ID` | `evidence.begin` | Open an evidence set under the exact execution receipt. |
| `submit artifact` | `artifact.submit` | Register and attach verified evidence under the holder's receipt and evidence set. |
| `submit testament` | `testament.submit` | Respondent authors an explicit outcome, summary and confidence after success or failure, freezing the exact artifact manifest. Every non-Complete outcome requires a durable error artifact. |
| `testament receive ID --claim ID` | `testament.receive` | Issuer records receipt of the exact current testament. This is delivery acknowledgment. |
| `validation begin --claim ID` | `validation.begin` | Issuer pins whole-work validation runs over the acknowledged response. |
| `validation begin-increment` | `validation.begin_increment` | Issuer pins an incremental requirement to an already attached artifact and an observed evidence manifest. |
| `artifact register` | `artifact.register` | Participant registers its own independent proof artifact without borrowing a respondent's receipt. |
| `submit validation` | `validation.submit` | Actual designated evaluator submits its fenced result and committed proof references. |
| `validation complete --claim ID` | `validation.complete` | Issuer requests deterministic aggregation of committed results and graph conditions. No desired outcome is accepted. |
| `claim cancel ID` | `claim.cancel` | Authorized cancellation with a reason. |
| `claim supersede ID` | `claim.supersede` | Generate a compatible successor with explicit immutable predecessor lineage. |
| `claim release-scope ID` | `claim.release_scope` | Native: the issuer releases a terminal claim's owned scope once. |
| `receipt adopt ID --holder P` | `receipt.adopt` | Native: the issuer replaces the current holder; the committed receipt is fenced one epoch earlier. |
| `artifact fail --claim ID --slot N --diagnostic ID[:HASH]` | `artifact.fail` | Native: the holder records an unproducible slot with its committed production diagnostic. |
| `artifact receive ID --claim ID` | `artifact.receive` | Native: the issuer receives one generated work artifact. |
| `artifact reject ID --claim ID --reason structure\|metadata` | `artifact.reject` | Native: the issuer rejects one work artifact with its own diagnostic, inheriting the product's visibility. |
| `validation begin\|report … --phase admission\|increment [--target ID]` | `validation.begin`, `validation.report` | Native: the same two verbs select admission and increment evaluations by phase. |
| `validation seal-increments --claim ID` | `validation.seal_increments` | Native: the issuer freezes increment target membership while the response is open. |
| `validation enter-whole-work ID --claim ID` | `validation.enter_whole_work` | Native: the issuer closes the increment cohort of the received testament and enters whole-work evaluation. |
| `audit generate --claim ID`, `audit post ID` | `audit.generate`, `audit.post` | Native: the issuer generates and posts the closed claim's result testament. |
| `monitor register\|rebind\|cancel` | `monitor.register`, `monitor.rebind`, `monitor.cancel` | Native: durable waits over committed claims with a logical-time deadline; rebinding follows a committed supersession; cancellation needs a terminal owner. |

Each of the four families has singular `get` and plural `list` commands. List
filters are optional. `get claim --source` enforces singular selection; use a list
for multiple matches. `get validation ID --context` and `validation.context`
return the requirement, claim, available closing testament, and bounded result
history at one fixed read prefix. This read does not execute a validator.

The respondent must report failed, refused, impossible, interrupted and partial
work through an ordinary testament. A summary or status flag cannot replace
error evidence. The shared builder refuses non-Complete empty manifests; Core
checks the actual committed or pending error row's ID/hash, ledger, current
receipt, producer, schema and durable custody, as well as the exact evidence set.
Only the claimant receives the report; designated evaluators then record what
their checks establish. Those facts determine acceptance independently of the
respondent's reported outcome. New `FailTestamentGeneration` requests are refused;
that old runtime-synthesized response is a historical replay operation only.

### Native wire profile

Native operations ride protocol version 4, negotiated only by a handler that
admits managed requests, participant requests and native frames
([native.rs](../../crates/focal-wire/src/native.rs)). The profile carries three
registered operations: `Native { frame }` (tag 25) submits one borrowed
`FCNINPUT` frame ([21](21-native-input-format.md)) that the owner decodes
exactly as the client journaled it; `NativeRead` (tag 26) returns fixed-prefix
documents of committed native rows; `NativeList` (tag 27) is a bounded list
over the native index families ([22 §7](22-native-record-format.md)), served
statelessly from the committed prefix with a node-authenticated continuation.
Under the profile the shared content transfer,
managed request stream, reconcile, summary and stream operations stay
admissible; legacy typed submissions and legacy reads do not, and native
operations under any other profile are refused as an unsupported protocol.

Before dispatch the wire layer inspects only the frame's fixed header: magic
and format version, a registered content profile, the actor namespace (timer
namespaces are unauthorized from every peer), the envelope's ledger, the
authenticated peer as the frame principal and the envelope's request epoch and
ID as the frame's request identity. Node peers hold no native capability. The
owner then proves every role and binding from its committed prefix; exact
retry of an identical frame returns the committed receipt because the owner
resolves request identity before admission, and a different intent under the
same key is a closed `Conflict` refusal. Replies are `Committed` (the native
receipt with prefix position, logical time, operation, intent and row counts),
`Pending` (a ticket the client resends until it commits; never success) or
`Refused` with one closed category: invalid input, unauthorized, not found,
stale binding, conflict, capacity, or a contract refusal code mirroring the
lifecycle contract errors plus `Legacy` and `Unsupported`.

Reads accept the existing consistency vocabulary: linearizable reads pass the
native read barrier first, `AtLeast` and `Exact` tokens are checked against the
native prefix, and stale projections read the current prefix. Documents mirror
the committed rows explicitly (claims with their obligations, lineage,
acceptance slots, scopes and authored content; definitions with their program
and authored specification; evaluations bound to their declaration; accepted
results; artifacts with local custody; work artifacts and diagnostics;
responses; result testaments; receipts; monitors; outcomes; creation results;
events; frozen legacy rows as bytes). Claim expansions append the related
objects to the same page. Bounded lists are served from the index families,
the validation context is composed from one prefix, and the node's timers
are scheduled by the due-timer family
([22 §7](22-native-record-format.md#7-secondary-index-families)); claim
history expansion still waits for a later batch and is refused, never
partially served; on the
replicated path artifact-bearing frames are refused until the content host
proves custody for native frames, while the embedded owner seals and verifies
them through its exclusive content writer.

### Native client path

Every host authors native operations the same way. A document (JSON, YAML or
CLI flags) parses into one `NativeAuthoredOperation`
([native_documents.rs](../../crates/focal-client/src/operations/native_documents.rs));
its descriptor is version 2 of the same verb name the V1 catalog exposes
([native_catalog.rs](../../crates/focal-client/src/operations/native_catalog.rs)),
carries `wire = Native` and `retry = NativeN1`, and publishes a hand-written
input schema ([native_schema.rs](../../crates/focal-client/src/operations/native_schema.rs)).
The host first asks the compiler which committed objects the verb binds to
(`requirements`: the claim, the claim and its response, or the claim and the
current evaluations of one declaration), reads them once at a fixed prefix and
extracts their bindings (`Resolved`), then compiles the document, the
authenticated context, the claimed request identity and those bindings into a
`NativeInput` and encodes the `FCNINPUT` frame
([focal-native-client](../../crates/focal-native-client/src/lib.rs)). The frame
bytes are the journaled wire body; the same decoder the owner runs computes
their intent fingerprint locally so a committed receipt can be bound to the
exact bytes without trusting the reply's own claim.

The compiler derives what the model derives: issuer, subject, action and cause
relations come from the actor, `target`, `action` and `parent`; authored
relations name committed claims of the same ledger, or, for `reviews` and
`derived_from` under descriptor schema 2, one artifact at its committed
descriptor hash (`artifact:ID@HASH`, input target tag 4), and a document's
`policy` selects schema 2 with the claim's follow-up rules; the mandatory required
whole-work receipt declaration becomes the delivery program; every declaration
is pinned by identity and specification hash; a slot's missing-slot obligation
is a virtual declaration index no declaration uses; work artifacts carry the
holder's receipt fence and the next cycle; a closing testament orders its
manifest by slot and its diagnostic citations by artifact identity and binds a
nonzero content hash of the report at revision one; a report names the begun
attempt the ledger reported and binds its result artifact to the exact target,
generation and attempt. What the compiler cannot see it does not guess: the
owner still fences stale bindings, missing citations and wrong actors.

Identity is durable before transmission
([native_store.rs](../../crates/focal-client/src/native_store.rs)): the store
claims an `n1:` reference (the request identity, epoch one) under the
canonical document, persists the compiled frame, fingerprint and minted object
identities, and only then marks the operation ready. `Client::submit_native`
resends the identical frame while the owner answers with a pending ticket; if
the ticket never commits within the retry policy the outcome is reported
unknown with the request retained, and a refusal is final even after an
uncertain attempt because the owner resolves the request key before admission.
A capacity refusal from the node admitted nothing, so the client resends the
identical request up to three times with backoff and then reports the refusal
itself; the journaled reference stays pending for a later exact retry.
Only a committed receipt whose invocation and intent equal the journaled frame
is recorded. Refusal categories map to the exit classes of
[failure.rs](../../crates/focal-client/src/failure.rs): invalid input 2,
unauthorized 3, not found 4, stale or conflicting 5, capacity 6, and a pending
ticket is the unknown outcome 7.

The R4.0 coverage table
([native_inventory.rs](../../crates/focal-client/src/operations/native_inventory.rs))
is an exhaustive match over every operation of the committed native prefix:
its frame tags (codec numbering), authoring actor, descriptor name, human CLI
path, result, the reads that precede compilation and its exposure. Every
participant operation of the committed prefix (27 of the 31 kinds) is an
authored tool: the four evaluation kinds share the two `validation.begin`
and `validation.report` descriptors selected by `phase`, and the twelve
descriptors added by the R4.5 verb batch (`claim.release_scope`,
`receipt.adopt`, `artifact.fail`, `artifact.receive`, `artifact.reject`,
`validation.seal_increments`, `validation.enter_whole_work`,
`audit.generate`, `audit.post`, `monitor.register`, `monitor.rebind`,
`monitor.cancel`) bring the native catalogue to 36 descriptors; the three
timers and the import are internal. The `WireOnly` exposure stays defined
and a test asserts it is empty, so a future owner operation must choose its
surface explicitly. Each verb's compile reads exactly the objects it binds
(a work artifact and its descriptor for a rejection, the diagnostic for a
failed slot, the result testament for its posting, both claims for a
rebinding), and the CLI's `focal audit` root is the only addition to the
command tree of [13](13-cli-and-agent-implementation-plan.md).

### Native CLI routing and replicated custody

The manual CLI decides the engine once per invocation with a standing read
under protocol 4 ([native.rs](../../crates/focal-node/src/cli/native.rs)); a
node without the engine refuses the profile at negotiation, and the V1 path
is unchanged. On a native ledger every verb adapts its flags into the native
document ([native_documents.rs](../../crates/focal-node/src/cli/native_documents.rs)),
reads the objects the compiler names, compiles and encodes the frame, claims
its `n1:` identity in the client journal and drives the exact frame; the
result, a closed refusal or the recovery command are printed in the shared
application result shape (schema version 2). `focal list` on a native ledger
serves eight families through the same flags (`claims`, `testaments`,
`artifacts`, `validations` and the native `evaluations`, `receipts`,
`monitors`, `events`): the flags become one list document, the driver
translates it into the wire filter, and `--all` follows the continuation
until it is absent, treating an empty page with a cursor as a
residual-filtered stretch rather than the end
([native_lists.rs](../../crates/focal-node/src/native_lists.rs)). Flags a
family does not index are refused, never ignored, and the V1 path refuses
the native-only flags the same way. `focal watch` and the four `watch.*`
tools run on a native ledger with the same names and options: the watch
journal saves the engine it was created for, speaks the native wire profile,
seeds through linearizable native reads after the source pins the snapshot
(a claim filter reads each claim with its responses and evaluations; an
unfiltered watch lists the family, responses being reached through their
claims), and then follows the tail of schema-2 deltas derived from the
committed native events on the ledger's continuous stream line
([23 §6](23-native-activation-and-import.md)); the validation context read is
served from one prefix
([native_reads.rs](../../crates/focal-node/src/native_reads.rs)
`validation_context`: the selected registration and evaluation, the
target's manifest with custody, the results after a revision cursor and the
delivery result) and reached by `get validation ID --context` and
`validation.context`; the trusted timers fire from the node's sweep
([native_timers.rs](../../crates/focal-node/src/native_timers.rs)) without
any verb. `focal schema coverage` prints the R4.0 table and `focal schema get
NAME --native` the version 2 schema.

On the replicated host the data service attests artifact-bearing frames
before the owner sees them: it decodes the frame's artifact under the
session's own decode limits, has the exclusive content writer seal and verify
the inline payload under the current custody placement, and submits the frame
with that evidence; the owner still binds the evidence to the exact frame
before admission ([evidence_service.rs](../../crates/focal-node/src/evidence_service.rs),
[content_host.rs](../../crates/focal-node/src/content_host.rs),
[fleet.rs](../../crates/focal-node/src/fleet.rs)). Native reads and receipts
are sized like their V1 counterparts in the replica's reply accounting, and a
lost native read reply is reported unavailable, never as an unknown outcome.

### Native MCP tools

The MCP adapter decides its catalogue once per connection: `serve` creates the
worker runtime first, runs the engine probe on it (a remote transport binds
its endpoint to the first runtime that drives it) and, on a native ledger,
opens the adapter's own `n1:` journal (`client/mcp-native`, beside the human
CLI's `client/native`) and serves the native catalogue
([catalog_native.rs](../../crates/focal-mcp/src/catalog_native.rs),
[native_backend.rs](../../crates/focal-mcp/src/native_backend.rs)): every
native descriptor the standing permits (a projection-only ledger withholds
`claim.submit`), the five exact reads, the eight bounded lists (`claim.list`,
`artifact.list`, `validation.list`, `evaluation.list`, `testament.list`,
`receipt.list`, `monitor.list`, `event.list`; results are `native_list`
pages whose `next` cursor is passed back verbatim), and `request.inspect`, `request.retry`,
`request.pending` and `request.acknowledge` over `n1:` references. Output
schemas carry `urn:focal:mcp:NAME:output:2`, so a native tool never shares an
identity with its V1 namesake. Both adapters compile through one driver
([driver.rs](../../crates/focal-native-client/src/driver.rs)): resolve the
compiler's requirements with the host's blocking read, compile, encode,
fingerprint, claim the identity in the journal and drive the exact frame;
identical documents therefore produce byte-identical frames from flags,
documents and tools. MCP results are consumed explicitly, as managed results
are: a committed operation stays in `request.pending` until acknowledged, a
recorded refusal leaves it, and `request.inspect` with `remote: true` (and
`focal request inspect --operation-id n1:… --remote`) reads the owner's
committed outcome by request key so each adapter can observe the other's
operations. An unreachable owner at startup keeps the V1 catalogue only for a
context that never journaled a native operation; a context with a native
journal refuses to start as V1.

### Admission profile and durable compatibility

Peer mutations use explicitly negotiated wire protocol 3. The server advertises
that profile only through a handler implementing participant admission. Profiles
1 and 2 retain their previous capability restrictions. A profile 3 envelope for
another operation is rejected; a node certificate is not participant authority.

The ledger owner checks committed immutable issuer/producer identity before
allowing an individual legacy runtime-gated command. Authentication alone leaves
`authority.runtime` false for an Actor. This is a command-specific trusted
bridge, not a Runtime credential or caller-supplied authority context.

Receive, begin, and complete commands require a claim revision. CLI and MCP read
that revision when preparing a new command and persist it in the expanded
request. They never refresh it during a retry. Core rechecks the revision and
legal state against pending changes. Incremental admission also checks the
requirement's claim and phase, the selected artifact's membership, and the exact
manifest observed in the committed evidence set. Later artifact attachment does
not rewrite that pinned manifest.

Verdict admission uses the authenticated evaluator. Core independently checks
the actual requirement and run, target hash, phase, epoch, handler identity and
version, attempt, manifest, current optional execution receipt, and evidence.
The verdict does not grant runtime authority. `fail` is a valid recorded result;
a successful write of failure evidence is not a transport error.

The profile uses existing persisted `Command` variants and authority fields.
It changes ingress admission, not historical reducer semantics, WAL command
ordinals, checkpoint object layouts, or the frozen managed decoder floor.
The independent lifecycle migration must still follow document 18.

Successor claim builders and dispatch have an earlier authored-content gate in
[18 §6.13](18-lifecycle-storage-upgrade.md#613-successor-input-codec-dependency-plan).
The current native creation projection has no complete work description/document,
occurrence/schema, action, authored work scopes or contextual input references.
CLI/MCP must preserve those fields and complete requirement instructions,
quality-bar/rubric references, contributor provenance and policy-resolution
revision through the checked immutable owner representation. A displayed document
that the submitted body omits is not an authored claim, and an opaque supplied
hash cannot stand in for that document. Runtime monitors/ownership scopes are a
separate concern. Explicit Consultation/Challenge purposes and Focal's self-work
restriction with its checked legitimate Handoff exception must survive the shared
builder and admission path.

Complete content ownership, recomputed nonrecursive content identity and bounded
effective-state dedup precede codec freeze and live successor activation. Existing
V1 content and exact retries, and current native semantic request identities,
remain unchanged until the extended body receives an explicit successor profile.
No builder may invent absent instructions, infer work scopes from prose, or treat
handler references as authority to execute tools inside Focal. See also
[17's executable boundary](17-lifecycle-state-and-authority.md#11-executable-contract-boundary).

For successor activation, the shared validation builder must preserve the native
Admission contract as well as artifact/verdict authority. A Posted Required
`BeginAdmission` accepts responsibility only after the owner funds its complete
indexed failure component. Its final blocking report atomically retains Accepted
at ordinal 2, the original PostFailed cut, dependency and monitor consequences,
and any cohort seals. The final claim revision may include a later monitor release
from that same transaction. CLI/MCP must return the original operation outcome and
publication coordinates on retry, without refreshing the claim or asking the
participant to replay graph completion. A successfully recorded failing verdict
remains a successful write of that evidence.

Eligible reports after receipt or terminality remain independent audit evidence;
they do not reopen the claim or create another graph transition. Later authored
dependencies and monitors can be refused when they exceed a held report's graph
promise; clients must retain the actual refusal rather than presenting an accepted
but unfunded operation. This describes the implemented native RAM contract and
its pending interface mapping. Existing live operations above continue to use V1
semantics until native codec/WAL/Session activation and shared dispatch are complete.

Successor receipt builders must preserve the native respondent funding boundary.
`AcquireReceipt` and `AdoptReceipt` use Ordinary admission for the exact receipt's
remaining diagnostic, close and post credits before returning an accepted
candidate. The held diagnostic path pins the builtin Work error contract; the
respondent still authors the actual artifact and the explicit response. Separate
`CloseResponse` and `PostResponse` operations must retain their own exact request
identities, manifests and source revisions. Several Generated responses can await
posting, and owner reconstruction includes that complete current-receipt backlog.
Clients must not translate acquired capacity into an automatically generated or
posted testament, or translate a diagnostic into acceptance.

Optional evidence remains subject to admission that preserves the guaranteed
closing shape and first Work diagnostic slot. Failed funding or candidate discard
preserves the source and restores provisional credit; adoption replaces the
receipt grant atomically while retaining original evidence/history. These are
implemented native RAM/finite-record semantics, not a guarantee already exposed
by the V1 endpoints in the table. Claimant receipt/evaluation-entry funding,
whole-claim/control closure, disk/replica quotas and native codec/WAL/Session
activation remain required before the complete successor interface is available.

### External execution

An issuer or designated peer invokes its own skill, MCP tool, agent framework,
script, test runner, or ordinary program. Focal records pinned handler identity
and version, expected evidence contracts, and actual submitted results. It does
not load scripts, launch agents, route jobs, or attest that external code ran.
An agentic verdict remains an assertion by the authenticated evaluator, supported
by immutable proof artifacts. Requirement definitions remain language agnostic.
Current admission requires a programmatic handler before a declared `quality_bar`
and one final agentic handler. An agentic check without that field is supported;
agentic-only quality-bar requirements still need the successor lifecycle profile.
The pinned handler currently carries ID/version/agentic metadata. A typed stored
tool/skill/code locator and definition binding remain L6 work; the participant
currently resolves the pinned implementation in its own environment.

## Recorded validator contracts

`validator list` / `validator.list` inspect the external handlers actually pinned
in immutable validation requirements. `validator get ID --version HASH` /
`validator.get` select an exact handler version. Every list filter is optional;
claim, evaluator, kind, phase, mode, agentic capability and expected evidence
schema are conjunctive. One row is a whole requirement, retaining its original
handler chain, quality bar, evidence schemas and policy revision. Different uses
of the same handler do not collapse into an invented universal contract.

There is no installed server worker registry. These reads report recorded
contracts; availability in the invoking participant's environment is unknown.
They do not install or execute code. Participants map the pinned ID/version to
their own program, skill or tool and submit their actual fenced verdicts.

The appended read-only `Operation::Validators` uses the existing graph owner and
fixed-prefix list leases. Claim selection uses the existing ByClaim index; other
predicates consume bounded requirement visits. Handler matching inspects at most
the admitted handler bound per requirement. Continuations authenticate all
predicates, limits, principal, ledger, route and lease expiry with the owner's
cursor key. Empty filtered pages retain their continuation. Repeated exact
cursors return the same prefix and rows; a modified selector cannot resume them.
No stored object, command, receipt or checkpoint encoding changes.

## Client contexts

Local operation requires only the data directory. Named contexts add connection
selection without changing the domain commands:

```sh
focal context add laptop --node-data-dir /absolute/path/to/node
focal context use laptop
focal list claims
focal --client-context laptop get claim CLAIM_ID
focal context show
focal context list
focal context use local
```

For an independently authenticated remote participant:

```sh
focal --data-dir /node cluster client invite --name alice --output /private/alice.invite
focal --data-dir /client context enroll alice --invite-file /private/alice.invite
focal --data-dir /client context use alice
focal --data-dir /client list claims
focal --data-dir /client mcp serve
```

Enrollment persists one invitation, key, CSR, request ID, and completed
certificate receipt. Retrying enrollment keeps them. A Client enrollment grants
Actor standing; it does not create a physical node or confer Runtime/Node
authority. Designated evaluators use normal participant standing. Committed
certificate projections retain active Client grants across refresh and restart
and remove revoked or expired ones.

The context catalogue is private, bounded to 64 names including retired names,
and backed by an atomic checksummed private journal. Each name gets separate
request, upload, and MCP history. Removed names retain their binding and recovery
files. Losing initialized catalogue or request history fails closed. Explicitly
selecting `local` restores the ordinary local context. `--client-context` avoids
colliding with the existing validation `--context` read option.

`context add --file` accepts an explicit Unix or QUIC connection document for
already provisioned credentials. Remote documents carry credential paths, not
secret bytes. Endpoint, trust, selected ledger, and client identity are distinct
fields; a principal in a document is a client hint checked by authenticated
ingress, not permission to impersonate it. Effective configuration output
redacts credential paths and key material. Remote MCP discovery does not inherit
the unrelated local node's administrative socket. Named Unix MCP contexts use
the selected physical node's socket, independently of their private request-history
directory. CLI cluster commands likewise resolve the selected Unix context and
reject remote participant contexts instead of acting on another local cluster.

## Reads, observation and recovery

`claim.wait` is a bounded read-only observer of satisfaction, terminality or
release. It performs at most 31 fresh point reads under one deadline of at most
30 seconds and retains only the last compact observation between probes. `Met`,
`Pending` and `Unmet` describe that observation, not a new lifecycle transition.
The command creates no monitor, request stream, worker or timer input. Longer
observation uses the explicit durable watch interface.

`ledger.summary` obtains actual map counts after a fresh quorum barrier without
scanning or cloning the ledger. The observation includes its applied index and
domain prefix; it does not allocate a historical snapshot lease. Traversal uses
bounded graph indexes and a query-bound fixed-prefix continuation. Neither read
claims to measure global health or deployment guarantees.

`list --all` streams one bounded page at a time. Each next request preserves the
same filters, limits and returned continuation. Empty filtered pages still
advance. JSON uses one complete page per line; YAML uses document markers.
Only successful write and flush permit fetching the next page. Cancellation or
an expired lease ends the query without mixing in a newer prefix. Already
written pages remain partial output and retain their continuation information.

Durable watches retain one exact held delivery until explicit consumption.
CLI consumption follows stdout flush; MCP clients acknowledge the delivery ID
only after their destination accepts it. Source acknowledgment and request
retirement happen separately on subsequent progress. Restart redelivers held
pages, including their original seed prefix. Expired seed leases require an
explicit new observation; a reconnect does not silently skip missing evidence.
On a native ledger a delivery is either a native seed page (the objects read
at a native prefix and the step that follows) or a tail page whose deltas
carry `schema` 2 and a `Native` fact: the exact committed event record, the
nearest legacy lifecycle action, the acting principal (zero for trusted
timers and the import) and the claim concerned. Facts committed between the
pinned snapshot and the seed's read prefix appear both in the seed objects
and as deltas; sinks deduplicate by object binding. A watch created for one
engine keeps that engine; journals written by earlier development builds
(`FCLWAT01`) are refused, not reinterpreted.

`monitor.register` records existing durable graph predicates under an active
claim issued by the authenticated participant. It requires explicit timer,
generation and future logical-time deadline values; no client clock event is
invented. `monitor.get` reads the actual monitor after a fresh quorum barrier.
Release is a stored fact, with its original sequence, and is not by itself proof
that every root succeeded. There is no associated Focal worker or callback
process. A caller's timeout cannot expire the monitor.

The separate native RAM owner now implements authenticated `RegisterMonitor`,
`RebindMonitor` and `CancelMonitor`, exact root subscriptions, frozen monitor/graph
settlement and owner-delivered monitor timers. These are typed Rust transactions,
not newly available live CLI/MCP commands. Their source and funding contract is in
[18 §6.12](18-lifecycle-storage-upgrade.md#612-runtime-scope-integration-sequence).
The current `monitor.register`/`monitor.get` operations above retain their V1
meaning and existing negotiated transport profile.

Successor interface activation still needs the native codec, WAL/Ready and Session
path, followed by shared authored-operation schemas and CLI/MCP dispatch. Expose
original predicates, registration, deadline, named rebind history and exact
monitor disposition through those reads. Registration/rebinding/cancellation must
resolve the actual claim revision and optional receipt for a fresh request and
retain them unchanged on retry. Rebinding names both actual endpoint bindings and
the required `Supersedes` relationship. Explicit terminal-owner cancellation
retains the original owner terminal position and its separate cancellation cut;
it must not display as successful predicate release or alter the claim's failure.

Owner `ReleaseScope` remains a separate claimant operation after its monitors and
owned children meet release preconditions. Automatic monitor settlement does not
release the owner or its targets. A client timeout remains a read outcome and can
never be decoded as native timer authority. Shared reads and optional-filter lists
must preserve these distinctions when the new lifecycle profile is activated;
old-profile release observations cannot supply missing successor provenance.

Large payload workflows save exact staged bytes, hashes, offsets, upload identity
and final artifact command before transmission. `artifact upload inspect` sends
nothing; `artifact upload cancel` saves and retries the existing cancellation
request. Cancellation preserves immutable content references and committed
artifact facts. A terminal server upload fence prevents delayed Begin from
reviving a canceled or finished local upload identity. File-transfer cancellation
does not seal an unrelated reserved domain operation.

Shared error classification distinguishes input, authentication, authorization,
capacity, stale views, conflicts, retirement and unknown mutation outcomes.
Errors never authorize fresh mutation identities. CLI structured diagnostics go
to stderr, while results and partial pages stay on stdout. MCP uses the same
SDK classification through nested store, watch and transfer errors. Administrative
errors preserve their separate root/application operation identities and fences.

Offline `schema validate` compiles strict authored input under a real selected
context; `--shape-only` explicitly limits the check to document structure.
`request build` freezes a complete raw envelope into a private, no-clobber file.
`request check` verifies supported raw shape, resource bounds and exact wire hash,
without authenticating the author. The existing raw sender remains compatible
with protocol families outside the offline checker's supported subset. Discovery,
examples and completion derive from the released descriptors and command tree.

The internal authored RAM owner now accepts complete grouped claim/validation
bodies and retains their content, responsibility profile and returned identity
mapping atomically. Its [typed read boundary](../../crates/focal-core/src/native/authored_reads.rs)
exposes matching content and lifecycle from one effective or pinned prefix. This
is a prerequisite for the successor CLI/MCP input compiler; the live CLI, SDK and
MCP still select V1. The internal `CreateAuthored` command does not silently change
existing JSON/YAML wire output or activate a new service codec.

## One registry for every surface

Since 2026-09-09 the watch, transfer and administration tools are shared
`OperationDescriptor`s in the client crate
([catalog_watch.rs](../../crates/focal-client/src/operations/catalog_watch.rs),
[catalog_transfer.rs](../../crates/focal-client/src/operations/catalog_transfer.rs),
[catalog_admin.rs](../../crates/focal-client/src/operations/catalog_admin.rs)),
beside the V1 and native application catalogues. Every descriptor names its
`Surface` (application, watch, transfer, administration), its `Capability`
(`Actor`, `Node`, or `FounderNode` for the two invitation tools), its retry
identity (`a1:`/`r1:` administration references, exact-argument resumption for
watches and uploads, or a fresh intent per call), its reviewed input schema
and, where the human CLI performs the same operation, its `cli_path`. The MCP
adapter derives its catalogues from these descriptors and dispatches by
surface (`operations::surface_of`), so a tool it did not advertise is refused
by the dispatcher as well as by the protocol layer; the command-tree test
resolves every `cli_path` to a leaf of the clap tree. The operator surfaces
R9 adds (`deployment.*`, `backup.*`, `upgrade.*`) extend this registry.

## Remaining independent work

The complete plan remains the completion checklist. Interface implementation
must not silently mark storage upgrades, independent artifact/testament states,
child-cause authorization, challenge/consult continuation policy, arbitrary
schema installation, placement activation,
credential rotation, global deployment journeys, or scale qualification complete.
Each requires its actual authoritative service contract and executable tests.
The release status document records the tests that have run; source availability
alone is not qualification.

In particular, the legacy `GenerateClaim` revision fence addresses the new claim,
not an independently changing parent, and the legacy engine has no child-cause
API. On the native engine a claim document's `parent` is that API
([13 P17.12](13-cli-and-agent-implementation-plan.md)): the client reads the
parent's committed binding and current receipt and pins both in the frame's
intent, and the owner admits the child only from the parent's issuer or its
current receipt holder, only while the parent is live and unreleased, only at
that exact binding and receipt, on the same ledger; the owner derives the
`caused_by` relation and the lineage itself, registers the child on the
parent's scope registry as one parent revision, cancels pending children with
a cancelled parent, and refuses forged, foreign, stale, late and unauthorized
parentage with typed outcomes. Released or terminal parents cannot be reopened
to add completion obligations; corrective/follow-up work needs valid enclosing
cause and immutable lineage.
Claim relation endpoints are other claims or, from descriptor schema 2, one
exact committed artifact (`RelationTarget::Evidence`, [21 §5](21-native-input-format.md));
a challenge against one exact artifact cites it there, and its follow-up
policy travels as authored content of the same descriptor (decision F28),
never as a hash in a display string, scope key or schema list. A correction
`invalidates` the terminal challenge and `reviews` the report of its failed
verdict; a follow-up consultation `refines` the consultation it continues;
the owner admits both under the followed claim's policy without reopening
it (decision F29), and `NativeView::related_claims` lists them from the
relation index. The client packages these as authored shapes of
`claim.submit` (`claim.challenge`, `claim.consult`, `claim.correct`,
`claim.follow_up`: typed documents lowered to one claim document by
`focal-native-client/src/peer.rs`, so the coverage table claims them through
`claim.submit`'s frame tags), composes `claim.lineage` from one full claim
read, bounded ancestor reads and three relation lists at or after the first
read's token (`observe.rs`), and observes `claim.wait` with the V1 bounds
plus the `testament` predicate; the CLI verbs `claim challenge|consult|
correct|follow-up|lineage|wait` and the MCP tools of the same names share
those documents.
