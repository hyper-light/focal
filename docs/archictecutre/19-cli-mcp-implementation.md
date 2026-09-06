# CLI and MCP implementation contracts

This records implemented interface work against [the CLI inventory](11-cli-spec-research.md)
and [the complete interface plan](13-cli-and-agent-implementation-plan.md).
It does not replace the independent lifecycle target in
[17](17-lifecycle-state-and-authority.md) or its required storage migration in
[18](18-lifecycle-storage-upgrade.md). The existing reducer still has one closing
testament per claim. Exposing its operations is not completion of that migration.

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

`monitor.register` records existing durable graph predicates under an active
claim issued by the authenticated participant. It requires explicit timer,
generation and future logical-time deadline values; no client clock event is
invented. `monitor.get` reads the actual monitor after a fresh quorum barrier.
Release is a stored fact, with its original sequence, and is not by itself proof
that every root succeeded. There is no associated Focal worker or callback
process. A caller's timeout cannot expire the monitor.

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

## Remaining independent work

The complete plan remains the completion checklist. Interface implementation
must not silently mark storage upgrades, independent artifact/testament states,
child-cause authorization, challenge/consult continuation policy, automatic
request-stream rotation, arbitrary schema installation, placement activation,
credential rotation, global deployment journeys, or scale qualification complete.
Each requires its actual authoritative service contract and executable tests.
The release status document records the tests that have run; source availability
alone is not qualification.

In particular, the legacy `GenerateClaim` revision fence addresses the new claim,
not an independently changing parent. A child-cause API must bind its parent
guard into the saved request's actual intent identity and check effective owner
state before staging. Writing `Cause::Claim` in a document alone does neither.
Released or terminal parents cannot be reopened to add completion obligations;
corrective/follow-up work needs valid enclosing cause and immutable lineage.
Current claim relation endpoints are other claims, while artifact inputs can
reference exact artifacts. A challenge against one exact artifact therefore
needs a supported versioned target/acceptance representation; an artifact hash
in a display string, scope key or schema list is not that authority contract.
