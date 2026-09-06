# Lifecycle storage upgrade and decoder transition

Status: storage upgrade contract, 2026-09-06. The V1 checkpoint graph, prepared
inputs, canonical command identity and explicit historical execution boundary
below are implemented, along with surrounding Session format isolation and
output identities. Consensus also has the bounded one-transition floor mechanism;
production Session still registers only V1. The successor decoder, activation and
lifecycle migration remain open. This is the storage
prerequisite for L2–L5 in
[the peer validation contract](16-peer-validation-contract.md), not permission to
change existing serialized types in place.

## 1. Recommended boundary

Keep the present formats and their replay behavior as V1. Add one explicit,
versioned decoder-floor transition, followed by a committed per-group lifecycle
activation. Decode historical state through frozen V1 types; use a distinct
checkpoint and prepared-command format for the evolved lifecycle. Preserve
historical content hashes, request hashes and retained receipts exactly.

The first storage increment should support **one known successor** to the current
floor, with a bounded transition record and an explicit activation fence. It does
not need an extensible migration framework, a new physical writer, or another
state owner. All persistence continues through the existing shared WAL and Ready
contract. Participants still execute their own tools; this upgrade introduces no
job queue, agent launcher or server-authored corrective work.

A new outer snapshot magic alone is insufficient. Existing snapshots embed the
serialized Core, and existing WAL entries replay commands against a reducer.
Changing either the embedded structs or historical reducer behavior would change
what the old bytes mean.

## 2. Actual frozen boundaries

| Boundary | Current representation | Consequence for L2 |
|---|---|---|
| Physical WAL | Postcard `Record`, `RecordKind` ordinals 0–7; `DecoderFloor` is ordinal 6 and `DecoderTransition` is ordinal 7 | Preserve all existing ordinals and frame checks. Older physical scanners reject the appended transition kind. |
| Local decoder promise | `FOCALDF1` plus a 32-byte fingerprint, exactly 40 payload bytes; one original baseline per group | The original record is immutable. One separately encoded transition may extend its requirement; it cannot replace the baseline hash. |
| Local decoder transition | `FOCALDT1`, big-endian `u16` schema 1, predecessor and successor fingerprints; exactly 74 payload bytes, index/term zero | Require an existing matching baseline and at most one transition. Preserve both through every checkpoint rewrite. This does not activate application semantics. |
| Legacy domain entry | `FOCALOP1` plus `PreparedMutation` | The prepared record includes the original authenticated command, base sequence and command hash, not the final rows or final receipt. |
| Managed domain entry | `FOCALMD1` plus `PreparedManagedMutation` | It has the same replay dependency; managed stream admission and the resulting receipt are published by Session. |
| Other Session entries | `FOCALCU1`, `FOCALCU2`, `FOCALCM1`, `FOCALMC1`, `FOCALPL1`, `FOCALMU1`, `FOCALMS1` | Cursor, membership, placement and request-stream bytes remain unchanged unless an explicitly new contract requires otherwise. |
| Core checkpoint | `FOCALCP1`, checksum, then the original Postcard `(1u16, Core)` layout through explicit V1 envelope/State/limits and nested model codecs | All four objects, lifecycles, runs, monitors, legacy receipts and epoch windows use fixed fields/variants. Importing them into a successor still requires explicit historical semantics. |
| Session checkpoints | `FOCALSS1` through `FOCALSS5` | V2 adds cursor state/history; V3 membership; V4 placement; V5 request streams. Each contains a Core checkpoint byte vector. Changing that embedded payload changes the checkpoint contract even if outer fields look unchanged. |
| Exact outcomes | `MutationReceipt`, cursor receipts, `ManagedReceipt`, control receipts, their content hashes | These are durable recovery identities. A migration cannot regenerate or reclassify an old result from today's lifecycle projection. |
| Wire objects and results | `ReadObject` embeds the existing object types; results embed model receipt/outcome types | Modifying a model lifecycle in place also changes old wire responses and client journals. Storage versioning alone does not solve client compatibility. |

Sources: [WAL record types](../../crates/focal-log/src/lib.rs),
[decoder floor](../../crates/focal-consensus/src/decoder.rs),
[Session entry dispatch](../../crates/focal-ledger/src/session.rs),
[managed envelopes](../../crates/focal-ledger/src/managed_session.rs),
[cursor and snapshot envelopes](../../crates/focal-ledger/src/cursor_session.rs),
[Core and checkpoint codec](../../crates/focal-core/src/lib.rs),
[model objects](../../crates/focal-model/src/objects.rs), and
[wire objects](../../crates/focal-wire/src/message.rs).

The current managed descriptor is a concrete fingerprint of managed schema 1,
`FOCALMD1`, `FOCALMU1`, `FOCALMS1`, `FOCALSS5` and their receipt-retirement
contracts. `confirm_decoder` currently permits one exact fingerprint;
`begin_decoder_floor` cannot replace an installed one. Session confirms that
fingerprint before its first recovery drain. The new trusted pair API is separate;
Session does not call it until the real successor decoders exist. These restrictions
are intentional.
[Managed support](../../crates/focal-ledger/src/managed_support.rs),
[Session construction](../../crates/focal-ledger/src/session.rs).

## 3. Freeze historical decoding and execution first

The actual Core checkpoint and Session legacy/managed prepared-entry paths use
[the V1 envelope codec](../../crates/focal-core/src/durable_v1.rs) and
[explicit nested model codecs](../../crates/focal-model/src/durable_v1/mod.rs).
The complete reachable checkpoint graph and all 29 prepared command variants
select historical fields, numeric codes and variant ordinals independently of
live Serde implementations. Legacy and managed domain intent hashing share one
frozen V1 schema/tag/body algorithm. Legacy hashing serializes once into a command
body vector and hashes it directly, eliminating its former second preimage
vector. Managed hashing retains its allocation-free streaming path: a fixed
1 KiB stack buffer batches scalar bytes, while contiguous byte payloads pass
directly to BLAKE3. Borrowed encoding avoids cloning inputs or Core maps;
elementwise decode constructs final collections directly and ignores untrusted
collection-size preallocation hints. Stored hashes remain opaque facts.

Core and prepared-entry schema checks precede nested decoding, and their bodies
require complete consumption. Session metadata retains the tag-specific historical
suffix rules described in section 3.2.
The old `FOCALSS1` and `FOCALSS2` outer snapshot readers also now reject suffixes,
matching versions 3–5; V1 retains its first decoded Core instead of decoding it
twice to establish the missing domain-prefix field. Original writer bytes remain
unchanged. [Original workflow fixtures](../../crates/focal-core/fixtures/durable-v1/README.md)
qualify exact replay results. The broader
[nested checkpoint corpus](../../crates/focal-core/fixtures/durable-v1-nested/README.md)
and [prepared input/hash corpus](../../crates/focal-core/fixtures/durable-v1-inputs/README.md)
cover every current variant and deliberately serialization-valid but inadmissible
values. Those synthetic vectors prove codec/identity preservation, not reducer
acceptance, complete historical execution isolation or successor activation.

Introduce explicit internal V1 durable DTOs for the affected Core, prepared
inputs, objects and outcomes before altering their current definitions. A
versioned codec must select those DTOs from the original magic/schema and require
complete consumption of the input. Do not rely on `serde(default)` to extend
Postcard structs: field order and embedded container boundaries are part of the
old format.

Keep the V1 canonical encoding functions and command ordinals for historical
inputs. The command and manifest algorithms now pin their V1 schemas explicitly.
Do not use a global `SCHEMA_MAJOR` bump as an upgrade protocol: persistence
versions, new authored content and admission must be selected separately.
Existing requirement hashes and artifact/testament references remain
bound to their original content bytes. A new target or validator-definition field
requires a versioned content representation and canonical encoder, rather than
silently adding a field to an old hash contract.
[Canonical encoding](../../crates/focal-model/src/canonical.rs),
[command/result types](../../crates/focal-model/src/command.rs),
[managed identity and receipts](../../crates/focal-model/src/managed.rs).

Historical prepared records execute under their historical rules. The
[execution dispatcher](../../crates/focal-core/src/execution.rs) selects the
prepared schema before invoking the
[owned V1 reducer, validation and graph rules](../../crates/focal-core/src/execution_v1.rs).
The [immutable model semantics](../../crates/focal-model/src/semantics_v1.rs)
pin their behavioral dependencies. Current proposal policy is separate in
[admission](../../crates/focal-core/src/admission.rs). Core replay verifies the
base and input hash, runs the selected reducer and constructs the receipt.
New artifact/testament propagation must have its own execution implementation;
it cannot be applied retroactively to an old `FOCALOP1` or `FOCALMD1` record.
This boundary shares the existing Core and publication machinery, without
duplicating a reducer or adding another state authority. Golden tests compare
final rows, deltas, effects and receipt bytes, not only successful deserialization.
[Legacy apply](../../crates/focal-core/src/lib.rs),
[managed staging/replay](../../crates/focal-core/src/managed.rs),
[reducer](../../crates/focal-core/src/reduce.rs).

An internal current representation may import V1 records, provided the import is
bounded and preserves their exact identities and known facts. The implementation
must choose between versioned records and an explicit current representation with
legacy provenance before freezing V2. Maintaining two independently mutable
lifecycle authorities is not an acceptable conversion strategy.

### 3.1 Concrete implementation sequence

The first three increments below are implemented for Core checkpoints, prepared
domain inputs and historical execution. Section 3.2 also implements surrounding
Session format isolation. The broader historical failure/agentic fixture matrix
remains open. None of these
increments installs a successor floor or enables new lifecycle states.

**Implemented: freeze the nested checkpoint graph.** Keep the shared model codecs in
`focal-model::durable_v1`, with separate collection, object, vocabulary and
receipt modules; retain the Core envelope in `focal-core::durable_v1`. The model
location lets canonical command hashing share the same frozen encoders
without a Core/model dependency cycle. A type alias or a wrapper delegating to
the current type's Serde implementation is not a frozen representation.

| Checkpoint root | Frozen nested V1 representation |
|---|---|
| Four object maps | `StoredObject` content/hash/lifecycle order and all four content/lifecycle records |
| Claim content | Relations and targets, scopes, requirement references, deadlines and typed object references |
| Claim lifecycle | Status history, receipt/fence, and original optional single testament/evidence-set references |
| Validation content | Ordered handler references, evidence-schema hashes, contributors and policy revision |
| Artifact content | Both payload variants, content reference, typed inputs and visibility strings |
| Testament content | Exact ordered artifact references, receipt fence, evidence-set ID, confidence and outcome |
| Evidence sets | Owner/fence, ordered manifest and closed flag |
| Validation runs | Run identity and target, verdict records, handler identity/version and ordered proof references |
| Monitors | All three wait-predicate variants, deadline and retained release facts |
| Identity index | Original object-kind/hash key and object ID value |
| Epoch windows | Epoch/floor fields and admitted epoch set |
| Retained receipts | Request keys, mutation receipts and all twelve `CommandResult` variants |
| Shared leaves | Ledger identity, fixed-width IDs/hashes, counters and numeric vocabulary codes |

Use borrowed serialization views and map/sequence adapters over existing rows.
During decode, consume each frozen row into its final current representation
before inserting it into the final collection. Do not build a whole frozen map
and then allocate a second map to convert it. Move strings, payload vectors and
hashes; do not recompute immutable identities or manufacture missing lifecycle
facts. Preserve numeric vocabulary codes separately from the Serde variant
ordinals used by `RelationTarget`, `ArtifactPayload`, `WaitPredicate` and
`CommandResult`. Integrate these codecs into the actual checkpoint State views
and retain the original byte/restart fixtures as acceptance checks. Map/set
ordering still relies on the original key types' `Ord`: preserve that ordering
for V1, or introduce explicit historical key representations before changing it.
The fixed corpus checks original iteration order as well as individual row bytes.

**Implemented: freeze prepared inputs and canonical identity.** Cover authenticated
legacy/managed inputs, authority context, causes, custody attestations, managed
stream identities/keys, all 29 `Command` variants, and their new-object inputs.
Reuse the frozen content encoders. Preserve the original `focal.command` domain,
schema 1, command code, expected revision and Postcard command body; authority
timestamps and custody refresh remain excluded exactly as before. The fixed
corpus contains all command/result variants, programmatic and agentic
metadata/outcomes, receipt adoption and historical malformed Receipt contracts.
It pins original canonical hashes independently of the frozen encoders. Actual
admitted histories must still qualify historical reducer isolation, beyond these
codec vectors. Session's retained deltas, effects and managed receipts also
require frozen surrounding representations; they are not all nested in the Core
checkpoint.

**Implemented: isolate historical execution.** The existing transition,
validation and graph-propagation rules belong to the explicit internal
`execution_v1` boundary. `Core::apply_recorded`, `Core::stage_recorded`,
managed staging/replay **and `epoch::execute_entry`** select it. The epoch path
is actual committed/follower execution, so changing only the public Core apply
method would leave replay dependent on current semantics. Today's
admission-only Receipt restrictions remain separate from historical execution. Version
selection comes from the decoded entry contract, never a process-wide active
profile. Compare final rows, deltas, effects, hashes and receipts across serial,
epoch and managed replay before adding any successor execution rules.

The implemented routes select the historical implementation or retain a
candidate already prepared under it:

| Route | Implemented version selection |
|---|---|
| Legacy prepare and pending proposal | `stage_recorded` selects V1 and persists that selection with its staged result |
| Direct, tracked and serial-oracle apply | `apply_recorded`, from prepared schema |
| Pending audit and committed Session replay | `epoch::execute_entry`, including planning, scoped workers and serial fallback |
| Managed proposal | `stage_managed_recorded` with `AdmitV1` |
| Managed audit and replay | `stage_managed_recorded` with `Replay(Version)` from the prepared contract |
| Managed committed candidate reuse | Preserve the exact prepared schema, entry hash and audited candidate provenance; never relabel it from a newer active profile |

The boundary owns the existing implementation files through explicit module
paths; it does not forward to mutable default rules. The extracted
`validate_receipt_admission` runs only on new proposal paths. Managed replay
carries an explicit `Replay(Version)` value through staging, while new proposals
select `AdmitV1`. Both families count admission bytes through the frozen input
codec. The historical reducer still performs its original domain checks.

The following behavioral dependencies are pinned:

- Schema `1` in old content admission, generated testament content, emitted
  deltas, prepared construction, public apply/epoch checks and managed
  basis/replay checks. The existing `manifest_hash` header now also uses the
  explicit V1 schema constant.
- The original seven nonterminal and thirteen terminal claim states; the
  `ClaimStatus` to `LifecycleAction` mapping; and verdict severity
  `Pass < Incomplete < Error < Fail`.
- All 29 commands' original revision target selection. In particular, generation
  addresses the new claim, supersession the predecessor, and monitor registration
  the owner. Preserve the original commands with no claim revision target.
- Typed claim relation interpretation for issuer, subject, action, cause and
  dependencies, including its existing ordered iteration.
- Managed stream/key validity: nonzero cluster, ledger, principal and request
  identity, positive generation and ordinal, with slot zero still valid.

These helpers live in the immutable V1 model module; existing convenience
methods delegate to them. Fixed IDs, existing canonical content/specification
contracts, stored hash getters, row access, resource accounting and atomic publication can remain
shared; their identities and ordering are retained contracts. Recovered limits
continue governing old execution. This still leaves the original state/output
types as a separate successor-representation boundary.

Qualify actual admitted histories across direct apply, pending audit, ordered
epochs, forced serial fallback and managed stage/audit/restart, comparing final
rows and exact outputs. Include the historical stronger Receipt requirement
that new admission rejects. The synthetic 58-command codec corpus cannot stand
in for those executable histories.

### 3.2 Surrounding Session formats and output identities

The actual Session readers and writers now use
[explicit historical codecs](../../crates/focal-ledger/src/session_durable_v1.rs).
Their embedded Core bytes use the frozen `FOCALCP1` codec. All five outer snapshot
readers require complete consumption. Versions 3–5 writers borrow cursor/history,
membership, placement and request-stream state, preserving the original bytes
without temporary cloned graphs. The writer preflights the existing eight-MiB
limit and fallibly reserves the final byte vector before encoding directly into
it. The existing conservative Recovery reservation remains held throughout;
this change does not claim tighter owner budget limits or measured scale.

| Snapshot | Original nested root |
|---|---|
| `FOCALSS1` | `SnapshotEnvelope { schema, ledger, raft_index, core }` |
| `FOCALSS2` | `SnapshotEnvelopeV2`: SS1 fields plus `CursorCheckpoint`, `CursorMetadata`, `delta_floor`, `Vec<Delta>` |
| `FOCALSS3` | `SnapshotEnvelopeV3 { state: SnapshotEnvelopeV2, membership: MembershipState }` |
| `FOCALSS4` | `SnapshotEnvelopeV4 { state: SnapshotEnvelopeV3, placement: PlacementState }` |
| `FOCALSS5` | `SnapshotEnvelopeV5 { state: SnapshotEnvelopeV4, requests: RequestStreamsCheckpoint }` |

SS1 recovery retains its original empty-cursor/history initialization at the
recovered Core prefix. Effects are **not** a retained Session snapshot queue:
the historical reducer regenerates them in outcomes, and recovery does not
execute them. They need an output compatibility contract; retained deltas need
both output and snapshot contracts.

| Owner | Implemented explicit historical representation |
|---|---|
| Model outputs | `DeltaId`, `Delta`, eleven `DeltaFact` variants and five `EffectIntent` variants; their nested domain leaves already have V1 codecs |
| Native stream | Consumer IDs/keys, positions/offsets, tokens, filters, resync reasons, modes, records, `CursorCheckpoint`, `CursorCommand` and nine `CursorOperation` variants |
| Ledger cursor metadata | `CursorInput`, `CursorReceipt`, and private `CursorMetadata { receipts, owners }` |
| Ledger membership | `MembershipState`, membership request/receipt/context, and explicit adapters for consensus configuration and four membership-change variants |
| Directory and ledger placement | `PlacementState`, `StoredPlacement`, `PlacementRecord`, placement request, session fence/kind, placement spec/policy/value, durability intent, failure class and region/group/operation IDs |
| Managed registry | `RequestStreamsCheckpoint { activated, slots }`, `StreamSlotData { principal, state, latest, rows }`, stream state/control receipts/outcomes, managed receipts/outcomes/families |
| Managed cursor snapshots | The model cursor-record/token/consumer-key/position/offset/filter/mode/resync snapshot DTOs persisted inside managed receipts |

The managed cursor filter stores a `Vec<ClaimId>`; the native cursor filter uses
a `BTreeSet<ClaimId>`. Both original representations and their collection
semantics are preserved. Model codecs include `Box<T>` and raw `[u8; 32]` leaves.

The seven metadata log formats use their corresponding envelope codecs:
`FOCALCU1` (`LegacyCursorEnvelope`), `FOCALCU2` (`CursorEnvelope`), `FOCALCM1`
(`MaintenanceEnvelope`), `FOCALMC1` (`MembershipContext`), `FOCALPL1`
(`PlacementRecord`), `FOCALMU1` (`ManagedCursorEnvelope`) and `FOCALMS1`
(`RequestStreamEnvelope`). Original-reader captures establish that CU1/CU2/CM1
accepted body suffixes through `postcard::from_bytes`; frozen readers preserve
that historical behavior. MC1/PL1/MU1/MS1 and SS1–SS5 enforce complete consumption
at their existing caller boundaries. Tightening CU1/CU2/CM1 replay would change
the meaning of existing stored bytes, so any stricter contract needs a new tag.
CU1's absent replay floor is still derived from committed stream bounds under
its historical rule.

The data and its three identity paths are frozen together: managed receipt/control
hashes, membership request hashes, and placement-record re-encoding plus
`placement_digest`. Receipt/control hash schema headers explicitly retain 1;
control request IDs remain excluded from their original intent commitment.

The implementation followed this dependency order:

1. Capture original writer bytes for all output/metadata variants and SS1–SS5
   before modifying their serializers. Existing Core `.result` fixtures cover
   one admitted workflow, not every output variant or a Session envelope.
2. Extend model V1 codecs for outputs, managed receipt/control types and cursor
   snapshots; freeze their corresponding hashes.
3. Implement native stream and directory codecs in their owning crates, which
   already depend on the model. Rust's orphan rule forbids placing those foreign
   trait/type implementations in Ledger.
4. Use Ledger-owned explicit membership adapters. Consensus currently has no
   model dependency; do not introduce one just to implement these codecs.
5. Freeze private Ledger rows and envelopes in a child of the existing private
   Session module, preserving access to its included files' private fields.
   Integrate actual readers, writers and hash entrypoints together.
6. Qualify exact recovery, retained receipts, history floors, acknowledgment and
   placement hashes, and rejection before publication. Keep the owner-funded
   recovery reservation throughout decode; construct final owned collections
   directly instead of allocating a second converted graph.

Existing cursor retry, membership migration, placement-fence and managed
domain/cursor/control checkpoint tests supplied real original-writer capture
scaffolds. The [Session corpus](../../crates/focal-ledger/fixtures/durable-session-v1/README.md)
contains 170 fixed data/context files, including actual SS3→SS4→SS5 history,
original SS1/SS2 representations and reader suffix probes. Its original generator
and source/build identities are preserved outside the executable test tree;
ordinary tests cannot regenerate expectations through the replacement writers.
Source boundaries:
[snapshot/cursor envelopes](../../crates/focal-ledger/src/cursor_session.rs),
[managed envelopes](../../crates/focal-ledger/src/managed_session.rs),
[request registry](../../crates/focal-ledger/src/request_streams.rs),
[membership](../../crates/focal-ledger/src/membership_session.rs),
[placement](../../crates/focal-ledger/src/placement_session.rs),
[native cursor types](../../crates/focal-stream/src/cursor.rs), and
[directory placement types](../../crates/focal-directory/src/placement.rs).

The native stream part of step 3 has explicit borrowed V1 encoders and direct
decoders in [focal-stream](../../crates/focal-stream/src/durable_v1.rs). Its
[original-writer corpus](../../crates/focal-stream/fixtures/durable-v1/README.md)
captures all twelve native types and nine operation variants, with 37 data files,
preserved source/compiler/library identities, and clearly identified reader
normalization probes. These adapters are wired into Session envelopes alongside
the [model output/managed codecs](../../crates/focal-model/src/durable_v1/outputs.rs)
and [directory placement codecs](../../crates/focal-directory/src/durable_v1.rs).
The [model output corpus](../../crates/focal-model/fixtures/durable-v1-outputs/README.md)
adds 24 row files and original receipt/control hashes; the
[directory corpus](../../crates/focal-directory/fixtures/durable-v1/README.md)
adds 49 fixed files, including distinct same-typed field sentinels and original
placement hashes. All captures precede their writer replacements or use retained
original libraries with byte equality against the initial capture.

This completes the surrounding Session codec boundary, not L2 or activation.
Directory authority/control/partition formats remain separate contracts. The
successor must still define its state representation, register its actual decoder
pair, and complete activation, wire compatibility and historical import before
accepting any new lifecycle encoding.

## 4. Durable successor floor

The existing consensus decoder module now implements one bounded ordered
transition. Its trusted application API is `confirm_decoder_pair(predecessor,
successor)` followed by `begin_decoder_transition()`. The original
`confirm_decoder(hash)` and `begin_decoder_floor(hash)` remain valid for V1-only
callers. The application must register actual compiled decoders; participant
input cannot select this pair. Production Session still confirms only its existing
managed V1 descriptor. A lifecycle successor fingerprint will be allocated only
after its decoders and execution semantics exist.

The appended `RecordKind::DecoderTransition` is ordinal 7. Its exact payload is
`FOCALDT1`, schema 1 as two big-endian bytes, the original 32-byte predecessor,
and the 32-byte successor, totaling 74 bytes. Record index and term are zero;
the existing logical-log field binds the group. The original `DecoderFloor`
record and its bytes remain unchanged. Older floor-aware binaries reject this
unknown physical kind before application replay.

The implemented local mechanism enforces these requirements:

1. Require the transitioned group's complete compiled pair before any recovery
   output, campaign, step or vote. A bounded fixed set of the two known descriptors
   is sufficient initially. The old single-decoder interface stays
   valid for callers that only support V1. A single successor hash cannot confirm
   transitioned recovery; confirmation must include the exact complete pair.
2. Accept only the known predecessor/successor pair in the correct order. Reject
   unknown, duplicate, conflicting or regressing transitions. A capability
   supplied as an arbitrary client boolean or version string is insufficient.
3. Retain one owned transition intent through WAL admission pressure and caller
   cancellation. Publish the new local support fact only after the existing
   asynchronous WAL receipt proves fsync. Use Completion capacity and the same
   persistence-pending gate as the present floor.
4. Keep the baseline floor and required transition in every checkpoint rewrite.
   Recovery must retain the effective successor requirement even when all older
   Raft entries have been compacted. Bound this history to the one supported
   transition; future successors require a separately designed extension.
5. Distinguish the **required durable floor** from the set of decoders the binary
   implements. A V2-capable process must still read V1 entries and snapshots.
   Existing `decoder_floor_ready(v1)` callers cannot simply remain exact equality
   checks after the effective floor becomes V2; update those call sites using
   the explicit supported transition relationship, never an arbitrary numeric
   comparison between hashes. The consensus readiness predicate now does this.
   Session's separate `managed_support_demanded` exact-hash check must be adapted
   when its actual successor support is integrated; that path remains V1-only.

Confirmation is monotone: a registered single predecessor may be widened once to
its explicit pair, including while the original floor write is retained. Repeated
matching calls preserve pending work; replacement, reversal and a second pair are
refused before mutation. Initial complete-pair confirmation is permitted while
recovered output waits, so it can unlock the decoder-gated replay. After transition,
the original `begin_decoder_floor(predecessor)` remains an idempotent assertion;
passing the successor cannot bypass the transition. The existing finish/poll
methods drive the same retained `WalAppend` receipt for either record.

Primary implementation boundaries are
[`decoder.rs`](../../crates/focal-consensus/src/decoder.rs),
[consensus replay](../../crates/focal-consensus/src/lib.rs),
[Ready persistence](../../crates/focal-consensus/src/persistence.rs),
[checkpoint rewrite](../../crates/focal-consensus/src/checkpoint.rs), and
[WAL control admission](../../crates/focal-log/src/writer.rs).

This floor is an irreversible decoder requirement, not lifecycle activation.
After the first successor promise is durable, that node cannot downgrade even if
the cluster has not activated V2. It may continue using the old application
semantics while other voters upgrade. Expose that distinction in upgrade status.
Requirements are scoped to logical groups, but the physical WAL scanner must
understand every record it encounters. An older binary encountering the appended
record kind therefore refuses the entire shared WAL, including groups that have
not transitioned. Qualification must cover that node-level downgrade boundary
before and after checkpoint rewriting, using an actual preserved older binary.

The [recorded older-binary experiment](../../crates/focal-consensus/fixtures/decoder-transition-old-binary/README.md)
qualifies that physical boundary using a real old `focal demo` data directory.
The old executable accepts the unchanged and V1-floor controls. It refuses a
test-only successor transition before compaction and after same-group or other-
group compaction, twice per branch, with every file unchanged. Original application
snapshot bytes are preserved. The synthetic successor exists only in that test
helper; the evidence does not establish lifecycle-V2 decoding or cluster activation.

The normal [consensus regressions](../../crates/focal-consensus/src/decoder_transition_tests.rs)
also exercise pair-only recovery, participation fencing, actual delayed fsync
under Ordinary pressure, incompatible pending retries, abandoned callers, both
ambiguous persistence cuts, original checkpoint order and malformed histories.

## 5. Quorum activation and membership

Add one replicated Session metadata activation record identifying the successor
contract and its exact predecessor, current configuration index and configuration
identity. It follows durable support from every current voter, including both
sets in a joint configuration. Serialize it against pending membership and
application proposals so an earlier prepared V1 command cannot be relabeled as
V2. Revalidate its configuration precondition at the ordered apply boundary.

The activation index is the application-semantics boundary. Before it, admit and
replay V1; after it, new lifecycle mutations use the successor contract. A node
having fsynced its local successor floor alone does not grant activation.
Activation and its exact retry identity must survive snapshots and leader changes.

Adapt the existing authenticated, configuration-scoped support exchange rather
than introducing another actor or physical worker. The present `ManagedSupport`
request describes managed V1 and its cache stores only node IDs for that fixed
contract. A successor exchange must explicitly identify which descriptor was
promised. Preserve the old request meaning; decide whether to append a narrow
support request or add a separately versioned exchange. A new V2 promise must not
be manufactured from an old managed fact.

After committed activation, existing voters need not all answer fresh capability
probes following a leader restart: their durable promises and guarded membership
changes establish the invariant. The leader still needs its local successor
floor and normal current-term quorum authority. Every later learner admission or
promotion must require the active successor capability, not merely managed V1.
Prospective facts retain the existing exact identity/bootstrap checks, and
promotion still requires real catch-up.

An already-present learner may not have joined the activation barrier. Detect its
first incoming V2 entry or snapshot **before Raft persists the message**, establish
its actual local successor floor asynchronously, and report non-success until
that floor is durable. Existing per-frame snapshot failure feedback must release
Raft's snapshot wait and allow retransmission. Term/index/incarnation fences must
keep a late failure from affecting a replacement snapshot.
[Managed admission and learner floor](../../crates/focal-ledger/src/managed_support.rs),
[Session membership](../../crates/focal-ledger/src/membership_session.rs),
[transport feedback](../../crates/focal-node/src/snapshot_feedback.rs).

## 6. V2 entries, state and checkpoints

Once L1 fixes the lifecycle definitions, add distinct prepared-entry and Core
checkpoint versions. For example, `FOCALOP2`, `FOCALMD2`, `FOCALCP2` and
`FOCALSS6` are possible names, **not allocated formats in this document**. The
actual descriptor must enumerate the implemented versions and their historical
support before any support fact is advertised.

The successor Session checkpoint retains the complete existing cursor state,
delta tail, membership receipt, placement state and managed request registry,
plus the activation fence and V2 Core. Do not discard the request registry merely
because object storage changed. Untouched groups continue writing their existing
formats; a node binary upgrade alone must not rewrite their genesis or opt them
into the new contract.

Maintain one atomic publication path for a mutation's own lifecycle facts,
dependent transitions, receipt, projected graph rows and deltas. Extend the
existing staged row/access tracking and memory accounting to every new field or
table. Keep failure cleanup before publication, and reserve bounded decode,
conversion, result and graph workspace before retaining allocations. Followers
and recovery use Completion capacity. Do not materialize a second whole Core
without accounting for both live copies.
[Ordered epoch publication](../../crates/focal-ledger/src/apply_epoch.rs),
[managed publication](../../crates/focal-ledger/src/managed_session.rs),
[Core row tracking](../../crates/focal-core/src/access.rs),
[graph projection](../../crates/focal-graph/src/projection.rs).

Metadata activation must not silently change the meaning of an already-published
`SessionSeq`/read token. If conversion introduces observable lifecycle facts,
either publish those facts at an explicit domain sequence or retain the old
observational projection until a versioned domain mutation advances it. Existing
pinned graph views stay immutable. Choose this rule before implementing the
activation-to-conversion handoff.

Old client requests remain exact requests. A retained V1 receipt wins before new
admission rules are considered. An unknown request must not acquire a different
command hash merely because it is retried after activation. Keep the historical
identity algorithm for old input shapes; explicitly gate any new shape/result
contract at the wire boundary. Changing model aliases used by old `ReadObject`,
`MutationReply`, managed receipts or private client journals is not transparent.

### 6.1 Concrete successor owner integration

Independent native validation ownership is implemented. The next owner increment
must close the remaining gaps: current Core overlays require `Clone + Serialize`,
and `ClaimState::generate` can create correction lineage before the only complete
ancestry check in `SuccessionPlan`. Copying those native objects into a parallel
mutable map would not fix the owner. Use this dependency order:

1. **Implemented: retain definitions and evaluations independently.** An immutable
   [Declaration](../../crates/focal-model/src/lifecycle/validation_definition.rs)
   owns slot text and ordered handler policies once. `Declaration::prepare`
   validates all input without allocation and reports checked requested row/buffer
   bytes; `DeclarationPlan::build` reserves those buffers fallibly.
   `Declaration::retained_bytes` measures actual capacities. The future owner must
   reserve before construction and reconcile actual capacity and allocator overhead
   before retaining the row; these methods alone are not budget integration.
   [EvaluationState](../../crates/focal-model/src/lifecycle/validation.rs) contains
   no references. `bind` and `into_state` create and detach a temporary checked
   view without allocation or policy copies. A private semantic stamp includes all
   actual immutable declaration fields, independently of supplied content identity.
   Acceptance registration, result capabilities and sealed audit membership also
   check that stamp. Exact reconstructed definitions are accepted; changed policies
   under the same supplied binding are rejected. This guard has no wire/content
   identity or successor durable representation.
2. **Create the sole successor owner inside Core.** Keep existing V1 Core and its
   execution immutable. Add successor row ownership, effective-prefix reads and
   bounded staging in Core, initially unreachable from Session. Session's eventual
   activation selects exactly one Core version; it must never run V1 and successor
   owners as independent mutable authorities for the same ledger. Import moves
   old rows into explicitly historical representations within that selected owner.
   Native lifecycle state replaces a row's historical representation only through
   its defined versioned transition; it does not mirror it beside an editable old
   object. Retained V1 receipts and their exact outcomes remain historical facts.
3. **Make creation checks mandatory.** A shared creation plan owns proposed
   definitions and borrows the sole owner's complete effective lookup, including
   earlier pending writes. It checks new-ID absence and uniqueness, resolves every
   Cause/Supersedes/Amends endpoint, validates compatible identities, and traverses
   every proposed component with one bounded iterative DFS. Missing endpoints do
   not mean roots. An ancestor cannot have been created after its referring claim;
   equal creation positions are permitted only for acyclic relations within the
   same atomic creation batch. Preserve stricter published-predecessor succession
   chronology. Pin positive reads and absence observations, and recheck them at
   publication. Existing public constructors must consume this private proof;
   adding an optional checked constructor while leaving a bypass is insufficient.
4. **Publish child ownership and control consequences together.** Creation installs
   a child and its parent's ownership registration in the same candidate. Later
   cancellation derives the complete owned closure from stored registries and
   evaluation fences from complete acceptance registration, including pending
   rows. It does not impersonate a child's issuer. Already-terminal children keep
   their original fact; informational relations and unrelated consults do not
   become ownership edges. Ordinary blocking validation failure preserves eligible
   begun checks, while explicit control/adoption fences old authority. Scope
   release waits for every owned obligation and occurs once.
5. **Reuse the existing transaction discipline.** Extend tracked row/absence and
   complete-set observations, bounded row patches, candidate audit, graph
   projection and exact request outcomes. Precharge changed rows, graph traversal,
   histories and output before retaining them. Every fallible check precedes joint
   publication. A stale positive read, newly conflicting pending ID or exhausted
   allowance leaves every object, registry, graph and result unchanged. Do not
   obtain this property by cloning the complete Core or creating another service.

Owner-level tests must cover missing direct/transitive creation endpoints, cycles
in a disconnected batch component, valid same-batch ancestry, duplicate/existing
IDs, impossible historical chronology, stale ancestry and new pending-ID conflicts.
They must also order child/evaluation creation against cancellation, preserve
terminal descendants and unrelated peers, distinguish result-before-cancel from
cancel-before-result, fence old adoption receipts, and preserve authorized late
begun results after ordinary required-check failure. Capacity refusal must show
no partial child registration, fencing, scope release or request outcome. A
successful candidate advances one prefix containing all derived facts. These are
actual Core owner tests; manually composing model proof tokens is not equivalent.

### 6.2 Concrete RAM owner and publication seam

Use the existing `Core` container with a default state parameter, `Core<S = State>`,
when introducing the native owner. Current V1 constructors, methods and codecs
remain specialized to the default state. A separate `Core<NativeState>` constructor
starts empty and exposes no arbitrary insertion of caller-constructed lifecycle
rows. This is an implementation plan, not an installed production version. It
avoids a mutable native sidecar and keeps native state outside the V1 codec at
compile time. Later Session activation selects one owner; it must not retain both
as writable representations of the same ledger.

Reuse [RangeStore](../../crates/focal-memory/src/range.rs) for native prepared
versions. It already checks process-local owner identity and exact base-root
provenance, supports unpublished predecessor chains, and publishes by an
allocation-free root replacement. Native claims and their transaction outcomes
must inhabit the same canonical range so publication cannot expose only one side.
Acceptance, scope and child registries stay within their authoritative rows.
Do not feed native values into the old `Backing<V: Clone + Serialize>` or use
V1 serialization to invent a native candidate identity.

**Implemented storage prerequisite:** `prepare_batch_with` and
`prepare_after_with` accept an explicit fallible copier for retained values in
touched pages. Newly supplied changes move directly into prepared pages. Native
rows need not implement infallible `Clone`. Preparation reserves each page's
complete charge before invoking the copier. The row's heap estimate must
conservatively cover copied key/value capacities and allocator overhead;
changed-row construction still needs its own earlier admission reservation.
`PreparedRange::entries` provides a borrowed, ordered view of the entire candidate.
`SnapshotLease::project_next` permits non-`Clone` values while preserving lease
checks and preventing a borrowed row from escaping. Pinned reads remain unchanged,
and foreign or stale candidates cannot publish. The existing shared immutable
page/root lifetime stays inside storage; this extension adds no `Arc` site or
per-object shared policy wrapper. Native Core integration remains open.

The alternative Arena/StableIndex composition is not currently sufficient for
this transaction: Arena has no prepared multi-slot publication, and StableIndex
allocates tree nodes during insertion. Combining their present mutation APIs would
allow publication to fail after earlier rows changed. RangeStore provides the
required existing publication seam, with its documented O(number of pages)
directory-copy cost. Address that directory cost during scale work; this choice
does not qualify Meta-scale throughput.

The first native Core transaction must include root, owned-child and successor
creation plus cancellation of the actual owned closure. Stage parent registration
and new children in the same candidate. Assign creation positions inside Core,
resolve all lineage from its actual effective root, and retain outcomes with the
changed rows. Derived child cancellation uses checked ownership authority rather
than pretending to be each child's issuer. Preserve prior terminal cuts and
unrelated informational peers. Evaluation creation must subsequently extend this
same registry and cancellation path with its authority fences; it cannot be added
through an unregistered parallel map.

Use explicit native row-copy plans to report and reserve dynamic storage before
copying changed claims or retained entries in touched pages. Reserve traversal,
candidate, history and outcome space before construction. Ordinary admission must
leave Completion capacity for committed terminal cleanup. Storage should retain
the existing budget permits with candidate pages and release them on rejection.
Count limits and an unowned byte allowance are not substitutes for those permits.
Test these properties through actual Core preparation, effective reads, candidate
audit and publication before assigning any successor wire or durable tags.

## 7. Import facts without inventing history

The upgrade may preserve and label historical evidence; it cannot fill missing
lifecycle events with assumed success. In particular:

- An artifact's old creation and custody revision do not establish claimant
  receipt or validation. Preserve the distinction between standalone registration,
  insertion into an open evidence set and inclusion in a closed testament.
- Historical `ArtifactAttached` deltas also represent evidence-set insertion or
  standalone registration. They cannot be reinterpreted as the new testament
  attachment event. A real closed manifest can establish its own exact reference;
  it cannot create an unrecorded earlier receipt event.
- Existing testament creation and acknowledgment sequences are real facts. New
  posting/evaluation histories that were never recorded need explicit legacy
  provenance or absence, not fabricated timestamps or transitions.
- Existing validation runs target their recorded claim/testament hashes. A
  schema match does not retroactively identify one artifact when several match.
- Retained refusal, cursor, domain, seal and control outcomes keep their original
  bytes and hashes. Retirement floors retain their original scope and meaning.

Document 17 fixes the semantic result-evidence roles and single-artifact target
rules. Their exact V2 wire/storage representation, legacy-provenance encoding,
and executable conformance remain unfinished. Freeze and qualify those concrete
representations before declaring the new decoder complete.

## 8. Required qualification

Use immutable older binaries as well as frozen DTO fixtures. At minimum:

1. Recover V1 checkpoints 1–5 and uncheckpointed legacy/managed mixed histories;
   compare normalized state, content hashes, deltas, effects and exact receipts.
   A decode-only upgrade must not change files or write a successor promise.
2. Exercise transition admission pressure, fsync failure, cancellation and crash
   cuts before enqueue, after enqueue, after durability and before reply. No new
   support is visible before durability; unresolved writes retain owned state.
3. Run the actual previous managed-capable binary against a floor-only successor
   WAL, before activation or any V2 domain mutation. Require a typed refusal,
   no panic, and no changed file hashes. Repeat after checkpoint compaction.
   Run the new binary against both fixtures and verify successful recovery.
4. Activate a real multi-voter group with every current voter promised. Reject
   missing, stale-config, wrong-node and forged-descriptor facts; cover joint
   configuration and pending membership races.
5. Restart an activated leader with one voter offline and commit a new V2
   mutation with the remaining quorum. Do not require a new all-voter probe round.
6. Test an existing learner's first V2 append and first V2 snapshot, including
   initial floor-pending rejection and actual snapshot retransmission. Test new
   learner admission, promotion, delayed feedback and exact catch-up.
7. Crash before/after activation, during checkpoint rewrite and around a mixed
   V1/V2 suffix. Verify one semantics boundary, sequence continuity, unchanged
   cursor/managed floors and no prematurely acknowledged mutation.
8. Retry saved legacy and managed client operations across activation and
   restart. Verify exact known outcomes, hash conflicts and retirement behavior.
   Exercise older clients against supported old read/result shapes.
9. Under memory pressure, reject unsafe staging before publication and retain
   Completion progress. Verify graph/core/receipt prefixes together and that
   dropping failed candidates releases their allowances.
10. Qualify independent lifecycle races from doc16: open versus closed historical
    evidence, absent artifact receipt, opposite validation completion order,
    optional failure, late verdicts, receipt adoption and terminal parents.

The existing managed-floor tests are useful scaffolding, not proof of this new
transition. Exact successor bytes, fixtures, old-binary identity and measured
qualification results belong in the implementation evidence when delivered.

## 9. Read-only validation context can ship separately

Existing operations can compose a coherent observation without a new wire
variant or durable schema:

1. Start `ReadQuery::ValidationResults` with `Linearizable`; retain its token and
   immutable requirement. This obtains the actual owner's quorum read boundary.
2. Read the owning Claim and its current closing Testament, when present, with
   `Objects` and `Exact(token)`. The current slice returns that testament's exact
   manifest references; artifact descriptors and payloads require separate reads.
   Paginate validation records with the same token and returned position. Check
   requested IDs/families, ledger, route and returned prefix.
3. Bound total pages, objects, evidence references and bytes. An absent requested
   validation returns `NotFound`; an absent referenced parent or closing testament
   is an inconsistent response and returns `InvalidResponse`. A missing closing
   testament is represented as absent when the Claim has no such reference.
   Unsupported target selection remains an explicit limitation, with no inferred
   target or fabricated empty evidence. A future extended context must define its
   own bounded artifact reads and partial-result contract before exposing them.
4. On `SnapshotExpired`, fail that context. **Do not automatically restart at a
   fresh prefix or mix fresh results with the expired view.** A caller may
   explicitly request a new context and observe its new token. Route/owner
   changes similarly cannot transfer a local pinned view silently.

The current context can show the requirement, run, assigned evaluator, handler,
receipt fence, target hash and current closing manifest at one prefix. It cannot grant
an execution lease, claim a missing artifact/testament lifecycle event, or assert
that a future peer verdict will still be authorized. Authoritative mutation
admission must recheck all fences against effective committed/pending state.

Open/historical increment evidence sets are not directly a `ReadObject`. Current
objects do not always establish a unique immutable evaluation target. Report that
limit rather than selecting an arbitrary artifact with the right schema. Full
single-target evaluation readiness remains part of L4/L5.
[Existing read selectors](../../crates/focal-wire/src/message.rs),
[pinned read ownership](../../crates/focal-node/src/reads.rs),
[bounded validation pages](../../crates/focal-node/src/validation_reads.rs).
