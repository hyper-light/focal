# Lifecycle storage upgrade and decoder transition

Status: storage upgrade contract, 2026-09-06. The V1 checkpoint graph, prepared
inputs, canonical command identity and explicit historical execution boundary
below are implemented, along with surrounding Session format isolation and
output identities. Consensus also has the bounded one-transition floor mechanism;
production Session still registers only V1. Typed `Core<NativeState>` transactions
now retain complete definitions, post claims, begin Admission evaluations and
fence evaluations during control changes over custom RAM storage (§6.2).
`NativeOwner` also integrates bounded Admission RAM and record-slot completion
entitlements (§6.7), with indexed grant ownership and cohort checks (§6.8),
without a native durable codec or Session entrypoint. The successor decoder, activation and
lifecycle migration remain open. Native audit inspection and claimant
result-testament generation/posting are implemented in RAM (§6.10), with
ordinary admission and no guaranteed whole-claim closure reservation. Receipt
adoption (§6.11), explicit terminal-owner scope release, and native monitor
registration/rebinding/cancellation, direct root subscriptions, fixed-point
settlement and exact monitor timers (§6.12) also use that RAM owner. Work funding
includes repeated monitor revisions. Posted Required Admission now shares that
indexed graph protection and reserves its complete failure consequences (§6.7).
Receipt acquisition and adoption also fund mandatory respondent diagnostics,
authored response closure and posting across the remaining cycles, including
current-receipt Generated posting history on owner reconstruction (§6.7).
Borrowed declaration/acceptance views and checked input construction plans now
supply model prerequisites for a successor input codec (§6.13). AuthoredV1 now
retains complete claim content, validation instructions/provenance and atomic
creation results in partitioned RAM rows. A dormant full-body input encoder and
bounded structural inspector exist ([21](21-native-input-format.md)). Complete
typed construction and managed owner admission of borrowed plans now exist;
authenticated transport integration and durable native activation remain open. The live V1
service does not yet expose those native bodies or lifecycles.
Remaining control/funding gates, whole-claim RAM/disk promises and live activation
remain open. This is the storage
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
admission-only Receipt and respondent-report restrictions remain separate from historical execution. Version
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
`admission::validate_admission` runs only on new proposal paths. It rejects new
runtime-generated failure testimony and requires actual diagnostic evidence for
new non-Complete respondent reports. The original `FailTestamentGeneration`
execution remains available to historical direct, epoch and managed replay;
retained exact retries resolve before new-admission policy. Managed replay
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

Independent native validation ownership and initial native Core transactions
are implemented. V1 overlays retain their original `Clone + Serialize` contract;
the specialized native owner uses fallible row copies and the existing custom
RAM store instead. Production native claim creation now requires the complete creation
plan. The implementation and remaining dependency order are:

1. **Implemented model values: independently owned definitions and evaluations.** An immutable
   [Declaration](../../crates/focal-model/src/lifecycle/validation_definition.rs)
   owns slot text and ordered handler policies once. `Declaration::prepare`
   validates all input without allocation and reports checked requested row/buffer
   bytes; `DeclarationPlan::build` reserves those buffers fallibly.
   Its borrowed `program()` and `deadline()` views expose the complete authored
   policy. `DeclarationPlan::intent_fingerprint()` reuses the checked private
   stamp and produces the built declaration's existing native request identity
   before any owned buffer is constructed; it does not define a wire encoding.
   `Declaration::retained_bytes` measures actual capacities. Its copy APIs also
   report requested/actual dynamic bytes and allocation counts. Core charges the
   owned definitions and retained neighbor copies; future ingress must reserve
   before decoding/construction and reconcile capacity and allocator overhead.
   [EvaluationState](../../crates/focal-model/src/lifecycle/validation.rs) contains
   no references. `bind` and `into_state` create and detach a temporary checked
   view without allocation or policy copies. A private semantic stamp includes all
   actual immutable declaration fields, independently of supplied content identity.
   Acceptance registration, result capabilities and sealed audit membership also
   check that stamp. Exact reconstructed definitions are accepted; changed policies
   under the same supplied binding are rejected. This guard has no wire/content
   identity or successor durable representation.
   Respondent reports now use the same owned construction discipline in
   [evidence_report](../../crates/focal-model/src/lifecycle/evidence_report.rs).
   `Response::prepare_close` validates explicit summary, confidence, outcome,
   exact work bindings and separately verified error references before allocation.
   `ClosePreparation::build` reserves and copies bounded buffers once;
   `retained_bytes` measures the resulting capacities. No production `Clone` or
   per-response `Arc` is required. The owner must publish the response and every
   work attachment atomically, and retain any closing incident with its claim
   revision. Non-Complete reports remain eligible even with no requested work
   slots produced. A closing incident does not terminalize or fabricate testimony.

   The successor durable schema must encode summary, confidence, all six reported
   outcomes, exact work-slot bindings, separately ordered diagnostic references,
   respondent/receipt/cycle identity and incident cuts. Include every immutable
   report field in canonical identity. Import V1 reports without inferring missing
   slots, retroactively reclassifying artifacts or replacing historical hashes.
   Test report mutation under a reused external binding, missing-output failure
   reports, error-only reports, requester receipt, owned buffer pressure and exact
   history across restart. Current native semantic stamps are in-memory guards,
   not the durable encoding. Budget completion evidence when admitting work;
   domain manifest headroom alone does not reserve node memory or disk capacity.
2. **Implemented owner: creation, Admission, respondent evidence and control.**
   `Core<NativeState>` owns claims and compact registration sets, full immutable
   declarations, independent evaluations, artifacts, responses, accepted results,
   metadata, successful outcomes and typed events in one `RangeStore`;
   §§6.2–6.3 describe its preparation/publication API.
   Existing V1 Core and execution remain separate specializations. Native
   effective-prefix reads and bounded staging are unreachable from Session.
   Session's eventual activation selects exactly one Core version; it must never
   run V1 and successor
   owners as independent mutable authorities for the same ledger. Import moves
   old rows into explicitly historical representations within that selected owner.
   Native lifecycle state replaces a row's historical representation only through
   its defined versioned transition; it does not mirror it beside an editable old
   object. Retained V1 receipts and their exact outcomes remain historical facts.
3. **Implemented: mandatory creation checks.** The shared creation plan owns proposed
   definitions and borrows the sole owner's complete effective lookup, including
   earlier pending writes. It checks new-ID absence and uniqueness, resolves every
   Cause/Supersedes/Amends endpoint, validates compatible identities, and traverses
   every proposed component with one bounded iterative DFS. Missing endpoints do
   not mean roots. An ancestor cannot have been created after its referring claim;
   equal creation positions are permitted only for acyclic relations within the
   same atomic creation batch. Preserve stricter published-predecessor succession
   chronology. Pin positive reads and absence observations, and recheck them at
   publication through exact immutable root provenance. Unchecked `generate` and
   `generate_child` entrypoints are test-only; the internal constructor is not a
   public production bypass.
4. **Implemented owned closure and cancellation/supersession fences.** Creation installs
   a child and its parent's ownership registration in the same candidate. Native
   cancellation derives the complete owned closure from stored registries,
   including earlier pending rows. Root-issuer authority yields private child
   cancellation tokens. Already-terminal children keep their original cuts and
   seals, while traversal continues through them; informational relations and
   unrelated consults do not become ownership edges. This command does not release
   scopes. Cancellation and supersession resolve every evaluation through the
   stored registration set and publish applicable authority fences in the same
   candidate. Terminal evaluation facts remain unchanged. Admission reporting and
   respondent evidence now use that boundary (§6.3); later sections add the other
   evaluation families and receipt adoption (§§6.7, 6.9 and 6.11). Ordinary
   blocking validation failure
   preserves eligible begun Admission checks. Scope release still waits for every
   owned obligation and occurs once.
5. **Extend the transaction discipline to the remaining families.** Extend tracked
   row/absence and complete-set observations, bounded row patches, candidate audit, graph
   projection and exact request outcomes. Precharge changed rows, graph traversal,
   histories and output before retaining them. Every fallible check precedes joint
   publication. A stale positive read, newly conflicting pending ID or exhausted
   allowance leaves every object, registry, graph and result unchanged. Do not
   obtain this property by cloning the complete Core or creating another service.

The [native owner test source](../../crates/focal-core/src/native/tests.rs) exercises
the owner's creation/lineage, pending closure, exact retries, publication,
memory pressure and pinned-read cases. Qualification results belong in
[implementation status](09-implementation-status.md); source presence is not a
passing-test claim. Complete owner-level qualification must cover missing
direct/transitive creation endpoints, cycles
in a disconnected batch component, valid same-batch ancestry, duplicate/existing
IDs, impossible historical chronology, stale ancestry and new pending-ID conflicts.
Owner-level qualification must also order evaluation creation against
cancellation and preserve terminal descendants and unrelated peers. The remaining
evaluation-family transactions must retain Admission's distinction between
result-before-cancel and cancel-before-result, fence old adoption receipts, and
preserve authorized late begun results after ordinary required-check failure. Capacity refusal must show
no partial child registration, fencing, scope release or request outcome. A
successful candidate advances one prefix containing all derived facts. These are
actual Core owner tests; manually composing model proof tokens is not equivalent.

### 6.2 Concrete RAM owner and publication seam

**Owner-seam milestone.** This section records the typed transaction and managed
queue boundary before completion funding. Section 6.7 records the subsequent
integrated Admission contract; descriptions of unfunded preparation below apply
to the low-level Core path or that earlier milestone.

**Implemented typed owner transactions; production activation remains open.**
[Core](../../crates/focal-core/src/lib.rs) now has the sealed state parameter
`Core<S = State>`. V1 constructors, execution and codecs remain specialized to the
default state. [Core<NativeState>](../../crates/focal-core/src/native.rs) starts
empty through `new_native(ledger, range, limits, budget)` and exposes no arbitrary
row insertion. It is an in-process owner with no Serde implementation, wire tag,
WAL writer, recovery decoder or Session/CLI/MCP entrypoint. Later activation must
select one writable owner for a ledger; it cannot maintain an editable V1 mirror
alongside native truth.

The [managed `NativeOwner`](../../crates/focal-core/src/native/owner.rs) now takes
exclusive ownership of that Core and a bounded, precharged pending queue. It
returns process-local candidate tickets and borrowed views, not owned speculative
roots or a mutable Core. This prevents callers from forking its retained chain.
It performs no log I/O. The initial managed queue used the Core's original
allocation source; §6.7 adds its completion book and funded report loans.

The owner uses one [RangeStore](../../crates/focal-memory/src/range.rs) with keys
for metadata, claims, validation definitions, evaluations, artifact descriptors
and content identities, work/diagnostic rows, cycle membership and slot indexes,
responses, accepted results, request outcomes and `(sequence, ordinal)` events. A candidate's rows, registry changes, counts, outcome
and events share one immutable prepared root. Each full immutable `Declaration`
owns its ordered handler/policy buffers once. `AcceptancePolicy` keeps the complete
declared-check manifest and semantic stamps, while a compact
[RegistrationSet](../../crates/focal-model/src/lifecycle/registration.rs) beside
the claim records actual evaluation membership without duplicating definitions.
Graph declarations, scope roots and owned-child registries remain claim-owned.
There is no second mutable authority for relationships or ancestors.

A declared check is not an evaluation run. Independent `EvaluationState` rows
are keyed by claim, validation ID, target identity and generation, and retain the
exact target content, revision and receipt binding. Temporary evaluation views
bind to their actual retained definition without copying policy buffers. Create
retains every declared check, including Observe checks; only eligible target
materialization creates an evaluation. Admission, Increment, pure Receipt and
WholeWork have native owner materialization paths. Increment derives from actual
work submission; Receipt and WholeWork derive from explicit claimant observation
of an actual response. WholeWork Ready targets use the exact attached source or
MissingSlot; §6.9 now supplies funded non-Receipt WholeWork entry and reporting.

Responses may be received in a different order from their authored cycles. The
first received response under the current entitlement advances the claim's
pending phase; it does not acknowledge an earlier still-Posted response. All
cohort generations and attachment bindings remain pinned to their actual source.

| Native API | Current contract |
|---|---|
| `NativeOwner::new(core)` | Transfer the Core after bounded queue admission; refusal returns the original Core in `NativeOwnerInitError`; assign a fresh process-local owner incarnation and import no detached candidates |
| `NativeOwner::prepare(context, input, evidence)` | Require exclusive access, stage against its entire retained chain and return a ticket; an exact pending retry returns its original ticket, while a committed retry returns no candidate |
| `NativeOwner::publish_after_durable(candidate)` | Publish only the head ticket without allocation; refusal retains the exact ticket/root and later candidates stay ordered; the external log owner must establish durability first |
| `NativeOwner::discard_from(candidate)` / `discard_all()` | Drop a known suffix tail first; foreign/stale tickets cannot mutate the chain, and an unresolved append or client timeout is not permission to discard |
| `NativeOwner::committed()` / `effective()` / `candidate(ticket)` | Return borrowed fixed-prefix `NativeView` projections with no root/candidate/source escape; `pin` issues committed-only leases |
| `prepare_native(context, input, pending)` | Authenticate the request principal, validate the ordered candidate chain and trusted monotonic logical time, then resolve against the actual effective root |
| `prepare_native_evidenced(context, input, pending, evidence)` | A fresh Admission report, work output or diagnostic requires an actual request-bound `VerifiedNativeArtifact`; an exact retained retry resolves before requiring another custody token |
| `prepare_native_in(source, context, input, pending, evidence)` | Use the owner's budget or a descendant source for both preparation and retained pages; preserve operation-derived lanes, ancestry checks and exact retries without granting a per-evaluation entitlement |
| `NativeCommand::Create { claims, declarations }` | Require revision one, new IDs and the exact full declaration cohort for each acceptance manifest; assign creation positions and atomically retain claims, definitions and checked parent/predecessor consequences |
| `NativeCommand::Post { expected }` | Resolve all retained definitions, perform issuer-authorized posting and atomically register Ready evaluations for every Admission declaration |
| `NativeCommand::BeginAdmission { claim, key, expected }` | Resolve exact retained authority/readiness and begin as the evaluator; NativeOwner funds a Posted Required evaluation's complete indexed failure component and protects its topology before accepting responsibility |
| `NativeCommand::ReportAdmission { claim, key, expected, report, artifact }` | Retain actual proof/diagnostic and Accepted at ordinal 2; an exact Posted → PostFailed proof publishes complete graph/monitor/cohort consequences under the held failure allowance, while other reports preserve independent audit semantics |
| `NativeCommand::BeginIncrement { claim, key, expected }` | Resolve the registered exact output/receipt/cycle and live evaluator authority; the managed owner funds the bounded report chain before accepting Begun |
| `NativeCommand::ReportIncrement { claim, key, expected, report, artifact }` | Require the actual begun attempt, typed provenance, inherited target visibility and local custody; retain result/evaluation/history without changing claim or work acceptance |
| `NativeCommand::SealIncrementTargets { claim }` | Let the claimant seal complete stored Increment membership; preserve registered checks, respondent diagnostics and the claim's attained phase |
| `NativeCommand::GenerateResultTestament { claim, id }` | Require the original claimant, exact claim binding and complete actual sealed audit; atomically retain one Generated result bundle, its per-claim index and original result publication witnesses |
| `NativeCommand::PostResultTestament { expected }` | Require the claimant and exact Generated bundle; advance only the bundle to Posted, preserving frozen content after later parent graph progress |
| `NativeCommand::AcquireReceipt { expected, receipt }` | Require the actual subject, Posted claim, passing Required Admission and complete checked graph/start predicates; assign epoch one and atomically retain unique receipt allocation, Received, acquisition cut, history and outcome without testimony |
| `NativeCommand::SubmitWork { claim, slot, artifact }` | Require the current respondent, exact receipt/cycle/output provenance, a declared vacant slot and local custody; atomically retain Generated work, cycle membership and every declared Ready Increment evaluation |
| `NativeCommand::SubmitDiagnostic { claim, reason, artifact }` | Require exact respondent/receipt/cycle diagnostic provenance, kind `error`, schema and local custody; retain the independently inspectable diagnostic in the complete cycle index |
| `NativeCommand::FailWorkProduction { claim, slot, diagnostic }` | Reuse the actual current respondent Production diagnostic as a GenerationFailed work binding in a vacant declared slot; retain cycle membership without invented output bytes |
| `NativeCommand::RejectWork { claim, expected, reason, artifact }` | Require claimant structure/metadata rejection with locally verified error evidence bound to the exact original output/receipt/cycle; retain ReceiptFailed independently of claim status and never attach that failure |
| `NativeCommand::ReceiveWork { claim, expected }` | Let the claimant observe an indexed unattached artifact, including after claim terminalization; preserve the claim and the artifact's original receipt/cycle |
| `NativeCommand::CloseResponse { claim, response, report }` | Require an explicit authored outcome and complete indexed evidence; freeze failed-work references separately and atomically retain Generated response, only attachable output rows, cycle closure, claim history and request outcome |
| `NativeCommand::PostResponse { claim, expected }` | Let the respondent post the exact Generated response under its valid entitlement; do not imply claimant receipt |
| `NativeCommand::ReceiveResponse { claim, expected }` | Record claimant receipt under the matching fence; for an open claim materialize every pure Receipt check and its eligible artifact-free Pass, retaining expired checks Ready; terminal/local-complete claim receipt is audit-only |
| `NativeCommand::Cancel { expected }` | Check the root issuer and revision, traverse the complete stored owned tree through terminal descendants, and publish nonterminal claim cancellations plus evaluation authority fences; no implicit scope release |
| `NativeCommand::ReleaseScope { expected }` | Require the issuer, exact terminal owner and every owned child already released; refuse active monitors, precharge the checked scope transition, and atomically publish OwnerReleased before indexed graph consequences while preserving original cuts and eligible begun evaluations |
| `NativeCommand::RegisterMonitor { expected, receipt, id, roots, deadline }` | Require the claimant, exact claim/receipt and unused monitor identity; atomically record typed roots, original finite deadline, direct root subscriptions and checked consequences |
| `NativeCommand::RebindMonitor { expected, receipt, id, predecessor, successor }` | Require exact owner/receipt/endpoints and compatible actual Supersedes lineage; retain original registration/deadline while moving only the named monitor roots and subscriptions |
| `NativeCommand::CancelMonitor { expected, receipt, id }` | Let the claimant dispose of a terminal owner's active wait while retaining original failure and separate cancellation cuts; do not infer successful root or owner release |
| `NativeOwner::prepare_monitor_deadline(input, logical_time)` | Trusted exact monitor timer; deduplicate its disjoint key, resolve actual allocation/scope, apply canonical SCC precedence and checked release/expiry, retaining all index changes and exact expiry fences |
| `NativePreparation::Existing { outcome, committed }` | Return an exact successful request match; `committed: false` identifies an earlier pending candidate and must await that candidate's durability |
| `NativePrepared::claim` / `definition` / `evaluation` / `artifact` / `result` / `receipt` / `recorded` | Read the complete prepared version, including all predecessors and independent evidence/result rows |
| `NativePrepared::registrations` / `work` / `diagnostic` / `response` / `delivery_result` | Borrow complete registration membership, respondent evidence, response state and artifact-free Receipt results at the same prepared prefix; matching projections exist on `NativeView` and expiring `NativeRead` |
| `publish_native(prepared)` | Replace the root without allocation; foreign, stale or out-of-order refusal returns ownership of the intact candidate in `NativePublishError` |
| `pin_native` / `NativeRead::with_claim` / `with_definition` / `with_evaluation` / `with_artifact` / `with_result` / `receipt` / `recorded` | Read one expiring fixed prefix through checked projection; release or clock advancement permits obsolete storage reclamation |

Preparation validates every supplied pending candidate's process-local owner and
exact predecessor-root provenance. This fences both existing-row reads and
observed ID absence. A sibling fork cannot masquerade as the same prefix. The
creation plan checks every proposed component, including disconnected cycles and
same-batch ancestry; a missing endpoint is an error. An actual Supersedes successor
must be compatible, and a published predecessor must be older. Terminal
predecessors retain their original status and terminal cut. Child creation and
parent registration are inseparable. Cancellation uses checked registry identity,
content, creation position and immutable Cause, including children staged earlier
in the pending chain. It preserves terminal descendants and unrelated peers.

Creation rejects missing, extra, duplicate and semantically substituted full
definitions, even when a caller reuses an external content stamp. Posting does
not treat Admission as already passed: it produces Ready evaluation rows and
complete membership in the same candidate as the Posted claim. Begin requires
the retained evaluator identity, exact expected evaluation revision and current
claim state. The participant supplies no readiness boolean, `Passed` predicate,
`OwnerState` or evaluator grant. `NativeContext` is supplied by the trusted owner;
its logical time cannot regress against the effective prefix. An exact retained
retry returns its original outcome and accepted time. A definition that requires
`required_policy` currently refuses begin until actual stored grant resolution is
implemented; absence of a grant is never converted to permission.

The [report transaction](../../crates/focal-core/src/native/reporting.rs) derives a
separate authority frame for an already-begun Admission attempt. It checks the
current evaluator, handler/version, phase, generation, expected revision,
deadline and retained fence. Actual proof or diagnostic schema and custody must
match the declared check. A retryable Error can advance only its declared retry
or fallback; a programmatic Pass awaiting quality is not a final passing check.
Observe outcomes remain recorded without blocking Required acceptance. A required
failure can atomically make the Posted claim PostFailed while eligible begun
siblings retain permission to report; their later results cannot change the
original claim cut. Cancellation, supersession and other explicit authority
fences still reject stale reports.

Artifacts own their descriptor once, including typed `ResultProvenance`: claim,
validation, exact target, generation, complete handler attempt and reported
verdict. Core checks it against the actual retained evaluation before constructing
trusted evidence facts. Identical diagnostic bytes from separate attempts can
reuse the same content tree while their descriptors have distinct content
identities. The artifact's own allocated ID remains excluded from its content
hash; the custody token also pins its request and artifact-specific intent, so it
cannot be reused for another artifact address. Inputs resolve against the same
effective root, including pending artifacts, with bounded inherited-visibility
checks. Testament input references resolve to retained native response rows in
that same effective root. Core does not resolve native inputs through an
independently mutable V1 map.

The [local custody verifier](../../crates/focal-evidence/src/native_artifact.rs)
checks actual bytes against an installed schema implementation. It seals inline
payloads into the existing synced content-tree format and verifies referenced
manifests, chunks, lengths, domains and classes before returning a private token.
Complete deterministic manifest bounds are checked before installing files.
Schema validity proves payload shape, not a validation verdict. This capability
proves local durability only; native admission has no installed replicated
placement qualification yet.

Cancellation and supersession obtain evaluation membership from the stored
registry, including earlier pending evaluations. They check each row against its
retained definition and registration before publishing authority fences together
with claim control changes. Fencing preserves an evaluation's recorded lifecycle
facts and original terminal results; it does not invent a result or testify for
the respondent. Cancellation continues through terminal owned descendants and
does not release their scopes. Adoption-specific fences remain future work.

Successful request keys and their private semantic intent fingerprints are stored
with the rows they produced. A changed intent under the same key is rejected;
an exact retry does not create another sequence. Published and pending matches
are explicitly distinguished. These fingerprints are neither V1 hashes nor a
specified successor wire identity. Native semantic refusals currently return
errors without retaining request outcomes; their eventual transport/durable
contract remains to be defined.

History preserves the transaction's model phases: each new claim has a `Created`
event at revision one, followed by each `ChildRegistered` event in child-ID order,
then any `Superseded`, `Cancelled` or `Posted` transition. Registration records the exact
child binding captured in the ownership registry and each successive parent
revision. Events include their before/after binding and resulting status;
preparation checks that the final reconstructed binding equals the stored row.
`NativeFact` distinguishes claim transitions, retained definitions, artifacts,
evaluation transitions and accepted results. Evaluation facts preserve
target/generation identity, before/after binding, state, phase, applicable attempt
and authority fence. `NativeOutcome` records claim, definition, evaluation,
artifact and result changes with the exact event count. All facts and
final rows publish together, including a parent with several new children or an
evaluation fenced by a control change. An Admission report records artifact at
ordinal 0, evaluation at 1, accepted result at 2, then a derived PostFailed claim
at 3 when applicable. `NativeAccepted` retains the actual reporting attempt,
sequence and ordinal, even when the next evaluation state already represents a
different retry or quality phase. The read-only `project_admission` checks the
complete registry, retained definitions and actual published results without
copying policy or creating another mutable aggregate. It uses original result
cuts for failure selection; later reports cannot repaint earlier failure.

The [preparation implementation](../../crates/focal-core/src/native/prepare.rs)
holds a real `MemoryBudget` Pending permit for its bounded model workspace,
changes buffer and event-construction scratch before constructing replacement
rows. Creation, posting and evaluator entry use the Ordinary lane; cancellation
and Admission reports use Completion. Input definitions and artifact descriptors
are already owned when passed in, and their retained size is bounded before
planning; a future ingress
must also reserve its decoding/construction buffers. Explicit claim, declaration
and registration copies account for dynamic capacities and allocator overhead;
registration growth charges old and replacement buffers together.
`prepare_batch_with` and
`prepare_after_with` reserve candidate pages before copying retained neighbors in
touched pages; newly supplied values move into those pages. The copier rejects
actual heap/allocator growth beyond the retained row's page charge. Candidate
pages retain their permits when temporary planning reservations are dropped.
Refusal or candidate drop cannot alter published rows or their prefix.

Native construction now caps ordinary leaf charge at 64 KiB and derives a maximum
entry charge from the existing preparation allowance, claim/event container
allowances and inline entry size. Tighter supplied internal limits remain in
force. These are RAM accounting bounds, not wire payload-size limits or new CLI
configuration. The [shared leaf partitioner](../../crates/focal-memory/src/range_layout.rs)
enforces byte and count limits in both preflight and construction. Larger admitted
rows occupy isolated singleton pages; adjacent inserts share an unchanged large
page instead of copying its payload. Replacing or deleting that row still uses
the checked merge. Import stages byte-bounded chunks and large rows alone.
Generic RangeStore byte defaults remain unbounded to preserve its existing V1
layout; native Core applies the finite ceilings when it creates its owner.

The trusted owner can now select `prepare_native_in` with a funded descendant of
its own budget. The same source pays for preparation and every new range root and
page; `verify_native_artifact` accepts it through its existing budget argument.
Temporary capacity returns to that source while published or pinned pages retain
their debits. Existing preparation methods use the original owner budget. The
operation still derives its own lane, and unrelated/ancestor sources refuse.
This is funding plumbing; receipt and Begin do not yet install the exclusive
entitlements, future-growth bounds or discrete/durable capacity guarantees in
§6.4.

Large claim/registration, declaration, evaluation, artifact, result and event rows
use private, fallibly allocated single-element containers so small outcomes and history do not occupy
their larger slots. Complete containers, nested capacities and allocator overhead
are precharged; moving new rows preserves their owned buffers. Retained
touched-page copies remain fallible and independent, and the reference-free
evaluation payload copies by value. Stored event bindings omit repeated ledger
identity and expand into exact public facts without allocation. This adds neither
per-object `Arc` nor infallible boxing.

Publication itself performs no allocation or I/O. Its caller must establish the
durability barrier first; this typed API does not prove that a log was synced.
Failed publication retains the candidate for retry or explicit drop. Completion
headroom is bounded, so new cancellation preparation can still refuse capacity;
an already-prepared candidate needs no new allocation to publish. Default native
limits are internal owner limits, not new mandatory operator configuration.
Precharge/reconciliation and pressure behavior require the owner-level
qualification recorded in [09](09-implementation-status.md).

Pinned reads use `SnapshotLease::project_next`, preserve lease/clock checks and
prevent borrowed rows from escaping a projection. The caller owns allocation of
any projected output and must enforce its output budget and ingress visibility
policy. Owned claims, definitions and registration sets do not implement
production `Clone`; evaluation state is a reference-free `Copy` value. This owner
adds no per-object `Arc`; shared budget and immutable page/root lifetimes remain
within the existing memory subsystem.

**Next integration boundaries:** the later RAM increments implement respondent
responsibility, adoption, WholeWork, deadline fencing, graph consequences and
audit sealing under this owner. Complete whole-claim closure funding and the
successor input/replay/activation boundary remain open (§§6.7, 6.10–6.13).
Resolve real stored policy grants before enabling checks that require them.
Preserve authorized begun late results after ordinary required-check failure;
explicit cancellation must not become scope release or fabricate testimony.

Native snapshots, prepared commands, import provenance, decoder identity, shared
WAL/Ready integration and quorum activation still require §§4–7. Current native
snapshots are RAM read leases, not durable checkpoint bytes. No current CLI or
MCP operation selects this owner.

RangeStore supplies the existing atomic publication primitive that Arena and
StableIndex individually lack. Its persistent directory now copies affected
paths and reuses untouched subtrees (§6.5). Native claim/outcome counts, retained
events, snapshot lifetimes and
per-command graph bounds remain subject to owner memory capacity; no native
history retirement or automatic sharding is introduced here. Directory costs,
large-graph behavior, retention, sharding, failure-domain placement and global
throughput/operations remain scale work. This increment does not qualify the
laptop-to-Meta-scale objective.

### 6.3 Admission results, first receipt and respondent evidence

**Result/receipt/evidence milestone.** These native RAM owner transactions share
the range publication seam; §6.7 describes integrated evaluation and mandatory
respondent funding. Complete whole-claim and durable completion reservations
remain open.

The native owner implements Admission reporting, first receipt acquisition,
respondent work and diagnostic submission, authored response closure, posting and
claimant receipt. Receipt creates responsibility only. The respondent explicitly
authors its testament after work ends, successfully or unsuccessfully; later
validation and acceptance remain separate. Guaranteed end-to-end completion
capacity is still required before production activation.

1. **Implemented: owned proof and diagnostic artifacts with local custody.**
   Fallible descriptor construction/copy accounting, typed result provenance and
   actual schema/storage verification feed Core-owned `EvidenceFacts`. Neither
   caller metadata nor a result value can manufacture custody. The retained
   artifact holds one descriptor and its local custody capability; the owner
   projects evidence facts without retaining a second copy of that provenance.
   Replicated placement remains open; §6.7 supplies the managed Admission Begin
   reservation for this local verification/report chain.
2. **Implemented: atomic Admission result publication.** `ReportAdmission`
   resolves the actual evaluation and records proof/diagnostic artifact, accepted
   attempt, next evaluation state, any claim failure, history and request outcome
   together. Retries, fallback, quality entry and terminal reporting preserve
   their actual attempts. No successful admission result creates a testament or
   marks whole-work acceptance complete.
3. **Implemented: report-specific continuation authority.**
   `admission_report_owner` requires a begun attempt and exact live authority;
   ordinary parent failure or progress does not revoke that chain. Core exercises
   eligible sibling reports after PostFailed and explicit control fencing. The
   actual owner now exercises eligible begun Observe Error/retry/quality reports
   after first receipt while preserving its immutable allocation and authority.
4. **Implemented: bounded read-only Admission projection.**
   [project_admission](../../crates/focal-model/src/lifecycle/aggregation_admission.rs)
   checks the complete `RegistrationSet`, declarations, evaluation rows and
   accepted-result history. It refuses missing/substituted membership and invalid
   publication cuts; Required final outcomes determine Pending/Passed/Blocked,
   while Observe outcomes cannot block. No mutable acceptance copy or summary is
   introduced. The private decision drives PostFailed in the report mutation and
   gates the actual subject's first receipt acquisition.
5. **Implemented: originating result history.** Every accepted report retains
   the actual handler attempt and original `(sequence, ordinal)` independently
   of its changing evaluation row. Pending results use their prepared sequence.
   Failure selection preserves the original cause cut when later sibling reports
   or projections arrive, including identical diagnostic bytes across distinct
   attempts.
6. **Implemented: consume real receipt eligibility.**
   [AcquireReceipt](../../crates/focal-core/src/native/receipt.rs) checks the exact
   Posted claim, actual subject, authored deadline and private Admission decision.
   A nonzero new ReceiptId is allocated in a retained ledger-wide index and the
   owner assigns epoch one. The index survives cancellation and must preserve all
   future adoption allocations. Exact retries resolve before uniqueness/deadline
   checks and return the original pending or committed outcome. First acquisition
   does not materialize a response, run a handler, fabricate a result or fence
   already-begun Admission evaluations. Ready Observe checks remain Ready and
   cannot newly begin after receipt; begun checks may complete their authorized
   retry and quality chains.

**Implemented Begin prerequisites and operation-specific staging.**
[admission_budget](../../crates/focal-core/src/native/admission_budget.rs)
checks the complete authoritative Admission cohort before a Begun state can
publish. It bounds projection work when every member has a result, resolves the
current declarations, registrations, evaluations and retained results, and requires
the original eleven-write failure floor. That prerequisite alone does not price
graph or cohort effects; the managed Required Begin envelope now derives the
complete shape from the indexed component (§6.7). Retries update the latest result,
so this projection-work bound does not multiply by historical attempt count.
The native owner also refuses Begin when any reachable programmatic, agentic or
quality phase requires policy evidence: no installed native policy-evidence registry yet
supplies those grants. Passing the first phase cannot admit an unavailable quality
continuation.

[ConstructionBudget](../../crates/focal-core/src/native/prepare_budget.rs)
sizes direct staging by operation. Begin needs one evaluation container, one
event and four changes including Meta and request outcome. A report starts with
the original nine-change ordinary shape or eleven-change isolated failure floor.
A real Admission failure expands those buffers through the complete checked
graph plan, including every monitor/index revision and original journal fact;
automatic cohort construction has its own admitted suffix. Actual counts are
checked before the final change vector is allocated, and actual capacities must
fit the reserved allowance. Both operations also check their actual range plan
against an occupancy-independent `RangeWriteEnvelope` (§6.5). Direct Core staging
prices the current mutation; `NativeOwner` additionally retains the future report
chain and graph failure entitlement before accepting Begin (§6.7).

The receipt owner gathers the complete bounded outgoing closure from the actual
effective root, including pending rows: immutable dependency/await declarations,
active runtime wait roots and every owned child recursively. It validates owned
child identity, Cause and registration chronology; missing endpoints refuse.
Unrelated ledger rows are never scanned. The sorted closure and exact peers are
owner-produced, not participant-selected.

[Snapshot::prepare_capture](../../crates/focal-model/src/lifecycle/graph_capture.rs)
performs allocation-free preflight for all four graph buffers. The owner reserves
the returned peak charge before building, and actual capacities must fit that
charge. The original capture/fixpoint algorithm is shared by the compatibility
wrapper. The owner checks the effective publication cut and consumes the private
`Start` token against every original binding. DependsOn requires satisfaction;
Awaits permits terminal failure; runtime roots retain their Satisfied, Terminal
or Released predicates. Received, the immutable holder/fence/acquisition sequence,
claim history, receipt allocation and request outcome publish together. There is
no automatic scope release or testament.

The adapter also stages checked `graph_release` transitions for locally complete
neighbors whose least-fixpoint satisfaction becomes established in that snapshot.
WholeWork now supplies those local outcomes (§6.9); explicit terminal-owner release
has its own checked graph entry (§6.12). That later section now implements native
monitor commands, direct incoming monitor subscriptions and frozen settlement.
Outgoing closure capture alone is not a complete propagation implementation;
remaining control paths must use the complete affected-source contract. Posted
Required Admission now has its own full graph promise and atomic failure path
(§6.7).
The receipt milestone
qualified terminal-failure Awaits, unresolved DependsOn and complete owned closure;
later native integration and its qualification are recorded in the sections below
and [09](09-implementation-status.md).

**Funding gap at this milestone, addressed for managed Admission in §6.7:**
low-level Core Begin retains no per-attempt reservation
for future evidence verification, artifact/result rows or history. Each current
local verification reserves an 8 MiB store workspace plus the schema byte bound
(up to 1 MiB) and a small retained token in the Completion lane before reading or
writing content. Core report preparation reserves its own bounded replacement
workspace and pages, using its operation-specific construction and guarded range
envelopes. These checks refuse safely under pressure, but a shared
Completion lane alone does not guarantee capacity for every begun attempt.
Preflight and reserve the combined future requirement before Begin, transfer
that reservation through accepted attempts and release it only under the checked
completion/fencing protocol. Section 6.4 specifies the dependency order and owner
boundary for that work. Pin permitted schema identities and their byte/workspace
limits, plus a complete authored descriptor contract, before promising that
combined requirement. The future decoder must separately reserve input descriptor
construction before allocating its owned buffers.

**Implemented respondent evidence and authored closure.**
[SubmitWork / SubmitDiagnostic](../../crates/focal-core/src/native/work_artifacts.rs)
authenticate the current receipt holder and require locally verified bytes. The
immutable descriptor binds `WorkProvenance` to the actual claim, cycle and either
a declared output slot or a diagnostic reason; its producer and receipt must
match. Work and result provenance are exclusive. Diagnostic admission uses the
real native descriptor and custody attestation through
`ResponseDiagnostic::record_native`, including kind `error` and a nonzero schema;
it does not manufacture a legacy Artifact. Work submission creates an independently
observable Generated artifact. Claimant `ReceiveWork` records observation without
creating a response or accepting the claim.

[Failed-work transactions](../../crates/focal-core/src/native/work_failures.rs)
preserve unsuccessful production and claimant rejection. `FailWorkProduction`
resolves an already-retained current respondent Production diagnostic and uses its
actual descriptor binding for GenerationFailed work in the declared slot. It
creates no nonexistent product bytes or second diagnostic. `RejectWork` requires
a new claimant-authored structure/metadata error artifact with exact
`ReceiptRejection` provenance: claim, original receipt/cycle, rejected artifact ID
and digest, and reason. Its visibility inherits the original output's restrictions.
The output becomes ReceiptFailed only while unattached; a late rejection cannot
rewrite Attached work. Claimant rejection remains permitted after parent
terminalization, without changing the parent. Neither failure automatically
creates testimony or terminalizes the claim.

The owner stores bounded linked work and diagnostic membership for each
claim/receipt/cycle, plus an exact slot index. Closing follows that retained
membership, including earlier pending submissions, and rejects truncated,
duplicated or substituted manifests. No ledger-wide scan or participant-provided
list establishes completeness. Every Generated/Received output must appear in the
attachable manifest and **every respondent cycle diagnostic** in the diagnostic
list, including diagnostics accompanying Complete. The owner also supplies every
GenerationFailed/ReceiptFailed row. `Response::failed_work` freezes its exact
binding, slot, state and diagnostic separately; those terminal rows are never
attached or revision-advanced. All retained failure fields participate in the
response's private semantic stamp and bounded fallible copy/accounting. Failed
references and diagnostics remain inspectable without filling absent required
output slots or becoming successful work witnesses.

[CloseResponse](../../crates/focal-core/src/native/responses.rs) requires the
respondent's bounded summary, confidence and explicit Complete, Partial, Refused,
Impossible, Interrupted or Failed outcome. Every non-Complete report requires at
least one real respondent diagnostic. Missing required output does not prevent
failure testimony. The shared `Response::prepare_close` checks the exact holder, receipt,
cycle and manifests; fallible owned copies and actual allocation capacities are
charged before staging. Claim history is copied with space for exactly one new
response. The Generated response, work attachments, cycle closure, claim
observation, history and request outcome publish atomically. Reported outcome
does not determine claim acceptance or fabricate later validation results.

`PostResponse` is a separate respondent action; `ReceiveResponse` is a separate
claimant observation of the posted response. An unattached work artifact or a
posted response can still be received after its claim becomes terminal or locally
complete. The claim then retains its binding, history, status and original cut.
Work observation checks its original indexed receipt/cycle; response receipt
still requires its current matching receipt fence. These observations neither
reopen the claim nor grant new work or evaluation authority. They create no new
Receipt evaluation or result after terminalization/local completion.

**Implemented pure Receipt results.** For an open claim,
[delivery preparation](../../crates/focal-core/src/native/delivery.rs) resolves the
complete retained declaration set on `ReceiveResponse`, then materializes every
pure Receipt check against the actual received response, its report stamp and
receipt fence. Generation is the response cycle. Registration growth is copied
and precharged once for the whole cohort; the original append ordinals remain
unchanged. Before a check's declared deadline, that same mutation records its
Validated evaluation and a separate
[NativeDeliveryResult](../../crates/focal-core/src/native/delivery_owned.rs), with
its original publication sequence and typed history. Pure Receipt has no proof
artifact, external handler attempt, reporter impersonation or content-store I/O.
This does not pass any output check or establish aggregate acceptance.

At or after a declaration's deadline, the response is still received and the
corresponding evaluation is retained Ready with its immutable deadline. No Pass or
terminal failure is invented; a later deadline/fence transaction remains open.
Terminal/local-complete parent receipt is audit-only and materializes no evaluation
or result. Participant-reported Complete versus Failed does not select these
Receipt outcomes. A failed-work reference likewise cannot act as the respondent
diagnostic required by a non-Complete report; only an actual respondent diagnostic
can fill that role. Its exact Production reference may be retained both in
`failed_work` and in the respondent diagnostic list.

**Implemented Increment materialization, begin/report authority and target sealing.**
[increments](../../crates/focal-core/src/native/increments.rs) checks the complete
retained declaration set and materializes every declared Increment, including
Observe, in the actual `SubmitWork` candidate. The owner copies and precharges the
registry once with space for the full cohort; per-member registration cannot
allocate another buffer. Each Ready evaluation pins the actual output and receipt,
with generation derived from its work cycle. Materialization neither begins a
handler nor creates a verdict. A Registrations history fact makes existing funded
grants recheck the actual append ordinals and row bounds. The claim's binding and
phase remain unchanged by registry-only publication.

If the claim declares any Required Increment, SubmitWork also checks the output's
mandatory visibility against the derived report descriptor dimensions and traversal
limit before custody I/O or exposure. An output whose restrictions make every later
report impossible is refused before it can leave a Required evaluation stranded
Ready. Observe-only work may still be submitted; Begin refuses an unfundable check
without making it a claim acceptance obligation. Begin and ownership reconstruction
always check the actual source against the pinned limits.

[Begin/Report authority](../../crates/focal-core/src/native/increment_authority.rs)
resolves the actual source output, complete declaration and stored registration.
It derives evaluator, phase, generation, attempt, deadline and receipt authority
without participant permission flags. Begin remains possible after the last
authored response closes; it does not allocate a new work cycle. A ReceiptFailed
output cannot attach or enter WholeWork validation, but an already-registered
Increment may begin and report against those real immutable bytes under live
authority. An existing begun chain may likewise finish after claimant rejection.
The evaluator supplies actual proof/diagnostics; no automatic Incomplete result,
artifact rehabilitation or replacement slot witness is generated. GenerationFailed
never acquires an Increment target.

`ReportIncrement` uses the ordinary artifact/evaluation/accepted-result history
path, retaining the actual attempt and original result position. It supports the
committed Error retry/fallback and quality policy, but never publishes a parent
failure or changes the work artifact's status, even for a Required failed check.
The managed owner funds this complete bounded RAM report chain before Begin,
including custody verification and immutable range versions (§6.7). Exact pending
retries, journals, rollback and reconstruction use the same owned completion book.
This is no disk or replica reservation.

[SealIncrementTargets](../../crates/focal-core/src/native/increment_seal.rs) is a
claimant action over the complete stored registry, with exact declaration/state
checks. It prevents new Increment targets while allowing registered Ready checks
to begin, begun checks to report, and respondent diagnostics or failure testimony
to be supplied. It does not complete a check, release a scope or seal final audit.
Registration/work/evaluation/result projections borrow one actual effective,
candidate or expiring committed prefix. Registry insertion scans existing members
for each new check, and declaration validation also scans the immutable policy.
Both collections have owner limits. The indexed completion book's separate cohort
complexity does not remove these repeated scans; their optimization and measured
cost remain performance work.

The shared [response_budget](../../crates/focal-core/src/native/response_budget.rs)
bounds accepted work membership by the actual closing transaction shape: attaching
*n* work rows requires `2*n + 7` changes, including events, metadata and outcome.
It also checks staging, event and traversal limits before accepting another work
artifact. This prevents an intrinsically unrepresentable closing batch. It does
**not** reserve future RAM, response rows, mandatory diagnostic headroom, disk or
replica capacity. The separate receipt-bound envelope and owner loans in §6.7
now reserve mandatory diagnostic, close and post RAM/record capacity; successful
output payloads and durable capacity remain independently admitted.
The same module counts the full immutable target allowance: Admission once;
Delivery and slot checks once per authored response; Increment once per possible
work slot per response, including slots without WholeWork checks. First receipt
refuses if that complete count exceeds either registry limit, or if the complete
combined pure Receipt/WholeWork cohort cannot fit one receive transaction. With
`D` pure Receipt declarations and `W` WholeWork slot checks, the worst receive
transaction writes `4*D + 2*W + 6` rows: one changed claim and response, their
facts, metadata/outcome, each Delivery evaluation/result/facts, and each work Ready
evaluation/fact. The owner allocates the full `D+W` additional registry capacity
once and neither cohort may grow it. It never silently reduces
`max_responses` to fit a smaller registry. These are count/shape guards, not future
funding. The Admission envelope prices bounded registry growth and complete
response history (§6.7), so eligible begun Admission reports can coexist with
append-only Receipt and WholeWork registrations. Admission reports reduce the
Admission gate only while the claim is Posted. Once received or terminal, a begun
Admission report records its actual authorized evidence without rescanning later
cohorts or changing the original claim outcome. This preserves completion when
legal response growth exceeds the earlier Admission projection visit bound.

Non-Receipt WholeWork entry/reporting, expired Ready deadline/fence
publication, aggregation, audit and receipt adoption still require owner
integration. Native codecs, WAL/Ready, Session and CLI/MCP activation remain
separate gates; these commands expose no durable successor format.

The [native report tests](../../crates/focal-core/src/native/report_tests.rs)
exercise actual pending owner chains, Required pass/failure, Observe outcomes,
quality and Error continuations, exact actor/attempt/provenance/schema/custody,
control ordering, stable first failure, exact retries, reopened content and
atomic capacity refusal. The receipt tests additionally exercise owner-level
receipt-before-late-report and graph-start preparation. The subsequent held
Admission capacity has the separate ownership and qualification contract in §6.7.
[09](09-implementation-status.md) records the completed
test runs. Native wire/codec definitions, replay/import, WAL/Ready integration,
Session activation, CLI/MCP dispatch and replicated custody remain gated until
the explicit durable representation and remaining object transactions exist.

### 6.4 Reserve completion capacity before responsibility

**Dependency plan and earlier primitive evidence.** Section 6.7 implements the
managed evaluation and mandatory respondent RAM/discrete-record portions of this
plan. The primitive and low-level Core APIs described here do not grant individual
entitlements; complete whole-claim responsibility and independent durable capacity
remain activation requirements. The guarantee covers the admitted bounded
report, including a diagnostic when participant work fails; it does not promise
that an external participant will run or that an unreachable participant will
respond. Focal never fabricates a respondent's testament to consume an allowance.
Held budget credit prevents later Focal admission from stealing that allowance;
it does not preallocate physical memory or ensure allocator/OS availability.
Allocation, I/O and custody failures must retain the entitlement for a checked
retry or fail closed, without recording successful delivery or validation.

1. **Implemented: preflight range construction against exact inputs.**
   [RangePreparationPlan](../../crates/focal-memory/src/range_preflight.rs)
   owns the sorted changes and borrows the actual base root. `plan_batch` and
   `plan_after` expose it; existing preparation methods consume the same plans.
   It routes sorted changes through the immutable base directory and inspects
   retained entries only in touched pages. It quotes the input buffer and its
   heap, conservative cumulative directory-node construction, exact new entry
   pages and maximum merge scratch without copying values. The additional
   construction peak and retained charge are separate upper bounds; existing
   roots and snapshot pins remain charged
   throughout. Building consumes the plan and checks actual allocated capacities
   against the quote before retaining any directory, merge or entry vector. A
   quote by itself grants no memory reservation. The same capacity checks cover
   checkpoint-import staging. Key/value copy heaps and extra copier workspace
   remain subject to the existing caller contract. Quoting and construction now
   share the same byte/count leaf partitioner and oversized-singleton reuse rule;
   a reused oversized leaf stays in place without a new page or payload charge;
   inserting neighbors copies only their affected directory paths.
2. **Implemented: one reservation interface with funded owner pools.**
   [MemoryBudget::funded_child](../../crates/focal-memory/src/budget.rs) reserves
   spendable capacity plus non-spendable pool metadata from its parent. Evidence
   Payload, native Pending workspace, range Pending scratch, Roots and Pages use
   the same `MemoryBudget` interface. A funded source spends locally and moves
   ancestor category charges from `Reserved` to the actual allocation kind; it
   never releases and reacquires the ancestor total. Refund restores every
   ancestor category before making local capacity reusable. This ordering also
   holds through normal descendants and nested funded pools.

   Ordinary-funded capacity can pay for later Completion work while its full
   original ancestor ordinary charge stays held. Completion-funded capacity
   cannot admit Ordinary work, including through descendants. New responsibility
   must fund its guarantee through Ordinary admission. `build_in_with` and
   `prepare_native_in` require a source within the actual owner's budget and retain
   their operation-derived lane. Existing APIs still use their original source.
   Failure and scratch shrink return credit to the pool; retained pages and
   custody tokens keep their allocation debit until their final drop. Input
   buffers drop before their Pending permit.

   The pool uses one shared counter allocation for an owner, with no per-object
   or per-evaluation `Arc` wrapper. Its fixed backing remains fully reserved until
   the last pool handle, descendant and issued allocation disappears; even a small
   token can keep idle capacity held. There is no implicit trim/close operation.
   This implements aggregate funding, not the individual entitlements or exclusive
   candidate spending required below. `NativeOwner` now supplies the managed chain
   boundary, but its current preparation still uses the original Core budget.
   Integrating the reusable pool, completion book and exclusive spending loans
   remains required before claiming a completion guarantee.
3. **Define a complete private `CompletionEnvelope`.** Bound descriptor bytes,
   proof/diagnostic schema bytes, input references, persistent artifact/result
   records, request outcomes, event rows and sequence consumption. Reserve the
   entire declared retry/fallback/quality chain using
   `Declaration::attempt_bound()`, not just the current attempt. Enforce separate
   slots for required diagnostics. Byte headroom alone cannot satisfy artifact,
   result or history limits. The respondent envelope now covers mandatory
   diagnostics, authored close and posting for its remaining cycles (§6.7);
   successful output payloads are admitted separately within that closing bound.

   The implemented `RangeWriteEnvelope` covers one storage write, not these
   complete domain obligations. Pin the allowed schemas and verifier limits,
   descriptor metadata/reference/visibility bounds and result provenance contract
   to the admitted evaluation. A later report cannot select an unpriced schema or
   widen descriptor construction. Include both direct construction and storage
   copies, and preserve credit for every possible fallback and quality outcome.
4. **Implemented bounded leaf copies and persistent directory paths.**
   `RangeConfig::page_bytes` bounds a leaf's full charge, including Page/Arc and
   vector bookkeeping, inline entries and declared heap. `max_entry_bytes` bounds
   inline entry plus declared heap, excluding page bookkeeping. Both must hold
   at least one empty entry. Entries above the ordinary page ceiling remain
   admissible only within the entry ceiling and occupy singleton leaves. They
   are never copied for adjacent inserts; same-key replacement/deletion follows
   the normal checked merge. Limits are fixed for the owner's lifetime.

   The [leaf implementation](../../crates/focal-memory/src/range_layout.rs) emits
   identical partition spans for quotes and builds. Checkpoint import uses the
   same layout and bounds each staging chunk by bytes, admitting an oversized
   first row alone. Native Core derives finite ceilings from its existing node
   allowance; generic defaults retain existing count-only layout. No per-object
   sharing wrapper is added.

   The persistent directory now bounds node fanout, minimum occupancy and
   height; branch splits and deletion repairs copy only affected paths and
   neighboring directory nodes. Child minima are borrowed from existing pages,
   so generic separator keys add no heap or clone requirement. Planning and
   construction visit touched leaves using the same immutable base boundaries.
   Section 6.5 records the implementation and its accounting contract.

   `RangeStore::future_write_envelope` now derives a one-write bound from declared
   change/delete/input-capacity limits, incoming heap and enforced layout limits,
   independently of today's occupancy. Its private owner stamp and `check_plan`
   guard bind the later concrete plan; §6.5 gives the page-count argument.
   Retained copies across every remaining retry/quality report must stay funded
   even if snapshots prevent reclamation; a serialized owner may share bounded
   temporary workspace separately. New rows, snapshot/pending admission and
   layout-limit changes cannot consume or invalidate existing entitlements.
   Complete domain envelopes and individual entitlements remain unimplemented.
   Preserve V1 bytes and interpretation; storage layout qualification alone
   cannot establish this resource guarantee.
5. **Keep one owner-controlled `CompletionBook`.** Key each entitlement by owner
   incarnation and exact evaluation generation or receipt/cycle. Immutable rows,
   history and snapshots retain only the entitlement identity. They must neither
   own a cloned reservation nor wrap every object in `Arc`. Keep the actual
   resources in one bounded owner pool; use an exclusive spending capability so
   two pending forks cannot spend the same allowance. Account the book itself.
6. **Implemented managed candidate ownership; resource loans remain open.**
   [NativeOwner](../../crates/focal-core/src/native/owner.rs) exclusively owns the
   Core and one bounded ordered queue. Its buffer is charged before allocation
   and checked against actual capacity. Failed construction returns the original
   Core in `NativeOwnerInitError`, without cloning it. Preparation uses the full
   pending chain; retry lookup precedes queue/memory admission. Tickets contain a
   private owner incarnation and non-reused serial, retain no pages and cannot
   create a fork.
   Publication accepts only the head, allocates nothing and restores the exact
   candidate on refusal. Suffix discard and owner teardown drop tail first.
   Borrowed views prevent mutation while in use; read leases pin committed state
   only. Existing low-level Core preparation remains available separately, but
   wrapping a Core exposes no path to extract or mutate it independently.

   The private `Checked`/`Fresh` split in
   [prepare.rs](../../crates/focal-core/src/native/prepare.rs) now resolves exact
   retries before source selection or reservation. Fresh owns the checked input,
   intent, cut and operation lane while borrowing the actual committed/pending
   base. Source ancestry is checked before construction, and provisional permits
   remain held until prepared pages own their charges. Fresh establishes request
   identity, base and admission preflight; it is not an evaluator or completion
   entitlement grant. A future loan selector must resolve the authoritative
   evaluation generation and attempt before lending its dedicated allowance.

   This boundary does not establish log durability, reserve completion envelopes
   or own a completion book. The external log owner must retain unresolved
   candidates and establish durability before `publish_after_durable`; timeout
   alone cannot authorize discard. The next step must attach exclusive resource
   loans to the owner-managed candidates. Begin then reserves once; exact retries
   reserve nothing. Publication transfers both rows and resource ownership, and
   discard drops dependent buffers/pages before returning loans. Recovery must
   rebuild entitlements from authoritative durable records without reusing
   process-local owner identities. Native wire/WAL activation remains open.
7. **Reuse bounded scratch without weakening persistent reservations.** A
   serialized owner may share a verification workspace across reports. The current
   roughly 9 MiB verification allowance is not a sufficient per-evaluation total:
   it excludes persistent descriptors, results/history, range copies, disk and
   installed replicated custody. Reserve persistent output separately and carry
   remaining credit through retry and quality transitions. Release unused credit
   on terminal evaluation completion or an explicit authority fence. Parent
   PostFailed alone must preserve eligible begun sibling and Observe reports.
8. **Reserve durable storage independently of RAM.** A `MemoryBudget` permit
   does not reserve filesystem or replica capacity. Account the admitted bounded
   payload and descriptor, staging/sealed-content coexistence, WAL growth and
   checkpoint replacement before promising durable completion capacity. Couple
   those reservations to the required evidence placement and log quorum policy;
   local custody alone is insufficient for a distributed promise. Preserve them
   across retries and owner recovery, and prevent unrelated admission from
   consuming them. Until that storage protocol is implemented and qualified,
   describe the memory work only as RAM/discrete-record capacity. Capacity
   reservation does not eliminate hardware I/O failure or quorum unavailability.

Qualification must exhaust an ancestor budget after Begin and still complete
the admitted chain; exercise growing directories, large retained neighbors,
pinned snapshots and pending forks; inject allocation, value-copy, custody and
publication failures; and prove exact retries never reserve twice. Cover
Error-to-retry, fallback and quality continuation, cancellation versus eligible
late siblings, and rollback of every discrete record allowance. Run these against
actual owner transactions and durable recovery before exposing the guarantee
through Session, CLI or MCP. Storage qualification must separately cover quota
pressure, staging and checkpoint coexistence, recovery and required replica
placement; unavailable durability must never yield a success receipt.
Resource ceilings remain internally derived from the
node allowance; this work must not add mandatory tuning concepts to laptop use.

### 6.5 Persistent directory and the remaining completion envelope

**Storage-envelope milestone.** This section records the directory and future-write
prerequisite before the complete managed Admission envelope in §6.7. Its remaining
work descriptions are the dependency state at that milestone.

The byte-bounded leaves and persistent page directory are implemented in
[focal-memory](../../crates/focal-memory/README.md). `Root.pages` no longer owns
a flat vector copied on every mutation. This is a private RAM layout change:
canonical ordered entries, root publication identity and public key-based
continuations retain their contracts. Qualification passed 65 memory unit tests
and the complete 1,385-test workspace suite; the dated implementation record in
document 09 records the executed checks and remaining work.

1. **Bounded immutable nodes.**
   [PageDirectory](../../crates/focal-memory/src/range_directory.rs) owns an
   optional root node. Directory leaves hold `Arc<Page>` handles; branches hold
   `Arc<Node>` handles. Each node owns one bounded vector, a cached subtree page
   count, its height and allocation debit. The outer range root retains owner,
   range, prefix and total entry count. Empty ranges have no directory root.
   Sharing is at page/node granularity, with no new per-row sharing wrapper.
2. **Occupancy and height under churn.** Non-root directory nodes hold 16–32
   handles. Insertion splits 33 into 16 and 17; deletion borrows from or merges
   with one sibling, then collapses unary branch roots. Root occupancy may be
   smaller. Checked height is bounded by the representable page count and the
   minimum fanout, including room for transient root growth. Directory repairs
   move handles without copying entry payloads. Entry pages keep their separate
   byte/count/singleton rules; partially full entry-page neighbors are not
   compacted by directory balancing.
3. **Borrowed keys and bounded scans.** Nodes store no cloned separator keys.
   Routing and construction checks borrow minima/maxima through descendants.
   Those extra descents add a height factor to comparison work; this version
   does not promise one descent per point lookup. Rank lookup uses cached page
   counts. A fixed-size borrowed ancestor stack advances between pages without
   allocating, cloning handles or restarting at the root for every entry.
   Snapshot sizing and copying recreate the same cursor. Public continuations
   still bind keys, lease and prefix; no tree addresses, ranks or stacks become
   durable identities.
4. **Only affected leaves and paths.**
   [Grouping](../../crates/focal-memory/src/range_groups.rs) routes sorted writes
   using the immutable base's page boundaries. Planning and construction inspect
   touched leaves without flattening or scanning the complete directory. All
   changes assigned to a leaf form one merge; rank translation accounts for
   earlier splits/deletions while retaining those original boundaries. Adjacent
   inserts preserve an unchanged oversized singleton. New pages replace, insert
   or remove directory links; untouched subtrees stay shared. Multiple edits can
   reconstruct a shared path more than once. A future shared-path batch builder
   can reduce this cost without changing the accounting contract.
5. **Conservative cumulative construction bound.**
   [Preflight](../../crates/focal-memory/src/range_preflight.rs) counts elementary
   leaf edits and bounds intermediate page count by existing plus newly emitted
   pages. `edit_bound` allows up to `6 × height_bound + 2` maximum-width nodes
   per edit, covering splits, recursive replacement, temporary underfull nodes,
   sibling redistribution and root changes. Small splice descriptions and
   traversal stacks remain on the stack. The builder decrements this checked
   cumulative byte allowance whenever it issues a node allocation; dropping an
   intermediate node refunds the budget but does not replenish the allowance.

   `directory_bytes` includes fixed outer-root metadata and this cumulative node
   bound. `new_pages_bytes` is exact. `additional_retained_bytes` and
   `additional_peak_bytes` are upper bounds, not exact live demand: intermediate
   nodes need not coexist, and constituent maxima may occur at different times.
   Existing roots, other candidates and pins remain separately charged. Quoting
   binds the owned inputs and exact base but reserves no memory. A pool smaller
   than the quote may fit a particular build; the quote is a sufficient capacity
   allowance, not a necessary minimum or an entitlement.
6. **Actual charges and publication.** Each new node's Roots debit covers its
   control block, child-vector capacity and allocator bookkeeping before
   construction. Actual buffer capacity is checked before retention. Only
   actual allocations spend the selected owner/descendant budget; shared nodes
   keep their original source. The full conservative quote is not debited as a
   second retained allocation. Input/merge buffers and new entry pages keep
   their provisional charges. Failure drops the private candidate and refunds
   its charges; publication remains an allocation-free root swap with owner,
   prefix and exact base-root checks.
7. **Canonical import.** Checkpoint construction stages byte-bounded ordered
   chunks, including isolated oversized rows, through the same page/directory
   invariants. Last-key checks and statistics use the directory without
   collecting its leaves. Invalid order, over-limit entries, arithmetic or
   allocation failure drops the partially built private store. Payloads drop
   before their permits. Checkpoints describe logical rows, never branch shape.
8. **Executed qualification.** The directory tests cover split, merge and
   root-collapse boundaries, changed separator minima, empty/all-deleted ranges,
   scattered edits, pending chains and pinned old roots. Three deterministic
   histories run 384 mixed batches against an independent ordered map. Internal
   node identity checks prove untouched subtrees stay shared; clone-observable
   generic keys prove no separator-key copies. A mixed update on a 2,049-leaf
   tree injects failure and excessive reported capacity at every observed buffer
   allocation, including after value copying. Each refusal preserves the exact
   budget, base root and pinned reads. Existing copier/import failure tests and
   full ancestor-pressure funded builds also pass. New-page charges stay exact;
   actual Roots debit fits its cumulative bound, including an exact-path charge
   test that refuses one byte short. One-leaf quotes grow with bounded height at
   32, 1,024 and 4,096 leaves. The complete workspace suite passes 1,385 tests,
   including the frozen V1 and current CLI/MCP, network, restart and native Core
   coverage. These results establish the exercised storage invariants; global
   load and production deployment qualification remain separate.

9. **Implemented future-write bounds and actual-plan guards.**
   [RangeWriteEnvelope](../../crates/focal-memory/src/range_envelope.rs) derives
   additional Pages, Roots, input and merge charges without inspecting current
   occupancy or allocating. `RangeWriteLimits` explicitly bounds changed keys,
   deleted keys, summed incoming Put heap and actual input-vector capacity.
   It requires `deleted_keys <= changed_keys <= input_capacity`.
   Delete-key charges use the largest possible old entry; a put-only report pays
   none of that allowance. Defaults with unbounded byte settings are clamped by
   the immutable owner budget. Its limit divided by minimum full-page charge
   bounds base page count, and existing plus possible new pages bounds directory
   construction height. This remains valid as unrelated writes grow the range.

   For `m` changes, at most `m` old leaves are affected. Within ordinary leaves,
   `p` incoming rows split retained entries into at most `t + p` runs that still
   fit their original byte/count limits. Each incoming row fits one singleton,
   giving an ordered partition of at most `t + 2p <= 3m` pages. Greedy partitioning
   cannot emit more pages than that valid partition. A fully deleted group needs
   one removal, covered by its old-leaf term. Modified oversized singletons have
   no retained payload; unchanged ones are reused with only their new neighbors
   partitioned. Thus the same `3m` bound covers elementary directory edits,
   without multiplying leaf output by the maximum entries per page.

   `check_plan` rejects a different owner incarnation, excessive changed/deleted
   keys, Put heap or spare vector capacity, and any preparation-charge component
   exceeding the envelope. It checks the actual immutable-base/owned-input plan
   before construction; ordinary preparation APIs do not implicitly enforce a
   caller's future envelope. Existing roots, pending candidates and pins stay
   separately charged. Native Begin and Report now use this guard with their
   operation-specific construction ceilings. Quoting and guarding do not reserve
   capacity or issue an evaluation entitlement.

This removes the flat-directory prerequisite and supplies the storage component
of a future completion bound. A complete private domain envelope must still price
the full remaining retry/fallback/quality chain, pinned schemas and authored
descriptor contract, direct claim/descriptor construction, evidence verification
and discrete record allowances. Retained writes need capacity even when old
snapshots prevent reclamation. The owner completion book, exclusive spending loans
and durable storage reservations were separate prerequisites at this milestone.
Sections 6.7–6.9 now integrate evaluation, graph and mandatory respondent reporting
envelopes in the native RAM owner. Complete whole-claim responsibility and durable
reservations remain required. These native changes do not activate
Session/CLI/MCP lifecycles or establish global-scale performance.


### 6.6 Expandable owner funding before individual completion loans

**Expandable-pool milestone.** This section records the memory primitive before
its integration into the managed owner in §6.7.

Reserving a large fixed fraction for every native owner would strand unrelated
capacity. The [elastic funded pool](../../crates/focal-memory/src/budget_elastic.rs)
now permits explicit growth and return of idle capacity through one
owner-controlled source. Fixed pools retain their existing complete-backing
lifetime. The primitive follows the contracts below; it does not itself attach
an entitlement to Begin. The managed integration owns one pool for the owner,
with individual grants rather than a separate pool per evaluation.

1. **Keep structural ceilings immutable.** The non-Clone
   `ElasticFundedPool` controller is created by
   `MemoryBudget::elastic_funded_child(lane, ceiling, initial_capacity)`.
   Zero initial capacity is valid, with metadata funded immediately. The
   controller exposes its ordinary `MemoryBudget` source for construction and
   exclusive `grow`/`trim_unused` operations. `MemoryBudget::limit()` continues to
   report the immutable ceiling used by future-write bounds. Current funded
   capacity and immediately available credit are separate observations.
2. **Store backing once per owner pool.** Keep the existing ordinary and fixed
   funding behavior. Elastic backing lives inside the coarse shared counters:
   immutable lane, metadata and ceiling; atomic held backing and available
   credit. Hold parent backing in this aggregate through every source clone,
   issued allocation and descendant. Do not add a permit vector per growth,
   per-evaluation source, mutex or domain-object sharing wrapper.
3. **Fund before publishing credit.** Growth checks ceiling and arithmetic,
   reserves parent `Reserved` bytes in the original funding lane, transfers the
   reservation into aggregate backing, then publishes additional available
   credit. Perform every fallible check before transfer. A private transfer may
   disarm a reservation only after the aggregate owns its entire charge; failure
   before that point uses normal RAII rollback.
4. **Use available-credit admission.** An elastic reservation first checks its
   lane and atomically acquires available credit, then updates local accounting
   and reclassifies ancestor `Reserved` bytes to the actual kind. Do not admit
   against a separately changing `limit - used` snapshot. Refund restores
   ancestor kinds and local accounting before publishing available credit last.
   Fixed pools and normal descendants retain their existing behavior and exact
   rollback when an elastic ancestor refuses.
5. **Trim only explicitly unpromised idle credit.** The unique controller
   serializes grow/trim while source clones may reserve and release concurrently.
   Trim atomically acquires idle available bytes before reducing aggregate backing
   and refunding the parent in the original funding lane. Metadata is never
   spendable or trimmable. Controller drop does not invalidate retained sources;
   final counter destruction releases remaining backing exactly once. An owner
   completion book must additionally exclude logically promised idle bytes from
   trim: the memory primitive sees issued allocations, not future obligations.
6. **Preserve hierarchy and publication lifetimes.** Nested fixed or elastic
   backing is itself an issued allocation in its parent, including the child's
   idle capacity. Preserve the existing maximum ancestry depth and the rule that
   Completion-funded capacity cannot admit Ordinary work. Prepared and pinned
   pages retain their source through owner teardown. Never infer safe credit
   reclamation by subtracting two non-atomic diagnostic snapshots.
7. **Qualify races and actual owners.** Cover metadata-only creation; repeated
   grow/spend/refund/trim; failed parent admission; trim beyond idle credit;
   mixed-kind allocation splitting/shrinking; normal/fixed/elastic descendants;
   both funding lanes; and controller drop with a live child, zero-byte split
   permit, prepared range or pinned page. Exercise controlled interleavings at
   credit acquisition, ancestor reclassification, growth publication and trim
   refund, plus concurrent churn. Every final category must return to baseline.
   Existing fixed-funded tests must remain unchanged and pass.

Section 6.7 integrates this source and the bounded Admission/Increment completion
book into `NativeOwner`, with exact retry ordering, authoritative loan selection and
candidate journals. The independent durable reservations and remaining object
families in §6.4 still require implementation.

### 6.7 Integrated NativeOwner RAM completion contract

**Initial integration milestone.** This section records the first completion-book
integration. Section 6.8 supersedes its sorted grant vector, repeated cohort work
and workspace high-water policy with the indexed implementation. The authority,
funding, journal and remaining activation boundaries below continue to apply.

**Implemented typed Admission/Increment integration; native production activation
remains open.** [NativeOwner](../../crates/focal-core/src/native/owner.rs) owns the Core,
its sole pending chain, one expandable Ordinary-funded source and a private
[CompletionBook](../../crates/focal-core/src/native/completion_book.rs). Eligible
Begin now reserves the admitted evaluation's bounded future reports before its
candidate can be accepted. Verification and report construction spend that held
capacity through exclusive owner loans. This contract covers RAM accounting and
finite native record/counter allowances. It does not reserve disk or replica
capacity, and low-level Core preparation alone does not provide it.

1. **Pin the complete evaluation contract before accepting responsibility.**
   Resolve the actual effective claim, immutable declaration, complete registration
   set and evaluation generation. Check the declared actor, target, current
   revision, readiness and trusted deadline before funding. Preserve the complete
   Admission projection visit bound and its four-event Required-failure prefix.
   For a Posted Required evaluation, also reserve the complete indexed component's
   graph/monitor consequences and cohort suffix described below. Increment reports
   have three events and no parent-failure
   write, irrespective of Required/Observe mode. The helper rejects any reachable programmatic or quality
   phase that requires unavailable policy evidence; it cannot synthesize grants.

   [SchemaSet](../../crates/focal-core/src/native/completion_schemas.rs) traverses
   the bounded proof and diagnostic hashes for every handler, fallback and quality
   phase. It retains unique sorted verification quotes and charges the actual
   buffer capacity. Unknown or unbounded schemas refuse before a grant is installed.
   Each report must select a declared hash whose current registered byte/workspace
   quote exactly matches the pin. The trusted verifier owns the implementation
   behind that immutable schema identity; the book does not execute validator code.

   [CompletionEnvelope](../../crates/focal-core/src/native/completion_envelope.rs)
   pins descriptor dimensions and aggregate owned construction charge, including
   kind, metadata, inline bytes, input references and visibility labels. Durable
   content references remain possible within the same verification bound. Price
   `Declaration::attempt_bound()` across the entire retry/fallback/quality chain,
   with one possible complete Required Admission failure transaction in addition
   to the ordinary attempts. That one-time shape includes the parent, affected
   component, monitor/index history and seals. Include descriptor ingress allowance, actual custody verification, Core staging, persistent range
   copies, artifacts, accepted results, events and outcomes. The future decoder
   must still acquire ingress capacity before constructing its owned input.

   Increment report descriptors must inherit all source-output visibility labels,
   even with no explicit input references. SubmitWork applies the derived descriptor
   and traversal limits before custody I/O whenever any Increment is Required;
   Observe-only targets may be admitted without promising a fundable Begin.
   Before Begin installs any grant, and again
   on ownership reconstruction,
   [check_completion_target](../../crates/focal-core/src/native/increment_authority.rs)
   resolves the exact immutable output and verifies label count, per-label length,
   traversal visits and aggregate descriptor capacity. At minimum a content-backed
   `error` descriptor must fit its kind bytes, label String slots and label text;
   allocator overhead is already priced by the envelope. This prevents funding
   an attempt whose mandatory restrictions make every later report inadmissible.
   Actual reports repeat inheritance, descriptor and custody checks before spending
   their loan. These checks do not fabricate a payload or invoke a validator.

2. **Fund once through Ordinary admission; spend through the checked owner.**
   The pool grows only by securing parent backing. New responsibility cannot use
   protected Completion headroom to escape Ordinary admission. Later verification
   and report construction use the same already-funded source in their derived
   Completion lane, including when ordinary parent capacity is exhausted.
   Issued pages and custody tokens retain their actual debits; their final drop
   refunds the pool, independently of whether the logical grant has ended.
   Pinned old pages therefore cannot silently make future retained writes unfunded.

   `prepare_with_custody` resolves exact request retries first, then checks actual
   report authority, attempt, provenance and input references before content I/O.
   It selects the matching grant and pinned schema, verifies submitted bytes through
   `ContentStore`, and builds the report against the full effective pending root.
   A trusted embedding may supply already-verified custody through
   `prepare_evidenced_with_schemas`; that capability remains exact-request and
   descriptor bound, and its schema must still match the grant. No worker, script,
   agent, skill or tool is launched by either path. The participant performs the
   validation and submits its proof or diagnostic evidence.

3. **Protect discrete records and mutable parent bounds as well as bytes.**
   Each remaining report reserves one artifact/content-identity pair, accepted
   result, request outcome and sequence, plus three base event rows. A Required
   Admission grant also reserves its possible PostFailed prefix and complete
   affected graph/monitor/cohort event and row shape. Increment grants reserve
   the three-event report path; they cannot spend an
   Admission parent-failure allowance or turn an Increment failure into PostFailed.
   Core increments `Meta.events` by the actual `NativeOutcome.events`; a three-event Observe report
   does not consume a fourth event. Every candidate checks actual effective counts
   plus the book's remaining promises, including earlier pending candidates.
   Ordinary commands cannot consume those reserved artifact, result, event or
   outcome allowances. Checked arithmetic also protects total-row growth and
   sequence exhaustion. Candidate serials have a separate remaining-report margin
   because discarded tickets are never reused. One additional outcome, sequence
   and ticket is kept for an authority control while grants remain live; this is
   not a universal RAM reservation for an arbitrarily large cancellation closure.

   Preserve evaluation revision space for remaining attempts. While an active
   Required grant can still fail a Posted parent, preserve its failure revision
   and the component's possible monitor/graph revisions too. Check every affected
   protected parent's immutable policy, full
   registered cohort, bounded scope/ownership growth and charged row size before
   accepting ordinary changes. Receipt progress may change the parent's phase;
   it does not revoke an otherwise eligible begun Admission report. The parent
   envelope prices all `max_responses` history records and future evaluation
   registrations, including allocator charges and existing spare capacity floors.
   The shared target count covers Admission, Delivery, Slot and Increment; at
   Admission Begin its upper bound is capped by the immutable registry hard cap.
   First responsibility separately requires the full authored count to fit that
   cap. Full history and the bounded registry must fit the entry ceiling; neither
   is clipped to make Begin succeed. Only additional scope growth may use the
   remaining entry space. Later checks pin the policy, response limit and registry
   cap, require registration counts between the original and maximum, and enforce
   actual charged heaps. The book also checks each grant's saved append ordinal
   and exact key; count bounds alone cannot prove the original cohort survived.
   This protects begun Admission reporting across response/history/registration
   growth. The evaluation grant itself does not fund respondent work; the separate
   receipt grant below funds its mandatory diagnostic, close and post. Claimant
   Receipt results and optional output payloads remain separately admitted.

4. **Journal provisional credit with the one candidate chain.** Each grant is
   keyed by exact evaluation identity/generation and carries its current binding,
   remaining report allowance and possible failure allowance. Begin installs a
   provisional grant; Report advances or terminates that grant only after building
   the actual checked candidate. Cancellation and supersession retire grants from
   actual evaluation AuthorityFenced events. Ordinary PostFailed alone leaves
   eligible begun sibling and Observe grants live.

   A journal retains the prior credit totals and any replaced grant buffer, so
   rollback requires no new allocation. Exact pending retry returns the original
   ticket; committed retry returns the retained outcome. Neither reserves another
   grant, re-verifies custody nor consumes another report. Head publication seals
   its journal without reapplying a state already advanced by later candidates.
   A Core publication refusal keeps the identical candidate and journal. An
   internal bookkeeping inconsistency faults the owner; it must not admit fresh
   work as though reconstruction had succeeded.

   Discard drops the suffix tail first: prepared pages first, then the matching
   journal. Restore credits, prior metadata buffers and discrete allowances before
   further admission. A Begin journal records exactly the backing it added;
   rollback returns that amount even with earlier pending journals, without
   returning capacity reused from an earlier grant. General excess remains held
   while any candidate is pending, including candidates with an empty journal.
   An unresolved append or client timeout is not authorization to discard.

5. **Earlier shared-workspace policy; unchanged ownership reconstruction.** At
   this milestone the book kept a workspace high-water mark while any grant
   remained live, resetting only when the live count became zero. Section 6.8
   replaces that conservative maximum with the exact indexed live maximum. This avoids scanning the
   entire book on each terminal report. General idle backing is trimmed only with
   an empty pending queue, after preserving all remaining persistent promises and
   that workspace. A steady stream of speculative work can retain peak backing.
   Book metadata and schema buffers are separately charged. Grants have no
   per-evaluation shared ownership wrapper or counter pool; lifecycle truth remains
   in the single native Core.

   `NativeOwner::new` uses built-in schemas; `with_schemas` accepts the trusted
   registry needed by retained declarations. Ownership transfer scans the actual
   Core, resolves every begun nonterminal unfenced evaluation through its stored
   registration and complete declaration, computes remaining attempts, pins schema
   contracts and funds the resulting book before accepting new work. This includes
   eligible reports after receipt or ordinary parent failure. Failure returns the
   original Core unchanged. A fresh incarnation does not import detached pending
   candidates or old tickets. This is reconstruction from an already-owned RAM
   Core, not a native checkpoint decoder or crash-recovery protocol.

**Index limit at the initial milestone, replaced in §6.8.** The first grant index
used a charged, sorted `Vec`. Lookup is logarithmic, but insertion and removal move O(N) grant
entries; completing or replacing a large cohort can therefore cost O(N²) across
the cohort. This is an explicit limit of this implementation, not evidence of the
global-scale objective. Replace it with a uniquely owned balanced tree over
fallibly allocated indexed slots. Keep integer child links and subtree workspace
maxima, charge new slots/buffer growth before Begin, and make report updates,
removal, rotations and journal rollback allocate nothing. Parent-cohort lookup
must cost O(log N + cohort), with capacity/fault and head-commit/tail-rollback
qualification. Reusing a container that allocates infallibly or adds a shared
allocation per evaluation would not satisfy that replacement contract.

Admission/Increment RAM report ownership and the respondent receipt grant are
separate credits in one owner pool. The latter funds the mandatory diagnostic,
authored close and post described below; receipt acquisition still creates no
testimony. Native WholeWork and Posted Required Admission have bounded graph and
monitor completion envelopes. Optional successful output payloads and claimant
audit closure use separate ordinary admission. Claimant receipt, evaluation entry
and complete whole-claim/control responsibility reservations remain required.
Independently reserve
payload staging/sealing, WAL growth, checkpoint coexistence and required replica
placement before claiming durable completion capacity. Disk failure or unavailable
quorum cannot produce a success acknowledgment. Native durable codecs, replay and
import, shared WAL/Ready integration, decoder/quorum activation and Session/CLI/MCP
dispatch remain open. Existing V1 bytes and replay semantics are unchanged.

Qualification uses actual owner chains, checked ContentStore custody, exhausted
ancestor budgets, pending and pinned retention, exact retry, failed funding and
rollback, late siblings, control ordering, reconstruction and finite-counter
boundaries. The relevant source is in the native completion-book, owner-completion,
schema, envelope and event-budget tests; completed run evidence belongs in
[09](09-implementation-status.md).

**Posted Required Admission graph completion is implemented in RAM.** The
[dedicated envelope](../../crates/focal-core/src/native/completion_admission_graph.rs)
uses the actual indexed component before Begin or owner reconstruction accepts
responsibility. It includes immutable incoming/outgoing dependencies, active
monitor roots and subscribers, ownership/lineage membership, complete registration
bounds and every affected cohort. The shared
[graph ceiling](../../crates/focal-core/src/native/completion_graph.rs) prices source
copies, repeated captures, up to `M + L` graph/monitor revisions, reverse-link
writes, original journal validation, automatic seal construction, range publication
and discrete slots. Retained capacity includes one complete failure transaction
plus all remaining ordinary attempts; retry and quality history cannot spend that
one-time allowance twice. The complete failure shape must fit the configured
transaction limits before the evaluation becomes Begun.

The [failure capability](../../crates/focal-core/src/native/admission_graph.rs)
consumes an actual `Posted → PostFailed` model decision with the exact registered
report, artifact provenance, evaluator, accepted result and original cut. Its
history prefix is stable: Artifact ordinal 0, Evaluation Reported ordinal 1,
Accepted ordinal 2 and PostFailed ordinal 3. Frozen graph consequences follow,
then the independently checked cohort suffix. A monitor on the failed parent may
release after a dependent becomes terminal, advancing that parent's final binding
again. The retained proof preserves the original failure binding and Required cut;
`CompletionUse::AdmissionFailure` authenticates that proof instead of assuming the
final parent is exactly one revision newer than the source. No extra result or
participant authorization is inferred from graph changes.

All actual report, claim, dependency, monitor, index and seal rows publish in one
candidate. Their pending credit journal supports exact retry, publication and
suffix discard. Other reports keep their original three-fact evidence shape:
nonblocking attempts, retryable Error and eligible begun reports after receipt or
terminality do not create another failure cut or graph pass. They do not rescan
later response cohorts or manufacture testimony.

The [indexed growth check](../../crates/focal-core/src/native/completion_growth.rs)
is shared with Work grants. Even an isolated Posted Required parent installs
protected membership. New incoming dependencies, monitor endpoints and owned
membership are checked against all directly affected held promises before the
candidate becomes accepted. A source cannot shrink the original protected union
to conceal an unfunded endpoint. Current incompatible growth is refused; accepting
such growth in a future extension would require atomically backing the enlarged
promise first. Reconstruction rebuilds the same contract from actual retained
Core state and refuses without losing that Core when funding cannot be acquired.
This is a RAM owner seam, not native checkpoint or restart decoding.

Qualification of this increment belongs in document 09. Its actual-source tests
must cover Required failure under exhausted ancestor RAM, preserved earlier Error
artifacts and Accepted ordinal 2, dependency failure and monitor release, a later
monitor revision on the reporting parent, rollback of all effects and held credit,
protected topology growth, late report-only evidence and owner reconstruction.
Whole-claim responsibility still needs reserved claimant receipt/evaluation entry,
control, audit, timer and owner-release capacity, including disk and replica resources.
Native codec/import, WAL/Ready, Session/quorum and CLI/MCP activation remain required.

#### Respondent mandatory reporting

**Implemented native RAM and finite-record contract; qualification is recorded
separately in document 09.** [RespondentEnvelope](../../crates/focal-core/src/native/respondent_envelope.rs)
quotes one actual claim/receipt/epoch, using the immutable response allowance,
slot policy, descriptor/verifier contract and future parent/registry heaps.
[Respondent state](../../crates/focal-core/src/native/respondent_state.rs) derives
three independent credits from actual indexed rows: one Work-reason diagnostic
per remaining cycle unless the current cycle already has one, one authored close
per remaining cycle, and one post for each future close plus every existing
Generated response of the current receipt. Reconstruction follows the complete
response chain, exact recorded report stamps, receipt allocations and cycle rows;
it never substitutes an attempt counter, current status or newest response for
that retained history.

`AcquireReceipt` and `AdoptReceipt` acquire this backing through Ordinary admission
before their candidates are accepted. [Respondent bookkeeping](../../crates/focal-core/src/native/respondent_book.rs)
uses the existing indexed `CompletionBook` and its one expandable funded pool,
with inline credit and pinned verifier values. There is no new pool or `Arc` per
receipt. The [owner adapter](../../crates/focal-core/src/native/respondent_owner.rs)
selects a loan only for the authenticated current holder's exact operation.
The reserved diagnostic path uses the pinned builtin error-report schema and
`EvidenceFailure::Work`; other evidence is ordinary admission. No entitlement
authorizes Focal to supply a response, outcome, confidence, summary or diagnostic
on the respondent's behalf.

Let `W` be the immutable slot count capped by the checked closable work bound and
`D` the admitted diagnostic count. The quote covers complete manifested and failed
work, all cycle diagnostics and the bounded authored summary. Diagnostic, close
and post require at most 8, `2W + 7` and 6 changed keys respectively. Their separate
storage envelopes include direct rows, complete parent/registry and response
copies, event containers, retained neighboring range pages and directory paths.
Every possible action retains its own range-write allowance while old snapshots
or pending candidates remain pinned; one shared workspace covers serial input,
verification, construction and range transients. Discrete promises include the
diagnostic artifact/identity, response rows, outcomes, events, sequences and new
unique rows. No successful output payload is reserved merely because its slot
exists.

Optional work and diagnostics must preserve these limits before publication,
including the first unspent Work diagnostic slot. The owner checks actual input
capacities, current work attachment revision margins, every remaining close/post
claim revision, future response history and registry sizes, and the retained heap
of existing Generated reports. The counted respondent reader checks
`3 + 4R + 3W + 3D` against its allowance using the full authored response count `R`.
Journal checks use complete final events, including automatic cohort suffixes;
the conservative multi-update bound is `9E + 2N` for `E` events and `N` changed
respondents. Individual source readers, construction and journal stages retain
their own checked limits; this does not assert a shared transaction-wide visit
counter.

Actual publication consumes or retires the corresponding credit once. A close
can leave older Generated reports unposted, so it cannot retire their posting
allowance. Adoption atomically retires the old receipt credit and funds the new
one; terminal/local-complete claims and authorized controls retire only their
actual respondent entitlements. Work/Admission reports that terminalize graph
peers also reserve the retained multi-respondent journal, on every Work report
or the one Admission failure respectively. Rollback drops prepared pages before
restoring earlier credit and refunds newly added pool/index backing; a head
commit preserves later pending advances. Evidence and original response history
remain retained independently of entitlement retirement.

The guarantee is accounting capacity for these admitted RAM operations, not
physical preallocation or a promise that external execution, schema custody or
allocation succeeds. Refusal preserves the source and retryable responsibility.
Full claimant receipt/evaluation-entry, whole-claim and control closure remain
separate funding requirements. Payload staging, disk/WAL/checkpoint coexistence,
replica quotas and durable native codec/import/activation remain open. Existing
V1 bytes, replay interpretation and live CLI/MCP behavior are unchanged.

### 6.8 Indexed completion ownership and bounded cohort checks

**Implemented source; qualification evidence is recorded separately.** The private
[CompletionIndex](../../crates/focal-core/src/native/completion_index.rs) replaces
the sorted grant vector described in §6.7. It is a uniquely owned AVL tree over a
fallibly grown slot buffer. Grants retain one owner-controlled resource authority;
this change introduces no lifecycle mirror, per-grant shared ownership wrapper,
new wire tag or durable representation.

1. **Keep slot identity stable while balancing.** Each occupied slot owns its
   grant, lookup key, integer parent/child links, height, local workspace weight and
   subtree maximum. Rotations change links rather than exchanging grants. Removing
   a node with two children transplants its successor's slot into the tree position,
   repairs from the successor's former parent and preserves every survivor's slot
   index. A doubly linked free list allows immediate slot reuse without shifting
   the remaining grants or scanning vacancies. Slot indices are private process
   facts; they are neither object identities nor stable memory addresses.

   Lookup, weight updates and deletion visit O(log N) nodes. Insertion into an
   available slot also costs O(log N). Buffer growth remains an occasional O(N)
   move with separately charged old/new buffers; it is not a constant-time Begin.
   A parent-cohort cursor seeks its lower bound and traverses O(log N + G) nodes
   using parent links and constant cursor storage, where G is that parent's grant
   interval. No generic separator-key copies or recursive traversal stack are
   required.

2. **Use the exact live workspace maximum.** An active grant's node weight is its
   pinned temporary report-workspace bound; a provisional terminal/fenced grant
   has weight zero while its journal remains retained. Each path repair recomputes
   the subtree maximum from the node and its children. The root therefore supplies
   the current exact maximum without a whole-book scan. Report continuation,
   retirement and rollback update the affected paths. This supersedes the
   high-water retention rule in §6.7; retained persistent report promises and actual
   issued-page debits are still accounted independently.

   A lower logical maximum does not authorize returning backing needed by an
   earlier pending state. General pool trimming still requires an empty pending
   queue. Tail rollback restores the original credit totals and indexed weights;
   only the exact Begin-added backing is returned while earlier candidates remain.
   Publication retains any actual page/custody debit through its final release.

3. **Retain exact growth ownership through commit and rollback.** Before allocating
   a replacement slot buffer, reserve its complete capacity and allocator charge
   while the original buffer remains charged. Reconcile actual capacity before
   moving entries. The growth journal owns the emptied original buffer, its
   allocation, owner identity, original slot length and replacement capacity.
   Failed funding restores the original buffer; head commit releases only the
   superseded empty buffer and its charge.

   Tail rollback first drops candidate pages and removes that Begin's grant.
   Check that every newly appended slot is now vacant, unlink only those trailing
   vacancies from the free list, then move the surviving prefix back into the
   retained original buffer. Preserve committed earlier removals and their free
   links; never restore an obsolete tree root or resurrect a retired grant.
   Nested growth journals unwind in reverse order. This path allocates nothing,
   although a growth rollback moves O(N) retained slots. Normal capacity refusal
   must preserve the index, values, root accounting and pending journals exactly.

4. **Check an affected parent's complete grant cohort once.** Registration rows
   retain append order; they are not sorted by `EvaluationKey`. Begin and ownership
   reconstruction pin the actual matched registration ordinal in each grant.
   For a changed parent, traverse the actual index interval and require every live
   grant's saved ordinal to resolve to its exact key in the current registry.
   Iterating only caller-supplied or remaining registration rows would miss a
   dropped cohort. Saved ordinals additionally reject removed/reordered rows and
   stale generation/target keys. Existing checked registration and reporting still
   enforce full definition stamps, target content/revision and receipt identity.

   [ParentFacts](../../crates/focal-core/src/native/completion_envelope.rs) validates
   the actual registry and computes the policy fingerprint, binding, scope limits,
   charged claim/registry heaps, membership count, response count, declared response
   limit and Posted phase once. Its borrow is tied to the immutable source claim and registry. Every grant
   then performs scalar envelope comparisons and one ordinal lookup; it cannot
   rehash the policy or walk the parent's nested buffers. The parent check costs
   O(D + G) after the index seek, where D is the work to inspect that parent's
   policy and owned-buffer accounting. This includes the conditional Required
   failure revision margin and all retained-size guards.

   A batch can emit several ChildRegistered facts for the same parent. Check its
   grant interval only at the event whose `after` binding equals the actual final
   retained claim binding. Earlier intermediate revisions remain history; they do
   not trigger repeated cohort scans. The immutable candidate's complete event
   construction remains responsible for emitting that final fact.

5. **Refuse inconsistent private indexes without treating missing links as empty
   cohorts.** All downward searches and upward repairs have a height-based walk
   bound. Missing occupied links, impossible root/length relationships, broken
   reciprocal parent links, invalid free links or exhausted walks poison the index.
   Cursor output also enforces a bounded count and strictly increasing keys.
   A truncated or poisoned cursor cannot establish complete membership.

   The book checks health after cohort traversal and maximum use, and before
   trimming; the owner checks it before fresh work. A bookkeeping failure after
   an impossible private mutation must remain a typed, fail-closed owner error,
   not a success or a claim that rollback restored corrupted state. No recovery
   path may use a zero maximum substituted for a missing subtree to release
   promised capacity. These guards supplement algorithm qualification; they are
   not a durable decoder or automatic repair mechanism.

**Required structural and owner qualification.** The
[index tests](../../crates/focal-core/src/native/completion_index_tests.rs) specify
an ordered-map oracle for inserts, removals, weight updates and lower-bound scans;
all rotation forms; two-child deletion preserving survivor slots; nested growth
rollback across earlier committed deletion; exact buffer/debit refunds; and
capacity refusal. Node-touch counters must demonstrate logarithmic operations at
increasing populations and cohort traversal proportional to the selected interval.
Exercise sustained churn within existing capacity, with no growth allocation or
per-grant sharing added. Inject invalid private links to verify typed refusal,
bounded termination and poison propagation rather than silently accepted maxima
or incomplete cohorts.

Owner qualification must additionally retain later speculative updates across
head commit, restore exact resources on tail discard, enforce ordinal membership
and once-per-final-parent checks, and continue actual custody-backed reporting
under parent pressure. Envelope tests compare shared parent facts against the
same identity, policy, registry, revision and real retained-size guards. Executed
results and measured bounds belong in [09](09-implementation-status.md); source
presence here does not claim those runs have completed.

This removes the grant vector's repeated O(N) shifts and the workspace high-water
policy. It does not establish global workload throughput, independent disk/replica
reservations or native durable recovery. The separately integrated respondent
reporting contract is described in §6.7. The
remaining native object transactions, codec/import, WAL/Ready and quorum activation,
and Session/CLI/MCP dispatch gates in §§6.4 and 6.7 remain in force.

### 6.9 WholeWork owner projection and funding

**Owner projection, explicit claimant entry and funded native WholeWork
Begin/report are implemented in RAM. Durable and Session/CLI/MCP activation remain open.**
The [bounded model projection](../../crates/focal-model/src/lifecycle/aggregation_projection.rs)
borrows actual retained sources through the
[native owner adapter](../../crates/focal-core/src/native/projection.rs).
Response rows retain original claimant-receipt and WholeWork-entry coordinates.
The native `EnterWholeWork` command publishes the claimant's request, exact
response/work entry, structural MissingSlot outcomes, zero-check consequences,
claim acceptance and indexed immutable dependency effects atomically. An explicit
event journal preserves all intermediate transitions while storage retains the
final rows. Its precharged object index now checks complete work/response revision
chains, exact missing-evaluation/result correspondence and original publication
positions before the final write. `BeginWork` and `ReportWork` add exact designated
evaluator authority, held completion capacity, complete result history and derived
object consequences. An acceptance read remains observational. Final audit closure
remains required.
Immutable incoming graph links are indexed at creation. Native active monitors
now have direct root reverse membership and frozen consequences (§6.12), including
expanded Work bounds. Items 1–4 below describe the projection/entry contract;
items 5–6 describe the held-resource contract. Posted Required Admission now
shares the graph ceiling and protection index (§6.7). Remaining control/funding
and durable activation gates are distinct from those implemented report paths. Document 09 records executed qualification. Preserve V1
decoding, execution and bytes throughout this sequence.

1. **Retain exact entry and internal-result positions.** Extend the native
   [response row](../../crates/focal-core/src/native/response_owned.rs) with an
   owner-assigned WholeWork entry sequence and ordinal, set atomically with
   Received → Validating and preserved by every copy. A terminal cut cannot
   substitute for this entry position: a still-pending response has none, while
   missing Required slots and present zero-check slots have consequences at entry.
   Delivery rows now retain their original sequence and event ordinal; preserve
   them when adding artifact-free MissingSlot accepted rows. Check role,
   declaration, target, generation, receipt and response
   report identity; do not fabricate an external attempt, reporter or artifact.
   The Admission publication guard's `has_begun` requirement cannot be reused for
   these internal outcomes. A missing slot with no check uses its immutable
   presence declaration index and entry fact, without inventing an evaluation.

2. **Resolve the complete owner source at one effective prefix.** Build a native
   projection adapter over committed rows plus the actual prepared overlay. Walk
   `ClaimState::latest_response` and each response's `prior`, checking the complete
   bounded chain, cycles, receipt and report identity. Resolve every immutable
   declaration, registered evaluation, manifest work row and accepted result by
   exact identity. Derive the expected cohort from declarations and eligible
   responses; scanning only existing registrations cannot detect omitted members.
   Required Increment targets must be sealed and have final outcomes before
   WholeWork entry. Enumerate the complete actual work membership, including
   unclosed outputs, to detect omitted Increment registrations. A sealed empty
   eligible-output set is ready: required output presence is assessed by WholeWork
   slots. An unsealed set is not ready. Coverage may use a response only after all its Required pure
   Receipt checks supply real Pass facts.
   Receipt Pass is not an additional WholeWork entry prerequisite.
   Use owner lookups, not participant-selected subsets or a global event scan.

3. **Build a bounded borrowed proof projection.** Add a model projection beside
   [aggregation](../../crates/focal-model/src/lifecycle/aggregation.rs), borrowing
   the canonical policy and definitions. An allocation-free preparation plan must
   validate counts and quote the complete construction peak before allocating.
   Price response rows, response/declared-slot cells including absent and zero-check
   slots, result references, chronological facts and flat witness/cause buffers.
   Counts based only on evaluation rows omit zero-check slot work. Include allocator
   metadata, reconcile actual capacities against the precharge, and reject growth
   before filling. Keep permits alive through consumption of the proof buffers.
   Do not copy a complete policy/registry for every response or retain a second
   mutable aggregate. Private-backed proof views must share the existing model
   guards in `WorkArtifact::apply_aggregate`, `Response::plan_aggregate` and
   `ClaimState::apply_aggregate`; callers cannot manufacture acceptance flags.

4. **Reconstruct chronological acceptance, not a latest-state verdict.** Combine
   exact response-entry facts with immutable terminal-result publications; validate
   retained nonterminal results without treating retryable Error or intermediate
   Pass as acceptance. Terminal results cannot be replaced, so old intermediate
   attempts need not be copied into the acceptance timeline. Batch equal sequences,
   preserve publication provenance and reject conflicting complete cause keys.
   Derive artifact and response consequences before claim coverage; simultaneous
   complete coverage precedes an uncovered blocking cause. Select causes using
   document 17's canonical target/declaration/generation/attempt/phase order.
   Compare reconstructed original cuts with retained terminal rows. A later
   response cannot heal an earlier claim cut; an already-failed response remains
   failed even when an alternative covers its slot. Unrelated begun work may still
   finish and supply a different slot of an open claim. No entered response means
   claim acceptance remains Pending, including zero-check and Receipt-only
   policies. Never repaint severity.

5. **Generalize completion credits before exposing a funded WholeWork Begin.**
   [CompletionEnvelope](../../crates/focal-core/src/native/completion_envelope.rs)
   and [CompletionBook](../../crates/focal-core/src/native/completion_book.rs)
   now aggregate checked envelope-derived remaining slot vectors through spend,
   retirement, reconstruction and rollback. The book no longer infers event or
   row counts from the number of reports. Increment retains its report-only
   demand; Work reserves its complete remaining chain, and Posted Required
   Admission reserves its full one-time graph failure shape (§6.7).
   Price every attempt's artifact/evaluation/result
   writes plus possible work, response and claim changes, events, outcomes and
   copied neighbors. One parent-failure surcharge cannot cover transitions that
   occur on different reports. Loan selection and spending now use typed
   `CompletionUse::{Regular, AdmissionFailure}`. Only an actual ReportAdmission
   Posted → PostFailed transition with its exact Required Admission cut may spend
   the one-off surcharge. Its retained proof distinguishes the original failure
   revision from later monitor revisions on the same parent; final-row equality
   with `before + 1` is not required. Other report families use Regular; WholeWork must
   price its complete effect shape on every Regular attempt. Separately admit the first-entry transaction: it
   may change every manifest artifact, create internal missing-target results and
   reduce the response and claim before any external report. Bound its full batch,
   projection work and retained bytes; do not charge it as an ordinary retry.

   Explicit claimant entry and evaluator-first entry are implemented. A designated
   peer's successful Begin can enter a Received response in the same admitted
   transaction. The first-entry effects use ordinary admission, while its future
   reports receive a held completion grant before publication. Consume the actual
   begun evaluation with `ClaimState::observe_evaluation` and
   `Response::plan_evaluation`; do not construct a claimant Principal on behalf of
   the evaluator. Derive any other work-entry and structural-absence effects from
   a private capability for that checked response entry. The
   `WorkArtifact::observe_evaluation` authorizes only the begun evaluation's exact
   artifact, and `settle_missing` authorizes an explicit claimant; neither alone
   grants blanket actor authority over the other slots. The model now provides
   `ResponseEntry`: the owner obtains it from the checked Received → Validating
   plan after checked claim advancement and a refreshed acceptance decision.
   It pins the exact resulting response and current claim bindings, report stamp,
   receipt, cycle and respondent. `WorkArtifact::begin_entered` and
   `Evaluation::settle_missing_entered` consume it without a Principal argument;
   the latter preserves explicit fences/seals and creates no external attempt.
   Native claimant and evaluator-first entry use these APIs for all derived effects.
   A low-level Core call cannot bypass managed funding for BeginWork or ReportWork.
   Preserve the
   same chronology, all-slot structural assessment and exact entry/result cuts as
   explicit claimant entry. Fund all such initial effects before recording Begin.

6. **Discover consequences and preserve future growth bounds.** The receipt
   closure follows outgoing dependencies, active scope roots and owned children;
   it does not discover all incoming dependents or monitors affected by completion.
   Native creation now maintains deduplicated incoming DependsOn/Awaits links and
   explicit claimant entry discovers the bounded affected closure through those
   links, outgoing declarations and exact owned/lineage membership. It publishes
   checked dependency failure and fixed-point graph release with the root's local
   outcome. Native scope writers now retain direct root reverse membership and
   publish repeated frozen monitor/graph consequences (§6.12). These Work paths
   price their affected rows and history; remaining family paths and future native
   import must use the same complete source and capacity checks. Larger work
   needs an explicitly retained, funded continuation. Later response,
   registry, coverage-witness, audit-cohort, scope or graph growth must remain
   inside held grants' byte, visit, write and discrete-slot envelopes, including
   later ordinary creation and registration. Refuse incompatible growth or reserve its
   added obligation atomically; successful earlier Begin must not become unfunded.

   The owner now retains a typed reverse protection index from every member of a
   Work grant's checked component to that exact evaluation key. New claim creation
   checks its dependency targets and lineage parent as well as its own row, so an
   incoming link cannot enlarge a protected component unnoticed. Other native
   facts identify their owning claim. The complete affected cohort is discovered
   through those indexes; temporary scalar marks deduplicate checks and are reset
   for the whole affected cohort before each candidate. No ledger-wide or grant-wide
   scan is needed. Every check runs serially inside held completion workspace.

   Work grants pin complete graph membership and immutable content, while pricing
   each mutable member's full authored response-history and registration capacity.
   Incompatible topology is refused before candidate acceptance; automatic added
   funding is not implemented. Protected membership survives pending retirement
   until publication and rolls back with Begin, including geometric index-buffer
   growth. Already-begun checks remain funded after ordinary parent completion or
   failure; explicit fences retire the corresponding grants through the existing
   journal. Each active grant preserves future terminal revision capacity in its
   work/response and affected claims.

7. **Qualify in dependency order.** First test entry provenance, complete-cohort
   refusal and real artifact-free outcomes. Then test missing and zero-check slots,
   multiple Receipts, pending/failed Required Increment gates, complementary-artifact
   false passes, simultaneous cause permutations and alternative-before-failure
   versus failure-before-alternative. Exercise late audit results without terminal
   repair. Finally exhaust parent RAM after Begin across retry/fallback/quality and
   staggered work/response/claim effects; verify exact retries, pinned pages, every
   allocation failure, pending-tail rollback and refused growth leave original
   state and credit intact. Run the no-panic, strict Clippy and workspace gates.
   RAM qualification does not activate native codecs, WAL/Session/CLI/MCP dispatch
   or establish disk or replica completion reservations. Respondent closing uses
   the separate receipt-bound RAM contract in §6.7.

#### WholeWork funding implementation checklist

The model's checked `ProjectionQuote` and native `NativeProjectionQuote` now
implement the following shape, construction-byte and counted-visit bounds. The
native projection shares one lookup allowance across inspection, reduction,
nested work cursors and staged-row searches. Graph execution also consumes a
checked preflight over its complete actual indexed closure. The native Work
completion envelope now reserves these costs and guards later growth before
publication; these are internal accounting bounds, not new required user settings.
Durable and Session/CLI/MCP activation remain separate. An overflow or a bound exceeding the owner's
allowance must refuse Begin before it creates an attempt.

1. **Pin the eventual projection shape.** Let `r` be the promised maximum responses,
   `w` maximum indexed work rows, `s` immutable declared slots, `d` declarations,
   `e` maximum registrations and `k` total checks across the slot policies. These
   use legal future occupancy, including failed and unclosed work, rather than
   currently present evidence. The response promise must cover the current count
   and cannot exceed the authored maximum. A smaller promise is valid only if
   the owner guards later growth or atomically adds funding; the estimator itself
   cannot establish that guarantee. Define `t = r*s`, `h = r+e`, `a = e+t`, and
   `tri(x) = x*(x-1)/2`, with `tri(0) = 0`. The existing projection allocation
   quote must cover `Counts { responses: r, slots: t, results: e, events: h }`.
   Its eleven buffers include both final borrowed proofs and all simultaneous
   temporary state; charge allocator metadata and the inline projection too.

2. **Price visits separately from bytes.** A conservative bound for the current
   source inspection's explicit visit counter is:

   ```text
   (d+s+k) + d*(d+1)
   + r*(1+s*(w+1)+d*(e+1)) + tri(r)
   + e*(2*d+7+w+r) + 3*tri(e) + e + d*(e+1)
   + w*(2+r+s+d*(e+1)) + tri(w) + d
   + r*(1+e*(d+4))
   ```

   A conservative bound for the current reduction's explicit visit counter is:

   ```text
   2*r + t + e*(d+r+3) + h
   + h*(2*r+2*t+e*(a+1)+s+(3*d+k)*(e+1)+a*(s+1))
   + t*a + d*(e+1) + 2*e + s + k*(e+1) + r + r*t
   ```

   Across all chronological groups, a dirty started response is reduced at most
   `h` times total: each such reduction needs an entry or result event. A slot
   produces at most one terminal response cause, bounding the total cause-list
   insertion work by `t*a`. Preserve these invariants when changing the reducer.
   These expressions cover the counted visits, not sort comparisons, descriptor
   validation, range-tree descent or native membership lookups.

   Native lookup accounting uses `c = t+e+w+1` cursor starts, including constructors
   for partial or empty duplicate-prefix scans, and `X` maximum staged extra rows.
   Across inspection and reduction, direct source lookups are bounded by:

   ```text
   D = d + 9*e + 3*r + tri(r) + 2*t + 2*tri(e) + 3*r*e + w + w*r
   ```

   The complete adapter lookup ceiling is:

   ```text
   2 + c*(2+2*r+8*w) + (D+c*w)*(1+X)
   ```

   The initial two lookups cover the claim and registry headers. Each cursor
   charges membership and provenance lookups before access; its mapped work reads
   and every direct source lookup also charge each examined overlay row. All
   cursors share the same owner-local scalar allowance, so nesting cannot renew
   it. Exhaustion is sticky and surfaces as Capacity before the owner interprets
   an infallible source callback's absent value or invokes the acceptance callback.
   Range-tree descent and uncounted model helper/sort comparisons remain separate
   bounded costs. The quote does not reserve bytes, pin evidence or authorize Begin.

3. **Price each report's complete effects.** After entry has already published
   all structural/zero-check effects, a report adds artifact, identity,
   evaluation and accepted-result rows. It can additionally replace the target
   work, its response and `g` claims, including the target claim. If each other
   claim changes once and the target may publish LocalComplete then Satisfied,
   the per-report maxima are:

   | Resource | Per-attempt bound |
   | --- | --- |
   | Artifacts / identities / results / outcomes / sequences | One each |
   | Events | `g + 6` |
   | Changed keys and input capacity | `2*g + 14` |
   | New unique rows | `g + 10` |

   These bounds assume the report stages only its target work and response; if
   the implementation restages a larger set, quote that larger set. Retained
   bytes cover every remaining retry/fallback/quality attempt and pinned old
   pages; serialized verification/projection/construction needs one held peak
   workspace. Include the new object-journal index in construction bytes.
   First-entry all-slot transitions and internal missing results are separately
   admitted before recording Begin. An entry consequence may close the claim
   while the newly begun evaluation retains its authorized reporting capacity.

4. **Protect the promise against later mutations.** Add typed Work completion
   bounds pinning response/work identity and receipt, future response heap,
   projection shape/visits, complete affected graph closure and revision margins.
   Reserve one future terminal revision for target work and response, up to two
   local-outcome/release revisions for the target claim, and a terminal revision
   for each affected peer. Ordinary response observations and graph mutations
   must preserve that headroom while a grant is active.

   `CompletionBook::check_parents` visits Claim/Registrations facts.
   Creating a dependent can instead grow IncomingHead/IncomingLink rows without
   changing the funded claim itself. Therefore guard changes to reverse graph
   membership and owned children, discover the affected grants, and check their
   complete proposed closures before publication. Either refuse incompatible
   growth or fund the added obligation atomically. Rechecking only when a report
   arrives is too late. Runtime scope activation needs the same guarantee through
   its own reverse membership index. The Work protection index and candidate guard
   now supply this requirement for the currently enabled native writers.

   The graph preflight now supplies exact-current node, discovery, snapshot,
   witness, row, event and heap ceilings from the same incoming/outgoing/ownership
   traversal and frozen snapshot used by execution. It checks one graph revision
   for each nonterminal member and reconciles actual output costs. Its optional
   prior ceiling rejects a proposed closure that grows even when the root Claim
   binding is unchanged. The Work completion book now installs this comparison
   before admitting affected native mutations. The full report envelope adds non-graph rows, target
   local-outcome revision headroom, range construction and the common object-journal
   index; graph preflight alone is not a complete report reservation.

5. **Qualify the funded path before exposing it.** Exercise maximum legal future
   response/work/registration growth after Begin; new incoming dependencies and
   owned children; staggered Required/Observe outcomes; retryable Error and
   quality phases; terminal parent with an outstanding audit report; revision
   exhaustion; pinned snapshots; exhausted ancestor RAM; and every pending-chain
   rollback. Record exact per-attempt credit consumption and no partial evidence,
   history, row, or grant changes on refusal. Only then enable native Begin/Report
   dispatch and proceed to the separately required durable and CLI/MCP activation.

### 6.10 Native deadlines and remaining audit integration

**Exact native evaluation/claim deadlines and claimant result-bundle
generation/posting are implemented in RAM; guaranteed whole-claim completion
funding remains open.** Keep timeout
authority, business acceptance, cohort sealing and audit completion distinct.
An expired evaluation cannot report, but refusing that report does not itself
record a deadline fence or close the retained audit obligation. The trusted
owner now exposes `prepare_evaluation_deadline` and `prepare_claim_deadline`; future due-timer scheduling,
durable activation and node service dispatch must call that separate ingress.

1. **Implemented exact trusted evaluation deadlines.** The owner derives the due evaluation
   from its actual declaration, target, generation, current binding and trusted
   owner time. A participant-supplied timestamp or Node identity is not a timer
   capability. Keep due-deadline ingress separate from actor mutation ingress,
   preserve request retries and reject early or stale timers before mutation.
   Retain consumed timer generation/target independently of participant request
   deduplication. Expired Ready checks, pure Receipt declarations and declared
   policy requirements need this fence without external report authorization.
   Use the model's checked deadline fence: preserve state and original accepted
   results, append the actual authority-fence event, and invent no Fail/Error,
   artifact or testament. Existing terminal results and earlier fences retain
   their original coordinates. Begin and Report must observe a pending fence.

   Native invocation identity is now an explicit `Request(RequestKey)`,
   `EvaluationDeadline(NativeDeadlineKey)` or `ClaimDeadline(NativeClaimDeadlineKey)`,
   shared by outcomes and event history.
   The timer key pins the full evaluation target/generation and timer identity/
   generation; its separately domain-separated intent also commits the authored
   due time. Fired time is trusted context and does not change exact retry
   identity. The owner resolves and caches the checked replacement once against
   its immutable borrowed prefix before construction. A terminal/fenced no-op
   consumes its timer by writing only Meta and the typed outcome. Actor command
   fingerprints and frozen V1 bytes remain unchanged. No participant command
   variant or synthetic claimant principal supplies this authority.

2. **Implemented bounded funding for Ready and begun deadlines.** An unbegun
   Ready evaluation has no report grant. Its deadline operation therefore cannot
   assume that a completion loan exists. Its bounded checked owner transaction
   uses Ordinary admission, as does timer consumption for a terminal evaluation.
   For a begun
   evaluation, select the exact held grant, stage the actual fence and retire its
   remaining report promises through the existing journal only after successful
   construction. Do not borrow another evaluation's credit or infer available
   bytes from diagnostic budget snapshots. Preserve needed parent/graph effects
   in the same funded transaction or an explicitly retained funded continuation.

   Before native Begin or reconstruction accepts responsibility,
   `CompletionEnvelope::deadline_storage` proves the exact four-put, one-event,
   two-new-row fence fits one Regular report's range, temporary workspace and
   discrete slots. `CompletionBook::deadline_contract` lends that exact grant
   without reporter/schema checks; the actual range plan is checked before
   construction. Single-grant fence retirement uses the existing inline journal
   update with no new Ordinary allocation. Commit/discard retain the same page,
   credit and reverse-membership lifetime rules as reports. Evaluation deadlines
   change no parent/response/work result and produce no Accepted row or artifact;
   subsequent claim expiry and audit publication need their own funded effects.
3. **Implemented deadlock precedence before bounded claim expiry.** For claim deadlines,
   evaluate the qualifying unsatisfied strongly connected component before
   ordinary expiry as required by D-08. The canonical victim can differ from
   the timer's trigger. Apply its checked witness against the same complete
   snapshot and then propagate graph consequences. Account separately for
   `Snapshot::deadlock` reachability/component buffers; dependency-failure
   preparation does not cover them. Capacity, stale-cut or incomplete-closure
   errors cannot authorize expiry. Expired claims fence their evaluations;
   Deadlocked and DependencyFailed claims preserve already-begun report authority.

   `prepare_claim_deadline` accepts the actual authored claim/timer identity and
   trusted firing time; it resolves the current claim revision and full closure
   from the effective owner prefix. The separately fingerprinted typed outcome
   handles exact retries before clock/capacity checks. Terminal claims consume
   the timer without rewriting their cut, evidence or graph. The model's
   [bounded query](../../crates/focal-model/src/lifecycle/graph_deadline.rs)
   distinguishes a checked negative answer from every error, reserves all
   reachability/queue/component buffers before traversal, and preserves the
   original canonical witness fingerprint and deadline provenance. A settled
   self-edge does not qualify as a wait cycle.

   The [native control transaction](../../crates/focal-core/src/native/claim_deadlines.rs)
   applies the actual witness or expiry, complete registered expiry fences and
   checked graph consequences in one candidate. When the victim differs from
   the due trigger, it captures the complete resulting component again and
   resolves any remaining SCC or ordinary expiry until the original trigger
   is terminal. Every round must terminalize a previously live member; later
   witnesses bind the complete staged source at that round and preserve earlier
   terminal cuts. The same typed timer cannot be consumed with the trigger still
   open. Exact redelivery remains a retry, not a new firing or renewal.
   Its dedicated journal validates
   the exact initiating cut, fence membership/order, changed rows and events;
   it cannot inject work, testimony or evaluation results. Multi-grant retirement
   uses the same Completion lane as the bounded control construction, including
   the additional coexisting journal allocation. All successive SCC and graph
   captures are cumulatively precharged and share a finite traversal allowance.
   Active runtime-scope closures now include direct reverse monitor membership.
   A scope release can advance a still-open trigger before its next SCC/expiry
   round; the next witness consumes that actual staged revision. The journal
   retains terminal-owner monitor releases as separate facts, and the common
   index checker validates every associated subscription write (§6.12).

   This is atomic bounded control admission, not a guarantee that every future
   claim deadline fits. The current registry and graph growth bounds can admit
   a closure larger than one control transaction. Expiry writes
   `2 * changed_evaluations + 2 * changed_claims + 2` rows in this profile and
   refuses the entire candidate if any construction, journal, traversal or
   range allowance is exhausted. Before guaranteeing timer completion, reserve
   the complete future claim obligation at responsibility admission and
   revalidate topology/registration growth, or retain an explicitly funded
   continuation with equivalent atomic authority fencing. Price the control
   journal, discrete rows/events, RAM coexistence, disk and replica capacity;
   evaluator report promises cannot silently subsidize arbitrary claim control.
4. **Implemented automatic native seal publication and grant rebinding.**
   `Evaluation::record_seal` advances the evaluation binding even when the
   evaluation has already begun. It preserves that attempt and its reporting
   authority. The completion book now supports an exact before/after
   rebind which preserves remaining reports, descriptor/schema contracts,
   memberships, workspace and retained capacity. Rollback must restore the old
   binding without allocation. A seal cannot consume an attempt, retire an active
   grant or leave its binding stale. Preserve the extra revision margin before
   admitting the responsibility that can require the seal.
   Report advancement and sibling rebinds share one atomic journal. The explicit
   `Sealed` events now come from a separately checked suffix after the complete
   original history. They do not pass through `Extras::evaluation`, whose
   unfenced existing-row change represents Begin. Original object-journal
   checks remain intact; no synthetic Begin is used for a seal.

   Derive a checked seal update from actual before/after evaluation states and
   the retained declaration/cohort cause. Require exactly the next binding and
   preserve current attempt, state, phase, target, generation, receipt, last
   result and fence; only the seal and applicable Ready suppression may change.
   A live grant preserves report count, failure surcharge, schemas, reverse
   memberships and workspace. Ready/terminal members never create grants.
   Fold a report followed by a seal of that same evaluation into one update:
   debit the report once, bind the grant to the final sealed row, and retain the
   result's original pre-seal evaluation revision and publication ordinal.

   `CompletionBook`'s multi-update journal now handles this purpose.
   `Change::Many` removes only retired grants at commit; live rebinds remain.
   A later pending report may already
   have advanced a live rebound grant when the earlier seal candidate commits.
   Commit must preserve that later credit, while suffix rollback restores exact
   before bindings and per-entry workspace weights without allocation. Validate
   all mixed updates before mutation, then return one journal for report advance,
   sibling seals and any real fences. Allocate its vector from the triggering
   transaction's funded source and include its coexistence in the envelope.
   Integrate it after report construction and before owner candidate checks;
   control construction uses the same checked collector. Begin and reconstruction
   retain one additional revision for an unsealed nonterminal member. Existing
   single-evaluation report envelopes also preflight the collector's full visit
   bound: `maximum_report_events + 3 * declarations + slots + 3`. The immutable
   policy fixes this cost for all future attempts. Exact source-root provenance,
   complete registry membership, and Accepted-after-Reported event ordering are
   checked before any credit change. The common collector bounds sorting,
   lookup and all cohort passes; automatic writers must price their larger
   mixed-update visit demand before adding those events.
   Evaluator-first `BeginWork` can itself produce a local outcome while entering
   the full cohort. Its owner branch now sends the actual checked Begin transition
   through the collector and composes the installation with any resulting
   own/sibling updates in one candidate journal. The new grant's original
   funding and reverse-membership journal remain owned until commit/discard.
   The automatic writer uses the complete preparation/envelope profile below.

   Keep the composition bounded: one publication can own at most the existing
   Begin-install journal plus one complete mixed-update journal. Reserve their
   pending representation before accepting the publication; avoid a recursive
   journal or an independently allocated list of journal objects. Check their
   owner identities and adjacent book revisions. On candidate refusal, drop
   prepared pages before rolling back the mixed updates, then roll back the
   installation and return its newly added funding. Commit in publication order
   without overwriting any younger pending report's live credit. Empty journals
   must not create phantom revision steps. Qualify both own-evaluation and
   sibling seals, immediate retirement, later pending reports and suffix discard.

   The collector starts from source evaluation rows and accepts the one external
   Begin only with its owner-created transition capability. For a Begin followed
   by a seal of the new evaluation, it verifies the actual intermediate Begin state:
   the source is Ready while the newly installed grant is already bound to the
   begun revision. The capability borrows the exact original source root and
   retains the authorized complete before/after states, request identity, intent
   and logical time. It survives consumption of the preparation input without
   cloning a root or budget handle. Retain the
   original Begun history before Sealed, and prove both deltas against the same
   immutable source. A participant-supplied binding or fabricated Begun event
   cannot authorize the grant or bypass actor/target/receipt/deadline checks.
   Do not weaken source-state checks to accommodate the later binding.
   The proof requirement applies to every Begun fact, even when it contains no
   attempt. Inactive authorization, if returned by the owner model, travels
   through the same checked path without installing a grant. Structural
   MissingTarget outcomes use their separate event kind; an arbitrary Begun
   event cannot be ignored merely because its final row is still Ready.
5. **Implemented complete cohort pricing and suffix construction.** A Work report's
   original `g + 6` event and `2g + 14` changed-key bounds cover its object and graph
   effects. The extended envelope also covers sealing every registered sibling. Derive the entire
   affected cohort from owner indexes and immutable declarations, including
   Ready, suppressed, begun and already-terminal members. Extend each possible
   triggering envelope with only the actual bounded additional row/event,
   registry, revision, journal and persistent range costs. Revalidate future
   registration growth against these promises. First entry, final Required
   results and explicit controls need separate consequence profiles; no
   unbounded cohort scan may appear in an otherwise funded report.

   The writer runs at the common preparation boundary, after the operation
   has produced its final claim rows and before outcome counts/history/range
   construction. Compare each actual source claim's original `local_sealed_at`
   with its final row; only newly sealed claims create a cohort. This includes
   Admission failure, WholeWork entry/report outcomes and affected graph peers,
   cancellation, supersession and claim deadlines. A late result or later graph
   release cannot reseal the cohort. `Plan` now carries a bounded collection of
   registration overrides for all affected claims, with inline empty/single
   cases and charged buffer growth for multiple claims. Use canonical claim
   order so appends and final merging have linear total movement/traversal.
   Each override must match one final claim row; duplicate and unmatched IDs
   refuse. The collection moves the already-charged registry buffer into the
   final claim instead of copying it again. Future multi-claim writer envelopes
   must include the override buffer's full coexisting growth charges and both
   per-claim policy checks at insertion and final-row validation. Do not copy a
   registry again for each evaluation. Resolve state from the staged override
   first, then the immutable source, so a report followed by a seal preserves
   its real intermediate state and original Accepted coordinates.

   Let `e` be the sum of the checked maximum future registration counts for all
   potentially affected claims, and `g` their existing claim-row bound. A safe
   initial profile adds at most `e` evaluation writes, `e` seal events and `g`
   registration-seal events to the existing transaction; the reporting
   evaluation can share its existing row, but its distinct seal event remains.
   For a baseline of `h0` events and `m0` unique changed keys, and `o` sealed
   evaluations already present in the original extra rows, use
   `h = h0 + e + g` and `m = m0 + 2e + g - o`. With no overlap deduction,
   the original isolated Admission profile grows from four events/eleven changed
   keys to `e + 5` events/`2e + 12` changed keys. The current graph-backed Admission
   profile instead derives `h0`/`m0` from its complete indexed component and monitor
   journal before adding this suffix; the isolated formula is not its full bound. Work reports grow from `g + 6` events/
   `2g + 14` changed keys to `e + 2g + 6` events/`2e + 3g + 14` changed keys.
   These seals replace existing evaluation and claim rows; new-row demand adds
   only the `e + g` history records. Work's per-report new-row allowance therefore
   grows from `g + 10` to `e + 2g + 10`. Here `e` and `g` are conservative
   affected-cohort bounds, not global ledger cardinalities.
   Price the full `SealTransition` buffer (previous and next state), evaluation
   containers, sorted lookup/history buffers and mixed completion-update journal
   while candidate pages coexist. Registry heaps are counted once per rewritten
   claim. Tighten the profile after proving actual overlap; do not subtract an
   event merely because two transitions share one row.

   Admission's one-time Required-failure surcharge must include its complete
   cohort. WholeWork's regular envelope must include every potential affected
   graph cohort. Ordinary entry and controls require their own complete shape.
   For first WholeWork entry, let `p` be the manifest size, `q` the changed
   missing evaluations, `r` the Required missing results and `b = 1` when an
   evaluator's Begin triggers entry (`0` for explicit claimant entry). With
   graph-row bound `G`, the original conservative bounds are
   `p + 1 + q + r + b` extra rows, `G + 2p + q + r + b + 4` events and
   `2G + 3p + 2q + 2r + 2b + 7` changed keys. Add the seal suffix to these
   bounds; do not substitute the smaller report profile. Entry can refuse its
   complete Ordinary admission before accepting new evaluator responsibility.
   Extend held membership/growth checks so later target materialization cannot
   exceed any prior promise. A protected Work component must pin the sum of
   each member's maximum future registry count and declaration/check traversal
   bounds, including Ready evaluation revision headroom. Protecting only its
   identity and current heap ceiling is insufficient. Materialization on any
   protected peer must revalidate this promise. Cancellation, superseding
   creation and claim deadlines fund their own complete control consequence;
   they cannot borrow an unrelated evaluation's report grant. ReportIncrement
   and EvaluationDeadline need no new cohort surcharge while their semantics
   produce no newly sealed claim.

   The existing single-key completion visit quote (`H + 3D + S + 3`) is not a
   multi-cohort quote. Include the second event pass, comparison/move work,
   per-claim registry resolution, full declaration checks and seal proof lookup.
   The collector now requires seal proofs in strict canonical order: complete
   before binding, complete target bindings/slot, then generation. It checks this
   borrowed slice once and uses binary search with one debit per probe; it adds
   no index allocation or empty-slice visit overhead. The writer charges its
   own ordering work and preserves original event order independently.
   Do not reinstate a complete proof-list scan for every evaluation. The mixed completion-update
   buffer remains live alongside prepared pages until commit/discard and belongs
   in each report's retained allowance. It cannot be funded only through the
   maximum shared workspace: several pending reports retain several buffers.
   Admission reserves this with its one-time failure surcharge; WholeWork
   reserves the conservative journal for every remaining report. Construction,
   seal capabilities and page-building scratch still share the serial workspace.
   A shared checked cohort-cost profile drives both preparation and each
   retained completion promise. Traversal refusals identify their visit limit
   separately from insufficient preparation bytes.

   The collector includes structural MissingTarget facts before a later seal.
   It resolves the real source state and exact response-entry publication,
   verifies missing manifest membership and any Required Missing result, then
   checks the intermediate state against the seal capability. The structural
   event's MissingTarget phase is distinct from the evaluation's unchanged
   configured execution phase. This Ordinary entry path grants no external
   attempt and spends no held report credit.

   Existing nonjournaled operations must preserve
   their complete original history before they can append seals; a second evaluation
   event cannot be hidden in the final row's single `fact` field. Keep seals
   internal consequences, with no extra participant command required to make
   an already sealed local outcome consistent.

   Original history is now assembled by the shared borrowed
   `claim_changes::visit_history` visitor. Immediate publication uses this same
   path, preserving the exact ordering of creations, child registrations,
   report evidence and Accepted/Delivery/Missing coordinates, and derived claim
   facts. The visitor owns no output buffer and uses the existing precharged
   claim-history workspace. Preserve the original `Plan` and `Extras` together
   behind private ownership until this pass finishes. A separate checked seal
   suffix may then supply final evaluation/registry replacements while original
   rows move into publication. This streams the original prefix directly into
   final events and needs no duplicate `NativeFact` capture buffer. Full original
   and replacement payloads still coexist and must be charged. Existing explicit
   journal semantic checks run against the original rows; implicit claim chains
   remain checked during the single emission pass. No original payload or fact
   may be edited after that ownership boundary. The suffix must prove its exact
   source/intermediate state, start at the original event count and account for
   every replacement and appended event before publication. Nonjournaled commands
   cannot simply set `Extras.journal` or bypass the original object/claim checks.
6. **Implemented bounded native audit inspection.**
   [RegistrationSet](../../crates/focal-model/src/lifecycle/registration.rs)
   retains the original local-seal position and supplies a checked borrowed
   capability matching the actual claim and immutable policy. An unstamped
   open-claim target seal cannot grant audit authority. The
   [native model plan](../../crates/focal-model/src/lifecycle/audit_native.rs)
   consumes complete evaluations in registry order and accepted history grouped
   by that same order. One forward pass checks exact declarations, targets,
   generations, receipts, contiguous attempts, increasing accepted revisions
   and each member's actual latest result; counted in-place sorting then creates
   canonical audit order. Terminal members need no invented seal revision.
   Construction and fallible copies expose full heap/allocation accounting.
   Complete cohorts reserve exact retained results; incomplete cohorts retain
   the capacity for all admitted attempts. The older `AuditCohort::seal` and
   `EvaluationRegistry` APIs remain available without reconstructing them on
   the native path.

   The [native owner reader](../../crates/focal-core/src/native/audit.rs) gathers
   complete actual membership and all Accepted, Delivery and Missing ranges
   through bounded per-member key seeks on one committed or pending prefix.
   Wrong-family rows, missing retries, substituted artifact IDs/provenance and
   absent or mismatched original event/outcome records refuse inspection.
   Native result keys contain evaluation revisions, so attempts never substitute
   for row addresses. A separate sorted publication index preserves every
   original sequence/ordinal, including legitimate reports after the local
   seal and no later than the captured prefix. Query admission covers both
   simultaneously live source and output buffers, and actual capacity checks
   precede their use. The callback cannot outlive its owned audit buffers or
   release their permits early. Inspection retains no second mutable audit
   registry and grants no future whole-claim closure reservation. The separate
   mutation path below consumes the same checked construction under its own
   transaction allowance.
7. **Implemented claimant result-testament generation and posting.** The
   [native owner](../../crates/focal-core/src/native/audit_bundle.rs) resolves
   the actual claimant and exact claim binding before constructing the complete
   sealed audit. Every begun member, including Observe, must have completed or
   acquired a real fence. Unbegun suppression remains a fact without a verdict.
   `GenerateResultTestament { claim, id }` consumes that audit into a Generated
   bundle. It retains the source's captured prefix, original local-seal cut and
   complete Accepted/Delivery/Missing sequence/ordinal witnesses. The model's
   native audit hash includes every canonical member/result field and the exact
   optional seal-cause hash; the enclosing bundle hash also commits the captured
   prefix and separate sorted publication index. Canonical generation checks
   revision one and consumes the buffers without another sort or allocation.

   `PostResultTestament { expected }` checks the claimant and exact stored
   Generated bundle, copies its owned buffers fallibly, then records Posted at
   the next bundle revision. Content, source capture and original witnesses are
   immutable. Posting does not regather the audit or depend on an unchanged
   parent revision after graph progress. A locally complete claim may close its
   bundle before graph release. The per-claim index admits only one bundle;
   reciprocal checks prevent response/result-testament ID collisions. Exact
   request retries return the original outcome before fresh admission.

   Native storage uses separate `ResultTestament` and `ClaimResultTestament`
   keys/rows, a `ResultTestament` history fact and its own metadata/outcome
   count. Generation has five changed keys (bundle, index, event, metadata,
   outcome); posting has four (bundle replacement, event, metadata, outcome).
   Both have zero changed claim/response/artifact/evaluation rows and retain
   independent Generated/Posted publication positions. The ledger counter grows
   only on generation. Existing per-result artifacts are sufficient evidence;
   no second aggregate artifact or synthetic result is required. These objects
   never replace respondent testimony or enter work acceptance/attachment.

   Both commands use ordinary RAM admission and preserve other evaluations'
   held completion slots. The internal audit builder charges coexisting source
   arrays, cohort and publication buffers to the already reserved construction
   `Scratch`, then moves the final buffers into the owned bundle. It does not
   escape borrowed query funding or duplicate the complete audit. Posting
   precharges the full owned copy, including both accepted-result and publication
   arrays. Final hashing/copying use checked linear `E + R` visit bounds, in
   addition to the history gather and counted canonical sorts. Range preparation
   charges the final owned heap and copied neighbors before atomic publication.
   Borrowed and pinned read projections expose the role and original positions.
   Native history packing changes no V1 codec, hash or command preimage.

   Executed qualification is recorded in
   [09](09-implementation-status.md#native-claimant-result-testaments--2026-09-07).
   These commands provide no guaranteed future whole-claim closure funding.
8. **Qualify races before broader activation.** Exercise exact and stale timer
   generations, early deadlines, actor spoofing, Ready versus begun evaluations,
   retry/quality phases, and ordinary terminal parents with unfinished audits.
   Race a report against an earlier pending deadline in both publication orders;
   exact request retries must retain their original result. Seal a begun sibling,
   then finish it under exhausted ancestor RAM with pinned old versions. Discard
   the sealing/fencing suffix and recover original bindings, report credit and
   reverse protection memberships. Check that authority-only closure produces
   no participant result and that complete audit closure does not repaint an
   earlier work/response/claim terminal cut.
   Also cover a deadlock victim distinct from the triggering claim, expired
   pure Receipt/required-policy checks, omitted or duplicate attempt history,
   begun Observe alongside suppressed Ready, bundle closure before graph release,
   and claimant-only bundle generation/posting.

After these bounded seams and adoption below, complete the remaining
control/succession completeness and funding gates. Posted Required Admission now
shares indexed graph promises (§6.7). Native monitor reverse membership,
disposition, timers and explicit terminal-owner release are implemented in §6.12.
Native
durable import/codec, WAL/Session/quorum and CLI/MCP activation remain downstream.

### 6.11 Receipt adoption and retained unfinished cycles

The [native adoption transaction](../../crates/focal-core/src/native/adoption.rs)
uses `AdoptReceipt { expected, previous, receipt, holder }`. The claimant supplies
the intended replacement participant and a fresh receipt ID; the owner assigns
`previous.epoch + 1` with checked arithmetic. The source claim's exact binding,
current entitlement, original receipt allocation, creation/publication positions,
deadline and available response allowance must all agree before mutation. A
replacement cannot reuse a receipt allocated to another claim or an older epoch.
Exact request retries return their original retained outcome, including after
later responsibility transfers.

1. **Resolve authority without copying source truth.**
   `ClaimState::prepare_receipt_adoption` returns a borrowed capability carrying
   the exact original claim, previous/replacement entitlements, next binding and
   cause/cut. Applying it to a charged copy compares the complete source state.
   Native adoption preserves status and immutable content. It refuses completed
   claims and transfers that cannot leave a fresh response cycle for the new
   holder; it never resets `max_responses` or adopts old testimony implicitly.
2. **Resolve and fence complete actual membership.** Read the stored registration
   set once and resolve every declaration, evaluation, original receipt and
   target. Bound the per-member source lookups and declaration checks before
   traversal. `EvaluationState::adopt_receipt` adds one authority fence to every
   unfenced nonterminal member, including Ready and begun Admission/Increment/
   Work. Keep original attempts, phases, results and target identities. Terminal
   members and previously fenced members remain byte-for-byte unchanged.
3. **Retain abandoned work.** Closed cycles remain reachable through response
   lineage. A nonempty open cycle receives an inline `RetiredCycle` link and
   `RetiredCycleHead` update. The exact key includes receipt and epoch, so the
   replacement may reuse `response_count + 1` and a slot without colliding with
   earlier evidence. Validate complete old index termination/aggregates and
   actual current work/diagnostic membership before appending. Empty cycles add
   no invented history. Bound retained cycle/work counts and require strictly
   descending receipt epochs in the retained chain.
4. **Preserve complete projection and future bounds.** The indexed work cursor
   includes current, retired and closed cycles. Historical duplicate and response
   association checks use receipt-qualified cycle/slot identity. Reset only the
   current receipt's Increment target seal; retain the registry's original rows
   and advance its checked claim-binding baseline. Previously entered responses
   retain their original Required-check result gates. Charge actual retired work
   in addition to the immutable future response/slot allowance; charge old
   diagnostic-only cycles even when they add no work. New Work report grants
   must cover those histories. Existing live peer grants can refuse shape growth
   which exceeds their protected bounds.
5. **Publish and retire atomically.** Claim, new receipt, evaluation fences,
   registry and optional retired index share the ordinary candidate. Claim
   `ReceiptAdopted` history records its exact before/after revision; the separate
   receipt fact retains both holders/fences and the adoption cause. No claim
   status, response, work, accepted result or artifact is fabricated. The common
   completion journal retires the fenced grants before checking later shape
   growth. Refusal or discard rolls back those retirements as well as every row.
   With `E` changed evaluation rows, adoption writes `2E + 6` keys, or `2E + 8`
   when retaining a nonempty open cycle. Alongside those rows, adoption now
   replaces the respondent grant atomically and funds the replacement receipt's
   remaining diagnostic/close/post operations through Ordinary admission (§6.7).
   Static shape checks remain prerequisites for that complete RAM quote.
6. **Carry these facts through durable activation.** Native command intent uses
   its own appended tag, leaving V1 hashes/bytes untouched. The forthcoming
   native codec/checkpoint/import must encode both receipt allocations, complete
   fences and retired-cycle membership. Recovery must reconstruct report grants
   only for actual unfenced begun members and preserve every original accepted
   result/publication. CLI/MCP activation must expose an explicit claimant
   transfer operation without requiring a worker framework or launching agents.

Tests and executed qualification belong in [09](09-implementation-status.md).
Adoption has no automatic execution, report, graph release or whole-claim funding
guarantee. Complete claim-responsibility reservations, remaining control/succession
completeness, native WAL/Session/quorum recovery and live interface activation remain
required. Native monitor propagation and its bounded Work and Admission
integration are described in §6.12.

### 6.12 Runtime-scope integration sequence

**Native RAM implementation now includes monitor registration, named rebinding,
terminal-owner cancellation, direct root reverse subscriptions, repeated frozen
settlement, exact monitor timers and shared Work/Admission graph bounds.** Explicit
terminal-owner release was implemented and qualified in the earlier increment.
Qualification of this new monitor increment is recorded separately in
[09](09-implementation-status.md); the implementation description below does not
claim an additional completed test run. The live Session decoder and reducer still
use V1. This sequence preserves the ownership contract in
[02 §7](02-domain-and-lifecycle.md#7-graph-satisfaction-and-parked-scopes).

1. **Bounded model and native actor ingress are implemented.**
   [`MonitorPlan`](../../crates/focal-model/src/lifecycle/scope_monitor.rs) checks
   exact source/peer bindings and quotes replacement/read buffers before
   allocation. `BuildLimits` bounds bytes and visits; construction rejects actual
   capacities beyond its quote. The
   [native commands](../../crates/focal-core/src/native/monitor_commands.rs) consume
   those capabilities using the authenticated claimant, current claim binding
   and exact optional receipt fence:

   - `RegisterMonitor { expected, receipt, id, roots, deadline }` records nonempty
     typed roots under a globally unused monitor ID and a finite future deadline.
   - `RebindMonitor { expected, receipt, id, predecessor, successor }` checks both
     actual endpoint bindings and a real compatible `Supersedes` relationship.
     It changes the named monitor's roots while retaining its original deadline
     and registration; immutable claim graph declarations stay unchanged.
   - `CancelMonitor { expected, receipt, id }` requires a terminal owner. It records
     the original owner terminal position and a separate cancellation cut without
     rewriting that failure, testimony, root predicates or child obligations.

   `Released` and `Cancelled` are mutually exclusive monitor dispositions.
   Successful release requires settled predicates. Cancellation is the claimant's
   explicit disposal of a terminal owner's outstanding wait. Each real change
   emits one revision-bound monitor event in the claim history; no worker,
   callback, report or response is created.

2. **Direct subscriptions publish with their source changes.**
   The [monitor index](../../crates/focal-core/src/native/monitor_index.rs) retains
   one immutable global monitor allocation, including owner identity, original
   registration and full deadline. Each distinct direct root has a bounded
   reverse chain keyed by target and monitor. Links retain owner, registration,
   rebind stamp and adjacent link identities; removed links remain tombstones
   for global allocation accounting. Registration, rebind and disposition update
   the actual scopes, reverse links, history and outcome in one candidate.
   Neighbor edits use earlier staged index writes before committed source rows.

   Discovery follows actual immutable incoming/outgoing dependencies, active
   runtime roots, direct monitor subscribers and exact ownership/lineage links.
   It validates both index membership and the referenced source scope under finite
   visit and node bounds. The complete original transaction receives one
   [index-journal check](../../crates/focal-core/src/native/monitor_index_journal.rs):
   replay the genuine monitor deltas into a precharged overlay and require all
   and only the corresponding index rows. Omitted, duplicate or unrelated writes
   cannot be hidden by an event count. This direct index does **not** materialize
   transitive blocking closures or deliver durable notifications. Those caches,
   notifications and recovery reconciliation remain separate work.

3. **Frozen monitor and graph convergence is implemented.**
   [Native scope consequences](../../crates/focal-core/src/native/graph_monitor_effects.rs)
   recapture the full affected component after every real monitor release or
   claim graph transition. `Satisfied`, `Terminal` and `Released` remain distinct:
   terminal failure does not satisfy a success predicate, and claim terminality
   does not establish ownership release. Only a checked settled monitor loses
   its blocking links. A terminal owner's monitor can release later while its
   original terminal/local cuts, evidence and testimony remain unchanged.

   Each round consumes an actual staged source revision and retains its event;
   the final row cannot erase intermediate releases. Dependency failure and claim
   graph satisfaction converge without turning an already-begun eligible check
   into an unauthorized report. The model graph/source checks remain in force
   at the checked scope-transition entry. Refusal discards the candidate and
   preserves committed and earlier pending obligations.

   `ReleaseScope { expected }` remains an **explicit issuer action**. The terminal
   owner must be unreleased, its active monitors disposed, and every owned child
   already released. Its bounded private capability publishes `OwnerReleased`
   before derived consequences and preserves original terminal facts. Automatic
   monitor settlement does not call it. Cancelling a monitor does not cancel or
   release its targets or children. The earlier owner-release qualification
   remains a distinct record in document 09.

4. **Exact native monitor timers and claim-timer convergence are implemented.**
   [`NativeMonitorDeadlineInput`](../../crates/focal-core/src/native/monitor_deadlines.rs)
   has a disjoint `(claim, monitor, timer, generation)` retry key. Intent hashes
   the complete deadline, including its due time. Fresh resolution checks the
   retained global monitor allocation, actual source scope, original publication
   coordinates and trusted owner logical time. Exact pending and committed
   redelivery retain the original outcome before new timing or capacity checks.
   Participant commands cannot supply timer provenance or select a deadlock victim.

   [`MonitorDeadlinePlan`](../../crates/focal-model/src/lifecycle/scope_deadline.rs)
   separates an inactive timer, a settled wait, a canonical SCC witness and a
   checked negative-SCC expiry capability. Inactive delivery records a no-op;
   settled waits take their real release path. An unsettled timer must process
   the earliest effective claim/monitor deadline. Resource refusal, stale source
   or a different earlier deadline is an error, never an absence-of-cycle proof.
   Shared visit budgets retain preflight and SCC traversal costs on errors as
   well as success. All allocations are admitted before construction.

   `ClaimState::expire_monitor` consumes the private expiry authority even when
   the claim has no authored deadline. Expiry atomically fences the exact
   registered evaluations and preserves prior results; ordinary deadlock or
   dependency failure preserves eligible begun report authority. Repeated frozen
   SCC/fallback rounds include monitor releases and their index changes.
   [Ordinary claim timers](../../crates/focal-core/src/native/claim_deadlines.rs)
   still require their own authored deadline, but now consume fresh trigger
   revisions after monitor settlement before deciding further SCC or expiry.
   Their original trigger must become terminal before that claim timer is
   consumed. Neither timer fabricates participant work or testimony.

5. **Held graph bounds include repeated monitor revisions.**
   [Graph preparation](../../crates/focal-core/src/native/graph_monitor_effects.rs)
   and the shared [completion graph ceiling](../../crates/focal-core/src/native/completion_graph.rs)
   used by Work and Posted Required Admission price at most `M + L` automatic
   graph transitions for a captured component:
   `M` initially active monitors may each release once, and `L` live claims may
   each acquire one terminal graph outcome. This includes terminal owners whose
   monitors settle later. It does not include an implicit owner-release action.
   Bounds cover repeated source copies and captures, reverse-link neighbor edits,
   index-journal replay, intermediate history, cohort seals, revision margins,
   incoming publication storage and discrete event/outcome/sequence slots.

   Work and Posted Required Admission grant protection retains the complete
   original protected membership and checks proposed scope endpoints and reverse
   consequences before accepting
   topology changes. A later source cannot shrink that protection by presenting
   a smaller traversal. Growth must fit held byte, visit, write and slot promises
   or be refused before publication. These bounds provide native RAM completion
   capacity for the admitted Work cohort and one possible Required Admission
   failure. A claim-wide reservation for every future action remains required.
   Explicit owner release continues to use ordinary
   bounded admission. Separate bounded stages also need a checked aggregate
   traversal contract wherever a transaction-wide guarantee is advertised.

6. **Complete the remaining integration and durable capacity work.** Posted
   Required Admission now uses a complete indexed failure component, atomic
   failure/monitor/cohort publication and protected topology (§6.7). Other
   Admission results remain report-only where receipt or terminal history has
   already fixed the claim's disposition. Complete the remaining control/succession
   family audit and capacity work under the same source and fixed-point contract;
   generic graph support does not prove every owner entrypoint has a complete
   future reservation. Materialized transitive caches need
   explicit prefix coverage and invalidation if introduced. Durable notification
   delivery, timer scheduling/redelivery, continuation and reconciliation need
   replayable cursor/provenance rules; the direct in-memory index alone supplies
   none of those delivery guarantees.

   Reserve the full future claim obligation at responsibility admission before
   promising that all deadlines, cancellations, audit closure and scope/owner
   release can always complete. That reservation must cover future RAM, WAL and
   artifact bytes, disk/replica headroom, bounded continuation and retirement.
   Revalidate later topology and registration growth against it. The native codec,
   checkpoint/import, WAL/Ready publication, Session/quorum activation and CLI/MCP
   dispatch remain downstream gates in §§4–7. No running V1 command or record
   acquires successor semantics merely because the Rust owner path exists.

Qualification must use actual native creation, registration, receipt, response,
validation, supersession and timer histories. Cover local WholeWork completion
waiting on a real monitor; all three predicates; terminal-owner late release;
claimant-only register/rebind/cancel; exact receipt/binding checks; earlier monitor
and claim deadlines; SCC victims distinct from the trigger; intermediate trigger
revisions; begun late reports; pending/discard races; original reverse protection
under shrink/growth; and failures across quoted memory, visits and publication
slots. Preserve retained snapshots, audit coordinates and original cuts through
those transitions. Record executed results in [09](09-implementation-status.md);
source tests and implementation descriptions alone do not establish production
or cross-platform qualification.

### 6.13 Successor input-codec dependency plan

**Authored-content gate, now implemented in the RAM owner.** The
[`ClaimDefinition`/`ClaimState`](../../crates/focal-model/src/lifecycle/claim.rs)
represent lifecycle-relevant parties, ancestry, acceptance and runtime scope
limits. They are lifecycle projections; the AuthoredV1 profile now retains the
complete request separately in native claim-content and content-identity rows.
ProjectionOnly remains an incomplete representation of the authored contract in
[02 §§2–3](02-domain-and-lifecycle.md#2-identity-and-canonical-content), not a
complete claim document ready to serialize. A codec cannot recover omitted
instructions from an opaque hash or silently supply defaults for missing intent.

The checked immutable content representation includes schema and `OccurrenceId`,
a bounded inline work description, the closed Focal action, normalized authored
work scopes, full typed contextual relations and referenced inputs, and exact
ordered requirement specifications. Parties and cause remain verified
content; the existing graph/lineage/acceptance fields must be derived from, or
checked against, that same body. Preserve the single-authority relation rule in
[02 §3.1](02-domain-and-lifecycle.md#31-claim): cached typed views cannot become
independently writable alternatives. Retain and expose this content through the
actual owner at a fixed prefix, with bounded borrowed preparation, fallible
construction/copying, and atomic creation alongside its lifecycle and indices.

Keep immutable authored content in separate rows of the same native range root;
lifecycle transitions should operate on their smaller state projections. Resolve
content and state through one checked borrowed view at the same effective prefix.
This uses existing fallible owned containers, with no per-object Arc, secondary
mutable authority or separately published arena. Price the new content rows,
identity indices and creation changes before admission. Separate key namespaces
alone do not prevent copying: range writes copy retained neighbors on touched
pages. A strict guarantee that lifecycle updates never copy request text requires
an actual content-page partition respected by both preflight and construction.
Do not simulate isolation by inflating heap charges.

The range implementation now supplies fixed allocation-free key classifiers
through `new_partitioned` and `from_entries_partitioned`. Shared preflight and
construction rules force leaf boundaries at partition changes and reuse an
unchanged page when neighboring writes belong to other partitions. Ordinary
unpartitioned callers retain their layout and merge charges; partitioned merge
descriptors explicitly account for their classifier token. Native Core now
separates Definition pages and Artifact/ArtifactIdentity pages from changing
lifecycle rows. This protects small immutable rows as well as the existing large
singleton case. Authored claim content/identity and creation-result rows also
have their own explicit partitions. See the
[range implementation](../../crates/focal-memory/src/range.rs),
[partition cases](../../crates/focal-memory/src/range_partition_tests.rs) and
[actual native histories](../../crates/focal-core/src/native/layout_tests.rs).

Authored File/Symbol/Api/TestSurface/Component/UxSurface scopes describe the work
and its allowed scope. They are distinct from the native runtime monitor/child
registry and its capacity limits. Consultation and Challenge must remain explicit
actions with their actual questions, targets and contextual evidence. Required
receipt proves consultation delivery; it cannot supply the missing question or
turn an answer into quality approval. Challenge proof/follow-up requirements stay
under [16 L7](16-peer-validation-contract.md#6-dependency-ordered-implementation-and-acceptance),
without automatic corrective work. Enforce Focal/Hecate's prohibition on
self-targeted work with only an explicitly checked legitimate Handoff exception
([02 §8](02-domain-and-lifecycle.md#8-affordance-policy-rank-and-visibility));
authored posting now refuses self-work and also refuses self-Handoff until an
authenticated transfer capability exists.
Sylk's self-task bookkeeping and runtime handoff cleanup do not enlarge this
exception or authorize fabricated testimony or implicit scope release.

Validation uses the same body/projection distinction. The
[`DeclarationSpec`](../../crates/focal-model/src/lifecycle/validation.rs) and
legacy definition expose target, modes, evaluator/handler policies, schemas,
deadline and external definition hashes. AuthoredV1 now retains the complete
ValidationDescriptor, including checking instruction, optional quality-bar text,
contributed-by provenance and policy revision, with that same declaration.
This implements the authored body contract in
[02 §3.3](02-domain-and-lifecycle.md#33-validation-requirement-and-verdict) and
[16 §4](16-peer-validation-contract.md#4-supplying-and-invoking-validators).
Referenced-standard and policy-resolution authority remain separate gates. A code
or skill reference identifies participant-owned execution; it grants no new
executor authority, and Focal does not invoke it. Provenance records origin,
not permission to change requirements or report as another evaluator.

Derive content hashes from the complete canonical immutable body under the
assigned schema/domain, including occurrence and referenced stable IDs but
excluding the object's own allocated ID and mutable lifecycle. Requirement↔claim
references must remain nonrecursive as specified in [02 §2.2](02-domain-and-lifecycle.md#22-hash-domains).

For native descriptors, `RequirementRef.specification` pins the separately derived
`ValidationDescriptor::specification_hash()`: it commits the complete instruction,
policy and provenance under a separate domain while excluding the parent claim
ID and the validation's own ID/revision. The full `content_hash()` still includes
the actual parent claim and supplies `declaration().binding().content`; assembly
checks both identities and exact allocated IDs/parentage. Neither hash is an
author-supplied substitute for the body. This distinction changes no existing
native declaration stamp/intent, external `PhasePolicy.definition`, or V1 hash.

The effective owner needs a bounded content-identity/dedup index: identical
content including occurrence resolves to its existing identity; intentionally
repeated work carries a new occurrence; conflicting or partially duplicate batches
refuse without partial publication. A caller's binding hash is not this proof.
Do not rehash V1 evidence or silently change existing native request identities
and exact retries. Complete authored bodies and any new durable identity preimage require
an explicitly assigned successor wire profile. The internal AuthoredV1 RAM
profile below is separate; no durable profile is assigned here.

**Implemented typed construction, creation correspondence and managed decoded admission; transport activation remains.**
The dormant byte encoder and structural inspector now cover all 28 commands and
three timer namespaces, with the full grammar and bounds in
[21](21-native-input-format.md). Structural inspection establishes no content or
request identity and selects no owner loan. Separate bounded construction plans
now cover all 28 actor commands and three timer namespaces, including complete
creation cohorts. The owner accepts both existing owned Rust inputs and complete
opaque `DecodedRequest` plans. The latter preserve borrowed variable content
through exact retry inspection and actual held/Ordinary source selection, then
retain the input reservation through custody and candidate preparation. See
[21 §6.3](21-native-input-format.md#63-managed-owner-admission-of-decoded-requests).
The following borrowed model surfaces
make a bounded decoder possible without replacing actual policies with hashes or
constructing unreserved copies just to inspect them:

| Source | Available construction/inspection boundary | Remaining decoder obligation |
| --- | --- | --- |
| [Authored claim descriptor](../../crates/focal-model/src/lifecycle/claim_descriptor.rs) | `ClaimSpec` and repeatable `ClaimSource` share instruction, occurrence, canonical relation/scope, requirement and nested slot checking, original hash preimages and final-buffer construction. The complete creation decoder preserves ordered pins and zero-check slots, verifies actual validation correspondence and constructs final bodies without temporary typed arrays. | Connect the complete local cohort proof to effective-state owner inspection, reference-policy visibility, retry/dedup and funding. Legitimate Handoff authority remains unresolved; durable/live activation remains separate. |
| [Authored validation descriptor](../../crates/focal-model/src/lifecycle/validation_descriptor.rs) | `ValidationSpec` and repeatable `ValidationSource` share complete policy, instruction, quality standard, contributor and revision checks. The decoder derives original specification/full identities and complete checked acceptance metadata from actual bodies. Final allocation and every repeated source/model pass are explicitly quoted. | Preserve those identities and local cohort proofs through owner-selected funding. Resolve referenced standards and policy authority; provenance supplies no evaluator grant. No durable successor or live request dispatch is activated by these codecs. |
| [Validation definitions](../../crates/focal-model/src/lifecycle/validation_definition.rs) | Borrowed program views and repeatable `DeclarationSource` expose every phase, fallback, handler ID/version/agentic flag, attempts and both schemas. The standalone legacy body decoder prepares the same checked stamp and intent, then constructs final arrays directly. Shared model construction verifies actual owned identity and capacity. | Preserve the supplied binding while checking it against the enclosing cohort and its real authored bodies. Fund complete parsing, model work, final arrays and bookkeeping through actual ingress; no temporary typed handler array is needed by the new adapter. |
| [Acceptance policy](../../crates/focal-model/src/lifecycle/acceptance.rs) and [value-source plan](../../crates/focal-model/src/lifecycle/acceptance_source.rs) | Existing typed plans and the repeatable `AcceptanceSource` check complete real declaration correspondence. The value-source path consumes opaque checked metadata from actual declaration bodies and borrowed nested slots, preserves zero-check slots and Required Delivery, checks repeated-source stability, and quotes canonical selection, all full-scan callbacks and final construction. Both creation frame decoders consume it. | Keep complete local correspondence and intent tied to the immutable frame through owner retry/funding selection. Include the actual descriptor/parser work performed by every callback, custody and candidate construction in the complete ingress budget. |
| [Artifact descriptor](../../crates/focal-model/src/lifecycle/artifact_descriptor.rs) | `ArtifactSpec` and repeatable `ArtifactSource` share bounded semantic checks, accounting and complete hashes. Raw plans borrow reference/label spans and construct final buffers. Managed admission now shares actual receipt/target/provenance, visibility, schema and held-contract checks with owned reports before reserving input capacity; final owned identity/authority is checked again. | Complete transport buffer and transaction-wide work accounting. Source parsing remains cumulative through borrowed preflight and build; final construction headroom is checked before allocation. Extend integrated qualification across every report target. See [21 §6.3](21-native-input-format.md#63-managed-owner-admission-of-decoded-requests). |
| [Authored response input](../../crates/focal-core/src/native/response_input.rs) | Shared response/monitor plans preserve full authored content and bound final construction. Managed decoded admission now resolves exact retries and selects actual respondent receipt credits before constructing response buffers. Failure diagnostics, authored close and posting pass full ancestor-memory pressure tests. | Connect authenticated transport and complete total ingress work accounting. Evidence acceptance, cycle closure and monitor authority remain existing candidate checks. The API and pressure tests do not activate durable or live CLI/MCP dispatch. |

The model now provides
[`AuthoredCreationPlan`](../../crates/focal-model/src/lifecycle/authored_creation.rs)
for the assembly in step 1. It derives graph, correction lineage and acceptance
from the actual claim and validation descriptors, preserving zero-check slots
and parent-independent requirement pins. It verifies the complete descriptor set
without copying handler arrays. Preparation quotes the actual inspection work,
the exact bounded build visits, and all final/temporary buffers with allocator
bookkeeping before construction. Canonical graph/lineage buffers move directly
into the projection; typed slot and declaration-reference arrays remain charged
until acceptance is built. `into_definition()` ends the descriptor borrows so the
owner can move the original bodies into immutable storage. Reference existence,
current ownership and ordered creation cuts still require the normal owner
`CreationPlan`. The model plan itself does not publish; the authored Core profile below consumes it before moving the original bodies into storage.

**Authored-content owner/store requirements.** The native RAM implementation
below establishes assembly, retention, dedup and matching reads. Preserve these
requirements through its remaining durable import, authority and activation work:

1. **Integrate the checked model projection.** Use `AuthoredCreationPlan` with the actual `ClaimDescriptor`
   relations to derive/check graph obligations and cause/correction lineage, and
   its complete `SlotPolicy` entries with each actual `ValidationDescriptor`'s
   `Declaration` to construct/check acceptance. Match every ordered requirement
   ID and `RequirementRef.specification` to the descriptor's actual ID and
   `specification_hash()`. Separately check the actual declaration's full
   `binding().content` against descriptor `content_hash()`, and exact parent
   claim/issuer, declaration index, target and mode. Require the complete
   declaration set, Required Receipt and zero-check slot presence rules.
   Hashes or supplied summaries cannot substitute for the actual descriptors.
   Keep `max_responses` and runtime `ScopeLimits` as explicit checked responsibility
   capacity/profile values, preserved through assembly and reconstruction. They
   are not omitted request text or values to recover through invented defaults.
2. **Stage content identity with the whole creation batch.** Add immutable full-body
   content rows and namespace/family/schema-bound identity indices in the same
   native root as lifecycle state. Resolve endpoints, descriptor pins, positive
   reads and absence against committed state plus earlier pending candidates and
   the complete proposed batch. Recompute identity; exact duplicate content must
   resolve consistently, while collisions, substituted bodies, conflicting IDs
   and partially duplicate batches refuse without silent rebinding or partial
   publication. Creation must still permit valid compound dependency/wait cycles
   and refuse forbidden causation/correction cycles.
3. **Fund real storage isolation and rollback.** Precharge all added counters,
   content/identity rows, containers, indices, grouping workspace, retained-page
   copies and journal buffers, including temporary/final coexistence. Implement
   strict content-page isolation in both range preflight and construction before
   promising that lifecycle writes never copy request text; key separation and
   fake padding do not provide it. Revalidate existing completion envelopes and
   receipt/Begin growth promises against the actual partition and new row shapes.
   Publication and discard must preserve exact counter, page and permit ownership.
4. **Expose one checked read and authority boundary.** Borrow content, descriptor
   policy, lifecycle and references from the same committed/effective prefix,
   retaining snapshot pins and bounded traversal. Check authenticated issuer,
   actual cause/ownership, reference visibility and supported action semantics;
   `check_postable` alone does not establish legitimate Handoff or permissions.
   Lifecycle updates must preserve immutable content and descriptor identities.
5. **Qualify actual owner histories before wiring the codec.** Cover substituted
   claim/validation content, missing/extra declarations and changed slot modes;
   Work/Consultation/Challenge and self-work/Handoff authority; identical content
   versus a new occurrence; pending-batch races and legal versus forbidden cycles;
   same-prefix reads, pinned old snapshots and content-page isolation; allocation,
   counter and visit refusal with exact rollback under existing held promises.
   Preserve frozen V1 bytes/hashes/replay and existing native exact retry identity
   until an extended successor profile is explicitly assigned. Source descriptors
   alone qualify neither stored content, Core assembly nor a wire entrypoint.

**Implemented native RAM creation profile and result contract.**
[`Core::new_native_authored`](../../crates/focal-core/src/native.rs) creates an
empty `AuthoredV1` owner. The existing `new_native` constructor remains
`ProjectionOnly`; neither constructor promotes or repairs another profile's root.
`CreateAuthored` groups each claim descriptor with its complete validation
specifications, explicit response/scope capacities and original owner
precondition. Assembly borrows that group, derives its projection and moves the
original descriptor buffers into immutable range rows. The mandatory multi-claim
`CreationPlan` still resolves ownership, creation cuts, lineage and graph effects.
The existing `Create` input and its tag-zero native intent remain unchanged.
The new command has a separate internal tag/domain; neither profile is a durable
schema allocation or a live transport entrypoint.

[Authored preparation](../../crates/focal-core/src/native/authored_prepare.rs)
stores claim content and family/schema/full-content indices with state,
definitions, history and a creation-result row under the same candidate root.
Definitions own either the old declaration or one complete validation descriptor;
policy reads borrow the descriptor's actual declaration without a second handler
array. Claim content retains the originally admitted response/scope capacities
and owner fence separately from mutable state. Actual page partitions isolate
these bodies and frozen results from lifecycle-only writes.

The [creation result](../../crates/focal-core/src/native/creation_result.rs)
records each input ordinal, family, schema, full content hash, requested ID and
resolved ID in one bounded vector. A wholly consistent existing-content batch
publishes a new outcome and frozen mapping with no new claims or definition
facts. Exact request retries return the recorded outcome and mapping. Partial
matches, reused conflicting IDs, duplicate same-family identities/content or
changed responsibility profiles refuse the whole transaction. Validation content
indices use the parent-bound hash; requirement pins use the separate specification
hash. Focal never rewrites parent references to make a duplicate batch fit.
Claims and validations may share the same numeric ID bytes.

The [publication checker](../../crates/focal-core/src/native/authored_check.rs)
verifies the private preparation proof, exact row/index membership, descriptor
policies, profile and returned mapping before common history emission. The proof
binds the actual retained body identities, original owner precondition and result
order. Both ordinary and graph-controlled creation pass this check. Reconstruction
inspects real bodies and descriptor variants plus both directions of identity
membership; matching counters or an enum flag are insufficient. A missing body
refuses reconstruction and returns the original Core unchanged.
[Content reads](../../crates/focal-core/src/native/authored_reads.rs) borrow the
same effective or pinned prefix as their lifecycle and result rows.

Contextual object relations resolve against the effective prefix and complete
proposed batch. Authenticated issuer and actual ownership still use the existing
owner gates. Authored posting checks self-work explicitly; a self-Handoff label
currently refuses too, because this owner has no authenticated transfer capability
yet. That capability, referenced-standard/policy authority beyond the current
checked declaration contract, durable import/codec provenance and live CLI/MCP
activation remain separate work. No tool execution or worker launch was added.

The complete next boundary includes all **28 actor commands** in
[`NativeCommand`](../../crates/focal-core/src/native.rs), with no implicit
fallback to a V1 command:

| Family | Commands |
| --- | --- |
| Creation, control and responsibility | `Create`, `CreateAuthored`, `Post`, `Cancel`, `AcquireReceipt`, `AdoptReceipt`, `ReleaseScope` |
| Monitors | `RegisterMonitor`, `RebindMonitor`, `CancelMonitor` |
| Respondent and claimant evidence | `SubmitWork`, `SubmitDiagnostic`, `FailWorkProduction`, `RejectWork`, `ReceiveWork`, `CloseResponse`, `PostResponse`, `ReceiveResponse` |
| Assessment entry and target membership | `SealIncrementTargets`, `EnterWholeWork` |
| Evaluation execution | `BeginAdmission`, `ReportAdmission`, `BeginIncrement`, `ReportIncrement`, `BeginWork`, `ReportWork` |
| Claimant audit bundle | `GenerateResultTestament`, `PostResultTestament` |

Keep the three trusted timer namespaces separate: `NativeDeadlineInput` identifies
an exact evaluation; `NativeClaimDeadlineInput` identifies a claim;
`NativeMonitorDeadlineInput` identifies a claim and monitor. Each carries the
complete authored `Deadline`, including `at`; invocation keys alone omit that
field and cannot establish exact retry identity. The actual authenticated
principal, trusted firing/logical time, assigned creation cut and receipt epoch,
custody attestation, model transition proofs and completion loans remain owner
values. A participant body cannot supply or select trusted timer authority.

The existing native `Create` projection contains ordered proposals
with claim binding, issuer/subject, optional deadline, maximum responses, graph
declaration, succession lineage, acceptance policy, scope limits and optional
exact owner binding/receipt fence, followed by the ordered declaration inputs.
These fields must be bound to the complete authored content above before codec
freeze. The owner assigns `created`; no description, occurrence or missing V1
content is recoverable from the opaque native binding hash. The graph encodes every
DependsOn/Awaits obligation; lineage encodes binding, Root/Claim cause and every
Supersedes/Amends predecessor reference. Existing borrowed constructors in
[`graph.rs`](../../crates/focal-model/src/lifecycle/graph.rs) and
[`succession.rs`](../../crates/focal-model/src/lifecycle/succession.rs) now also
provide bounded canonical value checks and consuming constructors for final
funded vectors. The projection decoder uses these seams without temporary typed
graph/correction arrays. Interleaved global declarations are checked through a
bounded repeated filter; every callback and final array is included in its quote.

The complete local construction boundary is implemented in
[21 §6.2](21-native-input-format.md#62-complete-creation-frames). Effective-state
authority/retries, owner-selected funding and durable/live activation below are
still required.

Implement the wire boundary in this dependency order:

1. **Authored content is retained and qualified in RAM.** The checked claim and
   validation ownership, derived action/scope views, nonrecursive identities and
   effective-state dedup above are implemented. Referenced-policy authority and
   legitimate self-Handoff remain unresolved. Qualification covers changed instruction,
   action, scope, requirement and occurrence cases, self-work/Handoff refusal,
   exact reconstructed content, and all additional creation/copy/page costs.
   Codec primitives may be developed independently, but the current lifecycle
   projection must not be frozen as the complete successor `Create` body.
2. **Define and validate the complete borrowed body.** Use explicit schema,
   command/variant tags, widths, lengths, option markers and UTF-8 rules. Reject
   unknown tags, truncation, trailing bytes, overflow, duplicate identities and
   noncanonical set encodings before retaining owned input. Preserve authored
   order where the current native intent includes it: outer proposals and
   declarations, monitor roots, handler fallbacks and response references.
   Graph obligations and slot/check sets retain their canonical ordering rules;
   normalized lineage corrections must have one canonical representation. Never
   use unsafe byte casts to manufacture typed slices. Derive all dimensions from
   the complete body, including nested arrays and every potential allocation.
3. **Recompute identity before retry or funding selection.** Validate and hash all
   represented fields using the same checked native semantic rules as the owned
   input. Do not trust a serialized summary, digest, dimension table or supplied
   content hash as proof of the body. Reuse the shared plan identities and
   [`intent.rs`](../../crates/focal-core/src/native/intent.rs) logic where applicable;
   the new format must define its own canonical bytes and explicitly assign any
   extended authored-body identity, preserving existing V1 and native request
   identities rather than changing their preimages in place. A valid retained
   exact request can return its
   original outcome before fresh-source admission; malformed or changed bodies
   cannot exploit a known request key. Only after this complete borrowed check
   may the owner choose an existing retry, a held evaluation/respondent loan or
   Ordinary admission for a fresh request.
4. **Reserve, construct and reconcile under the selected source.** Count borrowed
   parser workspace that owns storage, typed temporary arrays, grouping/index
   buffers, final input containers, nested capacities and allocator bookkeeping
   while they are simultaneously live. Model quotes report their own layers;
   they do not include all decoder buffers. In particular, `AcceptancePlan`'s
   allocation count is separate from its byte quote, while `NativeResponsePlan`
   includes native allocator bookkeeping. Hold the chosen loan through owned
   input construction, custody verification and candidate preparation; refund
   every temporary allocation on refusal. An already promised report or response
   must not need a new Ordinary allocation merely to decode its body. Bound the
   full parsing, hashing, model validation, sorting and reconstruction work too;
   the new preparation plans do not establish a transaction-wide visit budget.
5. **Qualify the unadvertised codec before durable integration.** Require exact
   vectors and borrowed/owned identity parity for all commands and all timer
   namespaces, malformed nested fields and limits, unchanged V1 vectors, exact
   pending/committed retries, and construction failure under a real held loan
   with Ordinary RAM exhausted. Verify that no refusal publishes custody,
   results, timers or range changes. This codec can be implemented independently
   of whole-claim closure reservations, but cannot claim those guarantees.
6. **Advertise only the complete durable successor.** Input bytes alone do not
   provide native recorded-mutation replay, explicit historical import,
   checkpoint hydration and entitlement reconstruction, Session pending/committed
   prefix handling, shared WAL/Ready publication or quorum activation. Finish
   those gates and allocate the real successor descriptor before advertising a
   decoder or wiring live Session/CLI/MCP dispatch. The existing V1 floor remains
   authoritative until the ordered transition and replicated activation in §§4–5.

### 6.14 Constructing durable native state without cloning or re-executing it

The native input codec is not a redo record. Current request authorization,
deadline discovery and graph reduction must not run again to decide what an
already committed mutation meant. Native recovery needs actual recorded rows,
checked model hydration and complete cross-row validation before publishing a
detached root. The physical WAL can retain its existing checksums and durability
fences while the application record and checkpoint receive distinct native
formats and an explicitly activated decoder requirement.

The implemented envelope, row families and construction boundaries are specified
in [22 — Native recorded mutations](22-native-record-format.md).

The [precharged range loader](../../crates/focal-memory/src/range_hydration.rs)
accepts borrowed row plans and does not require `V: Clone`. Each plan carries the
final key and complete quoted key/value heap. The engine reserves that allowance
before invoking the trusted row builder, immediately checks the actual owned
charge, and releases unused allowance. Source preparation and decoder workspace
remain separately bounded. Values move into final pages; retained-neighbor copies
use the supplied fallible copier under existing range precharges. A single
bounded chunk is staged, partitions remain physical leaf boundaries, and every
failure drops rows before their permits and discards the detached store. Existing
Clone-based checkpoint loaders retain their prior behavior.

The [dependency-phased range builder](../../crates/focal-memory/src/range_hydration_phased.rs)
now extends this construction path. `RangeHydration` owns the detached range;
each consuming `insert_phase` accepts exactly the declared number of strictly
ordered, previously absent keys. Phases may arrive in a different order from
their canonical keys. During construction, a scoped immutable lookup includes
earlier phases and earlier rows in the current chunk, so a dependency does not
change availability at a page boundary. The owner checks the total expected row
count and maximum phase count. It stages at most
`min(page_entries, max_batch_entries)` owned rows, without a whole-ledger row
array or cloned high-water key. Any phase error consumes and drops the complete
detached import. `finish` checks final cardinality, invokes the supplied complete
validator through a read-only view, and binds the verified prefix only after
validation succeeds. Neither callback can obtain a root handle, snapshot or pin.
Parser work, repeated scans, dependency lookups, row-plan preparation and any
decoder-owned indices or workspace still need their own enclosing byte/work
accounting. The native decoded-row adapter and validator are now connected in
[checkpoint recovery](../../crates/focal-core/src/native/record_codec/recovery.rs),
with the phase order, custody boundary and scratch indices specified in
[22](22-native-record-format.md#native-checkpoint-restoration). Their consolidated
qualification is recorded in [09](09-implementation-status.md#native-checkpoint-restoration--2026-09-07).
The subsequent [incremental replay adapter](../../crates/focal-core/src/native/record_codec/replay.rs)
constructs and validates an exact successor against that Core without executing
the original command. Its graph capture provenance, funding transfers and
restored-owner qualification are recorded in
[09](09-implementation-status.md#native-incremental-replay-and-restored-owner-qualification--2026-09-08).
Session provenance, durable buffer funding and activation remain separate requirements.

The [scalar validation snapshots](../../crates/focal-model/src/lifecycle/validation_snapshot.rs)
now preserve every retained EvaluationState and AcceptedResult field, including
handler-local cursors, programmatic evidence, older last results, receipt and
generation, suppression, authority fences and cohort seals. Hydration derives the
private definition stamp from the actual retained Declaration. It checks intrinsic
target/policy/cursor/result consistency with a bounded, allocation-free scan;
exact work limits are checked before scanning handlers. Real early transitions
from programmatic checking to quality assessment remain valid, and structural
delivery/missing-target results retain their absence of participant attempts.
These model views are not serialized native records or authenticated history.
Monotone/minimum revisions cannot prove that all intermediate events occurred;
complete ordered history, registry membership, original publication coordinates,
immutable evidence and local custody remain importer obligations.

The implemented construction stages and remaining integration are concrete:

1. **Retain and encode the actual write set.** The
   [native write-set owner](../../crates/focal-core/src/native/mutation.rs) now
   captures every canonical changed key after the range's preflight and before
   its builder consumes row values. `NativePrepared` retains these keys, its
   content profile, range and outcome; values remain in the candidate's single
   immutable root. All 30 row families participate, including Meta, Outcome,
   Event, content and every index/link row. Explicit deletions remain distinct
   from present tombstone rows. Preparation reserves exactly the actual key
   count before allocating, checks the final candidate against the captured
   plan, and preserves the debit through pending ownership and publication
   refusal. Evaluator/respondent promises now retain this charge per action;
   shared construction workspace excludes it. Capturing the maximum possible
   failure shape during a smaller regular report would exceed that report's
   retained promise, so exact reservation follows canonical shape validation.
   `NativeOwner::prepared_candidate` exposes the actual ticket's borrowed
   candidate to the [native mutation encoder](../../crates/focal-core/src/native/record_codec.rs).
   Its `EncodingPlan` measures and writes the complete values found at exactly
   those captured keys in that immutable candidate. It does not replay requests
   or dump the whole root. Measurement, row sizing, lookup and hashing share a
   checked work allowance; a length-prefixed row can be measured and written
   without a body buffer or per-row offset array. The plan itself allocates no
   buffer and acquires no additional reference-counted owner. Each measurement
   and write pass consumes its quoted work; callers must budget both passes.
   The destination must have the exact quoted size and remain funded through
   pending WAL ownership, commit or rollback. Incorporating those encoded bytes
   into evaluator/respondent completion promises, admission peaks and retained
   pending charges is still required. Existing key-manifest funding does not
   cover the encoded buffer. A whole-root record per mutation would scale with
   occupancy and violate held report envelopes.
2. **Hydrate model state explicitly.** Versioned model snapshot views and checked
   constructors now cover scalar EvaluationState/AcceptedResult,
   [claims](../../crates/focal-model/src/lifecycle/claim_snapshot.rs),
   [registration sets](../../crates/focal-model/src/lifecycle/registration_snapshot.rs),
   [scope registries](../../crates/focal-model/src/lifecycle/scope_snapshot.rs),
   [work and response evidence](../../crates/focal-model/src/lifecycle/evidence_snapshot.rs)
   and [audit/result testimony](../../crates/focal-model/src/lifecycle/audit_snapshot.rs).
   They preserve original receipt, target, generation, cursors, histories,
   reserved audit capacity, fences and seals. Claim history derives private
   report stamps from actual restored responses; registrations and audits derive
   private definition stamps from actual declarations. Original Generated bindings
   come from recorded history, not an assumed revision one. This preserves broader
   reachable model states while the native importer separately enforces its
   original command profile. Construction quotes include actual owned capacities,
   bounded source reinspection and final values. No restoration invokes current
   Begin/report/expiry/adoption authority. A model DTO version is distinct from
   the frozen live V1 wire/application formats.
   The native loader must respect these dependencies: restore immutable policies
   and declarations, evaluations and work evidence before responses; restore
   response-dependent claim history and scopes before its registrations; then
   restore claimant audit bundles. Canonical key order alone is not a construction
   order. Connect these constructors to the implemented phased range builder
   using borrowed decoded-row plans and checked heap quotes. Derive model plans
   that need restored dependencies inside the scoped row-construction callback,
   under its reserved row allowance. If a decoder creates a phase index, price
   that index and its workspace separately. Avoid a second whole-ledger array of
   owned rows merely to reorder construction.
3. **Decode and check the complete recorded rows.** The implemented encoder uses
   its own dormant `FCMUTATE` magic and version, explicit tags for all 30 key
   families and four invocation namespaces (participant request, evaluation
   deadline, claim deadline and monitor deadline), and length-delimited bodies.
   It uses explicit fixed integer widths and encodes scalar ledger, linked-head
   and cycle counters as checked `u64`; collection lengths use `u32`. It does not
   serialize Rust enum layouts or use serde. Complete immutable descriptors,
   original hashes, receipts, active/retired cycles, index links, publication
   coordinates and creation mappings are represented. Response and result
   testament rows retain their actual original Generated binding/revision
   metadata; restoration must not infer it from a later lifecycle state or assume
   revision one. The trailer hashes every preceding byte using the BLAKE3
   derive-key domain `focal.native.record.v2`, independently of physical WAL
   checksums. The recorded range ID identifies the original process-local
   incarnation, which changes on restoration; recovery must prove and retain
   its mapping to the new owner instead of comparing it directly with a newly
   allocated `RangeId`.

   The [structural inspector](../../crates/focal-core/src/native/record_codec/inspect.rs)
   checks the envelope/version/profile, ledger and adjacent prefix frame, exact
   row count, key order, mutation kind/body lengths, presence of Meta and the
   byte-identical current Outcome, trailing bytes and checksum under explicit
   byte/row/work bounds. Its row iterator borrows opaque body slices and has an
   explicit work limit for each scan. A structural record is not decoded model
   state: a body labeled as one family may still contain invalid fields, foreign
   references or fabricated history. Bounded per-family decoders, intrinsic
   model hydration and native checks now supply the complete checkpoint and
   exact-predecessor incremental mutation paths. Their component qualification
   does not activate durable service replay. Artifact custody fields encode only the retained content-tree address
   and observed local revision. They do not serialize a capability or prove
   availability on a follower or after restart; recover actual local
   content/placement evidence separately from accepted-result provenance.

   The complete native Core checkpoint encoder in [22](22-native-record-format.md#complete-root-checkpoint-envelope)
   now shares these row bodies, with a distinct `FCNROOTS` envelope and checksum
   domain, exact u64 total row count, genesis rules and put-only rows. It supports
   an exactly sized output slice and bounded callback streaming, without a second
   encoded-root buffer. Callback errors preserve their original value; partial
   output has no successful publication or durability acknowledgment. The
   borrowed-Core plan does not yet provide online scheduling from an accounted
   fixed-prefix pin. Complete checkpoint row decoding and native validation are
   implemented; persistence/retention and enclosing Session metadata remain required.
4. **Validate the detached complete root — implemented for Core checkpoints.** Recompute every Meta count from actual
   row families, including inactive links, result families and retired cycles.
   Validate identities, registration membership, reference/index completeness,
   contiguous accepted history and original event coordinates before import.
   `NativeOwner::with_schemas` then reconstructs live evaluator/respondent credits;
   its existing live-obligation checks alone do not validate an arbitrary snapshot.
5. **Connect application durability and replication.** Add a Session path owning
   NativeOwner with an exact record-hash, ticket, Raft-index and native-prefix
   mapping. Publish only committed candidate heads and discard only a suffix whose
   durability is resolved. Native prefix advances once per native mutation;
   membership, cursors and other Raft entries mean it is not simply the Raft index.
   Checkpoints retain this mapping alongside membership, placement, retirement and
   request-stream metadata. Preserve explicit activation/import base provenance.
6. **Activate only the complete decoder.** Keep the existing physical WAL and
   Ready persistence contract; connect incremental native replay to the durable
   log and complete the enclosing Session checkpoint and decoder descriptors before registering the dormant
   mutation format. Encoding and
   structural inspection do not activate a native WAL decoder or live dispatch.
   Local decoder-floor promises do not establish replicated
   activation or membership agreement. Complete those fences and durable
   completion-capacity obligations before exposing native live CLI/MCP dispatch.

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
