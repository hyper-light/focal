# 23 — Native activation and import

Updated 2026-09-08. This document records how one `Session` hosts two domain
engines, which persisted fields survive the transition, how native history is
activated on a replicated group, and how legacy history is to be imported. It
extends [18](18-lifecycle-storage-upgrade.md) and [22](22-native-record-format.md);
the durable native engine itself is specified in 22 §6.

## 1. One Session, two engines

[Session](../../crates/focal-ledger/src/session.rs) keeps every ancillary
protocol it always had: cursor registry and retained deltas, cursor maintenance,
membership, placement, managed request streams, evidence snapshots and
reconciliation. Its domain engine is the frozen V1 reducer until a committed
activation record applies; from that index on, the
[native engine](../../crates/focal-ledger/src/native_session_engine.rs) applies
native genesis and record entries in the same Raft order as every other entry.
The V1 core stays in memory as frozen history; legacy domain commands are
refused with `UnsupportedSchema` at admission, and a legacy domain entry after
the activation index is corruption. Nothing is cloned, re-executed or relabeled.

A replica hosts the native engine only when its node supplies
[NativeHosting](../../crates/focal-ledger/src/native_hosting.rs) at construction:
the engine limits, a lock-free content reader over the node's content directory
and the replica's producer range. Hosting is a construction input because
recovery may replay a committed activation or install a native checkpoint
before any later call could attach it.

## 2. Persisted field matrix

Source names the protocol that writes the field; representation names the
envelope and codec; authority names who may change it; default legality states
whether recovery accepts its absence; validation names what recovery checks.

| Field | Source | Representation | Authority | Absent | Recovery validation |
|---|---|---|---|---|---|
| V1 core rows and receipts | V1 domain epochs (`FOCALOP1`, managed `FOCALMD1`) | `FOCALCP1` bytes inside every snapshot; entries replayed by the frozen reducer | leader admission, quorum commit | never (empty core at genesis) | ledger identity, sequence continuity, frozen normalized bytes |
| Cursor registry | `FOCALCU1`/`FOCALCU2` entries | `CursorCheckpoint` in SS2+ | cursor commands | SS1 recovers an empty registry at the core prefix | ledger, revision, floor against retention limit, consumers vs owners |
| Cursor receipts and owners | cursor entries | `CursorMetadata` in SS2+ | cursor commands | empty | keys match receipts, epochs admitted, records name owned consumers |
| Delta floor and retained deltas | domain publication, maintenance (`FOCALCM1`) | SS2+ tail | publication, retention | empty tail | schema 1, contiguous ordinals, floor within retained range, byte limit |
| Membership state | `FOCALMC1` entries | `MembershipState` in SS3+ | membership commands | default state | configuration index ≤ snapshot index, receipt matches installed configuration |
| Placement state | `FOCALPL1` entries | `PlacementState` in SS4+ | placement commands | default state | never regresses once present; fences validated |
| Request streams | `FOCALMS1`/`FOCALMU1` entries | `RequestStreamsCheckpoint` in SS5+ | managed protocol after the managed floor | inactive with no slots | activated implies slots, slot bounds |
| Managed decoder floor | `FOCALDF1` in the physical log | consensus baseline promise | local durable write | absent before managed support | compiled descriptor must equal the recorded floor |
| Native decoder transition | `FOCALDT1` in the physical log | consensus transition pair | local durable write after the baseline | absent until a replica promises | pair must equal the compiled predecessor and successor |
| Activation record | `FOCALAC1` entry | fixed 228-byte record with checksum, retained verbatim in SS6 | leader proposal after every voter's promise | absent means V1 | predecessor and successor descriptors, configuration index and hash, sealed legacy prefix, empty prefix for genesis kind |
| Native metadata and core | native engine | `FCNSESS1` section of SS6 (physical identity, Raft coordinates, recording range/term, activation, `FCNROOTS2` core) | native admission and replay | absent means V1 | cluster, group, ledger, profile, genesis, decoder floor, applied index/term, membership, prefix |
| Native genesis | `FCNGENES1` entry | committed record | first native leader after activation | absent until proposed | derived from cluster, group, ledger, profile, decoder |
| Native mutations | `FCMUTATE2` entries | committed records | native admission | none after genesis | ledger, profile, base prefix, producer range per term, record hash and outcome |

Every SS1–SS5 writer and reader is unchanged. A native ledger always writes
`FOCALSS7`: the SS4 sections, the request-stream section regardless of
activation, the activation record, the native section and the request
registry's slot-generation watermark ([15](15-managed-request-streams.md)).
`FOCALSS6` (the same without the watermark) is still read, deriving the
watermark from its retained pairs. A V1 ledger keeps writing SS3/SS4/SS5
byte-identically.

## 3. Activation protocol

1. **Promise.** A hosted replica writes the managed baseline floor and then the
   transition to the native successor (`begin_native_support`). Both are local
   durable facts; neither activates anything. The support exchange advertises
   the native descriptor hash once the transition is durable.
2. **Barrier.** The leader proposes activation only when every current voter,
   in both sets of a joint configuration, has a recorded native promise at the
   current configuration index, and no membership change is pending.
3. **Record.** `FOCALAC1` names the predecessor and successor descriptors, the
   configuration index and hash, the sealed legacy prefix and, for imports,
   the legacy checkpoint and expected native root. Genesis activation requires
   an empty legacy prefix with no receipts or admitted epochs.
4. **Apply.** Every replica revalidates the configuration precondition at the
   ordered apply boundary, requires its own durable transition, constructs the
   native engine under its hosting, adopts the applied prefix, and — when it is
   already an authority past this term's readiness barrier — reconstructs the
   owner at once. A duplicate identical record from a concurrent proposer is
   inert; anything else is corruption.
5. **Genesis.** The ready authority proposes `FCNGENES1`; followers validate it
   against their physical identities. Native admission opens only after it.
6. **Fence.** A replica without a durable transition refuses native entries,
   the activation record and SS6 snapshots before Raft persists them; the
   transport drops the packet and retransmission resumes once the floor is
   durable. A replica without hosting refuses permanently, and a downgraded
   binary cannot open a native ledger at all: recovery replays the committed
   activation and stops before touching state.
7. **Membership.** After activation, adding a learner or promoting a voter
   requires that node's native promise; the managed baseline is not enough.

## 4. Deliveries under refusal

A retryable native refusal (memory, missing custody, a staged floor write)
retains the drained delivery inside the Session and returns `Retry`; the next
poll resumes at the same entry and nothing applies twice. V1 application keeps
its fail-closed behaviour. Native committed outcomes and correlated read
boundaries are delivered on `SessionEvents` next to the V1 results.

## 5. Import of populated legacy history

Import turns a sealed legacy prefix into native prefix one without rerunning
any command and without inventing lifecycle facts (18 §7). This is the frozen
representation; it is implemented in
[import.rs](../../crates/focal-core/src/native/import.rs) and the activation
paths of [native_hosting.rs](../../crates/focal-ledger/src/native_hosting.rs).

### 5.1 Mechanism

- The activation record (`FOCALAC1`, schema 2) of kind `Imported` carries the
  sealed legacy prefix (`v1_sequence`, the hash of its frozen `FOCALCP1`
  checkpoint), the import parameters every replica must reuse (the trusted
  logical time, the canonical inline chunking and manifest bound) and the
  `native_root` the leader computed. The import intent hash covers every field
  except the root, so the root can depend on it.
- Every replica translates its own legacy core at the activation apply
  boundary: rows are built with the recorded-fact constructors recovery uses,
  written as one `FCNROOTS2` image under a canonical incarnation, restored
  through `recovery::restore` under the replica's own range, and the image
  hash is compared with `native_root`. A different root is a divergent replica
  and fails closed (`ActivationConflict`); no bytes are transferred and no
  legacy command is rerun.
- The import is native sequence one: `Outcome(NativeInvocation::Import)` with
  operation `Import`, intent = the import intent hash, logical time from the
  record. No record produced it, so the enclosing checkpoint records the
  prefix that holds no native record (`records_floor`, zero at genesis and
  one after an import) and the first native record binds to that floor. The
  genesis record is committed after activation exactly as for an empty ledger.
- Inline legacy payloads are sealed into the local content tree with the
  recorded chunk size before the translation re-verifies custody. The leader's
  host seals them before proposing; a replica whose host has not sealed them
  retains the delivery (`CustodyPending`, retryable) and reports the pending
  import; its host seals and the next poll applies it. The legacy prefix is
  fenced while the record is in flight: legacy proposals get `Retry`.
- The image is bounded by the native checkpoint limit. A legacy ledger that
  does not fit is refused with a typed `Capacity` outcome and stays legacy;
  chunked checkpoints (R7) lift the bound.

### 5.2 Object mapping

| Legacy fact | Native representation | Provenance |
|---|---|---|
| Claim content | `ClaimDefinition` with issuer, subject and cause derived by `semantics_v1`; graph from `DependsOn`/`Awaits` claim targets; lineage corrections from `Supersedes`/`Amends`; an empty acceptance policy; `max_responses = 1`; binding content = the legacy content hash; the claim row carries `ClaimOrigin::Legacy` | the origin byte on the row; no `ClaimIdentity` row under the projection profile |
| Claim status history (`StatusFact` list) | one claim event per fact, kind `Imported(legacy sequence)`, native revisions `1..=n`; a `LocallyComplete` event for a locally complete non-terminal claim; an `OwnerReleased` event when the scope was released; the native revision counts these events, the legacy `revision` stays in the retained legacy core | a chain may open with `Imported` only under the `Import` invocation |
| Terminal status, local completion | explicit terminal cut at the import position; `local_sealed_at` = import position for terminal or locally complete claims | — |
| Work receipt | `Receipt` row and `ReceiptEntitlement` with the legacy fence and holder; `acquired` = import position; one `Receipt` event bound to the last `Imported` revision | the receipt fact may carry any epoch and bind to an `Imported` event only under the `Import` invocation |
| Validation definitions | `LegacyDefinition(id)` rows holding the frozen `Validation` bytes; registrations stay empty | never `Definition` rows: legacy programs, schemas and quality bars do not map onto native handler policies |
| Validation runs, attempts, verdicts | `LegacyRun(validation, ordinal)` rows holding the frozen `ValidationRun` bytes (recorded target hash, manifest, handler index, attempts, final verdict) | never `Evaluation`/`Accepted` rows: native attempts require result artifacts V1 never recorded |
| Testaments | `LegacyTestament(id)` rows holding the frozen `Testament` bytes (created and acknowledged sequences, manifest, summary, outcome); the claim's native response list stays empty | never `Response` rows: no cycle, slot or posting history was recorded |
| Evidence sets | `LegacyEvidenceSet(id)` rows holding the frozen `EvidenceSet` bytes | distinguishes open insertion from closed manifests and standalone registration |
| Artifacts | `Artifact` rows: descriptor fields verbatim, standalone provenance (no work or result role), custody re-verified from the local content tree under the import request key; `ArtifactIdentity(native hash)`; one `Artifact` event; inputs must name imported objects and inherit no visibility rule | `RequestKey { principal: producer, epoch: 1, id: derive("focal.native.import.request.v1", ledger, artifact) }` |
| Inline payloads | the row keeps the inline bytes and the sealed pointer computed with the recorded chunking | every replica seals from its own legacy core |
| Monitors | scopes on the owner claim (`WaitPredicate` roots, deadline, released cut) with the monitor allocation, link and head rows; `registered` = import position; one `Monitor(Registered)` and, for a released monitor, one `Monitor(Released)` event | — |
| Epoch windows, mutation receipts | not translated; the retained legacy core keeps answering legacy exact-retry and reconciliation reads | 18 §7 retained outcomes |
| Counters | `Meta` counts the rows above including a `legacy` counter; the outcome counts created claims, artifacts, receipts and events | — |

A legacy claim admits no native completion operation: acquire, post, progress,
adoption, admission, evaluation, reporting, response recording and derived
acceptance are refused with `InvalidTransition`; cancellation, revocation,
expiry, supersession, scope release, monitors and graph effects apply. A
legacy state that current admission would refuse (the synthetic broad corpus,
an artifact whose content is absent or fails its schema, a claim without an
issuer) is refused with a typed outcome naming the object; nothing activates.

### 5.3 Validation branches

Recovery validation gains exactly these branches, every one keyed on the
`Import` invocation or the row's legacy origin: a claim chain may open with
`Imported` at revision one; a legacy-origin claim must open that way, have an
empty policy and be created at the import position, and skips native response,
work and projection rules; a receipt fact may bind to an `Imported` revision;
an artifact event under `Import` resolves its custody request to the derived
import key, may carry no work or result provenance, and its inputs resolve to
legacy rows; legacy rows are counted, family-checked, decoded with the frozen
codec and must name an imported claim (runs must name a legacy definition).
Mutation records never carry `Import`, `Imported` or legacy rows.

## 6. Hosting and operation

Every node constructs its sessions with hosting over `<data>/content`
([network_service.rs](../../crates/focal-node/src/network_service.rs),
[embedded.rs](../../crates/focal-node/src/embedded.rs)). The support driver
makes hosted replicas promise and advertise the successor; `cluster replicas
activate-native --session` proposes activation through the replica admin
protocol; replica diagnostics report `native_hosted`, `native_ready`,
`native_active` and the compiled native decoder. Genesis activation uses the
authored content profile (claims are authored natively); a populated prefix is
imported projection-only because legacy claims carry no authored native
content. A laptop node that runs no network listener has no admin socket: the
same command opens the stopped data directory exclusively
([native_activation.rs](../../crates/focal-node/src/native_activation.rs)),
proposes under the node's own authority, polls until the record and genesis are
applied, checkpoints and returns; running it again on a native ledger changes
nothing. The `FCNSESS1` section retains the activation record's Raft index
(version 3, [22](22-native-record-format.md)) so a restored replica reports the
exact activation position.

**Streams and watches.** The Session keeps one continuous stream sequence
line ([07 F26](07-decisions-and-traceability.md)): the sealed legacy prefix
`0..=N` (the V1 domain sequence, `N = 0` on a genesis ledger) followed by the
native records, native record `s` of a genesis ledger being stream sequence
`s` and record `s > 1` of an imported ledger being `N + s - 1`, native sequence
one being the image of the legacy prefix and emitting no deltas. Cursor
positions, retention floors and delta identities are positions on that line
([native_deltas.rs](../../crates/focal-ledger/src/native_deltas.rs):
`stream_published`, `stream_sequence_of`); the registry validates every
position against the published end of the line while cursor envelopes and
receipts keep naming the sealed domain sequence. The legacy tail keeps its
retained schema-1 deltas; past the prefix a replay walks the committed native
events (`Key::Event` rows, [22 §3](22-native-record-format.md)) and builds a
schema-2 delta per event on demand (`DeltaFact::Native` with the exact
event record, the nearest legacy action, the request principal or the zero
participant for timers and the import, and the claim the fact concerns), so
a watch registered before activation continues without a resync and native
history never expires before the retention floor. Cursor receipts name the
published end of the line when their entry applies, computed identically on
every replica, so a receipt bounds every position its record names. The node
serves stream requests on a native ledger only under the native wire profile
(managed cursor requests carry that profile, [19](19-cli-mcp-implementation.md)),
takes the native read barrier as the stream prefix, and reports the published
end of the line in the reply token; because the native engine keeps no
historical read snapshot, a seeded watch reads its own seed through
linearizable native reads after the snapshot is pinned, hence at a prefix no
older than it, and the tail from the snapshot.

**Custody of native payloads.** An artifact-bearing native frame reaches the
owner only through the data service, which seals and verifies the inline
payload under the current custody placement and, exactly as for a sealed
upload, replicates the sealed bytes to every other required copy before the
frame is admitted ([evidence_service.rs](../../crates/focal-node/src/evidence_service.rs)
`attest_native`); an unreachable required copy refuses the frame, so a
follower that later leads or serves reads holds the payload under its own
custody and no replica admits a native artifact on one node's word.

## 7. Evidence

[session_native_tests.rs](../../crates/focal-ledger/src/session_native_tests.rs):
activation over an empty ledger through the unified Session with a full native
workflow, SS6 checkpoint, restart, exact retry and legacy refusal; a voter
without hosting blocking activation and a downgraded replica refused at open;
a lagging replica installing the SS6 checkpoint and taking authority by planned
handover; a populated legacy ledger imported on three replicas (followers seal
through the host path), keeping legacy exact-retry receipts, refusing legacy
commands, continuing native work, checkpointing and restarting with the
imported prefix; the transition under crash cuts (authority crash after
proposing, follower crash with the record appended but unapplied, authority
crash after activation before genesis, a late-sealing replica restarting with
the retained import) and across ancillary protocols (a protected watch
replaying without resync, cursor and managed receipts resolving, late legacy
managed work refused, everything surviving the checkpoint and a restart).
[import_tests.rs](../../crates/focal-core/src/native/import_tests.rs) proves the
translation (restore, identical re-encoding, the same root on another replica,
typed refusals) and
[fleet_import_tests.rs](../../crates/focal-node/src/fleet_import_tests.rs) the
node path (host sealing, an unsealed proposal refused with `CustodyPending`,
the ledger becoming native and authoritative). The standalone native session
suites in 22 §6 prove the engine both paths share.
