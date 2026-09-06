# SPEC: the claims-plane protocol — two planes over UDP + hecate-wire

Status: ACCEPTED 2026-08-18 (whole-spec verdict "accepted." — Branch 3;
amended under the maximal audit A1–A6 + the D-10 a–e settlements + the
§1.1 header-encryption revision folded in-exchange). Amended 2026-08-20
(CACHE/QUEUE/FANOUT acceptance): delivery class 7 EphemeralAtMostOnce
(append, no renumber), the ephemeral-at-most-once archetype + queue/topic
archetype declarations, and the fan-out-degree axis on the
non-interference scale-walk. Companions:
`WIRE_FORMAT.md` (payload codec, normative), `WIRE_SECURITY.md` (seal-once
pipeline, flow keys, boot classifier), `TRANSFER.md` (content plane),
`CONSENSUS.md` (fencing authorities, epoch scoping). Scope note: hecate-wire
is Rust-only; TS/Python SDKs speak MCP/JSON at the tool plane and never
touch the claims plane; format stability is guaranteed by committed test
vectors, never cross-language parity.

## 1. Two planes, one substrate: everything rides UDP

**There is no TCP anywhere in the mesh** (D-10(d), verbatim law: "no
fallback. Period. QUIC + UDP over TCP utilizing the standard(s) we just
designed."). Every participant — host, pod (via its host, per
`WIRE_SECURITY.md`), user terminal — speaks the same two planes. The
cleartext rule spans both: **cleartext on the wire is only what is needed
to find the key.** (RUNTIME §5's provider-egress HTTP is the external
boundary to third-party APIs — their wire, not this mesh.)

### 1.1 The control plane: stateless bare-UDP datagrams

Carries the supersession and idempotent-fenced-control archetypes:
consensus votes/terms/fencing probes (Raft rides this plane —
protocol-sound because Raft is loss-tolerant by design: idempotent
AppendEntries, leader retry), node-liveness fabric support claims,
membership, gossip, health piggyback, class-1 telemetry. Loss is absorbed
by supersession or idempotent retry; nothing here is ever retransmitted by
the transport (a stale heartbeat resent is anti-information).

**Datagram layout (header-encrypted — the §1.1 revision, 2026-08-18)**:

```
cleartext prologue (the key-finding minimum):
  ver: u8
  key_hint: sender_id (u64) ‖ key_epoch (u32)   — or the opaque derived key-id form
  payload_len: u16
crypto:
  nonce: 12 bytes   = 64-bit per-(key, direction) counter ‖ 32-bit channel id.
                    Counter-based, never random; reuse refused + typed +
                    counted in every build (no assertion path — the no-panic law).
  ciphertext + 16-byte tag  (AES-256-GCM), containing the FULL envelope —
    kind, class, flags(must-be-zero), hlc, cluster_id, epoch, sender_term,
    src_pod, dst_pod, request_id, trace_ctx, schema_hash — followed by the
    hecate-wire payload. Encrypted AND authenticated, not merely
    authenticated: the former AAD-cleartext posture was a fossil of the
    deleted on-path-policing assumption (policing is at endpoints, which
    hold keys; nothing between endpoints routes on envelope fields — IP
    routes).
```

Payload budget derived at the datagram layer from **per-path MTU**
(loopback/jumbo included — the 1500 anchor is a floor derivation input,
never a hard-code); the builder rejects oversize before send.

**`trace_ctx` (TRACING.md §2)**: the execution-tracing context —
`{trace_id: 16B, span_id: 8B, flags: 1B}` — mandatory on every message
(codec-rejected absence). It is not key-finding data, so it never rides
the cleartext prologue; inside the envelope it is authenticated and
warden-readable. **Distinct from `request_id`**: `request_id` pairs a
response with its request (transport); `trace_ctx` threads one
operation's execution across hops (observability) — the two never merge.
A message sent while servicing a traced operation carries that trace's
id + the sender's current span; a message minted outside any traced
operation roots a fresh trace (there is no untraced message class —
sampling, not exemption, is what keeps background classes cheap).

**Minting authorities (A1, per CONSENSUS §1/§6)**: `cluster_id` =
deployment-wide, root-group-minted at creation. `epoch` = the sender's
**region membership epoch** (region meta group's authority). Root-plane
cross-region control (the only cross-region control traffic; human-cadence)
validates against the region directory. Consensus messages are class-0
citizens; the node-liveness fabric rides the same class.

### 1.2 The session plane: hecate-quic

`hecate-quic` (owned, RFC 9000/9002 dialect-as-exemplar — D-10(b)) carries
every reliable class: turn streams, delta streams, consults, claims,
directed commands, secrets issuance, registry ops, all content transfer,
cross-region included. Sessions exist **host↔host and host↔terminal
only**; pods reach the plane through the WIRE_SECURITY pipeline (staging
ring → warden seal → host session). Private version + private Initial
salt; Noise-IKpsk2 handshake in CRYPTO frames; connection identity =
host/enrollment identity; a term/epoch advance kills the session with a
typed reason. Header protection per the dialect — the cleartext rule holds
natively. Four frame classes (`WIRE_SECURITY.md` §4): `CTRL` (full packet
protection), `SEALED_FRAME/claims` (warden-sealed payload + hop GMAC —
transport retransmit preserved), `SEALED_FRAME/bulk` (payload exempt under
the name-verify + envelope-truncation guard), `DATAGRAM_SUPERSEDE`
(RFC 9221-shaped, no retransmit). **The envelope is the fixed pre-codec
layer**: its length field is where `WIRE_FORMAT.md`'s `FrameLen` cap is
enforced numerically, beneath the typed length domains (A6).

### 1.3 Parser-resident enforcement — at every acceptance point

Universal order (WIRE_SECURITY layer 3), revised for the encrypted
envelope: length caps → prologue parse → **key lookup (unknown sender =
drop, zero crypto spent)** → AEAD verify + decrypt (one bounded pass —
the same cost QUIC endpoints accept for the same posture) → envelope parse
(flags must-be-zero, version) → fencing check → replay window → admission
(§3) → decode/unseal. Enforcement points: host parsers (both planes),
guest unseal paths, terminal clients — identical order, identical
categorized counters everywhere. **The HLC window serves liveness only**
(A2): it bounds replay-window memory; safety rests entirely on nonce
counters + AEAD + fencing; under the FAULTS clock nemesis the failure mode
is typed rejection, asserted (P15). An attacker without keys cannot
present well-formed admission fields — garbage dies at the tag, counted.

## 2. Keys and identity

Owned by `WIRE_SECURITY.md`; this section defers to it: one summon-mint
root per pod with HKDF-labeled **per-workload** derivations (amended
2026-08-22, the five-site flow-key landing — the pod's four interior
workloads {primary, Scribe, sensor, init} each derive their own flow keys
from the pod root; `k_workload = HKDF(pod_root, pod_uid ‖ workload_id ‖
epoch)`), atomic `key_epoch` rotation at handoff (all four re-derive
together); per-flow end-to-end keys (container-custodied receive keys,
host-brokered seal-key grants with counter epochs, never wall-clock);
host/terminal Noise identities for sessions; the warden seals what it
inspected; sending guests hold no transport keys. **A compromised WORKLOAD
speaks only as itself** — enforced by physics (fd and key custody per
container) — and therefore a compromised pod still speaks only as that
pod (the mint root is pod-scoped). The envelope stays pod-granular
(`src_pod`/`dst_pod`, unchanged — no version bump): workload identity is
carried by the key itself, so the cleartext `key_hint` is
workload-granular **by construction** and mis-attribution is
unrepresentable — there is no sender-written identity field to
cross-check or to lie in.

## 3. Classes, archetypes, admission

Delivery classes: `0 Control`, `1 Observation` (sheddable), `2 Phase`,
`3 Directed` (never shed), `4 ConsultRequest`, `5 ConsultResolved` (never
shed — issuer is parked on it), `6 StreamData` (credit-governed, §4),
`7 EphemeralAtMostOnce` (sheddable / unordered / counted-drop; mClock
reservation derived at this site = target ephemeral delivery rate × p99
datagram bytes — a guaranteed **minimum** share so this sheddable class
can never be starved, its idle-borrow never priority over control).

**Every message kind declares exactly one traffic archetype**
(supersession / idempotent-fenced-control / ordered-log /
directed-request-response / **quorum-critical transfer** / bulk /
**ephemeral-at-most-once**); the archetype — never the subsystem —
determines carriage and lane. **`UplinkInterval` (`COLLECTOR.md` §7) registers
under `supersession`, class-1 carriage** — a newer absolute for the same
`(node, series, window)` supersedes an older one and nothing is ever
retransmitted, exactly supersession's contract (amended 2026-08-22, COLLECTOR
acceptance). **Ephemeral-at-most-once** (CACHE's
at-most-once pub-sub, FANOUT's ephemeral topic delivery — delivery
class 7) REUSES supersession's no-retransmit datagram for carriage, but
its drops are counted as their own category: a counted ephemeral drop is
NOT supersession's anti-information drop (a superseded non-delivery is
uncounted by design), so the drop taxonomy stays exhaustive. New-kind
archetype declarations (QUEUE/FANOUT): queue records → ordered-log;
queue/topic control commands (enqueue / lease / ack / publish / subscribe)
→ directed-request-response; durable topic-subscription delivery →
ordered-log (it rides a QUEUE).

**The traffic non-interference law** (elevated to system law 2026-08-18,
user directive: "different types of traffic for different work should NOT
block one another with the scheduler, and our scheduler needs to be smart
enough to know the difference"): the transport's bandwidth scheduler
arbitrates **by declared class with reservation + weight + limit** (the
Ceph-mClock shape, implemented natively in hecate-quic — a D-10(b) crate
obligation): every class holds a guaranteed minimum share, may borrow idle
capacity, and can never be starved by another class. **Class membership is
by purpose, never by volume** — the Kafka KIP-73 lesson as law: bulk-shaped
traffic that a commit or quorum is waiting on is quorum-critical, not bulk.
Quorum-critical transfer membership, enumerated closed: green placement
pushes (MERGE §5); consensus snapshot/log catch-up to a member whose
currency quorum needs. Opportunistic bulk: cache-fill, prefetch,
durable-plane archival replication. The control-latency law is untouched
in both directions: control frames never queue behind any bulk class, and
reservations arbitrate bandwidth among bulk-shaped flows, never priority
over control. Reservations derive at definition sites (e.g. the
quorum-critical reservation = target merges/sec × p99 placement bytes per
merge).

**The structural non-interference guarantee** (strengthened 2026-08-18,
user directive: "a 2GB upload CANNOT possibly block other work, control
frames, etc."): reservations are policy; this is construction. **The
invariant: no class's latency bound contains any term dependent on another
class's object size or queue depth.** Enforced per shared resource, each
by one of two structural means — *partition* (the resource is per-class;
interference is unrepresentable) or *quantum bound* (another class's
maximum occupancy is one frame-cap quantum, independent of object size):

| Shared resource | Structural guarantee |
|---|---|
| The wire | Frame cap: no frame larger than the cap can be BUILT (builder rejects) — a 2 GB object is ~1.4M independent packets; worst-case control queueing behind bulk = one packet transmission (~µs), invariant in object size |
| Transport queues | **Partitioned per class** — separate queues, not one queue with priorities; control's queue physically cannot contain bulk occupancy; head-of-line applies within a class only |
| Buffers/credits | Per-class credit pools + the never-whole-object-in-credit invariant: bulk credit exhaustion backpressures the SENDER; the reserved-slot discipline (already law for the health plane) generalizes to every class — no class can consume another's buffer allocation |
| Host CPU/crypto | Bulk payloads carry ZERO host cryptography (Lane-A passthrough: D-3 ciphertext, name-verified) and envelope-only warden inspection (metadata-completeness law) — the 2 GB upload does per-packet envelope work only; its payload bytes never enter an AEAD, a parser, or a policy evaluation on any host |
| The pod device boundary | Class-separated virtqueue pairs (WIRE_SECURITY §2) — a pod's own upload cannot HOL-block its own claims frames at the ring |
| Store disk IO | **The owed instantiation, now with its structural form named**: per-class IO queues at the pack store with reservation arbitration (the Seastar/Scylla scheduling-group shape; io_uring submission partitioning) — ingest staging writes and archival bulk cannot occupy the WAL-flush or placement-read queues. Owed to OBJECT_TIER/RUNTIME as a named rider, with THIS table's invariant as its acceptance bar |
| Store memory/arena | Per-class/per-volume budget charges (existing budget doctrine) — bulk fill cannot evict or starve another class's arena share |

**The scale walk, as the permanent test — megabytes to petabytes** (the
invariant is scale-free by statement: *no size term, period* — so the
proof must span the range we run at, laptop to Meta scale): saturating
transfers run while control frames, claims traffic, and merge placement
proceed — **their p99 latencies AND memory footprints must be flat across
a sweep of transfer sizes spanning MB → GB → TB in real CI tiers, and
PB-class in the deterministic cluster-SIM**, where simulated bytes are
free and a petabyte walk costs seeds, not days (the SIM's reason to
exist). Any size term appearing in any other class's latency or memory
curve is a structural failure, not degradation.

**A second, orthogonal axis walks the same invariant — fan-out degree**
(the CACHE/QUEUE/FANOUT amendment): 1 → millions of subscriptions on a
single topic, PB-class in the deterministic cluster-SIM. The control,
quorum-critical, and claims classes must hold **flat p99 latency AND flat
memory footprint** across the degree sweep exactly as across the size
sweep — a fan-out-degree term appearing in any other class's latency or
memory curve is a structural failure, not degradation (fan-out cost stays
confined to the ephemeral-at-most-once class that owns it).

Three failure modes that exist only at the top of the range, named so the
walk exercises them rather than discovering them:

1. **Rekey-in-flight**: a PB at line rate crosses the derived AEAD
   invocation limits mid-transfer — key rotation (hop-session key update)
   must proceed without pausing the transfer or perturbing any other
   class (tested at the SIM tier with limits scaled down to force
   rotations).
2. **Duration-invariance**: a multi-day transfer WILL see leadership
   changes, epoch bumps, node deaths, and its own lease renewals — and
   must resume by missing-set from wherever it was, never restart-from-
   zero, because its only state is content addressing (the TRANSFER
   scoping theorem, now asserted at PB duration under the nemesis matrix).
3. **Fleet-aggregate effects**: per-node reservations do not compose into
   fleet guarantees by themselves — a PB rebalance saturates the
   opportunistic class on MANY nodes at once, and destination incast is
   bounded by receiver-driven credit admission (already the design, now
   asserted at fan-in under aggregate load); the maintenance work a PB
   transfer *generates* (staging leases, GC of unreferenced staged
   content, scrub of the new content) is itself opportunistic-class by
   the purpose rule — a transfer must not be able to promote its own
   cleanup into anyone's critical path.

CPU-instantiation of the law (scheduling-group shares for maintenance vs
serving work on hecate-rt) rides the same rider. Tests: the starvation pair
(opportunistic saturation ⇒ quorum-critical latency within derived budget;
quorum-critical bursts never delay control) + catch-up membership (a
quorum-needed member's snapshot joins the critical class and completes
within the failover budget under full background load). **The metadata-completeness law** governs
every admission decision in the system: no verdict anywhere may require
payload plaintext (payload sight is the minting authority's deliberate,
logged escalation, never fast-path). Classification is boot-validated via
the transport registry (`WIRE_SECURITY.md` §6): an unclassified kind or
path fails startup.

Admission runs in the protocol callback before any task exists: per-class
caps + named admission groups with reserved slots (the health plane owns
capacity the data plane cannot touch); Control gets head-of-line
scheduling into the next executor iteration. Caps derived from shard count
and queue bounds; every shed is a counted drop; classes 0/3/5 are never
shed — overload surfaces as backpressure (credit exhaustion or typed
retryables), never silent loss. Admission fields are trustworthy because
the hop layer authenticates-and-decrypts the envelope **before** admission
logic runs (the ingress-tamper settlement).

## 4. Delta streams (hecate-quic ordered streams)

- Subscribe `{subject, from_seq}`; records carry the ledger sequence as
  `stream_seq`; delivery strictly ordered, no gaps — the ordered-log
  archetype on a dedicated stream per (session, subject). Named subjects
  include the green-chain subscription (D-11's record, on its acceptance).
- **One flow-control law, natively ours**: `hecate-quic`'s stream +
  connection flow control *is* the ratified credit design — dual-level,
  absolute-offset credits (idempotent under loss/reorder), windows derived
  as `k × frame_cap` per delivery class and BDP-autotuned, the
  **never-whole-object-in-credit** invariant permanent. No delegated
  second law exists; we own the transport, so the credit clauses are
  implemented *in* it.
- Resume: reconnect with cursor; the server replays forward. Cursor below
  the retention floor → typed `RESYNC_REQUIRED` with a snapshot handle;
  the client re-derives deterministically (watermark-recovery invariant).
  No best-effort repair path exists.
- Idle streams heartbeat in-stream (liveness is per-stream). Bulk
  (class 6, `TRANSFER.md` traffic) runs in its own delivery class with
  dumb prioritization — control frames never queue behind bulk, which is
  the frame cap's reason to exist.

## 5. Liveness and observability

- Dedup eligibility is a structural property of each message kind;
  probes/acks/votes are never content-deduped. Supersession-archetype
  datagrams are never retransmitted — the next one supersedes.
- Terminal-abort barriers checked before dispatch and after handler
  return; endpoint shutdown is ownership-tree-ordered (RUNTIME's
  task-lifecycle law) — a restart never inherits a dead process's
  connection set.
- Categorized drop counters (rate_limited / too_large / decrypt_failed /
  malformed / replayed / shed / ephemeral_dropped / non_canonical /
  unknown_sender),
  aggregated as periodic structured records. A drop with no signal is a
  bug.
- Dead peers are discovered by the owning plane: the node-liveness fabric
  (fleet) or session keepalives (terminal).

## 6. hecate-wire (payload codec)

Defers to `WIRE_FORMAT.md` (normative, accepted) on every point:
canonical-or-reject with a rejection clause and negative vector per rule;
the two length domains (`FrameLen`/`ContentLen`); `ContentRef`/
`ContentClass` and the inline-vs-reference derive law; append-only,
ancestor-hash evolution, trybuild-gated; the committed vector corpus;
one derive, one interpreter, no serde/schemars anywhere.

## 7. Test matrix (failure each catches)

| # | Test | Catches |
|---|---|---|
| P1 | Roundtrip property fuzz (via WF1) | codec correctness |
| P2 | Canonical-reject fuzz (via WF2) | content-identity break class |
| P3 | Garbage/truncation fuzz: no panic, bounded time/allocation at every enforcement point | untrusted-input DoS |
| P4 | Evolution: ancestor-hash decode; snapshot compile gates (via WF4) | silent wire breaks |
| P5 | Envelope statics: prologue layout compile-asserted; flags must-be-zero enforced post-decrypt | layout drift, dirty reserved bits |
| P6 | Parser-order instrumentation: stale epoch/term, replayed nonce, bad tag, unknown sender each rejected with counters at the specified stage, before any dispatch — at every enforcement point class | enforcement behind dispatch |
| P7 | Nonce discipline: per-(key,direction) counters monotonic; reuse refused + typed + counted in every build; SIM oracle flags reuse as harness defect | AEAD nonce catastrophe |
| P8 | Envelope tamper: any flipped ciphertext/prologue byte ⇒ tag failure / key-lookup miss, counted; **header-privacy vector: on-wire capture shows prologue fields only** | header malleability; metadata leakage |
| P9 | Admission reserve: data-plane flood in SIM; health-group probes still admitted; sheds counted | control-plane starvation |
| P10 | Stream kill/resume at arbitrary points: no gap, no duplicate at any cursor; RESYNC path exercised end to end | delta loss/dup; dead resync path |
| P11 | Credit stall: zero-credit halt, bounded memory; classes 3/5 never dropped | flow-control fiction |
| P12 | Per-path MTU: datagram builder rejects oversize (computed with prologue+nonce+tag); loopback/jumbo paths derive larger budgets | wrong-layer MTU budget; fragmenting groups needlessly |
| P13 | Fencing sweep: every kind × stale (cluster/epoch/sender/term) combination rejected, both planes | zombie senders |
| P14 | No-TCP structural: no TCP socket, listener, or dependency exists in the mesh (registry cannot construct one) | fallback creep |
| P15 | Clock strobe: HLC-window rejections typed+counted; every safety oracle green (FAULTS clock nemesis) | wall-clock in a safety path |
| P16 | Missing-set escape: stalled bulk stream ⇒ re-request by identity on a healthy stream/session within derived bound; zero duplicate store writes | HOL as a wait instead of a scheduling event |
| P17 | Incast: O(10k)-class fan-in in cluster-SIM ⇒ credit-bounded buffer occupancy, zero collapse | the storage fan-in class |
| P18 | Loopback ratchet: session-plane N=1 throughput ≥ ratcheted baseline | laptop regression |
| P19 | Archetype coverage: every message kind classified; boot check red on any unclassified kind | carriage chosen by subsystem instead of archetype |

## 8. Acceptance criteria

1. `WIRE_FORMAT.md` merged before codec implementation (satisfied).
2. Continuous fuzz (P1–P3) in CI: zero panics, zero non-canonical
   acceptances — permanent.
3. Parser-resident enforcement order verified by P6 instrumentation at
   every enforcement-point class on every merge.
4. Classes 0/3/5 show zero drops across SIM overload sweeps; the drop
   taxonomy is exhaustive (unknown-drop = bug).
5. One frame cap per plane, derived; datagram budgets derive from per-path
   MTU including all prologue/crypto overhead.
6. Prologue layout compile-asserted; any change is a version bump.
7. Schema snapshot committed; breaking type changes fail compilation.
8. Two SIM runs, same (seed, trace): byte-identical wire sequences.
9. Every derived constant carries its derivation at the definition site.
10. Epoch fields have named minting authorities per CONSENSUS §6,
    boot-validated.
11. No dangling cross-references: LEDGER §7.3 and ADR-0002 corrected to
    this spec + `WIRE_SECURITY.md`; SIBYL's grants citation corrected to
    Branch 25 — in the acceptance commit (done).
12. No TCP exists in the mesh, structurally (P14 permanent).
13. Every kind carries its archetype; carriage follows archetype only
    (P19 permanent).
14. Cleartext on any wire is only what is needed to find the key (P8's
    header-privacy vector permanent, both planes).
