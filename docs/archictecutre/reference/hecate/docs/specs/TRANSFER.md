# SPEC: the transfer plane — ingest, upload, verified streaming

Status: ACCEPTED 2026-08-17 (with `WIRE_FORMAT.md`, "accepted for the sake of
output" — user; presented in-message through the full arc: TransferRecord sketch
rejected as hand-waved → ingest-state-machine draft → transfer-research revision
→ class-aligned verification addendum). Receipts on file (GRILLING.md research
index): S3/Azure/GCS/tus primary docs, RFC 9113/9000, BLAKE3 spec + bao/iroh,
BitTorrent v2, casync/restic/borg. Companions: `WIRE_FORMAT.md` (record shapes,
rejection clauses, vectors), `PROTOCOL.md` (framing, delivery classes, credit —
amended by §8's flow-control clauses), `OBJECT_TIER.md` (addressed-content
verbs, staging pack role, GC).

## 1. The scoping theorem (what needs machinery at all)

Content on the wire is in exactly one of two states:

- **Already-addressed content** (repair, cache-fill, migration, generation
  fetch, and any upload whose client computed identity locally): every chunk is
  an independent, self-verifying, idempotent unit. Concurrency is free, resume
  is free (`batch_exists` skips what landed), ordering is irrelevant, and **no
  transfer state machine exists on this path** — the OBJECT_TIER verbs
  (`put/get/batch_exists/ranged_get`) over credit-governed streams are
  complete. "Multipart" is just concurrent chunk movement. (TR9: architecture
  test — zero session state on this path.)
- **Unaddressed content** (a user submits a photo/video/audio, a Designer
  ingests a file, a live capture streams in): identity does not exist yet — it
  is *computed by the receiving boundary*. Only this case gets the §4 ingest
  state machine.

A client that chunks and hashes locally converts its upload into the first
state; the §3 flow is therefore the primary upload path, and §4 exists for
principals that cannot or should not compute identity (streams of unknown
length, thin SDK clients, capture-in-progress).

## 2. Verified streaming (the substrate both paths share)

- **Identity**: `ContentRef { root, len, class }` (`WIRE_FORMAT.md` §3b). The
  BLAKE3 tree root is **chunking-independent** (iroh, documented): storage
  chunking and transfer granularity can differ and change without touching
  identity. Keyed mode in private dedup domains (OBJECT_TIER §9).
- **No unverified byte is released.** Verification granularity = **16 KiB chunk
  groups** — a *derived* constant: the smallest group restoring full BLAKE3
  SIMD batching (spec §7.1: 16-chunk batches restore peak), outboard overhead
  64 B/group ≈ 1/256 of content, zero outboard for content ≤ 16 KiB (the
  common small-source-file case pays nothing); BitTorrent v2's independent
  convergence on 16 KiB is the corroborating receipt.
- **The length rule is law**: `len` is untrusted until the final chunk group
  verifies (BLAKE3 spec §6.4; iroh `last_chunk()`) — no EOF-relative operation
  before it, anywhere.
- **Class-aligned verification** (`WIRE_FORMAT.md` §3b, one law): CDC-class
  objects verify **per-chunk against the content-addressed manifest of
  standalone chunk hashes** (the trusted object — casync's model; BLAKE3
  chaining values are position-bound, so standalone hashes are the correct
  position-independent dedup identity). Fixed-framed media verifies against the
  **whole-blob BLAKE3 tree**, with stored bao outboards served for the three
  root-only cases: user-media streaming to a client holding only the root,
  cross-fence grants (a grantee verifying ranges of a blob it cannot list), and
  possession-proofs for the write-closed global domain (the Dropship rule).
  Verification structure follows content class — never two mechanisms for one
  object.
- **Ranged/streamed reads**: requests are `(root, chunk-ranges in 1 KiB units)`
  — iroh's GetRequest shape — served as verified streams. One verb for media
  playback-style consumption, generation-segment fetch, and cache fill
  (OBJECT_TIER §8's ranged verbs).

## 3. Upload = offer → missing-set → parallel verified streams → atomic commit

The synthesis the receipts rank first (casync index-then-fetch, Git want/have,
Azure block-list-commit generalized with hash-native IDs, IPFS want-have).
Offset-serial (tus/GCS) has no dedup slot and no parallelism; S3's ordinal
parts force boundary-dependent composite checksums — a decade-long repair. Ours:

1. Client sends the object's chunk-hash manifest (CDC classes) or bao outboard
   (media classes).
2. Server replies with the **missing set** — the `batch_exists` verb, now
   load-bearing: for duplicate content **this reply is the entire upload**.
   Dedup is the fast path, not a bolt-on — the primitive none of S3/GCS/tus
   possesses.
3. Missing chunks stream in parallel on bulk-class hecate-quic streams
   (SEALED_FRAME/bulk lane — D-10; a stalled stream is a scheduling event:
   re-request by identity on a healthy stream, the missing-set HOL escape),
   each verified on arrival per the class-aligned law. A failed chunk
   rejects that chunk, not the transfer.
4. **One atomic commit**: the ref/manifest CAS (`set_ref_if`). A failed
   conditional commit **orphans nothing** — chunks are content-addressed and
   global; the retry re-references them (versus S3, where parts die with the
   upload ID).
5. Unreferenced staged chunks GC by age (OBJECT_TIER §7 mark-sweep; Azure's
   7-day GC is the precedent, S3's bills-until-abort the cautionary tale).

Mid-chunk resume is deliberately absent: at our chunk sizes retransmitting a
partial chunk is cheaper than the bookkeeping (Google itself tolerates
whole-redo at small sizes).

## 4. The ingest state machine (unaddressed content only)

```
OPEN ──► STAGING ──► COMMITTING ──► COMMITTED
  │          │            │
  └──────────┴────────────┴──► ABORTED (explicit, lease-expiry, or hash-mismatch)
```

Messages (control = class 3 Directed; records = class 6 StreamData under
credit, carried on hecate-quic streams per D-10 — the credit clauses are
implemented natively in the owned transport, one flow-control law;
canonical shapes + vectors in `WIRE_FORMAT.md` §6):

```rust
TransferOpen {
    token: TransferToken,             // client-minted idempotency key; resume = re-OPEN with same token
    class_hint: ContentClass,         // selects chunking policy; verified at commit, never trusted
    declared_len: Option<ContentLen>, // None ⇒ streaming (single-part, unknown length)
    parts: PartCount,                 // structurally 1 if declared_len is None
    scope: QuotaScope,                // session/principal — admission + accounting
}
TransferOpened {
    transfer_id: u64,                 // receiver-minted, fencing-bound
    part_watermarks: Vec<ContentLen>, // zero on first open; durable progress on re-OPEN — this IS resume
    lease: Deadline,                  // derived staging lease; renewed by progress
}
PartRecord  { transfer_id, part: PartNo, offset: ContentLen, bytes }  // offset MUST equal the part's watermark
PartAck     { transfer_id, part, durable_through: ContentLen }        // batched; rung 1 below
TransferCommit    { transfer_id, declared: Option<ContentRef> }       // Some ⇒ verify-or-discard
TransferCommitted { content: ContentRef, descriptor: ManifestRef }
TransferAbort     { transfer_id }                                     // + implicit: lease expiry ⇒ abort, counted
```

Rules: per-part records strictly sequential (`offset == watermark`, else typed
reject — no sparse writes, no overlap ambiguity); parts partition
`[0, declared_len)` into client-chosen contiguous ranges, count bounded by §6;
each part is an independent stream with its own credit window — parallelism
across parts, ordering within a part. Re-`OPEN` with the same token returns
durable watermarks; duplicates at-or-below a watermark are acked-and-discarded.
Every message carries the standard fencing tuple; a transfer belongs to the
connection's principal and dies with its epoch.

## 5. The three witness rungs (what is acked — never blurred)

1. **`PartAck.durable_through`** = bytes group-committed to the receiver's
   **staging pack volume** (OBJECT_TIER §2's engine in the staging role — same
   format, scan-recoverable). Ack-after-durable, exactly the witness
   discipline. A crashed receiver re-derives watermarks by staging-pack scan;
   resume is truthful by construction.
2. **`TransferCommitted`** = content exists under its identity: chunked, in the
   real store, manifest written, `ContentRef` returned.
3. **Referencing** (a claim/descriptor pointing at it) obeys the standing
   placed-strictly-precedes-referenced ladder — commit does not imply fleet
   placement; the ref-flip does.

Transport credit is flow control (PROTOCOL §4), never a durability signal.

## 6. Chunking decoupled from the wire (the determinism move)

Parts transfer **raw byte ranges into staging**. Chunking runs **at commit, as
one sequential pass over staged bytes at device speed** — CDC or fixed-frame
per class policy. Each consequence is load-bearing:

- **Part boundaries cannot influence chunk boundaries** (TR3: same content
  under different part splits ⇒ byte-identical chunk set and `ContentRef`).
  Streaming CDC over parallel out-of-order arrival is structurally impossible
  to make deterministic; this design makes determinism trivial by sequencing
  the chunker.
- Unknown-length streams (single part, sequential) *may* chunk inline as an
  optimization, but the committed artifact must be identical to the commit-pass
  result (TR10 differential) — an optimization can never change identity.
- Content-type verification (magic bytes vs `class_hint`), EXIF hygiene, and
  parser sandboxing run at commit against staged bytes — Branch 26 owns the
  policies; this spec owns the hook point: **commit is the single gate between
  staged bytes and addressed content** — policy runs exactly once, on complete
  data, before identity exists.
- Hash mismatch on a declared `ContentRef`: the entire transfer discards
  (staging reclaimed, typed + counted). Never partial acceptance.

## 7. Bounds: formulas, then carriers

No literal appears as a bound; every bound is a derived value checked at named
validation points; carrier types are chosen after (`WIRE_FORMAT.md` §2b):

- `inline_value_cap` = `frame_cap − AAD − record_header` — transitively from
  PROTOCOL's one derived frame cap. Validated at encode and decode.
- `max_content_len` = `min(staging capacity anchor, manifest_chunk_count_cap ×
  max_chunk_size, per-scope quota)` — validated at OPEN (declared) and commit
  (actual). The object-size ceiling is manifest arithmetic, with
  HashSeq/piece-layer indirection past it.
- `parts_cap` = `ceil(declared_len / part_floor)` clamped by
  `ceil(BDP / credit_window)` — parallelism bounded by what the
  bandwidth-delay product can use; `part_floor` derives from the staging pack's
  append-batch anchor.
- `lease` = observed-throughput-floor × remaining bytes, renewed on progress —
  a stalled transfer dies in bounded time; a slow-but-moving one never does.
- Concurrent transfers per principal and staging bytes per scope: admission
  vector checks against summon/session budgets (SCHEDULER §6's Borg-quota
  discipline) — ingest cannot DoS the store.
- Derived-constants provenance table: BLAKE3 1 KiB leaf = fixed by the hash
  definition (the floor of range addressing); 16 KiB group = derived §2; CDC
  average for text = derived from borg's index-cost model (~40–164 B/chunk)
  against our corpus and RAM anchors (restic/borg's 512 KiB–8 MiB envelope as
  sanity band); fixed media framing = casync-author bit-avalanche statement
  (flagged THIN — sole primary source); scheduling quantum = k × chunk-group ×
  credit-window depth, explicitly **not** S3's 8 MiB (that amortizes
  per-request HTTP/TLS overhead a persistent multiplexed protocol doesn't pay).
  Rejected magic numbers, confirmed rationale-free in their own docs: S3
  5 MiB/5 GiB/10,000; GCS 256 KiB; gRPC 4 MiB.

## 8. Flow control (amendments this spec places into PROTOCOL.md)

Each with its receipt; the clauses live in PROTOCOL §4, cross-referenced here:

1. Credit at **both stream and connection level** (RFC 9113 §5.2 + RFC 9000
   §4.1: one bulk stream must not exhaust the connection buffer).
2. Credits as **absolute offsets, QUIC-style** — idempotent under loss/reorder,
   mechanically better than HTTP/2's deltas.
3. Windows derived as `k × frame_cap` per delivery class, **BDP-autotuned**
   (QUIC's practice; OpenSSH's 64×-bulk/4×-interactive pattern as the citable
   derivation shape), with the **never-whole-object-in-credit** test: no credit
   grant may admit an entire content object into buffer.
4. Bulk transfer runs in its **own delivery class** with dumb (strict-priority-
   free) prioritization, so the frame-cap control-latency law holds — control
   frames never queue behind bulk.

## 9. Test matrix (failure each catches)

| # | Test | Catches |
|---|---|---|
| TR1 | Resume fuzz: kill client/receiver/connection at arbitrary frames; re-OPEN completes; final bytes identical; zero duplicate chunk-store writes | resume fiction; staging drift |
| TR2 | Declared-hash mismatch ⇒ whole-transfer discard, staging reclaimed, typed+counted; nothing addressable exists | partial acceptance |
| TR3 | Part-split independence: same content, N partitionings ⇒ identical chunk set + ContentRef | wire topology leaking into identity |
| TR4 | Unknown-length stream: single-part ingest, identity at commit; inline-chunk optimization differential vs commit pass | streaming as a second identity path |
| TR5 | Lease expiry: stalled transfer aborts at derived deadline; slow-but-progressing never aborted; staging reclaimed by scan | leaked staging; punished slowness |
| TR6 | Admission storm: transfer floods ⇒ typed+counted quota refusals; existing transfers' credit windows unaffected | ingest starving the plane |
| TR7 | Credit stall mid-part: zero-credit halt, bounded memory, no timeout-abort while lease renews on eventual progress | flow-control/lease interaction |
| TR8 | Crash at every commit step ⇒ fast-forward: COMMITTED with full artifact or clean ABORTED; never half-addressed content | the torn-commit class |
| TR9 | Architecture: the addressed-content path holds zero transfer state | machinery creep onto the idempotent path |
| TR10 | Chunk determinism: delivery-order/part fuzz + inline-vs-commit chunking ⇒ identical manifests | nondeterministic identity |

Plus: verified-streaming conformance (no byte released before its group
verifies; length untrusted until final group — negative vectors in
`WIRE_FORMAT.md` §6), missing-set dedup fast path (duplicate upload transfers
zero content bytes), failed-commit orphan test (retry re-references, zero
re-upload).

## 10. Seam statement

TRANSFER.md owns wire mechanics + staging + the commit gate + resume.
Branch 26 keeps content policy: class-assignment, content-type verification,
EXIF hygiene, parser sandboxing, the per-class chunk-policy table — the branch
inherits a settled transport instead of designing one. OBJECT_TIER owns the
addressed-content verbs this spec explicitly does not duplicate, plus the
staging pack role (§2 there). `WIRE_FORMAT.md` owns every record shape,
rejection clause, and vector.

## 11. Acceptance criteria

1. No state machine, session state, or bookkeeping exists on the
   addressed-content path (TR9 permanent).
2. Identity is delivery-independent: TR3 + TR10 green under fuzz before any
   media feature ships.
3. Every ack maps to exactly one of the three rungs; no code path treats
   credit or `PartAck` as placement (architecture test).
4. All §7 bounds carry derivations at definition sites; no width or size
   literal is cited as a bound anywhere in the transfer plane.
5. Duplicate-content upload completes with zero content bytes transferred
   (missing-set test, measured).
6. A failed commit CAS orphans nothing and the retry transfers nothing
   (measured).
7. Crash injection at every state edge (TR1/TR8) green before ingest accepts
   its first real submission.

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
