# SPEC: the ledger WAL — logical logs over derived-ω physical streams

Status: ACCEPTED 2026-08-17 (whole-spec verdict "amend and accept" under the
maximal audit — five amendments folded: consensus-substrate clause (logs for
every group incl. the meta tree, raft record kinds, entries-then-HardState by
append order, prefix-truncation API), FAULTS §2 disposition conformance
(rebuild-from-quorum when replicated; refusal is the N=1 disposition),
checkpoint ownership (WAL owns the floor API only), hecate-wire payload law +
CRC-is-not-identity clause, cluster-SIM extension + Branch 25
encryption-at-rest interlock flag). **Amended 2026-08-20 (CACHE/QUEUE/FANOUT
acceptance)**: inline-body budget exception (§1), queue-partition + fan-out
durable-subscriber logical-log clients with floor = contiguous-acked prefix (§6),
queue/topic record kinds (§3). Ratified direction: per-group
**logical logs** (one sequencer each — replay, watermark, and monotone-cut invariants
untouched) multiplexed over **ω physical WAL streams** (durability only, never an
ordering authority), CRDB/TiKV-lineage; **one durability policy: always-full**,
naturally batched, pipelined. References: TiKV raft-engine (writer group, purge),
Pebble/etcd WAL discipline, TigerBeetle batching, fsync research on file
(GRILLING.md).

## 1. Model

- **Logical log** = per-group ordered record stream, `log_seq` strictly monotonic,
  written only by that group's sequencer task. **A logical log serves every
  consensus group** — session groups *and* the meta-tree groups (root,
  per-region; `CONSENSUS.md` §1), which postdate this spec's first draft.
  For a session, the sequencer is the ledger core (under replication, the
  Raft leader — locally a 1-replica group whose append self-acks through the
  same call path).
- **Physical stream** = an append-only segment chain on disk. Records from many
  logical logs interleave, each tagged `(log_id, log_seq)`. A stream has exactly one
  writer task (single-owner, per §Runtime); "multi-writer" is the MPSC front-end.
- Artifact **content never enters the WAL** — content-addressed blob store; the log
  carries references. The law binds **bulk** content only: a logical-log client MAY
  inline a payload body up to the derived `WIRE_FORMAT.md` §3c inline budget
  (`frame_cap − AAD − record_header`); above that budget the record carries a
  `ContentRef` and the body lives in the content store. A below-budget inline body is
  governed exactly as index and consumer-state records already are — small by
  derivation; log records stay small, flush batches stay dense. `QUEUE.md` §2 relies
  on this budget for small message bodies.
- **The consensus-substrate clause** (`CONSENSUS.md` §§2/5 consumer contract):
  record kinds include the raft set — `entry`, `hard_state`, vote records.
  **Entries-then-HardState holds by append order**: single-writer append-only
  means a HardState can never be durable ahead of the entries it references —
  same flush batch ⇒ atomically durable together; a cut between them loses
  the HardState and retains the entries, the safe side. CS9 executes at this
  layer (W9). A **prefix-truncation API** serves CONSENSUS §5's host-owned
  truncation (watermark machinery of §6, with the raft consumer named and
  the slow-vs-dead follower debt bound honored — W11).
- **Payload encoding law**: record payloads are hecate-wire canonical
  (`WIRE_FORMAT.md`), carrying the schema discipline. The header's CRC32C is
  **transport integrity for the recovery scan, never content identity** —
  BLAKE3 remains the only content hash in the tree (the one-hash law,
  unbroken).

## 2. Derived-ω topology (constants from data)

At boot, measure the device: k flushes on a preallocated probe file → `flush_p50`,
plus per-stream submission overhead `submit_cost` (measured, not assumed).
Choose ω ∈ {1..N_shards} maximizing the closed-form throughput model:

```
throughput(ω) = ω × batch_size(ω) / (flush_p50 + submit_cost)
where batch_size(ω) = expected arrivals during one flush at rate λ/ω (natural batching)
```

— cheap flush (PLP NVMe, µs-scale) drives ω → N_shards (parallelism dominates);
expensive flush (consumer NVMe ms-scale, macOS F_FULLFSYNC 17–24ms) drives ω → 1
(batch amortization dominates). λ starts from the arrival-rate prior recorded on
last run (first boot: model evaluated at the measured probe rate) and ω is
re-derived per boot, never hand-set. The derivation and its inputs are logged.

Session→stream assignment: stable hash of session id over ω; a session's records
never split across streams within an epoch (recovery locality; migration §6).

## 3. Record and segment format

- **Segments**: fixed-size, preallocated and zero-filled at creation (file length
  never changes → `fdatasync` suffices on Linux; no metadata journal churn).
  Segment size derived from measured write bandwidth × target rotation interval.
- **Records**: 8-byte aligned; header
  `{len: u32, crc: u32, log_id: u64, log_seq: u64, stream_seq: u64, kind: u8, ver: u8}`
  then payload, zero-padded to alignment. `crc` is CRC32C **chained** — computed over
  (prev record's crc ‖ header-sans-crc ‖ payload) — so a valid-looking stale record
  in a recycled segment can never be accepted (ghost-record defense).
- **Record kinds** (`kind: u8`): the kind byte-space is **append-only — existing
  kinds are never renumbered** (the §1 raft set — `entry`, `hard_state`, vote —
  included). Queue/topic clients add `queue_item` (an enqueued message body),
  `consumer_state` (the consumer-state delta: an ack/floor advance), and `topic`
  (the fan-out topic record); the collector capture adds `capture_checkpoint`
  (a delta-stream cursor + the bounded per-claim watermark map — `COLLECTOR.md`
  §10; amended 2026-08-22, COLLECTOR acceptance).
- **Recovery** per stream: sequential scan, verify chain, with etcd's
  torn-vs-corrupt discrimination and **dispositions conforming to
  `FAULTS.md` §2** (amended 2026-08-17 — the universal-refusal path is
  deleted): examine the failed region in 512-byte-aligned sectors —
  all-zero sectors ⇒ **torn write**: truncate, only unacked records lost,
  safe by the witness discipline; non-zero garbage ⇒ **detected body
  corruption**, typed disposition by replication state: a replicated log
  ⇒ **rebuild-from-quorum** (fetch the committed entries from peers;
  refuse to serve until repaired); N=1 or quorum-unavailable ⇒ **refuse
  loudly** (typed, names stream/offset, operator-surfaced — never guess).
  Never silent truncation of acked data; every disposition counted and
  categorized. **No doublewrite** — append-only logs never overwrite acked
  bytes, so page-store machinery is waste.

## 4. Commit path

- Producers send `Append{log_id, records, ack}` over a bounded MPSC to the stream's
  writer task. **Natural batching, no timers**: everything that queues while a flush
  is in flight rides the next write+flush (etcd model). `commit_many` pipelines a
  producer's burst as one message; entries are individually acked (a pipeline, not a
  transaction).
- Durability primitive per platform, one policy — **always-full**:
  Linux `fdatasync` on preallocated segments via **io_uring linked SQE
  (write→fdatasync)**; macOS `F_FULLFSYNC` (`File::sync_all`); Windows
  `FlushFileBuffers`. Ack is sent only after the durable return. Accepted cost on
  ledger: ack p50 ≈ one flush (µs on PLP, ~0.5–5ms consumer NVMe, 17–24ms macOS) —
  invisible against seconds-scale agent turns; nothing latency-sensitive awaits a
  flush.
- Backpressure: full producer queue ⇒ typed retryable error with derived retry hint,
  counted — never blocking the producer's shard, never unbounded growth.

## 5. Replication hook

The sequencer appends through the consensus-group API in every mode. Locally the
group has one replica and the leader's durable-append self-acks; distributed, the
same call path runs 3-replica Raft with pipelined appends (leader = sequencer). The
physical stream stores the group's log entries below consensus. No local-only commit
shortcut exists.

## 6. GC and migration

- Reclaim is watermark-driven per logical log (snapshot floor + retention). Segments
  free when all resident records are below their logs' floors; long-lived stragglers
  are **rewritten forward** (raft-engine purge model) rather than pinning old
  segments.
- **Checkpoint ownership**: the WAL owns **only the floor API**. Each
  logical-log client owns its checkpoint cadence and format — the ledger
  core per `LEDGER_CORE.md`'s replay discipline; consensus groups per
  `CONSENSUS.md` §5's checkpoint-retention invariant (applied-state
  checkpoints and log prefixes retire together; no prefix drops while any
  recovery path needs it); **queue partitions** (`QUEUE.md` §2), **the collector capture task**
  (`COLLECTOR.md` §10 — a client-owned checkpoint log, boot-classified like every
  durable writer; amended 2026-08-22), and **fan-out
  durable-subscriber logs** (`FANOUT.md` §7 — durable subscriptions *are* queue
  partitions), each owning its floor as the **contiguous-acked prefix**: an ack
  advances the floor, while leased-but-unacked records are retained for
  at-least-once re-lease. These ride the watermark-reclaim model above —
  segments free only when all resident records fall below their logs' floors.
- Session migration (colocation-unit move): snapshot at a sequence + tail export of
  `(log_id ≥ floor)` records; the receiving node opens the logical log at the same
  `log_seq` — physical stream identity is never exposed above the WAL API.

## 7. Simulation

The stream writer runs on hecate-rt; in SIM the disk is a simulated device with the
fault matrix (torn write at arbitrary byte, reordered completions, EIO, ENOSPC,
power-cut). WAL invariant oracles run under the same seeds as everything else.
Replicated-log scenarios (A2's rebuild-from-quorum, W9–W11) run under the
**cluster-SIM harness** (RUNTIME §2's cluster clause; FAULTS §4) — N nodes,
one seed, same oracles.

**Branch 25 interlock, flagged**: encryption-at-rest of WAL bytes is owned
by Branch 25 (wire security / key hierarchy) — the D-3 settlement covers the
content store, not ledger records on disk. Named here so it cannot be
silently absent.

## 8. Test cases (failure each catches)

| # | Test | Catches |
|---|---|---|
| W1 | Power-cut fuzz: cut at every byte offset across a commit batch (SIM disk); recover; assert acked prefix intact, unacked cleanly truncated | torn-write mishandling; the acked-loss class |
| W2 | Torn-vs-corrupt: zeroed tail sectors ⇒ truncate + start; non-zero garbage ⇒ typed refusal naming offset | silently replaying corrupt state |
| W3 | Recycled-segment ghost: stale chain-valid-looking records rejected by chained CRC | ghost records resurrecting old state |
| W4 | Interleave replay: N logical logs on one stream; two recoveries byte-identical; per-log order preserved | cross-log ordering leak; nondeterministic recovery |
| W5 | ω-derivation: same measured inputs ⇒ same ω; model-vs-measured throughput within 20% on CI hardware | topology derivation drift; model fantasy |
| W6 | Ack-durability property: any commit acked before a SIM power-cut survives recovery, across full seed sweep | the only unforgivable WAL bug |
| W7 | Backpressure: saturated queue ⇒ typed retryable + counter increments; memory bounded under sustained overload | unbounded growth; silent drops |
| W8 | Migration: snapshot+tail export then re-open on a fresh node; log_seq continuity and content identity preserved | migration losing or reordering a log |
| W9 | Entries-then-HardState crash sweep: cut at every byte across entry/HardState batches ⇒ recovery never observes a HardState referencing missing entries (CS9 at the WAL layer) | the v3.5 watermark class at the log substrate |
| W10 | Disposition conformance: injected body corruption ⇒ rebuild-from-quorum invoked when replicated, typed refusal at N=1, torn tail truncated — each counted and categorized | universal-refusal regression; silent truncation of acked data |
| W11 | Truncation-vs-debt: prefix truncation held within the derived debt bound for a slow follower, released for a dead one (with CONSENSUS CN6) | truncating a live follower's tail; pinning segments forever |

## 9. Acceptance criteria

1. **Zero acked-commit loss** under the SIM power-cut sweep (W1/W6) — CI-gated on
   every merge, permanently.
2. Recovery never scans beyond the segment index + the clients' last
   checkpoints (§6's ownership split); recovery throughput ≥ 0.5 × measured
   sequential-read bandwidth of the device.
3. Throughput floors phrased against measured device numbers (constants-from-data),
   ratcheted from first CI baseline: sustained durable commits/s ≥ 0.8 × the ω-model
   prediction for that hardware; macOS ack p50 ≤ 1.5 × measured F_FULLFSYNC latency
   under load.
4. One durability policy in the tree: no barrier tier, no checkpoint tier, no
   config knob that weakens the ack contract.
5. Every constant (segment size, probe count, queue bounds, ω) derives from measured
   anchors with the derivation at the definition site.
6. Determinism: same (seed, trace) ⇒ byte-identical stream files in SIM.
7. The consensus-group append path is the only commit path, at every replica count.
8. The consensus-substrate obligations — raft record kinds,
   entries-then-HardState by construction, the prefix-truncation API — exist
   before any group commits through this WAL (W9 permanent).
9. Recovery dispositions are FAULTS §2-conformant; no universal-refusal path
   remains in the tree (W10).

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
