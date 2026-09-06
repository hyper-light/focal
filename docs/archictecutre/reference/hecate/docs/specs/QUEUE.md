# SPEC: QUEUE — at-least-once delivery over WAL logical logs

Status: ACCEPTED 2026-08-20 (grilling finalization). Folds the Kafka /
RabbitMQ / SmoothMQ reference study and the user-settled principles: the
**opt-in-optimization principle** (OSS/SQS-compatible safe defaults;
architecture-native optimizations opt-in behind a declared workload property);
and **environment-derived durability** — the default derives from the
failure-domain tree, never a hardcoded replica count. Sibling of CACHE and
FANOUT on the shared substrate (CACHE §0). References: Kafka (ISR + high-water
mark, KRaft/KIP-500, tiered storage/KIP-405, idempotent producer); RabbitMQ
(quorum queues + the mirrored→quorum migration lesson, `credit_flow`, DLX);
SmoothMQ (SQS API surface + the receipt-handle gotcha); WAL, CONSENSUS,
OBJECT_TIER.

## 0. Shared substrate

Per CACHE §0 (durable log = WAL logical-log + CONSENSUS; arena fan-out;
single-owner shards; HRW placement; N=1 collapse; Driver determinism;
chokepoint boot law).

## 1. Role & framing

At-least-once delivery to the **non-recoverable hand-off boundary** — a consumer
that cannot rebuild state by replaying our log (external integrations; any
consumer that does not cursor the log). SQS-compatible. It is *not* the ledger's
internal dispatch (that is CACHE pub-sub / FANOUT push-notify).

- **Default** (safe, OSS-compatible, environment-derived): at-least-once;
  environment-derived durability (§4); standard unordered at max throughput;
  environment-derived tiered storage (§7).
- **Opt-in** (declared workload property, §10): lossy fast tier; strict
  per-partition FIFO; content-identity dedup; log-backed cursor consumer.
- **Strictly-better defaults** (§11): lease-epoch, enqueue-notify wake,
  inline-vs-ContentRef, per-edge credit backpressure, DLQ as a queue-to-queue
  move.

## 2. Data model

```rust
struct QueuePartition {
    log:        WalLogicalLog,               // this partition's durable log (WAL)
    head:       u64,                         // next enqueue offset (= log_seq)
    floor:      u64,                         // contiguous-acked prefix; reclaim below it
    dispatched: DetHashMap<u64, Lease>,      // leased-but-unacked
    class:      DurabilityClass,             // = backing consensus group replica count (env-derived)
}
struct Lease { offset: u64, epoch: u64, deadline: Instant, receive_count: u32 }
```

`partition = stable_hash(key) % P`; per-partition FIFO by construction;
`offset = log_seq`. A body rides **inline up to the WIRE_FORMAT §3c budget**
(`frame_cap − AAD − record_header`), above it a `ContentRef` into the pack
store's `queue` storage class — the WAL "content never enters the log" law binds
*bulk* content; a below-budget body is governed exactly as index and
consumer-state records already are.

## 3. Hot paths

```rust
fn enqueue(&mut self, body: Body) -> Offset {                 // WAL append (durable per class)
    let rec = self.frame(body); let off = self.log.append(rec); self.head = off + 1; off
}
fn lease(&mut self, n: usize, vis: Duration) -> Vec<(Offset, ArenaRef)> {
    let batch = self.log.read_from(self.floor, n, |o| !self.dispatched.contains(o));
    for (o, _) in &batch {
        self.dispatched.insert(*o, Lease{ offset:*o, epoch: self.epoch, deadline: Driver::now()+vis,
                                          receive_count: self.recv_count(*o)+1 });
    } batch
}
fn ack(&mut self, o: Offset, epoch: u64) -> Result<(), StaleAck> {
    let l = self.dispatched.get(o).ok_or(StaleAck)?;
    if l.epoch != epoch { return Err(StaleAck); }             // lease-epoch: reject wrong generation
    self.dispatched.remove(o);
    while self.dispatched_absent(self.floor) && self.floor < self.head { self.floor += 1; }
    self.log.advance_floor(self.floor); Ok(())
}
/// Amended 2026-08-22 (COLLECTOR acceptance): seal a partition at a lifecycle
/// boundary (e.g. a session's archive-finalize — COLLECTOR §4). Legal only with
/// the floor at head (every record acked: the draining consumer has taken it
/// all); a closed partition refuses enqueue with a typed error, and its storage
/// reclaims via the existing floor watermark — no new reclaim machinery.
fn close_partition(&mut self) -> Result<(), NotDrained> {
    if self.floor < self.head { return Err(NotDrained); }
    self.closed = true; Ok(())
}
fn sweep(&mut self) {                                          // tracked, derived cadence
    for l in self.dispatched.expired(Driver::now()) {
        if l.receive_count >= self.max_receive { self.to_dlq(l.offset); } // queue-to-queue move
        else { self.dispatched.remove(l.offset); }            // re-lease eligible (at-least-once)
    }
}
```

Crash recovery re-leases leased-but-unacked records, so **duplicates are
expected** (FAULTS: not-a-fault). A consumer feeding the ledger sizes its dedup
window ≥ the max redelivery span (`visibility × max_receive_count`), so a late
redelivery cannot slip past the ledger's three-layer dedup.

## 4. Durability — environment-derived

The default is **the strongest durability the environment structurally affords**,
derived from the failure-domain tree (the N=1 collapse applied to durability; a
derivation, not a mode). One commit path.

- **Laptop** (depth-one tree, one node): replica = 1 — a single-node WAL
  always-full fsync. Power-cut/crash safe (the ack waits for the flush);
  node-death is not survivable because no second node exists — you cannot beat
  the environment.
- **Cluster** (N fault domains): WAL fsync + replication derived across those
  domains (Kafka ISR + high-water-mark and RabbitMQ quorum queues are the
  templates), node-death safe.

The WAL ack contract is never relaxed by the default (WAL AC-1/AC-4). "Lossy" is
the opt-in tier (§10), and its loss is node-death hot-tier loss **priced as
Degraded, never silent** — not power-cut acked-loss.

## 5. Replication & placement

Each partition's log is replicated by its **backing consensus group**; the
replica count *is* the durability class (§4). The partition's single-owner
sequencer is a **lease+fence standing writer** (CONSENSUS §6; epoch = the
smallest failure domain containing every legal writer). On owner death the lease
expires, consensus elects a new owner, and the epoch fence rejects the zombie.
**KRaft validates keeping consensus in one reusable layer** (our CONSENSUS): a
partition needs only lease+fence + `seal`, not its own consensus protocol. The
mirrored→quorum migration lesson is why only the quorum (Raft) form is built,
never the deprecated mirrored form.

## 6. Ordering

Default is a **standard unordered** queue at maximum throughput (SQS standard).
**Strict per-partition FIFO** is opt-in: `MessageGroupId → partition key` routes
a group to one partition whose FIFO is total (SQS's own standard-vs-FIFO split).

## 7. Tiered storage — environment-derived

The hot WAL is always present. The **cold OBJECT_TIER tier** (content-addressed
+ copyset + EC) engages for capacity/retention, robustly provisioned at scale
and hot-only locally — the same environment-derived shape as the CACHE flash
tier (Kafka KIP-405 tiered storage is the template). Partition-log-referenced
bodies are OBJECT_TIER §7 GC liveness roots until the ack floor advances past
them.

## 8. Boot & chokepoint

Every partition boot-classifies **lease+fence** in the CONSENSUS §6 roster; the
`queue` storage class boot-validates in OBJECT_TIER (OT11); the `queue.*` IAM
actions (`create`/`enqueue`/`lease`/`ack`/`nack`/`purge`/`dead_letter_read`/
`snapshot_read`) compile to PEP. Any unclassified piece fails startup.

## 9. Laptop / N=1

`P = replicas = 1`; enqueue/lease/ack run in-process against one local log; the
DLQ is a second local queue; hot-tier only. Same verbs, same code path.

## 10. Opt-in optimizations (declared-property table)

| Declared property | Optimization unlocked |
|---|---|
| (none — default) | at-least-once, env-derived durability, unordered, env-derived tiering |
| reconstructible / loss-tolerant | **lossy fast tier** — relax below the env durability default (self-ack before fsync/replication) |
| ordered group (`MessageGroupId`) | **strict per-partition FIFO** |
| content-identity available (`MessageDeduplicationId`) | **effectively-once** consumer dedup |
| log-cursor consumer | **push-notify + pull-recover** — skip durable per-message delivery |

## 11. Strictly-better defaults (not capabilities)

- **Lease-epoch**: the lease token binds `(offset, epoch, deadline)`; a stale ack
  from an expired-then-redelivered lease is **rejected**, not applied to the
  current generation (SmoothMQ's bare-message-id receipt handle acks the wrong
  generation).
- **Enqueue-notify wake** (a condition-variable wake on the target partition),
  not SmoothMQ's 1-second busy re-select long-poll.
- **Inline-vs-ContentRef** body placement, derived from size.
- **Per-edge credit backpressure** (RabbitMQ `credit_flow` shape) **isolated per
  edge** — it never blocks an unrelated producer (RabbitMQ's gotcha: its
  backpressure propagates to block the publisher).
- **DLQ as a queue-to-queue move** at `max_receive_count` (SmoothMQ wedges failed
  messages in-table forever).

## 12. Rejected dead-ends (SmoothMQ)

SQLite/GORM as the queue engine (we have no SQL/external DB); table-scan
`deliver_at`-index polling as the discovery mechanism (→ log-floor + lease-set +
derived-cadence sweep, O(in-flight) not O(table)); wall-clock visibility (→
seeded/logical clock, determinism); inline-everything bodies; the global
`sync.Mutex` serializing all queues (→ per-partition single-owner sequencers,
P× parallelism, same at-most-once-dispatch safety); the untracked ticker
goroutine; the single-file no-replication scale ceiling.

## 13. Failure handling (FAULTS §5)

Duplication = **expected**, not a fault (at-least-once). N=1 loss = **Degraded**-
priced, never silent. Corruption = rebuild-from-quorum where replicated, refusal
at N=1. Every drop counted.

## 14. Acceptance criteria

1. At-least-once holds: no record is lost before ack, even across a re-lease.
2. Durability is **environment-parameterized**: acked writes survive power-cut
   everywhere (WAL fsync) and node death wherever the failure-domain tree affords
   replicas; the SIM sweeps 1-node → N-node.
3. A **stale ack is rejected by epoch**, never applied to the current generation.
4. DLQ fires at exactly `max_receive_count` as a queue-to-queue move.
5. Standard unordered is the default (max throughput); strict FIFO is opt-in via
   `MessageGroupId` and holds under concurrent lease/ack.
6. Per-edge credit backpressure never blocks an unrelated producer.
7. The cold tier is environment-derived (hot-only laptop, robust cold at scale);
   partition-referenced bodies are pinned until the floor advances.
8. N=1 loss surfaces as a Degraded price, never a silent drop; the floor is
   always the contiguous-acked prefix.
9. The `queue` storage class and the partition lease+fence role both
   boot-validate or startup fails.
10. Every derived constant carries its derivation; SIM is bit-reproducible.

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
