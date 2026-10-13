# 31 — The message ledger: in memory, sharded, durable in the background

Status: design, 2026-10-13, its decisions made (§8). It corrects the hot path that
04 §4, 25 and 30 built, and keeps 00's premise ("RAM is primary and disk provides durability,
analogous to Kafka's durable log"), which the implementation drifted from. Sources are listed at
the end. Measurements are cited by run.

## 1. What focal is for

The owner's statement (2026-10-13), which every section below is held to:

- **Focal moves messages.** Claims, testaments, artifacts, validations and receipts are
  *messages*. The priority is to get each one to the party it is for (the respondent, the
  claimant, the validator, the target) as quickly and as correctly as possible. Durability is
  taken with absolute care: first in memory, replicated, then retired to disk.
- **It is not a database.** The hot state is what is in flight: messages not yet delivered and
  claims still open. Finished claims retire. What is on disk must still be found quickly, but
  memory never holds everything ever written.
- **The rules:**
  - reduce work rigorously, or avoid it entirely;
  - put critical work such as disk flushes in the background;
  - no process blocks another;
  - in memory and sharded, with replication as non-blocking as it can be;
  - contiguous allocation;
  - the fewest hops to find a thing; CPU caches and SIMD key lookups used deliberately;
  - no allocation, reallocation or page fault on the hot path;
  - every step profiled, benchmarked and tested at scale;
  - recency matters: what was just written is what will be read next.

## 2. What the current engine does instead

All measured on 2026-10-12/13.

| Cost | Today | Where |
|---|---|---|
| p99 at 1,000/s, three replicas | focal 74.2 ms; Kafka 12.0, Redis 9.9, NATS 77.5 | compare/land13, run 38201803578 |
| CPU a write, busiest node | focal 949 µs; Kafka 264, NATS 262, Redis 80 | same run |
| Refused at 4,000/s offered | focal 92%; Kafka and Redis none | same run |
| Leader owner thread a write | ~360 µs a write offered (preparation ~216) | profile run 38381477219 |
| Memory a claim | 7.2 KB: 18–20 rows, each with a 112-byte key, in a page-structured sorted map | doc 30 §9.1 |
| Transient reservation a creation | 2.3 MB (construction bounds 1.65 MB, plan 0.6 MB) for 7 KB retained | doc 30 §9.2 |
| Lookups a creation | ~56 directory descents plus page searches | [[focal-leader-cpu-profile]] |
| Hashing | 20% of the owner thread at 4,000/s; an intent's digest computed ~5 times a write | run 38381477219 |
| Retirement | one family per replica per 5 s, after a day's grace; the allowance fills at ~50k claims and stays full | `archive_agent.rs` |
| Checkpoints | rewrite the whole root, writes ∝ state² | [[focal-checkpoint-write-amplification]] |
| Followers | re-validate each record (replay ~57 µs a claim) | doc 30 §9.1 |
| Cores | every session's owner on one thread a node | `fleet_group.rs` |

The pattern: per-write work grows with what is stored, not with the message. That is a
database's shape.

## 3. What systems that move messages fast do

Read in their code and docs where noted.

- **Kafka.**
  - A topic is partitioned; each partition is an append-only log whose bytes the broker treats
    as opaque batches.
  - Followers fetch from the leader, pipelined. `acks=all` acknowledges once the in-sync
    replicas have the batch.
  - The docs recommend leaving application fsync off: durability comes from replication, and
    the OS flushes in the background.
  - Retention drops whole segments, and tiered storage moves old segments to object storage
    (KIP-405). Consumers are served from the page cache by zero-copy sends.
- **NATS JetStream** (nats-server, read locally).
  - Each stream is a Raft group whose leader batches proposals, up to 256 KiB or 512 entries
    (`raft.go`).
  - The file store syncs in the background every `sync_interval`, two minutes by default
    (`filestore.go: defaultSyncInterval = 2 * time.Minute`). `sync: always` is opt-in.
  - Under work-queue and interest retention, a message is removed once no consumer still needs
    it (`stream.go`, `ackMsg`).
- **Redis.**
  - All data is in memory. The AOF is fsynced once a second by a background thread
    (`appendfsync everysec`, the default).
  - Snapshots are taken by `fork` and copy-on-write. Replication is asynchronous, and the
    cluster shards keys over 16,384 slots.
- **Shard per core** (Seastar, so ScyllaDB and Redpanda). Each core owns its data and talks to
  the others only by messages; no locks are shared [Enberg et al. 2019].
- **mantle** (ours; `engine-structure.md` on its branches, 2026-10-06):
  - per-range engines, shared-nothing, on hyper-rt shards, one per core;
  - requests batched per shard, refused `Full` at the channel's bound;
  - no WAL of its own: the Raft log is the log, and checkpoints record the applied index;
  - SplinterDB's size-tiered Bε-tree with maplets for data on disk;
  - background work scheduled against foreground work (SILK).
- **Durability off the critical path in in-memory systems:**
  - SiloR logs and checkpoints on separate cores without slowing transactions [Zheng et al.
    2014];
  - Hekaton keeps a redo log and checkpoint files and nothing on disk is in the transaction's
    path [Diaconu et al. 2013];
  - VoltDB/H-Store use command logging plus snapshots [Malviya et al. 2014];
  - FASTER keeps a hybrid log, hot in memory and cold on disk [Chandramouli et al. 2018];
  - RAMCloud's log-structured memory keeps memory 80–90% used by cleaning its in-memory log
    [Rumble et al. 2014].
- **Contiguous, cache-aware structures:**
  - SwissTable (Rust's `hashbrown`) probes 16 control bytes at once with SIMD and stores
    entries in one flat array;
  - the LMAX Disruptor passes messages through a preallocated ring with one writer [Thompson et
    al. 2011];
  - p²KVS gives each pinned worker its own partition and batches requests [Lu et al. 2022].

None of them rebuilds sorted pages to store a message, or keeps every message in memory
forever. Focal's messages carry decisions and evidence, so unlike theirs each replica validates
them (§8, decision 3); that work is made cheap and parallel, never skipped.

## 4. The design

### 4.1 Partitions and shards

- **A ledger is cut into partitions, by claim id.** A claim's messages (its validations,
  evaluations, work, testaments, receipts) live in its partition, so the hot path never leaves
  it. Each partition is one ordered log, one Raft group.
- **A node runs one owner shard a core** (hyper-rt shards). A shard owns whole partitions, both
  their state and their log, and is the only thread that touches them. Requests reach a shard
  through a bounded channel, are drained in batches, and are refused `Full` past the bound
  (mantle's rule).
- **Shared references are read-only on every shard.** Validation definitions are immutable and
  content-addressed, held in a registry partition and cached on every shard. Artifacts are
  content-store blobs referenced by hash. Enrollment and identities belong to the control plane.
- **A participant's messages are found in its inbox.** Delivery keeps, for each participant,
  the list of messages for it, in the partition its id maps to, so a query by participant reads
  one partition (§8, decision 5).

### 4.2 A message's path

1. **Admission at the source.** Credit at the client (Breakwater, credit-17), and the owner's
   shortfall refusal at its memory bound (doc 30 §9.2).
2. **Check once, on the leader.**
   - The frame is read in place: no decode into owned structures.
   - One hash probe finds the claim's slot (§4.3), and its state answers the transition's
     preconditions.
   - Identities are computed once and carried in the event header. The intent's digest is never
     recomputed downstream.
3. **Append.**
   - The event is the client's frame plus a fixed header (sequence, kind, claim slot,
     identities), appended to the partition's in-memory log segment.
   - The slot is updated in place.
   - No rows, no pages, no copies of the payload: the slot points at the event in the log.
4. **Replicate, pipelined.**
   - The leader ships byte ranges of segments, and followers append the same bytes.
   - One stream a peer pair carries every partition's appends, many in flight under a window
     (multi-Raft batching).
   - Each follower validates the event itself and compares its decision with the leader's
     before it acknowledges (§8, decision 3). A mismatch is a detected fault, and the node that
     disagrees is fenced.
   - Validation of independent messages runs in parallel across the shard's cores, pipelined
     with the leader's own work. Apply order stays sequential.
   - Checksums and the log's identity guard the bytes in transit.
5. **Commit and deliver.**
   - At the commit point of the current durability mode (§4.5), with a quorum agreeing on the
     decision, the event's position goes to each target's inbox.
   - A connected target is sent the event's bytes straight from the segment. A disconnected one
     reads them later, from memory while they are hot and from the cold tier after.
   - A receipt is an event too.
6. **Background, off every message's path:**
   - the log writer flushes segments (§4.5);
   - snapshots of the live set (§4.6);
   - retirement into the cold tier (§4.7);
   - segment cleaning (§4.3).

### 4.3 The hot state: contiguous and one hop deep

On each partition's shard:

- **The log.**
  - Segments are fixed-size contiguous buffers (sized by measurement, e.g. 4–64 MiB) drawn from
    a per-shard pool that is allocated and pre-faulted at start, on huge pages where the
    platform gives them.
  - Appending is a copy into the current segment.
  - A segment returns to the pool once every event in it has retired and is flushed. A segment
    held by a few long-lived claims is cleaned, its live events copied forward, as RAMCloud
    cleans its log.
- **The claim table.**
  - A SwissTable index maps claim id (16 bytes) to a slot (u32). Probing is SIMD over 16
    control bytes, so a lookup is one or two cache lines.
  - Slots sit in a chunked array of fixed-size, cache-line-aligned records. Chunks never move,
    so a slot's address is stable and growing the array is a new chunk, never a reallocation.
  - Hot fields come first: state, counters, the claim's validation slots inline, deadlines, the
    log positions of its creating and latest events. Cold fields live in a parallel array.
  - Free slots are reused LIFO, so they are warm in cache.
- **What delivery needs, and nothing more:**
  - each target's delivery queue is a ring of log positions;
  - deadlines are a hierarchical timing wheel over slot indices, so a timer allocates nothing;
  - claims waiting for validators are intrusive lists through slot indices.
- **No index row is stored anywhere.** Today's 18–20 rows a claim become one slot plus the
  claim's events in the log.
- **Hashing.** Ids are 128-bit random values: the table hashes them with a keyed mixer (a
  per-shard secret, so client-chosen ids cannot be made to collide), not SipHash.
- **Memory.** Charged by chunk and segment, which is what is actually allocated, not by
  worst-case quotes. The live set is bounded by the shard's allowance, and admission refuses
  creations, never completions, when it is full.

Target, to be measured: under 1 KB a live claim beside its events (today 7.2 KB). The log
positions replace the payload copies.

### 4.4 No process blocks another

| Thread | Does | Waits only on |
|---|---|---|
| Transport shards | sockets, crypto, framing | their bounded channels |
| Owner shards | check, append, apply, deliver | their request channel |
| Log writer, one a device | flush segments, group commit with several batches in flight [Chen et al. 2021] | the device |
| Cold maintenance | flush and compaction, in each shard's idle time (SILK) | the shard's idle time |
| Snapshot workers | write captured chunks | the device |

- **A slow disk** delays only disk-class acknowledgements and the flush lag. Past the lag's
  bound, the memory class is refused rather than letting memory grow.
- **A slow follower** does not hold commit, which needs a quorum. Past the in-memory log, it
  catches up from disk or a snapshot.
- **A slow subscriber** fills only its own queue, then pulls from the log or the cold tier. The
  producer never waits.
- **Between threads,** bounded SPSC/MPSC rings carry values. No lock is shared.

### 4.5 Durability: situation-aware, calibrated to the device

One rule for every message kind (§8, decision 2):

- **Healthy: a memory quorum.**
  - While every replica is up, a write commits once a quorum, in distinct failure domains,
    holds it in memory.
  - The log writer flushes continuously in the background, within a bound measured per device
    (e.g. 1 ms or 256 KiB). The bound is held: when flushing falls behind it, writes are
    refused.
- **Suspected failure: a disk quorum, at once.**
  - The moment a failure is suspected, every replica flushes what it holds, and writes commit
    on fsync until the group is healthy again. The triggers are a SWIM suspicion or a missed
    heartbeat, a peer's shutdown or power notice, and the node's own pressure signals.
  - This is SAUCR [Alagappan et al. 2018]: correlated failures rarely strike at one instant,
    and the time between the first and the rest is enough to flush. Measured in ZooKeeper,
    within 0–9% of a memory-only system, with far greater durability and availability.
  - Memory-only and asynchronous systems lose acknowledged data in correlated crashes
    [Alagappan et al. 2016]; this is the answer to that.
- **Cheap fsync: always a disk quorum.** Where the device's fsync is measured cheap (drives
  with power-loss protection), every write commits on fsync: there is nothing to trade (the
  device calibration of the terminating condition 10).
- **One node: disk.** A laptop or a single replica commits on fsync, with group commit and
  several batches in flight: a memory quorum of one is no durability.
- **What a reader is told.** A target is delivered a message only at the commit point of the
  current mode, so no reader observes data the group may still lose. That is CAD's principle,
  durability before observation [Ganesan et al. 2020].

### 4.6 Snapshots of the live set

- **A snapshot copies the slot chunks and the live events' segments.** The index is rebuilt
  from the slots on recovery: rebuilding a hash table of a million slots costs milliseconds and
  saves writing it.
- **It is fuzzy.** It runs while writes go on and records the log position it began at.
  Recovery loads it and replays from that position, which works because apply is
  sequence-checked and idempotent (SiloR, Hekaton).
- **Its cost is the live set, never the history.** It can be incremental by dirty chunk.

### 4.7 Retirement and the cold tier

- **A claim retires when its terminal event commits.** Retirement is an event, batched so one
  event retires many claims, and every replica applies it alike. A message not yet delivered
  stays in its target's inbox, and is read from memory or the cold tier.
- **Retirement is logical at once and physical later** (§8, decision 4).
  - At once: the claim becomes immutable, leaves the live table, and stops counting against
    admission.
  - Then, in background batches, its events are compacted and compressed into the partition's
    cold engine, so it is durable and queryable on disk soon. A size-tiered tree wants large
    sorted batches, and batching gives them.
  - Its bytes leave memory only when recency and use say so, under pressure (§4.8). Completion
    is when re-reads peak: the claimant reads the result, and the next agent reads the testament
    and its artifacts.
- **The cold engine is a size-tiered Bε-tree with maplets** (SplinterDB, as mantle's engine).
  It is keyed by claim id, with secondary trees on (participant, time, claim) and
  (testament/artifact id → claim). A point read costs about one device read, and a short scan a
  few.
- **The partition's Raft log is its WAL** (mantle's arrangement). The engine checkpoints its
  applied index, and a segment is dropped once both the cold engine and the live-set snapshot
  are past it. The cold engine flushes and compacts in the background, scheduled against
  foreground work.
- **Retention.** Focal is a ledger, so the cold tier keeps everything by default, for
  provenance. Old trees tier to object storage (mantle), as Kafka's tiered storage moves
  segments, and data is deleted only by an explicit tenant retention policy.
- **Queries look in memory first, then the cold tier.** An as-of-sequence read is served from
  the log or the cold tier, never by keeping old versions in the hot table.

This replaces the archive agent's sweep. Memory holds the live set, and a workload whose
claims complete reaches a steady state and holds it.

### 4.8 What will be read next

Focal can count on what a general store cannot. What was submitted recently will be needed
again soon:
- an agent that posts a claim refers to it again soon;
- an agent that posts a testament, and the claimant it answers, read the testament and its
  artifacts soon.

Recency is the first dimension the tiers are ordered by, though not the only one.

- **The log orders by recency already.** Its newest segments hold what was just written, and
  they are in memory. A read of a recent item costs one probe and one copy from a segment
  that is already in cache or in RAM.
- **Retirement does not evict.**
  - A retired claim's events stay in the hot log until memory is wanted. Its slot is freed and
    its id moves to a recently-retired index of log positions.
  - When memory is wanted, the oldest segments go first. Their events are already in the cold
    tier, so letting them go is free.
- **Usage is tracked beside recency**, so a burst of one-off reads cannot push out what is read
  again and again. Two measured policies are candidates for the cold tier's cache:
  - S3-FIFO: a small FIFO for new items, and a main queue that an item reaches only by being
    read again [Yang et al. 2023];
  - W-TinyLFU: a recency window, and admission by a frequency sketch [Einziger et al. 2017].
  The policy and its sizes are chosen by measurement on focal's traces.
- **Related items are brought in together.**
  - A testament's artifacts and the claim it answers share the claim's partition and its
    affinity.
  - Reading one schedules its siblings into the cache: a testament's artifacts, a claim's
    validation definitions.
  - Cold rows of one claim lie contiguous under its key, so one device read returns the
    claim's set.
- **Access counters are kept per participant and per kind of item.** They feed the cache
  policy and the measurements of §6: hit ratios by tier, reuse distance, and an agent's
  re-read interval. The counters are fixed-size sketches, never a log of reads.

### 4.9 Work removed, by item

| Today | Becomes |
|---|---|
| 18–20 rows and their 112-byte keys a claim | one slot, and positions in the log |
| Page copy-on-write, directory paths, cut rule | none for live claims |
| Worst-case per-change reservations (2.3 MB) | chunk and segment charges, as allocated |
| ~56 lookups a creation | one hash probe, plus one per referenced definition |
| A digest recomputed ~5 times | computed once, carried in the event |
| Followers re-validate serially (57 µs) | followers validate in parallel, pipelined, digests once |
| Re-encoding records | the frame is the event, sent and stored as received |
| Whole-root checkpoints | fuzzy snapshots of the live set |
| Archive sweeps every 5 s after a day | retirement events into the cold tier |
| One owner thread a node | one owner shard a core |

## 5. What stays

- **Infrastructure:**
  - the Raft core (focal-raft / hyper-raft);
  - the transport (hyper-quic, hyper-transport) and its admission credit;
  - aws-lc-rs post-quantum encryption, in transit and at rest;
  - the lifecycle model's rules (doc 17), the evidence verification, and the CLI, MCP and
    skills.
- **Interim work:** the in-place store (doc 30 §9) still serves the families not yet moved.
  engine-sparse lands because it cuts today's costs, and §7's stages supersede it for live
  claims.

## 6. Proof at every step

- **Microbenchmarks** (criterion) of each structure: probes, appends, retirements.
- **Counters on Linux CI** (`perf stat`): instructions, cache and TLB misses, page faults and
  allocations a message.
- **Scaled tests in CI:** at least a million live claims, at least 100k/s offered, at least
  1,000 agents, and steady state over hours with claims completing.
- **Profiles** of the leader on the comparison's runners.
- **The four-system comparison** after each stage, on one runner.

Nothing lands on argument alone.

## 7. Order of work

Each stage lands on its own, measured:

1. **M1, owner shards.**
   - K owner threads a node, sessions spread over them. The data structures do not change.
   - Measure: commits a second a node against cores.
   - **As built.**
     - **Shards.** A fleet runs `owner_shards` threads, by default one a core it is given (on
       Linux, the cgroup's quota), at most 64, and never more than the sessions it may hold. A
       session lives on the shard its ledger names (`shard_of`), and its host sends to that
       shard's queue alone, so routing is fixed at install, never decided per message.
     - **Management** runs on its own thread, keeping the contiguous management sequence and
       the fleet's entries. An install builds the session and hands it to its shard as an
       adoption, answered only once the shard holds it, so every request through the host
       reaches the shard after it.
     - **Fleet-wide work.** A tenant's admission and usage are asked of every shard, in order
       behind the shard's earlier input; shutdown, or the last manager going away, stops every
       shard.
     - **Status.** A shard publishes its running count, and the fleet's status sums them as it
       is read. Nothing waits on another shard's map.
     - **Proof.** `a_held_shard_holds_up_only_its_own_sessions`: one shard's thread held, the
       other shard's session commits, and the held one commits once let go.
       `sessions_spread_over_owner_shards_each_commit_and_every_shard_ends_at_shutdown`: eight
       sessions over four shards.
2. **M2, retirement as events and a first cold store.**
   - The cold store starts as per-partition segment files with a sorted index, and moves to the
     STBε engine in M7.
   - Measure: hours of steady state with claims completing, memory flat.
   - **What retirement did, read from the code** (to be confirmed under a lifecycle load):
     - **A retirement was a session decision of its own.** A record named one family, its
       bundle and the prefix it was derived at. It applied only at exactly that prefix, so any
       write committed while the bundle went under custody left the record inert on every
       replica. Under steady writes, nothing retired.
     - **It stopped the pipeline.** The authority proposed only with nothing pending,
       admission refused writes (`Retiring`) until the record committed, and applying the
       record discarded the authority's candidates and rebuilt its owner, a walk of every row.
     - **It was slow by construction.** One family per replica per 5-second tick.
     - **Deriving a family walked every event the session held**, since events leave the core
       only with their family: about 67 ns an event, 4.4 ms with 32,768 open claims.
   - **First step, as built (A1).**
     - **The event index.** A family's events are read from an index of each object's events,
       a funded B-tree set that is never encoded. It is built from the rows at restore, kept as
       each publication commits, and let go whole wherever it cannot be kept exactly, after
       which a walk finds the same family.
       - Measured: 3.6 µs a derivation with 32,768 open claims, against 4.4 ms by the walk.
     - **Retirement in the owner's pipeline** (`retirement_batch.rs`).
       - The owner prepares a batch of up to 64 families as one candidate at its chain's
         sequence, admitted as a write is, among the writes around it.
       - Each family is derived from the committed rows and deferred where a pending
         candidate changes a row its derivation read. An eligible family's rows cannot
         change, so the chain derives the same family.
       - Its record (`FCRETIRE`) carries each family's root and bundle, never its rows: 56
         bytes a family.
       - Every replica derives the families at the record's place and checks them against the
         batch's digest, the outcome's intent. A replica that cannot match them fails closed
         as diverged.
       - No exact prefix is needed, no admission held, no pipeline drained, no owner rebuilt.
     - **The archive agent** seals each eligible family's bundle under custody as before. It
       offers each with the digest of the family the bundle holds, so a bundle never stands
       for other rows, and retires them a batch at a time. A batch that filled sends the agent
       on at once rather than at the next tick.
     - **Behind the upgrade fence.** A binary below `RETIRE_LEVEL` (6) cannot read `FCRETIRE`
       records.
       - A node takes its sessions' rule from the fence it last recorded, as it does the
         seal rule.
       - Below the fence the engine refuses a batch by name (`RetireExact`), and the agent
         retires a family at a time as before (`FOCALRT1`).
       - Every binary at the level applies both kinds of record.
     - **Measured under a lifecycle load.** Three local nodes ran 100 lifecycles a second for
       40 s: each lifecycle creates a claim, cancels it, releases it and reads it, and the
       nodes retire at once (`FOCAL_RETIRE_AFTER_MS=0`).
       - Before (1701678): 6,731 committed, p99 842 ms, no family retired.
       - After: 13,365 committed, p99 337 ms. Families retire among the writes: 26 on the
         leader in the window.
       - Two defects stood in the way first, both now fixed:
         - Every native read reserved 32 times the client's frame for as long as it waited.
         - Custody charged every export the manifest's bound (4 MiB), kept for a minute.
           That filled the content budget, and the archive's replication was refused.
       - **What the run proves: per-family custody cannot keep pace.** Retirement ran at
         about 0.6 families a second against 100 arriving, so resident outcomes still grew
         linearly (12,146 at the end).
         - Each family is a custody object: it is sealed on the leader, then pushed to and
           sealed on each peer. That is about twelve fsyncs across three nodes.
         - Timed, it is 230 ms a family on an idle machine and about 1 s under the write
           load.
         - Concurrency would not change the order of magnitude: the flushes are the cost.
     - **Next (A2).** The local cold store replaces custody bundles for retirement, and stubs
       leave memory (the design follows).
       - A replica applies a retirement record by writing the families to its own cold
         stream, deterministically and in the background.
       - Its durability joins the checkpoint the log already waits for.
       - No seal, no transfer and no fsync is made per family.
3. **M3, the claim table.**
   - SwissTable index, slot chunks and log-resident payloads for the claim families, replacing
     the row store for live claims.
   - Measure: µs, cache misses, allocations and page faults a message; memory a claim.
4. **M4, durability classes.**
   - Memory-quorum acknowledgement with a bounded background flush; the disk class kept.
   - Measure: p99 at 1k, 4k and 12k/s against Kafka, NATS and Redis.
5. **M5, validation once a replica, in parallel.** Events carry identities computed once,
   followers' validation runs in parallel and pipelined, and a decision commits with a quorum
   agreeing.
6. **M6, partitions within a ledger.**
   - Several Raft groups a ledger, multiplexed, with routing by claim id and fan-out queries.
   - Measure: commits a second a ledger against partitions.
7. **M7, the cold tier on the STBε engine.**
   - Shared with mantle through hyper-raft's crates if its engine moves there.
   - Measure: cold point and scan latency at p99.

## 8. Decisions

1. **Order per partition.**
   - Request de-duplication and principal windows are kept per (principal, partition): requests
     are routed by claim id, and creations carry client-minted ids.
   - Epochs and seals are per partition. Enrollment stays in the control plane.
   - A message that refers to another claim reads that claim's committed state by sequence
     token. No transaction spans partitions on the hot path.
2. **Durability is situation-aware and calibrated** (§4.5). Neither fixed class is best:
   - a memory quorum alone loses acknowledged data in correlated crashes [Alagappan et al.
     2016];
   - a disk quorum alone puts fsync on every message's path;
   - SAUCR keeps the first's speed while healthy and the second's safety when it matters,
     within 0–9% of memory-only [Alagappan et al. 2018].

   One rule for every message kind.
3. **Followers validate, independently.** Validation is focal's product: an acceptance, and
   the evidence checks behind a testament (signatures, artifact digests, schemas), which agents
   act on.
   - **The risk.** At fleet scale, CPUs that silently compute wrong results are measured, not
     hypothetical: Meta found hundreds across its fleet [Dixit et al. 2021], and Google's
     "mercurial cores" [Hochschild et al. 2021]. A leader on such a core, or a buggy or
     compromised one, that accepts a forged testament would, with followers trusting it, have
     every replica agree.
   - **The damage.** Thousands of agents would then build on the decision, and nothing could
     ever detect it.
   - **The defence.** Each replica decides for itself, and a decision commits only with a
     quorum agreeing, so the first wrong decision is a detected fault and never reaches a
     target.
   - **The cost** is kept off the path: validation is parallel across cores, pipelined with the
     leader's, and each digest is computed once.
4. **Retirement is logical at once and physical lazily** (§4.7). Immediate eviction would
   turn the hottest reads into disk reads, and every completion into a burst of cold-tier writes
   on the hot path. The cold tier keeps everything by default, tiered to object storage by age.
5. **Participant inboxes are the cross-claim index** (§4.1).
   - Delivery needs each participant's list of messages anyway.
   - On a laptop it is local. At Meta scale, a fan-out over thousands of partitions makes every
     query as slow as its slowest partition and costs O(partitions) work [Dean and Barroso
     2013]; the inbox is one partition.
   - It costs one append per target per message, off the commit path, with sequence tokens for
     read-your-writes. Rare global scans, by status, stay bounded fan-outs.

## Sources

- Kafka documentation: design (replication, persistence and the OS page cache, flush policy),
  and KIP-405 (tiered storage). https://kafka.apache.org/documentation/
- nats-server v2.12 source: `server/raft.go`, `server/filestore.go`, `server/stream.go` (read
  locally at `../nats-server`).
- Redis documentation: persistence (RDB, AOF, `appendfsync everysec`), replication, cluster
  specification. https://redis.io/docs/
- mantle: `docs/design/engine-structure.md` and research notes 33–34 (on mantle's branches,
  2026-10-06/08).
- [Conway et al. 2020] *SplinterDB: Closing the Bandwidth Gap for NVMe Key-Value Stores*,
  USENIX ATC 2020. https://www.usenix.org/system/files/atc20-conway.pdf
- [Conway et al. 2023] *SplinterDB and Maplets*, SIGMOD 2023.
  https://dl.acm.org/doi/10.1145/3588726
- [Balmau et al. 2019] *SILK*, USENIX ATC 2019.
  https://www.usenix.org/system/files/atc19-balmau.pdf
- [Chen et al. 2021] *SpanDB*, FAST 2021.
  https://www.usenix.org/system/files/fast21-chen-hao.pdf
- [Lu et al. 2022] *p²KVS*, EuroSys 2022.
- [Zheng et al. 2014] *Fast Databases with Fast Durability and Recovery Through Multicore
  Parallelism* (SiloR), OSDI 2014.
- [Diaconu et al. 2013] *Hekaton: SQL Server's Memory-Optimized OLTP Engine*, SIGMOD 2013.
- [Malviya et al. 2014] *Rethinking Main Memory OLTP Recovery*, ICDE 2014.
- [Chandramouli et al. 2018] *FASTER*, SIGMOD 2018.
- [Rumble et al. 2014] *Log-structured Memory for DRAM-based Storage*, FAST 2014.
- [Enberg et al. 2019] *The Impact of Thread-Per-Core Architecture on Application Tail
  Latency*, ANCS 2019.
- [Thompson et al. 2011] *Disruptor: High performance alternative to bounded queues for
  exchanging data between concurrent threads*, LMAX technical paper.
- [Cho et al. 2020] *Overload Control for µs-scale RPCs with Breakwater*, OSDI 2020.
- Abseil SwissTable design notes; Rust `hashbrown`. https://abseil.io/about/design/swisstables
- [Yang et al. 2023] *FIFO Queues are All You Need for Cache Eviction* (S3-FIFO), SOSP 2023.
- [Alagappan et al. 2016] *Correlated Crash Vulnerabilities*, OSDI 2016.
- [Alagappan et al. 2018] *Fault-Tolerance, Fast and Slow: Exploiting Failure Asynchrony in
  Distributed Systems* (SAUCR), OSDI 2018.
  https://www.usenix.org/conference/osdi18/presentation/alagappan
- [Ganesan et al. 2020] *Strong and Efficient Consistency with Consistency-Aware Durability*,
  FAST 2020. https://www.usenix.org/conference/fast20/presentation/ganesan
- [Dixit et al. 2021] *Silent Data Corruptions at Scale*, arXiv:2102.11245 (Meta).
  https://arxiv.org/abs/2102.11245
- [Hochschild et al. 2021] *Cores that don't count*, HotOS 2021.
- [Dean and Barroso 2013] *The Tail at Scale*, Communications of the ACM 56(2), 2013.
- [Einziger et al. 2017] *TinyLFU: A Highly Efficient Cache Admission Policy*, ACM
  Transactions on Storage 13(4), 2017.
