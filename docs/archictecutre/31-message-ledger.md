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
   - **Second step (A2), as built: the local cold stream.** Every replica writes the families
     it retires to its own cold stream as it applies the retirement record, from its own rows,
     deterministically. Durability rides the checkpoint, never a family.
     - **The record decides what enters the stream.**
       - A version-2 retirement record (`FCRETIRE` v2) carries each family's root, the digest of
         its frame and the frame's length. Nothing is encoded to quote it: a counting pass.
       - Every replica that applies one appends that record's frames, and only those. So the
         stream starts and grows at the same log positions on every replica, whenever each node
         took the rule.
       - Every replica, the leader included, encodes the batch from its committed rows at the
         record's base (`Core::cold_batch`) and checks each frame against the record, so a
         difference is a detected divergence (`InvalidManifest`).
     - **Frames.**
       - A family's frame is its `FCNARCHV` encoding: rows in key order under a header, with a
         digest trailer.
       - A record's batch is a header followed by its frames. The header holds the count (at most
         64), then each family's root, digest and length.
     - **Stubs keep their shape.**
       - A native session's core rows cannot change format: its decoder gate allows one
         transition, and that one is spent from managed to native.
       - So a stub still names its family's "bundle" by digest and length, now the frame's. A
         cold stub claims the record's own sequence as its prefix (`through == retired_at`).
       - A second stream, the **marks**, holds 16 bytes a record: its sequence and its batch's
         offset. A retired claim is read as: its stub's sequence, a binary search of the marks,
         the batch's header, then the frame. A stub whose sequence has no mark is a version-1
         retirement, read from custody as before.
     - **Chunks at fixed offsets.**
       - The stream is cut into 1 MiB chunks at fixed offsets, so every replica's chunks hash
         alike. Every 4,096 chunk names form a manifest chunk of 128 KiB, which names 4 GiB.
       - An install takes any replica's chunks by hash, in two rounds: the manifest chunks, then
         the chunks behind them. A chunk that differs from a peer's is a detected divergence.
     - **Files: written through, made durable by the checkpoint** (`ColdStore`).
       - **The open chunk** is a file appended in place and never synced on its own.
       - **A completed chunk** is closed, then renamed under its hash as `.pending`, unsynced.
       - **A checkpoint** names the stream: its length, its manifest chunks, the names after them
         and the open chunk's bytes, inline. On the thread that already makes the root's seeds
         durable, it syncs the pending chunks it names, renames them `.seed` and syncs the
         directory once. Only then may it become the recovery base.
       - **Why this is correct.** Until that checkpoint is the base, recovery starts from an
         earlier one, whose image still holds the retired rows. Replay re-encodes the same frames
         and cuts the same chunks. At open, files no checkpoint names are removed.
       - **Room before the record applies.** What an append takes (the volume of every chunk and
         manifest chunk it cuts, and the names, charged) is promised before the record applies
         (`ColdStore::room`). A refusal leaves the record to be applied again with nothing
         changed; once it applies, its batch can fail only on IO, and a replica whose stream
         fails then stops, naming why, until a reopen restores the stream from its checkpoint and
         the log. The first build appended after publishing, so a full volume left a published
         record without its frames, and the retry found its sequence gone (`Corrupt`).
       - **Cost.** One sync pass a checkpoint, never one a family. Memory is the chunk names,
         charged as they grow; a read of a frame is charged before it reads.
       - **Prior art.** Group commit (DeWitt et al., SIGMOD 1984), with the Raft log as the WAL
         (mantle's engine; SplinterDB, ATC 2020). The snapshot is written while the state machine
         goes on (Ongaro's thesis §5.1).
     - **Its own directory, never swept.** `cold/<session>` sits beside the seed directory.
       Retired data is kept, so no protection list can lose it. The seed transfer serves from
       either directory, and an install puts cold chunks into this one. GC protects what a cold
       frame names (its artifacts' objects); a backup (schema 2) carries both streams' chunks and
       restores them in place.
     - **Behind the fence (`COLD_LEVEL` 7).** A checkpoint's cold section is flagged by its
       ancillary byte 2, which older readers refuse. Every binary at the level keeps the streams;
       the fence gates only the leader's version-2 proposals.
     - **A retirement takes every offer at once.** The owner derives each offered family once and
       packs them into candidates, each within the store's bound on a batch's changes (256 here, a
       latency bound on one candidate, kept), staged one after another, each a record of its own
       whose frames claim its own sequence. The first build staged one candidate a call: 13 of
       these lifecycle families (19 rows each), the other 51 of every 64 deferred, walked and
       derived again.
     - **The agent walks on the replica's thread.** A cold walk visits 64 status rows, offers
       what may retire and retires it in one call to the replica (`cold_walk`), where a walk was
       a call a family (66 under load, each 20–37 ms behind the writes). Each replica's walk has
       its own pace: at once while it retires and has more to visit; else once a batch's worth is
       likely ready at the rate it found families; through a pass with nothing found at a scan's
       pace (100 ms); and at the interval once a pass finds nothing. A pass that ended with a
       partial batch once waited the whole interval, five seconds, at any rate.
     - **Measured under the lifecycle load.** Three local nodes, each lifecycle creating,
       cancelling, releasing and reading a claim, retiring at once (`FOCAL_RETIRE_AFTER_MS=0`).
       - 100 lifecycles a second for 40 s: 13,494 committed, p99 218 ms, 4,457 families retired
         by the end (A1: 13,365, p99 337 ms, 26 retired). The leader's memory stays flat, about
         157 MB with 12 MB of pages (A1: 192 and 36, growing).
       - 300 a second (past this machine's capacity: the overload controller sheds a few
         hundred): 40,066 committed, p99 270 ms; 13,018 retired against about 13,660 finished,
         keeping pace through the run. The first build retired 11,629, five-second stalls and
         thousands behind under the load, at p99 981 ms.
     - **Then:**
       - The walk visits every terminal claim in claim-id order, so under a grace period it
         rescans what may not retire yet. An index of families by the time they settled, derived
         as the event index is, makes a walk cost what is due; then the owner retires as terminal
         events commit (§4.7), with no agent walk at all.
       - Closed outcomes still grow until the seal's pressure (three a lifecycle here, 1,000,000
         by default) and are sealed under custody.
       - Stubs leave memory behind an on-disk index (sorted runs, deterministic, merged at fixed
         points), until M7's STBε tree.
3. **M3, the claim table.**
   - SwissTable index, slot chunks and log-resident payloads for the claim families, replacing
     the row store for live claims.
   - Measure: µs, cache misses, allocations and page faults a message; memory a claim.
   - **Measured first (2026-10-14).** Today one authored claim with one declaration holds 18
     rows: 6,579 bytes in memory, of which 2,738 are the entries themselves (152 bytes each,
     the key alone 112 because the enum is as wide as its widest variant) and 3,841 their
     decoded payloads. Its rows' own encoding, the bytes a checkpoint or a record carries, is
     2,430 bytes, 2.7 times smaller:

     | Family | In memory (B) | Encoded (B) |
     |---|---|---|
     | 11 index rows | 1,672 | 420 |
     | Claim | 1,528 | 701 |
     | ClaimContent | 1,159 | 297 |
     | Definition | 963 | 232 |
     | 2 events | 646 | 378 |
     | CreationResult, Outcome | 608 | 402 |

     With pages and their slack a claim is about 7.2 KB, so a 2 GiB container's native
     allowance (384 MiB) holds about 50,000 open claims: where the comparison's runs fill.
   - **Staged, so every stage is a measured reduction that stands on its own.**
     1. The immutable payloads (a claim's descriptor, a declaration's) are held as their record
        encodings, as events already are, beside the few fields admission reads on its common
        paths (the content's hash, schema and profile); the body is decoded where a check needs
        it, charged while held. A page's copy-on-write then copies bytes, not a descriptor's
        nested heap. About 1.3 KB a claim.
        - **As built.** The content row is one allocation exactly its own length: the
          content's hash, the descriptor's encoding, the profile's. Hash, schema, binding,
          intent and profile are read in place; a zero-copy view of the encoding answers its
          roles, scopes, relations and requirements, which index derivation, retirement, peer
          checks, list filters and replay's reference checks read; a full decode, charged while
          it is held, is made only where a model check needs the descriptor whole. Replay keeps
          the record's checked bytes. A declaration's body stays decoded for now: the model's
          views lend it by reference deep into evaluation.
        - **Measured.** The content row 1,007 → 343 heap bytes; a claim 6,579 → 5,915 bytes.
          Three local nodes at the comparison's 1.5 GiB node budget, 1,000 creations a second
          for 60 s: 48,248 → 52,742 committed before the session filled, p99 314 → 262 ms.
     2. The claim's own row: its hot state kept, its cold parts held encoded.
     3. Keys held as their order-preserving encoding (25–57 bytes for these families), so an
        entry stops paying for the widest key. About 0.9 KB a claim.
        - **As built (`PackedKey`).** A member stores each key as its order in bytes: the
          affinity, the family big-endian, then the fields the affinity does not fix,
          fixed-width and big-endian, nested enums by their tag. Keys of up to 55 bytes are held
          inline, and longer ones (the evaluation and monitor-timer families') are boxed.
          Two keys compare as their bytes do; the order test checks this against `Key` for every
          family, and `unpack` is total. Reads, scans and plans still speak `Key`. An entry
          went from 152 to 96 bytes.
     4. Pages of encoded entries: what replay, recovery and a checkpoint move become copies of
        bytes the record already carries.
     - Then the claim table proper, where it beats the encoded store on the measures above.
   - **Index rows are keys, and are held as keys** (`focal_memory::KeySet`, as built).
     - **What the others do.** Nine of a claim's eighteen rows are secondary index rows and due
       timers (families 34–48): a key and nothing else. A row store paid a fixed key slot, a value
       and a heap count for each (96 bytes for a 34-byte key). The systems the comparison runs
       against keep keys contiguous and delta-coded, not a node a key:
       - Redis keeps a stream's entries in listpacks, contiguous and delta-coded against a master
         entry, in a radix tree of them.
       - LevelDB and RocksDB blocks prefix-compress their keys with a restart every sixteen, and
         a search halves the restarts and decodes forward [LevelDB table format].
       - WiredTiger prefix-compresses its leaf pages the same way.
       - NATS keeps per-subject state in a compact radix tree (`stree`), not a map a message.
     - **The structure.** Keys of an index bucket share their affinity and family (18 bytes),
       so focal keeps them that way:
       - Each key is front-coded against the one before it, every sixteenth stored whole.
       - Keys sit in blocks of at most 1 KiB or 64 keys. Blocks sit in chunks of 32, chunks in
         mids of 32, and mids in a root the set alone holds.
       - An edited block is cut evenly, as a B-tree splits a node. Cut greedily, a full block
         plus one key left one-key blocks, and a set filled at random kept a third of its room
         empty: 30.5 against 23.1 bytes a key.
     - **Copy on write without an occupancy term.**
       - Freezing or pinning the set copies its root, one slot a mid, and shares everything
         below it.
       - A publication edits a mid, chunk or block in place where the set alone holds it, and
         copies it where an image still reads it. This is FASTER's division between in-place and
         read-copy-update, decided by reference counts, as the row store's sparse publication
         decides it for pages.
       - The root is never shared, so no publication copies it. What a publication may spend is
         therefore bounded by its edits alone, whatever the set holds: the bound a write's
         envelope promises before the write is planned (`with_key_set_bytes`).
       - A publication spends only what its plan reserved from the write's own source (`fund`).
         The root's growth is absorbed into the root's charges.
     - **In the range group.** Each member keeps its index keys in a set beside its rows. Every
       read merges the two in key order (`get`, scans, frozen images, leases, a restore's
       validation view), and an index key reads back as the unit row.
       - A write's index changes leave its rows' vector before it is divided among members. Each
         member's part is checked against the state the write is planned on, through the
         pending chain as before. Its changes take positions after the member's row changes, so
         the pending index addresses them as it does any change.
       - A read pins each member's set with its leases. The group retains the frozen sets in a
         bounded registry (1,024 reads) until the read is released or expires or the group
         goes, as a store's lease registry retains its pinned roots; the read holds weak handles
         alone.
       - A split or merge divides or joins the sets with the stores. A restore streams phase 0's
         index rows into a set builder, each checked as every fixed row is.
     - **Measured.** 100,000 keys of one bucket shape (16-byte affinity, 2-byte family, 16-byte
       identity) take 21.5 bytes a key built and 23.1 published in random batches of nine,
       against 96 as row entries.
   - **One page class.**
     - **The defect.** Page classes kept the immutable payloads (definitions, artifacts,
       content, creation results) off the pages lifecycle writes copy. But a leaf cut at every
       change of class in the key order, and a claim's own rows sit together there: its state
       beside its content, an outcome beside its creation result. The classes alternated row by
       row, so at the node's 4 KiB leaves the store held 2.3 rows a page, a page's fixed charge
       each.
     - **The fix.** Every family now shares one class except the session's counters, which
       every mutation rewrites (the 2026-09-29 audit's R4). A leaf is bounded by its bytes, heap
       included, so a copy-on-write copies a page's worth whichever rows share it. Publication
       copies a page only where a capture or a lease still reads it; otherwise it edits in
       place.
   - **Measured** (`measure_memory_per_authored_claim_by_family`, release, 2,000 authored
     claims, at the node's range configuration of 128-entry leaves within 4 KiB, counting what
     the native budget charges):

     | Step | Bytes a claim | Pages |
     |---|---|---|
     | Index rows as row entries, eight page classes | 6,184 (fixture leaves) | — |
     | Index keys in key sets, eight classes | 4,982 | 7,731 |
     | One page class | 4,578 | 2,760 |

     The rest of a claim's 4.6 KB is its rows:
     - its state, 1.47 KB (`ClaimState` is 1,000 bytes inline, 136 of heap);
     - its definition, 0.91 KB, decoded;
     - its content, 0.44 KB, held encoded since stage 1;
     - two events, 0.53 KB, encoded;
     - its creation result and outcome, 0.5 KB;
     - two identity rows;
     - the event index, 192 bytes.
   - **A definition held compact, decoded** (as built).
     - **Measured first.** Held as its record encoding, a definition would cost every read a
       checked decode: 4.9 µs for a programmatic definition (`measure_definition_decode_cost`),
       and lifecycle steps read a claim's definition several times each. So it stays decoded,
       and is made compact instead.
     - **As built.**
       - A declaration's phases sit behind one allocation, the check then any quality phase. A
         delivery declaration has none, so it no longer carries two 112-byte phase slots inline.
       - The slot name, a descriptor's description, its quality bar and its contributors are
         exact boxed slices.
       - The row holds its one-descriptor container itself, narrow enough that no box is
         needed around it.
       - `Declaration` 464 → 248 bytes, `ValidationDescriptor` 584 → 344.
     - **Measured.** The definition row 907 → 603 bytes a claim.
   - **A claim state without an absent terminal cut inline** (as built).
     - An open claim held the cut's widest form (208 bytes) inline. The cut now sits behind its
       own allocation, made when the claim becomes terminal. `ClaimState` 1,000 → 800 bytes.
     - **Every terminal transition** allocates the cut before it changes anything, so a refusal
       leaves the claim as it was. The transitions are cancellation, expiry, supersession,
       admission and aggregate failure, graph failure and deadlock, and graph and receipt
       release.
     - **Every quote that promised such a transition no growth now counts the cut**, at its
       source:
       - the graph consequence quote: each member that may become terminal, and the root's cut
         in its own stage;
       - the completion envelopes' parent bound;
       - the deadline copies;
       - supersession in the creation plan;
       - claim recovery's quote for a recorded terminal row.
   - **One binding a claim.** A claim's binding (88 bytes) was held five times: its state, its
     lineage, its scope registry, its acceptance manifest and its registration set.
     - **As built.** The claim keeps its one binding and, beside it, the revisions the others
       were made at: the one it was created at (its lineage's and its registry's owner), the
       one its acceptance manifest was checked for, and the one its registration set was last
       adopted at. Each part is read bound to the claim's binding at its revision: the lineage
       is checked against it by what it carries, the registry's and the registration set's
       snapshots (what records and checkpoints encode) are taken with it, and the manifest is
       read through a borrowed view (`Acceptance`) carrying the binding and the claim's issuer,
       while the claim holds the manifest's slots and declarations alone (`PolicyBody`). Every
       revision a part may hold apart from the claim's was checked to be its binding's but for
       the revision where it is admitted and restored, so every encoding is the one held
       before. A definition's manifest, and a legacy registry's, stay whole.
     - **Measured.** ClaimState 800 → 536 bytes, the registration set 176 → 96; a claim
       3,776 → 3,554 bytes (lineage and registry) and 2,826 → 2,650 (registration set and
       manifest, after the payloads below).
   - **The payloads written once and read rarely off the heap.**
     - **The model, borrowed from Kafka and NATS.** A claim's events, its content, its creation
       result and its outcome hold where their bytes are in a log, sixteen or twenty-four bytes,
       instead of the bytes.
     - **The log.**
       - Each replica keeps its own log, in segment files under the session's directory, read
         by position.
       - It is a cache of what the replica's records and checkpoints hold, so it is emptied
         whenever the replica opens and refilled as they apply.
       - **Written behind.** An append is a copy into a 64 KiB tail held in memory, charged to
         the session; the tail is written to its segment a run at a time (when it fills, when
         the segment seals, before a reader is handed to another thread). The newest payloads,
         those a candidate's record and the steps after it read, are read from memory.
       - **Checked.** A reference keeps the payload's CRC-32, and a read that returns other
         bytes is refused. The log is the replica's own, never a peer's input: the check
         guards against accident, not adversaries. Measured here: an append 1.4–2.1 µs → 29–132
         ns, a read 0.4–0.9 → 0.3 µs, the check (BLAKE3's prefix before) 0.1–0.6 µs → 5–43 ns.
     - **What a spilled row holds.** Its record encoding (a content row its own bytes, whose
       tail is that encoding): a record or a checkpoint copies the bytes and never decodes
       them, and a measuring pass counts a spilled row by its length (`Sink::measures`), so a
       record's size, a checkpoint's and the running total read nothing back. A spilled
       outcome keeps beside where it is the sequence and event count that seals and
       retirements walk across many outcomes, in twenty-four bytes, so a row is no wider for it
       and no walk reads one back. A content row wider than 1 KiB (a long description) stays
       held, as does any row the log refuses (its bound, an I/O error).
     - **Where a row is spilled: once every check has read it.** A leader's plan and a replay's
       record build their rows held, check them (the authored checks, the fingerprint, index
       derivation, replay's validation), and only then spill their events, contents, creation
       results and outcomes together (`payloads::spill_changes`), each entry charged as it is
       spilled. Spilling where rows were built read each back up to five times a creation. A
       restore spills as it builds. Publication stays without I/O.
     - **Who reads it.** Every reader of a spilled row reads through the log: exact retries,
       the outcome expansion, the event index, retirement's derivation and bundles, audits,
       the completion books, peer and posting checks, replay's checks, reads, and record and
       checkpoint encoding. A frozen image's encoder, and a read lease, hold a reader with
       handles of its own.
     - **Failure is typed, never an absence.** A row that cannot be read back is
       `NativeError::Payload`: an outcome read returns it rather than none (an exact retry
       would run again), a retirement refuses (`PayloadUnavailable`), and the session fails
       closed until reopening rebuilds the log.
     - **Its bounds.**
       - A segment (64 MiB, at most 256) is charged to the volume in the ordinary lane before
         it is made.
       - A log that cannot have one refuses the payload, and the row keeps its bytes. The log
         never takes the completion reserve or the headroom.
       - One that cannot open is tried again, and its refusals are counted
         (`focal_session_payload_open_refusals_total`); `focal_session_payload_segments` and
         `focal_session_payload_bytes` say what it holds.
     - **Collection.**
       - A sealed segment no committed or pending row references is removed once retirements
         let it go, a sweep at most once a segment.
       - A restore sweeps everything the core it replaced spilled.
       - A reader made before a sweep keeps its handles, so its reads finish.
     - **Measured.** A claim's events, content, creation result and outcome 1,481 → 480 bytes
       (their entries alone); a claim 4,138 → 2,826 bytes charged, its held rows (2,379 bytes)
       now fewer than their encoding (2,429).
   - **Where a claim's 2,650 bytes are now** (one authored claim, one definition, production
     configuration): its row 928 (its state 536 inline, the registration set 96, the
     manifest's buffers 136, an entry), its definition 603, seven entries of 96 bytes for its
     spilled and identity rows (672: a 56-byte packed key, a 32-byte row, the heap count), its
     nine index keys in their key sets 192, and pages about 240.
   - **Next on this path, measured first.** The claim's 2,650 bytes put the comparison's
     envelope at about 250,000 open claims, past the 165,000 its runs reach, so what comes
     next is chosen by the comparison and a profile of the owner, as the claim table is to be
     built where it beats this store on those measures:
     - **The entries** (a third of a claim): a key front-coded against its neighbour in its
       leaf where a fixed 56-byte packed key takes each, and small fixed rows held as their
       key and value bytes in key sets, as index rows already are (about 400 bytes).
     - **The claim's state** (536 bytes): the registry's release cut and the claim's
       optional parts behind their own allocations or in their niches (about 110 bytes).
     - **The claim table proper.**
   - **Measured on CI (2026-10-14), and what it changed.**
     - **The fill.** With the door holding rather than refusing (doc 27 §18), the comparison
       refused nothing at 100, 200 and 1,000 writes a second. At 4,000 a second every node's
       session filled at about 241,000 open claims, 617 MB native of its 705 MB allowance
       (2,560 bytes a claim at scale), while the node used 710 MB of its 2 GB, and the 58,940
       writes after it were refused for memory. The run writes 300,000.
     - **CPU per claim doubled with this stage.** The leader's owner took 624 µs a claim at
       1,000 a second (cluster profile run 38586796329). The index families' key sets cost 160
       µs of it to publish, 58% in memcpy, and each follower 195 µs plus 71 µs in membership
       checks. A key (`SetKey`, 130 bytes inline) was moved four or five times a key as a block
       was merged with its edits, every edited block walked twice; on x86 each move is a call to
       memcpy, so aarch64 runs barely showed it. A key set now lends each key from its walk, and
       rebuilds a block from its first edit on, the keys before it copied with their restarts:
       2,754 -> 1,133 ns a published key, every block byte for byte what it was. A creation's
       plan check decoded its claim's descriptor a second time for each definition; it reads the
       requirement in place.
     - **What a claim's memory can lose and keep its readers.** Spilling the definition was
       built and set aside. Its readers reach through the model's traits
       (`aggregation::AdmissionView::declaration`, `object_journal::Source::definition`), which
       hand out references, and lifecycle steps read a claim's definition several times each,
       which is why it is held decoded. Within the readers as they are, the claim's optional
       parts behind one allocation (about 200 bytes, the scope registry the largest), the
       ledger its stored bindings repeat, identity rows as key-set keys and front-coded leaf
       keys come to about 450 bytes: 292,000 claims in this envelope, short of the 300,000 a
       4,000-a-second run writes.
     - **So rows must leave memory, read back where they are touched.** A row of any family,
       spilled as its record encoding, is decoded by `View::get` into an arena the operation
       owns, charged while it is held and freed with the operation, and lent from there: no
       reader changes. A policy spills what was not touched recently. The entries themselves
       still take about 1,300 bytes a claim (about 470,000 claims here), so past that, cold
       leaves leave memory as Kafka's old segments do, or the claim table takes their place.
   - **The backlog leaves memory (as built).** A claim waiting to be delivered is a message no
     consumer has taken: Kafka and NATS keep it in their logs. Focal now does the same with the
     claim's row and its authored definitions.
     - **Which claims.** One generated or posted, no receipt taken, no evaluation registered
       against it, no dependency of its own, none on it, created by a root command and owning no
       scope: the independent backlog. A claim in work, or tied into a graph or a scope that work
       on another claim reaches, stays held, so a lifecycle's steps, the completions they fund
       and the walks over claims in work read it where it is, and never wait on memory to read it
       back under pressure.
     - **Where.** Once a publication stands, each such claim and each authored definition it
       wrote is encoded to the replica's payload log and its row rewritten where it stands to
       hold where its bytes are (`RangeStore::rewrite`, `Core::spill_published`): the same key
       in the same page, the page's charge let go by the heap it held, the store's prefix and
       state unchanged, so a plan made before it publishes as it would have. What a publication
       just wrote is the store's own, so the rewrite never meets a page an image still reads.
       Candidates keep their claims held while they are pending: the owner's checks of a
       candidate read them there. A record and a checkpoint copy a spilled row's bytes, byte for
       byte what the held row encodes (the spill test compares records, replays, inline and
       frozen checkpoints and every claim and definition read back, leader and follower).
     - **Read back where touched** (`read_back.rs`). `View::get`, the state's own reads and
       replay's base reads decode a spilled claim or definition (checked as a replayed row is)
       into an arena the state holds, lent for the borrow, so no reader changes. A row read back
       twice between two changes of the state is decoded once; every row read back is let go at
       the next change (a publication, the owner's next operation). A walk of every row (the
       owner's integrity check) reads into an arena of its own let go row by row, and a lease
       reads into one of its own for each projection. A row that cannot be read back is never
       taken for an absent one: the read is lent the row as stored, which holds nothing a reader
       can take, and the failure fails the operation (a plan, a replay) that met it. A spilling
       state holds a 16 KiB floor for its reads from the time its log is attached, so a few
       reads never wait on memory and a refusal leaves the budget as it was.
     - **Measured.** One authored claim with one definition (`measure_memory_per_authored_claim_by_family`,
       release, 2,000 claims, the node's range configuration): 2,650 → 1,241 bytes charged a
       claim, the claim's row 928 → 96 bytes and its definition 603 → 96 (each now an entry, its
       bytes in the log). About 500,000 open claims in the comparison's envelope, against the
       300,000 a 4,000-a-second run writes. Publication 12.6 → 13.1 µs a claim, preparation
       unchanged (`measure_time_per_authored_claim`, release, 5,000 claims, three interleaved
       runs, least of each).
     - **Found by forcing it.** With `FOCAL_TEST_SPILL` set, every core of the test suite spills
       to a log in memory. The paths it found are fixed and tested: the owner's post-plan checks
       read a candidate's claims (so candidates keep them held), completions under pressure read
       a failed claim's dependents (so a claim another depends on stays held), the integrity
       walk and leases read stored rows (so both read back into arenas of their own).
   - **Archival: a seal is known short from counters.** The archive agent derived the outcome
     seal's plan every five seconds on the owner's thread, which visits every outcome awaiting a
     seal, only to drop it while they were fewer than half a bundle (about 15,000). The leader's
     slowest steps in the comparison at 200 writes a second were `range: seal bundle` every
     5.08 s, 61.6 ms growing to 119.5 ms, and the clients' p99.9 matched (96 ms against NATS's
     9.7). The resident outcomes less those the open generations hold, each with a creation
     result, bound every plan's rows (`Core::sealable_rows_bound`), so a round with nothing to
     seal visits nothing. Next for archival, borrowed from Kafka's segment roll and log cleaner:
     seal small and as generations close, a constant step rather than half a 6 MiB bundle at
     once, and cut the bundle off the owner from the payload log, where outcomes already are in
     append order, the owner proposing only the seal's record.
   - **The session's share of its allowance, beside the stages.**
     - **Measured first.** The per-session memory gauges (`focal_session_memory_*`,
       `focal_session_native_memory_*`) put the fill at the native engine's share, not at the
       node. In the comparison's envelope (a 1.5 GiB node budget, an 805 MB session allowance),
       the session refused at 403 MB native of a 403 MB share. About 1 MB sat beside it, and the
       node used 491 MB. The share was a constant ratio (half) carried over from the fixed
       budgets the allowances replaced, never derived from what the rest of a session holds.
     - **Why not simply wider.** Every replica holds the whole session: a leader admits within
       its share and the followers apply what it admitted. A binary that admitted within a wider
       share than an older follower holds would leave that follower unable to apply, mid rolling
       upgrade.
     - **As built (`SHARE_LEVEL` 8).**
       - A third budget lane, `Committed`: what a committed fact requires where it applies,
         held to a level's hold limit, never less than its limit. Replay, materialization, a
         record's publication, retirement and seal application, a layout's split or merge, the
         movement state, the import, a checkpoint's capture, freeze, assembly and restore, and
         the cold stream's capture reserve in it. The leader's admissions keep their lanes
         (`Ordinary` for fresh work, `Completion` for work completing it), and so do the
         proposals it checks.
       - Every binary at the level holds its session's whole allowance, so it follows a leader
         of either share. A leader admits within the wide share (the allowance less the
         session's completion reserve, ⅞) only once the fence its node recorded as the session's
         hosting was built opens the level; below it, half, as every older binary admits. The
         checkpoint a mutation may project follows the share; the bounds a checkpoint is encoded
         and read under follow the hold.
       - The movement coordinator's command check takes the lane from its caller: committed
         where a record applies, an admission lane where the leader checks a command before it
         proposes it.
     - **Found by the test that proves it.** A follower whose share was spoken for applied a
       claim's lifecycle, a split, and (lagging) the leader's checkpoint and its tail, then took
       its own checkpoint. Each failure named a site still charged as admission:
       - the movement state's re-layout and restore;
       - a phased hydration's pages, which ignored the lane they were begun in;
       - a checkpoint's frozen image, charged as fresh work. This one was a defect of its own:
         a leader at the fill, its ordinary lane full by definition, could take no checkpoint,
         so its log could not compact for as long as the fill lasted. It is fixed with its own
         test.
     - **Measured.** Three local nodes in the comparison's envelope, open claims at 1,000/s:
       - 94,038 committed before the session filled, against stage 1's 52,742. The fill came
         at 616 MB, the wide share's ordinary limit, against 403 MB.
       - At 4,000/s: 94,111 committed before the fill, the same capacity.
       - What the session held beside its native engine peaked at 0.27 MB at both rates,
         sampled every 5 s, against the eighth of the allowance (100 MB) the wide share leaves
         it.
   - **Refused work leaves nothing in the payload log (2026-10-15).** A candidate's plan spills
     its events and outcome to the log as it builds them, before its session has weighed it. A
     candidate refused after that (the checkpoint's bound, the pending room, the disk headroom, a
     moving range, the owner's own post-plan checks) left its payloads in a segment the open claims
     kept live: a million open claims offered to one node in a 2 GiB envelope grew the log from
     787 MB to 1.39 GB while nothing committed.
     - Now the log holds what a fresh preparation appends (`PayloadLog::hold`): none of it is
       handed to the writer until the candidate is admitted (`keep`). Refused or discarded, it is
       given back (`give_back`): the window cut where the hold began, segments opened since sealed
       empty, the next append where the first held one went. This is what a broker does when it
       validates a batch before appending it.
     - The owner keeps a hold before anything else appends: another preparation, a staged
       retirement, a publication once its ticket is the head's, the log detached. So a hold only
       ever covers its own candidate's bytes, and no published row names a byte the files lack.
     - Tested: refused creations leave the log's bytes as they were, a discarded candidate gives
       its bytes back, a rejected publication leaves the next candidate's held bytes to its
       discard, and held bytes never reach a file, even when a reader's barrier asks for every
       byte or a hold crosses chunks and segments.
     - **A refused transfer kept the hold (review).** Handing the committed core back from the
       owner kept the newest candidate's hold before it checked that nothing was pending; refused,
       it returned an owner whose discard could give nothing back. It keeps only once the
       transfer goes ahead. Tested: a transfer refused with a candidate pending, then the
       candidate's discard, leaves nothing in the log (698 bytes stayed before).
   - **At the checkpoint's bound (2026-10-15).** A million open claims offered to one node in
     a 2 GiB envelope (2,000 creations a second, none consumed) committed 434,661. Every write
     after that was refused at the checkpoint's bound, while the native share was at 577 of
     939.5 MB. The bound is real while an installing replica assembles the encoded root whole
     (`SeedManifest::assemble`). Besides the payloads refused work left behind (above), the run
     showed two defects at the bound, each fixed at its cause:
     - **A ledger at its bound could not empty.** The projection counted every write as a row
       and took nothing away, so a retirement projected growth. One limit stood for all work,
       so a cancel and a release were refused as a creation was. A ledger that reached the
       bound could neither finish its claims nor retire its families. The test that found it:
       2 of 14 claims could be cancelled at the bound, and the retirement was refused.
       - The projection is now exact (`NativeOwner::checkpoint_projection`): committed rows
         and bytes, plus what each pending candidate puts or adds, less what it replaces,
         deletes or takes away. Each candidate is measured on the root its predecessors leave,
         and a discard takes a suffix.
       - The bound has the memory's lanes (`CheckpointBound`): fresh work stops an eighth
         short of it, and work that finishes what was taken goes on to the bound.
         `ReleaseScope` joins the completion lane: it requires a terminal claim, and it is what
         lets the family retire. A candidate that takes away at least what it adds is never
         refused by the bound.
       - A root within a quarter of the bound of where fresh work stops retires finished
         families without waiting out their grace, as a session pressed for memory does
         (`Core::checkpoint_pressed`). The bound lives on the core and is set where the session
         makes the core its owner's, so admission and the archive's due rule read one value.
       - Tested: at the bound, 8 of 13 claims are cancelled where fresh work stops. Then the
         retirement is admitted, and a cancel the bound had refused is admitted after it.
     - **Each refusal there cost a preparation.** The check ran after the candidate was built,
       about 46 µs of owner time per refused request. The owner now checks the bound as it
       stages the candidate. A refused one leaves a shortfall, as a memory refusal does
       (`ingress::Need::Checkpoint`: its lane, and the rows and bytes it grew the root by).
       While the root has no room for that, a like request is refused from its header. Every
       32nd is prepared as a probe, the interval doubling to 1,024 while it is refused.
       - Measured (`measure_refusal_at_the_checkpoint_bound`, release): 45.8 µs a prepared
         creation, then 5,000 of 5,000 refused at 0.21 µs each. At the memory's bound the
         refusal costs 0.25 µs.
     - **A fixed reserve could not hold every claim's finishing.** The bound's lanes left an
       eighth of it to finishing work, and a review found the order that wedged it: at the
       bound, cancel every claim first, then release them. A cancel adds 3 rows and 680 bytes
       (its outcome, the claim's event and its status event, and the cut the claim ends at), a
       release 2 rows and 503 bytes, and only a released family retires. The cancels took the
       eighth before any release came: every release was refused, no family could retire, and the
       ledger could finish nothing more (14 claims, 10 cancels, every release refused).
       - Now each claim reserves its finishing where it is admitted (`Finishing`), by the shape
         it has: an open claim what its terminal transition adds and its release, a terminal one
         its release, each monitor it has active the cancel that must come before the release,
         and each live evaluation its fence. A claim with dependencies or a monitor active reserves
         for the largest cut a graph ends it at (204 bytes, a monitor cycle's, which names the
         claim whose deadline fired it; a dependency's failure is 164), one with a monitor for the
         monitor's deadline as well. A review found the monitor cycle: two claims waiting on each
         other's satisfaction, the older's own monitor firing, ended it at that cut 99 bytes past
         its reservation.
         The root with its reservations never passes the bound, and fresh work stops an eighth
         short of it as before. A transition that ends a claim or releases one takes no more
         than the reservation it gives back, so the bound never refuses it, in any order. This is
         how XFS and ext4's jbd2 reserve a transaction's log space before it changes anything,
         and how doc 22's completion promises reserve a report's rows before it arrives.
       - The reservations are kept as the encoded bytes are: counted for each candidate as its
         build counts what it replaces, kept as each publication, seal, replay and retirement
         applied straight to the root moves them, and summed once where a root is built whole. A spilled claim waits to be delivered, on
         nothing and in nothing, so it reserves what an open claim of no dependency and no
         monitor does, unread. Nothing a transition deletes (a timer it consumes) is counted on.
       - What it costs: an open claim of no dependency and no monitor reserves 5 rows and 1,187
         bytes beside the 12 to 14 rows and 1.7 to 1.9 KB its creation takes. With the rows
         deciding (a 200-row bound), 9 claims fit where 13 did; the other 4 were claims the
         ledger could not have finished.
       - Tested: every finishing path takes no more than it reserved, rows and bytes, and the
         running reservation is after every step what a walk of the rows gives
         (`finishing_a_claim_takes_no_more_than_it_reserved`: cancels of a posted claim, of one
         with an evaluation begun or not, of one others depend on (one on two claims) or await, of
         an owned tree's root; a claim's deadline and a monitor's; monitor cycles of two and three
         claims, the trigger's monitor firing or the victim's own; a monitor's cancel; every
         release). A
         ledger at its bound cancels every claim, then releases every one, then retires every
         family, then admits fresh work. Both fail with the reservations set to nothing.
     - **Not fixed, recorded.**
       - An installing replica still assembles the encoded root whole, so the bound stays where
         its memory puts it. Streaming the install chunk by chunk, as TiKV and CockroachDB
         ingest snapshots and Redis loads an RDB, would leave memory the only bound.
       - `NATIVE_ENTRY_BYTES` (96, `size_of::<(Key, Row)>()`) bounds the checkpoint's rows. A
         resident row measured 74 bytes in the run, so the row bound refuses before memory
         would.
   - **The payload log's sweep (2026-10-15).** The 2 GiB deliver run (env-7) held native
     memory near 690 MB, with retirement keeping pace. Its payload log grew by about 755 bytes a
     write all the same: 883 MB in 14 segments after 1.17 million writes, none ever let go.
     - **A sweep could let go of a segment rows still named.** The sweep's walk of the
       committed rows dated from when the log took events alone: it named the segments of
       events and nothing else. A segment only claims, definitions, outcomes, contents or
       creation results named was let go, and those rows could no longer be read back. A
       checkpoint, which reads every spilled row, then failed. It never showed in a run because
       events pinned every segment. Found by reading the walk; shown by a test that gives each
       append a segment of its own.
     - **The walk held the replica's thread.** It walked the root at once, at most once a
       segment's worth of payloads, and only after a retirement: a ledger whose rows were
       replaced and never retired never swept at all.
     - **Now (`payloads::Marks`).**
       - A pass over every committed row, of every kind, a bounded step at each applied batch:
         4,096 rows a step, a few tens of microseconds. A pass begins once the log has sealed
         two segments since the last pass that swept, or once rows an eighth of the root were
         written since that pass began (`CHURN_SHARE`): retirements and deletions leave copies
         no row names without sealing a segment, and a log at its segment cap seals none.
       - Rows spilled while it goes on land where appends go, from where it began, so its sweep
         lets go only of segments sealed before it began.
       - A publication while it goes on names its rows' segments to it. A candidate prepared
         before the pass and published after the pass had been past its keys would otherwise
         lose its bytes. A leader's pending candidates are named at the end.
       - What a pass notes (each segment met, with the bytes seen naming it) is charged as it
         grows, a doubling at a time on the completion lane, its charge admitted first and kept
         with the room between passes. A pass that cannot be charged for a segment it meets sweeps
         nothing (its notes are incomplete) and the next begins afresh; the log's sweep reads the
         notes in place, with no copy. A pass that fell short is not taken for a sweep
         (`SweepStep::Short`): the next is as due, after a wait that doubles from one step with
         each that falls short, to 1,024 at most (`SWEEP_RETRY_STEPS`).
       - The replica's diagnostics report the passes, the segments let go, the passes that fell
         short, and the bytes the last pass saw its rows name (`payload_passes`,
         `payload_let_go`, `payload_short`, `payload_named` beside `payload_bytes`). The last is
         an upper bound of the live bytes, not an exact count: a row published while a pass goes
         on may be counted twice, and one replaced after its count still counts.
     - **A cleaner, built and measured, not kept.** A pass that moved the rows of segments named
       half or less to where appends go (LFS's cleaner, WiscKey's value-log collection) freed
       nothing in the deliver run (env-9). 43% of its moves were refused: a frozen checkpoint
       image or a lifecycle read's lease shares the store's pages much of the time, and a
       rewrite in place must refuse a shared page (a reader holding the old reference needs the
       old segment as long as it reads). Its moved copies only grew the log, 1.06 GB against
       933 MB.
     - **Open: what pins the oldest segments.** With 97,000 of 146,000 families retired, the last
       pass of env-9 still found 321 MB named and no segment empty: something in every old
       segment outlives retirement. That decides the fix: retire or seal what pins them, or a
       cleaner that waits for the store's pages and keeps what it moves apart from new writes.
     - **A pass that fell short was taken for a sweep (review).** The replica counted the log's
       segments as swept when a pass ended, short or not, and the next pass waited for two more
       to be sealed. A log at its segment cap seals none, so a passing memory refusal stopped its
       reclamation for good. Now the pass says it fell short, the replica tries again at the
       bounded cadence above, and rows written make a pass due without the log growing.
     - **Retirements applied directly kept stale totals (review).** A follower applies a
       committed retirement record (and a legacy retirement applies a family) by publishing its
       changes straight to the root, as a seal does, but neither moved the root's running totals:
       the encoded bytes kept the retired families' (11,086 against the 7,081 the rows give in the
       follower test), so a follower promoted to leader projected its checkpoint too large and
       refused fresh work it had room for, and the rows it wrote were no churn, so a follower's
       log at its cap never swept for its retirements. Both now count their changes before
       anything changes and move the totals with the publication. Tested: a follower's totals
       equal a walk of its rows and the leader's after a derived retirement; a legacy
       retirement's checkpoint is what its total says; both fail without the counts.
     - **What a retirement leaves.** In a session at its cap, a finished family's retirement left
       its oldest segments named by its requests' outcomes and creation results. A seal takes
       those once their generation closes, as the archive's seals do in a running node.
     - **Tested.** A sweep keeps the segment of every committed row of any kind (a log whose
       every append is a segment of its own; it fails with events alone named). A candidate
       published after a pass's last step keeps its segments (it fails without the
       publication's word), and what an unpublished one spilled goes. A session's log at its cap
       of 256 segments, every pass short while finished families and then open claims fill it,
       the families retired and their generation sealed: once memory is back, 12 dead segments
       are let go within the longest wait and one pass, no segment sealed (10 passes fell short;
       it fails with the short pass taken for a sweep).
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
- [LevelDB table format] LevelDB, `doc/table_format.md`: data blocks of prefix-compressed keys
  with a restart point every sixteen entries. https://github.com/google/leveldb/blob/main/doc/table_format.md
- Redis streams: `t_stream.c` and `listpack.c` (a stream's entries delta-coded in listpacks
  under a radix tree). https://github.com/redis/redis
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
