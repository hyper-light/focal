# 33 — Fate-aware durability: the bus's replicated and persisted classes

Status: 2026-10-17, a design. The owner chose durability per ledger ("Per-ledger choice") and asked
whether anything improves on the situation-aware model of [32](32-event-bus.md) D2. This document is
the answer: the research read for it, what it shows the existing models leave open, and the design
for step 3 of doc 32 (three nodes, the replicated and persisted classes) that closes it. Nothing here
is built yet; §8 is the order it will be built and measured in.

## 1. What was measured first

The ledger's durable path today, on the bare-metal box (three nodes as containers, one CPU each,
128-byte messages, every system configured durable: Kafka `acks=all` with `flush.messages=1`, Redis
`WAITAOF`, JetStream `sync_interval: always`):

| 400 lifecycles/s | deliver p50 | deliver p99 |
|---|---|---|
| Kafka | 59 ms | 215 ms |
| Redis | 66 ms | 328 ms |
| focal (ledger path) | 329 ms | 1.06 s |
| JetStream | 26.7 s | 27.3 s (5,613 refused) |

Where focal's time goes:

- A lifecycle is five sequential commits on the old ledger path (create, post, acquire, submit, post),
  against two durable steps for Kafka and Redis. Doc 32's lifecycle is two publishes and two
  acknowledgements; step 3 is what brings it to the cluster.
- A commit's log wait is p50 23 ms on the leader (19 ms on followers) against a 5.8 ms flush.
  hyper-log answers a frame only once a later durable record confirms it (AGL+18 §3.3.3, doc 28),
  so each answer waits for two serial flushes plus queueing.
- On this box's btrfs (`compress=zstd:1`, Samsung 9100 PRO, `queue/write_cache` = "write back",
  `queue/fua` = 1), a 4 KiB durable overwrite costs 2.6 ms p50 whether made with
  `pwrite`+`fdatasync` or `pwritev2(RWF_DSYNC)`: btrfs gives the log no FUA fast path. A NOCOW
  file (`chattr +C`) is no faster (`RWF_DSYNC` 4.4 ms). The log writes about 83 KB a frame, about
  5.5 KB of log a 128-byte message.
- The tail is environmental on this shared box: the same image gave a write p99 of 253 ms and of
  82 ms minutes apart. The slow writes clustered into 0.2–1.3 s episodes that hit every in-flight
  write at once, while other sessions' builds loaded the host.

## 2. Sources read

Each was read at its source on 2026-10-17. Numbers quoted are the sources' own.

- **SAUCR** — Alagappan, Ganesan, Liu, Arpaci-Dusseau and Arpaci-Dusseau, "Fault-Tolerance, Fast and
  Slow: Exploiting Failure Asynchrony in Distributed Systems", OSDI 2018.
  - Fast mode holds while more than a bare majority is functional (⌈n/2⌉+1), and commits once
    ⌈n/2⌉+1 nodes have buffered an update. With n = 3 that is every node.
  - The leader switches to slow mode (synchronous persistence) on its first missing heartbeat
    response. Followers flush as soon as they suspect the leader.
  - Recovery uses persistent mode markers and replicated last-logged-entry maps, recovered
    "max-among-minority".
  - Any number of independent or non-simultaneous failures leaves it durable and available. If more
    than a bare minority fails in fast mode at the same instant, it stays unavailable rather than
    lose data silently.
  - Its evidence that failures are not simultaneous (the Non-Simultaneity Conjecture): in Google's
    cluster traces, two failures among five machines picked at random are more than 50 ms apart in
    99.9999% of samples, picked "without considering failure domains".
  - Overhead within 0–9% of memory-only ZooKeeper; 2.5× faster than disk-durable on SSD.
- **CAD / ORCA** — Ganesan, Alagappan, Arpaci-Dusseau and Arpaci-Dusseau, "Strong and Efficient
  Consistency with Consistency-Aware Durability", FAST 2020.
  - Writes complete as the asynchronous baseline does. A read of item *i* is served only once every
    update up to *i*'s update index is persisted on a majority (the durable index, kept from
    followers' persisted indexes); otherwise the leader replicates and flushes synchronously first.
  - Follower reads come only from a leased active set.
  - The result is cross-client monotonic reads, not linearizability: recent writes nobody has read
    may still be lost.
  - 1.8–3.3× the throughput of strongly consistent ZooKeeper.
- **Ford et al.**, "Availability in Globally Distributed Storage Systems", OSDI 2010 (a year of
  Google's storage cells).
  - A failure burst is failures each within 120 s of the next. About 37% of failures are in bursts
    of at least two nodes.
  - Of bursts of at least ten nodes, only 3% have every node on a distinct rack: correlated failures
    follow failure domains.
  - Mean time to failure: node 4.3 months, rack 10.2 years, disk 10–50 years.
  - A power-outage burst took about 50 nodes over about 20 s, one after another.
- **Redpanda write caching** (24.1 documentation and the vendor's benchmark post). It acknowledges
  once a majority has received a write, without waiting for its fsync. It is opt-in for production,
  and its latency gains (19–98%) are the vendor's own.
- **Kafka KIP-966** (Eligible Leader Replicas). Unflushed data lost in an unclean shutdown can become
  global loss when the last in-sync replica, having lost it, is re-elected and the rest truncate to
  it. Kafka acknowledges from the page cache and relies on replication.
- **RAMCloud**, design slides 2010–2013 and the 2015 TOCS paper as summarised. Backups buffer log
  segments in DRAM, and durability assumes enough power after a failure warning to flush them: the
  battery assumption, made explicit.
- **Amazon MemoryDB** (SIGMOD 2024), via reviews; its acknowledgement numbers are not yet checked
  against the paper. Durability moves to a separate multi-zone transaction log, and a write becomes
  visible only once that log has committed it.
- **Linux block layer**, from the kernel's documentation.
  - `queue/write_cache` reads "write back" when the device caches writes in volatile memory and
    "write through" when it does not. Writing the file changes only the kernel's view.
  - For a device with no volatile cache, the block layer completes empty `REQ_PREFLUSH` requests
    before the driver and strips `REQ_PREFLUSH` and `REQ_FUA` from requests with a payload, so a
    flush costs nothing.
  - `queue/fua` says whether the driver passes FUA, a write that bypasses the volatile cache.
- **Carbonite** (Chun et al., NSDI 2006) and **Tempo** (Cornell, 2006): choose a replica count and a
  repair rate that meet a durability target over years. They concern storage repair, not a
  per-write acknowledgement.
- **AGL+18** (protocol-aware recovery, FAST 2018) and hyper-log's §6.2: why a frame is answered only
  after a later record confirms its flush.
  - Without the confirmation, a torn tail and a damaged acknowledged frame read the same.
  - Answering at the frame's own flush lost an acknowledged entry in hyper-raft's audit (S01).
  - Marking every such frame uncertain left groups that could elect no one after a power loss tore
    every node's last frame.

Searched and not found: a system or paper that picks, for each write, between memory and stable
media from the failure domains its holders span, the media they hold it on, and a stated loss bound
(searches 2026-10-17; the nearest are the static choices above and the replica-count models). That
absence is a claim to keep testing, not a proof.

## 3. What the existing models leave open

1. **They count nodes, not fates.** SAUCR's fast mode counts functional nodes, and its evidence for
   non-simultaneity was sampled without regard to failure domains. Yet the cause of a simultaneous
   failure is shared fate: Ford's large bursts are rack- and power-concentrated. Three replicas on one
   host, one power feed or one laptop fail at the same instant whenever the host does.
   - Kafka without flush, Redpanda's write caching and SAUCR all acknowledge from memory there, so a
     power cut loses acknowledged writes or, at best, makes them unavailable.
   - Doc 32's own benchmark runs three nodes in one box.
2. **A mode is all or nothing.** A stream is either acknowledged from memory or from disk. Nothing
   relates the acknowledgement to a loss target, to the measured time the system needs to react, or
   to what each replica's medium costs. A power-loss-protected drive makes disk durability nearly
   free, and a laptop's btrfs makes it 2.6 ms a sync.
3. **The confirmation's second flush is paid everywhere.** It exists for the case where every node's
   last frame tears at once, which only replicas that share fate can do.
4. **Delivery ignores durability.** CAD gates reads; the bus delivers. A consumer acting on a message
   below its stream's bound is the bus's version of CAD's non-monotonic read.

## 4. The design: a commit rule chosen per write

### 4.1 What the bus knows

- **Fate domains.** Each node has a path from coarse to fine, for example region › zone › power ›
  host. It comes from the node's configured topology (`topology: region, zone` already) and from what
  the node detects of itself, such as its host identity and the machine it runs on. Two replicas on
  one host are one fate, whatever their processes. An operator may declare finer independence (two
  power feeds in one zone); the bus never infers more independence than it can see.
- **Media.** For each replica's log device:
  - its claim: `queue/write_cache` and `queue/fua` on Linux; `F_FULLFSYNC` on macOS, which has no
    write-through claim; `FlushFileBuffers` on Windows;
  - its measured cost: a calibration of a durable write and a flush at the sizes the log writes, made
    at start and refreshed from the log's own `focal_log_flush` and commit-wait histograms.
  - Safety comes only from the claim, cost only from the measurement. A "write through" device counts
    as stable at the write's completion. A "write back" device counts as stable only after a flush.
- **Liveness, per node, not per group.** Each peer node's state comes from hyper-liveness: one
  heartbeat stream for each node pair, shared by every group the pair shares, each pair an NFD-E
  detector, Lifeguard-adjusted. A suspicion applies at once to every group with a replica on that
  node. No group's own heartbeats decide it: at Meta scale a node hosts many thousands of groups, and
  an idle group goes quiet, as CockroachDB quiesces idle ranges. A count of heartbeats per group would
  then be a storm of messages, or a suspicion that lags.
- **The reaction window.** ρ is the measured time from a peer's failure to the survivors' writes of
  what they hold completing: detection plus the writes already under way.

### 4.2 The classes

Doc 32's three classes stay. What changes is what "replicated" and "persisted" commit on.

| Class | Acknowledged when | Survives |
|---|---|---|
| ephemeral | the owner has it | nothing past the owner's crash; gaps are announced |
| replicated | ⌈n/2⌉+1 voters hold it in memory (SAUCR's fast quorum), none suspected, and the holders span enough independent fates to meet the stream's loss bound with the measured ρ (§4.3); otherwise as persisted | any failure the bound covers; beyond it, unavailable rather than lossy (§4.5) |
| persisted | a majority holds it on stable media: a flushed write-back device, or a write-through device at completion | any crash of any number of nodes |
| windowed | the owner has written it to its log, ordered, and a full flush follows within the stream's window W | any crash, keeping a consistent prefix: writes acknowledged within W of a power cut may be lost, never torn or reordered |

A replicated stream with every replica in one fate (a laptop, a single box) commits as persisted, and
its acknowledgement says so. The bus never acknowledges a memory quorum that a single power cut would
erase.

**The windowed class is the single machine's honest fast path.** It is SQLite's WAL at
`synchronous=NORMAL`, which its documentation states "is always consistent" but "might roll back"
transactions committed before a power loss or system crash, made explicit by the window and typed in
the acknowledgement.

- **Consistency.** The log is append-only and checksummed, and recovery stops at the first record
  that does not verify. What survives is always a prefix, whatever order the device persisted it in.
- **Loss bound.** A full flush at most every W (`F_FULLFSYNC`, `fdatasync`, `FlushFileBuffers`)
  bounds what a power cut can take back.
- **macOS ordering.** `F_BARRIERFSYNC` orders each commit at the cost of a barrier, not a drain. The
  local `fcntl(2)` page: what was flushed before the barrier "is guaranteed to be persisted before
  any other I/O that would follow the barrier", with no claim on what is persisted when it returns.
  So less of the window is lost.
- **Use.** Agent work on a laptop, where seconds of loss after a power cut cost a retry. The ledger's
  records of value stay persisted.

### 4.3 The loss bound

A stream's policy is a target ε: the acceptable probability that an acknowledged write is lost.
The default is to be derived from the owner's statement, not chosen here. For a write held unflushed
in fates *F* = {f₁…f_k}:

- It is lost only if every fate in *F* fails before the write is flushed somewhere stable.
- Background flushing bounds how long it stays unflushed, τ (measured: the age of the oldest
  unflushed byte at each flush).
- The reaction flushes the survivors within ρ of the first failure among *F*.
- With fate failure rates λᵢ, the loss probability is at most about τ·Σᵢ λᵢ·Πⱼ≠ᵢ (λⱼ·ρ) for small rates.
  - The rates are learned from the cluster's own failure history.
  - Ford et al.'s fleet figures seed them until the history is long enough, labelled as such.
- The commit rule takes the smallest *F* among the ⌈n/2⌉+1 holders (§4.4a) that meets ε. If the live fates cannot
  meet it, the write commits as persisted.
- The estimate is for the failure model of §4.5 only; Byzantine faults, firmware that lies about
  flushes and operator error are outside it, as for every class.
- **First to arrive.** A write commits on whichever comes first: the durable majority or the held
  quorum. Within a region the held quorum wins, at a round trip without a flush. Across regions, a
  flush is shorter than the extra replica's round trip, so the durable majority wins and the rule is
  never slower than today's.

### 4.4 Reaction and background flushing

- Every replica flushes its log in the background, so the dirty set stays small enough to flush
  within the reaction budget the stream's bound needs. It is SAUCR's background flush, sized by the
  bound instead of left to chance.
- On suspicion of a peer node, every leader with a replica there stops counting that replica's held
  answers, and commits subsequent writes under the rule the remaining fates allow. This is SAUCR's
  switch, triggered per fate rather than per node count.
- **No flush storm.** Persistence never stops, so the switch needs no burst of flushes. Every
  replica's writes are already under way, and the switch only changes which answers count. This
  matters at scale: one node's failure touches thousands of groups at once, exactly when capacity has
  just fallen.
- A fate that recovers and responds promptly for a measured number of rounds restores the faster
  rule, as SAUCR restores fast mode.

### 4.4a Why the memory quorum stays ⌈n/2⌉+1

The first draft of this design committed a replicated write once a plain majority held it in
memory across enough fates. That is unsafe, and the counterexample fixes the rule.

1. Take three voters. The leader L and N hold entry *i* in memory, and L commits *i* on their two
   held acknowledgements. C lags and never received *i*'s append.
2. L and N crash before either flushes *i*.
3. Each restarts not knowing how far it acknowledged. It asks a bare minority, which is C, for its
   last logged entry, as SAUCR's recovery does. C's map, never sent *i*'s append, names an older
   entry.
4. L takes its durable log as complete, campaigns, and C votes for it. The group then leads on
   without *i*: an acknowledged write lost silently.

SAUCR's fast quorum of ⌈n/2⌉+1 is what makes its max-among-minority recovery sound. The holders of
a fast commit and any bare minority answering a recovery overlap, so the recovered last-logged
entry is never below a committed one (its §3.5). The fate condition is therefore added to SAUCR's
quorum, never in place of it. It refuses a memory commit that SAUCR would make among replicas that
share a fate (one host, one power feed), and it binds the loss probability where the replicas do
not. With three voters that means all three hold the write in memory, one round trip and no flush.

### 4.5 Recovery and the guarantee past the bound

- SAUCR's recovery, carried into hyper-durable:
  - a persistent marker of the mode an entry was logged in;
  - each replica's last logged entry, replicated in the leader's appends;
  - a replica recovering from a memory-mode crash takes the maximum among a bare minority's maps
    before it may vote, as VR Revisited's recovering replica does.
- **Batched per node pair.** A restarted node that hosts thousands of groups sends each peer one
  query naming them all. The peer answers from the maps of every group it shares, in one message, as
  hyper-multilog batches its groups' traffic per node pair. The node's groups recover in one round
  trip, not one per group.
- If more fates fail together than the bound covers, the stream becomes unavailable and says so. It
  never silently loses data. An operator may accept a loss explicitly, as Kafka's unclean election
  and KIP-1275's tool let one do.

### 4.6 The log's confirmation, chosen by fate

- Where a group's replicas share one fate, hyper-log keeps today's rule: a frame is answered after a
  later record confirms its flush, two serial flushes, because a power cut can tear every replica's
  last frame at once.
- Where they span independent fates, a frame is answered at its own flush. A last frame that fails
  verification at recovery is resolved from peers, as AGL+18's CTRL resolves faulty entries: a
  simultaneous tear of every replica's tail would need every fate to fail together, which §4.5
  already treats as unavailability. That halves the persisted class's disk cost per commit.
- **A node with no peers** (a laptop's single replica) has no election to lose to a correlated tear.
  The second flush buys only a diagnosis: whether a last frame that fails verification was torn
  before its answer, or damaged at rest after it, a media fault a single replica cannot survive in
  either case.
  - So a frame is answered at its own flush, and a confirmation is written periodically, every W or
    every N frames.
  - It bounds the frames whose at-rest damage would be taken for a tear, and reported as such, to
    those since the last confirmation.
  - That halves the laptop's flushes per commit.
- This changes hyper-log's §6.2 and needs its audit S01 scenario re-run under the new rules before
  it lands.

### 4.7 Delivery waits for the bound

A consumer is delivered a batch only once the batch meets its stream's bound, which is CAD's
durability check moved to the bus's delivery. Without it, a consumer that acted on a message a crash
then erased would have done work the ledger no longer records. Most batches meet their bound before
a consumer asks, because the background flush runs ahead; the check costs a compare against the
part's durable index (as CAD keeps one index, not one per item).

### 4.8 Every acknowledgement is typed

The producer's acknowledgement carries the class achieved and how: fates held and media. A producer
may ask to wait for a stronger one, as MongoDB's write concern and Kafka's `acks` let a client
choose, but per write and with the measured cost known. A benchmark reports which class each system
was held to.

### 4.9 Energy is a cost

A laptop is battery-bound, and a flush costs the device its power for its duration and the host a
wakeup.

- **Measured, not assumed.** The calibration (§4.1) records a flush's device time, and hyper-measure
  the process's energy where the platform reports it (macOS's per-process energy, Linux's RAPL where
  readable). A cost is then milliseconds and joules per committed byte.
- **Coalesced.** Background flushes are coalesced to the largest interval the streams' bounds allow:
  the windowed class's W, the replicated class's τ budget.
- **Scheduled with the machine.** Writes not owed to an acknowledgement go when the device is
  otherwise awake, and stretch on battery (macOS's power source state,
  `/sys/class/power_supply` on Linux).
- **The floor.** No acknowledgement a class promises is ever delayed for energy. Only the work behind
  it moves.

### 4.10 Locality

At global scale a stream also chooses where its quorum lives, as CockroachDB's survival goals do:
zone survival is its default, and region survival costs writes "at least as much as the round-trip
time to the nearest region".

- A **region-local** stream commits within one region's replicas, under the classes above.
- It replicates to other regions asynchronously, its acknowledgement typed as such.
- A **multi-region** stream's quorum spans regions, and §4.3's first-to-arrive rule then picks the
  durable majority.

## 5. What every class needs on the path

These levers pay whichever rule commits:

- **Bytes a frame.** About 83 KB a frame for about 15 entries today. hyper-log lays a frame out after
  each sweep's copies of older live records; find what the bytes are before changing anything (doc
  28's frame layout).
- **Dependent writes pipelined.** A principal's next write is not held behind the previous one's
  commit; only what another principal observes waits (§4.7). This is Nightingale et al.'s external
  synchrony (OSDI 2006), and Kafka's idempotent producer keeps its in-flight requests ordered the
  same way (KIP-98). Not yet re-read for this document.
- **Medium calibration** (§4.1) exported as metrics, so every benchmark row shows what the disk cost.

## 6. What is claimed new, and what would falsify it

- **Claim.** A per-write commit rule chosen from fate diversity, media and the measured reaction
  window to meet a stated loss bound, with delivery gated on the same bound and the log's
  confirmation chosen by the same fates. To our knowledge from §2's search, no published system or
  paper does this; the parts are SAUCR's, CAD's, AGL+18's and the replica-count models'.
- **Falsified if** a prior system is found that does it. That would be a source to adopt and cite,
  not a reason to stop.
- **Falsified also if** the measurements do not pay. The replicated class across independent fates
  must reach doc 32's bar against Kafka `acks=all` and JetStream R3 at their settings. The single-fate
  case must not be slower than the persisted class it falls back to. The adversarial battery (§7) must
  find no acknowledged write lost within the bound.

## 7. The adversarial battery

Every run checks acknowledged writes against what survives, with the black-box history reader, and
classifies each run as correct, unavailable (allowed past the bound), or lost (never allowed).

- **Simultaneous kill** of all nodes, with `docker kill`, where memory is lost and the kernel has not
  flushed (in one fate, then across declared fates).
- **Staggered kills** at gaps below and above the measured ρ.
- **Leader kill at each step** of the switch and the flush.
- **Partitions:** minority, majority, flapping.
- **Disk:**
  - a flush stall (cgroup I/O throttling to a near halt);
  - a device that loses its last unflushed writes (`dm-flakey` or the hyper-sim device);
  - torn last frames on every replica.
- **Lies:** a device claiming write-through that drops writes at a cut, to show what the claim
  costs when false, recorded as outside the failure model.
- **Clock and scheduling:** the reaction under 1-CPU starvation (the busy-machine rule), measuring ρ
  as the starved node sees it.

## 8. Steps

| Step | What | Bar |
|---|---|---|
| 3a | Medium detection and calibration on Linux, macOS and Windows; exported; shown in every benchmark row | claims read and costs measured on the box and in CI |
| 3b | A part replicated over hyper-durable across three nodes; persisted class | against Kafka `flush.messages=1`, JetStream `sync: always` and Redis `WAITAOF`, class for class; the battery's crash rows |
| 3c | Fate model from topology and detection; replicated class with the commit rule, reaction and recovery of §4.3–4.5 | against Kafka `acks=all` (rack-aware), Redpanda-style write caching and JetStream R3 at their defaults; the battery's full rows |
| 3d | Delivery gated on the bound (§4.7); typed acknowledgements (§4.8) | no consumer sees a write later lost, under the battery |
| 3e | hyper-log's confirmation chosen by fate and by peers (§4.6), with the S01 scenario re-run | persisted-class commit at one flush across fates and on a single node |
| 3f | The windowed class (§4.2), energy-coalesced flushes (§4.9) | loss after a power cut within W and a consistent prefix, under the battery; joules per committed byte against persisted |
| 3g | Node-level suspicion for every group, recovery batched per node pair (§4.1, §4.5) | one query per peer on a restart of a node hosting many groups; no per-group heartbeats while idle |
| 3h | Region-local and multi-region streams (§4.10) | commit latency against CockroachDB's zone and region goals' published costs, class against class |
