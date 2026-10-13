# 30. A write path ten times better: in memory first

Status: research, 2026-10-12. The owner's question was what focal can use to beat NATS JetStream,
Kafka and Redis by an order of magnitude (goal condition 4) while holding 100k+ claims, rates and
clients without collapse. This note measures where a write's time goes, reads what the literature
and the competitors' code say about each part, tests the leading candidates on focal's own row
shapes, and states what to use. Nothing here is built yet; §8 is the order to build it in.

## 1. The target in numbers

The comparison (doc 09, `tools/compare`) runs three containers of one CPU each on one GitHub
`ubuntu-24.04` runner, open-loop, every acknowledged write on disk at a quorum. The competitors'
latest durable rows (run 38024634728, 128-byte records), p50/p99:

| Offered | Kafka (flush per message) | NATS (`sync_interval: always`) | Redis (AOF always, `WAITAOF 1 2`) |
|---|---|---|---|
| 100/s | 7.7 / 12.5 ms | 2.3 / 4.3 ms | 11.5 / 56 ms |
| 1,000/s | 5.7 / 12.6 ms | 2.8 / 5.4 ms | 2.7 / 5.4 ms |
| 4,000/s | 5.7 / 12 ms | 33% refused, p99 1.6 s | 2.3 / 5.5 ms |

focal on the same runner (run 38327120235, head c2b89c5, an authored claim of about 2.9 KB a
record): 100/s p50 2.5 ms, p99 5.1–5.4 ms; 1,000/s p50 8.3 ms, p99 48–55 ms, 6.5–11% refused;
4,000/s committed 1,044–1,220/s, p99 132–143 ms; 12,000/s committed about 750/s.

**What ten times can mean.** The floor under any quorum-durable write is the client's round trip
plus the slower of the leader's flush and a follower's round trip and flush. A commit is two
flushes (doc 28 §2), and the runner's disk took about 334 µs a 4 KiB synchronous write
(`dd oflag=dsync`, the workflow's disk step). So the floor there is roughly 1 ms, and NATS's
5.4 ms p99 at 1,000/s divided by ten is under it. Ten times at low rates is not available on
that disk, to anyone. It is available in three places:
- a p99 at the floor (about 1 ms) where the others sit at 5–13 ms;
- throughput at a p99 bound: NATS stops near 3,000/s (§3), so ten times is about 30,000/s at a
  p99 of a few milliseconds, where Kafka's is 12 ms;
- tails under load and on a busy machine, where NATS reached 1.6 s at 4,000/s.

All three need the same thing first: much less CPU per write (§2, §4.4).

## 2. Where a write goes today (measured)

**CPU per committed write**, from the cgroup counters (run pc18: 1,000/s offered, 1-CPU nodes,
musl and mimalloc). Leader about **1,400 µs**; each follower about **820 µs**. The leader is
pegged: 0.82 CPU over the run, throttled in 520 of 1,055 periods. Per committed write the leader
moved 9.5 datagrams in and 8.7 out, 2.2 KB in and 7.9 KB out.

**By thread** (perf, folded stacks, the leader at 1,000/s):

| Thread | Share of the leader's CPU |
|---|---|
| Session owner (state machine) | 50% |
| Network (hyper-rt shards: QUIC, transport, request handling) | 42% |
| Checkpoint | 3.5% |
| WAL (hyper-log, its I/O thread) | 4% |

The disk is not the cost. **The owner's half** splits as follows (shares of the owner's samples):

| Owner work | Share |
|---|---|
| Rebuilding every leaf a write touches (`build_in_with`'s row copier: locate, deep-copy, allocate, charge) | 17% |
| Planning which leaves change (`ranges::plan`, `plan_after`) | 10% |
| Lookups and checks through the page directory (`mutation::check`, `original_plan::check`) | 6% |
| The write's own logic (`transactions::prepare`: validate and build the claim) | 9% |
| BLAKE3 (input inspection, fingerprints, digests) | 10–12% |
| memcpy / allocator, spread over the above | 12% / 7% |

The rest is the engine around the write: drain, progress publication, delivery and events. The
store's machinery costs about three times the write's own logic. That is because the session's
state, although in memory, is a copy-on-write B-tree of memory-page leaves (4 KiB on Linux, up to
128 entries; `crates/focal-ledger/src/native_session.rs`), and "a mutation rebuilds its touched
leaves whole". A claim writes about 18 rows: its own rows under its affinity, nine
secondary-index rows each under a different affinity, and the Meta and Epochs rows that every
write rewrites (`original_plan.rs`). So each write rebuilds about eight leaves, deep-copying every
unchanged neighbour. docs/qualification/allocations-2026-09-29.md measured about 80 allocations
over about 8 touched pages a claim (§2.6.2 site 6), and 212 allocations and 219 KB requested per
claim in all at 16 KiB leaves.

**The network half** splits by innermost layer: kernel UDP path 38% (about 12 µs a datagram,
including the bridge delivering to the receiver in the sender's context), hyper-quic 14%,
hyper-transport 9%, focal-wire 9%, hyper-rt 8%, memcpy 8%, focal-node request handling 6%,
allocator 4%, AEAD 3%. Signature verification is under 0.5%.

**The same measurement locally**, three 1-CPU containers on one Apple-silicon machine, one generator:

| System | CPU per write |
|---|---|
| NATS 2.12.15 (R3, file, sync always) | 130–200 µs a node |
| focal (`focal:bench` of 2026-10-12) | leader 577 µs, followers about 400 µs |

focal's write carries a 2.9 KB claim against NATS's 128 bytes. NATS's stream leader profile: 57%
in system calls (`writev` to clients and routes, the per-message sync), the stream store's write
24%, the apply loop 21%.

## 3. What the competitors do (their code and published analyses)

- **NATS** (nats-server v2.12.15, cloned at `../nats-server`):
  - The Raft leader batches every queued proposal into one append, up to 256 KiB or 512 entries
    (`server/raft.go`, `runAsLeader`).
  - With `sync_interval: always` the stream store syncs on every message-block write
    (`server/filestore.go`, `if mb.syncAlways { mb.mfd.Sync() }`). So durable NATS pays a device
    sync per message on every replica, which is why it stops near the disk's sync rate (about
    3,000/s here) and its p99 at 4,000/s is 1.6 s.
  - Its CPU per message is low because the message is opaque: no per-write state structure, a
    subject lookup, appends.
- **Kafka**:
  - Batches records per partition and keeps them opaque on the broker, so per-record broker work
    is small.
  - Its p99 at our rates (about 12 ms) is its replication and flush path, not CPU.
- **Redpanda** (thread-per-core, Raft per partition) claimed tails far below Kafka's. An
  independent reproduction found Kafka ahead in most tests, tails included, and Redpanda's p99
  reaching 3.5 s after 12 hours of sustained load [Vanlightly 2023]. Thread-per-core alone is not
  the order of magnitude; the work per write is.

## 4. What the literature says, by layer

Each result below was read in the source unless marked *abstract*.

### 4.1 State: in place, hashed where point access suffices, ordered only where needed

- **In-place updates are what in-memory speed requires.** FASTER keeps a hash index over a log
  whose in-memory tail is updated in place. Records are copied only once they age into a
  read-only region. The authors found log-structured allocators "unable to scale beyond a few
  million operations per second, due to the overhead of read-copy-update", and reached up to 160M
  operations a second on one machine. Its index is cache-line buckets of seven 8-byte entries
  (48-bit address, 15-bit tag) that do not store keys [FASTER, VLDB 2018 demo §3].
- **Speculation and versions without copies.** HyPer updates in place and keeps before-images in
  undo buffers, with very little overhead compared to serial single-version execution
  [Neumann et al. 2015, *abstract*]. That is the shape of focal's candidate pipeline: a candidate
  applies in place and keeps its undo until it commits; a refusal rolls it back.
- **Hash tables beat radix trees for point operations.** A well-chosen hash table is at least 2×
  faster than ART for lookups and inserts; at one billion keys, 4.8× for inserts and 2.8× for
  lookups, sometimes in half the space. For range queries ART is up to 3× slower than a B+-tree
  [Alvarez et al. 2015 §V].
- **Ordered indexes: B+-tree or ART, never skip lists.** ART leads on point workloads; the B+-tree
  is 3–5× faster than the others on scan/insert; the skip list shows "high variation and low
  performance"; the Bw-tree is slower than locked structures [Wang et al. 2018 §6].
- **Growth must not stop a write.** A hash table that rehashes all at once stops the write that
  triggers it (§5: 1,968 µs). It must be sized to its bound up front, which focal's bounds allow,
  or resized incrementally.

### 4.2 Checkpoints without per-write copies

- **Copy only what changes while a capture runs.** Cao et al. checkpoint main-memory state with
  no locking and no bulk copies, at nearly constant latency. That cut overhead by more than an
  order of magnitude at low and medium update rates, and by more than 2× under heavy load. The
  cost is memory: twice the state for Zigzag, three times for Ping-Pong
  [Cao et al. 2011 Abstract, Table 1]. CALC captures a consistent checkpoint asynchronously,
  without a physical point of consistency and without latency spikes, keeping a second copy only
  of records updated during the capture; it rules out full multi-versioning as too costly in
  memory [Ren et al. 2016 §1–2].
- **Command logging.** Logging commands and replaying them from a consistent checkpoint gave
  1.5× the throughput of main-memory ARIES on TPC-C, at the cost of slower recovery
  [Malviya et al. 2014, *abstract*]. focal's log already carries the frames (commands), so
  checkpoints can be rarer and cheaper without losing anything.

### 4.3 Memory beyond the working set

- **Log-structured memory** holds 80–90% utilization where general allocators fragment
  [Rumble et al. 2014, *abstract*].
- **Spilling beyond memory without losing in-memory speed.** LeanStore keeps in-memory speed when
  data fits and spills beyond it without a translating buffer pool [Leis et al. 2018, *abstract*].
  FASTER's HybridLog does the same for a hash store.
- Both matter for 100k+ open claims in a 128–256 MB envelope: the hot set stays in place, the
  cold tail is compact or on disk.

### 4.4 Tails on few cores are utilization and scheduling

- **Tails come from more than queueing.** With a 50 µs request and Poisson arrivals, p99 grows
  10× between 50% and 95% utilization. On a single core, background processes and scheduling
  made real servers' p99 10–1000× their ideal. A naïve single-core memcached ran at a 100 µs
  median and 5 ms p99.9, against 11 µs and 32 µs once each cause was removed on four cores. FIFO
  gives the best tail [Li et al. 2014 §2, §4.1].
- **Thread-per-core** cut a key-value store's tail latency by up to 71% against memcached
  [Enberg et al. 2019].
- For focal this makes CPU per write the latency lever: 1,400 µs a write at 1,000/s is a
  saturated core, and every thread a write crosses on a 1-CPU node is a scheduling delay.

### 4.5 Replication

- **Batching is the first lever.** NATS batches proposals per append (§3). Raft over eRPC
  replicates in 5.5 µs, and one core serves 10M small RPCs a second
  [Kalia et al. 2019, *abstract*]. Compartmentalized MultiPaxos gains 6× on writes
  [Whittaker et al. 2021, *abstract*].
- **One round trip.** CURP replicates commutative operations to witnesses in parallel with the
  master, completing in one round trip. In RAMCloud that took writes from 13.8 to 7.3 µs and gave
  4× the throughput; non-commutative operations take two round trips
  [Park and Ousterhout 2019 §1–3]. focal's claims on distinct ids commute, except that every
  write rewrites the shared Meta and Epochs rows. Those shared rows also serialize the page
  rebuilds today (allocations note §2.6.2 site 4).

### 4.6 Transport

- **QUIC's CPU.** QUIC's server CPU was 3.5× TLS/TCP's, cut to about 2× by optimizing its three
  costs: cryptography, sending and receiving UDP packets, and maintaining connection state
  [Langley et al. 2017 §6.7]. On fast links its receive side costs up to 45% of throughput
  against TCP [Zhang et al. 2024, *abstract*].
- **Batched sends.** `sendmmsg` and UDP segmentation offload (GSO) send many datagrams per
  system call and per traversal of the stack [Cloudflare 2019].
- **Small-message tails.** Homa's Linux implementation cut p99 for short messages 7–83× against
  TCP under load [Ousterhout 2021, *abstract*].
- focal spends about 12 µs of kernel time a datagram and about 18 datagrams per write at the
  leader. Fewer, fuller datagrams per write is the lever, not cheaper crypto.

## 5. Measurements made for this note

**Interference.** focal's own isolated harness (`measure_time_per_authored_claim`, macOS,
20,000 claims) with a cache-thrashing write of 32 MiB between claims, standing in for the other
threads sharing a 1-CPU node:

| Leaves | prepare + publish | with 32 MiB between claims |
|---|---|---|
| 16 KiB (macOS page) | 48.8 + 5.1 µs | 70.6 + 5.6 µs (+41%) |
| 4 KiB (Linux page) | 37.5 + 4.2 µs | 53.8 + 4.3 µs (+39%) |

**Structures** (scratch `structbench`, macOS). Store work only, per claim: 24 reads of 6 distinct
rows, 18 row writes with focal's payload sizes (3,479 bytes of payload), nine index rows under
nine key ranges, Meta and Epochs rewritten. 20,000 writes measured after the preload:

| Structure | 100k claims: mean / p99 | +32 MiB interference | 1M claims, +32 MiB | allocations / bytes per claim | live per claim |
|---|---|---|---|---|---|
| Copy-on-write pages, 4 KiB (today's shape) | 20.7 / 28.7 µs | 36.0 / 72.9 µs | 43.8 µs | 137 / 105,933 | 7,740 B |
| Copy-on-write pages, 16 KiB | 31.7 / 46.6 µs | 48.2 / 78.8 µs | — | 203 / 310,782 | 7,335 B |
| One B-tree, 112-byte keys, in place | 8.1 / 12.0 µs | 17.9 / 33.5 µs | 24.4 µs | 11 / 9,477 | 6,850 B |
| Persistent shared (imbl), snapshots held 25% of the time | 11.8 / 21.3 µs | 25.4 / 64.3 µs | — | 22 / 20,575 | 7,242 B |
| **Hash for objects + typed ordered indexes, in place, undo kept 32 deep** | **2.7 / 4.5 µs** | **9.9 / 22.0 µs** | **12.1 µs** | **10 / 5,946** | **4,152 B** |

The in-place hash design does the store's work at a seventh of the page design's cost without
interference and under a third with it, and asks for 18–52× fewer bytes per claim. Its live
memory is the payload plus 500–700 bytes a claim, against about 4,200 bytes of overhead for pages.

The hash design's single worst write was 1,968 µs: the table rehashing at a power of two. Sized
to its bound up front, its worst write was 41 µs.

The page emulation here is cheaper than focal's real store, which also path-copies its directory
and derives affinities per comparison. So these ratios understate the gain.

## 6. What focal should use

### 6.1 The session's state: per-access-pattern structures, mutated in place by the owner

| Families | Structure |
|---|---|
| Objects (claims, declarations, evaluations, artifacts, receipts, cycles) | A hash table from the object's id to one record holding all of its primary rows: one probe where five or six keyed lookups are made now |
| Identity rows (content hash → id) | Hash tables |
| Secondary indexes that are listed in order (by issuer, subject, status, action, scope, relation, evaluator, producer, verdict) | Ordered sets of compact fixed-width keys (24–48 bytes, not 112), B+-tree nodes updated in place |
| Monotonic families (events by sequence, by-created) | Append-only rings: push at the end, find a position by binary search, retire from the front |
| Due timers | An ordered set by deadline (or a timing wheel, if measurement asks for O(1)) |
| Meta, epochs, seals | Fields in place |

- **Sizing.** Every table is sized to the session's bound when the bound is set, or grows
  incrementally, so no write ever rehashes a table (§5).
- **Candidates.** A candidate applies in place and records before-images in an undo log. Commit
  discards the log; a refusal or a lost leadership applies it in reverse [HyPer]. The candidate
  pipeline (tail validation, prefixes) keeps its meaning, without a copy.
- **Reads.** Reads run on the owner, the only writer. Long listings are bounded per call (they
  already are: `max_query_items`, continuation tokens). No locks, no `Arc`.
- **Determinism.** Hash tables serve point access only. Anything iterated into a replicated
  result, a record or a checkpoint is iterated from an ordered structure, or sorted at the point
  it is encoded.

### 6.2 Checkpoints: capture-scoped copies, not per-write copies

A capture runs for a bounded period. During it, the first write to a record or index node saves
its before-image to the capture [Cao et al.; CALC]. The encoding then reads the live state plus
those images. Two ways to run the encoding, to be decided by measurement on these shapes:
- **On the owner, in bounded slices between writes.** No sharing, no `Arc`; the owner's CPU per
  checkpoint grows with the state but is spread in slices.
- **From a structurally shared snapshot handed to a thread.** imbl-style, O(1) to take; writes
  cost about 4× the in-place structure (§5) and the capture costs the owner nothing.

Either way, with command logging (§4.2) a checkpoint is needed only to bound replay and the
log's size, not per write.

### 6.3 Followers

A follower replays the same frames through the same in-place store, so its store cost falls as
the leader's does. What it re-verifies stays what integrity requires. Re-deriving what the
leader's record already carries is the part to measure and remove.

### 6.4 The write path around the state

- **Shared rows off the write path.** Meta and Epochs are rewritten by every write. They become
  per-batch fields, so writes on distinct claims commute: the precondition for batching
  everywhere and for a CURP-style one-round-trip path later.
- **Fewer, fuller datagrams per write.**
  - Replies coalesced per connection per drain, as proposals already are.
  - GSO for multi-datagram sends; datagrams filled to the path's MTU.
  - Fewer acknowledgement-only packets.
  - A smaller record: the 2.9 KB claim's composition is to be measured; content already named by
    hash need not travel again.
- **Fewer threads on a small node.** On a 1-CPU node the write path runs to completion on one
  thread (receive, admit, apply, append, send) in FIFO order. The WAL flush is an asynchronous
  completion, not a handoff to another thread. Checkpoint slices run in idle time.
- **Hash once.** Each input is hashed with BLAKE3 once, where its identity is taken; every later
  check uses that digest.

### 6.5 The budget

For 1,000/s at the floor and 30,000/s at a p99 of a few milliseconds on a CI-class core
(§4.4: utilization is the tail), the leader's CPU per write must fall from about 1,400 µs to
under 35 µs:

| Part | Today (leader) | Target |
|---|---|---|
| Store (state structure) | ~200–300 µs | ≤ 5 µs |
| Validation and logic, hashing included | ~120 µs | ≤ 10 µs |
| Engine (drain, progress, delivery), per batch | ~250 µs | ≤ 3 µs amortized |
| Consensus and WAL, per batch | ~100 µs | ≤ 3 µs amortized |
| Network (bytes and datagrams) | ~590 µs | ≤ 15 µs amortized |

At 35 µs a write, 1,000/s is 3.5% of a core and the p99 is the floor; 28,000/s is a full core.
Beyond that, rung 5 of the ladder: one owner per core (doc 27, the 100k bounds).

## 7. What this note does not settle

- **The checkpoint encoding's thread** (§6.2), by measurement on focal's shapes.
- **The new checkpoint and record formats.** A store of typed structures encodes per family; the
  checkpoint format changes under a new version.
- **The 2.9 KB record.** What it carries, and how much of it is needed.
- **Where the cluster's owner loses the rest.** The cluster's owner costs about 3× the isolated
  harness on the same CI CPU, and 32 MiB of interference explains only about 40% of it locally.
  Profile the 1-CPU node's cache misses and SMT siblings on CI before attributing it.

## 8. The order to build it in (the optimization ladder)

1. **Need it at all:**
   - copies on every write (6.1);
   - shared rows rewritten by every write (6.4);
   - repeated hashing (6.4).
2. **Need this much:**
   - 112-byte keys;
   - deep copies;
   - datagrams and acknowledgements per write.
3. **Elsewhere or later:**
   - capture-scoped checkpoints (6.2);
   - replies and appends per drain.
4. **Fewer resources:**
   - one thread on a small node;
   - GSO;
   - presized tables.
5. **Spread out:** owners per core.

Each step is measured before and after:
- the isolated harness;
- `structbench`'s shapes, moved into focal-memory's benches;
- the CI comparison's A/B (one runner, `compare-ab.yml`).

## Sources

- [FASTER] Chandramouli et al., *FASTER: An Embedded Concurrent Key-Value Store for State
  Management*, PVLDB 11(12), 2018 (demo); SIGMOD 2018 (full paper).
  https://microsoft.com/en-us/research/uploads/prod/2018/08/p809-chandramouli.pdf
- [Neumann et al. 2015] *Fast Serializable Multi-Version Concurrency Control for Main-Memory
  Database Systems*, SIGMOD 2015. https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
- [Alvarez et al. 2015] *A Comparison of Adaptive Radix Trees and Hash Tables*, ICDE 2015.
  https://users.cs.utah.edu/~pandey/courses/cs6530/fall24/papers/trees/alverez-icde2015.pdf
- [Wang et al. 2018] *Building a Bw-Tree Takes More Than Just Buzz Words*, SIGMOD 2018.
  https://15721.courses.cs.cmu.edu/spring2019/papers/07-oltpindexes1/mod342-wangA.pdf
- [Cao et al. 2011] *Fast Checkpoint Recovery Algorithms for Frequently Consistent Applications*,
  SIGMOD 2011. https://faculty.cc.gatech.edu/~jarulraj/courses/4420-s19/papers/13-checkpointing/p265-cao.pdf
- [Ren et al. 2016] *Low-Overhead Asynchronous Checkpointing in Main-Memory Database Systems*
  (CALC), SIGMOD 2016. https://par.nsf.gov/servlets/purl/10156629
- [Malviya et al. 2014] *Rethinking Main Memory OLTP Recovery*, ICDE 2014.
  https://dspace.mit.edu/handle/1721.1/75716
- [Rumble et al. 2014] *Log-structured Memory for DRAM-based Storage*, FAST 2014.
  https://www.usenix.org/node/179822
- [Leis et al. 2018] *LeanStore: In-Memory Data Management Beyond Main Memory*, ICDE 2018.
  https://db.in.tum.de/~leis/papers/leanstore.pdf
- [Li et al. 2014] *Tales of the Tail: Hardware, OS, and Application-level Sources of Tail
  Latency*, SoCC 2014. https://www.comp.nus.edu.sg/~lijl/papers/latency-socc14.pdf
- [Enberg et al. 2019] *The Impact of Thread-Per-Core Architecture on Application Tail Latency*,
  ANCS 2019. https://penberg.org/papers/tpc-ancs19.pdf
- [Park and Ousterhout 2019] *Exploiting Commutativity For Practical Fast Replication*, NSDI 2019.
  https://arxiv.org/pdf/1710.09921
- [Kalia et al. 2019] *Datacenter RPCs can be General and Fast*, NSDI 2019.
  https://www.usenix.org/conference/nsdi19/presentation/kalia
- [Whittaker et al. 2021] *Scaling Replicated State Machines with Compartmentalization*, VLDB 2021.
  https://vldb.org/pvldb/vol14/p2203-whittaker.pdf
- [Langley et al. 2017] *The QUIC Transport Protocol: Design and Internet-Scale Deployment*,
  SIGCOMM 2017. https://networks.cs.northwestern.edu/CS497-s22/papers/sigcomm17-quic.pdf
- [Zhang et al. 2024] *QUIC is not Quick Enough over Fast Internet*, WWW 2024.
  https://arxiv.org/html/2310.09423v2
- [Ousterhout 2021] *A Linux Kernel Implementation of the Homa Transport Protocol*, USENIX ATC
  2021; Montazeri et al., *Homa*, SIGCOMM 2018. https://arxiv.org/pdf/1803.09615
- [Cloudflare 2019] *Accelerating UDP packet transmission for QUIC*.
  https://blog.cloudflare.com/accelerating-udp-packet-transmission-for-quic/
- [Vanlightly 2023] *Kafka vs Redpanda Performance — Do the claims add up?*
  https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up
- nats-server v2.12.15: `server/raft.go` (`runAsLeader`), `server/filestore.go` (sync always).
  https://github.com/nats-io/nats-server
