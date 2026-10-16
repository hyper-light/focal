# 32 — The event bus: in memory, durable off the delivery path

Status: 2026-10-15. The owner ordered the rewrite ("We're re-writing this… a proper in-memory event
bus with disk durability… bench it at each step and ensure it hits our 10x requirement… dense
JSON"). Steps 0 and 1 are built and measured here; steps 2 and 3 are next. This supersedes
[31](31-message-ledger.md) as the implementation plan; its decisions that carry over are named
below.

## 1. What the ledger is for

The owner's statement of scope (2026-10-15): the ledger's only concerns are that a **claim reaches
its correct respondent**, a **testament reaches its correct recipient**, and a **testament's
artifacts can be retrieved correctly and quickly**. Artifacts are arbitrary bytes of any size
(audio, video, text), kept by an independent system built for them; a testament carries references.

So the bus carries small messages and a lifecycle is two publishes and two acknowledgements (a
claim to a respondent's stream, acknowledged; a testament to the recipient's stream, acknowledged),
where the old design took eight writes and seven reads.

### 1.1 Artifacts are references

A testament carries `{BLAKE3 root, length, media type, key if encrypted, location hints}`:

- The root says what the bytes are, so the recipient checks them whatever served them (a replica,
  another region, a cache, object storage); BLAKE3's tree lets a stream or a range read be checked
  chunk by chunk (Bao verified streaming). The bus vouches for the reference, the reference for the
  bytes.
- Hints say where to look first and change without the testament changing.
- **Data before pointer**: a testament commits only after the artifact store confirms the artifact
  durable.
- The bus bounds a message's size; anything larger is an artifact.

Precedents: the Claim Check pattern (Hohpe and Woolf); Kafka's and NATS's 1 MB default message
bounds; NATS's object store, which stores every 128 KiB chunk of an object as a stream message (what
this avoids); GFS's separation of the data flow from the control flow (SOSP 2003); Haystack and f4
(OSDI 2010, 2014); Ambry (SIGMOD 2016).

## 2. Decisions

| # | Decision | Grounds |
|---|---|---|
| D1 | Durability is off the delivery path; replication across failure domains is on it only as far as a stream's class promises | Kafka writes to the page cache and relies on replication with recovery (Vanlightly 2023) and still needed KIP-966; JetStream's two-minute fsync lost 131,418 of 930,005 acknowledged writes in Jepsen's power cut (2.12.1, 2025) and bit flips on a minority lost 679,153 of 1,367,069; Redpanda's argument that plain Raft turns one node's unsynced loss into global loss |
| D2 | Three classes a stream: **ephemeral** (at most once; memory, a bounded replay ring, explicit gap notices), **replicated** (at least once; a memory quorum while healthy, a disk quorum once a failure is suspected), **persisted** (at least once; acknowledged after a quorum's fsync) | SAUCR (OSDI 2018: 0–9% over memory-only), doc 31's situation-aware decision; VR Revisited's recovery (a restarted member does not vote or serve until it recovers from a quorum); protocol-aware recovery (FAST 2018: a corrupt record is repaired from peers, never truncated) |
| D3 | At least once plus idempotence: producer sequences, receivers' ids | Kafka KIP-98; exactly-once delivery is impossible on a lossy network, an exactly-once effect is not |
| D4 | The replicated log is the store: a batch's bytes are written once and the same bytes are replicated, flushed and delivered; only small state is checkpointed | Kafka (NetDB 2011); the old focal's profile spent 56% of its owner thread decoding and inspecting frames, the message re-encoded into rows, checkpoints, a payload log and a cold stream |
| D5 | One log a part, the part's streams interleaved in it; segments from a demand-sized warm pool; a segment goes back once none of its batches is held, out of order | RAMCloud's log-structured memory (FAST 2014: 80–90% utilization; its per-segment live counts); BookKeeper's interleaved entry log |
| D6 | A stream keeps no index: its batches chain through the log (`next`, stamped by the owner) | Measured in step 1 (§4.2): a block index cost 1 to 5.3 times an event's payload with sparse streams |
| D7 | A work queue's state is a bounded window: an ack floor and a byte and a lease a claim in flight | KIP-932's share-partition window; Redis's per-message PEL entries and JetStream's map entries as the cost avoided |
| D8 | Thread per core, one writer per part; idle: run to completion, no handoff; contended: drain what arrived, one append each, one flush | Seastar/Redpanda; the Disruptor's single-writer principle; Aether (VLDB 2010) and flat combining; ZygOS (SOSP 2017) for the tail risk of per-core queues under skew |
| D9 | Binary frames; payloads compressed and decompressed by claimants and respondents, never by the bus; zstd with a per-stream trained dictionary by default, a schema-driven encoding where a stream declares a schema | Measured in step 0 (§4.1); Viotti and Kinderkhedia (2022): compressed JSON beats CBOR and MessagePack, compressed schema-driven encodings beat compressed JSON in every case; QPACK's interning for subjects |
| D10 | Built on the shared crates at their latest (hyper-rt, hyper-transport over hyper-quic, hyper-durable, hyper-log, hyper-seal, hyper-liveness, hyper-measure) | The owner's direction, 2026-10-15; all re-vendored at hyper-raft `3d86b42` |

## 3. The batch

One producer's burst of records to one stream: the unit appended, replicated, flushed and
delivered (`crates/focal-bus/src/batch.rs`). A 56-byte header: length, a CRC-32C over everything the
producer wrote, the base offset and `next` the owner stamps outside the CRC (as Kafka keeps
`baseOffset` and `partitionLeaderEpoch` outside its batch CRC), stream, producer, sequence, count,
flags and a timestamp; then each record's payload length and time delta as LEB128 and its payload,
opaque. The receiver of a delivered batch checks the producer's own CRC, so a fault anywhere between
is caught end to end.

## 4. Measured

Both benches print tables: `cargo bench -p focal-bus --bench codec` and `--bench inmemory`. Apple
silicon laptop, shared with other work (load in each row's run), release build.

### 4.1 Step 0: the edge codec on dense JSON

`focal_bus::corpus` writes progress events, claims, testaments and an incompressible blob shape, a
function of its seed. A dictionary is trained on one seed and every codec measured on another
seed's 4,000 messages, each round-tripped; 7 passes, load 6.8; ns are a message's medians.

| shape | raw B | zstd 3 B (ratio) | zstd 3 + 64 KiB dict B (ratio) | compress / decompress ns | receiver's JSON parse ns |
|---|---|---|---|---|---|
| event | 495 | 361 (1.37) | 151 (3.29) | 1,392 / 595 | 1,391 |
| claim | 3,499 | 1,873 (1.87) | 1,149 (3.04) | 11,714 / 3,327 | 8,361 (218 allocations) |
| testament | 5,201 | 2,291 (2.27) | 1,486 (3.50) | 14,069 / 5,820 | 9,353 (239 allocations) |
| entropy | 4,086 | 3,128 (1.31) | 3,077 (1.33) | 6,953 / 3,212 | 1,159 |

- A dictionary is worth 1.5 to 2.4 times plain zstd; 64 KiB beats 16 KiB and 112 KiB adds nothing.
- Level 9 buys under 4% for four to seven times the CPU.
- Sixteen messages to a frame add little beyond a dictionary (claims 3.04 → 3.14), so a message is
  its own frame and any subscriber takes it alone.
- Compressing and decompressing allocate nothing a message once contexts are kept.
- Parsing JSON costs a receiver more than decompressing it: the bus must never parse a payload, and
  what it reads belongs in its binary header.

### 4.2 Step 1: the in-memory core

Each shape's messages compressed as above, one record a batch. **Quiet**: one thread appends,
delivers (reads the stored batch where the chain leads) and acknowledges once 10,000 later batches
have followed; per message after a warm-up pass, 5 passes, 100,000 messages a pass, load 17.3:

| shape | streams | segment | ns | allocations | page faults | payload B | held B a message in flight |
|---|---|---|---|---|---|---|---|
| event | 1 | 1 MiB | 57 | 0 | 0 | 210 | 419 |
| event | 10,000 | 64 KiB | 82 | 0 | 0 | 210 | 342 |
| event | 1,000,000 | 1 MiB | 109 | 0 | 0 | 210 | 898 |
| claim | 10,000 | 1 MiB | 204 | 0 | 0 | 1,209 | 1,483 |
| claim | 1,000,000 | 1 MiB | 215 | 0 | 0 | 1,209 | 1,842 |
| testament | 10,000 | 1 MiB | 235 | 0 | 0 | 1,545 | 1,797 |
| entropy | 1,000,000 | 1 MiB | 493 | 0 | 0 | 3,137 | 3,729 |

"Held" counts the segments in use and warm and the stream table, divided by the batches in flight;
at a million streams it is mostly the streams' own forty bytes and map entries amortized over
10,000 batches. The first version kept a 2 KiB index block a stream: 906 and 1,420 bytes held for
an event at 10,000 and a million streams, and the chain (D6) replaced it.

**Contended**, one owner thread fed by producer threads over hyper-rt's bounded ring (claims, 10,000
streams, closed loop): 2.7 M messages a second from one producer, 2.3 M from two, 1.85 M from four,
the owner's time a message rising from 342 to 540 ns as producers share the ring and the cache lines
it hands over. **Open loop** (one producer, Poisson arrivals, hand to append): p50 4.8 µs at 400,000 a
second to 10.6 µs at 1,000 a second, the difference a parked owner's wake; at load 7.8 the p99 was 40
to 48 µs at every rate, at load 17 it reached milliseconds where the host descheduled the owner.

What these say for step 2: the core's own work is 57 to 493 ns a message with no allocation and no
fault, so a node's cost is its transport and its handoffs. A connection served on the shard that owns
its stream's part hands nothing over; one that is not hands batches over in runs, never one at a
time.

### 4.3 Step 2: one node over QUIC

`focal start bus` runs one node in its standalone mode (`crates/focal-bus/src/serve.rs`): its part,
its endpoint on hyper-transport over hyper-rt's batched UDP socket, on one shard of the thread that
starts it, run to completion (D8). A publish is a lane frame carrying one batch, appended once and
delivered to each subscriber by its own cursor; a delivery is the stored batch, its producer's CRC
checked by the transport as the frame's own (each byte summed once, the KIP-98 batch CRC end to
end) and again by the client. Lanes are lossless: a subscriber whose lane is full holds the
publishers feeding its stream at their own lanes, so no queue grows past what the path carries,
and nothing a live subscriber is owed is let go.

**Nothing is configured, and no number is the bus's own choice.** Each bound is the protocol's, the
transport's, or cited (`crates/focal-bus/src/limits.rs`, `wire.rs`):

| Bound | Value | Ground |
|---|---|---|
| Largest batch | 1 MiB | nats-server's `MAX_PAYLOAD_SIZE`; Kafka's `message.max.bytes`, the largest record batch |
| Segment | five of the largest batches | a segment's tail wastes at most a fifth, jemalloc's rounding bound ("approximately 20%", jemalloc(3)) |
| Control message | 24 bytes | the largest of a subscribe's head and its reply's |
| Datagrams a system call | 64 | Linux's `NAPI_POLL_WEIGHT`, the packets one device poll hands up |
| Lane window | 64 datagrams of 1,200 bytes of the smallest frames | RFC 9000 §14's least datagram |
| Receive window ceiling | the transport's stream ceiling a stream a peer sends on | hyper-transport's `stream_window_ceiling`, auto-tuned below it |
| Idle timeout | 2 minutes | RFC 4787 REQ-5, the least a NAT keeps a UDP mapping |
| Keep-alive | 15 seconds | RFC 8085 §3.5's shortest interval |
| Connections a client | 2 | the one it is served on and the one replacing it after a reconnect |
| Node memory | three quarters of the process's limit | .NET's GC share of a container's limit (focal's node envelope) |
| Warm segments and buffers | what the last periods of demand needed | Bonwick and Adams's working set, delimited by demand's troughs |
| Streams, subscriptions | the rows the node's memory holds | each row charged to the budget as it is taken |

**Memory is charged as held, and the window is what memory holds.** A node keeps the batches its
memory holds for subscribers that start behind; when a batch, or a frame the transport waits to
take, needs room the memory has not, the oldest batches retained give it theirs (warm segments
first), and a subscriber that falls below what is kept is told the offsets it missed. Nothing is
reserved for a worst case: the transport's buffers, the tables, the index of retained batches and
the segments are all charged when they are held, to one budget.

Found on the way:
- a log's tail emptied while it took batches was never given back once the log moved past it, a
  segment held for good at each such rollover (fixed: the log gives it back as it takes the next);
- a delivery the node found no memory for took its subscriber as gone: the first container matrix
  (a busy laptop's Docker, 2026-10-16) lost 1.1 million messages at 100,000 a second, the
  subscriber let go in silence while it waited. A refusal for memory is the window's to answer:
  the oldest retained batches give their room and the delivery is asked again, and where nothing
  can be given the subscriber waits, starved, until the transport gives memory back
  (`Node::starving`; `a_delivery_the_node_has_no_memory_for_waits_and_its_subscriber_stays`);
- a third of a node's datagrams were QUIC acknowledgements, one for every other packet (RFC 9000
  §13.2.2's default): every connection now asks for Google QUIC's decimation, an ACK for every 10
  packets or a quarter of the round trip (hyper-quic patch Q22, hyper-raft `a8f5941`).

**Measured** on a bare-metal machine (AMD Ryzen 9 9950X3D2, Linux 7.2), each system's node alone
on one core in its container, the generator on four others, the claim corpus's 1,150-byte
messages, 20 s after 5 s of warm-up; CPU is the node's container's, user and kernel, a message
received (2026-10-16, one run each):

| 10,000 a second | p50 | p99 | p99.9 | Lost | CPU a message |
|---|---|---|---|---|---|
| Bus | 24 µs | 36 µs | 95 µs | 0 | 10.3 µs |
| NATS | 14 µs | 24 µs | 677 µs | 1 | 6.0 µs |
| NATS over TLS | 21 µs | 34 µs | 714 µs | 1 | 7.9 µs |

| 100,000 a second | p50 | p99 | p99.9 | Lost | CPU a message |
|---|---|---|---|---|---|
| Bus | 28 µs | 297 µs | 1.29 ms | 0 | 6.5 µs |
| NATS | 15 µs | 48 µs | 0.98 ms | 1 | 4.8 µs |
| NATS over TLS | 21 µs | 709 µs | 2.79 ms | 2 | 5.6 µs |
| Redis | 19 µs | 504 µs | 4.14 ms | 0 | 4.2 µs |

| 1,000,000 offered | Delivered a second | p99 | p99.9 | Lost |
|---|---|---|---|---|
| Bus | 766,000 | 6.2 ms | 6.7 ms | 0 |
| NATS | 895,000 | 4.3 ms | 11.4 ms | 67 |
| NATS over TLS | 399,000 | 250 ms | 266 ms | 2,463,006 |
| Redis | 390,000 | 29 ms | 46 ms | 12,202,622 |
| Kafka | 344,000 | 4.6 s | 4.7 s | 0 |

Every system's kernel cost a message is about the same (4.2 to 4.4 µs at 100,000 a second); the
bus's user time is what it pays above NATS (2.3 µs against 0.6). Decimated acknowledgements, three
interleaved pairs at each rate: at 100,000 a second the node handles 2.7 datagrams a message
instead of 3.4 and its CPU falls 6 % (6.10 µs against 6.50); at 10,000 a second it falls 27 %
(7.27 µs against 9.98), and on a core a busy loop shares, 25 % (7.89 against 10.55), its p99 there
0.04 ms against 0.05 and its p99.9 0.11 ms against 0.14.

Where the node's time goes (perf on the same machine, hardware counters, kernel symbols placed
from the kernel's `System.map`): at 100,000 a second, 61 % in the kernel, a fifth of that in the
container bridge's modules every system pays, and the epoll re-arm each wait makes; at saturation
(1.33 µs a message, 909 messages a batch), AES-GCM 22 %, QUIC's packet bookkeeping about 25 %, the
kernel 21 % (7 % clearing pages faulted afresh), five user copies of each message 16.5 % and the
CRC-32C pass 8.6 %.

Open: a subscriber of many streams waits a round trip a stream (subscribes as lane frames would
not); the retained batches' index costs 16 bytes a batch beside the log, which already holds what
it says (a scan of the log from its oldest segment would replace it). From the profile: a segment
freed and faulted afresh where the window could reuse it; a one-shot epoll registration re-armed
at every wait; five user copies of each message where two would carry it; CRC-32C at about half a
cycle a byte; receive headers rebuilt at every call; kernel receive stamps the transport does not
read. The busy-machine matrix of every system on the bare-metal machine follows these.

Open, from that matrix's first run (2026-10-16): at 1,000,000 offered on a core a busy loop shares,
the node told its subscriber one gap of two batches (1,819 messages) in 9,487: the window gave up
batches its subscriber had not yet taken, where memory was short for a delivery. Every bound around
a live subscriber (a lossless lane holds one frame beyond what QUIC took; a held publisher's lane is
not read) keeps it within megabytes of the head, so a window of 1.5 GB should not reach it; it is
being traced with the window, the pool and the budget logged at each gap.

## 5. Steps

| Step | What | Bar |
|---|---|---|
| 0 | Dense JSON corpus and edge codec | done (§4.1) |
| 1 | In-memory core: batch format, part log, segment pool, streams, ack window | done (§4.2); 0 allocations and faults a message |
| 2 | One node over QUIC (hyper-rt-transport), ephemeral class, in a 1-CPU, 2 GB container, against NATS core and Redis Pub/Sub | p99 and p99.9 delivery latency at most a tenth of the best competitor's at the same offered load; no collapse as load rises |
| 3 | Three nodes, replicated class on hyper-durable against Kafka acks=all and JetStream R3, then persisted against their synchronous settings | the same bar, class against class |

Open after step 1: a header smaller than 56 bytes for one-record batches (27% of a 210-byte event);
the stream table's keyed hash (SipHash) against an id the directory assigns; a compacting cleaner
should long-held batches fragment segments (RAMCloud's two-level cleaning), measured before built.
