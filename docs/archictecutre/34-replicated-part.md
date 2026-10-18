# 34 — Three nodes: a part replicated over hyper-durable

Status: 2026-10-18. The design of doc 32's step 3 as doc 33 refines it (its steps 3b, 3c and 3g),
and what was built of it: steps 3b.1 to 3b.4 and 3c are in (§8); the comparison (3b.5) ran once
and found records left unacknowledged at 10,000 a second and a tail of 1.4 s at 1,000, both open.
Doc 33 says what each class commits on; this says how a part becomes a group of three
replicas, and what every publish, acknowledgement and delivery does on the way.

## 1. What it builds

- **Three nodes, each a replica of each part.** A part is one Raft group: hyper-durable's
  `Replica` over hyper-log, the part's batches the group's entries (doc 32 D4).
- **The persisted class first.** A publish is acknowledged once a majority's disks hold its batch:
  the classic commit, the leader counting its own disk. The replicated class (doc 33's step 3c) is the
  same group with the members' fates stated, R-8's held commits (hyper-raft `docs/raft.md` §3.7):
  nothing below the commit rule changes for it.
- **Elections by suspicion.** One heartbeat stream per node pair judges each peer node for every
  group the pair shares (doc 33 §4.1, step 3g), as hyper-durable-e2e's members elect: no ticks, and
  no group's own heartbeats while it is idle.

## 2. How Kafka, JetStream and Redis do it

- **Kafka.** A partition's leader takes every produce request. A broker that does not lead answers
  `NOT_LEADER_OR_FOLLOWER` (error 6), and since 3.7 says who leads and in which epoch (KIP-951,
  "Leader discovery optimizations for the client"); a client takes the new leader only where its
  epoch advances what the client knew. Followers fetch from the leader; `acks=all` answers once the
  in-sync replicas hold the batch; consumers fetch from the leader, or the nearest replica
  (KIP-392). The log on disk is the store, and `flush.messages=1` fsyncs each.
- **JetStream.** Each stream is an R3 Raft group: "one of the three copies takes every write ...
  first, then sends it to the other replicas, and the write is acknowledged only once a majority
  hold it" (NATS docs, JetStream clustering). A publish reaches the leader through core NATS's
  interest routing, a hop through the server the client is connected to. Each replica holds the
  write in its file store, fsynced every `sync_interval` (two minutes by default; Jepsen's power cut
  of 2.12.1 lost 131,418 of 930,005 acknowledged writes, doc 32 D1) or each time with `sync: always`.
- **Redis.** The primary takes every write and replicates asynchronously. `WAITAOF numlocal
  numreplicas timeout` blocks a client until its earlier writes are fsynced to the primary's AOF
  and to that many replicas' (Redis 7.2), and, as WAIT, "does not make Redis a strongly-consistent
  store": a failover may promote a replica that missed them.

## 3. Decisions

| # | Decision | Grounds |
|---|---|---|
| P1 | A part is one Raft group of the nodes that hold it | JetStream's stream group, Kafka's partition; hyper-durable's `Owner` holds an owner thread's groups in one arena |
| P2 | Publishes go to the part's leader. A node that does not lead refuses, naming the leader it knows and that leader's term; a client takes the name only where the term advances its view | Kafka's `NOT_LEADER_OR_FOLLOWER` with KIP-951's leader and epoch, and its epoch rule. JetStream's forwarding costs a hop and the forwarder's buffering on every publish |
| P3 | The group's entries are the batches: the same bytes proposed, replicated, written and applied | doc 32 D4 |
| P4 | The state machine is the part: applying an entry appends its batch to the part's log and streams, which the node delivers from | doc 32 §3; one copy into the segment the delivery reads |
| P5 | A publish is acknowledged by a frame of its own when its leader applies it: stream, base offset, records, and the class achieved | Kafka's produce response (base offset), JetStream's PubAck (stream, sequence); doc 33 §4.8's typed acknowledgement |
| P6 | Idempotent publishes: a batch whose producer and sequence the part already applied is acknowledged with its first offsets and not appended again; each producer's last sequence is kept a part, bounded, and evicted by the oldest use | Kafka's idempotent producer (KIP-98) and its producer-state expiry; doc 32 D3. A client resends what a failover left unacknowledged |
| P7 | Deliveries are made by the leader from its applied part; a follower refuses a subscriber, naming the leader | Kafka's fetch from the leader; reading from a follower (KIP-392) waits for doc 33 §4.7's gate |
| P8 | Raft messages travel between nodes as lane frames on the bus's endpoint, in hyper-raft's own wire records | one endpoint and one congestion state a peer, sealed by QUIC's TLS; the records carry every R-8 field, where focal-consensus's envelope refuses them (doc 33, step 3c) |
| P9 | A node's groups write to one hyper-log log on its data directory | hyper-durable's `GroupStore`: one flush commits every group's writes |
| P10 | A restarted node replays its committed entries into its parts; retention compacts the log, and a part's image is its stream table | D4's small state; Kafka's log is its own recovery |
| P11 | Every byte charged as held, to the node's one budget: the core's entries not yet durable, the store's writes out, the part's segments, the acknowledgements owed | doc 32 §4.3; the owner's no-worst-case law |

## 4. A publish, persisted

1. The client sends a publish frame to the part's leader, on its lane.
2. The leader checks the batch (its CRC, verified by the transport as the frame's own), proposes it
   (`Replica::propose`), and keeps the acknowledgement it owes: publisher, stream, producer and
   sequence. A publisher's owed acknowledgements are bounded by its lane's window: what the lane
   carries unacknowledged is what may be owed.
3. The replica's drive submits the leader's write and sends its appends to the followers.
4. Each follower steps the appends, writes them, and answers once they are durable.
5. The leader commits once a majority's disks hold the entry, its own counted by its persisted index.
6. Applying it, the leader's part appends the batch; the node delivers it to the stream's
   subscribers and sends the publisher its acknowledgement.
7. Each follower applies it too, so its part is current when it leads.

## 5. Failures

- **A leader lost.** The stream suspects it within its stated bound; a follower campaigns, and the
  elected one's clients are redirected (P2) by the first refusal they meet.
- **A publish whose acknowledgement was lost.** Its client sends it again to the new leader; the
  part acknowledges it with the offsets it took, or appends it if no majority took it (P6).
- **A node restarted.** hyper-log replays its group; the part is rebuilt from the committed entries
  (P10), and the node rejoins as a follower.
- **What doc 33 §7's battery checks of 3b**: kill -9 of the leader and of every node, partitions
  of a minority and of the leader, a stalled disk; every acknowledged batch read back by the
  black-box history reader.

## 6. Measured against

| System | Settings | Class |
|---|---|---|
| Kafka 4.1, three brokers | `acks=all`, `min.insync.replicas=2`, `flush.messages=1` | persisted |
| JetStream 2.12, R3 | `sync: always` | persisted |
| Redis 8.2, a primary and two replicas | `appendfsync always`, `WAITAOF 1 1 0` after each write | persisted |

The bar is doc 32's: the bus's p99 and p99.9 at most a tenth of the best competitor's at the same
offered load, and no collapse as the load rises, each system held to the class it states.

## 7. Steps

| Step | What | Shown by |
|---|---|---|
| 3b.1 | The wire: node-to-node Raft frames, acknowledgements and refusals naming the leader | `wire.rs`'s round trips; the kinds' roles |
| 3b.2 | The part as a state machine, idempotent publishes | apply, replay and image tests against the in-memory part |
| 3b.3 | The replicated node: replica, log and endpoint on one shard; elections by the liveness stream | three nodes in one process over loopback: publish, acknowledge, deliver; a leader killed, publishes resent, nothing acknowledged lost |
| 3b.4 | `focal start bus` for a node of a group; the client following the leader | three processes, kill -9 at every persistence point |
| 3b.5 | The comparison of §6 on the bare-metal machine | the bar |

## 8. What was built, and found

| Step | Built | Shown |
|---|---|---|
| 3b.1 | `wire.rs`: a group's messages (hyper-raft's own records, their own class ranked ahead of deliveries, held rather than skipped), heartbeats on a liveness lane of their own, acknowledgements (stream, producer, sequence, offsets, class), redirects naming the leader and its term, a subscribe's reply tagged to name the leader; `RAFT_BOUND` derived from the record layout and the largest batch | round trips; every kind's roles |
| 3b.2 | `group.rs`: the part as the group's state machine; resends recognised by the stream's newest producer's sequence; the image (each stream's floor and its newest batch's producer there, the configuration) as of the newest entry the window let go, CRC-sealed and installed durably; the log compacted to it and no further | apply, resend, image, reopen-and-replay and install tests |
| 3b.3 | `member.rs`: the replica over the node's sealed hyper-log log (facts by `Config::derive`), the liveness stream, publishes proposed at the leader and redirected elsewhere, a parked publish holding its publisher's lane, acknowledgements at apply, a node that stops leading redirecting what it owed, Raft frames authenticated by the transport's peer | `tests/group.rs`: three nodes and a client on one shard; a leader elected; 100 acknowledged in order; delivered in order; the leader killed; 100 more; nothing acknowledged lost; a resend not appended twice |
| 3b.4 | `focal start bus --node --peer --data --key-file`; `focal-compare --system focal-bus --mode append` | three processes: 5,000 acknowledged, none failed |
| 3c | `--fate ID=FATE` (each node, the same at every node), `--spanning` (by default every distinct fate); hyper-raft's `Fates` where the members' fates reach the span; acknowledgements `Replicated`, or `Persisted` where replicas share their fates | `tests/group.rs` for both classes; three processes with fates: 7,000 acknowledged replicated |
| 3b.5 | the comparison on the bare-metal machine (9950X3D2): each system's three nodes in Docker, open loop, 128-byte records, 30 s after 10 s of warm-up; Kafka, NATS and Redis durable per write (`flush=1`; `sync always`; `appendfsync always` with `WAITAOF 1 2`) | below |

The comparison's first run (2026-10-18), p50 / p99 / p99.9 in milliseconds:

| System | 1,000/s | 10,000/s | Unacknowledged at 10,000/s |
|---|---|---|---|
| focal, persisted | 33.2 / 1,404 / 1,674 | 35.4 / 70.2 / 275 | 2,441 |
| focal, replicated (held) | 0.3 / 34.5 / 103 | 2.2 / 143 / 2,322 (7,983/s) | 1,022 |
| Kafka | 35.4 / 172 / 252 | 38.5 / 343 / 538 | 0 |
| Redis | 43.5 / 969 / 1,145 | 1,323 / 3,290 / 3,406 (2,600/s) | 0 |
| NATS | 24,562 / 25,048 / 25,065 (93/s) | 23,236 / 24,059 / 24,075 (98/s) | 0 |

The unacknowledged records and the persisted class's 1.4-second tail at 1,000 a second are defects,
open until root-caused; the replicated class is a held commit, not a write on every replica's disk,
and the rows durable per write are not its comparison.

Found on the way, each root-caused:
- **No leader was ever elected.** Every heartbeat was refused `Unmeasured` and every pair stayed
  `Unconfigured`: the owner never told the liveness stream its waits, the timer granularity hyper-
  liveness builds every estimator from (`Liveness::on_wait`). `Member::waited` tells it, for a wait
  begun before the stream's wake and ended at or past it, before anything the wait brought is fed.
  Found by a state dump of the three nodes, not by reading.
- **The comparison never ended.** A batch whose publish found the leader not yet connected left
  the resend list (only a full lane kept it) and stayed in flight unacknowledged; and while no
  leader was named the client resent every batch each turn. Both fixed in the client; it backs off
  a leaderless redirect as Kafka's client backs off a retriable error (KIP-580).
- **A held commit of three members waits for both followers.** The leader counts its own disk and
  never its memory (doc 33 §4.4a), so a held quorum without the leader's disk is both followers'
  held answers; whenever either follower's durable promise falls behind the stream, the commit
  waits a flush, the leader's or that follower's. Traced on three processes (macOS, debug):
  propose to commit p50 1.2–4 ms, p90 23–46 ms, commit to apply 0. Open: what of it a group of
  five changes (a held quorum is then three of four followers), and the promise's reach under bursts.
- **Resends are recognised by the stream's newest producer only.** A producer's batch sent again
  after another producer's batch on the same stream is appended again: at least once, the class's
  promise. Open: a bounded producer table, as Kafka keeps per partition.
