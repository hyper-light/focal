# 34 — Three nodes: a part replicated over hyper-durable

Status: 2026-10-18, the design of doc 32's step 3 as doc 33 refines it (its steps 3b and 3g), before
code. Doc 33 says what each class commits on; this says how a part becomes a group of three
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
