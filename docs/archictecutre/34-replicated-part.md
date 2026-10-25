# 34 — Three nodes: a part replicated over hyper-durable

Status: 2026-10-18. The design of doc 32's step 3 as doc 33 refines it (its steps 3b, 3c and 3g),
and what was built of it: steps 3b.1 to 3b.4 and 3c are in (§8); the comparison (3b.5) ran once
and found records left unacknowledged at 10,000 a second and a tail of 1.4 s at 1,000: the first is
fixed (every publish answered, P6 as designed), the second is a shared disk's stall electing, open.
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
| P2 | Publishes go to the part's leader. A node that does not lead refuses, naming the leader it knows and that leader's term; a client takes the name only where the term advances its view. A client that cannot hear the node it sends to (its path degrading, its connection closed, a dial to it failed) asks another node of the group; one that names no leader, or names the unheard leader at the term the client knows, is asked again, the next in turn, after a back-off | Kafka's `NOT_LEADER_OR_FOLLOWER` with KIP-951's leader and epoch, and its epoch rule; its producer's metadata refresh from another broker on a disconnect or a request timeout, backed off per KIP-580. JetStream's forwarding costs a hop and the forwarder's buffering on every publish |
| P3 | The group's entries are the batches: the same bytes proposed, replicated, written and applied | doc 32 D4 |
| P4 | The state machine is the part: applying an entry appends its batch to the part's log and streams, which the node delivers from | doc 32 §3; one copy into the segment the delivery reads |
| P5 | A publish is acknowledged by a frame of its own when its leader applies it: stream, base offset, records, and the class achieved | Kafka's produce response (base offset), JetStream's PubAck (stream, sequence); doc 33 §4.8's typed acknowledgement |
| P6 | Idempotent publishes in each session's order: the part keeps, for each stream and each identity that publishes to it, the identity's sessions (the one it publishes on and the one it replaced), each with the sequence its next batch must begin with; a batch its session took already is acknowledged with its first offsets where it was the session's newest and is not appended again, one past a gap or across the next sequence is refused `OUT_OF_ORDER` naming the sequence the session waits for, a new session begins at zero, and a session is judged by the identity the leader states with the entry, so no publisher takes another's by naming it | Kafka's idempotent producer (KIP-98: `DUPLICATE_SEQUENCE_NUMBER`, `OUT_OF_ORDER_SEQUENCE_NUMBER`, a fresh producer id for a new producer) and its producer state kept per partition and snapshotted; doc 32 D3. A client resends what a failover left unacknowledged |
| P7 | Deliveries are made by the leader from its applied part; a follower refuses a subscriber, naming the leader; a node that stops leading ends each subscription it holds with a notice (`Moved`) naming the leader it knows and the offset it would have delivered next, sent as the subscriber's lane makes room, and delivers nothing more | Kafka's fetch from the leader, and its `NOT_LEADER_OR_FOLLOWER` to a fetch at a former leader with KIP-951's leader; reading from a follower (KIP-392) waits for doc 33 §4.7's gate |
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
  elected one's clients are redirected (P2) by the first refusal they meet. A client learns that it
  cannot hear the lost leader from its own endpoint: a killed process sends nothing, and four probe
  timeouts with nothing it sent acknowledged tell it the path is degrading (hyper-transport's
  `Degrading`, hyper-raft docs/transport.md §4ai, quiche's rule), where a Kafka producer has TCP's
  RST from a live host's kernel and otherwise its request timeout (30 s). It then asks another node
  (P2), which names the leader or, while the group has not yet elected one, is asked again.
- **A publish whose acknowledgement was lost.** Its client sends it again to the new leader; the
  part acknowledges it with the offsets it took, or appends it if no majority took it (P6).
- **A node restarted.** hyper-log replays its group; the part is rebuilt from the committed entries
  (P10), and the node rejoins as a follower. Its endpoint keeps the key it resets its last run's
  connections with (hyper-transport's `RestartKey`, from the node's private key and its address), so
  each peer learns at its next send that its connection is gone, as from a restarted host's TCP
  RST, and the pair's lesser node dials it again: the lesser keeps each pair connected, dialing at
  once after a connection that lasted and backing off after dials that gave none (RFC 8085 §3.1.3,
  1 s doubled to 3 s).
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
- **A batch overtaken by the next of its producer's was acknowledged and lost.** 3b.2 kept only each
  stream's newest batch's producer and its next sequence, and took any batch of that producer below
  it for a resend: a batch redirected, or sent to a leader that stopped leading, while the next went
  straight through came after it, was acknowledged as taken and was never appended; and a replica
  that reopened on its image appended again a batch its floor held, where another producer's came
  between, so its offsets could part from its group's. P6 as now built replaces it (`producer`): each
  identity's sessions in each stream, in order, gaps refused; the image keeps the sessions as of the
  newest batch the window let go, fed each batch as it goes in the order it was appended, so a
  replica that reopens replays to the same sessions (version 2).
- **Answers were dropped on a full lane.** At 10,000 a second the leader stepped down for a moment
  (below), redirected the 2,300 publishes it owed at once, and dropped 1,657 of the redirects and 75
  acknowledgements on the publisher's full lane: those 1,732 batches were never answered, the run's
  3,386 failed records, sent once each and neither redirected nor acknowledged (traced on the
  bare-metal machine, every count matching). Every publish is now answered: an answer that finds its
  publisher's lane full waits, in order, its publisher's publishes held until it goes; the client
  sends in its session's order, and sends again what goes unanswered for a serve period.
- **A shared disk's stall elects.** The same run's step-down: each node's log flush took 584 to
  587 ms in the same second, the bare-metal machine's one disk stalling under all three nodes, so
  no node's heartbeat was proved for 0.6 s and each node suspected both others; the leader, left no
  trusted quorum, stepped down, and won the next term. Each detector stood at 165 to 216 ms of
  detection with a mistake expected every 29 to 36 s, charging an election the ballot law's span
  (a round trip and a vote's flush, about 0.1 s) where the step-down cost a second. A stall that
  every member shares no longer elects since focal f8052a5e (hyper-raft 25cf889, the liveness
  stream's judge of its own flush proof): on fedora (i9-14900HX, one Samsung 9100 PRO), every node's
  writes held at once for 0.61 s through its cgroup's `io.max` under 128-byte durable appends at
  1,000 a second, 7a79f0d7 stepped down in 4 of 4 runs and left 282 to 480 publishes
  unacknowledged, f8052a5e in 0 of 3 with every publish acknowledged, the longest in 675 to 689 ms,
  the hold itself (Quiver's survey for CP-T2.2, 2026-10-25, its docs/design/persisted-tail.md). Open,
  in the shared repository's liveness stream: the cost of an election as measured.
- **A group restarted one node at a time lost its leader** (2026-10-25, found by step 5 of the
  at-rest program, 29 §12.9). A node never dialed a pair again once its connection closed (the
  member only cleared the pair's outbox), and a restarted node dialed only the nodes above it; and a
  restarted endpoint could not tell a peer that its last run's connection was gone, its QUIC reset
  key and connection-ID key drawn afresh at every start, so the peer held a dead connection until its
  two-minute idle timeout. Restarting node 1, then 2, then 3 left no pair connected: nodes 1 and 2
  stood for term after term with no vote reaching anyone. Fixed at both causes: the pair's lesser
  node dials whenever its endpoint tells it the pair has no connection (`Event::Closed`,
  `Event::Unreachable`), and a restarted node resets what its last run held (hyper-raft 698c0da,
  its docs/transport.md §4ah). With either fix alone the test's acknowledgements froze for 30 s; with
  both, three restarts under publishing and consuming each served their next 20 batches at the rate
  before any restart (this Mac under load: 60 batches in 6.6 to 18.9 s before, 20 in 2.3 to 6.2 s
  after each).
- **A node that stopped leading kept delivering to its subscribers** (2026-10-25, found by the same
  test, a subscriber following the leader through the restarts). P7 was held when a subscription
  was made, never after: a leader that stepped down kept its subscriptions, and as a follower, which
  applies what its group commits and cannot hold a publisher for a subscriber that takes no more,
  it let a stalled subscriber's offsets go from under it. Under other sessions' load on this Mac, a
  subscriber at node 3 took two batches, node 3 stepped down for a term, its subscriber's lane filled
  three times, and it was told a gap. A node that stops leading now ends every subscription it holds
  with a `Moved` notice (a frame of the deliver class: the stream, the offset it would have delivered
  next, the leader it knows), sent as the subscriber's lane makes room, never skipped, and delivers
  nothing more; the lanes it held for those subscribers go at once. The subscriber subscribes again
  at the leader from that offset, as a Kafka consumer does on a former leader's
  `NOT_LEADER_OR_FOLLOWER`. Test (`tests/loopback.rs`): a subscriber stalled until its lane is full
  and the node holds the publisher, the node told it stopped leading: nothing told while the lane is
  full, the publisher let go at once, then every queued batch in order and the notice naming the
  leader and the offset after the last batch, and nothing delivered after; without the move the
  publisher stays held and the test stops.
- **A client never left a killed leader** (2026-10-25, found by Quiver on fedora: three
  `focal start bus` units, a client publishing 1,000 a second, kill -9 of its leader at 25 s). The
  survivors elected another within about 110 ms of suspecting the first, and the client kept
  publishing into the dead connection to the end of its run, 4,096 publishes unacknowledged. A
  publisher learned leaders only from redirects, which a dead node sends none of, and its only
  judgement of a silent node was QUIC's idle timeout, two minutes (`limits::IDLE_TIMEOUT`, RFC 4787
  REQ-5): QUIC carries nothing from a host whose endpoint is gone. `tests/group.rs` hid it, its
  harness switching the client to a survivor by hand and reading the leader from the nodes for its
  consumer. Fixed at both causes. The transport tells a dialer when its peer stops acknowledging, as
  Chromium's QUIC tells its session (hyper-raft's path-degrading detection, docs/transport.md §4ai:
  four probe timeouts, RFC 1122 §4.2.3.5's "inform the application ... when R1 is reached"). And the
  decision is focal-bus's client's (`client::Client::group`, P2): told it cannot hear the node it
  sends to, it asks the next node of the group at once; a node naming the leader at a newer term is
  followed, and dialed afresh where its connection went (a restarted node resets what its last run
  held); a node naming no leader, or the unheard one at the term the client knows, is asked again,
  the next in turn, after Kafka's back-off (100 ms doubled to 1 s, `limits::ASK_BACKOFF_FIRST`,
  KIP-580, drawn within a fifth of it, one step a round of asking however many batches the node
  redirected in it), the unheard leader dialed beside the asking where it has no connection; a node
  it asked that acknowledges leads; while it asks it sends only its oldest batch. A degrading path is a reason to look elsewhere, not to give the
  connection up: a node just restarted held its loop 169 ms (`busy_most_ns`), past four probe
  timeouts of loopback (about 104 ms), and a client that disconnected its leader on it churned, its
  owed answers and its consumer's subscription dropped with the connection. The leader is followed
  again once heard (`Progressing`), and its connection is given up only when the client follows
  another (named at a newer term, or acknowledging), as a Kafka client disconnects a broker whose
  request timed out. The caller is handed `Delivery::Rerouted` and sends the node what it owes, the
  part recognising what it took (P6), and `Delivery::Lost { node }` for each node whose connection
  ended, told by the epochs it saw up so a close the endpoint folded into a later state is told too,
  on which it subscribes again wherever it held a subscription: a subscription lives on its
  connection, which ends it with no exchange left to refuse. focal-compare's bus modes use the client
  (and its endpoint, `client::endpoint`). Tests: `tests/group.rs`, with no hand-switching and its
  consumer subscribing where the client sends, the leader killed with its socket gone and nothing
  said, every batch after acknowledged once at the offsets that follow and the client sending to the
  new leader (on this Mac: acknowledged again 30 to 178 ms after the kill, fifteen kills), and the three nodes
  restarted one at a time under publishing and consuming, the client and the consumer finding each
  next leader themselves; the harness counts a batch sent again and acknowledged as taken without
  its offsets (not its session's newest, P6) as acknowledged, where it had waited for offsets it
  never gets. `focal-node/tests/bus_failover.rs`: three `focal start bus` processes and the client,
  kill -9 of the leader's process, every batch acknowledged and the stream holding each once and in
  order (on this Mac, four runs: the first acknowledgement 0.45 to 4.5 s after the kill, the group's
  own election 0.3 to 3.6 s of it, each survivor's suspicion, and the client's remainder 0.15 to
  0.9 s). Open: the client waits out the group's election by asking again on Kafka's back-off, so it
  hears the new leader up to a second after the group elected it; a node that held a client's ask
  until its term passed the one the client knew would answer as the election ends (a leadership ask,
  not yet in the protocol). a dial to a node whose host is gone ends only at the idle timeout, the transport
  having no deadline for a handshake (Chromium's `kMaxTimeForCryptoHandshakeSecs`, 10 s; Kafka's
  `socket.connection.setup.timeout.ms`, 10 s doubled to 30 s, KIP-601), so a client whose first
  configured node is down waits two minutes before it asks the next; a subscriber learns of its
  leader's death only from what it sends (window updates while it is delivered, or a keep-alive
  within 15 s, `limits::KEEP_ALIVE`, on a quiet stream) or from the client's following another; and
  a connection's second subscribe to a stream it is subscribed to makes a second subscription, so a
  subscriber must not ask again on a connection whose loss it was not told of.
