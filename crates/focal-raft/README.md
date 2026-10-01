# focal-raft

focal's consensus core: elections and the log as a state machine with no clock, no
disk and no network. It is told what time has passed (`RawNode::tick`) and what arrived
(`RawNode::step`), and it says what to persist, send and apply (`RawNode::ready`). The
durable shell that drives it is `focal-consensus`.

It is Raft as Ongaro's thesis states it, with pre-vote, check-quorum, election priority,
learners, joint consensus, leader transfer, an inflight window with conflict hints,
ReadIndex and snapshots. Reads asked before the member is next asked what there is to do
share one round of heartbeats (`ReadRounds`, 27 §10). What a member is sent ahead of its
answers is bounded in messages and in bytes, the bytes by what its owner says the path to
it carries (`RawNode::set_inflight_bytes`, 27 §11). It keeps the log and speaks the messages of `raft-rs` 0.7
(`raft-proto`), which focal's groups ran on before. What it decides differently, and
why, is in [27 §4.5](../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md).

A group may have the fast track (`Config::fast`, `fast.rs`, `track.rs`; 27 §4.6) — which
is **not safe as built** and is used by no owner: an election in a fast group can commit a
second entry at an index that holds one (27 §4.6). As designed, a
member that does not lead proposes to every voter at once (`RawNode::propose_fast`),
and its entry is committed when three quarters of the voters hold it, or a majority
holds it from the leader, whichever is first.

## Rules

- Nothing unwinds. `Error` says whether an operation was refused and changed nothing,
  whether a peer's message contradicted what the member holds, or whether the member's
  state no longer adds up (`Error::is_fatal`), which alone stops the replica.
- Everything that grows has a bound (`Limits`, `MAX_MEMBERS`).
- A run is its seed: election timeouts are drawn from `Config::seed`.

## Tests

| Where | What |
|---|---|
| `src/**` | the log, quorums, configurations, progress, reads; what the core refuses and the bounds it keeps |
| `tests/differential.rs` | this core and `raft-rs` on one schedule, compared after every step |
| `tests/group.rs` | groups of this core, and of both cores together, under schedules: safe whatever the schedule, and settled once the network is whole; the decisions of 27 §4.5 |
| `tests/fast.rs` | the fast track: committed by the fast quorum, taken again by the leader that follows, and groups that propose by it under schedules |
| `docs/models/FastTrack.tla` | the fast track's model, checked by `scripts/check-model.sh` |
| `benches/replicate.rs` | what replication costs with either core |

`FOCAL_RAFT_SEEDS`, `FOCAL_RAFT_STEPS` and `FOCAL_RAFT_SEED` set how many schedules
run, how long each is and where they begin. A failure names its seed and its step:

```
FOCAL_RAFT_SEED=1 FOCAL_RAFT_SEEDS=1 cargo test -p focal-raft --test differential
```
