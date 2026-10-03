# hyper-raft

The Raft core slates, focal and mantle share: elections and the log as a state machine
with no clock, no disk and no network. It is told what time has passed (`RawNode::tick`)
and what arrived (`RawNode::step`), and it says what to persist, send and apply
(`RawNode::ready`). A durable shell drives it: focal's is `focal-consensus`; the shared one
will be `hyper-durable` (`docs/raft.md`).

It began as focal's own core, `focal-raft`, and moved here with its history. `ORIGIN.md`
records the source revision and every change since.

It is Raft as Ongaro's thesis states it, with pre-vote, check-quorum, election priority,
learners, joint consensus, leader transfer, an inflight window with conflict hints,
ReadIndex and snapshots. Reads asked before the member is next asked what there is to do
share one round of heartbeats (`ReadRounds`). What a member is sent ahead of its
answers is bounded in messages and in bytes, the bytes by what its owner says the path to
it carries (`RawNode::set_inflight_bytes`). It keeps the log and speaks the messages of `raft-rs` 0.7
(`raft-proto`), which focal's groups ran on before. What it decides differently, and
why, is in the module header of `src/raft.rs` and in focal's
`docs/archictecutre/27-consensus-roadmap-and-slates-port.md` §4.5 (focal `a8e95f7`).

A group may have the fast track (`Config::fast`, `fast.rs`, `track.rs`; focal 27 §4.6): a
member that does not lead proposes to every voter at once (`RawNode::propose_fast`),
and its entry is committed when three quarters of the voters hold it, or a majority
holds it from the leader, whichever is first. A voter that holds the entry beside its log
counts for the three quarters only once its log holds an entry of the leader's term: without
that rule an election could commit a second entry at an index that held a committed one
(`docs/raft.md`, "The fast track's election defect"). No owner enables the fast track yet.

An owner that writes its log out and keeps its own copy of what it wrote drives the member with
`RawNode::ready_in_place`: it writes from `RawNode::to_persist`, applies the range
`Ready::committed_range` names from its own storage, and keeps the entries the member gives up at
`RawNode::advance_append_keeping`. Nothing is copied, and the member decides exactly as it does
under `RawNode::ready`.

An owner whose store takes several writes at once takes `Ready`s ahead of their persistence
(`docs/durable.md` §2.1): it issues each `Ready`'s write (`RawNode::advance_issued`), the member
takes operations again, and the store's answers come back in order (`RawNode::on_persist`, or
`on_persist_keeping` for entries written in place), up to `Limits::readies_in_flight` writes out.
A message that waits for a write is a `Ready`'s persisted message, and the owner sends it once that
write and every earlier one are durable.

A member's answers to appends and heartbeats state no commit beyond what its storage states when
they leave (`RawNode::durable_commit`; `docs/durable.md` §4.4). The member reads that from each
durable `Ready`'s hard state, which the owner writes as given; a commit the owner writes otherwise
(the one a `LightReady` gives, a write of the hard state alone) it states with
`RawNode::commit_durable`. An owner whose commit fence holds a committed page asks for no more with
`RawNode::pause_apply` until `resume_apply`, and `Config::apply_unpersisted` gives a leader its own
term's committed entries before its own write of them is durable.

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
| `tests/differential.rs` | this core and `raft-rs` on one schedule, compared after every step, with `Ready`s copied and given in place |
| `tests/group.rs` | groups of this core, and of both cores together, under schedules: safe whatever the schedule, and settled once the network is whole; the decisions of focal 27 §4.5 |
| `tests/fast.rs` | the fast track: committed by the fast quorum, taken again by the leader that follows, and groups that propose by it under schedules |
| `tests/pipeline.rs` | members that take `Ready`s ahead of their persistence, under schedules that interleave persistence with everything else and crash at every persistence step: held at every step to what their disks hold (`docs/durable.md` §3) |
| `benches/replicate.rs` | what replication costs with either core |
| `benches/pipeline.rs` | readies in flight against one at a time, on simulated devices and a simulated network |

The fast track's TLA+ model is still in focal (`docs/models/FastTrack.tla`). It moves here
with the fast-track decision (`docs/raft.md`).

`HYPER_RAFT_SEEDS`, `HYPER_RAFT_STEPS` and `HYPER_RAFT_SEED` set how many schedules
run, how long each is and where they begin. A failure names its seed and its step:

```
HYPER_RAFT_SEED=1 HYPER_RAFT_SEEDS=1 cargo test -p hyper-raft --test differential
```

Every count the tests print is fixed by the seed but one: in
`a_group_of_both_cores_is_safe_and_settles`, `raft-rs` draws its own election timeouts
from `rand::thread_rng`, so the terms each core led vary from run to run.
