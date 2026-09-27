# 27. Consensus roadmap and what to port from slates

Status: analysis and plan, 2026-09-27. Evidence for each closed item is recorded in
[09](09-implementation-status.md).

This document answers two questions. What in `../slates` should focal take, and what
should it leave. And how focal reaches the planned consensus feature set: Fast Raft
(Castiglia, Goldberg and Patterson, ICDCS 2020, arXiv 2004.06215; implementation report
arXiv 2506.17793), pre-vote, priority elections, parallel vote replication and
processing, learners, multi-log synchronization (MLRaft, EITCE 2022) and leader
transfer.

## 1. Where focal stands

focal's consensus is `focal-consensus::DurableNode`, a durable, memory-accounted shell
around tikv raft-rs 0.7 `RawNode`. The shell owns persistence, checkpoints, decoder
fences and the unwind boundary. raft-rs owns elections and the log.

| Planned feature | raft-rs 0.7 | focal today |
|---|---|---|
| Pre-vote | yes | on (`pre_vote: true`), tested in `raft_safety_tests` |
| Check-quorum | yes | on, tested |
| Learners | yes | root learners and session learners in production |
| Joint consensus | yes | tested |
| Leader transfer | yes | `transfer_leader`, `cluster leader transfer` |
| Priority elections | yes (`Config::priority`, `set_priority`) | not wired |
| Pipelining, inflight window | yes | `max_inflight_messages` 128 |
| Parallel vote and append fan-out | driver concern | per-peer sends through the pool; not progress-aware |
| Multi-group | driver concern | root, partition and session groups share one WAL and one pool; no leader balancing |
| Fast track commit | no | no |

Four of the seven planned features are configuration and drive work on the library
focal already has. Fast track is not: see section 4.

## 2. slates: what it is

slates does not use a Raft library or a QUIC library. Its Raft is a hand-written
sans-io core (`crates/cluster/src/raft.rs`, about 2,600 lines with tests) and its
transport is its own QUIC-shaped protocol over `rustls::quic`. Consensus there holds
configuration only; data writes are per-object fenced registers.

Its core has pre-vote, check-quorum, learners, joint consensus, ReadIndex and
snapshots. It has no leader transfer, no priority elections, no fast track, no
pipelining and no conflict hints. ReadIndex and snapshot install exist in the core but
are not driven in production.

## 3. Port decisions

### 3.1 Take

| # | From slates | Why focal needs it | Lands in |
|---|---|---|---|
| P1 | Progress-aware fan-out: `broadcast`, `DispatchWait`, `CommitBudget`, `Stragglers` | A round stops when every peer has reported or when no reply arrives within a stall window, and extends while a quorum is still filling. Late replies are folded into the operation they belong to. focal's drivers bound sends per peer but still decide by fixed deadlines. | `focal-wire` pool plus the replication and placement drivers |
| P2 | Derived timing: `PathRtt` (RFC 9002 smoothing), `ElectionTiming::derive`, `round_budget` | focal's election and heartbeat ticks are constants. A WAN group whose round trip exceeds the fixed budget never elects. slates derives the election base from the slowest voter's tail. | `focal-consensus` config, fed by liveness RTT |
| P3 | Period-counted timers | A starved node waits longer instead of campaigning. focal's owner ticks on wall time. | control and replica owners |
| P4 | Voter reconciliation rules: retire only a death held continuously for one election window; sitting live voters keep their seats | focal's placement controller heals on liveness; the hold window and seat stability are not stated rules. | placement controller |
| P5 | Admission by certificate: a pending-handshake reservation separate from authenticated slots, two slots per identity, replace on redial | focal bounds connections in total. One identity can take them. | `focal-wire` listener |
| P6 | Link validity inside a wait | A pending request should end when its peer's identity is replaced or retired, not at its deadline. | `focal-wire` pool |
| P7 | Simulated network: bottleneck with drop-tail queue, Gilbert-Elliott loss, MTU, NAT rebinding | focal-sim has disk faults and no network model. Election, fast-track and transfer claims need one. | `focal-sim` |
| P8 | Per-progress test deadlines (`poll_until` charged to the slowest node's progress counter) | focal's fleet tests use wall-clock deadlines and fail under load; this recurred three times in this work. | test support |
| P9 | The bug corpus as regression cases (section 3.3) | Each is a class, not an instance. | tests |
| P10 | Copa congestion control, as a candidate | slates measured ping p99 104 ms under bulk load on a 100 ms path, against 185 to 199 ms for NewReno, CUBIC and BBR. One simulated result. quinn accepts a custom controller. | `focal-wire`, behind a measurement |

### 3.2 Leave

- The transport protocol. It serves one exchange per session, is IPv4 only, has no
  idle timeout, close, reset, migration or key update, and copies each packet several
  times. quinn covers all of these.
- The Raft core as a replacement for raft-rs in the classic path. raft-rs is more
  complete (transfer, pipelining, conflict hints) and more exercised.
- Fixes for stream-id reuse, handshake tails and flight size, which quinn handles.

### 3.3 Bug classes already checked against focal

| slates class | focal state |
|---|---|
| Fan-out waits out a dead voter | Fixed: unreachable cooldown; detached dial (section 09, 2026-09-13) |
| Stale ack counted as liveness | Fixed: probe nonces |
| Fixed probe deadline kills a starved peer | Fixed: RTT-bounded probe timeout, unconfirmed patience |
| Abandoned request poisons the next | Not applicable: quinn streams |
| Voter set never shrinks | Fixed: drain, remove, contact retirement |
| Hard consensus budget under load | Open: P1 |
| Round expires inside the WAN round trip | Open: P2 |
| Council retires a suspected voter | Open: P4 |
| Wall-clock test deadlines | Open: P8 |

## 4. Fast Raft

### 4.1 The algorithm

With M voting members the fast quorum is ⌈3M/4⌉ and the classic quorum is a majority.

1. A proposer sends its entry for index i to every member, not to the leader.
2. A member that has no entry at i inserts it there, marked self-approved, and sends
   the entry as its vote to the leader. It never overwrites an entry with a proposal.
3. The leader tallies votes per index. For k = commitIndex + 1, once a classic quorum
   of votes is in, it inserts the entry with the most votes, marked leader-approved.
   If a fast quorum voted for that entry and the entry's term is current, it commits.
4. Otherwise the leader replicates its choice by AppendEntries, the classic track.
   Members overwrite a self-approved entry with the leader's.
5. Elections compare only leader-approved entries. Vote replies carry the voter's
   self-approved entries, and a new leader tallies them before anything else: an entry
   chosen by a fast quorum has the most votes in every classic quorum, so the new
   leader makes the old leader's decision.

The fast track is only open at commitIndex + 1. Joins and leaves go through the leader
one at a time; a member that stops answering is removed after a member timeout.

Measured by its authors: about half of classic Raft's commit latency below 5% message
loss, worse above it, because a failed fast attempt costs one extra round.

### 4.2 What it requires of the core

- The log admits an entry at an index above the last, leaving gaps, and an entry can
  be replaced by the leader's choice.
- The up-to-date rule in elections reads the last leader-approved entry.
- Vote replies carry self-approved entries.
- A new leader runs recovery before it writes its first entry.

raft-rs provides none of these. Its log is append-only, its vote messages are built
inside the library, and on election it appends a no-op at last + 1, the index where a
fast-committed entry from the previous leader may live. A layer beside raft-rs cannot
be made safe: the fast decision and the leader's no-op contend for the same index.

So Fast Raft needs a core focal owns. The decision is to build `focal-raft`, a sans-io
core in the shape of slates' (pure state machine, messages in and out, no clock, no
I/O), and to run it under the existing `DurableNode` shell, which keeps persistence,
memory accounting, checkpoints and decoder fences unchanged.

### 4.3 Where the fast track pays

The fast track removes the proposer-to-leader hop. It helps a proposer that is not the
leader: a host forwarding to the root leader, a client attached to a follower. A
proposer on the leader already commits in two message delays. The fast quorum is also
larger: with three voters it is all three, so one slow voter closes the fast track,
and with five it is four.

The plan therefore keeps the classic track as the default per group and enables the
fast track per group by policy, with the decision recorded in the group's
configuration so every member agrees on the quorum rule in force.

### 4.4 Safety obligations to prove and test

- Invariant 1: a follower commits at an index only after the leader committed the same
  entry there.
- Invariant 2: a leader never commits at an index an entry different from one a
  previous leader committed there.
- Recovery: for every index where a fast quorum voted for one entry, every classic
  quorum of vote replies shows that entry with the most votes.
- A self-approved entry is durable before its vote is sent.
- Configuration entries never take the fast track.

These are checked three ways: a TLA+ model (slates keeps models beside its code and
focal has none for consensus), property tests over the sans-io core with a
deterministic scheduler and P7's network model, and the existing black-box history
checker on real processes.

## 5. The other features

**Priority elections.** A voter refuses its vote and its pre-vote to a candidate of
lower priority unless the candidate's log is strictly longer than its own. Priority
never outranks the log, and a group whose highest priority member is gone elects among
the rest (`DurableNode::set_priority`, three tests in `raft_safety_tests`). focal
sets the priority per group member from the committed placement: the intended leader
highest, then members in the leader's zone, then the rest. Priorities are configuration,
never liveness.

**Leader transfer.** Already present. Added: transfer on drain before a voter leaves,
and transfer as the balancer's action.

**Multi-log synchronization.** MLRaft splits one log into n logs, each with its own
leader, and spreads the leaders with priority election and dynamic transfer. focal
already runs many groups per node. What it lacks is MLRaft's two balancing mechanisms:
initial spread through priorities, and a balancer that moves leaders when the spread
drifts. Cross-log order is not needed: focal's sessions are independent ledgers.

**Parallel vote replication and processing.** Votes and appends go to all peers at
once through P1's fan-out, and inbound replies are stepped as they arrive instead of
in send order. A group's owner thread stays the only writer of its state.

**Learners.** Present. Added with the new core: learners never count toward either
quorum and never vote, tested as slates tests it.

## 6. Order of work

| Stage | Content | Exit evidence |
|---|---|---|
| A | P8 per-progress deadlines; P7 network model in `focal-sim` | fleet suites pass under injected CPU load |
| B | Priority elections wired; transfer on drain; P2 derived timing | election tests under LAN, regional and geographic profiles |
| C | P1 progress-aware fan-out; P5, P6 | dead-voter and straggler tests; no round waits out a dead peer |
| D | `focal-raft` core: classic track at parity with raft-rs for focal's use | differential test against raft-rs over random schedules |
| E | Fast track in `focal-raft`; TLA+ model | section 4.4 invariants; latency measured against classic under 0 to 10% loss |
| F | MLRaft leader balancer | leader spread converges; no transfer storms |
| G | P10 congestion measurement; decide | bake-off numbers recorded |

Each stage closes on the workspace gates and on CI for Linux, macOS and Windows.
