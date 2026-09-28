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
around a core that owns elections and the log. The shell owns persistence, checkpoints,
decoder fences and the unwind boundary. Until 2026-09-28 the core was tikv raft-rs 0.7
`RawNode`; since then it is `focal-raft` (section 4.5), which keeps raft-rs's log and
speaks its messages. The table states what raft-rs gave and what focal ran when this
plan was made.

| Planned feature | raft-rs 0.7 | focal today |
|---|---|---|
| Pre-vote | yes | on (`pre_vote: true`), tested in `raft_safety_tests` |
| Check-quorum | yes | on, tested |
| Learners | yes | root learners and session learners in production |
| Joint consensus | yes | tested |
| Leader transfer | yes | `transfer_leader`, `cluster leader transfer`; before a leading voter is removed (section 5) |
| Priority elections | yes (`Config::priority`, `set_priority`) | session groups: from the committed placement's preferred leader |
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
| P1 | Progress-aware fan-out: `broadcast`, `DispatchWait`, `CommitBudget`, `Stragglers` | A round stops when every peer has reported or when no reply arrives within a stall window, and extends while a quorum is still filling. Late replies are folded into the operation they belong to. focal's drivers bound sends per peer but still decide by fixed deadlines. | `focal_timing::{RoundBudget, RoundWait}`, `focal_wire::gather`. Two differences from slates, both from what focal's transport is. The budget is derived from what exchanges with the round's peers were measured to take, the peer's work included, because a focal request waits on a commit at its peer and not on the path alone. And an exchange outstanding when its round ends is dropped, not kept to fold later: a Raft reply in focal is an inbound message of its own, and a signature past the majority has no use. The pool counts a dropped exchange as given up on and doubles what that peer is expected to take until it answers one (RFC 9002 §6.2), so an estimate that ended a round too early corrects itself |
| P2 | Derived timing: `PathRtt` (RFC 9002 smoothing), `ElectionTiming::derive`, `round_budget` | focal's election and heartbeat ticks are constants. A WAN group whose round trip exceeds the fixed budget never elects. slates derives the election base from the slowest voter's tail. | `focal_timing::{PathRtt, TickPace}`; the pool measures each path by the liveness probes the peer answers, as the median of the latest sixteen and their median absolute deviation, so that an answer that came late does not set a group's election timeout; both owners tick at the derived period, and a leader beats at the configured cadence whatever its period. As in slates, what stretches is the election timeout and never the heartbeat |
| P3 | Period-counted timers | A starved node waits longer instead of campaigning. focal's owner ticks on wall time. | control and replica owners |
| P4 | Voter reconciliation rules: retire only a death held continuously for one election window; sitting live voters keep their seats | focal's placement controller heals on liveness; the hold window and seat stability are not stated rules. | placement controller |
| P5 | Admission by certificate: a pending-handshake reservation separate from authenticated slots, two slots per identity, replace on redial | focal bounds connections in total. One identity can take them. | `focal_wire::Admission`, in the node's listener. focal's identities are principals, and a participant may run several clients: a node holds 4 connections and any other identity 16. A connection past the bound replaces the one of that identity that was idle longest. Refusing the newcomer, which was the first rule here, made a participant whose clients exit without closing wait out the idle timeout of what they left behind |
| P6 | Link validity inside a wait | A pending request should end when its peer's identity is replaced or retired, not at its deadline. | `focal-wire` pool: a retired route closes its connection under the lock a dial stores it under, so no order of the two leaves one open |
| P7 | Simulated network: bottleneck with drop-tail queue, Gilbert-Elliott loss, MTU, NAT rebinding | focal-sim's network delivered at delays the test chose, with partitions and no path model. Election, fast-track and transfer claims need one. | `focal_sim::path` (`Fabric`, `Path`, `Loss`, `Link`, `Nat`), every table bounded (`FabricLimits`) |
| P8 | Per-progress test deadlines (`poll_until` charged to the slowest node's progress counter) | focal's fleet tests use wall-clock deadlines and fail under load; this recurred four times in this work. | `focal_timing::ProgressDeadline`; owners count their periods (`periods()`, `focal_root_periods_total`) |
| P9 | The bug corpus as regression cases (section 3.3) | Each is a class, not an instance. | tests |
| P10 | Copa congestion control, as a candidate | slates measured ping p99 104 ms under bulk load on a 100 ms path, against 185 to 199 ms for NewReno, CUBIC and BBR. One simulated result. quinn accepts a custom controller. | `focal-wire`, behind a measurement |

### 3.2 Leave

- The transport protocol. It serves one exchange per session, is IPv4 only, has no
  idle timeout, close, reset, migration or key update, and copies each packet several
  times. quinn covers all of these.
- The Raft core as a replacement for raft-rs in the classic path. raft-rs is more
  complete (transfer, pipelining, conflict hints) and more exercised. focal's own core
  (section 4.5) takes slates' shape, a state machine with no clock, disk or network,
  and raft-rs's behaviour, which it is compared with step for step.
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
| Wall-clock test deadlines | Converted: the fleet, placement, service, split, route, credential and liveness suites of `focal-node`, and every binary suite (`tests/support/deadline.rs`, `tests/support/fleet.rs`) |
| A leader removes itself and keeps leading | Fixed: refused by the shell (`LeaderLeaving`), removal and demotion alike; the controller and `cluster nodes remove` transfer first; and a leader that applies its own leaving all the same, proposed by the one that led before it, hands the group over and follows (section 4.5) |
| A new copy refuses a leader outside its genesis configuration | Fixed: copies admit the members the committed directory names (`ReplicaHost::admit_members`) |

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
memory accounting, checkpoints and decoder fences unchanged. The classic track of that
core is built and in service (section 4.5). Its log is the classic one; the entries a
member approves by itself are kept beside it and not in it, which is stage E.

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

### 4.5 The core as built: the classic track

`focal-raft` is Raft as Ongaro's thesis states it with pre-vote, check-quorum, election
priority, learners, joint consensus, leader transfer, an inflight window with conflict
hints, ReadIndex and snapshots. It uses raft-rs's wire and log types (`raft-proto`, the
same pinned revision), so the bytes on the wire and in the WAL did not change, and a
group whose nodes are replaced one by one is one group throughout.

**Compared with raft-rs step for step.** `tests/differential.rs` runs both cores on one
schedule: five members, three of them voters at first; messages delivered, lost,
repeated and held back; members stopped and reopened from what was durable; logs
compacted; membership changed by simple and joint changes; leadership transferred;
reads asked; priorities set. After every step both say what they persisted, sent,
committed and answered and what each knows of every member, and all of it is equal.
raft-rs draws its election timeouts from the thread; the harness gives it the ones this
core drew from its seed. It is driven as focal's shell drove it, its priority rules
included.

**Where the two differ, by decision.** Each is tested by itself (`tests/group.rs`,
`src/tests.rs`, `raft_safety_tests`).

| raft-rs 0.7 | `focal-raft` |
|---|---|
| Asserts; the shell contains the unwind and stops the replica | Returns an error of one of three kinds: a refusal that changed nothing, a peer's message that contradicts what the member holds, or a state that no longer adds up, which alone stops the replica |
| Its generated accessors unwind on an enumeration value they do not know, which a peer chooses | Reads them as options (`focal_raft::proto`); the accessors are forbidden by lint in production (`clippy.toml`) |
| A leader that applies a change which leaves it no voter leads on, and unwinds when it next commits | It tells the voter that holds the whole log to campaign, and follows |
| One told to campaign while a change it committed is not applied forgets that it was told | It campaigns once the change is applied, unless it heard of a leader since |
| A voter refuses a candidate of lower priority unless the candidate has more entries | Unless the candidate's log is more current, by its last term and then its length (`Precedence::Log`). By length alone, two voters whose logs are equally long and end in different terms refuse each other, one for priority and one for the log, and with the third away the group elects no one. The rule of raft-rs is kept (`Precedence::Length`) to compare the cores under one rule |
| Priority judges the vote a transfer asks for; a member without a term that refuses for priority unwinds | Priority never judges a transfer and is not in force without a term. The shell did both for raft-rs; they are the core's now |
| One that is no voter may campaign, and unwinds when it wins | Refused (`NotPromotable`) |
| A change refused for the size of what is uncommitted leaves the leader believing one is pending | It does not |
| Election timeouts from the thread's generator | From a seed the owner gives: a run is reproducible |
| Queues without a bound of their own | `Limits`: messages and reads that wait, entries not yet durable, entries in one message. A member takes of a message what it may hold and answers with the last entry taken |

**What is kept although it could be otherwise.** A member that a change removes and
adds again is known anew, and a member added by a change is first probed one entry
before the log's end. Both are raft-rs's, harmless, and kept so that the comparison
needs no exception for them.

## 5. The other features

**Priority and transfer.** Priority orders elections and never vetoes a transfer: the
vote a transferred campaign asks for is judged by the log alone. A transfer is a
decision that a member shall lead, the leader's or its operator's or the controller's,
and the priority of the voters is no part of it.

**Priority elections.** A voter refuses its vote and its pre-vote to a candidate of
lower priority unless the candidate's log is more current than its own: a later last
term, or the same and more entries (section 4.5). Priority
never outranks the log, and a group whose highest priority member is gone elects among
the rest (`DurableNode::set_priority`, four tests in `raft_safety_tests`). A session's
owner sets its replica's priority each period from the placement the session has
committed: the preferred leader 2, every other voter 1 (`fleet::PREFERRED_LEADER_PRIORITY`,
`VOTER_PRIORITY`). Priorities are configuration, never liveness. A third rank, voters
in the preferred leader's zone above the rest, needs the members' zones in the
session's own committed state, which holds node identities only; it is part of stage F,
where the balancer chooses by zone.

A node that has no term yet keeps the neutral priority: a node with no term has no log
to defend, and its refusal would bear no term a candidate could hear (raft-rs 0.7
unwinds there: `term should be set when sending MsgRequestPreVoteResponse`).

**Leader transfer.** Already present. A leader does not propose its own leaving,
neither its removal nor its becoming a learner: `DurableNode` refuses the proposal
(`ConsensusError::LeaderLeaving`) on every path a configuration change takes, so that
leadership moves by the decision of whoever removes the node. A leader that applies
its own leaving all the same hands the group over and follows (section 4.5). The
placement controller moves a session's leadership to a voter that stays (the
preferred leader where it votes) before it removes a draining voter
(`SessionCall::Transfer` reaches a leader on another node), and `cluster nodes remove`
does the same for a root voter. Transfer as the balancer's action is stage F.

**A node's own socket reaches a log only where that node leads it** (24 §14, a limit
stated there). With leadership placed by priority and moved by transfer, that limit is
met in ordinary operation and no longer only after a failure: a client on the local
socket of a node that follows is refused until leadership returns. A client over QUIC
is sent on to the leader. Serving a local client through the leader needs the node to
speak for that client to another node, which is a change to who a leader trusts; it is
an open decision and not made here.

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
| | *State 2026-09-27:* P7 in place with 32 tests. P8 in place for the suites named in section 3.3; the run under injected load is not recorded yet. | |
| B | Priority elections wired; transfer on drain; P2 derived timing | election tests under LAN, regional and geographic profiles |
| | *State 2026-09-28:* wired for session groups, with `drain_leader` on real processes. Elections run over `focal_sim::path` at the three profiles (`sim_election_tests`): real replicas on real logs in virtual time, at the derived pace. | |
| C | P1 progress-aware fan-out; P5, P6 | dead-voter and straggler tests; no round waits out a dead peer |
| | *State 2026-09-28:* P5 and P6 in place (`focal_wire::Admission`; a retirement always closes its connection). P1: the round is in place (`focal_timing::RoundBudget`, `RoundWait`; `focal_wire::gather`; `PeerConnectionPool::exchange_tail`, `round_budget`) and session-fact signatures are collected by it. Still asked one peer after another on fixed deadlines: custody replication and the custody and seed pulls (`evidence_service`, `managed_support`), and the fallback of enrollment control and of the contact announcement to the installed routes (`network_control`, `network_controller`). | |
| D | `focal-raft` core: classic track at parity with raft-rs for focal's use | differential test against raft-rs over random schedules |
| | *State 2026-09-28:* built and in service under `DurableNode` (section 4.5). Five campaigns of schedules compare the two cores step for step; a run of 15,000 schedules compared 80.8 million steps and found them equal ([09](09-implementation-status.md)). Groups of both cores together, and of this core alone under schedules that also remove the leader, are safe and settle. Replication costs what it cost (`benches/replicate.rs`). | |
| E | Fast track in `focal-raft`; TLA+ model | section 4.4 invariants; latency measured against classic under 0 to 10% loss |
| F | MLRaft leader balancer | leader spread converges; no transfer storms |
| G | P10 congestion measurement; decide | bake-off numbers recorded |

Each stage closes on the workspace gates and on CI for Linux, macOS and Windows.
