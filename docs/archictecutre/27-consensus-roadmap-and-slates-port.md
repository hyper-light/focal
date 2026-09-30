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
decoder fences and the unwind boundary. Its guard reserves, before a transition runs,
what the transition copies and nothing the size of the history (2026-09-29, the audit's
F15/F16): a page of the log is chosen before it is copied and copied exactly, the
entries not yet durable, the held proposals, the committed page, each lagging peer's
page or snapshot and the window of pages for the one that answers are named from
counters the core keeps as it changes and running totals the storage keeps beside its
entries, and a heartbeat over a long history asks for what a heartbeat copies. Until 2026-09-28 the core was tikv raft-rs 0.7
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
| P1 | Progress-aware fan-out: `broadcast`, `DispatchWait`, `CommitBudget`, `Stragglers` | A round stops when every peer has reported or when no reply arrives within a stall window, and extends while a quorum is still filling. Late replies are folded into the operation they belong to. focal's drivers bound sends per peer but still decide by fixed deadlines. | `focal_timing::{RoundBudget, RoundWait}`, `focal_wire::gather`. Two differences from slates, both from what focal's transport is. The budget is derived from what exchanges with the round's peers were measured to take, the peer's work included, because a focal request waits on a commit at its peer and not on the path alone. And an exchange outstanding when its round ends is dropped, not kept to fold later: a Raft reply in focal is an inbound message of its own, and a signature past the majority has no use. The pool counts a dropped exchange as given up on and doubles what that peer is expected to take until it answers one (RFC 9002 §6.2), so an estimate that ended a round too early corrects itself A round that has gathered nothing is given its whole deadline (2026-09-29): the deadline is derived from the tail of these peers' exchanges, so an answer inside it is the one the round opened for, and the judgement at three quarters of the deadline in force applies once something has arrived — the defect slates found in its `DispatchWait` on 2026-09-29 (a dispatch that had gathered nothing stopped at three quarters, and every pre-election whose one live voter answered in the last quarter failed), present in focal's port until then. |
| P2 | Derived timing: `PathRtt` (RFC 9002 smoothing), `ElectionTiming::derive`, `round_budget` | focal's election and heartbeat ticks are constants. A WAN group whose round trip exceeds the fixed budget never elects. slates derives the election base from the slowest voter's tail. | `focal_timing::{PathRtt, TickPace}`; the pool measures each path by the liveness probes the peer answers, as the median of the latest sixteen and their median absolute deviation, so that an answer that came late does not set a group's election timeout; both owners tick at the derived period, and a leader beats at the configured cadence whatever its period. As in slates, what stretches is the election timeout and never the heartbeat. An owner's own stalls are covered too, and in ticks rather than in the period: the replica waits, beyond its election timeout, as many ticks as the longest stall the owner remembers took and a tail of the paths after it (`TickPeriod::patience`, `focal_raft::Raft::set_patience`), since a heartbeat leaves the leader's tick, a leader that stalls as this node does sends none for the stall, and a node that stalls cannot tell such a leader from one that died. A stall is remembered for the margin times its own length, ten seconds for a stall of one, and a longer one takes its place at once; it is the excess over the intended period that is measured, never the period itself, which the pace made and which would hold the pace wherever it was. Nothing else that is counted in periods grows with a stall except what the stall itself delayed: a request the owner holds is given the same ticks beyond its time, since a barrier the owner was late to run for a stall of its own is not given up for it; a wait's budget and the cadence of everything the owner does stay what the paths make them (`focal_root_period_longest_ms` is the longest a period took). A request an owner holds is given its time in the owner's periods and not the clock's (`ControlHost`, `ReplicaHost`, their directory, placement, managed and evidence sub-owners): a loaded machine slows the rounds and the request's time with them |
| P3 | Period-counted timers | A starved node waits longer instead of campaigning. | The control and the replica owner tick once when their period has passed and begin the next from then: a period that was missed is not made up for, and the core counts ticks, so an owner that was starved for ten periods has waited eleven |
| P4 | Voter reconciliation rules: retire only a death held continuously for one election window; sitting live voters keep their seats | focal's placement controller healed on the first verdict, and one ordering decided both who keeps a seat and who is nearest home. | `focal_directory::{heal_placement, home_move, deaths_held}` and the controller (section 5, seats) |
| P5 | Admission by certificate: a pending-handshake reservation separate from authenticated slots, two slots per identity, replace on redial | focal bounds connections in total. One identity can take them. | `focal_wire::Admission`, in the node's listener. focal's identities are principals, and a participant may run several clients: a node holds 4 connections and any other identity 16. A connection past the bound replaces the one of that identity that was idle longest. Refusing the newcomer, which was the first rule here, made a participant whose clients exit without closing wait out the idle timeout of what they left behind. The connections held in all are bounded after that rule, never by an outer count (the audit's F20); a body is permitted from the listener's budget, within its identity's share, before it is allocated (F03); the grant is current at dispatch and a revocation closes the certificate's connections (F35) |
| P6 | Link validity inside a wait | A pending request should end when its peer's identity is replaced or retired, not at its deadline. | `focal-wire` pool: a retired route closes its connection under the lock a dial stores it under, so no order of the two leaves one open |
| P7 | Simulated network: bottleneck with drop-tail queue, Gilbert-Elliott loss, MTU, NAT rebinding | focal-sim's network delivered at delays the test chose, with partitions and no path model. Election, fast-track and transfer claims need one. | `focal_sim::path` (`Fabric`, `Path`, `Loss`, `Link`, `Nat`), every table bounded (`FabricLimits`) |
| P8 | Per-progress test deadlines (`poll_until` charged to the slowest node's progress counter) | focal's fleet tests use wall-clock deadlines and fail under load; this recurred four times in this work. | `focal_timing::ProgressDeadline`; owners count their periods (`periods()`, `focal_root_periods_total`) |
| P9 | The bug corpus as regression cases (section 3.3) | Each is a class, not an instance. | tests |
| P10 | Copa congestion control, as a candidate | slates measured ping p99 104 ms under bulk load on a 100 ms path, against 185 to 199 ms for NewReno, CUBIC and BBR. One simulated result. quinn accepts a custom controller. | `focal_wire::congestion`, chosen by measurement (section 7); with it slates' classes of traffic (`focal_wire::TrafficClass`) |

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
| Hard consensus budget under load | Fixed for what a group decides by (P1, `focal_wire::gather`), and for content, which is asked of every copy at once and waits as long as its path takes (section 7) |
| Round expires inside the WAN round trip | Fixed: P2, the derived pace and the derived round budget |
| Council retires a suspected voter | Fixed: a death moves a seat once it has stood for one election window of the group, and a heal moves nothing else (section 5, seats) |
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

These are checked three ways: a TLA+ model (`docs/models/FastTrack.tla`, checked by
`scripts/check-model.sh` in CI, with the rule the core does not follow beside it, which
the checker must refuse), tests over the sans-io core under seeded schedules
(`crates/focal-raft/tests/fast.rs`) and over durable nodes and P7's network model
(`fast_track_tests`, `sim_fast_tests`), and the existing black-box history checker on
real processes, for an owner that takes the fast track (section 4.6).

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
| One told to campaign while it asks whether it could be elected ignores it; the leader waits an election timeout for it and takes no proposal meanwhile | It campaigns: the leader of its term asks, and the lease that refuses what it asked refuses no hand-over. The comparison loses that message for both cores and goes on |
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

### 4.6 The fast track as built

`focal_raft::fast` and `track` in the core, `DurableNode::propose_fast` in the shell. A
group has the fast track or has none from the day it is made (`NodeConfig::fast`), and
says so in a record of its own in its log (`RecordKind::FastTrack`), which a binary
that knows no fast track refuses. The identity record's bytes are as they were.

**What a member approved by itself is held beside its log, never in it.** The log holds
what a leader approved and nothing else, so it is the classic log, elections compare
it as they always did, and the fast track changes no byte of an entry. What is held is
bounded (`Limits::proposals`, `proposal_bytes`, `fast_window`), is on disk before the
member says that it holds it (`RecordKind::Proposal`, in the same write as the rest of
the `Ready`), is given back when the member opens, and is carried by a checkpoint.

**A leader stamps what it takes with its own term.** The term a proposer gave says
nothing of an entry: two proposers of one term propose different entries for one
index, and an index and a term name one entry of a log only while one member writes
each term. Fast Raft as published keeps the proposer's term; two entries of one index
and one term, approved by two leaders, would then pass each other's check of the point
an append follows.

**The leader takes what it hears of first.** For the next index of its log a leader
takes the first entry it hears of, from the proposer or from a voter that holds it, and
sends it to its members as it sends any entry. The index is committed by whichever
quorum comes first: the fast quorum that holds the entry, or the classic quorum that
holds it from the leader. Fast Raft as published waits for the votes of a classic
quorum before the leader takes an entry, and pays a round when the fast quorum does
not come, which is why its authors measured it slower than classic Raft above 5%
loss. Taking at once costs no more than the classic track does at any loss
(section 09, 2026-09-28).

It is safe for the reason the classic track is. Only a leader commits. What a leader
committed by the classic quorum every later leader holds in its log. What a leader
committed by the fast quorum R every later leader takes at its election: of the
members that elected it, more hold that entry by themselves than are outside R, so it
is the entry most held among them; and a member that holds it from the leader votes
for no one whose log lacks it. An entry of an index no voter holds anything at was
committed by no one, and one that is elected writes an entry there that states
nothing.

**A member does not compare terms at or below its commit.** An entry committed by the
fast quorum bears the term of the leader that took it; the leader after it, which
took it again at its election, gave it its own. Both state the same. A member
therefore takes what is committed at it to be what the leader holds there
(`Log::append_after`), which a leader's log is by what makes it a leader. It follows
that **an owner of a group with the fast track derives nothing from the term of an
entry**: it is not the same at every member.

**The fast quorum commits only under a configuration that is applied and not joint**,
so that the quorum a leader counts is of the configuration a later leader counts by.
Configuration changes never take the fast track.

**What the fast track asks of an owner**, and which owners can give it:

| | Needs | Session groups | Control groups |
|---|---|---|---|
| An entry states a request, which every member evaluates when it applies it | yes | no: an entry is the outcome of the leader's admission, which is the sequencer | yes: the machine prepares and publishes each command at every member (`ControlReplica`) |
| Nothing is derived from an entry's term | yes | no: `NativeCommit::raft_term` | no: an envelope binds `owner_term` to the entry's term, and a receipt states it |
| A proposer that is told its entry lost its index proposes it again, and the owner knows the request it has already applied | yes | yes: exact retry | yes: `retries` |

No owner takes the fast track today. Giving it to the control groups is a change of
two checks and of what a receipt states, which is a durable format; it is a decision
and not made here.

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
committed: the preferred leader 3, the voters in its zone 2, every other voter 1
(`fleet::PREFERRED_LEADER_PRIORITY`, `ZONE_PRIORITY`, `VOTER_PRIORITY`). Priorities are
configuration, never liveness. The session's committed state holds node identities
only, so who is in the preferred leader's zone is said by the directory, which has
committed every node's region and zone: the node's agent tells each copy it hosts
(`ReplicaHost::admit`, `fleet::Near`), with the members it admits. What it says names
the preferred leader it is said of, and ranks a voter only while the session's own
placement prefers that same leader; a zone that is not known is near to nothing.

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
does the same for a root voter. A planned stop hands off too (2026-09-29, from slates'
4e38d3e): a replica that leads when its owner is told to stop — SIGTERM, a process
manager's stop, an embedded node's — asks the most caught-up voter it hears from to
campaign (`focal_control::heir`; a session prefers its placement's preferred leader
when that one qualifies) and keeps ticking and beating until its log leads elsewhere,
or for one election timeout in its own periods, before it stops as before; the control
groups do the same (`ControlHost` waits on the hand-off in its loop). A leader that
went silent cost the survivors their whole election timeout for every log it led:
5.2 to 7.0 s on three loaded processes in `tests/stop_handoff.rs` before the change,
84 ms after it, in one term, the stopping node reporting the hand-off in its last
status line (`sessions_handed_off`), which the test requires. Four defects of the
stop stood in the way and are fixed with it: the service dropped the drivers that
carry its owners' messages, and the listener that receives their peers', before the
owners stopped; the fleet's quiesce refused every routing lookup; the shared worker
discarded every message routed to a stopping session; the peer pool closed before
the owners. A stop now runs as a phase of the service: the owners stop while the
listener and the egress drivers are still polled, and the pool closes after them.

**Leadership returns.** Priority decides an election and starts none: after the
preferred leader was away and came back, another voter leads, and would until it
fails. The voter that leads hands leadership on (`leader_return`, run by the owner
each period): to the preferred leader, and while that one is not there to take it, to
a voter of its zone where the one that leads is of neither. What it decides on is what
the leader observes of the member on its own periods, and everything it counts has a
bound:

| Rule | Value | Why |
|---|---|---|
| Fit | 2 election timeouts | The member replicates without probing, holds everything committed and has answered since the leader last checked its quorum, every period of them. One that has just returned is not asked to lead while it may leave again |
| Quiet | until nothing proposed is undecided, 8 election timeouts at most | A leader takes no proposal while it hands over. Load that never pauses delays the hand-over and cannot prevent it |
| Rest | 4 election timeouts, doubled by each failure up to 6 times | A hand-over that was abandoned, or after which leadership came back to the same replica within the longest rest, is a failure. A preferred leader that cannot keep leadership costs the group one election per rest, never a storm of them |
| Held | while a change of configuration or of placement is in progress, a hand-over is under way, or the replica is stopping | The decision of whoever changes the group comes first |

A refusal where the hand-over is asked is counted as a failure and rested on. The
replica says how often it asked and how often that did not hold
(`leader_returns`, `leader_returns_failed` in `cluster replicas diagnostics`;
`focal_session_leader_returns_total`, `focal_session_leader_returns_failed_total`,
`focal_session_preferred_leader` in the node's metrics).

**Seats.** Who keeps a seat when a placement is planned again was one ordering, home
region first and then incumbency, and it was wrong at both ends: the death of one
voter moved every voter that was not at home, and nothing moved a voter toward home
unless something died. They are two decisions (`focal_directory::seats`).

*A heal* fills the seats that were vacated and moves nothing else. A voter that sits,
is enrolled at the generation it sits at, is eligible, alive and inside the residency
keeps its seat, wherever it is and whatever has joined since. What admits a node to a
seat it does not have (a load report, memory, disk) is no condition of keeping one. The
one seat a heal takes from a voter that could keep it is the one the policy cannot do
without: a session with home regions is led at home, and where no voter that sits is
there, a node that is takes the seat given last. A death moves a seat once it has
stood for one election window of the group that would lose the voter, twice its
election timeout at the pace the group runs at (`retirement_hold`): two seconds on one
network, forty across the planet at the ceiling of the pace. A member that is back
within it keeps its seat. What an operator asks for is not held.

*A move toward home* gives one seat of a voter that is not at home to a node that is,
the most loaded voter's first. It is decided as a move of a preferred leader is: for a
state that has lasted `FOCAL_HOME_BALANCE_HOLD_SECS` (30), one session of a partition
at a time, leaders and seats together, and not at all with `FOCAL_HOME_BALANCE=off`.
Every move leaves one voter fewer away from home and none more, so the moves end.

On one node neither decides anything. Five real processes: a session placed while its
home region had one node keeps its two voters elsewhere until two nodes join at home,
and then moves one seat, and then the other, and rests (`tests/home_balance.rs`).

**A node's own socket reaches a log only where that node leads it** (24 §14, a limit
stated there). With leadership placed by priority and moved by transfer, that limit is
met in ordinary operation and no longer only after a failure: a client on the local
socket of a node that follows is refused until leadership returns. A client over QUIC
is sent on to the leader. Serving a local client through the leader needs the node to
speak for that client to another node, which is a change to who a leader trusts; it is
an open decision and not made here.

**Multi-log synchronization.** MLRaft splits one log into n logs, each with its own
leader, and spreads the leaders with priority election and dynamic transfer. focal
already runs many groups per node, and has both of MLRaft's balancing mechanisms: the
spread through priorities when a session is placed, and a balancer that moves leaders
when the spread drifts. Cross-log order is not needed: focal's sessions are
independent ledgers.

A session's preferred leader is part of its committed placement, so how many sessions
prefer each node is a fact of the directory (`focal_directory::leading`; a session
that is moving counts where it is going). Nothing is decided by who happens to lead.

*When a session is placed or placed again* (`propose_placement_leading`) its preferred
leader is kept where it is still a candidate and moving would not help, and is
otherwise the candidate at home that the fewest sessions prefer. Moving a session from
a node preferred by `a` to one preferred by `b` leaves them at `a - 1` and `b + 1`,
which is no better unless `b + 2 <= a`. Every move by that rule lowers the sum of the
squares of what the nodes lead by two at least, so moves end and leadership comes to
rest however the sessions share their nodes.

*When the spread drifts* (`focal_directory::leader_move`, `leader_balancer`) the
controller moves the preferred leader of a session that holds its placement to another
of its voters by the same rule: one that is alive, eligible, reporting, of the enrolled
generation and at home; of the least led, one in the zone the session is led in; of
those, the least loaded. The move is a placement plan over the members the session
has. Every copy verifies its custody under the new route and the session is cut over,
as for any plan, so it is made for an imbalance that lasts:

| Rule | Value |
|---|---|
| Observed | the same move on 3 passes of the controller in a row, over `FOCAL_LEADER_BALANCE_HOLD_SECS` at least (30, a load report's interval) |
| One at a time | no move while another session of the partition moves its leader among the members it has |
| After a move | every count begins again: two moves are a hold apart, and each is decided on what the one before it left |
| Bound | 4096 sessions observed at once; one past it is not observed until another lapses, and is counted |
| Off | `FOCAL_LEADER_BALANCE=off`: the planner keeps every preferred leader and the controller moves none |

Leadership follows the placement once it is active: the replicas take their ranks
from it, and the one that leads hands over.

**Parallel vote replication and processing.** Votes and appends go to all peers at
once through P1's fan-out, and inbound replies are stepped as they arrive instead of
in send order. A group's owner thread stays the only writer of its state.

**Learners.** Present. Added with the new core: learners never count toward either
quorum and never vote, tested as slates tests it.

**Follower reads** (2026-09-29; the KIND campaign's D4). A linearizable native read
asked of a replica that does not lead is served by that replica: its core forwards
the read's barrier to the leader (`MsgReadIndex`, thesis §6.4), the answer names the
leader's commit index, the engine parks the barrier until this copy has applied
that index (bounded by the reads the core holds in flight, `DurableNode::pending_reads`),
and the page is read from the copy's own committed core (`read_at_least`). The
hosted `Session` parks the same way (2026-09-29, the audit's F55: the leader's
answer may reach a follower before the append that carries its index — QUIC
streams and separate exchanges owe no ordering between them — and that is
replication lag, never corruption; before this the hosted session failed closed on
it). The parked set is charged once for its bound; a copy at the bound refuses a
new read as `Capacity` at the request and drops, counted (`reads_dropped`), a
barrier it cannot hold rather than hold back the delivery that carries the very
entries the parked reads wait for; a parked barrier leaves the set only once it is
answered, so a retryable refusal loses none. A
follower that knows no leader refuses the read as before (`NotReady`, and the
client's bounded resends ride out the election). Before this a follower's operator
socket refused every such read and the client resent it for its whole 30 s ceiling
before reporting `unavailable` (`tests/follower_reads.rs`, three real processes:
written through the leader, read through each host). Reads therefore scale across
the voters of a log, and a copy's read is exactly as fresh as the leader's commit
index at the moment it asked.

## 6. Order of work

| Stage | Content | Exit evidence |
|---|---|---|
| A | P8 per-progress deadlines; P7 network model in `focal-sim` | fleet suites pass under injected CPU load |
| | *State 2026-09-28:* P7 in place with 32 tests. P8 in place for the suites named in section 3.3, and the run under injected load recorded ([09](09-implementation-status.md)): every core held busy, the fleet modules and ten binary suites pass, in the time they take on an idle machine. What the load does to an owner is measured (its longest period) and covered (its replica's patience). | |
| B | Priority elections wired; transfer on drain; P2 derived timing | election tests under LAN, regional and geographic profiles |
| | *State 2026-09-28:* wired for session groups, with `drain_leader` on real processes. Elections run over `focal_sim::path` at the three profiles (`sim_election_tests`): real replicas on real logs in virtual time, at the derived pace. | |
| C | P1 progress-aware fan-out; P5, P6 | dead-voter and straggler tests; no round waits out a dead peer |
| | *State 2026-09-28:* P5 and P6 in place (`focal_wire::Admission`; a retirement always closes its connection). P1: the round is in place (`focal_timing::RoundBudget`, `RoundWait`; `focal_wire::gather`; `PeerConnectionPool::exchange_tail`, `round_budget`) and session-fact signatures are collected by it. Custody replication, the obligation and the repair ask every copy at once, and the custody and seed pulls bring a replica as much at once as the path holds, apart from the exchange of facts and without a deadline of their own (section 7). Enrollment control and the contact announcement still ask the installed routes one after another, and that is what they are: one request that one owner may take, handed to the leader that is known and then to whom a refusal names. What they wait for each is its share of the round, or what an exchange with that peer was measured to take where that is more (`PeerConnectionPool::exchange_wait`); a round none of whose asks could be dialed, the pool's connections all taken by dials to the dead, says so (`Capacity`), and one any of whose asks left the node cannot know their outcome. | |
| D | `focal-raft` core: classic track at parity with raft-rs for focal's use | differential test against raft-rs over random schedules |
| | *State 2026-09-28:* built and in service under `DurableNode` (section 4.5). Five campaigns of schedules compare the two cores step for step; a run of 15,000 schedules compared 80.8 million steps and found them equal ([09](09-implementation-status.md)). Groups of both cores together, and of this core alone under schedules that also remove the leader, are safe and settle. Replication costs what it cost (`benches/replicate.rs`). | |
| E | Fast track in `focal-raft`; TLA+ model | section 4.4 invariants; latency measured against classic under 0 to 10% loss |
| | *State 2026-09-28:* built in the core and the shell (section 4.6), modelled and checked, and measured: a member that does not lead waits three quarters of what the classic track takes with nothing lost, and less of it as more is lost ([09](09-implementation-status.md)). No owner takes it yet; what it asks of one is in section 4.6. | |
| F | MLRaft leader balancer | leader spread converges; no transfer storms |
| | *State 2026-09-28:* built (section 5): leadership returns to the preferred leader and to its zone, preferred leaders are spread when sessions are placed and moved when the spread drifts. Three real processes whose sessions were all led by their founder spread them one each and every log is led where its placement prefers ([09](09-implementation-status.md)). | |
| G | P10 congestion measurement; decide | bake-off numbers recorded |
| | *State 2026-09-28:* measured and decided (section 7): Copa is the law of focal's connections, a stream is given a megabyte ahead of its reader, and what a group needs goes before what is asked, and that before content. | |

Each stage closes on the workspace gates and on CI for Linux, macOS and Windows.

## 7. The transport as measured (stage G)

focal's connections are quinn's. What is set on them is decided by measurement
(`crates/focal-wire/tests/congestion.rs`): two real endpoints, TLS and loss recovery
and pacing included, exchange over `focal_sim::path` in virtual time, through a
bottleneck of a stated rate with a drop-tail queue of one bandwidth-delay product in
each direction. One connection carries a transfer that sends as fast as it may and,
every 50 ms, an exchange of 200 bytes each way. A run is its seed.

**The rule, fixed before any run.** A law stalls in a scenario where its transfer
carries less than a tenth of what the best law carries there, where fewer than nine in
ten of its exchanges are answered, or where its connection closes. A law that stalls
anywhere is not chosen. Of the others the one whose exchanges' 99th percentile, as a
multiple of the best law's in each scenario, has the least geometric mean is preferred.
quinn's default is replaced only by a law preferred by a tenth at least whose transfer
carries nine tenths at least of what the default's carries, or where the default stalls.

**A connection that one lost datagram closed.** The first grid closed connections
under every law at 100 Mbit/s. quinn keeps what arrives out of order as the spans it
arrived in, and closes a connection whose stream holds more than 1,024 of them once it
has merged what it merges (`too many gaps in stream buffer`). Two causes, both closed:

| Cause | Where | Closed by |
|---|---|---|
| quinn-proto 0.11.17 never merged a span that filled its datagram, which is every span of a transfer. 2,048 of them behind one missing datagram closed the connection: any path that carries 2.4 MB in the time a loss takes to repair | quinn | quinn-proto 0.11.18, which merges them |
| A sender that overshoots in its first round trips loses every second datagram to the bottleneck's queue. With the 10 MiB of a frame as a stream's window there were more than 1,024 holes in one window, which no merging removes | focal's windows | `STREAM_WINDOW_CEILING`: a stream is given a megabyte ahead of what its reader has taken. A megabyte holds no more than 1,024 spans that are not merged, however it arrived |

A frame longer than the window is read as it arrives, so the window bounds what is in
flight and not what is sent. What it costs is stated by the grid: one stream carries a
megabyte in a round trip at most, 73% of 100 Mbit/s at 100 ms and 25% at 300 ms.
Transfers that need more go by several streams (below).

**The laws.** Twenty-seven paths of rate, round trip and loss, and three more (a deep
queue, bursts of loss, a link within a building), thirty virtual seconds each:

| Law | p99 of the best (geometric mean) | Carried of the best (geometric mean) | Stalled |
|---|---|---|---|
| NewReno | 1.367 | 0.399 | on 5 paths |
| CUBIC, quinn's default | 1.278 | 0.452 | on 5 paths |
| BBR | 2.140 | 0.958 | nowhere |
| Copa (δ = 1/2, stride 2) | 1.082 | 0.998 | nowhere |

NewReno and CUBIC take a loss for congestion. With one datagram in a thousand lost
they carry 93 to 96% of 10 Mbit/s at 20 ms, 45 to 50% at 100 ms, and 2 to 6% of
100 Mbit/s at 300 ms; with one in a hundred, 50 to 54% at best and under 1% at worst.
BBR carries nearly what Copa carries and fills the queue to do it: at 10 Mbit/s and
100 ms its exchanges take 604 ms at the 99th percentile, six round trips, where
Copa's take 113. Copa is chosen. It
is slates' law (`crates/transport/src/congestion/copa.rs`) behind quinn's interface;
what quinn does not let a law decide is the pacing, which stays quinn's. Where Copa is
the worse: on a path of 1 Mbit/s and 20 ms its exchanges take 134 ms at the 99th
percentile where CUBIC's take 78, since it keeps about two datagrams queued, each 10 ms
at that rate.

**Two things the law does otherwise than it was taken**, both found on paths that
hold megabytes in flight, where the law as taken carried 69% of 10 Mbit/s at 300 ms
and opened its window to three megabytes on a path that holds one:

| What | As taken | Here | Why |
|---|---|---|---|
| When slow start doubles | Once in a round trip, by the delay of what is acknowledged now | Once in a round trip, and only by what was sent after the doubling before it and a round trip more | What is acknowledged in the round trip after a doubling was sent before it: its delay says nothing of the window it is asked about, and a window doubled by it is doubled twice for one answer |
| How far a round trip moves the window | By the velocity, which doubles each round trip the window moves the same way | By the velocity, and by no more than half of what the window over δ holds in datagrams (`DEFAULT_STRIDE`) | A velocity that has doubled for ten round trips moves a window past what the path holds before the delay it causes is seen |

The stride was chosen by a rule fixed before the run (of the strides that carry, by
geometric mean, ninety-nine hundredths of what the best carries, the one whose
exchanges wait least), over the grid with a transfer by eight streams:

| Stride | p99 of the best (geometric mean) | Carried of the best (geometric mean) |
|---|---|---|
| 1 | 1.149 | 0.951 |
| 2 | 1.060 | 0.998 |
| 4 | 1.064 | 0.999 |
| 8 | 1.107 | 0.999 |
| 16 | 1.131 | 0.999 |

Copa carries 96.9% of 10 Mbit/s at 300 ms now, and 85.4% of 100 Mbit/s at 300 ms in
thirty seconds that begin with its slow start.

**A transfer by several streams.** A chunk of content is a megabyte at most and an
exchange of its own, so a transfer has as many streams as it has chunks under way
(`evidence_service::striped`). A copy takes the chunks of a manifest in any order: the
store holds each to the hash the manifest names it by, and the copy keeps a bit for each
chunk the manifest names, charged with the manifest. By how many streams a transfer goes is decided by what
the connection's law holds in flight, whenever a chunk is begun: one for every
megabyte of its window, and one (`focal_wire::bulk_width`). A window of less than a
megabyte is filled by one stream; and the law opens its window no further than what
is sent fills it, so the stream that is one more is what lets it find that the path
holds more. Measured over twenty virtual seconds, against transfers by a fixed number
of streams:

| Path | One stream | Four | Eight | By the window | Streams it came to |
|---|---|---|---|---|---|
| 1 Mbit/s, 100 ms | 93.3% | 93.4% | 93.2% | 93.3% | 1 |
| 10 Mbit/s, 100 ms | 96.9% | 96.3% | 96.9% | 96.9% | 1 |
| 100 Mbit/s, 1 ms | 96.5% | 96.5% | 96.5% | 96.5% | 1 |
| 100 Mbit/s, 100 ms | 73.4% | 92.2% | 92.6% | 95.2% | 2 |
| 100 Mbit/s, 300 ms | 24.5% | 81.3% | 81.1% | 84.2% | 4 |
| 1 Gbit/s, 100 ms | 7.4% | 29.5% | 59.0% | 81.1% | 11 |

A path that carries a megabit in a second has one megabyte on it at a time, which it
carries in eight seconds; eight would each take a minute. The last row reached eleven
of the thirteen streams the lane holds: the window the law had reached in twenty
seconds, not the path.

Content has a lane of its own to each peer (`PeerConnectionPool::bulk_lane`): the
streams of a connection that nothing else can have in flight, which are all of them but
those of what is asked of the peer (`per_peer_inflight`) and of its probe. Each stream
has the megabyte of its own window within the connection's, so what a group sends a
peer is never refused for content. Content that finds the lane full waits its turn, in
the order it came, so transfers to one peer share it; what waits and what is in flight
are counted together and bounded (`focal_peer_content_inflight`). What the copy has room
for is found as a sender finds what a path carries (RFC 5681 §3.1): a chunk refused the
room while another is in flight is sent again and the transfer keeps half as many in
flight; from then on one more for every time as many were answered as it keeps. A
transfer all of whose chunks were refused, none answered between them, is refused. A
copy of a release that takes chunks in their order only refuses one that came ahead,
and is sent them one after another from the first.

**The lanes of a connection are derived (2026-09-29).** A connection had sixteen
streams, two of them for what a group asks of the peer, and the pool let two exchanges
to a peer be in flight for all the groups the two nodes share, while the core keeps a
hundred and twenty-eight messages in flight to a follower (`NodeConfig::max_inflight_messages`,
the pipeline of the thesis's §10.2.1): the pipeline on the wire was two, and a message
that found the lane full was refused (`Busy`), counted by the driver and dropped, so the
leader learned of the gap only from the follower's next answer. The streams are derived
now (`WireLimits::for_consensus`, `PeerPoolLimits::for_consensus`): one for the probe,
the consensus window for what groups ask of the peer (`DEFAULT_INFLIGHT_WINDOW`, 128;
a window derived from the path will follow, section 8.5 C6), and for content what the
reference path holds in flight a stream's window at a time and one (`content_streams`:
1 Gbit/s at 100 ms is twelve megabytes, thirteen streams) — 142, and the pool's lanes
follow: the window to each peer, and the window for every connection it may open in
all. A message of a group that finds its lane full waits its turn as content does, for
as long as its exchange has (`PeerPoolLimits::timeout`), and is refused past that,
which the driver counts (`focal_peer_messages_busy_total`); one to a peer the pool
could not reach at all is told to the frame's owner, which reports the peer to its
core (`Session::report_unreachable`), so the leader probes the member instead of
streaming to it, counted per session (`focal_session_peers_unreachable_total`).

**How long an exchange waits.** An exchange was given a time: five seconds by the
pool, thirty by a connection. A megabyte needs 1.7 Mbit/s for the first and 0.28 for
the second, and a path that carries less carried no content at all. The parts of an
exchange have waits of their own now, each charged to what it waits on
(`transport::carried`, `frame::read_payload_arriving`):

| Part | Waits until | Given up when |
|---|---|---|
| What is sent, and the peer's answer to it | The answer begins | A period ends in which the connection sent less than a datagram; or a period ends that began when the connection had sent all that its exchanges had to send: the peer had the request, and a period to answer |
| What arrives | Its last byte | A period brings neither its end nor a datagram more of it |
| The handler | It answers | The time of a request, as before |

The period is the time the exchange was given before. What a connection sent is what
it sent and has not found lost. So a megabyte crosses a path of 4 Mbit/s in both
directions between endpoints that give a request one second
(`narrow_path_carries_a_megabyte_that_takes_longer_than_a_request_is_given`, through a
relay that carries that and drops what finds 32 datagrams waiting), and a peer that
takes a request and does not answer is given up in one period or two on any path.
Every period but the last moves a datagram of a bounded frame: the wait ends.

**Classes** (`focal_wire::TrafficClass`, `Operation::class`). A stream of a higher
priority sends all it has before one of a lower sends anything. Consensus, probes and
the fleet's control go first, then what participants ask, then content. Before, a
consensus message went before everything else and everything else took turns:

| Path | Beside one transfer, p99 | Beside eight, in turn | Beside eight, by class |
|---|---|---|---|
| 1 Mbit/s, 100 ms | 205.0 ms | 260.3 ms | 170.9 ms |
| 10 Mbit/s, 20 ms | 28.0 ms | 34.6 ms | 26.8 ms |
| 100 Mbit/s, 1 ms | 2.1 ms | 2.7 ms | 2.0 ms |

A class orders what a sender has not sent; what is on the path already is kept short
by the law.

**What slates has beyond this, and why it is not taken.** Its measured gains of the
last week (loopback p90 from 1,311 to 30 µs, a directory listing from 782 to 6.5 ms)
were defects of its own executor and transport, which tokio and quinn do not have.
Credit that rides acknowledgements, bounded probe copies and its path MTU search are
parts of its own transport; quinn has delayed acknowledgements, MTU discovery,
segmentation offload, key update and migration, which slates lacks. Its consensus core
has not changed since what focal took from it.

## 8. Where the plan stands, and slates examined again (2026-09-28)

### 8.1 What was planned, and the evidence for each

| Planned | State | Where | Evidence |
|---|---|---|---|
| Fast Raft | Built in the core and the shell; no owner takes it yet (section 4.6) | `focal_raft::{fast, track}`, `DurableNode::propose_fast` | `tests/fast.rs` under schedules; `docs/models/FastTrack.tla` checked in CI; latency against the classic track, 0 to 10% loss |
| Pre-vote | In service | `focal_raft::raft`, `Config::pre_vote` | compared with raft-rs step for step; `sim_election_tests` over three path profiles |
| Priority elections | In service, three ranks | `fleet::{PREFERRED_LEADER_PRIORITY, ZONE_PRIORITY, VOTER_PRIORITY}` | `raft_safety_tests`; `fleet_leader_return_tests` |
| Parallel vote replication and processing | In service for what a group decides by | `focal_wire::gather`, `focal_timing::RoundBudget` | round tests; dead-voter and straggler tests. Content is asked of every copy at once and goes by as many streams as the path holds (section 7) |
| Learners | In service | `focal_raft::configuration`, `progress` | differential campaigns with changes; the placement suites |
| Multi-log synchronization | In service | `leader_return`, `focal_directory::leading`, `leader_balancer`, `seats` | `tests/leader_balance.rs` and `tests/home_balance.rs`, real processes |
| Leader transfer | In service | `focal_raft::raft`, `ReplicaHost::transfer_leader` | differential campaigns; `drain_leader`; a transfer that reaches a member asking for votes (section 4.5) |

### 8.2 slates, by what it has

Examined in its code, its records of defects and its measurements, three times over:
its transport, its consensus and fleet, and three questions asked of its code.

| slates has | focal | Why |
|---|---|---|
| Copa | Taken | Section 7 |
| Classes of traffic, strict priority | Taken | Section 7 |
| A collector that reads what is queued before it expires | Had it | `gather` reads a reply in hand before it judges the wait |
| Test ports held from claim to use | Had it | `tests/support/ports.rs` claims across processes, outside the ephemeral range |
| A council seated by incumbency and liveness | Taken, and split | Section 5, seats |
| A death held for one election window | Taken | Section 5, seats |
| Progress-aware fan-out, derived timing, period-counted timers, admission by certificate, link validity inside a wait, a modelled network, waits charged to progress | Taken before | Section 3.1, P1 to P8 |
| Its own transport: packet and frame format, handshake, loss recovery, pacing, credit that rides acknowledgements, path MTU search | Not taken | quinn has each of them, and delayed acknowledgements, segmentation offload, key update and migration, which slates lacks |
| Its executor: shards, io_uring, timer wheel, wake estimate | Not taken | tokio. The gains slates measured there (loopback p90 from 1,311 to 30 µs) repaired defects of its own |
| Its consensus core | Not taken | Unchanged since what focal took; configuration only, without transfer, priority or a fast track |

### 8.3 What slates lacks at the scale both must reach

Found in its code, and stated here because the two projects are held to one bar. None
of it was changed.

| Finding | At one host | Across the planet |
|---|---|---|
| An object is one exchange on one stream, handed on when its last byte has arrived | Nothing to see | One lost datagram stalls the object |
| The credit of a connection is one number for every class, asked for before a class is chosen | Nothing to see | A hole in a transfer that has filled the window stops consensus and probes on that session until it is filled |
| The receive window's ceiling is about 135 KB whatever the memory, since a floor on the number of peers decides it | Far above what loopback needs | 3.6% of what 100 Mbit/s at 300 ms holds in flight: a session carries 3.6 Mbit/s |
| Its measurements set the window to eight times the path | | What they report is not what a daemon reaches |
| An object is owned where it was created, until that host dies; then by a survivor chosen by hash | The only host | Every write from elsewhere crosses the long path; nothing moves an owner to its writers or spreads owners |
| No migration, no path validation, no keep-alive, no key update | Nothing to see | A laptop that changes networks, or a NAT that forgets, ends the session |
| Copa carries 0.795 of 100 Mbit/s at 100 ms and 0.496 at 300 ms | | focal measured the same law at 69% of 10 Mbit/s at 300 ms. With slow start judged by what was sent after a doubling and the stride bounded it carries 96.9% there (section 7); slates' law is as it was |
| An exchange is given a time, whatever it carries | Nothing to see | A path that carries less than the object in that time carries none of it. focal had the same defect and waits on what the connection sent now (section 7) |

### 8.4 What is open in focal

| Open | Why it matters |
|---|---|
| The streams of a connection, sixteen by default (`WireLimits::streams_per_connection`) | Done (2026-09-29): derived from the consensus window and the reference path (`WireLimits::for_consensus`, section 7), and a group's message waits its turn on its lane, bounded by its exchange's time, instead of being refused; a peer the pool could not reach at all is told to the core, which probes it |
| The pool's deadline of five seconds for what is not content, the announcement's round of five and enrollment control's of four | Set, not derived. An exchange of a group with a peer further than that is not made; no path on this planet is, but a peer under load may be |
| The request time an owner gives what it holds (`request_timeout`, five seconds) | Set, not derived; counted in the owner's periods now, so a loaded machine stretches it, but a follower whose owner stalls for longer than the leader's request time is not seen by the leader, whose own periods run on time. Under eight and sixteen copies of the control suite at once this is what remains (five of eight runs, none of sixteen): a leader whose term entry the stalled followers do not acknowledge in time answers `NotReady` until leadership has moved again. The request time should follow the exchange tails of the voters (`PeerConnectionPool::exchange_tail`), which a stalled follower stretches and the leader's own stall does not |
| The control suite's single asks inside a request deadline of 350 ms | Done, at the cause: the owner gave a request 350 ms of the clock while a loaded machine slowed its rounds, so every ask timed out. A request now waits its time in the owner's own periods (`ControlHost`, `Pending::deadline`), and every ask of the suite that expects an answer waits for a definite one, charged to the hosts' periods (`Rig::definite`, `read_on_leader`); eight copies of the suite at once pass |
| A restore cut where it records its copy | Cut at three places and issued again on real processes (`runbook_interrupted_restore`); the cut between the copy's record and its attachment is covered by the record alone |
| A voter that dies and returns within the hold, end to end | Done: `cluster plan` says for how many seconds a death still stands (`focal_directory::deaths_stand_for`), and a voter that returns within the hold keeps its seat on real processes while a spare waits; one that stays dead loses it to the spare without an operator (`runbook_node_loss_within_the_hold_moves_no_seat`) |
| The fast track for an owner | A receipt states its entry's term: a durable format, and a decision |
