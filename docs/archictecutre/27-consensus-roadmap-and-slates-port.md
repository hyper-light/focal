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
`RawNode`; from then it was `focal-raft` (section 4.5), and since 2026-10-03 it is that
core moved to the shared repository, hyper-raft (section 14), whose log and messages
focal keeps in raft-rs's encoding. The table states what raft-rs gave and what focal ran when this
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
`scripts/check-model.sh` in CI, every configuration within the states it states, with
the rules the core does not follow beside it, which the checker must refuse;
section 4.6), tests over the sans-io core under seeded schedules
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
| A leader sends a round of heartbeats for each read as it is asked, and again each time the read is asked again: twenty reads taken together are forty heartbeats to two members, of which the answers to the last two confirm all twenty | One round when the member is next asked what there is to do, carrying the last read asked, for every read asked since the round before (`ReadRounds::Shared`, section 10). A read asked again while it waits asks for no round of its own; a round that was lost is asked again by the leader's clock, whose heartbeat carries the last read. The rule of raft-rs is kept (`ReadRounds::Each`) to compare the cores under one rule |

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
nothing. That argument takes the later leader's log to end below the index, and the
members that elect it to count by the configuration the fast quorum was counted in.
Neither held as first built; the two rules that follow make them hold.

**A vote counts once the voter's log is of the leader's term (2026-10-01).** As first
built an election could commit a second entry at an index that held a committed one.
Forty thousand schedules of `a_group_with_the_fast_track_is_safe_and_settles` met it
(seed 9843 from seed 3,000, on the commit before any change of that date; ninety-six
and three thousand did not): the leader of term 3 committed index 32 by the fast
quorum {1, 2, 4} of four voters, of whom 2 and 4 held the entry beside their logs and
had told the leader nothing of their logs; member 3's log held an entry of an older
term at 32; 2 and 4, whose logs were no more current than its own, elected it; one
that is elected takes the entry most held only above its own log, so it kept its
entry and committed it. The cause is that a fast quorum's members vote for the entry
in the leader's round and for a candidate by their logs, and nothing an election
reads recorded the first. Fast Paxos chooses a value in a round only by votes cast in
that round, and its recovery reads each acceptor's round of its last vote, which the
acceptor keeps durable (Lamport, "Fast Paxos", Distributed Computing 19(2), 2006,
§3.3, condition O4); Raft counts replicas only for an entry of the leader's own term
for the same reason (Ongaro's thesis, §3.6.2). Fast Raft's proof (Castiglia, Goldberg
and Patterson, Lemma 2) shows that a follower never overwrites a chosen entry and
that a new leader inserts the most-voted one, and says nothing of a leader elected
with a leader-approved entry of an older term at the index: the paper's rule has the
defect too.

The rule: a member that holds the entry beside its log counts toward a fast quorum
only once the leader knows its log holds an entry of the leader's term
(`log.term(progress.matched) == term`, `Raft::fast_commit`). Every member of R then
has a last term at or above the leader's and keeps it, because what it holds through
the leader's first entry of the term a majority holds (R is one), so no later leader
truncates below it. By the classic rule each refuses a candidate whose last term is
older. A candidate whose last term is the leader's or later holds, through its last
entry, the log of a leader that holds the committed entry: it holds the entry, or its
log ends below the index and the argument above applies. The round is recorded where
elections read it. It adds no message, no field and no durable state: the leader's
first entry of its term reaches a member with its first append. Considered and not
taken: a voter that refuses a candidate whose log is older than its own latest fast
vote (the vote's term must be durable, and one term for the whole log does not
protect an index: a candidate stale at the committed index can have voted later
entries to the same leader); and holding the term with every entry beside the log
and weighing it at every index above the candidate's commit (a durable write each
time a held entry is voted to a new leader, a candidate that truncates its own log,
and a term that is only a lower bound of the round the entry was accepted in).

**A fast quorum is one of every configuration a member may count by (2026-10-01).**
With the first rule in, forty thousand schedules from seed 43,000 and from 200,000
each failed once (seeds 54104 and 203544). A leader had applied a change that made a
voter a learner and committed an index by three of its four voters; one of the three
had not heard the change committed and counted by the five voters before it (a
member campaigns by the configuration it has applied); it was elected by itself and
two of the five that held another entry, the most held among them. Majorities of two
configurations one change apart meet, which is all the classic track needs; a fast
quorum of the new one need not be a fast quorum of the old, and the guard below
(applied, not joint, at the leader) says nothing of the members. A member that took
an entry of the leader's term took the leader's commit with it, which covers the
configuration the leader was elected under, and campaigns only once it has applied
every change it committed (`Raft::hup`): it counts by that configuration or by one
the leader applied since. One that took no entry of the term has an older last term
than every member of R, which refuse it, and no majority of a configuration one
change away is without them. So a leader notes the voters it was elected under and
the one other set of voters a change in its term named (the two halves of a joint
configuration are the two sets), and counts a fast quorum only where it is one of
each (`Raft::note_term_configuration`, `note_term_change`,
`fast_quorum_of_the_term`). A change that names a third set leaves it to the classic
quorum until its term ends.

This takes a member to write an entry and the commit it took with it in one write,
and to apply a change only on a commit its log holds. focal's shell does both: the
hard state follows the entries in the same batch (`persistence.rs`), and a change of
the configuration waits for the write that states its commit (section 9).

**What the rules cost.** A fast commit that counted a member whose log was not yet of
the leader's term becomes a classic one, a round later: those at a new leader's first
indexes, chiefly what it recovered at its election, which Fast Paxos also commits by
a classic round. After a change, a fast quorum must also be one of the voters before
it; after a second change in one term there is none until the next term.

| Fast schedules | Fast commits before | With both rules |
|---|---|---|
| 96 from seed 0 (an ordinary run) | 418 | 188 |
| 40,000 from seed 3,000 | fails at seed 9843 | 93,864 |

The schedules change leaders and configurations far more often than a group in
service does (five terms and many changes in 4,000 steps); a group with one leader
and no change commits every proposal by the fast quorum as before.

**Open before any owner takes it.** A leader that outlives two changes of its
configuration has no fast track for the rest of its term, and terms in service are
long. A set of voters can be dropped once no member can still count by it, which is
when every member of it has applied the change after it; a leader knows what a member
holds and not what it has applied, so that takes an answer that says so, and the
model a configuration per member. It is not built. No owner sets `NodeConfig::fast`.

**Evidence.** `an_election_never_commits_a_second_entry_at_a_committed_index` runs
seed 9843's schedule under the rules it was found under and
`a_member_that_counts_by_the_configuration_before_commits_no_second_entry` runs seed
54104's; each fails without its rule. 160,000 schedules of 4,000 steps pass, 40,000
from each of the seeds 3,000, 43,000, 100,000 and 200,000 (`FOCAL_RAFT_SEEDS=40000
FOCAL_RAFT_SEED=<seed> cargo test -p focal-raft --release --test fast
a_group_with_the_fast_track`), with 373,536 fast commits among 11,967,015 entries.
The classic track is untouched: 3,000 schedules of the group and of the comparison
with raft-rs pass as before.


**The model.** `docs/models/FastTrack.tla` missed the run twice over. It had no step
by which a leader that was deposed campaigns again with the log it led with, so no
member whose log held what no other took was ever elected, at any bound; and its five
voters ran two terms where the run takes three. It has the step and the rule now
(`OfTheRound`). Without the rule the checker refuses it: four voters over three terms
reach a leader that lacks what was committed (`FastTrackAnyRound.cfg`, in 190,662
states); with it the same four voters hold every property in all 3,207,204 states
(`FastTrackFour.cfg`). With the rule one index shows nothing of the fast quorum — a
member whose log is of the leader's term holds the index from the leader — so three
voters are checked over two indexes (`FastTrackRound.cfg`, 2,462,010 states), where
the checker must also find an index committed by what members hold by themselves
(`FastTrackReached.cfg`), or the configuration would check nothing of it. Four voters
over two indexes, three voters over three terms and two indexes, and five voters are
each more than twenty million states and are not visited (five voters at one index
ran nightly, for hours, and with the rule that index shows nothing of the fast
quorum); the schedules of the core are what cover them. The model has no change of configuration,
so the second rule rests on its argument, its directed test and the schedules.

**The checker is bounded like everything else.** As first changed, the model at two
indexes did not end: 208 million states, and 26 GB of them on disk, in 87 minutes,
under a JVM free to take half the machine's memory. Three things changed. An election
is one step of the model, where it was four and more, each interleaved with every
other step; a member says what it holds when it comes to hold it and with its vote,
and the separate record of what it held when it voted is gone. Both keep every run, up
to the order of steps that do not touch each other (the model's header says why).
Three voters at three terms and one index were 1,219,562 states before the model had
the step a deposed leader takes and are 560,563 with it, and the rule one that is
elected does not follow is refused in 89,337 states where it took 26,212,234. A
configuration states how many distinct states it has, and the checker stops at one
more (`StateBudget`, `WithinBudget`); one that passes must have exactly that many, so
a change that makes a model larger or smaller is refused until its states are counted
and stated again. And `scripts/check-model.sh` gives the checker 256 MB of heap and as
much again beside it, which is what the largest configuration was measured to need
(404 MB resident, and no faster with four times that), one thread unless it is told
of more, and removes a run's states however the run ends.

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

A control replica — the root's, a directory partition's — serves its followers'
reads the same way since 2026-10-02 (`ControlReplica::read_index`, a follower's
read through its leader), and parks the same way since 2026-10-03: a barrier
answered above what it has applied is held until the entries it names are applied
(`ControlReplica::{park_ahead, release_parked}`), its bytes charged while held and
handed to the delivery that lets it go, no more than the reads the core holds in
flight; a replica at that bound refuses a new read as `Capacity`, and one dropped
there is counted (`reads_dropped`). Before, the control replica took such a barrier
for corruption: a member brought up by snapshot that read before it caught up
failed, and with it the hosted partition it served (the split-and-merge test's
failures on ubuntu CI, 2026-10-02 and 2026-10-03, named once the replica kept the
error that stopped it).

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
the second, and a path that carries less carried no content at all. Content was given
waits of its own for its parts on 2026-09-28; what a group sends a peer kept the pool's
five seconds for the whole of an exchange until the audit's F36, and the waits were
charged to what the connection sent until its F38. Every exchange has these parts now,
and each its own wait (`PeerConnectionPool::send_bounded`, `transport::Carriage`,
`frame::read_payload_arriving`):

| Part | Waits until | Given up when |
|---|---|---|
| Its turn on the peer's lane, and its connection | It has them | The pool's time for each (`PeerPoolLimits::timeout`). The dial goes on without the caller, for the next: it is given what the connector gives a handshake (`request_timeout`, for the connection's and for the protocol's) |
| What is sent | The peer has acknowledged the whole of its stream, or has answered | The residency of what the connection held to send beside it is spent; never before a period |
| The peer's answer | It begins | A period after the peer had the request, and what the path is given to carry the first of it |
| What arrives | Its last byte | It stopped arriving: a judgement (a period, or the probe timeout of the longest round trip seen) brought less than a datagram of the connection's bytes, or began with everything the peer owed of its class and the less urgent ones delivered and the body still not among it (`frame::Arriving`, 2026-10-02) |
| The handler | It answers | The time of a request, as before |

The period is the pool's time. A group's exchange that ends by time is not asked again
by the pool: its owner asks again by its own clock, and a second time would hold the
peer's lane as long again.

**What arrives is charged with what arrives (2026-10-02; found by hyper-raft's port of
this law, ca8d44f).** A body was given its residency — its bytes at two datagrams of the
least size a probe timeout of the longest round trip the path showed while it arrived,
and a period at least. That is the pace of a sender limited by its path alone. A peer
whose owner writes a body as it has it, whose exchanges share the connection under
strict priority, or that is short of CPU, sends slower than its path, on a path whose
round trip says nothing of when the body ends: at the 395 µs QUIC measures on loopback
an 8 MiB body got one period, and the first judgement after it refused the body however
much was arriving (under a CPU quota, 7 of 15 runs at half a core, 25 of 30 at a fifth;
the rare refusals CI saw on ubuntu-24.04 and windows-11-arm). A body's arrival is now
judged once a period — the peer's time, or the probe timeout of the longest round trip
seen, whichever is longer, since a lost flight is sent again when the probe timer ends —
and given up when a judgement brought less than a datagram of the connection's bytes
(silence), or began with everything the peer owed delivered and the body still not among
it: the wait is charged with the stream bytes the connection delivered of the body's
class and the less urgent ones (`TrafficClass::rank`; a request's body, whose class is in
it, counts as the most urgent) against what the peer declared of them and has still to
deliver, this body included (`frame::Delivery`, one per connection on either role). A
peer that withholds a body while it sends others is given up once it has sent everything
it owed; one that sends nothing, within a judgement. What a slow body holds is bounded by
the identity's share of the listener's ingress (F03), not by time.

**What a sender knows of its own stream** is one thing: that the peer has acknowledged
all of it, or stopped taking it (`SendStream::stopped`). What the connection sent is in
flight, sent again, or another stream's. Charged with it, an exchange was given its
peer's period to answer while its last window was still on the path: sixty-four
kilobytes over eight kilobits a second were given up at sixty seconds, twelve periods
to the second, five seconds before they had arrived. And a stream its peer had stopped
reading was kept for as long as the other exchanges of its connection moved a datagram
a period: two megabytes, twenty-five seconds, where it ends in half a second now. What
a stream takes of what is written to it says little either: a window at once, and more
only as the peer's reading lets it, an eighth of a window at a time, which on a narrow
path is longer than any period (a megabyte over sixty-four kilobits was given up ten
seconds in by that rule, tried and taken out). So between the last byte written and
the acknowledgment the wait ends by a bound and not by a guess at delivery.

**The bound is the residency** (`frame::residency`): what the bytes take at the least
a live sender delivers, two datagrams of the least size in a probe timeout, at the
longest round trip the path has shown. A sender whose window is as small as QUIC keeps
it, and whose every flight must be asked for again, sends two datagrams when its probe
timer ends (RFC 9002 §6.2.4, §7.2, §7.5), and the timer is three round trips with the
variance a single sample is given (§5.3, §6.2.1) and the acknowledgement delay the peer
may take (`frame::MAX_ACK_DELAY`, the transport parameter's 25 ms every connection here
advertises: on a path of a millisecond a loss is recovered no sooner, which a timer of
three milliseconds missed — found by the ordered fleet of §12, 2026-10-03). It was two
datagrams a round trip,
which is a path's best with that window and not its least: 256 kilobytes over eight
kilobits a second took 262 seconds and were given 225. A receiver holds a payload to
the same bound and to nothing else. It also gave a payload up when a period brought
less than a datagram of it, which asked of every path a datagram a period and of every
sender that it lose nothing: sixteen kilobytes arriving a datagram every 2.6 s were
given up five seconds into the silence after a lost flight, with 175 s of their
residency left. A sender that stops for good is given up when the residency is spent,
one whose connection carries nothing when the connection ends, and a connection ends
when its path is silent for ten seconds (`IDLE_TIMEOUT`).

**What a path must carry**, then, is what a connection needs to live: a datagram of the
least size returned within the idle timeout, 1,920 bits a second. Below that no
connection is made, and every caller is told so in the pool's time; above it a message
of any size the frame allows is carried in the time the path takes. Measured through a
relay that carries so many bits a second each way with 50 ms of round trip, the pool
as a node configures it sending a message of a group and then sending it again
(`slow_paths_measured`, `adverse_paths_measured`, `narrow_lossy_paths_measured`):

| Path | 64 B | 4 KiB | 64 KiB | 1 MiB |
|---|---|---|---|---|
| 100 bit/s, 1 kbit/s | `Lost`, `Lost` | `Lost`, `Lost` | `Lost`, `Lost` | `Lost`, `Lost` |
| 8 kbit/s | `Lost`, then 5.0 s | `Lost`, 9.1 s | `Lost`, 75.0 s | 1,078 s on a connection that is open |
| 64 kbit/s | 1.0 s, then 0.2 s | 1.6 s, 0.7 s | 9.8 s, 8.5 s | 136.7 s, 135.5 s |
| 256 kbit/s | 0.4 s, then 0.1 s | 0.5 s, 0.2 s | 2.7 s, 2.2 s | 35.6 s, 35.4 s |

The first send has no connection. Where the dial takes longer than the pool's five
seconds (8.4 s at 8 kbit/s) it is told `Lost` at five, and the second has the
connection when the dial is done. Before, nothing was delivered at 8 kbit/s (a dial was
given the callers' five seconds and begun again from nothing by the next), and nothing
that takes its path more than five seconds at any rate: 64 KiB at 64 kbit/s, a megabyte
at 256. At 64 and 256 kbit/s, 4, 64 and 256 KiB are delivered with two and ten percent
of datagrams lost, with 50 ms of jitter, with eight kilobits a second in either direction
against the other, and across a path that carries nothing for three seconds or for
eight; one that carries nothing for fifteen ends the connection, the message is `Lost`
and the next is delivered over a new one. At 4, 8 and 16 kbit/s, sixteen and sixty-four
kilobytes sent or asked for are answered at none, two and ten percent loss, thirty-six
cases of which fourteen failed before the receiver's rule changed.

**A chunk goes in parts where its path is slow (2026-10-02, the audit's F49).** A
transfer's lease on a copy (`CustodyConfig::transfer_ttl`, 60 s) was renewed only when a
request for it was executed, and a request's body is read before it is executed: a
megabyte chunk, the unit of custody and what a manifest hashes, takes 131 s to cross
64 kbit/s, so it arrived to a transfer that had expired. The sender sizes a part to what
the path carries in the time the pool gives an exchange, at the rate the law holds in
flight over the round trip (`PeerConnectionPool::part_bytes`; a datagram at least, the
chunk at most, the whole chunk where the path is not yet measured), and sends a chunk in
parts (`ChunkPart`; `ReadChunkPart` for a pull), each a request that crosses within an
exchange's time and renews the lease as it is taken. The receiver stages a chunk's parts
in order beside its objects, promised to the volume part by part, verifies the whole
against the manifest's hash and installs it under the same name
(`ContentStore::import_chunk_part`): a chunk made of parts is the chunk. A transfer
that expires or is cancelled discards what it staged. The lease is thereby tied to
admitted progress and covers twelve crossings; the sixty seconds themselves are set,
not derived (section 8.4). A client that gives up its exchange before the copies hold
the object asks again, exactly; the coordinator ran the exact retry's job beside the
first, two pushes of one transfer each sending every part, and runs it after the first
now, where it finds the copy holds the object. Measured in process over real QUIC, each
replica behind a relay: at 128 kbit/s a megabyte and seven bytes is sealed on its copy
75.9 s after the upload began (the path alone takes 65 s), 1,152,399 bytes crossing
toward the copy in 87 parts; at 64 kbit/s, 152.9 s and 1,193,907 bytes (131 s); beside the first, the retry
made it 1,970,721 bytes and 124 s.

**Discovery of the native decoder's support has no clock of its own (2026-10-02, the
audit's F48).** The exchange of durable promises that native activation and every
promotion need gave each of its parts 250 ms — the replica's own fact, the peer's, the
recording — so a healthy path further than that never contributed one. Each part is
given what it takes: the replica's owner its periods, the peer its path; discoveries of
different ledgers run at once, as many as one lane to a peer holds, bounded by their
charges; a ledger is asked again a period of its owner after its last, and the loop
waits for the fleet to change when none is due (`managed_support::support`). On real
processes behind relays of 300, 600 and 1,200 ms round trip, activation, two promotions
and a claim settle (`native_support_across_latency`); with the ceilings, the 1,200 ms
fleet never settles.

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

**A queue manager's mark (the audit's F39, 2026-10-03).** quinn sends its datagrams
ECT(0) where the path keeps the field and tells the law of a mark as a congestion event
with no bytes lost (`Connection::process_ecn`); Copa took it for a loss, which in its
default mode is no signal, so a mark changed nothing. A mark is never noise — a queue
manager judged its queue too long — and a sender of ECT(0) answers it as congestion (RFC
3168 §5, RFC 9002 §7.1). Copa now does (`Copa::on_mark`): once a round trip (RFC 9002
§7.3.2) slow start ends, `1/δ` halves while Copa competes, and the window halves
(`DEFAULT_MARK_BACKOFF`, RFC 9002 §B.2's `kLossReductionFactor`). For ten seconds after a
mark past slow start the window grows as a classic sender's, a datagram a round trip, in
either mode: a queue manager keeps the queue short for every sender, so Copa does not see
the classic senders that fill it to the manager's target — their marks are what it sees
of them — and Copa's own step after each mark took back the share they regrow a datagram
a round trip. While it grows so, competing raises `1/δ` only after a round trip in which
the target held the window back: beside a target raised a packet a round trip, a window
growing a datagram a round trip never reaches it, never falls, and never empties the
queue Copa alone keeps, and Copa alone, misjudging itself competing, filled CoDel's queue
to its target. Marks round trip after round trip halve the window each round trip to the
least: the response to persistent marking.

The harness carries the ECN field on the paths that say so (`Scenario::ecn`; the grid
above is of paths that bleach it, and its numbers are unchanged — the flows refactor runs
one flow exactly as before), its bottleneck marks by a step at one datagram (DCTCP's
threshold, RFC 8257 §3.1) or by CoDel's defaults (RFC 8289: 5 ms over 100 ms;
`focal_sim::path::Marking`, CoDel evaluated at each datagram's dequeue time), and it runs
flows side by side through one bottleneck (`compete`). Copa alone, 30 s, against itself
without a manager and NewReno under the same manager:

| Path | Manager | Copa queue p99, ms (without manager) | Copa carried | NewReno carried |
|---|---|---|---|---|
| 1 Mbit/s, 20 ms | step | 19.20 (27.71) | 57.5% | 50.4% |
| 1 Mbit/s, 20 ms | CoDel | 27.71 (27.71) | 91.5% | 90.4% |
| 1 Mbit/s, 100 ms | step | 19.20 (80.47) | 16.5% | 16.1% |
| 1 Mbit/s, 100 ms | CoDel | 34.11 (80.47) | 80.4% | 76.6% |
| 10 Mbit/s, 20 ms | step | 1.92 (12.61) | 11.5% | 8.9% |
| 10 Mbit/s, 20 ms | CoDel | 12.61 (12.61) | 96.0% | 89.8% |
| 10 Mbit/s, 100 ms | step | 1.92 (11.33) | 2.1% | 1.8% |
| 10 Mbit/s, 100 ms | CoDel | 7.49 (11.33) | 82.0% | 77.6% |
| 100 Mbit/s, 20 ms | step | 0.19 (0.73) | 1.2% | 1.0% |
| 100 Mbit/s, 20 ms | CoDel | 0.73 (0.73) | 97.1% | 87.2% |
| 100 Mbit/s, 100 ms | step | 0.19 (7.30) | 0.2% | 0.2% |
| 100 Mbit/s, 100 ms | CoDel | 7.30 (7.30) | 73.4% | 73.5% |

In no scenario is the managed queue longer than Copa keeps without a manager; by
geometric mean it is 0.377 of it, and Copa carries 1.108 times what NewReno carries under
the same manager. (Under the one-datagram step, the threshold of DCTCP's senders of
ECT(1), every classic sender starves, NewReno with it.) Beside NewReno and CUBIC under
CoDel, the incumbent's share over its bar — what it carries beside its own kind or CUBIC,
the worse-off of each pair (Ware, Mukerjee, Seshan and Sherry, HotNets 2019) — by
geometric mean over eight seeds, each seed's bar its own:

| Path | NewReno beside Copa (bar) | of its bar | CUBIC beside Copa (bar) | of its bar |
|---|---|---|---|---|
| 1 Mbit/s, 100 ms | 37.3% (37.0%) | 1.008 | 40.8% (40.3%) | 1.014 |
| 10 Mbit/s, 20 ms | 60.9% (45.0%) | 1.354 | 57.2% (47.5%) | 1.203 |
| 10 Mbit/s, 100 ms | 44.8% (36.2%) | 1.233 | 52.7% (39.8%) | 1.325 |
| 100 Mbit/s, 20 ms | 63.5% (44.0%) | 1.445 | 65.3% (42.4%) | 1.539 |

By geometric mean NewReno carries 1.249 of its bar beside Copa and CUBIC 1.256. The
backoff, over the four paths (alone: geometric means over both managers):

| Backoff | Alone: queue of unmanaged | Alone: carried of NewReno | Least of the bar under CoDel, NewReno / CUBIC |
|---|---|---|---|
| 1/2 (RFC 3168, RFC 9002) | 0.382 | 1.130 | 1.008 / 1.014 |
| 7/10 (RFC 9438) | 0.401 | 1.558 | 0.842 / 0.880 |
| 4/5 (RFC 8511) | 0.375 | 1.934 | 0.812 / 0.794 |

The gentler backoffs carry more alone and take it from NewReno and CUBIC at 1 Mbit/s,
100 ms; only 1/2 leaves each nine tenths of its bar. How the answer was found — two first
answers that failed, ten seconds of classic growth chosen over the mode's four round
trips, a growth cap that changed the paths without a manager, the competing raise that
let Copa alone fill CoDel's queue, and the harness's own rules, which judged one run's
and then one seed's noise — is in the record's F39.

Open, the next batch (the record's F39): without a manager, Copa competing takes more
than the bar at long round trips (NewReno 33.8% against 43.3% at 1 Mbit/s, 100 ms; CUBIC
35.6% against 40.1% at 100 Mbit/s, 20 ms), answering a loss by `1/δ` alone; Copa's test
of the mode takes the least of four smoothed round trips where the paper's detector asks
for five, and Copa alone judged itself competing in 13.5% of samples at 100 Mbit/s,
20 ms; on a link without jitter Copa does not leave the competitive mode once a
competitor has left; and at 100 Mbit/s, 20 ms under CoDel Copa yields, carrying about 32%
beside either.

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
| An exchange is given a time, whatever it carries | Nothing to see | A path that carries less than the object in that time carries none of it. focal had the same defect, for content until 2026-09-28 and for what groups send until 2026-10-01, and gives each part of an exchange what its own stream and its path take (section 7) |

### 8.4 What is open in focal

| Open | Why it matters |
|---|---|
| The streams of a connection, sixteen by default (`WireLimits::streams_per_connection`) | Done (2026-09-29): derived from the consensus window and the reference path (`WireLimits::for_consensus`, section 7), and a group's message waits its turn on its lane, bounded by its exchange's time, instead of being refused; a peer the pool could not reach at all is told to the core, which probes it |
| The pool's five seconds, the announcement's round of five and enrollment control's of four | Set, not derived. The pool's is no longer the time of an exchange (section 7, 2026-10-01): it is what a caller waits for its lane and for its connection, and what a peer is given to answer once it has the request. A peer under load that takes longer to answer is still given up |
| The idle timeout of a connection, ten seconds | Set, not derived. It is what a path must return a datagram in (1,920 bit/s), and a path that carries nothing for longer loses its connections and what they carried |
| A transfer's lease, sixty seconds | Set, not derived. It is the longest a copy holds an unfinished transfer without a part taken; a part is sized to cross in a twelfth of it at the rate the path showed (section 7), so a sender that stops is dropped after it and a live one renews it many times over |
| The request time an owner gives what it holds (`request_timeout`, five seconds) | Set, not derived; counted in the owner's periods now, so a loaded machine stretches it, but a follower whose owner stalls for longer than the leader's request time is not seen by the leader, whose own periods run on time. Under eight and sixteen copies of the control suite at once this is what remains (five of eight runs, none of sixteen): a leader whose term entry the stalled followers do not acknowledge in time answers `NotReady` until leadership has moved again. The request time should follow the exchange tails of the voters (`PeerConnectionPool::exchange_tail`), which a stalled follower stretches and the leader's own stall does not |
| The control suite's single asks inside a request deadline of 350 ms | Done, at the cause: the owner gave a request 350 ms of the clock while a loaded machine slowed its rounds, so every ask timed out. A request now waits its time in the owner's own periods (`ControlHost`, `Pending::deadline`), and every ask of the suite that expects an answer waits for a definite one, charged to the hosts' periods (`Rig::definite`, `read_on_leader`); eight copies of the suite at once pass |
| A restore cut where it records its copy | Cut at three places and issued again on real processes (`runbook_interrupted_restore`); the cut between the copy's record and its attachment is covered by the record alone |
| A voter that dies and returns within the hold, end to end | Done: `cluster plan` says for how many seconds a death still stands (`focal_directory::deaths_stand_for`), and a voter that returns within the hold keeps its seat on real processes while a spare waits; one that stays dead loses it to the spare without an operator (`runbook_node_loss_within_the_hold_moves_no_seat`) |
| The fast track for an owner | A receipt states its entry's term: a durable format, and a decision. Its election is mended (section 4.6, 2026-10-01). Before an owner takes it: a leader that outlives two changes of its configuration has no fast track until its term ends, and the model has no change of configuration |

## 9. What a commit waits for (2026-09-30)

The audit's F17, and what mantle's replica does with the same core (its
`docs/design/replica.md` §3, read against this shell on 2026-09-30). The core says what a
`Ready` needs: `Ready::messages` are a leader's and may be sent at once,
`Ready::persisted_messages` answer for what the `Ready` persists, `Ready::must_sync` is
false when nothing but the commit moved, and `LightReady::commit_index` "need not be
durable to be acted on". Until this section the shell used none of it: every output of a
`Ready` waited for its write, a leader's appends among them; a commit that moved once a
write was durable was given a write and a flush of its own before anything it committed
was released; and a `Ready` that moved nothing but the commit was written and waited for.
A member that alone decides paid two flushes for every entry, a follower one for every
commit it was told of, and a leader's followers began to persist only after it had.

| Rule | Why it is safe | Where |
|---|---|---|
| A commit waits for no write of its own. A `Ready` that asks for no write is not waited for; a commit that moves once a write is durable is released at once | The commit index is volatile state (Ongaro's thesis, figure 3.1): what is durable is the term, the vote and the entries, and an entry is committed by where it is durable, not by a member's record of it. A member that restarts replays what its log says committed and is told the rest by its group; what it applied before it stopped it applies again from the same entries | `persistence.rs`, `finish_light` |
| The commit is kept with the stored hard state and rides the group's next record; a checkpoint writes it too | The log's records of a group are in order, so a commit a record carries names entries the log holds by then (`replay_record` reads it back under the same check) | `commit_unwritten` |
| A member that alone decides — it leads, it is the one voter, the configuration is not joint, the entries are of its term — writes `commit = last entry` in the append that holds the entries | No other member's answer is waited for: the commit is true exactly when that append is durable, which is when the record that states it is. Asked once the entries the `Ready` gave to apply are applied, so a change of membership among them is in force when the core counts; the core's commit is checked against it after the append and a difference stops the member | `sole_commit` |
| A commit no record has carried for a whole period of the owner is written then, and when the member is let go; one such write in flight, no one waiting | A group that keeps writing never writes a commit for itself — a write made the moment the commit moved would hold the disk the next entry needs (measured: 64 ms a commit against 37 ms, three members on one disk). A quiet group's log says what it applied within a period, so a member that stops reads back what it applied but for what a cut inside the period took | `settle_commit`, `Drop` |
| What a leader sends may leave while its write is in flight (`DurableNode::sendable`), and nothing else: the events of a drain are given whole, with nothing still to persist | Its members persist what it sends for themselves (Ongaro's thesis §10.2.1), and the core counts the leader's own copy only once `advance_append` says it is durable. A follower's acknowledgement and a vote stay behind the write they answer for. A snapshot stays with the drain, whose owner answers for what became of it | `sendable`, `wait_persisted`; the session owner and the control owner send before they wait |
| A change of membership is applied only once a write has stated the commit that covers it, and that write is waited for | Whoever is told that a change committed may act on it where no log records it: stop the member it removed. A member that then restarted without the commit would count that member again and wait for it for good — two voters, one removed and stopped, leave one that cannot elect itself (`cli_network` met it: the founder removed its peer, both were stopped, and the founder did not come back). Changes are rare; the wait is one flush for each | `fenced`, `after_advance`, `commit_durable` |
| A control group — the root, a directory partition — applies nothing, and says of nothing that it committed, before a write has stated the commit that covers it | What a control group applies its members act on when they next start, before the group has told them anything: who is enrolled and who was revoked, the fence below which a binary does not serve (24 §21), where a ledger is placed. A member that stopped within its owner's period would start again without what it had applied and act against it (`cli_upgrade` met it: a host that had honoured the fence, killed and started below it, published that it was ready before its group told it of the fence again). What a ledger applied is served only through its group — a read by a barrier, a write by a leader that committed in its term — so its members need no such rule. Under load the commit rides the group's next append, as before; a quiet group pays one flush for the commit; a control group of one voter pays nothing, its commit being in the append | `apply_on_written_commit`, `fenced`; `ControlReplica::open`, `open_on_wal` |
| The entries a `Ready` gives to apply are applied before its write | They are committed and durable here already (`Ready::committed_entries`); a `Ready` with a snapshot gives none | `drain_progress` |
| The owner that shares a thread among sessions is told when the log answers a write of one of them, drains that session then, and asks nothing meanwhile (the audit's F45) | It asked the log every millisecond for every session with a write out: the floor of a commit on a device that flushes faster than that, and a scan of every waiting session a thousand times a second on one that does not. The log's writer calls what the group gave it for the write — a `Ready`'s, a commit's, a checkpoint's, a decoder floor's — on its own thread; the call queues a signal the owner takes at the top of every pass and waits on when it has nothing due. A signal says there is something to take and nothing more: the drain reads the write's own answer, and a signal for a session that was stopped or replaced costs one look. One that is lost, or finds the queue of signals full, is made good at the session's tick, which is its only other deadline while it persists. A write the log had no room for tells no one; one such session is asked again for each write of the owner's that the log answers — a write answered is its room given back — and each at its tick | `DurableNode::{notify_persisted, wakes_owner}`, `focal_log::Persisted`, `fleet_group::{OwnerQueue, GroupOwner::signalled, take_signals}`, `Owner::group_deadline` |

What a member opens with follows from the first rule, and one place did not allow for it:
a member authorized the credential it holds against the registry its own replica
recovered, and a replica can be behind the registry its credential was committed in — a
follower always could be, and a leader now can for the last period before a cut. A member
may present a credential issued at a revision its registry has not reached, for the
identity the registry lists under the same enrollment
(`EnrollmentRegistry::authorize_held`); the founder's enrollment control authenticates
what the founder presents at each request and not when it is built.

Measured on this host (`cargo bench -p focal-consensus --bench commits`; an APFS volume
where a group commit is three `F_FULLFSYNC`; three members share the one disk, so their
flushes queue behind each other and the overlap of a leader's write with its followers'
shows as far less than it is on a disk each):

| | before | after, an owner that waits before it sends | after |
|---|---|---|---|
| One voter, an entry at a time, median | 25.5 ms | 12.8 ms | 12.8 ms |
| One voter, flushes a commit | 2.00 | 1.00 | 1.00 |
| Three voters, an entry at a time, median | 70.1 ms | 38.5 ms | 36.2 ms |
| Three voters, 4,000 entries as fast as the leader takes them | 18,092 /s | 24,213 /s | 28,880 /s |
| Three voters, flushes by member for 4,201 entries | 405, 404, 404 | 203, 204, 203 | 202, 203, 203 |

What mantle's replica does that this shell does not, and why:

| mantle | focal |
|---|---|
| Holds the messages and ticks that come while a `Ready` is out and takes them after (its R19: refused, none of a loaded leader's proposals committed) | The owners queue what comes behind a pending write (`fleet_group`'s scheduler, a blocking owner's channel) and always did: nothing is refused. A tick that comes meanwhile is not held: the period has passed without it and the owner's stalls are the replica's patience (section 3.1 P3) — a member that replays held ticks after a stall campaigns for a leader that was only as slow as itself |
| Confirms reads a round at a time: one round out, and a read asked meanwhile waits for the next | A round for every drain of the owner, carrying the reads asked since the last (section 10, the audit's F43). A read asked while a round is out does not wait for that round's answers before its own leaves: on a quorum a long path away, waiting costs up to a round trip more for every read that overlaps another |
| Applies committed entries without the commit durable and repairs the log's commit from its engine at open | The same rule, without an engine: the state is the log's, replayed to the commit the log holds |
| Marks a member whose last frame was lost and repairs it in place | A log damaged before its fence does not open; the member is replaced (24 §19). Open with the log's fence (below) |

Open, and the audit's F17 still: a group commit is three device flushes here (the data,
the fence file, the directory entry of its rename; `focal_log::install_fence`), where the
fence could be made durable by one write in place.

## 10. Reads that share a round (2026-10-01)

The audit's F43. A linearizable read asks the leader at which index it may be served
(`ReadIndex`, Ongaro's thesis §6.4): the leader notes its commit and asks a quorum whether
it still leads; a round of heartbeats sent after the read was asked, and answered by a
quorum, confirms it and every read asked before it. The core had the second half — one
answer released every read asked before it — and not the first: it broadcast a round for
each read as it was asked, so twenty reads taken together were forty heartbeats to two
followers, of which the answers to the last two did all the work. And the owners drained
after every request, so even a core that shared rounds would have been asked for one each.

| Rule | Why it is safe, and what it costs | Where |
|---|---|---|
| A read asked is queued and nothing is sent. One round leaves when the member is next asked what there is to do, carrying the last read asked, for every read asked since the round before | The round is sent after each of those reads was asked, which is all a round must be to confirm a read. A read asked alone leaves with the `Ready` its owner takes next: nothing waits for a timer or for another read | `Raft::read_index`, `Raft::ask_reads`, `RawNode::ready`; `RawNode::has_ready` is true while a read is unasked |
| A read asked after a round left is asked for by the next round, never confirmed by the one before | That round's heartbeats left before the read was asked and say nothing of who led when it was: a leader deposed between the two would answer the read at a commit the group had passed. `a_round_confirms_no_read_asked_after_it_left` holds a round's answers while another leader is elected and commits, asks the old leader a second read and delivers the answers: with the rule broken the read is answered at 2 when 4 was committed | `ReadOnly::asked`, `unasked`, `advance` |
| A round that was lost is asked again by the leader's clock | The heartbeat a leader sends each beat carries the last read asked and is a round for every read that waits. A read asked again while it waits asks for no round of its own (raft-rs sent one) | `Raft::bcast_heartbeat` |
| An owner takes what is queued behind a read before the drain that sends its round | The reads among it share the round. Counted by what the owner admits at once (`pending_clients`, `pending_requests`); a request that is no read is drained for as it was, which sends the round with it and ends the taking. An owner that shares its thread among sessions drains a session on its next pass, after the work it dispatched in this one | `Owner::take` and the `Work::Request` arm (`fleet.rs`), `ControlHost::run` and its `Work::Request` arm |

A round for each drain, and not one round out at a time (what mantle's replica does): a
read asked while a round is out would wait for that round's answers before its own left,
up to a round trip more on every read that overlaps another — nothing on a quorum in one
room, and half again of a read's latency on a quorum a long path away. The rounds an
owner sends are bounded by its drains, and under load its drains carry what queued
meanwhile, as a group commit carries what was written meanwhile.

Measured by count, not by time: 1, 32 and 128 reads asked of a leader of three before it
drains leave in two heartbeats and are all confirmed by one follower's answer
(`reads_asked_together_are_confirmed_by_one_round_of_heartbeats`); a hundred requests
queued for a session's owner leave in two heartbeats where a drain for each sent two
hundred (`reads_queued_together_leave_in_one_round_and_all_are_answered`).

Open: the reads a leader may hold are bounded by the window it lets a peer have in flight
(`DurableNode::pending_reads`), a size that was right when each read was a round in
flight. A read a follower forwards to a leader at that bound is refused there and found
by its asker's deadline, not told to its asker at once.

## 11. What a member is sent ahead of its answers (2026-10-01)

The audit's F41. A leader sends a member entries ahead of its answers, and a window
bounds how far (Ongaro's thesis §10.2.1). The window counted messages: 128 of them,
each a page of up to an entry's bound and a kilobyte, so a member's window stood for
forty bytes or for half a gigabyte, and the same number served a loopback, a 64 kbit/s
path and a long fat one. Nothing bounded the bytes queued for a slow member but the
memory that refused them.

| Rule | Why, and what it costs | Where |
|---|---|---|
| A window holds messages and bytes, and is full by either. A message takes one place and what its entries encode to, by the measure a page is cut by | A page is cut to what the window has room for before any of it is copied, and holds one entry at least: what is out passes the bound by one entry at most. A window that holds nothing is never full for its bytes, so an entry larger than the bound is sent, alone, and the member is not left waiting for good | `Inflights::{full, room, add}`, `Progress::page_bytes`, `Outbox::append` |
| An answer gives back what the messages it answers took | The bytes are kept with each message's last index; an answer out of date gives back nothing, one that repeats gives back once, one that skips ahead gives back all it covers. A change of the member's state empties the window and keeps its bound. The bytes counted are checked against the messages held wherever the core's accounting is (`check_accounting`, after every step of every schedule) | `Inflights::{free_to, reset, check}` |
| The bound is the member's, and its owner says it | It is what the path to that member carries, which the core cannot know. Until it is said a member is sent one page: the least that always makes progress. A member the configuration makes anew begins there again | `Config::max_inflight_bytes`, `RawNode::set_inflight_bytes`, `DurableNode::set_inflight_bytes` |
| The owner says: twice what the transport to the member holds in flight; and a page at least where the path carries a page within one beat of the leader | The transport's congestion window is the measure of what a path holds before it answers, and it is already kept for every peer (`focal_wire::congestion`). Twice it, as a sender's buffer is sized against its window (Linux `tcp_sndbuf_expand`: "Cubic needs 1.7 factor, rounded to 2 to include extra cushion (application might react slowly"): a sender held to the window itself never fills it, and a window never filled is never found too small. The page: a new connection's window says only that nothing was sent on it yet, and a path that carries a page in a beat is not kept to that; a thin path is never given a page it would take many beats to carry. A path with no round trip measured says nothing and the member keeps what it has | `fleet::inflight_bytes`, `Work::Windows`, `ReplicaHost::inflight_windows`, `PeerConnectionPool::window`; the node's pacer says it once a round |
| Never more than the group's budget can stage | One transition stages a page for every member and the window of the one whose answer it may be. A window the budget's limit cannot hold beside those pages and what the group holds at rest would refuse every answer of the member it was made for; the bound is cut to what can be staged. The staging a transition reserves is priced by each member's bound, where it was priced by 128 pages | `DurableNode::set_inflight_bytes`, `memory::sends_bytes` |

What a heartbeat's answer does (2026-10-01, with the audit's F42): raft-rs's rule freed a
full window's first message at every heartbeat the member answered and sent the next,
whatever became of the first, so the bytes out passed their bound by a message a beat. A
member now says in its answer how far its log goes — its last index and that entry's term
(`HeartbeatAnswers::Position`). Where the term is the leader's own, the leader made that
entry and the member took it and all before it from the leader's appends: the answer is
an append's answer for all of it, and is taken as one (not in a fast group, whose terms
differ by member; there it gives the window back and nothing else). Answers that were
lost are made good exactly; a member that holds nothing new is sent nothing more; one
whose window is full and that has answered for none of it through a beat of the leader's
ticks is probed, with one message cut to what its path carries; and a probe is sent again
when its owner is told it was lost (`MsgUnreachable`), or once a beat of ticks has passed
since it was sent — not at every heartbeat's answer, which on a path slower than the
heartbeats sent the page again and again behind itself. The beat is counted in the
leader's ticks because its owner stretches those by the path to the group's members
(section 3.1 P2) while the heartbeats keep their configured cadence: a beat of ticks is,
on whatever path, time enough for what was sent to have been answered. etcd's core sends an empty append to a full window instead; it was not taken,
because this node sends a peer's frames each on its own stream, in no order, and an
empty append overtakes what is still on a far path and is refused for the entry before
it. raft-rs's rule is kept (`HeartbeatAnswers::Bare`) for the comparison alone.

Control groups keep the page: their entries are small and their pacer says nothing of
windows.

## 12. What carries a group's messages to its peers (2026-10-01)

The audit's F42. The owners hand their messages to a driver (`replication::drive`) that
sends each as an exchange with the peer's node, answered once the peer has persisted
what the message caused. The driver counted an exchange as under way before it had the
peer's lane (`PeerConnectionPool`: as many exchanges with one peer at once as the
consensus window), and held 1,024: a peer that stopped answering filled it with
exchanges waiting for its lane, the driver stopped taking from the owners' channel, and
the frames of every peer that did answer waited behind them. And a frame that was not
delivered was told to its owner only when the peer could not be reached at all: a lane
that was full, a peer that refused, a queue with no room dropped the frame untold, and
the group took it to be on its way.

| Rule | Why | Where |
|---|---|---|
| An exchange is begun only when its peer's lane has a place for it | A peer that stopped answering holds its own lane and nothing of another's | `replication::drive`, `Waiting::sending` |
| What a peer's lane has no place for waits its turn in that peer's own queue, in the order it came, what a group cannot do without before its entries | A heartbeat, a vote or an answer behind a page of entries for a slow peer is the group's election timeout; entries and snapshots are what can wait | `Waiting::{urgent, bulk, next}`, `ReplicationFrame::urgent`, `fleet::urgent` |
| The driver never stops receiving, and holds what the pool itself admits: every connection's lane at once | A frame left in its owner's channel holds back every frame behind it, whoever they are for. Sized by the pool, a burst from many groups to one peer waits in the driver and is carried, and the driver's own bound is met only when more peers are sent to than the pool has connections for | `PeerPoolLimits::for_consensus`, `NetworkService::run` |
| When the driver holds all it may, the other peer that holds the most waiting gives up its newest frame for the frame that came, unless the frame's own peer holds more; then the frame that came is given up. On a tie the other's goes (2026-10-01): it is the older, and of a group's frames the newer carries the more | The room is shared by the peers that need it, and none takes another's by being slow. Nothing under way is given up. With the frame that came going on a tie, the driver's own test of a dead peer beside a live one hung in two runs of sixty: the live peer's fifth frame, arriving before any of its four was seen carried, found both peers holding two and went | `Waiting::newest` |
| A frame that is not accepted — lost, refused by its peer, given up for the room, or dropped by its owner before it reached the driver — is told to its owner | The core then probes the member instead of sending ahead into a lane that is full or a void. Nothing is taken to be on its way that is not, which is what lets a full window wait for answers (section 11) | `Frame::give_up`, `ReplicationFrame::lost`, `Owner::send`, `ControlHost::carry`, `Owner::report_lost` |
| A peer's appends carry the order they left in, and are stepped in it | Each frame goes on its own stream and the path completes the streams in any order — a datagram lost is sent again a round trip and an acknowledgement delay later, and the frame behind it arrives first: an append that overtook the one before it was refused by the core and the member probed, a round trip lost for nothing lost. The sender counts its appends to each peer (`Operation::RaftOrdered { group, epoch, sequence, message }`, tag 33, node-only: an epoch, the term of the appends — only a leader sends them and a node leads again only in a later term, so a source's epochs only grow, where an incarnation drawn at random at a restart was smaller than the one before as often as not; a sequence within it, given before the sender can give the frame up, so a frame given up leaves its gap as one the path lost does) — appends with entries and the empty appends that carry a commit alike, since each names the entry before what it carries — and the receiver keeps, per source, the sequence it expects next and the frames that came ahead of it, stepping each when its turn comes; what a group cannot do without — heartbeats, votes, answers — goes unordered, as `Raft`, since a monotone commit or match is the same in any order | `fleet::urgent`, `fleet::Owner::ordered`, `ControlHost` `Owner::ordered`, `resequence::Resequencer`, `admit_replication`, `HeldFrame`, `HeldControlFrame` |
| A held frame is let go by three bounds: its patience, its source's lane, the configuration | It is held no longer than its patience — the probe timeout of the path it came by, as this node's connection measures the path (`frame::probe_timeout`: three round trips and the acknowledgement delay the sender's probe timer counts, RFC 9002 §6.2.1), in the owner's periods and one more for the period under way: a frame that waited longer did not overtake its predecessor, the predecessor was lost, and the leader that was told so is probing. No more are held for one source than the source may have in flight (the inflight window), and no more sources have a lane than a configuration may name (`LOST_PEERS`). Past any bound what is held is stepped in its order and the core judges it as it did; a newer epoch from a source resets its lane, the older's frames stepped first; a source that left the configuration has its frames refused. The owner asks at every period it runs, a replica's own owner as a group's: a replica's own owner never asked, and a follower that lost appends held every one after them (2026-10-03) | `Resequencer::{hold, expire, prune}`, `patience_until`, `Owner::expire_held`, `VerifiedRequest::path_round_trip` |
| The order rides the ordered profile and nothing else | A connection that negotiated it (`ORDERED_PROTOCOL_VERSION`, 5, topping the ladder; offered by the node's replication handlers, `supports_ordered_replication`) carries `RaftOrdered`; to a peer whose connection does not admit it — an older binary — the pool sends the frame as a plain `Raft` at the connection, the one copy a mixed window costs, so the old peer loses nothing but the order. A receiver holds an ordered frame to the ordered profile and a plain one to the base (`verify_request`) | `PeerConnectionPool::peer_protocol`, `send_bounded`, `QuicConnector::offering` (what an older binary offers; the fleet's test of a mixed window) |

Measured on three in-process replicas over real QUIC behind relays of 5 ms each way that lose one datagram in fifty (`fleet_quic`, `a_peers_appends_are_stepped_in_their_order_across_a_lossy_path`; `support/relay.rs`, `Relay::lossy`): a burst of 512 entries had the followers refuse 27 appends with every connector offering what a binary before the profile offered, and none with the ordered profile — 61 frames held for the one they overtook, none let go past its patience (`ReplicaProgress::appends_rejected`; `frames_held`, `frames_let_go` and `frames_stale` say what the order did). Found by the measurement: the probe timeout of a path counted three round trips and not the acknowledgement delay a peer may take (RFC 9000 §18.2's 25 ms, which every connection here advertises) — on a path of ten milliseconds a loss is recovered no sooner than it, and a patience derived from it was shorter than the recovery (`frame::MAX_ACK_DELAY`); and an empty append, which carries the commit after the entries it follows, went unordered as a heartbeat does and was refused for naming an entry its member had yet to receive — every refusal left with the order kept was one. Open: a frame lost on the path still costs the probe it did; a held frame's patience is one probe timeout, which a frame lost twice exceeds. Each follower also counts the refusals the order should have spared (`ReplicaProgress::appends_rejected_in_order`, 2026-10-03): a loss — a frame let go past its patience, found stale or not stepped — is kept as the first index the follower's log lacked then, and a refusal in that term whose hint, the last entry the log could agree on, falls before it is the loss's; a term's first exchange, the plain appends of a leader of an older binary and a request for a snapshot are not counted. How many appends a lossy path has refused is the load's as much as the path's (29 on the ubuntu CI, 14 on a laptop alone, none in three copies beside a suite), so the test asserts this count, never theirs.

## 13. A delivery a refusal stops is continued, not failed (2026-10-03)

A control replica (`ControlReplica`, the shell of the root and of every directory
partition) took what its node handed over — a snapshot, the entries after it — and
applied it in one pass; any error of the pass marked the replica failed, and every call
after answered `Failed`. The node had handed the events over once, so a replica could
not ask for them again: a refusal that changed nothing of the node — memory for an
entry's decode, for a snapshot's restore — ended the replica as a corrupt entry would,
and its owner, which treats such a refusal as a checkpoint's (the pace refused, the
poll after resumes), resumed into a replica that had already failed. A member brought
up by snapshot on a loaded runner ended that way (ubuntu CI, 2026-10-03), reporting its
egress's end and then, once the owner named it, the generic failure.

| Rule | Why | Where |
|---|---|---|
| A delivery is continued from where it stands, never taken twice | The events the node handed over are kept with the output built so far and two cursors — the entries and the configuration changes applied — and a snapshot's installation is marked done once its state is swapped in; the next drain continues at the entry a refusal stopped, nothing applied twice, no new drain while one is held (one at most; its memory is the node's allocation for the events) | `RetainedDelivery`, `ControlReplica::continue_delivery`, `drive` |
| Only a refusal that changed nothing of the node is retained | Memory refused for a decode or a restore leaves the node and the replica as they were; the drain answers the refusal and the owner polls again. Any other error — a corrupt entry, a wrong owner, a node that failed — fails the replica by its name, kept as its first failure | `drive`, `ControlReplica::{fail, failure}`, the owner's `checkpoint_retryable` |

Measured on the owner's fixture: a follower whose budget is filled before its snapshot's
decode answers the drain `Memory`, says nothing failed, and installs the snapshot once
the room is given back (`a_delivery_a_memory_refusal_stops_is_continued_by_the_next_
drain`); every delivery runs the same cursors, and a refusal between two entries has no
deterministic lever in the fixture, whose commands are all of a size.

## 14. The core is hyper-raft's (2026-10-03)

focal's core moved to the shared repository on 2026-10-01 as hyper-raft (R-1: `focal-raft` at
`a8e95f7`, with its history), and every core change focal made since was ported there in a commit
of its own (F43, F41, F42, and the fast track's election fix of `7ea6f63` with a second rule for a
change of configuration; hyper-raft `crates/hyper-raft/ORIGIN.md`). focal now runs that crate:
a snapshot of it and of `hyper-timing`, which it depends on, under `vendor/` (`SNAPSHOT` names the
revision), and `crates/focal-raft` is gone. The order the owner set for the rest (2026-10-03): the
core; then the WAL onto hyper-log (F-1); then the durable shell onto hyper-durable (D-2); then
timing and liveness; then the transport.

**No byte changes.** hyper-raft speaks its own types and format (its R-2). focal's WAL records and
peer messages keep raft-rs 0.7's protocol-buffer encoding (18 §1: no serialized type changes in
place), through focal's own codec, `focal-consensus/src/envelope.rs`:

| Way | Rule | Why |
|---|---|---|
| Written | Exactly as raft-proto's prost codec writes it: fields in the order of their numbers, defaults and empty fields left out, repeated numbers packed, a message field whenever it is present; a vote request's positive priority in raft-rs's older field too, as focal's core sent it | An older binary, and a peer of one, reads every byte as before; a node can go back to the binary before this one |
| Read | As any protocol-buffer reader reads it: fields in any order, the last of a scalar winning, a message field met twice merged, repeated numbers packed or not, unknown fields skipped by wire type; refused: a field number outside 1 to 2^29 − 1, a known field of another wire type, a varint past ten bytes, a group, bytes that end inside a field, a kind the core does not name | What raft-rs reads, focal reads; what no raft-rs message holds is no message |
| A change of configuration | Its data is raft-rs's `ConfChange`/`ConfChangeV2` in the log and the core's own record in the core: translated with the entry; the empty change is no bytes in both | The core decodes a change's data where it applies it |
| What the core has and the encoding has not | Refused, never dropped: a member's mark (`Message::lost`), which focal's log never sets | — |

No protocol-buffer runtime ships: `raft-proto` is a test dependency, the oracle. `envelope_tests`
holds the codec to it — 4,096 generated values of each type written to raft-proto's own bytes
exactly and read back (messages with changes of configuration among their entries, snapshots,
both priority fields), every prefix and one-byte mutation of generated messages read as raft-proto
reads them — and the fixed original encoder fixtures are written again byte for byte. Peer bytes
that do not read are `MalformedMessage`. The memory accounting that priced a committed change from
raft-rs's bytes reads the core's record in place (`hyper_raft::wire::changes_stated`, added to the
shared crate for it).

**What the core brings beyond `focal-raft`**, by the shared repository's steps
(hyper-raft `docs/raft.md` §3): pages chosen and entries moved without copying, a `Ready` in place
(the law of measurement); readies taken ahead of their persistence (R-4), off at focal's depth of
one; answers that state no commit beyond the durable one (R-6); a marked member's repair and
election (R-5, R-7), off without hyper-log's marks; elections by suspicion (L-2), off on ticks; the
fast track's second rule. focal's suites run unchanged on it but for the types: the core's are
typed (a kind is an enum, not a number to check), and a value raft-rs's numbers could carry and
the core does not name is refused by the envelope before the core is given it.

**The format switch comes later, by layer**, with F-1: on the wire, a new protocol profile
negotiated per connection, as the ordered profile is (§12), the old encoding sent to a peer that
did not negotiate it; on disk, the new encoding under new `RecordKind` ordinals, which an older
binary refuses, written only once the cluster's upgrade fence opens a named capability level
(24 §21), the old kinds read for old histories. 18 §5's activation is for what entry data means,
which this does not change.
