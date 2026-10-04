# hyper-durable: origin

hyper-durable is new code. It is not extracted from one project's shell: its design is
`docs/durable.md`, drawn from the three shells that exist and from the literature
(`docs/research/durable.md`), and each rule below names where it comes from.

## The shells read

- **mantle**, `crates/range/src/{replica.rs,store.rs}` at origin/dev `1c179e8`
  (`1c179e8a734d40640b82b6bddf8cad2030d0b55f`), its commit fence included, and its range
  simulation's four directed cases (`crates/range/tests/sim.rs` at the same revision).
- **focal**, `crates/focal-consensus` at `aa1f162` (F17: `persistence.rs`, `sole_commit`,
  `settle_commit`, `apply_on_written_commit`) and `7a6170e` (`DurableNode`, `guarded_in`,
  `memory.rs`), as `docs/research/durable.md` §4 and §6 record them.
- **slates**, its anchor publication at `ec5e0df` (`docs/research/durable.md` §6).

## What came from where

| Rule | From | Here |
|---|---|---|
| The shared device log as the store; a group's handle that reads where the replica is | mantle | `GroupStore` over hyper-log's `GroupLog` (`src/hyperlog.rs`); entries encoded as mantle's store encodes them (`encode_entry`, `ENTRY_OVERHEAD`) |
| A leader sends before its own write | mantle R31, thesis §10.2.1 | the core gives them (`Ready::messages`), `Replica::take_ready` emits them at once |
| A write refused for room waits whole; the replica takes no part while it does | mantle (audit S04) | `Replica::refused`, `make_again` (with the fast track's proposals, `RawNode::issued_proposals`), `ReplicaError::Stalled`; campaigns held (`RawNode::hold_campaigns`) |
| A snapshot report kept while stalled | mantle (a simulation seed) | `Replica::report_snapshot`, one a member of the configuration |
| A failed write fences the replica | mantle; Rebello et al. | `Cause::Write`; every call answers `Fenced` |
| Open: finish an install the log never recorded; raise the commit to what the state machine holds | mantle (`complete_install`, `commit_applied`) | `repair_at_open`, with the state machine's point's term from `StateMachine::durable`, so mantle's `Damaged` inference is gone |
| Marks: votes judged against the mark, repair, a marked member's election | mantle (R34; PAR), then the core's (R-5, R-7) | `Config::lost` from the store's mark; the rest in the core (sections R-5 and R-7 below) |
| One `Ready` a drive, the owner's quantum | mantle (`DRIVE_BUDGET`) | `Replica::drive`, `Owner::turn` |
| A write submitted in a drive is never polled in it | mantle (replica.md §5) | `Replica::take_answers` runs first and only first |
| The commit rides the next write; `commit = last` where the member decides alone; a quiet group's commit written after a period | focal F17 | `Replica::stated_commit`, `decides_alone`, `quiet_commit` |
| A change, and what the state machine acts on at start, applied only on a logged commit | focal F17, generalised by `C_d` | `Replica::walk`'s fence, `StateMachine::acts_at_start` |
| Memory reserved before a transition | focal R28 | `Budget`, `Replica::reserve` and `settle`; `Unbounded` compiles it out |
| The unwind boundary | focal R27 (`guarded_in`) | `Replica::guarded`, `Cause::Unwound` |
| The owner woken by the log's answer | focal F45 | `LogStore::submit`'s waker, `Owner::woken` |
| A store that completes synchronously is a store of depth one | slates | `RamStore` |
| Readies ahead of persistence; the unstable log true to its name; the ABA guards | etcd, raft-rs (core R-4) | `Limits::readies_in_flight` set to `LogStore::depth` |
| Apply before local durability on a leader | TiKV RFC 0112; raft-rs #537, #561 (core R-6) | `Config::apply_unpersisted` passed through; `Replica::walk` reads the leader's own entries past the store where the core holds them |

## Core steps

- **R-4** (readies ahead of their persistence) and **R-6** (the durable commit, the apply pause,
  applying before durability; hyper-raft `94a3a6a`) are built and used: `RawNode::ready_in_place`,
  `to_persist`, `advance_issued`, `on_persist`, `durable_commit`, `commit_durable`, `pause_apply`,
  `resume_apply`.
- **R-5**, **R-7** and **L-2** are built and used; what each changed here is in its section below.

Asked of the core, not built here: `RawNode::into_store`, so that an owner gets its store back from
a replica it closes (`Replica::into_machine` gives back the state machine only). Built since:
`RawNode::issued_proposals`, the fast track's proposals of the writes issued and not yet durable,
with which a write refused for room that held them is made again (`Replica::make_again`); the
replica was fenced there before, so a full log cost a fast group its member.

## hyper-log changes made with it

- **Writes sent behind a refused one are refused** (`LogError::Behind`): a group's handle carries
  an epoch it moves on every refusal it takes, and the log refuses a write of the group sent in an
  epoch at or before a refused one's. Without it a write that held only a hard state, sent behind
  entries refused for the group's bound, was written: the group then stated a commit past the
  entries it held (`tests/log.rs`, `a_write_sent_behind_a_refused_one_is_refused_too`, fails
  without the owner's check).
- `GroupLog::depth` (the log's `PIPELINE_FRAMES`, three) and `GroupLog::has_room`.
- Not made: `GROUP_SUBMISSIONS` from `PIPELINE_FRAMES` (`docs/durable.md` §11). At four a group of
  frame-sized writes would hold all three frames' bytes and the log's tests of a cold group's room
  fail; a group's third write waits in the log for its group's room instead, never refused, and
  `docs/durable.md` §14 item 1 measures whether the third frame earns its place.

## L-2: elections by suspicion, on the node-pair stream

Timing step L-2 (`docs/timing.md` §2.9, `docs/durable.md` §8). The shell opens every core electing
by suspicion and has no `tick`; `suspect`, `trust`, `restarted`, `set_timing` and `deadline` reach
the core, and `drive` wakes it. The time every call takes is the owner's monotonic clock in
nanoseconds (`u64`), as the core's and hyper-liveness's. The campaigns are held while the member is
marked or stalled (`RawNode::hold_campaigns`); the detectors' words are not withheld, which the plan
had said, since a marked follower that kept trusting a suspected leader would refuse every
pre-vote of its group. The owner wires the node's `hyper_liveness::Liveness`: `Owner::pairs`,
`Owner::believe`, `Owner::measure`, `Replica::measure`, `Replica::believe_all`, `Replica::peers`,
`Driven::flushed`. The simulation's settle resumed a stalled replica once and never again, so a
refusal taken after it stalled the replica for good; found by the suspicion soak (seed 5, crash 4),
it now resumes each round.

## R-5: a marked member repaired by entries

Core step R-5 (`docs/durable.md` §5.1). The repair path moves into the core; the shell's part is
what it tells the core at open. The edits, all in `src/replica.rs` but the tests:

- `Replica::open` sets `Config::lost` from the store's `Health::Marked` (a `hyper_raft::Lost`).
- Gone: the fields `mark` and `repair`; `judged_by_mark` (a vote request behind the mark dropped,
  an order to campaign dropped, a heartbeat that counts lost entries naming its leader for repair),
  `refresh_mark`, `ask_repair` (a refusal carrying `request_snapshot`, mantle's repair),
  `serve_requests` (a leader preparing the snapshot asked for), the campaign filter in `emit` and
  the free function `campaigns`. The core judges a vote request by the mark (it answers it
  refused, where the shell dropped it), holds the campaigns, and sends the refusal flagged lost.
- `Replica::mark` reads the core's (`RawNode::raft().lost()`), which ends by hyper-log's rule as the
  durable log reaches it; `Replica::wake` holds campaigns only while stalled; `Replica::campaign`
  maps the core's `Error::Lost` to `ReplicaError::Marked`.
- Tests: `tests/shell.rs`, `a_marked_member_takes_no_part_in_elections`, now asserts a refused vote
  answer where it asserted none; `tests/support/cluster.rs` gains `Cluster::rot` (power lost, the
  last entries above the state machine's durable point gone with their persist record kept, reopened
  marked) and `tests/sim.rs` `a_member_whose_last_writes_were_lost_at_rest_is_repaired_by_entries`:
  the leader resends every lost entry, sends no snapshot, the mark ends and the group settles.

## R-7: a marked member's election

Core step R-7 (`docs/durable.md` §5.2). Nothing in `src/` changes: the shell already left a marked
member's campaigns to the core (R-5), and `Replica::campaign` maps the core's refusal. Tests:
`tests/shell.rs`'s `a_marked_member_takes_no_part_in_elections` becomes two,
`a_marked_member_campaigns_on_its_log_and_votes_by_its_mark` (three voters: it refuses a candidate
behind its mark and asks its pre-votes naming its log's last entry) and
`a_marked_member_of_two_takes_no_part_in_elections` (two voters: refused `Marked`, due for nothing,
asking no one).


## Ticks and a store's hold, for focal's D-2 (2026-10-03)

Asked by focal's design for F-1 and D-2 (focal 27 §15, reviewed by focal's session), and needed by
mantle's D-1 where its owner cannot yet carry the node-pair stream.

- **Ticks** (`docs/durable.md` §8). `Settings::elections` replaces the shell's fixed choice of
  suspicion. It has no default, so every owner states it, and it is fixed while the replica runs.
  On ticks, `Replica::tick`, `beat`, `set_randomized_election_timeout` and `set_patience` are
  focal-consensus's `DurableNode` calls of the same meaning. The detectors' words are refused, as
  the core refuses them, so there is no mixed mode. A stalled replica is not ticked, and the ticks
  it missed are never given again: mantle's replay of them was a defect (§10). The tick mode goes
  once the last owner elects by suspicion.
- **A store's hold** (§2.4). focal's `DurableNode` refuses to persist the first entry that needs a
  successor decoder until the group's floor is durable (focal 18 §4–§5, `decoder.rs`). In the shell
  that rule is the store's: `LogStore::Hold` names what a write waits for, `Fault::Held` refuses
  it, changing nothing, and `Replica::held` and `release` let the owner see it and meet it. The
  replica stalls whole, as for room; the held write is counted (`Writes::held`); a store refuses
  every write behind a held one. A release that comes before the shell took the refusal is kept:
  the stall starts freed when the store holds nothing. The second test below failed without that,
  a replica waiting for a release that had come.
- Tests (`tests/shell.rs`):
  - `a_write_the_store_holds_waits_whole_until_its_owner_meets_what_it_holds_for`;
  - `a_member_stopped_between_release_and_the_write_made_again_reopens_without_it`;
  - `a_replica_on_ticks_elects_by_its_owners_ticks_and_hears_no_detector`;
  - `a_replica_by_suspicion_takes_no_ticks`.

## A machine sees the change it applies (2026-10-03)

focal's owners report each change of configuration with the context its entry carried and the
configurations before and after it (focal-consensus's `AppliedMembership`). Over the shell, focal's
hand-over machine produces that report (focal 27 §15.7, option (B)), and it was given only the
point and the configuration the change left. `StateMachine::apply_change` now takes the change as
its entry stated it, decoded once by the replica as before; the configuration before is the
machine's own. mantle's range machine gains the same view. Test:
`a_machine_is_given_the_change_it_applies` (`tests/shell.rs`), the context of an added learner's
change read by the machine.

## An image carries the configuration held at its point (2026-10-03)

The replica prepared the snapshot it serves to members behind the log's start from the state
machine's image and the machine's current configuration, which holds only for a machine that
images everything it applied. focal's hand-over machine images its owner's latest checkpoint,
behind what it applied (focal 27 §15.7), and a change applied since would have ridden the
snapshot at the image's point, to be applied again from the log by the member that installed it.
The Raft paper's snapshot carries "the latest configuration in the log as of last included index"
(Ongaro and Ousterhout 2014, §7; `docs/research/durable.md` §1). `StateMachine::image` now
returns the configuration held at its point with the point. The tests' machine images what it
persisted when it keeps checkpoints. Test: `a_snapshot_carries_the_configuration_held_at_its_images_point`
(`tests/shell.rs`): before the change, its first snapshot named a learner added after the image;
now it does not, and the learner, served once its leader checkpointed past the addition, ends
holding what its leader holds.

## One page applied a drive (2026-10-03)

A drive took the answers of every write the log had made durable, and each answer's notice gave
a committed page to apply; the fence's page and the `Ready`'s followed. One drive could hand the
state machine a page for each write out and two more. An owner that reserves before a transition
what it hands on (focal's R28, which its hand-over machine keeps, focal 27 §15.7 contract (g))
would have to reserve all of them before every drive. A drive now applies one page at most: the
core's `max_committed_size_per_ready`, entries counted as the core counts them, or one entry
larger than it. This is the quantum the owner already gives a replica (one `Ready`, mantle's
`DRIVE_BUDGET`), applied to what it applies. What is past the page waits with the core's apply
paused, as entries behind the fence do, for the next drive only, and the drive says it is due.
etcd bounds what it hands out and has not seen applied by bytes (`maxApplyingEntsSize`,
`docs/research/durable.md` §3); here the shell bounds a drive's share. Test:
`a_drive_applies_one_page_and_the_next_drive_the_next` (`tests/shell.rs`). Four writes answered
in one drive gave it 16 entries, 3,436 bytes, against a page of 512. Now every drive stays within
the page, or applies one larger entry alone.

