# Where hyper-raft came from

- **Source.** focal (`github.com/hyper-light/focal`), `crates/focal-raft`, at
  `a8e95f71496461ebc8984926a65666746e946473` (`origin/slates-port`, 2026-09-30).
- **Why.** mantle note 32 (`docs/research/32-shared-transport-and-raft.md`) §3.8 and §5.2 step R-1: the
  shared Raft core is focal's, moved unchanged in behaviour. The owner's decisions in that note's §6 name
  it `hyper-raft`.
- **History.** The eight focal commits that touched `crates/focal-raft` were split out
  (`git subtree split --prefix=crates/focal-raft a8e95f7`), moved under `crates/hyper-raft` with an index
  filter, and merged into this repository unchanged (merge `a24589b`). `git log --follow` on any file
  reaches focal's first commit of it.

  | focal | here | |
  |---|---|---|
  | `b92492c` | `d5a222e` | focal's own consensus core under the durable shell, compared with raft-rs step for step |
  | `f929ea0` | `13df4de` | the fast track, modelled and measured |
  | `bd5e54b` | `817a7ce` | leadership that returns, a zone that outranks, preferred leaders spread |
  | `c1af831` | `3906c6a` | a stall covered in ticks |
  | `078d9ac` | `0a130e7` | the allocation audit's counting benches |
  | `653616e` | `352864b` | Raft acknowledgments admitted beside the participants (focal audit F56, F63) |
  | `58c8141` | `7c5e32f` | a page is chosen before it is copied (focal audit F15, F16) |
  | `1f87bfc` | `7026c0b` | every fixed allowance of the consensus pricing is derived (focal audit F16) |

  The tree at `7026c0b` equals focal's `a8e95f7:crates/focal-raft` byte for byte.

## Changes since

Each change below preserves behaviour. "Proof" at the end says how that was shown.

### Name

- Package `focal-raft` is now `hyper-raft`, and the library `hyper_raft`.
- Tests and the bench import `hyper_raft::` (45 lines).
- The schedule variables are `HYPER_RAFT_SEEDS`, `HYPER_RAFT_STEPS` and `HYPER_RAFT_SEED`, in place of
  `FOCAL_RAFT_*` (14 lines, README included).
- Printed labels say `hyper-raft` (5 lines).
- `Settings::focal()` keeps its name: it is the settings focal runs the core with.

### Manifest

- The crate inherits `version`, `edition`, `rust-version`, `license` and `repository` from the
  workspace, and `[lints] workspace = true`.
- `raft-proto`, `thiserror`, and the dev-dependencies `raft` and `slog`, are workspace dependencies here,
  at focal's pins: raft-rs `8e4cef172421bf77b2ae1c26628a9531b0be41f0`, no default features,
  `prost-codec`. raft-rs is a dev-dependency only: the differential's oracle.
- `Cargo.lock` was seeded from focal's at `a8e95f7` and pruned to this workspace, so every package
  resolves to the version focal tested.
- This workspace declares `rust-version = "1.98"`. focal pins 1.94.1.

### The lint wall

This repository's wall adds to focal's: `missing_docs`, `cognitive_complexity` (threshold 10), the cast
lints, `string_slice`, `panic_in_result_fn`, `unwrap_in_result`, `undocumented_unsafe_blocks` and
`disallowed_types`. Shipped code meets all of them with no `#[allow]`.

- **`missing_docs`.** 208 public items were documented: 124 methods, 33 enum variants, 19 struct fields,
  12 associated functions, 8 structs, 5 enums, 5 functions, 1 type alias and 1 trait. Each doc comment
  states what the code does; none changes it.
- **`cognitive_complexity`.** One shipped function was over the threshold: `Raft::step`, at 13/10.
  - It is split along the lines it already had, and the moved bodies are unchanged.
    - `counts_beyond_bound` is the check for a term or index at `u64::MAX`.
    - `Raft::step_term` holds a message's term against the member's.
    - `step_term` passes a newer term to `Raft::step_newer_term` and an older one to
      `Raft::step_older_term`.
    - The private enum `TermChecked` says whether the message goes on to its kind's handler.
  - Every branch and early return keeps its order.
- **Casts, `string_slice`, `panic_in_result_fn`, `unwrap_in_result`, `undocumented_unsafe_blocks`.**
  Shipped code had no finding. The crate has no `unsafe`.
- **`disallowed_types`.** Shipped code had no finding.
- **`clippy.toml`.** focal's seven `disallowed-methods` came with the core. These are raft-proto's
  enumeration accessors, which unwind on an unknown value.
- **Test code.** Tests keep the opt-out pattern the rules allow, so that they stay the unchanged proof:
  - `src/lib.rs`'s `cfg_attr(test, allow(..))` adds `cognitive_complexity`,
    `cast_possible_truncation` and `unreachable_pub`. These cover 19 test functions over the threshold,
    7 casts and 4 `pub` helpers in the in-crate test modules.
  - The crate-root `allow` of `tests/differential.rs`, `tests/fast.rs`, `tests/group.rs` and
    `benches/replicate.rs` adds the three cast lints, `cognitive_complexity` and `unreachable_pub`.
    These cover 30 casts, 9 test functions over the threshold and 58 `pub` helpers in `tests/support`.
  - No test code opts out of `disallowed_types`. The harness's disk had been an `Rc<RefCell<Disk>>`;
    see the next section.

### The harness's disk has one owner

In focal, the harness kept each member's disk as an `Rc<RefCell<Disk>>`, shared by the member's node
and by the cluster. This repository denies `Rc` in test code too. The harness now gives each disk one
owner at a time. This is a separate commit, so that the commit before it keeps the tests unchanged as
its proof.

- `Store` is the `Disk` itself.
- A running member's node owns its store, and the harness reaches it through the node: raft-rs's
  `mut_store`, or this crate's `store_mut`.
- `Cluster` holds each member as `Member::Up(node)` or `Member::Down(store)`.
  - `Cluster::stop` takes the store out of the node it drops.
  - `Cluster::restart` opens the member on what was stopped.
  - `Cluster::disk` reads a member's disk, running or stopped.
- The tests stopped a member by writing `group.nodes[i] = None`; they now call `group.stop(i)`
  (5 sites in `tests/fast.rs` and `tests/group.rs`). They read a configuration through
  `group.disk(id)` in place of `group.stores[i].0.borrow()` (2 sites in `tests/group.rs`). No assertion
  changed.
- The proof is the same as for the commit before. Every seed-fixed count the tests print is identical to
  focal `a8e95f7`'s, at 96 seeds and at 1,000 seeds from seed 1,000.

### Constants

Every value is unchanged. These doc comments now state where each value comes from:

- `Config::new` gives raft-rs 0.7's `Config::default` values, so the two cores compare under one setting.
- `CAMPAIGN_TRANSFER` is raft-rs's bytes.
- `approximate_bytes`'s twelve is raft-rs's `entry_approximate_size`, with its derivation.
- `FAST_PROPOSE` and `FAST_VOTE` are wire identifiers outside raft-proto's `MessageType` range (0 to 18).

Two kinds of value are literals, not derivations, and are marked so:

- `Limits::default`: mantle note 32 §2.10. `Limits::derive` replaces it in R-3.
- `MAX_MEMBERS`.

They are open against the no-arbitrary-numbers rule until R-3.

### Removed

- `benches/allocs.rs`, focal's allocation-count bench. It includes focal-memory's counting global
  allocator (`focal-memory/benches/support/alloc_count.rs`, 833 lines) by path. That allocator needs
  `unsafe` (`GlobalAlloc`) and a `std::sync::Mutex` site table. This repository allows `unsafe` only in
  files its contract script lists, and it has no such script yet. It also denies `Mutex` everywhere.
  - The counting allocator is shared test infrastructure, so it belongs with `hyper-sim` (note 32
    §3.10). It returns there, without the lock, as that crate's own work.
  - It was still run as evidence for this move; see Proof.
- The README's row for the TLA+ model. The model is still in focal (`docs/models/FastTrack.tla`), and it
  moves here with the fast-track decision (`docs/raft.md`).

### Proof

All runs used `--test-threads=4`.

- **The tests.** focal-raft's tests at focal `a8e95f7`, and this crate's tests changed only by the
  renames above, both pass:
  - 53 unit tests;
  - 6 differential tests;
  - 9 fast-track tests;
  - 7 group tests.
- **96 seeds.** Every count the tests print is identical at the default 96 seeds. In the differential
  that is, for each of the six mixes, the schedules compared, the steps compared, where each run ended,
  and the `Reached` totals. In the fast-track and group tests it is the terms led, the entries committed
  and the `FastStats`.
- **1,000 seeds.** The same holds at 1,000 seeds from seed 1,000 (`*_SEEDS=1000 *_SEED=1000`). The six
  differential mixes compared 22,275,363 steps and gave identical totals.
- **The one line that varies.** In `a_group_of_both_cores_is_safe_and_settles`, raft-rs draws its own
  election timeouts from `rand::thread_rng`, so the terms each core led vary from run to run. Three runs
  at focal `a8e95f7` gave 525/313, 497/340 and 494/282. Three runs here gave 496/318, 498/337 and
  484/316.
- **Allocations.** focal's `allocs` bench was run in a scratch clone of focal, on `a8e95f7`'s sources and
  then with this crate's `src/` in their place. Per committed entry, these are identical for every
  configuration:
  - allocations;
  - reallocations;
  - bytes requested;
  - peak and live growth;
  - the size histograms.

  Only the "moved by the allocator" column differs, and it differs for the unchanged raft-rs rows too:
  it records where the system allocator placed blocks.

## After R-1: the law of measurement (branch `raft-law`)

`CLAUDE.md` §1a: allocations, reallocations and page faults are measured on every hot path and
driven down, and the crate is benchmarked against each core it replaces. The measurements are in
`docs/benchmarks.md`; the counting allocator is `crates/hyper-measure` (it replaces the
`benches/allocs.rs` this move removed, without its lock, its `unsafe` in two files the contract
script lists). Two changes to this crate followed from them. Each decides exactly what the
crate decided before.

### Entries move into the log uncopied

- A leader's proposals were copied into the log not yet durable, through a staging vector, and a
  follower copied every entry of an append out of the leader's message the same way.
- They now move in: `Log::append_owned` and `Log::append_after_owned` take the entries by value,
  and when the log holds nothing not yet durable the incoming vector becomes its own.
- `Log::append` and `Log::append_after` keep their borrowed signatures and copy, as before.
- One count rose: a batch of appends to a log that holds nothing grows the adopted vector from
  its exact length, one reallocation more per batch than growing a fresh one. Each such batch
  saves one allocation and every entry's copy, so allocator calls fell in every workload
  (`docs/benchmarks.md`, "Optimisations").

### A `Ready` given in place

- `RawNode::ready_in_place` decides exactly as `RawNode::ready` and copies nothing the owner can
  read where it is: the owner writes from `RawNode::to_persist`, applies the range
  `Ready::committed_range` names from its own storage (`Log::next_range_since` chooses it by the
  rule `Log::next_entries_since` pages by), and keeps the entries and snapshot the member gives
  up at `RawNode::advance_append_keeping`.
- `RawNode::ready` and `RawNode::advance_append` are unchanged.

### A page's entries are read once

- A leader's page held what storage gave to the byte rule three times: storage's own count, a
  running total for what followed it, and a final cut over the whole page. It now counts each
  entry once, in the walk that also sums what the page's buffers hold, which the message queue's
  accounting had walked the entries again for (`Log::slice`, `Outbox::send_page`,
  `proto::message_bytes_with`).
- A storage that gives more than the rule admits is still cut by the rule (a unit test holds it).

### Proof

- All tests pass: 55 unit tests, 7 differential tests, 10 fast-track tests, 8 group tests.
- The differential runs every mix twice, with `Ready`s copied and given in place. `fast.rs` and
  `group.rs` run their schedules both ways and assert equal results.
- At 1,000 seeds from seed 1,000 every seed-fixed count the tests print is identical to focal
  `a8e95f7`'s (R-1's record, above), and each in-place mix's counts equal its copying twin's: the
  six mixes compared 22,275,363 steps each way.

## Ports from focal

focal's own session changes the core in its copy, `crates/focal-raft`, and each change is ported
here in a commit of its own, adapted to this crate's names and its optimisations (above). Each
keeps the raft-rs differential unchanged: the differential runs the rule raft-rs has wherever the
port adds one of its own.

### F43: reads asked together share one round of heartbeats (focal `6af1c6b`)

- A leader no longer sends a round of heartbeats as each read is asked. `Raft::ask_reads` sends
  one when the member is next asked for a `Ready` (`RawNode::ready` and `RawNode::ready_in_place`
  alike), carrying the context of the last read asked; a quorum's answer confirms it and every read
  before it (Ongaro's thesis §6.4).
- `ReadOnly::asked` marks how many reads the last round sent asks for; a round never confirms a
  read asked after it left (`ReadOnly::advance` takes the confirmed reads off the asked ones).
  `RawNode::has_ready` is true while a read is unasked.
- `Config::read_rounds` is `ReadRounds::Shared` by default. `ReadRounds::Each` keeps raft-rs's
  round per read, and the differential runs with it (`Settings::shell`).
- Tests: three unit tests from focal; the schedule harness checks every answered read against
  the highest index committed when it was asked (`Cluster::report`), and this core's schedules ask
  reads in bursts (`Mix::bursts`, `Op::Reads`); the directed safety test
  `a_round_confirms_no_read_asked_after_it_left`.
- Changed from focal's form: the round is asked in `RawNode::ready_given`, the one path both
  `Ready` forms take, so the in-place form sends it too; the schedule count of
  `schedules_of_this_core` returns the reads answered as well as the terms, so that the in-place
  twin is held to answer the same reads; the doc comments the lint wall asks for.

### F41: a member is sent no more bytes ahead of its answers than its path carries (focal `052ae4a`)

- `Inflights` holds each message's last index and the bytes of its entries, and is full at
  `cap` messages or at its byte bound. One entry larger than the bound is sent, alone; a bound is
  never zero.
- `Config::max_inflight_bytes` (default `u64::MAX`, no bound of its own; zero refused) seeds every
  member's bound, and `RawNode::set_inflight_bytes` sets one member's as its owner learns the
  path. `Tracker::new` takes the bound; `Progress::sent(last, bytes)` charges the window;
  `Progress::page_bytes` cuts a page to the window's room while entries are sent ahead of their
  answers; `Raft::check_accounting` checks every window's count of bytes.
- The differential runs with no byte bound (`Settings::shell`). This core's schedules run with
  256 bytes (`Settings::focal`, and so `Settings::fast`) and change bounds while they run
  (`Mix::windows`, `Op::Window`).
- Changed from focal's form: focal walked the page a second time after cutting it, summing
  `proto::encoded_bytes`, to charge the window. Here the bytes are counted in the walk that chooses
  the page (`Page::bytes`), which the byte rule makes anyway wherever it cuts: storage's page held
  to the rule, and the tail not yet durable. Where it does not count them (a storage that cut the
  page itself, or a page with no bound), the bytes are counted in the walk that already counts the
  page's buffers, and only for a leader's page (`Log::page`); `Log::slice`, which pages what is
  applied, counts nothing more than before. A unit test holds `Page::bytes` to the sum of the
  encodings on every path, against a storage that pages by the rule and one that gives too much.

### F42: a peer that answers nothing holds its own lane (focal `4bf7b64`)

Taken from focal's patch of `crates/focal-raft` `7ab57e0..04a45f6`, after focal gated `4bf7b64`
(`origin/slates-port`); the patch is byte for byte focal's diff `052ae4a..4bf7b64` of the crate.

- `HeartbeatAnswers::Position` (the default): a member's heartbeat answer carries its last index
  and that entry's term. Where the entry is of the leader's term, the answer is taken as an
  append's answer for everything through it (not in a fast group, whose terms differ by member), so
  lost answers are made good exactly and a full window gives back what the member holds and nothing
  more. A member whose window is full and that answers for none of it through a beat of the
  leader's ticks (`Progress::stalled`, counted by `Progress::tick` on the leader's ticks, never by
  heartbeat answers) is probed; a probe is sent again when told lost (`MsgUnreachable`) or once a
  beat has passed. `HeartbeatAnswers::Bare` keeps raft-rs's rule (answers say nothing; a full window
  frees its first message at every answer), and the differential runs with it.
- `Raft::settle_priority` puts no priority in force for a member that is not promotable: a member
  that applied its own removal refused, for good, the voter that remained. The directed test is
  `a_member_that_left_refuses_no_one_for_priority`. The harness's emulation of priority for
  raft-rs (`Old::settle`) takes the same rule, so the differential still agrees step for step; its
  `Reached` totals moved in the two mixes with priorities, `shell` (votes refused 2,217 → 2,215)
  and `plain` (terms 5,036 → 5,025, committed 76,316 → 75,998), and reverting the two lines of the
  rule restores them exactly.
- `Cluster::settles` proposes again a proposal whose leader was deposed before it committed.
- focal's documentation that the fast track is not safe as built (`fast.rs`, README) came with
  it; the next change below mends it.
- Changed from focal's form: the handling of a heartbeat's answer by `HeartbeatAnswers` moved
  unchanged into `Progress::heard_heartbeat`, since `Raft::handle_heartbeat_response` with it was
  over the lint wall's cognitive-complexity threshold (11/10); the harness reads a configuration
  through `Cluster::disk`, since its disks have one owner here.

## The fast track's safety fix

Not a port: this repository's own change, after the ports above. focal found the defect and
documented the fast track as not safe as built (F42); the design, the literature it cites and the
evidence are in `docs/raft.md`, "The fast track's election defect, and its fix".

- **First rule** (`Raft::fast_commit`): a member that holds the entry beside its log counts toward a
  fast quorum only once the leader knows its log holds an entry of the leader's term. Without it, an
  election committed a second entry at an index that held a committed one (seed 9843 of 40,000 from
  seed 3,000, reproduced on `e1e292c` before any change; directed test
  `an_election_never_commits_a_second_entry_at_a_committed_index`).
- **Second rule** (`Raft::fast_quorum_of_the_term`): a fast quorum counts only where it is one of the
  voters the leader was elected under and of the one other set of voters a change in its term named;
  after a change that names a third, none until the next term. Without it, a member counting by the
  configuration before a change it had not applied was elected and committed a second entry (seeds
  54104 and 203544, found once the first rule was in; directed test
  `a_member_that_counts_by_the_configuration_before_commits_no_second_entry`).
- The fast-track and README notes that it was not safe as built are replaced by the rules.
- Evidence: 160,000 fast schedules from four base seeds pass; the raft-rs differential, which runs
  no fast group, is unchanged; allocation counts in `docs/benchmarks.md`.

## R-2: its own types and its own wire format

The owner's decision (2026-10-01): hyper-raft speaks its own protocol. `raft-proto` is gone from
production; the format is `docs/raft.md` §3.1, written and read by `src/wire.rs`.

- **The types** (`src/proto.rs`) are plain structs with typed kinds: `MessageType`, `EntryType`,
  `ConfChangeType` and `ConfChangeTransition` are enums held as themselves, so a kind no value
  names is refused when bytes are read and never held. The fast track's two kinds, which were
  numbers past raft-proto's range (100 and 101), are `MsgFastPropose` and `MsgFastVote`.
  raft-rs's `deprecated_priority`, `sync_log` and the change `id` are not carried: hyper-raft never
  read them. `HardState` is `Copy`.
- **The format**: a record is a version, a kind, a fixed-width little-endian body with
  variable-length bytes after the fixed fields, and a CRC-32C. Every count and length is checked
  against the bytes left before anything is taken or allocated; an unknown version, kind, flag or
  presence bit, a checksum mismatch and trailing bytes are each refused with a typed
  `wire::DecodeError`.
- **An entry's bytes** are counted by the format: `proto::encoded_bytes` is the entry's length in a
  message (its 25 fixed bytes, its data and its context), and the unpersisted-entry count
  (`approximate_bytes`, raft-rs's twelve-byte estimate before) is the same length.
- **The empty change** has one encoding: no data. A leader's own leave entry always had none; a
  proposal of the empty change is now written the same way, and `Plan::of_entry` reads an entry
  with no data as the empty change (protocol buffers read empty bytes as the default message; this
  format refuses a record cut short, so the case is explicit). Found by the differential.
- **`apply_conf_change_v1`** builds the joint form of a single change directly (`proto::joint`)
  instead of encoding the change into an entry and reading it back.
- **The differential against raft-rs** (`tests/differential.rs`) compares the two cores as values
  through `tests/support/convert.rs`, which converts field by field and re-encodes a change carried
  in an entry's data between the formats. raft-rs counts an uncommitted change by its own encoding;
  the adapter replays raft-rs's rule (reset on taking the lead, counted past the tail, taken off on
  commit while leading, floored at zero) for the difference, so the counts compare in this
  format's bytes. Four settings compare step for step as before (the shell's, without pre-vote and
  check-quorum, a window of two and one entry a message, a network that loses nothing, each with
  readies copied and in place). Where a bound counts bytes (pages of 100 and 64 bytes, a ready's
  committed bytes, the uncommitted bound), the two cores cut at different entries by design, so
  raft-rs is no oracle there: this core runs those schedules alone and is held after every step to
  its bounds as it measures them (`alone`, `bounded`).
- **Tests of the format** (`src/wire.rs`): every layout written out field by field, every type
  round-tripped, every truncation, one-byte extension and single-bit flip of every record refused,
  counts past the bytes refused with a valid checksum, and 140,000 arbitrary bodies under valid
  checksums decoded without a panic.

## R-4: readies ahead of their persistence

Not a port: this repository's change, the first of the core steps the durable shell needs
(`docs/durable.md` §2.1, which states the design as built; `docs/raft.md` §3).

- **The calls** (`src/node.rs`): `RawNode::advance_issued`, `on_persist`, `on_persist_keeping` and
  `in_flight`; `Limits::readies_in_flight` (one by default) bounds the writes out, and a `Ready`
  beyond it is refused `Capacity`. `advance_append` and `advance_append_keeping` are the two at once
  and take a path of their own that passes through no queue. `outstanding` says a `Ready` is taken or
  a write is out.
- **The log** (`src/log.rs`): the unstable part keeps its entries until a notice says they are
  durable, with an issue mark (`Unstable::issued`, `snapshot_issued`, `unissued`,
  `unissued_snapshot`, `has_unissued`); `Log::take_stable_to` and `take_stable_snapshot` replace
  `take_stable_entries`, `stable_entries` and `stable_snapshot`, and refuse nothing: what a notice
  names that is no longer held is not made durable.
- **The fast track** (`src/fast.rs`): what a member approved by itself is given to one write
  (`Proposals::issue`, `unissued`, `has_unissued` in place of `unstable` and `has_unstable`).
- **A leader** (`src/raft.rs`): `Raft::become_leader` no longer refuses a log that is not durable,
  for a sole voter is elected while its writes are out; it counts itself by what is durable, as
  before.
- **What leaves when**: a leader's messages leave at once only while its term and vote are durable
  (before, a leader's always did; the raft-rs differential merges the two lists, and the case is a
  sole voter elected with learners); what a notice makes leaves with it only when nothing is out or
  unwritten.
- **Tests**: the unit tests of the calls, the ABA schedule (fails with the term guard taken out), the
  answers held for the write that holds what they say, a sole voter's term held before it sends;
  `tests/pipeline.rs` over `tests/support/lagged.rs` (a member whose persistence is three steps of
  the schedule) and the durability oracle of `Cluster::check_durable`. Recorded on 2026-10-02:
  1,000 schedules of 4,000 steps at each of three settings (three writes out, focal's at two in
  place, focal's at three with a window of two and one-entry messages) and twice 1,000 fast-track
  schedules: 72,670, 73,180 and 62,069 entries committed, 97,483, 64,903 and 158,208 `Ready`s taken
  behind another, 36,864, 31,263 and 72,679 notices of several writes, 8,305, 6,301 and 7,899 writes
  lost at crashes; 1,690 crashes, one at each persistence step of 40 schedules in turn. The harness's
  `Cluster::settles` drives a lagged member's writes and changes nothing a synchronous member does
  (the recorded-seed equivalence).
- **Equivalence of the synchronous path**: `HYPER_RAFT_SEEDS=300 HYPER_RAFT_SEED=1000 cargo test -p
  hyper-raft --release --test differential --test group --test fast -- --nocapture` prints the same
  coverage on `main` and here, every campaign and count, but one line: the group of both cores
  (`a_group_of_both_cores_is_safe_and_settles`) differs between two runs of `main` itself, since
  raft-rs draws its election timeouts from the thread.
- **Measured**: allocations identical on every workload, time within the noise
  (`docs/benchmarks.md`, "Readies in flight").

## R-6: the durable commit, the apply pause, applying before durability

Not a port: this repository's change (`docs/durable.md` §4.4, which states the design as built;
`docs/raft.md` §3).

- **The durable commit** (`src/node.rs`, `src/raft.rs`): `RawNode::durable_commit` and
  `commit_durable`; the issue mark keeps the commit each `Ready`'s hard state states, and its notice
  makes it durable; `Raft::durable_commit` opens at what storage states.
- **Answers** (`src/node.rs`, `state_durable_commit`): `MsgAppendResponse` and
  `MsgHeartbeatResponse` are made as before and held, where they leave, to the durable commit: at
  the `Ready` that takes them, with its own hard state's commit; at a notice, for a member that does
  not lead. Nothing is walked where the durable commit covers the commit, nor for a leader.
- **The apply pause**: `RawNode::pause_apply`, `resume_apply`, `apply_paused`.
- **Applying before durability**: `Config::apply_unpersisted` (off by default), `Log::unpersisted_after`,
  the apply bound and `Log::next_range_since` reading a range's tail where the log holds it.
- `RawNode::operate` is `#[inline]`: `sample` found it out of line on the proposal path once the
  member grew.
- **Tests**: four directed unit tests (`an_answer_states_no_commit_that_no_durable_write_stated`,
  mantle's case; `an_answer_a_notice_releases_states_the_durable_commit`, which fails on R-4;
  `an_owner_that_pauses_apply_is_given_nothing_more`, with a snapshot given while paused;
  `a_leader_applies_its_own_committed_entries_before_its_write_is_durable`). In `tests/support/lagged.rs`
  the oracle holds every answer's commit to the disk's when it leaves (`check_commit`), and the
  owner keeps the commit fence as a shell does: it states a commit only as `Ready`s give one, holds a
  change and every entry after it behind the fence with the core paused, writes the hard state
  alone to state the commit once no write is out, drops what it holds when a snapshot replaces it,
  and compacts only what its disk states committed. With the old answer rule in, the oracle fails
  at once ("member 1: MsgAppendResponse said commit 1 with 0 durable"). `tests/pipeline.rs` gains a
  fourth setting, a leader applying before its write in place with its disk the slowest
  (`Mix::leader_durable`), and the crash enumeration runs with and without it. A schedule draws
  exactly as before wherever a leader's disk is not slow.
- **Recorded on 2026-10-02** (`HYPER_RAFT_SEEDS=1000 HYPER_RAFT_STEPS=4000 HYPER_RAFT_CRASH_SEEDS=40`):
  1,000 schedules of 4,000 steps at each of four settings, 73,634, 72,589, 60,657 and 62,565 entries
  committed; answers held to the disk 308,533, 237,150, 293,537 and 222,532; changes held behind the
  fence 1,813, 905, 1,003 and 1,471; commits stated by a write of the hard state alone 7,781, 7,126,
  2,604 and 4,598; entries a leader applied before its write was durable 303 in the fourth. Twice
  1,000 fast-track schedules, 21,352 entries committed each. The crash at every persistence step of
  40 schedules: 1,689 crashes (1,656 writes lost) and, with a leader applying before its write,
  1,642 (2,030 lost, 58 entries applied ahead). Seed 730 of the narrow setting found the harness
  applying a held change over the snapshot that replaced it; the owner now drops it.
- **Equivalence of the synchronous path**: `HYPER_RAFT_SEEDS=300 HYPER_RAFT_SEED=1000` over the
  differential, group and fast suites prints what `main` prints but for the one line that differs
  between two runs of `main` itself. The differential needs no translation: where the owner writes
  every commit it is given, answers state what raft-rs's do.
- **Measured**: allocations identical on every workload; time in `docs/benchmarks.md`, "The durable
  commit and the apply pause (R-6)".


## L-2: elections by suspicion

Not a port: timing step L-2 (`docs/timing.md` §2.9, which states the design as built and the
source of each rule; `docs/raft.md` §3).

- **The mode** (`src/raft.rs`): `Config::elections`, `Elections::Ticks` (raft-rs's rule, the
  default, every existing suite and the differential) or `Elections::Suspicion`, which
  `Config::validate` refuses without pre-vote and check-quorum. On ticks nothing changes; the member
  carries `watch: Option<Box<Watch>>`, none on ticks, eight bytes.
- **What a member keeps by suspicion** (`src/watch.rs`, `Watch`): the members suspected (at most
  `MAX_MEMBERS`, refused past it), the timing (`Timing { span, round }`, `Timing::of` from
  hyper-timing's ballot and span), three timers on the owner's clock (a campaign, a leader's beat, a
  transfer's end: `Arm`), the campaign count the draw takes, the term it last led, and whether its
  owner holds its campaigns. `TRANSFER_ROUNDS` is two, a protocol fact.
- **The calls** (`src/node.rs`, `src/raft.rs`): `suspect`, `trust`, `restarted`, `set_timing`,
  `hold_campaigns`, `wake(now)`, `deadline()`; `tick` is refused. `Tracker::quorum_of` counts a
  quorum of each half by a predicate, for the trusted quorum.
- **Where the rules sit**: the lease (`step_newer_term`): while leading, or trusting a leader the
  request is not from, and the asker no later than this member's term; a leader asked for a vote
  answers with a heartbeat (`probe`). Granting a vote and every `reset` arm a campaign a round out;
  opening arms one from now, and a member that voted for itself in its term opens as one that led it
  (`led`), cleared when it follows another leader of the term (`followed`). Check-quorum and
  hand-over (`step_down`, `hand_over`; `post_conf_change` hands over where no voter holds the whole
  log). An older-term `MsgTimeoutNow` is answered as an older leader's heartbeat is
  (`step_older_term`), by suspicion only. A heartbeat's answer counts a beat as one tick by
  suspicion. hyper-timing gains `election_delay`, the draw `ElectionTiming::delay` makes, for a
  core given the span alone; hyper-raft depends on hyper-timing for it (a crate of this repository
  with no dependencies).
- **Tests**: `tests/suspicion.rs`, 15 directed tests in time (§2.9's list). The schedules' harness
  (`tests/support`) gains `Settings::suspicion` and each member's own clock (a tick moves it by
  `TICK_NS` and wakes the member), the detectors' words (`Op::Suspect`, `Op::Trust`; right nine in
  ten about a member down or cut off, wrong one in ten about one that is not) and the others told of
  a restart; it settles with every detector trusting every member. Its round tail and span are ten
  of a member's ticks, as `Settings::shell`'s election waits ten ticks and draws over ten more:
  measured on 24 five-voter fast-track schedules of 2,000 steps, a round of two ticks (the beat)
  held a leader in 7 % of their steps, of five 10 %, of ten 18 %, against 17 % on ticks (5 % at two
  before a reset waited a round). `tests/pipeline.rs` runs its
  three schedule tests by suspicion as well; the crash enumeration's default rises from three seeds
  to four, the fewest from zero at which every variant holds a change behind the fence.
- **Found by the schedules, each fixed in the core before the counts below**: a leader that stepped
  down kept its followers' trust (seed 1, `random_interleavings_by_suspicion`: its own campaign now
  ends their lease and makes them forget it); a leader that removed itself with no voter holding its
  whole log left followers trusting it (seed 7: hand-over); campaigns drawn from now after every
  reset overlapped in five-voter groups (a round first); a restarted leader whose old heartbeat
  arrived after its restart was trusted again (seed 1,322: `led` at open, and the older-term order
  answered); a voter with an empty log trusting a leader of an older term refused the one candidate
  that could win (seed 2,313: the asker's later term ends the lease).
- **Recorded on 2026-10-02**: on ticks, `HYPER_RAFT_SEEDS=1000 HYPER_RAFT_STEPS=4000` prints R-6's
  counts exactly (73,634, 72,589, 60,657 and 62,565 entries committed at the four settings), and
  `HYPER_RAFT_SEEDS=300 HYPER_RAFT_SEED=1000` over the differential, group and fast suites prints
  what `main` prints but for the mixed group's line, which differs between two runs of `main`
  itself (raft-rs draws its timeouts from its thread). By suspicion: 1,000 schedules of 4,000 steps
  at the four settings committed 75,539, 76,083, 67,930 and 66,354 entries in 4,561, 4,666, 4,482 and
  4,635 terms led, with 1,791, 793, 902 and 1,593 changes held behind the fence; 10,000 more from
  seed 1,000 committed 733,867, 746,577, 679,846 and 667,082. The fast track, twice 2,000 schedules
  of five voters, 39,691 entries each (42,064 on ticks). The crash at every persistence step of 20
  schedules: 943 crashes (1,078 writes lost) and, with a leader applying before its write, 904
  (1,178). The split rate over 1,000 crashes of a five-voter leader: 135 first rounds split
  against 110.8 expected.
- **Measured** (`docs/benchmarks.md`, "Elections by suspicion (L-2)"): allocations identical on all
  22 cells of the comparison; time and cycles within `main`'s intervals or below them, instructions
  0.1–0.4 % above; `benches/pipeline.rs`'s simulated figures identical. `benches/idle.rs`: an idle
  group costs its owner 542.5 ns and two messages a tick on ticks, 25 ns a scan and no message by
  suspicion, nothing with a timer queue.

## R-5: repair by entries

Not a port: this repository's change (`docs/durable.md` §5.1, which states the design as built and
its sources; `docs/raft.md` §3).

- **The mark** (`src/raft.rs`): `Lost { index, term }`, `Config::lost`, `Raft::lost`,
  `Raft::settle_lost` (at open and at every notice, from storage: `Lost::resolved_by`, hyper-log's
  rule), `Raft::claim` (what a member answers for in an election). mantle's rules for a marked
  member, until now the shell's, are the core's: `step_vote` judges by the claim (the vote and
  `Precedence`), `hup` refuses (`Error::Lost`) and forgets the leader that asked it to campaign by
  its silence, `deadline` and `wake_follower` arm no campaign, and `step_vote` refuses no one for
  priority while marked.
- **The word** (`src/proto.rs`, `src/wire.rs`): `Message::lost`, flag bit 2 of the message body; an
  older reader refuses it as an unknown flag. `Raft::send_lost` answers a heartbeat whose commit
  passes the log, or an append after a point past it, while marked.
- **The repair** (`src/raft.rs`, `src/progress.rs`): `Raft::take_lost` and `Progress::lost`; the
  named entry checked against the leader's log where it still holds it (a schedule found the
  leader's compacted log reading term zero there, read as a mismatch, before the check asked
  first whether it holds the entry); `RawNode`'s notice settles the mark (`src/node.rs`).
- **Tests**: `tests/repair.rs` (four directed); `src/wire.rs` pins the lost refusal's layout and
  round-trips it. The schedules' harness (`tests/support`): a checksum beside every entry of the
  disk (`Disk::sums`), verified when a member opens (`Disk::verify`, cut at the first mismatch and
  marked through what it held); `Fault::Flip` and `Fault::Lose`, `Op::Corrupt` and
  `Mix::corrupt`, one marked member at a time; the oracle counts a mark for what its member
  acknowledged (`check_durable`); `Cluster::check_kept` after every settled schedule;
  `Cluster::electable`, which a group that does not settle must leave empty (a group whose marks
  the rule must wait on is counted, not failed). `tests/pipeline.rs`:
  `faults_at_rest_lose_nothing_acknowledged` (the four settings on ticks) and the crash at every
  persistence step with faults (two variants on ticks: by suspicion a marked member that led cannot
  end its followers' lease on its node until R-7 lets it campaign). A schedule without faults draws
  as before: no draw is taken where `Mix::corrupt` is zero.
- **Found by the schedules, fixed before the counts below**: the leader's term check of the named
  entry where its log was compacted (seed 1); a marked member of the highest priority refusing
  every candidate it was not behind, which no one else could then be elected past (seed 560, the
  shell setting).
- **Recorded on 2026-10-02** (`HYPER_RAFT_SEEDS=1000 HYPER_RAFT_STEPS=4000 HYPER_RAFT_CRASH_SEEDS=40`,
  load 27–33): every schedule without faults prints R-6's and L-2's counts exactly (on ticks 73,634,
  72,589, 60,657 and 62,565 entries committed; by suspicion 75,539, 76,083, 67,930 and 66,354; the
  crash enumeration 1,689 crashes and 1,656 writes lost, and 1,642 and 2,030, on ticks; 1,867 and
  1,889, 1,764 and 2,049 by suspicion). With faults at rest, 1,000 schedules of 4,000 steps at each
  setting: 61,806, 53,933, 43,583 and 50,087 entries committed through 2,815, 2,878, 2,979 and 2,880
  faults; 1,953, 1,880, 2,132 and 1,999 marks ended, after 967, 952, 890 and 968 steps on average;
  134, 192, 116 and 128 groups left waiting on a mark the rule could elect past no one (§14.5's
  measure, before R-7). The crash at every persistence step of 40 schedules that suffered a fault:
  1,878 crashes (2,374 writes lost, 1,277 faults) and, with a leader applying before its write, 1,769
  (2,478, 1,067). `HYPER_RAFT_SEEDS=300 HYPER_RAFT_SEED=1000` over the differential, group and fast
  suites prints what `main` prints but for the mixed group's line, which differs between two runs of
  `main` itself.
- **Measured** (`docs/benchmarks.md`, "Repair by entries (R-5)"): one lost entry of 30,000
  repaired in 1.8 KB and 32 µs in process against a snapshot's 30.7 MB and 8.9 ms; allocations,
  reallocations and bytes identical on all 32 cells of the comparison; instructions 0.2 % above
  `main` an op after three hot-path costs found and removed (the first build was 1.2 % above); no
  interval of time above 1; `benches/pipeline.rs`'s simulated figures identical.

## R-7: a marked member's election

Not a port: this repository's change (`docs/durable.md` §5.2, which states the rule, the safety
argument, the configurations and marks it covers, and CTRL mapped onto it; `docs/raft.md` §3).

- **The rule** (`src/raft.rs`): `Raft::may_campaign` lets a marked member campaign where the others
  can be a quorum of each half of its configuration (`Tracker::quorum_of`) and the group has no
  fast track; `Raft::campaign` polls its own vote as a refusal while marked; `trusted_quorum` counts
  a marked member out of its own quorum; `Raft::become_leader` ends the mark; `commit_apply` takes a
  campaign a change made impossible as the refusal it is (a schedule found `advance_apply_to`
  returning `Error::Lost` where a marked member had been told to campaign once a change applied).
- **Two rules of L-2 found wanting by the schedules with faults, by suspicion** (`docs/timing.md`
  §2.9 states both): a leader that restarted and cannot campaign hands over to each heir that holds
  as much in turn (`hand_over`, by `Watch::attempt`), where it named the lowest for good (seed 75: a
  member that was a learner by its own configuration, while the followers kept their lease on the
  leader's node); and a leader told a member started again probes it with its window emptied
  (`restarted`), where it waited on ten messages that went with the old incarnation, freeing one a
  beat (seed 478, a window of one byte, `HeartbeatAnswers::Bare`). Each has its directed test in
  `tests/suspicion.rs`, which fails without it.
- **Tests**: `tests/repair.rs` gains three: PAR's Figure 4(b) in suffix form (fails on R-5: "no one
  was elected"), a marked log that may lack a committed entry never elected (fails with the
  candidate's own vote counted, and with the voters judging by their logs: "committed another entry
  at 5"), and a marked member of one or two voters asking no one. The schedules' harness:
  `Cluster::electable` holds the rule (a marked candidate counted out of its own quorum, none in a
  fast group), `Mix::marks` the most members marked at once; `tests/pipeline.rs` runs the faults at
  rest by suspicion too, and with two of three voters marked at once
  (`faults_at_rest_on_two_members_at_once_lose_nothing_acknowledged`); the crash enumeration with
  faults runs by suspicion too. Mutated, the schedules fail at once: the candidate's own vote counted
  (seed 26: "member 2 committed another entry at 9"), the voters judging by their logs (seed 4).
- **Recorded on 2026-10-02** (`HYPER_RAFT_SEEDS=1000 HYPER_RAFT_STEPS=4000 HYPER_RAFT_CRASH_SEEDS=40`,
  load 44–48): on ticks every schedule without faults prints R-6's counts exactly; by suspicion the
  two L-2 changes move them (74,181, 77,062, 68,754 and 66,719 entries committed at the four
  settings, against L-2's 75,539, 76,083, 67,930 and 66,354), and the fast track's 19,826 (19,252).
  With faults at rest, one marked member at a time, on ticks: 58,963, 51,879, 42,891 and 48,102
  entries committed through 2,774, 2,736, 2,907 and 2,818 faults, 148, 186, 97 and 132 groups left
  waiting on a mark; by suspicion 60,246, 56,054, 45,361 and 51,424 through 2,782, 2,829, 2,892 and
  2,875, 137, 181, 121 and 132 waiting. Two marked at once, on ticks: 413, 452, 334 and 428 groups
  waiting through 4,370, 4,165, 4,282 and 4,216 faults, where R-5's rule, the same harness and seeds,
  left 422, 523, 516 and 489 (1,627 against 1,950); by suspicion 384, 453, 356 and 412. The crash at
  every persistence step of 40 schedules with faults: 1,880 crashes (1,279 faults) and 1,772
  (1,070) on ticks, 1,953 (1,437) and 1,789 (1,023) by suspicion. `HYPER_RAFT_SEEDS=300
  HYPER_RAFT_SEED=1000` over the differential, group and fast suites prints what `main` prints but
  for the mixed group's line.
- **Where the groups wait.** Of 11 waits in 200 schedules inspected, every one was a group whose
  configuration (or a half of a joint one) held two voters or one, with the marked member among
  them, or whose committed entries were held only by a member a change had removed: the rule's
  bound (`docs/durable.md` §5.2), not a defect. R-7's gain is where the only current logs are marked
  in a group of three or more, which two marks at once make common and one rarely.
- **Measured** (`docs/benchmarks.md`, "A marked member's election (R-7)"): allocations identical
  on all 32 cells; instructions within 0.3 % of `main`'s an op; at load 42–57 no interval of time or
  cycles above 1 in a second pass of the cells the first pass leaned on, where `main` with 48 inert
  bytes moved one by 3.4 %; `benches/pipeline.rs`'s simulated figures identical.


## A draw at every arming

Not a port: found when `tests/suspicion.rs`'s split test was made exact (the owner's rule,
2026-10-03: no test passes or fails on a statistical level).

- **The test.** It predicts each crash's first round from the four delays the members armed before
  the crash runs: the law's event (Ongaro, dissertation §9.2), three of the four starting within the
  one-way latency of the first, splits it, with pre-vote as well (the first starter's vote requests
  land three latencies after its draw; a member that started within one latency of it is a
  candidate two latencies after its own, before they land, and refuses). Every one of 1,000 crashes
  came out as predicted, before and after the change below; the 99.9 % interval on the count it
  replaced is gone. The delay test checks the law's draw exactly; its ten bins, judged within five
  standard deviations, are gone (how the draws spread is the law's to show).
- **The defect.** The draw's index was the member's campaign count (`Watch::attempt`), so a member
  that did not campaign kept its delay from one election to the next. The members whose delays fired
  drew again and the others kept theirs, so the delays an election ran on leaned long: 135 first
  rounds of the 1,000 split, against the law's 110.8 for independent draws. Raft's randomized
  timeout is drawn anew at every reset (§5.2, §9.3), and the law's split probability takes the draws
  as independent.
- **The rule** (`src/watch.rs`, `src/raft.rs`): every arming draws anew (`Watch::draw`, the index
  `Watch::draws`); a campaign, a hand-over and an unresolved round each arm, and so draw, once. The
  hand-over's turn among heirs that hold as much has its own count (`Watch::handovers`), taken in
  `hand_over` itself, one a hand-over. `every_arming_draws_anew` (a member opens, follows a leader,
  and twice suspects it and trusts it again: each delay is the law's draw at the next index) fails
  on the kept draw ("suspicion 1": the opening's draw again) and passes. 99 first rounds of the 1,000
  split.
- **What it costs where the detectors are often wrong.** The schedules' detectors are wrong one time
  in ten about a member that is up, and the kept draw had made campaigns on those suspicions rarer:
  the long delays it kept outlasted short wrong suspicions. A member that campaigns knows no leader
  until one answers it (raft-rs's and etcd's pre-candidate, `Raft::become_pre_candidate`), so a
  proposal made to it is not forwarded, and it casts no fast-track vote (`Raft::hold` votes only as
  a follower that knows its leader). Counted over the fast-track schedules by suspicion, 2,400 of
  2,000 steps, with the draw kept and with it drawn anew: pre-vote requests 295,051 and 302,452,
  proposals forwarded 6,170 and 5,665, fast-track votes 47,443 and 43,389, entries committed 25,732
  and 24,309, terms led 4,608 and 4,534 (on ticks 26,384 in 4,628 for both; with the hand-over's turn
  counted as before, 24,302). No message was lost to the schedules' network bound in either. The
  kept draw bought those entries by electing later and splitting more on a real failure, which is
  what the delay is for.
- **The schedules' coverage checks** (`tests/pipeline.rs`) ask that each mechanism was reached, not
  that it was reached a picked number of times: 24 fast-track schedules by suspicion committed 185
  entries, under the eight a schedule the check had asked since R-4. "Every schedule suffered faults
  at rest" was never what it checked, nor true: seed 11 of the shell's three writes out draws none.

## A campaign supersedes the requests still waiting

Not a port: found moving mantle's range replica onto the durable shell (mantle's D-1,
2026-10-03). mantle's replica held the ticks that came while a `Ready` flushed and capped them at
the longest election timeout the core draws, after a follower whose device stalled for 100 timeouts
replayed them as 116 vote requests at once (mantle `docs/design/replica.md` §3). Since R-4 the shell
steps ticks into the core while its writes are out, and a follower whose device holds its writes
through many timeouts campaigns at each, as its clock says. Its requests wait in the core's queue,
for every message of a member that does not lead leaves with the write of the `Ready` that takes it,
and once the device goes on every campaign's leave together: 234 requests, two for each of 117
campaigns, after a hundred of the longest timeouts the core draws, with three writes out, measured
on mantle's range group of three over hyper-log with one member's store answers withheld (mantle
`crates/range/tests/group.rs`,
`a_member_whose_writes_stay_out_through_many_timeouts_sends_one_campaign_a_write`).

- **The rule** (`src/raft.rs`: `Outgoing::supersede_requests`, called by `Raft::campaign`): a
  campaign drops the member's own vote requests, pre-vote or vote, that wait to be taken. Each asked
  every voter, and the campaign asks each again; an answer to a campaign the member gave up can win
  it nothing, for its votes were reset when it campaigned anew (`become_pre_candidate`,
  `become_candidate`). Dropping a message that has not left is what the network may do to one that
  has, so no invariant moves and the TLA+ model is unchanged.
- **The test.** `a_campaign_supersedes_the_requests_of_those_before_it_still_waiting`: a follower
  ticked through a hundred of the longest timeouts it draws, no `Ready` taken, holds its last
  campaign's two pre-vote requests. Before the rule it held 400, two for each of 200 campaigns.
- **Where it shows.** Only to an owner that leaves the core's queue untaken across campaigns, as the
  shell does while its writes fill its store's depth: after a stall the member sends at most one
  campaign's requests for each write it had out and its last campaign's. The raft-rs differential
  drains every `Ready` after each operation and compares unchanged, as does every suite of this
  crate. The cost is one walk of the queue a campaign, over what the member queued since its last
  `Ready`; nothing is allocated.

## A spare queue for each ready in flight

Not a port: found tracing mantle's range replica on the durable shell (mantle's D-1, 2026-10-03)
against mantle's own shell, whose driving thread it out-reallocated by 0.02 an entry once a group
had run 12,000 entries. Every reallocation of an 8-aligned block on that thread was recorded with
its sizes, and the excess was one: a follower's queue of messages growing from four slots to eight
(`Outgoing::push_counted`, from `handle_append_entries`), 176 times in 12,000 entries at three
members and 212 at five, where mantle's shell grew none. Each `Ready` hands the member's queue to
the owner (`Outgoing::take`) and leaves the spare the owner gave back in its place; the shell gives
each write's queue back only once the write is durable (`RawNode::recycle_messages`), and with three
writes out it takes three `Ready`s before the first comes back. The member kept one spare and
dropped every other queue given back, so the second and third `take` started a queue from nothing,
and a follower that queued more than four answers while its writes were out grew it again.

- **The rule** (`src/raft.rs`: `Outgoing::recycle`, `Outgoing::take`): the member keeps up to one
  spare for each `Ready` whose write may be out (`Limits::readies_in_flight`, which the shell sets to
  its store's depth), ordered by room; a `take` leaves the one with the most, and past the bound a
  queue with more room replaces the one with the least. The bound on any one queue
  (`Limits::pending_messages`) stands, and `Outgoing::resident_bytes` counts every spare. An owner
  that keeps one write out (`readies_in_flight` one) keeps one spare, as before.
- **The tests.** `raft::outgoing::a_member_keeps_a_spare_queue_for_each_ready_in_flight`: three
  queues of eight given back while the member queues, then three takes, each starting with room
  for eight when three are kept, and only the first when one is;
  `raft::outgoing::spares_past_the_bound_keep_the_most_room`.
- **Measured** with `crates/hyper-durable-compare` on the simulated device, 4 rounds of 3,000
  entries after 12,000 (`docs/benchmarks.md`, "mantle's range replica on the shell (D-1)"): the
  shell's driving thread reallocates 21.00 times an entry at three members and 33.00 at five, as
  mantle's shell does, against 21.02 and 33.02 before; the growths from four slots fall to 1 and 22.
  After only 50 entries the first 3,000 still grow each circulating queue to its high water once,
  about 18 growths a group of three more than mantle's shell makes, 0.01–0.02 an entry over those
  3,000; they are done by 3,000 entries, after which the counts are mantle's.
