# hyper-log: origin

- **Source.** mantle's `crates/log` (mantle-log) at mantle `147f035`
  (`147f0355513e802fd4b8b3fa3717d8cc1138ded5`), brought in with its history by `git subtree`.
  - The imported tree hash equals mantle's `147f035:crates/log`
    (`96dc4f2375283a2db82644c41fb58edb49393a87`).
  - Its design is mantle's `docs/design/raft-log.md`, which this crate keeps citing; the on-disk
    format is mantle's, unchanged (format 3, magics `MNLS`, `MNLF`, `MNLP`), so hyper-log reads the
    files mantle-log wrote and writes the files it reads.

## L-1: moved, behaviour unchanged (commit `5bd0699`)

- Package `hyper-log`, on `hyper-block` (`../hyper-block/ORIGIN.md`).
- mantle-codec's `Reader` and `Writer` are `src/codec.rs`; CRC-32C is the `crc32c` crate (RFC 3720
  §B.4's vectors and a pieces-equal-the-whole property test added), where mantle-crc used
  `crc-fast`.
- **Equivalence** (`tests/equivalence.rs`): 24 seeds, each four cycles of 40 rounds of groups
  appending, conflicting, compacting, voting, proposing, leaving and sending invalid updates, each
  round's batch fixed by holding the device in a flush, with power cuts, random-sector crashes and
  bit flips at rest between cycles, so opens restore, mark and fence. The same harness, renamed, run
  against mantle `147f035` wrote 48 files: transcripts of every answer, view and recovery, and the
  device image with each segment's random nonce replaced by its incarnation and the checksums over
  it recomputed (a checksum that failed keeps failing). This crate writes the same 48 files byte for
  byte, at L-1 and at every commit since. `EXPECTED` holds mantle's hashes of them.

## L-2: one owner, tickets, fetches by ticket (commit `4e58930`)

mantle note 32 §3.9 and §5.2 L-2.

- **The owner** (`src/owner/`). One thread holds everything the log knows: groups, segments, the
  queue's room and its waiters, the writer's batch and fair queue. mantle shared it behind
  `Arc<Shared>` with a `RwLock<State>`, a `Mutex` around the room and atomics. Callers reach the
  owner through a bounded inbox; nothing else is shared.
- **Tickets** (`src/ticket.rs`). Every call carries a ticket and is answered through a reply port of
  the caller's own, which only the caller receives on, so an answer unparks only its caller; a
  `Waker` given with a submission is woken once, after its answer. mantle answered through an mpsc
  channel per submission and woke a waker beside it. Ports are reused: the answer carries the port's
  sending end back, and a thread keeps the ports it has finished with.
- **The device thread** (`src/device.rs`). The owner never waits on the device: it hands each I/O
  (a frame and its persist record then one flush, a confirmation, a sweep's read of the tail,
  entries read back, a caller's look at the file) to the log's device thread, which owns the file,
  and goes on answering callers; each completion comes back to the owner as a message. mantle's
  writer thread wrote and flushed while readers took the lock. A log runs these two threads,
  whatever its groups or callers. The device runs each job inside an unwind boundary, since the
  file is the caller's code.
- **The writer's rules are mantle's** (`src/writer.rs`), run by the owner as steps between messages
  (`src/owner/write.rs`); the order of everything the log decides is mantle's, which the equivalence
  shows.
- **Fetches by ticket.** `Log::fetch` and `Log::fetch_waking` copy entries into the caller's
  reservation (`Fetched`), which comes back with the answer and keeps its capacity. `Log::entries`
  is that, copied out into `Entry`s.
- **Owned bytes.** `Entry` and `Proposal` hold `Vec<u8>`, which the log takes and keeps while the
  entry is recent; mantle held `Arc<[u8]>`.
- **Files back.** `Log::close` gives the file back once everything taken is answered;
  `Log::try_open` and `Log::try_create` give it back with a refusal (`Refused`). `Log::with_file`
  runs a closure on the device thread, how a simulation reaches the device the log owns.

### Tests

The suite's assertions are unchanged; its plumbing is not:
- a shared `Arc<SimFile>` is now the log's while it runs, reached by `Log::with_file`, and taken
  back by `Log::close` or a refusal;
- the devices that held the writer (`Stepped`, `Gated`, `Gate`, each a `Mutex` and a `Condvar`) are
  one `Held` device the test commands over a channel (`tests/common`), which also arms a fault while
  the device is held;
- threads share the log by reference in a scope;
- the room's three tests drive the owner-held room directly and assert who is told, where they
  counted thread wakes;
- `a_waker_is_woken_once_for_each_answer` counts each waker's wakes (`hyper_measure::wake`) where it
  read a channel's disconnection;
- `a_completion_wakes_only_its_submitter` is new: 48 submitters with counting wakers over 12 rounds
  each, every wake its own answer's, each waker woken exactly as often as answered.
- `a_groups_queued_updates_become_durable_in_order` hung once on Linux, in mantle's test as
  written: each round holds the writer by a plug update and waits for its flush, but a log full of
  what the groups keep refuses the plug `Full` with no frame written, so no flush ever came. The
  round now waits for the plug's flush or its answer, whichever comes first (its waker tells the
  holder), takes a `Full` or `Backlog` plug as the refusal it is, and runs that round unheld; the
  holder forgets each round's events before the next. A direct case (a 4-block, 8-segment log
  filled by one group's 8 KiB entries) shows the plug refused `Full` unheld. mantle's test at
  `147f035` carries the same wait.

## Allocations (commit `4b48295`)

Appends and fetches into a reservation allocate nothing once warm: `docs/benchmarks.md`,
"hyper-log". What changed, none of it in the bytes written:
- format: entries records encoded and sized from the update's entries with no list made of them; a
  frame header laid into an array; a persist record encoded into a kept buffer;
- placements are offsets, an entry's place following from its record's;
- the batch order sorts in place, with unstable sorts over keys made unique by position, which order
  exactly as mantle's two stable sorts;
- the owner keeps from frame to frame the payload, the persist record, the aligned buffers, the
  ordering scratch, the batch's deque, one update list for each frame it holds at once, and a fetch's
  lists.

## The wall (commit `af2679b`)

- Every public item documented, every const with its derivation.
- Functions over the complexity threshold split along the lines they had, the moved bodies
  unchanged (the commit lists each).
- Test opt-outs as the rules allow: `cognitive_complexity` and `unwrap_in_result` at test crate
  roots. No test opts out of `disallowed_types`: proptest's `prop_oneof!` boxes its arms in `Arc`,
  so the step strategy draws the kind as a number by the same weights. The seven cases
  `log.proptest-regressions` recorded are run as the inputs they shrank to
  (`recorded_regressions_hold`), since the file's seeds replay other inputs under the new strategy.

## Reads where the replica is, two hand-offs a write (commits `d04934e`, `7585e19`)

mantle measured its replica path on this crate at 828 µs a committed entry against 159 µs on its
own `crates/log` (mantle `docs/measurements/2026-10-01-shared-log.md`): every read was a round trip
to the owner thread, and every write crossed four threads. Both are fixed at their cause.

- **A group's handle** (`src/group.rs`, `Log::group`). A replica reads and writes its group
  through the group's `GroupLog`, which keeps, on the replica's thread, what the core reads: the
  start, the last entry, the term of every retained entry as runs of one term, the hard state and
  marks, and the bytes of the recent entries within `group_cache`, by the log's own cache rule. It
  is exact because the core reads its storage only between its own writes (hyper-raft's `RawNode`
  takes no call while a `Ready` is out) and only the group's own writes move what it reads; each
  write's answer brings its entries back, the log having written them, and the handle applies it
  as the log did (`writer::apply`). It asks the owner only for entries older than its cache, a view
  with proposals, and everything after a failure it cannot account for. The log holds the group
  for its handle: another writer is refused `LogError::Claimed`. No shared memory, no atomics, no
  `unsafe`. A property test checks every read of the handle against the log's own after every
  write of a random history (`tests/group.rs`).
- **Leader/followers** (`src/owner/mod.rs`, Schmidt, O'Ryan, Pyarali, Kircher and Buschmann,
  PLoP 2000), since replaced ("The owner on its own thread", below). The two threads take turns holding the owner; the one with I/O to do hands the owner
  to the other and does the I/O itself, the device travelling with the job. It finishes a frame as
  mantle's writer did: the frame before answered once this frame's record confirms it; then, unless
  the owner said another frame follows, this frame confirmed and answered. The owner hears of the
  flush before the confirmation is written, through a side channel it reads before every message,
  so a frame is published once flushed without waking it. A write crosses from its caller to the
  thread that flushes it and back. A blocking write is no longer woken for its admission.
- **Fixed with it** (`7585e19`): a round's framing no longer depends on how fast its callers
  return. The busy period of the fair queue ends when the answers go out with nothing queued, not
  after the owner has drained what answered callers sent since; and the device hears that another
  frame follows before the caller hears it is admitted. The equivalence's rounds now wait for the
  plug's flush or its answer: `is_held` never consumed its event. Together these were the
  equivalence failures on Windows and macOS CI, reproduced pinned to one core
  (`docs/benchmarks.md`, "Equivalence").

## The owner on its own thread, the blocking caller flushing its own frame

The replica's path cost one context switch a write more than mantle-log (19.5 a committed entry
against 12.1, six writes). The cause was the hand-off itself: with leader/followers, the thread
that took a submission handed the owner to the other thread before flushing, so the other woke,
took the owner, and went to sleep on the inbox while the frame was flushed; then both log threads
slept for every write (the one waiting to lead again, the one waiting for the next message) as
well as the caller. mantle's writer had one thread sleep besides the caller. Waking the owner only
when it waited for the device left the count where it was, since the wake was the hand-off's.

- **The owner stays on its thread** (`src/owner/mod.rs`) and never does I/O. It hands each job,
  with the device, in a box kept from job to job (`device::Carrier`), to a thread that does it:
  a caller whose update the frame carries and who waits on its answer from the moment it
  submitted (`Log::write`, `Log::write_waiting`, `GroupLog::write` with nothing else out), which
  is awake for its answer anyway, through its reply port (`Reply::Io`); otherwise the log's I/O
  thread (`device::serve`). The caller does the frame's write, flush, confirmation and answers on
  its own thread, as mantle's writer did on its. A caller that leaves its wait with a job in its
  port does the job as it leaves.
- **Jobs come back without a wake.** The completion and the device go into the owner's returns,
  which the owner reads before every message, so anything sent after an answer the job gave is
  heard after the completion that gave back its room: release, then answer, as when mantle's
  writer answered under its lock. The job's thread wakes the owner (`Message::Returned`) only for
  a completion that leaves it something to answer: a frame another frame will confirm, a failure,
  a sweep, a read, a look. When the owner has work waiting on the device (submissions for the
  next frame, waiters for room, I/O, a close), it asks the I/O thread to wake it once the job is
  back (`Request::Watch`): every job's thread sends the job's sequence on a token channel after
  the job is back, which the I/O thread reads while it watches and the owner empties otherwise.
- **Measured** (`docs/benchmarks.md`, "The replica's path"): 12.0 switches a committed entry,
  mantle-log's count, and the wall time at or below mantle-log's in the same runs; allocations
  and reallocations unchanged (72.2, 0.2).
- **Tests**: a blocking writer's two flushes happen on its own thread, a non-waiting submitter's
  on another (`a_blocking_writer_flushes_its_own_frame`); a submission taken while a frame
  confirms itself is written (`a_submission_taken_while_a_frame_confirms_itself_is_written`,
  which hangs with the watch removed); and room is given back before the answer, for a write, a
  submission answered by the I/O thread and two refusals, with a queue of one
  (`room_is_given_back_before_the_answer`, the check focal's `5219002` asked of every writer).

The 48 equivalence files are byte-identical to mantle-log's throughout.

## A group's handle splits its own updates

- **`GroupLog::parts`** (`src/group.rs`): an update in the parts that each fit a frame, exactly as
  `Log::parts` gives them, both now `parts` of `src/lib.rs` over the frame's room, which the handle
  holds; no message to the owner. mantle's range store kept the log beside the handle only to call
  `Log::parts`: a replica needs nothing but its handle.
- **Test** `a_handle_splits_an_update_as_the_log_does`: a property test over updates that fit, split
  into several frames, or are refused as too large; the handle's parts are the log's, and it asks
  the owner nothing.

## A waking submitter does not wait for its admission

mantle measured its `mantle bench log` at 256 replicas of 128 B on this crate 2–4% below its
`crates/log` (mantle `docs/measurements/2026-10-01-group-log.md`), its frames gathering nearly
every replica where mantle-log's carried 127–253.

- **The cause** was not the writer's wait, whose rule and inputs are unchanged. `Log::submit_waking`
  waited for its admission, a round trip to the owner on every submission, and the owner woke each
  caller as it admitted it. A thread keeping many submissions out could send its next only once the
  owner had admitted the last, so the submitters a frame answered came back one owner wake apart
  (about 4 µs each at load 30) and the writer's gathering of them, 1.1–1.2 ms a frame, was the
  owner's own work while the file sat idle: it was blocked on its inbox for 55–65 µs of it, and 145
  of its 191 busy samples were in `Ticket::admit`'s wake. The wait went on because each next
  submitter came within it. Counting the file's flushes showed the rest of mantle's reading: both
  logs carry about 128 appends a flush, the protocol's bound at 256 replicas (a replica's append
  takes its frame's flush and the next durable record's), so the frame counts compared frames, not
  flushes.
- **The change** (`src/lib.rs`): `Log::submit_waking` returns once the submission is on its way; the
  caller hears everything through its waker, as a group's handle already did. A submission without
  room still waits in the log; a refusal at admission (`Busy` past the waiters, `Fenced`, `Claimed`)
  is its answer. `Log::submit` and `Log::submit_waiting` are unchanged.
- **Measured** (`docs/benchmarks.md`, "Many small appends"; `hyper-log-compare` now drives mantle-log
  `a2021df` in-process exactly as it drives hyper-log, and counts the file's flushes): at load 26–34,
  29,772 appends a second at 128 B and 256 replicas against mantle-log's 27,726 and main's 26,703,
  ahead in every paired round, p50 and p99 lower; 23,079 at 16 KiB against 20,555 and 21,470; one
  replica unchanged (the device's); the replica's path, allocations and reallocations unchanged.
- **Built, measured and not kept**: the device waiting for a frame to follow before confirming one
  on its own, and the wait weighing the unconfirmed frame's appends. Together with the change they
  ran 4% below the change alone.

### Tests

- `a_waking_submitter_does_not_wait_for_its_admission`: with the queue's one submission held in a
  flush, a waking submission returns at once and waits in the log; once the flush is let through it
  is written and answered, its waker woken. It hangs before the change, the call waiting for room
  behind the held flush.

The 48 equivalence files are byte-identical to main's, `EXPECTED` unchanged; on Linux the
equivalence passed pinned to one core 200 times and unpinned 50.

## Not done

- The writes through hyper-block's device issuer (mantle `docs/design/node.md` §1.2): the log does
  its own I/O on its second thread until then.
- Entries read from the file wait behind a frame's write and flush: one I/O at a time.
- mantle's replica shell on this crate (D-1), and mantle and focal consuming it (F-1).

## Openers (focal 27 §15.8)

`Log::opener` gives a `LogOpener`: the log's inbox and parameters, `Clone + Send + Sync`, without
the log's two threads, so an owner thread spawned for the node's life claims groups from it as
from the log (`group`, `groups`, `write_waiting`, `entry_room`, `frame_room`, `config`). `Log`'s
own calls and the opener's share one implementation of each. Once the log has closed every call
answers `Closed`. hyper-durable's `GroupStore::claim` and `remove` take either through
`LogGroups`.

### Tests

- `an_opener_claims_as_the_log_does_from_any_thread`: a handle claimed through an opener on another
  thread writes and reads its group; a second opener, and the log, are refused `Claimed` while it
  holds the group, and claim it once it is dropped.
- `an_opener_after_its_log_closed_answers_closed`.

The equivalence files are unchanged.

## For the durable shell (hyper-durable D-1)

- **A write sent behind a refused one is refused** (`LogError::Behind`). A group's handle keeps an
  epoch it moves each time it takes a refusal, and every write it sends carries it; the owner
  records the epoch of a handle's refused write and refuses the group's writes of that epoch or an
  earlier one, until one of a later epoch is laid. A group's writes are laid one a frame, in the
  order sent, so the record is cleared only once every write sent before it was refused. Before,
  a write of the hard state alone sent behind entries refused for the group's bound was written,
  and the group stated a commit past what it held (`tests/log.rs`,
  `a_write_sent_behind_a_refused_one_is_refused_too`, fails without the owner's check). The
  equivalence transcripts are unchanged: no write of theirs follows a refusal of its group.
- `GroupLog::depth`, the log's `PIPELINE_FRAMES`: the writes a replica keeps out at once; and
  `GroupLog::has_room`, the handle's own bound, before which a write is never refused `Busy`.

## The file grows as its owner admits

mantle's log grows its file to its quota (`docs/design/raft-log.md` §5, "up to its quota"), which a
device of its own makes the whole story. An owner whose log shares a volume with other writers
cannot state that quota up front: a volume that filled before it failed the write that grew the
file, and the log fenced. `Growth` is the owner's admission, asked before a frame opens a slot past
the file's end, two slots ahead at most; a refusal is the quota reached (`Full`, compaction,
sweeps), never a fence. Admissions are whole segments, committed once their slot is durable,
released when the growing write fails or the log ends unused, and the owner is told what the
file's slots take at open. Without a `Growth` the log is mantle's.

### Tests

- `a_refused_slot_is_the_bound_reached_never_a_fence`: a gate admitting four slots answers the
  fifth `Full`, never `Fenced`, counted, the file within what was admitted.
- `a_compaction_frees_slots_and_writes_resume_as_under_the_bound`: after a compaction, exactly as
  many writes resume as under `max_segments` alone at the slots admitted, with no admission more.
- `a_failed_growth_gives_its_admission_back_and_fences`: a volume that ends where the next slot
  begins fails the write, the admission goes back, and the log fences.
- `a_reopened_log_tells_its_owner_what_its_slots_take`: whole segments, at least the file's
  length and less than a segment past it.
- `a_new_log_refused_its_first_slot_writes_nothing`.
- `a_gate_that_admits_everything_writes_as_no_gate`: the same frames and file across a reopen;
  without the admission ahead of the sweep decision, the reopened log sweeps segments it has room
  beside and the file differs.
