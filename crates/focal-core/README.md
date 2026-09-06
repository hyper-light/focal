# Deterministic domain core

`Core::prepare` admits an authenticated command against one effective session prefix.
`Core::apply` publishes a versioned prepared command at the next `SessionSeq`, returning
its exact request receipt, ordered immutable deltas, and external effect intents.
The reducer has no clock, filesystem, network, random source, external-handler executor, or callback registry.
Authority snapshots, identifiers, timer events, evidence custody attestations, and
validator outcomes enter through logged inputs.

`Core::snapshot` exposes immutable current state. `PendingState` retains only owned
changed rows for ordered pending commands; `CoreView` reads those versions over the
committed base. Staging binds the exact effective prefix and state/limits digest.
Accepting a staged command requires reserved row slots and validates that basis.
Publishing a committed prefix removes its pending versions; losing a proposal drops
the uncommitted suffix. A pending duplicate is **not a durable acknowledgment**: the
composition layer waits for the original proposal's commitment/publication. The
older cloned pending-state oracle is compiled only for unit tests.

`Core::reconcile` observes one authenticated principal's committed epoch window
and, optionally, one exact domain request receipt. It does not read pending
versions or clone the epoch set. A retained receipt wins even below the floor;
an absent receipt below the floor means new admission is fenced, without claiming
that the request never committed. All other absence remains `Unknown`.
The returned borrowed view exposes a checked output allowance before its fallible
owned copy. Hosts obtain a fresh quorum barrier and retain that allowance through
response delivery; this local accessor cannot establish quorum authority.

`stage_managed_pending_bounded` takes a real `ManagedAuthenticatedInput`, whose
cluster/ledger/principal/slot/generation/ordinal identity is separate from legacy
request epochs. A borrowed reducer adapter shares all actor, authority, revision
and domain checks without manufacturing a legacy key. Managed execution rejects
legacy epoch controls and writes no legacy epoch or receipt rows. Its owned row
version participates in the same effective pending view. Staging, independent
replay/audit and publication bind the exact prefix, state/limits digest, input and
complete deterministic output. The result is not a receipt until the enclosing
ledger assigns its actual committed Raft index.

The session's committed stream registry must enforce stream registration,
generation, ordinal window, exact duplicate outcomes and retirement for both
domain and cursor requests. Core alone cannot prove those admission conditions.
Its managed publication API currently handles one command at a time; integrating
managed commands into bounded multi-command epochs remains a separate execution
step. Legacy key/input/receipt and Core checkpoint encodings are unchanged.

`stage_pending_bounded` uses a recorder with an explicit byte ceiling and no access
trace allocations. It accounts input copies, changed rows, graph scratch and
outputs before allocating them. Exhaustion returns `StagingError::Capacity`,
separate from domain refusal, and cannot install a partial pending version.
Effective-state checksums stream directly into the hasher without a state buffer.

`PreparedMutation` and checkpoints are schema version 1. A checkpoint binds its entire
payload with BLAKE3. `normalized_bytes` exports authoritative state for replay comparisons;
there are no lazy caches to strip in this implementation.

Implemented semantics include four object families, all twenty statuses, immutable
content identity, generated/post activation, request generation admission/floors,
receipt adoption fences, append/close evidence staging, receipt acknowledgments,
pinned validation runs, logged receipt-fenced verdict admission, error-only fallback, agentic quality evaluation, all-required
severity aggregation, late Observe results, immutable terminal supersession,
ownership-tree cancellation, logged timer/SCC victim selection, and durable monitors.
The graph oracle uses the least fixed point, including transitively established
terminality, so related completions and releases appear at one committed prefix.

Production model/core code has no shared ownership handles. Checked counters,
fallible canonical identities, and typed missing-endpoint errors replace panic
paths. Effective pending work uses an owned row-version vector. Truncated checkpoint input is
rejected through checked prefix parsing. Strict Clippy rules forbid production
panic macros, unwrap/expect, unchecked indexing, and potentially overflowing
arithmetic; test-only code has explicit exemptions. Standard-library collection
allocation and upstream codec internals are outside that lint guarantee; this
reference reducer does not claim recoverable process-wide allocator failure.

`RecordFencedValidationVerdict` is appended at canonical command tag 29 (Postcard
variant index 28). Earlier tags and payloads remain replayable, including legacy
`RecordValidationVerdict`. New execution ingress should use the fenced command;
legacy decoding exists for persisted-log compatibility. Its optional receipt must
exactly match current state, including `None`, and the existing run, attempt,
handler-version, evaluator and manifest checks still apply.

`prepare_tracked(input, max_entries)` and `apply_tracked(sequence, prepared,
max_entries)` execute the same serial implementation and return its result plus
an `AccessFootprint`, including failures. Admission, reducer tables and graph
algorithms use private recording adapters with no raw-map or `Deref` escape.
The normal entry points use those adapters with recording disabled; their
persisted footprint stays session-exclusive, and their public behavior and bytes
are unchanged. Preparation also executes receipt/prefix publication on its private
draft so the trace includes the eventual atomic commit's metadata writes.

The trace covers ten state tables, absent point reads, whole-table predicates,
counts, identities, request generations and receipts. Mutable row access records
a whole-row write before returning a reference. That covers nested lifecycle,
adjacency, deadline, validation-attempt and monitor fields. Actual scans record
whole-table predicates even when empty, so concurrent insertion cannot become an
untracked phantom. `covers` audits omitted reads/writes, and `conflicts` handles
RAW/WAR/WAW overlap, including scan/point overlap. Count changes are recorded on
insertion; replacement does not claim to change table cardinality.

The bound counts distinct read and write keys together. Exhaustion collapses to
an allocation-free session-exclusive representation; it never truncates a trace
or rejects an otherwise valid command. The recorder is stack owned and uses
fallible `RefCell` borrowing, with the same conservative fallback on reentrancy.
There are no shared ownership handles. Its collection allocations still need a
host memory reservation if tracing is enabled in admission.

`prepare` and `apply` now read an immutable base and retain only owned changed
rows. `apply_serial` retains the cloned-map oracle for differential verification.
The ten tables expose no underlying map or `Deref` escape. Overlay lookup prefers
its own row, then the newest earlier command version, then the base. Ordered
scans merge those sources without cloning whole tables. This implementation has
insertion and replacement; adding deletion requires versioned tombstones and
absence observations before that operation can participate in an epoch.

`Core::plan_epoch(Vec<PreparedMutation>, EpochLimits)` binds commands already
admitted against their ordered pending prefixes. The plan validates each schema,
ledger, command hash and exact assigned base/index. It executes a serial planning
pass over owned row versions and stores digests of the exact result, deltas,
effects, row updates, actual accesses and read-version observations. Count
observations also include their value. State and result hashing stream directly
into BLAKE3 without a full serialized byte buffer. Planning retains earlier row
versions only until that pass finishes; the returned plan keeps inputs,
declarations and reference digests.

`EpochPlan::execute(&Core)` checks the immutable base checksum and runs the reducer
on bounded ready waves. A singleton runs inline under the same unwind boundary
and output audit; wider waves use scoped threads. A one-command or one-worker
epoch does not reserve worker-stack allowance. Workers borrow the base and completed
versions; no `Arc` or detached lifetime is needed. Each worker sees only indices
strictly earlier than its assigned command. A bounded log-order dependency graph
retains read/write, write/read and write/write conflicts, including scans and
cardinality reads. Sequence is an assigned immutable command input and an atomic
publication obligation, not a mutable worker-shared scalar. Structural counts
are derived sums of row births: count/count write pairs commute, while every
count read still conflicts with structural changes. Ordinary serial admission
continues to enforce request-capacity and epoch policy before this apply stage.

Actual worker access sets, observed row versions and exact output digests are
checked against the serial plan. `declare` accepts bounded scheduler hints but
cannot weaken that audit. Any mismatch discards every speculative version and
reruns the entire epoch in log order. Exhausted tracking goes directly to serial
execution, without constructing a dependency graph. Thread creation errors and
worker unwinds return typed errors after all created workers are joined; they
publish nothing. Recoverable unwinds are contained, while allocator aborts and
stack-overflow aborts remain process failures.

`Core::publish_epoch(EpochOutput)` verifies the base state/limits checksum, output
seal, all contiguous sequences, ledger and receipt rows before any mutation.
It consumes the owned row sets under the exclusive core owner and then advances
one contiguous prefix. `EpochOutput::results` and `report` support inspection
before publication; external effects must wait for successful publication of
committed inputs. Epoch plans/outputs are local execution values and introduce
no new persisted command or checkpoint encoding.

`EpochLimits` bounds commands, edges, concurrent workers, worker stack size,
trace entries and a checked workspace allowance. Fixed container metadata,
retained input representation, access nodes, queues and stacks are accounted
before planning; row copies and delta/effect/receipt outputs reserve conservative
serialized-size allowances. Serial-plan rows are dropped before worker execution;
failed speculative output is dropped before fallback reuses its allowance.
This is not a process-wide allocator proof: caller-owned spare capacity, existing
base state and standard collection allocator behavior still require host
reservations. Reducer worklists, sets and state-derived clones are charged before
growth against the same per-entry allowance; charges conservatively accumulate
through the entry, including scratch that has already been dropped. The core executor accepts a byte allowance without depending on a host allocator.
`focal-ledger` reserves that allowance in `MemoryBudget`, plus admission scratch,
pending rows, graph pages, results and delta copies. Core BTreeMap publication
still allocates. Graph roots are prepared beforehand and published by root swap.

The default `focal-ledger::Session` path executes and publishes these audited
epochs for leaders, followers and replay. Cursor controls split domain epochs.
Admission uses `stage_pending_bounded` and a bounded one-command apply audit
before Raft proposal; committed execution validates the reserved rows and results against the
actual epoch outputs. Broad graph fixed-point scans serialize many domain commands.
Serial planning, effective-state hashing and charge computation still scan state,
and the admission audit adds work; no throughput or speedup claim is made. The
separate paged graph indexes are not this reducer's backing store, so a future
reducer storage adapter must record its own index/range accesses.

Run `cargo test -p focal-model -p focal-core --offline` and
`cargo clippy -p focal-model -p focal-core --offline --all-targets -- -D warnings`.
The current tests include 729 exhaustively enumerated three-node mixed dependency
graphs, pending close/receipt/cancel/epoch races, canonical golden vectors, receipt
adoption, late/contradictory verdicts, and exact prepared-log/checkpoint replay.
Every shared workflow helper compares tracked and ordinary preparation, results,
state and checkpoint bytes. Focused tests cover missing footprints, empty scans,
mutable nested fields, structural counts and exhausted trace budgets. Epoch tests
compare 1/2/4/8-worker execution and both ready-order directions with the cloned
serial oracle, including graph/lifecycle changes and receipt/delta/checkpoint
bytes. Tests also cover omitted declarations, full fallback, stale/tampered
publication, bounded row/edge/stack/workspace admission, worker unwind and partial
thread creation failure.

This is the retained **serial reference implementation**, not completed P01/P02/P06
qualification or a production scalability claim. Remaining obligations are explicit:

- Ordinary preparation/apply and ledger pending admission use owned row overlays.
  The ledger publishes through the audited epoch executor and prepares paged graph
  changes from exact row patches. Core map publication, affected-only planning and
  complete allocator accounting remain; the cloned serial oracle remains available
  for differential verification.
- The persisted legacy footprint stays session-exclusive. Epoch execution uses
  separately audited actual accesses. Per-monitor materialized closure/SCC indexes
  and affected-only dispatch remain unimplemented.
- Limits bound command fields, object/request counts, and major graph loops. Allocator
  overhead, every nested graph visit, pending drafts, and retained history still need
  integrated resource accounting, control reserves, and custody-backed retirement.
- Exact old request receipts are retained across epoch floor changes. Archival lookup,
  pinned catalog validation, and safe receipt reclamation belong to the archive seam.
- Runtime timer cancellation is deterministic. `focal-runtime` now persists validator
  dispatch/diagnostic artifacts, bounds worker attempts, fences late results and
  reconciles claim/monitor deadlines. Its quorum adapter retains frozen pending
  commands and waits for dispatch commitment/current-term authority. Process isolation,
  scope result accumulation and full execution cancellation acknowledgment remain.
  Timer control transitions do not yet require a newly allocated error artifact.
- External verdict records retain all attempts in the ordered log/state. Missing
  declared proof schemas are accepted only for Incomplete/Error with durable error
  evidence; Pass/Fail still require their full schema set. The runtime supplies a
  distinct diagnostic for every outcome, while the general core API does not require
  one when complete evidence is already present. Trusted ingress must enforce artifact
  schema/custody attestations. Required quality evaluation follows programmatic Pass;
  Error fallback cannot bypass that gate.
- Rank integration and registered cross-family relation profiles remain incomplete;
  `Invalidates` is refused until a rank policy is configured. Current claim graph
  relation endpoints are claims, and artifact input endpoints cover all four families.
- Terminal progress is exhaustively checked; the full status × command × authority
  conformance matrix, upgrade compatibility suite, and cold-recovery qualification
  remain broader than the current tests.

Do not treat green unit tests as evidence that these remaining obligations are met.
