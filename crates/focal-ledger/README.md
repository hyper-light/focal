# focal-ledger

`Session` owns the durable Raft application boundary for one ledger. Domain
proposals reserve owned pending row versions, bounded epoch workspace, COW
graph/index pages, immutable results and retained delta copies before consensus
admission. Candidates do not copy the whole core. Publication moves prepared
roots after commitment. A lost reply can be retried with the original
request key; an uncommitted proposal is never reported as success.

Cursor metadata uses the same Raft group and durable log. `submit_cursor` admits
an authenticated projection command, and `submit_cursor_control` additionally
admits trusted retention maintenance and protected service consumers. Both use
`CursorInput`; ingress must compute `intent_hash` from the original versioned
client operation and bind the authenticated ledger/principal. Do not trust a
payload-provided hash or derive control authority from the payload. Server time,
lease expiry and registry revision are logged transition inputs, excluded from
that stable original-intent hash. Exact retries return the original receipt even
if those derived inputs or the current consumer state have subsequently changed.

Domain and cursor requests share the complete `(principal, epoch, request ID)`
namespace. Reusing a key across command families conflicts. Cursor requests use
the core's admitted epoch window and minimum epoch; retained exact outcomes
remain retrievable after that minimum advances. Consumer IDs are owned by the
principal that registered or first seeded them. Ingress still verifies scope,
filter authority and the delivered acknowledgment frontier. The ledger verifies
owner, namespace, generation, position existence and monotonicity; it cannot
prove that a remote effect actually finished.

The owner calls `maintain_cursor_clock_local(now)` when `next_cursor_expiry()`
is due. This trusted internal control uses the same Raft log, requires no client
request epoch, and allocates no request receipt. Idle ticks never write. A fleet
host uses `propose_cursor_clock(now)` and pumps quorum until the returned clock
and revision are committed. Protected consumers are excluded from this timer;
no local wall-clock observation alone releases a durable pin.

`SessionSeq` advances only for a committed domain mutation. Cursor commands
advance a separate cursor revision at an explicitly logged domain prefix.
`CursorReceipt` includes that revision, domain sequence, Raft index, original
record and original replay floor. Current cursor commands log the leader's
replay floor so replicas with different cache sizes return the same outcome.
A domain sequence alone does not certify current cursor metadata; authoritative
cursor reads require the leader's completed ReadIndex/application barrier.

`AcknowledgeAndRenew` advances a projection acknowledgment and lease atomically.
An invalid acknowledgment cannot extend its lease. Protected consumers use
plain `Acknowledge`: they never expire, silently resync, or skip an obligation by
reseeding. Only trusted service code can register a protected consumer. Their
retention floor remains pinned until their durable acknowledgment advances.

The immutable delta source exposes `Position::after_delta` and
`Position::resolved`. An ordinal inside transaction S pins all of S. The source
binary-searches its starting cursor and bounds visited rows, encoded bytes and
sequence work. It returns Resolved only after every delta through that prefix
was visited. Filtering belongs to the subscription layer. No current-state
rehydration is used to synthesize historical deltas.

The retained RAM tail is finite. Admission forecasts whole-transaction eviction
across published and pending work. If fitting the next mutation would cross any
active consumer pin, the proposal fails with `RetentionPinned` before consensus
admission. Cursor progress emits no domain deltas and uses the completion memory
lane. Cursor receipts have a dedicated bounded allowance, independent of domain
receipt slots; new registration/seed work leaves its final eighth (at least one
slot) for existing-consumer progress. Once the entire cursor outcome allowance
is exhausted, new cursor outcomes also pause safely; exact retries still work.

Checkpoints atomically contain core state, cursor registry/owners/outcomes,
replay floor and the complete retained tail. Outstanding history is preserved,
and insufficient checkpoint/recovery capacity fails instead of discarding it.
The decoder validates namespaces, prefixes, receipt overlap, owners, delta order
and active pins. Legacy core-only checkpoints restore an explicit history floor
at their domain prefix, requiring a new projection seed for earlier history.

Persisted formats currently read:

- `FOCALOP1`: versioned prepared domain mutation.
- `FOCALCU1`: initial cursor command envelope (legacy floor reconstruction).
- `FOCALCU2`: cursor command envelope with the authoritative replay floor.
- `FOCALCM1`: bounded internal lease-clock maintenance, without client receipts.
- `FOCALSS1`: legacy core-only checkpoint.
- `FOCALSS2`: core, cursor metadata and retained-tail checkpoint.

The same one-voter and multi-voter application paths implement these rules.
Cursor controls currently serialize with pending domain proposals; this avoids
retention decisions against speculative cursor state. Contiguous committed domain
records execute as bounded epochs through `Core::plan_epoch`, real scoped worker
waves and audited publication. Cursor and maintenance records form epoch boundaries.
Leader reservations must exactly match actual row versions, intents and results;
followers and replay build the same graph patches from audited epoch outputs.
Missing access declarations or exhausted tracking trigger serial fallback.
`last_epoch_report` exposes the most recently published execution report.

`SessionLimits::apply` bounds each epoch's commands, workers, dependency edges,
stack and byte allowance. The byte allowance is clamped to the session and
completion reserve; small allowances use one worker. New admission reserves a
fixed staging allowance equal to half the epoch allowance, independent of total
core size. Its recorder charges input copies, touched rows, outputs and graph
worklists before growth. Exhaustion returns retryable capacity without a proposal.
A bounded one-command apply audit also runs before proposal. Singleton waves run
inline; wider waves use scoped workers. A committed batch may shrink on capacity
pressure; a command that cannot fit stops application. Before publication, the
owner validates every output, graph-root ancestor, retained delta and core charge.
Receipts, deltas and effect intents retain input order. Existing log and snapshot
encodings are unchanged.

`try_poll` obtains consensus progress without waiting for disk. `None` retains
pending work and publishes nothing; `persistence_pending` exposes the outstanding
durability operation. `poll` is the blocking compatibility entry point. Both
use the same committed-epoch publication path, and authoritative readiness still
requires the current-term committed barrier.

The core retains one authoritative map-backed state alongside the paged graph.
Core map publication still allocates, admission and planning still hash or scan
effective state, and cursor metadata clones a bounded partition. Resource charges
use conservative serialized-size estimates rather than allocator measurements.
There is no measured throughput or global-scale qualification claim.

There is no durable archive tier, proof-consumer migration protocol, automatic
cursor garbage collection, or request-outcome reclamation yet. Finite capacity
therefore pauses admission. A replica configured below the required retained
history or checkpoint working set stops safely and needs sufficient capacity
before recovery. Full P11/P12 history reclamation, replica-aware archive retention
and multi-region throughput qualification remain necessary before claiming the
architecture's global-scale target.

Session-owned metadata uses owned values and has no direct `Arc` handles. Immutable
graph snapshots and memory counters retain the documented cross-thread ownership
provided by `focal-memory`. Persisted-prefix parsing, reserved-candidate lookup,
and retention accounting return typed errors. A committed-application error stops
the session before serving further authoritative reads; recovery replays the
durable source. Drop paths release owned reservations without an invariant panic.
Strict production lint rules cover panic macros, unwrap/expect, unchecked indexing,
and arithmetic. They do not guarantee recovery from allocator aborts or arbitrary
upstream panics; upstream consensus containment has its own documented boundary.

Validation:

```sh
bash scripts/cargo.sh test -p focal-ledger -p focal-stream --offline
bash scripts/cargo.sh clippy -p focal-ledger -p focal-stream --all-targets --offline -- -D warnings
```

Tests cover pre-consensus capacity rejection, allocation rollback, quorum-only
cursor authority, leadership loss, checkpointed outstanding tails, lost replies,
exact retries, idle/due maintenance and quorum-fenced expiry, cross-family request conflicts, admitted epoch/owner fencing,
partial versus resolved retention, mixed replica cache sizes, reserved ACK
capacity and the legacy checkpoint history boundary. Epoch tests compare exact
serial receipts and state bytes across one and three voters, forced audit fallback,
multiple real workers and restart. They also reject malformed later inputs and
foreign graph ancestry before publishing an earlier prefix, and verify admission
capacity rollback and pending-row reclamation. A populated-core regression fills
ordinary admission while an 8 MiB completion reserve commits an epoch update;
the reference-state charge already exceeds that entire completion reserve.
