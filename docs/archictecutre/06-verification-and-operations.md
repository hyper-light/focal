# Verification, capacity qualification, and operations

Status: future executable acceptance contract. Existing Sylk test names are evidence in [01](01-source-audit.md); none of the Focal suites below have run or exist yet. Work package ownership is in [05](05-implementation-plan.md).

## 1. Verification layers

Use the same deterministic reducer in production, serial reference execution, and simulation. Keep the oracle for graph satisfaction intentionally simple and independent of optimized monitors. A test that calls the implementation twice and compares it to itself cannot establish correctness.

1. Domain conformance exhausts status/command/authority combinations and expected whole-object transitions.
2. Storage property tests compare arenas/indexes/snapshots to simple immutable maps.
3. Deterministic simulation varies message delivery, disk completion, time inputs, scheduling, crashes and topology.
4. Multi-process tests exercise real filesystem synchronization, consensus integration, encryption and restart behavior.
5. Deployment qualification runs the same public workflow across the six complexity steps and records operator effort.
6. Capacity qualification measures named workloads on named hardware; it cannot substitute for safety testing.

A failing history includes seed, toolchain, git revision, configuration digest, topology, fault schedule, committed records, returned receipts, and the smallest reproduced counterexample. Secrets and artifact contents need not appear in diagnostic bundles.

## 2. Required correctness suites

| Test ID | Suite / stimulus | Required assertion | Owner |
|---|---|---|---|
| T01 | Every ClaimStatus × command × authority/receipt condition | Correct transition/Inform/Yield/Refuse; exhaustive terminal predicates | P01–P02 |
| T02 | Canonical byte fixtures, sets reordered, lifecycle changed | Identity stable exactly for immutable authored content; distinct schema/hash domains | P01 |
| T03 | Retry with same/different RequestId, content, occurrence and epoch | One original durable outcome; conflict/expired semantics; intentional repeated work stays distinct | P02, P12 |
| T04 | Pipeline close/progress/cancel/verdict against pending state | Preparation and committed serial application agree | P02 |
| T05 | Generate without post, then post with/without valid admission | No early work dispatch; one receipt after legal activation | P02, P06 |
| T06 | Replay all prefixes with executors disabled | Same normalized state and delta bytes; zero validator/handler calls | P02, P04 |
| T07 | Arena free/reuse, wrong owner, index rebuild, generation limits | No stale/cross-owner handle access; stable IDs remain correct | P03 |
| T08 | Pin reader, modify/retire/move data, resume pagination | One fixed prefix until explicit expiry; no mixed-prefix page | P03, P11 |
| T09 | Crash around append/flush/commit/apply/reply | All successful receipts survive; unknown outcomes recover idempotently | P04, P08 |
| T10 | Short write, torn tail, full disk, fsync failure, corrupted committed frame | Typed failure; no acknowledged corrupt/gapped history served | P04 |
| T11 | Snapshot while writes commit, crash at manifest publication/trim | Complete checkpoint prefix plus every later committed record retained | P04, P12 |
| T12 | Upload lost/corrupt/staged/unavailable in promised region | Closing testament never acknowledges missing durable evidence | P05, P13 |
| T13 | Deterministic pass/fail/error, agentic stage, observe/required, fallback | Correct two-phase semantics; failure does not trigger error fallback; every result preserved | P05 |
| T14 | Stale receipt/evaluator/handler token after reassignment | Stale result cannot alter new run or repaint terminal claim | P05, P06 |
| T15 | Random mixed dependency graphs and cycles | Optimized monitor equals least-fixpoint oracle at every committed prefix | P06 |
| T16 | Register/settle race, lost wake, restart parked scope | Durable release discoverable; no stranded continuation | P06, P07 |
| T17 | Deadline/cancel/supersession with reordered duplicate inputs | Same canonical victim/outcome; stale timer is inert | P06 |
| T18 | Seed at S, commit during seed, reconnect after lost ack | Snapshot + ordered suffix complete; duplicates handled, gaps explicit | P07 |
| T19 | Slow consumer and below-retention cursor | Bounded buffers; Resync/archival recovery; source never blocks on consumer | P07, P12 |
| T20 | Leader partition, restart, configuration change, stale route | Only quorum/fenced authority succeeds; histories linearizable | P08 |
| T21 | One voter versus multi-voter same input history | Same client/domain semantics; durability scope correctly explained | P08, P16 |
| T22 | Many sessions, skewed tenants, metadata failure | Independent ordering, fair progress, bounded idle footprint, no global hotspot | P09 |
| T23 | Serial versus workers/ranges; missing footprint, read-modify-write | Same prefix and deltas; deterministic fallback reexecutes affected work correctly | P10, P11 |
| T24 | Crash every split/move/merge cutover step | One authority, no missing state, current-route retry, fixed snapshot semantics | P11 |
| T25 | Cross-range atomic batch, partition pauses during apply | No partial transaction visible; publication reflects complete prefix | P11 |
| T26 | Crash archive custody/identity-index/GC stages | Retired proof retrievable; no premature blob/WAL deletion | P12 |
| T27 | AZ/region loss, correlated object-store failure, minority return | Advertised failures tolerated; no unsafe promotion; proof bytes remain durable | P13 |
| T28 | Auth spoofing, revoked invite, tenant crossing, malicious frames | Isolation and bounded parsing; no foreign existence/data leak | P07–P13 |
| T29 | Rolling upgrades and stale readers on new schema | Capability fence precedes new writes; old proof replays unchanged | P15 |
| T30 | Sustained overload and archive/consumer lag | Memory/disk/queues bounded; reserved completion progress; explicit refusal/pressure | P03–P14 |
| T31 | Arbitrary external effect executed before crash/result commit | Reconciliation/indeterminate outcome; no false exactly-once promise | P06 |
| T32 | Six deployment transitions and unchanged proof fixture | DC01–DC20; minimal added user decisions, preserved semantics | P16 |

T04, T06, T09, T15, T20, T23 and T25 are permanent release gates. Reduced deterministic histories run on each change; larger campaigns run nightly and before release. Select campaign counts from coverage/runtime budgets, publish them, and retain every known counterexample permanently.

## 3. Crash and fault matrix

Inject failures before and after each durable or visible transition, including both sides of an asynchronous completion. For each fault assert durable state, client result, publication, retry behavior, memory reclamation, and emitted diagnostics.

| Boundary | Faults to inject | Recovery source |
|---|---|---|
| Command admission | Duplicate, stale revision, changed policy snapshot, capacity exhausted | Committed projection + ordered pending state |
| WAL | Partial header/body, fsync error, power-loss model, record/segment checksum mismatch | Verified checkpoint and contiguous committed tail |
| Consensus | Delayed/reordered votes/appends, minority partitions, config transition interrupted | Persisted hard state, log and membership |
| Apply | Worker crash, missing footprint, cancelled batch, partial range completion | Common committed log + last published/checkpoint prefix |
| Evidence | Missing chunk, wrong digest, acknowledged storage then node/AZ/region loss | Required verified content replicas and custody manifest |
| Snapshot | Partial object upload, stale root, missing range, manifest rename crash | Previous complete manifest + longer tail |
| Range movement | Source/destination/directory death at each intent/cutover/cleanup step | Session-log intent and fenced activation |
| Delivery | Duplicate frame, lost ack, slow reader, cache owner movement | Durable cursor, retained deltas or typed reseed |
| Runtime | Execution performed, result not committed, stale claimant returns | Durable assignment plus external reconciliation evidence |
| Retirement | Archive written not committed, retirement committed not evicted, GC crash | Content-addressed custody and committed retirement record |
| Geography | Whole region isolated; root metadata unavailable; old region returns | Surviving quorum + durable content under active contract |
| Operator | Wrong store, wrong cluster invite, unsafe placement, incompatible schema | Refuse before alteration; explain corrective action |

Crash cuts on the native mutation path are injected into the real binary
through `crates/focal-node/src/fault.rs`: built with the `focal-node`
`test-support` feature (which the crate's own tests enable through a
dev-dependency on itself), `FOCAL_FAULT=<site>:<n>` aborts the process at
the `n`-th arrival at `before-propose` (frame received and admitted, nothing
proposed) or `after-commit-before-reply` (owner committed and published, no
reply written), or at one of the seven movement sites of the placement
controller (`movement-begin`, `movement-seed`, `movement-barrier`,
`movement-ready`, `movement-seal`, `movement-activate`,
`movement-cleanup`: each after the step's evidence is gathered and before
its proposal, [25 §9](25-parallel-materialization-and-ranges.md)). Release
binaries are built without the feature and contain no hook. The A4 gate
(`cli_native_a4.rs`, `mcp_native_a4.rs`) uses the first two cuts: the
first leaves a journaled frame that commits exactly once on retry; the
second leaves a durable commit the restarted node re-commits in its new
term and the exact retry finds by identity without a second effect. The
movement cuts are used by `placement_binary.rs`, where the founder dies at
each site in turn and the transfer completes from the committed map after
its restart.

A simulation of `fsync` is not proof that a storage device honors it. Real qualification documents OS, filesystem, mount options, device/cache behavior and failure assumptions. Laptop guarantees require intact storage that honors acknowledged flushes; independent disk loss requires another durable copy.

## 4. Capacity model and derived budgets

Users choose resource ceilings and desired guarantees. Most allocator/queue/shard parameters are internal derived values exposed through diagnostics. Some inputs are operational policy choices (headroom, fairness weights, target latency), not quantities uniquely determined by hardware; label them honestly and ship measured defaults.

Let:

- `M_limit` be usable process/container memory after honoring host limits.
- `M_fixed` cover runtime, networking, code and minimum control metadata.
- `M_recovery` reserve checkpoint transfer/rebuild and learner staging.
- `M_control` reserve completions, timers, cursor control and retirement progress.
- `M_work = M_limit - M_fixed - M_recovery - M_control` be the allocatable working budget.

If `M_work <= 0`, startup cannot satisfy the declared resource profile. Explain the minimum and stop before admitting work. Derive sub-budgets for live objects, indexes, overlays, snapshot versions, monitors, uploads and subscriptions so their sum cannot exceed `M_work`. A shared reservation authority prevents several modules independently consuming the same headroom.

| Derived limit | Calculation / required measurement | Failure response |
|---|---|---|
| Maximum epoch entries | Minimum of overlay-byte budget / measured worst-case transition expansion and scheduling efficiency target | Smaller epoch; serial work if useful parallelism disappears |
| Pending proposals | Pending byte budget / bounded encoded plus prepared size | Retryable admission pressure before accepting more payload |
| Watch buffer | Per-consumer byte share bounded by session/tenant aggregate; item count also bounded | Typed Resync; reserve control frame capacity |
| Snapshot pin duration | MVCC byte headroom / measured version creation rate, bounded by query service policy | Expire pin; require restart at new snapshot |
| Upload concurrency | Evidence staging share / admitted transfer reservation | Queue with deadline or reject before receiving excess bytes |
| Monitor admission | Aggregate closure allocation budget minus current pinned/duplicate closures | Typed Capacity/Retryable; Yield requires a separately budgeted durable wait registration, never an unregistered monitor |
| Checkpoint cadence | Replayable bytes limited by `(RTO - fixed recovery cost) × replay bytes/sec`; convert via log growth rate | Checkpoint sooner, add resources, or declare RTO unsatisfied |
| Replica placement | Voters/copies satisfying explicitly enumerated promised failure sets | UnsatisfiedPlacement; do not silently weaken contract |
| Split threshold | Minimum resource bottleneck across bytes, CPU, read demand, apply lag and replication bandwidth | Split/move shardable work; throttle irreducible hot work |
| Disk retention | Recovery/custody/cursor floors plus physical-segment pinned amplification | Compact safely, add space, or backpressure before reserve exhausted |

Under a fixed live working set and bounded external lag, RAM usage must plateau as historical completed work grows. If the live graph itself grows without bound, report its growth and stop admission at budget rather than pretending retirement solves it.

## 5. Workload qualification matrix

| Axis | Required cases | Report |
|---|---|---|
| Deployment | Laptop, VM fleet, Kubernetes, multi-AZ, multi-region, delegated global fleet | Exact topology, versions and active guarantees |
| Session count | Few active large sessions, many small active sessions, very many idle sessions | Per-session memory/task/connection overhead and activation latency |
| Graph | Disjoint, chains, fan-in, fan-out, mixed cycles, deep closures | Apply/monitor CPU, duplicated closure memory, serial fraction |
| Evidence | Small inline metadata, large chunked evidence, many tiny chunks, duplicate content | Throughput, metadata cost, custody latency, dedup scope |
| Validators | Fast pure, long-running, failing, timed out, agentic adapters | Queue delay, deadline outcomes, stale-result rejection |
| Reads | Point, range scan, deep traverse, long paginated snapshot | p50/p95/p99 latency, pinned bytes, expiry rate |
| Subscribers | Many narrow, few broad, fanout skew, slow/offline | Bytes/event amplification, cursor lag, resync, CPU |
| Conflicts | From disjoint operations through one hot lifecycle | Scalability curve and actual serial ceiling |
| Failures | Disk, node, zone, region, directory, archive, credentials | Recovery time, unavailable interval, acknowledged prefix retained |
| Churn | Join/leave/move/upgrade plus sustained writes | Control overhead, tail latency, temporary replica/copy amplification |

Report both committed and published throughput; pending queues must not make accepted throughput look like completed throughput. Include durable evidence bytes in end-to-end latency and disk/network accounting. Do not exclude failing/slow requests from latency/error reports.

The capacity envelope names maximum tested active sessions, proof rate, live bytes, evidence volume, watcher count, conflict shape and fault load. A large session's sequencer is benchmarked separately from aggregate fleet capacity. Global deployment must demonstrate bounded root metadata work as tenant/session count rises.

## 6. Minimal operational signals

Expose bounded, content-free metrics grouped by node, region, deployment or capped workload class. High-cardinality claim/session/participant identifiers belong in sampled diagnostic traces, not unbounded metric labels.

| Signal | Meaning / response |
|---|---|
| Commit versus publish lag | Durable work is waiting on apply/ranges; inspect hot partition and worker budgets |
| WAL flush latency, error, reserve | Storage cannot sustain or guarantee acknowledgement; stop unsafe admission |
| Quorum/leader/configuration condition | Current write authority and reasons unavailable |
| Memory by owner and reserved bytes | Real live/temporary occupancy and upcoming exhaustion |
| Replica and artifact placement conditions | Actual versus requested failure survivability |
| Range ownership epoch / transfer debt | Stuck movement or stale clients; no placement authority guessing |
| Validator queue/attempt/deadline | Handler health and stuck evidence production |
| Monitor count/closure bytes/oldest wait | Potential stranded work or graph pressure |
| Consumer cursor lag/resync | Consumer recovery and retention risk |
| Checkpoint/custody/retirement debt | Replay-time and reclamation risk |
| Archived proof verification failures | Corruption or incomplete custody; prevent GC |
| Request unknown outcomes and retry conflicts | Network/recovery problems; preserve request IDs |

Use simple conditions with stable reason codes and readable next steps. `Running` is not sufficient: writable means identity, durability, ownership, schema and publication prerequisites are met. Liveness probes must not repeatedly kill a healthy node merely waiting for quorum.

## 7. Required recovery runbooks

Each runbook contains symptoms, read-only diagnostics, preconditions, exact commands, preserved guarantee, stop conditions, verification and escalation. Test it on a disposable deployment.

1. **Lost leader:** verify quorum, wait/elect normally, retry original RequestId, verify read token and proof. No manual force promotion.
2. **Lost quorum:** keep unavailable; restore members or verified disks. If irrecoverable, use explicit disaster restore after fencing old authority, with a reported recovery point and new generation.
3. **Disk corruption:** stop serving affected state; verify checkpoint and committed tail; rebuild from verified sources; preserve evidence for diagnosis. Never skip corrupt committed entries.
4. **Full disk:** protect control/commit reserves, stop new bulk work, inspect pinned retention, restore archive/cursor progress or add capacity; trim only safe prefixes.
5. **Stalled archive:** preserve hot proof and log; backpressure new work; recover custody; verify hashes before resuming retirement.
6. **Region loss:** check whether active placement promised that failure; use surviving quorum only; verify artifact availability; block return of stale ownership on rejoin.
7. **Restore:** select complete manifest and exact prefix, verify all roots, exclude old authority, restore schema/membership/identity state, replay once, audit before accepting writes.
8. **Slow consumer:** inspect cursor retention; replay or projection reseed; reconcile external effect consumers explicitly when history is missing.
9. **Upgrade failure:** stop capability activation, keep old-compatible service where possible, use declared downgrade boundary; never start an old decoder over unsupported persisted schema.
10. **Deployment step failed:** leave requested contract inactive until ready; explain missing prerequisite; roll back only membership/placement changes that preserve the active guarantee.

## 8. Release evidence

A release includes correctness-suite results, SIM seeds, real crash-test environment, capacity reports, six-stage complexity report, supported durability/residency guarantees, upgrade matrix, and verified restore runbooks. Known limits are expressed as workload/resource boundaries and typed outcomes.

The release objective is measured progress toward global scale with stable semantics and stepped operator effort. Both false capacity promises and unnecessary configuration concepts are release defects.
