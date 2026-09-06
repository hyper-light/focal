# Implementation plan

Status: implementation in progress; see the [implementation evidence and remaining work](09-implementation-status.md). Complete work in dependency order. Unchecked items remain open until their complete acceptance criteria are verified, even when part of the code exists. Every package must update its associated architecture contract when its implementation changes a proposed detail.

The target is Rust, custom RAM-primary state, disk durability, one laptop through global distribution, and [stepped configuration complexity](08-stepped-complexity-and-deployment.md). Hecate is the semantic reference; Sylk supplies behavior and regression fixtures, not Go code to transliterate.

## 1. Delivery rules and dependencies

Each package produces a runnable vertical slice or a conformance artifact consumed by the next package. Completion requires code, interface documentation, meaningful tests, failure behavior, resource accounting, and a reproducible demonstration. An implementation-only package that leaves its acknowledgement semantics undefined is incomplete.

| Package | Depends on | Demonstrable outcome |
|---|---|---|
| P00 | This architecture | Reproducible Rust workspace, schema/config contracts, CI skeleton |
| P01 | P00 | Canonical typed proof objects and wire identity fixtures |
| P02 | P01 | Serial deterministic ledger with exhaustive lifecycle behavior |
| P03 | P01, P02 | Custom memory graph with stable snapshots and enforced budgets |
| P04 | P02, P03 | Durable laptop ledger; kill/restart recovers acknowledged work |
| P05 | P04 | Durable streamed evidence and real validator/testament workflow |
| P06 | P04, P05 | Dependency graph, cyclic consults, parking, execution fencing |
| P07 | P04, P05; integrate P06 before release | One client interface over embedded and authenticated network adapters; resumable deltas |
| P08 | P04, P07 | Multi-node Raft, quorum durability, membership and secure join |
| P09 | P08 | Partitioned session placement and fair multi-tenant node operation |
| P10 | P03, P04, P06 | Deterministic within-node parallel apply equivalent to serial |
| P11 | P08, P09, P10 | One session's RAM graph and apply distributed over ranges |
| P12 | P05, P07, P11 | Archive custody, safe eviction, bounded long-running operation |
| P13 | P08, P09, P11, P12 | Multi-AZ and multi-region durability/placement contracts |
| P14 | P06–P13 | Recorded correctness, fault, resource and capacity qualification |
| P15 | P07–P14 | Operational release: upgrades, restore, packaging and runbooks |
| P16 | P00–P15 | Six-stage deployment progression with measured minimal user decisions |

P03/P04 include enough snapshots and local custody to support crash tests; P12 extends them into distributed archival retirement. P08 should reuse a session-log interface introduced at P04, preserving the local public semantics. P10 can progress while P08/P09 are built. P16's interface requirements constrain P00 onward; it is not a final layer of UI polish.

## 2. P00 — Workspace, contract registry, and delivery harness

**Files:** `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `.github/workflows/ci.yml`, initial `crates/focal-model/`, `crates/focal-sim/`, `tests/contracts/`, `examples/`, `config/schema/`.

- [ ] P00.1 Pin the compiler, edition, targets, release profile and dependency features. Use the inspected 1.94.1 as the initial candidate; verify Linux server and laptop builds. Record actual MSRV and license/advisory checks.
- [ ] P00.2 Create only the initial crates. Establish dependency direction and public visibility rules from [03](03-rust-workspace-and-interfaces.md). Add CI format/lint/build targets and deterministic test seed recording.
- [ ] P00.3 Create a versioned registry for domain enums, lifecycle-action-to-delta mappings, artifact schema IDs, command schema versions, and error codes. One command may emit several deltas; the bijection is between lifecycle action and delta action. Reserve removed numeric IDs permanently.
- [ ] P00.4 Establish SIM interfaces for clock, ID source, disk operations, network delivery, crash/restart and executor scheduling. Production pure modules consume the same explicit inputs.
- [ ] P00.5 Freeze the command/result and deployment configuration shapes. Draft `focal start`, `cluster invite`, `join`, and deployment explain/plan behavior without exposing Raft or shard configuration to ordinary users.
- [ ] P00.6 Add the first same-workflow fixture: generate a claim, activate it, acknowledge receipt, upload evidence, close testament, validate, read result. Subsequent packages progressively replace stubbed infrastructure; a stubbed response never counts as a passed durability gate.
- [ ] P00.7 Apply the [ownership and failure policy](10-ownership-and-failure-policy.md) to every production crate. Gate production and test configurations separately; reject panic-prone arithmetic, indexing, assertions, and unwrap paths. Review dependency preconditions and initialization failures. Replace each avoidable `Arc` with ownership or borrowing and record concrete concurrency reasons for retained sharing. Keep these gates active through P16.

**Acceptance:** clean checkout builds on declared targets; fixed seed yields the same fixture IDs; unknown schema IDs fail decoding; documentation links and registry mappings validate. `cargo test --workspace` exists once packages contain executable tests. No runtime or benchmark performance is claimed yet.

**Risk closure:** record dependency choices before implementing wrappers. A custom runtime or transport is not hidden scope in this plan.

## 3. P01 — Typed domain and canonical identity

**Files:** `crates/focal-model/src/{ids,claim,testament,artifact,validation,relation,command,delta,error}.rs`, `tests/contracts/canonical_identity.rs`, `fixtures/wire/`.

- [ ] P01.1 Implement the object/status/relationship types in [02](02-domain-and-lifecycle.md), with separate authored and lifecycle structures. Constructors validate bounded strings, vectors, IDs, scope keys and required parentage.
- [ ] P01.2 Define canonical encoding: explicit schema/version prefix, field order, length encodings, enum numeric values, sorting rules for sets, null/absent distinction, text normalization policy, and permitted numeric values. Do not normalize arbitrary artifact bytes.
- [ ] P01.3 Define authored-content hash scope, object identity, claim context, artifact byte digest, and testament attachment digest separately. Exclude timestamps stamped by the runtime, status, trace refs and memoization presence from authored hashes.
- [ ] P01.4 Distinguish stable participant IDs from instance/attempt IDs. Add TenantId/LedgerId, RequestEpoch and RequestId as distinct newtypes. Derive service identity from registered namespace/version rules; log any randomly generated public IDs once. Validate session binding for every reference.
- [ ] P01.5 Implement typed retryable/terminal evidence disposition, structural refusal, ordinary Inform/Yield, read errors and transport unknown outcome. Preserve display text separately from machine-readable reasons.
- [ ] P01.6 Check the action/delta registry bijection and append-only enum evolution. Write fixture files once from reviewed expected bytes, not by regenerating expected values during the test.

**Acceptance:** golden canonical bytes/hashes are stable; reordered set inputs canonicalize identically; changed authored content changes identity; lifecycle changes do not; malformed/oversized/unknown variants reject before large allocations. Compile/visibility tests show callers cannot write lifecycle fields as content.

**Demonstration:** serialize a complete four-family proof example; decode and compare all fields and hashes after a process restart.

## 4. P02 — Serial state machine and effective-state admission

**Files:** `crates/focal-core/src/{prepare,reduce,lifecycle,affordance,validation,satisfaction}.rs`, `tests/contracts/lifecycle_matrix.rs`, `tests/contracts/pending_admission.rs`.

- [ ] P02.1 Implement one transition table and derive terminal/active predicates from it. Enumerate every status × command pair with valid transition, Inform, Yield, or structural refusal. Include durable failure boundaries, explicit cancellation/expiry, supersession and deadlock results.
- [ ] P02.2 Implement generated versus posted activation, runtime receipt, repeated observational progress, testament generation/acknowledgement, required validation aggregation, and atomic related-object updates.
- [ ] P02.3 Implement immutable command preparation with explicit actor/cause context, expected revisions, policy/registry versions, timestamps and IDs. The reducer reads only explicit inputs and committed state.
- [ ] P02.4 Maintain effective admission state as committed projection plus ordered pending transitions. Test terminalization racing with progress, two closes, two posts, contradictory validation results and concurrent content-identical creation.
- [ ] P02.5 Handle request IDs and content identity independently. Same admitted `(principal, LedgerId, RequestEpoch, RequestId)`/same digest returns the original result; same key/different digest refuses. Implement durable epoch negotiation and minimum-admitted-generation advancement against effective state. Reject an unseen old key as RequestHistoryExpired after floor advancement, never as fresh work. Same authored content/new request resolves existing object identity without a second activation.
- [ ] P02.6 Emit immutable deltas from the transition batch, including sequence/ordinal allocation rules, and effect intents. Do not execute effect intents in the reducer.
- [ ] P02.7 Introduce normalized state export for serial oracle comparison; strip derived memoization state only, never authoritative fields.

**Acceptance:** lifecycle matrix and pending-admission property suites pass; any prefix replays to the same normalized state and delta bytes; late errors cannot rewrite terminal success; every generated claim remains undispatched until posting. Test old unseen RequestIds, concurrent epoch advancement, future/unallocated epochs and exact archived receipt returns. Whole transaction appears or none of it does.

**Demonstration:** in-process claim workflow with conflicting retry and terminal progress examples; no disk-durability claim until P04.

## 5. P03 — Custom memory graph, indexes, and bounded views

**Files:** `crates/focal-memory/src/{arena,range,index,snapshot,budget,reclaim}.rs`, `tests/storage/arena_model.rs`, `tests/storage/snapshot_isolation.rs`.

- [ ] P03.1 Implement safe generational arenas for all four families and edges. Stable IDs resolve through typed indexes; freed slot generation prevents ABA reuse. Handle overflow is checked.
- [ ] P03.2 Separate immutable content buffers from lifecycle version records. Group small allocations into accounted pages; track exact live bytes and conservative allocator/index overhead.
- [ ] P03.3 Implement per-kind, per-claim, identity, relation forward/reverse, lifecycle/deadline and required-validation indexes. Every index update is part of a transition batch; rebuild from canonical state and compare.
- [ ] P03.4 Publish immutable versioned read views. Readers pin a prefix with a bounded lifetime; writers never mutate a pinned view. Point reads and graph traversals agree at that prefix.
- [ ] P03.5 Add memory reservation before mutation preparation, including pending overlays and anticipated index expansion. A reservation rolls back on failed admission or log rejection. Reserve a completion/retirement/control lane.
- [ ] P03.6 Implement chunked list/traverse with bounded depth, edge visits, result bytes and snapshot lifetime. Return a continuation; never materialize an unbounded graph response.
- [ ] P03.7 Budget monitor references, dedup, timers, read pins and recovery buffers even before their owners are complete. Expose accounting to the node's shared budget manager.

**Acceptance:** arena/property tests match a simple reference map; stale handles never resolve new objects; snapshot roots survive concurrent writes; index rebuild is identical; sustained allocation pressure returns typed capacity outcomes with no unaccounted growth. If any unsafe optimization is introduced, its own Miri/schedule tests are mandatory.

**Demonstration:** pause a paginated reader, mutate the same graph, resume at its original prefix, then release all versions when the pin expires.

## 6. P04 — Laptop durability, WAL, and recovery

**Files:** `crates/focal-log/src/{record,segment,writer,recovery,retention}.rs`, initial `crates/focal-consensus/src/{session_log,raft,read_barrier}.rs`, `crates/focal-ledger/src/{session,sequencer}.rs`, local checkpoint module, `tests/durability/`.

- [ ] P04.1 Define framed segment headers and records with format version, logical log identity, index/term metadata, length bounds and checksum. Persist segment creation/rotation using correct file and directory synchronization for supported OSes.
- [ ] P04.2 Implement a node-owned WAL writer multiplexing logical logs and group flushes. Keep ordering per logical log; return durable acknowledgements only after required data and hard-state persistence.
- [ ] P04.3 Introduce `SessionLog` with proposal, committed-entry stream, read barrier, snapshot floor, and membership hooks. Host the real Raft adapter with one voter from this package, including correct Ready/persistence ordering, term/vote state, SessionSeq/RaftIndex mapping and snapshot metadata. P08 extends this same adapter to network replication and membership; no local-only log format or later conversion is introduced.
- [ ] P04.4 Apply only committed entries; return normal success only after publication. On disk failure stop affected admission and preserve unknown outcomes; retry the same RequestId after recovery.
- [ ] P04.5 Stream recovery with bounded frame buffers. Distinguish an incomplete uncommitted tail from corruption in a known committed prefix. Quarantine corruption; never silently skip a committed record.
- [ ] P04.6 Seal checkpoint data at a pinned prefix, flush its files, publish a checksummed manifest atomically, then record the recoverable floor. Rotate WAL; reclaim only whole eligible segments or copy retained logical records through a verified compaction protocol.
- [ ] P04.7 Persist request-epoch allocations/floors, dedup receipts, active lifecycle/deadline state, registry/policy inputs and indexes necessary for exact replay, or reconstruct them from the retained committed log. Crash immediately after generation closure must not readmit old unseen requests.
- [ ] P04.8 Provide `focal start` with durable local data-directory defaults, explain the single-disk guarantee, and detect identity/config incompatibility on restart. No configuration is required for the first local demo.

**Acceptance:** kill between append/flush/commit/apply/reply and at every snapshot/rename/trim boundary. Every acknowledged mutation survives; unacknowledged ones may appear and deduplicate correctly. Replay executes zero external handlers. Snapshot plus tail equals uninterrupted execution. Test full disk, short writes, torn tail, checksum failure and interrupted segment rotation.

**Demonstration:** run the claim fixture, kill the process after acknowledgment, restart from the same directory, retrieve the same proof and original request receipt.

## 7. P05 — Artifact custody, testaments, and validators

**Files:** `crates/focal-evidence/src/{chunks,manifest,registry,dispatch,verdict}.rs`, `crates/focal-core/src/validation.rs`, `tests/evidence/`, `examples/validated_claim.rs`.

- [ ] P05.1 Implement resumable bounded artifact upload with length/digest/schema validation, staging quotas and cancellation. Seal a manifest only after bytes meet the session's durability placement contract.
- [ ] P05.2 Implement generated/unattached artifacts and immutable attachment manifests. Close testament only when all referenced evidence is available and authorized. A failed upload cannot create a dangling successful close.
- [ ] P05.3 Implement testament confidence, success/failure/partial result shapes, and immutable terminal correction through supersession/amendment. Duplicate attachments are normalized before constructing a close; conflicting duplicate names/IDs reject.
- [ ] P05.4 Implement versioned validator registration and boot validation: schema compatibility, required/observe status, fallback priority, timeout and concurrency budget, deterministic versus agentic phase.
- [ ] P05.5 Implement receipt success in the core at acknowledged testament arrival. Execute programmatic validation outside the core; only after its pass dispatch any required quality-bar evaluator.
- [ ] P05.6 Persist dispatch identity and fence result acceptance by validation generation, evidence digest, registered version and attempt token. Record typed errors/evidence and fallback transitions as new committed inputs.
- [ ] P05.7 Supply real example validators: schema/contract validation plus an injected agentic evaluator adapter. Differentiate missing evidence, negative evidence and execution error. New observed validators never silently become required.
- [ ] P05.8 Account handler queue, evidence bytes, upload staging, result retention and deadlines. Under overload produce a typed result or admission pressure without dropping pending proof.

**Acceptance:** evidence-before-reference crash suite; exact result replay without re-execution; deterministic-pass/agentic-fail; deterministic-error/fallback; observe failure does not gate; stale evaluator cannot close a newer testament; artifact hash mismatch is never served as valid evidence.

**Demonstration:** stream test output, close the testament, observe receipt, run required validation, query final proof after restart; repeat with missing evidence and validator timeout.

## 8. P06 — Graph satisfaction, parked work, and execution lifecycle

**Files:** `crates/focal-ledger/src/{monitor,execution,deadline}.rs`, `crates/focal-core/src/satisfaction.rs`, `tests/graph/`, `tests/runtime/`.

- [ ] P06.1 Implement the brute-force least-fixpoint oracle first. Encode `awaits` terminality and `depends_on` satisfaction/failure separately. Include mixed cycles, self-edge checks, supersession, terminal failures and shared descendants.
- [ ] P06.2 Build per-parked-scope blocking closures and SCC condensation; maintain separate graph-monitor and serving-cache subscriber indexes. Dispatch only to affected monitors.
- [ ] P06.3 Register monitor and consumed-prefix atomically, evaluate current truth before parking, then consume later deltas. Completion between check and subscription cannot be lost.
- [ ] P06.4 Persist or reconstruct monitor definitions, release tokens and execution expectations. Bound aggregate duplicated closures; admission must fail or yield explicitly before exhausting the monitor budget.
- [ ] P06.5 Commit timer/deadline inputs from the current fenced authority. Elect deterministic deadlock victims by creation sequence and stable ID tie-breaker; late duplicate timer delivery is inert.
- [ ] P06.6 Persist execution assignment IDs and fencing tokens; claimants acknowledge receipt through the runtime. Lost assignment response must not create two active logical attempts.
- [ ] P06.7 Implement scoped result accumulation, dedup attachments, flush at successful/failure scope close, suppress on Yield, and commit acknowledgement before releasing scope ownership.
- [ ] P06.8 Classify external effects by retry/reconciliation contract. Provide a fixture that records an indeterminate effect after crash and requires reconciliation rather than blind re-execution.

**Acceptance:** random graph monitor versus oracle; cycle/deadline victim determinism; register/release race; pause/restart parked claimant; stale execution token refused; terminal replay does not launch work. Saturate overlapping closures and verify bounded memory with useful progress in the reserved completion lane.

**Demonstration:** A awaits B while B awaits A; deadline resolves the declared victim and both parked continuations settle according to edge semantics, with no polling loop.

## 9. P07 — Protocol, client interface, and durable subscriptions

**Files:** `crates/focal-wire/`, `crates/focal-client/`, `crates/focal-stream/`, node transport adapter, `tests/protocol/`, `tests/streams/`.

- [ ] P07.1 Implement bounded framing/version negotiation and registered message/error enums. Authenticate node and client peers before authorizing any operation. Add session/tenant isolation tests and reject spoofed cause/actor fields.
- [ ] P07.2 Implement embedded and QUIC client adapters for submit/read/subscribe. Preserve Inform/Yield/Refuse and unknown outcome exactly across adapters.
- [ ] P07.3 Implement read tokens, linearizable barrier translation, fixed-prefix graph paging and explicit stale projection reads. Return route changes and snapshot expiry as typed results.
- [ ] P07.4 Implement deltas with `(session, seq, ordinal)`, immutable historical payloads and per-consumer durable cursor. No outbox table or live-state rehydration of historical events.
- [ ] P07.5 Implement snapshot seed at prefix S, delta handover strictly after S, cursor replay, leader disconnect, and reset below retention. Projection re-seed replaces/merges by stable identity; it cannot replay irreversible external effects as if they were new work.
- [ ] P07.6 Add byte and item credits, bounded consumer buffers, priority for proof/control traffic, and explicit slow-consumer Resync. Final control status must remain deliverable even if a data buffer is full.
- [ ] P07.7 Implement client route cache with bounded entries, retry budget, request-ID preservation, backoff and redacted errors. Cancellation ends waiting, not already committed work.

**Acceptance:** embedded/network differential suite; handshake/auth/oversized frame fuzz; seed/stream races; reconnect after lost consumer ack; no delta before publication; older cursor produces typed Resync rather than an invented exact replay guarantee.

**Demonstration:** two local processes exchange a claim, disconnect the receiver, then resume proof delivery and read-your-watch at the same committed sequence.

## 10. P08 — Replicated session log and secure membership

**Files:** `crates/focal-consensus/src/{raft,membership,read_barrier}.rs`, node join/identity adapter, `tests/consensus/`, multi-process harness.

- [ ] P08.1 Extend the P04 Raft adapter to authenticated multi-node message transport with explicit Ready/persistence/send/apply ordering. Persist term/vote/log and configuration changes as required before corresponding acknowledgements. Use no second state-engine WAL.
- [ ] P08.2 Extend and verify the P04 session sequence to Raft index mapping under elections and configuration changes, including control/no-op entries. Recovery and ReadIndex never compare different clocks directly.
- [ ] P08.3 Exercise one-voter and three-voter configurations through the same session-log contract. Multi-voter mutation success waits durable quorum plus publication, not local append or remote memory acknowledgements.
- [ ] P08.4 Implement authenticated one-use expiring join invitations, learner catch-up, verified snapshot install, and safe voter promotion/reconfiguration. Persist cluster identity and prevent accidental joining to a different cluster.
- [ ] P08.5 Implement leader loss, stale leader fencing, pending-overlay rebuild, and idempotent unknown-outcome retry. External execution workers carry a separate durable assignment fence.
- [ ] P08.6 Implement read barriers without lease-clock assumptions. Followers may serve only a requested verified prefix under the documented read contract.
- [ ] P08.7 Bound proposal queues, replication inflight bytes, slow learner retention, election work and snapshot transfers. Removing failed replicas requires committed membership change and recovery policy.

**Acceptance:** partition/election/reconfiguration model suite; kill every Ready boundary; old leader cannot acknowledge new work without quorum; one-voter crash durability matches three-voter client semantics. Loss of quorum returns unavailable and never forks authority.

**Demonstration:** join three VMs/processes using invitations, run the unchanged proof fixture, kill leader, retry by RequestId, recover the same result.

## 11. P09 — Session directory, tenancy, and fleet resource control

**Files:** `crates/focal-node/src/{directory,placement,tenant,admission,scheduler}.rs`, `tests/fleet/`, configuration explain adapter.

- [ ] P09.1 Implement regional directory partitions, root regional delegation, session placement records and bounded route caches. Session creation allocates a durable ID and independent ordered log.
- [ ] P09.2 Fence directory updates with versions and coordinate authority changes through the affected session log. Existing valid routes continue serving when metadata optimization is unavailable.
- [ ] P09.3 Implement tenant/session/node budgets and weighted fair scheduling across append, apply, transfer and query queues. Idle sessions must not allocate full worker pools or individual physical WAL writers.
- [ ] P09.4 Multiplex logical logs, connections and group flushes. Account physical-segment retention when one slow session pins part of a shared segment; isolate or compact without deleting other sessions' tails.
- [ ] P09.5 Make placement decisions from measured load and declared resource/failure constraints. Expose explanation and unsatisfied placement conditions; do not expose shard maps as normal config.
- [ ] P09.6 Add stable cluster/session discovery, cleanup of retired session machinery, and authenticated tenant/session request scopes. Root metadata must not observe every claim mutation.

**Acceptance:** scale the number of idle sessions and active tenants independently; bounded per-session idle footprint; noisy tenant cannot starve completion/health of another; directory loss stalls changes rather than corrupting session writes; cross-session edge injection fails.

**Demonstration:** grow a VM deployment while existing clients continue; inspect automatic session distribution and redacted explanation of effective guarantees.

## 12. P10 — Deterministic parallel apply

**Files:** `crates/focal-ledger/src/materializer/{epoch,footprint,dag,worker,publish}.rs`, `tests/materializer/`, `benches/apply.rs`.

- [ ] P10.1 Batch committed entries into versioned epochs. Epoch-sizing changes are explicit deterministic control inputs or implementation choices proven not to affect externally observable output.
- [ ] P10.2 Derive complete read/write footprints over objects, indexes, adjacency, aggregate counters, deadlines, identities and monitor state. Track actual reads/writes; no unchecked pointer escape bypasses tracking.
- [ ] P10.3 Build RAW/WAR/WAW predecessor edges in log order, deduplicating edges and excluding self-edges for read-modify-write on the same key. Bound graph allocation by admitted epoch bytes.
- [ ] P10.4 Execute ready tasks over an immutable base plus per-entry multiversion output. Preserve the values earlier-index reads require. A maximum-writer-index cell alone is insufficient.
- [ ] P10.5 At a deterministic validation barrier audit complete actual conflicts. On incomplete footprints discard the entire affected speculative epoch/suffix and recompute it serially, including downstream results. No timing-dependent partial overlay is published.
- [ ] P10.6 Publish only a contiguous successful prefix with immutable deltas in sequence/ordinal order. Reclaim speculative memory even on cancellation/crash. Retain the serial execution path as correctness reference, not legacy behavior.
- [ ] P10.7 Measure zero-conflict, dense-conflict, long-chain and high-fan-in workloads. Automatically use serial execution when overhead exceeds useful parallel work without changing semantics.

**Acceptance:** state and delta bytes match serial at all worker counts and schedule seeds; omitted read and write footprints are caught; read-modify-write has no self-deadlock; dependent stale computations are not retained; failure at publication exposes no half epoch.

**Demonstration:** compare one worker and a CPU-derived worker pool on the same log, report speed and memory without implying conflict-independent linear scaling.

## 13. P11 — Partitioned state/apply and safe range movement

**Files:** `crates/focal-memory/src/partition.rs`, `crates/focal-ledger/src/{partition_apply,publication}.rs`, node range transfer/directory hooks, `tests/sharding/`.

- [ ] P11.1 Implement a single ordered range map for graph storage and apply. Affinity groups and secondary-index buckets have explicit keys; claim-owned state remains colocated where practical.
- [ ] P11.2 Make every active range consume/skip the common session prefix, tracking applied completeness even when an entry does not touch it. Select bounded verified replica readers for each range.
- [ ] P11.3 Implement deterministic cross-range read exchange and write batches keyed by session sequence, epoch and transaction identity. The log commits the decision once; retrying materialization cannot create an independent transaction decision.
- [ ] P11.4 Publish a stable prefix only after all required range results are complete. A reader pins the route map and consistent prefix; new leaders reconstruct the publication lower bound before serving.
- [ ] P11.5 Implement logged split/move intent, checkpoint seed, tail catch-up, stop barrier, fenced activation, directory update and delayed cleanup. Directory CAS alone must never authorize both source and destination.
- [ ] P11.6 Implement idempotent resume at every transfer step, snapshot and cursor token translation, range merge, balancing hysteresis and fair transfer budgets. User sessions retain their IDs and claim history.
- [ ] P11.7 Detect hotspots by measured bytes/CPU/fan-in/read demand. Split shardable indexes or move ranges; report an irreducible serial hot lifecycle honestly.

**Acceptance:** kill at every move step; no overlap/gap in write authority; no mixed-prefix graph read; missing partition stalls only its declared publication domain with typed availability; one versus many ranges recovers identical state; cursor resumes without silent gaps.

**Demonstration:** while creating/validating claims, move half the graph to a new node and fail the source immediately before/after activation; observe correct retry and unchanged proof.

## 14. P12 — Archival custody and sustained bounded operation

**Files:** `crates/focal-archive/src/{checkpoint,custody,catalog,restore,gc}.rs`, retention coordinators, `tests/archive/`, long-running load harness.

- [ ] P12.1 Define checkpoint manifests binding every range, prefix, schema, route epoch, membership/config, identity/cursor state and referenced content roots. Verify availability and hashes before accepting a recovery floor.
- [ ] P12.2 Retire terminal-and-released objects only when live graph/monitor/request obligations permit. Write content-addressed archive records and searchable identity/relationship indexes, verify custody, commit retirement, then drop hot slots.
- [ ] P12.3 Preserve released tokens, archived identity lookup and typed traversal continuations. Retain request receipts for admitted epochs; safely close generations before reclaiming receipt memory, archive exact outcomes, and revalidate cold identity lookups against pinned catalog generations plus pending admission. A repeated old claim remains a duplicate after eviction and restart. An expired request with no archived receipt never executes again automatically.
- [ ] P12.4 Derive WAL retention from the minimum safe floor across recovery, checkpoints, publication, subscriptions, dedup and custody obligations. Distinguish client reseedable projections from proof/effect consumers that cannot skip history.
- [ ] P12.5 Implement GC roots for live objects, uploads, checkpoints, snapshots, unfinished retirement, grants and restore jobs. Use quarantine/grace plus committed root-generation checks before deleting bytes.
- [ ] P12.6 Pace retirement/compaction under sustained mutation load with reserved work credits. Full archive or stalled custody stops unsafe eviction and surfaces capacity pressure.
- [ ] P12.7 Provide cold proof lookup, whole-session restore, source checksum verification, and prefix audit. Restore does not invoke historic validators or revive terminal assignments.

**Acceptance:** crash every custody phase; archive never misses retired proof; live-reference pin prevents eviction; history lookup remains correct after compaction; memory reaches a bounded steady state at constant live work; lagged archive cannot trigger silent deletion.

**Demonstration:** process and retire more history than fits in RAM, restart, then traverse an old claim into archival continuation and retrieve its exact evidence.

## 15. P13 — Zones, regions, and global placement

**Files:** node failure-domain planner, regional directory delegation, remote evidence/checkpoint adapters, `tests/regions/`, deployment topology fixtures.

- [ ] P13.1 Model node/zone/region facts independently of deployment packaging. Validate trusted topology sources and detect duplicate or missing failure-domain labels.
- [ ] P13.2 Compile user-facing durability intent into quorum and artifact placement constraints. Enumerate promised failure sets and reject unsafe layouts before claiming the guarantee active.
- [ ] P13.3 Implement learner/snapshot placement before promoting geographic voters. A durability-contract change has planned, preparing and active states; acknowledgment uses the active contract until the transition commits.
- [ ] P13.4 Couple artifact and checkpoint custody to the promised region survivability; quorum log copies alone are insufficient when all referenced bytes are in the failed region.
- [ ] P13.5 Implement regional home selection, residency constraints, delegated metadata groups and bounded region-aware routing. Global control must stay outside the steady-state per-claim commit path.
- [ ] P13.6 Handle regional network partitions, loss of metadata services, credentials and object stores independently. A minority never auto-promotes by timeout. Rejoin follows log/custody reconciliation and epoch fencing.
- [ ] P13.7 Support explicit disaster restore to a verified prefix when synchronous quorum cannot recover. Report actual recovery point and require a fenced new lineage/session incarnation when split authority cannot otherwise be excluded.
- [ ] P13.8 Preserve local and regional read choices as explicit consistency contracts; never improve latency by silently switching required durable writes to asynchronous replication.

**Acceptance:** AZ and region failure matrices; quorum survivability enumeration; data residency never violated by automatic placement/backup; remote evidence lost despite log quorum blocks unsafe close; stale regional directory cannot grant ownership; disconnected old region cannot resurrect authority.

**Demonstration:** unchanged proof fixture in multi-AZ then synchronous three-region placement; kill each promised domain and report failover, write latency, availability and preserved acknowledged prefix.

## 16. P14 — Correctness and capacity qualification

**Files:** `crates/focal-sim/`, `tests/history/`, `benches/`, `tools/load/`, `docs/qualification/`, CI/nightly qualification pipelines.

- [ ] P14.1 Run all [06](06-verification-and-operations.md) model, crash, schedule, corruption, auth and upgrade suites; persist failing seeds and minimized histories as regression fixtures.
- [ ] P14.2 Check linearizable mutation/read histories per session, including unknown outcomes and reconfiguration. Compare graph/delta state at common prefixes across replicas and recovered runs.
- [ ] P14.3 Define workload generator distributions: session count, tenant skew, graph size/depth/cycles, artifact size, validation time, conflict density, watcher fanout, retention and churn. Publish exact generation seeds.
- [ ] P14.4 Measure laptop resource floor, fleet idle-session footprint, one-session sequencer and apply ceilings, cross-range amplification, network fanout, disk/retention pressure and cross-region RTT costs.
- [ ] P14.5 Fit capacity from measured CPU/bytes/disk/network/conflict costs, with stated headroom and workload limits. Extrapolation is separate from direct measurements.
- [ ] P14.6 Inject overload in each queue and budget independently. Verify fair recovery, bounded memory, completion reserve, no proof loss and no retry amplification storm.
- [ ] P14.7 Run long-duration mixed load with repeated leadership changes, upgrades, archive stalls and range moves. Define pass thresholds from declared deployment SLOs and record the exact test envelope.

**Acceptance:** no violated correctness invariant, unexplained memory growth, unbounded queue, or missing acknowledged proof. Qualification reports identify bottlenecks and supported envelopes. Meta-scale language is permitted only with clear tested/extrapolated boundaries and a plan to validate the next envelope.

## 17. P15 — Operations, packaging, upgrades, and release

**Files:** node observability/config lifecycle modules, `deploy/`, `docs/runbooks/`, `examples/`, release workflows.

- [ ] P15.1 Expose content-free metrics/spans and conditions for quorum, lag, custody, queues, memory, disk, routing, stale workers and placement. Avoid tenant/object IDs as unbounded metric labels.
- [ ] P15.2 Implement explain/plan/apply configuration transitions with redacted effective output and stable machine-readable conditions. Defaults derive mechanics, never the user's required durability/residency intent.
- [ ] P15.3 Package native laptop binary, VM/system-service guidance, container, plain Kubernetes manifests and a Helm chart as an optional installation method. Use persistent volumes and stable node identities; Kubernetes replicas/readiness are not quorum or data-durability proofs.
- [ ] P15.4 Implement safe boot ordering, admission readiness, graceful drain, bounded shutdown and pending-result semantics. Membership/placement recover before data traffic is accepted.
- [ ] P15.5 Implement rolling upgrade, committed capability activation, unknown-format rejection, migration/rebuild and rollback boundaries. Retain historical decoders necessary to replay supported logs/checkpoints.
- [ ] P15.6 Write and execute runbooks for leader loss, quorum loss, region loss, corrupt disk, restore, full disk, stalled archive, expired credentials, slow consumer, unsafe placement and failed upgrade.
- [ ] P15.7 Ship the unchanged claim example, client docs, supported-platform matrix, capacity guidance, backup verification and reproducible release artifacts.

**Acceptance:** clean-machine installation; rolling upgrade with ongoing work; loss/recovery of a persistent volume; backup restore to audited prefix; no traffic before readiness guarantee holds; all required operational conditions have a recommended next action.

## 18. P16 — Stepped complexity qualification

**Files:** `tests/deployment/`, `deploy/examples/`, `docs/qualification/deployment-complexity.md`; interface contract [08](08-stepped-complexity-and-deployment.md).

- [ ] P16.1 Implement the six deployment journeys exactly as documented. Count new required concepts, fields, commands, infrastructure dependencies and irreversible decisions at each transition.
- [ ] P16.2 Verify local use needs no cluster, shard, quorum or certificate expertise; network joining adds only identity/reachability/trust decisions; packaging adds no ledger semantics.
- [ ] P16.3 Verify zone/region/global steps introduce only the irreducible failure-tolerance, placement/residency and delegated administration choices required by that step.
- [ ] P16.4 Run the same claim/testament/validator workflow before and after every transition. Existing sessions, proofs, cursors and client request identities remain valid.
- [ ] P16.5 Run plan/explain on invalid or underprovisioned layouts. Output must identify the unmet guarantee and minimal corrective action without suggesting unsafe forced promotion.
- [ ] P16.6 Conduct independent operator walkthroughs; record every undeclared prerequisite or hidden concept as a defect. Remove configuration knobs that exist only to expose internals and retain explainable escape hatches for expert capacity work.
- [ ] P16.7 Publish migration, failure and rollback evidence per step. Reverse deployment changes only when the new topology still satisfies the active durability/residency contract, or after an explicit contract change.

**Acceptance:** every DC test in [08](08-stepped-complexity-and-deployment.md) passes; documentation matches actual CLI output; no engine mode fork, manual data conversion, silent weakened guarantee, or requirement to understand internal partition mechanics appears in the normal journey.

## 19. Parallel work ownership

After P01, one workstream can implement pure lifecycle/oracle work while another implements arenas and disk framing against reviewed contracts. After P04, evidence/validator integration and protocol/consensus integration can proceed independently. P10 has its own serial-equivalence gate and must not delay a correct serial distributed prototype.

Shared schema, receipt, sequence and visibility changes require joint review because they affect every owner. Allocate one owner per crate family; use small integration fixtures across seams rather than duplicating model definitions in each workstream. Deployability, resource accounting and fault tests are part of each package, not deferred to an unspecified hardening phase.

## 20. Completion record for each package

Record: implementation revision; exact contracts implemented; commands executed; tests and seeds; demonstrated workload; known limitations; resource budget derivations; compatibility/upgrade impact; and links to updated architecture sections. Mark a package complete only when its acceptance conditions are met. If a design is revised, update [07](07-decisions-and-traceability.md) and downstream packages before implementing the new semantics.
