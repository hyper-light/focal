# focal-runtime

A bounded worker service for committed validation obligations and claim/monitor
timers. `Runtime::drive` runs on a local or replicated session owner; a fixed worker pool does
evidence IO and pinned validator execution on separate threads. The core reducer
never invokes a handler or reads a clock.

The service uses `State::runs` and durable monitor definitions as its source of
truth. It periodically scans bounded batches, so a lost dispatch hint does not
strand the final run. It acknowledges generated testaments, starts whole-work
validation when increment requirements are finished, records results and requests
whole-work completion. It executes only runs assigned to its configured evaluator
principal. Claim subject work/receipt assignment remains a separate actor service.

The embedding constructor is:

```rust,ignore
let runtime = Runtime::new(
    RuntimeConfig::default(),
    RuntimeIdentity { ledger, principal: evaluator, epoch, root, policy_revision },
    Arc::new(catalog),
    Arc::new(evidence_reader),
    Arc::new(clock),
)?;
```

`Catalog::register` binds the immutable `Registration`, `ExecutionPolicy`, and
validator implementation. It rejects conflicting registrations and zero limits.
The requirement's policy revision must match the pinned execution policy and fit
the runtime's authorized revision. `RegistryExecutor` invokes the real
`focal-evidence` registry. `SharedStoreReader` reads authenticated sealed content
in bounded chunks through `Arc<RwLock<ContentStore>>`; `InlineOnly` explicitly
refuses external content. `Runtime::with_executor` injects another executor,
including an agentic provider or an external-effect reconciliation adapter.

`drive(&mut Session)` never polls the session or consumes replication messages.
The host pumps Raft `poll`/`step` and invokes `drive` after commit progress or a timer
wake. `drive_local` remains a convenience wrapper for the one-voter owner.
`DriveReport::pending` exposes the exact request key awaiting commitment, while
`pending_input()` borrows its frozen logged input for inspection. Pending commit is
normal progress, not an execution failure. Each call attempts at most one waiting
proposal, so capacity pressure returns to the host without an internal retry loop.
`RuntimeError::is_retryable()` classifies resource pressure, lost authority and
stale domain transitions for the host's next wakeup. The host continues draining
Raft messages in those cases; corruption and persistence failures remain fatal.

The runtime owns at most one pending protocol input, bounded by
`max_pending_bytes` and separately reserved completion memory. Its command, schema,
timestamp, deadline, evidence, authority snapshot and key never change on retry.
Leadership loss cancels active execution, retains unresolved input and forbids
re-dispatch until the session crosses its new current-term readiness barrier.
After failover, a committed artifact from the winning owner takes precedence over
a different uncommitted local proposal for that same logical artifact. The local
proposal is reported stale; its different input is never reported committed.
Worker completion and cancellation still run while a protocol stage waits.

Each validation attempt follows this sequence:

1. Resolve the exact committed manifest and check the run, handler version,
   attempt, evaluator, quality phase and receipt-adoption fence.
2. Reserve task/evidence/result capacity, then durably register a versioned
   `runtime-dispatch` artifact containing the frozen assignment and deadline.
3. Enqueue the worker only after that assignment is committed and the session
   has completed its current-term readiness barrier.
4. Recheck the assignment and owner term when the worker returns. A cancellation,
   receipt adoption, changed attempt, terminal control or leader-term change
   prevents an old result from being submitted.
5. Persist a typed diagnostic artifact, then submit the deterministic verdict
   referencing the original evidence and that diagnostic. The appended
   `RecordFencedValidationVerdict` also records the exact optional receipt fence,
   so an already-pending adoption cannot admit a stale result behind it. `None`
   matches only a claim that still has no receipt. These are ordinary
   logged domain inputs with stable request keys. They perform no external work
   during replay.

A crash after the dispatch record leaves a reconstructible obligation. A crash
after the diagnostic but before the verdict reuses that exact diagnostic without
calling the handler again. The assignment's original time/deadline and policy
remain authoritative after restart. Typed outcomes distinguish missing evidence,
negative evidence, execution failure, timeout, policy mismatch and indeterminate
external effects. Missing required schemas can produce Incomplete/Error only
with a durable diagnostic; Pass/Fail still require the declared proof schemas.
Fallback happens only after Error. A required agentic quality phase follows a
programmatic Pass and cannot be reached by skipping failed programmatic work.

`RetryContract::ReadOnly` permits repeating a handler after a crash. For
`RetryContract::Reconcile`, the executor checks the external system before every
execution, including the first local dispatch and receipt adoption. The outcomes
are NotStarted, Completed or Indeterminate. Indeterminate records a durable marker
and parks the obligation without submitting a verdict or triggering fallback.
`active_assignments` exposes these IDs; an explicit `retry_reconciliation(id)`
wakes a parked attempt after external state changes. A proven completed effect
can be accepted after the original execution deadline without executing it again;
the reconciliation call itself has a bounded observation deadline.

Effectful adapters must use `Task::effect_key()` in an atomic external idempotency
or fencing operation. That key binds the logical run/attempt and remains stable
across receipt adoption and owner restart. A non-atomic "not found" query is not
sufficient to prevent two owners from causing the same external effect. This
crate does not claim exactly-once effects for providers without that contract.

All runtime clock and authored deadline values use milliseconds. Timer commands
record the exact authored deadline as `fired_at`; repeated delivery is inert after
terminality or generation changes. The owner reconciles claim and monitor timer
lanes independently. A parked cycle uses the core's deterministic SCC victim
selection. Existing stream/history machinery supplies durable monitor release
notifications; the worker queue is not an authoritative notification source.

Configuration bounds worker count, in-flight tasks, per-handler concurrency,
aggregate evidence bytes, artifact references, result reason bytes, reconciliation
scan batches, and owner command work. Evidence capacity is reserved before copying
payloads or committing a dispatch. Fixed channels bound queued and completed
work. Completion memory is reserved separately, and owner command budgeting
leaves progress for timers. Overload preserves pending proof and returns typed
admission pressure. Finished-at timestamps prevent a late worker result from
beating a timer merely because the owner has not polled recently.

The owner stores ordinary owned assignment metadata. Each worker owns its receiver;
there is no shared receiver mutex or poisoning path. Owned tasks and their memory
charges move from the owner to a worker and then through the completion channel.
The owner retains a separate metadata/result allowance. Dropping the owner releases
that allowance immediately while a running callback retains its payload charge
until it exits. Closed/full channels and corrupt state return typed errors.

`Arc` is limited to actual sharing across threads: the injected executor, clock,
immutable catalog, cancellation flag, content-store reader lock, and the memory
budget's atomic accounting. Task payloads and allocations do not use `Arc`.
Production code is checked against panic macros, unwrap/expect, unchecked indexing,
and arithmetic that can overflow. Canonical identity errors propagate as typed
runtime errors instead of panicking or using a partial hash.
This policy covers code maintained in these crates. Standard-library collections,
serialization, and upstream dependencies still allocate; process-wide allocator
failure may abort and is not made recoverable by accounting reservations. Callback
containment applies to Rust unwinding only, not `panic=abort`, allocator aborts, or
process termination. Injected clocks and evidence adapters must honor their
nonpanicking, bounded contract.

Synchronous Rust callbacks cannot be forcibly preempted safely. Cancellation is
cooperative; a timed-out callback retains its worker slot and memory allowance
until it exits. The service never spawns unbounded replacements for stuck workers.
Dropping the runtime signals cancellation but does not wait indefinitely for a
non-cooperative callback. Trusted implementations must bound their own temporary
allocations and IO timeouts. Hard CPU/RSS isolation and forcibly terminating an
untrusted or wedged handler require a separate process/container executor and are
not implemented by this thread pool.

The adapter supports one-voter and real quorum-driven sessions. Three-voter tests
cover pending dispatch, isolated owners, late results, changing receipt generations,
checkpoint/restart, and exact frozen retries. It currently serializes runtime
protocol proposals per session; this favors a small, explicit recovery state machine.
`focal-node::fleet::ReplicaHost::spawn_with_runtime` owns the quorum runtime drive;
its threaded-host tests cover failover, recovery and nonfatal runtime admission
pressure. Cross-session weighted scheduling, shared executor pools, indexed
monitor/SCC dispatch, and scope result accumulation remain integration work.
Whole-work manifests use point lookup; old increment
manifests have a bounded exact-match fallback and pause if no exact retained
manifest is available. The retained serial core still performs some whole-partition
checks; this is not a sublinear global-scale scheduling claim. Runtime artifacts
and exact request outcomes also require the planned archive/retirement machinery
for long-running operation.

Validation:

```sh
bash scripts/cargo.sh test -p focal-runtime -p focal-core --offline
bash scripts/cargo.sh clippy -p focal-runtime -p focal-core -p focal-ledger --all-targets --offline -- -D warnings
```

The runtime tests execute the real report validator, read real durable chunks,
check missing/negative evidence and fallback, recover frozen diagnostics, bound
concurrency/evidence, race late results against deadlines, fence receipt adoption
and leader terms, reconcile indeterminate effects, and replay claim/monitor timers.
Quorum tests also verify no execution before assignment commit, no recovered
execution before the leader barrier, unchanged pending input and memory across
retries, owner loss at dispatch/result boundaries, and committed diagnostic reuse.
