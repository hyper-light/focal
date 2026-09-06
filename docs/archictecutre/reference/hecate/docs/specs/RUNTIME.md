# SPEC: hecate-rt — the sharded deterministic async runtime

Status: ACCEPTED 2026-08-17 (whole-spec verdict "amend and accept" under the
maximal audit — five amendments folded: Driver cancellation surface, the
no-panic law (user correction: "we do NOT panic. Period. Ever." — supersedes
the drafted catch-and-convert; LEDGER_CORE reconciled same commit),
task-lifecycle law, cluster-SIM clause, provider-egress boundary corrected to
ALPN h2-primary/1.1-fallback on the Bedrock-receipt pushback). Direction
ratified: async everywhere on our own runtime; deterministic by construction;
zero refcounting.
References: actix-web's shard model (pinned current-thread workers, `!Send` tasks, no
work stealing), TigerBeetle/FDB determinism discipline, hyperscale's
admission-before-task-creation, S2/Polar Signals leak reports as the anti-checklist.

## 1. Shape

- **N shards**, N **bundle-declared with its derivation** (a two-process guest
  derives per process: the primary N-from-cores, its Scribe clamped to 1 — census
  N+1, never 2N; `MONITORING.md` §1; amended 2026-08-22). One pinned OS
  thread per shard, each running a single-threaded executor: FIFO ready queue,
  hierarchical timer wheel, per-shard driver handle.
- **Tasks are `!Send`-capable and never migrate.** Spawn targets a shard; placement is
  explicit (caller names the shard or uses the placement policy); there is no work
  stealing and no load-balancing migration. State lives in plain owned types.
- **Cross-shard communication is move-only** over bounded SPSC/MPSC channels. No type
  containing `Arc`/`Rc` crosses (or exists — §4).
- **Every stateful component is one task** owning its state, fed by its mailbox:
  ledger core, claims-graph monitor, merge serializer, protocol endpoint, WAL stream
  writer, VMM controller. Effects leave as messages or as returned effect values.
- **The task-lifecycle law — no untracked tasks.** Every task has exactly one
  owner (a component task or the shard root); spawn returns a handle the owner
  retains; the ownership tree is acyclic. Teardown is ownership-tree-ordered:
  the owner cancels its children (via the §2 cancellation surface), awaits
  their terminal states, then dies — deterministic teardown order derives from
  the tree; shard shutdown = root teardown. PROTOCOL §5's ordered listener
  shutdown, AUTOSCALING's drain, and session teardown stand on this law.
  Per-component task counts are derived caps, admission-checked at spawn —
  admission-before-task-creation as spec law, not a references-line citation.

## 2. The driver seam

```rust
pub trait Driver {
    fn now(&self) -> Tick;                                  // monotonic; virtual in SIM
    fn rng(&mut self) -> u64;                               // seeded in SIM, OS entropy in REAL
    fn submit(&mut self, op: IoOp, waker: TaskRef) -> IoId; // completion-shaped API
    fn timer(&mut self, at: Tick, waker: TaskRef) -> TimerId;
    fn cancel(&mut self, id: IoId) -> CancelRequested;      // op STILL completes:
    fn cancel_timer(&mut self, id: TimerId) -> CancelRequested; // Cancelled | its result if it raced
}
```

- **Cancellation is a request with guaranteed completion** (io_uring's
  ASYNC_CANCEL semantics, the only shape that stays deterministic in SIM — a
  cancel racing a completion is a seed-ordered event, never a coin flip). Law:
  **every wait site handles `Cancelled` as a typed outcome.** Consumers: lease
  expiry (CONSENSUS §7's lease shadow), credit stalls (TRANSFER TR7),
  park/resume, transfer abort, and §1's ownership-tree teardown.

- The API is **completion-shaped** on all platforms (an op is submitted and completes),
  so io_uring is the native fit and readiness backends emulate completion.
- **REAL drivers, day one — no deferred work, no silent fallback:**
  - Linux: **io_uring** primary, with linked SQE support (write→fsync chains).
    Minimum kernel version declared in one place and probed at boot; below it the
    binary fails loudly naming the requirement. No epoll fallback.
  - macOS: kqueue (readiness) under the completion API. Disk ops via the blocking
    pool (kqueue does not signal regular-file readiness usefully).
  - Windows: IoRing where present, IOCP otherwise; minimum Windows version declared
    alongside the WHP requirement.
- **SIM driver**: virtual clock (time advances only by simulator decision), seeded
  PRNG, simulated transports/disk with a fault matrix (drop, delay, partition,
  reorder, duplicate, corrupt, torn write, ENOSPC, EIO, power-cut at arbitrary byte),
  and seeded cross-shard delivery order. Component code is identical under REAL and
  SIM; the driver is the only substitution point.
- **The cluster-SIM clause** (`FAULTS.md` §4 is the consumer contract): the SIM
  driver hosts **N simulated nodes in one process under one seed** over a
  simulated failure-domain tree — network edges carrying the FAULTS §3 nemesis
  vocabulary (including WAN latency distributions on inter-region edges),
  per-node simulated storage. Single-node SIM is the depth-one degenerate;
  component code is unchanged either way.

## 3. Determinism contract

- Per-shard scheduling is FIFO and single-threaded; in SIM, cross-shard delivery and
  timer firing order derive from the seed. Two SIM runs with identical (seed, input
  trace) are **bit-identical** in all outputs (WAL bytes, delta order, frame order).
- **Banned in all core crates, enforced by workspace lint (CI-fatal):**
  `std::time::{Instant, SystemTime}`, `std::thread::spawn`, `rand`/`getrandom`
  direct use, `tokio`/`async-std`/foreign runtimes, unbounded channel types, and
  **std `HashMap`/`HashSet` in component state** (DoS-randomized iteration order is a
  determinism leak — S2's receipt); deterministic-iteration maps only.
- The blocking pool (provider HTTP, file fallbacks) exists only at edges; results
  re-enter shards as messages. In SIM, blocking-pool dependencies are simulated
  components.

## 4. Memory doctrine — zero refcounting

- **`Arc` and `Rc` are denied workspace-wide** (lint). The only exceptions are named
  FFI edge modules (libkrun bindings, platform APIs) in a PR-reviewed allowlist.
- Intra-component references are **generational handles** (`u32` index + `u32`
  generation) into owner-managed arenas. A stale handle is a **typed error**, never a
  silent hit; generation checks are mandatory on every dereference.
- Shared-immutable fan-out (one payload referenced by N in-flight consumers) uses
  **explicit acquire/release counts stored as data in the owning arena** — visible in
  replay, single-threaded, auditable. Never a smart pointer.
- Buffers transfer by move; within a shard, loans (`&[u8]`) are fine; across shards,
  ownership moves or an owner-mediated handle is sent.

## 4b. The no-panic law — we do not panic. Period. Ever.

- **Statically enforced, same wall as `Arc`** (CI-fatal across every crate):
  `clippy::unwrap_used`, `expect_used`, `panic`, `unreachable`, `todo`,
  `unimplemented`, `indexing_slicing` (`.get()` or it doesn't compile),
  `arithmetic_side_effects` (checked/saturating/wrapping, explicitly chosen).
  Assertions live in test and SIM-harness code only. Every fallible operation
  returns a typed error — errors-as-artifacts as compile-time shape: failures
  are evidence values, never exceptions.
- **Allocation exhaustion is typed by construction**, never a panic source:
  budget-charged arena allocation (§4 + the VFS budget doctrine) surfaces
  exhaustion as the typed retryable error; budgets derived from physical
  anchors keep OOM unreachable by design.
- **The residue** (dependency internals; compiler-inserted paths lints cannot
  see): **`panic = "abort"` in every profile** — unwinding does not exist in
  any binary. An abort is a crash, and crash-at-any-instruction is already in
  the accepted fault scope (`FAULTS.md` §1) with the witness/WAL discipline
  guaranteeing nothing acked is lost. Every production abort is a named
  defect: counted, root-caused, surfaced through the crash-recovery forensics
  path. The ratified fault model is the backstop — no recovery plumbing
  exists for an event that must not happen.
- **No `catch_unwind` exists anywhere in the tree.** There is nothing to
  catch. (LEDGER_CORE's former "catch_unwind → failure testament" clause is
  superseded and reconciled in that spec: failure testaments are produced
  from typed error values; the mechanism never needed unwinding.)
- **Enforcement beyond lints**: release-binary panic-symbol scan in CI — no
  code path in our crates reaches `core::panicking`; per-dependency residue
  documented in the same reviewed allowlist that governs FFI `Arc`.

## 5. Provider egress (the external boundary)

hecate-wire is the only protocol between Hecate components; HTTP exists
solely at the egress edge to third-party provider APIs whose wire we don't
control. Transport: **ALPN-negotiated HTTP/2 primary, HTTP/1.1 fallback**
(the negotiated degenerate and the proxy-compatibility path — the field
pattern, incl. the Bedrock SDK's h2-default receipt), over rustls on the
dedicated tracked egress pool. The h2 client is **owned**: client-side-only
HPACK + framing + stream-level flow control — the same flow-control
discipline PROTOCOL §4 specs, applied to a wire we consume instead of
author; no tokio/hyper enters the tree; rustls stays the sole TLS. SSE and
streaming bodies ride h2 streams identically. Connection/stream counts and
window sizes derive from measured per-node concurrent-stream anchors (a
multi-agent node runs dozens-to-hundreds of concurrent provider streams —
the multiplexing case, not the CLI's low-count shape) at the definition
site. All provider traffic routes through the provider-gateway chokepoint
(Guardian-gated, epoch-checked per CONSENSUS §7's externalization-fencing
law). No HTTP type crosses into component state; no HTTP client usage exists
outside the egress module (architecture test).

## 6. Test cases (failure each catches)

| # | Test | Catches |
|---|---|---|
| T1 | Bit-reproducibility: same (seed, trace) twice → byte-identical WAL, delta sequence, frame order | any nondeterminism leak: stray clock, map iteration, racy wake |
| T2 | Planted-bug exploration: a test component with a deliberate ordering bug must be found within N seeds | a sim harness that runs but doesn't explore (hyperscale's never-executed replay guard, as a test) |
| T3 | Lint wall red: `Arc`/`Rc`/std-time/`thread::spawn`/rand/tokio/std-HashMap/unbounded channel anywhere outside allowlist → CI failure | doctrine decay |
| T4 | Reactor conformance (echo, timers, timeouts, partial IO, linked write→fsync) green on Linux/macOS/Windows CI | per-platform driver drift — the REAL≠SIM divergence class |
| T5 | No-migration: task state address stable across polls (debug assert) | work stealing reintroduced |
| T6 | Stale-handle: arena slot reuse + old handle → typed generation error, never data | use-after-free-by-index |
| T7 | io_uring linked-SQE vs emulated path produce identical results on shared test corpus | Linux fast path semantically diverging |
| T8 | Boot probe below minimum kernel/OS → loud failure naming requirement | silent degraded mode |
| T9 | Cancellation determinism: cancel-vs-completion race under seed sweep ⇒ per-seed deterministic outcome; no lost wakeups; every wait site's `Cancelled` arm exercised | nondeterministic cancel races; unhandled cancellation |
| T10 | Panic-freedom wall: clippy gate red on every panic source (negative fixtures, trybuild-style); abort profile verified in every build; release-binary symbol scan reaches no `core::panicking` from our crates (residue = the existing kill-at-any-instruction nemesis — no separate coverage needed) | panic sources compiling; unwinding reappearing |
| T11 | Teardown order: ownership-tree teardown deterministic under seed sweep; no task survives its owner (leak check at shard shutdown) | orphan tasks; nondeterministic teardown |
| T12 | Cluster-SIM smoke: N simulated nodes over a simulated failure-domain tree, partition+heal, bit-reproducible under seed | the FAULTS §4 harness drifting from the runtime |
| T13 | h2/1.1 differential: the same provider exchange over negotiated h2 and forced-1.1 fallback produces identical component-visible results (streams, errors, retries); fallback exercised in CI, not theorized | fallback rot; h2-path semantic divergence |

## 7. Acceptance criteria

1. T1 enforced in CI on every merge, permanently.
2. Zero `Arc`/`Rc` outside the reviewed FFI allowlist; zero unbounded queues; zero
   banned APIs — all lint-enforced, CI-fatal.
3. Identical component code under REAL and SIM; driver is the only substitution.
4. Tasks never migrate; cross-shard traffic is move-only over bounded channels.
5. io_uring is the operative Linux driver from the first merge that touches disk or
   network on Linux; no epoll fallback exists in the tree.
6. Performance floors are **ratcheted from first measured baseline** on pinned CI
   hardware (constants-from-data): baseline run establishes shard wake→poll
   throughput and bounded-channel p99 latency; any regression >10% fails CI.
   Provisional expectations pending baseline (≥1M wake→poll cycles/s/shard;
   channel send→recv p99 ≤ 1µs) are pre-baseline placeholders that **die on
   the first baseline run** — the measured ratchet replaces them.
7. Every derived constant (shard count, pool sizes, queue bounds) carries its
   derivation at the definition site; bare tuning literals fail review. T2's
   seed count derives from the ratcheted seed-budget floor, not a literal.
8. No task without an owner; the ownership tree is acyclic; teardown is
   deterministic (T11 permanent).
9. `Cancelled` is a typed outcome at every wait site (audit + T9); the
   cancellation surface exists from the first Driver implementation.
10. The SIM driver hosts the FAULTS §4 whole-cluster harness with zero
    component-code changes (T12).
11. No panic source compiles in any non-test crate; no unwinding exists in
    any binary (`panic = "abort"`, every profile); the LEDGER_CORE
    catch_unwind reconciliation landed in the same commit as this criterion
    (T10 permanent).
