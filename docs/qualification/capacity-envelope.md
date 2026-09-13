# Capacity envelope

What one session, one node, and a fleet can be expected to sustain, and on what
evidence. Every row is labelled **measured** (a run recorded under
[performance/](performance/)), **extrapolated** (arithmetic from a measured
figure, stated as such), or **simulated/unmeasured** (an architectural bound not
yet exercised end to end). Nothing here is a marketing number; the
single-session ceiling is stated separately from fleet scaling, and global
("Meta") scale is given only as an envelope, never as a demonstrated result.

Measured figures below are macOS arm64 (Apple Silicon, 18 logical CPUs),
`bench` profile — see [performance/2026-09-12-macos-arm64.md](performance/2026-09-12-macos-arm64.md).
Absolute numbers move with hardware; the *shapes* (flat vs linear vs quadratic,
CPU- vs copy- vs fsync-bound) are what the envelope rests on.

## Per-operation floors (one node, one session, small state)

The fixed cost every operation pays before any I/O or consensus. These bound how
fast a single owner can go, and none is the bottleneck relative to durability.

| Path | Cost | Basis | Evidence |
|---|---|---|---|
| Memory admission (`MemoryBudget` reserve/commit) | ~8 ns/op | measured | benches/budget |
| Request codec fixed overhead (`encode`/`decode`) | ~40 ns/op each way | measured | benches/codec |
| Domain `prepare` (admission + claim validation) | ~4–8 µs/op | measured | benches/reduce |
| Domain `apply` at small state | ~170 µs/op | measured | benches/reduce |

## Durability (one node)

| Path | Cost | Basis | Evidence |
|---|---|---|---|
| Durable append, one record per fsync | ~12–15 ms, ~80 rec/s | measured (APFS `F_FULLFSYNC`) | benches/append |
| Durable append, 256 records per fsync | ~14 ms, ~17 K rec/s | measured | benches/append |

The fsync latency dominates and is nearly independent of batch size, so committed
throughput is set by **batching**, not per-record work: batching amortizes one
~13 ms sync across the batch (~200× from 1→256 records/fsync). The shared WAL
batches concurrent session and control appends into one durable write for exactly
this reason. On server-grade NVMe the per-fsync figure is typically far lower;
the *batching-dominated* shape holds regardless.

## The one-session ceiling (the number that bounds sharding)

`apply` — the serial F05 reduce — grows with the number of claims already in the
session: ~179 µs/op over the first 1000 claims, ~645 µs/op over the next 1000
(**measured**, benches/reduce), i.e. roughly linear per-op in accumulated claims,
so **quadratic total** for one session. Verified general (a distinct root cause
per claim reproduces the same growth), not a fixture artifact.

| Session size (claims) | apply/op | Basis |
|---|---|---|
| ~1 K | ~180 µs | measured |
| ~2 K | ~650 µs | measured |
| ~10 K | ~3 ms (order of magnitude) | extrapolated (linear per-op) |
| ~100 K | tens of ms/op | extrapolated — beyond a sound single-session range |

This is the ceiling the architecture **measures rather than hides**: a single
serial domain has a per-session scaling limit. The answer is not a faster serial
apply but spreading a large session across ranges/materializers
([25](../archictecutre/25-parallel-materialization-and-ranges.md), R7): once a
session exceeds the low-thousands of live claims, its ranges should be split so
each materializer reduces a bounded share. The measured figure is what a
range-split policy should be tuned against; a single session is not intended to
hold 100 K live claims on one owner.

## Fleet and global scale (envelope, not yet measured here)

Fleet scaling is **orthogonal** to the single-session ceiling: independent
sessions/tenants each own a separate `MemoryBudget` envelope and a separate serial
owner, so cross-session work does not contend on the same counters (the budget
contention measured in benches/budget — ~131→270 ns/op at 2→8 threads sharing
*one* envelope — applies only within a single shared envelope, not across
sessions). Horizontal capacity is therefore a placement/replication property, not
a single-owner one.

| Dimension | Bound | Basis |
|---|---|---|
| Independent sessions per node | placement- and memory-budget-limited | simulated/unmeasured (R6/R9 harnesses) |
| Copies per session (durability) | `durability.survive` policy | simulated/unmeasured |
| Cross-region placement | residency-fenced; latency = inter-region RTT | simulated/unmeasured |
| Global ("Meta") scale | an envelope of independent partitions × per-partition capacity | **envelope only** — not demonstrated |

Fleet throughput/latency under real workloads, multi-region convergence, and
sustained bounded-resource operation are the province of the R9 deployment
journeys and the R11 workload generator and fault campaign; when those runs
exist their measured numbers replace the simulated rows above. Until then, treat
fleet and global figures as design bounds, not results.

## How to refresh this

Re-run the leaf-crate benches (`cargo bench -p focal-memory --bench budget`, and
likewise `focal-wire`/`codec`, `focal-log`/`append`, `focal-core`/`reduce`;
`FOCAL_BENCH_ITERS` overrides counts), record a dated file under
[performance/](performance/), and update the measured rows here. The extrapolated
and simulated rows become measured only when a real run backs them.
