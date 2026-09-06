# Decisions, source conflicts, and traceability

Status: proposed Focal decisions. The user specified Rust; RAM-primary storage with disk durability; laptop through multi-region/Meta-scale ambition; and minimal incremental complexity through six deployment stages. These requirements govern the adaptation. Nothing here asserts implementation is already complete.

The frozen [reference snapshot](reference/README.md) preserves upstream accepted wording even when conflicting or superseded. [01](01-source-audit.md) records observed Sylk behavior and static concerns. [02](02-domain-and-lifecycle.md) records domain decisions D-01 through D-08; the F-series below records system decisions.

## 1. Source precedence

1. User requirements and explicit corrections in this task.
2. The reconciled numbered Focal documents as the proposed implementation baseline.
3. Later explicit Hecate amendments over earlier conflicting Hecate passages.
4. Sylk source and tests as evidence of existing behavior and lessons, not automatic target semantics.
5. Historical bug reports as regression scenarios, not proof the current Sylk revision still has the bug.

Where Focal improves an underspecified algorithm, the improvement is labeled a design decision. A citation to Hecate does not validate pseudocode under arbitrary schedules or crashes.

## 2. System decisions

| ID | Decision | Reason and alternative considered | Contract / work |
|---|---|---|---|
| F01 | Custom RAM graph with durable segment log and immutable checkpoint/archive objects | User-confirmed storage model. Hecate's cold LSM/B-tree profiles are not the authoritative Focal state engine; an embedded database would obscure ownership and duplicate storage semantics. | [04](04-storage-and-distribution.md), P03/P04/P12 |
| F02 | One authoritative ordered log per session; range-shard its state/apply | Preserves Hecate effective-state, graph and delta order. Independent claim-log shards would change atomicity/order and require a different consistency design. | [00](00-target-architecture.md), P08–P11 |
| F03 | Durable quorum before success; one voter includes disk flush | A RAM-only replica ack cannot preserve acknowledged proof after the claimed failure set. Kafka-like durability does not imply Kafka implementation or ack policy. | [04](04-storage-and-distribution.md), P04/P08/P13 |
| F04 | Normal mutation success waits published visibility; read tokens expose sequence | Distinguishes durable from readable state and closes apply-lag surprises. Explicit async receipts can be added later with a different named contract. | [03](03-rust-workspace-and-interfaces.md), P04/P11 |
| F05 | Safe serial reducer is permanent oracle; tracked deterministic parallel apply | Hecate's atomic-max/safety-net sketch does not by itself establish serial equivalence. Full conflict audit and recomputation preserve one meaning. | [04](04-storage-and-distribution.md), P02/P10 |
| F06 | Maintained Rust runtime, Raft and QUIC adapters initially | User requires custom storage, not an entirely custom runtime/consensus/cryptographic transport. Hecate runtime/transport are docs, not reusable binaries. | [03](03-rust-workspace-and-interfaces.md), P00/P07/P08 |
| F07 | Owned mutable core; safe arenas first; library/internal immutable sharing permitted | Preserves writer ownership without an unenforceable blanket dependency ban on reference counting. Unsafe optimizations require measured need and proof. | [03](03-rust-workspace-and-interfaces.md), P03/P10 |
| F08 | Log-derived immutable deltas; no separate outbox | Removes Sylk cross-journal delivery gaps and current-state historical rehydration. Consumers own durable cursors. | [01](01-source-audit.md), P07 |
| F09 | At-least-once recovery delivery plus durable idempotent logical outcomes | Cursor resume and dedup do not make arbitrary external effects exactly once. Explicit effect reconciliation closes the crash gap. | [00](00-target-architecture.md), P06/P07 |
| F10 | Region topology compiles explicit failure/residency intent | Region names and replica count alone cannot promise survivability. Placement must cover voter quorum and evidence/checkpoint bytes. | [04](04-storage-and-distribution.md), P13 |
| F11 | One engine and additive deployment concepts | “Stepped complexity” is an acceptance requirement. No laptop/cluster/regional semantic modes or mandatory expert storage knobs. | [08](08-stepped-complexity-and-deployment.md), P00/P15/P16 |
| F12 | Indefinite immutable proof custody initially; bounded hot/log/cursor windows | Complete proof survives retirement. A future destructive proof-deletion policy must be explicit and cannot arise from memory pressure or WAL compaction. | [04](04-storage-and-distribution.md), P12 |
| F13 | Stable opaque object IDs plus separate versioned canonical content hashes | Keeps stable references and cyclic graph construction tractable while preserving content-identical no-op behavior. Content hash and physical/allocated identity are separate. | [02](02-domain-and-lifecycle.md), P01/P02 |
| F14 | Archive-backed identity plus request-generation fences | Bounded hot dedup cannot guarantee lifetime identity or reject arbitrarily old random request IDs without durable information. Generation floors and verified archive lookup define safe expiration. | [03](03-rust-workspace-and-interfaces.md), P02/P12 |
| F15 | Root/regional control metadata outside steady-state claim commits | Fleet scale cannot depend on one global session directory write or membership broadcast per claim. Root stores delegation; regional/session groups own local work. | [00](00-target-architecture.md), P09/P13 |

## 3. Reconciled Hecate gaps and contradictions

| Source issue | Why it cannot be copied literally | Focal resolution |
|---|---|---|
| LEDGER §3 happy-path status list omits failures required by LEDGER_CORE §4 | No exhaustive reducer or wire enum can be built from it | [02](02-domain-and-lifecycle.md) defines complete active/terminal vocabulary, transition table and refusal cases |
| LEDGER §4 describes incremental SCC machinery; LEDGER_CORE §3 explicitly rejects a global SCC monitor | Conflicting monitor ownership/algorithm | Per-parked-scope closures with SCC condensation and independent brute-force oracle |
| Rank refers to `invalidates` while LEDGER relation enumeration omits it | Security classification cannot match an unregistered string | Closed `Invalidates` variant plus exhaustive override classification |
| Earlier protocol ADR text refers to TCP; later PROTOCOL mandates QUIC | Different wire assumptions | Focal authenticated framed QUIC; no claim of Hecate byte compatibility |
| STORE generic data shard has its own log; claims-specific clauses retain one session log | Applying generic sharding to claims destroys the chosen session order | Claims partitions consume one session log; directory/metadata groups have separate authority |
| MATERIALIZER §5 mentions HRW while later STORE requires one range directory | Two independent splitters create mismatched ownership | One range map for authoritative state/apply; cache placement is separate and nonauthoritative |
| MATERIALIZER conflict detection depends on overlay observations while workers race | Smaller-index writers may not have executed yet; stale reads can evade a local check | Complete tracked access audit at deterministic barrier; recompute speculative suffix/epoch |
| MATERIALIZER read+write DAG example can add self-edges | Read-modify-write can deadlock its own node | Exclude self-edges, deduplicate predecessors, test RMW explicitly |
| Max-writer-index overlay drops earlier versions | Earlier log entries may need earlier values; write-write resolution alone does not fix read dependencies | Per-entry writes/multiversion overlay plus serial-order validation |
| Deferred entry with already computed successors | Successors may contain stale dependent computations | Discard and recompute all affected results, including indirect dependencies |
| Epoch size derived from local timing but claimed replay deterministic | Replica measurements can disagree | Version sizing inputs or prove epoch selection cannot affect state/delta outcome |
| Cross-node apply called “no 2PC” without visibility protocol | Common order does not prevent torn reads or partial apply after crash | Deterministic participant batches, recovery replay, fixed-prefix publication barrier |
| Split described as seed/tail/directory CAS | Source may keep accepting work after destination cutoff | Logged stop/activation barrier and epoch fencing before directory publication |
| Watch no-gap/no-duplicate language conflates transport and effects | Lost acknowledgements cause replay; unrelated sequence clocks are not comparable | Session sequence + ordinal cursors, explicit redelivery/Resync, idempotent consumer effects |
| “Read-your-watch” assumes current read barrier is at least received delta | Client may route to a lagged/moved reader | Carry minimum sequence across retries and wait for the requested publication prefix |
| “Small” sequencer lifecycle projection | Its size can grow with a large live session | Measure and budget projection; asynchronous versioned proofs for nonlocal admission; declare ceiling |
| Monitor closure budget “alarms” after breach | Alarm alone does not bound allocation | Reserve before registration; reject/pressure before unsafe growth |
| Idle-time or unspecified retirement cadence | Saturated systems might never reclaim | Paced debt-based custody/retirement with reserved control capacity |
| Durable log but artifact replicas not in failure contract | Committed proof could reference unavailable/lost evidence after region loss | Verify evidence custody under the same promised failure set before close |
| Minimum/maximum watermarks across different clocks | Per-shard indexes and physical offsets cannot be compared directly | Typed clock domains and explicit mapping; common session prefix for graph visibility |

The amended algorithm is motivated by the references, not a claim to have implemented Calvin or reproduced a paper's throughput. Deterministic sequencing and declared-access execution have precedent in [Calvin](https://www.cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf); Focal's precise retry, memory and publication rules remain obligations of its own implementation.

## 4. Requirement-to-work-to-test mapping

| Requirement | Architecture | Work | Verification |
|---|---|---|---|
| Rust project | 03 | P00–P01 | Build/lint/visibility and canonical fixtures |
| Claims/testaments/artifacts/validators | 02 | P01/P02/P05 | T01–T06, T12–T14 |
| Generated work not yet actionable | 02 | P02/P06 | T05 |
| Immutable content, correction lineage | 02/03 | P01/P02 | T02/T03/T14 |
| Writer and execution fencing | 02/04 | P02/P06/P08 | T04/T14/T20 |
| RAM-primary custom storage | 03/04 | P03/P10/P11 | T07/T08/T23/T30 |
| Disk-backed durability | 04 | P04/P08/P12 | T09–T11/T26 |
| Graph cycles and no lost wakes | 02/04 | P06/P11 | T15–T17/T25 |
| One session order with sharded apply | 00/04 | P08/P10/P11 | T20/T23–T25 |
| Bounded long-running memory | 04/06 | P03/P09/P12/P14 | T19/T22/T26/T30 |
| Laptop to multi-node | 00/08 | P04/P07/P08 | T09/T20/T21/T32 |
| Kubernetes without new ledger semantics | 08 | P15/P16 | DC07/DC08 and T32 |
| Multi-AZ and region survival | 00/04/08 | P13/P14 | T12/T27 |
| Global control scale | 00/04/08 | P09/P13/P14 | T22/T27/T30 |
| Minimal intuitive deployment increments | 08 | P00/P15/P16 | DC01–DC20, T32 |
| Operational recovery and upgrade | 06/08 | P15 | T10/T11/T26/T29 |

T-identifiers are defined in [06](06-verification-and-operations.md); DC-identifiers in [08](08-stepped-complexity-and-deployment.md). Package acceptance adds concrete cases beyond this index.

## 5. Inputs resolved now and measured later

| Topic | Resolution / implementation default | Remaining task |
|---|---|---|
| In-memory versus volatile | Resolved by user: RAM primary, disk durability | Implement P04/P08 guarantees |
| Deployment ambition | Resolved by user: laptop to global/Meta-scale goal | Qualify actual supported envelopes at P14 |
| Complexity progression | Resolved by user: minimal next-step concepts/config | Measure six journeys at P16 |
| Ordering model | Preserve Hecate session-wide order | Revisit only if measured workload needs a new semantic partition model |
| Runtime and transport | Proposed maintained Rust adapters | Pin/qualify concrete versions at P00/P07 |
| Hash and IDs | Stable opaque IDs; versioned BLAKE3 content identity consistent with Hecate's hash choice | Commit exact canonical byte fixtures at P01 |
| Replica geography | Per-deployment declared failures and residency | Compile/validate placement; no universal region-count default |
| SLOs and target hardware | Workload/deployment inputs, not blockers to code | Record measured local baseline then named regional/global targets |
| Destructive proof retention | Not enabled in initial design | Separate explicit product/authority decision if requested later |
| External effect exactly once | Unsupported without idempotent/reconcilable effect owner | Expose honest adapter contract and indeterminate result at P06 |
| Byte compatibility with Hecate/Sylk | Not promised | Optional import/interoperability adapters require their own tested schema mapping |

No missing deployment size justifies stopping the implementation plan. Defaults cover local use; users provide additional intent only when the next use case requires it. Scale and safety claims must follow measurements and active placement state.
