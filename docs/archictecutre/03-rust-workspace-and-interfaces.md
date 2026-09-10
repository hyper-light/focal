# Rust workspace and interfaces

Status: target interfaces with an implementation underway. Concrete crate APIs and known gaps are tracked in [09](09-implementation-status.md); pseudocode below remains a design contract, not an API compatibility promise. The domain contract is [02](02-domain-and-lifecycle.md), storage mechanics are [04](04-storage-and-distribution.md), and work packages are [05](05-implementation-plan.md).

## 1. Ownership strategy

Use deep modules: small public interfaces that hide admission, ordering, retries, memory ownership, and recovery. Public consumers submit commands, read snapshots, and consume deltas. They never acquire board locks, edit slots, call a validator in the reducer, or select physical shards.

The deterministic domain core has no network, filesystem, wall clock, task spawning, or ambient random source. It accepts explicit inputs and returns a prepared transition plus effects. A single owner handles a session's sequencing projection; each memory partition has one mutable owner. Internal storage/apply tasks consume bounded work with explicit cancellation and join ownership. Agent and validator execution remains participant-owned: the ledger never launches peers or invokes their providers. The optional `focal-runtime` embedding helper does not change this boundary. [Peer validation contract](16-peer-validation-contract.md).

Production Rust must not panic, and `Arc` must be avoided wherever ownership or borrowing can replace it. This applies throughout the implementation, not only to authoritative state. Focal initially uses a maintained Rust async/runtime adapter and consensus library; retained reference counting must have a concrete concurrent lifetime or a required dependency interface. A one-time transfer to another thread does not require an `Arc`. The [ownership and failure policy](10-ownership-and-failure-policy.md) specifies enforcement, fallible boundaries, and the reviewed remaining sharing. The arena implementation uses safe Rust; custom unsafe storage would require a separate documented safety proof and concurrency qualification before changing the current workspace prohibition.

## 2. Planned workspace

Create crates at the work package that first needs them. Do not scaffold empty abstraction crates solely because this table lists them.

| Crate | Owns | Public interface / main files | Depends on |
|---|---|---|---|
| `focal-model` | Domain IDs, authored objects, lifecycle, relations, command/result types | `ids.rs`, `claim.rs`, `testament.rs`, `artifact.rs`, `validation.rs`, `command.rs`, `delta.rs`, `error.rs` | Small codec/hash primitives only |
| `focal-core` | Pure preparation, admission, serial reducer, validation aggregation, graph oracle | `prepare.rs`, `reduce.rs`, `affordance.rs`, `lifecycle.rs`, `satisfaction.rs`, `footprint.rs` | model |
| `focal-memory` | Generational arenas, indexes, versions, immutable views, range state (stores, page-sharing split and merge, group write envelopes), the generic range map, memory and disk admission envelopes | `arena.rs`, `range.rs`, `range_split.rs`, `range_map.rs`, `index.rs`, `snapshot.rs`, `budget.rs`, `disk.rs`; optional `serde` feature for the map types | model |
| `focal-log` | Physical segment WAL, logical log adapter, durable framing, recovery | `segment.rs`, `record.rs`, `writer.rs`, `recovery.rs`, `retention.rs` | model, filesystem adapter |
| `focal-consensus` | Raft adapter, durable hard state, ReadIndex, membership, fencing | `session_log.rs`, `raft.rs`, `membership.rs`, `read_barrier.rs` | model, log |
| `focal-ledger` | Composition of session sequencer, materializer, monitors, publication | `session.rs`, `sequencer.rs`, `materializer.rs`, `publication.rs`, `monitor.rs` | model, core, memory, consensus |
| `focal-evidence` | Chunk ingest, content verification, custody, validator contracts and optional participant-side evaluation helpers | `chunks.rs`, `manifest.rs`, `registry.rs`, `dispatch.rs`, `verdict.rs` | model; ledger client seam only |
| `focal-stream` | Subscription cursor, projection seed, credits, bounded replay/fanout | `cursor.rs`, `subscription.rs`, `seed.rs`, `fanout.rs` | model, ledger read seam |
| `focal-wire` | Canonical domain codec and bounded network envelopes | `canonical.rs`, `frame.rs`, `version.rs`, `message.rs` | model |
| `focal-client` | In-process/network adapters, typed authored operations, human DTO conversion, routing, durable retry and query token propagation | `client.rs`, `operations/`, `input/`, `pending/`, `query/`, transport adapters | model, wire |
| `focal-native-client` | Native document-to-frame compiler, ledger-binding resolution, `FCNINPUT` frame encoder and owner-side intent fingerprint for the CLI and MCP hosts (decision F17); the `n1:` operation journal itself lives in `focal-client` | `compile.rs`, `resolve.rs`, `frame.rs` | model, memory, core, evidence, wire, client |
| `focal-node` | Runtime composition, authenticated transport, placement, budgets, boot/shutdown and thin manual CLI | `main.rs`, `cli/`, `config.rs`, `boot.rs`, `transport.rs`, `directory.rs`, `placement.rs`, `admission.rs` | Concrete adapters above |
| `focal-mcp` (planned if a separate crate is warranted) | Bounded MCP protocol adapter and capability-scoped tool/resource registry | `server.rs`, `tools.rs`, `resources.rs`, `transport.rs`; no business reducer | client, chosen qualified MCP transport/codec |
| `focal-archive` | Checkpoint/archive manifests, retrieval, retirement, restore | `checkpoint.rs`, `custody.rs`, `catalog.rs`, `restore.rs`, `gc.rs` | model, log, evidence; snapshot seam |
| `focal-sim` | Deterministic driver, fault injection, reference oracle, reproducible histories | `driver.rs`, `network.rs`, `disk.rs`, `nemesis.rs`, `history.rs` | Production pure modules and adapters under test |

Avoid cyclic dependencies: core defines reducer inputs without importing the ledger; evidence calls a model-defined submission port without importing node composition; archive reads immutable snapshot exports rather than owning the memory store. The same published client interface is exercised by embedded and network conformance tests.

Physical WAL offsets belong to `focal-log`; Raft hard state belongs to `focal-consensus`; graph lifecycle belongs to `focal-core`. A generic `storage` object that exposes all three would erase the safety seams.

## 3. Toolchain and dependency decisions

The inspected workstation has Rust/Cargo 1.94.1. P00 should pin that toolchain initially, verify supported targets and dependency MSRVs, and record any justified change. Use edition 2024; edition selection is a package setting, separate from the toolchain pin. See the [Rust edition guide](https://doc.rust-lang.org/edition-guide/rust-2024/index.html).

Proposed dependency choices below are implementation defaults, not claims that their APIs have already been integrated. P00 records selected versions, licenses, MSRV, features, advisory checks, and lockfile. Never use an unbounded `latest` version in a reproducible build.

| Need | Proposed choice | Limit / qualification |
|---|---|---|
| Async/network runtime | Tokio behind node/client adapters | No runtime dependency in pure reducer; account spawned tasks and queues |
| Consensus algorithm | TiKV `raft-rs` adapter | Integrator owns durable storage, message transport, Ready handling, snapshots, and correct advancement; library alone does not provide a durable database |
| Reliable transport | Maintained QUIC implementation, initially evaluate Quinn | Authenticate peers; one framed protocol; no custom cryptography; measure supported laptop/server targets |
| TLS and hashing | Maintained TLS implementation and BLAKE3 library | Explicit hash algorithm/domain/schema version, following Hecate's hash choice; content digest is not an authorization credential |
| Serialization | Focal versioned canonical codec for authored identity; typed bounded wire codec | Never derive identity from generic JSON map ordering or default Rust enum layout |
| Test generation | Property generator plus deterministic SIM; small-step schedule enumeration | Production reducer is shared; fault models must cover filesystem and network behavior |
| Benchmarking | Criterion or equivalent plus process/cluster workload runner | Store workload, hardware, config, distribution, and raw results |

`raft-rs` exposes consensus building blocks with an embedding/storage integration surface; its [upstream repository](https://github.com/tikv/raft-rs) is the primary integration reference. Validate the concrete adapter against the selected release. A custom in-memory graph does not require a custom consensus algorithm.

Hecate specifies custom `hecate-rt`, `hecate-wire`, and custom QUIC. Focal preserves their required semantics but proposes mature Rust adapters for the first implementation. Byte-level Hecate wire compatibility is not claimed; there is no working Hecate server to interoperate with. Publish Focal's versioned protocol and a future adapter seam.

## 4. Identifiers and values

Use distinct transparent newtypes and checked constructors. Public IDs are stable across restarts and moves; arena handles are process-local implementation details.

```rust
// Illustrative shapes; exact codec derives are implemented in P01/P07.
pub struct TenantId([u8; 16]);
pub struct SessionId([u8; 16]);
pub struct LedgerId { pub tenant: TenantId, pub session: SessionId }
pub struct ObjectId([u8; 16]);
pub struct ClaimId(ObjectId);
pub struct TestamentId(ObjectId);
pub struct ArtifactId(ObjectId);
pub struct ValidationId(ObjectId);
pub struct ParticipantId([u8; 16]);
pub struct RequestId([u8; 16]);
pub struct RequestEpoch(u64);
pub struct SessionSeq(u64);
pub struct ObjectRevision(u64);
pub struct RouteEpoch(u64);
pub struct ContentHash([u8; 32]);
pub struct EvidenceDigest([u8; 32]);
pub struct RaftIndex(u64);
pub struct RaftTerm(u64);
pub struct WalOffset { stream: u32, segment: u64, byte: u64 }

// Private to focal-memory; never serialized as an object reference.
struct Handle<T> {
    arena: RangeInstanceId,
    slot: u32,
    generation: u64,
    marker: std::marker::PhantomData<fn() -> T>,
}
```

Choose and register ID-generation rules at P01. IDs generated outside the reducer enter the logged command; replay never generates fresh IDs. A `(LedgerId, ObjectId)` is the complete proof address. Stable opaque allocated IDs are distinct from the versioned canonical content digest and its dedup index. Validate uniqueness, tenant/session binding, integer exhaustion, arena provenance and generation overflow. Handle reuse with a mismatched generation returns stale-handle internally; it cannot address a newly allocated claim accidentally.

Ordered indexes use canonical key bytes and explicit comparison. Rust struct memory layout, hash-map iteration, pointer values, locale, and wall-clock time are never persistent encodings.

## 5. Public ledger interface

Expose three primary operations plus artifact upload and participant integration. The pseudocode below describes the caller contract; it is not compiled code delivered in this documentation task.

```rust
pub trait Ledger {
    async fn submit(&self, request: MutationRequest)
        -> Result<MutationReply, AccessError>;
    async fn read(&self, request: ReadRequest)
        -> Result<ReadPage, ReadError>;
    async fn subscribe(&self, request: SubscribeRequest)
        -> Result<Subscription, StreamError>;
}

pub struct MutationRequest {
    pub ledger: LedgerId,
    pub request_epoch: RequestEpoch,
    pub request_id: RequestId,
    pub expected_revision: Option<ObjectRevision>,
    pub command: Command,
}

pub enum MutationReply {
    Committed { receipt: MutationReceipt, outcome: CommandOutcome },
    Inform(CurrentAffordance),
    Yield { monitor: MonitorId, observed: ReadToken },
    Refuse(StructuralViolation),
}

pub struct MutationReceipt {
    pub ledger: LedgerId,
    pub request_epoch: RequestEpoch,
    pub request_id: RequestId,
    pub sequence: SessionSeq,
    pub read_token: ReadToken,
    pub outcome_hash: ContentHash,
}
```

Rust static dispatch is the initial choice; an object-safe boxed-future adapter can be added where embedding requires runtime polymorphism. Do not pretend native async trait methods are automatically object-safe.

Authenticated principal and cause context are supplied by the trusted ingress/runtime, not blindly trusted from request payload fields. Root session work has an explicit root cause; nested work inherits the calling turn's cause. Policy decisions carry pinned authority epoch and evidence into preparation. A user-provided `actor=admin` field cannot establish authority.

`AccessError` covers wire/authentication, wrong owner, capacity, unavailable quorum, and unknown transport outcome. Structural domain refusal is an explicit domain result. Ordinary dependency and lifecycle obstacles produce Inform/Yield. These distinctions survive network serialization and client presentation.

Read requests include `ReadConsistency::{Linearizable, AtLeast(ReadToken), Snapshot(SnapshotToken), StaleProjection}`; `Query::{Get, Traverse, ListClaims, GetReceipt}`; filters; and server-bounded page budgets. A returned page names its exact prefix, continuation, and any archive continuation. A cursor binds query hash and authority scope so it cannot be reused to change sessions or filters.

`AtLeast` preserves its minimum SessionSeq while following current ownership; an old route epoch prompts routing refresh. `Snapshot` instead preserves its exact prefix across pages for a bounded pin lease, or returns a typed expiry/restart result. A topology move cannot silently change the snapshot's meaning.

## 6. Request identity and retry

The request key is `(authenticated principal, LedgerId, RequestEpoch, RequestId)` and binds a canonical command digest. The client negotiates its epoch automatically; users do not configure it. Reusing a key with different content yields `IdempotencyConflict`. Exact retry returns the original committed result, even if the claim has moved or terminalized. Expected revision prevents stale concurrent edits; replay applies already-validated prepared mutations without rerunning current policy.

Content identity is a separate rule over canonical authored fields. It prevents duplicate proof objects even under new request IDs. Historical identities survive hot retirement in the archive catalog. Before creating new content when its identity index is cold, resolve the archived identity and feed the verified lookup result into the sequencer; do not introduce an unbounded in-memory lifetime index. The lookup carries its pinned catalog generation/prefix and is revalidated against the effective pending state when it returns. Retirement/catalog updates preserve the pin or invalidate and retry the lookup; a stale absence cannot authorize creation. While lookup is unavailable, creation waits or returns typed capacity/unavailability, never assumes absence.

The durable request-result journal can be compacted into an archive index with a declared retention contract. The session stores a durable minimum admitted RequestEpoch per `(authenticated principal, LedgerId)`; receipts for still-admitted epochs are retained. Multiple clients for that principal share an admitted epoch and use distinct RequestIds; reconnecting negotiates the existing valid epoch and does not itself advance the floor. Advancing that floor invalidates all old-generation requests, including IDs never previously seen. Below the floor, return an exact archived receipt when found, otherwise `RequestHistoryExpired`; never execute an old unknown key as fresh work. The client must reconcile an unknown old outcome before creating a new-epoch operation. Consumer cursors and content-identity records have their own retention obligations.

## 7. Pure core seam

```rust
pub fn prepare(
    effective: &impl AdmissionView,
    input: AuthenticatedInput,
) -> Result<PreparedMutation, AffordanceResult>;

pub fn reduce(
    view: &impl ReadState,
    entry: &CommittedMutation,
) -> Result<TransitionBatch, DeterminismFault>;
```

`PreparedMutation` contains assigned IDs, normalized authored fields, expected revisions, pinned registry/policy inputs, explicit timestamps/deadline events, declared footprint, and any already-observed handler result. `CommittedMutation` adds session order and schema version. A `TransitionBatch` contains all object/index updates, immutable delta facts, request receipt, monitor changes, and effect intents for the one transaction.

Preparation does not precompute the full graph/index reduction; that work belongs to apply. A yielded preparation result is a request to install a durable monitor registration, not permission to return an unregistered MonitorId. The ledger logs/registers the scope at a captured prefix, catches up its suffix atomically, and only then returns Yield or the already-settled result. Inform and Refuse require no proof mutation.

Core-local admission projection must include every field required for structural decisions. Complex graph-dependent queries either maintain sufficient deterministic summaries or asynchronously fetch a version-pinned proof and retry against the same prefix. No synchronous remote lookup occurs inside the owner loop. Budget the projection; do not claim lifecycle/affordance state is constant-sized.

The serial reducer is the reference oracle permanently. Parallel execution uses tracked reads and writes through this same interface. Missing footprints trigger deterministic recomputation, not a different lifecycle implementation.

## 8. Evidence and evaluator seam

An artifact upload is a bounded resumable byte transfer followed by digest verification and durable custody. Only a verified `ArtifactRef` can enter a closing testament. Inline metadata still counts against frame and memory budgets. Custom artifact kinds register stable schema IDs; kind names are open, lifecycle and relationship enums are closed.

Validator registrations declare immutable version/digest, accepted artifact schema, output schema, execution kind, required/observe policy, priority, deadline policy, and whether an agentic quality bar follows deterministic success. Registration changes create new versions. An execution request binds claim/testament/validation IDs, exact evidence digest, registry version, attempt, and fencing token.

A completed result references evidence, typed disposition, and the same token. The core accepts it only for the active validation generation. Missing artifact, failed evidence, and validator crash/timeout remain distinct outcomes. An executed error becomes durable evidence through a later command; a disk failure cannot manufacture a committed error fact before durability recovers.

## 9. Wire and compatibility requirements

- Define append-only numeric enums, versioned schema IDs, bounded lengths/counts, and canonical byte order. Unknown critical commands or enum variants are rejected before allocation or state changes.
- Authenticated envelopes carry session identity, request identity, negotiated protocol version, trace context, and route epoch. Direction and size class are registered per message type.
- Separate control/credit traffic from large evidence transfers so large uploads cannot starve commits or completion acknowledgements.
- Encode deltas as `(session, sequence, ordinal)` plus immutable transition content. Multiple deltas from one compressed transaction keep distinct ordinals and order.
- Pin canonical hashing version in content identities. Schema upgrades may read old versions, but cannot reinterpret old committed statuses or change replay results.
- Rolling upgrade feature activation is a committed capability fence after all voting and serving members can decode the new format. Downgrade is permitted only before activating incompatible persisted features.

The current `focal-wire::Operation` registry is below. Registered tags are explicit protocol identifiers; the current postcard envelope separately encodes the Rust enum's zero-based ordinal. Preserve both columns when appending operations. Registered tags alone do not make reordering the enum compatible.

| Operation | Registered tag | Postcard ordinal |
|---|---:|---:|
| Submit | 1 | 0 |
| Read | 2 | 1 |
| Subscribe | 3 | 2 |
| Raft | 4 | 3 |
| OpenEpoch | 5 | 4 |
| Stream | 6 | 5 |
| Upload | 7 | 6 |
| Download | 8 | 7 |
| Control | 9 | 8 |
| Custody | 10 | 9 |
| PeerControl | 11 | 10 |
| NodeContact | 12 | 11 |
| EnrollmentControl | 13 | 12 |
| PlacementControl | 28 | 27 |
| SessionSign | 29 | 28 |
| Probe | 30 | 29 |

`EnrollmentControl` permits the immutable genesis founder's certificate and dedicated signer principal to read enrollment state and submit enrollment decisions through the current root leader. The receiver checks the pinned root identity, active committed enrollment, permitted command and principal, and a fresh quorum read before releasing a result, including a cached receipt. Its request payload is bounded at 128 KiB. It grants no general Runtime, placement, or membership authority. `NodeContact` commits only certificate-bound reachability; `PeerControl` accepts bounded metadata reads. `PlacementControl` (2026-09-09, [24](24-placement-execution-and-fleet-control.md) §8) lets an enrolled node submit to a directory partition owner only its own enrollment, load, progress and readiness under a certificate-bound Node identity, plus bounded reads; a plan, a fence or another node's facts are refused before decoding. `SessionSign` asks a node to sign one session fact it can witness from its own hosted replica and returns that node's signature alone; quorum is assembled by the caller. Both are bounded (256 KiB and 64 KiB) and unavailable to trusted local Node grants without a certificate. `Probe` (2026-09-09, [24](24-placement-execution-and-fleet-control.md) §12) carries one liveness probe or its acknowledgement between enrolled nodes (at most 8 KiB, certificate-bound, answered by the data service out of the liveness driver's state without an owner round trip); it grants nothing and a probe naming a node other than the authenticated one is refused before the driver sees it. The peer pool sends probes on a lane of their own (`PeerPoolLimits.max_probe_inflight`, one per peer) so replication to an unresponsive peer cannot starve the failure detector. `RootObservation` is a local owned export of one durable prefix, has no wire selector, and supplies no quorum authority.

## 10. Configuration is a product interface

See [08](08-stepped-complexity-and-deployment.md). Most users should never learn SessionSeq, arena generations, materializer epochs, routing cuts, Raft tuning, or checkpoint watermarks. The runtime derives those from resource budgets, topology, and measured costs.

The irreducible user choices are location/data custody, who may join, desired failure tolerance, and capacity/resource constraints. The same configuration schema grows through local, VM, Kubernetes, multi-AZ, regional, and global deployment. `explain` output states effective guarantees and why automatic placement or admission could not satisfy them.

## 11. Manual and agent adapters

The added manual and agent interfaces follow [13](13-cli-and-agent-implementation-plan.md). Flag, JSON and YAML parsers compile through one typed application schema before building a wire request. MCP and skills call that same operation surface; they do not own reducer state. New query, receipt reconciliation, validation-result and child-cause admission seams require explicit protocol changes and authorization tests. Existing source arrays/enum tags remain compatible; readable CLI IDs belong in separate human DTOs. Adapter cancellation releases its owned buffers without canceling an already admitted durable mutation.

## 12. Repository checks

P00 introduces formatting, lint, dependency-direction checks, and deterministic test targets. Public mutation methods outside the canonical ingress are forbidden by visibility and architecture tests. Business-state crates prohibit ambient time and randomness and remain safe Rust initially.

Document each proposed future unsafe block's aliasing, lifetime, reclamation, ordering, and panic assumptions before relaxing the current prohibition. Use Miri for applicable memory tests and a schedule exploration tool for concurrency primitives. Review every Focal-controlled `Arc`, including immutable state and adapters, for an owned or borrowed replacement; document required dependency sharing explicitly.
